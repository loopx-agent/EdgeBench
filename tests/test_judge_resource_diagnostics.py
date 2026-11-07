# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Resource evidence does not determine scoring, retries, or opportunities."""
from __future__ import annotations

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from fastapi.testclient import TestClient
from test_judge_failures import SyntheticBackend, task as _task
from test_judge_checkpoint import capture, register, restore, submit
from test_judge_checkpoint import service as _service
from test_judge_checkpoint_tail import adoption, restart
from test_submit_recovery import judge as _judge, accepted_id

from sforge.harness import judge_server as native
from sforge.harness.backend.base import ContainerResourceObservation
from sforge.harness.backend.docker_backend import DockerBackend, DockerContainerHandle
from sforge.harness.config import SForgeConfig
from sforge.harness.run_evaluation import EvaluationError, judge_submission

OBSERVATION = ContainerResourceObservation(True, 128 * 1024**2, True)
task = _task
service = _service
judge = _judge


def test_observation_summary_keeps_false_distinct_from_unknown():
    observed = ContainerResourceObservation(False, None, False)
    assert "OOM observed=False" in observed.summary()
    assert "memory_limit_bytes=None" in observed.summary()
    assert "container_running=False" in observed.summary()


@pytest.mark.parametrize("oom,limit,running,expected", [
    (True, 134217728, True, OBSERVATION),
    (True, None, None, ContainerResourceObservation(True)),
    (True, 0, False, ContainerResourceObservation(True, 0, False)),
    (True, "secret", "secret", ContainerResourceObservation(True)),
    (True, True, 1, ContainerResourceObservation(True)),
    (False, 134217728, True, None),
    (None, 134217728, True, None),
    ("true", 134217728, True, None),
])
def test_docker_observation_is_bounded_and_allowlisted(oom, limit, running, expected):
    api = Mock()
    api._result.return_value = {
        "State": {"OOMKilled": oom, "Running": running, "Error": "private"},
        "HostConfig": {"Memory": limit}, "Config": {"Env": ["SECRET=value"]},
        "Mounts": [{"Source": "/private/host/path"}], "Id": "private-id",
    }
    api._url.return_value = "unused-url"
    backend = DockerBackend(SimpleNamespace(api=api))
    actual = backend.resource_observation(DockerContainerHandle(SimpleNamespace(id="fixture")))
    assert actual == expected
    assert api._get.call_args.kwargs == {"timeout": 2.0}
    if actual:
        assert set(asdict(actual)) == {"memory_oom_observed", "memory_limit_bytes", "container_running"}
        assert "private" not in json.dumps(asdict(actual))
    assert api.inspect_container.call_count == 0


@pytest.mark.parametrize("timed_out,valid,score", [(False, True, 7), (True, False, 0)])
def test_observation_preserves_grade_and_original_timeout(tmp_path, task, timed_out, valid, score):
    backend = SyntheticBackend(None, score=score, valid=valid, timed_out=timed_out)
    order = []
    backend.resource_observation = lambda handle: (order.append("observe") or OBSERVATION)
    original_cleanup = backend.cleanup_container
    backend.cleanup_container = lambda *args: (order.append("cleanup"), original_cleanup(*args))
    result = judge_submission(task, b"", SForgeConfig(), backend, log_dir=tmp_path)
    report = result.to_dict()
    assert report["score"] == score and report["valid"] is valid
    assert report["timed_out"] is timed_out
    assert report["resource_diagnostics"] == asdict(OBSERVATION)
    assert order == ["observe", "cleanup"]
    assert json.loads((tmp_path / "report.json").read_text()) == report


@pytest.mark.parametrize("observation", [None, "unavailable", "malformed"])
def test_missing_or_unavailable_observation_preserves_legacy_report(tmp_path, task, observation):
    baseline = SyntheticBackend(None)
    expected = judge_submission(task, b"", SForgeConfig(), baseline,
                                submission_id="fixture", submitted_at=123, log_dir=tmp_path / "base").to_dict()
    backend = SyntheticBackend(None)
    if observation == "unavailable":
        def fail(handle):
            raise requests.exceptions.ReadTimeout("SECRET credential daemon address")
        backend.resource_observation = fail
    elif observation == "malformed":
        backend.resource_observation = lambda handle: {"secret": "never project raw provider dictionaries"}
    actual = judge_submission(task, b"", SForgeConfig(), backend,
                              submission_id="fixture", submitted_at=123, log_dir=tmp_path / "head").to_dict()
    assert actual == expected and "resource_diagnostics" not in actual
    assert "SECRET" not in (tmp_path / "head/run_instance.log").read_text()
    assert len(backend.cleaned) == 1


def test_execution_failure_retains_safe_resource_evidence_and_cleanup(tmp_path, task):
    backend = SyntheticBackend("execute")
    backend.resource_observation = lambda handle: OBSERVATION
    with pytest.raises(EvaluationError) as caught:
        judge_submission(task, b"", SForgeConfig(), backend, log_dir=tmp_path)
    assert str(caught.value).startswith("judge_execution_failed:execute_evaluation;")
    assert "memory_limit_bytes=134217728" in str(caught.value)
    assert "does not determine" in str(caught.value)
    assert "private-backend-detail" not in str(caught.value)
    assert len(backend.cleaned) == 1
    assert not (tmp_path / "report.json").exists()


def test_terminal_tail_roundtrip_preserves_diagnostics_without_refilling(service, monkeypatch):
    state, client = service
    original = native.judge_submission
    def grade(**kwargs):
        data = original(**kwargs).to_dict()
        data["resource_diagnostics"] = asdict(OBSERVATION)
        return SimpleNamespace(to_dict=lambda: data)
    monkeypatch.setattr(native, "judge_submission", grade)
    token = register(state, client, max_agent_submissions=1, submission_cooldown=3600)
    prefix = capture(state, client, token)
    submitted = submit(client, token, 7)
    sid = submitted.json()["submission_id"]
    before = state.get_result_for_token(token, sid)
    assert before["report"]["score"] == 7 and before["report"]["valid"] is True
    body = adoption(state, token, prefix)
    app = restart(state)
    with TestClient(app) as fresh:
        adopted = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
        assert adopted.status_code == 200, adopted.text
        receiver = app.state.judge
        new = restore(receiver, fresh, adopted.json(), run="next").json()["token"]
        origin = json.loads((receiver.config.log_dir / "runs/next/checkpoint-origin.json").read_text())
        restored_sid = origin["submission_id_map"][sid]
        result = fresh.get("/api/v1/result/" + restored_sid, params={"token": new})
        assert result.status_code == 200
        assert result.json()["report"]["resource_diagnostics"] == asdict(OBSERVATION)
        assert result.json()["report"]["score"] == before["report"]["score"]
        assert receiver.tokens[new]["next_agent"] == 2
        assert submit(fresh, new, 9).status_code == 429


def test_generated_client_renders_resource_details_without_new_submission(judge, monkeypatch):
    original = native.judge_submission
    def grade(**kwargs):
        report = original(**kwargs)
        report.resource_diagnostics = OBSERVATION
        return report
    monkeypatch.setattr(native, "judge_submission", grade)
    result = judge.run("--no-wait")
    assert result.returncode == 0, result.stderr
    sid = accepted_id(judge)
    detailed = judge.run("--result", sid, "--details")
    assert detailed.returncode == 0, detailed.stderr
    assert "Score:       7" in detailed.stdout
    assert "Memory OOM observed; memory limit (bytes): 134217728; container running: true" in detailed.stdout
    assert "does not determine the evaluation result" in detailed.stdout
    ordinary = judge.run("--result", sid)
    assert ordinary.returncode == 0 and "Resource observation" not in ordinary.stdout
    machine = judge.run("--result", sid, "--json")
    assert machine.returncode == 0, machine.stderr
    assert json.loads(machine.stdout)["report"]["resource_diagnostics"] == asdict(OBSERVATION)
    assert len(judge.graded_answers) == 1
    assert judge.state.tokens[judge.token]["next_agent"] == 2
