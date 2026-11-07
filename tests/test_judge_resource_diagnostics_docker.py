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

"""Explicit small synthetic OOM qualification, never a model or benchmark run."""
from __future__ import annotations

import json
import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SFORGE_RESOURCE_DOCKER_TEST") != "1",
    reason="Requires explicit isolated Docker resource qualification",
)


@pytest.mark.parametrize("fallback", [True, False], ids=["child_oom_valid_score", "child_oom_original_timeout"])
def test_native_resource_diagnostics_do_not_decide_grade(tmp_path, monkeypatch, fallback):
    import docker
    import requests
    from test_judge_checkpoint_docker import server, wait_for
    from sforge.harness import judge_server as native
    from sforge.harness.config import SForgeConfig

    name = "resource-diagnostic-" + uuid.uuid4().hex[:10]
    for variable, filename in [
        ("SFORGE_JUDGE_HOST_SLOT_FILE", "slots.json"),
        ("SFORGE_JUDGE_HOST_OWNER_FILE", "owners.json"),
        ("SFORGE_JUDGE_HOST_MEMORY_FILE", "memory.json"),
    ]:
        monkeypatch.setenv(variable, str(tmp_path / filename))
    client = docker.from_env()
    base = client.images.get("python:3.12-slim")
    image = f"{name}.judge.fixture:fixture"
    base.tag(f"{name}.judge.fixture", tag="fixture")
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    (tasks / "BENCHMARK.yaml").write_text(f"name: {name}\nbase_images: {{python: {{}}}}\n")
    code = (
        "import json,subprocess,time; "
        "child=subprocess.run(['python3','-c','x=bytearray(512*1024*1024)']); "
        "print('Child exit:',child.returncode,flush=True); "
        "assert child.returncode == -9; "
        + ("print(json.dumps(dict(score=7,valid=True)),flush=True)" if fallback else "time.sleep(60)")
    )
    import shlex
    (tasks / "fixture.json").write_text(json.dumps({
        "task_id": "fixture", "name": "Synthetic resource observation", "base_image": "python",
        "platform": "linux/amd64", "cwd": "/tmp", "submit_paths": ["."], "submit_exclude": [],
        "work": {"image_tag": "fixture", "agent_query": "Synthetic resource fixture"},
        "judge": {"image_tag": "fixture", "parser": "structured_json", "eval_timeout": 12,
                  "eval_cmd": "python3 -c " + shlex.quote(code)},
    }))
    config = SForgeConfig(tasks_dir=tasks, log_dir=tmp_path / "logs", judge_cpu_limit=1, judge_mem_limit="128m")
    app = native.create_app(config)
    try:
        with server(app) as url:
            http = requests.Session()
            http.trust_env = False
            state = app.state.judge
            response = http.post(url + "/api/v1/register", json={
                "task_id": "fixture", "run_id": "source", "admin_secret": state.admin_secret,
                "max_agent_submissions": 2, "submission_cooldown": 3600,
            }, timeout=10)
            response.raise_for_status()
            token = response.json()["token"]
            submitted = http.post(url + "/api/v1/submit", data={"token": token},
                                  files={"archive": ("candidate.tar.gz", b"")}, timeout=10)
            submitted.raise_for_status()
            sid = submitted.json()["submission_id"]
            def result():
                response = http.get(url + "/api/v1/result/" + sid, params={"token": token}, timeout=10)
                response.raise_for_status()
                data = response.json()
                return data if data["status"] in ("completed", "error") else None
            result = wait_for(result, 55)
            assert result["status"] == "completed", result
            report = result["report"]
            assert report["resource_diagnostics"] == {
                "memory_oom_observed": True, "memory_limit_bytes": 128 * 1024**2,
                "container_running": True,
            }
            assert report["timed_out"] is (not fallback)
            if fallback:
                assert report["valid"] is True and report["score"] == 7
                assert report["runtime_seconds"] < 12
            else:
                assert report["valid"] is False and report["score"] is None
                assert report["runtime_seconds"] >= 12
            logdir = tmp_path / "logs/runs/source/fixture/submissions/agent-1"
            assert "Child exit: -9" in (logdir / "test_output.txt").read_text()
            assert json.loads((logdir / "report.json").read_text())["resource_diagnostics"] == report["resource_diagnostics"]
            denied = http.post(url + "/api/v1/submit", data={"token": token},
                               files={"archive": ("candidate.tar.gz", b"")}, timeout=10)
            assert denied.status_code == 429
            assert state.tokens[token]["next_agent"] == 2
            assert len(state.get_run_history("source", "fixture")["entries"]) == 1
            (tmp_path / "qualification.json").write_text(json.dumps({
                "fallback": fallback, "status": result["status"], "report": report,
                "native_submissions": 1, "second_submit_http": denied.status_code,
            }, indent=2))
            wait_for(lambda: not client.containers.list(all=True, filters={"name": name + ".judge"}), 10)
            http.close()
    finally:
        # Only this test's uniquely named containers and image tag are owned.
        for container in client.containers.list(all=True, filters={"name": name + ".judge"}):
            container.remove(force=True)
        client.images.remove(image)
        client.close()
