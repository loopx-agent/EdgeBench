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

"""Native HTTP/owner contracts; synthetic grading here, real Docker separately."""

from __future__ import annotations

import copy
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from sforge.harness import judge_server as native
from sforge.harness.checkpoint import (
    CheckpointUnavailable, _decode_manifest, _encode, _encode_manifest, _sha,
)
from sforge.harness.config import SForgeConfig


def task_files(root):
    root.mkdir()
    (root / "BENCHMARK.yaml").write_text(
        "name: checkpoint-test\nbase_images: {python: {}}\n"
    )
    (root / "fixture.json").write_text(
        json.dumps(
            {
                "task_id": "fixture",
                "name": "Synthetic checkpoint",
                "base_image": "python",
                "platform": "linux/arm64",
                "cwd": "/tmp",
                "internet": False,
                "submit_paths": ["candidate.json"],
                "submit_exclude": [],
                "work": {"image_tag": "fixture", "agent_query": "Synthetic fixture"},
                "judge": {
                    "image_tag": "fixture",
                    "eval_timeout": 15,
                    "parser": "structured_json",
                    "selection": "score_first",
                    "score_direction": "maximize",
                    "eval_cmd": 'python3 -c \'import json; print(json.dumps(dict(score=json.load(open("candidate.json"))["score"], valid=True)))\'',
                },
            }
        )
    )


@pytest.fixture
def service(tmp_path, monkeypatch):
    task_files(tmp_path / "tasks")
    backend = Mock(backend_name="docker")
    backend.client.images.get.return_value.id = "sha256:synthetic-image"
    monkeypatch.setattr(native, "create_backend_from_config", lambda config: backend)

    def grade(**kwargs):
        folder = kwargs["log_dir"]
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "submission.tar.gz").write_bytes(kwargs["archive"])
        report = {
            "score": float(kwargs["archive"]),
            "pass_rate": 1.0,
            "passed": 1,
            "failed": 0,
            "total_tests": 1,
            "valid": True,
            "summary": "synthetic",
        }
        return SimpleNamespace(to_dict=lambda: report)

    monkeypatch.setattr(native, "judge_submission", grade)
    config = SForgeConfig(
        tasks_dir=tmp_path / "tasks",
        log_dir=tmp_path / "logs",
        judge_cpu_limit=1,
        judge_mem_limit="256m",
    )
    app = native.create_app(config, checkpoints=True)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield app.state.judge, client
    app.state.judge._reaper_stop.set()
    app.state.judge._reaper_thread.join(timeout=1)


def register(state, client, run="source", **options):
    response = client.post(
        "/api/v1/register",
        json={
            "task_id": "fixture",
            "run_id": run,
            "admin_secret": state.admin_secret,
            **options,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["token"]


def submit(client, token, score):
    response = client.post(
        "/api/v1/submit",
        data={"token": token},
        files={"archive": ("candidate.tar.gz", str(score).encode())},
    )
    if response.status_code != 200:
        return response
    identifier = response.json()["submission_id"]
    until = time.monotonic() + 5
    while time.monotonic() < until:
        result = client.get(
            "/api/v1/result/" + identifier, params={"token": token},
        )
        if result.json()["status"] in ("completed", "error"):
            return response
        time.sleep(0.005)
    raise AssertionError("Synthetic native grade did not settle")


def capture(state, client, token):
    response = client.post(
        "/api/v1/checkpoint/capture",
        json={"token": token, "admin_secret": state.admin_secret},
    )
    assert response.status_code == 200, response.text
    return response.json()


def restore(state, client, point, run="branch", **options):
    return client.post(
        "/api/v1/checkpoint/restore",
        json={
            "checkpoint_id": point["checkpoint_id"],
            "manifest_sha256": point["manifest_sha256"],
            "run_id": run,
            "admin_secret": state.admin_secret,
            **options,
        },
    )


def history(client, token):
    return client.get("/api/v1/history", params={"token": token}).json()


def restore_result(state, client, point, run="branch", **options):
    return client.post(
        "/api/v1/checkpoint/restore-result",
        json={
            "checkpoint_id": point["checkpoint_id"],
            "manifest_sha256": point["manifest_sha256"],
            "run_id": run,
            "admin_secret": state.admin_secret,
            **options,
        },
    )


def test_restore_result_recovers_existing_authority_without_replay(
    service, monkeypatch
):
    state, client = service
    token = register(state, client, max_agent_submissions=2, submission_cooldown=3600)
    submit(client, token, 7)
    point = capture(state, client, token)
    accepted = restore(state, client, point)
    assert accepted.status_code == 200
    original = accepted.json()
    before = copy.deepcopy((state.tokens, state.run_history, state.submissions))

    # An original response may be lost after publication. Readback cannot
    # prepare another backend, reanchor cooldown or issue another token.
    def no_prepare(*args, **kwargs):
        raise AssertionError("Readback must not allocate registration authority")

    monkeypatch.setattr(state, "_prepare_session", no_prepare)
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(lambda _: restore_result(state, client, point), range(8))
        )
    assert all(r.status_code == 200 and r.json() == original for r in responses)
    assert (state.tokens, state.run_history, state.submissions) == before
    assert submit(client, original["token"], 9).status_code == 429
    assert restore(state, client, point).status_code == 409


@pytest.mark.parametrize(
    "fault",
    [
        "wrong_admin",
        "wrong_point",
        "wrong_digest",
        "cold_run",
        "missing_run",
        "missing_marker",
        "wrong_origin",
        "lost_authority",
        "malformed_marker",
        "linked_marker",
    ],
)
def test_restore_result_rejects_unbound_or_lost_authority(service, fault):
    state, client = service
    token = register(state, client)
    point = capture(state, client, token)
    fresh = restore(state, client, point).json()["token"]
    args = {}
    marker = state.config.log_dir / "runs/branch/checkpoint-origin.json"
    if fault == "wrong_admin":
        args["admin_secret"] = token
    elif fault == "wrong_point":
        args["checkpoint_id"] = "0" * 32
    elif fault == "wrong_digest":
        args["manifest_sha256"] = "0" * 64
    elif fault == "cold_run":
        args["run"] = "source"
    elif fault == "missing_run":
        args["run"] = "never-admitted"
    elif fault == "missing_marker":
        marker.unlink()
    elif fault == "wrong_origin":
        value = json.loads(marker.read_bytes())
        value["task_id"] = "another-task"
        marker.write_text(json.dumps(value))
    elif fault == "malformed_marker":
        marker.write_text("[]")
    elif fault == "linked_marker":
        saved = marker.with_suffix(".saved")
        marker.rename(saved)
        marker.symlink_to(saved)
    else:
        # A private origin file survives a server restart, but grants no token.
        state.tokens.pop(fresh)
    before = copy.deepcopy((state.tokens, state.run_history, state.submissions))
    response = restore_result(state, client, point, **args)
    assert response.status_code == (403 if fault == "wrong_admin" else 409)
    assert "token" not in response.json()
    assert (state.tokens, state.run_history, state.submissions) == before


def test_restore_result_after_continuation_preserves_exhaustion(service):
    state, client = service
    source = register(state, client, max_agent_submissions=2, submission_cooldown=0)
    submit(client, source, 7)
    point = capture(state, client, source)
    original = restore(state, client, point).json()
    fresh = original["token"]
    assert submit(client, fresh, 3).status_code == 200
    before = copy.deepcopy((state.tokens, state.run_history, state.submissions))
    recovered = restore_result(state, client, point)
    assert recovered.status_code == 200 and recovered.json() == original
    assert (state.tokens, state.run_history, state.submissions) == before
    assert history(client, fresh)["agent_submissions"] == 2
    assert history(client, fresh)["best_score"] == 7
    assert submit(client, fresh, 99).status_code == 429


def test_restore_result_does_not_recreate_authority_in_restarted_service(service):
    state, client = service
    source = register(state, client)
    point = capture(state, client, source)
    assert restore(state, client, point).status_code == 200
    app = native.create_app(state.config, checkpoints=True)
    restarted = app.state.judge
    try:
        with TestClient(app) as receiver:
            assert restore_result(restarted, receiver, point).status_code == 409
            assert restarted.tokens == {} and restarted.run_history == {}
            assert restore(restarted, receiver, point).status_code == 409
        assert (state.config.log_dir / "runs/branch/checkpoint-origin.json").is_file()
    finally:
        restarted._reaper_stop.set()
        restarted._reaper_thread.join(timeout=1)


def test_preserves_old_selected_archive_and_fresh_result_identity(service):
    state, client = service
    token = register(state, client, max_agent_submissions=3, submission_cooldown=0)
    submit(client, token, 10)
    submit(client, token, 3)
    before = copy.deepcopy((state.tokens, state.run_history, state.submissions))
    point = capture(state, client, token)
    assert point["incumbent"]["round"] == "agent-1"
    response = restore(state, client, point)
    assert response.status_code == 200, response.text
    new_token = response.json()["token"]
    assert new_token != token
    old, new = history(client, token), history(client, new_token)
    assert (new["best_round"], new["best_score"], new["agent_submissions"]) == (
        "agent-1",
        10,
        2,
    )
    assert {e["submission_id"] for e in old["entries"]}.isdisjoint(
        e["submission_id"] for e in new["entries"]
    )
    for e in new["entries"]:
        assert client.get(
            "/api/v1/result/" + e["submission_id"], params={"token": new_token},
        ).json()["report"][
            "score"
        ] in (10, 3)
    archive = (
        state.config.log_dir
        / "runs/branch/fixture/submissions/agent-1/submission.tar.gz"
    )
    assert archive.read_bytes() == b"10"
    assert state.tokens[token] == before[0][token]
    assert state.run_history["source/fixture"] == before[1]["source/fixture"]
    for sid in before[2]:
        assert state.submissions[sid] == before[2][sid]
    assert len(state.submissions) == 4  # Retained results, zero implicit grades.
    third = submit(client, new_token, 12)
    assert (
        third.json()["round_id"] == "agent-3"
        and third.json()["remaining_submissions"] == 0
    )
    assert history(client, new_token)["best_round"] == "agent-3"
    assert submit(client, new_token, 20).status_code == 429
    # The history is unchanged; availability is a newly sampled server snapshot.
    current = history(client, token)
    current_observed = current["agent_submission_availability"].pop("observed_at")
    previous_observed = old["agent_submission_availability"].pop("observed_at")
    assert current_observed >= previous_observed
    assert current == old


def test_offline_wait_does_not_refill_cooldown_or_budget(service, monkeypatch):
    state, client = service
    now = [10000.0]
    monkeypatch.setattr(time, "time", lambda: now[0])
    token = register(state, client, max_agent_submissions=2, submission_cooldown=3600)
    submit(client, token, 10)
    now[0] += 600
    point = capture(state, client, token)
    assert point["opportunity"]["remaining_cooldown_seconds"] == 3000
    now[0] += 50000
    response = restore(state, client, point)
    assert response.status_code == 200, response.text
    fresh = response.json()["token"]
    assert submit(client, fresh, 11).status_code == 429
    now[0] += 2999
    assert submit(client, fresh, 11).status_code == 429
    now[0] += 1
    assert submit(client, fresh, 11).json()["round_id"] == "agent-2"
    now[0] += 3600
    assert submit(client, fresh, 12).status_code == 429


@pytest.mark.parametrize("legacy", [False, True])
def test_restore_preserves_each_archive_receipt_with_new_lookup_ids(service, legacy):
    import hashlib

    state, client = service
    token = register(state, client, max_agent_submissions=3)
    original = [submit(client, token, score).json()["submission_id"] for score in (3, 5)]
    if legacy:
        for sid in original:
            state.submissions[sid].pop("archive_sha256")
            state.submissions[sid].pop("archive_bytes")
    point = capture(state, client, token)
    response = restore(state, client, point)
    assert response.status_code == 200, response.text
    fresh = response.json()["token"]
    entries = client.get("/api/v1/history", params={"token": fresh}).json()["entries"]
    assert len(entries) == 2
    for score, entry, old in zip((3, 5), entries, original):
        assert entry["submission_id"] != old
        source = client.get("/api/v1/result/" + old,
            params={"include_receipt": True, "token": token}).json()
        recovered = client.get("/api/v1/result/" + entry["submission_id"],
            params={"include_receipt": True, "token": fresh}).json()
        assert recovered["report"] == source["report"]
        assert recovered["receipt"] == source["receipt"]
        receipt = recovered["receipt"]
        assert receipt["archive_sha256"] == (None if legacy else
            hashlib.sha256(str(score).encode()).hexdigest())
        assert receipt["archive_bytes"] == (None if legacy else 1)
        assert receipt["round_id"] == entry["round"]
    assert state.resolve_token(fresh)["next_agent"] == 3


def test_pending_allocation_and_duplicate_history_rejected(service):
    state, client = service
    token = register(state, client)
    state.consume_round(token)
    assert (
        client.post(
            "/api/v1/checkpoint/capture",
            json={"token": token, "admin_secret": state.admin_secret},
        ).status_code
        == 409
    )
    assert not (state.config.log_dir / "checkpoints").exists()


def test_failed_grade_is_released_as_error_and_preserved(service, monkeypatch):
    state, client = service
    token = register(state, client, max_agent_submissions=1)

    def failed(**kwargs):
        raise RuntimeError("Synthetic grade failure")

    monkeypatch.setattr(native, "judge_submission", failed)
    submit(client, token, 10)
    assert history(client, token)["entries"][0]["status"] == "error"
    point = capture(state, client, token)
    assert point["incumbent"] is None
    new = restore(state, client, point).json()["token"]
    entry = history(client, new)["entries"][0]
    assert entry["status"] == "error" and "score" not in entry
    assert (
        client.get(
            "/api/v1/result/" + entry["submission_id"], params={"token": new},
        ).json()["status"]
        == "error"
    )
    assert submit(client, new, 10).status_code == 429


@pytest.mark.parametrize(
    "change",
    [
        "source",
        "task",
        "image",
        "pending",
        "counter",
        "selection",
        "result",
        "bool_counter",
    ],
)
def test_incompatible_capsules_fail_without_publication(service, change):
    state, client = service
    token = register(state, client, max_agent_submissions=3)
    submit(client, token, 10)
    point = capture(state, client, token)
    path = (
        state.config.log_dir / "checkpoints" / point["checkpoint_id"] / "manifest.json"
    )
    packet = json.loads(path.read_bytes())
    if change == "source":
        packet["evaluator"]["native_source_sha256"] = "wrong"
    elif change == "task":
        packet["evaluator"]["task_spec_sha256"] = "wrong"
    elif change == "image":
        packet["evaluator"]["judge_image_id"] = "different"
    elif change == "pending":
        packet["history"][0]["status"] = "running"
    elif change == "counter":
        packet["opportunity"]["next_agent"] += 1
    elif change == "bool_counter":
        packet["opportunity"]["next_auto"] = True
    elif change == "selection":
        packet["incumbent"]["round"] = "agent-2"
    elif change == "result":
        next(iter(packet["results"].values()))["report"]["score"] = 20
    raw = _encode(packet)
    path.write_bytes(raw)
    point["manifest_sha256"] = _sha(raw)
    path.with_name("manifest.sha256").write_text(_sha(raw))
    before = copy.deepcopy(
        (state.tokens, state.run_history, state.submissions, state.run_resource_limits)
    )
    assert restore(state, client, point).status_code == 409
    assert (
        state.tokens,
        state.run_history,
        state.submissions,
        state.run_resource_limits,
    ) == before
    assert not (state.config.log_dir / "runs/branch").exists()


@pytest.mark.parametrize(
    "change", ["archive", "manifest", "symlink", "auth", "alias", "override"]
)
def test_bad_bytes_authority_alias_and_resource_override(service, change):
    state, client = service
    token = register(state, client)
    submit(client, token, 10)
    point = capture(state, client, token)
    base = state.config.log_dir / "checkpoints" / point["checkpoint_id"]
    options = {}
    if change == "archive":
        (base / "incumbent.tar.gz").write_bytes(b"20")
    elif change == "manifest":
        (base / "manifest.json").write_bytes(b"{}")
    elif change == "symlink":
        raw = (base / "incumbent.tar.gz").read_bytes()
        (base / "incumbent.tar.gz").unlink()
        outside = state.config.log_dir / "outside.tar.gz"
        outside.write_bytes(raw)
        (base / "incumbent.tar.gz").symlink_to(outside)
    elif change == "auth":
        options["admin_secret"] = token
    elif change == "alias":
        options["run_id"] = "source"
    elif change == "override":
        options["judge_cpu_limit"] = 9
    result = restore(state, client, point, **options)
    assert result.status_code == (
        403 if change == "auth" else 422 if change == "override" else 409
    )
    assert len(state.tokens) == 1 and len(state.submissions) == 1
    assert not (state.config.log_dir / "runs/branch").exists()


def test_import_is_single_publication_and_rollback_retries(service, monkeypatch):
    state, client = service
    token = register(state, client)
    submit(client, token, 10)
    point = capture(state, client, token)
    original = state._publish_session

    def interrupted(*args):
        original(*args)
        raise RuntimeError("Synthetic interruption after publication")

    with monkeypatch.context() as patch:
        patch.setattr(state, "_publish_session", interrupted)
        assert restore(state, client, point).status_code == 500
    assert len(state.tokens) == 1 and len(state.submissions) == 1
    assert set(state.run_resource_limits) == {"source/fixture"}
    assert not (state.config.log_dir / "runs/branch").exists()
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: restore(state, client, point), range(2)))
    assert sorted(r.status_code for r in responses) == [200, 409]
    assert len(state.tokens) == 2
    assert restore(state, client, point).status_code == 409
    assert (
        client.post(
            "/api/v1/register",
            json={
                "task_id": "fixture",
                "run_id": "branch",
                "admin_secret": state.admin_secret,
            },
        ).status_code
        == 404
    )
    # A crash before token publication leaves a destination reservation. Refuse
    # it on retry instead of granting the original opportunities a second time.
    orphan = state.config.log_dir / "runs/interrupted"
    orphan.mkdir()
    assert restore(state, client, point, run="interrupted").status_code == 409
    assert orphan.exists()


def test_default_off_and_cold_registration_contract(service):
    state, client = service
    app = native.create_app(state.config)
    with TestClient(app) as off:
        assert off.post("/api/v1/checkpoint/capture", json={}).status_code == 404
        assert off.post("/api/v1/checkpoint/restore-result", json={}).status_code == 404
        a = register(app.state.judge, off, run="cold", max_agent_submissions=2)
        b = register(app.state.judge, off, run="cold", max_agent_submissions=2)
        assert a != b
        assert (
            app.state.judge.consume_round(a)[2]
            == app.state.judge.consume_round(b)[2]
            == "agent-1"
        )
    assert not (state.config.log_dir / "checkpoints").exists()
    app.state.judge._reaper_stop.set()
    app.state.judge._reaper_thread.join(timeout=1)


def test_backend_preparation_failure_never_publishes_token(service, monkeypatch):
    state, client = service

    def failed(*args, **kwargs):
        raise RuntimeError("Synthetic backend preparation failure")

    monkeypatch.setattr("sforge.harness.backend.factory.create_backend", failed)
    result = client.post(
        "/api/v1/register",
        json={
            "task_id": "fixture",
            "run_id": "failed",
            "backend": "e2b",
            "admin_secret": state.admin_secret,
        },
    )
    assert result.status_code == 500
    assert not state.tokens and not state.run_resource_limits and not state.run_backends


def test_new_controller_imports_bank_with_its_own_authority(service):
    import shutil
    from dataclasses import replace

    state, client = service
    token = register(state, client, max_agent_submissions=2)
    submit(client, token, 10)
    point = capture(state, client, token)
    bank = state.config.log_dir / "checkpoints"
    manifest = (bank / point["checkpoint_id"] / "manifest.json").read_bytes()
    assert (
        token.encode() not in manifest and state.admin_secret.encode() not in manifest
    )
    config = replace(
        state.config, log_dir=state.config.log_dir.parent / "new-controller"
    )
    config.log_dir.mkdir()
    shutil.copytree(bank, config.log_dir / "checkpoints")
    app = native.create_app(config, checkpoints=True)
    try:
        fresh_state = app.state.judge
        assert fresh_state.admin_secret != state.admin_secret and not fresh_state.tokens
        with TestClient(app) as destination:
            assert (
                restore(
                    fresh_state, destination, point, admin_secret=state.admin_secret
                ).status_code
                == 403
            )
            response = restore(fresh_state, destination, point)
            assert response.status_code == 200, response.text
            fresh = response.json()["token"]
            assert history(destination, fresh)["best_score"] == 10
            assert submit(destination, fresh, 11).json()["round_id"] == "agent-2"
            assert submit(destination, fresh, 12).status_code == 429
            # A second imported branch is intentional and needs the new admin
            # to explicitly name another fresh run, rather than replaying one.
            assert (
                restore(fresh_state, destination, point, run="other-branch").status_code
                == 200
            )
            assert history(destination, fresh)["agent_submissions"] == 2
    finally:
        app.state.judge._reaper_stop.set()
        app.state.judge._reaper_thread.join(timeout=1)


def test_capture_retry_retains_original_point_and_rejects_another_actor(service):
    state, client = service
    token = register(state, client, max_agent_submissions=3)
    submit(client, token, 10)
    identifier = "1" * 32
    body = {
        "token": token,
        "admin_secret": state.admin_secret,
        "capture_id": identifier,
    }
    first = client.post("/api/v1/checkpoint/capture", json=body)
    assert first.status_code == 200
    submit(client, token, 11)
    replay = client.post("/api/v1/checkpoint/capture", json=body)
    assert replay.json() == first.json()
    assert len(list((state.config.log_dir / "checkpoints").iterdir())) == 1
    new = restore(state, client, first.json()).json()["token"]
    assert state.resolve_token(new)["next_agent"] == 2
    assert history(client, new)["best_score"] == 10  # Later source gain is absent.
    other = register(state, client, run="other-source")
    assert (
        client.post(
            "/api/v1/checkpoint/capture", json={**body, "token": other}
        ).status_code
        == 409
    )


def test_duplicate_round_history_and_resource_default_drift_rejected(service):
    state, client = service
    token = register(state, client)
    submit(client, token, 10)
    with state._history_lock:
        state.run_history["source/fixture"].append(
            copy.deepcopy(state.run_history["source/fixture"][0])
        )
    assert (
        client.post(
            "/api/v1/checkpoint/capture",
            json={"token": token, "admin_secret": state.admin_secret},
        ).status_code
        == 409
    )
    state.run_history["source/fixture"].pop()
    point = capture(state, client, token)
    path = (
        state.config.log_dir / "checkpoints" / point["checkpoint_id"] / "manifest.json"
    )
    packet = json.loads(path.read_bytes())
    packet["resources"]["judge_cpu_limit"] = None
    path.write_bytes(_encode(packet))
    point["manifest_sha256"] = _sha(path.read_bytes())
    path.with_name("manifest.sha256").write_text(point["manifest_sha256"])
    assert restore(state, client, point).status_code == 409
    assert not (state.config.log_dir / "runs/branch").exists()


@pytest.mark.parametrize(
    "policy,direction,best_round,best_score",
    [
        ("score_first", "maximize", "agent-1", 10),
        ("score_first", "minimize", "agent-2", 3),
        ("pass_rate_first", "maximize", "agent-1", 10),
        ("valid_then_score", "maximize", "agent-1", 10),
    ],
)
def test_preserves_native_policy_direction_and_round(
    policy, direction, best_round, best_score, service
):
    state, client = service
    state.tasks["fixture"].judge.selection = policy
    state.tasks["fixture"].judge.score_direction = direction
    token = register(state, client)
    submit(client, token, 10)
    submit(client, token, 3)
    point = capture(state, client, token)
    assert point["incumbent"]["round"] == best_round
    response = restore(state, client, point)
    assert response.status_code == 200
    result = history(client, response.json()["token"])
    assert (result["best_round"], result["best_score"]) == (best_round, best_score)
    assert (
        state.resolve_token(response.json()["token"])["registration_id"]
        != state.resolve_token(token)["registration_id"]
    )


def test_wrong_unicode_admin_is_a_permission_denial(service):
    state, client = service
    token = register(state, client)
    response = client.post(
        "/api/v1/checkpoint/capture",
        json={"token": token, "admin_secret": "错误管理权限"},
    )
    assert (
        response.status_code == 403
        and not (state.config.log_dir / "checkpoints").exists()
    )


def test_capture_retry_rejects_damaged_original_capsule(service):
    state, client = service
    token = register(state, client, submission_cooldown=3600)
    submit(client, token, 10)
    body = {"token": token, "admin_secret": state.admin_secret, "capture_id": "2" * 32}
    first = client.post("/api/v1/checkpoint/capture", json=body)
    assert first.status_code == 200
    path = state.config.log_dir / "checkpoints" / ("2" * 32) / "manifest.json"
    packet = json.loads(path.read_bytes())
    packet["opportunity"]["remaining_cooldown_seconds"] = 0
    path.write_bytes(_encode(packet))
    assert client.post("/api/v1/checkpoint/capture", json=body).status_code == 409
    assert len(list(path.parent.parent.iterdir())) == 1


def test_unrepresentable_cooldown_anchor_fails_closed(service, monkeypatch):
    state, client = service
    # At this controlled positive epoch, float spacing at 10**20 erases
    # the timestamp entirely. Current wall-clock rounding is not deterministic.
    monkeypatch.setattr(time, "time", lambda: 1000.0)
    token = register(state, client, max_agent_submissions=2, submission_cooldown=10**20)
    submit(client, token, 10)
    point = capture(state, client, token)
    response = restore(state, client, point)
    assert response.status_code == 409
    assert "destination clock" in response.json()["detail"]
    assert len(state.tokens) == 1
    assert not (state.config.log_dir / "runs/branch").exists()
    assert submit(client, token, 11).status_code == 429


def test_restore_materialization_does_not_spend_remaining_cooldown(
    service, monkeypatch
):
    from sforge.harness import checkpoint

    state, client = service
    now = [10000.0]
    monkeypatch.setattr(time, "time", lambda: now[0])
    token = register(state, client, max_agent_submissions=2, submission_cooldown=3600)
    submit(client, token, 10)
    now[0] += 600
    point = capture(state, client, token)
    assert point["opportunity"]["remaining_cooldown_seconds"] == 3000
    original_sync = checkpoint._sync_directory

    def slow_materialization(path):
        original_sync(path)
        now[0] += 3600

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint, "_sync_directory", slow_materialization)
        response = restore(state, client, point)
    assert response.status_code == 200, response.text
    fresh = response.json()["token"]
    assert submit(client, fresh, 11).status_code == 429
    now[0] += 2999
    assert submit(client, fresh, 11).status_code == 429
    now[0] += 1
    assert submit(client, fresh, 11).json()["round_id"] == "agent-2"
    assert submit(client, fresh, 12).status_code == 429


@pytest.mark.parametrize(
    "score,direction,next_score", [(float("inf"), "minimize", 6),
                                  (-float("inf"), "maximize", 8),
                                  (float("nan"), "maximize", 8)],
)
def test_nonfinite_result_checkpoint_preserves_history_and_opportunities(
    service, monkeypatch, score, direction, next_score,
):
    state, client = service
    state.tasks["fixture"].judge.score_direction = direction
    now = [10000.0]
    monkeypatch.setattr(time, "time", lambda: now[0])
    original_grade = native.judge_submission

    def grade(**kwargs):
        report = original_grade(**kwargs).to_dict()
        report["metrics"] = {
            "bounds": [float("inf"), -float("inf"), float("nan"), None],
            "literal": {"kind": "nan", "path": ["score"], "value": None},
            "text": "Infinity",
        }
        return SimpleNamespace(to_dict=lambda: report)

    token = register(state, client, max_agent_submissions=3, submission_cooldown=30)
    first = submit(client, token, 7)
    now[0] += 30
    monkeypatch.setattr(native, "judge_submission", grade)
    second = submit(client, token, score)
    assert first.status_code == second.status_code == 200
    source = copy.deepcopy((state.tokens, state.run_history, state.submissions))
    source_result = client.get("/api/v1/result/" + second.json()["submission_id"],
                               params={"token": token}).json()

    point = capture(state, client, token)
    assert point["incumbent"]["round"] == "agent-1"
    assert point["opportunity"]["next_agent"] == 3
    assert point["opportunity"]["remaining_cooldown_seconds"] == 30
    assert (state.tokens, state.run_history, state.submissions) == source
    manifest_path = state.config.log_dir / "checkpoints" / point["checkpoint_id"] / "manifest.json"

    def reject_constant(value):
        raise AssertionError("Checkpoint manifest contains a non-JSON constant: " + value)

    manifest = json.loads(manifest_path.read_bytes(), parse_constant=reject_constant)
    assert manifest["incumbent"]["round"] == "agent-1"
    now[0] += 600  # Offline time cannot consume a retained cooldown.
    response = restore(state, client, point)
    assert response.status_code == 200, response.text
    fresh = response.json()["token"]
    retained = state.run_history["branch/fixture"]
    assert len(retained) == 2 and retained[1]["round"] == "agent-2"
    restored_score = retained[1]["score"]
    assert math.isnan(restored_score) if math.isnan(score) else restored_score == score
    restored_report = state.submissions[retained[1]["submission_id"]]["report"]
    bounds = restored_report["metrics"]["bounds"]
    assert bounds[:2] == [float("inf"), -float("inf")]
    assert math.isnan(bounds[2]) and bounds[3] is None
    assert restored_report["metrics"]["literal"] == {
        "kind": "nan", "path": ["score"], "value": None,
    }
    # Ordinary native report files keep their established json.loads format;
    # the checkpoint's private strict-JSON representation does not leak here.
    report_file = state.config.log_dir / "runs/branch/fixture/submissions/agent-2/report.json"
    persisted = json.loads(report_file.read_bytes())
    assert math.isnan(persisted["score"]) if math.isnan(score) else persisted["score"] == score
    assert persisted["metrics"]["bounds"][:2] == bounds[:2]
    assert math.isnan(persisted["metrics"]["bounds"][2])
    restored_result = client.get("/api/v1/result/" + retained[1]["submission_id"],
                                 params={"token": fresh}).json()
    assert restored_result["report"] == source_result["report"]
    assert history(client, fresh)["best_score"] == 7
    recaptured = capture(state, client, fresh)
    assert recaptured["incumbent"] == point["incumbent"]
    assert recaptured["opportunity"]["next_agent"] == 3
    assert submit(client, fresh, next_score).status_code == 429
    now[0] += 29
    assert submit(client, fresh, next_score).status_code == 429
    now[0] += 1
    assert submit(client, fresh, next_score).json()["round_id"] == "agent-3"
    assert history(client, fresh)["best_score"] == next_score
    assert submit(client, fresh, next_score).status_code == 429


def test_manifest_codec_preserves_literal_objects_and_finite_v1_bytes():
    literal = {"schema_version": "sforge_judge_checkpoint_v2", "nonfinite_values": [
        {"kind": "nan", "path": ["score"], "value": None},
    ]}
    packet = {
        "schema_version": "sforge_judge_checkpoint_v1",
        "history": [{"score": 7.0}],
        "results": {"synthetic": {"report": {"metrics": {
            "literal": literal, "0": "Infinity", "finite": [None, True, 0, 1.5],
        }}}},
    }
    finite_bytes = _encode(packet)
    assert _encode_manifest(packet) == finite_bytes
    assert _decode_manifest(finite_bytes) == packet
    packet["results"]["synthetic"]["report"]["metrics"]["numbers"] = [
        float("inf"), -float("inf"), float("nan"),
    ]
    before = copy.deepcopy(packet)
    encoded = _encode_manifest(packet)
    wire = json.loads(encoded, parse_constant=lambda value: pytest.fail(value))
    assert wire["schema_version"] == "sforge_judge_checkpoint_v2"
    assert packet == before  # Encoding cannot alter live grading/selection facts.
    decoded = _decode_manifest(encoded)
    metrics = decoded["results"]["synthetic"]["report"]["metrics"]
    assert metrics["literal"] == literal and metrics["0"] == "Infinity"
    assert metrics["finite"] == [None, True, 0, 1.5]
    assert metrics["numbers"][:2] == [float("inf"), -float("inf")]
    assert math.isnan(metrics["numbers"][2])
    assert _encode_manifest(decoded) == encoded


@pytest.mark.parametrize("key", [0, 1.25, False, None])
def test_finite_v1_preserves_existing_json_object_key_encoding(key):
    packet = {"schema_version": "sforge_judge_checkpoint_v1", "history": [],
              "results": {"synthetic": {"report": {"metrics": {key: "literal"}}}}}
    assert _encode_manifest(packet) == _encode(packet)


def test_nonfinite_paths_follow_json_object_keys_without_mutating_source():
    metrics = {1: float("inf"), 2: float("nan"), 3: "Infinity"}
    packet = {"schema_version": "sforge_judge_checkpoint_v1", "history": [],
              "results": {"synthetic": {"report": {"metrics": metrics}}}}
    encoded = _encode_manifest(packet)
    restored = _decode_manifest(encoded)["results"]["synthetic"]["report"]["metrics"]
    assert restored["1"] == float("inf") and math.isnan(restored["2"])
    assert restored["3"] == "Infinity" and set(metrics) == {1, 2, 3}
    packet["results"]["synthetic"]["report"]["metrics"] = {1: float("inf"), "1": None}
    with pytest.raises(CheckpointUnavailable, match="collide"):
        _encode_manifest(packet)


@pytest.mark.parametrize("fault", [
    "unknown_schema", "v1_sidecar", "missing_sidecar", "empty_sidecar",
    "extra_record_key", "unknown_kind", "duplicate_path", "missing_path",
    "non_null_target", "authority_path", "boolean_index", "negative_index",
    "string_index", "empty_path",
])
def test_manifest_codec_rejects_ambiguous_nonfinite_paths(fault):
    wire = json.loads(_encode_manifest({
        "schema_version": "sforge_judge_checkpoint_v1", "captured_at_epoch": None,
        "history": [{"score": float("inf"), "passed": 1}], "results": {},
    }))
    entry = wire["nonfinite_values"][0]
    if fault == "unknown_schema":
        wire["schema_version"] = "sforge_judge_checkpoint_v99"
    elif fault == "v1_sidecar":
        wire["schema_version"] = "sforge_judge_checkpoint_v1"
    elif fault == "missing_sidecar":
        del wire["nonfinite_values"]
    elif fault == "empty_sidecar":
        wire["nonfinite_values"] = []
    elif fault == "extra_record_key":
        entry["value"] = "Infinity"
    elif fault == "unknown_kind":
        entry["kind"] = "infinite"
    elif fault == "duplicate_path":
        wire["nonfinite_values"].append(copy.deepcopy(entry))
    elif fault == "missing_path":
        entry["path"][-1] = "absent"
    elif fault == "non_null_target":
        entry["path"][-1] = "passed"
    elif fault == "authority_path":
        entry["path"] = ["captured_at_epoch"]
    elif fault == "boolean_index":
        entry["path"][1] = False
    elif fault == "negative_index":
        entry["path"][1] = -1
    elif fault == "string_index":
        entry["path"][1] = "0"
    else:
        entry["path"] = []
    with pytest.raises(CheckpointUnavailable):
        _decode_manifest(_encode(wire))


@pytest.mark.parametrize("raw", [
    b'{"schema_version":"sforge_judge_checkpoint_v1","history":[NaN]}',
    b'{"schema_version":"sforge_judge_checkpoint_v1","history":[Infinity]}',
    b'{"schema_version":"sforge_judge_checkpoint_v1","history":[-Infinity]}',
    b'{"schema_version":"sforge_judge_checkpoint_v1","history":[1e999]}',
    b'{"schema_version":"sforge_judge_checkpoint_v1","history":[],"history":[]}',
])
def test_manifest_codec_rejects_unmarked_numbers_and_duplicate_keys(raw):
    with pytest.raises(CheckpointUnavailable):
        _decode_manifest(raw)


@pytest.mark.parametrize("score", [float("inf"), -float("inf"), float("nan")])
def test_all_nonfinite_history_and_capture_retry_retain_native_selection(service, score):
    state, client = service
    token = register(state, client, max_agent_submissions=2)
    submitted = submit(client, token, score)
    assert submitted.status_code == 200
    body = {"token": token, "admin_secret": state.admin_secret, "capture_id": "4" * 32}
    first = client.post("/api/v1/checkpoint/capture", json=body)
    assert first.status_code == 200, first.text
    point = first.json()
    assert point["incumbent"]["round"] == "agent-1"
    # A new source submission must not change a retry's original history.
    assert submit(client, token, 8).status_code == 200
    assert client.post("/api/v1/checkpoint/capture", json=body).json() == point
    restored = restore(state, client, point)
    assert restored.status_code == 200, restored.text
    fresh = restored.json()["token"]
    assert state.resolve_token(fresh)["next_agent"] == 2
    retained = state.run_history["branch/fixture"]
    assert len(retained) == 1 and history(client, fresh)["best_round"] == "agent-1"
    assert math.isnan(retained[0]["score"]) if math.isnan(score) else retained[0]["score"] == score
    recaptured = capture(state, client, fresh)
    assert recaptured["incumbent"] == point["incumbent"]


@pytest.mark.parametrize("fault", ["history_value", "result_value", "duplicate_path", "unknown_schema"])
def test_nonfinite_manifest_tamper_cannot_publish_restore(service, monkeypatch, fault):
    state, client = service
    token = register(state, client)
    submit(client, token, float("nan"))
    point = capture(state, client, token)
    path = state.config.log_dir / "checkpoints" / point["checkpoint_id"] / "manifest.json"
    wire = json.loads(path.read_bytes())
    if fault in {"history_value", "result_value"}:
        root = "history" if fault == "history_value" else "results"
        entry = next(row for row in wire["nonfinite_values"] if row["path"][0] == root)
        entry["kind"] = "positive_infinity"
    elif fault == "duplicate_path":
        wire["nonfinite_values"].append(copy.deepcopy(wire["nonfinite_values"][0]))
    else:
        wire["schema_version"] = "sforge_judge_checkpoint_v99"
    path.write_bytes(_encode(wire))
    point["manifest_sha256"] = _sha(path.read_bytes())
    path.with_name("manifest.sha256").write_text(point["manifest_sha256"])
    prepare = Mock(side_effect=AssertionError("Invalid capsule cannot prepare authority"))
    monkeypatch.setattr(state, "_prepare_session", prepare)
    response = restore(state, client, point)
    assert response.status_code == 409, response.text
    prepare.assert_not_called()
    assert len(state.tokens) == 1 and len(state.submissions) == 1
    assert not (state.config.log_dir / "runs/branch").exists()
