# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
# SPDX-License-Identifier: Apache-2.0
"""Recovery through actual HTTP/native state; all evaluation data is synthetic."""

from __future__ import annotations

import copy
import hashlib
import json
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from test_judge_checkpoint import capture, history, register, restore, submit
from test_judge_checkpoint import service as _shared_service

from sforge.harness import judge_server as native
from sforge.harness.checkpoint import CheckpointUnavailable

service = _shared_service


def adoption(state, token, prefix, **changes):
    return {
        "checkpoint_id": prefix["checkpoint_id"],
        "manifest_sha256": prefix["manifest_sha256"],
        "source_run_id": state.tokens[token]["run_id"],
        "task_id": "fixture",
        "source_registration_id": state.tokens[token]["registration_id"],
        "capture_id": "a" * 32,
        "admin_secret": state.admin_secret,
        **changes,
    }


def restart(state):
    # Same source files, fresh in-memory native authority. This explicitly
    # closes the source writer rather than pretending a live service is dead.
    state._checkpoint_tail.close()
    app = native.create_app(state.config, checkpoints=True)
    return app


def stop_reaper(state):
    state._reaper_stop.set()
    state._reaper_thread.join(timeout=1)


def test_terminal_tail_preserves_invalid_rounds_and_cooldown(service, monkeypatch):
    state, client = service
    clock = [10_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    actual_grade = native.judge_submission

    def grade(**kwargs):
        report = actual_grade(**kwargs).to_dict()
        if kwargs["archive"] == b"9":
            report.update(
                valid=False, score=None, summary="Synthetic invalid candidate"
            )
        return SimpleNamespace(to_dict=lambda: report)

    monkeypatch.setattr(native, "judge_submission", grade)
    token = register(state, client, max_agent_submissions=12, submission_cooldown=3600)
    for value in range(1, 6):
        assert submit(client, token, value).status_code == 200
        clock[0] += 3600
    prefix = capture(state, client, token)
    old_folder = state.config.log_dir / "checkpoints" / prefix["checkpoint_id"]
    old_hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in old_folder.iterdir()
    }
    for value in range(6, 12):
        assert submit(client, token, value).status_code == 200
        if value < 11:
            clock[0] += 3600
    before = history(client, token)
    assert len(before["entries"]) == 11 and before["entries"][8]["valid"] is False
    body = adoption(state, token, prefix)
    # Writer ownership, not an asserted boolean, prevents stealing a live tail.
    assert (
        client.post("/api/v1/checkpoint/adopt-terminal", json=body).status_code == 409
    )
    last_accepted = state.tokens[token]["last_agent_submit_at"]
    app = restart(state)
    receiver = app.state.judge
    clock[0] += 90_000  # offline waiting never spends retained cooldown
    try:
        with TestClient(app) as fresh:
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert response.status_code == 200, response.text
            point = response.json()
            assert point["opportunity"]["next_agent"] == 12
            assert point["opportunity"]["remaining_cooldown_seconds"] == 3600
            assert (
                fresh.post("/api/v1/checkpoint/adopt-terminal", json=body).json()
                == point
            )
            assert receiver.tokens == {}  # adoption itself creates no credentials
            new_token = restore(receiver, fresh, point, run="new-branch").json()[
                "token"
            ]
            restored = history(fresh, new_token)
            assert restored["agent_submissions"] == 11 and restored["best_score"] == 11
            assert restored["entries"][8]["valid"] is False
            assert {e["submission_id"] for e in restored["entries"]}.isdisjoint(
                e["submission_id"] for e in before["entries"]
            )
            assert submit(fresh, new_token, 12).status_code == 429
            clock[0] += 3600
            assert submit(fresh, new_token, 12).json()["round_id"] == "agent-12"
            clock[0] += 3600
            assert submit(fresh, new_token, 13).status_code == 429
            new_packet = json.loads(
                (
                    state.config.log_dir
                    / "checkpoints"
                    / point["checkpoint_id"]
                    / "manifest.json"
                ).read_bytes()
            )
            assert new_packet["durable_tail"]["last_agent_submit_at"] == last_accepted
            assert (
                new_packet["durable_tail"]["checkpoint_id"] == prefix["checkpoint_id"]
            )
    finally:
        stop_reaper(receiver)
    assert old_hashes == {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in old_folder.iterdir()
    }
    assert token.encode() not in b"".join(
        p.read_bytes()
        for p in (state.config.log_dir / "checkpoint-registrations").rglob("*.json")
    )


def test_tail_of_restored_registration_keeps_original_map(service):
    state, client = service
    token = register(state, client)
    submit(client, token, 7)
    prefix = capture(state, client, token)
    branch = restore(state, client, prefix).json()["token"]
    inherited = history(client, branch)["entries"][0]["submission_id"]
    submit(client, branch, 3)
    body = adoption(state, branch, prefix)
    app = restart(state)
    try:
        with TestClient(app) as fresh:
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert response.status_code == 200, response.text
            point = response.json()
            assert point["incumbent"]["round"] == "agent-1"
            packet = json.loads(
                (
                    state.config.log_dir
                    / "checkpoints"
                    / point["checkpoint_id"]
                    / "manifest.json"
                ).read_bytes()
            )
            assert inherited in packet["durable_tail"]["submission_id_map"].values()
            new = restore(app.state.judge, fresh, point, run="third").json()["token"]
            assert history(fresh, new)["best_score"] == 7
    finally:
        stop_reaper(app.state.judge)


@pytest.mark.parametrize(
    "fault",
    [
        "wrong_admin",
        "wrong_registration",
        "wrong_task",
        "wrong_run",
        "wrong_prefix_digest",
        "pending",
        "reserved_not_accepted",
        "missing_invalid",
        "duplicate_round",
        "reordered_rounds",
        "archive_hash",
        "archive_missing",
        "archive_symlink",
        "wrong_source",
        "wrong_image",
        "wrong_resource",
        "last_acceptance",
        "changed_prefix_result",
        "wrong_reservation",
        "missing_journal",
        "journal_symlink",
        "extra_unsettled_reservation",
    ],
)
def test_terminal_tail_rejects_incomplete_or_mismatched_authority(service, fault):
    state, client = service
    token = register(state, client)
    submit(client, token, 7)
    prefix = capture(state, client, token)
    submit(client, token, 3)
    body = adoption(state, token, prefix)
    folder = state._checkpoint_tail.folder("source", "fixture")
    path = folder / "state.json"
    value = json.loads(path.read_bytes())
    if fault == "wrong_admin":
        body["admin_secret"] = "dummy-denied"
    elif fault == "wrong_registration":
        body["source_registration_id"] = "b" * 32
    elif fault == "wrong_task":
        body["task_id"] = "other"
    elif fault == "wrong_run":
        body["source_run_id"] = "other"
    elif fault == "wrong_prefix_digest":
        body["manifest_sha256"] = "b" * 64
    elif fault == "pending":
        value["results"][value["history"][-1]["submission_id"]]["status"] = "queued"
    elif fault in ("reserved_not_accepted", "extra_unsettled_reservation"):
        value["opportunity"]["next_agent"] += 1
        value["reservations"]["agent-3"] = value["last_agent_submit_at"]
    elif fault == "missing_invalid":
        removed = value["history"].pop()
        value["results"].pop(removed["submission_id"])
    elif fault == "duplicate_round":
        value["history"][-1]["round"] = "agent-1"
    elif fault == "reordered_rounds":
        value["history"].reverse()
    elif fault == "archive_hash":
        (folder / "agent-2.tar.gz").write_bytes(b"other")
    elif fault == "archive_missing":
        (folder / "agent-2.tar.gz").unlink()
    elif fault == "archive_symlink":
        archive = folder / "agent-2.tar.gz"
        archive.rename(folder / "moved")
        archive.symlink_to("moved")
    elif fault == "wrong_source":
        value["evaluator"]["native_source_sha256"] = "b" * 64
    elif fault == "wrong_image":
        value["evaluator"]["judge_image_id"] = "other"
    elif fault == "wrong_resource":
        value["resources"]["judge_cpu_limit"] = 99
    elif fault == "last_acceptance":
        value["last_agent_submit_at"] = 0
    elif fault == "changed_prefix_result":
        value["results"][value["history"][0]["submission_id"]]["report"]["score"] = 88
    elif fault == "wrong_reservation":
        value["reservations"]["agent-2"] = 1
    path.write_text(json.dumps(value))
    if fault == "missing_journal":
        path.unlink()
    elif fault == "journal_symlink":
        path.rename(folder / "saved.json")
        path.symlink_to("saved.json")
    app = restart(state)
    try:
        with TestClient(app) as fresh:
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert response.status_code == (403 if fault == "wrong_admin" else 409), (
                response.text
            )
            assert app.state.judge.tokens == {}
            assert not (
                state.config.log_dir / "checkpoints" / body["capture_id"]
            ).exists()
    finally:
        stop_reaper(app.state.judge)


def test_counter_write_failure_does_not_publish_unrecorded_opportunity(
    service, monkeypatch
):
    state, client = service
    token = register(state, client)
    before = copy.deepcopy(state.tokens[token])

    def fail(*args):
        raise OSError("Synthetic durable write failure")

    monkeypatch.setattr(state._checkpoint_tail, "_save", fail)
    with pytest.raises(OSError):
        state.consume_round(token)
    assert state.tokens[token] == before
    assert state.submissions == {}


def test_closed_writer_and_restarted_cold_registration_cannot_reset_history(service):
    state, client = service
    token = register(state, client)
    submit(client, token, 7)
    app = restart(state)
    try:
        with TestClient(app) as fresh:
            response = fresh.post(
                "/api/v1/register",
                json={
                    "task_id": "fixture",
                    "run_id": "source",
                    "admin_secret": app.state.judge.admin_secret,
                },
            )
            assert response.status_code == 404
            assert app.state.judge.tokens == {}
        with pytest.raises(CheckpointUnavailable):
            state.consume_round(token)
    finally:
        stop_reaper(app.state.judge)


def test_checkpoint_off_keeps_cold_http_flow_and_no_recovery_storage(service):
    state, _ = service
    app = native.create_app(state.config, checkpoints=False)
    off = app.state.judge
    try:
        with TestClient(app) as client:
            token = register(off, client, run="plain", max_agent_submissions=2)
            assert submit(client, token, 7).status_code == 200
            assert history(client, token)["best_score"] == 7
            assert (
                client.post("/api/v1/checkpoint/adopt-terminal", json={}).status_code
                == 404
            )
            assert client.post("/api/v1/checkpoint/capture", json={}).status_code == 404
            assert not hasattr(off, "_checkpoint_tail")
            assert not (off.config.log_dir / "checkpoint-registrations").exists()
    finally:
        stop_reaper(off)


def test_acceptance_disk_failure_leaves_unsettled_reservation(service, monkeypatch):
    state, client = service
    token = register(state, client)
    prefix = capture(state, client, token)
    state.consume_round(token)

    def fail(*args):
        raise OSError("Synthetic accepted archive disk failure")

    with monkeypatch.context() as patch:
        patch.setattr("sforge.harness.checkpoint_tail._write", fail)
        with pytest.raises(OSError):
            state.submit("fixture", b"7", "source", "agent-1")
    body = adoption(state, token, prefix)
    app = restart(state)
    try:
        with TestClient(app) as fresh:
            assert (
                fresh.post("/api/v1/checkpoint/adopt-terminal", json=body).status_code
                == 409
            )
            assert app.state.judge.tokens == {}
    finally:
        stop_reaper(app.state.judge)


def test_terminal_error_is_consumed_and_never_promoted_to_success(service, monkeypatch):
    state, client = service
    token = register(state, client, max_agent_submissions=1)
    prefix = capture(state, client, token)

    def fail(**kwargs):
        raise RuntimeError("Synthetic backend failure")

    monkeypatch.setattr(native, "judge_submission", fail)
    assert submit(client, token, 7).status_code == 200
    body = adoption(state, token, prefix)
    app = restart(state)
    try:
        with TestClient(app) as fresh:
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert response.status_code == 200, response.text
            new = restore(app.state.judge, fresh, response.json()).json()["token"]
            entry = history(fresh, new)["entries"][0]
            assert entry["status"] == "error" and "score" not in entry
            assert submit(fresh, new, 8).status_code == 429
    finally:
        stop_reaper(app.state.judge)


def test_copied_tail_cannot_bypass_live_original_writer_lock(service, tmp_path):
    import shutil
    from dataclasses import replace

    state, client = service
    token = register(state, client)
    prefix = capture(state, client, token)
    submit(client, token, 7)
    body = adoption(state, token, prefix)
    copied = tmp_path / "copied"
    copied.mkdir()
    # No source credentials are transferred, even in this synthetic fixture.
    for name in ("checkpoints", "checkpoint-registrations"):
        shutil.copytree(state.config.log_dir / name, copied / name)
    app = native.create_app(replace(state.config, log_dir=copied), checkpoints=True)
    body["admin_secret"] = app.state.judge.admin_secret
    try:
        with TestClient(app) as fresh:
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert (
                response.status_code == 409 and "original writer lock" in response.text
            )
            assert app.state.judge.tokens == {}
        assert submit(client, token, 8).json()["round_id"] == "agent-2"
    finally:
        stop_reaper(app.state.judge)


def test_fifo_source_is_rejected_without_blocking(tmp_path):
    import os
    import subprocess
    import sys

    fifo = tmp_path / "state.json"
    os.mkfifo(fifo)
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from pathlib import Path
from sforge.harness.checkpoint import _read, CheckpointUnavailable
try:
    _read(Path(sys.argv[1]), Path(sys.argv[2]))
except CheckpointUnavailable:
    raise SystemExit(0)
raise SystemExit(1)
""",
            str(tmp_path),
            str(fifo),
        ],
        timeout=5,
        capture_output=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr


def test_process_death_releases_original_writer_but_not_admission_facts(service):
    import subprocess
    import sys

    state, client = service
    token = register(state, client)
    prefix = capture(state, client, token)
    submit(client, token, 7)
    body = adoption(state, token, prefix)
    folder = state._checkpoint_tail.folder("source", "fixture")
    app = restart(state)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import sys
from pathlib import Path
from sforge.harness.checkpoint_tail import _lock
fd = _lock(Path(sys.argv[1]))
print("locked", flush=True)
sys.stdin.read()
""",
            str(folder / "writer.lock"),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "locked"
        with TestClient(app) as fresh:
            assert (
                fresh.post("/api/v1/checkpoint/adopt-terminal", json=body).status_code
                == 409
            )
            child.kill()
            child.wait(timeout=5)
            response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
            assert response.status_code == 200, response.text
            assert response.json()["opportunity"]["next_agent"] == 2
            assert app.state.judge.tokens == {}
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        stop_reaper(app.state.judge)
