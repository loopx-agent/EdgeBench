# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
# SPDX-License-Identifier: Apache-2.0
"""Durable tails share bounded admission, lifecycle and existing result codecs."""

from __future__ import annotations

import hashlib
import json
import math
import threading
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from test_judge_checkpoint import capture, history, register, restore, submit
from test_judge_checkpoint import service as _shared_service
from test_judge_checkpoint_tail import adoption

from sforge.harness import judge_server as native
from sforge.harness import checkpoint_tail as tail
from sforge.harness.checkpoint import (
    CheckpointUnavailable,
    _decode_manifest,
    _encode,
    _encode_manifest,
)
from sforge.harness.checkpoint_tail import TAIL_NONFINITE_SCHEMA, TAIL_SCHEMA

service = _shared_service


def test_queued_acceptance_precedes_grading_and_graceful_shutdown_adopts(service, monkeypatch):
    state, client = service
    token = register(state, client)
    submit(client, token, 7)
    prefix = capture(state, client, token)
    state._capacity = threading.BoundedSemaphore(1)
    entered = threading.Event()
    released = threading.Event()
    original = native.judge_submission

    def grade(**kwargs):
        folder = state._checkpoint_tail.folder("source", "fixture")
        journal = json.loads((folder / "state.json").read_bytes())
        accepted = journal["accepted_archives"]["agent-2"]
        assert accepted["archive_sha256"] == hashlib.sha256(b"8").hexdigest()
        assert journal["results"][kwargs["submission_id"]]["status"] == "queued"
        assert journal["opportunity"]["next_agent"] == 3
        entered.set()
        assert released.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(native, "judge_submission", grade)
    sid, round_id, _ = state.submit_for_token(token, b"8", "agent")
    assert round_id == "agent-2" and entered.wait(5)
    assert state.get_result(sid)["status"] == "running"
    journal_path = state._checkpoint_tail.folder("source", "fixture") / "state.json"
    before = journal_path.read_bytes()
    with pytest.raises(native.JudgeCapacityExceeded):
        state.submit_for_token(token, b"9", "agent")
    assert state.tokens[token]["next_agent"] == 3
    assert journal_path.read_bytes() == before
    body = adoption(state, token, prefix)
    closing = threading.Thread(target=state.close)
    closing.start()
    try:
        assert not state._checkpoint_tail.closed
    finally:
        released.set()
        closing.join(5)
    assert not closing.is_alive() and state._checkpoint_tail.closed
    app = native.create_app(state.config, checkpoints=True)
    with TestClient(app) as fresh:
        response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
        assert response.status_code == 200, response.text
        new = restore(app.state.judge, fresh, response.json(), run="next").json()["token"]
        assert history(fresh, new)["best_score"] == 8
        assert history(fresh, new)["agent_submissions"] == 2
    assert app.state.judge._checkpoint_tail.closed


def test_acceptance_failure_retains_reservation_but_releases_queue_capacity(service, monkeypatch):
    state, client = service
    token = register(state, client)
    state._capacity = threading.BoundedSemaphore(1)
    original = state._checkpoint_tail.accept
    invoked = []
    monkeypatch.setattr(native, "judge_submission", lambda **kw: invoked.append(kw))

    def fail(*args):
        raise OSError("Synthetic acceptance storage failure")

    monkeypatch.setattr(state._checkpoint_tail, "accept", fail)
    with pytest.raises(OSError, match="Synthetic acceptance"):
        state.submit_for_token(token, b"7", "agent")
    assert invoked == [] and state.submissions == {}
    assert not list((state.config.log_dir / "judge-pending").iterdir())
    assert state._capacity.acquire(blocking=False)
    state._capacity.release()
    assert state.tokens[token]["next_agent"] == 2
    journal = json.loads((state._checkpoint_tail.folder("source", "fixture") / "state.json").read_bytes())
    assert journal["reservations"] == {"agent-1": state.tokens[token]["last_agent_submit_at"]}
    assert journal["accepted_archives"] == {} and journal["history"] == []
    assert client.post("/api/v1/checkpoint/capture", json={
        "token": token, "admin_secret": state.admin_secret,
    }).status_code == 409
    monkeypatch.setattr(state._checkpoint_tail, "accept", original)


@pytest.mark.parametrize("score,direction,improved", [
    (float("inf"), "minimize", 6),
    (-float("inf"), "maximize", 8),
    (float("nan"), "maximize", 8),
])
def test_tail_restart_preserves_nonfinite_prefix_and_released_tail(service, monkeypatch, score, direction, improved):
    state, client = service
    state.tasks["fixture"].judge.score_direction = direction
    original = native.judge_submission

    def grade(**kwargs):
        report = original(**kwargs).to_dict()
        report["metrics"] = {"bounds": [float("nan"), float("inf"), -float("inf")],
                             "literal": "Infinity", "absent": None}
        return SimpleNamespace(to_dict=lambda: report)

    monkeypatch.setattr(native, "judge_submission", grade)
    token = register(state, client, max_agent_submissions=4)
    submit(client, token, 7)
    submit(client, token, score)
    prefix = capture(state, client, token)
    submit(client, token, improved)
    body = adoption(state, token, prefix)
    folder = state._checkpoint_tail.folder("source", "fixture")
    raw = (folder / "state.json").read_bytes()
    wire = json.loads(raw, parse_constant=lambda value: pytest.fail(value))
    assert wire["schema_version"] == TAIL_NONFINITE_SCHEMA
    state.close()
    app = native.create_app(state.config, checkpoints=True)
    app.state.judge.tasks["fixture"].judge.score_direction = direction
    with TestClient(app) as fresh:
        response = fresh.post("/api/v1/checkpoint/adopt-terminal", json=body)
        assert response.status_code == 200, response.text
        point = response.json()
        assert point["opportunity"]["next_agent"] == 4
        new = restore(app.state.judge, fresh, point, run="next").json()["token"]
        entries = app.state.judge.run_history["next/fixture"]
        assert len(entries) == 3 and history(fresh, new)["best_score"] == improved
        kept_score = entries[1]["score"]
        assert math.isnan(kept_score) if math.isnan(score) else kept_score == score
        result = app.state.judge.submissions[entries[1]["submission_id"]]["report"]
        assert math.isnan(result["metrics"]["bounds"][0])
        assert result["metrics"]["bounds"][1:] == [float("inf"), -float("inf")]
        assert result["metrics"]["literal"] == "Infinity" and result["metrics"]["absent"] is None
        assert submit(fresh, new, 9).json()["round_id"] == "agent-4"
        assert submit(fresh, new, 10).status_code == 429


def test_tail_codec_keeps_finite_bytes_and_rejects_nonfinite_authority():
    packet = {"schema_version": TAIL_SCHEMA, "history": [], "results": {},
              "opportunity": {"next_agent": 1}, "observed_at_epoch": 100.0}
    assert _encode_manifest(packet, nonfinite_schema=TAIL_NONFINITE_SCHEMA) == _encode(packet)
    assert _decode_manifest(_encode(packet), finite_schema=TAIL_SCHEMA,
                            nonfinite_schema=TAIL_NONFINITE_SCHEMA) == packet
    packet["observed_at_epoch"] = float("inf")
    with pytest.raises(ValueError):
        _encode_manifest(packet, nonfinite_schema=TAIL_NONFINITE_SCHEMA)
    packet["observed_at_epoch"] = None
    packet["schema_version"] = TAIL_NONFINITE_SCHEMA
    packet["nonfinite_values"] = [{"path": ["observed_at_epoch"], "kind": "positive_infinity"}]
    with pytest.raises(CheckpointUnavailable, match="path or kind"):
        _decode_manifest(_encode(packet), finite_schema=TAIL_SCHEMA,
                         nonfinite_schema=TAIL_NONFINITE_SCHEMA)


@pytest.mark.parametrize("entrypoint", ["executor", "spooled"])
@pytest.mark.parametrize("after_replace", [False, True])
def test_queue_error_with_uncertain_terminal_storage_is_not_published(
    service, monkeypatch, entrypoint, after_replace,
):
    state, client = service
    token = register(state, client)
    state._capacity = threading.BoundedSemaphore(1)
    original = tail._atomic

    def fail_terminal(path, raw):
        packet = json.loads(raw)
        if any(value["status"] == "error" for value in packet["results"].values()):
            if after_replace:
                original(path, raw)
            raise OSError("Synthetic terminal storage failure")
        return original(path, raw)

    monkeypatch.setattr(tail, "_atomic", fail_terminal)

    def fail(*args, **kwargs):
        raise RuntimeError("Synthetic queue startup failure")

    if entrypoint == "executor":
        monkeypatch.setattr(state._executor, "submit", fail)
        with pytest.raises(RuntimeError, match="queue startup"):
            state.submit_for_token(token, b"7", "agent")
    else:
        monkeypatch.setattr(state, "_grade_worker", fail)
        monkeypatch.setattr(state._executor, "submit", lambda fn, *args: fn(*args))
        state.submit_for_token(token, b"7", "agent")
    sid, value = next(iter(state.submissions.items()))
    assert value["status"] == "queued" and value["report"] is None and value["error"] is None
    assert state._capacity.acquire(blocking=False)
    state._capacity.release()
    assert not list((state.config.log_dir / "judge-pending").iterdir())
    assert client.get("/api/v1/result/" + sid, params={"token": token}).status_code == 503
    assert client.post("/api/v1/submit", data={"token": token},
                       files={"archive": ("candidate.tar.gz", b"8")}).status_code == 503
    assert client.post("/api/v1/checkpoint/capture", json={
        "token": token, "admin_secret": state.admin_secret,
    }).status_code == 503
    assert state.tokens[token]["next_agent"] == 2
    journal = json.loads((state._checkpoint_tail.folder("source", "fixture") / "state.json").read_bytes())
    assert journal["opportunity"]["next_agent"] == 2
    assert journal["results"][sid]["status"] == ("error" if after_replace else "queued")
