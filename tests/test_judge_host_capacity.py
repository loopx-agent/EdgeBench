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

"""Independent real-entry tests; synthetic tasks, no containers or model calls."""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import threading
import time
from pathlib import Path

import pytest

from sforge.harness import judge_server as js
from sforge.harness import run_agent as ra


def wait_for(predicate, seconds=3):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if predicate():
            return True
        time.sleep(0.005)
    return bool(predicate())


def test_host_capacity_cannot_be_enlarged_by_a_second_launcher(tmp_path):
    path = str(tmp_path / "slots.json")
    small, large = js.HostJudgeCapacity(1, path), js.HostJudgeCapacity(2, path)
    a, b = f"{os.getpid()}:first", f"{os.getpid()}:second"
    assert small.try_acquire(a)
    try:
        try:
            accepted = large.try_acquire(b)
        except RuntimeError:
            accepted = False
        assert not accepted, "capacity=2 admitted over active shared capacity=1"
    finally:
        small.release(a)
        large.release(b)


def test_host_capacity_accounts_for_weighted_resource_units(tmp_path):
    capacity = js.HostJudgeCapacity(10, str(tmp_path / "capacity.json"))
    first = f"{os.getpid()}:first"
    second = f"{os.getpid()}:second"
    assert capacity.try_acquire(first, "docker:synthetic", units=7)
    try:
        assert not capacity.try_acquire(second, "docker:synthetic", units=4)
        assert capacity.try_acquire(second, "docker:synthetic", units=3)
    finally:
        capacity.release(first)
        capacity.release(second)


def test_single_slot_holder_remains_legacy_reader_compatible(tmp_path):
    path = tmp_path / "slots.json"
    capacity = js.HostJudgeCapacity(2, str(path))
    token = f"{os.getpid()}:owned"
    assert capacity.try_acquire(token, "docker:synthetic")
    try:
        holder = json.loads(path.read_text())["holders"][token]
        assert isinstance(holder, (int, float))
        owners = json.loads((tmp_path / "slots.json.owners").read_text())
        assert owners["owners"] == {token: "docker:synthetic"}
    finally:
        capacity.release(token)


def test_corrupt_slot_state_does_not_grant_new_capacity(tmp_path):
    path = tmp_path / "slots.json"
    slots = js.HostJudgeCapacity(1, str(path))
    a, b = f"{os.getpid()}:first", f"{os.getpid()}:second"
    assert slots.try_acquire(a)
    try:
        path.write_text('{"incomplete":')
        try:
            accepted = slots.try_acquire(b)
        except (ValueError, RuntimeError):
            accepted = False
        assert not accepted, "corrupt state was treated as an empty host"
    finally:
        # Test owns both synthetic holders and the private file.
        path.write_text("{}")


@pytest.mark.parametrize("mode", ["running", "poll_error", "completed", "error"])
def test_auto_loop_preserves_previous_until_terminal(tmp_path, monkeypatch, mode):
    stop = threading.Event()
    requests_seen, extracts, polls = [], [], []
    class Resp:
        def __init__(self, value): self.value = value
        def json(self): return self.value
        def raise_for_status(self): pass
    def send(url, **kwargs):
        requests_seen.append(url)
        if len(requests_seen) == 2:
            stop.set()
        return Resp({"submission_id": f"s{len(requests_seen)}", "round_id": "auto"})
    def get(url, **kwargs):
        polls.append(url)
        if mode not in ("completed", "error") and len(polls) >= 3:
            stop.set()
        if mode == "poll_error":
            raise ra.requests.Timeout("synthetic poll timeout")
        return Resp({"status": mode})
    monkeypatch.setattr(ra.requests, "post", send)
    monkeypatch.setattr(ra.requests, "get", get)
    monkeypatch.setattr(ra, "_extract_archive_from_container",
                        lambda *a, **kw: (extracts.append(1), b"x")[1])
    ra._auto_eval_loop(
        backend=None, handle=None, task_spec=None,
        host_judge_url="http://synthetic.invalid", session_token="synthetic",
        eval_interval=0, stop_event=stop, logger=logging.getLogger("test"),
        log_dir=tmp_path, admin_secret="synthetic")
    expected = 2 if mode in ("completed", "error") else 1
    assert len(requests_seen) == len(extracts) == expected
    assert polls and all(url.endswith("/s1") for url in polls)


def test_slot_path_is_resolved_at_runtime(tmp_path, monkeypatch):
    path = tmp_path / "local-slots.json"
    monkeypatch.setenv("SFORGE_JUDGE_HOST_SLOT_FILE", str(path))
    assert js.HostJudgeCapacity(1).path == path


@pytest.mark.parametrize("content", ['[]', '{}', '{"version":1,"capacity":1,"holders":[]}'])
def test_invalid_slot_shape_is_rejected(tmp_path, content):
    path = tmp_path / "slots.json"
    path.write_text(content)
    with pytest.raises(RuntimeError):
        js.HostJudgeCapacity(1, str(path)).try_acquire("synthetic-holder")
    assert path.read_text() == content


def _reserve_and_exit(path):
    assert js.HostJudgeCapacity(1, path).try_acquire(f"{os.getpid()}:orphan")


def _reserve_owned_and_exit(path):
    assert js.HostJudgeCapacity(1, path).try_acquire(
        f"{os.getpid()}:orphan", "synthetic:engine"
    )


def test_dead_launcher_requires_cleanup_evidence_before_reclaim(tmp_path):
    path = str(tmp_path / "slots.json")
    ctx = mp.get_context("spawn")
    process = ctx.Process(target=_reserve_and_exit, args=(path,))
    process.start()
    process.join(4)
    try:
        assert process.exitcode == 0
        slots = js.HostJudgeCapacity(1, path)
        assert not slots.try_acquire(f"{os.getpid()}:next")
        observed = []
        assert slots.reconcile_stale_holders(
            lambda token, _owner_id: observed.append(token) is None and False
        ) == []
        assert observed == [f"{process.pid}:orphan"]
        assert not slots.try_acquire(f"{os.getpid()}:next")
        assert slots.reconcile_stale_holders(lambda _token, _owner_id: True) == [
            f"{process.pid}:orphan"
        ]
        assert slots.try_acquire(f"{os.getpid()}:next")
    finally:
        if process.is_alive():
            process.terminate()
        process.join(2)


def test_live_capacity_owner_is_never_reconciled(tmp_path):
    slots = js.HostJudgeCapacity(1, str(tmp_path / "slots.json"))
    token = f"{os.getpid()}:live"
    assert slots.try_acquire(token)
    cleanup_calls = []
    try:
        assert slots.reconcile_stale_holders(
            lambda token, _owner_id: cleanup_calls.append(token)
        ) == []
        assert cleanup_calls == []
        assert not slots.try_acquire(f"{os.getpid()}:other")
    finally:
        slots.release(token)
