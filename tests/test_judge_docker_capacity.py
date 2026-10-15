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

"""Opt-in ownership tests with real containers and a real JudgeState worker."""
from __future__ import annotations

import multiprocessing as mp
import os
import time
import types
import uuid
from pathlib import Path

import docker
import pytest

from sforge.harness import judge_server as js
from sforge.harness.backend.docker_backend import DockerBackend
from sforge.harness.config import SForgeConfig


def _launch_judge(image, label, root, ready, orphan):
    client = docker.from_env()

    def judge(**kwargs):
        owned = client.containers.run(
            image, ["sleep", "60" if orphan else "0.2"], detach=True,
            network_disabled=True, labels={"sforge-capacity-test": label},
            name=f"synthetic.judge.synthetic.{kwargs['submission_id']}",
        )
        token = f"{os.getpid()}:{kwargs['submission_id']}"
        ready.put({"container_id": owned.id, "slot_token": token})
        try:
            assert owned.wait(timeout=65)["StatusCode"] == 0
            return types.SimpleNamespace(to_dict=lambda: {
                "valid": True, "pass_rate": 1.0, "score": 1.0})
        finally:
            owned.remove(force=True)

    js.judge_submission = judge
    os.environ["SFORGE_JUDGE_HOST_SLOT_FILE"] = str(Path(root) / "slots.json")
    os.environ["SFORGE_JUDGE_HOST_OWNER_FILE"] = str(Path(root) / "owners.json")
    state = js.JudgeState(SForgeConfig(log_dir=Path(root), judge_max_concurrent=1,
                                      judge_host_capacity=1))
    state.host_capacity = js.HostJudgeCapacity(1, str(Path(root) / "slots.json"))
    state.tasks = {"synthetic": types.SimpleNamespace(task_id="synthetic")}
    sid = state.submit("synthetic", b"synthetic", "run", "auto-1")
    deadline = time.monotonic() + 70
    while any(x["status"] in ("queued", "running") for x in state.submissions.values()) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not any(x["status"] in ("queued", "running") for x in state.submissions.values())
    assert state.get_result(sid)["status"] == "completed"
    state.close()
    client.close()


@pytest.mark.parametrize("orphan", [False, True], ids=["normal_cleanup", "launcher_crash"])
def test_slot_release_requires_judge_cleanup(tmp_path, orphan):
    image = os.environ.get("SFORGE_DOCKER_TEST_IMAGE")
    if not image:
        pytest.skip("set SFORGE_DOCKER_TEST_IMAGE to a cached image")
    client = docker.from_env()
    client.images.get(image)  # Never pull or build.
    label = uuid.uuid4().hex
    ctx = mp.get_context("spawn")
    ready = ctx.Queue()
    process = ctx.Process(target=_launch_judge,
                          args=(image, label, str(tmp_path), ready, orphan))
    owned_id = None
    process.start()
    try:
        receipt = ready.get(timeout=15)
        owned_id = receipt["container_id"]
        slots = js.HostJudgeCapacity(1, str(tmp_path / "slots.json"))
        if orphan:
            process.terminate()
            process.join(5)
            assert process.exitcode is not None
            owned = client.containers.get(owned_id)
            assert owned.labels.get("sforge-capacity-test") == label
            assert owned.status == "running"
            assert not slots.try_acquire("replacement")
            backend = DockerBackend(client)
            assert slots.reconcile_stale_holders(
                backend.reconcile_judge_capacity_holder
            ) == [receipt["slot_token"]]
            assert not client.containers.list(all=True, filters={"id": owned_id})
        else:
            process.join(15)
            assert process.exitcode == 0
            assert not client.containers.list(all=True, filters={"id": owned_id})
        assert slots.try_acquire("replacement")
        slots.release("replacement")
    finally:
        if process.is_alive():
            process.terminate()
        process.join(5)
        # Only containers created by this test's unique ownership label.
        for owned in client.containers.list(all=True, filters={"label": f"sforge-capacity-test={label}"}):
            owned.remove(force=True)
        assert not client.containers.list(all=True, filters={"label": f"sforge-capacity-test={label}"})
        client.close()


def test_unconfirmed_live_cleanup_retains_count_and_memory(tmp_path, monkeypatch):
    """A terminal error cannot release reservations for a live owned container."""
    image = os.environ.get("SFORGE_DOCKER_TEST_IMAGE")
    if not image:
        pytest.skip("set SFORGE_DOCKER_TEST_IMAGE to a cached image")
    client = docker.from_env()
    client.images.get(image)
    label = uuid.uuid4().hex
    backend = DockerBackend(client)
    owned = []
    monkeypatch.setenv("SFORGE_JUDGE_HOST_SLOT_FILE", str(tmp_path / "slots.json"))
    monkeypatch.setenv("SFORGE_JUDGE_HOST_OWNER_FILE", str(tmp_path / "owners.json"))
    monkeypatch.setenv("SFORGE_JUDGE_HOST_MEMORY_FILE", str(tmp_path / "memory.json"))

    def fail_cleanup(**kwargs):
        owned.append(client.containers.run(
            image, ["sleep", "120"], detach=True, network_disabled=True,
            name=f"synthetic.judge.synthetic.{kwargs['submission_id']}",
            labels={"sforge-capacity-test": label},
        ))
        raise RuntimeError("synthetic provider cleanup unavailable")

    monkeypatch.setattr(js, "judge_submission", fail_cleanup)
    # False is the provider's explicit inability to attest owned-resource absence.
    monkeypatch.setattr(backend, "reconcile_judge_capacity_holder", lambda *_: False)
    state = js.JudgeState(SForgeConfig(
        log_dir=tmp_path, judge_max_concurrent=1,
        judge_host_capacity=1, judge_host_memory_capacity="128m",
    ), backend_factory=lambda _: backend)
    state.tasks = {"synthetic": types.SimpleNamespace(
        task_id="synthetic", judge=types.SimpleNamespace(mem_limit="64m"))}
    sid = state.submit("synthetic", b"", "run", "agent-1")
    try:
        state.close()
        assert state.get_result(sid)["status"] == "error"
        assert len(owned) == 1
        owned[0].reload()
        assert owned[0].status == "running"
        token = f"{os.getpid()}:{sid}"
        assert token in state.host_capacity._read()["holders"]
        assert state.host_memory_capacity._read()["holders"][token]["units"] == 64 * 1024 * 1024
        assert not state.host_capacity.try_acquire("replacement")
        assert not state.host_memory_capacity.try_acquire("replacement", units=128 * 1024 * 1024)
        owned[0].remove(force=True)
        owned.clear()
        # Explicit operator recovery follows verified removal, without rescoring.
        state.host_memory_capacity.release(token)
        state.host_capacity.release(token)
        assert state.host_capacity.try_acquire("replacement")
        state.host_capacity.release("replacement")
    finally:
        for container in owned:
            container.remove(force=True)
        assert not client.containers.list(all=True, filters={"label": f"sforge-capacity-test={label}"})
        client.close()
