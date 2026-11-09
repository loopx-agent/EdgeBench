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

"""Real Docker evaluator, TCP API and generated client; no grading substitutes."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

import pytest
import requests

from sforge.harness import judge_server as native
from sforge.harness.checkpoint import JudgeCheckpoints, _sha
from sforge.harness.config import SForgeConfig
from sforge.harness.evaluation_conditions import native_source_sha256
from sforge.harness.evolve_scripts import generate_submit_script
from test_judge_checkpoint import task_files
from test_judge_checkpoint_docker import candidate, server, wait_for

pytestmark = pytest.mark.skipif(
    os.environ.get("SFORGE_CONDITIONS_DOCKER_TEST") != "1",
    reason="Requires explicit isolated Docker/HTTP evaluation-conditions qualification",
)


@pytest.fixture(scope="module")
def images():
    import docker

    client = docker.from_env()
    client.images.get("python:3.12-slim")  # Require an existing local base; do not pull.
    name = "native-conditions-" + uuid.uuid4().hex[:10]
    owned, built = [], []
    try:
        for offset in (3, 11):
            tag = f"{name}:{offset}"
            source = (
                "FROM python:3.12-slim\n"
                f"RUN printf '%s' '{{\"offset\":{offset}}}' > /tmp/evaluator-data.json\n"
            )
            image, _ = client.images.build(fileobj=io.BytesIO(source.encode()), tag=tag, rm=True)
            owned.append(tag)
            built.append(image)
        yield client, built
    finally:
        for tag in owned:
            client.images.remove(tag)


@pytest.fixture
def setup(tmp_path, images):
    client, built = images
    name = "condition-http-" + uuid.uuid4().hex[:10]
    task_files(tmp_path / "tasks")
    (tmp_path / "tasks/BENCHMARK.yaml").write_text(
        f"name: {name}\nbase_images: {{python: {{}}}}\n"
    )
    task_path = tmp_path / "tasks/fixture.json"
    task = json.loads(task_path.read_bytes())
    task["judge"]["eval_cmd"] = (
        "python3 -c 'import json, os; "
        "print(json.dumps(dict(valid=True, score="
        "json.load(open(\"candidate.json\"))[\"score\"] + "
        "json.load(open(\"/tmp/evaluator-data.json\"))[\"offset\"] + "
        "int(os.environ.get(\"CONDITION_OFFSET\", \"0\")))))'"
    )
    task_path.write_text(json.dumps(task))
    tag = f"{name}.judge.fixture:fixture"
    built[0].tag(tag)
    config = SForgeConfig(tasks_dir=tmp_path / "tasks", log_dir=tmp_path / "logs")
    try:
        yield config, client, built, tag
    finally:
        client.images.remove(tag)


def register(url, state, run="run-a", **options):
    reply = requests.post(url + "/api/v1/register", json={
        "task_id": "fixture", "run_id": run, "admin_secret": state.admin_secret,
        **options,
    }, timeout=15)
    reply.raise_for_status()
    registration = reply.json()
    return registration, url + "/api/v1/feedback/" + registration["feedback_relay_id"]


def get(url):
    reply = requests.get(url, timeout=15)
    reply.raise_for_status()
    return reply.json()


def submit(relay, score=7):
    reply = requests.post(relay + "/submit", data={"include_receipt": "true"},
                          files={"archive": ("candidate.tar.gz", candidate(score))}, timeout=15)
    reply.raise_for_status()
    return reply.json()


def result(relay, sid):
    def terminal():
        value = get(relay + "/result/" + sid + "?include_receipt=true")
        return value if value["status"] in ("completed", "error") else None

    value = wait_for(terminal)
    assert value["status"] == "completed", value
    return value


def test_real_facts_client_and_two_run_isolation(setup, tmp_path, monkeypatch):
    config, client, built, _ = setup
    source_reads = []
    def observed_source():
        source_reads.append(True)
        return native_source_sha256()
    monkeypatch.setattr(native, "native_source_sha256", observed_source)
    app = native.create_app(config)
    task_raw = (config.tasks_dir / "fixture.json").read_bytes()
    benchmark_raw = (config.tasks_dir / "BENCHMARK.yaml").read_bytes()
    with server(app) as url:
        state = app.state.judge
        image_reads = []
        collection = type(state.backend.client.images)
        actual_image_get = collection.get
        def observed_image(owner, key):
            if owner.client is state.backend.client:
                image_reads.append(key)
            return actual_image_get(owner, key)
        monkeypatch.setattr(collection, "get", observed_image)
        registration, relay = register(url, state, evaluation_context={"task_id": "forged"})
        assert len(image_reads) == 1
        context = get(relay + "/context")
        assert context == registration["evaluation_context"]
        conditions = context["evaluation_conditions"]
        assert conditions["task_sha256"] == hashlib.sha256(task_raw).hexdigest()
        assert conditions["benchmark_sha256"] == hashlib.sha256(benchmark_raw).hexdigest()
        assert conditions["task_spec_sha256"] == _sha(json.dumps(
            asdict(state.tasks["fixture"]), sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode())
        assert conditions["native_source_sha256"] == native_source_sha256()
        assert conditions["judge_image_id"] == built[0].id
        assert conditions["score_direction"] == "maximize"
        assert context["task_id"] == "fixture" and context["run_id"] == "run-a"
        assert requests.get(url + "/api/v1/feedback/not-a-relay/context", timeout=5).status_code == 404
        original_tokens = copy.deepcopy(state.tokens)
        duplicate = requests.post(url + "/api/v1/register", json={
            "task_id": "fixture", "run_id": "run-a", "admin_secret": state.admin_secret,
        }, timeout=15)
        assert duplicate.status_code == 200 and duplicate.json() == registration
        assert state.tokens == original_tokens and not state.submissions

        script = tmp_path / "sforge-submit"
        script.write_text(generate_submit_script())
        patch = tmp_path / "patch"
        patch.mkdir()
        (patch / "candidate.json").write_text('{"score":7}')
        env = {**os.environ, "SFORGE_FEEDBACK_URL": relay, "SFORGE_PATCH_DIR": str(patch),
               "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}

        def cli(*args):
            completed = subprocess.run(["bash", str(script), *args], env=env,
                                       capture_output=True, text=True, timeout=20)
            assert completed.returncode == 0, completed.stderr
            return json.loads(completed.stdout)

        assert cli("--context", "--json") == context
        assert get(relay + "/history")["agent_submissions"] == 0
        retained = tmp_path / "retained-uploads"
        retained.mkdir()
        accepted = cli("--no-wait", "--json", "--retain-archive", str(retained))
        sid = accepted["submission_id"]
        read = result(relay, sid)
        assert cli("--result", sid, "--no-wait", "--json") == read
        assert read["report"]["score"] == 10  # submitted 7 + actual image data 3
        saved = list(retained.glob("*/submission.tar.gz"))
        assert len(saved) == 1
        uploaded = saved[0].read_bytes()
        assert hashlib.sha256(uploaded).hexdigest() == read["receipt"]["archive_sha256"]
        assert len(uploaded) == read["receipt"]["archive_bytes"]
        native_archive = config.log_dir / "runs/run-a/fixture/submissions/agent-1/submission.tar.gz"
        assert native_archive.read_bytes() == uploaded
        assert accepted["receipt"]["evaluation_context"] == context
        assert read["receipt"]["evaluation_context"] == context
        raw = get(relay + "/result/" + sid)
        assert raw == {k: v for k, v in read.items() if k != "receipt"}
        _, other = register(url, state, "run-b")
        other_context = get(other + "/context")
        assert other_context["evaluation_conditions"] == conditions
        assert other_context["registration_id"] != context["registration_id"]
        assert requests.get(other + "/result/" + sid + "?include_receipt=true", timeout=5).status_code == 404
        assert get(other + "/history")["agent_submissions"] == 0
        reads_before = len(image_reads)
        for _ in range(3):
            assert get(relay + "/context") == context
            assert get(relay + "/result/" + sid + "?include_receipt=true") == read
        assert get(relay + "/history")["agent_submissions"] == 1
        assert len(source_reads) == 1 and len(image_reads) == reads_before

        # Identical registration retries share one counter; concurrent different
        # candidates cannot both write agent-1's archive or evaluation output.
        repeated, repeated_relay = register(url, state)
        assert repeated == registration and repeated_relay == relay
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(submit, route, score) for route, score in ((relay, 12), (repeated_relay, 19))]
            candidates = [future.result() for future in futures]
        reads = [result(relay, value["submission_id"]) for value in candidates]
        assert [value["report"]["score"] for value in reads] == [15, 22]
        assert {value["receipt"]["round_id"] for value in reads} == {"agent-2", "agent-3"}
        for value, score in zip(reads, (12, 19)):
            archived = config.log_dir / "runs/run-a/fixture/submissions" / value["receipt"]["round_id"] / "submission.tar.gz"
            assert hashlib.sha256(archived.read_bytes()).hexdigest() == value["receipt"]["archive_sha256"]


def test_loaded_bytes_image_and_repeated_registration_do_not_relabel_execution(setup):
    config, client, built, tag = setup
    app = native.create_app(config)
    with server(app) as url:
        state = app.state.judge
        original, relay_a = register(url, state)
        before = original["evaluation_context"]
        task_path = config.tasks_dir / "fixture.json"
        changed = json.loads(task_path.read_bytes())
        changed["judge"]["score_direction"] = "minimize"
        changed["judge"]["eval_cmd"] = changed["judge"]["eval_cmd"].replace(
            'json.load(open("candidate.json"))["score"] +',
            '2 * json.load(open("candidate.json"))["score"] +',
        )
        task_path.write_text(json.dumps(changed))
        (config.tasks_dir / "BENCHMARK.yaml").write_text(
            (config.tasks_dir / "BENCHMARK.yaml").read_text() + "# another revision\n"
        )
        built[1].tag(tag)
        assert get(relay_a + "/context") == before
        state.load_tasks()
        state.config.judge_extra_env["CONDITION_OFFSET"] = "5"
        # Native logs/opportunity are run/task scoped. Do not merge different conditions into them.
        rejected = requests.post(url + "/api/v1/register", json={
            "task_id": "fixture", "run_id": "run-a", "admin_secret": state.admin_secret,
            "judge_cpu_limit": 1, "judge_mem_limit": "192m",
        }, timeout=15)
        assert rejected.status_code == 409 and "fresh run_id" in rejected.json()["detail"]
        replacement, relay_b = register(url, state, "run-b", judge_cpu_limit=1, judge_mem_limit="192m")
        after = replacement["evaluation_context"]
        for key in ("task_sha256", "benchmark_sha256", "task_spec_sha256", "judge_image_id",
                    "execution_config_sha256", "score_direction"):
            assert after["evaluation_conditions"][key] != before["evaluation_conditions"][key]
        first, second = submit(relay_a), submit(relay_b)
        old = result(relay_a, first["submission_id"])
        new = result(relay_b, second["submission_id"])
        assert old["report"]["score"] == 10  # old task + old image + old environment
        assert new["report"]["score"] == 30  # new task 2*7 + new image 11 + env 5
        assert old["receipt"]["evaluation_context"] == before
        assert new["receipt"]["evaluation_context"] == after
        assert get(relay_a + "/context") == before


def test_unbuilt_image_and_missing_legacy_source_remain_unknown(setup):
    config, client, built, tag = setup
    client.images.remove(tag)
    app = native.create_app(config)
    try:
        with server(app) as url:
            state = app.state.judge
            registration, relay = register(url, state)
            assert registration["evaluation_context"]["evaluation_conditions"] is None
            assert get(relay + "/context")["evaluation_conditions"] is None
            assert not state.backend.image_exists(tag)
            built[0].tag(tag)
            accepted = submit(relay)
            read = result(relay, accepted["submission_id"])
            assert read["report"]["score"] == 10
            assert read["receipt"]["evaluation_context"]["evaluation_conditions"] is None
            # Historical-format fixture: never borrow context from a current registration.
            del state.submissions[accepted["submission_id"]]["evaluation_context"]
            legacy = get(relay + "/result/" + accepted["submission_id"] + "?include_receipt=true")
            assert legacy["report"] == read["report"]
            assert legacy["receipt"]["evaluation_context"] is None
    finally:
        built[0].tag(tag)


def test_checkpoint_preserves_original_context_and_legacy_unknown(setup):
    config, client, built, tag = setup
    app = native.create_app(config, checkpoints=True)
    with server(app) as url:
        state = app.state.judge
        registration, relay = register(url, state)
        accepted = submit(relay)
        read = result(relay, accepted["submission_id"])
        bank = JudgeCheckpoints(state)
        point = bank.capture(registration["token"], "conditions-point")
        restored = bank.restore(point["checkpoint_id"], point["manifest_sha256"], "restored")
        restored_relay = url + "/api/v1/feedback/" + state.feedback_relay_id(restored)
        inherited_sid = get(restored_relay + "/history")["entries"][0]["submission_id"]
        inherited = result(restored_relay, inherited_sid)
        assert inherited_sid != accepted["submission_id"]
        assert inherited["receipt"] == read["receipt"]
        assert inherited["receipt"]["evaluation_context"]["run_id"] == "run-a"
        assert get(restored_relay + "/context")["run_id"] == "restored"
        # Separate legacy-format capsule; the original immutable capsule is unchanged.
        _, packet = bank._packet(point["checkpoint_id"])
        legacy_packet = copy.deepcopy(packet)
        for value in legacy_packet["results"].values():
            value.pop("evaluation_context", None)
        archive = (bank.bank / point["checkpoint_id"] / "incumbent.tar.gz").read_bytes()
        legacy_point = bank._publish_point("legacy-point", legacy_packet, archive)
        legacy_token = bank.restore(legacy_point["checkpoint_id"], legacy_point["manifest_sha256"], "legacy-restored")
        legacy_relay = url + "/api/v1/feedback/" + state.feedback_relay_id(legacy_token)
        legacy_sid = get(legacy_relay + "/history")["entries"][0]["submission_id"]
        legacy = result(legacy_relay, legacy_sid)
        assert legacy["report"] == read["report"]
        assert legacy["receipt"]["evaluation_context"] is None
        assert get(legacy_relay + "/context")["evaluation_conditions"] is not None

def test_changed_service_source_is_reported_by_the_actual_remote_process(setup, tmp_path):
    import shutil
    import socket
    import sys
    import time
    from pathlib import Path

    config, client, built, _ = setup
    import sforge

    copied = tmp_path / "separate-service"
    shutil.copytree(Path(sforge.__file__).parent, copied / "sforge",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    altered = copied / "sforge/harness/grading.py"
    altered.write_bytes(altered.read_bytes() + b"\n# Isolated service source revision fixture.\n")
    expected = native_source_sha256(copied / "sforge")
    assert expected != native_source_sha256()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    launcher = (
        "import sys,uvicorn; from pathlib import Path; "
        "from sforge.harness.config import SForgeConfig; "
        "from sforge.harness.judge_server import create_app; "
        "app=create_app(SForgeConfig(tasks_dir=Path(sys.argv[1]),log_dir=Path(sys.argv[2]))); "
        "uvicorn.run(app,host='127.0.0.1',port=int(sys.argv[3]),log_level='error',access_log=False)"
    )
    env = {**os.environ, "PYTHONPATH": str(copied), "SFORGE_ADMIN_SECRET": "isolated-condition-test",
           "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    with (tmp_path / "separate-service.stderr").open("wb") as diagnostics:
        process = subprocess.Popen(
            [sys.executable, "-c", launcher, str(config.tasks_dir), str(tmp_path / "child-logs"), str(port)],
            cwd=copied, env=env, stdout=diagnostics, stderr=diagnostics,
        )
        try:
            url = f"http://127.0.0.1:{port}"
            def ready():
                assert process.poll() is None
                try:
                    return requests.get(url + "/api/v1/feedback/absent/context", timeout=.2).status_code == 404
                except requests.ConnectionError:
                    return False
            wait_for(ready)
            registered = requests.post(url + "/api/v1/register", json={
                "task_id": "fixture", "run_id": "different-source",
                "admin_secret": "isolated-condition-test",
            }, timeout=15)
            registered.raise_for_status()
            packet = registered.json()
            relay = url + "/api/v1/feedback/" + packet["feedback_relay_id"]
            context = get(relay + "/context")
            assert context["evaluation_conditions"]["native_source_sha256"] == expected
            accepted = submit(relay)
            read = result(relay, accepted["submission_id"])
            assert read["report"]["score"] == 10
            assert read["receipt"]["evaluation_context"] == context
        finally:
            process.terminate()
            process.wait(timeout=10)


def test_single_axis_task_direction_and_resources_are_distinct(setup):
    config, client, built, tag = setup
    app = native.create_app(config)
    with server(app) as url:
        state = app.state.judge
        original, _ = register(url, state, "baseline")
        baseline = original["evaluation_context"]["evaluation_conditions"]
        task_path = config.tasks_dir / "fixture.json"
        task_path.write_bytes(task_path.read_bytes() + b"\n")
        state.load_tasks()
        whitespace, _ = register(url, state, "task-bytes")
        changed = whitespace["evaluation_context"]["evaluation_conditions"]
        assert changed["task_sha256"] != baseline["task_sha256"]
        assert changed["task_spec_sha256"] == baseline["task_spec_sha256"]
        direction = json.loads(task_path.read_bytes())
        direction["judge"]["score_direction"] = "minimize"
        task_path.write_text(json.dumps(direction))
        state.load_tasks()
        downward, _ = register(url, state, "direction")
        conditions = downward["evaluation_context"]["evaluation_conditions"]
        assert conditions["score_direction"] == "minimize"
        assert conditions["task_spec_sha256"] != baseline["task_spec_sha256"]
        assert conditions["judge_image_id"] == baseline["judge_image_id"]
        cpu, _ = register(url, state, "cpu", judge_cpu_limit=1)
        memory, _ = register(url, state, "memory", judge_mem_limit="128m")
        assert cpu["evaluation_context"]["evaluation_conditions"]["execution_config_sha256"] != conditions["execution_config_sha256"]
        assert memory["evaluation_context"]["evaluation_conditions"]["execution_config_sha256"] != conditions["execution_config_sha256"]
        assert cpu["evaluation_context"]["evaluation_conditions"]["execution_config_sha256"] != memory["evaluation_context"]["evaluation_conditions"]["execution_config_sha256"]

def test_terminal_tail_restart_keeps_original_per_result_conditions(setup):
    config, client, built, tag = setup
    app = native.create_app(config, checkpoints=True)
    with server(app) as url:
        state = app.state.judge
        registration, relay = register(url, state)
        first = submit(relay)
        first_read = result(relay, first["submission_id"])
        point = JudgeCheckpoints(state).capture(registration["token"], uuid.uuid4().hex)
        second = submit(relay, 9)
        second_read = result(relay, second["submission_id"])
        info = state.resolve_token(registration["token"])
        _, retained = state._checkpoint_tail._load(info)
        for accepted in (first, second):
            assert retained["results"][accepted["submission_id"]]["evaluation_context"] == registration["evaluation_context"]
    restarted = native.create_app(config, checkpoints=True)
    with server(restarted) as url:
        state = restarted.state.judge
        adopted = state._checkpoint_tail.adopt(
            checkpoint_id=point["checkpoint_id"], manifest_sha256=point["manifest_sha256"],
            source_run_id="run-a", task_id="fixture",
            source_registration_id=registration["evaluation_context"]["registration_id"],
            capture_id=uuid.uuid4().hex,
        )
        token = JudgeCheckpoints(state).restore(adopted["checkpoint_id"], adopted["manifest_sha256"], "tail-restored")
        relay = url + "/api/v1/feedback/" + state.feedback_relay_id(token)
        entries = get(relay + "/history")["entries"]
        reads = [result(relay, entry["submission_id"]) for entry in entries]
        assert [value["report"]["score"] for value in reads] == [10, 12]
        assert [value["receipt"]["evaluation_context"] for value in reads] == [
            first_read["receipt"]["evaluation_context"], second_read["receipt"]["evaluation_context"],
        ]

def test_actual_container_uses_registered_resource_environment_and_image_snapshot(setup):
    import docker

    config, client, built, _ = setup
    task_path = config.tasks_dir / "fixture.json"
    task = json.loads(task_path.read_bytes())
    task["judge"]["eval_cmd"] = "sleep 1; " + task["judge"]["eval_cmd"]
    task_path.write_text(json.dumps(task))
    config.judge_extra_env["CONDITION_OFFSET"] = "4"
    app = native.create_app(config)
    with server(app) as url:
        state = app.state.judge
        registered, relay = register(url, state, judge_cpu_limit=1, judge_mem_limit="128m")
        context = registered["evaluation_context"]
        # A later caller-side config change must not change accepted execution.
        state.config.judge_extra_env["CONDITION_OFFSET"] = "9"
        accepted = submit(relay)
        name = f"{state.tasks['fixture'].benchmark_name}.judge.fixture.{accepted['submission_id']}"
        def running():
            try:
                found = client.containers.get(name)
                return found if found.attrs["State"]["Running"] else None
            except docker.errors.NotFound:
                return None
        owned = wait_for(running)
        assert owned.attrs["HostConfig"]["NanoCpus"] == 1_000_000_000
        assert owned.attrs["HostConfig"]["Memory"] == 128 * 1024 * 1024
        assert owned.attrs["Image"] == context["evaluation_conditions"]["judge_image_id"]
        assert "CONDITION_OFFSET=4" in owned.attrs["Config"]["Env"]
        assert "CONDITION_OFFSET=9" not in owned.attrs["Config"]["Env"]
        read = result(relay, accepted["submission_id"])
        assert read["report"]["score"] == 14
        assert read["receipt"]["evaluation_context"] == context
