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

"""Task/source load provenance and honest unknowns, without evaluator substitutes."""

import hashlib
import json
from dataclasses import asdict
from types import SimpleNamespace

from sforge.harness.benchmark import load_benchmark
from sforge.harness.checkpoint import _encode
from sforge.harness.config import SForgeConfig
from sforge.harness.evaluation_conditions import capture_conditions, native_source_sha256
from sforge.harness.task_spec import load_all_tasks, task_spec_sha256
from test_judge_checkpoint import task_files


def test_exact_parsed_bytes_and_legacy_effective_spec_encoding(tmp_path):
    task_files(tmp_path / "tasks")
    task_path = tmp_path / "tasks/fixture.json"
    content = json.loads(task_path.read_bytes())
    content["name"] = "公开 fixture"
    task_path.write_text(json.dumps(content, ensure_ascii=False))
    benchmark_path = tmp_path / "tasks/BENCHMARK.yaml"
    benchmark_raw, task_raw = benchmark_path.read_bytes(), task_path.read_bytes()
    benchmark = load_benchmark(tmp_path / "tasks")
    task = load_all_tasks(tmp_path / "tasks", benchmark)[0]
    original = task._source_revision
    assert original.task_sha256 == hashlib.sha256(task_raw).hexdigest()
    assert original.benchmark_sha256 == hashlib.sha256(benchmark_raw).hexdigest()
    assert original.task_spec_sha256 == task_spec_sha256(task)
    assert task_spec_sha256(task) == hashlib.sha256(_encode(asdict(task))).hexdigest()
    assert "_source_revision" not in asdict(task)
    task_path.write_bytes(task_raw + b"\n")
    benchmark_path.write_bytes(benchmark_raw + b"# later revision\n")
    assert task._source_revision == original
    reloaded = load_all_tasks(tmp_path / "tasks", load_benchmark(tmp_path / "tasks"))[0]
    assert reloaded._source_revision.task_sha256 != original.task_sha256
    assert reloaded._source_revision.benchmark_sha256 != original.benchmark_sha256
    assert task_spec_sha256(reloaded) == task_spec_sha256(task)


def test_missing_source_non_docker_and_mutated_spec_are_unknown(tmp_path):
    task_files(tmp_path / "tasks")
    task = load_all_tasks(tmp_path / "tasks", load_benchmark(tmp_path / "tasks"))[0]
    config = SForgeConfig()
    class NoImageLookup:
        backend_name = "docker"
        @property
        def client(self):
            raise AssertionError("an unknown source/spec must not inspect or build an image")
    assert capture_conditions(task, NoImageLookup(), None, config) is None
    assert capture_conditions(task, SimpleNamespace(backend_name="k8s"), "a" * 64, config) is None
    task.judge.score_direction = "minimize"
    assert capture_conditions(task, NoImageLookup(), "a" * 64, config) is None
    task._source_revision = None
    assert capture_conditions(task, NoImageLookup(), "a" * 64, config) is None


def test_source_digest_is_bytes_and_relative_names_not_git_labels(tmp_path):
    assert native_source_sha256(tmp_path) is None
    (tmp_path / "native.py").write_bytes(b"print('first')\n")
    expected = hashlib.sha256(b"native.py\0" + hashlib.sha256(b"print('first')\n").digest()).hexdigest()
    assert native_source_sha256(tmp_path) == expected
    (tmp_path / "native.py").write_bytes(b"print('second')\n")
    assert native_source_sha256(tmp_path) != expected
