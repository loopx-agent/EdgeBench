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

"""Service-observed evaluation facts; no caller-supplied revision labels."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sforge.harness.task_spec import TaskSpec, task_spec_sha256
from sforge.harness.config import SForgeConfig, get_container_env
from sforge.harness.backend import ContainerBackend


@dataclass(frozen=True)
class RegisteredEvaluator:
    """An accepted submission keeps its exact registration's executable inputs."""

    task: TaskSpec
    config: SForgeConfig
    backend: ContainerBackend


class EvaluationConditions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["sforge_evaluation_conditions_v1"] = "sforge_evaluation_conditions_v1"
    task_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    benchmark_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_spec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    native_source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    judge_image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    execution_config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    score_direction: Literal["maximize", "minimize"]
    selection: str


class RunEvaluationContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["sforge_run_evaluation_context_v1"] = "sforge_run_evaluation_context_v1"
    task_id: str
    run_id: str
    registration_id: str
    evaluation_conditions: EvaluationConditions | None


def native_source_sha256(source_root: Path | None = None) -> str | None:
    """Use the checkpoint owner's existing source digest algorithm, once at boot."""
    if source_root is None:
        import sforge

        source_root = Path(sforge.__file__).parent
    source = hashlib.sha256()
    try:
        paths = sorted(source_root.rglob("*.py"))
        if not paths:
            return None
        for path in paths:
            source.update(path.relative_to(source_root).as_posix().encode() + b"\0")
            source.update(hashlib.sha256(path.read_bytes()).digest())
        return source.hexdigest()
    except OSError:
        return None


def capture_conditions(
    task: TaskSpec, backend, source_sha256: str | None, config: SForgeConfig,
) -> EvaluationConditions | None:
    """Unknown image/data identity keeps native feedback usable but unattributed.

    Docker's image content ID includes the evaluator and data baked into it.
    Mutable external services/data read by an eval command are outside this
    identity; this receipt does not certify deterministic or hermetic scoring.
    """
    revision = getattr(task, "_source_revision", None)
    if revision is None or source_sha256 is None or task.game_mode or backend.backend_name != "docker":
        return None
    effective = task_spec_sha256(task)
    if effective != revision.task_spec_sha256 or task.judge.score_direction not in ("maximize", "minimize"):
        return None
    try:
        image_id = backend.client.images.get(task.judge_image_key).id
    except Exception:
        # An unbuilt image is not evidence of evaluator bytes. Do not build or
        # download anything merely to qualify a read-only feedback context.
        return None
    if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        return None
    environment = get_container_env(config, include_judge_extra=True)
    environment.setdefault("SFORGE_JUDGE_URL", "http://host.docker.internal:8080")
    execution = {
        "judge_cpu_limit": config.judge_cpu_limit if config.judge_cpu_limit is not None else task.judge.cpu_limit,
        "judge_mem_limit": config.judge_mem_limit if config.judge_mem_limit is not None else task.judge.mem_limit,
        "environment": environment,
    }
    return EvaluationConditions(
        task_sha256=revision.task_sha256,
        benchmark_sha256=revision.benchmark_sha256,
        task_spec_sha256=effective,
        native_source_sha256=source_sha256,
        judge_image_id=image_id,
        execution_config_sha256=hashlib.sha256(json.dumps(
            execution, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        ).encode()).hexdigest(),
        score_direction=task.judge.score_direction,
        selection=task.judge.selection,
    )
