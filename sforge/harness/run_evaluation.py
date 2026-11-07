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

"""Evaluation orchestration: extract archive in judge container, run tests, grade."""

from __future__ import annotations

import fnmatch
import json
import logging
import tarfile
import time
import uuid
from pathlib import Path, PurePosixPath

from sforge.harness.backend import ContainerBackend
from sforge.harness.backend.base import ContainerResourceObservation
from sforge.harness.config import (
    SForgeConfig,
    get_container_env,
    sanitize_e2b_proxy_env,
)
from sforge.harness.constants import DOCKER_USER
from sforge.harness.docker_build import (
    build_judge_image,
    close_logger,
    setup_logger,
)
from sforge.harness.grading import EvalReport, grade_output
from sforge.harness.score_rescale import rescale_score
from sforge.harness.task_spec import TaskSpec


class EvaluationError(Exception):
    def __init__(self, task_id: str, message: str, logger: logging.Logger):
        super().__init__(message)
        self.task_id = task_id
        self.log_file = getattr(logger, "log_file", None)


def _resource_observation(backend, handle, logger) -> ContainerResourceObservation | None:
    """Best-effort terminal diagnostics must never change grading or cleanup."""
    observe = getattr(backend, "resource_observation", None)
    if handle is None or not callable(observe):
        return None
    try:
        value = observe(handle)
        return value if isinstance(value, ContainerResourceObservation) else None
    except Exception as exc:
        # Do not expose provider errors, which may include daemon URLs or auth.
        logger.warning("Resource observation unavailable (%s)", type(exc).__name__)
        return None


def _filter_archive_files(
    file_list: list[str],
    submit_paths: list[str],
    submit_exclude: list[str],
) -> list[str]:
    """Filter archive entries against submit_paths (whitelist) and submit_exclude (blacklist).

    A file is allowed if:
      1. It falls under at least one submit_paths entry (prefix match, or "." allows all)
      2. It does NOT match any submit_exclude pattern (prefix or fnmatch glob)
      3. It is a relative path without parent traversal or NUL characters
      4. It is not a directory entry (trailing /)

    Match canonical relative paths, but return the exact archive spelling for
    extraction. Whitespace and backslashes are legal filename characters.
    """
    allowed: list[str] = []
    for f in file_list:
        path = PurePosixPath(f)
        if (not f or f.endswith("/") or "\0" in f or path.is_absolute()
                or ".." in path.parts or str(path) == "."):
            continue
        canonical = str(path)

        # Whitelist: must be under at least one submit_paths entry
        in_whitelist = False
        for sp in submit_paths:
            if not sp:
                continue
            sp = str(PurePosixPath(sp))
            if sp == ".":
                in_whitelist = True
                break
            if canonical == sp or canonical.startswith(sp + "/"):
                in_whitelist = True
                break
        if not in_whitelist:
            continue

        # Blacklist: reject if any exclude pattern matches
        excluded = False
        for exc in submit_exclude:
            exc = str(PurePosixPath(exc))
            if canonical == exc or canonical.startswith(exc + "/"):
                excluded = True
                break
            basename = path.name
            if fnmatch.fnmatch(basename, exc):
                excluded = True
                break
            if fnmatch.fnmatch(canonical, exc):
                excluded = True
                break
        if excluded:
            continue

        allowed.append(f)
    return allowed


def judge_submission(
    task_spec: TaskSpec,
    archive: bytes,
    config: SForgeConfig,
    backend: ContainerBackend,
    submission_id: str | None = None,
    timeout: int | None = None,
    log_dir: Path | None = None,
    verbose: bool = False,
    submitted_at: float | None = None,
) -> EvalReport:
    """
    Grade an archive submission in an ephemeral judge container.

    1. Ensure judge image exists
    2. Create ephemeral container from judge image
    3. Extract archive (tar.gz) into patch_dir
    4. Inject eval.sh into container
    5. Run eval script
    6. Parse output, compute score
    7. Cleanup container

    Args:
        archive: Raw bytes of a tar.gz containing submitted files.
        log_dir: Override log output directory. If None, defaults to
                 logs/runs/<submission_id>/<task_id>/submissions/001/
    """
    if submission_id is None:
        submission_id = uuid.uuid4().hex[:12]

    submitted_at = time.time() if submitted_at is None else submitted_at
    timeout = timeout or task_spec.judge.eval_timeout

    # Set up logging
    if log_dir is None:
        log_dir = (
            config.log_dir / "runs" / submission_id
            / task_spec.task_id / "submissions" / "1"
        )
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(
        f"judge.{task_spec.task_id}.{submission_id}",
        log_dir / "run_instance.log",
        verbose=verbose,
    )

    handle = None
    resources = None
    resources_observed = False
    stage = "prepare_image"
    try:
        # 1. Ensure judge image exists (Docker builds locally; K8s requires pre-built images in CR)
        if backend.backend_name == "docker":
            from sforge.harness.backend.docker_backend import DockerBackend
            assert isinstance(backend, DockerBackend)
            build_judge_image(task_spec, config, backend.client, force_rebuild=False)

        # 2. Create + start ephemeral container
        container_name = f"{task_spec.benchmark_name}.judge.{task_spec.task_id}.{submission_id}"
        env = get_container_env(config, include_judge_extra=True)
        if backend.backend_name == "e2b":
            sanitize_e2b_proxy_env(env)
        env.setdefault("SFORGE_JUDGE_URL", "http://host.docker.internal:8080")
        # Resolve resource limits: config > task defaults
        cpu = config.judge_cpu_limit if config.judge_cpu_limit is not None else task_spec.judge.cpu_limit
        mem = config.judge_mem_limit if config.judge_mem_limit is not None else task_spec.judge.mem_limit
        stage = "create_container"
        handle = backend.create_container(
            task_spec.judge_image_key,
            container_name,
            environment=env,
            extra_hosts={"host.docker.internal": "host-gateway"},
            cpu_limit=cpu,
            mem_limit=mem,
            user=DOCKER_USER,
        )
        stage = "start_container"
        backend.start_container(handle)
        logger.info(f"Judge container started: {handle.id}")

        # 3. Extract archive into patch_dir (filtered by submit_paths/submit_exclude)
        stage = "extract_submission"
        if archive:
            local_tar = log_dir / "submission.tar.gz"
            local_tar.write_bytes(archive)
            logger.info(f"Archive saved ({len(archive)} bytes)")

            tar_path = PurePosixPath("/tmp/submission.tar.gz")
            backend.copy_to_container(handle, local_tar, tar_path)

            # Parse member headers, not tar's human-readable listing: names can
            # contain newlines, control characters and non-ASCII bytes. Only
            # inspect on the host; extraction remains inside the judge.
            with tarfile.open(local_tar, "r:gz", encoding="utf-8",
                              errors="surrogateescape") as archive_info:
                all_files = [member.name for member in archive_info
                             if not member.isdir()]
            allowed_files = _filter_archive_files(
                all_files, task_spec.submit_paths, task_spec.submit_exclude,
            )

            rejected = set(all_files) - set(allowed_files)
            if rejected:
                logger.warning(f"Archive path filter rejected {len(rejected)} file(s): {sorted(rejected)[:20]}")

            if allowed_files:
                # NUL + verbatim avoids line splitting, trimming, unescaping
                # and option parsing. Deduplicate selectors (tar still applies
                # archive entries in order), and never recursively extract a
                # selected name: descendants must pass their own path filter.
                file_list_content = b"".join(
                    name.encode("utf-8", "surrogateescape") + b"\0"
                    for name in dict.fromkeys(allowed_files)
                )
                local_file_list = log_dir / "allowed_files.nul"
                local_file_list.write_bytes(file_list_content)
                backend.copy_to_container(handle, local_file_list, PurePosixPath("/tmp/allowed_files.nul"))

                val = backend.exec_run(
                    handle,
                    f"tar xzf {tar_path} --no-recursion --null "
                    "--verbatim-files-from -T /tmp/allowed_files.nul",
                    workdir=task_spec.cwd,
                    user=DOCKER_USER,
                )
                if val.exit_code != 0:
                    err_msg = val.output
                    logger.error(f"Archive extraction failed: {err_msg}")
                    raise EvaluationError(
                        task_spec.task_id,
                        f"Archive extraction failed: {err_msg}",
                        logger,
                    )
                logger.info(f"Archive extracted: {len(allowed_files)} file(s) allowed, {len(rejected)} rejected")
            else:
                logger.warning("All archive entries were filtered out — running eval on unmodified skeleton")
        else:
            logger.info("Empty archive — running eval on unmodified skeleton")

        # 4. Inject eval.sh dynamically (not baked into the image)
        stage = "prepare_evaluation"
        eval_script_local = log_dir / "eval.sh"
        eval_script_local.write_text(task_spec.eval_script)
        backend.copy_to_container(handle, eval_script_local, PurePosixPath("/tmp/eval.sh"))

        # 5. Run eval script. workdir matters for eval_cmds that use relative
        # paths: docker defaults to the image WORKDIR but envd execs land in
        # the user's home, so pin the task cwd explicitly.
        logger.info("Running eval script...")
        stage = "execute_evaluation"
        result = backend.exec_run_with_timeout(
            handle, "/bin/bash /tmp/eval.sh", timeout, workdir=task_spec.cwd
        )
        test_output, timed_out, runtime = result.output, result.timed_out, result.elapsed_seconds
        resources = _resource_observation(backend, handle, logger)
        resources_observed = True

        # Save test output
        (log_dir / "test_output.txt").write_text(test_output)
        logger.info(f"Test runtime: {runtime:.2f}s, timed_out: {timed_out}")

        # 6. Grade
        stage = "grade_output"
        report = grade_output(
            task_spec, test_output, submission_id, timed_out, runtime
        )
        report.submitted_at = submitted_at
        report.resource_diagnostics = resources
        report.score_0_100 = rescale_score(task_spec.judge.rescale, report.score)
        stage = "record_report"
        (log_dir / "report.json").write_text(
            json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
        )
        logger.info(
            f"Result: {report.passed}/{report.total_tests} passed "
            f"(pass_rate={report.pass_rate:.2%})"
        )
        return report

    except Exception as e:
        # An empty EvalReport defaults to valid=True. Returning it here makes
        # the async judge mark an unevaluated submission COMPLETED. Let its
        # existing ERROR transition retain the request identity instead. Raw
        # backend details stay in the private log, not solver-visible history.
        logger.exception("Judge execution failed during %s", stage)
        if not resources_observed:
            resources = _resource_observation(backend, handle, logger)
        diagnostic = f"; {resources.summary()}" if resources is not None else ""
        raise EvaluationError(
            task_spec.task_id, f"judge_execution_failed:{stage}{diagnostic}", logger,
        ) from e
    finally:
        try:
            backend.cleanup_container(handle, logger)
        finally:
            close_logger(logger)
