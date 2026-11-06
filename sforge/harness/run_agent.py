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

"""Agent execution with iterative evaluation via judge server.

The agent runs in a work container with:
- sforge-submit script for on-demand evaluation
- Stop hook to prevent premature exit (unconditional block)
- Auto-eval runs on the host side (invisible to agent)

Architecture:
    Host: sforge serve (judge HTTP server, started separately)
          + auto-eval thread (extracts code from container, submits to judge)
    Container: Agent + sforge-submit (curl-based)
    Communication: HTTP (container → host judge server)
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import shlex
import signal
import tarfile as _tarfile
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import requests

from sforge.harness.agent import Agent
from sforge.harness.backend import ContainerBackend, ContainerHandle
from sforge.harness.config import SForgeConfig, sanitize_e2b_proxy_env
from sforge.harness.constants import get_admin_secret
from sforge.harness.docker_build import (
    close_logger,
    setup_logger,
)
from sforge.harness.evolve_scripts import (
    generate_evolve_prompt,
    generate_game_prompt,
    generate_submit_script,
)
from sforge.harness.restored_registration import RestoredExecution, RestoredRegistration
from sforge.harness.runtime_attestation import RuntimeAttestationRecorder
from sforge.harness.task_spec import TaskSpec

_CREDENTIAL_PLACEHOLDER = "sforge-host-credential"
_USE_CONFIG_VALUE = object()


@dataclass
class RunResult:
    """Result of running an agent on a task."""

    archive: bytes = b""
    best_pass_rate: float = 0.0
    best_score: float | None = None
    best_round: str = ""
    total_rounds: int = 0
    agent_submissions: int = 0
    auto_submissions: int = 0
    agent_output: str = ""
    timed_out: bool = False
    runtime_seconds: float = 0.0
    resume_count: int = 0
    grading_status: str = "unknown"
    grading_reason: str = "not_finalized"
    solver_execution: dict | None = None

    def to_dict(self) -> dict:
        d = {
            "best_pass_rate": self.best_pass_rate,
            "best_round": self.best_round,
            "total_rounds": self.total_rounds,
            "agent_submissions": self.agent_submissions,
            "auto_submissions": self.auto_submissions,
            "timed_out": self.timed_out,
            "runtime_seconds": self.runtime_seconds,
            "archive_size_bytes": len(self.archive),
            "resume_count": self.resume_count,
            "grading_status": self.grading_status,
            "grading_reason": self.grading_reason,
            "solver_execution": self.solver_execution,
        }
        if self.best_score is not None:
            d["best_score"] = self.best_score
        return d


# ---------------------------------------------------------------------------
# Environment and command helpers
# ---------------------------------------------------------------------------


def _build_agent_env(
    agent: Agent,
    model: str | None = None,
    *,
    api_key_override: str | None | object = _USE_CONFIG_VALUE,
    api_base_url_override: str | None | object = _USE_CONFIG_VALUE,
) -> dict[str, str]:
    """Build environment variables dict for an agent container."""
    config = agent._config
    env: dict[str, str] = {}

    if config.http_proxy:
        env["http_proxy"] = config.http_proxy
        env["HTTP_PROXY"] = config.http_proxy
    if config.https_proxy:
        env["https_proxy"] = config.https_proxy
        env["HTTPS_PROXY"] = config.https_proxy
    if config.no_proxy:
        env["no_proxy"] = config.no_proxy
        env["NO_PROXY"] = config.no_proxy

    api_key = (
        config.agent_api_key
        if api_key_override is _USE_CONFIG_VALUE
        else api_key_override
    )
    if api_key:
        env[agent.api_key_env] = str(api_key)

    api_base_url = (
        config.agent_api_base_url
        if api_base_url_override is _USE_CONFIG_VALUE
        else api_base_url_override
    )
    if api_base_url and agent.api_base_env:
        env[agent.api_base_env] = str(api_base_url)

    effective_model = model or config.agent_model or agent.default_model
    if effective_model and agent.model_env:
        env[agent.model_env] = effective_model

    if config.nodejs_mirror_url:
        env["SFORGE_NODEJS_MIRROR_URL"] = config.nodejs_mirror_url
    if config.npm_registry_url:
        env["npm_config_registry"] = config.npm_registry_url

    env.update(config.agent_extra_env)

    agent.augment_env(env, model)

    # Agent hooks and extra_env may populate aliases for the same credential.
    # Apply host-gateway overrides last so no real provider secret remains in
    # the container environment.
    if api_key_override is not _USE_CONFIG_VALUE:
        credential_names = set(agent.credential_env_names)
        credential_names.add(agent.api_key_env)
        for name in credential_names:
            env.pop(name, None)
        if api_key_override:
            env[agent.api_key_env] = str(api_key_override)
    if api_base_url_override is not _USE_CONFIG_VALUE and agent.api_base_env:
        if api_base_url_override:
            env[agent.api_base_env] = str(api_base_url_override)
        else:
            env.pop(agent.api_base_env, None)

    return env


def _prepare_credential_proxy(
    agent: Agent,
    config: SForgeConfig,
    backend: ContainerBackend,
):
    """Prepare a Docker-hosted credential gateway when the protocol is known."""
    if backend.backend_name != "docker" or not config.agent_api_key:
        return None, None
    headers = agent.credential_proxy_headers(config.agent_api_key)
    if not headers:
        return None, None
    target_url = config.agent_api_base_url or agent.default_api_base_url
    if not target_url or not agent.api_base_env:
        raise ValueError(
            f"Agent {agent.name!r} cannot use host credential isolation without "
            "an API base URL environment"
        )

    from sforge.harness.api_proxy import APIProxy

    proxy = APIProxy(
        target_url=target_url,
        http_proxy=config.http_proxy,
        https_proxy=config.https_proxy,
        host="0.0.0.0",
        injected_headers=headers,
        rate_limit_retry_seconds=config.agent_rate_limit_retry_seconds,
    )
    return proxy, f"http://host.docker.internal:{proxy.port}"


def _e2b_install_env(env: dict[str, str], agent: Agent) -> dict[str, str]:
    """Keep only non-secret values required while installing the agent."""
    allowed = {
        "http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY",
        "no_proxy", "NO_PROXY", "SFORGE_NODEJS_MIRROR_URL",
        "npm_config_registry",
    }
    if agent.api_base_env:
        allowed.add(agent.api_base_env)
    if agent.model_env:
        allowed.add(agent.model_env)
    return {key: value for key, value in env.items() if key in allowed}


# ---------------------------------------------------------------------------
# Container setup helpers
# ---------------------------------------------------------------------------


def _install_task_file(
    backend: ContainerBackend,
    handle: ContainerHandle,
    source: Path,
    destination: PurePosixPath,
    *,
    executable: bool = False,
) -> None:
    """Install solver-visible data without inheriting private host permissions.

    Archive transfer can preserve the host UID and a restrictive umask. Only
    these generated, non-secret task files are made readable in the container;
    the host copy and general file-transfer semantics remain unchanged.
    """
    backend.copy_to_container(handle, source, destination)
    mode = "0755" if executable else "0644"
    result = backend.exec_run(
        handle, ["chmod", mode, str(destination)], user="root"
    )
    if result.exit_code != 0:
        raise RuntimeError(f"Cannot install task file permissions: {destination}")
    # Test as the container's configured execution user, not as the privileged
    # installer. Docker/Kubernetes preserve the image default; E2B stores the
    # runner-selected ``agent`` user on the handle and resolves it from user=None.
    # Scripts require read permission as well as execute permission for their
    # interpreter to work.
    for permission in (["-r", "-x"] if executable else ["-r"]):
        result = backend.exec_run(
            handle, ["test", permission, str(destination)]
        )
        if result.exit_code != 0:
            raise RuntimeError(f"Task file unavailable to solver: {destination}")


def _install_tools(
    backend: ContainerBackend,
    handle: ContainerHandle,
    task_spec: TaskSpec,
    agent: Agent,
    log_dir: Path,
    logger,
    disable_stop_hook: bool = False,
) -> None:
    """Install sforge-submit and agent stop hooks."""

    # 1. Install sforge-submit script
    submit_script = generate_submit_script()
    local_submit = log_dir / "_sforge-submit.sh"
    local_submit.write_text(submit_script)
    _install_task_file(
        backend, handle, local_submit, PurePosixPath("/usr/local/bin/sforge-submit"),
        executable=True,
    )
    logger.info("Installed sforge-submit script")

    # 2. Install agent-specific stop hook (unless disabled)
    if not disable_stop_hook:
        agent.install_stop_hook(backend, handle, log_dir, logger)
    else:
        logger.info("Stop hook disabled by flag")


def _extract_archive_from_container(
    backend: ContainerBackend,
    handle: ContainerHandle,
    task_spec: TaskSpec,
    *,
    shutdown_event: threading.Event | None = None,
) -> bytes:
    """Extract the current submission archive from a running work container."""
    remote_archive = PurePosixPath(
        f"/tmp/.sforge-submission-{uuid.uuid4().hex}.tar.gz"
    )
    path_checks = "\n".join(
        "if [[ -e {path} || -L {path} ]]; then "
        "submit_paths+=({path}); fi".format(path=shlex.quote(path))
        for path in task_spec.submit_paths
    )
    excludes = " ".join(
        shlex.quote(f"--exclude={pattern}")
        for pattern in task_spec.submit_exclude
    )
    tar_cmd = f"""
cd -- {shlex.quote(task_spec.cwd)} || exit $?
submit_paths=()
{path_checks}
if (( ${{#submit_paths[@]}} )); then
    tar czf {shlex.quote(str(remote_archive))} --exclude=.git {excludes} -- "${{submit_paths[@]}}"
else
    tar czf {shlex.quote(str(remote_archive))} --files-from /dev/null
fi
"""
    try:
        result = backend.exec_run_with_exit_code(
            handle, ["/bin/bash", "-c", tar_cmd], timeout=300,
        )
        if result.timed_out:
            raise RuntimeError("Timed out while creating the submission archive")
        if result.exit_code != 0:
            raise RuntimeError(
                "Could not create the submission archive: "
                + result.output[-4000:]
            )
        if backend.backend_name == "e2b":
            raw = backend.copy_from_container(
                handle, remote_archive, shutdown_event=shutdown_event,
            )
        else:
            raw = backend.copy_from_container(handle, remote_archive)
    finally:
        try:
            backend.exec_run(
                handle, ["rm", "-f", str(remote_archive)], user="root",
            )
        except Exception:
            pass
    with _tarfile.open(fileobj=io.BytesIO(raw)) as outer:
        members = [member for member in outer.getmembers() if member.isfile()]
        if len(members) != 1:
            raise RuntimeError(
                f"Unexpected archive wrapper: expected 1 file, got {len(members)}"
            )
        source = outer.extractfile(members[0])
        if source is None:
            raise RuntimeError("Could not read the submission archive wrapper")
        archive = source.read()
    return archive


def _auto_eval_loop(
    backend: ContainerBackend,
    handle: ContainerHandle,
    task_spec: TaskSpec,
    host_judge_url: str,
    session_token: str,
    eval_interval: int,
    stop_event: threading.Event,
    logger,
    log_dir: Path,
    admin_secret: str,
) -> None:
    """Host-side auto-eval: periodically extract code and submit to judge.

    Runs as a daemon thread. Production is **back-pressured**: the next snapshot is
    only taken after the previous auto submission reached a terminal state, so a long-running judge cannot accumulate overlapping
    auto work. It still never polls for *results* — it only observes terminality.
    Submissions already accepted are always finished by the judge; stopping the
    wait early hands them to the drain, they are never abandoned.
    Writes tick entries to auto_eval_ticks.log for post-mortem inspection.
    """
    ticks_log = log_dir / "auto_eval_ticks.log"
    previous: str | None = None
    while True:
        stop_event.wait(eval_interval)
        if stop_event.is_set():
            break
        # Back-pressure: wait for the previous auto submission to finish before
        # producing the next snapshot. Bounded, and abandoned on stop so shutdown
        # stays prompt; the drain then finishes whatever was already accepted.
        if previous is not None:
            previous = _await_previous_auto(
                host_judge_url, session_token, previous, stop_event, logger,
                ticks_log, eval_interval,
            )
            if stop_event.is_set():
                break
            if previous is not None:
                # Still not terminal: do NOT take a new snapshot. Falling through
                # here is what made the back-pressure cosmetic - the caller posted a
                # fresh archive and overwrote `previous`, so nothing was ever bounded.
                continue
        try:
            archive = _extract_archive_from_container(
                backend, handle, task_spec, shutdown_event=stop_event,
            )
            if stop_event.is_set():
                break
            resp = requests.post(
                f"{host_judge_url}/api/v1/submit",
                data={
                    "token": session_token,
                    "kind": "auto",
                    "admin_secret": admin_secret,
                },
                files={"archive": ("archive.tar.gz", archive, "application/gzip")},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            previous = data.get("submission_id")
            ts = time.strftime("%Y-%m-%dT%H:%M:%S")
            with open(ticks_log, "a") as f:
                f.write(f"[{ts}] submitted {len(archive)} bytes -> {data.get('submission_id', '?')} round={data.get('round_id', '?')}\n")
        except Exception as e:
            ts = time.strftime("%Y-%m-%dT%H:%M:%S")
            try:
                with open(ticks_log, "a") as f:
                    f.write(f"[{ts}] error: {e}\n")
            except Exception:
                pass
            logger.debug("Auto-eval tick failed: %s", e)


def _await_previous_auto(host_judge_url, session_token, submission_id, stop_event, logger,
                         ticks_log, wait_budget) -> str | None:
    """Wait until `submission_id` reaches a terminal state, or the budget runs out.

    Returns the submission id when it is still not terminal (so the caller keeps
    waiting on the same one) or None once it finished. Never abandons an accepted
    submission: it only decides when the *next* snapshot may be taken.
    """
    deadline = time.time() + max(0.0, float(wait_budget))
    while True:
        if stop_event.is_set():
            return None
        try:
            resp = requests.get(
                f"{host_judge_url}/api/v1/result/{submission_id}",
                params={"token": session_token}, timeout=30,
            )
            resp.raise_for_status()
            status = (resp.json() or {}).get("status")
        except Exception as e:  # noqa: BLE001 - a transient poll failure must not
            status = None          # turn into an auto-submission storm
            logger.debug("Auto-eval back-pressure poll failed: %s", e)
        if status in ("completed", "error"):
            return None
        if time.time() >= deadline:
            ts = time.strftime("%Y-%m-%dT%H:%M:%S")
            try:
                with open(ticks_log, "a") as f:
                    f.write(f"[{ts}] back-pressure: {submission_id} still {status}; deferring next snapshot\n")
            except Exception:
                pass
            return submission_id
        if stop_event.wait(5.0):
            return None


@dataclass
class _GradingResult:
    history: dict | None
    status: str
    reason: str
    pending_submissions: int | None = None
    elapsed_seconds: float = 0.0
    wait_budget_seconds: float = 0.0


def _read_grading_history(host_judge_url: str, session_token: str, admin_secret: str,
                          *, timeout: float = 10) -> tuple[dict, int]:
    response = requests.get(
        f"{host_judge_url}/api/v1/history",
        params={"token": session_token, "admin_secret": admin_secret},
        timeout=timeout,
    )
    response.raise_for_status()
    history = response.json()
    if not isinstance(history, dict) or not isinstance(history.get("entries"), list):
        raise TypeError("Invalid grading history")
    # Validate the official summary before consuming it in success/error paths.
    # This is shape validation only; score selection remains owned by the judge.
    for key in ("best_score", "best_pass_rate"):
        value = history.get(key)
        if value is None and key == "best_score":
            continue
        if key not in history:
            continue
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("Invalid grading summary score")
    for key in ("agent_submissions", "auto_submissions"):
        value = history.get(key, 0)
        if type(value) is not int or value < 0:
            raise ValueError("Invalid grading summary count")
    if not isinstance(history.get("best_round", ""), str):
        raise TypeError("Invalid grading summary round")
    pending = 0
    for entry in history["entries"]:
        if not isinstance(entry, dict):
            raise TypeError("Invalid grading history entry")
        if entry.get("type", "submission") not in ("submission", "game"):
            raise ValueError("Unknown grading history entry type")
        status = entry.get("status")
        if status in ("running", "queued"):
            pending += 1
        elif status is None:
            if not any(key in entry for key in ("score", "pass_rate", "max_score")):
                raise ValueError("Unrecognized legacy grading entry")
        elif status not in ("completed", "error"):
            raise ValueError("Unknown grading history status")
    return history, pending


def _wait_for_grading(host_judge_url: str, session_token: str, admin_secret: str,
                      *, eval_timeout: int, shutdown_event: threading.Event | None,
                      logger, incomplete_reason: str | None = None,
                      max_wait_seconds: float | None = None,
                      history_reader=None) -> _GradingResult:
    # eval_timeout covers execution only. Keep a bounded allowance for judge
    # container setup, report persistence and cleanup, in addition to that budget.
    if type(eval_timeout) not in (int, float) or not math.isfinite(eval_timeout) or eval_timeout < 0:
        raise ValueError("eval_timeout must be finite and non-negative")
    if max_wait_seconds is not None and (
        type(max_wait_seconds) not in (int, float)
        or not math.isfinite(max_wait_seconds) or max_wait_seconds <= 0
    ):
        raise ValueError("max_wait_seconds must be finite and positive")
    per_evaluation_budget = max(900.0, float(eval_timeout) + 600.0)
    budget = per_evaluation_budget
    hard_deadline = (
        time.monotonic() + max_wait_seconds
        if max_wait_seconds is not None else None
    )
    queue_budget_frozen = False
    previously_queue_only = False
    read_history = history_reader or _read_grading_history
    started = time.monotonic()
    deadline = started + budget
    last_accounted = started
    history = None
    pending = None
    last_read_ok = False
    while True:
        now = time.monotonic()
        if previously_queue_only:
            deadline += max(0.0, now - last_accounted)
        last_accounted = now
        effective_deadline = min(deadline, hard_deadline) if hard_deadline is not None else deadline
        remaining = effective_deadline - now
        if remaining <= 0:
            break
        try:
            observed, count = read_history(
                host_judge_url, session_token, admin_secret,
                timeout=min(10.0, remaining),
            )
            history, pending = observed, count
            last_read_ok = True
        except (requests.RequestException, ValueError, TypeError) as exc:
            last_read_ok = False
            # Request exceptions can contain token-bearing URLs.
            logger.warning("Grading history unavailable (%s)", type(exc).__name__)
        now = time.monotonic()
        if previously_queue_only:
            deadline += max(0.0, now - last_accounted)
        last_accounted = now
        pending_statuses = []
        if last_read_ok:
            pending_statuses = [
                entry.get("status")
                for entry in history["entries"]
                if entry.get("status") in ("queued", "running")
            ]
        queue_only = bool(pending_statuses) and all(
            status == "queued" for status in pending_statuses
        )
        interrupted = shutdown_event is not None and shutdown_event.is_set()
        if incomplete_reason or interrupted:
            return _GradingResult(
                history, "pending" if last_read_ok and pending else "unknown",
                incomplete_reason or "interrupted", pending if last_read_ok else None,
                now - started,
                min(budget, max_wait_seconds) if max_wait_seconds is not None else budget,
            )
        # requests' timeout bounds socket inactivity, not total response time.
        # A late response can preserve evidence but cannot qualify this drain.
        effective_deadline = min(deadline, hard_deadline) if hard_deadline is not None else deadline
        if now > effective_deadline:
            break
        if last_read_ok and not queue_budget_frozen:
            # Producers must already be stopped. Allow serial completion of the
            # queue observed once, keeping the original start time. Later polls
            # never add work admitted after producer shutdown.
            budget = per_evaluation_budget * max(1, pending)
            deadline = started + budget
            queue_budget_frozen = True
        if last_read_ok and pending == 0:
            return _GradingResult(history, "complete", "drained", 0,
                                  now - started,
                                  min(budget, max_wait_seconds) if max_wait_seconds is not None else budget)
        previously_queue_only = queue_only
        effective_deadline = min(deadline, hard_deadline) if hard_deadline is not None else deadline
        remaining = effective_deadline - now
        if remaining <= 0:
            break
        delay = min(15.0, remaining)
        if shutdown_event is None:
            time.sleep(delay)
        else:
            shutdown_event.wait(delay)
    return _GradingResult(
        history, "pending" if last_read_ok and pending else "unknown",
        "deadline" if last_read_ok else "history_unavailable",
        pending if last_read_ok else None, time.monotonic() - started,
        min(budget, max_wait_seconds) if max_wait_seconds is not None else budget,
    )


def _write_grading(log_dir: Path, history_file: str, result: _GradingResult,
                   run_id: str) -> dict:
    # A failed read is explicit in the sidecar; retain the last successful
    # snapshot rather than replacing a known partial score with an empty result.
    history = result.history if result.history is not None else {
        "run_id": run_id, "best_score": None, "best_pass_rate": 0.0,
        "best_round": "", "agent_submissions": 0, "auto_submissions": 0,
        "entries": [],
    }
    raw = json.dumps(history, indent=2, ensure_ascii=False).encode("utf-8")
    path = log_dir / history_file
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(raw)
    temp.replace(path)
    status = {
        "schema_version": 1,
        "status": result.status,
        "reason": result.reason,
        "pending_submissions": result.pending_submissions,
        "elapsed_seconds": result.elapsed_seconds,
        "wait_budget_seconds": result.wait_budget_seconds,
        "history_file": history_file,
        "history_observed": result.history is not None,
        "history_sha256": hashlib.sha256(raw).hexdigest(),
    }
    temp = log_dir / "grading_status.json.tmp"
    temp.write_text(json.dumps(status, indent=2), encoding="utf-8")
    temp.replace(log_dir / "grading_status.json")
    return history


def _write_solver_execution(log_dir: Path, execution: dict) -> None:
    """Persist the latest receipt, including an in-flight segment before dispatch."""
    temp = log_dir / "solver_execution.json.tmp"
    temp.write_text(json.dumps(execution, indent=2), encoding="utf-8")
    temp.replace(log_dir / "solver_execution.json")


def _solver_segment_result(result, stop_requested: bool) -> dict:
    code = getattr(result, "exit_code", None)
    native_code = getattr(result, "native_exit_code", None)
    observed = getattr(result, "terminal_observed", None)
    term_signal = getattr(result, "termination_signal", None)
    code = code if type(code) is int else None
    native_code = native_code if type(native_code) is int else None
    observed = observed if type(observed) is bool else None
    # A shell return value of 128+N can also be an explicit exit code. Preserve
    # signal evidence only when the backend independently supplies it.
    term_signal = (term_signal if observed is True and type(term_signal) is int
                   and 0 < term_signal < signal.NSIG else None)
    if result.timed_out:
        status = "cancelled" if stop_requested else "timed_out"
    elif observed is True and term_signal is not None:
        status = "signaled"
    elif observed is True and native_code is not None and native_code >= 0:
        status = "normal_exit" if native_code == 0 else "nonzero_exit"
    else:
        status = "unknown"
    return {
        "status": status, "returned_exit_code": code,
        "native_exit_code": native_code, "terminal_observed": observed,
        "termination_signal": term_signal, "timed_out": result.timed_out,
        "stop_requested": stop_requested, "elapsed_seconds": result.elapsed_seconds,
        "finished_at": time.time(),
    }


def _runtime_protocol(
    *, agent: Agent, backend: ContainerBackend, model: str | None,
    effective_timeout: int, eval_interval: int, disable_stop_hook: bool,
    disable_auto_eval: bool, disable_auto_resume: bool, internet: bool,
    max_submissions: int | None, submission_cooldown: int | None,
) -> dict:
    """Return the bounded control protocol that the runner will enforce."""
    return {
        "agent": agent.name,
        "model": model or agent._config.agent_model or agent.default_model,
        "backend": backend.backend_name,
        "solver_timeout_seconds": effective_timeout,
        "stop_hook_enabled": not disable_stop_hook and agent.stop_hook is not None,
        "auto_resume_enabled": not disable_auto_resume and agent.resume_cmd is not None,
        "internet_enabled": internet,
        "background_evaluation": {
            "enabled": not disable_auto_eval and eval_interval > 0,
            "interval_seconds": eval_interval,
        },
        "active_evaluation": {
            "max_submissions": max_submissions,
            "cooldown_seconds": submission_cooldown,
        },
    }


def _record_terminal_attestation(
    recorder: RuntimeAttestationRecorder | None,
    log_dir: Path,
    result: RunResult,
) -> None:
    if recorder is None:
        return
    grading_path = log_dir / "grading_status.json"
    evaluation: dict = {}
    if grading_path.is_file():
        try:
            value = json.loads(grading_path.read_text())
            if isinstance(value, dict):
                evaluation = {
                    key: value.get(key) for key in (
                        "status", "reason", "pending_submissions",
                        "elapsed_seconds", "wait_budget_seconds",
                        "history_observed", "history_sha256",
                    )
                }
        except (OSError, ValueError):
            evaluation = {"status": "unreadable"}
    evidence = {
        name: log_dir / name for name in (
            "solver_execution.json", "grading_status.json", "run_history.json",
            "game_history.json", "worker-profile.json", "final_archive.tar.gz",
            "feedback-transport.jsonl",
        )
    }
    recorder.record(
        "terminal",
        "complete" if result.grading_status == "complete" else "incomplete",
        {
            "solver": {
                "timed_out": result.timed_out,
                "runtime_seconds": result.runtime_seconds,
                "resume_count": result.resume_count,
                "execution_status": (result.solver_execution or {}).get("status"),
            },
            "evaluation": evaluation,
            "outcome": {
                "grading_status": result.grading_status,
                "grading_reason": result.grading_reason,
                "agent_submissions": result.agent_submissions,
                "background_submissions": result.auto_submissions,
                "best_score": result.best_score,
                "best_pass_rate": result.best_pass_rate,
            },
        },
        evidence_files=evidence,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run_agent(
    task_spec: TaskSpec,
    agent: Agent,
    config: SForgeConfig,
    backend: ContainerBackend,
    run_id: str,
    model: str | None = None,
    timeout: int | None = None,
    judge_url: str = "http://host.docker.internal:8080",
    eval_interval: int = 300,
    disable_stop_hook: bool = False,
    disable_auto_eval: bool = False,
    disable_auto_resume: bool = False,
    internet: bool = True,
    verbose: bool = False,
    shutdown_event: threading.Event | None = None,
    grading_shutdown_event: threading.Event | None = None,
    max_submissions: int | None = None,
    submission_cooldown: int | None = None,
    restored_registration: RestoredRegistration | None = None,
    host_registration_callback: Callable[[str], None] | None = None,
) -> RunResult:
    """Run an agent on a task with iterative evaluation.

    Requires a running judge server (started via `sforge serve`).

    1. Build work + judge images
    2. Create work container (with judge URL in env)
    3. Install agent + tools (submit script, stop hook, auto-eval daemon)
    4. Run agent (high max-turns for iterative work)
    5. Read final state, extract archive

    ``restored_registration`` opts into an already activated native checkpoint.
    Supply the controller's original absolute activation deadline, not a new
    duration. Setup and every resume consume that deadline; terminal collection
    may drain previously accepted evaluations after work stops. Mismatched or
    unavailable authority fails before work, with no cold-registration fallback.
    The Agent must install its corresponding retained code/session/research state
    through its ordinary installation path. Judge recovery alone does not prove
    full worker continuity. This entry neither creates nor restores authority.
    Periodic auto-evaluation must be disabled for this agent-only continuation.

    ``host_registration_callback`` receives the run-scoped judge token after
    registration and before container creation.  It is a host integration seam
    for read-only observers; the callback must never forward the token into the
    solver environment or persist it in run artifacts.

    ``grading_shutdown_event`` lets an external controller stop solver work while
    allowing already accepted evaluations to drain. When omitted, grading uses
    ``shutdown_event`` as before. A caller that supplies a distinct event remains
    responsible for setting it when scorer drain must also stop.
    """
    # Warm admission is explicit and does not invoke checkpoint restore itself.
    # The caller must admit the matching worker state and arrange its installation.
    if restored_registration is not None and (backend.backend_name != "docker" or task_spec.game_mode):
        raise ValueError("Restored registration execution requires a non-game Docker task")
    if restored_registration is not None and timeout is not None:
        raise ValueError("Restored execution uses its original absolute deadline, not a new timeout")
    if restored_registration is not None and not disable_auto_eval and eval_interval > 0:
        raise ValueError("Restored agent-only execution requires automatic evaluation disabled")
    effective_timeout = timeout if timeout is not None else config.agent_timeout
    if effective_timeout is None:
        effective_timeout = agent.timeout
    if type(effective_timeout) is not int or effective_timeout <= 0:
        raise ValueError("Agent execution timeout must be a positive integer in seconds")
    effective_grading_shutdown = (
        shutdown_event if grading_shutdown_event is None else grading_shutdown_event
    )

    log_dir = config.log_dir / "runs" / run_id / task_spec.task_id
    log_dir.mkdir(parents=True, exist_ok=True)
    # Admit exclusively before opening any run artifacts. Rejected retries must
    # neither truncate evidence nor close a concurrent invocation's log handlers.
    restored_execution = (
        RestoredExecution(restored_registration, log_dir, run_id, task_spec.task_id, shutdown_event)
        if restored_registration is not None else None
    )
    try:
        logger = setup_logger(
            f"agent.{task_spec.task_id}.{run_id}",
            log_dir / "run_agent.log",
            mode="a" if restored_execution else "w",
            verbose=verbose,
        )
    except BaseException:
        if restored_execution is not None:
            restored_execution.close()
        raise

    # judge_url is for container use; derive a host-local URL for registration/polling
    parsed_judge = urlparse(judge_url)
    if parsed_judge.hostname == "host.docker.internal":
        host_judge_url = judge_url.replace("host.docker.internal", "127.0.0.1")
    elif backend.backend_name == "k8s":
        # judge_url points to VPC IP (for pods); host talks to judge server locally
        host_judge_url = f"http://127.0.0.1:{parsed_judge.port or 8080}"
    else:
        host_judge_url = judge_url

    handle = None
    net_isolation = None
    credential_proxy = None
    credential_proxy_url = None
    # Cleanup and interruption handlers can run before installation completes.
    auto_eval_stop = None
    auto_eval_thread = None
    auto_eval_started = False
    auto_eval_joined = False
    resume_count = 0
    install_parts: list[str] = []
    admin_secret = ""
    session_token = None
    solver_execution = {"schema_version": 1, "status": "not_started", "segments": []}
    runtime_attestation = None

    if config.runtime_attestation_key_file is not None:
        try:
            runtime_attestation = RuntimeAttestationRecorder(
                log_dir,
                key_file=config.runtime_attestation_key_file,
                runner_source_revision=config.runner_source_revision or "",
                run_id=run_id,
                task_id=task_spec.task_id,
                resume=restored_execution is not None,
                protocol=_runtime_protocol(
                    agent=agent, backend=backend, model=model,
                    effective_timeout=effective_timeout, eval_interval=eval_interval,
                    disable_stop_hook=disable_stop_hook,
                    disable_auto_eval=disable_auto_eval,
                    disable_auto_resume=disable_auto_resume, internet=internet,
                    max_submissions=max_submissions,
                    submission_cooldown=submission_cooldown,
                ),
            )
        except BaseException:
            if restored_execution is not None:
                restored_execution.close()
            close_logger(logger)
            raise

    try:
        _write_solver_execution(log_dir, solver_execution)
        if runtime_attestation is not None:
            runtime_attestation.record(
                "admission", "accepted",
                {"restored_execution": restored_execution is not None},
                evidence_files={"solver_execution": log_dir / "solver_execution.json"},
            )
        admin_secret = get_admin_secret(config.log_dir)
        # 0. Clean up stale iptables chains from previous runs that were killed
        if backend.backend_name == "docker":
            from sforge.harness.network_isolation import cleanup_stale_chains
            cleanup_stale_chains(logger)

        # 1. Check images exist (must run `sforge build` first)
        for image_key in (task_spec.work_image_key, task_spec.judge_image_key):
            if not backend.image_exists(image_key):
                if backend.backend_name == "e2b":
                    raise RuntimeError(
                        f"E2B Template for {image_key!r} was not found. Make sure "
                        "the task uses a published image version with an official "
                        "Template and SFORGE_E2B_TEMPLATE_NAMESPACE is set, or "
                        "point SFORGE_E2B_TEMPLATE_MAP at a self-built Template."
                    )
                raise RuntimeError(
                    f"Image '{image_key}' not found. Run `sforge pull --task {task_spec.task_id}` to fetch from registry, or `sforge build --task {task_spec.task_id}` to build locally."
                )
        logger.info("Images ready")

        if restored_execution is not None:
            registered = restored_execution.registration_result(host_judge_url, admin_secret)
            for key, expected in (
                ("max_agent_submissions", max_submissions),
                ("submission_cooldown", submission_cooldown),
                ("judge_cpu_limit", config.judge_cpu_limit),
                ("judge_mem_limit", config.judge_mem_limit),
            ):
                if expected is not None and registered.get(key) != expected:
                    raise ValueError(f"Restored registration conflicts with requested {key}")
            session_token = registered["token"]
            feedback_relay_id = registered["feedback_relay_id"]
            max_submissions = registered["max_agent_submissions"]
            submission_cooldown = registered["submission_cooldown"]
            logger.info("Reusing the exact live restored judge registration")
        else:
            # 1b. Register session with judge server
            reg_body: dict = {"task_id": task_spec.task_id, "run_id": run_id, "admin_secret": admin_secret}
            if config.judge_cpu_limit is not None:
                reg_body["judge_cpu_limit"] = config.judge_cpu_limit
            if config.judge_mem_limit is not None:
                reg_body["judge_mem_limit"] = config.judge_mem_limit
            if max_submissions is not None:
                reg_body["max_agent_submissions"] = max_submissions
            if submission_cooldown is not None:
                reg_body["submission_cooldown"] = submission_cooldown
            if backend.backend_name == "k8s":
                reg_body["backend"] = backend.backend_name
                reg_body["k8s_image_registry"] = config.k8s_image_registry
                reg_body["k8s_namespace"] = config.k8s_namespace
                if config.k8s_node_selector:
                    reg_body["k8s_node_selector"] = config.k8s_node_selector
                if config.k8s_kubeconfig:
                    reg_body["k8s_kubeconfig"] = config.k8s_kubeconfig
            elif backend.backend_name == "e2b":
                reg_body["backend"] = "e2b"
                reg_body["e2b_template_map"] = config.e2b_template_map
                reg_body["e2b_template_namespace"] = config.e2b_template_namespace
                reg_body["e2b_sandbox_ttl"] = config.e2b_sandbox_ttl
            for _reg_attempt in range(1, 6):
                try:
                    reg_resp = requests.post(
                        f"{host_judge_url}/api/v1/register",
                        json=reg_body,
                        timeout=10,
                    )
                    reg_resp.raise_for_status()
                    break
                except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                    if _reg_attempt == 5:
                        raise
                    wait = 2 * _reg_attempt
                    logger.warning("Register attempt %d/5 failed (%s), retrying in %ds...", _reg_attempt, exc, wait)
                    time.sleep(wait)
            registration = reg_resp.json()
            session_token = registration["token"]
            feedback_relay_id = registration["feedback_relay_id"]
            logger.info("Registered session with judge server")

        if host_registration_callback is not None:
            if not isinstance(session_token, str) or not session_token:
                raise RuntimeError("Judge registration did not return a session token")
            host_registration_callback(session_token)

        if restored_execution is not None:
            restored_execution.remaining()

        # 2. Create container
        container_name = f"sforge.run.{task_spec.task_id}.{run_id}"

        backend.remove_container_by_name(container_name)

        credential_proxy, credential_proxy_url = _prepare_credential_proxy(
            agent, config, backend,
        )
        if credential_proxy is not None:
            env = _build_agent_env(
                agent,
                model,
                api_key_override=_CREDENTIAL_PLACEHOLDER,
                api_base_url_override=credential_proxy_url,
            )
        else:
            env = _build_agent_env(agent, model)
        if backend.backend_name == "e2b":
            sanitize_e2b_proxy_env(env, internet=internet)
        env["SFORGE_FEEDBACK_URL"] = (
            judge_url.rstrip("/")
            + f"/api/v1/feedback/{feedback_relay_id}"
        )
        env["SFORGE_PATCH_DIR"] = task_spec.cwd
        env["SFORGE_SUBMIT_PATHS"] = " ".join(task_spec.submit_paths)
        env["SFORGE_SUBMIT_EXCLUDE_FLAGS"] = " ".join(
            f"--exclude={e}" for e in task_spec.submit_exclude
        )

        if task_spec.game_mode:
            game_api_base = (
                judge_url.rstrip("/")
                + f"/api/v1/game/{run_id}/{task_spec.task_id}"
            )
            env["GAME_SERVER_URL"] = game_api_base

        # Ensure judge server URL bypasses proxy
        bypass_hosts = {urlparse(judge_url).hostname or ""}
        if credential_proxy_url:
            bypass_hosts.add(urlparse(credential_proxy_url).hostname or "")
        for key in ("NO_PROXY", "no_proxy"):
            existing = env.get(key, "")
            parts = {part.strip() for part in existing.split(",") if part.strip()}
            parts.update(host for host in bypass_hosts if host)
            if parts:
                env[key] = ",".join(sorted(parts))

        # Network isolation: preflight + extra_hosts pre-resolve
        container_extra_hosts = {"host.docker.internal": "host-gateway"}
        container_cap_drop: list[str] = []

        if not internet and backend.backend_name == "docker":
            # Docker-only preflight: isolation runs as host iptables rules, so
            # it needs sudo and conflicts with host proxy env leaking into the
            # container. Remote backends (k8s, e2b) isolate inside the cluster/
            # provider and are unaffected by either.
            from sforge.harness.network_isolation import (
                check_iptables_permission,
            )

            if (config.http_proxy or config.https_proxy) and credential_proxy is None:
                raise RuntimeError(
                    "Network isolation (internet=false) is not compatible with "
                    "direct proxy configuration. Start a local API proxy first:\n"
                    "  python -m sforge proxy --target <YOUR_API_URL>\n"
                    "Then set SFORGE_AGENT_API_BASE_URL="
                    "http://host.docker.internal:9090 "
                    "and unset HTTP_PROXY/HTTPS_PROXY before running."
                )
            if not check_iptables_permission():
                raise RuntimeError(
                    "Network isolation requires sudo iptables access. "
                    "Ensure passwordless sudo is configured for iptables."
                )

        if not internet:
            from sforge.harness.network_isolation import (
                is_ip_address,
                resolve_hostname,
            )

            api_url = (
                credential_proxy_url
                or config.agent_api_base_url
                or agent.default_api_base_url
            )
            if api_url:
                api_host = urlparse(api_url).hostname or ""
                if (
                    backend.backend_name != "e2b"
                    and api_host
                    and api_host != "host.docker.internal"
                    and not is_ip_address(api_host)
                ):
                    resolved_ips = resolve_hostname(api_host, logger)
                    if resolved_ips:
                        container_extra_hosts[api_host] = resolved_ips[0]

            container_cap_drop = ["NET_RAW"]
            logger.info("Network isolation enabled: preflight passed")

        cpu = config.work_cpu_limit if config.work_cpu_limit is not None else task_spec.work.cpu_limit
        mem = config.work_mem_limit if config.work_mem_limit is not None else task_spec.work.mem_limit

        handle = backend.create_container(
            task_spec.work_image_key,
            container_name,
            environment=(
                _e2b_install_env(env, agent)
                if backend.backend_name == "e2b" else env
            ),
            extra_hosts=container_extra_hosts,
            cap_drop=container_cap_drop or None,
            cpu_limit=cpu,
            mem_limit=mem,
            # E2B execs default to the SDK's "user" account rather than the
            # image USER. Docker and Kubernetes retain the image default.
            user="agent" if backend.backend_name == "e2b" else None,
        )
        backend.start_container(handle)
        if credential_proxy is not None:
            container_ip = backend.get_container_ip(handle)
            if not container_ip:
                raise RuntimeError(
                    "Cannot determine container IP for credential isolation"
                )
            credential_proxy.restrict_clients({container_ip})
            credential_proxy.start()
        logger.info(f"Container started: {container_name} (judge_url={judge_url})")

        # 3. Install agent runtime
        for i, cmd in enumerate(agent.install_cmds):
            logger.info(
                f"Install step {i + 1}/{len(agent.install_cmds)}: {cmd}"
            )
            result = backend.exec_run_with_exit_code(
                handle, f"/bin/bash -c {shlex.quote(cmd)}",
                timeout=restored_execution.timeout(600) if restored_execution else 600
            )
            install_parts.append(result.output)
            if result.timed_out:
                raise RuntimeError(f"Install command timed out: {cmd}")
            if result.exit_code != 0:
                raise RuntimeError(
                    f"Install command failed (exit {result.exit_code}): {cmd}\n{result.output}"
                )
            logger.info(f"Install step {i + 1} done ({result.elapsed_seconds:.1f}s)")

        (log_dir / "install_output.txt").write_text("\n".join(install_parts))
        logger.info("Agent installation complete")

        if restored_execution is not None:
            restored_execution.remaining()

        # 4. Install tools.
        #    Game-mode tasks skip sforge-submit (scoring is built into the
        #    game HTTP API and there is no archive to submit), but still need
        #    the stop hook — otherwise the agent exits naturally as soon as
        #    the model decides it's "done", losing the full timeout budget.
        effective_eval_interval = 0 if disable_auto_eval else eval_interval
        if not task_spec.game_mode:
            _install_tools(
                backend,
                handle,
                task_spec,
                agent,
                log_dir,
                logger,
                disable_stop_hook=disable_stop_hook,
            )

            # Start host-side auto-eval thread (if enabled)
            if effective_eval_interval > 0:
                auto_eval_stop = threading.Event()
                auto_eval_thread = threading.Thread(
                    target=_auto_eval_loop,
                    args=(
                        backend, handle, task_spec, host_judge_url,
                        session_token, effective_eval_interval,
                        auto_eval_stop, logger, log_dir,
                        admin_secret,
                    ),
                    daemon=True,
                )
                auto_eval_thread.start()
                auto_eval_started = True
                logger.info(f"Host-side auto-eval started (interval={effective_eval_interval}s)")
        elif not disable_stop_hook:
            agent.install_stop_hook(backend, handle, log_dir, logger)

        # 4b. Apply network isolation (after install + tools, before agent)
        if not internet:
            from sforge.harness.network_isolation import (
                build_allowed_endpoints,
            )

            gateway_ip = backend.get_container_gateway_ip(handle) or ""
            if not gateway_ip and backend.backend_name == "docker":
                raise RuntimeError(
                    "Cannot determine gateway IP for network isolation"
                )

            effective_api_url = (
                credential_proxy_url
                or config.agent_api_base_url
                or agent.default_api_base_url
            )
            endpoints = build_allowed_endpoints(
                judge_url, effective_api_url, gateway_ip, logger,
                resolve_hostnames=backend.backend_name != "e2b",
                host_ip_overrides=container_extra_hosts,
            )
            net_isolation = backend.create_network_isolation(handle, endpoints, logger)
            net_isolation.apply()

        if runtime_attestation is not None:
            credential_mode = (
                "host_gateway" if credential_proxy is not None
                else "container_environment" if config.agent_api_key else "none"
            )
            runtime_attestation.record(
                "solver_ready", "ready",
                {
                    "container": {
                        "backend": backend.backend_name,
                        "name": handle.name,
                        "id_sha256": hashlib.sha256(handle.id.encode()).hexdigest(),
                        "work_image": task_spec.work_image_key,
                        "judge_image": task_spec.judge_image_key,
                    },
                    "network_boundary": {
                        "restricted_requested": not internet,
                        "restricted_applied": net_isolation is not None,
                    },
                    "credential_boundary": {
                        "provider_credential_configured": bool(config.agent_api_key),
                        "mode": credential_mode,
                        "real_credential_absent_from_container": credential_proxy is not None,
                        "gateway_client_ip_restricted": credential_proxy is not None,
                    },
                },
                evidence_files={"prompt": log_dir / "agent_prompt.md"},
            )

        # 5. Write prompt
        if task_spec.game_mode:
            prompt = generate_game_prompt(
                task_spec.work.agent_query,
                internet=internet,
                agent_budget_seconds=effective_timeout,
            )
        else:
            prompt = generate_evolve_prompt(
                task_spec.work.agent_query,
                submit_paths=task_spec.submit_paths,
                internet=internet,
                max_submissions=max_submissions,
                submission_cooldown=submission_cooldown,
                agent_budget_seconds=effective_timeout,
            )
        prompt_path = "/tmp/agent_prompt.md"
        local_prompt = log_dir / "agent_prompt.md"
        local_prompt.write_text(prompt)
        _install_task_file(backend, handle, local_prompt, PurePosixPath(prompt_path))
        logger.info(f"Prompt written ({len(prompt)} bytes)")

        # 6. Run agent (with auto-resume on abnormal exit)
        can_resume = not disable_auto_resume and agent.resume_cmd is not None
        remaining_timeout = restored_execution.remaining() if restored_execution else effective_timeout
        resume_count = 0
        total_runtime = 0.0
        agent_timed_out = False
        all_output_parts: list[str] = []
        agent_live_log = log_dir / "agent_output.txt"
        MIN_RUNTIME_FOR_RESUME = 1
        MAX_RESUMES = 100

        started_at = time.time()
        from datetime import datetime
        started_at_iso = datetime.fromtimestamp(started_at).strftime("%Y-%m-%dT%H:%M:%S")
        (log_dir / "started_at").write_text(f"{started_at_iso}\n{started_at}\n")

        on_chunk_cb = None

        while remaining_timeout > 0:
            if restored_execution is not None:
                try:
                    remaining_timeout = restored_execution.remaining()
                except TimeoutError:
                    agent_timed_out = True
                    break
                if resume_count == 0:
                    restored_execution.start_work()
            # Backends accept whole seconds; never dispatch a zero-second timeout.
            segment_timeout = int(remaining_timeout)
            if segment_timeout <= 0:
                agent_timed_out = True
                break
            is_resume = resume_count > 0
            run_cmd = agent.format_run_cmd(
                prompt_path, model=model,
                internet=internet, resume=is_resume,
            )
            if is_resume:
                logger.info(
                    f"Resuming agent (attempt {resume_count}/{MAX_RESUMES}, "
                    f"{remaining_timeout:.0f}s left): {run_cmd[:200]}..."
                )
            else:
                logger.info(f"Running agent: {run_cmd[:200]}... (timeout={remaining_timeout:.0f}s)")

            # Resume commands may reuse a session without reading the prompt again.
            # Refresh the budget snapshot for each dispatched process as well.
            segment_env = {
                **env,
                "SFORGE_AGENT_BUDGET_SECONDS": str(effective_timeout),
                "SFORGE_AGENT_REMAINING_SECONDS": str(segment_timeout),
                "SFORGE_AGENT_SEGMENT_STARTED_AT": str(time.time()),
            }
            segment = {
                "index": len(solver_execution["segments"]) + 1,
                "resume": is_resume, "backend": backend.backend_name,
                "status": "pending", "started_at": time.time(),
                "returned_exit_code": None, "native_exit_code": None,
                "terminal_observed": None, "termination_signal": None,
            }
            solver_execution["segments"].append(segment)
            solver_execution["status"] = "pending"
            _write_solver_execution(log_dir, solver_execution)
            try:
                seg_result = backend.exec_run_with_timeout(
                    handle,
                    ["/bin/bash", "-c", run_cmd],
                    timeout=segment_timeout,
                    log_file=agent_live_log,
                    workdir=task_spec.cwd,
                    environment=segment_env,
                    stream_to_stdout=verbose,
                    shutdown_event=shutdown_event,
                    log_append=is_resume,
                    on_chunk=on_chunk_cb,
                )
            except BaseException as exc:
                # A caller interruption or transport error does not establish a
                # signal delivered to the solver or confirm that it stopped.
                segment.update(status="unknown", error_type=type(exc).__name__,
                               finished_at=time.time())
                solver_execution["status"] = "unknown"
                try:
                    _write_solver_execution(log_dir, solver_execution)
                except OSError:
                    logger.warning("Could not persist solver exception evidence")
                raise
            segment.update(_solver_segment_result(
                seg_result, shutdown_event is not None and shutdown_event.is_set()))
            solver_execution["status"] = segment["status"]
            _write_solver_execution(log_dir, solver_execution)
            all_output_parts.append(seg_result.output)
            total_runtime += seg_result.elapsed_seconds

            if seg_result.timed_out:
                agent_timed_out = True
                break

            if not can_resume:
                break
            observe_resume = getattr(agent, "resume_block_reason", None)
            resume_block = observe_resume(seg_result) if observe_resume is not None else None
            if resume_block is not None:
                segment["resume_block_reason"] = resume_block
                _write_solver_execution(log_dir, solver_execution)
                logger.warning("Agent restart blocked by terminal provider failure: %s", resume_block)
                break
            if seg_result.elapsed_seconds < MIN_RUNTIME_FOR_RESUME:
                logger.warning(
                    f"Agent exited after only {seg_result.elapsed_seconds:.1f}s "
                    f"(< {MIN_RUNTIME_FOR_RESUME}s), not resuming (likely systematic failure)"
                )
                break
            if resume_count >= MAX_RESUMES:
                logger.warning(f"Max resume attempts ({MAX_RESUMES}) reached")
                break

            if restored_execution is None:
                remaining_timeout -= seg_result.elapsed_seconds
            resume_count += 1
            logger.info(f"Agent exited after {seg_result.elapsed_seconds:.1f}s, will resume")

        agent_output = "\n".join(all_output_parts)
        runtime = total_runtime
        logger.info(
            f"Agent finished: runtime={runtime:.1f}s, timed_out={agent_timed_out}, "
            f"resumes={resume_count}"
        )

        # 7. Stop auto-eval thread before extracting final archive
        if auto_eval_stop is not None:
            auto_eval_stop.set()
            # A set event does not cancel an in-flight submit HTTP request.
            # Wait for the producer before interpreting an empty pending set.
            auto_eval_thread.join(timeout=435)
            auto_eval_joined = True
            logger.info("Auto-eval stop requested")
        auto_eval_finished = auto_eval_thread is None or not auto_eval_thread.is_alive()

        # 8. Extract final archive (tar of submit_paths)
        try:
            final_archive = _extract_archive_from_container(
                backend, handle, task_spec,
            )
            (log_dir / "final_archive.tar.gz").write_bytes(final_archive)
            logger.info(f"Final archive: {len(final_archive)} bytes")
        except Exception as e:
            logger.warning(f"Failed to extract final archive (container may have stopped): {e}")
            final_archive = b""

        # 8. Collect results
        if task_spec.game_mode:
            close_error = None
            try:
                close_response = requests.post(
                    f"{host_judge_url}/api/v1/game/{run_id}/{task_spec.task_id}/close-all",
                    timeout=30,
                )
                close_response.raise_for_status()
                closed = close_response.json().get("closed")
                if type(closed) is not int or closed < 0:
                    raise ValueError("Invalid game close response")
            except (requests.RequestException, ValueError, AttributeError):
                close_error = "game_close_failed"
                logger.warning("Failed to confirm game session closure")

            grading = _wait_for_grading(
                host_judge_url, session_token, admin_secret,
                eval_timeout=task_spec.judge.eval_timeout,
                shutdown_event=effective_grading_shutdown, logger=logger,
                incomplete_reason=close_error,
            )
            history = _write_grading(log_dir, "game_history.json", grading, run_id)

            game_entries = [
                e for e in history.get("entries", []) if e.get("type") == "game"
            ]
            best_score_raw = history.get("best_score")
            best_score = (
                float(best_score_raw) if best_score_raw is not None else None
            )
            best_pass_rate_raw = history.get("best_pass_rate", 0.0)
            best_pass_rate = float(best_pass_rate_raw) if best_pass_rate_raw else 0.0
            best_round = history.get("best_round", "")
            total_rounds = len(game_entries)

            result = RunResult(
                archive=final_archive,
                best_pass_rate=best_pass_rate,
                best_score=best_score,
                best_round=best_round,
                total_rounds=total_rounds,
                agent_submissions=total_rounds,
                auto_submissions=0,
                agent_output=agent_output,
                timed_out=agent_timed_out,
                runtime_seconds=runtime,
                resume_count=resume_count,
                grading_status=grading.status,
                grading_reason=grading.reason,
                solver_execution=solver_execution,
            )
            logger.info(
                f"Done (game): best_score={best_score} ({best_round}), "
                f"sessions={total_rounds}, resumes={resume_count}, runtime={runtime:.1f}s"
            )
        else:
            # Save debug artifacts from container (non-authoritative, best-effort)
            try:
                state_result = backend.exec_run_with_timeout(
                    handle,
                    [
                        "/bin/bash",
                        "-c",
                        "cat /tmp/sforge_state.json 2>/dev/null || echo '{}'",
                    ],
                    timeout=10,
                )
                state = json.loads(state_result.output.strip())
                (log_dir / "evolve_state.json").write_text(
                    json.dumps(state, indent=2, ensure_ascii=False)
                )
            except Exception:
                logger.warning("Failed to read state file from container")

            # Stop work container processes
            try:
                backend.exec_run(handle, "kill 1 2>/dev/null || true", user="root")
                logger.info("Work container processes stopped")
            except Exception:
                pass

            grading = _wait_for_grading(
                host_judge_url, session_token, admin_secret,
                eval_timeout=task_spec.judge.eval_timeout,
                shutdown_event=effective_grading_shutdown, logger=logger,
                incomplete_reason=None if auto_eval_finished else "auto_eval_active",
            )
            history = _write_grading(log_dir, "run_history.json", grading, run_id)

            best_pass_rate = history.get("best_pass_rate", 0.0)
            best_score_raw = history.get("best_score")
            best_score = (
                float(best_score_raw) if best_score_raw is not None else None
            )
            best_round = history.get("best_round", "")
            agent_subs = history.get("agent_submissions", 0)
            auto_subs = history.get("auto_submissions", 0)
            total_rounds = agent_subs + auto_subs

            result = RunResult(
                archive=final_archive,
                best_pass_rate=best_pass_rate,
                best_score=best_score,
                best_round=best_round,
                total_rounds=total_rounds,
                agent_submissions=agent_subs,
                auto_submissions=auto_subs,
                agent_output=agent_output,
                timed_out=agent_timed_out,
                runtime_seconds=runtime,
                resume_count=resume_count,
                grading_status=grading.status,
                grading_reason=grading.reason,
                solver_execution=solver_execution,
            )

            logger.info(
                f"Done: best={result.best_pass_rate:.2%} "
                f"(round {result.best_round!r}), "
                f"agent_subs={agent_subs}, auto_subs={auto_subs}, "
                f"resumes={resume_count}, runtime={runtime:.1f}s"
            )
        _record_terminal_attestation(runtime_attestation, log_dir, result)
        return result

    except KeyboardInterrupt:
        logger.info("Run interrupted by user (Ctrl+C)")

        # Stop auto-eval thread
        if auto_eval_stop is not None:
            auto_eval_stop.set()

        # Try to extract archive from the container before it's destroyed
        interrupted_archive = b""
        try:
            if handle is not None:
                interrupted_archive = _extract_archive_from_container(
                    backend, handle, task_spec,
                )
                (log_dir / "final_archive.tar.gz").write_bytes(interrupted_archive)
                logger.info(f"Final archive (interrupted): {len(interrupted_archive)} bytes")
        except Exception:
            pass

        grading = (_wait_for_grading(
            host_judge_url, session_token, admin_secret,
            eval_timeout=task_spec.judge.eval_timeout,
            shutdown_event=effective_grading_shutdown, logger=logger,
            incomplete_reason="interrupted",
        ) if session_token is not None else _GradingResult(None, "unknown", "interrupted"))
        try:
            history_file = "game_history.json" if task_spec.game_mode else "run_history.json"
            history = _write_grading(log_dir, history_file, grading, run_id)
        except Exception as evidence_error:
            logger.warning("Grading evidence unavailable (%s)", type(evidence_error).__name__)
            history = grading.history or {}
            grading.status, grading.reason = "unknown", "evidence_write_failed"

        best_pass_rate = history.get("best_pass_rate", 0.0)
        best_score_raw = history.get("best_score")
        best_score = float(best_score_raw) if best_score_raw is not None else None
        best_round = history.get("best_round", "")
        agent_subs = history.get("agent_submissions", 0)
        auto_subs = history.get("auto_submissions", 0)
        total_rounds = agent_subs + auto_subs

        logger.info(
            f"Interrupted results: best={best_pass_rate:.2%} "
            f"(round {best_round!r}), "
            f"agent_subs={agent_subs}, auto_subs={auto_subs}"
        )

        result = RunResult(
            archive=interrupted_archive,
            best_pass_rate=best_pass_rate,
            best_score=best_score,
            best_round=best_round,
            total_rounds=total_rounds,
            agent_submissions=agent_subs,
            auto_submissions=auto_subs,
            agent_output="Stopped by user (Ctrl+C)",
            timed_out=False,
            resume_count=resume_count,
            grading_status=grading.status,
            grading_reason=grading.reason,
            solver_execution=solver_execution,
        )
        _record_terminal_attestation(runtime_attestation, log_dir, result)
        return result
    except Exception as e:
        logger.error(f"Error: {e}\n{traceback.format_exc()}")
        grading = _GradingResult(None, "unknown", "harness_error")
        history = {}
        try:
            if session_token is not None:
                grading = _wait_for_grading(
                    host_judge_url, session_token, admin_secret,
                    eval_timeout=task_spec.judge.eval_timeout,
                    shutdown_event=effective_grading_shutdown, logger=logger,
                    incomplete_reason="harness_error",
                )
            history_file = "game_history.json" if task_spec.game_mode else "run_history.json"
            history = _write_grading(log_dir, history_file, grading, run_id)
        except Exception as evidence_error:
            # Preserve the original harness exception if recording also fails.
            logger.warning("Grading evidence unavailable (%s)", type(evidence_error).__name__)
            history = grading.history or {}
            grading.status, grading.reason = "unknown", "evidence_write_failed"
        if restored_registration is not None or backend.backend_name == "e2b":
            raise

        best_pass_rate = history.get("best_pass_rate", 0.0)
        best_score_raw = history.get("best_score")
        best_score = float(best_score_raw) if best_score_raw is not None else None
        best_round = history.get("best_round", "")
        agent_subs = history.get("agent_submissions", 0)
        auto_subs = history.get("auto_submissions", 0)

        result = RunResult(
            best_pass_rate=best_pass_rate,
            best_score=best_score,
            best_round=best_round,
            total_rounds=agent_subs + auto_subs,
            agent_submissions=agent_subs,
            auto_submissions=auto_subs,
            agent_output=str(e),
            timed_out=False,
            resume_count=locals().get("resume_count", 0),
            grading_status=grading.status,
            grading_reason=grading.reason,
            solver_execution=solver_execution,
        )
        _record_terminal_attestation(runtime_attestation, log_dir, result)
        return result
    finally:
        if auto_eval_stop is not None:
            auto_eval_stop.set()
            if auto_eval_started and not auto_eval_joined:
                auto_eval_thread.join(timeout=435)
        if net_isolation is not None:
            try:
                net_isolation.cleanup()
            except Exception as exc:
                if logger:
                    logger.warning(f"Failed to cleanup network isolation: {exc}")
        try:
            if threading.current_thread() is threading.main_thread():
                prev_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
                try:
                    print("\nStopping container, please wait... (Ctrl+C disabled during cleanup)")
                    backend.cleanup_container(handle, logger)
                finally:
                    signal.signal(signal.SIGINT, prev_handler)
            else:
                backend.cleanup_container(handle, logger)
        finally:
            try:
                if credential_proxy is not None:
                    credential_proxy.stop()
            finally:
                if restored_execution is not None:
                    restored_execution.close()
                close_logger(logger)
