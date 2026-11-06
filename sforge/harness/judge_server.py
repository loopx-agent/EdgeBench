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

"""Judge HTTP REST API server (FastAPI)."""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import math
import os
import secrets
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import requests
from docker.utils import parse_bytes
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from sforge import __version__
from sforge.harness.backend import ContainerBackend
from sforge.harness.benchmark import load_benchmark
from sforge.harness.config import (
    SForgeConfig,
    create_backend_from_config,
    get_container_env,
    load_config,
    sanitize_e2b_proxy_env,
)
from sforge.harness.constants import get_admin_secret
from sforge.harness.docker_build import BuildImageError, build_judge_image
from sforge.harness.feedback_transport import FeedbackTransportRecorder
from sforge.harness.run_evaluation import judge_submission
from sforge.harness.selection import select_best
from sforge.harness.task_spec import TaskSpec, load_all_tasks

logger = logging.getLogger("sforge.judge_server")

GAME_SERVER_APP_PATH = Path(__file__).parent / "game_server_app.py"


class JudgeCapacityExceeded(Exception):
    pass


class SubmissionBudgetExceeded(Exception):
    pass


class SubmissionCooldownActive(Exception):
    pass


# --- Models ---


class SubmissionStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


def json_safe_feedback_payload(value: Any) -> Any:
    """Project non-finite scores into explicit standard-JSON string markers.

    Judge parsers may legitimately return infinity for an invalid optimization
    candidate. Keep those raw numbers in server state so selection and final
    score reconciliation retain their original semantics. HTTP responses and
    signed feedback transport records must be standard JSON, though, and the
    default FastAPI encoder rejects non-finite floats with a 500 response.

    Strings distinguish a non-finite score from ``null`` (missing). Finite
    values and container shape remain unchanged.
    """
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, dict):
        return {key: json_safe_feedback_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe_feedback_payload(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe_feedback_payload(item) for item in value]
    return value


class SubmissionReceipt(BaseModel):
    schema_version: Literal["sforge_submission_receipt_v1"] = "sforge_submission_receipt_v1"
    task_id: str
    round_id: str | None
    archive_sha256: str | None
    archive_bytes: int | None


class ResultResponse(BaseModel):
    submission_id: str
    status: SubmissionStatus
    report: dict | None = None
    error: str | None = None
    receipt: SubmissionReceipt | None = None


class GameNewRequest(BaseModel):
    pass


class GameStepRequest(BaseModel):
    action: str


class GameNewResponse(BaseModel):
    session_id: str
    observation: str
    score: int
    peak_score: int
    max_score: int
    done: bool
    moves: int


class GameStepResponse(BaseModel):
    session_id: str
    observation: str
    score: int
    peak_score: int
    max_score: int
    done: bool
    moves: int


class GameStatusResponse(BaseModel):
    session_id: str
    score: int
    peak_score: int
    max_score: int
    done: bool
    moves: int


class GameCloseResponse(BaseModel):
    session_id: str
    final_score: int
    peak_score: int
    max_score: int
    moves: int


class RegisterRequest(BaseModel):
    task_id: str
    run_id: str
    admin_secret: str = ""
    judge_cpu_limit: int | None = None
    judge_mem_limit: str | None = None
    backend: str | None = None
    k8s_image_registry: str | None = None
    k8s_namespace: str | None = None
    k8s_node_selector: dict[str, str] | None = None
    k8s_kubeconfig: str | None = None
    max_agent_submissions: int | None = None
    submission_cooldown: int | None = None
    e2b_template_map: dict[str, str] | None = None
    e2b_template_namespace: str | None = None
    e2b_sandbox_ttl: int | None = None


class RegisterResponse(BaseModel):
    token: str
    feedback_relay_id: str


class SubmitResponse(BaseModel):
    submission_id: str
    round_id: str
    status: SubmissionStatus
    remaining_submissions: int | None = None
    receipt: SubmissionReceipt | None = None


# --- Game session state ---

GAME_SESSION_IDLE_TIMEOUT = 600  # 10 minutes
GAME_SESSION_MAX = 200
GAME_SESSION_PER_RUN_MAX = 3
GAME_REAPER_INTERVAL = 60
GAME_CONTAINER_READY_TIMEOUT = 30
GAME_CONTAINER_READY_POLL = 0.5
GAME_CONTAINER_PORT = 8000
GAME_GATEWAY_ATTEMPTS = 3
GAME_GATEWAY_BACKOFF = 1.0


@dataclass
class GameSessionState:
    session_id: str
    run_id: str
    task_id: str
    game_num: int
    container: object  # ContainerHandle
    container_url: str
    headers: dict = field(default_factory=dict)
    max_score: int = 0
    peak_score: int = 0
    current_score: int = 0
    moves: int = 0
    done: bool = False
    steps: list = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    operation_lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False,
    )


def submission_history_entry(submission_id: str, task_id: str, round_id: str | None,
                             report_dict: dict | None, error: str | None, status: str) -> dict:
    entry: dict[str, Any] = {
        "type": "submission",
        "status": status,
        "submission_id": submission_id,
        "task_id": task_id,
        "round": round_id,
    }
    if report_dict:
        entry["pass_rate"] = report_dict.get("pass_rate", 0.0)
        entry["score"] = report_dict.get("score")
        entry["passed"] = report_dict.get("passed", 0)
        entry["failed"] = report_dict.get("failed", 0)
        entry["total_tests"] = report_dict.get("total_tests", 0)
        entry["valid"] = report_dict.get("valid", True)
        entry["summary"] = report_dict.get("summary")
        for key in ("timed_out", "runtime_seconds"):
            if key in report_dict:
                entry[key] = report_dict[key]
    if error:
        entry["error"] = error
    return entry


#: Host-wide slot file. Every launcher process on this machine contends for the
#: same file, so the total number of concurrently running judges is bounded once
#: instead of once per process.
JUDGE_HOST_SLOT_FILE = os.environ.get(
    "SFORGE_JUDGE_HOST_SLOT_FILE", "/tmp/sforge-judge-host-slots.json")
JUDGE_HOST_MEMORY_FILE = os.environ.get(
    "SFORGE_JUDGE_HOST_MEMORY_FILE", "/tmp/sforge-judge-host-memory.json")
JUDGE_HOST_OWNER_FILE = os.environ.get(
    "SFORGE_JUDGE_HOST_OWNER_FILE", "/tmp/sforge-judge-host-owners.json")
#: Default host-wide total. Chosen so several launchers share one machine budget.
JUDGE_HOST_CAPACITY = 8
#: A launcher can disappear after another server has already completed its one-time
#: startup reconciliation.  Capacity waiters periodically retry backend-confirmed
#: cleanup so an orphan cannot stall every later judge indefinitely.
JUDGE_CAPACITY_RECONCILE_INTERVAL = 5.0
# Solver-requested feedback can bypass older background evaluations, but only in
# bounded bursts. This keeps the interactive path responsive without starving
# periodic score history when a client submits continuously.
JUDGE_FOREGROUND_BYPASS_BURST = 4


class HostJudgeCapacity:
    """Cross-process judge slot budget backed by an flock-guarded file.

    A per-process pool multiplies with the number of launcher processes; this is the
    bound that actually limits the host. The first writer fixes the capacity.
    A dead launcher does not prove its judge containers stopped, so slots remain
    reserved until explicit release or backend-confirmed cleanup.
    """

    def __init__(
        self, capacity: int, path: str | None = None, owner_path: str | None = None,
    ) -> None:
        self.capacity = max(1, int(capacity))
        self.path = Path(path or os.environ.get("SFORGE_JUDGE_HOST_SLOT_FILE", JUDGE_HOST_SLOT_FILE))
        self._lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        if owner_path is not None:
            self._owner_path = Path(owner_path)
        elif path is not None:
            self._owner_path = self.path.with_suffix(self.path.suffix + ".owners")
        else:
            self._owner_path = Path(
                os.environ.get("SFORGE_JUDGE_HOST_OWNER_FILE", JUDGE_HOST_OWNER_FILE)
            )

    def _read(self) -> dict:
        try:
            state = json.loads(self.path.read_text())
        except FileNotFoundError:
            return {"version": 1, "capacity": self.capacity, "holders": {}}
        except (OSError, ValueError) as exc:
            raise RuntimeError("Cannot read judge host capacity state") from exc
        if (not isinstance(state, dict) or state.get("version") != 1
                or type(state.get("capacity")) is not int or state["capacity"] < 1
                or not isinstance(state.get("holders"), dict)
                or any(
                    not isinstance(k, str)
                    or not (
                        isinstance(v, (int, float))
                        or (
                            isinstance(v, dict)
                            and isinstance(v.get("acquired_at"), (int, float))
                            and (v.get("owner_id") is None or isinstance(v.get("owner_id"), str))
                            and type(v.get("units", 1)) is int
                            and v.get("units", 1) >= 1
                        )
                    )
                    for k, v in state["holders"].items()
                )):
            raise RuntimeError("Invalid judge host capacity state; explicit recovery required")
        return state

    def _write(self, state: dict) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(self.path)

    def _read_owners(self) -> dict[str, str]:
        try:
            state = json.loads(self._owner_path.read_text())
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            raise RuntimeError("Cannot read judge host owner state") from exc
        owners = state.get("owners") if isinstance(state, dict) else None
        if (
            not isinstance(state, dict)
            or state.get("version") != 1
            or not isinstance(owners, dict)
            or any(not isinstance(k, str) or not isinstance(v, str) for k, v in owners.items())
        ):
            raise RuntimeError("Invalid judge host owner state; explicit recovery required")
        return owners

    def _write_owners(self, owners: dict[str, str]) -> None:
        tmp = self._owner_path.with_suffix(self._owner_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"version": 1, "owners": owners}))
        tmp.replace(self._owner_path)

    @staticmethod
    def _holder_units(holder: float | dict) -> int:
        return holder.get("units", 1) if isinstance(holder, dict) else 1

    @staticmethod
    def _process_start(token: str) -> str | None:
        pid, separator, _ = token.partition(":")
        if not separator or not pid.isdecimal():
            return None
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            if stat[0] == "Z":
                return "exited"
            boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            return f"{boot}:{stat[19]}"
        except (OSError, IndexError):
            return None

    def _read_waiters(self, state: dict) -> list[dict]:
        waiters = state.get("waiters", [])
        if (
            not isinstance(waiters, list)
            or any(
                not isinstance(w, dict)
                or not isinstance(w.get("token"), str)
                or (
                    w.get("owner_id") is not None and not isinstance(w["owner_id"], str)
                )
                or type(w.get("units")) is not int
                or w["units"] < 1
                or type(w.get("memory_units")) is not int
                or w["memory_units"] < 0
                or type(w.get("priority", 0)) is not int
                or w.get("priority", 0) < 0
                or (
                    w.get("memory_path") is not None
                    and not isinstance(w["memory_path"], str)
                )
                or (
                    w.get("process_start") is not None
                    and not isinstance(w["process_start"], str)
                )
                for w in waiters
            )
            or len({w["token"] for w in waiters}) != len(waiters)
        ):
            raise RuntimeError(
                "Invalid judge host waiter state; explicit recovery required"
            )
        return waiters

    def try_acquire(
        self,
        token: str,
        owner_id: str | None = None,
        *,
        units: int = 1,
        memory_capacity: HostJudgeCapacity | None = None,
        memory_units: int = 0,
        priority: int = 0,
    ) -> bool:
        """Admit by bounded priority and FIFO when every resource fits.

        Higher-priority requests can bypass older lower-priority requests, while a
        persisted burst counter admits the oldest waiter after a bounded number of
        bypasses. FIFO order is preserved within a priority class. Waiters reserve
        no capacity. Queue state survives polling and process-local object
        recreation. A dead waiter can be removed without backend cleanup; holders
        still require explicit release or confirmed container cleanup.
        """
        if type(units) is not int or units < 1:
            raise ValueError("Judge host capacity units must be a positive integer")
        if type(memory_units) is not int or memory_units < 0:
            raise ValueError("Judge memory units must be a nonnegative integer")
        if type(priority) is not int or priority < 0:
            raise ValueError("Judge request priority must be a nonnegative integer")
        if (memory_capacity is None) != (memory_units == 0):
            raise ValueError(
                "Judge memory capacity and positive units must be supplied together"
            )
        resources = [(self, units)]
        if memory_capacity is not None:
            if memory_capacity.path.resolve() == self.path.resolve():
                raise ValueError("Judge slot and memory files must be distinct")
            resources.append((memory_capacity, memory_units))
        # Fixed ordering prevents deadlocks even if two callers reverse resources.
        with ExitStack() as locks:
            for capacity, _ in sorted(
                resources, key=lambda r: str(r[0].path.resolve())
            ):
                capacity._lock_path.parent.mkdir(parents=True, exist_ok=True)
                lock = locks.enter_context(open(capacity._lock_path, "a+"))
                fcntl.flock(lock, fcntl.LOCK_EX)
            snapshots = []
            for capacity, required in resources:
                state = capacity._read()
                if state["capacity"] != capacity.capacity:
                    raise RuntimeError("Judge host capacity configuration mismatch")
                if required > state["capacity"]:
                    raise ValueError("Judge request exceeds host capacity")
                owners = capacity._read_owners()
                current = state["holders"].get(token)
                if current is not None:
                    current_owner = (
                        current.get("owner_id")
                        if isinstance(current, dict)
                        else owners.get(token)
                    )
                    if (
                        capacity._holder_units(current) != required
                        or current_owner != owner_id
                    ):
                        raise RuntimeError(
                            "Judge host capacity holder identity mismatch"
                        )
                snapshots.append((capacity, required, state, owners, current))
            state = snapshots[0][2]
            waiters = self._read_waiters(state)
            # PID reuse and zombies are detectable on Linux. Unknown identity is
            # retained rather than interpreted as a dead process.
            live = []
            for waiter in waiters:
                birth = self._process_start(waiter["token"])
                if not self._owner_is_alive(waiter["token"]) or birth == "exited":
                    continue
                if (
                    waiter["process_start"] is not None
                    and birth is not None
                    and waiter["process_start"] != birth
                ):
                    continue
                live.append(waiter)
            request = {
                "token": token,
                "owner_id": owner_id,
                "units": units,
                "memory_path": str(memory_capacity.path.resolve())
                if memory_capacity
                else None,
                "memory_units": memory_units,
                "process_start": self._process_start(token),
                "priority": priority,
            }
            prior = next((w for w in live if w["token"] == token), None)
            if prior is not None and any(
                prior.get(k, 0 if k == "priority" else None) != request[k]
                for k in request if k != "process_start"
            ):
                raise RuntimeError(
                    "Judge host waiter identity mismatch; cancel before changing request"
                )
            already_held = all(item[4] is not None for item in snapshots)
            if prior is None and not already_held:
                live.append(request)
            state["waiters"] = live
            fits = all(
                current is not None
                or sum(capacity._holder_units(h) for h in st["holders"].values())
                + required
                <= st["capacity"]
                for capacity, required, st, _, current in snapshots
            )
            oldest = live[0] if live else None
            candidate = oldest
            priority_burst = state.get("priority_burst", 0)
            if type(priority_burst) is not int or priority_burst < 0:
                raise RuntimeError(
                    "Invalid judge host priority state; explicit recovery required"
                )
            if live:
                highest = max(w.get("priority", 0) for w in live)
                preferred = next(
                    w for w in live if w.get("priority", 0) == highest
                )
                if (
                    preferred is not oldest
                    and priority_burst < JUDGE_FOREGROUND_BYPASS_BURST
                ):
                    candidate = preferred
            if not already_held and (candidate["token"] != token or not fits):
                if live != waiters:
                    self._write(state)
                return False
            originals = [
                (capacity, deepcopy(st), dict(owners))
                for capacity, _, st, owners, _ in snapshots
            ]
            # Persist memory first. Process crashes retain reservations until
            # backend-confirmed cleanup; ordinary write failures roll back here.
            try:
                state["waiters"] = [w for w in live if w["token"] != token]
                state["priority_burst"] = (
                    priority_burst + 1 if candidate is not oldest else 0
                )
                for capacity, required, st, owners, current in reversed(snapshots):
                    if current is None:
                        st["holders"][token] = (
                            {
                                "acquired_at": time.time(),
                                "owner_id": owner_id,
                                "units": required,
                            }
                            if required != 1
                            else time.time()
                        )
                        if owner_id is not None:
                            owners[token] = owner_id
                        else:
                            owners.pop(token, None)
                    capacity._write(st)
                    capacity._write_owners(owners)
            except Exception:
                for capacity, original_state, original_owners in reversed(originals):
                    try:
                        capacity._write_owners(original_owners)
                        capacity._write(original_state)
                    except Exception:
                        logger.exception("Could not roll back judge admission %s", token)
                raise
            return True

    def cancel_waiter(self, token: str) -> None:
        """Remove an abandoned queued request, never an admitted holder."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = self._read()
            waiters = self._read_waiters(state)
            remaining = [w for w in waiters if w["token"] != token]
            if remaining != waiters:
                state["waiters"] = remaining
                self._write(state)

    def release(self, token: str) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                state = self._read()
                owners = self._read_owners()
                state["holders"].pop(token, None)
                state["waiters"] = [
                    w for w in self._read_waiters(state) if w["token"] != token
                ]
                owners.pop(token, None)
                self._write(state)
                self._write_owners(owners)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @staticmethod
    def _owner_is_alive(token: str) -> bool:
        owner, separator, _ = token.partition(":")
        if not separator:
            return True
        try:
            pid = int(owner)
        except ValueError:
            return True
        if pid < 1:
            return True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def reconcile_stale_holders(
        self, cleanup: Callable[[str, str | None], bool],
    ) -> list[str]:
        """Release dead owners only after their backend resources are gone."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                state = self._read()
                if state["capacity"] != self.capacity:
                    raise RuntimeError("Judge host capacity configuration mismatch")
                snapshot = dict(state["holders"])
                owner_snapshot = self._read_owners()
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

        reconciled = []
        for token, holder in sorted(snapshot.items()):
            if self._owner_is_alive(token):
                continue
            owner_id = (
                holder.get("owner_id") if isinstance(holder, dict)
                else owner_snapshot.get(token)
            )
            try:
                cleaned = cleanup(token, owner_id)
            except Exception:
                logger.exception("Could not clean stale judge slot %s", token)
                continue
            if not cleaned:
                continue
            with open(self._lock_path, "a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                try:
                    state = self._read()
                    if state["holders"].get(token) != holder:
                        continue
                    owners = self._read_owners()
                    state["holders"].pop(token)
                    owners.pop(token, None)
                    self._write(state)
                    self._write_owners(owners)
                    reconciled.append(token)
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
        return reconciled


# --- Server state ---


class JudgeState:
    def __init__(
        self, config: SForgeConfig, *,
        backend_factory: Callable[[SForgeConfig], ContainerBackend] | None = None,
    ):
        if config.judge_max_concurrent < 1 or config.judge_max_pending < 0:
            raise ValueError("judge_max_concurrent must be >= 1 and judge_max_pending >= 0")
        if config.judge_max_outstanding_per_run is not None and config.judge_max_outstanding_per_run < 1:
            raise ValueError("judge_max_outstanding_per_run must be >= 1 or unset")
        if config.judge_host_capacity is not None and (
            type(config.judge_host_capacity) is not int or config.judge_host_capacity < 1
        ):
            raise ValueError("judge_host_capacity must be a positive integer or unset")
        runs = config.judge_isolated_runs
        if not isinstance(runs, (list, tuple)) or any(not isinstance(r, str) or not r.strip() for r in runs):
            raise ValueError("Isolated judge runs must be a list of nonempty run IDs")
        if len(set(runs)) != len(runs) or len(runs) > config.judge_max_concurrent:
            raise ValueError("Each isolated run requires a unique ID and one configured evaluator")
        if runs and config.judge_host_capacity is not None and len(runs) > config.judge_host_capacity:
            raise ValueError("Host judge capacity cannot accommodate all isolated runs")
        self.config = config
        # Synchronous failures release while holding this same admission lock.
        self._admission_lock = threading.RLock()
        self._closed = False
        self._outstanding_by_run: dict[tuple[str, str], int] = {}
        self._capacity = threading.BoundedSemaphore(
            config.judge_max_concurrent + config.judge_max_pending
        )
        self._executor = ThreadPoolExecutor(
            max_workers=config.judge_max_concurrent, thread_name_prefix="sforge-judge"
        )
        # Divide the pending budget evenly. An overloaded arm cannot borrow a
        # peer's admission slots or block its evaluator with work already queued.
        self._run_lanes = {
            run: (ThreadPoolExecutor(max_workers=1, thread_name_prefix="sforge-arm-judge"),
                  threading.BoundedSemaphore(1 + config.judge_max_pending // len(runs)))
            for run in runs
        }
        self.admin_secret = get_admin_secret(config.log_dir)
        self.feedback_transport = FeedbackTransportRecorder(config.log_dir)
        self.tasks: dict[str, TaskSpec] = {}
        self.submissions: dict[str, dict] = {}  # submission_id -> {status, report, ...}
        self.game_sessions: dict[str, GameSessionState] = {}
        self.game_counters: dict[str, int] = {}
        self.run_history: dict[str, list[dict]] = {}  # run_id -> [entries]
        self.tokens: dict[str, dict] = {}  # token -> {task_id, run_id, next_agent, next_auto, judge_cpu_limit, judge_mem_limit}
        self.run_resource_limits: dict[str, dict] = {}  # run/task -> limits
        self.run_backends: dict[str, ContainerBackend] = {}  # run/task -> backend
        self._capacity_reconcile_lock = threading.Lock()
        self._capacity_reconciled_at = 0.0
        self._judge_queue_drained = threading.Event()
        self._game_lock = threading.Lock()
        self._history_lock = threading.Lock()
        self._tokens_lock = threading.Lock()
        self._registration_lock = threading.RLock()
        self._checkpoint_runs: set[str] = set()
        self._checkpoint_enabled = False
        self._custom_backend_factory = backend_factory
        self._backend_factory = create_backend_from_config if backend_factory is None else backend_factory
        self.backend = self._make_backend(config)
        # Keep the existing process/run admission owner. Host budgets are explicit
        # and apply only to Docker resources on this host, not remote sandboxes.
        self.host_capacity = (
            HostJudgeCapacity(config.judge_host_capacity)
            if config.judge_host_capacity is not None else None
        )
        if config.judge_host_memory_capacity is not None:
            memory_bytes = parse_bytes(config.judge_host_memory_capacity)
            if memory_bytes < 1 or self.host_capacity is None:
                raise ValueError("Judge host memory requires a positive size and host capacity")
            self.host_memory_capacity = HostJudgeCapacity(
                memory_bytes, os.environ.get("SFORGE_JUDGE_HOST_MEMORY_FILE", JUDGE_HOST_MEMORY_FILE)
            )
        else:
            self.host_memory_capacity = None
        self._reconcile_stale_capacity_holders(self.backend, force=True)

        self._reaper_stop = threading.Event()
        self._reaper_thread = threading.Thread(
            target=self._reaper_loop, daemon=True
        )
        self._reaper_thread.start()

    def _make_backend(self, config: SForgeConfig) -> ContainerBackend:
        backend = self._backend_factory(config)
        if backend is None:
            raise TypeError("backend factory must return a backend")
        return backend

    def _reconcile_stale_capacity_holders(
        self, backend: ContainerBackend, *, force: bool = False,
    ) -> int:
        """Reclaim dead-launcher capacity after backend resource cleanup.

        This is safe to call from every capacity waiter.  A process-local lock and
        monotonic interval bound backend queries, while ``HostJudgeCapacity`` keeps
        the cross-process compare-and-remove atomic.
        """
        reconcile = getattr(backend, "reconcile_judge_capacity_holder", None)
        if self.host_capacity is None or backend.backend_name != "docker" or not callable(reconcile):
            return 0
        if not self._capacity_reconcile_lock.acquire(blocking=False):
            return 0
        try:
            now = time.monotonic()
            if (
                not force
                and now - self._capacity_reconciled_at
                < JUDGE_CAPACITY_RECONCILE_INTERVAL
            ):
                return 0
            # Record the attempt before invoking the backend so concurrent waiters
            # do not fan out if cleanup is slow or raises.
            self._capacity_reconciled_at = now
            count = 0
            for capacity in (self.host_capacity, self.host_memory_capacity):
                if capacity is None:
                    continue
                reconciled = capacity.reconcile_stale_holders(reconcile)
                count += len(reconciled)
                if reconciled:
                    logger.info(
                        "Reconciled %d stale judge capacity holders", len(reconciled)
                    )
            return count
        finally:
            self._capacity_reconcile_lock.release()

    def _reaper_loop(self) -> None:
        while not self._reaper_stop.is_set():
            self._reaper_stop.wait(GAME_REAPER_INTERVAL)
            self._cleanup_idle_sessions()

    def _cleanup_idle_sessions(self) -> None:
        now = time.time()
        expired: list[str] = []
        with self._game_lock:
            for sid, sess in self.game_sessions.items():
                if now - sess.last_active > GAME_SESSION_IDLE_TIMEOUT:
                    expired.append(sid)
        for sid in expired:
            self._destroy_game_session(sid)

    def _destroy_game_session(self, session_id: str) -> None:
        with self._game_lock:
            sess = self.game_sessions.pop(session_id, None)
            if sess is not None:
                sess.operation_lock.acquire()
        if sess is None:
            return
        try:
            try:
                backend = self._get_backend(sess.run_id, sess.task_id)
                backend.cleanup_container(sess.container, logger)
            except Exception:
                logger.exception("Error cleaning up game container for session %s", session_id)
                with self._game_lock:
                    self.game_sessions.setdefault(session_id, sess)
                return
            self._archive_game_session(sess)
        finally:
            sess.operation_lock.release()

    def _game_log_dir(self, sess: GameSessionState) -> Path:
        return (
            self.config.log_dir / "runs" / sess.run_id
            / sess.task_id / "submissions" / f"game-{sess.game_num}"
        )

    def _archive_game_session(self, sess: GameSessionState) -> None:
        entry = {
            "type": "game",
            "round": f"game-{sess.game_num}",
            "session_id": sess.session_id,
            "task_id": sess.task_id,
            "max_score": sess.max_score,
            "peak_score": sess.peak_score,
            "final_score": sess.current_score,
            "score": sess.current_score,
            "moves": sess.moves,
            "steps": sess.steps,
        }
        if sess.max_score and sess.max_score > 0:
            entry["pass_rate"] = sess.current_score / sess.max_score
        else:
            entry["pass_rate"] = 0.0
        with self._history_lock:
            history_key = f"{sess.run_id}/{sess.task_id}"
            self.run_history.setdefault(history_key, []).append(entry)

        log_dir = self._game_log_dir(sess)
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "game_result.json").write_text(json.dumps(entry, indent=2, ensure_ascii=False))

    def _flush_game_step(self, sess: GameSessionState) -> None:
        log_dir = self._game_log_dir(sess)
        log_dir.mkdir(parents=True, exist_ok=True)
        step = sess.steps[-1]
        with open(log_dir / "steps.jsonl", "a") as f:
            f.write(json.dumps(step, ensure_ascii=False) + "\n")

    def _commit_game_session(self, sess: GameSessionState) -> None:
        """Publish a ready session with a unique, monotonic round identity."""
        with self._game_lock:
            history_key = f"{sess.run_id}/{sess.task_id}"
            game_num = self.game_counters.get(history_key, 0) + 1
            self.game_counters[history_key] = game_num
            sess.game_num = game_num
            self.game_sessions[sess.session_id] = sess

    # --- Token-based session registration ---

    def register_session(self, task_id: str, run_id: str,
                         judge_cpu_limit: int | None = None,
                         judge_mem_limit: str | None = None,
                         backend: str | None = None,
                         k8s_image_registry: str | None = None,
                         k8s_namespace: str | None = None,
                         k8s_node_selector: dict[str, str] | None = None,
                         k8s_kubeconfig: str | None = None,
                         max_agent_submissions: int | None = None,
                         submission_cooldown: int | None = None,
                         e2b_template_map: dict[str, str] | None = None,
                         e2b_template_namespace: str | None = None,
                         e2b_sandbox_ttl: int | None = None) -> str:
        with self._registration_lock:
            if run_id in self._checkpoint_runs:
                raise ValueError("A restored run cannot be cold-registered again")
            if self._checkpoint_enabled:
                from sforge.harness.checkpoint import RESTORE_MARKER
                if (self.config.log_dir / "runs" / run_id / RESTORE_MARKER).exists():
                    raise ValueError("A restored run cannot be cold-registered again")
            info, prepared_backend, token = self._prepare_session(task_id, run_id,
                judge_cpu_limit=judge_cpu_limit,
                judge_mem_limit=judge_mem_limit,
                backend=backend,
                k8s_image_registry=k8s_image_registry,
                k8s_namespace=k8s_namespace,
                k8s_node_selector=k8s_node_selector,
                k8s_kubeconfig=k8s_kubeconfig,
                max_agent_submissions=max_agent_submissions,
                submission_cooldown=submission_cooldown,
                e2b_template_map=e2b_template_map,
                e2b_template_namespace=e2b_template_namespace,
                e2b_sandbox_ttl=e2b_sandbox_ttl,
            )
            with self._tokens_lock:
                return self._publish_session(info, prepared_backend, token)

    def _publish_session(self, info: dict, prepared_backend: ContainerBackend | None, token: str) -> str:
        """Called under the native registration and token locks after preparation."""
        if self._checkpoint_enabled:
            if prepared_backend is not None:
                raise ValueError("Durable native registration requires the configured Docker backend")
            self._checkpoint_tail.register(info)
        run_key = self._run_key(info["run_id"], info["task_id"])
        self.run_resource_limits[run_key] = {
            "judge_cpu_limit": info["judge_cpu_limit"], "judge_mem_limit": info["judge_mem_limit"],
        }
        if prepared_backend is not None:
            self.run_backends[run_key] = prepared_backend
        self.tokens[token] = info
        return token

    def _prepare_session(self, task_id: str, run_id: str,
                         judge_cpu_limit: int | None = None,
                         judge_mem_limit: str | None = None,
                         backend: str | None = None,
                         k8s_image_registry: str | None = None,
                         k8s_namespace: str | None = None,
                         k8s_node_selector: dict[str, str] | None = None,
                         k8s_kubeconfig: str | None = None,
                         max_agent_submissions: int | None = None,
                         submission_cooldown: int | None = None,
                         e2b_template_map: dict[str, str] | None = None,
                         e2b_template_namespace: str | None = None,
                         e2b_sandbox_ttl: int | None = None) -> tuple[dict, ContainerBackend | None, str]:
        if task_id not in self.tasks:
            raise ValueError(f"Unknown task: {task_id}")
        if self._run_lanes and run_id not in self._run_lanes:
            raise ValueError("Run has no reserved evaluator")
        backend_override = None
        info = {
            "task_id": task_id,
            "run_id": run_id,
            "registration_id": uuid.uuid4().hex,
            # The judge token stays with the host. The work container receives
            # only this route, whose handlers expose the documented agent
            # feedback surface for this one registration.
            "feedback_relay_id": secrets.token_hex(16),
            "next_agent": 1,
            "next_auto": 1,
            "judge_cpu_limit": judge_cpu_limit,
            "judge_mem_limit": judge_mem_limit,
            "max_agent_submissions": max_agent_submissions,
            "submission_cooldown": submission_cooldown,
            "last_agent_submit_at": 0.0,
        }
        effective_e2b_template_map = (
            self.config.e2b_template_map
            if not e2b_template_map
            else e2b_template_map
        )
        effective_e2b_template_namespace = (
            self.config.e2b_template_namespace
            if not e2b_template_namespace
            else e2b_template_namespace
        )
        effective_e2b_sandbox_ttl = (
            self.config.e2b_sandbox_ttl
            if e2b_sandbox_ttl is None
            else e2b_sandbox_ttl
        )
        e2b_config_changed = backend == "e2b" and (
            effective_e2b_template_map != self.config.e2b_template_map
            or effective_e2b_template_namespace
            != self.config.e2b_template_namespace
            or effective_e2b_sandbox_ttl != self.config.e2b_sandbox_ttl
        )
        if self._custom_backend_factory is not None or (backend and (
            backend != self.backend.backend_name or e2b_config_changed
        )):
            effective_config = replace(
                self.config, backend=backend or self.config.backend,
                k8s_namespace=k8s_namespace or self.config.k8s_namespace,
                k8s_node_selector=k8s_node_selector or self.config.k8s_node_selector,
                k8s_image_registry=k8s_image_registry or self.config.k8s_image_registry,
                k8s_kubeconfig=k8s_kubeconfig or self.config.k8s_kubeconfig,
                e2b_template_map=effective_e2b_template_map,
                e2b_template_namespace=effective_e2b_template_namespace,
                e2b_sandbox_ttl=effective_e2b_sandbox_ttl,
                judge_cpu_limit=self.config.judge_cpu_limit if judge_cpu_limit is None else judge_cpu_limit,
                judge_mem_limit=self.config.judge_mem_limit if judge_mem_limit is None else judge_mem_limit,
            )
            if self._custom_backend_factory is not None:
                backend_override = self._make_backend(effective_config)
            else:
                from sforge.harness.backend.factory import create_backend
                backend_override = create_backend(
                    effective_config.backend,
                    k8s_namespace=effective_config.k8s_namespace,
                    k8s_node_selector=effective_config.k8s_node_selector,
                    k8s_image_registry=effective_config.k8s_image_registry,
                    k8s_kubeconfig=effective_config.k8s_kubeconfig,
                    e2b_template_map=effective_config.e2b_template_map,
                    e2b_template_namespace=effective_config.e2b_template_namespace,
                    e2b_sandbox_ttl=effective_config.e2b_sandbox_ttl,
                )
        return info, backend_override, secrets.token_hex(16)

    @staticmethod
    def _run_key(run_id: str, task_id: str) -> str:
        return f"{run_id}/{task_id}"

    def _get_backend(
        self, run_id: str | None = None, task_id: str | None = None,
    ) -> ContainerBackend:
        if run_id and task_id:
            backend = self.run_backends.get(self._run_key(run_id, task_id))
            if backend is not None:
                return backend
        return self.backend

    def resolve_token(self, token: str) -> dict:
        with self._tokens_lock:
            info = self.tokens.get(token)
            if info is None:
                raise KeyError("Invalid token")
            return dict(info)

    def resolve_feedback_relay(self, relay_id: str) -> tuple[str, dict]:
        """Resolve one opaque relay route to its host-held judge authority."""
        with self._tokens_lock:
            matches = [
                (token, info)
                for token, info in self.tokens.items()
                if info.get("feedback_relay_id") == relay_id
            ]
            if len(matches) != 1:
                raise KeyError("Invalid feedback relay")
            token, info = matches[0]
            return token, dict(info)

    def feedback_relay_id(self, token: str) -> str:
        info = self.resolve_token(token)
        relay_id = info.get("feedback_relay_id")
        if not isinstance(relay_id, str) or not relay_id:
            raise KeyError("Feedback relay unavailable")
        return relay_id

    def resolve_game_session(
        self, run_id: str, task_id: str, session_id: str,
    ) -> GameSessionState:
        """Resolve a game route without trusting caller-supplied run identity."""
        with self._game_lock:
            session = self.game_sessions.get(session_id)
            if (
                session is None
                or session.run_id != run_id
                or session.task_id != task_id
            ):
                raise KeyError("Invalid game session")
            return session

    @staticmethod
    def _agent_cooldown_wait(info: dict, now: float) -> int:
        """Use the same conservative whole-second wait for admission and readback."""
        cooldown = info.get("submission_cooldown")
        last_submit = info.get("last_agent_submit_at", 0.0)
        if cooldown is not None and last_submit > 0:
            elapsed = now - last_submit
            if elapsed < cooldown:
                return int(cooldown - elapsed) + 1
        return 0

    def agent_submission_availability(self, token: str) -> dict:
        """Read current token limits, not timestamps copied into result history.

        This snapshot reserves nothing. Submission admission still checks the
        token under its lock; other admission constraints can reject a request.
        observed_at is server Unix time; null remaining_submissions means no
        submission-count limit. Cooldown seconds use admission's rounding.
        """
        with self._tokens_lock:
            info = self.tokens.get(token)
            if info is None:
                raise KeyError("Invalid token")
            now = time.time()
            maximum = info.get("max_agent_submissions")
            used = info.get("next_agent", 1) - 1
            return {
                "observed_at": now,
                "remaining_submissions": None if maximum is None else max(0, maximum - used),
                "cooldown_remaining_seconds": self._agent_cooldown_wait(info, now),
            }

    def consume_round(self, token: str, kind: str = "agent") -> tuple[str, str, str, int | None, str | None, int | None]:
        """Allocate the next round ID for a token. Returns (task_id, run_id, round_id, cpu, mem, remaining).

        For agent submissions, enforces max_agent_submissions and submission_cooldown.
        Raises SubmissionBudgetExceeded or SubmissionCooldownActive on violation.
        Auto submissions are never limited.
        """
        with self._registration_lock, self._tokens_lock:
            info = self.tokens.get(token)
            if info is None:
                raise KeyError("Invalid token")

            if self._checkpoint_enabled:
                info = dict(info)

            if kind == "agent":
                max_subs = info.get("max_agent_submissions")
                next_n = info.get("next_agent", 1)

                if max_subs is not None and next_n > max_subs:
                    raise SubmissionBudgetExceeded(
                        f"Submission budget exhausted ({max_subs}/{max_subs} used)"
                    )
                remaining_wait = self._agent_cooldown_wait(info, time.time())
                if remaining_wait:
                    raise SubmissionCooldownActive(
                        f"Submission cooldown active — wait {remaining_wait}s before next submission. "
                        f"Continue working on improvements and submit again later."
                    )

                info["last_agent_submit_at"] = time.time()

            counter_key = f"next_{kind}"
            n = info.get(counter_key, 1)
            round_id = f"{kind}-{n}"
            info[counter_key] = n + 1
            if self._checkpoint_enabled:
                self._checkpoint_tail.reserve(info)
                self.tokens[token].update(info)

            remaining = None
            if kind == "agent":
                max_subs = info.get("max_agent_submissions")
                if max_subs is not None:
                    remaining = max_subs - n

        return info["task_id"], info["run_id"], round_id, info.get("judge_cpu_limit"), info.get("judge_mem_limit"), remaining

    def _record_submission(self, run_id: str, submission_id: str, task_id: str, round_id: str | None, report_dict: dict | None, error: str | None, status: str = "completed") -> None:
        entry = submission_history_entry(submission_id, task_id, round_id, report_dict, error, status)
        history_key = f"{run_id}/{task_id}"
        if self._checkpoint_enabled and status in ("completed", "error"):
            info = self._checkpoint_tail.info(run_id, task_id)
            self._checkpoint_tail.release(info, entry, report_dict, error, status)
        with self._history_lock:
            entries = self.run_history.setdefault(history_key, [])
            for i, e in enumerate(entries):
                if e.get("submission_id") == submission_id:
                    entries[i] = entry
                    return
            entries.append(entry)

    def get_run_history(self, run_id: str, task_id: str | None = None) -> dict:
        history_key = f"{run_id}/{task_id}" if task_id else run_id
        with self._history_lock:
            entries = list(self.run_history.get(history_key, []))

        # Resolve selection policy from task spec
        resolved_tid = task_id
        if not resolved_tid:
            for e in entries:
                if e.get("task_id"):
                    resolved_tid = e["task_id"]
                    break
        task_spec = self.tasks.get(resolved_tid) if resolved_tid else None
        policy = task_spec.judge.selection if task_spec else "pass_rate_first"
        direction = task_spec.judge.score_direction if task_spec else "maximize"

        best = select_best(entries, direction, policy)

        sub_entries = [e for e in entries if e.get("type") == "submission"]
        game_entries = [e for e in entries if e.get("type") == "game"]
        return {
            "run_id": run_id,
            "best_score": best["best_score"],
            "best_pass_rate": best["best_pass_rate"],
            "best_round": best["best_round"],
            # Game rounds are initiated by the agent through the game API.  The
            # runner therefore reports every archived game as an agent
            # submission; keep the authoritative history summary on the same
            # accounting basis so terminal integrity checks can compare them.
            "agent_submissions": (
                len(game_entries)
                + sum(
                    1
                    for e in sub_entries
                    if (e.get("round") or "").startswith("agent-")
                )
            ),
            "auto_submissions": sum(1 for e in sub_entries if (e.get("round") or "").startswith("auto-")),
            "entries": entries,
        }

    def get_agent_history(self, token: str) -> dict:
        """Project only feedback intentionally visible to the current solver."""
        info = self.resolve_token(token)
        full = self.get_run_history(info["run_id"], info["task_id"])
        availability = self.agent_submission_availability(token)
        agent_entries = [
            entry for entry in full.get("entries", [])
            if (entry.get("round") or "").startswith("agent-")
        ]
        task_spec = self.tasks.get(info["task_id"])
        policy = task_spec.judge.selection if task_spec else "pass_rate_first"
        direction = task_spec.judge.score_direction if task_spec else "maximize"
        best = select_best(agent_entries, direction, policy)
        return {
            "run_id": info["run_id"],
            "best_score": best["best_score"],
            "best_pass_rate": best["best_pass_rate"],
            "best_round": best["best_round"],
            "agent_submissions": sum(
                1 for entry in agent_entries if entry.get("type") == "submission"
            ),
            "auto_submissions": 0,
            "agent_submission_availability": availability,
            "entries": agent_entries,
        }

    def get_result_for_token(
        self, token: str, submission_id: str, *, agent_only: bool = False,
    ) -> dict | None:
        """Return a result only when the submission belongs to this registration."""
        info = self.resolve_token(token)
        result = self.get_result(submission_id)
        visible = bool(
            result
            and result.get("run_id") == info["run_id"]
            and result.get("task_id") == info["task_id"]
            and (
                not agent_only
                or (result.get("round") or "").startswith("agent-")
            )
        )
        if not visible:
            return None
        return result

    def load_tasks(self) -> None:
        benchmark = load_benchmark(self.config.tasks_dir)
        task_list = load_all_tasks(self.config.tasks_dir, benchmark)
        self.tasks = {t.task_id: t for t in task_list}

    def close(self) -> None:
        """Drain accepted evaluations on graceful shutdown; stop new admission."""
        with self._admission_lock:
            self._closed = True
        self._reaper_stop.set()
        self._executor.shutdown(wait=True)
        for executor, _ in self._run_lanes.values():
            executor.shutdown(wait=True)
        self._reaper_thread.join()
        if self._checkpoint_enabled:
            # The application owns shutdown through its lifespan. Release the
            # writer only after every accepted evaluation has drained.
            self._checkpoint_tail.close()

    def _evaluation_lane(self, run_id: str | None):
        if not self._run_lanes:
            return self._executor, self._capacity
        if run_id not in self._run_lanes:
            raise JudgeCapacityExceeded("Run has no reserved evaluator")
        return self._run_lanes[run_id]

    def _reserve(self, run_id: str | None, task_id: str) -> None:
        limit = self.config.judge_max_outstanding_per_run
        key = (run_id, task_id) if run_id is not None and limit is not None else None
        if key is not None and self._outstanding_by_run.get(key, 0) >= limit:
            raise JudgeCapacityExceeded("Run/task evaluation capacity full; retry this snapshot later")
        _, capacity = self._evaluation_lane(run_id)
        if self._closed or not capacity.acquire(blocking=False):
            raise JudgeCapacityExceeded("Judge capacity full; retry this snapshot later")
        if key is not None:
            self._outstanding_by_run[key] = self._outstanding_by_run.get(key, 0) + 1

    def _release(self, run_id: str | None, task_id: str) -> None:
        with self._admission_lock:
            if run_id is not None and self.config.judge_max_outstanding_per_run is not None:
                key = (run_id, task_id)
                remaining = self._outstanding_by_run[key] - 1
                if remaining:
                    self._outstanding_by_run[key] = remaining
                else:
                    del self._outstanding_by_run[key]
            _, capacity = self._evaluation_lane(run_id)
            capacity.release()

    def submit_for_token(self, token: str, archive: bytes, kind: str) -> tuple[str, str, int | None]:
        """Reserve capacity before spending a round or starting its cooldown."""
        with self._admission_lock:
            session = self.resolve_token(token)
            run_id, task_id = session["run_id"], session["task_id"]
            self._reserve(run_id, task_id)
            try:
                # Spool before consuming budget: disk failure must not spend a round.
                spool = self._spool(archive)
            except BaseException:
                self._release(run_id, task_id)
                raise
            try:
                task_id, run_id, round_id, cpu, mem, remaining = self.consume_round(token, kind)
            except BaseException:
                try:
                    spool.unlink(missing_ok=True)
                finally:
                    self._release(run_id, task_id)
                raise
            submission_id = self._enqueue(
                task_id, spool, run_id, round_id, cpu, mem,
                hashlib.sha256(archive).hexdigest(), len(archive), kind=kind,
            )
            return submission_id, round_id, remaining

    def submit(self, task_id: str, archive: bytes, run_id: str | None = None, round: str | None = None,
               judge_cpu_limit: int | None = None, judge_mem_limit: str | None = None) -> str:
        """Submit within the same aggregate and optional run/task limits as HTTP."""
        with self._admission_lock:
            if task_id not in self.tasks:
                raise ValueError(f"Unknown task: {task_id}")
            self._reserve(run_id, task_id)
            try:
                spool = self._spool(archive)
            except BaseException:
                self._release(run_id, task_id)
                raise
            return self._enqueue(task_id, spool, run_id, round, judge_cpu_limit, judge_mem_limit,
                                 hashlib.sha256(archive).hexdigest(), len(archive))

    def _spool(self, archive: bytes) -> Path:
        # Queued snapshots must not accumulate in RAM. Retain the spool after a
        # process crash for operator recovery, but do not claim automatic replay.
        directory = self.config.log_dir / "judge-pending"
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=directory, suffix=".tar.gz", delete=False) as f:
            path = Path(f.name)
            try:
                f.write(archive)
            except BaseException:
                path.unlink(missing_ok=True)
                raise
        return path

    def _enqueue(self, task_id: str, spool: Path, run_id: str | None,
                 round_id: str | None, cpu: int | None, mem: str | None,
                 archive_sha256: str, archive_bytes: int,
                 *, kind: str | None = None) -> str:
        submission_id = uuid.uuid4().hex[:12]
        if self._checkpoint_enabled:
            try:
                # The queue owns this spooled snapshot. Publish acceptance after
                # the durable reservation and before an executor can grade it.
                archive = spool.read_bytes()
                if len(archive) != archive_bytes or hashlib.sha256(archive).hexdigest() != archive_sha256:
                    raise ValueError("Accepted native spool differs from its snapshot")
                with self._registration_lock, self._tokens_lock:
                    info = self._checkpoint_tail.info(run_id, task_id)
                    self._checkpoint_tail.accept(info, submission_id, round_id, archive)
            except BaseException:
                try:
                    spool.unlink(missing_ok=True)
                finally:
                    self._release(run_id, task_id)
                raise
        self.submissions[submission_id] = {
            "status": SubmissionStatus.QUEUED, "task_id": task_id,
            "run_id": run_id, "round": round_id, "report": None, "error": None,
            "submitted_at": time.time(),
            "archive_sha256": archive_sha256, "archive_bytes": archive_bytes,
        }
        try:
            self._record_submission(run_id or submission_id, submission_id, task_id,
                                    round_id, None, None, status="queued")
            executor, _ = self._evaluation_lane(run_id)
            executor.submit(self._grade_spooled, submission_id, task_id,
                                  spool, run_id, round_id, cpu, mem, kind)
        except BaseException as e:
            try:
                if self._checkpoint_enabled:
                    self._publish_terminal_result(submission_id, task_id, run_id,
                                                  round_id, None, str(e), SubmissionStatus.ERROR)
                else:
                    self.submissions[submission_id].update(status=SubmissionStatus.ERROR, error=str(e))
                    self._record_submission(run_id or submission_id, submission_id, task_id,
                                            round_id, None, str(e), status="error")
            finally:
                try:
                    spool.unlink(missing_ok=True)
                finally:
                    self._release(run_id, task_id)
            raise
        return submission_id

    def _grade_spooled(self, submission_id: str, task_id: str, spool: Path,
                       run_id: str | None, round_id: str | None,
                       cpu: int | None, mem: str | None,
                       kind: str | None = None) -> None:
        backend = self._get_backend(run_id, task_id)
        token = f"{os.getpid()}:{submission_id}"
        slot_acquired = memory_acquired = False
        owner_id = None
        try:
            if self.host_capacity is not None and backend.backend_name == "docker":
                owner_id = backend.judge_capacity_owner_id()
                if owner_id is None:
                    raise RuntimeError("Docker engine identity is required for host judge capacity")
                memory_units = None
                if self.host_memory_capacity is not None:
                    memory_limit = (mem if mem is not None else self.config.judge_mem_limit)
                    if memory_limit is None:
                        memory_limit = self.tasks[task_id].judge.mem_limit
                    memory_units = (parse_bytes(memory_limit) if memory_limit is not None
                                    else self.host_memory_capacity.capacity)
                    if memory_units < 1 or memory_units > self.host_memory_capacity.capacity:
                        raise ValueError("Judge memory requirement exceeds host memory capacity")
                while True:
                    if self._judge_queue_drained.is_set():
                        raise RuntimeError("Judge capacity wait cancelled before grading")
                    slot_acquired = self.host_capacity.try_acquire(
                        token, owner_id, memory_capacity=self.host_memory_capacity,
                        memory_units=memory_units or 0,
                        priority=1 if kind == "agent" else 0,
                    )
                    if slot_acquired:
                        memory_acquired = self.host_memory_capacity is not None
                        break
                    self._reconcile_stale_capacity_holders(backend)
                    time.sleep(0.2)
            self._grade_worker(submission_id, task_id, spool.read_bytes(), run_id, round_id, cpu, mem)
        except Exception as e:
            if self._checkpoint_enabled:
                self._publish_terminal_result(submission_id, task_id, run_id,
                                              round_id, None, str(e), SubmissionStatus.ERROR)
            else:
                self.submissions[submission_id].update(status=SubmissionStatus.ERROR, error=str(e))
                self._record_submission(run_id or submission_id, submission_id, task_id,
                                        round_id, None, str(e), status="error")
        finally:
            # A terminal report or cleanup exception does not establish that the
            # owned container disappeared. Retain both ledgers if Docker cannot
            # confirm absence; a later dead-owner reconciliation can recover them.
            cleaned = not slot_acquired
            if slot_acquired:
                try:
                    cleaned = backend.reconcile_judge_capacity_holder(token, owner_id)
                except Exception:
                    logger.exception("Judge cleanup unconfirmed; retaining host capacity")
            if cleaned:
                for acquired, capacity in ((memory_acquired, self.host_memory_capacity),
                                           (slot_acquired, self.host_capacity)):
                    if acquired:
                        try:
                            capacity.release(token)
                        except Exception:
                            logger.exception("Could not release confirmed judge host capacity")
            try:
                spool.unlink(missing_ok=True)
            finally:
                if self.host_capacity is not None:
                    try:
                        self.host_capacity.cancel_waiter(token)
                    except Exception:
                        logger.exception("Could not cancel judge waiter %s", token)
                self._release(run_id, task_id)

    def _grade_worker(self, submission_id: str, task_id: str, archive: bytes,
                      run_id: str | None = None, round: str | None = None,
                      judge_cpu_limit: int | None = None, judge_mem_limit: str | None = None) -> None:
        self.submissions[submission_id]["status"] = SubmissionStatus.RUNNING
        self._record_submission(run_id or submission_id, submission_id, task_id,
                                round, None, None, status="running")
        try:
            task_spec = self.tasks[task_id]
            log_run_id = run_id or submission_id
            sub_num = str(round) if round is not None else "1"
            sub_log_dir = (
                self.config.log_dir / "runs" / log_run_id
                / task_id / "submissions" / sub_num
            )
            overrides: dict = {}
            if judge_cpu_limit is not None:
                overrides["judge_cpu_limit"] = judge_cpu_limit
            if judge_mem_limit is not None:
                overrides["judge_mem_limit"] = judge_mem_limit
            config = replace(self.config, **overrides) if overrides else self.config
            report = judge_submission(
                task_spec=task_spec,
                archive=archive,
                config=config,
                backend=self._get_backend(run_id, task_id),
                submission_id=submission_id,
                submitted_at=self.submissions[submission_id]["submitted_at"],
                log_dir=sub_log_dir,
            )
            report_dict = report.to_dict()
            report_dict.pop("score_0_100", None)
            status, error = SubmissionStatus.COMPLETED, None
        except Exception as e:
            if self._checkpoint_enabled:
                logger.exception("Native evaluation failed")
            report_dict, status, error = None, SubmissionStatus.ERROR, str(e)
        self._publish_terminal_result(submission_id, task_id, run_id, round,
                                      report_dict, error, status)

    def _publish_terminal_result(self, submission_id: str, task_id: str,
                                 run_id: str | None, round_id: str | None,
                                 report: dict | None, error: str | None,
                                 status: SubmissionStatus) -> bool:
        """One publication order for grading and bounded-queue failures."""
        log_run_id = run_id or submission_id
        with self._registration_lock:
            if self._checkpoint_enabled:
                try:
                    # Persistence failure is not a failed evaluation. Keep the
                    # pending native result and block further admission/readback.
                    self._record_submission(log_run_id, submission_id, task_id, round_id, report, error, status=status)
                except Exception:
                    info = self._checkpoint_tail.info(run_id, task_id)
                    self._checkpoint_tail.failed.add(info["registration_id"])
                    logger.exception("Native terminal result could not be durably released")
                    return False
            self.submissions[submission_id].update(status=status, report=report, error=error)
            if not self._checkpoint_enabled:
                self._record_submission(log_run_id, submission_id, task_id, round_id, report, error, status=status)
            return True

    def get_result(self, submission_id: str) -> dict | None:
        if self._checkpoint_enabled:
            # A released result is visible only after its durable publication.
            with self._registration_lock:
                value = self.submissions.get(submission_id)
                if value is not None and value["status"] not in (SubmissionStatus.COMPLETED, SubmissionStatus.ERROR):
                    self._checkpoint_tail.require_available(self._checkpoint_tail.info(value["run_id"], value["task_id"]))
                return dict(value) if value is not None else None
        return self.submissions.get(submission_id)

    # --- Game session management ---

    def create_game_session(self, run_id: str, task_id: str) -> tuple[GameSessionState, dict]:
        """Start a game container and create a new game session.

        Returns (session_state, response_from_container).
        """
        task_spec = self.tasks.get(task_id)
        if task_spec is None:
            raise ValueError(f"Unknown task: {task_id}")
        if not task_spec.game_mode:
            raise ValueError(f"Task {task_id} is not a game task")
        if not task_spec.judge.game_server_cmd:
            raise ValueError(f"Task {task_id} has no game_server_cmd")

        with self._game_lock:
            if len(self.game_sessions) >= GAME_SESSION_MAX:
                raise RuntimeError(f"Too many active game sessions ({GAME_SESSION_MAX})")

        # Evict oldest sessions for this run+task if at per-task cap
        with self._game_lock:
            task_sessions = sorted(
                [s for s in self.game_sessions.values() if s.run_id == run_id and s.task_id == task_id],
                key=lambda s: s.game_num,
            )
            to_evict = task_sessions[:len(task_sessions) - GAME_SESSION_PER_RUN_MAX + 1] if len(task_sessions) >= GAME_SESSION_PER_RUN_MAX else []
        for sess in to_evict:
            logger.info("Evicting oldest game session %s for %s/%s", sess.session_id, run_id, task_id)
            self._destroy_game_session(sess.session_id)

        backend = self._get_backend(run_id, task_id)

        if backend.backend_name == "docker":
            from sforge.harness.backend.docker_backend import DockerBackend
            assert isinstance(backend, DockerBackend)
            build_judge_image(task_spec, self.config, backend.client, force_rebuild=False)

        session_id = uuid.uuid4().hex[:12]
        container_name = f"sforge.game.{task_id}.{session_id}"
        port = GAME_CONTAINER_PORT
        env = get_container_env(self.config, include_judge_extra=True)
        game_headers = {}
        if backend.backend_name == "e2b":
            sanitize_e2b_proxy_env(env)
            # E2B exposes the service through a public gateway. Protect the
            # in-sandbox app as well as the outer Judge route.
            game_token = uuid.uuid4().hex
            env["SFORGE_GAME_TOKEN"] = game_token
            game_headers["X-SForge-Game-Token"] = game_token

        rl = self.run_resource_limits.get(self._run_key(run_id, task_id), {})
        cpu = rl.get("judge_cpu_limit")
        mem = rl.get("judge_mem_limit")
        # fall back to task defaults
        if cpu is None:
            cpu = task_spec.judge.cpu_limit
        if mem is None:
            mem = task_spec.judge.mem_limit
        handle = None
        committed = False
        try:
            handle = backend.create_container(
                task_spec.judge_image_key,
                container_name,
                environment=env,
                cpu_limit=cpu,
                mem_limit=mem,
            )
            backend.start_container(handle)
            logger.info(
                "Game container started: %s (%s)", container_name, handle.id[:12],
            )

            backend.copy_to_container(
                handle, GAME_SERVER_APP_PATH,
                PurePosixPath("/tmp/game_server_app.py"),
            )
            backend.exec_run(
                handle, f"/bin/bash -c '{task_spec.judge.game_server_cmd} &'",
                detach=True,
            )

            endpoint = backend.get_service_endpoint(handle, port)
            container_url = endpoint.url
            game_headers = {**endpoint.headers, **game_headers}

            deadline = time.time() + GAME_CONTAINER_READY_TIMEOUT
            while time.time() < deadline:
                try:
                    resp = requests.get(
                        f"{container_url}/health", headers=game_headers, timeout=5,
                    )
                    if resp.status_code == 200:
                        break
                except requests.RequestException:
                    pass
                time.sleep(GAME_CONTAINER_READY_POLL)
            else:
                raise RuntimeError("Game container did not become ready in time")

            gateway_attempts = (
                GAME_GATEWAY_ATTEMPTS if backend.backend_name == "e2b" else 1
            )
            for attempt in range(1, gateway_attempts + 1):
                try:
                    resp = requests.post(
                        f"{container_url}/new", json={}, headers=game_headers,
                        timeout=10,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except (requests.RequestException, ValueError) as exc:
                    if attempt == gateway_attempts:
                        raise RuntimeError(
                            "Game gateway failed while creating a session after "
                            f"{gateway_attempts} attempts: {exc}"
                        ) from exc
                    time.sleep(GAME_GATEWAY_BACKOFF * attempt)

            sess = GameSessionState(
                session_id=session_id,
                run_id=run_id,
                task_id=task_id,
                game_num=0,
                container=handle,
                container_url=container_url,
                headers=game_headers,
                max_score=data.get("max_score", 0),
                peak_score=data.get("peak_score", 0),
                current_score=data.get("score", 0),
                moves=data.get("moves", 0),
                done=data.get("done", False),
            )
            self._commit_game_session(sess)
            committed = True
            return sess, data
        except Exception as original_error:
            if handle is not None and not committed:
                try:
                    backend.cleanup_container(handle, logger)
                except Exception as cleanup_error:
                    raise RuntimeError(
                        "Game container initialization failed and cleanup also "
                        f"failed: {cleanup_error}"
                    ) from original_error
            raise

    def game_step(self, session_id: str, action: str) -> dict:
        with self._game_lock:
            sess = self.game_sessions.get(session_id)
            if sess is not None:
                sess.operation_lock.acquire()
        if sess is None:
            raise KeyError(f"Game session not found: {session_id}")
        try:
            resp = requests.post(
                f"{sess.container_url}/step",
                json={"action": action},
                headers=sess.headers,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            sess.current_score = data.get("score", sess.current_score)
            sess.max_score = data.get("max_score", sess.max_score)
            sess.peak_score = data.get("peak_score", sess.peak_score)
            sess.moves = data.get("moves", sess.moves)
            sess.done = data.get("done", sess.done)
            sess.last_active = time.time()
            sess.steps.append({
                "move": sess.moves,
                "action": action,
                "observation": data.get("observation", ""),
                "score": data.get("score", 0),
                "peak_score": data.get("peak_score", 0),
                "max_score": data.get("max_score", 0),
                "done": data.get("done", False),
            })

            self._flush_game_step(sess)

            if sess.done:
                threading.Thread(
                    target=self._auto_close_session,
                    args=(session_id,),
                    daemon=True,
                ).start()

            return data
        finally:
            sess.operation_lock.release()

    def _auto_close_session(self, session_id: str) -> None:
        with self._game_lock:
            sess = self.game_sessions.pop(session_id, None)
            if sess is not None:
                sess.operation_lock.acquire()
        if sess is None:
            return
        try:
            backend = self._get_backend(sess.run_id, sess.task_id)
            if backend.backend_name != "e2b":
                self._archive_game_session(sess)
                try:
                    backend.cleanup_container(sess.container, logger)
                except Exception:
                    logger.exception(
                        "Error auto-closing game session %s", session_id,
                    )
                return
            try:
                backend.cleanup_container(sess.container, logger)
            except Exception:
                logger.exception("Error auto-closing game session %s", session_id)
                with self._game_lock:
                    self.game_sessions.setdefault(session_id, sess)
                return
            self._archive_game_session(sess)
            logger.info("Auto-closed done game session %s (max_score=%d)", session_id, sess.max_score)
        finally:
            sess.operation_lock.release()

    def game_status(self, session_id: str) -> dict:
        with self._game_lock:
            sess = self.game_sessions.get(session_id)
            if sess is not None:
                sess.operation_lock.acquire()
        if sess is None:
            raise KeyError(f"Game session not found: {session_id}")
        try:
            resp = requests.get(f"{sess.container_url}/status", headers=sess.headers, timeout=5)
            resp.raise_for_status()
            data = resp.json()

            sess.current_score = data.get("score", sess.current_score)
            sess.max_score = data.get("max_score", sess.max_score)
            sess.peak_score = data.get("peak_score", sess.peak_score)
            sess.moves = data.get("moves", sess.moves)
            sess.done = data.get("done", sess.done)
            sess.last_active = time.time()

            return data
        finally:
            sess.operation_lock.release()

    def close_game_session(self, session_id: str) -> dict:
        with self._game_lock:
            sess = self.game_sessions.pop(session_id, None)
            if sess is not None:
                sess.operation_lock.acquire()
        if sess is None:
            raise KeyError(f"Game session not found: {session_id}")
        try:
            try:
                resp = requests.post(f"{sess.container_url}/close", headers=sess.headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                data = {
                    "final_score": sess.current_score,
                    "peak_score": sess.peak_score,
                    "max_score": sess.max_score,
                    "moves": sess.moves,
                }
            backend = self._get_backend(sess.run_id, sess.task_id)
            if backend.backend_name != "e2b":
                self._archive_game_session(sess)
                try:
                    backend.cleanup_container(sess.container, logger)
                except Exception:
                    logger.exception(
                        "Error cleaning up game container for session %s",
                        session_id,
                    )
                return data
            try:
                backend.cleanup_container(sess.container, logger)
            except Exception:
                with self._game_lock:
                    self.game_sessions.setdefault(session_id, sess)
                raise
            self._archive_game_session(sess)
            return data
        finally:
            sess.operation_lock.release()

    def close_all_game_sessions(self, run_id: str, task_id: str) -> int:
        backend = self._get_backend(run_id, task_id)
        with self._game_lock:
            sessions = [
                (sid, sess) for sid, sess in self.game_sessions.items()
                if sess.run_id == run_id and sess.task_id == task_id
            ]
            for sid, sess in sessions:
                self.game_sessions.pop(sid, None)
                sess.operation_lock.acquire()
        failures = []
        try:
            if backend.backend_name == "e2b":
                for sid, sess in sessions:
                    try:
                        backend.cleanup_container(sess.container, logger)
                    except Exception as exc:
                        logger.exception(
                            "Error cleaning up game container for session %s", sid,
                        )
                        failures.append((sid, sess, exc))
                        continue
                    self._archive_game_session(sess)
                if failures:
                    detail = ", ".join(f"{sid}: {exc}" for sid, _, exc in failures)
                    raise RuntimeError(f"Failed to clean up game containers: {detail}")
                return len(sessions)

            # Preserve the existing non-blocking Docker/K8s cleanup behavior.
            containers_to_cleanup = []
            for sid, sess in sessions:
                self._archive_game_session(sess)
                containers_to_cleanup.append((sid, sess.container))

            def _bg_cleanup():
                for sid, container in containers_to_cleanup:
                    try:
                        backend.cleanup_container(container, logger)
                    except Exception:
                        logger.exception("Error cleaning up game container for session %s", sid)
            threading.Thread(target=_bg_cleanup, daemon=True).start()
            return len(sessions)
        finally:
            for _, sess in sessions:
                if sess.operation_lock.locked():
                    sess.operation_lock.release()
            if failures:
                # A reader waits for operation_lock while holding _game_lock.
                # Publish retries only after every owned operation lock is free.
                with self._game_lock:
                    for sid, sess, _ in failures:
                        self.game_sessions.setdefault(sid, sess)



# --- FastAPI app factory ---


def create_app(
    config: SForgeConfig | None = None, *, checkpoints: bool = False,
    backend_factory: Callable[[SForgeConfig], ContainerBackend] | None = None,
) -> FastAPI:
    if config is None:
        config = load_config()

    if type(checkpoints) is not bool:
        raise ValueError("checkpoints must be an explicit boolean")
    state = JudgeState(config, backend_factory=backend_factory)
    state.load_tasks()
    state._checkpoint_enabled = checkpoints

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            state.close()

    app = FastAPI(title="SForge Judge", version=__version__, lifespan=lifespan)
    app.state.judge = state
    if checkpoints:
        from sforge.harness.checkpoint import install_routes
        install_routes(app, state)

    def submission_receipt(result: dict) -> SubmissionReceipt:
        # A restored legacy result may lack provenance. Never hash the current
        # workspace or infer an archive from the result's round/lookup ID.
        return SubmissionReceipt(
            schema_version="sforge_submission_receipt_v1",
            task_id=result["task_id"], round_id=result["round"],
            archive_sha256=result.get("archive_sha256"),
            archive_bytes=result.get("archive_bytes"),
        )

    def result_response(
        submission_id: str, result: dict | None, *, include_receipt: bool = False,
    ) -> ResultResponse:
        if result is None:
            raise HTTPException(status_code=404, detail="Submission not found")
        return ResultResponse(
            submission_id=submission_id,
            status=result["status"],
            report=json_safe_feedback_payload(result["report"]),
            error=result["error"],
            **({"receipt": submission_receipt(result)} if include_receipt else {}),
        )

    @app.get("/api/v1/result/{submission_id}", response_model_exclude_unset=True)
    def get_result(
        submission_id: str, token: str | None = Query(None),
        include_receipt: bool = Query(False),
    ) -> ResultResponse:
        """Host API: get a result only within the token's registered run."""
        if not token:
            raise HTTPException(status_code=401, detail="Token required")
        try:
            result = state.get_result_for_token(
                token, submission_id, agent_only=include_receipt,
            )
        except KeyError:
            raise HTTPException(status_code=401, detail="Invalid token")
        return result_response(
            submission_id, result, include_receipt=include_receipt,
        )

    @app.post("/api/v1/register")
    def register(req: RegisterRequest) -> RegisterResponse:
        """Register a session and get a token for submissions."""
        if req.admin_secret != state.admin_secret:
            raise HTTPException(status_code=403, detail="Invalid admin secret")
        try:
            token = state.register_session(
                req.task_id, req.run_id,
                judge_cpu_limit=req.judge_cpu_limit,
                judge_mem_limit=req.judge_mem_limit,
                backend=req.backend,
                k8s_image_registry=req.k8s_image_registry,
                k8s_namespace=req.k8s_namespace,
                k8s_node_selector=req.k8s_node_selector,
                k8s_kubeconfig=req.k8s_kubeconfig,
                max_agent_submissions=req.max_agent_submissions,
                submission_cooldown=req.submission_cooldown,
                e2b_template_map=req.e2b_template_map,
                e2b_template_namespace=req.e2b_template_namespace,
                e2b_sandbox_ttl=req.e2b_sandbox_ttl,
            )
            return RegisterResponse(
                token=token,
                feedback_relay_id=state.feedback_relay_id(token),
            )
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/submit", response_model_exclude_unset=True)
    def submit(
        token: str = Form(...),
        archive: UploadFile = File(...),
        kind: str = Form("agent"),
        admin_secret: str = Form(""),
        include_receipt: bool = Form(False),
    ) -> SubmitResponse:
        """Submit an archive using a session token.

        The server resolves task_id, run_id, and assigns a round_id.
        kind=auto requires a valid admin_secret (host-side only).
        """
        if kind not in ("agent", "auto"):
            raise HTTPException(status_code=400, detail="kind must be 'agent' or 'auto'")
        if kind == "auto" and admin_secret != state.admin_secret:
            raise HTTPException(status_code=403, detail="admin_secret required for auto submissions")
        try:
            submission_id, round_id, remaining = state.submit_for_token(
                token, archive.file.read(), kind,
            )
            return SubmitResponse(
                submission_id=submission_id, round_id=round_id,
                status=SubmissionStatus.QUEUED, remaining_submissions=remaining,
                **({"receipt": submission_receipt(state.get_result(submission_id))}
                   if include_receipt else {}),
            )
        except KeyError:
            raise HTTPException(status_code=401, detail="Invalid token")
        except (SubmissionBudgetExceeded, SubmissionCooldownActive) as e:
            raise HTTPException(status_code=429, detail=str(e))
        except JudgeCapacityExceeded as e:
            raise HTTPException(status_code=503, detail=str(e), headers={"Retry-After": "5"})
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.get("/api/v1/history")
    def history(token: str = Query(...), admin_secret: str = Query("")) -> dict:
        """Get run history using a session token.

        Without admin_secret: returns only agent submissions and a read-only
        snapshot of current agent submission limits. History timestamps need
        not share the current session's cooldown clock. A snapshot is not a
        reservation or a guarantee of admission.
        With valid admin_secret: returns all entries including auto-eval (host-side view).
        """
        try:
            info = state.resolve_token(token)
        except KeyError:
            raise HTTPException(status_code=401, detail="Invalid token")
        full = state.get_run_history(info["run_id"], info["task_id"])
        if admin_secret == state.admin_secret:
            return json_safe_feedback_payload(full)
        try:
            return json_safe_feedback_payload(state.get_agent_history(token))
        except KeyError:
            raise HTTPException(status_code=401, detail="Invalid token")

    # Solver-facing capability relay. The opaque route selects exactly one
    # registration; no judge token or admin authority enters the work container.
    @app.post("/api/v1/feedback/{relay_id}/submit")
    def feedback_submit(
        relay_id: str, archive: UploadFile = File(...),
        include_receipt: bool = Form(False),
    ) -> SubmitResponse:
        exchange_id = None
        run_id = task_id = ""
        try:
            token, info = state.resolve_feedback_relay(relay_id)
            run_id, task_id = info["run_id"], info["task_id"]
            archive_bytes = archive.file.read()
            exchange_id = state.feedback_transport.request(
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="submit",
                payload=archive_bytes,
            )
            submission_id, round_id, remaining = state.submit_for_token(
                token, archive_bytes, "agent",
            )
            payload = {
                "submission_id": submission_id,
                "round_id": round_id,
                "status": SubmissionStatus.QUEUED,
                "remaining_submissions": remaining,
            }
            if include_receipt:
                payload["receipt"] = dict(
                    submission_receipt(state.get_result(submission_id))
                )
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="submit",
                subject_id=submission_id,
                payload=payload,
                status="accepted",
            )
            return SubmitResponse(**payload)
        except KeyError:
            if exchange_id is not None:
                state.feedback_transport.response(
                    exchange_id=exchange_id,
                    run_id=run_id,
                    task_id=task_id,
                    channel="active_submission",
                    operation="submit",
                    payload={"detail": "Feedback relay not found"},
                    status="not_found",
                )
            raise HTTPException(status_code=404, detail="Feedback relay not found")
        except SubmissionBudgetExceeded as exc:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="submit",
                payload={"detail": str(exc)},
                status="budget_exhausted",
            )
            raise HTTPException(status_code=429, detail=str(exc))
        except SubmissionCooldownActive as exc:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="submit",
                payload={"detail": str(exc)},
                status="cooldown_active",
            )
            raise HTTPException(status_code=429, detail=str(exc))
        except JudgeCapacityExceeded as exc:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="submit",
                payload={"detail": str(exc)},
                status="capacity_exceeded",
            )
            raise HTTPException(
                status_code=503, detail=str(exc), headers={"Retry-After": "5"},
            )
        except ValueError as exc:
            if exchange_id is not None:
                state.feedback_transport.response(
                    exchange_id=exchange_id,
                    run_id=run_id,
                    task_id=task_id,
                    channel="active_submission",
                    operation="submit",
                    payload={"detail": str(exc)},
                    status="invalid_request",
                )
            raise HTTPException(status_code=404, detail=str(exc))

    @app.get("/api/v1/feedback/{relay_id}/history")
    def feedback_history(relay_id: str) -> dict:
        try:
            token, info = state.resolve_feedback_relay(relay_id)
            run_id, task_id = info["run_id"], info["task_id"]
            exchange_id = state.feedback_transport.request(
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="history",
                payload={},
            )
            payload = json_safe_feedback_payload(state.get_agent_history(token))
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="history",
                payload=payload,
                status="delivered",
            )
            return payload
        except KeyError:
            raise HTTPException(status_code=404, detail="Feedback relay not found")

    @app.get(
        "/api/v1/feedback/{relay_id}/result/{submission_id}",
        response_model_exclude_unset=True,
    )
    def feedback_result(
        relay_id: str, submission_id: str,
        include_receipt: bool = Query(False),
    ) -> ResultResponse:
        exchange_id = None
        run_id = task_id = ""
        try:
            token, info = state.resolve_feedback_relay(relay_id)
            run_id, task_id = info["run_id"], info["task_id"]
            exchange_id = state.feedback_transport.request(
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="result",
                subject_id=submission_id,
                payload={"submission_id": submission_id},
            )
            result = state.get_result_for_token(
                token, submission_id, agent_only=True,
            )
        except KeyError:
            if exchange_id is not None:
                state.feedback_transport.response(
                    exchange_id=exchange_id,
                    run_id=run_id,
                    task_id=task_id,
                    channel="active_submission",
                    operation="result",
                    subject_id=submission_id,
                    payload={"detail": "Feedback relay not found"},
                    status="not_found",
                )
            raise HTTPException(status_code=404, detail="Feedback relay not found")
        if result is None:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="active_submission",
                operation="result",
                subject_id=submission_id,
                payload={"detail": "Submission not found"},
                status="not_found",
            )
            raise HTTPException(status_code=404, detail="Submission not found")
        payload = json_safe_feedback_payload({
            "submission_id": submission_id,
            "status": result["status"],
            "report": result["report"],
            "error": result["error"],
        })
        if include_receipt:
            payload["receipt"] = dict(submission_receipt(result))
        state.feedback_transport.response(
            exchange_id=exchange_id,
            run_id=run_id,
            task_id=task_id,
            channel="active_submission",
            operation="result",
            subject_id=submission_id,
            payload=payload,
            status="delivered",
        )
        return ResultResponse(**payload)

    # --- Game session routes ---

    @app.post("/api/v1/game/{run_id}/{task_id}/new")
    def game_new(run_id: str, task_id: str, req: GameNewRequest) -> GameNewResponse:
        """Start a new game session in a dedicated container."""
        exchange_id = state.feedback_transport.request(
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="new",
            payload={},
        )
        try:
            sess, data = state.create_game_session(run_id, task_id)
        except ValueError as e:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="new",
                payload={"detail": str(e)},
                status="invalid_request",
            )
            raise HTTPException(404, str(e))
        except (RuntimeError, BuildImageError) as e:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="new",
                payload={"detail": str(e)},
                status="unavailable",
            )
            raise HTTPException(503, str(e))
        payload = {
            "session_id": sess.session_id,
            "observation": data.get("observation", ""),
            "score": data.get("score", 0),
            "peak_score": data.get("peak_score", 0),
            "max_score": data.get("max_score", 0),
            "done": data.get("done", False),
            "moves": data.get("moves", 0),
        }
        state.feedback_transport.response(
            exchange_id=exchange_id,
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="new",
            subject_id=sess.session_id,
            payload=payload,
            status="delivered",
        )
        return GameNewResponse(**payload)

    @app.post("/api/v1/game/{run_id}/{task_id}/{session_id}/step")
    def game_step(run_id: str, task_id: str, session_id: str, req: GameStepRequest) -> GameStepResponse:
        try:
            state.resolve_game_session(run_id, task_id, session_id)
        except KeyError:
            raise HTTPException(404, f"Game session not found: {session_id}")
        exchange_id = state.feedback_transport.request(
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="step",
            subject_id=session_id,
            payload={"action": req.action},
        )
        try:
            data = state.game_step(session_id, req.action)
        except KeyError:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="step",
                subject_id=session_id,
                payload={"detail": "Game session not found"},
                status="not_found",
            )
            raise HTTPException(404, f"Game session not found: {session_id}")
        except requests.HTTPError as e:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="step",
                subject_id=session_id,
                payload={"detail": str(e)},
                status="upstream_rejected",
            )
            raise HTTPException(400, str(e))
        payload = {
            "session_id": session_id,
            "observation": data.get("observation", ""),
            "score": data.get("score", 0),
            "peak_score": data.get("peak_score", 0),
            "max_score": data.get("max_score", 0),
            "done": data.get("done", False),
            "moves": data.get("moves", 0),
        }
        state.feedback_transport.response(
            exchange_id=exchange_id,
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="step",
            subject_id=session_id,
            payload=payload,
            status="delivered",
        )
        return GameStepResponse(**payload)

    @app.get("/api/v1/game/{run_id}/{task_id}/{session_id}/status")
    def game_status(run_id: str, task_id: str, session_id: str) -> GameStatusResponse:
        try:
            state.resolve_game_session(run_id, task_id, session_id)
        except KeyError:
            raise HTTPException(404, f"Game session not found: {session_id}")
        exchange_id = state.feedback_transport.request(
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="status",
            subject_id=session_id,
            payload={},
        )
        try:
            data = state.game_status(session_id)
        except KeyError:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="status",
                subject_id=session_id,
                payload={"detail": "Game session not found"},
                status="not_found",
            )
            raise HTTPException(404, f"Game session not found: {session_id}")
        except requests.HTTPError as e:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="status",
                subject_id=session_id,
                payload={"detail": str(e)},
                status="upstream_rejected",
            )
            raise HTTPException(400, str(e))
        payload = {
            "session_id": session_id,
            "score": data.get("score", 0),
            "peak_score": data.get("peak_score", 0),
            "max_score": data.get("max_score", 0),
            "done": data.get("done", False),
            "moves": data.get("moves", 0),
        }
        state.feedback_transport.response(
            exchange_id=exchange_id,
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="status",
            subject_id=session_id,
            payload=payload,
            status="delivered",
        )
        return GameStatusResponse(**payload)

    @app.post("/api/v1/game/{run_id}/{task_id}/{session_id}/close")
    def game_close(run_id: str, task_id: str, session_id: str) -> GameCloseResponse:
        try:
            state.resolve_game_session(run_id, task_id, session_id)
        except KeyError:
            raise HTTPException(404, f"Game session not found: {session_id}")
        exchange_id = state.feedback_transport.request(
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="close",
            subject_id=session_id,
            payload={},
        )
        try:
            data = state.close_game_session(session_id)
        except KeyError:
            state.feedback_transport.response(
                exchange_id=exchange_id,
                run_id=run_id,
                task_id=task_id,
                channel="game",
                operation="close",
                subject_id=session_id,
                payload={"detail": "Game session not found"},
                status="not_found",
            )
            raise HTTPException(404, f"Game session not found: {session_id}")
        payload = {
            "session_id": session_id,
            "final_score": data.get("final_score", 0),
            "peak_score": data.get("peak_score", 0),
            "max_score": data.get("max_score", 0),
            "moves": data.get("moves", 0),
        }
        state.feedback_transport.response(
            exchange_id=exchange_id,
            run_id=run_id,
            task_id=task_id,
            channel="game",
            operation="close",
            subject_id=session_id,
            payload=payload,
            status="delivered",
        )
        return GameCloseResponse(**payload)

    @app.post("/api/v1/game/{run_id}/{task_id}/close-all")
    def game_close_all(run_id: str, task_id: str) -> dict:
        count = state.close_all_game_sessions(run_id, task_id)
        return {"closed": count}

    return app
