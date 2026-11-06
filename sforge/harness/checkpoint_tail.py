# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
# SPDX-License-Identifier: Apache-2.0
"""Private durability for the existing opt-in native checkpoint owner.

A reservation is durable before its counter is visible. A missing acceptance or
terminal result therefore holds recovery; a directory of reports never supplies
an inferred admission count. No credential is written to this store.
"""

from __future__ import annotations

import copy
import fcntl
import json
import math
import os
import shutil
import stat
import tempfile
import time
from pathlib import Path

from sforge.harness.checkpoint import (
    RESTORE_MARKER,
    SCHEMA,
    CheckpointUnavailable,
    JudgeCheckpoints,
    _encode_manifest,
    _decode_manifest,
    _same_released_value,
    _name,
    _read,
    _sha,
    _sync_directory,
    _write,
)

TAIL_SCHEMA = "sforge_judge_registration_tail_v1"
TAIL_NONFINITE_SCHEMA = "sforge_judge_registration_tail_v2"


class DurableWriteUnavailable(RuntimeError):
    """The native owner cannot acknowledge more facts after a storage failure."""


def _atomic(path: Path, raw: bytes) -> None:
    fd, name = tempfile.mkstemp(prefix=".state-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.resolve() != path or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise CheckpointUnavailable(
            "Native registration storage must be private and physical"
        )


def _storage_identity(folder: Path) -> dict:
    directory = folder.stat(follow_symlinks=False)
    lock = (folder / "writer.lock").stat(follow_symlinks=False)
    return {
        "directory": [directory.st_dev, directory.st_ino],
        "writer_lock": [lock.st_dev, lock.st_ino],
    }


def _lock(path: Path):
    if path.parent.resolve(strict=True) != path.parent:
        raise CheckpointUnavailable(
            "Native registration lock is outside its physical owner"
        )
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise CheckpointUnavailable("Native registration lock is not a file")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


class DurableRegistrations:
    """One native writer per registration, held until service shutdown/death."""

    def __init__(self, state):
        self.state = state
        self.bank = JudgeCheckpoints(state)
        self.root = self.bank.root / "checkpoint-registrations"
        self.writers = {}
        self.failed = set()
        self.closed = False

    def folder(self, run_id, task_id):
        return self.root / _name(run_id) / _name(task_id)

    def register(self, info):
        if self.closed:
            raise CheckpointUnavailable("Checkpoint writer is closed")
        folder = self.folder(info["run_id"], info["task_id"])
        # A retained registration, including an interrupted initial publish, must
        # never become a cold registration after process restart.
        if folder.exists():
            raise CheckpointUnavailable(
                "Retained native registration requires recovery"
            )
        _directory(self.root)
        _directory(folder.parent)
        folder.mkdir(mode=0o700)
        descriptor = _lock(folder / "writer.lock")
        self.writers[info["registration_id"]] = (descriptor, folder)
        task = self.state.tasks[info["task_id"]]
        try:
            origin_path = self.bank.root / "runs" / info["run_id"] / RESTORE_MARKER
            origin = (
                json.loads(_read(self.bank.root, origin_path))
                if origin_path.exists()
                else None
            )
            history = copy.deepcopy(
                self.state.run_history.get(
                    self.state._run_key(info["run_id"], info["task_id"]), []
                )
            )
            results = {
                entry["submission_id"]: {
                    key: self.state.submissions[entry["submission_id"]][key]
                    for key in ("status", "report", "error")
                }
                for entry in history
            }
            value = {
                "schema_version": TAIL_SCHEMA,
                "storage_identity": _storage_identity(folder),
                "source_run_id": info["run_id"],
                "task_id": info["task_id"],
                "source_registration_id": info["registration_id"],
                "evaluator": self.bank._binding(info["task_id"]),
                "resources": {
                    name: info[name]
                    if info[name] is not None
                    else (
                        getattr(self.state.config, name)
                        if getattr(self.state.config, name) is not None
                        else getattr(task.judge, name.removeprefix("judge_"))
                    )
                    for name in ("judge_cpu_limit", "judge_mem_limit")
                },
                "origin": origin,
                "history": history,
                "results": results,
                "accepted_archives": {},
                "reservations": {},
            }
            self._save(info, value)
        except BaseException:
            self.writers.pop(info["registration_id"], None)
            os.close(descriptor)
            # Keep a failed publication reserved: never guess whether its write
            # reached durable storage after an I/O error.
            raise

    def require_available(self, info):
        if info["registration_id"] in self.failed:
            raise DurableWriteUnavailable("Native checkpoint storage is unavailable")
        if self.closed or info["registration_id"] not in self.writers:
            raise CheckpointUnavailable("Original native writer is unavailable")

    def _load(self, info):
        self.require_available(info)
        folder = self.writers[info["registration_id"]][1]
        try:
            return folder, _decode_manifest(
                _read(self.bank.root, folder / "state.json"),
                finite_schema=TAIL_SCHEMA, nonfinite_schema=TAIL_NONFINITE_SCHEMA,
            )
        except (OSError, ValueError, TypeError) as error:
            self.failed.add(info["registration_id"])
            raise DurableWriteUnavailable(
                "Native checkpoint storage is unavailable"
            ) from error

    def _save(self, info, value):
        if self.closed:
            raise CheckpointUnavailable("Checkpoint writer is closed")
        folder = self.writers[info["registration_id"]][1]
        value = {
            **value,
            "observed_at_epoch": time.time(),
            "last_agent_submit_at": info["last_agent_submit_at"],
            "opportunity": {
                key: info[key]
                for key in (
                    "next_agent",
                    "next_auto",
                    "max_agent_submissions",
                    "submission_cooldown",
                )
            },
        }
        try:
            _atomic(folder / "state.json", _encode_manifest(
                value, nonfinite_schema=TAIL_NONFINITE_SCHEMA,
            ))
        except (OSError, ValueError, TypeError) as error:
            self.failed.add(info["registration_id"])
            raise DurableWriteUnavailable(
                "Native checkpoint storage is unavailable"
            ) from error

    def reserve(self, info):
        _, value = self._load(info)
        for kind in ("agent", "auto"):
            key = "next_" + kind
            previous = value["opportunity"][key]
            if info[key] != previous:
                if info[key] != previous + 1:
                    raise CheckpointUnavailable("Native reservation sequence differs")
                value["reservations"][f"{kind}-{previous}"] = info[
                    "last_agent_submit_at"
                ]
        self._save(info, value)

    def info(self, run_id, task_id):
        matches = [
            info
            for info in self.state.tokens.values()
            if info["run_id"] == run_id and info["task_id"] == task_id
        ]
        if len(matches) != 1:
            raise CheckpointUnavailable(
                "Native registration is unavailable or ambiguous"
            )
        return matches[0]

    def accept(self, info, submission_id, round_id, archive):
        from sforge.harness.judge_server import submission_history_entry

        folder, value = self._load(info)
        if round_id in value["accepted_archives"] or any(
            entry["round"] == round_id for entry in value["history"]
        ):
            raise CheckpointUnavailable(
                "Native round already has an accepted submission"
            )
        _name(round_id)
        if round_id not in value["reservations"]:
            raise CheckpointUnavailable("Native acceptance has no durable reservation")
        archive_path = folder / (round_id + ".tar.gz")
        try:
            _write(archive_path, archive)
            _sync_directory(folder)
        except OSError as error:
            self.failed.add(info["registration_id"])
            raise DurableWriteUnavailable(
                "Native accepted archive storage is unavailable"
            ) from error
        value["accepted_archives"][round_id] = {
            "submission_id": submission_id,
            "archive_sha256": _sha(archive),
            "archive_bytes": len(archive),
            "accepted_at_epoch": value["reservations"][round_id],
        }
        value["history"].append(
            submission_history_entry(
                submission_id, info["task_id"], round_id, None, None, "queued"
            )
        )
        value["history"].sort(
            key=lambda entry: (
                entry["round"].split("-")[0],
                int(entry["round"].split("-")[1]),
            )
        )
        value["results"][submission_id] = {
            "status": "queued",
            "report": None,
            "error": None,
        }
        self._save(info, value)

    def release(self, info, entry, report, error, status):
        _, value = self._load(info)
        sid = entry["submission_id"]
        if sid not in value["results"] or value["results"][sid]["status"] != "queued":
            raise CheckpointUnavailable(
                "Terminal result has no pending native acceptance"
            )
        value["history"] = [
            entry if e["submission_id"] == sid else e for e in value["history"]
        ]
        value["results"][sid] = {"status": status, "report": report, "error": error}
        self._save(info, value)

    def rollback(self, registration_id):
        writer = self.writers.pop(registration_id, None)
        if writer is not None:
            descriptor, folder = writer
            try:
                shutil.rmtree(folder)
                _sync_directory(folder.parent)
            finally:
                os.close(descriptor)

    def close(self):
        # A pending journal remains pending. Closing a writer is not settlement.
        with self.state._registration_lock:
            self.closed = True
            for descriptor, _ in self.writers.values():
                os.close(descriptor)
            self.writers.clear()

    def adopt(
        self,
        *,
        source_run_id,
        task_id,
        source_registration_id,
        checkpoint_id,
        manifest_sha256,
        capture_id,
    ):
        folder = self.folder(source_run_id, task_id)
        if not folder.is_dir():
            raise CheckpointUnavailable("Original durable registration is unavailable")
        descriptor = _lock(folder / "writer.lock")
        try:
            raw = _read(self.bank.root, folder / "state.json")
            value = _decode_manifest(raw, finite_schema=TAIL_SCHEMA, nonfinite_schema=TAIL_NONFINITE_SCHEMA)
            if value["storage_identity"] != _storage_identity(folder):
                raise CheckpointUnavailable(
                    "Durable tail was copied away from its original writer lock"
                )
            if (
                value["schema_version"] != TAIL_SCHEMA
                or value["source_run_id"] != source_run_id
                or value["task_id"] != task_id
                or value["source_registration_id"] != source_registration_id
            ):
                raise CheckpointUnavailable("Durable registration identity differs")
            _, prefix = self.bank._packet(checkpoint_id, manifest_sha256)
            if (
                prefix["task_id"] != task_id
                or value["evaluator"] != prefix["evaluator"]
                or value["resources"] != prefix["resources"]
            ):
                raise CheckpointUnavailable(
                    "Durable tail task, source or resource profile differs"
                )
            origin = value["origin"]
            marker = self.bank.root / "runs" / source_run_id / RESTORE_MARKER
            if (
                origin is not None
                and json.loads(_read(self.bank.root, marker)) != origin
            ):
                raise CheckpointUnavailable("Durable restore origin marker changed")
            if prefix["source_run_id"] == source_run_id:
                if (
                    prefix["source_registration_id"] != source_registration_id
                    or origin is not None
                ):
                    raise CheckpointUnavailable("Checkpoint registration differs")
                mapped = prefix["history"]
            else:
                if (
                    not isinstance(origin, dict)
                    or origin["checkpoint_id"] != checkpoint_id
                    or origin["manifest_sha256"] != manifest_sha256
                    or origin["source_run_id"] != prefix["source_run_id"]
                    or origin["source_registration_id"]
                    != prefix["source_registration_id"]
                    or origin["task_id"] != task_id
                ):
                    raise CheckpointUnavailable("Original restore lineage differs")
                ids = origin["submission_id_map"]
                if set(ids) != {e["submission_id"] for e in prefix["history"]} or len(
                    set(ids.values())
                ) != len(ids):
                    raise CheckpointUnavailable(
                        "Restored submission identity map differs"
                    )
                mapped = [
                    {**e, "submission_id": ids[e["submission_id"]]}
                    for e in prefix["history"]
                ]
            history = value["history"]
            if not _same_released_value(history[: len(mapped)], mapped):
                raise CheckpointUnavailable(
                    "Sealed checkpoint history is not the durable prefix"
                )
            for old, inherited in zip(prefix["history"], mapped):
                if not _same_released_value(value["results"][inherited["submission_id"]], {
                    k: prefix["results"][old["submission_id"]][k]
                    for k in ("status", "report", "error")
                }):
                    raise CheckpointUnavailable("Sealed checkpoint result changed")
            next_agent = value["opportunity"]["next_agent"]
            if type(next_agent) is not int or [e["round"] for e in history] != [
                f"agent-{n}" for n in range(1, next_agent)
            ]:
                raise CheckpointUnavailable(
                    "Unsettled reservation or non-contiguous native rounds"
                )
            inherited_rounds = {e["round"] for e in mapped} if origin else set()
            expected_archives = {e["round"] for e in history} - inherited_rounds
            if (
                set(value["accepted_archives"]) != expected_archives
                or set(value["reservations"]) != expected_archives
            ):
                raise CheckpointUnavailable("Accepted native archive coverage differs")
            previous = 0
            for entry in history:
                round_id = entry["round"]
                if round_id in inherited_rounds:
                    continue
                accepted = value["accepted_archives"][round_id]
                archive = _read(self.bank.root, folder / (round_id + ".tar.gz"))
                accepted_at = accepted["accepted_at_epoch"]
                if (
                    accepted["submission_id"] != entry["submission_id"]
                    or accepted["archive_bytes"] != len(archive)
                    or accepted["archive_sha256"] != _sha(archive)
                    or type(accepted_at) not in (int, float)
                    or not math.isfinite(accepted_at)
                    or accepted_at <= 0
                    or accepted_at < previous
                    or value["reservations"][round_id] != accepted_at
                ):
                    raise CheckpointUnavailable(
                        "Accepted native archive or order differs"
                    )
                previous = accepted_at
            observed, last = value["observed_at_epoch"], value["last_agent_submit_at"]
            if (
                type(observed) not in (int, float)
                or not math.isfinite(observed)
                or type(last) not in (int, float)
                or not math.isfinite(last)
                or observed < last
                or last < previous
                or (previous and last != previous)
            ):
                raise CheckpointUnavailable("Native acceptance clock differs")
            cooldown = value["opportunity"]["submission_cooldown"]
            selected = self.bank._selection(task_id, history)
            incumbent = archive = None
            if selected:
                if selected in value["accepted_archives"]:
                    archive = _read(self.bank.root, folder / (selected + ".tar.gz"))
                elif prefix["incumbent"] and selected == prefix["incumbent"]["round"]:
                    archive = _read(
                        self.bank.root,
                        self.bank.bank / checkpoint_id / "incumbent.tar.gz",
                    )
                else:
                    raise CheckpointUnavailable(
                        "Selected native archive is unavailable"
                    )
                incumbent = {
                    "round": selected,
                    "archive_bytes": len(archive),
                    "archive_sha256": _sha(archive),
                }
            packet = {
                "schema_version": SCHEMA,
                "task_id": task_id,
                "source_run_id": source_run_id,
                "source_registration_id": source_registration_id,
                "captured_at_epoch": observed,
                "evaluator": value["evaluator"],
                "resources": value["resources"],
                "history": history,
                "results": value["results"],
                "opportunity": {
                    **value["opportunity"],
                    "remaining_cooldown_seconds": max(
                        0, last + (cooldown or 0) - observed
                    )
                    if last
                    else 0,
                },
                "incumbent": incumbent,
                "durable_tail": {
                    "checkpoint_id": checkpoint_id,
                    "manifest_sha256": manifest_sha256,
                    "registration_state_sha256": _sha(raw),
                    "last_agent_submit_at": last,
                    "submission_id_map": origin["submission_id_map"] if origin else {},
                },
            }
            self.bank._validate(packet)
            return self.bank._publish_point(capture_id, packet, archive)
        finally:
            os.close(descriptor)
