---
title: Judge HTTP API
---

# Judge HTTP API

The Judge server exposes a REST API for submitting code archives, polling results, and managing interactive game sessions. Start it with `sforge serve --port 8080`.

## Base URL

```
http://localhost:8080
```

All endpoints are prefixed with `/api/v1`.

## Authentication

The controller API uses a **token-based session model**. A controller registers
a session and keeps the judge token on the host. Work containers receive only a
run-scoped feedback relay URL, which exposes agent submit, agent-visible history,
and results for accepted agent submissions from the same registration.

## Endpoints

### Task Discovery

List all available tasks loaded from the `tasks/` directory.

```http
GET /api/v1/tasks
```

**Response:**

```json
[
  {"task_id": "ad_placement_optimization", "name": "Ad Placement Optimization"},
  {"task_id": "gitlet", "name": "Gitlet"},
  {"task_id": "tinykv", "name": "TinyKV"}
]
```

### Session Registration

Register a new session and receive a token for subsequent submissions.

```http
POST /api/v1/register
Content-Type: application/json

{
  "task_id": "ad_placement_optimization",
  "run_id": "run-001",
  "admin_secret": "CURRENT_CONTROLLER_SECRET"
}
```

**Response:**

```json
{
  "token": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "feedback_relay_id": "11223344556677889900aabbccddeeff",
  "evaluation_context": {
    "schema_version": "sforge_run_evaluation_context_v1",
    "task_id": "ad_placement_optimization",
    "run_id": "run-001",
    "registration_id": "00000000000000000000000000000000",
    "evaluation_conditions": null
  }
}
```

The token is a 32-character hex string. It tracks the task, run, and separate counters for agent submissions (`agent-1`, `agent-2`, ...) and auto-eval submissions (`auto-1`, `auto-2`, ...).

### Preserving a settled judge registration (opt-in)

Start `sforge serve --checkpoints --port 8080` to expose the controller endpoints below. Without `--checkpoints`, all return 404 and no checkpoint bank is created. Disable them by restarting without the flag; retain the private bank and restored run reservations for diagnosis, and use a fresh run ID for a separate experiment.

All checkpoint endpoints require the running judge's `admin_secret` in the JSON body. An agent's submission token identifies a capture source but cannot authorize capture or restore. For another controller, transfer only the authorized `logs/checkpoints/` bank into its own log directory; use that controller's independently created admin secret. Do not transfer the source admin-secret file or session tokens.

```http
POST /api/v1/checkpoint/capture
Content-Type: application/json

{"token":"SOURCE_SESSION_TOKEN","admin_secret":"CURRENT_CONTROLLER_SECRET","capture_id":"11111111111111111111111111111111"}
```

`capture_id` is an optional caller-generated 32-character hexadecimal operation ID. A retry for the same registration returns the original point, even after later submissions; it neither captures the later state nor writes another capsule. Another registration cannot reuse the ID. Omit it for a new manual capture. The response contains `checkpoint_id`, `manifest_sha256`, the task/source run, preserved opportunity counters and remaining cooldown, and the native-selected incumbent's round/size/digest. The private capsule contains all released agent history and full result reports plus the selected archive; it contains no session or admin token. Archive bytes remain opaque and are never extracted during capture/restore.

Capsule manifests remain strict JSON. Finite history and reports keep the
`sforge_judge_checkpoint_v1` representation. A report can legitimately retain a
nonfinite score or metric; capsules containing those values use
`sforge_judge_checkpoint_v2`. That representation replaces only the nonfinite
numbers in `history` and `results` with `null`, and records each original value
in the top-level `nonfinite_values` list, for example
`{"path":["history",0,"score"],"kind":"positive_infinity"}`. Kinds are
`positive_infinity`, `negative_infinity`, and `nan`; path components are exact
object keys or nonnegative array indices. This is storage encoding, not missing
feedback: the reader restores the original numbers before checking history,
results, or the native-selected incumbent. Literal strings and similarly shaped
objects in reports are ordinary data. Unknown versions, duplicate or unavailable
paths, non-null targets, paths outside released data, and unmarked nonfinite
numbers are rejected. Earlier readers reject v2 rather than treating its nulls
as scores. New readers retain finite v1 format support; the existing exact
native-source/evaluator binding still applies to both versions.

```http
POST /api/v1/checkpoint/restore
Content-Type: application/json

{"checkpoint_id":"11111111111111111111111111111111","manifest_sha256":"CAPTURED_64_CHARACTER_SHA256","run_id":"fresh-branch","admin_secret":"CURRENT_CONTROLLER_SECRET"}
```

The response issues a fresh submission token and feedback relay for `fresh-branch`. Read back from the controller with `/api/v1/history?token=...` and `/api/v1/result/{submission_id}?token=...`. Retained results have fresh lookup IDs and unchanged original reports; `logs/runs/fresh-branch/checkpoint-origin.json` records their source mapping. The selected archive is retained under its original round. Restore performs **no evaluation**. The next `/submit` uses the existing native counter, budget, cooldown and selection rules. Cooldown resumes from the captured remaining **duration** when the fresh registration is published; offline waiting and capsule materialization do not refill evaluation opportunities or spend that duration. A later source improvement is absent from an earlier capsule.

Restored `report.json` files retain the ordinary native report format used by
evaluation, including Python JSON's `Infinity`, `-Infinity`, and `NaN` spellings
when present. They do not contain the capsule's path encoding. Live reports,
selection, and public API projections keep their existing semantics; neither
capture nor restore converts a nonfinite score to zero or missing feedback.

If the restore response is lost, call `POST /api/v1/checkpoint/restore-result` with the **same** JSON body (`checkpoint_id`, `manifest_sha256`, `run_id`, and current `admin_secret`). It returns the original response shape and the already published submission token. It does not allocate another registration, replay grading, or restart cooldown. Readback also remains valid after subsequent submissions and preserves their consumed opportunities. The existing origin marker must match and the registration must still be live; a missing, mismatched, partial or lost registration returns 409. Wrong admin authority returns 403, and malformed bodies return 422. A persisted origin marker alone cannot recreate credentials after a judge process restart. This recovery does not recover a runner's phase deadline: its controller must retain the original activation-time deadline and must not initialize a new duration on retry. A new `run_id` represents a different branch, not recovery of this response.

#### Adopting a terminal durable tail after the judge stops

With the same `--checkpoints` flag, native registrations now persist reservations,
accepted archive bytes/digests and terminal reports under the private
`logs/checkpoint-registrations/<run>/<task>/` owner. There is no second logging
switch. Pull/build the pinned judge image before registering: this opt-in path
requires the configured Docker backend and a non-game task. Each registration
holds an exclusive filesystem writer lock. Counters are persisted before they
become visible; a reserved round without an accepted archive or terminal result
blocks recovery rather than disappearing. Invalid results and execution errors
still consume their original rounds. A storage failure is not a failed grade:
the result remains pending, its readback and further admission return 503, and
recovery remains blocked until the durable source is complete.
An uncertain write or closed writer also prevents a new capture from stale
memory. Replaying an already sealed capture ID only reads that historical point;
it does not certify that the point includes later accepted work.

After the source service has stopped, restart the **same compatible provider**
against its original physical log directory and make an administrator request:

```http
POST /api/v1/checkpoint/adopt-terminal
Content-Type: application/json

{"checkpoint_id":"11111111111111111111111111111111","manifest_sha256":"ORIGINAL_PREFIX_64_CHARACTER_SHA256","source_run_id":"source","task_id":"example-task","source_registration_id":"22222222222222222222222222222222","capture_id":"33333333333333333333333333333333","admin_secret":"CURRENT_CONTROLLER_SECRET"}
```

Read the original registration ID from its private `state.json`; use the exact
prior capture reference (or the original restore reference for a continuation).
The endpoint verifies that sealed prefix, any restore ID map, every subsequent
reservation/archive/result, and source/task/image/resource pins. The source
writer must be gone; copying its files to another directory cannot bypass its
lock. It returns a **new immutable checkpoint reference**, creates no token and
leaves the old point unchanged. Repeating the same request returns that point;
a conflicting capture ID is refused. Consume the returned reference through the
existing `restore` endpoint and read back history/results. The new manifest's
`durable_tail` records the prefix, durable-state digest, last acceptance time and
inherited ID map. Restore still assigns new result lookup IDs, without regrading.

Remaining cooldown is frozen at the last durable observation, conservatively
excluding unobserved time before process death and all downtime. A live writer,
missing/altered archive, pending grade, counter gap, changed prefix, copied source,
or incompatible provider returns 409 and publishes no token or point. Existing
report folders, `checkpoint-origin.json`, or a successful worker exit **alone**
are not an admission/drain record: older runs without this durable owner remain
unqualified. Do not edit counters or remove invalid rounds to make them qualify.

The bounded submission queue records acceptance before its evaluator starts.
Graceful shutdown drains accepted evaluations before releasing the registration
writer. A failed acceptance write retains its spent reservation but releases the
queue slot; it cannot be treated as an unspent submission or a complete tail.
Finite journals retain the v1 representation. Non-finite result numbers use the
existing lossless checkpoint codec with `sforge_judge_registration_tail_v2`;
only history/results support that encoding, never provenance or budget clocks.

This retains each accepted archive once, in addition to the normal grading
artifacts; disk use grows with submissions. No automatic pruning runs. Retain the
original store until an administrator has verified the new point and receiver;
then archive/remove it explicitly while its writer is stopped. Cleanup is not
permission to reset the old run ID. Without `--checkpoints`, these writes,
locks and recovery routes are absent and ordinary execution is unchanged.

This version supports settled non-game registrations on the configured Docker backend, with no automatic evaluations. The native Python source, full task specification, actual Docker judge image ID, selection policy/direction and effective CPU/memory limits are bound. Missing/moved bytes, mismatched evaluator facts, queued/running results, consumed rounds without released history, conflicting result/history records and unsupported profiles return 409. Wrong admin authority returns 403; malformed request bodies return 422. Existing run identities, replayed restores and interrupted destination reservations are rejected rather than reset. An ordinary retry can recover an exception rolled back before completion; a process crash leaving a reservation fails closed and needs controller inspection. An accepted restore reserves the entire destination run ID, including against later cold registration while this feature is enabled.

The bank is trusted private controller storage. SHA-256 detects disagreement with the controller's captured reference; it does not authenticate a malicious administrator, certify arbitrary report contents as credential-free, or make a worker checkpoint complete. Restore does not restore worker code beyond the incumbent archive, model sessions, research state, external-effect authority, or the runner's remaining total time budget. A continuation runner must preserve those separately before treating branches as comparable. Do not restart a restored run as a cold registration after disabling checkpoints.

Two native consistency fixes also apply with the feature off: backend preparation failure publishes no token/resource row, and grading failures now release history with `status="error"`, matching `/result`, rather than labeling the history completed. Successful cold registration defaults, submission allocation and ranking are unchanged.

### Submit Archive

Submit a code archive for evaluation. The server resolves the task ID and run ID from the token and assigns an incrementing round ID.

```http
POST /api/v1/submit
Content-Type: multipart/form-data

token: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
archive: @solution.tar.gz
kind: agent
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `token` | string | yes | Session token from `/register` |
| `archive` | file | yes | `.tar.gz` archive of the solution |
| `kind` | string | no | `"agent"` (default) or `"auto"`. Determines which round counter is incremented. |

**Response:**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "round_id": "agent-1",
  "status": "queued"
}
```

Submissions are processed asynchronously --- the server returns immediately with `status: "queued"`. Poll the result endpoint to check progress.

### Optional source receipts and JSON client output

`sforge-submit --json` emits one native JSON response on stdout; progress and
errors use stderr. Add `--no-wait` to return the accepted submission without
reading grading, or use `--result ID --no-wait --json` for a single read of that
same submission. A normal `--result ID --json` waits for completion. JSON reads
preserve the full public report, including details, zero/negative scores,
missing scores and `valid=false`. Pending responses have no report; grading
errors retain their error and exit nonzero. JSON mode neither reads nor updates
the human display cache. Omit `--json` for the existing text/cache behavior.
`--list --json` returns only the current relay's visible history/availability
snapshot.

The client requests `include_receipt=true` as a submit form field or result
query parameter through the opaque, run-scoped feedback relay. The work
container receives neither the judge URL nor a judge token. Direct host API
receipt queries still require a session token. Both routes are limited to the
registration's visible agent history; other runs and host-only automatic
submissions are not exposed. Without this option, the API response fields
remain unchanged. Both opt-in responses include:

```json
{
  "receipt": {
    "schema_version": "sforge_submission_receipt_v1",
    "task_id": "example",
    "round_id": "agent-1",
    "archive_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "archive_bytes": 1234
  }
}
```

The digest is computed by the judge from the exact accepted archive bytes,
before asynchronous grading. It stays associated with the outer `submission_id`
after workspace edits/deletion or a lost-response recovery; reads never submit
again or replenish budget/cooldown. Checkpoint capture/restore preserves each
receipt while remapping lookup IDs. Older retained results have explicit null
archive fields. A server without receipt support cannot provide `--json`
source receipts; recover with the existing text client instead of resubmitting.
The example digest above is illustrative. An archive digest is not a source-tree
digest, task-input revision or evaluator identity. Those service facts have a
separate envelope below; matching receipts alone do not prove a comparable
experiment, valid score or best candidate.

#### Service-owned evaluation conditions

`GET {SFORGE_FEEDBACK_URL}/context` and `sforge-submit --context --json` read the
current registration's immutable `evaluation_context` without submitting,
spending a round, or reading a grading report. Registration also returns this
object. The example registration above shows unknown conditions. When the
service can establish all supported facts, `evaluation_conditions` contains:

| Field | Service-observed source |
|---|---|
| `schema_version` | `sforge_evaluation_conditions_v1` |
| `task_sha256` | Exact task JSON bytes used by the task loader |
| `benchmark_sha256` | Exact `BENCHMARK.yaml` bytes used by the loader |
| `task_spec_sha256` | Effective `TaskSpec`, with the existing checkpoint canonical encoding |
| `native_source_sha256` | Running service's native Python source, captured once at startup using the checkpoint digest algorithm |
| `judge_image_id` | Actual Docker image content ID (`sha256:` plus 64 hex characters), resolved at registration |
| `execution_config_sha256` | Effective judge CPU/memory limits and injected judge environment, including the default judge URL |
| `score_direction` | Loaded `maximize` or `minimize` metric direction |
| `selection` | Loaded native selection policy |

SHA-256 fields are lowercase, 64-character hexadecimal digests. The execution
config digest hashes compact, sorted-key UTF-8 JSON (non-ASCII characters are
preserved) with keys `judge_cpu_limit`, `judge_mem_limit`, and `environment`;
limits resolve registration override, service config, then task defaults.
Environment values and hidden evaluator/data contents are never returned.
The task and benchmark digests come from the same bytes that were parsed,
not a later filesystem read. The registration freezes the effective task,
grading config and backend; accepted submissions use that snapshot and the
resolved immutable image ID even after its tag changes. Source files belong to
the deployed service; deploy changed source in a new service instance.

Every opt-in submit/result receipt additionally includes `evaluation_context`,
copied from acceptance rather than reconstructed at read time. A consumer can
qualify the relay once against its intended run, task, task bytes and direction,
save the service context, then check each result with **one** receipt-enabled
GET. Use the service's actual source/image/config identities; a worker's local
checkout label does not identify a separately running grader. Missing or changed
conditions leave machine attribution unknown; ordinary native feedback remains
available. Older servers may return 404 for `/context`; older results have null
`receipt.evaluation_context`. Never fill either case from a newer worker.

Currently, game tasks, non-Docker backends, unavailable source bytes, missing
loader provenance, and images not already available at registration produce
`evaluation_conditions: null`. Qualification does not build/pull an image, and
later grading does not retroactively label the registration or earlier scores.
Use a fresh registration/run after preparing an image. Image identity covers
data baked into that image; mutable data or services fetched by an evaluation
command are outside this contract. These facts do not certify hermetic scoring
or equality of all experimental budgets and hardware.

For known conditions, repeating the same live run/task with identical conditions
and registration limits returns the original token, relay, counters and cooldown.
Concurrent candidates therefore receive distinct rounds and native directories.
A conflicting registration returns **409** and requires a fresh `run_id`; it
cannot create another `agent-1` writer. Legacy registrations with unknown
conditions retain their previous behavior and provide no attribution guarantee.
Checkpoint capsules and terminal tails preserve each result's original context,
including its original run/registration identity after lookup-ID remapping.
Historical missing contexts stay missing; restore never relabels old scores as
the destination evaluator. Existing strict checkpoint source/evaluator checks
remain in force.

### Get Result

Poll for the result of a submission.

```http
GET /api/v1/result/{submission_id}?token={host_session_token}
```

**Response (queued/running):**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "running",
  "report": null,
  "error": null
}
```

**Response (completed):**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "completed",
  "report": {
    "total_tests": 50,
    "passed": 45,
    "failed": 5,
    "errors": 0,
    "pass_rate": 0.9,
    "score": null,
    "valid": true,
    "summary": "45/50 tests passed. Failed: test_forward, test_backward, ...",
    "test_details": [...]
  },
  "error": null
}
```

**Response (error):**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "error",
  "report": null,
  "error": "judge_execution_failed:execute_evaluation"
}
```

**Status values:**

| Status | Description |
|--------|-------------|
| `queued` | Submission received, waiting to be processed |
| `running` | Judge container is running tests |
| `completed` | Grading finished, report is available |
| `error` | Grading failed (container crash, timeout, etc.) |

### Terminal resource observations

After evaluation, Docker can attach an optional `report.resource_diagnostics`:

```json
{"memory_oom_observed": true, "memory_limit_bytes": 134217728, "container_running": true}
```

This is a bounded, best-effort observation before cleanup, covering the container's
lifetime. An OOM event may have killed a child while the evaluator recovered and
produced a valid score. It does **not** identify the failing process, prove the
cause of a timeout, or change score, validity, timeout, submission count or cooldown.
`memory_limit_bytes: 0` means Docker reports no hard memory limit; `null` means
unknown. A missing diagnostic means no OOM evidence was returned, not proof that
memory was sufficient. Other backends currently omit it. No raw container metadata,
environment, task-private output or host paths are exposed.

`sforge-submit --result ID --details` displays this observation; JSON results retain
the structured fields, including through checkpoint restoration. An evaluation
exception retains its original error status and appends the same safe observation
to the error text when available. No extra evaluation, retry, automatic early stop,
pending-status polling or memory-limit adjustment is performed.

### Agent Feedback Relay

The runner sets `SFORGE_FEEDBACK_URL` to
`/api/v1/feedback/{feedback_relay_id}` inside the work container. The generated
`sforge-submit` client uses these credential-free, registration-scoped routes:

```http
POST {SFORGE_FEEDBACK_URL}/submit
GET  {SFORGE_FEEDBACK_URL}/context
GET  {SFORGE_FEEDBACK_URL}/history
GET  {SFORGE_FEEDBACK_URL}/result/{submission_id}
```

The relay always submits as `agent`, omits auto-eval history, and rejects result
IDs from another run or from host-side auto evaluation. The raw controller
result endpoint requires the host session token.

### Submitting snapshots and reading results

The judge evaluates the archive uploaded at submission time. Once accepted,
later edits to the working directory do not change that submission. To return
after acceptance and continue independent work while grading runs:

```bash
sforge-submit --no-wait
# Continue working; retain the printed submission ID and its candidate identity.
sforge-submit --result <submission_id> --no-wait --details
```

For a new submission, `--no-wait` exits successfully on acceptance, without
reading or caching a score. With `--result ID`, it checks the result once after
verifying relay-visible history: `queued` or `running` returns success with an
explicit pending status and no score or cache change; a terminal result uses the
same feedback and error handling as blocking retrieval. Pending means the read
succeeded, not that the candidate passed. Continue useful independent work
before checking the same ID again.

Without `--no-wait`, submission and result retrieval still wait for feedback.
`--no-wait` cannot be combined with `--list` or with `--details` unless a
`--result ID` is supplied. Keep recoverable copies of candidates you may need to
restore: result retrieval reads feedback, not source files. Periodic automatic
evaluations capture later workspace states independently.

The generated `sforge-submit` helper can read an accepted submission without
creating a new evaluation:

```bash
sforge-submit --list
sforge-submit --result <submission_id> --details
```

Use the ID printed on acceptance, or find it in `--list` if the acceptance reply
was lost. Recovery reads only IDs in the current token's visible history and
preserves the original round, score and public details. It does not archive the
current workspace, consume another submission, or change the cooldown clock.
`--details` alone still submits new code; use `--result ID --details` to read
existing details.

Transport/HTTP errors, terminal grading errors and a completed response without
a report fail with a nonzero exit status instead of creating a zero-score cache
entry. A legitimate zero score remains zero; a missing score remains missing.
If acceptance is unknown, inspect history before submitting again. This is
client-side result retrieval, not idempotent submission retry or durable server
restart recovery; it requires the server to retain the session and result.

### Run History

Retrieve the full submission history for a run session, including the best score selection.

**By token:**

```http
GET /api/v1/history?token=<token>
```

**By run ID:**

```http
GET /api/v1/runs/{run_id}/history?task_id=<task_id>
```

**Response:**

```json
{
  "run_id": "run-001",
  "best_pass_rate": 0.95,
  "best_score": null,
  "best_round": "agent-3",
  "agent_submissions": 4,
  "auto_submissions": 2,
  "entries": [
    {
      "type": "submission",
      "status": "completed",
      "submission_id": "abc123",
      "task_id": "ad_placement_optimization",
      "round": "agent-1",
      "pass_rate": 0.8,
      "score": null,
      "passed": 40,
      "failed": 10,
      "total_tests": 50,
      "valid": true,
      "summary": "40/50 tests passed. Failed: ..."
    }
  ]
}
```

The `best_*` fields are computed using the task's configured selection policy (see [Benchmark & Task Integration](/en/tasks/integration-guide#selection-strategies)).

### Game Endpoints

For tasks with `game_mode: true`, the Judge server manages interactive game sessions in dedicated containers.

#### Start New Game

```http
POST /api/v1/game/{run_id}/{task_id}/new
Content-Type: application/json
{}
```

**Response:**

```json
{
  "session_id": "abc123def456",
  "observation": "You are standing in a dark room...",
  "score": 0,
  "peak_score": 0,
  "max_score": 100,
  "done": false,
  "moves": 0
}
```

#### Take Action

```http
POST /api/v1/game/{run_id}/{task_id}/{session_id}/step
Content-Type: application/json

{"action": "go north"}
```

**Response:**

```json
{
  "session_id": "abc123def456",
  "observation": "You enter a dimly lit hallway...",
  "score": 10,
  "peak_score": 10,
  "max_score": 100,
  "done": false,
  "moves": 1
}
```

#### Get Status

```http
GET /api/v1/game/{run_id}/{task_id}/{session_id}/status
```

**Response:**

```json
{
  "session_id": "abc123def456",
  "score": 10,
  "peak_score": 10,
  "max_score": 100,
  "done": false,
  "moves": 1
}
```

#### Close Session

```http
POST /api/v1/game/{run_id}/{task_id}/{session_id}/close
```

**Response:**

```json
{
  "session_id": "abc123def456",
  "final_score": 25,
  "peak_score": 30,
  "max_score": 100,
  "moves": 15
}
```

#### Close All Sessions

Close all active game sessions for a given run and task.

```http
POST /api/v1/game/{run_id}/{task_id}/close-all
```

**Response:**

```json
{"closed": 3}
```

Game sessions have a 10-minute idle timeout. Sessions are automatically archived when the game ends (`done: true`) or when they are explicitly closed. A maximum of 200 concurrent game sessions is enforced.

## curl Examples

### Register, submit, and poll

```bash
# 1. Register a session
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{"task_id": "ad_placement_optimization", "run_id": "run-001"}' \
  | jq -r '.token')

echo "Token: $TOKEN"

# 2. Submit an archive
SUBMISSION_ID=$(curl -s -X POST http://localhost:8080/api/v1/submit \
  -F "token=$TOKEN" \
  -F "archive=@solution.tar.gz" \
  -F "kind=agent" \
  | jq -r '.submission_id')

echo "Submission: $SUBMISSION_ID"

# 3. Poll for result (repeat until status is "completed" or "error")
curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq .

# 4. Check run history
curl -s "http://localhost:8080/api/v1/history?token=$TOKEN" | jq .
```

### Poll loop

```bash
while true; do
  STATUS=$(curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq -r '.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "error" ]; then
    curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq .
    break
  fi
  sleep 5
done
```
