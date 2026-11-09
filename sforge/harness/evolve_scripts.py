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

"""Script generators for agent execution.

Produces bash scripts that run inside the work container:
- sforge-submit: submits current code to judge server via HTTP, prints results,
  updates shared state file (display-only cache)

Agent-specific scripts (stop hooks, settings files) live in the corresponding
agent modules under ``sforge.harness.agent``.

State file `/tmp/sforge_state.json` (display-only, not used for final scoring):
  {
    "best_pass_rate":  float,
    "best_score":      float|null,
    "best_round":      str,   # "agent-N" or "auto-N"
    "submissions": [
      {"kind": "agent"|"auto", "round": str, "at": int, "pass_rate": float, "score": float|null}
    ]
  }
"""

from __future__ import annotations


def generate_submit_script() -> str:
    """Generate the sforge-submit bash script.

    Submits through a run-scoped feedback relay and polls for results. The
    host retains judge credentials; the relay manages task_id, run_id, and
    round counters without exposing broader evaluator authority to the agent.

    Flags:
      --details, -d    submit and show detailed per-test results
      --list, -l       show submission history and current limits (no new submission)
      --context       show immutable run evaluation conditions (no new submission)
      --result ID     poll a submission from this run's history (no new submission)
      --no-wait       return after acceptance, or read --result once without polling
      --json          emit the native response and optional source receipt as JSON
      --retain-archive DIR  save the exact uploaded archive in an external directory
    """
    return _generate_submit_script()


def _generate_submit_script() -> str:
    """Generate sforge-submit: token-based POST + poll until result is ready."""
    return r"""#!/bin/bash
set -e

LIST_MODE=0
CONTEXT_MODE=0
RESULT_MODE=0
DETAILS_MODE=0
NO_WAIT=0
JSON_MODE=0
SUBMISSION_ID=""
RETAIN_DIR=""
while [ "$#" -gt 0 ]; do
    arg="$1"
    shift
    case "$arg" in
        --list|-l) LIST_MODE=1 ;;
        --context) CONTEXT_MODE=1 ;;
        --details|-d) DETAILS_MODE=1 ;;
        --no-wait) NO_WAIT=1 ;;
        --json) JSON_MODE=1 ;;
        --retain-archive)
            if [ -n "$RETAIN_DIR" ]; then
                echo "ERROR: --retain-archive can be specified only once" >&2
                exit 1
            fi
            case "${1:-}" in
                ""|-*)
                    echo "ERROR: --retain-archive requires an existing directory" >&2
                    exit 1
                    ;;
            esac
            RETAIN_DIR="$1"
            shift
            ;;
        --result)
            if [ "$#" -eq 0 ] || [ -z "$1" ]; then
                echo "ERROR: --result requires a submission ID" >&2
                exit 1
            fi
            SUBMISSION_ID="$1"
            RESULT_MODE=1
            shift
            ;;
        -h|--help)
            echo "Usage: sforge-submit [OPTIONS]"
            echo ""
            echo "Submit current code to the judge server for evaluation."
            echo "Results include score, pass rate, and a summary of findings."
            echo ""
            echo "Options:"
            echo "  --list, -l      Show previous scores, remaining submissions and current cooldown"
            echo "                   Read-only; does not submit or reserve a round"
            echo "  --context      Read service-owned task/evaluator conditions for this registration"
            echo "  --details, -d   Submit and show detailed per-test results (triggers a new submission)"
            echo "  --result ID     Read/poll an existing submission from this run without submitting again"
            echo "                  Combine with --details for full test details"
            echo "  --no-wait       Submit a snapshot and return its ID without waiting for grading"
            echo "                  With --result ID: query once; return pending or the final feedback"
            echo "  --json          Emit one native JSON response; progress and errors use stderr"
            echo "  --retain-archive DIR"
            echo "                  Save the exact attempted upload in an existing directory outside submitted paths"
            echo "                  The saved archive alone does not confirm acceptance"
            echo "  -h, --help      Show this help message"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $arg" >&2
            echo "Run 'sforge-submit --help' for usage." >&2
            exit 1
            ;;
    esac
done

FEEDBACK_URL="${SFORGE_FEEDBACK_URL}"
PATCH_DIR="${SFORGE_PATCH_DIR:-$(pwd)}"
STATE_FILE="/tmp/sforge_state.json"

if [ -z "$FEEDBACK_URL" ]; then
    echo "ERROR: SFORGE_FEEDBACK_URL not set" >&2
    exit 1
fi

if [ "$LIST_MODE" -eq 1 ] && [ -n "$SUBMISSION_ID" ]; then
    echo "ERROR: Choose only one of --list or --result" >&2
    exit 1
fi
if [ "$CONTEXT_MODE" -eq 1 ] && { [ "$LIST_MODE" -eq 1 ] || [ "$RESULT_MODE" -eq 1 ] || [ "$DETAILS_MODE" -eq 1 ] || [ "$NO_WAIT" -eq 1 ]; }; then
    echo "ERROR: --context is a read-only operation; combine only with --json" >&2
    exit 1
fi
if [ "$NO_WAIT" -eq 1 ] && { [ "$LIST_MODE" -eq 1 ] || { [ "$DETAILS_MODE" -eq 1 ] && [ "$RESULT_MODE" -eq 0 ]; }; }; then
    echo "ERROR: --no-wait cannot be combined with --list or with --details without --result ID" >&2
    exit 1
fi
if [ -n "$RETAIN_DIR" ]; then
    if [ "$LIST_MODE" -eq 1 ] || [ "$RESULT_MODE" -eq 1 ] || [ "$CONTEXT_MODE" -eq 1 ]; then
        echo "ERROR: --retain-archive requires a new submission" >&2
        exit 1
    fi
    if ! RETAIN_DIR=$(CDPATH= cd -P -- "$RETAIN_DIR" && pwd -P); then
        echo "ERROR: --retain-archive directory is not accessible" >&2
        exit 1
    fi
fi

exec 3>&1
if [ "$JSON_MODE" -eq 1 ]; then
    exec 1>&2
fi

umask 077
REQUEST_TMP=$(mktemp -d)
trap 'rm -rf "$REQUEST_TMP"' EXIT

get_json() {
    local code
    if ! code=$(curl -sS -o "$REQUEST_TMP/response.json" -w '%{http_code}' -m 30 "$1"); then
        echo "ERROR: Judge request failed; no evaluation was submitted by this read" >&2
        return 1
    fi
    if [ "$code" != "200" ]; then
        echo "ERROR: Judge request returned HTTP $code" >&2
        cat "$REQUEST_TMP/response.json" >&2
        return 1
    fi
    if ! jq -e 'type == "object"' "$REQUEST_TMP/response.json" >/dev/null; then
        echo "ERROR: Malformed judge response" >&2
        return 1
    fi
    cat "$REQUEST_TMP/response.json"
}

emit_receipt() {
    # Verify the native response matches the accepted/visible submission before
    # emitting it. Null archive fields explicitly mean legacy provenance unknown.
    if ! echo "$1" | jq -e --arg sid "$SUBMISSION_ID" --arg round "$ROUND_ID" '
        .submission_id == $sid and .receipt.schema_version == "sforge_submission_receipt_v1"
        and .receipt.round_id == $round and (.receipt.task_id | type == "string")
        and ((.receipt.archive_sha256 == null and .receipt.archive_bytes == null)
          or ((.receipt.archive_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
            and (.receipt.archive_bytes | type == "number" and . >= 0 and floor == .)))
        ' >/dev/null; then
        echo "ERROR: Native source receipt unavailable or mismatched; do not resubmit" >&2
        return 1
    fi
    echo "$1" | jq -c . >&3
}

if [ "$CONTEXT_MODE" -eq 1 ]; then
    CONTEXT=$(get_json "$FEEDBACK_URL/context")
    echo "$CONTEXT" | jq -c . >&3
    exit 0
fi

if [ "$LIST_MODE" -eq 1 ]; then
    HIST=$(get_json "$FEEDBACK_URL/history")
    if ! echo "$HIST" | jq -e '.entries | type == "array"' >/dev/null; then
        echo "ERROR: Malformed submission history" >&2
        exit 1
    fi
    if [ "$JSON_MODE" -eq 1 ]; then
        echo "$HIST" | jq -c . >&3
        exit 0
    fi
    BEST_RATE=$(echo "$HIST" | jq -r '.best_pass_rate // 0')
    BEST_SCORE=$(echo "$HIST" | jq -r '.best_score // "N/A"')
    COUNT=$(echo "$HIST" | jq -r '.entries | length')
    echo ""
    echo "========================================"
    echo "  Submission History"
    echo "  Total submissions: $COUNT"
    echo "  Best pass rate: $(jq -n --argjson r "$BEST_RATE" '$r * 100 | . * 10 | floor / 10')%"
    if [ "$BEST_SCORE" != "N/A" ] && [ "$BEST_SCORE" != "null" ]; then
        echo "  Best score: $BEST_SCORE"
    fi
    if echo "$HIST" | jq -e '.agent_submission_availability != null' >/dev/null; then
        echo "$HIST" | jq -r '.agent_submission_availability |
            "  Agent submission limits (server snapshot):",
            ("  Remaining submissions: " + (if .remaining_submissions == null then "unlimited" else (.remaining_submissions | tostring) end)),
            (if .remaining_submissions == 0 then "  Submission budget exhausted."
             elif .cooldown_remaining_seconds > 0 then "  Cooldown: wait " + (.cooldown_remaining_seconds | tostring) + "s from this read."
             else "  Cooldown: ready at this read." end),
            "  Read-only; does not reserve or guarantee submission admission."'
    else
        echo "  Current submission limits unavailable from this server."
    fi
    echo "========================================"
    echo ""
    if [ "$COUNT" -gt 0 ]; then
        printf "  %-14s %-14s %-10s %-8s %-12s %-10s %s\n" "SUBMISSION_ID" "ROUND" "STATUS" "VALID" "PASS_RATE" "SCORE" "SUMMARY"
        printf "  %-14s %-14s %-10s %-8s %-12s %-10s %s\n" "-------------" "-----" "------" "-----" "---------" "-----" "-------"
        echo "$HIST" | jq -r '.entries[] | select(.type == "submission") |
            "  " +
            ((.submission_id // "-") | . + " " * ([14 - length, 0] | max)) + " " +
            ((.round // "-") | . + " " * ([14 - length, 0] | max)) + " " +
            ((.status // "-") | . + " " * ([10 - length, 0] | max)) + " " +
            (if .valid == false then "no" elif .valid == true then "yes" else "-" end | . + " " * ([8 - length, 0] | max)) + " " +
            (if .pass_rate != null then (.pass_rate * 100 * 10 | floor / 10 | tostring + "%") else "-" end | . + " " * ([12 - length, 0] | max)) + " " +
            (if .score != null then (.score | tostring) else "-" end | . + " " * ([10 - length, 0] | max)) + " " +
            ((.summary // "-") | if length > 40 then .[:37] + "..." else . end)'
    fi
    echo ""
    exit 0
fi

if [ -n "$SUBMISSION_ID" ]; then
    # Respect relay-visible history before reading a guessed ID; don't expose
    # another run's result or reveal host-only automatic evaluations.
    HIST=$(get_json "$FEEDBACK_URL/history")
    ROUND_ID=$(echo "$HIST" | jq -r --arg sid "$SUBMISSION_ID" '.entries[]? | select(.type == "submission" and .submission_id == $sid) | .round // empty')
    if [ -z "$ROUND_ID" ]; then
        echo "ERROR: Submission ID is not in this relay's visible history" >&2
        exit 1
    fi
    echo "  Reading existing submission: $SUBMISSION_ID ($ROUND_ID)"
else
# ── Archive ──

cd "$PATCH_DIR"
ARCHIVE_FILE="$REQUEST_TMP/submission.tar.gz"
TAR_PATHS="${SFORGE_SUBMIT_PATHS:-.}"
if [ -n "${SFORGE_SUBMIT_PATHS:-}" ]; then
    EXISTING_PATHS=""
    for p in $SFORGE_SUBMIT_PATHS; do
        [ -e "$p" ] && EXISTING_PATHS="$EXISTING_PATHS $p"
    done
    TAR_PATHS="${EXISTING_PATHS# }"
fi
if [ -n "$RETAIN_DIR" ]; then
    PATCH_ROOT=$(pwd -P)
    case "$RETAIN_DIR/" in
        "${PATCH_ROOT%/}/"*)
            echo "ERROR: --retain-archive directory must be outside the patch directory" >&2
            exit 1
            ;;
    esac
    for p in $TAR_PATHS; do
        [ -d "$p" ] || continue
        SOURCE_ROOT=$(CDPATH= cd -P -- "$p" && pwd -P)
        case "$RETAIN_DIR/" in
            "${SOURCE_ROOT%/}/"*)
                echo "ERROR: --retain-archive directory overlaps a submitted directory" >&2
                exit 1
                ;;
        esac
    done
fi
if [ -z "$TAR_PATHS" ]; then
    tar czf "$ARCHIVE_FILE" --files-from /dev/null
else
    tar czf "$ARCHIVE_FILE" --exclude='.git' ${SFORGE_SUBMIT_EXCLUDE_FLAGS:-} $TAR_PATHS
fi
ARCHIVE_SIZE=$(wc -c < "$ARCHIVE_FILE")
if [ -n "$RETAIN_DIR" ]; then
    SAVED_DIR=$(mktemp -d "$RETAIN_DIR/submission.XXXXXXXX")
    SAVED_ARCHIVE="$SAVED_DIR/submission.tar.gz"
    if ! cp "$ARCHIVE_FILE" "$SAVED_ARCHIVE.tmp" || ! mv "$SAVED_ARCHIVE.tmp" "$SAVED_ARCHIVE"; then
        rm -f "$SAVED_ARCHIVE.tmp"
        rmdir "$SAVED_DIR"
        echo "ERROR: Could not retain the archive; no evaluation was submitted" >&2
        exit 1
    fi
    printf '  Retained attempted upload: %s\n' "$SAVED_ARCHIVE" >&2
fi

echo ""
echo "========================================"
echo "  Submitting for evaluation"
echo "  Archive size: $ARCHIVE_SIZE bytes"
echo "  Uploading snapshot..."
echo "========================================"
echo ""

# ── Submit + poll ──

RECEIPT_ARGS=()
if [ "$JSON_MODE" -eq 1 ]; then
    RECEIPT_ARGS=(-F "include_receipt=true")
fi
if ! HTTP_CODE=$(curl -sS -o "$REQUEST_TMP/submit.json" -w '%{http_code}' -m 120 -X POST "$FEEDBACK_URL/submit" \
    "${RECEIPT_ARGS[@]}" \
    -F "archive=@$ARCHIVE_FILE"); then
    echo "ERROR: Submission response lost; acceptance is unknown. Do not automatically resubmit." >&2
    echo "Use sforge-submit --list, then --result ID to recover an accepted result." >&2
    exit 1
fi
SUBMIT_RESP=$(cat "$REQUEST_TMP/submit.json")

if [ "$HTTP_CODE" = "429" ]; then
    DETAIL=$(echo "$SUBMIT_RESP" | jq -r '.detail // empty')
    if echo "$DETAIL" | grep -qi "budget\|exhausted"; then
        echo "SUBMISSION LIMIT REACHED: $DETAIL" >&2
        echo "$DETAIL"
    else
        echo "COOLDOWN: $DETAIL" >&2
        echo "$DETAIL"
    fi
    exit 1
fi

if [ "$HTTP_CODE" != "200" ]; then
    echo "ERROR: Failed to submit to judge server (HTTP $HTTP_CODE)" >&2
    echo "$SUBMIT_RESP" >&2
    exit 1
fi
SUBMISSION_ID=$(echo "$SUBMIT_RESP" | jq -r '.submission_id // empty')
ROUND_ID=$(echo "$SUBMIT_RESP" | jq -r '.round_id // empty')
REMAINING=$(echo "$SUBMIT_RESP" | jq -r '.remaining_submissions // empty')
if [ -z "$SUBMISSION_ID" ]; then
    echo "ERROR: Failed to submit to judge server (HTTP $HTTP_CODE)" >&2
    echo "$SUBMIT_RESP" >&2
    exit 1
fi

echo "  Submission ID: $SUBMISSION_ID"
echo "  If interrupted: sforge-submit --result $SUBMISSION_ID"

if [ -n "$ROUND_ID" ]; then
    if [ -n "$REMAINING" ] && [ "$REMAINING" != "null" ]; then
        echo "  Round: $ROUND_ID  (remaining submissions: $REMAINING)"
    else
        echo "  Round: $ROUND_ID"
    fi
    echo ""
fi
fi

echo "  Evaluation uses the submitted archive snapshot; later workspace edits do not change it."
if [ "$NO_WAIT" -eq 1 ] && [ "$RESULT_MODE" -eq 0 ]; then
    if [ "$JSON_MODE" -eq 1 ]; then
        emit_receipt "$SUBMIT_RESP"
        exit 0
    fi
    echo "  Accepted for evaluation; no grading result has been read."
    echo "  Continue independent work, then check feedback once: sforge-submit --result $SUBMISSION_ID --no-wait --details"
    exit 0
fi

if [ "$NO_WAIT" -eq 0 ]; then
    echo "  Waiting for test results..."
fi
for _ in $(seq 1 720); do
    RESULT_URL="$FEEDBACK_URL/result/$SUBMISSION_ID"
    if [ "$JSON_MODE" -eq 1 ]; then
        RESULT_URL="$RESULT_URL?include_receipt=true"
    fi
    RESULT=$(get_json "$RESULT_URL")
    STATUS=$(echo "$RESULT" | jq -r '.status // empty')
    if [ "$STATUS" = "completed" ] || [ "$STATUS" = "error" ]; then
        break
    fi
    if [ "$STATUS" != "queued" ] && [ "$STATUS" != "running" ]; then
        echo "ERROR: Missing or unknown evaluation status; recover with --result $SUBMISSION_ID" >&2
        exit 1
    fi
    if [ "$NO_WAIT" -eq 1 ]; then
        if [ "$JSON_MODE" -eq 1 ]; then
            emit_receipt "$RESULT"
            exit 0
        fi
        echo "  Pending: $SUBMISSION_ID ($STATUS); no score available yet."
        echo "  Continue independent work; check this same ID later with --result $SUBMISSION_ID --no-wait --details."
        exit 0
    fi
    sleep 10
done

STATUS=$(echo "$RESULT" | jq -r '.status // empty')
if [ "$STATUS" != "completed" ] && [ "$STATUS" != "error" ]; then
    echo "ERROR: Evaluation timed out or judge unreachable" >&2
    exit 1
fi

# ── Parse + update display cache + print ──

TS=$(date +%s)
ERROR_MSG=$(echo "$RESULT" | jq -r '.error // empty')

if [ "$STATUS" = "error" ]; then
    if [ "$JSON_MODE" -eq 1 ]; then emit_receipt "$RESULT"; fi
    echo "ERROR: $SUBMISSION_ID evaluation failed: ${ERROR_MSG:-no diagnostic returned}" >&2
    exit 1
fi
if ! echo "$RESULT" | jq -e '.report | type == "object"' >/dev/null; then
    if [ "$JSON_MODE" -eq 1 ]; then emit_receipt "$RESULT"; fi
    echo "ERROR: $SUBMISSION_ID completed without a report; no score available" >&2
    exit 1
fi

REPORT=$(echo "$RESULT" | jq -r '.report')
if [ "$JSON_MODE" -eq 1 ]; then
    # A machine read does not depend on, or update, the human display cache.
    emit_receipt "$RESULT"
    exit 0
fi
PASSED=$(echo "$REPORT" | jq -r '.passed // 0')
TOTAL=$(echo "$REPORT" | jq -r '.total_tests // 0')
FAILED=$(echo "$REPORT" | jq -r '.failed // 0')
CURRENT_RATE=$(echo "$REPORT" | jq -r '.pass_rate // 0')
CURRENT_SCORE=$(echo "$REPORT" | jq -r '.score // null')
VALID=$(echo "$REPORT" | jq -r 'if .valid == false then false else true end')
SUMMARY=$(echo "$REPORT" | jq -r '.summary // empty')

# Update local state file (display-only cache — not used for final scoring)
if [ ! -f "$STATE_FILE" ]; then
    echo '{"best_pass_rate": 0, "best_score": null, "best_round": "", "submissions": []}' > "$STATE_FILE"
fi
TMP=$(mktemp)
jq --arg round "${ROUND_ID:-unknown}" \
   --arg sid "$SUBMISSION_ID" \
   --argjson ts "$TS" \
   --argjson rate "$CURRENT_RATE" \
   --argjson score "$CURRENT_SCORE" \
   --argjson valid "$VALID" \
   '
   .submissions = [.submissions[]? | select(.submission_id != $sid)]
   | .submissions += [{kind: "agent", submission_id: $sid, round: $round, at: $ts, pass_rate: $rate, score: $score, valid: $valid}]
   | if $valid and $rate > (.best_pass_rate // 0) then
       .best_pass_rate = $rate | .best_round = $round | .best_score = $score
     else . end
   ' "$STATE_FILE" > "$TMP" && mv "$TMP" "$STATE_FILE"

echo "========================================"
echo "  ${ROUND_ID:-submission} Results"
echo "========================================"
if [ "$VALID" = "false" ]; then
    echo "  Valid:       no"
fi
if [ "$CURRENT_SCORE" != "null" ]; then
    echo "  Score:       $CURRENT_SCORE"
fi
if [ "$TOTAL" -gt 0 ] 2>/dev/null; then
    PASS_PCT=$(jq -n --argjson r "$CURRENT_RATE" '$r * 100 | . * 10 | floor / 10')
    echo "  Pass rate:   ${PASS_PCT}%"
    echo "  Passed:      $PASSED/$TOTAL"
fi
if [ -n "$SUMMARY" ]; then
    echo ""
    echo "  Summary:"
    echo "    $SUMMARY"
fi
# Show metrics if present
METRICS=$(echo "$REPORT" | jq -r '.metrics // empty')
if [ -n "$METRICS" ] && [ "$METRICS" != "{}" ] && [ "$METRICS" != "null" ]; then
    echo ""
    echo "  Metrics:"
    echo "$REPORT" | jq -r '.metrics | to_entries[] | "    \(.key): \(.value)"'
fi
echo ""
# Show failed items (from details if available, else from test_details)
DETAIL_FAILURES=$(echo "$REPORT" | jq -r '[.details[]? | select(.status != "PASSED")] | length')
if [ "$DETAIL_FAILURES" -gt 0 ] 2>/dev/null; then
    echo "  Failed checks:"
    echo "$REPORT" | jq -r '.details[] | select(.status != "PASSED") | .name' | head -20 | while read -r t; do echo "    - $t"; done
    if [ "$DETAIL_FAILURES" -gt 20 ] 2>/dev/null; then
        echo "    ... and $((DETAIL_FAILURES - 20)) more"
    fi
else
    FAILED_TESTS=$(echo "$REPORT" | jq -r '.test_details[]? | select(.status != "PASSED") | .name')
    if [ -n "$FAILED_TESTS" ]; then
        echo "  Failed tests:"
        echo "$FAILED_TESTS" | head -20 | while read -r t; do echo "    - $t"; done
    else
        if [ "$TOTAL" -gt 0 ] 2>/dev/null; then
            echo "  All tests passed!"
        fi
    fi
fi
# Show full details if --details flag
if [ "$DETAILS_MODE" -eq 1 ]; then
    if echo "$REPORT" | jq -e '.resource_diagnostics.memory_oom_observed == true' >/dev/null; then
        echo ""
        echo "  Resource observation (container lifetime):"
        echo "$REPORT" | jq -r '.resource_diagnostics | "    Memory OOM observed; memory limit (bytes): \(.memory_limit_bytes); container running: \(.container_running)"'
        echo "    This observation does not determine the evaluation result."
    fi
    HAS_DETAILS=$(echo "$REPORT" | jq -r '.details | length')
    if [ "$HAS_DETAILS" -gt 0 ] 2>/dev/null; then
        echo ""
        echo "  Details:"
        echo "$REPORT" | jq -r '.details[] | "    [\(.status)] \(.name)\(if .message then ": " + .message else "" end)"'
    fi
fi
echo "========================================"
echo ""
"""


def _agent_execution_budget_note(agent_budget_seconds: int | None) -> str:
    """Describe the runner's execution limit without changing task-local limits."""
    if agent_budget_seconds is None:
        return ""
    if type(agent_budget_seconds) is not int or agent_budget_seconds <= 0:
        raise ValueError("Agent execution budget must be a positive integer in seconds")
    return (
        "### Agent Execution Budget\n\n"
        f"The runner allows **{agent_budget_seconds} seconds** of Agent execution, "
        "shared across all resumed executions. Installation and final grading are "
        "outside this budget. This replaces conflicting whole-run Agent time limits "
        "in the task query or workspace documentation; per-command, per-test, "
        "training, and judge limits still apply. This is an upper limit, not a "
        "minimum required duration.\n\n"
        "Each execution receives `SFORGE_AGENT_BUDGET_SECONDS` (total budget), "
        "`SFORGE_AGENT_REMAINING_SECONDS` (remaining whole seconds at dispatch), "
        "and `SFORGE_AGENT_SEGMENT_STARTED_AT` (host Unix timestamp at dispatch). "
        "These environment values are snapshots, not live counters; refresh your "
        "remaining-time estimate using elapsed time within the execution. "
        "A resumed execution receives a new snapshot, not a fresh total budget.\n\n"
    )


def generate_evolve_prompt(
    original_query: str,
    submit_paths: list[str] | None = None,
    internet: bool = True,
    max_submissions: int | None = None,
    submission_cooldown: int | None = None,
    agent_budget_seconds: int | None = None,
) -> str:
    """Generate the enhanced prompt wrapping the task query with eval instructions."""
    submit_files_note = ""
    if submit_paths:
        paths_str = ", ".join(f"`{p}`" for p in submit_paths)
        submit_files_note = (
            f"\n### Submitted Files\n\n"
            f"Only the following paths are submitted for evaluation: {paths_str}\n\n"
            f"**Keep these files in a compilable/runnable state at all times.** "
            f"A background process periodically auto-evaluates your code — if the "
            f"submitted files are broken, incomplete, or contain syntax errors at "
            f"that moment, the auto-evaluation will fail. Write changes to disk "
            f"promptly and ensure the submitted files always represent your current "
            f"best solution.\n"
        )

    network_note = ""
    if not internet:
        network_note = (
            "\n### Network Environment\n\n"
            "**This environment has NO internet access.** "
            "Only the judge server and the AI API are reachable. "
            "Do not attempt to download packages, fetch remote resources, "
            "or access external URLs — all dependencies are pre-installed "
            "in the workspace.\n"
        )

    budget_note = ""
    if max_submissions is not None or submission_cooldown is not None:
        parts = []
        if max_submissions is not None:
            parts.append(
                f"- You have a **limited number of submissions ({max_submissions} total)**. "
                f"Plan carefully and validate locally before submitting."
            )
        if submission_cooldown is not None:
            parts.append(
                f"- There is a **minimum interval of {submission_cooldown}s** between submissions."
            )
        budget_note = "\n### Submission Limits\n\n" + "\n".join(parts) + "\n"

    how_to_test = (
        "### How to Test Your Code\n\n"
        "- Run `sforge-submit` to submit your current code for evaluation. "
        "It will package the files, send them to the judge server, and return "
        "results showing score, pass rate, and a summary of findings.\n"
        "- Run `sforge-submit --details` to submit and see detailed per-test results.\n"
        "- Run `sforge-submit --no-wait` to submit a snapshot and return its ID "
        "as soon as it is accepted. Acceptance is not a grading result. "
        "The judge evaluates the archived bytes: later workspace edits do not change "
        "that submission. You can continue independent work while grading runs, then "
        "check its feedback once with `sforge-submit --result ID --no-wait --details`. "
        "If still queued or running, it returns pending with no score; continue useful "
        "independent work before checking the same ID again. Omit `--no-wait` when "
        "you need to wait for that result. Keep track of "
        "which candidate each ID represents, and preserve any version you may need "
        "to restore; result retrieval does not restore source files.\n"
        "- Run `sforge-submit --list` to view previous submissions and scores, "
        "remaining submissions, and current cooldown for this run. This read-only "
        "query does not submit or reserve a round; the server checks limits again "
        "when you submit. Older servers may report limits unavailable.\n"
        "- If submission or polling is interrupted, run `sforge-submit --result ID --details` "
        "to read the accepted submission's result without submitting again. "
        "If its ID was not received, inspect `--list` first; do not blindly resubmit.\n\n"
        "You should use these regularly to check your progress and identify issues.\n"
    )

    return (
        "## Iterative Evaluation Mode\n\n"
        "You are working with iterative test feedback. "
        "After implementing code, you can submit your work for evaluation at any "
        "time to see which tests pass and which fail, then iterate based on the results.\n\n"
        f"{_agent_execution_budget_note(agent_budget_seconds)}"
        f"{how_to_test}"
        f"{submit_files_note}"
        f"{budget_note}"
        f"{network_note}\n"
        "### Strategy\n\n"
        "- **Implement incrementally**: Complete one module/project at a time\n"
        "- **Read test feedback carefully**: Failed test names often hint at what's broken\n"
        "- **Iterate**: Fix failing tests based on the feedback, then submit again\n\n"
        "### Scoring\n\n"
        "- Your **best score** across all submissions is your final score\n"
        "- You don't lose points for failed attempts — experimentation is encouraged\n\n"
        "---\n\n"
        f"{original_query}\n"
    )


def generate_game_prompt(
    original_query: str,
    internet: bool = True,
    agent_budget_seconds: int | None = None,
) -> str:
    """Generate the enhanced prompt for game mode.

    Wraps the task-specific query with common game interaction instructions
    (HTTP API docs, tips, keypress handling, rules, scoring). When `internet`
    is False, also tells the agent that walkthrough lookups are blocked, so
    it doesn't waste turns on failing web fetches.
    """
    network_note = ""
    if not internet:
        network_note = (
            "## Network Environment\n\n"
            "**This environment has NO internet access.** Only the game server "
            "(`GAME_SERVER_URL`) and the AI API are reachable; everything else "
            "is iptables-blocked. **Do not** attempt to search the web, fetch "
            "walkthroughs, or download external resources — those calls will "
            "time out and waste your turns. Solve the game through gameplay.\n\n"
            "---\n\n"
        )

    return (
        "## Game Mode\n\n"
        "You are playing an **interactive fiction game** via an HTTP API. "
        "Send commands to the game server and maximize your score.\n\n"
        f"{_agent_execution_budget_note(agent_budget_seconds)}"
        "---\n\n"
        f"{network_note}"
        "## Game Server HTTP API\n\n"
        "The server URL is available in the `GAME_SERVER_URL` environment variable.\n\n"
        "### Start a new game\n"
        "```\n"
        "POST {GAME_SERVER_URL}/new\n"
        "Body: {}\n"
        'Response: {"session_id": "abc123", "observation": "...", "score": 0, '
        '"peak_score": 0, "max_score": 350, "done": false, "moves": 0}\n'
        "```\n\n"
        "### Take an action\n"
        "```\n"
        "POST {GAME_SERVER_URL}/{session_id}/step\n"
        'Body: {"action": "go north"}\n'
        'Response: {"session_id": "...", "observation": "...", "score": 5, '
        '"peak_score": 5, "max_score": 350, "done": false, "moves": 1}\n'
        "```\n\n"
        "### Check status\n"
        "```\n"
        "GET {GAME_SERVER_URL}/{session_id}/status\n"
        'Response: {"session_id": "...", "score": 5, "peak_score": 5, '
        '"max_score": 350, "done": false, "moves": 1}\n'
        "```\n\n"
        "### Close session\n"
        "```\n"
        "POST {GAME_SERVER_URL}/{session_id}/close\n"
        'Response: {"session_id": "...", "final_score": 5, "peak_score": 5, '
        '"max_score": 350, "moves": 50}\n'
        "```\n\n"
        "### Score fields\n"
        "- `score` — the score you currently have in this session.\n"
        "- `peak_score` — the highest score you reached so far in this session "
        "(useful in games where score can decrease).\n"
        "- `max_score` — the theoretical maximum score for the game (constant). "
        "Your goal is to get `score` as close to `max_score` as possible.\n\n"
        "---\n\n"
        "## Tips\n\n"
        "- Interactive fiction games accept natural language commands: "
        '"go north", "take lamp", "examine door", "open mailbox", "read leaflet", etc.\n'
        "- `look` describes the current room. Moving in a direction that doesn't exist gives a failure message.\n"
        "- The game has puzzles — read descriptions carefully and experiment.\n"
        "- You can start multiple game sessions to explore different strategies.\n"
        "- **Keypress prompts**: Some games pause with prompts like "
        '`(Press SPACE to Continue)` or `[press BACKSPACE to return to game]`. '
        "When you see these in the observation, send a **single space** `\" \"` as "
        "the next action to dismiss them. These are Z-machine interactive prompts "
        "that require a single-character response, not a regular text command.\n\n"
        "---\n\n"
        "## Environment\n\n"
        "- `GAME_SERVER_URL` environment variable is pre-set.\n"
        "- Use `curl`, `urllib.request`, or `http.client` for HTTP requests.\n"
        "- Python 3.10 stdlib is available.\n\n"
        "---\n\n"
        "## Rules\n\n"
        "- All game interaction must go through the HTTP API.\n"
        "- You may start multiple game sessions to explore.\n"
        "- Maximize your score across all sessions.\n\n"
        "## Scoring\n\n"
        "- Your **best score** across all game sessions is your final result, "
        "normalized against `max_score` (the game's theoretical maximum).\n"
        "- You don't lose points for failed sessions — experimentation is encouraged.\n\n"
        "---\n\n"
        f"{original_query}\n"
    )
