#!/usr/bin/env bash
# scripts/local/worker-supervisor.sh — unattended worker recycle loop (issue #2090).
#
# One issue, one worker session, one process exit (context economy). This
# supervisor is what turns that into a safe *overnight loop*: single-instance
# lock, fail-closed preflight (wait, never spin), a crash-loop breaker, hard
# issue/wall-clock budgets, and one journal line + notification per iteration.
# The worker itself is opaque to this script — it is judged ONLY by its exit
# code and the ITERATION MARKER it leaves behind.
#
# ============================================================================
# ITERATION MARKER CONTRACT (mirrored in
# docs/scratch/agentic-workflow-audit/doc-deltas-supervisor.md — the worker
# skill MUST implement exactly this contract)
# ============================================================================
# The worker's LAST act, whatever else happened, is to write JSON to
# $MARKER_FILE (default: <repo-root>/.worker-last-iteration.json):
#
#   {"outcome":"finalized|no-work|blocked","issue":<int|null>,
#    "pr":"<url>|null","duration_s":<int>,"reason":"<string, required if blocked>"}
#
#   finalized — claimed an issue, drove it through gate/review/merge-on-green,
#               flow-finalized it. issue + pr MUST be set (both non-null,
#               non-missing) — a "finalized" marker with a null/missing issue
#               or pr is judged ABNORMAL (counts toward BREAKER_N, does NOT
#               count toward MAX_ISSUES) rather than trusted at face value.
#               A well-formed "finalized" is still NOT trusted on field shape
#               alone (issue #2670): the supervisor VERIFIES the claimed PR is
#               actually merged via `gh pr view <pr> --json state,mergedAt,
#               autoMergeRequest` before crediting the iteration. Verdicts, in the
#               journal's `verified:` field:
#                 verified=merged           — gh reports state MERGED. Counts
#                                             toward MAX_ISSUES, resets the breaker
#                                             (the ONLY path that credits an issue).
#                 verified=pending-automerge — OPEN with auto-merge armed (the
#                                             closer's documented auto-merge path).
#                                             The PR WILL land — a legitimate PENDING
#                                             state, not a false finalize: does NOT
#                                             count toward MAX_ISSUES yet, a
#                                             default-priority page, breaker-NEUTRAL.
#                                             Outcome `finalized-pending-automerge`
#                                             (roborev 1813).
#                 verified=mismatch:<STATE> — a STABLE non-MERGED state without
#                                             auto-merge armed, confirmed across
#                                             MISMATCH_RETRIES grace reads (absorbing
#                                             read-after-merge lag). Judged ABNORMAL:
#                                             does NOT count toward MAX_ISSUES, counts
#                                             toward the breaker, HIGH page naming the
#                                             discrepancy.
#                 verified=mismatch:UNRESOLVED — the marker's `pr` is FORGED: a
#                                             non-numeric ref, a URL that is not
#                                             EXACTLY a pmcfadin/cqlite PR URL, gh
#                                             could not resolve it (stderr signature),
#                                             or a cleanly-PARSED response lacking the
#                                             PR state. Same ESCALATION as mismatch.
#                 verified=unverified       — a TOOLING/TRANSPORT gap, NEVER forgery:
#                                             gh binary missing, network/auth/rate-limit
#                                             error, OR no JSON parser present / the
#                                             payload failed to parse (a tooling gap
#                                             must never read as forgery — roborev 1813).
#                                             FAIL-INFORMATIVE: logged + default-priority
#                                             page, does NOT count as a completed issue,
#                                             NEUTRAL to the breaker (neither trips nor
#                                             resets). Outcome `finalized-unverified`. A
#                                             PERSISTENT outage is bounded: UNVERIFIED_MAX
#                                             consecutive unverified finalizes STOP the
#                                             loop (verify-unavailable, exit 1, HIGH page)
#                                             so the MAX_ISSUES ceiling can't drift.
#               Bounded holds: a `leftover-processes` preflight hold that never
#               clears (a non-self-clearing orphan) STOPS the loop loudly after
#               LEFTOVER_HOLD_MAX passes — naming the surviving PIDs — and every hold
#               pass re-checks the stop-file and the wall-clock budget (issue #2670,
#               roborev 1810): a hold can never latch the supervisor silently.
#   no-work   — rehydrated from the board, nothing Ready (or nothing to
#               resume). issue/pr may be null. Does NOT count toward
#               MAX_ISSUES; triggers a BACKOFF_NOWORK_SECS sleep so the loop
#               doesn't hot-poll an empty board. Resets the breaker.
#   blocked   — made progress but stopped short of merge for a reason needing
#               the owner (design-call finding, scope question, HOLD order,
#               unmet requirement). "reason" MUST be set. Does NOT count
#               toward MAX_ISSUES. Resets the breaker; the issue is
#               remembered (LAST_BLOCKED_ISSUE). If the SAME issue reports
#               "blocked" on two consecutive iterations, the supervisor treats
#               the queue as head-blocked and STOPS cleanly (high-priority
#               notify, exit 0) rather than looping until MAX_HOURS — it is
#               retried exactly once, never auto-retried indefinitely.
#
#               Two "blocked" reasons are a distinct CLEAN PARK (issue #2666,
#               park-and-resume) rather than an owner-escalation:
#                 reason=seam1-approval  — the worker hit Seam 1 (an unapproved
#                                          design spec) in an unattended session.
#                 reason=needs-decision  — a genuine mid-run owner decision.
#               For either, the worker's contract is: post ONE structured
#               question comment on the issue (rendered options + recommendation
#               + default), add the `needs-decision` label, write this marker
#               (optionally an extra "question" field = one-line question
#               summary for the page), and EXIT — releasing the machine. NEVER
#               wait, NEVER call AskUserQuestion unattended. The supervisor
#               judges these NORMAL: verdict `parked-on-owner`, never toward the
#               breaker, and normally does NOT head-block the queue (the labeled
#               issue is excluded from the worker's next pickup until the owner
#               answers and the label clears), fires ONE high-priority page, and
#               moves on to the next Ready issue. Safety valve (mirrors the
#               blocked-path F2 guard): if the SAME issue parks on two
#               consecutive iterations — the label evidently never applied, so
#               the pickup exclusion is not holding — the supervisor pages the
#               owner (head-blocked-on-decision) and STOPS cleanly rather than
#               re-asking one question until MAX_ISSUES.
#
# stuck-on-question (mid-iteration, no marker) — a worker WEDGED on an
#               interactive prompt (AskUserQuestion / permission prompt / menu)
#               in an unattended session never writes a marker; it just burns
#               MAX_ITER_SECS and would look "abnormal". A watchdog classifies
#               this on POSITIVE WEDGE EVIDENCE, not a bare substring match (the
#               Claude CLI routinely prints tool names like `AskUserQuestion` in
#               normal trace, so a whole-log substring match would misclassify
#               ordinary crashes as stuck and permanently defeat the breaker —
#               roborev 1773). Every STUCK_POLL_SECS the watchdog scans, and
#               declares `stuck-on-question` ONLY when ALL hold across TWO
#               consecutive scans: (a) the process is alive, (b) a prompt
#               signature is in the LAST ~20 log lines (tail, not whole file),
#               and (c) the log has not grown between the scans (a wedged prompt
#               emits nothing). It then pages the owner and records the verdict
#               when the worker later exits without a clean marker. NOT abnormal,
#               never toward BREAKER_N. A marker-less exit whose only signature is
#               a stray scrollback match (fails tail-locality or no-growth) stays
#               ABNORMAL and counts toward the breaker.
#
# Any other outcome value, a marker missing required fields, a nonzero worker
# exit code, OR no marker file present when the worker process exits => the
# iteration is judged ABNORMAL and counts toward BREAKER_N. The supervisor
# removes any pre-existing marker before every spawn — a marker left behind
# by a prior (possibly crashed) invocation must never be re-judged as this
# iteration's outcome.
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Config (env-overridable; all defaults per issue #2090)
# ---------------------------------------------------------------------------
MAX_ISSUES="${MAX_ISSUES:-4}"
MAX_HOURS="${MAX_HOURS:-8}"
DISK_FLOOR_GB="${DISK_FLOOR_GB:-40}"
BREAKER_N="${BREAKER_N:-3}"
BACKOFF_NOWORK_SECS="${BACKOFF_NOWORK_SECS:-900}"
HOLD_POLL_SECS="${HOLD_POLL_SECS:-300}"
MAX_ITER_SECS="${MAX_ITER_SECS:-7200}"
# Mid-iteration stuck-on-question watchdog (issue #2666): how often to tail the
# live worker log for an interactive-prompt signature, and the signatures to
# match. Both env-overridable (tests tighten the poll interval).
STUCK_POLL_SECS="${STUCK_POLL_SECS:-30}"
PROMPT_SIGNATURE_RE="${PROMPT_SIGNATURE_RE:-AskUserQuestion|Do you want to|waiting for input|❯}"
# issue #2670: bound the two ways verdict-integrity can otherwise latch the loop.
# LEFTOVER_HOLD_MAX — consecutive preflight holds on `leftover-processes` before
#   the supervisor STOPS loudly. A non-self-clearing orphan (a wedged worker
#   Claude CLI that never dies) must terminate the loop, never latch it silently.
# UNVERIFIED_MAX — consecutive finalized markers gh could not verify before the
#   supervisor STOPS. A persistent verification outage is an operator problem and
#   would otherwise let the MAX_ISSUES ceiling drift (uncounted-forever iterations).
LEFTOVER_HOLD_MAX="${LEFTOVER_HOLD_MAX:-3}"
UNVERIFIED_MAX="${UNVERIFIED_MAX:-2}"
# issue #2670 (roborev 1813): before escalating a non-merged PR state to a
# mismatch, re-read gh a few times to absorb read-after-merge lag. Env-tunable so
# the tooling tests set the wait to 0 (nothing sleeps in the suite).
MISMATCH_RETRIES="${MISMATCH_RETRIES:-3}"
MISMATCH_RETRY_WAIT_SECS="${MISMATCH_RETRY_WAIT_SECS:-10}"

SUPERVISOR_LOCK="${SUPERVISOR_LOCK:-${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock}"
STOP_FILE="${STOP_FILE:-$REPO_ROOT/.worker-stop}"
MARKER_FILE="${MARKER_FILE:-$REPO_ROOT/.worker-last-iteration.json}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/worker-supervisor}"

detect_ncpu() {
  if command -v nproc >/dev/null 2>&1; then nproc
  elif command -v getconf >/dev/null 2>&1 && getconf _NPROCESSORS_ONLN >/dev/null 2>&1; then getconf _NPROCESSORS_ONLN
  elif command -v sysctl >/dev/null 2>&1; then sysctl -n hw.ncpu
  else echo 4
  fi
}
LOAD_MAX="${LOAD_MAX:-$(detect_ncpu)}"

# Real invocation (documented default; every real fleet run should pin this
# explicitly rather than rely on the default prompt text drifting).
if [[ -z "${WORKER_CMD:-}" ]]; then
  WORKER_CMD="claude --agent worker 'Resume the existing issue-<N>-* claim branch on this machine if one exists; otherwise claim the next Ready issue from the board. Run it to completion (implement, gate, review, merge on green, finalize, telemetry stamp). Write the iteration marker as your last act, then exit.'"
fi

# NOTE: default probe commands are assigned via explicit if/then blocks, not a
# compact "${VAR:-default}" one-liner — the deeply nested/escaped quoting that
# form requires corrupts the ALREADY-SET value at bash's parse stage (verified
# empirically: a real override got mangled even though the default clause is
# never evaluated when the var is set). Plain assignment has no such hazard.
if [[ -z "${LOAD_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # literal $1 for the later `bash -c` eval, not expanded now.
  LOAD_PROBE_CMD='if [ -r /proc/loadavg ]; then cut -d" " -f1 /proc/loadavg; else sysctl -n vm.loadavg | tr -d "{}" | awk "{print \$1}"; fi'
fi
if [[ -z "${DISK_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # single-quoted $4 is intentional: literal text for the later `bash -c` eval, not expanded now.
  DISK_PROBE_CMD="df -Pk \"$REPO_ROOT\" | awk 'NR==2{print int(\$4/1024/1024)}'"
fi
if [[ -z "${PROC_PROBE_CMD:-}" ]]; then
  # Leftover-process probe (issue #2670): two families of prior-iteration debris
  # block the next spawn (HOLD-and-poll, same as load/disk):
  #   (1) build/gate processes — cargo/nextest/gate_slot_daemon.
  #   (2) an orphaned worker Claude CLI from a prior iteration (a classic survivor
  #       of a SIGTERM'd wrapper, and a stuck-on-question hazard that would burn
  #       the next iteration).
  # The Claude match is keyed on the supervisor's OWN spawn shape — `--agent worker`
  # (see WORKER_CMD) — NOT a bare `claude`. A plain interactive `claude` REPL, or a
  # different-agent session (`--agent flow-lead`), never carries that marker, so a
  # legitimate interactive session on the box is not matched. LIMIT: an operator who
  # deliberately runs `claude --agent worker` by hand WILL be matched (correctly — by
  # the one-worker-per-machine rule #1930 that is itself leftover worker debris). The
  # current iteration's own worker has already exited before preflight runs, so any
  # `--agent worker` process seen here is genuinely from a prior iteration. `sort -u`
  # dedups a PID that matched both patterns.
  #
  # SELF-MATCH DEFUSED (roborev 1813, MED-HIGH): the brace-group form keeps a live
  # `bash -c` wrapper whose OWN argv contains these pattern strings, so a naive
  # pattern would match the wrapper and report a phantom leftover at EVERY boot —
  # hard-stopping every supervisor. The classic pgrep bracket trick (`[c]argo`,
  # `[c]laude`) makes each regex still match the real process (`cargo`, `claude`)
  # while the bracketed pattern TEXT in the wrapper's argv (`[c]argo`) no longer
  # contains the literal substring, so the wrapper cannot match itself.
  PROC_PROBE_CMD="{ pgrep -f '[c]argo |[n]extest|[g]ate_slot_daemon'; pgrep -f '[c]laude.*--agent worker'; } 2>/dev/null | sort -u | wc -l | tr -d ' '"
fi
# Companion to PROC_PROBE_CMD: lists the surviving PIDs (pid + cmd) so a
# leftover-processes STOP can NAME what never cleared (issue #2670). Best-effort —
# an empty result just yields "<unavailable>" in the page. Same bracket-trick
# self-match defusal as PROC_PROBE_CMD (roborev 1813).
if [[ -z "${PROC_LIST_CMD:-}" ]]; then
  PROC_LIST_CMD="{ pgrep -lf '[c]argo |[n]extest|[g]ate_slot_daemon'; pgrep -lf '[c]laude.*--agent worker'; } 2>/dev/null | sort -u"
fi

# GH verification of a "finalized" marker's PR (issue #2670). $1 (passed by the
# later `bash -c ... _ "$prnum"`) is the PR number. Emits the raw `gh pr view` JSON
# on stdout. `autoMergeRequest` is queried so the verifier can recognize the
# closer's auto-armed path (OPEN with auto-merge enabled), a legitimate pending
# state rather than a false-finalize mismatch (roborev 1813). Overridable so the
# tooling tests can stub GitHub with a PATH-free command string.
if [[ -z "${GH_VERIFY_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # literal $1 for the later `bash -c` eval, not expanded now.
  GH_VERIFY_CMD='gh pr view "$1" --repo pmcfadin/cqlite --json state,mergedAt,autoMergeRequest'
fi

NOOP_NOTIFY_MARKER="__noop_notify__"
if [[ -z "${NOTIFY_CMD:-}" ]]; then
  if command -v agent-notify >/dev/null 2>&1; then NOTIFY_CMD="agent-notify"; else NOTIFY_CMD="$NOOP_NOTIFY_MARKER"; fi
fi

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
WARNED_NOOP_NOTIFY=0
log() { printf '[worker-supervisor] %s\n' "$*" >&2; }

notify() {
  local priority="$1" title="$2" message="$3" category="completion"
  [[ "$priority" == "high" ]] && category="error"
  if [[ "$NOTIFY_CMD" == "$NOOP_NOTIFY_MARKER" ]]; then
    if [[ "$WARNED_NOOP_NOTIFY" -eq 0 ]]; then
      log "WARN: agent-notify not on PATH; notifications are no-ops for this run"
      WARNED_NOOP_NOTIFY=1
    fi
    return 0
  fi
  "$NOTIFY_CMD" --category "$category" "$title" "$message" || log "WARN: notify command failed (non-fatal)"
}

is_gt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)>(b+0)) exit 0; exit 1 }'; }
is_lt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)<(b+0)) exit 0; exit 1 }'; }

# detect_prompt_signature <logfile>: true (exit 0) when the LAST ~20 lines of the
# worker's live log show an interactive-prompt signature — a wedged
# AskUserQuestion, a permission prompt, a `❯` menu block, or a "waiting for input"
# line (issue #2666). Deliberately a TAIL scan, not a whole-log scan: the Claude
# CLI routinely prints tool names like `AskUserQuestion` in its normal trace, so
# a bare whole-file substring match would misclassify ordinary crashes as stuck
# and permanently defeat the breaker (roborev 1773). A stray match in old
# scrollback is not evidence of a live wedge; only a signature still resident in
# the last frames (paired with no-growth — see the supervise loop) is.
STUCK_TAIL_LINES="${STUCK_TAIL_LINES:-20}"
detect_prompt_signature() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 1
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -qE "$PROMPT_SIGNATURE_RE"
}

# captured_question <logfile>: the matching prompt line(s) from the same tail
# window, collapsed to a single ≤300-char line for the ntfy body. Empty when
# nothing matched.
captured_question() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 0
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -E "$PROMPT_SIGNATURE_RE" 2>/dev/null \
    | head -n 3 | tr '\n' ' ' | cut -c1-300
}

# log_size <logfile>: byte size of the file, or 0 when absent. A wedged
# interactive prompt produces NO further output, so a frozen byte size across two
# consecutive scans is the positive evidence that distinguishes a genuine wedge
# from a busy worker that merely printed a tool name and kept writing.
log_size() {
  local f="$1"
  [[ -f "$f" ]] || { printf '0'; return 0; }
  wc -c <"$f" 2>/dev/null | tr -d ' '
}

marker_field() {
  local field="$1"
  [[ -f "$MARKER_FILE" ]] || return 0
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg f "$field" '.[$f] // empty' "$MARKER_FILE" 2>/dev/null
  else
    python3 -c '
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
v = d.get(sys.argv[2])
print(v if v is not None else "")
' "$MARKER_FILE" "$field" 2>/dev/null || true
  fi
}

# verify_finalized_pr <pr>: check the claimed PR is actually merged (issue #2670).
# Echoes exactly one verdict token on stdout (never returns nonzero — the caller's
# `case` handles every path explicitly):
#   merged              — gh reported state MERGED. The ONLY path that credits an issue.
#   pending-automerge   — OPEN with `autoMergeRequest` armed: the closer's documented
#                         auto-merge path, so the PR WILL land. Legitimate pending
#                         state — uncounted (for now), default-priority page,
#                         breaker-NEUTRAL — not an escalation (roborev 1813).
#   mismatch:<STATE>    — a STABLE non-MERGED state without auto-merge armed, after
#                         MISMATCH_RETRIES grace reads (absorbing read-after-merge lag).
#   mismatch:UNRESOLVED — the marker's `pr` is FORGED/GARBAGE: a non-numeric ref, a
#                         URL that is not EXACTLY a pmcfadin/cqlite PR URL (finding 4),
#                         gh could not resolve it (resolve-failure stderr signature),
#                         or a cleanly-PARSED response that simply lacks a PR state. An
#                         ESCALATION (abnormal, high page, breaker-counting).
#   unverified          — a TOOLING/TRANSPORT gap, never forgery (finding 2): gh binary
#                         missing, a network/auth/rate-limit error, OR no JSON parser
#                         present / the payload failed to parse. Fail-informative;
#                         neutral to the breaker and uncounted.
verify_finalized_pr() {
  local pr="$1" prnum out err rc parser_ok state automerge errfile pyout attempt=0
  # Finding 4 (URL shape): a URL-form pr must be EXACTLY a pmcfadin/cqlite PR URL;
  # any other host/repo is a forged/foreign reference. A bare number (optional
  # leading '#') is accepted; anything else is forged.
  if [[ "$pr" =~ ^[a-zA-Z][a-zA-Z0-9+.-]*:// ]]; then
    if [[ "$pr" =~ ^https://github\.com/pmcfadin/cqlite/pull/([0-9]+)$ ]]; then
      prnum="${BASH_REMATCH[1]}"
    else
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
  else
    prnum="${pr#\#}"
    if [[ ! "$prnum" =~ ^[0-9]+$ ]]; then
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
  fi

  while :; do
    errfile="$(mktemp "${TMPDIR:-/tmp}/gh-verify.XXXXXX")"
    # Context-independent capture (roborev 1810): capture rc explicitly via the
    # `&& rc=0 || rc=$?` list, not the errexit-in-assignment quirk.
    out="$(bash -c "$GH_VERIFY_CMD" _ "$prnum" 2>"$errfile")" && rc=0 || rc=$?
    err="$(cat "$errfile" 2>/dev/null || true)"
    rm -f "$errfile"

    # gh binary missing (exec failure) is transport class, never a forged PR.
    if [[ "$rc" -eq 127 ]]; then
      printf 'unverified'
      return 0
    fi
    # gh RAN and could not resolve the ref (bad PR number) — a forged reference,
    # distinguished from a transport error by the resolve-failure stderr signature.
    if printf '%s' "$err" | grep -qiE 'could not resolve|no (pull request|pr)s? (found|with)|not found|no pull requests found'; then
      printf 'mismatch:UNRESOLVED'
      return 0
    fi

    # Parse. parser_ok = a parser is present AND the payload parsed as JSON.
    # Finding 2: a tooling gap (no parser / parse error) is transport class
    # (unverified), NEVER forgery. mismatch:UNRESOLVED is reserved for a
    # SUCCESSFULLY-parsed response that simply lacks the PR state.
    parser_ok=0
    state=""
    automerge=""
    if command -v jq >/dev/null 2>&1; then
      if printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
        parser_ok=1
        state="$(printf '%s' "$out" | jq -r '.state // empty' 2>/dev/null || true)"
        automerge="$(printf '%s' "$out" | jq -r '.autoMergeRequest // empty' 2>/dev/null || true)"
      fi
    elif command -v python3 >/dev/null 2>&1; then
      if pyout="$(printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
print(d.get("state") or "")
print("armed" if d.get("autoMergeRequest") else "")
' 2>/dev/null)"; then
        parser_ok=1
        state="$(printf '%s\n' "$pyout" | sed -n 1p)"
        automerge="$(printf '%s\n' "$pyout" | sed -n 2p)"
      fi
    fi

    if [[ "$parser_ok" -eq 0 ]]; then
      # No parser, or the payload failed to parse — our tooling/transport gap, not
      # the worker's forgery.
      printf 'unverified'
      return 0
    fi
    if [[ -z "$state" ]]; then
      # Parsed cleanly but no PR state — a genuine unresolved/garbage response.
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
    if [[ "$state" == "MERGED" ]]; then
      printf 'merged'
      return 0
    fi
    # Finding 3: OPEN with auto-merge armed is a LEGITIMATE closer path (the PR will
    # land automatically) — a distinct, non-escalating verdict, not a mismatch.
    if [[ "$state" == "OPEN" && -n "$automerge" ]]; then
      printf 'pending-automerge'
      return 0
    fi
    # A resolved, non-merged state without auto-merge. Grace: read-after-merge lag
    # can briefly show OPEN just after a merge — retry a few times before escalating.
    attempt=$((attempt + 1))
    if [[ "$attempt" -lt "$MISMATCH_RETRIES" ]]; then
      sleep "$MISMATCH_RETRY_WAIT_SECS"
      continue
    fi
    printf 'mismatch:%s' "$state"
    return 0
  done
}

# json_or_null <value>: quotes+escapes a string field for embedding into the
# JSONL journal, or emits a bare `null` when empty. A quote/backslash/newline
# in an untrusted field (pr URL, blocked "reason" text) must never be allowed
# to corrupt journal JSON — printf '"%s"' alone does not escape those chars.
# Preferred path: python3 json.dumps (already a project dependency, same as
# marker_field's jq-absent fallback). Degraded fallback (python3 somehow
# absent): strip to a conservative safe charset so the line still parses,
# rather than emitting broken JSON.
json_or_null() {
  local v="$1"
  [[ -n "$v" ]] || { printf 'null'; return 0; }
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$v" 2>/dev/null && return 0
  fi
  printf '"%s"' "$(printf '%s' "$v" | tr -cd 'A-Za-z0-9:/._#?=&-')"
}
num_or_null() { [[ "$1" =~ ^[0-9]+$ ]] && printf '%s' "$1" || printf 'null'; }

# journal_line <iter> <outcome> <issue> <pr> <duration_s> <exit_code> [reason] [verified]
# `reason` (only "blocked"/park iterations) and `verified` (only the finalized
# family, issue #2670) are optional; both `pr` and `reason` are free-form
# worker-controlled text and go through json_or_null so a stray quote/backslash/
# newline can never corrupt the JSONL line. `verified` is appended only when set,
# so every pre-existing 6/7-arg caller emits an unchanged line.
journal_line() {
  mkdir -p "$LOG_DIR"
  local jf="${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local verified_json=""
  [[ -n "${8:-}" ]] && verified_json=",\"verified\":$(json_or_null "$8")"
  printf '{"ts":"%s","iter":%d,"outcome":"%s","issue":%s,"pr":%s,"duration_s":%d,"exit_code":%d,"reason":%s%s}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "$(num_or_null "$3")" "$(json_or_null "$4")" "$5" "$6" "$(json_or_null "${7:-}")" "$verified_json" >>"$jf"
}

# ---------------------------------------------------------------------------
# Single-instance lock. macOS ships no flock(1); an atomic mkdir + pid-liveness
# check gives the same "only one supervisor per machine" guarantee portably.
# ---------------------------------------------------------------------------
acquire_lock() {
  if mkdir "$SUPERVISOR_LOCK" 2>/dev/null; then
    echo $$ >"$SUPERVISOR_LOCK/pid"
    trap 'rm -rf "$SUPERVISOR_LOCK" 2>/dev/null || true' EXIT
    return 0
  fi
  local holder_pid=""
  [[ -f "$SUPERVISOR_LOCK/pid" ]] && holder_pid="$(cat "$SUPERVISOR_LOCK/pid" 2>/dev/null || true)"
  if [[ -n "$holder_pid" ]] && kill -0 "$holder_pid" 2>/dev/null; then
    echo "worker-supervisor: another instance is already running (pid $holder_pid, lock $SUPERVISOR_LOCK)" >&2
    exit 1
  fi
  log "reclaiming stale lock $SUPERVISOR_LOCK (holder pid $holder_pid not alive)"
  # Atomic reclaim (rename-then-remove), not rm-then-mkdir: two supervisors
  # racing a dead-pid lock both taking the rm-then-mkdir path could both end up
  # believing they won. `mv` on the same filesystem is atomic, so only ONE
  # racer's mv can succeed against a given stale directory name; that racer
  # removes the renamed-aside copy and falls through to its own mkdir below.
  # The loser's mv fails (the name is already gone), so it falls through to the
  # normal mkdir-fails path and exits loudly instead of silently co-running.
  if mv "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null; then
    rm -rf "$SUPERVISOR_LOCK.stale.$$"
  fi
  if mkdir "$SUPERVISOR_LOCK" 2>/dev/null; then
    echo $$ >"$SUPERVISOR_LOCK/pid"
    trap 'rm -rf "$SUPERVISOR_LOCK" 2>/dev/null || true' EXIT
    return 0
  fi
  echo "worker-supervisor: failed to acquire lock $SUPERVISOR_LOCK" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# Preflight: fail-closed, wait-don't-spin. Returns a hold reason on stdout, or
# empty when clear. stop-file / budgets are handled by the caller (clean exit,
# not a hold).
# ---------------------------------------------------------------------------
preflight_reason() {
  local load procs disk
  load="$(bash -c "$LOAD_PROBE_CMD" 2>/dev/null || echo 0)"
  if is_gt "$load" "$LOAD_MAX"; then
    echo "load"
    return 0
  fi
  procs="$(bash -c "$PROC_PROBE_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-processes"
    return 0
  fi
  disk="$(bash -c "$DISK_PROBE_CMD" 2>/dev/null || echo 999999)"
  if is_lt "$disk" "$DISK_FLOOR_GB"; then
    echo "disk"
    return 0
  fi
  echo ""
}

LAST_HOLD_REASON=""
# preflight_wait is BOUNDED (issue #2670, roborev 1810 HIGH): a hold loop must not
# be able to latch the supervisor silently. Every pass re-checks the stop-file AND
# the wall-clock budget, and a `leftover-processes` hold that never clears (a
# non-self-clearing orphan) stops the loop loudly after LEFTOVER_HOLD_MAX passes
# rather than holding until MAX_HOURS with no page beyond the first HOLD.
preflight_wait() {
  local leftover_holds=0
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    local reason
    reason="$(preflight_reason)"
    if [[ -z "$reason" ]]; then
      LAST_HOLD_REASON=""
      return 0
    fi
    # Finding 5 (roborev 1813): count leftover-processes holds CUMULATIVELY across
    # the whole preflight_wait invocation — do NOT reset the counter when a
    # different hold reason (a transient load/disk blip) intervenes. Otherwise an
    # orphan that never dies but is occasionally masked by a load spike would reset
    # the bound every time and hold forever. A different reason simply doesn't add
    # to the leftover tally; a genuinely transient mix still clears (returns above).
    # The wall-clock budget re-check at the top of the loop bounds the total hold
    # time regardless of the reason mix.
    if [[ "$reason" == "leftover-processes" ]]; then
      leftover_holds=$((leftover_holds + 1))
      if [[ "$leftover_holds" -ge "$LEFTOVER_HOLD_MAX" ]]; then
        local pids
        pids="$(bash -c "$PROC_LIST_CMD" 2>/dev/null | tr '\n' ';' | cut -c1-300)"
        notify "high" "worker-supervisor: leftover-processes will not clear" \
          "held $leftover_holds times on leftover processes that never cleared — stopping loudly. Surviving: ${pids:-<unavailable>}"
        log "leftover-processes held $leftover_holds times (>= LEFTOVER_HOLD_MAX=$LEFTOVER_HOLD_MAX) without clearing; stopping. Surviving: ${pids:-<unavailable>}"
        finalize_exit "leftover-processes" 1
      fi
    fi
    if [[ "$reason" != "$LAST_HOLD_REASON" ]]; then
      notify "high" "worker-supervisor HOLD" "HOLD: $reason (repolling every ${HOLD_POLL_SECS}s, no spawn)"
      LAST_HOLD_REASON="$reason"
    fi
    log "HOLD: $reason; sleeping ${HOLD_POLL_SECS}s"
    sleep "$HOLD_POLL_SECS"
  done
}

# ---------------------------------------------------------------------------
# State + exit paths
# ---------------------------------------------------------------------------
ITER=0
ISSUES_DONE=0
CONSECUTIVE_ABNORMAL=0
CONSECUTIVE_UNVERIFIED=0
LAST_BLOCKED_ISSUE=""
LAST_PARKED_ISSUE=""
START_TS=$(date +%s)
MAX_HOURS_SECS=$((MAX_HOURS * 3600))

finalize_exit() {
  local reason="$1" code="$2"
  local elapsed=$(($(date +%s) - START_TS))
  mkdir -p "$LOG_DIR"
  printf '{"ts":"%s","iter":%d,"outcome":"summary","reason":"%s","issues_done":%d,"elapsed_s":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ITER" "$reason" "$ISSUES_DONE" "$elapsed" \
    >>"${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local prio="info"
  [[ "$reason" == "breaker" ]] && prio="high"
  notify "$prio" "worker-supervisor stopped" "reason=$reason issues_done=$ISSUES_DONE elapsed_s=$elapsed"
  exit "$code"
}

# ---------------------------------------------------------------------------
# One iteration: spawn, judge, journal.
# ---------------------------------------------------------------------------
run_iteration() {
  ITER=$((ITER + 1))
  rm -f "$MARKER_FILE"
  mkdir -p "$LOG_DIR"
  local logfile="$LOG_DIR/iter-${ITER}.log"
  local stuck_flag="$LOG_DIR/.iter-${ITER}.stuck"
  rm -f "$stuck_flag"
  local t0 t1 rc=0
  t0=$(date +%s)
  # Spawn the worker in the background and supervise it with a split-cadence poll
  # loop (issue #2666, hardened per roborev 1773):
  #   * exit/deadline check every 1s — near-blocking completion latency and
  #     MAX_ITER_SECS enforced at 1s granularity (portably — no coreutils
  #     `timeout` on macOS).
  #   * WEDGE scan every STUCK_POLL_SECS — classify `stuck-on-question` ONLY on
  #     positive wedge evidence that holds across TWO consecutive scans: (a) the
  #     process is still alive, (b) a prompt signature is in the LAST ~20 log
  #     lines (tail scan, not whole-log), AND (c) the log has NOT GROWN between
  #     the two scans (byte size unchanged). A wedged prompt emits no further
  #     output, so its tail + size freeze; a busy worker that merely printed the
  #     tool name in its trace keeps writing (size grows) and/or scrolls the match
  #     out of the tail — either fails the test and is ignored. Tradeoff: a
  #     genuine wedge takes up to 2*STUCK_POLL_SECS to confirm (vs an instant but
  #     false-positive-prone substring match), and a marker-less abnormal exit
  #     with a stray scrollback match stays ABNORMAL (counts toward the breaker) —
  #     exactly the guarantee the substring approach would have defeated.
  set +e
  bash -c "$WORKER_CMD" >"$logfile" 2>&1 &
  local wpid=$!
  local deadline=$((t0 + MAX_ITER_SECS))
  local stuck_notified=0 g now
  local last_scan_ts=$t0 prev_size=-1 prev_sig=0 cur_size cur_sig
  while kill -0 "$wpid" 2>/dev/null; do
    now=$(date +%s)
    if [[ "$now" -ge "$deadline" ]]; then
      log "iteration $ITER exceeded MAX_ITER_SECS=${MAX_ITER_SECS}s; terminating worker"
      kill -TERM "$wpid" 2>/dev/null
      g=0
      while kill -0 "$wpid" 2>/dev/null && [[ "$g" -lt 5 ]]; do sleep 1; g=$((g + 1)); done
      kill -KILL "$wpid" 2>/dev/null
      break
    fi
    if [[ "$stuck_notified" -eq 0 ]] && [[ $((now - last_scan_ts)) -ge "$STUCK_POLL_SECS" ]]; then
      last_scan_ts="$now"
      cur_size="$(log_size "$logfile")"
      if detect_prompt_signature "$logfile"; then cur_sig=1; else cur_sig=0; fi
      # Wedge confirmed only when the signature is present in the tail at BOTH
      # this scan and the prior one AND the log did not grow between them (and
      # the process is still alive — the loop condition). prev_size<0 = no prior
      # scan yet, so the very first scan can never confirm.
      if [[ "$prev_sig" -eq 1 && "$cur_sig" -eq 1 && "$prev_size" -ge 0 && "$cur_size" -eq "$prev_size" ]]; then
        local qtext
        qtext="$(captured_question "$logfile")"
        printf '%s' "$qtext" >"$stuck_flag"
        notify "high" "worker-supervisor: stuck-on-question (iter $ITER)" \
          "worker appears wedged on an interactive prompt (frozen log + signature across 2 polls): ${qtext:-<no text captured>}"
        log "iteration $ITER: wedge confirmed (frozen log + tail signature x2 polls); paged owner (stuck-on-question)"
        stuck_notified=1
      fi
      prev_size="$cur_size"
      prev_sig="$cur_sig"
    fi
    sleep 1
  done
  wait "$wpid"
  rc=$?
  set -e
  t1=$(date +%s)
  local duration=$((t1 - t0))

  # A live-but-prompt-blocked worker (detected mid-iteration) that then exits
  # without a trustworthy clean marker is a PARK-shaped stall, not a crash:
  # verdict `stuck-on-question`, owner already paged, NOT counted toward the
  # breaker (issue #2666). Guarded on "no clean marker" so a late false-positive
  # signature can never mask a real finalized/park outcome.
  if [[ -f "$stuck_flag" ]] && { [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; }; then
    # NEUTRAL, not transparent: like every other non-abnormal verdict this
    # resets the consecutive-abnormal counter, so a real prior crash chain is
    # BROKEN by a stuck iteration rather than silently continuing across it
    # (roborev 1769: `abnormal → stuck → abnormal → abnormal` must not trip
    # BREAKER_N=3). The owner has already been paged.
    CONSECUTIVE_ABNORMAL=0
    journal_line "$ITER" "stuck-on-question" "" "" "$duration" "$rc" "$(cat "$stuck_flag" 2>/dev/null)"
    log "iteration $ITER stuck-on-question (owner paged; breaker chain reset, not counted)"
    return 0
  fi

  if [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; then
    journal_line "$ITER" "abnormal" "" "" "$duration" "$rc"
    log "iteration $ITER abnormal (exit=$rc marker_present=$([[ -f "$MARKER_FILE" ]] && echo yes || echo no))"
    trip_breaker_or_continue
    return 0
  fi

  local outcome issue pr reason question
  outcome="$(marker_field outcome)"
  issue="$(marker_field issue)"
  pr="$(marker_field pr)"
  reason="$(marker_field reason)"
  question="$(marker_field question)"

  case "$outcome" in
    finalized)
      if [[ -z "$issue" || -z "$pr" ]]; then
        # F5: contract requires BOTH issue and pr on "finalized" — a marker
        # claiming success with either missing/null is untrustworthy and must
        # not be counted as a done issue nor reset the breaker.
        journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
        log "iteration $ITER abnormal (finalized marker missing issue/pr: issue='$issue' pr='$pr')"
        trip_breaker_or_continue
      else
        # Issue #2670: a well-formed "finalized" is NOT trusted on field shape
        # alone — verify the claimed PR is actually merged on GitHub before
        # crediting the iteration. A worker that parked its endgame yet wrote
        # "finalized" (or a stale/forged marker) must never count as done nor
        # reset the crash breaker.
        local verify
        verify="$(verify_finalized_pr "$pr")"
        case "$verify" in
          merged)
            CONSECUTIVE_ABNORMAL=0
            CONSECUTIVE_UNVERIFIED=0
            ISSUES_DONE=$((ISSUES_DONE + 1))
            journal_line "$ITER" "finalized" "$issue" "$pr" "$duration" "$rc" "" "merged"
            notify "info" "worker-supervisor: finalized issue $issue" "pr=$pr duration_s=$duration verified=merged"
            ;;
          pending-automerge)
            # OPEN with auto-merge armed (roborev 1813): the closer's documented
            # auto-merge path — the PR WILL land on green. NOT counted toward
            # MAX_ISSUES yet (it isn't merged), a default-priority page, and
            # breaker-NEUTRAL (gh resolved fine, so reset the unverified streak but
            # do NOT touch the crash breaker — this is a legitimate pending state).
            CONSECUTIVE_UNVERIFIED=0
            journal_line "$ITER" "finalized-pending-automerge" "$issue" "$pr" "$duration" "$rc" "" "pending-automerge"
            notify "info" "worker-supervisor: finalized PENDING AUTO-MERGE issue $issue" \
              "PR $pr is OPEN with auto-merge armed — not counted yet; it will land automatically on green"
            log "iteration $ITER finalized-pending-automerge (PR $pr OPEN, auto-merge armed; not counted, breaker neutral)"
            ;;
          mismatch:*)
            # The PR is NOT merged (OPEN / CLOSED-unmerged / UNRESOLVED forged ref)
            # — the worker claimed a finalize it did not actually land. Judged
            # ABNORMAL: uncounted, counts toward the breaker, and a HIGH page names
            # the discrepancy. gh clearly RESOLVED (or the ref was forged), so a
            # verification-outage streak is NOT in progress — reset that counter.
            CONSECUTIVE_UNVERIFIED=0
            local mstate="${verify#mismatch:}"
            journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc" "" "$verify"
            notify "high" "worker-supervisor: finalized MISMATCH issue $issue" \
              "worker claimed finalized but PR $pr is $mstate (not MERGED) — not counted, breaker +1"
            log "iteration $ITER abnormal (finalized MISMATCH: PR $pr is $mstate, not MERGED)"
            trip_breaker_or_continue
            ;;
          *)
            # unverified — strictly a gh exec/transport failure. FAIL-INFORMATIVE:
            # log + default-priority page, do NOT count as done, and stay NEUTRAL
            # to the breaker (do not increment — a transient outage is not a crash;
            # do not reset — it must not mask a real prior crash chain). But a
            # PERSISTENT outage is an operator problem (roborev 1810 MED): after
            # UNVERIFIED_MAX consecutive unverified finalizes the supervisor STOPS
            # loudly, so the MAX_ISSUES ceiling can't drift on uncounted-forever runs.
            CONSECUTIVE_UNVERIFIED=$((CONSECUTIVE_UNVERIFIED + 1))
            journal_line "$ITER" "finalized-unverified" "$issue" "$pr" "$duration" "$rc" "" "unverified"
            notify "info" "worker-supervisor: finalized UNVERIFIED issue $issue" \
              "could not verify PR $pr merged (gh unavailable/network) — not counted, breaker unchanged"
            log "iteration $ITER finalized-unverified (gh could not confirm PR $pr; not counted, breaker neutral)"
            if [[ "$CONSECUTIVE_UNVERIFIED" -ge "$UNVERIFIED_MAX" ]]; then
              notify "high" "worker-supervisor: verification unavailable" \
                "$CONSECUTIVE_UNVERIFIED consecutive finalized markers could not be gh-verified — persistent verification outage; stopping so the MAX_ISSUES ceiling stays meaningful"
              log "iteration $ITER: $CONSECUTIVE_UNVERIFIED consecutive unverified (>= UNVERIFIED_MAX=$UNVERIFIED_MAX); stopping (verify-unavailable)"
              finalize_exit "verify-unavailable" 1
            fi
            ;;
        esac
      fi
      ;;
    no-work)
      CONSECUTIVE_ABNORMAL=0
      journal_line "$ITER" "no-work" "$issue" "$pr" "$duration" "$rc"
      log "no work available; backing off ${BACKOFF_NOWORK_SECS}s"
      sleep "$BACKOFF_NOWORK_SECS"
      ;;
    blocked)
      CONSECUTIVE_ABNORMAL=0
      case "$reason" in
        seam1-approval | needs-decision)
          # CLEAN PARK (issue #2666, park-and-resume). The worker hit Seam 1 (an
          # unapproved design spec) or a genuine mid-run owner decision: it posted
          # ONE structured question comment, added the `needs-decision` label, and
          # released the machine. Judged NORMAL — never abnormal, never toward the
          # breaker — and it does NOT head-block the queue: the labeled issue is
          # excluded from the worker's next pickup until the owner answers and the
          # label clears, so the loop moves straight to the next Ready issue. Fire
          # ONE high-priority page whose title carries the issue # and the first
          # line of the question (the marker's optional "question" field).
          journal_line "$ITER" "parked-on-owner" "$issue" "$pr" "$duration" "$rc" "$reason"
          if [[ -n "$issue" && "$issue" == "$LAST_PARKED_ISSUE" ]]; then
            # Head-block-on-decision guard (mirrors the F2 blocked-path guard,
            # roborev 1769): the SAME issue parked on two consecutive iterations
            # means the worker keeps re-parking it — typically because the
            # `needs-decision` label never applied, so the pickup exclusion is not
            # holding and the loop would burn to MAX_ISSUES re-asking one question.
            # Page the owner and STOP cleanly instead of looping.
            notify "high" "worker-supervisor: issue $issue head-blocked on decision" "issue #$issue parked twice in a row (needs-decision) — queue head-blocked on an owner decision, needs owner"
            log "issue $issue parked on two consecutive iterations; head-blocked on decision, stopping"
            finalize_exit "head-blocked-decision" 0
          fi
          LAST_PARKED_ISSUE="$issue"
          local qline="${question:-$reason}"
          notify "high" "worker-supervisor: parked issue $issue — ${qline}" \
            "issue #$issue parked awaiting owner (${reason}). Answer the needs-decision question comment on the issue; the worker resumes on a newer owner reply."
          log "issue $issue parked-on-owner ($reason); moving to next Ready issue"
          ;;
        *)
          journal_line "$ITER" "blocked" "$issue" "$pr" "$duration" "$rc" "$reason"
          if [[ -n "$issue" && "$issue" == "$LAST_BLOCKED_ISSUE" ]]; then
            # F2: the SAME issue blocked on two consecutive iterations means the
            # queue is head-blocked — looping would just reset the breaker every
            # time and burn wall-clock budget until MAX_HOURS with no progress.
            # Stop cleanly and page the owner instead.
            notify "high" "worker-supervisor: issue $issue persistently blocked" "issue #$issue persistently blocked — queue is head-blocked, needs owner"
            log "issue $issue blocked on two consecutive iterations; queue is head-blocked, stopping"
            finalize_exit "head-blocked" 0
          fi
          LAST_BLOCKED_ISSUE="$issue"
          notify "info" "worker-supervisor: blocked on issue $issue" "${reason:-no reason given}"
          log "remembered blocked issue $LAST_BLOCKED_ISSUE (not auto-retried this run)"
          ;;
      esac
      ;;
    *)
      journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
      log "iteration $ITER abnormal (unrecognized marker outcome '$outcome')"
      trip_breaker_or_continue
      ;;
  esac
}

trip_breaker_or_continue() {
  CONSECUTIVE_ABNORMAL=$((CONSECUTIVE_ABNORMAL + 1))
  if [[ "$CONSECUTIVE_ABNORMAL" -ge "$BREAKER_N" ]]; then
    notify "high" "worker-supervisor BREAKER" "$BREAKER_N consecutive abnormal iterations — stopping, no hot respawn"
    finalize_exit "breaker" 1
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  acquire_lock
  log "started: MAX_ISSUES=$MAX_ISSUES MAX_HOURS=$MAX_HOURS LOAD_MAX=$LOAD_MAX DISK_FLOOR_GB=$DISK_FLOOR_GB BREAKER_N=$BREAKER_N"
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    [[ "$ISSUES_DONE" -ge "$MAX_ISSUES" ]] && finalize_exit "budget-issues" 0

    preflight_wait

    run_iteration
  done
}

# Run the loop only when executed directly; when sourced (e.g. by the tooling
# tests to exercise detect_prompt_signature/captured_question in isolation) the
# functions are defined but the machine-guarding loop never starts.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
