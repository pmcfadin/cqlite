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
#               Bounded holds: a leftover-process preflight hold that never clears
#               STOPS the loop loudly — naming the surviving PIDs — and every hold pass
#               re-checks the stop-file and the wall-clock budget (issue #2670, roborev
#               1810): a hold can never latch the supervisor silently. The two leftover
#               families are bounded SEPARATELY (roborev 1839): the non-self-clearing
#               worker-CLI orphan trips the TIGHT LEFTOVER_HOLD_MAX; the self-clearing
#               build/gate family gets the LOOSE BUILD_HOLD_MAX so a legitimate
#               concurrent full gate is waited out, never mistaken for a stuck orphan.
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
# MAX_HOURS_SECS is DERIVED from MAX_HOURS in validate_numeric_knobs (the first thing
# main() does, before any preflight_wait path reads it — roborev 1819) rather than here,
# so the `$((MAX_HOURS * 3600))` integer arithmetic runs only AFTER MAX_HOURS is confirmed
# a valid integer (roborev 1842): a malformed MAX_HOURS pages a FATAL instead of silently
# deriving 0 (which would exit budget-wallclock rc=0) or crashing under `set -e`. An
# explicit MAX_HOURS_SECS override is honored (and integer-validated) there too.
MAX_HOURS_SECS="${MAX_HOURS_SECS:-}"
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
# issue #2670: bound the ways verdict-integrity can otherwise latch the loop. The
# two leftover-process families are bounded SEPARATELY (roborev 1839 HIGH) because
# they behave differently:
# LEFTOVER_HOLD_MAX — consecutive preflight holds on the NON-self-clearing worker
#   family (`leftover-worker`: an orphaned `claude --agent worker` CLI from a prior
#   iteration that never dies) before the supervisor STOPS loudly. This must be a
#   TIGHT bound — a wedged worker CLI is a stuck-on-question hazard, not something to
#   wait out. Default 3 holds (3 x HOLD_POLL_SECS = 15 min at defaults).
# BUILD_HOLD_MAX — consecutive preflight holds on the SELF-CLEARING build/gate
#   family (`leftover-build`: cargo/nextest/gate_slot_daemon) before stopping. This
#   is a LOOSE bound: a legitimate concurrent full gate runs 15-25 min (per CLAUDE.md),
#   so cap x HOLD_POLL_SECS must comfortably exceed it or the loop would false-stop a
#   healthy box that merely has a gate running. Default 12 holds (= 1 h at 300 s poll).
#   A value <=0 DISABLES this bound (wait indefinitely for the build to clear — still
#   backstopped by the wall-clock budget), same <=0-disables semantics as
#   MISMATCH_GRACE_CAP_SECS.
# UNVERIFIED_MAX — consecutive finalized markers gh could not verify before the
#   supervisor STOPS. A persistent verification outage is an operator problem and
#   would otherwise let the MAX_ISSUES ceiling drift (uncounted-forever iterations).
LEFTOVER_HOLD_MAX="${LEFTOVER_HOLD_MAX:-3}"
BUILD_HOLD_MAX="${BUILD_HOLD_MAX:-12}"
UNVERIFIED_MAX="${UNVERIFIED_MAX:-2}"
# issue #2670 (roborev 1813): before escalating a non-merged PR state to a
# mismatch, re-read gh a few times to absorb read-after-merge lag. Env-tunable so
# the tooling tests set the wait to 0 (nothing sleeps in the suite).
MISMATCH_RETRIES="${MISMATCH_RETRIES:-3}"
MISMATCH_RETRY_WAIT_SECS="${MISMATCH_RETRY_WAIT_SECS:-10}"
# Hard ceiling on TOTAL mismatch-grace wall-clock regardless of retries*wait
# (roborev 1819 MED): even a misconfigured MISMATCH_RETRIES/WAIT can never make a
# single verification block longer than this. EXPLICIT semantics (roborev 1821):
# a value >0 is the wall-clock ceiling; a value <=0 DISABLES the wall-clock cap
# entirely, leaving grace bounded solely by the retry count (MISMATCH_RETRIES).
MISMATCH_GRACE_CAP_SECS="${MISMATCH_GRACE_CAP_SECS:-120}"
# issue #2670 (roborev 1819/1839): auto-merge-stuck detection is PER-PR. Under the
# documented `gh pr merge --auto` closer path, a finalize that verifies as OPEN+armed
# is the NORMAL healthy state, and a fast fleet legitimately has several DISTINCT PRs
# pending at once — so a distinct armed PR is NOT evidence that the same PR is stuck.
# The supervisor re-verifies each still-pending PR on later iterations, retroactively
# CREDITS ones that reach MERGED toward MAX_ISSUES, and only STOPS loudly when the
# SAME PR is observed still-unmerged across this many consecutive iterations (a PR
# whose auto-merge genuinely never lands).
PENDING_AUTOMERGE_MAX="${PENDING_AUTOMERGE_MAX:-3}"
# PENDING_AUTOMERGE_MIN_SECS (roborev 1840): the observation count alone is not enough
# to declare a PR stuck — a burst of fast no-progress iterations (e.g. several quick
# owner-decision parks, which have no backoff) could rack up PENDING_AUTOMERGE_MAX
# observations of a PR whose CI simply hasn't finished yet. So automerge-stuck ALSO
# requires the PR to have been pending at least this long (wall-clock, comfortably above
# CI time). Default 20 min.
PENDING_AUTOMERGE_MIN_SECS="${PENDING_AUTOMERGE_MIN_SECS:-1200}"

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
# Single source of the leftover-process match patterns (roborev 1821): BOTH the
# per-family count probes (PROC_PROBE_WORKER/BUILD_CMD) and list probes
# (PROC_LIST_WORKER/BUILD_CMD) DERIVE from these, so the "what counts as a leftover" set
# and the "what we name in the page" set can never drift apart. `[c]argo`/`[c]laude` are
# the bracket-trick forms (see below).
PROC_MATCH_BUILD='[c]argo |[n]extest|[g]ate_slot_daemon'
PROC_MATCH_WORKER='[c]laude.*--agent worker'
# Leftover-process probes (issue #2670): two families of prior-iteration debris block
# the next spawn (HOLD-and-poll, same as load/disk), bounded SEPARATELY (roborev 1839)
# so each needs its OWN count probe:
#   (1) build/gate processes — cargo/nextest/gate_slot_daemon — SELF-CLEARING; a
#       legitimate concurrent full gate is one of these, so it gets the LOOSE
#       BUILD_HOLD_MAX (reason `leftover-build`).
#   (2) an orphaned worker Claude CLI from a prior iteration (a SIGTERM'd-wrapper
#       survivor and stuck-on-question hazard) — NON-self-clearing; gets the TIGHT
#       LEFTOVER_HOLD_MAX (reason `leftover-worker`).
# The Claude match is keyed on the supervisor's OWN spawn shape — `--agent worker`
# (see WORKER_CMD) — NOT a bare `claude`. A plain interactive `claude` REPL, or a
# different-agent session (`--agent flow-lead`), never carries that marker, so a
# legitimate interactive session on the box is not matched. LIMIT: an operator who
# deliberately runs `claude --agent worker` by hand WILL be matched (correctly — by the
# one-worker-per-machine rule #1930). The current iteration's own worker has already
# exited before preflight runs, so any `--agent worker` seen here is from a prior iteration.
#
# SELF-MATCH DEFUSED (roborev 1813, MED-HIGH): a live `bash -c` wrapper's OWN argv
# contains these pattern strings, so a naive pattern would match the wrapper and report
# a phantom leftover at EVERY boot. The classic pgrep bracket trick (`[c]argo`,
# `[c]laude`) makes each regex still match the real process (`cargo`, `claude`) while
# the bracketed pattern TEXT in the wrapper's argv (`[c]argo`) no longer contains the
# literal substring, so the wrapper cannot match itself.
#
# SELF-EXCLUSION (roborev 1819, LOW): additionally drop the supervisor's OWN pid ($$)
# and parent ($PPID) — expanded HERE at definition time so they are the supervisor's,
# not the probe subshell's. RESIDUAL LIMIT: the match is still an argv substring test,
# so a `claude --agent worker` process ELSEWHERE in the ancestry, or an unrelated
# process carrying the literal substring, could still be counted.
if [[ -z "${PROC_PROBE_WORKER_CMD:-}" ]]; then
  PROC_PROBE_WORKER_CMD="pgrep -f '$PROC_MATCH_WORKER' 2>/dev/null | grep -vxF -e '$$' -e '$PPID' | wc -l | tr -d ' '"
fi
if [[ -z "${PROC_PROBE_BUILD_CMD:-}" ]]; then
  PROC_PROBE_BUILD_CMD="pgrep -f '$PROC_MATCH_BUILD' 2>/dev/null | grep -vxF -e '$$' -e '$PPID' | wc -l | tr -d ' '"
fi
# Companion PER-FAMILY list probes (roborev 1839/1821): a leftover-worker / leftover-build
# STOP names ONLY its OWN family's surviving PIDs (never unrelated PIDs from the other
# family). DERIVED from the SAME $PROC_MATCH_* patterns as the count probes, list-from-
# count-set so they can't drift, with the same $$/$PPID self-exclusion. Best-effort — an
# empty result yields "<unavailable>" in the page.
if [[ -z "${PROC_LIST_WORKER_CMD:-}" ]]; then
  PROC_LIST_WORKER_CMD="pgrep -lf '$PROC_MATCH_WORKER' 2>/dev/null | grep -vE '^($$|$PPID) '"
fi
if [[ -z "${PROC_LIST_BUILD_CMD:-}" ]]; then
  PROC_LIST_BUILD_CMD="pgrep -lf '$PROC_MATCH_BUILD' 2>/dev/null | grep -vE '^($$|$PPID) '"
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

  # Total mismatch-grace wall-clock ceiling. The natural budget is retries*wait;
  # MISMATCH_GRACE_CAP_SECS>0 tightens it to at most the cap, while a cap <=0
  # DISABLES the ceiling (grace then bounded solely by the retry count — roborev
  # 1821 explicit semantics). The guard below only binds when grace_budget>0, so a
  # disabled cap / a wait=0 config leaves retries count-bounded, never blocked.
  local grace_start grace_budget
  grace_start=$(date +%s)
  grace_budget=$((MISMATCH_RETRIES * MISMATCH_RETRY_WAIT_SECS))
  if [[ "$MISMATCH_GRACE_CAP_SECS" -gt 0 && "$grace_budget" -gt "$MISMATCH_GRACE_CAP_SECS" ]]; then
    grace_budget="$MISMATCH_GRACE_CAP_SECS"
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
    # gh RAN and could not RESOLVE the ref — a forged reference. Finding 2 (roborev
    # 1819): match gh's ACTUAL resolve-failure signature ONLY, anchored and
    # case-tolerant on the leading letter — `Could not resolve to a PullRequest|Issue`
    # and the `no pull requests found` form. Everything else nonzero is a transport
    # error (`dial tcp ... no such host`, DNS/proxy 404) → unverified, NEVER forgery.
    # The bare `not found` substring is deliberately GONE.
    if printf '%s' "$err" | grep -qE '[Cc]ould not resolve to a (PullRequest|Issue)|[Nn]o pull requests found'; then
      printf 'mismatch:UNRESOLVED'
      return 0
    fi

    # Parse. parser_ok = a parser is present AND the payload parsed as JSON.
    # Finding 2: a tooling gap (no parser / parse error) is transport class
    # (unverified), NEVER forgery. mismatch:UNRESOLVED is reserved for a
    # SUCCESSFULLY-parsed response that simply lacks the PR state. Finding 3 (roborev
    # 1819): jq is tried first, but a jq PARSE FAILURE falls THROUGH to python3 before
    # giving up (jq present but broken must not defeat a valid response).
    parser_ok=0
    state=""
    automerge=""
    if command -v jq >/dev/null 2>&1 && printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
      parser_ok=1
      state="$(printf '%s' "$out" | jq -r '.state // empty' 2>/dev/null || true)"
      # Normalize the auto-merge sentinel (roborev 1821): both parsers emit the
      # SAME token — "armed" when autoMergeRequest is present, "" otherwise — so the
      # downstream `-n "$automerge"` test can never diverge between jq and python3
      # (jq's raw object dump vs python's boolean string).
      automerge="$(printf '%s' "$out" | jq -r 'if .autoMergeRequest then "armed" else "" end' 2>/dev/null || true)"
    fi
    if [[ "$parser_ok" -eq 0 ]] && command -v python3 >/dev/null 2>&1; then
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
    # The grace loop is BOUNDED (roborev 1819): the retry COUNT (MISMATCH_RETRIES) is
    # the primary bound; it ALSO stops if the stop-file appears (a requested shutdown
    # must not wait out the grace) or the total grace wall-clock budget is spent. The
    # wall-clock guard only binds when there is an actual per-retry wait — with
    # wait=0 the budget is 0, so instant retries stay count-bounded (not blocked).
    attempt=$((attempt + 1))
    if [[ "$attempt" -lt "$MISMATCH_RETRIES" ]]; then
      # Retries remain, but the grace loop is being CUT SHORT rather than by observing
      # a stable non-merged state MISMATCH_RETRIES times. The PR state was never allowed
      # to settle, so reporting it as a forged finalize (abnormal + HIGH page + breaker)
      # would falsely accuse a worker whose endgame was merely interrupted (roborev 1837).
      #  - stop-file present  → `aborted`: a NEUTRAL non-verdict (the supervisor is
      #    shutting down; it is neither a forged mismatch NOR a verification outage, so it
      #    must not accumulate the unverified-outage streak and trip verify-unavailable
      #    during an ordinary shutdown).
      #  - wall-clock budget spent (no shutdown) → `unverified` (transport-class): we
      #    genuinely could not confirm the state in the allotted grace.
      if [[ -f "$STOP_FILE" ]]; then
        printf 'aborted'
        return 0
      fi
      if [[ "$grace_budget" -gt 0 ]] && [[ $(( $(date +%s) - grace_start )) -ge "$grace_budget" ]]; then
        printf 'unverified'
        return 0
      fi
      sleep "$MISMATCH_RETRY_WAIT_SECS"
      continue
    fi
    # Retries exhausted. Re-check the stop-file here too: with MISMATCH_RETRIES=1 the
    # loop never reaches the mid-loop guard above, and even with more retries a shutdown
    # can arrive during this FINAL read — either way a requested shutdown must never be
    # reported as a forged finalize (roborev 1838), so emit the neutral `aborted`.
    if [[ -f "$STOP_FILE" ]]; then
      printf 'aborted'
      return 0
    fi
    # A stable, resolved, non-merged state that outlasted the grace — a genuine mismatch.
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
  # Two leftover families, bounded differently (roborev 1839). The worker orphan is a
  # NON-self-clearing hazard and takes PRIORITY when both are present (its tight bound
  # governs); the build/gate family is self-clearing with a loose bound.
  procs="$(bash -c "$PROC_PROBE_WORKER_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-worker"
    return 0
  fi
  procs="$(bash -c "$PROC_PROBE_BUILD_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-build"
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
# the wall-clock budget, and a leftover hold that never clears stops the loop loudly
# rather than holding until MAX_HOURS with no page beyond the first HOLD. The two
# leftover families are bounded SEPARATELY (roborev 1839): a non-self-clearing worker
# orphan trips the TIGHT LEFTOVER_HOLD_MAX; a self-clearing build/gate process is
# allowed the LOOSE BUILD_HOLD_MAX so a legitimate concurrent full gate is waited out.
preflight_wait() {
  local worker_holds=0 build_holds=0
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    local reason
    reason="$(preflight_reason)"
    if [[ -z "$reason" ]]; then
      LAST_HOLD_REASON=""
      return 0
    fi
    # Count each leftover family's holds CUMULATIVELY across the whole preflight_wait
    # invocation (roborev 1813) — a different hold reason (a transient load/disk blip,
    # or the OTHER leftover family) does NOT reset a family's tally. Otherwise an
    # orphan that never dies but is occasionally masked by a load spike would reset the
    # bound every time and hold forever. The wall-clock budget re-check at the top
    # bounds total hold time regardless of the reason mix.
    if [[ "$reason" == "leftover-worker" ]]; then
      worker_holds=$((worker_holds + 1))
      if [[ "$worker_holds" -ge "$LEFTOVER_HOLD_MAX" ]]; then
        local pids
        pids="$(bash -c "$PROC_LIST_WORKER_CMD" 2>/dev/null | tr '\n' ';' | cut -c1-300)"
        notify "high" "worker-supervisor: leftover worker CLI will not clear" \
          "held $worker_holds times on an orphaned worker CLI that never cleared — stopping loudly. Surviving: ${pids:-<unavailable>}"
        log "leftover-worker held $worker_holds times (>= LEFTOVER_HOLD_MAX=$LEFTOVER_HOLD_MAX) without clearing; stopping. Surviving: ${pids:-<unavailable>}"
        finalize_exit "leftover-worker" 1
      fi
    elif [[ "$reason" == "leftover-build" ]]; then
      # BUILD_HOLD_MAX<=0 DISABLES the build bound (wait indefinitely — still
      # backstopped by the wall-clock budget above).
      build_holds=$((build_holds + 1))
      if [[ "$BUILD_HOLD_MAX" -gt 0 ]] && [[ "$build_holds" -ge "$BUILD_HOLD_MAX" ]]; then
        local pids
        pids="$(bash -c "$PROC_LIST_BUILD_CMD" 2>/dev/null | tr '\n' ';' | cut -c1-300)"
        notify "high" "worker-supervisor: build/gate processes will not clear" \
          "held $build_holds times on build/gate processes that never cleared (well beyond a normal gate) — stopping loudly. Surviving: ${pids:-<unavailable>}"
        log "leftover-build held $build_holds times (>= BUILD_HOLD_MAX=$BUILD_HOLD_MAX) without clearing; stopping. Surviving: ${pids:-<unavailable>}"
        finalize_exit "leftover-build" 1
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
# Auto-merge-stuck is tracked PER-PR (roborev 1839/1840), not as a single across-PR
# streak: PENDING_PR_LIST holds one TAB-separated "pr<TAB>issue<TAB>observations<TAB>
# first-observed-epoch" record per still-pending armed PR. Each iteration re-verifies
# them (credit_merged_pending_prs): a PR that reached MERGED is credited toward
# MAX_ISSUES and dropped; a PR still pending has its observation count bumped and trips
# automerge-stuck only when the SAME PR has been seen unmerged PENDING_AUTOMERGE_MAX
# times AND has been pending at least PENDING_AUTOMERGE_MIN_SECS (a wall-clock floor so
# fast no-progress iterations can't burn the budget before CI could finish).
PENDING_PR_LIST=""
STUCK_SKIP_PR=""
LAST_BLOCKED_ISSUE=""
LAST_PARKED_ISSUE=""
START_TS=$(date +%s)
# MAX_HOURS_SECS is derived in validate_numeric_knobs (roborev 1842) — the FIRST
# statement of main(), before acquire_lock / preflight_wait / the main loop read it — so
# the derivation runs only after MAX_HOURS is confirmed a valid integer (roborev 1819's
# "budget defined before any preflight_wait read" guarantee is preserved). Do NOT move the
# derivation back to the config block: that reintroduces the silent `MAX_HOURS=abc → 0`
# budget hazard #1842 closed.

# report_pending_at_exit: a run that stops while armed PRs are still pending must not
# drop them silently (roborev 1841) — the PENDING_AUTOMERGE_MIN_SECS floor means a
# recently-armed PR can legitimately never have tripped automerge-stuck in this run. On
# exit, journal one `pending-at-exit` line per still-tracked PR (with its age) and fire a
# single summary notify, so an operator can follow up on any PR that hadn't landed yet.
report_pending_at_exit() {
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  local pr issue count first_ts age n=0 summary=""
  while IFS=$'\t' read -r pr issue count first_ts; do
    [[ -z "$pr" ]] && continue
    # A currently-tripping stuck PR already got its own HIGH "auto-merge stuck" page —
    # don't ALSO announce it here as a generic still-pending PR (roborev 1842/1843).
    # STUCK_SKIP_PR is a newline-delimited list (all stuck PRs this pass).
    case $'\n'"$STUCK_SKIP_PR" in *$'\n'"$pr"$'\n'*) continue ;; esac
    age=$(( $(date +%s) - first_ts ))
    journal_line "$ITER" "pending-at-exit" "$issue" "$pr" 0 0 "pending_age_s=$age"
    n=$((n + 1))
    summary="${summary}${pr} (issue $issue, ${age}s); "
  done <<< "$PENDING_PR_LIST"
  [[ "$n" -eq 0 ]] && return 0
  notify "info" "worker-supervisor: $n armed PR(s) still pending at exit" \
    "these PRs were OPEN with auto-merge armed and had not yet landed when the run stopped — check they merge: ${summary}"
}

finalize_exit() {
  local reason="$1" code="$2"
  local elapsed=$(($(date +%s) - START_TS))
  mkdir -p "$LOG_DIR"
  report_pending_at_exit
  printf '{"ts":"%s","iter":%d,"outcome":"summary","reason":"%s","issues_done":%d,"elapsed_s":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ITER" "$reason" "$ISSUES_DONE" "$elapsed" \
    >>"${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local prio="info"
  [[ "$reason" == "breaker" ]] && prio="high"
  notify "$prio" "worker-supervisor stopped" "reason=$reason issues_done=$ISSUES_DONE elapsed_s=$elapsed"
  exit "$code"
}

# ---------------------------------------------------------------------------
# Per-PR auto-merge-stuck tracking (roborev 1839)
# ---------------------------------------------------------------------------
# remember_pending_pr <pr> <issue>: record an OPEN+armed PR for later re-verification,
# de-duped by PR. New PRs start at observation count 1 (this finalize is observation 1).
remember_pending_pr() {
  local pr="$1" issue="$2" line found=""
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if [[ "${line%%$'\t'*}" == "$pr" ]]; then found=1; fi
  done <<< "$PENDING_PR_LIST"
  [[ -n "$found" ]] && return 0
  # Record: pr <TAB> issue <TAB> observation-count <TAB> first-observed-epoch.
  PENDING_PR_LIST="${PENDING_PR_LIST}${pr}"$'\t'"${issue}"$'\t'"1"$'\t'"$(date +%s)"$'\n'
}

# forget_pending_pr <pr>: drop a PR from the retroactive-credit set once its OWN
# finalized-marker iteration authoritatively resolves it (merged or mismatch), so it
# can never be re-verified/credited a second time by credit_merged_pending_prs.
forget_pending_pr() {
  local pr="$1" line new=""
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "${line%%$'\t'*}" == "$pr" ]] && continue
    new="${new}${line}"$'\n'
  done <<< "$PENDING_PR_LIST"
  PENDING_PR_LIST="$new"
}

# credit_merged_pending_prs: re-verify every still-pending armed PR. One that reached
# MERGED is CREDITED toward MAX_ISSUES (retroactive finalize credit) and dropped. One
# still pending has its observation count bumped and, if the SAME PR has now been seen
# unmerged PENDING_AUTOMERGE_MAX times, trips automerge-stuck (STOP loudly). One that is
# neither merged nor pending (closed/forged/gone) is dropped uncredited; an unverified/
# aborted transient is kept for a later retry. Called once per main-loop iteration.
credit_merged_pending_prs() {
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  local new_list="" pr issue count first_ts verify age mstate any_stuck=""
  STUCK_SKIP_PR=""
  while IFS=$'\t' read -r pr issue count first_ts; do
    [[ -z "$pr" ]] && continue
    verify="$(verify_finalized_pr "$pr")"
    case "$verify" in
      merged)
        ISSUES_DONE=$((ISSUES_DONE + 1))
        journal_line "$ITER" "pending-credited" "$issue" "$pr" 0 0 "" "merged"
        notify "info" "worker-supervisor: pending PR landed" \
          "PR $pr (issue $issue) reached MERGED — retroactively credited toward MAX_ISSUES (issues_done=$ISSUES_DONE)"
        log "retroactive credit: PR $pr merged (issue $issue); ISSUES_DONE=$ISSUES_DONE"
        ;;
      pending-automerge)
        count=$((count + 1))
        age=$(( $(date +%s) - first_ts ))
        new_list="${new_list}${pr}"$'\t'"${issue}"$'\t'"${count}"$'\t'"${first_ts}"$'\n'
        # Auto-merge-stuck needs BOTH enough observations AND enough wall-clock (roborev
        # 1840): the time floor stops a burst of fast no-progress iterations (parks with
        # no backoff) from burning the observation budget before CI could plausibly finish.
        # Page/log/journal each stuck PR HERE at detection (no delay — roborev 1843), and
        # record it so report_pending_at_exit skips ALL stuck PRs (not just the first) and
        # the ACTUAL stop is deferred until after the list is rebuilt (roborev 1842).
        if [[ "$count" -ge "$PENDING_AUTOMERGE_MAX" ]] && [[ "$age" -ge "$PENDING_AUTOMERGE_MIN_SECS" ]]; then
          notify "high" "worker-supervisor: auto-merge stuck" \
            "PR $pr (issue $issue) has stayed OPEN pending auto-merge across $count observations over ${age}s — auto-merge is not landing; stopping so it isn't looped forever"
          log "PR $pr observed still-pending $count times over ${age}s (>= PENDING_AUTOMERGE_MAX=$PENDING_AUTOMERGE_MAX, >= PENDING_AUTOMERGE_MIN_SECS=$PENDING_AUTOMERGE_MIN_SECS); stopping (automerge-stuck)"
          journal_line "$ITER" "finalized-pending-automerge" "$issue" "$pr" 0 0 "pending_age_s=$age" "pending-automerge"
          any_stuck=1
          STUCK_SKIP_PR="${STUCK_SKIP_PR}${pr}"$'\n'
        fi
        ;;
      mismatch:*)
        # A tracked armed PR that ended NOT-merged (CLOSED-unmerged, auto-merge dropped,
        # or the ref forged/removed) is the very failure this feature exists to catch —
        # it must NOT be swallowed silently (roborev 1840). Page HIGH naming the PR/issue/
        # state and journal it abnormal-shaped, then drop it (the crash breaker is not
        # touched: the PR was legitimately armed earlier, so this is an escalation to the
        # owner, not a worker-crash signal).
        mstate="${verify#mismatch:}"
        journal_line "$ITER" "pending-dropped" "$issue" "$pr" 0 0 "" "$verify"
        notify "high" "worker-supervisor: armed PR did not land" \
          "PR $pr (issue $issue) was OPEN+armed but is now $mstate (not MERGED) — auto-merge did not land; needs owner attention"
        log "pending PR $pr resolved $verify (issue $issue); dropped from retroactive-credit set, paged high"
        ;;
      *)
        # unverified / aborted — a transient gh/transport gap or a shutdown; keep the PR
        # (count + first_ts unchanged) for a later retry rather than crediting or dropping.
        new_list="${new_list}${pr}"$'\t'"${issue}"$'\t'"${count}"$'\t'"${first_ts}"$'\n'
        ;;
    esac
  done <<< "$PENDING_PR_LIST"
  PENDING_PR_LIST="$new_list"
  # PENDING_PR_LIST now reflects this pass (resolved PRs removed, stuck ones kept but in
  # STUCK_SKIP_PR). Stop AFTER the rebuild so report_pending_at_exit reads a fresh list and
  # skips every already-paged stuck PR (roborev 1842/1843). Use an explicit `if` (not a
  # `&&` tail) so the function returns 0 under `set -e` when nothing is stuck.
  if [[ -n "$any_stuck" ]]; then
    finalize_exit "automerge-stuck" 1
  fi
  return 0
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

  # The verification-outage streak (CONSECUTIVE_UNVERIFIED) measures CONSECUTIVE gh
  # verification FAILURES with no gh SUCCESS in between (roborev 1840): it is reset ONLY
  # by an outcome where gh actually RESOLVED the PR (merged / pending-automerge /
  # mismatch:* below), NOT by an intervening no-work/abnormal/parked iteration that
  # never exercised gh — otherwise a persistent gh outage interleaved with unrelated
  # outcomes (a common shape, since gh and worker failures share causes) would never
  # reach UNVERIFIED_MAX, restoring the MAX_ISSUES-drift hazard #2670 closed.

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
            forget_pending_pr "$pr"
            ISSUES_DONE=$((ISSUES_DONE + 1))
            journal_line "$ITER" "finalized" "$issue" "$pr" "$duration" "$rc" "" "merged"
            notify "info" "worker-supervisor: finalized issue $issue" "pr=$pr duration_s=$duration verified=merged"
            ;;
          pending-automerge)
            # OPEN with auto-merge armed (roborev 1813): the closer's DOCUMENTED
            # auto-merge path — the PR WILL land on green. NOT counted toward
            # MAX_ISSUES yet (it isn't merged), a default-priority page, and
            # breaker-NEUTRAL (gh resolved fine — do NOT touch the crash breaker).
            # A DISTINCT armed PR is a healthy fresh finalize, NOT evidence any PR is
            # stuck (roborev 1839): record it for per-PR re-verification. It is credited
            # toward MAX_ISSUES retroactively once it reaches MERGED (credit_merged_
            # pending_prs), and only trips automerge-stuck when the SAME PR is seen
            # unmerged PENDING_AUTOMERGE_MAX times — never on N distinct healthy PRs.
            CONSECUTIVE_UNVERIFIED=0
            remember_pending_pr "$pr" "$issue"
            journal_line "$ITER" "finalized-pending-automerge" "$issue" "$pr" "$duration" "$rc" "" "pending-automerge"
            notify "info" "worker-supervisor: finalized PENDING AUTO-MERGE issue $issue" \
              "PR $pr is OPEN with auto-merge armed — not counted yet; it will land automatically on green"
            log "iteration $ITER finalized-pending-automerge (PR $pr OPEN, auto-merge armed; tracked for retroactive credit, breaker neutral)"
            ;;
          aborted)
            # The finalize verification was cut short by a requested shutdown mid-grace
            # (roborev 1837): the stop-file appeared before the PR state could settle.
            # This is NOT a verdict about the worker — it is neutral. Journal it, do NOT
            # count it toward MAX_ISSUES, do NOT increment the unverified-outage streak
            # (so an ordinary shutdown can't false-trip verify-unavailable), do NOT page,
            # and leave the breaker untouched. The main loop exits on the stop-file at its
            # next top.
            journal_line "$ITER" "finalized-aborted" "$issue" "$pr" "$duration" "$rc" "" "aborted"
            log "iteration $ITER finalized-aborted (verify interrupted by shutdown; neutral, not counted)"
            ;;
          mismatch:*)
            # The PR is NOT merged (OPEN / CLOSED-unmerged / UNRESOLVED forged ref)
            # — the worker claimed a finalize it did not actually land. Judged
            # ABNORMAL: uncounted, counts toward the breaker, and a HIGH page names
            # the discrepancy. gh clearly RESOLVED (or the ref was forged), so the
            # verification-outage streak is broken — reset it (a gh-SUCCESS outcome).
            CONSECUTIVE_UNVERIFIED=0
            forget_pending_pr "$pr"
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
            # UNVERIFIED_MAX consecutive unverified finalizes (with no gh-SUCCESS
            # resetting the streak in between) the supervisor STOPS loudly, so the
            # MAX_ISSUES ceiling can't drift on uncounted-forever runs.
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
# validate_numeric_knobs: every env-overridable numeric bound is used in arithmetic /
# `-gt`/`-ge` comparisons; a malformed value (an operator typo like `12h` in a plist)
# would otherwise evaluate to 0 and SILENTLY disable/misbehave a bound — the opposite of
# fail-closed (roborev 1840). Validate each against `^-?[0-9]+$` at startup and STOP loudly
# on a bad value rather than running with a silently-broken safety bound.
_bad_knob() {
  log "FATAL: numeric knob $1 has value '$2' — expected $3; refusing to start with a silently-broken bound"
  notify "high" "worker-supervisor: bad config" "$1='$2' is not $3; supervisor refused to start (fix the env/plist)"
  exit 2
}

validate_numeric_knobs() {
  local name val
  # FLOAT-tolerant knobs — compared via awk is_gt/is_lt (which coerce with `+0`). A
  # fractional value (e.g. DISK_FLOOR_GB=37.5, `.5`) is valid; a NON-numeric OR NEGATIVE
  # value would coerce/compare so the bound silently DISABLES (fail-open), so require a
  # NON-NEGATIVE decimal (roborev 1843). Accepts `40`, `40.5`, `.5`, `40.`.
  for name in LOAD_MAX DISK_FLOOR_GB; do
    val="${!name}"
    [[ "$val" =~ ^([0-9]+\.?[0-9]*|\.[0-9]+)$ ]] || _bad_knob "$name" "$val" "a non-negative number"
  done
  # NON-NEGATIVE integer knobs — counts/seconds where a negative is meaningless and would
  # silently break the bound (e.g. MAX_ISSUES=-1 ⇒ instant budget-issues rc=0, roborev
  # 1843). MAX_HOURS is here because its `$((MAX_HOURS * 3600))` derivation is integer
  # arithmetic (a bare-word MAX_HOURS would coerce to 0 → a broken wall-clock budget).
  for name in MAX_HOURS MAX_ISSUES BREAKER_N BACKOFF_NOWORK_SECS HOLD_POLL_SECS \
              MAX_ITER_SECS STUCK_POLL_SECS STUCK_TAIL_LINES LEFTOVER_HOLD_MAX \
              UNVERIFIED_MAX MISMATCH_RETRIES MISMATCH_RETRY_WAIT_SECS \
              PENDING_AUTOMERGE_MAX PENDING_AUTOMERGE_MIN_SECS; do
    val="${!name}"
    [[ "$val" =~ ^[0-9]+$ ]] || _bad_knob "$name" "$val" "a non-negative integer"
  done
  # SIGNED integer knobs — the two with a documented `<=0 disables` contract.
  for name in BUILD_HOLD_MAX MISMATCH_GRACE_CAP_SECS; do
    val="${!name}"
    [[ "$val" =~ ^-?[0-9]+$ ]] || _bad_knob "$name" "$val" "an integer"
  done
  # Derive the wall-clock budget now that MAX_HOURS is a confirmed non-negative integer;
  # honor an explicit MAX_HOURS_SECS override (also validated).
  if [[ -z "$MAX_HOURS_SECS" ]]; then
    MAX_HOURS_SECS=$(( MAX_HOURS * 3600 ))
  else
    [[ "$MAX_HOURS_SECS" =~ ^[0-9]+$ ]] || _bad_knob MAX_HOURS_SECS "$MAX_HOURS_SECS" "a non-negative integer"
  fi
}

main() {
  validate_numeric_knobs
  acquire_lock
  log "started: MAX_ISSUES=$MAX_ISSUES MAX_HOURS=$MAX_HOURS LOAD_MAX=$LOAD_MAX DISK_FLOOR_GB=$DISK_FLOOR_GB BREAKER_N=$BREAKER_N"
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    # Re-verify previously-pending armed PRs FIRST (roborev 1839): credit any that have
    # since reached MERGED toward MAX_ISSUES, and trip automerge-stuck if the SAME PR is
    # stuck — done before the ceiling check so a retroactive credit can satisfy it.
    credit_merged_pending_prs
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
