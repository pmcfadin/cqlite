#!/usr/bin/env bash
# scripts/tests/test_worker_supervisor.sh — fast, self-contained tests for
# scripts/local/worker-supervisor.sh (issue #2090). No cargo, no gate, no
# network: every external probe/worker/notify is a stub script written to a
# per-test mktemp dir. Target: <30s total.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
SUPERVISOR="$REPO_ROOT/scripts/local/worker-supervisor.sh"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
}
# skip: an ENVIRONMENTAL non-result (e.g. a live control process that never
# scheduled within the wait cap) — explicitly reported, never counted as failure.
skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  echo "SKIP: $1"
}

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cqlite-supervisor-test.XXXXXX")"
cleanup() { rm -rf "$TMP_ROOT" 2>/dev/null || true; }
trap cleanup EXIT

new_case_dir() {
  local d
  d="$(mktemp -d "$TMP_ROOT/case.XXXXXX")"
  mkdir -p "$d/bin" "$d/logs"
  echo "$d"
}

# ---------------------------------------------------------------------------
# Stub writers
# ---------------------------------------------------------------------------
write_notify_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
cat_arg="unknown"
if [[ "${1:-}" == "--category" ]]; then cat_arg="$2"; shift 2; fi
printf '%s|%s|%s\n' "$cat_arg" "${1:-}" "${2:-}" >>"${NOTIFY_LOG:?NOTIFY_LOG not set}"
EOF
  chmod +x "$1"
}

# Always finalizes; issue number = contents of $2, incremented each call.
# Optional $3 = seconds to sleep after writing the marker, before exit.
write_finalize_stub() {
  local path="$1" counter_file="$2" sleep_s="${3:-0}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
sleep "$sleep_s"
EOF
  chmod +x "$path"
}

# Finalize stub that always claims the SAME issue/PR (roborev 1839): used to exercise
# the per-PR auto-merge-stuck path (the same PR observed unmerged N times), distinct
# from write_finalize_stub's incrementing PR (used for the healthy distinct-PR case).
write_fixed_pr_finalize_stub() {
  local path="$1" counter_file="$2" pr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":$pr,"pr":"https://github.com/pmcfadin/cqlite/pull/$pr","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# Always exits 1 without writing a marker (abnormal iteration).
write_abnormal_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$1"
}

# First call: outcome=no-work. Every call after: outcome=finalized (issue
# counter separate from the call counter, so budget-vs-no-work is unambiguous).
write_nowork_then_finalize_stub() {
  local path="$1" call_ctr="$2" issue_ctr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
else
  n=0
  [[ -f "$issue_ctr" ]] && n=\$(cat "$issue_ctr")
  n=\$((n + 1))
  echo "\$n" >"$issue_ctr"
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# F2 regression: writes outcome=blocked with a fixed issue number on every
# call (never finalizes) — used to prove the supervisor stops after the SAME
# issue reports blocked on two consecutive iterations, rather than looping.
write_blocked_same_issue_stub() {
  local path="$1" issue="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"needs owner decision"}
JSON
EOF
  chmod +x "$path"
}

# F5 regression: writes outcome=finalized with issue set but pr MISSING
# entirely (not just null) — the marker contract requires BOTH issue and pr
# on "finalized"; a marker missing either must be judged abnormal.
write_finalize_missing_pr_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":42,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# F3 regression: writes outcome=blocked with a "reason" containing a double
# quote and an embedded literal newline (via printf %b), to prove the journal
# line stays valid JSON end-to-end (marker_field's python3 read handles the
# marker side; journal_line's json_or_null handles the journal-write side).
write_blocked_nasty_reason_stub() {
  local path="$1"
  cat >"$path" <<'PYEOF'
#!/usr/bin/env bash
set -euo pipefail
python3 - "$MARKER_FILE" <<'PY'
import json, sys
d = {"outcome": "blocked", "issue": 55, "pr": None, "duration_s": 1,
     "reason": 'has a "quote" and\na newline'}
open(sys.argv[1], "w").write(json.dumps(d))
PY
PYEOF
  chmod +x "$path"
}

# Fails loudly (sentinel + exit 1) if the marker file is already present at
# start (proves the supervisor removed a stale marker before spawning).
write_stale_check_stub() {
  local path="$1" sentinel="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -f "\$MARKER_FILE" ]]; then
  touch "$sentinel"
  exit 1
fi
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":1,"pr":"https://github.com/pmcfadin/cqlite/pull/1","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 CLEAN PARK: first call writes a `blocked` marker with a park
# reason (seam1-approval | needs-decision) and an optional one-line question;
# every call after finalizes. Used to prove a park is judged parked-on-owner,
# fires a high page, never trips the breaker, and the loop advances.
write_park_then_finalize_stub() {
  local path="$1" call_ctr="$2" reason="$3" question="${4:-}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":77,"pr":null,"duration_s":1,"reason":"$reason","question":"$question"}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":78,"pr":"https://github.com/pmcfadin/cqlite/pull/78","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666: writes a marker with an UNKNOWN outcome value — must be judged
# abnormal (counts toward the breaker), never silently trusted.
write_unknown_outcome_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"weird-outcome","issue":9,"pr":null,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 stuck-on-question: first call prints an interactive-prompt line
# then sleeps past MAX_ITER_SECS (so the watchdog detects + pages and it gets
# timeout-killed WITHOUT a marker); every call after finalizes so the test
# terminates at MAX_ISSUES.
write_stuck_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  echo "AskUserQuestion: Do you want to proceed with option A?"
  sleep 120
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":88,"pr":"https://github.com/pmcfadin/cqlite/pull/88","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: abnormal → stuck → abnormal → abnormal → finalize.
# The stuck iteration (call 2) must RESET the consecutive-abnormal counter so the
# crash chain is broken and BREAKER_N=3 never trips; call 5 finalizes so the run
# terminates at MAX_ISSUES.
write_abnormal_stuck_abnormal_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1|3|4) exit 1 ;;
  2)
    echo "AskUserQuestion: choose an option"
    sleep 120 ;;
  *)
    cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":91,"pr":"https://github.com/pmcfadin/cqlite/pull/91","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks the SAME issue with a park reason on EVERY
# call (never finalizes) — proves the park-path head-block guard stops after the
# same issue parks on two consecutive iterations.
write_park_same_issue_stub() {
  local path="$1" issue="$2" reason="${3:-needs-decision}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"$reason","question":"same question"}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks issue 41, then a DIFFERENT issue 42, then
# finalizes — proves distinct-issue parks do NOT trip the head-block guard.
write_park_two_issues_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":41,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q41"}
JSON
    ;;
  2) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":42,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q42"}
JSON
    ;;
  *) cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":43,"pr":"https://github.com/pmcfadin/cqlite/pull/43","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case a): prints a stray signature line, then keeps
# WRITING many lines (log grows + signature scrolls out of the tail) and exits 1
# WITHOUT a marker. No wedge evidence → must stay ABNORMAL (counts to breaker),
# never misclassified as stuck.
write_crash_stray_signature_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "AskUserQuestion: stray tool-name printed in normal trace"
for i in $(seq 1 60); do
  echo "working on step $i ..."
  sleep 0.1
done
exit 1
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case c): a BUSY worker that prints the signature on
# EVERY line (so it is always in the tail) but keeps WRITING (log grows every
# scan) — the no-growth evidence fails, so it must NOT be classified stuck. Call 1
# runs until killed at the deadline (abnormal); call 2 finalizes to terminate.
write_busy_signature_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  while true; do
    echo "AskUserQuestion tick — still working, writing more output"
    sleep 0.3
  done
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":95,"pr":"https://github.com/pmcfadin/cqlite/pull/95","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670: a gh-verify stub that FAILS (exit 1, no output → unverified) on the
# first call and returns MERGED JSON on every call after. Used to prove an
# unverified finalize is not counted and does not trip the breaker, while still
# terminating the run (the second, verified-merged finalize hits MAX_ISSUES=1).
write_gh_flaky_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  exit 1
fi
printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z"}'
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that ALWAYS fails transport (exit 1,
# NO stderr → unverified). Used to prove UNVERIFIED_MAX consecutive unverified
# finalizes stop the loop (verify-unavailable).
write_gh_transport_fail_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that emulates `gh pr view` on a PR
# number that does NOT exist — a resolve failure (stderr signature + nonzero exit),
# distinct from a transport outage. verify_finalized_pr must classify this
# mismatch:UNRESOLVED (forged marker), not unverified.
write_gh_notfound_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
echo "GraphQL: Could not resolve to a PullRequest with the number of $1. (repository.pullRequest)" >&2
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): worker finalizes with a FORGED pr on every call —
# call 1 a not-found number (999999), call 2 a garbage non-numeric string — both
# must be judged abnormal mismatch:UNRESOLVED. Never finalizes cleanly.
write_finalize_forged_pr_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":61,"pr":"999999","duration_s":1}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":62,"pr":"not-a-real-pr","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub whose FIRST read reports OPEN and
# every read after reports MERGED — proves the mismatch-grace retry absorbs
# read-after-merge lag (ends up merged, never mismatch).
write_gh_open_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":null}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub — FIRST read OPEN with auto-merge
# ARMED (pending-automerge verdict), every read after MERGED (so a following
# iteration terminates the run). Proves the auto-merge path is not a false mismatch.
write_gh_automerge_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":{"enabledAt":"2026-01-01T00:00:00Z"}}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813, finding 4): worker finalizes with a FOREIGN-host PR
# URL (correct path shape, wrong host/repo) — must classify mismatch:UNRESOLVED,
# never merged. Never finalizes cleanly.
write_finalize_foreign_url_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":71,"pr":"https://github.com/evil/other/pull/5","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# ---------------------------------------------------------------------------
# Common env baseline: every test starts here, then overrides what it needs.
# Clear preflight (no holds), generous budgets, fast polling/backoff.
# ---------------------------------------------------------------------------
common_env() {
  local d="$1"
  export MARKER_FILE="$d/marker.json"
  export STOP_FILE="$d/stop"
  export LOG_DIR="$d/logs"
  export JOURNAL_FILE="$d/logs/journal.jsonl"
  export SUPERVISOR_LOCK="$d/lock"
  export NOTIFY_LOG="$d/notify.log"
  : >"$NOTIFY_LOG"
  write_notify_stub "$d/bin/notify.sh"
  export NOTIFY_CMD="$d/bin/notify.sh"
  export LOAD_PROBE_CMD="echo 0"
  export DISK_PROBE_CMD="echo 999999"
  export PROC_PROBE_CMD="echo 0"
  # roborev 1839: preflight bounds the two leftover families separately, so it reads
  # per-family probes. Default both to clear; leftover-hold tests override the family
  # they exercise.
  export PROC_PROBE_WORKER_CMD="echo 0"
  export PROC_PROBE_BUILD_CMD="echo 0"
  # issue #2670: every "finalized" marker is now GH-verified. Default the mock to
  # MERGED so all pre-existing finalize-based cases credit the issue as before;
  # verification tests override GH_VERIFY_CMD to exercise mismatch/unverified.
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\",\"autoMergeRequest\":null}"'
  # roborev 1813: mismatch grace re-reads gh a few times; keep the wait at 0 so no
  # test ever sleeps for it.
  export MISMATCH_RETRY_WAIT_SECS=0
  export LOAD_MAX=999999
  export MAX_ISSUES=100
  export MAX_HOURS=8
  export BREAKER_N=3
  export BACKOFF_NOWORK_SECS=1
  export HOLD_POLL_SECS=1
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  unset LOAD_CONTROL_FILE 2>/dev/null || true
  unset WORKER_CMD 2>/dev/null || true
}

jline_count() { grep -c "$2" "$1" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# Test 1: happy path — 2 finalized iterations, then MAX_ISSUES=2 budget stop.
# ---------------------------------------------------------------------------
test_happy_path_budget_stop() {
  local d counter jf rc fcount scount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  scount=$(jline_count "$jf" '"outcome":"summary"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 2 && "$scount" -eq 1 ]] && grep -q '"reason":"budget-issues"' "$jf"; then
    pass "happy path: 2 finalized + budget-issues summary stop"
  else
    fail "happy path: rc=$rc finalized=$fcount summary=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 2: crash-loop breaker — N abnormal exits stop + alert, no hot respawn.
# ---------------------------------------------------------------------------
test_breaker_stops_on_abnormal() {
  local d jf rc acount ncount
  d="$(new_case_dir)"
  common_env "$d"
  write_abnormal_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BREAKER_N=3
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ncount=$(grep -c '^error|.*BREAKER' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 3 ]] && grep -q '"reason":"breaker"' "$jf" && [[ "$ncount" -ge 1 ]]; then
    pass "breaker: 3 consecutive abnormal exits stop with ALERT, no hot respawn"
  else
    fail "breaker: rc=$rc abnormal=$acount notify_breaker=$ncount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 3: stop-file honored between iterations (clean exit, exactly 1 ran).
# ---------------------------------------------------------------------------
test_stop_file_honored() {
  local d counter jf sup_pid rc waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # Sleep after the marker write so the test can create the stop-file while
  # this iteration is still "in flight" — guarantees the NEXT loop-top check
  # sees it, with no race on how fast the stub itself runs.
  write_finalize_stub "$d/bin/worker.sh" "$counter" 1
  export WORKER_CMD="$d/bin/worker.sh"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  local fcount
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file: honored between iterations, exactly 1 ran"
  else
    fail "stop-file: rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 4: preflight hold — high load blocks the spawn until it clears.
# ---------------------------------------------------------------------------
test_preflight_load_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`, not here.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # Load-proof: this suite is now gate-wired (agent-gate.sh tooling-tests), so a
  # fixed sleep-then-assert window flakes when the box is busy. POLL (hard-capped
  # at 30s) until the HOLD notify appears instead — the semantic is unchanged
  # (HOLD fires while load is high, spawn is deferred). Load stays pinned high
  # (LOAD_CONTROL_FILE=99) throughout, so the spawn cannot happen until we clear
  # it below; the counter must remain absent the whole time.
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  local invoked_while_high="no"
  [[ -f "$counter" ]] && invoked_while_high="yes"

  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  wait "$sup_pid"
  rc=$?

  if [[ "$invoked_while_high" == "no" && "$rc" -eq 0 && -f "$counter" && "$hold_notifies" -ge 1 ]]; then
    pass "preflight: high load holds the spawn (no invoke), then proceeds once clear (HOLD notify fired)"
  else
    fail "preflight: invoked_while_high=$invoked_while_high rc=$rc hold_notifies=$hold_notifies (see $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 5: no-work backoff does NOT count against MAX_ISSUES.
# ---------------------------------------------------------------------------
test_nowork_not_counted() {
  local d call_ctr issue_ctr jf rc
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  issue_ctr="$d/issues"
  common_env "$d"
  write_nowork_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "$issue_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  local nwcount fcount calls
  nwcount=$(jline_count "$jf" '"outcome":"no-work"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  calls=$(cat "$call_ctr" 2>/dev/null || echo -1)
  # MAX_ISSUES=1: if no-work counted, the supervisor would stop after the
  # very first (no-work) iteration and never reach a second, finalizing call.
  if [[ "$rc" -eq 0 && "$nwcount" -eq 1 && "$fcount" -eq 1 && "$calls" -eq 2 ]]; then
    pass "no-work: backoff sleeps but does not count toward MAX_ISSUES"
  else
    fail "no-work: rc=$rc no-work=$nwcount finalized=$fcount calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 6: single-instance lock — second concurrent invocation refuses loudly.
# ---------------------------------------------------------------------------
test_single_instance_lock() {
  local d counter pid_a rc_b stderr_b
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter" 3
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100

  bash "$SUPERVISOR" >"$d/stdout_a.log" 2>&1 &
  pid_a=$!
  local waited=0
  while [[ ! -f "$counter" && "$waited" -lt 50 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done

  bash "$SUPERVISOR" >"$d/stdout_b.log" 2>&1
  rc_b=$?
  stderr_b="$(cat "$d/stdout_b.log")"

  kill "$pid_a" 2>/dev/null || true
  wait "$pid_a" 2>/dev/null || true

  if [[ "$rc_b" -ne 0 ]] && echo "$stderr_b" | grep -q "already running" && echo "$stderr_b" | grep -q "pid $pid_a"; then
    pass "flock: second concurrent instance refuses loudly with holder pid"
  else
    fail "flock: rc_b=$rc_b pid_a=$pid_a stderr_b='$stderr_b'"
  fi
}

# ---------------------------------------------------------------------------
# Test 7: stale marker is removed before spawn (never re-judged).
# ---------------------------------------------------------------------------
test_stale_marker_removed() {
  local d sentinel jf rc
  d="$(new_case_dir)"
  sentinel="$d/stale-detected"
  common_env "$d"
  write_stale_check_stub "$d/bin/worker.sh" "$sentinel"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"
  mkdir -p "$(dirname "$MARKER_FILE")"
  echo '{"outcome":"finalized","issue":999,"pr":"stale"}' >"$MARKER_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$sentinel" ]] && grep -q '"issue":1,' "$jf" && ! grep -q '"issue":999' "$jf"; then
    pass "stale marker: removed before spawn, fresh outcome judged (issue 1, not stale 999)"
  else
    fail "stale marker: rc=$rc sentinel_present=$([[ -f "$sentinel" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 8 (F2 regression): the SAME issue reports "blocked" on two consecutive
# iterations → supervisor stops after the second with a head-blocked notify,
# clean exit, and never reaches MAX_HOURS/MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_blocked_head_of_queue_stops() {
  local d jf rc bcount hb_notifies
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  bcount=$(jline_count "$jf" '"outcome":"blocked"')
  hb_notifies=$(grep -c '^error|.*persistently blocked' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$bcount" -eq 2 ]] && grep -q '"reason":"head-blocked"' "$jf" && [[ "$hb_notifies" -ge 1 ]]; then
    pass "F2: same issue blocked twice in a row stops cleanly with head-blocked notify"
  else
    fail "F2: rc=$rc blocked_iters=$bcount head_blocked_notifies=$hb_notifies (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 9 (F5 regression): a "finalized" marker missing "pr" is judged
# abnormal — ISSUES_DONE must not advance, and it must count toward the
# crash-loop breaker (proven here by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_missing_pr_is_abnormal() {
  local d jf rc fcount acount
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_missing_pr_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "F5: finalized marker missing pr is judged abnormal, not counted done"
  else
    fail "F5: rc=$rc finalized=$fcount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 10 (F3 regression): a "reason" containing a double-quote and an
# embedded newline must still produce a journal line that parses as valid
# JSON — proves journal_line's json_or_null escaping (not just printf %s).
# ---------------------------------------------------------------------------
test_journal_escapes_nasty_reason() {
  local d jf rc line all_valid
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_nasty_reason_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ ! -f "$jf" ]]; then
    fail "F3: no journal file written ($jf)"
    return
  fi
  all_valid="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    printf '%s' "$line" | python3 -c 'import json,sys; json.loads(sys.stdin.read())' 2>/dev/null || all_valid="no"
  done <"$jf"
  if [[ "$rc" -eq 0 && "$all_valid" == "yes" ]] && grep -q '"outcome":"blocked"' "$jf"; then
    pass "F3: reason with embedded quote+newline still yields valid JSON journal lines"
  else
    fail "F3: rc=$rc all_valid=$all_valid (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 11 (#2666): blocked/seam1-approval is a CLEAN PARK → verdict
# parked-on-owner, ONE high-priority page, never abnormal, never trips the
# breaker (BREAKER_N=1 here would stop before finalizing if it did), and the
# loop advances to the next issue.
# ---------------------------------------------------------------------------
test_park_seam1_parked_on_owner() {
  local d call_ctr jf rc pcount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "seam1-approval" ""
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: parked issue 77' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "park(seam1-approval): parked-on-owner + high page, no breaker, loop advances"
  else
    fail "park(seam1): rc=$rc parked=$pcount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 12 (#2666): blocked/needs-decision parks the same way AND the page title
# carries the marker's one-line "question" field (issue # + first line).
# ---------------------------------------------------------------------------
test_park_needs_decision_question_in_title() {
  local d call_ctr jf rc pcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "needs-decision" "Which compaction strategy for wide rows?"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  page=$(grep -c 'parked issue 77 — Which compaction strategy for wide rows?' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$page" -ge 1 ]] && grep -q '"reason":"needs-decision"' "$jf"; then
    pass "park(needs-decision): parked-on-owner + question text in the page title"
  else
    fail "park(needs-decision): rc=$rc parked=$pcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 13 (#2666): a marker with an UNKNOWN outcome value is still judged
# abnormal (counts toward the breaker) — parks must not have widened the set of
# "trusted" outcomes.
# ---------------------------------------------------------------------------
test_unknown_outcome_is_abnormal() {
  local d jf rc acount pcount
  d="$(new_case_dir)"
  common_env "$d"
  write_unknown_outcome_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$pcount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "unknown outcome: judged abnormal, trips breaker (not parked/trusted)"
  else
    fail "unknown outcome: rc=$rc abnormal=$acount parked=$pcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 14 (#2666): a worker WEDGED on an interactive prompt is detected
# mid-iteration → immediate high page + verdict stuck-on-question when it exits
# without a marker; NEVER abnormal, never trips the breaker (BREAKER_N=1 here).
# The second iteration finalizes so the run terminates at MAX_ISSUES.
# ---------------------------------------------------------------------------
test_stuck_on_question_detected() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # Generous deadline headroom: the watchdog detects on its first poll (~1s in);
  # a large MAX_ITER_SECS keeps detection well ahead of the deadline-kill even
  # under a heavily loaded box (the wedged stub sleeps 120s, so it is always
  # killed by the deadline, never by exiting on its own).
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf" &&
     grep -q 'AskUserQuestion' "$NOTIFY_LOG"; then
    pass "stuck-on-question: detected mid-iteration, high page, no breaker, loop advances"
  else
    fail "stuck-on-question: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 15 (#2666): unit-test the prompt-signature grep directly by SOURCING the
# supervisor (the source-guard keeps main() from running) and calling
# detect_prompt_signature/captured_question against fixture logs — fires on a
# menu block, stays silent on a clean log, and captures the question text.
# ---------------------------------------------------------------------------
test_prompt_signature_grep() {
  local d fixture clean out
  d="$(new_case_dir)"
  fixture="$d/iter.log"
  clean="$d/clean.log"
  printf 'building project...\nrunning tests\n\xe2\x9d\xaf 1. Yes\n  2. No\n' >"$fixture"
  printf 'building project...\nall tests green\nfinalized issue 5\n' >"$clean"

  out="$(SUPERVISOR="$SUPERVISOR" FIX="$fixture" CLN="$clean" bash -c '
    # shellcheck disable=SC1090
    source "$SUPERVISOR"
    set +e
    if detect_prompt_signature "$FIX"; then echo MATCH; else echo NOMATCH; fi
    if detect_prompt_signature "$CLN"; then echo CLEAN-MATCH; else echo CLEAN-NOMATCH; fi
    captured_question "$FIX"
  ' 2>/dev/null)"

  if echo "$out" | grep -q '^MATCH$' && echo "$out" | grep -q '^CLEAN-NOMATCH$' &&
     echo "$out" | grep -q '1. Yes'; then
    pass "prompt-signature grep: fires on menu block, silent on clean log, captures text"
  else
    fail "prompt-signature grep: out='$out'"
  fi
}

# ---------------------------------------------------------------------------
# Test 16 (#2666 / roborev 1769): a stuck-on-question iteration RESETS the
# consecutive-abnormal counter — the chain abnormal→stuck→abnormal→abnormal must
# NOT trip a BREAKER_N=3 breaker (the stuck iteration breaks the chain). Call 5
# finalizes so the run terminates at MAX_ISSUES=1.
# ---------------------------------------------------------------------------
test_stuck_breaks_abnormal_chain() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_abnormal_stuck_abnormal_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=3
  # Headroom so the stuck iteration (call 2) is reliably DETECTED (not
  # deadline-killed before the first poll) even under load — the whole point of
  # this test is that a detected stuck iteration resets the abnormal chain.
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$acount" -eq 3 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "stuck breaks abnormal chain: 3 abnormals split by a stuck iter never trip BREAKER_N=3"
  else
    fail "stuck-chain: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 17 (#2666 / roborev 1769): the SAME issue parking on two consecutive
# iterations → head-block-on-decision page + clean stop (mirrors the F2
# blocked-path guard); never loops to MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_park_same_issue_stops() {
  local d jf rc pcount hb
  d="$(new_case_dir)"
  common_env "$d"
  write_park_same_issue_stub "$d/bin/worker.sh" 33 "needs-decision"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  hb=$(grep -c '^error|worker-supervisor: issue 33 head-blocked on decision' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$hb" -ge 1 ]] && grep -q '"reason":"head-blocked-decision"' "$jf"; then
    pass "repeated park (same issue): head-blocked-on-decision page + clean stop after 2"
  else
    fail "repeated-park: rc=$rc parked=$pcount head_block=$hb (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 18 (#2666 / roborev 1769): parks of DIFFERENT issues do NOT trip the
# head-block guard — issue 41 then 42 park, then a finalize terminates the run.
# ---------------------------------------------------------------------------
test_different_issue_parks_do_not_head_block() {
  local d call_ctr jf rc pcount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_two_issues_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"head-blocked-decision"' "$jf" && ! grep -q 'head-blocked on decision' "$NOTIFY_LOG"; then
    pass "different-issue parks: no head-block, loop advances through both then finalizes"
  else
    fail "different-park: rc=$rc parked=$pcount finalized=$fcount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 19 (#2666 / roborev 1773, case a): a crash whose ONLY signature is a stray
# match in scrollback (log grew + match scrolled out of the tail) must stay
# ABNORMAL and count toward the breaker — NOT be misclassified as stuck.
# ---------------------------------------------------------------------------
test_stray_signature_scrollback_is_abnormal() {
  local d jf rc acount scount
  d="$(new_case_dir)"
  common_env "$d"
  write_crash_stray_signature_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=20
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$scount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "stray scrollback signature: crash stays ABNORMAL (breaker), not stuck"
  else
    fail "stray-scrollback: rc=$rc abnormal=$acount stuck=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 20 (#2666 / roborev 1773, case b): a GENUINE wedge — alive + signature in
# the tail + log frozen across two consecutive scans → stuck-on-question, high
# page, never toward the breaker. Call 2 finalizes so the run terminates.
# ---------------------------------------------------------------------------
test_genuine_wedge_frozen_is_stuck() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=10
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "genuine wedge (frozen log + tail signature x2 polls): stuck, high page, no breaker"
  else
    fail "genuine-wedge: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 21 (#2666 / roborev 1773, case c): a BUSY worker printing the signature on
# every line while STILL WRITING (log grows between scans) → no-growth evidence
# fails → NOT stuck (marker-less kill stays abnormal). Call 2 finalizes.
# ---------------------------------------------------------------------------
test_busy_writing_signature_not_stuck() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_busy_signature_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=2
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=5
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 0 && "$acount" -eq 1 && "$fcount" -eq 1 ]]; then
    pass "busy worker printing signature while writing: NOT stuck (growth defeats it)"
  else
    fail "busy-writing: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 22 (#2666 / roborev 1773, case d): exit-latency — a fast-finalizing worker
# is judged on the ~1s exit cadence, NOT held until the 30s wedge-scan cadence.
# Loose, load-proof cap (well under STUCK_POLL_SECS=30).
# ---------------------------------------------------------------------------
test_fast_exit_latency() {
  local d counter t0 t1 elapsed rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export STUCK_POLL_SECS=30
  export MAX_ITER_SECS=7200

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  fcount=$(jline_count "$JOURNAL_FILE" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$elapsed" -lt 15 ]]; then
    pass "exit-latency: fast finalize judged in ${elapsed}s (<15s, not the 30s scan cadence)"
  else
    fail "exit-latency: rc=$rc finalized=$fcount elapsed=${elapsed}s (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 23 (#2670): a "finalized" marker whose PR gh-verifies as MERGED is
# credited normally — outcome finalized, journal `verified: merged`, counted
# toward MAX_ISSUES (proven by the budget-issues stop).
# ---------------------------------------------------------------------------
test_finalized_verified_merged_counts() {
  local d counter jf rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  # explicit MERGED mock (common_env already defaults to this; pin it here so the
  # case is self-describing)
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\"}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] &&
     grep -q '"outcome":"finalized".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf"; then
    pass "verify(merged): finalized credited, journal verified=merged, counts to budget"
  else
    fail "verify(merged): rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24 (#2670): a "finalized" marker whose PR gh-verifies as OPEN is judged
# ABNORMAL — a HIGH page names the discrepancy, ISSUES_DONE does NOT advance
# (the false finalize is not counted), and it counts toward the breaker (proven
# by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_mismatch_open_is_abnormal() {
  local d counter jf rc fcount acount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"abnormal".*"verified":"mismatch:OPEN"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(mismatch OPEN): abnormal + high page, not counted, trips breaker"
  else
    fail "verify(mismatch): rc=$rc finalized=$fcount abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 25 (#2670): gh unavailable → UNVERIFIED — outcome finalized-unverified
# (journal `verified: unverified`), NOT counted toward MAX_ISSUES, and NEUTRAL to
# the breaker (BREAKER_N=1 here must NOT trip on it). The gh mock fails on call 1
# (unverified) then returns MERGED, so the second, verified-merged finalize hits
# MAX_ISSUES=1 — proving the unverified iteration was not counted (else the run
# would have stopped before it).
# ---------------------------------------------------------------------------
test_finalized_unverified_not_counted_no_breaker() {
  local d counter gh_ctr jf rc ucount fcount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_flaky_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c 'finalized UNVERIFIED' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ucount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-unverified".*"verified":"unverified"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(unverified): finalized-unverified, not counted, breaker neutral, loop continues"
  else
    fail "verify(unverified): rc=$rc unverified=$ucount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 26 (#2670): PROC_PROBE discriminates the supervisor's OWN worker spawn
# shape (`claude ... --agent worker`) from a legitimate interactive `claude`
# session / a different-agent session. Portable PROPERTY proof is a pure-string
# `grep -E` regex check (always runs, deterministic); the live-process PID check
# is a bonus that SKIPs (never fails) if the control process never schedules
# within the wait cap — an environmental non-result, not a property failure
# (roborev 1819 finding 7).
# ---------------------------------------------------------------------------
test_proc_probe_discriminates_worker_claude() {
  local pat='[c]laude.*--agent worker'
  # Pure-string property proof (no live process): the pattern matches the worker
  # argv shape and NOT a different-agent session.
  if ! printf 'claude --agent worker resume the claim\n' | grep -qE "$pat" ||
       printf 'claude --agent flow-lead review the board\n' | grep -qE "$pat"; then
    fail "proc-probe: pure-string regex does not discriminate worker vs other-agent"
    return
  fi
  # Bonus live check: spawn the two argv-shaped stubs and confirm the same
  # discrimination against real PIDs.
  bash -c 'exec -a "claude --agent worker resume the claim branch" sleep 30' &
  local wpid=$!
  bash -c 'exec -a "claude --agent flow-lead review the board" sleep 30' &
  local ipid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$ipid" 2>/dev/null || true
    wait "$wpid" "$ipid" 2>/dev/null || true
    skip "proc-probe (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local worker_matched="yes" interactive_matched="no"
  pgrep -f "$pat" | grep -qw "$ipid" && interactive_matched="yes"
  kill "$wpid" "$ipid" 2>/dev/null || true
  wait "$wpid" "$ipid" 2>/dev/null || true
  if [[ "$worker_matched" == "yes" && "$interactive_matched" == "no" ]]; then
    pass "proc-probe: matches --agent worker spawn, excludes interactive/other-agent claude"
  else
    fail "proc-probe: worker_matched=$worker_matched interactive_matched=$interactive_matched"
  fi
}

# ---------------------------------------------------------------------------
# Test 27 (#2670 / roborev 1810 HIGH, 1839): a `leftover-worker` preflight hold (an
# orphaned worker CLI) that NEVER clears must STOP the supervisor loudly
# (leftover-worker, exit 1, high page naming survivors) after the TIGHT
# LEFTOVER_HOLD_MAX passes — not latch it silently until MAX_HOURS.
# ---------------------------------------------------------------------------
test_leftover_hold_bounded_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # Worker-family probe never clears (always reports an orphaned worker CLI); the
  # worker would finalize if ever spawned (it must not be).
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_CMD="echo '12345 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=3
  export HOLD_POLL_SECS=1
  export MAX_HOURS=8
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf" &&
     grep -q '12345' "$NOTIFY_LOG"; then
    pass "leftover-worker bound: never-clearing worker orphan stops loudly (exit 1, survivors named), no spawn"
  else
    fail "leftover-worker: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 28 (#2670 / roborev 1810 MED): UNVERIFIED_MAX consecutive unverified
# finalizes STOP the supervisor (verify-unavailable, exit 1, high page) — a
# persistent verification outage must not let uncounted-forever iterations drift
# past the MAX_ISSUES ceiling.
# ---------------------------------------------------------------------------
test_persistent_unverified_stops() {
  local d counter jf rc ucount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_transport_fail_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: verification unavailable' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$ucount" -eq 2 && "$page" -ge 1 ]] &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "persistent unverified: 2 consecutive stop the loop (verify-unavailable, exit 1, high page)"
  else
    fail "persistent-unverified: rc=$rc unverified=$ucount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 29 (#2670 / roborev 1810 MED): a FORGED `pr` is an escalation, not a blip —
# a gh-not-found number (999999) and a garbage non-numeric string both classify
# mismatch:UNRESOLVED (abnormal, high MISMATCH page, breaker-counting), NEVER
# unverified. BREAKER_N=2 stops after the two forged finalizes.
# ---------------------------------------------------------------------------
test_forged_pr_is_unresolved_mismatch() {
  local d call_ctr jf rc acount ucount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_finalize_forged_pr_stub "$d/bin/worker.sh" "$call_ctr"
  write_gh_notfound_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  # both forged finalizes → mismatch:UNRESOLVED (one via gh not-found, one via
  # shape-check), never unverified; breaker trips at 2.
  if [[ "$rc" -ne 0 && "$acount" -eq 2 && "$ucount" -eq 0 && "$page" -ge 2 ]] &&
     [[ "$(jline_count "$jf" '"verified":"mismatch:UNRESOLVED"')" -eq 2 ]] &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "forged pr: not-found number + garbage string both mismatch:UNRESOLVED (escalation, not unverified)"
  else
    fail "forged-pr: rc=$rc abnormal=$acount unverified=$ucount mismatch_page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 30 (#2670 / roborev 1810 HIGH): the bounded hold loop re-checks exit
# conditions on EVERY pass — a stop-file created WHILE preflight is holding (a
# non-leftover reason, so the leftover cap is not what stops it) exits cleanly
# from inside the hold loop, never spawning. Deterministic (no timing race): load
# stays pinned high until the stop-file lands.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  # stop while still holding (load never cleared)
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$counter" && "$hold_notifies" -ge 1 ]] &&
     grep -q '"reason":"stop-file"' "$JOURNAL_FILE"; then
    pass "stop-file mid-hold: bounded hold loop exits cleanly from inside the hold, no spawn"
  else
    fail "stop-mid-hold: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) holds=$hold_notifies (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 31 (#2670 / roborev 1813 MED-HIGH): the DEFAULT proc probe must not count
# its own brace-group `bash -c` wrapper (whose argv carries the pattern text). On
# Linux a naive pattern matches that wrapper → a phantom leftover at EVERY boot,
# hard-stopping every supervisor (macOS `pgrep -f` happens not to, but the bracket
# trick is the portable fix). Proven portably + deterministically: a process whose
# argv holds the LITERAL pattern text (as the wrapper's does) must NOT match the
# bracketed pattern, while a REAL `claude --agent worker` process MUST. Plus a
# sanity run of the verbatim default probe: well-formed, and its worker-Claude
# sub-probe is 0 with no worker running.
test_probe_no_self_match() {
  local pat='[c]laude.*--agent worker'
  # PROPERTY proof (pure-string, always runs, deterministic): the bracketed pattern
  # matches a REAL worker argv, and does NOT match a process whose argv literally
  # contains the bracketed PATTERN TEXT (exactly as the probe's own `bash -c`
  # wrapper does) — `[c]laude` = literal `c`+`laude`; the text `[c]laude` has `c`
  # followed by `]`, so no match. This is the self-exclusion property.
  if ! printf 'claude --agent worker resume the claim\n' | grep -qE "$pat" ||
       printf 'wrap [c]laude.*--agent worker probe\n' | grep -qE "$pat"; then
    fail "probe self-match: pure-string regex fails the self-exclusion property"
    return
  fi
  # static sanity: the real DEFAULT probe string carries the bracket trick + the
  # $$/$PPID self-exclusion on BOTH families. Source with PROC_PROBE_CMD unset so a
  # leaked test override can't mask the default; assert the string, don't execute it.
  local probe_cmd defaulted="no"
  # shellcheck disable=SC2016  # $SUP/$PROC_PROBE_CMD expand inside the sub-bash, not here.
  probe_cmd="$(env -u PROC_PROBE_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_PROBE_CMD"' 2>/dev/null)"
  [[ "$probe_cmd" == *'[c]laude.*--agent worker'* && "$probe_cmd" == *'[c]argo '* &&
     "$probe_cmd" == *'grep -vxF'* ]] && defaulted="yes"
  if [[ "$defaulted" != "yes" ]]; then
    fail "probe self-match: default probe string missing bracket trick / self-exclusion: '${probe_cmd:0:60}'"
    return
  fi
  # Bonus live check: real `claude --agent worker` stub matches, wrapper-text stub
  # does not. SKIPs (never fails) if the control worker never schedules.
  bash -c 'exec -a "claude --agent worker resume the claim" sleep 30' &
  local wpid=$!
  bash -c 'exec -a "wrap [c]laude.*--agent worker probe" sleep 30' &
  local xpid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$xpid" 2>/dev/null || true
    wait "$wpid" "$xpid" 2>/dev/null || true
    skip "probe self-match (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local wrapper_matched="no"
  pgrep -f "$pat" | grep -qw "$xpid" && wrapper_matched="yes"
  kill "$wpid" "$xpid" 2>/dev/null || true
  wait "$wpid" "$xpid" 2>/dev/null || true
  if [[ "$wrapper_matched" == "no" ]]; then
    pass "probe self-match: bracket trick matches a real worker, excludes the wrapper-argv text"
  else
    fail "probe self-match: wrapper-argv text was matched (self-exclusion broken)"
  fi
}

# ---------------------------------------------------------------------------
# Test 32 (#2670 / roborev 1813 MED): a tooling gap (NO json parser present) on a
# VALID gh response must read as `unverified` (transport class), NEVER
# mismatch:UNRESOLVED — a missing parser is our problem, not the worker's forgery.
# Unit-tests verify_finalized_pr under a PATH with jq/python3 removed.
test_parser_absent_is_unverified() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/nobin"
  mkdir -p "$bindir"
  # symlink only the tools sourcing + verify_finalized_pr's unverified path need
  # (dirname/date are used at source time); jq AND python3 are deliberately absent.
  for t in bash mktemp cat rm grep dirname date; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "unverified" ]]; then
    pass "parser-absent: no jq/python3 on a valid response → unverified (tooling gap, not forgery)"
  else
    fail "parser-absent: got '$out' (expected unverified)"
  fi
}

# ---------------------------------------------------------------------------
# Test 33 (#2670 / roborev 1813 MED, 1839): OPEN with auto-merge ARMED is a legitimate
# pending state, not a false finalize — verdict finalized-pending-automerge: NOT
# counted toward MAX_ISSUES immediately, default-priority page, breaker-NEUTRAL. The
# PR is re-verified on the NEXT iteration and, now MERGED, RETROACTIVELY credited
# (pending-credited), reaching MAX_ISSUES=1 — proving the armed PR both wasn't
# double-counted and wasn't lost.
test_pending_automerge_verdict() {
  local d counter gh_ctr jf rc pcount ccount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_automerge_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"finalized-pending-automerge"')
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  page=$(grep -c 'finalized PENDING AUTO-MERGE' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$ccount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-pending-automerge".*"verified":"pending-automerge"' "$jf" &&
     grep -q '"outcome":"pending-credited".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "pending-automerge: armed → not counted yet, then retroactively credited on MERGED (breaker-neutral)"
  else
    fail "pending-automerge: rc=$rc pending=$pcount credited=$ccount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 34 (#2670 / roborev 1813 MED): mismatch grace absorbs read-after-merge lag —
# gh reports OPEN on read 1, MERGED on read 2, so the verdict is `merged` (counted),
# never a spurious mismatch. Proves the retry re-reads gh (call counter reaches 2).
test_mismatch_grace_absorbs_lag() {
  local d counter gh_ctr jf rc fcount acount calls
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_open_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export MISMATCH_RETRIES=3
  export MISMATCH_RETRY_WAIT_SECS=0
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  calls=$(cat "$gh_ctr" 2>/dev/null || echo -1)
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$acount" -eq 0 && "$calls" -ge 2 ]] &&
     grep -q '"verified":"merged"' "$jf"; then
    pass "mismatch grace: OPEN-then-MERGED across a retry → merged, no spurious mismatch (gh read ${calls}x)"
  else
    fail "mismatch-grace: rc=$rc finalized=$fcount abnormal=$acount gh_calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 35 (#2670 / roborev 1813 finding 4): a foreign-host PR URL (right path
# shape, wrong host/repo) is a forged reference → mismatch:UNRESOLVED (abnormal,
# high page), never merged. BREAKER_N=1 stops on the single forged finalize.
test_foreign_url_is_unresolved() {
  local d jf rc acount page
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_foreign_url_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "foreign URL: non-pmcfadin/cqlite PR URL → mismatch:UNRESOLVED (escalation), never merged"
  else
    fail "foreign-url: rc=$rc abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 36 (#2670 / roborev 1813 finding 5, 1839): leftover-worker holds are counted
# CUMULATIVELY across the invocation — a transient load blip interleaved between
# leftover holds must NOT reset the bound. Alternating load(high)/leftover holds
# still trip the leftover-worker bound and stop the loop. (With the pre-fix reset, the
# leftover tally would zero on each load pass and never trip.)
test_alternating_holds_still_bounded() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # load probe toggles high/low each poll via a counter file; worker probe always
  # reports a leftover. preflight checks load BEFORE procs, so odd polls hold on
  # `load`, even polls hold on `leftover-worker` — never clearing to a spawn.
  export LOAD_CONTROL_FILE="$d/loadctr"
  echo 0 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='n=$(cat "$LOAD_CONTROL_FILE"); n=$((n+1)); echo "$n" >"$LOAD_CONTROL_FILE"; if [ $((n % 2)) -eq 1 ]; then echo 99; else echo 0; fi'
  export LOAD_MAX=1
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_CMD="echo '999 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=2
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf"; then
    pass "alternating holds: leftover-worker tally is cumulative across a load blip → still bounded (stops)"
  else
    fail "alternating-holds: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 37 (#2670 / roborev 1819 HIGH, finding 1): a hold entered with ONLY MAX_HOURS
# set (MAX_HOURS_SECS derived, not passed) must NOT spuriously abort budget-wallclock
# from inside the hold loop — proves the derived budget is defined on the hold path.
# Load pinned high, then cleared; the run holds, then finalizes normally.
# ---------------------------------------------------------------------------
test_maxhours_only_hold_no_abort() {
  local d counter jf rc sup_pid waited hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export MAX_HOURS=8
  unset MAX_HOURS_SECS 2>/dev/null || true   # force derivation on the hold path
  export LOAD_CONTROL_FILE="$d/load"; echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "${hold_notifies:-0}" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do sleep 0.1; waited=$((waited + 1)); done
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     ! grep -q '"reason":"budget-wallclock"' "$jf"; then
    pass "maxhours-only hold: derived budget on hold path, no spurious budget-wallclock abort"
  else
    fail "maxhours-only: rc=$rc counter=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 38 (#2670 / roborev 1819 HIGH, finding 2): a TRANSPORT error whose stderr
# merely contains "not found" (`dial tcp ... host not found`) must classify
# `unverified`, NOT mismatch:UNRESOLVED — the tightened classifier keys only on
# gh's actual resolve-failure signature, so a DNS/proxy 404 is never read as forgery.
# ---------------------------------------------------------------------------
test_transport_notfound_is_unverified() {
  local d counter jf rc ucount acount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=1   # a single unverified stops the loop → deterministic end
  export GH_VERIFY_CMD='echo "dial tcp: lookup github.com: no such host: host not found" >&2; exit 1'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$ucount" -eq 1 && "$acount" -eq 0 ]] &&
     grep -q '"verified":"unverified"' "$jf" &&
     ! grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "transport not-found: DNS/host-not-found stderr → unverified, never forgery"
  else
    fail "transport-notfound: rc=$rc unverified=$ucount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 39 (#2670 / roborev 1819 MED, finding 3): with jq ABSENT but python3
# present, verify_finalized_pr falls through to the python3 parser and correctly
# classifies an OPEN+auto-merge-armed response as pending-automerge. Unit-tests the
# function under a PATH with jq removed (python3 kept).
# ---------------------------------------------------------------------------
test_python_only_parser_automerge() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/pybin"
  mkdir -p "$bindir"
  # symlink the tools the function needs PLUS python3; jq deliberately absent.
  for t in bash mktemp cat rm grep dirname date sed python3; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE=/nonexistent \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "pending-automerge" ]]; then
    pass "python-only parser: jq absent, python3 parses OPEN+auto-merge → pending-automerge"
  else
    fail "python-only: got '$out' (expected pending-automerge)"
  fi
}

# ---------------------------------------------------------------------------
# Test 40 (#2670 / roborev 1819 MED, finding 4): the mismatch-grace retry loop
# honors the stop-file mid-grace — a shutdown request must not wait out the full
# grace. gh always returns OPEN (never merges), MISMATCH_RETRIES large with a 1s
# wait; the stop-file is created while grace is sleeping, and the supervisor exits
# cleanly (stop-file) well under the would-be full grace time.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_grace() {
  local d counter jf rc sup_pid t0 elapsed waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":null}"'
  export MISMATCH_RETRIES=100
  export MISMATCH_RETRY_WAIT_SECS=1   # would be ~100s of grace without the stop check
  jf="$JOURNAL_FILE"

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # wait until the worker has run (counter written → we're into verify/grace)
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do sleep 0.1; waited=$((waited + 1)); done
  sleep 1   # let grace enter its sleep
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  if [[ "$rc" -eq 0 && "$elapsed" -lt 30 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file mid-grace: grace loop honors the stop-file, exits in ${elapsed}s (not full grace)"
  else
    fail "stop-mid-grace: rc=$rc elapsed=${elapsed}s (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 41 (#2670 / roborev 1819 MED, 1839): the SAME PR observed OPEN-with-auto-merge-
# armed across PENDING_AUTOMERGE_MAX consecutive observations is auto-merge-stuck — the
# supervisor pages high and STOPS (automerge-stuck, exit 1) rather than looping forever.
# The stub finalizes the SAME fixed PR each iteration; gh always returns OPEN+armed for
# it; PENDING_AUTOMERGE_MAX=2.
# ---------------------------------------------------------------------------
test_persistent_pending_automerge_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_fixed_pr_finalize_stub "$d/bin/worker.sh" "$counter" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$page" -ge 1 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf" &&
     grep -q '/pull/7' "$NOTIFY_LOG"; then
    pass "persistent pending-automerge: SAME PR unmerged x2 stops the loop (automerge-stuck, exit 1, high page)"
  else
    fail "persistent-pending: rc=$rc page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 45 (#2670 / roborev 1839 HIGH): the HEALTHY case — a fast fleet finalizes N
# DISTINCT PRs that are each briefly OPEN+armed then land. Under the per-PR model this
# must NEVER trip automerge-stuck: each distinct PR is retroactively credited toward
# MAX_ISSUES once it reaches MERGED, and the run ends cleanly at budget-issues. (Under
# the old across-PR streak this false-tripped after PENDING_AUTOMERGE_MAX distinct PRs.)
# gh stub is per-PR: OPEN+armed on the FIRST view of a PR (its finalize), MERGED after.
# ---------------------------------------------------------------------------
test_healthy_multi_pr_no_false_stop() {
  local d counter jf rc scount ccount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"   # distinct incrementing PRs
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=3
  export PENDING_AUTOMERGE_MAX=2   # would false-trip on 2 distinct PRs under the old model
  mkdir -p "$d/ghviews"
  # Per-PR view counter: OPEN+armed on first view, MERGED thereafter.
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='n="${1##*/}"; f="'"$d"'/ghviews/$n"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$c" -le 1 ]; then printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; else printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  scount=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ccount" -ge 3 && "$scount" -eq 0 ]] &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"automerge-stuck"' "$jf"; then
    pass "healthy multi-PR: N distinct armed PRs are credited on MERGED, never false-trip automerge-stuck"
  else
    fail "healthy-multi-pr: rc=$rc credited=$ccount stuck-page=$scount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 46 (#2670 / roborev 1839 HIGH): the self-clearing build/gate family is bounded
# by the LOOSE BUILD_HOLD_MAX, NOT the tight LEFTOVER_HOLD_MAX — a legitimate concurrent
# gate must be waited out, not killed at 15 min. A `leftover-build` hold that never
# clears must survive past LEFTOVER_HOLD_MAX and only stop at BUILD_HOLD_MAX (as
# `leftover-build`, exit 1). LEFTOVER_HOLD_MAX=1 (would stop immediately if it governed
# builds); BUILD_HOLD_MAX=3.
# ---------------------------------------------------------------------------
test_build_hold_uses_loose_bound() {
  local d counter jf rc page holds
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export PROC_PROBE_BUILD_CMD="echo 1"   # a concurrent build/gate that never clears
  export PROC_LIST_CMD="echo '4242 cargo test --workspace'"
  export LEFTOVER_HOLD_MAX=1             # tight worker bound — must NOT govern builds
  export BUILD_HOLD_MAX=3               # loose build bound governs
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: build/gate processes will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  holds=$(grep -c 'leftover-build held' "$d/stdout.log" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-build"' "$jf" &&
     grep -q '4242' "$NOTIFY_LOG"; then
    pass "build hold: self-clearing family uses the LOOSE BUILD_HOLD_MAX (survives LEFTOVER_HOLD_MAX, stops at BUILD_HOLD_MAX)"
  else
    fail "build-hold-loose: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 47 (#2670 / roborev 1839 HIGH): a concurrent build/gate that CLEARS after a few
# polls (a legitimate gate finishing) must be WAITED OUT — the supervisor then spawns
# the worker, which finalizes normally. Proves the loose build bound doesn't kill a run
# that merely had a gate running. Build probe reports busy for 2 polls then clears.
# ---------------------------------------------------------------------------
test_build_hold_clears_then_proceeds() {
  local d counter jf rc
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BUILD_HOLD_MAX=12   # loose; the build clears well before this
  export HOLD_POLL_SECS=1
  export PROC_CTL="$d/buildctr"; echo 0 >"$PROC_CTL"
  # shellcheck disable=SC2016  # expanded later by the supervisor's own `bash -c`.
  export PROC_PROBE_BUILD_CMD='n=$(cat "'"$d"'/buildctr"); n=$((n+1)); echo "$n">"'"$d"'/buildctr"; if [ "$n" -le 2 ]; then echo 1; else echo 0; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"leftover-build"' "$jf"; then
    pass "build hold clears: a concurrent gate that finishes is waited out, then the worker runs"
  else
    fail "build-hold-clears: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 42 (#2670 / roborev 1821, finding a): the count probe (PROC_PROBE_CMD) and
# the list probe (PROC_LIST_CMD) DERIVE from the SAME shared match patterns
# (PROC_MATCH_BUILD/PROC_MATCH_WORKER) — the "what counts" set and the "what we
# name" set cannot drift. Source with both probe overrides unset and assert both
# command strings embed both shared patterns verbatim.
test_probe_list_derives_from_count_set() {
  local out build worker probe list
  # shellcheck disable=SC2016  # $SUP/$PROC_* expand inside the sub-bash, not here.
  out="$(env -u PROC_PROBE_CMD -u PROC_LIST_CMD SUP="$SUPERVISOR" bash -c '
    # shellcheck disable=SC1090
    source "$SUP"
    printf "%s\n%s\n%s\n%s\n" "$PROC_MATCH_BUILD" "$PROC_MATCH_WORKER" "$PROC_PROBE_CMD" "$PROC_LIST_CMD"' 2>/dev/null)"
  build="$(printf '%s' "$out" | sed -n 1p)"
  worker="$(printf '%s' "$out" | sed -n 2p)"
  probe="$(printf '%s' "$out" | sed -n 3p)"
  list="$(printf '%s' "$out" | sed -n 4p)"
  if [[ -n "$build" && -n "$worker" &&
        "$probe" == *"$build"* && "$probe" == *"$worker"* &&
        "$list" == *"$build"* && "$list" == *"$worker"* ]]; then
    pass "probe derivation: count + list probes both derive from the shared match-pattern set"
  else
    fail "probe-derivation: build='$build' worker='$worker' probe-has-both=$([[ "$probe" == *"$build"* && "$probe" == *"$worker"* ]] && echo y) list-has-both=$([[ "$list" == *"$build"* && "$list" == *"$worker"* ]] && echo y)"
  fi
}

# ---------------------------------------------------------------------------
# Test 43 (#2670 / roborev 1821, finding b): MISMATCH_GRACE_CAP_SECS<=0 DISABLES
# the wall-clock cap — grace stays bounded solely by the retry count and must NOT
# be blocked. gh reports OPEN then MERGED; with cap=-1, retries=3, wait=0 the grace
# still retries and resolves `merged` (never a spurious mismatch). Unit-tests
# verify_finalized_pr directly.
test_grace_cap_disabled_semantics() {
  local d ctr out
  d="$(new_case_dir)"
  ctr="$d/gh-calls"
  cat >"$d/gh.sh" <<EOF
#!/usr/bin/env bash
n=0; [[ -f "$ctr" ]] && n=\$(cat "$ctr"); n=\$((n + 1)); echo "\$n" >"$ctr"
if [[ \$n -eq 1 ]]; then printf %s '{"state":"OPEN","autoMergeRequest":null}'
else printf %s '{"state":"MERGED","autoMergeRequest":null}'; fi
EOF
  chmod +x "$d/gh.sh"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  out="$(GH_VERIFY_CMD="$d/gh.sh \"\$1\"" \
        MISMATCH_RETRIES=3 MISMATCH_RETRY_WAIT_SECS=0 MISMATCH_GRACE_CAP_SECS=-1 STOP_FILE=/nonexistent \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "merged" && "$(cat "$ctr" 2>/dev/null)" -ge 2 ]]; then
    pass "grace cap<=0: disabled ceiling, grace stays count-bounded (OPEN→MERGED resolves merged)"
  else
    fail "grace-cap-disabled: got '$out' gh_calls=$(cat "$ctr" 2>/dev/null)"
  fi
}

# ---------------------------------------------------------------------------
# Test 44 (#2670 / roborev 1837 MED): a grace loop CUT SHORT by the stop-file
# (a requested shutdown mid-grace) is NOT a confirmed mismatch — the PR state was
# never allowed to settle, so verify_finalized_pr must return the NEUTRAL `aborted`
# verdict, NEVER `mismatch:OPEN` (which the caller turns into an abnormal "worker
# forged a finalize" HIGH page + breaker+1). `aborted` is also distinct from
# `unverified` so an ordinary shutdown cannot accumulate the unverified-outage
# streak. Unit-tests verify_finalized_pr directly with an already-present stop-file.
# ---------------------------------------------------------------------------
test_mid_grace_stop_is_aborted() {
  local d out
  d="$(new_case_dir)"
  touch "$d/stop"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  local out1
  out="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=5 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  # roborev 1838: also cover MISMATCH_RETRIES=1 — the loop never reaches the mid-loop
  # guard, so the final-read stop-file re-check is what defuses the forgery verdict.
  out1="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "aborted" && "$out1" == "aborted" ]]; then
    pass "mid-grace stop: shutdown cuts grace short → aborted (neutral; retries=5 AND retries=1)"
  else
    fail "mid-grace-stop: got retries5='$out' retries1='$out1' (expected aborted, NOT mismatch:* / unverified)"
  fi
}

# ---------------------------------------------------------------------------
# F1 (stale-lock reclaim double-acquire race) note: the fix makes reclaim
# atomic via `mv "$LOCK" "$LOCK.stale.$$" && rm -rf "$LOCK.stale.$$"` instead
# of `rm -rf "$LOCK"; mkdir "$LOCK"`. Reliably reproducing the ORIGINAL race
# requires two processes hitting the reclaim window at the exact same instant
# — any test harness recreation of that is inherently sleep/timing-dependent
# and would be flaky by construction (the class of test this suite explicitly
# avoids per its <30s/no-sleep-loop design goal). Covered by code inspection
# instead: `mv` on the same filesystem is atomic (POSIX rename(2)), so of two
# racers only one `mv "$LOCK" "$LOCK.stale.$$"` can succeed against a given
# stale directory name; the loser's `mv` fails (source already gone) and it
# falls through to its own `mkdir "$LOCK"`, which fails against the winner's
# fresh lock, hitting the existing loud "failed to acquire lock" exit path.
# No test function here by design — see comment in acquire_lock() itself.

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=== worker-supervisor test suite ==="
test_happy_path_budget_stop
test_breaker_stops_on_abnormal
test_stop_file_honored
test_preflight_load_hold
test_nowork_not_counted
test_single_instance_lock
test_stale_marker_removed
test_repeated_blocked_head_of_queue_stops
test_finalized_missing_pr_is_abnormal
test_journal_escapes_nasty_reason
test_park_seam1_parked_on_owner
test_park_needs_decision_question_in_title
test_unknown_outcome_is_abnormal
test_stuck_on_question_detected
test_prompt_signature_grep
test_stuck_breaks_abnormal_chain
test_repeated_park_same_issue_stops
test_different_issue_parks_do_not_head_block
test_stray_signature_scrollback_is_abnormal
test_genuine_wedge_frozen_is_stuck
test_busy_writing_signature_not_stuck
test_fast_exit_latency
test_finalized_verified_merged_counts
test_finalized_mismatch_open_is_abnormal
test_finalized_unverified_not_counted_no_breaker
test_proc_probe_discriminates_worker_claude
test_leftover_hold_bounded_stops
test_persistent_unverified_stops
test_forged_pr_is_unresolved_mismatch
test_stop_file_honored_mid_hold
test_probe_no_self_match
test_parser_absent_is_unverified
test_pending_automerge_verdict
test_mismatch_grace_absorbs_lag
test_foreign_url_is_unresolved
test_alternating_holds_still_bounded
test_maxhours_only_hold_no_abort
test_transport_notfound_is_unverified
test_python_only_parser_automerge
test_stop_file_honored_mid_grace
test_persistent_pending_automerge_stops
test_healthy_multi_pr_no_false_stop
test_build_hold_uses_loose_bound
test_build_hold_clears_then_proceeds
test_probe_list_derives_from_count_set
test_grace_cap_disabled_semantics
test_mid_grace_stop_is_aborted
echo "=== $PASS_COUNT passed, $FAIL_COUNT failed, $SKIP_COUNT skipped ==="
[[ "$FAIL_COUNT" -eq 0 ]]
