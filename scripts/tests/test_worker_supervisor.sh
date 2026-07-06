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
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
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
{"outcome":"finalized","issue":\$n,"pr":"https://example.invalid/pull/\$n","duration_s":1}
JSON
sleep "$sleep_s"
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
{"outcome":"finalized","issue":\$n,"pr":"https://example.invalid/pull/\$n","duration_s":1}
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
{"outcome":"finalized","issue":1,"pr":"https://example.invalid/pull/1","duration_s":1}
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
  export LOAD_MAX=999999
  export MAX_ISSUES=100
  export MAX_HOURS=8
  export BREAKER_N=3
  export BACKOFF_NOWORK_SECS=1
  export HOLD_POLL_SECS=1
  export MAX_ITER_SECS=10
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
  sleep 2.5
  local invoked_while_high="no"
  [[ -f "$counter" ]] && invoked_while_high="yes"
  hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)

  echo 0 >"$LOAD_CONTROL_FILE"
  local waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  wait "$sup_pid"
  rc=$?

  if [[ "$invoked_while_high" == "no" && "$rc" -eq 0 && -f "$counter" && "$hold_notifies" -eq 1 ]]; then
    pass "preflight: high load holds the spawn (no invoke), then proceeds once clear (1 HOLD notify)"
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
echo "=== $PASS_COUNT passed, $FAIL_COUNT failed ==="
[[ "$FAIL_COUNT" -eq 0 ]]
