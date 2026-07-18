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
{"outcome":"finalized","issue":78,"pr":"https://example.invalid/pull/78","duration_s":1}
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
{"outcome":"finalized","issue":88,"pr":"https://example.invalid/pull/88","duration_s":1}
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
{"outcome":"finalized","issue":91,"pr":"https://example.invalid/pull/91","duration_s":1}
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
{"outcome":"finalized","issue":43,"pr":"https://example.invalid/pull/43","duration_s":1}
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
{"outcome":"finalized","issue":95,"pr":"https://example.invalid/pull/95","duration_s":1}
JSON
fi
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
  export STUCK_POLL_SECS=1
  unset LOAD_CONTROL_FILE 2>/dev/null || true
  unset WORKER_CMD 2>/dev/null || true
  # Claim stamping (issue #2655) OFF by default so most tests stay focused; the
  # dedicated claim tests set a hermetic CLAIM_CMD stub that logs its args.
  export CLAIM_CMD=""
  unset HEARTBEAT_MACHINE 2>/dev/null || true
}

jline_count() { grep -c "$2" "$1" 2>/dev/null || true; }

# A hermetic claim-heartbeat.sh stand-in: append "<subcmd> <args...>" to
# $CLAIM_LOG on every call, always succeed. Lets a test assert the supervisor
# invoked `stamp`/`reap` with the right shape, without any origin/network.
write_claim_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
exit 0
EOF
  chmod +x "$path"
}

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
# Test 23 (#2655): the supervisor STAMPS refs/machine-claims/<machine> before each spawn
# and CLEARS (reap) it on a clean exit — via CLAIM_CMD, mechanically, without the
# worker LLM. A hermetic CLAIM_CMD stub logs every invocation; we assert one
# `stamp <issue> <pid>` per iteration and exactly one `reap <machine>` at stop.
# ---------------------------------------------------------------------------
test_claim_stamp_each_iter_and_clear_on_exit() {
  local d counter jf rc stamps reaps
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # 2 finalized iterations => 2 stamps; a clean budget stop => exactly 1 reap.
  stamps=$(grep -c '^stamp ' "$CLAIM_LOG" 2>/dev/null || true)
  reaps=$(grep -c '^reap testbox' "$CLAIM_LOG" 2>/dev/null || true)
  # Every stamp carries the SUPERVISOR pid as the 3rd token (numeric).
  local well_formed="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    [[ "$line" =~ ^stamp\ [0-9]+\ [0-9]+$ ]] || well_formed="no"
  done < <(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null)
  if [[ "$rc" -eq 0 && "$stamps" -eq 2 && "$reaps" -eq 1 && "$well_formed" == "yes" ]]; then
    pass "claim: stamp per iteration (with pid) + one reap on clean exit"
  else
    fail "claim: rc=$rc stamps=$stamps reaps=$reaps well_formed=$well_formed (see $CLAIM_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24 (#2655): the NEXT spawn's claim stamp carries the issue LEARNED from a
# non-finalized (blocked) marker — so the reaper's open-PR guard tracks the real
# issue. Iter 1 blocks issue 88; iter 2's stamp must name issue 88 (before the
# head-block guard stops the run on the 2nd consecutive block).
# ---------------------------------------------------------------------------
test_claim_issue_learned_from_marker() {
  local d rc second_stamp
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # Iter1 stamp = "stamp 0 <pid>" (issue unknown); iter2 stamp = "stamp 88 <pid>"
  # (learned from iter1's blocked marker). Grab the 2nd stamp line.
  second_stamp=$(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null | sed -n '2p')
  if [[ "$rc" -eq 0 ]] && printf '%s' "$second_stamp" | grep -qE '^stamp 88 [0-9]+$'; then
    pass "claim: issue learned from a blocked marker names the next stamp (issue 88)"
  else
    fail "claim-learn: rc=$rc second_stamp='$second_stamp' (see $CLAIM_LOG)"
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
test_claim_stamp_each_iter_and_clear_on_exit
test_claim_issue_learned_from_marker
echo "=== $PASS_COUNT passed, $FAIL_COUNT failed ==="
[[ "$FAIL_COUNT" -eq 0 ]]
