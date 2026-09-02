#!/usr/bin/env bash
# Regression test for issue #3755: the FULL gate's DISK ADMISSION is evaluated at
# LAUNCH and RE-EVALUATED AT SLOT GRANT, using ONE predicate.
#
# THE DEFECT. A full gate admitted with 167G free can sit an hour in the #1825 queue
# and begin building at 30G — the whole queue wait wasted, the build aborting into a
# floor while still HOLDING the slot a peer could have used. An admission test taken
# at launch says nothing about the moment the resource is actually consumed, which is
# slot grant.
#
# WHAT IS EXERCISED, and how it is made evidence rather than a proxy:
#
#   (a) AC5 — a REAL agent-gate.sh process that measures ABOVE the bar at launch,
#       GENUINELY QUEUES behind a peer holding the only slot, and measures BELOW the
#       bar when the slot is granted, must REFUSE: exit non-zero, emit the named
#       `disk-admission: FAIL-CLOSED (#3755)` line + `RESULT: FAIL`, and NEVER BEGIN
#       WORK. The never-began-work half is asserted AFFIRMATIVELY, by a differential:
#       the same harness with readings that stay high DOES reach its work phase and
#       drops the stub's "I am working" marker, and the refusing run never drops it.
#       A bare non-zero exit would prove nothing — an unrelated breakage produces the
#       same exit code — so every negative case here is paired with that control.
#
#   (b) The bar's SOURCE token (default|pinned|invalid|clamped), the #3414
#       `cpu-budget:` idiom: an UNSET variable and a MIS-SET one are different
#       operational facts and `${VAR:-40}` renders them identically.
#
#   (c) UNMEASURED (df absent / df failing / df output unparsable) is DECLARED in the
#       emitted line and NON-FATAL at both moments — never a silent permissive branch.
#
#   (d) The LAUNCH evaluation is ADVISORY: a run reading BELOW the bar at launch and
#       ABOVE it at slot grant PROCEEDS. That asymmetry is deliberate (a low launch
#       reading can be freed by the very peer gate we are about to queue behind), so
#       it is pinned rather than left to be "simplified" later.
#
# HOW THE READINGS ARE DRIVEN — a PATH-shim `df`, never a seam in the shipped gate.
# Doctrine forbids a test-only override in agent-gate.sh (an override is settable by
# the party it constrains). So this test puts a scripted `df` ahead of the real one on
# the child gate's PATH, exactly as the feature-matrix annotation guard puts a
# recording `cargo` there. The gate is UNMODIFIED for testing.
#
# The vehicle is the gate's existing test-only stub mode (CQLITE_GATE_STUB_RUNDIR,
# #1825): it acquires a REAL slot through the REAL acquire_gate_slot, drops a per-PID
# marker while "working", sleeps, and exits without running a component. It is reached
# BEFORE the #3544 component-set pre-flight, so no case here touches the network.
#
# Run standalone:   bash scripts/tests/test_agent_gate_disk_admission.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
SKIP=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
# A control that cannot RUN on this host is reported, never counted as a pass: a green
# derived from a control's absence is the shape this repo's doctrine exists to forbid.
skip() { printf 'skip - %s\n' "$1"; SKIP=$((SKIP + 1)); }

# df_calls <case-label>: how many times the shim was invoked by that run. An integer
# always, never empty — "measured once" and "measured twice" is the fact under test.
df_calls() {
  local n; n=$(cat "$tmp/$1.dfstate" 2>/dev/null || printf '0')
  case "$n" in ''|*[!0-9]*) n=0 ;; esac
  printf '%s' "$n"
}

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP - no python3 on PATH (the #1825 slot cap this test drives needs it)"
  exit 0
fi

# The bar's source token is decided from whether CQLITE_GATE_MIN_FREE_GB is SET, so a
# value inherited from the caller's environment would change what several cases observe.
unset CQLITE_GATE_MIN_FREE_GB

tmp=$(mktemp -d "${TMPDIR:-/tmp}/gate-disk-adm.XXXXXX")
cleanup() { kill $(jobs -p) 2>/dev/null; rm -rf "$tmp"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# The PATH-shim `df`. Each invocation consumes the NEXT line of $DF_SHIM_SCRIPT
# (the last line repeats once exhausted, so a case need only script the readings
# it cares about) and renders it as POSIX `df -Pk` output:
#   * an integer            -> that many AVAILABLE KiB
#   * FAIL                  -> exit 1 (df ran and could not answer)
#   * NOTFOUND              -> exit 127, which is EXACTLY what a shell reports for an
#                              ABSENT command, so this drives the df-unavailable branch
#                              on the same observable a df-less PATH would produce
#   * GARBAGE               -> well-formed columns with a NON-NUMERIC Available
#   * RAW <data-line>       -> that EXACT data line, verbatim, under the standard
#                              header — how the space-bearing / capacity-anchor cases
#                              deliver a payload no field-index parse can read
# A per-run state file keeps the counter, so concurrent runs never share one.
# ---------------------------------------------------------------------------
mkdir -p "$tmp/shim"
# A PATH-shim `cargo` used ONLY by the cases that need the resolved target dir to CHANGE
# between the two measurements. It lives in its own directory so the default cases keep
# using the real cargo (Case K's whole point).
mkdir -p "$tmp/cargoshim"
# NON-`metadata` invocations are delegated to the REAL cargo and consume NO scripted
# line. Found the hard way: the gate's accelerator detection runs `cargo nextest
# --version` at startup, which ate script line 1, so BOTH resolutions read the same
# value and the "subject moved" case silently became a "subject unchanged" case — a
# control that did not control, the third instance of that family on this branch.
_REAL_CARGO=$(command -v cargo 2>/dev/null || printf '/nonexistent/cargo')
cat > "$tmp/cargoshim/cargo" <<CSHIM
#!/usr/bin/env bash
if [ "\${1:-}" != metadata ]; then exec "$_REAL_CARGO" "\$@"; fi
CSHIM
cat >> "$tmp/cargoshim/cargo" <<'CSHIM'
n=$(cat "$CARGO_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1)); printf '%s' "$n" > "$CARGO_SHIM_STATE"
val=$(sed -n "${n}p" "$CARGO_SHIM_SCRIPT" 2>/dev/null)
[ -n "$val" ] || val=$(tail -n 1 "$CARGO_SHIM_SCRIPT" 2>/dev/null)
printf '{"target_directory":"%s","packages":[],"workspace_members":[],"version":1}\n' "$val"
CSHIM
chmod +x "$tmp/cargoshim/cargo"

cat > "$tmp/shim/df" <<'SHIM'
#!/usr/bin/env bash
n=$(cat "$DF_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1))
printf '%s' "$n" > "$DF_SHIM_STATE"
printf 'call %s: %s\n' "$n" "$*" >> "$DF_SHIM_STATE.log"
val=$(sed -n "${n}p" "$DF_SHIM_SCRIPT" 2>/dev/null)
[ -n "$val" ] || val=$(tail -n 1 "$DF_SHIM_SCRIPT" 2>/dev/null)
case "$val" in
  FAIL) exit 1 ;;
  NOTFOUND) exit 127 ;;
  'RAW '*)
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
    printf '%s\n' "${val#RAW }"
    exit 0 ;;
  GARBAGE)
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
    printf '/dev/shim 999999999 1 not-a-number 1%% /shimfs\n'
    exit 0 ;;
esac
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 %s 1%% /shimfs\n' "$val"
SHIM
chmod +x "$tmp/shim/df"

KIB_PER_GIB=1048576
gib_kib() { printf '%s' "$(( $1 * KIB_PER_GIB ))"; }

# df_script <name> <reading...>: write a shim script file, return its path via echo.
df_script() {
  local name="$1"; shift
  local f="$tmp/df-$name.script"
  : > "$f"
  local r
  for r in "$@"; do printf '%s\n' "$r" >> "$f"; done
  printf '%s' "$f"
}

# marker_count <rundir>: how many stub runs are advertising "I am working" right now.
marker_count() {
  local d="$1" c=0 f
  for f in "$d"/holding.*; do
    [ -e "$f" ] && c=$(( c + 1 ))
  done
  printf '%s' "$c"
}

# run_stub_gate <case> <df-script> [env assignments...] -> backgrounds a stub gate.
# Sets, for the caller: RS_PID, RS_RUNDIR, RS_SUMMARY, RS_ERR.
RS_PATH_PREFIX=""
run_stub_gate() {
  local case_name="$1" script="$2"; shift 2
  RS_RUNDIR="$tmp/$case_name.run"; mkdir -p "$RS_RUNDIR"
  RS_SUMMARY="$tmp/$case_name.summary.txt"
  RS_ERR="$tmp/$case_name.err"
  # RS_PATH_PREFIX prepends to the child's PATH. It is a dedicated variable rather than a
  # `PATH=` in "$@" because `env` applies assignments LEFT TO RIGHT and the function's own
  # PATH= comes last, so a caller-supplied one is silently overridden — which is exactly
  # how the k-nocargo case first ran against the REAL cargo and reported a resolution it
  # was written to prove impossible.
  env "$@" \
    PATH="${RS_PATH_PREFIX:+$RS_PATH_PREFIX:}$tmp/shim:$PATH" \
    DF_SHIM_SCRIPT="$script" \
    DF_SHIM_STATE="$tmp/$case_name.dfstate" \
    AGENT_GATE_SUMMARY_FILE="$RS_SUMMARY" \
    CQLITE_GATE_STUB_RUNDIR="$RS_RUNDIR" \
    CQLITE_GATE_POLL_SECS=0.3 \
    bash "$GATE" >"$tmp/$case_name.out" 2>"$RS_ERR" &
  RS_PID=$!
}

# watch_until_exit <pid> <rundir> <timeout_s>: poll the rundir while <pid> runs, then
# reap it. Sets WX_STATUS (exit status), WX_MARKERS (the MAX number of "I am working"
# markers ever observed — the AFFIRMATIVE evidence for "did this run begin its work
# phase") and WX_TIMEDOUT.
#
# Sets GLOBALS rather than printing: a `$( ... )` capture runs in a SUBSHELL, where
# `wait <pid>` cannot reap a job of the PARENT shell and silently yields 127.
#
# THE TIMEOUT IS REAL — THERE IS NO UNBOUNDED `wait` ON ANY PATH (roborev job 323).
# A version that stopped POLLING at the deadline and then called a bare `wait` would
# hang FOREVER on a deadlocked gate, and this file runs inside `tooling-tests`, i.e. in
# the gate of record for every lane on the fleet. A hang there is worse than a failure:
# it burns the machine-wide slot with no verdict — which is the exact resource-waste
# #3755 exists to remove, reintroduced by its own test. So expiry is detected
# explicitly, the child is terminated, reaped on a BOUNDED path, and reported as a
# DISTINCT TIMEOUT status (124, the `timeout(1)` convention) — never a silent pass and
# never a generic FAIL.
#
# The signal goes to THE PID WE STILL HOLD, never to a process GROUP (roborev job 279):
# once bash has reaped the leader that pgid can be recycled, and on a four-lane box the
# group most likely to inherit it is a PEER LANE'S GATE. And no `wait` is issued after
# the kill: a process wedged in uninterruptible sleep would make even that call
# unbounded, so the reap is a bounded poll and a survivor is left to the EXIT trap.
WX_STATUS=0
WX_MARKERS=0
WX_TIMEDOUT=0
watch_until_exit() {
  local pid="$1" rundir="$2" timeout="$3"
  local deadline=$(( $(date +%s) + timeout )) max=0 c expired=0 i=0
  WX_TIMEDOUT=0
  while :; do
    c=$(marker_count "$rundir")
    [ "$c" -gt "$max" ] && max="$c"
    kill -0 "$pid" 2>/dev/null || break
    if [ "$(date +%s)" -ge "$deadline" ]; then expired=1; break; fi
    sleep 0.05
  done
  c=$(marker_count "$rundir"); [ "$c" -gt "$max" ] && max="$c"
  WX_MARKERS="$max"
  if [ "$expired" -eq 0 ]; then
    # Bounded by construction: the loop only leaves here once `kill -0` says the child
    # is gone, so bash already holds its status and `wait` returns immediately.
    wait "$pid"; WX_STATUS=$?
    return 0
  fi
  WX_TIMEDOUT=1
  WX_STATUS=124
  kill -TERM "$pid" 2>/dev/null
  i=0
  while [ "$i" -lt 40 ] && kill -0 "$pid" 2>/dev/null; do sleep 0.05; i=$((i + 1)); done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null
    i=0
    while [ "$i" -lt 40 ] && kill -0 "$pid" 2>/dev/null; do sleep 0.05; i=$((i + 1)); done
  fi
  return 0
}

# assert_no_timeout <label>: a TIMEOUT is its own named failure. Called after every
# watch_until_exit, so a hung child is reported as a hang rather than surfacing as a
# confusing cascade of value assertions against a run that never finished.
assert_no_timeout() {
  if [ "$WX_TIMEDOUT" -eq 0 ]; then
    return 0
  fi
  bad "TIMEOUT: $1 — the child gate did not exit within its deadline; it was terminated"
  return 1
}

# grep_line <file> <pattern>: print the first matching line (empty when none).
grep_line() { grep -m1 -E "$2" "$1" 2>/dev/null; }

# ---------------------------------------------------------------------------
# Self-check of the harness's own timeout path (roborev job 323, finding 2).
#
# This case IS the positive control for boundedness: under the pre-fix helper — which
# stopped POLLING at the deadline and then called a bare `wait` — it would hang
# FOREVER, so the fact that this file reaches its final tally at all is the property
# being demonstrated. Deliberately no elapsed-time assertion: "it returned" is the
# observable, and a wall-clock threshold in a correctness path is a flake generator.
# ---------------------------------------------------------------------------
sleep 300 &
_hang_pid=$!
mkdir -p "$tmp/hang.run"
watch_until_exit "$_hang_pid" "$tmp/hang.run" 1
if [ "$WX_TIMEDOUT" -eq 1 ] && [ "$WX_STATUS" -eq 124 ]; then
  ok "harness: a child that outlives its deadline is reported as a DISTINCT TIMEOUT (status 124)"
else
  bad "harness: a hung child was not reported as a timeout (timedout=$WX_TIMEDOUT status=$WX_STATUS)"
fi
if kill -0 "$_hang_pid" 2>/dev/null; then
  bad "harness: the timed-out child is still alive — the deadline terminated nothing"
  kill -KILL "$_hang_pid" 2>/dev/null
else
  ok "harness: the timed-out child was terminated (by pid, never by process group)"
fi
wait "$_hang_pid" 2>/dev/null

HIGH=$(gib_kib 200)
LOW=$(gib_kib 10)

# ===========================================================================
# Case A (AC5): ABOVE the bar at launch, QUEUED behind a peer, BELOW the bar at
# slot grant -> refuses, releases the slot, never begins work.
# ===========================================================================
a_slots="$tmp/a-slots"
peer_script=$(df_script a-peer "$HIGH")
run_stub_gate a-peer "$peer_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=15
a_peer_pid=$RS_PID; a_peer_run=$RS_RUNDIR

# The peer must actually HOLD the only slot before the subject launches, AND must go
# on holding it for longer than the subject's own startup takes on a loaded box — or
# the subject finds a free slot, never queues, and this case silently degrades into
# case B: a green that measured the wrong thing. Hence the generous peer hold; the
# subject exits the moment the slot is granted, so the hold only bounds the wait.
a_peer_holding=0
a_deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$a_deadline" ]; do
  [ "$(marker_count "$a_peer_run")" -ge 1 ] && { a_peer_holding=1; break; }
  sleep 0.1
done
if [ "$a_peer_holding" -eq 1 ]; then
  ok "AC5 setup: peer holds the only slot (N=1) before the subject launches"
else
  bad "AC5 setup: peer never acquired the slot — the subject would not have queued"
fi

subj_script=$(df_script a-subj "$HIGH" "$LOW")
run_stub_gate a-subj "$subj_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=4
a_subj_pid=$RS_PID; a_subj_run=$RS_RUNDIR; a_subj_sum=$RS_SUMMARY; a_subj_err=$RS_ERR

watch_until_exit "$a_subj_pid" "$a_subj_run" 180; a_status=$WX_STATUS; a_markers=$WX_MARKERS
assert_no_timeout "AC5 subject"
wait "$a_peer_pid" 2>/dev/null

if grep -q 'waiting for gate slot' "$a_subj_err" 2>/dev/null; then
  ok "AC5: the subject GENUINELY QUEUED for the slot (not a free-slot fast path)"
else
  bad "AC5: no queue notice on the subject's stderr — it did not queue behind the peer"
fi
if [ "$a_status" -ne 0 ]; then
  ok "AC5: below-bar at slot grant REFUSES (exit $a_status)"
else
  bad "AC5: below-bar at slot grant exited 0 — the gate was admitted into a floor"
fi
if [ "$a_markers" -eq 0 ]; then
  ok "AC2/AC5: the refusing run NEVER began its work phase (0 work markers observed)"
else
  bad "AC2/AC5: the refusing run began working ($a_markers marker(s) observed)"
fi
a_line=$(grep_line "$a_subj_sum" '^disk-admission: ')
case "$a_line" in
  'disk-admission: FAIL-CLOSED (#3755)'*)
    ok "AC4: distinct NAMED outcome in the SUMMARY: ${a_line:0:60}…" ;;
  '') bad "AC3/AC4: no disk-admission: line in the refusal SUMMARY ($a_subj_sum)" ;;
  *)  bad "AC4: refusal SUMMARY carries the wrong verdict: $a_line" ;;
esac
# The shared assembly is IDEMPOTENT: this block's builder passes the line explicitly, so
# a non-dropping assembly would emit it TWICE.
a_count=$(grep -c '^disk-admission: ' "$a_subj_sum" 2>/dev/null || printf '0')
if [ "$a_count" -eq 1 ]; then
  ok "AC3: exactly ONE disk-admission: line in the block (the shared assembly de-duplicates)"
else
  bad "AC3: expected exactly 1 disk-admission: line, found $a_count"
fi
# AC3: value observed, bar applied, and BOTH moments named.
for needle in 'post-slot 10.0GiB' 'bar 40GiB(default)' 'launch 200.0GiB' 'evaluated 2x'; do
  case "$a_line" in
    *"$needle"*) ok "AC3: refusal line states '$needle'" ;;
    *)           bad "AC3: refusal line omits '$needle': $a_line" ;;
  esac
done
case "$a_line" in
  *'slot RELEASED'*) ok "AC2: the refusal line reports the slot RELEASED" ;;
  *)                 bad "AC2: the refusal line does not report a slot release: $a_line" ;;
esac
# AC4: the terminal RESULT stays the pollable FAIL — never a new token that would
# break the mandated `grep -qE 'RESULT: (PASS|FAIL)'` completion probe (#3041).
if grep -qx 'RESULT: FAIL' "$a_subj_sum" 2>/dev/null; then
  ok "AC4: RESULT: FAIL (the #3041 completion probe still fires on a refusal)"
else
  bad "AC4: refusal SUMMARY lacks an exact 'RESULT: FAIL' line"
  grep -E '^RESULT:' "$a_subj_sum" 2>/dev/null
fi
if grep -q '^refusal: disk admission (#3755) — refused at SLOT GRANT;' "$a_subj_sum" 2>/dev/null; then
  ok "AC4: the refusal is NAMED on its own refusal: line, and NAMES THE MOMENT"
else
  bad "AC4: no named 'refusal: disk admission (#3755) — refused at SLOT GRANT' line"
  grep -m1 '^refusal:' "$a_subj_sum" 2>/dev/null
fi
# AC2, behavioural half: the slot is usable by a follow-up run immediately after.
follow_script=$(df_script a-follow "$HIGH")
run_stub_gate a-follow "$follow_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; f_status=$WX_STATUS; f_markers=$WX_MARKERS
assert_no_timeout "AC2 follow-up run"
if [ "$f_status" -eq 0 ] && [ "$f_markers" -ge 1 ]; then
  ok "AC2: the released slot is immediately usable by a follow-up run"
else
  bad "AC2: follow-up run did not get the slot (exit $f_status, markers $f_markers)"
fi

# ===========================================================================
# Case B (POSITIVE CONTROL): the SAME harness with readings that stay ABOVE the
# bar proceeds past the check and DOES begin work. Without this, case A's
# non-zero exit is not evidence — any breakage produces the same exit code.
# ===========================================================================
b_slots="$tmp/b-slots"
b_peer_script=$(df_script b-peer "$HIGH")
run_stub_gate b-peer "$b_peer_script" \
  CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=15
b_peer_pid=$RS_PID; b_peer_run=$RS_RUNDIR
b_deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$b_deadline" ]; do
  [ "$(marker_count "$b_peer_run")" -ge 1 ] && break
  sleep 0.1
done
b_subj_script=$(df_script b-subj "$HIGH" "$HIGH")
run_stub_gate b-subj "$b_subj_script" \
  CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=3
b_subj_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; b_status=$WX_STATUS; b_markers=$WX_MARKERS
assert_no_timeout "positive control"
wait "$b_peer_pid" 2>/dev/null
if [ "$b_status" -eq 0 ] && [ "$b_markers" -ge 1 ]; then
  ok "CONTROL: above-bar at BOTH moments proceeds and DOES begin work (exit 0, $b_markers marker(s))"
else
  bad "CONTROL: above-bar run did not proceed (exit $b_status, markers $b_markers)"
fi
b_line=$(grep_line "$b_subj_err" '^agent-gate: disk-admission: ')
case "$b_line" in
  *'disk-admission: PASS'*'evaluated 2x'*'launch 200.0GiB'*'post-slot 200.0GiB'*)
    ok "AC3: the PASS line names BOTH evaluations affirmatively" ;;
  *) bad "AC3: PASS line malformed or missing: ${b_line:-<none>}" ;;
esac

# ===========================================================================
# Case C: the LAUNCH evaluation is ADVISORY — below at launch, above at slot
# grant PROCEEDS (design point: a low launch reading can be freed by the very
# peer we are about to queue behind, so refusing there is a FALSE refusal).
# ===========================================================================
c_script=$(df_script c "$LOW" "$HIGH")
run_stub_gate c "$c_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/c-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
c_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; c_status=$WX_STATUS; c_markers=$WX_MARKERS
assert_no_timeout "launch-advisory case"
if [ "$c_status" -eq 0 ] && [ "$c_markers" -ge 1 ]; then
  ok "LAUNCH ADVISORY: below-at-launch/above-at-grant PROCEEDS (exit 0)"
else
  bad "LAUNCH ADVISORY: a low LAUNCH reading refused the run (exit $c_status)"
fi
c_line=$(grep_line "$c_err" '^agent-gate: disk-admission: ')
case "$c_line" in
  *'launch 10.0GiB(BELOW BAR)'*'post-slot 200.0GiB'*)
    ok "LAUNCH ADVISORY: the low launch reading is DECLARED in the line" ;;
  *) bad "LAUNCH ADVISORY: line does not declare the low launch reading: ${c_line:-<none>}" ;;
esac

# ===========================================================================
# Case D: UNMEASURED is DECLARED and NON-FATAL at both moments.
# ===========================================================================
run_unmeasured_case() {
  local label="$1" reading="$2" why="$3"
  local s; s=$(df_script "$label" "$reading")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
  local err=$RS_ERR st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
    ok "UNMEASURED($why): non-fatal — the run proceeded and began work"
  else
    bad "UNMEASURED($why): the run was refused (exit $st, markers $mk)"
  fi
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"disk-admission: UNMEASURED ($why)"*'NOT APPLIED'*)
      ok "UNMEASURED($why): DECLARED in the line, bar NOT APPLIED" ;;
    *) bad "UNMEASURED($why): not declared: ${line:-<none>}" ;;
  esac
}
run_unmeasured_case d-fail    FAIL    df-failed
run_unmeasured_case d-garbage GARBAGE df-unparsable

# df ABSENT: a shell reports rc 127 for an absent command, so the NOTFOUND reading
# drives exactly the branch a df-less PATH would. The probe must ALSO not leak a
# `command not found` line onto the gate's own stderr — the minimal-PATH case in
# test_agent_gate_summary.sh reads any such line as a missing-tool defect.
d_script=$(df_script d-absent NOTFOUND)
run_stub_gate d-absent "$d_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/d-absent-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
d_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; d_status=$WX_STATUS; d_markers=$WX_MARKERS
assert_no_timeout "df-absent case"
d_line=$(grep_line "$d_err" '^agent-gate: disk-admission: ')
if [ "$d_status" -eq 0 ] && [ "$d_markers" -ge 1 ]; then
  ok "UNMEASURED(df-unavailable): non-fatal — the run proceeded and began work"
else
  bad "UNMEASURED(df-unavailable): refused the run (exit $d_status, markers $d_markers)"
fi
case "$d_line" in
  *'disk-admission: UNMEASURED (df-unavailable)'*'NOT APPLIED'*)
    ok "UNMEASURED(df-unavailable): DECLARED in the line, bar NOT APPLIED" ;;
  *) bad "UNMEASURED(df-unavailable): not declared: ${d_line:-<none>}" ;;
esac
if grep -q 'command not found' "$d_err" 2>/dev/null; then
  bad "UNMEASURED: the probe leaked 'command not found' onto the gate's stderr"
  grep -m3 'command not found' "$d_err"
else
  ok "UNMEASURED: no 'command not found' leaked onto the gate's stderr"
fi

# ===========================================================================
# Case E: the bar's SOURCE token (#3414 idiom). unset|pinned|invalid|clamped.
# ===========================================================================
bar_case() {
  local label="$1" expect="$2"; shift 2
  local s; s=$(df_script "$label" "$HIGH")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1 "$@"
  local err=$RS_ERR st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"bar $expect"*) ok "bar-source: $label -> 'bar $expect'" ;;
    *)               bad "bar-source: $label expected 'bar $expect', got: ${line:-<none>}" ;;
  esac
}
bar_case e-unset   '40GiB(default)'
bar_case e-pinned  '50GiB(pinned)'   CQLITE_GATE_MIN_FREE_GB=50
bar_case e-frac    '0.5GiB(pinned)'  CQLITE_GATE_MIN_FREE_GB=0.5
bar_case e-empty   '40GiB(invalid)'  CQLITE_GATE_MIN_FREE_GB=
bar_case e-nonnum  '40GiB(invalid)'  CQLITE_GATE_MIN_FREE_GB=abc
bar_case e-neg     '0GiB(clamped)'   CQLITE_GATE_MIN_FREE_GB=-5

# A pinned bar ABOVE the reading refuses even with no contention — the same
# predicate, the same disposition, reached without a queue.
e_script=$(df_script e-above "$HIGH")
run_stub_gate e-above "$e_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/e-above-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CQLITE_GATE_MIN_FREE_GB=500
e_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; e_status=$WX_STATUS; e_markers=$WX_MARKERS
assert_no_timeout "pinned-bar refusal"
if [ "$e_status" -ne 0 ] && [ "$e_markers" -eq 0 ]; then
  ok "bar-source: a PINNED bar above the reading refuses and never begins work"
else
  bad "bar-source: pinned bar 500GiB did not refuse (exit $e_status, markers $e_markers)"
fi
if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$e_sum" 2>/dev/null; then
  ok "bar-source: the pinned-bar refusal emits the same named line"
else
  bad "bar-source: pinned-bar refusal SUMMARY lacks the named line"
fi

# ===========================================================================
# Case F: --lite is EXEMPT — the cap exempts it, so the admission probe must not
# run for it either (it builds nothing that fills a disk the way a full gate does,
# and it is never queued). Asserted on the probe's own call log: ZERO df calls.
# ===========================================================================
f_state="$tmp/f.dfstate"
f_script=$(df_script f "$HIGH")
env PATH="$tmp/shim:$PATH" DF_SHIM_SCRIPT="$f_script" DF_SHIM_STATE="$f_state" \
  AGENT_GATE_SUMMARY_FILE="$tmp/f.summary.txt" \
  CQLITE_GATE_STUB_RUNDIR="$tmp/f.run" CQLITE_GATE_STUB_SLEEP=1 \
  CQLITE_GATE_SLOTS_DIR="$tmp/f-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  bash "$GATE" --lite >"$tmp/f.out" 2>"$tmp/f.err"
f_calls=$(cat "$f_state" 2>/dev/null || printf '0')
case "$f_calls" in ''|*[!0-9]*) f_calls=0 ;; esac
if [ "$f_calls" -eq 0 ]; then
  ok "exemption: the admission probe made 0 df calls on a non-full-gate run"
else
  bad "exemption: the admission probe ran on a non-full-gate run ($f_calls df call(s))"
fi

# ===========================================================================
# Case G (roborev job 323, finding 1): the df parse is ANCHORED ON THE CAPACITY
# FIELD, so a source name or mount point containing SPACES cannot shift a column
# into $4 and admit a run that is below the bar.
#
# This is a FALSE-PASS class, not a cosmetic one: a shifted $4 lands on the USED
# value, which is large and NUMERIC, so a "is it a number" validation succeeds and
# the gate is ADMITTED BELOW THE FLOOR. Every negative case below therefore carries
# the PRE-FIX PARSE as a POSITIVE CONTROL — the defective one-liner is reproduced
# verbatim against the same payload and must be shown to yield a number that WOULD
# have cleared the bar. A test that merely passes after the fix does not establish
# that the defect was ever reachable.
# ===========================================================================
BAR_KIB=$(gib_kib 40)

# prefix_parse_admits <payload>: the PRE-FIX parse (`awk 'END { print $4 }'`),
# reproduced exactly. Exit 0 when it yields a numeric value at or above the 40GiB
# bar, i.e. when the old code would have ADMITTED this payload.
prefix_parse_admits() {
  local v
  v=$(printf '%s\n' "$1" | awk 'END { print $4 }' 2>/dev/null)
  case "$v" in ''|*[!0-9]*) return 1 ;; esac
  [ "$v" -ge "$BAR_KIB" ]
}

# raw_case <label> <payload> <kind> <expect-substring>
#   kind refuse     -> exit non-zero, 0 work markers, FAIL-CLOSED line in the SUMMARY
#   kind unmeasured -> exit 0, work began, UNMEASURED line on stderr
#   kind pass       -> exit 0, work began, PASS line on stderr
raw_case() {
  local label="$1" payload="$2" kind="$3" expect="$4"
  local s; s=$(df_script "$label" "RAW $payload")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=2
  local err=$RS_ERR sum=$RS_SUMMARY st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$kind" in
    refuse)
      if [ "$st" -ne 0 ] && [ "$mk" -eq 0 ]; then
        ok "df-anchor/$label: REFUSED and never began work (exit $st)"
      else
        bad "df-anchor/$label: ADMITTED a below-bar payload (exit $st, markers $mk)"
      fi
      if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$sum" 2>/dev/null; then
        ok "df-anchor/$label: the refusal carries the named line"
      else
        bad "df-anchor/$label: no FAIL-CLOSED line in the SUMMARY"
      fi ;;
    unmeasured)
      if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
        ok "df-anchor/$label: UNMEASURED is non-fatal — the run proceeded"
      else
        bad "df-anchor/$label: an unparsable payload refused the run (exit $st)"
      fi ;;
    pass)
      if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
        ok "df-anchor/$label: an above-bar space-bearing payload is ADMITTED (no over-refusal)"
      else
        bad "df-anchor/$label: over-refused a legitimate above-bar payload (exit $st)"
      fi ;;
  esac
  case "$line" in
    *"$expect"*) ok "df-anchor/$label: line states '$expect'" ;;
    *)           bad "df-anchor/$label: line omits '$expect': ${line:-<none>}" ;;
  esac
}

G_SRC='my server:/export vol 999999999 900000000 10485760 90% /data'
G_MNT='/dev/sda1 999999999 900000000 10485760 90% /mnt/my disk'
G_BOTH='my server:/export vol 999999999 900000000 10485760 90% /mnt/my disk'
G_NOCAP='/dev/sda1 999999999 900000000 10485760 - /data'
# A mount PATH ending in `%` is still ONE anchor (`/mnt/50%` does not match
# `^[0-9]+%$`), so it must parse normally — the anchor must not be so eager that an
# ordinary path defeats it.
G_PCTPATH='/dev/sda1 999999999 900000000 10485760 90% /mnt/50%'
# GENUINELY ambiguous: a mount point whose SPACE-SEPARATED tokens include one that IS
# capacity-shaped. Two anchors identify nothing, so the parse must refuse rather than
# pick one — and must NOT fall back to $4, which would reinstate the false pass in
# exactly the payloads that defeat the anchor.
G_TWOCAP='/dev/sda1 999999999 900000000 10485760 90% /mnt/vol 50% spare'
G_HIGH='my server:/export vol 999999999 1 209715200 1% /data'

# The POSITIVE CONTROLS. Without these, the refusals below prove only that something
# refused — not that the old parse would have let these through.
for pl in "$G_SRC" "$G_BOTH"; do
  if prefix_parse_admits "$pl"; then
    ok "df-anchor CONTROL: the PRE-FIX \$4 parse ADMITS this below-bar payload — the defect was reachable"
  else
    bad "df-anchor CONTROL: the PRE-FIX \$4 parse did NOT admit '$pl' — this case does not demonstrate the defect"
  fi
done
# ...and the mount-only payload is the counter-control: $4 is correct there, so that
# case is about the RENDERED mount point, not about admission.
if prefix_parse_admits "$G_MNT"; then
  bad "df-anchor CONTROL: the PRE-FIX parse admits the mount-only payload — the case split is wrong"
else
  ok "df-anchor CONTROL: the PRE-FIX parse reads the mount-only payload correctly (that case tests rendering, not admission)"
fi

raw_case g-space-source "$G_SRC"    refuse     'post-slot 10.0GiB(BELOW BAR)'
raw_case g-space-mount  "$G_MNT"    refuse     'fs /mnt/my disk'
raw_case g-space-both   "$G_BOTH"   refuse     'fs /mnt/my disk'
raw_case g-no-capacity  "$G_NOCAP"  unmeasured 'UNMEASURED (df-unparsable)'
raw_case g-pct-path     "$G_PCTPATH" refuse    'fs /mnt/50%'
raw_case g-two-capacity "$G_TWOCAP" unmeasured 'UNMEASURED (df-unparsable)'
raw_case g-space-ok     "$G_HIGH"   pass       'post-slot 200.0GiB'

# ===========================================================================
# Case H (roborev job 329, finding 1): THE MEASUREMENT IMMEDIATELY PRECEDING THE
# BUILD IS ALWAYS FAIL-CLOSED. A launch measurement is advisory ONLY when a slot
# grant will follow it.
#
# The first draft made post-slot-grant binding and left FIVE paths returning into
# the build with nothing binding in front of them. Two routes are exercised here:
# the cap never engaging (no queue, so the launch reading IS the consumption-moment
# reading), and the daemon dying AFTER the queue (where the launch reading is stale
# by exactly the interval #3755 is about, so it must be RE-TAKEN).
# ===========================================================================

# --- H1: the cap never engages. One reading, and it is BINDING. ---
h1_script=$(df_script h1 "$LOW")
run_stub_gate h1 "$h1_script" \
  CQLITE_GATE_DISABLE_CAP=1 CQLITE_GATE_STUB_SLEEP=2
h1_sum=$RS_SUMMARY; h1_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h1_status=$WX_STATUS; h1_markers=$WX_MARKERS
assert_no_timeout "H1 cap-disabled"
if [ "$h1_status" -ne 0 ] && [ "$h1_markers" -eq 0 ]; then
  ok "H1: cap-inactive + below bar REFUSES and never begins work — the launch reading is BINDING"
else
  bad "H1: a cap-inactive run BUILT below the bar with nothing binding (exit $h1_status, markers $h1_markers)"
fi
h1_line=$(grep_line "$h1_err" '^agent-gate: disk-admission: ')
case "$h1_line" in
  *'FAIL-CLOSED (#3755)'*'evaluated 1x'*'NOT RE-MEASURED'*)
    ok "H1: reported as ONE binding evaluation, not re-measured for the sake of it" ;;
  *) bad "H1: wrong rendering: ${h1_line:-<none>}" ;;
esac
# The PRE-FIX behaviour is exactly the ADVISORY rendering, so its ABSENCE on a binding
# path is the differential: a below-bar cap-inactive run must never render ADVISORY.
case "$h1_line" in
  *ADVISORY*) bad "H1: still renders ADVISORY on a BINDING path — the pre-fix disposition survives" ;;
  *)          ok "H1: no ADVISORY rendering on a binding path (the pre-fix disposition is gone)" ;;
esac
if grep -q '^refusal: disk admission (#3755) — refused at LAUNCH' "$h1_sum" 2>/dev/null; then
  ok "H1: the refusal NAMES the moment it refused at (LAUNCH, not 'post-slot')"
else
  bad "H1: refusal line does not name the LAUNCH moment"
  grep -m1 '^refusal:' "$h1_sum" 2>/dev/null
fi
if [ "$(df_calls h1)" -eq 1 ]; then
  ok "H1: measured exactly ONCE — no queue elapsed, so there is nothing to re-measure"
else
  bad "H1: expected 1 measurement with no queue, got $(df_calls h1)"
fi

# --- H1 CONTROL: same route, above the bar, proceeds. ---
h1c_script=$(df_script h1c "$HIGH")
run_stub_gate h1c "$h1c_script" CQLITE_GATE_DISABLE_CAP=1 CQLITE_GATE_STUB_SLEEP=2
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h1c_status=$WX_STATUS; h1c_markers=$WX_MARKERS
assert_no_timeout "H1 control"
if [ "$h1c_status" -eq 0 ] && [ "$h1c_markers" -ge 1 ]; then
  ok "H1 CONTROL: cap-inactive + above bar PROCEEDS and begins work (the rule does not red correct input)"
else
  bad "H1 CONTROL: cap-inactive above-bar run was refused (exit $h1c_status, markers $h1c_markers)"
fi

# --- H2: the daemon dies AFTER the queue. The stale launch reading must NOT decide. ---
#
# THE INJECTION MUST NOT BE BYPASSABLE BY PRIVILEGE (roborev job 335). The first version
# used `chmod 555` on the slots dir, which a privileged user simply writes through: as
# root the daemon ACQUIRES, the grant-failed path is never taken, and the case fails for
# a reason that has nothing to do with its subject — a control that does not control,
# whose green is nonetheless read as evidence.
#
# Instead `slot.0` is pre-created as a DIRECTORY. The daemon's acquire sweep does
# `os.open(path, O_RDWR|O_CREAT)` on exactly that path (with --slots 1 it is the only
# one it tries) and EISDIR is raised for root and non-root alike; the daemon catches
# only the flock error, so it dies before acquiring. Nothing about the failure depends
# on who is running.
#
# The readings are HIGH then LOW: an implementation that reused the launch reading would
# ADMIT, so the refusal can only have come from the SECOND, fresh measurement.
h2_slots="$tmp/h2-slots"; mkdir -p "$h2_slots/slot.0"
h2_script=$(df_script h2 "$HIGH" "$LOW")
run_stub_gate h2 "$h2_script" \
  CQLITE_GATE_SLOTS_DIR="$h2_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
h2_sum=$RS_SUMMARY; h2_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h2_status=$WX_STATUS; h2_markers=$WX_MARKERS
assert_no_timeout "H2 grant-failed-after-queue"
if grep -q 'slot daemon exited before acquiring' "$h2_err" 2>/dev/null; then
  ok "H2 setup: the run really took the grant-failed-after-queue route"
else
  bad "H2 setup: the grant-failed route was not exercised — this case measured something else"
fi
if [ "$h2_status" -ne 0 ] && [ "$h2_markers" -eq 0 ]; then
  ok "H2: refused on the FRESH post-queue reading (a stale launch reading would have ADMITTED)"
else
  bad "H2: built on a STALE launch reading (exit $h2_status, markers $h2_markers)"
fi
if [ "$(df_calls h2)" -eq 2 ]; then
  ok "H2: measured TWICE — the launch reading was re-taken after the queue"
else
  bad "H2: expected 2 measurements after a queue, got $(df_calls h2)"
fi
h2_line=$(grep_line "$h2_err" '^agent-gate: disk-admission: ')
case "$h2_line" in
  *'evaluated 2x'*'RE-MEASURED after the queue'*)
    ok "H2: the line DECLARES the re-measurement and its cause" ;;
  *) bad "H2: wrong rendering: ${h2_line:-<none>}" ;;
esac
case "$h2_line" in
  *'slot RELEASED'*) bad "H2: claims a slot was RELEASED when none was ever held" ;;
  *'no slot was held'*) ok "H2: the block states honestly that no slot was ever held" ;;
  *) bad "H2: the block says nothing about the slot state: ${h2_line:-<none>}" ;;
esac

# --- H2 CONTROL, the inverse pair: LOW then HIGH on the same route must PROCEED. ---
# Together with H2 this pins that the verdict follows the SECOND reading in BOTH
# directions — a run that simply always refused on this route would pass H2 alone.
h2c_script=$(df_script h2c "$LOW" "$HIGH")
run_stub_gate h2c "$h2c_script" \
  CQLITE_GATE_SLOTS_DIR="$h2_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h2c_status=$WX_STATUS; h2c_markers=$WX_MARKERS
assert_no_timeout "H2 control"
if [ "$h2c_status" -eq 0 ] && [ "$h2c_markers" -ge 1 ]; then
  ok "H2 CONTROL: low-then-high on the same route PROCEEDS — the verdict follows the FRESH reading, not the stale one"
else
  bad "H2 CONTROL: refused despite a fresh above-bar reading (exit $h2c_status, markers $h2c_markers)"
fi

# ===========================================================================
# Case I (roborev job 329, finding 2): the threshold comparison is FLOATING
# POINT — no `printf %d` conversion, which saturates implementation-dependently
# and, in the busybox direction, ADMITS a filesystem that must be refused.
# ===========================================================================

# --- I1: the awk census. The POSITIVE CONTROL for reachability, and the proof the
#     shipped comparison is correct under every awk this host has. ---
I_BAR_HUGE=8796093022208           # 8 EiB, whose KiB value exceeds INT64_MAX
I_AVAIL=209715200                  # 200 GiB available — must be REFUSED against it
i_admits=0; i_broken=0; i_awks=0
for a in awk gawk mawk nawk "busybox awk"; do
  command -v "${a%% *}" >/dev/null 2>&1 || continue
  i_awks=$((i_awks + 1))
  # The PRE-FIX chain, reproduced verbatim: awk %d, then bash's integer `[ -ge ]`.
  v=$($a -v g="$I_BAR_HUGE" 'BEGIN { printf "%d", (g * 1048576) + 0.5 }' 2>/dev/null)
  if [ "$I_AVAIL" -ge "$v" ] 2>/dev/null; then
    i_admits=$((i_admits + 1))
    printf 'info - pre-fix chain under %-12s -> %%d=%s ADMITS (false PASS)\n' "$a" "$v"
  fi
  # rc 2 == bash could not compare at all: a verdict reached by an ERROR, not a measurement.
  [ "$I_AVAIL" -ge "$v" ] 2>/dev/null; rc=$?
  if [ "$rc" -ge 2 ]; then
    i_broken=$((i_broken + 1))
    printf 'info - pre-fix chain under %-12s -> %%d=%s makes bash [ ] ERROR (rc %s)\n' "$a" "$v" "$rc"
  fi
  # The SHIPPED comparison, same inputs, must be exactly "below the bar" everywhere.
  $a -v k="$I_AVAIL" -v g="$I_BAR_HUGE" 'BEGIN { exit ((k + 0) >= (g * 1048576)) ? 0 : 1 }' </dev/null 2>/dev/null
  if [ $? -eq 1 ]; then
    ok "I1: the shipped float comparison is correct under $a (200GiB is BELOW an 8-EiB bar)"
  else
    bad "I1: the shipped float comparison is WRONG under $a"
  fi
done
if [ "$i_awks" -eq 0 ]; then
  skip "I1 CONTROL: no awk implementation on this host — reachability could not be measured"
elif [ "$i_admits" -gt 0 ] || [ "$i_broken" -gt 0 ]; then
  ok "I1 CONTROL: the PRE-FIX %d chain is defective under $((i_admits + i_broken)) of $i_awks awk(s) here ($i_admits ADMIT, $i_broken error out) — the defect was reachable"
else
  skip "I1 CONTROL: none of this host's $i_awks awk(s) reproduces the %d defect — reachability not demonstrable here"
fi

# --- I2/I3: the accepted bar range is STATED, and an over-range bar CLAMPS DOWN. ---
# I3 is the anti-loosening assertion: discarding an over-range bar in favour of the
# 40GiB default would turn a refusal into an ADMISSION of this same 200GiB payload.
bar_case i-max      '1048576GiB(pinned)'      CQLITE_GATE_MIN_FREE_GB=1048576
bar_case i-overmax  '1048576GiB(out-of-range)' CQLITE_GATE_MIN_FREE_GB="$I_BAR_HUGE"

i3_script=$(df_script i3 "$HIGH")
run_stub_gate i3 "$i3_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/i3-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CQLITE_GATE_MIN_FREE_GB="$I_BAR_HUGE"
i3_sum=$RS_SUMMARY; i3_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; i3_status=$WX_STATUS; i3_markers=$WX_MARKERS
assert_no_timeout "I3 over-range bar"
if [ "$i3_status" -ne 0 ] && [ "$i3_markers" -eq 0 ]; then
  ok "I3: an over-range bar still REFUSES a 200GiB filesystem — clamped DOWN, never defaulted (defaulting would ADMIT)"
else
  bad "I3: an over-range bar ADMITTED a 200GiB filesystem (exit $i3_status, markers $i3_markers) — the bar was loosened"
fi
if grep -q 'was NOT used AS SET (out-of-range)' "$i3_err" 2>/dev/null; then
  ok "I3: the unusable bar is named on stderr as an operator action, with the accepted range"
else
  bad "I3: no stderr note naming the out-of-range bar"
fi

# ===========================================================================
# Case J (roborev job 335, Medium): EVERY full-gate SUMMARY carries the line —
# including the EARLY-TERMINAL paths, which no builder of ours ever reaches.
#
# Omission is the one rendering that must never happen: a block with no line at
# all leaves a reader unable to tell "never probed" from "predates the probe"
# from "somebody forgot a call site", and only the third ships a hole.
#
# Driven through a REAL early-terminal path, not a synthetic block: a real full
# gate (no stub) against a corpus-less CQLITE_DATASETS_ROOT, which exits at a
# preflight before any component. WHICH preflight it hits depends on the host
# (the #3544 component-set pre-flight runs BEFORE the probe and needs the
# network; the #2078 fixture preflight runs AFTER it), so this case MEASURES
# which path ran and then asserts the rendering that path is required to carry —
# never a rendering it hoped for.
# ===========================================================================
j_root="$tmp/j-empty-datasets"; mkdir -p "$j_root/sstables"
j_script=$(df_script j "$HIGH" "$HIGH")
j_sum="$tmp/j.summary.txt"; j_err="$tmp/j.err"
env PATH="$tmp/shim:$PATH" \
  DF_SHIM_SCRIPT="$j_script" DF_SHIM_STATE="$tmp/j.dfstate" \
  AGENT_GATE_SUMMARY_FILE="$j_sum" \
  CQLITE_DATASETS_ROOT="$j_root" \
  CQLITE_GATE_SLOTS_DIR="$tmp/j-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$GATE" >"$tmp/j.out" 2>"$j_err" &
j_pid=$!
mkdir -p "$tmp/j.norun"
watch_until_exit "$j_pid" "$tmp/j.norun" 900; j_status=$WX_STATUS
assert_no_timeout "J early-terminal full gate"

j_line=$(grep_line "$j_sum" '^disk-admission: ')
j_count=$(grep -c '^disk-admission: ' "$j_sum" 2>/dev/null || printf '0')
case "$j_count" in ''|*[!0-9]*) j_count=0 ;; esac

# Which early terminal did we actually reach? Measured, then asserted against.
j_path=""
grep -q '^missing-fixtures: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=post-probe-fixtures
[ -z "$j_path" ] && grep -q '^missing-schemas: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=post-probe-schemas
[ -z "$j_path" ] && grep -q '^component-set: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=pre-probe-component-set
if [ -n "$j_path" ]; then
  ok "J setup: the run really terminated at an early preflight ($j_path, exit $j_status)"
else
  bad "J setup: no early-terminal marker in the block — this case did not exercise an early-terminal path"
  grep -E '^(RESULT|refusal|component-set|missing-)' "$j_sum" 2>/dev/null | head -4
fi
if [ "$j_count" -eq 1 ]; then
  ok "J: the early-terminal block carries EXACTLY ONE disk-admission: line — the contract has no hole"
else
  bad "J: the early-terminal block carries $j_count disk-admission: lines (the contract says exactly 1)"
fi
case "$j_path" in
  post-probe-*)
    # The probe ran before this preflight, so the block must carry a REAL verdict.
    case "$j_line" in
      *'disk-admission: PASS'*'evaluated 2x'*)
        ok "J: a POST-probe early terminal carries the real verdict, both evaluations named" ;;
      *'NOT EVALUATED'*)
        bad "J: a POST-probe early terminal claims NOT EVALUATED — the verdict existed and was dropped: $j_line" ;;
      *) bad "J: unexpected rendering on a post-probe early terminal: ${j_line:-<none>}" ;;
    esac ;;
  pre-probe-*)
    # This block genuinely precedes the probe; its honest value names the ordering.
    case "$j_line" in
      *'NOT EVALUATED'*'emitted BEFORE the #3755 probe'*)
        ok "J: a PRE-probe early terminal says so, naming the ordering — not a fabricated verdict" ;;
      *) bad "J: a pre-probe early terminal does not name the ordering: ${j_line:-<none>}" ;;
    esac ;;
esac
# Whatever the path, the block must never claim a verdict the probe did not reach.
case "$j_line" in
  *'INTERNAL (#3755)'*) bad "J: the block reports the probe ran but left no verdict — a defect state was reached" ;;
esac

# ===========================================================================
# Case K (roborev job 341, Medium): the probe measures the filesystem CARGO will
# actually write to, resolved by ASKING CARGO.
#
# The pre-fix subject was `${CARGO_TARGET_DIR:-$REPO_ROOT/target}`. Cargo also
# honours CARGO_BUILD_TARGET_DIR and `[build] target-dir` in a .cargo/config.toml
# (workspace, $CARGO_HOME, or any ancestor). Point either at another volume and the
# guard measures a device the build never touches — a confident, specific, WRONG
# number, which is worse than none because a reader acts on it.
#
# Every below case carries the PRE-FIX resolver evaluated on the SAME input, so the
# defect is shown to have been reachable rather than merely fixed.
# ===========================================================================
K_PREFIX_DEFAULT=$(cd "$SCRIPT_DIR/../.." && pwd)/target

# prefix_resolver <env-name> <env-value>: the PRE-FIX subject resolution, reproduced
# exactly — `${CARGO_TARGET_DIR:-$REPO_ROOT/target}` — under the given single override.
prefix_resolver() {
  case "$1" in
    CARGO_TARGET_DIR) printf '%s' "$2" ;;
    *) printf '%s' "$K_PREFIX_DEFAULT" ;;
  esac
}

# k_case <label> <expected-target-dir> <env-name> <env-value> [more env...]
# Runs a real stub gate and asserts the line names <expected-target-dir>.
k_case() {
  local label="$1" expect="$2" envname="$3"; shift 3
  local sc; sc=$(df_script "$label" "$HIGH")
  run_stub_gate "$label" "$sc" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1 "$@"
  local err=$RS_ERR line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"target-dir $expect (via cargo metadata)"*)
      ok "target-dir/$label: resolved to $expect, and the line says HOW" ;;
    *) bad "target-dir/$label: expected 'target-dir $expect (via cargo metadata)', got: ${line:-<none>}" ;;
  esac
  # The differential: what the pre-fix resolver would have picked on this same input.
  if [ -n "$envname" ]; then
    local was; was=$(prefix_resolver "$envname" "$expect")
    if [ "$was" = "$expect" ]; then
      ok "target-dir/$label CONTROL: the pre-fix resolver also picked $expect (this case is not a differential — it guards against over-correction)"
    else
      ok "target-dir/$label CONTROL: the PRE-FIX resolver picked $was, NOT $expect — it measured the wrong filesystem"
    fi
  fi
}

k_ct="$tmp/k-target-ct"
k_bt="$tmp/k-target-bt"
k_ch="$tmp/k-cargo-home"; mkdir -p "$k_ch"
k_cfg="$tmp/k-target-cfg"
printf '[build]\ntarget-dir = "%s"\n' "$k_cfg" > "$k_ch/config.toml"

k_case k-default   "$K_PREFIX_DEFAULT" ""                     CQLITE_GATE_POLL_SECS=0.3
k_case k-cargo-td  "$k_ct"  CARGO_TARGET_DIR       CARGO_TARGET_DIR="$k_ct"
k_case k-build-td  "$k_bt"  CARGO_BUILD_TARGET_DIR CARGO_BUILD_TARGET_DIR="$k_bt"
k_case k-config-td "$k_cfg" CARGO_HOME             CARGO_HOME="$k_ch"
# Precedence: env CARGO_TARGET_DIR must beat CARGO_BUILD_TARGET_DIR and the config file.
k_case k-precedence "$k_ct" CARGO_TARGET_DIR \
  CARGO_HOME="$k_ch" CARGO_BUILD_TARGET_DIR="$k_bt" CARGO_TARGET_DIR="$k_ct"

# The measurement must follow the RESOLVED directory, not just be reported beside it: a
# target dir on a filesystem the shim reports as BELOW the bar must REFUSE.
k_low_script=$(df_script k-low "$LOW")
run_stub_gate k-low "$k_low_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/k-low-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CARGO_BUILD_TARGET_DIR="$k_bt"
k_low_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; k_low_status=$WX_STATUS; k_low_markers=$WX_MARKERS
assert_no_timeout "k-low"
if [ "$k_low_status" -ne 0 ] && [ "$k_low_markers" -eq 0 ]; then
  ok "target-dir/k-low: a below-bar reading on the RESOLVED target dir refuses (the verdict follows the resolution)"
else
  bad "target-dir/k-low: did not refuse (exit $k_low_status, markers $k_low_markers)"
fi
if grep -q "target-dir $k_bt (via cargo metadata)" "$k_low_sum" 2>/dev/null; then
  ok "target-dir/k-low: the refusal SUMMARY names the directory it measured"
else
  bad "target-dir/k-low: the refusal SUMMARY does not name the resolved directory"
fi

# RESOLUTION FAILURE is UNMEASURED with a cause naming TARGET-DIR RESOLUTION — distinct
# from a df cause and from a bar cause, because they are three different operator
# actions — and it NEVER falls back to $REPO_ROOT/target, which would reinstate the
# defect in exactly the configurations that trigger it. Driven by a PATH with no cargo,
# which is what an absent cargo really looks like to the probe (rc 127).
mkdir -p "$tmp/k-nocargo-bin"
cat > "$tmp/k-nocargo-bin/cargo" <<'NOCARGO'
#!/usr/bin/env bash
exit 127
NOCARGO
chmod +x "$tmp/k-nocargo-bin/cargo"
k_nc_script=$(df_script k-nocargo "$HIGH")
RS_PATH_PREFIX="$tmp/k-nocargo-bin"
run_stub_gate k-nocargo "$k_nc_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/k-nocargo-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
k_nc_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; k_nc_status=$WX_STATUS; k_nc_markers=$WX_MARKERS
assert_no_timeout "k-nocargo"
k_nc_line=$(grep_line "$k_nc_err" '^agent-gate: disk-admission: ')
case "$k_nc_line" in
  *'UNMEASURED (target-dir-'*)
    ok "target-dir/k-nocargo: resolution failure is UNMEASURED with a cause naming TARGET-DIR resolution" ;;
  *'UNMEASURED (df-'*)
    bad "target-dir/k-nocargo: a resolution failure is reported as a DF failure — wrong operator action: $k_nc_line" ;;
  *) bad "target-dir/k-nocargo: expected a target-dir UNMEASURED cause, got: ${k_nc_line:-<none>}" ;;
esac
case "$k_nc_line" in
  *"target-dir $K_PREFIX_DEFAULT"*)
    bad "target-dir/k-nocargo: fell back to \$REPO_ROOT/target — the defect is reinstated in exactly the configurations that trigger it" ;;
  *) ok "target-dir/k-nocargo: NO fallback to \$REPO_ROOT/target on a resolution failure" ;;
esac
if [ "$k_nc_status" -eq 0 ] && [ "$k_nc_markers" -ge 1 ]; then
  ok "target-dir/k-nocargo: a resolution failure is NON-FATAL (declared, not un-runnable)"
else
  bad "target-dir/k-nocargo: a resolution failure refused the run (exit $k_nc_status)"
fi
# The df shim must NOT have been consulted: with no subject there is nothing to measure.
if [ "$(df_calls k-nocargo)" -eq 0 ]; then
  ok "target-dir/k-nocargo: df was never called — the probe refuses before measuring an unresolved subject"
else
  bad "target-dir/k-nocargo: df was called $(df_calls k-nocargo) time(s) against an unresolved subject"
fi

# ===========================================================================
# Case L (roborev job 345, Medium): the PROBE and the BUILDS resolve the target
# directory the SAME way — one resolver, one truth.
#
# The probe stopped modelling cargo in round 5, which exposed that
# run_side_component still did, with the very expression the probe had shed. So a
# config-based target dir made the guard measure cargo's directory while several
# large side-lane builds wrote somewhere else entirely.
#
# Asserted against the REAL functions: both bodies are EXTRACTED VERBATIM from the
# shipped agent-gate.sh and executed (the idiom test_cargo_output_parsers.sh uses),
# so unwiring them reds this suite instead of greening it. Only two things are
# substituted, and neither is the subject: `dispatch_component`, replaced by a
# recorder, and `_gate_resolve_target_dir`, scripted — the resolver itself is
# covered against the REAL cargo by Case K, and stubbing it here isolates the
# wiring question this case exists to answer.
# ===========================================================================
l_extract() { awk -v f="^$1\\\(\\\) \\\{$" '$0 ~ f {p=1} p {print} p && /^\}$/ {exit}' "$GATE"; }

for fn in _gate_side_target_base_init run_side_component; do
  if [ -n "$(l_extract "$fn")" ]; then
    ok "side-base: extracted the REAL $fn from the shipped gate"
  else
    bad "side-base: could not extract $fn — this case would be testing nothing"
  fi
done

# l_side_base <resolver-answer> <_DA_TARGET_DIR> <CARGO_TARGET_DIR> -> the base
# run_side_component actually passes, via the real bodies.
l_side_base() {
  local answer="$1" datd="$2" ctd="$3"
  (
    set -uo pipefail
    REPO_ROOT="/repo-root"
    _DA_TARGET_DIR="$datd"
    CARGO_TARGET_DIR="$ctd"
    _GATE_SIDE_BASE=""; _GATE_SIDE_BASE_NOTE=""
    # `$answer`, never `$1`: inside a function body `$1` is THAT FUNCTION's first
    # argument, so the obvious `printf '%s' "$1"` printed the empty string and every
    # scripted answer read as UNRESOLVED — a stub that silently stubbed nothing.
    _L_ANSWER="$answer"
    _gate_resolve_target_dir() { printf '%s' "$_L_ANSWER"; }
    dispatch_component() { printf '%s' "${CARGO_TARGET_DIR%/agent-gate-side/*}"; }
    eval "$(l_extract _gate_side_target_base_init)"
    eval "$(l_extract run_side_component)"
    _gate_side_target_base_init 2>/dev/null
    run_side_component smoke
  ) 2>/dev/null
}
# The PRE-FIX body, reproduced verbatim, for the differential.
l_prefix_base() { printf '%s' "${2:-/repo-root/target}"; }

l_case() {
  local label="$1" expect="$2" resolver="$3" datd="$4" ctd="$5"
  local got; got=$(l_side_base "$resolver" "$datd" "$ctd")
  if [ "$got" = "$expect" ]; then
    ok "side-base/$label: the REAL run_side_component bases on $expect"
  else
    bad "side-base/$label: expected base $expect, got '${got:-<none>}'"
  fi
  local was; was=$(l_prefix_base "$resolver" "$ctd")
  if [ "$was" = "$expect" ]; then
    ok "side-base/$label CONTROL: the pre-fix body also produced $expect (over-correction guard, not a differential)"
  else
    ok "side-base/$label CONTROL: the PRE-FIX body produced $was, NOT $expect — the builds wrote to a filesystem the probe never measured"
  fi
}
# (a) the probe already resolved it: reuse that answer verbatim, never re-ask.
l_case probe-verdict  /cfg-target  'OK /never-asked' /cfg-target ''
# (b) no probe verdict (--only): ask the SAME resolver.
l_case only-mode      /cfg-target  'OK /cfg-target'  ''           ''
# (c) CARGO_TARGET_DIR set: cargo resolves it, and the base follows cargo's answer.
l_case cargo-td       /env-target  'OK /env-target'  ''           /env-target
# (d) resolution FAILS: the legacy modelled base survives HERE and only here.
l_case unresolved     /repo-root/target 'UNRESOLVED target-dir-cargo-unavailable' '' ''

# The behaviour change, stated as an assertion rather than left in prose: with a
# config-based target dir the side base is NO LONGER under the repo.
l_cfg=$(l_side_base 'OK /cfg-target' /cfg-target '')
case "$l_cfg" in
  /repo-root/*) bad "side-base: a config-based target dir still lands under the repo — the disagreement survives" ;;
  *) ok "side-base: with a config-based target dir, side-lane builds are placed under it, not under the repo (declared behaviour change)" ;;
esac
# ...and the placement suffix is unchanged, so nothing else about the side lane moved.
l_full=$(
  ( set -uo pipefail
    REPO_ROOT=/repo-root; _DA_TARGET_DIR=/cfg-target; CARGO_TARGET_DIR=""
    _GATE_SIDE_BASE=""; _GATE_SIDE_BASE_NOTE=""
    _gate_resolve_target_dir() { printf 'OK /cfg-target'; }
    dispatch_component() { printf '%s' "$CARGO_TARGET_DIR"; }
    eval "$(l_extract _gate_side_target_base_init)"; eval "$(l_extract run_side_component)"
    _gate_side_target_base_init 2>/dev/null; run_side_component smoke ) 2>/dev/null)
if [ "$l_full" = "/cfg-target/agent-gate-side/smoke" ]; then
  ok "side-base: the per-component suffix is unchanged (<base>/agent-gate-side/<name>)"
else
  bad "side-base: unexpected per-component path '$l_full'"
fi

# ===========================================================================
# Case M (roborev job 345, Low): a fresh target dir is never paired with a stale
# mount. The target dir is deliberately RE-RESOLVED at slot grant, so if it moved
# during the queue the retained mount describes a different filesystem — and the
# remedy line would send an operator to clean the wrong one.
# ===========================================================================
# NOTE the target dirs below live under $tmp. Since the subject became a bounded
# `mkdir -p` (job 351) an unwritable path such as `/td-A` is legitimately
# `target-dir-uncreatable`, so a fixture using one would measure that instead of the
# subject-moved property this case is about.
m_case() {
  local label="$1" td1="$2" td2="$3" expect_fs="$4" why="$5"
  local cs="$tmp/$label.cargoscript"
  printf '%s\n%s\n' "$td1" "$td2" > "$cs"
  local ds; ds=$(df_script "$label" "$HIGH" FAIL)
  RS_PATH_PREFIX="$tmp/cargoshim"
  run_stub_gate "$label" "$ds" \
    CARGO_SHIM_SCRIPT="$cs" CARGO_SHIM_STATE="$tmp/$label.cargostate" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1
  RS_PATH_PREFIX=""
  local err=$RS_ERR line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"fs $expect_fs;"*) ok "stale-mount/$label: $why" ;;
    *) bad "stale-mount/$label: expected 'fs $expect_fs', got: ${line:-<none>}" ;;
  esac
  case "$line" in
    *"target-dir $td2 "*) ok "stale-mount/$label: the line names the RE-RESOLVED target dir ($td2)" ;;
    *) bad "stale-mount/$label: the line does not name the re-resolved target dir: ${line:-<none>}" ;;
  esac
}
# The subject MOVED during the queue -> the mount measured for the old one is dropped.
m_case m-moved   "$tmp/td-A" "$tmp/td-B" unknown \
  "the mount is CLEARED when the re-resolved subject differs (no fresh-dir/stale-mount pairing)"
# CONTROL: the subject is unchanged -> the mount IS retained. Without this, a rule that
# simply always cleared would pass the case above and lose real information.
m_case m-same    "$tmp/td-A" "$tmp/td-A" /shimfs \
  "the mount is RETAINED when the re-resolved subject is PROVEN identical"

# ===========================================================================
# Case N (roborev job 349): the SUBJECT SET declares itself NON-EXHAUSTIVE, on
# every rendering, naming #3886.
#
# This probe measures ONE filesystem. The venv and bindings/node/node_modules are
# not measured, and node_modules is under the REPOSITORY whatever cargo's target
# dir says — a counting-completeness gap split to #3886. A bare
# `disk-admission: PASS` would invite a reader to infer a closure this check does
# not deliver. A declaration nothing tests is a comment, so it is tested here.
# ===========================================================================
n_declares() {
  local label="$1" text="$2"
  case "$text" in
    *'subjects 1 RECOGNISED'*'NON-EXHAUSTIVE'*'(#3886)'*)
      ok "non-exhaustive/$label: declares an AFFIRMATIVE count, its incompleteness, and #3886" ;;
    *) bad "non-exhaustive/$label: missing or malformed declaration: ${text:-<none>}" ;;
  esac
  # The affirmative form is the point: `1 RECOGNISED`, never a bare figure, for the same
  # reason the cfg-gated-subtree census spells `0 RECOGNISED`.
  case "$text" in
    *'subjects 1 RECOGNISED (the cargo-resolved BUILD-OUTPUT filesystem only)'*)
      ok "non-exhaustive/$label: names WHAT the one measured subject is" ;;
    *) bad "non-exhaustive/$label: does not name the measured subject" ;;
  esac
}
# PASS rendering.
n_pass_script=$(df_script n-pass "$HIGH")
run_stub_gate n-pass "$n_pass_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-pass-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_pass_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-pass"
n_declares PASS "$(grep_line "$n_pass_err" '^agent-gate: disk-admission: ')"
# FAIL-CLOSED rendering — a refusal must not lose the declaration.
n_fail_script=$(df_script n-fail "$LOW")
run_stub_gate n-fail "$n_fail_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-fail-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_fail_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-fail"
n_declares FAIL-CLOSED "$(grep_line "$n_fail_sum" '^disk-admission: ')"
# UNMEASURED rendering.
n_unm_script=$(df_script n-unm FAIL)
run_stub_gate n-unm "$n_unm_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-unm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_unm_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-unm"
n_declares UNMEASURED "$(grep_line "$n_unm_err" '^agent-gate: disk-admission: ')"

# ===========================================================================
# Case O (roborev job 349, Low): the bounded runner's capture TRIPLE is owned and
# released — no `agent-gate-bcap.*` strays.
#
# Every bounded call on the admission path is made from inside a `$( … )`, where
# the runner's lazily-mktemp'd triple was memoized in a subshell and left three
# files behind per resolution — multiplied by every nested gate this suite runs.
# Counted in a PRIVATE TMPDIR so a peer lane on the same box cannot perturb it.
# ===========================================================================
o_tmp="$tmp/o-tmpdir"; mkdir -p "$o_tmp"
o_count() { local c=0 f; for f in "$o_tmp"/agent-gate-bcap.*; do [ -e "$f" ] && c=$((c+1)); done; printf '%s' "$c"; }
# PROVE THE COUNTER DISCRIMINATES before trusting a zero from it — a counter that can
# never see a leak reports "no leak" on a leaking build (four instances of that family on
# this branch already).
: > "$o_tmp/agent-gate-bcap.control"
if [ "$(o_count)" -eq 1 ]; then
  ok "capture-leak CONTROL: the counter SEES a planted bcap file (a zero from it means something)"
else
  bad "capture-leak CONTROL: the counter cannot see a planted bcap file — its zero would prove nothing"
fi
rm -f "$o_tmp/agent-gate-bcap.control"
o_before=$(o_count)
o_script=$(df_script o "$HIGH")
run_stub_gate o "$o_script" \
  TMPDIR="$o_tmp" CQLITE_GATE_SLOTS_DIR="$tmp/o-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=1
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "o"
o_after=$(o_count)
if [ "$o_after" -eq "$o_before" ]; then
  ok "capture-leak: a full-gate run left 0 stray bcap files in its TMPDIR (before=$o_before after=$o_after)"
else
  bad "capture-leak: the run leaked $((o_after - o_before)) bcap file(s) (before=$o_before after=$o_after)"
fi

# ===========================================================================
# Case P (roborev job 349, Medium): `df` is BOUNDED.
#
# A stalled NFS/FUSE mount hangs `df` indefinitely — at the post-grant
# measurement, while the machine-wide slot is HELD. That is #3755's own failure
# recreated inside its fix. The bound firing is reported with a cause DISTINCT
# from a parse failure, because a hang and a bad payload are different operator
# situations. The proof that nothing hangs is that this case returns at all.
# ===========================================================================
mkdir -p "$tmp/p-hangbin"
cat > "$tmp/p-hangbin/df" <<'PHANG'
#!/usr/bin/env bash
# Hangs on the FIRST call only, longer than _GATE_DF_BOUND_SECS; answers normally
# afterwards, so the case costs one bound rather than two.
n=$(cat "$DF_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1)); printf '%s' "$n" > "$DF_SHIM_STATE"
if [ "$n" -eq 1 ]; then sleep 120; fi
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 209715200 1%% /shimfs\n'
PHANG
chmod +x "$tmp/p-hangbin/df"
p_script=$(df_script p "$HIGH")
RS_PATH_PREFIX="$tmp/p-hangbin"
run_stub_gate p "$p_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/p-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
p_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; p_status=$WX_STATUS; p_markers=$WX_MARKERS
assert_no_timeout "p bounded df"
p_line=$(grep_line "$p_err" '^agent-gate: disk-admission: ')
case "$p_line" in
  *'launch UNMEASURED(df-timeout)'*)
    ok "bounded-df: a hanging df is CUT OFF and reported as df-timeout, distinct from df-failed" ;;
  *'launch UNMEASURED(df-failed)'*)
    bad "bounded-df: a hang is reported as a generic df-failed — a hang and a parse failure are different situations: $p_line" ;;
  *) bad "bounded-df: expected a df-timeout launch reading, got: ${p_line:-<none>}" ;;
esac
case "$p_line" in
  *'post-slot 200.0GiB'*)
    ok "bounded-df CONTROL: the run continued and the SECOND (fast) reading was taken normally" ;;
  *) bad "bounded-df CONTROL: the run did not recover after the bound fired: ${p_line:-<none>}" ;;
esac
if [ "$p_status" -eq 0 ] && [ "$p_markers" -ge 1 ]; then
  ok "bounded-df: the gate did not hang holding the slot — it measured, declared and proceeded"
else
  bad "bounded-df: the run did not complete normally (exit $p_status, markers $p_markers)"
fi

# ===========================================================================
# Case Q (roborev job 351, Medium): a MAIN-ONLY invocation never runs
# `cargo metadata`.
#
# _gate_side_target_base_init used to be called before anything established
# whether a side component had even been selected, so `--only file-size` —
# DOCUMENTED as cargo-free and hermetic, and the shape the nested tooling
# self-tests use — invoked cargo metadata: a delay and a possible Cargo.lock
# write on a path whose contract is that it touches neither.
#
# Asserted from an OBSERVATION (a recording shim), never from a timing or an
# absence nobody measured — and the shim is proved to discriminate first.
# ===========================================================================
mkdir -p "$tmp/q-cargoshim"
_Q_REAL_CARGO=$(command -v cargo 2>/dev/null || printf '/nonexistent/cargo')
cat > "$tmp/q-cargoshim/cargo" <<QSHIM
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "\$CARGO_RECORD"
exec "$_Q_REAL_CARGO" "\$@"
QSHIM
chmod +x "$tmp/q-cargoshim/cargo"
# `grep -c` PRINTS 0 and EXITS 1 when nothing matches, so a `|| printf '0'` fallback
# emits BOTH and the result is the two-line string "0\n0" — which then blows up in
# `[ -eq ]`. Take grep's output and sanitize it; never add a fallback beside it.
q_meta_calls() {
  local n; n=$(grep -c '^metadata' "$1" 2>/dev/null); n="${n%%$'\n'*}"
  case "$n" in ''|*[!0-9]*) n=0 ;; esac
  printf '%s' "$n"
}

# THE SUBJECT SET IS DERIVED, NOT LISTED (roborev job 357). Round 8 fixed the INSTANCE the
# review named — `--only file-size`, a MAIN-lane component — and left the CLASS open: the
# SIDE lane also holds explicitly Cargo-free components, and `--only delivery-telemetry`
# still ran cargo metadata. A finding names an instance; the defect is a class, so the set
# is computed from the gate's OWN classification (_component_lane + _fm_component_class,
# extracted from the shipped file) and a future Cargo-free component joins with no edit here.
q_extract() { sed -n "/^$1() {/,/^}$/p" "$GATE"; }
q_derive_free() {
  (
    eval "$(q_extract _component_lane)"
    eval "$(q_extract _fm_component_class)"
    local c
    for c in $(sed -n 's/^COMPONENTS=(\(.*\))$/\1/p' "$GATE"); do
      case "$(_fm_component_class "$c" 2>/dev/null)" in
        no-cargo) printf '%s\n' "$c" ;;
      esac
    done
  )
}
q_free=$(q_derive_free)
q_free_n=$(printf '%s\n' "$q_free" | grep -c '[^[:space:]]' || true)
q_free_n="${q_free_n%%$'\n'*}"; case "$q_free_n" in ''|*[!0-9]*) q_free_n=0 ;; esac
# A derivation that yields nothing would make every assertion below vacuous.
if [ "$q_free_n" -ge 3 ]; then
  ok "only-cargo-free: derived $q_free_n Cargo-free component(s) from the gate's own classification"
else
  bad "only-cargo-free: the derivation yielded $q_free_n component(s) — too few to be the real set; every assertion below would be vacuous"
fi

# (a) THE SUBJECTS: one --only run per Cargo-free component, MAIN lane and SIDE lane alike.
q_only=0
q_worst=""
for q_c in $q_free; do
  q_rec="$tmp/q-only.$q_c.record"; : > "$q_rec"
  env PATH="$tmp/q-cargoshim:$PATH" CARGO_RECORD="$q_rec" \
    AGENT_GATE_SUMMARY_FILE="$tmp/q-only.$q_c.summary.txt" \
    CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-/nonexistent}" \
    bash "$GATE" --only "$q_c" >"$tmp/q-only.$q_c.out" 2>"$tmp/q-only.$q_c.err"
  q_n=$(q_meta_calls "$q_rec")
  if [ "$q_n" -eq 0 ]; then
    ok "only-cargo-free[$q_c]: 0 'cargo metadata' calls — its Cargo-free contract holds"
  else
    bad "only-cargo-free[$q_c]: invoked cargo metadata $q_n time(s) — a documented Cargo-free path runs Cargo"
    q_only=$((q_only + q_n)); q_worst="$q_c"
  fi
done
q_rec="$tmp/q-only.file-size.record"
# (b) THE DISCRIMINATION CONTROL, run FIRST in spirit and asserted here: the SAME shim on a
#     full gate MUST record metadata calls. Without it, "0" proves only that the shim is
#     inert — the failure mode four earlier controls on this branch actually had.
q_rec2="$tmp/q-full.record"; : > "$q_rec2"
q_full_script=$(df_script q-full "$HIGH")
mkdir -p "$tmp/q-full.run"
env PATH="$tmp/q-cargoshim:$tmp/shim:$PATH" CARGO_RECORD="$q_rec2" \
  DF_SHIM_SCRIPT="$q_full_script" DF_SHIM_STATE="$tmp/q-full.dfstate" \
  AGENT_GATE_SUMMARY_FILE="$tmp/q-full.summary.txt" \
  CQLITE_GATE_STUB_RUNDIR="$tmp/q-full.run" CQLITE_GATE_STUB_SLEEP=1 \
  CQLITE_GATE_SLOTS_DIR="$tmp/q-full-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  bash "$GATE" >"$tmp/q-full.out" 2>"$tmp/q-full.err"
q_full=$(q_meta_calls "$q_rec2")
if [ "$q_full" -ge 1 ]; then
  ok "only-cargo-free CONTROL: the same shim records $q_full 'cargo metadata' call(s) on a full gate — a 0 from it means something"
else
  bad "only-cargo-free CONTROL: the shim recorded NO metadata call even on a full gate — it is inert and the subject assertion below proves nothing"
fi
if [ "$q_only" -eq 0 ]; then
  ok "only-cargo-free: the WHOLE derived class is Cargo-metadata-free, not just the one instance a review named"
else
  bad "only-cargo-free: $q_only metadata call(s) across the class (worst: $q_worst)"
fi
# The claim is precisely about `cargo metadata`. The gate's accelerator detection runs
# `cargo nextest --version` at startup on EVERY invocation, which is pre-existing and not
# this issue's subject; asserting "no cargo at all" would be asserting something false.
if [ "$(wc -l < "$q_rec")" -ge 1 ]; then
  ok "only-cargo-free: the run DID make its pre-existing startup cargo probe — the shim was on PATH and active"
else
  bad "only-cargo-free: no cargo call at all was recorded — the shim was not on the child's PATH"
fi

# ===========================================================================
# Case R (roborev job 351, Medium): the two-valued ancestor walk is GONE.
#
# `test -e` answers 1 for a permission-denied component, a symlink loop and a
# non-directory component exactly as for a genuinely missing path, so the walk
# climbed PAST an inaccessible mount and measured a DIFFERENT filesystem — a
# FALSE ADMISSION, the 1699-find-tristate shape. `mkdir -p` replaces it and
# answers the question the probe actually has: can the build write here.
#
# The fixture uses a NON-DIRECTORY path component, not chmod: ENOTDIR is raised
# for root and non-root alike, so this control cannot be bypassed by privilege
# (the H2 lesson from job 335).
# ===========================================================================
r_file="$tmp/r-not-a-directory"; : > "$r_file"
r_target="$r_file/target"

# THE POSITIVE CONTROL: the PRE-FIX walk, reproduced verbatim, on the same input.
r_walk() {
  local d="$1"
  while [ -n "$d" ] && [ "$d" != "/" ]; do
    [ -e "$d" ] && { printf '%s' "$d"; return 0; }
    d="$(dirname "$d")"
  done
  printf '/'
}
r_would=$(r_walk "$r_target")
if [ "$r_would" = "$r_file" ]; then
  ok "mkdir-subject CONTROL: the PRE-FIX walk resolved to '$r_would' — a plain FILE, not the build directory — and would have measured its filesystem and ADMITTED"
else
  bad "mkdir-subject CONTROL: the pre-fix walk resolved to '$r_would'; this fixture does not demonstrate the defect"
fi

r_script=$(df_script r "$HIGH")
run_stub_gate r "$r_script" \
  CARGO_TARGET_DIR="$r_target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
r_err=$RS_ERR; r_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_status=$WX_STATUS; r_markers=$WX_MARKERS
assert_no_timeout "r uncreatable target dir"
r_line=$(grep_line "$r_err" '^agent-gate: disk-admission: ')
# ---- roborev job 357, Medium: a failure that ESTABLISHES the build cannot write is a
# BINDING REFUSAL, not a non-fatal UNMEASURED. Classifying it "could not tell" bypassed
# admission on exactly the condition this change exists to catch.
if [ "$r_status" -ne 0 ] && [ "$r_markers" -eq 0 ]; then
  ok "cannot-write: an uncreatable target dir REFUSES and never begins work (exit $r_status)"
else
  bad "cannot-write: the run PROCEEDED into a build already known to be impossible (exit $r_status, markers $r_markers)"
fi
case "$r_line" in
  *'UNWRITABLE-FAIL-CLOSED (#3755)'*'UNWRITABLE(ENOTDIR)'*)
    ok "cannot-write: reported under its OWN verdict token, naming the errno that established it" ;;
  *'UNMEASURED'*)
    bad "cannot-write: still classified UNMEASURED — an affirmative 'cannot write' read as 'cannot tell': $r_line" ;;
  *) bad "cannot-write: unexpected rendering: ${r_line:-<none>}" ;;
esac
if grep -qx 'RESULT: FAIL' "$r_sum" 2>/dev/null; then
  ok "cannot-write: RESULT: FAIL (the pollable terminal token, as for a below-bar refusal)"
else
  bad "cannot-write: no exact 'RESULT: FAIL' line in the refusal SUMMARY"
fi
# The two binding causes must be TEXTUALLY distinct — different operator situations,
# different remedies — so a below-bar refusal must NOT carry the unwritable token.
if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$a_subj_sum" 2>/dev/null \
   && ! grep -q '^disk-admission: UNWRITABLE-FAIL-CLOSED' "$a_subj_sum" 2>/dev/null; then
  ok "cannot-write: the below-bar refusal keeps its own distinct token (the two are not merged)"
else
  bad "cannot-write: the below-bar and unwritable refusals are not textually distinct"
fi
if [ "$(df_calls r)" -eq 0 ]; then
  ok "cannot-write: df was NEVER called — no filesystem other than the subject was measured"
else
  bad "cannot-write: df ran $(df_calls r) time(s) — some other filesystem was measured"
fi
# A REAL EACCES subject, in addition to the ENOTDIR one above, because the finding named
# permission failures explicitly. Skipped rather than faked when running as root, where
# chmod cannot deny us.
if [ "$(id -u)" -eq 0 ]; then
  skip "cannot-write[EACCES]: running as root — chmod cannot produce a real permission denial here"
else
  r_locked="$tmp/r-locked"; mkdir -p "$r_locked"; chmod 500 "$r_locked"
  r_perm_script=$(df_script r-perm "$HIGH")
  run_stub_gate r-perm "$r_perm_script" \
    CARGO_TARGET_DIR="$r_locked/target" \
    CQLITE_GATE_SLOTS_DIR="$tmp/r-perm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
  r_perm_err=$RS_ERR
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_perm_status=$WX_STATUS; r_perm_markers=$WX_MARKERS
  assert_no_timeout "r-perm"
  chmod 700 "$r_locked"
  r_perm_line=$(grep_line "$r_perm_err" '^agent-gate: disk-admission: ')
  case "$r_perm_line" in
    *'UNWRITABLE-FAIL-CLOSED (#3755)'*'UNWRITABLE(EACCES)'*)
      if [ "$r_perm_status" -ne 0 ] && [ "$r_perm_markers" -eq 0 ]; then
        ok "cannot-write[EACCES]: a REAL permission denial refuses and never begins work"
      else
        bad "cannot-write[EACCES]: named the errno but did not refuse (exit $r_perm_status)"
      fi ;;
    *) bad "cannot-write[EACCES]: expected an UNWRITABLE(EACCES) refusal, got: ${r_perm_line:-<none>}" ;;
  esac
fi

# ---- THE OTHER HALF OF THE SPLIT: a failure that establishes NOTHING stays non-fatal.
# Driven by a REAL bound firing, not a simulated status: a python3 shim that hangs ONLY on
# the mkdir classifier (3 argv: -c, script, path) and delegates the metadata parse (2 argv),
# so the resolution still succeeds and the hang lands exactly on the call under test.
mkdir -p "$tmp/r-hangpy"
_R_REAL_PY=$(command -v python3 2>/dev/null || printf '/nonexistent/python3')
cat > "$tmp/r-hangpy/python3" <<RPY
#!/usr/bin/env bash
if [ "\$#" -eq 3 ] && [ "\$1" = -c ]; then sleep 120; fi
exec "$_R_REAL_PY" "\$@"
RPY
chmod +x "$tmp/r-hangpy/python3"
r_unm_script=$(df_script r-unm "$HIGH")
RS_PATH_PREFIX="$tmp/r-hangpy"
run_stub_gate r-unm "$r_unm_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-unm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
r_unm_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; r_unm_status=$WX_STATUS; r_unm_markers=$WX_MARKERS
assert_no_timeout "r-unm bounded classifier"
r_unm_line=$(grep_line "$r_unm_err" '^agent-gate: disk-admission: ')
case "$r_unm_line" in
  *'UNMEASURED (target-dir-mkdir-timeout)'*)
    ok "cannot-tell: the bound firing stays UNMEASURED with its own cause — it establishes nothing" ;;
  *'UNWRITABLE'*)
    bad "cannot-tell: a bound timeout was read as an affirmative 'cannot write' — that would red correct runs: $r_unm_line" ;;
  *) bad "cannot-tell: expected UNMEASURED (target-dir-mkdir-timeout), got: ${r_unm_line:-<none>}" ;;
esac
if [ "$r_unm_status" -eq 0 ] && [ "$r_unm_markers" -ge 1 ]; then
  ok "cannot-tell: 'could not tell' is NON-FATAL — the run proceeded, declared"
else
  bad "cannot-tell: an unclassifiable failure refused the run (exit $r_unm_status)"
fi

# THE OTHER HALF: a target dir that simply does not exist yet — the cold-lane case the
# walk existed for — must be CREATED and measured, not refused. This is what stops the
# fix being an over-correction that reds every cold lane.
r_cold="$tmp/r-cold/deep/target"
r_cold_script=$(df_script r-cold "$HIGH")
run_stub_gate r-cold "$r_cold_script" \
  CARGO_TARGET_DIR="$r_cold" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-cold-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
r_cold_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_cold_status=$WX_STATUS
assert_no_timeout "r cold target dir"
r_cold_line=$(grep_line "$r_cold_err" '^agent-gate: disk-admission: ')
case "$r_cold_line" in
  *"disk-admission: PASS"*"target-dir $r_cold "*)
    ok "mkdir-subject: a not-yet-existing target dir is CREATED and measured (the cold-lane case the walk existed for)" ;;
  *) bad "mkdir-subject: a cold target dir was not measured: ${r_cold_line:-<none>}" ;;
esac
if [ -d "$r_cold" ]; then
  ok "mkdir-subject: the accepted side effect is real and asserted — the directory now exists (cargo would create it seconds later anyway)"
else
  bad "mkdir-subject: the target dir was reported measured but does not exist"
fi

printf '\n%s\n' "-----------------------------------------------"
printf 'passed: %d  failed: %d  skipped: %d\n' "$PASS" "$FAIL" "$SKIP"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
