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
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

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
# A per-run state file keeps the counter, so concurrent runs never share one.
# ---------------------------------------------------------------------------
mkdir -p "$tmp/shim"
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
run_stub_gate() {
  local case_name="$1" script="$2"; shift 2
  RS_RUNDIR="$tmp/$case_name.run"; mkdir -p "$RS_RUNDIR"
  RS_SUMMARY="$tmp/$case_name.summary.txt"
  RS_ERR="$tmp/$case_name.err"
  env "$@" \
    PATH="$tmp/shim:$PATH" \
    DF_SHIM_SCRIPT="$script" \
    DF_SHIM_STATE="$tmp/$case_name.dfstate" \
    AGENT_GATE_SUMMARY_FILE="$RS_SUMMARY" \
    CQLITE_GATE_STUB_RUNDIR="$RS_RUNDIR" \
    CQLITE_GATE_POLL_SECS=0.3 \
    bash "$GATE" >"$tmp/$case_name.out" 2>"$RS_ERR" &
  RS_PID=$!
}

# watch_until_exit <pid> <rundir> <timeout_s>: poll the rundir while <pid> runs, then
# reap it. Sets WX_STATUS (exit status) and WX_MARKERS (the MAX number of "I am working"
# markers ever observed) — the AFFIRMATIVE evidence for "did this run begin its work
# phase". Sets GLOBALS rather than printing: a `$( ... )` capture runs in a SUBSHELL,
# where `wait <pid>` cannot reap a job of the PARENT shell and silently yields 127.
WX_STATUS=0
WX_MARKERS=0
watch_until_exit() {
  local pid="$1" rundir="$2" timeout="$3"
  local deadline=$(( $(date +%s) + timeout )) max=0 c
  while [ "$(date +%s)" -lt "$deadline" ]; do
    c=$(marker_count "$rundir")
    [ "$c" -gt "$max" ] && max="$c"
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.05
  done
  c=$(marker_count "$rundir"); [ "$c" -gt "$max" ] && max="$c"
  wait "$pid"; WX_STATUS=$?
  WX_MARKERS="$max"
}

# grep_line <file> <pattern>: print the first matching line (empty when none).
grep_line() { grep -m1 -E "$2" "$1" 2>/dev/null; }

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
if grep -q '^refusal: post-slot disk admission (#3755)' "$a_subj_sum" 2>/dev/null; then
  ok "AC4: the refusal is NAMED on its own refusal: line"
else
  bad "AC4: no named 'refusal: post-slot disk admission (#3755)' line"
fi
# AC2, behavioural half: the slot is usable by a follow-up run immediately after.
follow_script=$(df_script a-follow "$HIGH")
run_stub_gate a-follow "$follow_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
watch_until_exit "$RS_PID" "$RS_RUNDIR" 30; f_status=$WX_STATUS; f_markers=$WX_MARKERS
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

printf '\n%s\n' "-----------------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
