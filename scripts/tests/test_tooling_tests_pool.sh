#!/usr/bin/env bash
# Self-test for the tooling-tests parallel runner (#4468): scripts/agent-gate.sh `_tt_*` helpers.
#  * the pool's capacity comes from per-suite .rc files, not pid probes: with a hostile `kill`
#    that reports every pid alive (pid reuse), the pool must still drain and respect the width;
#  * every failing suite is named, in every mode; unknown modes never run and FAIL.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$HERE/../agent-gate.sh"
W="$(mktemp -d "${TMPDIR:-/tmp}/tt-pool-selftest.XXXXXX")"
trap 'rm -rf "$W"' EXIT
a=$(grep -n '^_TT_N=0' "$GATE" | head -1 | cut -d: -f1)
b=$(grep -n '^run_tooling_tests() {' "$GATE" | head -1 | cut -d: -f1)
[ -n "$a" ] && [ -n "$b" ] || { echo "FAIL: cannot locate the _tt_ helpers in agent-gate.sh"; exit 1; }
sed -n "${a},$((b - 1))p" "$GATE" >"$W/helpers.sh"

# bounded <secs> <cmd...>: portable watchdog (macOS has no timeout(1) without coreutils). Kills only
# the child this function started; returns its status (143 if the watchdog fired).
bounded() {
  local secs=$1 p w rc; shift
  "$@" & p=$!
  ( sleep "$secs"; kill -TERM "$p" 2>/dev/null ) >/dev/null 2>&1 & w=$!
  wait "$p"; rc=$?
  kill "$w" 2>/dev/null; wait "$w" 2>/dev/null
  return "$rc"
}
pass=0; fail=0
ok()  { pass=$((pass + 1)); echo "ok   - $1"; }
bad() { fail=$((fail + 1)); echo "FAIL - $1"; }

# 1. hostile kill: 8 one-second suites, width 2 => ~4s; must finish well inside the timeout.
cat >"$W/t1.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
kill() { return 0; }
. "$W/helpers.sh"
_tt_reset
for n in 1 2 3 4 5 6 7 8; do _tt_add P "s\$n" 'sleep 1'; done
: >"$W/c1.log"
_tt_run_all "$W/c1.log" "$W/d1"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=2 bash "$W/t1.sh" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^failed=\[\]$' <<<"$out"; then
  ok "pool drains with a pid-probe that reports every pid alive (liveness is bash's job table, not pid probes)"
else
  bad "pool stalled or failed under a hostile kill shim (rc=$rc)"
fi
# 2. every failure named (pool, lane, tail), unknown mode never executes.
cat >"$W/t2.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
_tt_reset
_tt_add P 'pool-fail' 'exit 3'
_tt_add L:x 'lane-fail' 'exit 4'
_tt_add S 'tail-fail' 'exit 5'
_tt_add BOGUS 'bogus' 'touch "$W/marker"'
_tt_add P 'pool-ok' 'true'
: >"$W/c2.log"
_tt_run_all "$W/c2.log" "$W/d2"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
for J in 4 1; do
  out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=$J bash "$W/t2.sh" 2>&1)
  if [ "$J" = 4 ]; then
    want='pool-fail.*lane-fail.*bogus.*tail-fail'
  else
    want='failed=\[pool-fail\]'
  fi
  if grep -Eq "$want" <<<"$out" && [ ! -e "$W/marker" ]; then
    ok "JOBS=$J: failures named as designed and the unknown-mode suite never ran"
  else
    bad "JOBS=$J: wrong failure naming or the unknown-mode suite ran: $(tail -1 <<<"$out")"
  fi
done
# 3. a worker KILLED mid-run (only the pid this test's runner launched) must not stall the pool and
#    must FAIL, named - and the planted suite's own process (and its sleeping child) must be gone
#    when the pool returns: each worker is its own process group and the group is terminated.
#    (bash 3.2 lacks the subshell-pid variable: the planted script uses $$ and walks up its ancestors to the worker,
#    the child of this test's runner, TT_MAIN.)
cat >"$W/planted.sh" <<EOS
echo \$\$ >"$W/suite.pid"
sleep 30 & echo \$! >"$W/child.pid"
p=\$\$
while :; do
  par=\$(ps -o ppid= -p "\$p"); set -- \$par; par=\$1
  [ -n "\$par" ] || exit 9
  [ "\$par" = "\$TT_MAIN" ] && break
  p=\$par
done
kill -9 "\$p"
wait
EOS
cat >"$W/t3.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
_tt_reset
_tt_add P 'ok-a' 'sleep 1'
_tt_add P 'killed-worker' 'TT_MAIN=\$TT_MAIN bash "$W/planted.sh"'
_tt_add P 'ok-b' 'sleep 1'
: >"$W/c3.log"
TT_MAIN=\$\$
export TT_MAIN
_tt_run_all "$W/c3.log" "$W/d3"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=2 bash "$W/t3.sh" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^failed=\[killed-worker\]$' <<<"$out"; then
  ok "a SIGKILLed worker does not stall the pool and FAILs, named"
else
  bad "killed worker: rc=$rc; $(tail -2 <<<"$out" | tr '\n' ' ')"
fi
orph=""
for f in suite.pid child.pid; do
  pid=$(cat "$W/$f" 2>/dev/null)
  if [ -z "$pid" ]; then orph="$orph $f(never written)"; elif kill -0 "$pid" 2>/dev/null; then orph="$orph $f=$pid(alive)"; fi
done
if [ -z "$orph" ]; then
  ok "after the worker was killed, the suite and its sleeping child are gone (process group terminated)"
else
  bad "orphaned processes survive the pool:$orph"
  for f in suite.pid child.pid; do kill -9 "$(cat "$W/$f" 2>/dev/null)" 2>/dev/null; done
fi
# 4. a worker whose status write FAILS (mv of the .rc refused) must FAIL, named, without stalling.
cat >"$W/t4.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
mv() { case "\${2:-}" in *.rc) return 1 ;; esac; command mv "\$@"; }
. "$W/helpers.sh"
_tt_reset
_tt_add P 'unpublished' 'true'
_tt_add P 'unpublished-too' 'true'
: >"$W/c4.log"
_tt_run_all "$W/c4.log" "$W/d4"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=2 bash "$W/t4.sh" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^failed=\[unpublished; unpublished-too\]$' <<<"$out"; then
  ok "workers whose status write fails FAIL, named, without stalling the pool"
else
  bad "unpublished status: rc=$rc; $(tail -2 <<<"$out" | tr '\n' ' ')"
fi
# 5. an unwritable canonical log (a directory in its place): the replay fails, so the suite must FAIL,
#    named, even though it passed - no PASS with missing evidence.
mkdir "$W/c5dir"
cat >"$W/t5.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
_tt_reset
_tt_add P 'passes-but-unreplayable' 'true'
_tt_run_all "$W/c5dir" "$W/d5"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
for J in 4 1; do
  out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=$J bash "$W/t5.sh" 2>&1); rc=$?
  if [ "$rc" -eq 0 ] && grep -q '^failed=\[passes-but-unreplayable (log replay failed)\]$' <<<"$out"; then
    ok "JOBS=$J: a failed log replay FAILs the suite, named (no PASS with missing evidence)"
  else
    bad "JOBS=$J: log-replay failure not reported: rc=$rc; $(tail -2 <<<"$out" | tr '\n' ' ')"
  fi
done
# 6. interrupt path: TERM to the runner (only the pid this test launched) while workers - two in the
#    pool and one in a lane - are running. No planted worker or its sleeping child may survive, no
#    tt-par.* scratch dir may remain, and the signal must be re-raised (the runner dies of TERM).
mkdir "$W/tmp6"
cat >"$W/planted2.sh" <<EOS
echo \$\$ >"$W/w\$1.pid"
sleep 30 & echo \$! >"$W/w\$1.child"
wait
EOS
cat >"$W/t6.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
TMPDIR="$W/tmp6"; export TMPDIR
. "$W/helpers.sh"
_tt_reset
_tt_add P 'w1' 'bash "$W/planted2.sh" 1'
_tt_add P 'w2' 'bash "$W/planted2.sh" 2'
_tt_add L:x 'w3' 'bash "$W/planted2.sh" 3'
: >"$W/c6.log"
CQLITE_TOOLING_TESTS_JOBS=4 _tt_run_all "$W/c6.log" "$W/d6"
echo "returned-normally"
EOS
bash "$W/t6.sh" >"$W/t6.out" 2>&1 &
rp=$!
n=0
while [ "$n" -lt 100 ] && ! { [ -s "$W/w1.child" ] && [ -s "$W/w2.child" ] && [ -s "$W/w3.child" ]; }; do sleep 0.1; n=$((n + 1)); done
kill -TERM "$rp"
n=0
while kill -0 "$rp" 2>/dev/null && [ "$n" -lt 300 ]; do sleep 0.1; n=$((n + 1)); done
kill -0 "$rp" 2>/dev/null && kill -9 "$rp" 2>/dev/null   # our own runner only, never by pattern
wait "$rp" 2>/dev/null; rc6=$?
surv=""
for f in w1.pid w1.child w2.pid w2.child w3.pid w3.child; do
  pid=$(cat "$W/$f" 2>/dev/null)
  if [ -z "$pid" ]; then surv="$surv $f(never written)"; elif kill -0 "$pid" 2>/dev/null; then surv="$surv $f=$pid(alive)"; kill -9 "$pid" 2>/dev/null; fi
done
left=$(ls -d "$W"/tmp6/tt-par.* 2>/dev/null)
if [ -z "$surv" ] && [ -z "$left" ] && [ "$rc6" -eq 143 ] && ! grep -q returned-normally "$W/t6.out"; then
  ok "TERM to the runner: every worker group is gone, no tt-par.* scratch dir remains, TERM was re-raised"
else
  bad "interrupt path: survivors=[$surv] scratch=[$left] rc=$rc6"
fi
# 7. a signal that arrives inside a launch-and-register critical section is only RECORDED (deferred).
cat >"$W/t7.sh" <<EOS
set -uo pipefail
. "$W/helpers.sh"
_TT_CRIT=1
_tt_on_signal TERM
echo "survived pending=\$_TT_SIGPEND"
EOS
out=$(bounded 20 bash "$W/t7.sh" 2>&1)
if [ "$out" = "survived pending=TERM" ]; then
  ok "a signal inside the launch-and-register critical section is deferred, not acted on"
else
  bad "critical-section deferral: got '$out'"
fi
echo "tooling-tests pool self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
