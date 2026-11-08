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
cat >"$W/t3.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
_tt_reset
_tt_add P 'ok-a' 'sleep 1'
_tt_add P 'killed-worker' 'echo \$BASHPID >"$W/suite.pid"; sleep 30 & echo \$! >"$W/child.pid"; me=\$BASHPID; w=\$(ps -o ppid= -p \$me | tr -d " "); kill -9 \$w; wait'
_tt_add P 'ok-b' 'sleep 1'
: >"$W/c3.log"
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
echo "tooling-tests pool self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
