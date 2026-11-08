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
out=$(CQLITE_TOOLING_TESTS_JOBS=2 timeout 40 bash "$W/t1.sh" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^failed=\[\]$' <<<"$out"; then
  ok "pool drains with a pid-probe that reports every pid alive (capacity is from .rc files)"
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
  out=$(CQLITE_TOOLING_TESTS_JOBS=$J timeout 40 bash "$W/t2.sh" 2>&1)
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
echo "tooling-tests pool self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
