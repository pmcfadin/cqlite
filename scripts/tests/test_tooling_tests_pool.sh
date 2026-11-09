#!/usr/bin/env bash
# Self-test for the tooling-tests parallel runner (#4468): scripts/agent-gate.sh `_tt_*` helpers.
#  * the pool's capacity comes from per-suite .rc files, not pid probes: with a hostile `kill`
#    that reports every pid alive (pid reuse), the pool must still drain and respect the width;
#  * every failing suite is named, in every mode; unknown modes never run and FAIL.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$HERE/../agent-gate.sh"
W="$(mktemp -d "${TMPDIR:-/tmp}/tt-pool-selftest.XXXXXX")"
trap "[ -n \"\${KEEP:-}\" ] || rm -rf \"\$W\"" EXIT
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

# 1. hostile kill + concurrency: 8 one-second suites at width 2. The pool must drain under a `kill` that
#    reports every pid alive, AND the observed maximum overlap must be EXACTLY 2 (the planted job
#    touches a marker, counts the live markers and appends the count; the max is computed from that).
mkdir -p "$W/cc/active"
cat >"$W/cc.sh" <<EOS
touch "$W/cc/active/\$\$"
set -- "$W"/cc/active/*
echo \$# >>"$W/cc/counts"
sleep 1
rm -f "$W/cc/active/\$\$"
EOS
cat >"$W/t1.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
kill() { return 0; }
. "$W/helpers.sh"
_tt_reset
for n in 1 2 3 4 5 6 7 8; do _tt_add P "s\$n" 'bash "$W/cc.sh"'; done
: >"$W/c1.log"
_tt_run_all "$W/c1.log" "$W/d1"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=2 bash "$W/t1.sh" 2>&1); rc=$?
max=0; nruns=0
while read -r c; do nruns=$((nruns + 1)); [ "$c" -gt "$max" ] && max=$c; done <"$W/cc/counts"
if [ "$rc" -eq 0 ] && grep -q '^failed=\[\]$' <<<"$out"; then
  ok "pool drains with a pid-probe that reports every pid alive (liveness is bash's job table, not pid probes)"
else
  bad "pool stalled or failed under a hostile kill shim (rc=$rc)"
fi
if [ "$nruns" -eq 8 ] && [ "$max" -eq 2 ]; then
  ok "width 2 is honoured exactly: 8 suites ran, observed maximum concurrency = 2"
else
  bad "width-2 pool: $nruns suites ran, observed maximum concurrency $max (want 8 runs, max exactly 2)"
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
# By THE INVARIANT a group is never signalled once its leader has been reaped, so the descendants of a
# SIGKILLed worker are expected to survive; they must be NAMED in the suite log (#4475 owns cleaning
# them up). This test removes them itself, by the pids the planted script recorded.
alive=""
for f in suite.pid child.pid; do
  pid=$(cat "$W/$f" 2>/dev/null)
  [ -n "$pid" ] && builtin kill -0 "$pid" 2>/dev/null && { alive="$alive $f"; builtin kill -9 "$pid" 2>/dev/null; }
done
if grep -q 'descendants of this suite may survive' "$W/d3/001.log"; then
  ok "a worker killed without a status: its group is NOT signalled (leader already reaped); the suite log notes descendants may survive (survivors:${alive:- none})"
else
  bad "no 'descendants may survive' note in the killed worker's suite log"
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
# 8. THE INVARIANT, ordering: on the interrupt path every group TERM must precede any `wait` of that
#    group's leader. Instrument kill and wait with a recorder shim; TERM the runner (our own pid).
cat >"$W/t8.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
REC="$W/rec8"; : >"\$REC"
kill() { case "\$*" in "-TERM -- -"*|"-KILL -- -"*) echo "kill \$*" >>"\$REC" ;; esac; builtin kill "\$@"; }
wait() { echo "wait \$*" >>"\$REC"; builtin wait "\$@"; }
. "$W/helpers.sh"
_tt_reset
_tt_add P 'w1' 'bash "$W/planted2.sh" 1'
_tt_add P 'w2' 'bash "$W/planted2.sh" 2'
: >"$W/c8.log"
CQLITE_TOOLING_TESTS_JOBS=4 _tt_run_all "$W/c8.log" "$W/d8"
EOS
rm -f "$W/w1.child" "$W/w2.child" "$W/w1.pid" "$W/w2.pid"
bash "$W/t8.sh" >"$W/t8.out" 2>&1 &
rp=$!
n=0
while [ "$n" -lt 100 ] && ! { [ -s "$W/w1.child" ] && [ -s "$W/w2.child" ]; }; do sleep 0.1; n=$((n + 1)); done
kill -TERM "$rp"
n=0
while kill -0 "$rp" 2>/dev/null && [ "$n" -lt 300 ]; do sleep 0.1; n=$((n + 1)); done
kill -0 "$rp" 2>/dev/null && kill -9 "$rp" 2>/dev/null
wait "$rp" 2>/dev/null
viol=$(awk '$1=="wait"{w[$2]=1} $1=="kill"{g=$4; sub(/^-/,"",g); if (g in w) print "signal to group " g " after its wait"; if ($2=="-TERM") k++} END{if(k<2) print "fewer than 2 group TERMs recorded"}' "$W/rec8")
nkill=$(grep -c '^kill -KILL' "$W/rec8")
for f in w1.pid w1.child w2.pid w2.child; do builtin kill -9 "$(cat "$W/$f" 2>/dev/null)" 2>/dev/null; done
if [ -z "$viol" ] && [ "$nkill" -eq 0 ]; then
  ok "every group TERM precedes the wait of its leader, and no KILL is sent once the leaders exited on TERM"
else
  bad "signal ordering: [$viol] KILLs=$nkill"
fi
# 9. THE INVARIANT, escalation: a leader that IGNORES TERM gets TERM then (leader re-checked alive) KILL;
#    a leader that exits on TERM gets no KILL; a leader that is already gone gets nothing at all.
cat >"$W/t9.sh" <<EOS
set -uo pipefail
REC="$W/rec9"; : >"\$REC"
kill() { case "\$*" in "-TERM -- -"*|"-KILL -- -"*) echo "kill \$*" >>"\$REC" ;; esac; builtin kill "\$@"; }
. "$W/helpers.sh"
set -m
( trap '' TERM; while :; do sleep 0.1; done ) >/dev/null 2>&1 & stubborn=\$!
( while :; do sleep 0.1; done ) >/dev/null 2>&1 & polite=\$!
( exit 0 ) & gone=\$!
sleep 0.5
_tt_signal_groups \$stubborn \$polite \$gone
echo "stubborn=\$stubborn polite=\$polite gone=\$gone GONE=[\$_TT_GONE]"
wait \$stubborn \$polite \$gone 2>/dev/null
EOS
out=$(bounded 40 bash "$W/t9.sh" 2>&1)
eval "$(sed -n 's/^\(stubborn=[0-9]* polite=[0-9]* gone=[0-9]*\) .*/\1/p' <<<"$out")"
g_ok=1
grep -q "^kill -TERM -- -$stubborn\$" "$W/rec9" && grep -q "^kill -KILL -- -$stubborn\$" "$W/rec9" || g_ok=0
grep -q "^kill -TERM -- -$polite\$" "$W/rec9" && ! grep -q "^kill -KILL -- -$polite\$" "$W/rec9" || g_ok=0
! grep -q -- "-$gone\$" "$W/rec9" || g_ok=0
case "$out" in *"GONE=["*" $gone"*) ;; *) g_ok=0 ;; esac
if [ "$g_ok" -eq 1 ]; then
  ok "TERM->KILL only while the leader is still listed: stubborn leader escalated, polite leader got no KILL, gone leader got nothing (reported in GONE)"
else
  bad "escalation rules violated: $(tr '\n' '|' <"$W/rec9") $out"
fi
# 10. a suite that PASSES but whose log file has vanished must FAIL, named (no PASS with missing evidence).
cat >"$W/t10.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
_tt_reset
_tt_add P 'log-vanishes' 'rm -f "$W/d10/000.log"'
_tt_add P 'fine' 'true'
: >"$W/c10.log"
_tt_run_all "$W/c10.log" "$W/d10"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
out=$(bounded 40 env CQLITE_TOOLING_TESTS_JOBS=2 bash "$W/t10.sh" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^failed=\[log-vanishes (log missing)\]$' <<<"$out"; then
  ok "a suite whose log is missing FAILs, named '(log missing)'"
else
  bad "missing suite log not reported: rc=$rc; $(tail -2 <<<"$out" | tr '\n' ' ')"
fi

# 11. #4479: a sccache server auto-started from INSIDE a pool suite must not keep that suite's private
#     TMPDIR (deleted when the suite ends). Hermetic: a private server port and cache dir, the server is
#     stopped BY ITS PORT, and the shared server is never touched (a unique port cannot reach it). The
#     server's TMPDIR is read from /proc/<pid>/environ of the process that carries our port.
SCC=$(command -v sccache 2>/dev/null || true)
scc_port=0
scc_stop() { # stop OUR server, by port only
  [ "$scc_port" != 0 ] || return 0
  SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" "$SCC" --stop-server >/dev/null 2>&1 || true
}
scc_free_port() { # a port nothing listens on
  local p n=0
  while [ "$n" -lt 50 ]; do
    p=$((20000 + RANDOM % 20000)); n=$((n + 1))
    (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { echo "$p"; return 0; }
  done
  return 1
}
scc_server_tmpdir() { # TMPDIR of the sccache server carrying our port ("" when none)
  local e f
  for e in /proc/[0-9]*/environ; do
    [ -r "$e" ] || continue
    f=$(tr '\0' '\n' <"$e" 2>/dev/null) || continue
    grep -qx "SCCACHE_SERVER_PORT=$scc_port" <<<"$f" || continue
    grep -qx 'SCCACHE_START_SERVER=1' <<<"$f" || continue
    grep '^TMPDIR=' <<<"$f" | head -1 | cut -d= -f2-
    return 0
  done
  return 0
}
scc_case() { # scc_case <guard 0|1> <stub-prestart 0|1>: prints the server's TMPDIR
  local guard=$1 stub=$2 stable="$W/stable-$1$2"
  mkdir -p "$stable" "$W/sccdir"
  scc_port=$(scc_free_port) || { echo "NOPORT"; return 0; }
  cat >"$W/t11.sh" <<EOS
set -uo pipefail
_ncpu=8; REPO_ROOT=/tmp
. "$W/helpers.sh"
$( [ "$stub" = 1 ] && echo '_tt_sccache_prestart() { :; }' )
_tt_reset
_tt_add P scc-suite '"\$RUSTC_WRAPPER" rustc --version'
: >"$W/c11.log"
_tt_run_all "$W/c11.log" "$W/d11"
echo "failed=[\$_TT_FAILED_NAMES]"
EOS
  bounded 60 env TMPDIR="$stable" RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" \
    SCCACHE_CACHE_SIZE=1G CQLITE_TOOLING_TESTS_JOBS=2 CQLITE_TT_SCCACHE_GUARD=$guard bash "$W/t11.sh" >"$W/t11.out" 2>&1
  scc_server_tmpdir
  scc_stop
  scc_port=0
}
if [ -z "$SCC" ]; then
  echo "skip - sccache not installed: the #4479 TMPDIR-leak cases did not run"
else
  trap 'scc_stop; [ -n "${KEEP:-}" ] || rm -rf "$W"' EXIT
  ctl=$(scc_case 0 0)
  case "$ctl" in
    */tt-par.*) ok "#4479 control (guard off): the server a pool suite auto-starts keeps the suite's private TMPDIR ($ctl) - the bug is observable" ;;
    *) bad "#4479 control: guard off did not reproduce the leak (server TMPDIR='$ctl'); the test cannot see the bug" ;;
  esac
  shimonly=$(scc_case 1 1)
  case "$shimonly" in
    ''|NOPORT|*/tt-par.*) bad "#4479 shim only (no pre-start): server TMPDIR='$shimonly', want the gate's stable dir" ;;
    "$W"/stable-11) ok "#4479 shim only: a server spawned by a compile inside the suite starts under the gate's stable TMPDIR" ;;
    *) bad "#4479 shim only: unexpected server TMPDIR '$shimonly'" ;;
  esac
  full=$(scc_case 1 0)
  case "$full" in
    "$W"/stable-10) ok "#4479 pre-start: with no server running, the pool starts it under the gate's stable TMPDIR before any suite runs" ;;
    *) bad "#4479 pre-start: server TMPDIR='$full', want $W/stable-10" ;;
  esac
fi
# 11d. the shim must pass EVERY environment variable through, including names that are not shell
#      identifiers: cargo hands rustc CARGO_BIN_EXE_<hyphenated-bin>, and dash drops such variables
#      (an sh shim made `env!("CARGO_BIN_EXE_ws0-corpus-gen")` tests fail to compile). A fake
#      `sccache` records its environment; the shim is built by the pool's own helper.
mkdir -p "$W/fakebin" "$W/shimdir"
printf '#!/usr/bin/env bash\nenv > "%s"\n' "$W/fake.env" >"$W/fakebin/sccache"; chmod +x "$W/fakebin/sccache"
cat >"$W/t11d.sh" <<EOS
set -uo pipefail
. "$W/helpers.sh"
_TT_STABLE_TMPDIR="$W/stable-shim"
_tt_sccache_shim "$W/shimdir"
[ -n "\$_TT_SCC_SHIM" ] || exit 3
env 'CARGO_BIN_EXE_a-b=1' TMPDIR="$W/private" "\$_TT_SCC_SHIM" rustc --version
EOS
RUSTC_WRAPPER="$W/fakebin/sccache" bash "$W/t11d.sh" >/dev/null 2>&1; shim_rc=$?
if [ "$shim_rc" -eq 0 ] && grep -qx 'CARGO_BIN_EXE_a-b=1' "$W/fake.env" && grep -qx "TMPDIR=$W/stable-shim" "$W/fake.env"; then
  ok "#4479 shim: passes a hyphenated CARGO_BIN_EXE_* variable through and sets the stable TMPDIR (bash shim, not dash)"
else
  bad "#4479 shim: lost a non-identifier env var or did not set TMPDIR (rc=$shim_rc; $(grep -E '^(CARGO_BIN_EXE|TMPDIR)' "$W/fake.env" 2>/dev/null | tr '\n' ' '))"
fi
echo "tooling-tests pool self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
