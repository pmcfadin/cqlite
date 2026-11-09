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
REAL_ROOT="$(cd "$HERE/../.." && pwd)"
SCC=$(command -v sccache 2>/dev/null || true)
# The server-lifecycle cases find the server by its listening socket (ss) and read its environment
# from /proc, so they need both; otherwise they are SKIPPED, declared, never silently passed.
SCC_PROC_OK=0
if command -v ss >/dev/null 2>&1 && [ -r /proc/self/environ ]; then SCC_PROC_OK=1; fi
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
_ncpu=8; REPO_ROOT="$REAL_ROOT"
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
elif [ "$SCC_PROC_OK" != 1 ]; then
  echo "skip - no ss(8) or unreadable /proc/self/environ: the #4479 TMPDIR-leak cases did not run"
else
  trap 'scc_stop; [ -n "${KEEP:-}" ] || rm -rf "$W"' EXIT
  ctl=$(scc_case 0 0)
  case "$ctl" in
    "$W"/stable-00/tt-par.*) ok "#4479 control (guard off): the server a pool suite auto-starts keeps the suite's private TMPDIR ($ctl) - the bug is observable" ;;
    *) bad "#4479 control: guard off did not reproduce the leak (server TMPDIR='$ctl'); the test cannot see the bug" ;;
  esac
  shimonly=$(scc_case 1 1)
  case "$shimonly" in
    "$W"/stable-11) ok "#4479 shim only: a server spawned by a compile inside the suite starts under the gate's stable TMPDIR" ;;
    *) bad "#4479 shim only (no pre-start): server TMPDIR='$shimonly', want $W/stable-11" ;;
  esac
  full=$(scc_case 1 0)
  case "$full" in
    "$W"/stable-10) ok "#4479 pre-start: with no server running, the pool starts it under the gate's stable TMPDIR before any suite runs" ;;
    *) bad "#4479 pre-start: server TMPDIR='$full', want $W/stable-10" ;;
  esac
fi
# 11d. the RUSTC_WRAPPER shim is the CHECKED-IN scripts/lib/sccache-stable-tmpdir-wrapper.sh, never a file
#      generated under TMPDIR (a noexec temp filesystem would stop cargo executing a generated one).
#      Structural: the wrapper path is under the repo, not under TMPDIR; it is executable and tracked
#      with mode 100755. Behavioural: it passes EVERY environment variable through, including names that
#      are not shell identifiers (cargo hands rustc CARGO_BIN_EXE_<hyphenated-bin>; dash drops them),
#      and sets the stable TMPDIR for the exec only.
WRAP="$REAL_ROOT/scripts/lib/sccache-stable-tmpdir-wrapper.sh"
mkdir -p "$W/fakebin"
printf '#!/usr/bin/env bash\nenv > "%s"\n' "$W/fake.env" >"$W/fakebin/sccache"; chmod +x "$W/fakebin/sccache"
cat >"$W/t11d.sh" <<EOS
set -uo pipefail
REPO_ROOT="$REAL_ROOT"
. "$W/helpers.sh"
_TT_STABLE_TMPDIR="$W/stable-shim"
_tt_sccache_shim
[ -n "\$_TT_SCC_SHIM" ] || exit 3
echo "SHIM=\$_TT_SCC_SHIM" >"$W/shim.path"
env 'CARGO_BIN_EXE_a-b=1' TMPDIR="$W/private" CQLITE_TT_STABLE_TMPDIR="\$_TT_STABLE_TMPDIR" \
  CQLITE_TT_REAL_RUSTC_WRAPPER="\$_TT_SCC_REAL" "\$_TT_SCC_SHIM" rustc --version
EOS
RUSTC_WRAPPER="$W/fakebin/sccache" TMPDIR="$W/private" bash "$W/t11d.sh" >/dev/null 2>&1; shim_rc=$?
shimpath=$(sed -n 's/^SHIM=//p' "$W/shim.path" 2>/dev/null)
if [ "$shim_rc" -eq 0 ] && grep -qx 'CARGO_BIN_EXE_a-b=1' "$W/fake.env" && grep -qx "TMPDIR=$W/stable-shim" "$W/fake.env"; then
  ok "#4479 shim: passes a hyphenated CARGO_BIN_EXE_* variable through and sets the stable TMPDIR (bash, not dash)"
else
  bad "#4479 shim: lost a non-identifier env var or did not set TMPDIR (rc=$shim_rc; $(grep -E '^(CARGO_BIN_EXE|TMPDIR)' "$W/fake.env" 2>/dev/null | tr '\n' ' '))"
fi
case "$shimpath" in
  "$W"/*|"") bad "#4479 shim: the wrapper path ('$shimpath') is under TMPDIR or empty - a noexec temp dir would break it" ;;
  "$WRAP") ok "#4479 shim: the wrapper is the checked-in script ($shimpath), outside TMPDIR, so a noexec temp filesystem cannot stop it" ;;
  *) bad "#4479 shim: unexpected wrapper path '$shimpath'" ;;
esac
if [ -x "$WRAP" ]; then
  mode=$(git -C "$REAL_ROOT" ls-files -s -- scripts/lib/sccache-stable-tmpdir-wrapper.sh 2>/dev/null | cut -d' ' -f1)
  case "$mode" in
    100755) ok "#4479 shim: the wrapper is tracked with mode 100755" ;;
    '') ok "#4479 shim: the wrapper is executable (not a git checkout here, so the tracked mode was not checked)" ;;
    *) bad "#4479 shim: the wrapper is tracked with mode $mode, not 100755" ;;
  esac
else
  bad "#4479 shim: $WRAP is missing or not executable"
fi
# 11e. #4479 recovery: a RUNNING server that already holds a dead TMPDIR is replaced; a healthy one is left
#      alone (same pid). Private port and cache dir; the server is stopped by its port.
if [ -n "$SCC" ] && [ "$SCC_PROC_OK" != 1 ]; then
  echo "skip - no ss(8) or unreadable /proc/self/environ: the #4479 recovery cases did not run"
fi
if [ -n "$SCC" ] && [ "$SCC_PROC_OK" = 1 ]; then
  scc_pid_of() { # the pid listening on our port
    local l; l=$(ss -H -ltnp "sport = :$scc_port" 2>/dev/null); l=${l#*pid=}; printf '%s' "${l%%[!0-9]*}"
  }
  scc_start_under() { # scc_start_under <tmpdir>: start our private server with that TMPDIR
    TMPDIR=$1 SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G "$SCC" --start-server >/dev/null 2>&1
  }
  scc_prestart() { # run the pool's prestart against our server; stderr (the log line) is returned
    cat >"$W/t11e.sh" <<EOS
set -uo pipefail
. "$W/helpers.sh"
_TT_STABLE_TMPDIR="$W/stable-rec"
_tt_sccache_prestart
EOS
    env RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G \
      bash "$W/t11e.sh" 2>&1 >/dev/null
  }
  mkdir -p "$W/stable-rec" "$W/sccdir"
  # (i) poisoned: the server's TMPDIR is deleted after it started.
  scc_port=$(scc_free_port) || scc_port=0
  if [ "$scc_port" = 0 ]; then bad "#4479 recovery: no free port"; else
    mkdir -p "$W/doomed"; scc_start_under "$W/doomed"; old=$(scc_pid_of); rm -rf "$W/doomed"
    logline=$(scc_prestart); new=$(scc_pid_of); newtmp=$(scc_server_tmpdir)
    if [ -n "$old" ] && [ -n "$new" ] && [ "$old" != "$new" ] && [ "$newtmp" = "$W/stable-rec" ] \
       && grep -q "holds a dead TMPDIR" <<<"$logline"; then
      ok "#4479 recovery: a server holding a DELETED TMPDIR is replaced (pid $old -> $new) by one under the stable TMPDIR, with one log line"
    else
      bad "#4479 recovery: poisoned server not replaced (old=$old new=$new tmpdir='$newtmp' log='$logline')"
    fi
    # a compile through the shim now works
    if env SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" CQLITE_TT_STABLE_TMPDIR="$W/stable-rec" \
         CQLITE_TT_REAL_RUSTC_WRAPPER="$SCC" "$WRAP" rustc --version >/dev/null 2>&1; then
      ok "#4479 recovery: a compile through the shim succeeds against the replacement server"
    else
      bad "#4479 recovery: a compile through the shim failed against the replacement server"
    fi
    scc_stop; scc_port=0
  fi
  # (ii) control: a healthy server (TMPDIR exists, writable) is NOT restarted.
  scc_port=$(scc_free_port) || scc_port=0
  if [ "$scc_port" = 0 ]; then bad "#4479 recovery control: no free port"; else
    mkdir -p "$W/healthy"; scc_start_under "$W/healthy"; old=$(scc_pid_of)
    logline=$(scc_prestart); new=$(scc_pid_of)
    if [ -n "$old" ] && [ "$old" = "$new" ] && [ -z "$logline" ]; then
      ok "#4479 recovery control: a server with a healthy TMPDIR keeps its pid ($old) and nothing is logged"
    else
      bad "#4479 recovery control: healthy server was disturbed (old=$old new=$new log='$logline')"
    fi
    scc_stop; scc_port=0
  fi
fi
# 11f. #4479 serialisation: two prestarts racing against ONE poisoned server end with exactly one
#      replacement; the second re-reads the (now healthy) server under the lock and does not stop it.
if [ -n "$SCC" ] && [ "$SCC_PROC_OK" = 1 ] && ! command -v flock >/dev/null 2>&1; then
  echo "skip - no flock(1): the #4479 serialisation and lock-timeout cases did not run"
fi
if [ -n "$SCC" ] && [ "$SCC_PROC_OK" = 1 ] && command -v flock >/dev/null 2>&1; then
  scc_port=$(scc_free_port) || scc_port=0
  if [ "$scc_port" = 0 ]; then bad "#4479 serialisation: no free port"; else
    mkdir -p "$W/doomed2" "$W/stable-rec" "$W/lockdir"
    scc_start_under "$W/doomed2"; old=$(scc_pid_of); rm -rf "$W/doomed2"
    cat >"$W/t11f.sh" <<EOS
set -uo pipefail
. "$W/helpers.sh"
_TT_STABLE_TMPDIR="$W/stable-rec"
_tt_sccache_prestart
EOS
    envv=(RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G CQLITE_TT_SCCACHE_LOCK_DIR="$W/lockdir")
    env "${envv[@]}" bash "$W/t11f.sh" >/dev/null 2>"$W/f1.err" & f1=$!
    env "${envv[@]}" bash "$W/t11f.sh" >/dev/null 2>"$W/f2.err" & f2=$!
    wait "$f1"; wait "$f2"
    new=$(scc_pid_of); sleep 2; later=$(scc_pid_of); newtmp=$(scc_server_tmpdir)
    stops=$(cat "$W/f1.err" "$W/f2.err" | grep -c "holds a dead TMPDIR")
    if [ "$stops" -eq 1 ] && [ -n "$new" ] && [ "$new" != "$old" ] && [ "$new" = "$later" ] && [ "$newtmp" = "$W/stable-rec" ]; then
      ok "#4479 serialisation: two racing prestarts replaced the poisoned server exactly once (pid $old -> $new, stable afterwards, 1 stop logged)"
    else
      bad "#4479 serialisation: stops logged=$stops old=$old new=$new later=$later tmpdir='$newtmp'"
    fi
    scc_stop; scc_port=0
  fi
fi
# 11g-i. #4479 lock behaviour of the prestart. flock IS present in these cases. The lock is held by a
#      background process this test starts: it creates a readiness file only AFTER flock succeeded, the
#      test waits (bounded) for that file, and the holder is killed by its recorded pid.
scc_hold_lock() { # scc_hold_lock <lockfile> <readyfile>: sets scc_holder (pid) once the lock IS held
  rm -f "$2"
  ( flock 9 && : >"$2" && exec sleep 120 ) 9>>"$1" & scc_holder=$!
  local n=0; while [ "$n" -lt 100 ] && [ ! -f "$2" ]; do sleep 0.1; n=$((n + 1)); done
  [ -f "$2" ]
}
scc_prestart_script() { # scc_prestart_script <file> [trap-term]
  { printf 'set -uo pipefail\n. "%s/helpers.sh"\n_TT_STABLE_TMPDIR="%s/stable-rec"\n' "$W" "$W"
    [ -z "${2:-}" ] || printf '%s\n' "trap 'echo got-term >\"$W/term.mark\"; exit 143' TERM"
    printf '_tt_sccache_prestart\n'; } >"$1"
}
if [ -n "$SCC" ] && [ "$SCC_PROC_OK" = 1 ] && command -v flock >/dev/null 2>&1; then
  scc_port=$(scc_free_port) || scc_port=0
  if [ "$scc_port" = 0 ]; then bad "#4479 lock timeout: no free port"; else
    mkdir -p "$W/doomed3" "$W/lockdir3"
    scc_start_under "$W/doomed3"; old=$(scc_pid_of); rm -rf "$W/doomed3"
    lockf="$W/lockdir3/cqlite-sccache-$scc_port.lock"
    if ! scc_hold_lock "$lockf" "$W/ready3"; then
      bad "#4479 lock timeout: the holder never acquired the lock"
    else
      scc_prestart_script "$W/t11g.sh"
      logline=$(env RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G \
        CQLITE_TT_SCCACHE_LOCK_DIR="$W/lockdir3" CQLITE_TT_SCCACHE_LOCK_WAIT=1 bash "$W/t11g.sh" 2>&1 >/dev/null)
      new=$(scc_pid_of)
      if [ -n "$old" ] && [ "$old" = "$new" ] && grep -q "could not take" <<<"$logline" && ! grep -q "holds a dead TMPDIR" <<<"$logline"; then
        ok "#4479 lock timeout: with the lock really held elsewhere the prestart warns and skips recovery (poisoned server pid $old untouched, no stop)"
      else
        bad "#4479 lock timeout: old=$old new=$new log='$logline'"
      fi
      # 11h. TERM during a LONG lock wait is handled promptly (the wait is polled, not a blocking flock,
      #      so the trap runs within a fraction of a second). The prestart runs under a TERM trap like the gate's.
      scc_prestart_script "$W/t11h.sh" trap
      rm -f "$W/term.mark"
      env RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G \
        CQLITE_TT_SCCACHE_LOCK_DIR="$W/lockdir3" CQLITE_TT_SCCACHE_LOCK_WAIT=60 bash "$W/t11h.sh" >/dev/null 2>&1 & hp=$!
      sleep 1
      t0=$SECONDS; kill -TERM "$hp" 2>/dev/null
      n=0; while [ "$n" -lt 100 ] && kill -0 "$hp" 2>/dev/null; do sleep 0.1; n=$((n + 1)); done
      alive=0; kill -0 "$hp" 2>/dev/null && alive=1
      wait "$hp" 2>/dev/null; hrc=$?
      if [ "$alive" = 0 ] && [ "$((SECONDS - t0))" -le 4 ] && [ -f "$W/term.mark" ] && [ "$(scc_pid_of)" = "$old" ]; then
        ok "#4479 TERM during a 60s lock wait: the prestart's trap ran and it exited in $((SECONDS - t0))s (rc $hrc), server untouched"
      else
        bad "#4479 TERM during a lock wait: still alive=$alive after $((SECONDS - t0))s (rc $hrc, mark=$([ -f "$W/term.mark" ] && echo yes || echo no))"
        kill -KILL "$hp" 2>/dev/null
      fi
    fi
    kill "$scc_holder" 2>/dev/null; wait "$scc_holder" 2>/dev/null
    scc_stop; scc_port=0
  fi
  # 11i. no lock available (the lock dir does not exist): NEVER stop the poisoned server; only the
  #      non-destructive start, with a log line saying recovery was skipped.
  scc_port=$(scc_free_port) || scc_port=0
  if [ "$scc_port" = 0 ]; then bad "#4479 no-lock: no free port"; else
    mkdir -p "$W/doomed4"
    scc_start_under "$W/doomed4"; old=$(scc_pid_of); rm -rf "$W/doomed4"
    scc_prestart_script "$W/t11i.sh"
    logline=$(env RUSTC_WRAPPER="$SCC" SCCACHE_SERVER_PORT=$scc_port SCCACHE_DIR="$W/sccdir" SCCACHE_CACHE_SIZE=1G \
      CQLITE_TT_SCCACHE_LOCK_DIR="$W/no-such-lock-dir" bash "$W/t11i.sh" 2>&1 >/dev/null)
    new=$(scc_pid_of)
    if [ -n "$old" ] && [ "$old" = "$new" ] && grep -q "recovery skipped: no lock available" <<<"$logline" && ! grep -q "holds a dead TMPDIR" <<<"$logline"; then
      ok "#4479 no lock available: the poisoned server is NOT stopped (pid $old kept) and the skip is logged"
    else
      bad "#4479 no lock available: old=$old new=$new log='$logline'"
    fi
    scc_stop; scc_port=0
  fi
fi
# 11j. #4479: _tt_sccache_bounded must END a lifecycle command that ignores TERM: TERM at the bound, a
#      short grace, then KILL to the SAME recorded pid. Command: `trap "" TERM; exec sleep 60` (pid recorded).
cat >"$W/t11j.sh" <<EOS
set -uo pipefail
. "$W/helpers.sh"
t0=\$SECONDS
_tt_sccache_bounded 1 bash -c 'trap "" TERM; echo \$\$ >"$W/ign.pid"; exec sleep 60'
echo "rc=\$? elapsed=\$((SECONDS - t0))"
EOS
res=$(bash "$W/t11j.sh" 2>&1); ign=$(cat "$W/ign.pid" 2>/dev/null)
elapsed=${res##*elapsed=}; jrc=${res#rc=}; jrc=${jrc%% *}
gone=0; [ -n "$ign" ] && ! kill -0 "$ign" 2>/dev/null && gone=1
if [ "$gone" = 1 ] && [ "$jrc" != 0 ] && [ "${elapsed:-99}" -le 4 ]; then
  ok "#4479 bounded: a TERM-ignoring command is KILLed after the grace (returned rc $jrc in ${elapsed}s, bound 1s + grace 2s; pid $ign gone)"
else
  bad "#4479 bounded: TERM-ignoring command not ended (res='$res' pid=$ign gone=$gone)"
  [ -z "$ign" ] || kill -KILL "$ign" 2>/dev/null
fi
echo "tooling-tests pool self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
