#!/usr/bin/env bash
# test_gate_liveness.sh — non-vacuity proof for the #3473 gate liveness mechanism:
# scripts/lib/gate-heartbeat.sh (the beater) and scripts/gate-liveness.sh (the reader).
#
# WHAT THIS HAS TO PROVE, and why the obvious test is not enough
# -------------------------------------------------------------
# The mechanism's whole job is to distinguish three states that #3041's INCOMPLETE
# sentinel collapses into one: queued/running, reaped, and finished. A test that only
# drives the happy path ("a live gate reads RUNNING") would pass against a reader
# hard-wired to say RUNNING — which is the fail-OPEN direction, and the direction that
# costs a lane a silently-lost 40-minute gate. So every state is asserted with its own
# green AND its own red, and the two dangerous confusions get dedicated cases:
#
#   * a MISSING heartbeat must read UNKNOWN, never REAPED. A gate predating this
#     mechanism, or one whose summary path is unwritable, produces the same absence,
#     and "absence of a beat" is not evidence of death (CLAUDE.md: never derive a
#     verdict from the absence of a bad signal).
#   * a beat left by a CONCURRENT PEER must read UNKNOWN, never RUNNING/COMPLETE —
#     the #2874 reader contract, which holds for a PASS block just as much as for a
#     beat.
#
# Hermetic: temp dirs only, no cargo, no datasets, no network, no gh. The one nested
# gate invocation is `--only file-size` (~2s, self-exempt from the #1825 slot, and
# --only file-size can never select tooling-tests, so it cannot recurse).
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
READER="$REPO_ROOT/scripts/gate-liveness.sh"
BEATER="$REPO_ROOT/scripts/lib/gate-heartbeat.sh"
GATE="$REPO_ROOT/scripts/agent-gate.sh"

TMP=$(mktemp -d "${TMPDIR:-/tmp}/gate-liveness-test.XXXXXX")
# Reap any beater this suite started, whatever the exit path: a leaked beater would
# outlive the test and keep rewriting a file under $TMP.
# shellcheck disable=SC2317
cleanup() {
  local p
  for p in $(cat "$TMP/beater-pids" 2>/dev/null); do kill "$p" 2>/dev/null || true; done
  chmod -R u+rwX "$TMP" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0; fail=0
# NOTE: these counters are incremented in the TOP-LEVEL shell only. Never wrap a case
# in `( … )` — a subshell's increments are discarded and the suite reports failed:0
# while printing FAILs (a real incident in this repo's tooling tests).
ok()   { pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -ge 2 ] && printf '     %s\n' "$2"; }

# run_reader <args...> -> sets RC and OUT
run_reader() {
  OUT=$(bash "$READER" "$@" 2>&1)
  RC=$?
}

# expect_reader <label> <want-status> <want-rc> <want-substring> -- <reader args...>
expect_reader() {
  local label="$1" want="$2" wantrc="$3" needle="$4"; shift 5
  run_reader "$@"
  if ! printf '%s' "$OUT" | grep -q "^gate-liveness: $want "; then
    bad "$label" "expected status $want, got: $(printf '%s' "$OUT" | head -1)"; return
  fi
  if [ "$RC" != "$wantrc" ]; then
    bad "$label" "expected exit $wantrc, got $RC"; return
  fi
  if [ -n "$needle" ] && ! printf '%s' "$OUT" | grep -q "$needle"; then
    bad "$label" "expected cause to mention '$needle', got: $(printf '%s' "$OUT" | head -1)"; return
  fi
  ok "$label"
}

# PORTABILITY (roborev job 157, Medium). macOS/BSD is a first-class gate host in this
# repo, and this suite is wired into the full gate's `tooling-tests` — so a GNU-only
# construct here does not fail "a test", it fails the GATE on every macOS host.
#
#   * `sed -i` needs a suffix argument on BSD (`sed -i '' -e …`) and rejects the GNU form,
#     so in-place editing is done by rewriting through a temp file instead. That is
#     portable everywhere and needs no per-platform branch at all.
#   * `timeout` is GNU coreutils and is absent from a stock macOS; coreutils installs it
#     as `gtimeout`. Resolved once, with an explicit no-timeout fallback rather than an
#     unconditional invocation that would be a "command not found".
#
# edit_lines <file> <sed-expression> — apply <sed-expression> to <file> in place, portably.
edit_lines() {
  local f="$1" expr="$2" tmpf
  tmpf="$f.edit.$$"
  sed "$expr" "$f" > "$tmpf" 2>/dev/null && mv -f "$tmpf" "$f"
  rm -f "$tmpf" 2>/dev/null || true
}

# TIMEOUT_CMD: the resolved timeout runner, or empty when this host has none. Callers use
# it unquoted-and-empty-safe via $TIMEOUT_CMD, so a host without it simply runs the
# command directly rather than failing.
TIMEOUT_CMD=""
if command -v timeout >/dev/null 2>&1; then TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then TIMEOUT_CMD="gtimeout"
fi

# mk_summary <path> <run-id> <result-line-or-empty>
mk_summary() {
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: $2"
    [ -n "${3:-}" ] && echo "RESULT: $3"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$1"
}
# NOTE what the #3473 descope removed from this suite. While the reader claimed `REAPED` it
# had to inspect a pid, so these tests needed a KNOWN-DEAD pid — and deriving one was itself
# a source of flakiness (the first attempt built it inside a command substitution, where the
# parent cannot reap it, so every death assertion could silently become UNKNOWN on a green
# suite). `STALLED` claims nothing about a process, so none of that machinery is needed and
# a whole class of test flakiness is gone with it.
# mk_beat <path> <run-id> <age-secs> [interval]
mk_beat() {
  local iv="${4:-20}"
  { echo "==== AGENT-GATE HEARTBEAT ===="
    echo "run-id: $2"
    echo "gate-pid: 4242"
    echo "parent-check: starttime"
    echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "interval: $iv"
    echo "beat-seq: 7"
    echo "beat-epoch: $(( $(date +%s) - $3 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$1"
}

echo "=== section 1: usage (a reader that guesses its subject is worse than one that refuses) ==="
run_reader;                                   [ "$RC" = 64 ] && ok "1.1 no args => 64"            || bad "1.1 no args => 64" "rc=$RC"
run_reader --bogus x;                          [ "$RC" = 64 ] && ok "1.2 unknown option => 64"     || bad "1.2 unknown option => 64" "rc=$RC"
run_reader /a /b;                              [ "$RC" = 64 ] && ok "1.3 two positionals => 64"    || bad "1.3 two positionals => 64" "rc=$RC"
run_reader --help;                             [ "$RC" = 0  ] && ok "1.4 --help => 0"              || bad "1.4 --help => 0" "rc=$RC"

echo "=== section 2: the terminal verdict set, enumerated (not assumed to be two values) ==="
i=0
for r in PASS FAIL PARTIAL ERROR REFUSED; do
  i=$((i+1)); f="$TMP/s-$r.txt"; mk_summary "$f" run-$r "$r"
  expect_reader "2.$i RESULT: $r => COMPLETE" COMPLETE 0 "terminal verdict" -- "$f"
done
# A verdict this reader does not know must NOT be classified. Fail-closed: a lane reads
# UNKNOWN and asks, rather than the reader inventing a meaning for a future value.
mk_summary "$TMP/s-weird.txt" run-w "MAGENTA"
expect_reader "2.6 unknown RESULT value => UNKNOWN" UNKNOWN 4 "unrecognised-result" -- "$TMP/s-weird.txt"
mk_summary "$TMP/s-nores.txt" run-n ""
expect_reader "2.7 no RESULT line => UNKNOWN" UNKNOWN 4 "no-result-line" -- "$TMP/s-nores.txt"

echo "=== section 3: the summary artifact itself ==="
expect_reader "3.1 missing summary => UNKNOWN" UNKNOWN 4 "no-summary-artifact" -- "$TMP/does-not-exist.txt"
if [ "$(id -u)" != 0 ]; then
  f="$TMP/s-noperm.txt"; mk_summary "$f" run-p "PASS"; chmod 000 "$f"
  expect_reader "3.2 unreadable summary => UNKNOWN" UNKNOWN 4 "summary-unreadable" -- "$f"
  chmod 644 "$f"
else
  echo "skip 3.2 unreadable summary (running as root: chmod 000 does not deny root)"
fi

echo "=== section 4: THE CORE STATE SPLIT — the three faces of INCOMPLETE ==="
# 4.1 is the case #3473 exists for: before this mechanism, this was the ONLY state, and
# it was indistinguishable from 4.2 and 4.3.
mk_summary "$TMP/a.txt" run-a "INCOMPLETE (gate did not finish)"
expect_reader "4.1 INCOMPLETE + no beat => UNKNOWN (absence is NOT death)" UNKNOWN 4 "no-heartbeat-artifact" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 5
expect_reader "4.2 INCOMPLETE + fresh beat => RUNNING" RUNNING 2 "alive" -- "$TMP/a.txt"
# interval 1 keeps the clock-independent confirmation wait at ~6s instead of ~25s; the
# confirmation itself is exercised on purpose in section 11g.
mk_beat "$TMP/a.txt.heartbeat" run-a 4000 1
expect_reader "4.3 INCOMPLETE + stale beat => STALLED" STALLED 3 "no liveness" -- "$TMP/a.txt"
# The INCOMPLETE (foreign) variant (#2874) is likewise not a verdict.
mk_summary "$TMP/b.txt" run-b "INCOMPLETE (foreign)"
mk_beat "$TMP/b.txt.heartbeat" run-b 5
expect_reader "4.4 INCOMPLETE (foreign) + fresh beat => RUNNING" RUNNING 2 "" -- "$TMP/b.txt"

echo "=== section 5: the staleness window is derived from the beat, and its boundary holds ==="
# The window is 3*interval with a 90s floor, read from the beat's OWN interval line, so
# the reader carries no duplicate of the gate's beat period.
mk_beat "$TMP/a.txt.heartbeat" run-a 89 20
expect_reader "5.1 age 89s, floor window 90s => RUNNING"  RUNNING 2 "" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 90 20
expect_reader "5.2 age 90s == window => RUNNING"          RUNNING 2 "" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 91 1
expect_reader "5.3 age 91s > the 90s floor => STALLED"    STALLED 3 "window 90s" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 179 60
expect_reader "5.4 interval 60 => window 180, age 179 RUNNING" RUNNING 2 "window 180s" -- "$TMP/a.txt"
# (The old 5.5 asserted the same 3x-interval derivation from the STALLED side with interval 60,
# which after the clock-independent confirmation would cost a 65s wait for coverage 5.4 already
# provides: age 179 can only read RUNNING if the window is >=179, i.e. 3x60. Dropped rather
# than paid for twice.)

echo "=== section 6: a peer's artifacts are never read as ours (#2874 reader contract) ==="
mk_summary "$TMP/p.txt" peer-run "PASS"
expect_reader "6.1 foreign run-id on a PASS => UNKNOWN, not COMPLETE" \
  UNKNOWN 4 "summary-run-id-mismatch" -- "$TMP/p.txt" --run-id my-run
mk_summary "$TMP/q.txt" my-run "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/q.txt.heartbeat" peer-run 5
expect_reader "6.2 foreign run-id on a fresh beat => UNKNOWN, not RUNNING" \
  UNKNOWN 4 "heartbeat-run-id-mismatch" -- "$TMP/q.txt" --run-id my-run
# With NO --run-id the reader still refuses when the two artifacts disagree: they
# describe different runs, so neither is evidence about the other.
expect_reader "6.3 summary/beat disagree, no --run-id => UNKNOWN" \
  UNKNOWN 4 "run-id-disagree" -- "$TMP/q.txt"
# Control: matching run-ids and a matching --run-id must still answer.
mk_beat "$TMP/q.txt.heartbeat" my-run 5
expect_reader "6.4 control: matching run-ids => RUNNING" RUNNING 2 "" -- "$TMP/q.txt" --run-id my-run

echo "=== section 7: malformed beats are UNKNOWN, never silently fresh ==="
mk_summary "$TMP/m.txt" run-m "INCOMPLETE (gate did not finish)"
hb="$TMP/m.txt.heartbeat"
mk_beat "$hb" run-m 5; edit_lines "$hb" 's/^run-id: .*//'
expect_reader "7.1 beat with no run-id => UNKNOWN" UNKNOWN 4 "heartbeat-no-run-id" -- "$TMP/m.txt"
mk_beat "$hb" run-m 5; edit_lines "$hb" 's/^beat-epoch: .*/beat-epoch: soon/'
expect_reader "7.2 non-numeric beat-epoch => UNKNOWN" UNKNOWN 4 "unparseable-epoch" -- "$TMP/m.txt"
mk_beat "$hb" run-m 5; edit_lines "$hb" 's/^interval: .*/interval: often/'
expect_reader "7.3 non-numeric interval => UNKNOWN" UNKNOWN 4 "unparseable-interval" -- "$TMP/m.txt"
mk_beat "$hb" run-m 5; edit_lines "$hb" 's/^interval: .*/interval: 0/'
expect_reader "7.4 interval 0 => UNKNOWN" UNKNOWN 4 "bad-interval" -- "$TMP/m.txt"
# A future-dated beat would otherwise be fresh FOREVER — a clock step or a hand-edited
# artifact must not buy an unlimited RUNNING.
mk_beat "$hb" run-m -600
expect_reader "7.5 beat dated in the future => UNKNOWN" UNKNOWN 4 "in-the-future" -- "$TMP/m.txt"
# ...but a beat a hair ahead of the reader's clock is ordinary jitter, not a fault.
mk_beat "$hb" run-m -5
expect_reader "7.6 beat 5s ahead (within one interval) => RUNNING" RUNNING 2 "" -- "$TMP/m.txt"
if [ "$(id -u)" != 0 ]; then
  mk_beat "$hb" run-m 5; chmod 000 "$hb"
  expect_reader "7.7 unreadable beat => UNKNOWN" UNKNOWN 4 "heartbeat-unreadable" -- "$TMP/m.txt"
  chmod 644 "$hb"
else
  echo "skip 7.7 unreadable beat (running as root)"
fi
# --heartbeat override: the default is a fixed suffix, but a caller may point elsewhere.
mk_beat "$TMP/elsewhere.hb" run-m 5; rm -f "$hb"
expect_reader "7.8 --heartbeat override is honoured" RUNNING 2 "" -- "$TMP/m.txt" --heartbeat "$TMP/elsewhere.hb"

echo "=== section 8: the beater's usage contract ==="
b_usage() { # b_usage <label> <args...>
  local label="$1"; shift
  local out rc
  out=$(bash "$BEATER" "$@" 2>&1); rc=$?
  [ "$rc" = 64 ] && ok "$label" || bad "$label" "expected 64, got $rc ($out)"
}
b_usage "8.1 no args => 64"
b_usage "8.2 missing --gate-pid => 64"       --file "$TMP/x" --run-id r
b_usage "8.3 missing --run-id => 64"         --file "$TMP/x" --gate-pid 1
b_usage "8.4 missing --file => 64"           --run-id r --gate-pid 1
b_usage "8.5 non-numeric --gate-pid => 64"   --file "$TMP/x" --run-id r --gate-pid self
b_usage "8.6 interval 0 => 64"               --file "$TMP/x" --run-id r --gate-pid 1 --interval 0
b_usage "8.7 unknown argument => 64"         --file "$TMP/x" --run-id r --gate-pid 1 --forever

echo "=== section 9: the beater beats for a live gate, and STOPS when the gate dies ==="
# This is the mechanism's load-bearing behaviour. A beater that outlived its gate would
# report a dead gate as RUNNING forever — #3473's own defect, one level down.
sleep_pid=""
start_fake_gate() {                     # a stand-in "gate" process we can kill on cue
  bash -c 'while :; do sleep 1; done' >/dev/null 2>&1 &
  sleep_pid=$!
}
start_fake_gate
hbf="$TMP/live.hb"
bash "$BEATER" --file "$hbf" --run-id live-run --gate-pid "$sleep_pid" --mode full --interval 1 \
  </dev/null >/dev/null 2>&1 &
beater_pid=$!
echo "$beater_pid" >> "$TMP/beater-pids"
# Wait for the first beat rather than assuming a timing (bounded, so a broken beater
# fails the case instead of hanging the suite).
for _ in 1 2 3 4 5 6 7 8 9 10; do [ -f "$hbf" ] && break; sleep 0.3; done
if [ -f "$hbf" ]; then
  ok "9.1 beater writes a beat for a live gate"
  grep -q "^run-id: live-run$"      "$hbf" && ok "9.2 beat carries the run-id"       || bad "9.2 beat carries the run-id" "$(cat "$hbf")"
  grep -q "^gate-pid: $sleep_pid$"  "$hbf" && ok "9.3 beat names the gate pid"       || bad "9.3 beat names the gate pid"
  grep -q "^mode: full$"            "$hbf" && ok "9.4 beat carries the mode"         || bad "9.4 beat carries the mode"
  grep -q "^parent-check: starttime$" "$hbf" && ok "9.5 /proc host => reuse-proof parent-check" \
    || ok "9.5 non-/proc host => parent-check kill0 (declared, not assumed)"
else
  bad "9.1 beater writes a beat for a live gate" "no beat file appeared"
fi
# Freshness control: the reader must see this live beat as RUNNING.
mk_summary "$TMP/live.txt" live-run "INCOMPLETE (gate did not finish)"
expect_reader "9.6 live beater => reader says RUNNING" RUNNING 2 "" -- "$TMP/live.txt" --heartbeat "$hbf" --run-id live-run
# Now kill the "gate". The beater must notice and EXIT, leaving the last beat to age.
kill -9 "$sleep_pid" 2>/dev/null; wait "$sleep_pid" 2>/dev/null
beater_gone=no
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  kill -0 "$beater_pid" 2>/dev/null || { beater_gone=yes; break; }
  sleep 0.4
done
[ "$beater_gone" = yes ] && ok "9.7 beater exits when its gate dies" \
                         || bad "9.7 beater exits when its gate dies" "beater $beater_pid still alive"
# And the beat must not advance after the gate's death: a terminating beater that wrote
# one last beat would date the gate's liveness to the moment it was killed.
before=$(grep '^beat-epoch: ' "$hbf" 2>/dev/null)
sleep 2
after=$(grep '^beat-epoch: ' "$hbf" 2>/dev/null)
[ "$before" = "$after" ] && ok "9.8 no beat is written after the gate dies" \
                         || bad "9.8 no beat is written after the gate dies" "$before -> $after"

echo "=== section 10: the beater refuses to beat for a gate that is ALREADY dead ==="
# Affirmative liveness, not "absence of a death signal": with a dead pid there must be
# no artifact at all, so a reader reports UNKNOWN (no beat) rather than RUNNING.
start_fake_gate; dead_pid="$sleep_pid"
kill -9 "$dead_pid" 2>/dev/null; wait "$dead_pid" 2>/dev/null
deadf="$TMP/dead.hb"
bash "$BEATER" --file "$deadf" --run-id dead-run --gate-pid "$dead_pid" --interval 1 </dev/null >/dev/null 2>&1
[ ! -f "$deadf" ] && ok "10.1 dead gate pid => no beat is ever written" \
                  || bad "10.1 dead gate pid => no beat is ever written" "$(cat "$deadf")"

echo "=== section 11: the /proc starttime parser, differentially vs awk ==="
# The reuse-proofing rests entirely on field 22 of /proc/<pid>/stat, whose field 2 may
# contain spaces AND parens. A port is only as good as the original it was tested
# against (CLAUDE.md, #3283), so the parser is compared against an INDEPENDENT
# implementation over every pid on this host, not against a model of one.
if [ -d /proc/1 ]; then
  fn=$(sed -n '/^_starttime() {$/,/^}$/p' "$BEATER")
  if ! printf '%s' "$fn" | grep -q '_starttime()'; then
    # A failed derivation is a FAIL naming the derivation, never a silent skip: a
    # renamed function would otherwise make this whole section vacuous.
    bad "11.0 extract _starttime from the beater" "sed found no _starttime() { ... } block in $BEATER"
  else
    ok "11.0 extract _starttime from the beater"
    harness="$TMP/starttime-harness.sh"
    { echo 'set -uo pipefail'; printf '%s\n' "$fn"
      echo 'for p in "$@"; do printf "%s %s\n" "$p" "$(_starttime "$p" 2>/dev/null)"; done'
    } > "$harness"
    pids=$(ls -1 /proc 2>/dev/null | grep -E '^[0-9]+$' | head -400)
    mismatch=0; compared=0
    while read -r pid mine; do
      theirs=$(awk '{ for(i=NF;i>0;i--) if ($i ~ /\)$/) { print $(i+20); exit } }' "/proc/$pid/stat" 2>/dev/null)
      # A pid that exited between the two reads yields empty on one side; that is a
      # race, not a disagreement, so it is not counted either way.
      [ -n "$mine" ] && [ -n "$theirs" ] || continue
      compared=$((compared+1))
      [ "$mine" = "$theirs" ] || { mismatch=$((mismatch+1)); echo "     pid $pid: beater=$mine awk=$theirs"; }
    done <<< "$(bash "$harness" $pids)"
    # An empty comparison set is a FAILED measurement, not a pass.
    if [ "$compared" -lt 5 ]; then
      bad "11.1 parser matches awk over live pids" "only $compared pids compared — measurement did not happen"
    elif [ "$mismatch" -eq 0 ]; then
      ok "11.1 parser matches awk over $compared live pids"
    else
      bad "11.1 parser matches awk over live pids" "$mismatch of $compared disagreed"
    fi
    # The shape a naive `awk '{print $22}'` gets wrong: a comm containing BOTH a space
    # and a ')'. Fields below are numbered as /proc/<pid>/stat numbers them: f1=pid,
    # f2=comm, f3=state, so starttime (f22) is the 20th token AFTER the comm. The 18
    # fillers below are f4..f21, putting 987654 at f22 exactly.
    weird='4242 (my ) proc) S 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 987654 23 24'
    got=$(printf '%s' "$weird" | { read -r raw; rest="${raw##*) }"; set -- $rest; printf '%s' "${20}"; })
    [ "$got" = 987654 ] && ok "11.2 comm with a space and ')' parses to field 22" \
                        || bad "11.2 comm with a space and ')' parses to field 22" "got '$got', want 987654"
    # RED control for 11.2: the naive whole-line `$22` reads INTO the comm on this
    # input, so the case above is proving something a wrong parser would fail.
    naive=$(printf '%s\n' "$weird" | awk '{print $22}')
    [ "$naive" != 987654 ] && ok "11.3 naive awk \$22 is wrong on this input (case 11.2 is non-vacuous)" \
                           || bad "11.3 naive awk \$22 is wrong on this input" "naive also got 987654 — 11.2 proves nothing"
  fi
else
  echo "skip section 11 (no /proc on this host)"
fi

echo "=== section 12: structural asserts on the gate wiring ==="
# Behavioural cases only cover the shapes someone already thought of, so the two
# invariants that are easy to silently undo are asserted against the source.
#
# (a) bash traps do NOT compose: a second `trap ... EXIT` REPLACES the first. The slot
#     trap used to be armed on its own; if anyone re-adds that bare form, the beater is
#     orphaned on every full gate and outlives it.
if grep -qE "^[[:space:]]*trap '_gate_release_slot' EXIT" "$GATE"; then
  bad "12.1 no bare _gate_release_slot EXIT trap" "found one — it would replace the composed _gate_atexit trap and orphan the beater"
else
  ok "12.1 no bare _gate_release_slot EXIT trap"
fi
grep -qE "^[[:space:]]*trap '_gate_atexit' EXIT" "$GATE" && ok "12.2 the composed _gate_atexit trap is armed" \
  || bad "12.2 the composed _gate_atexit trap is armed" "not found in $GATE"
# (b) the EXIT trap is inherited by backgrounded pool subshells (#1737), so both
#     at-exit helpers need the BASHPID guard or a pool subshell's exit kills the
#     parent's beater and freezes the beat under a LIVE gate.
for f in _hb_stop _hb_ensure; do
  body=$(sed -n "/^$f() {\$/,/^}\$/p" "$GATE")
  if ! printf '%s' "$body" | grep -q "$f()"; then
    bad "12.3 $f is present and extractable" "no $f() { ... } block in $GATE"
  elif printf '%s' "$body" | grep -q 'BASHPID:-\$\$'; then
    ok "12.3 $f carries the BASHPID pool-subshell guard"
  else
    bad "12.3 $f carries the BASHPID pool-subshell guard" "guard missing from $f"
  fi
done
# (c) no opt-out env var may widen the staleness window or disable the beat: that is
#     the escape hatch that would buy a vacuous "RUNNING" for a dead gate.
if grep -qE 'CQLITE_GATE_(DISABLE_HEARTBEAT|HEARTBEAT_INTERVAL)|AGENT_GATE_HEARTBEAT' "$GATE" "$READER" "$BEATER"; then
  bad "12.4 no heartbeat opt-out env var" "found an env override for the heartbeat"
else
  ok "12.4 no heartbeat opt-out env var"
fi

echo "=== section 11b: the three roborev job-155 findings, each with a RED control ==="
# All three were shapes this repo already documents, reproduced here one layer down, so
# each gets a case that would have FAILED before the fix.
#
# (a) Low — the verdict was matched by PREFIX GLOB (`'RESULT: PASS'*`), which accepts
#     `RESULT: PASSENGER`: a SPELLING test wearing a closed grammar's clothes. CLAUDE.md
#     records the identical defect in the roborev wrapper's own verdict scan (`PASS*`
#     accepting `PASSthisNeverRan`). The value is now reduced to its verdict TOKEN and
#     matched EXACTLY.
i=0
for bogus in PASSENGER FAILURE PARTIALLY ERRORS REFUSEDLY INCOMPLETEISH PASS_ FAILs; do
  i=$((i+1)); f="$TMP/tok-$bogus.txt"; mk_summary "$f" run-t "$bogus"
  expect_reader "11b.$i RESULT: $bogus => UNKNOWN (token matched exactly, not by prefix)" \
    UNKNOWN 4 "unrecognised-result" -- "$f"
done
# Controls: the real tokens must still be recognised, bare AND with trailing detail.
mk_summary "$TMP/tok-ok1.txt" run-t "PASS"
expect_reader "11b.9 control: bare PASS still COMPLETE" COMPLETE 0 "" -- "$TMP/tok-ok1.txt"
mk_summary "$TMP/tok-ok2.txt" run-t "FAIL (3 components)"
expect_reader "11b.10 control: FAIL with trailing detail still COMPLETE" COMPLETE 0 "" -- "$TMP/tok-ok2.txt"
mk_summary "$TMP/tok-ok3.txt" run-t "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/tok-ok3.txt.heartbeat" run-t 5
expect_reader "11b.11 control: INCOMPLETE with trailing detail still consults the beat" \
  RUNNING 2 "" -- "$TMP/tok-ok3.txt"

# (b) Medium — with --run-id supplied but NO `run-id:` line in the summary, validation
#     was skipped entirely and the terminal verdict was attributed to the requested run.
#     A permissive branch keyed on the ABSENCE of the bad signal — the shape CLAUDE.md
#     forbids. The binding is only a guarantee if it is unconditional.
noid="$TMP/noid.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "RESULT: PASS"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$noid"
expect_reader "11b.12 --run-id given + summary has NO run-id => UNKNOWN, not COMPLETE" \
  UNKNOWN 4 "summary-no-run-id" -- "$noid" --run-id my-run
# Control: with no --run-id there is nothing to bind to, so an id-less summary still answers.
expect_reader "11b.13 control: no --run-id + id-less summary => COMPLETE" \
  COMPLETE 0 "" -- "$noid"
# And the same demand holds for a non-terminal summary reaching the heartbeat.
noid2="$TMP/noid2.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "RESULT: INCOMPLETE (gate did not finish)"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$noid2"
mk_beat "$noid2.heartbeat" my-run 5
expect_reader "11b.14 --run-id given + id-less INCOMPLETE summary => UNKNOWN, not RUNNING" \
  UNKNOWN 4 "summary-no-run-id" -- "$noid2" --run-id my-run

# (c) Medium — fields were read by RE-OPENING the path once per field. These are SHARED
#     paths that peers replace ATOMICALLY, so each field could come from a DIFFERENT
#     version of the file: one run's run-id combined with another's RESULT or a fresher
#     beat-epoch, yielding a confident verdict about a state no run was ever in.
#
#     Asserted STRUCTURALLY. A true interleaving cannot be scheduled deterministically
#     from shell, and a probabilistic loop would be a flaky wall-clock-ish test of the
#     kind this repo's roborev-lints reject; the invariant "one open per artifact" is
#     exactly checkable at the source, and it is the property that makes the race
#     impossible rather than merely unlikely.
if grep -nE '_field "\$(SUMMARY|HB)"' "$READER" >/dev/null 2>&1; then
  bad "11b.15 no field is read by re-opening the artifact path" \
      "$(grep -nE '_field "\$(SUMMARY|HB)"' "$READER" | head -3)"
else
  ok "11b.15 no field is read by re-opening the artifact path"
fi
if grep -nE 'grep [^|]*"\$(SUMMARY|HB)"' "$READER" >/dev/null 2>&1; then
  bad "11b.16 no grep reads the artifact path directly" \
      "$(grep -nE 'grep [^|]*"\$(SUMMARY|HB)"' "$READER" | head -3)"
else
  ok "11b.16 no grep reads the artifact path directly"
fi
# Exactly one snapshot per artifact — and the derivation must find them, or this is
# vacuous: a renamed helper would silently satisfy the two negative checks above.
sl=$(grep -cE '^[A-Z_]+_TEXT=\$\(_slurp(_settled)? "\$(SUMMARY|HB)"\)$' "$READER")
[ "$sl" -eq 2 ] && ok "11b.17 each artifact is snapshotted exactly once (found $sl)" \
                || bad "11b.17 each artifact is snapshotted exactly once" "found $sl _slurp assignments, want 2"

echo "=== section 11c: the four roborev job-157 findings, each with a control ==="
# (a) Medium — a TERMINAL result in a TRUNCATED block was reported COMPLETE. emit_summary
#     verifies its own end marker precisely because the single-`>` write can be cut short
#     (ENOSPC, or a kill between the RESULT line and the closing marker), and such an
#     artifact is PERMANENTLY unfinished — so accepting it reports a verdict the gate never
#     published.
trunc="$TMP/trunc.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-x"; echo "RESULT: PASS"; } > "$trunc"
expect_reader "11c.1 terminal RESULT with no end marker => UNKNOWN, not COMPLETE" \
  UNKNOWN 4 "summary-truncated" -- "$trunc"
nostart="$TMP/nostart.txt"
{ echo "run-id: run-x"; echo "RESULT: PASS"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$nostart"
expect_reader "11c.2 terminal RESULT with no start marker => UNKNOWN" \
  UNKNOWN 4 "summary-no-start-marker" -- "$nostart"
# Controls: all three real marker dialects must still be accepted.
for m in "AGENT-GATE SUMMARY" "AGENT-GATE LITE SUMMARY" "AGENT-GATE DELTA SUMMARY"; do
  f="$TMP/mk-$(echo "$m" | tr ' ' '_').txt"
  { echo "==== $m ===="; echo "run-id: run-x"; echo "RESULT: PASS"; echo "==== END $m ===="; } > "$f"
  expect_reader "11c.3 control: '$m' framing is accepted" COMPLETE 0 "" -- "$f"
done
# A truncated INCOMPLETE block is NOT rejected on these grounds — it falls through to the
# heartbeat, which is the conservative direction and must stay reachable.
tinc="$TMP/trunc-inc.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-y"; echo "RESULT: INCOMPLETE (gate did not finish)"; } > "$tinc"
mk_beat "$tinc.heartbeat" run-y 5
expect_reader "11c.4 truncated INCOMPLETE still consults the beat (asymmetry is deliberate)" \
  RUNNING 2 "" -- "$tinc"

# (b) Medium — a STALE beat alone was reported REAPED. Fixed by DESCOPING the death claim
#     rather than by inspecting the process: see the note in gate-liveness.sh. What must hold
#     now is that a stale beat yields STALLED, that STALLED says plainly it is NOT a death
#     claim (so nobody re-runs a gate on it reflexively), and that it works on EVERY host —
#     the previous pid/host/boot machinery could not, which is what made these cases fail
#     deterministically on macOS.
mk_summary "$TMP/st.txt" run-S "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/st.txt.heartbeat" run-S 4000 1
expect_reader "11c.5 stale beat => STALLED" STALLED 3 "no liveness" -- "$TMP/st.txt"
run_reader "$TMP/st.txt"
printf '%s' "$OUT" | grep -q 'NOT a claim that the process is dead' \
  && ok "11c.6 STALLED states it is NOT a death claim" \
  || bad "11c.6 STALLED states it is NOT a death claim" "$(printf '%s' "$OUT" | head -1)"
printf '%s' "$OUT" | grep -q 'relaunches it at the next component boundary' \
  && ok "11c.7 STALLED explains the beater-recovery path instead of advising a re-run" \
  || bad "11c.7 STALLED explains the beater-recovery path" "$(printf '%s' "$OUT" | head -1)"
# Host-independence, asserted at the source: no verdict may depend on /proc, a pid check or a
# machine identity, or this suite becomes host-dependent again.
if grep -nE '/proc|kill -0|boot-id|boot_id|gate-starttime' "$READER" | grep -vE '^[0-9]+:#' >/dev/null 2>&1; then
  bad "11c.8 no verdict depends on /proc, a pid probe or machine identity" \
      "$(grep -nE '/proc|kill -0|boot-id|boot_id|gate-starttime' "$READER" | grep -vE '^[0-9]+:#' | head -3)"
else
  ok "11c.8 no verdict depends on /proc, a pid probe or machine identity"
fi
# ...and the verdict vocabulary must not quietly regain REAPED.
if grep -qE '^\s*verdict REAPED' "$READER"; then
  bad "11c.9 the reader emits no REAPED verdict" "the descoped death claim has returned"
else
  ok "11c.9 the reader emits no REAPED verdict"
fi

# (d) Medium — PORTABILITY. This suite is wired into the full gate's tooling-tests, and
#     macOS/BSD is a first-class gate host, so a GNU-only construct here fails the GATE on
#     every macOS box rather than merely failing a test. Asserted structurally over BOTH
#     suites: behavioural coverage cannot see a platform this run is not on.
port_bad=0
for f in "$REPO_ROOT/scripts/tests/test_gate_liveness.sh" "$REPO_ROOT/scripts/tests/test_gate_detached.sh"; do
  # strip comments before scanning, so the explanatory prose above is not a false hit
  body=$(sed 's/[[:space:]]*#.*$//' "$f")
  # The needle is SPLIT so this guard cannot match its own source line — the first
  # version did exactly that and reported a violation against itself.
  _sed_needle="sed"' -i'
  if printf '%s\n' "$body" | grep -qE "(^|[^a-zA-Z_])$_sed_needle"; then
    bad "11c.12 no GNU-only in-place sed in $(basename "$f")" "found one (BSD sed needs a suffix argument)"; port_bad=1
  fi
  if printf '%s\n' "$body" | grep -qE '(^|[^A-Z_$])timeout [0-9]'; then
    bad "11c.13 no unconditional 'timeout N' in $(basename "$f")" "stock macOS has no timeout(1)"; port_bad=1
  fi
done
[ "$port_bad" -eq 0 ] && ok "11c.12/13 both suites are free of GNU-only in-place sed and bare timeout"

echo "=== section 11d: the surviving roborev job-160 finding (the other two were descoped) ==="
# job 160's cross-host finding and job 162's hostname-collision finding are both GONE by
# construction: the reader no longer inspects a process at all, so there is no host to prove.
# Asserted by 11c.8/11c.9 above rather than by more cases here.

# (b) Medium — the "atomic snapshot" claim held only for rename-published files. The SUMMARY
#     is written in place with `>`, so a reader can observe a PREFIX of a block being
#     written. Truncation is already rejected by the mandatory end-marker check (11c.1), so
#     a torn read degrades to UNKNOWN rather than a wrong COMPLETE; `_slurp_settled` then
#     re-reads ONCE so the common "caught mid-write" case resolves correctly.
#
#     Asserted structurally: a real interleaving cannot be scheduled deterministically from
#     shell, and a timing-based case would be the flaky wall-clock shape roborev-lints
#     reject. The properties that matter are exactly checkable at the source.
if grep -q 'SUM_TEXT=$(_slurp_settled "$SUMMARY")' "$READER"; then
  ok "11d.7 the summary is read through the settle-retry reader"
else
  bad "11d.7 the summary is read through the settle-retry reader" "not found"
fi
retries=$(grep -c 'sleep 0.2' "$READER")
[ "$retries" -eq 1 ] && ok "11d.8 the settle retry is bounded to exactly one re-read" \
                     || bad "11d.8 the settle retry is bounded to exactly one re-read" "found $retries pauses, want 1"
# The claim itself must no longer be over-stated in the comment — a false justification is
# how this defect survived review once already.
if grep -q 'is NOT published that way' "$READER"; then
  ok "11d.9 the comment states the summary is NOT atomically published"
else
  bad "11d.9 the comment states the summary is NOT atomically published" "the over-claim may have returned"
fi

echo "=== section 11e: the interleaved-write blend (roborev job 164) ==="
# The previous revision of gate-liveness.sh asserted a blend was IMPOSSIBLE "because O_TRUNC
# resets the length and content is written forward". That was false: two writers hold
# INDEPENDENT file offsets, so if B truncates while A is mid-block, A's next write lands at
# ITS old offset and the file becomes B's opener + a sparse hole + A's tail. A reader could
# then pair one run's run-id with another run's RESULT and end marker — a FALSE COMPLETE, the
# worst verdict this script can produce.
#
# Built here by performing the ACTUAL interleaving with two file descriptors, not by
# hand-writing a file that merely resembles one — the point is to reproduce the mechanism.
blend="$TMP/blend.txt"
{
  exec 3> "$blend"
  printf '==== AGENT-GATE SUMMARY ====\nrun-id: run-AAAA\n' >&3
  printf 'padding %.0s' $(seq 1 40) >&3
  exec 4> "$blend"                     # writer B truncates; A keeps its offset
  printf '==== AGENT-GATE SUMMARY ====\nrun-id: run-BBBB\n' >&4
  exec 4>&-
  printf '\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ====\n' >&3   # A's tail, past the hole
  exec 3>&-
}
if LC_ALL=C tr -d '\000' < "$blend" | cmp -s - "$blend"; then
  bad "11e.0 the fixture really is a blended file (contains a sparse hole)" "no NUL bytes — the interleaving did not reproduce, so 11e.1 would be vacuous"
else
  ok "11e.0 the fixture really is a blended file (contains a sparse hole)"
  # It carries B's run-id AND A's terminal RESULT AND a valid end marker — i.e. it would have
  # satisfied every framing check the previous revision applied.
  expect_reader "11e.1 a genuinely blended summary => UNKNOWN, never COMPLETE" \
    UNKNOWN 4 "summary-contains-nul" -- "$blend"
  expect_reader "11e.2 ...and still UNKNOWN when a run-id is demanded" \
    UNKNOWN 4 "summary-contains-nul" -- "$blend" --run-id run-BBBB
fi
# The structural half, independent of NULs: more than one of any framing element means the
# file holds fragments of more than one write.
dup="$TMP/dup.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-A"
  echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-B"
  echo "RESULT: PASS"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$dup"
expect_reader "11e.3 two openers / two run-ids => UNKNOWN (not a single block)" \
  UNKNOWN 4 "summary-not-a-single-block" -- "$dup"
dup2="$TMP/dup2.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-A"; echo "RESULT: INCOMPLETE (x)"
  echo "RESULT: PASS"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$dup2"
expect_reader "11e.4 two RESULT lines => UNKNOWN (not a single block)" \
  UNKNOWN 4 "summary-not-a-single-block" -- "$dup2"
# A NUL in the HEARTBEAT is rejected the same way.
mk_summary "$TMP/hbn.txt" run-N "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/hbn.txt.heartbeat" run-N 5
printf '\000' >> "$TMP/hbn.txt.heartbeat"
expect_reader "11e.5 a NUL-bearing heartbeat => UNKNOWN" UNKNOWN 4 "heartbeat-contains-nul" -- "$TMP/hbn.txt"
# CONTROLS: ordinary artifacts must be unaffected by both checks.
mk_summary "$TMP/ctl.txt" run-C "PASS"
expect_reader "11e.6 control: an ordinary complete block is still COMPLETE" COMPLETE 0 "" -- "$TMP/ctl.txt"
mk_summary "$TMP/ctl2.txt" run-C "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/ctl2.txt.heartbeat" run-C 5
expect_reader "11e.7 control: an ordinary fresh beat is still RUNNING" RUNNING 2 "" -- "$TMP/ctl2.txt"
# And the over-claim must not come back in the comment.
if grep -q 'It cannot observe a blend' "$READER"; then
  bad "11e.8 the 'cannot blend' over-claim is gone from the comment" "it has returned"
else
  ok "11e.8 the 'cannot blend' over-claim is gone from the comment"
fi

echo "=== section 11g: liveness is decided by counter progression, not by comparing clocks ==="
# roborev job 166, Medium. `AGE` compares the WRITER's beat-epoch against the READER's clock.
# Nothing guarantees they agree, and the documented response to a persistent STALLED is
# "relaunch" — so a gate host running behind could cause a DUPLICATE gate launch. Rather than
# special-case skew (the third cross-machine assumption to bite this script), the STALLED
# decision now watches `beat-seq` advance over a window THIS process times: only the reader's
# clock is used for the wait, only the writer's counter for progress, and the two are never
# compared.
mk_summary "$TMP/skew.txt" run-K "INCOMPLETE (gate did not finish)"
# A beat that LOOKS ancient by epoch, but whose counter is advancing — i.e. exactly what a
# clock-skewed live gate produces. A background writer bumps beat-seq while the reader waits.
mk_beat "$TMP/skew.txt.heartbeat" run-K 99999 1
( sleep 2
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-K"; echo "gate-pid: 4242"
    echo "interval: 1"; echo "beat-seq: 8"; echo "beat-epoch: $(( $(date +%s) - 99999 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$TMP/skew.txt.heartbeat"
) &
adv_pid=$!; echo "$adv_pid" >> "$TMP/pids"
expect_reader "11g.1 ancient epoch but ADVANCING beat-seq => RUNNING (clocks disagree, writer alive)" \
  RUNNING 2 "beat-seq advanced" -- "$TMP/skew.txt"
wait "$adv_pid" 2>/dev/null || true
# A beat that is stale AND whose counter does not move is STALLED — and the text must say the
# decision came from progression, not from the clock comparison.
mk_beat "$TMP/skew.txt.heartbeat" run-K 99999 1
expect_reader "11g.2 stale epoch AND static beat-seq => STALLED" STALLED 3 "did NOT advance" -- "$TMP/skew.txt"
# A counter that advances but under a DIFFERENT run-id is a peer's beat, not ours.
mk_beat "$TMP/skew.txt.heartbeat" run-K 99999 1
( sleep 2
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: SOMEONE-ELSE"; echo "gate-pid: 4242"
    echo "interval: 1"; echo "beat-seq: 99"; echo "beat-epoch: $(date +%s)"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$TMP/skew.txt.heartbeat"
) &
adv2=$!; echo "$adv2" >> "$TMP/pids"
expect_reader "11g.3 progression under a FOREIGN run-id does not count as ours" \
  STALLED 3 "did NOT advance" -- "$TMP/skew.txt"
wait "$adv2" 2>/dev/null || true
# The confirmation wait must be bounded regardless of what the artifact claims.
if grep -qE '_confirm_wait"? -le 65' "$READER"; then
  ok "11g.4 the confirmation wait is hard-capped (a hostile interval cannot stretch it)"
else
  bad "11g.4 the confirmation wait is hard-capped" "no cap found"
fi
# A fresh beat must NOT pay the confirmation cost. Asserted STRUCTURALLY, not by timing the
# run: a wall-clock threshold in a correctness test is exactly the flaky shape this repo's
# roborev-lints reject (a loaded box makes it red for no reason), and the property is exactly
# checkable — the confirmation `sleep` must sit BELOW the fresh-beat RUNNING verdict, so a
# fresh beat returns before reaching it.
_run_ln=$(grep -n 'verdict RUNNING 2 "this run beat' "$READER" | head -1 | cut -d: -f1)
_slp_ln=$(grep -n 'sleep "$_confirm_wait"' "$READER" | head -1 | cut -d: -f1)
if [ -n "$_run_ln" ] && [ -n "$_slp_ln" ] && [ "$_slp_ln" -gt "$_run_ln" ]; then
  ok "11g.5 the confirmation wait sits below the fresh-beat verdict (a fresh beat never waits)"
else
  bad "11g.5 the confirmation wait sits below the fresh-beat verdict" \
      "RUNNING at line ${_run_ln:-?}, sleep at line ${_slp_ln:-?}"
fi
# ...and there is exactly ONE such wait, so no path can pay it twice.
_nslp=$(grep -c 'sleep "$_confirm_wait"' "$READER")
[ "$_nslp" -eq 1 ] && ok "11g.6 exactly one confirmation wait exists" \
                   || bad "11g.6 exactly one confirmation wait exists" "found $_nslp"

echo "=== section 11f: predictable temp files, closed as a RULE not per site ==="
# The same shape appeared THREE times in this change: the default /tmp artifact names, the
# beater's sibling temp, and the launcher's probe. Each was a predictable path opened with
# `>`, which follows symlinks — so a pre-created symlink gets truncated. Fixing instances
# one at a time is what let it recur, so the rule is pinned at the shared definition:
# no script in this change may build a temp path from $$ .
tf_bad=0
for f in "$REPO_ROOT/scripts/gate-liveness.sh" "$REPO_ROOT/scripts/lib/gate-heartbeat.sh" \
         "$REPO_ROOT/scripts/flow/gate-detached.sh"; do
  body=$(sed 's/[[:space:]]*#.*$//' "$f")
  if printf '%s\n' "$body" | grep -qE '\.\$\$'; then
    bad "11f.1 no temp path is built from \$\$ in $(basename "$f")" \
        "$(printf '%s\n' "$body" | grep -nE '\.\$\$' | head -2)"; tf_bad=1
  fi
done
[ "$tf_bad" -eq 0 ] && ok "11f.1 no temp path is built from \$\$ in any of the three scripts"
# ...and every temp that IS created goes through mktemp.
mk=$(grep -c 'mktemp' "$REPO_ROOT/scripts/lib/gate-heartbeat.sh" "$REPO_ROOT/scripts/flow/gate-detached.sh" | awk -F: '{t+=$2} END{print t}')
[ "$mk" -ge 3 ] && ok "11f.2 temp creation goes through mktemp ($mk call sites)" \
                || bad "11f.2 temp creation goes through mktemp" "only $mk mktemp references"

echo "=== section 12b: an early-exiting gate emits no undefined-function noise ==="
# The EXIT trap is armed thousands of lines above where _gate_release_slot is DEFINED,
# and bash defines functions as it reads the file. So every early-exit path — including
# every --emit-summary-selftest run — would print `_gate_release_slot: command not found`
# onto the gate's own stderr. That is not cosmetic: scripts/tests/test_agent_gate_summary.sh
# reads any `command not found` on the gate's stderr as a MISSING TOOL under its minimal
# PATH, so the noise fails an unrelated case with a misleading cause (measured, on this
# change). Pinned here at the source rather than only there, where it looks like a
# toolchain problem.
selftest_err="$TMP/selftest.err"
bash "$GATE" --emit-summary-selftest >/dev/null 2>"$selftest_err"
if grep -q 'command not found' "$selftest_err"; then
  bad "12.5 an early-exiting gate emits no 'command not found'" "$(grep -m3 'command not found' "$selftest_err")"
else
  ok "12.5 an early-exiting gate emits no 'command not found'"
fi
# Non-vacuity: the probe must be reading a stream that CAN carry the message. If the
# selftest produced nothing at all on stderr the case above is trivially satisfiable, so
# assert the invocation actually ran by checking it emitted a summary block on stdout.
sel_out="$TMP/selftest.out"
bash "$GATE" --emit-summary-selftest >"$sel_out" 2>/dev/null
grep -q 'AGENT-GATE' "$sel_out" \
  && ok "12.6 the selftest invocation really ran (case 12.5 is non-vacuous)" \
  || bad "12.6 the selftest invocation really ran" "no AGENT-GATE block on stdout"

echo "=== section 13: end-to-end through the real gate (wiring evidence) ==="
# A mechanism is only done when its PUBLIC surface exercises it. --only file-size is
# ~2s, self-exempt from the #1825 slot, and cannot select tooling-tests (no recursion).
e2e="$TMP/e2e-summary.txt"
if AGENT_GATE_SUMMARY_FILE="$e2e" $TIMEOUT_CMD ${TIMEOUT_CMD:+600} bash "$GATE" --only file-size >"$TMP/e2e.log" 2>&1 </dev/null; then :; fi
if [ ! -f "$e2e" ]; then
  bad "13.1 real gate writes its summary" "no $e2e (last 20 lines: $(tail -20 "$TMP/e2e.log"))"
else
  ok "13.1 real gate writes its summary"
  rid=$(grep -m1 '^run-id: ' "$e2e" | sed 's/^run-id: //')
  [ -f "$e2e.heartbeat" ] && ok "13.2 real gate publishes a heartbeat at <summary>.heartbeat" \
    || bad "13.2 real gate publishes a heartbeat at <summary>.heartbeat" "absent"
  if [ -f "$e2e.heartbeat" ]; then
    grep -q "^run-id: $rid$" "$e2e.heartbeat" && ok "13.3 beat run-id matches the summary's" \
      || bad "13.3 beat run-id matches the summary's" "$(grep '^run-id: ' "$e2e.heartbeat")"
    grep -q '^mode: only$' "$e2e.heartbeat" && ok "13.4 an --only run stamps mode: only, never full" \
      || bad "13.4 an --only run stamps mode: only, never full" "$(grep '^mode: ' "$e2e.heartbeat")"
  fi
  # The block must DECLARE the mechanism ran — a pasted SUMMARY with no heartbeat line
  # is indistinguishable from one whose beater was never wired.
  grep -q '^heartbeat: on file: ' "$e2e" && ok "13.5 SUMMARY declares heartbeat: on" \
    || bad "13.5 SUMMARY declares heartbeat: on" "$(grep '^heartbeat' "$e2e")"
  # And the reader, bound to that run-id, must resolve it.
  expect_reader "13.6 reader resolves the real run" COMPLETE 0 "terminal verdict" -- "$e2e" --run-id "$rid"
  # No beater may outlive the gate that started it.
  if [ -n "${rid:-}" ] && pgrep -f "gate-heartbeat.sh --file $e2e" >/dev/null 2>&1; then
    bad "13.7 no beater outlives the gate" "a beater for $e2e is still running"
  else
    ok "13.7 no beater outlives the gate"
  fi
fi

echo "=== section 14: the heartbeat must not make the gate fail ITSELF (#2926 x #3473) ==="
# The gate's tree-integrity guard hashes every untracked NON-IGNORED path in the checkout
# and FAILs closed if the identity changes mid-run. The heartbeat is a file the gate
# writes into the checkout — every 20s, for the whole run, whenever the summary path is
# the checkout default or any in-repo path. Without an explicit carve-out the gate creates
# it after the start capture and then FAILs ITSELF with `tree-mutated-midrun`, naming its
# own heartbeat as the mutation.
#
# This was NOT caught by a short `--only` run: file-size completes in ~0s, so the first
# beat can land after the last boundary check and the run passes by RACE. On a 30-50 min
# full gate the beat always precedes the first boundary, so it would be a DETERMINISTIC
# failure of the gate of record. Hence a deterministic probe here — the gate's own
# `AGENT_GATE_TREE_SELFTEST=capture` hook, which prints the identity it would compare —
# instead of a timing-dependent end-to-end run.
#
# Run inside a DETACHED THROWAWAY WORKTREE: the control case deliberately creates a path
# that must NOT be excluded, and creating that in the live checkout would trip the
# enclosing gate's own tree guard when this test runs inside tooling-tests.
#
# NOTE, so a FAIL here is read correctly: the worktree is checked out at HEAD, so this
# section asserts the COMMITTED gate, not your working tree. An uncommitted fix reads as a
# FAIL (observed while developing #3473). That is the right subject — the gate certifies
# commits — but commit before believing a red here.
if ! command -v git >/dev/null 2>&1; then
  echo "skip section 14 (no git)"
else
  wt="$TMP/wt"
  if ! git -C "$REPO_ROOT" worktree add --detach "$wt" HEAD >/dev/null 2>&1; then
    bad "14.0 create a detached scratch worktree" "git worktree add failed"
  else
    ok "14.0 create a detached scratch worktree"
    # digest_of <summary-path-relative-to-worktree> -> the identity digest the guard uses
    digest_of() {
      ( cd "$wt" && AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_SUMMARY_FILE="$1" \
          bash "$wt/scripts/agent-gate.sh" 2>/dev/null \
          | sed -n 's/.*digest=\([0-9a-f]*\).*/\1/p' )
    }
    base=$(digest_of .agent-gate-summary.txt)
    if [ -z "$base" ]; then
      bad "14.1 the capture hook yields a digest" "empty — cannot measure, so nothing below is asserted"
    else
      ok "14.1 the capture hook yields a digest"
      # (a) the DEFAULT summary path's heartbeat is excluded.
      : > "$wt/.agent-gate-summary.txt.heartbeat"
      d=$(digest_of .agent-gate-summary.txt)
      [ "$d" = "$base" ] && ok "14.2 the run's own heartbeat (default path) is excluded from the tree identity" \
        || bad "14.2 the run's own heartbeat (default path) is excluded from the tree identity" "$base -> $d"
      rm -f "$wt/.agent-gate-summary.txt.heartbeat"
      # (b) so is the atomic-write temp the beater renames from.
      : > "$wt/.agent-gate-summary.txt.heartbeat.tmp.4242"
      d=$(digest_of .agent-gate-summary.txt)
      [ "$d" = "$base" ] && ok "14.3 the beater's atomic-write temp is excluded too" \
        || bad "14.3 the beater's atomic-write temp is excluded too" "$base -> $d"
      rm -f "$wt/.agent-gate-summary.txt.heartbeat.tmp.4242"
      # (c) and a CALLER-PINNED in-repo path gets the same carve-out — no .gitignore rule
      #     can predict that path, so the code carve-out is what covers it.
      pinbase=$(digest_of .pinned-3473.txt)
      : > "$wt/.pinned-3473.txt.heartbeat"
      d=$(digest_of .pinned-3473.txt)
      [ "$d" = "$pinbase" ] && ok "14.4 a caller-pinned in-repo path's heartbeat is excluded" \
        || bad "14.4 a caller-pinned in-repo path's heartbeat is excluded" "$pinbase -> $d"
      rm -f "$wt/.pinned-3473.txt.heartbeat"
      # (d) THE CONTROL. An exclusion that swallowed any sibling of the summary path would
      #     satisfy (a)-(c) while re-opening the hole #2926 exists to close, so an
      #     unrelated sibling must STILL change the identity.
      : > "$wt/.agent-gate-summary.txt.somethingelse"
      d=$(digest_of .agent-gate-summary.txt)
      [ "$d" != "$base" ] && ok "14.5 CONTROL: an unrelated sibling still counts (exclusion is not over-broad)" \
        || bad "14.5 CONTROL: an unrelated sibling still counts" "excluded at $d — the carve-out is too wide"
      rm -f "$wt/.agent-gate-summary.txt.somethingelse"
      # (e) and an ordinary untracked file anywhere still counts.
      : > "$wt/zzz-unrelated-3473.txt"
      d=$(digest_of .agent-gate-summary.txt)
      [ "$d" != "$base" ] && ok "14.6 CONTROL: an ordinary untracked file still counts" \
        || bad "14.6 CONTROL: an ordinary untracked file still counts" "excluded at $d"
      rm -f "$wt/zzz-unrelated-3473.txt"
    fi
    git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || true
  fi
fi

echo
echo "==== test_gate_liveness.sh: passed=$pass failed=$fail ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
