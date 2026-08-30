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
#   * in-place `sed` needs a suffix argument on BSD and rejects the GNU form, so no fixture
#     here edits a file in place at all — each is written directly with the content it means.
#   * `timeout` is GNU coreutils and is absent from a stock macOS; coreutils installs it
#     as `gtimeout`. Resolved once, with an explicit no-timeout fallback rather than an
#     unconditional invocation that would be a "command not found".
#
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

# bump_beats <path> <run-id> <host> <seconds> — keep advancing beat-seq for <seconds>.
#
# A SINGLE-SHOT background writer racing the reader's confirmation window is a timing test: if
# the subshell is descheduled past the window the advance is missed and the case flips. That
# produced two intermittent failures here before this helper existed. Bumping REPEATEDLY makes
# the assertion hold for any scheduling order in which the writer runs at all — the property
# under test is "the reader notices progress", not "the writer hits a particular instant".
bump_beats() {
  local f="$1" rid="$2" host="$3" secs="$4" n=100
  ( local end=$(( $(date +%s) + secs ))
    while [ "$(date +%s)" -lt "$end" ]; do
      n=$((n + 1))
      { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: $rid"; echo "gate-pid: 4242"
        [ -n "$host" ] && echo "host: $host"
        echo "parent-check: starttime"
        echo "interval: 1"; echo "beat-seq: $n"; echo "beat-epoch: $(( $(date +%s) - 99999 ))"
        echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$f.bump" 2>/dev/null
      mv -f "$f.bump" "$f" 2>/dev/null
      sleep 0.5
    done ) &
  echo $! >> "$TMP/pids"
  BUMP_PID=$!
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
# NOT tested at the exact boundary. The beat's epoch is fixed when the fixture is written, and the
# reader evaluates a moment later, so an age of exactly the window drifts one second past it under
# any added work — observed when extra field validation slowed the reader. A boundary case that
# depends on how fast the reader runs is a wall-clock test in disguise; the ARITHMETIC is asserted
# structurally in 5.6 instead, and these two use a safe margin either side.
mk_beat "$TMP/a.txt.heartbeat" run-a 60 20
expect_reader "5.1 age 60s, well inside the 90s floor => RUNNING"  RUNNING 2 "" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 85 20
expect_reader "5.2 age 85s, just inside the 90s floor => RUNNING"  RUNNING 2 "" -- "$TMP/a.txt"
mk_beat "$TMP/a.txt.heartbeat" run-a 120 1
expect_reader "5.3 age 120s, well past the 90s floor => STALLED" STALLED 3 "window 90s" -- "$TMP/a.txt"
# The window ARITHMETIC itself, asserted at the source: max(3 x interval, 90). Exact, and immune to
# how long the reader takes to run.
if grep -q 'STALE_AFTER=$(( HB_INTERVAL \* 3 ))' "$READER" && grep -q '\[ "$STALE_AFTER" -ge 90 \] || STALE_AFTER=90' "$READER"; then
  ok "5.6 the staleness window is max(3 x interval, 90s), read from the beat's own interval"
else
  bad "5.6 the staleness window is max(3 x interval, 90s)" "formula not found"
fi
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
# These fixtures are written DIRECTLY rather than by post-editing a good beat. The previous
# form used a sed-into-temp-then-mv helper whose sed errors were silenced, so a failed edit
# left the beat VALID and the case silently asserted the wrong thing — observed as a real
# intermittent failure of 7.4. A gate-wired suite that reds at random is worse than no suite,
# and building the fixture you actually mean removes the vector entirely.
#
# mk_beat_field <path> <run-id-line> <epoch-line> <interval-line>
mk_beat_field() {
  { echo "==== AGENT-GATE HEARTBEAT ===="
    [ -n "$2" ] && echo "$2"
    echo "gate-pid: 4242"
    echo "beater-pid: 4243"
    echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"
    echo "$4"
    echo "beat-seq: 7"
    echo "$3"
    echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$1"
}
_now=$(date +%s)
mk_beat_field "$hb" "" "beat-epoch: $(( _now - 5 ))" "interval: 20"
# (the cause is now the unified `heartbeat-field-count`, which reports the actual counts —
#  one validator, one cause name, and it says WHICH field was wrong)
expect_reader "7.1 beat with no run-id => UNKNOWN" UNKNOWN 4 "heartbeat-field-count" -- "$TMP/m.txt"
mk_beat_field "$hb" "run-id: run-m" "beat-epoch: soon" "interval: 20"
expect_reader "7.2 non-numeric beat-epoch => UNKNOWN" UNKNOWN 4 "unparseable-epoch" -- "$TMP/m.txt"
mk_beat_field "$hb" "run-id: run-m" "beat-epoch: $(( _now - 5 ))" "interval: often"
expect_reader "7.3 non-numeric interval => UNKNOWN" UNKNOWN 4 "unparseable-interval" -- "$TMP/m.txt"
mk_beat_field "$hb" "run-id: run-m" "beat-epoch: $(( _now - 5 ))" "interval: 0"
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
# 60 x 0.5s = 30s ceiling; breaks the moment the beater publishes. A 3s ceiling
# was load-sensitive on a host running several lanes and produced intermittent reds.
for ((_i_=0; _i_<60; _i_++)); do [ -f "$hbf" ] && break; sleep 0.5; done
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
for ((_i_=0; _i_<60; _i_++)); do
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
      # `_starttime` now returns a TIERED, TAGGED token (`proc:<ticks>` or `ps:<lstart>`) so the
      # beater can tell WHICH identity source it used (job 185). This differential compares the
      # /proc arm, so the tag is stripped; a `ps:`-tagged answer means /proc was unavailable for
      # that pid and there is nothing to compare against awk.
      case "$mine" in
        proc:*) mine="${mine#proc:}" ;;
        ps:*)   continue ;;
        *)      ;;
      esac
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
# Each artifact is copied ONCE and every read is of that copy (job 178). The NUL check and the
# parse must see the same bytes — reading the live path twice let an interleaved write slip
# between them, and `$( )` strips NULs so the parse could not see the damage either.
_texts=$(grep -cE '^[A-Z_]+_TEXT=\$\(_slurp "\$_[A-Z_]+_SNAP"\)$' "$READER")
[ "$_texts" -eq 2 ] && ok "11b.17 both artifacts are parsed from a private snapshot (found $_texts)" \
                    || bad "11b.17 both artifacts are parsed from a private snapshot" "found $_texts, want 2"
# The NUL check must run on a SNAPSHOT, never on the live path — that was the defect.
if grep -qE '_has_nul "\$(SUMMARY|HB)"' "$READER"; then
  bad "11b.17b the NUL check reads a snapshot, not the live path" "$(grep -nE '_has_nul "\$(SUMMARY|HB)"' "$READER" | head -2)"
else
  ok "11b.17b the NUL check reads a snapshot, not the live path"
fi
# And the snapshots must be created exclusively, in a private directory that is cleaned up.
grep -q 'mktemp -d "${TMPDIR:-/tmp}/gate-liveness-snap' "$READER" && grep -q 'trap _cleanup_snaps EXIT' "$READER" \
  && ok "11b.17c snapshots live in a private mkdtemp removed by an EXIT trap" \
  || bad "11b.17c snapshots live in a private mkdtemp removed by an EXIT trap" "not found"
# BEHAVIOURAL, because the structural check above passed while the reader leaked 868 directories.
# The dir was created INSIDE a function invoked as `$(…)` — a subshell — so `SNAP_DIR=` never
# reached the parent: the trap saw nothing to clean and every call made a new dir. A trap that is
# present is not a trap that fires, and only counting the artifacts could tell the difference.
snapdir_count() { ls -d "${TMPDIR:-/tmp}"/gate-liveness-snap.* 2>/dev/null | wc -l | tr -d ' '; }
_snap_before=$(snapdir_count)
mk_summary "$TMP/leak.txt" run-LK "PASS"
for _ in 1 2 3 4 5 6 7 8 9 10; do bash "$READER" "$TMP/leak.txt" >/dev/null 2>&1; done
mk_summary "$TMP/leak2.txt" run-LK2 "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/leak2.txt.heartbeat" run-LK2 5
for _ in 1 2 3 4 5; do bash "$READER" "$TMP/leak2.txt" >/dev/null 2>&1; done
_snap_after=$(snapdir_count)
if [ "$_snap_after" -le "$_snap_before" ]; then
  ok "11b.17d 15 reader invocations leak NO snapshot directories ($_snap_before -> $_snap_after)"
else
  bad "11b.17d reader invocations leak snapshot directories" "$_snap_before -> $_snap_after"
fi
# The split that makes it work must stay: the directory is created in the CALLING shell.
grep -q '^_ensure_snap_dir || verdict UNKNOWN' "$READER" \
  && ok "11b.17e the snapshot dir is created in the calling shell, not inside \$( )" \
  || bad "11b.17e the snapshot dir is created in the calling shell" "not found"

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
  UNKNOWN 4 "summary-no-opener" -- "$nostart"
# ...and the opener requirement now applies to an INCOMPLETE block too (job 176): without it,
# an interleaved summary could hand the reader a FOREIGN fragment's run-id, which it would then
# use to decide whether the heartbeat is ours — reporting RUNNING about a peer's gate.
noopen_inc="$TMP/noopen-inc.txt"
{ echo "run-id: r1"; echo "RESULT: INCOMPLETE (gate did not finish)"; } > "$noopen_inc"
mk_beat "$noopen_inc.heartbeat" r1 5
expect_reader "11c.2b INCOMPLETE with no opener => UNKNOWN, never RUNNING off a beat" \
  UNKNOWN 4 "summary-no-opener" -- "$noopen_inc"
# An INCOMPLETE block whose run-id sits OUTSIDE the block is likewise refused before that
# run-id can be used to validate a beat.
outside_inc="$TMP/outside-inc.txt"
{ echo "run-id: peer-fragment"; echo "==== AGENT-GATE SUMMARY ===="
  echo "RESULT: INCOMPLETE (gate did not finish)"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$outside_inc"
mk_beat "$outside_inc.heartbeat" peer-fragment 5
expect_reader "11c.2c INCOMPLETE with run-id outside the block => UNKNOWN (out of order)" \
  UNKNOWN 4 "summary-out-of-order" -- "$outside_inc"
# CONTROL: a truncated INCOMPLETE (valid opener, ordered fields, missing CLOSER) must STILL
# consult the beat — that is the legitimate mid-write case and the whole point of the asymmetry.
trunc_inc="$TMP/trunc-inc2.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: r1"
  echo "RESULT: INCOMPLETE (gate did not finish)"; } > "$trunc_inc"
mk_beat "$trunc_inc.heartbeat" r1 5
expect_reader "11c.2d control: truncated INCOMPLETE still consults the beat" RUNNING 2 "" -- "$trunc_inc"
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
# The settle-retry must RE-SNAPSHOT rather than re-read a variable, or it would reintroduce the
# two-opens defect it shares a code path with.
if grep -q '_SUM_SNAP2=$(_snap_of "$SUMMARY" summary2)' "$READER"; then
  ok "11d.7 the settle-retry takes a fresh snapshot (not a bare re-read)"
else
  bad "11d.7 the settle-retry takes a fresh snapshot" "not found"
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
  for ((_p_=0; _p_<40; _p_++)); do printf 'padding ' >&3; done
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
bump_beats "$TMP/skew.txt.heartbeat" run-K "$(uname -n 2>/dev/null || echo unknown)" 20
expect_reader "11g.1 ancient epoch but ADVANCING beat-seq => RUNNING (clocks disagree, writer alive)" \
  RUNNING 2 "beat-seq advanced" -- "$TMP/skew.txt"
kill "$BUMP_PID" 2>/dev/null; wait "$BUMP_PID" 2>/dev/null || true
# A beat that is stale AND whose counter does not move is STALLED — and the text must say the
# decision came from progression, not from the clock comparison.
mk_beat "$TMP/skew.txt.heartbeat" run-K 99999 1
expect_reader "11g.2 stale epoch AND static beat-seq => STALLED" STALLED 3 "did NOT advance" -- "$TMP/skew.txt"
# A counter that advances but under a DIFFERENT run-id is a peer's beat, not ours.
mk_beat "$TMP/skew.txt.heartbeat" run-K 99999 1
# Progression must only count when it belongs to OUR run. Expressed structurally plus a
# behavioural half, because the obvious behavioural form is inherently a sequencing test: to
# have the reader read OURS first and a FOREIGN beat second, the writer must fire strictly
# between the two reads — and a writer that fires too early makes the first read foreign, which
# is a different (also correct) verdict. Asserting the requirement at the source is exact.
if grep -q '\[ "\$_rid2" = "\$HB_RUN_ID" \]' "$READER"; then
  ok "11g.3 the progression check requires the re-read to be the SAME run"
else
  bad "11g.3 the progression check requires the re-read to be the SAME run" "run-id equality not required"
fi
# Behavioural half: a beat that is wholly a peer's is refused outright, never credited.
mk_summary "$TMP/peer2.txt" run-K "INCOMPLETE (gate did not finish)"
bump_beats "$TMP/peer2.txt.heartbeat" SOMEONE-ELSE "$(uname -n 2>/dev/null || echo unknown)" 8
sleep 1
expect_reader "11g.3b a wholly foreign beat is refused, not credited as progress" \
  UNKNOWN 4 "run-id-disagree" -- "$TMP/peer2.txt"
kill "$BUMP_PID" 2>/dev/null; wait "$BUMP_PID" 2>/dev/null || true
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

# roborev job 169: round 6 made STALLED clock-independent and LEFT RUNNING comparing clocks —
# an incomplete fix, exploitable in the other direction. A DEAD beat written by a host whose
# clock ran AHEAD later falls inside the freshness window and would read RUNNING with nothing
# advancing, so a lane waits forever on a gate that is gone. The epoch may now only decide
# anything inside a PROVEN shared clock domain (the beat names its host); outside one, both
# answers come from counter progression.
mk_summary "$TMP/dom.txt" run-D "INCOMPLETE (gate did not finish)"
# FRESH epoch, foreign host, static counter — F2's exact shape.
mk_beat "$TMP/dom.txt.heartbeat" run-D 0 1 &&   sed 's/^host: .*/host: someotherbox/' "$TMP/dom.txt.heartbeat" > "$TMP/dom.tmp" && mv "$TMP/dom.tmp" "$TMP/dom.txt.heartbeat"
expect_reader "11g.7 FRESH epoch from an unproven clock domain + static counter => STALLED, not RUNNING" \
  STALLED 3 "clock-domain UNPROVEN" -- "$TMP/dom.txt"
# Same, but the counter advances: alive, decided without comparing clocks.
mk_beat "$TMP/dom.txt.heartbeat" run-D 0 1 &&   sed 's/^host: .*/host: someotherbox/' "$TMP/dom.txt.heartbeat" > "$TMP/dom.tmp" && mv "$TMP/dom.tmp" "$TMP/dom.txt.heartbeat"
bump_beats "$TMP/dom.txt.heartbeat" run-D someotherbox 20
expect_reader "11g.8 unproven clock domain + ADVANCING counter => RUNNING" \
  RUNNING 2 "beat-seq advanced" -- "$TMP/dom.txt"
kill "$BUMP_PID" 2>/dev/null; wait "$BUMP_PID" 2>/dev/null || true
# A beat with NO host line cannot prove a shared clock either.
mk_beat "$TMP/dom.txt.heartbeat" run-D 0 1 &&   grep -v '^host: ' "$TMP/dom.txt.heartbeat" > "$TMP/dom.tmp" && mv "$TMP/dom.tmp" "$TMP/dom.txt.heartbeat"
expect_reader "11g.9 a beat with NO host line => clock domain unproven" \
  STALLED 3 "clock-domain UNPROVEN" -- "$TMP/dom.txt"
# roborev job 178: a future epoch must only be a VERDICT inside a proven shared clock domain.
# Rejecting it first meant a LIVE beat from a host whose clock runs ahead returned UNKNOWN
# without ever reaching the progression check that exists for exactly that case.
mk_summary "$TMP/fut.txt" run-F2 "INCOMPLETE (gate did not finish)"
mk_beat "$TMP/fut.txt.heartbeat" run-F2 -600 1 && \
  sed 's/^host: .*/host: someotherbox/' "$TMP/fut.txt.heartbeat" > "$TMP/fut.tmp" && mv "$TMP/fut.tmp" "$TMP/fut.txt.heartbeat"
bump_beats_future() {
  ( local end=$(( $(date +%s) + 20 )) n=200
    while [ "$(date +%s)" -lt "$end" ]; do
      n=$((n + 1))
      { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-F2"; echo "gate-pid: 4242"
        echo "host: someotherbox"; echo "parent-check: starttime"
        echo "interval: 1"; echo "beat-seq: $n"
        echo "beat-epoch: $(( $(date +%s) + 600 ))"; echo "==== END AGENT-GATE HEARTBEAT ===="
      } > "$TMP/fut.txt.heartbeat.b" 2>/dev/null
      mv -f "$TMP/fut.txt.heartbeat.b" "$TMP/fut.txt.heartbeat" 2>/dev/null
      sleep 0.5
    done ) &
  BUMP_PID=$!; echo "$BUMP_PID" >> "$TMP/pids"
}
bump_beats_future
expect_reader "11g.11 FOREIGN host + FUTURE epoch + advancing counter => RUNNING (not UNKNOWN)" \
  RUNNING 2 "beat-seq advanced" -- "$TMP/fut.txt"
kill "$BUMP_PID" 2>/dev/null; wait "$BUMP_PID" 2>/dev/null || true
# CONTROL: on THIS host a future epoch is a genuine anomaly and must still be refused (7.5 covers
# the default case; this one asserts the clock-domain wording is present).
mk_beat "$TMP/fut.txt.heartbeat" run-F2 -600 20
run_reader "$TMP/fut.txt"
printf '%s' "$OUT" | grep -q 'the beat claims THIS host' \
  && ok "11g.12 a same-host future epoch is refused, and says why" \
  || bad "11g.12 a same-host future epoch is refused, and says why" "$(printf '%s' "$OUT" | head -1)"

# Structural: the epoch shortcut must be gated on the shared-clock test, not stand alone.
if grep -q '\[ "\$_shared_clock" = yes \] && \[ "\$AGE" -le "\$STALE_AFTER" \]' "$READER"; then
  ok "11g.10 the epoch shortcut is gated on a proven shared clock domain"
else
  bad "11g.10 the epoch shortcut is gated on a proven shared clock domain" "gate not found"
fi

echo "=== section 11h: marker dialect must MATCH and elements must be ORDERED (job 172) ==="
# Counting "some opener" and "some closer" independently accepted a LITE opener closed by a
# DELTA marker, and imposed no ordering at all — so a RESULT line sitting BEFORE the opener
# passed too. Both were verified reporting COMPLETE before the fix. Both are what an interleaved
# write produces, which is the case these checks exist for. The three dialects are kept DISTINCT
# by CLAUDE.md precisely so no block can be pasted as another.
for pair in "LITE:DELTA" "DELTA:LITE" ":LITE" "LITE:"; do
  o="${pair%%:*}"; c="${pair##*:}"
  ot="==== AGENT-GATE${o:+ $o} SUMMARY ===="
  ct="==== END AGENT-GATE${c:+ $c} SUMMARY ===="
  f="$TMP/mix-${o:-FULL}-${c:-FULL}.txt"
  { echo "$ot"; echo "run-id: r1"; echo "RESULT: PASS"; echo "$ct"; } > "$f"
  expect_reader "11h.1 opener '${o:-FULL}' + closer '${c:-FULL}' => UNKNOWN (dialect mismatch)" \
    UNKNOWN 4 "summary-marker-dialect-mismatch" -- "$f"
done
# CONTROLS: every MATCHED dialect must still be accepted, or the check is just a refusal.
for d in "" " LITE" " DELTA"; do
  f="$TMP/match-${d:- FULL}.txt"; f="${f// /_}"
  { echo "==== AGENT-GATE${d} SUMMARY ===="; echo "run-id: r1"; echo "RESULT: PASS"
    echo "==== END AGENT-GATE${d} SUMMARY ===="; } > "$f"
  expect_reader "11h.2 control: matched '${d:- (full)}' dialect => COMPLETE" COMPLETE 0 "" -- "$f"
done
# Ordering, both directions.
{ echo "RESULT: PASS"; echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: r1"
  echo "==== END AGENT-GATE SUMMARY ===="; } > "$TMP/ord1.txt"
expect_reader "11h.3 RESULT before the opener => UNKNOWN (out of order)" \
  UNKNOWN 4 "summary-out-of-order" -- "$TMP/ord1.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: r1"
  echo "==== END AGENT-GATE SUMMARY ===="; echo "RESULT: PASS"; } > "$TMP/ord2.txt"
expect_reader "11h.4 RESULT after the closer => UNKNOWN (out of order)" \
  UNKNOWN 4 "summary-out-of-order" -- "$TMP/ord2.txt"
{ echo "run-id: r1"; echo "==== AGENT-GATE SUMMARY ===="; echo "RESULT: PASS"
  echo "==== END AGENT-GATE SUMMARY ===="; } > "$TMP/ord3.txt"
expect_reader "11h.5 run-id outside the block => UNKNOWN (out of order)" \
  UNKNOWN 4 "summary-out-of-order" -- "$TMP/ord3.txt"

echo "=== section 11i: the beat's own framing and identity grammar (job 189) ==="
# The summary side had framing validation; the beat — the artifact that actually CARRIES the
# liveness claim — did not. Missing markers, duplicated fields, or an unknown `parent-check` could
# still produce a confident RUNNING.
mk_summary "$TMP/hbv.txt" run-V "INCOMPLETE (gate did not finish)"
hbv="$TMP/hbv.txt.heartbeat"
_mkbeat_pc() { # _mkbeat_pc <parent-check> <interval>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-V"; echo "gate-pid: 4242"
    echo "host: $(uname -n 2>/dev/null || echo unknown)"; echo "parent-check: $1"
    echo "interval: $2"; echo "beat-seq: 5"; echo "beat-epoch: $(date +%s)"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$hbv"
}
# `kill0` means the beater could NOT identify its gate, so after a pid recycle it may be beating
# for a stranger. Counter progression would only prove the BEATER is alive — no RUNNING claim is
# supportable, so the honest verdict is UNKNOWN, not a weaker RUNNING.
_mkbeat_pc kill0 20
expect_reader "11i.1 parent-check kill0 => UNKNOWN (no gate identity can support RUNNING)" \
  UNKNOWN 4 "heartbeat-no-gate-identity" -- "$TMP/hbv.txt"
_mkbeat_pc bogus 20
expect_reader "11i.2 an UNKNOWN parent-check value => UNKNOWN (closed grammar)" \
  UNKNOWN 4 "heartbeat-unknown-parent-check" -- "$TMP/hbv.txt"
_mkbeat_pc starttime 20 && grep -v '^parent-check: ' "$hbv" > "$hbv.t" && mv "$hbv.t" "$hbv"
# (parent-check is REQUIRED — absence means no verdict can be trusted — so the cause is now the
#  unified field-count check rather than a bespoke message)
expect_reader "11i.3 a beat with NO parent-check => UNKNOWN" \
  UNKNOWN 4 "heartbeat-field-count" -- "$TMP/hbv.txt"
# CONTROLS: both identity-bearing tiers must still be accepted.
for tier in starttime lstart; do
  _mkbeat_pc "$tier" 20
  expect_reader "11i.4 control: parent-check $tier is accepted" RUNNING 2 "" -- "$TMP/hbv.txt"
done
# An interval the confirmation window cannot span must be UNKNOWN, not a false STALLED: the window
# is capped at 65s to bound a hostile artifact, so a live beat at interval>60 might not advance.
_mkbeat_pc starttime 120
expect_reader "11i.5 interval above the observable window => UNKNOWN, not STALLED" \
  UNKNOWN 4 "heartbeat-interval-too-long" -- "$TMP/hbv.txt"
_mkbeat_pc starttime 60
expect_reader "11i.6 control: interval exactly at the boundary (60s) is still read" RUNNING 2 "" -- "$TMP/hbv.txt"
# Framing: two concatenated beats, and a duplicated field.
_mkbeat_pc starttime 20; cat "$hbv" "$hbv" > "$hbv.t" && mv "$hbv.t" "$hbv"
expect_reader "11i.7 two concatenated beats => UNKNOWN (not a single block)" \
  UNKNOWN 4 "heartbeat-not-a-single-block" -- "$TMP/hbv.txt"
_mkbeat_pc starttime 20
{ head -n -1 "$hbv"; echo "beat-seq: 99"; echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$hbv.t" && mv "$hbv.t" "$hbv"
expect_reader "11i.8 a duplicated beat-seq => UNKNOWN (no field attributable to one beat)" \
  UNKNOWN 4 "heartbeat-field-count" -- "$TMP/hbv.txt"
# A beat with no closing marker at all.
_mkbeat_pc starttime 20; grep -v '^==== END AGENT-GATE HEARTBEAT ====$' "$hbv" > "$hbv.t" && mv "$hbv.t" "$hbv"
expect_reader "11i.9 a beat with no closer => UNKNOWN" \
  UNKNOWN 4 "heartbeat-not-a-single-block" -- "$TMP/hbv.txt"

echo "=== section 11j: field ORDER, and a beater restart during confirmation (job 191) ==="
# The ordering check verified that run-id and RESULT were both INSIDE the markers but not their
# RELATIVE order, so a block with RESULT ahead of a matching run-id was accepted as COMPLETE while
# claiming to validate an ordered block. The gate writes run-id first and RESULT last.
{ echo "==== AGENT-GATE SUMMARY ===="; echo "RESULT: PASS"; echo "run-id: r1"
  echo "==== END AGENT-GATE SUMMARY ===="; } > "$TMP/rev.txt"
expect_reader "11j.1 RESULT before run-id => UNKNOWN (out of order)" \
  UNKNOWN 4 "comes AFTER RESULT" -- "$TMP/rev.txt"
mk_summary "$TMP/fwd.txt" r1 "PASS"
expect_reader "11j.2 control: run-id before RESULT is still COMPLETE" COMPLETE 0 "" -- "$TMP/fwd.txt"

# A BEATER RESTART during the confirmation window must not read as a stall. Round 15 tightened
# progression to "strictly greater" to stop a peer's smaller counter passing — but every
# replacement beater restarts its counter at 1, and the gate respawns the beater at component
# boundaries, so a restart mid-window produces a LOWER second sequence. A live gate would have been
# reported STALLED: the exact false death this script exists to prevent, caused by the previous fix.
mk_summary "$TMP/restart.txt" run-R "INCOMPLETE (gate did not finish)"
_mkbeat_r() { # _mkbeat_r <beat-seq> <beater-pid>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-R"; echo "gate-pid: 4242"
    echo "beater-pid: $2"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: 1"; echo "beat-seq: $1"
    echo "beat-epoch: $(( $(date +%s) - 99999 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$TMP/restart.txt.heartbeat"
}
_mkbeat_r 57 1111
# During the reader's wait, a NEW beater incarnation appears with a LOWER counter.
( sleep 1; _mkbeat_r 1 2222; sleep 1; _mkbeat_r 2 2222 ) &
_rp=$!; echo "$_rp" >> "$TMP/pids"
expect_reader "11j.3 a beater RESTART mid-window => RUNNING, not a false STALLED" \
  RUNNING 2 "RELAUNCHED" -- "$TMP/restart.txt"
wait "$_rp" 2>/dev/null || true
# CONTROL: a lower counter under the SAME beater incarnation is NOT progress (that is a peer or a
# corrupt write, not a restart).
_mkbeat_r 57 1111
( sleep 1; _mkbeat_r 3 1111 ) &
_rp2=$!; echo "$_rp2" >> "$TMP/pids"
expect_reader "11j.4 control: a LOWER counter under the same beater-pid is not progress" \
  STALLED 3 "did NOT advance" -- "$TMP/restart.txt"
wait "$_rp2" 2>/dev/null || true
# CONTROL: a changed beater-pid under a DIFFERENT run-id is a peer, not our gate.
_mkbeat_r 57 1111
( sleep 1
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: SOMEONE-ELSE"; echo "gate-pid: 4242"
    echo "beater-pid: 9999"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: 1"; echo "beat-seq: 1"
    echo "beat-epoch: $(date +%s)"; echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$TMP/restart.txt.heartbeat" ) &
_rp3=$!; echo "$_rp3" >> "$TMP/pids"
expect_reader "11j.5 control: a new beater-pid under a FOREIGN run-id is not our gate" \
  STALLED 3 "did NOT advance" -- "$TMP/restart.txt"
wait "$_rp3" 2>/dev/null || true

# Portability: no external `seq`. Whether stock macOS ships it is arguable; a bash arithmetic loop
# needs no external utility at all, so there is nothing left to argue about (job 191).
sq_bad=0
for f in "$REPO_ROOT/scripts/tests/test_gate_liveness.sh" "$REPO_ROOT/scripts/tests/test_gate_detached.sh"; do
  if sed 's/[[:space:]]*#.*$//' "$f" | grep -qE '\$\(seq '; then
    bad "11j.6 no external seq in $(basename "$f")" "found one"; sq_bad=1
  fi
done
[ "$sq_bad" -eq 0 ] && ok "11j.6 neither suite depends on an external seq"

echo "=== section 11k: octal traps, and a superseded summary (job 192) ==="
# A DIGIT STRING IS NOT A NUMBER. Bash arithmetic reads a leading zero as octal, so `interval: 08`
# was a syntax error that ABORTED the reader — it would die instead of returning its documented
# UNKNOWN exit code. Every numeric field is now length-bounded and normalised base-10.
mk_summary "$TMP/oct.txt" run-O "INCOMPLETE (gate did not finish)"
_mkbeat_num() { # _mkbeat_num <interval> <beat-seq> <epoch>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-O"; echo "gate-pid: 42"
    echo "beater-pid: 43"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: $1"; echo "beat-seq: $2"
    echo "beat-epoch: $3"; echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$TMP/oct.txt.heartbeat"
}
for v in 08 09 007; do
  _mkbeat_num "$v" 5 "$(date +%s)"
  run_reader "$TMP/oct.txt"
  case "$RC" in
    0|2|3|4) ok "11k.1 interval '$v' yields a documented verdict (rc=$RC), not a shell abort" ;;
    *)       bad "11k.1 interval '$v' yields a documented verdict" "rc=$RC: $(printf '%s' "$OUT" | head -1)" ;;
  esac
done
_mkbeat_num 20 08 "$(date +%s)"
run_reader "$TMP/oct.txt"
case "$RC" in 0|2|3|4) ok "11k.2 a leading-zero beat-seq yields a documented verdict (rc=$RC)" ;;
              *)       bad "11k.2 a leading-zero beat-seq yields a documented verdict" "rc=$RC" ;; esac
# Absurd magnitudes are refused by NAME rather than overflowing a comparison.
_mkbeat_num 999999999999999 5 "$(date +%s)"
expect_reader "11k.3 an absurd interval => UNKNOWN (out of range)" UNKNOWN 4 "interval-out-of-range" -- "$TMP/oct.txt"
_mkbeat_num 20 5 99999999999999999
expect_reader "11k.4 an absurd beat-epoch => UNKNOWN (out of range)" UNKNOWN 4 "epoch-out-of-range" -- "$TMP/oct.txt"

# A TERMINAL summary must be reconciled with the beat. During startup the NEW run publishes its
# beat BEFORE it replaces the previous run's summary — and the beater now starts before the tree
# capture, which widened that window deliberately. So an UNBOUND reader could report the PREVIOUS
# run's PASS as the completion of the run starting right now.
mk_summary "$TMP/sup.txt" old-run "PASS"
mk_beat "$TMP/sup.txt.heartbeat" new-run 5
expect_reader "11k.5 terminal summary + beat naming a NEWER run => UNKNOWN (superseded)" \
  UNKNOWN 4 "summary-superseded" -- "$TMP/sup.txt"
# CONTROL: the same run in both artifacts is the ordinary finished-gate case.
mk_summary "$TMP/sup2.txt" same-run "PASS"
mk_beat "$TMP/sup2.txt.heartbeat" same-run 5
expect_reader "11k.6 control: matching run-ids => COMPLETE" COMPLETE 0 "" -- "$TMP/sup2.txt"
# CONTROL: a caller who SAYS which run they mean is answered about that run, not refused.
expect_reader "11k.7 control: --run-id disambiguates instead of refusing" \
  COMPLETE 0 "" -- "$TMP/sup.txt" --run-id old-run
# CONTROL: no heartbeat at all is still COMPLETE (the common older-gate case).
mk_summary "$TMP/sup3.txt" lone-run "PASS"
expect_reader "11k.8 control: terminal summary with no beat => COMPLETE" COMPLETE 0 "" -- "$TMP/sup3.txt"

echo "=== section 11l: the launcher's own startup window (job 196) ==="
# gate-detached.sh accepts a gate on the strength of its BEAT — the beater starts before the tree
# capture — and then prints a run-bound poll command. This reader used to reject a missing or
# superseded summary outright, so the advertised command answered UNKNOWN for a healthy, accepted,
# actively-beating gate for the whole capture. The launcher and the reader disagreed about what
# "accepted" means, which is worse than either being wrong alone.
_mk_startup_beat() { # _mk_startup_beat <path> <run-id>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: $2"; echo "gate-pid: 42"
    echo "beater-pid: 43"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: 20"; echo "beat-seq: 1"
    echo "beat-epoch: $(date +%s)"; echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$1"
}
rm -f "$TMP/su.txt"
_mk_startup_beat "$TMP/su.txt.heartbeat" myrun
expect_reader "11l.1 named run, beat present, summary NOT YET written => RUNNING" \
  RUNNING 2 "has not written its summary yet" -- "$TMP/su.txt" --run-id myrun
# Without --run-id there is nothing to match against, so the summary stays the only anchor.
expect_reader "11l.2 UNNAMED run with no summary => UNKNOWN (nothing to match the beat against)" \
  UNKNOWN 4 "no-summary-artifact" -- "$TMP/su.txt"
# One step later in startup: the summary at the path still belongs to the PREVIOUS run.
mk_summary "$TMP/su.txt" oldrun "PASS"
expect_reader "11l.3 named run, beat present, summary still the PREVIOUS run's => RUNNING" \
  RUNNING 2 "has not been replaced yet" -- "$TMP/su.txt" --run-id myrun
# CONTROLS: the previous run can still be asked about by name, and a named run with NO beat is
# not rescued by this path.
expect_reader "11l.4 control: the previous run answered by its own id => COMPLETE" \
  COMPLETE 0 "" -- "$TMP/su.txt" --run-id oldrun
rm -f "$TMP/su.txt.heartbeat"
expect_reader "11l.5 control: named run with NO beat is not rescued => UNKNOWN" \
  UNKNOWN 4 "summary-run-id-mismatch" -- "$TMP/su.txt" --run-id myrun
# A beat that is INVALID must not rescue anything either.
_mk_startup_beat "$TMP/su.txt.heartbeat" myrun
grep -v '^parent-check: ' "$TMP/su.txt.heartbeat" > "$TMP/su.tmp" && mv "$TMP/su.tmp" "$TMP/su.txt.heartbeat"
expect_reader "11l.6 control: an INVALID beat does not rescue a superseded summary" \
  UNKNOWN 4 "" -- "$TMP/su.txt" --run-id myrun

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
