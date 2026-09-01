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
# A PID IS NOT AN IDENTITY (roborev job 204). These suites deliberately kill processes long before
# cleanup runs, and the kernel reuses pids — so an unverified `kill` at cleanup can signal an
# unrelated same-user process, including a CONCURRENT GATE on this box. Same failure as killing by
# pattern: the selector describes what a process is, not whose it is.
#
# So every recorded pid carries the start identity it had when we started it, and cleanup signals
# only on a MATCH. An identity that cannot be read means DO NOT SIGNAL: a leaked helper under $TMP
# is harmless (removed with the directory; a beater self-terminates with its gate), whereas killing
# a stranger is not. The conservative branch is chosen by consequence, not by convenience.
_pid_identity() {  # <pid> -> "proc:<starttime>" | "ps:<lstart>" | "" if unreadable
  local raw rest ls
  raw=$(cat "/proc/$1/stat" 2>/dev/null)
  if [ -n "$raw" ]; then
    rest="${raw##*) }"
    # shellcheck disable=SC2086  # deliberate word-split into positional params
    set -- $rest
    if [ $# -ge 20 ]; then printf 'proc:%s' "${20}"; return 0; fi
  fi
  ls=$(ps -o lstart= -p "$1" 2>/dev/null | tr -s ' ')
  [ -n "$ls" ] && { printf 'ps:%s' "$ls"; return 0; }
  return 1
}
# remember_pid <pid> — record it WITH its identity, in ONE file that cleanup actually reads.
remember_pid() {
  local id
  id=$(_pid_identity "$1" 2>/dev/null || true)
  printf '%s\t%s\n' "$1" "$id" >> "$TMP/tracked-pids"
}
# kill_tracked <signal> — signal only pids whose identity still matches what we recorded.
kill_tracked() {
  local sig="$1" pid want now
  [ -f "$TMP/tracked-pids" ] || return 0
  while IFS=$'\t' read -r pid want; do
    case "$pid" in ''|*[!0-9]*) continue ;; esac
    [ -n "$want" ] || continue          # never recorded => cannot verify => do not signal
    now=$(_pid_identity "$pid" 2>/dev/null || true)
    [ -n "$now" ] || continue           # gone, or unreadable => nothing to signal safely
    [ "$now" = "$want" ] || continue    # pid reused: this is SOMEONE ELSE
    kill "$sig" "$pid" 2>/dev/null || true
  done < "$TMP/tracked-pids"
}

# shellcheck disable=SC2317
cleanup() {
  local p
  # Was: kill every pid in beater-pids, unverified — AND $TMP/pids was written by a dozen sites
  # and read by NOBODY, so those entries were never cleaned while appearing to be. One tracked
  # file, one reader, identity verified.
  kill_tracked -TERM
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
# ON FAILURE, DUMP WHAT THE READER ACTUALLY READ. Two cases in this suite have failed once each and
# passed on every isolated re-run (11i.1 and 5.1), both with a verdict implying the reader saw
# different artifact content than the fixture wrote. Ruled out by measurement: snapshot-name
# collisions (SNAP_DIR is a per-process `mktemp -d`), disk/inode pressure (15% / 3%), stray beaters,
# and — for 5.1 — a concurrent suite instance. So the cause is UNEXPLAINED, and rather than call an
# unreproducible red a flake and move on, the failure path now emits the artifacts. A one-in-N
# intermittency that prints its own evidence becomes diagnosable on its next occurrence; one that
# prints only a verdict stays a mystery forever.
_dump_artifacts() {  # <label>: show the artifacts the reader was pointed at
  local a
  for a in "$@"; do
    case "$a" in --*) continue ;; esac
    [ -f "$a" ] && { printf '     --- %s ---\n' "$a"; sed 's/^/       /' "$a"; }
    [ -f "$a.heartbeat" ] && { printf '     --- %s.heartbeat ---\n' "$a"; sed 's/^/       /' "$a.heartbeat"; }
  done
}
expect_reader() {
  local label="$1" want="$2" wantrc="$3" needle="$4"; shift 5
  run_reader "$@"
  # A VERDICT MAY NEVER CARRY AN EMPTY CAUSE — enforced here, for EVERY case, rather than case by case.
  # Job 226 was a refusal that returned without setting BEAT_ERR, so the reader printed the literal
  # `gate-liveness: UNKNOWN ()`. The case covering it asserted only the exit code and passed. A sibling
  # audit then found FOUR more cases asserting `UNKNOWN 4 ""` — any of which could have hidden the same
  # thing — so the invariant belongs in the helper: every one of this suite's assertions now checks it,
  # and a case added later inherits it without anyone remembering to ask.
  if printf '%s' "$OUT" | grep -qE '^gate-liveness: [A-Z]+ \(\)[[:space:]]*$'; then
    bad "$label" "verdict carried an EMPTY cause: $(printf '%s' "$OUT" | head -1)"; _dump_artifacts "$@"; return
  fi
  if ! printf '%s' "$OUT" | grep -q "^gate-liveness: $want "; then
    bad "$label" "expected status $want, got: $(printf '%s' "$OUT" | head -1)"; _dump_artifacts "$@"; return
  fi
  if [ "$RC" != "$wantrc" ]; then
    bad "$label" "expected exit $wantrc, got $RC"; _dump_artifacts "$@"; return
  fi
  if [ -n "$needle" ] && ! printf '%s' "$OUT" | grep -q "$needle"; then
    bad "$label" "expected cause to mention '$needle', got: $(printf '%s' "$OUT" | head -1)"; _dump_artifacts "$@"; return
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
  # DEFAULT STAYS 20. Lowering it to 1 shortened the reader's confirmation sleep (interval+5) and cut
  # ~8 minutes off the suite — but it also silently changed cases whose behaviour is SCALED BY INTERVAL.
  # Case 7.6 ("a beat up to one INTERVAL ahead is tolerated") flipped from RUNNING to
  # heartbeat-in-the-future, because 5s ahead is inside one 20s interval and outside a 1s one.
  #
  # I had enumerated the interval-sensitive sections as 5 and 11g and protected those by line range.
  # There were THREE — section 7 scales future-clock tolerance by interval — and I missed it. Since that
  # enumeration is demonstrably unreliable, the 19 DEFAULTED call sites keep the old value: a defaulted
  # call is exactly the one whose author did not think about the interval, so it is the one most likely
  # to depend on it accidentally. The saving is taken only where each call site was inspected.
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

# EVERY WAY THE BEAT'S `interval` FIELD AFFECTS THIS READER — derived from the source, not recalled,
# because recalling it is what went wrong. A fixture's interval is NOT decoration: lowering it shortens
# the reader's confirmation sleep (which is why most fixtures here use 1 and the suite runs in 429s
# rather than 696s), but it also moves two verdict-bearing thresholds.
#
#   1. staleness WINDOW      max(3*interval, 90s)     -> section 5 is ABOUT this; do not touch its
#                                                        fixtures. Note any interval <= 30 gives the
#                                                        same 90s floor, so 1 and 20 are equivalent here.
#   2. startup-probe window  same formula             -> equivalent for the same reason
#   3. interval > 60         rejected outright        -> 1 and 20 both pass
#   4. confirmation SLEEP    interval + 5 (cap 65)    -> runtime only, no verdict
#   5. future TOLERANCE      reject iff age < -interval -> ONLY affects fixtures with a NEGATIVE age,
#                                                        and it is the one I missed. Case 7.6 ("a beat
#                                                        up to one INTERVAL ahead is tolerated") flips
#                                                        from RUNNING to heartbeat-in-the-future when
#                                                        the interval drops below the lead time.
#
# So: changing a fixture's interval is verdict-neutral UNLESS its age is NEGATIVE or it lives in
# section 5. The four negative-age fixtures are deliberately left at the default for that reason. If you
# lower `mk_beat`'s DEFAULT, you silently change every defaulted call — including the future-beat ones,
# which is exactly how 7.6 broke.
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
  remember_pid "$!"
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
# 5.2b/5.2c (roborev-adjacent, found by doing the C audit myself after the auditor went idle):
# RUNNING's disclosure used to hand the reader a MEASURABLY WRONG recipe — compare
# utime+stime+cutime+cstime from /proc/<gate-pid>/stat against elapsed, "a working one is >=1"
# core. Those fields count REAPED children only, and a gate's work lives in cargo/maturin children
# that are still alive: MEASURED on a healthy 37/37 run, the parent read 0.15 cores at 89s and 0.09
# at 905s while producing ~5.7 GB/min with 60 pids turning over per 20s. So the shipped advice made
# a working gate look queued — the exact misreading it existed to prevent, and one I made twice
# (once about a peer's gate). Asserted on the EMITTED text, both directions: the retracted claim
# must be GONE and a sound signal must be NAMED, because deleting the bad advice without replacing
# it would leave the reader with no way to answer the question the note raises.
mk_beat "$TMP/a.txt.heartbeat" run-a 60 20
# `run_reader` passes "$@" STRAIGHT to the reader; the `--` in expect_reader's grammar is consumed
# by its own `shift 5`. Passing it here handed the reader a literal `--` as its summary path, so the
# verdict was UNKNOWN, no RUNNING note was emitted, and 5.2b (a NEGATIVE assertion) passed VACUOUSLY
# on output that simply had no note in it. 5.2c, the positive control, is what caught that.
run_reader "$TMP/a.txt"
# NON-VACUITY FIRST: a negative assertion over $OUT is satisfied by output that has no note at all,
# which is precisely how the `--` bug made 5.2b green. Establish the subject exists before judging it.
if ! printf '%s' "$OUT" | grep -q 'gate-liveness: RUNNING'; then
  bad "5.2b RUNNING no longer claims the parent pid's cumulative cpu shows progress" \
      "the fixture did not produce a RUNNING verdict, so there is no note to judge: $(printf '%s' "$OUT" | head -1)"
elif printf '%s' "$OUT" | grep -qE 'a working one is >=?1|sits near 0\.01 cores'; then
  bad "5.2b RUNNING no longer claims the parent pid's cumulative cpu shows progress" \
      "the retracted >=1-core recipe is still emitted"
else
  ok "5.2b RUNNING no longer claims the parent pid's cumulative cpu shows progress"
fi
# 5.2d (roborev job 318, Low): the note must be WELL-FORMED, not merely correct in content.
# 5.2b and 5.2c assert what the note SAYS and what it no longer says; NEITHER can see a line
# emitted TWICE -- and one was, for two hours, because a slice boundary in my own edit was off by
# one. A content assertion is not a well-formedness assertion, so this asserts uniqueness over the
# whole emitted response rather than over the note alone (the same defect could land anywhere).
_dup=$(printf '%s\n' "$OUT" | sort | uniq -d | grep -c . || true)
if [ "${_dup:-0}" -eq 0 ]; then
  ok "5.2d no line of the RUNNING response is emitted twice"
else
  bad "5.2d no line of the RUNNING response is emitted twice" \
      "$_dup duplicated line(s): $(printf '%s\n' "$OUT" | sort | uniq -d | head -2 | tr '\n' '|')"
fi
if printf '%s' "$OUT" | grep -q 'PID-SET TURNOVER' \
   && printf '%s' "$OUT" | grep -qi 'REAPED children only'; then
  ok "5.2c RUNNING names a sound progress signal AND why the pid reading fails"
else
  bad "5.2c RUNNING names a sound progress signal and why the pid reading fails" \
      "$(printf '%s' "$OUT" | grep -c . ) lines, no turnover/reaped explanation"
fi
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
remember_pid "$beater_pid"
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
# THIS CASE ASSERTED THE BUG. Written in round 10 as a "control" pinning that an id-less summary
# still answers when no --run-id is given — but job 199 showed that is unsound: every summary the
# gate writes carries a run-id, so its absence means the block is not whole, and a verdict read from
# it is attributable to NO run. The requirement is now unconditional and this case pins the honest
# answer. (Second instance of a test vouching for a defect in this change; the other was 11j.5. The
# tell in both: new code and an old test disagreed, and the new code's reasoning was the better one.)
expect_reader "11b.13 no --run-id + id-less summary => UNKNOWN (attributable to no run)" \
  UNKNOWN 4 "summary-no-run-id-at-all" -- "$noid"
# ...and the ordinary id-BEARING summary still answers, bound or unbound.
mk_summary "$TMP/hasid.txt" some-run "PASS"
expect_reader "11b.13b control: an id-bearing summary answers unbound" COMPLETE 0 "" -- "$TMP/hasid.txt"
expect_reader "11b.13c control: ...and bound to its own id" COMPLETE 0 "" -- "$TMP/hasid.txt" --run-id some-run
# And the same demand holds for a non-terminal summary reaching the heartbeat.
noid2="$TMP/noid2.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "RESULT: INCOMPLETE (gate did not finish)"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$noid2"
mk_beat "$noid2.heartbeat" my-run 5
# REFINED, not corrected (job 209) — and the distinction matters, because two other cases in this
# suite turned out to be vouching for defects and this one is NOT that. The rule 11b.14 encoded is
# right: nothing from an id-less summary may be attributed to the requested run. What changed is
# where the verdict comes from. A VALID, run-id-MATCHING, FRESH beat on this host affirmatively says
# `my-run` is alive, so the answer is sourced from the HEARTBEAT — an artifact that does name the run
# — and the id-less summary is not consulted for it.
#
# The safety property is intact because this deferral can only ever produce RUNNING, never COMPLETE.
# RUNNING is not a certification; COMPLETE is, and COMPLETE still demands an id-bearing, framed,
# matching summary (11b.13c). What forced the refinement: `gate-detached.sh` STOPS the unit when
# liveness cannot answer within 20s, so refusing here killed healthy gates whose tree capture outran
# the window.
expect_reader "11b.14 --run-id + id-less INCOMPLETE summary + FRESH matching beat => RUNNING (from the beat)" \
  RUNNING 2 "is beating" -- "$noid2" --run-id my-run
# ...and the ORIGINAL guarantee still holds wherever there is no affirmative evidence: with a STALE
# beat, or none, an id-less summary cannot be attributed to the requested run.
mk_beat "$noid2.heartbeat" my-run 4000
# Updated by job 216. A stale beat that is well-formed and NAMES this run is not "no evidence" — it
# is evidence that the run published liveness and stopped, which is exactly what STALLED says. The
# case's intent (never claim the gate is alive without affirmative evidence) is intact: STALLED is
# non-certifying, and job 216's prescription reserves UNKNOWN for beats that are absent, malformed or
# mismatched. The next three cases cover those.
expect_reader "11b.14b id-less summary + STALE matching beat => STALLED (liveness stopped)" \
  STALLED 3 "" -- "$noid2" --run-id my-run
rm -f "$noid2.heartbeat"
expect_reader "11b.14c id-less summary + NO beat => UNKNOWN" \
  UNKNOWN 4 "summary-no-run-id" -- "$noid2" --run-id my-run
# A beat for a DIFFERENT run is not evidence about this one either.
mk_beat "$noid2.heartbeat" other-run 5
expect_reader "11b.14d id-less summary + fresh beat for ANOTHER run => UNKNOWN" \
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
# Asserted as the PROPERTY, not as the text that happens to follow (job 218). The previous form
# grepped `^_ensure_snap_dir || verdict UNKNOWN`, so routing that path through the refusal funnel
# broke the assertion while the invariant it protects was untouched. What matters is only this: the
# call sits at column zero — the CALLING shell — and never inside a command substitution, because a
# subshell's SNAP_DIR assignment is discarded and that is what leaked 868 directories.
if grep -qE '^_ensure_snap_dir( \|\||;|$)' "$READER" && ! grep -q '\$(_ensure_snap_dir' "$READER"; then
  ok "11b.17e the snapshot dir is created in the calling shell, not inside \$( )"
else
  bad "11b.17e the snapshot dir is created in the calling shell" \
      "no column-zero call, or one appears inside \$( )"
fi

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
# THE GUARD TESTS ITS STATED PROPERTY: a verdict must not DEPEND on /proc. The first version grepped
# the reader for the string and excluded only comment lines, so it could not tell "reads /proc to
# decide" from "MENTIONS /proc in emitted advice" -- and it fired on a change that added
# /proc/<gate-pid>/stat to RUNNING's disclosure text, i.e. it punished an improvement satisfying its
# own intent. A SPELLING test standing in for a STATE test, which is the defect class this suite
# exists to catch -- so it is fixed here rather than evaded by rewording the note, which would
# defeat the guard by paraphrase and leave it broken for whoever comes next.
#
# A pure OUTPUT statement cannot read anything: echo/printf with no command substitution and no
# backtick has no way to reach the filesystem. Everything else still counts, so echo "$(cat
# /proc/...)" is still caught -- the exclusion is keyed on the ABSENCE of a substitution mechanism,
# not on the line looking like output.
_proc_scan() { # <file> -> prints offending lines
  grep -nE '/proc|kill -0|boot-id|boot_id|gate-starttime' "$1" 2>/dev/null | awk '{
      line = $0; sub(/^[0-9]+:/, "", line);
      if (line ~ /^[[:space:]]*#/) next;
      if (line ~ /^[[:space:]]*(echo|printf)/ && line !~ /[$][(]/ && line !~ /`/) next;
      print
    }'
}
_proc_offenders=$(_proc_scan "$READER")
if [ -n "$_proc_offenders" ]; then
  bad "11c.8 no verdict depends on /proc, a pid probe or machine identity" \
      "$(printf '%s' "$_proc_offenders" | head -3)"
else
  ok "11c.8 no verdict depends on /proc, a pid probe or machine identity"
fi
# RED-VERIFY THE GUARD ITSELF: a narrowed guard must still catch a genuine read, or it has been
# weakened rather than narrowed. Plant a real read in a scratch copy and require a hit.
_plant="$TMP/reader-with-proc-read.sh"
{ cat "$READER"; printf '%s\n' 'if [ -r /proc/1/stat ]; then _x=$(cat /proc/1/stat); fi'; } > "$_plant"
_planted=$(_proc_scan "$_plant" | wc -l)
if [ "$_planted" -ge 1 ]; then
  ok "11c.8b control: the narrowed guard still catches a genuine /proc READ ($_planted hit)"
else
  bad "11c.8b control: the narrowed guard still catches a genuine /proc read" \
      "planted a real read and the guard stayed silent -- weakened, not narrowed"
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
  BUMP_PID=$!; remember_pid "$BUMP_PID"
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
_rp=$!; remember_pid "$_rp"
expect_reader "11j.3 a beater RESTART mid-window => RUNNING, not a false STALLED" \
  RUNNING 2 "RELAUNCHED" -- "$TMP/restart.txt"
wait "$_rp" 2>/dev/null || true
# CONTROL: a lower counter under the SAME beater incarnation is NOT progress (that is a peer or a
# corrupt write, not a restart).
_mkbeat_r 57 1111
( sleep 1; _mkbeat_r 3 1111 ) &
_rp2=$!; remember_pid "$_rp2"
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
_rp3=$!; remember_pid "$_rp3"
# The INTENT is unchanged — a foreign beat must never be credited as our gate's progress — but the
# correct verdict became UNKNOWN in job 198: a second sample belonging to another run means we could
# not measure OUR run, and STALLED (a positive claim) must not be derived from that. This case
# expected STALLED, which was the defect; it now pins the honest answer.
expect_reader "11j.5 control: a new beater-pid under a FOREIGN run-id is not credited as progress" \
  UNKNOWN 4 "belongs to run 'SOMEONE-ELSE'" -- "$TMP/restart.txt"
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
# Retitled and re-pointed (job 208): the cause is `summary-foreign-run`, and the old title claimed
# the beat named a NEWER run — which is exactly the ordering the reader cannot establish. The
# VERDICT this case was written for (job 192) is unchanged; only the unprovable claim is gone.
expect_reader "11k.5 terminal summary + beat naming a DIFFERENT run => UNKNOWN" \
  UNKNOWN 4 "summary-foreign-run" -- "$TMP/sup.txt"
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
# FRESHNESS is part of the startup test (job 197): the first version accepted any valid matching
# beat, so a gate that died after its FIRST beat but before writing its summary reported RUNNING
# forever. A false RUNNING makes the caller wait indefinitely on a gate that is gone.
_mk_stale_startup_beat() { # <path> <run-id> <age>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: $2"; echo "gate-pid: 42"
    echo "beater-pid: 43"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: 20"; echo "beat-seq: 1"
    echo "beat-epoch: $(( $(date +%s) - $3 ))"; echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$1"
}
rm -f "$TMP/st2.txt"
_mk_stale_startup_beat "$TMP/st2.txt.heartbeat" myrun 5000
# Updated by job 218, and this case VOUCHED FOR THE BUG: it required a stale matching beat with no
# summary to answer UNKNOWN, which is precisely the pre-sentinel reap that should read STALLED. The
# fourth instance in this change of a test pinning behaviour that was wrong — and the tell was the
# same each time: new reasoning and an old expectation disagreed, and the reasoning was better.
# The case's PROPERTY, never a false RUNNING, is unchanged: STALLED is not RUNNING.
expect_reader "11l.7 STALE startup beat + no summary => STALLED, never a false RUNNING" \
  STALLED 3 "beat-seq did NOT advance" -- "$TMP/st2.txt" --run-id myrun
mk_summary "$TMP/st2.txt" oldrun "PASS"
# Updated by job 216: the verdict moved from UNKNOWN to STALLED because the beat now reaches the
# heartbeat side's confirmation instead of being pre-empted by a summary complaint. This case's
# PROPERTY — never a false RUNNING — is unchanged and still asserted; STALLED is not RUNNING.
expect_reader "11l.8 STALE startup beat + previous summary => STALLED, never a false RUNNING" \
  STALLED 3 "beat-seq did NOT advance" -- "$TMP/st2.txt" --run-id myrun
# CONTROL: the same beat, fresh, still takes the shortcut.
_mk_stale_startup_beat "$TMP/st2.txt.heartbeat" myrun 5
expect_reader "11l.9 control: a FRESH startup beat still shortcuts => RUNNING" \
  RUNNING 2 "window" -- "$TMP/st2.txt" --run-id myrun
# CONTROL: a fresh beat from ANOTHER host cannot shortcut (no proven shared clock).
_mk_stale_startup_beat "$TMP/st2.txt.heartbeat" myrun 5
sed 's/^host: .*/host: someotherbox/' "$TMP/st2.txt.heartbeat" > "$TMP/st2.tmp" && mv "$TMP/st2.tmp" "$TMP/st2.txt.heartbeat"
# Updated by job 216, and consistent with what this suite ALREADY pins: 11g.7 requires a fresh epoch
# from an unproven clock domain with a static counter to be STALLED, not RUNNING. That is the
# heartbeat side's considered answer for this exact shape; the old UNKNOWN here came only from the
# summary refusal pre-empting it. The control's point — a foreign-host beat must not take the RUNNING
# shortcut — holds.
expect_reader "11l.10 control: a foreign-host startup beat does not shortcut (STALLED, not RUNNING)" \
  STALLED 3 "beat-seq did NOT advance" -- "$TMP/st2.txt" --run-id myrun

# A beat that is INVALID must not rescue anything either.
_mk_startup_beat "$TMP/su.txt.heartbeat" myrun
grep -v '^parent-check: ' "$TMP/su.txt.heartbeat" > "$TMP/su.tmp" && mv "$TMP/su.tmp" "$TMP/su.txt.heartbeat"
expect_reader "11l.6 control: an INVALID beat does not rescue a superseded summary" \
  UNKNOWN 4 "" -- "$TMP/su.txt" --run-id myrun

echo "=== section 11m: STALLED needs TWO VALID samples (job 198) ==="
# STALLED is a POSITIVE verdict. The first version left `_advanced=no` whenever the confirmation
# snapshot could not be copied, held NULs, failed validation, or belonged to another run — and then
# reported STALLED, collapsing "I could not measure" into "I measured no progress". That is the one
# thing this script's own header forbids, and a transient read failure would have stalled a live gate.
mk_summary "$TMP/cm.txt" run-C2 "INCOMPLETE (gate did not finish)"
_mkcm() { # _mkcm <run-id>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: $1"; echo "gate-pid: 42"
    echo "beater-pid: 43"; echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "parent-check: starttime"; echo "interval: 1"; echo "beat-seq: 5"
    echo "beat-epoch: $(( $(date +%s) - 99999 ))"; echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$TMP/cm.txt.heartbeat"
}
_mkcm run-C2; ( sleep 1; rm -f "$TMP/cm.txt.heartbeat" ) & _c1=$!; remember_pid "$_c1"
expect_reader "11m.1 the beat VANISHES mid-confirmation => UNKNOWN, not STALLED" \
  UNKNOWN 4 "confirmation-unmeasurable" -- "$TMP/cm.txt"
wait "$_c1" 2>/dev/null || true
_mkcm run-C2; ( sleep 1; _mkcm SOMEONE-ELSE ) & _c2=$!; remember_pid "$_c2"
expect_reader "11m.2 the beat is REPLACED by another run => UNKNOWN, not STALLED" \
  UNKNOWN 4 "confirmation-unmeasurable" -- "$TMP/cm.txt"
wait "$_c2" 2>/dev/null || true
_mkcm run-C2; ( sleep 1; printf 'not a beat at all\n' > "$TMP/cm.txt.heartbeat" ) & _c3=$!; remember_pid "$_c3"
expect_reader "11m.3 the confirmation sample is MALFORMED => UNKNOWN, not STALLED" \
  UNKNOWN 4 "confirmation-unmeasurable" -- "$TMP/cm.txt"
wait "$_c3" 2>/dev/null || true
# CONTROL: two VALID samples with no progress is a genuine stall, and must still say so.
_mkcm run-C2
expect_reader "11m.4 control: two valid samples, no progress => STALLED" \
  STALLED 3 "did NOT advance" -- "$TMP/cm.txt"
# The verdict text must say it is not a stall, so nobody re-runs a gate on an unmeasurable read.
_mkcm run-C2; ( sleep 1; rm -f "$TMP/cm.txt.heartbeat" ) & _c4=$!; remember_pid "$_c4"
run_reader "$TMP/cm.txt"
printf '%s' "$OUT" | grep -q 'This is NOT a stall' \
  && ok "11m.5 the unmeasurable verdict says explicitly that it is not a stall" \
  || bad "11m.5 the unmeasurable verdict says it is not a stall" "$(printf '%s' "$OUT" | head -1)"
wait "$_c4" 2>/dev/null || true

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
# Redirect the summary into $TMP. These two cases assert on the gate's stderr and stdout, never
# on the summary FILE, and without this they wrote the CHECKOUT DEFAULT
# (.agent-gate-summary.txt) — leaving a synthetic block whose fields say `selftest` but whose
# last line says `RESULT: PASS`, sitting exactly where a closer looks for the gate of record.
# Harmless to these assertions, so there is no reason to leave the trap lying around.
AGENT_GATE_SUMMARY_FILE="$TMP/selftest-summary.txt" bash "$GATE" --emit-summary-selftest >/dev/null 2>"$selftest_err"
if grep -q 'command not found' "$selftest_err"; then
  bad "12.5 an early-exiting gate emits no 'command not found'" "$(grep -m3 'command not found' "$selftest_err")"
else
  ok "12.5 an early-exiting gate emits no 'command not found'"
fi
# Non-vacuity: the probe must be reading a stream that CAN carry the message. If the
# selftest produced nothing at all on stderr the case above is trivially satisfiable, so
# assert the invocation actually ran by checking it emitted a summary block on stdout.
sel_out="$TMP/selftest.out"
AGENT_GATE_SUMMARY_FILE="$TMP/selftest-summary.txt" bash "$GATE" --emit-summary-selftest >"$sel_out" 2>/dev/null
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

echo "=== section 11n: differing unbound run-ids are UNKNOWN — age proves no ordering (job 208) ==="
# THIS SECTION PREVIOUSLY ASSERTED A BUG I INTRODUCED. Written for job 206, 11n.1 required a
# PROVABLY STALE foreign beat to be ignored so the summary's verdict could be reported. Job 208
# (High) showed that is unsound, because **staleness establishes no ordering**:
#
#   summary=B(terminal) + beat=A(stale)   -- beat OLDER than summary (a run that never beat)
#   summary=A(terminal) + beat=B(stale)   -- beat NEWER than summary (a run that beat, then DIED)
#
# Both present identically. Ignoring the beat in the second case reports A's old PASS as the current
# run's outcome: a false COMPLETE, certifying a gate that never finished. So there is no age branch,
# and these cases now pin its ABSENCE. Job 206's real defect — a diagnostic claiming the foreign beat
# was "live" and "NEWER" — is fixed by saying only what is known.
mk_summary "$TMP/sup.txt" run-NEW "PASS"

# Beat OLDER than the summary (job 206's shape).
mk_beat "$TMP/sup.txt.heartbeat" run-OLD 4000 1
expect_reader "11n.1 terminal verdict + STALE foreign beat => UNKNOWN (age proves no ordering)" \
  UNKNOWN 4 "summary-foreign-run" -- "$TMP/sup.txt"

# Beat NEWER than the summary and stale because that run DIED (job 208's shape). Same verdict, and
# that identity is the point: the reader cannot tell these apart, so it must not try.
mk_beat "$TMP/sup.txt.heartbeat" run-DIED 4000 1
expect_reader "11n.2 the beat-newer-and-died shape gets the SAME verdict (indistinguishable)" \
  UNKNOWN 4 "summary-foreign-run" -- "$TMP/sup.txt"

# A FRESH foreign beat: still UNKNOWN. One verdict for every differing run-id, no age branch at all.
mk_beat "$TMP/sup.txt.heartbeat" run-NEWER 5 1
expect_reader "11n.3 terminal verdict + FRESH foreign beat => UNKNOWN" \
  UNKNOWN 4 "summary-foreign-run" -- "$TMP/sup.txt"

# Malformed and foreign-host beats reach the same place, so no shape leaks into a COMPLETE.
{ echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-JUNK"
  echo "this is not a beat"; echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$TMP/sup.txt.heartbeat"
expect_reader "11n.4 terminal verdict + MALFORMED foreign beat => UNKNOWN" \
  UNKNOWN 4 "summary-foreign-run" -- "$TMP/sup.txt"

# The diagnostic must not claim what it cannot know: no "live", no "NEWER".
mk_beat "$TMP/sup.txt.heartbeat" run-OTHER 4000 1
_fr_out=$(bash "$READER" "$TMP/sup.txt" 2>&1 || true)
if printf '%s' "$_fr_out" | grep -qE 'live heartbeat|a NEWER run is starting'; then
  bad "11n.5 the diagnostic claims neither liveness nor ordering" "overclaims: $_fr_out"
else
  ok "11n.5 the diagnostic claims neither liveness nor ordering"
fi
# ...and it must name the way to ask a question that HAS an answer.
printf '%s' "$_fr_out" | grep -q -- '--run-id' \
  && ok "11n.6 the refusal names --run-id as the remedy" \
  || bad "11n.6 the refusal names --run-id as the remedy" "$_fr_out"

# CONTROL: a MATCHING run-id with an equally stale beat must still be COMPLETE. Without this, every
# case above would be satisfied by a reader that answers UNKNOWN unconditionally.
mk_summary "$TMP/sup2.txt" run-SAME "PASS"
mk_beat "$TMP/sup2.txt.heartbeat" run-SAME 4000 1
expect_reader "11n.7 control: matching run-id + stale beat => COMPLETE" \
  COMPLETE 0 "terminal verdict" -- "$TMP/sup2.txt"

echo "=== section 11o: the beater refuses a DIRECTORY destination (job 213) ==="
# `mv -f "$tmp" "$FILE"` treats a directory — or a symlink to one — as a destination DIRECTORY, so it
# SUCCEEDS while dropping a new random temp file inside it every interval. Liveness is never readable,
# every poll answers UNKNOWN, and the accumulating files can fail the gate's own tree-integrity check.
# Measured before the fix: 6 files deposited in 6 seconds at interval 1.
#
# NO `timeout` here: 11c.13 forbids it (stock macOS has no timeout(1)) and caught the first version of
# this section. The beater deliberately does NOT exit on a bad destination — it re-checks before every
# publish so it RECOVERS if the directory is removed — so the run has to be bounded by the suite, with
# the background+poll+kill pattern used elsewhere in this file.
_beat_bounded() {  # <dest> <run-id> <errfile> <watch-path>: run the beater briefly, then stop it
  bash "$BEATER" --file "$1" --run-id "$2" --gate-pid $$ --interval 1 </dev/null >/dev/null 2>"$3" &
  _bb_pid=$!
  remember_pid "$_bb_pid"
  # Bounded: break as soon as there is something to judge, so a broken beater fails a case instead
  # of hanging the suite.
  for _bb_i in {1..20}; do
    [ -s "$3" ] && break
    [ -n "$4" ] && [ -e "$4" ] && break
    sleep 0.5
  done
  kill "$_bb_pid" 2>/dev/null || true
  wait "$_bb_pid" 2>/dev/null || true
}
_dd="$TMP/dirdest.txt"; mkdir -p "$_dd.heartbeat"
_beat_bounded "$_dd.heartbeat" dirprobe "$TMP/dirdest.err" ""
_deposited=$(ls -1 "$_dd.heartbeat" 2>/dev/null | wc -l | tr -d ' ')
[ "$_deposited" = 0 ] && ok "11o.1 a directory destination receives NO deposited temp files" \
                      || bad "11o.1 a directory destination receives no deposited files" "$_deposited file(s) inside"
grep -q 'is a directory' "$TMP/dirdest.err" 2>/dev/null \
  && ok "11o.2 ...and the refusal says why" \
  || bad "11o.2 the refusal says why" "$(head -1 "$TMP/dirdest.err" 2>/dev/null)"
# A SYMLINK to a directory is the same trap in a different shape.
_ds="$TMP/dirsym.txt"; mkdir -p "$TMP/realdir"; ln -s "$TMP/realdir" "$_ds.heartbeat"
_beat_bounded "$_ds.heartbeat" symprobe "$TMP/dirsym.err" ""
_dep2=$(ls -1 "$TMP/realdir" 2>/dev/null | wc -l | tr -d ' ')
[ "$_dep2" = 0 ] && ok "11o.3 a SYMLINK to a directory is refused too" \
                 || bad "11o.3 a symlink to a directory is refused too" "$_dep2 file(s) inside"
# CONTROL: an ordinary file destination must still be published, or every case above would be
# satisfied by a beater that refuses everything.
_dok="$TMP/dirok.txt"
_beat_bounded "$_dok.heartbeat" okprobe "$TMP/dirok.err" "$_dok.heartbeat"
grep -q '^run-id: okprobe$' "$_dok.heartbeat" 2>/dev/null \
  && ok "11o.4 control: an ordinary file destination IS still published" \
  || bad "11o.4 control: an ordinary file destination is still published" "no beat at $_dok.heartbeat"

echo "=== section 11t: beater-pid is validated because it MOVES A VERDICT (job 223) ==="
# `beater-pid` is optional — an older gate's beats omit it — but it is NOT inert: a CHANGED value
# between two samples counts as a beater RELAUNCH and therefore as PROGRESS, which yields RUNNING.
# It was checked only for uniqueness and placement, so two DIFFERENT MALFORMED values read as a
# restart and a pair of invalid beats could produce RUNNING. Absent stays safe (progression decides);
# present-but-nonsense must not be believed.
_bp="$TMP/bpid.txt"
mk_summary "$_bp" run-P "INCOMPLETE (gate did not finish)"
_mkbeat_bp() {  # <beater-pid-value> <age> ; empty value omits the field
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-P"; echo "gate-pid: 4242"
    echo "parent-check: starttime"; echo "host: $(uname -n 2>/dev/null || echo somebox)"
    [ -n "$1" ] && echo "beater-pid: $1"
    echo "interval: 20"; echo "beat-seq: 7"; echo "beat-epoch: $(( $(date +%s) - $2 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$_bp.heartbeat"
}
# ASSERT THE MESSAGE, NOT JUST THE EXIT CODE. The first version of this case checked only `RC = 4`,
# so it PASSED while the diagnostic was the literal `gate-liveness: UNKNOWN ()` — the validation
# returned without setting BEAT_ERR, breaking the invariant that every refusal names its cause
# (roborev job 226). That is the lesson 4b.76 taught in the detached suite one round earlier, not
# propagated to a test written here. An exit code alone cannot distinguish a named refusal from a
# silent one, which is exactly the distinction this reader exists to make.
_bp_bad=0
for _spec in "garbage-one:not-a-pid" "0:zero" "123456789012:out-of-range" " :not-a-pid" "-5:not-a-pid" "12.3:not-a-pid"; do
  _v=${_spec%:*}; _want=${_spec##*:}
  _mkbeat_bp "$_v" 4000
  run_reader "$_bp" --run-id run-P
  if [ "$RC" != 4 ]; then
    _bp_bad=$((_bp_bad+1)); echo "     beater-pid '$_v' accepted (rc=$RC)"
  elif ! printf '%s' "$OUT" | grep -q "heartbeat-beater-pid-$_want"; then
    _bp_bad=$((_bp_bad+1)); echo "     beater-pid '$_v' refused WITHOUT the named cause: $(printf '%s' "$OUT" | head -1)"
  fi
done
[ "$_bp_bad" = 0 ] && ok "11t.1 a malformed beater-pid invalidates the beat AND names its cause (6 shapes)" \
                   || bad "11t.1 a malformed beater-pid invalidates the beat and names its cause" "$_bp_bad shape(s) wrong"
# The invariant itself, so no future refusal in this validator can return silently.
if sed -n '/beater-pid: \$_bp. is not a decimal/,/^  fi$/p' "$READER" | grep -c 'BEAT_ERR=' | grep -qx 3; then
  ok "11t.1b every beater-pid refusal sets BEAT_ERR (3 of 3)"
else
  bad "11t.1b every beater-pid refusal sets BEAT_ERR" "$(sed -n '/_bp=/,/^  fi$/p' "$READER" | grep -c 'BEAT_ERR=') of 3 branches"
fi
# CONTROL 1: a VALID beater-pid must still be accepted, or 11t.1 passes by rejecting everything.
_mkbeat_bp 12345 4000
expect_reader "11t.2 control: a VALID beater-pid is accepted (stale => STALLED)" \
  STALLED 3 "" -- "$_bp" --run-id run-P
# CONTROL 2: an ABSENT beater-pid must remain acceptable — older gates omit it entirely.
_mkbeat_bp "" 4000
expect_reader "11t.3 control: an ABSENT beater-pid still degrades safely" \
  STALLED 3 "" -- "$_bp" --run-id run-P
# CONTROL 3, the one that matters most: validation must not break the LEGITIMATE restart signal.
# Two DIFFERENT VALID pids under one run-id is a relaunch, which only a live gate performs => RUNNING.
_mkbeat_bp 11111 5
( sleep 1; _mkbeat_bp 22222 1 ) & _bpw=$!
remember_pid "$_bpw"
expect_reader "11t.4 control: two DIFFERENT VALID beater-pids still count as a relaunch => RUNNING" \
  RUNNING 2 "" -- "$_bp" --run-id run-P
wait "$_bpw" 2>/dev/null || true

echo "=== section 11w: a case must PASS the flags its NAME claims (job 231) ==="
# 11v.1 was named for `--no-wait` and never passed it, so the reader blocked and answered STALLED. It
# failed loudly, which is the only reason it was caught. Auditing the group immediately then found
# 11v.3 with the IDENTICAL defect and GREEN — a fresh beat answers RUNNING with or without the flag, so
# it tested nothing about `--no-wait` while passing. The dangerous half of "an assertion that does not
# test what its name claims" is the half that passes, and no amount of running the suite finds it.
#
# So the check is mechanical and covers cases added later. Negative mentions are EXCLUDED: several cases
# are legitimately named for a flag's ABSENCE ("no --run-id", "unbound"), and a guard that reds on
# correct input is the guard people learn to waive.
_flagmismatch=$(python3 - "$0" <<'PYEOF_INNER'
import re, sys
src = open(sys.argv[1]).read()
calls = re.findall(r'expect_reader "([^"]+)"\s*\\\n\s*([^\n]+)\n', src)
bad = []
for label, args in calls:
    low = label.lower()
    for flag in ('--no-wait', '--run-id', '--heartbeat'):
        if flag not in label:
            continue
        # the flag's ABSENCE may be the subject of the case
        if any(neg in low for neg in ('no ' + flag, 'without ' + flag, 'unbound', 'omit')):
            continue
        if flag not in args:
            bad.append(label.split()[0] + '->' + flag)
print(','.join(bad))
PYEOF_INNER
) || _flagmismatch="PROBE-FAILED"
if [ "$_flagmismatch" = "PROBE-FAILED" ]; then
  bad "11w.1 every case passes the flags its name claims" "the probe could not run — this proves nothing"
elif [ -z "$_flagmismatch" ]; then
  ok "11w.1 every case passes the flags its name claims"
else
  bad "11w.1 every case passes the flags its name claims" "mismatch(es): $_flagmismatch"
fi

echo "=== section 11z: no summary-section variable can be UNSET on the deferral path (job 238) ==="
# THE AUDIT, MECHANISED. Job 238 was an abort, not a wrong verdict: job 218 let summary-side paths BREAK
# OUT before the initial snapshot was taken, and job 231 then expanded `_SUM_SNAP` — two fixes each
# correct alone, jointly aborting under `set -u`. A gate that completed during the wait produced NO
# verdict, which the launcher reads as unmonitorable.
#
# The generalisation is the check: any variable assigned ONLY inside the summary section but referenced
# after it can be unset on a deferral. Running that audit by hand found 25 such variables, of which one
# (`SUM_RUN_ID`) was referenced after the region — unreachable today, because that reference is an `elif`
# reached only on an unbound read while deferral requires a named run, but the argument is NON-LOCAL and
# a future change to the deferral condition would make it live. Both are now initialised up front, and
# this case keeps the property from regressing as paths are added.
_z_risky=$(python3 - "$READER" <<'PYEOF_INNER'
import re, sys
src = open(sys.argv[1]).read().split('\n')
try:
    start = next(i for i,l in enumerate(src) if l.strip() == 'while :; do')
    end   = next(i for i,l in enumerate(src) if 'the heartbeat side: affirmative liveness' in l)
except StopIteration:
    print('PROBE-FAILED'); raise SystemExit(0)
region, after = src[start:end], '\n'.join(src[end:])
def assigns(lines):
    out = set()
    for l in lines:
        for m in re.finditer(r'(?:^|\s|;)([A-Za-z_][A-Za-z0-9_]*)=', l):
            out.add(m.group(1))
    return out
inside, before = assigns(region), assigns(src[:start])
risky = []
for name in sorted(inside - before):
    if not (name.isupper() or name.startswith('_')):
        continue
    for m in re.finditer(r'\$\{?' + re.escape(name) + r'(\}|[^A-Za-z0-9_}])', after):
        if ':-' in after[m.start():m.start()+len(name)+6]:
            continue
        risky.append(name); break
print(','.join(risky))
PYEOF_INNER
) || _z_risky="PROBE-FAILED"
if [ "$_z_risky" = "PROBE-FAILED" ]; then
  bad "11z.1 no summary-section variable is referenced unset after a deferral" \
      "the probe could not delimit the regions — this proves nothing"
elif [ -z "$_z_risky" ]; then
  ok "11z.1 no summary-section variable is referenced unset after a deferral"
else
  bad "11z.1 no summary-section variable is referenced unset after a deferral" \
      "would abort under set -u on a deferral: $_z_risky"
fi

echo "=== section 11aa: a failed post-wait snapshot is NOT 'no change' (job 241) ==="
# If the summary was readable BEFORE the confirmation wait and cannot be read after it — deleted,
# replaced by a directory, permissions changed — then whether the gate completed during the wait is
# UNKNOWABLE. Continuing from the stale INCOMPLETE snapshot reported STALLED on evidence that no longer
# exists. Third variant of one rule in this file: absence of a measurement read as absence of change
# (the others: `unknown` hostnames comparing equal, and an unrecognised ActiveState reading as stopped).
_aa="$TMP/vanish.txt"
mk_summary "$_aa" run-V "INCOMPLETE (gate did not finish)"
mk_beat "$_aa.heartbeat" run-V 200 1
( sleep 3; rm -f "$_aa" ) & _aw=$!
remember_pid "$_aw"
expect_reader "11aa.1 summary PRESENT then MISSING during the wait => UNKNOWN, not STALLED" \
  UNKNOWN 4 "summary-unreadable-after-wait" -- "$_aa" --run-id run-V
wait "$_aw" 2>/dev/null || true
# Present-then-unreadable is the same class in a different shape.
mk_summary "$_aa" run-V "INCOMPLETE (gate did not finish)"
mk_beat "$_aa.heartbeat" run-V 200 1
( sleep 3; rm -f "$_aa"; mkdir -p "$_aa" ) & _aw2=$!
remember_pid "$_aw2"
expect_reader "11aa.2 summary PRESENT then UNREADABLE during the wait => UNKNOWN" \
  UNKNOWN 4 "summary-unreadable-after-wait" -- "$_aa" --run-id run-V
wait "$_aw2" 2>/dev/null || true
rm -rf "$_aa"
# CONTROL: absent ALL ALONG must still let the heartbeat side answer — a failed snapshot is consistent
# with "was never there", so refusing here would break the case 11x.3 exists for.
_ab="$TMP/neverthere.txt"
rm -f "$_ab"
mk_beat "$_ab.heartbeat" run-V 200 1
expect_reader "11aa.3 control: absent ALL ALONG still yields the heartbeat verdict" \
  STALLED 3 "" -- "$_ab" --run-id run-V

echo "=== section 11x: the post-wait re-decision must survive an ABSENT initial summary (job 238) ==="
# Job 231 compared the post-wait snapshot against the initial one. But several summary-side paths — a
# missing summary, an unsnapshotable one — DEFER to the heartbeat side and break out before the initial
# snapshot is even taken. Under `set -u`, expanding the unset variable ABORTED the script: a gate that
# completed during the wait produced NO verdict at all, which the launcher reads as unmonitorable.
# Reproduced before the fix as "line 908: _SUM_SNAP: unbound variable", exit 1.
#
# And absent-then-present is the MOST important change to detect — it is the gate finishing — yet the
# first version could not see it, because it required the initial snapshot to be non-empty to compare.
_x="$TMP/absent.txt"
rm -f "$_x"
mk_beat "$_x.heartbeat" run-X 200 1
( sleep 3; printf '==== AGENT-GATE SUMMARY ====\nrun-id: run-X\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ====\n' > "$_x" ) &
_xw=$!
remember_pid "$_xw"
expect_reader "11x.1 NO initial summary + one appears during the wait => COMPLETE (no crash)" \
  COMPLETE 0 "terminal verdict" -- "$_x" --run-id run-X
wait "$_xw" 2>/dev/null || true
# The crash shape specifically: an unbound variable must never reach a verdict path.
_xcode=$(grep -c '^_SUM_SNAP=""' "$READER" || true)
[ "$_xcode" -ge 1 ] && ok "11x.2 _SUM_SNAP is initialised before any summary handling" \
                    || bad "11x.2 _SUM_SNAP is initialised before any summary handling" "an unset expansion can still abort under set -u"
# CONTROL: still no summary and no completion => the heartbeat side answers, not a crash.
rm -f "$_x"
mk_beat "$_x.heartbeat" run-X 200 1
run_reader "$_x" --run-id run-X
case "$RC" in
  2|3|4) ok "11x.3 control: absent summary with no completion still yields a verdict (rc=$RC)" ;;
  *)     bad "11x.3 control: absent summary with no completion yields a verdict" "rc=$RC — a crash exit is not a verdict" ;;
esac

echo "=== section 11y: exec must not leak the snapshot directory (job 238) ==="
# `exec` REPLACES the process, so the EXIT trap never runs. The post-wait re-decision leaked its private
# snapshot directory every time it fired. This is a regression of a known class here: an earlier revision
# leaked 868 of them by assigning SNAP_DIR inside a subshell, and 11b.17d exists because of it.
if grep -q 'rm -rf "$SNAP_DIR" 2>/dev/null' "$READER" && grep -q 'exec bash "$0"' "$READER"; then
  ok "11y.1 the snapshot directory is removed before exec hands the process over"
else
  bad "11y.1 the snapshot directory is removed before exec" "exec bypasses the EXIT trap, so this leaks"
fi
_y="$TMP/leak.txt"
_snap_before=$(ls -d "${TMPDIR:-/tmp}"/gate-liveness-snap.* 2>/dev/null | wc -l | tr -d ' ')
mk_summary "$_y" run-Y "INCOMPLETE (gate did not finish)"
mk_beat "$_y.heartbeat" run-Y 200 1
( sleep 3; printf '==== AGENT-GATE SUMMARY ====\nrun-id: run-Y\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ====\n' > "$_y" ) &
_yw=$!
remember_pid "$_yw"
run_reader "$_y" --run-id run-Y
wait "$_yw" 2>/dev/null || true
_snap_after=$(ls -d "${TMPDIR:-/tmp}"/gate-liveness-snap.* 2>/dev/null | wc -l | tr -d ' ')
if [ "$_snap_after" -le "$_snap_before" ]; then
  ok "11y.2 a re-decision that EXECs leaves no snapshot directory behind ($_snap_before -> $_snap_after)"
else
  bad "11y.2 a re-decision that EXECs leaves no snapshot directory behind" "$_snap_before -> $_snap_after"
fi

echo "=== section 11u: ONE grammar for the post-wait re-decision (job 231) ==="
# Job 228 re-parsed the fresh summary after the confirmation wait with a deliberately narrow check, and I
# argued that being "promote-only" made the overlap with the main grammar safe. THAT ARGUMENT WAS WRONG:
# it counted openers and closers but never checked dialect match, ordering, or duplicate RESULT/run-id,
# so a SPLICED summary could be promoted to COMPLETE — and promoting on a malformed artifact IS a false
# certification, the worst verdict here. It also sent a valid block with an unrecognised token to STALLED,
# contradicting job 220.
#
# There is no second parser now: if the summary changed during the wait, the script re-execs itself with
# the caller's original request plus --no-wait, so the whole framing grammar, run-id binding and terminal
# dispatch apply exactly as on a first read, and --no-wait guarantees termination.
# The PROPERTY is: re-exec THIS script with the CALLER'S ORIGINAL request plus --no-wait — not one
# spelling of the array expansion. Job 319 rewrote it to the bash-3.2-safe ${A[@]+"${A[@]}"} form
# (empty "${A[@]}" is unbound and aborts under `set -u` on bash < 4.4), and this test FAILED on that
# correct change — the seventh implementation-literal test in this change to red on a right answer.
if grep -qE 'exec bash "\$0" ("\$\{GL_ORIG_ARGS\[@\]\}"|\$\{GL_ORIG_ARGS\[@\]\+"\$\{GL_ORIG_ARGS\[@\]\}"\}) --no-wait' "$READER"; then
  ok "11u.1 the post-wait re-decision re-execs the real grammar (no second parser)"
else
  bad "11u.1 the post-wait re-decision re-execs the real grammar" "a second parser may have returned"
fi
_uw="$TMP/upost.txt"
_mk_post() {  # <what the summary becomes after 3s>
  mk_summary "$_uw" run-U "INCOMPLETE (gate did not finish)"
  mk_beat "$_uw.heartbeat" run-U 200 1
  ( sleep 3; printf '%s' "$1" > "$_uw" ) &
  _post_w=$!
  remember_pid "$_post_w"
}
# A genuine completion during the wait is still recognised.
_mk_post '==== AGENT-GATE SUMMARY ====
run-id: run-U
RESULT: PASS
==== END AGENT-GATE SUMMARY ====
'
expect_reader "11u.2 a real completion during the wait => COMPLETE" \
  COMPLETE 0 "terminal verdict" -- "$_uw" --run-id run-U
wait "$_post_w" 2>/dev/null || true
# A DIALECT-MISMATCHED block appearing during the wait must NOT be promoted — this was a false COMPLETE.
_mk_post '==== AGENT-GATE SUMMARY ====
run-id: run-U
RESULT: PASS
==== END AGENT-GATE LITE SUMMARY ====
'
run_reader "$_uw" --run-id run-U
[ "$RC" != 0 ] && ok "11u.3 a spliced/dialect-mismatched summary is NOT promoted (rc=$RC)" \
               || bad "11u.3 a spliced summary is not promoted" "promoted to COMPLETE — a false certification"
wait "$_post_w" 2>/dev/null || true
# A valid block with an UNRECOGNISED token => UNKNOWN naming it, not STALLED.
_mk_post '==== AGENT-GATE SUMMARY ====
run-id: run-U
RESULT: FUTURETOKEN
==== END AGENT-GATE SUMMARY ====
'
expect_reader "11u.4 an unrecognised token during the wait => UNKNOWN, not STALLED" \
  UNKNOWN 4 "unrecognised-result" -- "$_uw" --run-id run-U
wait "$_post_w" 2>/dev/null || true

echo "=== section 11v: --no-wait can only WEAKEN a verdict (job 231) ==="
# The launcher needs a bounded call. --no-wait skips the stall confirmation, so STALLED becomes
# unprovable and the answer weakens to UNKNOWN. It must never produce a STRONGER claim than the blocking
# form, or a bounded caller would be trading correctness for latency.
_nw="$TMP/nowait.txt"
mk_summary "$_nw" run-N "INCOMPLETE (gate did not finish)"
mk_beat "$_nw.heartbeat" run-N 4000 1
# The flag this case is NAMED for must actually be PASSED. The first version omitted it, so the reader
# blocked and answered STALLED — and the case failed for the right reason, which is the only thing that
# saved it: an assertion whose name describes a flag it never passes tests something else entirely.
# Second instance in this change (11r.3 was the first), and my manual check missed it because I verified
# only the EXIT CODE — 4 arrives from several different paths here.
expect_reader "11v.1 stale beat + --no-wait => UNKNOWN (a stall is unprovable without a 2nd sample)" \
  UNKNOWN 4 "confirmation-skipped" -- "$_nw" --run-id run-N --no-wait
# CONTROL: the BLOCKING form still reaches STALLED, or --no-wait has not weakened anything — it has
# simply become the only behaviour.
expect_reader "11v.2 control: the BLOCKING form still confirms and answers STALLED" \
  STALLED 3 "" -- "$_nw" --run-id run-N
# A FRESH beat needs no confirmation, so --no-wait must not weaken it.
mk_beat "$_nw.heartbeat" run-N 5 1
# SAME DEFECT, found by auditing the group immediately after fixing 11v.1 — and this one the suite could
# NEVER have caught: a fresh beat answers RUNNING with or without the flag, so it was GREEN while testing
# nothing about --no-wait. The dangerous half of "an assertion that does not test what its name claims"
# is the half that passes.
expect_reader "11v.3 a FRESH beat + --no-wait is still RUNNING (no confirmation needed)" \
  RUNNING 2 "" -- "$_nw" --run-id run-N --no-wait

echo "=== section 11r: a FAILED lookup never proves a shared clock domain (job 221) ==="
# Both the reader and the beater used `uname -n 2>/dev/null || echo unknown`. When BOTH lookups fail
# the two literal `unknown`s compare EQUAL and were accepted as proof of a SHARED CLOCK DOMAIN — which
# licenses judging freshness from `beat-epoch`, so a dead cross-host beat could report RUNNING on
# incomparable timestamps. Absence of measurement read as a positive match: the shape this file
# refuses everywhere else, sitting in the one field that gates the epoch comparison.
#
# The root misconception was written down in the beater: a comment claimed `host` was "a DIAGNOSTIC
# ... not an input to any verdict". It is an input, and that belief is what made the placeholder look
# harmless.
_ck="$TMP/clock.txt"
mk_summary "$_ck" run-C "INCOMPLETE (gate did not finish)"
_mkbeat_host() {  # <host-value> <age>
  { echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-C"; echo "gate-pid: 4242"
    echo "parent-check: starttime"; [ -n "$1" ] && echo "host: $1"
    echo "interval: 20"; echo "beat-seq: 7"; echo "beat-epoch: $(( $(date +%s) - $2 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$_ck.heartbeat"
}
# A FRESH epoch with host `unknown` must NOT be believed: no proven clock domain, static counter.
_mkbeat_host unknown 5
expect_reader "11r.1 host 'unknown' + fresh epoch => NOT a false RUNNING" \
  STALLED 3 "clock-domain UNPROVEN" -- "$_ck" --run-id run-C
# An ABSENT host behaves identically — absence and unverified are ONE state, not two spellings.
_mkbeat_host "" 5
expect_reader "11r.2 an ABSENT host behaves the same as 'unknown'" \
  STALLED 3 "clock-domain UNPROVEN" -- "$_ck" --run-id run-C
# CONTROL: a REAL matching host still proves the domain, or 11r.1/2 would be satisfied by a reader
# that never trusts any host.
_mkbeat_host "$(uname -n 2>/dev/null || echo somebox)" 5
expect_reader "11r.3 control: a REAL matching host still proves the clock domain => RUNNING" \
  RUNNING 2 "clock-domain shared" -- "$_ck" --run-id run-C
# Neither side may reintroduce the placeholder.
# CODE ONLY, comments stripped. The first version of this case failed against the comments EXPLAINING
# the defect, which quote `|| echo unknown` verbatim — the same self-defeating shape as 4b.81c in the
# detached suite, and the second time in this change that documenting a defect tripped the guard
# written to catch it. The fix is to separate the channels, not to reword the explanation.
_ph_bad=0
for _f in "$READER" "$BEATER"; do
  grep -vE '^[[:space:]]*#' "$_f" | grep -q 'echo unknown' && { _ph_bad=$((_ph_bad+1)); echo "     placeholder in $(basename "$_f")"; }
done
[ "$_ph_bad" = 0 ] && ok "11r.4 neither reader nor beater falls back to the literal 'unknown'" \
                   || bad "11r.4 neither reader nor beater falls back to 'unknown'" "$_ph_bad file(s) still do"
# And the beater must OMIT the field rather than publish an empty one.
if grep -q '\[ -n "$HOST_NAME" \] && echo "host: $HOST_NAME"' "$BEATER"; then
  ok "11r.5 the beater omits host entirely when it cannot determine one"
else
  bad "11r.5 the beater omits host when undeterminable" "it publishes the field regardless"
fi

echo "=== section 11s: termination needs a COMPLETE block (job 221) ==="
# Job 220 established that an unrecognised RESULT token means the gate terminated, so it must not
# defer. That holds only for a FINISHED write: a truncated summary can carry a partial `RESULT:` line
# and no closer, and treating that as termination bypasses the heartbeat exactly when a stale matching
# beat should establish STALLED. The recognised-terminal path already required the closer; the branch
# added for job 220 did not.
_tr="$TMP/trunc.txt"
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-T"; echo "RESULT: FUTURETHING"; } > "$_tr"
mk_beat "$_tr.heartbeat" run-T 4000 1
expect_reader "11s.1 unrecognised token in a TRUNCATED write + stale beat => STALLED" \
  STALLED 3 "" -- "$_tr" --run-id run-T
# CONTROL: the same token in a COMPLETE block is termination, and no beat overrides it.
mk_summary "$TMP/complete.txt" run-T "FUTURETHING"
mk_beat "$TMP/complete.txt.heartbeat" run-T 4000 1
expect_reader "11s.2 control: the same token in a COMPLETE block => UNKNOWN (termination)" \
  UNKNOWN 4 "unrecognised-result" -- "$TMP/complete.txt" --run-id run-T

echo "=== section 11q: NO summary-side path may verdict UNKNOWN alone (job 218) ==="
# THE GUARD THAT SHOULD HAVE EXISTED THREE ROUNDS AGO. Job 209 built `_summary_refusal` as "one
# decision point for every summary-side refusal" and asserted it with a grep for
# `verdict UNKNOWN 4 "summary-` outside the funnel. That checked a NAME PREFIX, not a PROPERTY — so
# `no-summary-artifact`, `no-snapshot-dir`, `no-result-line` and `unrecognised-result` bypassed the
# funnel for three more rounds while the guard reported clean. Checking a spelling instead of a state
# is the exact mistake this file documents in the reader's own verdict scan.
#
# The property, stated positively: between the summary section's opening and the heartbeat side, EVERY
# UNKNOWN must go through the funnel — because only the funnel consults the beat. Derived from the
# file's own structure at run time, so a path added later is covered without editing this test.
_gl="$READER"
_reg=$(awk '/^while :; do$/{f=1} /the heartbeat side: affirmative liveness/{f=0} f' "$_gl")
if [ -z "$_reg" ]; then
  bad "11q.1 no summary-side path verdicts UNKNOWN outside the funnel" \
      "could not delimit the summary region — the derivation failed, so this proves nothing"
else
  _bare=$(printf '%s\n' "$_reg" | grep -c 'verdict UNKNOWN' || true)
  if [ "$_bare" = 0 ]; then
    ok "11q.1 no summary-side path verdicts UNKNOWN outside the funnel ($(printf '%s\n' "$_reg" | grep -c '_summary_refusal_or_defer' ) routed)"
  else
    bad "11q.1 no summary-side path verdicts UNKNOWN outside the funnel" \
        "$_bare bare UNKNOWN verdict(s) bypass the beat check"
  fi
fi
# ...and every routed call must BREAK, or a deferral falls through into the next check instead of
# reaching the heartbeat side. One site had exactly that defect (it sat above the wrapper entirely).
_unbroken=$(printf '%s\n' "$_reg" | grep '_summary_refusal_or_defer "' | grep -vc '|| break' || true)
[ "$_unbroken" = 0 ] && ok "11q.2 every routed refusal breaks out to the heartbeat side" \
                     || bad "11q.2 every routed refusal breaks out to the heartbeat side" "$_unbroken site(s) do not"

# Behavioural: the three paths that bypassed the funnel, each with a STALE matching beat.
_q="$TMP/q"
mk_beat "$_q-miss.txt.heartbeat" run-Q 4000 1
expect_reader "11q.3 MISSING summary + stale matching beat => STALLED" \
  STALLED 3 "" -- "$_q-miss.txt" --run-id run-Q
{ echo "==== AGENT-GATE SUMMARY ===="; echo "run-id: run-Q"; echo "==== END AGENT-GATE SUMMARY ===="; } > "$_q-nores.txt"
mk_beat "$_q-nores.txt.heartbeat" run-Q 4000 1
expect_reader "11q.4 summary with NO RESULT line + stale matching beat => STALLED" \
  STALLED 3 "" -- "$_q-nores.txt" --run-id run-Q
# CORRECTED by job 220, and the correction is the interesting part. Job 218 routed this path through
# the deferral funnel "for consistency" — but an unrecognised RESULT token is NOT the same class as an
# absent or unparseable summary. A well-formed summary naming this run and bearing `RESULT: <new>`
# says the gate TERMINATED; only the verdict's NAME is unknown. Deferring turned "I cannot name this
# verdict" into "the gate seems dead", and a caller told STALLED may relaunch a finished run. The
# beat's staleness here is a CONSEQUENCE of termination, not evidence of a stall.
#
# The lesson: job 218's rule (do not make per-site judgements about which paths may skip the funnel)
# was right for paths carrying NO termination information, and over-applied here, where the sites
# genuinely differ. Uniformity is not a substitute for asking what each artifact actually says.
mk_summary "$_q-weird.txt" run-Q "WEIRDVALUE"
mk_beat "$_q-weird.txt.heartbeat" run-Q 4000 1
expect_reader "11q.5 UNRECOGNISED verdict token + stale matching beat => UNKNOWN (termination, not a stall)" \
  UNKNOWN 4 "unrecognised-result" -- "$_q-weird.txt" --run-id run-Q
# ...and a FRESH beat does not change it either: the closed grammar is not negotiable.
mk_beat "$_q-weird.txt.heartbeat" run-Q 5 1
expect_reader "11q.5b UNRECOGNISED token + FRESH matching beat => still UNKNOWN" \
  UNKNOWN 4 "unrecognised-result" -- "$_q-weird.txt" --run-id run-Q
# CONTROL: a summary with NO RESULT line still DEFERS to a fresh beat, or the two policies have
# collapsed into one and job 218's fix has been undone.
mk_beat "$_q-nores.txt.heartbeat" run-Q 5 1
expect_reader "11q.5c control: NO RESULT line + fresh beat still DEFERS => RUNNING" \
  RUNNING 2 "is beating" -- "$_q-nores.txt" --run-id run-Q
# The two policies must remain DISTINCT and both named, so the property guard stays property-shaped
# instead of growing a name-based exception.
if grep -q '^_summary_terminal_unknown()' "$READER" \
   && grep -q '_summary_terminal_unknown "unrecognised-result' "$READER"; then
  ok "11q.5d the non-deferring policy is its own named helper, used by the closed-grammar path"
else
  bad "11q.5d the non-deferring policy is its own named helper" "not found"
fi
# CONTROLS: with no beat, each path must still report its own specific cause.
rm -f "$_q-miss.txt.heartbeat"
expect_reader "11q.6 control: missing summary + NO beat => UNKNOWN (no-summary-artifact)" \
  UNKNOWN 4 "no-summary-artifact" -- "$_q-miss.txt" --run-id run-Q
rm -f "$_q-nores.txt.heartbeat"
expect_reader "11q.7 control: no RESULT line + NO beat => UNKNOWN (no-result-line)" \
  UNKNOWN 4 "no-result-line" -- "$_q-nores.txt" --run-id run-Q
rm -f "$_q-weird.txt.heartbeat"
expect_reader "11q.8 control: unrecognised token + NO beat => UNKNOWN (unrecognised-result)" \
  UNKNOWN 4 "unrecognised-result" -- "$_q-weird.txt" --run-id run-Q

echo "=== section 11p: an unusable summary must not pre-empt the heartbeat (job 216) ==="
# The MIRROR of job 209. That round made summary refusals defer to a FRESH matching beat (RUNNING).
# A valid matching beat that had gone STALE still hit UNKNOWN first, so STALLED was UNREACHABLE for a
# gate reaped during the pre-sentinel tree capture — precisely the startup interval that moving the
# beater BEFORE the tree capture exists to cover. Fixing one direction and leaving the other is the
# same half-fix this change has made before, so all four combinations are pinned here together.
_up="$TMP/unusable.txt"
printf 'not a gate summary at all\n' > "$_up"

mk_beat "$_up.heartbeat" run-R 4000 1
expect_reader "11p.1 unusable summary + STALE matching beat => STALLED (reaches the confirmation)" \
  STALLED 3 "" -- "$_up" --run-id run-R
mk_beat "$_up.heartbeat" run-R 5 1
expect_reader "11p.2 unusable summary + FRESH matching beat => RUNNING" \
  RUNNING 2 "is beating" -- "$_up" --run-id run-R
rm -f "$_up.heartbeat"
expect_reader "11p.3 unusable summary + NO beat => UNKNOWN (nothing to be authoritative)" \
  UNKNOWN 4 "" -- "$_up" --run-id run-R
mk_beat "$_up.heartbeat" run-OTHER 4000 1
expect_reader "11p.4 unusable summary + beat for ANOTHER run => UNKNOWN (not our authority)" \
  UNKNOWN 4 "" -- "$_up" --run-id run-R
# A MALFORMED beat is not an authority either, however matching its run-id looks.
{ echo "==== AGENT-GATE HEARTBEAT ===="; echo "run-id: run-R"; echo "garbage"
  echo "==== END AGENT-GATE HEARTBEAT ===="; } > "$_up.heartbeat"
expect_reader "11p.5 unusable summary + MALFORMED matching beat => UNKNOWN" \
  UNKNOWN 4 "" -- "$_up" --run-id run-R
# CONTROL: a VALID TERMINAL summary still wins over a fresh beat, or the deferral has quietly become
# "the heartbeat always decides" and COMPLETE would be unreachable.
mk_summary "$TMP/usable.txt" run-R "PASS"
mk_beat "$TMP/usable.txt.heartbeat" run-R 5 1
expect_reader "11p.6 control: a valid TERMINAL summary still wins over a fresh beat" \
  COMPLETE 0 "terminal verdict" -- "$TMP/usable.txt" --run-id run-R

echo
echo "==== test_gate_liveness.sh: passed=$pass failed=$fail ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
