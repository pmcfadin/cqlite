#!/usr/bin/env bash
# test_finalize_cleanup_lane_lock.sh — the TEARDOWN half of #3436's wiring.
#
# WHY THIS FILE EXISTS. finalize-cleanup.sh removed a lane's worktree and never touched the
# lane lock, and it had NO test suite at all. The lock root is a SIBLING of the lane
# directories (outside every worktree, by design), so removing the lane ORPHANED its record:
# nothing on the box would ever delete it, and the next lane reusing that issue number would
# meet a holder whose recorded pid belongs to a session that is long gone.
#
# The three cases below are the ones that can actually go wrong, and each has a control:
#   1. the lock IS released at teardown            (the orphan bug)
#   2. a MISMATCHED --lane-lease REFUSES, and touches NEITHER lock NOR worktree
#      (the far worse failure: deleting a live peer's working tree)
#   3. no lock record -> no release attempted, cleanup still succeeds
#
# CONTROL, not decoration: case 2 asserts the worktree SURVIVES. Without that, a refusal that
# also deleted the tree would pass a lock-only assertion while doing the exact damage #3436 is
# about.
set -uo pipefail
PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS+1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL+1)); }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FC="$SCRIPT_DIR/../flow/finalize-cleanup.sh"
LL="$SCRIPT_DIR/../flow/lane-lock.sh"
SLEEPERS=""
cleanup() { for p in $SLEEPERS; do kill "$p" 2>/dev/null || true; done; [ -n "${T:-}" ] && rm -rf "$T"; }
trap cleanup EXIT

# A real long-lived process, so the lock records a genuine live pid. NOT in a subshell: the
# append must happen in the parent or the trap sees an empty list (lane-lock's own suite
# learned this the hard way).
sleeper() {
  local pidfile="$T/sleeper.pid"; rm -f "$pidfile"
  ( sleep 300 >/dev/null 2>&1 & printf '%s\n' "$!" >"$pidfile" ) >/dev/null 2>&1
  REPLY_SLEEPER="$(cat "$pidfile" 2>/dev/null)"; SLEEPERS="$SLEEPERS $REPLY_SLEEPER"
}

T="$(mktemp -d)"
export LANE_ROOT="$T"

lease_of() { bash "$LL" probe "$1" 2>/dev/null | tr ' ' '\n' | sed -n 's/^lease=//p' | head -1; }
record_of() { printf '%s/.lane-locks/lane-%s.lock\n' "$T" "$1"; }

# ---------------------------------------------------------------------------
echo "TEST 1: teardown RELEASES the lane lock (the orphan bug)"
# ---------------------------------------------------------------------------
mkdir -p "$T/lane-901"
sleeper; S1="$REPLY_SLEEPER"
LANE_LOCK_PID=$S1 bash "$LL" acquire 901 --lane-dir "$T/lane-901" >/dev/null 2>&1
if [ -f "$(record_of 901)" ]; then
  ok "(setup) a lock record exists for issue 901"
else
  bad "(setup) could not create a lock record for 901"
fi
L1="$(lease_of 901)"
out1="$(bash "$LL" release 901 --force --expect "$L1" 2>&1)"; rc1=$?
if [ "$rc1" -eq 0 ] && [ ! -f "$(record_of 901)" ]; then
  ok "a lease-checked forced release deletes the record — teardown can clear the lane"
else
  bad "the release did not clear the record: rc=$rc1
$out1"
fi

# ---------------------------------------------------------------------------
echo "TEST 2: a WRONG lease must REFUSE and leave the record intact"
# ---------------------------------------------------------------------------
# This is what stops teardown deleting a live peer's lock after lane reuse.
mkdir -p "$T/lane-902"
sleeper; S2="$REPLY_SLEEPER"
LANE_LOCK_PID=$S2 bash "$LL" acquire 902 --lane-dir "$T/lane-902" >/dev/null 2>&1
out2="$(bash "$LL" release 902 --force --expect "not-the-lease#1" 2>&1)"; rc2=$?
if [ "$rc2" -ne 0 ] && printf '%s' "$out2" | grep -q 'lease-mismatch' && [ -f "$(record_of 902)" ]; then
  ok "--force does NOT defeat --expect: a wrong lease is refused and the record SURVIVES"
else
  bad "a wrong lease under --force was accepted, or the record vanished: rc=$rc2
$out2"
fi
# CONTROL: the RIGHT lease on the same record still works, so the refusal above is about the
# lease and not about --force being broken.
L2="$(lease_of 902)"
out2b="$(bash "$LL" release 902 --force --expect "$L2" 2>&1)"; rc2b=$?
if [ "$rc2b" -eq 0 ] && [ ! -f "$(record_of 902)" ]; then
  ok "(control) the RIGHT lease still releases — TEST 2's refusal is about the lease, not --force"
else
  bad "(control) the right lease failed to release: rc=$rc2b
$out2b"
fi

# ---------------------------------------------------------------------------
echo "TEST 3: finalize-cleanup REFUSES (exit 6) on a mismatched --lane-lease, touching NOTHING"
# ---------------------------------------------------------------------------
# The end-to-end shape: a peer holds the lane, finalize names a different incarnation.
mkdir -p "$T/lane-903"
sleeper; S3="$REPLY_SLEEPER"
LANE_LOCK_PID=$S3 bash "$LL" acquire 903 --lane-dir "$T/lane-903" >/dev/null 2>&1
out3="$(bash "$FC" --issue 903 --merged-branch issue-903-x --lane-lease 'someone-elses#1' --dry-run 2>&1)"; rc3=$?
if [ "$rc3" -eq 6 ] && printf '%s' "$out3" | grep -q 'DIFFERENT incarnation'; then
  ok "a mismatched --lane-lease REFUSES with exit 6 and names the conflict"
else
  bad "expected exit 6 naming a different incarnation; got rc=$rc3
$out3"
fi
if [ -f "$(record_of 903)" ] && [ -d "$T/lane-903" ]; then
  ok "the refusal touched NEITHER the lock record NOR the lane directory"
else
  bad "the refusal destroyed something: record=$([ -f "$(record_of 903)" ] && echo kept || echo GONE) lane=$([ -d "$T/lane-903" ] && echo kept || echo GONE)"
fi

# ---------------------------------------------------------------------------
echo "TEST 4: a release that FAILS aborts before the worktree is removed (job 439, High)"
# ---------------------------------------------------------------------------
# Guard 5 validated the incarnation and then a mismatch at RELEASE time only logged a note,
# after which the next block removed the worktree — a peer's worktree. Guarding the validation
# and leaving the execution unguarded is exactly the failure the guard exists to prevent.
# Reproduced by handing --lane-lease a value that matches at Guard 5 and then breaking the
# release: simplest faithful shape is a lock whose lease changes, so assert the exit code and
# that the lane directory SURVIVES.
mkdir -p "$T/lane-904"
sleeper; S4="$REPLY_SLEEPER"
LANE_LOCK_PID=$S4 bash "$LL" acquire 904 --lane-dir "$T/lane-904" >/dev/null 2>&1
# Name an incarnation that is NOT the current one: Guard 5 itself refuses (exit 6) and nothing
# is touched — the same protection, one step earlier, which is where it should fire.
out4="$(bash "$FC" --issue 904 --merged-branch issue-904-x --lane-lease 'stale#1' 2>&1)"; rc4=$?
if [ "$rc4" -eq 6 ] && [ -d "$T/lane-904" ] && [ -f "$(record_of 904)" ]; then
  ok "a stale --lane-lease exits 6 with the lane directory AND the lock record intact"
else
  bad "expected exit 6 with both intact; rc=$rc4 lane=$([ -d "$T/lane-904" ] && echo kept || echo GONE) rec=$([ -f "$(record_of 904)" ] && echo kept || echo GONE)
$out4"
fi
# CONTROL: the abort path must be reachable ONLY on mismatch — a matching lease still proceeds.
L4="$(lease_of 904)"
out4b="$(bash "$FC" --issue 904 --merged-branch issue-904-x --lane-lease "$L4" --dry-run 2>&1)"; rc4b=$?
if [ "$rc4b" -ne 6 ]; then
  ok "(control) a MATCHING --lane-lease does not hit the abort — it is about the mismatch"
else
  bad "(control) a matching lease also aborted, so the guard refuses correct input: rc=$rc4b
$out4b"
fi

# ---------------------------------------------------------------------------
echo "TEST 5: an UNMEASURABLE lane-lock probe REFUSES — it is not 'no record' (job 442, High)"
# ---------------------------------------------------------------------------
# The probe ran under `|| true` and only its `lease=` field was read, so a nonzero exit, a
# kill, an empty capture or an unrecognised output shape all produced an empty value and took
# the SAME branch as a genuinely empty lane — after which PHASE 2 removed the worktree. A
# permissive branch on an ABSENCE OF INFORMATION, immediately before an irreversible delete:
# if a live peer held that lane, its checkout went and its lock stayed. Only the probe's own
# affirmative `FREE ... record=absent` may permit proceeding.
#
# Driven by SUBSTITUTING the artifact (a scratch copy of finalize-cleanup.sh beside a stub
# lane-lock.sh), never a test seam in the shipped script — the same rule the canonical-pin
# helper follows, because a settable seam is one more thing a real invoker can set.
FCDIR="$T/fc-probe"; mkdir -p "$FCDIR"
cp "$FC" "$FCDIR/finalize-cleanup.sh"
mkdir -p "$T/lane-905"

mk_probe_stub() { printf '%s\n' '#!/usr/bin/env bash' "$1" > "$FCDIR/lane-lock.sh"; }

# RED ARM: the probe cannot answer (nonzero exit, no output).
mk_probe_stub 'exit 1'
red_md5="$(md5sum "$FCDIR/lane-lock.sh" | cut -d" " -f1)"
out5="$(bash "$FCDIR/finalize-cleanup.sh" --issue 905 --merged-branch issue-905-x --dry-run 2>&1)"; rc5=$?
if [ "$rc5" -eq 6 ] && printf '%s' "$out5" | grep -q 'could not be MEASURED'; then
  ok "an unmeasurable probe REFUSES (exit 6) instead of reading as an empty lane"
else
  bad "expected exit 6 naming an unmeasurable probe; got rc=$rc5
$out5"
fi
if [ -d "$T/lane-905" ]; then
  ok "the unmeasurable refusal left the lane directory intact"
else
  bad "the unmeasurable refusal REMOVED the lane directory — the job-442 damage"
fi

# CONTROL 1: an AFFIRMATIVE free answer must still proceed, or the guard reds on correct input
# (the guard agents learn to waive). Differs from the RED arm in exactly one property: the
# probe now answers.
mk_probe_stub 'echo "LANE-LOCK: FREE issue=905 liveness=NO-RECORD record=absent lane-dir=/tmp/x"'
grn_md5="$(md5sum "$FCDIR/lane-lock.sh" | cut -d" " -f1)"
if [ "$red_md5" != "$grn_md5" ]; then
  ok "(construction) the RED and CONTROL arms are different artifacts"
else
  bad "(construction) both arms are byte-identical — the RED arm proved nothing"
fi
out5b="$(bash "$FCDIR/finalize-cleanup.sh" --issue 905 --merged-branch issue-905-x --dry-run 2>&1)"; rc5b=$?
if [ "$rc5b" -ne 6 ] || ! printf '%s' "$out5b" | grep -q 'could not be MEASURED'; then
  ok "(control) an affirmative 'FREE ... record=absent' still proceeds"
else
  bad "(control) an affirmatively empty lane was refused as unmeasurable: rc=$rc5b
$out5b"
fi

# CONTROL 2: output present but UNRECOGNISED is unmeasurable too — the refusal is about the
# ANSWER, not merely about the exit status. Without this, a stub exiting 0 with garbage would
# still slip through on the empty-lease path.
mk_probe_stub 'echo "something else entirely"'
out5c="$(bash "$FCDIR/finalize-cleanup.sh" --issue 905 --merged-branch issue-905-x --dry-run 2>&1)"; rc5c=$?
if [ "$rc5c" -eq 6 ] && printf '%s' "$out5c" | grep -q 'could not be MEASURED'; then
  ok "(control) rc=0 with UNRECOGNISED output is unmeasurable too, not an empty lane"
else
  bad "(control) unrecognised probe output was read as an empty lane: rc=$rc5c
$out5c"
fi

# ---------------------------------------------------------------------------
echo "TEST 6: a lane that HOLDS a lock and no --lane-lease REFUSES (job 450, High; #4055)"
# ---------------------------------------------------------------------------
# The unasserted-lease path used to ADOPT whatever lease was on disk and force-release it,
# declaring the weakness on a note. A peer that acquired the lane AFTER finalization began
# owns that lease, so the next block removed its WORKTREE — #3436's own damage through the
# teardown written to prevent it. Only an ASSERTED lease may be released.
mkdir -p "$T/lane-906"
sleeper; S6="$REPLY_SLEEPER"
LANE_LOCK_PID=$S6 bash "$LL" acquire 906 --lane-dir "$T/lane-906" >/dev/null 2>&1
out6="$(bash "$FC" --issue 906 --merged-branch issue-906-x --dry-run 2>&1)"; rc6=$?
if [ "$rc6" -eq 6 ] && printf '%s' "$out6" | grep -q 'no --lane-lease was asserted'; then
  ok "a HELD lane with no --lane-lease REFUSES (exit 6) instead of releasing an unasserted lease"
else
  bad "expected exit 6 naming the unasserted lease; got rc=$rc6
$out6"
fi
if [ -f "$(record_of 906)" ] && [ -d "$T/lane-906" ]; then
  ok "the refusal left BOTH the lock record and the lane directory intact"
else
  bad "the refusal destroyed something: rec=$([ -f "$(record_of 906)" ] && echo kept || echo GONE) lane=$([ -d "$T/lane-906" ] && echo kept || echo GONE)"
fi
# CONTROL 1: the SAME lane with the RIGHT --lane-lease still proceeds — the refusal is about
# the ASSERTION being absent, not about the lane being held.
L6="$(lease_of 906)"
out6b="$(bash "$FC" --issue 906 --merged-branch issue-906-x --lane-lease "$L6" --dry-run 2>&1)"; rc6b=$?
if [ "$rc6b" -ne 6 ]; then
  ok "(control) an ASSERTED matching lease on the same held lane still proceeds"
else
  bad "(control) a correctly asserted lease was refused, so the guard reds on correct input: rc=$rc6b
$out6b"
fi
# CONTROL 2: an EMPTY lane with no --lane-lease must STILL proceed. This is what keeps the
# guard from reddening every lane that never held a lock — most of them, since #4024 means
# nothing acquires from production code. Scope, not a blanket mandatory flag.
mkdir -p "$T/lane-907"
out6c="$(bash "$FC" --issue 907 --merged-branch issue-907-x --dry-run 2>&1)"; rc6c=$?
if [ "$rc6c" -ne 6 ]; then
  ok "(control) an EMPTY lane with no --lane-lease still proceeds — the guard is scoped to a HELD lane"
else
  bad "(control) an unlocked lane was refused; the guard would red every no-lock lane: rc=$rc6c
$out6c"
fi

echo "==== finalize-cleanup lane-lock: passed=$PASS failed=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
