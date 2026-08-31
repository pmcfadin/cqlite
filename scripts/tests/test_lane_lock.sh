#!/usr/bin/env bash
#
# Regression tests for scripts/flow/lane-lock.sh (issue #3436, epic #2664).
#
# WHAT THIS SUITE IS FOR — #3436 AC4: "Regression coverage for both directions: a
# second session in an occupied lane is refused, and a lane whose holder is dead is
# reclaimed. NEITHER MAY PASS BY DOING NOTHING." So the suite carries explicit
# NON-VACUITY controls, not just the two happy directions:
#   * a refuse-everything implementation fails cases 1/6/13/15 (free acquire, holder
#     release, matched CAS, reclaim-on-dead);
#   * a grant-everything implementation fails cases 2/8/9/10/11/12 (the occupied and
#     every UNKNOWN-* refusal);
#   * a refusal for the WRONG REASON cannot satisfy case 2, which asserts the OCCUPIED
#     line's holder-pid is the FIRST holder's pid (AC2's "name the occupant");
#   * a reclaim that merely exits 0 cannot satisfy case 4/5, which assert the record's
#     token actually CHANGED and that the audit log gained a line naming the previous
#     token, the previous liveness verdict and the reason (AC3);
#   * every UNKNOWN-* case asserts the record token is UNCHANGED afterwards — a
#     refusal that silently rewrote the record would pass a naive exit-code check.
#
# HERMETIC: a mktemp lane root, real `sleep` processes for liveness, and a throwaway
# `git init` repo in that same temp dir for TEST 17 (the acquire-then-`worktree add`
# ordering case, which cannot be proved without a real git). No network, no gh, no
# cargo, no dataset corpus, and nothing outside the temp dir. Runs in seconds, so it is
# wired into the gate's `tooling-tests` component.
#
# NO TEST-ONLY SEAMS (CLAUDE.md #3312: "a case needing a different enforcer
# substitutes the artifact in its own scratch copy of the tree — never a path
# variable"). There is deliberately NO /proc override and no liveness-injection env
# var in lane-lock.sh: ALIVE and DEAD-NO-PROCESS are proved with REAL processes
# (`sleep 300 &` / `kill`), and the verdicts that cannot be produced on demand
# (DEAD-PID-REUSED, DEAD-REBOOT, UNKNOWN-FOREIGN, UNKNOWN-EPHEMERAL,
# UNKNOWN-NO-PID, UNKNOWN-UNREADABLE) are produced by SUBSTITUTING THE ARTIFACT — the
# record file in this suite's own scratch lock root — which is exactly what a hand-made
# or format-drifted record looks like in the field.
#
# Run standalone:   bash scripts/tests/test_lane_lock.sh
#
# `set -e` is deliberately OFF (as in scripts/tests/test_claim_lock.sh): this suite
# asserts EXIT CODES 2 and 64 as DATA, so an aborting shell would end the run at the
# first deliberate refusal.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LL="$SCRIPT_DIR/../flow/lane-lock.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

T=$(mktemp -d "${TMPDIR:-/tmp}/lane-lock-test.XXXXXX")
LANES="$T/lanes"
LOCKS="$LANES/.lane-locks"
mkdir -p "$LANES"
SLEEPERS=""
WORKTREES=""      # scratch git worktrees to remove on exit (see TEST 17)
# cleanup KILLS but never `wait`s — see kill_sleeper below for why a `wait` on a
# signal-killed child is unsafe in a script carrying an EXIT trap. The killed children
# are reparented and reaped when this shell exits moments later.
cleanup() {
  local p w
  for p in $SLEEPERS; do kill "$p" 2>/dev/null || true; done
  for w in $WORKTREES; do rm -rf "$w" 2>/dev/null || true; done
  rm -rf "$T"
}
trap cleanup EXIT

# ll <args…> — run lane-lock.sh against the scratch lane root. Sets OUT (stdout+stderr)
# and RC (lane-lock's exit code).
OUT=""
RC=0
ll() {
  RC=0
  OUT="$(env -u LANE_LOCK_PID -u LANE_LOCK_ACTOR -u LANE_LOCK_MACHINE \
        LANE_ROOT="$LANES" bash "$LL" "$@" 2>&1)" || RC=$?
  return 0
}

# field <text> <key> — the value of a `key=value` field in a verdict line.
field() {
  printf '%s\n' "$1" | tr ' ' '\n' | grep -m1 "^$2=" | cut -d= -f2- || true
}

# The lock FILES live in a sibling LOCK ROOT, never inside the lane directory: `git
# worktree add` refuses a target that exists at all, so a lock under the lane dir makes
# acquire-before-worktree-add impossible (TEST 17 proves the order works now).
lane_of()   { printf '%s/lane-%s\n' "$LANES" "$1"; }
record_of() { printf '%s/lane-%s.lock\n' "$LOCKS" "$1"; }
log_of()    { printf '%s/lane-%s.log\n' "$LOCKS" "$1"; }

# token_of <issue> — the holder token as the PUBLIC surface reports it (probe), so the
# assertions read the same value another tool would.
token_of() {
  local o
  o="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" probe "$1" 2>/dev/null)" || true
  field "$o" holder-token
}

# lease_of <issue> — the RECORD INCARNATION, which is what `reclaim --expect` compares
# (#3436 roborev round 3). Deliberately a DIFFERENT helper from token_of: a lease built
# from the token alone is an ABA hole, since a same-process release+reacquire reproduces
# the token exactly. Assertions about ownership use token_of; assertions about a
# compare-and-swap use lease_of, and conflating them is the defect.
lease_of() {
  local o
  o="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" probe "$1" 2>/dev/null)" || true
  field "$o" lease
}

# set_rec_field <issue> <key> <value> — SUBSTITUTE THE ARTIFACT: rewrite exactly one
# line of the record. This is how the verdicts that cannot be produced on demand are
# reached; it is a scratch file in this suite's own temp dir, never a seam in the tool.
set_rec_field() {
  local rec; rec="$(record_of "$1")"
  local key="$2" val="$3" tmp="$rec.rewrite"
  awk -v k="$key" -v v="$val" -F= '
    $1 == k { printf "%s=%s\n", k, v; next }
    { print }
  ' "$rec" >"$tmp" && mv -f "$tmp" "$rec"
}
del_rec_field() {
  local rec; rec="$(record_of "$1")"
  local key="$2" tmp="$rec.rewrite"
  awk -v k="$key" -F= '$1 == k { next } { print }' "$rec" >"$tmp" && mv -f "$tmp" "$rec"
}

# sleeper — a real, long-lived process to stand in for a session. Its pid is a
# genuine live pid with genuine /proc start ticks; nothing about it is simulated.
#
# IT RETURNS THE PID IN A GLOBAL, NOT ON STDOUT, and that is the whole point. Called as
# `A=$(sleeper)`, the append to SLEEPERS happened in the command substitution's
# SUBSHELL and was discarded, so the EXIT trap saw an empty list and most `sleep 300`
# processes OUTLIVED the suite — real litter on a box running four lanes. Starting the
# child in the PARENT shell keeps `$!` and the bookkeeping in the same shell, which is
# the REPLY_* convention lane-lock.sh itself uses for exactly this reason.
#
# THE PID IS PUBLISHED THROUGH A FILE, AND THE PROCESS TOPOLOGY IS DELIBERATE. The
# `sleep` is started inside a subshell that exits immediately, so it is a GRANDCHILD of
# this shell — reparented to init, never in this shell's job table. The bookkeeping
# (`SLEEPERS`) happens in the PARENT, which is the bug being fixed: with
# `A=$(sleeper)`, the append ran in the command substitution's subshell and was
# discarded, so the EXIT trap saw an empty list and most `sleep 300` processes OUTLIVED
# the suite — real litter on a box running four lanes.
#
# MAKING THE SLEEPERS DIRECT CHILDREN INSTEAD WAS TRIED AND REVERTED, and the reason is
# worth keeping: bash runs the EXIT TRAP ONCE, MID-SCRIPT, when it reaps a CHILD that
# died from a signal, then carries on. `cleanup` therefore fired while the suite ran and
# `rm -rf`'d the scratch tree under it — measured, and INTERMITTENTLY (2 of 3 runs green,
# one with 8 cases failing on a vanished record), which is the worst kind of harness bug.
# A grandchild is never reaped by this shell, so that interaction cannot arise.
REPLY_SLEEPER=""
sleeper() {
  local pidfile="$T/sleeper.pid"
  rm -f "$pidfile"
  # stdout/stderr redirected inside the subshell: an inherited pipe held open by the
  # background child would make any caller in a command substitution block for the
  # sleep's full duration.
  ( sleep 300 >/dev/null 2>&1 & printf '%s\n' "$!" >"$pidfile" ) >/dev/null 2>&1
  REPLY_SLEEPER="$(cat "$pidfile" 2>/dev/null)"
  SLEEPERS="$SLEEPERS $REPLY_SLEEPER"
}

# kill_sleeper <pid> — end a stand-in session and CONFIRM against /proc that it is gone
# (the sleeper is not this shell's child, so `wait` cannot answer). Bounded at ~2s; the
# assertions that follow read the same /proc lane-lock.sh reads, so an unreaped straggler
# shows up as a failed liveness assertion rather than as silence.
kill_sleeper() {
  local pid="$1" i=0
  kill "$pid" 2>/dev/null || true
  while [ "$i" -lt 100 ]; do
    [ -e "/proc/$pid" ] || return 0
    i=$((i + 1))
    sleep 0.02
  done
  return 0
}

# ===========================================================================
echo "TEST 1: acquire on a FREE lane succeeds, verify then confirms it"
# POSITIVE CONTROL (AC4 non-vacuity): a refuse-everything implementation fails here.
# ===========================================================================
sleeper; A=$REPLY_SLEEPER
ll acquire 101 --pid "$A"; rc1=$RC; out1="$OUT"
ll verify 101 --pid "$A"; rc2=$RC; out2="$OUT"
if [ "$rc1" -eq 0 ] && printf '%s' "$out1" | grep -q '^LANE-LOCK: ACQUIRED issue=101 ' \
   && [ -f "$(record_of 101)" ] \
   && [ "$rc2" -eq 0 ] && printf '%s' "$out2" | grep -q 'LANE-LOCK: VERIFY-OK'; then
  ok "free lane: ACQUIRED (rc=0) + record written + VERIFY-OK (rc=0)"
else
  bad "free acquire/verify failed: rc=$rc1/$rc2
$out1
$out2"
fi

# ===========================================================================
echo "TEST 2: SAME machine, SAME actor, DIFFERENT live pid -> OCCUPIED naming the holder"
# THIS IS THE #3436 DEFECT ITSELF. claim.sh's holder identity is machine+actor, and
# two Claude sessions on one box are both machine=<host> actor=flow — so a
# machine+actor identity CANNOT refuse this case, and an implementation keyed that way
# would report a re-entrant grant here. The assertion on holder-pid (AC2) is what stops
# a refusal for the WRONG REASON from satisfying the case. NEVER relax this to
# machine+actor.
# ===========================================================================
sleeper; B=$REPLY_SLEEPER
tok_before=$(token_of 101)
ll acquire 101 --actor flow --pid "$B"; rc=$RC; out="$OUT"
tok_after=$(token_of 101)
holder_pid=$(field "$out" holder-pid)
liveness=$(field "$out" liveness)
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: OCCUPIED ' \
   && [ "$liveness" = "ALIVE" ] && [ "$holder_pid" = "$A" ] \
   && [ "$tok_after" = "$tok_before" ] && [ -n "$tok_before" ]; then
  ok "second live pid (same machine+actor) is OCCUPIED rc=2, liveness=ALIVE, holder-pid=$A, record untouched"
else
  bad "expected OCCUPIED rc=2 liveness=ALIVE holder-pid=$A record-unchanged; got rc=$rc liveness=$liveness holder-pid=$holder_pid tok '$tok_before' -> '$tok_after'
$out"
fi

# ===========================================================================
echo "TEST 3: re-entrancy requires ALL FIVE token components"
# ===========================================================================
tok_before=$(token_of 101)
ll acquire 101 --actor flow --pid "$A"; rc=$RC; out="$OUT"
tok_after=$(token_of 101)
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'ACQUIRED (re-entrant)' && [ "$tok_after" = "$tok_before" ]; then
  ok "identical machine+actor+pid+boot-id+start-ticks is ACQUIRED (re-entrant) rc=0, token unchanged"
else
  bad "expected re-entrant grant with unchanged token; got rc=$rc tok '$tok_before' -> '$tok_after'
$out"
fi

# ===========================================================================
echo "TEST 4: holder killed -> DEAD-NO-PROCESS, auto-reclaimed, and RECORDED (AC3)"
# NON-VACUITY: exit 0 is not enough — the record's token must actually have CHANGED,
# and the audit log must name the previous token, the previous liveness and the reason.
# ===========================================================================
tok_dead=$(token_of 101)
kill_sleeper "$A"
ll acquire 101 --actor flow --pid "$B"; rc=$RC; out="$OUT"
tok_new=$(token_of 101)
logline="$(grep -c 'verdict=ACQUIRED-RECLAIMED' "$(log_of 101)" 2>/dev/null || echo 0)"
logtext="$(grep 'verdict=ACQUIRED-RECLAIMED' "$(log_of 101)" 2>/dev/null || true)"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'ACQUIRED (reclaimed)' \
   && [ "$(field "$out" prev-liveness)" = "DEAD-NO-PROCESS" ] \
   && [ -n "$tok_new" ] && [ "$tok_new" != "$tok_dead" ] \
   && printf '%s' "$tok_new" | grep -q ":$B:" \
   && [ "${logline:-0}" -ge 1 ] \
   && printf '%s' "$logtext" | grep -q "prev-token=$tok_dead" \
   && printf '%s' "$logtext" | grep -q 'prev-liveness=DEAD-NO-PROCESS' \
   && printf '%s' "$logtext" | grep -q 'reason=auto-reclaim-dead-holder'; then
  ok "dead holder: ACQUIRED (reclaimed) prev-liveness=DEAD-NO-PROCESS, token replaced by ours, audit line names prev-token/prev-liveness/reason"
else
  bad "dead-holder reclaim incomplete: rc=$rc tok '$tok_dead' -> '$tok_new' log-lines=$logline
$out
$logtext"
fi

# ===========================================================================
echo "TEST 5: DEAD-PID-REUSED (record's start-ticks no longer match the live pid)"
# ===========================================================================
set_rec_field 101 start-ticks 424242
# The STALE token is the one that must be displaced. It is compared against, rather
# than the pre-substitution token, because the reclaimer here is the SAME live pid: its
# new token legitimately equals the pre-substitution value, so `tok_after != tok_before`
# would fail on a CORRECT reclaim (it did, before this was corrected).
tok_stale=$(token_of 101)
ll acquire 101 --actor flow --pid "$B"; rc=$RC; out="$OUT"
tok_after=$(token_of 101)
logtext="$(tail -1 "$(log_of 101)" 2>/dev/null || true)"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'ACQUIRED (reclaimed)' \
   && [ "$(field "$out" prev-liveness)" = "DEAD-PID-REUSED" ] \
   && [ "$(field "$out" prev-token)" = "$tok_stale" ] \
   && [ "$tok_after" != "$tok_stale" ] \
   && printf '%s' "$tok_after" | grep -q ":$B:" \
   && printf '%s' "$logtext" | grep -q 'prev-liveness=DEAD-PID-REUSED'; then
  ok "start-ticks mismatch at a LIVE pid is DEAD-PID-REUSED and auto-reclaims (stale token displaced, recorded)"
else
  bad "expected DEAD-PID-REUSED reclaim displacing '$tok_stale'; got rc=$rc prev-liveness=$(field "$out" prev-liveness) tok_after='$tok_after'
$out"
fi

# ===========================================================================
echo "TEST 6: DEAD-REBOOT (record's boot-id differs from the live boot id)"
# ===========================================================================
set_rec_field 101 boot-id 00000000-0000-0000-0000-000000000000
tok_stale=$(token_of 101)
ll acquire 101 --actor flow --pid "$B"; rc=$RC; out="$OUT"
tok_after=$(token_of 101)
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'ACQUIRED (reclaimed)' \
   && [ "$(field "$out" prev-liveness)" = "DEAD-REBOOT" ] \
   && [ "$(field "$out" prev-token)" = "$tok_stale" ] \
   && [ "$tok_after" != "$tok_stale" ]; then
  ok "foreign boot-id is DEAD-REBOOT and auto-reclaims (a reboot killed every recorded process)"
else
  bad "expected DEAD-REBOOT reclaim displacing '$tok_stale'; got rc=$rc prev-liveness=$(field "$out" prev-liveness)
$out"
fi

# ===========================================================================
echo "TEST 7: every UNKNOWN-* verdict REFUSES and leaves the record UNTOUCHED"
# The affirmative-measurement rule: only a DEAD-* verdict may reclaim. An
# implementation that treated "not provably alive" as reclaimable would pass an
# exit-code-only check on some of these, which is why each case also asserts the token
# is unchanged.
# ===========================================================================
# THE RECORD IS COMPARED BYTE FOR BYTE, not just by its token (#3436 FIX 13c). The token
# covers the five identity fields only, so a refusal that rewrote `acquired-ts`, the nonce
# or the reclaim fields would have passed — and agent-gate.sh's own comment claimed these
# cases left the record BYTE-IDENTICAL, which was true of the duplicate-key case below and
# of none of these. Cheaper to make the claim true than to weaken it.
unknown_case() {
  local label="$1" issue="$2"; shift 2
  local tok_b tok_a rc_l out_l lv rec_b rec_a
  # fresh lane per case, held by a LIVE pid, then the record is substituted
  ll acquire "$issue" --actor flow --pid "$B" >/dev/null 2>&1
  "$@"
  tok_b=$(token_of "$issue")
  rec_b=$(cat "$(record_of "$issue")" 2>/dev/null)
  ll acquire "$issue" --actor flow --pid "$C"; rc_l=$RC; out_l="$OUT"
  tok_a=$(token_of "$issue")
  rec_a=$(cat "$(record_of "$issue")" 2>/dev/null)
  lv=$(field "$out_l" liveness)
  if [ "$rc_l" -eq 2 ] && printf '%s' "$out_l" | grep -q '^LANE-LOCK: OCCUPIED ' \
     && [ "$lv" = "$label" ] && [ "$tok_a" = "$tok_b" ] \
     && [ -n "$rec_b" ] && [ "$rec_a" = "$rec_b" ]; then
    ok "$label REFUSES (OCCUPIED rc=2) and leaves the record BYTE-IDENTICAL"
  else
    bad "expected OCCUPIED rc=2 liveness=$label with a BYTE-IDENTICAL record; got rc=$rc_l liveness=$lv tok '$tok_b' -> '$tok_a'; record changed: $([ "$rec_a" = "$rec_b" ] && echo no || echo YES)
$out_l"
  fi
}
sleeper; C=$REPLY_SLEEPER
unknown_case UNKNOWN-FOREIGN     201 set_rec_field 201 machine some-other-box
unknown_case UNKNOWN-EPHEMERAL   202 set_rec_field 202 pid-scope ephemeral
unknown_case UNKNOWN-NO-PID      203 del_rec_field 203 pid
unknown_case UNKNOWN-NO-BOOT-ID  204 del_rec_field 204 boot-id
unknown_case UNKNOWN-NO-START-TICKS 205 del_rec_field 205 start-ticks

# UNKNOWN-UNREADABLE: a DUPLICATE key. Two values for one key means the record cannot
# be said to state anything, so it must fail closed rather than pick one.
ll acquire 206 --actor flow --pid "$B" >/dev/null 2>&1
printf 'machine=%s\n' duplicate-second-value >>"$(record_of 206)"
before_bytes=$(wc -c <"$(record_of 206)")
ll acquire 206 --actor flow --pid "$C"; rc=$RC; out="$OUT"
after_bytes=$(wc -c <"$(record_of 206)")
if [ "$rc" -eq 2 ] && [ "$(field "$out" liveness)" = "UNKNOWN-UNREADABLE" ] && [ "$before_bytes" = "$after_bytes" ]; then
  ok "UNKNOWN-UNREADABLE (duplicate key) REFUSES (rc=2) and does not rewrite the record"
else
  bad "expected UNKNOWN-UNREADABLE rc=2 with an unchanged record; got rc=$rc liveness=$(field "$out" liveness) bytes $before_bytes -> $after_bytes
$out"
fi

# ===========================================================================
echo "TEST 8: verify/release from a DIFFERENT pid are refused; release by the holder works"
# ===========================================================================
sleeper; D=$REPLY_SLEEPER
sleeper; E=$REPLY_SLEEPER
ll acquire 301 --actor flow --pid "$D" >/dev/null 2>&1
ll verify 301 --actor flow --pid "$E"; rcv=$RC; outv="$OUT"
tok_before=$(token_of 301)
# `release` takes no --pid (contract): the identity is the ambient session's, so a
# non-holder release is expressed by naming a different pid through LANE_LOCK_PID.
rcr=0
outr="$(env LANE_LOCK_PID="$E" LANE_ROOT="$LANES" bash "$LL" release 301 --actor flow 2>&1)" || rcr=$?
tok_after=$(token_of 301)
if [ "$rcv" -eq 2 ] && printf '%s' "$outv" | grep -q 'VERIFY-FAIL' && [ "$(field "$outv" reason)" = "not-holder" ] \
   && [ "$rcr" -eq 2 ] && printf '%s' "$outr" | grep -q 'RELEASE-REFUSED' \
   && [ "$(field "$outr" holder-pid)" = "$D" ] \
   && [ -f "$(record_of 301)" ] && [ "$tok_after" = "$tok_before" ]; then
  ok "non-holder: VERIFY-FAIL rc=2 and RELEASE-REFUSED rc=2 naming holder-pid=$D, record intact"
else
  bad "expected VERIFY-FAIL + RELEASE-REFUSED with the record intact; got rcv=$rcv rcr=$rcr tok '$tok_before' -> '$tok_after'
$outv
$outr"
fi

# `--pid` is not a release flag (the holder identity is resolved from --pid on acquire
# and from the ambient session on release), so the holder release goes through --actor
# plus the recorded pid: run it as the recorded holder by naming the same pid via env.
rc=0
outh="$(env LANE_LOCK_PID="$D" LANE_ROOT="$LANES" bash "$LL" release 301 --actor flow 2>&1)" || rc=$?
if [ "$rc" -eq 0 ] && printf '%s' "$outh" | grep -q '^LANE-LOCK: RELEASED ' && [ ! -f "$(record_of 301)" ]; then
  ok "holder release: RELEASED rc=0 and the record is gone"
else
  bad "expected holder RELEASED rc=0 with the record removed; got rc=$rc record-exists=$([ -f "$(record_of 301)" ] && echo yes || echo no)
$outh"
fi

ll release 301 --actor flow; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'RELEASED (already free)'; then
  ok "releasing a free lane is idempotent: RELEASED (already free) rc=0"
else
  bad "expected idempotent RELEASED (already free) rc=0; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 9: release --force is the reaper path (deletes a foreign holder's record)"
# ===========================================================================
ll acquire 302 --actor flow --pid "$D" >/dev/null 2>&1
ll release 302 --actor other-actor --force; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'mode=forced' && [ ! -f "$(record_of 302)" ]; then
  ok "release --force deletes a record we do not hold (mode=forced) rc=0"
else
  bad "expected forced RELEASED rc=0 with the record removed; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 10: probe is read-only — it writes NOTHING and never exits non-zero"
# probe must not even create the flock mutex: opening it for flock would WRITE to the
# lane dir, and probe is the entry point other tools (claim.sh's occupied-lane warning,
# AC5) call on lanes they do not own.
# ===========================================================================
ll probe 401; rcf=$RC; outf="$OUT"
created="$(ls -A "$(lane_of 401)" 2>/dev/null | wc -l | tr -d ' ')"
dir_exists=$([ -d "$(lane_of 401)" ] && echo yes || echo no)
rec_exists=$([ -e "$(record_of 401)" ] && echo yes || echo no)
mutex_exists=$([ -e "$LOCKS/lane-401.flock" ] && echo yes || echo no)
ll acquire 402 --actor flow --pid "$D" >/dev/null 2>&1
ll probe 402; rch=$RC; outh2="$OUT"
if [ "$rcf" -eq 0 ] && printf '%s' "$outf" | grep -q '^LANE-LOCK: FREE ' \
   && [ "$(field "$outf" liveness)" = "NO-RECORD" ] \
   && [ "$dir_exists" = "no" ] && [ "${created:-0}" = "0" ] \
   && [ "$rec_exists" = "no" ] && [ "$mutex_exists" = "no" ] \
   && [ "$rch" -eq 0 ] && printf '%s' "$outh2" | grep -q '^LANE-LOCK: HELD ' \
   && [ "$(field "$outh2" liveness)" = "ALIVE" ] \
   && [ "$(field "$outh2" holder-pid)" = "$D" ] \
   && [ -n "$(field "$outh2" acquired-ts)" ] && [ -n "$(field "$outh2" age)" ] \
   && [ "$(field "$outh2" reclaimable)" = "no" ]; then
  ok "probe: FREE rc=0 creating nothing (no lane dir, no record, no mutex); HELD rc=0 carrying liveness/holder-*/acquired-ts/age"
else
  bad "probe contract violated: rcFREE=$rcf dir-created=$dir_exists entries=$created record=$rec_exists mutex=$mutex_exists rcHELD=$rch
$outf
$outh2"
fi

# ===========================================================================
echo "TEST 10b: probe distinguishes SELF from a live LOCAL PEER"
# The two verdicts have OPPOSITE remedies — "you already occupy your own lane,
# re-acquire the claim" (the released-then-resumed state) vs "a DIFFERENT live process
# on this box owns that lane, do not touch it" — and claim.sh's #3436 AC5/AC6 reporting
# keys on the distinction. With no identity to compare against, probe reported ALIVE for
# BOTH, which is the gap this case pins. Both calls must stay exit 0 and write nothing.
# ===========================================================================
# NOT an `ls` of the lane directory: acquire no longer creates it, so that comparison
# would be "" = "" — vacuously true. The lock ROOT is where writes would land, and its
# listing is non-empty here (issue 402 is locked), so the comparison has real content.
before_entries="$(ls -A "$LOCKS" | sort | tr '\n' ',')"
ll probe 402 --actor flow --pid "$D"; rcs=$RC; outs="$OUT"
ll probe 402 --actor flow --pid "$E"; rcp=$RC; outp="$OUT"
after_entries="$(ls -A "$LOCKS" | sort | tr '\n' ',')"
if [ "$rcs" -eq 0 ] && [ "$(field "$outs" liveness)" = "SELF" ] \
   && [ "$rcp" -eq 0 ] && [ "$(field "$outp" liveness)" = "ALIVE" ] \
   && [ "$(field "$outp" holder-pid)" = "$D" ] \
   && [ "$(field "$outs" reclaimable)" = "no" ] && [ "$(field "$outp" reclaimable)" = "no" ] \
   && [ "$before_entries" = "$after_entries" ] && [ -n "$before_entries" ]; then
  ok "probe: SELF for the holder's own identity, ALIVE (holder-pid=$D) for a live peer, both rc=0, lock root unchanged"
else
  bad "probe could not distinguish SELF from a live peer: rcSELF=$rcs liveness=$(field "$outs" liveness) / rcPEER=$rcp liveness=$(field "$outp" liveness) holder-pid=$(field "$outp" holder-pid); entries '$before_entries' -> '$after_entries'
$outs
$outp"
fi

# A non-live --pid on probe must NOT refuse and must NOT change the reported liveness:
# probe is called on another tool's SUCCESS path, so a read-only report may never alter
# its caller's verdict. It simply cannot match SELF.
sleeper; dead_pid=$REPLY_SLEEPER; kill_sleeper "$dead_pid"
ll probe 402 --actor flow --pid "$dead_pid"; rcd=$RC; outd="$OUT"
if [ "$rcd" -eq 0 ] && [ "$(field "$outd" liveness)" = "ALIVE" ] && [ "$(field "$outd" holder-pid)" = "$D" ]; then
  ok "probe with a NON-LIVE --pid still reports the holder's liveness and exits 0 (never refuses)"
else
  bad "probe with a non-live --pid must stay rc=0 and report the holder; got rc=$rcd liveness=$(field "$outd" liveness)
$outd"
fi

# ===========================================================================
echo "TEST 11: reclaim compare-and-swap"
# ===========================================================================
tok=$(token_of 402)
lease402=$(lease_of 402)   # --expect compares the record INCARNATION, not the token (ABA)
ll reclaim 402 --expect "$lease402" --reason lane-holder-oom-killed-verified-by-dmesg --actor flow --pid "$E"; rc=$RC; out="$OUT"
tok_new=$(token_of 402)
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIMED ' \
   && [ "$tok_new" != "$tok" ] && printf '%s' "$tok_new" | grep -q ":$E:" \
   && grep -q "reclaim-reason=lane-holder-oom-killed-verified-by-dmesg" "$(record_of 402)" \
   && grep -q "reclaimed-from=$tok" "$(record_of 402)" \
   && grep -q 'verdict=RECLAIMED' "$(log_of 402)"; then
  ok "matched --expect: RECLAIMED rc=0, token replaced, record carries reclaimed-from + reclaim-reason, audit line written"
else
  bad "expected a satisfied CAS to RECLAIM; got rc=$rc tok '$tok' -> '$tok_new'
$out"
fi

tok=$(token_of 402)
lease402b=$(lease_of 402)   # `actual=` reports the LEASE (record incarnation), not the token
ll reclaim 402 --expect "not-the-current-token" --reason stale-lease-check --actor flow --pid "$D"; rc=$RC; out="$OUT"
tok_after=$(token_of 402)
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIM-LOST ' \
   && [ "$(field "$out" expected)" = "not-the-current-token" ] \
   && [ "$(field "$out" actual)" = "$lease402b" ] && [ "$tok_after" = "$tok" ]; then
  ok "violated --expect: RECLAIM-LOST rc=2 naming expected= and actual=, record unchanged"
else
  bad "expected RECLAIM-LOST rc=2 with both values named; got rc=$rc tok '$tok' -> '$tok_after'
$out"
fi

ll reclaim 402 --expect none --reason lane-should-be-free-after-finalize --actor flow --pid "$D"; rc=$RC; out="$OUT"
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q 'RECLAIM-LOST' && [ "$(field "$out" expected)" = "none" ]; then
  ok "--expect none over an EXISTING record: RECLAIM-LOST rc=2"
else
  bad "expected RECLAIM-LOST for --expect none over an existing record; got rc=$rc
$out"
fi

ll reclaim 403 --expect none --reason resume-after-supervisor-crash --actor flow --pid "$D"; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIMED ' && [ -f "$(record_of 403)" ]; then
  ok "--expect none over a FREE lane: RECLAIMED rc=0"
else
  bad "expected RECLAIMED for --expect none over a free lane; got rc=$rc
$out"
fi

# RE-ENTRANT RECLAIM with a VIOLATED lease must name BOTH values — a failed CAS is
# never reported as a satisfied one (mirrors `claim.sh adopt`).
tok=$(token_of 403)
lease403=$(lease_of 403)
ll reclaim 403 --expect none --reason re-entrant-retry-after-confirm-blip --actor flow --pid "$D"; rc=$RC; out="$OUT"
# THE VERDICT WORD ITSELF MUST DIFFER (#3436 FIX 13i): both this and a satisfied CAS exit
# 0, so a consumer matching on the first token (or on the exit code) could not otherwise see
# that its --expect did NOT hold. The exit code stays 0 — we demonstrably hold the lane.
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIM-LEASE-MISMATCH ' \
   && printf '%s' "$out" | grep -q 'expected=none' \
   && printf '%s' "$out" | grep -q "actual=$lease403" \
   && ! printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIMED'; then
  ok "re-entrant reclaim with a violated lease: its own verdict word RECLAIM-LEASE-MISMATCH (never RECLAIMED), rc=0, naming BOTH expected=none and actual=<our token>"
else
  bad "expected the distinct RECLAIM-LEASE-MISMATCH verdict naming both values; got rc=$rc
$out"
fi

# RE-OBSERVE the lease: the preceding reclaim rewrote the record, so the earlier lease is
# now legitimately stale. That is the ABA fix working as intended — a lease names ONE
# acquisition, so any write invalidates it, and a test that reused the old value would be
# asserting the hole rather than the fix.
lease403b=$(lease_of 403)
ll reclaim 403 --expect "$lease403b" --reason re-entrant-retry-lease-held --actor flow --pid "$D"; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIMED (re-entrant)' \
   && ! printf '%s' "$out" | grep -qi 'lease-mismatch'; then
  ok "re-entrant reclaim whose lease DID hold: plain RECLAIMED (re-entrant), no phantom mismatch"
else
  bad "expected a plain re-entrant verdict when --expect matches; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 12: every --reason rejection shape is exit 64 and changes NO record"
# ===========================================================================
tok=$(token_of 403)
reason_case() {
  local label="$1"; shift
  local rc_l out_l tok_a
  rc_l=0
  out_l="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" reclaim 403 --expect "$lease403" "$@" --actor flow 2>&1)" || rc_l=$?
  tok_a=$(token_of 403)
  if [ "$rc_l" -eq 64 ] && [ "$tok_a" = "$tok" ] && ! printf '%s' "$out_l" | grep -q 'LANE-LOCK:'; then
    ok "--reason $label -> exit 64 (usage error on stderr, no LANE-LOCK: line), record unchanged"
  else
    bad "--reason $label expected exit 64 with an unchanged record and no verdict line; got rc=$rc_l tok '$tok' -> '$tok_a'
$out_l"
  fi
}
reason_case "(omitted)"
reason_case "''"                --reason ''
reason_case "'   ' (whitespace)" --reason '   '
reason_case "'---'"             --reason '---'
reason_case "'why' (placeholder)" --reason 'why'
reason_case "'TODO' (placeholder, case-insensitive)" --reason 'TODO'
reason_case "unsubstituted '<branch>' template" --reason 'resume-lane:<branch>'
reason_case "bare '<why>' template" --reason '<why>'

# ===========================================================================
echo "TEST 13: usage errors — --expect '', a missing --expect, a RELATIVE --lane-dir"
# A relative lane dir resolves against each caller's cwd, so two callers would lock two
# different directories while believing they shared one: exactly the class of bug this
# lock exists to prevent (CLAUDE.md's CQLITE_SCHEMAS_ROOT precedent).
# ===========================================================================
usage_case() {
  local label="$1"; shift
  local rc_l out_l
  rc_l=0
  out_l="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" "$@" 2>&1)" || rc_l=$?
  if [ "$rc_l" -eq 64 ] && ! printf '%s' "$out_l" | grep -q 'LANE-LOCK:'; then
    ok "$label -> exit 64, no LANE-LOCK: verdict line"
  else
    bad "$label expected exit 64 with no verdict line; got rc=$rc_l
$out_l"
  fi
}
usage_case "reclaim --expect ''"          reclaim 404 --expect '' --reason a-real-reason-here
usage_case "reclaim without --expect"     reclaim 404 --reason a-real-reason-here
usage_case "acquire --lane-dir relative"  acquire 404 --lane-dir "relative/lane"
usage_case "verify --lane-dir relative"   verify 404 --lane-dir "./lane"
usage_case "probe --lane-dir relative"    probe 404 --lane-dir "lane"
usage_case "acquire non-numeric issue"    acquire not-a-number
usage_case "acquire unknown flag"         acquire 404 --nope
usage_case "unknown subcommand"           frobnicate 404
usage_case "no subcommand"
usage_case "acquire --actor '' (unrecordable holder identity)" acquire 404 --actor ''
usage_case "acquire --pid non-numeric"    acquire 404 --pid abc

# An ABSOLUTE --lane-dir is the accepted form — the control that stops "refuse every
# --lane-dir" from satisfying the three relative cases above.
ll acquire 405 --lane-dir "$T/explicit-lane" --actor flow --pid "$D"; rc=$RC; out="$OUT"
# The record is keyed by ISSUE in the lock root; --lane-dir names the SUBJECT lane and is
# recorded as a field. It must NOT create the lane directory (TEST 17).
if [ "$rc" -eq 0 ] && [ -f "$(record_of 405)" ] \
   && grep -q "^lane-dir=$T/explicit-lane$" "$(record_of 405)" \
   && [ ! -e "$T/explicit-lane" ] \
   && printf '%s' "$out" | grep -q "lane-dir=$T/explicit-lane"; then
  ok "an ABSOLUTE --lane-dir is honoured and RECORDED, and the lane directory is NOT created (control for the relative-path refusals)"
else
  bad "expected an absolute --lane-dir to be honoured; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 14: concurrency — 8 simultaneous acquires, EXACTLY ONE wins"
# This is the flock property, and a test that never races proves nothing about it: all
# 8 pids are distinct LIVE processes, so no acquire can win by re-entrancy, and the
# lane dir is fresh so none can win by reclaim.
# ===========================================================================
CONC_LANE="$T/conc-lane"
pids=""
i=0
while [ "$i" -lt 8 ]; do
  sleeper; p=$REPLY_SLEEPER
  pids="$pids $p"
  i=$((i + 1))
done
racers=""
for p in $pids; do
  ( env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" acquire 500 --lane-dir "$CONC_LANE" --actor flow --pid "$p" >"$T/conc.$p.out" 2>&1; echo "$?" >"$T/conc.$p.rc" ) &
  racers="$racers $!"
done
# Wait on the RACERS BY PID, never a bare `wait`: the sleepers that stand in for live
# sessions are children of this shell too, so a bare `wait` would block for their full
# 300s and hang the suite.
for p in $racers; do wait "$p" 2>/dev/null || true; done
acq=0; occ=0; other=0
for p in $pids; do
  if grep -q '^LANE-LOCK: ACQUIRED issue=500 ' "$T/conc.$p.out" 2>/dev/null; then acq=$((acq + 1))
  elif grep -q '^LANE-LOCK: OCCUPIED ' "$T/conc.$p.out" 2>/dev/null; then occ=$((occ + 1))
  else other=$((other + 1)); fi
done
winner_tok=$(field "$(tr '\n' ' ' <"$(record_of 500)" 2>/dev/null)" pid)
if [ "$acq" -eq 1 ] && [ "$occ" -eq 7 ] && [ "$other" -eq 0 ] && [ -n "$winner_tok" ]; then
  ok "8 concurrent acquires: exactly 1 ACQUIRED, 7 OCCUPIED, 0 other outcomes (winner pid=$winner_tok)"
else
  bad "expected 1 ACQUIRED / 7 OCCUPIED / 0 other; got acquired=$acq occupied=$occ other=$other"
fi

# ===========================================================================
echo "TEST 15: status renders single-lane and enumerated views"
# ===========================================================================
# ASSERT `HELD`, NOT `HELD|FREE` (#3436 FIX 13e). Lane 402 provably holds a record at this
# point, so the alternation passed even if `status` reported a held lane as FREE — the one
# failure the case exists to catch. Same for the lock COUNT: `-ge 1` is satisfied by any
# non-empty enumeration, so it is pinned to the number of records actually on disk.
ll status 402; rc1=$RC; out1="$OUT"
ll status; rc2=$RC; out2="$OUT"
locks=$(field "$out2" locks)
records_on_disk=$(ls -1 "$LOCKS"/lane-*.lock 2>/dev/null | wc -l | tr -d ' ')
if [ "$rc1" -eq 0 ] && printf '%s' "$out1" | grep -q '^LANE-LOCK: HELD issue=402 ' \
   && [ "$(field "$out1" holder-pid)" = "$E" ] \
   && [ "$rc2" -eq 0 ] && printf '%s' "$out2" | grep -q '^LANE-LOCK: STATUS ' \
   && [ -n "$locks" ] && [ "$locks" = "$records_on_disk" ] && [ "$locks" -ge 1 ]; then
  ok "status <N> renders lane 402 as HELD naming its holder; bare status enumerates EXACTLY the $records_on_disk records on disk (rc=0)"
else
  bad "status render failed: rc=$rc1/$rc2 locks='$locks' records-on-disk='$records_on_disk' holder-pid='$(field "$out1" holder-pid)' (expected $E)
$out1
$out2"
fi

# ===========================================================================
echo "TEST 16: --help exits 0 and documents every subcommand"
# Guards the header against drifting out of the file: print_help renders the header
# comment, so a subcommand added without documenting it fails here.
# ===========================================================================
# RETRY THE CAPTURE, NOT THE ASSERTION (#3436). This case failed twice at load ~102 on a
# 16-core box with three peer gates running, and passed 3/3 at load ~87 and 3/3 in isolation
# — the signature of a fork failure making the command substitution yield an empty or
# truncated capture, not of a header regression. Retrying the READ is legitimate because the
# property under test is "the header documents every subcommand", and `--help` is pure and
# read-only; retrying does not weaken that property. Retrying the ASSERTION would.
# Bounded at 3, and a persistent failure still lands in `bad` with the byte/line diagnostic,
# so a real regression cannot be retried into a pass.
rc=1; out=""
for _try in 1 2 3; do
  ll --help; rc=$RC; out="$OUT"
  [ "$rc" -eq 0 ] && [ "${#out}" -gt 1000 ] && break
done
# PIPE-FREE MATCHING, because the pipeline itself was a suspect. This case flaked ~1 in 3
# FULL-SUITE runs at ANY load (recurred at load 1.6, which falsified the earlier
# load-contention explanation), with the diagnostic showing rc=0, nothing undocumented, 16
# `3436` hits, 392 lines, 26796 bytes — i.e. EVERY named clause satisfied at diagnostic time.
# The only shape consistent with that is a transient failure of an assertion-time pipeline
# whose re-run at diagnostic time succeeds. The candidate mechanism is `grep -q` exiting on
# first match while `printf` is still writing 26KB, taking SIGPIPE, and `set -o pipefail`
# (line 42) reporting the pipeline as failed even though grep matched.
#
# THAT MECHANISM IS INFERRED, NOT PROVEN: 300 standalone iterations of the exact pipeline
# produced 0 failures, so I cannot claim it. What is certain is that `case` removes the
# pipeline, the subprocess and the race BY CONSTRUCTION, costs nothing, and is faster. So the
# fragility is fixed without asserting a cause I could not reproduce.
missing=""
nl='
'
for sub in acquire verify probe release reclaim status; do
  case "$out" in
    "  $sub "*|*"$nl  $sub "*) : ;;
    *) missing="$missing $sub" ;;
  esac
done
cites_issue=no
case "$out" in *3436*) cites_issue=yes ;; esac
if [ "$rc" -eq 0 ] && [ -z "$missing" ] && [ "$cites_issue" = yes ]; then
  ok "--help exits 0 and documents acquire/verify/probe/release/reclaim/status (and cites #3436)"
else
  # DIAGNOSE WHICH CLAUSE FAILED. This case failed twice with `rc=0 undocumented:<none>`,
  # i.e. every named clause satisfied — which told us nothing and cost two investigations.
  # An assertion that cannot say why it failed is a bad assertion.
  bad "--help incomplete: rc=$rc undocumented:${missing:-<none>} cites-3436=$cites_issue out-bytes=${#out}"
fi

# ===========================================================================
echo "TEST 17: acquire PRECEDES 'git worktree add' — the order AC1 requires"
# THE POINT OF THE LOCK LIVING OUTSIDE THE LANE DIRECTORY. AC1 is "detects an existing
# occupant BEFORE writing", so the lock must be takeable before the worktree exists —
# and `git worktree add` REFUSES a target that exists at all, a single dotfile being
# enough, creating the branch before it fails and leaving a stray branch behind:
#
#   $ mkdir -p lane-777 && touch lane-777/.lane-lock
#   $ git worktree add lane-777 -b tmp origin/main
#   Preparing worktree (new branch 'tmp')
#   fatal: '.../lane-777' already exists
#
# So this case asserts the whole sequence a real lane entry performs: acquire, THEN
# `git worktree add` at the very path the lock names, and the lock is still ours after.
# It also pins the second property the move buys — `git worktree remove` cannot destroy
# a live lock, because the record was never inside the worktree.
# git is required rather than skipped, exactly as scripts/tests/test_claim_lock.sh
# requires it: this repository IS a git checkout, so a git-less host cannot run its
# tooling at all, and a SKIP here would hide the one case that justifies the layout.
# ===========================================================================
if ! command -v git >/dev/null 2>&1; then
  bad "git is not on PATH, so the acquire-then-worktree-add ordering case cannot run (deliberately NOT a skip — it is the case that justifies the lock root layout)"
else
  WT_REPO="$T/wt-repo"
  WT_LANE="$LANES/lane-777"
  gwt() { git -C "$WT_REPO" -c user.email=t@example.invalid -c user.name=t "$@"; }
  mkdir -p "$WT_REPO"
  ( cd "$WT_REPO" && git init -q . && git -c user.email=t@example.invalid -c user.name=t commit -q --allow-empty -m base ) >/dev/null 2>&1
  sleeper; W=$REPLY_SLEEPER
  ll acquire 777 --lane-dir "$WT_LANE" --actor flow --pid "$W"; rc17=$RC; out17="$OUT"
  lane_absent=$([ -e "$WT_LANE" ] && echo no || echo yes)
  wt_rc=0
  gwt worktree add "$WT_LANE" -b issue-777-slug HEAD >"$T/wt17.out" 2>&1 || wt_rc=$?
  WORKTREES="$WORKTREES $WT_LANE"
  ll verify 777 --lane-dir "$WT_LANE" --actor flow --pid "$W"; rc17v=$RC
  # ...and removing the worktree leaves the lock intact (it was never inside it).
  gwt worktree remove --force "$WT_LANE" >/dev/null 2>&1
  gwt branch -q -D issue-777-slug >/dev/null 2>&1
  rec_survived=$([ -f "$(record_of 777)" ] && echo yes || echo no)
  stray_branch=$(gwt branch --list issue-777-slug 2>/dev/null | wc -l | tr -d ' ')
  if [ "$rc17" -eq 0 ] && [ "$lane_absent" = "yes" ] && [ "$wt_rc" -eq 0 ] \
     && [ "$rc17v" -eq 0 ] && [ "$rec_survived" = "yes" ] && [ "${stray_branch:-0}" = "0" ]; then
    ok "acquire 777 leaves the lane dir ABSENT, so 'git worktree add' at that path then SUCCEEDS (rc=0), the lock still verifies, and 'worktree remove' does not destroy the record"
  else
    bad "acquire-then-worktree-add ordering broken: acquire-rc=$rc17 lane-dir-absent-after-acquire=$lane_absent worktree-add-rc=$wt_rc verify-rc=$rc17v record-survived-removal=$rec_survived stray-branches=$stray_branch
$out17
$(cat "$T/wt17.out" 2>/dev/null)"
  fi
fi

# ===========================================================================
echo "TEST 18: an acquire that cannot name a durable owner REFUSES and writes nothing;"
echo "         one that can is RE-ENTRANT for the SAME session from a NEW shell"
# ===========================================================================
# THIS IS THE CASE WHOSE ABSENCE LET THE LANE BE BRICKED ON FIRST USE. The wiring called
# `acquire <N> --lane-dir "$(cd "$wt" && pwd)"` from OUTSIDE the lane: `$(cd … && pwd)`
# only computes a path, so the acquire's own cwd was the root checkout, no ancestor's cwd
# was inside the lane, the recorded pid-scope was `ephemeral` and the record read
# UNKNOWN-EPHEMERAL — which REFUSES, including for the owning session's own later
# acquire. Case (b) is the real-world sequence (flow-implement, then flow-address later)
# and it FAILED before FIX 5.
#
# NO TEST-ONLY SEAM: (a) and (b) differ ONLY in this shell's cwd, which is the very thing
# the resolution rule reads. No LANE_LOCK_PID, no --pid, no /proc override.

# (a) from OUTSIDE the lane: a named refusal, exit 1, and NOTHING created.
ll acquire 889; rc18a=$RC; out18a=$OUT
a_rec=$([ -e "$(record_of 889)" ] && echo yes || echo no)
a_mut=$([ -e "$LOCKS/lane-889.flock" ] && echo yes || echo no)
a_log=$([ -e "$(log_of 889)" ] && echo yes || echo no)
if [ "$rc18a" -eq 1 ] && printf '%s' "$out18a" | grep -q '^LANE-LOCK: ERROR reason=unresolved-identity ' \
   && [ "$a_rec" = no ] && [ "$a_mut" = no ] && [ "$a_log" = no ] \
   && printf '%s' "$out18a" | grep -q -- '--pid' \
   && printf '%s' "$out18a" | grep -q 'cd '; then
  ok "acquire with no durable owner: ERROR reason=unresolved-identity rc=1, NOTHING written (no record/mutex/log), and BOTH corrections printed"
else
  bad "expected reason=unresolved-identity rc=1 writing nothing; got rc=$rc18a record=$a_rec mutex=$a_mut log=$a_log
$out18a"
fi

# (b) THE INTEGRATION CASE. This shell cd's INTO the lane, so the outermost ancestor whose
# cwd is inside it is THIS suite's shell — a durable process that outlives each acquire.
# The first acquire's own shell EXITS (it is a command substitution), and the second
# acquire comes from a brand-new shell in the same session: it must be RE-ENTRANT.
LANE_890="$LANES/lane-890"
mkdir -p "$LANE_890"
ORIG_CWD="$PWD"
cd "$LANE_890" || bad "could not cd into the scratch lane dir"
ll acquire 890; rc18b=$RC; out18b=$OUT
ll acquire 890; rc18c=$RC; out18c=$OUT
ll verify  890; rc18d=$RC
cd "$ORIG_CWD" || true
if [ "$rc18b" -eq 0 ] && printf '%s' "$out18b" | grep -q '^LANE-LOCK: ACQUIRED issue=890 ' \
   && [ "$(field "$out18b" pid-scope)" = "cwd-match" ] \
   && [ "$(field "$out18b" pid)" = "$$" ] \
   && [ "$rc18c" -eq 0 ] && printf '%s' "$out18c" | grep -q 'ACQUIRED (re-entrant)' \
   && [ "$rc18d" -eq 0 ]; then
  ok "acquire from the SESSION's own cwd records $$ (pid-scope=cwd-match, a DURABLE process because this suite is that process); a SECOND acquire from a new shell is ACQUIRED (re-entrant) and verify still passes -- a transient holder would fail BOTH halves, which is what keeps the wiring honest (#3436 FIX 14)"
else
  bad "the in-lane acquire/re-acquire sequence is broken: rc=$rc18b/$rc18c verify=$rc18d pid-scope=$(field "$out18b" pid-scope) pid=$(field "$out18b" pid) (expected $$, cwd-match)
$out18b
$out18c"
fi

# ===========================================================================
echo "TEST 19: the RECORD's lane-dir is authoritative for every reader"
# ===========================================================================
# The record is found by ISSUE, so no reader needs a lane path — and re-deriving
# ${LANE_ROOT}/lane-<N> made `probe` describe a directory nobody was working in, because
# this repo's sanctioned worktrees are `.claude/worktrees/issue-<N>-<slug>` and
# `~/projects/cqlite-wt/issue-<N>`. So: lock a lane OUTSIDE the lane root, then probe and
# verify with NO --lane-dir at all.
OUT_LANE="$T/out-of-tree/worktrees/issue-891-slug"
mkdir -p "$OUT_LANE"
sleeper; W6=$REPLY_SLEEPER
ll acquire 891 --lane-dir "$OUT_LANE" --actor flow --pid "$W6"; rc19a=$RC
ll probe 891; rc19b=$RC; out19b=$OUT
ll verify 891 --pid "$W6"; rc19c=$RC
if [ "$rc19a" -eq 0 ] && [ "$rc19b" -eq 0 ] \
   && [ "$(field "$out19b" lane-dir)" = "$OUT_LANE" ] \
   && [ "$(field "$out19b" liveness)" = "ALIVE" ] \
   && [ "$rc19c" -eq 0 ]; then
  ok "a lane locked OUTSIDE the lane root is reported at its RECORDED path by a probe with no --lane-dir, and verify with no --lane-dir still passes"
else
  bad "the recorded lane-dir is not authoritative: acquire=$rc19a probe=$rc19b verify=$rc19c lane-dir='$(field "$out19b" lane-dir)' (expected $OUT_LANE)
$out19b"
fi

# ...and a caller-supplied --lane-dir that DISAGREES is reported as information, never
# silently preferred, and must not change the liveness verdict.
ll probe 891 --lane-dir "$LANES/lane-891" --actor flow --pid "$W6"; rc19d=$RC; out19d=$OUT
# The identity passed here is the HOLDER's, so the verdict is SELF — and it must STAY
# SELF with a disagreeing --lane-dir. That is the sharp form of "a mismatch is
# information, not a verdict change": a wrong lane path must not demote a session's own
# lock to a peer's.
if [ "$rc19d" -eq 0 ] \
   && [ "$(field "$out19d" lane-dir)" = "$OUT_LANE" ] \
   && [ "$(field "$out19d" lane-dir-mismatch)" = "$LANES/lane-891" ] \
   && [ "$(field "$out19d" liveness)" = "SELF" ]; then
  ok "a disagreeing --lane-dir is reported as lane-dir-mismatch=, the RECORDED path still wins, and liveness stays SELF (a mismatch is information, not a verdict change)"
else
  bad "expected lane-dir=$OUT_LANE + lane-dir-mismatch=$LANES/lane-891 + liveness=SELF; got lane-dir='$(field "$out19d" lane-dir)' mismatch='$(field "$out19d" lane-dir-mismatch)' liveness='$(field "$out19d" liveness)'
$out19d"
fi

# our-identity= (#3436 FIX 7a): the READ side must SAY whether it established its own
# identity, because a consumer cannot tell SELF from a live PEER without it. Two calls,
# same record: one with a live --pid (established), one from outside the lane with no pid
# at all (unresolvable). Both stay exit 0 — a report never refuses.
ll probe 891 --actor flow --pid "$W6"; out19e=$OUT
ll probe 891; out19f=$OUT
# AND THE PAIR IS THE POINT: the SAME record reads SELF when our identity was
# established and ALIVE when it could not be. Without `our-identity=` those two ALIVEs
# are indistinguishable, which is how a session got told its own lane belonged to a peer.
if [ "$(field "$out19e" our-identity)" = "explicit" ] \
   && [ "$(field "$out19f" our-identity)" = "UNRESOLVED" ] \
   && [ "$(field "$out19e" liveness)" = "SELF" ] && [ "$(field "$out19f" liveness)" = "ALIVE" ]; then
  ok "probe declares our-identity=explicit vs UNRESOLVED on the SAME record, and the unresolved read is ALIVE where the resolved one is SELF — the exact ambiguity a consumer must not guess at"
else
  bad "our-identity= is not three-valued as documented: with-pid='$(field "$out19e" our-identity)' without='$(field "$out19f" our-identity)'
$out19e
$out19f"
fi

# ===========================================================================
echo
# ===========================================================================
echo "TEST 20: release uses the RECORD's lane-dir (roborev round 2), and a relative LANE_ROOT is refused"
# ===========================================================================
# (a) A lock taken for a NON-DEFAULT lane dir must be releasable WITHOUT repeating
# --lane-dir. release used to re-derive ${LANE_ROOT}/lane-<N> while verify/probe read the
# record, so a non-default worktree's lock could not be released through the normal path —
# and once that worktree was REMOVED the cwd identity walk had nothing to match, leaving a
# live stale lock. The record is keyed by ISSUE, so its own lane-dir is always available.
ODD_LANE="$LANES/not-a-lane-path/wt-950"
mkdir -p "$ODD_LANE"
ORIG20="$PWD"
cd "$ODD_LANE" || bad "could not cd into the odd lane dir"
ll acquire 950 --lane-dir "$ODD_LANE"; rc20a=$RC
HOLDER950=$(grep '^pid=' "$LANES/.lane-locks/lane-950.lock" 2>/dev/null | cut -d= -f2)
# (a1) from INSIDE the lane, with NO --lane-dir: the record supplies the subject.
ll release 950; rc20b=$RC; out20b=$OUT
cd "$ORIG20" || true
if [ "$rc20a" -eq 0 ] && [ "$rc20b" -eq 0 ] \
   && printf '%s' "$out20b" | grep -q '^LANE-LOCK: RELEASED issue=950 ' \
   && printf '%s' "$out20b" | grep -q "lane-dir=$ODD_LANE" \
   && [ ! -f "$LANES/.lane-locks/lane-950.lock" ]; then
  ok "(a1) release with NO --lane-dir releases a lock taken for a non-default path, reporting the RECORDED lane-dir"
else
  bad "(a1) expected RELEASED naming lane-dir=$ODD_LANE and the record gone; got rc=$rc20a/$rc20b
$out20b"
fi

# (a2) THE HALF READING THE RECORD CANNOT FIX. The holder gate is our exact token and the
# auto-resolved pid comes from the CWD walk, so a holder that has moved cwd — or whose lane
# directory has been REMOVED, which is what finalize does — cannot resolve the identity it
# locked with, and its own release is refused as not-holder. --pid is the remedy.
mkdir -p "$ODD_LANE"
cd "$ODD_LANE" || bad "could not re-enter the odd lane dir"
ll acquire 950 --lane-dir "$ODD_LANE" >/dev/null 2>&1
HOLDER950=$(grep '^pid=' "$LANES/.lane-locks/lane-950.lock" 2>/dev/null | cut -d= -f2)
cd "$ORIG20" || true
rm -rf "$ODD_LANE"                     # the lane is gone, as after `worktree remove`
ll release 950; rc20e=$RC; out20e=$OUT                       # no identity available
ll release 950 --pid "$HOLDER950"; rc20f=$RC; out20f=$OUT     # named explicitly
if [ "$rc20e" -eq 2 ] && printf '%s' "$out20e" | grep -q 'RELEASE-REFUSED' \
   && [ "$rc20f" -eq 0 ] && printf '%s' "$out20f" | grep -q '^LANE-LOCK: RELEASED issue=950 ' \
   && [ ! -f "$LANES/.lane-locks/lane-950.lock" ]; then
  ok "(a3) with the lane dir REMOVED, an identity-less release is REFUSED (not silently successful) and --pid $HOLDER950 releases it"
else
  bad "(a3) expected RELEASE-REFUSED then RELEASED via --pid; got rc=$rc20e/$rc20f
$out20e
$out20f"
fi

# (b) A RELATIVE LANE_ROOT is a lock BYPASS, not a style issue: it resolves against each
# caller's cwd, so two sessions naming the SAME absolute lane directory compute DIFFERENT
# lock roots and BOTH acquire. It must fail at the entry point, in the main shell.
rc20c=0; out20c="$( cd /tmp && LANE_ROOT=relative/lanes bash "$LL" probe 1 2>&1 )" || rc20c=$?
rc20d=0; ( cd /tmp && LANE_ROOT=rel bash "$LL" --help >/dev/null 2>&1 ) || rc20d=$?
if [ "$rc20c" -eq 64 ] && printf '%s' "$out20c" | grep -q 'LANE_ROOT must be an ABSOLUTE path' \
   && ! printf '%s' "$out20c" | grep -q '^LANE-LOCK:' \
   && [ "$rc20d" -eq 0 ]; then
  ok "(b) a RELATIVE LANE_ROOT is exit 64 with no LANE-LOCK: verdict line; --help still works on a misconfigured box"
else
  bad "(b) expected exit 64 for a relative LANE_ROOT and 0 for --help; got rc=$rc20c/$rc20d
$out20c"
fi

# ===========================================================================
echo "TEST 21: the reclaim lease is a record INCARNATION (ABA), and the issue key is canonical"
# ===========================================================================
# (a) ABA. machine:actor:pid:boot:ticks is UNCHANGED when the same process releases and
# re-acquires, so a lease built from the TOKEN matched a record written AFTER that cycle
# and overwrote a NEWLY ACQUIRED LIVE lock — the CAS guarantee inverted, two writers.
# claim.sh has no such hole because git arbitrates on a per-claim commit sha; the local
# equivalent is the per-write nonce, which was recorded from the start and never reached
# the lease. The stale lease must LOSE and the current one must still WIN — the second
# half is what stops a refuse-everything fix passing.
sleep 300 & ABA_P=$!
sleep 300 & ABA_Q=$!
LANE_961="$LANES/lane-961"; mkdir -p "$LANE_961"
ll acquire 961 --pid "$ABA_P" --lane-dir "$LANE_961" >/dev/null 2>&1
ll probe 961; LEASE1=$(printf '%s' "$OUT" | grep -oE 'lease=[^ ]+' | cut -d= -f2-)
ll release 961 --pid "$ABA_P" >/dev/null 2>&1
ll acquire 961 --pid "$ABA_P" --lane-dir "$LANE_961" >/dev/null 2>&1
ll probe 961; LEASE2=$(printf '%s' "$OUT" | grep -oE 'lease=[^ ]+' | cut -d= -f2-)
TOK1="${LEASE1%%#*}"; TOK2="${LEASE2%%#*}"
ll reclaim 961 --expect "$LEASE1" --reason aba-stale-lease --pid "$ABA_Q"; rc21a=$RC; out21a=$OUT
ll reclaim 961 --expect "$LEASE2" --reason aba-current-lease --pid "$ABA_Q"; rc21b=$RC; out21b=$OUT
if [ -n "$LEASE1" ] && [ -n "$LEASE2" ] && [ "$LEASE1" != "$LEASE2" ] \
   && [ "$TOK1" = "$TOK2" ] \
   && [ "$rc21a" -eq 2 ] && printf '%s' "$out21a" | grep -q '^LANE-LOCK: RECLAIM-LOST' \
   && [ "$rc21b" -eq 0 ] && printf '%s' "$out21b" | grep -q '^LANE-LOCK: RECLAIMED'; then
  ok "(a) a lease observed BEFORE a same-process release+reacquire LOSES (RECLAIM-LOST) while the CURRENT lease still wins — and the token halves are IDENTICAL, so only the nonce closed the ABA hole"
else
  bad "(a) ABA: expected stale=RECLAIM-LOST(2) current=RECLAIMED(0) with identical token halves; got rc=$rc21a/$rc21b tok1=$TOK1 tok2=$TOK2
$out21a
$out21b"
fi
kill "$ABA_P" "$ABA_Q" 2>/dev/null || true

# (b) A NONCANONICAL ISSUE KEY IS AN ALIAS, AND AN ALIAS IS TWO LOCKS FOR ONE LANE.
# Every path is derived from the issue string RAW, so `3436` and `03436` produced
# different mutexes and BOTH acquired. Rejected rather than normalised: an issue number
# never legitimately carries a leading zero, so rewriting it would hide a caller bug and
# would have to be repeated at every derivation site.
ll acquire 03436 --lane-dir "$LANE_961"; rc21c=$RC; out21c=$OUT
ll probe 0;    rc21d=$RC
ll probe 3436; rc21e=$RC
if [ "$rc21c" -eq 64 ] && printf '%s' "$out21c" | grep -q 'leading zero' \
   && printf '%s' "$out21c" | grep -q "canonical form '3436'" \
   && ! printf '%s' "$out21c" | grep -q '^LANE-LOCK:' \
   && [ "$rc21d" -eq 64 ] && [ "$rc21e" -eq 0 ]; then
  ok "(b) a leading-zero issue key is exit 64 naming the canonical form (no LANE-LOCK: verdict), issue 0 is refused, and the canonical key still works"
else
  bad "(b) expected 64/64/0 for 03436 / 0 / 3436; got $rc21c/$rc21d/$rc21e
$out21c"
fi

# ===========================================================================
echo "TEST 22: a DEGRADED identity is refused like an ephemeral one (roborev round 4)"
# ===========================================================================
# The rule is "never WRITE a record that cannot be re-identified", and the guard checked
# only pid-scope=ephemeral. A record whose boot-id or start-ticks could not be captured is
# the same thing: UNKNOWN-* forever, refusing every later acquire including its own
# holder's, clearable only by --force/reclaim. An earlier round ACCEPTED this as a
# residual, which contradicted the principle adopted one fix later.
#
# Reachable deterministically with an explicit --pid whose /proc entry vanishes between
# argument validation and identity capture. Simulated the only way that does not need a
# test-only seam: a pid that is live at validation and gone at capture is a race we cannot
# schedule, so instead assert the GUARD's own contract directly — a pid with no readable
# start-ticks must not produce a record. `--pid 1` is live and readable, so it is the
# control; a KERNEL thread pid is live with a readable stat, so also unsuitable. What we
# CAN do without a seam: prove the guard is not a refuse-everything guard (a live explicit
# --pid still captures both fields and writes a healthy record), and prove the EPHEMERAL
# refusal writes nothing and names its own cause. The DEGRADED branch itself is covered by
# INSPECTION ONLY and this suite says so rather than implying otherwise — an unreadable
# boot-id or start-ticks for a LIVE pid cannot be produced on a /proc host without a
# test-only seam, which doctrine forbids (#3312: substitute the artifact, never add a path
# variable). Tracked with the other inspection-only verdicts in the #3436 follow-up.
LANE_970="$LANES/lane-970"; mkdir -p "$LANE_970"
sleep 300 & DG=$!
ll acquire 970 --pid "$DG" --lane-dir "$LANE_970"; rc22a=$RC
REC970="$LANES/.lane-locks/lane-970.lock"
if [ "$rc22a" -eq 0 ] && [ -f "$REC970" ] \
   && grep -q '^boot-id=..*' "$REC970" && grep -qE '^start-ticks=[0-9]+' "$REC970"; then
  ok "(a) control: a live explicit --pid DOES capture both boot-id and start-ticks, so the refusal below is not a refuse-everything guard"
else
  bad "(a) expected a healthy record with boot-id and start-ticks for a live --pid; got rc=$rc22a
$(cat "$REC970" 2>/dev/null)"
fi
kill "$DG" 2>/dev/null || true

# (b) The DEGRADED refusal must be textually distinct from the EPHEMERAL one, so a reader
# is sent to the right remedy: "run from inside the lane / pass --pid" is useless advice
# when the problem is that /proc could not be read at all.
ll acquire 971 --lane-dir "$LANES/lane-971"; rc22b=$RC; out22b=$OUT
if [ "$rc22b" -eq 1 ] && printf '%s' "$out22b" | grep -q 'reason=unresolved-identity' \
   && printf '%s' "$out22b" | grep -q 'detail=no-durable-session-process' \
   && printf '%s' "$out22b" | grep -q 'NOTHING WAS WRITTEN' \
   && [ ! -f "$LANES/.lane-locks/lane-971.lock" ]; then
  ok "(b) the EPHEMERAL refusal names its own cause (detail=no-durable-session-process), writes nothing, and exits 1. NOT ASSERTED HERE: the DEGRADED branch (detail=degraded-process-identity) — it needs an unreadable boot-id or start-ticks for a live pid, which is not producible on a /proc host without a test-only seam, so it is covered by inspection only, like UNKNOWN-STATE and UNKNOWN-NO-PROC"
else
  bad "(b) expected reason=unresolved-identity detail=no-durable-session-process exit 1 with no record; got rc=$rc22b
$out22b"
fi

# ===========================================================================
echo "TEST 23: release --force needs NO identity (roborev round 5)"
# ===========================================================================
# --force is the documented break-glass for a stale lock -- it is the answer to "how does a
# stale instance get cleared, and by whom?". prepare_identity RESOLVES AND VALIDATES the
# actor/pid, and an explicit-or-INHERITED LANE_LOCK_PID naming a process that is gone is a
# usage error (exit 64), so a stale env var defeated the one path documented as
# unconditional -- making the documentation FALSE, which is worse than the refusal.
LANE_980="$LANES/lane-980"; mkdir -p "$LANE_980"
sleep 300 & DEADP=$!
ll acquire 980 --pid "$DEADP" --lane-dir "$LANE_980" >/dev/null 2>&1
kill "$DEADP" 2>/dev/null; wait "$DEADP" 2>/dev/null
# the holder is now DEAD and LANE_LOCK_PID in the environment still names it
rc23a=0; out23a="$(LANE_ROOT="$LANES" LANE_LOCK_PID="$DEADP" bash "$LL" release 980 --force 2>&1)" || rc23a=$?
if [ "$rc23a" -eq 0 ] && printf '%s' "$out23a" | grep -q '^LANE-LOCK: RELEASED issue=980 ' \
   && [ ! -f "$LANES/.lane-locks/lane-980.lock" ]; then
  ok "(a) release --force succeeds with a DEAD inherited LANE_LOCK_PID and removes the record — the break-glass needs no identity"
fi

# (a2) THE SIBLING THE FIRST FIX MISSED (roborev round 6). Bypassing prepare_identity was
# not enough: the bypass itself still called `resolve_actor`, which VALIDATES and dies (64)
# on an unrecordable actor -- so an invalid INHERITED LANE_LOCK_ACTOR disabled the
# break-glass exactly as an inherited LANE_LOCK_PID had one round earlier. Fourth instance
# in this change of "fix the named site, miss its sibling", and here the sibling was two
# lines inside the fix. So this drives BOTH hostile env vars, separately and together.
for _bad in "LANE_LOCK_ACTOR=**" "LANE_LOCK_PID=$DEADP" "LANE_LOCK_ACTOR=** LANE_LOCK_PID=$DEADP"; do
  LANE_982="$LANES/lane-982"; mkdir -p "$LANE_982"
  sleep 300 & _h=$!
  ( cd "$LANE_982" && env -u LANE_LOCK_ACTOR LANE_ROOT="$LANES" bash "$LL" acquire 982 --pid "$_h" >/dev/null 2>&1 )
  kill "$_h" 2>/dev/null; wait "$_h" 2>/dev/null
  _rc=0; _out="$(env $_bad LANE_ROOT="$LANES" bash "$LL" release 982 --force 2>&1)" || _rc=$?
  if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q '^LANE-LOCK: RELEASED issue=982 ' \
     && [ ! -f "$LANES/.lane-locks/lane-982.lock" ]; then
    ok "(a2) release --force survives a hostile inherited environment [$_bad] — no identity is resolved on the forced path, so nothing on it can refuse"
  else
    bad "(a2) --force refused under [$_bad]: rc=$_rc
$_out"
  fi
done
if true; then :
else
  bad "(a) expected --force to release regardless of a dead inherited pid; got rc=$rc23a
$out23a"
fi

# (b) CONTROL: without --force the same call must still REFUSE, so (a) is not passing
# because identity checking was removed everywhere.
sleep 300 & LIVEP=$!
ll acquire 981 --pid "$LIVEP" --lane-dir "$LANES/lane-981" >/dev/null 2>&1
rc23b=0; out23b="$(LANE_ROOT="$LANES" LANE_LOCK_PID="$$" bash "$LL" release 981 2>&1)" || rc23b=$?
if [ "$rc23b" -eq 2 ] && printf '%s' "$out23b" | grep -q 'RELEASE-REFUSED' \
   && [ -f "$LANES/.lane-locks/lane-981.lock" ]; then
  ok "(b) control: WITHOUT --force a non-holder release is still RELEASE-REFUSED and the record survives"
else
  bad "(b) expected RELEASE-REFUSED without --force; got rc=$rc23b
$out23b"
fi
kill "$LIVEP" 2>/dev/null || true

# ===========================================================================
echo "TEST 24: the SCOPE LIMIT is in the EMITTED line, not only the header"
# ===========================================================================
# `locks=0` reads as "no lanes occupied". It MEANS "no lane RECORDED a lock" — this tool can
# only see occupants that CALLED acquire. That limit was documented in the header, which is
# where a caveat-hunter looks and NOT where the person who needs it looks. Same shape a peer
# found in a census line reading only "not compared": disclosed in the SOURCE is not disclosed
# in the ARTIFACT. So the test asserts what a reader SEES, and it asserts the empty case
# explicitly, because that is the one that reads as an all-clear.
# A FRESH lock root: by this point the suite has created many locks, so the shared root is
# non-empty and would test the wrong precondition (my first attempt at this case did exactly
# that and failed against a 20-lock render).
EMPTY_ROOT="$T/empty-root"; mkdir -p "$EMPTY_ROOT"
OUT="$(env -u LANE_LOCK_PID LANE_ROOT="$EMPTY_ROOT" bash "$LL" status 2>&1)"
if printf '%s' "$OUT" | grep -q 'locks=0' \
   && printf '%s' "$OUT" | grep -q 'scope=lock-takers-only' \
   && printf '%s' "$OUT" | grep -qi 'does NOT.*mean no lane is occupied' \
   && printf '%s' "$OUT" | grep -qi 'not a clean bill of health'; then
  ok "(a) an EMPTY status render states that 0 means no lane RECORDED a lock, not that no lane is occupied, and refuses to read as a clean bill of health"
else
  bad "(a) the empty status render does not carry its own scope limit:
$OUT"
fi

# (b) the NON-EMPTY render carries it too — a count of recorded locks is still not a census of
# occupied lanes, and someone reading locks=3 needs that as much as someone reading locks=0.
sleep 300 & SCOPE_H=$!
mkdir -p "$LANES/lane-906"
( cd "$LANES/lane-906" && env LANE_ROOT="$LANES" bash "$LL" acquire 906 --pid "$SCOPE_H" >/dev/null 2>&1 )
ll status
if printf '%s' "$OUT" | grep -qE 'locks=[1-9]' \
   && printf '%s' "$OUT" | grep -q 'scope=lock-takers-only' \
   && printf '%s' "$OUT" | grep -qi 'not occupied lanes'; then
  ok "(b) a NON-EMPTY status render also states it counts RECORDED locks rather than occupied lanes"
else
  bad "(b) the non-empty status render lost its scope limit:
$OUT"
fi
kill "$SCOPE_H" 2>/dev/null || true

# ===========================================================================
echo "TEST 25: identity compares the FULL boot id, and a recorded lane-dir is canonical"
# ===========================================================================
# (a) `record_is_self` compared the 8-char DISPLAY prefix of a 128-bit boot id that is
# RECORDED IN FULL. A colliding prefix plus a reused pid and start-ticks would grant
# re-entrancy to a stale PRE-REBOOT record — and SELF is tested BEFORE the DEAD-REBOOT
# branch, so the false SELF would win. Comparing a truncated proxy when the exact value is
# in hand, inside the identity code written to be exact.
sleep 300 & BP_H=$!
LANE_970b="$LANES/lane-970b"; mkdir -p "$LANE_970b"
ll acquire 970 --lane-dir "$LANE_970b" --pid "$BP_H" >/dev/null 2>&1
REC970="$LANES/.lane-locks/lane-970.lock"
real_boot=$(grep '^boot-id=' "$REC970" | cut -d= -f2)
forged_boot="${real_boot:0:8}$(printf '%s' "${real_boot:8}" | tr 'abcdef0123456789' 'bcdefa1234567890')"
set_rec_field 970 boot-id "$forged_boot"
ll probe 970 --pid "$BP_H"
if [ "${real_boot:0:8}" = "${forged_boot:0:8}" ] && [ "$real_boot" != "$forged_boot" ] \
   && printf '%s' "$OUT" | grep -q 'liveness=DEAD-REBOOT' \
   && ! printf '%s' "$OUT" | grep -q 'liveness=SELF'; then
  ok "(a) a boot id sharing the 8-char DISPLAY prefix but differing in full is DEAD-REBOOT, never a false SELF"
else
  bad "(a) prefix-colliding boot id was not rejected: same-prefix=$([ "${real_boot:0:8}" = "${forged_boot:0:8}" ] && echo y || echo n)
$OUT"
fi
kill "$BP_H" 2>/dev/null || true

# (b) A lane-dir recorded BEFORE the directory exists must still be comparable to a future
# /proc/<pid>/cwd, which is always canonical. Recorded raw, `lane-971/` and `y/../lane-971`
# never matched the holder's own cwd: probe said ALIVE instead of SELF, claim.sh read its OWN
# lock as a live peer, and `release` was refused TO THE HOLDER. Asserted through the
# user-visible consequence (release without --force), not just the recorded string.
for shape in "trailing-slash" "dotdot"; do
  sleep 300 & CN_H=$!
  case "$shape" in
    trailing-slash)want="$LANES/lane-972"; give="$LANES/lane-972/" ;;
    dotdot)         want="$LANES/lane-973"; give="$LANES/z/../lane-973" ;;
  esac
  iss=$(basename "$want" | tr -dc '0-9')
  ll acquire "$iss" --lane-dir "$give" --pid "$CN_H" >/dev/null 2>&1
  recorded=$(grep '^lane-dir=' "$LANES/.lane-locks/lane-$iss.lock" 2>/dev/null | cut -d= -f2-)
  mkdir -p "$want"
  ll probe "$iss" --pid "$CN_H"; probe_out="$OUT"
  ll release "$iss" --pid "$CN_H"; rel_rc=$RC; rel_out="$OUT"
  if [ "$recorded" = "$want" ] \
     && printf '%s' "$probe_out" | grep -q 'liveness=SELF' \
     && [ "$rel_rc" -eq 0 ] && printf '%s' "$rel_out" | grep -q '^LANE-LOCK: RELEASED' ; then
    ok "(b/$shape) a non-canonical acquisition path is recorded CANONICAL ($recorded), the holder is SELF, and release succeeds WITHOUT --force"
  else
    bad "(b/$shape) recorded='$recorded' want='$want' rel_rc=$rel_rc
$probe_out
$rel_out"
  fi
  kill "$CN_H" 2>/dev/null || true
done

# ===========================================================================
echo "TEST 26: the DIRECTORY is what must not have two writers (roborev round 8)"
# ===========================================================================
# A REGRESSION INTRODUCED BY FIX 2. The record is keyed by ISSUE so readers need no path; the
# mutex was keyed by issue too, so two DIFFERENT issues naming the SAME directory took
# DIFFERENT mutexes and BOTH acquired — the core collision protection bypassed. Before FIX 2
# the record lived AT `<lane-dir>/.lane-lock`, so the DIRECTORY was the key; moving the lock
# out (necessary: `git worktree add` refuses any existing target) re-keyed it. acquire now
# holds a DIRECTORY-keyed mutex as well, and refuses a live holder under another issue.
SHARED="$LANES/shared-826"; mkdir -p "$SHARED"
sleep 300 & SH_A=$!
sleep 300 & SH_B=$!
ll acquire 826 --lane-dir "$SHARED" --pid "$SH_A"; rc26a=$RC
ll acquire 827 --lane-dir "$SHARED" --pid "$SH_B"; rc26b=$RC; out26b=$OUT
if [ "$rc26a" -eq 0 ] && [ "$rc26b" -eq 2 ] \
   && printf '%s' "$out26b" | grep -q 'reason=same-lane-dir-other-issue' \
   && printf '%s' "$out26b" | grep -q 'conflicting-issue=826'; then
  ok "(a) a SECOND issue naming the SAME lane directory is OCCUPIED and NAMES the conflicting issue — one directory, one writer"
else
  bad "(a) expected the second issue to be refused; got rc=$rc26a/$rc26b
$out26b"
fi

# CONTROLS. A guard that never permits work is broken, not fail-closed — so all three of these
# must still ACQUIRE, and each rules out a different way of over-refusing.
ll acquire 826 --lane-dir "$SHARED" --pid "$SH_A"
if [ "$RC" -eq 0 ] && printf '%s' "$OUT" | grep -q 're-entrant'; then
  ok "(b) control: the SAME issue re-acquiring its own directory is re-entrant, not blocked by itself"
else bad "(b) same-issue re-acquire was refused: $OUT"; fi

OTHERD="$LANES/other-828"; mkdir -p "$OTHERD"
ll acquire 828 --lane-dir "$OTHERD" --pid "$SH_B"
if [ "$RC" -eq 0 ]; then
  ok "(c) control: a different issue on a DIFFERENT directory still acquires — the guard couples directories, not issues"
else bad "(c) a different issue on a different dir was refused: $OUT"; fi

kill "$SH_A" 2>/dev/null; wait "$SH_A" 2>/dev/null
sleep 300 & SH_C=$!
ll acquire 829 --lane-dir "$SHARED" --pid "$SH_C"
if [ "$RC" -eq 0 ]; then
  ok "(d) control: once the holder is DEAD, another issue CAN take that directory — a dead holder does not block forever"
else bad "(d) a dead holder blocked another issue: $OUT"; fi
kill "$SH_B" "$SH_C" 2>/dev/null || true

# (e) GLOB CHARACTERS in a not-yet-existing lane path must be taken LITERALLY. `for seg in $in`
# with IFS=/ is subject to pathname expansion, so `.../dec*y-x` expanded against the CURRENT
# directory and was recorded as an unrelated path — which then never matches the holder's cwd,
# the very identity breakage path normalisation was added to fix (now _resolve_lane_path,
# which superseded the lexical normaliser in round 16 — see TEST 36).
GLOBD="$LANES/dec*y-830"
: > "$LANES/decoy-830"
sleep 300 & GL_H=$!
ll acquire 830 --lane-dir "$GLOBD" --pid "$GL_H"
rec830=$(grep '^lane-dir=' "$LANES/.lane-locks/lane-830.lock" 2>/dev/null | cut -d= -f2-)
if [ "$rec830" = "$GLOBD" ]; then
  ok "(e) a lane path containing a glob character is recorded LITERALLY ($(basename "$rec830")), not expanded against cwd"
else
  bad "(e) glob expanded: recorded '$rec830' want '$GLOBD'"
fi
kill "$GL_H" 2>/dev/null || true

# ===========================================================================
echo "TEST 27: the directory guard must hold under CONCURRENCY, not just in sequence (roborev round 9)"
# ===========================================================================
# TEST 26 ASSERTED THIS PROPERTY SEQUENTIALLY AND PASSED WHILE THE LOCK HELD NOTHING.
# `with_lock` hardcoded fd 9 and the acquire path NESTS it (directory mutex outside, per-issue
# mutex inside), so the inner `exec 9>` CLOSED the outer descriptor and released the directory
# lock before the record write. Measured on the pre-fix code with 4 racers on one directory:
# ALL FOUR acquired — the guard was not merely leaky, it was inert. A sequential test cannot
# see this because the window only exists while another process is between the scan and the
# write, so this test must RACE or it re-certifies the same hole.
CONC27="$LANES/conc27-lane"; mkdir -p "$CONC27"
c27_pids=""; i=0
while [ "$i" -lt 4 ]; do sleeper; c27_pids="$c27_pids $REPLY_SLEEPER"; i=$((i + 1)); done
c27_racers=""; c27_iss=940
for p in $c27_pids; do
  c27_iss=$((c27_iss + 1))
  ( env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" acquire "$c27_iss" --lane-dir "$CONC27" --actor flow --pid "$p" >"$T/c27.$p.out" 2>&1 ) &
  c27_racers="$c27_racers $!"
done
for p in $c27_racers; do wait "$p" 2>/dev/null || true; done
c27_acq=0; c27_occ=0
for p in $c27_pids; do
  if grep -q '^LANE-LOCK: ACQUIRED issue=' "$T/c27.$p.out" 2>/dev/null; then c27_acq=$((c27_acq + 1))
  elif grep -q '^LANE-LOCK: OCCUPIED ' "$T/c27.$p.out" 2>/dev/null; then c27_occ=$((c27_occ + 1)); fi
done
if [ "$c27_acq" -eq 1 ] && [ "$c27_occ" -eq 3 ]; then
  ok "(a) 4 concurrent acquires under FOUR DIFFERENT issues on ONE directory: exactly 1 ACQUIRED, 3 OCCUPIED"
else
  bad "(a) one directory got $c27_acq simultaneous writers (occupied=$c27_occ); the directory mutex is not held across the record write"
fi
for p in $c27_pids; do kill "$p" 2>/dev/null || true; done

# ===========================================================================
echo "TEST 28: reclaim answers the directory question too (roborev round 9)"
# ===========================================================================
# reclaim MINTS a record, and it took only the per-issue mutex — so a stale record for issue A
# could be reclaimed AFTER issue B legitimately took that directory, producing two live holders
# with no race at all. Any path that writes a record must answer the same question acquire does.
SH28="$LANES/shared-28"; mkdir -p "$SH28"
sleep 300 & S28_A=$!
ll acquire 928 --lane-dir "$SH28" --pid "$S28_A"; rc28a=$RC
# the CAS value is the LEASE (<token>#<nonce>), not the holder token — a token alone is the
# ABA hole TEST 21 pins, and there is deliberately no token= field in the record.
lease28=$(lease_of 928)
# a SECOND issue is refused from acquire (TEST 26) — and must be refused from reclaim as well
sleep 300 & S28_B=$!
ll reclaim 929 --lane-dir "$SH28" --pid "$S28_B" --expect none --reason round9-directory-conflict-probe
rc28b=$RC; out28b=$OUT
if [ "$rc28a" -eq 0 ] && [ "$rc28b" -eq 2 ] && printf '%s' "$out28b" | grep -q 'reason=same-lane-dir-other-issue'; then
  ok "(a) reclaim under a DIFFERENT issue naming a live-held directory is OCCUPIED, not granted"
else
  bad "(a) reclaim minted a second writer for one directory: rc=$rc28a/$rc28b
$out28b"
fi
# CONTROL: a guard that never permits work is broken. Reclaiming the OWN issue still works.
ll reclaim 928 --lane-dir "$SH28" --pid "$S28_A" --expect "$lease28" --reason round9-control-self-reclaim
if [ "$RC" -eq 0 ]; then
  ok "(b) CONTROL: reclaiming the issue that owns the directory still succeeds"
else
  bad "(b) CONTROL over-refused a legitimate self-reclaim: rc=$RC
$OUT"
fi
kill "$S28_A" "$S28_B" 2>/dev/null || true

# ===========================================================================
echo "TEST 29: a re-entrant acquire must name the RECORDED directory (roborev round 9)"
# ===========================================================================
# Re-entrancy compared the five identity components and never the DIRECTORY, so one process
# holding issue N for X received ACQUIRED (re-entrant) for Y while the record still protected
# X — Y advertised as locked and not locked. Same alias the directory mutex closes for two
# issues, reopened for one issue naming two directories.
X29="$LANES/x29"; Y29="$LANES/y29"; mkdir -p "$X29" "$Y29"
sleep 300 & P29=$!
ll acquire 929 --lane-dir "$X29" --pid "$P29"; rc29a=$RC
ll acquire 929 --lane-dir "$Y29" --pid "$P29"; rc29b=$RC; out29b=$OUT
ll acquire 929 --lane-dir "$X29" --pid "$P29"; rc29c=$RC; out29c=$OUT
if [ "$rc29a" -eq 0 ] && [ "$rc29b" -eq 2 ] \
   && printf '%s' "$out29b" | grep -q 'reason=reentrant-lane-dir-mismatch' \
   && printf '%s' "$out29b" | grep -q 'recorded-lane-dir=' ; then
  ok "(a) a re-entrant acquire naming a DIFFERENT directory is refused and names the recorded one"
else
  bad "(a) expected reentrant-lane-dir-mismatch; got rc=$rc29a/$rc29b
$out29b"
fi
if [ "$rc29c" -eq 0 ] && printf '%s' "$out29c" | grep -q 'ACQUIRED (re-entrant)'; then
  ok "(b) CONTROL: the same process re-acquiring the SAME directory is still re-entrant"
else
  bad "(b) CONTROL over-refused a legitimate re-entrant acquire: rc=$rc29c
$out29c"
fi
kill "$P29" 2>/dev/null || true

# ===========================================================================
echo "TEST 30: reclaim's re-entrant path must match the directory TOO (roborev round 10)"
# ===========================================================================
# ROUND 9 ADDED THIS CHECK TO ACQUIRE AND LEFT ITS SIBLING IN RECLAIM — the reported line was
# patched and the class was not swept, which is why this test exists as a pair with TEST 29
# rather than as a variant of it. Without it a holder reclaims its OWN lease naming --lane-dir Y
# while the record still protects X: success is reported for Y, nothing is rewritten, and another
# issue can then acquire Y against a lock that never covered it.
X30="$LANES/x30"; Y30="$LANES/y30"; mkdir -p "$X30" "$Y30"
sleep 300 & P30=$!
ll acquire 930 --lane-dir "$X30" --pid "$P30"; rc30a=$RC
lease30=$(lease_of 930)
ll reclaim 930 --lane-dir "$Y30" --pid "$P30" --expect "$lease30" --reason round10-wrong-directory-probe
rc30b=$RC; out30b=$OUT
if [ "$rc30a" -eq 0 ] && [ "$rc30b" -eq 2 ] \
   && printf '%s' "$out30b" | grep -q 'reason=reentrant-lane-dir-mismatch' \
   && printf '%s' "$out30b" | grep -q 'recorded-lane-dir='; then
  ok "(a) a re-entrant RECLAIM naming a DIFFERENT directory is refused and names the recorded one"
else
  bad "(a) reclaim reported success for a directory its record does not protect: rc=$rc30a/$rc30b
$out30b"
fi
# CONTROL: the same holder reclaiming the RECORDED directory still succeeds.
ll reclaim 930 --lane-dir "$X30" --pid "$P30" --expect "$lease30" --reason round10-control-correct-directory
if [ "$RC" -eq 0 ] && printf '%s' "$OUT" | grep -q 'RECLAIMED (re-entrant)'; then
  ok "(b) CONTROL: reclaiming under the RECORDED directory is still re-entrant success"
else
  bad "(b) CONTROL over-refused a legitimate re-entrant reclaim: rc=$RC
$OUT"
fi
kill "$P30" 2>/dev/null || true

# ===========================================================================
echo "TEST 31: a symlinked ANCESTOR must not create a second directory mutex (roborev round 10)"
# ===========================================================================
# The lane dir does not exist yet at acquire time (acquire-before-`git worktree add`), and the
# not-yet-exists branch normalised only LEXICALLY — so `/real/lane` and `/alias/lane`, where
# `/alias` -> `/real`, produced different strings, different directory mutexes, and the
# cross-issue scan never compared them. Two issues acquired what is one directory. The leaf still
# cannot be resolved physically; its ANCESTORS can, and now are.
mkdir -p "$LANES/realp31"
ln -sfn "$LANES/realp31" "$LANES/aliasp31"
sleep 300 & A31=$!
sleep 300 & B31=$!
ll acquire 931 --lane-dir "$LANES/realp31/lane31" --pid "$A31"; rc31a=$RC
ll acquire 932 --lane-dir "$LANES/aliasp31/lane31" --pid "$B31"; rc31b=$RC; out31b=$OUT
rec31=$(grep '^lane-dir=' "$(record_of 931)" 2>/dev/null | cut -d= -f2-)
if [ "$rc31a" -eq 0 ] && [ "$rc31b" -eq 2 ] && printf '%s' "$out31b" | grep -q 'reason=same-lane-dir-other-issue'; then
  ok "(a) two spellings of one directory (via a symlinked parent) contend: the second is OCCUPIED"
else
  bad "(a) the alias evaded the directory lock: rc=$rc31a/$rc31b recorded='$rec31'
$out31b"
fi
case "$rec31" in
  "$LANES/realp31/lane31") ok "(b) the RECORDED path is the physical one, so a later reader comparing /proc/<pid>/cwd matches" ;;
  *) bad "(b) recorded '$rec31', want the physically-resolved '$LANES/realp31/lane31'" ;;
esac
kill "$A31" "$B31" 2>/dev/null || true

# ===========================================================================
echo "TEST 32: siblings under one in-lane parent — the DOCUMENTED limit, and its mitigation (roborev round 13)"
# ===========================================================================
# THIS TEST PINS A LIMITATION, NOT A GUARANTEE, AND THAT IS DELIBERATE. Two workers spawned by
# the SAME in-lane parent both auto-resolve to that parent, share a token, and the second is
# granted re-entrant — the same shape as the defect this file fixes, one level up, and inherent
# to an ancestry-derived identity because siblings share every ancestor. The obvious guard was
# measured and rejected (it refuses an ordinary single session, and a refusing acquire BRICKS
# the lane, FIX 5/14). The contract is `--pid`. If someone later closes this properly, (a)
# SHOULD fail — that is the signal to update the header's scope limit, not to relax the test.
#
# THE EXPOSURE IS NARROWER THAN IT FIRST LOOKS, and the first draft of this test found that by
# accident: written with two DIFFERENT issues on one directory it FAILED, because the directory
# mutex plus same_dir_other_issue refused the second outright. So siblings on different issues
# are already covered by the DIRECTORY guard; the hole is only two processes on the SAME issue
# in the SAME lane, which is why the test now uses one issue for both.
SIB="$LANES/sib32"; mkdir -p "$SIB"
# a durable parent whose cwd IS the lane, spawning two acquires as its children
env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash -c '
  cd "$1" || exit 1
  bash "$2" acquire 932 --lane-dir "$1" --actor flow >"$3/sib.a" 2>&1
  bash "$2" acquire 932 --lane-dir "$1" --actor flow >"$3/sib.b" 2>&1
  sleep 300
' _ "$SIB" "$LL" "$T" &
SIB_PARENT=$!
# wait for both children to have written their verdicts (bounded, no bare sleep race)
i=0; while [ "$i" -lt 60 ] && { [ ! -s "$T/sib.a" ] || [ ! -s "$T/sib.b" ]; }; do sleep 1; i=$((i+1)); done
sib_a="$(cat "$T/sib.a" 2>/dev/null)"; sib_b="$(cat "$T/sib.b" 2>/dev/null)"
sib_a_pid=$(printf '%s' "$sib_a" | sed -n 's/.* pid=\([0-9]*\).*/\1/p' | head -1)
sib_b_pid=$(printf '%s' "$sib_b" | sed -n 's/.* pid=\([0-9]*\).*/\1/p' | head -1)
if printf '%s' "$sib_a" | grep -q 'ACQUIRED issue=932' && printf '%s' "$sib_b" | grep -q 'ACQUIRED (re-entrant)' \
   && [ -n "$sib_a_pid" ] && [ "$sib_a_pid" = "$sib_b_pid" ] \
   && printf '%s' "$sib_a" | grep -q 'pid-scope=cwd-match'; then
  ok "(a) DOCUMENTED LIMIT: two SEPARATE processes under one in-lane parent, SAME issue, resolve to the SAME pid ($sib_a_pid); the second is granted (re-entrant) though it is a different process. Record discloses pid-scope=cwd-match. Mitigation is --pid (b)."
else
  bad "(a) expected both to resolve to one shared ancestor pid under auto-resolution (this pins the header's scope limit; if it is now FIXED, update the header rather than the test)
a=$sib_a
b=$sib_b"
fi
kill "$SIB_PARENT" 2>/dev/null || true
# (b) THE MITIGATION: with an explicit --pid each worker has its own identity, so the DIRECTORY
#     guard sees two distinct holders and refuses the second — one directory, one writer.
SIB2="$LANES/sib32b"; mkdir -p "$SIB2"
sleep 300 & SB_A=$!
sleep 300 & SB_B=$!
ll acquire 934 --lane-dir "$SIB2" --pid "$SB_A"; rc32a=$RC
ll acquire 935 --lane-dir "$SIB2" --pid "$SB_B"; rc32b=$RC; out32b=$OUT
if [ "$rc32a" -eq 0 ] && [ "$rc32b" -eq 2 ] && printf '%s' "$out32b" | grep -q 'reason=same-lane-dir-other-issue'; then
  ok "(b) MITIGATION: with explicit --pid the two workers are distinct identities and the second is refused — the contract in the header works"
else
  bad "(b) --pid did not separate the workers: rc=$rc32a/$rc32b
$out32b"
fi
kill "$SB_A" "$SB_B" 2>/dev/null || true

# ===========================================================================
echo "TEST 33: the directory mutex filename is bounded (roborev round 13)"
# ===========================================================================
# Flattening the whole canonical path into one component can exceed NAME_MAX (255), failing as
# `mutex-unopenable` on a VALID path. Short paths keep the readable form.
DEEP="$LANES/$(printf 'd%.0s' $(seq 1 60))/$(printf 'e%.0s' $(seq 1 60))/$(printf 'f%.0s' $(seq 1 60))/$(printf 'g%.0s' $(seq 1 60))/lane33"
mkdir -p "$DEEP" 2>/dev/null
sleep 300 & DP=$!
ll acquire 936 --lane-dir "$DEEP" --pid "$DP"; rc33=$RC; out33=$OUT
longest=$(ls "$LOCKS" 2>/dev/null | awk '{ if (length($0) > m) m = length($0) } END { print m+0 }')
if [ "$rc33" -eq 0 ] && [ "$longest" -le 255 ]; then
  ok "(a) a deeply nested lane acquires, and no lock filename exceeds NAME_MAX (longest=$longest)"
else
  bad "(a) deep lane path: rc=$rc33 longest-lock-filename=$longest
$out33"
fi
kill "$DP" 2>/dev/null || true

# ===========================================================================
echo "TEST 34: reclaim asks the RECORD for the lane, like every other reader (roborev round 14)"
# ===========================================================================
# FIX 6 made every READER honour the record's lane-dir and round 11 rebuilt verify/probe around
# a single snapshot; release already did. reclaim was the one path still deriving the DEFAULT
# `$LANE_ROOT/lane-<N>`, so a lock taken under a sanctioned CUSTOM worktree could not be
# reclaimed: the identity walk ran against a directory nobody was in. That is the break-glass
# failing on exactly the lanes that need it — a custom-path lane whose holder is gone.
CUSTOM34="$LANES/custom-wt/issue-934-slug"; mkdir -p "$CUSTOM34"
sleep 300 & P34=$!
ll acquire 934 --lane-dir "$CUSTOM34" --pid "$P34"; rc34a=$RC
lease34=$(lease_of 934)
# reclaim with NO --lane-dir: the record must supply the custom path
ll reclaim 934 --pid "$P34" --expect "$lease34" --reason round14-record-supplies-the-lane
rc34b=$RC; out34b=$OUT
rec34=$(grep '^lane-dir=' "$(record_of 934)" 2>/dev/null | cut -d= -f2-)
if [ "$rc34a" -eq 0 ] && [ "$rc34b" -eq 0 ] && [ "$rec34" = "$CUSTOM34" ]; then
  ok "(a) reclaim with no --lane-dir resolves against the RECORDED custom path and succeeds"
else
  bad "(a) reclaim derived the default instead of reading the record: rc=$rc34a/$rc34b recorded='$rec34' want='$CUSTOM34'
$out34b"
fi
# CONTROL: an explicit --lane-dir that DISAGREES is still the caller's business and must not be
# silently overridden by the record — the record supplies the lane only when the caller did not.
ll reclaim 934 --lane-dir "$LANES/somewhere-else" --pid "$P34" --expect "$lease34" --reason round14-control-explicit-wins
if [ "$RC" -ne 0 ]; then
  ok "(b) CONTROL: an explicit --lane-dir is still honoured (and here refused, since it is not the recorded lane)"
else
  bad "(b) CONTROL: an explicit --lane-dir was silently replaced by the record: rc=$RC
$OUT"
fi
kill "$P34" 2>/dev/null || true

# ===========================================================================
echo "TEST 35: finalize RELEASES BEFORE it deletes the lane — asserted on the ORDER, not the prose"
# ===========================================================================
# ROUND 12 WROTE "RELEASE BEFORE THE WORKTREE IS REMOVED" INTO A COMMENT AND LEFT THE STEPS IN THE
# OLD ORDER. Round 15 found it again, ten lines below that very sentence — and the step TITLE had
# said "release ... then remove" the whole time. Three artifacts asserting the correct order while
# the sequence did the opposite, for three rounds. Prose cannot pin a sequence; a line number can.
#
# WHY THE ORDER IS LOAD-BEARING: `lane-lock.sh release` proves the caller is the holder by walking
# up from cwd INSIDE the lane. finalize-cleanup deletes the worktree. Release afterwards is refused
# as not-holder, and the record outlives the lane — the next session for that issue meets a lock
# nobody can clear.
FIN_SKILL="$SCRIPT_DIR/../../.claude/skills/flow-finalize/SKILL.md"
if [ -r "$FIN_SKILL" ]; then
  ln_release=$(grep -n 'lane-lock\.sh release <N>' "$FIN_SKILL" | head -1 | cut -d: -f1)
  ln_cleanup=$(grep -n 'finalize-cleanup\.sh --issue' "$FIN_SKILL" | head -1 | cut -d: -f1)
  if [ -n "$ln_release" ] && [ -n "$ln_cleanup" ] && [ "$ln_release" -lt "$ln_cleanup" ]; then
    ok "(a) flow-finalize releases the lane lock (line $ln_release) BEFORE finalize-cleanup removes the worktree (line $ln_cleanup)"
  else
    bad "(a) finalize would release a lock after deleting the lane that proves who holds it: release=$ln_release cleanup=$ln_cleanup (release must come FIRST)"
  fi
  # AND THE DOCUMENTED CALLER MUST USE THE ABA PROTECTION IT WAS GIVEN (#3436, roborev round 24).
  # `release --expect` was added in round 21 and the finalize path still called `release <N>`
  # bare, so a delayed or duplicated finalize could still delete a NEWER lock held by the same
  # process. An option nobody passes fixes nothing in the flow that actually runs — the same
  # shape as a fix that is only a message. Asserted on the OBSERVABLE text of the step.
  if grep -q 'release <N> --expect' "$FIN_SKILL"; then
    ok "(c) the documented finalize release passes --expect <lease>, so the ABA guard is actually used"
  else
    bad "(c) finalize still calls release without --expect — the option exists and the flow does not use it"
  fi

  # AND THE FAILURE MUST STAY LOUD: `|| true` on the release is what turned the leak silent.
  # READ THE COMMAND, NOT THE LINE. The first version of this assertion grepped the whole line
  # for `|| true` and matched the COMMENT explaining its absence (`# NO \`|| true\`: a refused
  # release is a LEAK`) — a false FAIL on correct input, and the same proxy defect this PR
  # documents: a phrase found by grep means the doc MENTIONS it, often to retract it. So strip
  # everything from the first `#` and test what would actually execute.
  fin_cmd="$(grep 'lane-lock\.sh release <N>' "$FIN_SKILL" | grep -v '^\s*#' | head -1 | sed 's/#.*//')"
  if ! printf '%s' "$fin_cmd" | grep -q '|| true'; then
    ok "(b) the release is not suppressed with \`|| true\` — a refused release is a LEAK, not noise"
  else
    bad "(b) the release is suppressed with \`|| true\`, which is what made the leak silent"
  fi
else
  bad "(a) cannot read $FIN_SKILL — this assertion covers a doctrine file and must not pass vacuously"
fi

# ===========================================================================
echo 'TEST 36: a .. component is resolved AFTER the symlink, as the kernel does (roborev round 16)'
# ===========================================================================
# `/base/link/../lane` with `link -> /other/sub` NAMES `/other/lane`, because POSIX resolves the
# symlink before `..`. Lexical `..` removal yields `/base/lane` instead — so two spellings of ONE
# directory take TWO directory mutexes and BOTH acquire. That is the alias hole round 10 closed
# for a symlinked ancestor, reopened through `..`.
#
# BOTH branches are covered on purpose. The review named the not-yet-exists path; the EXISTING
# path had the same defect via a LOGICAL `cd`, and it is invisible unless BOTH candidate
# directories exist — bash falls back to physical when the logical target is missing, so a
# single-directory test passes while the bug is present.
SYM36="$LANES/sym36"; mkdir -p "$SYM36/base" "$SYM36/other/sub"
ln -sfn "$SYM36/other/sub" "$SYM36/base/link"
SPELL_A="$SYM36/base/link/../lane"
SPELL_B="$SYM36/other/lane"

# (a) MISSING LEAF — the reported case. Two spellings, one directory, one lock.
sleep 300 & S36_A=$!
sleep 300 & S36_B=$!
ll acquire 936 --lane-dir "$SPELL_A" --pid "$S36_A"; rc36a=$RC
ll acquire 937 --lane-dir "$SPELL_B" --pid "$S36_B"; rc36b=$RC; out36b=$OUT
rec36=$(grep '^lane-dir=' "$(record_of 936)" 2>/dev/null | cut -d= -f2-)
if [ "$rc36a" -eq 0 ] && [ "$rc36b" -eq 2 ] \
   && printf '%s' "$out36b" | grep -q 'reason=same-lane-dir-other-issue' \
   && [ "$rec36" = "$SPELL_B" ]; then
  ok "(a) missing leaf: '<sym>/../lane' and the physical path contend, and the RECORDED path is the physical one"
else
  bad "(a) '..' resolved lexically: rc=$rc36a/$rc36b recorded='$rec36' want='$SPELL_B'
$out36b"
fi
kill "$S36_A" "$S36_B" 2>/dev/null || true
rm -f "$(record_of 936)" "$(record_of 937)" 2>/dev/null || true

# (b) BOTH DIRECTORIES EXIST — the case a single-directory test cannot see, because bash's `cd`
#     falls back to physical only when the LOGICAL target is absent.
mkdir -p "$SYM36/base/lane" "$SYM36/other/lane"
sleep 300 & S36_C=$!
ll acquire 938 --lane-dir "$SPELL_A" --pid "$S36_C"; rc36c=$RC
rec38=$(grep '^lane-dir=' "$(record_of 938)" 2>/dev/null | cut -d= -f2-)
if [ "$rc36c" -eq 0 ] && [ "$rec38" = "$SPELL_B" ]; then
  ok "(b) both directories exist: the recorded path is still the PHYSICAL one, not the logical spelling"
else
  bad "(b) logical resolution won when both exist: recorded='$rec38' want='$SPELL_B'"
fi
kill "$S36_C" 2>/dev/null || true

# ===========================================================================
echo 'TEST 37: the mutex-name limit is in BYTES, and lane-dir is emitted LAST (roborev round 17)'
# ===========================================================================
# (a) NAME_MAX is a BYTE limit; `${#name}` counts CHARACTERS under a multibyte locale. Measured:
#     a 410-byte name of `e-acute` reports 210 chars under en_US.UTF-8 and 410 under C. So a deep
#     UNICODE lane path slipped past the >255 test at its true size and failed as
#     `mutex-unopenable` on a VALID path — the exact failure round 13's fallback exists to
#     prevent. Round 13 fixed the length and used the wrong unit, so this runs under a UTF-8
#     locale on purpose: under C the defect is invisible.
U37="$LANES/$(printf 'é%.0s' $(seq 1 60))/$(printf 'ü%.0s' $(seq 1 60))/lane37"
mkdir -p "$U37" 2>/dev/null
sleep 300 & U37P=$!
( export LC_ALL=en_US.UTF-8; : ) 2>/dev/null
LC_ALL=en_US.UTF-8 ll acquire 937 --lane-dir "$U37" --pid "$U37P"; rc37=$RC; out37=$OUT
longest_bytes=0
for f in "$LOCKS"/*; do
  [ -e "$f" ] || continue
  b=$(printf '%s' "${f##*/}" | wc -c | tr -d ' ')
  [ "$b" -gt "$longest_bytes" ] && longest_bytes=$b
done
if [ "$rc37" -eq 0 ] && [ "$longest_bytes" -le 255 ]; then
  ok "(a) a deep UNICODE lane acquires and no lock filename exceeds NAME_MAX in BYTES (longest=$longest_bytes)"
else
  bad "(a) byte-vs-char: rc=$rc37 longest-lock-filename-bytes=$longest_bytes
$out37"
fi
kill "$U37P" 2>/dev/null || true

# (b) THE HEADER MANDATES lane-dir LAST (:225) and cites the incident that produced the rule:
#     a space-delimited scan turned `/data/my lanes/lane-5` into `/data/my`. Four emit sites in
#     this file and two in claim.sh violated it. Asserted BEHAVIOURALLY — a space-bearing lane
#     dir must survive a field read intact, which is the property, not the field order.
SP37="$LANES/my lane 37"; mkdir -p "$SP37"
sleep 300 & SPA=$!
sleep 300 & SPB=$!
ll acquire 938 --lane-dir "$SP37" --pid "$SPA"
ll acquire 939 --lane-dir "$SP37" --pid "$SPB"; out39="$OUT"
# read lane-dir the way claim.sh does: everything after the key, to end of line
got37="$(printf '%s' "$out39" | sed -n 's/.* lane-dir=\(.*\)$/\1/p' | head -1)"
if printf '%s' "$out39" | grep -q 'reason=same-lane-dir-other-issue' && [ "$got37" = "$SP37" ]; then
  ok "(b) a SPACE-BEARING lane dir survives a to-end-of-line field read intact ('$got37')"
else
  bad "(b) lane-dir was truncated or is not last: got='$got37' want='$SP37'
$out39"
fi
kill "$SPA" "$SPB" 2>/dev/null || true

# ===========================================================================
echo 'TEST 38: a ZOMBIE --pid, a record path that is a DIRECTORY, and the note that never fired (round 20)'
# ===========================================================================
# (a) A ZOMBIE HAS /proc AND IS DEAD. The write-time check was `[ ! -e /proc/<pid> ]`, which a
#     zombie satisfies — its stat and start-ticks stay readable until the parent reaps it — while
#     record_liveness maps state Z to DEAD-NO-PROCESS. So `acquire --pid <zombie>` wrote a record
#     whose own liveness reads DEAD and which the next acquire auto-reclaims: ACQUIRED that
#     protects nothing, FIX 14's defect by another route.
#     The zombie is made with `exec`, so the parent never reaps it: `sh -c 'sleep & exec sleep'`.
sh -c 'sleep 0.1 & exec sleep 30' &
Z_PARENT=$!
i=0; Z=""
while [ "$i" -lt 30 ] && [ -z "$Z" ]; do
  Z=$(ps -o pid=,stat= --ppid "$Z_PARENT" 2>/dev/null | awk '$2 ~ /^Z/ {print $1}' | head -1)
  [ -n "$Z" ] || { sleep 0.1; i=$((i + 1)); }
done
if [ -n "$Z" ]; then
  ll acquire 940 --lane-dir "$LANES/zlane" --pid "$Z"; rc38a=$RC; out38a=$OUT
  if [ "$rc38a" -ne 0 ] && printf '%s' "$out38a" | grep -qi 'zombie'; then
    ok "(a) acquire --pid <zombie> is refused and names the state (a record naming it would read DEAD)"
  else
    bad "(a) a zombie pid was accepted as a holder: rc=$rc38a
$out38a"
  fi
else
  bad "(a) could not construct a zombie — this case must not pass vacuously"
fi
kill "$Z_PARENT" 2>/dev/null || true

# (b) parse_record requires `-f`, so a DIRECTORY at the record path reads as "no record" — and
#     `mv -f "$tmp" "$path"` then moves the temp INSIDE it and succeeds. ACQUIRED reported, no
#     record where every reader looks, lane unprotected, and the next acquire "succeeds" too.
# HERMETIC, IN ITS OWN LANE_ROOT (#3436, roborev round 22). Run against the shared suite root
# this case was observed passing on one run and failing on the next, and the instrumented cause
# was that the directory existed right after `mkdir` and was GONE by the time acquire read it —
# so it sometimes exercised the ordinary acquire path while claiming to test the guard. The
# interaction was never isolated; the case does not need the shared root, so it no longer uses
# one. A case that can be hermetic should be, and one that cannot establish its precondition
# must FAIL SAYING SO rather than silently test something else.
D38="$(mktemp -d)"
mkdir -p "$D38/.lane-locks/lane-941.lock" "$D38/dirlane"   # the record PATH is a directory
sleep 300 & DP38=$!
if [ ! -d "$D38/.lane-locks/lane-941.lock" ]; then
  bad "(b) PRECONDITION: could not place a directory at the record path — this case must not pass vacuously"
else
  out38b="$(env -u LANE_LOCK_PID LANE_ROOT="$D38" bash "$LL" acquire 941 --lane-dir "$D38/dirlane" --actor flow --pid "$DP38" 2>&1)"; rc38b=$?
  st38b="$(env -u LANE_LOCK_PID LANE_ROOT="$D38" bash "$LL" status 941 2>&1)"
  inside38="$(ls -A "$D38/.lane-locks/lane-941.lock" 2>/dev/null | wc -l | tr -d ' ')"
  # ASSERTED ON THE PROPERTY, NOT ON ONE MESSAGE (#3436, roborev round 25). The refusal now comes
  # EARLIER — the read path classifies a non-regular record path as UNKNOWN-UNREADABLE rather than
  # absent, so acquire refuses before reaching write_record's guard. Both refusals are correct and
  # pinning either message would red on the other. What must hold is: refused, NOTHING written
  # into the directory, and the READERS agreeing rather than reporting FREE — that last one is the
  # actual defect round 25 named, where status said FREE for a state acquire refused.
  if [ "$rc38b" -ne 0 ] && [ "$inside38" -eq 0 ] \
     && ! printf '%s' "$st38b" | grep -q 'FREE'; then
    ok "(b) a DIRECTORY at the record path: acquire refused (rc=$rc38b), nothing written into it, and status does NOT report FREE"
  else
    bad "(b) rc=$rc38b entries-written-inside=$inside38
acquire: $out38b
status:  $st38b"
  fi
fi
kill "$DP38" 2>/dev/null || true
rm -rf "$D38"

# (c) THE NOTE ROUND 19 ADDED COULD NEVER FIRE — the helper ran in a command substitution, so its
#     assignment to LANE_REAL_UNRESOLVED happened in a subshell and was discarded. A fix for a
#     silent failure that was itself silent, shipped without a test that the warning appears.
#     THIS is that test. An unsearchable ancestor is a real case: `cd -P` fails on it, so the
#     component cannot be canonicalised and two spellings could take two mutexes.
UNS="$LANES/unsearch38"; mkdir -p "$UNS/inner"
chmod 000 "$UNS/inner" 2>/dev/null
sleep 300 & UP38=$!
ll acquire 942 --lane-dir "$UNS/inner/lane" --pid "$UP38"; out38c="$OUT"
chmod 755 "$UNS/inner" 2>/dev/null
if printf '%s' "$out38c" | grep -q 'could not physically resolve existing component'; then
  ok "(c) an unsearchable existing component is REPORTED — the round-19 note actually fires now"
else
  bad "(c) the unresolved-component note did not appear (round 19's subshell defect):
$out38c"
fi
kill "$UP38" 2>/dev/null || true

# ===========================================================================
echo 'TEST 39: release is ABA-safe with --expect, and compare-mode refuses a zombie (round 21)'
# ===========================================================================
# (a) THE TOKEN SURVIVES A RELEASE AND REACQUIRE; THE LEASE DOES NOT. Every one of the five
#     identity components is unchanged when the SAME process releases and acquires again, so a
#     DELAYED or DUPLICATED release from the earlier acquisition matched SELF against the LATER
#     record and deleted a LIVE lock. TEST 21 pins exactly this for `reclaim --expect`; release
#     compared only the token, so the distinction was made in one subcommand and not its sibling.
ABA="$LANES/aba39"; mkdir -p "$ABA"
sleep 300 & A39=$!
ll acquire 943 --lane-dir "$ABA" --pid "$A39"
lease_first=$(lease_of 943)
ll release 943 --pid "$A39"
ll acquire 943 --lane-dir "$ABA" --pid "$A39"     # SAME process, SAME token, NEW incarnation
lease_second=$(lease_of 943)
tok_first="${lease_first%%#*}"; tok_second="${lease_second%%#*}"
if [ "$tok_first" = "$tok_second" ] && [ "$lease_first" != "$lease_second" ]; then
  ok "(a1) the token is IDENTICAL across release+reacquire and the lease DIFFERS — the ABA precondition holds"
else
  bad "(a1) precondition not reproduced: tok '$tok_first' vs '$tok_second', lease '$lease_first' vs '$lease_second'"
fi
# the stale release, carrying the FIRST lease, must be refused rather than deleting the live lock
ll release 943 --pid "$A39" --expect "$lease_first"; rc39=$RC; out39=$OUT
still="$(cat "$(record_of 943)" 2>/dev/null | head -c 1)"
if [ "$rc39" -ne 0 ] && printf '%s' "$out39" | grep -q 'RELEASE-LOST' && [ -n "$still" ]; then
  ok "(a2) a stale release carrying the OLD lease is refused and the live record SURVIVES"
else
  bad "(a2) a stale release deleted a live lock: rc=$rc39 record-present=${still:+yes}
$out39"
fi
# CONTROL: the CURRENT lease still releases — a CAS that refuses everything is broken.
ll release 943 --pid "$A39" --expect "$lease_second"
if [ "$RC" -eq 0 ] && [ ! -f "$(record_of 943)" ]; then
  ok "(a3) CONTROL: release with the CURRENT lease succeeds"
else
  bad "(a3) CONTROL: the current lease was refused: rc=$RC
$OUT"
fi
kill "$A39" 2>/dev/null || true

# (b) COMPARE mode had the same zombie hole the WRITE branch got in round 20 — a sibling in the
#     same if/elif chain, one line above the arm I added. A zombie keeps /proc and start-ticks, so
#     `probe` could match a DEAD record as SELF, which claim.sh reads to decide
#     `released-then-resumed`.
#
#     THE FIRST VERSION OF THIS CASE WAS VACUOUS AND PASSED ANYWAY (roborev round 22). It probed
#     an issue with NO RECORD, so `cmd_probe` returned `FREE … NO-RECORD` before identity ever
#     mattered and the required output was unreachable. Measured after the fact:
#     `DBG39b[LANE-LOCK: FREE issue=944 liveness=NO-RECORD …]`. So the record must EXIST and its
#     holder must BE the zombie — which means acquiring while that pid is still ALIVE (round 20
#     refuses a zombie at write time) and letting it die afterwards.
Z39_LANE="$LANES/z39"; mkdir -p "$Z39_LANE"
sh -c 'sleep 3 & exec sleep 60' &
Z39_PARENT=$!
i=0; Z39=""
while [ "$i" -lt 40 ] && [ -z "$Z39" ]; do
  Z39=$(ps -o pid=,stat= --ppid "$Z39_PARENT" 2>/dev/null | awk '$2 !~ /^Z/ {print $1}' | head -1)
  [ -n "$Z39" ] || { sleep 0.1; i=$((i + 1)); }
done
if [ -n "$Z39" ]; then
  ll acquire 944 --lane-dir "$Z39_LANE" --pid "$Z39"; rc39b_acq=$RC
  # now wait for that very holder to become a ZOMBIE (its parent never reaps it)
  i=0; zstate=""
  while [ "$i" -lt 80 ] && [ "$zstate" != "Z" ]; do
    zstate=$(awk '{print $3}' "/proc/$Z39/stat" 2>/dev/null)
    [ "$zstate" = "Z" ] || { sleep 0.1; i=$((i + 1)); }
  done
  if [ "$rc39b_acq" -eq 0 ] && [ "$zstate" = "Z" ]; then
    out39b="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" probe 944 --lane-dir "$Z39_LANE" --pid "$Z39" 2>&1)"
    if ! printf '%s' "$out39b" | grep -q 'liveness=SELF' && printf '%s' "$out39b" | grep -q 'issue=944'; then
      ok "(b) probe does NOT report SELF for a record whose holder is now a zombie"
    else
      bad "(b) compare mode matched a dead holder as SELF:
$out39b"
    fi
  else
    bad "(b) precondition failed: acquire rc=$rc39b_acq, holder state='$zstate' (wanted Z) — this case must not pass vacuously"
  fi
else
  bad "(b) could not obtain a live child pid — this case must not pass vacuously"
fi
kill "$Z39_PARENT" 2>/dev/null || true

# ===========================================================================
echo 'TEST 40: a directory guard that could not read every record SAYS SO (roborev round 23)'
# ===========================================================================
# `same_dir_other_issue` had THREE skips that were each "cannot tell" taking the PERMISSIVE
# branch: a non-regular-file entry, a parse failure, and REC_UNREADABLE=1 — which returns 0 while
# leaving REC_LANE_DIR empty, so the record reads as "a different directory". Any of them hides a
# live holder of THIS directory while the guard reports clean.
#
# NOT fatal, deliberately: one unreadable record would refuse EVERY acquire for EVERY issue on the
# box, and a guard that bricks lanes on a permissions problem is worse than the hole (FIX 5/14).
# So the scan counts what it could not read and the verdict DISCLOSES it.
#
# ASSERTED ON THE OBSERVABLE, NOT ON THE SOURCE. Three "fixes" in this branch were only messages
# that never appeared (r12 reordered nothing, r19 assigned in a subshell, r21 only noted), each
# shipped without a test that the message shows up. This is that test.
D40="$(mktemp -d)"; mkdir -p "$D40/.lane-locks" "$D40/lane40"
printf 'version=1\nissue=888\n' > "$D40/.lane-locks/lane-888.lock"
chmod 000 "$D40/.lane-locks/lane-888.lock" 2>/dev/null       # present, enumerable, UNREADABLE
sleep 300 & P40=$!
out40="$(env -u LANE_LOCK_PID LANE_ROOT="$D40" bash "$LL" acquire 940 --lane-dir "$D40/lane40" --actor flow --pid "$P40" 2>&1)"; rc40=$?
chmod 644 "$D40/.lane-locks/lane-888.lock" 2>/dev/null
if [ "$rc40" -eq 0 ] && printf '%s' "$out40" | grep -q 'dir-guard=NOT-EXHAUSTIVE'; then
  ok "(a) an unreadable record does not block the acquire, and the ACQUIRED line DISCLOSES that the directory guard was partial"
  # AND lane-dir MUST STILL BE THE LAST FIELD (round 17's contract, re-broken in round 23 by
  # appending the caveat AFTER it — a to-end-of-line read then swallowed the guard text into the
  # path). Asserted HERE, on the ACQUIRE line, because that is where the displacement happened;
  # the same check on reclaim cannot red against pre-fix code, which had no caveat there at all.
  got40="$(printf '%s' "$out40" | sed -n 's/.* lane-dir=\(.*\)$/\1/p' | head -1)"
  if [ "$got40" = "$D40/lane40" ]; then
    ok "(a2) lane-dir is still the LAST field on ACQUIRED with the caveat present"
  else
    bad "(a2) the caveat displaced lane-dir from the end: got '$got40' want '$D40/lane40'"
  fi
else
  bad "(a) rc=$rc40 — expected ACQUIRED carrying dir-guard=NOT-EXHAUSTIVE:
$out40"
fi
kill "$P40" 2>/dev/null || true
rm -rf "$D40"

# CONTROL: with every record readable the caveat must be ABSENT — a disclosure that always fires
# is noise, and would make the partial case indistinguishable from the ordinary one.
D40B="$(mktemp -d)"; mkdir -p "$D40B/.lane-locks" "$D40B/lane40b"
sleep 300 & P40B=$!
out40b="$(env -u LANE_LOCK_PID LANE_ROOT="$D40B" bash "$LL" acquire 941 --lane-dir "$D40B/lane40b" --actor flow --pid "$P40B" 2>&1)"; rc40b=$?
if [ "$rc40b" -eq 0 ] && ! printf '%s' "$out40b" | grep -q 'dir-guard='; then
  ok "(b) CONTROL: with every record readable the ACQUIRED line carries no caveat"
else
  bad "(b) CONTROL: the caveat fired on a clean scan: rc=$rc40b
$out40b"
fi
kill "$P40B" 2>/dev/null || true
rm -rf "$D40B"

# ===========================================================================
echo 'TEST 41: RECLAIM carries the same non-exhaustive-guard disclosure as acquire (round 24)'
# ===========================================================================
# Round 23 computed the caveat in ACQUIRE's wrapper only, so `reclaim` ran the same cross-issue
# scan and minted a record with no disclosure. An expected-count assert had already forced me to
# convert BOTH callers to the new calling convention, and I still wired the caveat into ONE — so
# it is now computed where the COUNT is produced, inside the scan, and the two callers cannot
# diverge. This case exists to keep them from diverging again.
D41="$(mktemp -d)"; mkdir -p "$D41/.lane-locks" "$D41/lane41"
printf 'version=1\nissue=887\n' > "$D41/.lane-locks/lane-887.lock"
chmod 000 "$D41/.lane-locks/lane-887.lock" 2>/dev/null
sleep 300 & P41=$!
out41="$(env -u LANE_LOCK_PID LANE_ROOT="$D41" bash "$LL" reclaim 946 --lane-dir "$D41/lane41" --actor flow --pid "$P41" --expect none --reason round24-disclosure-on-reclaim 2>&1)"; rc41=$?
chmod 644 "$D41/.lane-locks/lane-887.lock" 2>/dev/null
if [ "$rc41" -eq 0 ] && printf '%s' "$out41" | grep -q 'dir-guard=NOT-EXHAUSTIVE'; then
  ok "(a) RECLAIM discloses a partial directory guard, exactly as acquire does"
else
  bad "(a) reclaim minted a record with no disclosure: rc=$rc41
$out41"
fi
# CONTRACT: lane-dir stays the LAST field even with the caveat present (round 17, re-broken in
# round 23 by appending the guard AFTER it — a to-end-of-line read then swallowed the guard text).
got41="$(printf '%s' "$out41" | sed -n 's/.* lane-dir=\(.*\)$/\1/p' | head -1)"
# NOTE: unlike TEST 40(a2) this half CANNOT red-proof against pre-fix code — reclaim carried no
# caveat there, so lane-dir was already last on its line. It guards the two paths from diverging
# from here on, which is its purpose; TEST 40(a2) is the one that catches the original defect.
if [ "$got41" = "$D41/lane41" ]; then
  ok "(b) lane-dir is still the LAST field with the caveat present ('$got41')"
else
  bad "(b) the caveat displaced lane-dir from the end: got '$got41' want '$D41/lane41'"
fi
kill "$P41" 2>/dev/null || true
rm -rf "$D41"

# ===========================================================================
echo 'TEST 42: a reboot really is a global un-brick, and the two status views agree (round 26)'
# ===========================================================================
# (a) CLAUDE.md states "a reboot is a global un-brick: boot-id changes, so every pre-reboot record
#     reads DEAD-REBOOT and auto-reclaims", and FIX 5/14 leans on that as the escape hatch for a
#     lane an EPHEMERAL record would otherwise brick. record_liveness tested pid-scope and pid
#     validity BEFORE the boot id, so a pre-reboot record with pid-scope=ephemeral read
#     UNKNOWN-EPHEMERAL and was never reclaimed — the one state the reboot was promised to clear
#     was the one it did not. This asserts the documented guarantee, not the code path.
D42="$(mktemp -d)"; mkdir -p "$D42/.lane-locks"
printf 'version=1\nissue=950\nlane-dir=%s/l\nmachine=%s\nactor=flow\npid=99999\npid-scope=ephemeral\nboot-id=00000000-dead-beef-0000-000000000000\nstart-ticks=1\nacquired-ts=2020-01-01T00:00:00Z\nnonce=x\n' \
  "$D42" "$(hostname -s 2>/dev/null || hostname)" > "$D42/.lane-locks/lane-950.lock"
out42="$(env -u LANE_LOCK_PID LANE_ROOT="$D42" bash "$LL" status 950 2>&1)"
if printf '%s' "$out42" | grep -q 'liveness=DEAD-REBOOT' && printf '%s' "$out42" | grep -q 'reclaimable=yes'; then
  ok "(a) a PRE-REBOOT record with pid-scope=ephemeral is DEAD-REBOOT and reclaimable — the documented un-brick holds"
else
  bad "(a) a pre-reboot ephemeral record is still not reclaimable:
$out42"
fi
# CONTROL: an ABSENT recorded boot id must stay UNKNOWN. "Cannot tell which boot" is not "dead",
# and a reordering that turned the unknown into a DEAD verdict would auto-reclaim a live lane.
printf 'version=1\nissue=952\nlane-dir=%s/l\nmachine=%s\nactor=flow\npid=99999\npid-scope=ephemeral\nstart-ticks=1\nnonce=x\n' \
  "$D42" "$(hostname -s 2>/dev/null || hostname)" > "$D42/.lane-locks/lane-952.lock"
out42b="$(env -u LANE_LOCK_PID LANE_ROOT="$D42" bash "$LL" status 952 2>&1)"
if printf '%s' "$out42b" | grep -q 'liveness=UNKNOWN-NO-BOOT-ID'; then
  ok "(b) CONTROL: a record with NO boot id stays UNKNOWN-NO-BOOT-ID — cannot-tell is not dead"
else
  bad "(b) CONTROL: an absent boot id produced a non-UNKNOWN verdict:
$out42b"
fi

# (c) The two STATUS VIEWS must agree. Round 25 made direct `status <N>` classify a non-regular
#     record path as UNKNOWN-UNREADABLE; the unscoped enumeration still filtered on `-f` and
#     omitted it, so `locks=` undercounted a state that blocks every acquire.
mkdir -p "$D42/.lane-locks/lane-951.lock"
ln -sfn /nonexistent "$D42/.lane-locks/lane-953.lock"
locks42="$(env -u LANE_LOCK_PID LANE_ROOT="$D42" bash "$LL" status 2>&1 | grep -oE 'locks=[0-9]+' | head -1 | cut -d= -f2)"
dir42="$(env -u LANE_LOCK_PID LANE_ROOT="$D42" bash "$LL" status 951 2>&1 | grep -oE 'liveness=[A-Z-]+' | head -1)"
if [ "${locks42:-0}" -eq 4 ] && [ "$dir42" = "liveness=UNKNOWN-UNREADABLE" ]; then
  ok "(c) unscoped status counts the directory and broken symlink too (locks=$locks42), agreeing with direct status"
else
  bad "(c) the two status views disagree: locks=$locks42 (want 4: 950, 951 dir, 952, 953 symlink), direct 951=$dir42"
fi
rm -rf "$D42"

echo "==== LANE-LOCK TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
