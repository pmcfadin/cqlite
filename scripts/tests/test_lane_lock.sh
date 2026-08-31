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
#     token actually CHANGED and that .lane-lock.log gained a line naming the previous
#     token, the previous liveness verdict and the reason (AC3);
#   * every UNKNOWN-* case asserts the record token is UNCHANGED afterwards — a
#     refusal that silently rewrote the record would pass a naive exit-code check.
#
# HERMETIC: a mktemp lane root, real `sleep` processes for liveness, and nothing else.
# No network, no gh, no git, no cargo, no dataset corpus. Runs in seconds, so it is
# wired into the gate's `tooling-tests` component.
#
# NO TEST-ONLY SEAMS (CLAUDE.md #3312: "a case needing a different enforcer
# substitutes the artifact in its own scratch copy of the tree — never a path
# variable"). There is deliberately NO /proc override and no liveness-injection env
# var in lane-lock.sh: ALIVE and DEAD-NO-PROCESS are proved with REAL processes
# (`sleep 300 &` / `kill`), and the verdicts that cannot be produced on demand
# (DEAD-PID-REUSED, DEAD-REBOOT, UNKNOWN-FOREIGN, UNKNOWN-EPHEMERAL,
# UNKNOWN-NO-PID, UNKNOWN-UNREADABLE) are produced by SUBSTITUTING THE ARTIFACT — the
# record file in this suite's own scratch lane dir — which is exactly what a hand-made
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
mkdir -p "$LANES"
SLEEPERS=""
cleanup() {
  local p
  for p in $SLEEPERS; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
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

lane_of()   { printf '%s/lane-%s\n' "$LANES" "$1"; }
record_of() { printf '%s/lane-%s/.lane-lock\n' "$LANES" "$1"; }

# token_of <issue> — the holder token as the PUBLIC surface reports it (probe), so the
# assertions read the same value another tool would.
token_of() {
  local o
  o="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" probe "$1" 2>/dev/null)" || true
  field "$o" holder-token
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
sleeper() {
  # stdout/stderr MUST be redirected: this function is called in a command
  # substitution, and a background child inheriting that pipe keeps it open, so
  # `A=$(sleeper)` would block for the sleep's full duration.
  sleep 300 >/dev/null 2>&1 &
  local p=$!
  SLEEPERS="$SLEEPERS $p"
  printf '%s\n' "$p"
}

# ===========================================================================
echo "TEST 1: acquire on a FREE lane succeeds, verify then confirms it"
# POSITIVE CONTROL (AC4 non-vacuity): a refuse-everything implementation fails here.
# ===========================================================================
A=$(sleeper)
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
B=$(sleeper)
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
kill "$A" 2>/dev/null; wait "$A" 2>/dev/null
ll acquire 101 --actor flow --pid "$B"; rc=$RC; out="$OUT"
tok_new=$(token_of 101)
logline="$(grep -c 'verdict=ACQUIRED-RECLAIMED' "$(lane_of 101)/.lane-lock.log" 2>/dev/null || echo 0)"
logtext="$(grep 'verdict=ACQUIRED-RECLAIMED' "$(lane_of 101)/.lane-lock.log" 2>/dev/null || true)"
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
logtext="$(tail -1 "$(lane_of 101)/.lane-lock.log" 2>/dev/null || true)"
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
unknown_case() {
  local label="$1" issue="$2"; shift 2
  local tok_b tok_a rc_l out_l lv
  # fresh lane per case, held by a LIVE pid, then the record is substituted
  ll acquire "$issue" --actor flow --pid "$B" >/dev/null 2>&1
  "$@"
  tok_b=$(token_of "$issue")
  ll acquire "$issue" --actor flow --pid "$C"; rc_l=$RC; out_l="$OUT"
  tok_a=$(token_of "$issue")
  lv=$(field "$out_l" liveness)
  if [ "$rc_l" -eq 2 ] && printf '%s' "$out_l" | grep -q '^LANE-LOCK: OCCUPIED ' \
     && [ "$lv" = "$label" ] && [ "$tok_a" = "$tok_b" ]; then
    ok "$label REFUSES (OCCUPIED rc=2) and does not rewrite the record"
  else
    bad "expected OCCUPIED rc=2 liveness=$label with an unchanged record; got rc=$rc_l liveness=$lv tok '$tok_b' -> '$tok_a'
$out_l"
  fi
}
C=$(sleeper)
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
D=$(sleeper)
E=$(sleeper)
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
ll acquire 402 --actor flow --pid "$D" >/dev/null 2>&1
ll probe 402; rch=$RC; outh2="$OUT"
if [ "$rcf" -eq 0 ] && printf '%s' "$outf" | grep -q '^LANE-LOCK: FREE ' \
   && [ "$(field "$outf" liveness)" = "NO-RECORD" ] \
   && [ "$dir_exists" = "no" ] && [ "${created:-0}" = "0" ] \
   && [ "$rch" -eq 0 ] && printf '%s' "$outh2" | grep -q '^LANE-LOCK: HELD ' \
   && [ "$(field "$outh2" liveness)" = "ALIVE" ] \
   && [ "$(field "$outh2" holder-pid)" = "$D" ] \
   && [ -n "$(field "$outh2" acquired-ts)" ] && [ -n "$(field "$outh2" age)" ] \
   && [ "$(field "$outh2" reclaimable)" = "no" ]; then
  ok "probe: FREE rc=0 creating nothing (no lane dir, no mutex); HELD rc=0 carrying liveness/holder-*/acquired-ts/age"
else
  bad "probe contract violated: rcFREE=$rcf dir-created=$dir_exists entries=$created rcHELD=$rch
$outf
$outh2"
fi

# ===========================================================================
echo "TEST 11: reclaim compare-and-swap"
# ===========================================================================
tok=$(token_of 402)
ll reclaim 402 --expect "$tok" --reason lane-holder-oom-killed-verified-by-dmesg --actor flow --pid "$E"; rc=$RC; out="$OUT"
tok_new=$(token_of 402)
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIMED ' \
   && [ "$tok_new" != "$tok" ] && printf '%s' "$tok_new" | grep -q ":$E:" \
   && grep -q "reclaim-reason=lane-holder-oom-killed-verified-by-dmesg" "$(record_of 402)" \
   && grep -q "reclaimed-from=$tok" "$(record_of 402)" \
   && grep -q 'verdict=RECLAIMED' "$(lane_of 402)/.lane-lock.log"; then
  ok "matched --expect: RECLAIMED rc=0, token replaced, record carries reclaimed-from + reclaim-reason, audit line written"
else
  bad "expected a satisfied CAS to RECLAIM; got rc=$rc tok '$tok' -> '$tok_new'
$out"
fi

tok=$(token_of 402)
ll reclaim 402 --expect "not-the-current-token" --reason stale-lease-check --actor flow --pid "$D"; rc=$RC; out="$OUT"
tok_after=$(token_of 402)
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '^LANE-LOCK: RECLAIM-LOST ' \
   && [ "$(field "$out" expected)" = "not-the-current-token" ] \
   && [ "$(field "$out" actual)" = "$tok" ] && [ "$tok_after" = "$tok" ]; then
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
ll reclaim 403 --expect none --reason re-entrant-retry-after-confirm-blip --actor flow --pid "$D"; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'RECLAIMED (re-entrant, lease-mismatch expected=none actual=' \
   && printf '%s' "$out" | grep -q "actual=$tok"; then
  ok "re-entrant reclaim with a violated lease: rc=0 naming BOTH expected=none and actual=<our token>"
else
  bad "expected a re-entrant, lease-mismatch verdict naming both values; got rc=$rc
$out"
fi

ll reclaim 403 --expect "$tok" --reason re-entrant-retry-lease-held --actor flow --pid "$D"; rc=$RC; out="$OUT"
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'RECLAIMED (re-entrant)' \
   && ! printf '%s' "$out" | grep -q 'lease-mismatch'; then
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
  out_l="$(env -u LANE_LOCK_PID LANE_ROOT="$LANES" bash "$LL" reclaim 403 --expect "$tok" "$@" --actor flow 2>&1)" || rc_l=$?
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
if [ "$rc" -eq 0 ] && [ -f "$T/explicit-lane/.lane-lock" ] && printf '%s' "$out" | grep -q "lane-dir=$T/explicit-lane"; then
  ok "an ABSOLUTE --lane-dir is honoured (control for the relative-path refusals)"
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
  p=$(sleeper)
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
winner_tok=$(field "$(cat "$T/conc-lane/.lane-lock" 2>/dev/null | tr '\n' ' ')" pid)
if [ "$acq" -eq 1 ] && [ "$occ" -eq 7 ] && [ "$other" -eq 0 ] && [ -n "$winner_tok" ]; then
  ok "8 concurrent acquires: exactly 1 ACQUIRED, 7 OCCUPIED, 0 other outcomes (winner pid=$winner_tok)"
else
  bad "expected 1 ACQUIRED / 7 OCCUPIED / 0 other; got acquired=$acq occupied=$occ other=$other"
fi

# ===========================================================================
echo "TEST 15: status renders single-lane and enumerated views"
# ===========================================================================
ll status 402; rc1=$RC; out1="$OUT"
ll status; rc2=$RC; out2="$OUT"
locks=$(field "$out2" locks)
if [ "$rc1" -eq 0 ] && printf '%s' "$out1" | grep -q '^LANE-LOCK: \(HELD\|FREE\) issue=402 ' \
   && [ "$rc2" -eq 0 ] && printf '%s' "$out2" | grep -q '^LANE-LOCK: STATUS ' \
   && [ -n "$locks" ] && [ "$locks" -ge 1 ]; then
  ok "status <N> renders one lane; bare status enumerates the lane root (locks=$locks) rc=0"
else
  bad "status render failed: rc=$rc1/$rc2 locks='$locks'
$out1
$out2"
fi

# ===========================================================================
echo "TEST 16: --help exits 0 and documents every subcommand"
# Guards the header against drifting out of the file: print_help renders the header
# comment, so a subcommand added without documenting it fails here.
# ===========================================================================
ll --help; rc=$RC; out="$OUT"
missing=""
for sub in acquire verify probe release reclaim status; do
  printf '%s' "$out" | grep -q "^  $sub " || missing="$missing $sub"
done
if [ "$rc" -eq 0 ] && [ -z "$missing" ] && printf '%s' "$out" | grep -q '3436'; then
  ok "--help exits 0 and documents acquire/verify/probe/release/reclaim/status (and cites #3436)"
else
  bad "--help incomplete: rc=$rc undocumented:${missing:-<none>}"
fi

# ===========================================================================
echo
echo "==== LANE-LOCK TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
