#!/usr/bin/env bash
#
# Regression tests for scripts/flow/drive-issue-state.sh (issue #3822).
#
# Fast + hermetic: mktemp directories stand in for lane worktrees. No network, no
# gh, no cargo, no datasets. Identity is supplied entirely through the environment
# (CLAIM_MACHINE / CLAUDE_CODE_SESSION_ID / CLAUDE_PID) so one process can play
# several lanes and several sessions.
#
# Run standalone:   bash scripts/tests/test_drive_issue_state.sh
#
# No wall-clock timing assertions: every verdict is a file state, an exit code or a
# process-liveness answer about a pid this test itself controls.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DS="$SCRIPT_DIR/../flow/drive-issue-state.sh"
CLAIM="$SCRIPT_DIR/../flow/claim.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# CASE FLOOR bookkeeping (#3544). A span-replacing edit that silently deletes cases
# otherwise leaves a GREEN tally over a SHRUNKEN suite. Every case registers its
# NAME here, and the final case asserts both a minimum count and the presence of
# every required name — so a deleted case reds instead of greening.
CASES=""
case_begin() {
  CASES="$CASES $1"
  printf '\n'
  printf '========================================================\n'
  printf 'TEST %s: %s\n' "$1" "$2"
  printf '========================================================\n'
}

T=$(mktemp -d "${TMPDIR:-/tmp}/drive-issue-state-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

MARKER=".drive-issue-state.md"

# lane <name> — make a lane worktree directory and echo its physical path.
lane() { local d="$T/$1"; mkdir -p "$d"; ( cd "$d" && pwd -P ); }

# run <dir> <env-assignments...> -- <args...>
# Runs the script in <dir> with a CLEAN identity environment: CLAUDE_PID and
# CLAUDE_CODE_SESSION_ID are UNSET unless the caller names them, so a value
# inherited from the real agent session can never decide a test.
run() {
  local dir="$1"; shift
  local -a envs=()
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do envs+=("$1"); shift; done
  [ "${1:-}" = "--" ] && shift
  ( cd "$dir" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID "${envs[@]}" bash "$DS" "$@" )
}

# verdict_of <output> — echo the token from the single `verdict ` line.
verdict_of() {
  printf '%s\n' "$1" | sed -n 's/^DRIVE-STATE: verdict \([^ ]*\).*/\1/p' | head -1
}

# The CLOSED verdict token set (the script's own grammar). An unrecognised token is
# a refusal, so the test pins the set rather than accepting whatever is printed.
VERDICT_SET="OWNED WRITTEN ADOPTED SHOWN ABSENT UNSTAMPED MALFORMED DUPLICATE-SENTINEL FOREIGN-ISSUE FOREIGN-MACHINE FOREIGN-WORKTREE ADOPTABLE LIVE-PEER LIVENESS-UNKNOWN ERROR"
verdict_in_set() {
  local v="$1" t
  [ -n "$v" ] || return 1
  for t in $VERDICT_SET; do [ "$t" = "$v" ] && return 0; done
  return 1
}

# all_lines_anchored <output> — 0 iff every non-empty line begins `DRIVE-STATE: `.
# COUNTED, NOT PIPED THROUGH grep -q: `grep -c` exits 1 when it selects nothing, and
# under this file's `pipefail` that makes a CLEAN result read as a failed condition —
# the #3387 shape, and it cost a round here.
all_lines_anchored() {
  local bad
  bad="$(printf '%s\n' "$1" | grep -v '^$' | grep -cv '^DRIVE-STATE: ' || true)"
  [ "${bad:-1}" = 0 ]
}

SESS_A="sess-aaaaaaaa"
SESS_B="sess-bbbbbbbb"

# ===========================================================================
case_begin 1-write-verify-owned "write then verify in the SAME lane and session -> OWNED"
# ===========================================================================
L1=$(lane lane1)
w_out=$(run "$L1" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement); w_rc=$?
v_out=$(run "$L1" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); v_rc=$?
if [ "$w_rc" -eq 0 ] && [ "$(verdict_of "$w_out")" = WRITTEN ] \
   && [ "$v_rc" -eq 0 ] && [ "$(verdict_of "$v_out")" = OWNED ] \
   && [ -f "$L1/$MARKER" ]; then
  ok "write -> WRITTEN(0), verify in same lane+session -> OWNED(0)"
else
  bad "write/verify same session: w_rc=$w_rc v_rc=$v_rc
$w_out
$v_out"
fi
if all_lines_anchored "$w_out" && all_lines_anchored "$v_out"; then
  ok "every emitted line carries the single anchored DRIVE-STATE: prefix"
else
  bad "unanchored output line present:
$w_out
$v_out"
fi
if grep -q '^issue: 3822$' "$L1/$MARKER" \
   && grep -q '^machine: boxA$' "$L1/$MARKER" \
   && grep -q "^session: $SESS_A\$" "$L1/$MARKER" \
   && grep -q '^actor: ' "$L1/$MARKER" \
   && grep -q "^worktree: $L1\$" "$L1/$MARKER"; then
  ok "AC1: the stamp records issue, machine, worktree, session and actor"
else
  bad "AC1 stamp fields missing:
$(cat "$L1/$MARKER")"
fi

# ===========================================================================
case_begin 2-ac3-unstamped-prose-refused "AC3 RED demonstration: the PRE-FIX marker shape is REFUSED, not adopted"
# ===========================================================================
# This case IS acceptance criterion 3. The file below is exactly what
# .claude/commands/drive-issue.md prescribed BEFORE this change: free-form prose
# with NO ownership stamp. A session rehydrating in a shared or reused worktree
# used to adopt such a plan wholesale; the reader must now REFUSE it by name.
L2=$(lane lane2)
cat >"$L2/$MARKER" <<'PROSE'
# drive-issue state for #3822

- stage: implement
- open request: coord-3822-1
- branch: issue-3822-drive-issue-state-ownership-stamp
- timestamp: 2026-09-01T10:00:00Z
PROSE
p_out=$(run "$L2" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); p_rc=$?
if [ "$p_rc" -ne 0 ] && [ "$(verdict_of "$p_out")" = UNSTAMPED ]; then
  ok "AC3: an UNSTAMPED prose marker is a NAMED refusal (UNSTAMPED, rc=$p_rc), never a silent adoption"
else
  bad "AC3: unstamped prose marker was not refused by name: rc=$p_rc
$p_out"
fi
if printf '%s\n' "$p_out" | grep -q '3822'; then
  ok "AC3 refusal names the issue it refused for"
else
  bad "AC3 refusal does not name the issue:
$p_out"
fi

# ===========================================================================
case_begin 3-foreign-issue "a stamp for ANOTHER issue is refused, naming the axis"
# ===========================================================================
L3=$(lane lane3)
run "$L3" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 9999 >/dev/null 2>&1
i_out=$(run "$L3" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); i_rc=$?
if [ "$i_rc" -ne 0 ] && [ "$(verdict_of "$i_out")" = FOREIGN-ISSUE ] \
   && printf '%s\n' "$i_out" | grep -q 'axis=issue'; then
  ok "foreign issue -> FOREIGN-ISSUE(rc=$i_rc) naming axis=issue"
else
  bad "foreign issue not refused on the issue axis: rc=$i_rc
$i_out"
fi

# ===========================================================================
case_begin 4-foreign-machine "a stamp written on ANOTHER machine is refused, naming the axis"
# ===========================================================================
L4=$(lane lane4)
run "$L4" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
m_out=$(run "$L4" CLAIM_MACHINE=boxB "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); m_rc=$?
if [ "$m_rc" -ne 0 ] && [ "$(verdict_of "$m_out")" = FOREIGN-MACHINE ] \
   && printf '%s\n' "$m_out" | grep -q 'axis=machine'; then
  ok "foreign machine -> FOREIGN-MACHINE(rc=$m_rc) naming axis=machine"
else
  bad "foreign machine not refused on the machine axis: rc=$m_rc
$m_out"
fi

# ===========================================================================
case_begin 5-foreign-worktree "a stamp copied from ANOTHER worktree is refused, naming the axis"
# ===========================================================================
L5=$(lane lane5)
L5B=$(lane lane5b)
run "$L5" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
cp "$L5/$MARKER" "$L5B/$MARKER"
wt_out=$(run "$L5B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); wt_rc=$?
if [ "$wt_rc" -ne 0 ] && [ "$(verdict_of "$wt_out")" = FOREIGN-WORKTREE ] \
   && printf '%s\n' "$wt_out" | grep -q 'axis=worktree'; then
  ok "foreign worktree -> FOREIGN-WORKTREE(rc=$wt_rc) naming axis=worktree"
else
  bad "foreign worktree not refused on the worktree axis: rc=$wt_rc
$wt_out"
fi

# ===========================================================================
case_begin 6-session-gone-adoptable "session differs + writer provably GONE -> non-zero ADOPTABLE, then adopt succeeds"
# ===========================================================================
L6=$(lane lane6)
sleep 30 &
gone_pid=$!
run "$L6" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$gone_pid" -- write 3822 --stage implement >/dev/null 2>&1
kill "$gone_pid" 2>/dev/null
wait "$gone_pid" 2>/dev/null
g_out=$(run "$L6" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); g_rc=$?
if [ "$g_rc" -ne 0 ] && [ "$(verdict_of "$g_out")" = ADOPTABLE ]; then
  ok "AC2: writer gone -> verify is NON-ZERO ADOPTABLE(rc=$g_rc); an explicit adopt gesture is required"
else
  bad "writer-gone did not yield a non-zero ADOPTABLE verdict: rc=$g_rc
$g_out"
fi
a_out=$(run "$L6" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason writer-process-gone-cron-reinvoke); a_rc=$?
if [ "$a_rc" -eq 0 ] && [ "$(verdict_of "$a_out")" = ADOPTED ]; then
  ok "adopt over a provably-gone writer -> ADOPTED(0)"
else
  bad "adopt over a gone writer failed: rc=$a_rc
$a_out"
fi
if grep -q "^prior-session: $SESS_A\$" "$L6/$MARKER" \
   && grep -q "^session: $SESS_B\$" "$L6/$MARKER" \
   && grep -q '^adopt-reason: writer-process-gone-cron-reinvoke$' "$L6/$MARKER"; then
  ok "the rewritten stamp records the PRIOR session, the new session and the adopt reason"
else
  bad "adopt did not record prior session / reason:
$(cat "$L6/$MARKER")"
fi
av_out=$(run "$L6" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); av_rc=$?
if [ "$av_rc" -eq 0 ] && [ "$(verdict_of "$av_out")" = OWNED ]; then
  ok "after adopt, verify from the adopting session is OWNED(0)"
else
  bad "post-adopt verify not OWNED: rc=$av_rc
$av_out"
fi

# ===========================================================================
case_begin 7-session-live-peer "session differs + writer provably ALIVE -> refused; adopt ALSO refuses"
# ===========================================================================
L7=$(lane lane7)
sleep 300 &
live_pid=$!
run "$L7" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$live_pid" -- write 3822 >/dev/null 2>&1
lp_out=$(run "$L7" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); lp_rc=$?
la_out=$(run "$L7" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason peer-looks-idle-to-me); la_rc=$?
kill "$live_pid" 2>/dev/null
wait "$live_pid" 2>/dev/null
if [ "$lp_rc" -ne 0 ] && [ "$(verdict_of "$lp_out")" = LIVE-PEER ]; then
  ok "a LIVE recorded writer -> LIVE-PEER refusal (rc=$lp_rc)"
else
  bad "live writer was not refused as LIVE-PEER: rc=$lp_rc
$lp_out"
fi
if [ "$la_rc" -ne 0 ] && [ "$(verdict_of "$la_out")" = LIVE-PEER ]; then
  ok "adopt ALSO refuses over a live peer (rc=$la_rc) — adopt is not a mute button for the guard"
else
  bad "adopt over a LIVE peer was not refused: rc=$la_rc
$la_out"
fi

# ===========================================================================
case_begin 8-pid-unrecordable-unknown "CLAUDE_PID unset at write time -> liveness UNKNOWN, refused; adopt refuses"
# ===========================================================================
# The false-permissive this issue exists to close: falling back to $$ would record
# the transient bash that exits immediately, so a LIVE peer would read as DEAD and
# be adoptable. Unrecordable pid MUST mean liveness UNKNOWN, never DEAD.
L8=$(lane lane8)
run "$L8" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" -- write 3822 >/dev/null 2>&1
u_out=$(run "$L8" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); u_rc=$?
ua_out=$(run "$L8" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason no-pid-recorded-so-cannot-tell); ua_rc=$?
if [ "$u_rc" -ne 0 ] && [ "$(verdict_of "$u_out")" = LIVENESS-UNKNOWN ]; then
  ok "unrecordable writer pid -> LIVENESS-UNKNOWN refusal (rc=$u_rc), a token DISTINCT from LIVE-PEER"
else
  bad "unrecordable pid did not yield LIVENESS-UNKNOWN: rc=$u_rc
$u_out"
fi
if [ "$ua_rc" -ne 0 ] && [ "$(verdict_of "$ua_out")" = LIVENESS-UNKNOWN ]; then
  ok "adopt refuses on the UNKNOWN branch too — a positive verdict needs an affirmative measurement"
else
  bad "adopt on the UNKNOWN branch was not refused: rc=$ua_rc
$ua_out"
fi
if [ -f "$L8/$MARKER" ] && ! grep -qE "^session-pid: $$\$" "$L8/$MARKER"; then
  ok "NON-VACUITY: the writer did NOT fall back to its own \$\$ as the session pid"
else
  bad "the writer recorded its own transient \$\$ as the session pid:
$(cat "$L8/$MARKER")"
fi

# ===========================================================================
case_begin 9-writer-refuses-sentinel-body "the writer REFUSES a body carrying a sentinel at column zero"
# ===========================================================================
L9=$(lane lane9)
body="$T/body9.md"
sentinel=$(sed -n 's/^STAMP_BEGIN=.\(.*\).$/\1/p' "$DS" | head -1)
{
  printf 'plan notes\n'
  printf '%s\n' "$sentinel"
  printf 'issue: 1\n'
} >"$body"
s_out=$(run "$L9" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --body-file "$body"); s_rc=$?
if [ -n "$sentinel" ] && [ "$s_rc" -ne 0 ] && [ ! -f "$L9/$MARKER" ]; then
  ok "a body line reproducing the stamp sentinel at column zero is REFUSED (rc=$s_rc), not escaped, and nothing is written"
else
  bad "sentinel-bearing body was not refused: rc=$s_rc sentinel='$sentinel' marker-exists=$([ -f "$L9/$MARKER" ] && echo yes || echo no)
$s_out"
fi
body_ok="$T/body9ok.md"
printf 'plan notes only\n' >"$body_ok"
s2_out=$(run "$L9" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --body-file "$body_ok"); s2_rc=$?
if [ "$s2_rc" -eq 0 ] && grep -q '^plan notes only$' "$L9/$MARKER"; then
  ok "NON-VACUITY: an ordinary body IS accepted and lands in the marker"
else
  bad "an ordinary body was rejected: rc=$s2_rc
$s2_out"
fi

# ===========================================================================
case_begin 10-reader-refuses-duplicate-sentinel "the reader REFUSES a file with a duplicate sentinel"
# ===========================================================================
L10=$(lane lane10)
run "$L10" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
{
  printf '%s\n' "$sentinel"
  printf 'issue: 3822\n'
} >>"$L10/$MARKER"
d_out=$(run "$L10" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); d_rc=$?
if [ "$d_rc" -ne 0 ] && [ "$(verdict_of "$d_out")" = DUPLICATE-SENTINEL ]; then
  ok "a hand-edited second sentinel at column zero is its OWN named refusal (rc=$d_rc)"
else
  bad "duplicate sentinel was not refused by name: rc=$d_rc
$d_out"
fi

# ===========================================================================
case_begin 11-machine-agrees-with-claim-sh "machine identity is the SAME notion claim.sh records (anti-drift pin)"
# ===========================================================================
L11=$(lane lane11)
# Extract claim.sh's OWN machine resolution from the shipped script and run it in
# this environment: the agreement is measured, never asserted by care.
cm_body="$T/claim-machine.sh"
{
  sed -n '/^sanitize_field()/,/^}/p' "$CLAIM"
  sed -n '/^this_machine()/,/^}/p' "$CLAIM"
  printf 'this_machine\n'
} >"$cm_body"
claim_machine=$(CLAIM_MACHINE='build box' bash "$cm_body")
run "$L11" "CLAIM_MACHINE=build box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
ds_machine=$(sed -n 's/^machine: //p' "$L11/$MARKER" | head -1)
if [ -n "$claim_machine" ] && [ "$claim_machine" = "$ds_machine" ]; then
  ok "machine agrees with claim.sh's recorded machine in the same environment ('$ds_machine')"
else
  bad "machine identity drifted from claim.sh: claim.sh='$claim_machine' drive-issue-state='$ds_machine'"
fi
if [ "$ds_machine" != "build box" ]; then
  ok "NON-VACUITY: the value is SANITIZED (a space-bearing CLAIM_MACHINE cannot forge a stamp line)"
else
  bad "machine value was recorded unsanitized: '$ds_machine'"
fi

# ===========================================================================
case_begin 12-placeholder-reason-refused "adopt refuses placeholder --reason values as a USAGE error"
# ===========================================================================
L12=$(lane lane12)
sleep 30 &
p12=$!
run "$L12" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p12" -- write 3822 >/dev/null 2>&1
kill "$p12" 2>/dev/null; wait "$p12" 2>/dev/null
ph_fail=0
for r in '' '   ' 'why' 'TODO' 'tbd' '<why>' 'resume-legacy:<branch>'; do
  o=$(run "$L12" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason "$r" 2>&1); rc=$?
  if [ "$rc" -ne 64 ]; then
    ph_fail=$((ph_fail + 1))
    printf 'note   placeholder reason %q was not a usage error: rc=%s\n' "$r" "$rc"
  fi
done
o=$(run "$L12" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 2>&1); rc_noreason=$?
if [ "$ph_fail" -eq 0 ] && [ "$rc_noreason" -eq 64 ]; then
  ok "empty / whitespace / placeholder / unsubstituted-template reasons AND a missing --reason are exit 64, never a silent 'unspecified'"
else
  bad "placeholder --reason gate leaks: failures=$ph_fail no-reason-rc=$rc_noreason"
fi
o=$(run "$L12" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason 'cron re-invoke, writer pid gone'); rc=$?
if [ "$rc" -eq 0 ] && [ "$(verdict_of "$o")" = ADOPTED ]; then
  ok "NON-VACUITY: a real reason IS accepted"
else
  bad "a real reason was rejected: rc=$rc
$o"
fi

# ===========================================================================
case_begin 13-write-over-foreign-refuses "write over a foreign marker refuses without an adopt gesture"
# ===========================================================================
L13=$(lane lane13)
sleep 300 &
p13=$!
run "$L13" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p13" -- write 3822 --stage peer-plan >/dev/null 2>&1
ow_out=$(run "$L13" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- write 3822 --stage my-plan); ow_rc=$?
ow_m_out=$(run "$L13" CLAIM_MACHINE=boxB "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p13" -- write 3822 --stage my-plan); ow_m_rc=$?
kill "$p13" 2>/dev/null; wait "$p13" 2>/dev/null
if [ "$ow_rc" -ne 0 ] && [ "$(verdict_of "$ow_out")" = LIVE-PEER ] \
   && [ "$ow_m_rc" -ne 0 ] && [ "$(verdict_of "$ow_m_out")" = FOREIGN-MACHINE ] \
   && grep -q '^stage: peer-plan$' "$L13/$MARKER"; then
  ok "write refuses over a live peer's plan AND over a foreign machine's, leaving the recorded plan intact"
else
  bad "write overwrote or misjudged a foreign marker: session-rc=$ow_rc machine-rc=$ow_m_rc
$ow_out
$ow_m_out
$(cat "$L13/$MARKER")"
fi

# ===========================================================================
case_begin 14-absent-is-distinct "no marker at all is ABSENT — textually and numerically distinct from a refusal"
# ===========================================================================
L14=$(lane lane14)
ab_out=$(run "$L14" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); ab_rc=$?
ab_v=$(verdict_of "$ab_out")
if [ "$ab_v" = ABSENT ] && [ "$ab_rc" -ne 0 ] && [ "$ab_rc" -ne 1 ]; then
  ok "ABSENT is its own verdict token and its own exit code ($ab_rc) — a fresh start, not a refusal"
else
  bad "absent marker not reported distinctly: rc=$ab_rc verdict=$ab_v
$ab_out"
fi
# A fresh write in that lane must SUCCEED: ABSENT is legitimate.
fw_out=$(run "$L14" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822); fw_rc=$?
if [ "$fw_rc" -eq 0 ]; then
  ok "NON-VACUITY: a fresh write into an unmarked lane succeeds"
else
  bad "fresh write into an unmarked lane failed: rc=$fw_rc
$fw_out"
fi

# ===========================================================================
case_begin 15-pid-reuse-recognised "a recorded start window disjoint from the live pid's is NOT read as ALIVE"
# ===========================================================================
L15=$(lane lane15)
sleep 300 &
p15=$!
run "$L15" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p15" -- write 3822 >/dev/null 2>&1
# Rewrite the recorded start window into the distant past: the pid is LIVE but it
# cannot be the process that stamped this marker (pid reuse).
# `sed -i` is NOT portable (BSD/macOS requires an argument to -i), so rewrite via a temp.
sed -e 's/^session-pid-start-earliest: .*/session-pid-start-earliest: 1000000000/' \
    -e 's/^session-pid-start-latest: .*/session-pid-start-latest: 1000000002/' \
    "$L15/$MARKER" >"$T/marker15" && mv "$T/marker15" "$L15/$MARKER"
ru_out=$(run "$L15" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); ru_rc=$?
kill "$p15" 2>/dev/null; wait "$p15" 2>/dev/null
ru_v=$(verdict_of "$ru_out")
if [ "$ru_rc" -ne 0 ] && [ "$ru_v" != LIVE-PEER ] && [ "$ru_v" != OWNED ] && verdict_in_set "$ru_v"; then
  ok "a reused pid is not credited to the recorded writer (verdict=$ru_v, rc=$ru_rc)"
else
  bad "pid reuse was read as the recorded writer: rc=$ru_rc verdict=$ru_v
$ru_out"
fi

# ===========================================================================
case_begin 16-closed-verdict-grammar "every verdict token emitted in this run is in the CLOSED set; show + help behave"
# ===========================================================================
grammar_fail=0
for v in "$(verdict_of "$w_out")" "$(verdict_of "$v_out")" "$(verdict_of "$p_out")" \
         "$(verdict_of "$i_out")" "$(verdict_of "$m_out")" "$(verdict_of "$wt_out")" \
         "$(verdict_of "$g_out")" "$(verdict_of "$a_out")" "$(verdict_of "$lp_out")" \
         "$(verdict_of "$la_out")" "$(verdict_of "$u_out")" "$(verdict_of "$ua_out")" \
         "$(verdict_of "$d_out")" "$(verdict_of "$ab_out")" "$(verdict_of "$ow_out")"; do
  verdict_in_set "$v" || { grammar_fail=$((grammar_fail + 1)); printf 'note   out-of-grammar verdict: %q\n' "$v"; }
done
if [ "$grammar_fail" -eq 0 ]; then
  ok "all 15 sampled verdict tokens are members of the closed grammar"
else
  bad "$grammar_fail verdict tokens outside the closed grammar"
fi
sh_out=$(run "$L1" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822); sh_rc=$?
if [ "$sh_rc" -eq 0 ] && printf '%s\n' "$sh_out" | grep -q 'machine=boxA' \
   && printf '%s\n' "$sh_out" | grep -q "session=$SESS_A" && all_lines_anchored "$sh_out"; then
  ok "show prints the recorded stamp fields, anchored"
else
  bad "show output wrong: rc=$sh_rc
$sh_out"
fi
h_out=$(run "$L1" -- --help); h_rc=$?
if [ "$h_rc" -eq 0 ] && printf '%s\n' "$h_out" | grep -q 'ADOPTABLE' \
   && printf '%s\n' "$h_out" | grep -q 'EXIT CODES'; then
  ok "--help is authoritative: it documents the verdict tokens and the exit codes"
else
  bad "--help does not document the contract: rc=$h_rc"
fi
bad_out=$(run "$L1" -- frobnicate 3822 2>&1); bad_rc=$?
if [ "$bad_rc" -eq 64 ]; then
  ok "an unknown subcommand is a usage error (64)"
else
  bad "unknown subcommand rc=$bad_rc"
fi

# ===========================================================================
case_begin 17-write-failure-emits-a-verdict "an I/O failure emits ERROR on STDOUT — the verdict is never captured into a variable"
# ===========================================================================
# Regression pin for a real defect in the first cut: the writer printed its path on
# stdout and was called inside `$( )`, so a `refuse` inside it exited only the SUBSHELL
# and its verdict line was CAPTURED into the caller's variable — the run would emit no
# verdict at all and put a verdict string inside a path. Every emit site must be in the
# main shell, which this case measures rather than asserts.
L18=$(lane lane18)
wf_probe="unwritable-worktree"
if [ "$(id -u)" -eq 0 ]; then
  # ROOT BYPASSES DIRECTORY PERMISSIONS, so the chmod probe would silently SUCCEED and the
  # case would pass having measured nothing. Under root the probe is DECLARED unavailable
  # and replaced by the other route into the same emit path: a marker path that is not a
  # regular file. Narrower coverage, NAMED rather than hidden.
  wf_probe="non-regular-marker(root: permission probe unavailable)"
  mkdir -p "$L18/$MARKER"
else
  chmod 500 "$L18"
fi
wf_out=$(run "$L18" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>/dev/null); wf_rc=$?
chmod 700 "$L18" 2>/dev/null || true
if [ "$wf_rc" -ne 0 ] && [ "$(verdict_of "$wf_out")" = ERROR ] && all_lines_anchored "$wf_out"; then
  ok "an I/O failure ($wf_probe) yields ERROR(rc=$wf_rc) on stdout, anchored — not a swallowed verdict"
else
  bad "write failure did not surface a verdict ($wf_probe): rc=$wf_rc verdict=$(verdict_of "$wf_out")
$wf_out"
fi

# ===========================================================================
case_begin 18-control-chars-stay-anchored "a hand-edited stamp value carrying control characters cannot break the output anchor"
# ===========================================================================
# Contract (b), and the load-bearing half of the anchor: a path (or any hand-edited field)
# may contain a NEWLINE or an escape sequence, and printed verbatim it emits a line with no
# DRIVE-STATE: prefix — which every consumer and every case above rests on.
L17=$(lane lane17)
run "$L17" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
python3 - "$L17/$MARKER" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace('machine: boxA', 'machine: box\x1b[31mA\x07\x0b')
open(p, 'w').write(s)
PYEOF
cc_out=$(run "$L17" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822 2>&1); cc_rc=$?
if [ "$cc_rc" -ne 0 ] && [ "$(verdict_of "$cc_out")" = FOREIGN-MACHINE ] && all_lines_anchored "$cc_out" \
   && ! printf '%s' "$cc_out" | grep -q $'\x1b'; then
  ok "control characters in a recorded value are masked for display; every line stays anchored"
else
  bad "control-character value broke the anchor: rc=$cc_rc
$(printf '%s' "$cc_out" | cat -v)"
fi

# ===========================================================================
case_begin 19-control-char-worktree-refused "a worktree path that cannot be recorded on one line is REFUSED, not recorded lossily"
# ===========================================================================
# The worktree axis is stored VERBATIM (a path must compare EXACTLY; sanitizing would alias
# '/a b' onto '/a-b' and let two lanes verify each other's markers). The filesystem PERMITS
# a NEWLINE in a directory name, and such a path cannot be recorded on one line at all — so
# the writer refuses rather than recording an identity it would later mis-compare.
nl_dir="$T/lane$(printf '20\nnewline')"
if mkdir -p "$nl_dir" 2>/dev/null; then
  nl_out=$(run "$nl_dir" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); nl_rc=$?
  if [ "$nl_rc" -ne 0 ] && [ "$(verdict_of "$nl_out")" = ERROR ] \
     && all_lines_anchored "$nl_out" && [ ! -f "$nl_dir/$MARKER" ]; then
    ok "a newline-bearing worktree path is refused (rc=$nl_rc) with an anchored ERROR and nothing written"
  else
    bad "newline-bearing worktree path was not refused: rc=$nl_rc
$(printf '%s' "$nl_out" | cat -v)"
  fi
else
  bad "could not create a newline-bearing directory, so the worktree-recordability refusal was NOT measured (this is a gap, not a pass)"
fi

# ===========================================================================
case_begin 20-same-process-is-owned "the SAME process owns its own marker even with no session id — no false LIVE-PEER"
# ===========================================================================
# A guard that reds on correct input is the guard agents learn to waive. With CLAUDE_PID
# set but CLAUDE_CODE_SESSION_ID UNSET the session axis is UNMEASURED, and before this
# branch existed the writer's own next command was refused as a LIVE-PEER against itself.
# Sameness is measured affirmatively (pid identity + intersecting start window), which is
# strictly stronger than the session-id string it stands in for.
L20=$(lane lane20)
sp_w=$(run "$L20" CLAIM_MACHINE=boxA "CLAUDE_PID=$$" -- write 3822 --stage implement); sp_wrc=$?
sp_v=$(run "$L20" CLAIM_MACHINE=boxA "CLAUDE_PID=$$" -- verify 3822); sp_vrc=$?
if [ "$sp_wrc" -eq 0 ] && [ "$sp_vrc" -eq 0 ] && [ "$(verdict_of "$sp_v")" = OWNED ]; then
  ok "an unrecorded session id does not make a process a peer to itself (write 0, verify OWNED)"
else
  bad "the same process was refused its own marker: write-rc=$sp_wrc verify-rc=$sp_vrc verdict=$(verdict_of "$sp_v")
$sp_v"
fi
# NON-VACUITY: the branch is keyed on PID IDENTITY, not on 'the session id was unrecorded'.
# A DIFFERENT live pid with an unrecorded session id must still be a LIVE-PEER.
sleep 300 &
sp_peer=$!
L20B=$(lane lane20b)
run "$L20B" CLAIM_MACHINE=boxA "CLAUDE_PID=$sp_peer" -- write 3822 >/dev/null 2>&1
sp_p=$(run "$L20B" CLAIM_MACHINE=boxA "CLAUDE_PID=$$" -- verify 3822); sp_prc=$?
kill "$sp_peer" 2>/dev/null; wait "$sp_peer" 2>/dev/null
if [ "$sp_prc" -ne 0 ] && [ "$(verdict_of "$sp_p")" = LIVE-PEER ]; then
  ok "NON-VACUITY: a DIFFERENT live pid is still a LIVE-PEER when neither side records a session id"
else
  bad "an unrecorded session id let a foreign live pid through: rc=$sp_prc verdict=$(verdict_of "$sp_p")
$sp_p"
fi

# ===========================================================================
case_begin 21-case-floor "CASE FLOOR: a silently shrunken suite must RED, not green (#3544)"
# ===========================================================================
REQUIRED_CASES="1-write-verify-owned 2-ac3-unstamped-prose-refused 3-foreign-issue 4-foreign-machine
5-foreign-worktree 6-session-gone-adoptable 7-session-live-peer 8-pid-unrecordable-unknown
9-writer-refuses-sentinel-body 10-reader-refuses-duplicate-sentinel 11-machine-agrees-with-claim-sh
12-placeholder-reason-refused 13-write-over-foreign-refuses 14-absent-is-distinct
15-pid-reuse-recognised 16-closed-verdict-grammar 17-write-failure-emits-a-verdict
18-control-chars-stay-anchored 19-control-char-worktree-refused
20-same-process-is-owned 21-case-floor"
CASE_FLOOR=21
executed=0
for _c in $CASES; do executed=$((executed + 1)); done
missing=""
for req in $REQUIRED_CASES; do
  case " $CASES " in *" $req "*) : ;; *) missing="$missing $req" ;; esac
done
if [ "$executed" -ge "$CASE_FLOOR" ] && [ -z "$missing" ]; then
  ok "$executed cases executed (floor $CASE_FLOOR) and every required case name is present"
else
  bad "case floor breached: executed=$executed floor=$CASE_FLOOR missing:$missing"
fi
if [ "$PASS" -ge 30 ]; then
  ok "assertion floor: $PASS assertions passed (>= 30)"
else
  bad "assertion floor breached: only $PASS assertions passed"
fi

# ===========================================================================
echo
echo "==== DRIVE-ISSUE-STATE TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
