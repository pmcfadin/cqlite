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

# verdict_count <output> — how many `verdict ` lines the run emitted. Contract (c) says
# EXACTLY ONE, so both zero (a consumer's `case` falls through every arm) and two (a consumer
# reads whichever its parser picks first) are failures.
verdict_count() { printf '%s\n' "$1" | grep -c '^DRIVE-STATE: verdict ' || true; }

# The CLOSED verdict token set (the script's own grammar). An unrecognised token is
# a refusal, so the test pins the set rather than accepting whatever is printed.
VERDICT_SET="OWNED WRITTEN ADOPTED SHOWN ABSENT UNSTAMPED MALFORMED DUPLICATE-SENTINEL FOREIGN-ISSUE FOREIGN-MACHINE FOREIGN-WORKTREE ADOPTABLE LIVE-PEER LIVENESS-UNKNOWN ERROR USAGE"
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

# ENVIRONMENT PRECONDITIONS, asserted rather than assumed. The case FLOOR counts cases
# EXECUTED, which does not prove each one reached its subject — so anything this suite needs
# from the host is named here and FAILS loudly if absent, instead of surfacing as a puzzling
# verdict mismatch ten cases later (or, worse, as a case that quietly proves nothing).
if command -v flock >/dev/null 2>&1; then
  ok "precondition: flock is available, so the MUTATING subcommands can run at all"
else
  bad "precondition: flock is MISSING — write/adopt refuse by design without it, so most cases below cannot reach their subject. NOTHING here is measured."
fi

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
sentinel_end=$(sed -n 's/^STAMP_END=.\(.*\).$/\1/p' "$DS" | head -1)
# BOTH sentinels are load-bearing for cases 9, 10, 21 and 22. An empty extraction would make
# `grep -vFx ""` strip blank lines instead of the end sentinel, so the state under test would
# not be the state named — a case passing for the wrong reason. Asserted, not assumed.
if [ -n "$sentinel" ] && [ -n "$sentinel_end" ] && [ "$sentinel" != "$sentinel_end" ]; then
  ok "both stamp sentinels were extracted from the shipped script and are distinct"
else
  bad "sentinel extraction failed (begin='$sentinel' end='$sentinel_end') — the cases that plant them measure nothing"
fi
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
# Extract claim.sh's OWN machine resolution from the shipped script AND this script's own,
# then run BOTH in one environment over a TABLE of inputs: the agreement is measured, never
# asserted by care.
#
# WHY THE COMPARISON MOVED FROM THE MARKER TO THE FUNCTIONS (roborev job 37 J1): the USE SITE
# now REFUSES a machine identity that does not survive sanitization unchanged, so a
# space-bearing CLAIM_MACHINE never reaches the marker and the old single end-to-end value
# could no longer be read out of it. The SANITIZER itself is untouched, and this pins strictly
# MORE of it than one recorded value did — every row below compares claim.sh's answer with this
# script's for the same input, INCLUDING the lossy and unrecordable ones. The end-to-end leg is
# kept below, on a canonical value.
cm_body="$T/claim-machine.sh"
{
  sed -n '/^sanitize_field()/,/^}/p' "$CLAIM"
  sed -n '/^this_machine()/,/^}/p' "$CLAIM"
  printf 'this_machine\n'
} >"$cm_body"
# This script's `this_machine` is a ONE-LINE function, so a `/^}/` range would swallow the rest
# of the file: the two MULTI-LINE definitions it delegates to are extracted and driven directly.
ds_body="$T/ds-machine.sh"
{
  sed -n '/^sanitize_field()/,/^}/p' "$DS"
  sed -n '/^resolve_machine_axis()/,/^}/p' "$DS"
  printf 'MACHINE_AXIS_VALUE=; MACHINE_AXIS_STATE=; MACHINE_AXIS_RAW=\n'
  printf 'resolve_machine_axis\n'
  printf 'printf "%%s %%s\\n" "$MACHINE_AXIS_VALUE" "$MACHINE_AXIS_STATE"\n'
} >"$ds_body"
long120=$(printf 'a%.0s' $(seq 1 120))
long121a="${long120}X"
long121b="${long120}Y"
agree_fail=0
# input <US> expected-state
for row in "boxA:ok" "build box:lossy" "box@A:lossy" "$long120:ok" "$long121a:lossy" "***:unrecordable"; do
  in_v="${row%:*}"; want_state="${row##*:}"
  cm_v=$(CLAIM_MACHINE="$in_v" bash "$cm_body")
  ds_pair=$(CLAIM_MACHINE="$in_v" bash "$ds_body")
  ds_v="${ds_pair% *}"; ds_state="${ds_pair##* }"
  if [ "$cm_v" != "$ds_v" ]; then
    agree_fail=$((agree_fail + 1))
    printf 'note   sanitizer drift for %q: claim.sh=%q drive-issue-state=%q\n' "$in_v" "$cm_v" "$ds_v"
  fi
  if [ "$ds_state" != "$want_state" ]; then
    agree_fail=$((agree_fail + 1))
    printf 'note   axis state for %q: got=%s want=%s\n' "$in_v" "$ds_state" "$want_state"
  fi
done
if [ "$agree_fail" -eq 0 ]; then
  ok "machine sanitizer agrees with claim.sh's on every table row, and each row's measurability state is classified as expected"
else
  bad "machine identity drifted from claim.sh (or was misclassified) on $agree_fail check(s)"
fi
# NON-VACUITY of the table: the sanitizer really is LOSSY on these inputs (which is WHY the use
# site refuses them), and the 120-character cut really does collide two distinct names.
cm_space=$(CLAIM_MACHINE='build box' bash "$cm_body")
cm_l121a=$(CLAIM_MACHINE="$long121a" bash "$cm_body")
cm_l121b=$(CLAIM_MACHINE="$long121b" bash "$cm_body")
if [ "$cm_space" = 'build-box' ] && [ "$cm_space" != 'build box' ] \
   && [ -n "$cm_l121a" ] && [ "$cm_l121a" = "$cm_l121b" ]; then
  ok "NON-VACUITY: the shared sanitizer maps 'build box' onto 'build-box' AND collapses two names differing only past 120 chars onto ONE token — the collisions the use site now refuses are real"
else
  bad "the sanitizer did not behave as the table assumes: space='$cm_space' l121a='$cm_l121a' l121b='$cm_l121b'"
fi
# END-TO-END, on a CANONICAL value: what the marker records IS what claim.sh would record.
cm_canon=$(CLAIM_MACHINE='build-box' bash "$cm_body")
run "$L11" "CLAIM_MACHINE=build-box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
ds_machine=$(sed -n 's/^machine: //p' "$L11/$MARKER" | head -1)
if [ -n "$cm_canon" ] && [ "$cm_canon" = "$ds_machine" ]; then
  ok "end-to-end: the value RECORDED in the marker equals claim.sh's this_machine in the same environment ('$ds_machine')"
else
  bad "recorded machine drifted from claim.sh: claim.sh='$cm_canon' marker='$ds_machine'"
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
# THE PROBE ABOVE IS UID-DEPENDENT, so it is not the only one. A directory permission is
# bypassed by root and, since the flock pre-probe now refuses an unwritable worktree before
# `write_marker` is entered, that probe no longer reaches the WRITE path it is named for on
# ANY uid. This one does, on every uid: a PATH-shimmed failing `mktemp` fails inside
# `write_marker` itself, which is where the swallowed-verdict defect lived.
L17B=$(lane lane17b)
wf_bin="$T/fakebin17"; mkdir -p "$wf_bin"
printf '#!/bin/sh\nexit 1\n' >"$wf_bin/mktemp"; chmod +x "$wf_bin/mktemp"
wf2_out=$(run "$L17B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
  "PATH=$wf_bin:$PATH" -- write 3822 2>/dev/null); wf2_rc=$?
if [ "$wf2_rc" -ne 0 ] && [ "$(verdict_of "$wf2_out")" = ERROR ] \
   && printf '%s\n' "$wf2_out" | grep -q 'temporary file' \
   && all_lines_anchored "$wf2_out" && [ ! -f "$L17B/$MARKER" ]; then
  ok "a failing mktemp inside write_marker yields ERROR(rc=$wf2_rc) naming the cause, on stdout, with nothing written — the path the uid-dependent probe cannot reach"
else
  bad "the injected mktemp failure did not reach write_marker's error path: rc=$wf2_rc verdict=$(verdict_of "$wf2_out")
$wf2_out"
fi

# ===========================================================================
case_begin 18-control-chars-stay-anchored "a hand-edited stamp value carrying control characters cannot break the output anchor"
# ===========================================================================
# Contract (b), and the load-bearing half of the anchor: a path (or any hand-edited field)
# may contain a NEWLINE or an escape sequence, and printed verbatim it emits a line with no
# DRIVE-STATE: prefix — which every consumer and every case above rests on.
L17=$(lane lane17)
run "$L17" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
# Built with printf + awk rather than python3: a missing interpreter would leave the marker
# UNEDITED, the verdict would be OWNED, and this case would report a failure whose cause is
# nowhere in its message. No interpreter, no dependency.
cc_val=$(printf 'box\033[31mA\007\013')
awk -v new="machine: $cc_val" '/^machine: /{print new; next} {print}' "$L17/$MARKER" >"$T/m17cc" \
  && mv "$T/m17cc" "$L17/$MARKER"
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
case_begin 21-write-over-unstamped-migrates "MIGRATION: write SUCCEEDS over an UNSTAMPED marker, discards its body, and announces it"
# ===========================================================================
# The dead letter this case exists for: `verify` refused an UNSTAMPED marker (correctly)
# and so did `write` and `adopt`, while the refusal text named `write` as the remedy — so on
# rollout EVERY existing lane, all of which hold an unstamped marker by definition, had NO
# route forward. An unstamped marker asserts no ownership, so refusing to replace it
# protects no identifiable party.
L21=$(lane lane21)
cat >"$L21/$MARKER" <<'LEGACY'
# drive-issue state for #3822 (hand-written, pre-stamp)
- stage: implement
- note: DISTINCTIVE_LEGACY_BODY_MARKER must not survive the restamp
LEGACY
mig_v=$(run "$L21" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); mig_vrc=$?
mig_w=$(run "$L21" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement); mig_wrc=$?
mig_v2=$(run "$L21" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); mig_v2rc=$?
if [ "$mig_vrc" -eq 8 ] && [ "$(verdict_of "$mig_v")" = UNSTAMPED ] \
   && [ "$mig_wrc" -eq 0 ] && [ "$(verdict_of "$mig_w")" = WRITTEN ] \
   && [ "$mig_v2rc" -eq 0 ] && [ "$(verdict_of "$mig_v2")" = OWNED ]; then
  ok "the end-to-end migration path works: verify UNSTAMPED(8) -> write WRITTEN(0) -> verify OWNED(0)"
else
  bad "migration path broken: verify=$mig_vrc/$(verdict_of "$mig_v") write=$mig_wrc/$(verdict_of "$mig_w") reverify=$mig_v2rc/$(verdict_of "$mig_v2")
$mig_w"
fi
# The old body must be provably GONE — asserted on a DISTINCTIVE STRING, not on a length,
# because a length can coincide while the foreign plan survives.
if ! grep -q 'DISTINCTIVE_LEGACY_BODY_MARKER' "$L21/$MARKER"; then
  ok "the unstamped body is provably ABSENT from the restamped marker (a foreign plan is never carried forward)"
else
  bad "the unstamped body SURVIVED the restamp:
$(cat "$L21/$MARKER")"
fi
if printf '%s\n' "$mig_w" | grep -q 'DISCARDED its body' && all_lines_anchored "$mig_w"; then
  ok "the discard is ANNOUNCED on an anchored verdict-detail line (a quiet overwrite of someone's notes is not acceptable)"
else
  bad "the discard was not announced:
$mig_w"
fi
# NON-VACUITY: the exception is for UNSTAMPED ONLY. A marker that CLAIMS an identity which
# merely cannot be READ may be a live peer's, so write must still refuse it.
L21B=$(lane lane21b)
run "$L21B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
grep -vFx -- "$sentinel_end" "$L21B/$MARKER" >"$T/m21b" && mv "$T/m21b" "$L21B/$MARKER"
mal_w=$(run "$L21B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822); mal_wrc=$?
if [ "$mal_wrc" -ne 0 ] && [ "$(verdict_of "$mal_w")" = MALFORMED ]; then
  ok "NON-VACUITY: write still REFUSES a MALFORMED marker — the exception is scoped to UNSTAMPED"
else
  bad "write overwrote a marker whose stamp merely could not be READ: rc=$mal_wrc verdict=$(verdict_of "$mal_w")
$mal_w"
fi

# ===========================================================================
case_begin 22-no-dead-letter-remedies "DERIVED: no refusal may name a subcommand of this script that returns the same refusal"
# ===========================================================================
# Generalized from the UNSTAMPED dead letter. Derived from the verdict set rather than
# hand-copied per verdict, so a NEW refusal verdict cannot join without being covered: the
# table below must account for every non-success token in VERDICT_SET, and each state's
# refusal text is SCANNED for `drive-issue-state.sh <sub>` mentions which are then INVOKED
# on that same state. A verdict that names no mechanical remedy is fine (FOREIGN-* say
# "escalate"); naming a command that refuses identically is the defect.
#
# THE RULE IS DELIBERATELY STRICT ABOUT TWO-STEP REMEDIES: a text may not name a
# subcommand that only works AFTER the reader does something else first (e.g. "move the
# file aside, then `write`"), because the reader of these texts runs printed commands
# LITERALLY. Such a remedy must describe the STATE CHANGE and let the normal path follow —
# which is how MALFORMED/DUPLICATE-SENTINEL are worded. This case caught two such texts
# the moment it was written, in the same round as the UNSTAMPED dead letter itself.
DL_STATES="absent unstamped malformed displaced-sentinel duplicate-sentinel foreign-issue foreign-machine foreign-worktree adoptable live-peer liveness-unknown error usage"
expected_for() {
  case "$1" in
    absent)             printf 'ABSENT\n' ;;
    unstamped)          printf 'UNSTAMPED\n' ;;
    malformed)          printf 'MALFORMED\n' ;;
    displaced-sentinel) printf 'MALFORMED\n' ;;
    duplicate-sentinel) printf 'DUPLICATE-SENTINEL\n' ;;
    foreign-issue)      printf 'FOREIGN-ISSUE\n' ;;
    foreign-machine)    printf 'FOREIGN-MACHINE\n' ;;
    foreign-worktree)   printf 'FOREIGN-WORKTREE\n' ;;
    adoptable)          printf 'ADOPTABLE\n' ;;
    live-peer)          printf 'LIVE-PEER\n' ;;
    liveness-unknown)   printf 'LIVENESS-UNKNOWN\n' ;;
    error)              printf 'ERROR\n' ;;
    usage)              printf 'USAGE\n' ;;
    *)                  printf '\n' ;;
  esac
}
# setup_state <state> <dir> — build the state and set PROBE_* (+ SLEEPER when a live process
# is part of the state).
setup_state() {
  PROBE_MACHINE=boxA; PROBE_SESSION="$SESS_A"; PROBE_PID=$$; SLEEPER=''; PROBE_ARGS_BAD=0
  local st="$1" d="$2" other
  case "$st" in
    absent) : ;;
    unstamped) printf 'legacy hand-written plan\n' >"$d/$MARKER" ;;
    malformed)
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
      grep -vFx -- "$sentinel_end" "$d/$MARKER" >"$T/dl-mal" && mv "$T/dl-mal" "$d/$MARKER" ;;
    displaced-sentinel)
      # The NEW refusal shape from B1: a valid stamp displaced off line 1. It must be covered
      # by the dead-letter rule like every other refusal, not left to the states that existed
      # when the case was written.
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
      { printf '\n'; cat "$d/$MARKER"; } >"$T/dl-disp" && mv "$T/dl-disp" "$d/$MARKER" ;;
    duplicate-sentinel)
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
      { printf '%s\n' "$sentinel"; printf 'issue: 3822\n'; } >>"$d/$MARKER" ;;
    foreign-issue)
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 9999 >/dev/null 2>&1 ;;
    foreign-machine)
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
      PROBE_MACHINE=boxB ;;
    foreign-worktree)
      other=$(lane "dl-other-$$")
      run "$other" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
      cp "$other/$MARKER" "$d/$MARKER" ;;
    adoptable)
      sleep 30 & local dead=$!
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$dead" -- write 3822 >/dev/null 2>&1
      kill "$dead" 2>/dev/null; wait "$dead" 2>/dev/null
      PROBE_SESSION="$SESS_B" ;;
    live-peer)
      sleep 300 & SLEEPER=$!
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$SLEEPER" -- write 3822 >/dev/null 2>&1
      PROBE_SESSION="$SESS_B" ;;
    liveness-unknown)
      run "$d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" -- write 3822 >/dev/null 2>&1
      PROBE_SESSION="$SESS_B" ;;
    error) mkdir -p "$d/$MARKER" ;;
    # USAGE is an ARGUMENT-shaped refusal, not a marker state (roborev job 30 G2 added the
    # token). It still belongs in this table: the completeness assert below requires EVERY
    # non-success verdict to be reachable from a state here, so a token that joins the closed
    # set without a state would silently escape the dead-letter rule.
    usage) PROBE_ARGS_BAD=1 ;;
  esac
}
dl_probe() {  # dl_probe <dir> <subcommand-or-verify>
  local d="$1" sub="$2"
  if [ "${PROBE_ARGS_BAD:-0}" = 1 ]; then
    run "$d" "CLAIM_MACHINE=$PROBE_MACHINE" "CLAUDE_CODE_SESSION_ID=$PROBE_SESSION" "CLAUDE_PID=$PROBE_PID" -- "$sub" 3822 --actor 2>&1
    return
  fi
  case "$sub" in
    adopt) run "$d" "CLAIM_MACHINE=$PROBE_MACHINE" "CLAUDE_CODE_SESSION_ID=$PROBE_SESSION" "CLAUDE_PID=$PROBE_PID" -- adopt 3822 --reason no-dead-letter-probe:derived 2>&1 ;;
    write) run "$d" "CLAIM_MACHINE=$PROBE_MACHINE" "CLAUDE_CODE_SESSION_ID=$PROBE_SESSION" "CLAUDE_PID=$PROBE_PID" -- write 3822 2>&1 ;;
    *)     run "$d" "CLAIM_MACHINE=$PROBE_MACHINE" "CLAUDE_CODE_SESSION_ID=$PROBE_SESSION" "CLAUDE_PID=$PROBE_PID" -- "$sub" 3822 2>&1 ;;
  esac
}
dl_fail=0; dl_covered=''; dl_named=0
for st in $DL_STATES; do
  d=$(lane "dl-$st")
  setup_state "$st" "$d"
  exp="$(expected_for "$st")"
  out="$(dl_probe "$d" verify)"; got="$(verdict_of "$out")"
  if [ "$got" != "$exp" ]; then
    dl_fail=$((dl_fail + 1))
    printf 'note   state %s: expected verdict %s, got %s\n' "$st" "$exp" "$got"
  fi
  dl_covered="$dl_covered $exp"
  # Every subcommand of THIS script named in the refusal text must not return the same
  # refusal when invoked on the same state.
  subs="$(printf '%s\n' "$out" | grep -oE 'drive-issue-state\.sh [a-z]+' | awk '{print $2}' | sort -u)"
  for sub in $subs; do
    case "$sub" in write | verify | adopt | show) : ;; *) continue ;; esac
    dl_named=$((dl_named + 1))
    rout="$(dl_probe "$d" "$sub")"; rgot="$(verdict_of "$rout")"
    if [ "$rgot" = "$exp" ]; then
      dl_fail=$((dl_fail + 1))
      printf 'note   DEAD LETTER: %s names "%s", which returns %s again\n' "$exp" "$sub" "$rgot"
    fi
  done
  [ -z "$SLEEPER" ] || { kill "$SLEEPER" 2>/dev/null; wait "$SLEEPER" 2>/dev/null; }
done
if [ "$dl_fail" -eq 0 ]; then
  ok "13 refusal states reproduce their expected verdict, and every remedy they NAME ($dl_named invocation(s)) escapes that refusal"
else
  bad "$dl_fail dead-letter/verdict failures across the refusal states"
fi
if [ "$dl_named" -ge 2 ]; then
  ok "NON-VACUITY: the scan actually FOUND named remedies ($dl_named) — it is not passing on an empty subject set"
else
  bad "the remedy scan found $dl_named named subcommands: it cannot have measured the dead-letter property"
fi
# COMPLETENESS: every non-success verdict token must appear in the state table, so a new
# refusal verdict cannot be added without a state that reaches it.
dl_missing=''
for t in $VERDICT_SET; do
  case "$t" in OWNED | WRITTEN | ADOPTED | SHOWN) continue ;; esac
  case " $dl_covered " in *" $t "*) : ;; *) dl_missing="$dl_missing $t" ;; esac
done
if [ -z "$dl_missing" ]; then
  ok "every non-success verdict in the closed set is reached by a state in this table"
else
  bad "verdict tokens reached by NO state here (a new refusal joined uncovered):$dl_missing"
fi

# ===========================================================================
case_begin 23-durable-fields-survive "stage/request-id/pr/branch survive a later write AND an adopt; --clear is the only eraser"
# ===========================================================================
# drive-issue.md's Delta 3 names "stage reached, open request ID, PR/branch" as THE durable
# state, and `adopt` is the normal cron-resume path — so silently dropping request-id on a
# resume left the next session unable to tell which request it awaits, and it would re-ask,
# breaking "one marker, one wait". Omitting a flag must never erase state.
L23=$(lane lane23)
run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- \
  write 3822 --stage implement --request-id coord-3822-7 --pr 4001 --branch issue-3822-slug >/dev/null 2>&1
run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- \
  write 3822 --stage review >/dev/null 2>&1
fp_show=$(run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822)
if printf '%s\n' "$fp_show" | grep -q 'request-id=coord-3822-7' \
   && printf '%s\n' "$fp_show" | grep -q 'pr=4001' \
   && printf '%s\n' "$fp_show" | grep -q 'branch=issue-3822-slug' \
   && printf '%s\n' "$fp_show" | grep -q 'stage=review'; then
  ok "a later 'write --stage' PRESERVES request-id/pr/branch and updates the stage"
else
  bad "a write with one flag erased the other durable fields:
$fp_show"
fi
run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- \
  write 3822 --clear request-id >/dev/null 2>&1
fp_show2=$(run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822)
fp_bad=$(run "$L23" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --clear frobnicate 2>&1); fp_badrc=$?
if printf '%s\n' "$fp_show2" | grep -q 'request-id=none' \
   && printf '%s\n' "$fp_show2" | grep -q 'pr=4001' && [ "$fp_badrc" -eq 64 ]; then
  ok "'--clear request-id' erases exactly that field (pr survives), and an unknown field name is exit 64 — the field set is CLOSED"
else
  bad "--clear misbehaved: unknown-field rc=$fp_badrc
$fp_show2"
fi
# ADOPT is the cron-resume path: the durable fields must cross it.
L23B=$(lane lane23b)
sleep 30 & fp_dead=$!
run "$L23B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$fp_dead" -- \
  write 3822 --stage implement --request-id coord-3822-9 --pr 4002 --branch issue-3822-b >/dev/null 2>&1
kill "$fp_dead" 2>/dev/null; wait "$fp_dead" 2>/dev/null
fp_ad=$(run "$L23B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- \
  adopt 3822 --reason cron-reinvoke:writer-pid-gone); fp_adrc=$?
fp_show3=$(run "$L23B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- show 3822)
if [ "$fp_adrc" -eq 0 ] && printf '%s\n' "$fp_show3" | grep -q 'request-id=coord-3822-9' \
   && printf '%s\n' "$fp_show3" | grep -q 'pr=4002' \
   && printf '%s\n' "$fp_show3" | grep -q 'branch=issue-3822-b' \
   && printf '%s\n' "$fp_show3" | grep -q 'stage=implement' \
   && printf '%s\n' "$fp_show3" | grep -q "prior-session=$SESS_A"; then
  ok "an adopt carries stage/request-id/pr/branch across the ownership transfer (and still records the prior session)"
else
  bad "adopt destroyed durable state: rc=$fp_adrc
$fp_show3"
fi

# ===========================================================================
case_begin 24-serialization "the verify->replace sequence is SERIALIZED: the lock is really taken, and two racers produce one winner"
# ===========================================================================
# The ownership check is sound but was not ATOMIC with the replacement, so two sessions in
# ONE lane — the scenario this file exists for — could both pass verification and one clobber
# the other. Demonstrated here on BEHAVIOUR, never on "a lock file appeared".
L24=$(lane lane24)
# (a) A SHORTENED wait proves the lock is genuinely acquired and waited on. The timeout is a
# hard-coded constant with no env override, so the test SUBSTITUTES THE ARTIFACT: a scratch
# copy of the script with the constant rewritten (and its sibling library alongside, which
# the script sources from its own directory), with the substitution VERIFIED.
SCRATCH="$T/scratch"; mkdir -p "$SCRATCH/lib"
sed 's/^MARKER_LOCK_WAIT_SECS=30$/MARKER_LOCK_WAIT_SECS=1/' "$DS" >"$SCRATCH/drive-issue-state.sh"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$SCRATCH/lib/process-liveness.sh"
if grep -q '^MARKER_LOCK_WAIT_SECS=1$' "$SCRATCH/drive-issue-state.sh"; then
  ok "the shortened-wait pin took in the scratch copy (a test-only env seam is deliberately not offered)"
else
  bad "the scratch copy still carries the shipped wait — the contention case below would measure nothing"
fi
lock24="$L24/$MARKER.lock"
: >>"$lock24"
held24="$T/held24"
flock -x "$lock24" -c "touch '$held24'; sleep 5" &
holder24=$!
i=0
while [ ! -f "$held24" ] && [ "$i" -lt 60 ]; do i=$((i + 1)); sleep 0.1; done
if [ -f "$held24" ]; then
  ok "the external holder confirmed it holds the lock (handshake on a flag file, not a sleep)"
else
  bad "the external lock holder never confirmed acquisition — the contention case measures nothing"
fi
ser_out=$( cd "$L24" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$SCRATCH/drive-issue-state.sh" write 3822 2>&1 ); ser_rc=$?
if [ "$ser_rc" -ne 0 ] && [ "$(verdict_of "$ser_out")" = ERROR ] \
   && printf '%s\n' "$ser_out" | grep -q 'holds the marker lock' && [ ! -f "$L24/$MARKER" ]; then
  ok "with the lock held elsewhere, write REFUSES (rc=$ser_rc) rather than mutating unserialized, and writes nothing"
else
  bad "write ignored a held lock: rc=$ser_rc verdict=$(verdict_of "$ser_out")
$ser_out"
fi
kill "$holder24" 2>/dev/null; wait "$holder24" 2>/dev/null
# (b) NON-VACUITY: once the lock is free the very same call succeeds, so the refusal above is
# about CONTENTION and not about the lock being permanently unobtainable.
ser_ok=$(run "$L24" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822); ser_okrc=$?
if [ "$ser_okrc" -eq 0 ] && [ "$(verdict_of "$ser_ok")" = WRITTEN ]; then
  ok "NON-VACUITY: with the lock free the same write succeeds"
else
  bad "write cannot take a free lock: rc=$ser_okrc
$ser_ok"
fi
# (c) TWO CONCURRENT ADOPTERS OF ONE ADOPTABLE LANE -> exactly ONE winner. Deterministic
# whether or not the two overlap in time: serialized, the loser re-verifies INSIDE the lock,
# sees the winner's live session and refuses.
#
# MEASURED, not assumed: run against a copy of the shipped script with `lock_marker`
# neutered, this exact scenario produced ADOPTED / ADOPTED — both adopters winning and one
# clobbering the other's stamp. So the assertion below discriminates the fix rather than
# passing for free.
L24C=$(lane lane24c)
sleep 30 & sc_dead=$!
run "$L24C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$sc_dead" -- write 3822 --stage implement >/dev/null 2>&1
kill "$sc_dead" 2>/dev/null; wait "$sc_dead" 2>/dev/null
sleep 300 & sc_p1=$!
sleep 300 & sc_p2=$!
( run "$L24C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$sc_p1" -- \
    adopt 3822 --reason race-probe:adopter-one >"$T/race1.out" 2>&1 ) &
sc_j1=$!
( run "$L24C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=sess-cccccccc" "CLAUDE_PID=$sc_p2" -- \
    adopt 3822 --reason race-probe:adopter-two >"$T/race2.out" 2>&1 ) &
sc_j2=$!
wait "$sc_j1" 2>/dev/null; wait "$sc_j2" 2>/dev/null
kill "$sc_p1" "$sc_p2" 2>/dev/null; wait "$sc_p1" 2>/dev/null; wait "$sc_p2" 2>/dev/null
sc_v1=$(verdict_of "$(cat "$T/race1.out")"); sc_v2=$(verdict_of "$(cat "$T/race2.out")")
sc_won=0
[ "$sc_v1" = ADOPTED ] && sc_won=$((sc_won + 1))
[ "$sc_v2" = ADOPTED ] && sc_won=$((sc_won + 1))
if [ "$sc_won" -eq 1 ] && verdict_in_set "$sc_v1" && verdict_in_set "$sc_v2"; then
  ok "two concurrent adopters of one adoptable lane produce EXACTLY ONE ADOPTED (verdicts: $sc_v1 / $sc_v2)"
else
  bad "concurrent adopters produced $sc_won winners (verdicts: $sc_v1 / $sc_v2) — the verify->replace sequence is not serialized
$(cat "$T/race1.out")
$(cat "$T/race2.out")"
fi
# The surviving stamp must be ONE coherent record: the winner's session, and the durable
# stage it inherited. A clobber would show as a lost stage or a torn prologue.
sc_show=$(run "$L24C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- show 3822 2>&1); sc_shrc=$?
if [ "$sc_shrc" -eq 0 ] && printf '%s\n' "$sc_show" | grep -q 'stage=implement'; then
  ok "the surviving marker is one coherent stamp and kept the inherited stage — no torn or clobbered record"
else
  bad "the surviving marker is not readable/coherent: rc=$sc_shrc
$sc_show"
fi

# ===========================================================================
case_begin 25-displaced-sentinel-is-not-legacy "a DISPLACED stamp is MALFORMED, never migrated — a one-byte mutation must not overwrite a live peer"
# ===========================================================================
# The migration path decided "carries no ownership stamp" from the FIRST LINE ALONE, so a
# stamped marker with one prepended blank line or comment was classified legacy, DISCARDED
# and REPLACED — overwriting a live peer's state, which is the defect this whole file exists
# to close, reachable by a one-byte mutation. "No stamp at line 1 => no identity asserted" is
# valid only if no sentinel exists ANYWHERE.
disp_fail=0
for disp_kind in blank comment; do
  Ld=$(lane "lane25-$disp_kind")
  dsleep=''
  sleep 300 & dsleep=$!
  run "$Ld" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$dsleep" -- \
    write 3822 --stage peer-plan --request-id coord-3822-live >/dev/null 2>&1
  case "$disp_kind" in
    blank)   { printf '\n';                    cat "$Ld/$MARKER"; } >"$T/d25" ;;
    comment) { printf '%s\n' '<!-- note -->';  cat "$Ld/$MARKER"; } >"$T/d25" ;;
  esac
  mv "$T/d25" "$Ld/$MARKER"
  before=$(cksum <"$Ld/$MARKER")
  dv=$(run "$Ld" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822); dvrc=$?
  dw=$(run "$Ld" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- write 3822 --stage my-plan); dwrc=$?
  da=$(run "$Ld" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason displaced-probe:take-over); darc=$?
  after=$(cksum <"$Ld/$MARKER")
  kill "$dsleep" 2>/dev/null; wait "$dsleep" 2>/dev/null
  if [ "$dvrc" -ne 0 ] && [ "$(verdict_of "$dv")" = MALFORMED ] \
     && [ "$dwrc" -ne 0 ] && [ "$(verdict_of "$dw")" = MALFORMED ] \
     && [ "$darc" -ne 0 ] && [ "$(verdict_of "$da")" = MALFORMED ] \
     && [ "$before" = "$after" ]; then
    ok "a stamp displaced by a prepended $disp_kind is MALFORMED on verify AND write AND adopt, and the peer's file is byte-identical afterwards"
  else
    disp_fail=$((disp_fail + 1))
    bad "displaced-by-$disp_kind was mishandled: verify=$dvrc/$(verdict_of "$dv") write=$dwrc/$(verdict_of "$dw") adopt=$darc/$(verdict_of "$da") file-changed=$([ "$before" = "$after" ] && echo no || echo YES)
$dw"
  fi
done
# NON-VACUITY: a file with NO sentinel anywhere is still the migratable legacy shape, so the
# fix narrowed the migration path rather than closing it.
L25N=$(lane lane25n)
printf 'legacy plan, no sentinel anywhere\n' >"$L25N/$MARKER"
nv=$(run "$L25N" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822); nvrc=$?
if [ "$nvrc" -eq 0 ] && [ "$(verdict_of "$nv")" = WRITTEN ]; then
  ok "NON-VACUITY: a genuinely sentinel-free legacy marker still migrates (WRITTEN) — the migration path was narrowed, not closed"
else
  bad "the legacy migration path was closed by the displaced-sentinel fix: rc=$nvrc verdict=$(verdict_of "$nv")
$nv"
fi

# ===========================================================================
case_begin 26-unusable-start-window "an INVERTED or out-of-range start window is LIVENESS-UNKNOWN, never GONE"
# ===========================================================================
# The worst of the three: a false-permissive on the LIVENESS axis. Endpoints were only
# digit-checked (and CONCATENATED, so one could hide behind the other), and an inverted or
# out-of-range interval made `[` ERROR inside `if A && B` — an errored `[` reads as FALSE,
# and the false branch was `gone`, so a LIVE PEER became adoptable. Unmeasurable must never
# read as gone.
uw_fail=0
for uw_kind in inverted huge negative leading-zero; do
  Lu=$(lane "lane26-$uw_kind")
  sleep 300 & upid=$!
  run "$Lu" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$upid" -- write 3822 >/dev/null 2>&1
  case "$uw_kind" in
    inverted)     ulo=2000000000; uhi=1000000000 ;;
    huge)         ulo=999999999999999999999999;  uhi=999999999999999999999999 ;;
    negative)     ulo=-5;         uhi=10 ;;
    leading-zero) ulo=0000000001; uhi=0000000002 ;;
  esac
  sed -e "s/^session-pid-start-earliest: .*/session-pid-start-earliest: $ulo/" \
      -e "s/^session-pid-start-latest: .*/session-pid-start-latest: $uhi/" \
      "$Lu/$MARKER" >"$T/m26" && mv "$T/m26" "$Lu/$MARKER"
  uv=$(run "$Lu" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- verify 3822 2>&1); uvrc=$?
  ua=$(run "$Lu" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason unusable-window-probe 2>&1); uarc=$?
  kill "$upid" 2>/dev/null; wait "$upid" 2>/dev/null
  uvv="$(verdict_of "$uv")"
  # LIVENESS-UNKNOWN (the MEASUREMENT is unusable) — and specifically NOT ADOPTABLE, which
  # is the false-permissive, and NOT MALFORMED, since the marker itself is well-formed.
  if [ "$uvrc" -ne 0 ] && [ "$uvv" = LIVENESS-UNKNOWN ] \
     && [ "$uarc" -ne 0 ] && [ "$(verdict_of "$ua")" = LIVENESS-UNKNOWN ]; then
    ok "a $uw_kind start window yields LIVENESS-UNKNOWN on verify AND adopt — the live peer is not adoptable"
  else
    uw_fail=$((uw_fail + 1))
    bad "a $uw_kind start window was mis-resolved: verify=$uvrc/$uvv adopt=$uarc/$(verdict_of "$ua") (ADOPTABLE here would mean a LIVE peer could be taken over)
$uv"
  fi
done
if [ "$uw_fail" -eq 0 ]; then
  ok "all 4 unusable-interval shapes refuse on the UNKNOWN branch"
else
  bad "$uw_fail unusable-interval shapes were not refused as UNKNOWN"
fi

# ===========================================================================
case_begin 27-pre-rename-validation "the ASSEMBLED bytes are validated immediately before the atomic rename"
# ===========================================================================
# `--body-file` was validated once and READ AGAIN at assembly, so a body changing in between
# committed an unchecked sentinel — reachable by accident (an agent rewriting its own notes),
# which makes it a defect, and the result is a marker every later read refuses: self-bricking.
# The fix validates the committed bytes themselves, so it holds however the body arrived.
#
# THE RACE ITSELF IS NOT DETERMINISTICALLY REPRODUCIBLE, so this case measures the LAYER that
# closes it: a scratch copy with the EARLY body check neutered must still refuse at the
# pre-rename check. That proves the two layers are independent (defence in depth) rather than
# one check moved. It does NOT prove a timed interleaving is caught — nothing cheap can.
SCRATCH2="$T/scratch2"; mkdir -p "$SCRATCH2/lib"
sed 's/^assert_body_safe() {$/assert_body_safe() { return 0; # NEUTERED for this case/' \
  "$DS" >"$SCRATCH2/drive-issue-state.sh"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$SCRATCH2/lib/process-liveness.sh"
if grep -q 'NEUTERED for this case' "$SCRATCH2/drive-issue-state.sh"; then
  ok "the early-body-check neutering took in the scratch copy (so the pre-rename layer is what is being measured)"
else
  bad "could not neuter the early body check — this case would measure the early check, not the pre-rename one"
fi
L27=$(lane lane27)
body27="$T/body27.md"
{ printf 'notes\n'; printf '%s\n' "$sentinel"; printf 'issue: 1\n'; } >"$body27"
pr_out=$( cd "$L27" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
  bash "$SCRATCH2/drive-issue-state.sh" write 3822 --body-file "$body27" 2>&1 ); pr_rc=$?
# The `.lock` sidecar is a LEGITIMATE artifact of serialization, so the stray count must
# exclude it — counting it made this assert fail on correct behaviour, which is the
# "guard that reds on correct input" shape. What must be absent is an abandoned
# atomic-replace TEMPORARY.
stray27=$(find "$L27" -name "$MARKER.*" ! -name "$MARKER.lock" 2>/dev/null | wc -l | tr -d ' ')
lock27=$([ -f "$L27/$MARKER.lock" ] && echo yes || echo no)
if [ "$pr_rc" -ne 0 ] && [ "$(verdict_of "$pr_out")" = ERROR ] \
   && [ ! -f "$L27/$MARKER" ] && [ "$stray27" = 0 ] && [ "$lock27" = yes ] \
   && all_lines_anchored "$pr_out"; then
  ok "with the early check bypassed, the pre-rename validation refuses (ERROR), commits nothing and leaves no abandoned temporary (the .lock sidecar is expected and present)"
else
  bad "the assembled marker was committed unvalidated: rc=$pr_rc verdict=$(verdict_of "$pr_out") marker=$([ -f "$L27/$MARKER" ] && echo yes || echo no) strays=$stray27
$pr_out"
fi
# NON-VACUITY: the same neutered script writes a CLEAN body fine, so the refusal above is
# about the sentinel and not about the neutering having broken the script.
body27ok="$T/body27ok.md"; printf 'clean notes\n' >"$body27ok"
pr_ok=$( cd "$L27" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
  bash "$SCRATCH2/drive-issue-state.sh" write 3822 --body-file "$body27ok" 2>&1 ); pr_okrc=$?
if [ "$pr_okrc" -eq 0 ] && [ "$(verdict_of "$pr_ok")" = WRITTEN ]; then
  ok "NON-VACUITY: the same scratch script writes a clean body successfully"
else
  bad "the scratch script cannot write at all, so the refusal above proves nothing: rc=$pr_okrc
$pr_ok"
fi

# ===========================================================================
case_begin 29-missing-liveness-library "a MISSING shared liveness library emits the ERROR verdict TOKEN, not a bare prefixed line"
# ===========================================================================
# roborev job 26 F1. The guard was anchored (it carried the DRIVE-STATE: prefix) but was NOT
# the `verdict ERROR` shape, so the ONE line every caller branches on — the closed-set token
# on the `verdict ` line — was ABSENT. drive-issue.md's Delta 4 tells callers to `case` on
# that token, so this failure fell through every arm: a fatal, unreadable-by-construction
# refusal. The prefix is contract (a); the token is contract (c), and (a) does not imply (c).
#
# THE ARTIFACT IS SUBSTITUTED, never a path seam: the script is COPIED to a scratch directory
# WITHOUT its lib/, exactly as cases 22/27 substitute the artifact. A settable SCRIPT_HOME (or
# any test-only override) would be one more thing a real invoker can set.
SCRATCH29="$T/scratch29"; mkdir -p "$SCRATCH29"
cp "$DS" "$SCRATCH29/drive-issue-state.sh"
L29=$(lane lane29)
ml_out=$( cd "$L29" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
  bash "$SCRATCH29/drive-issue-state.sh" verify 3822 2>&1 ); ml_rc=$?
ml_v="$(verdict_of "$ml_out")"
if [ "$ml_rc" -eq 1 ] && [ "$ml_v" = ERROR ] && verdict_in_set "$ml_v" \
   && all_lines_anchored "$ml_out" \
   && printf '%s\n' "$ml_out" | grep -q 'process-liveness.sh'; then
  ok "an ABSENT lib/process-liveness.sh yields exit 1 AND a closed-set 'verdict ERROR' line naming the unreadable path"
else
  bad "the missing-liveness-library guard emitted no usable verdict token: rc=$ml_rc verdict='$ml_v'
$ml_out"
fi
# NON-VACUITY: the SAME scratch copy works once the library is beside it, so the refusal
# above is about the missing library and not about the copy being broken.
mkdir -p "$SCRATCH29/lib"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$SCRATCH29/lib/process-liveness.sh"
ml_ok=$( cd "$L29" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
  bash "$SCRATCH29/drive-issue-state.sh" write 3822 2>&1 ); ml_okrc=$?
if [ "$ml_okrc" -eq 0 ] && [ "$(verdict_of "$ml_ok")" = WRITTEN ]; then
  ok "NON-VACUITY: the same scratch copy writes normally with the library present"
else
  bad "the scratch copy is broken independently of the library, so the refusal above proves nothing: rc=$ml_okrc
$ml_ok"
fi
# THE UNREADABLE (as opposed to absent) ROUTE INTO THE SAME GUARD. Root bypasses file
# permissions, so under root the probe is DECLARED unavailable rather than passing vacuously.
if [ "$(id -u)" -eq 0 ]; then
  ok "unreadable-library probe DECLARED unavailable under root (permissions are bypassed) — the absent-library route above is what was measured"
else
  chmod 000 "$SCRATCH29/lib/process-liveness.sh"
  ml_u=$( cd "$L29" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" \
    bash "$SCRATCH29/drive-issue-state.sh" verify 3822 2>&1 ); ml_urc=$?
  chmod 644 "$SCRATCH29/lib/process-liveness.sh" 2>/dev/null || true
  if [ "$ml_urc" -eq 1 ] && [ "$(verdict_of "$ml_u")" = ERROR ] && all_lines_anchored "$ml_u"; then
    ok "an UNREADABLE lib/process-liveness.sh takes the same guard: exit 1 with a 'verdict ERROR' line"
  else
    bad "an unreadable library did not emit the ERROR verdict: rc=$ml_urc verdict=$(verdict_of "$ml_u")
$ml_u"
  fi
fi

# ===========================================================================
case_begin 30-native-diagnostics-stay-anchored "a FAILING external command's NATIVE stderr never reaches the terminal unprefixed"
# ===========================================================================
# roborev job 26 F2. `mktemp`, `mv`, `rm`, `cat`, `date`, `wc`, `tr`, `hostname` were invoked
# with their stderr unredirected, so on failure the NATIVE diagnostic ("mktemp: failed to
# create file via template ...") reached the terminal with NO `DRIVE-STATE: ` prefix — pure
# contract-(a) breakage, since the failure itself is already reported through the anchored
# WRITE_ERR/refuse path.
#
# THE SHIM MUST WRITE TO STDERR AND THEN FAIL. Case 17's shim exits SILENTLY, which is why it
# could not see this: a silent failure exercises the error PATH without exercising the output
# CONTRACT. Each command is shimmed ON ITS OWN (shimming all of them at once would kill the
# script before it reached most of the sites) and the property is asserted over BOTH STREAMS
# COMBINED, because contract (a) is about stdout AND stderr.
LEAK='NATIVE-LEAK-MARKER'
shim_dir_for() {  # shim_dir_for <cmd> — a PATH dir whose <cmd> is noisy AND failing
  local c="$1" d="$T/leakbin-$1"
  mkdir -p "$d"
  { printf '#!/bin/sh\n'
    printf 'echo "%s: %s simulated failure" >&2\n' "$c" "$LEAK"
    printf 'exit 1\n'; } >"$d/$c"
  chmod +x "$d/$c"
  printf '%s\n' "$d"
}
leak_fail=0; leak_probed=0
# probe <cmd> <lane-setup-fn> <extra-args...> — run `write` with <cmd> shimmed and assert the
# combined output is fully anchored.
leak_probe() {
  local c="$1" d="$2"; shift 2
  local sd out rc
  sd="$(shim_dir_for "$c")"
  out=$( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd:$PATH" \
    bash "$DS" "$@" 2>&1 ); rc=$?
  leak_probed=$((leak_probed + 1))
  LEAK_OUT="$out"; LEAK_RC="$rc"
  if all_lines_anchored "$out"; then
    return 0
  fi
  leak_fail=$((leak_fail + 1))
  printf 'note   LEAK via %s (rc=%s):\n%s\n' "$c" "$rc" "$(printf '%s' "$out" | cat -v)"
  return 1
}
# mktemp / mv / date / tr / hostname: reached by an ordinary write in a fresh lane.
for lc in mktemp mv date tr; do
  leak_probe "$lc" "$(lane "leak-$lc")" write 3822 || true
done
# hostname is only consulted when CLAIM_MACHINE is UNSET, so it needs its own invocation.
lh_dir="$(shim_dir_for hostname)"; L30H=$(lane leak-hostname)
lh_out=$( cd "$L30H" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID -u CLAIM_MACHINE \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$lh_dir:$PATH" \
  bash "$DS" write 3822 2>&1 ) || true
leak_probed=$((leak_probed + 1))
if all_lines_anchored "$lh_out"; then :; else
  leak_fail=$((leak_fail + 1))
  printf 'note   LEAK via hostname:\n%s\n' "$(printf '%s' "$lh_out" | cat -v)"
fi
# cat: only reached with a non-empty --body-file.
L30C=$(lane leak-cat); body30="$T/body30.md"; printf 'lane notes\n' >"$body30"
leak_probe cat "$L30C" write 3822 --body-file "$body30" || true
# wc: only reached on the UNSTAMPED migration branch.
L30W=$(lane leak-wc); printf 'legacy hand-written plan\n' >"$L30W/$MARKER"
leak_probe wc "$L30W" write 3822 || true
if [ "$leak_fail" -eq 0 ]; then
  ok "no native diagnostic escaped the anchor across $leak_probed failing-command probes (both streams combined)"
else
  bad "$leak_fail of $leak_probed failing-command probes leaked an unprefixed native diagnostic"
fi
if [ "$leak_probed" -ge 7 ]; then
  ok "NON-VACUITY: $leak_probed distinct external commands were actually shimmed and reached"
else
  bad "only $leak_probed probes ran — the sweep cannot have covered the named call sites"
fi
# CAPTURE, NOT MERELY SUPPRESSION, where the native text is diagnostically useful: a failing
# mktemp's own words must be FOLDED into the anchored ERROR detail, so the operator still
# learns WHY. Suppression alone would satisfy the anchor and lose the diagnosis.
if leak_probe mktemp "$(lane leak-mktemp2)" write 3822; then :; fi
if [ "$LEAK_RC" -ne 0 ] && [ "$(verdict_of "$LEAK_OUT")" = ERROR ] \
   && printf '%s\n' "$LEAK_OUT" | grep -q "$LEAK"; then
  ok "a failing mktemp's NATIVE text is FOLDED into the anchored ERROR detail (captured, not merely suppressed)"
else
  bad "the failing mktemp's native diagnostic was lost entirely: rc=$LEAK_RC verdict=$(verdict_of "$LEAK_OUT")
$LEAK_OUT"
fi
if leak_probe mv "$(lane leak-mv2)" write 3822; then :; fi
if [ "$LEAK_RC" -ne 0 ] && [ "$(verdict_of "$LEAK_OUT")" = ERROR ] \
   && printf '%s\n' "$LEAK_OUT" | grep -q "$LEAK"; then
  ok "a failing mv's NATIVE text is FOLDED into the anchored ERROR detail"
else
  bad "the failing mv's native diagnostic was lost entirely: rc=$LEAK_RC verdict=$(verdict_of "$LEAK_OUT")
$LEAK_OUT"
fi
# `rm` is the one whose failure lands in the EXIT TRAP, AFTER the verdict. MEASURED, not
# assumed: a failing command in a bash EXIT trap under `set -e` aborts the trap AND replaces
# the exit status (verified: a successful body plus a failing trap exits 1). So a broken `rm`
# turned a legitimate WRITTEN(0) into an unexplained non-zero with an unprefixed line after
# the verdict — cleanup is best-effort and must not be able to change either.
rm_dir="$(shim_dir_for rm)"; L30R=$(lane leak-rm)
rm_out=$( cd "$L30R" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$rm_dir:$PATH" \
  bash "$DS" write 3822 2>&1 ); rm_rc=$?
if [ "$rm_rc" -eq 0 ] && [ "$(verdict_of "$rm_out")" = WRITTEN ] && all_lines_anchored "$rm_out" \
   && [ -f "$L30R/$MARKER" ]; then
  ok "a failing (noisy) rm in the cleanup trap neither leaks a line nor changes the WRITTEN(0) verdict"
else
  bad "the cleanup trap's rm changed the outcome or leaked: rc=$rm_rc verdict=$(verdict_of "$rm_out")
$(printf '%s' "$rm_out" | cat -v)"
fi
# A FAILING `date` MUST NOT COMMIT A SELF-BRICKING MARKER. Found by this very case: because
# every caller invokes write_marker as `if ! write_marker ...`, `set -e` is suppressed for its
# whole subtree, so a failing `date` inside `$( )` left `ts` EMPTY and the marker committed —
# after which every read refuses it MALFORMED (missing required field). The lane bricks itself.
date_dir="$(shim_dir_for date)"; L30D=$(lane leak-date2)
d30_out=$( cd "$L30D" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$date_dir:$PATH" \
  bash "$DS" write 3822 2>&1 ); d30_rc=$?
if [ "$d30_rc" -ne 0 ] && [ "$(verdict_of "$d30_out")" = ERROR ] && all_lines_anchored "$d30_out" \
   && [ ! -f "$L30D/$MARKER" ]; then
  ok "a failing 'date' refuses with an anchored ERROR and commits NOTHING — no marker with an empty required field"
else
  bad "a failing date committed a self-bricking marker or emitted no verdict: rc=$d30_rc verdict=$(verdict_of "$d30_out")
$(printf '%s' "$d30_out" | cat -v)"
fi
# THE RESIDUAL THIS CASE ONCE DECLARED IS NOW CLOSED (roborev job 34 H1). With `tr` shimmed the
# identity sanitizer cannot run, so every sanitized field degraded to sanitize_field's
# `unspecified` sentinel and the write SUCCEEDED — a fail-open on the machine axis, reachable on
# any host whose `tr` is broken. The sentinel is still claim.sh's (case 11 pins that agreement)
# and is unchanged; what changed is that COMMITTING it as an identity is refused at the USE
# SITE, so the same shim now produces an anchored ERROR naming axis=machine and NO marker at
# all. What this case still owns is the anchor, which must hold either way.
tr_dir="$(shim_dir_for tr)"; L30T=$(lane leak-tr2)
tr_out=$( cd "$L30T" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
  "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$tr_dir:$PATH" \
  bash "$DS" write 3822 2>&1 ) || true
if all_lines_anchored "$tr_out" && [ ! -e "$L30T/$MARKER" ] \
   && [ "$(verdict_of "$tr_out")" = ERROR ] && printf '%s\n' "$tr_out" | grep -q 'axis=machine'; then
  ok "a failing 'tr' leaks nothing AND no longer fails open: the machine axis is refused (ERROR, axis=machine) and NO marker with a placeholder identity is committed"
else
  bad "a failing tr leaked a line, or committed a placeholder identity instead of refusing:
$(printf '%s' "$tr_out" | cat -v)
$(cat "$L30T/$MARKER" 2>/dev/null | cat -v)"
fi

# ===========================================================================
case_begin 31-adoption-provenance-survives "adoption provenance (prior-session/-pid/-ts, adopt-reason) survives a later write"
# ===========================================================================
# roborev job 26 F3, and the SAME CLASS as job 18 finding 2 (dropped durable fields)
# reappearing at the adopt fields: `adopt` records HOW this lane changed hands, and the very
# next `write --stage x` rebuilt the prologue from the write path's field list — which carried
# only stage/request-id/pr/branch — so the provenance VANISHED on the first stage update. A
# `--reason` that is mandatory, validated and then erased by the next command is worthless.
L31=$(lane lane31)
sleep 30 & ap_d1=$!
run "$L31" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$ap_d1" -- \
  write 3822 --stage implement --request-id coord-3822-31 --pr 4031 --branch issue-3822-ap >/dev/null 2>&1
kill "$ap_d1" 2>/dev/null; wait "$ap_d1" 2>/dev/null
# The adopting session records a pid that is ALSO gone, so a THIRD session can adopt later
# (the overwrite half below). Its own `write` is OWNED by session equality, which needs no
# liveness at all.
# It stays ALIVE across its own adopt and write (so both stamps record a MEASURABLE start
# window) and is killed only afterwards, which is what makes the third session's adopt reach
# the `gone` branch rather than LIVENESS-UNKNOWN.
sleep 300 & ap_d2=$!
ap_ad=$(run "$L31" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$ap_d2" -- \
  adopt 3822 --reason cron-reinvoke:writer-pid-gone-31); ap_adrc=$?
ap_ts1="$(sed -n 's/^prior-ts: //p' "$L31/$MARKER" | head -1)"
if [ "$ap_adrc" -eq 0 ] && [ "$(verdict_of "$ap_ad")" = ADOPTED ] \
   && grep -q "^prior-session: $SESS_A\$" "$L31/$MARKER" \
   && grep -q "^prior-session-pid: $ap_d1\$" "$L31/$MARKER" \
   && [ -n "$ap_ts1" ] \
   && grep -q '^adopt-reason: cron-reinvoke:writer-pid-gone-31$' "$L31/$MARKER"; then
  ok "the adopt recorded all four provenance fields (prior-session/-pid/-ts, adopt-reason)"
else
  bad "the adopt did not record the provenance this case is about: rc=$ap_adrc
$(cat "$L31/$MARKER")"
fi
# THE SUBJECT: an ordinary stage update by the now-owning session.
ap_w=$(run "$L31" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$ap_d2" -- \
  write 3822 --stage review); ap_wrc=$?
ap_show=$(run "$L31" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$ap_d2" -- show 3822 2>&1)
if [ "$ap_wrc" -eq 0 ] && [ "$(verdict_of "$ap_w")" = WRITTEN ] \
   && printf '%s\n' "$ap_show" | grep -q "prior-session=$SESS_A" \
   && printf '%s\n' "$ap_show" | grep -q "prior-session-pid=$ap_d1" \
   && printf '%s\n' "$ap_show" | grep -q "prior-ts=$ap_ts1" \
   && printf '%s\n' "$ap_show" | grep -q 'adopt-reason=cron-reinvoke:writer-pid-gone-31' \
   && printf '%s\n' "$ap_show" | grep -q 'stage=review' \
   && printf '%s\n' "$ap_show" | grep -q 'request-id=coord-3822-31'; then
  ok "a later 'write --stage' PRESERVES all four adoption-provenance fields AND still updates the stage"
else
  bad "the stage update erased the adoption provenance: rc=$ap_wrc
$ap_show"
fi
# A LATER ADOPT OVERWRITES them — the NEWEST hand-over is the one recorded, not the first.
kill "$ap_d2" 2>/dev/null; wait "$ap_d2" 2>/dev/null
ap_ad2=$(run "$L31" CLAIM_MACHINE=boxA CLAUDE_CODE_SESSION_ID=sess-cccccccc "CLAUDE_PID=$$" -- \
  adopt 3822 --reason second-handover:31); ap_ad2rc=$?
ap_show2=$(run "$L31" CLAIM_MACHINE=boxA CLAUDE_CODE_SESSION_ID=sess-cccccccc "CLAUDE_PID=$$" -- show 3822 2>&1)
if [ "$ap_ad2rc" -eq 0 ] && [ "$(verdict_of "$ap_ad2")" = ADOPTED ] \
   && printf '%s\n' "$ap_show2" | grep -q "prior-session=$SESS_B" \
   && printf '%s\n' "$ap_show2" | grep -q "prior-session-pid=$ap_d2" \
   && printf '%s\n' "$ap_show2" | grep -q 'adopt-reason=second-handover:31' \
   && ! printf '%s\n' "$ap_show2" | grep -q 'cron-reinvoke:writer-pid-gone-31'; then
  ok "a LATER adopt OVERWRITES the provenance with the newest hand-over (it is not accumulated)"
else
  bad "a second adopt did not replace the provenance: rc=$ap_ad2rc
$ap_show2"
fi
# NEGATIVE HALF: a fresh write over an ABSENT marker must INVENT none of them, and neither
# must the UNSTAMPED migration branch (which asserts no ownership and carries nothing forward).
L31B=$(lane lane31b)
run "$L31B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement >/dev/null 2>&1
L31C=$(lane lane31c); printf 'legacy hand-written plan\n' >"$L31C/$MARKER"
run "$L31C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
ap_negshow=$(run "$L31B" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822 2>&1)
if ! grep -qE '^(prior-session|prior-session-pid|prior-ts|adopt-reason): ' "$L31B/$MARKER" \
   && ! grep -qE '^(prior-session|prior-session-pid|prior-ts|adopt-reason): ' "$L31C/$MARKER" \
   && ! printf '%s\n' "$ap_negshow" | grep -q 'prior-' \
   && ! printf '%s\n' "$ap_negshow" | grep -q 'adopt-reason'; then
  ok "a fresh write (ABSENT marker) and the UNSTAMPED migration invent NO provenance fields, and show prints none"
else
  bad "provenance fields were invented where there was no adoption:
$(cat "$L31B/$MARKER")
--
$(cat "$L31C/$MARKER")
--
$ap_negshow"
fi
# A DUPLICATE provenance key is a MALFORMED refusal like every other stamp key: which
# occurrence is the record must not be the parser's choice.
L31D=$(lane lane31d)
sleep 30 & ap_d3=$!
run "$L31D" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$ap_d3" -- write 3822 >/dev/null 2>&1
kill "$ap_d3" 2>/dev/null; wait "$ap_d3" 2>/dev/null
run "$L31D" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason dup-probe:31 >/dev/null 2>&1
ap_dupfail=0
for dk in prior-session-pid prior-ts adopt-reason; do
  cp "$L31D/$MARKER" "$T/m31d.bak"
  awk -v k="$dk" '{print} $0 ~ "^"k": " && !d {print; d=1}' "$T/m31d.bak" >"$L31D/$MARKER"
  ap_du=$(run "$L31D" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- show 3822 2>&1); ap_durc=$?
  if [ "$ap_durc" -ne 0 ] && [ "$(verdict_of "$ap_du")" = MALFORMED ]; then :; else
    ap_dupfail=$((ap_dupfail + 1))
    printf 'note   duplicate %s was not refused: rc=%s verdict=%s\n' "$dk" "$ap_durc" "$(verdict_of "$ap_du")"
  fi
  cp "$T/m31d.bak" "$L31D/$MARKER"
done
if [ "$ap_dupfail" -eq 0 ]; then
  ok "a DUPLICATE prior-session-pid / prior-ts / adopt-reason is refused MALFORMED — these keys are PARSED, not merely written"
else
  bad "$ap_dupfail provenance keys accept a duplicate (they are written but not parsed)"
fi

# ===========================================================================
case_begin 32-failed-scan-is-not-no-match "a FAILING sentinel scan is ERROR, never a permissive 'legacy' that overwrites a peer"
# ===========================================================================
# roborev job 30 G1. `count_sentinel` collapsed grep's THREE outcomes (0 = matched,
# 1 = no match, >1 = the scan could not be performed) onto TWO and took the PERMISSIVE
# answer for the error case: an errored scan counted as ZERO sentinels. With a DISPLACED
# sentinel that made `marker_class` answer `legacy`, and `write`'s MIGRATION path then
# DISCARDED AND REPLACED the file — which may be a LIVE PEER's stamped state. That is the
# exact defect this whole script exists to prevent, arriving through the one branch that is
# allowed to destroy a marker. CLAUDE.md states the rule this violates: a positive verdict
# requires an AFFIRMATIVE MEASUREMENT, and a pass is never derived from the ABSENCE of a bad
# signal (`1699-find-tristate` lints the sibling `[ -z "$(find …)" ]` shape).
#
# THE ARTIFACT IS SUBSTITUTED, never a seam: a PATH-shim `grep` that fails ONLY when the file
# it is asked to scan is the marker itself. A blanket failing `grep` would red the assembled-
# marker validation instead and never reach the migration branch, i.e. it would pass for the
# wrong reason.
G1SHIM="$T/g1bin"; mkdir -p "$G1SHIM"
{ printf '#!/bin/sh\n'
  printf 'for a in "$@"; do last="$a"; done\n'
  printf 'case "$last" in *%s) exit 2 ;; esac\n' "$MARKER"
  printf 'exec %s "$@"\n' "$(command -v grep)"; } >"$G1SHIM/grep"
chmod +x "$G1SHIM/grep"
g1_run() {  # g1_run <dir> <path-prefix|''> <args...>
  local d="$1" pfx="$2"; shift 2
  ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
      "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" ${pfx:+"PATH=$pfx:$PATH"} \
      bash "$DS" "$@" 2>&1 )
}
# The state: a VALID stamp displaced off line 1 (case 25's shape) — the file DOES assert an
# identity, so misclassifying it as `legacy` is what destroys a peer's plan.
L32=$(lane lane32)
run "$L32" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement >/dev/null 2>&1
{ printf '\n'; cat "$L32/$MARKER"; } >"$T/m32" && mv "$T/m32" "$L32/$MARKER"
cp "$L32/$MARKER" "$T/m32.expected"
g32_w=$(g1_run "$L32" "$G1SHIM" write 3822 --stage groom); g32_wrc=$?
if [ "$g32_wrc" -ne 0 ] && [ "$(verdict_of "$g32_w")" = ERROR ] && all_lines_anchored "$g32_w"; then
  ok "an UNPERFORMABLE sentinel scan makes 'write' REFUSE with an anchored ERROR — the scan's failure is not read as 'no sentinels'"
else
  bad "a failed sentinel scan did not refuse: rc=$g32_wrc verdict=$(verdict_of "$g32_w")
$g32_w"
fi
# THE ASSERTION THAT MATTERS: the peer's bytes are still there.
if cmp -s "$T/m32.expected" "$L32/$MARKER"; then
  ok "the marker is BYTE-IDENTICAL after the refusal — an unmeasurable classification never reaches the DESTRUCTIVE migration branch"
else
  bad "the marker was MODIFIED while its classification was unmeasurable (a peer's state would have been destroyed):
$(cat "$L32/$MARKER" 2>/dev/null | cat -v)"
fi
# The same signal on the READ path: `verify` must not report UNSTAMPED (which tells the caller
# to run `write`, i.e. to destroy it) when the scan never ran.
g32_v=$(g1_run "$L32" "$G1SHIM" verify 3822); g32_vrc=$?
if [ "$g32_vrc" -eq 1 ] && [ "$(verdict_of "$g32_v")" = ERROR ]; then
  ok "'verify' reports ERROR(1) rather than UNSTAMPED(8) when the scan could not be performed"
else
  bad "verify derived a classification from an unperformed scan: rc=$g32_vrc verdict=$(verdict_of "$g32_v")
$g32_v"
fi
# The genuinely UNSTAMPED file takes the same route: the migration branch is entered only on a
# MEASURED absence of sentinels.
L32L=$(lane lane32-legacy); printf 'legacy hand-written plan\n' >"$L32L/$MARKER"
cp "$L32L/$MARKER" "$T/m32l.expected"
g32_l=$(g1_run "$L32L" "$G1SHIM" write 3822); g32_lrc=$?
if [ "$g32_lrc" -ne 0 ] && [ "$(verdict_of "$g32_l")" = ERROR ] && cmp -s "$T/m32l.expected" "$L32L/$MARKER"; then
  ok "the UNSTAMPED migration branch is entered only on a MEASURED absence of sentinels — an unmeasurable one leaves the file untouched"
else
  bad "the migration branch ran on an unmeasured scan: rc=$g32_lrc verdict=$(verdict_of "$g32_l")
$g32_l"
fi
# NON-VACUITY: with a WORKING grep the same two states reach their real verdicts, so the
# refusals above are about the failed scan and not about a broken fixture. FRESH lanes, because
# a probe that ran against a state an earlier probe may have destroyed proves nothing.
L32N=$(lane lane32-nv)
run "$L32N" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
{ printf '\n'; cat "$L32N/$MARKER"; } >"$T/m32n" && mv "$T/m32n" "$L32N/$MARKER"
L32NL=$(lane lane32-nv-legacy); printf 'legacy hand-written plan\n' >"$L32NL/$MARKER"
g32_nv=$(g1_run "$L32N" "" verify 3822); g32_nvrc=$?
g32_nl=$(g1_run "$L32NL" "" write 3822); g32_nlrc=$?
if [ "$g32_nvrc" -eq 8 ] && [ "$(verdict_of "$g32_nv")" = MALFORMED ] \
   && [ "$g32_nlrc" -eq 0 ] && [ "$(verdict_of "$g32_nl")" = WRITTEN ]; then
  ok "NON-VACUITY: with a working grep the displaced file is MALFORMED(8) and the unstamped one still MIGRATES — only the unmeasurable case changed"
else
  bad "the fixture is broken independently of the shim: displaced rc=$g32_nvrc/$(verdict_of "$g32_nv") legacy rc=$g32_nlrc/$(verdict_of "$g32_nl")"
fi

# ===========================================================================
case_begin 33-signals-emit-one-verdict "a SIGNAL emits exactly ONE anchored verdict describing what is KNOWN about the commit"
# ===========================================================================
# roborev job 30 G2, and the SECOND instance of round 5's F1 class: that round made the
# missing-liveness-library guard emit a verdict TOKEN, and left the three SIGNAL traps exiting
# 130/143/129 with no token at all — the same guarantee stopping short of what a consumer
# reads, one exit path over. Worse, the traps were phase-blind: a signal arriving AFTER the
# atomic rename left the state CHANGED while the caller was told nothing at all.
#
# EVERY PROBE IS DETERMINISTIC, NOT TIMED. A PATH-shimmed external command signals the script
# itself (`kill -TERM $PPID`, which inside a command substitution IS the script — measured), so
# the signal lands at a KNOWN point in the sequence rather than after a sleep the scheduler may
# reorder. The artifact is substituted; no seam is added to the shipped script.
sig_shim() {  # sig_shim <cmd> <when: before|after> [<arg-substring-filter>]
  local c="$1" when="$2" filt="${3:-}" d="$T/sigbin-$1-$2"
  mkdir -p "$d"
  { printf '#!/bin/sh\n'
    if [ -n "$filt" ]; then
      printf 'case "$*" in *%s*) : ;; *) exec %s "$@" ;; esac\n' "$filt" "$(command -v "$c")"
      printf '[ ! -f "%s/fired" ] || exec %s "$@"\n' "$d" "$(command -v "$c")"
      printf ': >"%s/fired"\n' "$d"
    fi
    if [ "$when" = before ]; then
      printf 'kill -TERM "$PPID"\n'
      printf 'exec %s "$@"\n' "$(command -v "$c")"
    else
      printf '%s "$@"; rc=$?\n' "$(command -v "$c")"
      printf 'kill -TERM "$PPID"\n'
      printf 'exit "$rc"\n'
    fi; } >"$d/$c"
  chmod +x "$d/$c"
  printf '%s\n' "$d"
}
# sig_run <dir> <shimdir> <args...> — prints the combined output and RETURNS the run's exit
# status. Deliberately not a global: this function is called inside `$( )`, so a global set
# here would be set in the SUBSHELL and lost — the same shape the script's own case 17 pins.
sig_run() {
  local d="$1" sd="$2"; shift 2
  local out rc
  out=$( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd:$PATH" \
    bash "$DS" "$@" 2>&1 ); rc=$?
  printf '%s\n' "$out"
  return "$rc"
}
# PHASE 1 — BEFORE the rename. `flock` is the last external command before any bytes are
# assembled, so nothing has been committed: the honest verdict is ERROR and the lane must be
# untouched. It is chosen over `mktemp` for the reason the DECLARED RESIDUAL below measures —
# `flock` is a PLAIN command, so the trap actually runs.
L33A=$(lane lane33-pre)
s33a=$(sig_run "$L33A" "$(sig_shim flock before)" write 3822 --stage implement); r33a=$?
if [ "$r33a" -eq 143 ] && [ "$(verdict_count "$s33a")" = 1 ] && [ "$(verdict_of "$s33a")" = ERROR ] \
   && all_lines_anchored "$s33a" && [ ! -f "$L33A/$MARKER" ]; then
  ok "SIGTERM BEFORE the atomic rename: exactly one anchored 'verdict ERROR', exit 143, and NOTHING written"
else
  bad "pre-rename signal: rc=$r33a verdicts=$(verdict_count "$s33a") token=$(verdict_of "$s33a") marker=$([ -f "$L33A/$MARKER" ] && echo present || echo absent)
$s33a"
fi
# PHASE 2 — DURING the rename. The signal is DEFERRED across the single commit so the run can
# report the outcome it actually achieved, instead of an 'undetermined'. Exactly one verdict,
# and it is the TRUE one; the exit code is still the signal's.
L33B=$(lane lane33-mid)
s33b=$(sig_run "$L33B" "$(sig_shim mv after)" write 3822 --stage implement); r33b=$?
if [ "$r33b" -eq 143 ] && [ "$(verdict_count "$s33b")" = 1 ] && [ "$(verdict_of "$s33b")" = WRITTEN ] \
   && all_lines_anchored "$s33b" && [ -f "$L33B/$MARKER" ]; then
  ok "SIGTERM DURING the atomic rename is DEFERRED across it: one anchored 'verdict WRITTEN', exit 143, and the marker IS on disk"
else
  bad "mid-rename signal: rc=$r33b verdicts=$(verdict_count "$s33b") token=$(verdict_of "$s33b") marker=$([ -f "$L33B/$MARKER" ] && echo present || echo absent)
$s33b"
fi
# PHASE 3 — AFTER the rename, before the verdict. This is the window the finding names: the
# state is CHANGED and the caller used to be told nothing. The `rm` of the carried body runs
# there, and only on a write over an ALREADY-OWNED marker, so the lane is primed first.
L33C=$(lane lane33-post)
run "$L33C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage groom >/dev/null 2>&1
s33c=$(sig_run "$L33C" "$(sig_shim rm before drive-issue-body)" write 3822 --stage implement); r33c=$?
if [ "$r33c" -eq 143 ] && [ "$(verdict_count "$s33c")" = 1 ] && [ "$(verdict_of "$s33c")" = WRITTEN ] \
   && all_lines_anchored "$s33c" && grep -q '^stage: implement$' "$L33C/$MARKER"; then
  ok "SIGTERM AFTER the atomic rename reports the COMPLETED write: one anchored 'verdict WRITTEN', exit 143, and the new stage is on disk"
else
  bad "post-rename signal: rc=$r33c verdicts=$(verdict_count "$s33c") token=$(verdict_of "$s33c")
$s33c
$(cat "$L33C/$MARKER" 2>/dev/null)"
fi
# DECLARED RESIDUAL, MEASURED RATHER THAN REASONED ABOUT. On bash 5.2 a trapped signal that
# arrives while the shell waits for a COMMAND SUBSTITUTION inside a FUNCTION is DISCARDED: the
# trap never runs, and the only trace is that the substitution reports failure — so a TERM
# during `mkout="$(mktemp ...)"` surfaces as a spurious "cannot create a temporary file". This
# is a bash property, not something this script can fix from inside, and it is why the ONE
# window that changes durable state (the rename) was moved OUT of a command substitution. What
# is asserted here is that the CONTRACT still holds on that path: exactly one anchored verdict,
# and nothing written.
L33D=$(lane lane33-substitution)
s33d=$(sig_run "$L33D" "$(sig_shim mktemp before)" write 3822); r33d=$?
if [ "$(verdict_count "$s33d")" = 1 ] && [ "$(verdict_of "$s33d")" = ERROR ] \
   && all_lines_anchored "$s33d" && [ ! -f "$L33D/$MARKER" ]; then
  ok "DECLARED RESIDUAL: a signal during a command substitution is swallowed by bash (rc=$r33d, not 143) — the contract still holds: one anchored ERROR, nothing written"
else
  bad "the swallowed-signal path broke the contract: rc=$r33d verdicts=$(verdict_count "$s33d") token=$(verdict_of "$s33d")
$s33d"
fi
# NON-VACUITY: the same shim WITHOUT the kill lets the write complete normally, so the three
# results above are about the signal and not about a shim that breaks the command.
L33N=$(lane lane33-nv)
nv33="$T/sigbin-nokill"; mkdir -p "$nv33"
{ printf '#!/bin/sh\n'; printf 'exec %s "$@"\n' "$(command -v mktemp)"; } >"$nv33/mktemp"
chmod +x "$nv33/mktemp"
s33n=$(sig_run "$L33N" "$nv33" write 3822); r33n=$?
if [ "$r33n" -eq 0 ] && [ "$(verdict_of "$s33n")" = WRITTEN ] && [ "$(verdict_count "$s33n")" = 1 ]; then
  ok "NON-VACUITY: the same PATH shim without the kill writes normally (rc 0, one WRITTEN verdict)"
else
  bad "the signal fixture is broken independently of the signal: rc=$r33n token=$(verdict_of "$s33n")
$s33n"
fi

# ===========================================================================
case_begin 34-shift-never-leaks-bash-diagnostics "an option with a MISSING value emits the anchored USAGE line and nothing of bash's own"
# ===========================================================================
# roborev job 30 G3, and the SECOND instance of round 5's F2 class: that round captured or
# suppressed 21 EXTERNAL commands' native stderr and left the SHELL's own. `shift 2` past the
# end returns non-zero, and bash prints its own UNPREFIXED `shift: 2: shift count out of range`
# BEFORE the anchored `|| die_usage` message whenever `shift_verbose` or POSIX mode is on.
#
# REACHABLE WITHOUT A HOSTILE INVOKER, which is what makes it a defect rather than a note:
# `BASHOPTS` is read from the ENVIRONMENT by every non-interactive bash at startup, so a
# caller (a wrapper script, a CI step, an exported shell profile) turns it on for every child
# without touching this file. The fix is to validate the argument COUNT before shifting, which
# makes the behaviour environment-independent; the shim here is only how the latent breakage is
# made observable.
sv_run() {  # sv_run <dir> <args...> — run under shift_verbose; prints output, returns rc
  local d="$1"; shift
  local out rc
  out=$( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" BASHOPTS=shift_verbose \
    bash "$DS" "$@" 2>&1 ); rc=$?
  printf '%s\n' "$out"
  return "$rc"
}
L34=$(lane lane34)
# EVERY option site in the file, plus every subcommand invoked with NO issue argument (the
# leading `shift`). Driven from a TABLE so a new option is covered by adding a row, not by
# remembering to write a case.
SHIFT_ROWS="write:--stage
write:--request-id
write:--pr
write:--branch
write:--body-file
write:--actor
write:--clear
verify:--actor
adopt:--reason
adopt:--actor
write:
verify:
adopt:
show:"
sv_fail=0; sv_rows=0
for row in $SHIFT_ROWS; do
  sub="${row%%:*}"; opt="${row#*:}"
  sv_rows=$((sv_rows + 1))
  if [ -n "$opt" ]; then
    out=$(sv_run "$L34" "$sub" 3822 "$opt"); rc=$?
  else
    out=$(sv_run "$L34" "$sub"); rc=$?
  fi
  why=''
  all_lines_anchored "$out" || why="$why unanchored-line"
  [ "$(verdict_count "$out")" = 1 ] || why="$why verdicts=$(verdict_count "$out")"
  [ "$(verdict_of "$out")" = USAGE ] || why="$why token=$(verdict_of "$out")"
  [ "$rc" -eq 64 ] || why="$why rc=$rc"
  if [ -n "$why" ]; then
    sv_fail=$((sv_fail + 1))
    printf 'note   %s %s ->%s\n%s\n' "$sub" "${opt:-<no-issue-arg>}" "$why" "$(printf '%s' "$out" | cat -v)"
  fi
done
if [ "$sv_fail" -eq 0 ]; then
  ok "all $sv_rows shift sites emit ONE anchored 'verdict USAGE' and exit 64 — no bash diagnostic escapes the anchor"
else
  bad "$sv_fail of $sv_rows shift sites leaked a bash diagnostic, emitted no/two verdicts, or used the wrong exit code"
fi
# NON-VACUITY IN TWO DIRECTIONS. (i) The shim really is on: an equivalent `shift 2` past the end
# in a throwaway script DOES print bash's unprefixed diagnostic under the same environment, so a
# clean sweep above is a property of the script and not of an inert fixture.
sv_probe="$T/shift-probe.sh"; printf 'set -- one\nshift 2 || true\n' >"$sv_probe"
sv_ctl=$(env BASHOPTS=shift_verbose bash "$sv_probe" 2>&1)
if printf '%s\n' "$sv_ctl" | grep -q 'shift count out of range'; then
  ok "POSITIVE CONTROL: the same environment DOES make bash print an unprefixed shift diagnostic, so the sweep above measured something"
else
  bad "shift_verbose did not reach the child at all — the sweep above proves nothing (control output: $sv_ctl)"
fi
# (ii) The COUNT guard did not turn a valid option into a usage error.
sv_ok=$(sv_run "$L34" write 3822 --stage implement); sv_okrc=$?
sv_ok2=$(sv_run "$L34" write 3822 --clear stage); sv_okrc2=$?
sv_ok3=$(sv_run "$L34" show 3822); sv_okrc3=$?
if [ "$sv_okrc" -eq 0 ] && [ "$(verdict_of "$sv_ok")" = WRITTEN ] \
   && [ "$sv_okrc2" -eq 0 ] && [ "$(verdict_of "$sv_ok2")" = WRITTEN ] \
   && [ "$sv_okrc3" -eq 0 ] && [ "$(verdict_of "$sv_ok3")" = SHOWN ]; then
  ok "NON-VACUITY: options WITH their values still work under the same environment (write/--clear/show all succeed)"
else
  bad "the count guard rejects valid invocations: write=$sv_okrc/$(verdict_of "$sv_ok") clear=$sv_okrc2/$(verdict_of "$sv_ok2") show=$sv_okrc3/$(verdict_of "$sv_ok3")"
fi

# ===========================================================================
case_begin 35-one-verdict-per-failure-mode "TABLE: every failure mode emits EXACTLY ONE closed-set verdict token, with the documented exit code"
# ===========================================================================
# THE CLASS COVERAGE, not another point fix. Rounds 5 and 6 both found a round-N fix that had
# reached ONE site of its own class: F1 gave the liveness-library guard a token and left the
# signal traps without one; F2 captured 21 external commands' stderr and left `shift`'s. What
# stops that regenerating is not another per-site case but a TABLE over the INVARIANT — so a
# NEW exit path is covered by adding a ROW, and a new exit path added with no row is what the
# row-count floor below reds on.
#
# The table is EXECUTED ONCE and its outputs are kept on disk, because case 36 asserts a
# DIFFERENT invariant over the SAME runs: two properties of one observation, never two
# observations that might disagree about which run they describe.
INV_DIR="$T/invariants"; mkdir -p "$INV_DIR"
INV_ROWS="bad-issue:USAGE:64
missing-option-value:USAGE:64
unknown-option:USAGE:64
unknown-subcommand:USAGE:64
no-subcommand:USAGE:64
body-carries-sentinel:USAGE:64
unreadable-body-file:ERROR:1
absent-marker:ABSENT:3
marker-not-regular:ERROR:1
unstamped-marker:UNSTAMPED:8
displaced-sentinel:MALFORMED:8
malformed-marker:MALFORMED:8
duplicate-sentinel:DUPLICATE-SENTINEL:8
foreign-issue:FOREIGN-ISSUE:4
foreign-machine:FOREIGN-MACHINE:4
unmeasurable-machine:ERROR:1
lossy-machine:ERROR:1
lossy-session:ERROR:1
foreign-worktree:FOREIGN-WORKTREE:4
adoptable:ADOPTABLE:5
live-peer:LIVE-PEER:6
liveness-unknown:LIVENESS-UNKNOWN:7
missing-liveness-library:ERROR:1
failing-sentinel-scan:ERROR:1
no-flock:ERROR:1
failing-date:ERROR:1
failing-mktemp:ERROR:1
signal-before-rename:ERROR:143
signal-after-rename:WRITTEN:143
write-succeeds:WRITTEN:0
verify-owned:OWNED:0
show-fields:SHOWN:0
adopt-succeeds:ADOPTED:0
lock-file-unusable:ERROR:1"
# An unwritable worktree is only measurable when permissions apply to us: root bypasses them,
# so the row is DECLARED unavailable rather than passing vacuously.
if [ "$(id -u)" -ne 0 ]; then
  INV_ROWS="$INV_ROWS
unwritable-worktree:ERROR:1"
else
  ok "DECLARED: the 'unwritable-worktree' row is unavailable under root (permissions are bypassed) — it is omitted rather than asserted vacuously"
fi
inv_bin() {  # inv_bin <cmd> <body-line> — a PATH dir holding one shim
  local c="$1" body="$2" d="$INV_DIR/bin-$1-$$-$RANDOM"
  mkdir -p "$d"
  { printf '#!/bin/sh\n'; printf '%s\n' "$body"; } >"$d/$c"
  chmod +x "$d/$c"
  printf '%s\n' "$d"
}
inv_exec() {  # inv_exec <dir> <script> <shimdir|''> <session> <pid> <args...>
  local d="$1" sc="$2" sd="$3" ss="$4" sp="$5"; shift 5
  ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
      ${ss:+"CLAUDE_CODE_SESSION_ID=$ss"} ${sp:+"CLAUDE_PID=$sp"} ${sd:+"PATH=$sd:$PATH"} \
      bash "$sc" "$@" 2>&1 )
}
inv_run() {  # inv_run <row-name> — set the state up, run it, print output, return rc
  local n="$1" d other dead sl scr sd
  d=$(lane "inv-$n")
  case "$n" in
    bad-issue)             inv_exec "$d" "$DS" '' "$SESS_A" $$ verify not-a-number ;;
    missing-option-value)  inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 --stage ;;
    unknown-option)        inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 --nope ;;
    unknown-subcommand)    inv_exec "$d" "$DS" '' "$SESS_A" $$ frobnicate ;;
    no-subcommand)         inv_exec "$d" "$DS" '' "$SESS_A" $$ ;;
    body-carries-sentinel)
      printf 'notes\n%s\nmore\n' "$sentinel" >"$INV_DIR/body-bad.md"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 --body-file "$INV_DIR/body-bad.md" ;;
    unreadable-body-file)
      # roborev job 37 J2, in the TABLE as well as its own case: a source whose READ fails must
      # refuse with one anchored token, never commit an empty body.
      printf 'plan\n' >"$INV_DIR/body-unreadable.md"
      sd=$(inv_bin cat 'case "$*" in *body-unreadable.md*) exit 1;; esac
exec '"$(command -v cat)"' "$@"')
      inv_exec "$d" "$DS" "$sd" "$SESS_A" $$ write 3822 --body-file "$INV_DIR/body-unreadable.md" ;;
    absent-marker)         inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    marker-not-regular)    mkdir -p "$d/$MARKER"; inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    unstamped-marker)      printf 'legacy plan\n' >"$d/$MARKER"; inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    displaced-sentinel)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      { printf '\n'; cat "$d/$MARKER"; } >"$INV_DIR/t" && mv "$INV_DIR/t" "$d/$MARKER"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    malformed-marker)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      grep -vFx -- "$sentinel_end" "$d/$MARKER" >"$INV_DIR/t" && mv "$INV_DIR/t" "$d/$MARKER"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    duplicate-sentinel)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      { printf '%s\n' "$sentinel"; printf 'issue: 3822\n'; } >>"$d/$MARKER"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    foreign-issue)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 9999 >/dev/null 2>&1
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    foreign-machine)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxB \
          "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$DS" verify 3822 2>&1 ) ;;
    unmeasurable-machine)
      # roborev job 34 H1, added to the TABLE rather than only to its own case: the class
      # lesson on this lane is that a fix reaches one site and the next round finds the same
      # defect one exit path over, so every new failure mode joins the one-verdict/anchor sweep.
      sd=$(inv_bin hostname 'exit 1')
      ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID -u CLAIM_MACHINE \
          "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd:$PATH" \
          bash "$DS" write 3822 2>&1 ) ;;
    lossy-machine)
      # roborev job 37 J1, added to the TABLE and not only to its own case: on this lane the
      # class lesson is that a fix reaches one site and the next round finds the same defect one
      # exit path over, so every new failure mode joins the one-verdict/anchor sweep.
      ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID "CLAIM_MACHINE=build box" \
          "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$DS" write 3822 2>&1 ) ;;
    lossy-session)
      ( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
          "CLAUDE_CODE_SESSION_ID=sess a" "CLAUDE_PID=$$" bash "$DS" write 3822 2>&1 ) ;;
    foreign-worktree)
      other=$(lane "inv-$n-other")
      inv_exec "$other" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      cp "$other/$MARKER" "$d/$MARKER"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    adoptable | adopt-succeeds)
      sleep 30 & dead=$!
      inv_exec "$d" "$DS" '' "$SESS_A" "$dead" write 3822 >/dev/null 2>&1
      kill "$dead" 2>/dev/null; wait "$dead" 2>/dev/null
      if [ "$n" = adoptable ]; then
        inv_exec "$d" "$DS" '' "$SESS_B" $$ verify 3822
      else
        inv_exec "$d" "$DS" '' "$SESS_B" $$ adopt 3822 --reason invariant-table:writer-gone
      fi ;;
    live-peer)
      sleep 300 & sl=$!
      inv_exec "$d" "$DS" '' "$SESS_A" "$sl" write 3822 >/dev/null 2>&1
      inv_exec "$d" "$DS" '' "$SESS_B" $$ verify 3822; local rc=$?
      kill "$sl" 2>/dev/null; wait "$sl" 2>/dev/null
      return "$rc" ;;
    liveness-unknown)
      inv_exec "$d" "$DS" '' "$SESS_A" '' write 3822 >/dev/null 2>&1
      inv_exec "$d" "$DS" '' "$SESS_B" $$ verify 3822 ;;
    missing-liveness-library)
      scr="$INV_DIR/nolib"; mkdir -p "$scr"; cp "$DS" "$scr/drive-issue-state.sh"
      inv_exec "$d" "$scr/drive-issue-state.sh" '' "$SESS_A" $$ verify 3822 ;;
    failing-sentinel-scan)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      { printf '\n'; cat "$d/$MARKER"; } >"$INV_DIR/t" && mv "$INV_DIR/t" "$d/$MARKER"
      inv_exec "$d" "$DS" "$G1SHIM" "$SESS_A" $$ write 3822 ;;
    no-flock)
      sd=$(inv_bin flock 'exit 1')
      inv_exec "$d" "$DS" "$sd" "$SESS_A" $$ write 3822 ;;
    failing-date)
      sd=$(inv_bin date 'echo "date: simulated" >&2; exit 1')
      inv_exec "$d" "$DS" "$sd" "$SESS_A" $$ write 3822 ;;
    failing-mktemp)
      sd=$(inv_bin mktemp 'echo "mktemp: simulated" >&2; exit 1')
      inv_exec "$d" "$DS" "$sd" "$SESS_A" $$ write 3822 ;;
    signal-before-rename)
      inv_exec "$d" "$DS" "$(sig_shim flock before)" "$SESS_A" $$ write 3822 ;;
    signal-after-rename)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 --stage groom >/dev/null 2>&1
      # sig_shim's filtered form fires ONCE per shim directory, and case 33 already spent this
      # one — a shared fixture that silently no-ops is a row that proves nothing, so the
      # once-stamp is cleared explicitly rather than relied upon to be fresh.
      sd=$(sig_shim rm before drive-issue-body); rm -f "$sd/fired"
      inv_exec "$d" "$DS" "$sd" "$SESS_A" $$ write 3822 --stage implement ;;
    write-succeeds)        inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 --stage implement ;;
    verify-owned)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      inv_exec "$d" "$DS" '' "$SESS_A" $$ verify 3822 ;;
    show-fields)
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 >/dev/null 2>&1
      inv_exec "$d" "$DS" '' "$SESS_A" $$ show 3822 ;;
    lock-file-unusable)
      # A FAILED REDIRECTION IS A NATIVE DIAGNOSTIC TOO — found by sweeping G3's class, not by
      # the finding. `: >>"$lock" 2>/dev/null` applies the failing redirection BEFORE stderr is
      # diverted, so bash prints its own unprefixed line. Making the lock path a DIRECTORY is
      # the root-proof way to make the redirection fail (permissions are bypassed under root,
      # `EISDIR` is not).
      mkdir -p "$d/$MARKER.lock"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822 ;;
    unwritable-worktree)
      chmod 555 "$d"
      inv_exec "$d" "$DS" '' "$SESS_A" $$ write 3822; local wrc=$?
      chmod 755 "$d" 2>/dev/null || true
      return "$wrc" ;;
    *) printf 'DRIVE-STATE: verdict UNROUTED-TABLE-ROW\n'; return 99 ;;
  esac
}
inv_names=''
for row in $INV_ROWS; do
  nm="${row%%:*}"
  inv_run "$nm" >"$INV_DIR/$nm.out" 2>&1; printf '%s\n' "$?" >"$INV_DIR/$nm.rc"
  inv_names="$inv_names $nm"
done
inv_fail=0; inv_count=0
for row in $INV_ROWS; do
  nm="${row%%:*}"; rest="${row#*:}"; want_v="${rest%%:*}"; want_rc="${rest##*:}"
  inv_count=$((inv_count + 1))
  out="$(cat "$INV_DIR/$nm.out")"; rc="$(cat "$INV_DIR/$nm.rc")"
  why=''
  [ "$(verdict_count "$out")" = 1 ] || why="$why verdicts=$(verdict_count "$out")"
  got="$(verdict_of "$out")"
  verdict_in_set "$got" || why="$why token-outside-closed-set='$got'"
  [ "$got" = "$want_v" ] || why="$why token=$got(want $want_v)"
  [ "$rc" = "$want_rc" ] || why="$why rc=$rc(want $want_rc)"
  if [ -n "$why" ]; then
    inv_fail=$((inv_fail + 1))
    printf 'note   row %s ->%s\n%s\n' "$nm" "$why" "$(printf '%s' "$out" | cat -v)"
  fi
done
if [ "$inv_fail" -eq 0 ]; then
  ok "all $inv_count table rows emit EXACTLY ONE closed-set verdict token with the documented exit code"
else
  bad "$inv_fail of $inv_count table rows broke the one-verdict/closed-set/exit-code invariant"
fi
# ROW FLOOR, the same idea as the case floor: a span-replacing edit that silently drops rows
# otherwise leaves a green tally over a shrunken table.
if [ "$inv_count" -ge 33 ]; then
  ok "TABLE FLOOR: $inv_count failure modes exercised (floor 33) — including all four success verdicts, both signal phases and every refusal token"
else
  bad "table floor breached: only $inv_count rows ran"
fi
# COMPLETENESS AGAINST THE GRAMMAR: every token in the CLOSED set must be produced by some row,
# so a verdict added to the script without a row here reds instead of joining uncovered.
inv_missing=''
for t in $VERDICT_SET; do
  found=0
  for row in $INV_ROWS; do
    nm="${row%%:*}"; [ "$(verdict_of "$(cat "$INV_DIR/$nm.out")")" = "$t" ] && { found=1; break; }
  done
  [ "$found" -eq 1 ] || inv_missing="$inv_missing $t"
done
if [ -z "$inv_missing" ]; then
  ok "every token in the CLOSED verdict set is actually produced by a row in this table"
else
  bad "verdict tokens produced by NO row (a token joined the grammar uncovered):$inv_missing"
fi

# ===========================================================================
case_begin 36-anchor-holds-on-every-stream "TABLE: the DRIVE-STATE: anchor holds on stdout AND stderr for every failure mode"
# ===========================================================================
# The second invariant over the SAME observations case 35 recorded. Contract (a) and contract
# (c) are independent — round 5's F1 was a line that satisfied (a) and not (c) — so they get
# independent assertions rather than one combined check that could pass on either.
anch_fail=0; anch_rows=0; anch_lines=0
for row in $INV_ROWS; do
  nm="${row%%:*}"
  anch_rows=$((anch_rows + 1))
  out="$(cat "$INV_DIR/$nm.out")"
  anch_lines=$((anch_lines + $(printf '%s\n' "$out" | grep -c '' || true)))
  if all_lines_anchored "$out"; then :; else
    anch_fail=$((anch_fail + 1))
    printf 'note   row %s emitted an UNPREFIXED line:\n%s\n' "$nm" "$(printf '%s' "$out" | grep -v '^DRIVE-STATE: ' | grep -v '^$' | cat -v)"
  fi
done
if [ "$anch_fail" -eq 0 ]; then
  ok "no unprefixed line on stdout+stderr combined across all $anch_rows table rows"
else
  bad "$anch_fail of $anch_rows rows emitted at least one line without the DRIVE-STATE: anchor"
fi
if [ "$anch_lines" -ge 60 ]; then
  ok "NON-VACUITY: $anch_lines output lines were actually inspected — the sweep is not passing on empty output"
else
  bad "only $anch_lines lines were inspected across $anch_rows rows: the rows cannot have produced real output"
fi

# ===========================================================================
case_begin 37-machine-axis-must-be-measurable "an UNMEASURABLE machine axis is refused, never committed as the 'unspecified' placeholder (roborev job 34 H1)"
# ===========================================================================
# THE DEFECT: `hostname -s` failing (or printing nothing) with CLAIM_MACHINE unset made the
# machine axis sanitize to the `unspecified` PLACEHOLDER, and the writer COMMITTED it. Two
# consequences in a file whose whole subject is ownership: a transient failure writes a stamp
# that becomes FOREIGN-MACHINE the moment hostname resolution recovers (the lane locks ITSELF
# out), and a PERSISTENT failure ALIASES EVERY BOX ONTO ONE OWNER — lane A's marker then
# verifies as OWNED on machine B, which is the peer-adoption defect this issue exists to close.
# THE ARTIFACT IS SUBSTITUTED (a PATH shim), never a settable seam.
mkm_fail=0
hostname_shim() {  # hostname_shim <kind> -> a PATH dir whose `hostname` fails|prints nothing|works
  local kind="$1" d="$T/hnbin-$1"
  mkdir -p "$d"
  case "$kind" in
    fail)  { printf '#!/bin/sh\n'; printf 'exit 1\n'; } >"$d/hostname" ;;
    empty) { printf '#!/bin/sh\n'; printf 'printf ""\n'; printf 'exit 0\n'; } >"$d/hostname" ;;
    real)  { printf '#!/bin/sh\n'; printf 'printf "realbox\\n"\n'; } >"$d/hostname" ;;
  esac
  chmod +x "$d/hostname"
  printf '%s\n' "$d"
}
run_nomachine() {  # run_nomachine <dir> <shim-dir> <args...> — CLAIM_MACHINE UNSET
  local dir="$1" sd="$2"; shift 2
  ( cd "$dir" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID -u CLAIM_MACHINE \
      "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd:$PATH" \
      bash "$DS" "$@" 2>&1 )
}
for hk in fail empty; do
  hsd="$(hostname_shim "$hk")"
  L37=$(lane "lane37-$hk")
  m37_out=$(run_nomachine "$L37" "$hsd" write 3822); m37_rc=$?
  if [ "$m37_rc" -ne 0 ] && [ "$(verdict_of "$m37_out")" = ERROR ] \
     && [ ! -e "$L37/$MARKER" ] && all_lines_anchored "$m37_out"; then
    ok "hostname '$hk' + no CLAIM_MACHINE: write refuses ERROR and commits NO marker"
  else
    mkm_fail=1
    bad "hostname '$hk': write did not refuse, or committed a marker: rc=$m37_rc verdict=$(verdict_of "$m37_out") marker=$([ -e "$L37/$MARKER" ] && echo present || echo absent)
$m37_out"
  fi
  if printf '%s\n' "$m37_out" | grep -q 'axis=machine'; then
    ok "hostname '$hk': the refusal NAMES the axis (axis=machine)"
  else
    bad "hostname '$hk': the refusal does not name the axis:
$m37_out"
  fi
done
# NON-VACUITY: the SAME lane, the same everything, with a WORKING hostname shim, writes.
hsd_ok="$(hostname_shim real)"
L37OK=$(lane lane37-real)
m37ok_out=$(run_nomachine "$L37OK" "$hsd_ok" write 3822); m37ok_rc=$?
if [ "$m37ok_rc" -eq 0 ] && [ "$(verdict_of "$m37ok_out")" = WRITTEN ] \
   && grep -q '^machine: realbox$' "$L37OK/$MARKER" 2>/dev/null; then
  ok "NON-VACUITY: a WORKING hostname writes normally (machine: realbox) — the refusals above are about the unmeasurable value, not the shim"
else
  bad "the working-hostname control did not write: rc=$m37ok_rc verdict=$(verdict_of "$m37ok_out")
$m37ok_out"
fi
# THE ALIAS, DEMONSTRATED: a marker RECORDING the placeholder must NOT verify as OWNED just
# because this box cannot measure its own name either. Without the use-site refusal both sides
# read `unspecified` and the axis comparison SUCCEEDS — every box owning every marker.
L37A=$(lane lane37-alias)
run "$L37A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
LC_ALL=C sed -i.bak 's/^machine: boxA$/machine: unspecified/' "$L37A/$MARKER" && rm -f "$L37A/$MARKER.bak"
cp "$L37A/$MARKER" "$T/alias-before.md"
hsd_f="$(hostname_shim fail)"
a37_out=$(run_nomachine "$L37A" "$hsd_f" verify 3822); a37_rc=$?
if [ "$a37_rc" -ne 0 ] && [ "$(verdict_of "$a37_out")" = ERROR ]; then
  ok "a recorded 'unspecified' machine does NOT alias onto an unmeasurable current machine: verify refuses ERROR (not OWNED)"
else
  bad "the placeholder ALIASED: verify rc=$a37_rc verdict=$(verdict_of "$a37_out") (OWNED here means every box owns every marker)
$a37_out"
fi
# NOTHING IS MUTATED BY A REFUSAL — byte-for-byte, not a line count.
w37_out=$(run_nomachine "$L37A" "$hsd_f" write 3822 --stage implement); w37_rc=$?
d37_out=$(run_nomachine "$L37A" "$hsd_f" adopt 3822 --reason cron-reinvoke:machine-unmeasurable); d37_rc=$?
if [ "$w37_rc" -ne 0 ] && [ "$(verdict_of "$w37_out")" = ERROR ] \
   && [ "$d37_rc" -ne 0 ] && [ "$(verdict_of "$d37_out")" = ERROR ] \
   && cmp -s "$T/alias-before.md" "$L37A/$MARKER"; then
  ok "write AND adopt both refuse ERROR and the marker is BYTE-IDENTICAL afterwards (nothing was committed)"
else
  bad "a refused write/adopt mutated the marker or produced a non-ERROR verdict: w_rc=$w37_rc w=$(verdict_of "$w37_out") a_rc=$d37_rc a=$(verdict_of "$d37_out")
$w37_out
$d37_out"
fi

# ===========================================================================
case_begin 38-adopt-never-calls-a-live-owner-gone "adopt is a re-entrant NO-OP for BOTH ownership bases; it may only rewrite when the writer is provably GONE (roborev job 34 H2)"
# ===========================================================================
# THE DEFECT: `check_ownership` returns success for THREE different facts — the recorded
# session IS me, the recorded pid is MY OWN LIVE pid (round 5's same-process branch), and
# (adopt mode) the recorded writer is provably GONE — and collapsed them into one undifferentiated
# `return 0`. `cmd_adopt` then re-derived "is it already mine?" from the SESSION ID ALONE, so a
# changed or unrecorded session with the SAME LIVE PID took the adoption path: it rewrote the
# marker, recorded a STILL-RUNNING process as `prior-session-pid`, and printed that the writer
# was "provably gone". That is a FALSE STATEMENT in the audit record this script exists to
# produce, and recording a live owner as the prior one is exactly what LIVE-PEER refuses.
GONE_TEXT='provably gone'
# (A) recorded session differs, recorded pid is THIS live process.
L38A=$(lane lane38-a)
run "$L38A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement >/dev/null 2>&1
cp "$L38A/$MARKER" "$T/38a-before.md"
a38_out=$(run "$L38A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason cron-reinvoke:same-process); a38_rc=$?
# (B) NO session id recorded at all, recorded pid is THIS live process (the same-process branch
# exists precisely for a session with CLAUDE_PID set and CLAUDE_CODE_SESSION_ID unset).
L38B=$(lane lane38-b)
( cd "$L38B" && env -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA "CLAUDE_PID=$$" bash "$DS" write 3822 >/dev/null 2>&1 )
cp "$L38B/$MARKER" "$T/38b-before.md"
b38_out=$( cd "$L38B" && env -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA "CLAUDE_PID=$$" \
  bash "$DS" adopt 3822 --reason cron-reinvoke:same-process-no-session 2>&1 ); b38_rc=$?
for probe in A B; do
  case "$probe" in
    A) p_out="$a38_out"; p_rc="$a38_rc"; p_lane="$L38A"; p_before="$T/38a-before.md" ;;
    B) p_out="$b38_out"; p_rc="$b38_rc"; p_lane="$L38B"; p_before="$T/38b-before.md" ;;
  esac
  if [ "$p_rc" -eq 0 ] && [ "$(verdict_of "$p_out")" = ADOPTED ] \
     && ! printf '%s\n' "$p_out" | grep -q "$GONE_TEXT"; then
    ok "($probe) adopt over a marker whose writer is THIS LIVE PROCESS does NOT claim the writer was gone"
  else
    bad "($probe) adopt claimed a live writer was gone (or did not succeed re-entrantly): rc=$p_rc verdict=$(verdict_of "$p_out")
$p_out"
  fi
  if cmp -s "$p_before" "$p_lane/$MARKER"; then
    ok "($probe) the re-entrant adopt changed NOTHING — the marker is byte-identical"
  else
    bad "($probe) the re-entrant adopt rewrote the marker:
$(diff "$p_before" "$p_lane/$MARKER" || true)"
  fi
  if grep -q "^prior-session-pid: $$\$" "$p_lane/$MARKER" 2>/dev/null; then
    bad "($probe) a STILL-RUNNING process ($$) was recorded as prior-session-pid:
$(cat "$p_lane/$MARKER")"
  else
    ok "($probe) no live process was recorded as prior-session-pid"
  fi
done
# NEGATIVE CONTROL — a GENUINELY DEAD writer still adopts, with the provenance recorded. Without
# this the case above would pass by simply disabling adoption.
L38C=$(lane lane38-c)
sleep 30 & dead38=$!
run "$L38C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$dead38" -- write 3822 --stage implement >/dev/null 2>&1
kill "$dead38" 2>/dev/null; wait "$dead38" 2>/dev/null
c38_out=$(run "$L38C" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason cron-reinvoke:writer-pid-gone); c38_rc=$?
if [ "$c38_rc" -eq 0 ] && [ "$(verdict_of "$c38_out")" = ADOPTED ] \
   && printf '%s\n' "$c38_out" | grep -q "$GONE_TEXT" \
   && grep -q "^prior-session: $SESS_A\$" "$L38C/$MARKER" \
   && grep -q "^prior-session-pid: $dead38\$" "$L38C/$MARKER"; then
  ok "NEGATIVE CONTROL: a provably GONE writer is still adopted, and the hand-over provenance IS recorded — the distinction is real, not a disabled adopt"
else
  bad "the dead-writer adoption regressed: rc=$c38_rc verdict=$(verdict_of "$c38_out")
$c38_out
$(cat "$L38C/$MARKER" 2>/dev/null)"
fi
# The OTHER caller of the same result: `write` treats BOTH ownership bases as OWNED, and must
# keep doing so — the same-process basis is an AFFIRMATIVE measurement of sameness, not a
# weaker one, so making adopt re-entrant must not make write refuse its own marker.
w38_out=$(run "$L38A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- write 3822 --stage address); w38_rc=$?
if [ "$w38_rc" -eq 0 ] && [ "$(verdict_of "$w38_out")" = WRITTEN ] \
   && grep -q '^stage: address$' "$L38A/$MARKER"; then
  ok "SWEEP: the same-process basis is still OWNED for `write` (the fix differentiates the bases, it does not narrow ownership)"
else
  bad "write over the same-process basis regressed: rc=$w38_rc verdict=$(verdict_of "$w38_out")
$w38_out"
fi

# ===========================================================================
case_begin 39-body-bytes-survive-repeated-writes "the BODY survives an owned write BYTE-FOR-BYTE, repeatedly and across an adopt (roborev job 34 H3)"
# ===========================================================================
# THE DEFECT: `marker_body` returned everything after the end sentinel — INCLUDING the canonical
# blank separator the WRITER emits — and `write_marker` then always emitted its own separator
# before copying that body back. So every ordinary write added one blank line: measured 1, then
# 2, then 3 across three successive writes of the SAME body. The header promises the body
# SURVIVES an owned write; it survived but MUTATED, and it grew without bound over a long-running
# lane. Asserted BYTE-FOR-BYTE with cmp, never as a line count — the finding is that the bytes
# change.
# after_sentinel <marker> <out> — the marker's bytes AFTER the end sentinel's newline, VERBATIM.
#
# BYTE-EXACT BY CONSTRUCTION, AND DELIBERATELY NOT THE SCRIPT'S OWN MECHANISM (roborev job 35 I2).
# This helper used to be `awk -v s=... 'seen{print} $0==s{seen=1}'`, and awk ALWAYS terminates the
# last record it prints — so a body whose final line carries NO trailing newline came back with one
# ADDED. That is exactly the defect this case exists to catch, which means the verification shared
# the blind spot of the thing it verified: four `cmp`s over awk-extracted bytes could not have
# failed. Nothing here is line-oriented: `grep -b` reports a BYTE offset of the sentinel line and
# `tail -c` copies BYTES from there on, so no final line is re-terminated, no CRLF is rewritten and
# no trailing blank line is trimmed. It is also a DIFFERENT mechanism from the offset walk the
# script now uses, so a defect in one cannot cancel a defect in the other.
after_sentinel() {
  local off
  off=$(LC_ALL=C grep -abFx -e "$sentinel_end" "$1" 2>/dev/null | head -1 | cut -d: -f1)
  case "$off" in ''|*[!0-9]*) : >"$2"; return 1 ;; esac
  # `off` is the 0-based byte offset of the sentinel line's first byte; the body begins after the
  # sentinel's own bytes AND its newline. `tail -c +N` is 1-based, hence the +2.
  tail -c "+$(( off + ${#sentinel_end} + 2 ))" "$1" >"$2"
}
L39=$(lane lane39)
body39="$T/body39.md"
printf '## plan\n\n- step one\n- step two\n' >"$body39"
run "$L39" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --body-file "$body39" >/dev/null 2>&1
after_sentinel "$L39/$MARKER" "$T/39-r1"
# The canonical shape, stated once: exactly ONE writer-owned blank separator, then the body bytes.
{ printf '\n'; cat "$body39"; } >"$T/39-canonical"
if cmp -s "$T/39-r1" "$T/39-canonical"; then
  ok "the first write lays down exactly one writer-owned separator followed by the body verbatim"
else
  bad "the first write's post-sentinel bytes are not the canonical shape:
$(cmp -l "$T/39-canonical" "$T/39-r1" | head -5)
$(cat -A "$T/39-r1" | head -8)"
fi
b39_fail=0
i=2
while [ "$i" -le 4 ]; do
  run "$L39" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage "round$i" >/dev/null 2>&1
  after_sentinel "$L39/$MARKER" "$T/39-r$i"
  cmp -s "$T/39-r1" "$T/39-r$i" || { b39_fail=1; printf 'note   write #%s changed the body bytes:\n%s\n' "$i" "$(cat -A "$T/39-r$i" | head -8)"; }
  i=$((i + 1))
done
if [ "$b39_fail" -eq 0 ]; then
  ok "FOUR successive owned writes leave the post-sentinel bytes IDENTICAL (no monotonic separator growth)"
else
  bad "the body mutated across repeated writes — the separator is owned twice"
fi
# ACROSS AN ADOPT, which carries the body through the same path.
L39A=$(lane lane39-adopt)
sleep 30 & dead39=$!
run "$L39A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$dead39" -- write 3822 --body-file "$body39" >/dev/null 2>&1
kill "$dead39" 2>/dev/null; wait "$dead39" 2>/dev/null
after_sentinel "$L39A/$MARKER" "$T/39a-before"
run "$L39A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason cron-reinvoke:writer-pid-gone >/dev/null 2>&1
after_sentinel "$L39A/$MARKER" "$T/39a-after"
if cmp -s "$T/39a-before" "$T/39a-after" && cmp -s "$T/39a-after" "$T/39-canonical"; then
  ok "an adopt carries the body across BYTE-FOR-BYTE and adds no separator of its own"
else
  bad "the adopt mutated the body bytes:
$(cat -A "$T/39a-after" | head -8)"
fi
# NON-VACUITY: the comparisons above are over a REAL body, not an empty one.
if [ -s "$T/39-r1" ] && grep -q 'step two' "$T/39-r1"; then
  ok "NON-VACUITY: the compared region is the real body ($(wc -c <"$T/39-r1" | tr -d ' ') bytes), not an empty file"
else
  bad "the compared region is empty — the byte comparisons above prove nothing"
fi

# ===========================================================================
case_begin 40-worktree-axis-must-be-measurable "CLASS SWEEP of job 34 H1: an UNMEASURABLE worktree axis is refused by name, and derives no marker path at the filesystem root"
# ===========================================================================
# H1 is one instance of "a required identity axis silently degrades"; the sweep asked which OTHER
# axis can degrade. `issue` is a validated CLI argument, `session`/`session-pid` have DELIBERATE
# sentinels that route to the liveness resolution (never to a match), and `actor` is recorded but
# is not an ownership axis. The worktree axis was the reachable one: with the lane's directory
# deleted `pwd -P` fails, which (1) leaked bash's own unprefixed diagnostic, (2) killed the shell
# under `set -e` so the run ended on the EXIT-trap's GENERIC ERROR naming no axis, and (3) made
# marker_path compose "/$MARKER" — a read, and a would-be write, at the FILESYSTEM ROOT.
wt_gone_run() {  # wt_gone_run <subcommand...> — run with a DELETED working directory
  local g="$T/gone-$1-$RANDOM"
  mkdir -p "$g"
  ( cd "$g" && rmdir "$g" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
      "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$DS" "$@" 2>&1 )
}
# THE MATRIX IS DERIVED FROM THE SCRIPT'S OWN DISPATCH TABLE, NEVER CURATED (roborev job 43
# K2). This loop used to read `for sub in write verify show` — `adopt` was simply absent, which
# is how `adopt` came to call `lock_marker` BEFORE `require_worktree_axis` (deriving a lock path
# at the filesystem root and refusing with a generic "not writable" instead of the axis form the
# consumer contract in .claude/commands/drive-issue.md publishes) with a green suite. The names
# now come from the `case "$SUB"` arms, so a NEW subcommand joins this matrix with no test edit —
# and if its extra arguments are not declared in wt_extra_args it REDS rather than not being
# covered. A FAILED derivation is a FAIL, never a fallback to an empty set: an empty loop is a
# vacuous green, which is this file's standing rule (a positive verdict needs an affirmative
# measurement).
WT_SUBS=$(LC_ALL=C sed -n 's/^  \([a-z][a-z-]*\)) *shift; cmd_.*/\1/p' "$DS" | tr '\n' ' ')
wt_nsubs=0
for _s in $WT_SUBS; do wt_nsubs=$((wt_nsubs + 1)); done
wt_missing=''
for _req in write verify adopt show; do
  case " $WT_SUBS " in *" $_req "*) : ;; *) wt_missing="$wt_missing $_req" ;; esac
done
if [ "$wt_nsubs" -ge 4 ] && [ -z "$wt_missing" ]; then
  ok "DERIVED: the axis matrix took its $wt_nsubs subcommands from the script's own dispatch table ($WT_SUBS)"
else
  bad "the subcommand derivation FAILED (n=$wt_nsubs missing:$wt_missing set='$WT_SUBS') — the matrix below would run vacuously"
fi

# wt_extra_args <sub> — the arguments a subcommand needs to REACH its axis guard, or return 1
# for a subcommand nobody has declared. Declared per subcommand, so a new one cannot join the
# matrix silently: the loop below FAILs on a `return 1`.
wt_extra_args() {
  case "$1" in
    write | verify | show) printf '%s\n' '' ;;
    adopt) printf '%s\n' '--reason cron-reinvoke:writer-pid-gone' ;;
    *) return 1 ;;
  esac
}

wt_fail=0
for sub in $WT_SUBS; do
  if ! wt_xargs=$(wt_extra_args "$sub"); then
    wt_fail=1
    bad "subcommand '$sub' exists in the dispatch table but declares no arguments in wt_extra_args — it is NOT covered by the unmeasurable-worktree matrix"
    continue
  fi
  # shellcheck disable=SC2086 -- deliberate word splitting: the extra args are a token list
  g_out=$(wt_gone_run "$sub" 3822 $wt_xargs); g_rc=$?
  if [ "$g_rc" -ne 0 ] && [ "$(verdict_of "$g_out")" = ERROR ] \
     && printf '%s\n' "$g_out" | grep -q 'axis=worktree' \
     && [ "$(verdict_count "$g_out")" = 1 ]; then
    ok "'$sub' with a deleted worktree: exactly ONE verdict, ERROR, and it NAMES axis=worktree"
  else
    wt_fail=1
    bad "'$sub' with a deleted worktree did not refuse by name: rc=$g_rc verdict=$(verdict_of "$g_out") count=$(verdict_count "$g_out")
$g_out"
  fi
  # DECLARED RESIDUAL, MEASURED: bash prints `shell-init:` / `chdir:` for a deleted cwd BEFORE
  # the script's first line runs, so those two are the INTERPRETER's and cannot be suppressed
  # from inside the script. Every line the SCRIPT itself emits must still carry the anchor.
  ours=$(printf '%s\n' "$g_out" | grep -v '^shell-init: ' | grep -v '^chdir: ' || true)
  if all_lines_anchored "$ours"; then
    ok "'$sub': every line the SCRIPT emits is anchored (bash's own pre-exec shell-init/chdir lines are a DECLARED, unsuppressable residual)"
  else
    bad "'$sub' leaked an unprefixed line of its own:
$(printf '%s' "$ours" | cat -v)"
  fi
done
# NOTHING IS DERIVED AT THE ROOT: a marker at / must never be consulted or created. Asserted by
# the refusal above happening BEFORE any marker access — measured here as the absence of a
# root-level marker after a write attempt (this test never has permission to create one, so the
# assertion is that the attempt is not even made: rc/verdict above, plus this belt).
if [ ! -e "/$MARKER" ]; then
  ok "no marker was derived or created at the filesystem root"
else
  bad "/$MARKER exists — a root-level marker is in play and this case cannot distinguish it from one this run created"
fi
# NON-VACUITY: the same three subcommands work in a LIVE worktree.
L40=$(lane lane40)
run "$L40" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
n40_v=$(run "$L40" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822); n40_vrc=$?
n40_s=$(run "$L40" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822); n40_src=$?
# `adopt` in a LIVE lane it already owns is the re-entrant no-op (ADOPTED, nothing transferred) —
# which is exactly what proves the adopt row above refused for the AXIS and not because adopt
# refuses everywhere.
n40_a=$(run "$L40" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- adopt 3822 --reason cron-reinvoke:writer-pid-gone); n40_arc=$?
if [ "$wt_fail" -eq 0 ] && [ "$n40_vrc" -eq 0 ] && [ "$(verdict_of "$n40_v")" = OWNED ] \
   && [ "$n40_src" -eq 0 ] && [ "$(verdict_of "$n40_s")" = SHOWN ] \
   && [ "$n40_arc" -eq 0 ] && [ "$(verdict_of "$n40_a")" = ADOPTED ]; then
  ok "NON-VACUITY: verify, show and adopt still work normally in a live worktree — the refusals are about the unmeasurable axis"
else
  bad "the live-worktree control regressed: v_rc=$n40_vrc v=$(verdict_of "$n40_v") s_rc=$n40_src s=$(verdict_of "$n40_s") a_rc=$n40_arc a=$(verdict_of "$n40_a")"
fi
# THE ORDERING IS ALSO PINNED STRUCTURALLY, because the behavioural row above can only see the
# CURRENT arrangement: in `cmd_adopt` the worktree-axis guard must appear BEFORE `lock_marker`
# (which derives its path from that axis). A reordering that reintroduces K2 reds here even if a
# future refactor changes the refusal text.
adopt_body=$(LC_ALL=C sed -n '/^cmd_adopt() {/,/^}/p' "$DS")
a_axis_ln=$(printf '%s\n' "$adopt_body" | grep -n '^  require_worktree_axis$' | head -1 | cut -d: -f1)
a_lock_ln=$(printf '%s\n' "$adopt_body" | grep -n '^  lock_marker$' | head -1 | cut -d: -f1)
if [ -n "$a_axis_ln" ] && [ -n "$a_lock_ln" ] && [ "$a_axis_ln" -lt "$a_lock_ln" ]; then
  ok "STRUCTURAL: cmd_adopt calls require_worktree_axis (line $a_axis_ln of the function) BEFORE lock_marker (line $a_lock_ln)"
else
  bad "cmd_adopt's axis guard does not precede its lock: axis='$a_axis_ln' lock='$a_lock_ln' (an absent line means the pin lost its subject)"
fi

# ===========================================================================
case_begin 41-body-without-trailing-newline "CLASS SWEEP of job 34 H3: a body whose LAST LINE HAS NO NEWLINE survives byte-for-byte (roborev job 35 I2)"
# ===========================================================================
# THE DEFECT: `marker_body` extracted the body with `awk '{ print }'`, and awk ALWAYS terminates
# the record it prints. So a 19-byte body `no trailing newline` came back as the 20 bytes
# `no trailing newline\n` on the NEXT owned write — the header promises the body SURVIVES a write,
# and it did not. Case 39 could not see it: its body was newline-terminated AND its `after_sentinel`
# helper extracted through awk too, so the comparison ran over bytes the extractor had already
# normalised. A verification that shares the defect's blind spot is not a verification, so the
# helper was rewritten (grep -b offset + tail -c, no line-oriented tool anywhere) before this case
# was written.
#
# THE SWEEP: the class is "body bytes routed through a line-oriented tool", so this case measures
# the four normalisations such a tool performs — a missing final newline, CRLF line endings,
# trailing blank lines, and leading whitespace — over one body, end to end, through a write, three
# carry-forward writes and an adopt.
L41=$(lane lane41)
body41="$T/body41.md"
# One body carrying every normalisation hazard at once. Built with printf so the bytes are exact:
# a CRLF line, a line of leading whitespace, two trailing blank lines, and a FINAL LINE WITH NO
# NEWLINE. No sentinel, no column-zero `<!--`, so the writer's body guard is not the subject here.
printf '## plan\r\n   indented note\n\n\nno trailing newline' >"$body41"
b41_bytes=$(wc -c <"$body41" | tr -d ' ')
{ printf '\n'; cat "$body41"; } >"$T/41-canonical"
if [ "$b41_bytes" -eq 47 ] && [ "$(tail -c 1 "$body41" | od -An -c | tr -d ' \n')" != '\n' ]; then
  ok "FIXTURE: the body is $b41_bytes bytes and its last byte is NOT a newline (the case's whole subject)"
else
  bad "FIXTURE BROKEN: the body is $b41_bytes bytes and ends $(tail -c 1 "$body41" | od -An -c) — this case would prove nothing"
fi
run "$L41" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --body-file "$body41" >/dev/null 2>&1
after_sentinel "$L41/$MARKER" "$T/41-r1"
if cmp -s "$T/41-r1" "$T/41-canonical"; then
  ok "the FIRST write lays the body down verbatim — no newline added, no CRLF rewritten, no blank line trimmed"
else
  bad "the first write already mutated the body:
$(cmp -l "$T/41-canonical" "$T/41-r1" | head -5)
$(cat -A "$T/41-r1" | tail -5)"
fi
b41_fail=0
i=2
while [ "$i" -le 4 ]; do
  run "$L41" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage "round$i" >/dev/null 2>&1
  after_sentinel "$L41/$MARKER" "$T/41-r$i"
  cmp -s "$T/41-canonical" "$T/41-r$i" || { b41_fail=1
    printf 'note   carry-forward write #%s changed the body bytes (%s -> %s bytes):\n%s\n' \
      "$i" "$(wc -c <"$T/41-canonical" | tr -d ' ')" "$(wc -c <"$T/41-r$i" | tr -d ' ')" \
      "$(cmp -l "$T/41-canonical" "$T/41-r$i" | head -3)"; }
  i=$((i + 1))
done
if [ "$b41_fail" -eq 0 ]; then
  ok "THREE carry-forward writes leave the body IDENTICAL — the read-back path adds no terminator"
else
  bad "a carry-forward write mutated a body whose last line has no newline (the job 35 I2 defect)"
fi
# ACROSS AN ADOPT, which carries the body through the very same read-back path.
L41A=$(lane lane41-adopt)
sleep 30 & dead41=$!
run "$L41A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$dead41" -- write 3822 --body-file "$body41" >/dev/null 2>&1
kill "$dead41" 2>/dev/null; wait "$dead41" 2>/dev/null
run "$L41A" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- adopt 3822 --reason cron-reinvoke:writer-pid-gone >/dev/null 2>&1
after_sentinel "$L41A/$MARKER" "$T/41a-after"
if cmp -s "$T/41-canonical" "$T/41a-after"; then
  ok "an ADOPT carries the unterminated body across byte-for-byte too"
else
  bad "the adopt mutated the body bytes ($(wc -c <"$T/41-canonical" | tr -d ' ') -> $(wc -c <"$T/41a-after" | tr -d ' ') bytes):
$(cmp -l "$T/41-canonical" "$T/41a-after" | head -3)"
fi
# THE HELPER ITSELF IS PROVED BYTE-EXACT, or every cmp above is an assertion about the extractor.
# A file whose post-sentinel region is KNOWN is extracted and compared to those known bytes.
h41="$T/41-helper-fixture"
{ printf '%s\n' "$sentinel"; printf 'issue: 3822\n'; printf '%s\n' "$sentinel_end"
  printf '\n'; cat "$body41"; } >"$h41"
after_sentinel "$h41" "$T/41-helper-out"
if cmp -s "$T/41-canonical" "$T/41-helper-out"; then
  ok "HELPER CONTROL: after_sentinel extracts a KNOWN unterminated region byte-for-byte (it no longer hides the defect it verifies)"
else
  bad "after_sentinel is still normalising: extracted $(wc -c <"$T/41-helper-out" | tr -d ' ') of $(wc -c <"$T/41-canonical" | tr -d ' ') bytes"
fi
# AND THE OLD HELPER IS THE POSITIVE CONTROL: the mechanism this case replaced must be SHOWN to
# miss the defect, or "the verification shared the blind spot" is a claim rather than a measurement.
awk -v s="$sentinel_end" 'seen{print} $0==s{seen=1}' "$h41" >"$T/41-old-helper-out"
if ! cmp -s "$T/41-canonical" "$T/41-old-helper-out"; then
  ok "POSITIVE CONTROL: the RETIRED awk helper returns $(wc -c <"$T/41-old-helper-out" | tr -d ' ') bytes for the same $(wc -c <"$T/41-canonical" | tr -d ' ')-byte region — it would have passed over the defect"
else
  bad "the retired awk helper is byte-exact here, so this case's premise is wrong and it proves nothing"
fi

# ===========================================================================
case_begin 42-verdict-emission-is-a-critical-section "the verdict line is PRINTED before the 'already emitted' state is committed — a signal in that window loses no verdict (roborev job 35 I1)"
# ===========================================================================
# THE DEFECT: `verdict()` was `VERDICT_EMITTED=1; printf ...` — the flag SET BEFORE the line was
# printed. A signal arriving between those two commands left the worst possible state: `on_signal`
# saw the flag and stayed silent, the EXIT-trap backstop saw it and stayed silent, and the run
# exited having printed NO verdict at all — while a write may already have been committed. That is
# contract (c) ("EVERY exit carries exactly one token") violated by round 6's own G2 fix, from
# inside the function the fix installed to enforce it.
#
# HOW THIS IS TESTED WITHOUT A COIN FLIP. The window is between two SHELL commands, so no PATH
# shim can reach it and no sleep can time it. Both halves below are deterministic:
#   * STRUCTURAL — the print must PRECEDE the state commit, and `VERDICT_EMITTED=1` must exist at
#     exactly ONE site in the whole script (the class sweep: `refuse`'s internal argument-count
#     guard had its own copy of the same flag-then-print ordering).
#   * BEHAVIOURAL, by ARTIFACT SUBSTITUTION — the signal is PLANTED into a scratch copy of the
#     script AT the window (`kill -TERM $$` between the print and the commit), so it arrives
#     exactly where the race would put it, every run. The POSITIVE CONTROL reconstructs the
#     PRE-FIX ordering in a second copy with the SAME plant and requires it to emit ZERO verdicts
#     — without it, a green here could mean the plant never reached the window.
SCRATCH42="$T/scratch42"; mkdir -p "$SCRATCH42/lib"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$SCRATCH42/lib/process-liveness.sh"

# --- STRUCTURAL -------------------------------------------------------------------------
# COMMENTS ARE STRIPPED BEFORE THE ORDER IS READ: these functions document the very identifiers
# being located, so a prose mention would otherwise "occur" before the code and the assert would
# be about the comment (measured — it did, on the first run of this case).
uncomment() { printf '%s\n' "$1" | grep -v '^[[:space:]]*#'; }
v42_body=$(awk '/^verdict\(\) \{$/{f=1} f{print} f && /^\}$/{exit}' "$DS")
v42_code=$(uncomment "$v42_body")
v42_print=$(printf '%s\n' "$v42_code" | grep -n "printf '%s verdict" | head -1 | cut -d: -f1)
v42_flag=$(printf '%s\n' "$v42_code" | grep -n '^ *VERDICT_EMITTED=1$' | head -1 | cut -d: -f1)
if [ -n "$v42_print" ] && [ -n "$v42_flag" ] && [ "$v42_print" -lt "$v42_flag" ]; then
  ok "STRUCTURAL: inside verdict(), the verdict line is PRINTED (body line $v42_print) before VERDICT_EMITTED is committed (body line $v42_flag)"
else
  bad "STRUCTURAL: verdict() commits the 'already emitted' state before (or without) printing the line — print=${v42_print:-none} flag=${v42_flag:-none}
$v42_code"
fi
v42_sites=$(grep -c '^ *VERDICT_EMITTED=1$' "$DS" || true)
if [ "$v42_sites" = 1 ]; then
  ok "CLASS SWEEP: VERDICT_EMITTED=1 is assigned at exactly ONE site — nothing else can claim 'already emitted' out of order"
else
  bad "VERDICT_EMITTED=1 is assigned at $v42_sites sites; every one that is not verdict()'s is a second copy of the job 35 I1 ordering:
$(grep -n '^ *VERDICT_EMITTED=1$' "$DS")"
fi
# ONE EMITTER, pinned. The window can only exist where a `verdict ` line is printed, so a second
# printf of one is a second place for it to come back — which is exactly what `refuse`'s internal
# argument-count guard was.
v42_emit=$(grep -c "printf '%s verdict %s" "$DS" || true)
if [ "$v42_emit" = 1 ]; then
  ok "CLASS SWEEP: exactly ONE site in the script prints a 'verdict ' line, so the ordering above is the only ordering there is"
else
  bad "$v42_emit sites print a 'verdict ' line; every one outside verdict() is a second copy of the window:
$(grep -n "printf '%s verdict %s" "$DS")"
fi
s42_body=$(awk '/^on_signal\(\) \{$/{f=1} f{print} f && /^\}$/{exit}' "$DS")
s42_code=$(uncomment "$s42_body")
s42_ing=$(printf '%s\n' "$s42_code" | grep -n 'VERDICT_EMITTING' | head -1 | cut -d: -f1)
s42_ed=$(printf '%s\n' "$s42_code" | grep -n 'VERDICT_EMITTED' | head -1 | cut -d: -f1)
if [ -n "$s42_ing" ] && [ -n "$s42_ed" ] && [ "$s42_ing" -lt "$s42_ed" ]; then
  ok "STRUCTURAL: on_signal consults the in-flight flag (body line $s42_ing) BEFORE it may conclude 'already emitted' (body line $s42_ed)"
else
  bad "on_signal can conclude 'already emitted' without first checking whether an emission is IN FLIGHT — emitting=${s42_ing:-none} emitted=${s42_ed:-none}
$s42_code"
fi

# --- BEHAVIOURAL, by artifact substitution ----------------------------------------------
# plant_verdict_window <fixed|prefix> <outfile> — copy the shipped script with `kill -TERM $$`
# planted inside verdict(): `fixed` puts it in the SHIPPED window (after the print, before the
# state commit); `prefix` first reconstructs the PRE-FIX body (commit, then print) and puts the
# plant between them. Nothing but verdict() is touched, and no seam is added to the shipped file.
plant_verdict_window() {
  awk -v mode="$1" '
    /^verdict\(\) \{$/ { inv = 1; print; next }
    inv && /^\}$/       { inv = 0; print; next }
    inv {
      if (mode == "fixed") {
        print
        if ($0 ~ /printf .%s verdict/) print "  kill -TERM $$"
        next
      }
      if ($0 ~ /^ *VERDICT_EMITTING=1$/) { print "  VERDICT_EMITTED=1"; print "  kill -TERM $$"; next }
      if ($0 ~ /^ *VERDICT_EMITTED=1$/)  next
      if ($0 ~ /^ *VERDICT_EMITTING=0$/) next
      if ($0 ~ /settle_verdict_signal$/) next
      print; next
    }
    { print }
  ' "$DS" >"$2"
}
plant_run() {  # plant_run <script> <lane> — combined output on stdout, run's status returned
  local sc="$1" d="$2" out rc
  out=$( cd "$d" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$sc" write 3822 --stage implement 2>&1 ); rc=$?
  printf '%s\n' "$out"
  return "$rc"
}
plant_verdict_window fixed  "$SCRATCH42/fixed.sh"
plant_verdict_window prefix "$SCRATCH42/prefix.sh"
if grep -q 'kill -TERM \$\$' "$SCRATCH42/fixed.sh" && grep -q 'kill -TERM \$\$' "$SCRATCH42/prefix.sh" \
   && bash -n "$SCRATCH42/fixed.sh" 2>/dev/null && bash -n "$SCRATCH42/prefix.sh" 2>/dev/null; then
  ok "PLANT FIXTURE: both scratch copies parse and both carry the planted signal inside verdict()"
else
  bad "the plant did not take — this case cannot reach its subject:
$(awk '/^verdict\(\) \{$/{f=1} f{print} f && /^\}$/{exit}' "$SCRATCH42/fixed.sh")"
fi
L42=$(lane lane42-fixed)
p42a=$(plant_run "$SCRATCH42/fixed.sh" "$L42"); r42a=$?
if [ "$(verdict_count "$p42a")" = 1 ] && [ "$(verdict_of "$p42a")" = WRITTEN ] \
   && [ "$r42a" -eq 143 ] && all_lines_anchored "$p42a" && [ -f "$L42/$MARKER" ]; then
  ok "BEHAVIOURAL: a SIGTERM planted IN the emission window still yields exactly ONE anchored 'verdict WRITTEN', exit 143, and the marker on disk"
else
  bad "a signal inside the emission window broke contract (c): rc=$r42a verdicts=$(verdict_count "$p42a") token=$(verdict_of "$p42a") marker=$([ -f "$L42/$MARKER" ] && echo present || echo absent)
$p42a"
fi
L42P=$(lane lane42-prefix)
p42b=$(plant_run "$SCRATCH42/prefix.sh" "$L42P"); r42b=$?
if [ "$(verdict_count "$p42b")" = 0 ]; then
  ok "POSITIVE CONTROL: the PRE-FIX ordering with the SAME plant emits ZERO verdicts (rc=$r42b) — the window is real and the plant reaches it"
else
  bad "the pre-fix reconstruction still emitted $(verdict_count "$p42b") verdict(s), so the green above may be about a plant that never landed:
$p42b"
fi
# NON-VACUITY: the same scratch copy WITHOUT a plant writes normally, so both results above are
# about the planted signal and not about a copy the rewrite broke.
cp "$DS" "$SCRATCH42/clean.sh"
L42N=$(lane lane42-clean)
p42n=$(plant_run "$SCRATCH42/clean.sh" "$L42N"); r42n=$?
if [ "$r42n" -eq 0 ] && [ "$(verdict_count "$p42n")" = 1 ] && [ "$(verdict_of "$p42n")" = WRITTEN ]; then
  ok "NON-VACUITY: the unplanted scratch copy writes normally (rc 0, one WRITTEN verdict)"
else
  bad "the scratch fixture is broken independently of the plant: rc=$r42n token=$(verdict_of "$p42n")
$p42n"
fi

# ===========================================================================
case_begin 43-identity-recorded-losslessly "an identity axis is recorded and compared LOSSLESSLY, or the run refuses (roborev job 37 J1)"
# ===========================================================================
# THE THIRD INSTANCE OF ONE FAMILY, pinned as a family rather than as a point fix: H1 committed
# the `unspecified` PLACEHOLDER for an UNMEASURABLE axis, the round-7 class sweep found the same
# shape on the worktree axis, and this is a MEASURABLE identity committed LOSSILY. Same
# consequence every time — two distinct lanes alias onto ONE owner.
L43=$(lane lane43)
# (i) THE DEMONSTRATED COLLISION: CLAIM_MACHINE='build box' records as 'build-box', so a
#     genuinely different box named 'build-box' used to verify as OWNED. The lossy write is now
#     refused outright, so the alias cannot be created in the first place.
lm_w=$(run "$L43" "CLAIM_MACHINE=build box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); lm_wrc=$?
if [ "$lm_wrc" -eq 1 ] && [ "$(verdict_of "$lm_w")" = ERROR ] \
   && [ "$(verdict_count "$lm_w")" = 1 ] \
   && printf '%s\n' "$lm_w" | grep -q 'axis=machine' \
   && all_lines_anchored "$lm_w" && [ ! -f "$L43/$MARKER" ]; then
  ok "a LOSSY machine identity ('build box') is ERROR(1) naming axis=machine, exactly one verdict, and NOTHING is written"
else
  bad "lossy machine identity was not refused: rc=$lm_wrc marker=$([ -f "$L43/$MARKER" ] && echo present || echo absent)
$lm_w"
fi
# The other direction of the SAME collision: a canonical 'build-box' marker must not be OWNED
# by a session whose CLAIM_MACHINE is the distinct value 'build box'.
run "$L43" "CLAIM_MACHINE=build-box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
lm_v=$(run "$L43" "CLAIM_MACHINE=build box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822 2>&1); lm_vrc=$?
lm_ok=$(run "$L43" "CLAIM_MACHINE=build-box" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822 2>&1); lm_okrc=$?
if [ "$lm_vrc" -ne 0 ] && [ "$(verdict_of "$lm_v")" != OWNED ] \
   && [ "$lm_okrc" -eq 0 ] && [ "$(verdict_of "$lm_ok")" = OWNED ]; then
  ok "THE COLLISION IS CLOSED: 'build box' does NOT verify as OWNED against a 'build-box' marker (got $(verdict_of "$lm_v")), while the genuine owner still does"
else
  bad "the machine collision is still reachable: lossy-rc=$lm_vrc/$(verdict_of "$lm_v") owner-rc=$lm_okrc/$(verdict_of "$lm_ok")"
fi
# (ii) THE LENGTH AXIS OF THE SAME DEFECT: two machines sharing a 120-character prefix.
L43b=$(lane lane43b)
l43_120=$(printf 'm%.0s' $(seq 1 120))
la_out=$(run "$L43b" "CLAIM_MACHINE=${l43_120}A" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); la_rc=$?
lb_out=$(run "$L43b" "CLAIM_MACHINE=${l43_120}B" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); lb_rc=$?
if [ "$la_rc" -eq 1 ] && [ "$(verdict_of "$la_out")" = ERROR ] \
   && [ "$lb_rc" -eq 1 ] && [ "$(verdict_of "$lb_out")" = ERROR ] \
   && [ ! -f "$L43b/$MARKER" ]; then
  ok "two machine names differing ONLY past the 120-character cut are BOTH refused, so they can never record the same owner"
else
  bad "the truncation collision is still reachable: a-rc=$la_rc/$(verdict_of "$la_out") b-rc=$lb_rc/$(verdict_of "$lb_out")"
fi
# A 120-character name is NOT lossy and must still work — a guard that reds on correct input is
# the guard agents learn to waive.
L43c=$(lane lane43c)
l120_out=$(run "$L43c" "CLAIM_MACHINE=$l43_120" "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); l120_rc=$?
if [ "$l120_rc" -eq 0 ] && [ "$(verdict_of "$l120_out")" = WRITTEN ]; then
  ok "NON-VACUITY: a canonical 120-character machine name is ACCEPTED (the bound is refused only where it CHANGES the value)"
else
  bad "a canonical 120-character machine name was refused: rc=$l120_rc
$l120_out"
fi
# (iii) THE SESSION AXIS, the same defect one axis over: an EQUAL session id is OWNED outright.
L43d=$(lane lane43d)
ls_w=$(run "$L43d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=sess a" "CLAUDE_PID=$$" -- write 3822 2>&1); ls_wrc=$?
if [ "$ls_wrc" -eq 1 ] && [ "$(verdict_of "$ls_w")" = ERROR ] \
   && [ "$(verdict_count "$ls_w")" = 1 ] \
   && printf '%s\n' "$ls_w" | grep -q 'axis=session' \
   && all_lines_anchored "$ls_w" && [ ! -f "$L43d/$MARKER" ]; then
  ok "a LOSSY session id ('sess a') is ERROR(1) naming axis=session, exactly one verdict, and NOTHING is written"
else
  bad "lossy session id was not refused: rc=$ls_wrc
$ls_w"
fi
run "$L43d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=sess-a" "CLAUDE_PID=$$" -- write 3822 >/dev/null 2>&1
ls_v=$(run "$L43d" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=sess a" "CLAUDE_PID=$$" -- verify 3822 2>&1); ls_vrc=$?
if [ "$ls_vrc" -ne 0 ] && [ "$(verdict_of "$ls_v")" != OWNED ]; then
  ok "THE SESSION COLLISION IS CLOSED: 'sess a' does NOT verify as OWNED against a 'sess-a' marker (got $(verdict_of "$ls_v"))"
else
  bad "the session collision is still reachable: rc=$ls_vrc verdict=$(verdict_of "$ls_v")"
fi
# An UNSET session id is the ABSENCE of a value, not a lossy one: it must still write.
L43e=$(lane lane43e)
lu_out=$(run "$L43e" CLAIM_MACHINE=boxA "CLAUDE_PID=$$" -- write 3822 2>&1); lu_rc=$?
if [ "$lu_rc" -eq 0 ] && [ "$(verdict_of "$lu_out")" = WRITTEN ] \
   && grep -q '^session: unrecorded$' "$L43e/$MARKER"; then
  ok "NON-VACUITY: an UNSET session id still records the 'unrecorded' sentinel and writes (absence is not lossiness)"
else
  bad "an unset session id was misread as lossy: rc=$lu_rc
$lu_out"
fi
# (iv) THE WORKTREE AXIS IS ALREADY VERBATIM — confirmed rather than assumed, because the
#      census that named it has to be measurable. A space-bearing lane path must be recorded
#      WITH its space and must NOT be owned from the '-'-substituted sibling.
L43f=$(lane "lane43 f")
L43g=$(lane "lane43-f")
wv_w=$(run "$L43f" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); wv_wrc=$?
wv_rec=$(sed -n 's/^worktree: //p' "$L43f/$MARKER" | head -1)
cp "$L43f/$MARKER" "$L43g/$MARKER"
wv_v=$(run "$L43g" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822 2>&1); wv_vrc=$?
if [ "$wv_wrc" -eq 0 ] && [ "$wv_rec" = "$L43f" ] && [ "$wv_rec" != "$L43g" ] \
   && [ "$wv_vrc" -eq 4 ] && [ "$(verdict_of "$wv_v")" = FOREIGN-WORKTREE ]; then
  ok "the worktree axis is recorded VERBATIM (space preserved) and the '-'-substituted sibling is FOREIGN-WORKTREE, not an alias"
else
  bad "worktree axis lossiness: w-rc=$wv_wrc recorded='$wv_rec' expected='$L43f' sibling-rc=$wv_vrc/$(verdict_of "$wv_v")"
fi
# (v) THE ACTOR AXIS IS DECLARED LOSSY AND DELIBERATELY NOT REFUSED. Nothing compares it, so a
#     collision there cannot grant ownership; refusing input claim.sh accepts would red on
#     correct input. Asserted so the DECISION is measurable and cannot be undone silently.
L43h=$(lane lane43h)
ac_out=$(run "$L43h" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --actor 'flow lead' 2>&1); ac_rc=$?
if [ "$ac_rc" -eq 0 ] && [ "$(verdict_of "$ac_out")" = WRITTEN ] \
   && grep -q '^actor: flow-lead$' "$L43h/$MARKER"; then
  ok "DECLARED: the actor is sanitized LOSSILY and accepted — it is recorded state, never an ownership axis (#3810 owns actor collisions)"
else
  bad "the actor axis decision changed without the contract changing: rc=$ac_rc
$ac_out
$(cat "$L43h/$MARKER" 2>/dev/null)"
fi

# ===========================================================================
case_begin 44-body-file-is-read-not-stat-gated "a --body-file is READ, never stat-gated: a source that changes after validation can never commit an EMPTY body (roborev job 37 J2)"
# ===========================================================================
# THE RACE IS MADE DETERMINISTIC BY A SHIM, not by timing: `flock` runs AFTER the caller's body
# file has been validated and BEFORE the marker is assembled in BOTH the pre-fix and the fixed
# script, so a shim that truncates (or deletes) the body there lands exactly in the window the
# finding names. A timing-based version of this test would prove nothing on a fast box.
j2_real_flock="$(command -v flock || true)"
j2_real_cat="$(command -v cat || true)"
j2_shim() {  # j2_shim <op> <victim> — a PATH dir whose `flock` mutates <victim> then defers
  # SPLIT DECLARATIONS ON PURPOSE: `local a="$1" d="$a"` expands EVERY word before the builtin
  # assigns any of them, so the second reference is unbound under `set -u` — which made this
  # helper return empty, the shim never fire, and the FIXED leg pass vacuously. The positive
  # control is what caught it.
  local op="$1"
  local victim="$2"
  local d="$T/j2-shim-$op-$RANDOM"
  mkdir -p "$d"
  {
    printf '#!/bin/sh\n'
    case "$op" in
      truncate) printf ': > "%s"\n' "$victim" ;;
      delete)   printf 'rm -f "%s"\n' "$victim" ;;
    esac
    printf 'exec %s "$@"\n' "$j2_real_flock"
  } >"$d/flock"
  chmod +x "$d/flock"
  printf '%s\n' "$d"
}
# THE PRE-FIX ARTIFACT, substituted in a scratch copy (never a settable seam in the shipped
# script) and VERIFIED to have taken: without it a green here would prove only that the plant
# does nothing.
mkdir -p "$T/j2-scratch/lib"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$T/j2-scratch/lib/"
j2_pre="$T/j2-scratch/drive-issue-state.sh"
sed -e 's|^    if \[ -n "\$bodyfile" \]; then cat "\$bodyfile" 2>/dev/null; fi$|    if [ -n "$bodyfile" ] \&\& [ -s "$bodyfile" ]; then cat "$bodyfile" 2>/dev/null; fi|' \
    -e 's|^    body_src="\$body_snap"$|    body_src="$bodyfile"|' "$DS" >"$j2_pre"
j2_pin=0
grep -q '^    if \[ -n "\$bodyfile" \] && \[ -s "\$bodyfile" \]; then cat' "$j2_pre" || j2_pin=$((j2_pin + 1))
grep -q '^    body_src="\$bodyfile"$' "$j2_pre" || j2_pin=$((j2_pin + 1))
bash -n "$j2_pre" 2>/dev/null || j2_pin=$((j2_pin + 1))
if [ "$j2_pin" -eq 0 ]; then
  ok "PRE-FIX FIXTURE: the scratch copy restores BOTH halves of the stat-then-act shape (the -s gate and the un-snapshotted body_src) and still parses"
else
  bad "the pre-fix substitution did not take ($j2_pin check(s) failed) — the positive control below would prove nothing"
fi
j2_fail=0
for j2_op in truncate delete; do
  # --- the FIXED script: the validated bytes reach the marker whatever happens to the source.
  L44=$(lane "lane44-$j2_op")
  body44="$T/body44-$j2_op.md"
  printf '## plan\n\n- step one\n- step two\n' >"$body44"
  cp "$body44" "$T/body44-$j2_op.orig"
  sd44=$(j2_shim "$j2_op" "$body44")
  o44=$( cd "$L44" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
           "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd44:$PATH" \
           bash "$DS" write 3822 --body-file "$body44" 2>&1 ); rc44=$?
  after_sentinel "$L44/$MARKER" "$T/44-$j2_op-body" || true
  { printf '\n'; cat "$T/body44-$j2_op.orig"; } >"$T/44-$j2_op-canonical"
  if [ "$rc44" -eq 0 ] && [ "$(verdict_of "$o44")" = WRITTEN ] \
     && cmp -s "$T/44-$j2_op-body" "$T/44-$j2_op-canonical"; then
    ok "a body file ${j2_op}d between validation and the copy still commits the VALIDATED bytes byte-for-byte (verdict WRITTEN)"
  else
    j2_fail=$((j2_fail + 1))
    bad "body ${j2_op} race: rc=$rc44 verdict=$(verdict_of "$o44"); body bytes=$(wc -c <"$T/44-$j2_op-body" 2>/dev/null) want=$(wc -c <"$T/44-$j2_op-canonical")
$o44"
  fi
  # --- the PRE-FIX script under the SAME plant: the body is committed EMPTY, silently.
  L44p=$(lane "lane44p-$j2_op")
  body44p="$T/body44p-$j2_op.md"
  printf '## plan\n\n- step one\n- step two\n' >"$body44p"
  sd44p=$(j2_shim "$j2_op" "$body44p")
  o44p=$( cd "$L44p" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
            "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd44p:$PATH" \
            bash "$j2_pre" write 3822 --body-file "$body44p" 2>&1 ); rc44p=$?
  after_sentinel "$L44p/$MARKER" "$T/44p-$j2_op-body" || true
  p_bytes=$(LC_ALL=C wc -c <"$T/44p-$j2_op-body" 2>/dev/null | tr -d ' ')
  if [ "$rc44p" -eq 0 ] && [ "$(verdict_of "$o44p")" = WRITTEN ] && [ "${p_bytes:-0}" -le 1 ]; then
    ok "POSITIVE CONTROL: the PRE-FIX script under the same ${j2_op} plant reports WRITTEN with an EMPTY body ($p_bytes byte(s)) — the silent data loss is real and the plant reaches it"
  else
    j2_fail=$((j2_fail + 1))
    bad "the positive control did not reproduce the defect (${j2_op}): rc=$rc44p verdict=$(verdict_of "$o44p") body-bytes=$p_bytes"
  fi
done
# --- THE READ-FAILURE PATH ITSELF: a source that cannot be read is a REFUSAL, never an empty
#     body. Shimmed on `cat` (the snapshot's reader) so the failure is the READ and not a stat.
L44r=$(lane lane44r)
body44r="$T/body44r.md"
printf 'plan text\n' >"$body44r"
sd44r="$T/j2-shim-cat-$RANDOM"; mkdir -p "$sd44r"
{
  printf '#!/bin/sh\n'
  printf 'case "$*" in *body44r.md*) exit 1;; esac\n'
  printf 'exec %s "$@"\n' "$j2_real_cat"
} >"$sd44r/cat"
chmod +x "$sd44r/cat"
o44r=$( cd "$L44r" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
          "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" "PATH=$sd44r:$PATH" \
          bash "$DS" write 3822 --body-file "$body44r" 2>&1 ); rc44r=$?
if [ "$rc44r" -eq 1 ] && [ "$(verdict_of "$o44r")" = ERROR ] \
   && [ "$(verdict_count "$o44r")" = 1 ] && all_lines_anchored "$o44r" \
   && [ ! -f "$L44r/$MARKER" ]; then
  ok "a --body-file whose READ fails is ERROR(1) with exactly one anchored verdict and NOTHING written — 'could not be read' is never committed as 'there was no body'"
else
  j2_fail=$((j2_fail + 1))
  bad "a failed body read was not refused: rc=$rc44r verdict=$(verdict_of "$o44r") marker=$([ -f "$L44r/$MARKER" ] && echo present || echo absent)
$o44r"
fi
# --- NON-VACUITY: a genuinely EMPTY --body-file is still legal and writes an empty body.
L44e=$(lane lane44e)
: >"$T/body44e.md"
o44e=$(run "$L44e" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --body-file "$T/body44e.md" 2>&1); rc44e=$?
if [ "$rc44e" -eq 0 ] && [ "$(verdict_of "$o44e")" = WRITTEN ]; then
  ok "NON-VACUITY: an EMPTY --body-file is still accepted (an empty source IS an empty body; only a failed READ refuses)"
else
  bad "an empty --body-file was refused: rc=$rc44e
$o44e"
fi

# ===========================================================================
case_begin 45-unknown-prologue-keys-survive "an UNRECOGNISED prologue key survives write AND adopt (roborev job 37 J3)"
# ===========================================================================
# The parser accepts an unknown key FOR FORWARD COMPATIBILITY. Until now the rewrite path
# dropped it, so an OLDER copy of this script silently DELETED a field a NEWER one introduced —
# the same durable-state erasure as a dropped `request-id`. PRESERVE was chosen over REFUSE
# because refusing would make a newer marker unusable by an older script, bricking every
# touched lane on a fleet mid-rollout; the header states the choice.
L45=$(lane lane45)
sleep 300 & p45=$!
run "$L45" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p45" -- \
    write 3822 --stage implement --request-id req-7 --pr 3837 >/dev/null 2>&1
# Inject two unknown keys the way a NEWER version of this script would have written them.
LC_ALL=C awk '/^actor: /{print; print "future-field: keep-me"; print "x-9: v2"; next} {print}' \
  "$L45/$MARKER" >"$T/45-injected" && cp "$T/45-injected" "$L45/$MARKER"
u45_w=$(run "$L45" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p45" -- \
    write 3822 --stage review 2>&1); u45_wrc=$?
u45_keep=$(LC_ALL=C grep -c '^future-field: keep-me$' "$L45/$MARKER" || true)
u45_x=$(LC_ALL=C grep -c '^x-9: v2$' "$L45/$MARKER" || true)
if [ "$u45_wrc" -eq 0 ] && [ "$(verdict_of "$u45_w")" = WRITTEN ] \
   && [ "$u45_keep" = 1 ] && [ "$u45_x" = 1 ] \
   && grep -q '^stage: review$' "$L45/$MARKER" \
   && grep -q '^request-id: req-7$' "$L45/$MARKER" && grep -q '^pr: 3837$' "$L45/$MARKER"; then
  ok "a write PRESERVES both unrecognised keys exactly once each, and the known durable fields still behave"
else
  bad "unknown keys did not survive a write: rc=$u45_wrc keep=$u45_keep x=$u45_x
$(cat "$L45/$MARKER")"
fi
# A SECOND write must not ACCUMULATE them (the separator/body defect one field over).
run "$L45" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p45" -- write 3822 --stage gate >/dev/null 2>&1
u45_keep2=$(LC_ALL=C grep -c '^future-field: keep-me$' "$L45/$MARKER" || true)
u45_show=$(run "$L45" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$p45" -- show 3822 2>&1)
if [ "$u45_keep2" = 1 ] \
   && printf '%s\n' "$u45_show" | grep -q '^DRIVE-STATE: field unrecognised future-field=keep-me$' \
   && printf '%s\n' "$u45_show" | grep -q '^DRIVE-STATE: field unrecognised x-9=v2$' \
   && all_lines_anchored "$u45_show"; then
  ok "repeated writes neither duplicate nor drop them, and 'show' reports each on an anchored 'field unrecognised' line"
else
  bad "accumulation or show gap: occurrences=$u45_keep2
$u45_show"
fi
# ADOPT carries them across a hand-over too.
kill "$p45" 2>/dev/null; wait "$p45" 2>/dev/null
u45_a=$(run "$L45" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_B" "CLAUDE_PID=$$" -- \
    adopt 3822 --reason j3-unknown-keys:cron-resume 2>&1); u45_arc=$?
u45_keep3=$(LC_ALL=C grep -c '^future-field: keep-me$' "$L45/$MARKER" || true)
u45_x3=$(LC_ALL=C grep -c '^x-9: v2$' "$L45/$MARKER" || true)
if [ "$u45_arc" -eq 0 ] && [ "$(verdict_of "$u45_a")" = ADOPTED ] \
   && [ "$u45_keep3" = 1 ] && [ "$u45_x3" = 1 ] \
   && grep -q "^prior-session: $SESS_A\$" "$L45/$MARKER" \
   && grep -q '^stage: gate$' "$L45/$MARKER"; then
  ok "an adopt carries both unrecognised keys across the hand-over, alongside the provenance and the durable fields"
else
  bad "unknown keys did not survive an adopt: rc=$u45_arc keep=$u45_keep3 x=$u45_x3
$u45_a
$(cat "$L45/$MARKER")"
fi
# POSITIVE CONTROL, by artifact substitution: with the carry-forward reverted in a scratch copy,
# the SAME sequence DELETES both keys — so the assertions above measure the fix, not the fixture.
mkdir -p "$T/j3-scratch/lib"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$T/j3-scratch/lib/"
j3_pre="$T/j3-scratch/drive-issue-state.sh"
sed -e 's|^      unknown="\$S_unknown"$|      unknown=""|' \
    -e 's|^  local unknown="\$S_unknown"$|  local unknown=""|' "$DS" >"$j3_pre"
j3_pin=0
grep -q '^      unknown=""$' "$j3_pre" || j3_pin=$((j3_pin + 1))
grep -q '^  local unknown=""$' "$j3_pre" || j3_pin=$((j3_pin + 1))
bash -n "$j3_pre" 2>/dev/null || j3_pin=$((j3_pin + 1))
L45p=$(lane lane45p)
( cd "$L45p" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$j3_pre" write 3822 --stage implement ) >/dev/null 2>&1
LC_ALL=C awk '/^actor: /{print; print "future-field: keep-me"; next} {print}' \
  "$L45p/$MARKER" >"$T/45p-injected" && cp "$T/45p-injected" "$L45p/$MARKER"
( cd "$L45p" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$j3_pre" write 3822 --stage review ) >/dev/null 2>&1
j3_left=$(LC_ALL=C grep -c '^future-field: keep-me$' "$L45p/$MARKER" || true)
if [ "$j3_pin" -eq 0 ] && [ "$j3_left" = 0 ]; then
  ok "POSITIVE CONTROL: with the carry-forward reverted, the same write DELETES the unrecognised key — the silent field loss is real"
else
  bad "the positive control did not reproduce the loss: pin-failures=$j3_pin remaining=$j3_left"
fi

# ===========================================================================
case_begin 46-symlink-marker-is-never-clobbered "roborev job 43 K1: a SYMLINK at the marker path — DANGLING or not — is refused as not-regular and is NEVER replaced"
# ===========================================================================
# THE DEFECT: `marker_class` detected absence with `[ -e "$path" ]`, and `-e` FOLLOWS the link,
# so it is FALSE for a DANGLING symlink — an entry that plainly EXISTS. That classified as
# `absent`, the ONE class whose handler in `write` REPLACES the path without a word, so `mv`
# destroyed the link. A symlink is a deliberate artifact someone placed; in a file whose whole
# job is refusing to clobber what it does not own, silently replacing one is the wrong default.
#
# VERIFICATION DISCIPLINE: every assertion below uses `-L` and `readlink`, never `-f` or `cat` —
# both of those FOLLOW the link and would normalize away the very property under test.
L46=$(lane lane46)
K46_DANGLING="$T/no-such-target-46"
ln -s "$K46_DANGLING" "$L46/$MARKER"
[ ! -e "$K46_DANGLING" ] || bad "fixture: the dangling target $K46_DANGLING unexpectedly exists"
d46_w=$(run "$L46" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 --stage implement 2>&1); d46_wrc=$?
d46_link_after=$(readlink "$L46/$MARKER" 2>/dev/null || true)
if [ "$d46_wrc" -ne 0 ] && [ "$(verdict_of "$d46_w")" = ERROR ] \
   && [ "$(verdict_count "$d46_w")" = 1 ] \
   && printf '%s\n' "$d46_w" | grep -q 'not a readable regular file'; then
  ok "write over a DANGLING symlink: exactly ONE verdict, ERROR, naming the not-regular refusal (it used to classify absent and replace it)"
else
  bad "write over a dangling symlink did not refuse: rc=$d46_wrc verdict=$(verdict_of "$d46_w") count=$(verdict_count "$d46_w")
$d46_w"
fi
if [ -L "$L46/$MARKER" ] && [ "$d46_link_after" = "$K46_DANGLING" ] && [ ! -e "$K46_DANGLING" ]; then
  ok "the SYMLINK ITSELF survives, still pointing at the same target, and nothing was created at that target (asserted with -L/readlink, never -f/cat)"
else
  bad "the symlink was clobbered: is-link=$([ -L "$L46/$MARKER" ] && echo yes || echo no) readlink='$d46_link_after' expected='$K46_DANGLING' target-exists=$([ -e "$K46_DANGLING" ] && echo yes || echo no)"
fi
# A DANGLING SYMLINK IS NOT A FRESH START. The readers used to answer ABSENT (exit 3) — the
# verdict that means "resume nothing, this lane is clean" — for an entry that exists.
d46_v=$(run "$L46" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- verify 3822 2>&1); d46_vrc=$?
d46_s=$(run "$L46" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- show 3822 2>&1); d46_src=$?
if [ "$d46_vrc" -ne 0 ] && [ "$(verdict_of "$d46_v")" = ERROR ] \
   && [ "$d46_src" -ne 0 ] && [ "$(verdict_of "$d46_s")" = ERROR ]; then
  ok "verify and show over a dangling symlink report ERROR, never ABSENT — an entry that exists is not a clean lane"
else
  bad "a dangling symlink still reads as absent: v_rc=$d46_vrc v=$(verdict_of "$d46_v") s_rc=$d46_src s=$(verdict_of "$d46_s")
$d46_v
$d46_s"
fi
# THE NON-DANGLING SHAPE, which `-e` DID see but `-f` FOLLOWED: it must take the SAME refusal,
# and the TARGET's bytes must be untouched. That is why the `-L` test precedes `-f`.
L46b=$(lane lane46b)
K46_TARGET="$T/real-target-46.md"
printf 'peer plan, do not touch\n' >"$K46_TARGET"
ln -s "$K46_TARGET" "$L46b/$MARKER"
b46_w=$(run "$L46b" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); b46_wrc=$?
b46_link_after=$(readlink "$L46b/$MARKER" 2>/dev/null || true)
b46_bytes=$(cat "$K46_TARGET" 2>/dev/null || true)
if [ "$b46_wrc" -ne 0 ] && [ "$(verdict_of "$b46_w")" = ERROR ] \
   && printf '%s\n' "$b46_w" | grep -q 'not a readable regular file' \
   && [ -L "$L46b/$MARKER" ] && [ "$b46_link_after" = "$K46_TARGET" ] \
   && [ "$b46_bytes" = 'peer plan, do not touch' ]; then
  ok "write over a symlink to a REAL file takes the same not-regular refusal; the link and the target's bytes are both untouched"
else
  bad "a non-dangling symlink was not refused, or was followed: rc=$b46_wrc verdict=$(verdict_of "$b46_w") readlink='$b46_link_after' target-bytes='$b46_bytes'
$b46_w"
fi
if all_lines_anchored "$d46_w" && all_lines_anchored "$d46_v" && all_lines_anchored "$d46_s" && all_lines_anchored "$b46_w"; then
  ok "every line of all four symlink refusals carries the anchored DRIVE-STATE: prefix"
else
  bad "a symlink refusal leaked an unprefixed line"
fi
# CLASS SWEEP, SAME ROUND: the lock sidecar is a SCRIPT-OWNED path too, and `: >>"$lock"`
# FOLLOWS a link — so a link planted there (by a peer lane, not only by the invoker) would have
# this script create a file OUTSIDE the lane. Refused by name, link intact, target not created.
L46c=$(lane lane46c)
K46C_TARGET="$T/no-such-lock-target-46"
ln -s "$K46C_TARGET" "$L46c/$MARKER.lock"
c46_w=$(run "$L46c" CLAIM_MACHINE=boxA "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" -- write 3822 2>&1); c46_wrc=$?
c46_link_after=$(readlink "$L46c/$MARKER.lock" 2>/dev/null || true)
if [ "$c46_wrc" -ne 0 ] && [ "$(verdict_of "$c46_w")" = ERROR ] \
   && printf '%s\n' "$c46_w" | grep -q 'lock path .* is a SYMLINK' \
   && [ -L "$L46c/$MARKER.lock" ] && [ "$c46_link_after" = "$K46C_TARGET" ] \
   && [ ! -e "$K46C_TARGET" ] && [ ! -e "$L46c/$MARKER" ]; then
  ok "a SYMLINK at the lock path is refused by name; the link survives, its target was NOT created, and no marker was written"
else
  bad "the lock-path symlink sweep regressed: rc=$c46_wrc verdict=$(verdict_of "$c46_w") readlink='$c46_link_after' target-created=$([ -e "$K46C_TARGET" ] && echo yes || echo no) marker=$([ -e "$L46c/$MARKER" ] && echo yes || echo no)
$c46_w"
fi
# POSITIVE CONTROL, by ARTIFACT SUBSTITUTION: with the `-L` existence handling reverted in a
# scratch copy, the same write DESTROYS the dangling symlink — so the assertions above measure
# the fix and not the fixture. (A test-only seam is never used for this; the artifact is
# substituted, per this file's own idiom.)
mkdir -p "$T/k1-scratch/lib"
cp "$SCRIPT_DIR/../flow/lib/process-liveness.sh" "$T/k1-scratch/lib/"
k1_pre="$T/k1-scratch/drive-issue-state.sh"
LC_ALL=C awk -v q="'" '
  index($0, "&& [ ! -L \"$path\" ]; then printf") { print "  [ -e \"$path\" ] || { printf " q "absent\\n" q "; return 0; }"; next }
  index($0, "[ ! -L \"$path\" ] || { printf " q "not-regular") { next }
  { print }
' "$DS" >"$k1_pre"
k1_pin=0
grep -q '^  \[ -e "\$path" \] || { printf .absent' "$k1_pre" || k1_pin=$((k1_pin + 1))
[ "$(grep -c '! -L "\$path"' "$k1_pre" || true)" = 0 ] || k1_pin=$((k1_pin + 1))
bash -n "$k1_pre" 2>/dev/null || k1_pin=$((k1_pin + 1))
L46p=$(lane lane46p)
K46P_TARGET="$T/no-such-target-46p"
ln -s "$K46P_TARGET" "$L46p/$MARKER"
( cd "$L46p" && env -u CLAUDE_PID -u CLAUDE_CODE_SESSION_ID CLAIM_MACHINE=boxA \
    "CLAUDE_CODE_SESSION_ID=$SESS_A" "CLAUDE_PID=$$" bash "$k1_pre" write 3822 ) >/dev/null 2>&1
if [ "$k1_pin" -eq 0 ] && [ ! -L "$L46p/$MARKER" ] && [ -f "$L46p/$MARKER" ]; then
  ok "POSITIVE CONTROL: with the -L handling reverted, the same write REPLACES the dangling symlink with a regular file — the silent clobber is real"
else
  bad "the positive control did not reproduce the clobber: pin-failures=$k1_pin still-a-link=$([ -L "$L46p/$MARKER" ] && echo yes || echo no) regular-file=$([ -f "$L46p/$MARKER" ] && echo yes || echo no)"
fi

# ===========================================================================
case_begin 28-case-floor "CASE FLOOR: a silently shrunken suite must RED, not green (#3544)"
# ===========================================================================
REQUIRED_CASES="1-write-verify-owned 2-ac3-unstamped-prose-refused 3-foreign-issue 4-foreign-machine
5-foreign-worktree 6-session-gone-adoptable 7-session-live-peer 8-pid-unrecordable-unknown
9-writer-refuses-sentinel-body 10-reader-refuses-duplicate-sentinel 11-machine-agrees-with-claim-sh
12-placeholder-reason-refused 13-write-over-foreign-refuses 14-absent-is-distinct
15-pid-reuse-recognised 16-closed-verdict-grammar 17-write-failure-emits-a-verdict
18-control-chars-stay-anchored 19-control-char-worktree-refused
20-same-process-is-owned 21-write-over-unstamped-migrates
22-no-dead-letter-remedies 23-durable-fields-survive 24-serialization
25-displaced-sentinel-is-not-legacy 26-unusable-start-window 27-pre-rename-validation
29-missing-liveness-library 30-native-diagnostics-stay-anchored
31-adoption-provenance-survives
32-failed-scan-is-not-no-match 33-signals-emit-one-verdict
34-shift-never-leaks-bash-diagnostics 35-one-verdict-per-failure-mode
36-anchor-holds-on-every-stream
37-machine-axis-must-be-measurable
38-adopt-never-calls-a-live-owner-gone
39-body-bytes-survive-repeated-writes
40-worktree-axis-must-be-measurable 41-body-without-trailing-newline
42-verdict-emission-is-a-critical-section
43-identity-recorded-losslessly 44-body-file-is-read-not-stat-gated
45-unknown-prologue-keys-survive 46-symlink-marker-is-never-clobbered 28-case-floor"
CASE_FLOOR=46
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
if [ "$PASS" -ge 172 ]; then
  ok "assertion floor: $PASS assertions passed (>= 172)"
else
  bad "assertion floor breached: only $PASS assertions passed (floor 172)"
fi

# ===========================================================================
echo
echo "==== DRIVE-ISSUE-STATE TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
