#!/usr/bin/env bash
#
# Regression tests for scripts/flow/advertised-collision-scan.sh (issue #3436,
# lead deliverable 2, epic #2664).
#
# HERMETIC BY CONSTRUCTION: a mktemp BARE repo stands in for origin (so
# `git ls-remote` is REAL, not stubbed), a PATH-shimmed fake `gh` stands in for
# the board read, and LANE_ROOT points into the sandbox. NO network, NO GitHub,
# NO python3, NO cargo — this suite runs in the gate's `tooling-tests` component
# BEFORE its python3 gate, so it must need nothing beyond bash + git + coreutils.
#
# Run standalone:   bash scripts/tests/test_advertised_collision_scan.sh
#
# THE PROPERTY UNDER TEST IS THREE-FACTS-ANDED PLUS POSITIVE-DETECTION-ONLY:
#   * all three facts true                 -> the row is reported, exit 3
#   * ANY ONE fact false                   -> nothing reported, exit 1
#   * ANY input unmeasurable               -> exit 1 AND a line NAMING the input
#   * nothing reported                     -> exit 1, NEVER 0
#   * the scan MUTATES NOTHING
# The one-fact-false cases are three separate cases on purpose: a detector that
# fires on two of the three facts passes a combined case and fails here.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAN="$SCRIPT_DIR/../flow/advertised-collision-scan.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

gg() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/collision-scan-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
W="$T/w"
export LANE_ROOT="$T/lanes"
mkdir -p "$LANE_ROOT"

gg init --bare -q "$ORIGIN"
gg clone -q "$ORIGIN" "$W" 2>/dev/null
(
  cd "$W" || exit 1
  echo seed >seed.txt
  gg add seed.txt
  gg commit -qm seed
  gg push -q -u origin main
)

push_branch() {   # <issue> — a pushed lane branch, no claim ref
  (
    cd "$W" || exit 1
    gg checkout -q -b "issue-$1-slug" main
    gg commit -q --allow-empty -m "work on issue $1"
    gg push -q origin "issue-$1-slug"
    gg checkout -q main
    gg branch -q -D "issue-$1-slug"
  )
}
push_claim() {    # <issue> — hold refs/claims/issue-<N>
  (
    cd "$W" || exit 1
    gg commit -q --allow-empty -m "claim issue=$1"
    gg push -q origin "HEAD:refs/claims/issue-$1"
    gg reset -q --hard HEAD~1
  )
}

# mk_gh <dir> <issue...> — a fake `gh` whose board read answers with those issue
# numbers, one row per line in the shape the real `--jq` now produces:
# `<type>|<owner/repo>|<number>` (#3436, roborev round 14 — the row used to be the bare
# number, so a Ready PULL REQUEST or another repository's item collided with the
# identically-numbered issue here). A bare number argument is expanded to an ordinary Issue
# on the scanned repo; an argument already containing `|` is passed through VERBATIM, which
# is how the drift/draft/PR/foreign-repo cases inject their own shapes.
# It also RECORDS its argv, so a case can assert HOW the board was read.
mk_gh() {
  local dir="$1"; shift
  mkdir -p "$dir"
  {
    printf '#!/usr/bin/env bash\n'
    printf 'printf "%%s\\n" "$@" >>"%s/gh-args.txt"\n' "$dir"
    local n
    for n in "$@"; do
      case "$n" in
        *"|"*) printf 'printf "%%s\\n" %s\n' "$(printf '%q' "$n")" ;;
        # A bare `null` is a DRAFT item, which is what `.content.type` reports it as — NOT an
        # Issue with a null number, which the API never emits and which the scan now correctly
        # calls schema drift (#3436, roborev round 21). The round-14 rewrite expanded every bare
        # value as an Issue, so this mock was manufacturing an impossible row and a control case
        # was asserting the scan tolerate it.
        null)  printf 'printf "%%s\\n" %s\n' "$(printf '%q' "DraftIssue|null|null")" ;;
        *)     printf 'printf "%%s\\n" %s\n' "$(printf '%q' "Issue|pmcfadin/cqlite|$n")" ;;
      esac
    done
    printf 'exit 0\n'
  } >"$dir/gh"
  chmod +x "$dir/gh"
}

run_scan() {   # <ghdir> [args...]
  local ghdir="$1"; shift
  ( PATH="$ghdir:$PATH" CLAIM_REMOTE="$ORIGIN" bash "$SCAN" "$@" )
}

refs_snapshot() { gg ls-remote "$ORIGIN" | sort; }
tree_snapshot() { find "$LANE_ROOT" 2>/dev/null | sort; }

# ===========================================================================
echo "TEST 1: all three facts true -> the row is REPORTED, exit 3"
# ===========================================================================
push_branch 600
GH1="$T/gh-600"; mk_gh "$GH1" 600
out=$(run_scan "$GH1"); rc=$?
# The prefix says WINDOW, never bare COLLISION: the three facts are a signature of a window in
# which a collision is possible, not proof one is occurring — nobody may be in that lane. A
# revert to `COLLISION:` reds here rather than passing.
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q '^COLLISION-WINDOW: issue=600 ' \
   && ! printf '%s\n' "$out" | grep -qE '^COLLISION: ' \
   && printf '%s\n' "$out" | grep -q 'branches=refs/heads/issue-600-slug' \
   && printf '%s\n' "$out" | grep -q 'claim-ref=absent' \
   && printf '%s\n' "$out" | grep -q 'RESULT=FOUND'; then
  ok "board Ready + pushed branch + no claim ref => one COLLISION row, exit 3"
else
  bad "expected a COLLISION row for issue 600 and exit 3; got rc=$rc
$out"
fi

# The row composes the two locks the issue says know nothing about each other.
if printf '%s\n' "$out" | grep -q 'lane-lock='; then
  ok "the row carries the machine-local lane-lock state"
else
  bad "the row carried no lane-lock= field:
$out"
fi

# HOW the board was read is part of the contract: an UNFILTERED item-list
# silently truncates this 900+ item board and has produced wrong 'nothing is
# Ready' reads, so the filter must be server-side and the limit explicit.
ghargs=$(cat "$GH1/gh-args.txt" 2>/dev/null)
if printf '%s\n' "$ghargs" | grep -qx 'status:Ready' \
   && printf '%s\n' "$ghargs" | grep -qx 'item-list' \
   && printf '%s\n' "$ghargs" | grep -qx -- '--query' \
   && printf '%s\n' "$ghargs" | grep -qx -- '-L' \
   && ! printf '%s\n' "$ghargs" | grep -qx 'api'; then
  ok "the board is read with a SERVER-SIDE filtered item-list (--query status:Ready, explicit -L), not GraphQL"
else
  bad "board read was not a filtered item-list; gh argv was:
$ghargs"
fi

# ===========================================================================
echo "TEST 2: fact (3) false — a HELD claim ref closes the window (exit 1)"
# ===========================================================================
push_branch 601
push_claim 601
GH2="$T/gh-601"; mk_gh "$GH2" 601
out=$(run_scan "$GH2"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=601' \
   && printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "a held refs/claims/issue-601 is NOT reported (exit 1)"
else
  bad "expected issue 601 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 3: fact (2) false — Ready with NO pushed branch (exit 1)"
# ===========================================================================
GH3="$T/gh-602"; mk_gh "$GH3" 602    # 602 is Ready, and has no branch at all
out=$(run_scan "$GH3"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=602'; then
  ok "a Ready issue with no pushed issue-602-* branch is NOT reported (exit 1)"
else
  bad "expected issue 602 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 4: fact (1) false — a pushed branch whose board Status is NOT Ready (exit 1)"
# ===========================================================================
push_branch 603
GH4="$T/gh-nonready"; mk_gh "$GH4" 999999   # 603 absent from the Ready column
out=$(run_scan "$GH4"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=603'; then
  ok "a pushed branch for an issue the board does NOT offer as Ready is NOT reported (exit 1)"
else
  bad "expected issue 603 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 5: NONE-REPORTED is exit 1 and NEVER exit 0"
# ===========================================================================
# The whole point of positive-detection-only: an implementation returning 0 for
# 'nothing found' would otherwise look correct, and a cron reading 0 as a clean
# bill of health is #3393's fail-open family.
GH5="$T/gh-empty"; mk_gh "$GH5"
out=$(run_scan "$GH5"); rc=$?
# `[ "$rc" -ne 0 ]` used to sit beside the `-eq 1`, which is tautological — it read like a
# second check and was none (#3436 FIX 13e). What actually needs asserting alongside the
# exit code is the TEXT: the run must SAY it is not a clean bill of health, since that
# sentence is the only thing standing between exit 1 and a cron treating it as all-clear.
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED' \
   && printf '%s\n' "$out" | grep -q 'positive-detection only' \
   && printf '%s\n' "$out" | grep -q 'NEVER 0'; then
  ok "an empty Ready column yields exit 1 (never 0) and SAYS in words that it is not a clean bill of health"
else
  bad "expected exit 1 with RESULT=NONE-REPORTED; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 6: unmeasurable board — gh EXITS NON-ZERO (exit 1, input NAMED)"
# ===========================================================================
GHFAIL="$T/gh-fail"; mkdir -p "$GHFAIL"
printf '#!/usr/bin/env bash\necho "gh: HTTP 502" >&2\nexit 1\n' >"$GHFAIL/gh"
chmod +x "$GHFAIL/gh"
out=$(run_scan "$GHFAIL" 2>/dev/null); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=board-status' \
   && ! printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "a failing gh is UNMEASURABLE what=board-status (exit 1), NOT a 'none found'"
else
  bad "expected UNMEASURABLE what=board-status exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 7: unmeasurable board — gh NOT ON PATH AT ALL (exit 1, input NAMED)"
# ===========================================================================
# A minimal PATH holding only the tools the scan needs, so `gh` is genuinely
# absent rather than shadowed by a stub that pretends to be absent.
MINBIN="$T/minbin"; mkdir -p "$MINBIN"
REALBASH="$(command -v bash)"
for tool in bash git awk grep head sort tr cut basename dirname timeout cat find; do
  p=$(command -v "$tool" 2>/dev/null) && ln -sf "$p" "$MINBIN/$tool"
done
out=$( PATH="$MINBIN" CLAIM_REMOTE="$ORIGIN" "$REALBASH" "$SCAN" 2>/dev/null ); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=board-status' \
   && printf '%s\n' "$out" | grep -q 'not on PATH'; then
  ok "gh absent from PATH is UNMEASURABLE what=board-status naming the missing tool (exit 1)"
else
  bad "expected UNMEASURABLE naming gh-not-on-PATH exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 8: unmeasurable branches — ls-remote against an unreachable remote (exit 1, input NAMED)"
# ===========================================================================
GH8="$T/gh-600b"; mk_gh "$GH8" 600
out=$( PATH="$GH8:$PATH" CLAIM_REMOTE="$T/does-not-exist.git" bash "$SCAN" 2>/dev/null ); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=issue-branches' \
   && printf '%s\n' "$out" | grep -q 'ls-remote' \
   && ! printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "an unreachable remote is UNMEASURABLE what=issue-branches naming ls-remote (exit 1)"
else
  bad "expected UNMEASURABLE what=issue-branches exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 9: the scan MUTATES NOTHING — refs and lane tree byte-identical"
# ===========================================================================
# It reports and never acts, because only the session on that box knows whether
# it owns the branch: from here, 'the lane is yours' and 'a peer abandoned it'
# look identical and have OPPOSITE remedies.
# Give lane 600 a REAL, live lane lock first, so the no-mutation claim is tested
# against an EXISTING record (a lane with no record can be left unchanged by a
# tool that writes only when it finds one) and so the row's lane-lock field is
# something other than FREE.
LANELOCK="$SCRIPT_DIR/../flow/lane-lock.sh"
sleep 900 &
SLEEPER=$!
LANE_LOCK_PID=$SLEEPER bash "$LANELOCK" acquire 600 >/dev/null 2>&1
# The lock record lives in the sibling LOCK ROOT, not in the lane directory (#3436:
# `git worktree add` refuses a target that exists at all, so a lock inside the lane
# would forbid acquire-before-worktree-add).
LANE_LOCK_RECORD_600="$LANE_ROOT/.lane-locks/lane-600.lock"
recordBefore=$(cat "$LANE_LOCK_RECORD_600" 2>/dev/null)
refsBefore=$(refs_snapshot)
treeBefore=$(tree_snapshot)
GH9="$T/gh-mutate"; mk_gh "$GH9" 600 601 602 603
out=$(run_scan "$GH9"); rc=$?
refsAfter=$(refs_snapshot)
treeAfter=$(tree_snapshot)
recordAfter=$(cat "$LANE_LOCK_RECORD_600" 2>/dev/null)
# THE LOCK-RECORD CLAUSE IS THE ONLY /proc-DEPENDENT THING IN THIS ENTIRE SUITE, so it is what
# gets conditioned -- NOT the suite, and not even this test (#3436, roborev round 30). The
# acquire above needs boot-id + /proc/<pid>/stat, and on a host without /proc it REFUSES with
# reason=unresolved-identity by design, leaving recordBefore empty and failing `-n`. An earlier
# revision handled that by skipping this whole suite at the gate, which deleted ALL 32 cases of
# portability coverage for a script explicitly written to bash 3.2 -- the opposite of what a
# macOS run is for, and inconsistent with how the very same round scoped test_claim_lock.sh to
# two tests rather than muting the file. The refs/tree/rc assertions are pure git + filesystem
# and are exactly what a macOS run should exercise, so they run everywhere.
if [ "${LANE_LOCK_TEST_OS:-$(uname -s 2>/dev/null || echo unknown)}" = "Linux" ]; then
  record_ok=$([ -n "$recordBefore" ] && [ "$recordBefore" = "$recordAfter" ] && echo 1 || echo 0)
  record_note="AND an existing lane-lock record"
else
  # Affirmatively NOT asserted, and said out loud rather than folded silently into a pass.
  record_ok=1
  record_note="(lane-lock record NOT asserted: no /proc on this host, so acquire cannot record one)"
fi
if [ "$refsBefore" = "$refsAfter" ] && [ "$treeBefore" = "$treeAfter" ] \
   && [ "$record_ok" = "1" ] && [ "$rc" -eq 3 ]; then
  ok "a FOUND run left every ref, the whole lane tree $record_note byte-identical"
else
  bad "the scan mutated something (rc=$rc)
refs before:
$refsBefore
refs after:
$refsAfter
tree before:
$treeBefore
tree after:
$treeAfter
record before: $recordBefore
record after:  $recordAfter"
fi

# The lane-lock field is READ, not invented: with a live holder it must report the
# probe's own HELD/ALIVE words rather than the FREE it reported when the lane was
# empty in TEST 1.
if printf '%s\n' "$out" | grep -q 'issue=600 .*lane-lock=HELD/ALIVE'; then
  ok "the row reports the probe's own verdict for a live holder (lane-lock=HELD/ALIVE), not a re-derived one"
else
  bad "expected lane-lock=HELD/ALIVE for issue 600 with a live holder:
$out"
fi
kill "$SLEEPER" 2>/dev/null || true
wait "$SLEEPER" 2>/dev/null || true

# ===========================================================================
echo "TEST 10: --issue narrows the scan; the three facts are unchanged"
# ===========================================================================
out=$(run_scan "$GH9" --issue 600); rc=$?
outOther=$(run_scan "$GH9" --issue 601); rcOther=$?
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q 'issue=600' \
   && [ "$rcOther" -eq 1 ] && ! printf '%s\n' "$outOther" | grep -q '^COLLISION:'; then
  ok "--issue 600 reports only 600 (exit 3); --issue 601 (claim held) reports nothing (exit 1)"
else
  bad "--issue filtering wrong: rc=$rc rcOther=$rcOther
600: $out
601: $outOther"
fi

# ===========================================================================
echo "TEST 11: --json emits one object per row plus a summary, same exit codes"
# ===========================================================================
out=$(run_scan "$GH9" --json --issue 600); rc=$?
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q '"issue":600' \
   && printf '%s\n' "$out" | grep -q '"result":"FOUND"' \
   && ! printf '%s\n' "$out" | grep -q '^COLLISION:'; then
  ok "--json emits a row object and a FOUND summary object, exit 3, with no text rows mixed in"
else
  bad "expected JSON row + summary with exit 3; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 12: --help exits 0 and DOCUMENTS the exit codes; a bad flag is refused"
# ===========================================================================
outHelp=$(bash "$SCAN" --help); rcHelp=$?
rcBad=0; outBad=$(bash "$SCAN" --bogus 2>&1) || rcBad=$?
# `grep` WITHOUT -q, output discarded — NOT `grep -q` (#3436). This capture is 5959 bytes,
# above the 4096-byte pipe buffer, so `grep -q` can exit on its match while `printf` is still
# writing, take SIGPIPE, and — under this suite's `set -o pipefail` — report the pipeline as
# failed even though the pattern MATCHED. That is the shape that flaked the lane-lock --help
# case ~1 in 3 full-suite runs at any load. Without -q, grep must read all input to emit all
# matches, so there is no early exit and no race. The regexes are kept because `case` cannot
# express `^ *3 +…`; this is the minimal change that removes the race and not the assertion.
# Fixed here BEFORE it flaked, because the writer is the same size class.
if [ "$rcHelp" -eq 0 ] \
   && printf '%s\n' "$outHelp" | grep 'POSITIVE-DETECTION ONLY' >/dev/null \
   && printf '%s\n' "$outHelp" | grep -E '^ *3 +at least one row' >/dev/null \
   && printf '%s\n' "$outHelp" | grep -E '^ *1 +no row was reported' >/dev/null \
   && printf '%s\n' "$outHelp" | grep -E '^ *64 +usage error' >/dev/null \
   && [ "$rcBad" -eq 64 ] && printf '%s\n' "$outBad" | grep -q 'unknown argument'; then
  ok "--help exits 0 documenting exit 3/1/64 and never-exit-0; an unknown flag is REFUSED (exit 64), not ignored"
else
  bad "help/usage contract wrong: rcHelp=$rcHelp rcBad=$rcBad
help: $outHelp
bad:  $outBad"
fi

# ===========================================================================
echo "TEST 13: --json ESCAPES its string values — a quote, a backslash and a tab"
# ===========================================================================
# Remote names, lane paths, branch refs and failure details are caller- or
# filesystem-derived and used to be interpolated RAW, so one `"` produced output that
# CLAIMED to be JSON and was not — the worst shape for a machine-read field, because the
# consumer's parse error names the parser and not the value.
#
# THE VECTOR IS THE REMOTE, because that is a value a caller really controls
# (CLAIM_REMOTE) and it reaches the UNMEASURABLE detail verbatim; a branch name cannot
# hold a quote, so it cannot carry this test.
BAD_REMOTE="$(printf 'ori"gin\\back\tslash')"
GH13="$T/gh-json-esc"; mk_gh "$GH13" 600
rc13=0
out13=$( PATH="$GH13:$PATH" CLAIM_REMOTE="$BAD_REMOTE" bash "$SCAN" --json 2>/dev/null ) || rc13=$?
# A clean run's JSON is checked in the SAME case, so a helper that escapes by mangling
# every value (or by emitting nothing) cannot pass by breaking only the happy path.
rc13ok=0
out13ok=$(run_scan "$GH13" --json --issue 600) || rc13ok=$?

json_verdict=""
# PRESENT IS NOT USABLE (#3436, found by measuring rather than reading). `command -v python3`
# only proves the name RESOLVES. A python3 that exists and cannot run — a truncated install,
# a broken venv shim, a wrapper on a full disk — took the python3 branch, failed the inner
# check, and made this case report "--json escaping broken": a FALSE RED blaming the code
# under test for a TOOLING failure. Same wrong-cause defect as the board parse reporting an
# auth fault for schema drift, in a test I wrote to catch wrong-cause defects.
#
# So probe that python3 actually EXECUTES before trusting it, and treat unusable exactly like
# absent — which is what makes this suite genuinely python3-OPTIONAL, as its header claims,
# rather than python3-optional-only-when-python3-is-missing-entirely.
py_usable=no
if command -v python3 >/dev/null 2>&1 && python3 -c 'pass' >/dev/null 2>&1; then py_usable=yes; fi
if [ "$py_usable" = yes ]; then
  # python3 is used WHEN PRESENT and never required: this suite runs in `tooling-tests`
  # before the gate's python3 gate, so the fallback below must be able to carry the case
  # alone.
  if printf '%s\n' "$out13" | python3 -c '
import json, sys
want = sys.argv[1]
lines = [l for l in sys.stdin.read().splitlines() if l.strip()]
if not lines:
    sys.exit("no JSON emitted at all")
objs = [json.loads(l) for l in lines]          # raises on invalid JSON
if not any(want in str(o.get("detail", "")) for o in objs):
    sys.exit("the raw remote did not round-trip through the escaping")
' "$BAD_REMOTE" >/dev/null 2>&1; then
    if printf '%s\n' "$out13ok" | python3 -c '
import json, sys
lines = [l for l in sys.stdin.read().splitlines() if l.strip()]
if len(lines) < 2:
    sys.exit("expected a row object and a summary object")
[json.loads(l) for l in lines]
' >/dev/null 2>&1; then
      json_verdict="parsed-by-python3"
    fi
  fi
else
  # BASH-ONLY FALLBACK: assert the escapes are literally present and that no RAW control
  # byte survived into the output.
  esc_q=no; esc_b=no; esc_t=no; raw_tab=yes
  case "$out13" in *'\"'*)  esc_q=yes ;; esac
  case "$out13" in *'\\'*)  esc_b=yes ;; esac
  case "$out13" in *'\t'*)  esc_t=yes ;; esac
  case "$out13" in *"$(printf '\t')"*) raw_tab=yes ;; *) raw_tab=no ;; esac
  if [ "$esc_q" = yes ] && [ "$esc_b" = yes ] && [ "$esc_t" = yes ] && [ "$raw_tab" = no ]; then
    json_verdict="escapes-present-no-raw-control (python3 absent or unusable)"
  fi
fi

if [ "$rc13" -eq 1 ] && [ -n "$json_verdict" ]; then
  ok "--json output stays valid JSON with a quote, a backslash and a tab in a value, and the clean run still parses ($json_verdict)"
else
  bad "--json escaping broken: rc=$rc13 (expected 1) verdict='${json_verdict:-FAILED}'
$out13
clean run (rc=$rc13ok):
$out13ok"
fi

# ===========================================================================
echo "TEST 14: the page-truncation notice counts the RAW page, not the filtered rows"
# ===========================================================================
# NO CASE COVERED THIS PATH AT ALL. The guard compared the ISSUE-NUMBERED subset against
# the 100-row limit, so a Ready column of exactly 100 containing 3 drafts counted 97, no
# notice fired, and the run printed `measured=yes` for a page that may have been
# TRUNCATED — a fail-OPEN, and a truncated Ready column can only ever HIDE rows.
# The fake gh emits one line per row exactly as the real `--jq` does — since #3436 round 14
# that is `<type>|<owner/repo>|<number>`, with a draft as `DraftIssue|null|null` — so the two
# counts differ by construction here.
mk_ready_page() {   # <dir> <numeric-rows> <draft-rows>
  local dir="$1" nums="$2" drafts="$3" i=0
  mkdir -p "$dir"
  {
    printf '#!/usr/bin/env bash\n'
    i=0
    while [ "$i" -lt "$nums" ]; do printf 'printf "%%s\\n" %s\n' "Issue\\|pmcfadin/cqlite\\|$((7000 + i))"; i=$((i + 1)); done
    i=0
    while [ "$i" -lt "$drafts" ]; do printf 'printf "DraftIssue|null|null\\n"\n'; i=$((i + 1)); done
    printf 'exit 0\n'
  } >"$dir/gh"
  chmod +x "$dir/gh"
}

# (a) 97 issues + 3 drafts = a 100-row page AT the limit. The old guard saw 97.
mk_ready_page "$T/gh-page-100" 97 3
out14a=$(run_scan "$T/gh-page-100" --json); rc14a=$?
out14at=$(run_scan "$T/gh-page-100"); rc14at=$?
if [ "$rc14a" -eq 1 ] && [ "$rc14at" -eq 1 ] \
   && printf '%s\n' "$out14a" | grep -q '"board_page_at_limit":true' \
   && printf '%s\n' "$out14a" | grep -q '"board_page_rows":100' \
   && printf '%s\n' "$out14a" | grep -q '"ready":97' \
   && printf '%s\n' "$out14at" | grep -q 'notice=board-page-at-limit'; then
  ok "(a) a 100-row page whose limit is reached partly by DRAFTS is reported at-limit (page-rows=100, ready=97) in both output modes"
else
  bad "(a) expected board_page_at_limit=true with board_page_rows=100 and ready=97; got rc=$rc14a/$rc14at
$out14a
$out14at"
fi

# (b) NEGATIVE CONTROL, so the guard is not simply always-on: 96 + 3 = 99 rows.
mk_ready_page "$T/gh-page-99" 96 3
out14b=$(run_scan "$T/gh-page-99" --json); rc14b=$?
out14bt=$(run_scan "$T/gh-page-99"); rc14bt=$?
if [ "$rc14b" -eq 1 ] \
   && printf '%s\n' "$out14b" | grep -q '"board_page_at_limit":false' \
   && printf '%s\n' "$out14b" | grep -q '"board_page_rows":99' \
   && ! printf '%s\n' "$out14bt" | grep -q 'notice=board-page-at-limit'; then
  ok "(b) control: a 99-row page is NOT reported at-limit and prints no notice — the guard is measuring, not asserting"
else
  bad "(b) expected board_page_at_limit=false with board_page_rows=99 and no notice; got rc=$rc14b
$out14b
$out14bt"
fi

# (c) A DRAFT ROW IS NOT A CANDIDATE. The `null` rows must never be read as issue numbers
# — otherwise the fix would trade a fail-open guard for a fabricated row.
if ! printf '%s\n' "$out14a" | grep -q '"issue":null' \
   && ! printf '%s\n' "$out14a" | grep -q '"issue":"null"'; then
  ok "(c) draft rows count toward the page length but are never emitted as candidate issues"
else
  bad "(c) a draft row leaked into the candidate set:
$out14a"
fi

# ===========================================================================
echo
# ===========================================================================
echo "TEST 15: an UNRECOGNISED board row is UNMEASURABLE, not a silent non-candidate (round 5)"
# ===========================================================================
# `*[!0-9]*) continue` dropped ANY unexpected value, so gh schema drift or a malformed --jq
# produced measured=yes with rows HIDDEN -- the fail-open direction, in the one tool whose
# entire contract is that it never gives a clean bill of health.
#
# The measured= assertions are anchored to the SUMMARY line on purpose. The scan reports
# measurement PER FACT, so a run whose board read failed still legitimately prints
# measured=yes for the BRANCHES fact -- an unanchored `! grep measured=yes` fails on correct
# output. Found by this case failing while the behaviour was right.
mk_gh "$T/bin13" not-an-issue-number
rc13=0; out13="$( run_scan "$T/bin13" 2>&1 )" || rc13=$?
if [ "$rc13" -eq 1 ] \
   && printf '%s' "$out13" | grep -q 'UNMEASURABLE what=board-status' \
   && printf '%s' "$out13" | grep -q 'not-an-issue-number' \
   && printf '%s' "$out13" | grep -q 'schema drift' \
   && printf '%s' "$out13" | grep -qE '^SCAN: advertised-collision .*measured=no' \
   && ! printf '%s' "$out13" | grep -qE '^SCAN: advertised-collision .*measured=yes'; then
  ok "an unrecognised board row is UNMEASURABLE (exit 1), NAMES the offending value and says gh ANSWERED — never measured=yes with rows hidden"
else
  bad "expected UNMEASURABLE naming the row and schema drift; got rc=$rc13
$out13"
fi

# CONTROL: a DRAFT row (`null` for .content.number) is a RECOGNISED non-candidate and must
# NOT trip the guard -- otherwise every board with a draft item reads as unmeasurable.
mk_gh "$T/bin13b" null 600
rc13b=0; out13b="$( run_scan "$T/bin13b" 2>&1 )" || rc13b=$?
if printf '%s' "$out13b" | grep -qE '^SCAN: advertised-collision .*measured=yes' \
   && ! printf '%s' "$out13b" | grep -q 'UNMEASURABLE'; then
  ok "control: a DRAFT row (null) is a recognised non-candidate — measured=yes, no UNMEASURABLE, so the guard does not fire on ordinary boards"
else
  bad "expected a null draft row to be skipped with measured=yes; got rc=$rc13b
$out13b"
fi

# AN EMPTY ISSUE NUMBER IS THE SAME CLASS AND MATCHED NEITHER ARM (#3436, roborev round 31).
# `Issue|pmcfadin/cqlite|` is not the literal `null`, and an empty string contains no non-digit
# character, so `*[!0-9]*` did not catch it either: the row fell through BOTH arms and was
# ACCEPTED, producing measured=yes off a row carrying no issue at all. The sibling `row_repo`
# arm had spelled this `null|''` since round 21; the number arm had not — the same rule written
# correctly for one field and incompletely for the field beside it.
mk_gh "$T/bin13c" "Issue|pmcfadin/cqlite|"
rc13c=0; out13c="$( run_scan "$T/bin13c" 2>&1 )" || rc13c=$?
if [ "$rc13c" -eq 1 ] \
   && printf '%s' "$out13c" | grep -q 'UNMEASURABLE what=board-status' \
   && printf '%s' "$out13c" | grep -qE '^SCAN: advertised-collision .*measured=no' \
   && ! printf '%s' "$out13c" | grep -qE '^SCAN: advertised-collision .*measured=yes'; then
  ok "an EMPTY issue number is UNMEASURABLE too, not accepted as a candidate (round 31)"
else
  bad "expected an empty issue number to read UNMEASURABLE; got rc=$rc13c
$out13c"
fi

# ===========================================================================
echo "TEST 16: a credential-bearing remote is REDACTED in every output path"
# ===========================================================================
# CLAIM_REMOTE accepts a URL and was printed VERBATIM in two `unmeasurable` details and in
# both summaries, so a token-bearing HTTPS remote wrote the SECRET into every log, PR comment
# and CI transcript carrying the output. JSON escaping does not help — escaping makes a secret
# well-formed, not absent. Asserted on BOTH output modes, and on the failure path as well as
# the summary, because the details are where the remote appears most.
SECRET='ghp_TESTONLYSECRETVALUE0001'
CRED_REMOTE="https://bot:${SECRET}@example.invalid/org/repo.git"
for mode in "" "--json"; do
  out16=$( cd "$T" && CLAIM_REMOTE="$CRED_REMOTE" LANE_ROOT="$LANE_ROOT" bash "$SCAN" $mode 2>&1 )
  label="${mode:-text}"
  if ! printf '%s' "$out16" | grep -q "$SECRET" \
     && printf '%s' "$out16" | grep -q '\*\*\*@example.invalid'; then
    ok "($label) the token is ABSENT and the remote is shown redacted as ***@example.invalid — host and path kept, so 'which remote failed' stays answerable"
  else
    bad "($label) credential leaked or redaction missing:
$out16"
  fi
done

# CONTROL: a remote with NO userinfo must pass through UNCHANGED, or the redaction would make
# every ordinary diagnostic unreadable — the over-redaction failure mode.
out16c=$( cd "$T" && CLAIM_REMOTE="$ORIGIN" LANE_ROOT="$LANE_ROOT" bash "$SCAN" 2>&1 )
if printf '%s' "$out16c" | grep -qF "$ORIGIN" && ! printf '%s' "$out16c" | grep -q '\*\*\*@'; then
  ok "control: a remote with no userinfo is printed unchanged — the redaction does not over-reach"
else
  bad "control: a credential-free remote was altered:
$out16c"
fi

# ===========================================================================
# ROUND 14: a board NUMBER is not an ISSUE NUMBER ON THIS REMOTE.
# ===========================================================================
# The row used to be `.content.number` alone, so a Ready PULL REQUEST — or an item from
# ANOTHER repository — produced a candidate for the identically-numbered issue here, i.e. a
# reported collision window for a lane that does not exist. All three kinds legitimately sit
# on this board. Branch + no-claim-ref are held CONSTANT across these cases; only the board
# row's KIND changes, so the row's kind is the only thing under test.
GHPR="$T/gh-pr-600"; mk_gh "$GHPR" 'PullRequest|pmcfadin/cqlite|600'
out_pr=$(run_scan "$GHPR"); rc_pr=$?
if [ "$rc_pr" -eq 1 ] && ! printf '%s\n' "$out_pr" | grep -q '^COLLISION-WINDOW: issue=600 '; then
  ok "a Ready PULL REQUEST numbered 600 is NOT a collision candidate (exit 1, no row)"
else
  bad "a Ready PR was treated as issue 600: rc=$rc_pr
$out_pr"
fi

GHFR="$T/gh-foreign-600"; mk_gh "$GHFR" 'Issue|someone-else/other-repo|600'
out_fr=$(run_scan "$GHFR"); rc_fr=$?
if [ "$rc_fr" -eq 1 ] && ! printf '%s\n' "$out_fr" | grep -q '^COLLISION-WINDOW: issue=600 '; then
  ok "an Issue numbered 600 in ANOTHER repository is NOT a collision candidate (exit 1, no row)"
else
  bad "another repo's issue 600 was treated as ours: rc=$rc_fr
$out_fr"
fi

# CONTROL: the SAME number, as an Issue on the scanned repo, still IS a candidate — so the two
# refusals above are the kind filter working, not the case having stopped reaching the report.
GHOK="$T/gh-ctl-600"; mk_gh "$GHOK" 600
out_ok=$(run_scan "$GHOK"); rc_ok=$?
if [ "$rc_ok" -eq 3 ] && printf '%s\n' "$out_ok" | grep -q '^COLLISION-WINDOW: issue=600 '; then
  ok "CONTROL: the same number as an Issue on the scanned repo IS still reported (exit 3)"
else
  bad "CONTROL: the kind filter is refusing everything, not just PRs/foreign repos: rc=$rc_ok
$out_ok"
fi

# An UNRECOGNISED kind is UNMEASURABLE, not a silent non-candidate — round 5's rule, which the
# new three-field row must not weaken.
GHUK="$T/gh-unknown-kind"; mk_gh "$GHUK" 'Sponsorship|pmcfadin/cqlite|600'
out_uk=$(run_scan "$GHUK"); rc_uk=$?
if [ "$rc_uk" -eq 1 ] && printf '%s\n' "$out_uk" | grep -q 'UNMEASURABLE'; then
  ok "an unrecognised board row KIND is UNMEASURABLE, not silently dropped"
else
  bad "unrecognised kind was dropped instead of reported unmeasurable: rc=$rc_uk
$out_uk"
fi

# ===========================================================================
# ROUND 14: the JSON branch must not contradict the text branch on `measured`.
# ===========================================================================
# Round 12 fixed the HUMAN line to make `measured=` follow coverage and left the JSON branch
# emitting a hard-coded "yes" — the same defect, one branch over, found two rounds later. The
# JSON is what an automated consumer reads, so it was the worse of the two to leave.
mk_ready_page "$T/gh-page-100b" 97 3
out_j=$(run_scan "$T/gh-page-100b" --json)
out_t=$(run_scan "$T/gh-page-100b")
if printf '%s\n' "$out_j" | grep -q '"board_page_at_limit":true' \
   && printf '%s\n' "$out_j" | grep -q '"measured":"no"' \
   && printf '%s\n' "$out_t" | grep -q 'measured=no'; then
  ok "at the page limit, BOTH the JSON and the text report measured=no — the two branches agree"
else
  bad "JSON/text disagree on measured at the page limit:
json=$out_j
text=$out_t"
fi

# ===========================================================================
# ROUND 18: a LEADING-ZERO issue number must not make a row disappear.
# ===========================================================================
# Every comparison in this script is TEXTUAL. `claim.sh` accepts a leading-zero issue, so
# `refs/heads/issue-0600-slug` is reachable — and against board issue `600` it silently matched
# nothing, dropping a real row. A false all-clear in the one tool whose contract is that it never
# gives one.
#
# ROUND 17 FOUND THIS ON `--issue` AND I FIXED ONLY THAT SITE. Round 18 found the branch/claim
# derivations one round later. These cases therefore cover EVERY entry point, not the reported
# one: the option, a branch ref, a claim ref, and the board row.
# 0620 is UNUSED by every earlier case (600/601/603 are taken), so a leftover branch cannot make
# these pass. The FIRST draft called helpers that DO NOT EXIST here (the real ones are
# `push_branch`/`push_claim`), so no padded branch was ever created and the case passed on the
# leftover CANONICAL 600 branch from line 108 — a green test that exercised nothing, in the block
# written to catch exactly that. The draft also suppressed the claim call's exit status, which is
# the same error-hiding this PR removed from flow-finalize in round 12; no call here is suppressed.
push_branch 0620                       # a zero-padded BRANCH ref: refs/heads/issue-0620-slug
GH18="$T/gh-0620"; mk_gh "$GH18" 620   # board carries the CANONICAL number
out18=$(run_scan "$GH18"); rc18=$?
if [ "$rc18" -eq 3 ] && printf '%s\n' "$out18" | grep -q '^COLLISION-WINDOW: issue=620 '; then
  ok "a zero-padded BRANCH (issue-0620-slug) matches board issue 620 and is reported"
else
  bad "zero-padded branch dropped the row: rc=$rc18
$out18"
fi

out18b=$(run_scan "$GH18" --issue 0620); rc18b=$?
if [ "$rc18b" -eq 3 ] && printf '%s\n' "$out18b" | grep -q '^COLLISION-WINDOW: issue=620 '; then
  ok "--issue 0620 selects the same row as the canonical number"
else
  bad "--issue 0620 selected nothing: rc=$rc18b
$out18b"
fi

# CONTROL: a zero-padded CLAIM ref must SUPPRESS the row. Canonicalisation has to work in the
# direction that REMOVES a candidate too, or the guard leaks rows rather than dropping them.
#
# NOTE ON ITS RED-PROOF: this half PASSES against the pre-fix scan, and that is correct, not a
# gap. Pre-fix the padded branch produced no row at all, so "no row" was trivially true — an
# absence assertion cannot tell "correctly suppressed" from "never there". Its force comes from
# the SEQUENCE above it in a post-fix run: the row is PRESENT at out18/out18b and ABSENT here.
push_claim 0620
out18c=$(run_scan "$GH18"); rc18c=$?
if [ "$rc18c" -eq 1 ] && ! printf '%s\n' "$out18c" | grep -q '^COLLISION-WINDOW: issue=620 '; then
  ok "CONTROL: a zero-padded CLAIM ref suppresses the row — canonicalisation works both ways"
else
  bad "CONTROL: a padded claim ref failed to suppress: rc=$rc18c
$out18c"
fi

echo "==== ADVERTISED-COLLISION-SCAN TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
