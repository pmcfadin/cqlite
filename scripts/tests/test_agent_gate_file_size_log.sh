#!/usr/bin/env bash
# Regression test for issues #3401 and #3402: the `file-size` gate component must PERSIST
# its arithmetic as $LOG_DIR/file-size.log on EVERY invocation (#3401), and an ENGAGED
# growth override must be VISIBLE IN THE SUMMARY BLOCK (#3402).
#
# The #3402 defect: with CQLITE_ALLOW_FILE_GROWTH=1 the component reported a bare
# `file-size: PASS (0s)`, so a pasted SUMMARY — the unit of evidence a PR reviewer reads —
# could not be told apart from a run where the ratchet was genuinely satisfied. The
# component now reports its own NON-FAILING `OPT-OUT` token carrying the env var and the
# COUNT, plus a pointer to this component's log — deliberately NOT the file names, which
# live in file-size.log (#3401) and, for a reviewer, in the PR diff. The property that
# matters most here is the
# NEGATIVE one (case 4c): `OPT-OUT` may be emitted ONLY for the affirmative value `1`,
# because a permissive branch keyed on `!= <bad>` would let a typo waive the ratchet.
#
# The #3401 defect: run_file_size computed the base ref, the over-threshold advisory list and
# the exact `path: before -> after (limit N)` growth entries, echoed them to STDOUT — i.e.
# into gate.log, the one file CLAUDE.md forbids an agent to read — and wrote only a bare
# `file-size.result` (`FAIL 0`). So a `file-size: FAIL` in a pasted SUMMARY named NOTHING:
# not the file, not the line counts, not the limit. Every reader re-derived by hand the
# arithmetic the component had just thrown away, and that FAIL is routinely EXPECTED (a
# legitimate diff that grows a big file), so the cost is paid often.
#
# The property pinned here is the LOG'S CONTENT, never its mere existence — a zero-byte or
# contentless file would satisfy "a log is written" while restoring the whole defect. Each
# case therefore asserts the REAL numbers (`900 -> 950 (limit 800)`), the REAL base sha,
# the thresholds and the terminal verdict.
#
# Paths through run_file_size, because the log must exist on ALL of them (AC3), plus the
# three #3402 SUMMARY-visibility paths:
#   1. grown + over threshold             -> FAIL
#   2. over threshold but SHRUNK          -> PASS (advisory list, empty ratchet)
#   3. no changed .rs files at all        -> PASS ("nothing over threshold" still logged)
#   3b. changed .rs files, none over the threshold -> PASS (same log line, DIFFERENT input:
#       case 3's tree is clean, so on its own it never proves a CHANGED-but-small file is
#       handled — only that the component ran with an empty file list)
#   4. grown + CQLITE_ALLOW_FILE_GROWTH=1 -> OPT-OUT (the opt-out is RECORDED, and — since
#      #3402 — VISIBLE in the SUMMARY block, not only in this log)
#   4c. #3402: a MALFORMED override, run for BOTH spellings (`0` and `true`) -> still a
#       ratchet FAIL, never OPT-OUT
#   5. base ref unresolvable              -> PASS (advisory only, ratchet skipped)
#   6. AC2: the FAIL stdout NAMES $LOG_DIR/file-size.log, so it is reachable from the
#      SUMMARY's existing `logs:` line without the reader guessing a filename.
#
# EVERY HELPER HERE FAILS CLOSED (repo doctrine: a positive verdict requires an affirmative
# measurement). A helper whose measurement did not happen must never let an assertion print
# `ok` — so `verdict_of` returns a sentinel no assert accepts rather than leaking the
# PREVIOUS case's verdict through non-`local` state, `has` refuses an EMPTY needle (an empty
# `grep -F` pattern matches every line), `logdir_of` refuses a missing/relative/nonexistent
# `logs:` dir (an empty one would make the AC2 needle the SUFFIX `/file-size.log`, which is
# a substring of the real path and would MATCH having measured nothing), and `mkrepo`
# CHECKS its git setup and re-measures the line counts it just wrote instead of discarding
# stderr.
#
# Hermetic and fast: throwaway git repos under one mktemp, each holding ONLY a copy of the
# gate script (so the gate's `cd "$(dirname "$0")/.."` resolves REPO_ROOT into the temp
# tree) plus a synthetic .rs file. Driven through the REAL `--only file-size` path, which
# skips the dataset preflight and compiles nothing. No cargo, no network, no datasets, no
# Docker. TMPDIR is redirected so every gate LOG_DIR also lands inside this run's namespace,
# and the fixture repos are cut off from the invoker's git config (a global
# `commit.gpgsign=true` / `core.hooksPath` would otherwise break every fixture commit and
# red the gate of record for a reason unrelated to the property under test).
#
# Run standalone:   bash scripts/tests/test_agent_gate_file_size_log.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
# Case 4i runs a REAL FULL gate in a fixture, which must clear the #3544 component-set
# pre-flight first: that needs the canonical remote identity pinned in the fixture's own copy
# of the gate and a committed component manifest beside it. Both are the sanctioned
# artifact-substitution helpers (a settable seam would reopen the hole #3544 closes).
# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.bash
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.bash"

# Never inherit a caller's summary path / parent marker (#2751/#2874 discipline), and
# never block on the machine-wide gate slot (#1825) — these runs compile nothing.
unset AGENT_GATE_SUMMARY_FILE
unset AGENT_GATE_PARENT_RUN_ID
unset GATE_BASE_OVERRIDE
unset CQLITE_ALLOW_FILE_GROWTH
export CQLITE_GATE_DISABLE_CAP=1

PASS=0
FAIL=0
SKIP=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
# A skip is VISIBLE and CENSUSED (it counts toward the exact total below), never silent —
# a case that quietly disappears on some host is a vacuous green by another route.
skip() { printf 'SKIP - %s\n' "$1"; SKIP=$((SKIP + 1)); }

if [ ! -r "$GATE" ]; then
  printf 'FAIL - agent-gate.sh not readable at %s — nothing to test\n' "$GATE"
  exit 1
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-file-size.XXXXXX") || tmp=""
# An unchecked mktemp is not a small risk here: an empty $tmp makes `export TMPDIR=""`
# fall back to /tmp, the EXIT trap `rm -rf ""` clean nothing, and every fixture path
# absolute-from-root — which a normal user survives as a loud failure but ROOT (containers)
# survives as six uncleaned fixture repos littered into `/`.
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - could not create a scratch dir under %s — refusing to run\n' "${TMPDIR:-/tmp}"
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT INT TERM
# Contain each gate run's LOG_DIR (mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX") inside
# this run's namespace so the trap above reclaims them too.
export TMPDIR="$tmp"

# Fixture git must be isolated from the INVOKER's environment, not merely given an
# identity: `git init`/`git commit` also read global `commit.gpgsign`, `core.hooksPath`
# and `init.templateDir`, any one of which turns a fixture commit into a hard failure on
# someone's box. GIT_CONFIG_GLOBAL/SYSTEM=/dev/null kills all three vectors at once (the
# convention scripts/tests/lib/perf-capability-test-lib.sh already uses); the explicit
# `-c` flags below are the belt-and-braces half, and also pin the branch name.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null
GIT_CFG=(-c user.email=gate@example.invalid -c user.name=gate-selftest
         -c init.defaultBranch=main -c commit.gpgsign=false)

# lines <n> <path> — write exactly <n> newline-terminated lines (so `wc -l` == n).
lines() { awk -v n="$1" 'BEGIN { for (i = 1; i <= n; i++) print "// filler line " i }' >"$2"; }

# mkrepo <name> <rs-relpath> <committed-lines> <worktree-lines> <branch>
#   Commits a .rs file of <committed-lines>, then rewrites the WORKTREE copy to
#   <worktree-lines>. A <worktree-lines> of 0 means "leave the commit untouched".
#   Publishes the repo path in the GLOBAL `REPO` (never via command substitution: a
#   `$(mkrepo …)` would run every `bad` inside a subshell and silently DISCARD the
#   failure count). Returns non-zero — loudly, with git's stderr — if setup failed, and
#   RE-MEASURES the line counts it just wrote, because a fixture that is not what the
#   case claims makes the case's verdict meaningless.
REPO=""
mkrepo() {
  local name="$1" rel="$2" nbase="$3" nhead="$4" branch="$5"
  local root="$tmp/$name" err="$tmp/$name.setup.err" n
  REPO=""
  if ! mkdir -p "$root/scripts" "$root/$(dirname "$rel")" 2>"$err"; then
    bad "fixture $name: mkdir failed — $(tr '\n' ' ' <"$err")"; return 1
  fi
  if ! cp "$GATE" "$root/scripts/agent-gate.sh" 2>"$err"; then
    bad "fixture $name: could not stage the gate script — $(tr '\n' ' ' <"$err")"; return 1
  fi
  printf 'target/\n*.log\n.agent-gate-summary.txt\n' >"$root/.gitignore"
  lines "$nbase" "$root/$rel"
  if ! ( cd "$root" && git "${GIT_CFG[@]}" init -q -b "$branch" . &&
         git "${GIT_CFG[@]}" add -A &&
         git "${GIT_CFG[@]}" commit -qm init ) >"$err" 2>&1; then
    bad "fixture $name: git setup FAILED — $(tr '\n' ' ' <"$err")"; return 1
  fi
  n=$( cd "$root" && git show "HEAD:$rel" 2>/dev/null | wc -l | tr -d ' ' )
  if [ "${n:-x}" != "$nbase" ]; then
    bad "fixture $name: committed $rel has ${n:-<unreadable>} lines, expected $nbase"; return 1
  fi
  if [ "$nhead" -gt 0 ]; then
    lines "$nhead" "$root/$rel"
    n=$(wc -l <"$root/$rel" 2>/dev/null | tr -d ' ')
    if [ "${n:-x}" != "$nhead" ]; then
      bad "fixture $name: worktree $rel has ${n:-<unreadable>} lines, expected $nhead"; return 1
    fi
  fi
  REPO="$root"
}

# run_only_file_size <repo> <outfile> [KEY=VAL …] -> exit status of the gate run
run_only_file_size() {
  local repo="$1" out="$2"; shift 2
  : >"$out"
  [ -n "$repo" ] || return 127
  ( cd "$repo" && env ${1+"$@"} AGENT_GATE_SUMMARY_FILE="$repo/.sum" \
      bash "$repo/scripts/agent-gate.sh" --only file-size >"$out" 2>&1 )
}

# logdir_of <gate-stdout-file> — the LOG_DIR the run published on its SUMMARY `logs:` line.
# Read from THERE rather than from an env var, so the test proves the route an agent
# actually has (AC2's premise). Anything other than an existing ABSOLUTE directory is a
# MEASUREMENT FAILURE, not a path: an empty value would leave the AC2 needle as the bare
# suffix `/file-size.log`, a substring of the real path, so the assert would MATCH having
# measured nothing. Emits an obviously-bogus absolute sentinel so every downstream assert
# fails closed with a readable diagnostic.
logdir_of() {
  local d
  d=$(sed -n 's/^logs:[[:space:]]*//p' "$1" 2>/dev/null | head -1)
  case "$d" in
    /*) if [ -d "$d" ]; then printf '%s\n' "$d"; return 0; fi ;;
  esac
  printf '%s\n' "$tmp/NO-LOGDIR-PUBLISHED"
  return 1
}

# verdict_of <logdir> — the component's own verdict, read from the UNCHANGED
# `file-size.result` (`STATUS SECONDS`). Deliberately NOT the gate's exit status: a
# passing `--only` run exits 3 (RESULT: PARTIAL) and a failing one exits 1, so an
# exit-code assert would conflate "the component failed" with "this was a partial run".
# Every variable is `local` and every non-measurement returns a SENTINEL: as globals,
# a failed `read` (missing dir/file — the redirect fails and `2>/dev/null` hides it, so
# `read` never runs) left the PREVIOUS case's verdict in place, and this is the sole
# per-case oracle, so a run that produced no result at all would have read as that
# earlier case's PASS.
verdict_of() {
  local st="" secs="" f="$1/file-size.result"
  [ -s "$f" ] || { printf '%s\n' '<no-result-file>'; return 1; }
  read -r st secs <"$f" || { printf '%s\n' '<unreadable-result-file>'; return 1; }
  printf '%s\n' "${st:-<empty-result-file>}"
}

# has <label> <file> <literal> — the workhorse content assert. An EMPTY needle is a
# refusal, never a match: `grep -Fq -- ""` matches every line, so an expected value that
# was never captured (an unchecked `$(git rev-parse …)`) would otherwise "pass".
has() {
  if [ -z "${3:-}" ]; then
    bad "$1 (EMPTY needle — the expected value was never captured)"; return
  fi
  if [ ! -f "$2" ]; then bad "$1 (no such file: $2)"; return; fi
  if [ ! -s "$2" ]; then bad "$1 (file is ZERO BYTES: $2)"; return; fi
  if grep -Fq -- "$3" "$2"; then ok "$1"; else bad "$1 (missing: '$3')"; fi
}

# has_re <label> <file> <ere> — the regex sibling of `has`, for shape assertions the
# `%-18s` padding makes awkward as a literal. Same fail-closed rules: an empty pattern
# matches every line, a missing/empty file is a measurement failure, never a match.
has_re() {
  if [ -z "${3:-}" ]; then
    bad "$1 (EMPTY pattern — the expected shape was never captured)"; return
  fi
  if [ ! -f "$2" ]; then bad "$1 (no such file: $2)"; return; fi
  if [ ! -s "$2" ]; then bad "$1 (file is ZERO BYTES: $2)"; return; fi
  if grep -Eq -- "$3" "$2"; then ok "$1"; else bad "$1 (no line matched: '$3')"; fi
}

# lacks <label> <file> <literal> — the NEGATIVE assert, and it is fail-closed the same way
# the positives are. A missing/empty file trivially "lacks" any needle, which is precisely
# the vacuous green this suite refuses: the absence of a token in a block that was never
# emitted proves nothing about the emitter (#3402).
lacks() {
  if [ -z "${3:-}" ]; then
    bad "$1 (EMPTY needle — nothing was actually being excluded)"; return
  fi
  if [ ! -f "$2" ]; then bad "$1 (no such file: $2 — absence UNMEASURED)"; return; fi
  if [ ! -s "$2" ]; then bad "$1 (file is ZERO BYTES: $2 — absence UNMEASURED)"; return; fi
  if grep -Fq -- "$3" "$2"; then bad "$1 (unexpectedly PRESENT: '$3')"; else ok "$1"; fi
}

# fs_summary_row <summary-file> <outfile> (#3402) — isolate the emitted block's
# `file-size:` component row so the SUMMARY-visibility asserts read the ROW, not the
# whole block. Reading the row matters: a needle found anywhere in the block (the
# component log path, a meta line) would not prove it landed on the line a reader of a
# pasted summary actually sees. A missing block or a missing row writes a SENTINEL, never
# an empty file, so every downstream `has`/`has_re` fails closed with a readable value.
fs_summary_row() {
  local out="$2"
  if [ ! -s "${1:-}" ]; then printf '%s\n' '<no-summary-file-emitted>' >"$out"; return 1; fi
  if ! grep -m1 -E '^file-size: ' "$1" >"$out" 2>/dev/null; then
    printf '%s\n' '<no-file-size-row-in-block>' >"$out"; return 1
  fi
  [ -s "$out" ] || { printf '%s\n' '<empty-file-size-row>' >"$out"; return 1; }
  return 0
}

# assert_log_present <label> <logfile> — existence AND non-emptiness, per AC3.
assert_log_present() {
  if [ -f "$2" ] && [ -s "$2" ]; then
    ok "$1"
  elif [ -f "$2" ]; then
    bad "$1 — file-size.log exists but is ZERO BYTES ($2)"
  else
    bad "$1 — no file-size.log written at all ($2)"
  fi
}

# assert_verdict <label> <logdir> <expected> — the per-case oracle, with the sentinel
# printed verbatim on failure so "no result file" never reads as a wrong verdict.
assert_verdict() {
  local got; got=$(verdict_of "$2")
  if [ "$got" = "$3" ]; then ok "$1"; else bad "$1 (component verdict was '$got', expected '$3')"; fi
}

# ---------------------------------------------------------------------------
# Case 1 — FAIL: an over-threshold source file GROWN by the diff (800-line src limit).
# ---------------------------------------------------------------------------
mkrepo grew cqlite-core/src/big.rs 900 950 main; r1="$REPO"
out1="$tmp/grew.out"
run_only_file_size "$r1" "$out1"
d1=$(logdir_of "$out1") ||
  bad "case1: the run published no usable 'logs:' dir — the component log cannot be located"
log1="$d1/file-size.log"
base1=$( cd "$r1" 2>/dev/null && git rev-parse HEAD 2>/dev/null )
[ -n "$base1" ] ||
  bad "case1: could not capture the fixture's base sha — the base-ref assert cannot measure anything"

assert_verdict "case1: --only file-size FAILs on a grown over-threshold file (the case being diagnosed)" \
    "$d1" FAIL
assert_log_present "case1: file-size.log written on the FAIL path" "$log1"
# The REAL numbers, not merely digits: 900 committed -> 950 in the worktree, src limit 800.
has "case1: log carries the exact growth entry (path + before -> after + limit)" \
    "$log1" "cqlite-core/src/big.rs: 900 -> 950 (limit 800)"
has "case1: log states the thresholds applied" \
    "$log1" "thresholds: src=800 test=1500"
has "case1: log states the metric (total lines, inline tests included)" \
    "$log1" "total lines, inline tests included"
has "case1: log names the resolved base sha used for the growth comparison" \
    "$log1" "$base1"
has "case1: log carries the over-threshold advisory entry (current/limit + path)" \
    "$log1" "950/800"
has "case1: log carries the terminal verdict line for the component" \
    "$log1" ">>> [file-size] FAIL"
has "case1: log carries the remedy (CQLITE_ALLOW_FILE_GROWTH acknowledgement)" \
    "$log1" "CQLITE_ALLOW_FILE_GROWTH=1"

# ---------------------------------------------------------------------------
# Case 6 (AC2) — the FAIL stdout must NAME the log path, so a reader who has only the
# SUMMARY's `logs: <dir>` line does not have to guess the filename.
# ---------------------------------------------------------------------------
has "case6/AC2: FAIL stdout names the component log path explicitly" "$out1" "$log1"

# ---------------------------------------------------------------------------
# Case 2 — PASS: over threshold but SHRUNK by the diff. The advisory list must still be
# logged, the ratchet must be empty, and the log must SAY it passed.
# ---------------------------------------------------------------------------
mkrepo shrank cqlite-core/src/big.rs 950 900 main; r2="$REPO"
out2="$tmp/shrank.out"
run_only_file_size "$r2" "$out2"
d2=$(logdir_of "$out2") || bad "case2: the run published no usable 'logs:' dir"
log2="$d2/file-size.log"

assert_verdict "case2: a shrunk over-threshold file PASSes the ratchet (fixture is a real PASS run)" \
    "$d2" PASS
assert_log_present "case2: file-size.log written on a PASS run too (AC3)" "$log2"
has "case2: PASS log still carries the over-threshold advisory entry" \
    "$log2" "900/800"
has "case2: PASS log carries the terminal PASS verdict" \
    "$log2" ">>> [file-size] PASS"
if [ ! -s "$log2" ]; then
  bad "case2: cannot check the ratchet emptiness — no readable log (absent or zero bytes)"
elif grep -Fq -- '-> ' "$log2"; then
  bad "case2: PASS log lists a growth entry for a file that SHRANK"
else
  ok "case2: PASS log lists no growth entry (ratchet genuinely empty)"
fi
# #3402 NEGATIVE CONTROL — the override UNSET. This is the run whose SUMMARY line must be
# UNCHANGED by #3402: a plain `file-size: PASS (Ns)` with no OPT-OUT token and no detail
# suffix anywhere in the block. Without it, "the token appears when the override is set"
# is only half a property — a component that stamped OPT-OUT unconditionally would satisfy
# case 4 and be a strictly worse defect than the one being fixed.
sumrow2="$tmp/shrank.sumrow"
fs_summary_row "$r2/.sum" "$sumrow2" ||
  bad "case2 (#3402): the run emitted no usable file-size row — the negative control is UNMEASURED"
has_re "case2 (#3402): with the override UNSET the SUMMARY row reads a plain PASS" \
    "$sumrow2" '^file-size: +PASS \([0-9]+s\)'
lacks "case2 (#3402): an unset override stamps NO OPT-OUT token anywhere in the block" \
    "$r2/.sum" "OPT-OUT"
lacks "case2 (#3402): an unset override stamps no CQLITE_ALLOW_FILE_GROWTH detail in the block" \
    "$r2/.sum" "CQLITE_ALLOW_FILE_GROWTH"

# ---------------------------------------------------------------------------
# Case 3 — PASS with NO changed .rs files at all: an empty-ish run still gets a log that
# SAYS SO (never an absent file, never a zero-byte one).
# ---------------------------------------------------------------------------
mkrepo clean cqlite-core/src/small.rs 20 0 main; r3="$REPO"
out3="$tmp/clean.out"
run_only_file_size "$r3" "$out3"
d3=$(logdir_of "$out3") || bad "case3: the run published no usable 'logs:' dir"
log3="$d3/file-size.log"

assert_verdict "case3: a clean tree PASSes file-size" "$d3" PASS
assert_log_present "case3: file-size.log written even with nothing to report (AC3)" "$log3"
has "case3: empty-ish log states that nothing is over threshold" \
    "$log3" "no changed .rs files over threshold"
has "case3: empty-ish log still states the thresholds" \
    "$log3" "thresholds: src=800 test=1500"
has "case3: empty-ish log still carries the terminal verdict" \
    "$log3" ">>> [file-size] PASS"

# ---------------------------------------------------------------------------
# Case 3b — the OTHER half of AC3's empty-ish path: .rs files ARE changed, none is over
# threshold. Same log line as case 3, materially different input (case 3's file list is
# empty; here it is non-empty and every entry is filtered by the limit).
# ---------------------------------------------------------------------------
mkrepo smallchange cqlite-core/src/small.rs 20 40 main; r3b="$REPO"
out3b="$tmp/smallchange.out"
run_only_file_size "$r3b" "$out3b"
d3b=$(logdir_of "$out3b") || bad "case3b: the run published no usable 'logs:' dir"
log3b="$d3b/file-size.log"

assert_verdict "case3b: a changed-but-small .rs file PASSes file-size" "$d3b" PASS
assert_log_present "case3b: file-size.log written when the diff has only sub-threshold .rs files (AC3)" \
    "$log3b"
has "case3b: log states that nothing is over threshold (non-empty file list, all filtered)" \
    "$log3b" "no changed .rs files over threshold"
has "case3b: log names the base ref it compared against" "$log3b" "base ref:"
has "case3b: log carries the terminal verdict" "$log3b" ">>> [file-size] PASS"

# ---------------------------------------------------------------------------
# Case 4 — the CQLITE_ALLOW_FILE_GROWTH=1 opt-out. The growth is ALLOWED, and the log must
# RECORD what was allowed (the numbers are the whole point of the acknowledgement).
#
# #3402 extends this case from the LOG to the SUMMARY BLOCK. The log was already complete;
# what was missing is that the SUMMARY — the unit of evidence agents paste into PRs —
# carried a bare `file-size: PASS (0s)`, byte-indistinguishable from a run where the
# ratchet was genuinely satisfied. So the component's own status TOKEN is now OPT-OUT, and
# the row must NAME the env var and the COUNT and POINT AT this log — deliberately NOT the
# file names, which are log-only (see the removed-cases note below for why rendering them on
# the row was tried and withdrawn). The asserts below check both directions: the disclosure
# is present, and no repository path rode onto the row with it.
# ---------------------------------------------------------------------------
mkrepo optout cqlite-core/src/big.rs 900 950 main; r4="$REPO"
out4="$tmp/optout.out"
run_only_file_size "$r4" "$out4" CQLITE_ALLOW_FILE_GROWTH=1
rc4=$?
d4=$(logdir_of "$out4") || bad "case4: the run published no usable 'logs:' dir"
log4="$d4/file-size.log"

assert_verdict "case4: CQLITE_ALLOW_FILE_GROWTH=1 turns the same growth into OPT-OUT, not PASS (#3402)" \
    "$d4" OPT-OUT
assert_log_present "case4: file-size.log written on the opt-out path (AC3)" "$log4"
has "case4: opt-out log records the acknowledgement" \
    "$log4" "ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1"
has "case4: opt-out log still records the exact growth entry" \
    "$log4" "cqlite-core/src/big.rs: 900 -> 950 (limit 800)"
has "case4: opt-out log carries the terminal OPT-OUT verdict" \
    "$log4" ">>> [file-size] OPT-OUT"

# --- case 4, #3402 half: the four facts a reader of a PASTED block needs ---------------
# Asserted against the ROW, not the block: a needle found anywhere in the block would not
# prove it landed on the line the reader actually sees.
sumrow4="$tmp/optout.sumrow"
fs_summary_row "$r4/.sum" "$sumrow4" ||
  bad "case4 (#3402): the run emitted no usable file-size row — the SUMMARY asserts are UNMEASURED"
has_re "case4 (#3402): the SUMMARY row carries the OPT-OUT status token, not PASS" \
    "$sumrow4" '^file-size: +OPT-OUT \([0-9]+s\)'
has "case4 (#3402): the SUMMARY row NAMES the env var that was engaged" \
    "$sumrow4" "CQLITE_ALLOW_FILE_GROWTH=1"
has "case4 (#3402): the SUMMARY row gives the COUNT of grown files" \
    "$sumrow4" "1 over-threshold file(s) grown"
# The row deliberately carries NO repository content (see the REMOVED-cases note below).
# What it must carry instead is a pointer to where the names DO live, plus — asserted from
# the other side — proof that no path leaked onto it.
has "case4 (#3402): the SUMMARY row POINTS AT the component log for the file list" \
    "$sumrow4" "see file-size.log under logs:"
lacks "case4 (#3402): the row renders NO repository path — the whole mangling family is unreachable" \
    "$sumrow4" "cqlite-core/src/big.rs"
has "case4 (#3402): the row keeps its feature-matrix annotation ahead of the detail" \
    "$sumrow4" "[no-cargo]"
lacks "case4 (#3402): the row does not ALSO read PASS — a reader greps one token, not two" \
    "$sumrow4" "PASS"
# NON-FAILING, measured through the gate's own verdict rather than asserted of the source.
# In `--only` mode a passing run is promoted to RESULT: PARTIAL and exits 3, while any
# component FAIL leaves RESULT: FAIL and exit 1 — so this pair distinguishes exactly the
# property #3402 must not break: a legitimate OPT-OUT does not fail the run.
if [ "$rc4" = 3 ]; then
  ok "case4 (#3402): a legitimate OPT-OUT is NON-FAILING (--only run exits 3, the pass code, not 1)"
else
  bad "case4 (#3402): expected the --only pass exit 3, got $rc4 — OPT-OUT failed the run"
fi
has "case4 (#3402): the block's RESULT is the passing PARTIAL, not FAIL" \
    "$r4/.sum" "RESULT: PARTIAL"

# ---------------------------------------------------------------------------
# REMOVED, deliberately: cases 4b (elision past three files), 4e (a path containing `: `)
# and 4g (a path containing the completion probe's verdict token). All three exercised the
# INLINE GROWN-PATH LIST, which is gone.
#
# Why the list went, recorded here because a future reader will otherwise re-add it: it was
# the OPTIONAL half of #3402 ("ideally naming the files", with the issue itself deferring
# that data to the sibling log issue #3401, now merged), and it produced THREE of this PR's
# seven review findings — one per round, each a different way of mangling a filename: a
# `: ` split when recovering a path from a display string; substitution inside a path
# carrying `RESULT:`; and `,` joining, which made `src/a.rs,b.rs` indistinguishable from two
# files. Every fix was correct and the next round found another. That is the shape #3229
# already ruled on — remove the mechanism rather than carve it a fourth time — and escaping
# would only move the argument to the escape grammar (#3312: a rarer delimiter is still
# forgeable).
#
# The row now carries env var + COUNT + a pointer to file-size.log, which is what the issue
# actually specified. The names live in the log (#3401's whole subject) and, for a PR
# reviewer, in the diff itself — the grown files are the files the PR changed.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Case 4h (#3402, roborev job 25) — the detail must carry NOTHING from the environment.
# `$LOG_DIR` is `mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX"`, so interpolating any path
# under it put CALLER-CONTROLLED text into a field whose contract says gate-authored — and a
# TMPDIR containing the completion probe's verdict token then tripped the withholding guard,
# taking the override name and the growth count with it. Exactly the disclosure this issue
# exists to produce, destroyed by an environment variable.
#
# The fixture IS the hostile TMPDIR: a directory literally named `RESULT: PASS`, which is a
# legal directory name and reaches LOG_DIR by the ordinary route. The needles assert the
# disclosure SURVIVES it, and that no part of the path rode onto the row.
# ---------------------------------------------------------------------------
hostile_tmp="$tmp/RESULT: PASS"
if ! mkdir -p "$hostile_tmp" 2>/dev/null; then
  bad "case4h: could not create the hostile TMPDIR fixture — the environment route is UNMEASURED"
  bad "case4h: (env-var needle not reached)"
  bad "case4h: (count needle not reached)"
  bad "case4h: (forged-verdict needle not reached)"
else
  mkrepo hostiletmp cqlite-core/src/big.rs 900 950 main; r4h="$REPO"
  out4h="$tmp/hostiletmp.out"
  run_only_file_size "$r4h" "$out4h" CQLITE_ALLOW_FILE_GROWTH=1 TMPDIR="$hostile_tmp"
  sumrow4h="$tmp/hostiletmp.sumrow"
  fs_summary_row "$r4h/.sum" "$sumrow4h" ||
    bad "case4h (#3402): the run emitted no usable file-size row — the TMPDIR asserts are UNMEASURED"
  has "case4h (#3402): a hostile TMPDIR does NOT cost the override name" \
      "$sumrow4h" "CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced)"
  has "case4h (#3402): a hostile TMPDIR does NOT cost the growth count" \
      "$sumrow4h" "1 over-threshold file(s) grown"
  if [ ! -s "$sumrow4h" ]; then
    bad "case4h (#3402): no file-size row captured — the forged-verdict check could not run"
  elif grep -Eq 'RESULT: (PASS|FAIL)' "$sumrow4h"; then
    bad "case4h (#3402): the TMPDIR's verdict token reached the row — it would forge a verdict"
  else
    ok "case4h (#3402): no part of the TMPDIR reached the row — the pointer composes, it does not carry"
  fi
fi

# ---------------------------------------------------------------------------
# Case 4c (#3402), run for BOTH malformed spellings — THE FALSE-PASS ROUTE, and the reason the emit is keyed on the
# AFFIRMATIVE `= 1`. `CQLITE_ALLOW_FILE_GROWTH` has THREE states, not two: exactly `1`
# (engaged), SET BUT NOT 1 (`0`, `true`, `yes` — a typo, and already reported as "this IS
# a ratchet violation"), and unset. A permissive branch keyed on `!= <bad>` would let the
# middle state buy an OPT-OUT, i.e. a mis-spelled override silently waiving the ratchet —
# strictly worse than the invisible opt-out this issue closes. Both spellings of the
# middle state are exercised on the SAME grown fixture as case 4, so the only difference
# between a FAIL here and an OPT-OUT there is the value of the variable.
# ---------------------------------------------------------------------------
for bad_val in 0 true; do
  mkrepo "badval$bad_val" cqlite-core/src/big.rs 900 950 main; rbv="$REPO"
  outbv="$tmp/badval$bad_val.out"
  run_only_file_size "$rbv" "$outbv" "CQLITE_ALLOW_FILE_GROWTH=$bad_val"
  rcbv=$?
  dbv=$(logdir_of "$outbv") || bad "case4c/$bad_val: the run published no usable 'logs:' dir"
  assert_verdict "case4c/$bad_val (#3402): a malformed override keeps the ratchet VIOLATION (FAIL)" \
      "$dbv" FAIL
  has "case4c/$bad_val (#3402): the log carries the unchanged ratchet-violation wording" \
      "$dbv/file-size.log" "FAIL: change makes over-threshold file(s) larger."
  lacks "case4c/$bad_val (#3402): NO OPT-OUT token anywhere in the emitted block" \
      "$rbv/.sum" "OPT-OUT"
  sumrowbv="$tmp/badval$bad_val.sumrow"
  fs_summary_row "$rbv/.sum" "$sumrowbv" ||
    bad "case4c/$bad_val (#3402): no usable file-size row — the malformed-value assert is UNMEASURED"
  has_re "case4c/$bad_val (#3402): the SUMMARY row reads FAIL" \
      "$sumrowbv" '^file-size: +FAIL \([0-9]+s\)'
  if [ "$rcbv" = 1 ]; then
    ok "case4c/$bad_val (#3402): the run FAILS (exit 1), so a typo can never buy a green"
  else
    bad "case4c/$bad_val (#3402): expected exit 1, got $rcbv — a malformed override did not fail the run"
  fi
done

# ---------------------------------------------------------------------------
# Case 4i (#3402, roborev job 26) — the disclosure must survive an EARLY-EXIT emit.
#
# `run_file_size` executes ~250 lines BEFORE the dataset and schemas preflights, and each
# early-exit `emit_summary` hand-builds its own meta list. So a full gate with an engaged
# override and a missing corpus recorded `file-size: OPT-OUT` and then emitted its ONLY
# block with NO component row at all — the override name and the growth count absent from
# the very artifact this issue exists to put them in. Round 1 found the same shape in the
# tree-integrity boundary block; this is the preflight emit.
#
# This is the suite's one REAL FULL-GATE run. It compiles nothing: the preflight fails before
# any component is dispatched, which is exactly the window under test. It needs the #3544
# pre-flight to pass, hence the pinned canonical remote + committed manifest and a local bare
# origin.
# ---------------------------------------------------------------------------
pf_root="$tmp/preflight-fixture"
pf_empty="$tmp/preflight-empty-root/sstables"
pf_ok=1
mkdir -p "$pf_root/scripts" "$pf_root/cqlite-core/src" "$pf_empty" 2>/dev/null || pf_ok=0
if [ "$pf_ok" = 1 ]; then
  cp "$GATE" "$pf_root/scripts/agent-gate.sh" || pf_ok=0
fi
if [ "$pf_ok" = 1 ]; then
  agent_gate_pin_canonical_remote "$pf_root/scripts/agent-gate.sh" "$pf_root.origin.git" || pf_ok=0
  agent_gate_install_components_manifest "$pf_root/scripts/agent-gate.sh" || pf_ok=0
fi
if [ "$pf_ok" = 1 ]; then
  lines 900 "$pf_root/cqlite-core/src/big.rs"
  printf 'target/\n*.log\n' > "$pf_root/.gitignore"
  ( cd "$pf_root" && git "${GIT_CFG[@]}" init -q -b main . &&
    git "${GIT_CFG[@]}" add -A && git "${GIT_CFG[@]}" commit -qm init &&
    git init -q --bare "$pf_root.origin.git" &&
    git "${GIT_CFG[@]}" remote add origin "$pf_root.origin.git" &&
    git "${GIT_CFG[@]}" push -q origin main ) >/dev/null 2>&1 || pf_ok=0
fi
if [ "$pf_ok" != 1 ]; then
  # NOT a skip: this suite needs git and a writable scratch, both of which every other case
  # already relies on, so a failure here is a broken harness rather than a missing capability.
  bad "case4i: could not build the full-gate preflight fixture — the early-exit path is UNMEASURED"
  bad "case4i: (OPT-OUT row needle not reached)"
  bad "case4i: (count-agreement needle not reached)"
  bad "case4i mutant: (not reached)"
else
  lines 950 "$pf_root/cqlite-core/src/big.rs"
  pf_sum="$tmp/preflight.sum"; pf_out="$tmp/preflight.out"
  ( cd "$pf_root" && env -u AGENT_GATE_SUMMARY_FILE CQLITE_DATASETS_ROOT="${pf_empty%/sstables}" \
      CQLITE_ALLOW_FILE_GROWTH=1 CQLITE_GATE_DISABLE_CAP=1 AGENT_GATE_SUMMARY_FILE="$pf_sum" \
      bash "$pf_root/scripts/agent-gate.sh" >"$pf_out" 2>&1 )
  # POSITIVE CONTROL: the run must actually have stopped at the corpus preflight. Without
  # this, an assert below could be measuring a run that never got there.
  if grep -q 'missing-fixtures: FAIL-CLOSED' "$pf_sum" 2>/dev/null; then
    ok "case4i: the fixture run really stopped at the #2078 corpus preflight (window under test)"
  else
    bad "case4i: the run did not reach the corpus preflight — the early-exit path is UNMEASURED"
    grep -E '^(preflight|component-set|RESULT):' "$pf_sum" 2>/dev/null | head -3
  fi
  # The annotation is REQUIRED, not incidental (roborev job 76). #3453's contract is that
  # EVERY component line names the feature matrix it ran; a funnel-appended row that omitted
  # it would satisfy "the disclosure is present" while losing the execution evidence beside
  # it — and the first version of this assert PINNED that incomplete shape, which is how a
  # test stops being a check and starts being a ratchet on a defect.
  has "case4i (#3402): the preflight-FAIL block carries the OPT-OUT row with its detail" \
      "$pf_sum" "OPT-OUT (0s)  [no-cargo] — CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced); 1 over-threshold file(s) grown"
  # The count must AGREE with the rows printed. It did not: the helper assigns its count
  # inside a command substitution, so the caller read 0 beside one row — a count
  # contradicting its own table, which is the invariant
  # scripts/tests/test_agent_gate_tree_provenance.sh asserts for the boundary block.
  pf_rows=$(grep -cE '^[a-z][a-z0-9-]*: +(PASS|FAIL|SKIP|OPT-OUT) \([0-9]+s\)' "$pf_sum" 2>/dev/null | tr -d ' ')
  pf_said=$(sed -n 's/^components-recorded: \([0-9]*\) .*/\1/p' "$pf_sum" | head -1)
  if [ -n "$pf_said" ] && [ "$pf_said" = "${pf_rows:-x}" ]; then
    ok "case4i (#3402): components-recorded ($pf_said) equals the rows printed — no contradicted count"
  else
    bad "case4i (#3402): the block says '$pf_said' completed but printed ${pf_rows:-<unmeasured>} row(s)"
  fi
  # The mutant: stop the EMIT FUNNEL appending the table, which is the state before this fix.
  # The block still emits, so only the row can distinguish the two.
  #
  # WRITE-AND-MOVE, never `sed -i` (roborev job 74): `sed -i EXPR FILE` is GNU-only — BSD and
  # macOS sed require an argument to -i (`sed -i '' EXPR FILE`), so the GNU spelling makes
  # this REGISTERED gate test fail on a platform the repo supports, and
  # scripts/tests/test_agent_gate_tree_portability.sh lints for exactly this shape. The
  # temp-file form needs no version sniff and is unambiguous on both.
  #
  # THE GUARD ASSERTS THE MUTATION CHANGED SOMETHING, not that a pattern is now absent. The
  # first version sed'd `${_pf_rows:+…}` — the per-site wiring this fix REPLACED — and then
  # checked that pattern was gone, which is trivially true of a file that never contained it.
  # So it built an identical copy, "proved" nothing, and reported the row surviving as a
  # failure of the check rather than of the mutant. A mutant that cannot be shown to differ
  # from the original is a vacuous control, and `cmp` is the only thing that shows it.
  pf_mut="$tmp/preflight-mutant"
  if cp -r "$pf_root" "$pf_mut" 2>/dev/null &&
     grep -q 'line=$(_recorded_component_rows_block)' "$pf_mut/scripts/agent-gate.sh" &&
     sed 's/^\( *\)line=$(_recorded_component_rows_block)$/\1line=""/' \
         "$pf_mut/scripts/agent-gate.sh" > "$pf_mut/scripts/agent-gate.sh.mut" &&
     mv "$pf_mut/scripts/agent-gate.sh.mut" "$pf_mut/scripts/agent-gate.sh" &&
     ! cmp -s "$pf_root/scripts/agent-gate.sh" "$pf_mut/scripts/agent-gate.sh"; then
    pf_msum="$tmp/preflight-mutant.sum"
    ( cd "$pf_mut" && env -u AGENT_GATE_SUMMARY_FILE CQLITE_DATASETS_ROOT="${pf_empty%/sstables}" \
        CQLITE_ALLOW_FILE_GROWTH=1 CQLITE_GATE_DISABLE_CAP=1 AGENT_GATE_SUMMARY_FILE="$pf_msum" \
        bash "$pf_mut/scripts/agent-gate.sh" >"$tmp/preflight-mutant.out" 2>&1 )
    if grep -q 'CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced)' "$pf_msum" 2>/dev/null; then
      bad "case4i mutant: the row survives without the rows argument — the check cannot fail"
    else
      ok "case4i mutant: without the rows argument the override is INVISIBLE in the block (proved discriminating)"
    fi
  else
    bad "case4i mutant: could not build the mutant fixture — the assert above is unproven"
  fi
fi

# ---------------------------------------------------------------------------
# Case 5 — base ref UNRESOLVABLE (no main/master, no origin/*): the ratchet is skipped and
# the log must say so EXPLICITLY, while the advisory list still works off `git diff HEAD`.
# ---------------------------------------------------------------------------
mkrepo nobase cqlite-core/src/big.rs 900 950 work; r5="$REPO"
# Affirmative precondition WITH A POSITIVE CONTROL (#3401 review blocker A). As a PURE
# NEGATIVE (`! ( cd … && for … )`) this printed `ok` for ANY reason the subshell exited
# non-zero: a missing or unreadable $r5 makes `cd` short-circuit the `&&`, ZERO rev-parse
# calls run, and "I could not even look" reads as "I looked and found none" — the same
# fail-open shape the round-1 sweep removed elsewhere. The probe now reports three
# distinct states, and only the one that PROVES it looked can pass.
if [ -z "$r5" ]; then
  case5_probe=2
else
  ( cd "$r5" 2>/dev/null || exit 2
    # positive control: we are in a usable git repo, so a "no ref resolved" answer below
    # is a measurement rather than an inability to measure.
    git rev-parse --verify -q HEAD >/dev/null 2>&1 || exit 2
    for ref in origin/main main origin/master master; do
      git rev-parse --verify -q "$ref" >/dev/null 2>&1 && exit 0
    done
    exit 1 )
  case5_probe=$?
fi
case "$case5_probe" in
  1) ok "case5: probe RAN and resolved none of origin/main|main|origin/master|master" ;;
  0) bad "case5: fixture DOES resolve a base ref — the no-base path is not being exercised" ;;
  *) bad "case5: could not probe the fixture's refs at all (unusable repo at '$r5') — precondition UNMEASURED" ;;
esac
out5="$tmp/nobase.out"
run_only_file_size "$r5" "$out5"
d5=$(logdir_of "$out5") || bad "case5: the run published no usable 'logs:' dir"
log5="$d5/file-size.log"

assert_verdict "case5: with no resolvable base ref the component is advisory-only and PASSes" "$d5" PASS
assert_log_present "case5: file-size.log written on the no-base path (AC3)" "$log5"
has "case5: no-base log states the ratchet was skipped, in those words" \
    "$log5" "base ref unavailable — growth ratchet skipped (advisory only)"
has "case5: no-base log still carries the advisory over-threshold entry" \
    "$log5" "cqlite-core/src/big.rs"
has "case5: no-base log carries the terminal verdict" \
    "$log5" ">>> [file-size] PASS"

# ---------------------------------------------------------------------------
# Cases 7/8/9 — LOG PERSISTENCE (#3401 review blockers A/B/C). The component now PROMISES
# a log, so an unverified promise is the original defect one level up: an agent follows the
# SUMMARY's `logs:` pointer, finds nothing, and is back to hand-computing line counts.
#
# Sabotage is surgical, via a PATH-shim `mktemp` that lets the gate create its LOG_DIR
# normally and then plants something unwritable at `file-size.log` inside it, so every
# OTHER LOG_DIR write (tree-identity capture, `.result`, the summary, and — the point of
# blocker B — the persistence-error sibling) still works. Two shapes, selected by
# FS_SABOTAGE:
#   dir      — a DIRECTORY: `: >"$log"` fails, so the TRUNCATE check fires. Uid-independent
#              (`: > <dir>` fails for root too), unlike `chmod 500 $LOG_DIR`, which is a
#              no-op for root — the case would silently self-skip in containers — and which
#              would also break the gate's own tree-identity capture, failing the run for a
#              reason other than the property under test.
#   devfull  — a SYMLINK TO /dev/full: the truncate SUCCEEDS and every append is rejected
#              with ENOSPC, so the APPEND-FAILURE COUNTER fires and names its own count.
#              Also uid-independent.
#   sibfull  — `dir` PLUS a /dev/full symlink at the SIBLING path: the persistence
#              diagnostic's own destination accepts the open and rejects every write, which
#              is the only shape that can make the "also written to" claim FALSE while the
#              sibling is openable. Also uid-independent.
#
# NOT reproducible here, stated rather than quietly omitted: the pure mid-sequence partial
# write (first append accepted, later ones rejected, leaving a NON-EMPTY but truncated
# log). Four techniques were considered. A quota/ENOSPC boundary hit mid-write and an
# LD_PRELOAD/FUSE fault injector are unavailable to a hermetic shell self-test; a test-only
# seam in the gate is rejected on principle (one more thing a real invoker can set). The
# fourth — `trap "" XFSZ` (SIG_IGN survives exec) plus `ulimit -f 1` — DOES work without
# root and IS named here so nobody re-derives it: it is rejected because the limit is
# PROCESS-WIDE, so it would equally truncate the SUMMARY and `.result` writes and break the
# run for reasons unrelated to the property under test. `devfull` covers the counter's
# trigger (an append the filesystem rejected) and the counter is by construction
# indifferent to WHICH append failed, so the untested residual is only "some appends
# succeeded first".
# ---------------------------------------------------------------------------
STUBBIN="$tmp/stubbin"
mkdir -p "$STUBBIN"
REAL_MKTEMP=$(command -v mktemp 2>/dev/null)
if [ -z "$REAL_MKTEMP" ]; then
  bad "case7/8/9: no mktemp on PATH — the persistence paths CANNOT be exercised (not a skip: this suite needs it)"
else
  cat >"$STUBBIN/mktemp" <<STUB
#!/usr/bin/env bash
d=\$("$REAL_MKTEMP" "\$@") || exit 1
case "\$d" in
  */agent-gate.*)
    if [ -d "\$d" ]; then
      case "\${FS_SABOTAGE:-}" in
        dir)     mkdir -p "\$d/file-size.log" ;;
        detaildir) mkdir -p "\$d/file-size.status-detail" ;;
        devfull) ln -s /dev/full "\$d/file-size.log" ;;
        sibfull) mkdir -p "\$d/file-size.log"
                 ln -s /dev/full "\$d/file-size.persistence-error.log" ;;
      esac
    fi
    ;;
esac
printf '%s\n' "\$d"
STUB
  chmod +x "$STUBBIN/mktemp"

  # -------------------------------------------------------------------------
  # Case 7 — unwritable log, CLEAN tree. Ratchet verdict is PASS, so the component's FAIL
  # can only have come from persistence.
  # -------------------------------------------------------------------------
  mkrepo persist cqlite-core/src/small.rs 20 0 main; r7="$REPO"
  out7="$tmp/persist.out"
  run_only_file_size "$r7" "$out7" PATH="$STUBBIN:$PATH" FS_SABOTAGE=dir
  d7=$(logdir_of "$out7") || bad "case7: the run published no usable 'logs:' dir"
  sib7="$d7/file-size.persistence-error.log"

  # POSITIVE CONTROL — without it a FAIL below could have come from anywhere.
  if [ -d "$d7/file-size.log" ]; then
    ok "case7: sabotage in place (file-size.log is a directory, so the component cannot write it)"
  else
    bad "case7: sabotage did NOT take effect at '$d7/file-size.log' — the persistence path was never exercised"
  fi
  assert_verdict "case7: an unpersistable log makes the component FAIL (never a silent PASS)" "$d7" FAIL
  has "case7: stdout names the failure as PERSISTENCE, not a ratchet violation" \
      "$out7" "LOG PERSISTENCE FAILURE"
  # The needle is bound to the DIAGNOSTIC'S OWN WORDING (#3401 review blocker D). A bare
  # "$d7/file-size.log" was satisfiable by bash's own `…: Is a directory` stderr, which
  # `2>&1` folds into $out7 — measured: the assert survived deleting the line that prints
  # it. "points at: " exists in no shell error message.
  has "case7: stdout names the unwritable path IN the diagnostic (not in a shell error)" \
      "$out7" "points at: $d7/file-size.log"
  has "case7: stdout states the ratchet's OWN verdict in the same breath" \
      "$out7" "The size ratchet itself computed: PASS"
  # Blocker B: the diagnostic must be reachable from `logs:`, not only from gate.log.
  assert_log_present "case7: the persistence diagnostic is ALSO written to a LOG_DIR sibling" "$sib7"
  has "case7: the sibling carries the diagnostic itself" "$sib7" "LOG PERSISTENCE FAILURE"
  has "case7: the sibling names the log that could not be written" \
      "$sib7" "points at: $d7/file-size.log"
  has "case7: stdout names the sibling, so both routes lead to it" \
      "$out7" "also written to: $sib7"
  if [ ! -s "$out7" ]; then
    bad "case7: no gate stdout captured — the wording check could not run"
  elif grep -Fq -- "change makes over-threshold file(s) larger" "$out7"; then
    bad "case7: a persistence failure was reported with campsite-rule/ratchet wording"
  else
    ok "case7: the persistence FAIL carries no campsite-rule/ratchet wording"
  fi

  # -------------------------------------------------------------------------
  # Case 8 — the APPEND-FAILURE COUNTER (blocker A1). /dev/full accepts the truncate and
  # rejects every write, so the truncate check CANNOT fire and the counter must. The
  # discriminator is the CAUSE TEXT: with only the `[ -s ]` end-state check the message
  # would read "absent or empty after writing"; the exact rejected-write COUNT can only
  # come from the counter having incremented per failed append.
  # -------------------------------------------------------------------------
  if [ ! -c /dev/full ]; then
    # ONE skip PER SKIPPED ASSERT (#3401 review blocker 1), the
    # scripts/tests/test_perf_capability.sh precedent: a single skip standing for six
    # asserts left PASS+FAIL+SKIP five short on any host without /dev/full (macOS/BSD),
    # so the census fired `assertion census mismatch` and hard-failed the suite — a
    # DELETED-ASSERTION accusation on a host where nothing was deleted. Keeping the count
    # per-assert keeps ONE invariant instead of two totals that can drift apart.
    skip "case8: no /dev/full (macOS/BSD) — sabotage-in-place control not run"
    skip "case8: no /dev/full (macOS/BSD) — truncate-ok/append-rejected control not run"
    skip "case8: no /dev/full (macOS/BSD) — FAIL verdict not asserted"
    skip "case8: no /dev/full (macOS/BSD) — exact rejected-write count not asserted"
    skip "case8: no /dev/full (macOS/BSD) — sibling presence not asserted"
    skip "case8: no /dev/full (macOS/BSD) — sibling rejected-write cause not asserted"
  else
    mkrepo appendfail cqlite-core/src/small.rs 20 0 main; r8="$REPO"
    out8="$tmp/appendfail.out"
    run_only_file_size "$r8" "$out8" PATH="$STUBBIN:$PATH" FS_SABOTAGE=devfull
    d8=$(logdir_of "$out8") || bad "case8: the run published no usable 'logs:' dir"
    sib8="$d8/file-size.persistence-error.log"

    # POSITIVE CONTROL, in two halves: the sabotage is in place AND it has the semantics
    # the case depends on (truncate succeeds, append rejected). Without the second half a
    # FAIL could be the truncate check firing, which is case 7's property, not this one.
    if [ -L "$d8/file-size.log" ] && [ "$(readlink "$d8/file-size.log")" = /dev/full ]; then
      ok "case8: sabotage in place (file-size.log -> /dev/full)"
    else
      bad "case8: sabotage did NOT take effect at '$d8/file-size.log' — the append-failure path was never exercised"
    fi
    if ( : >"$d8/file-size.log" ) 2>/dev/null && ! ( printf 'x\n' >>"$d8/file-size.log" ) 2>/dev/null; then
      ok "case8: control — the truncate SUCCEEDS and the append is REJECTED (so only the counter can fire)"
    else
      bad "case8: /dev/full did not behave as required (truncate-ok + append-rejected) — the case proves nothing"
    fi
    assert_verdict "case8: rejected appends make the component FAIL" "$d8" FAIL
    # 3 emitted lines on a clean tree, and the ENUMERATION is the point — this stays a
    # derived expectation, never a magic constant:
    #   1. thresholds
    #   2. base ref
    #   3. "no changed .rs files over threshold"
    # It went 3 -> 4 while #3162's `emitted` census added a contract line here, and back to 3
    # when that was reverted; BOTH moves were caught by this case, which is the property:
    # the EXACT count is the oracle — a fabricated or hardcoded flag cannot produce it, and
    # the `[ -s ]` check cannot produce a count at all. Adding or removing an _fs_emit call
    # in run_file_size MUST move this number, and the enumeration above says which line is
    # which so the next person can tell a real regression from an intended emit.
    has "case8: the cause names the EXACT number of rejected writes (the counter, not the -s check)" \
        "$out8" "3 write(s) to it were rejected"
    assert_log_present "case8: the persistence diagnostic sibling is written here too" "$sib8"
    has "case8: the sibling carries the rejected-write cause" \
        "$sib8" "write(s) to it were rejected"
  fi

  # -------------------------------------------------------------------------
  # Case 9 — BOTH failures at once (blocker C). A real ratchet violation PLUS an
  # unwritable log. The old unconditional wording claimed "this is NOT a campsite-rule
  # violation" while the ratchet had genuinely failed, steering the reader away from a
  # real growth violation — and the grown-file list died with the unwritable log.
  # -------------------------------------------------------------------------
  mkrepo bothfail cqlite-core/src/big.rs 900 950 main; r9="$REPO"
  out9="$tmp/bothfail.out"
  run_only_file_size "$r9" "$out9" PATH="$STUBBIN:$PATH" FS_SABOTAGE=dir
  d9=$(logdir_of "$out9") || bad "case9: the run published no usable 'logs:' dir"
  sib9="$d9/file-size.persistence-error.log"

  if [ -d "$d9/file-size.log" ]; then
    ok "case9: sabotage in place alongside a genuine growth violation"
  else
    bad "case9: sabotage did NOT take effect at '$d9/file-size.log' — the combined path was never exercised"
  fi
  assert_verdict "case9: growth + unpersistable log is a FAIL" "$d9" FAIL
  has "case9: the diagnostic reports BOTH failures" "$out9" "TWO failures"
  # The remediation data would otherwise be lost with the log — it must survive in the
  # sibling, with the REAL numbers.
  has "case9: the sibling preserves the exact grown-file entry" \
      "$sib9" "cqlite-core/src/big.rs: 900 -> 950 (limit 800)"
  has "case9: the sibling preserves the acknowledgement remedy" \
      "$sib9" "CQLITE_ALLOW_FILE_GROWTH=1"
  # The lie: a real ratchet violation must never be disclaimed.
  if [ ! -s "$out9" ]; then
    bad "case9: no gate stdout captured — the disclaimer check could not run"
  elif grep -Fq -- "this is NOT a campsite-rule" "$out9"; then
    bad "case9: a REAL ratchet violation was disclaimed as 'NOT a campsite-rule violation'"
  else
    ok "case9: the combined failure does NOT disclaim the real ratchet violation"
  fi
  # #3402: the same non-disclaimer, on the SUMMARY ROW. stdout is gate.log, which agents are
  # told never to read — the row is the artifact a reader actually sees, so the property has
  # to hold there too or it holds only where nobody looks.
  sumrow9="$tmp/bothfail.sumrow"
  fs_summary_row "$r9/.sum" "$sumrow9" ||
    bad "case9 (#3402): the run emitted no usable file-size row — the SUMMARY asserts are UNMEASURED"
  has "case9 (#3402): the row reports BOTH failures, not persistence alone" \
      "$sumrow9" "TWO failures: a REAL size-ratchet violation AND a log-persistence failure"
  lacks "case9 (#3402): the row does NOT disclaim the real ratchet violation" \
      "$sumrow9" "not a ratchet violation"

  # -------------------------------------------------------------------------
  # Case 10 — persistence failure on a NO-BASE run (#3401 review L1). With no resolvable
  # base ref the ratchet is SKIPPED, so a diagnostic claiming it "computed PASS" asserts a
  # computation that never happened. The `SKIPPED (base ref unavailable)` phrase exists
  # only inside the persistence block, so it cannot be satisfied by any other output.
  # -------------------------------------------------------------------------
  mkrepo nobasepersist cqlite-core/src/big.rs 900 950 work; r10="$REPO"
  out10="$tmp/nobasepersist.out"
  run_only_file_size "$r10" "$out10" PATH="$STUBBIN:$PATH" FS_SABOTAGE=dir
  d10=$(logdir_of "$out10") || bad "case10: the run published no usable 'logs:' dir"

  assert_verdict "case10: unpersistable log on a no-base run is a FAIL" "$d10" FAIL
  has "case10: the diagnostic says the ratchet was SKIPPED, not that it computed a verdict" \
      "$out10" "The size ratchet was SKIPPED (base ref unavailable)"
  # `grown: none` on a run that never compared anything asserts a COMPLETED comparison and
  # can conceal real growth (#3401 review item 2). This fixture DID grow its file 900->950,
  # so a "none" here would be an actively false statement; only the not-computed wording
  # can be true, and it is emitted from the no-base branch alone.
  has "case10: the sibling reports growth as NOT COMPUTED, never as 'none'" \
      "$d10/file-size.persistence-error.log" "grown: not computed (base unavailable)"
  if [ ! -s "$out10" ]; then
    bad "case10: no gate stdout captured — the false-computation check could not run"
  elif grep -Fq -- "The size ratchet itself computed:" "$out10"; then
    bad "case10: claims a ratchet verdict was computed on a run where the ratchet was skipped"
  else
    ok "case10: claims no ratchet verdict for a run that never computed one"
  fi
  # #3402: and the ROW must not claim it either. `ratchet_verdict` is PASS on this path
  # (nothing ever set it otherwise), so an arm that simply interpolated it would assert a
  # comparison that never ran — #3401 review L1, one artifact over.
  sumrow10="$tmp/nobasepersist.sumrow"
  fs_summary_row "$r10/.sum" "$sumrow10" ||
    bad "case10 (#3402): the run emitted no usable file-size row — the SUMMARY asserts are UNMEASURED"
  has "case10 (#3402): the row says the ratchet was SKIPPED, so nothing was compared" \
      "$sumrow10" "the ratchet was SKIPPED (base ref unavailable), so nothing was compared"
  lacks "case10 (#3402): the row claims NO computed ratchet verdict" \
      "$sumrow10" "the ratchet itself computed"

  # -------------------------------------------------------------------------
  # Case 11 — the sibling must carry the SAME arithmetic in EVERY ratchet state (#3401
  # review FIX 1). Concrete regression: CQLITE_ALLOW_FILE_GROWTH=1 (ratchet ALLOWS the
  # growth, verdict is not FAIL) plus an unwritable log — the file names and counts used to
  # be omitted from the sibling, i.e. lost from every reachable artifact, because stdout
  # only reaches gate.log. Every needle below is a REAL computed value (the fixture's own
  # 900 -> 950, its src limit, its resolved base sha, its current/limit advisory), none of
  # which any wording or shell message can produce.
  # -------------------------------------------------------------------------
  mkrepo optoutpersist cqlite-core/src/big.rs 900 950 main; r11="$REPO"
  out11="$tmp/optoutpersist.out"
  run_only_file_size "$r11" "$out11" PATH="$STUBBIN:$PATH" FS_SABOTAGE=dir \
      CQLITE_ALLOW_FILE_GROWTH=1
  d11=$(logdir_of "$out11") || bad "case11: the run published no usable 'logs:' dir"
  sib11="$d11/file-size.persistence-error.log"
  base11=$( cd "$r11" 2>/dev/null && git rev-parse HEAD 2>/dev/null )
  [ -n "$base11" ] ||
    bad "case11: could not capture the fixture's base sha — the base-ref assert cannot measure anything"

  assert_verdict "case11: unpersistable log on the ALLOWED-growth path is still a FAIL" "$d11" FAIL
  has "case11: the sibling preserves the exact grown-file entry on a NON-FAIL ratchet state" \
      "$sib11" "cqlite-core/src/big.rs: 900 -> 950 (limit 800)"
  has "case11: the sibling preserves the thresholds" \
      "$sib11" "thresholds: src=800 test=1500"
  has "case11: the sibling preserves the resolved base sha" "$sib11" "$base11"
  has "case11: the sibling preserves the over-threshold advisory entry" "$sib11" "950/800"
  # THE MISSING POSITIVE CONTROL (#3401 review item 1). F3 made the sibling's content
  # unconditional, which made every needle above BYTE-IDENTICAL to what the FAIL branch
  # emits on this same fixture (case 9 asserts the first one) — so if
  # CQLITE_ALLOW_FILE_GROWTH=1 ever stopped reaching the run, this case would silently
  # degrade into a second copy of case 9 and pass without ever entering the allowed-growth
  # state it exists to cover. This needle is emitted ONLY when the allowance is what let a
  # populated grown list pass, and every other state emits a "NOT enabled — …" variant
  # instead, so it cannot be satisfied from any other state.
  has "case11: CONTROL — the sibling records the allowance as the reason the ratchet passed" \
      "$sib11" "growth allowance: ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1"
  # #3402 — THE REPRODUCED DEFECT. The opt-out branch stamps the status detail, then the
  # persistence block turns the token into FAIL, so before the fix this row read
  #   file-size: FAIL (0s)  [no-cargo] — CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced);
  #   1 over-threshold file(s) grown: cqlite-core/src/big.rs
  # — a FAIL whose entire detail describes an opt-out that is NOT why it failed, sending the
  # reader to look for a growth violation that the gate had in fact ALLOWED. Measured on the
  # real gate, not reasoned about. The four needles are complementary and none is satisfiable
  # from another state: the FAIL token (persistence), the persistence wording, the ratchet's
  # OWN state preserved (so the opt-out is disclosed, not merely suppressed), and the ABSENCE
  # of the opt-out branch's own phrasing, which is the exact bytes that used to leak here.
  sumrow11="$tmp/optoutpersist.sumrow"
  fs_summary_row "$r11/.sum" "$sumrow11" ||
    bad "case11 (#3402): the run emitted no usable file-size row — the SUMMARY asserts are UNMEASURED"
  has_re "case11 (#3402): the row's TOKEN is FAIL — a persistence failure IS a component failure" \
      "$sumrow11" '^file-size: +FAIL \([0-9]+s\)'
  has "case11 (#3402): the row says the failure is LOG PERSISTENCE, not the ratchet" \
      "$sumrow11" "LOG PERSISTENCE FAILURE, not a ratchet violation"
  # THE DISCLOSURE ITSELF, not merely the token (roborev job 104). This is the one state where
  # the override name and the grown count can vanish from EVERY reachable artifact at once:
  # the log that would hold them is what failed to persist, and the sibling may be unwritable
  # too. Naming only "computed OPT-OUT" told a reader the ratchet produced a token, not that
  # it was switched off nor over how many files.
  has "case11 (#3402): the row says the ratchet was OPTED OUT OF, not merely what it computed" \
      "$sumrow11" "the ratchet was OPTED OUT OF: CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced)"
  has "case11 (#3402): the row RETAINS the grown count through a persistence failure" \
      "$sumrow11" "1 over-threshold file(s) grown"
  # ORDERING, not absence (roborev job 104). This assert used to be a `lacks` on the opt-out
  # branch's literal bytes, standing in for "the row does not blame the opt-out for the FAIL".
  # That proxy broke the moment the persistence arm legitimately RETAINED those bytes — and a
  # proxy that forbids the correct output is worse than no assert, because the obvious way to
  # green it is to delete the disclosure. The property actually wanted is that the persistence
  # cause LEADS: a reader must not be able to read the opt-out as the reason for the failure.
  # An ordered pattern says exactly that and is compatible with retaining the disclosure.
  has_re "case11 (#3402): the persistence cause LEADS the detail — the opt-out is not the reason for the FAIL" \
      "$sumrow11" 'LOG PERSISTENCE FAILURE, not a ratchet violation .*the ratchet was OPTED OUT OF'

  # -------------------------------------------------------------------------
  # Case 14 (#3402, roborev job 108) — an UNWRITABLE sidecar must leave NOTHING renderable,
  # never a stale or partial detail. `_record_status_detail` is called TWICE on this path:
  # once by the opt-out branch, then again by the persistence block to REPLACE that detail.
  # A truncate-in-place whose second write failed left the FIRST detail in the file, so the
  # row claimed `CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced)` while the component was
  # FAIL for a persistence reason — the C1 false attribution, arriving through a failed write.
  #
  # The fixture plants a DIRECTORY at the sidecar path, so every write to it fails
  # (uid-independent, unlike a chmod, which is a no-op for root).
  #
  # WHAT THIS CASE DOES *NOT* PROVE, said plainly rather than left to be assumed: it does NOT
  # discriminate the write-then-rename fix. With the sidecar wholly unwritable, the previous
  # truncate-in-place form fails identically and also renders nothing — verified against a
  # mutant restoring it, where this case still passes. The hazard the fix closes needs the
  # FIRST write to SUCCEED and the SECOND to FAIL, i.e. the path must become unwritable
  # BETWEEN two calls inside one component, which no external fixture can arrange at this
  # granularity (the same reason #3401 declared the mid-sequence partial write unreachable).
  # So this case pins the REACHABLE half — an unwritable sidecar renders nothing partial or
  # stale — and the unreachable half rests on the code being obviously safer rather than on a
  # green here. A case that cannot fail for the reason you think it can is worse than one
  # whose scope is written down.
  # -------------------------------------------------------------------------
  mkrepo detailfail cqlite-core/src/big.rs 900 950 main; r14="$REPO"
  out14="$tmp/detailfail.out"
  run_only_file_size "$r14" "$out14" PATH="$STUBBIN:$PATH" FS_SABOTAGE=detaildir \
      CQLITE_ALLOW_FILE_GROWTH=1
  d14=$(logdir_of "$out14") || bad "case14: the run published no usable 'logs:' dir"
  if [ -d "$d14/file-size.status-detail" ]; then
    ok "case14: sabotage in place (the status-detail path is a directory, so every write fails)"
  else
    bad "case14: sabotage did NOT take effect at '$d14/file-size.status-detail' — the path was never exercised"
  fi
  sumrow14="$tmp/detailfail.sumrow"
  fs_summary_row "$r14/.sum" "$sumrow14" ||
    bad "case14 (#3402): the run emitted no usable file-size row — the stale-detail asserts are UNMEASURED"
  lacks "case14 (#3402): an unwritable sidecar renders NO opt-out claim (no stale detail)" \
      "$sumrow14" "ratchet NOT enforced"
  lacks "case14 (#3402): and no partial fragment of it either" \
      "$sumrow14" "CQLITE_ALLOW_FILE_GROWTH"

  # -------------------------------------------------------------------------
  # Case 13 — CQLITE_ALLOW_FILE_GROWTH set to a NON-1 value (#3401 review item 3). The
  # branch distinguishing "set to something that is not 1" from "never set" was otherwise
  # unexercised, so it could regress while the suite stayed green. Saying "unset" to
  # someone who DID set the variable hides the one fact that fixes their invocation.
  # The needle is the SUPPLIED VALUE itself (`set to 'true'`): no other branch can emit it
  # — the =1 branch prints the ALLOWED line and the genuinely-unset branch prints "is not
  # set" — so the assert cannot pass unless this run really read that value back out.
  # -------------------------------------------------------------------------
  mkrepo badallow cqlite-core/src/big.rs 900 950 main; r13="$REPO"
  out13="$tmp/badallow.out"
  run_only_file_size "$r13" "$out13" PATH="$STUBBIN:$PATH" FS_SABOTAGE=dir \
      CQLITE_ALLOW_FILE_GROWTH=true
  d13=$(logdir_of "$out13") || bad "case13: the run published no usable 'logs:' dir"
  sib13="$d13/file-size.persistence-error.log"

  assert_verdict "case13: a non-1 allowance value does NOT allow the growth (still FAIL)" "$d13" FAIL
  has "case13: the sibling reports the SUPPLIED value, so the reader can see why it did not take" \
      "$sib13" "CQLITE_ALLOW_FILE_GROWTH is set to 'true', expected exactly 1"
  if [ ! -s "$sib13" ]; then
    bad "case13: no sibling written — the unset-vs-wrong-value distinction could not be checked"
  elif grep -Fq -- "CQLITE_ALLOW_FILE_GROWTH is not set" "$sib13"; then
    bad "case13: claims the variable is NOT SET on a run that supplied a value"
  else
    ok "case13: never claims the variable is unset when a value was supplied"
  fi

  # -------------------------------------------------------------------------
  # Case 12 — the "also written to" claim must be VERIFIED, not assumed (#3401 review
  # FIX 2). With the sibling itself pointed at /dev/full the open succeeds and every write
  # is rejected, so a claim based on the truncate alone would send the reader to an EMPTY
  # file for the block. The two needles are complementary and each is reachable from
  # exactly one branch: the negative wording exists only when the verification failed, and
  # the positive wording is asserted ABSENT, so a code path that always claimed success
  # could not satisfy both.
  # -------------------------------------------------------------------------
  if [ ! -c /dev/full ]; then
    skip "case12: no /dev/full (macOS/BSD) — sibling-sabotage control not run"
    skip "case12: no /dev/full (macOS/BSD) — FAIL verdict not asserted"
    skip "case12: no /dev/full (macOS/BSD) — honest negative claim not asserted"
    skip "case12: no /dev/full (macOS/BSD) — absence of the false claim not asserted"
  else
    mkrepo sibfail cqlite-core/src/small.rs 20 0 main; r12="$REPO"
    out12="$tmp/sibfail.out"
    run_only_file_size "$r12" "$out12" PATH="$STUBBIN:$PATH" FS_SABOTAGE=sibfull
    d12=$(logdir_of "$out12") || bad "case12: the run published no usable 'logs:' dir"
    sib12="$d12/file-size.persistence-error.log"

    if [ -L "$sib12" ] && [ "$(readlink "$sib12")" = /dev/full ] && [ -d "$d12/file-size.log" ]; then
      ok "case12: sabotage in place (log is a directory AND the sibling is -> /dev/full)"
    else
      bad "case12: sabotage did NOT take effect — the unverifiable-sibling path was never exercised"
    fi
    assert_verdict "case12: an unwritable log with an unwritable sibling is still a FAIL" "$d12" FAIL
    has "case12: stdout says the block could NOT be written IN FULL to the sibling" \
        "$out12" "It could NOT be written IN FULL to $sib12"
    if [ ! -s "$out12" ]; then
      bad "case12: no gate stdout captured — the false-claim check could not run"
    elif grep -Fq -- "also written to: $sib12" "$out12"; then
      bad "case12: stdout claims the block was written to a sibling that rejected every write"
    else
      ok "case12: stdout makes no false 'also written to' claim"
    fi
  fi
fi


# ---------------------------------------------------------------------------
printf '\n%s\n' "----------------------------------------"
printf 'file-size component log + opt-out marker guard (#3401/#3402): %d passed, %d failed, %d skipped\n' "$PASS" "$FAIL" "$SKIP"
# Census, not a floor (#3401 review N4/N2): every assertion reports exactly one of
# ok/FAIL/SKIP, so `PASS + FAIL + SKIP` is fixed for a run that reaches the end. A floor
# with slack tolerates silently deleted assertions — the vacuous-green shape this suite
# exists to refuse. A mismatch is NOT necessarily a deleted assertion: a fixture or
# precondition failure (an unusable repo, a missing mktemp) short-circuits its case's
# remaining asserts and lands here too, so the message names both causes rather than
# misattributing one as the other.
# 99 -> 107 on #3402's C1 fix: +2 case9, +2 case10, +4 case11, all unconditional (the
# FS_SABOTAGE=dir shape is uid-independent and needs no /dev/full, so none can self-skip).
# 75 (#3401) -> 107 -> 112 -> 114 -> 116 across #3402's review rounds, then DOWN to 105 on
# job 23, when the inline grown-path list was removed (cases 4b/4e/4g went with it, -12, and
# case 4 gained a log-pointer needle plus a no-repository-path needle, +1 net), then 105 ->
# 108 on job 25 (+3 case4h: a hostile TMPDIR must not cost the disclosure). A census that
# only ever rises is a census nobody re-derives — this one is recomputed from the run, and
# the first value written here was WRONG (109, guessed from the number of asserts typed
# rather than counted from a run: `fs_summary_row || bad` contributes nothing unless it
# fires).
# 113 -> 116 on job 108 (+3 case14: an unwritable sidecar must leave nothing renderable).
# Counted from a RUN, not from asserts typed: the `fs_summary_row || bad` guard contributes
# nothing unless it fires, which is why the first value written here (117) was wrong twice.
EXPECTED_CHECKS=116
if [ "$((PASS + FAIL + SKIP))" -ne "$EXPECTED_CHECKS" ]; then
  printf 'FAIL - assertion census mismatch: %d checks ran (%d ok / %d fail / %d skip), expected exactly %d.\n' \
    "$((PASS + FAIL + SKIP))" "$PASS" "$FAIL" "$SKIP" "$EXPECTED_CHECKS"
  printf '       Either an assertion was added/deleted, or a fixture/precondition failure short-circuited a case.\n'
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
