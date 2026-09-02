#!/usr/bin/env bash
# Regression test for issue #3401: the `file-size` gate component must PERSIST its
# arithmetic as $LOG_DIR/file-size.log, on EVERY invocation.
#
# The defect: run_file_size computed the base ref, the over-threshold advisory list and
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
# Seven paths through run_file_size, because the log must exist on ALL of them (AC3):
#   1. grown + over threshold             -> FAIL
#   2. over threshold but SHRUNK          -> PASS (advisory list, empty ratchet)
#   3. no changed .rs files at all        -> PASS ("nothing over threshold" still logged)
#   3b. changed .rs files, none over the threshold -> PASS (same log line, DIFFERENT input:
#       case 3's tree is clean, so on its own it never proves a CHANGED-but-small file is
#       handled — only that the component ran with an empty file list)
#   4. grown + CQLITE_ALLOW_FILE_GROWTH=1 -> PASS (the opt-out is RECORDED)
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
# ---------------------------------------------------------------------------
mkrepo optout cqlite-core/src/big.rs 900 950 main; r4="$REPO"
out4="$tmp/optout.out"
run_only_file_size "$r4" "$out4" CQLITE_ALLOW_FILE_GROWTH=1
d4=$(logdir_of "$out4") || bad "case4: the run published no usable 'logs:' dir"
log4="$d4/file-size.log"

assert_verdict "case4: CQLITE_ALLOW_FILE_GROWTH=1 turns the same growth into a PASS" "$d4" PASS
assert_log_present "case4: file-size.log written on the opt-out path (AC3)" "$log4"
has "case4: opt-out log records the acknowledgement" \
    "$log4" "ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1"
has "case4: opt-out log still records the exact growth entry" \
    "$log4" "cqlite-core/src/big.rs: 900 -> 950 (limit 800)"
has "case4: opt-out log carries the terminal PASS verdict" \
    "$log4" ">>> [file-size] PASS"

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
    # 4 emitted lines on a clean tree, and the ENUMERATION is the point — this stays a
    # derived expectation, never a magic constant:
    #   1. thresholds
    #   2. base ref
    #   3. AGENT-GATE-CENSUS (the #3162 `emitted` contract line; on a clean tree it is the
    #      NO-SUBJECT form, because the diff changed no .rs file)
    #   4. "no changed .rs files over threshold"
    # It was 3 before the census line was added, and this case correctly caught the change:
    # the EXACT count is the oracle — a fabricated or hardcoded flag cannot produce it, and
    # the `[ -s ]` check cannot produce a count at all. Adding or removing an _fs_emit call
    # in run_file_size MUST move this number, and the enumeration above says which line is
    # which so the next person can tell a real regression from an intended emit.
    has "case8: the cause names the EXACT number of rejected writes (the counter, not the -s check)" \
        "$out8" "4 write(s) to it were rejected"
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
# case14 (#3162, roborev job 389): THE CENSUS COUNT COMES FROM THE MEASUREMENT, NOT FROM A
# PREDICATE ABOUT THE PATH.
#
# `n_scanned` was derived by re-asking `[ -f "$path" ]` in a second pass. `[ -f ]` answers
# "does this path exist right now"; the census claims "I counted this file's lines". A file
# that is SELECTED but unreadable satisfied the predicate and was reported as MEASURED — the
# count asserting verification that did not happen, which is the one thing a census must
# never do. It is now incremented only after a `wc -l` that produced a validated number.
#
# THREE STATES, AND THE COLLAPSE THAT MUST NOT HAPPEN: nothing selected is NO-SUBJECT and
# preserves PASS; *some* selected of which *none* could be counted is NOT-MEASURED. Letting
# the second borrow the first's affirmative silence is the same "could not tell" -> permissive
# slide, one lane over — so it is asserted explicitly, not left implied by the happy path.
# ---------------------------------------------------------------------------
c14_pre=0
if [ "$(id -u)" -ne 0 ]; then
  c14_probe="$tmp/c13-probe"
  if printf 'x\n' >"$c14_probe" 2>/dev/null && chmod 000 "$c14_probe" 2>/dev/null \
     && ! ( wc -l <"$c14_probe" ) >/dev/null 2>&1; then
    c14_pre=1
  fi
  chmod 600 "$c14_probe" 2>/dev/null || true
fi
if [ "$c14_pre" -ne 1 ]; then
  # ONE skip PER SKIPPED ASSERT, the case8 precedent — the suite's census is exact.
  skip "case14: cannot make a file unreadable on this host (root, or a permissive FS) — positive control not run"
  skip "case14: cannot make a file unreadable on this host — uncountable-file census not asserted"
  skip "case14: cannot make a file unreadable on this host — NO-SUBJECT collapse not asserted"
  skip "case14: cannot make a file unreadable on this host — uncounted/selected counts not asserted"
else
  # POSITIVE CONTROL FIRST, and it differs from the plant in ONE property: whether the
  # selected file can be read. Without it a FAIL below could be the fixture, not the census.
  mkrepo censusread14 cqlite-core/src/big.rs 900 950 main; r14a="$REPO"
  out14a="$tmp/censusread14.out"
  run_only_file_size "$r14a" "$out14a"
  if grep -Fq 'AGENT-GATE-CENSUS: 1 changed .rs file(s) measured against the thresholds' "$out14a"; then
    ok "case14: control — a READABLE changed .rs file censuses as 1 measured"
  else
    bad "case14: control — expected the COUNT form for a readable changed file; got: $(grep -F 'AGENT-GATE-CENSUS:' "$out14a" | head -1)"
  fi

  mkrepo censusunread14 cqlite-core/src/big.rs 900 950 main; r14b="$REPO"
  out14b="$tmp/censusunread14.out"
  chmod 000 "$r14b/cqlite-core/src/big.rs" 2>/dev/null || true
  run_only_file_size "$r14b" "$out14b"
  c14_line=$(grep -F 'AGENT-GATE-CENSUS:' "$out14b" | head -1)
  chmod 600 "$r14b/cqlite-core/src/big.rs" 2>/dev/null || true
  case "$c14_line" in
    *'AGENT-GATE-CENSUS: NOT-MEASURED'*'could not be line-counted'*)
      ok "case14: a SELECTED but unreadable .rs file censuses as NOT-MEASURED, never as measured" ;;
    *) bad "case14: expected the NOT-MEASURED form for an unreadable selected file; got: ${c14_line:-<no contract line>}" ;;
  esac
  # THE COLLAPSE, asserted explicitly: this is NOT the empty-subject state.
  case "$c14_line" in
    *NO-SUBJECT*) bad "case14: an unreadable selected file rendered as NO-SUBJECT — 'there was nothing to measure' and 'there was something I could not measure' have collapsed" ;;
    *) ok "case14: it does NOT render as NO-SUBJECT — the empty-subject state keeps its own meaning" ;;
  esac
  # …and the numbers are the real ones, so the line cannot be a fixed string.
  case "$c14_line" in
    *'1 of 1 changed .rs file(s)'*)
      ok "case14: the cause names the EXACT uncounted/selected counts (1 of 1), so the count is derived and not a canned message" ;;
    *) bad "case14: expected '1 of 1 changed .rs file(s)' in the cause; got: ${c14_line:-<no contract line>}" ;;
  esac
fi

# ---------------------------------------------------------------------------
# case15 (#3162, roborev job 396): A FAILED ENUMERATION IS NOT AN EMPTY DIFF.
#
# `files=$(git diff --name-only …)` discarded its exit status. With no `set -e`, a failed
# enumeration left `files` empty — indistinguishable from "no .rs changed" — so the census
# emitted `NO-SUBJECT the diff changed no .rs file` and the component PASSED while
# affirmatively claiming it had measured an empty diff. That is the named
# `1699-find-tristate` shape: a THREE-valued signal read two-valued, with the unmeasured
# state taking the permissive branch.
#
# The stub fails ONLY `git diff --name-only --diff-filter=d`, which is uniquely this
# component's enumeration — every other git call the gate makes (tree identity, base-ref
# resolution, the component-set pre-flight) is delegated to the real binary, so a red here
# cannot come from a differently-broken fixture.
# ---------------------------------------------------------------------------
REAL_GIT=$(command -v git 2>/dev/null)
if [ -z "$REAL_GIT" ]; then
  bad "case15: no git on PATH — but this suite's fixtures need git, so this is a broken host, not a skip"
  bad "case15: enumeration-failure census not asserted (no git)"
  bad "case15: NO-SUBJECT collapse not asserted (no git)"
  bad "case15: control not asserted (no git)"
else
  cat >"$STUBBIN/git" <<STUB
#!/usr/bin/env bash
# Delegate everything to the real git EXCEPT run_file_size's own enumeration, which fails
# with a chosen rc when FS_GIT_DIFF_FAIL is set.
if [ -n "\${FS_GIT_DIFF_FAIL:-}" ]; then
  _saw_diff=0; _saw_name_only=0; _saw_filter=0
  for _a in "\$@"; do
    case "\$_a" in
      diff)              _saw_diff=1 ;;
      --name-only)       _saw_name_only=1 ;;
      --diff-filter=d)   _saw_filter=1 ;;
    esac
  done
  if [ "\$_saw_diff" -eq 1 ] && [ "\$_saw_name_only" -eq 1 ] && [ "\$_saw_filter" -eq 1 ]; then
    echo "stub: enumeration deliberately failed" >&2
    exit "\$FS_GIT_DIFF_FAIL"
  fi
fi
exec "$REAL_GIT" "\$@"
STUB
  chmod +x "$STUBBIN/git"

  # POSITIVE CONTROL FIRST — the same fixture and the same stub on PATH, differing in ONE
  # property: whether the enumeration is made to fail. Without it, a red below could be the
  # stub breaking git in general rather than the rc being honoured.
  mkrepo gitokfs cqlite-core/src/big.rs 900 950 main; r15a="$REPO"
  out15a="$tmp/gitokfs.out"
  run_only_file_size "$r15a" "$out15a" PATH="$STUBBIN:$PATH"
  if grep -Fq 'AGENT-GATE-CENSUS: 1 changed .rs file(s) measured against the thresholds' "$out15a"; then
    ok "case15: control — with the stub on PATH but NOT sabotaging, the enumeration succeeds and censuses 1 measured file"
  else
    bad "case15: control — the stub broke git in general; got: $(grep -F 'AGENT-GATE-CENSUS:' "$out15a" | head -1)"
  fi

  mkrepo gitfailfs cqlite-core/src/big.rs 900 950 main; r15b="$REPO"
  out15b="$tmp/gitfailfs.out"
  run_only_file_size "$r15b" "$out15b" PATH="$STUBBIN:$PATH" FS_GIT_DIFF_FAIL=7
  c15_line=$(grep -F 'AGENT-GATE-CENSUS:' "$out15b" | head -1)
  case "$c15_line" in
    *'AGENT-GATE-CENSUS: NOT-MEASURED the changed-.rs enumeration FAILED'*)
      ok "case15: a FAILED enumeration censuses as NOT-MEASURED, not as a measured empty diff" ;;
    *) bad "case15: expected the NOT-MEASURED form for a failed enumeration; got: ${c15_line:-<no contract line>}" ;;
  esac
  # THE COLLAPSE, asserted explicitly — this is the whole finding.
  case "$c15_line" in
    *NO-SUBJECT*) bad "case15: a FAILED enumeration rendered as NO-SUBJECT — 'the scan failed' has collapsed onto 'no match', which is the 1699-find-tristate shape" ;;
    *) ok "case15: it does NOT render as NO-SUBJECT — a failed scan cannot borrow the empty diff's affirmative silence" ;;
  esac
  # …and it names the ACTUAL rc, so the line cannot be a canned string.
  case "$c15_line" in
    *'git diff exited 7'*)
      ok "case15: the cause names the REAL exit status (7), so the state is derived from the rc and not from a fixed message" ;;
    *) bad "case15: expected 'git diff exited 7' in the cause; got: ${c15_line:-<no contract line>}" ;;
  esac
  rm -f "$STUBBIN/git"
fi

# ---------------------------------------------------------------------------
printf '\n%s\n' "----------------------------------------"
printf 'file-size component log guard (#3401): %d passed, %d failed, %d skipped\n' "$PASS" "$FAIL" "$SKIP"
# Census, not a floor (#3401 review N4/N2): every assertion reports exactly one of
# ok/FAIL/SKIP, so `PASS + FAIL + SKIP` is fixed for a run that reaches the end. A floor
# with slack tolerates silently deleted assertions — the vacuous-green shape this suite
# exists to refuse. A mismatch is NOT necessarily a deleted assertion: a fixture or
# precondition failure (an unusable repo, a missing mktemp) short-circuits its case's
# remaining asserts and lands here too, so the message names both causes rather than
# misattributing one as the other.
EXPECTED_CHECKS=83
if [ "$((PASS + FAIL + SKIP))" -ne "$EXPECTED_CHECKS" ]; then
  printf 'FAIL - assertion census mismatch: %d checks ran (%d ok / %d fail / %d skip), expected exactly %d.\n' \
    "$((PASS + FAIL + SKIP))" "$PASS" "$FAIL" "$SKIP" "$EXPECTED_CHECKS"
  printf '       Either an assertion was added/deleted, or a fixture/precondition failure short-circuited a case.\n'
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
