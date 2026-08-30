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
# Six paths through run_file_size, because the log must exist on ALL of them (AC3):
#   1. grown + over threshold          -> FAIL
#   2. over threshold but SHRUNK       -> PASS (advisory list, empty ratchet)
#   3. no changed .rs files at all     -> PASS ("nothing over threshold" still logged)
#   4. grown + CQLITE_ALLOW_FILE_GROWTH=1 -> PASS (the opt-out is RECORDED)
#   5. base ref unresolvable           -> PASS (advisory only, ratchet skipped)
#   6. AC2: the FAIL stdout NAMES $LOG_DIR/file-size.log, so it is reachable from the
#      SUMMARY's existing `logs:` line without the reader guessing a filename.
#
# Hermetic and fast: throwaway git repos under one mktemp, each holding ONLY a copy of the
# gate script (so the gate's `cd "$(dirname "$0")/.."` resolves REPO_ROOT into the temp
# tree) plus a synthetic .rs file. Driven through the REAL `--only file-size` path, which
# skips the dataset preflight and compiles nothing. No cargo, no network, no datasets, no
# Docker. TMPDIR is redirected so every gate LOG_DIR also lands inside this run's namespace.
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
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-file-size.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
# Contain each gate run's LOG_DIR (mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX") inside
# this run's namespace so the trap above reclaims them too.
export TMPDIR="$tmp"

GIT_ID=(-c user.email=gate@example.invalid -c user.name=gate-selftest)

# lines <n> <path> — write exactly <n> newline-terminated lines (so `wc -l` == n).
lines() { awk -v n="$1" 'BEGIN { for (i = 1; i <= n; i++) print "// filler line " i }' >"$2"; }

# mkrepo <name> <rs-relpath> <committed-lines> <worktree-lines> <branch>
#   Commits a .rs file of <committed-lines>, then rewrites the WORKTREE copy to
#   <worktree-lines>. A <worktree-lines> of 0 means "leave the commit untouched".
mkrepo() {
  local name="$1" rel="$2" nbase="$3" nhead="$4" branch="$5"
  local root="$tmp/$name"
  mkdir -p "$root/scripts" "$root/$(dirname "$rel")"
  cp "$GATE" "$root/scripts/agent-gate.sh"
  printf 'target/\n*.log\n.agent-gate-summary.txt\n' >"$root/.gitignore"
  lines "$nbase" "$root/$rel"
  ( cd "$root" && git init -q -b "$branch" . && git add -A &&
      git "${GIT_ID[@]}" commit -qm init ) >/dev/null 2>&1
  [ "$nhead" -gt 0 ] && lines "$nhead" "$root/$rel"
  printf '%s\n' "$root"
}

# run_only_file_size <repo> <outfile> [KEY=VAL …] -> exit status of the gate run
run_only_file_size() {
  local repo="$1" out="$2"; shift 2
  ( cd "$repo" && env ${1+"$@"} AGENT_GATE_SUMMARY_FILE="$repo/.sum" \
      bash "$repo/scripts/agent-gate.sh" --only file-size >"$out" 2>&1 )
}

# The component log is reachable ONLY via the SUMMARY's `logs:` line — read it from there
# rather than from an env var, so the test proves that route works (AC2's premise).
logdir_of() { sed -n 's/^logs:[[:space:]]*//p' "$1" | head -1; }

# verdict_of <logdir> — the component's own verdict, read from the UNCHANGED
# `file-size.result` (`STATUS SECONDS`). Deliberately NOT the gate's exit status: a
# passing `--only` run exits 3 (RESULT: PARTIAL) and a failing one exits 1, so an
# exit-code assert would conflate "the component failed" with "this was a partial run".
verdict_of() { read -r _st _secs <"$1/file-size.result" 2>/dev/null; printf '%s\n' "${_st:-<no-result-file>}"; }

# has <label> <file> <literal> — the workhorse content assert.
has() {
  if [ -f "$2" ] && grep -Fq -- "$3" "$2"; then ok "$1"; else
    bad "$1 (missing: '$3')"
  fi
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

# ---------------------------------------------------------------------------
# Case 1 — FAIL: an over-threshold source file GROWN by the diff (800-line src limit).
# ---------------------------------------------------------------------------
r1=$(mkrepo grew cqlite-core/src/big.rs 900 950 main)
out1="$tmp/grew.out"
run_only_file_size "$r1" "$out1"
d1=$(logdir_of "$out1")
log1="$d1/file-size.log"
base1=$( cd "$r1" && git rev-parse HEAD )

if [ "$(verdict_of "$d1")" = FAIL ]; then
  ok "case1: --only file-size FAILs on a grown over-threshold file (the case being diagnosed)"
else
  bad "case1: --only file-size did NOT fail on a grown over-threshold file — fixture is wrong"
fi
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
r2=$(mkrepo shrank cqlite-core/src/big.rs 950 900 main)
out2="$tmp/shrank.out"
run_only_file_size "$r2" "$out2"
d2=$(logdir_of "$out2")
log2="$d2/file-size.log"

if [ "$(verdict_of "$d2")" = PASS ]; then
  ok "case2: a shrunk over-threshold file PASSes the ratchet (fixture is a real PASS run)"
else
  bad "case2: a shrunk over-threshold file did NOT pass — fixture is wrong"
fi
assert_log_present "case2: file-size.log written on a PASS run too (AC3)" "$log2"
has "case2: PASS log still carries the over-threshold advisory entry" \
    "$log2" "900/800"
has "case2: PASS log carries the terminal PASS verdict" \
    "$log2" ">>> [file-size] PASS"
if [ ! -f "$log2" ]; then
  bad "case2: cannot check the ratchet emptiness — no log to read"
elif grep -Fq -- '-> ' "$log2"; then
  bad "case2: PASS log lists a growth entry for a file that SHRANK"
else
  ok "case2: PASS log lists no growth entry (ratchet genuinely empty)"
fi

# ---------------------------------------------------------------------------
# Case 3 — PASS with NO changed .rs files at all: an empty-ish run still gets a log that
# SAYS SO (never an absent file, never a zero-byte one).
# ---------------------------------------------------------------------------
r3=$(mkrepo clean cqlite-core/src/small.rs 20 0 main)
out3="$tmp/clean.out"
run_only_file_size "$r3" "$out3"
d3=$(logdir_of "$out3")
log3="$d3/file-size.log"

if [ "$(verdict_of "$d3")" = PASS ]; then
  ok "case3: a clean tree PASSes file-size"
else
  bad "case3: a clean tree did NOT pass file-size — fixture is wrong"
fi
assert_log_present "case3: file-size.log written even with nothing to report (AC3)" "$log3"
has "case3: empty-ish log states that nothing is over threshold" \
    "$log3" "no changed .rs files over threshold"
has "case3: empty-ish log still states the thresholds" \
    "$log3" "thresholds: src=800 test=1500"
has "case3: empty-ish log still carries the terminal verdict" \
    "$log3" ">>> [file-size] PASS"

# ---------------------------------------------------------------------------
# Case 4 — the CQLITE_ALLOW_FILE_GROWTH=1 opt-out. The growth is ALLOWED, and the log must
# RECORD what was allowed (the numbers are the whole point of the acknowledgement).
# ---------------------------------------------------------------------------
r4=$(mkrepo optout cqlite-core/src/big.rs 900 950 main)
out4="$tmp/optout.out"
run_only_file_size "$r4" "$out4" CQLITE_ALLOW_FILE_GROWTH=1
d4=$(logdir_of "$out4")
log4="$d4/file-size.log"

if [ "$(verdict_of "$d4")" = PASS ]; then
  ok "case4: CQLITE_ALLOW_FILE_GROWTH=1 turns the same growth into a PASS"
else
  bad "case4: CQLITE_ALLOW_FILE_GROWTH=1 did not allow the growth — fixture is wrong"
fi
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
r5=$(mkrepo nobase cqlite-core/src/big.rs 900 950 work)
out5="$tmp/nobase.out"
run_only_file_size "$r5" "$out5"
d5=$(logdir_of "$out5")
log5="$d5/file-size.log"

if [ "$(verdict_of "$d5")" = PASS ]; then
  ok "case5: with no resolvable base ref the component is advisory-only and PASSes"
else
  bad "case5: no-base run did not PASS — the ratchet ran without a base ref"
fi
assert_log_present "case5: file-size.log written on the no-base path (AC3)" "$log5"
has "case5: no-base log states the ratchet was skipped, in those words" \
    "$log5" "base ref unavailable — growth ratchet skipped (advisory only)"
has "case5: no-base log still carries the advisory over-threshold entry" \
    "$log5" "cqlite-core/src/big.rs"
has "case5: no-base log carries the terminal verdict" \
    "$log5" ">>> [file-size] PASS"

# ---------------------------------------------------------------------------
printf '\n%s\n' "----------------------------------------"
printf 'file-size component log guard (#3401): %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
[ "$PASS" -ge 25 ] || { printf 'FAIL - vacuous run: only %d checks executed\n' "$PASS"; exit 1; }
exit 0
