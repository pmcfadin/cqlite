#!/usr/bin/env bash
# Regression test for #3731/#3220 AC3: the #3358 BTI token-bound lane must FAIL — never
# skip, never pass vacuously — in BOTH fixture-absence directions.
#
# WHY THIS FILE EXISTS AT ALL. The #3220 hardening removed three SKIP guards from
# `issue_3358_bti_query_token_bound.rs` and its PR recorded the RED verification as
# MEASUREMENTS IN A COMMENT. That is evidence that the guard fired ONCE, on one machine,
# on one day — it is not a guard. Nothing stopped `fixture()` reverting to `Option` and
# every run staying green, because a run with the fixture PRESENT looks identical either
# way. roborev raised exactly that (job 252): "the new fail-closed behavior has no
# committed negative test". So the observation moves here, where it re-runs.
#
# ASSERTION CONTRACT, inherited from `test_point_vs_full_failclosed.sh` (#3220 AC2) and
# held to the same standard: every check must be UNSATISFIABLE by output that does not
# demonstrate the property claimed. Two forms are therefore banned in the failure cases:
#   * a bare nonzero exit — it proves only that SOMETHING failed, not that the STAGED
#     condition was rejected (an unrelated compile error produces an identical rc); and
#   * a bare mention of the table name — the fixture PATH carries it in healthy output
#     too, so accepting it would let a run that resolved to the real fixture satisfy the
#     control meant to rule that out.
# Each case therefore pins a count-bearing `test result:` line AND a message naming this
# target's own guard, and the terminal EXPECTED_CHECKS anchor keeps a dropped case from
# reading as green.
#
# The cases:
#   control — ambient corpus: the target PASSES and all THREE tests run (the count is
#             asserted, so a target that executed nothing cannot satisfy it).
#   absent  — no candidate root holds `wide_multiclustering_small-*/da-1-bti-Data.db`:
#             every case must FAIL with the generation-specific diagnostic. Also pins
#             the seam NOTE, since this case is what sets the seam.
#   empty   — the fixture dir and its `-Data.db` exist but the file is 0 bytes: must be
#             a hard failure, never a SKIP and never a 0-rows pass.
#   safety  — neither the git-tracked fixture nor the ambient corpus was mutated.
#
# DISCRIMINATION LIMIT, stated rather than implied. The sibling script can prove its
# staging is surgical from the LANE's own output, because its target drives many fixtures
# and a sibling case's `PASS` line shows only one was hidden. This target has exactly ONE
# fixture, so no such per-case list exists and that evidence is not available here.
# Instead the staging is asserted SURGICAL against the filesystem (the keyspace dir is
# mirrored with its sibling tables and only this fixture withheld), which is what pins
# the specific #3220 defect: keyspace-granular selection would have ACCEPTED that root.
#
# Run standalone:   bash scripts/tests/test_issue_3358_failclosed.sh
# Or via the gate:  `bti-multiclustering`, alongside the sibling fail-closed script, so
#                   the test binary it drives is already built.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

FIXTURE_REL="test-data/datasets/sstables/test_da/wide_multiclustering_small-47f6a3008f6911f1bc0f8df8badcc262"
FIXTURE_DIRNAME=$(basename "$FIXTURE_REL")
DATA_DB="da-1-bti-Data.db"
TEST_TARGET="issue_3358_bti_query_token_bound"
TEST_COUNT=3

# #2751 defense-in-depth: never clobber an inherited gate summary path.
unset AGENT_GATE_SUMMARY_FILE
# The guard under test is the UNCONDITIONAL one (committed fixture => no legitimate
# SKIP), so an inherited CQLITE_REQUIRE_FIXTURES must not decide any case.
unset CQLITE_REQUIRE_FIXTURES
# Scrub the seam this file itself drives, so a stale export cannot pre-decide a case.
unset CQLITE_TEST_CHECKOUT_SSTABLES_ROOT

# Single-quote a path for safe pasting into a shell, mirroring `shell_quote` in
# cqlite-core/tests/issue_3358_bti_query_token_bound.rs — same reason, same POSIX form:
# an embedded single quote is closed, escaped and reopened. Raw single quotes were used
# here first (roborev job 257), which breaks on a checkout path containing an apostrophe
# and can turn later path characters into shell syntax when pasted. The Rust side of this
# lane had already solved it; this mirrors it rather than re-deriving it.
shell_quote() { printf "'%s'" "${1//\'/\'\\\'\'}"; }

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Scratch root, VALIDATED before the cleanup trap is armed: this script runs without
# `errexit` (every case must run, so one failure cannot hide the rest), so an unchecked
# `mktemp -d` would leave `$tmp` empty, resolve every derived path under `/`, and hand
# the EXIT trap an `rm -rf ""`.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/i3358-failclosed.XXXXXX") || {
  echo "FATAL: mktemp -d failed; refusing to run with an unset scratch root" >&2
  exit 1
}
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  echo "FATAL: mktemp -d produced no usable directory ('$tmp'); refusing to run" >&2
  exit 1
fi
# ABSOLUTE, before the trap and before anything is staged under it. A relative TMPDIR
# yields a relative `$tmp`, and `run_lane` does `cd "$REPO"` — so every staged root would
# resolve against the REPO instead of here, the empty case would miss its staged corpus,
# fall back to the healthy checkout fixture, and the harness would FAIL a correct tree.
# A guard that reds on correct input is the guard agents learn to waive.
tmp=$(cd "$tmp" && pwd) || {
  echo "FATAL: could not canonicalize the scratch root; refusing to run" >&2
  exit 1
}
case $tmp in
  /*) ;;
  *) echo "FATAL: scratch root '$tmp' is not absolute; refusing to run" >&2; exit 1 ;;
esac
trap 'rm -rf "$tmp"' EXIT

# The committed fixture must be present for ANY of this to mean anything: the control's
# expected behavior is "it runs", and both failure cases are defined relative to it.
# Absent => hard error, not a skip. This fixture is git-tracked (#3220).
if [ ! -f "$REPO/$FIXTURE_REL/$DATA_DB" ]; then
  echo "FATAL: committed fixture $FIXTURE_REL/$DATA_DB is missing from the checkout." >&2
  echo "" >&2
  # DELIBERATELY NOT `git restore -- test-data/datasets`. That pathspec is the WHOLE
  # corpus directory, so it would also revert a reader's unrelated uncommitted fixture
  # changes — silent data loss in a message offered as a repair (roborev job 256, High).
  # It is also, precisely, one of the five defects the panic in
  # cqlite-core/tests/issue_3358_bti_query_token_bound.rs documents itself as refusing
  # to reproduce ("a glob that would restore the WHOLE fixture directory"). Same
  # resolution as there: point at the tested emitter, which scopes the restore to the
  # files that are actually missing, and do not hand-build one here.
  echo "       remedy: run the tracked-fixture probe, which names every missing" >&2
  echo "       git-tracked file and prints a restore command scoped to ONLY those:" >&2
  echo "" >&2
  echo "         CQLITE_DATASETS_ROOT=$(shell_quote "$REPO/test-data/datasets") \\" >&2
  echo "           bash $(shell_quote "$REPO/test-data/scripts/fetch-datasets.sh") --verify-only" >&2
  echo "" >&2
  echo "       Follow its 'ERROR: TRACKED FIXTURES MISSING ... (issue #3310)' block." >&2
  echo "       If it instead reports 'Tracked-fixture probe (#3310): OK', nothing is" >&2
  echo "       missing: ignore the remaining ERROR: lines (they report an incomplete" >&2
  echo "       FETCHED corpus, which a checkout is never meant to be) and do NOT run" >&2
  echo "       the '.dataset-pin' remedy they suggest — it re-runs the destructive" >&2
  echo "       fetch path against this checkout root." >&2
  exit 1
fi

# Run the target with an explicit environment. Prints nothing; the caller greps $out.
run_lane() {
  local out=$1; shift
  # `env -u` clears both seams unless the case re-supplies them, so no case can inherit
  # another's staging.
  ( cd "$REPO" && env -u CQLITE_DATASETS_ROOT -u CQLITE_TEST_CHECKOUT_SSTABLES_ROOT "$@" \
      cargo test -p cqlite-core --features "state_machine cli-helpers" \
        --test "$TEST_TARGET" -- --nocapture ) >"$out" 2>&1
  return $?
}

# ---------------------------------------------------------------------------
# 1. CONTROL: the ambient corpus. The target passes AND all three tests run.
# ---------------------------------------------------------------------------
control_out="$tmp/control.log"
control_env=()
[ -n "${CQLITE_DATASETS_ROOT:-}" ] && control_env=(CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT")
run_lane "$control_out" "${control_env[@]}"
control_rc=$?
if [ "$control_rc" -eq 0 ]; then
  ok "control: the target passes against the ambient corpus"
else
  bad "control: the target FAILED against the ambient corpus (rc=$control_rc) — the two \
failure cases below prove nothing until this passes; see $control_out"
  sed -n '1,40p' "$control_out"
fi
# COUNT-BEARING, deliberately: `test result: ok. 0 passed` is also an `ok` line, so a
# target whose cases were all filtered out or cfg'd away would satisfy a bare `ok`
# match. Requiring the exact count is what makes this a positive control.
if grep -qE "^test result: ok\. ${TEST_COUNT} passed; 0 failed" "$control_out"; then
  ok "control: all ${TEST_COUNT} cases actually RAN and passed (count asserted, not just 'ok')"
else
  bad "control: no 'test result: ok. ${TEST_COUNT} passed; 0 failed' line — the cases did \
not all run, so a green here would be vacuous; see $control_out"
fi

# ---------------------------------------------------------------------------
# 2. ABSENT: no candidate root carries THIS generation of THIS table, while every
#    sibling table stays resolvable.
#
#    The staging is SURGICAL, not "hide the whole corpus". Hiding everything would let
#    the assertion pass for a reason unrelated to this fixture. The checkout candidate
#    is mirrored by SYMLINK, table by table, minus the one fixture; the env candidate is
#    an empty root that exists (so the resolver walks it and finds nothing, rather than
#    skipping an unreadable dir).
# ---------------------------------------------------------------------------
absent_checkout="$tmp/absent-checkout/sstables"
mkdir -p "$absent_checkout" "$tmp/absent-env/sstables"
src_sstables="$REPO/test-data/datasets/sstables"
for ks_dir in "$src_sstables"/*/; do
  [ -d "$ks_dir" ] || continue
  ks_name=$(basename "$ks_dir")
  mkdir -p "$absent_checkout/$ks_name"
  for tbl_dir in "$ks_dir"*/; do
    [ -d "$tbl_dir" ] || continue
    tbl_name=$(basename "$tbl_dir")
    case "$tbl_name" in
      wide_multiclustering_small-*) continue ;;   # the ONLY thing hidden
    esac
    ln -s "${tbl_dir%/}" "$absent_checkout/$ks_name/$tbl_name"
  done
done
# The keyspace dir EXISTS and holds sibling tables: that is what pins the #3220 defect
# specifically, because keyspace-granular selection would have accepted this root.
if [ -d "$absent_checkout/test_da" ] \
   && [ -n "$(ls -A "$absent_checkout/test_da" 2>/dev/null)" ] \
   && [ ! -e "$absent_checkout/test_da/$FIXTURE_DIRNAME" ]; then
  ok "absent: staging is surgical (test_da present with sibling tables, fixture hidden)"
else
  bad "absent: staging is wrong — test_da must exist with siblings and without the fixture"
fi

absent_out="$tmp/absent.log"
run_lane "$absent_out" \
  CQLITE_DATASETS_ROOT="$tmp/absent-env" \
  CQLITE_TEST_CHECKOUT_SSTABLES_ROOT="$absent_checkout"
absent_rc=$?
if [ "$absent_rc" -ne 0 ]; then
  ok "absent: the target FAILS when no candidate root holds the fixture (rc=$absent_rc)"
else
  bad "absent: the target PASSED with the fixture absent from every candidate root — \
this is the #3220 silent-skip defect the hardening removed; see $absent_out"
fi
# EVERY case must fail, not merely one: `must_run` is per case (#3220 forbids a
# suite-wide `ran > 0`), so a run where one case failed and two silently skipped would
# be the very defect this pins. The count is what expresses that.
if grep -qE "^test result: FAILED\. 0 passed; ${TEST_COUNT} failed" "$absent_out"; then
  ok "absent: ALL ${TEST_COUNT} cases failed — fail-closed is per case, not suite-wide"
else
  bad "absent: no 'test result: FAILED. 0 passed; ${TEST_COUNT} failed' line — some case \
did not fail-close, or the run failed for an unrelated reason; see $absent_out"
fi
# The target's OWN diagnostic, generation-specific. A bare table-name match is banned:
# healthy output carries it in the fixture path.
if grep -q "no directory \`wide_multiclustering_small-\*\` holding \`${DATA_DB}\`" "$absent_out"; then
  ok "absent: the rejection is this target's own generation-specific diagnostic"
else
  bad "absent: no 'no directory \`wide_multiclustering_small-*\` holding \`${DATA_DB}\`' \
line — the failure does not demonstrate THIS guard fired; see $absent_out"
fi
# A SKIP must not reappear under any wording. The removed guards printed 'SKIP:'.
if grep -q 'SKIP' "$absent_out"; then
  bad "absent: output contains 'SKIP' — a committed fixture must never skip (#3220); \
see $absent_out"
else
  ok "absent: no SKIP anywhere in the output"
fi
# The seam NOTE is behavior this lane added: a stray seam value hard-FAILs a CORRECT
# checkout, which is the safe direction but opaque without it. This case sets the seam,
# so the note must appear.
if grep -q "CQLITE_TEST_CHECKOUT_SSTABLES_ROOT is SET to" "$absent_out"; then
  ok "absent: the diagnostic names the seam that replaced the checkout candidate"
else
  bad "absent: the seam NOTE is missing — a stray seam value would hard-FAIL a correct \
checkout with no hint why; see $absent_out"
fi

# ---------------------------------------------------------------------------
# 3. EMPTY: the fixture is present but its Data.db carries no bytes.
#
#    This is the direction doctrine warns is SILENT: a zero-length component can make a
#    read return 0 rows with no error, and "0 == 0" then passes. The fixture is COPIED,
#    never truncated in place — case 4 re-asserts that.
# ---------------------------------------------------------------------------
empty_root="$tmp/empty/sstables/test_da"
mkdir -p "$empty_root"
cp -r "$REPO/$FIXTURE_REL" "$empty_root/"
: > "$empty_root/$FIXTURE_DIRNAME/$DATA_DB"
if [ -f "$empty_root/$FIXTURE_DIRNAME/$DATA_DB" ] && [ ! -s "$empty_root/$FIXTURE_DIRNAME/$DATA_DB" ]; then
  ok "empty: staging is right (the copied Data.db exists and is 0 bytes)"
else
  bad "empty: staging is wrong — the copied Data.db must exist AND be empty"
fi
empty_out="$tmp/empty.log"
run_lane "$empty_out" CQLITE_DATASETS_ROOT="$tmp/empty"
empty_rc=$?
if [ "$empty_rc" -ne 0 ]; then
  ok "empty: the target FAILS on a present-but-empty fixture (rc=$empty_rc)"
else
  bad "empty: the target PASSED against a 0-byte Data.db — a dataset-dependent test \
must never pass on an empty dataset; see $empty_out"
fi
# Rules out the fallback this control exists for: if resolution had reached the HEALTHY
# checkout fixture, the cases would have PASSED and some sibling would have to supply
# the nonzero exit.
if grep -qE "^test result: ok\." "$empty_out"; then
  bad "empty: a 'test result: ok.' line — resolution fell back to a healthy fixture, so \
the empty one was never rejected; see $empty_out"
else
  ok "empty: the target did not report ok (no fallback to a healthy fixture)"
fi
# COUNT-BEARING, exactly as the absent case is. `must_run` is per case (#3220 forbids a
# suite-wide `ran > 0`), so "one case failed and two silently passed" is the defect
# itself, not a pass. This assertion was missing here while the file's own contract
# claimed it — roborev job 254.
if grep -qE "^test result: FAILED\. 0 passed; ${TEST_COUNT} failed" "$empty_out"; then
  ok "empty: ALL ${TEST_COUNT} cases failed — fail-closed is per case, not suite-wide"
else
  bad "empty: no 'test result: FAILED. 0 passed; ${TEST_COUNT} failed' line — some case \
did not reject the empty fixture; see $empty_out"
fi
# ONE line carrying BOTH the fixture identity and the zero-byte reader error. Two
# separate greps would only prove each string appears SOMEWHERE, which is satisfiable by
# an unrelated failure elsewhere in the output plus the fixture name in a healthy path —
# and the comment here previously CLAIMED "on one line" while the code did not check it
# (roborev job 254). That is this lane's own defect class, in the file asserting it.
if grep -qE "${FIXTURE_DIRNAME}.*Header buffer too small for parsing: 0 bytes" "$empty_out"; then
  ok "empty: ONE line carries both this fixture and the zero-byte reader rejection"
else
  bad "empty: no single line carrying both '$FIXTURE_DIRNAME' and 'Header buffer too \
small for parsing: 0 bytes' — the run failed for some OTHER reason, which does not \
prove the empty fixture was rejected; see $empty_out"
fi
if grep -q 'SKIP' "$empty_out"; then
  bad "empty: output contains 'SKIP' — an empty fixture must be a hard failure, not an \
absence; see $empty_out"
else
  ok "empty: the cases did not degrade into a SKIP"
fi

# ---------------------------------------------------------------------------
# 4. SAFETY: neither the tracked fixture nor the ambient corpus was mutated.
# ---------------------------------------------------------------------------
# `git status` is the evidence, so a git that could not ANSWER (not a repo, git missing)
# must not read as "untouched" — an unanswerable question is not a clean answer. The
# `-s` check is the independent second leg: case 3 truncates a COPY, and a staging bug
# that truncated the ORIGINAL in place would leave a 0-byte tracked file.
fixture_status=$(git -C "$REPO" status --porcelain -- "$FIXTURE_REL" 2>/dev/null)
fixture_status_rc=$?
if [ "$fixture_status_rc" -ne 0 ]; then
  bad "safety: 'git status' failed (rc=$fixture_status_rc) — the untouched-fixture check \
has no evidence and must not report green"
elif [ -n "$fixture_status" ]; then
  bad "safety: this self-test dirtied the tracked fixture $FIXTURE_REL"
elif [ ! -s "$REPO/$FIXTURE_REL/$DATA_DB" ]; then
  bad "safety: the tracked $FIXTURE_REL/$DATA_DB is now EMPTY — case 3 truncated the \
original instead of its copy"
else
  ok "safety: the git-tracked fixture is untouched (git-clean and non-empty)"
fi

# Anti-vacuity for THIS harness: `failed: 0` is only meaningful if every case actually
# asserted. A deleted case, an early `exit`, or a block skipped by an editing accident
# would otherwise report green.
EXPECTED_CHECKS=15
total_checks=$((PASS + FAIL))
if [ "$total_checks" -ne "$EXPECTED_CHECKS" ]; then
  printf 'FAIL - harness: %d assertions ran, expected %d — a dropped or '\
'short-circuited case must never read as green (update EXPECTED_CHECKS when '\
'adding one)\n' "$total_checks" "$EXPECTED_CHECKS"
  FAIL=$((FAIL + 1))
fi

# EMITTED, not merely commented. The header records this limit in source, but a reader
# sees the RUN: 15 `ok` lines and a `passed:` count convey "everything checked", and a
# caveat that lives only where a caveat-hunter looks is not a disclosure to the person
# who needs it. So the narrowing is printed on every run, affirmatively worded, and
# deliberately NOT an assertion — it is a disclosure, so it must not move PASS/FAIL or
# the EXPECTED_CHECKS count.
printf '\n%s\n' "DECLARED LIMIT (1 RECOGNISED): the 'absent' case's staging-is-surgical claim is"
printf '%s\n' "  evidenced from the FILESYSTEM (keyspace dir mirrored, one fixture withheld), NOT from"
printf '%s\n' "  this lane's own output. The sibling harness proves it from lane output because its"
printf '%s\n' "  target drives many fixtures and a sibling case's PASS line shows only one was hidden;"
printf '%s\n' "  this target has exactly ONE fixture, so that evidence does not exist here."

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
