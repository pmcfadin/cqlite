#!/usr/bin/env bash
# Regression test for issue #3220 AC2: the point-vs-full differential lane must FAIL
# — never skip green — in BOTH fixture-absence directions.
#
# POSITIVE CONTROL is the point of this file. #3220 existed because the lane's
# green was never proven to be a DECISION: with `CQLITE_DATASETS_ROOT=/data/datasets`
# (a corpus holding `test_da/` but not the git-committed
# `test_da/multiclustering_table-*`), the #3032 AC6 case skipped silently while the
# suite reported PASS. So a green run alone means nothing here; each case below
# stages a layout the lane MUST reject, and asserts the rejection text.
#
# The three cases:
#   control  — the ambient corpus: the lane PASSES and the committed BTI case RUNS.
#              Without this, the two failures below could equally be a broken harness.
#   absent   — NO candidate root holds `multiclustering_table-*/…-Data.db`: the
#              unconditional `must_run` guard FAILs (a resolver that falls back to the
#              checkout must still fail loudly when neither root has the table).
#   empty    — the fixture dir and its `-Data.db` EXIST but the file is empty (0 rows):
#              the anti-vacuous slice-count anchors FAIL rather than comparing 0 == 0
#              ("never let a dataset-dependent test pass on an empty dataset").
#
# Safety: nothing here mutates `$CQLITE_DATASETS_ROOT` or the git-tracked fixture —
# every staged root is a temp dir, and case 4 re-asserts the tracked fixture is
# untouched (a self-test that dirtied the worktree would also trip the gate's mid-run
# tree-integrity check).
#
# Run standalone:   bash scripts/tests/test_point_vs_full_failclosed.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside `bti-multiclustering`, where
#                   the test binary it drives is already built.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

FIXTURE_REL="test-data/datasets/sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf"
DATA_DB="da-2-bti-Data.db"
TEST_TARGET="point_vs_full_differential"
TEST_NAME="point_vs_full_differential_equality"

# #2751 defense-in-depth: never clobber an inherited gate summary path.
unset AGENT_GATE_SUMMARY_FILE
# The guard under test is the UNCONDITIONAL one, so an inherited
# CQLITE_REQUIRE_FIXTURES must not decide any case (it would also fail the control on
# a machine without the fetched corpus).
unset CQLITE_REQUIRE_FIXTURES
# Scrub the seam this file itself drives, so a stale export cannot pre-decide a case.
unset CQLITE_TEST_CHECKOUT_SSTABLES_ROOT

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Scratch root, VALIDATED before anything is built under it and before the cleanup
# trap is armed: this script runs without `errexit` (every case must run, so one
# failure cannot hide the rest), so an unchecked `mktemp -d` would leave `$tmp` empty,
# resolve every derived path under `/`, and hand the EXIT trap an `rm -rf ""`.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/pvf-failclosed.XXXXXX") || {
  echo "FATAL: mktemp -d failed; refusing to run with an unset scratch root" >&2
  exit 1
}
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  echo "FATAL: mktemp -d produced no usable directory ('$tmp'); refusing to run" >&2
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT

# The committed fixture must be there for ANY of this to mean anything: the lane's
# expected behavior in the control case is "it runs", and both failure cases are
# defined relative to it. An absent fixture is a hard error, not a skip (#3032).
if [ ! -f "$REPO/$FIXTURE_REL/$DATA_DB" ]; then
  echo "FATAL: committed fixture $FIXTURE_REL/$DATA_DB is missing from the checkout;" >&2
  echo "       remedy: git -C $REPO restore --source=HEAD -- test-data/datasets" >&2
  exit 1
fi

# Run the lane with an explicit environment. Prints nothing; the caller greps $out.
run_lane() {
  local out=$1; shift
  # `env -u` clears the two seams unless the case re-supplies them, so no case can
  # inherit another's staging.
  ( cd "$REPO" && env -u CQLITE_DATASETS_ROOT -u CQLITE_TEST_CHECKOUT_SSTABLES_ROOT "$@" \
      cargo test -p cqlite-core --features "state_machine cli-helpers" \
        --test "$TEST_TARGET" "$TEST_NAME" -- --nocapture ) >"$out" 2>&1
  return $?
}

# ---------------------------------------------------------------------------
# 1. CONTROL: the ambient corpus. The lane passes AND the committed case RUNS.
# ---------------------------------------------------------------------------
control_out="$tmp/control.log"
control_env=()
[ -n "${CQLITE_DATASETS_ROOT:-}" ] && control_env=(CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT")
run_lane "$control_out" "${control_env[@]}"
control_rc=$?
if [ "$control_rc" -eq 0 ]; then
  ok "control: the lane passes against the ambient corpus"
else
  bad "control: the lane FAILED against the ambient corpus (rc=$control_rc) — the two \
failure cases below prove nothing until this passes; see $control_out"
  sed -n '1,40p' "$control_out"
fi
if grep -q "PASS test_da.multiclustering_table" "$control_out"; then
  ok "control: the committed BTI case actually RAN (not skipped behind its siblings)"
else
  bad "control: no 'PASS test_da.multiclustering_table' line — the #3220 case did not run"
fi

# ---------------------------------------------------------------------------
# 2. ABSENT: no candidate root carries THIS table — while every other fixture
#    stays resolvable.
#
#    The staging is deliberately SURGICAL, not "hide the whole corpus". Hiding
#    everything makes every case skip, so the guard fires for some other committed
#    case and the assertion passes even with `multiclustering_table`'s `must_run`
#    removed — i.e. exactly the regression this file exists to catch would slip
#    through (measured: a coarse staging did not discriminate). So the checkout
#    candidate is mirrored by SYMLINK, table by table, minus the one fixture, and the
#    expected message is the EXACT one-element list.
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
      multiclustering_table-*) continue ;;   # the ONLY thing hidden
    esac
    ln -s "${tbl_dir%/}" "$absent_checkout/$ks_name/$tbl_name"
  done
done
if [ -d "$absent_checkout/test_da" ] \
   && [ -n "$(ls -A "$absent_checkout/test_da" 2>/dev/null)" ] \
   && [ ! -e "$absent_checkout/test_da/$(basename "$FIXTURE_REL")" ]; then
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
  ok "absent: the lane FAILS when no candidate root holds the fixture (rc=$absent_rc)"
else
  bad "absent: the lane PASSED with the fixture absent from every candidate root — \
this is the #3220 silent-skip defect; see $absent_out"
fi
if grep -q 'committed-fixture case(s) did NOT run: \["multiclustering_table"\]' "$absent_out"; then
  ok "absent: the unconditional must_run guard names EXACTLY the hidden case"
else
  bad "absent: no 'did NOT run: [\"multiclustering_table\"]' — the guard did not fire \
for the hidden fixture (a failure for any other reason does not prove the contract); \
see $absent_out"
fi
# The sibling committed case still ran in that same run: the failure above is the
# guard firing for the hidden fixture, not the corpus being unavailable.
if grep -q "PASS test_da.wide_table" "$absent_out"; then
  ok "absent: sibling committed cases still ran (the staging hid one fixture only)"
else
  bad "absent: test_da.wide_table did not run — the staging hid more than intended, so \
the guard's target is ambiguous; see $absent_out"
fi
# The keyspace dir EXISTS in the staged checkout, so this case also pins the specific
# defect: keyspace-granular selection would have accepted that root.
if grep -q "no \*-Data.db for test_da.multiclustering_table under any candidate" "$absent_out"; then
  ok "absent: resolution reports searching EVERY candidate root, per table"
else
  bad "absent: no per-table 'under any candidate' diagnostic; see $absent_out"
fi

# ---------------------------------------------------------------------------
# 3. EMPTY: the fixture is present but carries no rows.
# ---------------------------------------------------------------------------
empty_root="$tmp/empty/sstables/test_da"
mkdir -p "$empty_root"
cp -r "$REPO/$FIXTURE_REL" "$empty_root/"
: > "$empty_root/$(basename "$FIXTURE_REL")/$DATA_DB"
empty_out="$tmp/empty.log"
run_lane "$empty_out" CQLITE_DATASETS_ROOT="$tmp/empty"
empty_rc=$?
if [ "$empty_rc" -ne 0 ]; then
  ok "empty: the lane FAILS on a present-but-empty fixture (rc=$empty_rc)"
else
  bad "empty: the lane PASSED against a 0-byte Data.db — a dataset-dependent test \
must never pass on an empty dataset; see $empty_out"
fi
if grep -q "SKIP.*multiclustering_table" "$empty_out"; then
  bad "empty: the case SKIPped instead of failing — an empty fixture must be a hard \
failure, not an absence; see $empty_out"
else
  ok "empty: the case did not degrade into a SKIP"
fi
if grep -qE "must yield exactly|no partition keys to probe|multiclustering_table" "$empty_out"; then
  ok "empty: the failure is attributed to the multiclustering case"
else
  bad "empty: the failure does not mention the multiclustering case; see $empty_out"
fi

# ---------------------------------------------------------------------------
# 4. SAFETY: neither the tracked fixture nor the ambient corpus was mutated.
# ---------------------------------------------------------------------------
if [ -z "$(git -C "$REPO" status --porcelain -- "$FIXTURE_REL" 2>/dev/null)" ]; then
  ok "safety: the git-tracked fixture is untouched"
else
  bad "safety: this self-test dirtied the tracked fixture $FIXTURE_REL"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
