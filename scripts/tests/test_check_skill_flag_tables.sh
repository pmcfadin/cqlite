#!/usr/bin/env bash
# test_check_skill_flag_tables.sh — self-test for the auto-loaded-skill flag-table
# drift guard (issue #3054).
#
# Every negative assertion checks not just THAT the guard failed but WHY (a distinctive
# substring of the intended diagnostic). A bare non-zero-exit assertion is vacuous: it
# passes on an unrelated silent abort, which is exactly how the first cut of this suite
# let a `set -e` bug hide the guard's most actionable message.
#
# Proves check-skill-flag-tables.sh:
#   1. PASSes on the REAL repo (the skill tables match the decoder constants today),
#   2. FAILs on a SHIFTED value — the exact pre-#3054 bug (`0x01` labeled `IS_MARKER`
#      when `0x01` is `END_OF_PARTITION`, i.e. mis-detected partition boundaries),
#   3. FAILs on an INVENTED flag name (the pre-#3054 `HAS_IS_MARKER`),
#   4. FAILs when a real source constant is documented in NO skill table (dropped row),
#   5. FAILs CLOSED when the decoder source is missing/moved (never a vacuous PASS),
#   6. FAILs CLOSED when the flag table is reformatted out of existence,
#   7. FAILs on a SHIFTED CELL value in the reference file (the cell table lives ONLY
#      there, so without this the whole CELL_* path has no mutant),
#   8. FAILs on a NAMESPACE swap — a real name+value documented under the wrong flag
#      byte (`0x01 EXTENDED_IS_STATIC` inside the ROW table),
#   9. FAILs CLOSED on a NEW unclassified source flag (a hand-maintained allow-list
#      would silently exempt it from "must be documented"),
#  10. FAILs CLOSED when a flag table drifts out from under a recognizable heading,
#  11. RESOLVES the row-flag source across its two legal homes — the constants in the
#      PRE-SPLIT `mod.rs` still PASS (the guard looks in both, #1116/#3631),
#  12. FAILs CLOSED on the #3631 regression itself: a candidate that is PRESENT but
#      declares NONE of the row flags (the campsite split moved them out from under the
#      guard, which then reported drift against an EMPTY subject set), and
#  13. FAILs CLOSED when the row-flag source resolves to a file holding almost none of
#      its subject (per-source floor — the cell source alone must not carry a pass).
# Hermetic: copies the repo's relevant files into a temp dir and mutates the COPY;
# no cargo, network, or datasets. bash 3.2 compatible.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD="$REPO_ROOT/scripts/ci/check-skill-flag-tables.sh"

if [ ! -f "$GUARD" ]; then
  echo "FAIL: guard script not found at $GUARD"
  exit 1
fi

SKILL_DIR_REL=".claude/skills/sstable-parsing"
DECODER_DIR_REL="cqlite-core/src/storage/sstable/reader/parsing/row_decoder"
SKILL_MD="$SKILL_DIR_REL/SKILL.md"
REF_MD="$SKILL_DIR_REL/cassandra5-format-reference.md"
# The row + extended flag constants' CURRENT home (they left `mod.rs` in the #3631
# campsite split, which is what broke the guard). `MOD_RS` is their PRE-SPLIT home and is
# used by assertion 11 to prove the guard still resolves them there.
ROW_RS="$DECODER_DIR_REL/row_flags.rs"
MOD_RS="$DECODER_DIR_REL/mod.rs"
CELL_RS="$DECODER_DIR_REL/cell_value.rs"

# 1. The real repo must pass.
if ! bash "$GUARD" "$REPO_ROOT" >/dev/null 2>&1; then
  echo "FAIL: guard flagged the REAL repo — the skill flag tables have drifted from the decoder constants (#3054)"
  bash "$GUARD" "$REPO_ROOT" || true
  exit 1
fi
echo "OK: real repo PASSes"

# Per-run temp sandbox with a TERMINAL-XXXXXX template (macOS mktemp substitutes only
# a trailing run of X's). Cleaned up on exit, and on Ctrl-C / termination too.
tmp="$(mktemp -d "${TMPDIR:-/tmp}/skill-flag-tables-test-XXXXXX")"
trap 'rm -rf "$tmp"' EXIT INT TERM

# Build a minimal sandbox: the guard needs the two skill files + the two decoder sources.
sandbox="$tmp/repo"
mkdir -p "$sandbox/$SKILL_DIR_REL" "$sandbox/$DECODER_DIR_REL"
cp "$REPO_ROOT/$SKILL_MD" "$sandbox/$SKILL_MD"
cp "$REPO_ROOT/$REF_MD" "$sandbox/$REF_MD"
cp "$REPO_ROOT/$ROW_RS" "$sandbox/$ROW_RS"
cp "$REPO_ROOT/$CELL_RS" "$sandbox/$CELL_RS"

# Sanity: the pristine sandbox passes, so every failure below is caused by the mutation.
if ! bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: pristine sandbox does not PASS — the sandbox is missing an input the guard needs"
  bash "$GUARD" "$sandbox" || true
  exit 1
fi
echo "OK: pristine sandbox PASSes"

restore() {
  cp "$REPO_ROOT/$SKILL_MD" "$sandbox/$SKILL_MD"
  cp "$REPO_ROOT/$REF_MD" "$sandbox/$REF_MD"
  cp "$REPO_ROOT/$ROW_RS" "$sandbox/$ROW_RS"
  cp "$REPO_ROOT/$CELL_RS" "$sandbox/$CELL_RS"
  # Assertions 11 and 12 plant a `mod.rs` (the constants' pre-split home); the pristine
  # sandbox has none, so leaving one behind would silently change every later case's
  # subject set.
  rm -f "$sandbox/$MOD_RS"
}

# Portable in-place sed (GNU sed needs no arg; BSD/macOS sed needs an empty one).
sed_i() {
  local expr="$1" file="$2"
  if sed --version >/dev/null 2>&1; then sed -i "$expr" "$file"; else sed -i '' "$expr" "$file"; fi
}

# Run the guard on the sandbox, expecting FAILURE with a specific diagnostic.
# Capture first: the guard exits non-zero here, and piping it straight into grep would
# trip `pipefail` on the guard's own (expected) failure rather than on the grep.
expect_fail_with() {
  local label="$1" needle="$2" out
  out="$(bash "$GUARD" "$sandbox" 2>&1 || true)"
  if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
    echo "FAIL: guard did NOT trip on: $label"
    exit 1
  fi
  if ! grep -q "$needle" <<<"$out"; then
    echo "FAIL: $label tripped the guard, but NOT via the intended path (expected to see: $needle)"
    echo "$out"
    exit 1
  fi
  echo "OK: $label"
  restore
}

# 2. SHIFTED value: label 0x01 as IS_MARKER (0x01 is really END_OF_PARTITION).
#    This is precisely the pre-#3054 defect that taught partition-boundary mis-detection.
sed_i 's/| `0x01` | `END_OF_PARTITION`/| `0x01` | `IS_MARKER`/' "$sandbox/$SKILL_MD"
expect_fail_with "shifted row-flag value is caught (the pre-#3054 partition-boundary bug)" \
  'FLAG VALUE DRIFT'

# 3. INVENTED flag name: the pre-#3054 `HAS_IS_MARKER`.
sed_i 's/| `0x02` | `IS_MARKER`/| `0x02` | `HAS_IS_MARKER`/' "$sandbox/$SKILL_MD"
expect_fail_with "invented flag name is caught" \
  "documents flag 'HAS_IS_MARKER'"

# 4. DROPPED row: remove ROW_HAS_COMPLEX_DELETION from BOTH skill tables.
sed_i '/`ROW_HAS_COMPLEX_DELETION`/d' "$sandbox/$SKILL_MD"
sed_i '/`ROW_HAS_COMPLEX_DELETION`/d' "$sandbox/$REF_MD"
expect_fail_with "silently-dropped flag row is caught" \
  'documented in NONE of the skill flag tables'

# 5. FAIL-CLOSED when the row-flag decoder source is GONE (a source split must not
#    yield a vacuous PASS against a stale table). No candidate is present at all here.
mv "$sandbox/$ROW_RS" "$tmp/row_flags.rs.away"
out="$(bash "$GUARD" "$sandbox" 2>&1 || true)"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard PASSED vacuously with the row-flag decoder source missing"
  exit 1
fi
grep -q 'row-flag decoder source not found' <<<"$out" || {
  echo "FAIL: missing decoder source failed, but not via the fail-closed path"; echo "$out"; exit 1; }
echo "OK: missing row-flag decoder source FAILs closed"
mv "$tmp/row_flags.rs.away" "$sandbox/$ROW_RS"
restore

# 5b. FAIL-CLOSED when the CELL decoder source is gone (its own fail-closed path).
mv "$sandbox/$CELL_RS" "$tmp/cell_value.rs.away"
out="$(bash "$GUARD" "$sandbox" 2>&1 || true)"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard PASSED vacuously with the cell decoder source missing"
  exit 1
fi
grep -q 'decoder source not found' <<<"$out" || {
  echo "FAIL: missing cell source failed, but not via the fail-closed path"; echo "$out"; exit 1; }
echo "OK: missing cell decoder source FAILs closed"
mv "$tmp/cell_value.rs.away" "$sandbox/$CELL_RS"
restore

# 6. FAIL-CLOSED when the flag table is reformatted away entirely.
sed_i '/^| `0x/d' "$sandbox/$SKILL_MD"
expect_fail_with "reformatted-away flag table FAILs closed" \
  'NO parseable flag-table row'

# 7. SHIFTED CELL value, in the REFERENCE file (the only file carrying the cell table —
#    without this mutant the entire CELL_* path is unproven).
sed_i 's/| `0x08` | `USE_ROW_TIMESTAMP`/| `0x20` | `USE_ROW_TIMESTAMP`/' "$sandbox/$REF_MD"
expect_fail_with "shifted CELL-flag value in the reference file is caught" \
  'FLAG VALUE DRIFT'

# 8. NAMESPACE swap: EXTENDED_IS_STATIC is a real constant with the real value 0x01, but
#    documented inside the ROW flag-byte table. Name and value both check out — only the
#    byte is wrong, which is the pre-#3054 confusion in its subtlest form.
sed_i 's/| `0x01` | `END_OF_PARTITION`/| `0x01` | `EXTENDED_IS_STATIC`/' "$sandbox/$SKILL_MD"
expect_fail_with "namespace swap (a real extended-byte flag documented in the row table) is caught" \
  'FLAG NAMESPACE DRIFT'

# 9. A NEW source flag the classifier does not recognize must FAIL CLOSED, not be
#    silently exempted from "must be documented". Cassandra's extended byte really does
#    define HAS_SHADOWABLE_DELETION = 0x02, so this is the realistic next addition.
printf 'const HAS_SHADOWABLE_DELETION: u8 = 0x02;\n' >>"$sandbox/$ROW_RS"
expect_fail_with "a new unclassified source flag FAILs closed (no silent allow-list exemption)" \
  "unclassified u8 constant 'HAS_SHADOWABLE_DELETION'"

# 9b. ...and the documented escape hatch works for a genuine non-flag constant.
printf 'const SOME_TUNABLE: u8 = 0x07; // flag-table-lint-ignore\n' >>"$sandbox/$ROW_RS"
if ! bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: the flag-table-lint-ignore escape hatch did not exempt a non-flag constant"
  bash "$GUARD" "$sandbox" || true
  exit 1
fi
echo "OK: flag-table-lint-ignore exempts a genuine non-flag constant"
restore

# 10. A flag table that drifts out from under a recognizable heading must FAIL, not
#     inherit a stale namespace from whatever heading happened to precede it.
sed_i 's/^\*\*Cell Flags\*\*.*/**Assorted bits**/' "$sandbox/$REF_MD"
expect_fail_with "a flag table under an unrecognized heading FAILs closed" \
  'UNRECOGNIZED section heading'

# 11. The row-flag source is RESOLVED across its two legal homes, not hard-coded. Put
#     the constants back in their PRE-SPLIT home (`mod.rs`) and delete `row_flags.rs`:
#     the guard must still PASS. Hard-coding either path alone fails one of these two
#     layouts, and hard-coding the STALE one is the #3631 failure this replaces.
mv "$sandbox/$ROW_RS" "$sandbox/$MOD_RS"
if ! bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: the guard does not resolve the row flags in their pre-split home (mod.rs) — it is still effectively hard-coded"
  bash "$GUARD" "$sandbox" || true
  exit 1
fi
echo "OK: row flags in the pre-split mod.rs still resolve"
restore

# 12. THE #3631 REGRESSION ITSELF. A candidate is PRESENT but declares NONE of the row
#     flags, because a campsite split moved them to a file this check does not know
#     about. Before the fix the guard read that file, harvested nothing from it, and
#     reported drift against an EMPTY subject set; a differently-shaped guard could just
#     as easily have PASSED over it. It must fail NAMING the resolution.
printf 'const MAX_SOMETHING: usize = 10;\n' >"$sandbox/$MOD_RS"
mv "$sandbox/$ROW_RS" "$tmp/row_flags.rs.hidden"
out="$(bash "$GUARD" "$sandbox" 2>&1 || true)"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard PASSED with the row flags moved out from under every candidate"
  exit 1
fi
grep -q 'row-flag decoder source not found' <<<"$out" || {
  echo "FAIL: a flag-less candidate tripped the guard, but not via the resolution path"; echo "$out"; exit 1; }
grep -q 'refusing to pass vacuously' <<<"$out" || {
  echo "FAIL: the resolution failure does not say it is refusing to pass vacuously"; echo "$out"; exit 1; }
mv "$tmp/row_flags.rs.hidden" "$sandbox/$ROW_RS"
echo "OK: row flags moved out from under every candidate FAILs closed (the #3631 shape)"
restore

# 13. Per-source floor: a row-flag source holding almost none of its subject must FAIL.
#     The COMBINED floor cannot be relied on for this — the cell source contributes 5
#     flags on its own, so a nearly-empty row source can hide behind a lower bar.
grep -v 'ROW_HAS_' "$sandbox/$ROW_RS" >"$tmp/row_flags.trimmed" && mv "$tmp/row_flags.trimmed" "$sandbox/$ROW_RS"
expect_fail_with "a row-flag source holding almost none of its subject FAILs the per-source floor" \
  'row/extended flag constant'

echo "PASS: test_check_skill_flag_tables.sh — all 13 assertions hold"
