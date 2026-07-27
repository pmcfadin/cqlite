#!/usr/bin/env bash
# test_check_skill_flag_tables.sh — self-test for the auto-loaded-skill flag-table
# drift guard (issue #3054).
#
# Proves check-skill-flag-tables.sh:
#   1. PASSes on the REAL repo (the skill tables match the decoder constants today),
#   2. FAILs on a SHIFTED value — the exact pre-#3054 bug (`0x01` labeled `IS_MARKER`
#      when `0x01` is `END_OF_PARTITION`, i.e. mis-detected partition boundaries),
#   3. FAILs on an INVENTED flag name (the pre-#3054 `HAS_IS_MARKER`),
#   4. FAILs when a real source constant is documented in NO skill table (dropped row),
#   5. FAILs CLOSED when the decoder source is missing/moved (never a vacuous PASS),
#   6. FAILs CLOSED when the flag table is reformatted out of existence.
# Hermetic: copies the repo's relevant subtrees into a temp dir and mutates the COPY;
# no cargo, network, or datasets.
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

# 1. The real repo must pass.
if ! bash "$GUARD" "$REPO_ROOT" >/dev/null 2>&1; then
  echo "FAIL: guard flagged the REAL repo — the skill flag tables have drifted from the decoder constants (#3054)"
  bash "$GUARD" "$REPO_ROOT" || true
  exit 1
fi
echo "OK: real repo PASSes"

# Per-run temp sandbox with a TERMINAL-XXXXXX template (macOS mktemp substitutes only
# a trailing run of X's). Cleaned up on exit.
tmp="$(mktemp -d "${TMPDIR:-/tmp}/skill-flag-tables-test-XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

# Build a minimal sandbox: the guard needs the two skill files + the two decoder sources.
sandbox="$tmp/repo"
mkdir -p "$sandbox/$SKILL_DIR_REL" "$sandbox/$DECODER_DIR_REL" "$sandbox/scripts/ci"
cp "$REPO_ROOT/$SKILL_MD" "$sandbox/$SKILL_MD"
cp "$REPO_ROOT/$REF_MD" "$sandbox/$REF_MD"
cp "$REPO_ROOT/$DECODER_DIR_REL/mod.rs" "$sandbox/$DECODER_DIR_REL/mod.rs"
cp "$REPO_ROOT/$DECODER_DIR_REL/row_data.rs" "$sandbox/$DECODER_DIR_REL/row_data.rs"

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
  cp "$REPO_ROOT/$DECODER_DIR_REL/mod.rs" "$sandbox/$DECODER_DIR_REL/mod.rs"
}

# Portable in-place sed (GNU sed needs no arg; BSD/macOS sed needs an empty one).
sed_i() {
  local expr="$1" file="$2"
  if sed --version >/dev/null 2>&1; then sed -i "$expr" "$file"; else sed -i '' "$expr" "$file"; fi
}

# 2. SHIFTED value: label 0x01 as IS_MARKER (0x01 is really END_OF_PARTITION).
#    This is precisely the pre-#3054 defect that taught partition-boundary mis-detection.
sed_i 's/| `0x01` | `END_OF_PARTITION`/| `0x01` | `IS_MARKER`/' "$sandbox/$SKILL_MD"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a shifted flag value (0x01 labeled IS_MARKER)"
  exit 1
fi
# Capture first: the guard exits non-zero here, and piping it straight into grep would
# trip `pipefail` on the guard's own (expected) failure rather than on the grep.
shift_out="$(bash "$GUARD" "$sandbox" 2>&1 || true)"
if ! grep -q 'FLAG VALUE DRIFT' <<<"$shift_out"; then
  echo "FAIL: shifted value tripped the guard but not via the FLAG VALUE DRIFT path"
  echo "$shift_out"
  exit 1
fi
echo "OK: shifted flag value is caught (the pre-#3054 partition-boundary bug)"
restore

# 3. INVENTED flag name: the pre-#3054 `HAS_IS_MARKER`.
sed_i 's/| `0x02` | `IS_MARKER`/| `0x02` | `HAS_IS_MARKER`/' "$sandbox/$SKILL_MD"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on an invented flag name (HAS_IS_MARKER)"
  exit 1
fi
echo "OK: invented flag name is caught"
restore

# 4. DROPPED row: remove ROW_HAS_COMPLEX_DELETION from BOTH skill tables.
sed_i '/`ROW_HAS_COMPLEX_DELETION`/d' "$sandbox/$SKILL_MD"
sed_i '/`ROW_HAS_COMPLEX_DELETION`/d' "$sandbox/$REF_MD"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip when a real source constant is documented nowhere"
  exit 1
fi
echo "OK: silently-dropped flag row is caught"
restore

# 5. FAIL-CLOSED when the decoder source moved (a source split must not yield a
#    vacuous PASS against a stale table).
mv "$sandbox/$DECODER_DIR_REL/mod.rs" "$tmp/mod.rs.away"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard PASSED vacuously with the decoder source missing"
  exit 1
fi
echo "OK: missing decoder source FAILs closed"
mv "$tmp/mod.rs.away" "$sandbox/$DECODER_DIR_REL/mod.rs"
restore

# 6. FAIL-CLOSED when the flag table is reformatted away entirely.
sed_i '/^| `0x/d' "$sandbox/$SKILL_MD"
if bash "$GUARD" "$sandbox" >/dev/null 2>&1; then
  echo "FAIL: guard PASSED with no parseable flag-table row (a reformat must fail closed)"
  exit 1
fi
echo "OK: reformatted-away flag table FAILs closed"
restore

echo "PASS: test_check_skill_flag_tables.sh — all 6 assertions hold"
