#!/usr/bin/env bash
# check-skill-flag-tables.sh — mechanize the #3054 anti-rot invariant for the
# AUTO-LOADED agent skill flag tables.
#
# WHY: `.claude/skills/sstable-parsing/` enters agent context automatically for any
# binary-format work, and an agent trusts a skill table over reading the code. Before
# #3054 those tables claimed `0x01 = HAS_IS_MARKER` and `0x40 = IS_STATIC` — i.e. they
# taught the agent to mis-detect PARTITION BOUNDARIES (`0x01` is `END_OF_PARTITION`) and
# invented cell flags (`0x20 HAS_NULL_VALUE`, `0x40 EXTENDED_FLAG`) that do not exist in
# Cassandra 5.0. A wrong table is strictly worse than no table. This check pins every
# documented name->value pair to the REAL constant in the decoder source, so the class of
# rot cannot recur silently.
#
# WHAT it asserts, for each markdown flag-table row `| `0xNN` | `NAME` | ... |` in the
# skill files below:
#   1. NAME is a real flag constant in the decoder source (catches INVENTED flags), and
#   2. its documented 0xNN value equals the source constant's value (catches SHIFTED /
#      INVERTED tables), and
#   3. every source constant is documented at least once across the skill files (catches
#      a silently DROPPED flag).
#
# Sources of truth (CQLite code = authority for what CQLite does; Cassandra 5.0.8 =
# authority for the format itself):
#   cqlite-core/src/storage/sstable/reader/parsing/row_decoder/mod.rs   (row + extended flags)
#   cqlite-core/src/storage/sstable/reader/parsing/row_decoder/row_data.rs (CELL_* flags)
#
# Usage: check-skill-flag-tables.sh [REPO_ROOT]
# Exit 0 = every documented pair matches source; non-zero (named reason) = drift.
# Pure bash + grep/sed: no python3, cargo, network, or datasets.
set -euo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

ROW_SRC="$REPO_ROOT/cqlite-core/src/storage/sstable/reader/parsing/row_decoder/mod.rs"
CELL_SRC="$REPO_ROOT/cqlite-core/src/storage/sstable/reader/parsing/row_decoder/row_data.rs"

SKILL_FILES=(
  ".claude/skills/sstable-parsing/SKILL.md"
  ".claude/skills/sstable-parsing/cassandra5-format-reference.md"
)

fail() { echo "::error::check-skill-flag-tables: $*"; exit 1; }

for f in "$ROW_SRC" "$CELL_SRC"; do
  [ -f "$f" ] || fail "decoder source not found at $f (a source split may have moved it — retarget this check AND the skill citations)"
done

# ---- 1. Harvest the real constants ----------------------------------------
# Matches e.g. `const ROW_HAS_TIMESTAMP: u8 = 0x04;` and
# `const END_OF_PARTITION: u8 = 0x01; // comment`.
# Emits "NAME 0xNN" lines with the hex normalized to lowercase, 2 digits.
# NOTE: lowercase ONLY the hex field — `tr` on the whole line would also lowercase the
# constant NAME and silently break every comparison.
harvest() {
  sed -nE 's/^[[:space:]]*(pub[[:space:]]+)?const[[:space:]]+([A-Z][A-Z0-9_]*)[[:space:]]*:[[:space:]]*u8[[:space:]]*=[[:space:]]*0[xX]([0-9a-fA-F]{1,2})[[:space:]]*;.*/\2 \3/p' "$1" \
    | while read -r n h; do
        h="$(tr 'ABCDEF' 'abcdef' <<<"$h")"
        [ "${#h}" -eq 1 ] && h="0$h"
        printf '%s 0x%s\n' "$n" "$h"
      done
}

expected=""
# Row + extended flag constants. CELL_* live in row_data.rs and are harvested below.
while read -r name val; do
  [ -z "$name" ] && continue
  case "$name" in
    ROW_HAS_*|END_OF_PARTITION|IS_MARKER|EXTENDED_IS_STATIC) expected+="$name $val"$'\n' ;;
  esac
done <<<"$(harvest "$ROW_SRC")"

# CELL_* constants: the skill tables document them WITHOUT the `CELL_` prefix
# (Cassandra's own Cell.java names), so strip it for comparison.
while read -r name val; do
  [ -z "$name" ] && continue
  case "$name" in
    CELL_*) expected+="${name#CELL_} $val"$'\n' ;;
  esac
done <<<"$(harvest "$CELL_SRC")"

# Documented-by-Cassandra-only exception. HAS_EMPTY_VALUE (0x04) is a real Cassandra 5.0
# cell mask (`db/rows/Cell.java:264` HAS_EMPTY_VALUE_MASK) that CQLite handles without a
# named `const` (see row_data.rs:282). Pinned here with its authority so the skill table
# may document it; every OTHER name must come from a source constant.
expected+="HAS_EMPTY_VALUE 0x04"$'\n'

expected="$(grep -v '^[[:space:]]*$' <<<"$expected" | sort -u)"
[ -n "$expected" ] || fail "harvested ZERO flag constants from the decoder source — the parse is broken, refusing to pass vacuously"

# A shifted table is only caught if the anchors are present. Require the flags whose
# mis-assignment caused #3054.
for must in END_OF_PARTITION IS_MARKER ROW_HAS_COMPLEX_DELETION EXTENDED_IS_STATIC; do
  grep -q "^$must " <<<"$expected" \
    || fail "expected constant $must not found in $ROW_SRC — cannot verify the #3054 anchors; retarget this check"
done

lookup() { grep -m1 "^$1 " <<<"$expected" | awk '{print $2}'; }

# ---- 2. Check every documented pair --------------------------------------
documented=""
rows_seen=0
errors=0

for rel in "${SKILL_FILES[@]}"; do
  path="$REPO_ROOT/$rel"
  [ -f "$path" ] || fail "skill file not found at $path"

  # Markdown flag-table rows: | `0xNN` | `NAME` | description |
  mapfile -t rows < <(sed -nE 's/^\|[[:space:]]*`0[xX]([0-9a-fA-F]{1,2})`[[:space:]]*\|[[:space:]]*`([A-Z][A-Z0-9_]*)`[[:space:]]*\|.*/\1 \2/p' "$path" || true)

  if [ "${#rows[@]}" -eq 0 ]; then
    fail "$rel contains NO parseable flag-table row (| \`0xNN\` | \`NAME\` | … |). The row-flag table is the whole point of this file — if it was reformatted, update this check in the same change (#3054)."
  fi

  for row in "${rows[@]}"; do
    hex="$(awk '{print $1}' <<<"$row" | tr 'ABCDEF' 'abcdef')"
    [ "${#hex}" -eq 1 ] && hex="0$hex"
    doc_val="0x$hex"
    name="$(awk '{print $2}' <<<"$row")"
    rows_seen=$((rows_seen + 1))
    documented+="$name"$'\n'

    src_val="$(lookup "$name")"
    if [ -z "$src_val" ]; then
      echo "::error::check-skill-flag-tables: $rel documents flag '$name' ($doc_val) which is NOT a constant in the decoder source."
      echo "         Either it is INVENTED (e.g. the pre-#3054 HAS_IS_MARKER / HAS_NULL_VALUE / EXTENDED_FLAG) or the constant was renamed."
      echo "         Authority: $ROW_SRC and $CELL_SRC."
      errors=$((errors + 1))
      continue
    fi
    if [ "$doc_val" != "$src_val" ]; then
      echo "::error::check-skill-flag-tables: FLAG VALUE DRIFT in $rel — '$name' is documented as $doc_val but the source constant is $src_val."
      echo "         An auto-loaded skill table that mis-assigns a flag bit teaches a decode bug to every agent (#3054)."
      errors=$((errors + 1))
    fi
  done
done

# ---- 3. No source constant silently dropped ------------------------------
documented="$(grep -v '^[[:space:]]*$' <<<"$documented" | sort -u)"
while read -r name _val; do
  [ -z "$name" ] && continue
  grep -qx "$name" <<<"$documented" || {
    echo "::error::check-skill-flag-tables: flag constant '$name' exists in the decoder source but is documented in NONE of the skill flag tables."
    echo "         A missing row leaves an agent guessing that bit (no-heuristics mandate, #28). Add it with its citation."
    errors=$((errors + 1))
  }
done <<<"$expected"

if [ "$errors" -gt 0 ]; then
  echo "::error::check-skill-flag-tables: $errors flag-table drift error(s). Fix the skill table(s) against the decoder source (#3054)."
  exit 1
fi

echo "OK: $rows_seen documented flag rows across ${#SKILL_FILES[@]} skill file(s) match the decoder constants; all $(wc -l <<<"$expected" | tr -d ' ') source flags are documented."
