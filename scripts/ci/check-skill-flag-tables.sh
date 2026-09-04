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
#   3. NAME is documented in the right NAMESPACE — row / extended / cell (`0x01` means
#      three different things across the three bytes, so a correct name+value in the
#      wrong table still teaches the pre-#3054 confusion), and
#   4. every source constant is documented at least once (catches a silently DROPPED
#      flag, which leaves an agent guessing that bit — no-heuristics mandate, #28).
#
# It is FAIL-CLOSED by construction: a missing source file, an unparseable/renamed
# constant, an unclassifiable constant, a table row under an unrecognized heading, a
# suspiciously small harvest, and a file with no parseable table row all FAIL loudly
# rather than passing vacuously. A vacuous pass would reintroduce the very bug class.
#
# Sources of truth (CQLite code = authority for what CQLite does; Cassandra 5.0.8 =
# authority for the format itself):
#   .../row_decoder/row_flags.rs   — row flags + extended flags. RESOLVED, not
#                                    hard-coded: the campsite-rule splits of epic #1116
#                                    move constants between files (these left
#                                    `row_decoder/mod.rs` in the #3631 split), and a
#                                    hard-coded path silently became a file holding NONE
#                                    of its subject. Every candidate in
#                                    ROW_SRC_CANDIDATES is searched, the ones that
#                                    actually DECLARE row/extended flag constants are
#                                    used, and finding them in NONE is a named FAILURE
#                                    listing the candidates and the anchors — never a
#                                    pass over an empty subject set. The candidate list
#                                    is deliberately scoped to the row_decoder
#                                    directory: the writer, the commitlog and the merge
#                                    path each keep their own copy of these constants,
#                                    and pinning an agent-facing READ-path table to a
#                                    WRITE-path copy would be the wrong authority.
#   .../row_decoder/cell_value.rs  — CELL_* flags, from the PRODUCTION cell decoder
#                                    (`parse_cell_value_schema_order`). NOTE: row_data.rs
#                                    has a `#[cfg(test)]` mirror carrying only 4 of the 5
#                                    flags — pinning to that test helper would let a real
#                                    production change drift unnoticed, so we do not.
#
# Escape hatch: a u8 constant in a harvested file that is genuinely NOT an on-disk flag
# can be excluded with a trailing `flag-table-lint-ignore` comment on its line. There is
# deliberately no way to make a REAL flag silently exempt.
#
# Usage: check-skill-flag-tables.sh [REPO_ROOT]
# Exit 0 = every documented pair matches source; non-zero (named reason) = drift.
# Pure bash + grep/sed/awk: no python3, cargo, network, or datasets. bash 3.2 compatible.
set -euo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

DECODER_DIR="$REPO_ROOT/cqlite-core/src/storage/sstable/reader/parsing/row_decoder"
CELL_SRC="$DECODER_DIR/cell_value.rs"

# Where the row + extended flag constants may live. Ordered current-home-first; every
# existing candidate is searched, so a split that spreads them across two files is
# handled and a future move only needs its new home added here.
ROW_SRC_CANDIDATES=(
  "$DECODER_DIR/row_flags.rs"
  "$DECODER_DIR/mod.rs"
)

SKILL_FILES=(
  ".claude/skills/sstable-parsing/SKILL.md"
  ".claude/skills/sstable-parsing/cassandra5-format-reference.md"
)

# Floor on the harvest size. Cassandra 5.0's three flag bytes define 8 row + 1 extended
# + 5 cell = 14 flags today; a parse that yields far fewer is broken, not a shrunken
# format. Deliberately below 14 so ADDING a flag never trips it.
MIN_EXPECTED_FLAGS=12

fail() { echo "::error::check-skill-flag-tables: $*"; exit 1; }

[ -f "$CELL_SRC" ] || fail "decoder source not found at $CELL_SRC (a source split may have moved it — retarget this check AND the skill citations; refusing to pass vacuously)"

# ---- 1. Harvest the real constants ----------------------------------------
# Matches e.g. `const ROW_HAS_TIMESTAMP: u8 = 0x04;`,
# `pub(super) const FOO: u8 = 0x80;` and `const END_OF_PARTITION: u8 = 0x01; // note`.
# Any visibility qualifier is accepted, so a later `pub(crate)`/`pub(super)` flag cannot
# slip past the parse. Emits "NAME 0xNN" with the hex normalized to lowercase, 2 digits.
# NOTE: lowercase ONLY the hex field — `tr` on the whole line would also lowercase the
# constant NAME and silently break every comparison.
harvest() {
  grep -v 'flag-table-lint-ignore' "$1" \
    | sed -nE 's/^[[:space:]]*(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?const[[:space:]]+([A-Z][A-Z0-9_]*)[[:space:]]*:[[:space:]]*u8[[:space:]]*=[[:space:]]*0[xX]([0-9a-fA-F]{1,2})[[:space:]]*;.*/\3 \4/p' \
    | while read -r n h; do
        h="$(tr 'ABCDEF' 'abcdef' <<<"$h")"
        [ "${#h}" -eq 1 ] && h="0$h"
        printf '%s 0x%s\n' "$n" "$h"
      done
}

# Records are "NAMESPACE NAME 0xNN". The namespace matters: `0x01` is END_OF_PARTITION in
# the row byte, EXTENDED_IS_STATIC in the extended byte, and IS_DELETED in a cell byte.
expected=""

# ---- 1a. RESOLVE the row-flag source ---------------------------------------
# A hard-coded path is how this check broke: the constants moved to `row_flags.rs`
# under the campsite rule and the check kept reading a `mod.rs` that no longer held
# any of them, reporting drift against a file with an EMPTY subject set. So the
# candidates are SEARCHED, and a candidate counts as a row-flag source only if it
# actually DECLARES one of the names the classifier below recognizes. Anything else
# — no candidate present, none declaring a flag — is a named failure.
ROW_SRCS=()
row_candidates_present=""
for cand in "${ROW_SRC_CANDIDATES[@]}"; do
  [ -f "$cand" ] || continue
  row_candidates_present+=" $cand"
  # Capture first, then match on a here-string. `grep -q` closes the pipe on its first
  # hit, and under `pipefail` the resulting SIGPIPE on the upstream stage would make a
  # MATCHING candidate look like a non-match — the permissive direction.
  cand_names="$(harvest "$cand" | awk '{print $1}')"
  if grep -qE '^(EXTENDED_[A-Z0-9_]*|ROW_HAS_[A-Z0-9_]*|END_OF_PARTITION|IS_MARKER)$' <<<"$cand_names"; then
    ROW_SRCS+=("$cand")
  fi
done

if [ "${#ROW_SRCS[@]}" -eq 0 ]; then
  fail "row-flag decoder source not found: NONE of the searched candidates declares a row/extended flag constant (expected at least one of ROW_HAS_*, END_OF_PARTITION, IS_MARKER, EXTENDED_*). Candidates searched: ${ROW_SRC_CANDIDATES[*]}. Present on disk:${row_candidates_present:- <none>}. A source split may have moved them again — add the new home to ROW_SRC_CANDIDATES AND update the skill citations; refusing to pass vacuously over an empty subject set (#3054)."
fi

ROW_SRC_DESC="${ROW_SRCS[*]}"

# Row + extended flag constants. An UNCLASSIFIED u8 constant FAILs rather than being
# silently skipped — a hand-maintained allow-list is how a newly added flag (e.g.
# Cassandra's extended-byte HAS_SHADOWABLE_DELETION 0x02) would become exempt from the
# "documented somewhere" assertion. Use the `flag-table-lint-ignore` marker for a
# genuine non-flag constant.
row_flag_count=0
for row_src in "${ROW_SRCS[@]}"; do
  while read -r name val; do
    [ -z "$name" ] && continue
    case "$name" in
      EXTENDED_*)                          expected+="extended $name $val"$'\n'; row_flag_count=$((row_flag_count + 1)) ;;
      ROW_HAS_*|END_OF_PARTITION|IS_MARKER) expected+="row $name $val"$'\n'; row_flag_count=$((row_flag_count + 1)) ;;
      *) fail "unclassified u8 constant '$name' ($val) in $row_src — this check cannot tell which flag byte it belongs to, so it cannot require the skill tables to document it. Classify it here (row/extended/cell) and document it in the skill tables, or mark the constant line 'flag-table-lint-ignore' if it is not an on-disk flag (#3054)." ;;
    esac
  done <<<"$(harvest "$row_src")"
done

# Per-source floor, not just the combined one below: the cell source alone can clear a
# combined floor, so without this a row-flag source that resolved to a nearly-empty file
# could still pass. Cassandra 5.0 defines 6 ROW_HAS_* + END_OF_PARTITION + IS_MARKER +
# EXTENDED_IS_STATIC = 9 today; deliberately below 9 so ADDING one never trips it.
MIN_EXPECTED_ROW_FLAGS=7
[ "$row_flag_count" -ge "$MIN_EXPECTED_ROW_FLAGS" ] \
  || fail "harvested only $row_flag_count row/extended flag constant(s) (floor $MIN_EXPECTED_ROW_FLAGS) from $ROW_SRC_DESC — the row-flag source resolved to a file holding almost none of its subject, which is the #3631 split's failure mode. Fix the harvest regex or add the constants' real home to ROW_SRC_CANDIDATES (#3054)."

# CELL_* constants: the skill tables document them WITHOUT the `CELL_` prefix
# (Cassandra's own Cell.java names), so strip it for comparison.
while read -r name val; do
  [ -z "$name" ] && continue
  case "$name" in
    CELL_*) expected+="cell ${name#CELL_} $val"$'\n' ;;
    *) fail "unclassified u8 constant '$name' ($val) in $CELL_SRC — classify it or mark the line 'flag-table-lint-ignore' (#3054)." ;;
  esac
done <<<"$(harvest "$CELL_SRC")"

expected="$(grep -v '^[[:space:]]*$' <<<"$expected" | sort -u || true)"
[ -n "$expected" ] || fail "harvested ZERO flag constants from the decoder source — the parse is broken, refusing to pass vacuously"

expected_count="$(grep -c . <<<"$expected" || true)"
[ "$expected_count" -ge "$MIN_EXPECTED_FLAGS" ] \
  || fail "harvested only $expected_count flag constants (floor $MIN_EXPECTED_FLAGS) from $ROW_SRC_DESC + $CELL_SRC — a half-broken parse must not pass. Fix the harvest regex or retarget the sources (#3054)."

# A shifted table is only caught if the anchors are present. Require the flags whose
# mis-assignment caused #3054, each in its expected namespace.
for must in "row END_OF_PARTITION" "row IS_MARKER" "row ROW_HAS_COMPLEX_DELETION" \
            "extended EXTENDED_IS_STATIC" "cell IS_DELETED" "cell HAS_EMPTY_VALUE"; do
  grep -q "^$must " <<<"$expected" \
    || fail "expected constant '$must' not found in the decoder source — cannot verify the #3054 anchors; retarget this check"
done

# `|| true`: a miss is a REPORTABLE finding (an invented flag name), not a reason for the
# script to abort. Without it, `set -e` + `pipefail` kills the run at the assignment and
# the actionable "documents flag X which is NOT a constant" message below never prints.
lookup() { grep -m1 "^$1 $2 " <<<"$expected" | awk '{print $3}' || true; }
lookup_any_ns() { grep -m1 " $1 " <<<"$expected" | awk '{print $1}' || true; }

# ---- 2. Check every documented pair --------------------------------------
documented=""
rows_seen=0
errors=0

for rel in "${SKILL_FILES[@]}"; do
  path="$REPO_ROOT/$rel"
  [ -f "$path" ] || fail "skill file not found at $path"

  # Single pass: track the nearest preceding heading (markdown `#`/`**bold**` lead-in) so
  # each flag row is checked against the flag BYTE its section is about.
  section=""
  ns=""
  lineno=0
  file_rows=0
  while IFS= read -r line || [ -n "$line" ]; do
    lineno=$((lineno + 1))
    case "$line" in
      '#'*|'**'*)
        section="$line"
        # Order matters: an "EXTENDED flag byte" heading also mentions "(Row)".
        lower="$(tr 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' 'abcdefghijklmnopqrstuvwxyz' <<<"$line")"
        case "$lower" in
          *extended*flag*) ns=extended ;;
          *cell*flag*)     ns=cell ;;
          *main*flag*|*"flag bytes (row)"*) ns=row ;;
          # Any other heading CLEARS the namespace: a flag table that drifts away from a
          # recognizable heading must FAIL, not inherit a stale namespace.
          *) ns="" ;;
        esac
        continue
        ;;
    esac

    # Markdown flag-table row: | `0xNN` | `NAME` | description |
    row="$(sed -nE 's/^\|[[:space:]]*`0[xX]([0-9a-fA-F]{1,2})`[[:space:]]*\|[[:space:]]*`([A-Z][A-Z0-9_]*)`[[:space:]]*\|.*/\1 \2/p' <<<"$line")"
    [ -z "$row" ] && continue

    hex="$(awk '{print $1}' <<<"$row" | tr 'ABCDEF' 'abcdef')"
    [ "${#hex}" -eq 1 ] && hex="0$hex"
    doc_val="0x$hex"
    name="$(awk '{print $2}' <<<"$row")"
    rows_seen=$((rows_seen + 1))
    file_rows=$((file_rows + 1))

    if [ -z "$ns" ]; then
      echo "::error::check-skill-flag-tables: $rel:$lineno documents flag '$name' ($doc_val) under an UNRECOGNIZED section heading '${section:-<none>}'."
      echo "         \`0x01\` means END_OF_PARTITION (row byte), EXTENDED_IS_STATIC (extended byte), or IS_DELETED (cell byte) — this check cannot verify a row it cannot attribute to a byte."
      echo "         Put the table under a heading naming its byte (\"main flag byte\", \"EXTENDED flag byte\", \"Cell Flags\") or teach this check the new heading (#3054)."
      errors=$((errors + 1))
      continue
    fi

    documented+="$ns $name"$'\n'

    src_val="$(lookup "$ns" "$name")"
    if [ -z "$src_val" ]; then
      other_ns="$(lookup_any_ns "$name")"
      if [ -n "$other_ns" ]; then
        echo "::error::check-skill-flag-tables: FLAG NAMESPACE DRIFT in $rel:$lineno — '$name' is documented in the '$ns' flag-byte table, but it is a '$other_ns'-byte flag."
        echo "         The same bit value means different things in the row / extended / cell bytes; documenting a flag under the wrong byte teaches the pre-#3054 confusion."
      else
        echo "::error::check-skill-flag-tables: $rel:$lineno documents flag '$name' ($doc_val) which is NOT a constant in the decoder source."
        echo "         Either it is INVENTED (e.g. the pre-#3054 HAS_IS_MARKER / HAS_NULL_VALUE / EXTENDED_FLAG) or the constant was renamed."
        echo "         Authority: $ROW_SRC_DESC and $CELL_SRC."
      fi
      errors=$((errors + 1))
      continue
    fi
    if [ "$doc_val" != "$src_val" ]; then
      echo "::error::check-skill-flag-tables: FLAG VALUE DRIFT in $rel:$lineno — '$name' is documented as $doc_val but the source constant is $src_val."
      echo "         An auto-loaded skill table that mis-assigns a flag bit teaches a decode bug to every agent (#3054)."
      errors=$((errors + 1))
    fi
  done <"$path"

  if [ "$file_rows" -eq 0 ]; then
    fail "$rel contains NO parseable flag-table row (| \`0xNN\` | \`NAME\` | … |). The flag table is the whole point of this file — if it was reformatted, update this check in the same change (#3054)."
  fi
done

# ---- 3. No source constant silently dropped ------------------------------
documented="$(grep -v '^[[:space:]]*$' <<<"$documented" | sort -u || true)"
while read -r ns name _val; do
  [ -z "$name" ] && continue
  grep -qx "$ns $name" <<<"$documented" || {
    echo "::error::check-skill-flag-tables: flag constant '$name' ($ns byte) exists in the decoder source but is documented in NONE of the skill flag tables."
    echo "         A missing row leaves an agent guessing that bit (no-heuristics mandate, #28). Add it with its citation."
    errors=$((errors + 1))
  }
done <<<"$expected"

if [ "$errors" -gt 0 ]; then
  echo "::error::check-skill-flag-tables: $errors flag-table drift error(s). Fix the skill table(s) against the decoder source (#3054)."
  exit 1
fi

echo "OK: $rows_seen documented flag rows across ${#SKILL_FILES[@]} skill file(s) match the decoder constants (namespace-checked); all $expected_count source flags are documented."
