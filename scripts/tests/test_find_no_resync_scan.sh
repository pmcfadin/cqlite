#!/usr/bin/env bash
#
# Issue #4205 (find-key-probe, spec F6) — `probe_generation` reads ONLY decoded, authoritative
# structures (the bloom filter, `Index.db` entries, the BTI trie, the seek-decoded partition,
# and the production schema-driven compaction scan as its fail-safe) — NEVER a byte-pattern
# search of `Data.db`. This guard greps every PRODUCTION file of the `find_probe` module for a
# resync primitive outside test code and FAILs naming the line. Same shape as
# `test_verify_location_no_resync_scan.sh` (#4194): flat `find_probe*.rs` siblings AND a
# `find_probe/` module directory are discovered, test code is excluded by name and by an
# in-file `#[cfg(test)]` brace tracker, and a guard that scanned NOTHING is a REFUSAL.
# Registered in the UNSCOPED `roborev-lints` component for the reason that script documents.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSTABLE_DIR="$REPO_ROOT/cqlite-core/src/storage/sstable/reader/data_access"

if [ ! -d "$SSTABLE_DIR" ]; then
  echo "FAIL - sstable module directory not found: $SSTABLE_DIR"
  exit 1
fi

# Literal byte-pattern search primitives F6 forbids in production location
# code — the exact "resync by scanning for a plausible header" shape
# `sstable-salvage`'s R4.3 guard exists to catch, reused here verbatim.
PATTERNS=("memchr" ".windows(" "find(|" "position(|")

# The waiver KEY for each pattern, so a marker can waive one primitive without
# blanket-exempting the line (roborev job 108). Keys are stable names, not the
# raw pattern text, so a marker never has to embed punctuation the guard
# itself greps for.
#
# A `case` FUNCTION, not a `declare -A` associative array: associative arrays
# are bash 4+, macOS ships bash 3.2, and macOS is a first-class development
# host for --lite/--only. The gate's own portability guard (portability-8c)
# catches this, and caught exactly this regression here.
pattern_key() {
  case "$1" in
    "memchr") printf 'memchr' ;;
    ".windows(") printf 'windows' ;;
    "find(|") printf 'find' ;;
    "position(|") printf 'position' ;;
    # An unmapped pattern must NOT become waivable-by-empty-marker: emit a
    # sentinel no marker can match rather than an empty string.
    *) printf 'UNMAPPED-PATTERN' ;;
  esac
}

hits=0
allowed=0
scanned=0
scanned_names=""

scan_file() {
  local file="$1"
  # Exclude test code by NAME before reading a byte (see the header).
  case "$file" in
    */tests/* | *_tests.rs) return 0 ;;
  esac
  scanned=$((scanned + 1))
  scanned_names="$scanned_names ${file#"$REPO_ROOT"/}"

  local in_test_mod=0 seen_open=0 depth=0 line_no=0
  local line opens closes stripped pat key
  while IFS= read -r line; do
    line_no=$((line_no + 1))
    if [ "$in_test_mod" -eq 1 ]; then
      opens=$(grep -o '{' <<<"$line" | wc -l)
      closes=$(grep -o '}' <<<"$line" | wc -l)
      depth=$((depth + opens - closes))
      if [ "$depth" -gt 0 ]; then
        seen_open=1
      fi
      if [ "$seen_open" -eq 1 ] && [ "$depth" -le 0 ]; then
        in_test_mod=0
      elif [ "$seen_open" -eq 0 ] && [[ "$line" == *';'* ]]; then
        # A brace-LESS `#[cfg(test)]` item (a gated `use`/`const`/`type`, or
        # the `#[path = ".."] mod tests;` declaration this module uses) ends
        # at its own `;` with no `{` ever appearing. Gated on `seen_open == 0`
        # so a `;` inside an already-open block never closes tracking early.
        in_test_mod=0
      fi
      continue
    fi
    # ANCHORED match on a line that, with spaces AND tabs stripped, is EXACTLY
    # `#[cfg(test)]` — a bare substring match would also fire inside a doc
    # comment or string literal mentioning the attribute, silently disabling
    # the guard for the rest of the file, and this module's docs do quote cfg
    # attributes.
    stripped="${line// /}"
    stripped="${stripped//$'\t'/}"
    if [ "$stripped" = '#[cfg(test)]' ]; then
      in_test_mod=1
      seen_open=0
      depth=0
      continue
    fi
    # LINE-LEVEL OPT-OUT (roborev job 102 LOW), PER-PATTERN (roborev job 108).
    # A guard that constrains SYNTAX rather than behaviour needs an escape
    # hatch, or the next legitimate slice-pairs call over a slice of boundary
    # tuples — which has nothing to do with scanning `Data.db` bytes — FAILs
    # --lite with no way out. The marker names the SINGLE pattern it waives
    # (`no-resync-scan-allow:<key>`), so a line carrying both a legitimate
    # call and an illegitimate `memchr` is still caught; and an allowed line
    # is still COUNTED and reported, so the opt-out is visible, never silent.
    for pat in "${PATTERNS[@]}"; do
      if [[ "$line" == *"$pat"* ]]; then
        key="$(pattern_key "$pat")"
        if [[ "$line" == *"no-resync-scan-allow:$key"* ]]; then
          allowed=$((allowed + 1))
          continue
        fi
        echo "FAIL - byte-pattern search primitive '$pat' outside tests: $file:$line_no: $line"
        hits=$((hits + 1))
      fi
    done
  done <"$file"

  if [ "$in_test_mod" -eq 1 ]; then
    echo "FAIL - $file: brace tracker for a #[cfg(test)] block never returned to depth 0 by \
EOF (unbalanced or miscounted braces) — refusing rather than silently trusting the scan"
    hits=$((hits + 1))
  fi
}

# Shape 1: flat `find_probe*.rs` siblings.
while IFS= read -r -d '' f; do
  scan_file "$f"
done < <(find "$SSTABLE_DIR" -maxdepth 1 -name 'find_probe*.rs' -print0)

# Shape 2: a `find_probe/` module directory.
if [ -d "$SSTABLE_DIR/find_probe" ]; then
  while IFS= read -r -d '' f; do
    scan_file "$f"
  done < <(find "$SSTABLE_DIR/find_probe" -name '*.rs' -print0)
fi

if [ "$scanned" -eq 0 ]; then
  echo "FAIL - 0 production file(s) scanned: no \`find_probe*.rs\` sibling and no \
\`find_probe/\` module directory under $SSTABLE_DIR holds a non-test .rs file. The module \
was renamed, moved or removed out from under this guard — a guard that scanned nothing cannot \
certify a 'no byte-pattern search primitive' verdict, so this is a REFUSAL, not a pass (spec F6)"
  exit 1
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found outside tests (spec F6)"
  exit 1
fi

echo "ok   - $scanned production file(s) scanned (${scanned_names# }), \
0 byte-pattern-search hit(s) RECOGNISED outside tests, $allowed line(s) RECOGNISED as \
no-resync-scan-allow (spec F6)"
exit 0
