#!/usr/bin/env bash
#
# Issue #4194 (corruption-locator, spec verify-location L3) — partition
# location resolution reads ONLY the boundary source's own decoded entries
# (`Index.db`'s `PartitionIndexEntry`s, the BTI trie's decoded leaves) and the
# `CompressionInfo.db`/`CRC.db` chunk tables — NEVER by scanning `Data.db`
# bytes for a plausible partition header. This guard greps every PRODUCTION
# file of the `verify_location` module for any byte-pattern search primitive
# OUTSIDE test code and FAILs naming the line if one is found. Modeled on
# `sstable-salvage`'s `test_salvage_no_resync_scan.sh` (issue #4196, spec
# R4.3).
#
# SCOPE IS DISCOVERED, NOT HARD-PINNED (roborev Medium #2, certifying round).
# This guard used to name ONE file, `verify_location.rs`. That file sits at
# EXACTLY the 800-line source threshold and was already split once during this
# very change, so the next split was a question of when, not if — and after
# one, a filename-pinned guard would still have found its marker, still
# reported "0 hits RECOGNISED", and passed while covering NOTHING of the code
# that moved. A merge-blocking guard that scanned nothing must never read
# identically to one that scanned everything and found nothing.
#
# Both realistic split shapes are therefore covered:
#   1. FLAT siblings — `verify_location*.rs` next to the original (the shape
#      this change itself produced: `verify_location_tests.rs`).
#   2. A MODULE DIRECTORY — `verify_location/` with `mod.rs` + children (the
#      idiomatic Rust shape for a larger split).
# If NEITHER exists the module has been renamed or removed out from under this
# guard, which is a REFUSAL (FAIL), not a pass: see the `scanned -eq 0` check.
#
# Test code is EXCLUDED, by two rules, both mirroring the salvage guard:
#   * a `*/tests/*` path component, or a FLAT `*_tests.rs` sibling — the
#     campsite-rule split pattern puts `#[cfg(test)]` on the DECLARATION in
#     the production file, so the split-out file carries no in-file anchor and
#     would otherwise be scanned as production code;
#   * an in-file `#[cfg(test)]` block, tracked by brace depth.
#
# The in-file tracker replaces this guard's previous "skip from the FIRST
# `#[cfg(test)]` marker to EOF, and FAIL if no marker exists" rule. That rule
# was only exact for a single file known to carry exactly one trailing marker:
# across a discovered file set it would (a) FAIL a split-out production file
# that legitimately has no tests module at all, and (b) skip real production
# code in any file whose first `#[cfg(test)]` item (a gated `use`, `const` or
# `type`) precedes production code. The brace tracker is the salvage guard's,
# including its hardenings for an anchor line whose `{` is on a LATER line and
# for a brace-LESS `#[cfg(test)]` item ending at its own `;`.
#
# WIRING, and why it is not where the salvage guard is: registered in the
# gate's UNSCOPED `roborev-lints` component (`run_roborev_lints_cmd` in
# `scripts/agent-gate.sh`), which runs on every `--lite` round and in the full
# gate regardless of what the diff touches. It is deliberately NOT in
# `tooling-tests`: #4266 made that component diff-scoped
# (`scripts/lib/tooling-tests-scope.sh`) and `cqlite-core/**` is not in its
# declared harness-path set, so a core-only diff — precisely the diff that can
# reintroduce a byte scan here — would SKIP this guard.
# No cargo/python3/network needed (a pure grep over committed files), which is
# what makes it safe for the fast loop.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSTABLE_DIR="$REPO_ROOT/cqlite-core/src/storage/sstable"

if [ ! -d "$SSTABLE_DIR" ]; then
  echo "FAIL - sstable module directory not found: $SSTABLE_DIR"
  exit 1
fi

# Literal byte-pattern search primitives L3 forbids in production location
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

# Shape 1: flat `verify_location*.rs` siblings.
while IFS= read -r -d '' f; do
  scan_file "$f"
done < <(find "$SSTABLE_DIR" -maxdepth 1 -name 'verify_location*.rs' -print0)

# Shape 2: a `verify_location/` module directory.
if [ -d "$SSTABLE_DIR/verify_location" ]; then
  while IFS= read -r -d '' f; do
    scan_file "$f"
  done < <(find "$SSTABLE_DIR/verify_location" -name '*.rs' -print0)
fi

if [ "$scanned" -eq 0 ]; then
  echo "FAIL - 0 production file(s) scanned: no \`verify_location*.rs\` sibling and no \
\`verify_location/\` module directory under $SSTABLE_DIR holds a non-test .rs file. The module \
was renamed, moved or removed out from under this guard — a guard that scanned nothing cannot \
certify a 'no byte-pattern search primitive' verdict, so this is a REFUSAL, not a pass (spec L3)"
  exit 1
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found outside tests (spec L3)"
  exit 1
fi

echo "ok   - $scanned production file(s) scanned (${scanned_names# }), \
0 byte-pattern-search hit(s) RECOGNISED outside tests, $allowed line(s) RECOGNISED as \
no-resync-scan-allow (spec L3)"
exit 0
