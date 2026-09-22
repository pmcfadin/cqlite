#!/usr/bin/env bash
#
# Issue #4194 (corruption-locator, spec verify-location L3) — partition
# location resolution reads ONLY the boundary source's own decoded entries
# (`Index.db`'s `PartitionIndexEntry`s, the BTI trie's decoded leaves) and the
# `CompressionInfo.db`/`CRC.db` chunk tables — NEVER by scanning `Data.db`
# bytes for a plausible partition header. This guard greps
# `cqlite-core/src/storage/sstable/verify_location.rs` for any byte-pattern
# search primitive OUTSIDE test code and FAILs naming the line if one is
# found. Modeled directly on `sstable-salvage`'s
# `test_salvage_no_resync_scan.sh` (issue #4196, spec R4.3) — same mechanism,
# a single-file target rather than a directory.
#
# Registered in the gate's `tooling-tests` component (no cargo/network needed
# — a pure grep, so it always runs).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET="$REPO_ROOT/cqlite-core/src/storage/sstable/verify_location.rs"

if [ ! -f "$TARGET" ]; then
  echo "FAIL - target file not found: $TARGET"
  exit 1
fi

# Literal byte-pattern search primitives L3 forbids in production location
# code — the exact "resync by scanning for a plausible header" shape
# `sstable-salvage`'s R4.3 guard exists to catch, reused here verbatim.
PATTERNS=("memchr" ".windows(" "find(|" "position(|")

hits=0
in_test_mod=0
seen_open=0
depth=0
line_no=0
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
      in_test_mod=0
    fi
    continue
  fi
  stripped="${line// /}"
  stripped="${stripped//$'\t'/}"
  if [ "$stripped" = '#[cfg(test)]' ]; then
    in_test_mod=1
    seen_open=0
    depth=0
    continue
  fi
  for pat in "${PATTERNS[@]}"; do
    if [[ "$line" == *"$pat"* ]]; then
      echo "FAIL - byte-pattern search primitive '$pat' outside tests: $TARGET:$line_no: $line"
      hits=$((hits + 1))
    fi
  done
done <"$TARGET"

if [ "$in_test_mod" -eq 1 ]; then
  echo "FAIL - $TARGET: brace tracker for a #[cfg(test)] block never returned to depth 0 by EOF \
(unbalanced or miscounted braces) — refusing rather than silently trusting the scan"
  hits=$((hits + 1))
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found in $TARGET outside tests (spec L3)"
  exit 1
fi

echo "ok   - $TARGET scanned, 0 byte-pattern-search hit(s) RECOGNISED outside tests (spec L3)"
exit 0
