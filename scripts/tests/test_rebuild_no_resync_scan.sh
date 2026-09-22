#!/usr/bin/env bash
#
# Issue #4197 (spec R2.3) — `cqlite rebuild` locates partition boundaries
# ONLY via the structural compaction read path
# (`SSTableReader::distinct_partition_keys_with_positions` /
# `decode_partition_at_offset_for_salvage`), and NEVER by scanning `Data.db`
# bytes for a plausible header. This guard greps
# `cqlite-core/src/storage/write_engine/rebuild/` for any byte-pattern
# search primitive OUTSIDE test code and FAILs naming the line if one is
# found. Mirrors `test_salvage_no_resync_scan.sh` (#4196) verbatim, adapted
# to this directory.
#
# Registered in the gate's `tooling-tests` component (no cargo/network
# needed — a pure grep, so it always runs).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REBUILD_DIR="$REPO_ROOT/cqlite-core/src/storage/write_engine/rebuild"

if [ ! -d "$REBUILD_DIR" ]; then
  echo "FAIL - rebuild module directory not found: $REBUILD_DIR"
  exit 1
fi

PATTERNS=("memchr" ".windows(" "find(|" "position(|")

hits=0
scanned=0
while IFS= read -r -d '' file; do
  case "$file" in
    */tests/* | *_tests.rs) continue ;;
  esac
  scanned=$((scanned + 1))
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
        echo "FAIL - byte-pattern search primitive '$pat' outside tests: $file:$line_no: $line"
        hits=$((hits + 1))
      fi
    done
  done <"$file"
  if [ "$in_test_mod" -eq 1 ]; then
    echo "FAIL - $file: brace tracker for a #[cfg(test)] block never returned to depth 0 by EOF \
(unbalanced or miscounted braces) — refusing rather than silently trusting the scan"
    hits=$((hits + 1))
  fi
done < <(find "$REBUILD_DIR" -name '*.rs' -print0)

if [ "$scanned" -eq 0 ]; then
  echo "FAIL - 0 production .rs file(s) scanned under $REBUILD_DIR (every *.rs found was under \
a */tests/* path, or none exist) — a guard that scanned nothing cannot certify a 'no \
byte-pattern search primitive' verdict; this is a REFUSAL, not a pass"
  exit 1
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found in $REBUILD_DIR outside tests (spec R2.3)"
  exit 1
fi

echo "ok   - $scanned production file(s) scanned, 0 byte-pattern-search hit(s) RECOGNISED in \
$REBUILD_DIR outside tests (spec R2.3)"
exit 0
