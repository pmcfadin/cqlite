#!/bin/bash
# SSTable Magic Number Discovery Tool
# Scans all Data.db files and extracts magic numbers for validation

set -euo pipefail

SSTABLE_DIR="${1:-$(pwd)/test-data/datasets/sstables}"

echo "=== CQLite SSTable Magic Number Scanner ==="
echo "Scanning: $SSTABLE_DIR"
echo ""
echo "Magic      Table Name                         File Path"
echo "---------- ---------------------------------- --------------------------------------------------"

find "$SSTABLE_DIR" -name "*-Data.db" -type f | sort | while read file; do
  # Extract magic number (first 4 bytes, big-endian)
  magic=$(hexdump -n 4 -e '1/4 "%08x" "\n"' "$file" 2>/dev/null || echo "ERROR")
  
  # Extract table name from directory
  dir=$(dirname "$file")
  table=$(basename "$dir" | cut -d'-' -f1)
  
  # Format output
  printf "0x%s %-34s %s\n" "$magic" "$table" "$file"
done | sort -u

echo ""
echo "=== Summary ==="
echo "Unique magic numbers found:"
find "$SSTABLE_DIR" -name "*-Data.db" -type f -exec hexdump -n 4 -e '1/4 "0x%08x\n"' {} \; 2>/dev/null | sort -u | nl

echo ""
echo "=== Parser Validation ==="
echo "Check these magic numbers against:"
echo "  cqlite-core/src/parser/header.rs:CassandraVersion::from_magic_number()"
