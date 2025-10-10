#!/bin/bash
# Analyze Index.db structure empirically

INDEX_FILE="/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db"

echo "=== Index.db Binary Structure Analysis ==="
echo ""
echo "File size:"
ls -lh "$INDEX_FILE" | awk '{print $5}'
echo ""

echo "First 300 bytes with annotations:"
xxd -l 300 "$INDEX_FILE" | head -20
echo ""

echo "Pattern Analysis:"
echo "Looking for entry structure: 0x0010 + 16-byte digest + VInt offset"
echo ""

# Manually parse first few entries
echo "Entry 1 (bytes 0-18+):"
echo -n "  Marker: "
xxd -l 2 "$INDEX_FILE" | awk '{print $2}'
echo -n "  Digest: "
xxd -s 2 -l 16 "$INDEX_FILE" | awk '{for(i=2;i<=NF && i<18;i++) printf $i" "; print ""}'
echo -n "  Offset bytes: "
xxd -s 18 -l 4 "$INDEX_FILE" | awk '{print $2, $3, $4, $5}'

echo ""
echo "Entry 2 (bytes after first entry):"
echo "  Looking for next 0x0010 marker..."
xxd -l 100 "$INDEX_FILE" | grep -n "0010" | head -5

echo ""
echo "Entry spacing analysis:"
# Try to find pattern by looking at 0010 positions
xxd "$INDEX_FILE" | grep "0010" | head -10
