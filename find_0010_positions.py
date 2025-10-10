#!/usr/bin/env python3
"""
Find all positions of 0x0010 marker in Index.db
"""

# Read Index.db file
with open('/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db', 'rb') as f:
    data = f.read()

print("=== Finding 0x0010 Marker Positions ===\n")

# Find all 0x0010 occurrences
positions = []
for i in range(len(data) - 1):
    if data[i] == 0x00 and data[i+1] == 0x10:
        positions.append(i)

print(f"Total 0x0010 markers found: {len(positions)}")
print(f"File size: {len(data)} bytes\n")

# Analyze spacing between markers
if len(positions) > 1:
    print("First 20 marker positions and spacing:")
    for i in range(min(20, len(positions))):
        pos = positions[i]
        if i > 0:
            spacing = pos - positions[i-1]
            print(f"  Position {pos:5d} (spacing: {spacing:2d} bytes from previous)")
        else:
            print(f"  Position {pos:5d} (first marker)")

    print("\nSpacing distribution:")
    spacings = {}
    for i in range(1, len(positions)):
        spacing = positions[i] - positions[i-1]
        spacings[spacing] = spacings.get(spacing, 0) + 1

    for spacing, count in sorted(spacings.items()):
        pct = 100 * count / (len(positions) - 1)
        print(f"  {spacing:2d} bytes: {count:4d} occurrences ({pct:5.1f}%)")

print(f"\n=== Comparison with JSONL partition count ===")
print(f"0x0010 markers in Index.db: {len(positions)}")
print(f"Expected partitions (from JSONL): 999")
print(f"Match: {'YES ✅' if len(positions) == 999 else 'NO ❌'}")

if len(positions) >= 2:
    # Check if variable spacing indicates VInt offsets
    unique_spacings = len(set(positions[i] - positions[i-1] for i in range(1, len(positions))))
    print(f"\nUnique spacing values: {unique_spacings}")
    print(f"Variable-length entries: {'YES ✅' if unique_spacings > 1 else 'NO ❌'}")
