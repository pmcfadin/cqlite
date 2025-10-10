#!/usr/bin/env python3
"""
Decode the actual variable-length format in Index.db
Most entries are 22 bytes: 0x0010 (2) + digest (16) + ??? (4)
"""

# Read Index.db file
with open('/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db', 'rb') as f:
    data = f.read()

print("=== Decoding Variable-Length Format ===\n")

# Find all 0x0010 marker positions
positions = []
for i in range(len(data) - 1):
    if data[i] == 0x00 and data[i+1] == 0x10:
        positions.append(i)

print(f"Total entries: {len(positions)}\n")

# Analyze first 10 entries in detail
print("First 10 entries with full breakdown:")
for i in range(min(10, len(positions))):
    start = positions[i]
    next_start = positions[i+1] if i+1 < len(positions) else len(data)
    entry_size = next_start - start

    # Parse entry components
    marker = data[start:start+2]
    digest = data[start+2:start+18] if start+18 <= len(data) else b''
    extra = data[start+18:next_start]

    print(f"Entry {i}:")
    print(f"  Position: {start}-{next_start-1} ({entry_size} bytes)")
    print(f"  Marker: {marker.hex()} (2 bytes)")
    print(f"  Digest: {digest.hex()} (16 bytes)")
    print(f"  Extra:  {extra.hex()} ({len(extra)} bytes)")

    # Try to interpret extra bytes as integers
    if len(extra) == 2:
        extra_int_be = (extra[0] << 8) | extra[1]
        print(f"          -> as u16 BE: {extra_int_be} (0x{extra_int_be:04x})")
    elif len(extra) == 3:
        extra_int_be = (extra[0] << 16) | (extra[1] << 8) | extra[2]
        print(f"          -> as u24 BE: {extra_int_be} (0x{extra_int_be:06x})")
    elif len(extra) == 4:
        extra_int_be = (extra[0] << 24) | (extra[1] << 16) | (extra[2] << 8) | extra[3]
        print(f"          -> as u32 BE: {extra_int_be} (0x{extra_int_be:08x})")
    print()

print("\n=== Entry Size Distribution ===")
sizes = {}
for i in range(len(positions) - 1):
    size = positions[i+1] - positions[i]
    sizes[size] = sizes.get(size, 0) + 1

for size, count in sorted(sizes.items()):
    pct = 100 * count / (len(positions) - 1)
    print(f"  {size:2d} bytes: {count:4d} entries ({pct:5.1f}%)")

print("\n=== HYPOTHESIS ===")
most_common_size = max(sizes.keys(), key=lambda k: sizes[k])
print(f"Most common entry size: {most_common_size} bytes")
print(f"Structure guess: 0x0010 (2) + digest (16) + offset_field ({most_common_size - 18})")

# Check if the "extra" bytes could be VInts
print("\n=== VInt Analysis of Extra Bytes ===")
def is_vint_1byte(b):
    """VInt with high bit 0 = single byte value"""
    return b < 0x80

def is_vint_2byte(b0):
    """VInt with pattern 10xxxxxx = 2-byte value"""
    return 0x80 <= b0 < 0xC0

def is_vint_3byte(b0):
    """VInt with pattern 110xxxxx = 3-byte value"""
    return 0xC0 <= b0 < 0xE0

# Analyze first byte of extra field
vint_patterns = {1: 0, 2: 0, 3: 0, 4: 0, 'other': 0}
for i in range(len(positions) - 1):
    start = positions[i]
    next_start = positions[i+1]
    extra_start = start + 18

    if extra_start < len(data):
        first_extra_byte = data[extra_start]

        if is_vint_1byte(first_extra_byte):
            vint_patterns[1] += 1
        elif is_vint_2byte(first_extra_byte):
            vint_patterns[2] += 1
        elif is_vint_3byte(first_extra_byte):
            vint_patterns[3] += 1
        else:
            vint_patterns[4] += 1

print("VInt length patterns (based on first extra byte):")
for length, count in sorted(vint_patterns.items()):
    if count > 0:
        pct = 100 * count / (len(positions) - 1) if len(positions) > 1 else 0
        print(f"  {length:6s} byte VInt: {count:4d} ({pct:5.1f}%)")
