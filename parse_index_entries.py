#!/usr/bin/env python3
"""
Empirical analysis of Index.db VInt encoding pattern
"""

def parse_vint(data, offset):
    """
    Parse Cassandra VInt (variable-length integer)
    First byte determines length:
    - If high bit is 0: single byte value
    - If high bit is 1: count leading 1 bits for length
    """
    first_byte = data[offset]

    # Count leading 1 bits
    if first_byte == 0:
        return 0, 1

    # Check if single byte (high bit is 0)
    if first_byte < 128:
        return first_byte, 1

    # Multi-byte: count leading ones
    leading_ones = 0
    mask = 0x80
    while first_byte & mask:
        leading_ones += 1
        mask >>= 1

    # Total bytes = leading_ones
    length = leading_ones

    # Extract value
    value = first_byte & (0xFF >> leading_ones)
    for i in range(1, length):
        value = (value << 8) | data[offset + i]

    return value, length

# Read Index.db file
with open('/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db', 'rb') as f:
    data = f.read()

print("=== VInt Offset Pattern Validation ===\n")
print(f"Total file size: {len(data)} bytes\n")

# Parse entries
offset = 0
entry_num = 1
entries = []

while offset < len(data) - 18:
    # Read marker (2 bytes)
    if offset + 2 > len(data):
        break

    marker = (data[offset] << 8) | data[offset + 1]
    if marker != 0x0010:
        print(f"WARNING: Expected 0x0010 at offset {offset}, got 0x{marker:04x}")
        break

    # Read digest (16 bytes)
    digest_start = offset + 2
    digest = data[digest_start:digest_start + 16]

    # Parse VInt offset
    vint_offset_pos = digest_start + 16
    if vint_offset_pos >= len(data):
        break

    partition_offset, vint_length = parse_vint(data, vint_offset_pos)

    entry_size = 2 + 16 + vint_length  # marker + digest + vint

    entries.append({
        'entry_num': entry_num,
        'file_offset': offset,
        'marker': f"0x{marker:04x}",
        'digest_hex': digest.hex()[:32] + "...",
        'partition_offset': partition_offset,
        'vint_bytes': vint_length,
        'entry_size': entry_size
    })

    if entry_num <= 15:  # Print first 15 entries
        print(f"Entry {entry_num:3d} @ byte {offset:5d}:")
        print(f"  Marker:           {entries[-1]['marker']}")
        print(f"  Digest:           {entries[-1]['digest_hex']}")
        print(f"  Partition Offset: {partition_offset} (0x{partition_offset:x})")
        print(f"  VInt Length:      {vint_length} byte(s)")
        print(f"  Entry Size:       {entry_size} bytes")
        print()

    offset += entry_size
    entry_num += 1

print(f"\n=== Summary ===")
print(f"Total entries parsed: {len(entries)}")
print(f"Bytes consumed: {offset}")
print(f"Bytes remaining: {len(data) - offset}")

# Analyze VInt lengths
vint_lengths = {}
for e in entries:
    vlen = e['vint_bytes']
    vint_lengths[vlen] = vint_lengths.get(vlen, 0) + 1

print(f"\nVInt length distribution:")
for vlen, count in sorted(vint_lengths.items()):
    print(f"  {vlen} byte(s): {count} entries ({100*count/len(entries):.1f}%)")

# Verify variable-length entries
entry_sizes = set(e['entry_size'] for e in entries)
print(f"\nEntry size variation:")
print(f"  Unique entry sizes: {sorted(entry_sizes)}")
print(f"  Variable-length entries: {'YES ✅' if len(entry_sizes) > 1 else 'NO ❌'}")

# Check fixed 18-byte hypothesis
fixed_18_entries = sum(1 for e in entries if e['entry_size'] == 20)  # 2 + 16 + 2
print(f"\nFixed 18-byte hypothesis check:")
print(f"  Entries that are 20 bytes (2+16+2): {fixed_18_entries}/{len(entries)}")
print(f"  Hypothesis: {'REJECTED ❌' if fixed_18_entries < len(entries) else 'CONFIRMED ✅'}")
