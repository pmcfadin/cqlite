#!/usr/bin/env python3
"""
Test hypothesis: Index.db uses FIXED 2-byte offsets (not VInt)
Entry structure: 0x0010 (2) + digest (16) + offset (2) = 20 bytes fixed
"""

# Read Index.db file
with open('/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db', 'rb') as f:
    data = f.read()

print("=== Fixed 20-byte Entry Hypothesis Test ===\n")
print(f"Total file size: {len(data)} bytes\n")

# Calculate expected entries if fixed 20-byte format
expected_entries = len(data) // 20
print(f"Expected entries (if 20 bytes each): {expected_entries}")
print(f"Perfect fit: {len(data) % 20 == 0}\n")

# Parse as fixed 20-byte entries
ENTRY_SIZE = 20
offset = 0
entry_num = 1
entries = []

while offset + ENTRY_SIZE <= len(data):
    # Read marker (2 bytes, big-endian)
    marker = (data[offset] << 8) | data[offset + 1]

    # Read digest (16 bytes)
    digest = data[offset + 2:offset + 18]

    # Read offset (2 bytes, big-endian)
    partition_offset = (data[offset + 18] << 8) | data[offset + 19]

    entries.append({
        'entry_num': entry_num,
        'file_offset': offset,
        'marker': f"0x{marker:04x}",
        'digest_hex': digest.hex()[:16] + "...",
        'partition_offset': partition_offset
    })

    if entry_num <= 20 or marker != 0x0010:  # Print first 20 or any anomalies
        status = "✅" if marker == 0x0010 else "❌ INVALID"
        print(f"Entry {entry_num:4d} @ byte {offset:5d}: marker={entries[-1]['marker']} "
              f"digest={entries[-1]['digest_hex']} offset={partition_offset:5d} {status}")

    offset += ENTRY_SIZE
    entry_num += 1

print(f"\n=== Validation Results ===")
print(f"Total entries parsed: {len(entries)}")
print(f"Bytes consumed: {offset}")
print(f"Bytes remaining: {len(data) - offset}")

# Check all markers
invalid_markers = [e for e in entries if e['marker'] != '0x0010']
print(f"\nMarker validation:")
print(f"  Valid markers (0x0010): {len(entries) - len(invalid_markers)}")
print(f"  Invalid markers: {len(invalid_markers)}")

if invalid_markers:
    print("  First invalid entry:")
    print(f"    {invalid_markers[0]}")

# Check partition offset progression
print(f"\nPartition offset analysis:")
offsets = [e['partition_offset'] for e in entries[:20]]
print(f"  First 20 offsets: {offsets}")
print(f"  Monotonically increasing: {all(offsets[i] <= offsets[i+1] for i in range(len(offsets)-1))}")

# Summary
print(f"\n=== HYPOTHESIS VERDICT ===")
if len(data) % 20 == 0 and len(invalid_markers) == 0:
    print("✅ CONFIRMED: Index.db uses fixed 20-byte entries")
    print("   Structure: [0x0010 marker (2)] + [digest (16)] + [offset (2)]")
else:
    print("❌ REJECTED: Index.db does NOT use fixed 20-byte entries")
    print(f"   Remainde bytes: {len(data) % 20}")
    print(f"   Invalid markers: {len(invalid_markers)}")
