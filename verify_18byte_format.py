#!/usr/bin/env python3
"""
Test the ACTUAL hypothesis from index_reader.rs:
- Fixed 18-byte entries (2-byte marker + 16-byte digest)
- NO offset stored in Index.db
- Offsets come from Summary.db correlation
"""

# Read Index.db file
with open('/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db', 'rb') as f:
    data = f.read()

print("=== Fixed 18-byte Entry Format Test ===\n")
print(f"Total file size: {len(data)} bytes")

# Test if file is perfectly divisible by 18
ENTRY_SIZE = 18
expected_entries = len(data) / ENTRY_SIZE
remainder = len(data) % ENTRY_SIZE

print(f"Entry size: {ENTRY_SIZE} bytes")
print(f"Expected entries (if perfect fit): {expected_entries:.2f}")
print(f"Remainder bytes: {remainder}")
print(f"Perfect 18-byte alignment: {'YES ✅' if remainder == 0 else 'NO ❌'}\n")

# Parse as 18-byte entries
entries = []
offset = 0
invalid_markers = 0

while offset + ENTRY_SIZE <= len(data):
    # Read marker (2 bytes, big-endian)
    marker = (data[offset] << 8) | data[offset + 1]

    # Read digest (16 bytes)
    digest = data[offset + 2:offset + 18]

    entries.append({
        'offset': offset,
        'marker': marker,
        'marker_hex': f"0x{marker:04x}",
        'digest': digest.hex()[:16] + "..."
    })

    if marker != 0x0010:
        invalid_markers += 1

    offset += ENTRY_SIZE

print(f"=== Parsing Results ===")
print(f"Total entries parsed: {len(entries)}")
print(f"Valid markers (0x0010): {len(entries) - invalid_markers}")
print(f"Invalid markers: {invalid_markers}")
print(f"Bytes consumed: {offset}")
print(f"Bytes remaining: {len(data) - offset}\n")

# Show first 10 entries
print("First 10 entries:")
for i in range(min(10, len(entries))):
    e = entries[i]
    status = "✅" if e['marker'] == 0x0010 else f"❌ {e['marker_hex']}"
    print(f"  Entry {i:3d} @ byte {e['offset']:5d}: marker={e['marker_hex']} digest={e['digest']} {status}")

print()

# Show entries with invalid markers
if invalid_markers > 0:
    print(f"First 5 entries with invalid markers:")
    shown = 0
    for e in entries:
        if e['marker'] != 0x0010 and shown < 5:
            print(f"  Entry @ byte {e['offset']:5d}: marker={e['marker_hex']} digest={e['digest']}")
            shown += 1
    print()

print("=== VERDICT ===")
if remainder == 0 and invalid_markers == 0:
    print("✅ CONFIRMED: Index.db uses fixed 18-byte entries")
    print("   Structure: [0x0010 marker (2)] + [digest (16)]")
    print("   NO offsets stored in Index.db - they come from Summary.db")
elif remainder == 0 and invalid_markers > 0:
    print("⚠️  PARTIAL: File is 18-byte aligned but has invalid markers")
    print(f"   {invalid_markers} entries with marker != 0x0010")
else:
    print("❌ REJECTED: Index.db does NOT use fixed 18-byte entries")
    print(f"   Remainder: {remainder} bytes")
    print(f"   Invalid markers: {invalid_markers}")
