# Appendix B — On-Disk Encodings Cheat Sheet

In this appendix you will learn:
- How VInt and ZigZag encodings appear on disk in Cassandra
- Common row/cell header bits and where to find them upstream
- Quick rules for reading variable-length values

## VInt (Variable-length integer)

- Cassandra uses a variable-length integer format; lengths and many counters are VInt.
  
VInt is used extensively for lengths and counters in SSTable payloads.

Examples (unsigned lengths shown as hex bytes → value):
- `00` → 0
- `0A` → 10
- `81 00` → 256 (two-byte: 10xxxxxx xxxxxxxx)
- `C1 00 00` → 0x10000 - 1 example boundary (three-byte: 110xxxxx ...)

ZigZag (signed) quick reference:
- Maps signed to unsigned: 0→0, -1→1, 1→2, -2→3, 2→4, ...
- Used for compactly encoding small negative numbers; lengths/counters remain non-negative.

Upstream anchors (Cassandra 5.0.0):
- `org.apache.cassandra.io.util.DataInputPlus` and friends (reading primitives)
- `org.apache.cassandra.db.SerializationHeader` (presence/length handling)

Rules of thumb:
- Length prefixes for `text`, `blob`, collection elements, and UDT fields are VInt.
- Signed values may use ZigZag in compatibility layers; lengths are non-negative.

## Cell/row header flags

Cell flags vary by format; consult Cassandra sources for specifics. For 5.0, see `rows.*` and `SerializationHeader` for field presence.

Upstream references:
- `org.apache.cassandra.db.SerializationHeader`
- `org.apache.cassandra.db.rows.*`

## UDT Field Encoding (Issue #220)

UDT fields use **4-byte big-endian i32** length prefixes (NOT VInt):
```
[field_length: 4-byte BE i32][field_data: variable]
```

**Length semantics**:
| Value | Meaning |
|-------|---------|
| `-1` (0xFFFFFFFF) | NULL field |
| `0` (0x00000000) | Empty field (zero-length, present) |
| `>0` | Byte count of field data |

**UDT type string format** (in Statistics.db):
```
org.apache.cassandra.db.marshal.UserType(keyspace,hex_name,field:type,...)
```
- Names are hex-encoded: `616464726573735f74797065` = "address_type"
- Can exceed 500 bytes for complex nested UDTs (up to 5000 bytes supported)

## Row Size Measurement (Issue #237)

**Critical detail for V5CompressedLegacy format**:

The `row_size` VInt field indicates the byte count of row data, but this count is measured from AFTER the VInt itself is consumed, not from where it starts.

**Offset calculation**:
```
next_row_offset = (row_size_vint_start_offset + row_size_vint_byte_length) + row_size_value
```

**Example**:
- Row metadata starts at offset 100
- `row_size` VInt is 2 bytes (value: 150)
- Row data starts at offset 102 (100 + 2)
- Next row starts at offset 252 (102 + 150)

**Important**: There is NO trailing field after row data - the next partition/row starts immediately after `row_size` bytes.

This matches Cassandra's `getFilePointer()` semantics where the file position after reading the VInt is used as the base for measuring `row_size`.

## Key Takeaways
- Expect VInt before variable-sized payloads; decode, then slice the value.
- **Exception**: UDT fields use fixed 4-byte BE i32 lengths, not VInt.
- Signed fields that use ZigZag appear primarily in legacy contexts; length fields are non-negative.
- **Row size measurement**: VInt values like `row_size` are measured from AFTER the VInt is consumed (Issue #237).

## References
- Cassandra 5.0: `SerializationHeader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`
- Cassandra 5.0: `rows` — `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/db/rows`

