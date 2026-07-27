---
title: "Appendix B — On-Disk Encodings Cheat Sheet"
description: "In this appendix you will..."
sidebar:
  label: "Appendix B — On-Disk Encodings Cheat Sheet"
  order: 102
---

In this appendix you will learn:
- How VInt and ZigZag encodings appear on disk in Cassandra
- Which `Data.db` fields are unsigned VInt, which are signed, and which are fixed-width
- Common row/cell header bits and where to find them upstream
- Quick rules for reading variable-length values
- Write-side encoding patterns for SSTable generation

## VInt (Variable-length integer)

- Cassandra uses a variable-length integer format; lengths and many counters are VInt.

VInt is used extensively for lengths and counters in SSTable payloads.

Examples (unsigned lengths shown as hex bytes → value):
- `00` → 0
- `0A` → 10
- `81 00` → 256 (two-byte: 10xxxxxx xxxxxxxx)
- `C1 00 00` → 0x10000 (three-byte: 110xxxxx xxxxxxxx xxxxxxxx — five data bits in the first byte)

The count of leading 1-bits in the first byte is the number of *extra* bytes that follow; the
remaining bits of the first byte are the high-order data bits (`VIntCoding.java`).

ZigZag (signed) quick reference:
- Maps signed to unsigned: 0→0, -1→1, 1→2, -2→3, 2→4, ...
- Used for compactly encoding small negative numbers; lengths/counters remain non-negative.

Upstream anchors (Cassandra 5.0.8):
- `org.apache.cassandra.io.util.DataInputPlus` and friends (reading primitives)
- `org.apache.cassandra.db.SerializationHeader` (presence/length handling)

Rules of thumb:
- Length prefixes that *are* present are **unsigned** VInt (`ValueAccessor.writeWithVIntLength` →
  `writeUnsignedVInt32`, `ValueAccessor.java:171-175`; paths via `ByteBufferUtil.writeWithVIntLength`,
  `CollectionType.java:361-366`). That covers `text`/`blob` simple cell values, the path of **every**
  non-frozen collection cell, and the value of every non-frozen collection cell that *has* one.
- **A non-frozen collection cell PATH is always length-prefixed; its VALUE is length-prefixed *iff*
  `HAS_EMPTY_VALUE` (`0x04`) is clear.** That flag is **size-driven, not type-driven**:
  `Cell.Serializer.serialize` sets it from `cell.valueSize() > 0` (`Cell.java:271`, `:277-278`) and
  writes the value only `if (hasValue)` (`:303-304`); the reader mirrors it at `:310`, `:329-339`
  (`skip` at `:381`, `:399-400`). So **no length VInt and no bytes** are written for a zero-length
  value — the flag replaces the `0x00` a reader might expect. Three instances of the one rule:
  a non-frozen `set<T>` element (datum lives in the path; `SetType.valueComparator()` is
  `EmptyType.instance`, `SetType.java:106-109` → always zero-length); **any** zero-length value
  (a `map<text,text>` entry with value `''`, an empty blob in `list<blob>`); an element **tombstone**
  (`IS_DELETED`, `0x01` — see the mask's own comment at `Cell.java:264`).
- **When a value IS present it is unsigned-VInt-length-prefixed even for a fixed-width element type**
  (`list<int>`, `map<text,bigint>`). `Cell.Serializer.serialize` writes it as
  `header.getType(column).writeValue(...)` (`Cell.java:303-304`), and `header.getType(column)` is the
  **column's** type — the collection type, not the element type (`SerializationHeader.java:160-163`,
  `:250-257`). `CollectionType`/`ListType`/`MapType`/`SetType` never override `valueLengthIfFixed()`, so
  they are `VARIABLE_LENGTH = -1` (`AbstractType.java:62`, `:490-493`) and `writeValue` takes the
  `writeWithVIntLength` branch (`:550-552`).
- **Where the fixed-width no-prefix rule DOES apply: SIMPLE (non-collection) cells.** Only scalar types
  override `valueLengthIfFixed()` (e.g. `Int32Type` → `4`, `Int32Type.java:156-159`), so only a simple
  cell can take the raw-bytes branch (`AbstractType.java:538-543`). Cassandra's layout comment states
  both gates at once — the value size "is present unless either the cell has the
  `HAS_EMPTY_VALUE_MASK`, or the value for columns of this type have a fixed length"
  (`Cell.java:254-255`); for a collection cell only the first can ever fire.
- **Exception — fixed 4-byte BE i32, not VInt**: tuple/UDT field lengths and *frozen* collection
  counts/element lengths (`TupleType.buildValue`, `CollectionSerializer.writeCollectionSize`).
- Every VInt in `Data.db` is unsigned **except** the three components of a serialized `DurationType`
  payload — wherever that payload occurs, including nested in a collection/tuple/UDT. Scoped to
  `Data.db`: `Index.db`'s promoted-index width delta is a signed VInt (`IndexInfo.java:96,111-112`).

## ZigZag (Signed VInt) — Where It Actually Applies

ZigZag maps a signed integer onto an unsigned one so small negative magnitudes stay short, then
encodes the result as an unsigned VInt. In Cassandra it is reached through
`DataOutputPlus.writeVInt` → `VIntCoding.writeVInt` → `writeUnsignedVInt(encodeZigZag64(v))`
(`VIntCoding.java:449`, `:522`).

**ZigZag formula**: `(n << 1) ^ (n >> 63)` for 64-bit signed integers

**Encoding pattern**:
- Positive values: `zigzag(n) = 2 * n`
- Negative values: `zigzag(n) = 2 * |n| - 1`

**Examples**:
| Original (i64) | ZigZag (u64) | VInt Bytes |
|----------------|--------------|------------|
| 0 | 0 | `0x00` |
| 1 | 2 | `0x02` |
| -1 | 1 | `0x01` |
| 63 | 126 | `0x7E` |
| -64 | 127 | `0x7F` |
| 64 | 128 | `0x80 0x80` |
| 1000 | 2000 | `0x87 0xD0` |
| -1000 | 1999 | `0x87 0xCF` |

**Where ZigZag actually appears in `Data.db`** — inside a serialized `DurationType` payload, and
nowhere else:
- A `duration` payload is three consecutive **signed** VInts (months, days, nanos).
  `DurationSerializer.serialize` calls `output.writeVInt(...)` three times
  (`DurationSerializer.java:34,49-51`). ZigZag is genuinely required here — a negative CQL duration
  makes every non-zero component negative (`Duration.java:101-110`). See Appendix A.
- **This is not limited to a top-level `duration` cell.** Wherever a `duration` is *nested*, the same
  three signed VInts sit inside the enclosing value's bytes while the cell's own type is something
  else: `frozen<list<duration>>`, `map<text, frozen<tuple<duration,int>>>`, a UDT field declared
  `duration`. Cassandra models exactly this recursion — `DurationType.referencesDuration()` returns
  `true` (`DurationType.java:96-99`) and `TupleType.referencesDuration()` recurses over `allTypes()`
  (`TupleType.java:125-128`), and `UserType extends TupleType` inherits it (`UserType.java:52`).
  A decoder must therefore reach the signed-VInt path by *type descent*, not by checking whether the
  column type is `duration`.

**Where ZigZag does NOT appear in `Data.db`**: every **structural** field. Length prefixes, counts, and
the row/cell temporal deltas are all unsigned (next section), no matter how deeply a `duration` is
nested inside the value they frame.

**Signed VInt is not unique to `duration` across the whole component set.** The promoted index inside
`Index.db` also uses a signed VInt, and mixes both variants in one struct: `IndexInfo.Serializer`
writes the block offset unsigned but the **width delta signed** —
`out.writeUnsignedVInt(info.offset)` then `out.writeVInt(info.width - WIDTH_BASE)` with
`WIDTH_BASE = 64 * 1024` (`IndexInfo.java:96,111-112`), read back as `in.readVInt() + WIDTH_BASE`
(`:134`). The delta is signed because a block narrower than `WIDTH_BASE` yields a negative value.
ZigZag also appears in the internode messaging serialization path, which is not an SSTable concern.

**Implementation references** (`cqlite-core/src/storage/serialization/vint.rs::encode_signed()` =
ZigZag + unsigned VInt):
- `Data.db` `duration` payload — three `encode_signed` calls in
  `cqlite-core/src/storage/sstable/writer/data_writer/encoding.rs:232-234`. Nesting is handled by
  recursion, not by a top-level type check: `serialize_value_into` recurses into map keys/values, frozen
  collection elements, and `Value::Frozen` inners (`encoding.rs:303-315`), so a nested `duration` reaches
  the same three signed VInts.
- `Index.db` promoted-index width delta — `encode_signed(width_delta, buf)` in
  `cqlite-core/src/storage/sstable/writer/index_writer.rs:667`; the read side zigzag-decodes to invert
  it (`cqlite-core/src/storage/sstable/promoted_index_reader.rs`).

## SSTable Row Fields Always Use Unsigned VInt, Not ZigZag

All temporal delta fields written by `SerializationHeader` call the unsigned variant:

| Field | Method | Source |
|-------|--------|--------|
| timestamp delta | `writeUnsignedVInt(ts - min_ts)` | `SerializationHeader.java:165-168` |
| TTL delta | `writeUnsignedVInt32(ttl - min_ttl)` | `SerializationHeader.java:175-178` |
| local_deletion_time delta | `writeUnsignedVInt32(ldt - min_ldt)` | `SerializationHeader.java:170-173` |

`writeDeletionTime` is just those two in order — `writeTimestamp` then `writeLocalDeletionTime`
(`SerializationHeader.java:180-184`) — so a row/cell/complex deletion is also two unsigned VInts.

Because the baselines (`min_timestamp`, `min_ttl`, `min_local_deletion_time`) are the minimums
across the SSTable, every delta is non-negative, making unsigned encoding both correct and
optimal. Decoding one of these as signed ZigZag silently halves and sign-flips the value.

> **Do not infer signedness from the bytes.** A single byte `0x05` is `5` unsigned and `-3` under
> ZigZag; nothing in the byte distinguishes them. The writer's method (`writeUnsignedVInt` vs
> `writeVInt`) is the only authority — read it off the field's serializer, never off the data.

CQLite matches this on both sides: the reader parses these fields with `parse_vuint`
(`reader/parsing/row_decoder/cell_value.rs:95-99` for the cell timestamp delta,
`row_decoder/row_framing.rs:230-232` for deletion times) and the writer emits them with
`encode_unsigned` (`writer/data_writer/cells.rs:51-55`, `:170-183`). Issue #1623 (PR #1757,
`d31c897c`) reclassified 256 real corpus sites that a signed decode had been silently corrupting.

## Delta Encoding Pattern

SSTable components use delta encoding to reduce storage size by storing offsets from baseline values. These baselines are tracked in Statistics.db and used during both read and write operations.

**Formula**: `stored_value = actual_value - baseline_value`

**Statistics.db baselines**:
| Field | Purpose | Format |
|-------|---------|--------|
| `min_timestamp` | Timestamp baseline | i64, microseconds since epoch |
| `min_ttl` | TTL baseline | i32, seconds |
| `min_local_deletion_time` | Deletion time baseline | i32, seconds since epoch |

**Examples**:

*Timestamp encoding*:
- Statistics: `min_timestamp = 1000000`
- Actual timestamp: `1005000` (microseconds)
- Stored delta: `5000` (encoded as **unsigned** VInt — `SerializationHeader.writeUnsignedVInt`)
- Bytes: `0x93 0x88` (unsigned VInt(5000): `5000 = 0x1388`, 2-byte form `0x93 0x88`)

*TTL encoding*:
- Statistics: `min_ttl = 3600` (1 hour)
- Actual TTL: `7200` (2 hours)
- Stored delta: `3600` (encoded as **unsigned** VInt32 — `SerializationHeader.writeUnsignedVInt32`)
- Bytes: `0x8E 0x10` (unsigned VInt(3600): `3600 = 0x0E10`, 2-byte form `(0x0E | 0x80) = 0x8E`, `0x10`)
  — the encoded value is the **delta**, not the absolute TTL.

*Local deletion time encoding*:
- Statistics: `min_local_deletion_time = 1700000000` (Nov 2023)
- Actual deletion time: `1700000010`
- Stored delta: `10` (encoded as unsigned VInt)
- Bytes: `0x0A`

**Important**: When writing multiple partitions, compute Statistics.db values FIRST by scanning all mutations to find minimum values, then use these baselines during Data.db encoding.

**Implementation reference**: `cqlite-core/src/storage/sstable/writer/data_writer.rs::write_cell()`, `data_writer.rs::write_tombstone_cell()`

## Row/Cell Flag Quick Reference

V5CompressedLegacy format uses bit flags in row and cell headers to indicate field presence and semantics. These flags are critical for both reading and writing SSTables.

### Row Flags (1 byte)

Row flags appear at the start of each row and control row-level metadata.

| Bit | Name | Value | Description |
|-----|------|-------|-------------|
| 0 | `END_OF_PARTITION` | `0x01` | End of partition marker (no row data follows) |
| 1 | `IS_MARKER` | `0x02` | Unfiltered is a RangeTombstoneMarker, not a Row |
| 2 | `HAS_TIMESTAMP` | `0x04` | Row-level timestamp present (delta encoded) |
| 3 | `HAS_TTL` | `0x08` | Row-level TTL present (delta encoded) |
| 4 | `HAS_DELETION` | `0x10` | Row deletion present (markedForDeleteAt unsigned VInt first, then local_deletion_time unsigned VInt32) |
| 5 | `HAS_ALL_COLUMNS` | `0x20` | All columns present (no bitmap needed) |
| 6 | `HAS_COMPLEX_DELETION` | `0x40` | Row contains non-frozen collection column |
| 7 | `HAS_EXTENDED_FLAGS` | `0x80` | Extended flags byte follows |

**Common flag combinations**:
- `0x24`: Simple write (timestamp + all columns)
- `0x2C`: TTL write (timestamp + TTL + all columns)
- `0x04`: Partial update (timestamp, no HAS_ALL_COLUMNS)
- `0x14`: Row deletion (timestamp + deletion)

**Example**:
```
Row with timestamp and all columns present:
[0x24]  ← flags (HAS_TIMESTAMP | HAS_ALL_COLUMNS)
[...]   ← clustering prefix (if present)
[...]   ← row_size VInt
[...]   ← timestamp delta VInt
[...]   ← cell data (no bitmap needed)
```

### Cell Flags (1 byte)

Cell flags appear at the start of each cell and control cell-level metadata.

| Bit | Name | Value | Description |
|-----|------|-------|-------------|
| 0 | `IS_DELETED` | `0x01` | Cell is a tombstone (no value) |
| 1 | `IS_EXPIRING` | `0x02` | TTL fields follow (expiring cell) |
| 2 | `HAS_EMPTY_VALUE` | `0x04` | Zero-length value (not NULL) |
| 3 | `USE_ROW_TIMESTAMP` | `0x08` | Use row-level timestamp (no cell timestamp) |
| 4 | `USE_ROW_TTL` | `0x10` | Inherit row-level TTL **and** local_deletion_time (neither field is written) |

**Common flag combinations**:
- `0x08`: Normal write (use row timestamp)
- `0x0C`: Empty string write (use row timestamp + empty value)
- `0x01`: Tombstone (deleted, must include timestamp)
- `0x02`: Expiring cell with own timestamp (IS_EXPIRING, no USE_ROW_TIMESTAMP)
- `0x12`: Expiring cell with row TTL (IS_EXPIRING + USE_ROW_TTL)

**Critical distinction**:
- **Tombstones** (`IS_DELETED`): MUST NOT set `USE_ROW_TIMESTAMP` - tombstones require explicit timestamps and local_deletion_time
- **`IS_EXPIRING` (0x02) is strict and mutually exclusive with `IS_DELETED` (0x01)**:
  `IS_EXPIRING` means `ttl != NO_TTL` and is set only for a live cell that carries a TTL. A
  tombstone never also sets `IS_EXPIRING` (so no wasted TTL byte), and an expiring cell never
  sets `IS_DELETED`. Authority: `Cell.Serializer.flags()`. Verified in CQLite's emitter
  (`data_writer.rs` cell-flag construction: TTL fields only on the `Some(ttl)` path).
- **Empty strings**: `HAS_EMPTY_VALUE` (`0x04`) flag set and **no** value length and no value bytes —
  the flag replaces the length VInt, it does not precede a `0x00` (`Cell.java:277-278`, `:303-304`;
  reader `:310`). Distinct from NULL.
- **NULL values**: NOT written as cells - represented by absence in column bitmap. Caveat for
  **non-frozen collections**: a collection emptied or overwritten-with-`{}` reads back as NULL but is
  **present** in the subset, carrying a complex deletion and zero element cells — absence in the
  bitmap is not the only on-disk shape that surfaces as NULL. See Chapter 5, "Empty Collections".

**TTL field range — Cassandra's bound, and the reader-side cast hazard**:

On disk, the cell TTL is an **unsigned VInt32 delta** over `stats.minTTL`:
`SerializationHeader.writeTTL` emits `writeUnsignedVInt32(ttl - stats.minTTL)` and `readTTL` adds
`stats.minTTL` back (`SerializationHeader.java:175-178`, `:196-199`). Cassandra itself holds TTL in a
signed `int`, and enforces the range at request validation, not at parse time:
`Attributes.MAX_TTL = 20 * 365 * 24 * 60 * 60` — 20 years (20 × 365 days), `630,720,000` s
(`Attributes.java:47`) — with `ttl < 0` and `ttl > MAX_TTL` both rejected as
`InvalidRequestException` (`:135-139`). At *read* time the only check is
`if (ttl < 0) throw new IOException("Invalid TTL: " + ttl)` (`Cell.java:345-346`). So a
well-formed Cassandra 5.0 SSTable can never carry a TTL above `MAX_TTL`, which is comfortably
below `i32::MAX` (`2,147,483,647` s ≈ 68 years).

*Reader-implementation note (CQLite, not a format rule)*: a reader that decodes the delta into a
`u32`/`u64` and then does a bare `as i32` would wrap an out-of-range value to a **negative** TTL —
the one thing Cassandra's own reader rejects outright. CQLite therefore saturates instead of
wrapping, on both cell paths:
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs:46-53`
(`ttl.min(i32::MAX as u32) as i32`, collection-element path, issue #2498) and
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/cell_value.rs:165`
(`abs_ttl.min(i32::MAX as i64) as i32`, scalar path, issue #2173). No real Cassandra data reaches
this clamp; it exists so a corrupt or adversarial file yields a saturated positive TTL rather than a
sign-flipped one.

**Example**:
```
Normal cell (use row timestamp):
[0x08]  ← flags (USE_ROW_TIMESTAMP)
[...]   ← value_length VInt
[...]   ← value bytes

Tombstone cell:
[0x01]  ← flags (IS_DELETED only)
[...]   ← timestamp delta VInt (required)
[...]   ← local_deletion_time delta VUInt (required)
(no value bytes)

Empty string cell:
[0x0C]  ← flags (USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE)
(no value length VInt, no value bytes)
```

**Upstream references**:
- `org.apache.cassandra.db.SerializationHeader`
- `org.apache.cassandra.db.rows.*`
- `org.apache.cassandra.db.rows.UnfilteredSerializer` (V5CompressedLegacy encoding)

## Tuple and UDT Field Encoding (Issue #220)

Tuple and UDT fields share one on-disk framing: **4-byte big-endian i32** length prefixes
(NOT VInt), fields concatenated in schema/positional order with **no field count** on disk:
```
[field_length: 4-byte BE i32][field_data: variable]
```

**Length semantics**:
| Value | Meaning |
|-------|---------|
| `-1` (0xFFFFFFFF) | NULL field |
| `0` (0x00000000) | Empty field (zero-length, present) |
| `>0` | Byte count of field data |

Trailing omitted fields are implicitly NULL. `UserType extends TupleType` (`UserType.java:52`) and
`UserType.buildValue` delegates to `TupleType.buildValue` (`UserType.java:194`), so the two forms are
byte-identical — only their CQL semantics differ. Authority:
`org.apache.cassandra.db.marshal.TupleType.buildValue` / `.split` (`TupleType.java:301-364`).

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

## previousUnfilteredSize (`prev_size` VInt)

Each row body opens with `prev_size` (`UnfilteredSerializer.previousUnfilteredSize`): the byte
distance from the start of the previous unfiltered, **including that item's own `prev_size`
VInt**. Readers parse and discard it; writers must emit it byte-correctly.

| Position in partition | `prev_size` value |
|-----------------------|-------------------|
| First unfiltered | partition-header byte size (NOT 0) — e.g. 30 for a UUID PK (`2 + 16 + 12`) |
| Subsequent row | full serialized byte length of the immediately preceding unfiltered |
| Static row | hard-coded `0`; static row does **not** advance the chain |
| First regular row after a static row | `header_size + static_row_size` (= its in-partition offset), NOT the static row size alone |

Verified in `cqlite-core/src/storage/sstable/writer/data_writer.rs` and pinned (with real-nb
anchors) by `cqlite-core/tests/issue_821_writer_byte_invariants.rs`. See Chapter 5,
"previousUnfilteredSize". Authority: `org.apache.cassandra.db.rows.UnfilteredSerializer`.

## 64-bit Offsets vs Narrow Format Fields

Two kinds of integers are easy to confuse. **Offsets** are 64-bit; several **format fields**
are deliberately narrow and must stay so.

| Field | Width | Kind |
|-------|-------|------|
| In-partition offset / Data.db offset | `u64` / `i64` | offset — must be 64-bit |
| Index.db data position (vint) | encodes full `u64` | offset — must be 64-bit |
| BTI partition-leaf position (`SizedInts`) | encodes full `i64` | offset — must be 64-bit |
| Index.db promoted-index offset array | `i32` | format field — stays 32-bit |
| Summary.db sample positions | `int[]` | format field — stays 32-bit |
| `DeletionTime.localDeletionTime` | `i32` (seconds) | format field — stays 32-bit |

A 32-bit narrowing of an **offset** past 2 GiB wraps negative and corrupts block offsets.
Verified by `issue_821_writer_byte_invariants.rs::finding16_*` (a >2 GiB offset round-trips
through the Index.db vint, the BTI leaf `SizedInts`, and the raw in-partition offset vint).
Do **not** widen the narrow format fields — their widths are part of the wire format.

## Column-Subset Mode Boundary (`Columns.serializeSubset`)

When `HAS_ALL_COLUMNS` (0x20) is clear, the columns-subset field encodes **missing** columns
and its encoding is selected by the regular-column (superset) count — there is **no flag**, so
the boundary is **decode-critical**:

| Superset size | Encoding |
|---------------|----------|
| `< 64` | single unsigned VInt bitmap; bit `i` = 1 → column `i` MISSING. Value `0` means "none missing"; Cassandra avoids it via `HAS_ALL_COLUMNS`, but CQLite's writer can still emit `0` on the subset path (all-present without `HAS_ALL_COLUMNS`), so readers must accept it. |
| `≥ 64` | large-subset: unsigned VInt count of missing columns, then the **smaller** of {present indices, missing indices} as **absolute** column indices (unsigned VInts, not deltas). |

A reader that always reads one VInt mis-parses every `≥ 64`-column table. CQLite's writer
implements both modes (`data_writer.rs::write_column_subset`, pinned by
`issue_824_column_subset_and_filter.rs` at 63/64/65 columns); CQLite's reader currently lacks
the `≥ 64` branch (see Appendix F). Authority: `Columns.Serializer.serializeSubset`
(`Columns.java:503-531`).

## VInt Safety Limits (Issue #264)

For security and memory safety, CQLite's `parse_vint_length()` enforces a maximum of **1GB** (`MAX_VINT_LENGTH = 1,073,741,824 bytes`) for any length field. This prevents:

- **Overflow on 32-bit platforms**: Where `usize` is only 4 bytes, values > 4GB would wrap
- **Memory exhaustion attacks**: Malicious input claiming huge lengths could cause OOM
- **Allocation attacks**: Preventing attempts to allocate unreasonable buffer sizes

The 1GB limit is generous for real Cassandra data (where individual values rarely exceed 16MB) while providing robust protection against malformed or malicious input.

**Error handling**: Values exceeding `MAX_VINT_LENGTH` return `nom::error::ErrorKind::TooLarge`.

## Partition Key Serialization

Partition keys are serialized differently depending on whether they are single-component or multi-component (composite) keys.

### Single-Component Keys

Single-component keys are serialized as raw bytes with no length prefix:

```
[value_bytes]  ← Direct type-specific encoding
```

**Examples**:
- `int(42)`: `0x00 0x00 0x00 0x2A` (4 bytes, big-endian i32)
- `bigint(1000)`: `0x00 0x00 0x00 0x00 0x00 0x00 0x03 0xE8` (8 bytes, big-endian i64)
- `text("hello")`: `0x68 0x65 0x6C 0x6C 0x6F` (5 bytes, UTF-8)
- `uuid(...)`: 16 bytes (raw UUID bytes)

### Multi-Component (Composite) Keys (Issue #380, #422)

Multi-component keys use 2-byte big-endian length prefixes with 0x00 separators between components:

```
[u16 BE: len1][component1_bytes][0x00]
[u16 BE: len2][component2_bytes][0x00]
...
[u16 BE: lenN][componentN_bytes]  ← NO trailing 0x00
```

**CRITICAL**: The 0x00 separator appears after each component EXCEPT the last.

**Size cap**: the whole composite key (all components + their `u16` prefixes + separators) is
itself written behind the partition header's outer `u16` length, so the *entire serialized
composite key* is capped at 65,535 bytes — a composite key can be invalid even when each
individual component is under 65,535 bytes (the per-component `u16` is not the only limit).

**Example 1**: `(int(42), text("hello"))` partition key
```
0x00 0x04                 ← length of int component (4 bytes)
0x00 0x00 0x00 0x2A       ← int value 42
0x00                      ← separator after first component
0x00 0x05                 ← length of text component (5 bytes)
0x68 0x65 0x6C 0x6C 0x6F  ← text value "hello"
                          ← NO trailing 0x00 after last component
```
Total: 13 bytes (2 + 4 + 1 + 2 + 5)

**Example 2**: `(year int, month int, day int)` with values `(2024, 6, 15)`:
```
00 04 00 00 07 E8 00    ← year=2024: len(4) + value + separator
00 04 00 00 00 06 00    ← month=6: len(4) + value + separator
00 04 00 00 00 0F       ← day=15: len(4) + value (NO trailing 0x00)
```
Total: 20 bytes (7 + 7 + 6)

**Size limits**:
- Single-component: limited to 65,535 bytes — the V5CompressedLegacy partition header
  prefixes the whole key with a **2-byte big-endian u16** length
  (`ByteBufferUtil.writeWithShortLength`, `SortedTablePartitionWriter.java:104-105`), NOT a
  1-byte length. See Chapter 5, "Partition Header Format".
- Multi-component: Each component limited to 65,535 bytes (u16 length prefix)

### Token Computation

Partition keys are mapped to tokens using Murmur3 hash for cluster distribution:

**Algorithm**:
1. Serialize partition key to bytes (single or composite format)
2. Compute Murmur3 32-bit hash with seed 0
3. Token is the hash value as i64 (sign-extended from i32)

**Example**:
```rust
// For int(42) partition key
let key_bytes = [0x00, 0x00, 0x00, 0x2A];
let hash = murmur3_32(key_bytes, seed=0);  // Returns u32
let token = hash as i32 as i64;             // Sign-extend to i64
```

**Decorated keys**: Writers use `DecoratedKey` structs that bundle the token and raw key bytes together:
```
DecoratedKey {
    token: i64,        // Murmur3 hash (for ordering)
    key: Vec<u8>,      // Raw key bytes (for partition header)
}
```

**Ordering requirement**: Partitions MUST be written to Data.db in ascending token order. For equal tokens, order by raw key bytes (lexicographic).

**Implementation references**:
- `cqlite-core/src/storage/write_engine/mutation.rs::PartitionKey::to_bytes()`
- `cqlite-core/src/storage/write_engine/mutation.rs::calculate_murmur3_token()`
- `cqlite-core/src/storage/sstable/key_digest.rs::KeyDigestComputer`

## Key Takeaways
- Expect VInt before variable-sized payloads; decode, then slice the value.
- **Every STRUCTURAL VInt in `Data.db` is UNSIGNED**: lengths/counts *and* the
  timestamp/TTL/localDeletionTime deltas all use `writeUnsignedVInt`/`writeUnsignedVInt32`
  (`SerializationHeader.java:165-184`).
- **ZigZag (signed VInt) in `Data.db` appears only inside a serialized `DurationType` payload** — its
  three components (`DurationSerializer.java:49-51`) — **wherever that payload occurs**, including
  nested inside a collection, tuple, or UDT (`DurationType.referencesDuration()`,
  `DurationType.java:96-99`; `TupleType.referencesDuration()` recurses over `allTypes()`,
  `TupleType.java:125-128`). No other `Data.db` field uses it. Scoped to `Data.db`: the `Index.db`
  promoted index also writes a **signed** VInt for its per-block width delta,
  `writeVInt(info.width - WIDTH_BASE)` (`IndexInfo.java:96,111-112`).
- **A non-frozen collection cell length-prefixes BOTH path and value with an unsigned VInt**, fixed-width
  element types included (`Cell.java:303-304` → the *column's* collection type, which is
  `VARIABLE_LENGTH`). The `valueLengthIfFixed()` raw-bytes shortcut is a **simple-cell** rule
  (`AbstractType.java:538-543`); a non-frozen `set<T>`'s missing value is `HAS_EMPTY_VALUE_MASK`, a flag,
  not a width.
- Signedness is invisible in the bytes (`0x05` = `5` unsigned, `-3` ZigZag) — take it from the
  field's serializer, never guess from the data.
- **Exception — not VInt at all**: tuple/UDT field lengths and frozen-collection counts/element
  lengths are fixed 4-byte BE `i32` (`TupleType.java:341-364`, `CollectionSerializer.java:67-92`).
- **Row size measurement**: VInt values like `row_size` are measured from AFTER the VInt is consumed (Issue #237).
- **Safety limit**: Length VInts are capped at 1GB to prevent overflow and allocation attacks (Issue #264).
- **Write guidance**: Use delta encoding for timestamps/TTL/deletion times; compute Statistics.db baselines first.
- **Partition keys**: Single-component keys have no length prefix; multi-component keys use 2-byte BE lengths with 0x00 separators EXCEPT after the last component (Issue #380, #422).
- **Token ordering**: Partitions must be written in ascending token order (Murmur3 hash of partition key bytes).

## References
- Cassandra 5.0.8: `SerializationHeader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`
- Cassandra 5.0.8: `rows` — `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows`
- Cassandra 5.0.8: `VIntCoding` (ZigZag ⇄ unsigned VInt) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/vint/VIntCoding.java`
- Cassandra 5.0.8: `DurationSerializer` (the only signed-VInt payload in `Data.db`, nesting included) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/serializers/DurationSerializer.java`
- Cassandra 5.0.8: `IndexInfo` (signed-VInt promoted-index width delta in `Index.db`) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/IndexInfo.java`
- Cassandra 5.0.8: `CollectionSerializer` (frozen collection fixed-width framing) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/serializers/CollectionSerializer.java`
- Cassandra 5.0.8: `TupleType` (tuple/UDT `i32`-BE field framing) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TupleType.java`

