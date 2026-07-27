## Data.db Format

This chapter describes the on-disk layout of partitions, rows, and cells in `Data.db`: how headers reference schema, how unfiltered rows, range tombstones, and markers are encoded, and how encodings like vints and cell flags are interpreted.

### In this chapter you will learn
- Partition headers and row/cluster layout basics
- Cell value encodings, varints/vints, collections/UDTs
- Deletions, range tombstones, TTLs and expiring cells
- How readers interpret flags and headers during parsing

## Partition and Row Layout

Minimal annotated example from `test_basic/simple_table` (trimmed and formatted):

```text
partition key = 4d4321e2-662b-4ba1-b75f-48e080727a52
row liveness ts = 2025-09-16T22:14:23.739Z
cells: account_balance=21088.5, active=false, age=75, name=(utf8) ...
```

Underlying file shows a partition stream with a serialization header followed by unfiltered rows and optional tombstone markers.

![Data.db row layout](diagrams/data-db-row-layout)
- Alt text: Annotated Data.db partition/row/cell structure
- Caption: Serialization header → unfiltered rows/markers → cells with flags and vints

## Encodings and Flags

VInt parsing (Cassandra-compatible), used across headers and lengths. For a concise implementation walkthrough, see Appendix C.

### VInt Sign Convention

A VInt on disk carries no marker for its own signedness, so the reader must know which variant the
writer used. In `Data.db` the answer is nearly uniform: **unsigned VInt (no ZigZag)**.

Unsigned VInt (`writeUnsignedVInt` / `writeUnsignedVInt32`):

| Field | Writer call | Source |
|-------|-------------|--------|
| Variable-width **simple** cell value length (`text`, `blob`, `varint`, `decimal`, `duration`) | `writeWithVIntLength` → `writeUnsignedVInt32` | `ValueAccessor.java:171-175` |
| Non-frozen collection cell path length (**always** present) | `ByteBufferUtil.writeWithVIntLength` | `CollectionType.java:361-366`, `ByteBufferUtil.java:356-360` |
| Non-frozen collection cell **value** length (present iff `HAS_EMPTY_VALUE` is clear — and then present even for a fixed-width element type) | `writeWithVIntLength` → `writeUnsignedVInt32` | `Cell.java:271,303-304`, `AbstractType.java:550-552`, `ValueAccessor.java:171-175` |
| Clustering-prefix header (2 bits per clustering column, one VInt per batch of ≤32 columns) | `writeUnsignedVInt(makeHeader(...))` | `ClusteringPrefix.java:455-475` |
| `row_size` and `prev_size` (`previousUnfilteredSize`) | `writeUnsignedVInt` | `UnfilteredSerializer.java:199-202` |
| Complex-column cell count | `writeUnsignedVInt32(data.cellsCount())` | `UnfilteredSerializer.java:277` |
| Columns-subset field (missing-column bitmap, large-subset count and indices) | `writeUnsignedVInt` / `writeUnsignedVInt32` | `Columns.java:521-525`, `:614-639` |
| Row/cell timestamp, TTL, and local-deletion-time deltas | `writeTimestamp` / `writeTTL` / `writeLocalDeletionTime` | `SerializationHeader.java:165-184` |

Signed (ZigZag) VInt in `Data.db` — only inside a serialized `DurationType` payload: its months,
days, and nanos are three `writeVInt` calls (`DurationSerializer.java:34,49-51`). That payload is not
limited to a top-level `duration` cell: wherever a `duration` is *nested* — `frozen<list<duration>>`,
`map<text, frozen<tuple<duration,int>>>`, a UDT field of type `duration` — those same three signed
VInts appear inside the enclosing value's bytes. Cassandra models exactly this recursion:
`DurationType.referencesDuration()` returns `true` (`DurationType.java:96-99`) and
`TupleType.referencesDuration()` recurses over `allTypes()` (`TupleType.java:125-128`). Every
*structural* VInt in `Data.db` — lengths, counts, temporal deltas — stays unsigned. This is a
`Data.db` statement only; other components do use signed VInt (notably the `Index.db` promoted-index
width delta, `IndexInfo.java:96,111-112`).

**Where the "fixed-width types carry no length prefix" rule applies: SIMPLE (non-collection) cells
only.** `AbstractType.writeValue` (`AbstractType.java:535-552`) branches on `valueLengthIfFixed()`:
`>= 0` writes the raw bytes with no prefix (`:538-543`), otherwise it writes an unsigned-VInt length
+ bytes (`:550-552`). The type consulted is the *column's* type, so for a non-frozen collection
column the branch is decided by `ListType`/`SetType`/`MapType` — none of which override
`valueLengthIfFixed()` — so **whenever a value is written at all it is length-prefixed**. Whether a
value is written at all is a separate, earlier decision made by the `HAS_EMPTY_VALUE` flag
(`Cell.java:271`, `:303-304`). See "Non-Frozen Collection Serialization" below.

Not VInt at all — fixed-width 4-byte big-endian `i32`: frozen-collection counts and element lengths,
and tuple/UDT field lengths. See "Frozen Collection Serialization" and "UDTs" below.

> **Warning: signedness is not discoverable from the bytes.** The byte `0x05` decodes to `5`
> unsigned and to `-3` under ZigZag. Never infer the variant from a byte pattern — take it from the
> field's serializer, as tabulated above. A structural length decoded with the wrong variant does
> not fail loudly; it silently desynchronizes the rest of the row stream.

The temporal deltas are unsigned because their baselines (`min_timestamp`, `min_ttl`,
`min_local_deletion_time` in `Statistics.db`) are the minimums across the SSTable, so every delta
is non-negative. ZigZag also exists in Cassandra's internode messaging serialization, which is not
an SSTable concern. See Appendix B for byte-level examples.

Readers interpret row/cell flags to distinguish live cells, TTLs, and tombstones; see Chapter 11 for tombstone semantics. Cross-link to Appendix B for a compact encoding summary.

Common cell flags (high level):
- live cell vs tombstone
- presence of timestamp, ttl, local deletion time
- empty/expiring cells

Bit-level flags (Cassandra 5.0, authoritative references):

| Bit | Name                    | When set                                                                  |
|-----|-------------------------|---------------------------------------------------------------------------|
| 0   | `IS_DELETED_MASK`       | Cell is a tombstone                                                       |
| 1   | `IS_EXPIRING_MASK`      | Cell is expiring (has TTL)                                                |
| 2   | `HAS_EMPTY_VALUE_MASK`  | Cell value is empty (zero-length but present)                             |
| 3   | `USE_ROW_TIMESTAMP_MASK`| Cell timestamp **equals** row timestamp — timestamp field is **omitted**  |
| 4   | `USE_ROW_TTL_MASK`      | Cell TTL/LDT **equals** row TTL/LDT — TTL and LDT fields are **omitted** |
| 5+  | (reserved)              | Format-specific extensions                                                |

Authoritative classes to consult in Cassandra 5.0:
- `org.apache.cassandra.db.rows.*` (e.g., `Unfiltered`, `Cell`, `BufferCell`)
- `org.apache.cassandra.db.SerializationHeader`
- `org.apache.cassandra.db.rows.SerializationHelper`

Endianness:
- Integers in SSTable payloads are big-endian unless otherwise specified; varints are MSB-first variable-length.
- Network/binary compatibility relies on consistent big-endian parsing for fixed-width fields.

## Deletions and TTL Semantics

- Partition tombstone: marks entire partition deleted at a timestamp
- Row tombstone: targets a specific clustering row
- Range tombstone: spans clustering ranges
- TTL/expiring: cells carry ttl and local deletion time; expired cells are omitted at read

### Collections and UDTs

**Collections** (list/set/map) have two storage modes:
- **Frozen** (`frozen<list<...>>`): Single-cell storage, entire collection serialized as one blob
- **Non-frozen** (`list<...>`): Multi-cell storage, each element stored as separate cell

#### Column Ordering: `SerializationHeader` Is the Positional Key

A row body carries **no column names**. Cells are written positionally, and both the cell
sequence and the missing-columns bitmap index into the column lists recorded in the
`SerializationHeader` (`Statistics.db`, see Chapter 7). The header order is therefore the
*only* authority for which cell is which — there is nothing in `Data.db` to fall back on, and
nothing may be inferred from the bytes (no-heuristics mandate, issue #28).

**The order.** Static columns and regular columns are two independent lists. Within each list,
Cassandra sorts by `ColumnMetadata.comparisonOrder` — a packed `long`
(`ColumnMetadata.java:107-116`):

```
(kind.ordinal() << 61) | (isComplex ? 1L << 60 : 0) | (position << 48) | (name.prefixComparison >>> 16)
```

The `isComplex` bit sits **above** every name bit, so within one list **all complex (multi-cell)
columns sort after all simple ones**, and only then does the column name break ties (by
`ColumnIdentifier.compareTo` → `prefixComparison`, then unsigned byte comparison of the name
bytes, `ColumnIdentifier.java:90-106`, `:217-225`). Note this partitions the whole list by
complexity — it is not a per-name adjacency rule: a complex `a_map` still sorts after a simple
`z_text`. "Complex" means non-frozen collection or non-frozen UDT: `ColumnMetadata.isComplex()` is
`cellPathComparator != null` (`:418-420`), and that comparator is built only for a non-primary-key
column whose `type.isMultiCell()` (`makeCellPathComparator`, `:200-207`). A `frozen<…>` column is
simple.

**Why the two must agree.** The chain is:
`RegularAndStaticColumns.Builder` builds each list as a BTree in `naturalOrder()`
(`RegularAndStaticColumns.java:156-172`) → `SerializationHeader.toComponent()` copies that order
into insertion-ordered `LinkedHashMap`s for the static/regular columns
(`SerializationHeader.java:250-259`) → the header serializer writes the names in that map order →
`UnfilteredSerializer.serializeRowBody` walks the row's columns in the same order
(`UnfilteredSerializer.java:240-259`) and `Columns.Serializer.serializeSubset` numbers bitmap bits
by that same index (`Columns.java:503-531`). A reader replays it in reverse (`deserializeRowBody`,
`UnfilteredSerializer.java:564+`, subset at `:606`).

A writer whose header order and cell order disagree produces an SSTable that misdecodes without
any error: bitmap bit *i* is attributed to the wrong column, a complex column can be dropped, and
the cells after it are read with the wrong type and framing (complex cells carry a cell path,
simple cells do not) — desynchronizing the rest of the row.

Authority: `org.apache.cassandra.schema.ColumnMetadata` (`comparisonOrder`, `compareTo`),
`org.apache.cassandra.db.RegularAndStaticColumns`, `org.apache.cassandra.db.SerializationHeader`,
`org.apache.cassandra.db.Columns.Serializer`, `org.apache.cassandra.db.rows.UnfilteredSerializer`.

CQLite implements the same key as `column_order_key(column) -> (is_complex, name)` in
`cqlite-core/src/storage/sstable/writer/data_writer/encoding.rs:9-11`, and applies it to **both**
sides: the header lists at
`cqlite-core/src/storage/sstable/writer/stats_writer/serialization_header.rs:159` (static) and
`:186` (regular), and the cell stream from the same key. Regression guard:
`cqlite-core/tests/issue_2035_collection_roundtrip.rs` (issue #2035 — `map<text,int>` + `set<int>`
plus a trailing simple column round-trip; the pre-fix writer sorted the header by name only, so the
complex columns landed in the wrong positions).

#### Frozen Collection Serialization

Frozen collections are stored as a single cell with the entire collection serialized as a binary blob.

> **The count and every element length are FIXED 4-byte big-endian `i32` — NOT VInt.** VInt is the
> dominant length-prefix pattern elsewhere in `Data.db`, so this is a common misread.
> `CollectionSerializer.pack` writes the count with `ByteBuffer.putInt` (`writeCollectionSize`,
> `CollectionSerializer.java:67-70`) and each element with `writeValue` — another `putInt` plus raw
> bytes (`:82-92`); `sizeOfValue` is hard-coded to `4 + size` (`:123-126`), and `-1` means NULL
> (`readValue`, `:94-101`). The *outer* cell value wrapping the blob still carries the usual
> unsigned-VInt length — the fixed-width framing starts inside the blob.

**Frozen List/Set Format** (identical for both types):
```
[element_count: 4-byte BE i32]        ← Fixed-width, NOT VInt
[for each element:
  [element_length: 4-byte BE i32]     ← Fixed-width per element, NOT VInt (-1 = NULL)
  [element_bytes]
]
```

**Example** (frozen list with 2 integers):
```
Hex: 00 00 00 02  00 00 00 04 00 00 00 2A  00 00 00 04 00 00 00 64
     |___________|  |_____________________| |_____________________|
     count=2        elem1: len=4, val=42   elem2: len=4, val=100
```

**Frozen Map Format** (a map packs each entry as two consecutive values, key then value, so the
framing is identical — every prefix is a fixed 4-byte BE `i32`):
```
[entry_count: 4-byte BE i32]          ← Fixed-width, NOT VInt
[for each entry:
  [key_length: 4-byte BE i32]         ← Fixed-width, NOT VInt
  [key_bytes]
  [value_length: 4-byte BE i32]       ← Fixed-width, NOT VInt
  [value_bytes]
]
```

Authority: `org.apache.cassandra.serializers.CollectionSerializer` (`pack`, `writeCollectionSize`,
`writeValue`) plus `MapSerializer.serializeValues` (`MapSerializer.java:66-79`), which flattens each
entry into a key buffer followed by a value buffer before packing.

**Example** (frozen map with 1 entry: "a" -> 42):
```
Hex: 00 00 00 01  00 00 00 01 61  00 00 00 04 00 00 00 2A
     |___________|  |____________| |_______________________|
     count=1        key: len=1, "a" val: len=4, int(42)
```

#### Non-Frozen Collection Serialization

Non-frozen collections are stored as multiple cells, one per element or entry. Each cell has a `cell_path` that identifies the element and a `cell_value` that contains the data.

**Non-frozen collection cell format** (complex columns) — every VInt below is **unsigned**:
```
[flags: u8]
[timestamp: unsigned VInt if not USE_ROW_TIMESTAMP_MASK]
[local_deletion_time: unsigned VInt32 if deleted/expiring (and not USE_ROW_TTL_MASK)]
[ttl: unsigned VInt32 if expiring (and not USE_ROW_TTL_MASK)]
[cell_path: unsigned VInt length + bytes]  ← ALWAYS length-prefixed
[value: unsigned VInt length + bytes]      ← present iff HAS_EMPTY_VALUE_MASK is CLEAR;
                                              when present, ALWAYS length-prefixed
```

Field order and presence are authoritative in `Cell.Serializer.serialize` (`Cell.java:268-305`,
layout comment at `:242-259`) — note local deletion time comes **before** TTL. The complex column is
preceded by an unsigned-VInt cell count (`UnfilteredSerializer.java:277`) and, when
`HAS_COMPLEX_DELETION` is set, a deletion time.

**`USE_ROW_TTL` (`0x10`): element expiry inherited from row liveness.** Collection element cells
use the *same* `Cell.Serializer` as simple cells, so they get the same TTL-elision optimization —
and readers of complex columns must implement it. The writer sets the flag only when **all four**
conditions hold (`Cell.java:275`):

1. the cell is expiring (`cell.isExpiring()`, which also sets `IS_EXPIRING`, `0x02`),
2. the row's primary-key liveness is itself expiring (`rowLiveness.isExpiring()`),
3. `cell.ttl() == rowLiveness.ttl()`, and
4. `cell.localDeletionTime() == rowLiveness.localExpirationTime()`.

When set, **both** the `local_deletion_time` and the `ttl` VInts are omitted from the cell
(`Cell.java:295-298`) and the reader takes both values from the row's TTL fields
(`Cell.java:318-322`). So it is not a "may" — given identical row and cell expiry the flag is
mandatory, and `IS_EXPIRING` is set alongside it (combined byte `0x12`, or `0x1a` with
`USE_ROW_TIMESTAMP`).

In practice a single `INSERT … USING TTL n` writing a whole non-frozen collection gives every
element the row's TTL and expiry, so every element cell carries `USE_ROW_TTL`. A later per-element
update with a different TTL (or into a row with no row-level TTL) fails condition 2 or 3 and writes
explicit `local_deletion_time` + `ttl` VInts with `USE_ROW_TTL` clear. A reader must therefore be
prepared for a **mix** of inherited and explicit expiry within one complex column.

Authority: `org.apache.cassandra.db.rows.Cell.Serializer` (`serialize`, `deserialize`).
CQLite implements the element-level side in
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs:22-132`
(`ElementExpiryShape` / `ExpiryHomogeneity` — a tri-state tracker that folds per-element
inherited-vs-explicit expiry back into a column-level answer), with the round-trip pinned by
`cqlite-core/tests/issue_2038_collection_ttl_expiring_cell.rs` (issue #2038).

**The path is always unsigned-VInt-length-prefixed. The value is unsigned-VInt-length-prefixed *iff*
`HAS_EMPTY_VALUE` (`0x04`) is clear — and when present it is length-prefixed even for a fixed-width
element type:**

- **`cell_path` — always** an unsigned-VInt length + bytes.
  `CollectionType.CollectionPathSerializer.serialize` calls
  `ByteBufferUtil.writeWithVIntLength(path.get(0), out)` (`CollectionType.java:361-366`), and
  `writeWithVIntLength` is `out.writeUnsignedVInt32(bytes.remaining())` followed by the bytes
  (`ByteBufferUtil.java:356-360`). The path is not gated by any flag.
- **`value` — present iff `HAS_EMPTY_VALUE` (`0x04`) is CLEAR, and that flag is SIZE-driven, not
  type-driven.** `Cell.Serializer.serialize` computes `boolean hasValue = cell.valueSize() > 0;`
  (`Cell.java:271`), sets `flags |= HAS_EMPTY_VALUE_MASK` when `!hasValue` (`:277-278`), and writes
  the value only `if (hasValue)` (`:303-304`). The reader mirrors it exactly:
  `boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;` (`:310`) and only then does it consume a
  length + bytes (`:329-339`; `skip` at `:381`, `:399-400`). So a **zero-length value carries no
  length VInt and no bytes** — the flag *replaces* the `0x00` length a reader might expect. Three
  common situations hit this, and the unifying rule is the **flag**, not any one of them:
    1. a non-frozen `set<T>` element — the datum lives in the cell **path** and
       `SetType.valueComparator()` is `EmptyType.instance` (`SetType.java:106-109`), so the value is
       always zero-length;
    2. **any** genuinely zero-length value — e.g. a `map<text,text>` entry whose value is `''`, or an
       empty blob element in a `list<blob>`. `set<T>` is just the case where this holds for *every*
       element;
    3. an **element tombstone** (`IS_DELETED`, `0x01`) — a deleted element has no value bytes
       (`HAS_EMPTY_VALUE_MASK`'s own comment at `Cell.java:264` calls out the tombstone case).
- **When a value IS present, it is unsigned-VInt-length-prefixed even for a fixed-width element
  type** — `list<int>` exactly as `list<text>`. `Cell.Serializer.serialize` writes it as
  `header.getType(column).writeValue(cell.value(), cell.accessor(), out)` (`Cell.java:303-304`), and
  `header.getType(column)` yields the **column's** type — the *collection* type, never the element
  type (`SerializationHeader.java:160-163`; the header's per-column map is built from `column.type`,
  `:250-257`). `ListType`, `SetType`, `MapType`, and their `CollectionType` base do **not** override
  `valueLengthIfFixed()`, so they inherit `AbstractType`'s `VARIABLE_LENGTH = -1`
  (`AbstractType.java:62`, `:490-493`). `AbstractType.writeValue` therefore takes the `else`
  branch → `accessor.writeWithVIntLength(value, out)` (`:550-552`) →
  `out.writeUnsignedVInt32(size(value))` + bytes (`ValueAccessor.java:171-175`).

Cassandra's own layout comment states both conditions together: the value size "is present unless
either the cell has the `HAS_EMPTY_VALUE_MASK`, or the value for columns of this type have a fixed
length" (`Cell.java:254-255`). For a collection cell the second condition can never fire, so the
flag is the only gate.

> **Common misreading.** "Fixed-width types skip the length prefix" is a genuine Cassandra rule, but
> it lives one level down — at the **simple (non-collection) cell**, where `header.getType(column)` is
> a scalar type that *does* override `valueLengthIfFixed()` (e.g. `Int32Type` returns `4`,
> `Int32Type.java:156-159`). For a complex/collection cell the same call returns the collection type,
> which never overrides it, so the raw-bytes branch at `AbstractType.java:538-543` can never fire for
> a collection cell. Reading `list<int>` as raw 4-byte values silently desynchronizes the cell stream.
> The mirror-image misreading is just as costly: reading a length VInt for a cell whose flags carry
> `HAS_EMPTY_VALUE` — a `map<text,text>` entry with an `''` value, for instance — consumes the *next*
> cell's flag byte as a length. **Always branch on the flag first.**

**Cell Path and Value by Collection Type**:

| Collection Type | cell_path (always unsigned-VInt length + bytes) | cell_value | Value framing |
|-----------------|-----------------------------------------------|------------|---------------|
| `list<T>` | TimeUUID (16 bytes) | Serialized element | Unsigned-VInt length + bytes whether `T` is fixed- or variable-width — **unless** `HAS_EMPTY_VALUE` is set (zero-length element, or tombstone), then nothing |
| `set<T>` | Serialized element | Empty (0 bytes) | None — `HAS_EMPTY_VALUE_MASK` always set, no length and no value bytes |
| `map<K,V>` | Serialized key | Serialized value | Unsigned-VInt length + bytes whether `V` is fixed- or variable-width — **unless** `HAS_EMPTY_VALUE` is set (e.g. value `''`, or tombstone), then nothing |

**List Element Ordering**:

Lists use TimeUUID (UUID version 1) for the `cell_path` to maintain insertion order. TimeUUIDs are time-sortable, ensuring elements remain in the order they were written. Each element gets a unique TimeUUID generated at write time.

**Example** (`list<int>`, 2 elements) — both the path and the value are length-prefixed (`10` =
unsigned VInt 16, `04` = unsigned VInt 4). Verbatim bytes from the `nb` SSTable
`test_collections/collection_table` (column `scores list<int>`, values 23 and 99), Snappy-decompressed:
```
Cell 1:  08  10 79f2a080a25111f0a3fef1a551383fb9  04  00 00 00 17
         ^   ^  ^                                 ^   ^
         |   |  16-byte TimeUUID path             |   int 23
         |   VInt path len = 16                   VInt value len = 4
         cell flags (0x08 = USE_ROW_TIMESTAMP)

Cell 2:  08  10 79f2a08aa25111f0a3fef1a551383fb9  04  00 00 00 63   (int 99)
```
The `04` before each 4-byte int is the point: a fixed-width element type does **not** remove the value
length prefix on a non-frozen collection cell.

**Set Element Storage**:

Sets use the element value itself as the `cell_path` for efficient membership testing. The
`cell_value` is always empty — `SetType.valueComparator()` is `EmptyType.instance`
(`SetType.java:106-109`) — so `cell.valueSize()` is 0, the cell sets `HAS_EMPTY_VALUE_MASK` (`0x04`)
and writes zero value bytes (`Cell.java:264`, `:271`, `:277-278`, `:303-304`). This lets Cassandra
check set membership by looking for a cell with a matching path.

A `set<T>` is **not** a special case in the serializer — it is simply the collection whose values are
*always* zero-length, so it hits the general `HAS_EMPTY_VALUE` path on every cell. Nothing about `set`
is type-cased: the same code path omits the value for a `map<text,text>` entry whose value is `''`.
And the omission is driven by the **flag**, not by the element type's width: a `set<int>` and a
`set<text>` both carry no value bytes.

**Example** (`set<text>`, 2 elements) — path length-prefixed, value absent entirely:
```
Cell 1:
  path:  05 | 61 6C 70 68 61   (VInt len 5, then "alpha")
  value: (none — HAS_EMPTY_VALUE_MASK set in flags)

Cell 2:
  path:  04 | 62 65 74 61      (VInt len 4, then "beta")
  value: (none — HAS_EMPTY_VALUE_MASK set in flags)
```

**Map Entry Storage**:

Maps use the serialized key as the `cell_path` and the serialized value as the `cell_value`. This allows efficient key lookups.

**Example** (`map<int,text>`, 2 entries: 1->"one", 2->"two") — path and value each length-prefixed:
```
Cell 1:
  path:  04 | 00 00 00 01   (VInt len 4, then int key 1)
  value: 03 | 6F 6E 65      (VInt len 3, then "one")

Cell 2:
  path:  04 | 00 00 00 02   (VInt len 4, then int key 2)
  value: 03 | 74 77 6F      (VInt len 3, then "two")
```

A fixed-width *value* type changes nothing. Verbatim bytes for `metadata_map map<text,bigint>` in
`test_collections/collection_table` (entry `"want" -> 104237`):
```
08  04 77 61 6E 74  08  00 00 00 00 00 01 97 2D
^   ^  ^            ^   ^
|   |  "want"       |   bigint 104237 (0x1972D)
|   VInt path len=4 VInt value len = 8
cell flags
```

**An empty map value takes the flag path, not a `0x00` length.** Because `HAS_EMPTY_VALUE` is decided
by `cell.valueSize() > 0` (`Cell.java:271`), an entry whose value is the empty string is framed like a
set element — path present, value gone entirely. Schematically, for `map<text,text>` entry
`"k" -> ''`:
```
0C  01 6B
^   ^  ^
|   |  "k"
|   VInt path len = 1
cell flags (0x08 USE_ROW_TIMESTAMP | 0x04 HAS_EMPTY_VALUE) — no value length, no value bytes
```
A reader that unconditionally reads a value-length VInt here consumes the next cell's flag byte and
desynchronizes. Note this is distinct from a NULL: a NULL map *value* is not expressible in CQL, and a
NULL top-level column is never represented by a *cell* — for a simple column it is absence from the
column subset. (A non-frozen collection is the one case where "reads as NULL" does not imply "absent
from the subset": an emptied collection is present in the subset with a complex deletion and zero
cells — see "Empty collections" below.)

**Implementation References** (CQLite):
- Frozen collections: `cqlite-core/src/storage/sstable/writer/data_writer/encoding.rs::serialize_value_into()`
  (its `write_len_prefixed_i32` helper emits the fixed 4-byte BE prefixes);
  reader `.../reader/parsing/row_decoder/frozen.rs::parse_frozen_{list,set,map}_value()`
- Non-frozen collections — **the length prefix is never element-width-driven on either side**, but the
  two writer paths differ on the `HAS_EMPTY_VALUE` gate:
  - Reader `.../reader/parsing/row_decoder/complex_column.rs::parse_complex_cell_value()` (`:973`)
    is correct: it decodes the flag byte (`has_empty_value = (flags & 0x04) != 0`, `:1012`) and only
    `parse_vuint`s a value length when neither `IS_DELETED` nor `HAS_EMPTY_VALUE` is set
    (`:1128`, `:1136`).
  - Per-element writer `.../writer/data_writer/complex.rs::write_complex_element_cell()` (`:863`) is
    correct: it derives the flag (`:884-891`) and emits the length + bytes only when
    `HAS_EMPTY_VALUE` is clear (`:960-969`). `write_set_complex_cells()` (`:594`) likewise always
    sets `CELL_HAS_EMPTY_VALUE` and writes no value.
  - **Known gap (issue #2970)**: the *whole-column* writers `write_map_complex_cells()` (`:646`) and
    `write_list_complex_cells()` (`:705`) hardcode `flags = 0` (`:683`, `:737`) and emit
    `encode_unsigned(len)` unconditionally (`:694`, `:747`). A zero-length element value therefore
    goes out as `flags=0` + `0x00` where Cassandra writes `flags=0x04` + nothing. Cassandra **reads
    this without error** — on `flags=0` it expects a length VInt (`Cell.java:310`), consumes our `0x00`
    as `l=0` (`AbstractType.java:590`), and `read(in, 0)` returns `EMPTY_BYTE_BUFFER` without touching
    the stream (`ByteBufferUtil.java:444-448`), so framing stays aligned and the value decodes
    identically. The defect is **byte parity**: one extra byte and a `flags` byte off by `0x04`, which
    breaks byte-for-byte compaction parity, `Digest.crc32` digest matching, and `row_size`/`prev_size`
    accounting. See Appendix F.
- The fixed-width no-prefix rule lives on the **simple**-cell path:
  `.../writer/data_writer/encoding.rs::cell_value_uses_length_prefix()` (`:461`, issue #1672), used by
  `write_cell_value_into()` (`:353`) which `cells.rs` calls for simple cells (`:63`, `:108`, `:200`,
  `:230`).
- Tests: `cqlite-core/tests/issue_2035_collection_roundtrip.rs`,
  `cqlite-core/tests/collection_sstable_integration_test.rs`

**UDTs** (User-Defined Types) serialize fields in schema order with 4-byte BE length prefixes:
```
[field_1_length: 4-byte BE i32][field_1_data]
[field_2_length: 4-byte BE i32][field_2_data]
...
```

**UDT field length semantics** (confirmed via Issue #220):
- `-1` (0xFFFFFFFF): Field is NULL
- `0` (0x00000000): Field is empty (zero-length but present)
- `>0`: Number of bytes of field data following
- Trailing omitted fields are implicitly NULL

**Tuple field framing is identical to UDT field framing.** A `tuple<T1, T2, ...>` value is just its
fields concatenated, each behind a fixed 4-byte BE signed `i32` length: `-1` (0xFFFFFFFF) = NULL,
`0` = empty (zero-length but present), `>0` = byte count. Neither form writes a field **count** —
arity comes from the schema, and trailing omitted fields are implicitly NULL. Tuples and UDTs differ
only in semantics (tuples are positional and unnamed; UDTs are named-field records); on disk they
share one serializer, because `UserType extends TupleType` (`UserType.java:52`) and
`UserType.buildValue` delegates straight to `TupleType.buildValue` (`UserType.java:194`).

Authority: `TupleType.buildValue` (`TupleType.java:341-364`) writes `putInt(-1)` for a null component
and `putInt(size)` otherwise; `TupleType.split` (`:301-339`) reads a 4-byte length per component,
treats `size < 0` as null, and returns short when the buffer ends early. CQLite mirrors both sides in
`writer/data_writer/encoding.rs:307-313` and
`reader/parsing/row_decoder/frozen.rs::parse_tuple_elements_raw()` (`:515-583`).

**Critical distinction**: The **outer** type determines storage:
- `list<frozen<udt>>` = multi-cell (each UDT element is separate cell)
- `frozen<list<udt>>` = single-cell (entire list is one blob)

**Empty collections: frozen stores a zero-count blob; non-frozen stores no cells but is *not*
absent.** The two modes diverge, and the non-frozen side is easy to get wrong:

- **Frozen** (`frozen<set<text>>` written as `{}`): a normal single cell whose value is a
  zero-element blob — the 4-byte BE count `00 00 00 00` and nothing else
  (`CollectionSerializer.pack` always writes the count, `CollectionSerializer.java:52-64`). This is
  a present, non-null value and is distinguishable from a column that was never written.
- **Non-frozen** (`set<text>` written as `{}`): **zero element cells**, but the column is still
  present in the row — because writing a non-frozen collection is a *replace*, and a replace emits a
  **complex (collection) deletion** covering the prior contents before adding cells
  (`Lists.Setter`/`Sets.Setter`/`Maps.Setter` call
  `UpdateParameters.setComplexDeletionTimeForOverwrite`, `UpdateParameters.java:202-205`, then add
  zero cells because the literal is empty: `Sets.Adder.doAdd` returns early on
  `elements.size() == 0`). So the row carries `HAS_COMPLEX_DELETION` and, for that column, a
  deletion time plus a cell count of `0`.

  On the read side the collection reconciles to zero elements, which CQL surfaces as `NULL` — an
  empty non-frozen collection and a null one are indistinguishable *to a query*. But they are
  distinguishable **on disk**, and a reader must not conflate the two: `ComplexColumnData`'s
  invariant is `cells.length > 0 || !complexDeletion.isLive()` (`ComplexColumnData.java:64-70`) —
  i.e. zero cells is legal precisely when a complex deletion is present, and a column with neither is
  dropped entirely (`update`, `:229-235`).

Verbatim from the `nb_empty_collections` parity fixture (`test_types`, one row with all six columns
written empty in a single `INSERT`), via `sstabledump`:

```
"cells": [
  { "name": "fl", "value": [] },                 // frozen<list<int>>  — present, zero-count blob
  { "name": "fm", "value": {} },                 // frozen<map<text,int>>
  { "name": "fs", "value": [] },                 // frozen<set<text>>
  { "name": "ml", "deletion_info": { … } },      // list<int>  — complex deletion, ZERO cells
  { "name": "mm", "deletion_info": { … } },      // map<text,int>
  { "name": "ms", "deletion_info": { … } }       // set<text>
]
```

Note what the non-frozen entries are *not*: they are neither missing from the row nor a row/cell
tombstone. `sstabledump` prints a bare `deletion_info` object for a complex column when
`complexDeletion()` is live-less and there are no cells to follow
(`JsonTransformer.serializeColumnData`, `:400-429`) — that is exactly the on-disk shape above.
Reference: `test-data/datasets/sstables/test_types/nb_empty_collections-*/nb-1-big-Data.db.jsonl`;
generator `test-data/scripts/generate-cql-type-parity.sh` (`[B9]`), schema
`test-data/schemas/cql-type-parity.cql`.

A genuinely **absent** non-frozen collection — one never written for that row — is signalled the
same way as any absent column: its bit is set in the missing-columns bitmap (or the column is simply
outside the subset), with no deletion time and no cells. Distinguishing "written empty" from "never
written" therefore requires the complex deletion, not the cell count.

See `tables/type-mapping-complex.md` for detailed format specifications.

### Counter Cells (Issue #241)

Counter columns store a `CounterContext` structure, not a raw i64 value. The CounterContext tracks counter updates across multiple replicas (shards).

**Cell format**:
```
[VInt length]           ← Length of CounterContext bytes
[CounterContext]        ← Variable-length structure below
```

**CounterContext format** (from `CounterContext.java`):

| Field | Size | Description |
|-------|------|-------------|
| header_size | 2 bytes | BE signed short - number of shards |
| indices | 2 * \|header_size\| bytes | Shard type indicators (negative = global) |
| shards | 32 * \|header_size\| bytes | Each shard: counter_id (16) + clock (8) + count (8) |

**Shard structure** (32 bytes each):
```
[counter_id: 16 bytes]    ← Replica's CounterId (UUID)
[clock: 8 bytes]          ← Logical clock (BE unsigned long)
[count: 8 bytes]          ← Counter value for this shard (BE signed long)
```

The counter value is the **sum of all shard counts**, matching Cassandra's `total()` function.

**Example** (single-shard counter):
```
24                         ← VInt length (36 bytes)
0001                       ← header_size = 1
8000                       ← header index (0x8000 = global shard at index 0)
f35cf98a220c40fb8b04f4ff7ffcf681  ← counter_id (16 bytes)
00064073 23d1d210          ← clock (8 bytes)
00000000 00000029          ← count = 41 (8 bytes)
```

Reference: `org.apache.cassandra.db.context.CounterContext` in Cassandra 5.0 source.

### Key Takeaways
- `Data.db` is schema-driven and encodes partitions as unfiltered row streams.
- VInts and bit flags compactly encode sizes, timestamps, and cell metadata.
- Every *structural* `Data.db` VInt is **unsigned** — lengths, counts, and the
  timestamp/TTL/localDeletionTime deltas alike. Signed (ZigZag) VInts appear only inside a serialized
  `DurationType` payload (its three components), wherever that payload occurs — including nested inside
  a collection, tuple, or UDT.
- A **non-frozen collection** cell length-prefixes **both** the path and the value with an unsigned
  VInt, *even for a fixed-width element type* (`list<int>`, `map<text,bigint>`): `writeValue` sees the
  collection type, which is `VARIABLE_LENGTH`. The `valueLengthIfFixed()` raw-bytes shortcut applies to
  **simple (non-collection)** cells only. A non-frozen `set<T>` carries no value bytes at all, via
  `HAS_EMPTY_VALUE_MASK` — a flag, not a width.
- Frozen-collection counts/element lengths and tuple/UDT field lengths are **fixed 4-byte BE `i32`**,
  not VInt; tuples and UDTs share one serializer.
- Tombstones and TTLs are first-class and affect reconciliation.

### Troubleshooting
- If parsed sizes seem inconsistent, verify VInt decoding and endian assumptions.
- For collections with unexpected nulls, check for element tombstones and TTL expiration handling.

### References
- Cassandra 5.0.8:
  - Rows and tombstones: `org.apache.cassandra.db.rows.*` (`Unfiltered`, `RangeTombstoneMarker`)
  - Serialization header: [org.apache.cassandra.db.SerializationHeader](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)
  - Cell framing: [org.apache.cassandra.db.rows.Cell](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/Cell.java) (`Serializer`)
  - Cell value length prefix: [org.apache.cassandra.db.marshal.AbstractType](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/AbstractType.java) (`writeValue`) + [ValueAccessor](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/ValueAccessor.java) (`writeWithVIntLength`)
  - Frozen collection framing: [org.apache.cassandra.serializers.CollectionSerializer](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/serializers/CollectionSerializer.java)
  - Tuple/UDT framing: [org.apache.cassandra.db.marshal.TupleType](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TupleType.java), [UserType](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/UserType.java)
  - VInt/ZigZag primitives: [org.apache.cassandra.utils.vint.VIntCoding](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/vint/VIntCoding.java)
  
For implementation details, see Appendix C.

## V5CompressedLegacy Row Header Format (Cassandra 5.0)

The V5CompressedLegacy format (BigFormat with compression, "nb" file prefix) uses a structured row header with delta-encoded metadata fields. This format is used by Cassandra 5.0 SSTables with the legacy "big" format and compression enabled.

### Row Structure (Corrected - Issue #213, Issue #237)

The complete row format, confirmed via Cassandra's `UnfilteredSerializer.java`:

```
[row_flags: u8]
[extended_flags: u8 if 0x80 set]
[clustering_prefix: variable]          ← For tables with clustering keys
[row_size: VInt]                       ← Size measured from AFTER this VInt (Issue #237)
[prev_size: VInt]
[timestamp: VInt if 0x04 set]          ← Delta from min_timestamp (unsigned VInt)
[ttl: VInt32 if 0x08 set]              ← TTL delta from min_ttl (unsigned VInt32)
[liveness_ldt: VInt32 if 0x08 set]    ← Local expiration time delta from min_local_deletion_time (unsigned VInt32)
[deletion: 2 VInts if 0x10 set]        ← markedForDeleteAt delta (unsigned VInt) + local_deletion_time delta (unsigned VInt32)
[column_bitmap: VInt + bytes if NOT 0x20]
[cell_data...]
```

**Critical Notes**:

1. **Clustering Prefix Ordering (Issue #213)**: For tables WITH clustering keys, the clustering prefix comes IMMEDIATELY after flags and BEFORE `row_size`. This differs from initial documentation which placed `row_size` immediately after flags.

2. **`row_size` Measurement (Issue #237)**: The `row_size` value is measured from the position AFTER the `row_size` VInt is consumed, NOT from where the VInt starts. This matches Cassandra's `getFilePointer()` semantics:
   ```
   next_row_offset = position_after_row_size_vint + row_size_value
   ```

3. **No Trailing Field (Issue #237)**: There is NO trailing field after row data in V5CompressedLegacy format. The next partition or row starts immediately after `row_size` bytes from the position after the VInt.

4. **`prev_size` is `previousUnfilteredSize`**: the `prev_size` VInt is the byte distance from the start of the *previous* unfiltered, **including that previous item's own `prev_size` VInt** (it is part of the chain). It is written by `UnfilteredSerializer` but **skipped by readers** (it exists to support backward iteration). See the dedicated subsection [previousUnfilteredSize](#previousunfilteredsize-the-prev_size-chain) below for the exact convention CQLite emits and the static-row exception.

### Clustering Prefix Format

For tables with clustering keys, values are encoded between flags and row_size:

```
[header: unsigned VInt]                ← 2 bits per clustering column
[value_1: type-specific]               ← Only if state indicates PRESENT
[value_2: type-specific]
...
```

The header VInt uses 2 bits per column to indicate state:
- `00` (0): Value PRESENT - followed by type-specific bytes
- `01` (1): Value EMPTY - zero-length (no bytes follow)
- `10` (2): Value NULL - no bytes follow
- `11` (3): Reserved

> **Headers are batched at 32 columns.** Because a 64-bit header holds only 32 two-bit states,
> `serializeValuesWithoutSize` emits one unsigned-VInt header per batch of up to 32 clustering
> columns, then that batch's values, then the next header. Tables with ≤32 clustering columns —
> effectively all real tables — see exactly one header, but a reader must not assume that.
> Source: `ClusteringPrefix.java:455-475` (`makeHeader` at `:548-562`).

Type-specific encoding:
- **Fixed-width types** (timestamp, int, bigint, UUID): Raw bytes, no length prefix
  (`AbstractType.writeValue` skips the prefix when `valueLengthIfFixed() >= 0`, `:538-543`) — this holds
  for clustering values and **simple** cells, never for a non-frozen collection cell
- **Variable-width types** (text, varchar, blob): **unsigned** VInt length prefix + bytes

### Unfiltered Markers (Issue #229 Fix)

Between rows in a partition, or at the end of a partition, the parser may encounter special markers:

| Marker | Hex | Meaning |
|--------|-----|---------|
| END_OF_PARTITION | 0x01 | Signals end of partition - nothing follows this byte |
| IS_MARKER | 0x02 | Range tombstone marker (boundary or bound) |

**END_OF_PARTITION (0x01)**:
- Written by `UnfilteredSerializer.writeEndOfPartition()` as exactly `0x01`
- When detected, the partition is complete; move to next partition
- Critical for tables with clustering keys to avoid misinterpreting marker as row data

**IS_MARKER (0x02)**:
- Indicates a range tombstone boundary
- Followed by clustering bound/boundary data and deletion time(s)
- Must be skipped when parsing row data

**Implementation Note**: CQLite uses bitwise AND (`flags & IS_MARKER != 0`) to detect IS_MARKER because markers can have additional flag bits set (e.g., 0x52 = IS_MARKER | HAS_DELETION | HAS_COMPLEX_DELETION). END_OF_PARTITION still uses exact match (0x01) as it is always written alone without other flags.

### Row Flags

| Flag | Hex  | Meaning            | Details |
|------|------|--------------------|---------|
| 0x04 | HAS_TIMESTAMP      | Timestamp delta present | Delta-encoded from Statistics.db min_timestamp |
| 0x08 | HAS_TTL           | TTL delta present | Delta-encoded from Statistics.db min_ttl |
| 0x10 | HAS_DELETION      | Deletion time present | Two VInts in Cassandra canonical order: (1) markedForDeleteAt delta (unsigned VInt, base `min_timestamp`, microseconds — the authoritative tombstone reconciliation timestamp), then (2) local_deletion_time delta (unsigned VInt32, base `min_local_deletion_time`, seconds). See `DeletionTime.Serializer` / `SerializationHeader.writeDeletionTime`. |
| 0x20 | HAS_ALL_COLUMNS   | All columns present (no bitmap) | When set, all schema columns have values (no NULLs) |
| 0x80 | `EXTENSION_FLAG` (source) / `HAS_EXTENDED_FLAGS` (guide alias) | Extended flags byte follows | Reserved for future format extensions |

### Delta Decoding

All metadata fields use delta encoding against minimum values from Statistics.db:

```
absolute_timestamp = min_timestamp + timestamp_delta
absolute_ttl = min_ttl + ttl_delta
absolute_marked_for_delete_at = min_timestamp + marked_for_delete_at_delta   # microseconds (reconciliation ts)
absolute_local_deletion_time   = min_local_deletion_time + local_deletion_time_delta   # seconds
```

**Example**: If Statistics.db shows `min_timestamp = 1759713125983682` and row header contains `timestamp_delta = 1000`, the absolute timestamp is `1759713125984682` (microseconds since epoch).

### previousUnfilteredSize (the `prev_size` chain)

The `prev_size` VInt inside each row body is Cassandra's `previousUnfilteredSize`
(`UnfilteredSerializer`). It records the serialized byte length of the *previous*
unfiltered in the partition so a reader can iterate backwards. CQLite's reader
parses but discards it (`row_decoder::parse_row_metadata` reads it into
`_prev_size`); the writer must still emit the byte-correct value so the file matches
Cassandra and round-trips through `sstabledump`.

Exact convention (verified in
`cqlite-core/src/storage/sstable/writer/data_writer.rs` —
`write_partition` / `write_partition_with_index_blocks` — and pinned by
`cqlite-core/tests/issue_821_writer_byte_invariants.rs`, which anchors against real
Cassandra "nb" SSTables):

- **First unfiltered in a partition**: `prev_size` = the **partition-header byte
  size** (NOT 0). For a UUID partition key the header is `2 (u16 key length) + 16
  (key) + 12 (deletion time) = 30`, and the first row carries `prev_size = 30`
  (anchored against `test_basic.uncompressed_table`).
- **Subsequent rows**: `prev_size` = the full serialized byte length of the
  immediately preceding unfiltered, **including its own `prev_size` VInt** (the
  measurement spans `flags + ext_flags + clustering_prefix + row_size_vint +
  row_size_body`).
- **Static row exception**: a static row hard-codes `prev_size = 0` AND is **not**
  treated as the "previous unfiltered" for the chain. Its bytes still advance the
  running in-partition position, so the first *regular* row after a static row
  carries `prev_size = header_size + static_row_size` (= that regular row's own
  in-partition offset), **not** the static row's size alone. Anchored against
  `test_basic.static_columns_table` (header 30 + static 16 → regular-row
  `prev_size = 46`).

Authority: `org.apache.cassandra.db.rows.UnfilteredSerializer`
(`previousUnfilteredSize`).

### Offsets are 64-bit

In-partition offsets and Data.db offsets are **64-bit** (`u64`/`i64`). A 32-bit
narrowing of an offset past 2 GiB wraps negative and corrupts block offsets, so
every writer-path offset encoder treats them as 64-bit (verified by
`cqlite-core/tests/issue_821_writer_byte_invariants.rs::finding16_*`, which
round-trips a >2 GiB offset through the Index.db data-position VInt, the BTI
partition-leaf `SizedInts`, and the raw in-partition offset VInt).

The remaining narrow integer fields are **format fields, not offsets**, and stay at
their declared widths: the Index.db promoted-index offset array (`i32`), the
Summary.db sample positions (`int[]`), and `DeletionTime.localDeletionTime`
(`i32`, seconds). Do not "widen" these — see Appendix B.

### Column Bitmap

When `HAS_ALL_COLUMNS` (0x20) is **NOT** set, a columns-subset field follows the metadata fields.
Cassandra's on-disk format (`Columns.Serializer.serializeSubset`, `Columns.java:503-531`) encodes
**missing** columns, not present columns:

- **< 64 columns in superset**: write a single unsigned VInt where bit `i` = 1 means the column at
  index `i` is **absent** (set bit = MISSING). The value `0` means "all present"; Cassandra avoids
  it by emitting `HAS_ALL_COLUMNS` instead. CQLite's writer (`write_column_subset`) *does* still
  emit `encode_unsigned(0)` in the all-present case when it reaches the subset path without
  `HAS_ALL_COLUMNS` set (e.g. a row carrying deletions whose every regular column is nonetheless
  covered) — so a reader must accept a `0` subset VInt as "no columns missing", not treat it as
  reserved/impossible.
- **≥ 64 columns**: write the *large-subset* form — an unsigned VInt count of missing columns,
  then either the present-column indices or the missing-column indices (whichever is the **smaller**
  set) as **absolute** column indices, each an unsigned VInt (CQLite's `write_column_subset`
  writes absolute indices, not deltas).

> **The `< 64` vs `≥ 64` mode boundary is DECODE-CRITICAL.** The two modes are not
> distinguished by any flag — the reader must select the branch from the superset
> (regular-column) count alone. A reader that always treats the subset field as a
> single VInt mis-parses every `≥ 64`-column table: it consumes only the
> missing-count and then mis-reads the trailing index VInts as cell data, corrupting
> the rest of the row stream. CQLite's writer implements both modes
> (`data_writer.rs::write_column_subset`, pinned at the 63/64/65 boundary by
> `cqlite-core/tests/issue_824_column_subset_and_filter.rs`); the **reader currently
> lacks the `≥ 64` large-subset branch** (it reads one VInt into a `u64` bitmap) —
> see Appendix F.

Authority: `org.apache.cassandra.db.Columns.Serializer.serializeSubset`
(`Columns.java:503-531`).

CQLite's writer (`data_writer.rs::write_column_subset`) follows this authoritative
encoding directly — the missing-column bitmap for `< 64` columns and the large-subset
count+index form for `≥ 64` — it does **not** use a separate "present-bit" format.

**Example**: For a table with 10 columns, if columns 1 and 3 are absent:
- Bitmap VInt: bit 1 and bit 3 set = `0b00001010` = `0x0a`

### Validation

This format specification is confirmed through:
- Implementation: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder.rs`
- Cassandra Source: `org.apache.cassandra.db.rows.UnfilteredSerializer.java` (lines 151-210)
- Integration tests: All 26/33 test tables pass (tables with clustering keys now work)
- Test data: Real Cassandra 5.0 SSTables including sensor_data, wide_partition_table, app_metrics

### References

- Cassandra 5.0.0 Source: `org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer`
- SerializationHeader: Delta encoding semantics for Statistics.db integration
- Implementation research: See `docs/sstables-definitive-guide/ISSUE_162_LEARNINGS.md` for detailed findings

## Writing Data.db Files

This section documents how to construct valid V5CompressedLegacy Data.db files for write operations. All write operations must maintain the format described above while adhering to strict ordering and encoding rules.

### Partition Ordering

Partitions MUST be written in order of their Murmur3 token values (ascending). Within each token, partitions with the same token (rare but possible) are ordered by partition key bytes lexicographically.

**Enforcement**: The caller (write engine) is responsible for partition ordering. The DataWriter accepts partitions in the order provided.

### Partition Header Format

Each partition begins with:

```
[key_length: u16 BE]       ← Partition key length (2 bytes, big-endian)
[key_bytes]                ← Raw partition key bytes
[deletion_time: 12 bytes]  ← ALWAYS present, fixed-width: i32 localDeletionTime (4 BE)
                              + i64 markedForDeleteAt (8 BE). A LIVE partition is the
                              sentinel (localDeletionTime = i32::MAX = 0x7FFFFFFF,
                              markedForDeleteAt = i64::MIN) — it is NOT a 1-byte 0x80.
```

The partition-level `DeletionTime` here is the fixed 12-byte non-delta form (unlike the
delta-encoded row/cell deletion in the row body). This is why the partition header is exactly
`2 + key_length + 12` bytes — the basis for the first row's `previousUnfilteredSize` (see the
prev_size section above, anchored to real Cassandra "nb" SSTables).

Source: `SortedTablePartitionWriter.java:104-105` —
`ByteBufferUtil.writeWithShortLength(key.getKey(), writer)` writes a 2-byte big-endian u16
length followed by key bytes; then `DeletionTime.getSerializer(version).serialize(...)`.

**There is no leading partition_flags byte and no trailing unknown_field.** The partition key
length is a 2-byte unsigned short (max 65,535 bytes), not a 1-byte limit. For a **composite**
(multi-column) partition key this u16 caps the *entire serialized key* — all components plus
their inner `u16` length prefixes and separators — at 65,535 bytes, so a composite key can be
invalid even when every individual component is itself under 65,535 bytes.

**End-of-Partition Marker**: Each partition ends with a single byte 0x01 (END_OF_PARTITION) after all rows.

### Row Ordering

Within a partition, rows MUST be ordered by their clustering keys according to the table's clustering order (ASC or DESC per column). For tables without clustering keys, there is at most one row per partition.

**Enforcement**: The caller must provide rows in the correct clustering order. The DataWriter writes rows in the order provided.

### Writing Partitions

Complete partition structure:

```
[partition_header]          ← As described above
[row_1]                    ← Multiple rows in clustering order
[row_2]
...
[END_OF_PARTITION: 0x01]   ← Single byte marker
```

### Row Flag Construction

Row flags are constructed by OR-ing flag bits based on the row's properties:

| Condition | Flag | Hex | Result |
|-----------|------|-----|--------|
| Timestamp present (always for writes) | ROW_HAS_TIMESTAMP | 0x04 | Include timestamp delta |
| TTL specified | ROW_HAS_TTL | 0x08 | Include TTL delta |
| Row deletion | ROW_HAS_DELETION | 0x10 | Include deletion fields |
| All columns present (no NULLs) | ROW_HAS_ALL_COLUMNS | 0x20 | Skip column bitmap |
| Complex column deletion | ROW_HAS_COMPLEX_DELETION | 0x40 | Complex deletion present |
| Extended flags follow | `EXTENSION_FLAG` | 0x80 | Extended flags byte follows |

**ROW_HAS_ALL_COLUMNS Truth Table**:

This flag is set when ALL of these conditions are true:
1. All operations are writes (no deletes)
2. No NULL values present
3. Number of columns matches schema column count

| All Writes? | No NULLs? | Column Count Matches? | HAS_ALL_COLUMNS |
|-------------|-----------|----------------------|-----------------|
| Yes | Yes | Yes | SET (0x20) |
| Yes | Yes | No | NOT SET |
| Yes | No | Yes | NOT SET |
| No | Yes | Yes | NOT SET |

**Example**:
- Row with timestamp, no TTL, all columns present: `0x04 | 0x20 = 0x24`
- Row with timestamp and TTL, some columns NULL: `0x04 | 0x08 = 0x0c`
- Row with timestamp, TTL, and deletion: `0x04 | 0x08 | 0x10 = 0x1c`

### Cell Flag Construction

Cell flags are constructed based on cell properties:

| Condition | Flag | Hex | Result |
|-----------|------|-----|--------|
| Cell is tombstone | CELL_IS_DELETED | 0x01 | Include deletion fields |
| Cell has TTL | CELL_IS_EXPIRING | 0x02 | Include TTL fields |
| Value is empty string | CELL_HAS_EMPTY_VALUE | 0x04 | Zero-length value |
| Use row timestamp | CELL_USE_ROW_TIMESTAMP | 0x08 | Skip cell timestamp |
| Use row TTL | CELL_USE_ROW_TTL | 0x10 | Skip **both** cell TTL and cell local_deletion_time |

**CELL_USE_ROW_TIMESTAMP Truth Table**:

Most cells use the row-level timestamp for efficiency. Cells need their own timestamp when the cell timestamp differs from the row timestamp (e.g., different write operations).

| Cell Type | Timestamp Differs? | USE_ROW_TIMESTAMP |
|-----------|-------------------|-------------------|
| Regular write | No | SET (0x08) |
| Regular write | Yes | NOT SET |
| Tombstone | N/A | NOT SET (always own timestamp) |

**CELL_IS_DELETED Truth Table**:

Tombstone cells have special flag requirements:

| Cell Operation | Flag Bits | Timestamp | Deletion Time | Value |
|---------------|-----------|-----------|---------------|-------|
| Regular write | 0x08 (USE_ROW_TIMESTAMP) | Skip | Skip | Present |
| Regular write (own TS) | 0x00 | Include delta | Skip | Present |
| Empty string write | 0x08 \| 0x04 (0x0c) | Skip | Skip | Absent (no length VInt, no bytes) |
| Tombstone | 0x01 | Include delta | Include delta | None |

**Example**:
- Normal cell using row timestamp: `0x08`
- Empty string using row timestamp: `0x08 | 0x04 = 0x0c`
- Cell with own timestamp: `0x00` (no flags)
- Tombstone: `0x01` (no USE_ROW_TIMESTAMP)
- Expiring cell with row timestamp: `0x08 | 0x02 = 0x0a`

**Critical**: Tombstones MUST NOT use ROW_USE_ROW_TIMESTAMP (0x08). Tombstones always include their own timestamp delta.

**`IS_EXPIRING` (0x02) is strict and mutually exclusive with `IS_DELETED` (0x01).**
`IS_EXPIRING` means `ttl != NO_TTL` and nothing else: it is set only for a live cell
that actually carries a TTL, and it is **never** combined with `IS_DELETED`. A
tombstone (`IS_DELETED`) therefore never carries a wasted TTL byte, and an expiring
cell never sets the deletion bit. CQLite's emitter follows this: the TTL fields are
written only on the `Some(ttl)` path
(`data_writer.rs::write_complex_cell_header`, and the simple-cell expiring path),
and the deletion path writes `IS_DELETED` with no TTL. Authority:
`org.apache.cassandra.db.rows.Cell.Serializer` (`flags()`).

### NULL vs Empty Values

The format distinguishes between NULL and empty values:

**NULL Values**:
- NOT written as cells in the cell data section
- Represented by absence in the column bitmap (bit = 0)
- Presence of NULL prevents ROW_HAS_ALL_COLUMNS flag

**Empty Values** (e.g., empty string ''):
- Written as cells with CELL_HAS_EMPTY_VALUE flag (0x04)
- **No value length VInt and no value bytes** — the flag replaces them; it is *not* followed by a
  `0x00` length (`Cell.java:277-278`, `:303-304`; reader `:310`)
- Counted as "present" in column bitmap (bit = 1)

**Example Column Bitmap**:

For a table with columns [name, age, city]:
- Row with `name='Alice', age=NULL, city=''`:
  - Bitmap: `0b101` (name present, age absent, city present)
  - Cells: Two cells (name, city), city has HAS_EMPTY_VALUE flag

### Delta Encoding

All temporal metadata uses delta encoding against Statistics.db baseline values:

**Timestamp Delta** (unsigned VInt — `SerializationHeader.writeTimestamp` calls `writeUnsignedVInt`):
```
timestamp_delta = mutation_timestamp - min_timestamp
```

**TTL Delta** (unsigned VInt32 — `SerializationHeader.writeTTL` calls `writeUnsignedVInt32`):
```
ttl_delta = mutation_ttl - min_ttl
```

**Local Deletion Time Delta** (unsigned VInt32 — `SerializationHeader.writeLocalDeletionTime` calls `writeUnsignedVInt32`):
```
deletion_time_delta = local_deletion_time - min_local_deletion_time
```

**Constraints**:
- Timestamp deltas MUST be >= 0: `min_timestamp` is the minimum across all rows, so every delta is non-negative.
- TTL deltas MUST be >= 0: `min_ttl` is the minimum across all rows.
- Deletion time deltas MUST be >= 0 (error if local_deletion_time < min_local_deletion_time).
- All three fields use unsigned encoding because the baselines guarantee non-negative deltas.

### Delta Encoding Examples

**Example 1: Simple Row with Timestamp Delta**

Given Statistics.db values:
```
min_timestamp = 1000000 (microseconds)
min_ttl = 0
min_local_deletion_time = 0
```

Row with timestamp = 1005000:
```
[0x04]                    Row flags: HAS_TIMESTAMP
[VInt(5000)]              Timestamp delta: 1005000 - 1000000 = 5000
                          Unsigned VInt(5000) = 0x93 0x88 (2 bytes)
```

Byte-level encoding of unsigned VInt(5000):
- No ZigZag — timestamps use `writeUnsignedVInt`
- `5000 = 0x1388`; 2-byte encoding: `(0x13 | 0x80) = 0x93`, `0x88`

**Example 2: Row with TTL**

Row with timestamp = 1005000, ttl = 7200:
```
[0x0c]                    Row flags: HAS_TIMESTAMP | HAS_TTL (0x04 | 0x08)
[VInt(5000)]              Timestamp delta
[VInt(7200)]              TTL delta: 7200 - 0 = 7200
```

**Example 3: Cell Timestamp Delta**

Cell with own timestamp (not using row timestamp):
```
[0x00]                    Cell flags: no USE_ROW_TIMESTAMP
[VInt(2000)]              Timestamp delta from min_timestamp
[VInt(value_length)]      Value length
[value_bytes]             Value data
```

**Example 4: Tombstone Cell**

Tombstone with timestamp = 1003000, local_deletion_time = 1700000100:
```
[0x01]                    Cell flags: IS_DELETED (no USE_ROW_TIMESTAMP)
[VInt(3000)]              Timestamp delta: 1003000 - 1000000
[VUInt(1700000100)]       Deletion time delta: 1700000100 - 0 (unsigned)
```

**Example 5: No Negative Deltas**

Negative timestamp deltas cannot occur in a valid SSTable: `min_timestamp` is computed as
the minimum of all row timestamps, so every row's delta is >= 0. If you encounter a
value smaller than `min_timestamp`, the SSTable is malformed.

### Clustering Prefix Encoding

For tables with clustering keys, values are encoded immediately after row flags:

**Header VInt Construction**:
- 2 bits per clustering column, packed into a VInt
- Bits are packed starting from LSB (column 0 uses bits 0-1)

**State Values**:
- `00` (0): PRESENT - value bytes follow
- `01` (1): EMPTY - zero-length value (no bytes)
- `10` (2): NULL - no value (no bytes)
- `11` (3): Reserved

**Type-Specific Encoding**:

Fixed-width types (no length prefix):
- `int`: 4 bytes (BE)
- `bigint`: 8 bytes (BE)
- `timestamp`: 8 bytes (BE)
- `uuid`: 16 bytes (raw)

Variable-width types (VInt length + bytes):
- `text`/`varchar`: VInt(byte_length) + UTF-8 bytes
- `blob`: VInt(byte_length) + raw bytes

**Example**: Table with clustering keys (timestamp, text):

Row with clustering = (1234567890, "sensor1"):
```
[0x00]                              Header VInt: both PRESENT (0b0000)
[0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2]  timestamp (8 bytes)
[0x07]                              VInt length (7 bytes)
[0x73, 0x65, 0x6E, 0x73, 0x6F, 0x72, 0x31]  "sensor1" UTF-8
```

Row with clustering = (1234567890, NULL):
```
[0x02]                              Header VInt: timestamp PRESENT (00), text NULL (10)
[0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2]  timestamp (8 bytes)
                                    No text bytes (NULL)
```

### Column Bitmap Encoding

When `ROW_HAS_ALL_COLUMNS` (0x20) is NOT set, a columns-subset field follows — see the
authoritative **[Column Bitmap](#column-bitmap)** section above for the exact encoding. In
brief: it is **not** a `[column_count][bitmap_bytes]` present-bit format. For a superset of
`< 64` columns it is a single unsigned VInt whose **set bit = column MISSING** (the inverse of a
present-bitmap); for `≥ 64` columns it is the large-subset form (missing count + the smaller of
{present, missing} **absolute** column indices as unsigned VInts). CQLite's writer
(`data_writer.rs::write_column_subset`) follows this directly.

**Example**: 10-column table with columns 1 and 3 absent → single VInt with bits 1 and 3 set
(`0b00001010` = `0x0a`).

### Cell Data Format

**Regular Cell** (live value):
```
[flags: u8]                                        ← Cell flags
[timestamp_delta: VInt if NOT USE_ROW_TIMESTAMP]  ← Delta from min_timestamp
[value_length: VInt]                              ← Byte length; omitted if HAS_EMPTY_VALUE (0x04)
                                                     is set, or (simple cells) the type is fixed-width
[value_bytes]                                     ← Type-specific serialization; omitted if
                                                     HAS_EMPTY_VALUE is set
```

**Tombstone Cell** (deleted):
```
[flags: u8]                        ← CELL_IS_DELETED (0x01)
[timestamp_delta: VInt]            ← Delta from min_timestamp (required)
[deletion_time_delta: VUInt]       ← Delta from min_local_deletion_time
```

**Note**: Tombstones do NOT have value_length or value_bytes fields. The parser returns immediately after reading the deletion time delta.

### Cell Value Serialization

Type-specific serialization rules for cell values:

| Type | Format | Example |
|------|--------|---------|
| boolean | 1 byte | true = 0x01, false = 0x00 |
| tinyint | 1 byte (signed) | -5 = 0xFB |
| smallint | 2 bytes BE | 300 = 0x01 0x2C |
| int | 4 bytes BE | 42 = 0x00 0x00 0x00 0x2A |
| bigint | 8 bytes BE | 1000 = 0x00 0x00 0x00 0x00 0x00 0x00 0x03 0xE8 |
| float | 4 bytes BE (IEEE 754) | 3.14f = 0x40 0x48 0xF5 0xC3 |
| double | 8 bytes BE (IEEE 754) | 3.14 = 0x40 0x09 0x1E 0xB8 0x51 0xEB 0x85 0x1F |
| text/varchar | UTF-8 bytes (no prefix) | "test" = 0x74 0x65 0x73 0x74 |
| blob | Raw bytes (no prefix) | Binary data as-is |
| timestamp | 8 bytes BE (milliseconds) | Epoch milliseconds |
| date | 4 bytes BE (days + offset) | days - Integer.MIN_VALUE |
| time | 8 bytes BE (nanoseconds) | Nanoseconds since midnight |
| uuid/timeuuid | 16 bytes (raw) | UUID bytes |
| inet | 4 or 16 bytes | IPv4 (4) or IPv6 (16) |
| varint | Variable-length BE signed | Big integer, no length prefix |
| decimal | 4 bytes scale + varint | Scale (BE i32) + unscaled value |
| duration | 3x **signed (ZigZag) VInt** | months, days, nanos — NOT fixed-width i32. The same three signed VInts appear wherever a `duration` is nested (e.g. `frozen<list<duration>>`, a `duration` UDT field) |

**Special Cases**:
- **Empty string**: CELL_HAS_EMPTY_VALUE (`0x04`) set — **no length VInt and no value bytes**
  (`Cell.java:277-278`, `:303-304`)
- **NULL**: Not written as a cell (represented by bitmap absence)
- **Date encoding**: Add Integer.MIN_VALUE to days value for storage
- **Decimal**: Scale is 4-byte BE i32, followed by varint unscaled value
  (`DecimalSerializer.java:42-54`: `putInt(scale)` then `BigInteger.toByteArray()`)
- **Duration**: the one `Data.db` payload built from **signed** VInts —
  `DurationSerializer.serialize` calls `writeVInt` three times (`DurationSerializer.java:34,49-51`) —
  and it counts wherever that payload occurs, nested inside a collection/tuple/UDT included. Every
  *structural* field in `Data.db` uses unsigned VInt; see
  [VInt Sign Convention](#vint-sign-convention).

### Write Operation Flow

Complete write sequence for a partition:

1. **Compute Statistics**: Calculate min_timestamp, min_ttl, min_local_deletion_time from all mutations
2. **Initialize DataWriter**: Create with computed statistics for delta encoding
3. **Order Partitions**: Sort by Murmur3 token, then partition key bytes
4. **For Each Partition**:
   a. Write partition header
   b. Order rows by clustering key
   c. For each row:
      - Compute row flags
      - Write clustering prefix (if present)
      - Compute row_size (body bytes only)
      - Write row_size VInt
      - Write prev_size VInt (`previousUnfilteredSize`: header size for the first row,
        else the prior unfiltered's full byte length; `0` for a static row — see
        [previousUnfilteredSize](#previousunfilteredsize-the-prev_size-chain))
      - Write timestamp delta (if HAS_TIMESTAMP)
      - Write TTL delta (if HAS_TTL)
      - Write column bitmap (if NOT HAS_ALL_COLUMNS)
      - Write cells (skip NULLs)
   d. Write END_OF_PARTITION marker (0x01)
5. **Finish**: Return complete Data.db bytes

**Critical**: row_size is measured from AFTER the row_size VInt, not from where it starts (Issue #237).

### Validation

This write specification is validated through:
- **Implementation**: `cqlite-core/src/storage/sstable/writer/data_writer.rs`
- **Unit Tests**: 20+ tests covering all encoding paths
- **Round-trip Tests**: Written SSTables are readable by both CQLite and Cassandra's sstabledump
- **Cassandra Source**: Cross-referenced with `org.apache.cassandra.db.rows.UnfilteredSerializer.java`

### References

- **Implementation**: `cqlite-core/src/storage/sstable/writer/data_writer.rs`
- **Parser**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder.rs`
- **Cassandra Source**: `org.apache.cassandra.db.rows.UnfilteredSerializer` (lines 151-475)
- **Issue Tracking**: Issue #237 (row_size measurement), Issue #401 (tombstone encoding)


