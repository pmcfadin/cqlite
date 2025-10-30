# SerializationHeader Visual Binary Format Map

**Purpose:** Visual reference for understanding the SerializationHeader binary layout

---

## composite_key_table Example (Annotated Hex Dump)

```
╔════════════════════════════════════════════════════════════════════════════╗
║                    ENCODING STATS (Variable Length)                        ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Offset 0x1390                                                              ║
║ ff 7f fc f6 81 45 02 b7  8f fd 20 28 75 ed 4d 0d                           ║
║                                                   └──┬──┘                   ║
║                                                      │                      ║
║                                         Unknown VInt prefix                ║
║                                      (possibly tail of EncodingStats)      ║
╚════════════════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════════════════╗
║                    SERIALIZATION HEADER START                              ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Offset 0x139e                                                              ║
║ 00 00 28 6f 72 67 2e 61  70 61 63 68 65 2e 63 61                           ║
║ └──┬──┘└┬┘                                                                 ║
║    │    │                                                                  ║
║    │    └─ Partition Key Type String Length (0x28 = 40 bytes)             ║
║    │                                                                       ║
║    └────── MARKER (fixed 0x00 0x00 separator)                             ║
║                                                                            ║
║ Offset 0x13a2-0x13c6 (40 bytes)                                           ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 55 55 49 44 54 79 70 65                       ║
║ └──────────────────────────┬──────────────────────────┘                   ║
║                            │                                               ║
║                 "org.apache.cassandra.db.marshal.UUIDType"                ║
╚════════════════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════════════════╗
║                    CLUSTERING KEYS SECTION                                 ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Offset 0x13c7                                                              ║
║ 02 5b 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72           ║
║ └┬┘└┬┘                                                                     ║
║  │  │                                                                      ║
║  │  └─ Clustering Type 1 Length (0x5b = 91 bytes)                         ║
║  │                                                                         ║
║  └──── Clustering Count (0x02 = 2 clustering keys)                        ║
║                                                                            ║
║ Offset 0x13c9-0x1422 (91 bytes)                                           ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 52 65 76 65 72 73 65 64 54 79 70 65 28       ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 54 69 6d 65 73 74 61 6d 70 54 79 70 65 29     ║
║ └──────────────────────────┬──────────────────────────┘                   ║
║         "org.apache.cassandra.db.marshal.ReversedType(                    ║
║          org.apache.cassandra.db.marshal.TimestampType)"                  ║
║                                                                            ║
║ Offset 0x1423                                                              ║
║ 28 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72             ║
║ └┬┘                                                                        ║
║  │                                                                         ║
║  └─ Clustering Type 2 Length (0x28 = 40 bytes)                            ║
║                                                                            ║
║ Offset 0x1424-0x144f (40 bytes)                                           ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 55 54 46 38 54 79 70 65                       ║
║ └──────────────────────────┬──────────────────────────┘                   ║
║                 "org.apache.cassandra.db.marshal.UTF8Type"                ║
╚════════════════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════════════════╗
║                    SEPARATOR & REGULAR COLUMNS                             ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Offset 0x1450                                                              ║
║ 65 00 02 04 64 61 74 61  28 6f 72 67 2e 61 70 61                           ║
║    └┬┘└┬┘└┬┘└───┬───┘  └┬┘                                                ║
║     │  │  │     │       │                                                  ║
║     │  │  │     │       └─ Column 1 Type Length (0x28 = 40 bytes)         ║
║     │  │  │     │                                                          ║
║     │  │  │     └───────── Column 1 Name ("data", 4 bytes)                ║
║     │  │  │                                                                ║
║     │  │  └─────────────── Column 1 Name Length (0x04 = 4 bytes)          ║
║     │  │                                                                   ║
║     │  └────────────────── Regular Column Count (0x02 = 2 columns)        ║
║     │                                                                      ║
║     └───────────────────── SEPARATOR (0x00 after clustering)              ║
║                                                                            ║
║ Offset 0x1457-0x147e (40 bytes)                                           ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 55 54 46 38 54 79 70 65                       ║
║ └──────────────────────────┬──────────────────────────┘                   ║
║                 "org.apache.cassandra.db.marshal.UTF8Type"                ║
║                                                                            ║
║ Offset 0x147f                                                              ║
║ 05 76 61 6c 75 65 29 6f  72 67 2e 61 70 61 63 68                           ║
║ └┬┘└────┬────┘└┬┘                                                          ║
║  │      │      │                                                           ║
║  │      │      └─ Column 2 Type Length (0x29 = 41 bytes)                  ║
║  │      │                                                                  ║
║  │      └──────── Column 2 Name ("value", 5 bytes)                        ║
║  │                                                                         ║
║  └─────────────── Column 2 Name Length (0x05 = 5 bytes)                   ║
║                                                                            ║
║ Offset 0x1486-0x14af (41 bytes)                                           ║
║ 6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 2e 64 62     ║
║ 2e 6d 61 72 73 68 61 6c 2e 49 6e 74 33 32 54 79 70 65                    ║
║ └──────────────────────────┬──────────────────────────┘                   ║
║                 "org.apache.cassandra.db.marshal.Int32Type"               ║
╚════════════════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════════════════╗
║                    END OF SERIALIZATION HEADER                             ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Offset 0x14b0                                                              ║
║ 65 21 25 3a 57 ...                                                         ║
║ └┬┘                                                                         ║
║  │                                                                          ║
║  └─ Last byte of "Int32Type" ('e')                                         ║
║                                                                            ║
║ Next metadata section follows immediately                                 ║
╚════════════════════════════════════════════════════════════════════════════╝
```

---

## ttl_test_table Example (No Clustering Keys)

```
╔════════════════════════════════════════════════════════════════════════════╗
║                    SERIALIZATION HEADER (Simplified)                       ║
╠════════════════════════════════════════════════════════════════════════════╣
║ [VInt prefix] [0x00 0x00]                                                  ║
║                                                                            ║
║ 28 "org.apache.cassandra.db.marshal.UUIDType"  ← Partition Key            ║
║                                                                            ║
║ 00  ← Clustering Count (ZERO - no clustering)                             ║
║                                                                            ║
║ 00  ← Separator (even with zero clustering keys)                          ║
║                                                                            ║
║ 03  ← Regular Column Count (3 columns)                                    ║
║                                                                            ║
║ ┌─ Column 1                                                                ║
║ │  0e "expiring_value"                                                     ║
║ │  29 "org.apache.cassandra.db.marshal.Int32Type"                         ║
║ │                                                                          ║
║ ├─ Column 2                                                                ║
║ │  0c "session_info"                                                       ║
║ │  28 "org.apache.cassandra.db.marshal.UTF8Type"                          ║
║ │                                                                          ║
║ └─ Column 3                                                                ║
║    0e "temporary_data"                                                     ║
║    28 "org.apache.cassandra.db.marshal.UTF8Type"                          ║
╚════════════════════════════════════════════════════════════════════════════╝
```

---

## State Machine Diagram

```
                          START
                            ↓
                    ┌───────────────┐
                    │  Find Marker  │
                    │   0x00 0x00   │
                    └───────┬───────┘
                            ↓
                  ┌─────────────────────┐
                  │  Read Partition     │
                  │  Key Type String    │
                  │  [len:u8][string]   │
                  └─────────┬───────────┘
                            ↓
                  ┌─────────────────────┐
                  │  Read Clustering    │
                  │  Count (u8)         │
                  └─────────┬───────────┘
                            ↓
                ┌───────────────────────┐
                │  count == 0?          │
                └────┬──────────────┬───┘
                     NO             YES
                     ↓               ↓
         ┌───────────────────┐   (skip)
         │  FOR each count   │      ↓
         │  Read Type String │      ↓
         │  [len:u8][string] │      ↓
         └───────┬───────────┘      ↓
                 ↓                   ↓
         ┌───────────────────────────┘
         │
         ↓
    ┌─────────────────────┐
    │  Read Separator     │
    │  (must be 0x00)     │
    └─────────┬───────────┘
              ↓
    ┌─────────────────────┐
    │  Read Column Count  │
    │  (u8)               │
    └─────────┬───────────┘
              ↓
    ┌─────────────────────────────┐
    │  FOR each count             │
    │  Read Column Definition:    │
    │  [name_len:u8][name]        │
    │  [type_len:u8][type_string] │
    └─────────┬───────────────────┘
              ↓
          ┌───────┐
          │  END  │
          └───────┘
```

---

## Byte-Level Encoding Reference

### Length-Prefixed String

```
┌────┬────┬────┬────┬────┬────┬─────┐
│ 04 │ 'd'│ 'a'│ 't'│ 'a'│ ?? │ ... │
└─┬──┴────┴────┴────┴────┴────┴─────┘
  │        └────┬────┘
  │             │
  │             └─ String data (4 bytes)
  │
  └─ Length (1 byte, value = 4)
```

### Column Definition Encoding

```
┌────┬─────────┬────┬────────────────┐
│ NL │  NAME   │ TL │  TYPE_STRING   │
└─┬──┴────┬────┴─┬──┴────────┬───────┘
  │       │      │           │
  │       │      │           └─ Type string (TL bytes)
  │       │      │
  │       │      └─ Type string length (1 byte)
  │       │
  │       └─ Column name (NL bytes)
  │
  └─ Name length (1 byte)

Example:
┌────┬────┬────┬────┬────┬────┬─────────────────────────────┐
│ 04 │ 'd'│ 'a'│ 't'│ 'a'│ 28 │ "...UTF8Type" (40 bytes)   │
└────┴────┴────┴────┴────┴────┴─────────────────────────────┘
  └──────────────┬──────────────┘
                 │
         Column "data":UTF8Type
```

### Clustering Type Encoding (with ReversedType)

```
┌────┬──────────────────────────────────────────────────┐
│ 5B │  "...ReversedType(...TimestampType)" (91 bytes) │
└─┬──┴──────────────────────────┬───────────────────────┘
  │                             │
  │                             └─ Nested type as single string
  │
  └─ Length (0x5b = 91 decimal)

Structure of nested type string:
"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
 └──────────────┬──────────────┘                        └──────────────┬──────────────┘
         Outer type wrapper                                    Inner type parameter
```

---

## Memory Layout Visualization

### SerializationHeader Struct

```rust
pub struct SerializationHeader {
    // ┌─────────────────────────────────────────────┐
    // │ partition_key_type: String                  │  ← "org.apache...UUIDType"
    // │ (heap-allocated)                            │     (40 bytes)
    // └─────────────────────────────────────────────┘
    //
    // ┌─────────────────────────────────────────────┐
    // │ clustering_types: Vec<String>               │
    // │ ┌─────────────────────────────────────────┐ │
    // │ │ [0]: "org.apache...ReversedType(...)"   │ │  (91 bytes)
    // │ │ [1]: "org.apache...UTF8Type"            │ │  (40 bytes)
    // │ └─────────────────────────────────────────┘ │
    // └─────────────────────────────────────────────┘
    //
    // ┌─────────────────────────────────────────────┐
    // │ regular_columns: Vec<ColumnDefinition>      │
    // │ ┌─────────────────────────────────────────┐ │
    // │ │ [0]: { name: "data",                    │ │
    // │ │        type: "org.apache...UTF8Type" }  │ │
    // │ │ [1]: { name: "value",                   │ │
    // │ │        type: "org.apache...Int32Type" } │ │
    // │ └─────────────────────────────────────────┘ │
    // └─────────────────────────────────────────────┘
}
```

---

## Parsing Algorithm Flow

```
INPUT: &[u8] (Statistics.db data)
       usize (cursor position after EncodingStats)

STEP 1: Find marker
    ┌─────────────────────────────┐
    │ Read 2 bytes                │
    │ Verify == 0x00 0x00         │
    └──────────┬──────────────────┘
               │ OK
               ↓

STEP 2: Read partition key type
    ┌─────────────────────────────┐
    │ len ← read_u8()             │
    │ type ← read_string(len)     │
    └──────────┬──────────────────┘
               ↓

STEP 3: Read clustering keys
    ┌─────────────────────────────┐
    │ count ← read_u8()           │
    │ types ← Vec::new()          │
    │ FOR i in 0..count:          │
    │   len ← read_u8()           │
    │   type ← read_string(len)   │
    │   types.push(type)          │
    └──────────┬──────────────────┘
               ↓

STEP 4: Verify separator
    ┌─────────────────────────────┐
    │ sep ← read_u8()             │
    │ Verify sep == 0x00          │
    └──────────┬──────────────────┘
               │ OK
               ↓

STEP 5: Read regular columns
    ┌─────────────────────────────┐
    │ count ← read_u8()           │
    │ columns ← Vec::new()        │
    │ FOR i in 0..count:          │
    │   name_len ← read_u8()      │
    │   name ← read_string(...)   │
    │   type_len ← read_u8()      │
    │   type ← read_string(...)   │
    │   columns.push(...)         │
    └──────────┬──────────────────┘
               ↓

OUTPUT: SerializationHeader {
           partition_key_type,
           clustering_types,
           regular_columns
        }
```

---

## Comparison Table: Field Sizes

| Field | Example Value | Hex | Decimal | Notes |
|-------|---------------|-----|---------|-------|
| Partition key type len | UUIDType | `28` | 40 | With package prefix |
| Clustering count | 2 keys | `02` | 2 | Simple table |
| Clustering count | No clustering | `00` | 0 | Simple partition key |
| Clustering type len | ReversedType(...) | `5b` | 91 | Nested type |
| Clustering type len | UTF8Type | `28` | 40 | Simple type |
| Separator | After clustering | `00` | 0 | Fixed marker |
| Column count | 2 columns | `02` | 2 | Simple table |
| Column count | 3 columns | `03` | 3 | TTL table |
| Column count | 18 columns | `12` | 18 | Complex table |
| Column name len | "data" | `04` | 4 | Short name |
| Column name len | "expiring_value" | `0e` | 14 | Longer name |
| Column type len | UTF8Type | `28` | 40 | Common type |
| Column type len | Int32Type | `29` | 41 | Common type |

---

## Error Detection Points

```
┌─────────────────────────────────────────┐
│ Marker Check                            │
│ ─────────────                           │
│ IF bytes != [0x00, 0x00]                │
│    → ParseError::MissingMarker          │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Separator Check                         │
│ ────────────────                        │
│ IF byte != 0x00                         │
│    → ParseError::InvalidSeparator       │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ String UTF-8 Validation                 │
│ ─────────────────────────               │
│ IF !is_valid_utf8(bytes)                │
│    → ParseError::InvalidUtf8            │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ EOF Detection                           │
│ ──────────────                          │
│ IF cursor.remaining() < expected        │
│    → ParseError::UnexpectedEof          │
└─────────────────────────────────────────┘
```

---

## Type String Examples (Sorted by Length)

| Length | Type String | Usage |
|--------|-------------|-------|
| 40 | `org.apache.cassandra.db.marshal.UUIDType` | Partition keys, UUIDs |
| 40 | `org.apache.cassandra.db.marshal.UTF8Type` | Text columns |
| 41 | `org.apache.cassandra.db.marshal.Int32Type` | Integer columns |
| 41 | `org.apache.cassandra.db.marshal.LongType` | BIGINT columns |
| 42 | `org.apache.cassandra.db.marshal.FloatType` | Float columns |
| 43 | `org.apache.cassandra.db.marshal.DoubleType` | Double columns |
| 91 | `org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)` | DESC clustering |

---

**This visual map provides a comprehensive reference for understanding the SerializationHeader binary format at a glance.**
