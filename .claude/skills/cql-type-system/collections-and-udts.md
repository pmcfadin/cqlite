# Collections and User-Defined Types

## Collection Types Overview

Cassandra supports three collection types:
- **List** - Ordered, allows duplicates
- **Set** - Unordered, no duplicates (stored sorted)
- **Map** - Key-value pairs (stored sorted by key)

## Collection Wire Format — TWO distinct encodings

> ### ⚠️ Frozen and non-frozen collections do NOT share a wire format
> Choose the encoding from the **schema's frozen-ness of the column**, never from the byte
> pattern (no-heuristics mandate, #28). Assuming the frozen layout for a non-frozen column
> (or vice versa) mis-frames every element.

| | **FROZEN** | **NON-FROZEN** (the default) |
|---|---|---|
| Declared as | `frozen<list<int>>`; also any collection nested inside another collection/tuple/UDT | `list<int>`, `set<T>`, `map<K,V>` |
| On disk | ONE opaque cell value | a SET of cells (one per element) |
| Count | fixed **4-byte BE `i32`** | **unsigned VInt** |
| Element/value length | fixed **4-byte BE `i32`** (`-1` = null) | **unsigned VInt** (no `-1`; absent = `HAS_EMPTY_VALUE` flag) |
| Per-element metadata | none | cell flags + optional timestamp/TTL/LDT deltas + a cell **path** |
| Element deletion | not representable | per-cell `IS_DELETED`; whole-collection via `ROW_HAS_COMPLEX_DELETION` (`0x40`) |

### FROZEN collection structure (fixed 4-byte BE `i32` framing)

```
[i32 BE: element_count]
[for each element:]
    [i32 BE: element_size]   // -1 = null
    [bytes: element_data]
```

> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/frozen.rs:21`
> (`i32::from_be_bytes` count), `:98`, `:279`, `:370` (element/key sizes). Guide:
> `docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md:537-539`
> ("tuple/UDT field lengths and frozen-collection counts/element lengths are fixed 4-byte
> BE `i32`"). Cassandra: `CollectionSerializer.java:67-92`.

### NON-FROZEN collection structure (unsigned-VInt framing)

```
[if the row sets ROW_HAS_COMPLEX_DELETION (0x40):]
    [unsigned VInt: markedForDeleteAt delta]
    [unsigned VInt: local_deletion_time delta]
[unsigned VInt: cell_count]
[for each cell:]
    [1 byte: cell flags]                       // IS_DELETED 0x01, IS_EXPIRING 0x02,
                                               // HAS_EMPTY_VALUE 0x04, USE_ROW_TIMESTAMP 0x08,
                                               // USE_ROW_TTL 0x10
    [conditional unsigned-VInt timestamp / local_deletion_time / ttl deltas]
    [unsigned VInt: path_len][path bytes]      // element key / path (the list UUID, set
                                               // element, or map key)
    [unsigned VInt: value_len][value bytes]    // omitted when IS_DELETED or HAS_EMPTY_VALUE
```

> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs:279-300`
> (complex deletion deltas), `:332` (`cell_count` via `parse_vuint`), `:1089` (path length),
> `:1136` (value length). Guide: `appendix-b-encodings-cheat-sheet.md:530-536` — a
> non-frozen collection cell length-prefixes BOTH path and value with an unsigned VInt,
> **fixed-width element types included**; the `valueLengthIfFixed()` raw-bytes shortcut is a
> **simple-cell** rule only (`AbstractType.java:538-543`), and a non-frozen `set<T>`'s
> missing value is the `HAS_EMPTY_VALUE_MASK` flag, not a width.

### Examples below are FROZEN

Every hex example in this section (`List`, `Set`, `Map`, `Nested`) shows the **frozen**
layout — the fixed 4-byte BE `i32` framing above. For a non-frozen column, replace each
4-byte count/length with an unsigned VInt and add the per-cell flags/path shown in the
non-frozen structure.

### List Example (FROZEN)
```
CQL: list<int>
Value: [10, 20, 30]

Wire format:
[0x00, 0x00, 0x00, 0x03]  // count = 3
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x0A]  // value = 10
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x14]  // value = 20
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x1E]  // value = 30
```

### Set Example (FROZEN)
```
CQL: set<text>
Value: {'apple', 'banana', 'cherry'}

Wire format (stored sorted):
[0x00, 0x00, 0x00, 0x03]  // count = 3
[0x00, 0x00, 0x00, 0x05]  // size = 5
[0x61, 0x70, 0x70, 0x6C, 0x65]  // "apple"
[0x00, 0x00, 0x00, 0x06]  // size = 6
[0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61]  // "banana"
[0x00, 0x00, 0x00, 0x06]  // size = 6
[0x63, 0x68, 0x65, 0x72, 0x72, 0x79]  // "cherry"
```

### Map Example (FROZEN)
```
CQL: map<text, int>
Value: {'a': 1, 'b': 2}

Wire format (stored sorted by key):
[0x00, 0x00, 0x00, 0x02]  // count = 2
[0x00, 0x00, 0x00, 0x01]  // key size = 1
[0x61]                    // key = "a"
[0x00, 0x00, 0x00, 0x04]  // value size = 4
[0x00, 0x00, 0x00, 0x01]  // value = 1
[0x00, 0x00, 0x00, 0x01]  // key size = 1
[0x62]                    // key = "b"
[0x00, 0x00, 0x00, 0x04]  // value size = 4
[0x00, 0x00, 0x00, 0x02]  // value = 2
```

## Nested Collections

Nested collections MUST be frozen:

```cql
-- Valid
CREATE TABLE t (
    id int PRIMARY KEY,
    data list<frozen<list<int>>>  -- Frozen nested list
);

-- Invalid
CREATE TABLE t (
    id int PRIMARY KEY,
    data list<list<int>>  -- Error: nested collections must be frozen
);
```

### Nested Collection Wire Format
```
CQL: list<frozen<list<int>>>
Value: [[1, 2], [3, 4, 5]]

Wire format:
[0x00, 0x00, 0x00, 0x02]  // outer count = 2

// First nested list [1, 2]
[0x00, 0x00, 0x00, 0x18]  // size of entire frozen list
[0x00, 0x00, 0x00, 0x02]  // inner count = 2
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x01]  // value = 1
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x02]  // value = 2

// Second nested list [3, 4, 5]
[0x00, 0x00, 0x00, 0x24]  // size of entire frozen list
[0x00, 0x00, 0x00, 0x03]  // inner count = 3
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x03]  // value = 3
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x04]  // value = 4
[0x00, 0x00, 0x00, 0x04]  // size = 4
[0x00, 0x00, 0x00, 0x05]  // value = 5
```

## Deserialization Code

### List
```rust
fn deserialize_list(
    data: &[u8],
    element_type: &CqlType,
) -> Result<Vec<CqlValue>> {
    if data.len() < 4 {
        return Err(Error::NotEnoughBytes);
    }
    
    let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut offset = 4;
    let mut elements = Vec::with_capacity(count);
    
    for _ in 0..count {
        let (element_data, remaining) = read_length_prefixed(&data[offset..])?;
        let element = deserialize_value(&element_data, element_type)?;
        elements.push(element);
        offset = data.len() - remaining.len();
    }
    
    Ok(elements)
}
```

### Set
```rust
fn deserialize_set(
    data: &[u8],
    element_type: &CqlType,
) -> Result<BTreeSet<CqlValue>> {
    // Same format as list, but return BTreeSet
    let elements = deserialize_list(data, element_type)?;
    Ok(elements.into_iter().collect())
}
```

### Map
```rust
fn deserialize_map(
    data: &[u8],
    key_type: &CqlType,
    value_type: &CqlType,
) -> Result<BTreeMap<CqlValue, CqlValue>> {
    if data.len() < 4 {
        return Err(Error::NotEnoughBytes);
    }
    
    let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut offset = 4;
    let mut map = BTreeMap::new();
    
    for _ in 0..count {
        let (key_data, remaining) = read_length_prefixed(&data[offset..])?;
        let key = deserialize_value(&key_data, key_type)?;
        offset = data.len() - remaining.len();
        
        let (value_data, remaining) = read_length_prefixed(&data[offset..])?;
        let value = deserialize_value(&value_data, value_type)?;
        offset = data.len() - remaining.len();
        
        map.insert(key, value);
    }
    
    Ok(map)
}
```

## User-Defined Types (UDTs)

### UDT Schema Definition
```cql
CREATE TYPE address (
    street text,
    city text,
    zip int
);

CREATE TABLE users (
    id int PRIMARY KEY,
    home address
);
```

### UDT Wire Format
```
[for each field in schema order:]
    [4 bytes: field_size (i32)]
    [if size >= 0:]
        [bytes: field_data]
```

**Field size semantics:**
- `-1` (0xFFFFFFFF): Field is null
- `0`: Field is empty (zero-length)
- `>0`: Field has N bytes of data

### UDT Example
```
CQL: address
Value: {street: '123 Main St', city: 'NYC', zip: 10001}

Wire format:
[0x00, 0x00, 0x00, 0x0B]  // street size = 11
[0x31, 0x32, 0x33, 0x20, 0x4D, 0x61, 0x69, 0x6E, 0x20, 0x53, 0x74]  // "123 Main St"
[0x00, 0x00, 0x00, 0x03]  // city size = 3
[0x4E, 0x59, 0x43]        // "NYC"
[0x00, 0x00, 0x00, 0x04]  // zip size = 4
[0x00, 0x00, 0x27, 0x11]  // 10001
```

### UDT with Null Field
```
Value: {street: '123 Main St', city: null, zip: 10001}

Wire format:
[0x00, 0x00, 0x00, 0x0B]  // street size = 11
[0x31, 0x32, 0x33, 0x20, 0x4D, 0x61, 0x69, 0x6E, 0x20, 0x53, 0x74]
[0xFF, 0xFF, 0xFF, 0xFF]  // city is null (-1)
[0x00, 0x00, 0x00, 0x04]  // zip size = 4
[0x00, 0x00, 0x27, 0x11]
```

### UDT Deserialization
```rust
struct UdtDef {
    name: String,
    fields: Vec<(String, CqlType)>,  // Field name + type
}

fn deserialize_udt(
    data: &[u8],
    udt_def: &UdtDef,
) -> Result<BTreeMap<String, Option<CqlValue>>> {
    let mut offset = 0;
    let mut fields = BTreeMap::new();
    
    for (field_name, field_type) in &udt_def.fields {
        if offset + 4 > data.len() {
            return Err(Error::NotEnoughBytes);
        }
        
        let size = i32::from_be_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3]
        ]);
        offset += 4;
        
        let value = if size < 0 {
            None  // Null field
        } else if size == 0 {
            Some(CqlValue::Empty)  // Empty field
        } else {
            let size = size as usize;
            if offset + size > data.len() {
                return Err(Error::NotEnoughBytes);
            }
            let field_data = &data[offset..offset + size];
            Some(deserialize_value(field_data, field_type)?)
        };
        
        offset += size.max(0) as usize;
        fields.insert(field_name.clone(), value);
    }
    
    Ok(fields)
}
```

## Frozen Semantics

### Frozen Collections
```cql
-- Frozen collection: cannot update individual elements
CREATE TABLE t (
    id int PRIMARY KEY,
    data frozen<list<int>>
);

-- Can only replace entire list
UPDATE t SET data = [1, 2, 3] WHERE id = 1;
-- Cannot: UPDATE t SET data[0] = 99 WHERE id = 1;
```

**Wire format:** Same as non-frozen, but:
- Serialized as single blob in parent structure
- No tombstones for individual elements
- Entire collection replaced on update

### Frozen UDTs
```cql
CREATE TABLE t (
    id int PRIMARY KEY,
    addr frozen<address>
);

-- Must update entire UDT
UPDATE t SET addr = {street: '...', city: '...', zip: ...} WHERE id = 1;
```

**Why frozen is required in some cases:**
- Primary key components must be frozen
- Nested collections must be frozen
- Allows value comparison for equality

## Empty Collections

```
Empty list: [0x00, 0x00, 0x00, 0x00]  // count = 0
Empty set:  [0x00, 0x00, 0x00, 0x00]  // count = 0
Empty map:  [0x00, 0x00, 0x00, 0x00]  // count = 0
```

## Null vs Empty

- **Null collection:** Field not present, or size = -1 in length-prefixed context
- **Empty collection:** Field present with count = 0

## Performance Considerations

### Zero-Copy for Collections
```rust
use bytes::Bytes;

// Don't copy entire collection if just reading one element
fn get_list_element(data: Bytes, index: usize, element_type: &CqlType) -> Result<CqlValue> {
    let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    
    if index >= count {
        return Err(Error::IndexOutOfBounds);
    }
    
    let mut offset = 4;
    for i in 0..=index {
        let size = i32::from_be_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3]
        ]) as usize;
        offset += 4;
        
        if i == index {
            let element_data = data.slice(offset..offset + size);
            return deserialize_value(&element_data, element_type);
        }
        
        offset += size;
    }
    
    unreachable!()
}
```

## Testing

Generate test data with collections:
```cql
-- test-data/schemas/collections.cql
CREATE TABLE collection_table (
    id int PRIMARY KEY,
    tags set<text>,
    scores list<int>,
    metadata map<text, text>,
    nested list<frozen<list<int>>>
);
```

Validate parsing:
```bash
./scripts/generate.sh
sstabledump test-data/datasets/sstables/test_collections/collection_table/*.db
```

## Reference

See `cqlite-core/src/types/collections.rs` for implementation.

