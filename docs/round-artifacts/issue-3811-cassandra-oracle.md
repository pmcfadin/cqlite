# Issue #3811 — the Cassandra oracle for the consumption/bounds contract

**Authority:** pinned `cassandra-5.0.8`,
`src/java/org/apache/cassandra/db/marshal/TupleType.java`, static `split(...)`.
`UserType extends TupleType`, so a UDT value is split by this exact method.

Read at the pin (no local clone on this box; browse route per CLAUDE.md):
`https://raw.githubusercontent.com/apache/cassandra/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TupleType.java`

```java
public static <V> V[] split(ValueAccessor<V> accessor, V value, int numberOfElements, TupleType type)
{
    V[] components = accessor.createArray(numberOfElements);
    int length = accessor.size(value);
    int position = 0;
    for (int i = 0; i < numberOfElements; i++)
    {
        if (position == length)
            return Arrays.copyOfRange(components, 0, i);

        if (position + 4 > length)
            throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

        int size = accessor.getInt(value, position);
        position += 4;

        // size < 0 means null value
        if (size >= 0)
        {
            if (position + size > length)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            components[i] = accessor.slice(value, position, size);
            position += size;
        }
        else
            components[i] = null;
    }

    // error out if we got more values in the tuple/UDT than we expected
    if (position < length)
    {
        throw new MarshalException(String.format("Expected %s %s for %s column, but got more",
                                                 numberOfElements, numberOfElements == 1 ? "value" : "values",
                                                 type.asCQL3Type()));
    }

    return components;
}
```

## The four behaviours, in the order Cassandra tests them

The ORDER is the whole content of this oracle — it is what separates a legal
omission from a corruption, and the two are one byte apart.

| # | condition | Cassandra | note |
|---|---|---|---|
| 1 | `position == length` exactly, before reading field `i` | **LEGAL** — returns a SHORT array; fields `i..n` are absent (implicit null) | this is how a UDT that gained fields after the row was written still reads |
| 2 | `0 < length - position < 4` (a partial int32 field-length prefix) | **`MarshalException`** — `"Not enough bytes to read %dth component"` | NOT an omitted field. Checked only AFTER 1 fails, so it is unreachable when the buffer ends cleanly |
| 3 | `position + size > length` (declared field length overruns) | **`MarshalException`** — same message | |
| 4 | all `numberOfElements` read and `position < length` (trailing bytes) | **`MarshalException`** — `"Expected N value(s) for <type> column, but got more"` | exhaustion is REQUIRED, not optional |

## What this means for CQLite, stated as two distinct defects

They are distinct because they live in different places and neither fix
implies the other.

- **Defect A — partial prefix accepted (in the CALLEE).** CQLite's frozen-UDT
  loop guards with `if current_offset + 4 > udt_data.len() { /* trailing fields
  omitted (implicit null) */ }`, which collapses rows 1 and 2 of the table onto
  the legal answer. Cassandra distinguishes them: only `== length` is an
  omission; 1–3 leftover bytes are `MarshalException`. So CQLite accepts a
  truncated field header as a well-formed short UDT.
- **Defect B — trailing bytes accepted (at the BOUNDED CALLER).** Row 4 requires
  full consumption. `parse_raw_type_value` returns a possibly-SHORT offset and
  the bounded caller `parse_value_from_raw_bytes` discards it (`let (val,
  _offset) = ...`, both the marshal-form and registry-resolved UDT arms), so
  trailing bytes are silently dropped.

`parse_value_from_raw_bytes`'s own doc comment already STATES the contract it
does not enforce — *"Parse a value from a complete, bounded byte slice … The
entire `data` slice IS the value."* The property is not new; the enforcement is.

## Consequence for AC4 (two values must not collapse)

Under Defect B, `<valid-udt-bytes>` and `<valid-udt-bytes> || <any trailing
garbage>` decode to the SAME `Value`. Under Defect A, `<short-udt>` and
`<short-udt> || <1..3 stray bytes>` likewise collapse. Both are the
distinct-inputs-one-Value violation the issue names, and each needs its own
case — a test for one does not exercise the other.
