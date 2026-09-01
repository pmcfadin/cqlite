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

## What this means for CQLite — TWO SYMPTOMS, ONE ENFORCEMENT POINT

An earlier draft of this file called these two independent defects needing two
fixes. That is WRONG, and the correction is the most useful thing here, so it is
recorded rather than quietly overwritten.

- **Symptom A — partial prefix accepted.** CQLite's frozen-UDT loop guards with
  `if current_offset + 4 > udt_data.len() { /* trailing fields omitted (implicit
  null) */ break; }`, which collapses rows 1 and 2 of the table above onto the
  legal answer. Cassandra distinguishes them: only `== length` is an omission;
  1-3 leftover bytes are `MarshalException`.
- **Symptom B — trailing bytes accepted.** Row 4 requires full consumption.
  `parse_raw_type_value` returns a possibly-SHORT offset and the bounded caller
  `parse_value_from_raw_bytes` DISCARDS it (`let (val, _offset) = ...`) on BOTH
  the marshal-form and the registry-resolved UDT arm.

**They share one enforcement point.** On the partial-prefix path the loop
`break`s WITHOUT advancing `current_offset` past the 1-3 stray bytes, so the
returned offset is short by exactly those bytes — the same observable as trailing
garbage. So a single `consumed == slice.len()` test at the bounded caller refuses
BOTH, and no change to the callee's loop guard is required. This is not inferred
here; #3612 already established it one module over and says so in
`complex_column/cell_path_key.rs:414-425`:

> "This one comparison subsumes three separate behaviours ... trailing bytes
> after the components (`pos < len`) are REFUSED; a partial 1-3 byte
> component-length header (also `pos < len`, because the decoders treat it as
> 'trailing fields omitted' and do NOT advance past it) is REFUSED; and a
> genuinely SHORT encoding, whose omitted components leave `pos == len`, is
> ACCEPTED"

That is the whole design of the fix, and it is why the issue says the property
must be established **structurally** rather than site by site: the check is one
comparison, and the entire difficulty is making every bounded caller inherit it.

### The reference implementation, and what is wrong with copying it

`cell_path_key.rs` enforces exactly this — but only for cell-path keys, and via
a local `decode_reporting_consumption` whose `Ok((value, None))` arm means "this
arm consumes the whole slice by construction, nothing to compare". **That `None`
is the opt-out a new call site inherits by accident.** Hoisting the rule must not
hoist a silent `None`: a new arm that forgets to report must fail closed, not
land in the branch that skips the check.

## Consequence for AC4 (two values must not collapse)

Under Defect B, `<valid-udt-bytes>` and `<valid-udt-bytes> || <any trailing
garbage>` decode to the SAME `Value`. Under Defect A, `<short-udt>` and
`<short-udt> || <1..3 stray bytes>` likewise collapse. Both are the
distinct-inputs-one-Value violation the issue names, and each needs its own
case — a test for one does not exercise the other.
