# Issue #3805 — Format oracle: what a ZERO-LENGTH cell path means for a fixed-width map key

**Authority.** Every claim below is cited to Apache Cassandra at the pinned tag **`cassandra-5.0.8`**
(`git show cassandra-5.0.8:<path>`, or
`https://github.com/apache/cassandra/blob/cassandra-5.0.8/<path>`). Paths are given relative to
`src/java/` unless noted. No CQLite source is cited as authority anywhere in this document
(CLAUDE.md, *"Format authority — a CQLite `file:line` is NEVER format authority"*). The sources read
for this audit are saved untracked under `.oracle-3805/cassandra/`.

**A second authority is also used, and is labelled wherever it appears.** §4b and §4c rest on
**authority #2 — `sstabledump` output** from a real Cassandra 5.0.2 (raw evidence:
`.oracle-3805/empirical/`). Every empirical result CONFIRMS the source reading; where a claim is
measured rather than derived, it says so. §4c records two corrections to committed claims, each
flagged empirically and then verified against the pinned source.

---

## The question

A non-frozen `map<K,V>` is **multicell**: each entry is its own `Cell`, and the entry's KEY travels in
that cell's **CellPath**, framed on disk as `[VInt length][bare serialized key]`. A **zero-length**
cell path therefore means the key's serialized form is the **empty buffer**.

For the fixed-width key families whose `validate()` is spelled `size != N && !isEmpty` — so that an
**empty buffer is LEGAL** — namely `int`, `float`, `bigint`, `counter`, `double`, `timestamp`,
`uuid`, `timeuuid`, `boolean` (plus `inet`, which returns early on empty):

**What does that empty buffer MEAN as a map key, and what should a reader surface for it?**

---

## 1. `deserialize()` with an empty buffer — what does each serializer return?

**Answer: `null`, in every case. Verified independently, 10 of 10 families named in the question.**
(The sibling lane's `isEmpty ? null : …` finding is CONFIRMED. Its "12 of 12" count is over a
different, larger population — the value read path — and is not re-derived here; what is verified
here is the 10 families this question names. The `counter` and `timeuuid` rows required indirection:
neither has a file of its own name.)

| CQL type | Serializer | `deserialize(EMPTY)` | Citation |
|---|---|---|---|
| `int` | `Int32Serializer` | `null` | `org/apache/cassandra/serializers/Int32Serializer.java:30-33` |
| `bigint` | `LongSerializer` | `null` | `serializers/LongSerializer.java:30-33` |
| `float` | `FloatSerializer` | `null` | `serializers/FloatSerializer.java:30-36` |
| `double` | `DoubleSerializer` | `null` | `serializers/DoubleSerializer.java:30-35` |
| `boolean` | `BooleanSerializer` | `null` | `serializers/BooleanSerializer.java:32-38` |
| `uuid` | `UUIDSerializer` | `null` | `serializers/UUIDSerializer.java:31-34` |
| `timestamp` | `TimestampSerializer` | `null` | `serializers/TimestampSerializer.java:137-140` |
| `inet` | `InetAddressSerializer` | `null` | `serializers/InetAddressSerializer.java:32-45` |
| `counter` | `CounterSerializer` **extends `LongSerializer`, adds nothing** | `null` | `serializers/CounterSerializer.java:20-23` (whole class) → `LongSerializer.java:30-33` |
| `timeuuid` | `TimeUUID.Serializer` (nested; there is **no** `serializers/TimeUUIDSerializer.java` — a 404 at this tag) | `null` | `org/apache/cassandra/utils/TimeUUID.java:339-342` |

Representative quotes:

`serializers/Int32Serializer.java:30-33`
```java
    public <V> Integer deserialize(V value, ValueAccessor<V> accessor)
    {
        return accessor.isEmpty(value) ? null : accessor.toInt(value);
    }
```

`serializers/Int32Serializer.java:40-44` — the `validate()` spelling the question refers to:
```java
    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) != 4 && !accessor.isEmpty(value))
            throw new MarshalException(String.format("Expected 4 or 0 byte int (%d)", accessor.size(value)));
    }
```
Note the message itself — **"Expected 4 **or 0** byte int"**. Empty is not tolerated by accident; it is
named in the diagnostic.

`serializers/CounterSerializer.java:20-23` — the entire file:
```java
public class CounterSerializer extends LongSerializer
{
    public static final CounterSerializer instance = new CounterSerializer();
}
```

`utils/TimeUUID.java:339-342`
```java
        public <V> TimeUUID deserialize(V value, ValueAccessor<V> accessor)
        {
            return accessor.isEmpty(value) ? null : accessor.toTimeUUID(value);
        }
```

---

## 2. Does `serialize(null)` return an EMPTY buffer? And does that reasoning transfer to a cell path?

### 2a. Yes — empty IS the wire spelling of `null`, for a VALUE, and Cassandra says so in its own contract.

`serializers/Int32Serializer.java:35-38`
```java
    public ByteBuffer serialize(Integer value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }
```

`serializers/BooleanSerializer.java:40-44`
```java
    public ByteBuffer serialize(Boolean value)
    {
        return (value == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                               : value ? TRUE : FALSE; // false
    }
```
Same shape at `LongSerializer.java:35-38`, `FloatSerializer.java:38-41`,
`DoubleSerializer.java:37-40`, `UUIDSerializer.java:36-39`,
`TimestampSerializer.java:142-145`, `InetAddressSerializer.java:47-50`,
`TimeUUID.java:323-332`. So `serialize(null) == EMPTY` is universal across the family, and
`deserialize(EMPTY) == null` (§1) is its exact inverse.

This is not an emergent coincidence — it is the **declared base contract**:

`serializers/TypeSerializer.java:71-74`
```java
    public <V> boolean isNull(@Nullable V buffer, ValueAccessor<V> accessor)
    {
        return buffer == null || accessor.isEmpty(buffer);
    }
```

…and the two type families for which empty is **not** null override it precisely to say so:

`serializers/BytesSerializer.java:57-62`
```java
    @Override
    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        // !buffer.hasRemaining() is not "null" for bytes types, it is byte[0]
        return buffer == null;
    }
```

`serializers/AbstractTextSerializer.java:72-77`
```java
    @Override
    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        // !buffer.hasRemaining() is not "null" for string types, it is the empty string
        return buffer == null;
    }
```

And the type layer carries a dedicated predicate whose javadoc states the rule outright:

`db/marshal/AbstractType.java:455-461`
```java
    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }
```

`db/marshal/Int32Type.java:49-59`
```java
    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    @Override
    public boolean isEmptyValueMeaningless()
    {
        return true;
    }
```
(`db/marshal/CounterColumnType.java:40-50` is identical.)

Consequence at the CQL-literal renderer: `TypeSerializer.java:81-86`
```java
    public final @Nonnull String toCQLLiteral(@Nullable ByteBuffer buffer)
    {
        return isNull(buffer)
               ? "null"
               :  maybeQuote(toCQLLiteralNonNull(buffer));
    }
```
So Cassandra renders an empty `int` **as the CQL literal `null`**.

### 2b. **No — that reasoning does NOT transfer to a cell path. This is the crux.**

There are **three different framings** in play, and only one of them can spell "null" at all.

**(i) Cell path, on disk — an UNSIGNED VInt length. There is NO null spelling.**

`db/marshal/CollectionType.java:361-382`
```java
    private static class CollectionPathSerializer implements CellPath.Serializer
    {
        public void serialize(CellPath path, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(path.get(0), out);
        }

        public CellPath deserialize(DataInputPlus in) throws IOException
        {
            return CellPath.create(ByteBufferUtil.readWithVIntLength(in));
        }

        public long serializedSize(CellPath path)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(path.get(0));
        }

        public void skip(DataInputPlus in) throws IOException
        {
            ByteBufferUtil.skipWithVIntLength(in);
        }
    }
```

`utils/ByteBufferUtil.java:356-360` and `:382-389`
```java
    public static void writeWithVIntLength(ByteBuffer bytes, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(bytes.remaining());
        out.write(bytes);
    }
...
    public static ByteBuffer readWithVIntLength(DataInputPlus in) throws IOException
    {
        int length = in.readUnsignedVInt32();
        if (length < 0)
            throw new IOException("Corrupt (negative) value length encountered");

        return ByteBufferUtil.read(in, length);
    }
```

The length is **unsigned**; a negative length is not merely unused, it is a declared **corruption**.
And the in-memory object cannot be null either:

`db/rows/CellPath.java:44-48`
```java
    public static CellPath create(ByteBuffer value)
    {
        assert value != null;
        return new SingleItemCellPath(value);
    }
```

> **Therefore a zero-length cell path is UNAMBIGUOUSLY "the empty buffer". It is not "absent" and it
> is not "null", because this framing has no way to say either of those things.** The
> `-1`-vs-`0`-length distinction the question asks about **does not exist in the cell-path framing**.

**(ii) Native-protocol collection element — a SIGNED 32-bit length, where `-1` and `0` ARE distinct.**

`serializers/CollectionSerializer.java:82-101`
```java
    public static <V> void writeValue(ByteBuffer output, V value, ValueAccessor<V> accessor)
    {
        if (value == null)
        {
            output.putInt(-1);
            return;
        }

        output.putInt(accessor.size(value));
        accessor.write(value, output);
    }

    public static <V> V readValue(V input, ValueAccessor<V> accessor, int offset)
    {
        int size = accessor.getInt(input, offset);
        if (size < 0)
            return null;

        return accessor.slice(input, offset + TypeSizes.INT_SIZE, size);
    }
```

Here `-1` means null and `0` means empty, and they are genuinely different. **But a map cell path can
never produce `-1` on this path**, because the repack copies the path bytes verbatim into a non-null
`ByteBuffer` slot:

`db/marshal/MapType.java:323-334`
```java
    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
        {
            Cell<?> c = cells.next();
            bbs.add(c.path().get(0));
            bbs.add(c.buffer());
        }
        return bbs;
    }
```
An empty (but non-null) path buffer takes the `output.putInt(accessor.size(value))` branch and is
written as length **`0`**, never `-1`.

**(iii) Cell VALUE on disk — a FLAG BIT, not a length.**

`db/rows/Cell.java:241-259` (the format comment) and `:264`
```
     *     [ flags ][ timestamp ][ deletion time ][    ttl    ][ path size ][ path ][ value size ][ value ]
...
     *   - [ value size ] is the size of the [ value ] field. It's present unless either the cell has the HAS_EMPTY_VALUE_MASK, or the value
     *       for columns of this type have a fixed length.
     *   - [ path size ] is the size of the [ path ] field. Present iff this is the cell of a complex column.
```
```java
        private final static int HAS_EMPTY_VALUE_MASK        = 0x04; // Wether the cell has an empty value. This will be the case for tombstone in particular.
```
An empty **value** gets a dedicated flag bit. An empty **path** gets no such affordance — it is just
a VInt `0x00`. The asymmetry is structural: **the encoding treats an empty path as an ordinary path
of length zero, and offers no alternative reading of it.**

**Answer to 2b:** the empty-means-null reasoning is a property of **`TypeSerializer`'s
value-decoding contract**, not of the cell-path framing. The framing itself decides only that the
key's bytes are `[]`. Whether `[]` then *decodes* to `null` is a separate question, answered by §1 —
and answered inconsistently across Cassandra's own layers (§5).

---

## 3. **The crux: can Cassandra 5.0 WRITE such a cell, and READ it back?**

### Verdict: **YES to all three of (a) not-rejected, (b) written, (c) readable.** Traced end to end.

#### Step 1 — `blob_as_int(0x)` produces the empty buffer, and the bare `0x` literal is grammatical.

The lexer permits **zero** hex digits:

`src/antlr/Lexer.g:378-379`
```
HEXNUMBER
    : '0' X HEX*
```
(`HEX*` — Kleene star, `src/antlr/Lexer.g:299-301` defines `HEX` as one hex digit. So `0x` alone is a
valid blob literal.) It becomes an empty buffer via
`db/marshal/BytesType.java:47-52`:
```java
    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBuffer.wrap(Hex.hexToBytes(source));
```
— a **fresh** zero-length `ByteBuffer`.

`blob_as_int` is a pure pass-through gated only on `validate()`:

`cql3/functions/BytesConversionFcts.java:107-127`
```java
        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            ByteBuffer val = arguments.get(0);

            if (val != null)
            {
                try
                {
                    toType.getType().validate(val);
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException(String.format("In call to function %s, value 0x%s is not a " +
                                                                    "valid binary representation for type %s",
                                                                    name, ByteBufferUtil.bytesToHex(val), toType));
                }
            }

            return val;
        }
```
`toType.getType().validate(EMPTY)` → `AbstractType.java:202-205` → `Int32Serializer.validate` →
**passes** (§1). The declared-return-type recheck also passes:

`cql3/functions/FunctionCall.java:79-95`
```java
    private static ByteBuffer executeInternal(ScalarFunction fun, Arguments arguments) throws InvalidRequestException
    {
        ByteBuffer result = fun.execute(arguments);
        try
        {
            // Check the method didn't lie on it's declared return type
            if (result != null)
                fun.returnType().validate(result);
```

> **So: `blob_as_int(0x)` ACCEPTS a zero-length blob and yields an empty, non-null `int` buffer.**
> Both the modern (`blob_as_int`) and legacy (`blobasint`) spellings are registered —
> `BytesConversionFcts.java:99-105` builds the name as
> `(useLegacyName ? "blobas" : "blob_as_") + toType`, and `:129-133` registers the legacy alias.

A function call is grammatically legal as a map-literal key — `src/antlr/Parser.g:1558-1562`
frames both sides of a map entry as `term`, and `term` (`Parser.g:1631-1633` → `termAddition` →
`termMultiplication` → `termGroup`) reaches function calls:
```
mapLiteral[Term.Raw k] returns [Term.Raw value]
    @init { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); }
    @after { $value = new Maps.Literal(m); }
    : ':' v=term {  m.add(Pair.create(k, v)); } ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )*
    ;
```

#### Step 2 — (a) **CQL does NOT reject it.** Every map-key guard is a *reference* null check or an *identity* check, never an emptiness check.

`cql3/Maps.java:334-356` (`DelayedValue.bind` — the map-literal path):
```java
        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            SortedMap<ByteBuffer, ByteBuffer> buffers = new TreeMap<>(comparator);
            for (Map.Entry<Term, Term> entry : elements.entrySet())
            {
                // We don't support values > 64K because the serialization format encode the length as an unsigned short.
                ByteBuffer keyBytes = entry.getKey().bindAndGet(options);

                if (keyBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (keyBytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    throw new InvalidRequestException("unset value is not supported for map keys");
```

`cql3/Maps.java:421-441` (`SetterByKey.execute` — the `m[k] = v` path):
```java
        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to set a value for a single key on a frozen map";
            ByteBuffer key = k.bindAndGet(params.options);
            ByteBuffer value = t.bindAndGet(params.options);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");
            if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw new InvalidRequestException("Invalid unset map key");

            CellPath path = CellPath.create(key);
```
(`Maps.java:506-515`, `DiscarderByKey.execute`, is the same pair of checks.)

**`keyBytes == null` is a Java reference comparison** — an empty `ByteBuffer` is not `null`. And
`== ByteBufferUtil.UNSET_BYTE_BUFFER` is **object identity against a sentinel**, not a length test.
The two sentinels are distinct objects that merely happen to share a length:

`utils/ByteBufferUtil.java:90` and `:92`
```java
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
...
    public static final ByteBuffer UNSET_BYTE_BUFFER = ByteBuffer.wrap(new byte[]{});
```
Since `BytesType.fromString("")` allocates a **fresh** buffer (Step 1), it is neither sentinel and
passes both guards.

> **There is NO check anywhere on the map write path that rejects a zero-LENGTH key.**

#### Step 3 — (b) it is written, and it validates.

`cql3/UpdateParameters.java:164-175`
```java
    public Cell<?> addCell(ColumnMetadata column, CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        Guardrails.columnValueSize.guard(value.remaining(), column.name.toString(), false, clientState);

        if (path != null && column.type.isMultiCell())
            Guardrails.columnValueSize.guard(path.dataSize(), column.name.toString(), false, clientState);

        Cell<?> cell = ttl == LivenessInfo.NO_TTL
                       ? BufferCell.live(column, timestamp, value, path)
```
No emptiness guard — only a size **ceiling**, which `path.dataSize() == 0` trivially satisfies.

The contrast is decisive, and it is in the *same file*. Cassandra **does** know how to reject an
empty component, and states exactly why it chooses to — for a compact-table clustering value, and
nowhere else:

`cql3/UpdateParameters.java:88-101`
```java
    public <V> void newRow(Clustering<V> clustering) throws InvalidRequestException
    {
        if (metadata.isCompactTable())
        {
            if (TableMetadata.Flag.isDense(metadata.flags) && !TableMetadata.Flag.isCompound(metadata.flags))
            {
                // If it's a COMPACT STORAGE table with a single clustering column and for backward compatibility we
                // don't want to allow that to be empty (even though this would be fine for the storage engine).
                assert clustering.size() == 1 : clustering.toString(metadata);
                V value = clustering.get(0);
                if (value == null || clustering.accessor().isEmpty(value))
                    throw new InvalidRequestException("Invalid empty or null value for column " + metadata.clusteringColumns().get(0).name);
```

> **"we don't want to allow that to be empty (even though this would be fine for the storage
> engine)"** — Cassandra's own comment separates *"the storage engine accepts empty"* from *"CQL
> forbids it here"*. It forbids it **here** and does not forbid it for a cell path.

Cell-level validation accepts it too:

`schema/ColumnMetadata.java:457-467`
```java
    private void validateCellPath(CellPath path)
    {
        if (!isComplex())
            throw new MarshalException("Only complex cells should have a cell path");

        assert type.isMultiCell();
        if (type.isCollection())
            ((CollectionType)type).nameComparator().validate(path.get(0));
        else
            ((UserType)type).nameComparator().validate(path.get(0));
    }
```
For `map<int,…>` the name comparator **is** `Int32Type` (`db/marshal/MapType.java:128-131`:
`public AbstractType<K> nameComparator() { return keys; }`), whose `validate` accepts empty (§1).

And it reaches the disk unconditionally:

`db/rows/Cell.java:300-301` (inside `Serializer.serialize`)
```java
            if (column.isComplex())
                column.cellPathSerializer().serialize(cell.path(), out);
```

#### Step 4 — the empty key is a FIRST-CLASS, ORDERABLE key that coexists with ordinary keys.

`db/marshal/Int32Type.java:61-71`
```java
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int diff = accessorL.getByte(left, 0) - accessorR.getByte(right, 0);
        if (diff != 0)
            return diff;

        return ValueAccessor.compare(left, accessorL, right, accessorR);
    }
```
With `left` empty and `right` non-empty: `Boolean.compare(false, true) == -1`. **An empty `int`
sorts strictly BEFORE every non-empty `int`, including `Integer.MIN_VALUE`.** The comparator does not
error, does not treat empty as equal to anything else, and gives it a stable, unique position. That
same comparator is the `TreeMap` comparator in `Maps.DelayedValue.bind`
(`cql3/Maps.java:336`, `new TreeMap<>(comparator)`), so a single map literal can carry the empty key
**alongside** ordinary keys with no collision.

#### Step 5 — (c) it is readable.

On-disk read back: `db/rows/Cell.java:324-326`
```java
            CellPath path = column.isComplex()
                            ? column.cellPathSerializer().deserialize(in)
                            : null;
```
→ `CollectionPathSerializer.deserialize` → `CellPath.create(readWithVIntLength(in))` → a `CellPath`
holding the empty buffer. No branch inspects the length.

`SELECT` on the multicell map repacks via `db/marshal/CollectionType.java:171-177`
(`serializeForNativeProtocol` → `CollectionSerializer.pack`) using
`MapType.serializedValues` (quoted in §2b(ii)) — the empty key goes out as a **length-`0`** element,
not `-1`.

#### Step 6 — but Cassandra's own object-level map decoder yields a **NULL KEY**.

`serializers/MapSerializer.java:118-154`
```java
    @Override
    public <I> Map<K, V> deserialize(I input, ValueAccessor<I> accessor)
    {
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();
...
            Map<K, V> m = new LinkedHashMap<>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                I key = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(key, accessor);
                keys.validate(key, accessor);
...
                m.put(keys.deserialize(key, accessor), values.deserialize(value, accessor));
```

Two things happen here, and they matter:

1. `readNonNullValue` (`CollectionSerializer.java:103-109`) does **not** reject it — it throws only
   when `readValue` returns `null`, i.e. only when the length is **negative**. A `0` length is
   "non-null" by that definition, so the guard is satisfied.
2. `keys.deserialize(key, accessor)` on an empty buffer returns **`null`** (§1), and that `null` is
   used **as the map key**: `m.put(null, …)` into a `LinkedHashMap`, which permits exactly one
   null key.

> **So Cassandra's own reader — at the object layer — produces a map with a `null` KEY.** It does not
> throw, and it does not skip the entry.

---

## 4. What would `sstabledump` render?

`tools/JsonTransformer.java:444-458` (inside `serializeCell`)
```java
            if (type.isCollection() && type.isMultiCell()) // non-frozen collection
            {
                CollectionType ct = (CollectionType) type;
                json.writeFieldName("path");
                arrayIndenter.setCompact(true);
                json.writeStartArray();
                for (int i = 0; i < cell.path().size(); i++)
                {
                    json.writeString(ct.nameComparator().getString(cell.path().get(i)));
                }
                json.writeEndArray();
```

So the rendering is `nameComparator().getString(<path bytes>)`:

`db/marshal/AbstractType.java:146-156`
```java
    /** get a string representation of the bytes used for various identifier (NOT just for log messages) */
    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        if (value == null)
            return "null";

        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(value, accessor);

        return serializer.toString(serializer.deserialize(value, accessor));
    }
```
For an empty `int` path: `value != null` (it is an empty buffer, so the `"null"` branch is **not**
taken), `validate` **passes**, `deserialize` returns **`null`**, then:

`serializers/Int32Serializer.java:46-49`
```java
    public String toString(Integer value)
    {
        return value == null ? "" : String.valueOf(value);
    }
```

> ### `sstabledump` renders a zero-length `int` cell path as the EMPTY JSON STRING: `"path" : [""]`.
> ### It does NOT throw.

**Can it throw?** Not for any of the ten families in scope — every one has a null-guarded
`toString`: `Int32Serializer:46-49`, `LongSerializer:46-49` (also `counter`, by inheritance),
`FloatSerializer:49-52`, `DoubleSerializer:48-51`, `BooleanSerializer:52-55`,
`UUIDSerializer:49-52`, `TimestampSerializer:190-198` (`toStringUTC`: `value == null ? "" : …`),
`InetAddressSerializer:67-70`, `TimeUUID.java:318-321`. All render `""`.

**One adjacent NPE hazard, recorded but out of scope for map keys.** `CounterColumnType` overrides
`toJSONString` without a null guard —

`db/marshal/CounterColumnType.java:90-94`
```java
    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return CounterSerializer.instance.deserialize(buffer).toString();
    }
```
`deserialize(EMPTY)` is `null` (§1), so `.toString()` would NPE. This is **not** reachable through a
map cell path (`counter` cannot be a collection element type in CQL, and `JsonTransformer` uses
`getString` for the path, not `toJSONString`), but it is a real asymmetry worth knowing: the
`getString`/`toString` renderers are null-safe and `CounterColumnType.toJSONString` is not.

**Also decidable, and worth recording: `SELECT JSON` renders it the same way.**

`db/marshal/MapType.java:362-388`
```java
            // map keys must be JSON strings, so convert non-string keys to strings
            ByteBuffer kv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(kv, ByteBufferAccessor.instance);
            String key = keys.toJSONString(kv, protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(JsonUtils.quoteAsJsonString(key)).append('"');
```
with `db/marshal/Int32Type.java:126-130`
```java
    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return Objects.toString(getSerializer().deserialize(buffer), "\"\"");
    }
```
`deserialize(EMPTY)` → `null` → `Objects.toString(null, "\"\"")` → the two-character string `""`,
which `startsWith("\"")` → appended verbatim. **`SELECT JSON` yields `{"": <value>}`.**

**And the rendering round-trips.** `db/marshal/Int32Type.java:85-89`
```java
    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
```
`getString(EMPTY) == ""` and `fromString("") == EMPTY`. The `""` rendering is not a lossy
stringification; it is the designated textual form of the empty `int`.

---

---

## 4b. EMPIRICAL CONFIRMATION — measured against a real Cassandra 5.0.2

**Authority level 2 (`sstabledump` output), run by the team lead; distinct from and corroborating
everything above, which is authority level 1 (pinned source).** Container
`cqlite-issue3805-probe`, `release_version 5.0.2`, real flushed `nb-1-big-Data.db`, dumped with
`/opt/cassandra/tools/bin/sstabledump`. Raw evidence: `.oracle-3805/empirical/`
(`version.txt`, `describe-p.f.cql`, `sstable-listing.txt`, `sstabledump-p.f.json`).

**Every prediction §1–§4 makes from the source was confirmed.** The two authorities agree; nothing
below contradicts the source reading, and three source-derived claims that were *inferences* are now
*measurements*.

### 4b.1 Sub-question 3 is answered YES, empirically

`blobAsX(0x)` writes an empty fixed-width map key, both through a **map literal**
(`INSERT ... VALUES (1, {blobAsInt(0x): 7})`) and through **subscript assignment**
(`UPDATE t SET mi[blobAsInt(0x)] = 7 WHERE id = 2`) — the two routes §3 predicted from
`Maps.DelayedValue.bind` and `Maps.SetterByKey.execute` respectively.

Per-type census of `SELECT blobAsX(0x)`:

| Outcome | Types |
|---|---|
| **ACCEPTED** | `int`, `bigint`, `float`, `double`, `timestamp`, `uuid`, `timeuuid`, `boolean`, `inet`, `counter`, `decimal`, `varint`, `text` |
| **REFUSED** — `"value 0x is not a valid binary representation for type X"` | `tinyint`, `smallint`, `date`, `time` |

(`blobAsBlob` does not exist; a `blobAsBlob` refusal in the probe is a probe artifact, not a finding.)

**The split is exactly the `validate()` spelling, verified type by type at the pinned tag.** The four
REFUSED families are precisely and only those whose `validate` is a **bare `!= N` with no `isEmpty`
escape clause**:

`serializers/ByteSerializer.java:40-44` (`tinyint`)
```java
    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) != 1)
            throw new MarshalException(String.format("Expected 1 byte for a tinyint (%d)", accessor.size(value)));
    }
```
`serializers/ShortSerializer.java:40-44` (`smallint`) — `size != 2`, bare.
`serializers/SimpleDateSerializer.java:118-122` (`date`) — `size != 4`, bare.
`serializers/TimeSerializer.java:71-75` (`time`) — `size != 8`, bare.

Contrast the ACCEPTED families, every one of which carries the escape: `Int32Serializer.java:40-44`
(`size != 4 && !isEmpty`), `LongSerializer.java:40-44`, `FloatSerializer.java:43-47`,
`DoubleSerializer.java:42-46`, `TimestampSerializer.java:184-188`, `UUIDSerializer.java:42-47`
(each `size != N && !isEmpty`); `BooleanSerializer.java:46-50` (`size > 1`, so `0` passes);
`InetAddressSerializer.java:52-55` and `TimeUUID.java:306-316` (early `return` on empty);
`IntegerSerializer.java:41-44` (`varint` — validate body is the comment `// no invalid integers.`,
so everything passes); `DecimalSerializer.java:58-63` (see §4c).

> **The empirically observed CQL accept/refuse boundary IS the `validate()` spelling. This is a
> perfect two-way agreement between authority #1 and authority #2, and it makes the spelling a
> reliable predictor rather than an incidental detail.**

### 4b.2 A subtle asymmetry the measurement exposes, and it is the discriminator #3805 needs

For all four REFUSED families, `deserialize` **still returns `null` on an empty buffer** —
`ByteSerializer.java:30-33`, `ShortSerializer.java:30-33`, `SimpleDateSerializer.java:50-53`,
`TimeSerializer.java:32-35`. So their **readers are permissive while their validators are strict.**

That splits the two halves of "can this appear on disk" cleanly:

| Family | CQL can write it? | `validate` on empty | So a 0-length cell path is… |
|---|---|---|---|
| `int`, `bigint`, `float`, `double`, `timestamp`, `uuid`, `timeuuid`, `boolean`, `inet`, `varint`, `decimal` | **YES** (measured) | **passes** | **LEGAL data** — must be preserved |
| `tinyint`, `smallint`, `date`, `time` | **NO** (measured) | **throws** | **CORRUPTION on Cassandra's own terms** — `ColumnMetadata.validateCellPath:457-467` would reject it |

> **This is the line a reader should draw, and it is drawable entirely from `validate()`.** For the
> top row, refusing the entry rejects data Cassandra would have read. For the bottom row, accepting
> it accepts bytes Cassandra's own `validateCellPath` throws on.

### 4b.3 Cassandra returns the key as PRESENT and NOT NULL

The Python driver renders the row as `OrderedMapSerializedKey([(EMPTY, 7)])` — an **`EmptyValue`
sentinel, distinct from `None`** — and cqlsh's *formatter* then fails with
`required argument is not an integer`. So the driver **cannot render** the key as an int but
**keeps it as a present, distinct EMPTY key**; the entry is not dropped and the key is not null.

> **This is the most consequential empirical result, because it CONSTRAINS §5.** The
> "empty ⇒ null" contract that §1/§2a establish for the **value** read path (and that sibling issue
> #3847 answers for values) does **NOT** transfer unchanged to a **KEY**. §3 Step 6 showed
> Cassandra's server-side `MapSerializer.deserialize` produces a `null` key; the driver, on the same
> bytes, produces a present `EmptyValue`. **Cassandra's own two decoders disagree**, and the one on
> the path a user actually observes returns *present and distinct*.

### 4b.4 `sstabledump` renders every empty fixed-width key as `""` — measured

Verbatim from `.oracle-3805/empirical/sstabledump-p.f.json`:

```json
  { "name" : "m_int",    "path" : [ "" ],         "value" : 7 },
  { "name" : "m_int",    "path" : [ "42" ],       "value" : 1 },
  { "name" : "m_bigint", "path" : [ "" ],         "value" : 7 },
  { "name" : "m_bigint", "path" : [ "99" ],       "value" : 1 },
  { "name" : "m_uuid",   "path" : [ "" ],         "value" : 7 },
  { "name" : "m_bool",   "path" : [ "" ],         "value" : 7 },
  { "name" : "m_inet",   "path" : [ "" ],         "value" : 7 },
  { "name" : "m_inet",   "path" : [ "10.0.0.1" ], "value" : 1 },
  { "name" : "m_dec",    "path" : [ "" ],         "value" : 7 },
  { "name" : "m_text",   "path" : [ "" ],         "value" : 7 },
  { "name" : "m_text",   "path" : [ "k" ],        "value" : 1 }
```

Not null, not omitted, no throw — exactly what §4 derives from
`JsonTransformer.java:452` → `AbstractType.getString:147-156` → `<T>Serializer.toString(null)`.

**Three further facts visible in that dump that the source predicted and nobody had measured:**

1. **The empty key sorts FIRST within every column.** `""` precedes `"42"` (`m_int`), `"99"`
   (`m_bigint`), `"10.0.0.1"` (`m_inet`) and `"k"` (`m_text`). This is exactly
   `Int32Type.compareCustom:61-71`'s `Boolean.compare(right.isEmpty, left.isEmpty)` == `-1`, now
   confirmed on real bytes and for four independent key types. §3 Step 4's ordering claim is
   **measured, not merely derived.**
2. **The empty entry COEXISTS with its non-empty sibling** — 4 of the 7 columns carry both. So the
   fixture genuinely distinguishes "entry dropped" (one fewer entry) from "column missing", and the
   empty key collides with nothing.
3. **`m_text` `""` is byte-identical in the dump to `m_int` `""`.** §5's ambiguity note is therefore
   **demonstrated, not hypothetical**: `sstabledump`'s `""` does **not** distinguish an empty
   fixed-width key from an empty-string `text` key. **The `sstabledump` rendering is not by itself a
   sufficient oracle to tell those two apart — the schema is required.** Any parity comparison keyed
   only on the dump text is blind to that difference.

One incidental observation for fixture design: each column's cells are preceded by a **complex
deletion** (`"deletion_info" : { "marked_deleted" : … }`), because a whole-map `INSERT` is a
collection *overwrite* and emits a complex tombstone first. A parity golden built from an `INSERT`
must expect it; a subscript `UPDATE` would not produce one.

---

## 4c. TWO CORRECTIONS TO COMMITTED CLAIMS

Both were flagged empirically and are **verified here against the pinned source**, which is what
they must rest on. Both are recorded prominently because each currently sits in committed code or a
committed schema as a *justified* claim, and in each case the justification does not hold.

### (a) `decimal` is NOT "corrupt on Cassandra's own terms" — an empty `decimal` is EXPLICITLY LEGAL

**The full source, quoted rather than grepped** —
`org/apache/cassandra/serializers/DecimalSerializer.java:31-40` and `:58-63`:

```java
    public <V> BigDecimal deserialize(V value, ValueAccessor<V> accessor)
    {
        if (value == null || accessor.isEmpty(value))
            return null;

        // do not consume the contents of the ByteBuffer
        int scale = accessor.getInt(value, 0);
        BigInteger bi = new BigInteger(accessor.toArray(value, 4, accessor.size(value) - 4));
        return new BigDecimal(bi, scale);
    }
```
```java
    public <T> void validate(T value, ValueAccessor<T> accessor) throws MarshalException
    {
        // We at least store the scale.
        if (!accessor.isEmpty(value) && accessor.size(value) < 4)
            throw new MarshalException(String.format("Expected 0 or at least 4 bytes (%d)", accessor.size(value)));
    }
```

Three independent things in those eleven lines say empty is legal:

1. The guard is **`!isEmpty && size < 4`** — empty is *explicitly excluded from the failure
   condition*, the same escape-clause shape as `Int32Serializer`'s `size != 4 && !isEmpty`.
2. The **diagnostic message names it**: *"Expected **0** or at least 4 bytes"*.
3. `deserialize` **null-guards empty first** (`:33`), so the `getInt(value, 0)` that would
   underflow is never reached.

So `decimal` belongs in the **same family as `int`**, not in the refused set. Confirmed
empirically: an empty `decimal` map key inserted, flushed, and dumped as `"path" : [ "" ]`
(§4b.4, `m_dec`).

**What this falsifies.** Verified verbatim on `origin/issue-3747-empty-map-key-dropped` at
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/regression_3747_empty_map_key_tests.rs:175-178`:

> ```
>  ///   * **`Err` only where Cassandra's own `validate`/`split` THROWS.** `tinyint`/
>  ///     `smallint`/`date`/`time` are spelled with a strict `!= N` check and `decimal`
>  ///     needs >= 4 bytes, so an empty buffer is corrupt ON CASSANDRA'S OWN TERMS.
>  ///     Refusing adds no availability risk for data Cassandra would have read.
> ```

and the test itself, `:190`:
> ```rust
>     for ty in ["tinyint", "smallint", "date", "time", "decimal"] {
> ```

**The RULE stated at `:175` is correct and should be kept; only its APPLICATION to `decimal` is
wrong.** *"`Err` only where Cassandra's own `validate` THROWS"* is exactly the right test — and
`DecimalSerializer.validate` does **not** throw on empty. The four `!= N` families are correctly
classified (§4b.1 verifies all four at the pinned tag). So this is a **one-element membership error
inside a correct rule**, not a design flaw: the fix is to move `decimal` from the refused list to
the preserved list, and the sentence's own final clause is the reason —
*"Refusing adds no availability risk for data Cassandra would have read"* is **false for `decimal`**,
because Cassandra *would* have read it.

The same premise is committed in two more places, which this falsifies together:
- **issue #3805's own table** — "both layers agree empty decimal is short, so arguably fine". The
  two layers do *not* agree that it is short; both accept it.
- **the owner ruling's constraint 3** — "`decimal` stays refused; both layers agree". Same premise,
  same falsification. **This one is an owner ruling and so is flagged for the lead rather than
  treated as settled by this audit** — the measurement and the source are unambiguous, but changing
  a ruling is not a worker's call.

### (b) An empty `inet` map key IS producible in CQL — the exclusion is unnecessary, not merely unproven

Verified verbatim on the same branch,
`test-data/schemas/issue-3747-empty-map-key.cql:63-68`:

> ```
> -- NOT COVERED, AND DELIBERATELY SO: `inet`. `InetAddressSerializer.validate`
> -- early-returns on an empty buffer, so an empty inet key is representable ON
> -- DISK — but CQL has NO literal that expresses one, so a CASSANDRA-WRITTEN
> -- fixture cannot carry it. Claiming inet coverage from a fixture that cannot
> -- contain it would be a false claim; the empty-inet path stays unproven here and
> -- is called out rather than quietly implied.
> ```

The **premise about the serializer is correct** and correctly cited —
`InetAddressSerializer.java:52-55` does early-return on empty (§1, §4b.1). **The inference is what
fails:** `blobAsInet(0x)` expresses an empty `inet`, the INSERT succeeded, and the dump carries
`m_inet` `"path" : [ "" ]` beside `"10.0.0.1"` (§4b.4).

**Stated precisely and fairly, because the distinction is the whole reason the author missed it:**
read hyper-literally, *"CQL has no **literal** that expresses one"* is **true** —
`blobAsInet(0x)` is a *function application over* a blob literal, not an inet literal. What is false
is the step from there to *"a CASSANDRA-WRITTEN fixture cannot carry it"*: a fixture needs an
**expressible term**, not a literal. So the correct disposition is that the exclusion is
**unnecessary**, not that the comment was careless — and `inet` should move from "deliberately not
covered" to "covered", since it is the one family in this group whose `validate` uses the
early-return spelling and is therefore worth covering on its own merits as a distinct code path.

**Generalisable lesson, and it applies to every "no CQL literal expresses this" claim in this
repository:** `blobAsX(0x)` is a universal constructor for the empty value of **any** type whose
`validate` admits empty (§3 Step 1 — `HEXNUMBER : '0' X HEX*` plus a pass-through function gated
only on `validate`). Before declaring an empty-value fixture unproducible, check `blobAsX(0x)`.
Measured: it works for 13 of 17 types probed, and the 4 failures are exactly the 4 whose `validate`
refuses (§4b.1).

---

## 5. The defensible reader answer — and where the sources do NOT decide

**The question decomposes into three claims, and they have different epistemic status. Stating that
split is the substance of this section.**

| | Claim | Status |
|---|---|---|
| **(i)** | The entry must be **PRESENT** — not dropped | **DECIDED** by the sources |
| **(ii)** | Its key must be **DISTINCT from a null key**, and distinct from every other key | **DECIDED** by the sources |
| **(iii)** | Which concrete CQLite `Value` variant represents it | **NOT DECIDED** by Cassandra at all |

### (i) PRESENT — decided

- The bytes exist and are unambiguous: an unsigned-VInt-framed zero-length path is *the empty
  buffer*; "absent" is unrepresentable in that framing (`ByteBufferUtil.java:356-360`, `:382-389`;
  `CellPath.java:44-48`). §2b.
- Cassandra writes it, validates it, and reads it back (§3; measured §4b.1).
- Both of Cassandra's renderers emit a value for it — `sstabledump` `"path" : [ "" ]`
  (`JsonTransformer.java:452`; measured §4b.4) and `SELECT JSON` `{"": v}`
  (`MapType.java:376` → `Int32Type.java:127-130`).
- The driver returns the entry (§4b.3).

Nothing anywhere drops it. **A reader that omits the entry is wrong** — and detectably so, since the
entry count changes and the empty key coexists with non-empty siblings (measured, §4b.4).

### (ii) DISTINCT FROM NULL — decided

Three independent grounds:

1. **The comparator gives it a unique position.** `Int32Type.compareCustom:61-71` sorts empty
   strictly before every non-empty `int`, including `Integer.MIN_VALUE`. Measured on real bytes for
   four key types (§4b.4). A key with a unique sort position is not interchangeable with anything.
2. **A null map key is ILLEGAL in CQL.** `Maps.java:342-343` (`"null is not supported inside
   collections"`), `:426-427` and `:510-511` (`"Invalid null map key"`). So "null key" is not a
   state CQL admits — a reader producing one produces a value the data model forbids, which cannot
   be the faithful reading of bytes Cassandra accepted.
3. **The driver hands back a PRESENT `EmptyValue` sentinel, explicitly distinct from `None`**
   (§4b.3). This is the decisive one, because it is the decoder on the path a user observes.

**The tension is real and must be confronted rather than resolved by preference:** Cassandra's
*server-side* `MapSerializer.deserialize` **does** yield a `null` key (`MapSerializer.java:144`;
§3 Step 6). So Cassandra's two decoders disagree. But that path is not reachable for a *multicell*
map read — `serializeForNativeProtocol`/`MapType.serializedValues:323-334` repacks the raw path
bytes and never decodes the key server-side (§2b(ii), §3 Step 5) — so `MapSerializer.deserialize`
applies to *frozen* maps and to internal validation, not to the read path this issue concerns. And
its `null` is tolerable there only because `LinkedHashMap` accepts one null key: **a Java container
accident, not a format statement.** Weighing a Java accident on an unreachable path against a
comparator contract, a CQL prohibition, and the driver's observable output, **(ii) is decided:
distinct from null.**

### (iii) Which CQLite `Value` variant — NOT decided, and I will not invent one

> **Cassandra does not answer this, and cannot: it has no "typed empty" object.** The decode is
> `null` (§1), there is no CQL literal for it (only `blobAsX(0x)`, a function application — §4c(b)),
> and the native protocol draws no value-side distinction from null (§2a). The `EmptyValue` sentinel
> in §4b.3 is a *Python driver* construct, not a Cassandra type-system entity.

So **(iii) is a CQLite representation choice, constrained — but not determined — by (i) and (ii).**
What the sources license is a *filter on candidates*, and that is all:

| Candidate | Satisfies (i)? | Satisfies (ii)? | Verdict from the sources |
|---|---|---|---|
| **`Value::Null`** | yes | **NO** | **RULED OUT.** Violates (ii) on all three grounds; and a null map key is illegal in CQL. |
| **`Value::Integer(0)`** (or any typed zero) | yes | **NO** | **RULED OUT.** `0` has a distinct 4-byte encoding (`Int32Serializer.java:35-38`) and a distinct sort position (`:61-71`); it can collide with a genuine `0` key in the same map. |
| **Opaque empty blob** — `Value::blob(vec![])`, the choice already committed at `regression_3747_empty_map_key_tests.rs:165` | yes | yes | **ADMISSIBLE.** Present; distinct from null and from every typed value. Loses the *type* (which the schema still carries), and renders as `""` if formatted as text. |
| **A typed-empty variant** (e.g. an `int` marked empty) | yes | yes | **ADMISSIBLE.** Also preserves the type. Requires a new variant and a rendering rule. |

**Both of the last two are defensible on the sources; the sources do not choose between them.**
That choice belongs to CQLite's own type-model design and to the lead — not to this oracle. The
committed `Value::blob(vec![])` is **not** wrong on the evidence; it satisfies both decided
constraints.

**One constraint the sources DO place on the rendering**, whichever variant is chosen: any
JSON/text output must be **`""`**, because that is what both Cassandra renderers emit (§4, §4b.4)
and it round-trips (`Int32Type.fromString("")` → EMPTY, `Int32Type.java:85-89`). A reader printing
`null` or `0` diverges from `sstabledump`, which in this repository is a parity failure independent
of which internal variant is used.

**And one caveat on that rendering, now measured:** `""` does **not** distinguish an empty
fixed-width key from an empty-string `text` key — `m_text` `""` is byte-identical to `m_int` `""` in
the dump (§4b.4). So the rendering is correct for parity and **insufficient as a type oracle**; the
schema must supply the type.

### The line between legal data and corruption — the one substantive addition the measurement makes

The reader must **not** apply the same answer to all fixed-width families. §4b.2 establishes a
clean, source-derivable boundary:

- **`validate` accepts empty** (`int`, `bigint`, `float`, `double`, `timestamp`, `uuid`, `timeuuid`,
  `boolean`, `inet`, `varint`, **`decimal`**): a zero-length cell path is **legal data**. CQL can
  produce it (measured), and refusing it rejects data Cassandra would have read.
- **`validate` throws on empty** (`tinyint`, `smallint`, `date`, `time` — the bare `!= N` spelling):
  a zero-length cell path is **corruption on Cassandra's own terms**.
  `ColumnMetadata.validateCellPath:457-467` would itself reject it, and CQL refuses to construct it
  (measured). Refusing is correct here.

Note the asymmetry that makes this worth stating explicitly: **all four refused families'
`deserialize` still returns `null` on empty** (`ByteSerializer.java:30-33` and siblings), so a
reader that keys its decision on *decodability* rather than on *`validate`* will silently accept
bytes Cassandra rejects. **Key the decision on `validate()`.**

### What the sources do NOT decide — stated, not papered over

1. **(iii) above** — the concrete `Value` variant. The single most important non-decision here.
2. **Whether Cassandra INTENDS this shape to be reachable.** Nothing declares it supported; nothing
   declares it forbidden. The strongest statement of intent is weak and points away from
   intentionality:

   `db/marshal/AbstractType.java:505-516`
   ```java
    /**
     * Defines if the type allows an empty set of bytes ({@code new byte[0]}) as valid input.  The {@link #validate(Object, ValueAccessor)}
     * and {@link #compose(Object, ValueAccessor)} methods must allow empty bytes when this returns true, and must reject empty bytes
     * when this is false.
     * <p/>
     * As of this writing, the main user of this API is for testing to know what types allow empty values and what types don't,
     * so that the data that gets generated understands when {@link ByteBufferUtil#EMPTY_BYTE_BUFFER} is allowed as valid data.
     */
    public boolean allowsEmpty()
   ```
   *"the main user of this API is for testing"* — documented as a **test-data-generation
   affordance**, not a user-facing feature. Best read as **legal but unintended**: the storage
   engine accepts it, CQL never blocks it, and no document says what it means. **That is precisely
   why (iii) has no source answer** — nobody designed a meaning for it.
3. **Why Cassandra's two decoders disagree** (server-side `null` vs driver `EmptyValue`). Both are
   observed; no source reconciles them. Do not cite either as *the* semantics.
4. **Whether `""` is ambiguous across types.** The sources never need to disambiguate, because the
   schema supplies the type. Measured to be ambiguous (§4b.4) — declared, not resolved.
5. **Multiple empty keys.** Cannot arise: one empty buffer, one sort position, so a map carries at
   most one empty key. The distinct-keys invariant is **never violated on disk**; it is strained
   only by a decode to `null`, which (ii) rules out anyway.
6. **Non-Java drivers.** Only the Python driver was measured (§4b.3); other drivers are neither
   Cassandra source nor `sstabledump` and are outside the authority chain.

---

## Summary table

| # | Question | Answer | Key citation (`cassandra-5.0.8`) |
|---|---|---|---|
| 1 | `deserialize(EMPTY)` for the fixed-width families | **`null`**, 10 of 10 verified | `serializers/Int32Serializer.java:30-33` (+ table in §1) |
| 2a | `serialize(null)` → EMPTY? | **Yes**, all 10; and `TypeSerializer.isNull` declares empty≡null as the base contract, overridden only by bytes/text | `Int32Serializer.java:35-38`; `TypeSerializer.java:71-74`; `BytesSerializer.java:57-62` |
| 2b | Does that transfer to a **cell path**? | **NO.** Cell path is an **unsigned** VInt length — `-1`/absent is **unrepresentable**, so 0-length can only mean "the empty buffer". (`-1` vs `0` *is* meaningful in the *native-protocol* framing, and in the *value* framing an empty value has its own flag bit.) | `CollectionType.java:361-382`; `ByteBufferUtil.java:356-360`, `:382-389`; `CellPath.java:44-48`; cf. `CollectionSerializer.java:82-101`; `Cell.java:243-264` |
| 3a | Rejected by CQL at INSERT? | **NO.** All guards are reference-null or sentinel-identity checks; no length check exists. `EMPTY_BYTE_BUFFER != UNSET_BYTE_BUFFER` | `Maps.java:342-345`, `:426-429`, `:510-513`; `ByteBufferUtil.java:90,92` |
| 3b | Written to an SSTable? | **YES.** No emptiness guard in `addCell`; `validateCellPath` accepts; path serialized unconditionally | `UpdateParameters.java:164-175`; `ColumnMetadata.java:457-467`; `Cell.java:300-301` |
| 3c | Readable by SELECT? | **YES**, as a length-`0` native-protocol element (never `-1`) | `Cell.java:324-326`; `MapType.java:323-334`; `CollectionSerializer.java:82-92` |
| 3d | `blob_as_int(0x)` accepts a zero-length blob? | **YES.** `HEXNUMBER : '0' X HEX*` (zero digits legal); the function only calls `validate()`, which accepts empty | `src/antlr/Lexer.g:378-379`; `BytesConversionFcts.java:107-127`; `Int32Serializer.java:40-44` |
| 3e | Does the empty key collide with normal keys? | **NO.** It sorts strictly FIRST and is distinct from all | `Int32Type.java:61-71` |
| 3f | Cassandra's own object decoder? | Produces a **`null` map key** — does not throw, does not skip | `MapSerializer.java:136-144`; `CollectionSerializer.java:103-109` |
| 4 | `sstabledump` rendering | **`"path" : [""]`** — the empty JSON string. **Cannot throw** for any in-scope family. (`SELECT JSON` → `{"": v}`.) | `JsonTransformer.java:444-458`; `AbstractType.java:147-156`; `Int32Serializer.java:46-49`; `MapType.java:362-388`; `Int32Type.java:126-130` |
| 5 | Defensible reader answer | **PRESENT** and **DISTINCT FROM NULL** are DECIDED; the concrete CQLite `Value` variant is **NOT DECIDED** by Cassandra. `Value::Null` and a typed `0` are RULED OUT; an opaque empty blob and a typed-empty are both admissible. Rendering must be `""` | §5 |
| 6 | **Empirical (authority #2)**: can CQL build it? | **YES, measured** on Cassandra 5.0.2 — `blobAsX(0x)`, via BOTH a map literal and a subscript `UPDATE` | §4b.1; `.oracle-3805/empirical/` |
| 7 | Which types accept `blobAsX(0x)`? | 13 ACCEPTED / 4 REFUSED, and the split **is exactly the `validate()` spelling** — the 4 refused (`tinyint`, `smallint`, `date`, `time`) are the only bare `!= N` forms | §4b.1; `ByteSerializer.java:40-44`, `ShortSerializer.java:40-44`, `SimpleDateSerializer.java:118-122`, `TimeSerializer.java:71-75` |
| 8 | Legal data vs corruption | `validate` **accepts** empty ⇒ **legal data, must be preserved**; `validate` **throws** ⇒ **corruption, refuse**. Key the decision on `validate()`, NOT on decodability — all 4 refused families' `deserialize` still returns `null` on empty | §4b.2 |
| 9 | What does Cassandra hand back for the key? | The driver returns a **PRESENT `EmptyValue`, distinct from null**; the server-side `MapSerializer` returns a **`null` key**. **The two decoders DISAGREE**, and the observable one says present | §4b.3; §3 Step 6 |
| 10 | Ordering, measured | The empty key sorts **FIRST** in every column, on real bytes, for 4 independent key types — confirming `Int32Type.compareCustom:61-71` | §4b.4 |
| 11 | **CORRECTION (a)** | **`decimal` is NOT corrupt on Cassandra's own terms.** `validate` is `!isEmpty && size < 4`, message *"Expected **0** or at least 4 bytes"*, `deserialize` null-guards empty. Falsifies #3805's table, the owner ruling's constraint 3, and PR #3783's refused-list membership. The RULE is right; only `decimal`'s membership is wrong | §4c(a); `DecimalSerializer.java:31-40`, `:58-63` |
| 12 | **CORRECTION (b)** | **An empty `inet` map key IS producible** via `blobAsInet(0x)`. PR #3783's schema premise ("no CQL literal") is true of *literals* narrowly; the inference "a Cassandra-written fixture cannot carry it" is FALSE. The exclusion is unnecessary, not unproven | §4c(b); measured §4b.4 |

---

## Fixture design implication

### A Cassandra-written fixture IS producible through ordinary CQL — and this is now DEMONSTRATED, not merely derived.

> **Status: BUILT.** The team lead produced exactly such a fixture on Cassandra 5.0.2 and dumped it
> (§4b, `.oracle-3805/empirical/`). Everything below is therefore a *reproduction recipe for a
> result already obtained*, not a proposal. The source derivation and the measurement agree in
> every particular.

Every link in the chain is verified above at the pinned tag: the `0x` literal is grammatical
(`Lexer.g:378-379`), `blob_as_int` passes it through after a `validate()` that accepts empty
(`BytesConversionFcts.java:107-127`), a function call is legal as a map-literal key
(`Parser.g:1558-1562`), no map-key guard tests length (`Maps.java:342-345`, `:426-429`), nothing in
`addCell` rejects an empty path (`UpdateParameters.java:164-175`), `validateCellPath` accepts it
(`ColumnMetadata.java:457-467`), and the path is serialized unconditionally
(`Cell.java:300-301`).

**No CQLite code is involved in producing it** — this is a Cassandra-written oracle, which is the
requirement for an on-disk framing property (CLAUDE.md: *"for any on-disk framing/encoding property,
the oracle must be Cassandra-written bytes (or Cassandra source), never CQLite's own output"*; #3042).

### CQL

```sql
CREATE KEYSPACE IF NOT EXISTS test_empty_cellpath
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE test_empty_cellpath.empty_key_map (
  id       int PRIMARY KEY,
  m_int    map<int, text>,
  m_bigint map<bigint, text>,
  m_uuid   map<uuid, text>,
  m_bool   map<boolean, text>
);

-- Route 1: map literal with a function-call key.
-- The empty key is written ALONGSIDE ordinary keys, which is the load-bearing part:
-- Int32Type.compareCustom sorts the empty key STRICTLY FIRST, so the on-disk cell order
-- pins both the framing AND the ordering in one fixture.
INSERT INTO test_empty_cellpath.empty_key_map (id, m_int)
VALUES (1, {blob_as_int(0x): 'empty', -2147483648: 'min', 0: 'zero', 1: 'one'})
USING TIMESTAMP 1700000000000000;

-- Route 2: element-wise UPDATE (Maps.SetterByKey) — the other reachable write path.
UPDATE test_empty_cellpath.empty_key_map USING TIMESTAMP 1700000000000000
  SET m_bigint[blob_as_bigint(0x)] = 'empty' WHERE id = 1;
UPDATE test_empty_cellpath.empty_key_map USING TIMESTAMP 1700000000000000
  SET m_uuid[blob_as_uuid(0x)]     = 'empty' WHERE id = 1;
UPDATE test_empty_cellpath.empty_key_map USING TIMESTAMP 1700000000000000
  SET m_bool[blob_as_boolean(0x)]  = 'empty' WHERE id = 1;
```

`USING TIMESTAMP` on every statement is required so `liveness_info.tstamp` is stable across
regenerations — the convention every `test-data/scripts/generate-*.sh` in this repo already follows.
Note `blob_as_boolean(0x)` is legal for a different reason from the others:
`BooleanSerializer.validate` (`:46-50`) rejects only `size > 1`, so `0` passes without needing the
`&& !isEmpty` clause at all.

### Docker route

The established pattern in this repo (e.g. `test-data/scripts/generate-issue-3790-comparator-ordering.sh`,
which is the closest precedent — it is *also* a multicell-collection **cell-path ordering** fixture):

1. `docker run` (or `podman`) a `cassandra:5.0.x` container; wait for
   `cqlsh -e "SELECT cluster_name FROM system.local;"` to succeed.
2. `docker exec <c> cqlsh -f /tmp/schema.cql` for the DDL above.
3. `docker exec <c> cqlsh -k <ks> -e "<INSERT/UPDATE>"` for each statement.
4. `docker exec <c> nodetool flush <keyspace>` — keyspace-scoped, so a concurrent foreign Cassandra
   cannot contaminate it.
5. Copy `/var/lib/cassandra/data/<ks>/<table>-<uuid>/` out, checkout-relative into
   `test-data/datasets/sstables/<keyspace>/<table>-<uuid>/` (the second built-in candidate root of
   the table-granular resolver, so the fixture resolves without an env var — #3220).
6. Capture the `sstabledump` output as the committed `*-Data.db.jsonl` golden.

### What the fixture must CONTAIN, and the assertions it licenses

- Complex column `m_int` carrying **4 cells**, whose `[path size]` VInts are, in on-disk order:
  **`0x00`** (the empty key — **first**, per `Int32Type.compareCustom:61-71`), then `0x04` three
  times for `-2147483648`, `0`, `1`.
- The first cell has a `[path size]` of unsigned-VInt `0` and **zero** path bytes; the `[value]`
  field is a normal non-empty `text`. So the fixture distinguishes an **empty PATH** from an
  **empty VALUE** (the latter would set `HAS_EMPTY_VALUE_MASK = 0x04`, `Cell.java:264`) — the two
  must not be conflated by a reader.
- The `sstabledump` golden must show `"path" : [""]` for that cell (§4). **If the golden instead
  shows `null`, `0`, or an error, the assumption chain in §3–§4 is falsified and this document is
  wrong** — that is the falsifiable check to run first, before any reader change.
- Because the empty key coexists with ordinary keys **including `0`**, the fixture simultaneously
  falsifies the two wrong reader answers: collapsing empty→`0` would produce a duplicate key, and
  dropping/nulling the empty key would produce 3 entries instead of 4.

### Caveats for whoever builds it

- **`counter` cannot be exercised this way.** A `counter` cannot be a collection element or key in
  CQL, so the `counter` row of §1's table is verified from the source only (via
  `CounterSerializer extends LongSerializer`) and is not fixture-reachable as a map key. It is in the
  question's list, so it is answered — but do not try to build a `map<counter,…>`.
- **Confirm the `blob_as_*` spelling against the container's version.** Both `blob_as_int` (modern)
  and `blobasint` (legacy) are registered at this tag
  (`BytesConversionFcts.java:99-105`, `:129-133`), but only one may be advertised by a given cqlsh.
- **`float`/`double`/`timestamp`/`timeuuid`/`uuid`/`boolean`/`inet`/`varint`/`decimal` keys are all
  equally producible** by the same route (`blobAsFloat(0x)` etc.) — 13 types measured ACCEPTED in
  §4b.1, each licensed by its own `validate` in §1. Two deserve their own column in the fixture
  because their `validate` uses a *different spelling* and so exercises a different code path:
  **`inet`** (early `return` on empty, `InetAddressSerializer.java:52-55`) and **`decimal`**
  (`!isEmpty && size < 4`, `DecimalSerializer.java:58-63`). Both are currently mis-excluded in
  committed artifacts — see §4c — so both are the highest-value columns to include.
- **DO NOT put `tinyint`, `smallint`, `date` or `time` in the fixture as empty keys — they are
  IMPOSSIBLE, measured.** CQL refuses `blobAsX(0x)` for exactly those four (§4b.1) because their
  `validate` is a bare `!= N`. They belong in a *negative* fixture or a unit test asserting refusal,
  never in a Cassandra-written corpus, and a generator that tries will fail at INSERT.
- **This fixture is Cassandra-written and must stay so.** A CQLite-written + CQLite-read round-trip
  is invariant to a uniform framing error and can never substitute for it (#3042).
