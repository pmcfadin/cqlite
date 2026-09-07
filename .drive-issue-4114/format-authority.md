# cassandra-5.0.8 format authority for vector<float,n> — issue #4114
Derived by subagent `authority-4114` from the PINNED tag via `gh api ...?ref=cassandra-5.0.8`.
NO local clone on this box; no working tree was read (#3041).

## Wire format
Serializer is chosen FROM THE ELEMENT TYPE — VectorType.java:86-101:
    this.valueLengthIfFixed = elementType.isValueLengthFixed() ?
        elementType.valueLengthIfFixed() * dimension : super.valueLengthIfFixed();
    this.serializer = elementType.isValueLengthFixed() ? new FixedLengthSerializer() : new VariableLengthSerializer();

- FIXED-width element  => NO per-element prefix. FixedLengthSerializer.split (VectorType.java:445-460)
  slices `offset += elementLength`, reads no prefix, then checkConsumedFully.
- VARIABLE-width element => per-element UNSIGNED-VINT length prefix (VectorType.java:563-570).
- NO element count in the value. Loops are `for (i = 0; i < dimension; i++)`; n comes ONLY from the type.
- Endianness: FloatType.valueLengthIfFixed()==4 (FloatType.java:148-152);
  ByteBufferUtil.java:512-515 `bytes.getFloat(bytes.position())`, Java ByteBuffer defaults BIG_ENDIAN
  => big-endian IEEE-754 binary32.

### vector<float,3> == EXACTLY 12 bytes
    [f0: b0 b1 b2 b3][f1: b0 b1 b2 b3][f2: b0 b1 b2 b3]
No outer length, no element count, no per-element prefix, no terminator.
[1.0, 2.0, 3.0] == 3F 80 00 00  40 00 00 00  40 40 00 00

### Cell / clustering framing (load-bearing)
Cell.java:304,333 and ClusteringPrefix.java:473,536 delegate to AbstractType.writeValue/skipValue,
which branch PURELY on valueLengthIfFixed() — AbstractType.java:535-552:
    int expectedValueLength = valueLengthIfFixed();
    if (expectedValueLength >= 0) accessor.write(value, out);   // raw, NO vint prefix
    else accessor.writeWithVIntLength(value, out);
AbstractType.java:603-610 skipValue: `if (length >= 0) in.skipBytesFully(length); else skipWithVIntLength(in);`

## Fixed vs variable — DEFINITIVE
VectorType.valueLengthIfFixed() returns cached 4*n for a fixed element (VectorType.java:126-131 + :94-96);
a variable element falls through to AbstractType VARIABLE_LENGTH == -1 (AbstractType.java:62, 490-493).
isValueLengthFixed() is final: `valueLengthIfFixed() != VARIABLE_LENGTH` (AbstractType.java:500-502).
  vector<float,3> => FIXED, 12 bytes.
  vector<text,3>  => VARIABLE (-1): outer vint length AND a per-element vint each.
  GENERAL RULE: fixed IFF the element type is fixed; width = elem_width * n.

=> cqlite-core/src/parser/repair_clustering.rs:135 listing "VectorType" as variable-width is a BUG
   for fixed-element vectors: CQLite reads a phantom vint prefix Cassandra never wrote.
   It CANNOT be decided from the constructor name alone (repair_clustering.rs:88-89 splits at the
   first '(' and package-strips, DISCARDING the args) — the element type must be parsed and multiplied
   by n. repair_clustering.rs:65-78's own AUTHORITY NOTE already states the rule branches "PURELY on
   valueLengthIfFixed()", which is exactly what :135 violates. `Variable` stays correct for
   variable-element vectors only.

## EXACT AbstractType class-name string (NOTE THE SPACES AROUND THE COMMA)
TypeParser.java:239-242:  return "(" + type.toString(ignoreFreezing) + " , " + dimension + ")";
VectorType.java:339-342:  getClass().getName() + TypeParser.stringifyVectorParameters(...)
=>  org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)
Parse-back TypeParser.getVectorParameters (TypeParser.java:244-263), order (type, dimension):
    parse() -> skipBlankAndComma() -> Integer.parseInt(readNextIdentifier()) -> expect ')'
VectorType.getInstance(TypeParser) (:109-113) FREEZES the element: getInstance(v.type.freeze(), v.dimension).

## CQL3 surface
Spelling `vector<float, 3>` (CQL3Type.java:589, :938).
Only hard type-level constraint is n > 0 — VectorType.java:89-90:
    if (dimension <= 0) throw new InvalidRequestException("vectors may only have positive dimensions; given %d")
NO upper bound in the type. RawVector.validate (CQL3Type.java:921-926) applies only the SOFT
Guardrails.vectorDimensions, DISABLED BY DEFAULT (Config.java:924-925, both thresholds -1).
NO element-type restriction at 5.0.8 — RawVector.prepare (CQL3Type.java:928-933) prepares any
CQL3Type.Raw; the fixed/variable serializer split exists precisely to support variable-width elements.
(Float-only is an SAI/ANN INDEX restriction, not a type one.)
RawVector is super(true) (frozen), supportsFreezing()==true, freeze() returns this.

## Null / empty
NULL: YES. VectorType.java:409-414 `// we don't allow empty vectors, so we can just check for null`
      -> `return buffer == null`; deserialize returns null.
EMPTY: NO. n=0 rejected at construction (:89-90). A zero-length value THROWS —
      validate: `if (accessor.isEmpty(input)) rejectNullOrEmptyValue()` (:515-517, :653-655);
      rejectNullOrEmptyValue (:365-368) throws MarshalException("Invalid empty vector value").
      Trailing bytes also throw — checkConsumedFully (:358-363):
      "Unexpected N extraneous bytes after ... value".
=> CQLite MUST treat a 0-length fixed-vector value as an ERROR, never as an empty vector.

## Comparator / primary key
super(ComparisonType.CUSTOM) (VectorType.java:88); compareCustom delegates to the serializer (:122-125).
Fixed path short-circuits to raw byte compare when elementType.isByteOrderComparable, else element-wise
(:414-441); variable path walks vints (:541-560). asComparableBytes/fromComparableBytes (:205-230)
implemented => works in BTI. Nothing in CreateTableStatement.java restricts vectors from a primary key,
and vectors are always frozen => A VECTOR CAN BE A PARTITION OR CLUSTERING KEY AT 5.0.8.
That is exactly why the repair_clustering.rs:135 bug matters.

## CQLite today (read-only survey)
Enums: cqlite-core/src/schema/mod.rs:259-295 `CqlType` — List(Box), Map(Box,Box), Tuple(Vec),
  Udt(String,Vec), Frozen(Box), Custom(String). `Vector(Box<CqlType>, usize)` fits but would be the
  ONLY variant carrying a non-type param. Flat `DataType` also at cqlite-core/src/types.rs:1502-1541.
  CqlTypeId (parser/types/mod.rs:52-80) needs nothing — vectors have no native ID, they ride as Custom.
THREE independent marshal-string parsers, NONE handles a numeric arg:
  1. .../row_decoder/udt/type_string.rs:202 parse_cassandra_type_with_depth — has a reusable
     depth-aware split_type_args (:156), but recursing into "3" yields
     Custom("org.apache.cassandra.db.marshal.3"). Today VectorType(...) falls to Custom, refused by name.
  2. parser/enhanced_statistics_parser/marshal_type.rs:100 convert_marshal_type_to_cql — fallback
     `other => other.to_lowercase()` (:214) SILENTLY degrades a vector to a junk string, NO error.
  3. .../row_decoder/raw_value.rs:79 primitive_marshal_to_cql_short — scalar allowlist.
  Writer side: .../writer/stats_writer/marshal.rs:15. CQL-text: schema/cql_type_parser.rs:69 (no `vector<` arm).
repair_clustering.rs:79-140 resolve_clustering_value_layout; `fixed` map (:92-99) has
  "Int32Type" | "FloatType" => Some(4); `variable` list (:111-136) includes "VectorType" at :135.
commitlog/schema.rs:221-242 fn simple_scalar_type_allowlist_excludes_complex_types() asserts
  is_simple_scalar_type is false for "vector<float, 4>" at :234.
EXISTING SURFACE = 5 sites, all refusal/classification.
SECOND PINNED TEST THAT WILL FLIP: .../udt/regression_3631_marshal_field_types_tests.rs:262-273
  requires VectorType(FloatType, 3) over 12 bytes to ERROR naming "VectorType".
No real vector fixture exists — tests/src/cql_test_data_fixtures.rs:780-786 FAKES them as LIST<DOUBLE>.

## NOT settled by source — carry forward
1. No Cassandra-written vector<float,n> fixture in this repo; the fake LIST<DOUBLE> cannot serve (#3042).
2. The " , " spacing is what Cassandra WRITES; skipBlankAndComma tolerates variation on READ, but what
   other writers emit was not verified => a CQLite parser should tolerate optional whitespace around the
   comma rather than match the exact literal.
3. Unverified whether CQLite's Statistics.db/serialization-header path preserves the raw type string
   losslessly enough for marshal_type.rs to see the " , 3" at all — check before sizing that arm.

## ADDENDUM — layout VERIFIED against the committed Cassandra-5.0.8-written bytes
Checked by the lead directly (not reported by a subagent), on
test-data/fixtures/issue_4114/test_vector/vector_clustered-*/nb-1-big-Data.db.
Row `ck=10` holds v3 = [1.0, 2.5, -3.75] and z_after = "ck-after-10".

Data.db bytes 20..46:
    24 07 00 f1 0f 0a 1c 12 00 08 3f 80 00 00 40 20 00 00 c0 70 00 00 08 0b 63 6b
                                  ^^ ^^^^^^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^ ^^ ^^ ^^
                                  |  1.0         2.5         -3.75       |  |  "ck"...
                                  |                                      |  vint len 11
                                  cell flags 0x08                        cell flags 0x08

The 12-byte big-endian binary32 payload 3f800000 40200000 c0700000 appears RAW at offset 30
(LZ4 keeps incompressible float data as literals, so it survives verbatim despite CompressionInfo.db).
The byte immediately preceding it is 0x08 — the CELL FLAGS byte — and NOT 0x0c, i.e. NOT a vint
length of 12. Meanwhile the very next cell in the SAME row, a variable-width `text`, is framed
`08 0b` = flags + vint length 11 before "ck-after-11 chars".

=> CONFIRMED from Cassandra-written bytes, with fixed and variable framing side by side in one row:
   vector<float,3> carries NO value-length prefix; a text column does.
   This is the AC5 evidence. A reader that treats VectorType as variable-width (today's
   repair_clustering.rs:135) would consume 0x3f == 63 as a vint length and read 63 bytes for a
   12-byte value — an unrecoverable desync, not a bad value.

CAVEAT, carry forward: repair_clustering governs CLUSTERING values, and in THIS fixture the vector is
a REGULAR column. The same AbstractType.writeValue/skipValue rule governs both paths
(ClusteringPrefix.java:473,536), so the source argument transfers — but the BYTES above demonstrate the
regular-cell path. Worth establishing whether Cassandra 5.0.8 actually accepts a vector as a clustering
key (source says nothing forbids it, and vectors are always frozen); if it does, generate that fixture
too so the clustering path has Cassandra-written bytes of its own rather than a transferred argument.
