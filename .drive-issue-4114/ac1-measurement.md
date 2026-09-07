# AC1 — how a vector<float,n> column behaves on today's tree. MEASURED.
Measured by the lead on the committed Cassandra-5.0.8-written fixture
test-data/fixtures/issue_4114 (commit 52b1288b1), with cqlite-cli v0.16.1 built from
this branch at 390450c95. Every figure below is reproducible with the commands shown.

## VERDICT: it does NOT reliably fail closed. It SILENTLY MIS-DECODES, and whether the
## user sees an error at all is DATA-DEPENDENT, not a designed refusal.

## Surface 1 — schema SUPPLIED: fails closed, but at the CQL PARSER, before any read
    ./target/debug/cqlite --schema test-data/schemas/issue-4114-vector-float.cql \
      --data-dir test-data/fixtures/issue_4114 \
      --query "SELECT * FROM test_vector.vector_clustered" --out json
    -> exit 3
    Error: Schema loading failed: InvalidCql: Failed to parse CREATE TABLE ...
           Error { input: "<float, 3>, ...", code: Char }
`schema/cql_type_parser.rs` has no `vector<` arm, so the CREATE TABLE cannot be parsed at
all. Honest and fail-closed — but it means the schema-aware path never reaches the decoder,
so it tells us NOTHING about the read path. The interesting measurement is surface 3.

## Surface 2 — query path with no schema: fails closed for an unrelated reason
    ./target/debug/cqlite --data-dir test-data/fixtures/issue_4114 \
      --query "SELECT * FROM test_vector.vector_clustered" --out json
    -> exit 3   Error: Schema not found for table 'vector_clustered'
The query path REQUIRES a schema, so it also never reaches the decoder. Not evidence.

## Surface 3 — `read-sstable` (the schema-BYPASSING, Statistics.db-driven path). THE MEASUREMENT.

### 3a. vector_clustered — v3 = [1.0, 2.5, -3.75], 12 bytes
    ./target/debug/cqlite read-sstable \
      test-data/fixtures/issue_4114/.../vector_clustered-*/nb-1-big-Data.db --format json
    -> exit 5
    ERROR ...row_decoder::block_emit_windowed: Partition 0 - Failed to parse first row at
      offset 18: column 'v3' (column type floattype , 3) failed to decode at byte offset 27
      of the row: Data corruption: Cell 'v3': need 63 bytes for blob, only 24 available

TWO defects visible in that one line:
  (i)  `column type floattype , 3` — the marshal type
       `org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)`
       was degraded to the junk string `floattype , 3` with NO error, by
       parser/enhanced_statistics_parser/marshal_type.rs:214 `other => other.to_lowercase()`.
  (ii) `need 63 bytes for blob` — 63 == 0x3f == the FIRST BYTE of 1.0's big-endian binary32
       (3f 80 00 00). The 12 raw payload bytes are being read as a VINT-LENGTH-PREFIXED BLOB.
       Cassandra wrote NO length prefix (fixed-width), so the first float's high byte is
       consumed as a length. 63 > 24 remaining ⇒ error. It errored BY ARITHMETIC ACCIDENT.

### 3b. vector_pk_only — v1 = [1.5] (4 bytes), then v384 (1536 bytes)
    -> exit 5
    ERROR ... column 'v384' (column type floattype , 384) failed to decode at byte offset 98
      of the row: Data corruption: Cell 'v384': need 16640 bytes for blob, only 1479 available

NOTE WHICH COLUMN IS NAMED. Regular columns serialize in name order (a_before < v1 < v384 <
z_after), so v1 was decoded FIRST — and it raised NO ERROR. v1 = [1.5] = 3f c0 00 00, so its
bogus vint length is again 0x3f = 63, but here ~1542 row bytes remained, so 63 <= remaining
and the read SUCCEEDED: v1 was returned as a 63-BYTE BLOB — its own 4 bytes plus 59 bytes
STOLEN FROM v384 — instead of the 1-element vector [1.5]. No error, no warning.
The arithmetic confirms it: v1's payload begins at row byte offset 35, and the next column
v384 is reported failing at row byte offset 98 == 35 + 63. The 63-byte over-read is exactly
the displacement.

## MECHANISM (why fail-closed is an accident, not a design)
row_decoder/cell_value.rs:310-353 `read_vint_length_prefixed_bytes` parses a vint length and
slices that many bytes. Its ONLY rejection is:
    if len > data.len().saturating_sub(*offset) { return Err(corruption(...)) }
So the outcome is decided purely by `len > remaining`:
  - len > remaining  -> "Data corruption" error (3a, and 3b's later cascade)
  - len <= remaining -> WRONG BYTES RETURNED SILENTLY, offset advanced (3b's v1)
`len` is attacker-/data-controlled: it is the leading byte(s) of the first float element. A
vector whose first element has a small leading byte therefore yields a satisfiable length and
a silent wrong value. First element 0.0 (00 00 00 00) gives length 0 — an empty blob.

## SEVERITY: this is a correctness defect, not a missing feature
Issue #4114 anticipated "refusing an untyped column is correct behaviour and the gap is merely
'unsupported'". That is NOT what happens. Nothing refuses the type: marshal_type.rs:214
accepts it by lowercasing it, and the value is then misframed as a length-prefixed blob. The
error a user sometimes sees says "Data corruption" and blames the DATA, when the data is
byte-correct Cassandra output and the reader is wrong.
Also: the error is raised for the WRONG COLUMN. In 3b the reader blames v384 for a defect
whose cause is v1, which had already silently mis-decoded — so even the diagnostic misdirects.
Consistent with CLAUDE.md's fifth blind spot (#3890), a mis-framed column corrupts every
LATER column in the row, not just its own.

## Reproduce
    cargo build -p cqlite-cli
    ./target/debug/cqlite read-sstable \
      test-data/fixtures/issue_4114/test_vector/vector_clustered-*/nb-1-big-Data.db --format json
