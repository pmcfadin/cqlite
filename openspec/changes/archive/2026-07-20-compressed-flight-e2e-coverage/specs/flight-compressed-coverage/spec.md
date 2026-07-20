# flight-compressed-coverage

## ADDED Requirements

### Requirement: Compressed BIG-nb tables are exercised end-to-end through Flight do_get over real transport, per codec

The in-repo test suite SHALL exercise the compressed (chunk-stitching) BIG-`nb` read path end-to-end
through `FlightService::do_get` over **real gRPC transport** (a bound socket, a tonic server, and a
`FlightServiceClient`), against the real `test_comp` corpus — not an in-process service call and not a
WriteEngine-produced (uncompressed) fixture. Coverage SHALL include one case per compression codec
present in the corpus (LZ4, Snappy, Deflate, Zstd) and SHALL include the two chunk-boundary edge
tables (`short_final_chunk`, `incompressible_uncompressed_chunk`). For every case the returned rows
SHALL match the committed `nb-1-big-Data.db.jsonl` golden in both row count and `(pk, ck, body)`
values. A returned row count of zero when the fixture is present SHALL be a test FAILURE, never a
pass and never a skip.

#### Scenario: Per-codec full scan over transport matches the golden

- **GIVEN** the fetched `test_comp` corpus and a Flight service serving it
- **AND** a codec table among `lz4_table`, `snappy_table`, `deflate_table`, `zstd_table`
- **WHEN** a full-scan ticket is served through `do_get` over a real gRPC connection
- **THEN** the decoded record batches contain exactly the golden's row count for that table
- **AND** the `(pk, ck, body)` values equal the golden's
- **AND** a present-but-empty result (zero rows) fails the test.

#### Scenario: Chunk-boundary edge tables scan correctly

- **GIVEN** `test_comp.short_final_chunk` (LZ4, 4 KB chunks, final chunk shorter than 4096) and
  `test_comp.incompressible_uncompressed_chunk` (LZ4 with `min_compress_ratio=1.0`, so chunks fall
  back to raw)
- **WHEN** each is fully scanned through `do_get` over transport
- **THEN** the rows match that table's golden exactly — the short final chunk and the raw-fallback
  chunk are both stitched without truncation or corruption.

#### Scenario: LIMIT-k bounds the result on the compressed path

- **GIVEN** a compressed codec table whose golden row count exceeds k
- **WHEN** a ticket carrying LIMIT k is served through `do_get` over transport
- **THEN** exactly k rows are returned, and each is a prefix-consistent member of the golden.

#### Scenario: Midstream client drop releases the producer on the stitching path

- **GIVEN** a compressed table (LZ4, the field default) served with a batch size small enough to park
  the producer in a blocking send under backpressure
- **WHEN** the client reads a few batches and then drops the stream and channel midstream
- **THEN** the server-side producer is released and the `do_get` in-flight level settles back to its
  baseline within a bounded timeout — i.e. a mid-scan disconnect on the *stitching* path does not leak
  a parked producer.

### Requirement: Tests prove the chunk-stitching branch was actually routed

Each compressed case SHALL carry positive evidence that the chunk-stitching/decompress plane executed,
rather than inferring it from a green result. Because `requires_chunk_stitching()` is not publicly
callable, evidence SHALL be the public `SSTableReader` decompress counters: the case SHALL reset the
counter before the scan and SHALL assert a non-zero `decompress_call_count()` after it. The suite
SHALL additionally include an **uncompressed control** (`test_comp.uncompressed_table`, which has no
`CompressionInfo.db`) asserting the counter stays **zero** for the same scan shape, so a non-zero
count on a compressed case cannot be explained by unrelated decompression elsewhere in the process.
Tests reading these process-global counters SHALL be serialized against one another.

#### Scenario: Compressed scan increments the decompress counter

- **GIVEN** a compressed codec table
- **WHEN** the counter is reset and a full scan is served through `do_get`
- **THEN** `decompress_call_count()` is greater than zero after the scan.

#### Scenario: Uncompressed control leaves the decompress counter at zero

- **GIVEN** `test_comp.uncompressed_table` (no `CompressionInfo.db`)
- **WHEN** the counter is reset and the same full-scan shape is served through `do_get`
- **THEN** `decompress_call_count()` is exactly zero
- **AND** therefore the non-zero counts asserted for compressed tables are attributable to the
  compressed path.

### Requirement: The compressed corpus is present in a stock checkout and fixture gating never passes vacuously

The `test_comp` components these tests read SHALL be tracked in git (force-added past the `*.db`
ignore rule, following the precedent that tracked the `test_da` BTI binaries), so the tests execute on
a stock checkout without a dataset fetch. Fixture lookup SHALL locate a table directory by **name
prefix**, never by a hardcoded generation UUID. When a fixture is genuinely absent the test SHALL skip
with an explicit SKIP message; when `CQLITE_REQUIRE_FIXTURES=1` is set, an absent fixture SHALL be a
hard FAILURE instead of a skip.

#### Scenario: Tests run on a stock checkout

- **GIVEN** a fresh clone with no dataset fetch performed
- **WHEN** the compressed Flight coverage tests run
- **THEN** they execute against the committed corpus and assert real rows — they do not skip.

#### Scenario: Fixture lookup survives a corpus regeneration

- **GIVEN** a `test_comp` table directory whose trailing generation UUID differs from any previously
  recorded value
- **WHEN** the fixture helper resolves the table directory
- **THEN** it locates the directory by its `<table>-` name prefix and verifies the expected
  `-Data.db` component is a file.

#### Scenario: Absent fixture fails hard under CQLITE_REQUIRE_FIXTURES

- **GIVEN** the corpus is absent from the checkout
- **WHEN** the tests run with `CQLITE_REQUIRE_FIXTURES=1`
- **THEN** they FAIL with a message naming the missing fixture, rather than skipping.
