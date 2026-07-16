# runtime-decode-policy

## ADDED Requirements

### Requirement: Safe is the default decode policy on every public entry
`DecodePolicy::Safe` SHALL be the default decode policy at every public entry point, so that a reader,
chunk decompressor, or flight server constructed without an explicit policy performs bounds-checked
(`lz4_flex` `safe-decode`) LZ4 decode with zero behaviour change versus today. `DecodePolicy` SHALL
derive `Default = Safe`, and no code path SHALL reach `FastUnsafe` without an explicit caller choice.

#### Scenario: A reader constructed without a policy decodes via the checked path

- **GIVEN** a compressed (LZ4) SSTable opened through the ordinary public constructor with no decode
  policy specified
- **WHEN** a chunk is decompressed
- **THEN** the checked `lz4_flex` decode is used
- **AND** the decoded bytes are byte-identical to the current (pre-change) output and the JSONL parity
  golden.

#### Scenario: The flight server defaults to Safe

- **WHEN** `cqlite-flight` starts with no `--decode-policy` flag and no `CQLITE_DECODE_POLICY` env var
- **THEN** the active policy is `Safe`
- **AND** the startup log records `decode_policy = safe`.

### Requirement: FastUnsafe is reachable only via an explicit, unsafe-marked, documented path
`FastUnsafe` SHALL be selectable only through an explicit constructor that is `unsafe` (or an
explicitly unsafe-named builder) carrying a `# Safety` doc comment stating the trusted-input
precondition. It SHALL NOT be the `Default`, SHALL NOT be produced by `From`/parse of ordinary
(non-affirmed) input, and SHALL NEVER be inferred from byte patterns, CRC state, or file contents
(no-heuristics mandate, issue #28). Selecting it is always a deliberate operator/programmer act.

#### Scenario: There is no implicit route to FastUnsafe

- **WHEN** the public API surface for constructing readers/decompressors is exercised without calling
  the unsafe constructor
- **THEN** the resolved policy is `Safe`
- **AND** no configuration string, CRC outcome, or byte pattern maps to `FastUnsafe`.

#### Scenario: The unsafe constructor documents its safety contract

- **WHEN** the `FastUnsafe` constructor is inspected
- **THEN** it is marked `unsafe` (or unmistakably unsafe-named) and carries a `# Safety` section that
  states FastUnsafe is sound only for trusted, intact files and is undefined behaviour on
  adversarial/corrupt input.

### Requirement: FastUnsafe is scoped to the CRC-preceded LZ4 chunk decode path
`FastUnsafe` SHALL take effect only at the chunk-decompressor LZ4 decode site where the inline CRC32
over the compressed bytes is verified before decode, and the CRC-before-decompress ordering SHALL be
preserved unchanged. The Snappy, Deflate, and Zstd chunk paths, and the non-CRC small-block
`Compression::decompress` LZ4 path, SHALL ignore the policy and always decode via their checked path.

#### Scenario: CRC is still verified before decode under FastUnsafe

- **GIVEN** a chunk decompressor configured with `FastUnsafe`
- **WHEN** a chunk record is read
- **THEN** the stored inline CRC32 is compared against the CRC of the compressed bytes BEFORE any LZ4
  decode is attempted (the existing `read_compressed_chunk_at_verifies_crc_before_returning`
  ordering holds)
- **AND** a CRC mismatch returns the typed error without invoking the unchecked decoder.

#### Scenario: Non-LZ4 and small-block paths ignore the policy

- **GIVEN** `FastUnsafe` is configured
- **WHEN** a Snappy/Deflate/Zstd chunk, or the small-block `Compression::decompress` LZ4 path, is
  decoded
- **THEN** the checked decoder is used regardless of the policy.

### Requirement: The safety boundary is documented honestly and the flight default stays Safe
The change SHALL document, at the extraction site and in user-facing flight help, that `FastUnsafe`
assumes trusted, intact on-disk data and that on adversarial/corrupt input the removed bounds checks
make behaviour undefined (possible out-of-bounds read). The flight server SHALL keep `Safe` as its
default and SHALL require an explicit trusted-files affirmation flag in addition to
`--decode-policy fast-unsafe` before enabling the unchecked path, logging the active policy at startup.
The project SHALL NOT claim UB-freedom for `FastUnsafe` on arbitrary input; the claim is limited to
"Safe is UB-free on any input; FastUnsafe is UB-free only on trusted, intact input."

#### Scenario: Enabling FastUnsafe on the flight server requires an explicit trust affirmation

- **WHEN** `cqlite-flight` is started with `--decode-policy fast-unsafe` but WITHOUT the trusted-files
  affirmation flag
- **THEN** startup fails (or refuses to enable FastUnsafe and stays Safe) with a message naming the
  required affirmation flag
- **AND** when both are supplied, the startup log records `decode_policy = fast-unsafe` and a
  trusted-files warning.

#### Scenario: The trusted-only boundary is documented

- **WHEN** the rustdoc for the `FastUnsafe` constructor and the flight `--decode-policy` help are read
- **THEN** both state the trusted-files-only precondition and the undefined-behaviour-on-corrupt-input
  consequence.

### Requirement: No non-default or minimal build becomes silently unsafe
`lz4_flex` `safe-decode` SHALL remain compiled in every build, and `FastUnsafe` SHALL NOT be selected
by any cargo feature — including the minimal-features build and the default build. Any second unchecked
decoder adopted for the fast path SHALL be compiled unconditionally, so that the Safe/FastUnsafe choice
is purely a runtime decision and no feature-flag combination silently disables bounds-checked decode.

#### Scenario: Minimal and default builds decode via the checked path

- **WHEN** the minimal-features build and the default build each decompress an LZ4 chunk without
  calling the unsafe constructor
- **THEN** both use the checked `lz4_flex` decode
- **AND** no feature-flag combination causes decode to become unchecked without an explicit runtime
  `FastUnsafe` call.

### Requirement: FastUnsafe is built only after a measured Linux win on the real corpus
The `FastUnsafe` decode path SHALL be implemented only after a committed before/after benchmark on the
real SSTable corpus (a present compressed fixture, run on Linux) demonstrates a material end-to-end
scan-throughput win attributable to removing LZ4 bounds checks. The benchmark SHALL report BOTH the
decompress-only delta AND the end-to-end scan delta. If no material end-to-end win is demonstrated, the
`FastUnsafe` path SHALL NOT be built and issue #2211 SHALL be closed as not-worth-it. A synthetic
micro-benchmark alone SHALL NOT satisfy this requirement.

#### Scenario: The measurement gate produces a recorded corpus benchmark

- **WHEN** the FastUnsafe path is proposed for merge
- **THEN** a committed benchmark artifact on a present real compressed fixture records both the
  decompress-only and the end-to-end scan throughput for Safe vs FastUnsafe on Linux
- **AND** the merge proceeds only if the end-to-end win meets the owner-set threshold; otherwise the
  path is not built and the issue is closed.

### Requirement: FastUnsafe correctness is proven by a differential fuzz/parity against Safe
FastUnsafe SHALL be covered by a differential target/test that generates VALID LZ4 chunks (compress
arbitrary input, prepend the length, append the CRC) and asserts that `FastUnsafe` output is
byte-identical to `Safe` output — because feeding arbitrary bytes to an unchecked decoder produces
out-of-bounds reads by contract, so equivalence-on-valid-input, not robustness-on-arbitrary-input, is
the property to fuzz. The arbitrary-bytes fuzz targets SHALL continue to exercise only the Safe path
(preserving the no panic/hang/OOM guarantee on arbitrary input), and the out-of-contract nature of
arbitrary-bytes-into-FastUnsafe SHALL be documented in the fuzz README and the `# Safety` doc.

#### Scenario: FastUnsafe equals Safe on valid chunks

- **GIVEN** an arbitrary input compressed into a valid Cassandra-format LZ4 chunk (length prefix + CRC)
- **WHEN** it is decoded once under `Safe` and once under `FastUnsafe`
- **THEN** the two decoded outputs are byte-identical
- **AND** the differential harness runs over the fuzz corpus without a divergence.

#### Scenario: Arbitrary-bytes fuzz targets stay on the Safe path

- **WHEN** the arbitrary-bytes `fuzz_*` targets run
- **THEN** they decode via the Safe path only
- **AND** the fuzz README documents that arbitrary-bytes-into-FastUnsafe is out of contract by design.
