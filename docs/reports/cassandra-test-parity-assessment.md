# Cassandra Test Parity Assessment

> Foundational assessment for the CQLite ↔ Apache Cassandra byte-for-byte parity
> program (parent epic [#966](https://github.com/pmcfadin/cqlite/issues/966),
> reporting epic [#967](https://github.com/pmcfadin/cqlite/issues/967)).
>
> This document is the **canonical source** for the parity taxonomy: the
> capability groups, the public suite names, the byte-for-byte evidence bar, and
> the explicit out-of-scope boundary. The machine-readable manifest at
> `test-data/cassandra-parity-manifest.yml` is validated against the enums
> defined here, and `docs/reports/cassandra-test-parity.md` is generated from
> that manifest.

## Source corpus

- Cassandra source tree: [`apache/cassandra`](https://github.com/apache/cassandra),
  ref `cassandra-5.0.2` (git SHA `f278f6774fc76465c182041e081982105c3e7dbb`).
- Test index: [`docs/cassandra_test_index.md`](../cassandra_test_index.md) — 407
  Cassandra test files (~3,604 `@Test` methods), of which **118 are
  high-relevance** for SSTable data correctness and data-loss prevention.
- Scenario and evidence **counts** are not maintained by hand here; they are
  generated from the manifest into
  [`docs/reports/cassandra-test-parity.md`](cassandra-test-parity.md) by the
  `cassandra-parity report` subcommand.

CQLite is a single-node SSTable **reader / writer / compactor**. It does not run
as a Cassandra node, so a large fraction of the Cassandra test corpus exercises
behavior CQLite intentionally does not implement (see *What To Keep* below).

## Relevance and priority

The test index ranks each file 🔴 High / 🟡 Med / ⚪ Low for its bearing on
on-disk format correctness and data-loss prevention. Parity priority maps onto:

- **P0** — On-disk format, serialization, compression, checksums, Bloom filters,
  tombstone/TTL purging, compaction merge correctness, corruption/scrub/verify.
  A defect here is silent data loss, resurrection, or corruption.
- **P1** — Correctness-adjacent behavior where a defect produces wrong answers
  but not silent on-disk corruption.
- **P2** — Coverage breadth, tooling, and reporting.

## Recommended Grouping

To keep the manifest stable as the Cassandra corpus and CQLite test suite evolve,
parity work is organized along two fixed axes: a **capability** (what
correctness property is being proven) and a public **suite name** (where the
proving tests live). Both are closed enums — free-text values are rejected by
the manifest linter.

### Capability groups

The 16 canonical capability groups. Every manifest scenario's `capability` field
MUST be exactly one of these:

| Capability | Covers |
|---|---|
| `sstable_format` | Descriptor/version parsing, TOC component manifest, component naming/completeness |
| `component_discovery` | Locating and grouping SSTable component files on disk |
| `data_db_decode` | Data.db row/cell framing, flags, VInts, clustering bounds |
| `index_summary` | Index.db partition offsets, Summary.db boundaries (BIG format) |
| `statistics_metadata` | Statistics.db, serialization header, min/max, EstimatedHistogram |
| `compression_checksum` | CompressionInfo.db chunk offsets, CRC, inline checksum trailers |
| `corruption_verify` | Corruption detection, scrub, verify, Digest.crc32 |
| `filter_db_bloom` | Filter.db Bloom filter serialization and false-negative safety |
| `cql_types` | CQL type system decode/encode parity |
| `schema_evolution` | Serialization-header / schema column ordering and evolution |
| `tombstone_ttl` | Range/cell/row tombstones, TTL, local deletion time, purge boundaries |
| `delta_scan` | CDC-style delta-record extraction (tombstone/TTL/liveness facts) |
| `compaction_merge` | Compaction merge correctness, tombstone/TTL shadowing, purge |
| `write_load_path` | Cassandra-readable SSTable writer output (sstableloader/refresh) |
| `bti_big_version_matrix` | BIG (`nb`/`oa`) and BTI (`da`) version coverage matrix |
| `cli_reporting` | CLI/report tooling and manifest reporting itself |

### Suite names

The 13 stable public suite names. Public-facing suite organization MUST use one
of these (the manifest `cqlite.coverage.suite` and report grouping). They are
stable identifiers, **not** issue-number test file names:

- `sstable_parity_data_db_jsonl`
- `sstable_parity_delta_scan`
- `sstable_parity_statistics_db`
- `sstable_parity_index_db_big`
- `sstable_parity_summary_db_big`
- `sstable_parity_bti_partitions_rows`
- `sstable_parity_filter_db_bloom`
- `sstable_parity_compression_info_chunks`
- `sstable_parity_corruption_verify`
- `sstable_parity_component_manifest`
- `sstable_writer_cassandra_fixture_parity`
- `compaction_parity_tombstone_ttl`
- `schema_parity_serialization_header`

## Byte-for-Byte Testing Bar

Parity claims are graded by **evidence type**, strongest first:

- `byte_for_byte` — exact bytes, offsets, checksums, or whole component files
  match a Cassandra-produced reference. Strongest claim; requires `strict: true`.
- `canonical_semantic` — output matches a canonical reference (e.g. sstabledump
  JSONL) after a named normalization contract. Proves logical equivalence, not
  byte identity.
- `smoke` — parse/load/round-trip succeeds. Proves the artifact is well-formed,
  **not** that it is byte-identical to Cassandra.
- `partial` — some evidence exists; a named gap remains.
- `out_of_scope` — intentionally not claimed (see *What To Keep*).

### Evidence requirements

Every fixture-backed scenario's `evidence` block MUST record enough to regenerate
and re-compare the artifact against a snapshot or patched Cassandra build — not
just a release tag:

1. **Cassandra version** (`evidence.cassandra_version`, e.g. `5.0.2`).
2. **Cassandra git SHA** (`evidence.cassandra_git_sha`) and **on-disk
   storage-format version** (`evidence.storage_format_version`, e.g. `nb`, `oa`,
   `da`). SHA + format version make the byte evidence reproducible against
   snapshots or patched builds, not just a tagged release.
3. **Fixture-generation command** (`evidence.fixture_generation_command`) — the
   exact command that produced the reference fixture.

Additionally:

- `byte_for_byte` requires `strict: true`, at least one of
  bytes/offsets/checksums/component-file artifacts, a `comparison_command`,
  reference paths, and `failure_artifacts` for the diff.
- `canonical_semantic` requires a `normalization` description and JSONL
  reference paths.
- `smoke` requires `known_limitations` stating that parse/load success is not
  byte parity.
- `partial` requires `known_limitations` and a `scope.next_step`.
- `out_of_scope` MUST NOT define a `comparison_command`.

## What To Keep

CQLite mirrors the parts of Cassandra that govern **on-disk SSTable correctness**:
format, serialization, compression, checksums, Bloom filters, tombstone/TTL
semantics, compaction merge output, and corruption detection. These are the P0
capability groups above.

### Explicitly out of scope for "same tests as Cassandra"

"Out of scope does not mean unimportant." These node-level behaviors are
intentionally **not** claimed by CQLite parity, because CQLite is not a Cassandra
node. Each has a fixed `scope.out_of_scope_category` (see
[`docs/development/cassandra-parity-manifest.md`](../development/cassandra-parity-manifest.md)):

- **Cassandra commitlog and replay compatibility** (`commitlog_replay`).
- **Repair coordinator, read-repair coordinator, anti-entropy protocol**
  (`repair_coordinator`, `read_repair_coordinator`).
- **SSTable streaming protocol and node lifecycle**
  (`streaming_protocol`, `node_lifecycle`).
- **nodetool metrics, JMX, scheduling, operational controls**
  (`nodetool_jmx_metrics`).
- **Paxos/Accord serialization and distributed consensus**
  (`distributed_consensus`).
- **SAI/SASI behavior** unless CQLite implements those indexes (`sai_sasi_query`).
- **Memtable internals** except the generated SSTable flush artifacts
  (`memtable_internals`).
- **Java tooling / nodetool surface** that CQLite does not reimplement
  (`java_tooling`).
- **Compression-dictionary** features CQLite does not support
  (`unsupported_compression_dictionary`).
- Anything that is **not the SSTable reader/writer/compactor**
  (`not_sstable_reader_writer_compactor`).

High-relevance Cassandra files may only be marked out-of-scope with an explicit
`scope.cqlite_boundary` explaining why CQLite does not implement that behavior.

### Convention: "out of PARITY scope, but a CQLite-native surface" (issue #1403)

`out_of_scope` means only that a scenario is **not a Cassandra byte/semantic
parity target** — it does **not** license the false claim that CQLite has no such
behavior at all. Some Cassandra node behaviors have a **functional analogue** in
CQLite's own code (its WAL, its memtable, its crash-mid-compaction cleanup). For
those, "CQLite does not implement X" is wrong and misleading.

When a functional analogue exists, the `out_of_scope` boundary text MUST:

1. State that CQLite makes **no Cassandra parity claim** for that behavior
   (that is what keeps it out of parity scope), and
2. **Name the CQLite-native analogue** (with its source path), and
3. **Link the OPEN native (non-parity) coverage tracker issue** — in the prose
   fields (`rationale` / `cqlite_boundary` / `safe_claim`) and, structurally, in
   `scope.next_step`.

The analogue's correctness is proven by **native tests tracked on those issues**,
not by any Cassandra-parity scenario. Never write "CQLite does not maintain a
memtable / does not have a commit log / does not implement X" when the code says
otherwise.

#### Per-category audit sweep (issue #1403, AC3)

Every `out_of_scope` category was swept for a functional CQLite analogue. Verdict:

| Category | Functional CQLite analogue? | Native tracker |
|---|---|---|
| `commitlog_replay` | **Yes** — write-ahead log (`write_engine/wal.rs`), replay-after-crash | #1390, #1391, #1394 |
| `memtable_internals` | **Yes** — memtable (`write_engine/memtable.rs`), token-order iteration + estimate accounting | #1404 |
| `node_lifecycle` (early-open) | **Yes** — crash-mid-compaction orphan sweeps (`write_engine/maintenance.rs`) | #1393 |
| `repair_coordinator` | No — no peers, no Merkle exchange, no anti-compaction lifecycle | — (clean) |
| `read_repair_coordinator` | No — no coordinator, no digest-mismatch resolution, no cross-replica path | — (clean) |
| `streaming_protocol` | No — no node-to-node stream wire protocol or join/leave transitions | — (clean) |
| `distributed_consensus` | No — no Paxos/Accord participants or consensus rounds | — (clean) |
| `nodetool_jmx_metrics` | No — no JMX, live metrics registry, or operational control surface | — (clean) |
| `sai_sasi_query` | No — CQLite reads base-table components only; no SAI/SASI index engine | — (clean) |
| `java_tooling` | No — CQLite does not reimplement scrub/upgrader JVM tools | — (clean) |
| `unsupported_compression_dictionary` | No — feature CQLite does not support | — (clean) |
| `not_sstable_reader_writer_compactor` | No — by definition outside the reader/writer/compactor | — (clean) |

The three "Yes" categories were re-scoped by #1403 (boundary text corrected, native
trackers linked). The remaining nine categories were **audited clean**: their
Cassandra behavior has no CQLite-native analogue, so "CQLite does not implement X"
is accurate for them and no re-scope is required.

## P0 areas to classify

Drawn from the high-relevance list in the test index. Each must have at least one
manifest scenario:

| Area | Representative Cassandra tests | Capability |
|---|---|---|
| Descriptor / component resolution | `DescriptorTest`, `TOCComponentTest` | `sstable_format` |
| SSTable metadata / row index | `SSTableMetadataTest`, `RowIndexEntryTest` | `sstable_format`, `index_summary` |
| Reader / scanner | `SSTableReaderTest`, `SSTableScannerTest`, `SSTableSkippingReadTest` | `data_db_decode` |
| Serialization | `UnfilteredSerializerTest`, `SerializationHeaderTest`, `SerializationMirrorTest` | `data_db_decode`, `statistics_metadata`, `schema_evolution` |
| Writer fixtures | `CQLSSTableWriterTest` | `write_load_path` |
| Compression / checksum | `CompressionMetadataTest`, `CompressedRandomAccessReaderTest`, `ChecksumedDataTest`, `ChecksummedRandomAccessReaderTest`, `ChecksummedSequentialWriterTest` | `compression_checksum` |
| Corruption / scrub / verify | `VerifyTest`, `ScrubTest`, corruption-recovery tests | `corruption_verify` |
| Bloom filter | `BloomFilterTest`, `LongBloomFilterTest` | `filter_db_bloom` |
| Tombstone / TTL | `RangeTombstoneTest`, `RangeTombstoneListTest`, `RangeTombstoneBoundaryTest`, `NeverPurgeTest`, `TTLExpiryTest` | `tombstone_ttl` |
| Compaction merge | `CompactionsPurgeTest`, `GcCompactionTest`, `CompactionIteratorTest`, representative high-relevance compaction tests | `compaction_merge` |
| Delta scan | (CQLite-native; backed by `scan_delta_parity_test.rs`) | `delta_scan` |
| BTI / version matrix | `da`/BTI write+read; `nb`/`oa` BIG | `bti_big_version_matrix` |

## Safe vs unsafe public claim language

- **Safe**: "CQLite reads and writes Cassandra 5.0 SSTables and is validated for
  canonical-semantic equivalence against `sstabledump` for the covered dataset,
  with byte-for-byte parity proven where the manifest records `byte_for_byte`
  evidence."
- **Unsafe**: "CQLite passes the same tests as Cassandra" or "CQLite is
  byte-for-byte identical to Cassandra" — these overclaim node behavior and byte
  parity the manifest does not support.

## See also

- [`docs/cassandra_test_index.md`](../cassandra_test_index.md)
- [`docs/development/cassandra-parity-manifest.md`](../development/cassandra-parity-manifest.md)
- Generated report: [`docs/reports/cassandra-test-parity.md`](./cassandra-test-parity.md)
