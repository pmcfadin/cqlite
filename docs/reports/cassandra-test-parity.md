# Cassandra Test Parity Report

> Generated from `test-data/cassandra-parity-manifest.yml` by `cargo run -p cassandra-parity -- report`. Do not edit by hand — edit the manifest and regenerate.

Cassandra source: [`cassandra-5.0.2`](https://github.com/apache/cassandra/tree/cassandra-5.0.2) @ `f278f6774fc76465c182041e081982105c3e7dbb` (git SHA). Program: parent epic #966, reporting epic #967.

Sources: [`docs/cassandra_test_index.md`](../../docs/cassandra_test_index.md) · [`docs/reports/cassandra-test-parity-assessment.md`](../../docs/reports/cassandra-test-parity-assessment.md)

## Status counts

| Status | Scenarios |
|---|---|
| `mirrored` | 20 |
| `partial` | 7 |
| `planned` | 2 |
| `out_of_scope` | 7 |
| **total** | **36** |

## Evidence counts

| Evidence | Scenarios |
|---|---|
| `byte_for_byte` | 6 |
| `canonical_semantic` | 8 |
| `smoke` | 5 |
| `partial` | 10 |
| `out_of_scope` | 7 |

## ⚠️ P0 scenarios with weak evidence

These P0 scenarios are backed only by `smoke` or `partial` evidence and must not be cited as proof of byte parity:

- `cass.compaction_merge.byte_for_byte_output` — Compaction byte-for-byte output parity (future) (partial)
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1) (smoke)
- `cass.compression_checksum.checksum_trailer_detection` — Inline checksum / Digest.crc32 corruption detection (partial)
- `cass.corruption_verify.component_corruption_detection` — Component corruption detection, scrub, and verify (partial)
- `cass.delta_scan.tombstone_liveness_facts` — Delta-scan tombstone/TTL/liveness fact extraction (partial)
- `cass.filter_db_bloom.serialization_no_false_negative` — Filter.db Bloom filter serialization with no false negatives (partial)
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` — BIG Index.db promoted-index (wide-partition) boundary metadata (partial)
- `cass.index_summary.summary_boundaries` — Summary.db sampling boundaries (BIG) (partial)
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution (smoke)
- `cass.sstable_format.toc_component_manifest` — TOC.txt component manifest completeness (partial)
- `cass.tombstone_ttl.range_tombstone_boundaries` — Range tombstone boundary and deletion-time parity (partial)
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader (smoke)

## P0 scenarios

| ID | Capability | Status | Evidence | Suite | Risk |
|---|---|---|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.bti_big_version_matrix.bti_da_write_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.compaction_merge.byte_for_byte_output` | compaction_merge | planned | partial | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.load_path_validity` | compaction_merge | mirrored | smoke | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction_merge.tombstone_ttl_shadowing` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compression_checksum.checksum_trailer_detection` | compression_checksum | partial | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.compression_checksum.chunk_offsets_and_crc` | compression_checksum | mirrored | canonical_semantic | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.corruption_verify.component_corruption_detection` | corruption_verify | planned | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db_decode.row_cell_flags_and_vint` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.delta_scan.tombstone_liveness_facts` | delta_scan | partial | partial | `sstable_parity_delta_scan` | p1_correctness |
| `cass.filter_db_bloom.serialization_no_false_negative` | filter_db_bloom | partial | partial | `sstable_parity_filter_db_bloom` | p0_data_loss |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p0_data_loss |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.SSTableScannerTest.range_boundaries` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.big.raw_partition_keys_and_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.bti.index_component_discovery` | index_summary | mirrored | byte_for_byte | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.index_summary.big_index_offsets` | index_summary | mirrored | canonical_semantic | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.summary_boundaries` | index_summary | partial | partial | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.sstable_format.descriptor_component_resolution` | sstable_format | mirrored | smoke | `sstable_parity_component_manifest` | p1_correctness |
| `cass.sstable_format.toc_component_manifest` | sstable_format | mirrored | partial | `sstable_parity_component_manifest` | p1_correctness |
| `cass.statistics_metadata.serialization_header` | statistics_metadata | mirrored | canonical_semantic | `sstable_parity_statistics_db` | p1_correctness |
| `cass.tombstone_ttl.range_tombstone_boundaries` | tombstone_ttl | partial | partial | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p0_data_loss |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | write_load_path | mirrored | smoke | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |

## Byte-for-byte scenarios

- `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` — Truncated BIG Index.db fails explicitly
- `cass.index_db.RowIndexEntryTest.partition_offsets` — BIG Index.db partition data offsets vs Cassandra positions
  - Normalization: Index.db data offsets are relative to the Data.db data section while JSONL positions are absolute file offsets; the per-partition Data.db header is a constant only for uniform partition-key lengths without static blocks, so successive deltas (not absolute values) are compared in that case.
- `cass.index_db.SSTableReaderTest.point_lookup_offsets` — BIG Index.db point/absent-key lookup offsets
- `cass.index_db.SSTableScannerTest.range_boundaries` — BIG Index.db key order and range boundaries
- `cass.index_db.big.raw_partition_keys_and_offsets` — BIG Index.db raw partition-key bytes and entry-byte parity
  - Normalization: JSONL single-column partition keys are decoded to their raw on-disk byte encoding (UUID text -> 16 bytes) before comparison; composite-key byte decoding is not attempted (those fixtures are covered by entry-byte + count + offset parity).
- `cass.index_db.bti.index_component_discovery` — BTI index-component discovery and classification

## Canonical-semantic scenarios

- `cass.bti_big_version_matrix.big_nb_oa_read` — BIG nb/oa read parity matrix
  - Normalization: Decoded rows for nb (Cassandra 4-compatible BIG) and oa (Cassandra 5 BIG) datasets are compared against sstabledump JSONL.
- `cass.bti_big_version_matrix.bti_da_write_read` — BTI da write and read-back parity
  - Normalization: CQLite-written da BTI SSTables are dumped with Cassandra 5 sstabledump and compared for value equivalence; Partitions.db footer shape [firstPos|keyCount|root] is matched against a real Cassandra fixture.
- `cass.compaction_merge.tombstone_ttl_shadowing` — Compaction tombstone/TTL shadowing — canonical semantic parity
  - Normalization: Tier-2 logical equivalence: merged partition/cell/timestamp/TTL/local deletion time/is_deleted facts compared against Cassandra compaction output; presentation/file layout ignored.
- `cass.compression_checksum.chunk_offsets_and_crc` — CompressionInfo.db chunk decode and row-count parity
  - Normalization: Decompressed chunk payloads are decoded to rows and compared (row count and values) against sstabledump JSONL; chunk offset tables are used for positioning.
- `cass.data_db_decode.row_cell_flags_and_vint` — Data.db row/cell flags and VInt decode parity
  - Normalization: Rows and cells are normalized to the sstabledump JSONL fact model (partition key, clustering, cell name/value, liveness, deletion) and compared field-by-field; presentation ordering and whitespace ignored.
- `cass.index_summary.big_index_offsets` — Index.db partition key digests and data offsets (BIG)
  - Normalization: Partition key digests and Data.db offsets resolved through Index.db are compared against the partition order and keys derived from sstabledump JSONL.
- `cass.statistics_metadata.serialization_header` — Statistics.db metadata and serialization header parity
  - Normalization: Min/max timestamps, row count, partition count and serialization-header column types are compared against the Statistics.db.txt dump and sstabledump JSONL.
- `cass.tombstone_ttl.ttl_and_local_deletion_time` — TTL, local deletion time, and WRITETIME parity
  - Normalization: TTL, localDeletionTime and WRITETIME are compared against the ttl/expiresAt/tstamp facts emitted by sstabledump JSONL.

## Smoke-only scenarios

- `cass.cli_reporting.parity_manifest_lint_and_report` — Parity manifest lint and report tooling
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1)
- `cass.schema_evolution.serialization_header_column_order` — Serialization-header column order across schema evolution
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader

## Gaps and next steps

- `cass.compaction_merge.byte_for_byte_output` (planned): No gated byte-for-byte comparison of compaction output. → _Promote the debug byte tier in compaction-parity to a gated comparison once writer output is byte-stable._
- `cass.compression_checksum.checksum_trailer_detection` (partial): No gated byte comparison of Digest.crc32 against the Cassandra reference. → _Add a Digest.crc32 byte comparison to the sstable_parity_corruption_verify suite._
- `cass.corruption_verify.component_corruption_detection` (planned): No scrub/verify parity pass implemented. → _Implement a verify pass and compare detected-corruption outcomes against Cassandra VerifyTest/ScrubTest scenarios._
- `cass.delta_scan.tombstone_liveness_facts` (partial): test_deltas dataset asset not published/enforced (#701). → _Publish and enforce the test_deltas dataset in delta-roundtrip CI._
- `cass.filter_db_bloom.serialization_no_false_negative` (partial): No no-false-negative parity assertion against Cassandra Filter.db. → _Add a Filter.db serialization parity test asserting zero false negatives across the present-key set._
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` (partial): No committed BIG fixture triggers promoted-index emission (all partitions are below the column_index_size threshold). → _Generate a wide-partition BIG fixture (partition exceeding column_index_size_in_kb) and assert decoded promoted-index clustering boundaries against the Cassandra reference._
- `cass.index_db.big.wide_partition_promoted_entries` (partial): No committed wide BIG fixture exercises promoted clustering boundaries. → _Add a BIG fixture with a partition exceeding column_index_size_in_kb and compare decoded promoted clustering boundaries to the Cassandra reference._
- `cass.index_summary.summary_boundaries` (partial): Cassandra Summary.db reference dumps not published for all tables. → _Publish Summary.db reference dumps and enable strict first/last-key boundary comparison in the sstable_parity_summary_db_big suite._
- `cass.tombstone_ttl.range_tombstone_boundaries` (partial): test_deltas dataset asset not published/enforced in CI (#701). → _Publish the test_deltas dataset and enforce scan_delta parity in CI._

## Out-of-scope taxonomy

_Out of scope does not mean unimportant._ Node behaviors CQLite does not mirror:

### `commitlog_replay`

- `cass.commitlog_replay.recovery_out_of_scope` — Commitlog and replay compatibility (out of scope)
  - Safe wording: CQLite reads SSTables that Cassandra has already flushed; it does not replay or validate commitlogs.

### `distributed_consensus`

- `cass.distributed_consensus.paxos_accord_out_of_scope` — Paxos/Accord and distributed consensus (out of scope)
  - Safe wording: CQLite does not implement distributed consensus.

### `nodetool_jmx_metrics`

- `cass.nodetool_jmx_metrics.operational_out_of_scope` — nodetool, JMX, metrics, and operational controls (out of scope)
  - Safe wording: CQLite does not provide nodetool/JMX operational behavior.

### `read_repair_coordinator`

- `cass.read_repair_coordinator.out_of_scope` — Read-repair coordinator (out of scope)
  - Safe wording: CQLite does not perform read repair.

### `repair_coordinator`

- `cass.repair_coordinator.anti_entropy_out_of_scope` — Repair coordinator and anti-entropy protocol (out of scope)
  - Safe wording: CQLite does not perform or validate repair.

### `sai_sasi_query`

- `cass.sai_sasi_query.secondary_index_out_of_scope` — SAI/SASI secondary index query behavior (out of scope)
  - Safe wording: CQLite reads base-table SSTables only and does not implement SAI/SASI secondary indexes.

### `streaming_protocol`

- `cass.streaming_protocol.node_lifecycle_out_of_scope` — SSTable streaming protocol and node lifecycle (out of scope)
  - Safe wording: CQLite-written SSTables can be loaded with sstableloader, but CQLite does not implement the streaming protocol itself.

## CI workflow mapping

| Scenario | CI tier | Workflow |
|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.bti_big_version_matrix.bti_da_write_read` | nightly_docker | .github/workflows/e2e-readback.yml |
| `cass.cli_reporting.parity_manifest_lint_and_report` | fast_pr | .github/workflows/cassandra-parity.yml |
| `cass.commitlog_replay.recovery_out_of_scope` | fast_pr | — |
| `cass.compaction_merge.byte_for_byte_output` | manual_debug | — |
| `cass.compaction_merge.load_path_validity` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.tombstone_ttl_shadowing` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compression_checksum.checksum_trailer_detection` | fast_pr | — |
| `cass.compression_checksum.chunk_offsets_and_crc` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.corruption_verify.component_corruption_detection` | manual_debug | — |
| `cass.data_db_decode.row_cell_flags_and_vint` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.delta_scan.tombstone_liveness_facts` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.distributed_consensus.paxos_accord_out_of_scope` | fast_pr | — |
| `cass.filter_db_bloom.serialization_no_false_negative` | fast_pr | — |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.SSTableScannerTest.range_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.big.raw_partition_keys_and_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.big.wide_partition_promoted_entries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.bti.index_component_discovery` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_summary.big_index_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_summary.summary_boundaries` | fast_pr | — |
| `cass.nodetool_jmx_metrics.operational_out_of_scope` | fast_pr | — |
| `cass.read_repair_coordinator.out_of_scope` | fast_pr | — |
| `cass.repair_coordinator.anti_entropy_out_of_scope` | fast_pr | — |
| `cass.sai_sasi_query.secondary_index_out_of_scope` | fast_pr | — |
| `cass.schema_evolution.serialization_header_column_order` | fast_pr | — |
| `cass.sstable_format.descriptor_component_resolution` | fast_pr | — |
| `cass.sstable_format.toc_component_manifest` | fast_pr | — |
| `cass.statistics_metadata.serialization_header` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.streaming_protocol.node_lifecycle_out_of_scope` | fast_pr | — |
| `cass.tombstone_ttl.range_tombstone_boundaries` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | required_parity | .github/workflows/cassandra-validation.yml |

## Fixture and reference mapping

| Scenario | Storage fmt | References / failure artifacts |
|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | nb, oa | test-data/datasets/sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Data.db.jsonl |
| `cass.bti_big_version_matrix.bti_da_write_read` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Data.db.jsonl |
| `cass.cli_reporting.parity_manifest_lint_and_report` | — | — |
| `cass.commitlog_replay.recovery_out_of_scope` | — | — |
| `cass.compaction_merge.byte_for_byte_output` | — | — |
| `cass.compaction_merge.load_path_validity` | nb | — |
| `cass.compaction_merge.tombstone_ttl_shadowing` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compression_checksum.checksum_trailer_detection` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Digest.crc32<br>_fail:_ target/cassandra-parity/checksum-mismatch.log |
| `cass.compression_checksum.chunk_offsets_and_crc` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.corruption_verify.component_corruption_detection` | — | — |
| `cass.data_db_decode.row_cell_flags_and_vint` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.tombstone_liveness_facts` | nb | test-data/datasets/sstables/test_deltas/collection_ops-2a5006f06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.distributed_consensus.paxos_accord_out_of_scope` | — | — |
| `cass.filter_db_bloom.serialization_no_false_negative` | nb | — |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.SSTableScannerTest.range_boundaries` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.big.raw_partition_keys_and_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.big.wide_partition_promoted_entries` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.index_db.bti.index_component_discovery` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_summary.big_index_offsets` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.index_summary.summary_boundaries` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.nodetool_jmx_metrics.operational_out_of_scope` | — | — |
| `cass.read_repair_coordinator.out_of_scope` | — | — |
| `cass.repair_coordinator.anti_entropy_out_of_scope` | — | — |
| `cass.sai_sasi_query.secondary_index_out_of_scope` | — | — |
| `cass.schema_evolution.serialization_header_column_order` | nb | — |
| `cass.sstable_format.descriptor_component_resolution` | nb, oa, da | — |
| `cass.sstable_format.toc_component_manifest` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-TOC.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt |
| `cass.statistics_metadata.serialization_header` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt |
| `cass.streaming_protocol.node_lifecycle_out_of_scope` | — | — |
| `cass.tombstone_ttl.range_tombstone_boundaries` | nb | test-data/datasets/sstables/test_deltas/adjacent_ranges-972f22806c7811f1a24ff924a65838e2/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | nb | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | nb | — |

## Claim language

**Safe:** CQLite reads and writes Cassandra 5.0 SSTables and is validated for canonical-semantic equivalence against `sstabledump` for the covered dataset, with byte-for-byte parity proven only where this report records `byte_for_byte` evidence.

**Unsafe:** "CQLite passes the same tests as Cassandra" or "CQLite is byte-for-byte identical to Cassandra" — these overclaim node behavior and byte parity the manifest does not support.

