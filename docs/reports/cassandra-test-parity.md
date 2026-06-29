# Cassandra Test Parity Report

> Generated from `test-data/cassandra-parity-manifest.yml` by `cargo run -p cassandra-parity -- report`. Do not edit by hand — edit the manifest and regenerate.

Cassandra source: [`cassandra-5.0.2`](https://github.com/apache/cassandra/tree/cassandra-5.0.2) @ `f278f6774fc76465c182041e081982105c3e7dbb` (git SHA). Program: parent epic #966, reporting epic #967.

Sources: [`docs/cassandra_test_index.md`](../../docs/cassandra_test_index.md) · [`docs/reports/cassandra-test-parity-assessment.md`](../../docs/reports/cassandra-test-parity-assessment.md)

## Status counts

| Status | Scenarios |
|---|---|
| `mirrored` | 205 |
| `partial` | 17 |
| `planned` | 18 |
| `out_of_scope` | 14 |
| **total** | **254** |

## Evidence counts

| Evidence | Scenarios |
|---|---|
| `byte_for_byte` | 111 |
| `canonical_semantic` | 93 |
| `smoke` | 7 |
| `partial` | 27 |
| `out_of_scope` | 16 |

## ⚠️ P0 scenarios with weak evidence

These P0 scenarios are backed only by `smoke` or `partial` evidence and must not be cited as proof of byte parity:

- `cass.compaction_merge.byte_for_byte_output` — Compaction byte-for-byte output parity (future) (partial)
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1) (smoke)
- `cass.compression_checksum.checksum_trailer_detection` — Inline checksum / Digest.crc32 corruption detection (partial)
- `cass.compression_info.deflate.real_fixture_chunks.strict` — Deflate real-fixture CompressionInfo.db + chunk parity (partial)
- `cass.compression_info.zstd.real_fixture_chunks.strict` — Zstd real-fixture CompressionInfo.db + chunk parity (partial)
- `cass.corruption_verify.component_corruption_detection` — Component corruption detection, scrub, and verify (partial)
- `cass.data_db_decode.wide_partition.row_boundaries` — Wide-partition Data.db row boundaries align with promoted-index offsets (partial)
- `cass.filter_db_bloom.serialization_no_false_negative` — Filter.db Bloom filter serialization with no false negatives (partial)
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` — BIG Index.db promoted-index (wide-partition) boundary metadata (partial)
- `cass.index_db.promoted_index.clustering_bounds` — BIG promoted-index IndexInfo clustering bounds ordering and coverage (partial)
- `cass.index_db.promoted_index.index_info_offsets` — BIG promoted-index IndexInfo Data.db offsets and width chain (partial)
- `cass.index_db.promoted_index.range_tombstone_boundary_at_block_edge` — BIG promoted-index decode across a range-tombstone block boundary (partial)
- `cass.index_summary.column_index.range_tombstone_boundary_big_bti` — Column-index range-tombstone boundary across BIG and BTI formats (partial)
- `cass.index_summary.summary_boundaries` — Summary.db sampling boundaries (BIG) (partial)
- `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` — Dropped-column empty-index-block reverse-scan parity (partial)
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution (smoke)
- `cass.tombstone_ttl.range_tombstone_boundaries` — Range tombstone boundary and deletion-time parity (partial)
- `cass.tombstone_ttl.repaired_unrepaired_purge_gate` — Repaired vs unrepaired purge gate parity (partial)
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader (smoke)
- `cass.write_load_path.flush.tombstone_and_ttl_artifacts` — Flush produces tombstone + TTL Data.db / Statistics.db artifacts (partial)

## P0 scenarios

| ID | Capability | Status | Evidence | Suite | Risk |
|---|---|---|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.bti_big_version_matrix.bti_da_write_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.compaction.CompactionAwareWriterTest.live_row_count_preservation` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction.CompactionAwareWriterTest.row_count_and_order_preservation` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction.CompactionIteratorTest.differential_compaction_loop` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.CompactionIteratorTest.live_partition_merge` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.CompactionSimpleValueMergeTest.static_row_merge` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction.GcCompactionTest.static_and_complex_columns_survive_gc` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.LongCompactionsTest.live_rows_lww_overlap` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.SSTableRewriterTest.output_component_integrity` | compaction_merge | planned | byte_for_byte | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.harness_byte_tier_artifacts` | compaction_merge | planned | byte_for_byte | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.harness_logical_tier` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction.issue_899_per_element_collection_compaction` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.live_cells_clustering_lww` | compaction_merge | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction.live_cells_no_clustering` | compaction_merge | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.GcCompactionTest.row_cell_partition_tombstone_gc` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.byte_for_byte_output` | compaction_merge | planned | partial | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.issue_819.differential_input_merge_write_fidelity` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.issue_819.differential_row_tombstone_wide_partition_regression` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.load_path_validity` | compaction_merge | mirrored | smoke | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction_merge.partial_source_retains_tombstones` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.tombstone_ttl_shadowing` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compression.fixture_matrix.deflate` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.fixture_matrix.incompressible_uncompressed_chunk` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.fixture_matrix.lz4` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.fixture_matrix.short_final_chunk` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.fixture_matrix.snappy` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.fixture_matrix.zstd_no_dictionary` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression.registry.unknown_algorithm_rejected` | compression_checksum | mirrored | canonical_semantic | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_checksum.checksum_trailer_detection` | compression_checksum | partial | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.compression_checksum.chunk_offsets_and_crc` | compression_checksum | mirrored | canonical_semantic | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.deflate.real_fixture_chunks.strict` | compression_checksum | planned | partial | `sstable_parity_compression_info_chunks` | p2_coverage |
| `cass.compression_info.fields.algorithm_name` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.fields.chunk_length` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.fields.chunk_offsets` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.fields.data_length` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.fields.max_compressed_length` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.fields.options` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.lz4.real_fixture_chunks` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.lz4.real_fixture_chunks.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p1_correctness |
| `cass.compression_info.snappy.real_fixture_chunks` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.snappy.real_fixture_chunks.strict` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p1_correctness |
| `cass.compression_info.zstd.real_fixture_chunks.strict` | compression_checksum | planned | partial | `sstable_parity_compression_info_chunks` | p2_coverage |
| `cass.corruption.bti_partitions_footer_bit_flip` | corruption_verify | planned | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.bti_rows_truncation` | corruption_verify | planned | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.compression_info.bad_offset` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.data_db.bit_flip` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.data_db.truncation` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.digest_crc32_mismatch` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.index_db.bit_flip_big` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption.statistics_db.header_damage` | corruption_verify | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.corruption_verify.component_corruption_detection` | corruption_verify | planned | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.cql_types.boundaries.null_empty_text_blob` | cql_types | mirrored | canonical_semantic | `—` | p1_correctness |
| `cass.cql_types.jsonl.canonical_value_comparator` | cql_types | mirrored | canonical_semantic | `—` | tooling_only |
| `cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare` | cql_types | mirrored | canonical_semantic | `—` | tooling_only |
| `cass.cql_types.jsonl.manifest_report_generation` | cql_types | mirrored | canonical_semantic | `—` | tooling_only |
| `cass.cql_types.jsonl.no_placeholder_references` | cql_types | mirrored | canonical_semantic | `—` | tooling_only |
| `cass.cql_types.jsonl.schema_aware_normalization` | cql_types | mirrored | canonical_semantic | `—` | tooling_only |
| `cass.data_db.inline_crc.bad_trailer_rejected` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db.inline_crc.incompressible_uncompressed_chunk` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db.inline_crc.offset_delta_minus_crc_length` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db.inline_crc.short_final_chunk` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db.inline_crc.valid_trailer` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db_decode.clustering_bounds.desc_order` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.clustering_bounds.multi_column_prefix` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.clustering_bounds.null_vs_empty` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.range_tombstone.bound_markers` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.range_tombstone.boundary_markers` | tombstone_ttl | partial | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.row_cell_flags_and_vint` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.serialization_header.timestamp_ttl_ldt_deltas` | data_db_decode | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.data_db_decode.serialization_mirror.multi_clustering_column_order` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.static_rows.static_only_partition` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.static_rows.static_with_clustering_rows` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.tombstone.cell_deletion_time` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.tombstone.partition_deletion_time` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.tombstone.row_deletion_time` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.ttl.local_deletion_time_delta` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.data_db_decode.unfiltered_serializer.row_and_cell_flags` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.unfiltered_serializer.row_size_vints` | data_db_decode | mirrored | byte_for_byte | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.data_db_decode.wide_partition.row_boundaries` | data_db_decode | partial | partial | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.delta_scan.adjacent_ranges` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.cell_tombstones` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.collection_ops` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.partial_updates` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.partition_tombstones` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.range_tombstones` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.row_tombstones` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.static_with_rows` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.delta_scan.ttl_cells` | delta_scan | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.filter_db.corruption_fails_closed` | filter_db_bloom | mirrored | byte_for_byte | `sstable_parity_filter_db_bloom` | p1_correctness |
| `cass.filter_db.no_false_negative_membership` | filter_db_bloom | mirrored | byte_for_byte | `sstable_parity_filter_db_bloom` | p0_data_loss |
| `cass.filter_db.serialization_round_trip` | filter_db_bloom | mirrored | byte_for_byte | `sstable_parity_filter_db_bloom` | p1_correctness |
| `cass.filter_db_bloom.serialization_no_false_negative` | filter_db_bloom | partial | partial | `sstable_parity_filter_db_bloom` | p0_data_loss |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p0_data_loss |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.SSTableScannerTest.range_boundaries` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.big.raw_partition_keys_and_offsets` | index_summary | mirrored | byte_for_byte | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.bti.index_component_discovery` | index_summary | mirrored | byte_for_byte | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.index_db.promoted_index.clustering_bounds` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.promoted_index.index_info_offsets` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_db.promoted_index.range_tombstone_boundary_at_block_edge` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.big_index_offsets` | index_summary | mirrored | canonical_semantic | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.column_index.range_tombstone_boundary_big_bti` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.summary_boundaries` | index_summary | partial | partial | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` | schema_evolution | partial | partial | `sstable_parity_delta_scan` | p1_correctness |
| `cass.schema_evolution.dropped_column.per_cell_purge` | schema_evolution | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.schema_evolution.issue_847_dropped_column_filter` | schema_evolution | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.schema_evolution.issue_850_static_presence` | schema_evolution | mirrored | canonical_semantic | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.altered_column_type` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.altered_then_dropped_column` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.dropped_column_same_type` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.no_schema_change` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.schema_evolution.serialization_header.static_regular_kind_mismatch` | schema_evolution | mirrored | byte_for_byte | `schema_parity_serialization_header` | p1_correctness |
| `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` | schema_evolution | mirrored | canonical_semantic | `schema_parity_serialization_header` | p1_correctness |
| `cass.sstable_format.descriptor_component_resolution` | sstable_format | mirrored | smoke | `sstable_parity_component_manifest` | p1_correctness |
| `cass.sstable_format.toc_component_manifest` | sstable_format | mirrored | byte_for_byte | `sstable_parity_component_manifest` | p1_correctness |
| `cass.sstable_io.reader.tombstone_only_partition` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.statistics_db.MetadataSerializerTest.metadata_components` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` | tombstone_ttl | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` | schema_evolution | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.core_metadata_checksums` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p0_data_loss |
| `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_metadata.serialization_header` | statistics_metadata | mirrored | canonical_semantic | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_metadata.tombstone_histogram.deletion_times` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.offset_table_entries` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.serialization_round_trip` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.big.index_offset_references` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.bti.summary_discovery_classification` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.tombstone_ttl.NeverPurgeTest.preserve_all_tombstone_types` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.tombstone_ttl.RangeTombstoneTest.marker_merge_and_persistence` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.tombstone_ttl.TTLExpiryTest.gc_boundary` | tombstone_ttl | mirrored | byte_for_byte | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.tombstone_ttl.deletion_markers.cell_delete` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.deletion_markers.partition_delete` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.deletion_markers.range_delete_bounds` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.deletion_markers.range_tombstone_boundary` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.deletion_markers.row_delete` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.gc_grace.partition_row_cell` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.never_purge.cell_row_partition` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.range_tombstone.closed_last_block` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.range_tombstone.index_block_first_marker` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.range_tombstone.index_block_last_marker` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.range_tombstone.open_ended_middle_block` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.range_tombstone_boundaries` | tombstone_ttl | partial | partial | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.repaired_unrepaired_purge_gate` | tombstone_ttl | partial | partial | `sstable_parity_statistics_db` | p0_data_loss |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.static_row.dropped_static_header_preserved` | tombstone_ttl | mirrored | byte_for_byte | `schema_parity_serialization_header` | p0_data_loss |
| `cass.tombstone_ttl.static_row.with_row_cell_range_tombstones` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p0_data_loss |
| `cass.tombstone_ttl.ttl_cells.local_deletion_time` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.ttl_cells.mixed_expiring_and_live` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.tombstone_ttl.ttl_expiry.gc_before_boundary` | tombstone_ttl | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.verify.compression_info_parse` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.verify.digest_crc32_match` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.verify.full_row_scan` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.verify.healthy_compressed_sstable` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.verify.inline_crc_validation` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.verify.no_silent_empty_result_on_corruption` | corruption_verify | mirrored | canonical_semantic | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | write_load_path | mirrored | smoke | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |
| `cass.write_load_path.cql_sstable_writer.finished_data_db_artifacts` | write_load_path | mirrored | byte_for_byte | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |
| `cass.write_load_path.flush.partition_boundary_artifacts` | write_load_path | mirrored | byte_for_byte | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.write_load_path.flush.tombstone_and_ttl_artifacts` | write_load_path | partial | partial | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |
| `cass.write_load_path.live_readback.semantic_only` | write_load_path | mirrored | canonical_semantic | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |

## Byte-for-byte scenarios

- `cass.compaction.SSTableRewriterTest.output_component_integrity` — Compaction output components byte-identical to Cassandra SSTableRewriter (the parity claim) _(planned — no evidence yet)_
- `cass.compaction.harness_byte_tier_artifacts` — Differential harness byte-tier MECHANISM — per-component cmp engine + failure artifacts _(planned — no evidence yet)_
- `cass.compaction.live_cells_clustering_lww` — Live-cell compaction byte parity — clustering table, LWW overlap (the claim)
  - Normalization: Data.db/Index.db/Summary.db/Digest.crc32 whole-file byte-for-byte; CRC.db prefix (compaction-only trailing empty-chunk CRC32=0 excluded); JSONL secondary.
- `cass.compaction.live_cells_no_clustering` — Live-cell compaction byte parity — partition-key-only table (the claim)
  - Normalization: Data.db/Index.db/Summary.db/Digest.crc32 whole-file byte-for-byte; CRC.db prefix (compaction-only trailing empty-chunk CRC32=0 excluded); JSONL secondary.
- `cass.compression.fixture_matrix.deflate` — Deflate (zlib) compression fixture — CompressionInfo.db parity
- `cass.compression.fixture_matrix.incompressible_uncompressed_chunk` — Incompressible payload — chunk stored uncompressed within compressed file
- `cass.compression.fixture_matrix.lz4` — LZ4 compression fixture — CompressionInfo.db chunk-offset/CRC parity
- `cass.compression.fixture_matrix.short_final_chunk` — Short final chunk fixture — partial trailing chunk parity
- `cass.compression.fixture_matrix.snappy` — Snappy compression fixture — CompressionInfo.db chunk-offset/CRC parity
- `cass.compression.fixture_matrix.uncompressed_table` — Uncompressed table fixture — no CompressionInfo.db emitted
- `cass.compression.fixture_matrix.zstd_no_dictionary` — Zstd (no dictionary) compression fixture — CompressionInfo.db parity
- `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` — Truncated compressed chunk fail-closed parity
- `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection.strict` — Corrupted CompressionInfo.db / truncated chunk fails closed
- `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` — CompressionInfo.db ordered chunk-offset table parity
- `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets.strict` — CompressionInfo.db chunk-offset table parity
- `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` — Compressed chunk record boundaries vs Data.db parity
- `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries.strict` — Compressed chunk boundary consistency with Data.db
- `cass.compression_info.CompressionMetadataTest.metadata_serialization` — CompressionInfo.db metadata byte-for-byte serialization parity
- `cass.compression_info.CompressionMetadataTest.metadata_serialization.strict` — CompressionInfo.db metadata byte-for-byte round-trip
- `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` — Inline per-chunk CRC32 trailer validation parity
- `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation.strict` — Data.db inline per-chunk CRC32 trailer parity
- `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` — Compressed chunk payload + CRC round-trip byte parity
- `cass.compression_info.fields.algorithm_name` — CompressionInfo.db algorithm-name field parity
- `cass.compression_info.fields.chunk_length` — CompressionInfo.db chunk_length field parity
- `cass.compression_info.fields.chunk_offsets` — CompressionInfo.db per-chunk offset table parity
- `cass.compression_info.fields.data_length` — CompressionInfo.db data_length (uncompressed total) parity
- `cass.compression_info.fields.max_compressed_length` — CompressionInfo.db max_compressed_length field parity
- `cass.compression_info.fields.options` — CompressionInfo.db compressor-options map parity
- `cass.compression_info.layout.no_crc_fields` — CompressionInfo.db carries no per-chunk CRC (CRC lives inline in Data.db)
- `cass.compression_info.lz4.real_fixture_chunks` — LZ4Compressor real-fixture chunk + CRC parity
- `cass.compression_info.lz4.real_fixture_chunks.strict` — LZ4 real-fixture CompressionInfo.db + chunk parity
- `cass.compression_info.snappy.real_fixture_chunks` — SnappyCompressor real-fixture chunk + CRC parity
- `cass.compression_info.snappy.real_fixture_chunks.strict` — Snappy real-fixture CompressionInfo.db + chunk parity
- `cass.corruption.bti_partitions_footer_bit_flip` — BTI Partitions.db footer bit flip is detected _(planned — no evidence yet)_
- `cass.corruption.bti_rows_truncation` — BTI Rows.db truncation is detected _(planned — no evidence yet)_
- `cass.corruption.compression_info.bad_offset` — CompressionInfo.db out-of-bounds chunk offset is detected
- `cass.corruption.data_db.bit_flip` — Data.db single-bit flip is detected (LZ4 chunk decode / CRC)
- `cass.corruption.data_db.truncation` — Data.db mid-stream truncation is detected
- `cass.corruption.digest_crc32_mismatch` — Digest.crc32 mismatch is detected
- `cass.corruption.index_db.bit_flip_big` — BIG Index.db bit flip is detected
- `cass.corruption.statistics_db.header_damage` — Statistics.db header damage is detected
- `cass.corruption.summary_db_truncation` — Summary.db truncation is detected
- `cass.corruption.toc_missing_component` — TOC.txt missing component is detected
- `cass.cql_types.boundaries.length_prefix_edges` — Cell value length-prefix edge encoding parity
- `cass.cql_types.collections.map_key_lookup_offsets` — Frozen-map key lookup offset codec vectors
- `cass.cql_types.collections.map_key_range_offsets` — Frozen-map key range offset codec vectors
- `cass.cql_types.collections.set_lookup_offsets` — Frozen-set element lookup offset codec vectors
- `cass.cql_types.collections.set_range_offsets` — Frozen-set element range offset codec vectors
- `cass.cql_types.collections.single_cell_multicell_equivalence` — Single-cell vs multicell collection equivalence vectors
- `cass.cql_types.collections.vint_element_count_boundaries` — VInt element-count boundary codec vectors
- `cass.cql_types.primitives.fixed_width_vectors` — Fixed-width primitive serializer codec vectors
- `cass.cql_types.primitives.invalid_length_rejection` — Invalid-length primitive decode rejection
- `cass.cql_types.primitives.temporal_vectors` — Date / time / timestamp serializer codec vectors
- `cass.cql_types.primitives.text_blob_ascii_vectors` — Text / blob / ascii serializer codec vectors
- `cass.cql_types.primitives.uuid_inet_vectors` — UUID / timeuuid / inet serializer codec vectors
- `cass.cql_types.primitives.varint_decimal_duration_vectors` — Varint / decimal / duration serializer codec vectors
- `cass.data_db.inline_crc.bad_trailer_rejected` — Data.db corrupt inline CRC trailer is rejected (no silent decode)
- `cass.data_db.inline_crc.incompressible_uncompressed_chunk` — Data.db inline CRC on an uncompressed (incompressible) chunk
- `cass.data_db.inline_crc.offset_delta_minus_crc_length` — Chunk payload length = next_offset - this_offset - 4 (CRC length)
- `cass.data_db.inline_crc.short_final_chunk` — Data.db inline CRC on the short final chunk
- `cass.data_db.inline_crc.valid_trailer` — Data.db per-chunk inline CRC32 trailer validation
- `cass.data_db_decode.clustering_bounds.desc_order` — Data.db DESC (ReversedType) clustering-bound parity
  - Normalization: BYTE parity (DESC timestamp + ASC text clustering values at exact offsets, proving ReversedType does NOT invert on-disk value bytes) is asserted separately from SEMANTIC JSONL parity (full clustering tuple). The fixture is decompressed first (FAIL CLOSED on a missing fixture).
- `cass.data_db_decode.clustering_bounds.multi_column_prefix` — Data.db multi-column clustering-prefix byte consumption parity
  - Normalization: The deterministic writer lane asserts the FULL multi-column prefix → row_size framing byte-for-byte; the wide fixture lane asserts the first four real-Cassandra clustering values' widths/values at exact offsets (the 5th DATE column's on-disk offset is NOT byte-asserted — see known_limitations) and the JSONL lane asserts the full 5-tuple semantically. FAIL CLOSED on a missing fixture.
- `cass.data_db_decode.clustering_bounds.null_vs_empty` — Data.db null-vs-empty clustering value distinction parity
  - Normalization: Pure BYTE-level distinction: the EMPTY clustering's PRESENT header + zero-length value VInt vs the static (absent) row's omitted prefix (prev_size=0), both asserted at exact offsets via the public DataWriter encode surface and re-decoded through the reader's parse_vuint. The ABSENT/static byte shape is additionally observed in the pinned static_columns_table fixture (`fixture_static_columns_marker_byte_parity`).
- `cass.data_db_decode.range_tombstone.bound_markers` — Data.db range-tombstone bound-marker parity
  - Normalization: The bound-marker grammar is compared field-by-field at exact offsets: the IS_MARKER flag, the ClusteringPrefix.Kind ordinal, the u16 cluster count, the prefix clustering value (a PREFIX shorter than the full arity is expected for `[ck1,*]` deletes), the marker_body_size/prev_size framing, and the single mfda/ldt delta. Under `CQLITE_REQUIRE_FIXTURES=1` (as the comparison_command sets) the fixture lane is fail-closed — a missing fixture is a hard failure; skip-on-absence applies only to non-strict local runs. The deterministic writer lane runs everywhere, and the fixture lane also FAILS on present-but-empty / 0 markers.
- `cass.data_db_decode.range_tombstone.boundary_markers` — Data.db range-tombstone boundary-marker (two deletion times) parity
  - Normalization: The boundary-marker grammar is compared field-by-field at exact offsets: IS_MARKER, the kind-2/kind-5 ordinal, the u16 cluster count, the prefix clustering value, and BOTH deletion-time pairs (primary then secondary) reconstructed against the golden end/start marked_deleted µs.
- `cass.data_db_decode.serialization_header.timestamp_ttl_ldt_deltas` — Data.db timestamp / TTL / local-deletion-time delta parity
  - Normalization: Deltas are decoded as unsigned VInts and reconstructed against the Statistics.db EncodingStats minima; the fixture lane compares the reconstructed absolute timestamp to the JSONL golden in microseconds (FAIL CLOSED on a missing fixture). Wall-clock-derived LDT bytes are asserted only for sign/magnitude, not an exact constant.
- `cass.data_db_decode.serialization_mirror.multi_clustering_column_order` — Data.db multi-clustering-column order parity
  - Normalization: Clustering header and per-column value bytes are compared at their exact offsets in declared order; the fixture lane decompresses the LZ4 Data.db first (FAIL CLOSED on a missing fixture).
- `cass.data_db_decode.static_rows.static_only_partition` — Data.db static-only partition marker parity
  - Normalization: Two distinct assertion families: (1) STATIC-ROW BYTE parity — the row flag byte, extended IS_STATIC bit, hard-coded prev_size=0, and the omitted clustering prefix are compared at exact offsets in the decompressed Data.db; (2) SEMANTIC/JSONL parity — the decoded static cell value and the static structure are compared against the sstabledump golden. These are asserted separately. The pinned fixture is decompressed first (FAIL CLOSED on a missing pinned fixture); the truly static-ONLY partition shape is covered deterministically (always) plus the local-only static_with_rows lane (skip-on-presence).
- `cass.data_db_decode.static_rows.static_with_clustering_rows` — Data.db static cells + clustering rows in one partition parity
  - Normalization: STATIC-ROW + clustering BYTE parity (static marker, then clustered-row clustering header + value offsets) is asserted separately from SEMANTIC JSONL parity (static value + static/clustering coexistence). The pinned fixture is decompressed first (FAIL CLOSED on a missing pinned fixture).
- `cass.data_db_decode.tombstone.cell_deletion_time` — Data.db cell deletion-time (own delta fields, no value bytes) parity
  - Normalization: Cell tombstone flag byte (IS_DELETED | HAS_EMPTY_VALUE) and own ts/ldt deltas are compared at exact offsets; the no-value-bytes claim is asserted as the framed body ending exactly after the cell's ldt delta. Under `CQLITE_REQUIRE_FIXTURES=1` (as the comparison_command sets) the fixture lane is fail-closed — a missing fixture is a hard failure; skip-on-absence applies only to non-strict local runs. The fixture lane also FAILS on a present-but-empty body.
- `cass.data_db_decode.tombstone.partition_deletion_time` — Data.db partition deletion-time (fixed header form) parity
  - Normalization: The partition deletion is compared as a FIXED big-endian i32 LDT + i64 mfda at their exact header offsets (NOT a VInt delta). Under `CQLITE_REQUIRE_FIXTURES=1` (as the comparison_command sets) the fixture lane is fail-closed — a missing fixture is a hard failure; skip-on-absence applies only to non-strict local runs. The deterministic lanes cover the fixed-form shape everywhere, and the fixture lane also FAILS on a present-but-empty body.
- `cass.data_db_decode.tombstone.row_deletion_time` — Data.db row deletion-time (own delta fields, no value bytes) parity
  - Normalization: Row deletion mfda/ldt are decoded as UNSIGNED VInt deltas at exact offsets; the "no value bytes" claim is asserted as the framed body ending exactly after the column bitmap. Under `CQLITE_REQUIRE_FIXTURES=1` (as the comparison_command sets) the fixture lane is fail-closed — a missing fixture is a hard failure; skip-on-absence applies only to non-strict local runs. The fixture lane also FAILS on a present-but-empty body / 0 rows.
- `cass.data_db_decode.ttl.local_deletion_time_delta` — Data.db TTL cell local-deletion-time / TTL delta parity
  - Normalization: TTL and localDeletionTime are decoded as UNSIGNED VInt deltas at exact wire offsets and reconstructed against the Statistics.db EncodingStats minTTL / minLocalDeletionTime. Under `CQLITE_REQUIRE_FIXTURES=1` (as the comparison_command sets) the fixture lane is fail-closed — a missing fixture is a hard failure; skip-on-absence applies only to non-strict local runs. The deterministic lane runs everywhere, and the fixture lane also FAILS on a present-but-empty body or 0 rows.
- `cass.data_db_decode.unfiltered_serializer.row_and_cell_flags` — Data.db row and cell flag-byte parity
  - Normalization: Flag bytes are compared as raw u8 values at their exact wire offset; the fixture lane additionally checks every leading flag byte stays within the known UnfilteredSerializer row-flag mask (FAIL CLOSED on a missing fixture).
- `cass.data_db_decode.unfiltered_serializer.row_size_vints` — Data.db row-size and previous-size VInt framing parity
  - Normalization: row_size/prev_size are unsigned VInt deltas; the deterministic lane compares raw encoded bytes and decoded values at the exact width boundaries, the fixture lane compares the framing offsets of a real nb Data.db (FAIL CLOSED on a missing fixture).
- `cass.filter_db.corruption_fails_closed` — Filter.db malformed-byte rejection (fail-closed)
- `cass.filter_db.no_false_negative_membership` — Filter.db no-false-negative membership over Cassandra present keys
  - Normalization: Present keys are the raw partition-key bytes from Index.db (not the decoded CQL values), matching the bytes Cassandra's Murmur3 hashed into Filter.db. The reference_paths point at the committed per-fixture Data.db.jsonl siblings (the binary Filter.db and Index.db are gitignored, fetched on demand).
- `cass.filter_db.serialization_round_trip` — Filter.db byte-exact serialization round-trip and parameter decode
- `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` — Truncated BIG Index.db fails explicitly
- `cass.index_db.RowIndexEntryTest.partition_offsets` — BIG Index.db partition data offsets vs Cassandra positions
  - Normalization: Index.db data offsets are relative to the Data.db data section while JSONL positions are absolute file offsets; the per-partition Data.db header is a constant only for uniform partition-key lengths without static blocks, so successive deltas (not absolute values) are compared in that case.
- `cass.index_db.SSTableReaderTest.point_lookup_offsets` — BIG Index.db point/absent-key lookup offsets
- `cass.index_db.SSTableScannerTest.range_boundaries` — BIG Index.db key order and range boundaries
- `cass.index_db.big.raw_partition_keys_and_offsets` — BIG Index.db raw partition-key bytes and entry-byte parity
  - Normalization: JSONL single-column partition keys are decoded to their raw on-disk byte encoding (UUID text -> 16 bytes) before comparison; composite-key byte decoding is not attempted (those fixtures are covered by entry-byte + count + offset parity).
- `cass.index_db.bti.index_component_discovery` — BTI index-component discovery and classification
- `cass.repaired_metadata.statistics_db.repaired_at_field` — Statistics.db repairedAt field decode + report parity (unrepaired state)
- `cass.schema_evolution.serialization_header.altered_column_type` — Serialization-header parity after ALTER column type
- `cass.schema_evolution.serialization_header.altered_then_dropped_column` — Serialization-header parity after ALTER then DROP column
- `cass.schema_evolution.serialization_header.dropped_column_same_type` — Serialization-header parity after DROP column (same type)
- `cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch` — Serialization-header parity across frozen/multicell collection mismatch
- `cass.schema_evolution.serialization_header.no_schema_change` — Serialization-header parity with no schema change
- `cass.schema_evolution.serialization_header.static_regular_kind_mismatch` — Serialization-header parity across static/regular column-kind mismatch
- `cass.sstable_format.toc_component_manifest` — TOC.txt component manifest completeness
- `cass.statistics_db.MetadataSerializerTest.metadata_components` — Statistics.db metadata-component TOC byte parity (count + ordered types)
- `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` — Statistics.db min timestamp / local-deletion-time / TTL byte parity
- `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` — Statistics.db serialization-header column metadata byte parity
- `cass.statistics_db.SerializationMirrorTest.column_ordering_metadata` — Statistics.db clustering-key ordering / ReversedType byte parity
- `cass.statistics_db.core_metadata_checksums` — Statistics.db embedded CRC32 checksum byte parity
- `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` — Statistics.db max local deletion time for tombstone/TTL fixture
  - Normalization: The STATS-component SSTable max local deletion time decoded by CQLite is compared against the parenthesised integer parsed from the reference dump; the "no tombstones" sentinel (9223372036854775807) is treated as i64::MAX on both sides.
- `cass.statistics_metadata.tombstone_histogram.deletion_times` — Statistics.db estimated tombstone-drop-times histogram parity
  - Normalization: The estimated-tombstone-drop-times histogram buckets decoded by CQLite are compared against the `Estimated tombstone drop times` rows parsed from the reference dump (bucket cardinality, total count, and rounded drop-time points).
- `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` — Summary.db first/last decorated-key boundaries (BIG)
- `cass.summary_db.IndexSummaryTest.offset_table_entries` — Summary.db little-endian offset table + entry ordering (BIG)
- `cass.summary_db.IndexSummaryTest.serialization_round_trip` — Summary.db header + entry serialization round-trip (BIG)
  - Normalization: 24-byte big-endian header and length-prefixed first/last keys decoded from raw bytes; the little-endian offset table is decoded independently and cross-checked against SummaryReader.
- `cass.summary_db.big.index_offset_references` — Summary.db sampled positions resolve to Index.db partition entries (BIG)
  - Normalization: Sampled positions are decoded little-endian (the on-disk truth verified against Index.db) and matched to be16-length-prefixed Index.db keys.
- `cass.summary_db.bti.summary_discovery_classification` — BTI SSTables carry no Summary.db (trie Partitions.db replaces it)
  - Normalization: TOC.txt component manifests are parsed strictly; format is taken from the descriptor filename, never inferred from contents.
- `cass.tombstone_ttl.NeverPurgeTest.preserve_all_tombstone_types` — Preserve all tombstone types under never-purge (partition/row/cell/range)
  - Normalization: Deletion times decoded as deltas / fixed-header at exact wire offsets and reconstructed against EncodingStats minima; fixture lanes are local-only and SKIP when the binary is absent (deterministic lanes run everywhere) but FAIL on a present-but-empty body or 0 rows.
- `cass.tombstone_ttl.RangeTombstoneTest.marker_merge_and_persistence` — Range-tombstone bound-marker grammar merge and persistence
  - Normalization: Bound-marker grammar and deltas decoded at exact wire offsets; fixture lanes are local-only and SKIP when the binary is absent (the deterministic lane runs everywhere) but FAIL on 0 markers.
- `cass.tombstone_ttl.TTLExpiryTest.gc_boundary` — TTL expiry / gc-boundary delta parity (deterministic + fixture)
  - Normalization: TTL and localDeletionTime are decoded as UNSIGNED VInt deltas at exact wire offsets and reconstructed against the Statistics.db EncodingStats minTTL / minLocalDeletionTime; the fixture lane is local-only and SKIPS when the binary is absent (the deterministic lane runs everywhere) but FAILS on a present-but-empty body or 0 rows.
- `cass.tombstone_ttl.static_row.dropped_static_header_preserved` — Dropped static column SerializationHeader byte parity
  - Normalization: The dropped static column is preserved in the embedded SerializationHeader; its name set and kind are compared byte-equal against the StaticColumns line of the reference Statistics.db dump.
- `cass.write_load_path.cql_sstable_writer.finished_data_db_artifacts` — Finished CQLite-written Data.db / component artifacts (write path)
- `cass.write_load_path.flush.partition_boundary_artifacts` — Flush produces partition-boundary artifacts (promoted index / BTI Partitions.db)

## Canonical-semantic scenarios

- `cass.bti_big_version_matrix.big_nb_oa_read` — BIG nb/oa read parity matrix
  - Normalization: Decoded rows for nb (Cassandra 4-compatible BIG) and oa (Cassandra 5 BIG) datasets are compared against sstabledump JSONL.
- `cass.bti_big_version_matrix.bti_da_write_read` — BTI da write and read-back parity
  - Normalization: CQLite-written da BTI SSTables are dumped with Cassandra 5 sstabledump and compared for value equivalence; Partitions.db footer shape [firstPos|keyCount|root] is matched against a real Cassandra fixture.
- `cass.compaction.CompactionAwareWriterTest.live_row_count_preservation` — Live row count + clustering order preservation compaction byte parity
  - Normalization: Cassandra-test-class perspective cross-referencing the byte-level claim cass.compaction.live_cells_clustering_lww. The JSONL golden pins row-count and clustering-order preservation semantically via the (pk,ck)->v map assertion. Byte-level component verification is covered by the real byte_for_byte claim.
- `cass.compaction.CompactionAwareWriterTest.row_count_and_order_preservation` — Compaction preserves row count and partition order (logical)
  - Normalization: Compared on the sstabledump JSONL fact model (partition key order, clustering order, surviving cell set); presentation and file layout ignored.
- `cass.compaction.CompactionIteratorTest.differential_compaction_loop` — Differential compaction loop — same inputs through both engines (logical)
  - Normalization: Both outputs are dumped with Cassandra's own sstabledump (-l) and the wall-clock-derived `expired` flag is normalized out; merged partition/row/cell/timestamp/TTL/deletion facts are compared, file layout and presentation ignored.
- `cass.compaction.CompactionIteratorTest.live_partition_merge` — Live partition merge compaction byte parity (single-output, LWW survivors)
  - Normalization: Cassandra-test-class perspective cross-referencing the byte-level claim cass.compaction.live_cells_no_clustering. The JSONL golden pins the single-output constraint and LWW survivors semantically. Byte-level component verification is covered by the real byte_for_byte claim.
- `cass.compaction.CompactionSimpleValueMergeTest.static_row_merge` — Static row survives compaction with a Cassandra-compatible static prelude
  - Normalization: Static-cell presence + static kind are read back via KWayMerger and the output Statistics.db static-column set is decoded byte-derived; layout ignored.
- `cass.compaction.GcCompactionTest.static_and_complex_columns_survive_gc` — Static and non-frozen-collection columns survive a GC compaction by cell identity
  - Normalization: Per-element complex cells are read back via the compaction KWayMerger and the full (column, cell_path, ts, ttl, ldt, is_deleted) substrate is compared input-vs-output; the input is genuine Cassandra output so this is a faithful round-trip against the on-disk layout.
- `cass.compaction.LongCompactionsTest.live_rows_lww_overlap` — Live-row last-write-wins overlap compaction byte parity (partition-key-only)
  - Normalization: Cassandra-test-class perspective cross-referencing the byte-level claim cass.compaction.live_cells_no_clustering. The JSONL golden pins LWW survivor values semantically (partition count + per-partition v-cell set equality). Byte-level component verification is covered by the real byte_for_byte claim.
- `cass.compaction.harness_logical_tier` — Differential harness logical tier — canonical sstabledump equality
  - Normalization: sstabledump JSONL of both outputs with the wall-clock `expired` flag normalized out; logical facts compared, layout ignored.
- `cass.compaction.issue_899_per_element_collection_compaction` — Non-frozen collection elements reconcile by cell_path with no resurrection
  - Normalization: Per-element survival: complex cells (column, cell_path, ts, ttl, ldt, is_deleted) are read back via the compaction KWayMerger and compared input-vs-output. Complex-deletion-marker shadowing: the `tags` ComplexDeletion marker and surviving `tags` elements for the overwrite partition are read back from the compacted output via KWayMerger and asserted against the Cassandra timestamp rule (marker re-emitted at T; only the ts > T element survives; no ts <= T element resurrected; stable across a second compaction pass).
- `cass.compaction_merge.GcCompactionTest.row_cell_partition_tombstone_gc` — Row/cell/partition tombstone gc through compaction merge (canonical)
  - Normalization: Tier-2 logical equivalence: merged partition/cell/timestamp/TTL/local deletion time/is_deleted facts compared against Cassandra compaction output; presentation/file layout ignored.
- `cass.compaction_merge.issue_819.differential_input_merge_write_fidelity` — Differential compaction — per-cell write timestamps preserved (write fidelity)
  - Normalization: Tier-2 logical equivalence: every surviving cell's timestamp/TTL/local deletion time/is_deleted compared against the merge-from-inputs facts; presentation/file layout ignored.
- `cass.compaction_merge.issue_819.differential_row_tombstone_wide_partition_regression` — Differential compaction — row tombstone in a wide partition round-trips
  - Normalization: Tier-2 logical equivalence of merged partition/row/cell/deletion facts; presentation/file layout ignored.
- `cass.compaction_merge.partial_source_retains_tombstones` — Partial-source compaction retains row/cell tombstones
  - Normalization: When only a subset of the overlapping sources is compacted, row/cell tombstones that may still shadow data in the un-compacted sources are retained; deletion facts are mapped to the sstabledump JSONL and compared.
- `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` — Compaction partition-delete shadowing across skipped sources
  - Normalization: Compacting gen-1 (live pk=1 rows) with gen-2 (partition delete for pk=1, tombstone-only pk=2) now drops the pk=1 rows and retains the tombstone within gc-grace; compared against Cassandra compaction semantics. Verified now that #1072 is fixed.
- `cass.compaction_merge.resurrection_safety.overlapping_sources` — Compaction resurrection-safety across overlapping sources
  - Normalization: Compacting overlapping sources where a tombstone in one source shadows live data in another does not resurrect the shadowed data; partition, row, and cell shadowing is compared against Cassandra compaction output. The partition sub-case is verified now that #1072 is fixed.
- `cass.compaction_merge.static_row.survives_tombstone_gc` — Static row survives tombstone gc through compaction parity
  - Normalization: After compaction purges expired clustered-row tombstones, the live static row must survive; the post-compaction static_block liveness is compared against Cassandra compaction semantics.
- `cass.compaction_merge.tombstone_ttl_shadowing` — Compaction tombstone/TTL shadowing — canonical semantic parity
  - Normalization: Tier-2 logical equivalence: merged partition/cell/timestamp/TTL/local deletion time/is_deleted facts compared against Cassandra compaction output; presentation/file layout ignored.
- `cass.compression.registry.known_deflate` — Compressor registry resolves Deflate by Cassandra class name
  - Normalization: The DeflateCompressor class name resolves to the CQLite zlib decoder; the resolved decoder decodes rows compared against the sstabledump JSONL.
- `cass.compression.registry.known_lz4` — Compressor registry resolves LZ4 by Cassandra class name
  - Normalization: The LZ4Compressor class name from CompressionInfo.db resolves to the CQLite LZ4 decoder; the resolved decoder then decodes rows, which are compared against the sstabledump JSONL to confirm the registry selected the correct algorithm.
- `cass.compression.registry.known_snappy` — Compressor registry resolves Snappy by Cassandra class name
  - Normalization: The SnappyCompressor class name resolves to the CQLite Snappy decoder; the resolved decoder decodes rows compared against the sstabledump JSONL.
- `cass.compression.registry.known_zstd` — Compressor registry resolves Zstd by Cassandra class name
  - Normalization: The ZstdCompressor class name resolves to the CQLite Zstd decoder; the resolved decoder decodes rows compared against the sstabledump JSONL.
- `cass.compression.registry.uncompressed_disabled` — Compressor registry treats absent CompressionInfo.db as uncompressed
  - Normalization: With no CompressionInfo.db present, the registry selects the uncompressed reader path; the decoded rows are compared against the sstabledump JSONL.
- `cass.compression.registry.unknown_algorithm_rejected` — Compressor registry fails fast on an unknown algorithm class
  - Normalization: A synthetic CompressionInfo.db blob naming an unknown compressor class must produce a fail-fast unsupported-algorithm error (#1001 prod fix), never a silent fallback. The known-good LZ4 fixture (JSONL baseline) still decodes, proving the rejection is specific to the unknown class.
- `cass.compression.registry.unsupported_options_rejected` — Compressor registry rejects unsupported compressor options
  - Normalization: An options map the reader cannot honour produces an explicit error rather than a best-effort decode; the supported-options LZ4 fixture (JSONL baseline) still decodes, isolating the rejection to bad options.
- `cass.compression_checksum.chunk_offsets_and_crc` — CompressionInfo.db chunk decode and row-count parity
  - Normalization: Decompressed chunk payloads are decoded to rows and compared (row count and values) against sstabledump JSONL; chunk offset tables are used for positioning.
- `cass.cql_types.boundaries.absent_vs_null_regular_columns` — Absent vs null regular column distinction
  - Normalization: A regular column that was never written (absent) has no cell at all, a column written then deleted is a cell tombstone (null), and a column written with a zero-length value is a live empty cell; the three are mapped to distinct sstabledump JSONL facts (no cell / deletion_info / empty value) and compared.
- `cass.cql_types.boundaries.empty_collections` — Empty collection vs null collection distinction
  - Normalization: Empty multicell collections (no surviving cells) versus absent collections are mapped to the sstabledump JSONL cell-presence facts and compared so that an empty collection and a never-written collection are distinguished.
- `cass.cql_types.boundaries.null_empty_text_blob` — Null / empty text vs blob boundary decode
  - Normalization: Absent, null/tombstoned, empty-string, and empty-blob cells are decoded by the column's declared type (empty blob -> Blob([]) -> "0x", empty text -> "") and compared distinctly against the sstabledump JSONL value. Empty-value typed decode landed in bug fix #1077.
- `cass.cql_types.complex.frozen_udt_value` — Frozen UDT value decode parity
  - Normalization: Frozen UDT values (including the nested frozen<address_type>) are decoded structurally and compared field-by-field against the sstabledump JSONL value. Structured frozen<udt> decode landed in bug fix #1080 (PR #1088).
- `cass.cql_types.complex.legacy_dropped_tuple_udt_fields` — Legacy dropped tuple/UDT field decode parity
  - Normalization: Dropped tuple/UDT columns are skipped via on-disk header metadata and the surviving columns decode and compare against the sstabledump JSONL value; the Err-drops-trailing-column regression was fixed in #1080 (PR #1088).
- `cass.cql_types.complex.multicell_udt_collection_paths` — Multicell UDT / collection cell-path decode parity
  - Normalization: Multicell (non-frozen) UDT and collection cell-paths are decoded structurally and compared against the sstabledump JSONL value. Structured multicell-UDT decode landed in bug fix #1081 (PR #1087).
- `cass.cql_types.complex.nested_frozen_collections` — Nested frozen collection decode parity
  - Normalization: Nested frozen collections (list<frozen<map>>, etc.) are recursively decoded and mapped to the nested sstabledump JSONL value and compared structurally.
- `cass.cql_types.complex.tuple_field_order` — Frozen tuple field-order decode parity
  - Normalization: Frozen tuple field values are mapped in declared field order to the sstabledump JSONL tuple value and compared element-by-element.
- `cass.cql_types.complex.udt_field_order_null_empty` — UDT field-order with null/empty fields decode parity
  - Normalization: Frozen UDT field values are mapped in declared field order to the sstabledump JSONL value and compared field-by-field, including the null-field and empty-string-field distinctions. Structured frozen<udt> decode landed in bug fix #1080 (PR #1088).
- `cass.cql_types.counters.canonical_jsonl_value` — Counter canonical JSONL final-value comparison
  - Normalization: The merged counter final value is rendered to the canonical JSONL value model and compared against the sstabledump JSONL counter cell, independent of the SELECT sidecar.
- `cass.cql_types.counters.compacted_final_value` — Compacted counter final value
  - Normalization: After compaction merges all counter shards into one generation, the compacted final value is compared against the Cassandra SELECT counter sidecar and the post-compaction sstabledump JSONL.
- `cass.cql_types.counters.deleted_counter_shadowing` — Deleted-counter shadowing final value
  - Normalization: A deleted counter shadows older shards; the post-delete final value (counter absent / shadowed) is compared against the Cassandra SELECT counter sidecar and the sstabledump JSONL deletion fact.
- `cass.cql_types.counters.multi_sstable_increment_decrement_merge` — Multi-SSTable counter increment/decrement merge final value
  - Normalization: Counter shards across multiple SSTable generations are merged (increments and decrements) to one final value and compared against the Cassandra SELECT counter sidecar and the merged sstabledump JSONL.
- `cass.cql_types.counters.single_sstable_context_decode` — Single-SSTable counter context final-value decode
  - Normalization: The counter context (shard headers + local count) is decoded to a final scalar value and compared against the Cassandra SELECT counter sidecar and the sstabledump JSONL counter cell.
- `cass.cql_types.jsonl.canonical_value_comparator` — Canonical JSONL value comparator self-test
  - Normalization: The comparator parses a published sstabledump JSONL golden into a typed, ordered canonical value model and asserts an identical re-parse compares equal; type/order mismatches fail loud rather than coercing.
- `cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare` — JSONL cell-path / timestamp / TTL / tombstone comparison self-test
  - Normalization: Per-cell path, writetime/timestamp, TTL/local-deletion-time, and tombstone facts are extracted from the JSONL and compared field-by-field; presentation ordering and whitespace are ignored but typed facts must match.
- `cass.cql_types.jsonl.manifest_report_generation` — Manifest report generation self-test
  - Normalization: The canonical JSONL model feeds the manifest report generator; the comparator asserts the rendered report rows match the typed value model so report drift is caught against the published golden.
- `cass.cql_types.jsonl.no_placeholder_references` — No-placeholder-reference enforcement self-test
  - Normalization: The comparator refuses to treat placeholder or absent reference paths as a pass; a missing or empty reference JSONL fails loud, guaranteeing parity scenarios cannot false-pass on unpublished goldens.
- `cass.cql_types.jsonl.schema_aware_normalization` — Schema-aware JSONL normalization self-test
  - Normalization: Values are normalized against the declared schema type (not guessed) so that ascii vs text, frozen vs multicell, and numeric widths are compared with the correct typed model; ambiguous coercion is rejected.
- `cass.data_db_decode.row_cell_flags_and_vint` — Data.db row/cell flags and VInt decode parity
  - Normalization: Rows and cells are normalized to the sstabledump JSONL fact model (partition key, clustering, cell name/value, liveness, deletion) and compared field-by-field; presentation ordering and whitespace ignored.
- `cass.delta_scan.adjacent_ranges` — Delta-scan adjacent range tombstones (shared boundary markers)
  - Normalization: scan_delta range-delete records are reconstructed from sstabledump JSONL boundary markers (excl_end_incl_start / incl_end_excl_start) into synthetic end/start bound pairs and compared with a writetime tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.cell_tombstones` — Delta-scan cell tombstones (DELETE col)
  - Normalization: scan_delta cell-tombstone records are mapped to sstabledump JSONL deletion facts (deleted_at) and compared with a writetime tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.collection_ops` — Delta-scan collection operations (append/overwrite/element delete)
  - Normalization: scan_delta collection cell upserts and the complex-cell deletion markers emitted by SET append / overwrite / element removal are mapped to the sstabledump JSONL collection cells and compared with a writetime tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.partial_updates` — Delta-scan partial updates (UPDATE-only rows, no row liveness)
  - Normalization: scan_delta cell upserts are mapped to sstabledump JSONL per-cell records and compared; row-liveness presence (INSERT) vs absence (UPDATE-only) is asserted against the JSONL liveness_info markers.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.partition_tombstones` — Delta-scan partition tombstones (DELETE FROM ... WHERE pk)
  - Normalization: scan_delta partition-delete records are mapped to the sstabledump JSONL partition deletion_info marker (marked_deleted) and compared with a writetime tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.range_tombstones` — Delta-scan range tombstones (DELETE ... WHERE ck range)
  - Normalization: scan_delta range-delete records are mapped to consecutive sstabledump JSONL range_tombstone_bound start/end pairs and compared with a writetime tolerance; boundary markers are paired into synthetic end/start bounds.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.row_tombstones` — Delta-scan row tombstones (DELETE FROM ... WHERE pk AND ck)
  - Normalization: scan_delta row-delete records are mapped to sstabledump JSONL row deletion_info markers and compared with a writetime tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.static_with_rows` — Delta-scan static columns alongside clustered rows
  - Normalization: scan_delta static-cell upserts are mapped to the sstabledump JSONL static row block and clustered-row cells to their per-row cell lists; both are compared by value and writetime with tolerance.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.ttl_cells` — Delta-scan TTL / expiring cells (INSERT ... USING TTL)
  - Normalization: scan_delta expiring-cell records are mapped to sstabledump JSONL cell ttl / local_deletion_time fields and compared; mixed live + expiring cells in the same fixture exercise both code paths.
  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).
- `cass.delta_scan.wide_partition_corpus` — Delta-scan over wide partitions (planned — no test_deltas fixture yet) _(planned — no evidence yet)_
  - Normalization: Planned: scan_delta facts over a wide partition (many clustered rows + range/cell tombstones in one partition, exercising index-block skipping) compared to the sstabledump JSONL golden.
- `cass.index_summary.big_index_offsets` — Index.db partition key digests and data offsets (BIG)
  - Normalization: Partition key digests and Data.db offsets resolved through Index.db are compared against the partition order and keys derived from sstabledump JSONL.
- `cass.schema_evolution.dropped_column.per_cell_purge` — Dropped regular column per-cell purge parity
  - Normalization: Cells for a dropped regular column are purged per-cell on read; the surviving cells and the dropped-column metadata in the SerializationHeader are mapped to the sstabledump JSONL and Statistics.db dump and compared.
- `cass.schema_evolution.issue_847_dropped_column_filter` — Dropped-column cells purged by per-cell timestamp and stripped from the output header
  - Normalization: Surviving keep_col cells read back via KWayMerger are compared by (clustering, value) to the oracle positions; the output header is decoded byte-derived and must not list the purged drop_col.
- `cass.schema_evolution.issue_850_static_presence` — Static column declared in input header but absent from schema is re-added on compaction
  - Normalization: The compacted output static-column set is decoded byte-derived and must contain the re-added column as static (never demoted to regular); the static cell presence is read back via KWayMerger.
- `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` — Compaction preserves static + strips fully-purged dropped columns in the output SerializationHeader
  - Normalization: The compacted output Statistics.db SerializationHeader column sets (regular / static) are decoded byte-derived and compared to the oracle's committed sstablemetadata golden dumps; presentation ignored.
- `cass.serialization.SerializationMirrorTest.schema_evolution_ordering` — Serialization-header column ordering survives compaction across schema evolution
  - Normalization: Surviving regular cells read back via KWayMerger are compared at the clustering-key granularity to the expected oracle positions; the output header column set is decoded byte-derived.
- `cass.sstable_io.reader.tombstone_only_partition` — Tombstone-only partition reader parity
  - Normalization: A generation containing only a partition-level deletion (no live rows) is read; its partition_deletion fact is mapped to the sstabledump JSONL fact and compared.
- `cass.sstable_io.scanner.tombstone_only_partition_ranges` — Tombstone-only partition range-scan parity
  - Normalization: A partition-range scan over the tombstone-only generation surfaces the partition deletion fact and no live rows; mapped to the sstabledump JSONL and compared.
- `cass.statistics_metadata.serialization_header` — Statistics.db metadata and serialization header parity
  - Normalization: Min/max timestamps, row count, partition count and serialization-header column types are compared against the Statistics.db.txt dump and sstabledump JSONL.
- `cass.tombstone_ttl.deletion_markers.cell_delete` — Cell-level deletion marker parity
  - Normalization: scan_delta cell-tombstone records (deletion_time localDeletionTime) are mapped to the sstabledump JSONL cell deletion_info fact and compared.
- `cass.tombstone_ttl.deletion_markers.partition_delete` — Partition-level deletion marker parity
  - Normalization: scan_delta partition-deletion records (markedForDeleteAt / localDeletionTime) are mapped to the sstabledump JSONL partition_deletion fact and compared.
- `cass.tombstone_ttl.deletion_markers.range_delete_bounds` — Range-tombstone bound deletion marker parity
  - Normalization: scan_delta range-tombstone start/end bound markers (inclusive/exclusive + clustering values) are mapped to the sstabledump JSONL range_tombstone_bound facts and compared.
- `cass.tombstone_ttl.deletion_markers.range_tombstone_boundary` — Adjacent range-tombstone boundary marker parity
  - Normalization: scan_delta adjacent range-tombstone boundary markers (the shared boundary between two adjacent ranges) are mapped to the sstabledump JSONL range_tombstone boundary facts and compared.
- `cass.tombstone_ttl.deletion_markers.row_delete` — Row-level deletion marker parity
  - Normalization: scan_delta row-deletion records (deletion_info markedForDeleteAt) are mapped to the sstabledump JSONL row deletion_info fact and compared.
- `cass.tombstone_ttl.gc_grace.partition_row_cell` — gc_grace read-merge parity for partition/row/cell tombstones
  - Normalization: Read-merge of gc_grace=0 vs gc_grace=864000 fixtures is asserted identical for partition, row, and cell tombstones; deletion facts are mapped to the sstabledump JSONL and compared. The partition sub-case is verified now that #1072 is fixed.
- `cass.tombstone_ttl.never_purge.cell_row_partition` — never_purge keeps cell/row/partition tombstones on read and compaction
  - Normalization: With never_purge semantics, cell, row, and partition tombstones are retained on read and across compaction; their deletion facts are mapped to the sstabledump JSONL and compared. The partition sub-case is verified now that #1072 is fixed.
- `cass.tombstone_ttl.range_tombstone.closed_last_block` — Range-tombstone closing in the last index block parity
  - Normalization: A range tombstone whose end bound lies in the last column-index block has its close marker mapped to the sstabledump JSONL range_tombstone close bound and compared.
- `cass.tombstone_ttl.range_tombstone.index_block_first_marker` — Range-tombstone first-of-index-block marker parity
  - Normalization: For a wide partition spanning multiple column-index blocks, the open marker emitted at the first marker of a block is mapped to the sstabledump JSONL range_tombstone open bound and compared.
- `cass.tombstone_ttl.range_tombstone.index_block_last_marker` — Range-tombstone last-of-index-block marker parity
  - Normalization: The close marker emitted at the last marker of a column-index block is mapped to the sstabledump JSONL range_tombstone close bound and compared.
- `cass.tombstone_ttl.range_tombstone.open_ended_middle_block` — Range-tombstone spanning interior index blocks parity
  - Normalization: The materialized fixture range [1500,2500] is a CLOSED range spanning interior column-index blocks (not open-ended); its open/close bounds and the synthetic per-block boundary markers are mapped to the sstabledump JSONL range_tombstone facts and compared. Covered here as an interior-block-spanning closed range.
- `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` — Partition delete from a skipped SSTable is reincluded on read
  - Normalization: The cross-generation MERGE read path now reincludes the gen-2 partition tombstone; the merged read is compared against the sstabledump JSONL of each generation. Verified end-to-end now that #1072 is fixed.
- `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows` — Skipped-SSTable partition delete shadows older live rows
  - Normalization: A gen-2 partition delete with a higher timestamp now shadows the gen-1 live rows for pk=1, leaving zero live rows; compared against the merged read semantics implied by the per-generation sstabledump JSONL. Verified now that #1072 is fixed.
- `cass.tombstone_ttl.static_row.with_row_cell_range_tombstones` — Static row alongside row/cell/range tombstones parity
  - Normalization: The static row liveness and the co-located row/cell/range tombstones are mapped to the sstabledump JSONL static_block and deletion facts and compared.
- `cass.tombstone_ttl.ttl_and_local_deletion_time` — TTL, local deletion time, and WRITETIME parity
  - Normalization: TTL, localDeletionTime and WRITETIME are compared against the ttl/expiresAt/tstamp facts emitted by sstabledump JSONL.
- `cass.tombstone_ttl.ttl_cells.local_deletion_time` — Expiring-cell localDeletionTime parity
  - Normalization: scan_delta expiring-cell ttl + localDeletionTime are mapped to the sstabledump JSONL ttl/expires_at cell facts and compared.
- `cass.tombstone_ttl.ttl_cells.mixed_expiring_and_live` — Mixed expiring and live cells in one row parity
  - Normalization: For rows mixing expiring and non-expiring cells, the per-cell ttl/expires_at presence is mapped to the sstabledump JSONL cell facts so that only the expiring cells carry liveness expiry.
- `cass.tombstone_ttl.ttl_expiry.gc_before_boundary` — TTL expiry before gc-grace boundary parity
  - Normalization: Expired-but-not-yet-gc-purged cells retain their tombstone/expiry facts on read; the localDeletionTime relative to the gc-grace boundary is mapped to the sstabledump JSONL facts and compared.
- `cass.verify.component_presence` — Verifier checks required component presence (TOC parity)
  - Normalization: The verifier enumerates TOC.txt components and confirms each required component file is present; the toc_missing_component corruption case is expected to be flagged. Decoded rows of the clean fixture are compared against the sstabledump JSONL.
- `cass.verify.compression_info_parse` — Verifier parses CompressionInfo.db before scanning compressed Data.db
  - Normalization: The verifier loads the chunk-offset table from CompressionInfo.db and uses it to position chunk reads; a bad-offset corruption case must fail. Decoded rows compared to the JSONL golden.
- `cass.verify.digest_crc32_match` — Verifier validates Digest.crc32 against recomputed Data.db CRC
  - Normalization: The verifier recomputes the Data.db CRC and matches it against the recorded Digest.crc32 on a clean fixture (pass) and the digest_crc32_mismatch corruption case (fail).
- `cass.verify.full_row_scan` — Verifier performs a full row scan and matches JSONL row count
  - Normalization: The verifier scans every partition/row and the decoded row count and values are compared against the sstabledump JSONL golden.
- `cass.verify.healthy_compressed_sstable` — Verifier passes a healthy compressed SSTable
  - Normalization: The verifier runs a full row scan on a clean compressed SSTable; the decoded row count/values are compared against the sstabledump JSONL and the verify pass must report healthy (no errors).
- `cass.verify.healthy_uncompressed_sstable` — Verifier passes a healthy uncompressed SSTable
  - Normalization: Full row scan on a clean uncompressed SSTable (no CompressionInfo.db); decoded rows compared against the sstabledump JSONL with a healthy pass.
- `cass.verify.inline_crc_validation` — Verifier validates Data.db inline per-chunk CRC during scan
  - Normalization: During the verify scan each chunk's inline CRC32 trailer is checked; a clean fixture passes and a bit-flip case fails. Decoded rows compared to the JSONL golden.
- `cass.verify.no_silent_empty_result_on_corruption` — Verifier never returns a silent empty result on corruption
  - Normalization: For every corruption-corpus case the verifier must surface an explicit error; returning an empty (zero-row) result on corrupted input is a contract violation. The clean fixture's full row scan defines the non-empty baseline.
- `cass.write_load_path.live_readback.semantic_only` — Live Cassandra readback of CQLite-written SSTables (semantic-only)
  - Normalization: CQLite-written artifacts are loaded into Cassandra 5.0.2 and read back; the sstabledump JSON of the loaded SSTable and the cqlsh SELECT rows (including TTL(col), static columns, clustering rows, and tombstone presence/absence) are compared for semantic equivalence to what CQLite wrote. This proves loaded rows, TTLs, tombstones, static rows and clustering rows are semantically visible — NOT a byte comparison of writer output.

## Smoke-only scenarios

- `cass.cli_reporting.parity_manifest_lint_and_report` — Parity manifest lint and report tooling
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1)
- `cass.data_db_decode.row_preamble_size_mismatch` — Malformed row-preamble size fails loud
- `cass.filter_db.statistical_false_positive_rate` — Filter.db empirical false-positive-rate report _(planned — no evidence yet)_
- `cass.schema_evolution.serialization_header_column_order` — Serialization-header column order across schema evolution
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader

## Gaps and next steps

- `cass.compaction.SSTableRewriterTest.output_component_integrity` (planned): Byte-for-byte compaction output parity is computed and reported but not gated; the writer is not yet byte-identical to Cassandra. → _Land the divergence-fix children (#844/#846/#848 …), then drop continue-on-error on the byte step to promote it to a hard gate._
- `cass.compaction.harness_byte_tier_artifacts` (planned): No gated byte-for-byte comparison; the tier reports diffs + artifacts but does not fail the build yet. → _Promote to a hard gate (drop continue-on-error) once compaction output is byte-stable across the scenario matrix._
- `cass.compaction_merge.byte_for_byte_output` (planned): No gated byte-for-byte comparison of compaction output. → _Promote the debug byte tier in compaction-parity to a gated comparison once writer output is byte-stable._
- `cass.compression_checksum.checksum_trailer_detection` (partial): No gated byte comparison of Digest.crc32 against the Cassandra reference. → _Add a Digest.crc32 byte comparison to the sstable_parity_corruption_verify suite._
- `cass.compression_info.deflate.real_fixture_chunks` (planned): No real DeflateCompressor CompressionInfo.db / Data.db fixture in the committed corpus, so chunk + CRC parity cannot be byte-compared. → _Generate a DeflateCompressor SSTable via regenerate-datasets.sh and let the existing test exercise it (the codec dispatch already handles it)._
- `cass.compression_info.deflate.real_fixture_chunks.strict` (planned): No real Cassandra Deflate-compressed fixture in the corpus. → _Generate a DeflateCompressor SSTable fixture via issue #996 (epic #970) and add it to the dataset; the strict lane will then decode and round-trip it._
- `cass.compression_info.zstd.real_fixture_chunks` (planned): No real ZstdCompressor CompressionInfo.db / Data.db fixture in the committed corpus, so chunk + CRC parity cannot be byte-compared. → _Generate a ZstdCompressor SSTable via regenerate-datasets.sh and let the existing test exercise it (the codec dispatch already handles it)._
- `cass.compression_info.zstd.real_fixture_chunks.strict` (planned): No real Cassandra Zstd-compressed fixture in the corpus (non-dictionary). → _Generate a non-dictionary ZstdCompressor SSTable fixture via issue #996 (epic #970); the strict lane will then decode and round-trip it._
- `cass.corruption.bti_partitions_footer_bit_flip` (planned): Clean BTI source (test_da/wide_table Partitions.db) is not git-tracked, so the corrupted fixture cannot be regenerated by CI. → _Commit the clean test_da/wide_table BTI components (or add them to the published dataset bundle) so generate-corruption-corpus.sh can emit the corrupted Partitions.db, then flip status to mirrored._
- `cass.corruption.bti_rows_truncation` (planned): Clean BTI source (test_da/wide_table Rows.db) is not git-tracked, so the truncated fixture cannot be regenerated by CI. → _Commit the clean test_da/wide_table BTI components so generate-corruption-corpus.sh can emit the truncated Rows.db, then flip status to mirrored._
- `cass.corruption_verify.component_corruption_detection` (planned): No scrub/verify parity pass implemented. → _Implement a verify pass and compare detected-corruption outcomes against Cassandra VerifyTest/ScrubTest scenarios._
- `cass.data_db_decode.range_tombstone.boundary_markers` (partial): Boundary-marker (kind 2 / kind 5, two deletion-time pairs) byte parity is asserted on the READ side against the real adjacent_ranges fixture, which is now PINNED in the CI dataset (v3.2) and fail-closed under `CQLITE_REQUIRE_FIXTURES=1` / `CQLITE_PARITY_REQUIRE_DATASETS=1` (issue #1205), so the read lane IS CI-resident. The remaining gap is solely a WRITER lane: CQLite's writer never EMITS a boundary marker (it writes separate start/end BOUND pairs), so no deterministic writer round-trip can cover the two-deletion-time body — hence `partial`, not `mirrored`. → _Promote to strict-byte `mirrored` by teaching the writer to coalesce adjacent ranges into a boundary marker, then add a deterministic writer round-trip lane that emits + re-decodes the two-deletion-time form. (The former dataset-absence blocker is resolved — the fixture is pinned and the read lane fail-closes.)_
- `cass.data_db_decode.wide_partition.row_boundaries` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.delta_scan.wide_partition_corpus` (planned): No test_deltas wide-partition delete fixture; wide partitions are produced by the wide-row corpus (epic #993), not generate-deltas.sh. Byte-for-byte backing for delta_scan remains tracked by epic #969. → _Add a wide-partition delete shape (or reuse a wide-row corpus fixture) and a paired test_delta_parity_wide_partition test under epic #993._
- `cass.filter_db.bti_membership` (partial): No raw-partition-key source for BTI fixtures, so the no-false-negative probe cannot run against da Filter.db. → _Recover raw BTI partition keys (e.g. by decoding partitions during a Data.db scan) and extend the no-false-negative gate to cover da fixtures._
- `cass.filter_db.statistical_false_positive_rate` (planned): No gated comparison of measured FPR against Cassandra's configured bloom_filter_fp_chance. → _Add larger-cardinality fixtures and assert the measured FPR tracks the configured fp_chance within a documented statistical tolerance._
- `cass.filter_db_bloom.serialization_no_false_negative` (partial): No no-false-negative parity assertion against Cassandra Filter.db. → _Add a Filter.db serialization parity test asserting zero false negatives across the present-key set._
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.index_db.big.wide_partition_promoted_entries` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.index_db.promoted_index.clustering_bounds` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.index_db.promoted_index.index_info_offsets` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.index_db.promoted_index.range_tombstone_boundary_at_block_edge` (partial): Byte-level promoted-index parity (Index.db offsets, widths, clustering bounds) runs only against local-only binaries that are not in the pinned CI dataset; the required lane enforces only the committed-JSONL canonical-semantic facts. → _Add the test_big.wide_partition binaries to the dataset release pin (regenerate the tarball + update DATASET_SHA256) to promote these scenarios to byte_for_byte enforced in the required_parity lane._
- `cass.index_summary.column_index.range_tombstone_boundary_big_bti` (partial): BTI (da) range-tombstone-at-block-edge fixtures are not yet generated (no da tombstone generator). → _Add a da/BTI wide-partition range-tombstone fixture generator and assert BTI column-index boundary parity; file a follow-up issue._
- `cass.index_summary.summary_boundaries` (partial): Cassandra Summary.db reference dumps not published for all tables. → _Publish Summary.db reference dumps and enable strict first/last-key boundary comparison in the sstable_parity_summary_db_big suite._
- `cass.repaired_metadata.statistics_db.pending_repair_uuid` (planned): No Cassandra 5.0 pending-repair fixture available; the reference null (`Pending repair: --`) state is confirmed, and the read path reports the field as Unparsed (it is not walked from bytes) rather than a fabricated absent value. → _When a pending-repair fixture is generated, decode the pendingRepair UUID (type-aware skip past improvedMinMax + commitLogIntervals) — promoting the field from RepairField::Unparsed to Decoded — and assert it byte-for-byte against the `Pending repair: <uuid>` reference line._
- `cass.repaired_metadata.statistics_db.transient_repair_flag` (planned): No transiently-replicated fixture available; the reference `IsTransient: false` state is confirmed, and the read path reports the field as Unparsed (it is not walked from bytes) rather than a fabricated `false`. → _When a transient-replication fixture is generated, decode the isTransient flag (after the version-gated improvedMinMax block and commitLogIntervals) — promoting it from RepairField::Unparsed to Decoded — and assert it byte-for-byte against the `IsTransient: true` reference line._
- `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` (partial): A wide dropped-column empty-index-block reverse-scan fixture is not yet generated. → _Generate a wide dropped-column fixture that yields an empty index block under reverse scan and assert parity; file a follow-up issue._
- `cass.sstable_scan.wide_partition.forward_reverse_bounds` (partial): Reverse SSTable iteration is not implemented for BIG wide partitions; only a post-fetch in-memory ORDER BY DESC sort exists, which does not exercise reverse promoted-index block decoding. → _Implement a BIG-format reverse partition iterator that uses the promoted IndexInfo blocks to seek/decode blocks back-to-front (mirroring Cassandra SSTableReversedIterator), then assert forward and reverse scans of pk=1 return the identical 290-row clustering set with no rows lost adjacent to the deleted block._
- `cass.statistics_db.SSTableMetadataTest.max_local_deletion_time` (planned): STATS-section max timestamp / max local-deletion-time not yet decoded. → _Decode the STATS MetadataType component and assert max timestamp / max local-deletion-time against the reference dump._
- `cass.statistics_db.clustering_key_bounds` (planned): Covered-clustering min/max bounds not yet decoded from the STATS component. → _Decode the STATS-section clustering bounds and compare against the "Covered clusterings" reference line._
- `cass.statistics_db.histograms_and_estimates` (planned): STATS-section histograms and partition/row estimates not yet decoded. → _Decode the STATS-section EstimatedHistograms and count estimates and compare bucket boundaries against the reference dump._
- `cass.summary_db.IndexSummaryRedistributionTest.downsampled_summary_entries` (planned): No downsampled (sampling_level < 128) Summary.db fixture exists. → _Publish a redistributed Summary.db fixture and extend the strict suite to assert downsampled offset tables and size_at_full_sampling > entry count._
- `cass.tombstone_ttl.range_tombstone_boundaries` (partial): test_deltas dataset asset not published/enforced in CI (#701). → _Publish the test_deltas dataset and enforce scan_delta parity in CI._
- `cass.tombstone_ttl.repaired_unrepaired_purge_gate` (partial): repairedAt / pendingRepair parsing is not implemented (gated on #968/#988), so the repaired-vs-unrepaired purge gate is only partially exercised. → _Parse repairedAt / pendingRepair from Statistics.db and gate purge on repair status (#968/#988)._
- `cass.write_load_path.flush.tombstone_and_ttl_artifacts` (partial): The static-row writer gap (row-level HAS_TIMESTAMP / PK liveness wrongly set on a static-only UPDATE) is now RESOLVED (issue #1196): CQLite emits the static block byte-identical to Cassandra (flags 0xa0, static cell carries its own explicit timestamp), verified file-vs-file against the committed test_writeparity.static_clustering_shape reference. Whole-artifact byte parity for the broader tombstone/TTL shape REMAINS blocked by wall-clock-derived localDeletionTime on TTL/DELETE cells (nowInSeconds, not reproducible across independent writers). → _The remaining blocker is intrinsic: TTL/DELETE cells carry a wall-clock-derived localDeletionTime (nowInSeconds) that cannot byte-match an independent writer, so they stay semantic-only by nature. A future upgrade to byte_for_byte would require a non-TTL static-tombstone reference whose deletion time is deterministic (explicit localDeletionTime) plus a strict Data.db byte diff._

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

### `not_sstable_reader_writer_compactor`

- `cass.summary_db.IndexSummaryManagerTest.memory_constrained_summary_reload` — Runtime index-summary redistribution / memory-constrained reload
  - Safe wording: CQLite reads any Summary.db Cassandra wrote (including downsampled ones, pending a fixture); it does not reproduce the redistribution scheduler.

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

### `unsupported_compression_dictionary`

- `cass.zstd_dictionary.dictionary_assisted_decompression` — Zstd dictionary-assisted decompression (out of scope)
  - Safe wording: Plain Zstd decompression parity is claimed; dictionary-assisted decode is explicitly not.
- `cass.zstd_dictionary.dictionary_cache_reuse` — Zstd dictionary cache reuse across readers (out of scope)
  - Safe wording: Plain Zstd reuse is unaffected; no dictionary cache is claimed.
- `cass.zstd_dictionary.dictionary_checksum` — Zstd dictionary checksum validation (out of scope)
  - Safe wording: Plain Zstd checksum parity is covered; dictionary checksum is not claimed.
- `cass.zstd_dictionary.dictionary_ref_counting` — Zstd dictionary reference counting (out of scope)
  - Safe wording: CQLite does not claim Zstd dictionary write support.
- `cass.zstd_dictionary.dictionary_serialization` — Zstd dictionary serialization in SSTable metadata (out of scope)
  - Safe wording: CQLite supports plain Zstd compression parity; it does not claim Zstd dictionary support.
- `cass.zstd_dictionary.invalid_dictionary_rejected` — Invalid Zstd dictionary rejection (out of scope)
  - Safe wording: No Zstd dictionary validation is claimed.

## CI workflow mapping

| Scenario | CI tier | Workflow |
|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.bti_big_version_matrix.bti_da_write_read` | nightly_docker | .github/workflows/e2e-readback.yml |
| `cass.cli_reporting.parity_manifest_lint_and_report` | fast_pr | .github/workflows/cassandra-parity.yml |
| `cass.commitlog_replay.recovery_out_of_scope` | fast_pr | — |
| `cass.compaction.CompactionAwareWriterTest.live_row_count_preservation` | required_parity | .github/workflows/live-cell-compaction-parity.yml |
| `cass.compaction.CompactionAwareWriterTest.row_count_and_order_preservation` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction.CompactionIteratorTest.differential_compaction_loop` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction.CompactionIteratorTest.live_partition_merge` | required_parity | .github/workflows/live-cell-compaction-parity.yml |
| `cass.compaction.CompactionSimpleValueMergeTest.static_row_merge` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction.GcCompactionTest.static_and_complex_columns_survive_gc` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction.LongCompactionsTest.live_rows_lww_overlap` | required_parity | .github/workflows/live-cell-compaction-parity.yml |
| `cass.compaction.SSTableRewriterTest.output_component_integrity` | nightly_docker | .github/workflows/compaction-parity.yml |
| `cass.compaction.harness_byte_tier_artifacts` | nightly_docker | .github/workflows/compaction-parity.yml |
| `cass.compaction.harness_logical_tier` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction.issue_899_per_element_collection_compaction` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction.live_cells_clustering_lww` | required_parity | .github/workflows/live-cell-compaction-parity.yml |
| `cass.compaction.live_cells_no_clustering` | required_parity | .github/workflows/live-cell-compaction-parity.yml |
| `cass.compaction_merge.GcCompactionTest.row_cell_partition_tombstone_gc` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.byte_for_byte_output` | manual_debug | — |
| `cass.compaction_merge.issue_819.differential_input_merge_write_fidelity` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.issue_819.differential_row_tombstone_wide_partition_regression` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.load_path_validity` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.partial_source_retains_tombstones` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.tombstone_ttl_shadowing` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compression.fixture_matrix.deflate` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.incompressible_uncompressed_chunk` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.lz4` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.short_final_chunk` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.snappy` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.uncompressed_table` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.fixture_matrix.zstd_no_dictionary` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.compression.registry.known_deflate` | fast_pr | — |
| `cass.compression.registry.known_lz4` | fast_pr | — |
| `cass.compression.registry.known_snappy` | fast_pr | — |
| `cass.compression.registry.known_zstd` | fast_pr | — |
| `cass.compression.registry.uncompressed_disabled` | fast_pr | — |
| `cass.compression.registry.unknown_algorithm_rejected` | fast_pr | — |
| `cass.compression.registry.unsupported_options_rejected` | fast_pr | — |
| `cass.compression_checksum.checksum_trailer_detection` | fast_pr | — |
| `cass.compression_checksum.chunk_offsets_and_crc` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.deflate.real_fixture_chunks` | manual_debug | — |
| `cass.compression_info.deflate.real_fixture_chunks.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.fields.algorithm_name` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.fields.chunk_length` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.fields.chunk_offsets` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.fields.data_length` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.fields.max_compressed_length` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.fields.options` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.layout.no_crc_fields` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.compression_info.lz4.real_fixture_chunks` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.lz4.real_fixture_chunks.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.snappy.real_fixture_chunks` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.snappy.real_fixture_chunks.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.zstd.real_fixture_chunks` | manual_debug | — |
| `cass.compression_info.zstd.real_fixture_chunks.strict` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.corruption.bti_partitions_footer_bit_flip` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.bti_rows_truncation` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.compression_info.bad_offset` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.data_db.bit_flip` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.data_db.truncation` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.digest_crc32_mismatch` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.index_db.bit_flip_big` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.statistics_db.header_damage` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.summary_db_truncation` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption.toc_missing_component` | exhaustive_regeneration | .github/workflows/compression-corruption-parity.yml |
| `cass.corruption_verify.component_corruption_detection` | manual_debug | — |
| `cass.cql_types.boundaries.absent_vs_null_regular_columns` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.boundaries.empty_collections` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.boundaries.length_prefix_edges` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.boundaries.null_empty_text_blob` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.collections.map_key_lookup_offsets` | fast_pr | — |
| `cass.cql_types.collections.map_key_range_offsets` | fast_pr | — |
| `cass.cql_types.collections.set_lookup_offsets` | fast_pr | — |
| `cass.cql_types.collections.set_range_offsets` | fast_pr | — |
| `cass.cql_types.collections.single_cell_multicell_equivalence` | fast_pr | — |
| `cass.cql_types.collections.vint_element_count_boundaries` | fast_pr | — |
| `cass.cql_types.complex.frozen_udt_value` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.complex.legacy_dropped_tuple_udt_fields` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.complex.multicell_udt_collection_paths` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.complex.nested_frozen_collections` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.complex.tuple_field_order` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.complex.udt_field_order_null_empty` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.counters.canonical_jsonl_value` | nightly_docker | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.counters.compacted_final_value` | nightly_docker | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.counters.deleted_counter_shadowing` | nightly_docker | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.counters.multi_sstable_increment_decrement_merge` | nightly_docker | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.counters.single_sstable_context_decode` | nightly_docker | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.jsonl.canonical_value_comparator` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.jsonl.manifest_report_generation` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.jsonl.no_placeholder_references` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.jsonl.schema_aware_normalization` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.cql_types.primitives.fixed_width_vectors` | fast_pr | — |
| `cass.cql_types.primitives.invalid_length_rejection` | fast_pr | — |
| `cass.cql_types.primitives.temporal_vectors` | fast_pr | — |
| `cass.cql_types.primitives.text_blob_ascii_vectors` | fast_pr | — |
| `cass.cql_types.primitives.uuid_inet_vectors` | fast_pr | — |
| `cass.cql_types.primitives.varint_decimal_duration_vectors` | fast_pr | — |
| `cass.data_db.inline_crc.bad_trailer_rejected` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.data_db.inline_crc.incompressible_uncompressed_chunk` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.data_db.inline_crc.offset_delta_minus_crc_length` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.data_db.inline_crc.short_final_chunk` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.data_db.inline_crc.valid_trailer` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.data_db_decode.clustering_bounds.desc_order` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.clustering_bounds.multi_column_prefix` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.clustering_bounds.null_vs_empty` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.range_tombstone.bound_markers` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.range_tombstone.boundary_markers` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.row_cell_flags_and_vint` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.row_preamble_size_mismatch` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.data_db_decode.serialization_header.timestamp_ttl_ldt_deltas` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.serialization_mirror.multi_clustering_column_order` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.static_rows.static_only_partition` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.static_rows.static_with_clustering_rows` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.tombstone.cell_deletion_time` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.tombstone.partition_deletion_time` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.tombstone.row_deletion_time` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.ttl.local_deletion_time_delta` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.unfiltered_serializer.row_and_cell_flags` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.unfiltered_serializer.row_size_vints` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.data_db_decode.wide_partition.row_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.delta_scan.adjacent_ranges` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.cell_tombstones` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.collection_ops` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.partial_updates` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.partition_tombstones` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.range_tombstones` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.row_tombstones` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.static_with_rows` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.ttl_cells` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.delta_scan.wide_partition_corpus` | exhaustive_regeneration | .github/workflows/delta-roundtrip.yml |
| `cass.distributed_consensus.paxos_accord_out_of_scope` | fast_pr | — |
| `cass.filter_db.bti_membership` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.filter_db.corruption_fails_closed` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.filter_db.no_false_negative_membership` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.filter_db.serialization_round_trip` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.filter_db.statistical_false_positive_rate` | manual_debug | — |
| `cass.filter_db_bloom.serialization_no_false_negative` | fast_pr | — |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.SSTableScannerTest.range_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.big.raw_partition_keys_and_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.big.wide_partition_promoted_entries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.bti.index_component_discovery` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.promoted_index.clustering_bounds` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.promoted_index.index_info_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_db.promoted_index.range_tombstone_boundary_at_block_edge` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_summary.big_index_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.index_summary.column_index.range_tombstone_boundary_big_bti` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.index_summary.summary_boundaries` | fast_pr | — |
| `cass.nodetool_jmx_metrics.operational_out_of_scope` | fast_pr | — |
| `cass.read_repair_coordinator.out_of_scope` | fast_pr | — |
| `cass.repair_coordinator.anti_entropy_out_of_scope` | fast_pr | — |
| `cass.repaired_metadata.statistics_db.pending_repair_uuid` | manual_debug | — |
| `cass.repaired_metadata.statistics_db.repaired_at_field` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.repaired_metadata.statistics_db.transient_repair_flag` | manual_debug | — |
| `cass.repaired_metadata.statistics_db.write_roundtrip` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.sai_sasi_query.secondary_index_out_of_scope` | fast_pr | — |
| `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.schema_evolution.dropped_column.per_cell_purge` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.schema_evolution.issue_847_dropped_column_filter` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.schema_evolution.issue_850_static_presence` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.schema_evolution.serialization_header.altered_column_type` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header.altered_then_dropped_column` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header.dropped_column_same_type` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header.no_schema_change` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header.static_regular_kind_mismatch` | required_parity | .github/workflows/cql-type-parity.yml |
| `cass.schema_evolution.serialization_header_column_order` | fast_pr | — |
| `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.serialization.SerializationMirrorTest.schema_evolution_ordering` | required_parity | .github/workflows/tombstone-ttl-parity.yml |
| `cass.sstable_format.descriptor_component_resolution` | fast_pr | — |
| `cass.sstable_format.toc_component_manifest` | fast_pr | — |
| `cass.sstable_io.reader.tombstone_only_partition` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.sstable_scan.wide_partition.forward_reverse_bounds` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.MetadataSerializerTest.metadata_components` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.SSTableMetadataTest.max_local_deletion_time` | manual_debug | — |
| `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.SerializationMirrorTest.column_ordering_metadata` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.clustering_key_bounds` | manual_debug | — |
| `cass.statistics_db.core_metadata_checksums` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_db.histograms_and_estimates` | manual_debug | — |
| `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.statistics_metadata.serialization_header` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.statistics_metadata.tombstone_histogram.deletion_times` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.streaming_protocol.node_lifecycle_out_of_scope` | fast_pr | — |
| `cass.summary_db.IndexSummaryManagerTest.memory_constrained_summary_reload` | manual_debug | — |
| `cass.summary_db.IndexSummaryRedistributionTest.downsampled_summary_entries` | manual_debug | — |
| `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.summary_db.IndexSummaryTest.offset_table_entries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.summary_db.IndexSummaryTest.serialization_round_trip` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.summary_db.big.index_offset_references` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.summary_db.bti.summary_discovery_classification` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.tombstone_ttl.NeverPurgeTest.preserve_all_tombstone_types` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.tombstone_ttl.RangeTombstoneTest.marker_merge_and_persistence` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.tombstone_ttl.TTLExpiryTest.gc_boundary` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.tombstone_ttl.deletion_markers.cell_delete` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.deletion_markers.partition_delete` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.deletion_markers.range_delete_bounds` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.deletion_markers.range_tombstone_boundary` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.deletion_markers.row_delete` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.gc_grace.partition_row_cell` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.never_purge.cell_row_partition` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.range_tombstone.closed_last_block` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.range_tombstone.index_block_first_marker` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.range_tombstone.index_block_last_marker` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.range_tombstone.open_ended_middle_block` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.range_tombstone_boundaries` | required_parity | .github/workflows/delta-roundtrip.yml |
| `cass.tombstone_ttl.repaired_unrepaired_purge_gate` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.static_row.dropped_static_header_preserved` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.static_row.with_row_cell_range_tombstones` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.tombstone_ttl.ttl_cells.local_deletion_time` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.ttl_cells.mixed_expiring_and_live` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.tombstone_ttl.ttl_expiry.gc_before_boundary` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.verify.component_presence` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.compression_info_parse` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.digest_crc32_match` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.full_row_scan` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.healthy_compressed_sstable` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.healthy_uncompressed_sstable` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.inline_crc_validation` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.verify.no_silent_empty_result_on_corruption` | required_parity | .github/workflows/cassandra-parity.yml |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | required_parity | .github/workflows/cassandra-validation.yml |
| `cass.write_load_path.cql_sstable_writer.finished_data_db_artifacts` | exhaustive_regeneration | .github/workflows/cassandra-parity.yml |
| `cass.write_load_path.flush.partition_boundary_artifacts` | exhaustive_regeneration | .github/workflows/cassandra-parity.yml |
| `cass.write_load_path.flush.tombstone_and_ttl_artifacts` | exhaustive_regeneration | .github/workflows/cassandra-parity.yml |
| `cass.write_load_path.live_readback.semantic_only` | nightly_docker | .github/workflows/cassandra-validation.yml |
| `cass.zstd_dictionary.dictionary_assisted_decompression` | fast_pr | — |
| `cass.zstd_dictionary.dictionary_cache_reuse` | fast_pr | — |
| `cass.zstd_dictionary.dictionary_checksum` | fast_pr | — |
| `cass.zstd_dictionary.dictionary_ref_counting` | fast_pr | — |
| `cass.zstd_dictionary.dictionary_serialization` | fast_pr | — |
| `cass.zstd_dictionary.invalid_dictionary_rejected` | fast_pr | — |

## Fixture and reference mapping

| Scenario | Storage fmt | References / failure artifacts |
|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | nb, oa | test-data/datasets/sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Data.db.jsonl |
| `cass.bti_big_version_matrix.bti_da_write_read` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Data.db.jsonl |
| `cass.cli_reporting.parity_manifest_lint_and_report` | — | — |
| `cass.commitlog_replay.recovery_out_of_scope` | — | — |
| `cass.compaction.CompactionAwareWriterTest.live_row_count_preservation` | nb | test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Data.db.jsonl |
| `cass.compaction.CompactionAwareWriterTest.row_count_and_order_preservation` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compaction.CompactionIteratorTest.differential_compaction_loop` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compaction.CompactionIteratorTest.live_partition_merge` | nb | test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Data.db.jsonl |
| `cass.compaction.CompactionSimpleValueMergeTest.static_row_merge` | nb | — |
| `cass.compaction.GcCompactionTest.static_and_complex_columns_survive_gc` | nb | test-data/datasets/sstables/test_collections |
| `cass.compaction.LongCompactionsTest.live_rows_lww_overlap` | nb | test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Data.db.jsonl |
| `cass.compaction.SSTableRewriterTest.output_component_integrity` | nb | compaction-parity/build/parity-artifacts-byteParity/<scenario>/cassandra-output/<br>_fail:_ byte-diff.txt: first byte/offset diff per component (component, offset, ref byte, candidate byte, lengths), checksums.txt: SHA-256 of every component on both sides, cassandra-output/ and cqlite-output/: the full component dirs for offline decoding |
| `cass.compaction.harness_byte_tier_artifacts` | nb | compaction-parity/build/parity-artifacts-byteParity/<scenario>/cassandra-output/<br>_fail:_ byte-diff.txt: first differing byte/offset per component, checksums.txt: SHA-256 per component, both engines, commands.txt: exact cqlite compact + sstabledump command lines, cqlite-compact.stdout / cqlite-compact.stderr |
| `cass.compaction.harness_logical_tier` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compaction.issue_899_per_element_collection_compaction` | nb | test-data/datasets/sstables/test_collections<br>test-data/datasets/sstables/test_deltas/collection_ops-2a5006f06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.compaction.live_cells_clustering_lww` | nb | test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Data.db<br>test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Data.db.jsonl<br>test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Index.db<br>test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Summary.db<br>test-data/datasets/sstables/test_compactionparity/live_clustering-e094a78073a611f1b17b3da6654e7580/nb-3-big-Digest.crc32<br>_fail:_ panic diff: CQLite-compacted vs Cassandra-compacted component (cass len + ours len + first-diff byte index + full hex of both) for Data.db / Index.db / Summary.db / Digest.crc32; CRC.db prefix + trailing empty-chunk check; TOC component-set delta; JSONL partition-count + LWW-survivor assertion |
| `cass.compaction.live_cells_no_clustering` | nb | test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Data.db<br>test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Data.db.jsonl<br>test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Index.db<br>test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Summary.db<br>test-data/datasets/sstables/test_compactionparity/live_no_clustering-e08194b073a611f1b17b3da6654e7580/nb-3-big-Digest.crc32<br>_fail:_ panic diff: CQLite-compacted vs Cassandra-compacted component (cass len + ours len + first-diff byte index + full hex of both) for Data.db / Index.db / Summary.db / Digest.crc32; CRC.db prefix + trailing empty-chunk check; TOC component-set delta; JSONL partition-count + LWW-survivor assertion |
| `cass.compaction_merge.GcCompactionTest.row_cell_partition_tombstone_gc` | nb | test-data/datasets/sstables/test_deltas/cell_tombstones-29733830701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl |
| `cass.compaction_merge.byte_for_byte_output` | — | — |
| `cass.compaction_merge.issue_819.differential_input_merge_write_fidelity` | nb | test-data/datasets/sstables/test_deltas/cell_tombstones-29733830701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl |
| `cass.compaction_merge.issue_819.differential_row_tombstone_wide_partition_regression` | nb | test-data/datasets/sstables/test_deltas/row_tombstones-297f1f10701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl |
| `cass.compaction_merge.load_path_validity` | nb | — |
| `cass.compaction_merge.partial_source_retains_tombstones` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | nb | test-data/datasets/sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.compaction_merge.tombstone_ttl_shadowing` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compression.fixture_matrix.deflate` | nb | test-data/datasets/sstables/test_comp/deflate_table-2592698071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/deflate_table-2592698071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-deflate.log |
| `cass.compression.fixture_matrix.incompressible_uncompressed_chunk` | nb | test-data/datasets/sstables/test_comp/incompressible_uncompressed_chunk-25b8dd4071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/incompressible_uncompressed_chunk-25b8dd4071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-incompressible.log |
| `cass.compression.fixture_matrix.lz4` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-lz4.log |
| `cass.compression.fixture_matrix.short_final_chunk` | nb | test-data/datasets/sstables/test_comp/short_final_chunk-25aef23071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/short_final_chunk-25aef23071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-short-final-chunk.log |
| `cass.compression.fixture_matrix.snappy` | nb | test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-snappy.log |
| `cass.compression.fixture_matrix.uncompressed_table` | nb | test-data/datasets/sstables/test_comp/uncompressed_table-25a5ca7071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-uncompressed.log |
| `cass.compression.fixture_matrix.zstd_no_dictionary` | nb | test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-fixture-zstd.log |
| `cass.compression.registry.known_deflate` | nb | test-data/datasets/sstables/test_comp/deflate_table-2592698071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/deflate_table-2592698071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.known_lz4` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.known_snappy` | nb | test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.known_zstd` | nb | test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.uncompressed_disabled` | nb | test-data/datasets/sstables/test_comp/uncompressed_table-25a5ca7071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.unknown_algorithm_rejected` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression.registry.unsupported_options_rejected` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.compression_checksum.checksum_trailer_detection` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Digest.crc32<br>_fail:_ target/cassandra-parity/checksum-mismatch.log |
| `cass.compression_checksum.chunk_offsets_and_crc` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-truncated-chunk.log |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>_fail:_ target/cassandra-parity/compression-corruption-accepted.log |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-offsets-mismatch.log |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>_fail:_ target/cassandra-parity/compression-info-offsets-mismatch.log |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-boundaries-mismatch.log |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-chunk-boundary-mismatch.log |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-metadata-mismatch.log |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-TOC.txt<br>test-data/datasets/sstables/test_da/collection_table-de2c155064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>_fail:_ target/cassandra-parity/compression-info-roundtrip-mismatch.log |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-crc-mismatch.log |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation.strict` | nb, oa, da | test-data/datasets/sstables/test_da/collection_table-de2c155064e711f19ad401a8c8227b11/da-2-bti-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-inline-crc-mismatch.log |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-round-trip-mismatch.log |
| `cass.compression_info.deflate.real_fixture_chunks` | — | — |
| `cass.compression_info.deflate.real_fixture_chunks.strict` | — | — |
| `cass.compression_info.fields.algorithm_name` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/deflate_table-2592698071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-algorithm_name.log |
| `cass.compression_info.fields.chunk_length` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-chunk_length.log |
| `cass.compression_info.fields.chunk_offsets` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-chunk_offsets.log |
| `cass.compression_info.fields.data_length` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-data_length.log |
| `cass.compression_info.fields.max_compressed_length` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-max_compressed_length.log |
| `cass.compression_info.fields.options` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/zstd_table-259ca2b071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-options.log |
| `cass.compression_info.layout.no_crc_fields` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/compressioninfo-no_crc_fields.log |
| `cass.compression_info.lz4.real_fixture_chunks` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-lz4-mismatch.log |
| `cass.compression_info.lz4.real_fixture_chunks.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>_fail:_ target/cassandra-parity/compression-lz4-mismatch.log |
| `cass.compression_info.snappy.real_fixture_chunks` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-snappy-mismatch.log |
| `cass.compression_info.snappy.real_fixture_chunks.strict` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>_fail:_ target/cassandra-parity/compression-snappy-mismatch.log |
| `cass.compression_info.zstd.real_fixture_chunks` | — | — |
| `cass.compression_info.zstd.real_fixture_chunks.strict` | — | — |
| `cass.corruption.bti_partitions_footer_bit_flip` | da, bti | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>_fail:_ target/cassandra-parity/corruption-bti_partitions_footer_bit_flip.log |
| `cass.corruption.bti_rows_truncation` | da, bti | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>_fail:_ target/cassandra-parity/corruption-bti_rows_truncation.log |
| `cass.corruption.compression_info.bad_offset` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-compression_info_bad_offset.log |
| `cass.corruption.data_db.bit_flip` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-data_db_bit_flip.log |
| `cass.corruption.data_db.truncation` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-data_db_truncation.log |
| `cass.corruption.digest_crc32_mismatch` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-digest_crc32_mismatch.log |
| `cass.corruption.index_db.bit_flip_big` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-index_db_bit_flip_big.log |
| `cass.corruption.statistics_db.header_damage` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-statistics_db_header_damage.log |
| `cass.corruption.summary_db_truncation` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-summary_db_truncation.log |
| `cass.corruption.toc_missing_component` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/corruption/test_comp_corrupt/corruption-sha256.txt<br>_fail:_ target/cassandra-parity/corruption-toc_missing_component.log |
| `cass.corruption_verify.component_corruption_detection` | — | — |
| `cass.cql_types.boundaries.absent_vs_null_regular_columns` | nb | test-data/datasets/sstables/test_types/nb_absent_vs_null_regular-4fa69860706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.boundaries.empty_collections` | nb | test-data/datasets/sstables/test_types/nb_empty_collections-4faf9910706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.boundaries.length_prefix_edges` | nb | test-data/datasets/sstables/test_types/nb_length_prefix_edges-4fba4770706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.cql_types.boundaries.null_empty_text_blob` | nb | test-data/datasets/sstables/test_types/nb_null_empty_text_blob-4f9b26b0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.collections.map_key_lookup_offsets` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.collections.map_key_range_offsets` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.collections.set_lookup_offsets` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.collections.set_range_offsets` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.collections.single_cell_multicell_equivalence` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.collections.vint_element_count_boundaries` | nb | test-data/codec-vectors/collections.json<br>_fail:_ logs |
| `cass.cql_types.complex.frozen_udt_value` | nb | test-data/datasets/sstables/test_types/cx_frozen_udt_value-4fd68200706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.complex.legacy_dropped_tuple_udt_fields` | nb | test-data/datasets/sstables/test_types/cx_legacy_dropped_tuple_udt-4ff5a2c0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.complex.multicell_udt_collection_paths` | nb | test-data/datasets/sstables/test_types/cx_multicell_udt_collection_paths-4feb6990706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.complex.nested_frozen_collections` | nb | test-data/datasets/sstables/test_types/cx_nested_frozen_collections-4fe21ac0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.complex.tuple_field_order` | nb | test-data/datasets/sstables/test_types/cx_tuple_field_order-4fc4f5d0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.complex.udt_field_order_null_empty` | nb | test-data/datasets/sstables/test_types/cx_udt_field_order_null_empty-4fcd8150706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl |
| `cass.cql_types.counters.canonical_jsonl_value` | nb | test-data/datasets/sstables/test_types/ct_multi_sstable_merge-5007f240706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_types/ct_multi_sstable_merge-5007f240706211f197e20b846582ecc8/ct_multi_sstable_merge.counter-select.txt |
| `cass.cql_types.counters.compacted_final_value` | nb | test-data/datasets/sstables/test_types/ct_compacted_final_value-501f4ad0706211f197e20b846582ecc8/nb-3-big-Data.db.jsonl<br>test-data/datasets/sstables/test_types/ct_compacted_final_value-501f4ad0706211f197e20b846582ecc8/ct_compacted_final_value.counter-select.txt |
| `cass.cql_types.counters.deleted_counter_shadowing` | nb | test-data/datasets/sstables/test_types/ct_deleted_counter_shadowing-50114110706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_types/ct_deleted_counter_shadowing-50114110706211f197e20b846582ecc8/ct_deleted_counter_shadowing.counter-select.txt |
| `cass.cql_types.counters.multi_sstable_increment_decrement_merge` | nb | test-data/datasets/sstables/test_types/ct_multi_sstable_merge-5007f240706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_types/ct_multi_sstable_merge-5007f240706211f197e20b846582ecc8/ct_multi_sstable_merge.counter-select.txt |
| `cass.cql_types.counters.single_sstable_context_decode` | nb | test-data/datasets/sstables/test_types/ct_single_sstable-4fff8dd0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_types/ct_single_sstable-4fff8dd0706211f197e20b846582ecc8/ct_single_sstable.counter-select.txt |
| `cass.cql_types.jsonl.canonical_value_comparator` | — | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare` | — | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.cql_types.jsonl.manifest_report_generation` | — | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.cql_types.jsonl.no_placeholder_references` | — | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.cql_types.jsonl.schema_aware_normalization` | — | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.cql_types.primitives.fixed_width_vectors` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.cql_types.primitives.invalid_length_rejection` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.cql_types.primitives.temporal_vectors` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.cql_types.primitives.text_blob_ascii_vectors` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.cql_types.primitives.uuid_inet_vectors` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.cql_types.primitives.varint_decimal_duration_vectors` | nb | test-data/codec-vectors/primitives.json<br>_fail:_ logs |
| `cass.data_db.inline_crc.bad_trailer_rejected` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/inline-crc-bad_trailer_rejected.log |
| `cass.data_db.inline_crc.incompressible_uncompressed_chunk` | nb | test-data/datasets/sstables/test_comp/incompressible_uncompressed_chunk-25b8dd4071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/incompressible_uncompressed_chunk-25b8dd4071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/inline-crc-incompressible_uncompressed_chunk.log |
| `cass.data_db.inline_crc.offset_delta_minus_crc_length` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_comp/snappy_table-2588f3a071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>_fail:_ target/cassandra-parity/inline-crc-offset_delta_minus_crc_length.log |
| `cass.data_db.inline_crc.short_final_chunk` | nb | test-data/datasets/sstables/test_comp/short_final_chunk-25aef23071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/short_final_chunk-25aef23071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/inline-crc-short_final_chunk.log |
| `cass.data_db.inline_crc.valid_trailer` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-CompressionInfo.db.txt<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/inline-crc-valid_trailer.log |
| `cass.data_db_decode.clustering_bounds.desc_order` | nb, oa | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-desc-clustering-diff.log |
| `cass.data_db_decode.clustering_bounds.multi_column_prefix` | nb, oa | test-data/datasets/sstables/test_wide_rows/wide_partition_table-6d6d0f80a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-multi-clustering-diff.log |
| `cass.data_db_decode.clustering_bounds.null_vs_empty` | nb, oa | test-data/datasets/sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-null-empty-clustering-diff.log |
| `cass.data_db_decode.range_tombstone.bound_markers` | nb, oa | test-data/datasets/sstables/test_deltas/range_tombstones-298894f0701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-range-bound-diff.log |
| `cass.data_db_decode.range_tombstone.boundary_markers` | nb, oa | test-data/datasets/sstables/test_deltas/adjacent_ranges-29bdd5c0701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-range-boundary-diff.log |
| `cass.data_db_decode.row_cell_flags_and_vint` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.data_db_decode.row_preamble_size_mismatch` | nb | — |
| `cass.data_db_decode.serialization_header.timestamp_ttl_ldt_deltas` | nb, oa | test-data/datasets/sstables/test_basic/uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-delta-diff.log |
| `cass.data_db_decode.serialization_mirror.multi_clustering_column_order` | nb, oa | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-clustering-order-diff.log |
| `cass.data_db_decode.static_rows.static_only_partition` | nb, oa | test-data/datasets/sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-static-row-diff.log |
| `cass.data_db_decode.static_rows.static_with_clustering_rows` | nb, oa | test-data/datasets/sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-static-clustering-diff.log |
| `cass.data_db_decode.tombstone.cell_deletion_time` | nb, oa | test-data/datasets/sstables/test_deltas/cell_tombstones-29733830701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-cell-deletion-diff.log |
| `cass.data_db_decode.tombstone.partition_deletion_time` | nb, oa | test-data/datasets/sstables/test_deltas/partition_tombstones-299258f0701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-partition-deletion-diff.log |
| `cass.data_db_decode.tombstone.row_deletion_time` | nb, oa | test-data/datasets/sstables/test_deltas/row_tombstones-297f1f10701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-row-deletion-diff.log |
| `cass.data_db_decode.ttl.local_deletion_time_delta` | nb, oa | test-data/datasets/sstables/test_deltas/ttl_cells-299c9220701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-ttl-delta-diff.log |
| `cass.data_db_decode.unfiltered_serializer.row_and_cell_flags` | nb, oa | test-data/datasets/sstables/test_basic/uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-flag-diff.log |
| `cass.data_db_decode.unfiltered_serializer.row_size_vints` | nb, oa | test-data/datasets/sstables/test_basic/uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/data-db-row-framing-diff.log |
| `cass.data_db_decode.wide_partition.row_boundaries` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.delta_scan.adjacent_ranges` | nb | test-data/datasets/sstables/test_deltas/adjacent_ranges-972f22806c7811f1a24ff924a65838e2/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.cell_tombstones` | nb | test-data/datasets/sstables/test_deltas/cell_tombstones-29f7fbe06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.collection_ops` | nb | test-data/datasets/sstables/test_deltas/collection_ops-2a5006f06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.partial_updates` | nb | test-data/datasets/sstables/test_deltas/partial_updates-2a5ed4006c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.partition_tombstones` | nb | test-data/datasets/sstables/test_deltas/partition_tombstones-2a26fb206c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.range_tombstones` | nb | test-data/datasets/sstables/test_deltas/range_tombstones-2a1a50f06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.row_tombstones` | nb | test-data/datasets/sstables/test_deltas/row_tombstones-2a0e91206c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.static_with_rows` | nb | test-data/datasets/sstables/test_deltas/static_with_rows-2a4299706c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.ttl_cells` | nb | test-data/datasets/sstables/test_deltas/ttl_cells-2a35ef406c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.wide_partition_corpus` | nb | — |
| `cass.distributed_consensus.paxos_accord_out_of_scope` | — | — |
| `cass.filter_db.bti_membership` | da | — |
| `cass.filter_db.corruption_fails_closed` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-corruption.log |
| `cass.filter_db.no_false_negative_membership` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-false-negative.log |
| `cass.filter_db.serialization_round_trip` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-roundtrip.log |
| `cass.filter_db.statistical_false_positive_rate` | nb, oa, da | — |
| `cass.filter_db_bloom.serialization_no_false_negative` | nb | — |
| `cass.index_db.CorruptPrimaryIndexTest.big_primary_index_corruption` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.RowIndexEntryTest.partition_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.RowIndexEntryTest.promoted_index_entries` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.index_db.SSTableReaderTest.point_lookup_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.SSTableScannerTest.range_boundaries` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.big.raw_partition_keys_and_offsets` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.big.wide_partition_promoted_entries` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.index_db.bti.index_component_discovery` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>_fail:_ target/cassandra-parity/index-db-diff.log |
| `cass.index_db.promoted_index.clustering_bounds` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.index_db.promoted_index.index_info_offsets` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.index_db.promoted_index.range_tombstone_boundary_at_block_edge` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.index_summary.big_index_offsets` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.index_summary.column_index.range_tombstone_boundary_big_bti` | nb | test-data/datasets/sstables/test_tomb/wide_range_tombstone-4ce3add0702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt |
| `cass.index_summary.summary_boundaries` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.nodetool_jmx_metrics.operational_out_of_scope` | — | — |
| `cass.read_repair_coordinator.out_of_scope` | — | — |
| `cass.repair_coordinator.anti_entropy_out_of_scope` | — | — |
| `cass.repaired_metadata.statistics_db.pending_repair_uuid` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt |
| `cass.repaired_metadata.statistics_db.repaired_at_field` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Statistics.db.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-repaired-at-mismatch.log |
| `cass.repaired_metadata.statistics_db.transient_repair_flag` | nb, oa, da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt |
| `cass.repaired_metadata.statistics_db.write_roundtrip` | nb | — |
| `cass.sai_sasi_query.secondary_index_out_of_scope` | — | — |
| `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` | nb | test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.schema_evolution.dropped_column.per_cell_purge` | nb | test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-2-big-Statistics.db.txt |
| `cass.schema_evolution.issue_847_dropped_column_filter` | nb | test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-2-big-Statistics.db.txt |
| `cass.schema_evolution.issue_850_static_presence` | nb | test-data/datasets/sstables/test_tomb/dropped_static_col-4cd18560702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt |
| `cass.schema_evolution.serialization_header.altered_column_type` | nb | test-data/datasets/sstables/test_types/se_altered_column_type-4f6856e0706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_altered_column_type-4f6856e0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header.altered_then_dropped_column` | nb | test-data/datasets/sstables/test_types/se_altered_then_dropped_column-4f7cc940706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_altered_then_dropped_column-4f7cc940706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header.dropped_column_same_type` | nb | test-data/datasets/sstables/test_types/se_dropped_column_same_type-4f72b720706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_dropped_column_same_type-4f72b720706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch` | nb | test-data/datasets/sstables/test_types/se_frozen_multicell_collection_mismatch-4f913ba0706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_frozen_multicell_collection_mismatch-4f913ba0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header.no_schema_change` | nb | test-data/datasets/sstables/test_types/se_no_schema_change-4f5a4d20706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_no_schema_change-4f5a4d20706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header.static_regular_kind_mismatch` | nb | test-data/datasets/sstables/test_types/se_static_regular_kind_mismatch-4f87ecd0706211f197e20b846582ecc8/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_types/se_static_regular_kind_mismatch-4f87ecd0706211f197e20b846582ecc8/nb-1-big-Data.db.jsonl<br>_fail:_ logs |
| `cass.schema_evolution.serialization_header_column_order` | nb | — |
| `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` | nb | test-data/datasets/sstables/test_tomb/dropped_static_col-4cd18560702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_tomb/dropped_regular_col-4cc79a50702011f1b8f419c9a388d558/nb-2-big-Statistics.db.txt |
| `cass.serialization.SerializationMirrorTest.schema_evolution_ordering` | nb | — |
| `cass.sstable_format.descriptor_component_resolution` | nb, oa, da | — |
| `cass.sstable_format.toc_component_manifest` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-TOC.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Digest.crc32<br>test-data/datasets/sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Digest.crc32<br>_fail:_ panic diff: cqlite-recomputed Digest.crc32 payload vs Cassandra reference (bytes + decoded decimal + Data.db path) |
| `cass.sstable_io.reader.tombstone_only_partition` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.sstable_scan.wide_partition.forward_reverse_bounds` | nb | test-data/datasets/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl |
| `cass.statistics_db.MetadataSerializerTest.metadata_components` | nb, oa, da | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Statistics.db.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-toc-mismatch.log |
| `cass.statistics_db.SSTableMetadataTest.max_local_deletion_time` | — | — |
| `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` | nb, oa, da | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-encodingstats-mismatch.log |
| `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` | nb, oa, da | test-data/datasets/sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_oa/udt_table-4b9f738064e711f1bd3ac7dbf655c673/oa-2-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-header-mismatch.log |
| `cass.statistics_db.SerializationMirrorTest.column_ordering_metadata` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-ordering-mismatch.log |
| `cass.statistics_db.clustering_key_bounds` | — | — |
| `cass.statistics_db.core_metadata_checksums` | nb, oa, da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-crc-mismatch.log |
| `cass.statistics_db.histograms_and_estimates` | — | — |
| `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` | nb | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-max-ldt-mismatch.log |
| `cass.statistics_metadata.serialization_header` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt |
| `cass.statistics_metadata.tombstone_histogram.deletion_times` | nb | test-data/datasets/sstables/test_tomb/tombstone_histogram-4ca1e9e0702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-tombstone-histogram-mismatch.log |
| `cass.streaming_protocol.node_lifecycle_out_of_scope` | — | — |
| `cass.summary_db.IndexSummaryManagerTest.memory_constrained_summary_reload` | — | — |
| `cass.summary_db.IndexSummaryRedistributionTest.downsampled_summary_entries` | nb, oa, big | — |
| `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.IndexSummaryTest.offset_table_entries` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.IndexSummaryTest.serialization_round_trip` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.big.index_offset_references` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.bti.summary_discovery_classification` | nb, oa, da, big, bti | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.tombstone_ttl.NeverPurgeTest.preserve_all_tombstone_types` | nb, oa | test-data/datasets/sstables/test_deltas/partition_tombstones-299258f0701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_deltas/row_tombstones-297f1f10701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_deltas/cell_tombstones-29733830701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/tombstone-types-delta-diff.log |
| `cass.tombstone_ttl.RangeTombstoneTest.marker_merge_and_persistence` | nb, oa | test-data/datasets/sstables/test_deltas/range_tombstones-298894f0701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/range-marker-grammar-diff.log |
| `cass.tombstone_ttl.TTLExpiryTest.gc_boundary` | nb, oa | test-data/datasets/sstables/test_deltas/ttl_cells-299c9220701f11f1b5d1d98b0640ec05/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/ttl-gc-boundary-delta-diff.log |
| `cass.tombstone_ttl.deletion_markers.cell_delete` | nb | test-data/datasets/sstables/test_deltas/cell_tombstones-88c7bde06c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.deletion_markers.partition_delete` | nb | test-data/datasets/sstables/test_deltas/partition_tombstones-88f66f006c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.deletion_markers.range_delete_bounds` | nb | test-data/datasets/sstables/test_deltas/range_tombstones-88e928906c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.deletion_markers.range_tombstone_boundary` | nb | test-data/datasets/sstables/test_deltas/adjacent_ranges-972f22806c7811f1a24ff924a65838e2/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.deletion_markers.row_delete` | nb | test-data/datasets/sstables/test_deltas/row_tombstones-88dd41b06c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.gc_grace.partition_row_cell` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc_positive-4cbfab10702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc_positive-4cbfab10702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.tombstone_ttl.never_purge.cell_row_partition` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.tombstone_ttl.range_tombstone.closed_last_block` | nb | test-data/datasets/sstables/test_tomb/wide_range_tombstone-4ce3add0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.range_tombstone.index_block_first_marker` | nb | test-data/datasets/sstables/test_tomb/wide_range_tombstone-4ce3add0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.range_tombstone.index_block_last_marker` | nb | test-data/datasets/sstables/test_tomb/wide_range_tombstone-4ce3add0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.range_tombstone.open_ended_middle_block` | nb | test-data/datasets/sstables/test_tomb/wide_range_tombstone-4ce3add0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.range_tombstone_boundaries` | nb | test-data/datasets/sstables/test_deltas/adjacent_ranges-972f22806c7811f1a24ff924a65838e2/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.repaired_unrepaired_purge_gate` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Statistics.db.txt |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.tombstone_ttl.static_row.dropped_static_header_preserved` | nb | test-data/datasets/sstables/test_tomb/dropped_static_col-4cd18560702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/dropped-static-header-mismatch.log |
| `cass.tombstone_ttl.static_row.with_row_cell_range_tombstones` | nb | test-data/datasets/sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.ttl_and_local_deletion_time` | nb | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.ttl_cells.local_deletion_time` | nb | test-data/datasets/sstables/test_deltas/ttl_cells-890626706c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.ttl_cells.mixed_expiring_and_live` | nb | test-data/datasets/sstables/test_deltas/ttl_cells-890626706c9311f1ae1bf55502e5fa53/nb-1-big-Data.db.jsonl |
| `cass.tombstone_ttl.ttl_expiry.gc_before_boundary` | nb | test-data/datasets/sstables/test_tomb/gc_before_boundary-4c92ceb0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.verify.component_presence` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.compression_info_parse` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.digest_crc32_match` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.full_row_scan` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.healthy_compressed_sstable` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.healthy_uncompressed_sstable` | nb | test-data/datasets/sstables/test_comp/uncompressed_table-25a5ca7071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.inline_crc_validation` | nb | test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.verify.no_silent_empty_result_on_corruption` | nb | test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml<br>test-data/datasets/sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db.jsonl |
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | nb | — |
| `cass.write_load_path.cql_sstable_writer.finished_data_db_artifacts` | nb | test-data/datasets/sstables/test_writeparity/finished_data-18ca5be0735711f1a757db89184be81f/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_writeparity/finished_data-18ca5be0735711f1a757db89184be81f/nb-1-big-Data.db<br>test-data/datasets/sstables/test_writeparity/finished_data-18ca5be0735711f1a757db89184be81f/nb-1-big-Index.db<br>test-data/datasets/sstables/test_writeparity/finished_data-18ca5be0735711f1a757db89184be81f/nb-1-big-Summary.db<br>test-data/datasets/sstables/test_writeparity/finished_data-18ca5be0735711f1a757db89184be81f/nb-1-big-Digest.crc32<br>_fail:_ panic diff: CQLite-written vs Cassandra-reference component (cass len + ours len + first-diff byte index + full hex of both) for Data.db / Index.db / Summary.db / Digest.crc32; TOC component-set delta; JSONL partition-count / per-partition row assertion |
| `cass.write_load_path.flush.partition_boundary_artifacts` | nb | test-data/datasets/sstables/test_writeparity/partition_boundary-1909d5e0735711f1a757db89184be81f/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_writeparity/partition_boundary-1909d5e0735711f1a757db89184be81f/nb-1-big-Data.db<br>test-data/datasets/sstables/test_writeparity/partition_boundary-1909d5e0735711f1a757db89184be81f/nb-1-big-Index.db<br>test-data/datasets/sstables/test_writeparity/partition_boundary-1909d5e0735711f1a757db89184be81f/nb-1-big-Summary.db<br>test-data/datasets/sstables/test_writeparity/partition_boundary-1909d5e0735711f1a757db89184be81f/nb-1-big-Digest.crc32<br>_fail:_ panic diff: CQLite-written vs Cassandra-reference component (cass len + ours len + first-diff byte index + full hex of both) for Index.db / Data.db / Summary.db / Digest.crc32; TOC component-set delta; JSONL partition-count / per-partition row assertion |
| `cass.write_load_path.flush.tombstone_and_ttl_artifacts` | nb | — |
| `cass.write_load_path.live_readback.semantic_only` | nb | — |
| `cass.zstd_dictionary.dictionary_assisted_decompression` | — | — |
| `cass.zstd_dictionary.dictionary_cache_reuse` | — | — |
| `cass.zstd_dictionary.dictionary_checksum` | — | — |
| `cass.zstd_dictionary.dictionary_ref_counting` | — | — |
| `cass.zstd_dictionary.dictionary_serialization` | — | — |
| `cass.zstd_dictionary.invalid_dictionary_rejected` | — | — |

## Release-safe claim language

Public/release-facing parity claims are enforced by the claim-scan lint. Safe wordings below are manifest-backed; the blocked phrases are unqualified over-claims rejected unless explicitly scoped as a counter-example.

### Safe wordings

- **claim.safe.rust_byte_level_coverage** — byte-for-byte parity is proven only where this manifest records byte_for_byte evidence
  - Why safe: Byte-level equivalence is asserted only for scenarios carrying explicit byte_for_byte evidence (bytes/offsets/checksums/component files with a strict comparison). The wording forbids generalizing byte parity beyond those scenarios.
  - Backed by: `cass.compaction_merge.byte_for_byte_output`, `cass.compaction.harness_byte_tier_artifacts`
- **claim.safe.selected_fixture_validation** — validated against selected Apache Cassandra 5.0 SSTable fixtures
  - Why safe: CQLite is validated against the specific Cassandra-generated fixtures and datasets recorded in this manifest, not against every possible SSTable. The wording scopes the claim to the covered corpus rather than implying exhaustive coverage.
  - Backed by: `cass.sstable_format.toc_component_manifest`, `cass.data_db_decode.row_cell_flags_and_vint`, `cass.compression_info.lz4.real_fixture_chunks`
- **claim.safe.traceable_cassandra_parity_suite** — a traceable parity suite mapping CQLite tests to specific Cassandra scenarios
  - Why safe: The parity manifest maps each CQLite test/fixture to the Cassandra scenario it mirrors, so claims are traceable to named evidence. This is distinct from running Cassandra's own JVM test suite.
  - Backed by: `cass.compaction.CompactionIteratorTest.differential_compaction_loop`, `cass.data_db_decode.serialization_mirror.multi_clustering_column_order`

### Blocked phrases (rejected unless explicitly scoped)

- **claim.blocked.full_compaction_byte_parity** — "full compaction byte parity"
  - Why blocked: Byte-for-byte compaction parity is proven only for the scenarios that carry byte_for_byte evidence, not for all compaction inputs/strategies. "Full" generalizes byte parity beyond the manifest's evidence.
  - Use instead: `claim.safe.rust_byte_level_coverage`
- **claim.blocked.same_tests_as_cassandra** — "same tests as Cassandra"
  - Why blocked: CQLite does not run Apache Cassandra's JVM test suite; it runs its own Rust parity suite against recorded fixtures. Claiming the "same tests" overstates coverage and implies node-behavior parity CQLite never asserts.
  - Use instead: `claim.safe.traceable_cassandra_parity_suite`
- **claim.blocked.zero_diff_sstabledump_all_datasets** — "zero-diff sstabledump across every dataset"
  - Why blocked: sstabledump parity is validated for the selected fixtures in this manifest, not for every conceivable dataset. "Across every dataset" claims exhaustive coverage the evidence does not support.
  - Use instead: `claim.safe.selected_fixture_validation`

