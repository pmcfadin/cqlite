# Cassandra Test Parity Report

> Generated from `test-data/cassandra-parity-manifest.yml` by `cargo run -p cassandra-parity -- report`. Do not edit by hand — edit the manifest and regenerate.

Cassandra source: [`cassandra-5.0.2`](https://github.com/apache/cassandra/tree/cassandra-5.0.2) @ `f278f6774fc76465c182041e081982105c3e7dbb` (git SHA). Program: parent epic #966, reporting epic #967.

Sources: [`docs/cassandra_test_index.md`](../../docs/cassandra_test_index.md) · [`docs/reports/cassandra-test-parity-assessment.md`](../../docs/reports/cassandra-test-parity-assessment.md)

## Status counts

| Status | Scenarios |
|---|---|
| `mirrored` | 68 |
| `partial` | 13 |
| `planned` | 11 |
| `out_of_scope` | 8 |
| **total** | **100** |

## Evidence counts

| Evidence | Scenarios |
|---|---|
| `byte_for_byte` | 30 |
| `canonical_semantic` | 32 |
| `smoke` | 6 |
| `partial` | 22 |
| `out_of_scope` | 10 |

## ⚠️ P0 scenarios with weak evidence

These P0 scenarios are backed only by `smoke` or `partial` evidence and must not be cited as proof of byte parity:

- `cass.compaction_merge.byte_for_byte_output` — Compaction byte-for-byte output parity (future) (partial)
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1) (smoke)
- `cass.compression_checksum.checksum_trailer_detection` — Inline checksum / Digest.crc32 corruption detection (partial)
- `cass.corruption_verify.component_corruption_detection` — Component corruption detection, scrub, and verify (partial)
- `cass.delta_scan.tombstone_liveness_facts` — Delta-scan tombstone/TTL/liveness fact extraction (partial)
- `cass.filter_db_bloom.serialization_no_false_negative` — Filter.db Bloom filter serialization with no false negatives (partial)
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` — BIG Index.db promoted-index (wide-partition) boundary metadata (partial)
- `cass.index_summary.column_index.range_tombstone_boundary_big_bti` — Column-index range-tombstone boundary across BIG and BTI formats (partial)
- `cass.index_summary.summary_boundaries` — Summary.db sampling boundaries (BIG) (partial)
- `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` — Dropped-column empty-index-block reverse-scan parity (partial)
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution (smoke)
- `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` — Statistics.db max local deletion time for tombstone/TTL fixture (partial)
- `cass.statistics_metadata.tombstone_histogram.deletion_times` — Statistics.db estimated tombstone-drop-times histogram parity (partial)
- `cass.tombstone_ttl.range_tombstone_boundaries` — Range tombstone boundary and deletion-time parity (partial)
- `cass.tombstone_ttl.repaired_unrepaired_purge_gate` — Repaired vs unrepaired purge gate parity (partial)
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader (smoke)

## P0 scenarios

| ID | Capability | Status | Evidence | Suite | Risk |
|---|---|---|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.bti_big_version_matrix.bti_da_write_read` | bti_big_version_matrix | mirrored | canonical_semantic | `sstable_parity_bti_partitions_rows` | p1_correctness |
| `cass.compaction_merge.byte_for_byte_output` | compaction_merge | planned | partial | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.load_path_validity` | compaction_merge | mirrored | smoke | `compaction_parity_tombstone_ttl` | p1_correctness |
| `cass.compaction_merge.partial_source_retains_tombstones` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compaction_merge.tombstone_ttl_shadowing` | compaction_merge | mirrored | canonical_semantic | `compaction_parity_tombstone_ttl` | p0_data_loss |
| `cass.compression_checksum.checksum_trailer_detection` | compression_checksum | partial | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.compression_checksum.chunk_offsets_and_crc` | compression_checksum | mirrored | canonical_semantic | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.lz4.real_fixture_chunks` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.compression_info.snappy.real_fixture_chunks` | compression_checksum | mirrored | byte_for_byte | `sstable_parity_compression_info_chunks` | p0_data_loss |
| `cass.corruption_verify.component_corruption_detection` | corruption_verify | planned | partial | `sstable_parity_corruption_verify` | p0_data_loss |
| `cass.data_db_decode.row_cell_flags_and_vint` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_data_db_jsonl` | p1_correctness |
| `cass.delta_scan.tombstone_liveness_facts` | delta_scan | partial | partial | `sstable_parity_delta_scan` | p1_correctness |
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
| `cass.index_summary.big_index_offsets` | index_summary | mirrored | canonical_semantic | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.column_index.range_tombstone_boundary_big_bti` | index_summary | partial | partial | `sstable_parity_index_db_big` | p1_correctness |
| `cass.index_summary.summary_boundaries` | index_summary | partial | partial | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` | schema_evolution | partial | partial | `sstable_parity_delta_scan` | p1_correctness |
| `cass.schema_evolution.dropped_column.per_cell_purge` | schema_evolution | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p1_correctness |
| `cass.sstable_format.descriptor_component_resolution` | sstable_format | mirrored | smoke | `sstable_parity_component_manifest` | p1_correctness |
| `cass.sstable_format.toc_component_manifest` | sstable_format | mirrored | byte_for_byte | `sstable_parity_component_manifest` | p1_correctness |
| `cass.sstable_io.reader.tombstone_only_partition` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | data_db_decode | mirrored | canonical_semantic | `sstable_parity_delta_scan` | p0_data_loss |
| `cass.statistics_db.MetadataSerializerTest.metadata_components` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` | tombstone_ttl | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` | schema_evolution | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_db.core_metadata_checksums` | statistics_metadata | mirrored | byte_for_byte | `sstable_parity_statistics_db` | p0_data_loss |
| `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` | statistics_metadata | partial | partial | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_metadata.serialization_header` | statistics_metadata | mirrored | canonical_semantic | `sstable_parity_statistics_db` | p1_correctness |
| `cass.statistics_metadata.tombstone_histogram.deletion_times` | statistics_metadata | partial | partial | `sstable_parity_statistics_db` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.offset_table_entries` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.IndexSummaryTest.serialization_round_trip` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.big.index_offset_references` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
| `cass.summary_db.bti.summary_discovery_classification` | index_summary | mirrored | byte_for_byte | `sstable_parity_summary_db_big` | p1_correctness |
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
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | write_load_path | mirrored | smoke | `sstable_writer_cassandra_fixture_parity` | p0_data_loss |

## Byte-for-byte scenarios

- `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` — Truncated compressed chunk fail-closed parity
- `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` — CompressionInfo.db ordered chunk-offset table parity
- `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` — Compressed chunk record boundaries vs Data.db parity
- `cass.compression_info.CompressionMetadataTest.metadata_serialization` — CompressionInfo.db metadata byte-for-byte serialization parity
- `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` — Inline per-chunk CRC32 trailer validation parity
- `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` — Compressed chunk payload + CRC round-trip byte parity
- `cass.compression_info.lz4.real_fixture_chunks` — LZ4Compressor real-fixture chunk + CRC parity
- `cass.compression_info.snappy.real_fixture_chunks` — SnappyCompressor real-fixture chunk + CRC parity
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
- `cass.sstable_format.toc_component_manifest` — TOC.txt component manifest completeness
- `cass.statistics_db.MetadataSerializerTest.metadata_components` — Statistics.db metadata-component TOC byte parity (count + ordered types)
- `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` — Statistics.db min timestamp / local-deletion-time / TTL byte parity
- `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` — Statistics.db serialization-header column metadata byte parity
- `cass.statistics_db.SerializationMirrorTest.column_ordering_metadata` — Statistics.db clustering-key ordering / ReversedType byte parity
- `cass.statistics_db.core_metadata_checksums` — Statistics.db embedded CRC32 checksum byte parity
- `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` — Summary.db first/last decorated-key boundaries (BIG)
- `cass.summary_db.IndexSummaryTest.offset_table_entries` — Summary.db little-endian offset table + entry ordering (BIG)
- `cass.summary_db.IndexSummaryTest.serialization_round_trip` — Summary.db header + entry serialization round-trip (BIG)
  - Normalization: 24-byte big-endian header and length-prefixed first/last keys decoded from raw bytes; the little-endian offset table is decoded independently and cross-checked against SummaryReader.
- `cass.summary_db.big.index_offset_references` — Summary.db sampled positions resolve to Index.db partition entries (BIG)
  - Normalization: Sampled positions are decoded little-endian (the on-disk truth verified against Index.db) and matched to be16-length-prefixed Index.db keys.
- `cass.summary_db.bti.summary_discovery_classification` — BTI SSTables carry no Summary.db (trie Partitions.db replaces it)
  - Normalization: TOC.txt component manifests are parsed strictly; format is taken from the descriptor filename, never inferred from contents.
- `cass.tombstone_ttl.static_row.dropped_static_header_preserved` — Dropped static column SerializationHeader byte parity
  - Normalization: The dropped static column is preserved in the embedded SerializationHeader; its name set and kind are compared byte-equal against the StaticColumns line of the reference Statistics.db dump.

## Canonical-semantic scenarios

- `cass.bti_big_version_matrix.big_nb_oa_read` — BIG nb/oa read parity matrix
  - Normalization: Decoded rows for nb (Cassandra 4-compatible BIG) and oa (Cassandra 5 BIG) datasets are compared against sstabledump JSONL.
- `cass.bti_big_version_matrix.bti_da_write_read` — BTI da write and read-back parity
  - Normalization: CQLite-written da BTI SSTables are dumped with Cassandra 5 sstabledump and compared for value equivalence; Partitions.db footer shape [firstPos|keyCount|root] is matched against a real Cassandra fixture.
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
- `cass.compression_checksum.chunk_offsets_and_crc` — CompressionInfo.db chunk decode and row-count parity
  - Normalization: Decompressed chunk payloads are decoded to rows and compared (row count and values) against sstabledump JSONL; chunk offset tables are used for positioning.
- `cass.data_db_decode.row_cell_flags_and_vint` — Data.db row/cell flags and VInt decode parity
  - Normalization: Rows and cells are normalized to the sstabledump JSONL fact model (partition key, clustering, cell name/value, liveness, deletion) and compared field-by-field; presentation ordering and whitespace ignored.
- `cass.index_summary.big_index_offsets` — Index.db partition key digests and data offsets (BIG)
  - Normalization: Partition key digests and Data.db offsets resolved through Index.db are compared against the partition order and keys derived from sstabledump JSONL.
- `cass.schema_evolution.dropped_column.per_cell_purge` — Dropped regular column per-cell purge parity
  - Normalization: Cells for a dropped regular column are purged per-cell on read; the surviving cells and the dropped-column metadata in the SerializationHeader are mapped to the sstabledump JSONL and Statistics.db dump and compared.
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

## Smoke-only scenarios

- `cass.cli_reporting.parity_manifest_lint_and_report` — Parity manifest lint and report tooling
- `cass.compaction_merge.load_path_validity` — Compaction output load-path validity (Tier-1)
- `cass.filter_db.statistical_false_positive_rate` — Filter.db empirical false-positive-rate report
- `cass.schema_evolution.serialization_header_column_order` — Serialization-header column order across schema evolution
- `cass.sstable_format.descriptor_component_resolution` — Descriptor and on-disk version/component resolution
- `cass.write_load_path.cassandra_sstable_writer_fixtures` — CQLite-written SSTables load into Cassandra via sstableloader

## Gaps and next steps

- `cass.compaction_merge.byte_for_byte_output` (planned): No gated byte-for-byte comparison of compaction output. → _Promote the debug byte tier in compaction-parity to a gated comparison once writer output is byte-stable._
- `cass.compression_checksum.checksum_trailer_detection` (partial): No gated byte comparison of Digest.crc32 against the Cassandra reference. → _Add a Digest.crc32 byte comparison to the sstable_parity_corruption_verify suite._
- `cass.compression_info.deflate.real_fixture_chunks` (planned): No real DeflateCompressor CompressionInfo.db / Data.db fixture in the committed corpus, so chunk + CRC parity cannot be byte-compared. → _Generate a DeflateCompressor SSTable via regenerate-datasets.sh and let the existing test exercise it (the codec dispatch already handles it)._
- `cass.compression_info.zstd.real_fixture_chunks` (planned): No real ZstdCompressor CompressionInfo.db / Data.db fixture in the committed corpus, so chunk + CRC parity cannot be byte-compared. → _Generate a ZstdCompressor SSTable via regenerate-datasets.sh and let the existing test exercise it (the codec dispatch already handles it)._
- `cass.corruption_verify.component_corruption_detection` (planned): No scrub/verify parity pass implemented. → _Implement a verify pass and compare detected-corruption outcomes against Cassandra VerifyTest/ScrubTest scenarios._
- `cass.delta_scan.tombstone_liveness_facts` (partial): test_deltas dataset asset not published/enforced (#701). → _Publish and enforce the test_deltas dataset in delta-roundtrip CI._
- `cass.filter_db.bti_membership` (partial): No raw-partition-key source for BTI fixtures, so the no-false-negative probe cannot run against da Filter.db. → _Recover raw BTI partition keys (e.g. by decoding partitions during a Data.db scan) and extend the no-false-negative gate to cover da fixtures._
- `cass.filter_db.statistical_false_positive_rate` (planned): No gated comparison of measured FPR against Cassandra's configured bloom_filter_fp_chance. → _Add larger-cardinality fixtures and assert the measured FPR tracks the configured fp_chance within a documented statistical tolerance._
- `cass.filter_db_bloom.serialization_no_false_negative` (partial): No no-false-negative parity assertion against Cassandra Filter.db. → _Add a Filter.db serialization parity test asserting zero false negatives across the present-key set._
- `cass.index_db.RowIndexEntryTest.promoted_index_entries` (partial): No committed BIG fixture triggers promoted-index emission (all partitions are below the column_index_size threshold). → _Generate a wide-partition BIG fixture (partition exceeding column_index_size_in_kb) and assert decoded promoted-index clustering boundaries against the Cassandra reference._
- `cass.index_db.big.wide_partition_promoted_entries` (partial): No committed wide BIG fixture exercises promoted clustering boundaries. → _Add a BIG fixture with a partition exceeding column_index_size_in_kb and compare decoded promoted clustering boundaries to the Cassandra reference._
- `cass.index_summary.column_index.range_tombstone_boundary_big_bti` (partial): BTI (da) range-tombstone-at-block-edge fixtures are not yet generated (no da tombstone generator). → _Add a da/BTI wide-partition range-tombstone fixture generator and assert BTI column-index boundary parity; file a follow-up issue._
- `cass.index_summary.summary_boundaries` (partial): Cassandra Summary.db reference dumps not published for all tables. → _Publish Summary.db reference dumps and enable strict first/last-key boundary comparison in the sstable_parity_summary_db_big suite._
- `cass.repaired_metadata.statistics_db.pending_repair_uuid` (planned): No Cassandra 5.0 pending-repair fixture available; the reference null (`Pending repair: --`) state is confirmed, and the read path reports the field as Unparsed (it is not walked from bytes) rather than a fabricated absent value. → _When a pending-repair fixture is generated, decode the pendingRepair UUID (type-aware skip past improvedMinMax + commitLogIntervals) — promoting the field from RepairField::Unparsed to Decoded — and assert it byte-for-byte against the `Pending repair: <uuid>` reference line._
- `cass.repaired_metadata.statistics_db.transient_repair_flag` (planned): No transiently-replicated fixture available; the reference `IsTransient: false` state is confirmed, and the read path reports the field as Unparsed (it is not walked from bytes) rather than a fabricated `false`. → _When a transient-replication fixture is generated, decode the isTransient flag (after the version-gated improvedMinMax block and commitLogIntervals) — promoting it from RepairField::Unparsed to Decoded — and assert it byte-for-byte against the `IsTransient: true` reference line._
- `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan` (partial): A wide dropped-column empty-index-block reverse-scan fixture is not yet generated. → _Generate a wide dropped-column fixture that yields an empty index block under reverse scan and assert parity; file a follow-up issue._
- `cass.statistics_db.SSTableMetadataTest.max_local_deletion_time` (planned): STATS-section max timestamp / max local-deletion-time not yet decoded. → _Decode the STATS MetadataType component and assert max timestamp / max local-deletion-time against the reference dump._
- `cass.statistics_db.clustering_key_bounds` (planned): Covered-clustering min/max bounds not yet decoded from the STATS component. → _Decode the STATS-section clustering bounds and compare against the "Covered clusterings" reference line._
- `cass.statistics_db.histograms_and_estimates` (planned): STATS-section histograms and partition/row estimates not yet decoded. → _Decode the STATS-section EstimatedHistograms and count estimates and compare bucket boundaries against the reference dump._
- `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` (partial): CQLite's minimal Statistics parser does not decode the STATS-component SSTable max local deletion time and returns a placeholder equal to the min baseline. → _Decode the STATS max local-deletion-time field and assert it byte-equal to the sstablemetadata reference; tracked by follow-up issue #1073._
- `cass.statistics_metadata.tombstone_histogram.deletion_times` (partial): The estimated-tombstone-drop-times histogram in Statistics.db is not decoded or exposed by CQLite. → _Decode the estimated-tombstone-drop-times histogram and assert bucket parity against the sstablemetadata reference; tracked by follow-up issue #1073._
- `cass.summary_db.IndexSummaryRedistributionTest.downsampled_summary_entries` (planned): No downsampled (sampling_level < 128) Summary.db fixture exists. → _Publish a redistributed Summary.db fixture and extend the strict suite to assert downsampled offset tables and size_at_full_sampling > entry count._
- `cass.tombstone_ttl.range_tombstone_boundaries` (partial): test_deltas dataset asset not published/enforced in CI (#701). → _Publish the test_deltas dataset and enforce scan_delta parity in CI._
- `cass.tombstone_ttl.repaired_unrepaired_purge_gate` (partial): repairedAt / pendingRepair parsing is not implemented (gated on #968/#988), so the repaired-vs-unrepaired purge gate is only partially exercised. → _Parse repairedAt / pendingRepair from Statistics.db and gate purge on repair status (#968/#988)._

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

## CI workflow mapping

| Scenario | CI tier | Workflow |
|---|---|---|
| `cass.bti_big_version_matrix.big_nb_oa_read` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.bti_big_version_matrix.bti_da_write_read` | nightly_docker | .github/workflows/e2e-readback.yml |
| `cass.cli_reporting.parity_manifest_lint_and_report` | fast_pr | .github/workflows/cassandra-parity.yml |
| `cass.commitlog_replay.recovery_out_of_scope` | fast_pr | — |
| `cass.compaction_merge.byte_for_byte_output` | manual_debug | — |
| `cass.compaction_merge.load_path_validity` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compaction_merge.partial_source_retains_tombstones` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.compaction_merge.tombstone_ttl_shadowing` | required_parity | .github/workflows/compaction-parity.yml |
| `cass.compression_checksum.checksum_trailer_detection` | fast_pr | — |
| `cass.compression_checksum.chunk_offsets_and_crc` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.deflate.real_fixture_chunks` | manual_debug | — |
| `cass.compression_info.lz4.real_fixture_chunks` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.snappy.real_fixture_chunks` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.compression_info.zstd.real_fixture_chunks` | manual_debug | — |
| `cass.corruption_verify.component_corruption_detection` | manual_debug | — |
| `cass.data_db_decode.row_cell_flags_and_vint` | required_parity | .github/workflows/sstabledump-parity-gate.yml |
| `cass.delta_scan.tombstone_liveness_facts` | required_parity | .github/workflows/delta-roundtrip.yml |
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
| `cass.schema_evolution.serialization_header_column_order` | fast_pr | — |
| `cass.sstable_format.descriptor_component_resolution` | fast_pr | — |
| `cass.sstable_format.toc_component_manifest` | fast_pr | — |
| `cass.sstable_io.reader.tombstone_only_partition` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | nightly_docker | .github/workflows/tombstone-ttl-parity.yml |
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
| `cass.compaction_merge.partial_source_retains_tombstones` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.resurrection_safety.overlapping_sources` | nb | test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/resurrection_gc0-4cb523c0702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.compaction_merge.static_row.survives_tombstone_gc` | nb | test-data/datasets/sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl |
| `cass.compaction_merge.tombstone_ttl_shadowing` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compression_checksum.checksum_trailer_detection` | da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Digest.crc32<br>_fail:_ target/cassandra-parity/checksum-mismatch.log |
| `cass.compression_checksum.chunk_offsets_and_crc` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.compression_info.CompressedInputStreamTest.truncated_chunk_detection` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-truncated-chunk.log |
| `cass.compression_info.CompressedRandomAccessReaderTest.chunk_offsets` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-offsets-mismatch.log |
| `cass.compression_info.CompressedSequentialWriterTest.chunk_boundaries` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-boundaries-mismatch.log |
| `cass.compression_info.CompressionMetadataTest.metadata_serialization` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-metadata-mismatch.log |
| `cass.compression_info.DirectCompressedChunkReaderTest.inline_crc_validation` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-chunk-crc-mismatch.log |
| `cass.compression_info.StandardCompressedChunkReaderTest.round_trip_chunk_bytes` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-round-trip-mismatch.log |
| `cass.compression_info.deflate.real_fixture_chunks` | — | — |
| `cass.compression_info.lz4.real_fixture_chunks` | nb | test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-lz4-mismatch.log |
| `cass.compression_info.snappy.real_fixture_chunks` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/compression-info-snappy-mismatch.log |
| `cass.compression_info.zstd.real_fixture_chunks` | — | — |
| `cass.corruption_verify.component_corruption_detection` | — | — |
| `cass.data_db_decode.row_cell_flags_and_vint` | nb, oa | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl |
| `cass.delta_scan.tombstone_liveness_facts` | nb | test-data/datasets/sstables/test_deltas/collection_ops-2a5006f06c2a11f18135b3f5f7fa4418/nb-1-big-Data.db.jsonl |
| `cass.distributed_consensus.paxos_accord_out_of_scope` | — | — |
| `cass.filter_db.bti_membership` | da | — |
| `cass.filter_db.corruption_fails_closed` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-corruption.log |
| `cass.filter_db.no_false_negative_membership` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-false-negative.log |
| `cass.filter_db.serialization_round_trip` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Data.db.jsonl<br>_fail:_ target/cassandra-parity/filter-db-roundtrip.log |
| `cass.filter_db.statistical_false_positive_rate` | nb, oa, da | — |
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
| `cass.schema_evolution.serialization_header_column_order` | nb | — |
| `cass.sstable_format.descriptor_component_resolution` | nb, oa, da | — |
| `cass.sstable_format.toc_component_manifest` | nb, oa, da | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-TOC.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-TOC.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Digest.crc32<br>test-data/datasets/sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Digest.crc32<br>_fail:_ panic diff: cqlite-recomputed Digest.crc32 payload vs Cassandra reference (bytes + decoded decimal + Data.db path) |
| `cass.sstable_io.reader.tombstone_only_partition` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-1-big-Data.db.jsonl<br>test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.sstable_io.scanner.tombstone_only_partition_ranges` | nb | test-data/datasets/sstables/test_tomb/skipped_partition_delete-4caaea90702011f1b8f419c9a388d558/nb-2-big-Data.db.jsonl |
| `cass.statistics_db.MetadataSerializerTest.metadata_components` | nb, oa, da | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_oa/collection_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Statistics.db.txt<br>test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-toc-mismatch.log |
| `cass.statistics_db.SSTableMetadataTest.max_local_deletion_time` | — | — |
| `cass.statistics_db.SSTableMetadataTrackingTest.timestamp_and_ttl_metadata` | nb, oa, da | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-encodingstats-mismatch.log |
| `cass.statistics_db.SerializationHeaderTest.schema_evolution_header` | nb, oa, da | test-data/datasets/sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>test-data/datasets/sstables/test_oa/udt_table-4b9f738064e711f1bd3ac7dbf655c673/oa-2-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-header-mismatch.log |
| `cass.statistics_db.SerializationMirrorTest.column_ordering_metadata` | nb, oa, da | test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-ordering-mismatch.log |
| `cass.statistics_db.clustering_key_bounds` | — | — |
| `cass.statistics_db.core_metadata_checksums` | nb, oa, da | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Statistics.db.txt<br>_fail:_ target/cassandra-parity/statistics-db-crc-mismatch.log |
| `cass.statistics_db.histograms_and_estimates` | — | — |
| `cass.statistics_metadata.max_local_deletion_time.tombstones_ttl` | nb | test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt |
| `cass.statistics_metadata.serialization_header` | nb | test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt |
| `cass.statistics_metadata.tombstone_histogram.deletion_times` | nb | test-data/datasets/sstables/test_tomb/tombstone_histogram-4ca1e9e0702011f1b8f419c9a388d558/nb-1-big-Statistics.db.txt |
| `cass.streaming_protocol.node_lifecycle_out_of_scope` | — | — |
| `cass.summary_db.IndexSummaryManagerTest.memory_constrained_summary_reload` | — | — |
| `cass.summary_db.IndexSummaryRedistributionTest.downsampled_summary_entries` | nb, oa, big | — |
| `cass.summary_db.IndexSummaryTest.first_last_key_boundaries` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.IndexSummaryTest.offset_table_entries` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.IndexSummaryTest.serialization_round_trip` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.big.index_offset_references` | nb, oa, big | test-data/datasets/sstables/test_timeseries/app_metrics-6c87b890a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
| `cass.summary_db.bti.summary_discovery_classification` | nb, oa, da, big, bti | test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-TOC.txt<br>_fail:_ validation_artifacts/sstabledump/summary/summary_parity_report.md |
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
| `cass.write_load_path.cassandra_sstable_writer_fixtures` | nb | — |

## Claim language

**Safe:** CQLite reads and writes Cassandra 5.0 SSTables and is validated for canonical-semantic equivalence against `sstabledump` for the covered dataset, with byte-for-byte parity proven only where this report records `byte_for_byte` evidence.

**Unsafe:** "CQLite passes the same tests as Cassandra" or "CQLite is byte-for-byte identical to Cassandra" — these overclaim node behavior and byte parity the manifest does not support.

