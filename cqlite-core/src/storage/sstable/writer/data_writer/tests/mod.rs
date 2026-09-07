//! Relocated `data_writer` unit tests (issue #1118).
//! Shared fixtures live in `support`; tests are grouped by file.

mod collection_order_serialize;
/// WRITE-SIDE admission of the sentinel on a multicell SET's CELL PATH (issue
/// #4106) — the sibling of the above one collection over, with every byte of its
/// expectation derived from `cassandra-5.0.8` rather than from a round trip.
mod empty_sentinel_set_write;
/// WRITE-SIDE admission of the empty-buffer sentinel (issue #3805, roborev job
/// 449 finding D): legal ONLY on a multicell collection's cell path with the tag
/// validated against the declared component type; refused in every type-blind
/// context. This module covers the MAP half; the SET half is below (#4106).
mod empty_sentinel_write;
/// Correctness proof for `IncrementalPartitionWriter` (issue #1668, stage
/// 5c-iv part 1) against today's whole-slice `write_partition_with_index_blocks`.
mod incremental_partition;
/// Work-counter guard proving the schema-constant ordered column lists +
/// per-column `is_complex` classification are computed once per writer, never
/// per row (issue #1674, R3).
mod issue_1674_column_cache;
mod scenarios_1;
mod scenarios_2;
mod scenarios_3;
mod scenarios_4;
mod scenarios_5;
mod scenarios_6;
/// Correctness proof for `StreamingPartitionSession` (issue #1668, stage
/// 5c-iv part 3): cross-call resumable — a partition fed in TWO separate
/// batches (simulating a budget pause between them) must byte-match one fed
/// straight through, and both must byte-match the whole-slice path.
mod streaming_partition;
mod support;
/// DIFFERENTIAL pin between the writer's two independent sorted-collection
/// comparators (issue #3935) — `collection_order::compare_collection_elements`
/// (variant-driven) vs `marshal_comparator::compare_for_marshal`
/// (declared-marshal-driven). Both carried the same `time` defect.
mod writer_comparator_differential;
