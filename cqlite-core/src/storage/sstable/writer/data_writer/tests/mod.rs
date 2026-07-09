//! Relocated `data_writer` unit tests (issue #1118).
//! Shared fixtures live in `support`; tests are grouped by file.

mod collection_order_serialize;
/// Correctness proof for `IncrementalPartitionWriter` (issue #1668, stage
/// 5c-iv part 1) against today's whole-slice `write_partition_with_index_blocks`.
mod incremental_partition;
mod scenarios_1;
mod scenarios_2;
mod scenarios_3;
mod scenarios_4;
mod scenarios_5;
mod scenarios_6;
mod support;
