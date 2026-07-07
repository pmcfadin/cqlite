//! BTI (Big Trie Index) parser implementation
//!
//! This module provides parsing capabilities for BTI format components:
//! - Partitions.db BTI index for partition lookups
//! - Rows.db BTI index for clustering key lookups within partitions
//!
//! ## Node-type encoding (TrieNode.java, Cassandra 5.0)
//!
//! The high nibble (bits 7-4) of every node's first byte is a 4-bit ordinal that
//! selects one of up to 16 concrete trie-node subtypes:
//!
//! | Nibble | Java class          | Rust category    | Pointer size |
//! |--------|---------------------|------------------|--------------|
//! | 0      | PayloadOnly         | `PayloadOnly`    | —            |
//! | 1      | SingleNoPayload4    | `Single`         | 4-bit delta  |
//! | 2      | Single8             | `Single`         | 1 byte       |
//! | 3      | SingleNoPayload12   | `Single`         | 12-bit delta |
//! | 4      | Single16            | `Single`         | 2 bytes      |
//! | 5      | Sparse8             | `Sparse`         | 1 byte       |
//! | 6      | Sparse12            | `Sparse`         | 12-bit       |
//! | 7      | Sparse16            | `Sparse`         | 2 bytes      |
//! | 8      | Sparse24            | `Sparse`         | 3 bytes      |
//! | 9      | Sparse40            | `Sparse`         | 5 bytes      |
//! | 10     | Dense12             | `Dense`          | 12-bit       |
//! | 11     | Dense16             | `Dense`          | 2 bytes      |
//! | 12     | Dense24             | `Dense`          | 3 bytes      |
//! | 13     | Dense32             | `Dense`          | 4 bytes      |
//! | 14     | Dense40             | `Dense`          | 5 bytes      |
//! | 15     | LongDense           | `Dense`          | 8 bytes      |
//!
//! The low nibble (bits 3-0) carries payload flags; a non-zero value means a
//! payload follows immediately after the node's fixed-size header.
//!
//! All transition deltas are stored as *backward* distances (the child always
//! appears earlier in the file), so the absolute child position is:
//!   `child_pos = parent_pos - delta`
//!
//! The "no-payload" Single variants (nibbles 1 and 3) encode the delta in the
//! low nibble / low 12 bits of the first two bytes respectively, and cannot
//! carry a payload (their payload-flag nibble is always 0).
//!
//! ## Module layout (issue #1119, epic #1116)
//!
//! - [`node_decode`] — node-type nibble classification + the 16-ordinal
//!   `parse_bti_node` dispatcher and its byte/12-bit readers.
//! - [`partitions`]  — `Partitions.db` leaf payload decode, trie walk, and
//!   point lookup (`lookup_partition_in_bti_file`).
//! - [`traversal`]   — depth-capped in-order DFS primitives + the headerless
//!   partition iterator.
//! - [`rows`]        — `Rows.db` `RowIndexReader.IndexInfo` / `TrieIndexEntry`
//!   decode, row iteration, and range-block selection.
//! - [`rows_floor`]  — O(key-length) `separatorFloor` / strict-ceiling `Rows.db`
//!   walks (issue #1647 / L1) + the shared per-node payload primitive.
//! - [`encoding`]    — Cassandra OSS50 byte-comparable clustering-bound encoders.

mod encoding;
mod node_decode;
mod partitions;
mod rows;
mod rows_floor;
mod slice_walk;
mod traversal;

// Public surface — re-exported unchanged so `bti::parser::X` paths keep working.
pub use encoding::{encode_clustering_bound_oss50, encode_clustering_bound_oss50_with_order};
pub use partitions::{
    decode_bti_partition_payload, encode_partition_key_for_bti_trie, lookup_partition_in_bti_file,
    lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation, FLAG_HAS_HASH_BYTE,
};
pub use rows::{
    decode_bti_row_payload, iterate_rows_for_partition, iterate_rows_in_bti_file,
    iterate_rows_in_bti_trie, read_signed_vint_from_slice_for_test,
    read_unsigned_vint_from_slice_for_test, resolve_rows_db_entry,
    select_row_index_blocks_for_range, BtiRowIndexEntry, BtiRowIndexEntryWithKey,
    BtiRowIndexHeader, FLAG_OPEN_MARKER,
};
// Crate-internal only: the zero-copy slice walker's sole consumers are the
// SSTable reader (partition_lookup / data_access::bti). Kept off the public
// semver surface (rust-reviewer #1574). `lookup_partition_in_bti_slice` takes a
// PRE-ENCODED byte-comparable key so callers hoist the Murmur3 hash + encoding
// (issue #1575 / C4): every BTI partition lookup now encodes then walks via this
// single primitive.
pub(crate) use slice_walk::lookup_partition_in_bti_slice;
// Test-only hooks for the issue #1650 (L3) targeted-descent counter invariants.
#[doc(hidden)]
pub use slice_walk::{find_child_offset_for_test, parse_bti_node_for_test};
// Crate-internal only: the O(key-length) Rows.db floor/ceiling walks (issue #1647
// / L1). Their sole consumer is the SSTable reader's clustering-window path
// (data_access::bti), which is compiled out under `tombstones`; kept off the
// public semver surface like the slice walker.
#[cfg(not(feature = "tombstones"))]
pub(crate) use rows_floor::{rows_floor_block, rows_strict_ceiling_block};
pub use traversal::{iterate_partition_locations_in_bti_file, iterate_partitions_in_bti_file};
