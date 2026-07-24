//! Cassandra 5.0 **CommitLog segment** reader (issue #2389).
//!
//! A second on-disk Cassandra format CQLite reads, alongside SSTables. This
//! module parses a raw `CommitLog-<version>-<id>.log` segment — the file
//! Cassandra itself writes via `CommitLogSegment`/`CommitLogDescriptor` — into a
//! stream of decoded mutations, matching the semantics of Cassandra's own
//! `CommitLogReader`/`CommitLogReplayer` (CRC-validated per-record framing,
//! sync-marker-delimited sections, `tolerateTruncation` on a torn tail).
//!
//! This is a sibling of [`crate::storage::sstable`] and
//! [`crate::storage::write_engine`], deliberately **not** nested under either
//! (design decision D1): it is neither an SSTable component nor CQLite's own
//! write-ahead log (`write_engine::wal`, an unrelated CQLite-native format).
//!
//! ## Scope (v1)
//! - Uncompressed segments only; a compressed/encrypted segment fails closed
//!   with a typed error rather than being misdecoded (design decision D3).
//! - Cassandra 5.0-era descriptor version 7 (`VERSION_40`) only, version-gated
//!   the same way [`crate::storage::sstable::version_gate`] gates SSTables.
//! - Streaming decode — no whole-segment mutation materialization (D4).
//!
//! ## Layout
//! - [`descriptor`] — `CommitLogDescriptor` header + `CommitLogVersionGates`.
//! - [`frame`] — sync markers + per-record CRC framing + truncation tolerance.
//! - [`mutation`] — decoded mutation representation + the mutation-body decoder.
//! - [`schema`] — minimal per-table schema for schema-aware value decode.
//! - [`reader`] — [`CommitLogReader`], the streaming public entry point.

pub mod descriptor;
pub mod frame;
pub mod mutation;
pub mod reader;
pub mod schema;

pub use descriptor::{CommitLogDescriptor, CommitLogVersionGates, SUPPORTED_COMMITLOG_VERSION};
pub use mutation::{DecodedCell, DecodedRow, Mutation, PartitionUpdate};
pub use reader::{CommitLogReader, MutationIter, MAX_SEGMENT_BYTES};
pub use schema::{
    cql_fixed_len, format_table_id, parse_table_id, ColumnSpec, CommitLogSchema, SchemaSet,
};
