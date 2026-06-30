//! Arrow Flight server for CQLite.
//!
//! Exposes Cassandra SSTable data to Arrow Flight clients (e.g. a Trino
//! connector). On read, the server performs an on-the-fly compaction merge of a
//! node's SSTables — leaving the originals untouched — applies token-range and
//! predicate filters, and streams the result as Arrow record batches.
//!
//! See `docs/flight-trino/PLAN.md` for the full design and `JOURNAL.md` for the
//! change log.

pub mod agg;
pub mod filter;
pub mod obs;
pub mod producer;
pub mod service;
pub mod stats;
pub mod ticket;

#[cfg(test)]
mod testutil;
