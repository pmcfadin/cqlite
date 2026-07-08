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
pub mod cancel;
pub mod filter;
pub mod obs;
pub mod pathsafe;
pub mod producer;
pub mod scan_progress;
pub mod service;
pub mod shutdown;
pub mod stats;
pub mod streaming;
pub mod ticket;

#[cfg(test)]
mod testutil;

// Issue #2162 OTel-level assertions (phase histograms, bounded attributes,
// rpc.rows presence via the shared `observability-testing` capture harness) live
// in `tests/metrics_capture_test.rs` — a SEPARATE integration-test binary/process
// (roborev, matching the #2163 precedent), not a unit-test module here. The
// capture harness installs a PROCESS-GLOBAL in-memory meter provider on first
// use; sharing it with this crate's parallel `cargo test --lib` unit-test binary
// would risk cross-test metric contamination. The feature-independent
// `StreamProbe`/`ScanProgress` seam tests in `streaming_tests.rs` carry the
// always-compiled (feature-off-safe) wiring evidence.
