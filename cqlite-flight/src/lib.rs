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
pub mod point_read;
pub mod producer;
pub mod scan_progress;
pub mod service;
pub mod shutdown;
pub mod stats;
pub mod streaming;

// Shared byte-pin infrastructure (issues #2283/#2285): the single source of
// truth for the `cassandra_easy_stress.keyvalue` field shape AND the
// wire-metadata-order guard, used by BOTH the golden emitter example and the
// transport byte-pin test. Gated behind the default-off `test-util` feature so
// this test-only code never compiles into the production library/binary (it is
// enabled for the `examples/`/`tests/` targets via the self-referential
// dev-dependency in `Cargo.toml`). `#[doc(hidden)]` keeps it out of the public
// docs; it must be `pub` (not `pub(crate)`) because both callers are separate
// crates linked against this library.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod test_fixtures;

pub mod ticket;

#[cfg(test)]
mod testutil;

// Point-read behavioral tests (issue #2207): work-done probe, dual-path parity,
// index-less fail-safe, cancellation, LIMIT. In-crate so they can use the
// `testutil` fixture builders + the pub(crate) streaming seam.
#[cfg(test)]
mod point_read_tests;

// Issue #2162 OTel-level assertions (phase histograms, bounded attributes,
// rpc.rows presence via the shared `observability-testing` capture harness) live
// in `tests/metrics_capture_test.rs` — a SEPARATE integration-test binary/process
// (roborev, matching the #2163 precedent), not a unit-test module here. The
// capture harness installs a PROCESS-GLOBAL in-memory meter provider on first
// use; sharing it with this crate's parallel `cargo test --lib` unit-test binary
// would risk cross-test metric contamination. The feature-independent
// `StreamProbe`/`ScanProgress` seam tests in `streaming_tests.rs` carry the
// always-compiled (feature-off-safe) wiring evidence.
