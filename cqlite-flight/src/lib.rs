//! Arrow Flight server for CQLite.
//!
//! Exposes Cassandra SSTable data to Arrow Flight clients (e.g. a Trino
//! connector). On read, the server performs an on-the-fly compaction merge of a
//! node's SSTables — leaving the originals untouched — applies token-range and
//! predicate filters, and streams the result as Arrow record batches.
//!
//! See `docs/flight-trino/PLAN.md` for the full design and `JOURNAL.md` for the
//! change log.

pub mod admission;
pub mod agg;
// Byte-bounded Arrow egress batches (issue #2825): the dual row-cap / byte-cap
// batch boundary shared by BOTH producer drive loops, plus the published
// payload-to-capacity conversion constants issue #2821 composes on.
pub mod batch_bytes;
// Single-source (one post-prune SSTable) merge bypass for the warm `do_get` row
// route (issue #3058): the fail-closed predicate, the `CQLITE_FLIGHT_MERGE_PATH`
// forced-path seam, and the single-generation scan row source.
pub mod bypass;
pub mod cancel;
// The server binary's clap surface (issue #3225): the argument definitions, the
// parse entry point that also yields the `ArgMatches`, the admission-ceiling
// provenance resolution (flag > env > derived) and the startup log event. In
// the LIBRARY rather than `main.rs` so AC4's precedence/provenance contract is
// asserted through the REAL parser from an integration test — a binary target's
// `Args` is unreachable from `tests/`.
pub mod cli;

// THROWAWAY SPIKE (issue #2605), non-default `datafusion-spike` feature ONLY: a
// DataFusion `TableProvider`/`ExecutionPlan` over the EXISTING merge-producer
// scan path, plus the bench harness that separates the decode-to-column delta
// from the vectorized-exec delta for the #941 promotion decision. It MUST live
// in this crate because the seam it drives (`MergeProducer::produce_streaming`
// and the `BatchSink` trait) is `pub(crate)`. NOTHING in production reaches it —
// no service route, no ticket field, no CLI flag — and with the feature off not
// a line of it (nor of DataFusion) is compiled.
#[cfg(feature = "datafusion-spike")]
pub mod df_spike;
// Per-stream in-flight egress capacity-byte credit governor (issue #2821): the
// reserve-before-materialize credit pool, its RAII permit, the `CreditedBatch`
// channel element, and the `--max-inflight-egress-bytes` constants.
pub mod egress_credit;
// The single owning `reserve -> build -> true-up -> emit` batch-boundary helper
// shared by both producer drive loops (issue #2821).
mod egress_flush;
// The credit governor's observation seam (charged/resident high-water marks and
// the reservation lifecycle counters), split out of `egress_credit.rs` under the
// campsite rule (epic #1116).
mod egress_observation;
pub mod filter;
// The `do_get` drain side: metrics attribution, cancellation, and the deferred
// egress-credit slot (split out of `streaming.rs`, epic #1116).
mod metered_stream;
pub mod obs;
pub mod obs_abort;
pub mod obs_subphase;
pub mod pathsafe;
pub mod point_read;
pub mod producer;
// The BUFFERED (whole-partition) drive loops — campsite split of `producer.rs`
// (epic #1116); sibling of `producer_stream`'s row-granular loop.
mod producer_drive;
mod producer_point;
mod producer_stream;
mod producer_warm;
// The arm-independent ROW SOURCE seam shared by the k-way merge arm and the
// single-generation bypass arm (issue #3058).
mod row_source;
pub mod saturation;
pub mod scan_progress;
pub mod service;
pub mod shutdown;
pub mod stats;
// Cassandra STATIC-column `SELECT` semantics for the k-way merge arm of the row
// route (issue #3095).
mod statics;
pub mod streaming;
pub mod warm;

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

// Deterministic, self-contained synthetic wide/narrow row shapes for the
// byte-cap suite (issue #2825). Gated behind `test-util` for the same reason as
// `test_fixtures`: the shapes must reach BOTH the in-crate unit tests and a
// separate integration-test binary, which a `pub(crate)` module cannot do, but
// they must never compile into the production library/binary.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod wide_row_fixture;

pub mod ticket;

#[cfg(test)]
mod testutil;

// Point-read behavioral tests (issue #2207): work-done probe, dual-path parity,
// index-less fail-safe, cancellation, LIMIT. In-crate so they can use the
// `testutil` fixture builders + the pub(crate) streaming seam.
#[cfg(test)]
mod point_read_tests;

// Row-granular streaming for the point-read and cache-warm merge paths (issue
// #2423): bounded intra-partition materialisation + mid-partition cancellation +
// byte-identity, through the REAL producer point-read / warm paths.
#[cfg(test)]
mod point_read_streaming_tests;

// Issue #2339 (roborev F1): the effective UDT keyspace is one answer, shared by
// the Arrow column metadata and merged-read reassembly. In-crate because
// `with_udt_keyspace`/`udt_scope` are `pub(crate)`.
#[cfg(test)]
mod producer_udt_scope_tests;

// Issue #2162 OTel-level assertions (phase histograms, bounded attributes,
// rpc.rows presence via the shared `observability-testing` capture harness) live
// in `tests/metrics_capture_test.rs` — a SEPARATE integration-test binary/process
// (roborev, matching the #2163 precedent), not a unit-test module here. The
// capture harness installs a PROCESS-GLOBAL in-memory meter provider on first
// use; sharing it with this crate's parallel `cargo test --lib` unit-test binary
// would risk cross-test metric contamination. The feature-independent
// `StreamProbe`/`ScanProgress` seam tests in `streaming_tests.rs` carry the
// always-compiled (feature-off-safe) wiring evidence.
