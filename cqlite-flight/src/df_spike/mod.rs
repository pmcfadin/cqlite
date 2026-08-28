//! **THROWAWAY SPIKE** — a DataFusion `TableProvider` over the existing CQLite
//! Flight scan path (issue #2605).
//!
//! # What this is, and what it is deliberately NOT
//!
//! It is a measurement instrument. It exists to answer one question with numbers
//! instead of argument: how much of CQLite's scan cost is the row→column
//! transpose the current pipeline pays (`decode-to-column`), and how much is
//! execution over the resulting batches (`vectorized-exec`)? Those two figures
//! drive the #941 promotion decision and the columnar-producer slot trigger in
//! `docs/architecture/throughput-program-2026-07.md` (M15).
//!
//! It is **not** production wiring. Nothing in the Flight service, the ticket
//! surface, the CLI, or the Trino connector reaches this module; it compiles only
//! under the non-default `datafusion-spike` feature, and deleting the feature
//! deletes the spike. It adds NO decode work — every byte it reads goes through
//! `MergeProducer::produce_streaming`, the same call the streaming `do_get` route
//! makes.
//!
//! # Layout
//!
//! * [`scan`] — the shared batch-production seam both benchmark arms consume,
//!   plus the sub-phase timing readback and the read-arm evidence.
//! * [`pushdown`] — DataFusion `Expr` → CQLite predicate translation, fail-closed
//!   so an `Exact` pushdown claim is never made for a predicate the scan does not
//!   actually apply.
//! * [`provider`] — the `TableProvider`.
//! * [`exec`] — the single-partition `ExecutionPlan`.
//! * [`rowwise`] — row-at-a-time evaluation over an Arrow batch: the row-engine
//!   arm, and the caveat that it UNDERSTATES the production row engine.
//! * [`rss`] — per-run peak-RSS sampling for the 512Mi pod-budget question.
//! * [`bench`] — scenario/arm definitions and the JSON result record the harness
//!   binary emits.

// `pub` (not `pub(crate)`) because the bench harness binary
// (`src/bin/df_spike_bench.rs`) is a SEPARATE crate linked against this library
// and must reach the scenario/arm surface. `#[doc(hidden)]` keeps the spike out
// of the published API docs — it is a measurement instrument, not API.
#[doc(hidden)]
pub mod bench;
#[doc(hidden)]
pub mod exec;
#[doc(hidden)]
pub mod provider;
#[doc(hidden)]
pub mod pushdown;
#[doc(hidden)]
pub mod rowwise;
#[doc(hidden)]
pub mod rss;
#[doc(hidden)]
pub mod scan;

pub use bench::{
    ArmKind, BenchConfig, BenchError, BenchOutcome, BenchRunner, Scenario, ScenarioKind,
};
pub use provider::{CqliteTableProvider, SpikeError};
pub use scan::{ProbeDelta, ScanOutcome, SubPhaseNanos};
