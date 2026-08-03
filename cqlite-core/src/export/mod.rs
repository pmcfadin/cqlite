//! Embeddable export writers (Epic #682)
//!
//! This module hosts writers that convert query results into external file
//! formats so that non-CLI consumers (projection services, the Python/Node
//! bindings, library embedders) can produce them without shelling out to the
//! CLI.
//!
//! # Feature flags
//!
//! | Submodule       | Feature                    | In defaults? |
//! |-----------------|----------------------------|--------------|
//! | `arrow_convert` | `arrow`                    | No           |
//! | `parquet`       | `parquet`                  | No           |
//! | `delta_schema`  | `delta-scan` + `arrow`     | No           |
//! | `delta_parquet` | `delta-scan` + `parquet`   | No           |
//!
//! The `arrow` feature pulls in the `arrow` crate as an optional dependency
//! and exposes `build_arrow_schema` / `rows_to_record_batch` for use by any
//! consumer that wants Arrow RecordBatches without Parquet.
//!
//! The `parquet` feature depends on `arrow` and additionally pulls in the
//! `parquet` crate; it is off by default so the default build's dependency
//! surface is unchanged.
//!
//! The `delta_parquet` module (DS8, Issue #704) is compiled only when both
//! `delta-scan` and `parquet` are enabled.  It depends on `delta_schema`
//! (DS7, Issue #703) for schema derivation and reuses the epic #682 Arrow
//! writer machinery — no forked writer.
//!
//! # External-committer boundary
//!
//! Per the decisions in `docs/architecture/cassandra-sidecar-parquet-projections.md`,
//! CQLite produces Parquet **files** only.  Committing those files to lakehouse
//! table formats (Iceberg, Delta) — manifest/metadata transactions, snapshot
//! management — is the job of an external committer and is deliberately out of
//! scope for this crate.

#[cfg(feature = "arrow")]
pub mod arrow_convert;

// The CQL → Arrow converter, split by responsibility (epic #1116; issue #3096
// Phase 0a). `arrow_convert` keeps the public entry points and the top-level
// column dispatch and re-exports everything the rest of the crate used to reach
// through it, so no `use` path outside this module changed.
#[cfg(feature = "arrow")]
mod arrow_builders_nested;
#[cfg(feature = "arrow")]
mod arrow_builders_scalar;
#[cfg(feature = "arrow")]
mod arrow_convert_util;
#[cfg(feature = "arrow")]
mod arrow_schema;
#[cfg(feature = "arrow")]
mod arrow_typed_value;

// Per-column accessor resolution for Arrow conversion (issue #1495, AE1): resolves
// each schema column once and transposes rows into per-column value slices,
// killing the per-cell `values.get(name)` string-hash lookup (parser epic J1).
#[cfg(feature = "arrow")]
pub(crate) mod arrow_columnar;

// CQL decimal rescaling for Arrow/Parquet export (split out of `arrow_convert`,
// epic #1116; issue #1755 bounded/fail-closed fix).
#[cfg(feature = "arrow")]
mod arrow_decimal;

// Conservative pre-materialization Arrow payload-byte estimator (issue #2825):
// the per-row width the `cqlite-flight` byte-cap accumulates BEFORE
// `rows_to_record_batch` allocates a batch. Its own file — `arrow_convert.rs` is
// far over the campsite threshold (epic #1116).
#[cfg(feature = "arrow")]
pub mod arrow_size;

// The SHARED Arrow row-shape corpus (issues #2825/#2932). Compiled only for this
// crate's own tests or under the opt-in `arrow-shape-corpus` feature, which
// `cqlite-flight` enables as a DEV-dependency so its published-capacity-bound
// guard runs over the same shapes the estimator's conservatism contract does.
// A default `cargo build -p cqlite-core` links none of it (the `fuzz` /
// `bench-internals` / `work-counters` precedent).
#[cfg(all(feature = "arrow", any(test, feature = "arrow-shape-corpus")))]
#[doc(hidden)]
pub mod arrow_shape_corpus;

#[cfg(feature = "parquet")]
pub mod parquet;

// Re-export the public arrow_convert API at the `export` module level.
#[cfg(feature = "arrow")]
pub use arrow_convert::{
    build_arrow_schema, rows_to_record_batch, rows_to_record_batch_with_schema, ArrowConvertError,
};

// Re-export the byte estimator beside the converter it models (issue #2825).
// Deliberately narrow: the structural charging constants stay PRIVATE to
// `arrow_size` (review N4) — they are tuning parameters of the estimate, not a
// semver contract. The two node budgets are public because the fail-closed
// behaviour they define IS part of the contract.
#[cfg(feature = "arrow")]
pub use arrow_size::{
    arrow_payload_bytes, estimate_arrow_row_bytes, MAX_ESTIMATE_LEAF_SLOTS, MAX_ESTIMATE_NODES,
};

// Delta-scan Arrow schema derivation (Epic #696, Issue #703 / DS7).
// Requires both `delta-scan` (for the CDC envelope model) and `arrow` (for
// ArrowDataType / Field / Schema).  The `parquet` feature is NOT required —
// schema derivation is independent of writing Parquet files.
#[cfg(all(feature = "delta-scan", feature = "arrow"))]
pub mod delta_schema;

#[cfg(all(feature = "delta-scan", feature = "arrow"))]
pub use delta_schema::{derive_delta_schema, DeltaSchemaError, DeltaSchemaOpts};

// Delta-scan Parquet writer (Epic #696, Issue #704 / DS8).
// Requires both `delta-scan` AND `parquet` (which implies `arrow`).
// Absent from default builds — neither delta-scan nor parquet is in the
// default feature set.
#[cfg(all(feature = "delta-scan", feature = "parquet"))]
pub mod delta_parquet;

#[cfg(all(feature = "delta-scan", feature = "parquet"))]
pub use delta_parquet::{
    write_delta_records_to_bytes, DeltaParquetCompression, DeltaParquetError, DeltaParquetOptions,
    DeltaParquetWriter,
};
