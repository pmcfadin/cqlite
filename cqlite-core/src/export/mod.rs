//! Embeddable export writers (Epic #682)
//!
//! This module hosts writers that convert query results into external file
//! formats so that non-CLI consumers (projection services, the Python/Node
//! bindings, library embedders) can produce them without shelling out to the
//! CLI.
//!
//! # Feature flags
//!
//! | Submodule       | Feature   | In defaults? |
//! |-----------------|-----------|--------------|
//! | `arrow_convert` | `arrow`   | No           |
//! | `parquet`       | `parquet` | No           |
//!
//! The `arrow` feature pulls in the `arrow` crate as an optional dependency
//! and exposes `build_arrow_schema` / `rows_to_record_batch` for use by any
//! consumer that wants Arrow RecordBatches without Parquet.
//!
//! The `parquet` feature depends on `arrow` and additionally pulls in the
//! `parquet` crate; it is off by default so the default build's dependency
//! surface is unchanged.
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

#[cfg(feature = "parquet")]
pub mod parquet;

// Re-export the public arrow_convert API at the `export` module level.
#[cfg(feature = "arrow")]
pub use arrow_convert::{build_arrow_schema, rows_to_record_batch, ArrowConvertError};

// Delta-scan Arrow schema derivation (Epic #696, Issue #703).
// Requires both `delta-scan` (for the CDC envelope model) and `arrow` (for
// ArrowDataType / Field / Schema).  The `parquet` feature is NOT required —
// schema derivation is independent of writing Parquet files.
#[cfg(all(feature = "delta-scan", feature = "arrow"))]
pub mod delta_schema;

#[cfg(all(feature = "delta-scan", feature = "arrow"))]
pub use delta_schema::{derive_delta_schema, DeltaSchemaError, DeltaSchemaOpts};
