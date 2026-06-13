//! Embeddable export writers (Epic #682)
//!
//! This module hosts writers that convert query results into external file
//! formats so that non-CLI consumers (projection services, the Python/Node
//! bindings, library embedders) can produce them without shelling out to the
//! CLI.
//!
//! # Feature flags
//!
//! | Submodule | Feature   | In defaults? |
//! |-----------|-----------|--------------|
//! | `parquet` | `parquet` | No           |
//!
//! The `parquet` feature pulls in the `arrow` and `parquet` crates as
//! optional dependencies; the default build's dependency surface is
//! unchanged.
//!
//! # External-committer boundary
//!
//! Per the decisions in `docs/architecture/cassandra-sidecar-parquet-projections.md`,
//! CQLite produces Parquet **files** only.  Committing those files to lakehouse
//! table formats (Iceberg, Delta) — manifest/metadata transactions, snapshot
//! management — is the job of an external committer and is deliberately out of
//! scope for this crate.

#[cfg(feature = "parquet")]
pub mod parquet;
