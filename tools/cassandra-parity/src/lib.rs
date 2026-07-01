//! Library surface for the `cassandra-parity` tool so integration tests can
//! exercise the linter, coverage, and report logic directly.

pub mod claim_scan;
pub mod corpus_audit;
pub mod coverage;
pub mod enums;
pub mod failure_artifact;
pub mod lint;
pub mod model;
pub mod report;
pub mod report_dedup;
pub mod retention;
pub mod tier_contract;
pub mod workflow_check;
