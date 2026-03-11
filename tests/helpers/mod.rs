//! Test helpers module
//!
//! This module provides shared utilities for integration tests,
//! including Docker integration and Cassandra test runners.

#[cfg(feature = "docker-integration")]
pub mod cassandra_test;
#[cfg(feature = "docker-integration")]
pub mod docker;

#[cfg(feature = "docker-integration")]
pub use cassandra_test::{CassandraTestRunner, ComparisonResult, TestResult, TestSuiteResult};
#[cfg(feature = "docker-integration")]
pub use docker::{CassandraContainer, CqlshOutput, DockerCqlshClient, SstableloaderResult};
