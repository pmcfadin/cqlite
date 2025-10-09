//! Discovery module for SSTable directory scanning and schema coverage
//!
//! This module provides functionality for discovering SSTables in a data directory,
//! computing schema coverage, and generating coverage badges for status reporting.

pub mod coverage;
pub mod scanner;
pub mod service;

pub use coverage::{CoverageBadge, CoverageCalculator, CoverageInfo};
pub use scanner::{KeyspaceInfo, ScanResult, Scanner, TableInfo};
pub use service::{DiscoveryService, DiscoverySummary};
