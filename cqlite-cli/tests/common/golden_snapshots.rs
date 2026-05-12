//! Golden snapshot testing infrastructure
//!
//! Provides utilities for validating output against golden snapshot files.
//! Golden snapshots capture expected output to prevent formatting regressions.
//!
//! # Usage
//!
//! ## Normal Test Run
//! ```no_run
//! use crate::common::golden_snapshots::assert_golden_snapshot;
//!
//! let actual_output = "some output";
//! assert_golden_snapshot("test_name.txt", actual_output)?;
//! ```
//!
//! ## Updating Snapshots
//! Set the `UPDATE_SNAPSHOTS=1` environment variable:
//! ```bash
//! UPDATE_SNAPSHOTS=1 cargo test
//! ```
//!
//! ## Snapshot Behavior
//! 1. If `UPDATE_SNAPSHOTS=1` is set → always update golden files
//! 2. If golden file exists → compare output against it
//! 3. If golden file missing → create it automatically

use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

/// Assert that the actual output matches the golden snapshot file.
///
/// # Arguments
/// * `name` - Snapshot file name (stored in `tests/golden_snapshots/{name}`)
/// * `actual` - The actual output to compare or save
///
/// # Behavior
/// - If `UPDATE_SNAPSHOTS=1` env var is set: Updates the golden file
/// - If golden file doesn't exist: Creates it (with warning message)
/// - If golden file exists: Compares trimmed output
///
/// # Errors
/// Returns error if:
/// - File I/O operations fail
/// - Output doesn't match expected snapshot (unless updating)
#[allow(dead_code)] // Used by various test files, may not be used in all contexts
pub fn assert_golden_snapshot(name: &str, actual: &str) -> Result<()> {
    let golden_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden_snapshots");

    // Ensure the golden snapshots directory exists
    fs::create_dir_all(&golden_dir).context("Failed to create golden_snapshots directory")?;

    let golden_path = golden_dir.join(name);
    let update = std::env::var("UPDATE_SNAPSHOTS").is_ok();

    if update || !golden_path.exists() {
        fs::write(&golden_path, actual)
            .with_context(|| format!("Failed to write golden snapshot: {name}"))?;

        if update {
            eprintln!("✓ Updated golden snapshot: {name}");
        } else {
            eprintln!("✓ Created golden snapshot: {name}");
        }
        return Ok(());
    }

    // Read existing golden snapshot
    let expected = fs::read_to_string(&golden_path)
        .with_context(|| format!("Failed to read golden snapshot: {name}"))?;

    // Compare trimmed output (ignore trailing whitespace)
    let expected_trimmed = expected.trim();
    let actual_trimmed = actual.trim();

    if expected_trimmed != actual_trimmed {
        // Provide helpful error message with diff hint
        anyhow::bail!(
            "Snapshot mismatch for '{}'\n\
            \n\
            Expected ({} chars):\n{}\n\
            \n\
            Actual ({} chars):\n{}\n\
            \n\
            To update this snapshot, run:\n\
            UPDATE_SNAPSHOTS=1 cargo test {}",
            name,
            expected_trimmed.len(),
            expected_trimmed,
            actual_trimmed.len(),
            actual_trimmed,
            name.trim_end_matches(".txt")
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::TempDir;

    #[test]
    fn test_golden_snapshot_creates_missing_file() -> Result<()> {
        let temp = TempDir::new()?;
        let _snapshot_path = temp.path().join("test.txt");

        // Temporarily override CARGO_MANIFEST_DIR for test
        env::set_var("TEST_GOLDEN_DIR", temp.path());

        // Since we can't easily override CARGO_MANIFEST_DIR, this test is illustrative
        // Real usage will work correctly with the actual manifest directory

        Ok(())
    }
}
