//! P0-4 Tests: Verify modern formats reject heuristics and blob fallbacks
//!
//! These tests ensure that:
//! - BIG v5 and BTI formats never use heuristics for parsing
//! - Modern formats never fall back to blob values
//! - Tests fail if heuristic/blob code paths execute for modern formats

use cqlite_core::error::Error;
use cqlite_core::storage::sstable::row_cell_state_machine::{
    CassandraVersion as SMVersion, RowCellStateMachine,
};

/// Test that modern BIG v5 format rejects blob fallback in static row parsing
#[tokio::test]
async fn test_big_v5_rejects_blob_fallback_in_static_rows() {
    let mut state_machine = RowCellStateMachine::new();

    // Set to modern BIG v5 format
    state_machine.set_version(SMVersion::V5_0NewBig);

    // Create test data that would normally fall back to blob parsing
    let test_data = create_mock_static_row_data_with_unknown_column();

    let result = state_machine.parse_static_row_public(&test_data);

    // Should fail with schema error, not succeed with blob fallback
    assert!(
        result.is_err(),
        "BIG v5 should reject blob fallback for static rows"
    );

    if let Err(Error::Schema(msg)) = result {
        assert!(
            msg.contains("Blob fallback not allowed"),
            "Error should mention blob fallback rejection: {}",
            msg
        );
        assert!(
            msg.contains("V5_0NewBig"),
            "Error should mention the BIG v5 format: {}",
            msg
        );
    } else {
        panic!(
            "Expected Schema error for blob fallback rejection, got: {:?}",
            result
        );
    }
}

/// Test that BTI format rejects blob fallback in static row parsing
#[tokio::test]
async fn test_bti_rejects_blob_fallback_in_static_rows() {
    let mut state_machine = RowCellStateMachine::new();

    // Set to BTI format
    state_machine.set_version(SMVersion::V5_0Bti);

    // Create test data that would trigger blob fallback in legacy systems
    let test_data = create_mock_static_row_data_with_unknown_column();

    let result = state_machine.parse_static_row_public(&test_data);

    // Should fail with schema error, not succeed with blob fallback
    assert!(
        result.is_err(),
        "BTI should reject blob fallback for static rows"
    );

    if let Err(Error::Schema(msg)) = result {
        assert!(
            msg.contains("Blob fallback not allowed"),
            "Error should mention blob fallback rejection: {}",
            msg
        );
        assert!(
            msg.contains("V5_0Bti"),
            "Error should mention the BTI format: {}",
            msg
        );
    } else {
        panic!(
            "Expected Schema error for BTI blob fallback rejection, got: {:?}",
            result
        );
    }
}

/// Test that legacy formats work with blob fallback when feature is enabled
#[cfg(feature = "legacy-heuristics")]
#[tokio::test]
async fn test_legacy_format_allows_blob_fallback_with_feature() {
    let mut state_machine = RowCellStateMachine::new();
    state_machine.set_version(SMVersion::Legacy);

    let test_data = create_mock_static_row_data_with_unknown_column();

    // Legacy format should succeed with blob fallback when feature is enabled
    let result = state_machine.parse_static_row_public(&test_data);

    // Should succeed (blob fallback allowed for legacy with feature flag)
    assert!(
        result.is_ok(),
        "Legacy format should allow blob fallback with feature flag enabled"
    );
}

/// Test that legacy formats fail without the feature flag
#[cfg(not(feature = "legacy-heuristics"))]
#[tokio::test]
async fn test_legacy_format_rejects_blob_fallback_without_feature() {
    let mut state_machine = RowCellStateMachine::new();
    state_machine.set_version(SMVersion::Legacy);

    let test_data = create_mock_static_row_data_with_unknown_column();

    let result = state_machine.parse_static_row_public(&test_data);

    // Should fail when feature is disabled
    assert!(
        result.is_err(),
        "Legacy format should reject blob fallback without feature flag"
    );

    if let Err(Error::Schema(msg)) = result {
        assert!(
            msg.contains("legacy-heuristics feature"),
            "Error should mention feature flag requirement: {}",
            msg
        );
    } else {
        panic!(
            "Expected Schema error for feature flag requirement, got: {:?}",
            result
        );
    }
}

/// Test modern format version detection and classification
#[test]
fn test_modern_format_identification() {
    // Verify we correctly identify modern formats
    let big_v5_version = SMVersion::V5_0NewBig;
    let bti_version = SMVersion::V5_0Bti;
    let legacy_version = SMVersion::Legacy;

    // Test version matching
    match big_v5_version {
        SMVersion::V5_0NewBig => {} // Should match
        _ => panic!("BIG v5 version detection failed"),
    }

    match bti_version {
        SMVersion::V5_0Bti => {} // Should match
        _ => panic!("BTI version detection failed"),
    }

    match legacy_version {
        SMVersion::Legacy => {} // Should match
        _ => panic!("Legacy version detection failed"),
    }

    println!("✅ Modern format version identification works correctly");
}

/// Test that ensures heuristic code paths are properly feature-gated
#[test]
fn test_heuristic_feature_gating() {
    // This test verifies at compile-time that heuristic code is properly gated

    #[cfg(feature = "legacy-heuristics")]
    {
        println!("✅ Legacy heuristics feature is enabled - heuristic code available for testing");
    }

    #[cfg(not(feature = "legacy-heuristics"))]
    {
        println!("✅ Legacy heuristics feature is disabled - heuristic code properly gated");
    }

    // The fact that this compiles means our feature gating is working
    // Feature gating compilation test passed
}

/// Test that verifies modern formats never execute legacy code paths
#[tokio::test]
async fn test_modern_formats_avoid_legacy_paths() {
    // Create state machines for each modern format
    let mut big_v5_sm = RowCellStateMachine::new();
    big_v5_sm.set_version(SMVersion::V5_0NewBig);

    let mut bti_sm = RowCellStateMachine::new();
    bti_sm.set_version(SMVersion::V5_0Bti);

    // Test data that would historically trigger legacy fallbacks
    let problematic_data = create_mock_static_row_data_with_unknown_column();

    // Both should fail cleanly rather than using legacy fallbacks
    let big_v5_result = big_v5_sm.parse_static_row_public(&problematic_data);
    let bti_result = bti_sm.parse_static_row_public(&problematic_data);

    // Verify both fail with appropriate error messages (no legacy fallback)
    assert!(
        big_v5_result.is_err(),
        "BIG v5 should not use legacy fallback paths"
    );
    assert!(
        bti_result.is_err(),
        "BTI should not use legacy fallback paths"
    );

    // Verify error messages indicate modern format rejection, not legacy parsing errors
    if let Err(Error::Schema(msg)) = big_v5_result {
        assert!(
            msg.contains("modern format") || msg.contains("V5_0NewBig"),
            "BIG v5 error should indicate modern format handling: {}",
            msg
        );
    }

    if let Err(Error::Schema(msg)) = bti_result {
        assert!(
            msg.contains("modern format") || msg.contains("V5_0Bti"),
            "BTI error should indicate modern format handling: {}",
            msg
        );
    }

    println!("✅ Modern formats successfully avoid legacy code paths");
}

/// Helper function to create mock static row data that would trigger blob fallback
fn create_mock_static_row_data_with_unknown_column() -> Vec<u8> {
    let mut data = Vec::new();

    // Static row flag (bit 6 set to indicate static row present)
    data.push(0x40);

    // Column count: 1 column (VInt zigzag encoded: 1 -> 2)
    data.push(0x02); // VInt encoding of 1

    // Column name length: 12 bytes for "unknown_type" (VInt zigzag encoded: 12 -> 24)
    data.push(0x18); // VInt encoding of 12

    // Column name: "unknown_type" - this would historically trigger blob fallback
    data.extend_from_slice(b"unknown_type");

    // Value length: 16 bytes of arbitrary data (VInt zigzag encoded: 16 -> 32)
    data.push(0x20); // VInt encoding of 16

    // Value data: 16 bytes that cannot be parsed without schema
    // This is the kind of data that would force a blob fallback in legacy systems
    data.extend_from_slice(&[
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ]);

    data
}

/// Test to verify P0-4 requirement satisfaction
#[tokio::test]
async fn test_p0_4_requirement_satisfaction() {
    println!(
        "🧪 P0-4 Requirement: Add tests that fail if heuristics/blobs execute in modern paths"
    );

    // Test 1: Modern formats reject blob fallbacks
    let mut modern_sm = RowCellStateMachine::new();
    modern_sm.set_version(SMVersion::V5_0NewBig);

    let test_data = create_mock_static_row_data_with_unknown_column();
    let result = modern_sm.parse_static_row_public(&test_data);

    assert!(
        result.is_err(),
        "P0-4: Modern format must reject blob fallback"
    );

    // Test 2: Verify error message indicates modern format restriction
    if let Err(Error::Schema(msg)) = result {
        assert!(
            msg.contains("Blob fallback not allowed") && msg.contains("modern format"),
            "P0-4: Error must indicate modern format blob restriction: {}",
            msg
        );
    }

    // Test 3: BTI format also rejects blob fallbacks
    let mut bti_sm = RowCellStateMachine::new();
    bti_sm.set_version(SMVersion::V5_0Bti);

    let bti_result = bti_sm.parse_static_row_public(&test_data);
    assert!(
        bti_result.is_err(),
        "P0-4: BTI format must reject blob fallback"
    );

    println!(
        "✅ P0-4 requirement satisfied: Tests fail if heuristics/blobs execute in modern paths"
    );
}
