//! Comprehensive regression tests for Issue #37 - Read-time reconciliation

use sstabledump_validator::{
    reconciliation::{ReconciliationConfig, ReconciliationEngine},
    test_datasets::ReconciliationTestDatasets,
};
use std::time::Duration;

#[tokio::test]
async fn test_reconciliation_overlapping_writes() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator
        .generate_overlapping_writes_dataset()
        .await
        .unwrap();

    let engine = ReconciliationEngine::new();
    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    // Both datasets should have identical reconciliation results
    assert_eq!(
        result.cassandra_reconciled.len(),
        result.cqlite_reconciled.len()
    );

    // Check that newest write wins
    let cassandra_partition = &result.cassandra_reconciled[0];
    assert_eq!(cassandra_partition.visible_cells, 1);

    let row_result = &cassandra_partition.reconciled_rows[0];
    let reconciled_cell = row_result.reconciled_cells.get("value").unwrap();
    assert!(reconciled_cell.value.is_some());

    if let Some(cell) = &reconciled_cell.value {
        assert_eq!(
            cell.value,
            sstabledump_validator::CellValue::Text("third_write_winner".to_string())
        );
    }
}

#[tokio::test]
async fn test_reconciliation_ttl_expiration() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator.generate_expired_ttl_dataset().await.unwrap();

    // Use future time to ensure TTL has expired
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
        + 3600_000_000; // 1 hour in the future

    let engine = ReconciliationEngine::with_time(current_time);
    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    // Verify TTL expiration behavior
    let cassandra_partition = &result.cassandra_reconciled[0];

    // Should have cells with different visibility states
    let mut expired_cells = 0;
    let mut active_cells = 0;

    for row_result in &cassandra_partition.reconciled_rows {
        for reconciled_cell in row_result.reconciled_cells.values() {
            if reconciled_cell.affected_by_ttl {
                expired_cells += 1;
            } else if reconciled_cell.value.is_some() {
                active_cells += 1;
            }
        }
    }

    assert!(expired_cells > 0, "Some cells should be expired by TTL");
    assert!(active_cells > 0, "Some cells should still be active");
}

#[tokio::test]
async fn test_reconciliation_row_vs_cell_tombstones() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator
        .generate_row_vs_cell_tombstones_dataset()
        .await
        .unwrap();

    let engine = ReconciliationEngine::new();
    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    let cassandra_partition = &result.cassandra_reconciled[0];

    // Check row vs cell tombstone precedence
    let mut cell_tombstone_found = false;
    let mut row_tombstone_survived = false;

    for row_result in &cassandra_partition.reconciled_rows {
        for reconciled_cell in row_result.reconciled_cells.values() {
            if reconciled_cell.affected_by_tombstone {
                cell_tombstone_found = true;
            }
            if reconciled_cell.value.is_some() && reconciled_cell.affected_by_tombstone {
                // Cell survived row tombstone due to newer timestamp
                row_tombstone_survived = true;
            }
        }
    }

    assert!(cell_tombstone_found, "Cell tombstones should be applied");
    assert!(
        row_tombstone_survived,
        "Newer data should survive row tombstones"
    );
}

#[tokio::test]
async fn test_reconciliation_complex_mixed_scenarios() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator.generate_complex_mixed_dataset().await.unwrap();

    let engine = ReconciliationEngine::new();
    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    let cassandra_partition = &result.cassandra_reconciled[0];

    // Complex scenario should have various visibility states
    let mut visible_cells = 0;
    let mut deleted_cells = 0;
    let mut expired_cells = 0;

    for row_result in &cassandra_partition.reconciled_rows {
        for reconciled_cell in row_result.reconciled_cells.values() {
            if reconciled_cell.value.is_some() {
                visible_cells += 1;
            } else if reconciled_cell.affected_by_ttl {
                expired_cells += 1;
            } else if reconciled_cell.affected_by_tombstone {
                deleted_cells += 1;
            }
        }
    }

    assert!(visible_cells > 0, "Some cells should be visible");
    assert!(deleted_cells > 0, "Some cells should be deleted");
    assert!(expired_cells > 0, "Some cells should be expired");
}

#[tokio::test]
async fn test_reconciliation_multi_generation_conflicts() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator
        .generate_multi_generation_conflicts_dataset()
        .await
        .unwrap();

    let engine = ReconciliationEngine::new();
    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    let cassandra_partition = &result.cassandra_reconciled[0];

    // Final generation should win despite intermediate tombstone
    let row_result = &cassandra_partition.reconciled_rows[0];
    let reconciled_cell = row_result.reconciled_cells.get("contested_cell").unwrap();

    assert!(
        reconciled_cell.value.is_some(),
        "Final generation should resurrect the cell"
    );

    if let Some(cell) = &reconciled_cell.value {
        assert_eq!(
            cell.value,
            sstabledump_validator::CellValue::Text("generation_4_resurrection".to_string())
        );
    }

    // Should have processed multiple candidates
    assert!(
        reconciled_cell.candidates.len() >= 4,
        "Should have multiple generation candidates"
    );
}

#[tokio::test]
async fn test_reconciliation_range_tombstones() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator.generate_range_tombstones_dataset().await.unwrap();

    let engine = ReconciliationEngine::new();
    let mut result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    // Apply range tombstone: inclusive range from "key_a" to "key_d"
    let range_tombstone =
        generator.create_test_range_tombstone(Some("key_a"), Some("key_d"), true, true);

    for partition_result in &mut result.cassandra_reconciled {
        engine.apply_range_tombstones(partition_result, &[range_tombstone.clone()]);
    }

    let cassandra_partition = &result.cassandra_reconciled[0];

    // Only "key_z" should survive the range tombstone
    let mut surviving_rows = 0;
    for row_result in &cassandra_partition.reconciled_rows {
        for reconciled_cell in row_result.reconciled_cells.values() {
            if reconciled_cell.value.is_some() {
                surviving_rows += 1;
            }
        }
    }

    assert_eq!(
        surviving_rows, 1,
        "Only one row should survive range tombstone"
    );
}

#[tokio::test]
async fn test_reconciliation_dataset_comparison() {
    let mut generator = ReconciliationTestDatasets::new();
    let datasets = generator.generate_all_datasets().await.unwrap();

    let engine = ReconciliationEngine::new();

    for (dataset_name, dataset_pair) in datasets {
        let result = engine
            .reconcile_datasets(&dataset_pair.cassandra_data, &dataset_pair.cqlite_data)
            .await
            .unwrap();

        // Compare reconciled results
        let differences = result.compare();

        // For test datasets, Cassandra and CQLite should have identical reconciliation
        assert!(
            differences.is_empty(),
            "Dataset {dataset_name} should have zero reconciliation differences"
        );
    }
}

#[tokio::test]
async fn test_reconciliation_performance() {
    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator.generate_complex_mixed_dataset().await.unwrap();

    let engine = ReconciliationEngine::new();

    let start = std::time::Instant::now();
    let _result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Reconciliation should complete quickly for test datasets
    assert!(
        duration < Duration::from_millis(100),
        "Reconciliation should complete within 100ms for test data"
    );
}

#[tokio::test]
async fn test_reconciliation_strict_cassandra_semantics() {
    let config = ReconciliationConfig {
        strict_cassandra_semantics: true,
        ttl_grace_period: 0,
        enable_range_tombstones: true,
        gc_grace_seconds: 864000, // 10 days
    };

    let engine = ReconciliationEngine::with_config(config);

    let mut generator = ReconciliationTestDatasets::new();
    let dataset = generator
        .generate_ttl_tombstone_interaction_dataset()
        .await
        .unwrap();

    let result = engine
        .reconcile_datasets(&dataset.cassandra_data, &dataset.cqlite_data)
        .await
        .unwrap();

    // Strict semantics should produce consistent results
    let cassandra_partition = &result.cassandra_reconciled[0];
    let cqlite_partition = &result.cqlite_reconciled[0];

    assert_eq!(
        cassandra_partition.visible_cells, cqlite_partition.visible_cells,
        "Strict semantics should produce identical visibility"
    );
}
