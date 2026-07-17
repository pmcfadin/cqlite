//! Issue #1674 (R3): cache ordered column lists + per-column complexity so the
//! schema-constant column ordering is NOT recomputed per row and
//! `is_complex_column` (which allocates a lowercased `String` per call) never
//! runs on the per-row hot path.

#![allow(unused_imports)]

use super::super::column_cache::is_complex_scope::IsComplexScope;
use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use std::collections::HashMap;

/// Build a schema with `c` regular columns (`c_00`..) plus an `id` PK and `ck`
/// clustering column. A mix of simple + complex types so the ordering key
/// `(is_complex, name)` is exercised.
fn schema_with_regular_columns(c: usize) -> TableSchema {
    let columns: Vec<Column> = (0..c)
        .map(|i| Column {
            name: format!("c_{i:02}"),
            // Alternate simple/complex to exercise both `is_complex` branches.
            data_type: if i % 3 == 0 {
                "set<text>".to_string()
            } else {
                "text".to_string()
            },
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Writing R rows must invoke `is_complex_column` a number of times bounded by
/// `f(C)` — INDEPENDENT of the row count R — because the ordered column lists and
/// per-column `is_complex` classification are computed exactly ONCE per writer
/// and cached (issue #1674, R3).
///
/// On `main` (pre-R3) each `write_row` re-filters + re-sorts the columns via
/// `sort_by_key(column_order_key)` (calling `is_complex_column` O(C·log C) times
/// per `regular_columns` call, and `regular_columns` runs ~3× per row) plus a
/// per-op complex check and a per-column check in `write_merged_cells` — so R
/// rows produce far more than R·C calls, growing linearly with R. This test with
/// R = 200 therefore FAILS on main (>= R = 200 > the O(C) bound) and PASSES after
/// R3 (a single O(C) cache build; here exactly C simple lowercasings, well below
/// the generous `4·C` bound).
#[test]
fn is_complex_column_is_not_called_per_row_issue_1674() {
    const C: usize = 6;
    const R: u64 = 200;

    let schema = schema_with_regular_columns(C);
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let scope = IsComplexScope::new();
    for r in 0..R {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(r as i32));
        // One non-null write per simple regular column (skip the complex ones to
        // keep the write path a pure scalar emission; the classifier still runs
        // over ALL columns on main via the ordering sort + per-column checks).
        let ops: Vec<CellOperation> = (0..C)
            .filter(|i| i % 3 != 0)
            .map(|i| CellOperation::Write {
                column: format!("c_{i:02}"),
                value: Value::text("x".to_string()),
            })
            .collect();
        let mutation = Mutation::new(table_id, pk, Some(ck), ops, 1_001_000, None);
        writer.write_row(&mutation, &schema).unwrap();
    }
    let calls = scope.count();
    drop(scope);

    // After R3 the cache is built once: one `is_complex_column` pass over the C
    // columns (== C calls). Bound generously at 4·C to stay implementation-robust
    // while remaining FAR below R. On main this scaled with R (>= R = 200).
    let bound = 4 * C as u64;
    assert!(
        calls <= bound,
        "is_complex_column ran {calls} times for R={R} rows (C={C}); expected O(C) \
         (<= {bound}), INDEPENDENT of R. On main it scales with R (>= R = {R})."
    );
}
