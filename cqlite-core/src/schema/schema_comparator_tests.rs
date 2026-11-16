//! `comparators_are_compatible` for uuid / timeuuid (issue #4534: `TimeUuid` is its own
//! `ComparatorType` now, so the exact-match arm must cover it too).

use super::super::{Column, KeyColumn, TableSchema};

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: ["uuid", "timeuuid"]
            .iter()
            .map(|t| Column {
                name: format!("c_{t}"),
                data_type: (*t).into(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect(),
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

#[test]
fn timeuuid_and_uuid_columns_are_compatible_with_their_own_type() {
    let s = schema();
    assert!(s
        .is_column_type_compatible("c_timeuuid", "timeuuid")
        .unwrap());
    assert!(s.is_column_type_compatible("c_uuid", "uuid").unwrap());
}

#[test]
fn timeuuid_and_uuid_are_distinct_comparators() {
    let s = schema();
    assert!(!s.is_column_type_compatible("c_timeuuid", "uuid").unwrap());
    assert!(!s.is_column_type_compatible("c_uuid", "timeuuid").unwrap());
}
