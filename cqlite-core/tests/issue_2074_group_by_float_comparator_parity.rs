//! Issue #2074: GROUP BY on a `float`/`double` column must group by Cassandra's
//! total-order comparator (`float_cmp`), NOT IEEE `==`:
//!   * `-0.0` and `+0.0` are DISTINCT groups (Cassandra orders them apart);
//!   * ALL NaN bit-patterns collapse into ONE group (Java `doubleToLongBits`).
//!
//! Wiring evidence (acceptance criterion 3): this drives GROUP BY THROUGH the
//! real query engine end-to-end. It writes a small UNCOMPRESSED SSTable via the
//! public write engine (one single-row partition per float value), ingests it,
//! and runs `SELECT fv, COUNT(*) ... GROUP BY fv` via `Database::execute`,
//! asserting the group partitioning. `-0.0` and both NaN bit patterns are raw
//! IEEE bytes, so they round-trip exactly through write → read → the aggregation
//! `find_or_init_group` group-key path.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_2074_group_by_float_comparator_parity

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine"
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "gbf_ks";
const TBL: &str = "floats";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  fv double\n);\n")
}

fn write_mutation(id: i32, fv: f64, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "fv".to_string(),
        value: Value::Float(fv),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// Build a single-generation fixture with one single-row partition per float
/// value, ingest it, and return an open `Database`.
async fn open_float_fixture(values: &[f64]) -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        let values: Vec<f64> = values.to_vec();
        tokio::task::spawn_blocking(move || {
            use cqlite_core::schema::parse_cql_schema;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
            let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine");
            for (i, v) in values.iter().enumerate() {
                engine
                    .write(write_mutation(i as i32, *v, 100))
                    .expect("write row");
            }
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("must produce an SSTable");
            rt.block_on(engine.close()).expect("close");
        })
        .await
        .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    (result.database, temp_dir)
}

/// The COUNT(*) BigInt in a GROUP BY result row (the sole non-`fv` BigInt value).
fn count_of_row(row: &cqlite_core::query::result::QueryRow) -> Option<i64> {
    row.values.iter().find_map(|(k, v)| match v {
        Value::BigInt(n) if k.as_ref() != "fv" => Some(*n),
        _ => None,
    })
}

/// The grouped `fv` float value in a result row.
fn fv_of_row(row: &cqlite_core::query::result::QueryRow) -> Option<f64> {
    match row.values.get("fv") {
        Some(Value::Float(f)) => Some(*f),
        _ => None,
    }
}

/// GROUP BY on a `double` column groups by Cassandra's comparator through the
/// public `Database::execute` path:
///   * `+0.0` and `-0.0` → TWO distinct single-row groups;
///   * two DIFFERENT NaN bit patterns → ONE group aggregating both rows;
///   * a plain duplicated finite value → ONE group of its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_by_double_uses_cassandra_comparator_via_execute() {
    // Two different NaN bit patterns prove canonicalization (both are raw IEEE
    // bytes, so they round-trip through write → read exactly).
    let nan_a = f64::NAN;
    let nan_b = f64::from_bits(0xFFF8_0000_0000_0001);
    assert!(nan_a.is_nan() && nan_b.is_nan());

    // id: 0=+0.0, 1=-0.0, 2=NaN_a, 3=NaN_b, 4=1.0, 5=1.0
    let values = [0.0_f64, -0.0, nan_a, nan_b, 1.0, 1.0];
    let (db, _tmp) = open_float_fixture(&values).await;

    let result = db
        .execute(&format!("SELECT fv, COUNT(*) FROM {KS}.{TBL} GROUP BY fv"))
        .await
        .expect("GROUP BY query must execute through the public path");

    // Categorize the returned groups by their grouped `fv` value.
    let mut nan_groups = 0usize;
    let mut nan_group_count = 0i64;
    let mut pos_zero_groups = 0usize;
    let mut neg_zero_groups = 0usize;
    let mut one_groups = 0usize;
    let mut one_group_count = 0i64;

    for row in &result.rows {
        let fv = fv_of_row(row).expect("group row carries an fv double");
        let count = count_of_row(row).expect("group row carries a COUNT(*) BigInt");
        if fv.is_nan() {
            nan_groups += 1;
            nan_group_count = count;
        } else if fv == 0.0 && fv.is_sign_positive() {
            pos_zero_groups += 1;
        } else if fv == 0.0 && fv.is_sign_negative() {
            neg_zero_groups += 1;
        } else if fv == 1.0 {
            one_groups += 1;
            one_group_count = count;
        } else {
            panic!("unexpected group value {fv:?}");
        }
    }

    // Signed zeros are DISTINCT groups (#2074) — exactly one of each.
    assert_eq!(
        pos_zero_groups, 1,
        "+0.0 must form exactly one group (rows: {:?})",
        result.rows
    );
    assert_eq!(
        neg_zero_groups, 1,
        "-0.0 must form its OWN group distinct from +0.0 (#2074) (rows: {:?})",
        result.rows
    );

    // ALL NaN bit patterns collapse into ONE group aggregating BOTH NaN rows.
    assert_eq!(
        nan_groups, 1,
        "all NaN rows (two different bit patterns) must form exactly ONE group (#2074) \
         (rows: {:?})",
        result.rows
    );
    assert_eq!(
        nan_group_count, 2,
        "the single NaN group must aggregate BOTH NaN rows (#2074)"
    );

    // The plain finite duplicate is one group of both rows (sanity control).
    assert_eq!(one_groups, 1, "1.0 must form exactly one group");
    assert_eq!(one_group_count, 2, "the 1.0 group aggregates both 1.0 rows");

    // Total distinct groups: +0.0, -0.0, NaN, 1.0 = 4.
    assert_eq!(
        result.rows.len(),
        4,
        "exactly four distinct groups (+0.0, -0.0, NaN, 1.0); got {:?}",
        result.rows
    );
}
