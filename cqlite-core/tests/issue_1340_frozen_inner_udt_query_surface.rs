//! Issue #1340 — public query-surface wiring evidence for typed inner-UDT decode.
//!
//! PUBLIC SURFACE + CALL CHAIN under test:
//!   `cqlite_core::ingestion::ingest(IngestionConfig{schema, data_dir})`
//!     → builds a `Database` (schema loader parses the committed DDL, incl. the
//!       `person`/`address` CREATE TYPE defs)
//!   → `Database::execute("SELECT lp, ma FROM test_compactionparityudt.udt_collections")`
//!     → `SelectExecutor` → `SSTableReader` scan → V5CompressedLegacy row decode
//!       (the frozen-collection element decoder threaded with the on-disk
//!        SerializationHeader marshal type by issue #1340)
//!   → `QueryResult.rows[*].values["lp"|"ma"]`
//!
//! This is END-TO-END evidence (NOT a helper-only unit test, per spec Req 4): a
//! real `SELECT` over the committed #1240/#1020 SSTable fixture must expose
//! STRUCTURED inner person/address UDT field values that match the sstabledump
//! JSONL golden. Dataset-guarded: fixture-present-but-zero-rows is a FAILURE.
//!
//! Requires the `cli-helpers` feature (the public `ingestion` surface); the whole
//! file is gated so the default-feature test build neither compiles nor skips it.
#![cfg(feature = "cli-helpers")]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::types::{UdtValue, Value};
use cqlite_core::Database;

const KEYSPACE: &str = "test_compactionparityudt";
const TABLE: &str = "udt_collections";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schema_path() -> Option<PathBuf> {
    // schemas live next to the datasets root, or in the repo test-data dir.
    if let Some(root) = datasets_root() {
        if let Some(parent) = root.parent() {
            let p = parent.join("schemas").join("compaction-parity-udt.cql");
            if p.exists() {
                return Some(p);
            }
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let p = manifest_dir
        .parent()?
        .join("test-data")
        .join("schemas")
        .join("compaction-parity-udt.cql");
    p.exists().then_some(p)
}

fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1")
        .unwrap_or(false)
}

async fn open_db() -> Option<Database> {
    let Some(root) = datasets_root() else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT unset"
        );
        eprintln!("[issue_1340] CQLITE_DATASETS_ROOT unset; skipping");
        return None;
    };
    let Some(schema) = schema_path() else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but compaction-parity-udt.cql missing"
        );
        eprintln!("[issue_1340] compaction-parity-udt.cql not found; skipping");
        return None;
    };
    let data_dir = root.join("sstables");
    let config = IngestionConfig {
        schema_paths: vec![schema],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    };
    match ingest(config).await {
        Ok(res) => Some(res.database),
        Err(e) => {
            assert!(
                !require_fixtures_strict(),
                "CQLITE_REQUIRE_FIXTURES=1 but ingest failed: {e}"
            );
            eprintln!("[issue_1340] ingest failed ({e}); skipping");
            None
        }
    }
}

/// Peel `Frozen` wrappers to the inner `UdtValue`.
fn as_udt(v: &Value) -> &UdtValue {
    match v {
        Value::Udt(u) => u,
        Value::Frozen(inner) => as_udt(inner),
        other => panic!("expected a typed Value::Udt (issue #1340), got {other:?}"),
    }
}

fn udt_text(u: &UdtValue, field: &str) -> Option<String> {
    match u.fields.iter().find(|f| f.name == field) {
        Some(f) => match &f.value {
            Some(Value::Text(s)) => Some(s.clone()),
            None => None,
            Some(other) => panic!("field '{field}': expected text/null, got {other:?}"),
        },
        None => panic!("UDT '{}' missing field '{field}'", u.type_name),
    }
}

fn udt_int(u: &UdtValue, field: &str) -> i32 {
    match u.fields.iter().find(|f| f.name == field).map(|f| &f.value) {
        Some(Some(Value::Integer(i))) => *i,
        other => panic!("field '{field}': expected int, got {other:?}"),
    }
}

/// Peel to the `lp` list (frozen<list<frozen<person>>>).
fn as_list(v: &Value) -> &[Value] {
    match v {
        Value::List(items) => items,
        Value::Frozen(inner) => as_list(inner),
        other => panic!("lp: expected a List, got {other:?}"),
    }
}

/// Peel to the `ma` map entries (frozen<map<text, frozen<address>>>).
fn as_map(v: &Value) -> &[(Value, Value)] {
    match v {
        Value::Map(entries) => entries,
        Value::Frozen(inner) => as_map(inner),
        other => panic!("ma: expected a Map, got {other:?}"),
    }
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        Value::Frozen(inner) => text_of(inner),
        other => panic!("expected text, got {other:?}"),
    }
}

fn pk_int(row: &QueryRow) -> Option<i32> {
    match row.values.get("id") {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }
}

#[allow(clippy::type_complexity)]
#[tokio::test]
async fn select_returns_structured_inner_udt_fields() {
    let Some(db) = open_db().await else {
        return;
    };

    // The query surface itself resolves the keyspace/table + builds its schema
    // (incl. the person/address UDT defs) from the ingested DDL.
    let res = db
        .execute(&format!("SELECT id, lp, ma FROM {KEYSPACE}.{TABLE}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over {KEYSPACE}.{TABLE} failed: {e}"));

    // Dataset-guard: fixture-present-but-zero-rows is a FAILURE (not a skip).
    assert!(
        !res.rows.is_empty(),
        "issue #1340: SELECT over {KEYSPACE}.{TABLE} returned 0 rows — fixture present-but-empty \
         (Data.db not fetched?) is a hard FAILURE for this value-parity test"
    );

    // Expected inner-UDT field values from the committed sstabledump JSONL golden.
    // lp: pk -> [(first_name, last_name, age), ...]
    let expect_lp: &[(i32, &[(&str, &str, i32)])] = &[
        (1, &[("Ada", "Lovelace", 36)]),
        (2, &[("Grace", "Hopper", 85), ("Alan", "Turing", 41)]),
        (3, &[("Katherine", "Johnson", 101)]),
    ];
    // ma: pk -> (key, (street, city, zip))
    let expect_ma: &[(i32, &str, (&str, &str, &str))] = &[
        (1, "home", ("1 Navy Way", "Arlington", "22201")),
        (2, "office", ("9 Apollo", "Hampton", "23666")),
        (3, "h", ("9 Apollo", "Hampton", "23666")),
    ];

    let mut checked_lp = 0usize;
    let mut checked_ma = 0usize;
    for row in &res.rows {
        let Some(pk) = pk_int(row) else {
            panic!("row `id` did not decode as an int PK");
        };

        // ── lp: frozen<list<frozen<person>>> ─────────────────────────────────
        let lp_v = row
            .values
            .get("lp")
            .unwrap_or_else(|| panic!("row pk={pk} missing lp"));
        let list = as_list(lp_v);
        let (_, people) = expect_lp
            .iter()
            .find(|(p, _)| *p == pk)
            .unwrap_or_else(|| panic!("unexpected pk={pk}"));
        assert_eq!(list.len(), people.len(), "lp[pk={pk}] person count");
        for (el, (first, last, age)) in list.iter().zip(people.iter()) {
            let u = as_udt(el);
            assert_eq!(
                udt_text(u, "first_name").as_deref(),
                Some(*first),
                "lp[pk={pk}] first_name"
            );
            assert_eq!(
                udt_text(u, "last_name").as_deref(),
                Some(*last),
                "lp[pk={pk}] last_name"
            );
            assert_eq!(udt_int(u, "age"), *age, "lp[pk={pk}] age");
        }
        checked_lp += 1;

        // ── ma: frozen<map<text, frozen<address>>> ───────────────────────────
        let ma_v = row
            .values
            .get("ma")
            .unwrap_or_else(|| panic!("row pk={pk} missing ma"));
        let entries = as_map(ma_v);
        let (_, want_key, (street, city, zip)) = expect_ma
            .iter()
            .find(|(p, _, _)| *p == pk)
            .unwrap_or_else(|| panic!("unexpected pk={pk}"));
        let (_k, val) = entries
            .iter()
            .find(|(k, _)| text_of(k) == *want_key)
            .unwrap_or_else(|| panic!("ma[pk={pk}] missing key '{want_key}'"));
        let u = as_udt(val);
        assert_eq!(
            udt_text(u, "street").as_deref(),
            Some(*street),
            "ma[pk={pk}] street"
        );
        assert_eq!(
            udt_text(u, "city").as_deref(),
            Some(*city),
            "ma[pk={pk}] city"
        );
        assert_eq!(udt_text(u, "zip").as_deref(), Some(*zip), "ma[pk={pk}] zip");
        checked_ma += 1;
    }

    assert_eq!(checked_lp, 3, "expected typed lp on all 3 partitions");
    assert_eq!(checked_ma, 3, "expected typed ma on all 3 partitions");
    eprintln!(
        "[issue_1340] QUERY-SURFACE PASS — SELECT lp, ma FROM {KEYSPACE}.{TABLE} returned \
         structured inner person/address UDT fields matching the sstabledump golden on all 3 \
         partitions."
    );
}
