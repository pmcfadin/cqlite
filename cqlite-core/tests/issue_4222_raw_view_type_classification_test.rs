//! Issue #4222 — roborev round 11, finding F1: the raw SSTable view must not
//! REFUSE a base table merely because one of its columns is declared
//! `timeuuid` (or `varint`, `vector<..>`, …).
//!
//! The column-contract builder (`raw_view/columns.rs`) classified each
//! declared type as simple-or-complex through `row_build.rs`'s
//! `parse_cql_type_str` — i.e. `parser::complex_types::ComplexTypeParser` —
//! instead of `schema::CqlType::parse`, the parser its own doc comments cite.
//! That parser has NO `varint` arm, and its primitive alternation lists
//! `time` BEFORE `timeuuid`, so `"timeuuid"` matched `Time` with a trailing
//! `"uuid"` and was rejected as malformed. Both outcomes were HARD errors in
//! the fail-closed classifier, so EVERY query against the raw view of a table
//! carrying such a column failed — with a message that misdiagnosed the cause
//! as UDT ambiguity.
//!
//! Fixture: `test_basic.simple_table` (`test-data/schemas/basic-types.cql`) —
//! a real Cassandra 5.0 `nb` SSTable whose schema declares
//! `session_id TIMEUUID` alongside `account_balance DECIMAL`,
//! `duration_val DURATION` and `work_time TIME`. Its `Data.db` is fetch-only
//! (only the JSONL/`.txt`/`.crc32` sidecars are git-committed), so this lane
//! SKIPs cleanly when no candidate root carries the real bytes, exactly like
//! `issue_4222_raw_view_point_read_test.rs`; `CQLITE_REQUIRE_FIXTURES=1`
//! (issue #972) turns that SKIP into a hard failure for a CI lane that must
//! not pass having run nothing.
//!
//! Oracle: physical-dump parity (#1742) — the expected `session_id` value and
//! its write timestamp are PARSED AT RUNTIME from the committed
//! `nb-1-big-Data.db.jsonl` sstabledump golden beside the very `Data.db` the
//! query reads, never transcribed into a Rust literal and never taken from
//! CQLite's own output (#3041/#3042).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use chrono::DateTime;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};
use serde_json::Value as Json;

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn fixture_root_or_skip() -> Option<std::path::PathBuf> {
    if let Some(root) = sstables_root_for_table(KEYSPACE, TABLE) {
        return Some(root);
    }
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but '{KEYSPACE}.{TABLE}' was not found under any \
             candidate root — fetch the corpus first \
             (bash test-data/scripts/fetch-datasets.sh): {}",
            describe_search(KEYSPACE, TABLE)
        );
    }
    eprintln!(
        "SKIP: '{KEYSPACE}.{TABLE}' (fetch-only fixture) was not found under any candidate \
         root — {}",
        describe_search(KEYSPACE, TABLE)
    );
    None
}

async fn open_fixture_db() -> Option<(Database, std::path::PathBuf)> {
    let root = fixture_root_or_skip()?;
    let schema = schema_path("basic-types.cql")
        .expect("committed schema basic-types.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.clone(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load, else the raw view would refuse with Error::Schema"
    );
    Some((result.database, root))
}

/// Canonical 8-4-4-4-12 rendering of a 16-byte UUID — the shape sstabledump
/// prints, so the golden key and the surfaced `Value::Uuid` compare directly.
fn hyphenated(bytes: &[u8; 16]) -> String {
    let h = |r: std::ops::Range<usize>| -> String {
        bytes[r].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

/// Every partition object of this fixture's `nb-1-big` sstabledump golden,
/// keyed by its single `uuid` partition-key component. Panics rather than
/// defaulting when the golden is unreadable, so a regenerated fixture FAILs
/// instead of passing vacuously.
fn golden_partitions(root: &std::path::Path) -> std::collections::HashMap<String, Json> {
    let dir = std::fs::read_dir(root.join(KEYSPACE))
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", root.join(KEYSPACE).display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .unwrap_or_else(|| {
            panic!(
                "no '{TABLE}-<uuid>' generation dir under {}",
                root.display()
            )
        });
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let text = std::fs::read_to_string(&jsonl).unwrap_or_else(|e| {
        panic!(
            "the committed golden {} must be readable: {e}",
            jsonl.display()
        )
    });
    let mut out = std::collections::HashMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let parsed: Json = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("golden {} is not valid JSONL: {e}", jsonl.display()));
        let key = parsed["partition"]["key"][0]
            .as_str()
            .unwrap_or_else(|| panic!("golden partition has no string key: {parsed}"))
            .to_string();
        out.insert(key, parsed);
    }
    assert!(
        !out.is_empty(),
        "the golden must carry at least one partition"
    );
    out
}

/// A `timeuuid`-bearing base table must produce a usable raw view: the query
/// SUCCEEDS, `session_id` is classified SINGLE-CELL (it gets the
/// `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad, never the
/// multi-cell `_complex_deletion` trio), and the surfaced value + write
/// timestamp match the sstabledump golden.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeuuid_column_does_not_make_the_raw_view_unusable() {
    let Some((db, root)) = open_fixture_db().await else {
        return;
    };

    // Before the F1 fix this errored outright with
    // `Error::Schema(... 'timeuuid' ... refusing to guess ...)`.
    let result = db
        .execute(&format!(
            "SELECT id, session_id, session_id_timestamp, session_id_ttl, \
             session_id_local_deletion_time, session_id_tombstone \
             FROM {KEYSPACE}.{TABLE}_raw_sstable_data LIMIT 1"
        ))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "REGRESSION (issue #4222 F1): a raw-view query over a table with a \
                 `timeuuid` column must SUCCEED, got: {e}"
            )
        });
    assert_eq!(
        result.rows.len(),
        1,
        "LIMIT 1 over a 999-partition fixture must return exactly one physical row"
    );
    let row = &result.rows[0];

    // The single-cell QUAD is present — i.e. `timeuuid` classified simple.
    for col in [
        "session_id_timestamp",
        "session_id_ttl",
        "session_id_local_deletion_time",
        "session_id_tombstone",
    ] {
        assert!(
            result.metadata.columns.iter().any(|c| c.name == col),
            "'{col}' must be in the projected column contract — `timeuuid` is a \
             single-cell type"
        );
    }

    // Golden-backed: the row's own partition key locates its golden object,
    // whose `session_id` cell value and row liveness timestamp are the oracle.
    let goldens = golden_partitions(&root);
    let id = match row.values.get("id") {
        Some(Value::Uuid(bytes)) => hyphenated(bytes),
        other => panic!("the raw view must surface the `uuid` partition key, got {other:?}"),
    };
    let golden = goldens
        .get(&id)
        .unwrap_or_else(|| panic!("partition {id} must exist in the sstabledump golden"));

    let golden_session_id = golden["rows"][0]["cells"]
        .as_array()
        .expect("golden row must carry cells")
        .iter()
        .find(|c| c["name"] == "session_id")
        .and_then(|c| c["value"].as_str())
        .expect("the golden must carry a `session_id` cell value")
        .to_string();
    let surfaced = match row.values.get("session_id") {
        Some(Value::Uuid(bytes)) => hyphenated(bytes),
        other => panic!("`session_id` must decode as a (time)uuid, got {other:?}"),
    };
    assert_eq!(
        surfaced, golden_session_id,
        "the raw view's `session_id` must match the sstabledump golden byte-exact \
         (physical-dump parity, #1742)"
    );

    let golden_micros = DateTime::parse_from_rfc3339(
        golden["rows"][0]["liveness_info"]["tstamp"]
            .as_str()
            .expect("the golden row must carry a liveness tstamp"),
    )
    .expect("the golden tstamp must be RFC3339")
    .timestamp_micros();
    assert_eq!(
        row.values.get("session_id_timestamp"),
        Some(&Value::BigInt(golden_micros)),
        "`session_id_timestamp` must be the authoritative on-disk write time from the golden"
    );
}
