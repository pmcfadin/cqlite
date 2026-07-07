//! Issue #2038: a NON-FROZEN collection (or UDT) column written with a TTL must
//! round-trip so that its per-cell metadata surfaces the expiry — so
//! `TTL(collection_col)` resolves to the authoritative value instead of `null`.
//!
//! This is the complex-column analogue of #1743, which fixed only the SCALAR
//! cell path. Root cause (read side): the complex-column per-cell metadata
//! builder in `v5_compressed_legacy/row_data.rs` hardcoded `expiration: None`,
//! so a non-frozen collection/UDT column written as expiring (each element cell
//! IS_EXPIRING with its own explicit TTL + localExpirationTime — exactly what a
//! `USING TTL` collection write emits) surfaced NO expiry, and `TTL(col)`
//! returned `null` even though the on-disk cells ARE expiring.
//!
//! The fix resolves the per-cell metadata via an `ExpiryHomogeneity` tracker in
//! `complex_column.rs`: it surfaces a `CellExpiration { ttl_seconds,
//! expires_at_seconds }` ONLY when every VISIBLE (post shadow/TTL-filter,
//! non-tombstone) element of the collection shares the IDENTICAL explicit
//! expiry. A heterogeneous collection (elements with different TTLs, or a mix
//! of expiring and live-forever elements) surfaces `None` instead of
//! over-approximating with one element's TTL (roborev Medium finding on the
//! original fix — see `test_heterogeneous_element_ttls_surface_no_expiration`
//! below, which pins the corrected behavior). No heuristics: every input comes
//! from the decoded per-element cell fields.
//!
//! Three regression guards:
//! 1. `collection_ttl_write_round_trips_as_expiring_cell` — WRITE→READ via the
//!    metadata API (`scan_with_cell_metadata`): a `set<int>` column under a
//!    per-column TTL (`CellOperation::WriteWithTtl`) flushes to a single `nb`
//!    SSTable and reopens with its expiring per-cell metadata intact.
//! 2. `collection_ttl_reachable_via_real_sql` — WIRING EVIDENCE: the SAME
//!    fixture, but read through the REAL SQL surface (`Database::execute`,
//!    `SELECT ... TTL(tags) ...`) rather than only the internal metadata API.
//!    Ground truth (issue #2038 re-verify): `cqlite_core::query::
//!    writetime_ttl_validator::validate_writetime_ttl_call` DOES reject
//!    non-frozen collections, but that validator is dead code — it is never
//!    invoked from the SELECT planner/executor (`select_optimizer.rs`,
//!    `select_executor/execute.rs`) anywhere in the codebase. So
//!    `TTL(non_frozen_collection)` is NOT planner-rejected today; it reaches
//!    `evaluate_writetime_ttl` and this fix's surface is real-SQL reachable.
//! 3. `test_heterogeneous_element_ttls_surface_no_expiration` — the roborev
//!    Medium regression guard: two `set<text>` elements written with DIFFERENT
//!    explicit per-element TTLs (`CellOperation::WriteComplexElement`) must
//!    surface `expiration: None` (ambiguous), not one element's TTL.
//!
//! No wall-clock assertion race: the writer stamps each element's
//! localExpirationTime as `wallClockNowSeconds + ttl`. We bracket the write+flush
//! with a `[before, after]` wall-clock window and assert
//! `expires_at ∈ [before+ttl, after+ttl]`, plus `ttl_seconds == n` exactly. The
//! real-SQL test (`collection_ttl_reachable_via_real_sql`) reads `remaining =
//! expires_at - now_query` AFTER an extra `ingest()` + `db.execute()` gap of
//! unbounded latency past `after`, so its window is widened with a THIRD
//! `query_after` timestamp (captured post-query) instead of reusing the plain
//! write-only `[before, after]` bracket (roborev 1518 re-review fix).
//!
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support,cli-helpers,state_machine \
//!     --test issue_2038_collection_ttl_expiring_cell

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::{CellWriteMetadata, Value};
use cqlite_core::Config;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const KS: &str = "ttl_coll_ks";
const TBL: &str = "items";

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a row whose non-frozen `set<int>` column is written with a per-column
/// TTL — the whole-collection expiring write (`WriteWithTtl`), i.e. what the CQL
/// `INSERT ... USING TTL n` path produces for a collection column.
fn insert_collection_using_ttl(id: i32, elems: Vec<i32>, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::WriteWithTtl {
        column: "tags".to_string(),
        value: Value::Set(elems.into_iter().map(Value::Integer).collect()),
        ttl_seconds,
        local_deletion_time: None,
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// Build a row whose non-frozen `set<int>` column is written by a PLAIN
/// `CellOperation::Write` under a MUTATION-LEVEL `USING TTL n`
/// (`Mutation::ttl_seconds = Some(n)`) — i.e. the `INSERT ... USING TTL n`
/// ROW-LIVENESS path, distinct from the per-column `WriteWithTtl` path above.
///
/// Scope B (issue #2038): the writer's whole-column `Write` arm dropped this
/// row TTL (`write_complex_column(.., None)`), so the collection's element cells
/// were written NON-expiring and `TTL(tags)` came back `null` even though the
/// mutation carried a TTL. This is the symmetric write-side of the #2038 read
/// fix.
fn insert_collection_row_ttl(id: i32, elems: Vec<i32>, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "tags".to_string(),
        value: Value::Set(elems.into_iter().map(Value::Integer).collect()),
    }];
    // Mutation-level TTL (the row `USING TTL`) — NOT a per-column WriteWithTtl.
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, Some(ttl_seconds))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64
}

#[test]
fn collection_ttl_write_round_trips_as_expiring_cell() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // Pinned reconciliation timestamp (USING TIMESTAMP is independent of the TTL
    // expiry clock, so this need not track wall-clock).
    const TS: i64 = 1_700_000_000_000_000;
    const TTL: u32 = 86_400;
    const TTL_I32: i32 = TTL as i32;

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Bracket the write+flush with a wall-clock window: each element's on-disk
    // localExpirationTime is stamped as `wallClockNow + ttl`, so the surfaced
    // expiry must land inside `[before+ttl, after+ttl]`.
    let before = now_secs();
    engine
        .write(insert_collection_using_ttl(1, vec![10, 20, 30], TTL, TS))
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("gen1");
    let after = now_secs();
    rt.block_on(engine.close()).expect("close engine");

    // Reopen and scan WITH cell metadata (the authoritative surface `TTL()` reads).
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let results = rt
        .block_on(manager.scan_with_cell_metadata(&table_id, None, None, None, Some(&schema)))
        .expect("metadata scan must not error");

    assert_eq!(results.len(), 1, "expected the single row written");

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> =
        results.into_iter().map(|(k, _v, m)| (k.0, m)).collect();

    let meta = by_pk
        .get(1_i32.to_be_bytes().as_slice())
        .expect("row for id=1 present");

    let tags_meta = meta
        .get("tags")
        .expect("per-cell metadata for the 'tags' collection must be surfaced");

    // Core regression (#2038): TTL(tags) must NOT be null — the collection's
    // expiring element cells carry an explicit per-element TTL + localExpirationTime.
    let exp = tags_meta.expiration.as_ref().expect(
        "Issue #2038: TTL(tags) must be present (expiring non-frozen collection), not null",
    );

    assert_eq!(
        exp.ttl_seconds, TTL_I32,
        "TTL(tags) must equal the written per-column TTL value"
    );

    // localExpirationTime = wallClockNow + ttl, bracketed by the flush window.
    let lo = before + TTL as i64;
    let hi = after + TTL as i64;
    assert!(
        (lo..=hi).contains(&exp.expires_at_seconds),
        "expires_at {} must be wallClockNow+ttl, in [{lo}, {hi}]",
        exp.expires_at_seconds
    );

    drop(temp_dir);
}

/// Issue #2038 Scope B (write-path): a non-frozen collection written by a plain
/// `Write` op under a MUTATION-LEVEL `USING TTL n` must round-trip as expiring —
/// the whole-column `Write` arm of `emit_whole_column_op` must thread
/// `mop.row_ttl_seconds` into `write_complex_column` instead of dropping it
/// (`None`). Fails on the pre-fix branch: the element cells are written
/// non-expiring, so `TTL(tags)` surfaces `None`.
#[test]
fn collection_row_ttl_write_round_trips_as_expiring_cell() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    const TS: i64 = 1_700_000_000_000_000;
    const TTL: u32 = 86_400;
    const TTL_I32: i32 = TTL as i32;

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let before = now_secs();
    engine
        .write(insert_collection_row_ttl(1, vec![10, 20, 30], TTL, TS))
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("gen1");
    let after = now_secs();
    rt.block_on(engine.close()).expect("close engine");

    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let results = rt
        .block_on(manager.scan_with_cell_metadata(&table_id, None, None, None, Some(&schema)))
        .expect("metadata scan must not error");

    assert_eq!(results.len(), 1, "expected the single row written");

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> =
        results.into_iter().map(|(k, _v, m)| (k.0, m)).collect();
    let meta = by_pk
        .get(1_i32.to_be_bytes().as_slice())
        .expect("row for id=1 present");
    let tags_meta = meta
        .get("tags")
        .expect("per-cell metadata for the 'tags' collection must be surfaced");

    // Scope B regression: a row-level `USING TTL` collection write must NOT drop
    // the TTL — every element cell is expiring with the mutation's row TTL.
    let exp = tags_meta.expiration.as_ref().expect(
        "Issue #2038 Scope B: TTL(tags) must be present for a row-level USING TTL \
         collection write (whole-column Write op), not null",
    );

    assert_eq!(
        exp.ttl_seconds, TTL_I32,
        "TTL(tags) must equal the mutation-level row TTL value"
    );

    let lo = before + TTL as i64;
    let hi = after + TTL as i64;
    assert!(
        (lo..=hi).contains(&exp.expires_at_seconds),
        "expires_at {} must be wallClockNow+ttl, in [{lo}, {hi}]",
        exp.expires_at_seconds
    );

    drop(temp_dir);
}

/// Issue #2038 wiring evidence: `TTL(non_frozen_collection)` is reachable via
/// REAL SQL (`Database::execute`), not just the internal metadata API.
///
/// Ground truth check performed during the roborev re-verify round:
/// `cqlite_core::query::writetime_ttl_validator::validate_writetime_ttl_call`
/// DOES reject non-frozen collections with a Cassandra-shaped error — but that
/// validator is never called from the SELECT planner/executor anywhere in the
/// codebase (`select_optimizer.rs` builds `OptimizedQueryPlan` with no such
/// call; `select_executor/execute.rs`'s `execute()` never calls it either).
/// So `TTL(tags)` on a `set<int>` column reaches `evaluate_writetime_ttl`
/// unobstructed and returns a real value — this fix's surface is genuinely
/// exercised by end-user SQL today, not merely a metadata-layer improvement
/// behind a planner rejection.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
#[test]
fn collection_ttl_reachable_via_real_sql() {
    use cqlite_core::ingestion::{ingest, IngestionConfig};

    const SQL_KS: &str = "ttl_coll_sql_ks";
    const SQL_TBL: &str = "items";
    const TTL: u32 = 86_400;
    const TTL_I32: i32 = TTL as i32;
    const TS: i64 = 1_700_000_000_000_000;

    fn schema_cql() -> String {
        format!("CREATE TABLE {SQL_KS}.{SQL_TBL} (\n  id int PRIMARY KEY,\n  tags set<int>\n);\n")
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    let before = now_secs();
    {
        use cqlite_core::schema::parse_cql_schema;
        let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
        let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema);
        let mut engine = WriteEngine::new(config).expect("engine creation");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::WriteWithTtl {
            column: "tags".to_string(),
            value: Value::Set(vec![Value::Integer(10), Value::Integer(20)]),
            ttl_seconds: TTL,
            local_deletion_time: None,
        }];
        let mutation = Mutation::new(TableId::new(SQL_KS, SQL_TBL), pk, None, ops, TS, None);
        engine.write(mutation).expect("write mutation");
        rt.block_on(engine.flush()).expect("flush").expect("gen1");
        rt.block_on(engine.close()).expect("close engine");
    }
    let after = now_secs();

    let result = rt
        .block_on(ingest(IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: None,
            core_config: Config::default(),
            table_directory_filter: None,
        }))
        .expect("ingest fixture");

    let db = result.database;
    let sql = format!("SELECT id, TTL(tags) FROM {SQL_KS}.{SQL_TBL}");
    let query_result = rt
        .block_on(db.execute(&sql))
        .expect("Issue #2038: TTL() on a non-frozen collection must be reachable via real SQL");
    // Issue #2038 roborev re-review (1518, Medium): `remaining` is evaluated at
    // QUERY time (`expires_at - now_query`), and `ingest()` + `db.execute()`
    // above run with UNBOUNDED latency AFTER `after` (opening SSTables +
    // scanning, worse under load). Bracketing only by the write-only window
    // `[before, after]` (as the metadata-API test above correctly does, since
    // IT reads with no ingest/execute gap) is too tight here and can fail
    // spuriously on a correct implementation. Capture `query_after` so the
    // window covers the FULL elapsed span from write-start to query-end.
    let query_after = now_secs();

    assert_eq!(
        query_result.rows.len(),
        1,
        "expected the single row written"
    );
    let row = &query_result.rows[0];
    match row.values.get("ttl(tags)") {
        Some(Value::Integer(remaining)) => {
            // remaining = expires_at - now_query, where expires_at ∈
            // [before+TTL, after+TTL] (from the write window) and now_query ∈
            // [after, query_after] (write-close through query-return). The
            // widest possible remaining range is therefore
            // [(before+TTL) - query_after, (after+TTL) - before].
            let lo = (before + TTL as i64) - query_after;
            let hi = (after + TTL as i64) - before;
            assert!(
                (lo..=hi).contains(&(*remaining as i64)),
                "TTL(tags) remaining {} must be in [{lo}, {hi}]",
                remaining
            );
            assert!(
                *remaining > 0,
                "TTL(tags) must be a positive remaining-seconds value, got {}",
                remaining
            );
            assert!(
                *remaining <= TTL_I32,
                "TTL(tags) {} must not exceed the written TTL {}",
                remaining,
                TTL_I32
            );
        }
        other => panic!(
            "Issue #2038: TTL(tags) via real SQL must return Integer(remaining_secs), got {:?}",
            other
        ),
    }

    drop(temp_dir);
}

/// Issue #2038 roborev Medium finding regression guard: a non-frozen collection
/// whose VISIBLE elements carry DIFFERENT explicit per-element TTLs has no
/// single TTL that describes the whole value — `TTL(tags)` must surface `None`
/// rather than one element's (arbitrary, over-approximated) expiry.
///
/// Constructs the two `set<text>` elements directly via
/// `CellOperation::WriteComplexElement` (the per-element write path, epic
/// #899 Phase B) so each carries its OWN explicit `ttl_seconds` +
/// `local_deletion_time` — the on-disk shape the bug in the original fix
/// conflated (it took the MAX expiry across ALL elements and paired it with
/// that element's TTL, misattributing one element's TTL to the whole column).
#[test]
fn test_heterogeneous_element_ttls_surface_no_expiration() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");

    let schema = TableSchema {
        keyspace: "ttl_mixed_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    const TS: i64 = 1_700_000_000_000_000;
    // Two DIFFERENT explicit per-element expiries — deliberately heterogeneous.
    const LDT_A: i32 = 2_000_000_000;
    const LDT_B: i32 = 2_000_000_100;

    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![
        CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: b"alpha".to_vec(),
            value: None,
            timestamp_micros: TS,
            ttl_seconds: Some(100),
            local_deletion_time: Some(LDT_A),
            is_deleted: false,
        },
        CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: b"beta".to_vec(),
            value: None,
            timestamp_micros: TS,
            ttl_seconds: Some(200),
            local_deletion_time: Some(LDT_B),
            is_deleted: false,
        },
    ];
    let mutation = Mutation::new(
        TableId::new("ttl_mixed_ks", "items"),
        pk,
        None,
        ops,
        TS,
        None,
    );

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    engine.write(mutation).expect("write mutation");
    rt.block_on(engine.flush()).expect("flush").expect("gen1");
    rt.block_on(engine.close()).expect("close engine");

    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from("ttl_mixed_ks.items");
    let results = rt
        .block_on(manager.scan_with_cell_metadata(&table_id, None, None, None, Some(&schema)))
        .expect("metadata scan must not error");

    assert_eq!(results.len(), 1, "expected the single row written");

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> =
        results.into_iter().map(|(k, _v, m)| (k.0, m)).collect();
    let meta = by_pk
        .get(1_i32.to_be_bytes().as_slice())
        .expect("row for id=1 present");
    let tags_meta = meta
        .get("tags")
        .expect("per-cell metadata for the 'tags' collection must be surfaced");

    assert_eq!(
        tags_meta.expiration, None,
        "Issue #2038 (roborev Medium): a collection with heterogeneous \
         per-element TTLs must surface expiration=None, not one element's \
         (arbitrary) TTL — got {:?}",
        tags_meta.expiration
    );

    drop(temp_dir);
}
