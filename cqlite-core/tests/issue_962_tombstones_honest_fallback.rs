//! Issue #962 (Epic #951): END-TO-END honest-fallback-under-tombstones coverage.
//!
//! #962 made the executor report HONEST access paths. On the `tombstones` build
//! the partition-targeted storage surfaces (`scan_partition`,
//! `scan_partition_with_cell_metadata`) compile out the bloom/BTI prune and become
//! full-scan + retain fallbacks that return `engaged == false`. Rather than report
//! a fake targeted label there, the executor calls `honest_targeted_path(...)`,
//! which collapses every targeted label to
//! `AccessPath::FallbackFullScan { TombstonesBuildNoPrune }` whenever the storage
//! call did not actually prune. The result ROWS are byte-identical to the pruned
//! build — only the *reported* path differs.
//!
//! That behaviour was previously covered ONLY by a `select_executor.rs` helper
//! unit test (`honest_targeted_path_reports_fallback_when_not_engaged`). There was
//! NO end-to-end test that actually ran the affected SELECT surfaces under the
//! `tombstones` feature. This file adds that coverage by exercising the public
//! query API under `tombstones` and asserting, on each surface, BOTH:
//!   1. the executor records the honest `FallbackFullScan { TombstonesBuildNoPrune }`
//!      access path (NOT a targeted `PartitionLookup` / `StreamingPartitionLookup`
//!      / `MultiPartitionLookup` / `MetadataPartitionLookup` label), AND
//!   2. the rows (and metadata) are still correct — equal to the equivalent
//!      literal / single-key lookups — proving correctness is preserved while
//!      reporting is honest.
//!
//! This is the test that would catch a regression where a `tombstones` storage
//! call returns `engaged == true`, or where an executor branch records a targeted
//! label without routing through `honest_targeted_path`.
//!
//! Exact recorded paths confirmed from `cqlite-core/src/query/select_executor.rs`
//! (each routes through `honest_targeted_path(<targeted>, engaged=false)` which
//! returns `FallbackFullScan { TombstonesBuildNoPrune }` on the tombstones build):
//!   - materializing `WHERE pk = ?`: `honest_targeted_path(PartitionLookup, false)` (~L2026)
//!   - streaming `WHERE pk = ?`: `honest_targeted_path(StreamingPartitionLookup, false)` (~L1660)
//!   - materializing `WHERE pk IN (..)`: `honest_targeted_path(MultiPartitionLookup, false)` (~L2055)
//!   - metadata `WRITETIME() WHERE pk = ?`: `honest_targeted_path(MetadataPartitionLookup, false)` (~L1921)
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests. Gated on `tombstones` (the feature that triggers the honest
//! fallback) plus `state_machine` + `cli-helpers` (the ingest/query stack).

#![cfg(all(
    feature = "tombstones",
    feature = "state_machine",
    feature = "cli-helpers"
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath, FallbackReason};
use cqlite_core::query::result::{QueryResultIterator, QueryRow};
use cqlite_core::query::StreamingConfig;
use cqlite_core::{Database, Value};

const UUID_TABLE: &str = "test_basic.simple_table";

/// The honest access path EVERY targeted surface must record on the `tombstones`
/// build: the prune is compiled out, the storage call returns `engaged == false`,
/// and `honest_targeted_path(...)` collapses the would-be targeted label to this
/// single fallback (see `select_executor.rs::honest_targeted_path`).
const HONEST_FALLBACK: AccessPath = AccessPath::FallbackFullScan {
    reason: FallbackReason::TombstonesBuildNoPrune,
};

/// Serializes the tests in this file. The access-path *probe*
/// (`access_path::last()`) is a process-global signal that production code records
/// into for EVERY SELECT, so two tests running concurrently would clobber each
/// other's probe reads between `reset()`/the query and `last()`. A
/// `tokio::sync::Mutex` (not `std::sync::Mutex`) so the guard can be held across
/// `.await` without tripping `clippy::await_holding_lock`. Each test acquires it
/// as its first statement and holds it for the whole body. Mirrors the #960/#961
/// harness.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent()?.join("schemas");
        if dir.exists() {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

/// Open a database isolated to `test_basic.simple_table` (uuid pk, the only
/// fixture these tombstones tests exercise — it decodes cleanly on the
/// `tombstones` build). ISOLATION MATTERS: under the `tombstones` build the
/// targeted lookup is a full-scan+retain that resolves readers by table NAME, so
/// loading the whole tree would let a same-named table in another keyspace (e.g.
/// `test_da.simple_table`) contaminate or fail these queries. We therefore load
/// only `basic-types.cql` and filter ingestion to the `test_basic/simple_table`
/// directory. Skips (returns `Err`) when the data/schema is absent.
async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schemas = schemas_dir().ok_or("schemas dir not found")?;
    let schema_paths: Vec<PathBuf> = vec![schemas.join("basic-types.cql")];
    for p in &schema_paths {
        if !p.exists() {
            return Err(format!("schema not found at {p:?}"));
        }
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths,
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/simple_table-".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 hex unquoted literal.
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
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

/// Canonical, order-independent fingerprint of a row's columns.
fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

fn uuid_value(row: &QueryRow, col: &str) -> Option<[u8; 16]> {
    match row.values.get(col) {
        Some(Value::Uuid(b)) => Some(*b),
        _ => None,
    }
}

/// Learn one real UUID partition key from a full scan, or `None` if no data.
async fn one_present_uuid(db: &Database) -> Option<[u8; 16]> {
    let full = db
        .execute(&format!("SELECT id FROM {UUID_TABLE} LIMIT 1"))
        .await
        .ok()?;
    uuid_value(full.rows.first()?, "id")
}

/// Drain a streaming iterator into its rows. The streaming scan runs in a spawned
/// task, so the access-path probe is only meaningful AFTER the iterator is fully
/// drained (the producer records its path as it scans).
async fn drain_rows(mut it: QueryResultIterator) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    while let Some(item) = it.next_async().await {
        if let Ok(row) = item {
            rows.push(row);
        }
    }
    rows
}

// ===========================================================================
// 1. Materializing `WHERE <uuidpk> = <literal>`.
//    Honest path: FallbackFullScan { TombstonesBuildNoPrune } (NOT PartitionLookup).
//    Rows: equal to the literal lookup AND to the known-present row.
// ===========================================================================

#[tokio::test]
async fn materializing_pk_eq_reports_honest_fallback_and_correct_rows() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    // Learn a present uuid via a full scan first.
    let Some(id) = one_present_uuid(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let literal = uuid_to_literal(&id);

    access_path::reset();
    let targeted = db
        .execute(&format!(
            "SELECT id, name, age FROM {UUID_TABLE} WHERE id = {literal}"
        ))
        .await
        .expect("WHERE pk = <uuid> lookup must succeed under tombstones");

    // Honest access path: the would-be PartitionLookup collapses to the no-prune
    // fallback because the tombstones build's scan_partition returns engaged=false.
    assert_eq!(
        targeted.metadata.access_path,
        Some(HONEST_FALLBACK),
        "Issue #962 (tombstones): a fully-constrained WHERE pk = <uuid> must report the HONEST \
         FallbackFullScan {{ TombstonesBuildNoPrune }} (the prune is compiled out), NOT \
         PartitionLookup; got {:?}",
        targeted.metadata.access_path,
    );
    assert_eq!(access_path::last(), Some(HONEST_FALLBACK));
    let path = targeted
        .metadata
        .access_path
        .clone()
        .expect("access path present");
    assert!(path.is_full_scan(), "honest fallback must be a full scan");
    assert!(
        !path.is_targeted(),
        "the tombstones build must NOT report a targeted label, got {path:?}",
    );

    // Correctness preserved: rows match the known-present row and are non-empty.
    assert!(
        !targeted.rows.is_empty(),
        "the learned-present uuid must return at least one row",
    );
    assert!(
        targeted
            .rows
            .iter()
            .all(|r| uuid_value(r, "id") == Some(id)),
        "every returned row must belong to the requested partition",
    );
}

// ===========================================================================
// 2. Streaming `execute_streaming(... WHERE pk = <literal>)`.
//    Honest path (via access_path::last() after draining):
//    FallbackFullScan { TombstonesBuildNoPrune } (NOT StreamingPartitionLookup).
//    Rows: equal to the materializing literal lookup.
// ===========================================================================

#[tokio::test]
async fn streaming_pk_eq_reports_honest_fallback_and_correct_rows() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = one_present_uuid(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let literal = uuid_to_literal(&id);
    let sql = format!("SELECT id, name, age FROM {UUID_TABLE} WHERE id = {literal}");

    // Materializing oracle for the same key.
    let oracle = db
        .execute(&sql)
        .await
        .expect("materializing oracle must succeed");

    // Clear the probe, run the streaming query, and FULLY drain it so the spawned
    // producer task records its access path before we read the probe.
    access_path::reset();
    let it = db
        .execute_streaming(&sql, StreamingConfig::default())
        .await
        .expect("streaming WHERE pk = <uuid> must succeed under tombstones");
    let stream_rows = drain_rows(it).await;

    // Honest access path: StreamingPartitionLookup collapses to the no-prune
    // fallback (the streaming scan_partition also returns engaged=false).
    assert_eq!(
        access_path::last(),
        Some(HONEST_FALLBACK),
        "Issue #962 (tombstones): the streaming WHERE pk = <uuid> path must record the HONEST \
         FallbackFullScan {{ TombstonesBuildNoPrune }}, NOT StreamingPartitionLookup. The \
         streaming scan runs in a spawned task, so the signal is the global probe.",
    );

    // Correctness preserved: streaming rows equal the materializing oracle rows.
    assert_eq!(
        fingerprints(&stream_rows),
        fingerprints(&oracle.rows),
        "Issue #962 (tombstones): streaming WHERE pk = <uuid> must yield the SAME rows as the \
         materializing lookup while reporting the honest fallback",
    );
    assert!(
        stream_rows.iter().all(|r| uuid_value(r, "id") == Some(id)),
        "every streamed row must belong to the requested partition",
    );
}

// ===========================================================================
// 3. `WHERE pk IN (a, b)`.
//    Honest path: FallbackFullScan { TombstonesBuildNoPrune } (NOT
//    MultiPartitionLookup). Rows: union of the single-key lookups.
//
// NOTE: the task spec asked for an int-pk table here (`test_da.wide_table`). That
// fixture is a Cassandra 5.0 BTI (`da`) SSTable whose multi-row partitions DO NOT
// decode on the `tombstones` build (the BTI within-partition reader is compiled
// out, producing "V5CompressedLegacy: Not enough bytes" parse failures and rows
// missing the `pk` column). That is an orthogonal, pre-existing decoder
// limitation — not the behaviour under test. So this IN test uses the uuid-pk
// `test_basic.simple_table`, which decodes cleanly under `tombstones` (see tests
// 1/2). The `MultiTargeted` -> honest-fallback executor path is partition-key-type
// agnostic, so the access-path assertion is unaffected by the key type.
// ===========================================================================

#[tokio::test]
async fn multi_partition_in_reports_honest_fallback_and_union_rows() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Learn two distinct uuid partition keys from a full scan.
    let full = db
        .execute(&format!("SELECT id, name, age FROM {UUID_TABLE}"))
        .await
        .expect("full scan must succeed");
    let mut ids: Vec<[u8; 16]> = Vec::new();
    for row in &full.rows {
        if let Some(b) = uuid_value(row, "id") {
            if !ids.contains(&b) {
                ids.push(b);
            }
        }
        if ids.len() == 2 {
            break;
        }
    }
    if ids.len() < 2 {
        eprintln!(
            "Skipping: need >= 2 distinct uuid partitions in simple_table (Data.db not fetched?)"
        );
        return;
    }
    let (a, b) = (uuid_to_literal(&ids[0]), uuid_to_literal(&ids[1]));

    access_path::reset();
    let in_result = db
        .execute(&format!(
            "SELECT id, name, age FROM {UUID_TABLE} WHERE id IN ({a}, {b})"
        ))
        .await
        .expect("WHERE pk IN (...) must succeed under tombstones");

    // Honest access path: MultiPartitionLookup collapses to the no-prune fallback
    // (every per-key scan_partition returns engaged=false, so all_engaged=false).
    assert_eq!(
        in_result.metadata.access_path,
        Some(HONEST_FALLBACK),
        "Issue #962 (tombstones): WHERE pk IN (a, b) must report the HONEST \
         FallbackFullScan {{ TombstonesBuildNoPrune }}, NOT MultiPartitionLookup; got {:?}",
        in_result.metadata.access_path,
    );
    assert_eq!(access_path::last(), Some(HONEST_FALLBACK));

    // Correctness preserved: the IN result equals the union of the two single-key
    // lookups (the documented semantics of `WHERE pk IN (...)`).
    let single_a = db
        .execute(&format!(
            "SELECT id, name, age FROM {UUID_TABLE} WHERE id = {a}"
        ))
        .await
        .expect("single-key lookup a must succeed");
    let single_b = db
        .execute(&format!(
            "SELECT id, name, age FROM {UUID_TABLE} WHERE id = {b}"
        ))
        .await
        .expect("single-key lookup b must succeed");

    let mut union_rows = single_a.rows.clone();
    union_rows.extend(single_b.rows.clone());

    assert!(
        !union_rows.is_empty(),
        "the two learned-present partitions must return rows",
    );
    assert_eq!(
        fingerprints(&in_result.rows),
        fingerprints(&union_rows),
        "Issue #962 (tombstones): WHERE pk IN (a, b) must return the UNION of the single-key \
         lookups while reporting the honest fallback",
    );
}

// ===========================================================================
// 4. `SELECT WRITETIME(col) ... WHERE <uuidpk> = <literal>`.
//    Honest path: FallbackFullScan { TombstonesBuildNoPrune } (NOT
//    MetadataPartitionLookup). Rows + WRITETIME: equal to the full-scan-filtered
//    metadata result for the same key.
// ===========================================================================

#[tokio::test]
async fn metadata_pk_eq_reports_honest_fallback_and_correct_metadata() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    // Full metadata scan: the correctness oracle, and the source for a present
    // uuid. NOTE: on the `tombstones` build the read path does NOT attach per-cell
    // write metadata, so WRITETIME(name) resolves to NULL for every row. That is an
    // orthogonal behaviour of the tombstones build, NOT what #962 governs here. The
    // load-bearing #962 assertion is the HONEST access path; the WRITETIME VALUE is
    // then asserted to MATCH the full-scan oracle exactly (null == null is still a
    // parity check, and the assertion would catch a regression that diverged the
    // targeted-metadata value from the full-scan value).
    let full = db
        .execute(&format!(
            "SELECT id, WRITETIME(name) AS wt FROM {UUID_TABLE}"
        ))
        .await
        .expect("full metadata scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    let Some(id) = full.rows.iter().find_map(|r| uuid_value(r, "id")) else {
        eprintln!("Skipping: simple_table has no uuid id column");
        return;
    };
    let literal = uuid_to_literal(&id);

    access_path::reset();
    let targeted = db
        .execute(&format!(
            "SELECT id, WRITETIME(name) AS wt FROM {UUID_TABLE} WHERE id = {literal}"
        ))
        .await
        .expect("WRITETIME WHERE pk = <uuid> must succeed under tombstones");

    // Honest access path: the metadata branch's MetadataPartitionLookup collapses
    // to the no-prune fallback (scan_partition_with_cell_metadata returns
    // engaged=false on the tombstones build). Confirmed at select_executor.rs L1921
    // `honest_targeted_path(AccessPath::MetadataPartitionLookup, engaged)`.
    assert_eq!(
        targeted.metadata.access_path,
        Some(HONEST_FALLBACK),
        "Issue #962 (tombstones): a WRITETIME WHERE pk = <uuid> must report the HONEST \
         FallbackFullScan {{ TombstonesBuildNoPrune }}, NOT MetadataPartitionLookup; got {:?}",
        targeted.metadata.access_path,
    );
    assert_eq!(access_path::last(), Some(HONEST_FALLBACK));

    // Correctness preserved: rows + WRITETIME equal the full-scan-filtered oracle.
    let oracle: Vec<&QueryRow> = full
        .rows
        .iter()
        .filter(|r| uuid_value(r, "id") == Some(id))
        .collect();

    assert_eq!(
        targeted.rows.len(),
        oracle.len(),
        "targeted metadata row count must equal the full-scan-filtered count",
    );
    assert!(
        !targeted.rows.is_empty(),
        "the learned-present uuid must return at least one metadata row",
    );
    assert!(
        targeted
            .rows
            .iter()
            .all(|r| uuid_value(r, "id") == Some(id)),
        "every returned metadata row must belong to the requested partition",
    );

    // WRITETIME value parity against the full-scan oracle (compare the raw `wt`
    // Value so a null/null match still counts as parity).
    assert_eq!(
        targeted.rows[0].values.get("wt"),
        oracle[0].values.get("wt"),
        "Issue #962 (tombstones): targeted WRITETIME must equal the full-scan WRITETIME for the \
         same key while reporting the honest fallback",
    );
}
