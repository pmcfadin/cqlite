//! Issue #3747 — an EMPTY multicell MAP KEY must survive the read path.
//!
//! # The defect
//! The multicell MAP branch of the row decoder guarded its key decode on a
//! NON-EMPTY cell path. A map entry whose key is the empty value therefore
//! produced `decoded_key == None` and was not merely left undecoded — it never
//! reached the reconstructed `Value::Map` at all. A `SELECT` returned a map
//! SHORT ONE ENTRY, with no error and no warning.
//!
//! # Oracle — Cassandra-written bytes, never CQLite's own output
//! Fixture: `test-data/fixtures/issue_3747/test_empty_map_key/empty_key_map-*/`
//! written by cassandra:5.0.2, with its `sstabledump` golden
//! `nb-1-big-Data.db.jsonl`. Every expectation below is quoted from that golden.
//! A CQLite-written + CQLite-read round-trip could not settle this (CLAUDE.md,
//! #3042): both sides would make the identical mistake.
//!
//! Golden, id 1 (the SUBJECT row):
//! ```text
//! {"name":"m_frozen","value":{"":1000,"k":2000}}
//! {"name":"m_ascii","path":[""],"value":100}   {"name":"m_ascii","path":["z"],"value":200}
//! {"name":"m_blob","path":[""],"value":10}     {"name":"m_blob","path":["ab"],"value":20}
//! {"name":"m_text","path":[""],"value":1}      {"name":"m_text","path":["a"],"value":2}
//! ```
//! Golden, id 2 (the CONTRAST row, no empty key anywhere):
//! `m_text {"x":3,"y":4}`, `m_blob {0xcd:30}`, `m_ascii {"w":300}`,
//! `m_frozen {"j":3000}`.
//!
//! FORMAT AUTHORITY for "empty is legal data": read at the pinned tag
//! (`git show cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/CollectionSerializer.java`),
//! `readNonNullValue` rejects only a NULL (-1) length — a ZERO length is
//! accepted — and `UTF8Type`/`AsciiType`/`BytesType` are variable-width and
//! validate any byte sequence including none. A CQLite `file:line` is NEVER
//! format authority (CLAUDE.md).
//!
//! # What must NOT be asserted (MEASURED)
//! Each multicell column also carries a collection tombstone (an INSERT of a
//! whole non-frozen collection REPLACES it) whose `local_delete_time` is a WALL
//! CLOCK no CQL clause can pin. Never assert on `local_delete_time` and never
//! byte-compare the golden across regenerations. Entry SET membership is the
//! assertion subject, not cell order.

use std::path::{Path, PathBuf};
use std::sync::Arc;

// `cqlite_core::ingestion` (the SELECT-surface entry point) is gated behind
// `cli-helpers`.
//
// WHERE THESE LANES ACTUALLY EXECUTE — stated precisely, because an earlier
// revision of this comment claimed `pr-gate-core` runs them and that is FALSE:
//   * the local gate's `core-tests` component DOES run them — it invokes
//     `cargo test -p cqlite-core --features cli-helpers`, which builds and runs
//     every `--test` target in the package, this one included.
//   * `pr-gate-core` does NOT. It runs `--lib` plus exactly ONE named integration
//     target (`--test query_semantics_oracle_parity`, pr-gate.yml), so this file
//     is COMPILED there by clippy and EXECUTED there by nothing.
// So these lanes gate THIS merge (the gate of record runs `core-tests`), but they
// are not part of `required` and will not protect a future PR through CI alone.
// Wiring them into `pr-gate-core` is a change to a merge gate and is deliberately
// NOT bundled into a decode fix; it is raised on the issue instead.
//
// Without `--features cli-helpers` only the AC4 compaction lane runs — 1 of 5 —
// which is the "compiling a feature is not covering it" trap in miniature.
#[cfg(feature = "cli-helpers")]
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::Value;
use cqlite_core::Config;
#[cfg(feature = "cli-helpers")]
use cqlite_core::Database;

const KEYSPACE: &str = "test_empty_map_key";
const TABLE: &str = "empty_key_map";

// ── Fixture resolution (CHECKOUT-relative; committed ⇒ must_run) ────────────
//
// Deliberately NOT via `CQLITE_DATASETS_ROOT`: that names a machine-local
// fetched corpus which does not carry this git-committed fixture. Modelled on
// `issue_3629_core_tojson_udt_namespace.rs::fixture_root`.

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// The `issue_3747` fixture root (an "sstables root": it holds the keyspace dir
/// directly). Asserts the committed fixture is intact, GLOBBING the table dir —
/// a regeneration mints a fresh table UUID, so a hardcoded path would rot.
fn fixture_root() -> PathBuf {
    let root = repo_root().join("test-data/fixtures/issue_3747");
    let table_dir = table_dir(&root);
    let data: Vec<PathBuf> = std::fs::read_dir(&table_dir)
        .unwrap_or_else(|e| panic!("fixture table dir unreadable ({table_dir:?}): {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    assert!(
        !data.is_empty(),
        "no *-Data.db under {table_dir:?} — the binaries are force-added; \
         the fixture is git-tracked source and must never be absent"
    );
    root
}

fn table_dir(root: &Path) -> PathBuf {
    let ks = root.join(KEYSPACE);
    let dirs: Vec<PathBuf> = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| panic!("committed fixture keyspace dir unreadable ({ks:?}): {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .collect();
    assert_eq!(
        dirs.len(),
        1,
        "expected exactly one {TABLE}-* dir under {ks:?}, got {dirs:?}"
    );
    dirs.into_iter().next().expect("length asserted as 1")
}

fn schema_path() -> PathBuf {
    repo_root().join("test-data/schemas/issue-3747-empty-map-key.cql")
}

#[cfg(feature = "cli-helpers")]
async fn open_db() -> Database {
    let config = IngestionConfig {
        schema_paths: vec![schema_path()],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    };
    ingest(config)
        .await
        .expect("committed fixture must ingest (it is git-tracked source)")
        .database
}

/// `(id, column) -> Value` for every row of the fixture, via the PUBLIC
/// `Database::execute` SELECT surface.
#[cfg(feature = "cli-helpers")]
async fn select_rows() -> Vec<(i32, std::collections::HashMap<String, Value>)> {
    let db = open_db().await;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over committed fixture failed: {e}"));
    assert!(
        !result.rows.is_empty(),
        "committed fixture decoded ZERO rows (0-rows-when-present is a failure)"
    );
    result
        .rows
        .iter()
        .map(|r| {
            let id = match r.values.get("id") {
                Some(Value::Integer(i)) => *i,
                other => panic!("row without an integer id: {other:?}"),
            };
            let cols = r
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            (id, cols)
        })
        .collect()
}

/// The map entries of `column` in the row with primary key `id`, as a SORTED
/// (key-bytes, value) vector so membership is compared without depending on
/// on-disk cell order. Fail-closed: a missing row, a missing column, a NULL
/// cell, or a non-map value is a decode regression, never a skip.
#[cfg(feature = "cli-helpers")]
fn map_entries(
    rows: &[(i32, std::collections::HashMap<String, Value>)],
    id: i32,
    column: &str,
) -> Vec<(Value, Value)> {
    let row = rows
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    let v = row
        .get(column)
        .unwrap_or_else(|| panic!("row id={id} has no column {column}"));
    let entries = match peel_frozen(v) {
        Value::Map(entries) => entries,
        other => panic!("row id={id} column {column} is not a Map: {other:?}"),
    };
    let mut sorted = entries;
    sorted.sort_by_key(|(k, _)| key_bytes(k));
    sorted
}

fn peel_frozen(v: &Value) -> Value {
    match v {
        Value::Frozen(inner) => peel_frozen(inner),
        other => other.clone(),
    }
}

/// Sort key for a map key: the raw bytes of a Text/Blob key (so `""` sorts
/// first, exactly as an empty byte string does).
#[cfg(feature = "cli-helpers")]
fn key_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Text(b) | Value::Blob(b) => b.to_vec(),
        other => panic!("unexpected map key kind in fixture: {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// AC2 — text / ascii keys through the user-facing SELECT read path
// ════════════════════════════════════════════════════════════════════════════

/// AC2. The headline RED case: `m_text` at id 1 must return BOTH golden
/// entries, the empty key decoding to `Value::Text("")`.
///
/// REVERT-VERIFY NOTE (pre-fix, MEASURED at 8fa022519 + this test, before the
/// decoder change): the map came back with ONE entry —
/// `left: [(Text(b"a"), Integer(2))]` against the golden's two — i.e. the
/// empty-key entry was silently absent, no error and no warning.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn ac2_select_returns_empty_text_map_key() {
    let rows = select_rows().await;

    let m_text = map_entries(&rows, 1, "m_text");
    assert_eq!(
        m_text,
        vec![
            (Value::text(""), Value::Integer(1)),
            (Value::text("a"), Value::Integer(2)),
        ],
        "golden id 1 m_text is path:[\"\"]=1 and path:[\"a\"]=2; an empty key is \
         legal data (empty is a distinct value from null), never 'no key'"
    );

    let m_ascii = map_entries(&rows, 1, "m_ascii");
    assert_eq!(
        m_ascii,
        vec![
            (Value::text(""), Value::Integer(100)),
            (Value::text("z"), Value::Integer(200)),
        ],
        "golden id 1 m_ascii is path:[\"\"]=100 and path:[\"z\"]=200; `ascii` \
         shares the utf8 arm but is a DISTINCT declared type — this pins the fix \
         as a property of the guard, not of one type name"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// AC3 — blob keys (a SECOND variable-width family, a DIFFERENT decode arm)
// ════════════════════════════════════════════════════════════════════════════

/// AC3. `m_blob` at id 1 must return BOTH golden entries, the empty key
/// decoding to `Value::Blob(b"")` through the blob FALLBACK arm rather than the
/// utf8 arm AC2 exercises.
///
/// REVERT-VERIFY NOTE (pre-fix, MEASURED at 8fa022519 + this test, before the
/// decoder change): `left: [(Blob(b"\xab"), Integer(20))]` — one entry against
/// the golden's two; the empty-key entry was silently absent.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn ac3_select_returns_empty_blob_map_key() {
    let rows = select_rows().await;

    let m_blob = map_entries(&rows, 1, "m_blob");
    assert_eq!(
        m_blob,
        vec![
            (Value::blob(Vec::<u8>::new()), Value::Integer(10)),
            (Value::blob(vec![0xabu8]), Value::Integer(20)),
        ],
        "golden id 1 m_blob is path:[\"\"]=10 and path:[\"ab\"]=20 (hex), i.e. \
         Blob(b\"\") and Blob(b\"\\xab\")"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// AC4 — the COMPACTION (per-element) read path
// ════════════════════════════════════════════════════════════════════════════

/// AC4. This test PINS EXISTING CORRECT BEHAVIOUR; it does not prove a fix.
///
/// The compaction substrate was ALREADY empty-key-safe: `record_element` threads
/// the RAW `cell_path` (empty for an empty key) and downstream reconcile keys on
/// those raw bytes, never on `decoded_key`. So the loss the fix removes was
/// confined to the collapsed `Value::Map`, and this lane is a regression guard —
/// the fix must not disturb the per-element output.
///
/// It additionally asserts the (post-fix) `decoded_key == Some(Value::Text(""))`
/// now rides along on the element, which is the one observable this lane gains.
///
/// MEASURED pre-fix, which is the evidence for the paragraph above: the
/// `cell_path.is_empty()` element WAS found, carrying `value == Some(Integer(1))`
/// and `is_deleted == false` — the per-element substrate was already correct.
/// The only pre-fix failure here was `decoded_key: left None, right
/// Some(Text(b""))`, plus the collapsed-`Value::Map` assertion below (the half
/// the fix actually repairs).
#[tokio::test]
async fn ac4_compaction_read_surfaces_empty_key_element() {
    let schema = load_schema();
    let table_dir = table_dir(&fixture_root());
    let data_path = single_data_db(&table_dir);

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init for compaction read"),
    );
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("open {data_path:?} for compaction read failed: {e}"));
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .unwrap_or_else(|e| panic!("compaction iterate of {data_path:?} failed: {e}"));
    assert!(
        !rows.is_empty(),
        "compaction read of the committed fixture yielded ZERO rows"
    );

    // The id-1 partition (4-byte big-endian int pk).
    let subject = rows
        .iter()
        .find(|r| r.key.0.as_ref() == 1i32.to_be_bytes())
        .unwrap_or_else(|| panic!("no compaction row for pk id=1"));
    let complex = match &subject.row_data {
        CompactionRowData::Live { complex, .. } => complex,
        other => panic!("id=1 is a live row in the golden, got {other:?}"),
    };
    let m_text = complex
        .iter()
        .find(|c| c.column == "m_text")
        .unwrap_or_else(|| panic!("no complex column m_text in the compaction row"));

    let empty = m_text
        .elements
        .iter()
        .find(|e| e.cell_path.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "the empty-key element is absent from the per-element output: {:?}",
                m_text.elements
            )
        });
    assert_eq!(
        empty.value,
        Some(Value::Integer(1)),
        "golden id 1 m_text path:[\"\"] carries value 1"
    );
    assert_eq!(
        empty.decoded_key,
        Some(Value::text("")),
        "post-fix the element also carries the decoded empty key"
    );
    assert!(
        !empty.is_deleted,
        "the empty-key entry is LIVE data, not a tombstone"
    );

    // The collapsed value on the compaction path must carry it too (this is the
    // half the fix repairs).
    let collapsed = match peel_frozen(&m_text.collapsed_value) {
        Value::Map(entries) => entries,
        other => panic!("m_text collapsed value is not a Map: {other:?}"),
    };
    assert!(
        collapsed
            .iter()
            .any(|(k, v)| *k == Value::text("") && *v == Value::Integer(1)),
        "collapsed Value::Map on the compaction path must carry the empty key: {collapsed:?}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// CONTROL — the frozen map (a DIFFERENT encoding) and the contrast row
// ════════════════════════════════════════════════════════════════════════════

/// CONTROL 1. `m_frozen` is `frozen<map<text,int>>` — ONE inline
/// length-prefixed blob decoded by an entirely different path, MEASURED to
/// already handle an empty key. A change that "fixes" empty multicell keys by
/// breaking the frozen path must RED here.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn control_frozen_map_empty_key_unchanged() {
    let rows = select_rows().await;
    let m_frozen = map_entries(&rows, 1, "m_frozen");
    assert_eq!(
        m_frozen,
        vec![
            (Value::text(""), Value::Integer(1000)),
            (Value::text("k"), Value::Integer(2000)),
        ],
        "golden id 1 m_frozen is the inline {{\"\":1000,\"k\":2000}}"
    );
}

/// CONTROL 2. The id-2 CONTRAST row carries no empty key anywhere and must
/// decode identically before and after the fix — the fix changes nothing for an
/// ordinary map.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn control_contrast_row_unchanged() {
    let rows = select_rows().await;

    assert_eq!(
        map_entries(&rows, 2, "m_text"),
        vec![
            (Value::text("x"), Value::Integer(3)),
            (Value::text("y"), Value::Integer(4)),
        ]
    );
    assert_eq!(
        map_entries(&rows, 2, "m_ascii"),
        vec![(Value::text("w"), Value::Integer(300))]
    );
    assert_eq!(
        map_entries(&rows, 2, "m_blob"),
        vec![(Value::blob(vec![0xcdu8]), Value::Integer(30))]
    );
    assert_eq!(
        map_entries(&rows, 2, "m_frozen"),
        vec![(Value::text("j"), Value::Integer(3000))]
    );
}

// ── schema / path helpers for the compaction lane ───────────────────────────

fn load_schema() -> TableSchema {
    let cql = std::fs::read_to_string(schema_path())
        .unwrap_or_else(|e| panic!("committed schema unreadable: {e}"));
    // The committed .cql carries a CREATE KEYSPACE alongside the CREATE TABLE;
    // `parse_cql_schema` takes ONE CREATE TABLE, so slice that statement out of
    // the committed source rather than duplicating the column list here.
    let start = cql
        .find("CREATE TABLE")
        .unwrap_or_else(|| panic!("committed schema has no CREATE TABLE"));
    let end = cql[start..]
        .find(';')
        .map(|i| start + i + 1)
        .unwrap_or_else(|| panic!("committed CREATE TABLE is unterminated"));
    cqlite_core::schema::parse_cql_schema(&cql[start..end])
        .unwrap_or_else(|e| panic!("committed schema must parse: {e}"))
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("fixture dir unreadable: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one Data.db in {dir:?}");
    found.pop().expect("length asserted as 1")
}
