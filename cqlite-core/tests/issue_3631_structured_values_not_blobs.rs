//! Issue #3631 (oracle-driven) — a structured value whose type the schema NAMES must
//! decode from that type, never degrade to `Value::Blob`.
//!
//! ## The defect
//!
//! `parse_simple_udt_field_value` matched a CLOSED SET of primitive types and fell
//! back to `Value::Blob` for everything else, while the schema naming the real type
//! was in hand and unread — the silent degradation #28 (no-heuristics) forbids. So a
//! COLLECTION-typed field of a frozen UDT (`frozen<map<text,int>>`, `list`, `set`)
//! surfaced to callers as bytes.
//!
//! ## SCOPE: instance B only
//!
//! #3631 was filed as one class with TWO instances. **Instance A — a non-frozen
//! `map<frozen<udt>, int>` cell-path key — is FIXED ON MAIN by #3612 / PR #3736**
//! (`8c503f7cf`), which landed `complex_column/cell_path_key.rs` and its own test
//! suite. Its coverage lives there; nothing in this file asserts it, and criteria 1
//! and 2 of #3631 are satisfied upstream rather than here. Do not re-add cell-path
//! cases to this file.
//!
//! ## Oracle — Cassandra, never CQLite's own prior output (#3042)
//!
//! PRIMARY SOURCE, read at the pinned tag:
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/CollectionSerializer.java`
//!   — `writeCollectionSize` is `output.putInt(elements)` (4-byte BE i32 count) and
//!   `writeValue` is `putInt(size)` + bytes (`putInt(-1)` for null). So a frozen
//!   `map<text,int>` holding `{"a": 1}` is exactly
//!   `00000001 00000001 61 00000004 00000001` — the 17 bytes this fixture
//!   carries (COUNTED, not eyeballed: 4 + 4 + 1 + 4 + 4; an earlier revision of this
//!   line, and of five sibling comments, said 20).
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java` (a
//!   `TupleType`) — per-field `[i32 size][bytes]`, `-1` for null.
//!
//! COMMITTED GOLDEN: `test-data/fixtures/issue_3504/` is **Cassandra-5.0.2-written**
//! (a CQLite-written round-trip could not be the oracle here — both sides would make
//! the identical mistake, #3042). Its `sstabledump -l` JSONL renders
//!
//! * `udt_hashable_shapes` row 3 `stn` as
//!   `[[{"label": "unhashable", "m": {"a": 1}}, 30]]`.
//!
//! ## Fixtures are COMMITTED ⇒ fail closed (#3220)
//! No `CQLITE_DATASETS_ROOT`, no skip path: absence is a broken checkout, and a
//! dataset-dependent case that decodes zero rows is a failure, never a pass.
#![cfg(feature = "cli-helpers")]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};

use cqlite_core::{Database, Value};

const KEYSPACE: &str = "test_udt_collision";

// ── Fixture resolution (checkout-relative; committed ⇒ must_run) ────────────

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// Assert the committed fixture is intact, GLOBBING the table directory: a
/// regeneration mints a fresh table UUID, so a hardcoded path would rot.
fn fixture_root() -> PathBuf {
    let root = repo_root().join("test-data/fixtures/issue_3504");
    for table in ["udt_collide", "udt_hashable_shapes"] {
        let dirs: Vec<PathBuf> = std::fs::read_dir(root.join(KEYSPACE))
            .unwrap_or_else(|e| panic!("committed fixture keyspace dir unreadable ({root:?}): {e}"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{table}-")))
            })
            .collect();
        assert_eq!(
            dirs.len(),
            1,
            "expected exactly one {table}-* dir under {root:?}, got {dirs:?}"
        );
        let data_present = std::fs::read_dir(&dirs[0])
            .unwrap_or_else(|e| panic!("fixture table dir unreadable: {e}"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .any(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            });
        assert!(
            data_present,
            "no *-Data.db under {:?} — the binaries are force-added; see \
             test-data/fixtures/issue_3504/README.md",
            dirs[0]
        );
    }
    root
}

async fn open_db() -> Database {
    let config = IngestionConfig {
        schema_paths: vec![repo_root().join("test-data/schemas/issue-3504-udt-collision.cql")],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };
    ingest(config)
        .await
        .expect("committed fixture must ingest (it is git-tracked source)")
        .database
}

/// Every row of `table` keyed by `id`, as decoded `Value`s — the DECODER's own
/// output, which is what this file is about (a renderer's spelling is not).
async fn rows_by_id(table: &str) -> Vec<(i32, std::collections::HashMap<String, Value>)> {
    let db = open_db().await;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{table}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over committed fixture {table} failed: {e}"));
    assert!(
        !result.rows.is_empty(),
        "committed fixture {table} decoded ZERO rows (0-rows-when-present is a failure)"
    );
    result
        .rows
        .iter()
        .map(|r| {
            let id = match r.values.get("id") {
                Some(Value::Integer(i)) => *i,
                other => panic!("row without an integer id in {table}: {other:?}"),
            };
            let values = r
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            (id, values)
        })
        .collect()
}

type Rows = Vec<(i32, std::collections::HashMap<String, Value>)>;

/// The decoded cell for `column` in the row with primary key `id`. Fail-closed: a
/// missing row or a NULL cell is a decode regression, never a skip.
fn cell(rows: &Rows, id: i32, column: &str) -> Value {
    let row = rows
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    let v = row
        .get(column)
        .unwrap_or_else(|| panic!("row id={id} has no column {column}"));
    assert!(
        !matches!(v, Value::Null),
        "row id={id} column {column} decoded NULL; the fixture writes a value there"
    );
    v.clone()
}

/// `frozen<X>` is a TYPE-system marker, not a value distinction — the serialized
/// bytes of `X` are identical either way. CQLite surfaces it as a `Value::Frozen`
/// wrapper in some positions and not others, so peel it before comparing VALUES.
fn unfrozen(v: &Value) -> &Value {
    match v {
        Value::Frozen(inner) => unfrozen(inner),
        other => other,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// INSTANCE B — a collection-typed field of a frozen UDT
// ════════════════════════════════════════════════════════════════════════════

/// Criterion 3. `udt_hashable_shapes` row 3's `stn` is
/// `frozen<set<frozen<tuple<frozen<unhashable_fields>, int>>>>`, and
/// `unhashable_fields` declares `m frozen<map<text,int>>`.
///
/// Golden: `[[{"label": "unhashable", "m": {"a": 1}}, 30]]`. Before #3631 `m` arrived
/// as the 17 raw bytes `00000001 00000001 61 00000004 00000001`.
#[tokio::test]
async fn instance_b_collection_field_of_a_frozen_udt_decodes_to_a_map() {
    let rows = rows_by_id("udt_hashable_shapes").await;
    let stn = cell(&rows, 3, "stn");

    let members = match unfrozen(&stn) {
        Value::Set(members) | Value::List(members) => members.clone(),
        other => panic!("`stn` must decode to a set, got {other:?}"),
    };
    assert_eq!(members.len(), 1, "the golden set has one member: {stn:?}");

    let components = match unfrozen(&members[0]) {
        Value::Tuple(components) => components.clone(),
        other => panic!("`stn`'s member must decode to a 2-tuple, got {other:?}"),
    };
    assert_eq!(components.len(), 2, "golden tuple: [<udt>, 30]");
    assert_eq!(
        unfrozen(&components[1]),
        &Value::Integer(30),
        "the tuple's second component is the golden's 30"
    );

    let udt = match unfrozen(&components[0]) {
        Value::Udt(udt) => udt.clone(),
        other => panic!("`stn`'s tuple must hold an `unhashable_fields` UDT, got {other:?}"),
    };
    assert_eq!(udt.type_name, "unhashable_fields");
    assert_eq!(udt.keyspace, KEYSPACE);
    assert_eq!(
        udt.fields.len(),
        2,
        "golden fields: label, m — got {:?}",
        udt.fields
    );
    assert_eq!(udt.fields[0].name, "label");
    assert_eq!(udt.fields[0].value, Some(Value::text("unhashable")));

    assert_eq!(udt.fields[1].name, "m");
    let m = udt.fields[1]
        .value
        .as_ref()
        .unwrap_or_else(|| panic!("`m` decoded NULL; the golden writes {{\"a\": 1}}"));
    assert_eq!(
        unfrozen(m),
        &Value::Map(vec![(Value::text("a"), Value::Integer(1))]),
        "a `frozen<map<text,int>>` field of a frozen UDT must decode to the golden's \
         {{\"a\": 1}}, not to its 17 serialized bytes (issue #3631 instance B)"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// CRITERION 4 — the fix is at the CLASS, not at the one type the fixture uses.
//
// ROUTE TAKEN, stated explicitly: the committed corpus declares NO `list`- or
// `set`-typed UDT field. `unhashable_fields.m` (`frozen<map<text,int>>`) is the only
// collection-typed UDT field anywhere in `test-data/fixtures/issue_3504/`, and the
// schema is committed source (`test-data/schemas/issue-3504-udt-collision.cql`);
// generating a new Cassandra-written fixture needs Docker, which the gate does not
// have. So the `list` and `set` halves are covered at the DECODER's own level, with
// bytes DERIVED FROM CASSANDRA SOURCE rather than captured from CQLite's output, in
// `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/regression_3631_typed_value_tests.rs`
// — which also re-decodes the EXACT 17 bytes this fixture carries for
// `unhashable_fields.m`, tying that hand-built layer back to this real corpus.
