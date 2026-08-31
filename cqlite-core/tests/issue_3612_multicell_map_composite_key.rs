//! Issue #3612 — a MULTICELL map's COMPOSITE cell-path key on the public query
//! surface, against CASSANDRA-WRITTEN bytes.
//!
//! ## The oracle (why it is not a CQLite round-trip)
//! The subject is the committed Cassandra 5.0 fixture
//! `test-data/fixtures/issue_3504/test_udt_collision/udt_collide-*` and its
//! `sstabledump` golden `nb-1-big-Data.db.jsonl`. A CQLite-written/CQLite-read
//! round-trip would be INVALID here (doctrine, issue #3042): both sides would
//! share any framing error, so a symmetric test is invariant to exactly the class
//! of defect this is. Every expectation below is therefore DERIVED FROM THE
//! GOLDEN at run time, never from CQLite's own prior output.
//!
//! ## The subjects and the controls, in ONE table
//! `test_udt_collision.udt_collide` carries both spellings of the same map:
//!
//! | column | declared type                          | role |
//! |--------|----------------------------------------|------|
//! | `cm`   | `map<frozen<collide>, int>`            | SUBJECT — non-frozen ⇒ MULTICELL ⇒ key in the CELL PATH |
//! | `tm`   | `map<frozen<collide_twin>, int>`       | SUBJECT — same, distinct UDT type |
//! | `fcm`  | `frozen<map<frozen<collide>, int>>`    | CONTROL — frozen ⇒ one cell ⇒ key already decoded structurally |
//! | `ftm`  | `frozen<map<frozen<collide_twin>, int>>` | CONTROL — same |
//!
//! Before this fix `cm`/`tm` keys surfaced as `Value::Blob` (raw cell-path bytes)
//! while `fcm`/`ftm` surfaced as structured UDTs — the divergence #3612 closes.
//! Asserting the four in one test is what makes the control meaningful: a
//! regression that broke BOTH spellings could not hide behind the subject alone.
//!
//! The golden renders a multicell UDT cell path as its components joined with
//! `:` (Cassandra's `TupleType.getString`, which `UserType` inherits) —
//! `"path": ["key-type-marker:key-keyspace-marker:key-proto-marker:100"]` — and
//! the frozen spelling as a JSON-object map key. Both are parsed below so the
//! SUBJECT and the CONTROL are checked against the same authority.
//!
//! ## Fixture policy
//! The fixture is COMMITTED (git-tracked binaries, force-added), so this test is
//! MUST-RUN and fails closed: no `CQLITE_DATASETS_ROOT`, no SKIP path, and a
//! zero-row read is a FAILURE, never a pass.
//!
//! Needs `cli-helpers` for the public `cqlite_core::ingestion` surface (the same
//! entry point `cqlite.open(path, schema=…)` uses in the Python binding), which
//! the gate's `core-tests` and `--lite`'s cqlite-core lane both enable.
#![cfg(feature = "cli-helpers")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::{UdtValue, Value};
use cqlite_core::Database;

const KEYSPACE: &str = "test_udt_collision";
const TABLE: &str = "udt_collide";
const QUERY: &str = "SELECT * FROM test_udt_collision.udt_collide";
/// The fixture row that carries every map column (rows 2 and 3 are contrasts).
const SUBJECT_ROW_ID: i32 = 1;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a workspace parent")
        .to_path_buf()
}

fn fixture_root() -> PathBuf {
    workspace_root()
        .join("test-data")
        .join("fixtures")
        .join("issue_3504")
}

fn schema_path() -> PathBuf {
    workspace_root()
        .join("test-data")
        .join("schemas")
        .join("issue-3504-udt-collision.cql")
}

/// The single `udt_collide-<uuid>` table directory. GLOBBED, because a fixture
/// regeneration mints a new table UUID and a hardcoded path would rot.
fn table_dir() -> PathBuf {
    let ks_dir = fixture_root().join(KEYSPACE);
    let mut hits: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .unwrap_or_else(|e| panic!("committed fixture keyspace dir unreadable {ks_dir:?}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .collect();
    hits.sort();
    assert_eq!(
        hits.len(),
        1,
        "expected exactly one {TABLE}-* dir under {ks_dir:?}, got {hits:?}"
    );
    hits.remove(0)
}

// ---------------------------------------------------------------------------
// The golden (sstabledump JSONL) — the sole source of expected values
// ---------------------------------------------------------------------------

/// The expected map key components and the expected map value, per column,
/// derived from the committed `sstabledump` golden.
struct GoldenEntry {
    /// The UDT field values in DECLARED order, as strings (`""` for the int).
    components: Vec<String>,
    value: i64,
}

/// Parse the golden for the subject row, returning `column -> GoldenEntry`.
///
/// Two renderings are handled because sstabledump uses two:
/// * a MULTICELL entry is a cell with a `path` array whose sole element is the
///   UDT's components joined with `:` (`TupleType.getString`);
/// * a FROZEN map is one cell whose `value` is a JSON object keyed by the
///   JSON-rendered UDT.
fn golden_entries() -> BTreeMap<String, GoldenEntry> {
    let jsonl_path = {
        let dir = table_dir();
        let mut hits: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("fixture dir unreadable {dir:?}: {e}"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db.jsonl"))
            })
            .collect();
        hits.sort();
        assert_eq!(
            hits.len(),
            1,
            "expected exactly one sstabledump golden in {dir:?}, got {hits:?}"
        );
        hits.remove(0)
    };
    let raw = std::fs::read_to_string(&jsonl_path)
        .unwrap_or_else(|e| panic!("committed golden unreadable {jsonl_path:?}: {e}"));

    let mut out: BTreeMap<String, GoldenEntry> = BTreeMap::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let doc: serde_json::Value =
            serde_json::from_str(line).expect("the committed golden is valid JSON per line");
        let key = doc["partition"]["key"][0].as_str().unwrap_or_default();
        if key != SUBJECT_ROW_ID.to_string() {
            continue;
        }
        for row in doc["rows"].as_array().into_iter().flatten() {
            for cell in row["cells"].as_array().into_iter().flatten() {
                let name = match cell["name"].as_str() {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                // MULTICELL: `"path": ["a:b:c:100"]`, `"value": 1`
                if let Some(path) = cell["path"][0].as_str() {
                    let components: Vec<String> = path.split(':').map(|s| s.to_string()).collect();
                    let value = cell["value"]
                        .as_i64()
                        .expect("a multicell map entry's golden value is an integer");
                    out.insert(name, GoldenEntry { components, value });
                    continue;
                }
                // FROZEN: `"value": { "{\"_type\": \"a\", …}": 3 }`
                if let Some(obj) = cell["value"].as_object() {
                    if obj.len() != 1 {
                        continue;
                    }
                    let (k, v) = obj.iter().next().expect("len checked");
                    let Ok(key_doc) = serde_json::from_str::<serde_json::Value>(k) else {
                        continue;
                    };
                    let Some(key_obj) = key_doc.as_object() else {
                        continue;
                    };
                    let components: Vec<String> = key_obj
                        .values()
                        .map(|fv| match fv {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect();
                    let value = v
                        .as_i64()
                        .expect("a frozen map entry's golden value is an integer");
                    out.insert(name, GoldenEntry { components, value });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Reading through the public surface
// ---------------------------------------------------------------------------

async fn open_fixture() -> Database {
    let schema = schema_path();
    assert!(
        schema.is_file(),
        "committed schema missing: {schema:?} — this test is MUST-RUN"
    );
    let data_db = std::fs::read_dir(table_dir())
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .any(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        });
    assert!(
        data_db,
        "no *-Data.db under {:?} — the binaries are git-tracked (force-added); \
         see test-data/fixtures/issue_3504/README.md",
        table_dir()
    );
    ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    })
    .await
    .expect("ingesting the committed #3504 fixture must succeed")
    .database
}

/// Every column of the subject row, read through `Database::execute`.
async fn subject_row() -> BTreeMap<String, Value> {
    let db = open_fixture().await;
    let result = db
        .execute(QUERY)
        .await
        .expect("SELECT over the committed fixture must succeed");
    assert!(
        !result.rows.is_empty(),
        "zero rows from a PRESENT fixture is a decode failure, never a skip"
    );
    for row in &result.rows {
        if row.values.get("id") == Some(&Value::Integer(SUBJECT_ROW_ID)) {
            return row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
        }
    }
    panic!(
        "fixture row id={SUBJECT_ROW_ID} not found in {} rows",
        result.rows.len()
    );
}

/// Peel any `Value::Frozen` wrapper. Transparent at EVERY render surface (CLI
/// `ValueFormatter::format_value`, the CLI JSON writer, Arrow's
/// `unwrap_frozen_value`, and both bindings), so it is not part of the contract
/// asserted here — but see `frozen_wrapper_shape_matches_the_frozen_control`,
/// which pins that the subject and the control agree on it.
fn peel(v: &Value) -> &Value {
    match v {
        Value::Frozen(inner) => peel(inner),
        other => other,
    }
}

fn sole_map_entry(column: &str, v: &Value) -> (Value, Value) {
    match peel(v) {
        Value::Map(pairs) => {
            assert_eq!(pairs.len(), 1, "{column}: the fixture stores one entry");
            pairs[0].clone()
        }
        other => panic!("{column}: expected a Value::Map, got {other:?}"),
    }
}

fn udt_of(column: &str, v: &Value) -> UdtValue {
    match peel(v) {
        Value::Udt(u) => (**u).clone(),
        other => panic!(
            "{column}: expected a STRUCTURED Value::Udt map key (issue #3612), got {other:?}"
        ),
    }
}

/// The UDT's field values as strings in declared order, comparable to the
/// golden's `:`-joined components.
fn components_of(u: &UdtValue) -> Vec<String> {
    u.fields
        .iter()
        .map(|f| match &f.value {
            Some(Value::Text(s)) => String::from_utf8_lossy(s).into_owned(),
            Some(Value::Integer(i)) => i.to_string(),
            Some(other) => panic!("unexpected field value shape {other:?}"),
            None => String::new(),
        })
        .collect()
}

fn as_int(column: &str, v: &Value) -> i64 {
    match peel(v) {
        Value::Integer(i) => i64::from(*i),
        Value::BigInt(i) => *i,
        other => panic!("{column}: expected an integer map value, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The assertions
// ---------------------------------------------------------------------------

/// THE #3612 ASSERTION: the MULTICELL `cm`/`tm` keys decode to structured UDTs
/// matching the sstabledump golden — asserted alongside the frozen `fcm`/`ftm`
/// controls, which already did.
#[tokio::test]
async fn multicell_and_frozen_map_udt_keys_both_match_the_sstabledump_golden() {
    let row = subject_row().await;
    let golden = golden_entries();
    for column in ["cm", "tm", "fcm", "ftm"] {
        let expected = golden
            .get(column)
            .unwrap_or_else(|| panic!("the golden must carry column '{column}'"));
        let cell = row
            .get(column)
            .unwrap_or_else(|| panic!("SELECT * must project column '{column}'"));
        let (key, value) = sole_map_entry(column, cell);
        let udt = udt_of(column, &key);
        assert_eq!(
            components_of(&udt),
            expected.components,
            "{column}: the decoded UDT key must match the sstabledump golden's components"
        );
        assert_eq!(
            udt.keyspace, KEYSPACE,
            "{column}: the key's UDT keyspace identity survives"
        );
        assert_eq!(
            as_int(column, &value),
            expected.value,
            "{column}: the map value must match the golden"
        );
    }
}

/// The declared UDT TYPE NAME distinguishes `cm` (`collide`) from `tm`
/// (`collide_twin`) even though their field values are identical — the property
/// an opaque blob key destroys, since both keys are the same bytes.
#[tokio::test]
async fn multicell_map_keys_keep_the_declared_udt_type_identity() {
    let row = subject_row().await;
    let (cm_key, _) = sole_map_entry("cm", &row["cm"]);
    let (tm_key, _) = sole_map_entry("tm", &row["tm"]);
    let cm = udt_of("cm", &cm_key);
    let tm = udt_of("tm", &tm_key);
    assert_eq!(cm.type_name, "collide");
    assert_eq!(tm.type_name, "collide_twin");
    assert_eq!(
        components_of(&cm),
        components_of(&tm),
        "the fixture stores identical field values in both keys, on purpose"
    );
    assert_ne!(
        cm.type_name, tm.type_name,
        "identical fields must stay distinguishable by declared type"
    );
}

/// The MULTICELL key and the FROZEN control present the key IDENTICALLY —
/// same `Value` variant, no `Value::Frozen` wrapper on either side.
///
/// This is issue #3612's parity property, not a cosmetic one. The fixture stores
/// the SAME `collide` value in `cm`'s multicell key and `fcm`'s frozen key on
/// purpose, so the two must compare EQUAL: a consumer matching `Value::Udt(_)`
/// (rather than unwrapping `Frozen` first) would otherwise see the two legal
/// spellings of one `map<frozen<collide>, int>` as different shapes.
///
/// Pre-fix this failed with `Blob` vs `Udt`. It also fails if the delegated
/// `frozen<…>` wrapper is left in place (`Frozen(Udt)` vs `Udt`), which is what
/// the measurement behind `unwrap_frozen_cell_path_key` established.
#[tokio::test]
async fn multicell_key_presents_identically_to_the_frozen_control() {
    let row = subject_row().await;
    let depth = |mut v: &Value| {
        let mut n = 0;
        while let Value::Frozen(inner) = v {
            n += 1;
            v = inner;
        }
        n
    };
    for (multicell, frozen) in [("cm", "fcm"), ("tm", "ftm")] {
        let (multicell_key, _) = sole_map_entry(multicell, &row[multicell]);
        let (frozen_key, _) = sole_map_entry(frozen, &row[frozen]);
        assert_eq!(
            depth(&frozen_key),
            0,
            "{frozen}: the frozen control's key carries no Frozen wrapper \
             (it resolves its key type from the on-disk marshal form) — if this \
             changed, re-derive the expectation below rather than relaxing it"
        );
        assert_eq!(
            depth(&multicell_key),
            0,
            "{multicell}: a multicell map key must not carry a redundant \
             Value::Frozen wrapper the frozen spelling lacks, got {multicell_key:?}"
        );
        assert_eq!(
            multicell_key, frozen_key,
            "{multicell} vs {frozen}: the two legal spellings of one \
             map<frozen<udt>, int> must present the SAME key value"
        );
    }
}
