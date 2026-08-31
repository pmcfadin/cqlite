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

/// The DECLARED field order of the `collide` / `collide_twin` UDTs, from
/// `test-data/schemas/issue-3504-udt-collision.cql`. Named explicitly so the
/// golden's frozen-map key (a JSON object, whose key order depends on whether
/// `serde_json`'s `preserve_order` feature happens to be enabled) is read BY
/// NAME rather than by iteration order.
const COLLIDE_FIELD_ORDER: [&str; 4] = ["_type", "_keyspace", "__proto__", "real_field"];

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
                    // Read the fields BY NAME, in the DECLARED order, never by
                    // iterating the JSON object. `serde_json::Map` is a
                    // `BTreeMap` unless the `preserve_order` feature is on, so
                    // `.values()` would silently reorder to
                    // `__proto__, _keyspace, _type, real_field` in any build
                    // without that feature and this test would fail on correct
                    // output. The declared order is the fixture's schema
                    // (`test-data/schemas/issue-3504-udt-collision.cql`,
                    // `CREATE TYPE collide`), which is also the order
                    // `sstabledump` uses for the multicell `path` rendering, so
                    // this makes the two goldens directly comparable.
                    let components: Vec<String> = COLLIDE_FIELD_ORDER
                        .iter()
                        .map(|fname| {
                            match key_obj.get(*fname).unwrap_or_else(|| {
                                panic!("golden frozen-map key has no field '{fname}'")
                            }) {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            }
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

// ===========================================================================
// SECOND SUBJECT: a MULTICELL TUPLE key, on Cassandra-written bytes (AC 4)
// ===========================================================================
//
// The #3504 fixture's composite keys are all UDTs, so until `test_nested_udt_keys`
// landed on main (#3647) the TUPLE shape rested only on unit tests built from
// Cassandra's framing convention rather than on Cassandra-written bytes. This
// closes that: `m_tuple_udt map<frozen<tuple<frozen<key_part>, int>>, int>` is a
// MULTICELL map whose key is a tuple whose first component is a frozen UDT, so it
// exercises the composite cell-path route one nesting level deeper than `cm`.
//
// Expectations are derived from the committed `sstabledump` golden at run time,
// as for `cm`/`tm` — never from CQLite's own output (doctrine #3042).

/// `test_nested_udt_keys` is git-tracked under the CHECKOUT's
/// `test-data/datasets`, and MEASURED to be absent from the fleet-local
/// `/data/datasets` root that `CQLITE_DATASETS_ROOT` usually names — reading it
/// through that root yields ZERO rows, silently. So the root is resolved PER
/// TABLE (issue #3220) rather than from the env var alone.
#[path = "support/datasets_root.rs"]
mod datasets_root;

const TUPLE_KEYSPACE: &str = "test_nested_udt_keys";
const TUPLE_TABLE: &str = "nested_udt_keys";

/// One `m_tuple_udt` key as the golden RENDERS it: `key_part.label`,
/// `key_part.rank`, and the tuple's second component, each as the string the
/// golden carries (`None` for a null UDT field) — so the comparison is against
/// sstabledump's own text rather than a re-encoding of it.
type GoldenTupleKey = (Option<String>, Option<String>, String);

/// One golden `m_tuple_udt` cell path, decoded from sstabledump's composite
/// rendering into a [`GoldenTupleKey`].
///
/// The rendering nests two levels: the tuple's components are joined with `:`,
/// and the inner UDT's own fields are joined with an ESCAPED `\:`, with `\@`
/// standing for a null field. So `charlie\:3:8` is
/// `tuple(key_part{label: "charlie", rank: 3}, 8)`. Validated against all three
/// shapes the golden actually contains — a plain one, an all-null UDT
/// (`\@\:\@:0`) and an empty-label one (`\:0:0`) — and the arity is asserted, so
/// a change in sstabledump's escaping reds here instead of silently mis-parsing.
fn parse_tuple_golden_path(path: &str) -> GoldenTupleKey {
    // Split on UNESCAPED ':' only.
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            // An escape: keep both bytes, so `\:` cannot end a component and
            // `\@` survives for the null test below.
            cur.push(c);
            if let Some(n) = chars.next() {
                cur.push(n);
            }
        } else if c == ':' {
            parts.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    parts.push(cur);
    assert_eq!(
        parts.len(),
        2,
        "golden tuple path {path:?} must render exactly 2 tuple components"
    );
    let udt_fields: Vec<&str> = parts[0].split("\\:").collect();
    assert_eq!(
        udt_fields.len(),
        2,
        "golden UDT component {:?} must render exactly 2 fields",
        parts[0]
    );
    let unnull = |s: &str| {
        if s == "\\@" {
            None
        } else {
            Some(s.to_string())
        }
    };
    (
        unnull(udt_fields[0]),
        unnull(udt_fields[1]),
        parts[1].clone(),
    )
}

/// THE AC-4 ASSERTION: every `m_tuple_udt` key on Cassandra-written bytes decodes
/// to a structured tuple whose first component is a structured UDT, matching the
/// golden component-for-component.
#[tokio::test]
async fn multicell_tuple_keys_match_the_sstabledump_golden() {
    let Some(root) = datasets_root::sstables_root_for_table(TUPLE_KEYSPACE, TUPLE_TABLE) else {
        panic!(
            "{TUPLE_KEYSPACE}.{TUPLE_TABLE} is GIT-TRACKED, so no candidate root \
             holding it is a checkout problem, not a skip (issue #3220)"
        );
    };
    let schema = workspace_root()
        .join("test-data")
        .join("schemas")
        .join("nested-udt-keys.cql");
    assert!(schema.is_file(), "committed schema missing: {schema:?}");

    // Golden: pk -> the set of (label, n, second) triples for m_tuple_udt.
    let table_dir = std::fs::read_dir(root.join(TUPLE_KEYSPACE))
        .expect("keyspace dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TUPLE_TABLE}-")))
        })
        .expect("one nested_udt_keys-* dir");
    let jsonl = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db.jsonl"))
        })
        .expect("committed golden");
    let raw = std::fs::read_to_string(&jsonl).expect("golden readable");
    let mut expected: BTreeMap<String, Vec<GoldenTupleKey>> = BTreeMap::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let doc: serde_json::Value = serde_json::from_str(line).expect("golden json");
        let pk = doc["partition"]["key"][0]
            .as_str()
            .unwrap_or_default()
            .to_string();
        for row in doc["rows"].as_array().into_iter().flatten() {
            for cell in row["cells"].as_array().into_iter().flatten() {
                if cell["name"].as_str() != Some("m_tuple_udt") {
                    continue;
                }
                if let Some(p) = cell["path"][0].as_str() {
                    expected
                        .entry(pk.clone())
                        .or_default()
                        .push(parse_tuple_golden_path(p));
                }
            }
        }
    }
    assert!(
        !expected.is_empty(),
        "the golden must carry m_tuple_udt entries; an empty expectation would \
         make this test vacuous"
    );

    let db = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.clone(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(TUPLE_KEYSPACE.to_string()),
    })
    .await
    .expect("ingesting the committed nested_udt_keys fixture must succeed")
    .database;
    let result = db
        .execute(&format!(
            "SELECT id, m_tuple_udt FROM {TUPLE_KEYSPACE}.{TUPLE_TABLE}"
        ))
        .await
        .expect("SELECT must succeed");
    assert!(
        !result.rows.is_empty(),
        "zero rows from a PRESENT fixture is a decode failure, never a skip"
    );

    let mut checked = 0usize;
    for row in &result.rows {
        let Some(Value::Integer(id)) = row.values.get("id") else {
            continue;
        };
        let Some(want) = expected.get(&id.to_string()) else {
            continue;
        };
        let cell = row
            .values
            .get("m_tuple_udt")
            .unwrap_or_else(|| panic!("id={id}: SELECT must project m_tuple_udt"));
        let pairs = match peel(cell) {
            Value::Map(p) => p,
            other => panic!("id={id}: expected a Value::Map, got {other:?}"),
        };
        assert_eq!(
            pairs.len(),
            want.len(),
            "id={id}: entry count must match the golden — a COLLAPSE would show here"
        );
        let mut got: Vec<GoldenTupleKey> = Vec::new();
        for (k, _v) in pairs {
            // The AC-4 property: a structured TUPLE whose first component is a
            // structured UDT — not opaque bytes, and not a flattened blob.
            let items = match peel(k) {
                Value::Tuple(items) => items,
                other => panic!(
                    "id={id}: a multicell TUPLE map key must decode structurally \
                     (issue #3612), got {other:?}"
                ),
            };
            assert_eq!(items.len(), 2, "id={id}: tuple<frozen<key_part>, int>");
            let udt = udt_of("m_tuple_udt", &items[0]);
            assert_eq!(udt.type_name, "key_part", "id={id}");
            let f = |name: &str| -> Option<String> {
                udt.fields
                    .iter()
                    .find(|f| f.name == name)
                    .and_then(|f| f.value.clone())
                    .and_then(|v| match v {
                        Value::Text(s) => Some(String::from_utf8_lossy(&s).into_owned()),
                        Value::Integer(i) => Some(i.to_string()),
                        Value::Null => None,
                        other => panic!("id={id}: unexpected field shape {other:?}"),
                    })
            };
            let second = match peel(&items[1]) {
                Value::Integer(i) => i.to_string(),
                other => panic!("id={id}: tuple's second component is an int, got {other:?}"),
            };
            got.push((f("label"), f("rank"), second));
        }
        got.sort();
        let mut want_sorted = want.clone();
        want_sorted.sort();
        assert_eq!(
            got, want_sorted,
            "id={id}: decoded tuple keys must match the sstabledump golden"
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        expected.len(),
        "every golden partition carrying m_tuple_udt must have been checked"
    );
}

// ===========================================================================
// R7: cross-spelling parity ON THE PUBLIC SURFACE, unpeeled (issue #3612)
// ===========================================================================
//
// The assertion this class of defect needed and did not have for three rounds.
// R3-F2 was the same class; it was fixed by matching the frozen side per type and
// verified against the UDT keys `cm`/`tm`, the only composite subjects the fixture
// then had. The TUPLE subject added later exposed a case that fix never covered:
// multicell `Frozen(Tuple([Frozen(Udt), Int]))` against frozen `Tuple([Udt, Int])`.
// The bindings hide the wrappers, so only a comparison of the raw `Value` — no
// `peel`, no `matches!` on the inner — can see it.
//
// ROOT CAUSE, and why the fix is at the type-selection site: the multicell branch
// resolved its key type from the SCHEMA short form while the frozen reader prefers
// the authoritative MARSHAL spelling. Cassandra's own `Statistics.db` for
// `test_nested_udt_keys` shows the marshal key type is IDENTICAL for both columns
// (`TupleType(UserType(..),Int32Type)`, the frozen one merely under an outer
// `FrozenType` that is stripped), while the schema form
// `frozen<tuple<frozen<key_part>, int>>` carries `frozen` at BOTH levels — so the
// divergence was entirely in which string each reader started from. Peeling the
// outer wrapper afterwards could not have fixed the INNER one.
//
// WHAT EQUALITY IS ASSERTABLE, per subject: only where the fixture stores the SAME
// logical key in both spellings. That holds by construction for `cm`/`fcm` and
// `tm`/`ftm` (the #3504 fixture stores one key in both), and for `m_tuple_udt` /
// `f_map_tuple_udt` at id=3 ONLY — the other rows hold deliberately different data
// (`charlie`/`delta` vs `mkey-a`/`mkey-b`), so a value comparison there would be
// meaningless. Those rows are covered by SHAPE equality instead, which is the
// property that generalises: same `Value` variant nesting, whatever the payload.

/// The variant nesting of a `Value`, ignoring payloads — so two keys holding
/// different data can still be compared for the property R7 is about.
fn variant_shape(v: &Value) -> String {
    match v {
        Value::Frozen(inner) => format!("Frozen({})", variant_shape(inner)),
        Value::Tuple(items) => format!(
            "Tuple[{}]",
            items
                .iter()
                .map(variant_shape)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::List(items) => format!(
            "List[{}]",
            items
                .iter()
                .map(variant_shape)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Set(items) => format!(
            "Set[{}]",
            items
                .iter()
                .map(variant_shape)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(pairs) => format!(
            "Map[{}]",
            pairs
                .iter()
                .map(|(k, val)| format!("{}=>{}", variant_shape(k), variant_shape(val)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Udt(_) => "Udt".to_string(),
        // Scalars: the VARIANT NAME only. `Debug` renders `Integer(8)`, and the
        // payload is exactly what this function exists to ignore, so truncate at
        // the first `(`. (`type_name_of_val` was tried and prints
        // `cqlite_core::types::Value` for every variant — useless in a diff.)
        other => {
            let d = format!("{other:?}");
            d.split('(').next().unwrap_or("?").to_string()
        }
    }
}

fn hash_of(v: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// Every key of a map column, UNPEELED. `Frozen` at the COLUMN level is peeled
/// (a frozen map column is legitimately one `Frozen(Map)`), but nothing inside is.
fn map_keys_unpeeled(column: &str, v: &Value) -> Vec<Value> {
    let pairs = match v {
        Value::Map(p) => p,
        Value::Frozen(inner) => match &**inner {
            Value::Map(p) => p,
            other => panic!("{column}: expected Frozen(Map), got Frozen({other:?})"),
        },
        other => panic!("{column}: expected a Map, got {other:?}"),
    };
    pairs.iter().map(|(k, _)| k.clone()).collect()
}

/// THE R7 ASSERTION for the #3504 fixture's two UDT-keyed subjects: `cm` vs `fcm`
/// and `tm` vs `ftm` store the SAME key by construction, so their decoded keys must
/// be EQUAL as `Value`s and hash equally — compared with no wrapper peeling at all.
#[tokio::test]
async fn udt_keys_are_identical_across_spellings_unpeeled() {
    let row = subject_row().await;
    for (multicell, frozen) in [("cm", "fcm"), ("tm", "ftm")] {
        let mc = map_keys_unpeeled(multicell, &row[multicell]);
        let fz = map_keys_unpeeled(frozen, &row[frozen]);
        assert_eq!(mc.len(), 1, "{multicell}: the fixture stores one entry");
        assert_eq!(fz.len(), 1, "{frozen}: the fixture stores one entry");
        assert_eq!(
            mc[0],
            fz[0],
            "{multicell} vs {frozen}: the two spellings of one map must present the \
             SAME `Value` key with NO wrapper difference (issue #3612 R7). \
             multicell shape={} frozen shape={}",
            variant_shape(&mc[0]),
            variant_shape(&fz[0])
        );
        assert_eq!(
            hash_of(&mc[0]),
            hash_of(&fz[0]),
            "{multicell} vs {frozen}: equal keys must hash equally, or they are \
             distinct entries in any hashed projection"
        );
    }
}

/// THE R7 ASSERTION for the TUPLE subject. Value equality at id=3, where the
/// fixture stores the same key in both spellings; SHAPE equality everywhere else,
/// because the other rows hold deliberately different data.
#[tokio::test]
async fn tuple_keys_are_identical_across_spellings_unpeeled() {
    let Some(root) = datasets_root::sstables_root_for_table(TUPLE_KEYSPACE, TUPLE_TABLE) else {
        panic!("{TUPLE_KEYSPACE}.{TUPLE_TABLE} is git-tracked; absence is a checkout problem");
    };
    let db = ingest(IngestionConfig {
        schema_paths: vec![workspace_root()
            .join("test-data")
            .join("schemas")
            .join("nested-udt-keys.cql")],
        data_dir: root,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(TUPLE_KEYSPACE.to_string()),
    })
    .await
    .expect("ingest")
    .database;
    let result = db
        .execute(&format!(
            "SELECT id, m_tuple_udt, f_map_tuple_udt FROM {TUPLE_KEYSPACE}.{TUPLE_TABLE}"
        ))
        .await
        .expect("SELECT must succeed");
    assert!(!result.rows.is_empty(), "zero rows from a PRESENT fixture");

    let mut shape_checked = 0usize;
    let mut value_checked = 0usize;
    for row in &result.rows {
        let Some(Value::Integer(id)) = row.values.get("id") else {
            continue;
        };
        let (Some(mc_col), Some(fz_col)) = (
            row.values.get("m_tuple_udt"),
            row.values.get("f_map_tuple_udt"),
        ) else {
            continue;
        };
        if matches!(mc_col, Value::Null) || matches!(fz_col, Value::Null) {
            continue;
        }
        let mc = map_keys_unpeeled("m_tuple_udt", mc_col);
        let fz = map_keys_unpeeled("f_map_tuple_udt", fz_col);
        assert!(!mc.is_empty() && !fz.is_empty(), "id={id}");

        // SHAPE parity always: the variant nesting must not differ, whatever the
        // payload. This is what caught R7 — `Frozen(Tuple[Frozen(Udt),Int])` vs
        // `Tuple[Udt,Int]`.
        for k in &mc {
            assert_eq!(
                variant_shape(k),
                variant_shape(&fz[0]),
                "id={id}: a multicell tuple key must have the SAME `Value` nesting \
                 as the frozen spelling's (issue #3612 R7)"
            );
        }
        shape_checked += 1;

        // VALUE parity ONLY at id=3, AND DO NOT "FIX" THIS ONTO id=1.
        //
        // The two columns deliberately hold DIFFERENT data in the other partitions —
        // measured from the golden: id=1 is `charlie`/`delta` in `m_tuple_udt`
        // against `mkey-a`/`mkey-b` in `f_map_tuple_udt`, and id=2 is a
        // null-both-fields / empty-label pair against `nullrank3`/`rank=5`. id=3
        // (`solo`, 99, 42) is the ONE partition storing the same logical key in both
        // spellings, so it is the only one where a value comparison means anything.
        // Widening this to another partition would fail for a reason that has
        // nothing to do with cross-spelling parity, and the `value_checked == 1`
        // assertion below exists so narrowing it to zero cannot pass silently
        // either. Every partition is still covered by the SHAPE assertion above,
        // which is the half that generalises.
        if *id == 3 {
            assert_eq!(mc.len(), 1, "id=3 holds one key");
            assert_eq!(
                mc[0], fz[0],
                "id=3 stores the SAME key in both spellings, so the decoded \
                 `Value`s must be equal with no peeling"
            );
            assert_eq!(hash_of(&mc[0]), hash_of(&fz[0]), "id=3: hashes must agree");
            value_checked += 1;
        }
    }
    assert!(
        shape_checked >= 3,
        "expected the three populated partitions to be shape-checked, got {shape_checked}"
    );
    assert_eq!(
        value_checked, 1,
        "exactly one partition (id=3) stores the same key in both spellings; if \
         this is 0 the value-equality half ran against nothing"
    );
}
