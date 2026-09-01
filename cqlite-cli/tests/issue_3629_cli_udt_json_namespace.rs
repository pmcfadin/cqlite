//! Issue #3629 (oracle-driven) — `cqlite --format json` must render a UDT as its
//! DECLARED FIELDS AND NOTHING ELSE.
//!
//! ## The defect
//! `JSONWriter::value_to_json`'s `Value::Udt` arm used to `insert("_type",
//! type_name)` into the SAME object that then receives the UDT's own declared
//! fields — type identity and user data sharing one channel (the #3504 parent
//! class). A UDT that DECLARES a field named `_type` (legal CQL via a quoted
//! identifier) silently OVERWROTE the marker; every UDT that does not declare one
//! carried a key Cassandra never wrote.
//!
//! ## Oracle
//! PRIMARY SOURCE:
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java:261`
//! (`toJSONString`) iterates `types.size()` over `stringFieldNames` only and emits
//! NO type key and NO keyspace key, appending the literal `null` for an absent
//! field buffer (line 280) — which is also why row 3's `"_type": null` below is
//! CORRECT output for a null FIELD rather than a leftover marker.
//!
//! The Cassandra-written fixture `test-data/fixtures/issue_3504/` and its
//! `sstabledump` JSONL golden, which injects nothing: the non-colliding `p` cell
//! dumps as `{"label": ..., "real_field": 7}`. Expectations below are quoted from
//! that golden, never from the CLI's previous output.
//!
//! ## WHY THE GOLDEN IS BLIND FOR MOST COLUMNS (read before adding a case)
//! For `collide`/`collide_twin` columns the injected `_type` is written FIRST and
//! the user's own `_type` field overwrites it; `serde_json` has `preserve_order`
//! and `Map::insert` keeps an existing key's POSITION, and `collide` declares
//! `"_type"` first — so buggy output is textually identical to the golden, key
//! order included. Those cases PASS ON UNFIXED CODE and are labelled below as
//! preservation guards, never as evidence.
//!
//! RED-capable subjects (no declared `_type`): `udt_collide.p` (`frozen<plain>`)
//! and `udt_hashable_shapes.stn`'s `unhashable_fields`, nested in a tuple in a set.
//!
//! ## Independence
//! Deliberately duplicated from the cqlite-core half
//! (`cqlite-core/tests/issue_3629_core_tojson_udt_namespace.rs`) rather than
//! sharing a harness: the two renderers were two copies of the same defect, and a
//! shared test could not catch them diverging again. The two writers also render
//! field VALUES by different rules on purpose (blobs hex here, base64 there), so
//! the expectations genuinely differ.
//!
//! ## Fixtures are COMMITTED ⇒ fail closed (#3220): no env var, no skip path.

use std::path::{Path, PathBuf};

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::JSONWriter;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use serde_json::{json, Value as J};

const KEYSPACE: &str = "test_udt_collision";

// ── Fixture resolution (checkout-relative; committed ⇒ must_run) ────────────

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-cli always has a workspace parent directory")
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
        let has_data = std::fs::read_dir(&dirs[0])
            .unwrap_or_else(|e| panic!("fixture table dir unreadable: {e}"))
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
        assert!(
            has_data,
            "no *-Data.db under {:?} — the binaries are force-added; see \
             test-data/fixtures/issue_3504/README.md",
            dirs[0]
        );
    }
    root
}

/// Every row of `table`, rendered through the PUBLIC CLI surface
/// `JSONWriter::write(&QueryResult, &OutputConfig)`, then re-parsed so individual
/// cells can be asserted.
async fn rows_by_id(table: &str) -> Vec<(i64, J)> {
    let config = IngestionConfig {
        schema_paths: vec![repo_root().join("test-data/schemas/issue-3504-udt-collision.cql")],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };
    let db = ingest(config)
        .await
        .expect("committed fixture must ingest (it is git-tracked source)")
        .database;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{table}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over committed fixture {table} failed: {e}"));

    let rendered = JSONWriter::write(&result, &OutputConfig::default())
        .unwrap_or_else(|e| panic!("JSONWriter::write failed for {table}: {e}"));
    let parsed: J = serde_json::from_str(&rendered)
        .unwrap_or_else(|e| panic!("CLI JSON output for {table} did not parse: {e}"));
    let rows = parsed
        .as_array()
        .unwrap_or_else(|| panic!("CLI --format json emits a bare array; got {parsed}"))
        .clone();
    assert!(
        !rows.is_empty(),
        "committed fixture {table} decoded ZERO rows (0-rows-when-present is a failure)"
    );
    rows.into_iter()
        .map(|r| {
            let id = r
                .get("id")
                .and_then(J::as_i64)
                .unwrap_or_else(|| panic!("row without an integer id in {table}: {r}"));
            (id, r)
        })
        .collect()
}

/// The cell for `column` in the row with primary key `id`. Fail-closed: a missing
/// row or a NULL cell is a decode regression, never a skip.
fn cell(rows: &[(i64, J)], id: i64, column: &str) -> J {
    let row = rows
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    let v = row
        .get(column)
        .unwrap_or_else(|| panic!("row id={id} has no column {column}"));
    assert!(
        !v.is_null(),
        "row id={id} column {column} decoded NULL; the fixture writes a value there"
    );
    v.clone()
}

/// Field names of a rendered UDT object, in emitted order.
fn keys(v: &J) -> Vec<String> {
    v.as_object()
        .unwrap_or_else(|| panic!("expected a rendered UDT object, got {v}"))
        .keys()
        .cloned()
        .collect()
}

// ════════════════════════════════════════════════════════════════════════════
// RED-CAPABLE subjects — these FAIL on unfixed code.
// ════════════════════════════════════════════════════════════════════════════

/// `udt_collide.p` is `frozen<plain>` (`label text, real_field int`): it declares
/// no `_type`, so the injected key is OBSERVABLE. Golden (sstabledump):
/// `{"label": "no-colliding-field", "real_field": 7}` /
/// `{"label": "contrast-row", "real_field": 8}`.
#[tokio::test]
async fn plain_udt_renders_declared_fields_and_nothing_else() {
    let rows = rows_by_id("udt_collide").await;

    assert_eq!(
        cell(&rows, 1, "p"),
        json!({"label": "no-colliding-field", "real_field": 7}),
        "row 1 `p` must equal the sstabledump golden exactly; type identity is \
         not a member of the UDT's field namespace"
    );
    assert_eq!(
        cell(&rows, 2, "p"),
        json!({"label": "contrast-row", "real_field": 8}),
        "row 2 `p` must equal the sstabledump golden exactly"
    );
}

/// The NESTED RED case: `udt_hashable_shapes.stn` is
/// `frozen<set<frozen<tuple<frozen<unhashable_fields>, int>>>>` and
/// `unhashable_fields` (`label text, m frozen<map<text,int>>`) declares no
/// `_type`. Golden: `[[{"label": "unhashable", "m": {"a": 1}}, 30]]`.
///
/// The decode gap this used to characterize is FIXED (issue #3631 instance B): a
/// COLLECTION field inside a FROZEN UDT decoded to `Value::Blob`, so `m` rendered
/// as the CLI's `0x…` hex. It now decodes structurally, so this assert is
/// INVERTED.
///
/// The CLI spells a decoded map as an ARRAY OF `{"key":…,"value":…}` OBJECTS, not
/// as a JSON object — its established rendering for a CQL map, whose keys need not
/// be strings (the golden's `{"a": 1}` is `sstabledump`'s spelling of the same
/// value). That is a RENDERING convention, not a decode question: the decoded
/// `Value` is `Map([(Text("a"), Integer(1))])`, asserted against the golden in
/// `cqlite-core/tests/issue_3631_structured_values_not_blobs.rs`. Pinned exactly
/// here so a change in either layer is visible.
#[tokio::test]
async fn nested_udt_in_tuple_in_set_renders_declared_fields_and_nothing_else() {
    let rows = rows_by_id("udt_hashable_shapes").await;
    let stn = cell(&rows, 3, "stn");

    let tuple = stn
        .get(0)
        .unwrap_or_else(|| panic!("`stn` must be a one-element set, got {stn}"));
    let udt = tuple
        .get(0)
        .unwrap_or_else(|| panic!("`stn` element must be a 2-tuple, got {tuple}"));

    assert_eq!(
        keys(udt),
        vec!["label".to_string(), "m".to_string()],
        "the nested `unhashable_fields` UDT must expose its two declared fields \
         and nothing else (golden: {{\"label\": \"unhashable\", \"m\": ...}})"
    );
    assert_eq!(udt.get("label"), Some(&json!("unhashable")));
    assert_eq!(
        tuple.get(1),
        Some(&json!(30)),
        "the tuple's second element is the golden's 30"
    );
    // Issue #3631 instance B: structural, not `0x…` hex.
    assert_eq!(
        udt.get("m"),
        Some(&json!([{"key": "a", "value": 1}])),
        "a `frozen<map<text,int>>` field of a frozen UDT must render structurally \
         (the CLI's key/value array spelling of a CQL map), not as the hex of its \
         20 serialized bytes (issue #3631 instance B)"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// BLIND subjects — PRESERVATION GUARDS ONLY.
//
// Each PASSES ON UNFIXED CODE, by the total-overwrite mechanism in the module
// header. They are NOT evidence the defect is fixed; they exist so the fix
// cannot damage the columns whose UDTs declare `_type`.
// ════════════════════════════════════════════════════════════════════════════

/// BLIND (preservation only). `udt_collide.c` is `frozen<collide>`, which
/// DECLARES `"_type"`. Row 3's `"_type": null` is the USER's null field and is
/// CORRECT — the fix must not "repair" it into a type name.
#[tokio::test]
async fn nondiscriminating_passes_on_unfixed_code_colliding_udt_keeps_user_fields_verbatim() {
    let rows = rows_by_id("udt_collide").await;

    assert_eq!(
        cell(&rows, 1, "c"),
        json!({
            "_type": "user-supplied-type",
            "_keyspace": "user-supplied-keyspace",
            "__proto__": "user-supplied-proto",
            "real_field": 42
        }),
        "row 1 `c` must equal the sstabledump golden (declaration order included)"
    );
    assert_eq!(
        cell(&rows, 3, "c"),
        json!({
            "_type": J::Null,
            "_keyspace": "keyspace-field-only",
            "__proto__": J::Null,
            "real_field": 0
        }),
        "row 3 `c`: the null `_type` is the USER's data, not a missing marker"
    );
}

/// BLIND (preservation only). `fs frozen<set<frozen<collide>>>` — the
/// collection-member path into the same renderer.
#[tokio::test]
async fn nondiscriminating_passes_on_unfixed_code_udt_inside_frozen_set() {
    let rows = rows_by_id("udt_collide").await;
    assert_eq!(
        cell(&rows, 1, "fs"),
        json!([{
            "_type": "set-member-type",
            "_keyspace": "set-member-keyspace",
            "__proto__": "set-member-proto",
            "real_field": 200
        }]),
        "`fs` must equal the sstabledump golden"
    );
}

/// BLIND (preservation only) — and the MAP-KEY path, which really does reach
/// this renderer in the CLI: the CLI's Map arm emits `{"key": …, "value": …}`, so
/// a `frozen<map<frozen<collide>, int>>` key is a JSON-rendered UDT object.
/// (cqlite-core's Map arm stringifies keys with `Display` instead, so the same
/// columns are a NON-subject there — see the core half.)
#[tokio::test]
async fn nondiscriminating_passes_on_unfixed_code_frozen_map_udt_key_is_json_rendered() {
    let rows = rows_by_id("udt_collide").await;
    let expected_key = json!({
        "_type": "key-type-marker",
        "_keyspace": "key-keyspace-marker",
        "__proto__": "key-proto-marker",
        "real_field": 100
    });
    for (column, value) in [("fcm", 3), ("ftm", 4)] {
        assert_eq!(
            cell(&rows, 1, column),
            json!([{"key": expected_key, "value": value}]),
            "{column} must equal the sstabledump golden's key fields"
        );
    }
}

/// BLIND (preservation only). Table 2's `stu`/`ssu`/`mtu` all nest `collide`,
/// which declares `_type`.
#[tokio::test]
async fn nondiscriminating_passes_on_unfixed_code_shapes_table_collide_nestings() {
    let rows = rows_by_id("udt_hashable_shapes").await;
    let collide = json!({
        "_type": "tuple-type-marker",
        "_keyspace": "tuple-keyspace-marker",
        "__proto__": "tuple-proto-marker",
        "real_field": 300
    });

    assert_eq!(
        cell(&rows, 1, "stu"),
        json!([[collide, 10]]),
        "`stu` must equal the sstabledump golden"
    );
    assert_eq!(
        cell(&rows, 2, "ssu"),
        json!([[collide]]),
        "`ssu` must equal the sstabledump golden"
    );
    assert_eq!(
        cell(&rows, 1, "mtu"),
        json!([{"key": [collide, 20], "value": 5}]),
        "`mtu`'s tuple key must equal the sstabledump golden"
    );
}

/// A SUBJECT SINCE #3612, and it used to be a documented NON-subject.
///
/// `cm`/`tm` are NON-FROZEN `map<frozen<udt>, int>`, so each key lives in the cell
/// PATH. This test asserted that such a key rendered as a `0x…` hex string,
/// because `parse_cell_path_key` matched a closed set of primitive cell-path types
/// and fell back to `Value::Blob` for a UDT — so the key never became a
/// `Value::Udt` and could not reach the UDT renderer. Every clause of that is now
/// false: #3612 made that site
/// (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs`)
/// delegate to the structural decoder.
///
/// So these columns now STRENGTHEN #3629's coverage instead of being excluded
/// from it: the CLI's `Map` arm emits `{"key": …, "value": …}`, and the key is a
/// JSON-rendered UDT, which is a second independent route into the very renderer
/// #3629 fixed — one reached through a MULTICELL cell path rather than a frozen
/// value cell.
///
/// ORACLE: the expectation is NOT derived from what our decoder now emits, which
/// would be circular. It mirrors, field for field, the frozen sibling
/// `nondiscriminating_passes_on_unfixed_code_frozen_map_udt_key_is_json_rendered`
/// above — #3629's own contract for this shape, written for the columns that
/// always did reach the renderer, by someone with no stake in #3612. `cm`/`fcm`
/// and `tm`/`ftm` are the two legal spellings of one map and the fixture stores
/// the same key in both, so mirroring is exactly the right expectation: the map
/// VALUES differ (1/2 vs 3/4) and the KEYS must not.
#[tokio::test]
async fn non_frozen_map_udt_key_is_json_rendered_like_the_frozen_spelling() {
    let rows = rows_by_id("udt_collide").await;
    // Byte-for-byte the frozen sibling's `expected_key`. Declared fields and
    // nothing else — the fields are LITERALLY named `_type`/`_keyspace`/`__proto__`
    // in this fixture, which is the collision #3504/#3629 exist for; no marker is
    // injected ahead of them.
    let expected_key = json!({
        "_type": "key-type-marker",
        "_keyspace": "key-keyspace-marker",
        "__proto__": "key-proto-marker",
        "real_field": 100
    });
    for (column, value) in [("cm", 1), ("tm", 2)] {
        assert_eq!(
            cell(&rows, 1, column),
            json!([{"key": expected_key, "value": value}]),
            "{column}: a multicell UDT map key must JSON-render as its declared \
             fields, exactly as the frozen spelling's does (issues #3612, #3629)"
        );
    }
}
