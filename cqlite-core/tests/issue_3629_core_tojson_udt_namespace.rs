//! Issue #3629 (oracle-driven) — cqlite-core's `ToJson for Value` must render a
//! UDT as its DECLARED FIELDS AND NOTHING ELSE.
//!
//! ## The defect
//! `impl ToJson for Value`'s `Value::Udt` arm used to `insert("_type", type_name)`
//! into the SAME `serde_json` object that then receives the UDT's own declared
//! fields. Type identity and user data therefore shared one channel (the #3504
//! parent class): a UDT that DECLARES a field named `_type` — legal CQL via a
//! quoted identifier — silently OVERWROTE the injected marker, and every UDT that
//! does NOT declare one carried a key Cassandra never wrote.
//!
//! ## Oracle
//! PRIMARY SOURCE:
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java:261`
//! (`toJSONString`) iterates `types.size()` over `stringFieldNames` only and emits
//! NO type key and NO keyspace key, appending the literal `null` for an absent
//! field buffer (line 280) — which is also why row 3's `"_type": null` below is
//! CORRECT output for a null FIELD rather than a leftover marker.
//!
//! The committed Cassandra-written fixture `test-data/fixtures/issue_3504/` and
//! its `sstabledump` JSONL golden. sstabledump injects NOTHING: the non-colliding
//! `p` cell dumps as `{"label": ..., "real_field": 7}`. That is the target shape.
//! Every expectation below is quoted from that golden, never from CQLite's
//! previous output.
//!
//! ## WHY THE GOLDEN IS BLIND FOR MOST COLUMNS (read before adding a case)
//! For the `collide`/`collide_twin` columns the injected `_type` is inserted
//! FIRST and the user's own `_type` field then overwrites it. `serde_json` is
//! built with `preserve_order` and `Map::insert` on an existing key KEEPS the
//! original position, and `collide` declares `"_type"` first — so the buggy
//! output is TEXTUALLY IDENTICAL to the golden, key order included. A test over
//! those columns PASSES ON UNFIXED CODE and proves nothing about this fix. They
//! are kept below as explicitly LABELLED preservation guards (the fix must not
//! disturb them), never as evidence.
//!
//! The RED-capable subjects are the two UDTs that declare no `_type` field:
//! `udt_collide.p` (`frozen<plain>`) and `udt_hashable_shapes.stn`'s
//! `unhashable_fields` (a UDT nested in a tuple in a set).
//!
//! ## Independence
//! This file deliberately duplicates the CLI half
//! (`cqlite-cli/tests/issue_3629_cli_udt_json_namespace.rs`) instead of sharing a
//! harness: the two renderers are two copies of the defect, and a shared test
//! could not catch them diverging again.
//!
//! ## Fixtures are COMMITTED ⇒ fail closed (#3220)
//! No `CQLITE_DATASETS_ROOT`, no skip path: absence is a broken checkout.
#![cfg(feature = "cli-helpers")]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;
use serde_json::{json, Value as J};

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
        let data: Vec<PathBuf> = std::fs::read_dir(&dirs[0])
            .unwrap_or_else(|e| panic!("fixture table dir unreadable: {e}"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .collect();
        assert!(
            !data.is_empty(),
            "no *-Data.db under {:?} — the binaries are force-added; see \
             test-data/fixtures/issue_3504/README.md",
            dirs[0]
        );
    }
    root
}

async fn open_db() -> Database {
    let root = repo_root();
    let config = IngestionConfig {
        schema_paths: vec![root.join("test-data/schemas/issue-3504-udt-collision.cql")],
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

/// Every row of `table`, keyed by `id`, rendered through the PUBLIC
/// `QueryResult::to_json()` (the `ToJson for Value` surface under test).
async fn rows_by_id(table: &str) -> Vec<(i64, J)> {
    let db = open_db().await;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{table}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over committed fixture {table} failed: {e}"));
    let json = result.to_json();
    let rows = json
        .get("rows")
        .and_then(J::as_array)
        .unwrap_or_else(|| panic!("QueryResult::to_json() has no rows array for {table}"));
    assert!(
        !rows.is_empty(),
        "committed fixture {table} decoded ZERO rows (0-rows-when-present is a failure)"
    );
    rows.iter()
        .map(|r| {
            let id = r
                .get("id")
                .and_then(J::as_i64)
                .unwrap_or_else(|| panic!("row without an integer id in {table}: {r}"));
            (id, r.clone())
        })
        .collect()
}

/// The cell for `column` in the row with primary key `id`. Fail-closed: a
/// missing row or a NULL cell is a decode regression, never a skip.
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

/// `udt_collide.p` is `frozen<plain>` (`label text, real_field int`) — it
/// declares no `_type`, so the injected key is OBSERVABLE. Golden (sstabledump):
/// `{"label": "no-colliding-field", "real_field": 7}` /
/// `{"label": "contrast-row", "real_field": 8}`.
#[tokio::test]
async fn plain_udt_renders_declared_fields_and_nothing_else() {
    let rows = rows_by_id("udt_collide").await;

    let p1 = cell(&rows, 1, "p");
    assert_eq!(
        p1,
        json!({"label": "no-colliding-field", "real_field": 7}),
        "row 1 `p` must equal the sstabledump golden exactly; an injected \
         type-identity key is not part of the UDT's field namespace"
    );

    let p2 = cell(&rows, 2, "p");
    assert_eq!(
        p2,
        json!({"label": "contrast-row", "real_field": 8}),
        "row 2 `p` must equal the sstabledump golden exactly"
    );
}

/// The NESTED RED case: `udt_hashable_shapes.stn` is
/// `frozen<set<frozen<tuple<frozen<unhashable_fields>, int>>>>`, and
/// `unhashable_fields` (`label text, m frozen<map<text,int>>`) declares no
/// `_type`. Golden: `[[{"label": "unhashable", "m": {"a": 1}}, 30]]`.
///
/// MEASURED DECODE GAP, orthogonal to #3629: a COLLECTION field inside a FROZEN
/// UDT decodes to `Value::Blob`, so `m` renders as base64 rather than the
/// golden's `{"a": 1}` (the same gap the Python suite pins as characterization —
/// see the fixture schema's `unhashable_fields` note). The property under test
/// here is the FIELD NAMESPACE, so `m`'s value is asserted as the measured blob
/// and labelled, not silently golden-matched.
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
    // Characterization of the orthogonal decode gap (NOT the #3629 property):
    // the golden's `{"a": 1}` arrives as the frozen map's serialized bytes.
    assert!(
        udt.get("m").is_some_and(J::is_string),
        "known gap: a collection field inside a frozen UDT decodes to a blob; \
         got {:?}",
        udt.get("m")
    );
}

// ════════════════════════════════════════════════════════════════════════════
// BLIND subjects — PRESERVATION GUARDS ONLY.
//
// Each of these PASSES ON UNFIXED CODE, by the total-overwrite mechanism
// described in the module header. They are NOT evidence the defect is fixed;
// they exist so the fix cannot damage the columns whose UDTs declare `_type`.
// ════════════════════════════════════════════════════════════════════════════

/// BLIND (preservation only). `udt_collide.c` is `frozen<collide>`, which
/// DECLARES `"_type"`; the user's value wins on unfixed and fixed code alike.
/// Row 3's `"_type": null` is the USER's null field and is CORRECT — the fix
/// must not "repair" it into a type name.
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

/// BLIND (preservation only), and a MEASURED NON-SUBJECT for this renderer:
/// `fcm`/`ftm` are `frozen<map<frozen<collide>, int>>`, and cqlite-core's `Map`
/// arm stringifies each key with `Display` (`format!("{}", k)`) — so the UDT KEY
/// never reaches the `Value::Udt` JSON arm at all. Recorded so nobody counts
/// these columns as coverage of the fix on the core side. (They DO reach the
/// renderer in the CLI, whose Map arm emits `{"key": …, "value": …}` — asserted
/// in the CLI half.)
#[tokio::test]
async fn nondiscriminating_passes_on_unfixed_code_frozen_map_udt_key_is_display_stringified() {
    let rows = rows_by_id("udt_collide").await;
    for (column, type_name) in [("fcm", "collide"), ("ftm", "collide_twin")] {
        let m = cell(&rows, 1, column);
        let obj = m
            .as_object()
            .unwrap_or_else(|| panic!("{column} must render as a JSON object, got {m}"));
        let (key, _) = obj
            .iter()
            .next()
            .unwrap_or_else(|| panic!("{column} rendered no entries"));
        assert!(
            key.starts_with(&format!("{type_name}{{")),
            "{column}'s UDT key is Display-stringified by the core Map arm, not \
             JSON-rendered; got {key}"
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
    // `mtu` is a frozen map: its tuple key is Display-stringified by the core Map
    // arm (same non-subject note as `fcm`/`ftm` above).
    let mtu = cell(&rows, 1, "mtu");
    let obj = mtu
        .as_object()
        .unwrap_or_else(|| panic!("`mtu` must render as a JSON object, got {mtu}"));
    assert_eq!(obj.len(), 1, "`mtu` has one golden entry");
}

/// STILL A MEASURED NON-SUBJECT for THIS renderer — but the ORIGINAL REASON IS
/// NOW FALSE, and that distinction is the whole point of this docstring.
///
/// It used to read: `parse_cell_path_key` falls back to `Value::Blob` for a UDT,
/// such a key never becomes a `Value::Udt`, so these columns cannot exercise the
/// UDT renderer. #3612 falsified every clause — that site
/// (`row_decoder/complex_column/cell_path_key.rs`) now delegates to the structural
/// decoder, so `cm`/`tm` keys ARE `Value::Udt`, and the assertion below changed
/// from `BLOB(` to the UDT's `Display` form to prove it.
///
/// The CONCLUSION nevertheless survives, for the reason its own frozen siblings
/// already carry: `cqlite-core`'s `Map` arm stringifies every key with `Display`
/// (`format!("{}", k)`), so a map key of ANY type never reaches the `Value::Udt`
/// JSON arm on this side. These columns therefore still must not be counted as
/// coverage of #3629 HERE — exactly like `fcm`/`ftm` in
/// `nondiscriminating_passes_on_unfixed_code_frozen_map_udt_key_is_display_stringified`
/// above. They DO reach the renderer in the CLI, whose `Map` arm emits
/// `{"key": …, "value": …}`; that is asserted in the CLI half, which is where
/// #3612 turned these columns into a genuine subject.
///
/// ORACLE: the expected shape is NOT taken from what our decoder now emits, which
/// would be circular. It mirrors that frozen sibling's `format!("{type_name}{{")`
/// — #3629's own contract for a Display-stringified UDT key, written for columns
/// with no stake in #3612. A `Value::Blob` renders `BLOB(…)`, so this assertion
/// still discriminates: it fails if the cell-path fallback ever returns.
#[tokio::test]
async fn non_frozen_map_udt_key_is_display_stringified_like_the_frozen_spelling() {
    let rows = rows_by_id("udt_collide").await;
    for (column, type_name) in [("cm", "collide"), ("tm", "collide_twin")] {
        let m = cell(&rows, 1, column);
        let obj = m
            .as_object()
            .unwrap_or_else(|| panic!("{column} must render as a JSON object, got {m}"));
        let (key, _) = obj
            .iter()
            .next()
            .unwrap_or_else(|| panic!("{column} rendered no entries"));
        assert!(
            key.starts_with(&format!("{type_name}{{")),
            "{column}'s key must be a structured UDT that the core Map arm \
             Display-stringifies as `{type_name}{{…}}` — a `BLOB(…)` here means the \
             cell-path Blob fallback has returned (issues #3612, #3629); got {key}"
        );
    }
}
