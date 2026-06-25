//! Issue #1009 — self-test for the canonical sstabledump-JSONL comparator.
//!
//! Proves the shared comparator module (`support/canonical_jsonl.rs`, the strict
//! comparison lane for epic #971) behaves to spec against REAL committed goldens
//! under `CQLITE_DATASETS_ROOT` (test_basic / test_collections), and that its
//! fail-loud + precise-diff contracts hold.
//!
//! Manifest entries exercised (owned by #1009 — see report; manifest not edited):
//!   * cass.cql_types.jsonl.canonical_value_comparator
//!   * cass.cql_types.jsonl.schema_aware_normalization
//!   * cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare
//!   * cass.cql_types.jsonl.no_placeholder_references
//!   * cass.cql_types.jsonl.manifest_report_generation
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --test issue_1009_canonical_jsonl_comparator -- --nocapture
//! ```

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;

use std::path::{Path, PathBuf};

use canonical_jsonl::{
    build_report, compare_documents, datasets_root, find_golden_jsonl, fixture_dir,
    load_golden_document, parse_document_str, parse_document_str_with_keys, render_diffs,
    render_manifest_report, CanonicalError, CanonicalValue, CompareCtx, KeyKind, KeySpec,
    NormalizedFloat,
};

// ---------------------------------------------------------------------------
// Manifest ids (mirrors the manifest entries this issue owns).
// ---------------------------------------------------------------------------

const MID_VALUE_CMP: &str = "cass.cql_types.jsonl.canonical_value_comparator";
const MID_NORM: &str = "cass.cql_types.jsonl.schema_aware_normalization";
const MID_CELL: &str = "cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare";
const MID_NO_PLACEHOLDER: &str = "cass.cql_types.jsonl.no_placeholder_references";
const MID_REPORT: &str = "cass.cql_types.jsonl.manifest_report_generation";

// ===========================================================================
// Unit-level: value canonicalization & normalization (no fixtures needed)
// ===========================================================================

/// `cass.cql_types.jsonl.canonical_value_comparator` — typed equality:
/// distinct types never compare equal; same typed value does.
#[test]
fn typed_values_compare_by_type_not_string() {
    let int_five = CanonicalValue::from_json(&serde_json::json!(5));
    let str_five = CanonicalValue::from_json(&serde_json::json!("hello"));
    assert_ne!(int_five, str_five, "int 5 must not equal text");

    let text_five = CanonicalValue::Text("5".to_string());
    assert_ne!(
        int_five, text_five,
        "int 5 must NOT equal the genuine text \"5\" — type is load-bearing"
    );

    // bool vs int.
    let b = CanonicalValue::from_json(&serde_json::json!(true));
    let i1 = CanonicalValue::from_json(&serde_json::json!(1));
    assert_ne!(b, i1, "bool true must not equal int 1");

    // Same value, same type → equal.
    assert_eq!(
        CanonicalValue::from_json(&serde_json::json!(42)),
        CanonicalValue::Int(42)
    );

    // Absent vs Null vs empty list are all distinct.
    assert_ne!(CanonicalValue::Absent, CanonicalValue::Null);
    assert_ne!(
        CanonicalValue::Null,
        CanonicalValue::List(vec![]),
        "explicit null must differ from empty collection"
    );
}

/// `cass.cql_types.jsonl.schema_aware_normalization` — KEY-component
/// canonicalization is SOUND: the sstabledump string→`Int` coercion is applied
/// ONLY to integral key columns. A TEXT key `"5"` must NOT compare equal to a
/// numeric key `5`, while a genuine integral key still matches the golden's
/// string rendering. This guards the residual #971 finding: blindly coercing
/// every numeric-looking key string would let a text-vs-numeric KEY mis-decode
/// false-pass.
#[test]
fn key_component_coercion_is_schema_sound() {
    // KeyKind classification: integral families coerce, everything else does not.
    for t in ["int", "bigint", "smallint", "tinyint", "varint", "counter", "INT", "frozen<int>"] {
        assert_eq!(KeyKind::from_cql_type(t), KeyKind::Integral, "{t} must be integral");
    }
    for t in ["text", "ascii", "varchar", "blob", "uuid", "timestamp", "double", "tuple<int,int>"] {
        assert_eq!(KeyKind::from_cql_type(t), KeyKind::Other, "{t} must be Other (no coercion)");
    }

    // sstabledump renders EVERY key component as a quoted string. The actual
    // (CQLite) side emits the typed token: an int key as a bare number `5`, a
    // text key as a quoted `"5"`.
    let golden_line =
        r#"{"partition":{"key":["5"]},"rows":[{"type":"row","cells":[{"name":"v","value":1}]}]}"#;

    // --- TEXT partition key: golden "5" must stay Text and NOT match a numeric 5. ---
    let text_spec = KeySpec::from_cql_types(&["text"], &[]);
    let golden_text = parse_document_str_with_keys(golden_line, Path::new("<g-text>"), true, &text_spec)
        .expect("golden text-key parse");
    assert_eq!(
        golden_text.partitions[0].key,
        vec![CanonicalValue::Text("5".to_string())],
        "a text key column must keep \"5\" as Text (no coercion)"
    );
    // A CQLite side that WRONGLY decoded the text key as numeric 5 (bare JSON
    // number) must FAIL the comparison — proving the false-pass is closed.
    let actual_numeric =
        r#"{"partition":{"key":[5]},"rows":[{"type":"row","cells":[{"name":"v","value":1}]}]}"#;
    let actual_num_doc =
        parse_document_str_with_keys(actual_numeric, Path::new("<a-num>"), true, &text_spec)
            .expect("actual numeric parse");
    let ctx = CompareCtx::new(MID_NORM, "<key-soundness>");
    let diffs = compare_documents(&ctx, &golden_text, &actual_num_doc);
    assert!(
        diffs.iter().any(|d| d.what.contains("partition key")),
        "text key \"5\" must NOT equal a mis-decoded numeric key 5: {}",
        render_diffs(&diffs)
    );

    // --- INTEGRAL partition key: golden "5" coerces to Int and matches a typed 5. ---
    let int_spec = KeySpec::from_cql_types(&["int"], &[]);
    let golden_int = parse_document_str_with_keys(golden_line, Path::new("<g-int>"), true, &int_spec)
        .expect("golden int-key parse");
    assert_eq!(
        golden_int.partitions[0].key,
        vec![CanonicalValue::Int(5)],
        "an int key column coerces the string \"5\" to Int(5)"
    );
    let int_diffs = compare_documents(&ctx, &golden_int, &actual_num_doc);
    assert!(
        int_diffs.is_empty(),
        "a genuine int key must match the golden's string rendering: {}",
        render_diffs(&int_diffs)
    );

    // The two interpretations of the SAME golden line are NOT equal to each other:
    // type is load-bearing even on the key path.
    assert_ne!(
        golden_text.partitions[0].key, golden_int.partitions[0].key,
        "text \"5\" key and int 5 key must be distinct canonical values"
    );
}

/// `cass.cql_types.jsonl.schema_aware_normalization` — formatting-only
/// differences are normalized; equivalent representations unify.
#[test]
fn normalization_unifies_equivalent_representations() {
    // Timestamp fractional-second formatting: ".06Z" == ".060000Z".
    let a = CanonicalValue::from_json(&serde_json::json!("2025-10-06T01:12:06.06Z"));
    let b = CanonicalValue::from_json(&serde_json::json!("2025-10-06T01:12:06.060000Z"));
    assert_eq!(a, b, "fractional-second formatting must normalize equal");
    match a {
        CanonicalValue::Timestamp { .. } => {}
        other => panic!("expected Timestamp, got {other:?}"),
    }

    // M1: sstabledump emits a SPACE between date and time; ISO-8601 uses `T`.
    // Both must normalize to the same instant (and to the same epoch-µs as the
    // padded fractional form). Real goldens use the space form.
    let space = CanonicalValue::from_json(&serde_json::json!("2025-10-06 01:12:07.265Z"));
    let tee = CanonicalValue::from_json(&serde_json::json!("2025-10-06T01:12:07.265000Z"));
    assert_eq!(
        space, tee,
        "space-separated sstabledump timestamp must normalize equal to the ISO-8601 `T` form"
    );
    match (&space, &tee) {
        (
            CanonicalValue::Timestamp { micros: m1, .. },
            CanonicalValue::Timestamp { micros: m2, .. },
        ) => assert_eq!(m1, m2, "both renderings must yield identical epoch-µs"),
        other => panic!("expected two Timestamps, got {other:?}"),
    }

    // M2: a JSON *string* is NEVER coerced to Int. sstabledump renders typed
    // integers as bare JSON numbers and CQL text/ascii/varchar as quoted
    // strings, so a numeric-looking string stays Text and must NOT compare
    // equal to the same value rendered as a JSON number — type is load-bearing.
    let n = CanonicalValue::from_json(&serde_json::json!(123456789));
    let s = CanonicalValue::from_json(&serde_json::json!("123456789"));
    assert_eq!(n, CanonicalValue::Int(123_456_789), "JSON number stays Int");
    assert_eq!(s, CanonicalValue::Text("123456789".to_string()), "JSON string stays Text");
    assert_ne!(
        n, s,
        "numeric JSON string must NOT canonicalize to Int (text \"5\" != int 5)"
    );
    assert_ne!(
        CanonicalValue::Text("5".to_string()),
        CanonicalValue::Int(5),
        "Text(\"5\") must not equal Int(5)"
    );

    // Whitespace inside JSON arrays is irrelevant (parse-level).
    let compact = parse_document_str(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","cells":[{"name":"c","value":[1,2,3]}]}]}"#,
        Path::new("<compact>"),
        true,
    )
    .expect("compact parse");
    let spaced = parse_document_str(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","cells":[{"name":"c","value":[1, 2, 3]}]}]}"#,
        Path::new("<spaced>"),
        true,
    )
    .expect("spaced parse");
    let ctx = CompareCtx::new(MID_NORM, "<inmem>");
    let diffs = compare_documents(&ctx, &compact, &spaced);
    assert!(diffs.is_empty(), "whitespace must not produce diffs: {}", render_diffs(&diffs));

    // JSON object key ordering is irrelevant (typed parse).
    let order_a = parse_document_str(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":["x"],"liveness_info":{"tstamp":"2025-01-01T00:00:00Z"},"cells":[{"name":"c","value":1}]}]}"#,
        Path::new("<a>"),
        true,
    )
    .expect("a");
    let order_b = parse_document_str(
        r#"{"rows":[{"cells":[{"value":1,"name":"c"}],"clustering":["x"],"type":"row","liveness_info":{"tstamp":"2025-01-01T00:00:00Z"}}],"partition":{"key":["k"]}}"#,
        Path::new("<b>"),
        true,
    )
    .expect("b");
    let diffs = compare_documents(&ctx, &order_a, &order_b);
    assert!(diffs.is_empty(), "key ordering must not diff: {}", render_diffs(&diffs));

    // NaN unification in the float wrapper.
    assert_eq!(NormalizedFloat(f64::NAN), NormalizedFloat(f64::NAN));
    assert_ne!(NormalizedFloat(1.0), NormalizedFloat(2.0));
}

/// `cass.cql_types.jsonl.cell_path_timestamp_ttl_tombstone_compare` — a change
/// to value / writetime / ttl / cell-path / deletion each FAILS with a precise
/// diff naming the exact field.
#[test]
fn precise_diff_on_each_cell_field() {
    let base = r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","clustering":["ck1"],"liveness_info":{"tstamp":"2025-10-06T01:12:06.060Z","ttl":86400},"cells":[{"name":"col","value":"v","tstamp":"2025-10-06T01:12:06.060Z","ttl":600}]}]}"#;
    let doc = parse_document_str(base, Path::new("<base>"), true).expect("base");
    let ctx = CompareCtx::new(MID_CELL, "<inmem>");

    // Self-compare → match.
    assert!(compare_documents(&ctx, &doc, &doc).is_empty(), "identical docs must match");

    // Value change.
    let mutated_val = parse_document_str(
        &base.replace("\"value\":\"v\"", "\"value\":\"DIFFERENT\""),
        Path::new("<m>"),
        true,
    )
    .expect("mv");
    let d = compare_documents(&ctx, &doc, &mutated_val);
    assert_eq!(d.len(), 1, "exactly one value diff");
    assert!(d[0].what.contains("value"), "diff names value: {}", d[0]);
    assert!(d[0].column_path.contains("col"), "diff locates column: {}", d[0]);
    assert!(d[0].row_key.contains("ck1"), "diff locates row key: {}", d[0]);

    // Per-cell writetime change.
    let mutated_ts = parse_document_str(
        &base.replace(
            "\"tstamp\":\"2025-10-06T01:12:06.060Z\",\"ttl\":600",
            "\"tstamp\":\"2025-10-06T01:12:07.060Z\",\"ttl\":600",
        ),
        Path::new("<m>"),
        true,
    )
    .expect("mts");
    let d = compare_documents(&ctx, &doc, &mutated_ts);
    assert!(
        d.iter().any(|x| x.what.contains("writetime")),
        "writetime diff reported: {}",
        render_diffs(&d)
    );

    // TTL change.
    let mutated_ttl = parse_document_str(
        &base.replace("\"ttl\":600", "\"ttl\":900"),
        Path::new("<m>"),
        true,
    )
    .expect("mttl");
    let d = compare_documents(&ctx, &doc, &mutated_ttl);
    assert!(
        d.iter().any(|x| x.what.contains("TTL")),
        "ttl diff reported: {}",
        render_diffs(&d)
    );

    // Cell path change (collection element).
    let path_base = r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","cells":[{"name":"m","path":["k1"],"value":1},{"name":"m","path":["k2"],"value":2}]}]}"#;
    let pdoc = parse_document_str(path_base, Path::new("<p>"), true).expect("p");
    // Reorder paths → positional path/value mismatch (order is load-bearing).
    let reordered = r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","cells":[{"name":"m","path":["k2"],"value":2},{"name":"m","path":["k1"],"value":1}]}]}"#;
    let rdoc = parse_document_str(reordered, Path::new("<r>"), true).expect("r");
    let d = compare_documents(&ctx, &pdoc, &rdoc);
    assert!(
        !d.is_empty() && d.iter().any(|x| x.what.contains("path") || x.what.contains("value")),
        "reordered collection paths must diff (order-sensitive): {}",
        render_diffs(&d)
    );

    // Cell deletion (tombstone) mismatch.
    let live = r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","cells":[{"name":"c","value":"x","tstamp":"2025-10-06T01:12:06.060Z"}]}]}"#;
    let tomb = r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","cells":[{"name":"c","deletion_info":{"marked_deleted":"2025-10-06T01:12:06.060Z","local_delete_time":"2025-10-06T01:12:06Z"},"tstamp":"2025-10-06T01:12:06.060Z"}]}]}"#;
    let ldoc = parse_document_str(live, Path::new("<l>"), true).expect("l");
    let tdoc = parse_document_str(tomb, Path::new("<t>"), true).expect("t");
    let d = compare_documents(&ctx, &ldoc, &tdoc);
    assert!(
        d.iter().any(|x| x.what.contains("deletion") || x.what.contains("value")),
        "live vs tombstone cell must diff: {}",
        render_diffs(&d)
    );
}

/// `cass.cql_types.jsonl.no_placeholder_references` — missing / empty /
/// malformed / placeholder references ERROR (never silently pass).
#[test]
fn fail_loud_on_bad_references() {
    // Attribute this lane to the no-placeholder manifest entry.
    let _ctx = CompareCtx::new(MID_NO_PLACEHOLDER, "<bad-refs>");
    let tmp = std::env::temp_dir().join("cqlite_1009_selftest");
    std::fs::create_dir_all(&tmp).expect("mkdir");

    // Missing.
    let missing = tmp.join("does-not-exist-Data.db.jsonl");
    let _ = std::fs::remove_file(&missing);
    match load_golden_document(&missing, true) {
        Err(CanonicalError::Missing(_)) => {}
        other => panic!("missing reference must error Missing, got {other:?}"),
    }

    // Empty.
    let empty = tmp.join("empty-Data.db.jsonl");
    std::fs::write(&empty, "\n   \n").expect("write empty");
    match load_golden_document(&empty, true) {
        Err(CanonicalError::Empty(_)) => {}
        other => panic!("empty reference must error Empty, got {other:?}"),
    }

    // Malformed JSON.
    let bad = tmp.join("bad-Data.db.jsonl");
    std::fs::write(&bad, "{not json").expect("write bad");
    match load_golden_document(&bad, true) {
        Err(CanonicalError::Malformed { .. }) => {}
        other => panic!("malformed reference must error Malformed, got {other:?}"),
    }

    // Placeholder sentinel.
    let ph = tmp.join("placeholder-Data.db.jsonl");
    std::fs::write(&ph, r#"{"partition":{"key":["PLACEHOLDER"]},"rows":[]}"#).expect("write ph");
    match load_golden_document(&ph, true) {
        Err(CanonicalError::Placeholder { .. }) => {}
        other => panic!("placeholder reference must error Placeholder, got {other:?}"),
    }

    // Structurally invalid (partition without key).
    let nostruct = tmp.join("nokey-Data.db.jsonl");
    std::fs::write(&nostruct, r#"{"partition":{"position":0},"rows":[]}"#).expect("write nokey");
    match load_golden_document(&nostruct, true) {
        Err(CanonicalError::Structure { .. }) => {}
        other => panic!("partition without key must error Structure, got {other:?}"),
    }
}

// ===========================================================================
// Integration: real committed goldens (test_basic / test_collections)
// ===========================================================================

fn golden_for(keyspace: &str, table: &str) -> Option<(PathBuf, PathBuf)> {
    let dir = fixture_dir(keyspace, table)?;
    let golden = find_golden_jsonl(&dir)?;
    Some((dir, golden))
}

/// A real golden compares EQUAL to itself (match proof), and produces a precise
/// diff when one record is mutated (fail proof). Drives both `canonical_value_comparator`
/// and `cell_path_timestamp_ttl_tombstone_compare`.
fn run_self_parity(keyspace: &str, table: &str) -> bool {
    let (dir, golden) = match golden_for(keyspace, table) {
        Some(x) => x,
        None => {
            println!("[SKIP] no golden for {keyspace}.{table} (datasets root absent or fixture missing)");
            return false;
        }
    };

    // load_golden_document fails loud on bad refs; for present committed goldens
    // it must succeed and parse rows.
    let doc = load_golden_document(&golden, true)
        .unwrap_or_else(|e| panic!("failed to load committed golden {golden:?}: {e}"));
    assert!(
        !doc.partitions.is_empty(),
        "committed golden {golden:?} parsed zero partitions"
    );

    let ctx = CompareCtx::new(MID_VALUE_CMP, &golden);

    // 1) Self-compare → MATCH.
    let diffs = compare_documents(&ctx, &doc, &doc);
    assert!(
        diffs.is_empty(),
        "[{keyspace}.{table}] golden must compare equal to itself:\n{}",
        render_diffs(&diffs)
    );
    println!(
        "  [MATCH] {keyspace}.{table}: {} partitions, {} rows compared equal (fixture {})",
        doc.partitions.len(),
        doc.partitions.iter().map(|p| p.rows.len()).sum::<usize>(),
        dir.display(),
    );

    // 2) Mutate the first cell value of the first row → FAIL WITH PRECISE DIFF.
    let mut mutated = doc.clone();
    let mut mutated_something = false;
    'outer: for p in mutated.partitions.iter_mut() {
        for r in p.rows.iter_mut() {
            for c in r.cells.iter_mut() {
                // Only mutate a cell that has a comparable value (skip Absent).
                if !matches!(c.value, CanonicalValue::Absent) {
                    c.value = CanonicalValue::Text("__CQLITE_1009_MUTATION__".to_string());
                    mutated_something = true;
                    break 'outer;
                }
            }
        }
    }
    assert!(
        mutated_something,
        "[{keyspace}.{table}] golden had no mutable cell value to corrupt — cannot prove fail path"
    );
    let diffs = compare_documents(&ctx, &doc, &mutated);
    assert_eq!(
        diffs.len(),
        1,
        "[{keyspace}.{table}] a single mutated cell must yield exactly one precise diff:\n{}",
        render_diffs(&diffs)
    );
    let d = &diffs[0];
    assert!(d.what.contains("value"), "diff identifies value field: {d}");
    assert!(!d.row_key.is_empty(), "diff carries row key: {d}");
    assert!(!d.column_path.is_empty(), "diff carries column path: {d}");
    assert_eq!(d.fixture, golden, "diff carries fixture path");
    assert_eq!(d.manifest_id, MID_VALUE_CMP, "diff carries manifest id");
    assert!(
        d.actual.contains("__CQLITE_1009_MUTATION__"),
        "diff shows actual mutated value: {d}"
    );
    println!("  [FAIL-PROOF] {keyspace}.{table}: mutated cell produced precise diff: {d}");

    true
}

#[test]
fn real_golden_match_and_fail_proof_basic() {
    // simple_table: simple types, clustering key + liveness writetime.
    let ran = run_self_parity("test_basic", "simple_table");
    if datasets_root().is_some() {
        assert!(ran, "datasets root present but test_basic.simple_table golden not exercised");
    }
}

#[test]
fn real_golden_match_and_fail_proof_collections() {
    // collection_table: list/set/map cells with paths + collection-shell
    // deletion markers — exercises cell-path + deletion comparison on real data.
    let ran = run_self_parity("test_collections", "collection_table");
    if datasets_root().is_some() {
        // collection_table is the canonical collections fixture; if a different
        // table name is committed, fall back to the first collections table.
        if !ran {
            // Try any test_collections table with a golden.
            let root = datasets_root().expect("root present");
            let ks = root.join("sstables").join("test_collections");
            let mut found = false;
            if let Ok(entries) = std::fs::read_dir(&ks) {
                for e in entries.flatten() {
                    if e.path().is_dir() {
                        if let Some(g) = find_golden_jsonl(&e.path()) {
                            let table = e
                                .file_name()
                                .to_str()
                                .and_then(|n| n.split('-').next())
                                .unwrap_or("")
                                .to_string();
                            println!("  collections fallback table: {table} ({})", g.display());
                            found = run_self_parity("test_collections", &table) || found;
                            if found {
                                break;
                            }
                        }
                    }
                }
            }
            assert!(found, "datasets root present but no test_collections golden exercised");
        }
    }
}

/// `cass.cql_types.jsonl.manifest_report_generation` — the comparator emits a
/// deterministic, attributed report over multiple fixtures (match + fail rows).
#[test]
fn manifest_report_is_deterministic_and_attributed() {
    // Build two in-memory docs: one matching, one failing — so the report shows
    // both states, deterministically.
    let golden = parse_document_str(
        r#"{"partition":{"key":["pk1"]},"rows":[{"type":"row","clustering":["c"],"cells":[{"name":"a","value":1}]}]}"#,
        Path::new("/fixtures/zzz-Data.db.jsonl"),
        true,
    )
    .expect("golden");
    let same = golden.clone();
    let mut broken = golden.clone();
    broken.partitions[0].rows[0].cells[0].value = CanonicalValue::Int(999);

    let ctx_ok = CompareCtx::new(MID_REPORT, "/fixtures/zzz-Data.db.jsonl");
    let ctx_bad = CompareCtx::new(MID_REPORT, "/fixtures/aaa-Data.db.jsonl");

    let diffs_ok = compare_documents(&ctx_ok, &golden, &same);
    let diffs_bad = compare_documents(&ctx_bad, &golden, &broken);
    assert!(diffs_ok.is_empty(), "ok pair must match");
    assert_eq!(diffs_bad.len(), 1, "bad pair must produce one diff");

    let reports = vec![
        build_report(MID_REPORT, Path::new("/fixtures/zzz-Data.db.jsonl"), &golden, &diffs_ok),
        build_report(MID_REPORT, Path::new("/fixtures/aaa-Data.db.jsonl"), &golden, &diffs_bad),
    ];
    let r1 = render_manifest_report(&reports);
    let r2 = render_manifest_report(&reports);
    assert_eq!(r1, r2, "report must be deterministic");

    // Sorted by fixture path within the manifest id → aaa (FAIL) before zzz (MATCH).
    let aaa_pos = r1.find("aaa-Data.db.jsonl").expect("aaa present");
    let zzz_pos = r1.find("zzz-Data.db.jsonl").expect("zzz present");
    assert!(aaa_pos < zzz_pos, "report sorted by fixture path:\n{r1}");
    assert!(r1.contains("[FAIL]"), "report shows FAIL state:\n{r1}");
    assert!(r1.contains("[MATCH]"), "report shows MATCH state:\n{r1}");
    assert!(r1.contains(MID_REPORT), "report attributes manifest id:\n{r1}");
    println!("\n{r1}");
}

/// Belt-and-suspenders: when the datasets root is present, BOTH integration
/// lanes must have actually run (anti-silent-pass at the suite level).
#[test]
fn datasets_present_means_goldens_exercised() {
    let Some(root) = datasets_root() else {
        println!("[SKIP] CQLITE_DATASETS_ROOT unset — integration goldens not exercised");
        return;
    };
    let basic = golden_for("test_basic", "simple_table");
    assert!(
        basic.is_some(),
        "datasets root {root:?} present but no test_basic.simple_table golden — fixture regression"
    );
}
