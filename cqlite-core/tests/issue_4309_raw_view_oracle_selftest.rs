//! Issue #4309 — SELF-TEST of the parity sweep's GOLDEN MODEL.
//!
//! Sibling of `issue_4309_raw_view_census_selftest.rs`, which covers the
//! coverage CENSUS; the shared synthetic goldens live in
//! `support/raw_view_synthetic.rs`, whose module doc explains the split.
//! This lane owns the oracle: complex columns, TTL, `classify_columns`,
//! generation selection, and the fail-closed refusals.
//!
//! # What these control, and why none of it runs on the gate otherwise
//!
//! COMPLEX COLUMNS. `test_deltas.collection_ops` is the only table in the
//! whole sweep with a collection column (`test-data/schemas/deltas.cql:119`
//! — `SET`/`LIST`/`MAP`; the other four sweep schemas have zero), and it is
//! `Discipline::FetchOnly`. So `fold_complex_column`, the
//! `_complex_deletion` exclusion in `classify_columns` that its own comment
//! calls "load-bearing", and the `Fact::Bool(false)` census exclusion never
//! execute under `core-tests`.
//!
//! TTL. `cell_ttl`, `row_ttl` and `row_liveness_expires_at` are claimed only
//! by `ttl_cells` and `gc_before_boundary`, both `FetchOnly` — and the only
//! committed JSONL goldens carrying a `"ttl"` field belong to those two,
//! neither of which ships a `Data.db`. The module doc of `raw_view_parity`
//! flags TTL inheritance as the harness's riskiest assumption.
//!
//! GENERATION SELECTION. The `dirs[0]` pin in `load_goldens` is what keeps
//! oracle and query on the same bytes, and its multi-directory branch
//! executes on NO corpus: every committed table has exactly one
//! `Data.db`-bearing directory. These build the two-directory layout on disk.
//!
//! FAIL-CLOSED REFUSALS. Five refusals exist so a fixture the harness cannot
//! model FAILS instead of being silently skipped. Each is reachable only
//! from a corpus fixture that does not exist today. A refusal that has never
//! fired is a refusal nobody has checked still fires.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_synthetic.rs"]
mod synthetic;

use cqlite_core::query::result::ColumnInfo;
use cqlite_core::types::Value;
use raw_view_parity::golden::{
    build_expectations, classify_columns, load_goldens, Fact, DECLARED_GAP_COLUMNS,
    PARTITION_METADATA, RANGE_METADATA, ROW_LEVEL_METADATA,
};
use raw_view_parity::golden::{fact_of, render_actual_clustering};
use raw_view_parity::{
    is_negative_complex_marker, partition_key_predicate, resolve_root, Discipline, FixtureSpec,
};
use serde_json::json;
use std::collections::BTreeMap;
use synthetic::raw_view_parity;
use synthetic::{census, generation_of, roles, roles_full, roles_with, SPEC};

// ---------------------------------------------------------------------------
// The COMPLEX-COLUMN oracle, which no gate-executed fixture reaches (job 50)
// ---------------------------------------------------------------------------
//
// `test_deltas.collection_ops` is the only table in the sweep with a
// collection column, and it is `FetchOnly` — so `fold_complex_column` and
// the `Fact::Bool(false)` census exclusion never run under the gate's
// corpus-less `core-tests`. These controls need no corpus, so they do.

/// One collection element cell, as `JsonTransformer` renders it: a cell
/// carrying a `path` into the collection.
fn element_cell(name: &str, path: &str) -> serde_json::Value {
    json!({
        "name": name,
        "path": [path],
        "value": "v",
        "tstamp": "2021-01-01T00:00:00Z"
    })
}

/// The COMPLEX-DELETION MARKER: a cell for the collection column carrying
/// NO `path` and a `deletion_info` — what Cassandra writes for the
/// shadowing marker a full collection overwrite emits.
fn complex_marker(name: &str) -> serde_json::Value {
    json!({
        "name": name,
        "deletion_info": {
            "marked_deleted": "2021-01-01T00:00:00Z",
            "local_delete_time": "2021-01-01T00:00:00Z"
        }
    })
}

fn facts_for(cells: Vec<serde_json::Value>) -> BTreeMap<String, raw_view_parity::golden::Fact> {
    let (expected, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "row",
                    "clustering": ["10"],
                    "liveness_info": { "tstamp": "2021-01-01T00:00:00Z" },
                    "cells": cells
                }]
            })],
        )],
        &roles_full(&["ck"], &[], &["tags"]),
        &SPEC,
    );
    assert_eq!(expected.len(), 1, "one synthetic row");
    expected.into_iter().next().unwrap().facts
}

/// (a) A pathless marker present: the column reports the marker AND both of
/// its deletion times.
#[test]
fn a_complex_deletion_marker_yields_all_three_facts() {
    let facts = facts_for(vec![complex_marker("tags"), element_cell("tags", "red")]);
    assert_eq!(
        facts.get("tags_complex_deletion"),
        Some(&Fact::Bool(true)),
        "{facts:?}"
    );
    assert_eq!(
        facts.get("tags_complex_deletion_timestamp"),
        Some(&Fact::BigInt(1609459200000000)),
        "{facts:?}"
    );
    assert_eq!(
        facts.get("tags_complex_deletion_time"),
        Some(&Fact::BigInt(1609459200)),
        "{facts:?}"
    );
}

/// (b) Per-element cells ONLY, no marker: the column reports
/// `Fact::Bool(false)` — the ABSENCE of a marker — and states no deletion
/// times at all. A fabricated time here would be an invented fact (#28).
#[test]
fn elements_without_a_marker_report_a_negative_and_no_times() {
    let facts = facts_for(vec![
        element_cell("tags", "red"),
        element_cell("tags", "blue"),
    ]);
    assert_eq!(
        facts.get("tags_complex_deletion"),
        Some(&Fact::Bool(false)),
        "elements present but no pathless marker: {facts:?}"
    );
    assert_eq!(
        facts.get("tags_complex_deletion_timestamp"),
        None,
        "issue #4309: no marker means no deletion time to report — the sweep must never \
         invent one. {facts:?}"
    );
    assert_eq!(facts.get("tags_complex_deletion_time"), None, "{facts:?}");
}

/// (c) The column contributed NO cell at all: every one of its three
/// columns must be absent, not `Bool(false)`. "The collection was not
/// touched in this row" and "the collection was touched with no marker" are
/// different facts.
#[test]
fn a_complex_column_absent_from_the_row_states_nothing() {
    let facts = facts_for(vec![]);
    for column in [
        "tags_complex_deletion",
        "tags_complex_deletion_timestamp",
        "tags_complex_deletion_time",
    ] {
        assert_eq!(
            facts.get(column),
            None,
            "issue #4309: a collection column with no cell in this row must state NOTHING \
             — reporting Bool(false) would conflate 'untouched' with 'touched, no marker'. \
             {facts:?}"
        );
    }
}

/// The CENSUS exclusion (roborev finding S1): `Bool(false)` on a
/// `_complex_deletion` column is the absence of a marker and must NOT count
/// as observing the `complex_deletion` family — otherwise a fixture with no
/// marker anywhere would satisfy a coverage claim for it. Every OTHER
/// boolean is a genuine positive: `bound_inclusive = false` is an EXCLUSIVE
/// bound, a real measurement.
#[test]
fn only_a_false_complex_deletion_is_excluded_from_the_census() {
    assert!(
        is_negative_complex_marker("tags_complex_deletion", &Fact::Bool(false)),
        "a false complex-deletion marker is an ABSENCE, not an observation"
    );
    assert!(
        !is_negative_complex_marker("tags_complex_deletion", &Fact::Bool(true)),
        "a TRUE marker is a genuine observation"
    );
    assert!(
        !is_negative_complex_marker("bound_inclusive", &Fact::Bool(false)),
        "issue #4309: `bound_inclusive = false` is an EXCLUSIVE bound — a real measurement, \
         not an absence. Excluding it would under-count a family the range-tombstone lanes \
         legitimately claim"
    );
}

// ---------------------------------------------------------------------------
// `classify_columns`' COMPLEX branch, also gate-unreachable (roborev job 56)
// ---------------------------------------------------------------------------

/// Build the raw view's full contract column set for a table with the given
/// keys, simple base columns and collection columns.
///
/// MODELS ONE ORDERING PROPERTY, not the whole contract order (roborev job
/// 64). What `classify_columns` depends on is reproduced exactly: KEYS
/// FIRST, then each base column immediately followed by its synthesized
/// metadata siblings — that is what its `take_while` key-boundary scan
/// reads. The trailing always-applicable block's INTERNAL order is NOT
/// modelled: production emits `row_kind` BETWEEN the partition-deletion
/// pair and `bound_inclusive` (`raw_view/columns.rs:347`), whereas this
/// helper appends it with the other `DECLARED_GAP_COLUMNS`. Nothing in
/// `classify_columns` reads that order today, so the difference is inert —
/// but an order-sensitive check added there later would be validated
/// against a shape production never emits, so fix this helper before
/// adding one.
fn contract_columns(keys: &[&str], simple: &[&str], complex: &[&str]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    for c in simple {
        names.push((*c).to_string());
        for suffix in ["_timestamp", "_ttl", "_local_deletion_time", "_tombstone"] {
            names.push(format!("{c}{suffix}"));
        }
    }
    for c in complex {
        names.push((*c).to_string());
        for suffix in [
            "_complex_deletion",
            "_complex_deletion_time",
            "_complex_deletion_timestamp",
        ] {
            names.push(format!("{c}{suffix}"));
        }
    }
    for n in ROW_LEVEL_METADATA
        .iter()
        .chain(PARTITION_METADATA)
        .chain(RANGE_METADATA)
        .chain(DECLARED_GAP_COLUMNS)
    {
        names.push((*n).to_string());
    }
    names
        .into_iter()
        .enumerate()
        .map(|(position, name)| ColumnInfo {
            name,
            data_type: cqlite_core::types::DataType::Text,
            nullable: true,
            position,
            table_name: None,
            cql_type: None,
        })
        .collect()
}

/// The `_complex_deletion` EXCLUSION, which `classify_columns`' own comment
/// calls load-bearing. `tags_complex_deletion` itself has a
/// `tags_complex_deletion_timestamp` sibling, so without the
/// `!name.ends_with("_complex_deletion")` guard it would be misread as a
/// SIMPLE base column — and the sweep would then demand `<col>_ttl` /
/// `<col>_tombstone` siblings the contract never declares.
///
/// Only `test_deltas.collection_ops` has a collection column and it is
/// `FetchOnly`, so this branch is otherwise unexercised on the gate of
/// record.
#[test]
fn classify_columns_separates_a_collection_from_its_own_metadata() {
    let columns = contract_columns(&["pk", "ck"], &["body"], &["tags"]);
    let roles = classify_columns(&columns, &SPEC);

    assert_eq!(
        roles.complex_columns,
        vec!["tags".to_string()],
        "the collection column is the one with a `_complex_deletion` sibling"
    );
    assert_eq!(
        roles.simple_columns,
        vec!["body".to_string()],
        "issue #4309: `tags_complex_deletion` must NOT be classified as a simple base \
         column. It has a `tags_complex_deletion_timestamp` sibling, so dropping the \
         `!name.ends_with(\"_complex_deletion\")` guard admits it here — and the sweep \
         would then demand `tags_complex_deletion_ttl` / `_tombstone` columns the \
         contract never declares. Got: {:?}",
        roles.simple_columns
    );
    assert_eq!(roles.clustering_columns, vec!["ck".to_string()]);

    // The metadata sibling is in NEITHER base-column role.
    for role in [&roles.simple_columns, &roles.complex_columns] {
        assert!(
            !role.iter().any(|c| c == "tags_complex_deletion"),
            "`tags_complex_deletion` is a METADATA column, not a base column: {role:?}"
        );
    }

    // All three of the collection's metadata columns are compared.
    for expected in [
        "tags_complex_deletion",
        "tags_complex_deletion_time",
        "tags_complex_deletion_timestamp",
    ] {
        assert!(
            roles.compared_columns.iter().any(|c| c == expected),
            "issue #4309: '{expected}' must be compared against the golden. Compared: {:?}",
            roles.compared_columns
        );
    }
}

/// The CONVERSE accounting check (roborev finding R2): a contract column
/// that is neither compared nor a declared gap must FAIL, because a column
/// nobody accounts for is compared by nothing and noticed by nobody. Also
/// gate-unreachable today — every fixture's contract is exactly accounted.
#[test]
#[should_panic(expected = "neither compares against the golden nor declares as a gap")]
fn an_unaccounted_contract_column_is_refused() {
    let mut columns = contract_columns(&["pk", "ck"], &["body"], &["tags"]);
    let position = columns.len();
    columns.push(ColumnInfo {
        name: "a_column_the_sweep_never_heard_of".to_string(),
        data_type: cqlite_core::types::DataType::Text,
        nullable: true,
        position,
        table_name: None,
        cql_type: None,
    });
    let _ = classify_columns(&columns, &SPEC);
}

// ---------------------------------------------------------------------------
// The TTL half of the oracle, also gate-unreachable (roborev job 60)
// ---------------------------------------------------------------------------
//
// `cell_ttl`, `row_ttl` and `row_liveness_expires_at` are claimed only by
// `ttl_cells` and `gc_before_boundary`, both `Discipline::FetchOnly` — and
// the only committed JSONL goldens carrying a `"ttl"` field belong to those
// two fixtures, neither of which ships a `Data.db`. So `fold_simple_cell`'s
// TTL inheritance, its `expires_at` fallback chain, and `row_entry_facts`'
// `row_ttl` / `row_liveness_expires_at` inserts run on NO gate build.
//
// The module doc already flags inheritance as the harness's riskiest
// assumption; leaving it uncontrolled as well is the gap this lane exists
// to close. These three cases also pin the UNIT SPLIT that makes the facts
// comparable at all: timestamps are epoch MICROseconds, `expires_at` and
// every `*_local_deletion_time` are epoch SECONDS.

/// One row with a liveness marker, plus one cell, both under the
/// `body`-as-simple-column roles the TTL derivations need.
fn ttl_facts(
    liveness: serde_json::Value,
    cell: serde_json::Value,
) -> BTreeMap<String, raw_view_parity::golden::Fact> {
    let mut row = json!({
        "type": "row",
        "clustering": ["10"],
        "cells": [cell]
    });
    if !liveness.is_null() {
        row["liveness_info"] = liveness;
    }
    let (expected, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({ "partition": { "key": ["1"] }, "rows": [row] })],
        )],
        &roles_with(&["ck"], &["body"]),
        &SPEC,
    );
    assert_eq!(expected.len(), 1, "one synthetic row");
    expected.into_iter().next().unwrap().facts
}

/// (a) INHERITANCE. `serializeCell` omits `ttl` when the cell expires with
/// the SAME ttl as the row marker, so a cell with neither `ttl` nor
/// `expires_at` inherits both from the row's `liveness_info`. Also pins the
/// unit split: `row_timestamp` in MICROseconds, `row_liveness_expires_at`
/// and `body_local_deletion_time` in SECONDS.
#[test]
fn a_cell_inherits_the_rows_ttl_and_expiry() {
    let facts = ttl_facts(
        json!({
            "tstamp": "2021-01-01T00:00:00Z",
            "ttl": 3600,
            "expires_at": "2021-01-01T01:00:00Z"
        }),
        json!({ "name": "body", "value": "x", "tstamp": "2021-01-01T00:00:00Z" }),
    );
    assert_eq!(facts.get("row_ttl"), Some(&Fact::Int(3600)), "{facts:?}");
    assert_eq!(
        facts.get("row_liveness_expires_at"),
        Some(&Fact::BigInt(1609462800)),
        "issue #4309: `expires_at` is epoch SECONDS (iso_to_secs). A micros value here \
         would be 1609462800000000. {facts:?}"
    );
    assert_eq!(
        facts.get("row_timestamp"),
        Some(&Fact::BigInt(1609459200000000)),
        "issue #4309: a write timestamp is epoch MICROseconds (iso_to_micros). {facts:?}"
    );
    assert_eq!(
        facts.get("body_ttl"),
        Some(&Fact::Int(3600)),
        "issue #4309: the cell states no ttl of its own, so it INHERITS the row marker's \
         — dropping that inheritance is invisible on the gate of record. {facts:?}"
    );
    assert_eq!(
        facts.get("body_local_deletion_time"),
        Some(&Fact::BigInt(1609462800)),
        "the inherited expiry, also in SECONDS: {facts:?}"
    );
}

/// (b) The CELL'S OWN values win over the row's. If inheritance were
/// applied unconditionally the cell would report the row's 3600/01:00:00Z
/// rather than its own 60/00:01:00Z.
#[test]
fn a_cells_own_ttl_wins_over_the_rows() {
    let facts = ttl_facts(
        json!({
            "tstamp": "2021-01-01T00:00:00Z",
            "ttl": 3600,
            "expires_at": "2021-01-01T01:00:00Z"
        }),
        json!({
            "name": "body",
            "value": "x",
            "tstamp": "2021-01-01T00:00:00Z",
            "ttl": 60,
            "expires_at": "2021-01-01T00:01:00Z"
        }),
    );
    assert_eq!(facts.get("body_ttl"), Some(&Fact::Int(60)), "{facts:?}");
    assert_eq!(
        facts.get("body_local_deletion_time"),
        Some(&Fact::BigInt(1609459260)),
        "the CELL's own expiry, not the row's 1609462800: {facts:?}"
    );
    // The row still reports its own.
    assert_eq!(facts.get("row_ttl"), Some(&Fact::Int(3600)), "{facts:?}");
}

/// (c) NEGATIVE CONTROL. A row whose liveness marker carries NO ttl is not
/// expiring, so every TTL fact must be ABSENT — never a fabricated zero
/// (#28). `Fact::Absent` and `Fact::Int(0)` are different statements, and
/// telling them apart is the entire point of this sweep's fact model.
#[test]
fn a_row_without_a_ttl_states_no_ttl_facts_at_all() {
    let facts = ttl_facts(
        json!({ "tstamp": "2021-01-01T00:00:00Z" }),
        json!({ "name": "body", "value": "x", "tstamp": "2021-01-01T00:00:00Z" }),
    );
    for column in [
        "row_ttl",
        "row_liveness_expires_at",
        "body_ttl",
        "body_local_deletion_time",
    ] {
        assert_eq!(
            facts.get(column),
            None,
            "issue #4309: nothing here is expiring, so '{column}' must be ABSENT. A \
             fabricated zero would be an invented fact (#28), and the view correctly \
             reports nothing — the harness must too. {facts:?}"
        );
    }
    // The write timestamp is still stated: absence of a TTL is not absence
    // of the row.
    assert_eq!(
        facts.get("row_timestamp"),
        Some(&Fact::BigInt(1609459200000000)),
        "{facts:?}"
    );
}

// ---------------------------------------------------------------------------
// `load_goldens`' GENERATION SELECTION (roborev job 62)
// ---------------------------------------------------------------------------
//
// The `dirs[0]` pin is the linchpin keeping oracle and query on the same
// bytes, and it had no test: every case above constructs `GoldenSstable`
// values directly, and the `dirs.len() > 1` branch executes on NO corpus,
// because every committed table has exactly one `Data.db`-bearing
// directory. These build the two-directory layout on disk instead.

const SELECT_SPEC: FixtureSpec = FixtureSpec {
    keyspace: "selftest_ks",
    table: "sel",
    schema_file: "unused-no-database-is-opened.cql",
    partition_key_columns: &["pk"],
    discipline: Discipline::GitCommitted,
};

/// One `<table>-<uuid>/` directory carrying `<gen>-Data.db` binaries and
/// their sidecars. The binary content is irrelevant — `load_goldens` only
/// needs the NAME to exist so `table_generation_dirs` counts the directory
/// as `Data.db`-bearing; the oracle it reads is the `.jsonl`.
fn generation_dir_on_disk(root: &std::path::Path, dir_name: &str, gens: &[(&str, &str)]) {
    let dir = root.join(SELECT_SPEC.keyspace).join(dir_name);
    std::fs::create_dir_all(&dir).expect("temp dir");
    for (data_db, key) in gens {
        std::fs::write(dir.join(data_db), b"not read").expect("write binary");
        std::fs::write(
            dir.join(format!("{data_db}.jsonl")),
            format!(
                r#"{{"partition":{{"key":["{key}"]}},"rows":[{{"type":"row","clustering":["10"],"liveness_info":{{"tstamp":"2021-01-01T00:00:00Z"}},"cells":[]}}]}}"#
            ),
        )
        .expect("write sidecar");
    }
}

/// Two `Data.db`-bearing generation directories: the oracle must bind to
/// the lexically FIRST and read ONLY its goldens. Before the `dirs[0]` pin
/// this returned both, and the two same-named goldens tripped a hard
/// refusal on a `must_run` case.
#[test]
fn load_goldens_binds_to_the_lexically_first_generation_directory() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    // CREATED IN DESCENDING ORDER ON PURPOSE (roborev job 67). Creating
    // them ascending would let this pass on any filesystem whose `read_dir`
    // returns entries in creation order, whether or not
    // `table_generation_dirs` sorts — so the control would not actually
    // establish the determinism its message claims. Created later-first, a
    // `dirs[0]` that reflected readdir order would pick `sel-ffffffff` and
    // fail.
    generation_dir_on_disk(tmp.path(), "sel-ffffffff", &[("nb-1-big-Data.db", "2")]);
    generation_dir_on_disk(tmp.path(), "sel-00000000", &[("nb-1-big-Data.db", "1")]);

    let goldens = load_goldens(tmp.path(), &SELECT_SPEC);
    assert_eq!(
        goldens.len(),
        1,
        "issue #4309: exactly ONE generation directory is read; enumerating both is what \
         produced two goldens named 'nb-1-big-Data.db' and hard-failed a must_run case"
    );
    assert!(
        goldens[0].source_dir.ends_with("sel-00000000"),
        "the LEXICALLY FIRST directory — the selection must be deterministic across runs \
         and machines, not filesystem-order dependent. Got {}",
        goldens[0].source_dir.display()
    );
    assert_eq!(goldens[0].data_db, "nb-1-big-Data.db");
}

/// Binding to one directory must NOT collapse a genuinely multi-generation
/// fixture: every real one keeps `nb-1` and `nb-2` inside a SINGLE
/// directory, so both are still read. This is what makes
/// `shape:multi_generation` survive the pin.
#[test]
fn load_goldens_keeps_every_generation_inside_the_chosen_directory() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    generation_dir_on_disk(
        tmp.path(),
        "sel-00000000",
        &[("nb-1-big-Data.db", "1"), ("nb-2-big-Data.db", "1")],
    );

    let goldens = load_goldens(tmp.path(), &SELECT_SPEC);
    let names: Vec<&str> = goldens.iter().map(|g| g.data_db.as_str()).collect();
    assert_eq!(
        names,
        vec!["nb-1-big-Data.db", "nb-2-big-Data.db"],
        "issue #4309: both generations of a multi-generation fixture live in ONE \
         directory and must both be read — otherwise the directory pin silently retires \
         shape:multi_generation"
    );
}

/// A root with no `Data.db`-bearing directory is a broken checkout, not an
/// empty result.
#[test]
#[should_panic(expected = "no *-Data.db-bearing")]
fn load_goldens_refuses_a_root_with_no_generation_directory() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join(SELECT_SPEC.keyspace)).expect("ks dir");
    let _ = load_goldens(tmp.path(), &SELECT_SPEC);
}

/// A `Data.db` with no `.jsonl` sidecar is a FAILURE, never a skip — the
/// sidecar IS the oracle.
#[test]
#[should_panic(expected = "must be readable")]
fn load_goldens_refuses_a_generation_with_no_sidecar() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let dir = tmp.path().join(SELECT_SPEC.keyspace).join("sel-00000000");
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(dir.join("nb-1-big-Data.db"), b"not read").expect("write binary");
    let _ = load_goldens(tmp.path(), &SELECT_SPEC);
}

/// An empty sidecar is a fixture that stopped exercising the sweep, and
/// must FAIL rather than pass vacuously.
#[test]
#[should_panic(expected = "carried no partitions")]
fn load_goldens_refuses_an_empty_golden() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let dir = tmp.path().join(SELECT_SPEC.keyspace).join("sel-00000000");
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(dir.join("nb-1-big-Data.db"), b"not read").expect("write binary");
    std::fs::write(dir.join("nb-1-big-Data.db.jsonl"), b"\n").expect("write sidecar");
    let _ = load_goldens(tmp.path(), &SELECT_SPEC);
}

// ---------------------------------------------------------------------------
// The oracle's FAIL-CLOSED panics (roborev job 64)
// ---------------------------------------------------------------------------
//
// Five refusals in the golden model exist so a fixture the harness cannot
// model FAILS instead of being silently skipped. Each is reachable only
// from a corpus fixture that does not exist today — i.e. exactly the
// "derivation nobody executes" class this lane's charter names. A refusal
// that has never fired is a refusal nobody has checked still fires.

/// `bound_facts` refuses a bound `type` that is neither `inclusive` nor
/// `exclusive`, rather than defaulting `bound_inclusive` to a guess.
#[test]
#[should_panic(expected = "unexpected sstabledump bound type")]
fn an_unknown_bound_type_is_refused() {
    let _ = census(&[generation_of(
        "nb-1-big-Data.db",
        vec![json!({
            "partition": { "key": ["1"] },
            "rows": [{
                "type": "range_tombstone_bound",
                "start": {
                    "type": "sort_of_inclusive",
                    "clustering": ["10"],
                    "deletion_info": {
                        "marked_deleted": "2021-01-01T00:00:00Z",
                        "local_delete_time": "2021-01-01T00:00:00Z"
                    }
                }
            }]
        })],
    )]);
}

/// `fold_simple_cell` refuses a cell carrying neither its own `tstamp` nor
/// an enclosing `liveness_info.tstamp` — the sweep will not invent a write
/// time. Without the refusal the cell would silently compare as an absence.
#[test]
#[should_panic(expected = "refuses to invent a write time")]
fn a_cell_with_no_write_time_anywhere_is_refused() {
    let (_, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "row",
                    "clustering": ["10"],
                    "cells": [{ "name": "body", "value": "x" }]
                }]
            })],
        )],
        &roles_with(&["ck"], &["body"]),
        &SPEC,
    );
}

/// `render_golden_clustering` refuses an entry with no `clustering` array.
/// `serializeClustering` omits it for a size-0 prefix (an open range bound)
/// AND for every row of a clustering-free table, and the harness models
/// NEITHER — so it must say so rather than compare a fabricated shape.
#[test]
#[should_panic(expected = "carries no 'clustering' array")]
fn an_entry_with_no_clustering_array_is_refused() {
    let (_, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "row",
                    "liveness_info": { "tstamp": "2021-01-01T00:00:00Z" },
                    "cells": []
                }]
            })],
        )],
        &roles(),
        &SPEC,
    );
}

/// `golden_i32` refuses a TTL that does not fit an `int` rather than
/// wrapping with `as i32` — a wrapped value would make the equality
/// assertion compare the WRONG number and pass, which is an oracle blind
/// spot rather than a failure.
#[test]
#[should_panic(expected = "must fit the i32 an int column reports")]
fn a_ttl_too_large_for_an_int_is_refused() {
    let _ = ttl_facts(
        json!({
            "tstamp": "2021-01-01T00:00:00Z",
            "ttl": 4_294_967_296i64,
            "expires_at": "2021-01-01T01:00:00Z"
        }),
        json!({ "name": "body", "value": "x", "tstamp": "2021-01-01T00:00:00Z" }),
    );
}

/// The sweep refuses a golden entry `type` it cannot model, rather than
/// ignoring a physical entry — an ignored entry is a row the view could
/// omit with nothing noticing.
#[test]
#[should_panic(expected = "unrecognized sstabledump entry type")]
fn an_unmodelled_entry_type_is_refused() {
    let _ = census(&[generation_of(
        "nb-1-big-Data.db",
        vec![json!({
            "partition": { "key": ["1"] },
            "rows": [{ "type": "some_future_entry_kind" }]
        })],
    )]);
}

/// The PARTITION-TOMBSTONE fact derivation, which no gate-executed fixture
/// reaches (roborev job 69).
///
/// All four claimants of `entry:partition_deletion` are
/// `Discipline::FetchOnly`, and no `GitCommitted` sweep fixture carries a
/// partition-level `deletion_info` — verified across the committed goldens.
/// The census control asserts only that the token is COUNTED; the values
/// and the row shape were never asserted by anything the gate runs.
///
/// The two instants are DISTINCT on purpose: with equal values the
/// seconds-vs-micros unit split is unobservable, and swapping
/// `marked_deleted` for `local_delete_time` would pass.
#[test]
fn a_partition_deletion_yields_a_partition_tombstone_row_with_both_times() {
    let (expected, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": {
                    "key": ["1"],
                    "deletion_info": {
                        "marked_deleted": "2021-01-01T00:00:01Z",
                        "local_delete_time": "2021-01-01T00:00:02Z"
                    }
                },
                "rows": []
            })],
        )],
        &roles(),
        &SPEC,
    );

    assert_eq!(expected.len(), 1, "one partition tombstone row");
    let row = &expected[0];
    assert_eq!(
        row.row_kind, "partition_tombstone",
        "a partition-level deletion_info is a PARTITION tombstone, not a row"
    );
    assert_eq!(
        row.clustering,
        vec![None],
        "issue #4309: a partition tombstone has no clustering position — every component \
         must be ABSENT, never a fabricated value"
    );
    assert_eq!(
        row.facts.get("partition_deletion_timestamp"),
        Some(&Fact::BigInt(1609459201000000)),
        "issue #4309: `marked_deleted` is the write timestamp, in epoch MICROseconds. \
         Reading `local_delete_time` here instead would give 1609459202000000. {:?}",
        row.facts
    );
    assert_eq!(
        row.facts.get("partition_deletion_time"),
        Some(&Fact::BigInt(1609459202)),
        "issue #4309: `local_delete_time` is the GC clock, in epoch SECONDS. A micros \
         value would be 1609459202000000; the `marked_deleted` instant would be \
         1609459201. {:?}",
        row.facts
    );
}

// ---------------------------------------------------------------------------
// The SWEEP side's fail-closed refusals (roborev job 73)
// ---------------------------------------------------------------------------
//
// The five refusals controlled above are all in the GOLDEN model. The sweep
// has four of its own, and none of their panicking arms is reachable from
// any fixture in the corpus — so none had ever executed either. Same
// standard: a refusal that has never fired is a refusal nobody has checked
// still fires.

/// A spec naming a table no root carries, used to drive `resolve_root`'s
/// two disciplines.
const fn absent_spec(discipline: Discipline) -> FixtureSpec {
    FixtureSpec {
        keyspace: "selftest_absent_ks",
        table: "no_such_table",
        schema_file: "unused-no-database-is-opened.cql",
        partition_key_columns: &["pk"],
        discipline,
    }
}

/// `resolve_root` on a `GitCommitted` fixture that is absent: a broken
/// checkout, never an unfetched corpus, so it must PANIC rather than skip.
#[test]
#[should_panic(expected = "so its absence is a broken checkout")]
fn an_absent_git_committed_fixture_is_refused_not_skipped() {
    let _ = resolve_root(&absent_spec(Discipline::GitCommitted));
}

/// The other half of the same split: an absent `FetchOnly` fixture SKIPs
/// cleanly (returns `None`) when strict mode is off. Without this the
/// refusal above could be satisfied by a `resolve_root` that panics on
/// everything.
#[test]
fn an_absent_fetch_only_fixture_skips_cleanly() {
    // Guard the environment rather than assume it: under
    // CQLITE_REQUIRE_FIXTURES=1 this same call is REQUIRED to panic, so the
    // assertion below would be wrong. The sweep is run both ways.
    if std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref() == Ok("1") {
        return;
    }
    assert!(
        resolve_root(&absent_spec(Discipline::FetchOnly)).is_none(),
        "issue #4309: an absent FETCH-ONLY fixture is a missing corpus, not a broken \
         checkout — it must skip cleanly, or the discipline split means nothing"
    );
}

/// `partition_key_predicate` builds point predicates for INTEGER partition
/// keys only, and refuses anything else rather than emitting a predicate
/// that would silently match nothing (a point query returning no rows would
/// then read as a view defect).
#[test]
#[should_panic(expected = "only builds point predicates for INTEGER")]
fn a_non_integer_partition_key_predicate_is_refused() {
    let _ = partition_key_predicate(&SPEC, "not_an_integer");
}

/// `fact_of` refuses a metadata value shape the sweep does not compare,
/// rather than silently dropping it — an unrecognized shape is a contract
/// change that must be reviewed.
#[test]
#[should_panic(expected = "rendered an unexpected value shape")]
fn an_unexpected_metadata_value_shape_is_refused() {
    let _ = fact_of(Some(&Value::Float(1.0)));
}

/// `render_actual_clustering` renders int/text/bool components only, and
/// refuses the rest rather than formatting something the golden comparison
/// could never match.
#[test]
#[should_panic(expected = "renders only int/text/bool clustering components")]
fn an_unsupported_clustering_component_type_is_refused() {
    let _ = render_actual_clustering(&Value::Float(1.0), "selftest");
}
