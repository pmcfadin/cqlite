//! Issue #4309 — SELF-TEST of the parity sweep's coverage census.
//!
//! The sweep's 27 fixture cases test the raw SSTable VIEW. This lane tests
//! the HARNESS: it feeds `build_expectations` synthetic sstabledump goldens
//! whose shape is known by construction and asserts the census token the
//! sweep derives from them. Nothing here opens a database or touches the
//! corpus, so it is `must_run` on every checkout and every gate.
//!
//! # Why the census needs its own oracle (roborev job 42, issue #4309)
//!
//! A census token exists to catch a REGENERATED fixture that silently lost
//! the shape its lane exists for — golden and expectation model lose it
//! together and compare cleanly. A token derived from something WEAKER than
//! the shape it names reintroduces exactly that blindness inside the census.
//! `shape:multi_generation` was such a token: it was bumped from
//! `goldens.len() > 1`, i.e. "this fixture has more than one SSTable", while
//! the five lanes claiming it (`skipped_partition_delete`,
//! `resurrection_gc0`, `resurrection_gc_positive`, `dropped_regular_col`,
//! `dropped_static_col`) exist to show ONE PARTITION KEY yielding one
//! UNRECONCILED row per generation. A regeneration whose two generations
//! hold DISJOINT keys keeps the old token positive with the cross-generation
//! property gone. `disjoint_keys_across_generations_do_not_claim_multi_generation`
//! is the negative control for that, and it FAILS against the old
//! derivation.
//!
//! # Why this lane covers the shape tokens the GATE cannot reach (job 46)
//!
//! FOUR shape tokens — `entry:partition_deletion`,
//! `entry:range_tombstone_boundary`, `shape:prefix_bound`,
//! `shape:row_update_without_liveness` — are claimed ONLY by
//! `Discipline::FetchOnly` lanes (`partition_tombstones`, `adjacent_ranges`,
//! `range_tombstones`, `partial_updates`, `resurrection_*`,
//! `skipped_partition_delete`), all of which SKIP under the gate's
//! corpus-less `core-tests`. Their derivations in `build_expectations` were
//! therefore unexercised on the gate of record — "a token derived from
//! something nobody checks", the same blindness the census section argues
//! against, one level down.
//!
//! `entry:range_tombstone_bound` is NOT one of them, and saying so was a doc
//! defect of exactly the class this file exists to prevent (roborev job 50):
//! `static_with_tombstones` is `Discipline::GitCommitted` and claims it, and
//! its committed golden carries two `range_tombstone_bound` entries — so
//! that derivation IS exercised on every gate. Its control here is DEFENCE
//! IN DEPTH, not the only executor. Check a claimant's DISCIPLINE before
//! writing "no gate-executed fixture reaches this".
//!
//! The COMPLEX-COLUMN half of the oracle has the same gap and no
//! gate-reachable control at all: `test_deltas.collection_ops` is the only
//! table in the whole sweep with a collection column
//! (`test-data/schemas/deltas.cql:119` — `SET`/`LIST`/`MAP`; the other four
//! sweep schemas have zero), and it is `FetchOnly`. So `fold_complex_column`,
//! the `_complex_deletion` exclusion in `classify_columns` that its own
//! comment calls "load-bearing", and the `Fact::Bool(false)` census
//! exclusion never execute under `core-tests`. They are controlled here.
//!
//! This lane opens no database and reads no corpus, so every case here is
//! `must_run` on EVERY gate. Each of those tokens now has a positive control
//! and a near-miss negative control, which is what makes the "negative
//! control per token claim" statement in `raw_view_parity.rs` true rather
//! than aspirational.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_parity.rs"]
mod raw_view_parity;

use raw_view_parity::golden::Fact;
use raw_view_parity::golden::RowIdentity;
use raw_view_parity::golden::{build_expectations, ColumnRoles, GoldenSstable};
use raw_view_parity::{assert_value_key_sets_match, is_negative_complex_marker};
use raw_view_parity::{Discipline, FixtureSpec};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A single-clustering-column, no-regular-column table: the census tokens
/// under test are derived from partition keys and entry kinds, never from
/// the regular-column set, so the narrowest roles that still model a real
/// table keep the fixture honest.
fn roles() -> ColumnRoles {
    roles_with(&["ck"], &[])
}

const SPEC: FixtureSpec = FixtureSpec {
    keyspace: "selftest",
    table: "census",
    schema_file: "unused-no-database-is-opened.cql",
    partition_key_columns: &["pk"],
    discipline: Discipline::GitCommitted,
};

/// One generation holding exactly the named partition keys, each with a
/// single live `row` entry — the minimum shape `build_expectations` models.
fn generation(data_db: &str, keys: &[&str]) -> GoldenSstable {
    GoldenSstable {
        data_db: data_db.to_string(),
        source_dir: PathBuf::from("/nonexistent/selftest"),
        partitions: keys
            .iter()
            .map(|key| {
                json!({
                    "partition": { "key": [key] },
                    "rows": [{
                        "type": "row",
                        "clustering": ["10"],
                        "liveness_info": { "tstamp": "2021-01-01T00:00:00Z" },
                        "cells": []
                    }]
                })
            })
            .collect(),
    }
}

/// `roles()` widened for a case that needs more clustering columns or a
/// real base column. The census tokens do not depend on these, but the
/// harness's own fail-closed guards do: `render_golden_clustering` asserts
/// one golden component per clustering column, and `row_entry_facts`
/// refuses a cell naming a column outside the contract.
fn roles_with(clustering: &[&str], simple: &[&str]) -> ColumnRoles {
    roles_full(clustering, simple, &[])
}

fn roles_full(clustering: &[&str], simple: &[&str], complex: &[&str]) -> ColumnRoles {
    ColumnRoles {
        clustering_columns: clustering.iter().map(|c| c.to_string()).collect(),
        simple_columns: simple.iter().map(|c| c.to_string()).collect(),
        complex_columns: complex.iter().map(|c| c.to_string()).collect(),
        compared_columns: Vec::new(),
    }
}

fn census(goldens: &[GoldenSstable]) -> BTreeMap<&'static str, usize> {
    census_with(goldens, &roles())
}

fn census_with(goldens: &[GoldenSstable], roles: &ColumnRoles) -> BTreeMap<&'static str, usize> {
    build_expectations(goldens, roles, &SPEC).1
}

/// A generation built from explicit partition objects, for the shapes
/// `generation()`'s one-live-row-per-key form cannot express.
fn generation_of(data_db: &str, partitions: Vec<serde_json::Value>) -> GoldenSstable {
    GoldenSstable {
        data_db: data_db.to_string(),
        source_dir: PathBuf::from("/nonexistent/selftest"),
        partitions,
    }
}

/// One range-tombstone bound side, as `JsonTransformer` renders it.
/// `clustering` is passed through verbatim so a caller can write the literal
/// `"*"` sstabledump emits for an unspecified trailing component.
fn bound(kind: &str, clustering: serde_json::Value) -> serde_json::Value {
    json!({
        "type": kind,
        "clustering": clustering,
        "deletion_info": {
            "marked_deleted": "2021-01-01T00:00:00Z",
            "local_delete_time": "2021-01-01T00:00:00Z"
        }
    })
}

/// NEGATIVE CONTROL. Two generations that share NO partition key carry no
/// cross-generation shadowing/resurrection shape at all, so the token must
/// read an affirmative zero even though `goldens.len() == 2`.
#[test]
fn disjoint_keys_across_generations_do_not_claim_multi_generation() {
    let observed = census(&[
        generation("nb-1-big-Data.db", &["1", "2"]),
        generation("nb-2-big-Data.db", &["3", "4"]),
    ]);
    assert_eq!(
        observed.get("shape:multi_generation").copied().unwrap_or(0),
        0,
        "issue #4309: two generations with DISJOINT partition keys exercise no \
         cross-generation reconciliation shape, so the census must not claim one. \
         Deriving the token from `goldens.len() > 1` reports it anyway, which is the \
         blindness this self-test exists to keep closed. observed: {observed:?}"
    );
}

/// POSITIVE CONTROL. One key present in BOTH generations IS the shape the
/// five claiming lanes exist for, and it must be counted once per such key.
#[test]
fn a_key_in_two_generations_claims_multi_generation() {
    let observed = census(&[
        generation("nb-1-big-Data.db", &["1", "2"]),
        generation("nb-2-big-Data.db", &["2", "3"]),
    ]);
    assert_eq!(
        observed.get("shape:multi_generation").copied().unwrap_or(0),
        1,
        "issue #4309: partition key '2' yields an unreconciled row in each of two \
         generations — exactly one key with the cross-generation shape. observed: \
         {observed:?}"
    );
}

/// A single generation can never carry the shape, whatever it contains.
#[test]
fn one_generation_never_claims_multi_generation() {
    let observed = census(&[generation("nb-1-big-Data.db", &["1", "2", "3"])]);
    assert_eq!(
        observed.get("shape:multi_generation").copied().unwrap_or(0),
        0,
        "issue #4309: a one-SSTable fixture has no second generation to shadow. \
         observed: {observed:?}"
    );
}

/// The token counts KEYS with the shape, not generations: a third generation
/// re-touching the same key does not inflate it, and a second shared key
/// does.
#[test]
fn multi_generation_counts_cross_generation_keys_not_generations() {
    let three_generations_one_shared_key = census(&[
        generation("nb-1-big-Data.db", &["1"]),
        generation("nb-2-big-Data.db", &["1"]),
        generation("nb-3-big-Data.db", &["1"]),
    ]);
    assert_eq!(
        three_generations_one_shared_key
            .get("shape:multi_generation")
            .copied()
            .unwrap_or(0),
        1,
        "one key, three generations = one cross-generation key: \
         {three_generations_one_shared_key:?}"
    );

    let two_generations_two_shared_keys = census(&[
        generation("nb-1-big-Data.db", &["1", "2"]),
        generation("nb-2-big-Data.db", &["1", "2"]),
    ]);
    assert_eq!(
        two_generations_two_shared_keys
            .get("shape:multi_generation")
            .copied()
            .unwrap_or(0),
        2,
        "two keys each spanning both generations = two cross-generation keys: \
         {two_generations_two_shared_keys:?}"
    );
}

/// Guards the self-test itself: if these synthetic goldens ever stopped
/// producing physical rows, every assertion above would pass vacuously on an
/// empty census. `entry:row` is the affirmative zero for that.
#[test]
fn the_synthetic_goldens_actually_produce_rows() {
    let (expected, observed) = build_expectations(
        &[
            generation("nb-1-big-Data.db", &["1", "2"]),
            generation("nb-2-big-Data.db", &["2", "3"]),
        ],
        &roles(),
        &SPEC,
    );
    let origins: Vec<&str> = expected.iter().map(|r| r.origin.as_str()).collect();
    assert_eq!(
        expected.len(),
        4,
        "four synthetic partitions, one row each: {origins:?}"
    );
    assert_eq!(
        observed.get("entry:row").copied().unwrap_or(0),
        4,
        "the census must have seen all four `row` entries: {observed:?}"
    );
}

// ---------------------------------------------------------------------------
// The shape tokens no GATE-EXECUTED fixture reaches (roborev job 46)
// ---------------------------------------------------------------------------

/// `entry:partition_deletion` is claimed by four lanes, every one of them
/// `FetchOnly`. Positive control plus the near miss that matters: a
/// partition carrying no `deletion_info` must leave an affirmative zero,
/// because a regenerated fixture whose `DELETE` was dropped is precisely
/// what the token exists to catch.
#[test]
fn partition_deletion_is_counted_only_when_the_partition_carries_one() {
    let with_deletion = census(&[generation_of(
        "nb-1-big-Data.db",
        vec![json!({
            "partition": {
                "key": ["1"],
                "deletion_info": {
                    "marked_deleted": "2021-01-01T00:00:00Z",
                    "local_delete_time": "2021-01-01T00:00:00Z"
                }
            },
            "rows": []
        })],
    )]);
    assert_eq!(
        with_deletion
            .get("entry:partition_deletion")
            .copied()
            .unwrap_or(0),
        1,
        "a partition with deletion_info IS a partition tombstone: {with_deletion:?}"
    );

    let without = census(&[generation("nb-1-big-Data.db", &["1"])]);
    assert_eq!(
        without
            .get("entry:partition_deletion")
            .copied()
            .unwrap_or(0),
        0,
        "issue #4309: a partition with no deletion_info must NOT claim the partition-tombstone \
         shape — a regeneration that lost its DELETE is exactly what this token catches. \
         observed: {without:?}"
    );
}

/// `entry:range_tombstone_bound` and `entry:range_tombstone_boundary` are
/// DISTINCT tokens over entry kinds that emit the IDENTICAL metadata
/// families, which is the whole reason the census counts shapes and not just
/// families. Each must count itself and not the other.
#[test]
fn range_tombstone_bound_and_boundary_are_counted_separately() {
    let plain = census(&[generation_of(
        "nb-1-big-Data.db",
        vec![json!({
            "partition": { "key": ["1"] },
            // ONE SIDE PER ENTRY. `serializeTombstone` never writes both
            // sides of a NON-boundary marker in one entry: a
            // `RangeTombstoneBoundMarker` holds the single side it opens or
            // closes. Confirmed against the committed `static_with_tombstones`
            // golden, which carries two separate `range_tombstone_bound`
            // entries — one `start`-only, one `end`-only. A control built on
            // a shape the oracle cannot produce is weaker than it reads
            // (roborev job 50).
            "rows": [
                { "type": "range_tombstone_bound", "start": bound("inclusive", json!(["10"])) },
                { "type": "range_tombstone_bound", "end": bound("inclusive", json!(["20"])) }
            ]
        })],
    )]);
    assert_eq!(
        plain
            .get("entry:range_tombstone_bound")
            .copied()
            .unwrap_or(0),
        2,
        "{plain:?}"
    );
    assert_eq!(
        plain
            .get("entry:range_tombstone_boundary")
            .copied()
            .unwrap_or(0),
        0,
        "issue #4309: a plain bound must not claim the BOUNDARY shape — they emit identical \
         metadata families, so only the shape token can tell them apart. observed: {plain:?}"
    );

    let boundary = census(&[generation_of(
        "nb-1-big-Data.db",
        vec![json!({
            "partition": { "key": ["1"] },
            "rows": [{
                "type": "range_tombstone_boundary",
                "end": bound("exclusive", json!(["10"])),
                "start": bound("inclusive", json!(["10"]))
            }]
        })],
    )]);
    assert_eq!(
        boundary
            .get("entry:range_tombstone_boundary")
            .copied()
            .unwrap_or(0),
        1,
        "{boundary:?}"
    );
    assert_eq!(
        boundary
            .get("entry:range_tombstone_bound")
            .copied()
            .unwrap_or(0),
        0,
        "issue #4309: a boundary must not claim the PLAIN-bound shape. observed: {boundary:?}"
    );
}

/// A BOUNDARY closes one range and opens the next at the same clustering
/// position, and sstabledump renders both sides in ONE entry — so it must
/// expand to TWO physical raw-view rows, exactly as a bound pair does. The
/// census cannot see that; only the row set can.
#[test]
fn a_boundary_expands_to_both_an_end_and_a_start_row() {
    let (expected, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "range_tombstone_boundary",
                    "end": bound("exclusive", json!(["10"])),
                    "start": bound("inclusive", json!(["10"]))
                }]
            })],
        )],
        &roles(),
        &SPEC,
    );
    let kinds: Vec<&str> = expected.iter().map(|r| r.row_kind).collect();
    assert_eq!(
        kinds,
        vec!["range_tombstone_end", "range_tombstone_start"],
        "one boundary entry must expand to an END row and a START row"
    );
}

/// `shape:prefix_bound` is the PREFIX-bound shape: sstabledump renders an
/// unspecified trailing clustering component as the literal `"*"`, and the
/// view must report it ABSENT rather than fabricate a value. A fully
/// specified bound must leave an affirmative zero.
#[test]
fn prefix_bound_is_counted_only_for_a_star_component() {
    let two_ck = roles_with(&["ck1", "ck2"], &[]);
    let prefix = census_with(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [
                    { "type": "range_tombstone_bound", "start": bound("inclusive", json!(["10", "*"])) },
                    { "type": "range_tombstone_bound", "end": bound("inclusive", json!(["20", "*"])) }
                ]
            })],
        )],
        &two_ck,
    );
    assert_eq!(
        prefix.get("shape:prefix_bound").copied().unwrap_or(0),
        2,
        "both sides carry a '*' trailing component: {prefix:?}"
    );

    let full = census_with(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [
                    { "type": "range_tombstone_bound", "start": bound("inclusive", json!(["10", "1"])) },
                    { "type": "range_tombstone_bound", "end": bound("inclusive", json!(["20", "2"])) }
                ]
            })],
        )],
        &two_ck,
    );
    assert_eq!(
        full.get("shape:prefix_bound").copied().unwrap_or(0),
        0,
        "issue #4309: a FULLY specified bound is not a prefix bound. observed: {full:?}"
    );
}

/// `shape:row_update_without_liveness` is the partial-UPDATE shape: no
/// primary-key liveness marker, yet real cells, so `row_timestamp` must be
/// ABSENT while every cell still carries its own write time.
///
/// The load-bearing discrimination is the THIRD control. A row TOMBSTONE
/// also lacks liveness, so "no liveness" alone does not identify the shape —
/// the non-empty cell set is what does. That is precisely the distinction
/// the derivation encodes and the one a future edit is most likely to break.
#[test]
fn row_update_without_liveness_needs_both_no_liveness_and_real_cells() {
    let with_body = roles_with(&["ck"], &["body"]);
    let update_only = census_with(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "row",
                    "clustering": ["10"],
                    "cells": [{ "name": "body", "value": "x", "tstamp": "2021-01-01T00:00:00Z" }]
                }]
            })],
        )],
        &with_body,
    );
    assert_eq!(
        update_only
            .get("shape:row_update_without_liveness")
            .copied()
            .unwrap_or(0),
        1,
        "no liveness marker + real cells = the partial-UPDATE shape: {update_only:?}"
    );

    let with_liveness = census(&[generation("nb-1-big-Data.db", &["1"])]);
    assert_eq!(
        with_liveness
            .get("shape:row_update_without_liveness")
            .copied()
            .unwrap_or(0),
        0,
        "issue #4309: a row WITH a liveness marker is not a partial UPDATE. observed: \
         {with_liveness:?}"
    );

    // A row tombstone: no liveness AND no cells. `deletion_info` alone must
    // not be mistaken for the partial-UPDATE shape.
    let row_tombstone = census_with(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "row",
                    "clustering": ["10"],
                    "deletion_info": {
                        "marked_deleted": "2021-01-01T00:00:00Z",
                        "local_delete_time": "2021-01-01T00:00:00Z"
                    },
                    "cells": []
                }]
            })],
        )],
        &with_body,
    );
    assert_eq!(
        row_tombstone
            .get("shape:row_update_without_liveness")
            .copied()
            .unwrap_or(0),
        0,
        "issue #4309: a ROW TOMBSTONE also lacks liveness — the non-empty cell set is what \
         makes a row the partial-UPDATE shape, and conflating the two is the edit this \
         control exists to catch. observed: {row_tombstone:?}"
    );
}

// ---------------------------------------------------------------------------
// The point-vs-scan value-column comparison (roborev job 47)
// ---------------------------------------------------------------------------
//
// The assertion this replaces compared `result.metadata.columns` across the
// two producers. That CANNOT FAIL for `SELECT *`: `raw_view/mod.rs:194`
// computes the column list ONCE, before the point/scan branch, and the
// `SelectClause::All` arm returns it unchanged — so it was byte-identical by
// construction and never derived from what a producer put in a row. These
// controls exist because "a comparison that cannot fail" is precisely the
// defect being fixed, and the only way to know the replacement is different
// is to watch it fail.

fn ident() -> RowIdentity {
    ("row".to_string(), vec![Some("10".to_string())])
}

fn keys(names: &[&str]) -> std::collections::BTreeSet<String> {
    names.iter().map(|n| n.to_string()).collect()
}

/// POSITIVE CONTROL: identical value-column sets compare clean.
#[test]
fn identical_value_key_sets_pass() {
    let both = keys(&["pk", "ck", "sstable", "row_kind", "body_timestamp"]);
    assert_value_key_sets_match("selftest", "1", &ident(), &both, &both);
}

/// NEGATIVE CONTROL, direction 1 — the one the finding is about: a point row
/// that DROPPED a metadata column. Against the fact model this compares
/// clean whenever the golden value is `Absent`, because `fact_of(None)` and
/// `fact_of(Some(Null))` are both `Fact::Absent`. Against the key sets it
/// must fail by name.
#[test]
#[should_panic(expected = "MISSING from the point row")]
fn a_point_row_missing_a_column_fails() {
    let scan = keys(&["pk", "ck", "sstable", "row_kind", "body_timestamp"]);
    let point = keys(&["pk", "ck", "sstable", "row_kind"]);
    assert_value_key_sets_match("selftest", "1", &ident(), &point, &scan);
}

/// NEGATIVE CONTROL, direction 2 (#3890 pins BOTH directions): a column
/// present ONLY on the point row is equally a contract divergence.
#[test]
#[should_panic(expected = "present ONLY on the point row")]
fn a_point_row_with_an_extra_column_fails() {
    let scan = keys(&["pk", "ck", "sstable", "row_kind"]);
    let point = keys(&["pk", "ck", "sstable", "row_kind", "body_ttl"]);
    assert_value_key_sets_match("selftest", "1", &ident(), &point, &scan);
}

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
