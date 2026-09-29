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
//! Five shape tokens — `entry:partition_deletion`,
//! `entry:range_tombstone_bound`, `entry:range_tombstone_boundary`,
//! `shape:prefix_bound`, `shape:row_update_without_liveness` — are claimed
//! ONLY by `Discipline::FetchOnly` lanes (`partition_tombstones`,
//! `adjacent_ranges`, `range_tombstones`, `wide_range_tombstone`,
//! `partial_updates`, `resurrection_*`, `skipped_partition_delete`), all of
//! which SKIP under the gate's corpus-less `core-tests`. Their derivations
//! in `build_expectations` were therefore unexercised on the gate of record
//! — "a token derived from something nobody checks", the same blindness the
//! census section argues against, one level down.
//!
//! This lane opens no database and reads no corpus, so every case here is
//! `must_run` on EVERY gate. Each of those tokens now has a positive control
//! and a near-miss negative control, which is what makes the "negative
//! control per token claim" statement in `raw_view_parity.rs` true rather
//! than aspirational.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_parity.rs"]
mod raw_view_parity;

use raw_view_parity::golden::{build_expectations, ColumnRoles, GoldenSstable};
use raw_view_parity::{Discipline, FixtureSpec};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A single-clustering-column, no-regular-column table: the census tokens
/// under test are derived from partition keys and entry kinds, never from
/// the regular-column set, so the narrowest roles that still model a real
/// table keep the fixture honest.
fn roles() -> ColumnRoles {
    ColumnRoles {
        clustering_columns: vec!["ck".to_string()],
        simple_columns: Vec::new(),
        complex_columns: Vec::new(),
        compared_columns: Vec::new(),
    }
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
    ColumnRoles {
        clustering_columns: clustering.iter().map(|c| c.to_string()).collect(),
        simple_columns: simple.iter().map(|c| c.to_string()).collect(),
        complex_columns: Vec::new(),
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
            "rows": [{
                "type": "range_tombstone_bound",
                "start": bound("inclusive", json!(["10"])),
                "end": bound("inclusive", json!(["20"]))
            }]
        })],
    )]);
    assert_eq!(
        plain
            .get("entry:range_tombstone_bound")
            .copied()
            .unwrap_or(0),
        1,
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
                "rows": [{
                    "type": "range_tombstone_bound",
                    "start": bound("inclusive", json!(["10", "*"])),
                    "end": bound("inclusive", json!(["20", "*"]))
                }]
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
                "rows": [{
                    "type": "range_tombstone_bound",
                    "start": bound("inclusive", json!(["10", "1"])),
                    "end": bound("inclusive", json!(["20", "2"]))
                }]
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
