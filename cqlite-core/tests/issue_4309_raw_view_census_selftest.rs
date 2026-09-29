//! Issue #4309 — SELF-TEST of the parity sweep's coverage census.
//!
//! The sweep's 22 fixture cases test the raw SSTable VIEW. This lane tests
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

fn census(goldens: &[GoldenSstable]) -> BTreeMap<&'static str, usize> {
    build_expectations(goldens, &roles(), &SPEC).1
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
