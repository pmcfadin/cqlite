//! Issue #4309 — SELF-TEST of the parity sweep's coverage CENSUS.
//!
//! Sibling of `issue_4309_raw_view_oracle_selftest.rs`, which covers the
//! golden MODEL; the shared synthetic goldens live in
//! `support/raw_view_synthetic.rs`, whose module doc explains the split.
//! This lane owns the census: its shape tokens, its vocabulary, and
//! `require_observed`'s guards.
//!
//! # Why the census needs its own oracle (roborev job 42, issue #4309)
//!
//! A census token exists to catch a REGENERATED fixture that silently lost
//! the shape its lane exists for — golden and expectation model lose it
//! together and compare cleanly. A token derived from something WEAKER than
//! the shape it names reintroduces exactly that blindness inside the
//! census. `shape:multi_generation` was such a token: it was bumped from
//! `goldens.len() > 1`, i.e. "this fixture has more than one SSTable",
//! while the five lanes claiming it exist to show ONE PARTITION KEY
//! yielding one UNRECONCILED row per generation. A regeneration whose two
//! generations hold DISJOINT keys keeps the old token positive with the
//! cross-generation property gone.
//! `disjoint_keys_across_generations_do_not_claim_multi_generation` is the
//! negative control for that, and it FAILS against the old derivation.
//!
//! # Which shape tokens the GATE cannot reach (jobs 46/50/52)
//!
//! FIVE shape tokens — `entry:partition_deletion`,
//! `entry:range_tombstone_boundary`, `shape:prefix_bound`,
//! `shape:row_update_without_liveness` and `shape:multi_generation` — are
//! claimed ONLY by `Discipline::FetchOnly` lanes, all of which SKIP under
//! the gate's corpus-less `core-tests`. Their derivations in
//! `build_expectations` were therefore unexercised on the gate of record —
//! "a token derived from something nobody checks", the same blindness the
//! census section argues against, one level down.
//!
//! `entry:range_tombstone_bound` is NOT one of them, and saying so was a
//! doc defect of exactly the class this file exists to prevent (job 50):
//! `static_with_tombstones` is `Discipline::GitCommitted` and claims it, and
//! its committed golden carries two `range_tombstone_bound` entries — so
//! that derivation IS exercised on every gate. Its control here is DEFENCE
//! IN DEPTH, not the only executor. Check a claimant's DISCIPLINE before
//! writing "no gate-executed fixture reaches this".

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_synthetic.rs"]
mod synthetic;

use raw_view_parity::golden::SHAPE_TOKENS;
use raw_view_parity::golden::{build_expectations, Fact, RowIdentity};
use raw_view_parity::{
    assert_value_key_sets_match, fact_kind, FactKindMatch, SweepOutcome, FACT_KIND_RULES,
    KNOWN_COVERAGE_TOKENS, UNCLAIMABLE_TOKENS,
};
use serde_json::json;
use synthetic::raw_view_parity;
use synthetic::{bound, census, census_with, generation, generation_of, roles, roles_with, SPEC};

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
/// expand to TWO physical raw-view rows, exactly as a bound pair does.
///
/// THE TWO SIDES MUST BE DISTINGUISHABLE, or this control cannot see the
/// defect it exists for (roborev job 62). With identical `deletion_info` on
/// both sides, swapping the `[("end", …), ("start", …)]` pairing in
/// `build_expectations` — or reading `entry["start"]` for both rows —
/// changes nothing observable. So each side here carries its OWN
/// `type`/`marked_deleted`/`local_delete_time`, matching the real
/// `adjacent_ranges` golden, whose sides genuinely differ (start inclusive
/// at `…000002Z`, end exclusive at `…000001Z`). That fixture is `FetchOnly`,
/// so without this the mis-pairing would surface only on a strict-mode
/// fetched-corpus run — and there it would read as a VIEW failure rather
/// than a harness bug.
///
/// This is also the only place `bound_facts`' `"exclusive" => false` arm has
/// its RESULT asserted: the one gate-executed bound fixture,
/// `static_with_tombstones`, carries inclusive-only bounds.
#[test]
fn a_boundary_expands_to_two_rows_each_carrying_its_own_sides_facts() {
    let end_side = json!({
        "type": "exclusive",
        "clustering": ["10"],
        "deletion_info": {
            "marked_deleted": "2021-01-01T00:00:01Z",
            "local_delete_time": "2021-01-01T00:00:01Z"
        }
    });
    let start_side = json!({
        "type": "inclusive",
        "clustering": ["10"],
        "deletion_info": {
            "marked_deleted": "2021-01-01T00:00:02Z",
            "local_delete_time": "2021-01-01T00:00:02Z"
        }
    });
    let (expected, _) = build_expectations(
        &[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": { "key": ["1"] },
                "rows": [{
                    "type": "range_tombstone_boundary",
                    "end": end_side,
                    "start": start_side
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

    let end = &expected[0];
    assert_eq!(
        end.facts.get("bound_inclusive"),
        Some(&Fact::Bool(false)),
        "issue #4309: the END side is EXCLUSIVE here, so `bound_inclusive` must be \
         false — the only assertion of `bound_facts`' exclusive arm anywhere, since \
         the one gate-executed bound fixture has inclusive-only bounds. {:?}",
        end.facts
    );
    assert_eq!(
        end.facts.get("range_deletion_timestamp"),
        Some(&Fact::BigInt(1609459201000000)),
        "issue #4309: the END row must carry the END side's deletion time, not the \
         START side's (…202000000). A swapped pairing in build_expectations lands \
         here. {:?}",
        end.facts
    );
    assert_eq!(
        end.facts.get("range_deletion_time"),
        Some(&Fact::BigInt(1609459201)),
        "{:?}",
        end.facts
    );

    let start = &expected[1];
    assert_eq!(
        start.facts.get("bound_inclusive"),
        Some(&Fact::Bool(true)),
        "the START side is INCLUSIVE here: {:?}",
        start.facts
    );
    assert_eq!(
        start.facts.get("range_deletion_timestamp"),
        Some(&Fact::BigInt(1609459202000000)),
        "issue #4309: the START row must carry the START side's deletion time: {:?}",
        start.facts
    );
    assert_eq!(
        start.facts.get("range_deletion_time"),
        Some(&Fact::BigInt(1609459202)),
        "{:?}",
        start.facts
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
// ANCHORED ON THE RENDERED DIFFERENCE LIST, not the static prose (roborev
// job 51). `assert_value_key_sets_match`'s message ALWAYS contains both
// literal phrases, so matching on "MISSING from the point row" alone passes
// whichever direction actually diverged — swapping the two `expected`
// strings between these tests would leave both green, and neither control
// would establish the direction it names. The column name only appears in
// the list for the direction under test.
#[should_panic(expected = r#"MISSING from the point row: ["body_timestamp"]"#)]
fn a_point_row_missing_a_column_fails() {
    let scan = keys(&["pk", "ck", "sstable", "row_kind", "body_timestamp"]);
    let point = keys(&["pk", "ck", "sstable", "row_kind"]);
    assert_value_key_sets_match("selftest", "1", &ident(), &point, &scan);
}

/// NEGATIVE CONTROL, direction 2 (#3890 pins BOTH directions): a column
/// present ONLY on the point row is equally a contract divergence.
#[test]
#[should_panic(expected = r#"present ONLY on the point row: ["body_ttl"]"#)]
fn a_point_row_with_an_extra_column_fails() {
    let scan = keys(&["pk", "ck", "sstable", "row_kind"]);
    let point = keys(&["pk", "ck", "sstable", "row_kind", "body_ttl"]);
    assert_value_key_sets_match("selftest", "1", &ident(), &point, &scan);
}

// ---------------------------------------------------------------------------
// The census's OWN GUARDS, which never fire on a real run (roborev job 54)
// ---------------------------------------------------------------------------
//
// `require_observed`'s three guards — non-empty claim, known token name, not
// an unclaimable token — exist because a coverage claim that claims nothing
// certifies nothing. But no lane claims a bad token, so on every real run
// all three are silently satisfied. Inverting `!UNCLAIMABLE_TOKENS.contains`,
// dropping the `!kinds.is_empty()` assert, or moving either back BELOW the
// `if !self.ran { return; }` would leave every gate green while re-opening
// the exact hole each was added (jobs 47/52) to close.
//
// Every case below therefore drives the guard on a SKIPPED outcome
// (`ran == false`), which pins the pre-`ran` PLACEMENT as well as the check:
// 16 of the sweep's 27 cases skip under the gate's corpus-less `core-tests`,
// so a guard that runs only on the `ran` path is a guard the gate never runs.

/// POSITIVE CONTROL: a well-formed claim on a fixture that ran and observed
/// the token passes. Without this the `should_panic` cases below could all
/// be satisfied by a `require_observed` that rejects everything.
#[test]
fn a_well_formed_claim_on_an_observed_token_passes() {
    SweepOutcome::for_control("selftest", true, &[("entry:row", 3)])
        .require_observed(&["entry:row"]);
}

/// A claim for a token the fixture did NOT observe must fail — the
/// affirmative zero the whole census exists for.
#[test]
#[should_panic(expected = "states ZERO of them")]
fn a_claim_for_an_unobserved_token_fails() {
    SweepOutcome::for_control("selftest", true, &[("entry:row", 3)])
        .require_observed(&["entry:static_block"]);
}

/// GUARD 1, on a SKIPPED outcome: an EMPTY claim is not a claim.
#[test]
#[should_panic(expected = "an empty coverage claim")]
fn an_empty_claim_fails_even_when_the_fixture_skipped() {
    SweepOutcome::for_control("selftest", false, &[]).require_observed(&[]);
}

/// GUARD 2, on a SKIPPED outcome: a MISTYPED token can never be observed,
/// so it would make the claim vacuous rather than failing.
#[test]
#[should_panic(expected = "is not a known metadata family")]
fn a_mistyped_token_fails_even_when_the_fixture_skipped() {
    SweepOutcome::for_control("selftest", false, &[])
        .require_observed(&["shape:multi_generationn"]);
}

/// GUARD 3, on a SKIPPED outcome: the path witness is recorded but must
/// never be CLAIMED — under `feature = "tombstones"` it is necessarily zero.
#[test]
#[should_panic(expected = "no lane may CLAIM")]
fn claiming_the_path_witness_fails_even_when_the_fixture_skipped() {
    SweepOutcome::for_control("selftest", false, &[])
        .require_observed(&["shape:point_path_resolved"]);
}

/// `fact_kind`'s ARM ORDER is self-documented as load-bearing, and only
/// `test_deltas.collection_ops` (`FetchOnly`) has a complex column — so a
/// reorder mapping `tags_complex_deletion_timestamp` to `cell_timestamp` is
/// invisible on the gate of record. The generic `_timestamp` / `_time` arms
/// would both swallow these names if they came first.
#[test]
fn complex_deletion_arms_win_over_the_generic_timestamp_arms() {
    assert_eq!(
        fact_kind("tags_complex_deletion_timestamp"),
        "complex_deletion_timestamp",
        "issue #4309: the `_complex_deletion*` arms MUST precede the generic `_timestamp` \
         arm — otherwise this column is misclassified as `cell_timestamp` and the complex \
         family silently stops being observable"
    );
    assert_eq!(
        fact_kind("tags_complex_deletion_time"),
        "complex_deletion_time",
        "must not fall through to `cell_local_deletion_time`"
    );
    assert_eq!(fact_kind("tags_complex_deletion"), "complex_deletion");
    // The generic arms still work for ordinary columns.
    assert_eq!(fact_kind("body_timestamp"), "cell_timestamp");
    assert_eq!(
        fact_kind("body_local_deletion_time"),
        "cell_local_deletion_time"
    );
    assert_eq!(fact_kind("body_ttl"), "cell_ttl");
    assert_eq!(fact_kind("body_tombstone"), "cell_tombstone");
    // ...and the exact-match arms are not shadowed by the suffix arms.
    assert_eq!(fact_kind("row_timestamp"), "row_timestamp");
    assert_eq!(
        fact_kind("row_local_deletion_time"),
        "row_local_deletion_time"
    );
    assert_eq!(fact_kind("row_ttl"), "row_ttl");
    assert_eq!(fact_kind("row_tombstone"), "row_tombstone");
}

/// The vocabulary must stay in step with the two things that PRODUCE tokens
/// — `fact_kind`'s return set and `build_expectations`' `bump` calls. The
/// comment on `KNOWN_COVERAGE_TOKENS` asks a reader to keep them aligned by
/// hand; this asserts it instead.
#[test]
fn every_produced_token_is_in_the_known_vocabulary() {
    // A representative column PER RULE, synthesized FROM the rule table, so
    // the list cannot drift out of step with the arms it claims to cover
    // (roborev job 60). Asserting the synthesized column maps back to its
    // OWN rule also proves no rule is SHADOWED by an earlier one — an
    // unreachable rule is a family nothing can ever observe.
    for (rule, kind) in FACT_KIND_RULES {
        let column = match rule {
            FactKindMatch::Exact(name) => (*name).to_string(),
            FactKindMatch::Suffix(suffix) => format!("body{suffix}"),
        };
        assert_eq!(
            fact_kind(&column),
            *kind,
            "issue #4309: the representative column '{column}' for rule '{kind}' is \
             classified as '{}' instead — that rule is SHADOWED by an earlier one, so the \
             family it names can never be observed",
            fact_kind(&column)
        );
        assert!(
            KNOWN_COVERAGE_TOKENS.contains(kind),
            "issue #4309: fact_kind({column:?}) returns '{kind}', which is not in \
             KNOWN_COVERAGE_TOKENS — a family a lane can never claim, so its coverage \
             can never be asserted"
        );
    }

    // Every SHAPE token the synthetic goldens in this file can produce.
    // These censuses exercise each `bump` call in `build_expectations`.
    let mut produced: std::collections::BTreeSet<&str> = Default::default();
    for observed in [
        census(&[
            generation("nb-1-big-Data.db", &["1", "2"]),
            generation("nb-2-big-Data.db", &["2", "3"]),
        ]),
        census(&[generation_of(
            "nb-1-big-Data.db",
            vec![json!({
                "partition": {
                    "key": ["1"],
                    "deletion_info": {
                        "marked_deleted": "2021-01-01T00:00:00Z",
                        "local_delete_time": "2021-01-01T00:00:00Z"
                    }
                },
                "rows": [
                    { "type": "static_block", "cells": [] },
                    { "type": "range_tombstone_bound", "start": bound("inclusive", json!(["10"])) },
                    {
                        "type": "range_tombstone_boundary",
                        "end": bound("exclusive", json!(["10"])),
                        "start": bound("inclusive", json!(["10"]))
                    }
                ]
            })],
        )]),
    ] {
        produced.extend(observed.keys().copied());
    }
    // `shape:prefix_bound` needs a second clustering column to have a
    // trailing `"*"`, and `shape:row_update_without_liveness` needs a real
    // base column — so both come from their own roles.
    produced.extend(
        census_with(
            &[generation_of(
                "nb-1-big-Data.db",
                vec![json!({
                    "partition": { "key": ["1"] },
                    "rows": [
                        { "type": "range_tombstone_bound", "start": bound("inclusive", json!(["10", "*"])) }
                    ]
                })],
            )],
            &roles_with(&["ck1", "ck2"], &[]),
        )
        .keys()
        .copied(),
    );
    produced.extend(
        census_with(
            &[generation_of(
                "nb-1-big-Data.db",
                vec![json!({
                    "partition": { "key": ["1"] },
                    "rows": [{
                        "type": "row",
                        "clustering": ["10"],
                        "cells": [
                            { "name": "body", "value": "x", "tstamp": "2021-01-01T00:00:00Z" }
                        ]
                    }]
                })],
            )],
            &roles_with(&["ck"], &["body"]),
        )
        .keys()
        .copied(),
    );
    for token in &produced {
        assert!(
            KNOWN_COVERAGE_TOKENS.contains(token) || UNCLAIMABLE_TOKENS.contains(token),
            "issue #4309: build_expectations bumps '{token}', which is in neither \
             KNOWN_COVERAGE_TOKENS nor UNCLAIMABLE_TOKENS — a shape the census counts but \
             no lane can ever claim. Produced: {produced:?}"
        );
    }

    // SET EQUALITY against the RULE TABLE, not against the hand-maintained
    // representative list (roborev jobs 56 and 59).
    //
    // Job 56 replaced membership with equality; job 59 then showed the
    // equality did not catch the case its comment named. `FACT_KIND_RULES`
    // is now read directly, which is what makes the claim TRUE: a new rule
    // adds a family token to `reachable`, so the assertion FAILs until
    // `KNOWN_COVERAGE_TOKENS` gains it too. Against a `match`, a new arm
    // changed NEITHER side — `produced` carries only shape tokens (the
    // family half is folded in later, in `assert_raw_view_matches_golden`)
    // and the representative list is a literal a new arm does not touch —
    // so a new family could become silently unclaimable: the census would
    // bump it while `require_observed` rejected every claim for it as an
    // unknown name. That is exactly the under-coverage this assertion is
    // supposed to forbid.
    // BOTH halves are derived from declarations the producing code itself
    // reads (roborev job 61): families from `FACT_KIND_RULES`, shapes from
    // `SHAPE_TOKENS`, which `bump` asserts membership against as it counts.
    //
    // `produced` is deliberately NOT seeded into `reachable` (roborev job
    // 62): `bump` now guarantees `produced ⊆ SHAPE_TOKENS`, so seeding it
    // would contribute nothing while reading as though the synthetic
    // goldens still gate the shape side. Their job is the SEPARATE liveness
    // loop below — no token may be declared that nothing produces.
    let mut reachable: std::collections::BTreeSet<&str> =
        FACT_KIND_RULES.iter().map(|(_, kind)| *kind).collect();
    reachable.extend(SHAPE_TOKENS.iter().copied());

    // The synthetic goldens must still actually produce the shapes they
    // declare, so `SHAPE_TOKENS` cannot become a list of aspirations.
    for shape in SHAPE_TOKENS {
        assert!(
            produced.contains(shape),
            "issue #4309: SHAPE_TOKENS declares '{shape}' but no synthetic golden in this \
             lane produces it, so its derivation is unexercised on the gate. Produced: \
             {produced:?}"
        );
    }
    let vocabulary: std::collections::BTreeSet<&str> =
        KNOWN_COVERAGE_TOKENS.iter().copied().collect();
    assert_eq!(
        reachable,
        vocabulary,
        "issue #4309: the tokens the census can PRODUCE — every FACT_KIND_RULES family \
         plus every shape the synthetic goldens bump — must be exactly \
         KNOWN_COVERAGE_TOKENS. In the vocabulary but unproducible: {:?} (a token no lane \
         could ever observe, so any claim for it is unsatisfiable). Producible but NOT in \
         the vocabulary: {:?} (the census counts it while require_observed rejects every \
         claim for it as an unknown name — a silently unclaimable family, which is the \
         under-coverage this census exists to forbid).",
        vocabulary.difference(&reachable).collect::<Vec<_>>(),
        reachable.difference(&vocabulary).collect::<Vec<_>>(),
    );
}
