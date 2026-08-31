//! The PAIRING rule and the committed-ORACLE rule, pinned against synthetic
//! listings (issue #1491 review findings T2 and BB1).
//!
//! Both rules read `git ls-files` under the compile-time checkout anchor with no
//! environment override (`fixture_root::committed_listing`), so a scratch tree
//! cannot be substituted for the repository and neither rule can be demonstrated by
//! synthesizing files — that would mean staging fixtures in the real checkout, which
//! this lane never does. They are pinned here instead, over listings the checkout
//! does not contain.
//!
//! What the real tree does and does not carry, measured when BB1 landed: it tracks
//! 32 committed `*-Data.db`, and ALL 32 pair with a git-tracked
//! `*-Data.db.jsonl` golden — so no committed case is certified by an untracked
//! oracle today and the BB1 shape below is LATENT. The orphan shape IS real
//! (`test_deltas.static_with_rows` tracks goldens whose SSTable is not committed),
//! but the near miss it would license is not.

use super::*;
use std::collections::BTreeMap;

fn listing(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|l| (*l).to_string()).collect()
}

const DIR: &str = "test-data/datasets/sstables/ks/t-abc";

/// Only the golden named `<the tracked SSTable>.jsonl` verifies a claim about that
/// fixture. The orphan beside it is NAMED, because a table git does track an SSTable
/// for is exactly where an orphan could have supplied an exclusion's evidence.
#[test]
fn only_the_golden_paired_with_a_tracked_sstable_verifies_anything() {
    let tracked = committed_set(&listing(&[
        &format!("{DIR}/nb-2-big-Data.db"),
        &format!("{DIR}/nb-2-big-Data.db.jsonl"),
        // The orphan: no `nb-1-big-Data.db` is tracked.
        &format!("{DIR}/nb-1-big-Data.db.jsonl"),
    ]))
    .expect("classifies");

    assert_eq!(
        tracked.goldens,
        BTreeMap::from([(
            FixtureId::new("ks", "t", "t-abc", "nb-2-big-Data.db"),
            format!("{DIR}/nb-2-big-Data.db.jsonl")
        )]),
        "the paired golden, and only it, may verify a claim about nb-2"
    );
    assert_eq!(
        tracked.orphans_of_tracked_tables,
        vec![format!("ks.t: {DIR}/nb-1-big-Data.db.jsonl")],
        "the orphan beside a tracked table is named"
    );
    assert_eq!(tracked.orphans_of_untracked_tables, 0);
    assert_eq!(
        tracked.fixtures(),
        vec![FixtureId::new("ks", "t", "t-abc", "nb-2-big-Data.db")]
    );
}

/// A golden in a DIFFERENT fixture directory of the same table is an orphan too:
/// the pairing is per directory, exactly as `compare::golden_path` resolves it (it
/// reads one fixture directory and requires the golden beside the SSTable).
#[test]
fn the_pairing_is_per_directory_not_per_table() {
    let tracked = committed_set(&listing(&[
        &format!("{DIR}/nb-1-big-Data.db"),
        "test-data/datasets/sstables/ks/t-def/nb-1-big-Data.db.jsonl",
    ]))
    .expect("classifies");

    assert!(
        tracked.goldens.is_empty(),
        "a same-named golden in another directory does not describe this SSTable"
    );
    assert_eq!(
        tracked.orphans_of_tracked_tables,
        vec!["ks.t: test-data/datasets/sstables/ks/t-def/nb-1-big-Data.db.jsonl".to_string()],
    );
}

/// A golden for a table git tracks no SSTable for is not a near miss — it is
/// counted, not named, so the census line stays readable. Both counters are
/// asserted, so neither can absorb the other.
#[test]
fn a_golden_for_an_untracked_table_is_counted_and_not_named() {
    let tracked = committed_set(&listing(&[
        &format!("{DIR}/nb-1-big-Data.db"),
        &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        "test-data/datasets/sstables/system/local-abc/nb-1-big-Data.db.jsonl",
    ]))
    .expect("classifies");

    assert!(tracked.orphans_of_tracked_tables.is_empty());
    assert_eq!(tracked.orphans_of_untracked_tables, 1);
    assert_eq!(
        tracked.fixtures(),
        vec![FixtureId::new("ks", "t", "t-abc", "nb-1-big-Data.db")],
        "a table with only a golden is not a committed fixture"
    );
}

/// And a path shape the classifier refuses is an ERROR here, not a skip — no
/// caller can report on a listing it only partly understood.
#[test]
fn an_unrecognised_fixture_path_is_refused() {
    let why = committed_set(&listing(&[
        "test-data/datasets/sstables/ks/nb-1-big-Data.db",
    ]))
    .expect_err("an unrecognised path shape must be refused");
    assert!(!why.is_empty(), "the refusal must state a reason");
}

/// BB1, the negative: a golden git does NOT track may not be the oracle for a
/// committed case, even though it sits in the tracked directory beside the tracked
/// `*-Data.db` and is exactly what `compare::golden_path` finds there.
///
/// This is the whole finding: trackedness of the fixture was established and
/// trackedness of the ORACLE was not, so a fetched corpus copy, a stray local file
/// or a previous run's leftover could certify a committed case.
#[test]
fn an_untracked_golden_beside_a_tracked_sstable_may_not_be_the_oracle() {
    let checkout = Path::new("/checkout/test-data/datasets/sstables");
    // git tracks the SSTable and NOT its golden.
    let set = committed_set(&listing(&[&format!("{DIR}/nb-1-big-Data.db")])).expect("classifies");
    let resolved = checkout.join("ks/t-abc/nb-1-big-Data.db.jsonl");

    let why = require_tracked_oracle(&set, "ks", "t", checkout, &resolved)
        .expect_err("an untracked golden must be refused as a committed case's oracle");
    assert!(
        why.contains("nb-1-big-Data.db.jsonl") && why.contains("NOT git-tracked"),
        "the refusal must name the path and say it is untracked: {why}"
    );
}

/// BB1, the positive: with the golden tracked, the path the filesystem resolved IS
/// the git-tracked oracle and the check passes — so the failure above is
/// attributable to trackedness and not to the scaffolding.
#[test]
fn the_tracked_golden_paired_with_the_tracked_sstable_is_the_oracle() {
    let checkout = Path::new("/checkout/test-data/datasets/sstables");
    let set = committed_set(&listing(&[
        &format!("{DIR}/nb-1-big-Data.db"),
        &format!("{DIR}/nb-1-big-Data.db.jsonl"),
    ]))
    .expect("classifies");
    let resolved = checkout.join("ks/t-abc/nb-1-big-Data.db.jsonl");

    require_tracked_oracle(&set, "ks", "t", checkout, &resolved)
        .expect("the git-tracked golden paired with the tracked SSTable is the oracle");
}

/// A tracked golden for the SELECTED generation does not license comparing ANOTHER
/// generation's golden: an untracked `nb-2` pair on disk (the tracked `nb-1` deleted
/// from the working tree, say) is refused with both paths named.
#[test]
fn a_golden_describing_another_generation_is_not_the_oracle() {
    let checkout = Path::new("/checkout/test-data/datasets/sstables");
    let set = committed_set(&listing(&[
        &format!("{DIR}/nb-1-big-Data.db"),
        &format!("{DIR}/nb-1-big-Data.db.jsonl"),
    ]))
    .expect("classifies");
    let resolved = checkout.join("ks/t-abc/nb-2-big-Data.db.jsonl");

    let why = require_tracked_oracle(&set, "ks", "t", checkout, &resolved)
        .expect_err("another generation's golden must be refused");
    assert!(
        why.contains("nb-2-big-Data.db.jsonl") && why.contains("nb-1-big-Data.db.jsonl"),
        "the refusal must name the golden read and the one git tracks: {why}"
    );
}

/// And a table git tracks no `*-Data.db` for has no committed oracle at all — the
/// same verdict `fixture_root::committed_fixture_dir` gives its fixture, so the two
/// halves cannot disagree about whether a table is committed.
#[test]
fn a_table_git_tracks_no_sstable_for_has_no_committed_oracle() {
    let checkout = Path::new("/checkout/test-data/datasets/sstables");
    let set =
        committed_set(&listing(&[&format!("{DIR}/nb-1-big-Data.db.jsonl")])).expect("classifies");

    let why = require_tracked_oracle(&set, "ks", "t", checkout, Path::new("/nowhere"))
        .expect_err("a table with no tracked *-Data.db has no committed oracle");
    assert!(
        why.contains("tracks no *-Data.db"),
        "the refusal must say what is missing: {why}"
    );
}
