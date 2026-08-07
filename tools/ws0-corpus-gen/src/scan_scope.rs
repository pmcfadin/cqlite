//! ARM A's INGEST SCOPE: was the directory set ingestion actually selected the one intended?
//! (issue #3272 round 10, F-B)
//!
//! # Why this is a module and not four lines inside `scan_bench.rs`
//!
//! The check has to be OBSERVABLE. Its two refusals are — correctly — hard to provoke through the
//! CLI once [`cqlite_core::ingestion::TableDirSelection::Exact`] is in use: `Exact` compares
//! complete path components, so on every layout reachable from a shell it selects exactly the
//! requested directory or nothing. That is the point of using it, and it is also what would make a
//! guard living inside the binary's `run()` **unfireable by any test** — an assertion nobody can
//! watch fail, which is this issue's own governing defect (an instrument reporting success without
//! having measured).
//!
//! So the predicate is a pure function over two path sets, unit-tested on both branches, and the
//! binary CALLS it. The `Exact` selection is the primary mechanism; this is the affirmative
//! verification that the mechanism did what was asked, kept honest by being testable.
//!
//! # What it refuses, and why each is fatal to a MEASUREMENT rather than merely untidy
//!
//! * A **FOREIGN** directory (one that is not the intended table dir). Arm A's whole purpose is to
//!   be the same-session comparator arm B's figures are divided by, and the two arms reach
//!   ingestion by different routes — so a directory absorbed here and not there means the ratio
//!   compares two different corpora. An extra directory also changes the GENERATION COUNT, and the
//!   generation count selects the scan route.
//! * An **EMPTY** selection when the caller believed there was something to select. Nothing was
//!   ingested, so the scan observes nothing while every figure derived from it claims a full-corpus
//!   scan.

use std::path::{Path, PathBuf};

/// Complete-path-component identity: canonicalize both sides and compare.
///
/// `Path`'s `Eq` is component-wise, so this is never a substring or prefix comparison — the exact
/// property that makes `<table>-<uuid>` distinct from `<table>-<uuid>-backup`. A path that cannot
/// be canonicalized matches nothing: fail-closed, so an unresolvable path can never widen the
/// accepted set (it lands in `foreign` and is reported).
fn same_dir(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// `Ok(())` only when `selected` is EXACTLY `[intended]` by path-component identity.
///
/// `expect_non_empty` states whether the caller has independently established that there is
/// something to ingest (arm A checks for `*-Data.db` before opening anything). When it is true an
/// empty selection is a refusal; when false, an empty selection is legitimately empty — a
/// distinction kept explicit rather than inferred, so neither case takes the other's branch.
pub fn verify_exact_scope(
    selected: &[PathBuf],
    intended: &Path,
    expect_non_empty: bool,
) -> Result<(), String> {
    let foreign: Vec<String> = selected
        .iter()
        .filter(|d| !same_dir(d, intended))
        .map(|d| d.display().to_string())
        .collect();
    if !foreign.is_empty() {
        return Err(format!(
            "ingestion selected {} table directory/ies OUTSIDE {}:\n    {}\n  Arm A must measure \
             EXACTLY the bytes arm B does, so an unintended union voids the cross-arm ratio that \
             is this rig's only output — and the generation count selects the scan route. A \
             SUBSTRING filter of `/<ks>/<table>` is how this happens: it also matches any sibling \
             whose full name EXTENDS it (`<table>-backup`, `<table>-<uuid>-backup`). Refusing \
             (issue #3272 round 10, F-B).",
            foreign.len(),
            intended.display(),
            foreign.join("\n    ")
        ));
    }
    if expect_non_empty && selected.is_empty() {
        return Err(format!(
            "ingestion selected NO table directory, yet {} was established to hold at least one \
             `*-Data.db`. Nothing was ingested, so the scan would observe ZERO rows while every \
             figure derived from it claims a full-corpus scan. Refusing (issue #3272 round 10, \
             F-B).",
            intended.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real directory, because the predicate canonicalizes and a non-existent path is
    /// deliberately no-one's equal.
    fn dir(root: &Path, name: &str) -> PathBuf {
        let p = root.join(name);
        std::fs::create_dir_all(&p).expect("mkdir");
        p
    }

    /// THE ACCEPT CASE. Without it the two refusals below would be satisfied by a function that
    /// refuses everything.
    #[test]
    fn exactly_the_intended_directory_is_accepted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        assert!(verify_exact_scope(std::slice::from_ref(&intended), &intended, true).is_ok());
    }

    /// The accept case reached by a DIFFERENT SPELLING of the same directory — so the predicate is
    /// path-component identity after canonicalization, not string equality.
    #[test]
    fn the_same_directory_reached_by_another_spelling_is_accepted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        let indirect = tmp.path().join("ws0").join(".").join("events");
        assert!(
            verify_exact_scope(&[indirect], &intended, true).is_ok(),
            "`./` and `..` in a path do not make it a different directory"
        );
    }

    /// REFUSAL 1, THE SUBSTRING-FILTER SHAPE: a sibling whose full name EXTENDS the intended one.
    ///
    /// This is the exact set `table_directory_filter: Some("/ws0/events")` produced pre-fix
    /// (measured: it selected both `…/ws0/events` and `…/ws0/events-backup`). Asserting it here
    /// makes the refusal OBSERVABLE, which it is not through the CLI once `Exact` is in use — and
    /// a guard nobody can watch fail is the defect this issue is about.
    #[test]
    fn a_name_extending_sibling_in_the_selection_is_refused_by_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        let sibling = dir(tmp.path(), "ws0/events-backup");

        let err = verify_exact_scope(&[intended.clone(), sibling.clone()], &intended, true)
            .expect_err("a foreign directory must be refused");
        assert!(
            err.contains("events-backup"),
            "the refusal must NAME the offending directory, so an operator can act on it: {err}"
        );
        assert!(
            err.contains("OUTSIDE"),
            "and state the relation that was violated: {err}"
        );
    }

    /// The same refusal for the `<table>-<uuid>`-suffixed shape a Cassandra-style layout produces.
    #[test]
    fn a_uuid_suffixed_sibling_is_refused_too() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        let sibling = dir(tmp.path(), "ws0/events-71a911f1000000000000000000000000");
        assert!(verify_exact_scope(&[intended.clone(), sibling], &intended, true).is_err());
    }

    /// A PREFIX relation is not identity in the other direction either: the intended directory
    /// being a name-extension of a selected one is equally foreign.
    #[test]
    fn a_selected_directory_the_intended_one_extends_is_refused() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events-2");
        let shorter = dir(tmp.path(), "ws0/events");
        assert!(verify_exact_scope(&[shorter], &intended, true).is_err());
    }

    /// REFUSAL 2: an EMPTY selection when the caller established there was data to ingest.
    #[test]
    fn an_empty_selection_is_refused_when_data_was_established() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        let err = verify_exact_scope(&[], &intended, true)
            .expect_err("an empty selection over a corpus with data must be refused");
        assert!(
            err.contains("ZERO rows"),
            "the refusal must say what the measurement would have been, not just that a list was \
             empty: {err}"
        );
    }

    /// ...and the SAME empty selection is ACCEPTED when nothing was established to ingest, so the
    /// two states stay distinct rather than one silently taking the other's branch.
    #[test]
    fn an_empty_selection_is_accepted_when_nothing_was_established() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        assert!(verify_exact_scope(&[], &intended, false).is_ok());
    }

    /// An UNRESOLVABLE selected path is FOREIGN, never silently accepted.
    ///
    /// The fail-closed direction: `canonicalize` failing is an unmeasured state, and an unmeasured
    /// state must not take the permissive branch (#3272). A selected directory that has vanished
    /// cannot be shown to be the intended one, so it is reported.
    #[test]
    fn an_unresolvable_selected_path_is_refused_not_assumed_equal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let intended = dir(tmp.path(), "ws0/events");
        let gone = tmp.path().join("ws0").join("vanished");
        assert!(
            !gone.exists(),
            "the fixture path must genuinely not exist for this to be the unresolvable case"
        );
        assert!(verify_exact_scope(&[gone], &intended, true).is_err());
    }
}
