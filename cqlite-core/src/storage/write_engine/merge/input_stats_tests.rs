//! `InputStats::subset` keeps the caller's order and duplicates and shares the resolved sources.

use super::*;

fn paths(names: &[&str]) -> Vec<PathBuf> {
    names
        .iter()
        .map(|n| PathBuf::from(format!("/nonexistent/{n}")))
        .collect()
}

fn order(inputs: &InputStats) -> Vec<PathBuf> {
    inputs.iter().map(|i| i.path().to_path_buf()).collect()
}

#[test]
fn subset_follows_the_requested_order_and_duplicates() {
    let all = InputStats::resolve(&paths(&["a-Data.db", "b-Data.db", "c-Data.db"]));
    let want = paths(&["c-Data.db", "a-Data.db", "c-Data.db"]);
    assert_eq!(order(&all.subset(&want)), want);
}

#[test]
fn subset_resolves_paths_it_has_not_seen_in_position() {
    let all = InputStats::resolve(&paths(&["a-Data.db"]));
    let want = paths(&["z-Data.db", "a-Data.db", "y-Data.db"]);
    assert_eq!(order(&all.subset(&want)), want);
}

#[test]
fn subset_shares_the_cached_read_of_a_direct_input() {
    let all = InputStats::resolve(&paths(&["a-Data.db"]));
    let first = all.iter().next().unwrap().statistics().unwrap_err();
    assert!(first.is_absent());
    let sub = all.subset(&paths(&["a-Data.db", "a-Data.db"]));
    let (Source::Direct { cell: a, .. }, Source::Direct { cell: b, .. }) =
        (&all.inputs[0].1, &sub.inputs[0].1)
    else {
        panic!("unlisted inputs are direct");
    };
    assert!(Arc::ptr_eq(a, b) && a.get().is_some());
}

#[test]
fn a_name_without_data_db_is_absent_not_skipped_silently_as_valid() {
    let all = InputStats::resolve(&[PathBuf::from("/nonexistent/readme.txt")]);
    assert!(all
        .iter()
        .next()
        .unwrap()
        .statistics()
        .unwrap_err()
        .is_absent());
}
