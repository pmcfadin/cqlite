//! Tests for [`crate::commands::sweep`] (issue #4194).
//!
//! Split out of `sweep.rs` under the campsite rule (#1135): the production
//! code is ~740 lines, and carrying the 250-line inline test module pushed the
//! file past the gate's 800-line `src` threshold. Kept as a `#[path]` CHILD
//! module rather than an integration test under `cqlite-cli/tests/` because
//! these cases exercise private seams -- `classify_table_dir_entries`,
//! `descendable_dir`, `mixed_readability_row`, `classify_report` -- which a
//! `tests/` file cannot reach.
use super::*;

fn finding(class: VerifyErrorClass) -> VerifyFinding {
    VerifyFinding {
        class,
        component: "Data.db".to_string(),
        detail: "synthetic".to_string(),
        location: None,
    }
}

// roborev job 4376: `classify_report`'s Corrupt arm must name a REAL
// corruption cause, not merely the first finding recorded — a
// `FilterFalseNegative` ahead of a genuine corruption finding in the
// same (non-FilterFalseNegative-only) report must not become the row's
// cause.
#[test]
fn classify_report_corrupt_cause_skips_a_leading_filter_false_negative() {
    let findings = vec![
        finding(VerifyErrorClass::FilterFalseNegative),
        finding(VerifyErrorClass::RowScanFailed),
    ];
    let (severity, cause) = classify_report(&findings);
    assert_eq!(severity, Severity::Corrupt);
    assert_eq!(
        cause.as_deref(),
        Some(VerifyErrorClass::RowScanFailed.code())
    );
}

// Unchanged behavior: FilterFalseNegative-only stays Degraded, its cause
// still names FilterFalseNegative (the ONLY finding present).
#[test]
fn classify_report_filter_false_negative_only_is_degraded() {
    let findings = vec![finding(VerifyErrorClass::FilterFalseNegative)];
    let (severity, cause) = classify_report(&findings);
    assert_eq!(severity, Severity::Degraded);
    assert_eq!(
        cause.as_deref(),
        Some(VerifyErrorClass::FilterFalseNegative.code())
    );
}

// Unchanged behavior: a Corrupt report with no FilterFalseNegative at all
// still names its first finding.
#[test]
fn classify_report_corrupt_cause_is_first_finding_when_no_filter_false_negative() {
    let findings = vec![
        finding(VerifyErrorClass::RowScanFailed),
        finding(VerifyErrorClass::DigestMismatch),
    ];
    let (severity, cause) = classify_report(&findings);
    assert_eq!(severity, Severity::Corrupt);
    assert_eq!(
        cause.as_deref(),
        Some(VerifyErrorClass::RowScanFailed.code())
    );
}

// roborev job 4376: a table directory can hold BOTH readable
// `*-Data.db` generations AND an unreadable directory entry in the SAME
// `read_dir` pass — the count must not be silently dropped just because
// `data_dbs` ended up non-empty. Injects a failing entry directly (a
// `DirEntry` has no public constructor to synthesize one for real).
#[test]
fn table_dir_entries_unreadable_alongside_readable_generations_is_counted() {
    let dir = std::env::temp_dir().join(format!(
        "cqlite-sweep-unittest-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let data_db = dir.join("nb-1-big-Data.db");
    std::fs::write(&data_db, b"").expect("create Data.db stand-in");

    let entries = vec![
        Ok(data_db.clone()),
        Err(std::io::Error::other("injected failure")),
    ];
    let (data_dbs, unreadable_file_entries, last_file_entry_error) =
        classify_table_dir_entries(entries.into_iter());
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(data_dbs, vec![data_db]);
    assert_eq!(unreadable_file_entries, 1);
    assert_eq!(last_file_entry_error.as_deref(), Some("injected failure"));
    // NO tautological restatement here (roborev job 92 MEDIUM): asserting
    // `!data_dbs.is_empty() && unreadable_file_entries > 0` is implied by
    // the two assert_eq!s above and observes nothing. The branch those
    // counts drive is exercised directly in
    // `mixed_readability_row_*` below.
}

// descendable_dir: the directory-level fail-closed walk (roborev job 102
// MEDIUM). The dangerous case is the Err arm -- a dropped directory used
// to leave the sweep at exit 0.
#[test]
fn descendable_dir_descends_a_real_directory() {
    let dir = std::env::temp_dir();
    assert_eq!(descendable_dir(&dir), Ok(true));
}

#[test]
fn descendable_dir_skips_a_genuine_non_directory() {
    let f = std::env::temp_dir().join(format!("cqlite-4194-nd-{}", std::process::id()));
    std::fs::write(&f, b"x").expect("write temp file");
    let got = descendable_dir(&f);
    let _ = std::fs::remove_file(&f);
    assert_eq!(
        got,
        Ok(false),
        "a stray file is a silent skip, not an unreadable entry"
    );
}

#[test]
fn descendable_dir_reports_an_unstattable_path_as_a_named_cause() {
    let missing = PathBuf::from("/nonexistent-cqlite-4194/ks-dir");
    let err = descendable_dir(&missing)
        .expect_err("an unstattable directory must be an Err, never a silent false");
    assert!(
        err.contains("cannot stat") && err.contains("ks-dir"),
        "the cause must name the failure and the path: {err}"
    );
}

// The branch the classifier feeds (roborev job 92 MEDIUM). These four
// cases pin the GUARD and the CAUSE STRING, so the reviewer's named
// regression -- inverting the guard to `found_count == 0` -- FAILs here
// instead of passing silently.
#[test]
fn mixed_readability_row_fires_when_readable_and_unreadable_coexist() {
    let row = mixed_readability_row(Path::new("/ks/tbl-abc"), 3, 2, Some("injected failure"));
    let (path, cause) = row.expect("readable generations + unreadable entries must push a row");
    assert_eq!(path, PathBuf::from("/ks/tbl-abc"));
    // Both counts and the underlying error are NAMED, so the row explains
    // itself rather than just flipping the exit code.
    assert!(
        cause.contains('2'),
        "cause must name the unreadable count: {cause}"
    );
    assert!(
        cause.contains('3'),
        "cause must name the readable count: {cause}"
    );
    assert!(
        cause.contains("injected failure"),
        "cause must carry the last error: {cause}"
    );
    assert!(
        cause.contains("still verified"),
        "cause must say the readable generations are not skipped: {cause}"
    );
}

#[test]
fn mixed_readability_row_is_none_when_nothing_is_unreadable() {
    assert!(mixed_readability_row(Path::new("/ks/tbl-abc"), 3, 0, None).is_none());
}

#[test]
fn mixed_readability_row_is_none_when_no_generation_was_found() {
    // found_count == 0 is the OTHER arm's job (the "no *-Data.db" cause),
    // so this must not double-report.
    assert!(
        mixed_readability_row(Path::new("/ks/tbl-abc"), 0, 2, Some("boom")).is_none(),
        "an empty generation set is reported by the is_empty arm, not here"
    );
}

#[test]
fn mixed_readability_row_tolerates_a_missing_last_error() {
    let (_, cause) = mixed_readability_row(Path::new("/ks/tbl-abc"), 1, 1, None)
        .expect("guard depends on the counts, not on an error being present");
    assert!(
        cause.contains('1'),
        "cause must still name the counts: {cause}"
    );
}

// A `*-Data.db` NAME whose metadata cannot be read is an UNREADABLE entry,
// never a silent drop (roborev job 92 MEDIUM). A path under a directory
// that does not exist cannot be stat'ed, which is the portable way to
// provoke the Err arm without planting a symlink.
#[test]
fn classify_counts_an_unstattable_data_db_as_unreadable() {
    let missing = PathBuf::from("/nonexistent-cqlite-4194/ks/tbl/nb-1-big-Data.db");
    let (data_dbs, unreadable, last_err) =
        classify_table_dir_entries(vec![Ok(missing)].into_iter());
    assert!(
        data_dbs.is_empty(),
        "an unstattable entry is not a usable generation"
    );
    assert_eq!(
        unreadable, 1,
        "it must be COUNTED, not dropped: that silent drop is the finding"
    );
    let cause = last_err.expect("the stat failure must be recorded with a cause");
    assert!(
        cause.contains("cannot stat") && cause.contains("nb-1-big-Data.db"),
        "cause must name the stat failure and the file: {cause}"
    );
}

// A non-Data.db entry that cannot be stat'ed is NOT our business: the
// sweep only claims completeness over `*-Data.db` names, so counting
// unrelated entries would inflate the unreadable count.
#[test]
fn classify_ignores_a_non_data_db_entry_entirely() {
    let other = PathBuf::from("/nonexistent-cqlite-4194/ks/tbl/nb-1-big-Index.db");
    let (data_dbs, unreadable, last_err) = classify_table_dir_entries(vec![Ok(other)].into_iter());
    assert!(data_dbs.is_empty());
    assert_eq!(
        unreadable, 0,
        "a non-Data.db name is out of scope, not unreadable"
    );
    assert!(last_err.is_none());
}

// Clean case: no unreadable entries at all yields an empty last-error and
// a zero count, so the call site's guard never fires.
#[test]
fn table_dir_entries_all_readable_reports_zero_unreadable() {
    let dir = std::env::temp_dir().join(format!(
        "cqlite-sweep-unittest-clean-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let data_db = dir.join("nb-1-big-Data.db");
    std::fs::write(&data_db, b"").expect("create Data.db stand-in");

    let entries = vec![Ok(data_db.clone())];
    let (data_dbs, unreadable_file_entries, last_file_entry_error) =
        classify_table_dir_entries(entries.into_iter());
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(data_dbs, vec![data_db]);
    assert_eq!(unreadable_file_entries, 0);
    assert_eq!(last_file_entry_error, None);
}
