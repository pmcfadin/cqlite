//! Fixture-driven self-tests for the `oom-audit` STREAM_RETURNS_VEC rule and
//! allowlist (issue #2012). These are the public-surface acceptance evidence for
//! the spec scenarios: violating -> finding, bounded -> none, renamed -> still
//! caught, out-of-scope -> not analyzed, and the allowlist/enforce behaviors.

use std::path::Path;

use xtask::oom_audit::allowlist::{Allowlist, AllowlistProblem};
use xtask::oom_audit::rule::{analyze_file, Finding};
use xtask::oom_audit::scope;

fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

// A representative in-scope path used to attribute fixture findings.
const SCOPED: &str = "cqlite-core/src/query/select_executor/scan.rs";

fn analyze(name: &str) -> Vec<Finding> {
    analyze_file(SCOPED, &fixture(name)).expect("fixture must parse")
}

#[test]
fn violating_fixture_produces_findings() {
    let findings = analyze("violating.rs.txt");
    // Both the collect shape and the push-loop shape must fire.
    assert!(
        findings.len() >= 2,
        "expected >=2 findings (collect + push-loop), got {}: {findings:?}",
        findings.len()
    );
    assert!(findings.iter().all(|f| f.rule == "STREAM_RETURNS_VEC"));
    assert!(findings.iter().any(|f| f.function == "scan_all_partitions"));
    assert!(findings.iter().any(|f| f.function == "scan_via_push_loop"));
}

#[test]
fn bounded_fixture_produces_no_findings() {
    let findings = analyze("bounded.rs.txt");
    assert!(
        findings.is_empty(),
        "bounded fixture must be clean, got {findings:?}"
    );
}

#[test]
fn renamed_helper_with_same_shape_is_still_caught() {
    // Spec: matches on parsed shape, not identifier text.
    let original = analyze("violating.rs.txt");
    let renamed = analyze("violating_renamed.rs.txt");
    assert!(
        renamed.iter().any(|f| f.function == "scan_the_whole_thing"),
        "renamed helper must still be flagged, got {renamed:?}"
    );
    // The renamed collect shape matches the original collect shape's fingerprint
    // only when the tokens are identical; here the element type `DataRow` and the
    // collect turbofish are preserved, so the RULE fires regardless of the local
    // variable names — that is the property under test.
    assert!(original.iter().any(|f| f.function == "scan_all_partitions"));
}

#[test]
fn out_of_scope_path_is_not_analyzed() {
    // Scope is enforced by path, not content. The same violating body under an
    // out-of-scope path must yield no finding in a full walk. We assert the
    // path predicate directly (the walk consults `in_scope`).
    assert!(!scope::in_scope("cqlite-core/src/storage/mod.rs"));
    assert!(!scope::in_scope("cqlite-flight/src/service.rs"));
    assert!(scope::in_scope(SCOPED));
}

#[test]
fn allowlist_fingerprint_suppresses_a_finding() {
    let findings = analyze("violating.rs.txt");
    let target = &findings[0];
    let toml_src = format!(
        r##"
[[allow]]
file = "{}"
fn = "{}"
fingerprint = "{}"
issue = "#2012"
justification = "self-test: reviewed sound"
"##,
        target.file, target.function, target.fingerprint
    );
    let al = Allowlist::parse(&toml_src).unwrap();
    let (remaining, orphans) = al.apply(&findings);
    assert!(orphans.is_empty(), "matched entry must not orphan");
    assert!(
        remaining
            .iter()
            .all(|f| f.fingerprint != target.fingerprint),
        "the allowlisted fingerprint must be suppressed"
    );
}

#[test]
fn orphaned_allowlist_entry_is_reported() {
    let findings = analyze("violating.rs.txt");
    let toml_src = r##"
[[allow]]
file = "cqlite-core/src/query/gone.rs"
fn = "removed_fn"
fingerprint = "f1:deadbeefdeadbeef"
issue = "#2012"
justification = "code was deleted; entry should now orphan"
"##;
    let al = Allowlist::parse(toml_src).unwrap();
    let (_remaining, orphans) = al.apply(&findings);
    assert_eq!(orphans.len(), 1);
    assert!(matches!(orphans[0], AllowlistProblem::Orphaned { .. }));
}
