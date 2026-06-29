//! Tests for the machine-enforced "workflow actually runs the mapped test"
//! check (issue #1228).

use cassandra_parity::workflow_check::{
    check_scenario, is_java_test, rust_test_target, workflow_is_fail_closed, workflow_runs_gradle,
    workflow_runs_test,
};

#[test]
fn rust_test_target_extracts_integration_target() {
    assert_eq!(
        rust_test_target("cqlite-core/tests/issue_997_compressioninfo_parity.rs"),
        Some("issue_997_compressioninfo_parity")
    );
    // src/ paths are not integration targets
    assert_eq!(
        rust_test_target("cqlite-core/src/storage/sstable/reader/parsing/x.rs"),
        None
    );
    // nested module under tests/ is not a top-level integration target
    assert_eq!(
        rust_test_target("cqlite-core/tests/common/helpers.rs"),
        None
    );
    // non-rust
    assert_eq!(rust_test_target("foo/bar/X.java"), None);
}

#[test]
fn java_test_detected() {
    assert!(is_java_test(
        "compaction-parity/src/test/java/org/cqlite/parity/BasicDifferentialTest.java"
    ));
    assert!(!is_java_test("cqlite-core/tests/foo.rs"));
}

#[test]
fn fail_closed_flag_detection() {
    assert!(workflow_is_fail_closed(
        "env CQLITE_REQUIRE_FIXTURES=1 cargo test"
    ));
    assert!(workflow_is_fail_closed(
        "CQLITE_PARITY_REQUIRE_DATASETS: '1'"
    ));
    assert!(!workflow_is_fail_closed("cargo test -p cassandra-parity"));
}

#[test]
fn gradle_detection() {
    assert!(workflow_runs_gradle("gradle --no-daemon byteParity"));
    assert!(!workflow_runs_gradle("cargo test --test foo"));
}

#[test]
fn runs_test_matches_whole_token_across_line_continuations() {
    let wf = "          cargo test -p cqlite-core \\\n            --test issue_997_compressioninfo_parity \\\n            --test issue_998_inline_crc_trailers";
    assert!(workflow_runs_test(wf, "issue_997_compressioninfo_parity"));
    assert!(workflow_runs_test(wf, "issue_998_inline_crc_trailers"));
    // must not match a prefix of a longer target name
    assert!(!workflow_runs_test(wf, "issue_997"));
    assert!(!workflow_runs_test(wf, "issue_999_missing"));
}

#[test]
fn lint_only_workflow_fails_byte_scenario() {
    // A scenario mapping to a real Rust test, pointed at a workflow that only
    // lints the manifest (no --test, no fail-closed flag) must be flagged.
    let lint_only_wf = "name: Cassandra Parity Manifest\nsteps:\n  - run: cargo run -p cassandra-parity -- lint\n  - run: cargo test -p cassandra-parity\n";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/cassandra-parity.yml",
        lint_only_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.field == "ci.workflow" && f.message.contains("never runs the mapped test")),
        "expected an overstated finding, got: {findings:#?}"
    );
}

#[test]
fn wrong_test_workflow_fails() {
    // delta-roundtrip.yml runs delta_roundtrip_tests, NOT scan_delta_parity_test.
    let delta_roundtrip_wf =
        "env:\n  DELTA_ROUNDTRIP_DATA: /tmp/x\nsteps:\n  - run: cargo test --test delta_roundtrip_tests";
    let findings = check_scenario(
        "cass.delta_scan.cell_tombstones",
        ".github/workflows/delta-roundtrip.yml",
        delta_roundtrip_wf,
        &["cqlite-core/tests/scan_delta_parity_test.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("scan_delta_parity_test")),
        "expected a wrong-test finding, got: {findings:#?}"
    );
}

#[test]
fn correctly_wired_workflow_passes() {
    let good_wf = "env:\n  CQLITE_PARITY_REQUIRE_DATASETS: '1'\nsteps:\n  - run: |\n      cargo test --package cqlite-core --features write-support,cli-helpers \\\n        --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/sstabledump-parity-gate.yml",
        good_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(findings.is_empty(), "expected clean, got: {findings:#?}");
}

#[test]
fn fail_open_workflow_for_rust_test_is_flagged() {
    // A workflow that runs the right --test but arms no fail-closed flag is still
    // overstated (a vanished dataset could silently green it).
    let fail_open_wf = "steps:\n  - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        fail_open_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding, got: {findings:#?}"
    );
}

#[test]
fn java_harness_workflow_requires_gradle() {
    let no_gradle = "steps:\n  - run: echo nothing";
    let findings = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        no_gradle,
        &[
            "compaction-parity/src/test/java/org/cqlite/parity/BasicDifferentialTest.java"
                .to_string(),
        ],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("gradle")),
        "expected a gradle finding, got: {findings:#?}"
    );

    let with_gradle = "steps:\n  - run: gradle --no-daemon byteParity";
    let ok = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        with_gradle,
        &[
            "compaction-parity/src/test/java/org/cqlite/parity/BasicDifferentialTest.java"
                .to_string(),
        ],
    );
    assert!(ok.is_empty(), "expected clean for gradle, got: {ok:#?}");
}
