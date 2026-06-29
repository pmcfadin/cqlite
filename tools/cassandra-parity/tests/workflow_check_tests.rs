//! Tests for the machine-enforced "workflow actually runs the mapped test"
//! check (issue #1228).

use cassandra_parity::workflow_check::{
    check_scenario, command_runs_gradle, command_runs_test, is_java_test, rust_test_target,
    workflow_is_fail_closed,
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
fn command_runs_test_matches_whole_token_across_line_continuations() {
    let cmd = "          cargo test -p cqlite-core \\\n            --test issue_997_compressioninfo_parity \\\n            --test issue_998_inline_crc_trailers";
    assert!(command_runs_test(cmd, "issue_997_compressioninfo_parity"));
    assert!(command_runs_test(cmd, "issue_998_inline_crc_trailers"));
    // must not match a prefix of a longer target name
    assert!(!command_runs_test(cmd, "issue_997"));
    assert!(!command_runs_test(cmd, "issue_999_missing"));
}

#[test]
fn command_runs_test_rejects_no_run_compile_only() {
    // A `--no-run` invocation only COMPILES the target; it must NOT count as
    // running it (this is the `Build parity test targets` step pattern).
    let build_only =
        "cargo test --no-run --package cqlite-core \\\n  --test issue_997_compressioninfo_parity";
    assert!(!command_runs_test(
        build_only,
        "issue_997_compressioninfo_parity"
    ));
    // The same target in a real run (no --no-run) does count.
    let real = "cargo test --package cqlite-core --test issue_997_compressioninfo_parity";
    assert!(command_runs_test(real, "issue_997_compressioninfo_parity"));
}

#[test]
fn command_runs_test_rejects_commented_out_token() {
    // A commented-out `--test foo` must not count as running it.
    let commented = "echo hi\n# cargo test --test issue_997_compressioninfo_parity";
    assert!(!command_runs_test(
        commented,
        "issue_997_compressioninfo_parity"
    ));
    // Trailing comment after a real (different) command also stripped.
    let trailing = "cargo build  # cargo test --test issue_997_compressioninfo_parity";
    assert!(!command_runs_test(
        trailing,
        "issue_997_compressioninfo_parity"
    ));
}

#[test]
fn command_runs_gradle_requires_executable_token() {
    assert!(command_runs_gradle("gradle --no-daemon byteParity"));
    assert!(command_runs_gradle("./gradlew test"));
    assert!(!command_runs_gradle("cargo test --test foo"));
    // The word "gradle" in a comment or unrelated string must not count.
    assert!(!command_runs_gradle("echo 'see the gradle docs'"));
    assert!(!command_runs_gradle("# run gradle byteParity later"));
}

#[test]
fn lint_only_workflow_fails_byte_scenario() {
    // A scenario mapping to a real Rust test, pointed at a workflow that only
    // lints the manifest (no --test, no fail-closed flag) must be flagged.
    let lint_only_wf = "name: Cassandra Parity Manifest\njobs:\n  lint:\n    steps:\n      - run: cargo run -p cassandra-parity -- lint\n      - run: cargo test -p cassandra-parity\n";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/cassandra-parity.yml",
        lint_only_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.field == "ci.workflow" && f.message.contains("overstated")),
        "expected an overstated finding, got: {findings:#?}"
    );
}

#[test]
fn wrong_test_workflow_fails() {
    // delta-roundtrip.yml runs delta_roundtrip_tests, NOT scan_delta_parity_test.
    let delta_roundtrip_wf = "env:\n  DELTA_ROUNDTRIP_DATA: /tmp/x\njobs:\n  rt:\n    steps:\n      - run: cargo test --test delta_roundtrip_tests";
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
    let good_wf = "env:\n  CQLITE_PARITY_REQUIRE_DATASETS: '1'\njobs:\n  parity:\n    steps:\n      - run: |\n          cargo test --package cqlite-core --features write-support,cli-helpers \\\n            --test issue_997_compressioninfo_parity";
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
    let fail_open_wf =
        "jobs:\n  x:\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
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
fn continue_on_error_step_does_not_satisfy_required_parity() {
    // The Finding 1 shape: the mapped test runs ONLY inside a
    // `continue-on-error: true` step (so it can never fail the build) even
    // though the workflow is fail-closed. This must be flagged as overstated.
    let ce_wf = "env:\n  CQLITE_PARITY_REQUIRE_DATASETS: '1'\njobs:\n  parity:\n    steps:\n      - name: Build only\n        run: cargo test --no-run --test issue_997_compressioninfo_parity\n      - name: Informational\n        continue-on-error: true\n        run: |\n          cargo test --package cqlite-core \\\n            --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/sstabledump-parity-gate.yml",
        ce_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("continue-on-error")),
        "expected a continue-on-error finding, got: {findings:#?}"
    );
}

#[test]
fn no_run_only_build_step_does_not_satisfy_required_parity() {
    // The mapped test is only ever `--no-run` compiled (never executed). Even
    // under a fail-closed workflow, that proves nothing — must be flagged.
    let build_only_wf = "env:\n  CQLITE_PARITY_REQUIRE_DATASETS: '1'\njobs:\n  parity:\n    steps:\n      - run: cargo test --no-run --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/sstabledump-parity-gate.yml",
        build_only_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("--no-run")),
        "expected a no-run finding, got: {findings:#?}"
    );
}

#[test]
fn fail_closed_env_on_job_counts() {
    // The fail-closed flag lives on the JOB env (not the workflow or the step);
    // a blocking step that runs the test must still be accepted.
    let job_env_wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        job_env_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean (job-level fail-closed env), got: {findings:#?}"
    );
}

#[test]
fn fail_closed_env_on_step_counts() {
    // The fail-closed flag lives on the STEP env.
    let step_env_wf = "jobs:\n  parity:\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity\n        env:\n          CQLITE_REQUIRE_FIXTURES: '1'";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        step_env_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean (step-level fail-closed env), got: {findings:#?}"
    );
}

#[test]
fn java_harness_workflow_requires_blocking_gradle() {
    let no_gradle = "jobs:\n  c:\n    steps:\n      - run: echo nothing";
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

    let with_gradle = "jobs:\n  c:\n    steps:\n      - run: gradle --no-daemon byteParity";
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

    // A gradle step that is continue-on-error must NOT satisfy a required gate.
    let ce_gradle = "jobs:\n  c:\n    steps:\n      - continue-on-error: true\n        run: gradle --no-daemon byteParity";
    let ce = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        ce_gradle,
        &[
            "compaction-parity/src/test/java/org/cqlite/parity/BasicDifferentialTest.java"
                .to_string(),
        ],
    );
    assert!(
        ce.iter().any(|f| f.message.contains("continue-on-error")),
        "expected a continue-on-error finding for gradle, got: {ce:#?}"
    );
}

#[test]
fn unparseable_workflow_is_flagged() {
    // A workflow that does not parse as jobs/steps cannot be proven to run the
    // mapped test — must be flagged rather than vacuously pass.
    let garbage = "this: [is: not: valid: yaml";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/broken.yml",
        garbage,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        !findings.is_empty(),
        "expected the unparseable workflow to be flagged"
    );
}
