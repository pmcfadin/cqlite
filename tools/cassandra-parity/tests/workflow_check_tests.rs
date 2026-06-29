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
fn inline_fail_closed_requires_truthy_value() {
    // A truthy inline assignment counts...
    assert!(workflow_is_fail_closed(
        "CQLITE_REQUIRE_FIXTURES=1 cargo test --test foo"
    ));
    assert!(workflow_is_fail_closed(
        "env CQLITE_REQUIRE_FIXTURES=true cargo test"
    ));
    assert!(workflow_is_fail_closed(
        "CQLITE_PARITY_REQUIRE_DATASETS=yes cargo test"
    ));
    // ...but a falsey / disabled / empty value MUST NOT count as fail-closed.
    assert!(!workflow_is_fail_closed(
        "CQLITE_REQUIRE_FIXTURES=0 cargo test --test foo"
    ));
    assert!(!workflow_is_fail_closed(
        "env CQLITE_REQUIRE_FIXTURES=false cargo test"
    ));
    assert!(!workflow_is_fail_closed(
        "CQLITE_REQUIRE_FIXTURES=no cargo test"
    ));
    assert!(!workflow_is_fail_closed(
        "CQLITE_REQUIRE_FIXTURES= cargo test"
    ));
}

#[test]
fn yaml_inline_fail_closed_requires_truthy_value() {
    // YAML `KEY: value` lines that appear in a folded `run:` block / env text.
    assert!(workflow_is_fail_closed(
        "CQLITE_PARITY_REQUIRE_DATASETS: '1'"
    ));
    assert!(workflow_is_fail_closed("CQLITE_REQUIRE_FIXTURES: true"));
    assert!(!workflow_is_fail_closed("CQLITE_REQUIRE_FIXTURES: '0'"));
    assert!(!workflow_is_fail_closed("CQLITE_REQUIRE_FIXTURES: \"\""));
    assert!(!workflow_is_fail_closed("CQLITE_REQUIRE_FIXTURES: false"));
}

#[test]
fn fail_closed_env_map_value_zero_does_not_count() {
    // A workflow-level env that DECLARES the flag but sets it to "0" must NOT be
    // treated as fail-closed: the lane can still skip-clean.
    let zero_wf = "env:\n  CQLITE_REQUIRE_FIXTURES: '0'\njobs:\n  parity:\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        zero_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for CQLITE_REQUIRE_FIXTURES=0, got: {findings:#?}"
    );
}

#[test]
fn fail_closed_env_map_empty_value_does_not_count() {
    // An empty value is not fail-closed either.
    let empty_wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: ''\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        empty_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for empty CQLITE_REQUIRE_FIXTURES, got: {findings:#?}"
    );
}

#[test]
fn fail_closed_env_map_false_value_does_not_count() {
    let false_wf = "jobs:\n  parity:\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity\n        env:\n          CQLITE_REQUIRE_FIXTURES: 'false'";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        false_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for false CQLITE_REQUIRE_FIXTURES, got: {findings:#?}"
    );
}

#[test]
fn fail_closed_env_map_truthy_value_counts() {
    // A bool-typed `true` (not a quoted string) must still count.
    let true_wf = "env:\n  CQLITE_REQUIRE_FIXTURES: true\njobs:\n  parity:\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        true_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for CQLITE_REQUIRE_FIXTURES: true, got: {findings:#?}"
    );
}

#[test]
fn inline_fail_closed_zero_in_step_command_does_not_count() {
    // `CQLITE_REQUIRE_FIXTURES=0 cargo test --test foo` inline on the run line
    // must NOT satisfy fail-closed.
    let inline_zero_wf = "jobs:\n  parity:\n    steps:\n      - run: CQLITE_REQUIRE_FIXTURES=0 cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        inline_zero_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for inline =0, got: {findings:#?}"
    );
}

#[test]
fn inline_fail_closed_ignores_commented_out_assignment() {
    // A `CQLITE_REQUIRE_FIXTURES=1` that appears ONLY inside a shell comment must
    // NOT make a fail-open step look fail-closed (consistency with the
    // command-RUN detection, which already strips per-line comments).
    assert!(!workflow_is_fail_closed(
        "# CQLITE_REQUIRE_FIXTURES=1 was here\ncargo test --test foo"
    ));
    // A trailing comment on an otherwise fail-open command line is also stripped.
    assert!(!workflow_is_fail_closed(
        "cargo test --test foo  # CQLITE_REQUIRE_FIXTURES=1 once"
    ));
    // A real, uncommented inline assignment still counts.
    assert!(workflow_is_fail_closed(
        "CQLITE_REQUIRE_FIXTURES=1 cargo test --test foo"
    ));
}

#[test]
fn commented_inline_fail_closed_flags_required_scenario_as_overstated() {
    // End-to-end: the workflow's ONLY arming of CQLITE_REQUIRE_FIXTURES is inside
    // a `#` comment on the run line, so the step is genuinely fail-open and the
    // required_parity scenario must be flagged as overstated.
    let commented_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          # CQLITE_REQUIRE_FIXTURES=1 cargo test --test issue_997_compressioninfo_parity\n          cargo test --package cqlite-core --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        commented_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding when the only arming is commented out, got: {findings:#?}"
    );
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
fn command_runs_test_requires_real_cargo_test_head() {
    // Issue #1228 roborev finding A: a command that merely MENTIONS the
    // `--test <name>` tokens (e.g. `echo cargo test --test foo`) must NOT count —
    // only a real `cargo test` / `cargo nextest run` invocation does.
    assert!(!command_runs_test(
        "echo cargo test --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    assert!(!command_runs_test(
        "printf '%s' --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    assert!(!command_runs_test(
        ": cargo test --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    // A real `cargo test --test foo` DOES count.
    assert!(command_runs_test(
        "cargo test --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    // `cargo nextest run --test foo` also counts.
    assert!(command_runs_test(
        "cargo nextest run --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    // The real workflow prefix form `env VAR=val cargo test --test foo` counts.
    assert!(command_runs_test(
        "env CQLITE_REQUIRE_FIXTURES=1 cargo test --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
    // A bare `cargo nextest` without `run` is not a test runner here.
    assert!(!command_runs_test(
        "cargo nextest --test issue_997_compressioninfo_parity",
        "issue_997_compressioninfo_parity"
    ));
}

#[test]
fn echoed_test_command_does_not_satisfy_required_parity() {
    // End-to-end finding A: the workflow's only mention of the mapped test is an
    // `echo` of the command (never actually executed) — overstated.
    let echo_wf = "env:\n  CQLITE_REQUIRE_FIXTURES: '1'\njobs:\n  parity:\n    steps:\n      - run: echo cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        echo_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("overstated")),
        "expected an overstated finding for an echoed test command, got: {findings:#?}"
    );
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
fn command_runs_gradle_requires_a_known_harness_test_task() {
    // The accepted parity-harness Gradle tasks are exactly the ones the real
    // JVM-harness workflow invokes (compaction-parity.yml): the built-in `test`
    // task and the custom `byteParity` Test task (build.gradle.kts:146).
    assert!(command_runs_gradle("gradle --no-daemon test"));
    assert!(command_runs_gradle("gradle --no-daemon byteParity"));
    assert!(command_runs_gradle("./gradlew test"));
    assert!(command_runs_gradle("./gradlew --no-daemon byteParity"));

    // A gradle invocation that does NOT name a harness test task must NOT count
    // as running the mapped Java test — it never executes the parity harness.
    assert!(!command_runs_gradle("gradle --version"));
    assert!(!command_runs_gradle("gradle assemble"));
    assert!(!command_runs_gradle("gradle --no-daemon assemble"));
    assert!(!command_runs_gradle("gradle build"));
    assert!(!command_runs_gradle("./gradlew clean"));
    assert!(!command_runs_gradle("gradle"));
    // `byteParity` must match as a WHOLE token, not a substring.
    assert!(!command_runs_gradle("gradle byteParityCheckStuff"));
    assert!(!command_runs_gradle("gradle testReport"));
}

#[test]
fn java_harness_non_test_gradle_task_does_not_satisfy_required_parity() {
    // Regression for #1228 roborev follow-up: a BLOCKING, FAIL-CLOSED gradle
    // step that runs a NON-test task (`--version`, `assemble`) must NOT satisfy
    // a JVM-harness required_parity scenario — it does not run the Java test.
    let java = [
        "compaction-parity/src/test/java/org/cqlite/parity/BasicDifferentialTest.java".to_string(),
    ];

    let version_wf = "jobs:\n  c:\n    env:\n      CQLITE_PARITY_REQUIRE_DATASETS: '1'\n    steps:\n      - run: gradle --version";
    let version = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        version_wf,
        &java,
    );
    assert!(
        version.iter().any(|f| f.message.contains("overstated")),
        "expected `gradle --version` to be overstated, got: {version:#?}"
    );

    let assemble_wf = "jobs:\n  c:\n    env:\n      CQLITE_PARITY_REQUIRE_DATASETS: '1'\n    steps:\n      - run: gradle assemble";
    let assemble = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        assemble_wf,
        &java,
    );
    assert!(
        assemble.iter().any(|f| f.message.contains("overstated")),
        "expected `gradle assemble` to be overstated, got: {assemble:#?}"
    );

    // The real task DOES satisfy it.
    let test_wf = "jobs:\n  c:\n    env:\n      CQLITE_PARITY_REQUIRE_DATASETS: '1'\n    steps:\n      - run: gradle --no-daemon test";
    let ok = check_scenario(
        "cass.compaction.harness_logical_tier",
        ".github/workflows/compaction-parity.yml",
        test_wf,
        &java,
    );
    assert!(
        ok.is_empty(),
        "expected `gradle --no-daemon test` to satisfy it, got: {ok:#?}"
    );
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

// ---------------------------------------------------------------------------
// Issue #1228 roborev finding B: inert text must NOT count as fail-closed. Only
// a flag genuinely shell-visible to the mapped test process counts:
//   (a) a workflow/job/step `env:` map value (covered by the tests above),
//   (b) an `export CQLITE_REQUIRE_FIXTURES=<truthy>` shell statement, or
//   (c) an inline prefix on the ACTUAL mapped test command.
// ---------------------------------------------------------------------------

#[test]
fn echoed_fail_closed_assignment_does_not_count() {
    // `echo CQLITE_REQUIRE_FIXTURES=1` only PRINTS the text; it never exports the
    // variable, so the cargo subprocess never sees it. The scenario is fail-open.
    let echo_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          echo CQLITE_REQUIRE_FIXTURES=1\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        echo_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for `echo CQLITE_REQUIRE_FIXTURES=1`, got: {findings:#?}"
    );
}

#[test]
fn standalone_unexported_assignment_does_not_count() {
    // A bare `CQLITE_REQUIRE_FIXTURES=1` line on its own (NOT exported, NOT
    // prefixing the test command) does not export the variable to the cargo
    // subprocess in bash — the scenario is fail-open.
    let standalone_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          CQLITE_REQUIRE_FIXTURES=1\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        standalone_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for a standalone unexported assignment, got: {findings:#?}"
    );
}

#[test]
fn exported_fail_closed_then_test_counts() {
    // `export CQLITE_REQUIRE_FIXTURES=1` then a LATER `cargo test --test foo` —
    // the export persists for the rest of the script, so the test sees it.
    let export_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          export CQLITE_REQUIRE_FIXTURES=1\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        export_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for `export CQLITE_REQUIRE_FIXTURES=1` + cargo test, got: {findings:#?}"
    );
}

#[test]
fn export_after_test_command_does_not_count() {
    // Issue #1228 roborev finding A: a shell `export` only affects SUBSEQUENT
    // commands. Here the `cargo test --test foo` runs FIRST and the
    // `export CQLITE_REQUIRE_FIXTURES=1` comes AFTER it, so the test process
    // never saw the flag — the scenario is fail-open and must be flagged.
    let export_after_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          cargo test --test issue_997_compressioninfo_parity\n          export CQLITE_REQUIRE_FIXTURES=1";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        export_after_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding when export comes AFTER the test, got: {findings:#?}"
    );
}

#[test]
fn export_before_test_command_counts() {
    // The complement of `export_after_test_command_does_not_count`: an
    // `export CQLITE_REQUIRE_FIXTURES=1` on a PRIOR logical command arms the test
    // that runs later in the same script. (Mirrors the real
    // `exported_fail_closed_then_test_counts`, made explicit for finding A.)
    let export_before_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          export CQLITE_REQUIRE_FIXTURES=1\n          echo armed\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        export_before_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean when export precedes the test command, got: {findings:#?}"
    );
}

#[test]
fn exported_falsey_value_does_not_count() {
    // `export CQLITE_REQUIRE_FIXTURES=0` arms nothing — still fail-open.
    let export_zero_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          export CQLITE_REQUIRE_FIXTURES=0\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        export_zero_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding for `export CQLITE_REQUIRE_FIXTURES=0`, got: {findings:#?}"
    );
}

#[test]
fn inline_prefix_on_test_command_counts() {
    // `CQLITE_REQUIRE_FIXTURES=1 cargo test --test foo` — inline prefix directly
    // on the mapped test command (the cargo subprocess inherits it).
    let inline_wf = "jobs:\n  parity:\n    steps:\n      - run: CQLITE_REQUIRE_FIXTURES=1 cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        inline_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for inline prefix on the test command, got: {findings:#?}"
    );
}

#[test]
fn inline_env_prefix_on_test_command_counts() {
    // The real parity workflows use `env CQLITE_REQUIRE_FIXTURES=1 <more=vars> \`
    // continued onto the cargo test line. The folded logical command carries the
    // inline prefix onto the same `--test` invocation.
    let env_prefix_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          env CQLITE_REQUIRE_FIXTURES=1 \\\n            CQLITE_DATASETS_ROOT=\"$CQLITE_DATASETS_ROOT\" \\\n            cargo test -p cqlite-core \\\n              --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        env_prefix_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for `env CQLITE_REQUIRE_FIXTURES=1 ... cargo test --test foo`, got: {findings:#?}"
    );
}

#[test]
fn inline_prefix_on_wrong_command_does_not_count() {
    // The fail-closed assignment prefixes a DIFFERENT command (an echo), not the
    // mapped test command. The test command itself runs fail-open.
    let wrong_cmd_wf = "jobs:\n  parity:\n    steps:\n      - run: |\n          CQLITE_REQUIRE_FIXTURES=1 echo armed\n          cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wrong_cmd_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("fail-closed")),
        "expected a fail-closed finding when the prefix is on a different command, got: {findings:#?}"
    );
}

// ---------------------------------------------------------------------------
// Issue #1228 roborev finding B: a conditional/skipped mapped-test step must not
// be credited. A step with no `if:` (or a statically-true `if:`) is eligible; a
// statically-false `if:` (or any non-trivial/unprovable `if:`) is not.
// ---------------------------------------------------------------------------

#[test]
fn if_false_mapped_test_step_does_not_count() {
    // The mapped test runs in a fail-closed, blocking step — but `if: false`
    // means it never runs, so the scenario is overstated.
    let if_false_wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - if: false\n        run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        if_false_wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("overstated")),
        "expected an overstated finding for `if: false`, got: {findings:#?}"
    );
}

#[test]
fn if_expression_false_mapped_test_step_does_not_count() {
    // `${{ false }}` is the GitHub-expression spelling of a statically-false `if:`.
    let wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - if: ${{ false }}\n        run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("overstated")),
        "expected an overstated finding for `if: ${{{{ false }}}}`, got: {findings:#?}"
    );
}

#[test]
fn nontrivial_if_mapped_test_step_is_not_proven_to_run() {
    // A non-trivial / unprovable `if:` (e.g. event-name guard) cannot be proven
    // to run in the gate context, so a mapped-test step carrying it is NOT
    // credited (no-overclaim default). No allowlist condition was observed on any
    // real mapped-test step, so this stays a reject.
    let wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - if: github.event_name == 'schedule'\n        run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.iter().any(|f| f.message.contains("overstated")),
        "expected an overstated finding for a non-trivial `if:`, got: {findings:#?}"
    );
}

#[test]
fn no_if_mapped_test_step_still_counts() {
    // A mapped-test step with NO `if:` is eligible (unchanged behavior).
    let wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for a mapped-test step with no `if:`, got: {findings:#?}"
    );
}

#[test]
fn if_true_mapped_test_step_counts() {
    // A statically-true `if:` is eligible.
    let wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - if: true\n        run: cargo test --test issue_997_compressioninfo_parity";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for `if: true`, got: {findings:#?}"
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

// ---------------------------------------------------------------------------
// Issue #1228 roborev finding B (aggregator outcome polarity): a continue-on-error
// mapped-test step is only build-blocking when a later blocking `exit 1`
// aggregator PROVES it fails on a non-success outcome — an explicit
// `steps.<id>.outcome != 'success'` check. A bare reference or a positive
// `== 'success'` check must NOT credit it; a reference to a DIFFERENT id must NOT.
// ---------------------------------------------------------------------------

/// Build a workflow whose mapped test runs in a `continue-on-error: true` step
/// with `id: parity`, followed by an `exit 1` aggregator gated on `aggregator_if`.
fn ce_with_aggregator(aggregator_if: &str) -> String {
    format!(
        "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      \
         - id: parity\n        continue-on-error: true\n        run: cargo test --test issue_997_compressioninfo_parity\n      \
         - name: Fail build on parity differences\n        if: {aggregator_if}\n        run: |\n          exit 1"
    )
}

#[test]
fn aggregator_negative_success_check_credits_continue_on_error_test() {
    // `steps.parity.outcome != 'success'` in an `exit 1` aggregator proves a
    // non-success outcome fails the build — the continue-on-error test is gated.
    let wf = ce_with_aggregator("always() && steps.parity.outcome != 'success'");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "expected clean for an `!= 'success'` aggregator, got: {findings:#?}"
    );
}

#[test]
fn aggregator_positive_success_check_does_not_credit_continue_on_error_test() {
    // `steps.parity.outcome == 'success'` does NOT fail the build on failure
    // (failures fall through the `==`); the continue-on-error test is still
    // non-blocking and the scenario is overstated.
    let wf = ce_with_aggregator("always() && steps.parity.outcome == 'success'");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("continue-on-error")),
        "expected a continue-on-error finding for a positive `== 'success'` aggregator, got: {findings:#?}"
    );
}

#[test]
fn aggregator_reference_to_different_id_does_not_credit_continue_on_error_test() {
    // The aggregator negatively checks a DIFFERENT id (`other`), not `parity`, so
    // the mapped test's continue-on-error step is not gated — overstated.
    let wf = ce_with_aggregator("always() && steps.other.outcome != 'success'");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("continue-on-error")),
        "expected a continue-on-error finding when only a DIFFERENT id is guarded, got: {findings:#?}"
    );
}

#[test]
fn aggregator_bare_reference_does_not_credit_continue_on_error_test() {
    // A bare reference with no comparison (`if: always() && steps.parity.outcome`)
    // does not prove the build fails on non-success — overstated.
    let wf = ce_with_aggregator("always() && steps.parity.outcome");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("continue-on-error")),
        "expected a continue-on-error finding for a bare outcome reference, got: {findings:#?}"
    );
}

#[test]
fn real_sstabledump_parity_gate_aggregator_credits_its_tests() {
    // Confirm against the REAL workflow file: its "Fail build on parity
    // differences" step is `if: always() && (steps.X.outcome != 'success' || ...)`
    // with `exit 1`, so each continue-on-error parity step whose id is listed in
    // that negative aggregator stays credited — e.g. step `compression_info_parity`
    // (id IS in the `!= 'success'` list) running `sstable_parity_compression_info_test`.
    let wf = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.github/workflows/sstabledump-parity-gate.yml"
    ))
    .expect("read real sstabledump-parity-gate.yml");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/sstabledump-parity-gate.yml",
        &wf,
        &["cqlite-core/tests/sstable_parity_compression_info_test.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "real sstabledump-parity-gate.yml aggregator must still credit its \
         continue-on-error parity tests, got: {findings:#?}"
    );
}

// ---------------------------------------------------------------------------
// #1228 roborev finding A: the aggregator step's OWN `if:` must be gate-eligible.
// A `negative-success` aggregator only credits its continue-on-error test when the
// aggregator can actually RUN — `always() && …` and no-`if:` run; `false && …`
// (static-false / unprovable) does NOT.
// ---------------------------------------------------------------------------

#[test]
fn always_aggregator_if_credits_continue_on_error_test() {
    // Matches the real sstabledump-parity-gate.yml form:
    // `if: always() && (steps.parity.outcome != 'success')`.
    let wf = ce_with_aggregator("always() && steps.parity.outcome != 'success'");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "an `always() &&` aggregator must still credit its test, got: {findings:#?}"
    );
}

#[test]
fn false_and_aggregator_if_does_not_credit_continue_on_error_test() {
    // A static-false-prefixed aggregator (`if: false && …`) never runs, so it
    // cannot convert the continue-on-error test's failure into a build failure —
    // the scenario stays overstated.
    let wf = ce_with_aggregator("false && steps.parity.outcome != 'success'");
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        &wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("continue-on-error")),
        "an `if: false && …` aggregator must NOT credit its test, got: {findings:#?}"
    );
}

#[test]
fn no_if_aggregator_with_body_negative_check_credits_continue_on_error_test() {
    // A no-`if:` aggregator (always runs) whose RUN BODY branches on the recorded
    // outcome and `exit 1`s on non-success still converts a failure into a build
    // failure, so the continue-on-error test is credited (issue #1228 finding A,
    // third TDD case).
    let wf = "jobs:\n  parity:\n    env:\n      CQLITE_REQUIRE_FIXTURES: '1'\n    steps:\n      - id: parity\n        continue-on-error: true\n        run: cargo test --test issue_997_compressioninfo_parity\n      - name: Fail build on parity differences\n        run: |\n          echo \"parity outcome guard: ${{ steps.parity.outcome != 'success' }}\"\n          exit 1";
    let findings = check_scenario(
        "cass.compression_info.fields.algorithm_name",
        ".github/workflows/x.yml",
        wf,
        &["cqlite-core/tests/issue_997_compressioninfo_parity.rs".to_string()],
    );
    assert!(
        findings.is_empty(),
        "a no-`if:` aggregator with a body negative-success check must credit its test, got: {findings:#?}"
    );
}

// ---------------------------------------------------------------------------
// #1228 roborev finding B: a JVM-harness gradle test task must run UNRESTRICTED.
// A test-selection / task-exclusion flag that could skip the mapped harness must
// NOT credit it.
// ---------------------------------------------------------------------------

#[test]
fn gradle_unrestricted_harness_task_counts() {
    // The real compaction-parity.yml invocations carry only `--no-daemon`.
    assert!(command_runs_gradle("gradle test"));
    assert!(command_runs_gradle("./gradlew byteParity"));
    assert!(command_runs_gradle("gradle --no-daemon test"));
    assert!(command_runs_gradle("gradle --no-daemon byteParity"));
}

#[test]
fn gradle_test_selection_or_exclusion_flag_does_not_count() {
    // A JUnit class/method selector restricts the run to OTHER tests.
    assert!(!command_runs_gradle("gradle test --tests OtherTest"));
    assert!(!command_runs_gradle("gradle test --test OtherTest"));
    assert!(!command_runs_gradle("gradle test --tests=OtherTest"));
    // Excluding the very task we credited skips the harness.
    assert!(!command_runs_gradle("gradle test -x test"));
    assert!(!command_runs_gradle("gradle test --exclude-task test"));
    assert!(!command_runs_gradle("gradle test --exclude-task=test"));
    assert!(!command_runs_gradle("gradle test -xtest"));
    // A `-Dtest=` style system-property filter restricts the test set.
    assert!(!command_runs_gradle("gradle test -Dtest=Other"));
    assert!(!command_runs_gradle("gradle test -Dtest.single=Other"));
}

#[test]
fn gradle_exclude_of_unrelated_task_still_counts() {
    // Excluding a DIFFERENT task (not an accepted harness task) does not skip the
    // harness, so a named harness task still runs unrestricted and counts.
    assert!(command_runs_gradle("gradle test -x check"));
    assert!(command_runs_gradle(
        "gradle test --exclude-task spotlessCheck"
    ));
}
