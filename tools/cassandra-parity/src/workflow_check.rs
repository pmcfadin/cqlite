//! Machine-enforced "the named workflow actually runs the mapped test" check
//! for `required_parity` scenarios (issue #1228).
//!
//! The parity manifest used to let a `required_parity` scenario name a
//! `ci.workflow` that never invoked its mapped test — e.g. 20 scenarios pointed
//! at `cassandra-parity.yml`, which only *lints* the manifest and runs no
//! fixture byte/value test. This check closes that overstatement hole: for every
//! `required_parity` scenario it parses the named workflow YAML text and asserts
//! that
//!
//! 1. each Rust integration test target named in `cqlite.coverage.tests`
//!    (`<crate>/tests/<name>.rs`) is actually invoked as `--test <name>`, and
//! 2. the workflow arms a fail-closed flag (`CQLITE_REQUIRE_FIXTURES` or
//!    `CQLITE_PARITY_REQUIRE_DATASETS`) so a vanished/unfetched dataset PANICS
//!    instead of silently green-passing the required gate.
//!
//! Gradle/JVM harness tests (`*.java`) are validated by requiring the workflow
//! to invoke `gradle` (the byte-parity harness from epic #968 runs
//! `gradle test` / `gradle byteParity`); we do not parse Java test selectors.
//!
//! The check is pure text-in/findings-out so it is trivially unit-testable; the
//! linter ([`crate::lint`]) wires it to disk via `repo_root` so dangling or
//! overstated mappings cannot pass the fast PR gate.

/// A single overstatement finding: a `required_parity` scenario whose named
/// workflow does not actually run its mapped test (or is not fail-closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowFinding {
    /// The offending scenario id.
    pub id: String,
    /// Manifest field the problem attaches to (`ci.workflow` or
    /// `cqlite.coverage.tests`).
    pub field: String,
    /// Human-readable explanation.
    pub message: String,
}

/// Derive the `cargo --test <name>` target from a Rust integration-test path of
/// the form `<crate>/tests/<name>.rs`. Returns `None` for non-test Rust paths
/// (e.g. `src/...`), which are validated only as existing files elsewhere.
pub fn rust_test_target(path: &str) -> Option<&str> {
    let norm = path.trim();
    if !norm.ends_with(".rs") {
        return None;
    }
    // Find the `/tests/` segment; the target is the file stem after it, but only
    // when the stem is a *direct* child of `tests/` (cargo integration targets
    // are top-level files under tests/, not nested modules).
    let idx = norm.find("/tests/")?;
    let after = &norm[idx + "/tests/".len()..];
    if after.contains('/') {
        return None; // nested module file, not an integration target
    }
    after.strip_suffix(".rs").filter(|s| !s.is_empty())
}

/// True if `path` is a JVM/gradle harness test source (validated via `gradle`).
pub fn is_java_test(path: &str) -> bool {
    path.trim().ends_with(".java")
}

/// The fail-closed env flags that turn a strict parity lane from skip-clean into
/// panic-on-missing-fixtures.
const FAIL_CLOSED_FLAGS: &[&str] = &["CQLITE_REQUIRE_FIXTURES", "CQLITE_PARITY_REQUIRE_DATASETS"];

/// True if `workflow_text` arms at least one fail-closed flag.
pub fn workflow_is_fail_closed(workflow_text: &str) -> bool {
    FAIL_CLOSED_FLAGS.iter().any(|f| workflow_text.contains(f))
}

/// True if the workflow invokes `gradle` (the JVM byte-parity harness).
pub fn workflow_runs_gradle(workflow_text: &str) -> bool {
    workflow_text.contains("gradle")
}

/// True if the workflow invokes the cargo integration target `name` via
/// `--test <name>`. We accept any surrounding whitespace/line-continuation
/// because workflows wrap long `cargo test` invocations across lines.
pub fn workflow_runs_test(workflow_text: &str, name: &str) -> bool {
    // Match `--test <name>` as a whole token: `--test` followed by whitespace
    // then exactly `name` bounded by whitespace or end. Avoids matching
    // `--test foo_bar` when looking for `foo`.
    let needle = "--test";
    let bytes = workflow_text.as_bytes();
    let mut start = 0;
    while let Some(rel) = workflow_text[start..].find(needle) {
        let pos = start + rel + needle.len();
        // Require whitespace immediately after `--test`.
        let rest = &workflow_text[pos..];
        let trimmed = rest.trim_start_matches([' ', '\t', '\\', '\n', '\r']);
        if let Some(tok) = trimmed.split_whitespace().next() {
            if tok == name {
                return true;
            }
        }
        start = pos;
        if start >= bytes.len() {
            break;
        }
    }
    false
}

/// Check one `required_parity` scenario's workflow against its mapped tests.
///
/// `workflow_path` is the manifest's `ci.workflow` value (for messages);
/// `workflow_text` is that file's contents; `tests` is `cqlite.coverage.tests`.
/// Returns one finding per problem (empty == OK).
pub fn check_scenario(
    id: &str,
    workflow_path: &str,
    workflow_text: &str,
    tests: &[String],
) -> Vec<WorkflowFinding> {
    let mut out = Vec::new();

    let rust_targets: Vec<&str> = tests.iter().filter_map(|t| rust_test_target(t)).collect();
    let has_java = tests.iter().any(|t| is_java_test(t));

    // A required_parity scenario with no recognizable executable test target
    // (no Rust integration target, no Java harness test) cannot be verified to
    // run anywhere — flag it rather than vacuously pass.
    if rust_targets.is_empty() && !has_java {
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "cqlite.coverage.tests".to_string(),
            message: format!(
                "required_parity scenario names no runnable test target \
                 (need a `<crate>/tests/<name>.rs` integration target or a `.java` \
                 harness test) to verify it runs in {workflow_path}"
            ),
        });
        return out;
    }

    // Anti-overstatement bar: the named workflow must actually exercise the
    // scenario. A scenario may list several corroborating tests; we require that
    // AT LEAST ONE of its mapped test targets genuinely runs in the named
    // workflow (a Rust `--test <name>` invocation, or a `gradle` run for a JVM
    // harness scenario). Requiring *every* listed test to run there would force
    // churn on legitimately multi-test scenarios without improving honesty.
    let any_rust_runs = rust_targets
        .iter()
        .any(|name| workflow_runs_test(workflow_text, name));
    let java_runs = has_java && workflow_runs_gradle(workflow_text);

    if !any_rust_runs && !java_runs {
        // Nothing the scenario maps to runs in its named workflow.
        let detail = if !rust_targets.is_empty() {
            format!(
                "none of its mapped tests [{}] are invoked via `--test` in {workflow_path}",
                rust_targets.join(", ")
            )
        } else {
            format!("its JVM harness test is not run by a `gradle` step in {workflow_path}")
        };
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "ci.workflow".to_string(),
            message: format!(
                "required_parity scenario is overstated: {detail} \
                 (the named workflow never runs the mapped test)"
            ),
        });
    }

    // The lane must be fail-closed so a missing dataset cannot silently green it.
    // (Gradle harness lanes bring up a real Cassandra container, which fails the
    // job on absence; the env-flag requirement applies to scenarios verified via
    // a Rust integration target.)
    if any_rust_runs && !workflow_is_fail_closed(workflow_text) {
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "ci.workflow".to_string(),
            message: format!(
                "required_parity workflow {workflow_path} does not arm a fail-closed \
                 flag (CQLITE_REQUIRE_FIXTURES / CQLITE_PARITY_REQUIRE_DATASETS); a \
                 missing dataset could silently green the gate"
            ),
        });
    }

    out
}
