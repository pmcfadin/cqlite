//! Machine-enforced "the named workflow actually runs the mapped test" check
//! for `required_parity` scenarios (issue #1228).
//!
//! The parity manifest used to let a `required_parity` scenario name a
//! `ci.workflow` that never invoked its mapped test — e.g. 20 scenarios pointed
//! at `cassandra-parity.yml`, which only *lints* the manifest and runs no
//! fixture byte/value test. This check closes that overstatement hole.
//!
//! A first pass closed the *dangling pointer* hole with raw substring matching,
//! but that was vulnerable to the same overstatement it was meant to prevent:
//! a commented-out `# --test foo`, a `cargo test --no-run --test foo` *build*
//! step, or a `continue-on-error: true` step that can never fail the build all
//! satisfied a naive `contains("--test foo")` scan. This module therefore
//! parses the workflow YAML into its **jobs → steps** structure and evaluates,
//! per executable `run:` command, whether the step genuinely runs the mapped
//! test in a way that can FAIL the build. Specifically, a `required_parity`
//! scenario is satisfied only when SOME step:
//!
//! 1. is an executable `run:` step (not `uses:`, not a comment), and
//! 2. invokes the mapped cargo integration target as `--test <name>` in an
//!    actual test RUN — a `--no-run` (compile-only) invocation does NOT count,
//!    and a commented-out token does NOT count, and
//! 3. is NOT `continue-on-error: true` — a step that cannot fail the build does
//!    not gate anything, and
//! 4. arms a fail-closed flag (`CQLITE_REQUIRE_FIXTURES` /
//!    `CQLITE_PARITY_REQUIRE_DATASETS`) at the step, job, OR workflow level, so a
//!    vanished/unfetched dataset PANICS instead of silently green-passing.
//!
//! Gradle/JVM harness tests (`*.java`) are validated by requiring a NON
//! continue-on-error `run:` step to invoke `gradle` (the byte-parity harness
//! from epic #968 runs `gradle test` / `gradle byteParity`, which brings up a
//! real Cassandra container and fails the job on absence); we do not parse Java
//! test selectors.
//!
//! The check is pure text-in/findings-out so it is trivially unit-testable; the
//! linter ([`crate::lint`]) wires it to disk via `repo_root` so dangling or
//! overstated mappings cannot pass the fast PR gate.

use std::collections::BTreeMap;

use serde::Deserialize;

mod command;

// Re-export the public text/command-parsing helpers so the module's public API
// (and its integration tests) are unchanged after the #1228 source split.
pub use command::{
    command_runs_gradle, command_runs_test, is_java_test, rust_test_target, workflow_is_fail_closed,
};

use command::{
    command_fails_build, inline_fail_closed_for_test, is_truthy_value, FAIL_CLOSED_FLAGS,
};

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

// ---------------------------------------------------------------------------
// Structural workflow model (jobs → steps) parsed from the workflow YAML.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WorkflowYaml {
    #[serde(default)]
    env: BTreeMap<String, serde_yaml::Value>,
    #[serde(default)]
    jobs: BTreeMap<String, JobYaml>,
}

#[derive(Debug, Deserialize)]
struct JobYaml {
    #[serde(default)]
    env: BTreeMap<String, serde_yaml::Value>,
    #[serde(default)]
    steps: Vec<StepYaml>,
}

#[derive(Debug, Default, Deserialize)]
struct StepYaml {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "if")]
    condition: Option<String>,
    #[serde(default)]
    run: Option<String>,
    #[serde(default, rename = "continue-on-error")]
    continue_on_error: Option<serde_yaml::Value>,
    #[serde(default)]
    env: BTreeMap<String, serde_yaml::Value>,
}

/// Whether a step's `if:` condition lets us PROVE the step runs in the PR/push
/// gate context (issue #1228 roborev finding B).
///
/// `evaluate_steps` used to credit a `run:` step regardless of its `if:`, so a
/// mapped-test step guarded by `if: false` (or any condition that skips on
/// PR/push) was still treated as machine-enforced. We refuse to evaluate
/// arbitrary GitHub Actions expressions (that would itself be a fragile
/// heuristic) and instead apply a CONSERVATIVE, NON-HEURISTIC rule:
///
///   - NO `if:`                          → [`StepGate::Eligible`] (runs always).
///   - STATICALLY-true `if:` (`true`,
///     `${{ true }}`)                     → [`StepGate::Eligible`].
///   - STATICALLY-false `if:` (`false`,
///     `${{ false }}`)                    → [`StepGate::Disabled`] (never runs).
///   - any OTHER (non-trivial / unprovable)
///     `if:`                             → [`StepGate::Unprovable`] — we cannot
///     prove it runs in the gate context, so we do NOT credit it for a
///     required_parity scenario (no-overclaim default).
///
/// We checked every workflow referenced by a `required_parity` scenario
/// (compaction-parity, cql-type-parity, live-cell-compaction-parity,
/// sstabledump-parity-gate, tombstone-ttl-parity): NO mapped-test `run:` step
/// (the `cargo test --test <name>` / `gradle <task>` steps) carries an `if:` —
/// the only `if:`-bearing steps are aggregators / summaries / PR-comments
/// (`if: always()`, `if: failure()`, `if: always() && steps.*.outcome != …`),
/// which are handled by the separate aggregator path and are NOT mapped-test
/// steps. So no allowlist of "known-gate-running" conditions is needed; the
/// static-true / static-false / conservative-reject rule keeps the 254 green.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepGate {
    /// Proven to run (no `if:`, or a statically-true `if:`).
    Eligible,
    /// Proven NOT to run (a statically-false `if:`).
    Disabled,
    /// Cannot prove whether it runs (any non-trivial `if:`); treated as
    /// not-proven-to-run for required_parity crediting.
    Unprovable,
}

/// Normalize an `if:` expression by trimming whitespace and unwrapping a single
/// surrounding `${{ ... }}` so `${{ false }}` reduces to `false`.
fn normalize_if(cond: &str) -> String {
    let t = cond.trim();
    let inner = t
        .strip_prefix("${{")
        .and_then(|s| s.strip_suffix("}}"))
        .map(str::trim)
        .unwrap_or(t);
    inner.to_string()
}

impl StepYaml {
    /// Classify this step's `if:` per [`StepGate`] (issue #1228 finding B).
    fn gate(&self) -> StepGate {
        match &self.condition {
            None => StepGate::Eligible,
            Some(cond) => match normalize_if(cond).as_str() {
                "true" => StepGate::Eligible,
                "false" => StepGate::Disabled,
                _ => StepGate::Unprovable,
            },
        }
    }

    /// `continue-on-error: true` (literal bool or the string "true"). An
    /// expression-valued `continue-on-error` is conservatively treated as
    /// potentially-true (cannot be relied on to fail the build).
    fn is_continue_on_error(&self) -> bool {
        match &self.continue_on_error {
            None => false,
            Some(serde_yaml::Value::Bool(b)) => *b,
            Some(serde_yaml::Value::String(s)) => {
                let t = s.trim();
                // Literal "false" is the only value that guarantees the step can
                // fail the build; an expression like ${{ ... }} cannot be relied
                // on, so anything other than "false" is treated as may-be-true.
                t != "false"
            }
            Some(_) => true,
        }
    }
}

/// Whether an env map sets a fail-closed flag to a TRUTHY value. A flag declared
/// but set to `0`/empty/`false` (etc.) does NOT count — the lane can still
/// skip-clean (issue #1228, roborev follow-up). The YAML value may be a string
/// (`'1'`, `"true"`), a bool (`true`), or an integer (`1`); all are normalized to
/// text and run through [`is_truthy_value`].
fn env_is_fail_closed(env: &BTreeMap<String, serde_yaml::Value>) -> bool {
    env.iter()
        .any(|(k, v)| FAIL_CLOSED_FLAGS.contains(&k.as_str()) && yaml_value_is_truthy(v))
}

/// Normalize a YAML env value to text and apply the [`is_truthy_value`] rule.
fn yaml_value_is_truthy(v: &serde_yaml::Value) -> bool {
    match v {
        serde_yaml::Value::Bool(b) => *b,
        serde_yaml::Value::String(s) => is_truthy_value(s),
        serde_yaml::Value::Number(n) => is_truthy_value(&n.to_string()),
        // Null / sequence / mapping are not meaningful enabling values.
        _ => false,
    }
}

/// A flattened, evaluated view of a single `run:` step with the context needed
/// to judge it: its command, whether it can fail the build, and whether an
/// enclosing scope (workflow/job/step `env:` map) is fail-closed.
///
/// `scope_fail_closed` is the only fail-closed signal that is unconditionally
/// visible to *every* command in the step (a GitHub Actions `env:` map exports
/// the variable into the step's shell process). An inline shell assignment in
/// the `run:` text is NOT recorded here — it is judged per mapped command in
/// [`command::inline_fail_closed_for_test`], because only an assignment that is
/// genuinely shell-visible to the mapped test process (an `export`, or an inline
/// prefix on the test command itself) actually arms the lane. See issue #1228
/// roborev finding B.
struct EvaluatedStep {
    command: String,
    can_fail_build: bool,
    scope_fail_closed: bool,
}

/// Collect the set of step `id`s that are *guarded by a blocking aggregator*:
/// a `continue-on-error` step records `steps.<id>.outcome`, and a later BLOCKING
/// step (`if:` referencing that outcome, with an `exit 1`-style body) converts a
/// non-success outcome into a build failure. This is the standard GitHub Actions
/// "run-then-aggregate" fail-closed pattern; without recognizing it the lint
/// would falsely flag every step in such a workflow.
fn aggregator_guarded_ids(job: &JobYaml) -> std::collections::HashSet<String> {
    let mut guarded = std::collections::HashSet::new();
    for step in &job.steps {
        // An aggregator is a BLOCKING step (not continue-on-error) whose body
        // can fail the build and whose `if:` gates on some step outcome.
        if step.is_continue_on_error() {
            continue;
        }
        let Some(run) = &step.run else { continue };
        if !command_fails_build(run) {
            continue;
        }
        let Some(cond) = &step.condition else {
            continue;
        };
        // Issue #1228 roborev finding B: only an id whose outcome/conclusion is
        // PROVEN to fail the build on a non-success result guards a
        // continue-on-error test. A bare reference, or a POSITIVE
        // `steps.<id>.outcome == 'success'` check (as in the "Comment on PR
        // (Success)" step), does NOT make the non-blocking test build-blocking —
        // failures still pass that condition. We therefore credit only ids that
        // appear in an explicit NEGATIVE check (`!= 'success'`) in the aggregator
        // condition.
        for id in extract_negatively_guarded_ids(cond) {
            guarded.insert(id);
        }
    }
    guarded
}

/// Extract every `<id>` whose `steps.<id>.outcome` / `steps.<id>.conclusion` is
/// compared with an explicit NEGATIVE check (`!= 'success'`) in a GitHub Actions
/// `if:` expression. Only such a reference proves the aggregator FAILS the build
/// when that step did not succeed (issue #1228 roborev finding B).
///
/// We accept the canonical negative form `steps.<id>.outcome != 'success'`
/// (single OR double quotes around the literal, arbitrary whitespace around the
/// operator). A reference that is only positively checked
/// (`== 'success'`), or a bare reference with no comparison, is NOT credited —
/// the build would still pass when that step failed. Matching is anchored to the
/// SPECIFIC id (whole-token `outcome`/`conclusion` accessor immediately after the
/// id), so a reference to a DIFFERENT id never bleeds through.
fn extract_negatively_guarded_ids(cond: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let needle = "steps.";
    let mut start = 0;
    while let Some(rel) = cond[start..].find(needle) {
        let pos = start + rel + needle.len();
        let rest = &cond[pos..];
        // The id runs until the next `.`; require a following `.outcome` or
        // `.conclusion` accessor so we only consider a status reference, not e.g.
        // `steps.x.outputs.y`.
        if let Some(dot) = rest.find('.') {
            let id = &rest[..dot];
            let after = &rest[dot..];
            let accessor = if after.starts_with(".outcome") {
                Some(".outcome")
            } else if after.starts_with(".conclusion") {
                Some(".conclusion")
            } else {
                None
            };
            if let Some(acc) = accessor {
                if !id.is_empty() && comparison_is_negative_success(&after[acc.len()..]) {
                    ids.push(id.to_string());
                }
            }
        }
        start = pos;
        if start >= cond.len() {
            break;
        }
    }
    ids
}

/// True if the text immediately FOLLOWING a `steps.<id>.outcome`/`.conclusion`
/// accessor is an explicit negative-success comparison: `!= 'success'` (or
/// `!= "success"`), allowing arbitrary surrounding whitespace. A positive
/// `== 'success'` comparison, or no comparison at all, returns false — only the
/// negative form proves the aggregator fails the build on that step's failure.
fn comparison_is_negative_success(after_accessor: &str) -> bool {
    let t = after_accessor.trim_start();
    let Some(rest) = t.strip_prefix("!=") else {
        return false;
    };
    let rest = rest.trim_start();
    // Accept a single- or double-quoted `success` literal.
    let inner = rest
        .strip_prefix('\'')
        .and_then(|s| s.split_once('\'').map(|(lit, _)| lit))
        .or_else(|| {
            rest.strip_prefix('"')
                .and_then(|s| s.split_once('"').map(|(lit, _)| lit))
        });
    matches!(inner, Some("success"))
}

/// Parse the workflow YAML into the evaluated `run:` steps we care about. On a
/// parse failure (malformed / unexpected YAML) we return `None`; callers fall
/// back to flagging the scenario rather than vacuously passing.
fn evaluate_steps(workflow_text: &str) -> Option<Vec<EvaluatedStep>> {
    let wf: WorkflowYaml = serde_yaml::from_str(workflow_text).ok()?;
    let workflow_fail_closed = env_is_fail_closed(&wf.env);
    let mut out = Vec::new();
    for job in wf.jobs.values() {
        let job_fail_closed = workflow_fail_closed || env_is_fail_closed(&job.env);
        let guarded = aggregator_guarded_ids(job);
        for step in &job.steps {
            let Some(run) = &step.run else { continue };
            // A step's SCOPE fail-closed status: workflow/job env, or the step's
            // own `env:` map. These export the flag into the step's shell process
            // unconditionally. An inline shell assignment in the `run:` text is
            // judged per mapped command in `check_scenario` (it only arms the lane
            // when exported or inline-prefixing the test command — issue #1228).
            let scope_fail_closed = job_fail_closed || env_is_fail_closed(&step.env);
            // A step can fail the build if it is itself blocking, OR it is a
            // continue-on-error step whose outcome is guarded by a blocking
            // aggregator (`exit 1` gated on `steps.<id>.outcome`).
            let id_guarded = step
                .id
                .as_ref()
                .map(|i| guarded.contains(i))
                .unwrap_or(false);
            // Issue #1228 finding B: a step we cannot PROVE runs in the gate
            // context (statically-false `if:`, or any non-trivial/unprovable
            // `if:`) must NOT be credited as able to fail the build for a
            // required_parity scenario — a conditionally-skipped mapped-test step
            // is not machine-enforced. A statically-true / absent `if:` is
            // eligible. (The aggregator path that consumes `steps.<id>.outcome`
            // is evaluated separately and intentionally credits `always()`
            // aggregators; this gate is only for the mapped-test step itself.)
            let proven_to_run = matches!(step.gate(), StepGate::Eligible);
            let can_fail_build = proven_to_run && (!step.is_continue_on_error() || id_guarded);
            out.push(EvaluatedStep {
                command: run.clone(),
                can_fail_build,
                scope_fail_closed,
            });
        }
    }
    Some(out)
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

    // Parse the workflow into evaluated steps. A workflow we cannot parse cannot
    // be proven to run the mapped test fail-closed, so flag it rather than pass.
    let Some(steps) = evaluate_steps(workflow_text) else {
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "ci.workflow".to_string(),
            message: format!(
                "required_parity workflow {workflow_path} could not be parsed as a \
                 jobs/steps GitHub Actions workflow; cannot verify it runs the mapped \
                 test fail-closed"
            ),
        });
        return out;
    };

    // Anti-overstatement bar (hardened for #1228): the named workflow must
    // actually exercise the scenario in a step that CAN FAIL THE BUILD and is
    // FAIL-CLOSED. A scenario may list several corroborating tests; we require
    // that AT LEAST ONE of its mapped targets runs in such a step.
    //
    // We track *why* the bar is unmet to give a precise message: a target that
    // runs but only in a continue-on-error step, or only as `--no-run`, or in a
    // fail-open step, is overstated for a different reason than a target that
    // never appears at all.
    let mut rust_satisfied = false;
    // A target that runs in a blocking step but with no fail-closed env.
    let mut rust_runs_but_fail_open = false;
    // A target that appears only in a continue-on-error (non-blocking) step.
    let mut rust_runs_but_non_blocking = false;

    for name in &rust_targets {
        for step in &steps {
            if !command_runs_test(&step.command, name) {
                continue;
            }
            // This step runs the target as a real test (not --no-run). It is
            // fail-closed iff a scope `env:` map arms the flag OR the run block
            // genuinely makes it shell-visible to THIS test command (export, or
            // an inline prefix on the test command) — issue #1228 finding B.
            let fail_closed =
                step.scope_fail_closed || inline_fail_closed_for_test(&step.command, name);
            match (step.can_fail_build, fail_closed) {
                (true, true) => {
                    rust_satisfied = true;
                }
                (true, false) => rust_runs_but_fail_open = true,
                (false, _) => rust_runs_but_non_blocking = true,
            }
        }
        if rust_satisfied {
            break;
        }
    }

    // The JVM harness brings up a real Cassandra container and fails the job on
    // absence, so (unlike the Rust dataset lanes) we do not require a fail-closed
    // flag — a blocking `gradle <harness task>` step is sufficient.
    let java_satisfied = has_java
        && steps
            .iter()
            .any(|s| s.can_fail_build && command_runs_gradle(&s.command));
    let java_runs_but_non_blocking = has_java
        && !java_satisfied
        && steps
            .iter()
            .any(|s| !s.can_fail_build && command_runs_gradle(&s.command));

    if rust_satisfied || java_satisfied {
        return out;
    }

    // Nothing the scenario maps to runs blocking + fail-closed. Explain why.
    if !rust_targets.is_empty() {
        let detail = if rust_runs_but_non_blocking {
            "the only step running it is `continue-on-error: true` (it cannot fail the build)"
        } else if rust_runs_but_fail_open {
            "the step running it arms no fail-closed flag \
             (CQLITE_REQUIRE_FIXTURES / CQLITE_PARITY_REQUIRE_DATASETS); a missing \
             dataset could silently green the gate"
        } else {
            "no blocking `run:` step invokes it via `--test` (a `--no-run` build \
             or a commented-out token does not count)"
        };
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "ci.workflow".to_string(),
            message: format!(
                "required_parity scenario is overstated: in {workflow_path}, {detail} \
                 (mapped tests: [{}])",
                rust_targets.join(", ")
            ),
        });
    } else {
        let detail = if java_runs_but_non_blocking {
            "the only `gradle` step is `continue-on-error: true` (it cannot fail the build)"
        } else {
            "no blocking `run:` step invokes `gradle`"
        };
        out.push(WorkflowFinding {
            id: id.to_string(),
            field: "ci.workflow".to_string(),
            message: format!(
                "required_parity scenario is overstated: its JVM harness test does not \
                 run in {workflow_path} — {detail}"
            ),
        });
    }

    out
}
