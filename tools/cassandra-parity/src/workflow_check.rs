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

/// Truthy rule for a fail-closed flag VALUE (issue #1228, roborev follow-up).
///
/// Merely DECLARING a fail-closed flag is not enough — `CQLITE_REQUIRE_FIXTURES=0`
/// (or `""`, or `false`) leaves the lane able to skip-clean, so it must NOT count
/// as fail-closed. We accept only an explicitly-enabling value:
///   truthy  ⇔ (case-insensitive) one of `1`, `true`, `yes`, `on`
/// and reject everything else (notably `0`, ``, `false`, `no`, `off`). Surrounding
/// quotes/whitespace are stripped first so YAML `'1'` / `"true"` are honored. This
/// matches the conventional shell/CI boolean convention and is conservative: an
/// unrecognized value is treated as NOT fail-closed (no-overclaim default).
fn is_truthy_value(raw: &str) -> bool {
    let v = raw.trim().trim_matches(['\'', '"']).trim();
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

/// True if `text` mentions at least one fail-closed flag SET TO A TRUTHY VALUE.
/// Used against the effective env scope of a step's own command, recognizing both
/// inline shell assignments (`CQLITE_REQUIRE_FIXTURES=1 cargo`, `env FOO=1 cargo`)
/// and YAML-style `KEY: value` lines that may appear in folded text. A flag
/// declared but disabled (`=0`, empty, `false`) does NOT count.
///
/// Per-line shell comments are stripped first (same [`strip_shell_comment`] helper
/// the command-RUN detection uses), so a commented-out `# CQLITE_REQUIRE_FIXTURES=1`
/// does NOT make a genuinely fail-open step look fail-closed.
pub fn workflow_is_fail_closed(text: &str) -> bool {
    let stripped = strip_shell_comments(text);
    FAIL_CLOSED_FLAGS
        .iter()
        .any(|flag| flag_set_truthy_in_text(&stripped, flag))
}

/// True if `flag` appears in `text` with an assignment separator (`=` for a
/// shell inline `FOO=val`, `:` for a YAML `FOO: val`) and a TRUTHY value, at ANY
/// occurrence. We scan every occurrence so a later truthy assignment is honored
/// even if an earlier (or substring-only) one is not.
fn flag_set_truthy_in_text(text: &str, flag: &str) -> bool {
    let mut search_from = 0;
    while let Some(rel) = text[search_from..].find(flag) {
        let pos = search_from + rel;
        let after = &text[pos + flag.len()..];
        // The character immediately after the flag name must be an assignment
        // separator (`=` for shell, `:` for YAML) — guards against a substring
        // match like `CQLITE_REQUIRE_FIXTURES_LOG`.
        let sep = after.chars().next();
        if matches!(sep, Some('=') | Some(':')) {
            // Slice the value to end-of-line, then let `is_truthy_value` strip
            // quotes/whitespace.
            let value_part = &after[1..];
            let line_end = value_part.find('\n').unwrap_or(value_part.len());
            let value = &value_part[..line_end];
            // For an inline shell `FOO=val cmd`, the value ends at the first
            // whitespace (and may be empty: `FOO= cmd`); for YAML `FOO: val` we
            // keep the whole (trimmed) line.
            let value = if sep == Some('=') {
                let end = value.find(char::is_whitespace).unwrap_or(value.len());
                &value[..end]
            } else {
                value
            };
            if is_truthy_value(value) {
                return true;
            }
        }
        search_from = pos + flag.len();
        if search_from >= text.len() {
            break;
        }
    }
    false
}

/// Strip a shell line of trailing comments so a commented-out token (e.g.
/// `# --test foo`) is not mistaken for an executable invocation. A `#` only
/// starts a comment at the start of a line or after whitespace; a `#` glued to a
/// non-space char (e.g. `foo#bar`, rare in our workflows) is left intact.
/// Anything from the first such `#` to end-of-line is dropped.
fn strip_shell_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' && (i == 0 || bytes[i - 1].is_ascii_whitespace()) {
            return &line[..i];
        }
        i += 1;
    }
    line
}

/// Strip per-line shell comments from a multi-line block, reusing the single-line
/// [`strip_shell_comment`] helper. Newlines are preserved so downstream line/value
/// scanning (e.g. [`flag_set_truthy_in_text`]) keeps its line boundaries.
fn strip_shell_comments(text: &str) -> String {
    text.lines()
        .map(strip_shell_comment)
        .collect::<Vec<_>>()
        .join("\n")
}

/// True if any executable (comment-stripped) line in `command` invokes the cargo
/// integration target `name` via `--test <name>` AND that same line is NOT a
/// compile-only `--no-run` invocation. A workflow `run:` block is a shell
/// script: each *logical* command is the script with `\`-continuations folded,
/// but `--no-run` and `--test` always live in the same logical `cargo test`
/// invocation, so we evaluate per logical command (continuation-folded).
pub fn command_runs_test(command: &str, name: &str) -> bool {
    for logical in logical_commands(command) {
        if logical_runs_test(&logical, name) {
            return true;
        }
    }
    false
}

/// Fold `\`-line-continuations and split a `run:` script into logical commands.
/// We keep it simple: strip per-line shell comments, join lines ending in `\`
/// to the next line, and treat each resulting line as one logical command.
/// (Workflows here never chain `cargo test ... ; cargo test ...` on one line;
/// `&&`/`;` separation across distinct test invocations is uncommon and, if it
/// occurs, only makes the check stricter by keeping `--no-run` attached.)
fn logical_commands(command: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for raw in command.lines() {
        let line = strip_shell_comment(raw);
        let trimmed = line.trim_end();
        if let Some(prefix) = trimmed.strip_suffix('\\') {
            cur.push_str(prefix);
            cur.push(' ');
        } else {
            cur.push_str(trimmed);
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// True if a single logical command runs `--test <name>` and is not `--no-run`.
fn logical_runs_test(logical: &str, name: &str) -> bool {
    if !has_test_flag(logical, name) {
        return false;
    }
    // A `cargo test --no-run` only COMPILES the target; it never runs it. A
    // compile step (even of the right target) cannot fail-close a missing
    // dataset, so it does not satisfy "runs the mapped test".
    if logical
        .split_whitespace()
        .any(|tok| tok == "--no-run" || tok == "--no_run")
    {
        return false;
    }
    true
}

/// True if `logical` contains a `--test <name>` token pair (whole-token match on
/// `name`, so `--test foo` does not satisfy a search for `--test foo_bar`).
fn has_test_flag(logical: &str, name: &str) -> bool {
    let toks: Vec<&str> = logical.split_whitespace().collect();
    let mut i = 0;
    while i + 1 < toks.len() {
        if toks[i] == "--test" && toks[i + 1] == name {
            return true;
        }
        i += 1;
    }
    false
}

/// The set of Gradle task tokens that actually EXECUTE the JVM parity harness.
///
/// Grounded in the real JVM-harness workflow (`.github/workflows/compaction-parity.yml`):
///   - `test`      — the built-in JUnit `Test` task that runs the logical-tier
///     parity scenarios (`gradle --no-daemon test`, line 173).
///   - `byteParity`— the custom `Test`-typed task asserting byte-identical output
///     (`gradle --no-daemon byteParity`, line 194; registered in
///     `compaction-parity/build.gradle.kts:146`).
///
/// A `gradle`/`gradlew` invocation that names NONE of these tasks (e.g. bare
/// `gradle`, `gradle --version`, `gradle assemble`, `gradle build`, `gradle clean`)
/// does NOT run the harness, so it must not satisfy a JVM-harness required_parity
/// scenario (#1228 roborev follow-up). Add a task here only when a real parity
/// workflow invokes it to execute the harness.
const GRADLE_HARNESS_TEST_TASKS: &[&str] = &["test", "byteParity"];

/// True if a single logical command invokes `gradle`/`gradlew` as the
/// **executable** (the command head, possibly after `env VAR=val` / `sudo`
/// prefixes — not merely the substring `gradle` in an argument or comment) AND
/// names a known harness test task ([`GRADLE_HARNESS_TEST_TASKS`]) as a whole
/// argument token. Requiring the task token prevents a non-test invocation
/// (`gradle --version`, `gradle assemble`) from being credited with running the
/// mapped Java test.
fn logical_runs_gradle(logical: &str) -> bool {
    let mut toks = logical.split_whitespace().peekable();
    // Skip leading command prefixes that precede the real executable.
    while let Some(&tok) = toks.peek() {
        if tok == "sudo" || tok == "env" || tok.contains('=') {
            toks.next();
        } else {
            break;
        }
    }
    let is_gradle_head = match toks.next() {
        Some(head) => {
            let t = head.trim_start_matches("./");
            t == "gradle" || t == "gradlew"
        }
        None => false,
    };
    if !is_gradle_head {
        return false;
    }
    // The remaining whitespace tokens are the gradle args (flags + tasks). At
    // least one must be a recognized harness test task as a whole token; a flag
    // like `--no-daemon` is skipped, and a substring match (`byteParityFoo`)
    // does not count because we compare whole tokens.
    toks.any(|tok| GRADLE_HARNESS_TEST_TASKS.contains(&tok))
}

/// True if any executable line in `command` invokes `gradle`.
pub fn command_runs_gradle(command: &str) -> bool {
    logical_commands(command)
        .iter()
        .any(|l| logical_runs_gradle(l))
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

impl StepYaml {
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
/// [`inline_fail_closed_for_test`] / [`inline_fail_closed_for_gradle`], because
/// only an assignment that is genuinely shell-visible to the mapped test process
/// (an `export`, or an inline prefix on the test command itself) actually arms
/// the lane. See issue #1228 roborev finding B.
struct EvaluatedStep {
    command: String,
    can_fail_build: bool,
    scope_fail_closed: bool,
}

/// True if a fail-closed flag is **genuinely shell-visible** to the cargo
/// integration target `name` when run from the `run:` block `command`.
///
/// Per issue #1228 (roborev finding B) a flag counts ONLY when it actually
/// reaches the spawned test process. Within a `run:` script that means one of:
///
///   (b) an `export CQLITE_REQUIRE_FIXTURES=<truthy>` shell statement on any
///       earlier logical command (exports persist for the rest of the script), OR
///   (c) an inline prefix `CQLITE_REQUIRE_FIXTURES=<truthy> cargo test --test name`
///       (or `env CQLITE_REQUIRE_FIXTURES=<truthy> cargo test --test name`) on the
///       SAME logical command that runs the mapped test — the assignment prefixes
///       the command-token sequence whose target is `name`.
///
/// Explicitly NOT fail-closed: `echo CQLITE_REQUIRE_FIXTURES=1` (printed, never
/// exported), a bare standalone `CQLITE_REQUIRE_FIXTURES=1` line that neither
/// exports nor prefixes the test command (bash does not export it to the cargo
/// subprocess), and any mention inside a shell comment (stripped first). The
/// step/job/workflow `env:` map path is handled separately via
/// [`EvaluatedStep::scope_fail_closed`].
fn inline_fail_closed_for_test(command: &str, name: &str) -> bool {
    let logicals = logical_commands(command);
    // (b) An `export FOO=<truthy>` anywhere arms the rest of the script.
    if logicals.iter().any(|l| logical_exports_fail_closed(l)) {
        return true;
    }
    // (c) An inline `FOO=<truthy>` / `env FOO=<truthy>` prefix on the very
    // logical command that runs the mapped test.
    logicals
        .iter()
        .any(|l| logical_runs_test(l, name) && logical_inline_prefix_fail_closed(l))
}

/// True if a single logical command is an `export FOO=<truthy>` statement for a
/// fail-closed flag (optionally with a leading bare `export` of several names —
/// we only credit `export FOO=<truthy>`, not a bare `export FOO` referencing an
/// already-set value, which we cannot prove truthy from the text). The command
/// has already had shell comments stripped by [`logical_commands`].
fn logical_exports_fail_closed(logical: &str) -> bool {
    let toks: Vec<&str> = logical.split_whitespace().collect();
    let Some(first) = toks.first() else {
        return false;
    };
    if *first != "export" {
        return false;
    }
    // `export A=1 B=2 ...` — any assignment token that arms a fail-closed flag
    // to a truthy value counts.
    toks[1..].iter().any(|tok| assignment_is_fail_closed(tok))
}

/// True if `logical` begins with one or more inline assignment prefixes
/// (`FOO=val` / `env FOO=val ...`) and at least one of those prefixes arms a
/// fail-closed flag to a truthy value. Only the leading run of assignment tokens
/// (the inline-prefix region that precedes the real command) is inspected, so a
/// `FOO=1` that appears as a later *argument* (not a prefix) does not count.
fn logical_inline_prefix_fail_closed(logical: &str) -> bool {
    let mut toks = logical.split_whitespace().peekable();
    // An optional leading `env` introduces an inline-assignment prefix region.
    if matches!(toks.peek(), Some(&"env")) {
        toks.next();
    }
    let mut armed = false;
    for tok in toks {
        if tok.contains('=') && !tok.starts_with('=') {
            // Still inside the leading assignment-prefix region.
            if assignment_is_fail_closed(tok) {
                armed = true;
            }
        } else {
            // First non-assignment token: the prefix region is over (this is the
            // command head, e.g. `cargo`). Any later `FOO=1` is an argument.
            break;
        }
    }
    armed
}

/// True if a single `KEY=VALUE` shell token assigns a fail-closed flag to a
/// truthy value (`CQLITE_REQUIRE_FIXTURES=1`). The value is everything after the
/// first `=`; it is run through [`is_truthy_value`].
fn assignment_is_fail_closed(tok: &str) -> bool {
    let Some((key, value)) = tok.split_once('=') else {
        return false;
    };
    FAIL_CLOSED_FLAGS.contains(&key) && is_truthy_value(value)
}

/// True if a logical command contains a build-failing token (`exit 1` or a bare
/// `false`). Used to recognize a "Fail build on parity differences" aggregator
/// step that turns a recorded step outcome into a real build failure.
fn command_fails_build(command: &str) -> bool {
    for logical in logical_commands(command) {
        let toks: Vec<&str> = logical.split_whitespace().collect();
        // `exit <nonzero>`
        if let Some(pos) = toks.iter().position(|t| *t == "exit") {
            if let Some(code) = toks.get(pos + 1) {
                if code.parse::<i32>().map(|c| c != 0).unwrap_or(false) {
                    return true;
                }
            }
        }
        // a bare `false` command (common build-fail idiom)
        if toks.first() == Some(&"false") {
            return true;
        }
    }
    false
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
        // Any `steps.<id>.outcome` mentioned in the aggregator's condition is
        // guarded: a non-success outcome there reaches the failing body.
        for id in extract_outcome_ids(cond) {
            guarded.insert(id);
        }
    }
    guarded
}

/// Extract every `<id>` from `steps.<id>.outcome` / `steps.<id>.conclusion`
/// references in a GitHub Actions `if:` expression.
fn extract_outcome_ids(cond: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let needle = "steps.";
    let mut start = 0;
    while let Some(rel) = cond[start..].find(needle) {
        let pos = start + rel + needle.len();
        let rest = &cond[pos..];
        // The id runs until the next `.`; require a following `.outcome` or
        // `.conclusion` so we only credit a status reference, not e.g.
        // `steps.x.outputs.y`.
        if let Some(dot) = rest.find('.') {
            let id = &rest[..dot];
            let after = &rest[dot..];
            if (after.starts_with(".outcome") || after.starts_with(".conclusion")) && !id.is_empty()
            {
                ids.push(id.to_string());
            }
        }
        start = pos;
        if start >= cond.len() {
            break;
        }
    }
    ids
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
            let can_fail_build = !step.is_continue_on_error() || id_guarded;
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
