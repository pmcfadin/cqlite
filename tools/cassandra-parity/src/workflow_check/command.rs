//! Plain-text / shell-command parsing helpers for the workflow-check
//! (issue #1228). These operate purely on `&str` command text extracted from a
//! workflow `run:` block — no YAML structure — so they are trivially
//! unit-testable. The structural jobs→steps model and the `check_scenario`
//! orchestrator live in the parent [`super`] module.

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
pub(crate) const FAIL_CLOSED_FLAGS: &[&str] =
    &["CQLITE_REQUIRE_FIXTURES", "CQLITE_PARITY_REQUIRE_DATASETS"];

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
pub(crate) fn is_truthy_value(raw: &str) -> bool {
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
///
/// Issue #1228 roborev finding A: a bare `--test <name>` token pair is NOT enough.
/// `echo cargo test --test foo` (or `printf`, `:`, a comment, any non-test command
/// that merely MENTIONS the tokens) used to satisfy this check. We therefore first
/// require the logical command's executable HEAD to be a real cargo test
/// invocation (`cargo test` / `cargo nextest run`) — mirroring the gradle
/// command-head approach in [`logical_runs_gradle`] — before accepting the
/// `--test <name>` pair.
fn logical_runs_test(logical: &str, name: &str) -> bool {
    if !logical_head_is_cargo_test(logical) {
        return false;
    }
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

/// True if a single logical command invokes `cargo` as the **executable** command
/// head (possibly after the usual `env VAR=val` / `VAR=val` / `sudo` prefixes we
/// strip elsewhere) AND its first cargo subcommand is a real test RUNNER —
/// `cargo test` or `cargo nextest run`. This rejects a command that merely MENTIONS
/// the `--test <name>` tokens as arguments to some OTHER program (`echo cargo test
/// --test foo`, `printf ...`, `:`, etc.). Subcommands are matched as whole tokens.
fn logical_head_is_cargo_test(logical: &str) -> bool {
    let mut toks = logical.split_whitespace().peekable();
    // Skip leading inline-assignment / `env` / `sudo` prefixes that precede the
    // real executable (same prefix shapes [`logical_runs_gradle`] strips).
    while let Some(&tok) = toks.peek() {
        if tok == "sudo" || tok == "env" || (tok.contains('=') && !tok.starts_with('=')) {
            toks.next();
        } else {
            break;
        }
    }
    // The executable head must be `cargo` (whole token, optional `./` prefix is not
    // conventional for cargo so we do not strip it here).
    if toks.next() != Some("cargo") {
        return false;
    }
    // The first non-flag token after `cargo` is the subcommand. Accept `test`, or
    // `nextest run` (cargo-nextest's run subcommand). Flags between `cargo` and the
    // subcommand (e.g. `cargo +nightly test`) are skipped.
    let mut sub = toks.by_ref().find(|t| !t.starts_with('-'));
    match sub.take() {
        Some("test") => true,
        Some("nextest") => {
            // `cargo nextest run` — the next non-flag token must be `run`.
            toks.find(|t| !t.starts_with('-')) == Some("run")
        }
        _ => false,
    }
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
/// step/job/workflow `env:` map path is handled separately via the parent
/// module's `EvaluatedStep::scope_fail_closed`.
pub(crate) fn inline_fail_closed_for_test(command: &str, name: &str) -> bool {
    // Issue #1228 roborev finding A: a shell `export FOO=<truthy>` only affects
    // SUBSEQUENT commands, so we must walk the logical commands IN ORDER and only
    // credit an export seen STRICTLY BEFORE the mapped-test command. An export
    // that appears only after the test command never reaches the spawned process.
    let mut exported_fail_closed = false;
    for logical in logical_commands(command) {
        // The mapped-test command is credited if the export was armed by a PRIOR
        // command, OR this same logical command carries an inline / `env`
        // fail-closed prefix on the test invocation itself.
        if logical_runs_test(&logical, name)
            && (exported_fail_closed || logical_inline_prefix_fail_closed(&logical))
        {
            return true;
        }
        // Arm the running export flag only AT/AFTER an `export FOO=<truthy>`
        // statement, so it is visible to later commands but not this one.
        if logical_exports_fail_closed(&logical) {
            exported_fail_closed = true;
        }
    }
    false
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
pub(crate) fn command_fails_build(command: &str) -> bool {
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
