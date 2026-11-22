//! Public-surface checks for `cqlite diff` (issue #4201; spec `cli-diff` R9-R13).
//!
//! These tests spawn the binary. Expectations are derived from Cassandra, never from
//! `cqlite diff`'s own output (#3041/#3042):
//!
//!   * `test_tomb.resurrection_gc_positive` (datasets-v4, Cassandra 5.0.8; issue AC 1): side A =
//!     both generations, side B = generation 1 only. The committed expected JSON was produced by
//!     `fixtures/diff/derive_resurrection_gc_positive.py` from Cassandra's `sstabledump` goldens
//!     and Cassandra's rules (`DeletionTime.java:173-175`, `PurgeFunction.java:41-42`).
//!   * `test_diff.risk_cases` and `test_explain.trace_decisions` (committed, Cassandra 5.0.8),
//!     whose outcomes follow from pinned `USING TIMESTAMP` mutations (their READMEs).

use assert_cmd::Command;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// `--now` one second past `ldt (1794358424) + gc_grace (864000)` = 1795222424.
const NOW_PAST: &str = "1795222425";
const NOW_BOUNDARY: &str = "1795222424";

fn repo(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(rel)
}

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// The fetched `test_tomb.resurrection_gc_positive` generation directory (it is NOT committed);
/// `None` is a SKIP-clean absence that `CQLITE_REQUIRE_FIXTURES=1` turns into a failure.
fn fetched_dir(keyspace: &str, table: &str, marker: &str) -> Option<PathBuf> {
    let mut roots = Vec::new();
    if let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(root).join("sstables"));
    }
    roots.push(repo("test-data/datasets/sstables"));
    for root in &roots {
        let Ok(entries) = std::fs::read_dir(root.join(keyspace)) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&format!("{table}-")) && entry.path().join(marker).is_file() {
                return Some(entry.path());
            }
        }
    }
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent under {roots:?}");
    }
    eprintln!("[skip] {keyspace}.{table} not present under {roots:?}");
    None
}

fn gc_positive_src() -> Option<PathBuf> {
    fetched_dir("test_tomb", "resurrection_gc_positive", "nb-2-big-Data.db")
}

fn risk_src() -> PathBuf {
    fetched_dir_committed(
        repo("test-data/fixtures/issue_4201/test_diff"),
        "risk_cases",
    )
}

fn trace_src() -> PathBuf {
    fetched_dir_committed(
        repo("test-data/datasets/sstables/test_explain"),
        "trace_decisions",
    )
}

/// A committed fixture directory: absence is a failure, never a skip.
fn fetched_dir_committed(root: PathBuf, table: &str) -> PathBuf {
    for entry in std::fs::read_dir(&root)
        .unwrap_or_else(|error| panic!("committed fixture root {}: {error}", root.display()))
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{table}-")) && entry.path().join("nb-1-big-Data.db").is_file()
        {
            return entry.path();
        }
    }
    panic!(
        "committed fixture {table} not found under {}",
        root.display()
    );
}

fn schema(file: &str) -> PathBuf {
    repo(&format!("test-data/schemas/{file}"))
}

/// Copy the files of `src` whose names start with any of `prefixes` into `<root>/<name>/<ks>/<dir>`
/// (a Cassandra-shaped data root holding exactly one table directory) and return `<root>/<name>`.
fn stage(root: &Path, name: &str, src: &Path, keyspace: &str, prefixes: &[&str]) -> PathBuf {
    let side = root.join(name);
    let table_dir = side
        .join(keyspace)
        .join(src.file_name().expect("fixture dir name"));
    std::fs::create_dir_all(&table_dir).expect("create staged dir");
    let mut copied = 0;
    for entry in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        let file = entry.file_name().to_string_lossy().into_owned();
        if entry.path().is_file() && prefixes.iter().any(|p| file.starts_with(p)) {
            std::fs::copy(entry.path(), table_dir.join(&file)).expect("copy");
            copied += 1;
        }
    }
    assert!(copied > 0, "staged nothing from {}", src.display());
    side
}

fn table_dir_of(side: &Path, keyspace: &str) -> PathBuf {
    std::fs::read_dir(side.join(keyspace))
        .expect("staged keyspace dir")
        .flatten()
        .next()
        .expect("one table dir")
        .path()
}

fn snapshot_dir(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
            let path = entry.path();
            if entry.file_type().expect("file type").is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, std::fs::read(&path).expect("read file")));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn diff(
    schema_file: &str,
    a: &Path,
    b: &Path,
    table: &str,
    extra: &[&str],
) -> std::process::Output {
    Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args(["--schema", schema(schema_file).to_str().unwrap(), "diff"])
        .arg(a)
        .arg(b)
        .args(["--table", table])
        .args(extra)
        .output()
        .expect("diff process starts")
}

fn gc_diff(a: &Path, b: &Path, extra: &[&str]) -> std::process::Output {
    diff(
        "tombstone-parity.cql",
        a,
        b,
        "test_tomb.resurrection_gc_positive",
        extra,
    )
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `(first line, parsed JSON)` of a successful `--out json` run.
fn json_report(output: &std::process::Output) -> (String, Value) {
    assert!(output.status.success(), "diff failed: {}", stderr(output));
    let text = stdout(output);
    let mut lines = text.lines();
    let first = lines.next().expect("first line").to_string();
    let body: Value = serde_json::from_str(lines.next().expect("JSON object")).expect("JSON");
    assert!(
        lines.next().is_none(),
        "stdout after the first line is ONE JSON object"
    );
    (first, body)
}

/// Replace each side's directory by the label of the PHYSICAL directory it is (`<a>` for the
/// staged `a/` tree, `<b>` for `b/`), so a swapped run (whose `side_a` is the physical `b/`)
/// compares meaningfully after relabelling.
fn normalised(mut report: Value) -> Value {
    for side in ["side_a", "side_b"] {
        let dir = report[side]["dir"].as_str().expect("dir").to_string();
        let label = if dir.contains("/a/test_tomb/") || dir.contains("/a/test_") {
            "<a>"
        } else {
            "<b>"
        };
        report[side]["dir"] = label.into();
    }
    report
}

fn gc_sides(root: &Path) -> Option<(PathBuf, PathBuf)> {
    let src = gc_positive_src()?;
    Some((
        stage(root, "a", &src, "test_tomb", &["nb-1-big-", "nb-2-big-"]),
        stage(root, "b", &src, "test_tomb", &["nb-1-big-"]),
    ))
}

// ---------------------------------------------------------------------------------------------
// R9 — the verb exists, resolves each side, states its clock first.
// ---------------------------------------------------------------------------------------------

#[test]
fn r9_1_first_line_states_the_clock_and_both_sides_generations() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let output = gc_diff(&a, &b, &["--now", NOW_PAST, "--out", "json"]);
    let (first, _) = json_report(&output);
    assert!(
        first.starts_with("now=1795222425 (2026-11-21T00:53:45"),
        "{first}"
    );
    assert!(
        first.contains("side_a: generations=2 [nb-1-big, nb-2-big]"),
        "{first}"
    );
    assert!(
        first.contains("side_b: generations=1 [nb-1-big]"),
        "{first}"
    );
    assert!(!first.contains("(wall clock)"));
    // The text format states the same first line.
    let text = gc_diff(&a, &b, &["--now", NOW_PAST]);
    assert!(text.status.success());
    assert_eq!(stdout(&text).lines().next().unwrap(), first);
}

#[test]
fn r9_2_now_absent_is_wall_clock_and_says_so() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let output = gc_diff(&a, &b, &["--out", "json"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.lines().next().unwrap().contains("(wall clock)"));
}

#[test]
fn a_table_directory_path_resolves_like_a_data_root() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let by_root = json_report(&gc_diff(&a, &b, &["--now", NOW_PAST, "--out", "json"])).1;
    let by_dir = json_report(&gc_diff(
        &table_dir_of(&a, "test_tomb"),
        &table_dir_of(&b, "test_tomb"),
        &["--now", NOW_PAST, "--out", "json"],
    ))
    .1;
    assert_eq!(by_root["partitions"], by_dir["partitions"]);
}

#[test]
fn help_says_the_verb_is_read_only_and_never_writes() {
    for flag in ["--help", "-h"] {
        let output = Command::cargo_bin("cqlite")
            .expect("cqlite binary is built")
            .args(["diff", flag])
            .output()
            .expect("help starts");
        assert!(output.status.success());
        let text = stdout(&output);
        assert!(text.contains("READ-ONLY"), "{flag}: {text}");
        if flag == "--help" {
            assert!(text.contains("never writes to"), "{text}");
            assert!(text.contains("no repair or apply mode"), "{text}");
            // R11.3: the effective result budget cap is stated.
            assert!(text.contains("capped at 32 MiB"), "{text}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// R10 — output shape.
// ---------------------------------------------------------------------------------------------

#[test]
fn r10_1_json_equals_the_expected_report_derived_from_cassandras_goldens() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    for (now, file) in [
        (NOW_PAST, "resurrection_gc_positive.now_past.expected.json"),
        (
            NOW_BOUNDARY,
            "resurrection_gc_positive.now_boundary.expected.json",
        ),
    ] {
        let (_, report) = json_report(&gc_diff(&a, &b, &["--now", now, "--out", "json"]));
        let expected: Value = serde_json::from_str(
            &std::fs::read_to_string(repo("cqlite-cli/tests/fixtures/diff").join(file)).unwrap(),
        )
        .unwrap();
        let keys: Vec<&str> = report
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            [
                "gc_grace_seconds",
                "now",
                "partitions",
                "side_a",
                "side_b",
                "summary",
                "table"
            ],
            "exactly the D4 keys"
        );
        assert_eq!(normalised(report), expected, "now={now}");
    }
}

#[test]
fn r10_2_identical_snapshots_render_an_affirmative_non_vacuous_report() {
    let root = tempfile::tempdir().unwrap();
    let src = trace_src();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_explain",
        &["nb-1-big-", "nb-2-big-"],
    );
    let b = stage(
        root.path(),
        "b",
        &src,
        "test_explain",
        &["nb-1-big-", "nb-2-big-"],
    );
    let output = diff(
        "explain-trace.cql",
        &a,
        &b,
        "test_explain.trace_decisions",
        &["--now", "1795399358", "--out", "json"],
    );
    let (_, report) = json_report(&output);
    let summary = &report["summary"];
    assert_eq!(summary["only_a"], 0);
    assert_eq!(summary["only_b"], 0);
    assert_eq!(summary["tombstones_only_a"], 0);
    // README: eight partitions exist; partition 3's `ck=3 v='outside-range'` survives on both.
    assert_eq!(summary["partitions_compared"], 8);
    assert!(summary["equal"].as_u64().unwrap() >= 1, "non-vacuous");
    for partition in report["partitions"].as_array().unwrap() {
        for cell in partition["cells"].as_array().unwrap() {
            assert_eq!(cell["status"], "equal", "{cell}");
        }
    }
    // The text format also names the true count (never a silent empty report).
    let text = diff(
        "explain-trace.cql",
        &a,
        &b,
        "test_explain.trace_decisions",
        &["--now", "1795399358"],
    );
    assert!(
        stdout(&text).contains("summary  partitions_compared=8"),
        "{}",
        stdout(&text)
    );
}

#[test]
fn risk_cases_text_report_names_the_resurrection_verdicts() {
    let root = tempfile::tempdir().unwrap();
    let src = risk_src();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_diff",
        &["nb-2-big-", "nb-3-big-"],
    );
    let b = stage(root.path(), "b", &src, "test_diff", &["nb-1-big-"]);
    // risk_cases boundary = 1795307390 + 864000 = 1796171390.
    let output = diff(
        "issue-4201-diff-risk.cql",
        &a,
        &b,
        "test_diff.risk_cases",
        &["--now", "1796171391"],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    // README: id3 is the equal-timestamp tie => yes; id1/id2/id5 hold newer-or-no data => no.
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("tombstone"))
        .collect();
    assert_eq!(lines.len(), 10, "{text}");
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.contains("resurrection_risk=yes (past-gc-grace)"))
            .count(),
        5,
        "ids 3, 11 (ties), 16 (element tombstone) and 18 (two element tombstones)"
    );
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.contains("resurrection_risk=no"))
            .count(),
        5
    );
    // Conflicts name both values and the side Cassandra keeps.
    assert!(
        text.contains("zebra@1970-01-01T00:00:00.005Z  b=apple@1970-01-01T00:00:00.005Z")
            && text.contains("conflict  resolves_to=a"),
        "{text}"
    );
    assert!(
        text.lines().last().unwrap().starts_with("summary  "),
        "summary last"
    );
}

// ---------------------------------------------------------------------------------------------
// R11 — `--partition`, and the byte budget.
// ---------------------------------------------------------------------------------------------

#[test]
fn r11_1_partition_scopes_the_report_to_that_partition() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let (_, report) = json_report(&gc_diff(
        &a,
        &b,
        &["--now", NOW_PAST, "--out", "json", "--partition", "2"],
    ));
    let partitions = report["partitions"].as_array().unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0]["partition_key"], "2");
    assert_eq!(partitions[0]["tombstones"].as_array().unwrap().len(), 1);
    assert_eq!(partitions[0]["cells"].as_array().unwrap().len(), 6);
    assert_eq!(report["summary"]["partitions_compared"], 1);
    // The scoped partition equals the same partition of the full report.
    let (_, full) = json_report(&gc_diff(&a, &b, &["--now", NOW_PAST, "--out", "json"]));
    assert_eq!(partitions[0], full["partitions"][1]);
    // Repeating --partition compares exactly those partitions.
    let (_, both) = json_report(&gc_diff(
        &a,
        &b,
        &[
            "--now",
            NOW_PAST,
            "--out",
            "json",
            "--partition",
            "1",
            "--partition",
            "2",
        ],
    ));
    assert_eq!(both["partitions"], full["partitions"]);
}

#[test]
fn r11_2_the_byte_budget_is_exit_two_with_no_partial_stdout() {
    let root = tempfile::tempdir().unwrap();
    let src = risk_src();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_diff",
        &["nb-2-big-", "nb-3-big-"],
    );
    let b = stage(root.path(), "b", &src, "test_diff", &["nb-1-big-"]);
    let cfg = root.path().join("cqlite.toml");
    std::fs::write(
        &cfg,
        "[performance]\nquery_timeout_ms = 30000\ncache_size_mb = 64\nmax_result_bytes = 64\n",
    )
    .unwrap();
    // The sides really diverge (so the budget bites on a real report, not an empty one).
    let unbounded = diff(
        "issue-4201-diff-risk.cql",
        &a,
        &b,
        "test_diff.risk_cases",
        &["--now", "1796171391", "--out", "json"],
    );
    let (_, full) = json_report(&unbounded);
    assert!(full["summary"]["only_b"].as_u64().unwrap() > 0);
    assert!(full["summary"]["tombstones_only_a"].as_u64().unwrap() > 0);
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "--schema",
            schema("issue-4201-diff-risk.cql").to_str().unwrap(),
            "diff",
        ])
        .arg(&a)
        .arg(&b)
        .args([
            "--table",
            "test_diff.risk_cases",
            "--now",
            "1796171391",
            "--out",
            "json",
        ])
        .output()
        .expect("diff starts");
    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    assert!(
        output.stdout.is_empty(),
        "a budget failure never writes a truncated report"
    );
    assert!(
        stderr(&output).contains("materialization budget"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn r11_2_wide_rows_full_report_over_a_small_budget_is_a_budget_error() {
    let Some(src) = fetched_dir("test_wide_rows", "chat_messages", "nb-1-big-Data.db")
        .or_else(|| fetched_dir_any_generation("test_wide_rows", "chat_messages"))
    else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_wide_rows",
        &["nb-", "da-", "oa-"],
    );
    let b = stage(
        root.path(),
        "b",
        &src,
        "test_wide_rows",
        &["nb-", "da-", "oa-"],
    );
    let cfg = root.path().join("cqlite.toml");
    std::fs::write(
        &cfg,
        "[performance]\nquery_timeout_ms = 30000\ncache_size_mb = 64\nmax_result_bytes = 4096\n",
    )
    .unwrap();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "--schema",
            schema("wide-rows.cql").to_str().unwrap(),
            "diff",
        ])
        .arg(&a)
        .arg(&b)
        .args([
            "--table",
            "test_wide_rows.chat_messages",
            "--now",
            "1794545000",
            "--out",
            "json",
        ])
        .output()
        .expect("diff starts");
    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    assert!(output.stdout.is_empty());
    assert!(
        stderr(&output).contains("materialization budget"),
        "{}",
        stderr(&output)
    );
}

fn fetched_dir_any_generation(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let dir = PathBuf::from(root).join("sstables").join(keyspace);
    std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{table}-")))
        })
}

// ---------------------------------------------------------------------------------------------
// R12 — exit codes and the read-only guarantee.
// ---------------------------------------------------------------------------------------------

#[test]
fn r12_1_unreadable_generation_on_side_b_is_exit_two_naming_side_and_path() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let stats = table_dir_of(&b, "test_tomb").join("nb-1-big-Statistics.db");
    let bytes = std::fs::read(&stats).unwrap();
    std::fs::write(&stats, &bytes[..bytes.len() / 3]).unwrap();
    for extra in [
        &["--now", NOW_PAST][..],
        &["--now", NOW_PAST, "--partition", "1"][..],
    ] {
        let output = gc_diff(&a, &b, extra);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{extra:?}: {}",
            stderr(&output)
        );
        let message = stderr(&output);
        assert!(message.contains("side=b"), "{message}");
        assert!(
            message.contains(stats.to_str().unwrap()),
            "names the truncated path: {message}"
        );
        assert!(
            stdout(&output).is_empty(),
            "no partition findings on stdout"
        );
    }
}

#[test]
fn r12_2_usage_errors_are_exit_one_and_write_nothing() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let empty = root.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let before = snapshot_dir(root.path());
    // A directory with no Data.db for the named table.
    let output = gc_diff(&empty, &b, &["--now", NOW_PAST]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(stderr(&output).contains("side=a"), "{}", stderr(&output));
    // The same on side B.
    let output = gc_diff(&a, &empty, &["--now", NOW_PAST]);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("side=b"));
    // A table absent from --schema.
    let output = diff(
        "tombstone-parity.cql",
        &a,
        &b,
        "test_tomb.no_such_table",
        &["--now", NOW_PAST],
    );
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    // A malformed --table (the global --schema/--table pair is validated before any directory read).
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args([
            "--schema",
            schema("tombstone-parity.cql").to_str().unwrap(),
            "diff",
        ])
        .arg(&a)
        .arg(&b)
        .args(["--table", "nodot", "--now", NOW_PAST])
        .output()
        .expect("diff starts");
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("keyspace.table"),
        "{}",
        stderr(&output)
    );
    // A bad --partition literal.
    let output = gc_diff(
        &a,
        &b,
        &["--now", NOW_PAST, "--partition", "not-a-literal ("],
    );
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("--partition"),
        "{}",
        stderr(&output)
    );
    // No --schema at all.
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .arg("diff")
        .arg(&a)
        .arg(&b)
        .args(["--table", "test_tomb.resurrection_gc_positive"])
        .output()
        .expect("diff starts");
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("--schema"), "{}", stderr(&output));
    assert_eq!(
        before,
        snapshot_dir(root.path()),
        "nothing was written anywhere"
    );
}

#[test]
fn r12_3_both_inputs_are_byte_identical_after_every_kind_of_run() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let before = (snapshot_dir(&a), snapshot_dir(&b));
    for extra in [
        &["--now", NOW_PAST][..],
        &["--now", NOW_PAST, "--out", "json"][..],
        &["--now", NOW_PAST, "--partition", "1"][..],
        &["--now", NOW_PAST, "--partition", "2", "--out", "json"][..],
    ] {
        let output = gc_diff(&a, &b, extra);
        assert!(output.status.success(), "{extra:?}: {}", stderr(&output));
        assert_eq!(
            (snapshot_dir(&a), snapshot_dir(&b)),
            before,
            "{extra:?} mutated an input"
        );
    }
    // No stray file appeared next to the inputs either.
    assert_eq!(
        snapshot_dir(root.path()).len(),
        before.0.len() + before.1.len()
    );
}

#[test]
fn snapshots_and_backups_subdirectories_are_never_read() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    // Plant a stale full copy of the table under `snapshots/<tag>/` (Cassandra's layout) on B: if
    // discovery recursed, B would suddenly hold generation 2's tombstones too and the divergence
    // would vanish.
    let src = gc_positive_src().expect("present: gc_sides just staged it");
    let planted = table_dir_of(&b, "test_tomb")
        .join("snapshots")
        .join("stale-tag");
    std::fs::create_dir_all(&planted).unwrap();
    for entry in std::fs::read_dir(&src).unwrap().flatten() {
        if entry.path().is_file() && entry.file_name().to_string_lossy().starts_with("nb-2-big-") {
            std::fs::copy(entry.path(), planted.join(entry.file_name())).unwrap();
        }
    }
    let (first, report) = json_report(&gc_diff(&a, &b, &["--now", NOW_PAST, "--out", "json"]));
    assert!(
        first.contains("side_b: generations=1 [nb-1-big]"),
        "{first}"
    );
    assert_eq!(
        report["summary"]["tombstones_only_a"], 3,
        "B did not pick up snapshots/ tombstones"
    );
}

// ---------------------------------------------------------------------------------------------
// R13 — symmetry observable from the binary.
// ---------------------------------------------------------------------------------------------

fn relabel(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, inner) in map {
                let swapped = match key.as_str() {
                    "a" => "b",
                    "b" => "a",
                    "side_a" => "side_b",
                    "side_b" => "side_a",
                    "only_a" => "only_b",
                    "only_b" => "only_a",
                    "newer_in_a" => "newer_in_b",
                    "newer_in_b" => "newer_in_a",
                    "tombstones_only_a" => "tombstones_only_b",
                    "tombstones_only_b" => "tombstones_only_a",
                    other => other,
                };
                let new_value = match (key.as_str(), inner) {
                    ("status" | "side" | "resolves_to", Value::String(text)) => Value::String(
                        match text.as_str() {
                            "a" => "b",
                            "b" => "a",
                            "only-a" => "only-b",
                            "only-b" => "only-a",
                            "newer-in-a" => "newer-in-b",
                            "newer-in-b" => "newer-in-a",
                            other => other,
                        }
                        .to_string(),
                    ),
                    _ => relabel(inner),
                };
                out.insert(swapped.to_string(), new_value);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(relabel).collect()),
        other => other.clone(),
    }
}

#[test]
fn r13_1_swapping_the_directories_yields_the_label_swapped_mirror() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let args = ["--now", NOW_PAST, "--out", "json"];
    let (_, forward) = json_report(&gc_diff(&a, &b, &args));
    let (_, backward) = json_report(&gc_diff(&b, &a, &args));
    assert_eq!(relabel(&normalised(forward)), normalised(backward));
}

// ---------------------------------------------------------------------------------------------
// Wiring: the diff's per-side trail is what `cqlite explain` reports for that side.
// ---------------------------------------------------------------------------------------------

#[test]
fn each_sides_winners_equal_what_explain_reports_for_that_side() {
    let root = tempfile::tempdir().unwrap();
    let Some((a, b)) = gc_sides(root.path()) else {
        return;
    };
    let (_, report) = json_report(&gc_diff(&a, &b, &["--now", NOW_PAST, "--out", "json"]));
    let mut compared = 0;
    for (label, side) in [("a", &a), ("b", &b)] {
        for key in ["1", "2"] {
            let output = Command::cargo_bin("cqlite")
                .expect("cqlite binary is built")
                .args([
                    "--schema",
                    schema("tombstone-parity.cql").to_str().unwrap(),
                    "--data-dir",
                    side.to_str().unwrap(),
                    "explain",
                    "test_tomb.resurrection_gc_positive",
                    key,
                    "--now",
                    NOW_PAST,
                    "--out",
                    "json",
                ])
                .output()
                .expect("explain starts");
            assert!(output.status.success(), "explain: {}", stderr(&output));
            let text = stdout(&output);
            let explained: Value = serde_json::from_str(text.lines().nth(1).unwrap()).unwrap();
            let mut winners: Vec<(String, String)> = explained["cells"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|c| c["verdict"] == "winner")
                .map(|c| {
                    let ck = c["clustering"]["columns"][0][1].clone();
                    (ck.to_string(), c["column"].as_str().unwrap().to_string())
                })
                .collect();
            winners.sort();
            let partition = report["partitions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["partition_key"] == key)
                .unwrap();
            let mut diffed: Vec<(String, String)> = partition["cells"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|c| !c[label].is_null())
                .map(|c| {
                    (
                        c["clustering"].as_str().unwrap().to_string(),
                        c["column"].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            diffed.sort();
            let explained_flat: Vec<(String, String)> = winners
                .into_iter()
                .map(|(ck, col)| {
                    (
                        ck.trim_matches('"')
                            .trim_start_matches("{\"Integer\":")
                            .trim_end_matches('}')
                            .to_string(),
                        col,
                    )
                })
                .collect();
            assert_eq!(diffed, explained_flat, "side {label} partition {key}");
            compared += diffed.len();
        }
    }
    // Golden: A keeps 7 live cells (partition 1; partition 2 is wholly deleted) and B all 16
    // (partition 1: 5 rows x 2 columns, partition 2: 3 x 2): 7 + 16 = 23. Never a vacuous
    // comparison of two empty sets.
    assert_eq!(compared, 7 + 16);
}

// ---------------------------------------------------------------------------------------------
// Conflicts (owner Q21=b): named from the binary, JSON and text.
// ---------------------------------------------------------------------------------------------

#[test]
fn conflicts_are_reported_with_both_values_and_resolves_to() {
    let root = tempfile::tempdir().unwrap();
    let src = risk_src();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_diff",
        &["nb-2-big-", "nb-3-big-"],
    );
    let b = stage(root.path(), "b", &src, "test_diff", &["nb-1-big-"]);
    let args = [
        "--now",
        "1796171391",
        "--out",
        "json",
        "--partition",
        "9",
        "--partition",
        "11",
    ];
    let (_, report) = json_report(&diff(
        "issue-4201-diff-risk.cql",
        &a,
        &b,
        "test_diff.risk_cases",
        &args,
    ));
    let by_key = |key: &str| {
        report["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["partition_key"] == key)
            .unwrap()["cells"][0]
            .clone()
    };
    // README: id 9 zebra@5000 (A) vs apple@5000 (B): conflict, A keeps (larger bytes). id 11: A cell
    // tombstone @5000 vs B live @5000: conflict, the tombstone side keeps.
    let nine = by_key("9");
    assert_eq!(
        (nine["status"].as_str(), nine["resolves_to"].as_str()),
        (Some("conflict"), Some("a"))
    );
    assert_eq!(
        (nine["a"]["value"].as_str(), nine["b"]["value"].as_str()),
        (Some("zebra"), Some("apple"))
    );
    let eleven = by_key("11");
    assert_eq!(eleven["status"], "conflict");
    assert_eq!(eleven["resolves_to"], "a");
    assert_eq!(eleven["a"]["value"], "<tombstone>");
    assert_eq!(report["summary"]["conflicts"], 2);
    // Swapping the directories flips resolves_to (and only the labels).
    let (_, swapped) = json_report(&diff(
        "issue-4201-diff-risk.cql",
        &b,
        &a,
        "test_diff.risk_cases",
        &args,
    ));
    assert_eq!(swapped["partitions"][0]["cells"][0]["resolves_to"], "b");
}

#[test]
fn an_out_of_range_now_is_a_usage_error_naming_the_value() {
    let root = tempfile::tempdir().unwrap();
    let src = risk_src();
    let a = stage(
        root.path(),
        "a",
        &src,
        "test_diff",
        &["nb-2-big-", "nb-3-big-"],
    );
    let b = stage(root.path(), "b", &src, "test_diff", &["nb-1-big-"]);
    for now in ["9223372036854775807", "-9223372036854775808"] {
        let output = diff(
            "issue-4201-diff-risk.cql",
            &a,
            &b,
            "test_diff.risk_cases",
            &[format!("--now={now}").as_str()],
        );
        assert_eq!(output.status.code(), Some(1), "{now}: {}", stderr(&output));
        assert!(
            stderr(&output).contains(now),
            "names the value: {}",
            stderr(&output)
        );
        assert!(stdout(&output).is_empty());
    }
}
