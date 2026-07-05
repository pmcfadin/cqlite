//! Issue #1694 regression tests — data-safe diagnostic logging.
//!
//! Data-safety mandate: library diagnostic logs (and `eprintln!`) must record
//! the SHAPE of a query (predicate counts, column names, types, row counts) and
//! NEVER user data (WHERE-clause literals, row-key bytes, cell values, or the
//! raw SQL text) at any reachable verbosity.
//!
//! These tests **fail on the pre-fix `main` and pass after the fix**:
//!   1. `no_runtime_eprintln_in_query_executor_source` — the legacy executor had
//!      7 `eprintln!("DEBUG: …")` printf-debugging sites (the only runtime,
//!      non-`#[cfg(test)]`, `eprintln!` in library code); assert they are gone.
//!   2. `no_stdio_writes_in_query_execution_path` — the whole SELECT execution
//!      path must diagnose via `log`, never stdout/stderr, so `Database::execute`
//!      cannot leak to stderr. (In-process fd capture of `eprintln!` is not
//!      viable here because libtest intercepts the print macros at the Rust
//!      level, bypassing fd 2 — so this is enforced at the source.)
//!   3. `where_clause_literal_is_never_logged` — run a `WHERE name = <sentinel>`
//!      scan with a capturing `log` logger at the CLI-default (and stricter)
//!      level; the captured records must not contain the sentinel. Pre-fix, the
//!      `Executing SSTableScan … predicates={:?}` INFO log AND the
//!      `Database::execute('{sql}')` DEBUG log both leaked it.

/// A recognizable, otherwise-nonexistent value we plant in the WHERE clause. If
/// it appears anywhere in captured logs, user data leaked.
/// Only used by the cli-helpers-gated `where_clause_literal_is_never_logged`
/// test; gate the const identically so default-feature builds don't see it as
/// dead code under `-D warnings`.
#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
const SENTINEL: &str = "CQLITE_SENTINEL_1694_DO_NOT_LOG";

/// Runtime source modules on the `Database::execute` SELECT path. They diagnose
/// exclusively through `log`; none may contain a stdout/stderr write macro.
/// `src/lib.rs` is included because it hosts `Database::execute` itself — the
/// SQL-leak fix removed a raw-SQL log from that function (see
/// `lib_execute_does_not_log_raw_sql` for the value-bearing-log guard).
const QUERY_EXECUTION_MODULES: &[&str] = &[
    "src/lib.rs",
    "src/query/executor.rs",
    "src/query/engine.rs",
    "src/query/select_executor/execute.rs",
    "src/query/select_executor/mod.rs",
    "src/query/select_executor/value_ops.rs",
    "src/query/select_executor/row_build.rs",
];

/// Return `(line, text)` for every line in `rel_path` that uses a stdout/stderr
/// write macro, skipping comment lines (so doc comments mentioning the macros do
/// not false-positive).
fn stdio_write_hits(rel_path: &str) -> Vec<(usize, String)> {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel_path);
    let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    const MACROS: &[&str] = &["eprintln!", "eprint!", "println!", "print!"];
    src.lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !t.starts_with("//") && MACROS.iter().any(|m| l.contains(m))
        })
        .map(|(i, l)| (i + 1, l.trim().to_string()))
        .collect()
}

/// Issue #1694: the legacy `QueryExecutor` (query/executor.rs) must contain zero
/// `eprintln!` — they were value-carrying printf debugging (row keys, INSERT
/// conditions) and the only runtime `eprintln!` in library code.
#[test]
fn no_runtime_eprintln_in_query_executor_source() {
    let hits: Vec<(usize, String)> = stdio_write_hits("src/query/executor.rs")
        .into_iter()
        .filter(|(_, l)| l.contains("eprintln!"))
        .collect();
    assert!(
        hits.is_empty(),
        "query/executor.rs must not contain eprintln! (data-safety #1694); found: {hits:#?}"
    );
}

/// Issue #1694: no module on the `Database::execute` SELECT path may write to
/// stdout/stderr — diagnostics go through `log` (whose records carry only shapes
/// after this fix). This is the source-level guarantee behind "execute emits no
/// stderr".
#[test]
fn no_stdio_writes_in_query_execution_path() {
    let mut all = Vec::new();
    for module in QUERY_EXECUTION_MODULES {
        for (line, text) in stdio_write_hits(module) {
            all.push(format!("{module}:{line}: {text}"));
        }
    }
    assert!(
        all.is_empty(),
        "Query-execution modules must diagnose via `log`, never stdout/stderr \
         (data-safety #1694); found: {all:#?}"
    );
}

/// Extract the brace-delimited body of the first function whose text contains
/// `sig` (e.g. `"fn execute(&self, sql: &str)"`). Returns the substring from the
/// opening `{` through its matching `}`. Format strings in library code keep
/// their `{`/`}` balanced, so the simple depth counter is sufficient here.
/// Generic over the source string, so it serves any module (not just src/lib.rs).
fn fn_body(src: &str, sig: &str) -> String {
    let start = src
        .find(sig)
        .unwrap_or_else(|| panic!("signature not found: {sig}"));
    let rest = &src[start..];
    let bytes = rest.as_bytes();
    let open = rest.find('{').expect("function has no opening brace");
    let mut depth = 0usize;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return rest[open..=i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after signature: {sig}");
}

/// Return the full parenthesized argument text of every diagnostic/print macro
/// call in `text` (multi-line aware via paren balancing). Covers both `log::`
/// macros (the leak's actual channel) and the stdout/stderr macros.
fn diagnostic_macro_args(text: &str) -> Vec<String> {
    const MACROS: &[&str] = &[
        "log::error!",
        "log::warn!",
        "log::info!",
        "log::debug!",
        "log::trace!",
        "eprintln!",
        "eprint!",
        "println!",
        "print!",
    ];
    let bytes = text.as_bytes();
    let mut args = Vec::new();
    for mac in MACROS {
        let mut from = 0;
        while let Some(rel) = text[from..].find(mac) {
            let after = from + rel + mac.len();
            // Skip whitespace to the opening paren.
            let mut i = after;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'(' {
                let mut depth = 0i32;
                let mut j = i;
                while j < bytes.len() {
                    match bytes[j] {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                args.push(text[i..=j.min(bytes.len() - 1)].to_string());
                from = j + 1;
            } else {
                from = after;
            }
        }
    }
    args
}

/// True if `arg` references the raw `sql` query text: either an inline
/// `{sql...}` capture in a format string or the bare `sql` identifier as a
/// value argument.
fn references_raw_sql(arg: &str) -> bool {
    if arg.contains("{sql") {
        return true;
    }
    let bytes = arg.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while let Some(rel) = arg[i..].find("sql") {
        let pos = i + rel;
        let before_ok = pos == 0 || !is_word(bytes[pos - 1]);
        let after = pos + 3;
        let after_ok = after >= bytes.len() || !is_word(bytes[after]);
        if before_ok && after_ok {
            return true;
        }
        i = pos + 3;
    }
    false
}

/// Issue #1694: `Database::execute` (src/lib.rs) must diagnose the SHAPE only
/// (rows affected), NEVER the raw `sql` query text — a query string carries user
/// data (WHERE-clause literals). Pre-fix it logged `Database::execute('{sql}')`
/// at DEBUG. The runtime sentinel guard (`where_clause_literal_is_never_logged`)
/// covers this path but is feature+Data.db-gated and can skip; this ALWAYS-ON
/// source guard rejects a regression unconditionally.
#[test]
fn lib_execute_does_not_log_raw_sql() {
    let path = format!("{}/src/lib.rs", env!("CARGO_MANIFEST_DIR"));
    let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let body = fn_body(&src, "fn execute(&self, sql: &str)");
    let leaks: Vec<String> = diagnostic_macro_args(&body)
        .into_iter()
        .filter(|arg| references_raw_sql(arg))
        .collect();
    assert!(
        leaks.is_empty(),
        "Database::execute must not log the raw `sql` text (data-safety #1694); \
         found value-bearing diagnostic(s): {leaks:#?}"
    );
}

/// True if `arg` (a diagnostic macro's full argument text) formats a predicate
/// VALUE rather than its SHAPE. Two value-bearing patterns are rejected. First, a
/// debug format spec (`{:?}`, `{:#?}`, `{predicates:?}`) whose text also names the
/// `predicate`/`filter` — i.e. `Debug`-dumping the predicate list, which prints
/// the underlying `Value` literals (the exact #1694 leak `predicates={:?}`).
/// Second, a field access of `SSTablePredicate.values` (`p.values`, `.values,`) —
/// the `Vec<Value>` literal store — as opposed to a `.values()` iterator method
/// (allowed: shapes can be iterated). Shape-only references (`predicates.len()`,
/// `p.column`, column names, counts) are NOT flagged.
fn references_predicate_value(arg: &str) -> bool {
    // Debug-format of the predicate/filter (`?}` closes {:?} / {:#?} / {x:?}).
    let has_debug_fmt = arg.contains("?}");
    let names_predicate = arg.contains("predicate") || arg.contains("filter");
    if has_debug_fmt && names_predicate {
        return true;
    }
    // `.values` field access (not the `.values()` iterator method).
    let bytes = arg.as_bytes();
    let mut i = 0;
    while let Some(rel) = arg[i..].find(".values") {
        let pos = i + rel;
        let after = pos + ".values".len();
        if bytes.get(after).copied() != Some(b'(') {
            return true;
        }
        i = after;
    }
    false
}

/// Issue #1694: `execute_sstable_scan` (query/select_executor/execute.rs) must
/// log the scan SHAPE only (predicate count, constrained column names) and NEVER
/// predicate VALUES. Pre-fix it logged `Executing SSTableScan … predicates={:?}`
/// at INFO (CLI default), dumping the WHERE-clause literals. The runtime sentinel
/// guard (`where_clause_literal_is_never_logged`) exercises this path but is
/// feature+Data.db-gated and can skip; this ALWAYS-ON source guard rejects a
/// regression to a value-bearing scan diagnostic unconditionally.
#[test]
fn execute_sstable_scan_does_not_log_predicate_values() {
    let path = format!(
        "{}/src/query/select_executor/execute.rs",
        env!("CARGO_MANIFEST_DIR")
    );
    let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let body = fn_body(&src, "fn execute_sstable_scan(");
    let leaks: Vec<String> = diagnostic_macro_args(&body)
        .into_iter()
        .filter(|arg| references_predicate_value(arg))
        .collect();
    assert!(
        leaks.is_empty(),
        "execute_sstable_scan must log predicate SHAPE only (counts, column names), never \
         predicate VALUES (data-safety #1694); found value-bearing diagnostic(s): {leaks:#?}"
    );
}

// ============================================================================
// Runtime behavioral guard — requires cli-helpers + state_machine + Data.db.
// ============================================================================

#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
mod integration {
    use super::SENTINEL;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::Database;
    use serial_test::serial;

    // ---- capturing `log` logger --------------------------------------------

    static LOG_BUFFER: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    struct CapturingLogger;
    impl log::Log for CapturingLogger {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            if let Some(buf) = LOG_BUFFER.get() {
                if let Ok(mut v) = buf.lock() {
                    v.push(format!(
                        "{} [{}] {}",
                        record.level(),
                        record.target(),
                        record.args()
                    ));
                }
            }
        }
        fn flush(&self) {}
    }

    /// Install the capturing logger once for this test binary. We capture at
    /// `Trace` (a superset of the CLI-default `Info`), so "no sentinel at the
    /// CLI-default level" is proven and strengthened to "no sentinel at ANY
    /// level" — matching the shapes-not-values mandate.
    fn install_logger() {
        LOG_BUFFER.get_or_init(|| Mutex::new(Vec::new()));
        let _ = log::set_boxed_logger(Box::new(CapturingLogger)); // idempotent
        log::set_max_level(log::LevelFilter::Trace);
    }

    fn clear_logs() {
        if let Some(buf) = LOG_BUFFER.get() {
            if let Ok(mut v) = buf.lock() {
                v.clear();
            }
        }
    }

    fn captured_logs() -> Vec<String> {
        LOG_BUFFER
            .get()
            .and_then(|b| b.lock().ok().map(|v| v.clone()))
            .unwrap_or_default()
    }

    // ---- fixture setup (mirrors the issue_548 integration harness) ----------

    fn get_datasets_root() -> Option<PathBuf> {
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.exists())
    }

    fn get_schemas_dir() -> Option<PathBuf> {
        if let Some(datasets_root) = get_datasets_root() {
            let schemas_dir = datasets_root.parent()?.join("schemas");
            if schemas_dir.exists() {
                return Some(schemas_dir);
            }
        }
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
        schemas_dir.exists().then_some(schemas_dir)
    }

    fn data_db_files_exist() -> bool {
        let Some(datasets_root) = get_datasets_root() else {
            return false;
        };
        let sstables_dir = datasets_root.join("sstables").join("test_basic");
        let Ok(entries) = std::fs::read_dir(&sstables_dir) else {
            return false;
        };
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(files) = std::fs::read_dir(entry.path()) {
                    for file in files.flatten() {
                        if file
                            .file_name()
                            .to_str()
                            .is_some_and(|n| n.ends_with("-Data.db"))
                        {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    async fn setup_test_basic_database() -> Result<Database, String> {
        let datasets_root =
            get_datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or does not exist")?;
        let schemas_dir = get_schemas_dir().ok_or("schemas directory not found")?;

        let schema_path = schemas_dir.join("basic-types.cql");
        if !schema_path.exists() {
            return Err(format!("Schema not found: {schema_path:?}"));
        }

        let data_dir = datasets_root.join("sstables");
        if !data_dir.exists() {
            return Err(format!("sstables directory not found: {data_dir:?}"));
        }

        let ingestion_config = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: None,
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some("/test_basic/".to_string()),
        };

        let result = ingest(ingestion_config)
            .await
            .map_err(|e| format!("ingestion failed: {e}"))?;
        Ok(result.database)
    }

    /// Issue #1694: a WHERE-clause literal (and the raw SQL text) must never
    /// reach the diagnostic logs.
    ///
    /// Pre-fix, two sites leaked it at reachable levels:
    ///   * `execute_sstable_scan` logged `predicates={:?}` at INFO (CLI default);
    ///   * `Database::execute` logged the whole `'{sql}'` at DEBUG.
    /// Both are reshaped to log shapes (counts/column names) only.
    #[tokio::test]
    #[serial]
    async fn where_clause_literal_is_never_logged() {
        if !data_db_files_exist() {
            eprintln!("where_clause_literal_is_never_logged: SKIPPED (no Data.db files)");
            return;
        }
        install_logger();

        let db = match setup_test_basic_database().await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("where_clause_literal_is_never_logged: SKIPPED ({e})");
                return;
            }
        };

        clear_logs();

        // A `WHERE <non-pk> = <literal>` scan routes through the SelectExecutor
        // SSTableScan path (a pre-fix INFO leak site). We do not care whether any
        // row matches — only that the sentinel literal is never logged.
        let query =
            format!("SELECT id, name FROM test_basic.simple_table WHERE name = '{SENTINEL}'");
        // The query MUST run to completion — a failed execute would make the
        // no-leak assertion vacuous (nothing was scanned/logged).
        db.execute(&query)
            .await
            .expect("WHERE-clause scan query should execute successfully");

        let logs = captured_logs();
        let leaked: Vec<&String> = logs.iter().filter(|l| l.contains(SENTINEL)).collect();
        assert!(
            leaked.is_empty(),
            "WHERE-clause literal / SQL text leaked into diagnostic logs (data-safety #1694): \
             {leaked:#?}"
        );
        // Non-vacuous: prove the SSTableScan diagnostic path actually ran and was
        // logged. This is the exact site that pre-fix leaked `predicates={:?}`, so
        // if the scan log stops being emitted the test fails (rather than passing
        // trivially because nothing was logged).
        assert!(
            logs.iter().any(|l| l.contains("Executing SSTableScan")),
            "expected an `Executing SSTableScan` diagnostic proving the scan path ran \
             (test would be vacuous otherwise); captured: {logs:#?}"
        );
    }
}
