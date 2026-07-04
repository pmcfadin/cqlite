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
const SENTINEL: &str = "CQLITE_SENTINEL_1694_DO_NOT_LOG";

/// Runtime source modules on the `Database::execute` SELECT path. They diagnose
/// exclusively through `log`; none may contain a stdout/stderr write macro.
const QUERY_EXECUTION_MODULES: &[&str] = &[
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
        let _ = db.execute(&query).await;

        let logs = captured_logs();
        let leaked: Vec<&String> = logs.iter().filter(|l| l.contains(SENTINEL)).collect();
        assert!(
            leaked.is_empty(),
            "WHERE-clause literal / SQL text leaked into diagnostic logs (data-safety #1694): \
             {leaked:#?}"
        );
        // Sanity: the query actually produced diagnostic output, so a pass here
        // is meaningful rather than vacuous.
        assert!(
            !logs.is_empty(),
            "expected some diagnostic logs from the scan (test would be vacuous otherwise)"
        );
    }
}
