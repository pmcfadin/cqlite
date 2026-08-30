//! Write-path methods of the Node.js `Database` surface, plus the CQL schema
//! parser the `writable: true` open path needs.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion apart from the inlined single-table schema parser,
//! which is now the named `parse_single_table_schema` the open path calls.

#[cfg(feature = "write-support")]
use std::path::Path;
#[cfg(feature = "write-support")]
use std::sync::Arc;

use napi_derive::napi;

use crate::error::simple_error;
#[cfg(feature = "write-support")]
use crate::error::{runtime_init_error, to_napi_error};

use super::{Database, MaintenanceOptions, MaintenanceReport, WriteStats};

/// Parse a CQL schema file that must describe exactly ONE table, and return
/// that table's schema for the write engine.
///
/// Extracted verbatim from the `open()` write-engine branch (issue #1464); the
/// logic, the error texts and the no-heuristics single-table rule are unchanged.
#[cfg(feature = "write-support")]
pub(super) fn parse_single_table_schema(
    path: &Path,
) -> napi::Result<cqlite_core::schema::TableSchema> {
    // Parse schema directly from CQL file (same as CLI write-only mode).
    // Mirrors the logic in cqlite-cli/src/main.rs that extracts keyspace
    // from USE/CREATE KEYSPACE statements before applying to table schemas.
    use cqlite_core::schema::cql_parser::{
        classify_statement, parse_create_table, split_cql_statements, StatementType,
    };
    let content = std::fs::read_to_string(path).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to read schema file '{}': {}",
            path.display(),
            e
        ))
    })?;
    let statements = split_cql_statements(&content);

    // Pass 1: collect keyspace from USE / CREATE KEYSPACE statements
    // and all table schemas (with keyspace applied).
    let mut file_keyspace: Option<String> = None;
    let mut table_schemas: Vec<cqlite_core::schema::TableSchema> = Vec::new();

    for stmt in &statements {
        match classify_statement(stmt) {
            StatementType::Other(ref kind) if kind == "use" => {
                // Extract keyspace from USE <keyspace>;
                let name = stmt
                    .trim()
                    .strip_prefix("USE")
                    .or_else(|| stmt.trim().strip_prefix("use"))
                    .unwrap_or("")
                    .trim()
                    .trim_end_matches(';')
                    .trim()
                    .to_string();
                if !name.is_empty() {
                    file_keyspace = Some(name);
                }
            }
            StatementType::Other(ref kind) if kind == "create" => {
                // Extract keyspace from CREATE KEYSPACE IF NOT EXISTS <name>
                let lower = stmt.to_lowercase();
                if lower.contains("create keyspace") {
                    let after = if let Some(pos) = lower.find("exists") {
                        &stmt[pos + 6..]
                    } else if let Some(pos) = lower.find("keyspace") {
                        &stmt[pos + 8..]
                    } else {
                        ""
                    };
                    let name = after
                        .trim()
                        .split(|c: char| c.is_whitespace() || c == '{' || c == ';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !name.is_empty() {
                        file_keyspace = Some(name);
                    }
                }
            }
            StatementType::CreateTable => {
                if let Ok((_remaining, mut ts)) = parse_create_table(stmt) {
                    // Apply file-level keyspace if table doesn't have one yet
                    if ts.keyspace.is_empty()
                        || ts.keyspace == "unknown"
                        || ts.keyspace == "default"
                    {
                        if let Some(ref ks) = file_keyspace {
                            ts.keyspace = ks.clone();
                        }
                    }
                    table_schemas.push(ts);
                }
            }
            _ => {}
        }
    }

    // Enforce single-table write target (Issue #28 no-heuristics mandate).
    // Silently picking one table would hide ambiguity; require callers to
    // provide a schema file with exactly one CREATE TABLE statement.
    match table_schemas.len() {
        0 => Err(napi::Error::from_reason(format!(
            "No CREATE TABLE statement found in schema file '{}'",
            path.display()
        ))),
        1 => Ok(table_schemas.into_iter().next().expect("length is 1")),
        count => Err(napi::Error::from_reason(format!(
            "Schema file '{}' contains {} CREATE TABLE statements. \
             The Node bindings currently support a single-table write \
             target. Specify a schema with exactly one CREATE TABLE.",
            path.display(),
            count
        ))),
    }
}

#[napi]
impl Database {
    /// Flush the in-memory write buffer (memtable) to an SSTable on disk.
    ///
    /// Returns the path to the created Data.db file.  If the memtable is empty
    /// an empty string is returned (no-op flush).
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @returns Promise resolving to the Data.db path, or "" if nothing was flushed
    /// @throws {CqliteError} If write support is not enabled or the flush fails
    ///
    /// @example
    /// ```javascript
    /// const db = await Database.open('/data', { schema: 'schema.cql', writable: true, writeDir: '/tmp/w' });
    /// await db.execute("INSERT INTO t (id) VALUES (1)");
    /// const sstablePath = await db.flushRun();
    /// console.log(`Flushed to: ${sstablePath}`);
    /// ```
    #[napi(js_name = "flushRun")]
    pub async fn flush_run(&self) -> napi::Result<String> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            let we = self.writable_engine()?;

            // `flush()` is async and takes &mut self on the engine.
            // We hold the Mutex lock and block_on inside a spawn_blocking to avoid
            // blocking the napi async executor thread.
            let we_clone = Arc::clone(we);

            let result = tokio::task::spawn_blocking(move || {
                let mut engine = we_clone
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                crate::runtime::block_on(engine.flush())
                    .map_err(runtime_init_error)?
                    .map_err(to_napi_error)
            })
            .await
            .map_err(|e| simple_error(format!("flush_run task panicked: {e}")))??;

            // Flush statistics (l0Count, totalWritten) are read straight from the
            // engine's own counters in `writeStats` (issue #1620), so there are no
            // Node-side counters to update here.
            match result {
                Some(info) => Ok(info.data_path.to_string_lossy().into_owned()),
                None => Ok(String::new()),
            }
        }

        #[cfg(not(feature = "write-support"))]
        Err(simple_error(
            "Write support not enabled. Build with --features write-support to enable write operations.",
        ))
    }

    /// Perform time-bounded background maintenance (compaction).
    ///
    /// Runs incremental compaction work within the provided time budget.
    /// Can be called repeatedly to drain pending compaction work.
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @param options - Optional maintenance options (default budgetMs: 100)
    /// @returns Promise resolving to a MaintenanceReport
    /// @throws {CqliteError} If write support is not enabled or maintenance fails
    ///
    /// @example
    /// ```javascript
    /// const report = await db.maintenanceStep({ budgetMs: 100 });
    /// console.log(`Merged ${report.rowsMerged} rows in ${report.timeSpentMs}ms`);
    /// if (report.pendingCompaction) {
    ///   console.log('More compaction work pending');
    /// }
    /// ```
    #[napi(js_name = "maintenanceStep")]
    pub async fn maintenance_step(
        &self,
        options: Option<MaintenanceOptions>,
    ) -> napi::Result<MaintenanceReport> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            let budget_ms = options.as_ref().and_then(|o| o.budget_ms).unwrap_or(100) as u64;

            let we_clone = Arc::clone(self.writable_engine()?);

            let report = tokio::task::spawn_blocking(move || {
                let mut engine = we_clone
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                let budget = std::time::Duration::from_millis(budget_ms);
                engine.maintenance_step(budget).map_err(to_napi_error)
            })
            .await
            .map_err(|e| simple_error(format!("maintenanceStep task panicked: {e}")))??;

            Ok(MaintenanceReport {
                time_spent_ms: report.time_spent.as_secs_f64() * 1000.0,
                rows_merged: report.rows_merged as f64,
                bytes_written: report.bytes_written as f64,
                completed_merges: report
                    .completed_merges
                    .iter()
                    .map(|p| p.to_string_lossy().into_owned())
                    .collect(),
                pending_compaction: report.pending_compaction,
            })
        }

        #[cfg(not(feature = "write-support"))]
        {
            let _ = options;
            Err(simple_error(
                "Write support not enabled. Build with --features write-support to enable write operations.",
            ))
        }
    }

    /// Get current write engine statistics (synchronous).
    ///
    /// Returns statistics about the in-memory write buffer (memtable) and WAL.
    /// All sizes are in bytes.
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @returns WriteStats snapshot
    /// @throws {CqliteError} If write support is not enabled
    ///
    /// @example
    /// ```javascript
    /// const stats = db.writeStats;
    /// console.log(`Memtable: ${stats.memtableSize} bytes, ${stats.memtableRows} rows`);
    /// console.log(`L0 files: ${stats.l0Count}`);
    /// ```
    #[napi(getter, js_name = "writeStats")]
    pub fn write_stats(&self) -> napi::Result<WriteStats> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            let we = self.writable_engine()?;
            let engine = we
                .lock()
                .map_err(|_| simple_error("Write engine lock poisoned"))?;

            // Read L0 count and cumulative flushed bytes from the engine's own
            // authoritative counters (issue #1620). The engine increments these
            // on EVERY flush — including the automatic flushes the `execute()`
            // path now performs via `execute_flushing` — so the stats stay
            // accurate for auto-flushes, not just explicit `flushRun()` calls.
            Ok(WriteStats {
                memtable_size: engine.memtable_size() as f64,
                memtable_rows: engine.memtable_row_count() as u32,
                wal_size: engine.wal_size() as f64,
                l0_count: engine.l0_count() as u32,
                total_written: engine.total_flushed_bytes() as f64,
            })
        }

        #[cfg(not(feature = "write-support"))]
        Err(simple_error(
            "Write support not enabled. Build with --features write-support to enable write operations.",
        ))
    }
}
