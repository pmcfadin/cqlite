//! Delta-export command handler (Issue #705, Epic #696 DS9).
//!
//! Wires `scan_delta` + `DeltaParquetWriter` end-to-end as the `delta-export`
//! CLI subcommand.  The subcommand is compiled only when the `delta-export`
//! feature is enabled (which also enables `cqlite-core/delta-scan` and
//! `cqlite-core/parquet`).
//!
//! ```bash
//! cqlite delta-export <sstable-dir> \
//!     --schema <file.cql> \
//!     --out parquet \
//!     -o <file.parquet>
//! ```

/// Result summary from a completed delta-export run.
#[derive(Debug)]
pub struct DeltaExportResult {
    /// Path to the output Parquet file.
    pub output_path: std::path::PathBuf,
    /// Total records written (all DeltaRecord variants).
    pub records_written: u64,
    /// Wall-clock time in milliseconds.
    pub execution_time_ms: f64,
    /// Number of collection element tombstones detected (v1 limitation warning).
    pub element_tombstone_warnings: u64,
}

impl DeltaExportResult {
    /// Print a one-line summary to stdout and warnings to stderr.
    ///
    /// When `element_tombstone_warnings > 0` two lines are emitted on stderr:
    /// 1. A stable machine-readable key `cqlite.delta.element_tombstones=<n>`
    ///    that scripts and the generation harness can parse without fragile
    ///    free-text matching.
    /// 2. The human-readable warning message (kept for operator visibility).
    pub fn display(&self) {
        println!(
            "delta-export: {} record(s) written to {} ({:.1}ms)",
            self.records_written,
            self.output_path.display(),
            self.execution_time_ms,
        );
        if self.element_tombstone_warnings > 0 {
            // Machine-readable key — parsed by generate-delta-roundtrip.sh
            // and by any downstream consumer that needs the count without
            // screen-scraping the human-readable message below.
            eprintln!(
                "cqlite.delta.element_tombstones={}",
                self.element_tombstone_warnings
            );
            // Human-readable warning (preserved for operator visibility).
            eprintln!(
                "warning: {} collection element tombstone(s) detected but not represented \
                 in v1 delta output (issue #493 — element-level fidelity is a tracked follow-up)",
                self.element_tombstone_warnings
            );
        }
    }
}

/// Enforce delta-export's single bare-`CREATE TABLE` schema contract (issue #1489).
///
/// delta-export requires a schema file containing exactly one bare `CREATE TABLE`
/// statement — no `CREATE KEYSPACE` / `USE` preamble and no trailing statements.
/// Without this guard a leading preamble fails with an opaque raw nom `{:?}` debug
/// dump, and statements *after* the `CREATE TABLE` are silently discarded. Both are
/// fail-open surprises. For CQL schema files we require **exactly one** top-level
/// statement (via `split_cql_statements`) **and** that it classify as a
/// `CREATE TABLE` (via `classify_statement`) — a zero-statement schema or a single
/// non-`CREATE TABLE` statement (e.g. a lone `CREATE KEYSPACE`) would otherwise fall
/// through to the legacy parser and reproduce the opaque error this guard replaces.
/// JSON schema documents are exempt: they are validated by the JSON loader, not this
/// contract.
#[cfg(feature = "delta-export")]
fn ensure_bare_create_table_schema(
    schema_path: &std::path::Path,
    content: &str,
) -> anyhow::Result<()> {
    use cqlite_core::schema::cql_parser::{classify_statement, StatementType};

    let is_cql = matches!(
        schema_path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase())
            .as_deref(),
        Some("cql") | Some("sql") | None
    );
    if !is_cql {
        return Ok(());
    }

    let statements = cqlite_core::schema::cql_parser::split_cql_statements(content);
    let is_single_create_table = statements.len() == 1
        && matches!(
            classify_statement(&statements[0]),
            StatementType::CreateTable
        );
    if !is_single_create_table {
        return Err(anyhow::anyhow!(
            "delta-export requires a bare CREATE TABLE (no CREATE KEYSPACE / USE preamble)"
        ));
    }
    Ok(())
}

/// Run the `delta-export` subcommand.
///
/// This function is the `#[cfg(feature = "delta-export")]` path.  When the
/// feature is absent the caller emits a "rebuild with --features delta-export"
/// error instead of calling this function.
#[cfg(feature = "delta-export")]
pub async fn handle_delta_export(
    args: &crate::cli_types::DeltaExportArgs,
) -> anyhow::Result<DeltaExportResult> {
    use crate::cli_types::{DeltaCompressionCodec, DeltaOutFormat};
    use cqlite_core::export::delta_parquet::{
        DeltaParquetCompression, DeltaParquetOptions, DeltaParquetWriter,
    };
    use cqlite_core::export::delta_schema::DeltaSchemaOpts;
    use cqlite_core::storage::sstable::reader::delta_scan::scan_delta;
    use std::io::BufWriter;
    use std::time::Instant;

    let start = Instant::now();

    // -----------------------------------------------------------------------
    // Validate output format (Finding 2): exhaustive match so that adding a
    // new DeltaOutFormat variant is a compile-time error, not a silent no-op.
    // Only Parquet is supported in v1; future variants (e.g. Arrow IPC) must
    // add a new arm here.
    // -----------------------------------------------------------------------
    match args.out {
        DeltaOutFormat::Parquet => {
            // Parquet is the only supported format.  The match is intentionally
            // exhaustive so the compiler flags any new variant that is added to
            // DeltaOutFormat without a corresponding handler here.
        }
    }

    // -----------------------------------------------------------------------
    // Validate output path: refuse to overwrite unless --overwrite
    // -----------------------------------------------------------------------
    if args.output.exists() && !args.overwrite {
        return Err(anyhow::anyhow!(
            "Output file already exists: {}\n\
             Use --overwrite to replace it.",
            args.output.display()
        ));
    }

    // -----------------------------------------------------------------------
    // Load schema (errors at schema-derivation time, before any I/O)
    // -----------------------------------------------------------------------
    // Enforce the single bare-CREATE-TABLE contract before loading (issue #1489):
    // a leading CREATE KEYSPACE / USE preamble would otherwise fail with an opaque
    // nom debug dump, and trailing statements would be silently dropped.
    let schema_src = std::fs::read_to_string(&args.schema).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read schema file: {}: {}",
            args.schema.display(),
            e
        )
    })?;
    ensure_bare_create_table_schema(&args.schema, &schema_src)?;

    let schema = crate::commands::load_schema_file(&args.schema, false, None)?;

    // -----------------------------------------------------------------------
    // Build delta-parquet options (includes schema derivation via DS7).
    // Schema errors (counter table, column collision) surface here.
    // -----------------------------------------------------------------------
    let schema_opts = DeltaSchemaOpts::with_prefix(&args.envelope_prefix);
    let parquet_compression = match args.compression {
        DeltaCompressionCodec::Snappy => DeltaParquetCompression::Snappy,
        DeltaCompressionCodec::Zstd => DeltaParquetCompression::Zstd,
        DeltaCompressionCodec::Uncompressed => DeltaParquetCompression::Uncompressed,
    };

    // Derive source string: caller-supplied or the sstable_dir base name.
    let source = args.source.clone().unwrap_or_else(|| {
        args.sstable_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| args.sstable_dir.to_string_lossy().into_owned())
    });

    // Build options — this calls derive_delta_schema internally, so counter
    // tables and column collisions error here, before the output file is created.
    // We do a dry-run schema derivation first for early error detection with
    // richer CLI error messages.  DeltaParquetWriter::new will derive it a
    // second time internally; the double derivation is intentional: the first
    // call gives us nicer errors before any file is created, and the second
    // call is the writer's own internal schema build.
    {
        let schema_opts_check = DeltaSchemaOpts::with_prefix(&args.envelope_prefix);
        cqlite_core::export::delta_schema::derive_delta_schema(&schema, &schema_opts_check)
            .map_err(|e| {
                // Map DeltaSchemaError to anyhow with richer CLI context.
                let msg = match &e {
                    cqlite_core::export::DeltaSchemaError::ColumnCollision {
                        column,
                        reserved,
                    } => format!(
                        "Column '{column}' collides with envelope reserved name '{reserved}'.\n\
                         Use --envelope-prefix to choose a different prefix \
                         (e.g. --envelope-prefix \"_cqlite_\" gives \"_cqlite_op\", etc.).\n\
                         Original error: {e}"
                    ),
                    cqlite_core::export::DeltaSchemaError::CounterTable {
                        keyspace,
                        table,
                        columns,
                    } => format!(
                        "Counter tables cannot be exported as delta Parquet.\n\
                         Table '{keyspace}.{table}' contains counter column(s): {columns}.\n\
                         Counter semantics are incompatible with the per-cell writetime delta model."
                    ),
                    _ => format!("Schema error: {e}"),
                };
                anyhow::anyhow!("{}", msg)
            })?;
    }

    // -----------------------------------------------------------------------
    // Atomic write via sibling temp file (Finding 1).
    //
    // We write to `<output>.tmp` in the same directory so that
    // `std::fs::rename` stays on the same filesystem and is atomic.
    // On any error we remove the temp file and leave the original untouched,
    // closing both the TOCTOU window between the exists() check above and
    // the actual file creation, and the data-loss scenario where a mid-stream
    // failure with --overwrite destroys the original and leaves no replacement.
    // -----------------------------------------------------------------------

    // Create parent directories if needed.
    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create output directory '{}': {}",
                    parent.display(),
                    e
                )
            })?;
        }
    }

    // Build the sibling temp path.
    let tmp_path = {
        let mut p = args.output.clone();
        let mut name = p
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("delta_export"))
            .to_os_string();
        name.push(".tmp");
        p.set_file_name(name);
        p
    };

    // Helper: best-effort temp-file cleanup used on every error path below.
    let remove_tmp = || {
        let _ = std::fs::remove_file(&tmp_path);
    };

    let tmp_file = std::fs::File::create(&tmp_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create temporary output file '{}': {}",
            tmp_path.display(),
            e
        )
    })?;

    // Use a BufWriter to batch I/O; the Parquet writer already has its own
    // internal buffering but BufWriter reduces syscall overhead on large files.
    let buf_writer = BufWriter::new(tmp_file);

    // -----------------------------------------------------------------------
    // Create DeltaParquetWriter
    // -----------------------------------------------------------------------
    let opts = DeltaParquetOptions {
        row_group_size: args.row_group_size,
        compression: parquet_compression,
        schema_opts: schema_opts.clone(),
        source: source.clone(),
    };

    let mut writer = DeltaParquetWriter::new(buf_writer, &schema, opts).map_err(|e| {
        remove_tmp();
        anyhow::anyhow!("Failed to initialise delta Parquet writer: {}", e)
    })?;

    // -----------------------------------------------------------------------
    // Stream records from scan_delta.
    //
    // scan_delta returns a (Receiver, ScanSummaryHandle) tuple.  The receiver
    // streams DeltaRecords; the handle accumulates scan-level statistics
    // (element tombstone count) and is read after the stream is drained.
    // -----------------------------------------------------------------------
    let (mut rx, summary) = scan_delta(
        args.sstable_dir.clone(),
        schema.clone(),
        /* buffer_size */ 64,
    );

    while let Some(result) = rx.recv().await {
        match result {
            Ok(record) => {
                writer.write_record(record).map_err(|e| {
                    remove_tmp();
                    anyhow::anyhow!("Error writing delta record to Parquet: {}", e)
                })?;
            }
            Err(e) => {
                remove_tmp();
                return Err(anyhow::anyhow!(
                    "Error scanning SSTable '{}': {}",
                    args.sstable_dir.display(),
                    e
                ));
            }
        }
    }

    // Stream is fully drained — read the final scan summary.
    // element_tombstone_warnings is plumbed from the ScanSummaryHandle, not
    // hardcoded, so the DS4 warning path is now reachable (issue #493).
    let element_tombstone_warnings = summary.read().element_tombstones_detected;

    let records_written = writer.records_written();

    // -----------------------------------------------------------------------
    // Finalize (writes Parquet footer + metadata)
    // -----------------------------------------------------------------------
    writer.finalize().map_err(|e| {
        remove_tmp();
        anyhow::anyhow!("Failed to finalise delta Parquet file: {}", e)
    })?;

    // -----------------------------------------------------------------------
    // Atomic rename: temp file → final output path.
    //
    // Executed only after finalize() succeeds.  On rename failure the temp
    // file is removed and the original (if any) is still intact.
    // -----------------------------------------------------------------------
    std::fs::rename(&tmp_path, &args.output).map_err(|e| {
        remove_tmp();
        anyhow::anyhow!(
            "Failed to move temp file '{}' to '{}': {}",
            tmp_path.display(),
            args.output.display(),
            e
        )
    })?;

    Ok(DeltaExportResult {
        output_path: args.output.clone(),
        records_written,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
        element_tombstone_warnings,
    })
}

#[cfg(all(test, feature = "delta-export"))]
mod bare_create_table_schema_tests {
    use super::ensure_bare_create_table_schema;
    use std::path::Path;

    const BARE: &str = "CREATE TABLE ks.t (id uuid PRIMARY KEY, name text);";
    const LEADING_PREAMBLE: &str = "CREATE KEYSPACE ks WITH replication = \
        {'class': 'SimpleStrategy', 'replication_factor': 1}; \
        USE ks; \
        CREATE TABLE ks.t (id uuid PRIMARY KEY, name text);";
    const TRAILING_STATEMENT: &str = "CREATE TABLE ks.t (id uuid PRIMARY KEY, name text); \
        CREATE INDEX ON ks.t (name);";
    const LONE_KEYSPACE: &str = "CREATE KEYSPACE ks WITH replication = \
        {'class': 'SimpleStrategy', 'replication_factor': 1};";
    const EMPTY_SCHEMA: &str = "   \n  -- just a comment\n  ";

    /// A single bare CREATE TABLE is accepted.
    #[test]
    fn bare_create_table_is_accepted() {
        ensure_bare_create_table_schema(Path::new("schema.cql"), BARE)
            .expect("a bare CREATE TABLE must be accepted");
    }

    /// A leading CREATE KEYSPACE / USE preamble must yield a clear, targeted
    /// error naming the bare-CREATE-TABLE contract — not a raw nom debug dump.
    /// On main this reached parse_cql_schema and produced a generic
    /// "Failed to parse CQL schema: {:?}" nom error.
    #[test]
    fn leading_preamble_names_the_bare_create_table_contract() {
        let err = ensure_bare_create_table_schema(Path::new("schema.cql"), LEADING_PREAMBLE)
            .expect_err("a leading CREATE KEYSPACE / USE preamble must be rejected");
        assert!(
            err.to_string().contains("bare CREATE TABLE"),
            "error must name the bare-CREATE-TABLE contract, got: {err}"
        );
    }

    /// Statements *after* the CREATE TABLE must error rather than being silently
    /// dropped. On main these were discarded (parse_cql_schema ignored the
    /// remaining input) and the export silently succeeded.
    #[test]
    fn trailing_statement_names_the_bare_create_table_contract() {
        let err = ensure_bare_create_table_schema(Path::new("schema.cql"), TRAILING_STATEMENT)
            .expect_err("a trailing statement must be rejected");
        assert!(
            err.to_string().contains("bare CREATE TABLE"),
            "error must name the bare-CREATE-TABLE contract, got: {err}"
        );
    }

    /// A single statement that is NOT a CREATE TABLE (e.g. a lone CREATE
    /// KEYSPACE) must yield the same targeted error. On pre-fix code this
    /// single statement passed the `len() > 1` check and fell through to the
    /// legacy parser, producing the opaque nom error the guard was meant to
    /// replace.
    #[test]
    fn lone_non_create_table_names_the_bare_create_table_contract() {
        let err = ensure_bare_create_table_schema(Path::new("schema.cql"), LONE_KEYSPACE)
            .expect_err("a lone CREATE KEYSPACE must be rejected");
        assert!(
            err.to_string().contains("bare CREATE TABLE"),
            "error must name the bare-CREATE-TABLE contract, got: {err}"
        );
    }

    /// A zero-statement (empty / comment-only) schema must yield the same
    /// targeted error rather than falling through to the legacy parser.
    #[test]
    fn empty_schema_names_the_bare_create_table_contract() {
        let err = ensure_bare_create_table_schema(Path::new("schema.cql"), EMPTY_SCHEMA)
            .expect_err("an empty/zero-statement schema must be rejected");
        assert!(
            err.to_string().contains("bare CREATE TABLE"),
            "error must name the bare-CREATE-TABLE contract, got: {err}"
        );
    }

    /// JSON schema documents are exempt from the single-statement contract
    /// (they are validated by the JSON loader, not this guard).
    #[test]
    fn json_schema_is_exempt() {
        // Content that would trip the CQL guard, but with a .json extension.
        ensure_bare_create_table_schema(Path::new("schema.json"), LEADING_PREAMBLE)
            .expect("JSON schema files are exempt from the CQL single-statement contract");
    }
}
