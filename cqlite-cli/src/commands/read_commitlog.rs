//! `read-commitlog` command — decode a Cassandra 5.0 CommitLog segment
//! (issue #2389).
//!
//! Opens a raw `CommitLog-<version>-<id>.log` segment via
//! [`cqlite_core::storage::commitlog::CommitLogReader`] and reports the parsed
//! descriptor plus the decoded mutation stream (table id, partition key, column
//! names per partition update). This is the named CLI entry point that satisfies
//! the public-surface wiring-evidence requirement: CLI → `CommitLogReader` →
//! decoded mutations.
//!
//! Cell/clustering *values* need per-table schemas (see
//! `CommitLogReader::open_with_schemas`); without them the command still reports
//! every mutation's structural fields, mirroring how the library decodes a
//! segment with no schema supplied. A compressed/encrypted or unsupported-version
//! segment fails closed with the library's typed error, surfaced here as a
//! non-zero exit.

use anyhow::{Context, Result};
use std::io::IsTerminal;
use std::path::Path;

use cqlite_core::storage::commitlog::CommitLogReader;

use crate::cli::OutputFormat;

/// Execute the `read-commitlog` command.
pub async fn execute_read_commitlog_command(
    file_path: &Path,
    format: OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    // Decorative status to stderr so a redirected stdout stays machine-readable
    // (matches the read-sstable stdout/stderr contract).
    let show_status = !quiet && std::io::stderr().is_terminal();
    if show_status {
        eprintln!("Reading CommitLog segment: {}", file_path.display());
    }

    if !file_path.exists() {
        return Err(anyhow::anyhow!(
            "CommitLog segment not found: {}",
            file_path.display()
        ));
    }

    let reader = CommitLogReader::open(file_path).with_context(|| {
        format!(
            "Failed to open CommitLog segment {}",
            file_path.display()
        )
    })?;

    match format {
        OutputFormat::Json => render_json(&reader, limit),
        _ => render_text(&reader, limit),
    }
}

/// A flattened, display-oriented view of one decoded partition update.
struct UpdateView {
    table_id: String,
    partition_key_hex: String,
    columns: Vec<String>,
    rows_decoded: bool,
    row_count: usize,
    has_partition_deletion: bool,
}

fn collect(reader: &CommitLogReader, limit: Option<usize>) -> (Vec<UpdateView>, usize, bool, bool) {
    let mut views = Vec::new();
    let mut mutation_count = 0usize;
    let mut errored = false;
    let mut it = reader.mutations();
    for res in it.by_ref() {
        match res {
            Ok(mutation) => {
                mutation_count += 1;
                for upd in &mutation.updates {
                    views.push(UpdateView {
                        table_id: upd.table_id_uuid(),
                        partition_key_hex: hex(&upd.partition_key),
                        columns: upd.column_names.clone(),
                        rows_decoded: upd.rows_decoded,
                        row_count: upd.rows.len(),
                        has_partition_deletion: upd.has_partition_deletion,
                    });
                }
            }
            Err(_) => {
                // A genuinely corrupt record ends the stream with one Err; report
                // it as a decode error rather than pretending the tail is clean.
                errored = true;
                break;
            }
        }
        if let Some(n) = limit {
            if mutation_count >= n {
                break;
            }
        }
    }
    (views, mutation_count, it.truncated(), errored)
}

fn render_text(reader: &CommitLogReader, limit: Option<usize>) -> Result<()> {
    let desc = reader.descriptor();
    println!("CommitLog segment");
    println!("  version:     {}", desc.version);
    println!("  id:          {}", desc.id);
    println!(
        "  compression: {}",
        desc.compression_class.as_deref().unwrap_or("none")
    );

    let (views, mutation_count, truncated, errored) = collect(reader, limit);
    println!("  mutations:   {mutation_count}");
    println!("  truncated:   {truncated}");
    if errored {
        println!("  note:        stream ended on a corrupt record (typed decode error)");
    }
    println!();

    for (i, v) in views.iter().enumerate() {
        println!("[{i}] table={} pk=0x{}", v.table_id, v.partition_key_hex);
        if v.has_partition_deletion {
            println!("    partition-deletion");
        }
        if !v.columns.is_empty() {
            println!("    columns: {}", v.columns.join(", "));
        }
        println!(
            "    rows: {} (fully-decoded: {})",
            v.row_count, v.rows_decoded
        );
    }
    Ok(())
}

fn render_json(reader: &CommitLogReader, limit: Option<usize>) -> Result<()> {
    let desc = reader.descriptor();
    let (views, mutation_count, truncated, errored) = collect(reader, limit);
    let updates: Vec<serde_json::Value> = views
        .iter()
        .map(|v| {
            serde_json::json!({
                "table_id": v.table_id,
                "partition_key_hex": v.partition_key_hex,
                "columns": v.columns,
                "rows_decoded": v.rows_decoded,
                "row_count": v.row_count,
                "has_partition_deletion": v.has_partition_deletion,
            })
        })
        .collect();
    let out = serde_json::json!({
        "descriptor": {
            "version": desc.version,
            "id": desc.id,
            "compression": desc.compression_class,
        },
        "mutation_count": mutation_count,
        "truncated": truncated,
        "decode_error": errored,
        "updates": updates,
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
