//! `cqlite export --format vortex` streaming loop (issue #4237).
//!
//! Extracted out of the already-over-threshold `commands/export.rs` (#1116 campsite rule) so
//! adding the Vortex arm did not grow that file's already-260-line per-format `match` further.
//! Reuses `export::collect_chunk_within` — the SAME deadline-aware chunk collection the
//! CSV/JSON/Parquet loops use — rather than a fifth duplicate of its two-layer deadline check.

use crate::commands::export::collect_chunk_within;
use anyhow::Result;
use cqlite_core::export::vortex::{StreamingVortexWriter, VortexExportOptions};
use cqlite_core::query::result::QueryResultIterator;
use indicatif::ProgressBar;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

/// Stream a query result to a `.vortex` file, mirroring the shape of the Parquet arm in
/// `export.rs`: create the writer, collect+truncate+push chunks in a loop honoring the export
/// deadline and `--limit`, update progress, then finalize.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_vortex_export(
    file: &Path,
    result_iter: &mut QueryResultIterator,
    chunk_size: usize,
    export_deadline: Option<TokioInstant>,
    budget_start: Instant,
    export_budget: Duration,
    rows_remaining: &mut Option<usize>,
    rows_exported: &mut u64,
    pb: &ProgressBar,
) -> Result<()> {
    let mut writer = StreamingVortexWriter::create(
        file,
        &result_iter.metadata,
        &VortexExportOptions {
            row_group_size: chunk_size,
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to initialize Vortex writer: {}", e))?;

    loop {
        if *rows_remaining == Some(0) {
            break;
        }

        let chunk = collect_chunk_within(
            result_iter,
            chunk_size,
            export_deadline,
            budget_start,
            export_budget,
        )
        .await?;

        if chunk.is_empty() {
            break;
        }

        let chunk_to_write = if let Some(remaining) = *rows_remaining {
            if chunk.len() > remaining {
                chunk.into_iter().take(remaining).collect::<Vec<_>>()
            } else {
                chunk
            }
        } else {
            chunk
        };

        let written = chunk_to_write.len();
        writer
            .write_chunk(&chunk_to_write)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write Vortex chunk: {}", e))?;

        *rows_exported += written as u64;
        pb.set_position(*rows_exported);

        if let Some(ref mut remaining) = *rows_remaining {
            *remaining = remaining.saturating_sub(written);
        }
    }

    writer
        .finalize()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to finalize Vortex: {}", e))?;

    Ok(())
}
