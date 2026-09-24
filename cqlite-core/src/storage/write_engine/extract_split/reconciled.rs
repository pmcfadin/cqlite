//! Reconciled `extract` (design D1) — resolve `Selection` to a key list,
//! reconcile across every input generation via
//! `build_single_partition_merger` (no purge, the SAME point-read machinery
//! `query`'s point-read path uses), and write ONE output generation.
//!
//! Unlike `KWayMerger::merge`'s convenience form (which drives straight to a
//! writer and returns only aggregate `MergeStats`), this drives the merger
//! manually via `step()` so each `MergeStep::Partition{key,..}` is
//! individually observable — required for spec R3's `not_found` accounting
//! (a key that never produces a `Partition` step was never present in any
//! input generation).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::writer::SSTableWriter;
use crate::storage::write_engine::merge::{build_single_partition_merger, KWayMerger, MergeStep};

use super::{partition_decode_failed, ExtractReport, GenerationWritten, RefusalReason, Selection};

/// The fixed generation number reconciled `extract` writes its single output
/// generation as. `--out` is always a fresh, empty directory (the write-guard
/// convention), so there is no existing generation to collide with, and no
/// natural "the" input generation to inherit from when the source spans
/// several — `1` is unambiguous and matches `compact`'s single-output-file
/// convention for a similarly synthesized output.
const OUTPUT_GENERATION: u64 = 1;

pub(super) async fn extract_reconciled(
    generation_paths: &[PathBuf],
    selection: Selection,
    schema: &TableSchema,
    out_dir: &Path,
    mut report: ExtractReport,
) -> Result<ExtractReport> {
    let requested = selection.resolve(generation_paths)?;
    if requested.is_empty() {
        return Ok(report);
    }

    let scan_cancel = ScanCancel::default();
    let merger = match build_single_partition_merger(
        generation_paths.to_vec(),
        &requested,
        schema,
        scan_cancel,
    ) {
        Ok(m) => m,
        Err(e) => {
            report.refused = Some(super::Refused {
                reason: RefusalReason::PartitionDecodeFailed,
                generation: None,
                data_offset: None,
                detail: e.to_string(),
                remedy: "cqlite salvage (issue #4196) or cqlite rebuild (issue #4197)".to_string(),
            });
            return Ok(report);
        }
    };

    let Some(mut merger) = merger else {
        report.not_found = requested.iter().map(hex::encode).collect();
        return Ok(report);
    };

    let mut writer = SSTableWriter::new(out_dir.to_path_buf(), OUTPUT_GENERATION, schema)?;
    // Two-pass compaction convention (issue #729, mirrored from `compact_sstables`/
    // `salvage_sstable`): seed the output's timestamp/LDT/TTL delta-encoding
    // baselines from the INPUT generations' own Statistics.db before writing
    // any partition. Without this the writer's baseline defaults to
    // whatever it first observes, and any LATER-written row with an
    // actually-smaller timestamp underflows the delta encoding, corrupting
    // (not merely losing) the stored value — caught by
    // `issue_4199_split_parts.rs`'s golden-vs-output row_timestamp mismatch.
    let (min_ts, min_ldt, min_ttl) = crate::storage::write_engine::merge::compute_baseline_min(
        &generation_paths.to_vec(),
    );
    writer.pre_seed_encoding_baselines(min_ts, min_ldt, min_ttl);
    let mut found: HashSet<Vec<u8>> = HashSet::new();
    let mut partitions = 0usize;
    let mut rows = 0usize;

    loop {
        match merger.step() {
            Ok(MergeStep::Complete) => break,
            Ok(MergeStep::Partition { key, rows: entries }) => {
                found.insert(key.key.clone());
                let mut mutations = Vec::with_capacity(entries.len());
                for entry in entries {
                    match KWayMerger::merge_entry_to_mutation(entry, schema) {
                        Ok(m) => mutations.push(m),
                        Err(e) => {
                            report.refused = Some(partition_decode_failed(0, 0, e));
                            return Ok(report);
                        }
                    }
                }
                rows += mutations.len();
                if let Err(e) = writer.write_partition(key, mutations) {
                    report.refused = Some(partition_decode_failed(0, 0, e));
                    return Ok(report);
                }
                partitions += 1;
            }
            Err(e) => {
                report.refused = Some(partition_decode_failed(0, 0, e));
                return Ok(report);
            }
        }
    }

    report.not_found = requested
        .iter()
        .filter(|k| !found.contains(*k))
        .map(hex::encode)
        .collect();

    if partitions > 0 {
        let info = writer.finish().await?;
        let verify_report = super::verify_output_generation(&info.data_path).await?;
        if !verify_report.is_ok() {
            report.refused = Some(super::Refused {
                reason: RefusalReason::PartitionDecodeFailed,
                generation: Some(OUTPUT_GENERATION),
                data_offset: None,
                detail: verify_report.summary_line(),
                remedy: "the reconciled output failed its own verify --mode full self-audit; \
                         this is an extract-tool defect, not an input problem — please file an \
                         issue"
                    .to_string(),
            });
            return Ok(report);
        }
        report.generations_written.push(GenerationWritten {
            generation: OUTPUT_GENERATION,
            partitions,
            rows,
        });
    }

    Ok(report)
}
