//! `extract --raw` (design D2) — per-generation boundary walk, filtered to
//! the resolved selection, decoded via the SAME decode-at-offset primitive
//! `salvage` drives (`decode_partition_at_offset_for_salvage`), with NO
//! cross-generation reconciliation: one output generation per input
//! generation that held at least one match, each holding that generation's
//! own bytes verbatim (tombstones included).

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{PartitionAtOffsetOutcome, SSTableReader};
use crate::storage::sstable::writer::SSTableWriter;
use crate::storage::write_engine::merge::{
    KWayMerger, MergeEntry, MergeStep, SSTableRowIterator, SSTableRowIteratorAdapter,
};
use crate::storage::write_engine::salvage::boundaries::{enumerate_boundaries, BoundaryEntry};

use super::{
    base_and_format, generation_of, partition_decode_failed, ExtractReport, GenerationWritten,
    Selection,
};

/// A run backed by one already-decoded partition's [`MergeEntry`]s — used
/// ONLY to convert a single generation's decoded rows into a
/// `write_partition`-ready `(DecoratedKey, Vec<Mutation>)` pair via the SAME
/// single-run reconciliation `salvage`'s `recover_one_partition` uses. With
/// exactly one run there is nothing to reconcile ACROSS — this is purely the
/// `MergeEntry` → `Mutation` conversion path, reused rather than duplicated.
struct SinglePartitionRun {
    entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for SinglePartitionRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

pub(super) async fn open_reader(input: &Path) -> Result<SSTableReader> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await?);
    SSTableReader::open(input, &config, platform).await
}

/// Decode ONE boundary slot and convert its rows to `(DecoratedKey,
/// Vec<Mutation>)`, or `Ok(None)` when nothing survives to write (an
/// entirely-purged reconciliation — never reached with `purge_safe`/gc off,
/// which single-run conversion always is). `Err` names the failure for the
/// caller to fold into a whole-run refusal (design D2/D5).
#[allow(clippy::too_many_arguments)]
pub(super) async fn decode_and_convert(
    reader: &SSTableReader,
    entry: &BoundaryEntry,
    end_bound: Option<u64>,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
) -> std::result::Result<
    Option<(
        crate::storage::write_engine::mutation::DecoratedKey,
        Vec<crate::storage::write_engine::mutation::Mutation>,
    )>,
    String,
> {
    let outcome = reader
        .decode_partition_at_offset_for_salvage(
            entry.data_offset,
            end_bound,
            entry.expected_key.as_deref(),
            Some(schema),
            scan_cancel,
        )
        .await
        .map_err(|e| e.to_string())?;

    let rows = match outcome {
        PartitionAtOffsetOutcome::Rows(rows) => rows,
        PartitionAtOffsetOutcome::KeyMismatch => {
            return Err(
                "decoded key at this offset does not match the boundary source's key for this \
                 slot"
                    .to_string(),
            );
        }
        PartitionAtOffsetOutcome::DecodeError { error } => return Err(error.to_string()),
        PartitionAtOffsetOutcome::Truncated => {
            return Err(
                "partition's authoritative byte range extends past Data.db's actual end"
                    .to_string(),
            );
        }
        PartitionAtOffsetOutcome::SpanTooWide { span_bytes } => {
            return Err(format!(
                "partition's authoritative byte range is {span_bytes} bytes wide, exceeding the \
                 plausible-partition-span ceiling"
            ));
        }
    };

    let mut merge_entries = Vec::with_capacity(rows.len());
    for row in rows {
        merge_entries.push(
            SSTableRowIteratorAdapter::build_merge_entry(0, row, schema)
                .map_err(|e| e.to_string())?,
        );
    }
    let run = SinglePartitionRun {
        entries: merge_entries.into(),
    };
    let mut merger =
        KWayMerger::from_row_iterators(vec![Box::new(run)], schema).map_err(|e| e.to_string())?;
    let reconciled = merger.step().map_err(|e| e.to_string())?;
    let (key, entries) = match reconciled {
        MergeStep::Partition { key, rows } => (key, rows),
        MergeStep::Complete => return Ok(None),
    };
    let mut mutations = Vec::with_capacity(entries.len());
    for e in entries {
        mutations.push(KWayMerger::merge_entry_to_mutation(e, schema).map_err(|e| e.to_string())?);
    }
    Ok(Some((key, mutations)))
}

pub(super) async fn extract_raw(
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
    let requested_set: HashSet<Vec<u8>> = requested.iter().cloned().collect();
    let mut found: HashSet<Vec<u8>> = HashSet::new();
    let scan_cancel = ScanCancel::default();

    for path in generation_paths {
        let (base, is_bti) = base_and_format(path)?;
        let generation = generation_of(path)?;
        let dir = path.parent().ok_or_else(|| {
            crate::error::Error::InvalidInput(format!(
                "input path has no parent directory: {}",
                path.display()
            ))
        })?;
        let boundaries = match enumerate_boundaries(dir, &base, is_bti) {
            Ok(b) => b,
            Err(refusal) => {
                report.refused = Some(super::boundary_source_unreadable(format!(
                    "{}: {}",
                    path.display(),
                    refusal.remedy
                )));
                return Ok(report);
            }
        };

        // Candidate slots: those whose boundary-source key is already known
        // and requested, PLUS every slot lacking an independently-known key
        // (BTI narrow leaves) — the latter must be decoded to learn their
        // key before membership can be decided at all (documented in
        // `selection::resolve_token_range_to_keys`'s doc for the analogous
        // token-range case).
        let has_candidates = boundaries.entries.iter().any(|e| match &e.expected_key {
            Some(k) => requested_set.contains(k),
            None => true,
        });
        if !has_candidates {
            continue;
        }

        let reader = open_reader(path).await?;
        let mut writer: Option<SSTableWriter> = None;
        let mut partitions = 0usize;
        let mut rows = 0usize;

        for (idx, entry) in boundaries.entries.iter().enumerate() {
            let is_candidate = match &entry.expected_key {
                Some(k) => requested_set.contains(k),
                None => true,
            };
            if !is_candidate {
                continue;
            }
            let end_bound = boundaries.entries.get(idx + 1).map(|e| e.data_offset);

            match decode_and_convert(&reader, entry, end_bound, schema, &scan_cancel).await {
                Ok(Some((key, mutations))) => {
                    if !requested_set.contains(&key.key) {
                        continue;
                    }
                    found.insert(key.key.clone());
                    if writer.is_none() {
                        let mut w = SSTableWriter::new(out_dir.to_path_buf(), generation, schema)?;
                        // Seed the delta-encoding baselines from THIS generation's
                        // own Statistics.db (issue #729 convention; see
                        // `reconciled.rs`'s identical fix for why omitting this
                        // corrupts, not merely loses, a later row's timestamp).
                        let (min_ts, min_ldt, min_ttl) =
                            crate::storage::write_engine::merge::compute_baseline_min(
                                std::slice::from_ref(path),
                            );
                        w.pre_seed_encoding_baselines(min_ts, min_ldt, min_ttl);
                        writer = Some(w);
                    }
                    rows += mutations.len();
                    if let Err(e) = writer
                        .as_mut()
                        .expect("just set")
                        .write_partition(key, mutations)
                    {
                        report.refused =
                            Some(partition_decode_failed(generation, entry.data_offset, e));
                        return Ok(report);
                    }
                    partitions += 1;
                }
                Ok(None) => {
                    // Reconciled to nothing to write (fully purged); not a
                    // failure, but also not a match worth counting.
                }
                Err(e) => {
                    report.refused =
                        Some(partition_decode_failed(generation, entry.data_offset, e));
                    return Ok(report);
                }
            }
        }

        if let Some(writer) = writer {
            if partitions > 0 {
                let info = writer.finish().await?;
                let verify_report = super::verify_output_generation(&info.data_path).await?;
                if !verify_report.is_ok() {
                    report.refused = Some(super::Refused {
                        reason: super::RefusalReason::PartitionDecodeFailed,
                        generation: Some(generation),
                        data_offset: None,
                        detail: verify_report.summary_line(),
                        remedy: "the raw-copy output failed its own verify --mode full \
                                 self-audit; this is an extract-tool defect, not an input \
                                 problem — please file an issue"
                            .to_string(),
                    });
                    return Ok(report);
                }
                report.generations_written.push(GenerationWritten {
                    generation,
                    partitions,
                    rows,
                });
            }
        }
    }

    report.not_found = requested
        .iter()
        .filter(|k| !found.contains(*k))
        .map(hex::encode)
        .collect();
    Ok(report)
}
