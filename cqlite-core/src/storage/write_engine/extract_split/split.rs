//! `split_sstable` (design D3) — sequential boundary walk over ONE input
//! generation, rolling to the next of N (or byte-bounded) output
//! `SSTableWriter`s. No `KWayMerger`, no reconciliation, no purge: a single
//! healthy SSTable generation has no duplicate partition keys (Cassandra's
//! own on-disk invariant), so `split` is a straight sequential
//! decode-and-write, reusing [`super::raw_copy`]'s decode-at-offset →
//! single-run-conversion helper.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::writer::SSTableWriter;
use crate::storage::write_engine::salvage::boundaries::enumerate_boundaries;

use super::raw_copy::{decode_and_convert, open_reader};
use super::{base_and_format, discover_generations, generation_of, partition_decode_failed, Refused, RefusalReason};

/// Where a `split` run divides its ONE input generation (design D3).
#[derive(Debug, Clone, Copy)]
pub enum SplitBoundary {
    /// Divide into exactly `N` parts, partition-count balanced (design
    /// D3.1): `T / N` partitions per part, with the LAST part absorbing the
    /// `T % N` remainder — never a starved trailing part.
    Parts(u32),
    /// Roll to the next part once the CURRENT part's accumulated Data.db
    /// byte span reaches or exceeds `B`, at the next partition boundary at
    /// or past `B` (never mid-partition) — a part can therefore exceed `B`
    /// by up to one partition's width.
    MaxBytes(u64),
}

impl SplitBoundary {
    pub(crate) fn summarize(&self) -> SplitBoundarySummary {
        match self {
            SplitBoundary::Parts(n) => SplitBoundarySummary {
                kind: "parts".to_string(),
                value: *n as u64,
            },
            SplitBoundary::MaxBytes(b) => SplitBoundarySummary {
                kind: "max-bytes".to_string(),
                value: *b,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitBoundarySummary {
    pub kind: String,
    pub value: u64,
}

/// One output part (design D4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitPart {
    pub generation: u64,
    pub min_token: i64,
    pub max_token: i64,
    pub partitions: usize,
    pub rows: usize,
    pub bytes: u64,
    pub verify: String,
}

/// `split_sstable`'s JSON manifest contract (design D4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitReport {
    pub input: String,
    pub output: String,
    pub boundary: SplitBoundarySummary,
    pub parts: Vec<SplitPart>,
    pub source_partitions: usize,
    pub source_rows: usize,
    pub refused: Option<Refused>,
    pub now: String,
    pub cqlite_version: String,
}

/// Divide `input` (a bare `Data.db` path, or a table dir resolving to
/// EXACTLY one generation) into `boundary`-bounded parts under `out_dir`
/// (design D3).
pub async fn split_sstable(
    input: &Path,
    boundary: SplitBoundary,
    out_dir: &Path,
    schema: &TableSchema,
) -> Result<SplitReport> {
    let now = chrono::Utc::now().to_rfc3339();
    let cqlite_version = env!("CARGO_PKG_VERSION").to_string();
    let mut report = SplitReport {
        input: input.display().to_string(),
        output: out_dir.display().to_string(),
        boundary: boundary.summarize(),
        parts: Vec::new(),
        source_partitions: 0,
        source_rows: 0,
        refused: None,
        now,
        cqlite_version,
    };

    let generation_paths = discover_generations(input)?;
    let data_db = match generation_paths.as_slice() {
        [one] => one.clone(),
        many => {
            let names: Vec<String> = many.iter().map(|p| p.display().to_string()).collect();
            report.refused = Some(Refused {
                reason: RefusalReason::MultipleGenerations,
                generation: None,
                data_offset: None,
                detail: format!(
                    "{} resolves to {} generations ({}); split operates on exactly one — pass an \
                     explicit Data.db path",
                    input.display(),
                    many.len(),
                    names.join(", ")
                ),
                remedy: "pass the explicit *-Data.db path of the ONE generation to split"
                    .to_string(),
            });
            return Ok(report);
        }
    };

    let (base, is_bti) = base_and_format(&data_db)?;
    let generation = generation_of(&data_db)?;
    let dir = data_db.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "input path has no parent directory: {}",
            data_db.display()
        ))
    })?;
    let boundaries = match enumerate_boundaries(dir, &base, is_bti) {
        Ok(b) => b,
        Err(refusal) => {
            report.refused = Some(super::boundary_source_unreadable(refusal.remedy));
            return Ok(report);
        }
    };
    let total_partitions = boundaries.entries.len();
    report.source_partitions = total_partitions;
    if total_partitions == 0 {
        return Ok(report);
    }

    let total_data_length = total_data_length(&data_db).await?;

    // Per-entry part assignment, computed UP FRONT (design D3.1) so the
    // walk below is a straight sequential pass with no lookahead.
    let part_of_entry: Vec<usize> = match boundary {
        SplitBoundary::Parts(n) => parts_assignment(total_partitions, n as usize)?,
        SplitBoundary::MaxBytes(b) => max_bytes_assignment(&boundaries, total_data_length, b),
    };

    let reader = open_reader(&data_db).await?;
    let scan_cancel = ScanCancel::default();

    let mut current_part_index: Option<usize> = None;
    let mut writer: Option<SSTableWriter> = None;
    let mut partitions = 0usize;
    let mut rows = 0usize;
    let mut min_token = i64::MAX;
    let mut max_token = i64::MIN;
    let mut part_start_offset: u64 = 0;

    for (idx, entry) in boundaries.entries.iter().enumerate() {
        let this_part = part_of_entry[idx];
        if current_part_index != Some(this_part) {
            if let Some(w) = writer.take() {
                let bytes = entry.data_offset.saturating_sub(part_start_offset);
                finalize_part(&mut report, w, current_part_index.unwrap(), generation, partitions, rows, min_token, max_token, bytes).await?;
                if report.refused.is_some() {
                    return Ok(report);
                }
            }
            current_part_index = Some(this_part);
            writer = Some(SSTableWriter::new(
                out_dir.join(format!("part-{this_part:04}")),
                generation,
                schema,
            )?);
            partitions = 0;
            rows = 0;
            min_token = i64::MAX;
            max_token = i64::MIN;
            part_start_offset = entry.data_offset;
        }

        let end_bound = boundaries.entries.get(idx + 1).map(|e| e.data_offset);
        match decode_and_convert(&reader, entry, end_bound, schema, &scan_cancel).await {
            Ok(Some((key, mutations))) => {
                min_token = min_token.min(key.token);
                max_token = max_token.max(key.token);
                rows += mutations.len();
                let w = writer.as_mut().expect("writer opened for this part");
                if let Err(e) = w.write_partition(key, mutations) {
                    report.refused = Some(partition_decode_failed(generation, entry.data_offset, e));
                    return Ok(report);
                }
                partitions += 1;
            }
            Ok(None) => {}
            Err(e) => {
                report.refused = Some(partition_decode_failed(generation, entry.data_offset, e));
                return Ok(report);
            }
        }
    }

    if let Some(w) = writer.take() {
        let bytes = total_data_length.saturating_sub(part_start_offset);
        finalize_part(
            &mut report,
            w,
            current_part_index.unwrap_or(0),
            generation,
            partitions,
            rows,
            min_token,
            max_token,
            bytes,
        )
        .await?;
    }

    report.source_rows = report.parts.iter().map(|p| p.rows).sum();
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn finalize_part(
    report: &mut SplitReport,
    writer: SSTableWriter,
    part_index: usize,
    generation: u64,
    partitions: usize,
    rows: usize,
    min_token: i64,
    max_token: i64,
    bytes: u64,
) -> Result<()> {
    if partitions == 0 {
        return Ok(());
    }
    let info = writer.finish().await?;
    let verify_report = super::verify_output_generation(&info.data_path).await?;
    if !verify_report.is_ok() {
        report.refused = Some(Refused {
            reason: RefusalReason::PartVerifyFailed,
            generation: Some(generation),
            data_offset: None,
            detail: format!("part {part_index}: {}", verify_report.summary_line()),
            remedy: "a produced split part failed its own verify --mode full self-audit; this \
                     is a split-tool defect, not an input problem — please file an issue"
                .to_string(),
        });
        return Ok(());
    }
    report.parts.push(SplitPart {
        generation,
        min_token,
        max_token,
        partitions,
        rows,
        bytes,
        verify: "pass".to_string(),
    });
    Ok(())
}

/// `--parts N` assignment (design D3.1): `T / N` per part, LAST part
/// absorbing the `T % N` remainder. Errors when `N` exceeds `T` (more parts
/// requested than partitions available) rather than silently emitting empty
/// parts.
fn parts_assignment(total: usize, n: usize) -> Result<Vec<usize>> {
    if n == 0 {
        return Err(Error::InvalidInput("--parts must be at least 1".to_string()));
    }
    if n > total {
        return Err(Error::InvalidInput(format!(
            "--parts {n} exceeds the source's {total} partition(s); split cannot produce more \
             parts than partitions"
        )));
    }
    let base = total / n;
    let remainder = total % n;
    let mut sizes = vec![base; n];
    sizes[n - 1] += remainder;
    let mut assignment = Vec::with_capacity(total);
    for (part_index, size) in sizes.into_iter().enumerate() {
        assignment.extend(std::iter::repeat(part_index).take(size));
    }
    Ok(assignment)
}

/// `--max-bytes B` assignment (design D3.1): roll to the next part once the
/// CURRENT part's accumulated byte span (measured from each partition's own
/// `data_offset` extent in the boundary source) reaches or exceeds `B`, at
/// the next partition boundary at or past `B`.
fn max_bytes_assignment(
    boundaries: &crate::storage::write_engine::salvage::boundaries::Boundaries,
    total_data_length: u64,
    max_bytes: u64,
) -> Vec<usize> {
    let mut assignment = Vec::with_capacity(boundaries.entries.len());
    let mut part_index = 0usize;
    let mut part_start = boundaries.entries.first().map(|e| e.data_offset).unwrap_or(0);
    for idx in 0..boundaries.entries.len() {
        assignment.push(part_index);
        let next_offset = boundaries
            .entries
            .get(idx + 1)
            .map(|e| e.data_offset)
            .unwrap_or(total_data_length);
        if next_offset.saturating_sub(part_start) >= max_bytes && idx + 1 < boundaries.entries.len() {
            part_index += 1;
            part_start = next_offset;
        }
    }
    assignment
}

/// The input's total decompressed Data.db length (uncompressed: file length
/// minus header; compressed: `CompressionInfo.data_length`) — the same
/// resolution `decode_partition_at_offset_for_salvage`'s own `end_bound`
/// fallback for the last partition performs.
async fn total_data_length(data_db: &Path) -> Result<u64> {
    let reader = open_reader(data_db).await?;
    if let Some(ci) = reader.compression_info.as_deref() {
        return Ok(ci.data_length);
    }
    let file_len = std::fs::metadata(data_db).map_err(Error::Io)?.len();
    let header_size = reader.calculate_header_size() as u64;
    Ok(file_len.saturating_sub(header_size))
}
