//! `cqlite diagnose` — a read-only, files-only performance-report library for
//! one table's SSTable generations (issue #4204, epic #4192).
//!
//! Two cost tiers, matching `sstablemetadata`'s own cheap/`-s` split exactly
//! (`design.md` D1):
//!
//! - **Cheap (default)**: `Statistics.db`/`Index.db`/`Summary.db`/
//!   `CompressionInfo.db`/`TOC.txt` only, per generation. Data.db is NEVER
//!   opened (spec R5.1).
//! - **`--deep`**: additionally runs ONE bounded streaming scan per generation
//!   (spec R3), producing partition-size/clustering-width histograms, top-N
//!   rankings, and a dry reclaim-at-`now` prediction — see [`deep_scan`] /
//!   [`reclaim_prediction`].
//!
//! `diagnose` never validates consistency (spec R5) and never writes anything,
//! anywhere, with or without `--deep`.
//!
//! # No-heuristics / #4159 "never a bare zero" (spec R1)
//!
//! Every leaf value that can be genuinely unavailable is a [`SourcedField`]:
//! `Measured { value, source }` (even when `value` is itself a real,
//! authoritatively-decoded absence, e.g. "no pending repair") or
//! `Unmeasured { cause }` — never a bare `0`/`null` standing in for "we don't
//! know" (issue #1653's honest `Option` fields feed this directly).

pub mod deep_scan;
pub mod droppable_ratio;
pub mod reclaim_prediction;
pub mod token_overlap;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback_detailed;
use crate::parser::repair_metadata::{parse_repair_metadata, read_cell_per_partition_stats, RepairField};
use crate::parser::statistics::SSTableStatistics;
use crate::platform::Platform;
use crate::schema::TableSchema;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::sstable::version_gate::{SsTableDescriptor, VersionGates};
use crate::storage::write_engine::merge::is_fully_expired;
use crate::Config;

use droppable_ratio::{
    compute_gc_before_secs, estimated_droppable_tombstone_ratio as compute_droppable_ratio,
    DroppableRatio,
};
use reclaim_prediction::{compute_reclaim_prediction, ReclaimPrediction};
use token_overlap::{compute_token_overlap, TOKEN_OVERLAP_BUCKETS};

/// Options controlling a `diagnose_table` run.
#[derive(Debug, Clone)]
pub struct DiagnoseOptions {
    /// The read-time instant (seconds since epoch) used to derive `gcBefore` and
    /// to evaluate `fully_expired_at_now`/`reclaim_at_now`. Callers PIN this
    /// rather than sampling the wall clock inside the library, so a report is
    /// reproducible.
    pub now_secs: i64,
    /// `--deep`: run the additional bounded streaming scan per generation.
    pub deep: bool,
    /// `--top N`: cap on each `--deep` ranking list.
    pub top_n: usize,
    /// `gc_grace_seconds` from a loaded schema's table options, when one is
    /// available. `None` uses Cassandra's 10-day `TableParams` default (see
    /// [`droppable_ratio::compute_gc_before_secs`]) — the cheap tier never
    /// REQUIRES a schema (spec R6).
    pub gc_grace_seconds: Option<i64>,
    /// An optional schema for `--deep`'s row decode (spec R6: `--deep` renders
    /// raw key bytes when absent, never refusing).
    pub schema: Option<TableSchema>,
}

/// Where a reported value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSource {
    Statistics,
    Index,
    Scan,
}

impl FieldSource {
    pub fn as_str(self) -> &'static str {
        match self {
            FieldSource::Statistics => "statistics",
            FieldSource::Index => "index",
            FieldSource::Scan => "scan",
        }
    }
}

/// A reported value that is EITHER genuinely measured (even when the measured
/// value is itself an authoritative absence, e.g. "no pending repair") OR
/// honestly unmeasured with a stated cause — never a bare `0`/`null` conflating
/// the two (spec R1, the #4159 class).
#[derive(Debug, Clone, PartialEq)]
pub enum SourcedField<T> {
    Measured {
        value: Option<T>,
        source: FieldSource,
    },
    Unmeasured {
        cause: String,
    },
}

impl<T> SourcedField<T> {
    pub fn measured(value: T, source: FieldSource) -> Self {
        SourcedField::Measured {
            value: Some(value),
            source,
        }
    }

    /// A genuinely decoded ABSENCE — e.g. "Pending repair: --" — distinct from
    /// [`Self::unmeasured`] (issue #4204: rendering both as a bare `null` would
    /// erase exactly the distinction R1 exists to preserve).
    pub fn measured_absent(source: FieldSource) -> Self {
        SourcedField::Measured {
            value: None,
            source,
        }
    }

    pub fn unmeasured(cause: impl Into<String>) -> Self {
        SourcedField::Unmeasured {
            cause: cause.into(),
        }
    }

    pub fn is_measured(&self) -> bool {
        matches!(self, SourcedField::Measured { .. })
    }
}

/// One generation's cheap-tier diagnosis, plus its `--deep` results when run.
#[derive(Debug, Clone)]
pub struct GenerationDiagnosis {
    pub generation: u64,
    /// Two-letter on-disk format version, e.g. `"nb"`, `"oa"`, `"da"`.
    pub format: String,
    pub min_timestamp: i64,
    pub max_timestamp: SourcedField<i64>,
    pub min_local_deletion_time: i64,
    pub max_local_deletion_time: i64,
    pub estimated_partition_count: SourcedField<u64>,
    pub estimated_droppable_tombstone_ratio: SourcedField<f64>,
    /// The `gcBefore` (epoch seconds) the ratio above was computed against.
    pub gc_before: i64,
    pub repaired_at: i64,
    /// Lower-case hex UUID of a pending incremental-repair session, when one is
    /// authoritatively decoded and present.
    pub pending_repair: SourcedField<String>,
    pub compression_ratio: SourcedField<f64>,
    pub fully_expired_at_now: bool,
    pub deep: Option<DeepGenerationReport>,
}

/// One generation's `--deep` results (spec R3).
#[derive(Debug, Clone)]
pub struct DeepGenerationReport {
    pub partition_size_histogram: Vec<deep_scan::HistogramBucket>,
    pub clustering_width_histogram: Vec<deep_scan::HistogramBucket>,
    pub top_largest_partitions: Vec<deep_scan::RankedPartition>,
    pub top_tombstone_heaviest_partitions: Vec<deep_scan::RankedPartition>,
    pub reclaim_at_now: ReclaimPrediction,
}

/// Table-wide token-overlap histogram (spec R4, `design.md` D4).
#[derive(Debug, Clone)]
pub struct TokenOverlap {
    pub buckets: usize,
    pub counts: Vec<u64>,
}

/// The full `diagnose` report (spec R1-R5).
#[derive(Debug, Clone)]
pub struct DiagnoseReport {
    pub table_dir: PathBuf,
    pub now_secs: i64,
    pub deep: bool,
    pub top_n: usize,
    pub generations: Vec<GenerationDiagnosis>,
    pub token_overlap: TokenOverlap,
}

/// Discover every `*-Data.db` generation directly under `table_dir` (no
/// recursion), sorted for a deterministic report order. Never opens any file.
fn discover_generations(table_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(table_dir).map_err(|e| {
        Error::invalid_path(format!(
            "Cannot read SSTable table dir {}: {}",
            table_dir.display(),
            e
        ))
    })?;

    let mut data_paths = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db") {
                data_paths.push(path);
            }
        }
    }
    data_paths.sort();
    Ok(data_paths)
}

/// Run `diagnose` over every SSTable generation directly under `table_dir`
/// (spec R1-R5, plus R3/R4 under `--deep`).
pub async fn diagnose_table(table_dir: &Path, options: &DiagnoseOptions) -> Result<DiagnoseReport> {
    let data_paths = discover_generations(table_dir)?;
    if data_paths.is_empty() {
        return Err(Error::not_found(format!(
            "no SSTable generations (*-Data.db) found under {}",
            table_dir.display()
        )));
    }

    let gc_before = compute_gc_before_secs(options.gc_grace_seconds, options.now_secs);

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let mut generations = Vec::with_capacity(data_paths.len());
    let mut token_spans: Vec<(i64, i64)> = Vec::new();

    for data_path in &data_paths {
        let (gen_report, token_span) =
            diagnose_generation(data_path, options, gc_before, &config, platform.clone()).await?;
        if let Some(span) = token_span {
            token_spans.push(span);
        }
        generations.push(gen_report);
    }

    let counts = compute_token_overlap(&token_spans, TOKEN_OVERLAP_BUCKETS);

    Ok(DiagnoseReport {
        table_dir: table_dir.to_path_buf(),
        now_secs: options.now_secs,
        deep: options.deep,
        top_n: options.top_n,
        generations,
        token_overlap: TokenOverlap {
            buckets: TOKEN_OVERLAP_BUCKETS,
            counts,
        },
    })
}

/// Diagnose exactly one generation (cheap tier, plus `--deep` when requested).
/// Returns the generation's report and its `(first_token, last_token)` span for
/// the table-wide token-overlap histogram, when available (spec R4: `None`
/// — e.g. a BTI reader with no `Summary.db`-derived endpoint tokens — excludes
/// that generation from the histogram rather than guessing; see the module's
/// implementer-report note on BTI token-overlap scope).
async fn diagnose_generation(
    data_path: &Path,
    options: &DiagnoseOptions,
    gc_before: Option<i64>,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(GenerationDiagnosis, Option<(i64, i64)>)> {
    let stats_path = stats_path_for(data_path);
    let stats_bytes = tokio::fs::read(&stats_path).await.map_err(|e| {
        Error::not_found(format!(
            "Statistics.db not found/readable for {}: {}",
            data_path.display(),
            e
        ))
    })?;

    // #1249 floor gate: derive from the filename BEFORE any parse, mirroring
    // `StatisticsReader::open`.
    let gates = match VersionGates::from_path(&stats_path) {
        Ok(g) => Some(g),
        Err(e @ Error::UnsupportedVersion { .. }) => return Err(e),
        Err(_) => None,
    };

    let (_, stats): (&[u8], SSTableStatistics) =
        parse_statistics_with_fallback_detailed(&stats_bytes, gates.as_ref())?;

    let repair = parse_repair_metadata(&stats_bytes, gates.as_ref())?;
    let cell_per_partition = read_cell_per_partition_stats(&stats_bytes)?;

    let descriptor = SsTableDescriptor::parse(data_path).ok();
    let format = descriptor.map(|d| d.version).unwrap_or_default();

    // Open the reader for `generation`/`endpoint_tokens` metadata ONLY — this
    // never reads Data.db content (spec R5.1: only opened for its
    // Statistics/Index/Summary/CompressionInfo/TOC-derived metadata at
    // `open()` time; `Data.db` bytes are read only by a later `scan_stream`
    // call under `--deep`, never here).
    let reader = SSTableReader::open(data_path, config, platform.clone()).await?;
    let generation = reader.generation;
    let token_span = reader.endpoint_tokens();

    let max_timestamp = match stats.timestamp_stats.max_timestamp {
        Some(v) => SourcedField::measured(v, FieldSource::Statistics),
        None => SourcedField::unmeasured(
            "max_timestamp not authoritatively decoded from Statistics.db for this generation \
             (issue #1653)",
        ),
    };

    let estimated_partition_count =
        SourcedField::measured(stats.row_stats.partition_count, FieldSource::Statistics);

    let (estimated_droppable_tombstone_ratio, gc_before_for_ratio) = match (
        gc_before,
        cell_per_partition.as_ref(),
    ) {
        (Some(gcb), Some(cpp)) => {
            match compute_droppable_ratio(cpp, &stats.tombstone_drop_times, gcb) {
                DroppableRatio::Value(v) => {
                    (SourcedField::measured(v, FieldSource::Statistics), gcb)
                }
                DroppableRatio::Overflowed => (
                    SourcedField::unmeasured(
                        "estimatedCellPerPartitionCount histogram overflowed; Cassandra's own \
                         mean() is undefined in this state",
                    ),
                    gcb,
                ),
            }
        }
        (None, _) => (
            SourcedField::unmeasured("gc_grace_seconds is invalid (negative); purging disabled"),
            options.now_secs,
        ),
        (Some(gcb), None) => (
            SourcedField::unmeasured(
                "Statistics.db carries no STATS component; estimatedCellPerPartitionCount \
                 unavailable",
            ),
            gcb,
        ),
    };

    let pending_repair = match &repair.pending_repair {
        RepairField::Decoded(Some(uuid)) => {
            SourcedField::measured(hex_uuid(uuid), FieldSource::Statistics)
        }
        RepairField::Decoded(None) => SourcedField::measured_absent(FieldSource::Statistics),
        RepairField::Unparsed => SourcedField::unmeasured(
            "pendingRepair not reachable by the version-gated STATS walk for this generation",
        ),
    };

    let compression_ratio = match stats.compression_stats.as_ref() {
        Some(c) => SourcedField::measured(c.ratio, FieldSource::Statistics),
        None => SourcedField::unmeasured(
            "compression statistics not authoritatively parsed from Statistics.db (issue #1653)",
        ),
    };

    let fully_expired_at_now = match gc_before {
        Some(gcb) => is_fully_expired(&stats.timestamp_stats, gcb),
        None => false,
    };

    let deep = if options.deep {
        let scan = deep_scan::deep_scan_generation(
            Arc::new(reader),
            options.schema.clone(),
            options.top_n,
            gc_before.unwrap_or(options.now_secs),
        )
        .await?;
        Some(DeepGenerationReport {
            partition_size_histogram: scan.partition_size_histogram.clone(),
            clustering_width_histogram: scan.clustering_width_histogram.clone(),
            top_largest_partitions: scan.top_largest_partitions.clone(),
            top_tombstone_heaviest_partitions: scan.top_tombstone_heaviest_partitions.clone(),
            reclaim_at_now: compute_reclaim_prediction(&scan),
        })
    } else {
        None
    };

    Ok((
        GenerationDiagnosis {
            generation,
            format,
            min_timestamp: stats.timestamp_stats.min_timestamp,
            max_timestamp,
            min_local_deletion_time: stats.timestamp_stats.min_deletion_time,
            max_local_deletion_time: stats.timestamp_stats.max_deletion_time,
            estimated_partition_count,
            estimated_droppable_tombstone_ratio,
            gc_before: gc_before_for_ratio,
            repaired_at: repair.repaired_at,
            pending_repair,
            compression_ratio,
            fully_expired_at_now,
            deep,
        },
        token_span,
    ))
}

/// Sibling `Statistics.db` path for a `Data.db` path.
fn stats_path_for(data_path: &Path) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let stats_name = name.replace("-Data.db", "-Statistics.db");
    data_path
        .parent()
        .map(|p| p.join(&stats_name))
        .unwrap_or_else(|| PathBuf::from(stats_name))
}

fn hex_uuid(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_path_replaces_data_suffix() {
        let p = PathBuf::from("/a/b/nb-1-big-Data.db");
        assert_eq!(stats_path_for(&p), PathBuf::from("/a/b/nb-1-big-Statistics.db"));
    }

    #[test]
    fn hex_uuid_renders_lowercase_hex() {
        let bytes = [0xABu8; 16];
        assert_eq!(hex_uuid(&bytes), "ab".repeat(16));
    }

    #[test]
    fn sourced_field_distinguishes_absent_from_unmeasured() {
        let absent: SourcedField<String> = SourcedField::measured_absent(FieldSource::Statistics);
        let unmeasured: SourcedField<String> = SourcedField::unmeasured("cause");
        assert!(absent.is_measured());
        assert!(!unmeasured.is_measured());
        assert_ne!(absent, unmeasured);
    }
}
