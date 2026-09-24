//! `extract`/`split` (issue #4199, epic #4192) — REPAIR-family verbs that
//! pull one partition/token-range/key-set out of a table (`extract`) or
//! divide one SSTable generation into N (or byte-bounded) parts (`split`),
//! built entirely on primitives `salvage` (#4196) already established: the
//! point-read reconciling merger (`merge::point_read::build_single_partition_merger`),
//! the authoritative boundary-source walk (`salvage::boundaries`, promoted
//! `pub(crate)` by this change so a sibling module can reuse it), and the
//! decode-at-offset primitive (`SSTableReader::decode_partition_at_offset_for_salvage`).
//! See `openspec/changes/sstable-extract-split/{proposal,design}.md`.
//!
//! # Unlike `salvage`: refuse, never skip-and-record (design D2, D5)
//!
//! `salvage` exists to recover what it can from damaged input and reports a
//! loss manifest for the rest. `extract`/`split` are NOT corruption-recovery
//! tools — a decode/CRC failure on a partition either verb selected or
//! enumerated REFUSES THE WHOLE RUN (nothing published under `--out`),
//! naming the generation/offset and pointing at `salvage`/`rebuild` as the
//! remedy. There is no partial-success, `not_found` alone (an ABSENT key)
//! is the one non-refusing imperfect outcome (exit 3, design D5).
//!
//! # Module layout (design D7)
//!
//! [`selection`] — `Selection`, key-literal parsing, `Selection` → raw-key-
//! list resolution (D1.1). [`reconciled`] — reconciled `extract` (D1).
//! [`raw_copy`] — `--raw` `extract` (D2). [`split`] — `split_sstable` (D3).

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
mod raw_copy;
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
mod reconciled;
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
mod selection;
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
mod split;

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub use selection::{parse_key_literal, Selection};
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub use split::{split_sstable, SplitBoundary, SplitBoundarySummary, SplitPart, SplitReport};

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use crate::error::{Error, Result};
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use crate::schema::TableSchema;
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use crate::storage::sstable::verify::{verify_sstable_generation, VerifyMode};
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use serde::{Deserialize, Serialize};
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
use std::path::{Path, PathBuf};

/// Why `extract`/`split` refused to write any output (design D5) — unlike
/// `salvage`, there is no partial/lossy output: any decode/CRC failure on a
/// selected/enumerated partition refuses the WHOLE run.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefusalReason {
    /// `Index.db` (BIG) or `Partitions.db`/`Rows.db` (BTI) could not be read
    /// or walked structurally.
    BoundarySourceUnreadable,
    /// A selected/enumerated partition failed to decode or failed its
    /// chunk-CRC check (design D2/R6).
    PartitionDecodeFailed,
    /// `split`'s input directory resolves to more than one generation and no
    /// explicit `Data.db` path was given.
    MultipleGenerations,
    /// A produced `split` part failed its own `verify --mode full`
    /// self-audit.
    PartVerifyFailed,
}

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
impl RefusalReason {
    /// The serde kebab-case spelling design D4's manifest uses.
    pub fn manifest_label(self) -> &'static str {
        match self {
            RefusalReason::BoundarySourceUnreadable => "boundary-source-unreadable",
            RefusalReason::PartitionDecodeFailed => "partition-decode-failed",
            RefusalReason::MultipleGenerations => "multiple-generations",
            RefusalReason::PartVerifyFailed => "part-verify-failed",
        }
    }
}

/// A whole-run refusal (design D4/D5): neither verb published anything
/// under `--out`.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refused {
    pub reason: RefusalReason,
    /// The generation the refusal pertains to, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    /// The `Data.db` byte offset the refusal pertains to, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_offset: Option<u64>,
    pub detail: String,
    pub remedy: String,
}

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
const REBUILD_REMEDY: &str = "cqlite rebuild --components index (issue #4197)";
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
const SALVAGE_REMEDY: &str =
    "cqlite salvage (issue #4196) to recover every other decodable partition, or cqlite rebuild \
     (issue #4197) if the boundary source itself is suspect";

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) fn boundary_source_unreadable(detail: impl std::fmt::Display) -> Refused {
    Refused {
        reason: RefusalReason::BoundarySourceUnreadable,
        generation: None,
        data_offset: None,
        detail: detail.to_string(),
        remedy: REBUILD_REMEDY.to_string(),
    }
}

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) fn partition_decode_failed(
    generation: u64,
    data_offset: u64,
    detail: impl std::fmt::Display,
) -> Refused {
    Refused {
        reason: RefusalReason::PartitionDecodeFailed,
        generation: Some(generation),
        data_offset: Some(data_offset),
        detail: detail.to_string(),
        remedy: SALVAGE_REMEDY.to_string(),
    }
}

/// One output generation `extract` wrote (design D4).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationWritten {
    pub generation: u64,
    pub partitions: usize,
    pub rows: usize,
}

/// A resolved `Selection`, summarized for the manifest (design D4).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionSummary {
    pub kind: String,
    pub detail: String,
}

/// `extract_partitions`'s JSON manifest contract (design D4).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractReport {
    pub input: String,
    pub output: String,
    /// `"reconciled"` or `"raw"`.
    pub mode: String,
    pub selection: SelectionSummary,
    pub generations_written: Vec<GenerationWritten>,
    /// Every requested key that resolved to zero partitions in EVERY input
    /// generation (spec R3), hex-encoded raw key bytes.
    pub not_found: Vec<String>,
    pub refused: Option<Refused>,
    pub now: String,
    pub cqlite_version: String,
}

/// `extract_partitions`'s options (the `--raw` switch and where output
/// lands — design proposal's `options` bag).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone)]
pub struct ExtractOptions {
    pub out_dir: PathBuf,
    pub raw: bool,
}

/// Extract the partition(s) named by `selection` from every generation
/// under `table_dir` (design D1/D2). Default (`options.raw == false`)
/// reconciles across generations via `build_single_partition_merger` (no
/// purge) and writes ONE output generation; `--raw` writes one output
/// generation per input generation that held a match, each generation's own
/// bytes verbatim (tombstones included), never reconciled.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub async fn extract_partitions(
    table_dir: &Path,
    selection: Selection,
    schema: &TableSchema,
    options: ExtractOptions,
) -> Result<ExtractReport> {
    let now = chrono::Utc::now().to_rfc3339();
    let cqlite_version = env!("CARGO_PKG_VERSION").to_string();
    let selection_summary = selection.summarize();

    // `SSTableWriter` always nests its output under
    // `<out_dir>/<keyspace>/<table>/` (`writer/mod.rs`), so the manifest
    // records the RESOLVED landing directory, not the bare `--out` root
    // (matching salvage's own round-11 finding).
    let resolved_output = options
        .out_dir
        .join(&schema.keyspace)
        .join(&schema.table)
        .display()
        .to_string();
    let report_skeleton = || ExtractReport {
        input: table_dir.display().to_string(),
        output: resolved_output.clone(),
        mode: if options.raw { "raw" } else { "reconciled" }.to_string(),
        selection: selection_summary.clone(),
        generations_written: Vec::new(),
        not_found: Vec::new(),
        refused: None,
        now: now.clone(),
        cqlite_version: cqlite_version.clone(),
    };

    let generation_paths = match discover_generations(table_dir) {
        Ok(paths) if !paths.is_empty() => paths,
        Ok(_) => {
            let mut report = report_skeleton();
            report.refused = Some(boundary_source_unreadable(format!(
                "{} contains no *-Data.db generation",
                table_dir.display()
            )));
            return Ok(report);
        }
        Err(e) => {
            let mut report = report_skeleton();
            report.refused = Some(boundary_source_unreadable(e));
            return Ok(report);
        }
    };

    if options.raw {
        raw_copy::extract_raw(
            &generation_paths,
            selection,
            schema,
            &options.out_dir,
            report_skeleton(),
        )
        .await
    } else {
        reconciled::extract_reconciled(
            &generation_paths,
            selection,
            schema,
            &options.out_dir,
            report_skeleton(),
        )
        .await
    }
}

/// Every `*-Data.db` generation under `dir`, sorted by generation id
/// (ascending — oldest first, matching `salvage`'s discovery convention).
/// `dir` itself may also be a single `Data.db` FILE, in which case that one
/// path is returned. A file whose generation id does not parse as an
/// integer is refused by name (UUID-form SSTable ids are out of scope, same
/// restriction `salvage_sstable` enforces).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) fn discover_generations(dir: &Path) -> Result<Vec<PathBuf>> {
    if dir.is_file() {
        return Ok(vec![dir.to_path_buf()]);
    }
    if !dir.is_dir() {
        return Err(Error::InvalidInput(format!(
            "{} is neither a Data.db file nor a directory",
            dir.display()
        )));
    }
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(Error::Io)? {
        let entry = entry.map_err(Error::Io)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with("-Data.db") {
            continue;
        }
        let descriptor = SsTableDescriptor::parse(&path)?;
        let generation: u64 = descriptor.sstable_id.parse().map_err(|_| {
            Error::InvalidInput(format!(
                "{}: extract/split require a sequential-integer SSTable id (got '{}'); \
                 UUID-form SSTable ids are not yet supported",
                path.display(),
                descriptor.sstable_id
            ))
        })?;
        found.push((generation, path));
    }
    found.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok(found.into_iter().map(|(_, p)| p).collect())
}

/// `(base, is_bti)` for one `*-Data.db` path, per the shared descriptor
/// parse both `selection`/`raw_copy`/`split` need.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) fn base_and_format(data_db: &Path) -> Result<(String, bool)> {
    let descriptor = SsTableDescriptor::parse(data_db)?;
    let is_bti = descriptor.format == SsTableFormat::Bti;
    let base = crate::storage::sstable::reader::extract_sstable_base_name(data_db).ok_or_else(
        || Error::InvalidInput(format!("cannot derive an SSTable base name from {}", data_db.display())),
    )?;
    Ok((base, is_bti))
}

/// The generation id parsed from `data_db`'s filename (sequential-integer
/// SSTable ids only — see [`discover_generations`]).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) fn generation_of(data_db: &Path) -> Result<u64> {
    let descriptor = SsTableDescriptor::parse(data_db)?;
    descriptor.sstable_id.parse().map_err(|_| {
        Error::InvalidInput(format!(
            "{}: extract/split require a sequential-integer SSTable id (got '{}')",
            data_db.display(),
            descriptor.sstable_id
        ))
    })
}

/// Run `verify --mode full` against the just-written generation at
/// `data_db_path` (design D1/D3's self-audit, mirroring the REPAIR-family
/// convention). `Ok(true)` iff the report carries zero findings.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) async fn verify_output_generation(data_db_path: &Path) -> Result<crate::storage::sstable::verify::VerifyReport> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await?);
    verify_sstable_generation(data_db_path, VerifyMode::Full, &config, platform).await
}
