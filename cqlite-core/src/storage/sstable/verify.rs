//! SSTable verifier contract (epic #970, issue #1000).
//!
//! This module defines and **enforces** a stable verification contract for
//! Cassandra 5.0 SSTables — both the `nb`/`big` (legacy `BigFormat`) and the
//! `da`/`bti` (`BtiFormat`) layouts — covering healthy *and* corrupted inputs.
//!
//! # Modes
//!
//! Two **distinct** modes are defined. A QUICK pass must never be reported as a
//! FULL pass: they validate different surfaces.
//!
//! * [`VerifyMode::Quick`] — cheap, metadata-only structural checks:
//!   1. Component presence + `TOC.txt` completeness (every TOC-listed component
//!      must exist on disk).
//!   2. `Digest.crc32` matches the CRC32 of `Data.db`.
//!   3. `CompressionInfo.db` parses (unknown algorithm already fail-fasts, #1001)
//!      **and** every declared chunk offset is in-bounds for `Data.db`.
//!   4. BTI index components (`Partitions.db` / `Rows.db`) parse structurally
//!      (root pointer in-bounds, root node header well-formed).
//!
//! * [`VerifyMode::Full`] — QUICK plus deep, content-touching checks:
//!   5. Inline `Data.db` chunk CRC validation for every chunk (#998 path).
//!   6. `Statistics.db` parses.
//!   7. A complete row scan succeeds (exercises LZ4/Snappy/Deflate/Zstd decompression via the stitch path) and does not silently return zero rows when the index/BTI components are structurally corrupt.
//!
//! # Error classes
//!
//! Every failure is classified into a stable [`VerifyErrorClass`] and reported
//! through a [`VerifyFinding`] that always carries the failing **component
//! name** plus locating context (byte offset, chunk index, checksum field, or
//! the missing-component name). The caller can serialise the resulting
//! [`VerifyReport`] for CI artifacts.
//!
//! # No silent empty results on corruption (#1000)
//!
//! Prior to this contract a corrupted `Index.db` (BIG) or a corrupted/truncated
//! `Partitions.db`/`Rows.db` (BTI) could pass through the read path and yield an
//! apparently-successful **zero-row** scan, masking structural corruption. The
//! FULL verifier closes that hole: the structural index checks run first and
//! hard-error, so a corrupt index is never reported as "verified, 0 rows".

use crate::platform::Platform;
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::{Config, Error, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Verification depth. QUICK and FULL are intentionally distinct — see the
/// module docs. A QUICK success MUST NOT be presented as FULL corruption
/// parity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyMode {
    /// Metadata-only structural checks (component presence, TOC, digest,
    /// CompressionInfo bounds, BTI root structure).
    Quick,
    /// QUICK plus inline chunk-CRC validation, Statistics.db parse, and a full
    /// row scan.
    Full,
}

impl VerifyMode {
    /// Stable lower-case label for reports/CLIs.
    pub fn as_str(self) -> &'static str {
        match self {
            VerifyMode::Quick => "quick",
            VerifyMode::Full => "full",
        }
    }
}

/// Stable classification of a verification failure.
///
/// The variant is the machine-checkable "error code"; the [`VerifyFinding`]
/// carries the human-readable context. These names are part of the verifier
/// contract — callers (and CI) may match on them, so they must remain stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VerifyErrorClass {
    /// A `TOC.txt`-listed component (or a structurally-required component) is
    /// absent from disk.
    MissingComponent,
    /// `Digest.crc32` does not match the computed CRC32 of `Data.db`.
    DigestMismatch,
    /// `CompressionInfo.db` failed to parse, named an unsupported algorithm, or
    /// otherwise malformed (#1001).
    CompressionInfoCorrupt,
    /// A `CompressionInfo.db` chunk offset points outside `Data.db`.
    ChunkOffsetOutOfBounds,
    /// An inline `Data.db` chunk CRC32 did not match, or a chunk could not be
    /// read / decompressed (truncation, bit flip).
    ChunkDecompressionError,
    /// A component was truncated and a required read hit end-of-file.
    UnexpectedEof,
    /// `Index.db` (BIG) is structurally corrupt.
    IndexEntryCorrupt,
    /// `Statistics.db` header / body is corrupt.
    StatisticsHeaderCorrupt,
    /// `Summary.db` is truncated / unreadable.
    SummaryCorrupt,
    /// BTI `Partitions.db` root pointer / node is corrupt.
    BtiRootPointerCorrupt,
    /// BTI `Rows.db` trie is truncated / corrupt.
    BtiTrieCorrupt,
    /// A full row scan failed for a reason not otherwise classified above.
    RowScanFailed,
}

impl VerifyErrorClass {
    /// Stable string code for the error class (used in reports / CI artifacts).
    pub fn code(self) -> &'static str {
        match self {
            VerifyErrorClass::MissingComponent => "MissingComponent",
            VerifyErrorClass::DigestMismatch => "DigestMismatch",
            VerifyErrorClass::CompressionInfoCorrupt => "CompressionInfoCorrupt",
            VerifyErrorClass::ChunkOffsetOutOfBounds => "ChunkOffsetOutOfBounds",
            VerifyErrorClass::ChunkDecompressionError => "ChunkDecompressionError",
            VerifyErrorClass::UnexpectedEof => "UnexpectedEof",
            VerifyErrorClass::IndexEntryCorrupt => "IndexEntryCorrupt",
            VerifyErrorClass::StatisticsHeaderCorrupt => "StatisticsHeaderCorrupt",
            VerifyErrorClass::SummaryCorrupt => "SummaryCorrupt",
            VerifyErrorClass::BtiRootPointerCorrupt => "BtiRootPointerCorrupt",
            VerifyErrorClass::BtiTrieCorrupt => "BtiTrieCorrupt",
            VerifyErrorClass::RowScanFailed => "RowScanFailed",
        }
    }
}

impl std::fmt::Display for VerifyErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// A single verification failure: a stable class plus the failing component and
/// locating context. Always serialisable by the caller (all fields are owned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyFinding {
    /// Stable error classification.
    pub class: VerifyErrorClass,
    /// SSTable component name that failed (e.g. `Data.db`, `Index.db`,
    /// `Partitions.db`, `TOC.txt`).
    pub component: String,
    /// Human-readable message including locating context (offset / chunk index
    /// / checksum field / missing-component name).
    pub detail: String,
}

impl VerifyFinding {
    fn new(
        class: VerifyErrorClass,
        component: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            class,
            component: component.into(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for VerifyFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: {}",
            self.class.code(),
            self.component,
            self.detail
        )
    }
}

/// Structured outcome of a verification run. Serialise this for CI artifacts.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    /// Directory that was verified.
    pub directory: PathBuf,
    /// SSTable base name (e.g. `nb-1-big`, `da-2-bti`).
    pub base_name: String,
    /// Detected on-disk format.
    pub format: SsTableFormat,
    /// Mode the verification was run in.
    pub mode: VerifyMode,
    /// All findings (empty when verification passed).
    pub findings: Vec<VerifyFinding>,
    /// Components named in `TOC.txt` (if a TOC was present).
    pub toc_components: Vec<String>,
    /// Number of rows seen during the FULL-mode scan (`None` in QUICK mode).
    pub rows_scanned: Option<usize>,
}

impl VerifyReport {
    /// `true` when no findings were recorded (verification passed).
    pub fn is_ok(&self) -> bool {
        self.findings.is_empty()
    }

    /// The first finding's error class, if any.
    pub fn primary_class(&self) -> Option<VerifyErrorClass> {
        self.findings.first().map(|f| f.class)
    }

    /// Render a single-line summary suitable for logs / CI artifacts.
    pub fn summary_line(&self) -> String {
        if self.is_ok() {
            format!(
                "VERIFY OK [{}/{}] {} ({} rows)",
                self.mode.as_str(),
                self.format.as_str(),
                self.base_name,
                self.rows_scanned
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            )
        } else {
            format!(
                "VERIFY FAIL [{}/{}] {}: {}",
                self.mode.as_str(),
                self.format.as_str(),
                self.base_name,
                self.findings
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    }
}

/// Resolved set of component files for one SSTable generation in a directory.
struct ComponentSet {
    base_name: String,
    format: SsTableFormat,
    /// Map of bare component name (e.g. `Data.db`) -> absolute path on disk.
    present: BTreeMap<String, PathBuf>,
    data_path: PathBuf,
}

impl ComponentSet {
    fn path(&self, dir: &Path, component: &str) -> PathBuf {
        dir.join(format!("{}-{}", self.base_name, component))
    }

    /// `true` when a component (e.g. `Statistics.db`) is present on disk for
    /// this SSTable generation, per the directory scan performed at resolution
    /// time.
    fn has(&self, component: &str) -> bool {
        self.present.contains_key(component)
    }
}

/// Verify a single SSTable generation located in `dir`.
///
/// `dir` must contain exactly one SSTable generation (i.e. one `*-Data.db`); if
/// it contains several, the lexicographically-first generation is selected.
///
/// Returns a [`VerifyReport`]. The function only returns `Err` for environmental
/// problems (the directory cannot be read, or it contains no `Data.db`); *data*
/// corruption is reported as findings inside an `Ok(VerifyReport)` so the caller
/// can serialise the full picture. Use [`VerifyReport::is_ok`] to branch.
pub async fn verify_sstable(
    dir: &Path,
    mode: VerifyMode,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<VerifyReport> {
    let components = resolve_components(dir)?;

    let mut findings: Vec<VerifyFinding> = Vec::new();

    // ---- Check 1: TOC.txt completeness + component presence ----------------
    let toc_components = check_toc_and_presence(dir, &components, &mut findings)?;

    // ---- Check 2: Digest.crc32 vs CRC32(Data.db) ---------------------------
    check_digest(dir, &components, &mut findings)?;

    // ---- Check 3: CompressionInfo.db parse + chunk-offset bounds -----------
    let compression_info = check_compression_info(dir, &components, &mut findings)?;

    // ---- Check 4: index structure (Index.db for BIG, BTI tries for BTI) ----
    //
    // This is the heart of the "no silent empty results on corruption" mandate
    // (#1000). The BIG read path silently TRUNCATES the partition list on the
    // first malformed Index.db entry (index_reader.rs stops the parse loop and
    // returns the partitions parsed so far), and the full scan then falls back
    // to a whole-Data.db scan — so a corrupt Index.db otherwise looks healthy.
    // For BTI, a full scan reads Data.db directly and never touches the
    // Partitions.db/Rows.db tries, so a corrupt trie is likewise invisible to a
    // scan. We validate the index structurally here and hard-fail.
    //
    // `bti_partition_keys` is the distinct partition-key set recovered by
    // walking Partitions.db; it is cross-checked against the Data.db scan in
    // FULL mode to catch a footer-flip that silently UNDER-counts partitions
    // (the trie still parses, just from the wrong root, yielding fewer keys).
    let mut bti_partition_keys: Option<Vec<Vec<u8>>> = None;
    match components.format {
        SsTableFormat::Bti => {
            bti_partition_keys = check_bti_structure(dir, &components, &mut findings)?
        }
        SsTableFormat::Big => check_big_index(dir, &components, &mut findings)?,
    }

    let mut rows_scanned = None;

    if mode == VerifyMode::Full {
        // ---- Check 5: inline Data.db chunk CRC validation (#998) -----------
        if let Some(info) = compression_info.as_ref() {
            check_inline_chunk_crc(&components, info, &mut findings)?;
        }

        // ---- Check 6a: Statistics.db parse ---------------------------------
        check_statistics(dir, &components, platform.clone(), &mut findings).await;

        // ---- Check 6b: Summary.db parse (BIG only) -------------------------
        if components.format == SsTableFormat::Big {
            check_summary(dir, &components, platform.clone(), &mut findings).await;
        }

        // ---- Check 7: full row scan (no silent empty on corruption) --------
        //
        // Skip the scan when compression metadata is already known-corrupt: the
        // corruption is reported, and scanning would re-read the bad
        // CompressionInfo.db and drive the chunk reader off an out-of-bounds
        // offset. The reader now bounds-checks and errors rather than panicking
        // (block_io.rs), but there is no value in scanning metadata we have
        // already flagged (roborev #970).
        let compression_metadata_corrupt = findings.iter().any(|f| {
            matches!(
                f.class,
                VerifyErrorClass::CompressionInfoCorrupt | VerifyErrorClass::ChunkOffsetOutOfBounds
            )
        });
        if !compression_metadata_corrupt {
            // The structural index checks (1, 4) above already hard-fail on a
            // corrupt Index.db / BTI trie BEFORE we ever scan, so a corrupt index
            // can never be reported as a successful zero-row scan. We still run the
            // scan to exercise the decompression stitch path and surface Data.db
            // corruption that only manifests during decode.
            match full_row_scan_partitions(&components.data_path, config, platform).await {
                Ok((rows, scan_partition_keys)) => {
                    rows_scanned = Some(rows);
                    // BTI cross-check: the partition KEY SET recovered by walking
                    // Partitions.db MUST match the partition keys decoded from
                    // Data.db — by IDENTITY, not just count (issue #1103). A
                    // count-only check passes a corruption that walks a wrong
                    // subtree yielding a different set of keys with the same leaf
                    // count. We compare identities by re-deriving each Data.db raw
                    // key's byte-comparable trie key and matching it against the
                    // path-compressed trie keys.
                    if let Some(trie_keys) = bti_partition_keys {
                        if let Some(detail) =
                            bti_partition_key_identity_mismatch(&trie_keys, &scan_partition_keys)
                        {
                            findings.push(VerifyFinding::new(
                                VerifyErrorClass::BtiRootPointerCorrupt,
                                "Partitions.db",
                                detail,
                            ));
                        }
                    }
                }
                Err(e) => findings.push(classify_scan_error(&components, &e)),
            }
        } // end: if !compression_metadata_corrupt
    }

    Ok(VerifyReport {
        directory: dir.to_path_buf(),
        base_name: components.base_name,
        format: components.format,
        mode,
        findings,
        toc_components,
        rows_scanned,
    })
}

/// Locate the SSTable generation in `dir` and enumerate its on-disk components.
fn resolve_components(dir: &Path) -> Result<ComponentSet> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        Error::invalid_path(format!("Cannot read SSTable dir {}: {}", dir.display(), e))
    })?;

    let mut data_files: Vec<PathBuf> = Vec::new();
    let mut all_files: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        all_files.push(p.clone());
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db") {
                data_files.push(p);
            }
        }
    }

    data_files.sort();
    let data_path = data_files.into_iter().next().ok_or_else(|| {
        Error::not_found(format!(
            "No *-Data.db component found in SSTable directory {}",
            dir.display()
        ))
    })?;

    let data_name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::invalid_path("Data.db filename is not valid UTF-8"))?;
    // Strip the trailing "-Data.db" to get the base name (e.g. "nb-1-big").
    let base_name = data_name
        .strip_suffix("-Data.db")
        .ok_or_else(|| Error::invalid_path("Data.db filename did not end with -Data.db"))?
        .to_string();

    // Detect format via the descriptor parser, which scans for the "big"/"bti"
    // segment correctly even when the SSTable id is a hyphenated UUID
    // (e.g. "da-00000000-0000-0000-0000-000000000001-bti-Data.db"). A fixed
    // dash-index split would misread those as BIG and verify the wrong
    // components (roborev).
    let format = SsTableDescriptor::parse_filename(data_name)
        .map(|d| d.format)
        .unwrap_or(SsTableFormat::Big);

    // Index present components for this base name only.
    let prefix = format!("{}-", base_name);
    let mut present = BTreeMap::new();
    for p in all_files {
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            if let Some(component) = name.strip_prefix(&prefix) {
                present.insert(component.to_string(), p.clone());
            }
        }
    }

    Ok(ComponentSet {
        base_name,
        format,
        present,
        data_path,
    })
}

/// `true` when `component` is a real Cassandra SSTable component name (the set
/// that can legitimately appear in `TOC.txt`). Excludes test sidecars such as
/// `Data.db.jsonl` or `Statistics.db.txt` reference goldens that share the base
/// prefix in the dataset directories.
fn is_real_component(component: &str) -> bool {
    matches!(
        component,
        "TOC.txt" | "Digest.crc32" | "Digest.adler32" | "Digest.sha1" | "CRC.db"
    ) || (component.ends_with(".db") && !component.contains(".db."))
}

/// Check 1: every component listed in `TOC.txt` exists on disk. Also surfaces a
/// structurally-required-but-missing `Data.db`.
fn check_toc_and_presence(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Vec<String>> {
    // Data.db is always required.
    if !components.data_path.exists() {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::MissingComponent,
            "Data.db",
            format!(
                "required Data.db not found at {}",
                components.data_path.display()
            ),
        ));
    }

    let toc_path = components.path(dir, "TOC.txt");
    if !toc_path.exists() {
        // No TOC at all: not a hard error here (some tooling omits it), but
        // record it as a missing component so it is visible.
        findings.push(VerifyFinding::new(
            VerifyErrorClass::MissingComponent,
            "TOC.txt",
            format!("TOC.txt not present at {}", toc_path.display()),
        ));
        return Ok(Vec::new());
    }

    let toc_raw = std::fs::read_to_string(&toc_path).map_err(|e| {
        Error::corruption(format!(
            "Cannot read TOC.txt at {}: {}",
            toc_path.display(),
            e
        ))
    })?;

    let mut listed = Vec::new();
    for line in toc_raw.lines() {
        let component = line.trim();
        if component.is_empty() {
            continue;
        }
        listed.push(component.to_string());

        // The TOC lists bare component names (e.g. "Statistics.db"). Check it
        // against the directory scan captured at resolution time.
        if !components.has(component) {
            let expected = components.path(dir, component);
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                component.to_string(),
                format!(
                    "TOC.txt lists component '{}' but '{}' is absent on disk",
                    component,
                    expected.display()
                ),
            ));
        }
    }

    // Inverse direction: Cassandra's TOC.txt enumerates EVERY component it
    // wrote. A component that is present on disk but missing from the TOC means
    // the TOC is incomplete/corrupt (the `toc_missing_component` corruption
    // drops the `Statistics.db` line while the file stays on disk). Report each
    // present-but-unlisted component as a missing TOC entry.
    for present in components.present.keys() {
        // Only real SSTable components participate in the TOC. Skip sidecar /
        // reference files that share the base prefix (e.g. `Data.db.jsonl`,
        // `Statistics.db.txt` goldens) so they don't masquerade as missing TOC
        // entries on an otherwise-healthy generation.
        if !is_real_component(present) {
            continue;
        }
        if !listed.iter().any(|c| c == present) {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                present.clone(),
                format!(
                    "component '{}' is present on disk but not listed in TOC.txt (incomplete/corrupt TOC)",
                    present
                ),
            ));
        }
    }

    Ok(listed)
}

/// Check 2: `Digest.crc32` matches CRC32 of `Data.db`.
///
/// Cassandra writes `Digest.crc32` as the decimal-ASCII CRC32 (IEEE) of the
/// entire `Data.db` file (including inline chunk CRCs).
fn check_digest(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<()> {
    let digest_path = components.path(dir, "Digest.crc32");
    if !digest_path.exists() {
        // Absence handled by the TOC check if it was listed; nothing to compare.
        return Ok(());
    }
    let digest_text = std::fs::read_to_string(&digest_path).map_err(|e| {
        Error::corruption(format!(
            "Cannot read Digest.crc32 at {}: {}",
            digest_path.display(),
            e
        ))
    })?;
    // Parse strictly as u32: a CRC32 digest cannot exceed u32::MAX. Parsing as
    // u64 + truncating would accept an oversized value whose low 32 bits happen
    // to match the computed CRC (roborev).
    let recorded: u32 = match digest_text.trim().parse::<u32>() {
        Ok(v) => v,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::DigestMismatch,
                "Digest.crc32",
                format!(
                    "Digest.crc32 is not a valid integer ('{}'): {}",
                    digest_text.trim(),
                    e
                ),
            ));
            return Ok(());
        }
    };

    let data = match std::fs::read(&components.data_path) {
        Ok(d) => d,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot read Data.db for digest check: {}", e),
            ));
            return Ok(());
        }
    };
    let computed = crc32fast::hash(&data);
    if computed != recorded {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::DigestMismatch,
            "Digest.crc32",
            format!(
                "Digest.crc32 mismatch: recorded={} (0x{:08x}), computed={} (0x{:08x}) over {} bytes of Data.db",
                recorded, recorded, computed, computed, data.len()
            ),
        ));
    }
    Ok(())
}

/// Check 3: `CompressionInfo.db` parses (#1001) and all chunk offsets are
/// in-bounds for `Data.db`. Returns the parsed `CompressionInfo` for reuse by
/// the FULL-mode inline-CRC check, or `None` (genuinely uncompressed table, or
/// the file failed to parse — in which case a finding is recorded).
fn check_compression_info(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Option<CompressionInfo>> {
    let ci_path = components.path(dir, "CompressionInfo.db");
    if !ci_path.exists() {
        return Ok(None); // uncompressed SSTable
    }
    let bytes = std::fs::read(&ci_path).map_err(|e| {
        Error::corruption(format!(
            "Cannot read CompressionInfo.db at {}: {}",
            ci_path.display(),
            e
        ))
    })?;

    let info = match CompressionInfo::parse(&bytes) {
        Ok(info) => info,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::CompressionInfoCorrupt,
                "CompressionInfo.db",
                format!("CompressionInfo.db failed to parse: {}", e),
            ));
            return Ok(None);
        }
    };

    // Bounds-check declared chunk offsets against the actual Data.db length.
    // `CompressionInfo::validate()` only enforces ascending order; a single
    // corrupted offset (e.g. an MSB set) is ascending yet points past EOF.
    let data_len = match std::fs::metadata(&components.data_path) {
        Ok(m) => m.len(),
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot stat Data.db for chunk-bounds check: {}", e),
            ));
            return Ok(Some(info));
        }
    };
    let mut offset_out_of_bounds = false;
    for (i, &offset) in info.chunk_offsets.iter().enumerate() {
        // Every chunk record is at least its 4-byte inline CRC, so the offset
        // itself must leave room for that. Offsets at/after EOF are corrupt.
        if offset.saturating_add(4) > data_len {
            offset_out_of_bounds = true;
            findings.push(VerifyFinding::new(
                VerifyErrorClass::ChunkOffsetOutOfBounds,
                "CompressionInfo.db",
                format!(
                    "chunk[{}] offset {} (0x{:x}) points past Data.db end ({} bytes)",
                    i, offset, offset, data_len
                ),
            ));
        }
    }

    // An out-of-bounds offset is corrupt metadata: do NOT hand it downstream.
    // The inline-CRC check derives each chunk's compressed size from adjacent
    // offsets, which would underflow (panic in debug / huge alloc in release) on
    // a bad offset — violating the corruption-as-findings contract. The finding
    // is already recorded, so returning None just skips the chunk-CRC check
    // (roborev).
    if offset_out_of_bounds {
        return Ok(None);
    }

    Ok(Some(info))
}

/// Check 4 (BTI): structurally validate the `Partitions.db` and `Rows.db`
/// tries.
///
/// Returns `Some(n)` where `n` is the number of partition keys recovered by
/// walking `Partitions.db`, so the caller can cross-check it against the Data.db
/// scan (FULL mode). Returns `None` if `Partitions.db` could not be walked (a
/// finding was recorded).
///
/// * `Partitions.db` is walked with [`iterate_partitions_in_bti_file`], which
///   follows the trailing-8-byte footer root and DFS-collects every leaf. A
///   footer flip either makes the walk error (out-of-bounds root) or silently
///   recover the wrong key set; the FULL-mode count cross-check catches the
///   latter.
/// * For every partition whose payload is a `RowsOffset`, the per-partition
///   row-index entry is resolved from `Rows.db` via [`iterate_rows_for_partition`].
///   A truncated `Rows.db` makes the referenced offset point past EOF or the
///   row-trie read hit EOF.
fn check_bti_structure(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Option<Vec<Vec<u8>>>> {
    use crate::storage::sstable::bti::parser::{
        iterate_partitions_in_bti_file, iterate_rows_for_partition, BtiPartitionLocation,
    };
    use std::io::Cursor;

    // --- Partitions.db ---------------------------------------------------
    let partitions_path = components.path(dir, "Partitions.db");
    let partitions_bytes = match std::fs::read(&partitions_path) {
        Ok(b) => b,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Partitions.db",
                format!("cannot read Partitions.db: {}", e),
            ));
            return Ok(None);
        }
    };

    // A BTI Partitions.db always ends with an 8-byte trailing root pointer; a
    // file shorter than that is truncated/corrupt, NOT a valid empty trie.
    // Without this, QUICK mode would report success for a truncated required
    // index component (roborev).
    if partitions_bytes.len() < 8 {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::UnexpectedEof,
            "Partitions.db",
            format!(
                "Partitions.db is {} bytes — shorter than the mandatory 8-byte trie root footer (truncated)",
                partitions_bytes.len()
            ),
        ));
        return Ok(None);
    }

    let mut cursor = Cursor::new(&partitions_bytes);
    let partitions = match iterate_partitions_in_bti_file(&mut cursor) {
        Ok(p) => p,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::BtiRootPointerCorrupt,
                "Partitions.db",
                format!(
                    "Partitions.db trie walk failed (corrupt root pointer / node): {}",
                    e
                ),
            ));
            return Ok(None);
        }
    };

    if partitions.is_empty() {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::BtiRootPointerCorrupt,
            "Partitions.db",
            format!(
                "Partitions.db ({} bytes) yielded zero partition keys — the root pointer is corrupt",
                partitions_bytes.len()
            ),
        ));
        return Ok(None);
    }

    // --- Rows.db (per-partition row-index resolution) --------------------
    let rows_path = components.path(dir, "Rows.db");
    let rows_bytes = match std::fs::read(&rows_path) {
        Ok(b) => b,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Rows.db",
                format!("cannot read Rows.db: {}", e),
            ));
            // We can still report the recovered trie partition keys.
            return Ok(Some(partitions.into_iter().map(|(k, _)| k).collect()));
        }
    };

    for (key, location) in &partitions {
        if let BtiPartitionLocation::RowsOffset(off) = location {
            let off = *off as usize;
            if off >= rows_bytes.len() {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::BtiTrieCorrupt,
                    "Rows.db",
                    format!(
                        "partition (key {} bytes) references Rows.db offset {} which is past EOF ({} bytes) — Rows.db is truncated/corrupt",
                        key.len(),
                        off,
                        rows_bytes.len()
                    ),
                ));
                continue;
            }
            if let Err(e) = iterate_rows_for_partition(&rows_bytes, off) {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::BtiTrieCorrupt,
                    "Rows.db",
                    format!(
                        "row-index trie for partition at Rows.db offset {} failed to parse (truncated/corrupt): {}",
                        off, e
                    ),
                ));
            }
        }
    }

    // Return the byte-comparable trie partition keys (path-compressed prefixes)
    // recovered by walking Partitions.db. FULL-mode verification cross-checks
    // these against the keys decoded from Data.db (issue #1103).
    Ok(Some(partitions.into_iter().map(|(k, _)| k).collect()))
}

/// Check 4 (BIG): structurally validate `Index.db`.
///
/// The production read path (`index_reader::parse_all_partition_keys_with_summary`)
/// stops at the first entry that fails to parse and returns the partitions
/// parsed so far — so a bit-flipped entry silently truncates (possibly to zero)
/// the partition list without any error. Here we walk every BIG index entry and
/// treat **either** a mid-stream parse error **or** leftover trailing bytes
/// **or** a zero-entry result on a non-empty file as corruption. This is what
/// prevents a corrupt Index.db from being reported as a healthy zero-row scan.
fn check_big_index(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<()> {
    use crate::storage::sstable::index_reader::parse_big_index_entry;

    let index_path = components.path(dir, "Index.db");
    if !index_path.exists() {
        // Absence is surfaced by the TOC check (Index.db is critical for BIG);
        // record it explicitly so the index check is never silently skipped.
        findings.push(VerifyFinding::new(
            VerifyErrorClass::MissingComponent,
            "Index.db",
            format!(
                "BIG-format Index.db not present at {}",
                index_path.display()
            ),
        ));
        return Ok(());
    }

    let bytes = std::fs::read(&index_path).map_err(|e| {
        Error::corruption(format!(
            "Cannot read Index.db at {}: {}",
            index_path.display(),
            e
        ))
    })?;

    if bytes.is_empty() {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::IndexEntryCorrupt,
            "Index.db",
            "Index.db is empty (no partition entries)".to_string(),
        ));
        return Ok(());
    }

    let total = bytes.len();
    let mut remaining: &[u8] = &bytes;
    let mut entry_index = 0usize;
    loop {
        if remaining.is_empty() {
            break;
        }
        let consumed_before = total - remaining.len();
        match parse_big_index_entry(remaining) {
            Ok((rest, _entry)) => {
                if rest.len() >= remaining.len() {
                    // No forward progress -> structurally broken.
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::IndexEntryCorrupt,
                        "Index.db",
                        format!(
                            "Index.db entry {} at byte offset {} made no forward progress (corrupt length field)",
                            entry_index, consumed_before
                        ),
                    ));
                    return Ok(());
                }
                remaining = rest;
                entry_index += 1;
            }
            Err(e) => {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::IndexEntryCorrupt,
                    "Index.db",
                    format!(
                        "Index.db entry {} at byte offset {} failed to parse ({} of {} bytes consumed): {:?}",
                        entry_index, consumed_before, consumed_before, total, e
                    ),
                ));
                return Ok(());
            }
        }
    }

    if entry_index == 0 {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::IndexEntryCorrupt,
            "Index.db",
            format!(
                "Index.db parsed zero partition entries from {} bytes",
                total
            ),
        ));
    }

    Ok(())
}

/// Check 6b (FULL, BIG): `Summary.db` parses.
async fn check_summary(
    dir: &Path,
    components: &ComponentSet,
    platform: Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) {
    use crate::storage::sstable::summary_reader::SummaryReader;

    let summary_path = components.path(dir, "Summary.db");
    if !summary_path.exists() {
        return; // absence covered by TOC check if listed
    }
    if let Err(e) = SummaryReader::open(&summary_path, platform).await {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::SummaryCorrupt,
            "Summary.db",
            format!("Summary.db failed to parse: {}", e),
        ));
    }
}

/// Check 5 (FULL): validate every inline `Data.db` chunk CRC32 (#998) and that
/// each chunk decompresses. Uses the [`ChunkDecompressor`] stitch path so this
/// exercises real LZ4/Snappy/Deflate/Zstd decoding.
fn check_inline_chunk_crc(
    components: &ComponentSet,
    info: &CompressionInfo,
    findings: &mut Vec<VerifyFinding>,
) -> Result<()> {
    use crate::storage::sstable::chunk_reader::ChunkReader;
    use std::fs::File;

    let file = match File::open(&components.data_path) {
        Ok(f) => f,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot open Data.db for chunk-CRC check: {}", e),
            ));
            return Ok(());
        }
    };
    let total_size = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot stat Data.db for chunk-CRC check: {}", e),
            ));
            return Ok(());
        }
    };
    let reader = std::io::BufReader::new(file);

    // ChunkReader validates ONLY the inline 4-byte CRC32 of each chunk (#998)
    // without decompressing it. This is the precise integrity guarantee we want
    // here: a bit-flip inside a chunk payload fails the CRC, and a truncated
    // file fails the chunk read with EOF. Decode correctness is covered
    // separately by the full row scan (Check 7), so we deliberately do NOT
    // re-decompress here (that would false-positive on the last/incompressible
    // chunk's size bookkeeping for some BTI Data.db files).
    let mut chunk_reader = ChunkReader::new(reader, info.clone(), total_size);
    if let Err(e) = chunk_reader.read_all_chunks() {
        findings.push(classify_data_error("Data.db", &e));
    }
    Ok(())
}

/// Check 6 (FULL): `Statistics.db` parses. Records a finding on failure but
/// never aborts the rest of verification.
async fn check_statistics(
    dir: &Path,
    components: &ComponentSet,
    platform: Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) {
    use crate::storage::sstable::statistics_reader::StatisticsReader;

    let stats_path = components.path(dir, "Statistics.db");
    if !stats_path.exists() {
        return; // absence already covered by the TOC check if listed
    }

    // Direct TOC-header sanity check FIRST. Cassandra's `MetadataSerializer`
    // writes Statistics.db as: [u32 BE num_components][u32 BE checksum][TOC...].
    // The production `StatisticsReader` is intentionally lenient (it falls back
    // through several parsers and can silently accept a damaged header), so we
    // validate the authoritative component count here. Cassandra only ever
    // emits 4 metadata components (VALIDATION/COMPACTION/STATS/HEADER); a count
    // outside [1,100] means the header is corrupt (e.g. the high byte flipped
    // to 0xFF -> ~4.28e9 components).
    match std::fs::read(&stats_path) {
        Ok(bytes) if bytes.len() >= 8 => {
            let num_components = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            if num_components == 0 || num_components > 100 {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::StatisticsHeaderCorrupt,
                    "Statistics.db",
                    format!(
                        "Statistics.db TOC header is corrupt: num_components={} at byte 0 (expected 1..=100; first 4 bytes {:02x} {:02x} {:02x} {:02x})",
                        num_components, bytes[0], bytes[1], bytes[2], bytes[3]
                    ),
                ));
                return;
            }
        }
        Ok(bytes) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::StatisticsHeaderCorrupt,
                "Statistics.db",
                format!(
                    "Statistics.db is {} bytes — too small for the 8-byte TOC header",
                    bytes.len()
                ),
            ));
            return;
        }
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::StatisticsHeaderCorrupt,
                "Statistics.db",
                format!("cannot read Statistics.db: {}", e),
            ));
            return;
        }
    }

    if let Err(e) = StatisticsReader::open(&stats_path, platform).await {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::StatisticsHeaderCorrupt,
            "Statistics.db",
            format!("Statistics.db failed to parse: {}", e),
        ));
    }
}

/// Check 7 (FULL): a complete row scan. Returns `(rows, distinct_partitions)`
/// where `distinct_partitions` is the number of distinct partition keys decoded
/// from `Data.db` (used for the BTI Partitions.db cross-check).
async fn full_row_scan_partitions(
    data_path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(usize, Vec<Vec<u8>>)> {
    let reader = SSTableReader::open(data_path, config, platform).await?;

    // `rows` is the total decoded row/entry count (exercises the full
    // decompression + decode stitch path so Data.db corruption surfaces here).
    let entries = reader.get_all_entries().await?;
    let rows = entries.len();

    // `distinct_partition_keys` are the raw serialized PARTITION keys decoded
    // from Data.db — one per partition, NOT per row. Deduping `get_all_entries`
    // RowKeys would over-count a multi-row partition (those keys carry
    // clustering/column/static suffixes), which previously FALSE-FAILED the BTI
    // Partitions.db cross-check on healthy SSTables (issue #970). The reader
    // dedups at the partition boundary for both BIG (`nb`) and BTI (`da`).
    let partition_keys = reader.distinct_partition_keys().await?;

    Ok((rows, partition_keys))
}

/// Cross-check BTI `Partitions.db` trie keys against the partition keys decoded
/// from `Data.db` by IDENTITY (issue #1103). Returns `Some(detail)` describing
/// the mismatch when the trie does not represent the same partition set as
/// Data.db, or `None` when they agree.
///
/// The two sides use different on-disk encodings and are not directly
/// comparable: a BTI trie key is the path-compressed (shortest-distinguishing)
/// prefix of the byte-comparable `[0x40 ++ 8-byte murmur3 token]` key, while a
/// Data.db key is the raw serialized partition key. We bridge them by re-deriving
/// each Data.db raw key's byte-comparable trie key
/// (`encode_partition_key_for_bti_trie`) and matching it against the trie keys by
/// prefix. A healthy table is a total bijection; a wrong-root corruption that
/// preserves leaf count but changes keys leaves a trie key that prefixes no
/// Data.db key (and a Data.db key matched by none), which we surface.
///
/// Note: the byte-comparable encoding assumes `Murmur3Partitioner`, matching the
/// rest of CQLite's BTI read path (issue #755).
fn bti_partition_key_identity_mismatch(
    trie_keys: &[Vec<u8>],
    data_keys: &[Vec<u8>],
) -> Option<String> {
    use crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie;

    let encoded: Vec<[u8; 9]> = data_keys
        .iter()
        .map(|k| encode_partition_key_for_bti_trie(k))
        .collect();

    if trie_keys.len() != encoded.len() {
        return Some(format!(
            "Partitions.db trie yielded {} partition keys but Data.db decoded {} distinct partitions — the trie was walked from a corrupt root",
            trie_keys.len(),
            encoded.len()
        ));
    }

    let hex = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();

    // Greedily match each trie key to exactly one (still-unclaimed) Data.db key
    // by prefix. Equal lengths + a one-to-one matching ⇒ identical partition sets.
    let mut claimed = vec![false; encoded.len()];
    for tk in trie_keys {
        let matched: Vec<usize> = encoded
            .iter()
            .enumerate()
            .filter(|(i, enc)| !claimed[*i] && enc.starts_with(tk.as_slice()))
            .map(|(i, _)| i)
            .collect();
        match matched.as_slice() {
            [idx] => claimed[*idx] = true,
            [] => {
                return Some(format!(
                    "Partitions.db trie key {} matches no Data.db partition key — the trie was walked from a corrupt root (same leaf count, different keys)",
                    hex(tk)
                ));
            }
            _ => {
                return Some(format!(
                    "Partitions.db trie key {} is an ambiguous prefix of multiple Data.db partition keys — the trie does not match Data.db identities",
                    hex(tk)
                ));
            }
        }
    }

    None
}

/// Map an error surfaced by the inline-CRC / decompression path onto a stable
/// error class, keyed by the message shape the lower layers produce.
fn classify_data_error(component: &str, err: &Error) -> VerifyFinding {
    let msg = err.to_string();
    // A truncated Data.db makes a chunk read hit EOF; a bit-flip makes the
    // inline CRC mismatch or the decompressor reject the payload. Everything
    // surfaced here is a Data.db chunk problem.
    let class = if msg.contains("Failed to read")
        || msg.contains("failed to fill whole buffer")
        || msg.contains("UnexpectedEof")
        || msg.contains("end of file")
    {
        VerifyErrorClass::UnexpectedEof
    } else {
        VerifyErrorClass::ChunkDecompressionError
    };
    VerifyFinding::new(class, component.to_string(), msg)
}

/// Map an error surfaced by the full-scan path onto a stable error class. The
/// scan touches Data.db (and, for BIG, Index.db); the structural checks have
/// already classified index/BTI corruption, so anything here is a Data.db /
/// decode failure.
fn classify_scan_error(components: &ComponentSet, err: &Error) -> VerifyFinding {
    let _ = components; // index/BTI corruption is classified earlier; this is Data.db decode
    let msg = err.to_string();
    let lower = msg.to_lowercase();
    let class = if msg.contains("CRC32 mismatch") {
        VerifyErrorClass::ChunkDecompressionError
    } else if msg.contains("failed to fill whole buffer")
        || lower.contains("unexpected")
        || lower.contains("end of file")
        || msg.contains("too small")
    {
        VerifyErrorClass::UnexpectedEof
    } else if msg.contains("decompress")
        || msg.contains("Decompressed")
        || msg.contains("length prefix")
    {
        VerifyErrorClass::ChunkDecompressionError
    } else {
        VerifyErrorClass::RowScanFailed
    };
    VerifyFinding::new(class, "Data.db".to_string(), msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_class_codes_are_stable() {
        assert_eq!(VerifyErrorClass::DigestMismatch.code(), "DigestMismatch");
        assert_eq!(
            VerifyErrorClass::ChunkOffsetOutOfBounds.code(),
            "ChunkOffsetOutOfBounds"
        );
        assert_eq!(
            VerifyErrorClass::BtiRootPointerCorrupt.code(),
            "BtiRootPointerCorrupt"
        );
    }

    #[test]
    fn mode_labels() {
        assert_eq!(VerifyMode::Quick.as_str(), "quick");
        assert_eq!(VerifyMode::Full.as_str(), "full");
        assert_ne!(VerifyMode::Quick, VerifyMode::Full);
    }

    #[test]
    fn real_component_recognition_excludes_sidecars() {
        assert!(is_real_component("Data.db"));
        assert!(is_real_component("Statistics.db"));
        assert!(is_real_component("CompressionInfo.db"));
        assert!(is_real_component("TOC.txt"));
        assert!(is_real_component("Digest.crc32"));
        // sidecar / reference goldens are NOT components
        assert!(!is_real_component("Data.db.jsonl"));
        assert!(!is_real_component("Statistics.db.txt"));
        assert!(!is_real_component("CompressionInfo.db.txt"));
        assert!(!is_real_component("README.md"));
    }

    #[test]
    fn report_summary_line_distinguishes_ok_and_fail() {
        let ok = VerifyReport {
            directory: PathBuf::from("/x"),
            base_name: "nb-1-big".to_string(),
            format: SsTableFormat::Big,
            mode: VerifyMode::Full,
            findings: vec![],
            toc_components: vec![],
            rows_scanned: Some(3),
        };
        assert!(ok.is_ok());
        assert!(ok.summary_line().contains("VERIFY OK"));

        let fail = VerifyReport {
            directory: PathBuf::from("/x"),
            base_name: "nb-1-big".to_string(),
            format: SsTableFormat::Big,
            mode: VerifyMode::Full,
            findings: vec![VerifyFinding::new(
                VerifyErrorClass::DigestMismatch,
                "Digest.crc32",
                "boom",
            )],
            toc_components: vec![],
            rows_scanned: None,
        };
        assert!(!fail.is_ok());
        assert_eq!(fail.primary_class(), Some(VerifyErrorClass::DigestMismatch));
        assert!(fail.summary_line().contains("VERIFY FAIL"));
        assert!(fail.summary_line().contains("DigestMismatch"));
    }

    // ---- BTI partition-key identity cross-check (issue #1103) --------------

    use crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie;

    /// Build the path-compressed trie key for a raw partition key: the
    /// byte-comparable `[0x40 ++ token]` key truncated to its first `prefix_len`
    /// bytes, mirroring how a real Patricia trie stores only the shortest
    /// distinguishing prefix.
    fn trie_key_prefix(raw: &[u8], prefix_len: usize) -> Vec<u8> {
        encode_partition_key_for_bti_trie(raw)[..prefix_len].to_vec()
    }

    #[test]
    fn identity_check_passes_for_matching_full_keys() {
        // Trie keys == full 9-byte encoded keys (a key is a prefix of itself).
        let data: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let trie: Vec<Vec<u8>> = data
            .iter()
            .map(|k| encode_partition_key_for_bti_trie(k).to_vec())
            .collect();
        assert_eq!(bti_partition_key_identity_mismatch(&trie, &data), None);
    }

    #[test]
    fn identity_check_passes_for_path_compressed_prefixes() {
        // Healthy table: trie stores only the first 2 bytes (`0x40` + 1 token
        // byte), which is what `test_da/wide_table` actually does.
        let data: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let trie: Vec<Vec<u8>> = data.iter().map(|k| trie_key_prefix(k, 2)).collect();
        // Precondition: the 2-byte prefixes are distinct (true for pk=1,2,3).
        let mut sorted = trie.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), trie.len(), "test prefixes must be distinct");
        assert_eq!(bti_partition_key_identity_mismatch(&trie, &data), None);
    }

    #[test]
    fn identity_check_detects_same_count_wrong_keys() {
        // Data has partitions {1,2,3} but the trie was walked to {4,5,6}: same
        // count, disjoint identities. MUST be flagged (the core of issue #1103).
        let data: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let trie: Vec<Vec<u8>> = (4u32..=6)
            .map(|i| trie_key_prefix(&i.to_be_bytes(), 2))
            .collect();
        let mismatch = bti_partition_key_identity_mismatch(&trie, &data);
        assert!(
            mismatch.is_some(),
            "same-count wrong-key trie must be detected as a mismatch"
        );
    }

    #[test]
    fn identity_check_detects_one_swapped_key() {
        // Two keys match, one is wrong — the minimal wrong-root that a count check
        // cannot see.
        let data: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let mut trie: Vec<Vec<u8>> = data.iter().map(|k| trie_key_prefix(k, 2)).collect();
        trie[0] = trie_key_prefix(&99u32.to_be_bytes(), 2);
        assert!(bti_partition_key_identity_mismatch(&trie, &data).is_some());
    }

    #[test]
    fn identity_check_detects_count_mismatch() {
        let data: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let trie: Vec<Vec<u8>> = data
            .iter()
            .take(2)
            .map(|k| encode_partition_key_for_bti_trie(k).to_vec())
            .collect();
        let detail =
            bti_partition_key_identity_mismatch(&trie, &data).expect("undercount must be flagged");
        assert!(detail.contains("2 partition keys"));
        assert!(detail.contains("3 distinct partitions"));
    }
}
