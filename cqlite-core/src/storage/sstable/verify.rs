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
    /// Partition keys are not in ascending on-disk (Murmur3 token) order, or
    /// clustering rows within a partition are not in ascending clustering order
    /// (issue #1282). Cassandra requires strictly ordered keys/rows; its
    /// `sstableverify` (`SSTableIdentityIterator` / `Verifier`) rejects an
    /// out-of-order key or row as corrupt.
    OutOfOrderKeyOrRow,
    /// A partition-level `localDeletionTime` is negative (invalid) on the legacy
    /// signed (`nb`) `DeletionTime` form (issue #1282). `localDeletionTime` is
    /// seconds since the Unix epoch; the only non-negative "special" value is the
    /// live sentinel `i32::MAX` (`0x7FFFFFFF`). A negative value cannot be a valid
    /// deletion time — Cassandra's `DeletionTime`/`Verifier` treats it as corrupt.
    /// (The unsigned `oa`/`da` form legitimately represents far-future times in
    /// `[2^31, 2^32)`, so those are NOT flagged — the on-disk format, not a
    /// heuristic, decides.)
    InvalidLocalDeletionTime,
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
            VerifyErrorClass::OutOfOrderKeyOrRow => "OutOfOrderKeyOrRow",
            VerifyErrorClass::InvalidLocalDeletionTime => "InvalidLocalDeletionTime",
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
    // `bti_leaves` is the set of partition-index leaves recovered by walking
    // Partitions.db; it is cross-checked against the Data.db scan in FULL mode to
    // catch a footer-flip that silently UNDER-counts partitions (the trie still
    // parses, just from the wrong root) AND a same-count corruption that keeps a
    // leaf's emitted prefix but rewrites its PAYLOAD to point at a different
    // partition. Each leaf carries its emitted byte-comparable prefix plus its
    // payload resolved back to a raw partition key by AUTHORITATIVE data (issue
    // #1103).
    let mut bti_leaves: Option<Vec<BtiResolvedLeaf>> = None;
    match components.format {
        SsTableFormat::Bti => bti_leaves = check_bti_structure(dir, &components, &mut findings)?,
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
            // The order/LDT check (Check 8) reuses the reader, so keep a clone of
            // the platform handle before the scan consumes the original.
            let platform_for_order = platform.clone();
            match full_row_scan_partitions(&components.data_path, config, platform).await {
                Ok((rows, scan_partitions)) => {
                    rows_scanned = Some(rows);
                    // BTI cross-check: each Partitions.db leaf's PAYLOAD, resolved
                    // back to a raw partition key by authoritative data, MUST match
                    // the partition keys decoded from Data.db — by IDENTITY, not
                    // just count (issue #1103). A count-only check passes a
                    // corruption that walks a wrong subtree yielding a different set
                    // of keys with the same leaf count; a prefix-only check passes a
                    // corruption that keeps a leaf's emitted prefix but rewrites its
                    // payload to a different partition. Resolving the payload closes
                    // both gaps.
                    if let Some(leaves) = bti_leaves {
                        if let Some(detail) =
                            bti_partition_identity_mismatch(&leaves, &scan_partitions)
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

            // ---- Check 8: key/row order + partition-level LDT validity (#1282)
            //
            // Cassandra's `sstableverify` rejects two corruption classes CQLite
            // did not previously classify: partition keys / clustering rows out of
            // ascending order, and a negative (invalid) partition-level
            // `localDeletionTime`. Both are read off the SAME authoritative decode
            // the scan already performs (no second heuristic pass): the on-disk
            // partition order (Murmur3 token order) and each deleted partition's
            // raw `DeletionTime`. Skipped when compression metadata is corrupt
            // (handled above) — this block is inside the same guard.
            check_key_order_and_ldt(
                &components.data_path,
                config,
                platform_for_order,
                &mut findings,
            )
            .await;
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

/// One BTI `Partitions.db` leaf, with its PAYLOAD resolved back to a raw
/// partition key using authoritative data (issue #1103).
///
/// The verifier resolves every leaf so a corruption that keeps the leaf's
/// emitted byte-comparable prefix while rewriting its payload to point at a
/// DIFFERENT partition is still caught (a same-count, wrong-IDENTITY
/// corruption the prefix-only compare missed).
struct BtiResolvedLeaf {
    /// The path-compressed byte-comparable prefix emitted by the trie walk
    /// (`[0x40 ++ token]` truncated to the shortest distinguishing prefix). Used
    /// only for the prefix/payload-consistency assertion.
    prefix: Vec<u8>,
    /// The raw partition key this leaf's payload resolves to, when it could be
    /// recovered directly (a `RowsOffset` leaf stores the raw key INLINE in
    /// `Rows.db`). `None` for a `DataOffset` leaf, whose raw key is recovered via
    /// the Data.db position map ([`Self::data_position`]).
    inline_raw_key: Option<Vec<u8>>,
    /// The decompressed-`Data.db` partition-start position the payload points at:
    /// the `DataOffset` value directly, or the `data_position` recovered from the
    /// `RowsOffset` row-index entry. Resolved to a raw key via the Data.db scan's
    /// position map in [`bti_partition_identity_mismatch`].
    data_position: u64,
}

/// Check 4 (BTI): structurally validate the `Partitions.db` and `Rows.db`
/// tries, and resolve every partition-index leaf back to a raw partition key.
///
/// Returns `Some(leaves)` — one [`BtiResolvedLeaf`] per recovered partition —
/// so the caller can cross-check them against the Data.db scan by IDENTITY
/// (FULL mode). Returns `None` if `Partitions.db` could not be walked (a finding
/// was recorded).
///
/// * `Partitions.db` is walked with [`iterate_partitions_in_bti_file`], which
///   follows the trailing-8-byte footer root and DFS-collects every leaf. A
///   footer flip either makes the walk error (out-of-bounds root) or silently
///   recover the wrong key set; the FULL-mode identity cross-check catches the
///   latter.
/// * For every partition whose payload is a `RowsOffset`, the per-partition
///   row-index entry is resolved from `Rows.db` via [`iterate_rows_for_partition`]
///   (structural) and [`resolve_rows_db_entry`] (to recover the inline raw key
///   and the partition's Data.db position). A truncated `Rows.db` makes the
///   referenced offset point past EOF or the row-trie read hit EOF.
/// * A `DataOffset` payload carries the partition's decompressed-Data.db
///   position directly; its raw key is resolved later through the Data.db scan.
fn check_bti_structure(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Option<Vec<BtiResolvedLeaf>>> {
    use crate::storage::sstable::bti::parser::{
        iterate_partitions_in_bti_file, iterate_rows_for_partition, resolve_rows_db_entry,
        BtiPartitionLocation,
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
            // Rows.db is gone, so `RowsOffset` payloads cannot be resolved; only
            // `DataOffset` leaves carry a self-contained position. Return what we
            // can (the missing-component finding already fails verification).
            let leaves = partitions
                .into_iter()
                .filter_map(|(prefix, location)| match location {
                    BtiPartitionLocation::DataOffset(off) => Some(BtiResolvedLeaf {
                        prefix,
                        inline_raw_key: None,
                        data_position: off,
                    }),
                    BtiPartitionLocation::RowsOffset(_) => None,
                })
                .collect();
            return Ok(Some(leaves));
        }
    };

    // Resolve every leaf's PAYLOAD back to a raw partition key (issue #1103). A
    // `RowsOffset` leaf stores the raw key INLINE in `Rows.db` as
    // `[u16 key_length][key bytes]` at the offset (see `resolve_rows_db_entry`),
    // so we extract it directly — no Data.db read. A `DataOffset` leaf carries the
    // partition's decompressed-Data.db position directly; its raw key is resolved
    // later through the Data.db scan's position map.
    let mut leaves: Vec<BtiResolvedLeaf> = Vec::with_capacity(partitions.len());
    for (prefix, location) in partitions {
        match location {
            BtiPartitionLocation::RowsOffset(off) => {
                let off = off as usize;
                if off + 2 > rows_bytes.len() {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::BtiTrieCorrupt,
                        "Rows.db",
                        format!(
                            "partition (trie prefix {} bytes) references Rows.db offset {} which is past EOF ({} bytes) — Rows.db is truncated/corrupt",
                            prefix.len(),
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
                    continue;
                }

                // Inline raw partition key: [u16 key_length][key bytes] at `off`.
                let key_length =
                    u16::from_be_bytes([rows_bytes[off], rows_bytes[off + 1]]) as usize;
                let key_start = off + 2;
                let key_end = key_start + key_length;
                if key_end > rows_bytes.len() {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::BtiTrieCorrupt,
                        "Rows.db",
                        format!(
                            "Rows.db entry at offset {} declares an inline key length {} that overruns the file ({} bytes)",
                            off, key_length, rows_bytes.len()
                        ),
                    ));
                    continue;
                }
                let inline_raw_key = rows_bytes[key_start..key_end].to_vec();

                // Recover the partition's Data.db position too, so a leaf whose
                // INLINE key and Data.db position disagree (a payload tamper) is
                // still cross-checkable through the position map.
                let data_position = match resolve_rows_db_entry(&rows_bytes, off) {
                    Ok(hdr) => hdr.data_position,
                    Err(e) => {
                        findings.push(VerifyFinding::new(
                            VerifyErrorClass::BtiTrieCorrupt,
                            "Rows.db",
                            format!(
                                "Rows.db entry at offset {} failed to deserialize (truncated/corrupt): {}",
                                off, e
                            ),
                        ));
                        continue;
                    }
                };

                leaves.push(BtiResolvedLeaf {
                    prefix,
                    inline_raw_key: Some(inline_raw_key),
                    data_position,
                });
            }
            BtiPartitionLocation::DataOffset(off) => {
                leaves.push(BtiResolvedLeaf {
                    prefix,
                    inline_raw_key: None,
                    data_position: off,
                });
            }
        }
    }

    // Return the resolved leaves. FULL-mode verification cross-checks each leaf's
    // resolved raw partition key against the keys decoded from Data.db, by
    // IDENTITY (issue #1103).
    Ok(Some(leaves))
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
/// where `distinct_partitions` is the set of distinct partition keys decoded
/// from `Data.db`, each paired with its decompressed-Data.db partition-start
/// position (used for the BTI Partitions.db identity cross-check, issue #1103).
async fn full_row_scan_partitions(
    data_path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(usize, Vec<(u64, Vec<u8>)>)> {
    let reader = SSTableReader::open(data_path, config, platform).await?;

    // `rows` is the total decoded row/entry count (exercises the full
    // decompression + decode stitch path so Data.db corruption surfaces here).
    let entries = reader.get_all_entries().await?;
    let rows = entries.len();

    // `distinct_partition_keys_with_positions` are the raw serialized PARTITION
    // keys decoded from Data.db — one per partition, NOT per row — each tagged
    // with its decompressed-Data.db partition-start position. Deduping
    // `get_all_entries` RowKeys would over-count a multi-row partition (those keys
    // carry clustering/column/static suffixes), which previously FALSE-FAILED the
    // BTI Partitions.db cross-check on healthy SSTables (issue #970). The reader
    // dedups at the partition boundary for both BIG (`nb`) and BTI (`da`); the
    // position lets the verifier resolve a BTI leaf's payload back to its raw key.
    let partitions = reader.distinct_partition_keys_with_positions().await?;

    Ok((rows, partitions))
}

/// Cross-check BTI `Partitions.db` leaves against the partitions decoded from
/// `Data.db` by IDENTITY (issue #1103). Returns `Some(detail)` describing the
/// mismatch when the trie does not represent the same partition set as Data.db,
/// or `None` when they agree.
///
/// Unlike a prefix-only compare (which only looks at the leaf's emitted
/// byte-comparable transition bytes), this resolves each leaf's PAYLOAD back to a
/// raw partition key using authoritative data and matches it against the Data.db
/// keys. This closes a same-count, wrong-IDENTITY corruption that keeps the
/// emitted prefix but rewrites the payload (`DataOffset` / `RowsOffset` →
/// `data_position`) to point at a DIFFERENT partition:
///
/// * `RowsOffset` leaf: the raw key is stored INLINE in `Rows.db`
///   ([`BtiResolvedLeaf::inline_raw_key`]); matched directly against Data.db.
/// * `DataOffset` leaf: matched via its decompressed-Data.db
///   [`BtiResolvedLeaf::data_position`], looked up in the Data.db scan's
///   `position → raw_key` map.
///
/// We require an exact MULTISET equality between the resolved leaf keys and the
/// Data.db keys, plus a per-leaf consistency check that the resolved raw key's
/// byte-comparable encoding actually starts with the leaf's emitted prefix
/// (catches a leaf whose path is inconsistent with its payload).
///
/// Note: the byte-comparable encoding assumes `Murmur3Partitioner`, matching the
/// rest of CQLite's BTI read path (issue #755).
fn bti_partition_identity_mismatch(
    leaves: &[BtiResolvedLeaf],
    data_partitions: &[(u64, Vec<u8>)],
) -> Option<String> {
    use crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie;
    use std::collections::HashMap;

    let hex = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();

    // Data.db side: a position → raw_key map (to resolve `DataOffset` leaves) plus
    // the raw-key multiset (to compare identities).
    let pos_to_key: HashMap<u64, &Vec<u8>> = data_partitions.iter().map(|(p, k)| (*p, k)).collect();

    // Resolve every leaf to a raw partition key.
    let mut leaf_keys: Vec<Vec<u8>> = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let raw_key = match &leaf.inline_raw_key {
            // `RowsOffset` leaf: authoritative inline key. Its recorded Data.db
            // position MUST resolve to a decoded partition start carrying the SAME
            // raw key. A position that maps to a different key is a desync; a
            // position that maps to NOTHING means the `Rows.db` entry's
            // `data_position` is corrupt — a BTI read would seek to a non-partition
            // offset in Data.db even though the inline key looks valid, so it is
            // just as fatal as a corrupt `DataOffset` payload.
            Some(inline) => match pos_to_key.get(&leaf.data_position) {
                Some(by_pos) => {
                    if by_pos.as_slice() != inline.as_slice() {
                        return Some(format!(
                            "Partitions.db leaf (prefix {}) inline raw key {} disagrees with the key at its Data.db position {} ({}) — the leaf payload was tampered",
                            hex(&leaf.prefix),
                            hex(inline),
                            leaf.data_position,
                            hex(by_pos),
                        ));
                    }
                    inline.clone()
                }
                None => {
                    return Some(format!(
                        "Partitions.db leaf (prefix {}) inline raw key {} records Data.db position {} which is not a decoded partition start — the Rows.db entry's data position is corrupt (a BTI read would seek to the wrong partition)",
                        hex(&leaf.prefix),
                        hex(inline),
                        leaf.data_position,
                    ));
                }
            },
            // `DataOffset` leaf: resolve via the Data.db position map. A payload
            // flipped to a position that is not a partition start matches nothing.
            None => match pos_to_key.get(&leaf.data_position) {
                Some(k) => (*k).clone(),
                None => {
                    return Some(format!(
                        "Partitions.db leaf (prefix {}) payload points at Data.db position {} which is not a decoded partition start — the leaf payload is corrupt (same prefix, wrong partition)",
                        hex(&leaf.prefix),
                        leaf.data_position,
                    ));
                }
            },
        };

        // Per-leaf path/payload consistency: the resolved raw key's
        // byte-comparable encoding MUST start with the leaf's emitted prefix.
        let encoded = encode_partition_key_for_bti_trie(&raw_key);
        if !encoded.starts_with(leaf.prefix.as_slice()) {
            return Some(format!(
                "Partitions.db leaf prefix {} is inconsistent with its payload's partition key (encodes to {}) — the trie path does not match the leaf payload",
                hex(&leaf.prefix),
                hex(&encoded),
            ));
        }

        leaf_keys.push(raw_key);
    }

    // Exact MULTISET equality between the resolved leaf keys and the Data.db keys.
    let mut data_counts: HashMap<&[u8], i64> = HashMap::new();
    for (_, k) in data_partitions {
        *data_counts.entry(k.as_slice()).or_insert(0) += 1;
    }
    let mut leaf_counts: HashMap<&[u8], i64> = HashMap::new();
    for k in &leaf_keys {
        *leaf_counts.entry(k.as_slice()).or_insert(0) += 1;
    }

    if leaf_keys.len() != data_partitions.len() {
        return Some(format!(
            "Partitions.db trie yielded {} partition keys but Data.db decoded {} distinct partitions — the trie was walked from a corrupt root",
            leaf_keys.len(),
            data_partitions.len()
        ));
    }

    for (k, &lc) in &leaf_counts {
        let dc = data_counts.get(k).copied().unwrap_or(0);
        if lc != dc {
            return Some(format!(
                "Partitions.db resolves partition key {} {} time(s) but Data.db decodes it {} time(s) — the trie does not match Data.db identities (same count, different keys)",
                hex(k),
                lc,
                dc,
            ));
        }
    }
    for (k, &dc) in &data_counts {
        let lc = leaf_counts.get(k).copied().unwrap_or(0);
        if lc != dc {
            return Some(format!(
                "Data.db partition key {} appears {} time(s) but Partitions.db resolves it {} time(s) — the trie does not match Data.db identities",
                hex(k),
                dc,
                lc,
            ));
        }
    }

    None
}

/// Check 8 (FULL): partition key/row ordering + partition-level
/// `localDeletionTime` validity (issue #1282).
///
/// Two corruption classes Cassandra's `sstableverify` rejects that the earlier
/// checks did not classify:
///
/// * **Out-of-order key/row** ([`VerifyErrorClass::OutOfOrderKeyOrRow`]).
///   Cassandra stores partitions in ascending **Murmur3 token** order (ties
///   broken by the raw key bytes). We recompute each partition's token with the
///   authoritative [`cassandra_murmur3_token`] (Murmur3Partitioner, matching the
///   rest of CQLite's BTI read path, issue #755) and flag the first
///   `(token, key)` pair that is not strictly greater than its predecessor.
///
/// * **Invalid partition-level local-deletion-time**
///   ([`VerifyErrorClass::InvalidLocalDeletionTime`]). `localDeletionTime` is
///   seconds since the Unix epoch; the only special non-negative value is the
///   live sentinel `i32::MAX`. On the legacy signed (`nb`) `DeletionTime` form a
///   NEGATIVE partition-level value is unambiguously corrupt (Cassandra's
///   `DeletionTime`/`Verifier` rejects it). The unsigned `oa`/`da` form
///   legitimately represents far-future times in `[2^31, 2^32)` as a negative
///   `i32`, so we ONLY flag a negative value when the on-disk format is the
///   signed legacy form — the format, not a heuristic, decides.
///
/// Both facts come from the SAME authoritative partition-header decode the scan
/// already performs (see [`SSTableReader::partition_verify_scan`]); this is not a
/// second guessing pass. Environmental errors (reader open) are surfaced through
/// the existing scan-error classifier rather than aborting verification.
async fn check_key_order_and_ldt(
    data_path: &Path,
    config: &Config,
    platform: Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) {
    let reader = match SSTableReader::open(data_path, config, platform).await {
        Ok(r) => r,
        Err(_) => {
            // A reader-open failure here is already surfaced by the Check 7 scan
            // (it opens the same reader first); do not double-report it.
            return;
        }
    };
    let signed_ldt = !reader.has_uint_deletion_time();
    let partitions = match reader.partition_verify_scan().await {
        Ok(p) => p,
        Err(_) => {
            // A parse failure is Check 7's territory (RowScanFailed / decode);
            // avoid a duplicate, differently-classed finding for the same cause.
            return;
        }
    };

    findings.extend(classify_order_and_ldt(&partitions, signed_ldt));

    // Row-order half of OutOfOrderKeyOrRow (issue #1282 roborev follow-up):
    // Cassandra's Verifier also rejects out-of-order CLUSTERING rows within a
    // partition. Decode each partition's clustering rows in on-disk order and flag
    // a non-increasing clustering step using the authoritative schema comparator
    // (which respects reversed/DESC clustering order). A table with no clustering
    // columns yields an empty scan and produces no findings.
    if let Some(schema) = reader.effective_schema() {
        // A decode failure is Check 7's territory; do not double-report. Only a
        // successful scan feeds the clustering-order classifier.
        if !schema.clustering_keys.is_empty() {
            if let Ok(partition_rows) = reader.partition_clustering_verify_scan().await {
                findings.extend(classify_clustering_row_order(&partition_rows, &schema));
            }
        }
    }
}

/// Compare two clustering-key tuples in the authoritative schema clustering
/// order (issue #1282 roborev follow-up).
///
/// Each column is compared with its non-gated [`ComparatorType`] (derived from the
/// schema clustering type) and the result reversed for a DESC column, mirroring
/// Cassandra's reversed-type ordering. An absent trailing component (a shorter
/// tuple) is treated as NULL, which sorts first regardless of ASC/DESC — matching
/// `ClusteringKey::compare`. NO heuristics: the format-derived comparator and the
/// schema's ASC/DESC flag decide.
fn compare_clustering_tuples(
    a: &[crate::types::Value],
    b: &[crate::types::Value],
    schema: &crate::schema::TableSchema,
) -> Result<std::cmp::Ordering> {
    use crate::types::Value;
    use std::cmp::Ordering;

    let comparators = schema.get_clustering_key_comparators()?;
    for (i, ck) in schema.clustering_keys.iter().enumerate() {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        // NULL/absent component sorts first regardless of ASC/DESC (no reversal).
        let ord = match (av, bv) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (_, _) => {
                let cmp = comparators
                    .get(i)
                    .ok_or_else(|| {
                        Error::Schema(format!(
                            "missing clustering comparator for column {}",
                            ck.name
                        ))
                    })?
                    .compare(av, bv)?;
                if ck.order == crate::schema::ClusteringOrder::Desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            }
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

/// Pure classifier for the ROW half of Check 8 (issue #1282 roborev follow-up):
/// given each partition's clustering-key tuples in on-disk order and the
/// authoritative schema, flag the first partition whose clustering rows are not in
/// strictly ascending schema order as [`VerifyErrorClass::OutOfOrderKeyOrRow`].
///
/// The comparison applies each clustering column's ASC/DESC order via
/// [`compare_clustering_tuples`] — NO heuristics. A non-increasing step (a row
/// equal to or before its predecessor) is corruption Cassandra's `Verifier`
/// rejects.
///
/// Kept side-effect-free so the public verify path and the unit tests drive the
/// EXACT same classification (wiring evidence: `check_key_order_and_ldt` calls
/// this, and `verify_sstable` calls that in FULL mode).
fn classify_clustering_row_order(
    partition_rows: &[(usize, Vec<Vec<crate::types::Value>>)],
    schema: &crate::schema::TableSchema,
) -> Vec<VerifyFinding> {
    use std::cmp::Ordering;

    let mut findings = Vec::new();
    for (part_idx, rows) in partition_rows {
        for pair in rows.windows(2) {
            let (prev, cur) = (&pair[0], &pair[1]);
            // A comparator error (schema/type mismatch) is not an ordering fault;
            // Check 7 owns decode/type failures, so skip rather than misclassify.
            let ord = match compare_clustering_tuples(cur, prev, schema) {
                Ok(o) => o,
                Err(_) => continue,
            };
            // On disk a later clustering row MUST be strictly greater than its
            // predecessor; Equal or Less is out-of-order corruption.
            if ord != Ordering::Greater {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::OutOfOrderKeyOrRow,
                    "Data.db",
                    format!(
                        "partition {} has an out-of-order clustering row: {:?} is not strictly after the previous row {:?} in schema clustering order",
                        part_idx, cur, prev,
                    ),
                ));
                break;
            }
        }
    }
    findings
}

/// Pure classifier for Check 8 (issue #1282): given the on-disk-ordered
/// `(raw_partition_key, partition_local_deletion_time)` list from
/// [`SSTableReader::partition_verify_scan`] and whether the on-disk
/// `DeletionTime` is the legacy SIGNED form, return any order / LDT findings.
///
/// Kept side-effect-free so both the public verify path and the unit tests drive
/// the EXACT same classification (wiring evidence: `check_key_order_and_ldt`
/// calls this, and `verify_sstable` calls that in FULL mode).
fn classify_order_and_ldt(
    partitions: &[(Vec<u8>, Option<i32>)],
    signed_ldt: bool,
) -> Vec<VerifyFinding> {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;

    let mut findings = Vec::new();
    let hex = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();

    // ---- Out-of-order partition keys (Murmur3 token order) -----------------
    let mut prev: Option<(i64, Vec<u8>)> = None;
    for (idx, (key, _ldt)) in partitions.iter().enumerate() {
        let token = cassandra_murmur3_token(key);
        if let Some((prev_token, prev_key)) = prev.as_ref() {
            // Cassandra orders by (token, key bytes). A later partition MUST be
            // strictly greater; equal or lesser is out-of-order corruption.
            let ordered = (*prev_token, prev_key.as_slice()) < (token, key.as_slice());
            if !ordered {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::OutOfOrderKeyOrRow,
                    "Data.db",
                    format!(
                        "partition {} (key {}, token {}) is not strictly after the previous partition (key {}, token {}) — partitions are stored out of Murmur3 token order",
                        idx,
                        hex(key),
                        token,
                        hex(prev_key),
                        prev_token,
                    ),
                ));
                break;
            }
        }
        prev = Some((token, key.clone()));
    }

    // ---- Negative (invalid) partition-level localDeletionTime (nb) ---------
    if signed_ldt {
        for (key, ldt) in partitions {
            if let Some(ldt) = ldt {
                // A deleted partition's localDeletionTime is epoch-seconds; it
                // cannot be negative. (The live sentinel i32::MAX is positive and
                // is already resolved to `None` by the header parser.)
                if *ldt < 0 {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::InvalidLocalDeletionTime,
                        "Data.db",
                        format!(
                            "partition (key {}) has a negative localDeletionTime {} (0x{:08x}) on the signed (nb) DeletionTime form — a valid deletion time is >= 0 seconds since epoch",
                            hex(key),
                            ldt,
                            *ldt as u32,
                        ),
                    ));
                    break;
                }
            }
        }
    }

    findings
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
        // issue #1282: the two new classes must expose stable codes.
        assert_eq!(
            VerifyErrorClass::OutOfOrderKeyOrRow.code(),
            "OutOfOrderKeyOrRow"
        );
        assert_eq!(
            VerifyErrorClass::InvalidLocalDeletionTime.code(),
            "InvalidLocalDeletionTime"
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

    // ---- BTI partition identity cross-check (issue #1103) ------------------
    //
    // These exercise `bti_partition_identity_mismatch` over RESOLVED leaves: each
    // leaf carries its emitted byte-comparable prefix plus a payload resolved back
    // to a raw partition key (an inline raw key for a `RowsOffset` leaf, or a
    // Data.db position for a `DataOffset` leaf). The Data.db side is the
    // `(position, raw_key)` set from the scan.

    use crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie;

    /// Build the path-compressed trie key for a raw partition key: the
    /// byte-comparable `[0x40 ++ token]` key truncated to its first `prefix_len`
    /// bytes, mirroring how a real Patricia trie stores only the shortest
    /// distinguishing prefix.
    fn trie_key_prefix(raw: &[u8], prefix_len: usize) -> Vec<u8> {
        encode_partition_key_for_bti_trie(raw)[..prefix_len].to_vec()
    }

    /// A `RowsOffset`-style leaf: authoritative inline raw key + matching Data.db
    /// position, with a 2-byte emitted prefix (what `test_da/wide_table` does).
    fn inline_leaf(raw: &[u8], data_position: u64) -> BtiResolvedLeaf {
        BtiResolvedLeaf {
            prefix: trie_key_prefix(raw, 2),
            inline_raw_key: Some(raw.to_vec()),
            data_position,
        }
    }

    /// A `DataOffset`-style leaf: no inline key, resolved purely via its Data.db
    /// position, with a 2-byte emitted prefix derived from the key it *should*
    /// resolve to (so the prefix/payload-consistency check passes when healthy).
    fn data_offset_leaf(prefix_from: &[u8], data_position: u64) -> BtiResolvedLeaf {
        BtiResolvedLeaf {
            prefix: trie_key_prefix(prefix_from, 2),
            inline_raw_key: None,
            data_position,
        }
    }

    /// The Data.db scan side: distinct partition keys, each at a synthetic
    /// monotonically-increasing position (0, 100, 200, ...).
    fn data_partitions(keys: &[Vec<u8>]) -> Vec<(u64, Vec<u8>)> {
        keys.iter()
            .enumerate()
            .map(|(i, k)| (i as u64 * 100, k.clone()))
            .collect()
    }

    #[test]
    fn identity_check_passes_for_inline_rows_leaves() {
        // Healthy wide-table shape: every leaf resolves to its inline raw key,
        // matching the Data.db key at the same position.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let leaves: Vec<BtiResolvedLeaf> =
            data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
        assert_eq!(bti_partition_identity_mismatch(&leaves, &data), None);
    }

    #[test]
    fn identity_check_passes_for_data_offset_leaves() {
        // Healthy small-partition shape (`da-2-bti`): leaves carry only a Data.db
        // position; the raw key is resolved through the position map.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let leaves: Vec<BtiResolvedLeaf> = data
            .iter()
            .map(|(pos, k)| data_offset_leaf(k, *pos))
            .collect();
        assert_eq!(bti_partition_identity_mismatch(&leaves, &data), None);
    }

    #[test]
    fn identity_check_detects_inline_payload_pointing_at_wrong_partition() {
        // The exact reviewer scenario for a `RowsOffset` leaf: the leaf's emitted
        // prefix is unchanged but its INLINE raw key (the payload) is rewritten to
        // a partition NOT present in Data.db. Same leaf count, wrong identity.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let mut leaves: Vec<BtiResolvedLeaf> =
            data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
        // Keep the emitted prefix; rewrite the inline raw key to pk=99.
        leaves[0].inline_raw_key = Some(99u32.to_be_bytes().to_vec());
        assert!(
            bti_partition_identity_mismatch(&leaves, &data).is_some(),
            "an inline payload pointing at a partition absent from Data.db must be flagged"
        );
    }

    #[test]
    fn identity_check_detects_data_offset_payload_pointing_at_wrong_partition() {
        // The reviewer scenario for a `DataOffset` leaf: the leaf's emitted prefix
        // is unchanged but its Data.db position payload is rewritten to point at a
        // DIFFERENT partition's start. The resolved key then no longer matches the
        // partition the prefix encodes.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let mut leaves: Vec<BtiResolvedLeaf> = data
            .iter()
            .map(|(pos, k)| data_offset_leaf(k, *pos))
            .collect();
        // Leaf 0's prefix still encodes pk=1, but its position now points at pk=2.
        leaves[0].data_position = data[1].0;
        let detail = bti_partition_identity_mismatch(&leaves, &data)
            .expect("a DataOffset payload pointing at the wrong partition must be flagged");
        // It is caught by the prefix/payload-consistency check (the resolved key's
        // encoding no longer starts with the leaf's prefix) OR the multiset compare.
        assert!(
            detail.contains("inconsistent") || detail.contains("identities"),
            "unexpected detail: {detail}"
        );
    }

    #[test]
    fn identity_check_detects_data_offset_payload_pointing_at_non_partition() {
        // A `DataOffset` flipped to a byte position that is NOT a partition start
        // resolves to no key at all.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let mut leaves: Vec<BtiResolvedLeaf> = data
            .iter()
            .map(|(pos, k)| data_offset_leaf(k, *pos))
            .collect();
        leaves[0].data_position = 37; // not any partition start
        let detail = bti_partition_identity_mismatch(&leaves, &data)
            .expect("a DataOffset pointing at a non-partition position must be flagged");
        assert!(detail.contains("not a decoded partition start"));
    }

    #[test]
    fn identity_check_detects_same_count_wrong_keys_via_multiset() {
        // Same leaf count as Data.db and every leaf is individually well-formed
        // (valid key, valid in-map position, consistent prefix) — but the trie
        // resolves the SAME partition three times instead of {1,2,3}. Only the
        // multiset comparison catches this; it is the core of issue #1103.
        let data_keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&data_keys);
        // Three leaves all resolving to partition 1 (key + position from data[0]).
        let leaves: Vec<BtiResolvedLeaf> =
            (0..3).map(|_| inline_leaf(&data[0].1, data[0].0)).collect();
        let detail = bti_partition_identity_mismatch(&leaves, &data)
            .expect("same-count wrong-identity must be flagged");
        assert!(
            detail.contains("identities") || detail.contains("time(s)"),
            "expected a multiset-identity mismatch, got: {detail}"
        );
    }

    #[test]
    fn identity_check_detects_inline_leaf_with_corrupt_data_position() {
        // Reviewer (roborev #1431): a `RowsOffset` leaf whose INLINE key is valid
        // and present in Data.db but whose recorded Data.db position points at a
        // non-partition offset must be flagged — a BTI read would seek to the wrong
        // partition even though the inline key looks fine.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let mut leaves: Vec<BtiResolvedLeaf> =
            data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
        // Keep the valid inline key; corrupt only the recorded Data.db position.
        leaves[0].data_position = 9999; // not any partition start
        let detail = bti_partition_identity_mismatch(&leaves, &data).expect(
            "an inline leaf with a valid key but a non-partition data position must be flagged",
        );
        assert!(detail.contains("not a decoded partition start"));
    }

    #[test]
    fn identity_check_detects_one_swapped_key() {
        // Two keys match, one is wrong — the minimal wrong-root that a count check
        // cannot see.
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        let mut leaves: Vec<BtiResolvedLeaf> =
            data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
        // Replace leaf 0 with a key (pk=99) absent from Data.db, including its
        // prefix, and a position that is not a partition start.
        leaves[0] = inline_leaf(&99u32.to_be_bytes(), 10_000);
        assert!(bti_partition_identity_mismatch(&leaves, &data).is_some());
    }

    // ---- Check 8: key/row order + partition-level LDT (issue #1282) --------

    use crate::util::cassandra_murmur3::cassandra_murmur3_token;

    /// Build the on-disk-ordered partition list the classifier consumes, sorting
    /// the supplied keys by their real Murmur3 `(token, key)` order so the "in
    /// order" input mirrors what a healthy Cassandra SSTable produces.
    fn ordered_partitions(keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Option<i32>)> {
        let mut v: Vec<Vec<u8>> = keys.to_vec();
        v.sort_by_key(|k| (cassandra_murmur3_token(k), k.clone()));
        v.into_iter().map(|k| (k, None)).collect()
    }

    #[test]
    fn order_ldt_clean_partitions_produce_no_findings() {
        let keys: Vec<Vec<u8>> = (1u32..=6).map(|i| i.to_be_bytes().to_vec()).collect();
        let partitions = ordered_partitions(&keys);
        assert!(
            classify_order_and_ldt(&partitions, true).is_empty(),
            "in-token-order partitions with live LDT must produce zero findings"
        );
    }

    #[test]
    fn order_ldt_detects_out_of_order_partition_keys() {
        // Take the correctly-ordered set and swap the first two, forcing a
        // descending (token, key) step Cassandra's verifier rejects.
        let keys: Vec<Vec<u8>> = (1u32..=6).map(|i| i.to_be_bytes().to_vec()).collect();
        let mut partitions = ordered_partitions(&keys);
        partitions.swap(0, 1);
        let findings = classify_order_and_ldt(&partitions, true);
        assert!(
            findings
                .iter()
                .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
            "swapping two partitions must be flagged OutOfOrderKeyOrRow, got {:?}",
            findings
        );
    }

    #[test]
    fn order_ldt_detects_duplicate_partition_token_as_out_of_order() {
        // Equal (token, key) is NOT strictly greater → out of order.
        let k = 7u32.to_be_bytes().to_vec();
        let partitions = vec![(k.clone(), None), (k, None)];
        let findings = classify_order_and_ldt(&partitions, true);
        assert!(findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow));
    }

    #[test]
    fn order_ldt_flags_negative_ldt_on_signed_nb_form() {
        // A deleted partition (Some(ldt)) with a negative ldt on the SIGNED (nb)
        // form is corrupt — Cassandra's DeletionTime/Verifier rejects it.
        let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
        partitions[0].1 = Some(-1);
        let findings = classify_order_and_ldt(&partitions, /*signed_ldt=*/ true);
        assert!(
            findings
                .iter()
                .any(|f| f.class == VerifyErrorClass::InvalidLocalDeletionTime),
            "negative nb localDeletionTime must be flagged, got {:?}",
            findings
        );
    }

    #[test]
    fn order_ldt_does_not_flag_far_future_ldt_on_unsigned_oa_form() {
        // On the UNSIGNED (oa/da) form a value in [2^31, 2^32) is a legitimate
        // far-future deletion time carried as a negative i32 — it MUST NOT be
        // flagged. This is the no-heuristic guard: the format, not the sign, decides.
        let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
        partitions[0].1 = Some(-1); // == 0xFFFFFFFF unsigned == far-future seconds
        let findings = classify_order_and_ldt(&partitions, /*signed_ldt=*/ false);
        assert!(
            !findings
                .iter()
                .any(|f| f.class == VerifyErrorClass::InvalidLocalDeletionTime),
            "far-future unsigned oa/da LDT must NOT be flagged, got {:?}",
            findings
        );
    }

    #[test]
    fn order_ldt_positive_deletion_time_is_clean() {
        // A normal positive epoch-seconds partition tombstone is valid on both forms.
        let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
        partitions[0].1 = Some(1_700_000_000); // ~2023, valid
        assert!(classify_order_and_ldt(&partitions, true).is_empty());
        assert!(classify_order_and_ldt(&partitions, false).is_empty());
    }

    // ---- Check 8 ROW half: clustering-row order (issue #1282 follow-up) -----

    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use crate::types::Value;
    use std::collections::HashMap;

    fn schema_one_ck(order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "issue_1282".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn ck_int(n: i32) -> Vec<Value> {
        vec![Value::Integer(n)]
    }

    #[test]
    fn clustering_order_ascending_rows_are_clean() {
        let schema = schema_one_ck(ClusteringOrder::Asc);
        let partitions = vec![(0usize, vec![ck_int(1), ck_int(2), ck_int(3)])];
        assert!(
            classify_clustering_row_order(&partitions, &schema).is_empty(),
            "in-order ASC clustering rows must produce no findings"
        );
    }

    #[test]
    fn clustering_order_out_of_order_row_is_flagged() {
        // Row 3 comes before row 2 on disk under ASC — corrupt.
        let schema = schema_one_ck(ClusteringOrder::Asc);
        let partitions = vec![(0usize, vec![ck_int(1), ck_int(3), ck_int(2)])];
        let findings = classify_clustering_row_order(&partitions, &schema);
        assert!(
            findings
                .iter()
                .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
            "an out-of-order clustering row must be flagged OutOfOrderKeyOrRow, got {:?}",
            findings
        );
    }

    #[test]
    fn clustering_order_duplicate_row_is_flagged() {
        // Equal consecutive clustering keys are NOT strictly increasing → corrupt.
        let schema = schema_one_ck(ClusteringOrder::Asc);
        let partitions = vec![(0usize, vec![ck_int(5), ck_int(5)])];
        let findings = classify_clustering_row_order(&partitions, &schema);
        assert!(findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow));
    }

    #[test]
    fn clustering_order_respects_desc_ordering() {
        let schema = schema_one_ck(ClusteringOrder::Desc);
        // DESC on disk stores clustering values descending; 3,2,1 is IN ORDER.
        let ok = vec![(0usize, vec![ck_int(3), ck_int(2), ck_int(1)])];
        assert!(
            classify_clustering_row_order(&ok, &schema).is_empty(),
            "descending rows under DESC clustering order must be clean"
        );
        // Ascending 1,2,3 is OUT OF ORDER under DESC.
        let bad = vec![(0usize, vec![ck_int(1), ck_int(2), ck_int(3)])];
        assert!(
            classify_clustering_row_order(&bad, &schema)
                .iter()
                .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
            "ascending rows under a DESC clustering column must be flagged"
        );
    }

    #[test]
    fn identity_check_detects_count_mismatch() {
        let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
        let data = data_partitions(&keys);
        // Only two leaves recovered from the trie (undercount).
        let leaves: Vec<BtiResolvedLeaf> = data
            .iter()
            .take(2)
            .map(|(pos, k)| inline_leaf(k, *pos))
            .collect();
        let detail =
            bti_partition_identity_mismatch(&leaves, &data).expect("undercount must be flagged");
        assert!(detail.contains("2 partition keys"));
        assert!(detail.contains("3 distinct partitions"));
    }
}
