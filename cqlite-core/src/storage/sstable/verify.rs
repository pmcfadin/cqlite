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
use crate::storage::sstable::reader::{extract_sstable_base_name, SSTableReader};
use crate::storage::sstable::verify_location::{self, PendingLocation};
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::{Config, Error, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Corruption-location types (issue #4194), re-exported through `verify` so
// `VerifyFinding.location`'s type is reachable via the existing `verify`
// module path. Resolution LOGIC lives in `verify_location.rs` (file-size
// relocation); only check-site plumbing stays here.
pub use crate::storage::sstable::verify_location::{
    format_location, format_location_compact, KeyRef, Location, LogicalLenSource,
    PartitionResolution, PhysicalAnchor, BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS,
    BOUNDARY_ENTRY_ORDER_VIOLATION, BOUNDARY_SOURCE_UNREADABLE as BOUNDARY_SOURCE_UNREADABLE_CAUSE,
    BTI_IDENTITY_UNCORROBORATED, DATA_DB_SHORTER_THAN_BOUNDARY_SOURCE, MAX_RESOLVED_KEYS,
    PARTITION_KEY_UNAVAILABLE,
};

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
    /// A chunk is compressed with a valid but UNSUPPORTED compression feature —
    /// distinct from truncation/bit-flip ([`ChunkDecompressionError`]) and from a
    /// checksum mismatch ([`DigestMismatch`]) (issue #1414). The canonical case is
    /// a **zstd dictionary-compressed** chunk: the frame is well-formed and its
    /// inline chunk CRC is valid, but CQLite ships no-dictionary zstd only, so the
    /// frame cannot be decoded. The reader fails closed with
    /// `Error::UnsupportedFormat` naming the feature (e.g. the `Dictionary_ID`);
    /// this class makes the verify report say "unsupported feature", never
    /// "corruption".
    ///
    /// [`ChunkDecompressionError`]: VerifyErrorClass::ChunkDecompressionError
    /// [`DigestMismatch`]: VerifyErrorClass::DigestMismatch
    UnsupportedCompressionFeature,
    /// An **uncompressed** BIG `Data.db` chunk did not match its stored `CRC.db`
    /// per-chunk CRC32 (issue #1396) — the uncompressed analogue of the compressed
    /// path's inline chunk-CRC finding ([`ChunkDecompressionError`]). Cassandra
    /// writes a `CRC.db` for every uncompressed BIG SSTable and verifies reads
    /// against it; a bit flip inside an uncompressed chunk is detected here (and,
    /// default-on, on every read). Also covers a truncated / short `CRC.db` (fewer
    /// per-chunk CRC entries than the Data.db has chunks). Reported via a
    /// `VerifyFinding` naming the failing chunk and the `CRC.db`/`Data.db`
    /// component.
    ///
    /// [`ChunkDecompressionError`]: VerifyErrorClass::ChunkDecompressionError
    UncompressedChunkCrcMismatch,
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
    /// A parseable BIG `Filter.db` reports "not present" (`might_contain == false`)
    /// for a partition key that IS present in the SSTable (its raw key bytes are
    /// enumerated from the authoritative `Index.db`) — a Bloom-filter FALSE
    /// NEGATIVE (issue #1398). Cassandra's `Filter.db` carries no checksum, so a
    /// bit flipped from 1→0 inside the bit array is not detected on load and makes
    /// a live partition silently invisible on the BIG point-lookup path
    /// (`partition_lookup.rs` returns `Ok(None)` when the bloom says "miss"). Full
    /// scans and BTI (`da`) lookups are UNAFFECTED (they never gate on this bloom),
    /// so this is a detection tool Cassandra's `sstableverify` lacks — Cassandra
    /// does not verify Filter.db contents and would report the same fixture clean.
    FilterFalseNegative,
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
            VerifyErrorClass::UnsupportedCompressionFeature => "UnsupportedCompressionFeature",
            VerifyErrorClass::UncompressedChunkCrcMismatch => "UncompressedChunkCrcMismatch",
            VerifyErrorClass::UnexpectedEof => "UnexpectedEof",
            VerifyErrorClass::IndexEntryCorrupt => "IndexEntryCorrupt",
            VerifyErrorClass::StatisticsHeaderCorrupt => "StatisticsHeaderCorrupt",
            VerifyErrorClass::SummaryCorrupt => "SummaryCorrupt",
            VerifyErrorClass::BtiRootPointerCorrupt => "BtiRootPointerCorrupt",
            VerifyErrorClass::BtiTrieCorrupt => "BtiTrieCorrupt",
            VerifyErrorClass::RowScanFailed => "RowScanFailed",
            VerifyErrorClass::OutOfOrderKeyOrRow => "OutOfOrderKeyOrRow",
            VerifyErrorClass::InvalidLocalDeletionTime => "InvalidLocalDeletionTime",
            VerifyErrorClass::FilterFalseNegative => "FilterFalseNegative",
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
    /// Which `Data.db` byte range and partitions this finding is anchored to,
    /// when it has a natural byte range (issue #4194). `None` for a finding
    /// with no chunk/offset anchor (e.g. `MissingComponent`,
    /// `StatisticsHeaderCorrupt`) — additive: every pre-#4194 finding site
    /// that does not explicitly populate this leaves it `None`, unchanged.
    pub location: Option<Location>,
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
            location: None,
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
        )?;
        if let Some(loc) = &self.location {
            // COMPACT, not the full renderer (roborev job 131 MEDIUM): this
            // Display feeds `VerifyReport::summary_line()`, which the CLI prints
            // ABOVE its own explicit per-finding `location:` line. See
            // `format_location_compact`.
            write!(f, " (location: {})", format_location_compact(loc))?;
        }
        Ok(())
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
/// `pub(crate)`: `verify_location::finalize_locations` reads `format`/`path()`.
pub(crate) struct ComponentSet {
    base_name: String,
    pub(crate) format: SsTableFormat,
    /// Map of bare component name (e.g. `Data.db`) -> absolute path on disk.
    present: BTreeMap<String, PathBuf>,
    data_path: PathBuf,
}

impl ComponentSet {
    pub(crate) fn path(&self, dir: &Path, component: &str) -> PathBuf {
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
    verify_components(dir, components, mode, config, platform).await
}

/// Verify the EXACT SSTable generation identified by `data_db_path`.
///
/// Additive companion to [`verify_sstable`] (issue #1283, roborev). `verify_sstable`
/// resolves the *lexicographically-first* `*-Data.db` in a directory, which is the
/// wrong generation when a directory holds several: an `SSTableReader` opened on
/// generation N would otherwise report the integrity of whichever `Data.db` sorts
/// first. This entry point verifies precisely the generation whose components share
/// `data_db_path`'s base name (e.g. `nb-2-big`), so a caller that already knows its
/// own `Data.db` (an open reader) gets a verdict for THAT generation.
///
/// `data_db_path` must be an existing `*-Data.db` file; its parent directory supplies
/// the sibling components. Returns a [`VerifyReport`] with the same corruption-as-
/// findings contract as [`verify_sstable`].
pub async fn verify_sstable_generation(
    data_db_path: &Path,
    mode: VerifyMode,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<VerifyReport> {
    let dir = generation_dir(data_db_path);
    let components = resolve_components_for_data_path(dir, data_db_path)?;
    verify_components(dir, components, mode, config, platform).await
}

/// Resolve the directory that holds `data_db_path`'s sibling components.
///
/// `Path::parent()` returns an EMPTY path (not `None`) for a relative,
/// directory-less filename (e.g. `nb-1-big-Data.db` opened from the SSTable dir as
/// cwd), so a naive scan would look in the empty path instead of the current
/// directory and fail component resolution — even though `SSTableReader::open`
/// found the file (its sibling lookup joins onto the empty parent, which resolves
/// relative to cwd). Normalize a missing/empty parent to `.` so component
/// resolution scans the current directory (issue #1283, roborev).
fn generation_dir(data_db_path: &Path) -> &Path {
    match data_db_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

/// Shared verification body: runs all checks over an already-resolved
/// [`ComponentSet`]. Both [`verify_sstable`] (first-generation resolution) and
/// [`verify_sstable_generation`] (exact-generation resolution) delegate here so the
/// check pipeline is defined exactly once (issue #1283).
async fn verify_components(
    dir: &Path,
    components: ComponentSet,
    mode: VerifyMode,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<VerifyReport> {
    let mut findings: Vec<VerifyFinding> = Vec::new();
    // Issue #4194: chunk/offset-anchored findings awaiting location
    // resolution, resolved in one pass at the end (`finalize_locations`) once
    // every check has run and the boundary source's own health is known.
    let mut pending_locations: Vec<PendingLocation> = Vec::new();
    // Cloned up front (cheap: an `Arc` bump) so it survives the `platform`
    // move into `full_row_scan_partitions` below, for `finalize_locations`'s
    // own `IndexReader::open` re-read of `Index.db` (BIG).
    let platform_for_location = platform.clone();

    // ---- Check 1: TOC.txt completeness + component presence ----------------
    let toc_components = check_toc_and_presence(dir, &components, &mut findings)?;

    // ---- Check 2: Digest.crc32 vs CRC32(Data.db) ---------------------------
    check_digest(dir, &components, &mut findings)?;

    // ---- Check 3: CompressionInfo.db parse + chunk-offset bounds -----------
    let compression_info =
        check_compression_info(dir, &components, &mut findings, &mut pending_locations)?;

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

    // Issue #4194: the position -> raw-key map recovered by the FULL-mode row
    // scan (populated below, `None` in QUICK mode or when the scan does not
    // run) — used by `finalize_locations` to resolve a BTI `DataOffset`
    // leaf's raw key (its identity is only recoverable through the scan; a
    // `RowsOffset` leaf's key is already inline on `bti_leaves`).
    let mut scan_position_map: Option<std::collections::HashMap<u64, Vec<u8>>> = None;

    // Issue #4194 (owner ruling 2026-10-02: option (a), fail closed): whether
    // the BTI identity cross-check below RAN TO COMPLETION and AGREED. Starts
    // `false` and is only ever set by the affirmative outcome — never derived
    // from the ABSENCE of a mismatch finding, which is the whole defect
    // (CLAUDE.md: key the permissive branch on the affirmative value). QUICK
    // mode, a `compression_metadata_corrupt` skip, a failed scan and an absent
    // `bti_leaves` all leave it `false`, and each of those is a state in which
    // "no mismatch was reported" means "nothing looked". See
    // `verify_location::BTI_IDENTITY_UNCORROBORATED`.
    let mut bti_identity_corroborated = false;

    if mode == VerifyMode::Full {
        // ---- Check 5: inline Data.db chunk CRC validation (#998) -----------
        // THREE-WAY (roborev I3, #4194): an unreadable `CompressionInfo.db`
        // means the table is COMPRESSED and neither chunk check may run —
        // the inline check needs trustworthy offsets, and the uncompressed
        // check would validate this Data.db against a `CRC.db` grid that is
        // not its grid, reporting a physical range as a logical damaged
        // extent. The cause is already a finding; adding a second, wrong one
        // is strictly worse than adding none.
        match &compression_info {
            CompressionState::Compressed(info) => {
                check_inline_chunk_crc(&components, info, &mut findings, &mut pending_locations)?;
            }
            CompressionState::Unreadable => {}
            // ---- Check 5b: uncompressed CRC.db per-chunk validation (#1396)
            // An uncompressed BIG SSTable (no CompressionInfo.db) carries a
            // CRC.db per-chunk checksum sidecar. Read it and validate every
            // Data.db chunk — the uncompressed analogue of the inline
            // chunk-CRC check above. Replaces the prior behavior where CRC.db
            // was only name-whitelisted (recognized as a component) but never
            // content-validated.
            CompressionState::Uncompressed if components.format == SsTableFormat::Big => {
                check_uncompressed_crc_db(dir, &components, &mut findings, &mut pending_locations)
                    .await;
            }
            CompressionState::Uncompressed => {}
        }

        // ---- Check 6a: Statistics.db parse ---------------------------------
        check_statistics(dir, &components, platform.clone(), &mut findings).await;

        // ---- Check 6b: Summary.db parse (BIG only) -------------------------
        if components.format == SsTableFormat::Big {
            check_summary(dir, &components, platform.clone(), &mut findings).await;
        }

        // ---- Check 6c: Filter.db no-false-negative membership (BIG only) ----
        //
        // A parseable Filter.db with a bit flipped 1→0 inside the bit array is
        // NOT detected on load (Cassandra's Filter.db has no checksum, and the
        // read path is fail-open only for UNPARSEABLE filters) yet yields false
        // negatives: `might_contain == false` for a present key makes the BIG
        // point-lookup path return Ok(None) — a live partition silently invisible
        // (issue #1398). Cassandra's sstableverify does not verify Filter.db
        // contents, so this is a detection tool Cassandra lacks. BTI is immune
        // (bloom bypassed for the trie) and full scans never gate on the bloom, so
        // this check is BIG-only and probes the authoritative Index.db present
        // keys against the decoded filter.
        if components.format == SsTableFormat::Big {
            check_filter_false_negatives(dir, &components, platform.clone(), &mut findings).await;
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
                    //
                    // Issue #4194: borrowed (not moved) — `bti_leaves` is needed
                    // again by `finalize_locations` below.
                    if let Some(leaves) = bti_leaves.as_ref() {
                        match bti_partition_identity_mismatch(leaves, &scan_partitions) {
                            Some(detail) => findings.push(VerifyFinding::new(
                                VerifyErrorClass::BtiRootPointerCorrupt,
                                "Partitions.db",
                                detail,
                            )),
                            // THE one affirmative corroboration point (#4194
                            // option (a)): every leaf's payload, resolved back
                            // to a raw key by authoritative data, was compared
                            // against the keys decoded from Data.db and they
                            // agreed. Only this outcome licenses a BTI
                            // `Resolved` location. A mismatch is left `false`
                            // too — it also pushes `BtiRootPointerCorrupt`,
                            // which distrusts the source by class anyway, so
                            // the two signals agree rather than race.
                            None => bti_identity_corroborated = true,
                        }
                    }
                    // Issue #4194, roborev round-4 MEDIUM finding: only built
                    // when it can actually be USED — BTI with at least one
                    // pending location. Previously retained unconditionally
                    // (including for BIG, which never reads it, and for the
                    // overwhelmingly common clean-file case), materializing a
                    // whole-table position->key map — tens of MB on a large
                    // table — for zero benefit.
                    if components.format == SsTableFormat::Bti && !pending_locations.is_empty() {
                        scan_position_map = Some(
                            scan_partitions
                                .into_iter()
                                .collect::<std::collections::HashMap<_, _>>(),
                        );
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

    // Issue #4194: resolve every pending location in one pass, now that every
    // check has run and the boundary source's own health is fully known.
    // Lives in `verify_location.rs` (file-size relocation) — this call site
    // is the only thing that stays here.
    if !pending_locations.is_empty() {
        verify_location::finalize_locations(
            dir,
            &components,
            &mut findings,
            pending_locations,
            bti_leaves.as_deref(),
            scan_position_map.as_ref(),
            bti_identity_corroborated,
            platform_for_location,
        )
        .await;
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

/// Read all regular files in `dir`, returning `(all_files, data_files)` where
/// `data_files` is the subset ending in `-Data.db`.
fn read_dir_files(dir: &Path) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
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
    Ok((all_files, data_files))
}

/// Locate the SSTable generation in `dir` and enumerate its on-disk components.
///
/// If `dir` contains several generations, the lexicographically-first `*-Data.db`
/// is selected (documented behavior of [`verify_sstable`]). To verify a SPECIFIC
/// generation, use [`resolve_components_for_data_path`] / [`verify_sstable_generation`].
fn resolve_components(dir: &Path) -> Result<ComponentSet> {
    let (all_files, mut data_files) = read_dir_files(dir)?;

    data_files.sort();
    let data_path = data_files.into_iter().next().ok_or_else(|| {
        Error::not_found(format!(
            "No *-Data.db component found in SSTable directory {}",
            dir.display()
        ))
    })?;

    build_component_set(&all_files, data_path)
}

/// Enumerate the components for the EXACT generation identified by `data_path`
/// within `dir`. Unlike [`resolve_components`], this does not pick the first-sorted
/// generation; it uses precisely the caller-supplied `data_path` (issue #1283).
fn resolve_components_for_data_path(dir: &Path, data_path: &Path) -> Result<ComponentSet> {
    if !data_path.is_file() {
        return Err(Error::not_found(format!(
            "SSTable Data.db component not found at {}",
            data_path.display()
        )));
    }
    let (all_files, _data_files) = read_dir_files(dir)?;
    build_component_set(&all_files, data_path.to_path_buf())
}

/// Build a [`ComponentSet`] for `data_path`, indexing the sibling components in
/// `all_files` that share its base name.
fn build_component_set(all_files: &[PathBuf], data_path: PathBuf) -> Result<ComponentSet> {
    let data_name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::invalid_path("Data.db filename is not valid UTF-8"))?;
    // Derive the base name with the SAME tolerance as `SSTableReader::open`, which
    // locates its sibling components via `extract_sstable_base_name` and still opens
    // a file whose name it cannot map (it simply skips the siblings). A reader that
    // opened successfully MUST get an `IntegrityCheckResult`, not an `Err`, so this
    // never rejects on the "-Data.db" suffix (issue #1283, roborev):
    //   1. standard names end in "-Data.db" -> strip it (e.g. "nb-1-big");
    //   2. otherwise fall back to the reader's own base-name derivation (descriptor
    //      parse, then the {prefix}-{gen}-{format} heuristic) so any non-standard
    //      name the reader accepts resolves the same base here;
    //   3. if even that cannot map the name, degrade to the filename minus its ".db"
    //      extension so we still verify what we can (Data.db digest, chunk CRCs)
    //      rather than erroring — matching reader-open tolerance.
    let base_name = data_name
        .strip_suffix("-Data.db")
        .map(str::to_string)
        .or_else(|| extract_sstable_base_name(&data_path))
        .unwrap_or_else(|| {
            data_name
                .strip_suffix(".db")
                .unwrap_or(data_name)
                .to_string()
        });

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

/// What Check 3 established about this generation's compression — THREE
/// states, not two (roborev important finding I3, #4194).
///
/// `Option<CompressionInfo>` conflated "genuinely uncompressed" with
/// "compressed, but `CompressionInfo.db` cannot be trusted", and the caller's
/// `else` branch then dispatched the second case into
/// [`check_uncompressed_crc_db`]. MEASURED consequence, on a staged
/// `compression_info_bad_offset` generation carrying a `CRC.db`: an
/// `UncompressedChunkCrcMismatch` finding with
/// `location: Data.db: chunk 0, offset 0x0 len 6979 — 1 partition(s)` — a
/// COMPRESSED table's PHYSICAL byte range reported as a logical damaged
/// extent, checked against a chunk grid that is not its grid, with a
/// confidently-resolved partition list attached. A wrong answer presented as
/// a right one, which §D2 exists to prevent.
enum CompressionState {
    /// No `CompressionInfo.db` on disk: a genuinely uncompressed table. Its
    /// `CRC.db` (BIG) IS the authoritative chunk grid.
    Uncompressed,
    /// Parsed and bounds-checked. Usable by the FULL-mode inline-CRC check.
    Compressed(CompressionInfo),
    /// A `CompressionInfo.db` IS present but cannot be trusted — it failed to
    /// parse, or it declares a chunk offset out of bounds for `Data.db`. The
    /// table is COMPRESSED, so no uncompressed check may run against it, and
    /// the inline chunk-CRC check cannot run either (it derives each chunk's
    /// size from adjacent offsets).
    ///
    /// Carries no cause: the cause is already recorded as a `VerifyFinding` by
    /// the check that detected it, which is its single source of truth.
    /// Duplicating it here would invite two divergent renderings of one fact.
    Unreadable,
}

/// Check 3: `CompressionInfo.db` parses (#1001) and all chunk offsets are
/// in-bounds for `Data.db`. See [`CompressionState`] for why the result is
/// three-valued.
fn check_compression_info(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
    pending_locations: &mut Vec<PendingLocation>,
) -> Result<CompressionState> {
    let ci_path = components.path(dir, "CompressionInfo.db");
    if !ci_path.exists() {
        return Ok(CompressionState::Uncompressed);
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
            // PRESENT but unparseable: the table is compressed and nothing
            // downstream may treat it as uncompressed (I3).
            return Ok(CompressionState::Unreadable);
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
            // The offsets were never bounds-checked, so `info` is not
            // established as trustworthy — refuse it rather than handing
            // unvalidated offsets to the inline-CRC check (same fail-closed
            // direction as I4 below: a stat failure must not fail OPEN).
            return Ok(CompressionState::Unreadable);
        }
    };
    let mut offset_out_of_bounds = false;
    // Issue #4194, roborev round-1 HIGH finding: a location is attached ONLY
    // to the FIRST out-of-bounds chunk. This loop has no cap on how many
    // `ChunkOffsetOutOfBounds` findings it can push (one per bad chunk offset,
    // unbounded on a maliciously/severely truncated CompressionInfo.db), and
    // attaching a `Resolved(Vec<KeyRef>)` to EVERY one of them — each holding
    // a hex string for every partition from that chunk to EOF — makes the
    // resident location data O(bad_chunks × partitions_past_eof): quadratic
    // in the corruption's own severity, materialized as ONE `VerifyFinding`
    // per bad chunk and then serialized into a single JSON line, directly
    // contradicting this change's own <128 MB / no-data-dir-wide-structure
    // posture. The FIRST out-of-bounds chunk's range IS the most inclusive
    // (design.md §D1's `[new_eof, original_logical_length)` — every later
    // chunk's range is a strict subset), so it alone already answers "which
    // partitions does this truncation touch"; every subsequent
    // `ChunkOffsetOutOfBounds` finding still fires (unchanged corruption
    // signal) but is left `location: None`.
    let mut first_out_of_bounds_located = false;
    for (i, &offset) in info.chunk_offsets.iter().enumerate() {
        // Every chunk record is at least its 4-byte inline CRC, so the offset
        // itself must leave room for that. Offsets at/after EOF are corrupt.
        if offset.saturating_add(4) > data_len {
            offset_out_of_bounds = true;
            let finding_index = findings.len();
            findings.push(VerifyFinding::new(
                VerifyErrorClass::ChunkOffsetOutOfBounds,
                "CompressionInfo.db",
                format!(
                    "chunk[{}] offset {} (0x{:x}) points past Data.db end ({} bytes)",
                    i, offset, offset, data_len
                ),
            ));
            if !first_out_of_bounds_located {
                first_out_of_bounds_located = true;
                // Issue #4194: this is the truncation-anchored finding this
                // corruption class actually produces (verified against the real
                // `test_comp_corrupt/data_db_truncation` fixture — the boundary
                // source's declared LOGICAL length (`data_length`) is the extent
                // every partition past this chunk's logical start is measured
                // against; design.md §D1's "new_eof .. original_logical_length"
                // derivation, computed here rather than deferred since `info` is
                // only in scope in this function).
                let logical_start = (i as u64).saturating_mul(info.chunk_length as u64);
                pending_locations.push(PendingLocation {
                    finding_index,
                    component: "Data.db".to_string(),
                    byte_offset: offset,
                    byte_len: 4,
                    // The declared record does not FIT in the file, so this
                    // physical range is a declared location rather than a
                    // damaged extent; the damage is the logical tail
                    // `partitions` enumerates (roborev job 92 MEDIUM).
                    //
                    // `byte_len: 4` is a chunk record's MINIMUM SIZE, and the
                    // range `[offset, offset + 4)` is the record's HEAD at the
                    // declared offset -- NOT a length prefix (roborev job 108)
                    // and NOT the trailing CRC32 (roborev nit N1, #4194: this
                    // comment said "TRAILING inline CRC32", which names the
                    // wrong end of the record; the CRC lies at the record's far
                    // end, whose position is unknown for an out-of-bounds chunk
                    // because the payload length is not known). 4 IS the
                    // minimum because `compression_info.rs` documents the
                    // record layout as `[compressed_bytes][4-byte CRC32]`,
                    // citing CompressedSequentialWriter.java:203's
                    // `chunkOffset += compressedLength + 4`, so even a
                    // zero-length payload occupies 4 bytes -- which is also
                    // exactly what the bounds check three lines above asserts.
                    // Cassandra source was NOT re-read here (no pinned clone on
                    // this host), so this states the in-repo records rather than
                    // claiming fresh primary-source verification.
                    anchor: PhysicalAnchor::DeclaredRecord,
                    chunk_index: Some(i),
                    damaged_logical: (logical_start, info.data_length.max(logical_start)),
                    logical_len: info.data_length,
                    logical_len_source: LogicalLenSource::Declared,
                });
            }
        }
    }

    // An out-of-bounds offset is corrupt metadata: do NOT hand it downstream.
    // The inline-CRC check derives each chunk's compressed size from adjacent
    // offsets, which would underflow (panic in debug / huge alloc in release) on
    // a bad offset — violating the corruption-as-findings contract. The finding
    // is already recorded, so returning None just skips the chunk-CRC check
    // (roborev).
    if offset_out_of_bounds {
        return Ok(CompressionState::Unreadable);
    }

    Ok(CompressionState::Compressed(info))
}

/// One BTI `Partitions.db` leaf, with its PAYLOAD resolved back to a raw
/// partition key using authoritative data (issue #1103).
///
/// The verifier resolves every leaf so a corruption that keeps the leaf's
/// emitted byte-comparable prefix while rewriting its payload to point at a
/// DIFFERENT partition is still caught (a same-count, wrong-IDENTITY
/// corruption the prefix-only compare missed).
/// `pub(crate)`: `verify_location::finalize_locations` reads its fields.
pub(crate) struct BtiResolvedLeaf {
    /// The path-compressed byte-comparable prefix emitted by the trie walk
    /// (`[0x40 ++ token]` truncated to the shortest distinguishing prefix). Used
    /// only for the prefix/payload-consistency assertion.
    prefix: Vec<u8>,
    /// The raw partition key this leaf's payload resolves to, when it could be
    /// recovered directly (a `RowsOffset` leaf stores the raw key INLINE in
    /// `Rows.db`). `None` for a `DataOffset` leaf, whose raw key is recovered via
    /// the Data.db position map ([`Self::data_position`]).
    pub(crate) inline_raw_key: Option<Vec<u8>>,
    /// The decompressed-`Data.db` partition-start position the payload points at:
    /// the `DataOffset` value directly, or the `data_position` recovered from the
    /// `RowsOffset` row-index entry. Resolved to a raw key via the Data.db scan's
    /// position map in [`bti_partition_identity_mismatch`].
    pub(crate) data_position: u64,
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
///   row-index entry is resolved from `Rows.db` EXACTLY ONCE, via
///   [`resolve_rows_db_entry_uncounted`] — recovering the inline raw key, the
///   partition's Data.db position and the trie root that
///   [`iterate_rows_in_bti_trie`] is then walked from (structural check). The
///   UNCOUNTED resolver deliberately: verification is not the CLUSTERING read
///   path, so it must not perturb the L1 `ROWS_DB_ENTRY_RESOLVES` invariant
///   (issue #1647). A truncated `Rows.db` makes the referenced offset point past
///   EOF or the row-trie read hit EOF.
/// * A `DataOffset` payload carries the partition's decompressed-Data.db
///   position directly; its raw key is resolved later through the Data.db scan.
fn check_bti_structure(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Option<Vec<BtiResolvedLeaf>>> {
    use crate::storage::sstable::bti::parser::{
        iterate_partitions_in_bti_file, iterate_rows_in_bti_trie, resolve_rows_db_entry_uncounted,
        BtiPartitionLocation, RowsTrieRootRejectReason,
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
                // Resolve this partition's `TrieIndexEntry` ONCE: the one header serves
                // the structural walk below, the rejection reason and the Data.db
                // position. UNCOUNTED because verification is not the CLUSTERING read
                // path, so it must not inflate the L1 `ROWS_DB_ENTRY_RESOLVES`
                // invariant (issue #1647) that lane asserts is exactly 1 per read.
                let header = match resolve_rows_db_entry_uncounted(&rows_bytes, off) {
                    Ok(header) => header,
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
                let walk = header
                    .require_trie_root()
                    .and_then(|root| iterate_rows_in_bti_trie(&rows_bytes, root));
                if let Err(e) = walk {
                    // Issue #3002: an INTACT file whose row-index ROOT merely violates
                    // the writer-ordering invariant (e.g. a `Rows.db` written by CQLite
                    // <= 0.16, whose root delta was measured from a 2-byte-low base) is
                    // NOT damaged — the remedy is a rewrite, not data recovery. Naming
                    // it "truncated/corrupt" would send an operator hunting for damage
                    // that is not there, so the reason decides the wording.
                    let reject_reason = header.trie_root.err().map(|rejection| rejection.reason);
                    let detail = match reject_reason {
                        Some(
                            reason @ (RowsTrieRootRejectReason::ExtentNotAtEntry { .. }
                            | RowsTrieRootRejectReason::PayloadIncapableNodeType { .. }),
                        ) => format!(
                            "row-index root for the partition at Rows.db offset {} does not satisfy the BTI writer-ordering invariant ({}) — the file's bytes are intact, but the root is unusable, so clustering reads decode whole partitions; rewrite (re-flush/re-compact) this SSTable (issue #3002): {}",
                            off,
                            reason.label(),
                            e
                        ),
                        Some(reason) => format!(
                            "row-index trie for partition at Rows.db offset {} has an unusable root ({}) — truncated/corrupt: {}",
                            off,
                            reason.label(),
                            e
                        ),
                        None => format!(
                            "row-index trie for partition at Rows.db offset {} failed to parse (truncated/corrupt): {}",
                            off, e
                        ),
                    };
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::BtiTrieCorrupt,
                        "Rows.db",
                        detail,
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

                // The partition's Data.db position comes from the SAME (single) entry
                // resolve above, so a leaf whose INLINE key and Data.db position
                // disagree (a payload tamper) is still cross-checkable through the
                // position map.
                leaves.push(BtiResolvedLeaf {
                    prefix,
                    inline_raw_key: Some(inline_raw_key),
                    data_position: header.data_position,
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

/// Check 6c (FULL, BIG): the `Filter.db` Bloom filter must have NO false
/// negatives over the present partition keys (issue #1398).
///
/// A false negative — `might_contain == false` for a key Cassandra actually wrote
/// — makes that partition silently invisible on the BIG point-lookup path
/// (`partition_lookup.rs` returns `Ok(None)` on a bloom "miss"). Because
/// `Filter.db` carries no checksum, a bit flipped 1→0 inside the bit array is not
/// caught on load; only re-probing every present key against the decoded filter
/// surfaces it. The authoritative present-key set is the raw partition-key bytes
/// in the sibling `Index.db` (`key_digest`, issue #552) — exactly the bytes
/// Cassandra's Murmur3 hashed into the filter (no path/type heuristics).
///
/// Fail-open, safe direction (matches `component_loading.rs`): if `Filter.db` is
/// absent or does not decode, this check records nothing — an absent/unparseable
/// filter means the read path simply skips the bloom (no false negatives). Only a
/// PARSEABLE filter that drops a present key is flagged. If `Index.db` is
/// absent/corrupt the present-key set is unavailable, so nothing is probed here
/// (that corruption is surfaced by [`check_big_index`]).
async fn check_filter_false_negatives(
    dir: &Path,
    components: &ComponentSet,
    platform: Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) {
    use crate::storage::sstable::bloom::BloomFilter;
    use crate::storage::sstable::index_reader::IndexReader;

    let filter_path = components.path(dir, "Filter.db");
    let index_path = components.path(dir, "Index.db");
    // Absent Filter.db → the read path skips the bloom entirely (no false
    // negatives possible). Absent Index.db → no authoritative present-key source.
    if !filter_path.exists() || !index_path.exists() {
        return;
    }

    let Ok(filter_bytes) = std::fs::read(&filter_path) else {
        return;
    };
    // Fail-open: an unparseable filter is the safe direction (component_loading.rs
    // makes the bloom simply unavailable). Only a PARSEABLE-but-wrong filter is the
    // silent false-negative hazard this check exists to catch.
    let Ok(bloom) = BloomFilter::deserialize(&filter_bytes) else {
        return;
    };

    // Enumerate the authoritative present keys from Index.db. A parse failure here
    // is Index.db corruption, already surfaced by check_big_index — do not
    // fabricate a filter finding from it.
    let Ok(reader) = IndexReader::open(&index_path, platform).await else {
        return;
    };

    let mut present = 0usize;
    let mut false_negatives = 0usize;
    for entry in reader.get_partition_entries() {
        present += 1;
        if !bloom.might_contain(&entry.key_digest) {
            false_negatives += 1;
        }
    }

    if false_negatives > 0 {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::FilterFalseNegative,
            "Filter.db",
            format!(
                "Bloom filter reported {false_negatives} false negative(s) over {present} present \
                 partition key(s): a present key hashes to a bit the filter reports unset, so the \
                 BIG point-lookup path would return no rows for a live partition (silent data \
                 invisibility). Filter.db carries no checksum, so a 1→0 bit flip inside the bit \
                 array is not caught on load; full scans and BTI lookups are unaffected."
            ),
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
    pending_locations: &mut Vec<PendingLocation>,
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
    //
    // Issue #4194: reads chunk-by-chunk (rather than `read_all_chunks()` in one
    // call) so the FAILING chunk's index is known directly from the loop
    // variable — never parsed back out of the error message text (no-heuristics
    // mandate, issue #28) — for the location this finding carries. Fails fast
    // on the first bad chunk, same as `read_all_chunks()` did.
    let mut chunk_reader = ChunkReader::new(reader, info.clone(), total_size);
    for i in 0..chunk_reader.chunk_count() {
        if let Err(e) = chunk_reader.read_chunk(i) {
            let finding_index = findings.len();
            findings.push(classify_data_error("Data.db", &e));
            // Issue #4194, roborev round-3 LOW finding: `compressed_chunk_size`
            // returns `None` precisely when the chunk_offsets table is corrupt
            // enough that the checked subtraction underflows — the case this
            // check exists to detect. `.unwrap_or(0)` used to collapse that
            // into a fabricated `byte_len: 0`, indistinguishable from a
            // legitimately empty range and pointing an operator at the wrong
            // bytes ("never a guess" — this module's own doc). Skip the
            // location entirely when either physical lookup is unmeasurable;
            // the finding itself (chunk `i`, the decode error) is unaffected.
            if let (Some(phys_offset), Some(phys_len)) = (
                info.compressed_chunk_offset(i),
                info.compressed_chunk_size(i, total_size),
            ) {
                let logical_start = (i as u64).saturating_mul(info.chunk_length as u64);
                let logical_end = ((i as u64).saturating_add(1))
                    .saturating_mul(info.chunk_length as u64)
                    .min(info.data_length)
                    .max(logical_start);
                pending_locations.push(PendingLocation {
                    finding_index,
                    component: "Data.db".to_string(),
                    byte_offset: phys_offset,
                    byte_len: phys_len,
                    anchor: PhysicalAnchor::DamagedExtent,
                    chunk_index: Some(i),
                    damaged_logical: (logical_start, logical_end),
                    logical_len: info.data_length,
                    logical_len_source: LogicalLenSource::Declared,
                });
            }
            break;
        }
    }
    Ok(())
}

/// Check 5b (FULL, uncompressed BIG only): read `CRC.db` and validate every
/// uncompressed `Data.db` chunk against its stored per-chunk CRC32 (issue #1396).
///
/// This is the uncompressed analogue of [`check_inline_chunk_crc`]. Cassandra
/// writes a `CRC.db` for every uncompressed BIG SSTable; a bit flip inside a
/// chunk (or a truncated `CRC.db`) is reported as an
/// [`VerifyErrorClass::UncompressedChunkCrcMismatch`] `VerifyFinding` naming the
/// failing chunk. Streams the Data.db one `chunk_size` block at a time (bounded
/// memory) rather than buffering the whole file. An absent `CRC.db` is the
/// owner-pinned warn-and-proceed decision (design D4): no finding is recorded
/// (its absence is surfaced by the TOC/presence check when listed).
async fn check_uncompressed_crc_db(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut Vec<VerifyFinding>,
    pending_locations: &mut Vec<PendingLocation>,
) {
    use crate::storage::sstable::reader::crc::CrcDb;
    use tokio::io::AsyncReadExt;

    let crc_path = components.path(dir, "CRC.db");
    if !crc_path.exists() {
        // Absent CRC.db: warn-and-proceed (design D4). Not a checksum-mismatch.
        return;
    }

    // Data.db length bounds the maximum plausible CRC.db size (issue #1396
    // Fix 2): `CrcDb::open` rejects an oversized sidecar before reading its body.
    //
    // A TYPED FINDING, never `unwrap_or(0)` (roborev important finding I4,
    // #4194). `data_len` is not only that bound: it becomes the
    // `PendingLocation::logical_len` below, which bounds the LAST partition's
    // extent. A silent `0` collapsed that extent to `[last_start, 0)` — empty
    // — so the last partition could never be reported for any damage in the
    // file: the same silent-drop shape as blocker #2, reached through a failed
    // stat instead of a corrupt offset. It also disabled the oversize guard
    // the value exists for, since every CRC.db is "larger than 0 bytes of
    // Data.db". Fail CLOSED: name the stat failure and stop, exactly as
    // `check_compression_info`'s own bounds check already does for the same
    // call.
    let data_len = match tokio::fs::metadata(&components.data_path).await {
        Ok(m) => m.len(),
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot stat Data.db for CRC.db check: {e}"),
            ));
            return;
        }
    };
    let crc = match CrcDb::open(&crc_path, data_len).await {
        Ok(c) => c,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::UncompressedChunkCrcMismatch,
                "CRC.db",
                format!("CRC.db failed to parse: {e}"),
            ));
            return;
        }
    };

    let chunk_size = crc.chunk_size() as usize;
    let mut file = match tokio::fs::File::open(&components.data_path).await {
        Ok(f) => f,
        Err(e) => {
            findings.push(VerifyFinding::new(
                VerifyErrorClass::MissingComponent,
                "Data.db",
                format!("cannot open Data.db for CRC.db check: {e}"),
            ));
            return;
        }
    };

    // Walk Data.db one chunk_size block at a time and compare each block's CRC32
    // to the stored value (bounded memory, O(chunk_size)).
    let mut chunk_index = 0usize;
    let mut offset: u64 = 0;
    // `chunk_size` is bounded by `MAX_CRC_CHUNK_SIZE` at parse time
    // (`CrcDb::parse`, issue #1396) — a malformed sidecar advertising an absurd
    // size was already rejected above as typed corruption, so this scratch
    // allocation can never scale to an OOM.
    let mut buf = vec![0u8; chunk_size];
    loop {
        let mut filled = 0usize;
        // Accumulate a full chunk (or the short final chunk at EOF).
        loop {
            match file.read(&mut buf[filled..]).await {
                Ok(0) => break,
                Ok(n) => {
                    filled += n;
                    if filled == chunk_size {
                        break;
                    }
                }
                Err(e) => {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::UncompressedChunkCrcMismatch,
                        "Data.db",
                        format!("read error verifying chunk {chunk_index} against CRC.db: {e}"),
                    ));
                    return;
                }
            }
        }
        if filled == 0 {
            break; // clean EOF on a chunk boundary
        }
        let computed = crc32fast::hash(&buf[..filled]);
        match crc.crc_for_chunk(chunk_index) {
            Ok(expected) => {
                if computed != expected {
                    let finding_index = findings.len();
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::UncompressedChunkCrcMismatch,
                        "Data.db",
                        format!(
                            "uncompressed CRC32 mismatch for chunk {chunk_index} at Data.db offset 0x{offset:x} ({filled} bytes): expected=0x{expected:08x} (CRC.db), computed=0x{computed:08x}"
                        ),
                    ));
                    // Issue #4194: uncompressed, so physical == logical offset
                    // space — the CRC.db grid IS the chunk grid (design.md §D1).
                    pending_locations.push(PendingLocation {
                        finding_index,
                        component: "Data.db".to_string(),
                        byte_offset: offset,
                        byte_len: filled as u64,
                        anchor: PhysicalAnchor::DamagedExtent,
                        chunk_index: Some(chunk_index),
                        damaged_logical: (offset, offset.saturating_add(filled as u64)),
                        logical_len: data_len,
                        logical_len_source: LogicalLenSource::MeasuredDataDbLength,
                    });
                    // Report the first failing chunk and stop (matches the
                    // fail-fast read-path posture; naming one chunk is sufficient).
                    return;
                }
            }
            Err(e) => {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::UncompressedChunkCrcMismatch,
                    "CRC.db",
                    format!("CRC.db has no entry for Data.db chunk {chunk_index} (truncated): {e}"),
                ));
                return;
            }
        }
        offset += filled as u64;
        chunk_index += 1;
        if filled < chunk_size {
            break; // short final chunk consumed
        }
    }
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
    let partitions = reader.distinct_partition_keys_with_positions(None).await?;

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
    let class = classify_scan_error_class(err);
    VerifyFinding::new(class, "Data.db".to_string(), err.to_string())
}

/// Map a Data.db scan/decode error to its stable [`VerifyErrorClass`].
///
/// Split out from [`classify_scan_error`] so the classification is unit-testable
/// without constructing a [`ComponentSet`] (which the classifier ignores).
fn classify_scan_error_class(err: &Error) -> VerifyErrorClass {
    let msg = err.to_string();
    let lower = msg.to_lowercase();
    // Unsupported compression FEATURE (issue #1414): the reader fails closed with
    // `Error::UnsupportedFormat` on a valid-but-unimplemented compression feature
    // (canonically a zstd dictionary-compressed chunk). This is NEITHER corruption
    // NOR a checksum mismatch — the frame and its inline CRC are valid — so it must
    // NOT collapse into `ChunkDecompressionError` (truncation/bit-flip) or
    // `DigestMismatch`. Classify it FIRST, keyed on the authoritative error variant.
    //
    // INVARIANT (roborev): only a COMPRESSION-related `UnsupportedFormat` may reach
    // this scan classifier and earn the compression-specific class. Every such
    // producer names compression in its message — "Unknown/Unsupported compression
    // algorithm …", "<X> support not compiled in", or the zstd dictionary rejection
    // ("… dictionary compression … is unsupported …"). The version/format-detection
    // `UnsupportedFormat` producers fire at OPEN time and are classified on a
    // different path, so they never arrive here; but the coupling is implicit, so we
    // gate on the compression message-shape and fall through to the generic
    // `RowScanFailed` for any non-compression `UnsupportedFormat` rather than
    // mislabeling it as an unsupported compression feature. (Classifying an
    // already-typed error by message shape is not type inference; #28 is respected.)
    if matches!(err, Error::UnsupportedFormat(_))
        && (lower.contains("compress") || lower.contains("compiled in"))
    {
        return VerifyErrorClass::UnsupportedCompressionFeature;
    }
    if msg.contains("CRC32 mismatch") {
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
    }
}

#[cfg(test)]
#[path = "verify_tests.rs"]
mod tests;
