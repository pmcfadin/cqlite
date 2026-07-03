//! Repaired-metadata `Statistics.db` parity sub-lane (issue #988 / epic #968).
//!
//! Proves CQLite decodes and reports the *persisted* repair-state metadata that
//! Apache Cassandra 5.0 writes into the STATS component of `Statistics.db`,
//! validated against the committed `*-Statistics.db.txt` reference dumps
//! `sstablemetadata` produced for every fixture (nb / oa / da).
//!
//! Scope owned here (issue #988):
//!   * **repairedAt** — decoded directly from the STATS-component bytes for
//!     every fixture and asserted equal to the `Repaired at: <n>` reference
//!     line. The whole Cassandra 5.0 corpus is in the UNREPAIRED state
//!     (`Repaired at: 0`), so this proves the unrepaired `repairedAt=0` field
//!     round-trips byte-for-byte across nb/oa/da.
//!   * **pendingRepair** — this module does NOT walk this field (it sits after
//!     the version-gated improvedMinMax block + commitLogIntervals), so CQLite
//!     reports it honestly as `RepairField::Unparsed`. The lane asserts the
//!     authoritative reference is null (`Pending repair: --`) AND that CQLite
//!     reports `Unparsed` — never a fabricated `None` that would silently
//!     misreport a real pending-repair UUID as absent.
//!   * **isTransient** — likewise not walked; reported as
//!     `RepairField::Unparsed`. The lane asserts the reference is
//!     `IsTransient: false` AND that CQLite reports `Unparsed`.
//!   * **read -> report -> write round-trip** — the Statistics.db writer's
//!     persisted repair state (repairedAt=0 / pendingRepair=null /
//!     isTransient=false) is written and re-decoded through the read path and
//!     asserted field-exact (write-support feature only).
//!   * **negative tests** — synthetic in-memory byte mutation of the STATS
//!     component fails closed with an explicit error in strict mode.
//!
//! ============================================================================
//! PRESERVATION / REPORTING vs REPAIR-AWARE CORRECTNESS — read this.
//!
//! This lane validates *metadata preservation and reporting* ONLY. Decoding
//! `repairedAt` / `pendingRepair` / `isTransient` from `Statistics.db` proves
//! CQLite reports the persisted repair STATE accurately. It does NOT establish
//! repair-aware compaction or repair-aware tombstone purging correctness: this
//! lane makes no claim that tombstones are dropped (or retained) with respect to
//! a repair boundary, and asserts nothing about compaction's use of these
//! fields. Repair coordination, incremental-repair session tracking, and
//! repair-aware tombstone GC are explicitly out of scope (they live in the
//! compaction layer, not in persisted-metadata parse/report).
//! ============================================================================
//!
//! No-fixture states (honest classification): the Cassandra 5.0 corpus contains
//! NO repaired (`repairedAt>0`), pending-repair, or transient SSTable fixture.
//! Those *distinct* states are therefore covered by SYNTHETIC round-trip /
//! negative tests here and recorded in the manifest as `planned`
//! ("no Cassandra 5.0 fixture available"); this lane never fabricates reference
//! bytes for a repaired state in the strict real-fixture path.

use std::path::{Path, PathBuf};

use cqlite_core::parser::repair_metadata::{parse_repair_metadata, RepairField, RepairMetadata};
use cqlite_core::storage::sstable::version_gate::VersionGates;

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// Recursively collect every committed `*-Statistics.db.txt` reference dump,
/// skipping macOS AppleDouble shadow files (`._*`).
fn collect_statistics_txt(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            collect_statistics_txt(&path, out);
        } else if name.ends_with("-Statistics.db.txt") {
            out.push(path);
        }
    }
}

fn all_statistics_txt() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_statistics_txt(&root, &mut out);
    out.sort();
    // Fail closed: a broken reference path must turn the lane red, not green.
    assert!(
        !out.is_empty(),
        "no committed *-Statistics.db.txt references found under {} — repaired-metadata \
         parity cannot run (this is a fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// Lower-hex of a raw 16-byte UUID, for failure messages.
fn hex(uuid: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in uuid {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Derive the binary `*-Statistics.db` path from its `*-Statistics.db.txt` dump.
fn binary_for(txt: &Path) -> PathBuf {
    let name = txt
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 reference name: {}", txt.display()));
    let db_name = name
        .strip_suffix(".txt")
        .unwrap_or_else(|| panic!("reference not *.txt: {name}"));
    txt.with_file_name(db_name)
}

/// The repair-state fields parsed from an `sstablemetadata` reference dump.
/// Every value is sourced from the authoritative reference line — never
/// inferred from the file path.
#[derive(Debug)]
struct ReferenceRepairState {
    /// From `Repaired at: <n>`.
    repaired_at: i64,
    /// From `Pending repair: <uuid|-->` — `None` when `--` (null).
    pending_repair_is_null: bool,
    /// From `IsTransient: <bool>`.
    is_transient: bool,
}

fn parse_reference_repair_state(txt: &Path) -> ReferenceRepairState {
    let content = std::fs::read_to_string(txt)
        .unwrap_or_else(|e| panic!("read reference {} failed: {e}", txt.display()));

    let mut repaired_at = None;
    let mut pending_repair_is_null = None;
    let mut is_transient = None;

    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Repaired at:") {
            repaired_at = rest.trim().parse::<i64>().ok();
        } else if let Some(rest) = line.strip_prefix("Pending repair:") {
            // Cassandra renders a null pending-repair session as "--".
            pending_repair_is_null = Some(rest.trim() == "--");
        } else if let Some(rest) = line.strip_prefix("IsTransient:") {
            is_transient = match rest.trim() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            };
        }
    }

    ReferenceRepairState {
        repaired_at: repaired_at
            .unwrap_or_else(|| panic!("{}: missing `Repaired at:` line", txt.display())),
        pending_repair_is_null: pending_repair_is_null
            .unwrap_or_else(|| panic!("{}: missing `Pending repair:` line", txt.display())),
        is_transient: is_transient
            .unwrap_or_else(|| panic!("{}: missing `IsTransient:` line", txt.display())),
    }
}

/// Derive version gates from a `*-Statistics.db` path (authoritative descriptor
/// parse — the gates come from the parsed version, not a path heuristic).
fn gates_for(db: &Path) -> Option<VersionGates> {
    VersionGates::from_path(db).ok()
}

/// Strict repaired-metadata parity across every committed fixture.
///
/// Drives off the committed `*-Statistics.db.txt` references (always present);
/// for each, when the binary `*-Statistics.db` is fetched it decodes the repair
/// state from real bytes and compares against the reference. Absent binaries are
/// recorded as skips (never silent passes).
#[test]
fn repaired_metadata_strict_parity() {
    let refs = all_statistics_txt();

    let mut compared = 0usize;
    let mut skipped = 0usize;
    let mut format_nb = 0usize;
    let mut format_oa = 0usize;
    let mut format_da = 0usize;

    // Track which formats have a fetched binary on disk INDEPENDENTLY of the
    // compare count, so "binaries present yet 0 compared" fails the lane.
    let mut present_nb = false;
    let mut present_oa = false;
    let mut present_da = false;
    for txt in &refs {
        let db = binary_for(txt);
        if !db.exists() {
            continue;
        }
        match db.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => present_nb = true,
            Some(n) if n.starts_with("oa-") => present_oa = true,
            Some(n) if n.starts_with("da-") => present_da = true,
            _ => {}
        }
    }

    for txt in &refs {
        let db = binary_for(txt);
        if !db.exists() {
            skipped += 1;
            continue;
        }

        let reference = parse_reference_repair_state(txt);
        let bytes = std::fs::read(&db)
            .unwrap_or_else(|e| panic!("read binary {} failed: {e}", db.display()));

        let gates = gates_for(&db);
        let decoded = parse_repair_metadata(&bytes, gates.as_ref()).unwrap_or_else(|e| {
            panic!(
                "{}: CQLite failed to decode repair metadata from STATS component: {e:?}",
                db.display()
            )
        });

        // repairedAt is decoded from real bytes for every fixture.
        assert!(
            decoded.repaired_at_decoded,
            "{}: repairedAt should have been decoded from the STATS component, not defaulted",
            db.display(),
        );
        assert_eq!(
            decoded.repaired_at,
            reference.repaired_at,
            "{}: repairedAt mismatch (cqlite {} vs cassandra {})",
            db.display(),
            decoded.repaired_at,
            reference.repaired_at,
        );

        // The whole corpus is unrepaired; guard the precondition so that if a
        // repaired fixture is ever added, this lane is extended deliberately
        // rather than silently passing on a stale assumption.
        assert_eq!(
            reference.repaired_at,
            0,
            "{}: reference is NOT unrepaired (Repaired at: {}); the repaired-metadata lane \
             currently asserts the unrepaired state only — extend it before adding repaired fixtures",
            db.display(),
            reference.repaired_at,
        );

        // pendingRepair / isTransient: with authoritative version gates the read
        // path performs the version-gated walk (issue #1021). The walk decodes
        // these two fields FROM REAL BYTES whenever it can safely traverse the
        // min/max-clustering block — always for the legacy (`nb`) layout, and for
        // the improved (`oa`/`da`) layout only when the covered-clustering Slice
        // is empty (no clustering values). When the improved Slice carries
        // comparator-encoded values this decoder does not model, the walk stops
        // and reports the two fields honestly as `Unparsed`.
        //
        // So the valid outcomes are: the reference value DECODED, or `Unparsed`.
        // A fabricated NON-reference decoded value (e.g. a pending UUID or
        // isTransient=true when the reference is null/false) must FAIL.
        assert!(
            reference.pending_repair_is_null,
            "{}: reference Pending repair is NOT null — the corpus is expected \
             unrepaired; extend the lane before adding pending-repair fixtures",
            db.display(),
        );
        match decoded.pending_repair {
            RepairField::Unparsed => {}
            RepairField::Decoded(None) => {} // matches reference null
            RepairField::Decoded(Some(uuid)) => panic!(
                "{}: pendingRepair decoded a UUID {} but reference is null (--) — \
                 a fabricated/mis-decoded value",
                db.display(),
                hex(&uuid),
            ),
        }
        match decoded.is_transient {
            RepairField::Unparsed => {}
            RepairField::Decoded(v) => assert_eq!(
                v,
                reference.is_transient,
                "{}: isTransient decoded {} but reference is {}",
                db.display(),
                v,
                reference.is_transient,
            ),
        }

        // Idempotent re-decode: the read-side metadata API is stable across
        // repeated reads of the same bytes (read -> report preservation).
        let redecoded = parse_repair_metadata(&bytes, gates.as_ref())
            .unwrap_or_else(|e| panic!("{}: re-decode failed: {e:?}", db.display()));
        assert_eq!(
            decoded,
            redecoded,
            "{}: repair metadata not stable across re-decode",
            db.display(),
        );

        match db.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => format_nb += 1,
            Some(n) if n.starts_with("oa-") => format_oa += 1,
            Some(n) if n.starts_with("da-") => format_da += 1,
            _ => {}
        }
        compared += 1;
    }

    eprintln!(
        "repaired_metadata_strict_parity: {compared} compared, {skipped} skipped (binary absent) \
         | formats nb={format_nb} oa={format_oa} da={format_da} \
         | present nb={present_nb} oa={present_oa} da={present_da}"
    );

    let any_present = present_nb || present_oa || present_da;
    if !any_present {
        // Dataset-absent SKIP path (distinct from a silent pass); the committed
        // references were still validated for presence above.
        eprintln!(
            "repaired_metadata_strict_parity: SKIP — no *-Statistics.db binaries fetched \
             ({skipped} references present without binaries)"
        );
        return;
    }

    // Binaries present: the lane must actually have compared something, and must
    // have exercised every storage format the local dataset contains.
    assert!(
        compared > 0,
        "Statistics.db binaries are present (nb={present_nb} oa={present_oa} da={present_da}) \
         but zero fixtures were compared — repaired-metadata lane proved nothing"
    );
    if present_nb {
        assert!(
            format_nb > 0,
            "nb-* Statistics.db binaries present but none compared — nb repair parity unproven"
        );
    }
    if present_oa {
        assert!(
            format_oa > 0,
            "oa-* Statistics.db binaries present but none compared — oa repair parity unproven"
        );
    }
    if present_da {
        assert!(
            format_da > 0,
            "da-* Statistics.db binaries present but none compared — da repair parity unproven"
        );
    }
}

/// Negative tests (synthetic, in-memory byte mutation): malformed STATS-section
/// repair metadata and unsupported states fail closed with an explicit error in
/// strict mode — never silently accepted, never coerced to a placeholder.
///
/// These build a self-contained Statistics.db in memory (no fetched fixture),
/// so they run everywhere — including CI without datasets.
#[test]
fn repaired_metadata_negative_fails_closed() {
    // A minimal but valid Statistics.db with a TOC and a STATS component that is
    // self-describing through `repairedAt`.
    let clean = synthetic_statistics(/* repaired_at */ 0);

    // Baseline: the clean synthetic decodes repairedAt and reports the two
    // not-walked fields honestly as Unparsed (never a fabricated null / false).
    let md = parse_repair_metadata(&clean, None).expect("clean synthetic must decode");
    assert_eq!(md.repaired_at, 0);
    assert!(md.repaired_at_decoded);
    assert_eq!(md.pending_repair, RepairField::Unparsed);
    assert_eq!(md.is_transient, RepairField::Unparsed);

    // (a) Truncate inside the STATS body (past the trailing CRC) so the forward
    //     walk runs off the bounded component end.
    {
        let mut corrupt = clean.clone();
        // Drop the trailing CRC (4) plus the repairedAt i64 (8) so the bounded
        // STATS slice can no longer satisfy the repairedAt read.
        corrupt.truncate(corrupt.len() - (4 + 8));
        assert!(
            parse_repair_metadata(&corrupt, None).is_err(),
            "truncated STATS repair metadata must fail closed, not default"
        );
    }

    // (b) Corrupt the STATS TombstoneHistogram `size` to a huge value so the
    //     length-prefixed skip overruns the buffer → explicit corruption error.
    {
        let mut corrupt = clean.clone();
        let off = stats_offset(&corrupt);
        // Walk to the TombstoneHistogram `size` field and set it absurdly large.
        // STATS layout up to it: 2× EstimatedHistogram (empty: 4 bytes each)
        // + commitLogUpperBound (12) + min/maxTimestamp (16) + min/maxLDT (8)
        // + min/maxTTL (8) + compressionRatio (8) + maxBinSize (4) = 64 bytes,
        // then the `size` i32 at +64.
        let size_pos = off + 4 + 4 + 12 + 16 + 8 + 8 + 8 + 4;
        corrupt[size_pos..size_pos + 4].copy_from_slice(&i32::MAX.to_be_bytes());
        assert!(
            parse_repair_metadata(&corrupt, None).is_err(),
            "an over-large TombstoneHistogram size must overrun and fail closed"
        );
    }

    // (c) A negative EstimatedHistogram bucket count is an unsupported/malformed
    //     state and must be rejected explicitly.
    {
        let mut corrupt = clean.clone();
        let off = stats_offset(&corrupt);
        corrupt[off..off + 4].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(
            parse_repair_metadata(&corrupt, None).is_err(),
            "a negative EstimatedHistogram bucket count must fail closed"
        );
    }

    // (d) A STATS TOC offset past the end of the buffer must error, not panic or
    //     silently default.
    {
        let mut corrupt = clean.clone();
        // Rewrite the STATS TOC entry offset (type 2) to past EOF.
        let toc_entry_off = stats_toc_entry_offset(&corrupt);
        let bogus = (corrupt.len() as u32) + 1024;
        corrupt[toc_entry_off + 4..toc_entry_off + 8].copy_from_slice(&bogus.to_be_bytes());
        assert!(
            parse_repair_metadata(&corrupt, None).is_err(),
            "a STATS offset past EOF must fail closed"
        );
    }
}

/// Synthetic positive: a STATS component carrying a non-zero `repairedAt` (a
/// repaired-state SSTable, for which the corpus has NO fixture) is decoded
/// exactly from bytes. This covers the repaired DISTINCT state honestly via a
/// synthetic round-trip rather than fabricated reference bytes.
#[test]
fn repaired_metadata_synthetic_repaired_state_roundtrip() {
    let repaired_at: i64 = 1_700_000_000_000;
    let bytes = synthetic_statistics(repaired_at);
    let md = parse_repair_metadata(&bytes, None).expect("decode synthetic repaired state");
    assert_eq!(md.repaired_at, repaired_at);
    assert!(md.repaired_at_decoded);
    // pendingRepair / isTransient are not walked — reported honestly as
    // Unparsed. This synthetic exercises the repaired-at DISTINCT state only
    // (see module docs).
    assert_eq!(md.pending_repair, RepairField::Unparsed);
    assert_eq!(md.is_transient, RepairField::Unparsed);
}

/// read -> report -> write round-trip through the real Statistics.db writer.
///
/// The writer persists the unrepaired repair state (repairedAt=0,
/// pendingRepair=null, isTransient=false). We write a real Statistics.db, decode
/// the repair metadata back through the read path, and assert it is preserved
/// field-exact: repairedAt round-trips to 0 (decoded), and the two not-walked
/// fields are reported honestly as `Unparsed` (the read path makes no claim to
/// have decoded the null / false the writer persisted). This proves the
/// persisted repairedAt survives a write->read round-trip.
///
/// SCOPE: this proves the writer's persisted repair STATE round-trips through
/// the read path; it does NOT prove repair-aware compaction/tombstone behaviour
/// (see the module-level preservation-vs-repair-aware note).
#[cfg(feature = "write-support")]
#[test]
fn repaired_metadata_writer_roundtrip_preserves_unrepaired_state() {
    use cqlite_core::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};

    // Unique per-invocation dir (held to end of test) — a fixed shared
    // `env::temp_dir()` path races the file out from under the read when the
    // full test suite runs binaries in parallel (pre-existing flake).
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let stats_path = dir.path().join("nb-1-big-Statistics.db");

    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1_000_000);
    meta.update_timestamp(2_000_000);
    meta.increment_partition_count();
    meta.row_count = 10;

    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta, None).expect("write Statistics.db");

    let written = std::fs::read(&stats_path).expect("read written Statistics.db");

    // Decode the repair state back through the read path. The writer emits the
    // legacy (nb) tombstone-histogram width, matching the `None`-gates default.
    let decoded = parse_repair_metadata(&written, None).expect("decode written repair metadata");

    assert!(
        decoded.repaired_at_decoded,
        "writer round-trip: repairedAt should be decoded from the written STATS bytes"
    );
    assert_eq!(
        decoded,
        RepairMetadata {
            repaired_at: 0,
            // The writer's persisted null / false state is NOT walked by the
            // read path WITHOUT gates, so it is reported honestly as Unparsed
            // rather than a fabricated decoded value.
            pending_repair: RepairField::Unparsed,
            is_transient: RepairField::Unparsed,
            repaired_at_decoded: true,
        },
        "writer-persisted unrepaired repair state must round-trip field-exact \
         (repairedAt decoded; pending_repair / is_transient reported as Unparsed)"
    );

    // With authoritative nb gates the full walk decodes the writer's persisted
    // null / false state from real bytes (issue #1021).
    let gates = VersionGates::Big(
        cqlite_core::storage::sstable::version_gate::BigVersionGates::from_version("nb")
            .expect("nb gates"),
    );
    let decoded_full =
        parse_repair_metadata(&written, Some(&gates)).expect("decode written repair metadata");
    assert_eq!(
        decoded_full,
        RepairMetadata {
            repaired_at: 0,
            pending_repair: RepairField::Decoded(None),
            is_transient: RepairField::Decoded(false),
            repaired_at_decoded: true,
        },
        "writer-persisted unrepaired state must decode null / false through the \
         full version-gated walk"
    );

    let _ = std::fs::remove_file(&stats_path);
}

/// Compaction-preservation round-trip (issue #1021 AC2): a `StatisticsMetadata`
/// carrying a NON-default repair state (repairedAt != 0, a pendingRepair UUID,
/// isTransient = true — the state the compaction merge path preserves from
/// compatible inputs) is written by the real Statistics.db writer and decoded
/// back through the full version-gated read walk field-exact.
///
/// This proves the writer→read repair-state round-trip for the repaired /
/// pending / transient DISTINCT states for which the Cassandra 5.0 corpus has NO
/// fixture (so this is the provable, honest coverage of those states), and it is
/// the byte-level mechanism the compaction output relies on to carry repair
/// metadata forward.
#[cfg(feature = "write-support")]
#[test]
fn repaired_state_writer_roundtrip_preserves_repaired_pending_transient() {
    use cqlite_core::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};

    // Unique per-invocation dir (held to end of test) — see the sibling
    // roundtrip test; a fixed shared `env::temp_dir()` path races under the
    // parallel suite (pre-existing flake).
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let stats_path = dir.path().join("nb-1-big-Statistics.db");

    let repaired_at: i64 = 1_700_000_000_000;
    let pending_uuid: [u8; 16] = [
        0x9a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x82, 0x93, 0xa4, 0xb5, 0xc6, 0xd7, 0xe8,
        0xf9,
    ];

    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1_000_000);
    meta.update_timestamp(2_000_000);
    meta.increment_partition_count();
    meta.row_count = 3;
    meta.set_repair_state(repaired_at, Some(pending_uuid), true);

    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta, None).expect("write Statistics.db");

    let written = std::fs::read(&stats_path).expect("read written Statistics.db");

    let gates = VersionGates::Big(
        cqlite_core::storage::sstable::version_gate::BigVersionGates::from_version("nb")
            .expect("nb gates"),
    );
    let decoded =
        parse_repair_metadata(&written, Some(&gates)).expect("decode preserved repair state");

    assert_eq!(
        decoded,
        RepairMetadata {
            repaired_at,
            pending_repair: RepairField::Decoded(Some(pending_uuid)),
            is_transient: RepairField::Decoded(true),
            repaired_at_decoded: true,
        },
        "the writer must preserve a non-default repair state (repairedAt / \
         pendingRepair UUID / isTransient) byte-exact through a write→read round-trip"
    );

    let _ = std::fs::remove_file(&stats_path);
}

// ---------------------------------------------------------------------------
// Synthetic Statistics.db builders (in-memory; used by negative + synthetic
// positive tests so they run without a fetched fixture).
// ---------------------------------------------------------------------------

/// Fixed-size TOC region for `synthetic_statistics`: 4 components, STATS placed
/// LAST by offset (its end derives from `file_len - trailing CRC`).
const SYNTH_TOC_LEN: usize = 4 + 4 + 4 * 8; // count + marker + 4 entries
/// Three one-byte placeholder component bodies precede the STATS body.
const SYNTH_PLACEHOLDER_BODIES: usize = 3;

/// Byte offset of the STATS component body within a `synthetic_statistics`
/// buffer (after the TOC region and the three placeholder component bodies).
fn stats_offset(_bytes: &[u8]) -> usize {
    SYNTH_TOC_LEN + SYNTH_PLACEHOLDER_BODIES
}

/// Byte offset of the STATS (type 2) entry within the TOC of a
/// `synthetic_statistics` buffer. STATS is the 3rd entry (index 2).
fn stats_toc_entry_offset(_bytes: &[u8]) -> usize {
    let toc_start = 8; // after count + marker
    toc_start + 2 * 8
}

/// Build a minimal valid Statistics.db (TOC + STATS component through
/// `repairedAt`) carrying the given `repaired_at`. The STATS leading fields are
/// all self-describing, so the read-path decoder can walk to `repairedAt`.
///
/// STATS is placed as the LAST component (by offset), with three one-byte
/// placeholder component bodies preceding it and a trailing 4-byte metadata CRC,
/// so the decoder's component-end bound resolves to `file_len - CRC`.
fn synthetic_statistics(repaired_at: i64) -> Vec<u8> {
    // --- STATS body ---
    let mut stats = Vec::new();
    // 1-2. Two empty EstimatedHistograms (bucket count 0).
    stats.extend_from_slice(&0i32.to_be_bytes());
    stats.extend_from_slice(&0i32.to_be_bytes());
    // 3. commitLogUpperBound: i64 segmentId + i32 position.
    stats.extend_from_slice(&(-1i64).to_be_bytes());
    stats.extend_from_slice(&0i32.to_be_bytes());
    // 4. minTimestamp, maxTimestamp.
    stats.extend_from_slice(&100i64.to_be_bytes());
    stats.extend_from_slice(&200i64.to_be_bytes());
    // 5. min/maxLocalDeletionTime (8 bytes total).
    stats.extend_from_slice(&i32::MAX.to_be_bytes());
    stats.extend_from_slice(&i32::MAX.to_be_bytes());
    // 6. minTTL, maxTTL.
    stats.extend_from_slice(&0i32.to_be_bytes());
    stats.extend_from_slice(&0i32.to_be_bytes());
    // 7. compressionRatio.
    stats.extend_from_slice(&(-1.0f64).to_be_bytes());
    // 8. TombstoneHistogram: empty (maxBinSize=0, size=0).
    stats.extend_from_slice(&0i32.to_be_bytes());
    stats.extend_from_slice(&0i32.to_be_bytes());
    // 9. sstableLevel, repairedAt.
    stats.extend_from_slice(&0i32.to_be_bytes());
    stats.extend_from_slice(&repaired_at.to_be_bytes());

    // --- TOC: 4 components, STATS (type 2) last by offset ---
    let comp0_off = SYNTH_TOC_LEN;
    let comp1_off = comp0_off + 1;
    let comp3_off = comp1_off + 1;
    let stats_off = comp3_off + 1; // == stats_offset()
    let mut out = Vec::new();
    out.extend_from_slice(&4u32.to_be_bytes()); // num components
    out.extend_from_slice(&0u32.to_be_bytes()); // marker (unused by repair decoder)
    for (ty, off) in [
        (0u32, comp0_off as u32),
        (1u32, comp1_off as u32),
        (2u32, stats_off as u32), // STATS (last by offset)
        (3u32, comp3_off as u32),
    ] {
        out.extend_from_slice(&ty.to_be_bytes());
        out.extend_from_slice(&off.to_be_bytes());
    }
    out.extend_from_slice(&[0u8; SYNTH_PLACEHOLDER_BODIES]); // placeholder bodies
    assert_eq!(out.len(), stats_off);
    out.extend_from_slice(&stats);
    out.extend_from_slice(&0u32.to_be_bytes()); // trailing metadata CRC
    out
}
