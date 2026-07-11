//! Memory budget + LRU eviction accounting (issue #2310, WS4 #2343).
//!
//! Decision 4 (design.md) + spec Requirement 5: warm state is bounded by an
//! explicit byte budget inside the <128MB discipline, accounting the
//! per-generation parsed-state footprint EXPLICITLY — not by proxy — and
//! evicting least-recently-used (table, generation) entries when the budget
//! would be exceeded. A generation removed on disk is evicted immediately
//! regardless of LRU age (handled in the registry rebuild).
//!
//! [`account_footprint`] is EXHAUSTIVE BY CONSTRUCTION (issue #2310, roborev
//! 1641): every sibling component sharing the generation's filename prefix is
//! summed, with the single documented exception of `Data.db` (paged, not
//! parsed-resident) — see its doc for the full rationale. This covers BIG,
//! BTI, compressed, and uncompressed generations without a hardcoded
//! per-component suffix list to keep extending every time a new component
//! (Partitions/Rows, CRC, CompressionInfo, ...) turns out to matter.

use std::path::Path;

/// The FIXED named warm-state byte budget (Decision 4 / design open-question 2:
/// a fixed named default this epic, NO new user knob/env/ticket field). Sits well
/// inside the project-wide <128MB memory discipline while leaving ample headroom
/// for the merge pipeline's own working set (channels, batches, producer
/// threads). Not configurable by design — revisit only if the field needs it.
pub const DEFAULT_WARM_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// A small fixed per-generation bookkeeping overhead added to the accounted
/// component footprint (parsed schema handle, reader struct, per-generation
/// registry bookkeeping). Keeps the accounting from under-counting tiny SSTables
/// to ~0 and pinning an unbounded COUNT of them.
const PER_GENERATION_OVERHEAD_BYTES: u64 = 64 * 1024;

/// Explicitly account the parsed-state footprint of the generation backing
/// `data_path`.
///
/// EXHAUSTIVE BY CONSTRUCTION (issue #2310, roborev 1641 — ends the
/// hardcoded-suffix-list churn for good): rather than enumerating named
/// sidecars (Index/Summary/Statistics/Filter/Partitions/Rows/CRC/
/// CompressionInfo/...), this sums the on-disk size of EVERY file in the
/// generation's directory that shares its filename PREFIX — i.e. every
/// sibling component — with exactly ONE exclusion: `Data.db` itself.
///
/// Why exclude only `Data.db`: it is the one component the reader does NOT
/// keep fully parsed/resident — it is PAGED (mmap'd or positionally read), so
/// its size does not reflect memory pressure the way a fully-loaded sidecar's
/// does. Every OTHER component that exists for a generation — whichever
/// format, whichever compression setting — genuinely gets `stat`ed and (for
/// the ones the reader actually loads: Index/Summary/Statistics/bloom for
/// BIG; Partitions/Rows tries for BTI, issue #831/#909) parsed/held resident,
/// or is at minimum a small, format-agnostic sidecar (CRC.db, which can
/// DOMINATE on an uncompressed BIG table; CompressionInfo.db; Digest;
/// TOC.txt) whose bytes are cheap to include and NEVER wrong to over-count
/// slightly (the byte budget is meant to be a defensible upper bound, not a
/// razor-thin one). This is honest for BIG, BTI, compressed, and
/// uncompressed generations alike, and needs no future round when a new
/// component is added — it is accounted automatically by construction.
///
/// Computed by `read_dir` + `stat` only (no extra parse) at open time. A
/// fixed overhead covers the parsed-schema handle and registry bookkeeping.
pub fn account_footprint(data_path: &Path) -> u64 {
    let name = match data_path.file_name().and_then(|n| n.to_str()) {
        Some(n) if n.ends_with("-Data.db") => n,
        // A path that is not a `-Data.db` (should not happen) accounts as the
        // fixed overhead alone rather than fabricating a size.
        _ => return PER_GENERATION_OVERHEAD_BYTES,
    };
    let base = &name[..name.len() - "-Data.db".len()];
    let Some(parent) = data_path.parent() else {
        return PER_GENERATION_OVERHEAD_BYTES;
    };
    let Ok(read) = std::fs::read_dir(parent) else {
        return PER_GENERATION_OVERHEAD_BYTES;
    };
    let prefix = format!("{base}-");
    let mut total = PER_GENERATION_OVERHEAD_BYTES;
    for entry in read.flatten() {
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        if entry_name == name {
            // The single documented exclusion: Data.db is paged, not
            // parsed-resident.
            continue;
        }
        if !entry_name.starts_with(&prefix) {
            // Not a sibling component of THIS generation (a different
            // generation's files never share this exact byte prefix — the
            // generation-number/format-tag segments always differ before the
            // trailing hyphen).
            continue;
        }
        if let Ok(md) = std::fs::metadata(entry.path()) {
            total = total.saturating_add(md.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_is_inside_the_128mb_discipline() {
        assert!(
            DEFAULT_WARM_BUDGET_BYTES < 128 * 1024 * 1024,
            "warm budget must sit inside the <128MB discipline"
        );
    }

    #[test]
    fn footprint_sums_present_components_plus_overhead() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("nb-1-big-Data.db");
        std::fs::write(&data, b"data").unwrap();
        std::fs::write(dir.path().join("nb-1-big-Index.db"), vec![0u8; 100]).unwrap();
        std::fs::write(dir.path().join("nb-1-big-Summary.db"), vec![0u8; 50]).unwrap();
        // Statistics.db / Filter.db absent → contribute 0.
        let footprint = account_footprint(&data);
        assert_eq!(footprint, PER_GENERATION_OVERHEAD_BYTES + 150);
    }

    #[test]
    fn footprint_includes_bti_partitions_and_rows_sidecars() {
        // Finding 2 (#2310, roborev 1640): a BTI (da) generation's accounted
        // footprint must include Partitions.db + Rows.db — the tries
        // `SSTableReader::open` loads FULLY into memory for BTI, replacing
        // Index.db/Summary.db (issue #831/#909). Red on the pre-fix BIG-only
        // component list: the two BTI sidecars are silently uncounted, so a
        // BTI table's LRU budget cap is defeated by an undercount.
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("da-1-bti-Data.db");
        std::fs::write(&data, b"data").unwrap();
        std::fs::write(dir.path().join("da-1-bti-Partitions.db"), vec![0u8; 200]).unwrap();
        std::fs::write(dir.path().join("da-1-bti-Rows.db"), vec![0u8; 75]).unwrap();
        // Statistics.db / Filter.db absent → contribute 0. No Index.db/Summary.db
        // for BTI (by design, so they must NOT spuriously contribute either).
        let footprint = account_footprint(&data);
        assert_eq!(
            footprint,
            PER_GENERATION_OVERHEAD_BYTES + 275,
            "BTI Partitions.db + Rows.db must be accounted, not silently dropped"
        );
    }

    #[test]
    fn footprint_includes_crc_and_compression_info_sidecars() {
        // Finding 1 (#2310, roborev 1641): CRC.db + CompressionInfo.db are ALSO
        // materialized by `SSTableReader::open` — CRC.db in particular can
        // DOMINATE on an uncompressed BIG table. Exhaustive-by-construction
        // accounting must include them without a named-suffix-list entry. Red
        // on the pre-fix hardcoded list (Index/Summary/Statistics/Filter only,
        // even after round 4's BTI extension): CRC.db's bytes are silently
        // dropped, undercounting an uncompressed-BIG generation's footprint.
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("nb-1-big-Data.db");
        std::fs::write(&data, b"data").unwrap();
        std::fs::write(dir.path().join("nb-1-big-CRC.db"), vec![0u8; 300]).unwrap();
        std::fs::write(
            dir.path().join("nb-1-big-CompressionInfo.db"),
            vec![0u8; 40],
        )
        .unwrap();
        let footprint = account_footprint(&data);
        assert_eq!(
            footprint,
            PER_GENERATION_OVERHEAD_BYTES + 340,
            "CRC.db + CompressionInfo.db must be accounted, not silently dropped"
        );
    }

    #[test]
    fn footprint_excludes_only_data_db_itself() {
        // The single documented exclusion (issue #2310, roborev 1641): Data.db
        // is paged, not parsed-resident, so its (potentially large) size must
        // NEVER contribute — while a sibling of any name still does.
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("nb-1-big-Data.db");
        std::fs::write(&data, vec![0u8; 10_000]).unwrap(); // large Data.db
        std::fs::write(dir.path().join("nb-1-big-TOC.txt"), vec![0u8; 10]).unwrap();
        let footprint = account_footprint(&data);
        assert_eq!(
            footprint,
            PER_GENERATION_OVERHEAD_BYTES + 10,
            "Data.db's 10_000 bytes must be excluded; only the TOC.txt sibling counts"
        );
    }

    #[test]
    fn footprint_does_not_leak_a_different_generations_components() {
        // A generation whose number is a byte-prefix of another's (1 vs 10, 12,
        // ...) must never absorb the OTHER generation's sidecars — the
        // trailing hyphen in the match prefix prevents the collision.
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("nb-1-big-Data.db");
        std::fs::write(&data, b"data").unwrap();
        std::fs::write(dir.path().join("nb-1-big-Index.db"), vec![0u8; 50]).unwrap();
        // A DIFFERENT generation (12) whose components must not leak in.
        std::fs::write(dir.path().join("nb-12-big-Data.db"), b"data").unwrap();
        std::fs::write(dir.path().join("nb-12-big-Index.db"), vec![0u8; 9_999]).unwrap();
        let footprint = account_footprint(&data);
        assert_eq!(
            footprint,
            PER_GENERATION_OVERHEAD_BYTES + 50,
            "generation 12's Index.db must not leak into generation 1's footprint"
        );
    }

    #[test]
    fn non_data_path_accounts_overhead_only() {
        assert_eq!(
            account_footprint(Path::new("/tmp/not-an-sstable.txt")),
            PER_GENERATION_OVERHEAD_BYTES
        );
    }
}
