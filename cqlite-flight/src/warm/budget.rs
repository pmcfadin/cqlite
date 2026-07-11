//! Memory budget + LRU eviction accounting (issue #2310, WS4 #2343).
//!
//! Decision 4 (design.md) + spec Requirement 5: warm state is bounded by an
//! explicit byte budget inside the <128MB discipline, accounting the parsed-state
//! footprint (per-generation Index/Summary/Statistics/bloom) EXPLICITLY — not by
//! proxy — and evicting least-recently-used (table, generation) entries when the
//! budget would be exceeded. A generation removed on disk is evicted immediately
//! regardless of LRU age (handled in the registry rebuild).

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
/// The warm reader keeps resident, format-specific state per generation:
/// * **BIG** (`na`/`nb`) — Index/Summary/Statistics/bloom-filter.
/// * **BTI** (`oa`/`da`) — `SSTableReader::open` loads its `Partitions.db` +
///   `Rows.db` tries FULLY into memory (issue #831/#909, the BTI point-lookup
///   structures) INSTEAD of Index.db/Summary.db, plus the same
///   Statistics/bloom-filter sidecars.
///
/// Those parsed structures derive directly from the sibling component files,
/// so the sum of their on-disk sizes is an EXPLICIT, auditable accounting of
/// the resident footprint (never a heuristic guess). Rather than branching on
/// a detected format, this simply checks the UNION of both formats' sidecar
/// suffixes and sums whichever ones actually exist on disk (`std::fs::metadata`
/// naturally returns 0 contribution for the ones the OTHER format doesn't
/// have) — honest for both formats without needing a separate version-gate
/// parse. Computed by `stat` only (no extra parse) at open time. A fixed
/// overhead covers the parsed-schema handle and registry bookkeeping.
pub fn account_footprint(data_path: &Path) -> u64 {
    let name = match data_path.file_name().and_then(|n| n.to_str()) {
        Some(n) if n.ends_with("-Data.db") => n,
        // A path that is not a `-Data.db` (should not happen) accounts as the
        // fixed overhead alone rather than fabricating a size.
        _ => return PER_GENERATION_OVERHEAD_BYTES,
    };
    let mut total = PER_GENERATION_OVERHEAD_BYTES;
    for component in [
        // BIG (na/nb) resident sidecars.
        "-Index.db",
        "-Summary.db",
        // BTI (oa/da) resident sidecars (issue #2310 roborev 1640): BTI has no
        // Index.db/Summary.db, so these contribute 0 for a BIG generation and
        // vice versa — the union honestly covers both formats.
        "-Partitions.db",
        "-Rows.db",
        // Shared by both formats.
        "-Statistics.db",
        "-Filter.db",
    ] {
        let sibling = data_path.with_file_name(name.replace("-Data.db", component));
        if let Ok(md) = std::fs::metadata(&sibling) {
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
    fn non_data_path_accounts_overhead_only() {
        assert_eq!(
            account_footprint(Path::new("/tmp/not-an-sstable.txt")),
            PER_GENERATION_OVERHEAD_BYTES
        );
    }
}
