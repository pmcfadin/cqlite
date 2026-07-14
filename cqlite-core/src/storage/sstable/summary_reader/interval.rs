//! Token-ordered `Summary.db` find-by-key + the lazy `Index.db` interval accessor
//! for the Summary-guided BIG partition index (issue #2412, design §A/§B).
//!
//! Two primitives that make BIG open Cassandra-lazy:
//!
//! - [`find_interval_for_key`] binary-searches the token-ordered `Summary.db` samples
//!   for the half-open `Index.db` byte range [`SummaryInterval`] that must contain a
//!   query key's partition entry (the core new search; [`super::SummaryReader::find_by_key`]
//!   is the method wrapper).
//! - [`lookup_key_in_interval`] reads **only that one interval** from disk and
//!   resolves the exact key, recording exactly one **interval** parse
//!   (`cqlite.sstable.index_interval_parses_total`) — never a full parse, so a
//!   lazy-open regression stays visible on `index_parses_total` (design §F). An
//!   END-BOUNDED interval is scanned in full (its `[start, end)` byte range between
//!   two real samples is the bound, not an entry count — Cassandra's `Summary.db`
//!   downsampling can widen it well past `min_index_interval`); only the read-to-EOF
//!   LAST interval keeps a downsampling-adjusted entry cap (roborev job 1709).
//!
//! No-heuristics (issue #28): every seek offset and interval boundary is an
//! authoritative `Summary.db` sample position; entry framing is the on-disk `Index.db`
//! structure. Nothing is inferred from value bytes.

use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::SummaryEntry;
use crate::error::Result;
use crate::storage::sstable::index_reader::{parse_big_index_entry, PartitionIndexEntry};

/// A `Summary.db`-derived half-open byte range `[start_position, end_position)` into
/// `Index.db` that must contain a query key's partition entry (issue #2412).
///
/// Produced by [`super::SummaryReader::find_by_key`]. `end_position == None` means
/// "scan to EOF" (the covering sample is the last one). Both bounds are authoritative
/// summary sample positions — the interval is structural, never a guessed boundary (#28).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SummaryInterval {
    /// Inclusive start byte offset into `Index.db` (the covering sample's position).
    pub start_position: u64,
    /// Exclusive end byte offset into `Index.db` (the next sample's position), or
    /// `None` to read to EOF when the covering sample is the last one.
    pub end_position: Option<u64>,
    /// Index of the covering (floor) sample within [`super::SummaryReader::get_entries`].
    pub sample_index: usize,
}

/// Token-ordered binary search over `Summary.db` samples for the `Index.db` interval
/// covering `key` (issue #2412). Pure core of [`super::SummaryReader::find_by_key`],
/// factored out so it is unit-testable without a `Platform`/file open.
///
/// The samples are in Cassandra **token order** (murmur3 of the raw partition key,
/// ties broken by raw bytes), NOT lexicographic byte order, so the search compares
/// with `cmp_partition_keys_by_token` — comparing raw bytes would be wrong for any
/// non-degenerate ring. Returns the half-open `[start, end)`: `start` is the greatest
/// sample whose key is `<= key` (Cassandra's `getPosition` floor); `end` is the next
/// sample's position or `None` (EOF) for the last sample. A key below the first sample
/// clamps the floor to sample 0 (Cassandra walks from the index start); the C5 range
/// short-circuit already answers genuinely out-of-`[first_key,last_key]` reads upstream
/// with zero probe work. Returns `None` only for an empty summary.
pub(crate) fn find_interval_for_key(
    entries: &[SummaryEntry],
    key: &[u8],
) -> Option<SummaryInterval> {
    use crate::util::cassandra_murmur3::cmp_partition_keys_by_token;
    use std::cmp::Ordering;

    if entries.is_empty() {
        return None;
    }

    // Count of samples whose key is <= `key` in token order (monotone prefix).
    let le_count = entries.partition_point(|e| {
        cmp_partition_keys_by_token(&e.partition_key, key) != Ordering::Greater
    });
    // Floor index: greatest sample <= key, clamped to 0 when key sorts below all.
    let floor = le_count.saturating_sub(1);

    Some(SummaryInterval {
        start_position: entries[floor].position,
        end_position: entries.get(floor + 1).map(|e| e.position),
        sample_index: floor,
    })
}

/// Result of resolving a key within one Summary-bounded `Index.db` interval.
#[derive(Debug)]
pub struct IntervalLookup {
    /// The matched partition entry, or `None` when the key is genuinely absent from
    /// this interval (an authoritative absence between two summary samples — the
    /// whole-file `scan_for_key` oracle is NOT needed for the common case, design §B).
    pub entry: Option<PartitionIndexEntry>,
    /// Number of `Index.db` entries actually parsed/touched, bounded by ONE summary
    /// interval's actual entry count (an end-bounded interval's `[start, end)` byte
    /// range; a downsampling-adjusted cap for the read-to-EOF last interval). The
    /// scale-free work-probe for Requirement 2 — bounded by the interval, never by
    /// the SSTable's total partition count.
    pub entries_touched: usize,
}

/// Cassandra's `IndexSummary` base (non-downsampled) sampling level — sample
/// spacing is `min_index_interval` partitions ONLY at this level; a lower
/// `sampling_level` widens spacing proportionally (`IndexSummary.java`,
/// `BASE_SAMPLING_LEVEL = 128`). Duplicated from the (feature-gated,
/// write-support-only) writer-side constant of the same name and value so this
/// read-path module never depends on `write-support` being enabled.
const BASE_SAMPLING_LEVEL: u32 = 128;

/// Cap on entries scanned in the LAST (read-to-EOF) `Index.db` interval —
/// roborev job 1709 (High, data-loss class): the cap must be DOWNSAMPLING-AWARE.
///
/// Cassandra downsamples `Summary.db` under index-summary memory pressure
/// (`sampling_level < BASE_SAMPLING_LEVEL`), which widens EVERY interval —
/// including the last — up to `min_index_interval * BASE_SAMPLING_LEVEL /
/// sampling_level` partitions, not `min_index_interval`. A cap fixed at
/// `min_index_interval + 1` (the pre-fix behavior) would cut a downsampled last
/// interval's scan short before reaching a present key that legitimately sits
/// beyond the un-downsampled baseline. This is the ONLY cap that survives this
/// fix (see [`scan_interval_bytes`]'s caller): a `Some(end_position)` interval
/// is bounded by its authoritative `[start, end)` byte range instead, so no
/// entry-count cap is needed there at all.
///
/// `sampling_level` is clamped to Cassandra's documented `[1,
/// BASE_SAMPLING_LEVEL]` range (`IndexSummary.java`) before dividing: `0`
/// (corrupt/never-should-happen) clamps to `1` — the MAXIMUM legitimate
/// widening, the fail-safe direction for a cap (a too-generous cap only costs
/// extra scan work; a too-small one risks exactly this issue's false-negative).
/// A value `> BASE_SAMPLING_LEVEL` (equally never-should-happen) clamps to
/// `BASE_SAMPLING_LEVEL`, i.e. the un-downsampled baseline — never a guess,
/// always derived from the already-parsed header fields (no-heuristics, #28).
fn last_interval_entry_cap(min_index_interval: u32, sampling_level: u32) -> usize {
    let effective_sampling_level = sampling_level.clamp(1, BASE_SAMPLING_LEVEL);
    let effective_interval = (min_index_interval as u64).saturating_mul(BASE_SAMPLING_LEVEL as u64)
        / (effective_sampling_level as u64);
    // +1 covers the boundary entry that sits exactly at EOF.
    usize::try_from(effective_interval.saturating_add(1)).unwrap_or(usize::MAX)
}

/// Scan a slice of `Index.db` interval bytes for the entry whose key equals `key`.
///
/// Pure core of [`lookup_key_in_interval`] (unit-testable without a file). Parses
/// forward with [`parse_big_index_entry`] up to `entry_cap` entries, stopping at the
/// first exact key match, at buffer exhaustion, at the entry cap, or at the first
/// unparseable entry (a truncated boundary tail — treated as end-of-interval).
pub(crate) fn scan_interval_bytes(bytes: &[u8], key: &[u8], entry_cap: usize) -> IntervalLookup {
    let mut remaining = bytes;
    let mut entries_touched = 0usize;

    while !remaining.is_empty() && entries_touched < entry_cap {
        match parse_big_index_entry(remaining) {
            Ok((rest, entry)) => {
                // Forward-progress guard mirrors the full parser's debug_assert.
                if rest.len() >= remaining.len() {
                    break;
                }
                entries_touched += 1;
                if entry.key_digest.as_ref() == key {
                    return IntervalLookup {
                        entry: Some(entry),
                        entries_touched,
                    };
                }
                remaining = rest;
            }
            // Unparseable tail at the interval boundary — end of this interval's whole
            // entries (never a heuristic: the boundary is an authoritative summary sample).
            Err(_) => break,
        }
    }

    IntervalLookup {
        entry: None,
        entries_touched,
    }
}

/// Read one Summary-bounded `Index.db` interval from disk and resolve `key` within it
/// (issue #2412, design §B).
///
/// Seeks to `interval.start_position`, reads the interval bytes (`[start, end)`, or to
/// EOF when `interval.end_position` is `None`), and scans forward for the exact key.
/// Increments `cqlite.sstable.index_interval_parses_total` exactly once (one bounded
/// interval parse), never `index_parses_total` (design §F).
///
/// Entry-count cap (roborev job 1709, High — downsampling false-negative fix):
/// an END-BOUNDED interval (`end_position.is_some()`) is NOT entry-capped at all —
/// its authoritative `[start, end)` byte range, delimited by two REAL adjacent
/// `Summary.db` samples, already bounds the read; every whole entry the buffer
/// holds is scanned regardless of Cassandra's downsampling (which can widen an
/// interval to `min_index_interval * 128 / sampling_level` partitions — a fixed
/// `min_index_interval`-derived cap could otherwise stop short of a present key
/// and be (wrongly) treated as an authoritative absence by the caller,
/// `big_get_with_resolution`'s `covering_interval_is_end_bounded` gate). The
/// read-to-EOF LAST interval keeps a cap — [`last_interval_entry_cap`], derived
/// from the SAME downsampling-adjusted formula — because `covering_interval_is_end_bounded`
/// is `false` for it, so a capped miss there is never promoted to an authoritative
/// absence (the caller falls back to `scan_for_key` instead, preserving #1572).
pub async fn lookup_key_in_interval(
    index_path: &Path,
    interval: SummaryInterval,
    key: &[u8],
    min_index_interval: u32,
    sampling_level: u32,
) -> Result<IntervalLookup> {
    let mut file = File::open(index_path).await?;
    file.seek(std::io::SeekFrom::Start(interval.start_position))
        .await?;

    let mut buffer = Vec::new();
    let entry_cap = match interval.end_position {
        Some(end) if end > interval.start_position => {
            // Bounded read of exactly the interval's whole entries. The byte
            // range itself is the bound — no entry-count cap needed or applied.
            let len = (end - interval.start_position) as usize;
            buffer.resize(len, 0);
            file.read_exact(&mut buffer).await?;
            usize::MAX
        }
        // Last interval (or a degenerate empty/zero-width range): read to EOF.
        // The scan is bounded by the downsampling-adjusted
        // `last_interval_entry_cap`, so this can never become a whole-file walk
        // for a many-partition SSTable.
        _ => {
            file.read_to_end(&mut buffer).await?;
            last_interval_entry_cap(min_index_interval, sampling_level)
        }
    };

    // One bounded interval parse (distinct counter — never a full parse, design §F).
    crate::observability::add_counter(
        crate::observability::catalog::INDEX_INTERVAL_PARSES_TOTAL,
        1,
        &[],
    );

    Ok(scan_interval_bytes(&buffer, key, entry_cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- find_interval_for_key (token-ordered search) ----

    /// Build token-ordered summary samples from arbitrary raw keys (issue #2412). The
    /// `Summary.db` on disk is written in token order; a test mirrors that by sorting
    /// with the same comparator `find_interval_for_key` searches with and assigning
    /// strictly increasing `Index.db` positions.
    fn token_ordered_samples(keys: &[&[u8]]) -> Vec<SummaryEntry> {
        use crate::util::cassandra_murmur3::cmp_partition_keys_by_token;
        let mut keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        keys.sort_by(|a, b| cmp_partition_keys_by_token(a, b));
        keys.into_iter()
            .enumerate()
            .map(|(i, partition_key)| SummaryEntry {
                partition_key,
                position: (i as u64) * 128,
            })
            .collect()
    }

    #[test]
    fn find_by_key_empty_summary_returns_none() {
        assert_eq!(find_interval_for_key(&[], b"anything"), None);
    }

    #[test]
    fn find_by_key_present_sample_bounds_one_interval() {
        // Requirement 2: a present sample key resolves to the half-open interval
        // [its position, next sample's position).
        let samples = token_ordered_samples(&[b"aaaa", b"bbbb", b"cccc", b"dddd"]);
        for i in 0..samples.len() {
            let iv = find_interval_for_key(&samples, &samples[i].partition_key)
                .expect("present key must resolve an interval");
            assert_eq!(iv.sample_index, i, "floor must be the exact sample");
            assert_eq!(iv.start_position, samples[i].position);
            assert_eq!(
                iv.end_position,
                samples.get(i + 1).map(|e| e.position),
                "end is next sample position, or None (EOF) for the last sample"
            );
        }
    }

    #[test]
    fn find_by_key_between_samples_floors_to_lower() {
        // A key sorting strictly between two samples lands in the LOWER sample's
        // interval — the forward walk from there covers it (design §B). Asserted
        // structurally so the test is independent of the murmur3 layout.
        use crate::util::cassandra_murmur3::cmp_partition_keys_by_token;
        use std::cmp::Ordering;
        let samples = token_ordered_samples(&[b"k0", b"k1", b"k2", b"k3", b"k4"]);
        for probe in [b"zz".as_slice(), b"m", b"q7", b"abcd", b"7", b"XYZ"] {
            let iv = match find_interval_for_key(&samples, probe) {
                Some(iv) => iv,
                None => continue,
            };
            let floor = iv.sample_index;
            assert_ne!(
                cmp_partition_keys_by_token(&samples[floor].partition_key, probe),
                Ordering::Greater,
                "floor sample must be <= probe in token order"
            );
            if let Some(next) = samples.get(floor + 1) {
                assert_eq!(
                    cmp_partition_keys_by_token(&next.partition_key, probe),
                    Ordering::Greater,
                    "the sample after the floor must be > probe in token order"
                );
            }
        }
    }

    #[test]
    fn find_by_key_below_first_sample_clamps_to_zero() {
        // Requirement 2/6: a key below the first sample clamps the floor to sample 0
        // (Cassandra walks from the index start). Constructed DETERMINISTICALLY: take a
        // pool of keys, sort by token, drop the lowest-token key from the samples, then
        // probe with that dropped key — guaranteed to sort below sample 0.
        use crate::util::cassandra_murmur3::cmp_partition_keys_by_token;
        use std::cmp::Ordering;
        let pool: [&[u8]; 6] = [b"p1", b"p2", b"p3", b"p4", b"p5", b"p6"];
        let all = token_ordered_samples(&pool); // token-ascending
        let below = all[0].partition_key.clone(); // lowest token in the pool
        let samples: Vec<SummaryEntry> = all[1..].to_vec(); // exclude it from the samples
        assert_eq!(
            cmp_partition_keys_by_token(&below, &samples[0].partition_key),
            Ordering::Less,
            "constructed probe must sort below the first remaining sample"
        );
        let iv = find_interval_for_key(&samples, &below).expect("interval");
        assert_eq!(iv.sample_index, 0, "below-first key clamps to sample 0");
        assert_eq!(iv.start_position, samples[0].position);
    }

    // ---- interval byte scan + disk accessor ----

    /// Encode one BIG `Index.db` entry: `[key_len u16 BE][key][data_offset vint][promoted_len vint=0]`.
    fn encode_entry(key: &[u8], data_offset: u64) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(key.len() as u16).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&crate::parser::vint::encode_vuint(data_offset));
        out.extend_from_slice(&crate::parser::vint::encode_vuint(0));
        out
    }

    fn build_interval(keys_offsets: &[(&[u8], u64)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (k, off) in keys_offsets {
            buf.extend_from_slice(&encode_entry(k, *off));
        }
        buf
    }

    #[test]
    fn scan_finds_present_key_and_counts_bounded_entries() {
        let bytes = build_interval(&[
            (b"alpha", 0),
            (b"bravo", 100),
            (b"charlie", 250),
            (b"delta", 400),
        ]);
        let res = scan_interval_bytes(&bytes, b"charlie", 128);
        let entry = res.entry.expect("present key must be found");
        assert_eq!(entry.key_digest.as_ref(), b"charlie");
        assert_eq!(entry.data_offset, 250);
        assert_eq!(
            res.entries_touched, 3,
            "touched only up to and including the match"
        );
    }

    #[test]
    fn scan_absent_key_within_interval_is_authoritative_none() {
        let bytes = build_interval(&[(b"alpha", 0), (b"bravo", 100), (b"charlie", 250)]);
        let res = scan_interval_bytes(&bytes, b"zzz_absent", 128);
        assert!(res.entry.is_none(), "absent key resolves to None");
        assert_eq!(
            res.entries_touched, 3,
            "touched all interval entries, bounded"
        );
    }

    #[test]
    fn scan_respects_entry_cap() {
        let bytes = build_interval(&[
            (b"k0", 0),
            (b"k1", 10),
            (b"k2", 20),
            (b"k3", 30),
            (b"k4", 40),
        ]);
        let res = scan_interval_bytes(&bytes, b"k4", 2);
        assert!(res.entry.is_none(), "cap reached before the match");
        assert_eq!(res.entries_touched, 2, "scan stopped at the cap");
    }

    #[test]
    fn scan_stops_on_truncated_tail() {
        let mut bytes = build_interval(&[(b"alpha", 0), (b"bravo", 100)]);
        bytes.extend_from_slice(&[0x00, 0x40]); // dangling key-length header, no body
        let res = scan_interval_bytes(&bytes, b"missing", 128);
        assert!(res.entry.is_none());
        assert_eq!(res.entries_touched, 2, "only the two whole entries counted");
    }

    #[tokio::test]
    async fn lookup_reads_only_the_bounded_range_from_disk() {
        // Requirement 2: the async accessor seeks to `start_position` and reads only
        // `[start, end)`, resolving a key inside the interval WITHOUT touching the
        // trailing entries. The interval-parse COUNTER is asserted in the
        // observability-testing integration test (needs the process-global meter).
        let prefix = build_interval(&[(b"before0", 0), (b"before1", 5)]);
        let interval = build_interval(&[(b"target", 900), (b"neighbor", 950)]);
        let trailing = build_interval(&[(b"after0", 1000)]);
        let mut file_bytes = prefix.clone();
        file_bytes.extend_from_slice(&interval);
        file_bytes.extend_from_slice(&trailing);

        let dir = std::env::temp_dir().join(format!(
            "cqlite-2412-interval-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nb-1-big-Index.db");
        std::fs::write(&path, &file_bytes).unwrap();

        let iv = SummaryInterval {
            start_position: prefix.len() as u64,
            end_position: Some((prefix.len() + interval.len()) as u64),
            sample_index: 1,
        };
        let res = lookup_key_in_interval(&path, iv, b"target", 128, 128)
            .await
            .expect("interval read");
        let entry = res.entry.expect("target present in the interval");
        assert_eq!(entry.key_digest.as_ref(), b"target");
        assert_eq!(entry.data_offset, 900);
        assert_eq!(res.entries_touched, 1, "matched the first interval entry");

        // A key beyond the bounded range is structurally unreachable for this interval.
        let res2 = lookup_key_in_interval(&path, iv, b"after0", 128, 128)
            .await
            .expect("interval read");
        assert!(
            res2.entry.is_none(),
            "a key past end_position is not read (authoritative miss for this interval)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Roborev job 1709 (High, data-loss class): downsampled-summary false
    // authoritative-absence regression ----

    /// Write `n` synthetic entries at strictly increasing offsets, returning the
    /// bytes plus the byte offset the `target_index`'th entry starts at (so a
    /// caller can carve out a `[start, target_end)` sub-slice as an end-bounded
    /// interval whose LAST entry is the target — the exact shape
    /// `find_interval_for_key` would hand a caller: the interval's `end_position`
    /// sits at the position of the NEXT summary sample, which is the position
    /// immediately after the entries this interval covers).
    fn build_n_entries(n: usize, target_index: usize, target_key: &[u8]) -> (Vec<u8>, u64) {
        let mut buf = Vec::new();
        let mut target_end = 0u64;
        for i in 0..n {
            let key: Vec<u8> = if i == target_index {
                target_key.to_vec()
            } else {
                format!("k{i:08}").into_bytes()
            };
            buf.extend_from_slice(&encode_entry(&key, i as u64));
            if i == target_index {
                target_end = buf.len() as u64;
            }
        }
        (buf, target_end)
    }

    async fn write_temp_index(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cqlite-2412-ds-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nb-1-big-Index.db");
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// THE blocker pin (roborev job 1709): a downsampled `Summary.db`
    /// (`min_index_interval = 128`, `sampling_level = 32` — 4x downsampled, so a
    /// real interval can legitimately span up to `128 * 128 / 32 = 512`
    /// partitions) hands an END-BOUNDED interval (`end_position.is_some()`) that
    /// spans 200 entries — comfortably past the PRE-FIX cap
    /// (`min_index_interval + 1 = 129`), comfortably under the true downsampled
    /// width. The target key sits at index 150 (past the old cap). It MUST
    /// resolve: the false "authoritative absence" this fix closes would otherwise
    /// surface as a silent "not found" for a partition that is genuinely present
    /// on disk (`big_get_with_resolution`'s `covering_interval_is_end_bounded`
    /// gate treats an end-bounded interval miss as definitive, never falling back
    /// to `scan_for_key`).
    #[tokio::test]
    async fn downsampled_end_bounded_interval_resolves_key_past_the_old_cap() {
        const MIN_INDEX_INTERVAL: u32 = 128;
        const SAMPLING_LEVEL: u32 = 32; // 4x downsampled: effective width 512.
        const N: usize = 200; // > old cap (129), < downsampled width (512).
        const TARGET_INDEX: usize = 150; // past the old 129-entry cap.
        let target_key = b"the-present-partition";

        let (bytes, target_end) = build_n_entries(N, TARGET_INDEX, target_key);
        let path = write_temp_index("bounded-present", &bytes).await;

        let iv = SummaryInterval {
            start_position: 0,
            end_position: Some(bytes.len() as u64), // whole buffer is ONE interval
            sample_index: 0,
        };
        let _ = target_end; // (kept for clarity; the interval covers the WHOLE buffer)

        let res = lookup_key_in_interval(&path, iv, target_key, MIN_INDEX_INTERVAL, SAMPLING_LEVEL)
            .await
            .expect("interval read");
        assert!(
            res.entry.is_some(),
            "a present key at index {TARGET_INDEX} (past the pre-fix {}-entry cap) in a \
             downsampled end-bounded interval MUST resolve — a miss here is the false \
             authoritative-absence data-loss bug (roborev job 1709)",
            MIN_INDEX_INTERVAL + 1
        );
        assert_eq!(res.entry.unwrap().key_digest.as_ref(), target_key);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Mirror case: a key GENUINELY absent from the same downsampled end-bounded
    /// interval still resolves to an authoritative `None` — the fix must not turn
    /// "no premature cap" into "never stop" (an unbounded scan or a false hit).
    /// `entries_touched` must equal the FULL interval's real entry count (every
    /// whole entry scanned, no premature cap), not the old 129-entry cap.
    #[tokio::test]
    async fn downsampled_end_bounded_interval_absent_key_is_still_authoritative() {
        const MIN_INDEX_INTERVAL: u32 = 128;
        const SAMPLING_LEVEL: u32 = 32;
        const N: usize = 200;

        // No entry actually holds this key — build N entries with generated keys
        // and probe with a key that structurally cannot collide with any of them.
        let (bytes, _) = build_n_entries(N, usize::MAX, b"unused");
        let path = write_temp_index("bounded-absent", &bytes).await;

        let iv = SummaryInterval {
            start_position: 0,
            end_position: Some(bytes.len() as u64),
            sample_index: 0,
        };
        let res = lookup_key_in_interval(
            &path,
            iv,
            b"zzz-genuinely-absent-key",
            MIN_INDEX_INTERVAL,
            SAMPLING_LEVEL,
        )
        .await
        .expect("interval read");
        assert!(
            res.entry.is_none(),
            "a genuinely absent key in a downsampled end-bounded interval must still \
             resolve to an authoritative absence"
        );
        assert_eq!(
            res.entries_touched, N,
            "the WHOLE interval's {N} entries must be scanned (no premature cap), \
             proving the absence is genuinely exhaustive, not cap-truncated"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Last-interval (read-to-EOF) generous-cap bound test: `last_interval_entry_cap`
    /// must derive the SAME downsampling-adjusted width formula
    /// (`min_index_interval * 128 / sampling_level + 1`) the module doc promises —
    /// pinned directly (pure function, no I/O) so the formula itself is load-bearing,
    /// not just its end-to-end effect.
    #[test]
    fn last_interval_entry_cap_is_downsampling_adjusted() {
        // Full sampling (128): cap is the pre-fix baseline, min_index_interval + 1.
        assert_eq!(last_interval_entry_cap(128, 128), 129);
        // 4x downsampled (sampling_level = 32): width widens to 128*128/32 = 512.
        assert_eq!(last_interval_entry_cap(128, 32), 513);
        // Maximum downsampling (sampling_level = 1): width widens to 128*128 = 16384.
        assert_eq!(last_interval_entry_cap(128, 1), 16385);
        // Corrupt/never-should-happen sampling_level = 0 clamps to 1 (max legitimate
        // widening — the fail-safe direction for a cap, never a divide-by-zero).
        assert_eq!(last_interval_entry_cap(128, 0), 16385);
        // Corrupt/never-should-happen sampling_level > BASE_SAMPLING_LEVEL clamps to
        // 128 (the un-downsampled baseline).
        assert_eq!(last_interval_entry_cap(128, 9999), 129);
    }

    /// End-to-end confirmation that the read-to-EOF LAST interval (not merely the
    /// pure-formula cap above) actually uses the downsampling-adjusted cap: a key
    /// past the pre-fix 129-entry cap resolves when the interval is read to EOF
    /// (`end_position: None`) under downsampling.
    #[tokio::test]
    async fn downsampled_last_interval_resolves_key_past_the_old_cap() {
        const MIN_INDEX_INTERVAL: u32 = 128;
        const SAMPLING_LEVEL: u32 = 32; // downsampled width 512.
        const N: usize = 200;
        const TARGET_INDEX: usize = 150;
        let target_key = b"present-in-last-interval";

        let (bytes, _) = build_n_entries(N, TARGET_INDEX, target_key);
        let path = write_temp_index("last-present", &bytes).await;

        let iv = SummaryInterval {
            start_position: 0,
            end_position: None, // read-to-EOF: the last interval.
            sample_index: 0,
        };
        let res = lookup_key_in_interval(&path, iv, target_key, MIN_INDEX_INTERVAL, SAMPLING_LEVEL)
            .await
            .expect("interval read");
        assert!(
            res.entry.is_some(),
            "a present key past the pre-fix cap in a downsampled LAST interval must \
             resolve too (the generous, downsampling-adjusted cap applies here)"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
