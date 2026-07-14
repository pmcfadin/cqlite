//! Token-ordered `Summary.db` find-by-key + the lazy `Index.db` interval accessor
//! for the Summary-guided BIG partition index (issue #2412, design §A/§B).
//!
//! Two primitives that make BIG open Cassandra-lazy:
//!
//! - [`find_interval_for_key`] binary-searches the token-ordered `Summary.db` samples
//!   for the half-open `Index.db` byte range [`SummaryInterval`] that must contain a
//!   query key's partition entry (the core new search; [`super::SummaryReader::find_by_key`]
//!   is the method wrapper).
//! - [`lookup_key_in_interval`] reads **only that one interval** (≤ `min_index_interval`
//!   entries) from disk and resolves the exact key, recording exactly one **interval**
//!   parse (`cqlite.sstable.index_interval_parses_total`) — never a full parse, so a
//!   lazy-open regression stays visible on `index_parses_total` (design §F).
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
    /// Number of `Index.db` entries actually parsed/touched, bounded by one summary
    /// interval (≤ `min_index_interval`). The scale-free work-probe for Requirement 2.
    pub entries_touched: usize,
}

/// Cap on entries scanned in a single interval, derived from `min_index_interval`.
///
/// One summary interval spans `min_index_interval` partitions by construction, but a
/// corrupt/rewritten summary could point at a wider run. Bounding the scan at
/// `min_index_interval` (plus the boundary entry) means a hostile summary cannot turn
/// a "bounded interval read" into a full-file walk; a genuine miss within the bound is
/// still authoritative because the interval end is the next authoritative sample.
fn interval_entry_cap(min_index_interval: u32) -> usize {
    // +1 covers the boundary entry that sits exactly at the next sample position.
    (min_index_interval as usize).saturating_add(1)
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
pub async fn lookup_key_in_interval(
    index_path: &Path,
    interval: SummaryInterval,
    key: &[u8],
    min_index_interval: u32,
) -> Result<IntervalLookup> {
    let mut file = File::open(index_path).await?;
    file.seek(std::io::SeekFrom::Start(interval.start_position))
        .await?;

    let mut buffer = Vec::new();
    match interval.end_position {
        Some(end) if end > interval.start_position => {
            // Bounded read of exactly the interval's whole entries.
            let len = (end - interval.start_position) as usize;
            buffer.resize(len, 0);
            file.read_exact(&mut buffer).await?;
        }
        // Last interval (or a degenerate empty/zero-width range): read to EOF. The scan
        // is still bounded by `interval_entry_cap`, so this can never become a
        // whole-file walk for a many-partition SSTable.
        _ => {
            file.read_to_end(&mut buffer).await?;
        }
    }

    // One bounded interval parse (distinct counter — never a full parse, design §F).
    crate::observability::add_counter(
        crate::observability::catalog::INDEX_INTERVAL_PARSES_TOTAL,
        1,
        &[],
    );

    Ok(scan_interval_bytes(
        &buffer,
        key,
        interval_entry_cap(min_index_interval),
    ))
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
        let res = lookup_key_in_interval(&path, iv, b"target", 128)
            .await
            .expect("interval read");
        let entry = res.entry.expect("target present in the interval");
        assert_eq!(entry.key_digest.as_ref(), b"target");
        assert_eq!(entry.data_offset, 900);
        assert_eq!(res.entries_touched, 1, "matched the first interval entry");

        // A key beyond the bounded range is structurally unreachable for this interval.
        let res2 = lookup_key_in_interval(&path, iv, b"after0", 128)
            .await
            .expect("interval read");
        assert!(
            res2.entry.is_none(),
            "a key past end_position is not read (authoritative miss for this interval)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
