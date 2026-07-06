//! First/last-key range short-circuit for point lookups (issue #1576, Epic C / C5).
//!
//! An SSTable's partitions occupy a contiguous slice of the token ring: everything
//! sorts between the physically-first partition (`first_key`) and the
//! physically-last (`last_key`). Cassandra persists those two bounds in `Summary.db`
//! (`summary_reader.rs`), and [`SummaryReader::get_first_key`] /
//! [`SummaryReader::get_last_key`] already parse them — but nothing consulted them,
//! so every candidate SSTable paid bloom / `Index.db` / trie work even for a query
//! key that provably cannot live in it.
//!
//! [`SSTableReader::partition_key_out_of_range`] closes that gap. When the query
//! key sorts outside `[first_key, last_key]` the point read
//! ([`SSTableReader::get_with_resolution`]) returns authoritative absence
//! (`Ok(None)`) BEFORE any bloom check, `Index.db` probe, or BTI trie descent,
//! recording one [`record_range_short_circuit`](crate::storage::sstable::read_work_counters::record_range_short_circuit).
//!
//! ## No-heuristics + no-false-miss (the load-bearing correctness property)
//!
//! The bound comes ONLY from authoritative metadata (`Summary.db`), never a guess.
//! The comparison is performed in the index's OWN order domain — ascending Cassandra
//! Murmur3 token, ties broken by unsigned-lexicographic key bytes — the exact
//! ordering the on-disk partitions are sorted by and that
//! [`sort_by_token_order`](super::model::sort_by_token_order) and the write engine's
//! `PartitionPosition::cmp` use (spec §5, Appendix B §313). Comparing raw bytes
//! alone would be WRONG (physical order is token order, not byte order) and could
//! drop a present partition — the exact false-miss footgun the audit warns against.
//! The bound is INCLUSIVE at both ends: a key equal to `first_key` or `last_key` is
//! in range and is NEVER short-circuited.
//!
//! When no authoritative bound is available (no `Summary.db` — e.g. a BTI "da"
//! reader, which has no Summary and whose trie is itself the authoritative presence
//! oracle — or an empty endpoint), the check conservatively reports "cannot rule
//! out" (`false`) and the normal presence path runs unchanged. It can therefore only
//! ever turn a would-be miss into a cheaper miss; it can never manufacture one.

use super::super::SSTableReader;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;

impl SSTableReader {
    /// Return `true` iff `key` provably sorts OUTSIDE this SSTable's authoritative
    /// `[first_key, last_key]` partition-key bound, so the partition is definitely
    /// absent and the caller may skip all presence work.
    ///
    /// Returns `false` (cannot rule out) when no authoritative bound is available
    /// (`Summary.db` absent, e.g. a BTI reader) or an endpoint is empty. See the
    /// module docs for the no-false-miss contract: the comparison is in Cassandra
    /// token order (Murmur3 token, unsigned-byte tiebreak), inclusive at both ends.
    pub fn partition_key_out_of_range(&self, key: &[u8]) -> bool {
        let Some(summary) = self.summary_reader.as_ref() else {
            // No Summary.db → no authoritative bound (BTI readers, or a BIG reader
            // whose Summary failed to load). Conservatively cannot rule out.
            return false;
        };

        let first = summary.get_first_key();
        let last = summary.get_last_key();
        if first.is_empty() || last.is_empty() {
            // A degenerate/absent endpoint cannot bound the ring slice safely.
            return false;
        }

        // Authoritative on-disk partition order: ascending Murmur3 token, ties broken
        // by unsigned-lexicographic key bytes. `key` (the raw serialized partition
        // key) and the Summary endpoints share this domain, so tokenizing each and
        // comparing the (token, bytes) tuple reproduces `DecoratedKey.compareTo`
        // exactly. Inclusive bound: `< first` OR `> last` is out of range; equality
        // with either endpoint is in range.
        let key_token = cassandra_murmur3_token(key);
        let first_token = cassandra_murmur3_token(first);
        let last_token = cassandra_murmur3_token(last);

        (key_token, key) < (first_token, first) || (key_token, key) > (last_token, last)
    }
}
