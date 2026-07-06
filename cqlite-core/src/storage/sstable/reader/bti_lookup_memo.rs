//! C3 single-walk BTI partition-lookup memo (issue #1574).
//!
//! A single-candidate `WHERE pk = ?` point read against a BTI SSTable descends the
//! `Partitions.db` trie twice for the SAME key — once for the candidate prune
//! (`might_contain_partition`) and once for the seek. The resolved uncompressed
//! `Data.db` offset (or authoritative absence) is a pure function of the immutable
//! trie + key, so [`lookup_partition_via_bti_trie`] stores it in a single-entry
//! reader-local slot ([`SSTableReader::bti_lookup_memo`]) and the seek reuses it
//! without a second descent — keeping `TRIE_WALKS` at 1 per point read. A stale or
//! absent slot (a different key, or a concurrent read) simply misses and re-walks,
//! never a wrong result. This is deliberately NOT a cross-lookup key/offset cache
//! (that is Epic B/B4); it is bounded to one entry.
//!
//! Split out of `partition_lookup.rs` (campsite / epic #1116): keeps that file under
//! the source size threshold.
//!
//! [`lookup_partition_via_bti_trie`]: SSTableReader::lookup_partition_via_bti_trie

use super::SSTableReader;

impl SSTableReader {
    /// Emit the BTI presence-oracle observability counters (`READ_BLOOM_CHECKS`
    /// and `READ_PARTITION_LOOKUP`) for a resolved BTI partition lookup, keyed on
    /// whether the key was found. Used by the C3 single-walk memo-hit path so a
    /// reused resolution records the same presence decision a fresh descent does
    /// (only the `TRIE_WALKS` descent is skipped, not the presence accounting).
    pub(super) fn emit_bti_presence_counters(found: bool) {
        use crate::observability::{self as obs, catalog};
        let result = if found { "hit" } else { "miss" };
        obs::add_counter(
            catalog::READ_BLOOM_CHECKS,
            1,
            &[
                (catalog::attr::RESULT, result.into()),
                (catalog::attr::SSTABLE_FORMAT, "bti".into()),
            ],
        );
        obs::add_counter(
            catalog::READ_PARTITION_LOOKUP,
            1,
            &[
                (catalog::attr::RESULT, result.into()),
                (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                (catalog::attr::SSTABLE_FORMAT, "bti".into()),
            ],
        );
    }

    /// C3 same-key memo read: returns `Some(resolved)` when the most recent BTI
    /// resolution was for this exact `partition_key` (so the caller can skip a
    /// second trie descent), or `None` on a miss / different key / poisoned lock.
    /// The memoized value is a pure function of the immutable trie + key.
    pub(super) fn bti_lookup_memo_get(&self, partition_key: &[u8]) -> Option<Option<u64>> {
        let guard = self.bti_lookup_memo.lock().ok()?;
        match guard.as_ref() {
            Some((k, resolved)) if k.as_ref() == partition_key => Some(*resolved),
            _ => None,
        }
    }

    /// C3 same-key memo write: record this key's resolved offset for reuse by the
    /// immediately-following seek. Best effort — a poisoned lock is a no-op (the
    /// seek just re-walks, still correct).
    pub(super) fn bti_lookup_memo_store(&self, partition_key: &[u8], resolved: Option<u64>) {
        if let Ok(mut guard) = self.bti_lookup_memo.lock() {
            *guard = Some((Box::from(partition_key), resolved));
        }
    }
}

#[cfg(test)]
mod tests {
    /// The single-slot memo's match logic (same-key hit / different-key miss / the
    /// stored resolution round-trips) mirrors [`SSTableReader::bti_lookup_memo_get`]
    /// on the same `Option<(Box<[u8]>, Option<u64>)>` slot the reader field holds.
    /// The reader-level wiring (`TRIE_WALKS == 1`) is covered end-to-end by
    /// `tests/issue_1574_bti_single_walk.rs`.
    #[test]
    fn memo_slot_same_key_hits_different_key_misses() {
        // store a resolution for key-A
        let mut slot: Option<(Box<[u8]>, Option<u64>)> = Some((Box::from(&b"key-A"[..]), Some(63)));

        // The same-key match predicate used by `bti_lookup_memo_get`.
        let get = |slot: &Option<(Box<[u8]>, Option<u64>)>, key: &[u8]| -> Option<Option<u64>> {
            match slot.as_ref() {
                Some((k, resolved)) if k.as_ref() == key => Some(*resolved),
                _ => None,
            }
        };

        assert_eq!(get(&slot, b"key-A"), Some(Some(63)), "same key hits");
        assert_eq!(get(&slot, b"key-B"), None, "different key misses");

        // An absence (None) resolution is a valid, distinct memoized value.
        slot = Some((Box::from(&b"key-C"[..]), None));
        assert_eq!(get(&slot, b"key-C"), Some(None), "absence round-trips");
        assert_eq!(get(&slot, b"key-A"), None, "overwritten key misses");
    }
}
