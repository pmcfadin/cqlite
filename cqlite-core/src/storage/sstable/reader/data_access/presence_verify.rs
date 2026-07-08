//! Opt-in presence-oracle false-negative verification (issue #2163).
//!
//! Kept out of the already-large `data_access/mod.rs` entry point (campsite rule,
//! epic #1116). The verification is wired into the point-read path in
//! [`SSTableReader::get_with_resolution`](super::SSTableReader::get_with_resolution)
//! and driven by the default-off switch in
//! [`crate::storage::sstable::reader::presence_verification`].

use super::SSTableReader;
use crate::observability::{self as obs, catalog};
use crate::storage::sstable::reader::presence_verification;
use crate::types::TableId;
use crate::{Result, RowKey};
use tracing::warn;

impl SSTableReader {
    /// Authoritatively verify a presence-oracle "definitely absent" verdict for
    /// `partition_key` in THIS SSTable (issue #2163, opt-in / default-off).
    ///
    /// When the [`presence_verification`] switch is OFF (the default) this returns
    /// `Ok(false)` WITHOUT scanning — zero cost. When ON, it runs an AUTHORITATIVE
    /// sequential scan of this SSTable's own Data.db for the key (never a heuristic
    /// inference from byte patterns — the no-heuristics mandate). If the scan FINDS
    /// the key, the oracle produced a false negative:
    /// `cqlite.read.bloom.false_negatives` increments once with this SSTable's
    /// bounded `cqlite.sstable.format`, a warning is logged, and `Ok(true)` is
    /// returned. A genuine absence returns `Ok(false)` and never emits. Under a
    /// correct bloom/BTI-trie the counter stays 0.
    pub async fn verify_presence_oracle_negative(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
    ) -> Result<bool> {
        if !presence_verification::enabled() {
            return Ok(false);
        }
        // Authoritative confirmation: walk this SSTable's Data.db for the key.
        let key = RowKey::from(partition_key.to_vec());
        let found = self.scan_for_key(table_id, &key).await?.is_some();
        if found {
            let format = self.sstable_format_label();
            obs::add_counter(
                catalog::READ_BLOOM_FALSE_NEGATIVES,
                1,
                &[(catalog::attr::SSTABLE_FORMAT, format.into())],
            );
            warn!(
                sstable_format = format,
                partition_key_len = partition_key.len(),
                "presence-oracle false negative: a key reported definitely absent was found by an \
                 authoritative scan — the bloom/BTI-trie is unsound for this SSTable"
            );
        }
        Ok(found)
    }
}
