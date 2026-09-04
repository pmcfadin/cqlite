//! Partition lookup and index-based access methods for SSTableReader
//!
//! This module contains methods for efficient partition lookup using Index.db,
//! Summary.db, and Statistics.db readers.

use super::SSTableReader;
use crate::storage::cache::PartitionLoc;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};
use tracing::debug;

impl SSTableReader {
    /// Consult the process-global key→partition-offset cache (issue #2059) with this
    /// reader's authoritative generation identity. A `None` identity (a stat failure
    /// at open) BYPASSES the cache — a structural miss, never a fabricated identity
    /// (no-heuristics #28). Fail-closed on identity mismatch is enforced inside the
    /// cache: an entry keyed on a different generation is a miss.
    pub(crate) fn key_cache_get(&self, partition_key: &[u8]) -> Option<PartitionLoc> {
        let identity = self.generation_identity?;
        self.key_offset_cache.get(identity, partition_key)
    }

    /// Populate the process-global key→partition-offset cache (issue #2059) with this
    /// reader's generation identity. A `None` identity is a no-op (the cache is
    /// bypassed for this reader rather than fabricating an identity).
    pub(crate) fn key_cache_insert(&self, partition_key: &[u8], loc: PartitionLoc) {
        if let Some(identity) = self.generation_identity {
            self.key_offset_cache.insert(identity, partition_key, loc);
        }
    }

    /// Drop ALL of this reader's generation's entries from the process-global key
    /// cache (issue #2059 §C). Called when the generation is removed / compacted
    /// away / evicted from the warm registry, so its locations are reclaimed
    /// promptly and recorded on the distinct `invalidations` counter (never the
    /// budget-driven `evictions`). Returns the number of entries dropped. A
    /// #2383 rebind does NOT call this — the identity is unchanged across a rebind,
    /// so entries survive. No-op when the identity is `None`.
    ///
    /// `pub` so the flight `WarmTableRegistry` can invalidate on warm eviction
    /// (issue #2059 §C) — the warm registry is the process's largest reader pin, so
    /// its evictions are the primary reclamation trigger for the global cache.
    pub fn invalidate_key_cache_entries(&self) -> u64 {
        match self.generation_identity {
            Some(identity) => self.key_offset_cache.invalidate(identity),
            None => 0,
        }
    }

    /// Enhanced partition lookup using Index.db reader with promoted index support.
    ///
    /// `partition_key` must be the raw partition-key bytes as produced by
    /// [`PartitionKey::to_bytes`](crate::storage::write_engine::mutation::PartitionKey::to_bytes):
    ///
    /// - **Single-component keys** — raw value bytes (UUID = 16 bytes, int = 4 BE bytes, etc.).
    /// - **Multi-component (composite) keys** — `[len: u16 BE][value bytes][0x00]` per component,
    ///   including a trailing `0x00` after the final component.
    ///
    /// The Index.db key_lookup map is keyed on these exact raw bytes (set when the BIG-format
    /// parser was fixed in Issue #552).  The old digest-based path (which caused every lookup
    /// to miss) has been removed.  On a miss the function returns `Ok(None)` so callers can
    /// fall through to their existing sequential-scan fallback.
    pub async fn lookup_partition_with_index(
        &self,
        partition_key: &[u8],
    ) -> Result<Option<(u64, u32)>> {
        use crate::observability::{self as obs, catalog};

        let format = self.sstable_format_label();

        // B4 key→partition-offset cache (issue #1570): a repeated point read of a
        // present key returns the cached `(offset,size)` without re-probing Index.db
        // (`INDEX_PROBES` stays 0 on the hit). Positive-only. Checked FIRST — before
        // any materialize / interval read — so the hit skips ALL probe work on both
        // the Summary-guided and resident-map paths (issue #2412 §B).
        if let Some(loc) = self.key_cache_get(partition_key) {
            debug!(
                "B4 key-cache hit for partition (raw key len={}): offset={}, size={}",
                partition_key.len(),
                loc.data_offset,
                loc.data_size
            );
            obs::add_counter(
                catalog::READ_PARTITION_LOOKUP,
                1,
                &[
                    (catalog::attr::RESULT, "hit".into()),
                    (catalog::attr::LOOKUP_ROUTE, "index".into()),
                    (catalog::attr::SSTABLE_FORMAT, format.into()),
                ],
            );
            return Ok(Some((loc.data_offset, loc.data_size)));
        }

        // Stage 3 (issue #2412 §B): a usable `Summary.db` + a not-yet-materialized
        // lazy `Index.db` → resolve via ONE bounded Summary-guided interval instead of
        // materializing the whole map. See `reader::summary_point` for the
        // authoritative-absence rules the BIG point path applies to a resulting `None`.
        if self.should_use_summary_interval() {
            return self
                .lookup_partition_via_summary_interval(partition_key)
                .await;
        }

        // FellBack / already-materialized path: ensure the full resident map, then
        // probe it. `ensure_materialized` MUST run BEFORE the tracing span — the
        // non-`Send` `EnteredSpan` held across an await makes callers non-`Send`. A
        // no-op after the first call, or for an eagerly-opened (FellBack) reader.
        if let Some(index_reader) = &self.index_reader {
            index_reader.ensure_materialized(&self.scan_cancel).await?;
        }

        let _span = tracing::debug_span!("sstable.partition_lookup.index").entered();

        if let Some(index_reader) = &self.index_reader {
            // A5/B4 read-work counter (`INDEX_PROBES`; consumer B4): one per real
            // `Index.db` probe. A B4 cache hit above records nothing. No-op in release.
            crate::storage::sstable::read_work_counters::record_index_probe();
            // Direct raw-key lookup — O(1) HashMap lookup.
            // Index.db entries are keyed on the raw partition key bytes since #552;
            // no Murmur3 digest computation is needed or correct here.
            if let Some(entry) = index_reader.lookup_partition(partition_key) {
                debug!(
                    "Found partition via Index.db raw-key lookup: offset={}, size={}",
                    entry.data_offset, entry.data_size
                );
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "index".into()),
                        (catalog::attr::SSTABLE_FORMAT, format.into()),
                    ],
                );
                // Cache this present-key resolution so the next point read of the
                // same key skips the Index.db probe (issue #1570/#2059).
                self.key_cache_insert(
                    partition_key,
                    PartitionLoc::new(entry.data_offset, entry.data_size),
                );
                return Ok(Some((entry.data_offset, entry.data_size)));
            } else {
                debug!(
                    "Partition not found in Index.db for raw key (len={})",
                    partition_key.len()
                );
            }
        } else {
            debug!("No Index.db reader available for partition lookup");
        }
        obs::add_counter(
            catalog::READ_PARTITION_LOOKUP,
            1,
            &[
                (catalog::attr::RESULT, "miss".into()),
                (catalog::attr::LOOKUP_ROUTE, "index".into()),
                (catalog::attr::SSTABLE_FORMAT, format.into()),
            ],
        );
        Ok(None)
    }

    /// BTI ("da") partition point lookup via the in-memory Partitions.db trie
    /// (issue #831, building on the verified #755 primitive).
    ///
    /// Encodes `partition_key` into the BTI byte-comparable trie key and walks the
    /// trie to resolve the partition's location. Returns:
    ///
    /// - `Ok(Some(offset))` — the UNCOMPRESSED Data.db byte offset of the
    ///   partition. INVARIANT: this offset indexes into the DECOMPRESSED data
    ///   section, never raw file bytes. The offset is resolved in one of two ways:
    ///     - **NARROW** partition (`BtiPartitionLocation::DataOffset`) — the trie
    ///       returns the Data.db offset directly.
    ///     - **WIDE** partition (`BtiPartitionLocation::RowsOffset`) — the trie
    ///       returns a positive offset into `Rows.db`; we deserialize that
    ///       partition's `TrieIndexEntry` via [`resolve_rows_db_entry`] and use its
    ///       recovered `data_position` (issue #909/#910). Both forms share the same
    ///       uncompressed Data.db offset domain, so the caller treats them
    ///       identically.
    /// - `Ok(None)` — the reader is not BTI, or the key has no trie path (the
    ///   partition is definitely absent from this SSTable).
    /// - `Err(_)` — a structural trie parse error, or a `RowsOffset` was returned
    ///   without an accompanying `Rows.db` (a structurally invalid BTI SSTable).
    ///
    /// Because the BTI trie uses path compression, a returned offset may be a
    /// candidate for a *prefix-colliding* key. The caller (`bti_point_lookup`)
    /// MUST verify the partition-key bytes at the resolved offset equal the
    /// queried key before returning rows.
    ///
    /// [`resolve_rows_db_entry`]: crate::storage::sstable::bti::resolve_rows_db_entry
    pub fn lookup_partition_via_bti_trie(&self, partition_key: &[u8]) -> Result<Option<u64>> {
        use crate::storage::sstable::bti::encode_partition_key_for_bti_trie;

        // Not a BTI reader — no trie to consult (no descent, no encode, no count).
        if self.bti_partitions_db.is_none() {
            return Ok(None);
        }

        // B4 key→partition-offset cache (issue #1570) — the OUTERMOST layer, checked
        // BEFORE the C3 memo and BEFORE the encode, so a repeated point read of the
        // same present key returns the cached offset WITHOUT a trie descent
        // (`TRIE_WALKS` stays 0) AND WITHOUT re-encoding the key (no `KEY_HASH_CALLS`
        // bump — the encode below is skipped on a hit, preserving C4's
        // one-hash-per-read property). Unlike the single-entry C3 memo, the multi-key
        // B4 cache serves a hit even across interleaved keys. Trie-hit-only
        // (prefix-collision candidates included; the caller re-verifies the key bytes
        // at the resolved offset), never a trie MISS: the cached offset is the exact
        // offset a fresh descent resolves, so re-verification reaches the identical
        // conclusion (correctness guardrail; mirrors the C3 memo). Emit the presence
        // counters (found = true) so a cache-served lookup records the same presence
        // decision a fresh descent does. A disabled cache (`block_cache.enabled=false`)
        // is a genuine no-op `get`, so this falls through and the read re-walks.
        if let Some(loc) = self.key_cache_get(partition_key) {
            Self::emit_bti_presence_counters(true);
            return Ok(Some(loc.data_offset));
        }

        // Issue #1574 (C3) single walk: reuse the prune's resolution for the seek —
        // a pure function of the immutable trie + key. Checked BEFORE the encode so
        // a memo hit costs neither a trie descent NOR a Murmur3 key hash. A
        // stale/absent slot simply misses and re-walks. The presence-oracle
        // observability is preserved: a memo hit still records the presence decision.
        if let Some(resolved) = self.bti_lookup_memo_get(partition_key) {
            Self::emit_bti_presence_counters(resolved.is_some());
            return Ok(resolved);
        }

        // Issue #1575 (C4): encode ONCE here (Murmur3 hash + byte-comparable
        // encoding, `KEY_HASH_CALLS`). The multi-candidate prune path instead calls
        // `lookup_partition_via_bti_trie_encoded` with a single hoisted encoding so N
        // candidate generations share ONE hash rather than re-hashing per candidate.
        let encoded = encode_partition_key_for_bti_trie(partition_key);
        self.bti_trie_resolve(partition_key, &encoded)
    }

    /// Resolve a BTI partition using a PRE-ENCODED byte-comparable key, so the
    /// Murmur3 hash + encoding is computed once per read and reused across every
    /// candidate SSTable's prune instead of being recomputed per candidate
    /// (issue #1575 / C4). `encoded` MUST equal
    /// `encode_partition_key_for_bti_trie(partition_key)`; the caller
    /// (`SSTableManager`'s candidate prune) computes it exactly once. Semantics are
    /// otherwise identical to [`lookup_partition_via_bti_trie`]: same same-key memo,
    /// same presence-oracle observability, same resolved offset. A non-BTI reader
    /// returns `Ok(None)` (the `encoded` argument is simply unused).
    pub fn lookup_partition_via_bti_trie_encoded(
        &self,
        partition_key: &[u8],
        encoded: &[u8; 9],
    ) -> Result<Option<u64>> {
        if self.bti_partitions_db.is_none() {
            return Ok(None);
        }
        // B4 key→partition-offset cache (issue #1570) — the OUTERMOST layer on the
        // candidate-prune path too, checked BEFORE the C3 memo and the trie descent.
        // The caller (`prune_candidates`) already hoisted the single encode into
        // `encoded` (C4 / `KEY_HASH_CALLS == 1` per read); a cache hit here simply
        // skips this candidate's trie descent (`TRIE_WALKS` stays 0 on the hit) — the
        // multi-key cache covers interleaved keys the single-entry memo cannot. Same
        // trie-hit-only correctness guardrail as the raw-key entry: the cached offset
        // is the exact offset a fresh descent resolves. A disabled cache is a no-op
        // `get`, so this falls through and the candidate re-walks.
        if let Some(loc) = self.key_cache_get(partition_key) {
            Self::emit_bti_presence_counters(true);
            return Ok(Some(loc.data_offset));
        }
        if let Some(resolved) = self.bti_lookup_memo_get(partition_key) {
            Self::emit_bti_presence_counters(resolved.is_some());
            return Ok(resolved);
        }
        self.bti_trie_resolve(partition_key, encoded)
    }

    /// Shared BTI trie descent for a pre-encoded key — the body behind both
    /// [`lookup_partition_via_bti_trie`] and
    /// [`lookup_partition_via_bti_trie_encoded`]. The caller has already confirmed
    /// this is a BTI reader and consulted the same-key memo; this records the single
    /// `TRIE_WALKS`, walks the resident trie buffer with the pre-encoded key, emits
    /// the presence-oracle counters, and stores the memo. It never re-encodes the
    /// key (no `KEY_HASH_CALLS`), which is the property C4 hoists.
    fn bti_trie_resolve(&self, partition_key: &[u8], encoded: &[u8; 9]) -> Result<Option<u64>> {
        use crate::observability::{self as obs, catalog};
        use crate::storage::sstable::bti::{
            lookup_partition_in_bti_slice, resolve_rows_db_entry, BtiPartitionLocation,
        };

        let span = tracing::debug_span!(
            "sstable.partition_lookup.bti_trie",
            partition_shape = tracing::field::Empty,
        );
        let _entered = span.enter();

        let Some(partitions_db) = &self.bti_partitions_db else {
            // Not a BTI reader — no trie to descend (defensive; caller pre-checks).
            return Ok(None);
        };

        // A5 read-work counter (TRIE_WALKS; consumers C3/C4): one per real trie
        // descent — counted only once we have a `Partitions.db` to descend, so a
        // non-BTI reader that returned above never bumps it. No-op in release
        // (design.md Decision 1/2).
        crate::storage::sstable::read_work_counters::record_trie_walk();

        // Issue #1574 (C3): walk the resident `Arc<Vec<u8>>` trie buffer in place —
        // no per-lookup whole-file copy. Issue #1575 (C4): with the PRE-ENCODED key,
        // so the encode is not repeated per candidate.
        let lookup =
            lookup_partition_in_bti_slice(partitions_db.as_slice(), encoded).map_err(|e| {
                Error::corruption(format!(
                    "BTI Partitions.db trie lookup failed for partition key (len={}): {}",
                    partition_key.len(),
                    e
                ))
            });
        let lookup = match lookup {
            Ok(v) => v,
            Err(e) => {
                obs::record_error(&e, "reader");
                return Err(e);
            }
        };

        // The BTI Partitions.db trie IS the presence oracle for a BTI SSTable
        // (BTI has no bloom filter). Emit READ_BLOOM_CHECKS exactly ONCE here — the
        // single common path every BTI presence/point lookup funnels through
        // (get → locate → bti_point_lookup, might_contain_partition, the candidate
        // prune). `cqlite.result` is hit for a trie HIT
        // (maybe-present/found: a DataOffset/RowsOffset, possibly a prefix-collision
        // candidate the caller re-verifies) and miss for a trie MISS (definitive
        // absence). A trie parse error returns above without an outcome and is
        // counted as an error, not a bloom check, so this never double counts.
        // Emitting before the per-arm handling (which can still error on a missing
        // Rows.db for a wide partition) guarantees a single emission per call.
        obs::add_counter(
            catalog::READ_BLOOM_CHECKS,
            1,
            &[
                (
                    catalog::attr::RESULT,
                    if lookup.is_some() { "hit" } else { "miss" }.into(),
                ),
                (catalog::attr::SSTABLE_FORMAT, "bti".into()),
            ],
        );

        match lookup {
            Some(BtiPartitionLocation::DataOffset(off)) => {
                span.record("partition_shape", "narrow");
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                debug!(
                    "BTI trie resolved NARROW partition (key len={}) to Data.db offset {}",
                    partition_key.len(),
                    off
                );
                self.bti_lookup_memo_store(partition_key, Some(off));
                // Cache this trie-hit resolution (a prefix-collision candidate,
                // re-verified by the caller downstream) so a later interleaved read of
                // the same key skips the trie descent (issue #1570/#2059).
                self.key_cache_insert(partition_key, PartitionLoc::offset_only(off));
                Ok(Some(off))
            }
            Some(BtiPartitionLocation::RowsOffset(rows_offset)) => {
                // WIDE partition: the trie pointed at this partition's
                // TrieIndexEntry inside Rows.db. Deserialize it to recover the
                // partition's Data.db start position (`data_position`), which lives
                // in the SAME uncompressed-offset domain the narrow path uses, so
                // the caller can decode the partition identically (#909/#910).
                span.record("partition_shape", "wide");
                let rows_db = match self.bti_rows_db.as_ref().ok_or_else(|| {
                    Error::corruption(format!(
                        "BTI Partitions.db trie returned RowsOffset({}) for partition key \
                         (len={}) but this reader has no Rows.db loaded; the SSTable is \
                         structurally invalid (Rows.db is required for wide partitions).",
                        rows_offset,
                        partition_key.len()
                    ))
                }) {
                    Ok(v) => v,
                    Err(e) => {
                        obs::record_error(&e, "reader");
                        return Err(e);
                    }
                };

                let header = match resolve_rows_db_entry(rows_db.as_slice(), rows_offset as usize)
                    .map_err(|e| {
                        Error::corruption(format!(
                            "BTI Rows.db row-index entry at RowsOffset({}) is unreadable for \
                             partition key (len={}): {}",
                            rows_offset,
                            partition_key.len(),
                            e
                        ))
                    }) {
                    Ok(v) => v,
                    Err(e) => {
                        obs::record_error(&e, "reader");
                        return Err(e);
                    }
                };

                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                debug!(
                    "BTI trie resolved WIDE partition (key len={}) via RowsOffset {} -> Data.db \
                     position {} ({} row-index blocks)",
                    partition_key.len(),
                    rows_offset,
                    header.data_position,
                    header.block_count
                );
                self.bti_lookup_memo_store(partition_key, Some(header.data_position));
                // Cache this present WIDE-partition resolution (issue #1570/#2059).
                self.key_cache_insert(
                    partition_key,
                    PartitionLoc::offset_only(header.data_position),
                );
                Ok(Some(header.data_position))
            }
            None => {
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "miss".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                self.bti_lookup_memo_store(partition_key, None);
                Ok(None)
            }
        }
    }

    /// Cheap presence oracle: can this SSTable possibly contain `partition_key`?
    ///
    /// Used to prune SSTables before a partition-targeted scan (the query engine's
    /// `WHERE pk = ?` fast path). Returning `false` MUST be definitive — the SSTable
    /// is then skipped entirely without being parsed — so this only ever returns
    /// `false` for an authoritative "absent" signal:
    ///
    /// - **BTI ("da")** readers have no bloom filter; the Partitions.db trie is the
    ///   authoritative present/absent oracle. A trie miss (`Ok(None)`) is definitive
    ///   absence. A trie hit may be a prefix-collision candidate, which is a safe
    ///   *false positive* here (the partition scan re-verifies the key). Any trie
    ///   parse error is treated conservatively as "maybe present".
    /// - **BIG-format** readers consult the bloom filter, which never reports false
    ///   negatives: `might_contain == false` is definitive absence. With no bloom
    ///   filter loaded we cannot prune, so we conservatively return `true`.
    ///
    /// `partition_key` must be the raw partition-key bytes (same encoding the bloom
    /// filter and Index.db/BTI trie are keyed on).
    pub fn might_contain_partition(&self, partition_key: &[u8]) -> bool {
        use crate::observability::{self as obs, catalog};

        if self.bti_partitions_db.is_some() {
            // Issue #2163 (roborev r7): restore the C5 range short-circuit
            // (`locate`'s Step 1) ahead of the trie probe. The r2 refactor
            // removed `locate_encoded` (replaced by routing the candidate-prune
            // path through these `might_contain_partition[_encoded]` helpers)
            // and dropped this step for BTI, so an out-of-range key paid a
            // pointless trie descent and never recorded
            // `read_work_counters::record_range_short_circuit`. A no-op for a
            // genuine BTI reader today (no Summary.db, so
            // `partition_key_out_of_range` always returns `false`), but this
            // restores exact step-ordering parity with `locate` for any
            // degraded case and keeps the work-counter accounting consistent.
            if self.partition_key_out_of_range(partition_key) {
                crate::storage::sstable::read_work_counters::record_range_short_circuit();
                return false;
            }
            // BTI: trie miss is authoritative absence; any error is conservative.
            // `lookup_partition_via_bti_trie` is the single common path that emits
            // READ_BLOOM_CHECKS for the BTI presence check — do NOT emit again here
            // or the metric would be double counted.
            let present = matches!(
                self.lookup_partition_via_bti_trie(partition_key),
                Ok(Some(_)) | Err(_)
            );
            if !present {
                // Definitive trie miss → this SSTable is pruned from the read
                // (issue #2163).
                self.emit_sstable_pruned();
            }
            return present;
        }
        // BIG: only record READ_BLOOM_CHECKS when a bloom filter actually exists.
        // With no filter loaded we cannot prune, so we conservatively return `true`
        // WITHOUT recording a check (a no-filter "hit" is not a real bloom check and
        // would inflate the metric).
        match &self.bloom_filter {
            Some(bloom) => {
                let present = bloom.might_contain(partition_key);
                obs::add_counter(
                    catalog::READ_BLOOM_CHECKS,
                    1,
                    &[
                        (
                            catalog::attr::RESULT,
                            if present { "hit" } else { "miss" }.into(),
                        ),
                        (
                            catalog::attr::SSTABLE_FORMAT,
                            self.sstable_format_label().into(),
                        ),
                    ],
                );
                if !present {
                    // Bloom `might_contain == false` is a definitive negative →
                    // this SSTable is pruned from the read (issue #2163).
                    self.emit_sstable_pruned();
                }
                present
            }
            None => true,
        }
    }

    /// Emit `cqlite.read.sstables_pruned` for one SSTable excluded from a read by
    /// a presence-oracle definitive negative (issue #2163). Carries the bounded
    /// `cqlite.sstable.format` (`"big"`/`"bti"`); no-op when observability is off.
    ///
    /// `pub(crate)` (not just this module) so the PRIMARY single-reader point-read
    /// path (`get_with_resolution` in `data_access/mod.rs`) can call the SAME emit
    /// site the candidate-prune helpers (`might_contain_partition[_encoded]`) use —
    /// one implementation, no duplicated emission logic (roborev r4).
    pub(crate) fn emit_sstable_pruned(&self) {
        use crate::observability::{self as obs, catalog};
        obs::add_counter(
            catalog::READ_SSTABLES_PRUNED,
            1,
            &[(
                catalog::attr::SSTABLE_FORMAT,
                self.sstable_format_label().into(),
            )],
        );
    }

    /// `true` when this reader was opened on a BTI ("da") SSTable (its
    /// `Partitions.db` trie is resident). Used by the candidate-prune loop to decide
    /// whether to hoist the BTI key encoding once per read (issue #1575 / C4).
    pub fn is_bti(&self) -> bool {
        self.bti_partitions_db.is_some()
    }

    /// Candidate-prune presence check using a PRE-ENCODED BTI byte-comparable key,
    /// so a multi-generation `WHERE pk = ?` read hashes+encodes the key ONCE (the
    /// encoding is identical for every candidate) instead of once per candidate
    /// (issue #1575 / C4).
    ///
    /// `encoded` MUST equal `encode_partition_key_for_bti_trie(partition_key)`,
    /// computed once by the caller. For a BTI reader this consults the trie with the
    /// pre-encoded key (no re-hash); for a BIG reader (no `Partitions.db`) it falls
    /// back to the raw-key [`might_contain_partition`] path (the bloom filter is
    /// keyed on the raw key — there is no BTI encoding to hoist), so a mixed or
    /// non-BTI candidate set stays correct. Semantics match
    /// [`might_contain_partition`]: `false` is definitive absence; a BTI trie hit may
    /// be a safe prefix-collision false positive re-verified by the partition scan;
    /// any trie parse error is treated conservatively as "maybe present".
    pub fn might_contain_partition_encoded(&self, partition_key: &[u8], encoded: &[u8; 9]) -> bool {
        if self.bti_partitions_db.is_some() {
            // Issue #2163 (roborev r7): restore the C5 range short-circuit
            // (`locate`'s Step 1) ahead of the trie probe — see the identical
            // restoration + rationale in `might_contain_partition`'s BTI branch.
            // This is the candidate-prune path's OWN entry point (called from
            // `SSTableManager::prune_candidates` for every BTI candidate), so
            // restoring it here is what actually re-covers the multi-generation
            // prune loop the old `locate_encoded` used to gate.
            if self.partition_key_out_of_range(partition_key) {
                crate::storage::sstable::read_work_counters::record_range_short_circuit();
                return false;
            }
            let present = matches!(
                self.lookup_partition_via_bti_trie_encoded(partition_key, encoded),
                Ok(Some(_)) | Err(_)
            );
            if !present {
                // Definitive trie miss on the candidate-prune path → SSTable pruned
                // (issue #2163). The BIG branch below delegates to
                // `might_contain_partition`, which emits its own prune, so there is
                // no double count.
                self.emit_sstable_pruned();
            }
            return present;
        }
        // BIG: no BTI encoding to hoist — the bloom filter hashes the raw key.
        self.might_contain_partition(partition_key)
    }

    /// Enhanced partition iteration using Summary.db reader
    ///
    /// Note: Token-based range queries are not directly supported because Summary.db
    /// does not store token values (Issue #218). Instead, this iterates all summary
    /// entries and returns all partition data.
    ///
    /// For token-based filtering, compute tokens from partition keys after retrieval.
    ///
    /// ## Issue #500: Sequential-scan fallback for writer-produced SSTables
    ///
    /// The Summary.db → Index.db → Data.db lookup path depends on Index.db format
    /// compatibility between writer and reader (digest format vs. raw-key format).
    /// Locally written SSTables emit raw-key Index.db entries that the reader's
    /// digest-based parser cannot resolve, so the lookup loop returns 0 entries
    /// even though Summary.db enumerates the partitions correctly.
    ///
    /// When that happens we fall back to `sequential_scan`, which walks Data.db
    /// directly. For V5CompressedLegacy NB SSTables (the format the writer emits),
    /// `sequential_scan` uses the chunk-stitching path and returns every partition.
    pub async fn iterate_all_partitions(&self) -> Result<Vec<(RowKey, ScanRow)>> {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
                                       // Delegates to the per-call-token sibling with the reader's own field —
                                       // byte-identical to the pre-#2346 behaviour of this method (every
                                       // pre-existing caller keeps its exact cancellation semantics).
        self.iterate_all_partitions_cancellable(&self.scan_cancel)
            .await
    }

    /// Like [`Self::iterate_all_partitions`], but takes an explicit PER-CALL
    /// cancellation token instead of always reading the reader's own
    /// [`SSTableReader::scan_cancel`] field (issue #2346).
    ///
    /// A cached/shared `Arc<SSTableReader>` (a future warm-handle registry) has
    /// no `&mut self` available to set a per-request cancel flag, so this seam
    /// lets the compaction streaming path
    /// ([`SSTableReader::stream_all_partitions_for_compaction`]'s non-stitching
    /// fallback) drive two concurrent enumerations with two INDEPENDENT tokens.
    /// `iterate_all_partitions` itself is UNCHANGED — it delegates here with
    /// `&self.scan_cancel`, so every existing caller keeps identical behaviour.
    pub(crate) async fn iterate_all_partitions_cancellable(
        &self,
        scan_cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
                                       // Index-random-read path (issue #2302): when a `Index.db` is present on a
                                       // BIG SSTable, enumerate EVERY partition via the full Index.db
                                       // partition-offset table instead of the sparse `Summary.db` samples.
                                       //
                                       // The historical loop walked `Summary.db` (only ~1-in-128 partitions) AND
                                       // passed `data_size = 0` to `parse_partition_at_offset` (Index.db never
                                       // stores partition size), so it read zero bytes per entry, resolved zero
                                       // partitions, and SILENTLY fell back to a full `sequential_scan` on EVERY
                                       // read — even with complete, valid components. Each partition's span is
                                       // bounded instead by the successor entry's offset (the last by the
                                       // data-section end), authoritative on-disk structure rather than a size
                                       // guess (issue #28). Index.db offsets are in the UNCOMPRESSED data-section
                                       // domain: for an uncompressed reader that equals the raw file domain; for a
                                       // compressed reader the per-partition slice is mapped to its covering
                                       // compression chunk(s) and decompressed (`read_compressed_offset_window`).
        if self.index_reader.is_some() && self.bti_partitions_db.is_none() {
            match self
                .iterate_all_partitions_via_full_index(scan_cancel)
                .await?
            {
                Some(results) => return Ok(results),
                None => {
                    // Present components that did NOT fully resolve: never silent.
                    tracing::warn!(
                        "SSTable Index.db is present but iterate_all_partitions could not \
                         resolve every partition through the index-random-read path; \
                         falling back to a full sequential scan of Data.db (issue #2302). \
                         This should not happen for a well-formed BIG SSTable — the emitted \
                         Index.db/Summary.db may be malformed or the data-section length is \
                         inconsistent with the index offsets."
                    );
                }
            }
        } else if self.bti_partitions_db.is_none() {
            // BIG SSTable with NO usable `index_reader` (this branch is only reached
            // when `index_reader.is_none()`): the full-index random-read path cannot
            // run, so every iteration falls into the sequential Data.db scan below — a
            // real per-read perf cliff. The WARN trigger keys on Index.db availability,
            // NOT Summary.db presence (roborev job 1612): a present Summary.db only
            // samples ~1-in-128 partitions and never gates the full-index path, so a
            // present-Summary/missing-Index pair is the SAME silent degradation this
            // issue exists to kill and must be named LOUD, never dropped silently into
            // `sequential_scan`. The sub-cause (failed-to-open vs genuinely absent)
            // only shapes the message.
            if self.index_present_but_unloadable {
                // The sibling Index.db EXISTS on disk but failed to open/parse
                // (issue #2302, roborev job 1606): distinct from a genuinely absent
                // file — the component is malformed/truncated.
                tracing::warn!(
                    "SSTable Index.db is present on disk but failed to open/parse \
                     (iterate_all_partitions cannot use the index-random-read path) \
                     and falls back to a full sequential scan of Data.db (a per-read \
                     perf cliff, issue #2302). The Index.db component may be malformed \
                     or truncated — regenerate the SSTable or restore an intact Index.db."
                );
            } else {
                // Index.db genuinely absent. Whether or not Summary.db loaded, the
                // full-index path is unavailable, so this is still the degraded
                // full-scan cliff (issues #2295/#2302). Name the absent components so
                // an operator can spot an incomplete snapshot directory served with
                // only Data.db (or Data.db + Summary.db) instead of silently paying
                // the full-scan cost on every read.
                let missing = if self.summary_reader.is_none() {
                    "Index.db and Summary.db"
                } else {
                    "Index.db"
                };
                tracing::warn!(
                    "SSTable has no usable random-access partition index ({missing} \
                     absent): iterate_all_partitions falls back to a full sequential \
                     scan of Data.db, materializing and sorting every partition on \
                     each read (a per-read perf cliff, issues #2295/#2302). For \
                     snapshot reads, ensure the snapshot directory holds the full \
                     SSTable component set (Index.db + Summary.db), not just Data.db."
                );
            }
        }

        // Fallback path: sequential walk of Data.db.
        // Used when Summary.db is absent OR when the Index.db lookup loop returned
        // no entries (Issue #500: writer-produced SSTables emit a raw-key Index.db
        // format the reader's digest-based parser does not resolve).
        let table_id = self.scan_table_id();
        let schema = self.schema.as_deref();
        self.sequential_scan(&table_id, None, None, None, schema, scan_cancel)
            .await
    }

    /// Whether this reader has a **usable random-access partition index** — a
    /// loaded `Index.db` (BIG format; issue #2302 resolves CQLite-written Index.db
    /// too) or a `Partitions.db` trie (BTI format) — that lets
    /// [`iterate_all_partitions`](Self::iterate_all_partitions) and point lookups
    /// avoid a full sequential Data.db scan.
    ///
    /// This is a **capability probe**: it must report `true` only for the exact
    /// states that route to the non-sequential path. Both point lookups
    /// (`index_reader.lookup_partition` / the BTI trie) and `iterate_all_partitions`
    /// (the full-`Index.db` walk / BTI) gate on `index_reader`/`bti_partitions_db`
    /// — never on `summary_reader`.
    ///
    /// Issue #2302 (roborev job 1613): `summary_reader` is DELIBERATELY excluded. A
    /// `Summary.db` only samples ~1-in-128 partitions and never gates the
    /// random-access route, so a present-Summary / absent-Index BIG reader is
    /// degraded — every read falls into `sequential_scan` and WARNs. Including
    /// `summary_reader` here made the probe report "fast path available" for that
    /// degraded reader (a fidelity lie). Narrowing to the truly-supporting state
    /// keeps the probe honest.
    ///
    /// Snapshot-completeness probe (issue #2295): a directory served with only
    /// `Data.db` (its index siblings absent) still opens but reports `false`; a
    /// complete component set loads `Index.db` (→ `index_reader`) and reports
    /// `true`. All current callers (this module's own tests + the #2295 snapshot
    /// test) use this probe to mean "does the fast/index path apply," so a single
    /// narrowed predicate covers every caller without a split API.
    pub fn has_partition_index(&self) -> bool {
        self.index_reader.is_some() || self.bti_partitions_db.is_some()
    }

    /// Build the TableId used for fallback `sequential_scan` from header metadata.
    ///
    /// The reader populates `header.keyspace` / `header.table_name` from either the
    /// SSTable header or the parent directory path. When the V5CompressedLegacy
    /// stitching path is used, table_id matching is skipped, so any non-empty value
    /// is accepted; for other formats this returns the qualified `keyspace.table`
    /// form so the scan filter matches.
    pub(in crate::storage::sstable::reader) fn scan_table_id(&self) -> TableId {
        let keyspace = &self.header.keyspace;
        let table_name = &self.header.table_name;
        if !keyspace.is_empty() && !table_name.is_empty() {
            TableId::from(format!("{}.{}", keyspace, table_name))
        } else if !table_name.is_empty() {
            TableId::from(table_name.as_str())
        } else {
            TableId::from("default")
        }
    }

    /// Token range iteration (deprecated - tokens not stored in Summary.db)
    ///
    /// This method is kept for API compatibility but simply delegates to
    /// `iterate_all_partitions()` since Summary.db does not store token values.
    /// Token filtering should be done by the caller after retrieval.
    #[deprecated(
        since = "0.1.0",
        note = "Summary.db does not store tokens. Use iterate_all_partitions() and filter by computed tokens."
    )]
    pub async fn iterate_token_range(
        &self,
        _start_token: i64,
        _end_token: i64,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        // Token values are not stored in Summary.db (Issue #218)
        // Delegate to all-partition iteration
        self.iterate_all_partitions().await
    }

    /// Get min/max timestamps from Statistics.db reader
    pub async fn get_timestamp_range(&self) -> Result<Option<(i64, i64)>> {
        if let Some(statistics_reader) = &self.statistics_reader {
            let (min_ts, max_ts) = statistics_reader.timestamp_range();
            debug!(
                "Retrieved timestamp range from Statistics.db: {} to {}",
                min_ts, max_ts
            );
            return Ok(Some((min_ts, max_ts)));
        }
        Ok(None)
    }

    /// Get token coverage (deprecated - tokens not stored in Summary.db)
    ///
    /// Note: As of Issue #218, Summary.db does not store token values.
    /// This method now returns None since token coverage cannot be determined
    /// from Summary.db alone. Token computation requires partition keys and
    /// the partitioner algorithm.
    #[deprecated(
        since = "0.1.0",
        note = "Summary.db does not store tokens. Compute tokens from partition keys using the partitioner."
    )]
    pub async fn get_token_coverage(&self) -> Result<Option<(i64, i64)>> {
        // Token values are not stored in Summary.db (Issue #218)
        // Return None - caller should compute tokens from partition keys if needed
        debug!("get_token_coverage: Summary.db does not store token values");
        Ok(None)
    }
}
