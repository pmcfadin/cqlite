//! Next-partition (successor) offset resolution for the single-partition seek
//! window (issue #953 / #951; O(depth) local walk, issue #2058).
//!
//! The within-SSTable single-partition seek bounds its decompression window to
//! exactly one partition's byte extent `[target_offset, successor_offset)` using
//! authoritative index/trie layout metadata (never a heuristic boundary scan).
//! Both helpers are gated `#[cfg(not(feature = "tombstones"))]` like the seek path
//! they serve: their callers are `scan_single_partition_clustering`,
//! `read_single_partition_for_compaction`, and `big_reverse_partition_rows`, which
//! the `tombstones` build compiles out (it serves single-partition reads via a full
//! scan + filter, not a seek).
//!
//! Split out of `partition_lookup.rs` (campsite / epic #1116) so the B4 key-cache
//! wiring (issue #1570) does not push that file over the source-size threshold.

// `SSTableReader` is needed by the always-compiled test oracle at the end of this
// file as well as the `not(tombstones)` seek helpers, so its import is unconditional
// when either is present.
#[cfg(any(not(feature = "tombstones"), feature = "observability-testing"))]
use super::SSTableReader;
#[cfg(not(feature = "tombstones"))]
use crate::{Error, Result};

#[cfg(not(feature = "tombstones"))]
impl SSTableReader {
    /// Authoritatively resolve the UNCOMPRESSED `Data.db` offset of the partition
    /// that immediately FOLLOWS the partition starting at `target_offset` (whose
    /// partition key is `partition_key`), used to bound the within-SSTable seek's
    /// decompression window to exactly one partition's byte extent (issue #953 / #951
    /// MEDIUM).
    ///
    /// The successor's start offset is the partition's exclusive END: a partition
    /// occupies `[target_offset, successor_offset)`, so decompressing the chunks
    /// covering that half-open range materializes every byte of the target
    /// partition (including a row/cell that spans multiple compression chunks)
    /// without reading any of the next partition. This is authoritative metadata
    /// (the index/trie's own partition layout), NOT a heuristic boundary scan.
    ///
    /// Returns:
    /// - `Ok(Some(off))` — the next partition's start offset (`off > target_offset`).
    /// - `Ok(None)` — `target_offset` is the LAST partition (no successor); the
    ///   caller bounds the end with the authoritative data-section length.
    ///
    /// Resolution is per index format:
    /// - **BTI (`da`)** — the successor is resolved by a SINGLE O(depth) strict-ceiling
    ///   walk of the `Partitions.db` trie keyed on `partition_key`'s byte-comparable
    ///   token ([`partition_successor_in_bti_slice`]). Because the trie stores
    ///   partitions in byte-comparable key order — which for `Murmur3Partitioner`
    ///   equals token order equals `Data.db` layout order — the trie IN-ORDER
    ///   successor is exactly the OFFSET successor (the smallest partition start
    ///   strictly greater than `target_offset`). This replaces the pre-#2058
    ///   whole-trie DFS that enumerated + sorted EVERY partition offset into a
    ///   `OnceLock` array on the first seek. A defensive `> target_offset` guard
    ///   fails safe to `None` (data-section-length bound, a safe over-read that never
    ///   truncates) should a resolved successor not exceed the target — a case the
    ///   real-fixture oracle test proves does not occur (`tests/issue_2058_*`).
    /// - **BIG (`nb`)** — `Index.db` `partition_entries` are sorted by key (==
    ///   `Data.db` order); the successor is the smallest `data_offset` strictly
    ///   greater than `target_offset`.
    ///
    /// [`partition_successor_in_bti_slice`]: crate::storage::sstable::bti::partition_successor_in_bti_slice
    pub(crate) async fn successor_partition_offset(
        &self,
        target_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        if self.bti_partitions_db.is_some() {
            return self.bti_successor_partition_offset(target_offset, partition_key);
        }

        // BIG (`nb`): scan the sorted Index.db entries for the smallest data_offset
        // strictly greater than target_offset. `partition_entries` are emitted in
        // key (== Data.db) order, but we take the min over `> target` defensively
        // rather than rely on positional adjacency.
        if let Some(index_reader) = &self.index_reader {
            // Issue #2412 Stage 2: a lazily-opened reader defers the full parse to
            // first use — this successor scan IS that first use. No-op for an
            // eagerly-opened reader.
            index_reader.ensure_materialized(&self.scan_cancel).await?;
            let successor = index_reader
                .get_partition_entries()
                .iter()
                .map(|e| e.data_offset)
                .filter(|&o| o > target_offset)
                .min();
            return Ok(successor);
        }

        // No index available: cannot resolve a successor authoritatively.
        Ok(None)
    }

    /// BTI (`da`) next-partition successor via the O(depth) local strict-ceiling walk
    /// (issue #2058), resolving a WIDE successor's `RowsOffset` through `Rows.db`.
    ///
    /// The walk keys on `partition_key`'s byte-comparable token, so the successor it
    /// returns is byte-identical to the pre-#2058 whole-trie DFS + sorted-offset
    /// `partition_point(<= target_offset)`, for every partition (proven by the
    /// real-fixture oracle test). No shared state / `OnceLock`, so concurrent point
    /// reads on the same reader never race or double-walk.
    fn bti_successor_partition_offset(
        &self,
        target_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        use crate::storage::sstable::bti::{
            encode_partition_key_for_bti_trie_uncounted, partition_successor_in_bti_slice,
            resolve_rows_db_entry_uncounted, BtiPartitionLocation,
        };

        let Some(partitions_db) = &self.bti_partitions_db else {
            return Ok(None);
        };

        // Encode WITHOUT the C4 `KEY_HASH_CALLS` counter: a point read already hashed
        // this exact key once (the candidate prune), and the one-hash-per-read
        // invariant (issue #1575) must hold — the successor bound is not a new query
        // key, it re-encodes the same one.
        let encoded = encode_partition_key_for_bti_trie_uncounted(partition_key);

        let location = partition_successor_in_bti_slice(partitions_db.as_slice(), &encoded)
            .map_err(|e| {
                Error::corruption(format!(
                    "BTI Partitions.db next-partition successor walk failed while resolving the \
                     seek end bound (key len={}): {e}",
                    partition_key.len()
                ))
            })?;

        let successor_offset = match location {
            // Narrow successor: its Data.db start is the target's exclusive end.
            Some(BtiPartitionLocation::DataOffset(off)) => Some(off),
            // Wide successor: recover its Data.db start (`data_position`) via Rows.db.
            // Uncounted so the L1 clustering-window `ROWS_DB_ENTRY_RESOLVES == 1`
            // invariant (issue #1647) is not perturbed by this seek-bound resolve.
            Some(BtiPartitionLocation::RowsOffset(rows_offset)) => {
                let rows_db = self.bti_rows_db.as_ref().ok_or_else(|| {
                    Error::corruption(format!(
                        "BTI successor walk returned RowsOffset({rows_offset}) but this reader has \
                         no Rows.db; the SSTable is structurally invalid (Rows.db is required for \
                         wide partitions)."
                    ))
                })?;
                let header =
                    resolve_rows_db_entry_uncounted(rows_db.as_slice(), rows_offset as usize)
                        .map_err(|e| {
                            Error::corruption(format!(
                        "BTI Rows.db row-index entry at RowsOffset({rows_offset}) is unreadable \
                             while resolving the next-partition seek bound: {e}"
                    ))
                        })?;
                Some(header.data_position)
            }
            // No successor: `partition_key` is the LAST partition.
            None => None,
        };

        // Fail-safe (never a truncating bound): the resolved successor MUST start
        // strictly after the target partition. If it does not (a pathological trie /
        // token-collision shape the oracle test proves never occurs on real data),
        // return `None` so the caller bounds the end with the authoritative
        // data-section length — a safe over-read, never a truncation.
        Ok(successor_offset.filter(|&off| off > target_offset))
    }
}

#[cfg(not(feature = "tombstones"))]
impl SSTableReader {
    /// The authoritative UNCOMPRESSED length of this SSTable's `Data.db` data
    /// section — the exclusive end bound the LAST partition takes when
    /// [`successor_partition_offset`](Self::successor_partition_offset) returns
    /// `None` (there is no successor).
    ///
    /// The source depends on whether the table is compressed, and the two are NOT
    /// interchangeable:
    ///
    /// - **Compressed**: `CompressionInfo.db`'s `data_length` field, which Cassandra
    ///   writes as the total UNCOMPRESSED data length. This is the ONLY source for a
    ///   compressed table. If it is absent or zero the answer is `None` — falling
    ///   back to the file length would silently substitute the **compressed** size,
    ///   producing a too-small extent that would then be published as a *measured*
    ///   number. A missing measurement must read as missing.
    /// - **Uncompressed**: the `Data.db` file length. For an uncompressed SSTable the
    ///   file IS the data section, so its length is the uncompressed length — and the
    ///   production write surface is uncompressed-only (#1406).
    ///
    /// `None` when no authoritative length exists, in which case the caller fails
    /// closed and reports `size_source = unavailable` rather than guessing one.
    pub(crate) fn uncompressed_data_section_len(&self) -> Option<u64> {
        match self.compression_info.as_ref() {
            // Compressed: `data_length` or nothing. NEVER the file length.
            Some(info) => (info.data_length > 0).then_some(info.data_length),
            None => {
                let len = self.point_source.len();
                (len > 0).then_some(len)
            }
        }
    }

    /// MEASURE this partition's on-disk extent as the successor gap.
    ///
    /// Returns `Ok(Some(bytes))` when the extent is authoritative:
    /// `successor_offset - data_offset`, or
    /// `uncompressed_data_section_len() - data_offset` for the last partition.
    /// Returns `Ok(None)` when it is genuinely unknowable (no index, or no
    /// data-section length) — the caller then records `unavailable` and contributes
    /// zero bytes rather than estimating (no-heuristics, #28).
    ///
    /// This is the same authoritative index-layout metadata the single-partition
    /// seek uses to bound its decompression window, read at the same reader-level
    /// granularity as the B4 key-offset cache — so a caller at the LOGICAL
    /// point-read boundary can obtain a byte weight without counting per-SSTable
    /// probes (issue #2827, design D2/D6).
    ///
    /// The extent is in UNCOMPRESSED offsets, which is exactly the domain a
    /// decoded-size multiplier is applied to.
    pub(crate) async fn measure_partition_extent(
        &self,
        data_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        // Explicit guard, not an implicit coupling: `successor_partition_offset` is
        // shared with the READ path, where forcing materialization is correct and
        // intended. Reached from the probe it must not be, so refuse before the call
        // rather than relying on `partition_residency` having already established
        // residency (which today implies the index is resident). Costs one
        // `OnceCell` peek.
        if !self.is_bti() {
            match self.index_reader.as_ref() {
                Some(index) if index.is_materialized() => {}
                _ => return Ok(None),
            }
        }
        let end = match self
            .successor_partition_offset(data_offset, partition_key)
            .await?
        {
            Some(successor) => successor,
            None => match self.uncompressed_data_section_len() {
                Some(len) => len,
                None => return Ok(None),
            },
        };
        Ok(end.checked_sub(data_offset).filter(|&gap| gap > 0))
    }
}

/// Whether this SSTable holds a partition — the AUTHORITATIVE, non-emitting answer
/// the #2827 probe needs to price an access.
#[cfg(not(feature = "tombstones"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionResidency {
    /// Definitively absent from this SSTable: it contributes nothing to the access,
    /// and its absence is not a gap in the measurement.
    NotHeld,
    /// Present, starting at this uncompressed `Data.db` offset.
    HeldAt(u64),
    /// Could not be determined. The caller MUST fail closed — reporting the access
    /// `unavailable` — rather than assume absence.
    Unknown,
}

#[cfg(not(feature = "tombstones"))]
impl SSTableReader {
    /// Resolve whether this SSTable holds `partition_key`, WITHOUT emitting any
    /// metric.
    ///
    /// # Why the probe cannot just consult the key cache
    ///
    /// The key→offset cache answers "is this location memoised", not "does this
    /// SSTable hold this key". A miss means *either* — the cache is byte-budgeted
    /// and evicts, has a disabled mode that returns `None` unconditionally, misses
    /// entirely when the reader has no generation identity, and is populated by only
    /// one of the resolution paths. Reading a miss as absence lets ONE surviving
    /// cached generation plus one evicted-but-held generation report a partial sum
    /// as a fully measured extent: the working set is under-priced, which flatters
    /// the cache. That is the direction the whole fail-closed design exists to
    /// prevent, and the spec forbids it outright ("an access for which ANY resolved
    /// SSTable yielded no authoritative extent SHALL be `unavailable`"; "SHALL NOT
    /// silently under-report").
    ///
    /// So residency is resolved from the same authoritative structures the read
    /// itself uses, and the three states are kept DISTINCT:
    ///
    /// - **BTI** — the `Partitions.db` trie is the authoritative presence oracle: a
    ///   trie miss is definitive absence, a hit yields the offset directly (narrow)
    ///   or via the `Rows.db` row-index entry (wide). A corrupt trie is `Unknown`.
    /// - **BIG** — the bloom filter never reports a false negative, so
    ///   `might_contain == false` is definitive absence. Otherwise the raw-key
    ///   `Index.db` map resolves the offset. An `Index.db` MISS is **not** definitive
    ///   absence (#1572), so it is `Unknown` rather than `NotHeld`; with a bloom in
    ///   front this is only reached on a bloom false positive.
    ///
    /// # What it does not do
    ///
    /// - **Emits no metric.** It uses the slice-level trie primitive and the
    ///   resident index map directly, never the counter-emitting lookup façades, so
    ///   switching the probe on cannot perturb `cqlite.read.partition_lookup.total`,
    ///   `cqlite.read.bloom.checks` or `cqlite.read.sstables_pruned`.
    /// - **Bumps no read-work counter.** The BTI path uses the `_uncounted` key
    ///   encode and `Rows.db` resolve, so the C4 one-hash-per-read (#1575) and L1
    ///   single-resolve (#1647) assertions still hold with the probe enabled.
    /// - **Mutates no reader state.** It never calls `ensure_materialized`, so it
    ///   cannot defeat #2412's lazy Summary-guided open or add resident index bytes
    ///   to the process.
    ///
    /// The last one has a cost, stated rather than hidden: a BIG generation whose
    /// `Index.db` is not already resident cannot be priced, and is reported
    /// `Unknown` → `size_source = unavailable` → the window is refused. Pricing a
    /// lazily-opened BIG table therefore needs the index resident for some other
    /// reason; the probe will not materialize it to get an answer.
    pub(crate) async fn partition_residency(&self, partition_key: &[u8]) -> PartitionResidency {
        // C5 authoritative range short-circuit: provably outside this SSTable's
        // key bound is definitive absence, for either format.
        if self.partition_key_out_of_range(partition_key) {
            return PartitionResidency::NotHeld;
        }

        if let Some(partitions_db) = &self.bti_partitions_db {
            use crate::storage::sstable::bti::{
                encode_partition_key_for_bti_trie_uncounted, lookup_partition_in_bti_slice,
                resolve_rows_db_entry_uncounted, BtiPartitionLocation,
            };
            // UNCOUNTED variants: the probe must not bump the read-work counters
            // (`KEY_HASH_CALLS`, `ROWS_DB_ENTRY_RESOLVES`). Those back the C4
            // one-hash-per-read (#1575) and L1 single-resolve (#1647) assertions, so
            // a counted call here would fail those tests once per candidate
            // generation and inflate a `work-counters` build's reported read work.
            let encoded = encode_partition_key_for_bti_trie_uncounted(partition_key);
            return match lookup_partition_in_bti_slice(partitions_db.as_slice(), &encoded) {
                // A trie miss is AUTHORITATIVE absence for BTI.
                Ok(None) => PartitionResidency::NotHeld,
                Ok(Some(BtiPartitionLocation::DataOffset(off))) => PartitionResidency::HeldAt(off),
                Ok(Some(BtiPartitionLocation::RowsOffset(rows_offset))) => {
                    match self.bti_rows_db.as_ref() {
                        Some(rows_db) => {
                            match resolve_rows_db_entry_uncounted(
                                rows_db.as_slice(),
                                rows_offset as usize,
                            ) {
                                Ok(header) => PartitionResidency::HeldAt(header.data_position),
                                Err(_) => PartitionResidency::Unknown,
                            }
                        }
                        None => PartitionResidency::Unknown,
                    }
                }
                Err(_) => PartitionResidency::Unknown,
            };
        }

        // BIG: the bloom filter never yields a false negative.
        if let Some(bloom) = &self.bloom_filter {
            if !bloom.might_contain(partition_key) {
                return PartitionResidency::NotHeld;
            }
        }
        let Some(index_reader) = &self.index_reader else {
            return PartitionResidency::Unknown;
        };
        // NEVER force materialization. `ensure_materialized` is `File::open` +
        // `read_to_end` + a full parse that PERMANENTLY populates the reader's
        // `OnceCell`, so calling it here would defeat #2412's lazy Summary-guided
        // open for every candidate generation and permanently change the process
        // memory profile — an observability probe must not mutate the read path's
        // resident state. A generation whose index is not already resident is
        // therefore `Unknown` (fail closed), not silently absent.
        if !index_reader.is_materialized() {
            return PartitionResidency::Unknown;
        }
        match index_reader.lookup_partition(partition_key) {
            Some(entry) => PartitionResidency::HeldAt(entry.data_offset),
            // An `Index.db` miss is NOT definitive absence (#1572): the index may be
            // truncated or incomplete, so the read path re-checks by scanning. The
            // probe cannot re-check cheaply, so it fails closed.
            None => PartitionResidency::Unknown,
        }
    }
}

/// Test-only extent oracle, compiled in EVERY build configuration.
///
/// It lives outside the `not(tombstones)` impl block above because the cross-crate
/// test that uses it (`cqlite-flight`) has no `tombstones` feature of its own to gate
/// on, so the method has to exist even in the build where the seek machinery it needs
/// does not.
#[cfg(feature = "observability-testing")]
impl SSTableReader {
    /// This generation's OWN measured extent for `partition_key`, or `None` when it
    /// does not hold the key or the extent is unresolvable.
    ///
    /// Exists so a test can build an ORACLE for the per-generation sum independently
    /// of `AccessWeightBuilder` — evidencing the sum with the accumulator that
    /// computes it would close nothing.
    ///
    /// Gated on `observability-testing`, which the cross-crate test that needs it
    /// already enables (`cqlite-flight/observability-testing` turns on the core
    /// feature), so it is absent from every production build.
    ///
    /// Unlike the probe, this helper MAY force `Index.db` materialization. The probe
    /// deliberately will not (it would defeat #2412's lazy open and change the process
    /// memory profile, so it reports `unavailable` instead), but an oracle opening its
    /// own fresh readers has no read to have materialized them for it — and an oracle
    /// that silently resolved nothing would make the comparison it exists for
    /// vacuous.
    pub async fn measured_partition_extent_for_test(&self, partition_key: &[u8]) -> Option<u64> {
        #[cfg(not(feature = "tombstones"))]
        {
            if let Some(index) = self.index_reader.as_ref() {
                index.ensure_materialized(&self.scan_cancel).await.ok()?;
            }
            match self.partition_residency(partition_key).await {
                PartitionResidency::HeldAt(offset) => self
                    .measure_partition_extent(offset, partition_key)
                    .await
                    .ok()
                    .flatten(),
                PartitionResidency::NotHeld | PartitionResidency::Unknown => None,
            }
        }
        // `tombstones`: the seek/residency machinery is compiled out with the path it
        // serves, so no extent is resolvable — the same answer the probe itself gives
        // in that build.
        #[cfg(feature = "tombstones")]
        {
            let _ = partition_key;
            None
        }
    }
}
