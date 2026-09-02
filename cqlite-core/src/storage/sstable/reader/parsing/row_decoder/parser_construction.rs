//! Construction and small self-describing predicates of
//! [`V5CompressedLegacyParser`] — the constructor, the builder-style
//! configuration setters, and the two `Statistics.db`/version-gate predicates
//! that describe the sstable rather than decode it.
//!
//! Split out of `row_decoder/mod.rs` under the campsite rule (epic #1116); the
//! `impl` block is unchanged, so every method keeps its path and visibility.

use super::*;

impl V5CompressedLegacyParser {
    /// Create a new V5CompressedLegacy parser
    ///
    /// # Arguments
    /// * `keyspace` - Keyspace name
    /// * `table_name` - Table name
    /// * `min_timestamp` - Minimum timestamp for delta decoding (from Statistics.db)
    /// * `min_local_deletion_time` - Minimum local deletion time for delta decoding (from Statistics.db)
    /// * `min_ttl` - Minimum TTL for delta decoding (from Statistics.db)
    pub fn new(
        keyspace: String,
        table_name: String,
        min_timestamp: i64,
        min_local_deletion_time: i64,
        min_ttl: Option<i64>,
    ) -> Self {
        // Default to nb-compatible BIG gates when not supplied by the caller.
        // Use the infallible nb_fallback() constructor (no expect/unwrap in lib code).
        let version_gates = std::sync::Arc::new(VersionGates::Big(BigVersionGates::nb_fallback()));
        Self {
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
            udt_registry: None,
            version_gates,
            read_shadowing: false,
            // Issue #1741 (F2): sample the read clock ONCE per parser (== once per
            // read/scan operation); every block/partition below reuses this value.
            now_secs: now_epoch_secs(),
        }
    }

    /// Issue #1741: enable read-side SELECT-semantic shadowing on this parser. Call
    /// with `true` ONLY when building the parser for a user-facing query read; leave
    /// the default (`false`) for physical/verification/compaction/delta reads.
    pub fn with_read_shadowing(mut self, on: bool) -> Self {
        self.read_shadowing = on;
        self
    }

    /// Set the version gates for version-sensitive parsing decisions (VG1 plumbing).
    ///
    /// Call this after `new()` with the `Arc<VersionGates>` from `SSTableReader`.
    /// Until VG3 lands, passing gates here has no effect on parsing behaviour —
    /// the gate values are stored for future use only.
    pub fn with_version_gates(mut self, gates: std::sync::Arc<VersionGates>) -> Self {
        self.version_gates = gates;
        self
    }

    /// Set the UDT registry for resolving short UDT type names in frozen collections (Issue #238)
    pub fn with_udt_registry(mut self, registry: UdtRegistry) -> Self {
        self.udt_registry = Some(registry);
        self
    }

    /// Return `true` when the version gates indicate `hasUIntDeletionTime` (oa / da).
    ///
    /// Authority: BigFormat.java:409 — `hasUintDeletionTime = version.compareTo("oa") >= 0`
    #[inline]
    pub(super) fn has_uint_deletion_time(&self) -> bool {
        match self.version_gates.as_ref() {
            VersionGates::Big(g) => g.has_uint_deletion_time,
            VersionGates::Bti(g) => g.has_uint_deletion_time,
        }
    }

    /// Issue #1741 (Finding 2): `true` when this SSTable's authoritative
    /// EncodingStats prove it carries NO deletions of any kind — hence NO range
    /// tombstones. A clustering-slice read can then keep the O(slice) row-index
    /// fast-forward and skip prefix priming entirely (a range tombstone opening
    /// before the slice is impossible).
    ///
    /// `min_local_deletion_time` is `EncodingStats.minLocalDeletionTime`, the MIN
    /// of every cell's `localDeletionTime`. A live cell contributes the LIVE
    /// sentinel `Cell.NO_DELETION_TIME == Integer.MAX_VALUE`; a partition/row/
    /// range/cell tombstone OR an expiring cell contributes a smaller value. So
    /// the min equals `Integer.MAX_VALUE` iff the SSTable has no deletion and no
    /// TTL. This OVER-approximates range-tombstone presence (a cell tombstone or
    /// TTL also trips it), which is safe: priming then runs and stays correct. No
    /// stats (`min == 0` from the `build_v5_parser` fallback) conservatively primes.
    /// No heuristics — authoritative metadata only (issue #28).
    #[inline]
    pub(super) fn sstable_may_have_range_tombstones(&self) -> bool {
        // Integer.MAX_VALUE — Cassandra `Cell.NO_DELETION_TIME` LIVE sentinel.
        const NO_DELETION_TIME: i64 = i32::MAX as i64;
        self.min_local_deletion_time != NO_DELETION_TIME
    }

    /// Whether the bytes at `offset` begin a new partition header, WITHOUT
    /// consuming them.
    ///
    /// This is the NO-HEURISTICS approach: we validate the actual structure
    /// instead of guessing from byte patterns. Issue #1641 (K2) made it
    /// non-allocating on the fast-reject paths — it delegates to
    /// [`peek_partition_boundary`], which shares the structural walk of
    /// `parse_partition_header_full` (via `scan_partition_header`) but always
    /// skips the success-path key `to_vec` and the `PARTITION_HEADER_TRY_PARSES`
    /// counter. The marker pre-check and readiness gate allocate nothing; the
    /// strict scan on a `Ready` buffer may still build a discarded error string
    /// on a structural mismatch. The boolean result is identical to the former
    /// allocating implementation (marker pre-check + full-parse `is_ok`), proved
    /// by the `peek_matches_full_parse` proptest.
    ///
    /// # Arguments
    /// * `data` - Binary data buffer
    /// * `offset` - Offset to check
    ///
    /// # Returns
    /// * `true` if a valid partition header can be parsed at this offset
    /// * `false` if parsing fails (likely a row header or invalid data)
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition boundary detection
    ///
    /// [`peek_partition_boundary`]: Self::peek_partition_boundary
    #[doc(hidden)]
    pub fn peek_is_partition_header(&self, data: &[u8], offset: usize) -> bool {
        matches!(
            self.peek_partition_boundary(data, offset),
            BoundaryPeek::Header
        )
    }
}
