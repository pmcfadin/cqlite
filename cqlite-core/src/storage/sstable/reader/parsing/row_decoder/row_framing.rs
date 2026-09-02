use super::*;

/// `DeletionTime.Serializer` LIVE sentinel for oa/da (`hasUIntDeletionTime`)
/// partitions: a single byte `0x80` (`IS_LIVE_DELETION = 0b1000_0000`) encodes a
/// live partition; any other leading byte introduces the full 12-byte deleted
/// form (8-byte `markedForDeleteAt` + 4-byte unsigned `localDeletionTime`).
/// Authority: `DeletionTime.java:208` (`Serializer.serialize`).
pub(super) const OA_IS_LIVE_DELETION: u8 = 0x80;

/// Whether the leading partition header in a sliding-window chunk buffer can be
/// safely parsed yet. Computed WITHOUT consuming bytes so the two sliding
/// parsers ([`V5CompressedLegacyParser::parse_one_partition_with_timestamps`] and
/// [`V5CompressedLegacyParser::parse_one_partition_for_compaction`]) agree on the
/// need-more decision for both the nb (fixed 12-byte) and oa/da (1-byte LIVE /
/// 12-byte DELETED) `DeletionTime` forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PartitionHeaderReadiness {
    /// Enough bytes are present to parse the full header, INCLUDING the complete
    /// partition `DeletionTime` for its live/deleted form. Parsing cannot fail
    /// due to truncation.
    Ready,
    /// The header — or, for an oa/da deleted partition, its 12-byte
    /// `DeletionTime` — is split across the chunk boundary. More bytes must be
    /// appended before it can be parsed (non-final chunk → `NeedMore`).
    Incomplete,
    /// The header shape is invalid (zero or over-long key length): the caller
    /// should skip one byte to resynchronise.
    Malformed,
}

/// Result of the non-allocating partition-BOUNDARY peek (issue #1641, K2).
///
/// The post-row emit loop asks "does the next thing begin a new partition
/// header?" after every row. On `main` that ran the FULL allocating
/// [`V5CompressedLegacyParser::parse_partition_header_full`] as a trial (throwaway
/// key `to_vec` + eager `format!` error strings + a `PARTITION_HEADER_TRY_PARSES`
/// increment) purely to learn a boolean. This enum is returned by
/// [`V5CompressedLegacyParser::peek_partition_boundary`], which reaches the same
/// verdict by READING bytes — no allocation, no error strings, no gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundaryPeek {
    /// The bytes at the offset are structurally a partition header — i.e. the
    /// full parser would return `Ok` here. The caller runs the real
    /// (allocating) parse exactly once, at this confirmed start.
    Header,
    /// Definitely NOT a partition header at this offset: the leading byte is an
    /// END_OF_PARTITION / range-tombstone marker, the key length is zero, or the
    /// header is structurally invalid (e.g. an illegal oa/da IS_LIVE byte) so the
    /// full parser would return `Err` on the complete bytes.
    NotHeader,
    /// The header — or, for an oa/da deleted partition, its full `DeletionTime` —
    /// is not entirely present yet (truncated / split across a chunk boundary).
    /// The caller should request more bytes rather than decide.
    NeedMoreBytes,
}

/// Byte-offset layout of a structurally-valid partition header, produced WITHOUT
/// allocating the key (issue #1641, K2). Shared by the allocating full parser and
/// the non-allocating boundary peek so their structural rules cannot drift.
struct PartitionHeaderLayout {
    /// Range of the partition key bytes within the input buffer (not copied).
    key_range: std::ops::Range<usize>,
    /// Offset immediately after the header (start of the first row / marker).
    next_offset: usize,
    /// Partition-level deletion `(markedForDeleteAt µs, localDeletionTime s)`, or
    /// `None` for a live partition. Same contract as
    /// [`V5CompressedLegacyParser::parse_partition_header_full`].
    partition_deletion: Option<(i64, i32)>,
}

impl V5CompressedLegacyParser {
    /// Decide whether the leading partition header in `data` is fully present.
    ///
    /// Issue #1741 (roborev HIGH): the oa/da (`hasUIntDeletionTime`) partition
    /// `DeletionTime` is 1 byte only when LIVE (the [`OA_IS_LIVE_DELETION`]
    /// sentinel); a DELETED partition carries the full 12-byte form. Sizing the
    /// header minimum at a flat 1 byte let a deleted partition header that was
    /// split across a NON-FINAL chunk (partition key + only the first deletion
    /// byte present) pass the guard, so `parse_partition_header_full` failed
    /// mid-buffer and the sliding parser returned `Emitted(1)` instead of
    /// `NeedMore` — desyncing the scan and, on the compaction path, dropping the
    /// partition tombstone (data-resurrection risk).
    ///
    /// This peeks the deletion-time discriminator to size the header correctly
    /// and, when a `Ready` verdict is returned, guarantees the subsequent
    /// `parse_partition_header_full` has every byte it needs — so a truncated
    /// deletion-time can never be mis-parsed as a complete partition. No
    /// heuristics: the branch keys off the authoritative `hasUIntDeletionTime`
    /// version-gate flag and the canonical `DeletionTime` sentinel.
    pub(super) fn partition_header_readiness(&self, data: &[u8]) -> PartitionHeaderReadiness {
        // Cassandra partition key size limits (see the block loop guards).
        const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
        const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 length field limit

        // flags(1) + key_len(1) must both be present before anything else.
        if data.len() < 2 {
            return PartitionHeaderReadiness::Incomplete;
        }
        let key_len = data[1] as usize;
        if key_len == 0 || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE) {
            return PartitionHeaderReadiness::Malformed;
        }

        // Offset of the DeletionTime, immediately after flags + key_len + key.
        let deletion_offset = 2 + key_len;
        let deletion_time_min = if self.has_uint_deletion_time() {
            // oa/da: peek the DeletionTime discriminator to size the header.
            match data.get(deletion_offset) {
                // Discriminator byte itself is not present yet — split header.
                None => return PartitionHeaderReadiness::Incomplete,
                // LIVE sentinel (0x80): exactly 1 byte.
                Some(&b) if (b & OA_IS_LIVE_DELETION) != 0 => 1,
                // Any other leading byte introduces the full 12-byte deleted form.
                Some(_) => 12,
            }
        } else {
            // nb: fixed 12-byte signed DeletionTime (4-byte LDT + 8-byte MFDA).
            12
        };

        if deletion_offset + deletion_time_min > data.len() {
            PartitionHeaderReadiness::Incomplete
        } else {
            PartitionHeaderReadiness::Ready
        }
    }

    /// Parse row flags only (Issue #213 fix: split from parse_row_header)
    ///
    /// # Format
    /// ```text
    /// [row_flags: u8]
    /// [extended_flags: u8 if 0x80 set]
    /// ```
    ///
    /// Returns (row_flags, extended_flags, bytes_consumed)
    pub(super) fn parse_row_flags(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(u8, Option<u8>, usize)> {
        let mut pos = offset;

        // Read row flags
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading row flags",
            ));
        }
        let row_flags = data[pos];
        pos += 1;

        debug!(
            "V5CompressedLegacy: Row flags=0x{:02x} at offset {}",
            row_flags, offset
        );

        // Read extended flags if present
        let extended_flags = if (row_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading extended flags",
                ));
            }
            let ext = data[pos];
            pos += 1;
            Some(ext)
        } else {
            None
        };

        let bytes_consumed = pos - offset;
        Ok((row_flags, extended_flags, bytes_consumed))
    }

    /// Check if row flags indicate end of partition (Issue #229 fix)
    ///
    /// Per Cassandra's UnfilteredSerializer.java, END_OF_PARTITION is written as exactly 0x01
    /// by the `writeEndOfPartition()` method. The Cassandra source uses a bitmask check
    /// `(flags & END_OF_PARTITION) != 0`, but in practice the marker is always 0x01.
    ///
    /// We use an EXACT match to 0x01 to avoid false positives with row data that
    /// incidentally has bit 0 set (e.g., 0xb7 which would wrongly match a bitmask check).
    ///
    /// When END_OF_PARTITION (0x01) is detected, nothing follows the flags byte.
    /// The partition is complete and parsing should move to the next partition.
    #[inline]
    pub(super) fn is_end_of_partition(flags: u8) -> bool {
        flags == END_OF_PARTITION // Exact match, not bitmask
    }

    /// Check if row flags indicate a range tombstone marker (not a data row)
    ///
    /// Per Cassandra's UnfilteredSerializer.java, IS_MARKER (0x02) indicates a range
    /// tombstone boundary. The marker flag can be combined with other metadata flags
    /// (e.g., 0x52 = IS_MARKER | deletion metadata, 0x7a, 0x36, etc.).
    ///
    /// Issue #258 fix: Use bitwise AND to detect markers with additional flags.
    /// Previously used exact match (flags == 0x02) which missed markers like 0x52.
    #[inline]
    pub(super) fn is_range_tombstone_marker(flags: u8) -> bool {
        // Check if IS_MARKER bit is set, but END_OF_PARTITION bit is NOT set
        // IS_MARKER = 0x02, END_OF_PARTITION = 0x01
        // If END_OF_PARTITION bit is set (even with other bits), it's end of partition, not a marker
        (flags & IS_MARKER) != 0 && (flags & END_OF_PARTITION) == 0
    }

    /// Skip a range tombstone marker body (Issue #229 fix, VG6 fix)
    ///
    /// Range tombstone markers for SSTable format have this on-disk layout:
    ///   [flags: u8]                        ← IS_MARKER (0x02) bit set
    ///   [extended_flags: u8]               ← only if ROW_HAS_EXTENDED_FLAGS set
    ///   [bound_kind: u8]                   ← ordinal of ClusteringBoundOrBoundary.Kind
    ///   [cluster_count: u16 big-endian]    ← number of clustering values (bound.size())
    ///   [cluster_header: VUInt]            ← 2 bits per value (0=present, 1=empty, 2=null)
    ///   [cluster_values: ...]              ← type-specific bytes for non-null/non-empty values
    ///   [marker_body_size: VUInt]          ← size of the body that follows (including prev_size)
    ///   [prev_unfiltered_size: VUInt]      ← size of the previous unfiltered item
    ///   [marked_for_delete_at: VUInt]      ← timestamp delta from min_timestamp (µs)
    ///   [local_deletion_time: VUInt32]     ← seconds delta from min_local_deletion_time
    ///   [marked_for_delete_at2: VUInt]     ← ONLY for boundaries (kind 2 or 5)
    ///   [local_deletion_time2: VUInt32]    ← ONLY for boundaries (kind 2 or 5)
    ///
    /// Authority:
    ///   UnfilteredSerializer.java:282-303  (serialize(RangeTombstoneMarker, ...))
    ///   ClusteringBoundOrBoundary.Serializer.serialize (lines 103-107):
    ///     out.writeByte(bound.kind().ordinal())   ← kind byte
    ///     out.writeShort(bound.size())            ← u16 cluster count
    ///     ClusteringPrefix.serializer.serializeValuesWithoutSize(...)
    ///   SerializationHeader.writeDeletionTime (lines 180-183):
    ///     writeTimestamp → writeUnsignedVInt      ← VUInt, NOT ZigZag
    ///     writeLocalDeletionTime → writeUnsignedVInt32 ← VUInt, NOT ZigZag
    ///
    /// VG6 fix: The previous implementation had three bugs:
    ///   1. After kind byte: did not read the u16 cluster_count before the VUInt header.
    ///      The 2-byte short was being consumed as part of the clustering values, causing
    ///      all deletion-time bytes to be misaligned.
    ///   2. After clustering values: did not skip marker_body_size + prev_unfiltered_size
    ///      VUInts that precede the deletion times in SSTable format.
    ///   3. Used parse_vint (ZigZag) instead of parse_vuint (unsigned) for deletion times.
    ///
    /// Implementation strategy: use marker_body_size to skip the entire body
    /// (prev_size + deletion times) without manually decoding individual fields.
    pub(super) fn skip_range_tombstone_marker(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<usize> {
        let mut pos = offset;

        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at range tombstone marker",
            ));
        }

        let marker_flags = data[pos];
        pos += 1; // Skip flags byte

        tracing::debug!(
            "V5CompressedLegacy: Skipping range tombstone marker with flags=0x{:02x} at offset {}",
            marker_flags,
            offset
        );

        // Extended flags if present (unlikely for markers, but handle it)
        if (marker_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading marker extended flags",
                ));
            }
            pos += 1;
        }

        // Read bound kind byte.
        // Authority: ClusteringBoundOrBoundary.Serializer.serialize (line 104):
        //   out.writeByte(bound.kind().ordinal())
        //
        // Kind ordinals (ClusteringPrefix.java:67-81):
        //   0 = EXCL_END_BOUND (simple, 1 deletion time)
        //   1 = INCL_START_BOUND (simple, 1 deletion time)
        //   2 = EXCL_END_INCL_START_BOUNDARY (boundary, 2 deletion times)
        //   5 = INCL_END_EXCL_START_BOUNDARY (boundary, 2 deletion times)
        //   6 = INCL_END_BOUND (simple, 1 deletion time)
        //   7 = EXCL_START_BOUND (simple, 1 deletion time)
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone bound kind",
            ));
        }
        let bound_kind = data[pos];
        pos += 1;

        tracing::debug!(
            "V5CompressedLegacy: Range tombstone bound_kind={}",
            bound_kind,
        );

        // Read cluster count (u16 big-endian).
        // Authority: ClusteringBoundOrBoundary.Serializer.serialize (line 105):
        //   out.writeShort(bound.size())
        //
        // This is the number of clustering values in the bound. It is NOT the same as
        // schema.clustering_keys.len() — for regular rows, no count is written; for markers,
        // a u16 is always present. Failing to read this u16 causes all subsequent bytes to
        // be misaligned (the two count bytes get consumed as the VUInt header + first value
        // byte, producing garbage alignment).
        if pos + 2 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone cluster count (u16)",
            ));
        }
        let cluster_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        tracing::debug!(
            "V5CompressedLegacy: Range tombstone cluster_count={}",
            cluster_count,
        );

        // Read the clustering VUInt header + clustering values.
        // Authority: ClusteringPrefix.Serializer.serializeValuesWithoutSize (lines 455-477):
        //   Writes VUInt header (2 bits per value, 0=present/1=empty/2=null), then value bytes.
        //
        // Use a truncated schema when cluster_count < schema.clustering_keys.len() to avoid
        // reading past the bound's bytes into the marker body (prefix bound case).
        if cluster_count > 0 {
            let prefix_schema_owned = Self::clustering_prefix_schema(schema, cluster_count);
            let effective_schema = prefix_schema_owned.as_ref().unwrap_or(schema);
            let (_, new_pos) = self.parse_clustering_prefix(data, pos, effective_schema)?;
            pos = new_pos;
        }

        // Read marker_body_size and skip the body.
        // Authority: UnfilteredSerializer.java:291 (for SSTable format):
        //   out.writeUnsignedVInt(serializedMarkerBodySize(marker, header, previousUnfilteredSize, version))
        //   out.writeUnsignedVInt(previousUnfilteredSize)
        //   ... deletion time(s) ...
        //
        // serializedMarkerBodySize() returns the size of (prev_size + deletion_times).
        // So after reading marker_body_size, we can skip exactly that many bytes to reach
        // the next unfiltered item, without needing to decode individual deletion time VUInts.
        //
        // This is exactly the same pattern as regular row_size: after reading row_size,
        // skip row_size bytes to reach the next row/marker.
        let (remaining, marker_body_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse marker_body_size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let body_size_vint_len = data[pos..].len() - remaining.len();
        pos += body_size_vint_len;

        // Skip marker_body_size bytes (prev_size + deletion time(s))
        // #3848: framing size, not a cell value — bound the raw `u64` first.
        let body_len = vuint_length_within(marker_body_size, data.len().saturating_sub(pos))
            .ok_or_else(|| {
                Error::corruption(format!(
                    "V5CompressedLegacy: marker_body_size={} at pos={} exceeds data length {}",
                    marker_body_size,
                    pos,
                    data.len()
                ))
            })?;
        pos += body_len;

        tracing::debug!(
            "V5CompressedLegacy: Skipped range tombstone marker, advanced from {} to {}",
            offset,
            pos
        );

        Ok(pos)
    }

    /// Parse row metadata AFTER flags and clustering prefix (Issue #213 fix)
    ///
    /// # Corrected Format (from Cassandra UnfilteredSerializer.java)
    /// ```text
    /// [row_flags: u8]           ← Parsed by parse_row_flags()
    /// [extended_flags: u8]      ← Parsed by parse_row_flags()
    /// [clustering_prefix]       ← Parsed by parse_clustering_prefix()
    /// [row_size: VInt]          ← This function starts here
    /// [prev_size: VInt]
    /// [timestamp: VInt if 0x04 set] ← Delta from min_timestamp
    /// [ttl: VInt if 0x08 set] ← Delta from min_ttl
    /// [deletion: 2 VInts if 0x10 set]
    /// [column_bitmap: VUInt bitmask of missing columns if NOT 0x20]
    /// ```
    ///
    /// Returns RowHeader with decoded metadata, calculated header_size, and row_size.
    pub(super) fn parse_row_metadata(
        &self,
        data: &[u8],
        offset: usize,
        row_flags: u8,
        _extended_flags: Option<u8>,
    ) -> Result<(RowHeader, u64)> {
        let mut pos = offset;

        // V5CompressedLegacy format: row_size and prev_size come AFTER clustering
        // (which has already been parsed before this function is called)

        // Read row size (VInt) - CRITICAL for partition boundary detection!
        debug!(
            "V5CompressedLegacy: Parsing row_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, row_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse row size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let row_size_vint_len = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: row_size={}, consumed {} bytes, pos before={}, pos after={}",
            row_size,
            row_size_vint_len,
            pos,
            pos + row_size_vint_len
        );
        pos += row_size_vint_len;

        // Read prev size (VInt)
        debug!(
            "V5CompressedLegacy: Parsing prev_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let bytes_consumed = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: prev_size={}, consumed {} bytes, pos before={}, pos after={}",
            _prev_size,
            bytes_consumed,
            pos,
            pos + bytes_consumed
        );
        pos += bytes_consumed;

        // Read timestamp if HAS_TIMESTAMP flag is set.
        //
        // Fix #629 (C2): Cassandra writes an UNSIGNED VInt delta here
        // (SerializationHeader.java:165: out.writeUnsignedVInt(timestamp - stats.minTimestamp)).
        // The old code used parse_vint (ZigZag), causing ~50% undercount of timestamp deltas.
        let timestamp = if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
            let (remaining, delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse timestamp delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Apply delta decoding: absolute_timestamp = min_timestamp + delta
            let absolute_timestamp = self.min_timestamp.wrapping_add(delta as i64);
            debug!(
                "V5CompressedLegacy: Row timestamp: delta={}, min={}, absolute={}",
                delta, self.min_timestamp, absolute_timestamp
            );
            Some(absolute_timestamp)
        } else {
            None
        };

        // Read TTL and liveness local expiration time if HAS_TTL flag is set.
        //
        // Fix #630 (C3): Cassandra writes TWO VInt32 fields when HAS_TTL is set
        // (UnfilteredSerializer.java:225-228):
        //   1. pk_liveness.ttl()               → header.writeTTL(ttl, out)        [VInt32]
        //   2. pk_liveness.localExpirationTime()→ header.writeLocalDeletionTime(ldt, out) [VInt32]
        // The old code read only ONE VInt (TTL), leaving the LDT byte(s) unread and
        // misaligning all subsequent fields in HAS_TTL rows.
        let (ttl, ttl_liveness_ldt) = if (row_flags & ROW_HAS_TTL) != 0 {
            let (remaining, ttl_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse TTL delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Apply delta decoding: absolute_ttl = min_ttl + delta
            let absolute_ttl = if let Some(min_ttl) = self.min_ttl {
                min_ttl.wrapping_add(ttl_delta as i64) as i32
            } else {
                ttl_delta as i32
            };

            // Read liveness local expiration time (second mandatory field after TTL).
            let (remaining, ldt_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse liveness LDT delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let ldt_bytes_consumed = data[pos..].len() - remaining.len();
            pos += ldt_bytes_consumed;

            // Liveness localExpirationTime (SECONDS). Same VG3 unsigned-deletion-time
            // reinterpretation as the row/complex-cell deletion LDT reader below
            // (BigFormat.java:409, hasUIntDeletionTime): on oa/da a post-2038 expiry
            // occupies [2^31, 2^32) and must be read as UNSIGNED (year-2106-safe) so
            // it stays a large POSITIVE second count. Storing it as `i64` here (vs the
            // old `as i32`) prevents the wrap-to-negative that would make the read-time
            // TTL filter treat a still-live row as long-expired and hide it (#1741 F1).
            // nb (signed) values are capped at ~2038 so sign-extension is a no-op.
            let raw_liveness_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64);
            let absolute_ldt: i64 = if self.has_uint_deletion_time() {
                (raw_liveness_ldt as u32) as i64
            } else {
                raw_liveness_ldt as i32 as i64
            };

            debug!(
                "V5CompressedLegacy: Row TTL: ttl_delta={}, min={:?}, ttl={}, ldt_delta={}, ldt={}",
                ttl_delta, self.min_ttl, absolute_ttl, ldt_delta, absolute_ldt
            );
            (Some(absolute_ttl), Some(absolute_ldt))
        } else {
            (None, None)
        };

        // Read deletion if HAS_DELETION flag is set.
        //
        // Cassandra canonical DeletionTime.Serializer order (matches the CQLite writer,
        // data_writer.rs write_*_row HAS_DELETION block and write_complex_deletion):
        //   1. markedForDeleteAt: UNSIGNED VInt delta, base min_timestamp, MICROSECONDS
        //      -> the authoritative reconciliation timestamp (LWW shadowing).
        //   2. localDeletionTime: UNSIGNED VInt delta, base min_local_deletion_time, SECONDS
        //      -> the GC-grace clock, NOT a reconciliation timestamp.
        //
        // Fix #629 (C2): Both deltas are UNSIGNED per Cassandra SerializationHeader.java.
        // The old code used parse_vint (ZigZag) for markedForDeleteAt, causing ~50% undercount.
        //
        // (The complex-cell deletion reader, parse_complex_column, already uses this
        // markedForDeleteAt-first order; this aligns the row-level header with it.)
        let (marked_for_delete_at, local_deletion_time) = if (row_flags & ROW_HAS_DELETION) != 0 {
            // First VInt: markedForDeleteAt delta (unsigned).
            let (remaining, mfda_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse markedForDeleteAt delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Second VInt: localDeletionTime delta (unsigned VInt32).
            //
            // For both nb and oa the on-disk format is the same: an unsigned VInt32
            // encoding `(int)(localDeletionTime - stats.minLocalDeletionTime)`.
            // See: SerializationHeader.java — `writeLocalDeletionTime` /
            //      `readLocalDeletionTime` (same for all BIG versions).
            //
            // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa+)
            // The interpretation of the *result* differs:
            //   nb: `min_local_deletion_time + delta` cast to i32 (values capped at ~year 2038)
            //   oa: `min_local_deletion_time + delta` treated as u32 to support ~year 2106
            //
            // When the sum overflows an i32 (> 2^31-1 seconds) the value is negative
            // in a signed context; with hasUIntDeletionTime we reinterpret it as an
            // unsigned u32 (CassandraUInt.toLong, CassandraUInt.java).  For current
            // test fixtures all deletion times are well within i32 range so both
            // interpretations produce identical bit patterns; the gate is a no-op
            // in practice but is wired correctly for future large TTL values.
            let (remaining, ldt_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse localDeletionTime delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // markedForDeleteAt: absolute = min_timestamp + delta (microseconds).
            let absolute_marked_for_delete_at = self.min_timestamp.wrapping_add(mfda_delta as i64);
            // localDeletionTime: absolute = min_local_deletion_time + delta (seconds).
            //
            // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa/da)
            //   nb: store as i32 (may overflow for dates > ~year 2038)
            //   oa: reinterpret the 32-bit bit-pattern as unsigned (supports ~year 2106)
            //
            // Source: SerializationHeader.java readLocalDeletionTime + UnfilteredSerializer.java:671-676
            // "if (complexDeletion.localDeletionTime() < 0) {
            //    complexDeletion = DeletionTime.build(..., Cell.deletionTimeUnsignedIntegerToLong((int) ...));
            //  }" — this reinterpretation fires when hasUIntDeletionTime && bit31 set.
            let has_uint_ldt = match self.version_gates.as_ref() {
                crate::storage::sstable::version_gate::VersionGates::Big(g) => {
                    g.has_uint_deletion_time
                }
                crate::storage::sstable::version_gate::VersionGates::Bti(g) => {
                    g.has_uint_deletion_time
                }
            };
            let raw_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64);
            let absolute_local_deletion_time = if has_uint_ldt {
                // Reinterpret the low 32 bits as an unsigned integer (year-2106-safe).
                // CassandraUInt.toLong(int) = Integer.toUnsignedLong(int), so negative
                // i32 values get promoted to the [2^31, 2^32) long range.
                (raw_ldt as u32) as i32
            } else {
                raw_ldt as i32
            };
            debug!(
                "V5CompressedLegacy: Row deletion: markedForDeleteAt(delta={}, min_ts={}, abs={} us), localDeletionTime(delta={}, min_ldt={}, abs={} s)",
                mfda_delta,
                self.min_timestamp,
                absolute_marked_for_delete_at,
                ldt_delta,
                self.min_local_deletion_time,
                absolute_local_deletion_time
            );
            (
                Some(absolute_marked_for_delete_at),
                Some(absolute_local_deletion_time),
            )
        } else {
            (None, None)
        };

        // Parse column bitmap if HAS_ALL_COLUMNS is NOT set
        let missing_columns_bitmap = if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
            // Cassandra Columns.Serializer.serializeSubset() format:
            // Single unsigned VInt encoding a bitmask of MISSING columns
            // (bit=1 means column is missing, bit=0 means present)
            let (remaining, bitmap) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse column bitmap at offset {}: {:?}",
                    offset + pos,
                    e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            debug!(
                "V5CompressedLegacy: Parsed column bitmap: missing_bitmap=0x{:X} ({} bytes)",
                bitmap, bytes_consumed
            );
            Some(bitmap)
        } else {
            None
        };

        let header_size = pos - offset;
        debug!(
            "V5CompressedLegacy: Row header parsing complete: offset_start={}, pos_end={}, header_size={} bytes, row_size={} bytes (total row including cells), timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, pos, header_size, row_size, timestamp, ttl, local_deletion_time
        );

        // Note: ttl_liveness_ldt (from HAS_TTL) is the pk_liveness local expiration time.
        // It is distinct from local_deletion_time (from HAS_DELETION, row tombstone GC clock).
        // We store it in RowHeader so the delta-scan path can populate CellMeta.expires_at
        // for TTL-bearing INSERT rows (Issue #702).  is_row_tombstone() still checks
        // local_deletion_time (HAS_DELETION only), so TTL rows are NOT misclassified.

        Ok((
            RowHeader {
                timestamp,
                ttl,
                liveness_expires_at_seconds: ttl_liveness_ldt,
                local_deletion_time,
                marked_for_delete_at,
                header_size,
                row_size_vint_len,
                missing_columns_bitmap,
                // Issues #1741/#3094: populated after the cell loop in
                // `parse_row_data_with_offset_impl` (no data cell decoded yet).
                max_data_cell_timestamp: None,
                max_data_cell_expires_at: None,
                has_live_forever_data_cell: false,
                has_deleted_data_cell: false,
            },
            row_size,
        ))
    }

    /// Parse partition header (flags, key, deletion time)
    ///
    /// # Format
    /// ```text
    /// [flags: u8][key_len: u8][key_bytes: [u8; key_len]][del_time: i32][unknown: 8 bytes]
    /// ```
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition header parsing
    #[doc(hidden)]
    pub fn parse_partition_header(&self, data: &[u8], offset: usize) -> Result<(RowKey, usize)> {
        let (row_key, next_offset, _deletion) = self.parse_partition_header_full(data, offset)?;
        Ok((row_key, next_offset))
    }

    /// Like [`parse_partition_header`] but also returns the partition-level deletion
    /// `(markedForDeleteAt µs, localDeletionTime s)`, if the partition is deleted.
    ///
    /// Returns `(RowKey, next_offset, Option<(markedForDeleteAt_micros, localDeletionTime_secs)>)`.
    ///
    /// `None` means the partition is live (no partition tombstone).
    /// `Some((mfda, ldt))` means the partition carries a tombstone; `mfda` is the
    /// authoritative reconciliation timestamp in microseconds since the Unix epoch and
    /// `ldt` is the GC-grace clock in seconds (carried as the wrapping `as u32 as i32`
    /// for far-future local-deletion-times, matching the row/range tombstone contract).
    ///
    /// Authority: DeletionTime.java (getSerializer / legacySerializer / Serializer),
    /// BigFormat.java:409 (`hasUIntDeletionTime`).
    #[allow(clippy::type_complexity)]
    pub fn parse_partition_header_full(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(RowKey, usize, Option<(i64, i32)>)> {
        // Issue #1618 (H5): count every speculative partition-header parse — the
        // real allocating parse, which runs once per partition at a confirmed
        // start. The per-row BOUNDARY peek (issue #1641, K2) does NOT come through
        // here: it uses the non-allocating `peek_partition_boundary`, which shares
        // the structural walk below via `scan_partition_header` but records no
        // gauge and copies no key.
        crate::storage::sstable::read_work_counters::record_partition_header_try_parse();

        let layout = self.scan_partition_header(data, offset)?;
        // The single legitimate key allocation: once per real header parse.
        let key_bytes = data[layout.key_range].to_vec();
        Ok((
            RowKey::new(key_bytes),
            layout.next_offset,
            layout.partition_deletion,
        ))
    }

    /// Structural walk of a partition header WITHOUT allocating the key or
    /// recording the `PARTITION_HEADER_TRY_PARSES` gauge (issue #1641, K2).
    ///
    /// This is the single structural authority: [`parse_partition_header_full`]
    /// wraps it (gauge + key `to_vec`), and [`peek_partition_boundary`] uses it as
    /// the boundary detector. Every validation and every `Err` message is
    /// identical to the former inline body of `parse_partition_header_full`, so a
    /// peek can never accept a header the full parser rejects (no drift).
    ///
    /// [`parse_partition_header_full`]: Self::parse_partition_header_full
    /// [`peek_partition_boundary`]: Self::peek_partition_boundary
    fn scan_partition_header(
        &self,
        data: &[u8],
        mut offset: usize,
    ) -> Result<PartitionHeaderLayout> {
        let start_offset = offset;

        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition header offset {} out of bounds (data len: {})",
                offset,
                data.len()
            )));
        }

        // Byte 0: Flags (ignore for now - may indicate static rows, deletions, etc.)
        let _flags = data[offset];
        offset += 1;

        // Byte 1: Partition key length (u8, NOT VInt)
        if offset >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at partition key length",
            ));
        }
        let key_len = data[offset] as usize;
        offset += 1;

        // Issue #258 FIX: Partition key length must be non-zero
        // A key_len of 0 indicates this is NOT a valid partition header (likely row data).
        // This validation is critical for peek_is_partition_header() to correctly
        // distinguish partition headers from row data in the row loop.
        if key_len == 0 {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Invalid partition key length 0 at offset {} (not a partition header)",
                start_offset
            )));
        }

        debug!(
            "V5CompressedLegacy: Partition key length = {} bytes",
            key_len
        );

        // Next key_len bytes: Partition key data (raw bytes, no component structure)
        if offset + key_len > data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition key extends beyond data (offset: {}, key_len: {}, data_len: {})",
                offset, key_len, data.len()
            )));
        }
        // Record the key's byte RANGE — no copy here. The allocation happens
        // once, in `parse_partition_header_full`, at a confirmed header start
        // (issue #1641, K2); the boundary peek needs no key at all.
        let key_range = offset..offset + key_len;
        offset += key_len;

        // Partition-level DeletionTime deserialization.
        //
        // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa+)
        //
        // oa format uses a compact DeletionTime.Serializer
        // (DeletionTime.java, Serializer inner class):
        //   LIVE:    1 byte = 0x80 (IS_LIVE_DELETION = 0b10000000)
        //   DELETED: 8 bytes markedForDeleteAt (long) +
        //            4 bytes localDeletionTimeUnsignedInteger (int) = 12 bytes total
        //
        // nb format uses DeletionTime.legacySerializer:
        //   Always:  4 bytes localDeletionTime (int) +
        //            8 bytes markedForDeleteAt (long) = 12 bytes total
        //
        // Authority: DeletionTime.java:191-219 (getSerializer / Serializer.serialize)
        let partition_deletion: Option<(i64, i32)>;
        if self.has_uint_deletion_time() {
            // oa / da format
            if offset >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end at oa partition deletion time byte",
                ));
            }
            let del_flags = data[offset];
            if (del_flags & OA_IS_LIVE_DELETION) != 0 {
                // LIVE partition: exactly 1 byte — no tombstone.
                if del_flags != OA_IS_LIVE_DELETION {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Invalid IS_LIVE_DELETION byte 0x{:02x} at offset {} \
                         (only 0x80 is valid for oa-format LIVE partitions, per DeletionTime.java:227-229)",
                        del_flags, offset
                    )));
                }
                offset += 1;
                partition_deletion = None;
            } else {
                // DELETED partition (oa): 8 bytes markedForDeleteAt (big-endian i64)
                //                       + 4 bytes localDeletionTime (big-endian u32)
                if offset + 12 > data.len() {
                    return Err(Error::corruption(
                        "V5CompressedLegacy: Unexpected end at oa partition deletion time (deleted)",
                    ));
                }
                let mfda = i64::from_be_bytes(
                    data[offset..offset + 8]
                        .try_into()
                        .map_err(|_| Error::corruption("V5CompressedLegacy: oa mfda slice"))?,
                );
                // localDeletionTime is the next 4 bytes (big-endian u32). Keep the
                // wrapping `as u32 as i32` representation so far-future LDTs in
                // [2^31, 2^32) round-trip exactly like row/range tombstones.
                let ldt = u32::from_be_bytes(
                    data[offset + 8..offset + 12]
                        .try_into()
                        .map_err(|_| Error::corruption("V5CompressedLegacy: oa ldt slice"))?,
                ) as i32;
                offset += 12; // markedForDeleteAt(8) + localDeletionTime(4)
                partition_deletion = Some((mfda, ldt));
            }
        } else {
            // nb format: 4 bytes localDeletionTime (big-endian i32)
            //          + 8 bytes markedForDeleteAt (big-endian i64)
            if offset + 12 > data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end at nb partition deletion time",
                ));
            }
            // localDeletionTime is the first 4 bytes.
            let local_deletion_time = i32::from_be_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| Error::corruption("V5CompressedLegacy: nb ldt slice"))?,
            );
            offset += 4;
            // markedForDeleteAt is next 8 bytes (big-endian i64).
            let mfda = i64::from_be_bytes(
                data[offset..offset + 8]
                    .try_into()
                    .map_err(|_| Error::corruption("V5CompressedLegacy: nb mfda slice"))?,
            );
            offset += 8;
            // A live (not deleted) partition in nb format has
            // localDeletionTime = 0x7fffffff (i32::MAX, DeletionTime.LIVE sentinel).
            // Any other value indicates a real partition tombstone.
            const NB_LIVE_LOCAL_DELETION_TIME: i32 = i32::MAX;
            if local_deletion_time == NB_LIVE_LOCAL_DELETION_TIME {
                partition_deletion = None;
            } else {
                partition_deletion = Some((mfda, local_deletion_time));
            }
        }

        debug!(
            "V5CompressedLegacy: Parsed partition header at offset {}, consumed {} bytes, \
             partition_deletion={:?}",
            start_offset,
            offset - start_offset,
            partition_deletion
        );

        Ok(PartitionHeaderLayout {
            key_range,
            next_offset: offset,
            partition_deletion,
        })
    }

    /// Non-allocating partition-BOUNDARY peek (issue #1641, K2).
    ///
    /// Answers "do the bytes at `offset` begin a new partition header?" for the
    /// post-row emit loop while skipping the success-path key `to_vec` and the
    /// `PARTITION_HEADER_TRY_PARSES` counter that the full parse incurs. The
    /// fast-reject paths (marker pre-check + readiness gate) allocate nothing at
    /// all; only the strict scan on a `Ready` buffer may still build a discarded
    /// `format!` error string on a structural mismatch (inside
    /// `scan_partition_header`). It reaches the same verdict the old allocating
    /// `peek_is_partition_header` did — proved by the `#[cfg(test)]` proptest
    /// (`Header` ⟺ `!marker && parse.is_ok()`).
    ///
    /// Algorithm (fast-reject paths allocate nothing; no gauge on any path):
    /// 1. Marker pre-check (issue #229): an END_OF_PARTITION (`0x01`) or
    ///    range-tombstone (IS_MARKER) leading byte is never a partition header;
    ///    an offset past the buffer end is `NeedMoreBytes`.
    /// 2. Completeness gate via the shared #1741 [`partition_header_readiness`]
    ///    classifier: `Incomplete` → `NeedMoreBytes`, `Malformed` → `NotHeader`.
    /// 3. Under `Ready` every header byte is present, so a [`scan_partition_header`]
    ///    failure is a genuine STRUCTURAL rejection (e.g. an illegal oa/da IS_LIVE
    ///    byte), never truncation: `Ok` → `Header`, `Err` → `NotHeader`.
    ///
    /// [`partition_header_readiness`]: Self::partition_header_readiness
    /// [`scan_partition_header`]: Self::scan_partition_header
    pub(super) fn peek_partition_boundary(&self, data: &[u8], offset: usize) -> BoundaryPeek {
        // Step 1: marker / end-of-buffer pre-check.
        match data.get(offset) {
            None => return BoundaryPeek::NeedMoreBytes,
            Some(&flags) => {
                if Self::is_end_of_partition(flags) || Self::is_range_tombstone_marker(flags) {
                    return BoundaryPeek::NotHeader;
                }
            }
        }

        // Step 2 + 3: completeness gate, then the strict shared structural scan.
        // `offset < data.len()` holds (step 1 returned on `None`), so the subslice
        // is valid and zero-cost.
        match self.partition_header_readiness(&data[offset..]) {
            PartitionHeaderReadiness::Incomplete => BoundaryPeek::NeedMoreBytes,
            PartitionHeaderReadiness::Malformed => BoundaryPeek::NotHeader,
            PartitionHeaderReadiness::Ready => match self.scan_partition_header(data, offset) {
                Ok(_) => BoundaryPeek::Header,
                Err(_) => BoundaryPeek::NotHeader,
            },
        }
    }

    /// Parse a range tombstone marker in full, returning the decoded bound values,
    /// inclusivity flags, and deletion timestamp(s).
    ///
    /// This is the delta-scan counterpart to `skip_range_tombstone_marker`: instead
    /// of discarding the clustering values and deletion time, it decodes and returns
    /// them so the caller can emit `DeltaRecord::RangeDelete`.
    ///
    /// ## Return value
    ///
    /// `Ok((bound_values, bound_kind, deleted_at_primary, deleted_at_secondary, next_offset))`
    ///
    /// - `bound_values`: clustering-key prefix values for this bound (may be shorter
    ///   than the full clustering arity — a prefix bound).
    /// - `bound_kind`: the raw Cassandra `ClusteringPrefix.Kind` ordinal (0/1/2/5/6/7).
    /// - `deleted_at_primary`: `markedForDeleteAt` in µs for this bound's tombstone.
    /// - `deleted_at_secondary`: present only for boundary markers (kind 2 or 5) and
    ///   carries the deletion time for the *other* side of the boundary.
    /// - `next_offset`: position after this marker in `data`.
    ///
    /// ## Bound kind ordinals
    ///
    /// | ordinal | name | meaning |
    /// |---------|------|---------|
    /// | 0 | `EXCL_END_BOUND` | end of range, exclusive (`< ck`) |
    /// | 1 | `INCL_START_BOUND` | start of range, inclusive (`>= ck`) |
    /// | 2 | `EXCL_END_INCL_START_BOUNDARY` | boundary: end of prev range (exclusive) + start of new range (inclusive) |
    /// | 5 | `INCL_END_EXCL_START_BOUNDARY` | boundary: end of prev range (inclusive) + start of new range (exclusive) |
    /// | 6 | `INCL_END_BOUND` | end of range, inclusive (`<= ck`) |
    /// | 7 | `EXCL_START_BOUND` | start of range, exclusive (`> ck`) |
    ///
    /// Boundary markers (kind 2 or 5) carry **two** deletion times; simple markers (all others)
    /// carry **one** deletion time and `deleted_at_secondary` is `None`.
    ///
    /// Authority: UnfilteredSerializer.java:282-303, ClusteringBoundOrBoundary.java
    #[allow(clippy::type_complexity)]
    pub fn parse_range_tombstone_marker_full(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, u8, i64, Option<i64>, usize)> {
        // Delegate to the LDT-carrying parser and drop the localDeletionTime
        // fields the delta-scan caller does not need — the on-disk marker grammar
        // is decoded in exactly one place (issue #933 cleanup).
        let (bound_values, bound_kind, (mfda_primary, _ldt_primary), secondary, next_offset) =
            self.parse_range_tombstone_marker_with_ldt(data, offset, schema)?;
        Ok((
            bound_values,
            bound_kind,
            mfda_primary,
            secondary.map(|(mfda, _ldt)| mfda),
            next_offset,
        ))
    }

    /// Return a schema view truncated to `n` clustering keys.
    ///
    /// Range-tombstone bound markers may be **prefix bounds**: the Cassandra serializer
    /// writes only `cluster_count` clustering values when a DELETE specifies fewer
    /// clustering components than the full key arity (e.g. `DELETE WHERE pk=? AND ck1=?`
    /// on a table with `(ck1, ck2)` only pins the first component).
    ///
    /// Returns `None` (callers use the original schema) when `n >= schema.clustering_keys.len()`,
    /// avoiding a clone in the common non-prefix case.  Returns `Some(truncated)` when a
    /// shorter view is needed.
    fn clustering_prefix_schema(schema: &TableSchema, n: usize) -> Option<TableSchema> {
        if n >= schema.clustering_keys.len() {
            None
        } else {
            Some(TableSchema {
                keyspace: schema.keyspace.clone(),
                table: schema.table.clone(),
                partition_keys: schema.partition_keys.clone(),
                clustering_keys: schema.clustering_keys[..n].to_vec(),
                columns: schema.columns.clone(),
                comments: schema.comments.clone(),
                dropped_columns: schema.dropped_columns.clone(),
            })
        }
    }

    /// Decode one `(markedForDeleteAt, localDeletionTime)` pair, returning BOTH
    /// absolute values (issue #933 compaction range-tombstone surfacing).
    ///
    /// Decodes the `(markedForDeleteAt delta, localDeletionTime delta)` VInt pair
    /// and returns BOTH absolute values — the absolute
    /// `localDeletionTime` (GC-grace clock, seconds) so the compaction merger can
    /// retain/purge a surviving range marker by gc_grace AND the writer can
    /// re-emit the marker's LDT verbatim. The LDT is decoded with the same
    /// `hasUIntDeletionTime` reinterpretation as the row-level deletion reader
    /// (`min_local_deletion_time + delta`, then `as u32 as i32` for far-future
    /// values) so a year-2106 LDT round-trips bit-for-bit (#853/#889).
    ///
    /// Advances `*pos` past both fields.
    fn parse_deletion_time_pair_with_ldt(
        &self,
        data: &[u8],
        pos: &mut usize,
    ) -> Result<(i64, i32)> {
        // markedForDeleteAt delta (unsigned VInt, µs since epoch delta).
        let (remaining, mfda_delta) = parse_vuint(&data[*pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse markedForDeleteAt in marker body at {}: {:?}",
                *pos, e
            ))
        })?;
        *pos += data[*pos..].len() - remaining.len();
        let absolute_mfda = self.min_timestamp.wrapping_add(mfda_delta as i64);

        // localDeletionTime delta (unsigned VInt, seconds delta).
        let (remaining2, ldt_delta) = parse_vuint(&data[*pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse localDeletionTime in marker body at {}: {:?}",
                *pos, e
            ))
        })?;
        *pos += data[*pos..].len() - remaining2.len();

        let has_uint_ldt = match self.version_gates.as_ref() {
            crate::storage::sstable::version_gate::VersionGates::Big(g) => g.has_uint_deletion_time,
            crate::storage::sstable::version_gate::VersionGates::Bti(g) => g.has_uint_deletion_time,
        };
        let raw_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64);
        let absolute_ldt = if has_uint_ldt {
            (raw_ldt as u32) as i32
        } else {
            raw_ldt as i32
        };

        Ok((absolute_mfda, absolute_ldt))
    }

    /// Parse a range-tombstone bound marker in full, returning the decoded bound
    /// values, kind, AND the primary/secondary `(markedForDeleteAt, localDeletionTime)`
    /// pairs (issue #933).
    ///
    /// This is the compaction counterpart of [`Self::parse_range_tombstone_marker_full`]:
    /// it additionally surfaces the `localDeletionTime` of each deletion so the
    /// compaction path can re-emit / gc_grace-purge the marker faithfully.
    ///
    /// Returns `(bound_values, bound_kind, (mfda_primary, ldt_primary),
    /// Option<(mfda_secondary, ldt_secondary)>, next_offset)`.
    #[allow(clippy::type_complexity)]
    pub(super) fn parse_range_tombstone_marker_with_ldt(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, u8, (i64, i32), Option<(i64, i32)>, usize)> {
        let mut pos = offset;

        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at range tombstone marker (compaction)",
            ));
        }

        let marker_flags = data[pos];
        pos += 1;

        if (marker_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading marker extended flags (compaction)",
                ));
            }
            pos += 1;
        }

        // Bound kind byte.
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone bound kind (compaction)",
            ));
        }
        let bound_kind = data[pos];
        pos += 1;

        // Cluster count (u16 big-endian).
        if pos + 2 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone cluster count (compaction)",
            ));
        }
        let cluster_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Clustering values (may be a PREFIX shorter than the full arity).
        let bound_values = if cluster_count > 0 {
            let prefix_schema_owned = Self::clustering_prefix_schema(schema, cluster_count);
            let effective_schema = prefix_schema_owned.as_ref().unwrap_or(schema);
            let (values, new_pos) = self.parse_clustering_prefix(data, pos, effective_schema)?;
            pos = new_pos;
            values
        } else {
            Vec::new()
        };

        // marker_body_size VUInt — size of (prev_size VUInt + deletion_time(s)).
        let (remaining, marker_body_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse marker_body_size (compaction) at offset {}: {:?}",
                pos, e
            ))
        })?;
        pos += data[pos..].len() - remaining.len();

        let body_len = vuint_length_within(marker_body_size, data.len().saturating_sub(pos))
            .ok_or_else(|| {
                Error::corruption(format!(
                    "V5CompressedLegacy: marker_body_size={} at pos={} exceeds data length {} (compaction)",
                    marker_body_size, pos, data.len()
                ))
            })?;
        let body_end = pos + body_len;

        // prev_unfiltered_size VUInt — skip.
        let (remaining2, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev_size in marker body (compaction) at {}: {:?}",
                pos, e
            ))
        })?;
        pos += data[pos..].len() - remaining2.len();

        let primary = self.parse_deletion_time_pair_with_ldt(data, &mut pos)?;
        let secondary = if bound_kind == 2 || bound_kind == 5 {
            Some(self.parse_deletion_time_pair_with_ldt(data, &mut pos)?)
        } else {
            None
        };

        pos = body_end;

        Ok((bound_values, bound_kind, primary, secondary, pos))
    }

    /// Parse clustering prefix section (between row header and cells)
    ///
    /// The clustering prefix encodes clustering key values using a compact VInt header
    /// with 2 bits per clustering column to indicate value state.
    ///
    /// # Format
    /// ```text
    /// [prefix_header: VInt] ← 2 bits per clustering column
    ///   - 00 = null
    ///   - 01 = empty
    ///   - 10/11 = has value
    /// [value_1: bytes if present]
    /// [value_2: bytes if present]
    /// [... more values ...]
    /// ```
    ///
    /// Returns: (clustering_values, new_offset)
    pub(super) fn parse_clustering_prefix(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, usize)> {
        // If no clustering keys, skip this section
        if schema.clustering_keys.is_empty() {
            tracing::debug!(
                "V5CompressedLegacy: No clustering keys in schema, skipping clustering prefix"
            );
            return Ok((Vec::new(), offset));
        }

        tracing::debug!(
            "V5CompressedLegacy: Parsing clustering prefix at offset {} for {} clustering keys",
            offset,
            schema.clustering_keys.len()
        );

        // Read header VInt (2 bits per clustering column)
        let (remaining, header_vint) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse clustering prefix header VInt at offset {}: {:?}",
                offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        tracing::debug!(
            "V5CompressedLegacy: Clustering prefix header = 0x{:x}, consumed {} bytes",
            header_vint,
            bytes_consumed
        );

        // Decode each clustering value based on 2-bit state
        let mut clustering_values = Vec::new();
        for (i, col) in schema.clustering_keys.iter().enumerate() {
            let state = (header_vint >> (i * 2)) & 0x03;
            tracing::debug!(
                "V5CompressedLegacy: Clustering key {} '{}' state = {} (from bits {}..{})",
                i,
                col.name,
                state,
                i * 2,
                i * 2 + 1
            );

            // Issue #229 FIX: Correct state interpretation per Cassandra's ClusteringPrefix.Kind
            //
            // Per Cassandra 5.0 UnfilteredSerializer.java and ClusteringPrefix.Kind:
            // - 0 (PRESENT): Value is present, type-specific bytes follow
            // - 1 (EMPTY): Empty value (zero-length, no bytes follow)
            // - 2 (NULL): NULL value (no bytes follow)
            // - 3: Reserved
            //
            // Previous code had 0=NULL, 2/3=PRESENT which was inverted!
            match state {
                0 => {
                    // PRESENT - parse value based on type
                    let (value, new_off) = self.parse_clustering_value(data, offset, col)?;
                    tracing::debug!(
                        "V5CompressedLegacy:   -> PRESENT: {:?} (consumed {} bytes)",
                        value,
                        new_off - offset
                    );
                    clustering_values.push(value);
                    offset = new_off;
                }
                1 => {
                    // EMPTY - zero-length value
                    //
                    // Per Cassandra's ClusteringPrefix, EMPTY means zero-length byte array.
                    // For variable-width types, this is valid. For fixed-width types (int,
                    // bigint, UUID), EMPTY should not normally occur.
                    let col_type = col.data_type.to_lowercase();
                    let empty_value = match col_type.as_str() {
                        "text" | "varchar" | "ascii" => Value::text(String::new()),
                        "blob" => Value::blob(vec![]),
                        _ => {
                            // Fixed-width types shouldn't have EMPTY state in normal data
                            tracing::warn!(
                                "V5CompressedLegacy: EMPTY state for clustering key '{}' (type {}), treating as NULL",
                                col.name, col.data_type
                            );
                            Value::Null
                        }
                    };
                    clustering_values.push(empty_value);
                    tracing::debug!("V5CompressedLegacy:   -> EMPTY");
                }
                2 => {
                    // NULL
                    clustering_values.push(Value::Null);
                    tracing::debug!("V5CompressedLegacy:   -> NULL");
                }
                3 => {
                    // Reserved - treat as NULL for safety
                    tracing::warn!("V5CompressedLegacy: Clustering key {} has reserved state 3, treating as NULL", col.name);
                    clustering_values.push(Value::Null);
                }
                _ => unreachable!(),
            }
        }

        tracing::debug!(
            "V5CompressedLegacy: Parsed {} clustering values, new offset = {}",
            clustering_values.len(),
            offset
        );

        Ok((clustering_values, offset))
    }

    /// Parse individual clustering value (type-specific)
    ///
    /// Clustering values are encoded based on their CQL type. This handles the most
    /// common clustering key types: timestamp, text, int, uuid.
    ///
    /// Returns: (value, new_offset)
    fn parse_clustering_value(
        &self,
        data: &[u8],
        offset: usize,
        col: &crate::schema::ClusteringColumn,
    ) -> Result<(Value, usize)> {
        let normalized = col.data_type.to_lowercase();
        tracing::debug!(
            "V5CompressedLegacy: Parsing clustering value '{}' type '{}' at offset {}",
            col.name,
            normalized,
            offset
        );

        match normalized.as_str() {
            "timestamp" | "reversedtype(timestamptype)" => {
                // Fixed 8-byte timestamp (big-endian i64)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 8 bytes for timestamp, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }
                let ts = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                Ok((Value::Timestamp(ts), offset + 8))
            }

            "text" | "utf8type" | "varchar" => {
                // VInt length + UTF-8 bytes
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse text length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                let len = checked_vuint_length(
                    len,
                    data.len() - len_offset,
                    "V5CompressedLegacy: Clustering",
                    &col.name,
                    "text",
                )?;

                let text = String::from_utf8(data[len_offset..len_offset + len].to_vec()).map_err(
                    |e| {
                        Error::corruption(format!(
                            "V5CompressedLegacy: Clustering '{}': invalid UTF-8: {:?}",
                            col.name, e
                        ))
                    },
                )?;
                Ok((Value::Text(text.into()), len_offset + len))
            }

            "int" => {
                // Issue #258 fix: Fixed 4-byte int (big-endian i32) - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 4 bytes for int, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                Ok((Value::Integer(val), offset + 4))
            }

            "uuid" | "timeuuid" => {
                // Issue #258 fix: Fixed 16-byte UUID - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 16 bytes for UUID, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                Ok((Value::Uuid(uuid_bytes), offset + 16))
            }

            "bigint" | "counter" => {
                // Issue #258 fix: Fixed 8-byte bigint (big-endian i64) - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 8 bytes for bigint, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                Ok((Value::BigInt(val), offset + 8))
            }

            _ => {
                // For other types, read VInt length + skip that many bytes
                // Return as blob for now
                warn!(
                    "V5CompressedLegacy: Clustering '{}' has unsupported type '{}', treating as blob",
                    col.name, col.data_type
                );
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse blob length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                let len = checked_vuint_length(
                    len,
                    data.len() - len_offset,
                    "V5CompressedLegacy: Clustering",
                    &col.name,
                    "value",
                )?;

                Ok((
                    Value::blob(data[len_offset..len_offset + len].to_vec()),
                    len_offset + len,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_partition_header_parsing() {
        // Hex from test data: 00 10 15291a77... 7fffffff 8000000000000000
        let hex_str = "001015291a77d7394e738397b787442f3a1f7fffffff8000000000000000";
        let data = hex::decode(hex_str).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,    // min_timestamp
            0,    // min_local_deletion_time
            None, // min_ttl
        );
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction
        assert_eq!(row_key.0.len(), 16); // UUID is 16 bytes

        // Verify offset consumed: 1 (flags) + 1 (len) + 16 (uuid) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(offset, 30);

        // Verify UUID bytes match
        let expected_uuid_bytes = hex::decode("15291a77d7394e738397b787442f3a1f").unwrap();
        assert_eq!(row_key.as_bytes(), expected_uuid_bytes.as_slice());
    }

    /// Issue #1006 (manifest: cass.data_db_decode.row_preamble_size_mismatch).
    ///
    /// A truncated / malformed row PREAMBLE must FAIL with an explicit parse
    /// error and must NOT fabricate a partial row. `parse_row_metadata` decodes
    /// the row preamble (row_size VInt, prev_size VInt, then optional
    /// timestamp/ttl/deletion); a multi-byte VInt whose continuation bytes run
    /// off the end of the buffer must surface as a specific `corruption` error.
    ///
    /// There is no Cassandra fixture for a malformed row, so the buffers are
    /// crafted in-test. `parse_row_metadata` is the reader-free row-preamble
    /// decoder, so this exercises the real production parser directly.
    #[test]
    fn row_preamble_truncated_size_fails_loud() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Case 1: row_size VInt claims 2 extra bytes (lead byte 0x80 => one
        // continuation byte expected) but the buffer ends immediately after the
        // lead byte. parse_vuint must NOT fabricate a value; the preamble parse
        // must return a specific "Failed to parse row size" corruption error.
        let truncated_row_size = [0x80u8]; // 2-byte VInt header, no continuation.
        let err = parser
            .parse_row_metadata(&truncated_row_size, 0, /* row_flags */ 0x00, None)
            .expect_err("truncated row_size VInt must error, not fabricate a row");
        let msg = format!("{err}");
        assert!(
            msg.contains("Failed to parse row size"),
            "expected a specific row-size parse error, got: {msg}"
        );

        // Case 2: a valid single-byte row_size (0x05) followed by a truncated
        // prev_size VInt (0xC0 => 3-byte header, no continuation bytes). The
        // preamble parser must fail on prev_size specifically — proving it does
        // not silently accept a short preamble and emit a partial row.
        let truncated_prev_size = [0x05u8, 0xC0u8];
        let err = parser
            .parse_row_metadata(&truncated_prev_size, 0, 0x00, None)
            .expect_err("truncated prev_size VInt must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Failed to parse prev size"),
            "expected a specific prev-size parse error, got: {msg}"
        );

        // Case 3: HAS_TIMESTAMP set but the timestamp-delta VInt is truncated
        // (valid row_size + prev_size, then a 0x80 header with no continuation).
        // The preamble parser must fail on the timestamp delta — no partial row.
        let truncated_ts = [0x05u8, 0x00u8, 0x80u8];
        let err = parser
            .parse_row_metadata(&truncated_ts, 0, ROW_HAS_TIMESTAMP, None)
            .expect_err("truncated timestamp-delta VInt must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Failed to parse timestamp delta"),
            "expected a specific timestamp-delta parse error, got: {msg}"
        );

        // Sanity: a well-formed minimal preamble parses successfully and reports
        // the declared row_size verbatim — proving the failures above are caused
        // by the truncation, not by the parser rejecting all input. ROW_HAS_ALL_COLUMNS
        // (0x20) is set so no missing-columns bitmap VInt is expected, leaving the
        // preamble as exactly row_size + prev_size.
        let well_formed = [0x07u8, 0x00u8];
        let (_hdr, row_size) = parser
            .parse_row_metadata(&well_formed, 0, ROW_HAS_ALL_COLUMNS, None)
            .expect("well-formed minimal preamble must parse");
        assert_eq!(
            row_size, 7,
            "row_size must be decoded verbatim from preamble"
        );
    }

    #[test]
    fn test_non_zero_minima_delta_decoding() {
        // Test delta decoding with non-zero minima from ttl_test_table
        // Statistics.db shows:
        //   min_timestamp: 1759713125983682
        //   min_local_deletion_time: 1759799525
        //   min_ttl: 86400
        //
        // Row header format with HAS_TIMESTAMP (0x04) + HAS_TTL (0x08) + HAS_ALL_COLUMNS (0x20) = 0x2C
        // [row_flags: 0x2C] [row_size: VInt] [prev_size: VInt]
        // [timestamp_delta: UNSIGNED VInt]   ← fix #629: was ZigZag, now unsigned
        // [ttl_delta: UNSIGNED VInt]
        // [liveness_ldt_delta: UNSIGNED VInt] ← fix #630: was absent, now required
        // (NO column bitmap because HAS_ALL_COLUMNS is set)
        //
        // Updated from original: was "2c640087d000" which used ZigZag(1000)=[0x87,0xD0]
        // for the timestamp and was missing the liveness_ldt field for HAS_TTL.
        //
        // Now: unsigned_vint(1000) = [0x83, 0xE8], plus liveness_ldt_delta = 0 (0x00).

        let min_timestamp = 1759713125983682i64;
        let min_ttl = 86400i64;
        let min_ldt = 1759799525i64;
        let ts_delta: u64 = 1000;
        let ttl_delta: u64 = 0;
        let ldt_delta: u64 = 0;

        let mut data: Vec<u8> = Vec::new();
        data.push(0x2Cu8); // flags: HAS_TIMESTAMP(0x04)|HAS_TTL(0x08)|HAS_ALL_COLUMNS(0x20)
        encode_unsigned(100, &mut data); // row_size = 100 → [0x64]
        encode_unsigned(0, &mut data); // prev_size = 0  → [0x00]
        encode_unsigned(ts_delta, &mut data); // timestamp_delta = 1000 → [0x83, 0xE8]
        encode_unsigned(ttl_delta, &mut data); // ttl_delta = 0 → [0x00]
        encode_unsigned(ldt_delta, &mut data); // liveness_ldt_delta = 0 → [0x00]

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "ttl_test_table".to_string(),
            min_timestamp,
            min_ldt,
            Some(min_ttl),
        );

        // Issue #213: Use split functions - parse flags first, then metadata
        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        assert_eq!(flags_size, 1, "Flags should consume 1 byte");

        // For testing, since there's no clustering in this test data, metadata starts at offset 1
        let (row_header, row_size) = parser
            .parse_row_metadata(&data, flags_size, row_flags, extended_flags)
            .unwrap();

        // Verify delta decoding: absolute_timestamp = min_timestamp + delta (unsigned)
        assert_eq!(
            row_header.timestamp,
            Some(min_timestamp + ts_delta as i64),
            "Timestamp should be decoded as min_timestamp + delta (unsigned VInt, fix #629)"
        );

        // Verify TTL delta decoding: absolute_ttl = min_ttl + delta
        assert_eq!(
            row_header.ttl,
            Some(min_ttl as i32),
            "TTL should be decoded as min_ttl + delta (delta=0)"
        );

        // Verify row_size was parsed
        assert!(row_size > 0, "Row size should be positive");
    }

    #[test]
    fn test_row_header_with_deletion_time() {
        // Verify delta decoding of the HAS_DELETION field in Cassandra canonical order
        // (Issue #505). DeletionTime.Serializer writes markedForDeleteAt FIRST, then
        // localDeletionTime:
        //   [row_flags] [row_size: VInt] [prev_size: VInt]
        //   [markedForDeleteAt_delta: UNSIGNED VInt]  (base = min_timestamp, micros)
        //   [localDeletionTime_delta: UNSIGNED VInt]  (base = min_local_deletion_time, secs)
        //
        // Fix #629 (C2): Both deltas are UNSIGNED per Cassandra SerializationHeader.java.
        // Test updated to encode mfda_delta as unsigned VInt (was ZigZag/signed before).
        use crate::parser::vint::encode_vuint;

        // Row header with HAS_DELETION (0x10) + HAS_ALL_COLUMNS (0x20) = 0x30.
        let mut data: Vec<u8> = Vec::new();
        data.push(0x30); // flags
        data.extend(encode_vuint(100)); // row_size = 100
        data.extend(encode_vuint(0)); // prev_size = 0
        let mfda_delta: u64 = 80; // markedForDeleteAt delta (unsigned, fix #629)
        let ldt_delta: u64 = 50; // localDeletionTime delta (unsigned)
        data.extend(encode_vuint(mfda_delta));
        data.extend(encode_vuint(ldt_delta));

        let min_timestamp = 1759713125983682i64;
        let min_local_deletion_time = 1759799525i64;
        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "test_table".to_string(),
            min_timestamp,
            min_local_deletion_time,
            None,
        );

        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _row_size) = parser
            .parse_row_metadata(&data, flags_size, row_flags, extended_flags)
            .unwrap();

        // markedForDeleteAt: absolute = min_timestamp + delta (microseconds, UNSIGNED delta).
        // This is the authoritative reconciliation timestamp used by the compaction merger.
        assert_eq!(
            row_header.marked_for_delete_at,
            Some(min_timestamp + mfda_delta as i64),
            "markedForDeleteAt must be decoded from the FIRST (unsigned) VInt as min_timestamp + delta"
        );
        // The row-tombstone deletion time (used in Value::Tombstone) must equal it.
        assert_eq!(
            row_header.row_tombstone_deletion_time(),
            min_timestamp + mfda_delta as i64,
            "row tombstone deletion_time must be markedForDeleteAt, not local_deletion_time"
        );

        // localDeletionTime: absolute = min_local_deletion_time + delta (seconds).
        assert_eq!(
            row_header.local_deletion_time,
            Some((min_local_deletion_time + ldt_delta as i64) as i32),
            "localDeletionTime must be decoded from the SECOND (unsigned) VInt as min + delta"
        );

        assert!(
            row_header.is_row_tombstone(),
            "HAS_DELETION row must be reported as a row tombstone"
        );
    }

    #[test]
    fn test_sparse_column_bitmap_parsing() {
        // Test column bitmap parsing when NOT HAS_ALL_COLUMNS
        // Row header WITHOUT HAS_ALL_COLUMNS flag (0x20)
        // Should parse single VUInt bitmap after metadata fields
        //
        // Cassandra format: single VUInt bitmask of missing columns
        // (bit=1 → column missing, bit=0 → column present)
        //
        // Row header format: [flags: 0x04] [row_size] [prev_size] [timestamp]
        // [missing_columns_bitmap: VUInt]

        // Construct row with HAS_TIMESTAMP but NOT HAS_ALL_COLUMNS
        // bitmap=0x05 means columns 0 and 2 are MISSING
        let row_header_hex = "04640000 05"; // flags=0x04, size=100, prev=0, ts=0 (signed), bitmap=0x05
        let row_header_hex = row_header_hex.replace(' ', "");
        let data = hex::decode(row_header_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "sparse_table".to_string(),
            0,
            0,
            None,
        );

        // Issue #213: Use split functions - parse flags first, then metadata
        // This tests that parse_row_metadata handles column bitmap correctly
        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let result = parser.parse_row_metadata(&data, flags_size, row_flags, extended_flags);

        // Should succeed without panicking on bitmap parsing
        assert!(
            result.is_ok(),
            "Row header with column bitmap should parse successfully"
        );

        let (row_header, _row_size) = result.unwrap();
        // Verify header was parsed (has timestamp)
        assert_eq!(row_header.timestamp, Some(0));

        // Verify missing_columns_bitmap is captured
        assert_eq!(
            row_header.missing_columns_bitmap,
            Some(0x05),
            "Bitmap 0x05 means columns 0 and 2 are MISSING"
        );

        // Verify header_size includes bitmap VUInt (but NOT flags, parsed separately)
        // size(1) + prev(1) + timestamp(1) + bitmap(1) = 4
        assert_eq!(
            row_header.header_size, 4,
            "Header size should include column bitmap VUInt but not flags (parsed separately)"
        );
    }

    #[test]
    fn test_bitmap_filter_does_not_panic_for_wide_schemas() {
        // Verify that bitmap filtering with idx >= 64 does not panic.
        // Columns beyond bit 63 are not represented in the u64 bitmap
        // and should be treated as present (not filtered out).
        let bitmap: u64 = 0x05; // bits 0 and 2 are set (missing)
        let total_columns = 70; // wider than 64

        let kept: Vec<usize> = (0..total_columns)
            .filter(|idx| *idx >= 64 || (bitmap & (1u64 << idx)) == 0)
            .collect();

        // Columns 0 and 2 should be filtered out, all others kept
        assert!(!kept.contains(&0));
        assert!(kept.contains(&1));
        assert!(!kept.contains(&2));
        assert!(kept.contains(&3));
        // All columns >= 64 should be kept
        for i in 64..total_columns {
            assert!(kept.contains(&i), "Column {} should be kept", i);
        }
        assert_eq!(kept.len(), 68); // 70 - 2 missing = 68
    }

    #[test]
    fn test_clustering_key_partition_header() {
        // Test partition header parsing for composite key table
        // composite_key_table has clustering columns: [ReversedType(TimestampType), UTF8Type]
        //
        // Partition header format:
        // [flags: u8] [key_len: u8] [partition_key_bytes] [deletion_time: i32] [unknown: i64]
        //
        // From composite_key_table JSONL:
        // partition key: "245dff69-026f-45c6-b68f-ba0c964df3c9"
        // clustering: ["2025-10-06 01:12:06.059Z","information"]
        //
        // Note: Clustering keys are part of row data, not partition header
        // This test verifies partition header parsing for composite key tables

        let partition_hex = "0010245dff69026f45c6b68fba0c964df3c97fffffff8000000000000000";
        let data = hex::decode(partition_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "composite_key_table".to_string(),
            1759713125977357, // min_timestamp from Statistics.db
            1442880000,       // min_local_deletion_time
            None,
        );

        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction (UUID is 16 bytes)
        assert_eq!(row_key.0.len(), 16);

        // Verify correct partition key bytes
        let expected_uuid_bytes = hex::decode("245dff69026f45c6b68fba0c964df3c9").unwrap();
        assert_eq!(row_key.as_bytes(), expected_uuid_bytes.as_slice());

        // Verify offset: flags(1) + len(1) + uuid(16) + del_time(4) + unknown(8) = 30
        assert_eq!(offset, 30);

        // Note: Clustering key parsing would happen during row data parsing,
        // which is tested separately in integration tests
    }

    // Issue #229: END_OF_PARTITION and range tombstone marker detection tests
    #[test]
    fn test_end_of_partition_detection() {
        // END_OF_PARTITION marker is exactly 0x01
        assert!(V5CompressedLegacyParser::is_end_of_partition(0x01));

        // Any other value should NOT be detected as END_OF_PARTITION
        // (using exact match to avoid false positives with row data)
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x00));
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x02)); // IS_MARKER only
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x03)); // Not exact 0x01
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x04)); // HAS_TIMESTAMP
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x24)); // HAS_TIMESTAMP | HAS_ALL_COLUMNS
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x80)); // EXTENDED_FLAGS
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0xb7)); // Random byte with bit 0 set
    }

    #[test]
    fn test_range_tombstone_marker_detection() {
        // IS_MARKER (0x02) uses bitwise detection - any flags with IS_MARKER bit set
        // and END_OF_PARTITION bit NOT set should be detected as marker
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x02)); // IS_MARKER alone
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x06)); // IS_MARKER | HAS_TIMESTAMP
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x52)); // IS_MARKER | other flags (real data)

        // Should NOT be detected as marker:
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x00)); // No flags
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x01)); // END_OF_PARTITION
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x03)); // END_OF_PARTITION | IS_MARKER (EOP takes precedence)
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x04)); // HAS_TIMESTAMP (no IS_MARKER)
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x24)); // HAS_TIMESTAMP | HAS_ALL_COLUMNS
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x80)); // EXTENDED_FLAGS (no IS_MARKER)
    }

    #[test]
    fn test_marker_detection_mutually_exclusive() {
        // When both END_OF_PARTITION (0x01) and IS_MARKER (0x02) bits are set,
        // END_OF_PARTITION takes precedence (0x03 is treated as end of partition)
        let flags = 0x03;
        assert!(!V5CompressedLegacyParser::is_end_of_partition(flags)); // Exact match check fails
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(flags)); // END_OF_PARTITION bit excludes marker
    }

    // Issue #264: END_OF_PARTITION marker handling test
    #[test]
    fn test_partition_header_end_of_partition_marker() {
        // Test that END_OF_PARTITION marker (0x01) is correctly handled
        // at partition boundaries - not mistaken for valid row data

        // Single byte 0x01 should be recognized as end marker
        let marker_byte = 0x01u8;
        assert!(
            V5CompressedLegacyParser::is_end_of_partition(marker_byte),
            "0x01 should be END_OF_PARTITION marker"
        );

        // Verify marker is NOT a range tombstone
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(marker_byte),
            "END_OF_PARTITION should not be mistaken for range tombstone"
        );

        // Test the marker byte in context - ensure detection works at any offset
        let data_with_marker = [0x24, 0x00, 0x01, 0x10]; // marker at offset 2
        assert!(
            V5CompressedLegacyParser::is_end_of_partition(data_with_marker[2]),
            "Should detect END_OF_PARTITION at offset 2"
        );

        // Verify non-marker bytes are not detected as END_OF_PARTITION
        for byte in [0x00u8, 0x02, 0x04, 0x24, 0x80, 0xb7] {
            assert!(
                !V5CompressedLegacyParser::is_end_of_partition(byte),
                "Byte 0x{:02x} should NOT be detected as END_OF_PARTITION",
                byte
            );
        }
    }

    // Issue #264: Range tombstone marker handling test
    #[test]
    fn test_range_tombstone_marker_handling() {
        // Test that IS_MARKER (0x02) is correctly identified for range tombstones
        // Range tombstone markers indicate deletion boundaries, not data rows

        // Basic IS_MARKER flag
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x02),
            "0x02 should be detected as range tombstone marker"
        );

        // IS_MARKER with additional flags (common in real data)
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x52),
            "0x52 (IS_MARKER|HAS_TIMESTAMP|HAS_ALL_COLUMNS) should be range tombstone"
        );
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x7a),
            "0x7a should be detected as range tombstone marker"
        );
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x06),
            "0x06 (IS_MARKER|HAS_TIMESTAMP) should be range tombstone"
        );

        // Verify marker handling doesn't interfere with normal row flags
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x24),
            "0x24 (HAS_TIMESTAMP|HAS_ALL_COLUMNS) is NOT a marker - it's a normal row"
        );
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x00),
            "0x00 is NOT a marker"
        );
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x80),
            "0x80 (EXTENDED_FLAGS only) is NOT a marker"
        );

        // Verify END_OF_PARTITION takes precedence over IS_MARKER bit
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x03),
            "0x03 has END_OF_PARTITION bit set, should NOT be range tombstone"
        );
    }

    // =========================================================================
    // S1 Audit Verification Tests — Issue #623
    //
    // Behavioural tests verifying CQLite's read-path cell/row encoding against
    // Apache Cassandra 5.0.8 source (report-B1.md / facts-B1.md).
    //
    // Claim summary:
    //   C1: USE_ROW_TIMESTAMP (0x08) / USE_ROW_TTL (0x10) — field OMITTED when set
    //   C2: All temporal deltas are UNSIGNED VInt (never ZigZag)
    //   C3: HAS_TTL implies TWO fields: [ttl VInt32] + [liveness_ldt VInt32]
    //   C4: Partition header = u16 BE key_len + key + DeletionTime (writer correct;
    //       V5CompressedLegacy reader uses legacy format with u8 key_len, different variant)
    //   C5: Missing-columns bitmap: bit=1 means MISSING; unsigned VInt
    // =========================================================================

    // -------------------------------------------------------------------------
    // C1: Cell flags 0x08/0x10 — USE_ROW_TIMESTAMP_MASK / USE_ROW_TTL_MASK
    //
    // Cassandra Cell.java:262-266:
    //   0x08 = USE_ROW_TIMESTAMP_MASK → timestamp field OMITTED from cell stream
    //   0x10 = USE_ROW_TTL_MASK       → LDT + TTL fields OMITTED from cell stream
    //
    // Verdict: CORRECT_BUT_UNTESTED → now tested.
    // -------------------------------------------------------------------------

    /// C1-a: Cell with USE_ROW_TIMESTAMP (0x08): no timestamp bytes between flags and value.
    ///
    /// When bit 0x08 is set, the timestamp field is ABSENT from the cell stream.
    /// The value bytes immediately follow the flags byte.
    ///
    /// Stream layout: [flags=0x08][int_value_4_bytes]
    /// Expected value_start_offset: 1 (flags only, no temporal bytes)
    #[test]
    fn s1_c1_cell_use_row_timestamp_omits_timestamp_field() {
        // flags = 0x08 (USE_ROW_TIMESTAMP_MASK): timestamp reused from row, not present here
        // Normally a cell without this flag would have a VInt timestamp delta here.
        // With 0x08 set, the VInt is ABSENT — value bytes start immediately at offset 1.
        let data = vec![
            0x08u8, // USE_ROW_TIMESTAMP only — timestamp absent
            0xABu8, // sentinel bytes that would be wrong if timestamp was consumed
            0xCDu8, 0xEFu8,
        ];

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (flags_out, value_start) = parser
            .parse_cell_header_end_offset(&data, 0)
            .expect("parse_cell_header_end_offset must succeed for USE_ROW_TIMESTAMP");

        assert_eq!(flags_out, 0x08u8);
        assert_eq!(
            value_start, 1,
            "USE_ROW_TIMESTAMP (0x08): value must start at offset 1 (flags only).\n\
             If value_start > 1, timestamp bytes were wrongly consumed."
        );
    }

    /// C1-b: Cell with IS_EXPIRING (0x02) + USE_ROW_TTL (0x10): LDT and TTL bytes ABSENT.
    ///
    /// When IS_EXPIRING is set WITHOUT USE_ROW_TTL, two extra fields appear: LDT VUInt + TTL VUInt.
    /// When IS_EXPIRING + USE_ROW_TTL (0x12), those two fields are OMITTED.
    ///
    /// To isolate the TTL omission from timestamp, we also set USE_ROW_TIMESTAMP (0x08).
    /// flags = 0x1A = IS_EXPIRING | USE_ROW_TIMESTAMP | USE_ROW_TTL
    ///   → no timestamp bytes (0x08 set)
    ///   → no LDT/TTL bytes (0x10 set overrides IS_EXPIRING LDT/TTL)
    ///   → value starts immediately at offset 1
    ///
    /// Compare with IS_EXPIRING + USE_ROW_TIMESTAMP alone (0x0A = 0x08 | 0x02):
    ///   → no timestamp bytes, BUT LDT and TTL bytes ARE present
    #[test]
    fn s1_c1_cell_use_row_ttl_with_expiring_omits_ldt_ttl() {
        // flags = 0x1A = USE_ROW_TIMESTAMP (0x08) | IS_EXPIRING (0x02) | USE_ROW_TTL (0x10)
        // All three flags: timestamp absent, LDT absent, TTL absent → value at offset 1
        let data_with_use_row_ttl = vec![0x1Au8, 0xFFu8, 0xFFu8, 0xFFu8];

        // flags = 0x0A = USE_ROW_TIMESTAMP (0x08) | IS_EXPIRING (0x02)
        // No USE_ROW_TTL: timestamp absent but LDT + TTL VUInts are present
        // Use VUInt(50) = 0x32 (1 byte, < 128) for both LDT and TTL deltas
        let data_without_use_row_ttl = vec![0x0Au8, 0x32u8, 0x32u8, 0xFFu8];

        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);

        // With USE_ROW_TTL: value starts at offset 1 (no LDT, no TTL consumed)
        let (_, value_start_with) = parser
            .parse_cell_header_end_offset(&data_with_use_row_ttl, 0)
            .expect("parse_cell_header_end_offset for IS_EXPIRING+USE_ROW_TTL");
        assert_eq!(
            value_start_with, 1,
            "IS_EXPIRING+USE_ROW_TTL (0x1A): LDT and TTL must be ABSENT, value starts at 1"
        );

        // Without USE_ROW_TTL: value starts at offset 3 (LDT=1byte + TTL=1byte after flags)
        let (_, value_start_without) = parser
            .parse_cell_header_end_offset(&data_without_use_row_ttl, 0)
            .expect("parse_cell_header_end_offset for IS_EXPIRING without USE_ROW_TTL");
        assert_eq!(
            value_start_without, 3,
            "IS_EXPIRING without USE_ROW_TTL (0x0A): LDT+TTL present, value starts at 3"
        );

        // This contrast proves the USE_ROW_TTL flag causes LDT and TTL bytes to be omitted.
        assert!(
            value_start_with < value_start_without,
            "USE_ROW_TTL must reduce header size by omitting LDT+TTL bytes"
        );
    }

    /// C1-c: Cell with BOTH 0x08 and 0x10: no timestamp, no LDT, no TTL.
    ///
    /// Both USE_ROW_TIMESTAMP and USE_ROW_TTL set — all temporal fields absent.
    #[test]
    fn s1_c1_cell_use_row_timestamp_and_ttl_combined() {
        // 0x18 = USE_ROW_TIMESTAMP | USE_ROW_TTL
        let data = vec![0x18u8, 0xFFu8]; // sentinel

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (flags_out, value_start) = parser
            .parse_cell_header_end_offset(&data, 0)
            .expect("parse_cell_header_end_offset must succeed for USE_ROW_TIMESTAMP|USE_ROW_TTL");

        assert_eq!(flags_out, 0x18u8);
        assert_eq!(
            value_start, 1,
            "USE_ROW_TIMESTAMP|USE_ROW_TTL (0x18): value must start at offset 1.\n\
             All temporal fields must be absent."
        );
    }

    // -------------------------------------------------------------------------
    // C2: All temporal deltas must be UNSIGNED VInt (not ZigZag)
    //
    // Cassandra SerializationHeader.java:165-177:
    //   writeTimestamp()         → writeUnsignedVInt(ts - min_ts)
    //   writeTTL()               → writeUnsignedVInt32(ttl - min_ttl)
    //   writeLocalDeletionTime() → writeUnsignedVInt32(ldt - min_ldt)
    //
    // BUG: parse_row_metadata() uses parse_vint (ZigZag) for:
    //   - row-level timestamp delta (HAS_TIMESTAMP, line ~1120)
    //   - markedForDeleteAt delta (HAS_DELETION, line ~1179)
    // These must use parse_vuint (unsigned VInt) per Cassandra source.
    //
    // Verdict: BUG — failing tests document the required correct behavior.
    // Bug issue: see child issues filed for #623.
    // -------------------------------------------------------------------------

    /// C2-proof: Show that unsigned VInt(1000) ≠ ZigZag VInt(1000).
    /// This documents the byte-level discrepancy.
    ///
    ///   unsigned VInt(1000):  [0x83, 0xE8]  (1000 = 0x3E8 → 10_000011 11101000)
    ///   ZigZag VInt(1000):    [0x87, 0xD0]  (zigzag(1000)=2000 → 10_000111 11010000)
    ///
    /// When Cassandra writes unsigned VInt and CQLite reads with parse_vint (ZigZag):
    ///   parse_vint([0x83, 0xE8]) = zigzag_decode(1000) = 500  ← WRONG, should be 1000
    #[test]
    fn s1_c2_unsigned_vint_differs_from_zigzag_for_delta_1000() {
        use crate::parser::vint::{parse_vint, parse_vuint};

        let delta: u64 = 1000;

        // What Cassandra writes (unsigned VInt):
        let mut cassandra_bytes = Vec::new();
        encode_unsigned(delta, &mut cassandra_bytes);
        assert_eq!(
            cassandra_bytes,
            vec![0x83, 0xE8],
            "unsigned VInt(1000) must be [0x83, 0xE8]"
        );

        // What CQLite currently reads with parse_vint (ZigZag) applied to Cassandra bytes:
        let (_, from_zigzag) = parse_vint(&cassandra_bytes).unwrap();
        // zigzag_decode(1000) = 500, not 1000!
        assert_ne!(
            from_zigzag, 1000i64,
            "parse_vint (ZigZag decoder) mis-decodes Cassandra unsigned VInt(1000) as {}",
            from_zigzag
        );
        // Document what the wrong value is
        assert_eq!(
            from_zigzag, 500i64,
            "ZigZag mis-decode of unsigned VInt(1000) must yield 500 (proving the bug)"
        );

        // Correct decode via parse_vuint:
        let (_, correct) = parse_vuint(&cassandra_bytes).unwrap();
        assert_eq!(
            correct, 1000u64,
            "parse_vuint must correctly decode to 1000"
        );
    }

    /// C2: Row timestamp delta with Cassandra-canonical unsigned encoding must decode correctly.
    ///
    /// min_timestamp = 1_000_000, delta = 1000
    /// Expected absolute = 1_001_000
    ///
    /// Row bytes (HAS_TIMESTAMP | HAS_ALL_COLUMNS = 0x24, no clustering):
    ///   [0x24][row_size=0x00][prev_size=0x00][unsigned_vint(1000)]
    ///
    /// CURRENT behavior (ZigZag bug): 1_000_000 + 500 = 1_000_500
    /// CORRECT behavior (unsigned VInt): 1_000_000 + 1000 = 1_001_000
    ///
    /// This test asserts the CORRECT behavior and will FAIL until the bug is fixed.
    #[test]
    fn s1_c2_row_timestamp_cassandra_unsigned_encoding_must_decode_correctly() {
        let min_timestamp = 1_000_000i64;
        let delta: u64 = 1000;
        let expected = min_timestamp + delta as i64; // = 1_001_000

        let mut ts_bytes = Vec::new();
        encode_unsigned(delta, &mut ts_bytes); // [0x83, 0xE8]

        let mut data = Vec::new();
        data.push(0x24u8); // HAS_TIMESTAMP (0x04) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size VInt = 0
        data.push(0x00u8); // prev_size VInt = 0
        data.extend_from_slice(&ts_bytes);

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            min_timestamp,
            0,
            None,
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.timestamp,
            Some(expected),
            "Row timestamp delta must use unsigned VInt.\n\
             Expected {} (= min_timestamp {} + delta {})\n\
             Got {:?}\n\
             Note: if got Some({}), ZigZag is being used (bug C2)",
            expected,
            min_timestamp,
            delta,
            row_header.timestamp,
            min_timestamp + (delta as i64 >> 1), // what ZigZag would give
        );
    }

    /// C2: markedForDeleteAt delta (HAS_DELETION) must use unsigned VInt.
    ///
    /// Row bytes (HAS_DELETION | HAS_ALL_COLUMNS = 0x30):
    ///   [0x30][row_size=0x00][prev_size=0x00][unsigned_vint(mfda_delta)][unsigned_vint(ldt_delta)]
    ///
    /// CURRENT behavior (ZigZag bug): mfda decoded as 500 instead of 1000
    /// CORRECT behavior: mfda = 1_001_000
    #[test]
    fn s1_c2_marked_for_delete_at_cassandra_unsigned_encoding_must_decode_correctly() {
        let min_timestamp = 1_000_000i64;
        let mfda_delta: u64 = 1000;
        let ldt_delta: u64 = 100;
        let expected_mfda = min_timestamp + mfda_delta as i64; // 1_001_000

        let mut mfda_bytes = Vec::new();
        encode_unsigned(mfda_delta, &mut mfda_bytes);
        let mut ldt_bytes = Vec::new();
        encode_unsigned(ldt_delta, &mut ldt_bytes);

        let mut data = Vec::new();
        data.push(0x30u8); // HAS_DELETION (0x10) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size
        data.push(0x00u8); // prev_size
        data.extend_from_slice(&mfda_bytes);
        data.extend_from_slice(&ldt_bytes);

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            min_timestamp,
            0,
            None,
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.marked_for_delete_at,
            Some(expected_mfda),
            "markedForDeleteAt delta must use unsigned VInt.\n\
             Expected {} (= min {} + delta {})\n\
             Got {:?}\n\
             Note: if got Some({}), ZigZag is being used (bug C2)",
            expected_mfda,
            min_timestamp,
            mfda_delta,
            row_header.marked_for_delete_at,
            min_timestamp + (mfda_delta as i64 >> 1),
        );
    }

    // -------------------------------------------------------------------------
    // C3: HAS_TTL (0x08) implies TWO fields: [ttl: VInt32] + [liveness_ldt: VInt32]
    //
    // Cassandra UnfilteredSerializer.java:225-228:
    //   if ((flags & HAS_TTL) != 0) {
    //       header.writeTTL(pkLiveness.ttl(), out);                         // VInt32
    //       header.writeLocalDeletionTime(pkLiveness.localExpirationTime(), out); // VInt32
    //   }
    //
    // BUG: parse_row_metadata reads only ONE VInt (TTL), skips the LDT VInt.
    // This causes misalignment of all subsequent fields in TTL rows.
    //
    // Verdict: BUG — header_size must cover both VInts.
    // Bug issue: see child issues filed for #623.
    // -------------------------------------------------------------------------

    /// C3: Row with HAS_TTL must consume BOTH TTL and LDT VInts from the stream.
    ///
    /// Row bytes (HAS_TTL | HAS_ALL_COLUMNS = 0x28, no timestamp):
    ///   [flags=0x28][row_size=0x00][prev_size=0x00][ttl_delta=0x64][ldt_delta=0x32]
    ///   ^--- ttl=100 (1 byte, <128)                                 ^--- ldt=50 (1 byte, <128)
    ///
    /// parse_row_metadata starts at pos=flags_size=1 (flags already consumed):
    ///   row_size(1) + prev_size(1) + ttl(1) + ldt(1) = 4 bytes consumed after flags
    ///   header_size = pos_end - flags_size = 5 - 1 = 4
    ///
    /// PREVIOUS (bug): header_size = 3 — LDT byte not consumed, misaligning later fields.
    /// CORRECT after fix: header_size = 4 — both TTL and LDT consumed.
    ///
    /// Uses single-byte values (< 128) so encode_unsigned produces 1 byte each.
    #[test]
    fn s1_c3_has_ttl_reads_two_vint_fields_ttl_and_ldt() {
        let ttl_delta: u64 = 100; // 1 byte: 0x64 (100 < 128)
        let ldt_delta: u64 = 50; // 1 byte: 0x32 (50 < 128)

        let mut ttl_bytes = Vec::new();
        encode_unsigned(ttl_delta, &mut ttl_bytes); // [0x64]
        assert_eq!(ttl_bytes.len(), 1, "ttl_delta=100 must encode to 1 byte");
        let mut ldt_bytes = Vec::new();
        encode_unsigned(ldt_delta, &mut ldt_bytes); // [0x32]
        assert_eq!(ldt_bytes.len(), 1, "ldt_delta=50 must encode to 1 byte");

        let mut data = Vec::new();
        data.push(0x28u8); // HAS_TTL (0x08) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size VInt = 0
        data.push(0x00u8); // prev_size VInt = 0
        data.extend_from_slice(&ttl_bytes); // TTL delta (1 byte = 0x64)
        data.extend_from_slice(&ldt_bytes); // LDT delta (1 byte = 0x32) — fix: must now be read
        data.push(0xFFu8); // sentinel — must NOT be consumed by metadata parsing

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            0,
            1_600_000_000,
            Some(3600),
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        assert_eq!(flags_size, 1);

        let result = parser.parse_row_metadata(&data, flags_size, row_flags, ext_flags);
        assert!(
            result.is_ok(),
            "parse_row_metadata must succeed for HAS_TTL row"
        );
        let (row_header, _row_size) = result.unwrap();

        // TTL must decode correctly
        let expected_ttl = (3600i64 + ttl_delta as i64) as i32;
        assert_eq!(
            row_header.ttl,
            Some(expected_ttl),
            "TTL delta must decode correctly"
        );

        // header_size must include BOTH TTL (1) and LDT (1) bytes plus row_size(1) + prev_size(1) = 4
        // Explanation: parse_row_metadata starts at pos=flags_size=1; after consuming
        //   row_size(1), prev_size(1), ttl(1), ldt(1) → pos=5; header_size = 5-1 = 4.
        // Before fix: header_size was 3 (ldt not consumed).
        assert_eq!(
            row_header.header_size, 4,
            "HAS_TTL row_header.header_size must be 4 (row_size + prev_size + ttl + ldt).\n\
             Got {} — if 3, the LDT VInt after TTL was NOT consumed (C3 bug present)",
            row_header.header_size
        );
    }

    // -------------------------------------------------------------------------
    // C4: Partition header format — u16 BE key_len + key + DeletionTime
    //
    // Cassandra SortedTablePartitionWriter.java:104-105:
    //   ByteBufferUtil.writeWithShortLength(key) → [u16 BE key_len][key_bytes]
    //   then DeletionTime serialized.
    //
    // V5CompressedLegacyParser.parse_partition_header() uses [u8 flags][u8 key_len]
    // which is the legacy compressed block format — intentionally different from the
    // modern Cassandra BigFormat. The data_writer.rs correctly uses u16 BE key_len.
    //
    // Verdict: CORRECT (writer uses Cassandra-canonical u16 BE key length).
    //          V5CompressedLegacy reader uses legacy format by design.
    // -------------------------------------------------------------------------

    /// C4: Verify partition key length in data_writer uses u16 BE (Cassandra-canonical).
    /// Tests existing data_writer unit test vectors to confirm the format.
    ///
    /// data_writer.rs write_partition_header():
    ///   self.buffer.write_all(&(key.key.len() as u16).to_be_bytes())
    ///
    /// The existing test at line ~2664 in data_writer.rs already verifies:
    ///   assert_eq!(&bytes[0..2], &[0x00, 0x04])  // key length 4 as u16 BE
    ///
    /// This test documents C4 as CORRECT by verifying the legacy reader format:
    /// [u8 flags=0x00][u8 key_len][key_bytes][i32 del_time][u64 unknown] = 30 bytes for UUID.
    #[test]
    fn s1_c4_v5_legacy_reader_partition_header_format_documented() {
        // The V5CompressedLegacy format uses [u8 flags][u8 key_len] — legacy design.
        // This test documents and validates the legacy format is handled consistently.
        //
        // Real Cassandra SSTable partition header hex from test_basic/simple_table:
        //   00 10 15291a77d7394e738397b787442f3a1f 7fffffff 8000000000000000
        //   ^flags ^len  ^16-byte UUID                  ^i32 del  ^u64 unknown
        let hex_str = "001015291a77d7394e738397b787442f3a1f7fffffff8000000000000000";
        let data = hex::decode(hex_str).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,
            0,
            None,
        );
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();
        assert_eq!(row_key.0.len(), 16, "UUID partition key must be 16 bytes");
        // Total: 1 (flags) + 1 (len) + 16 (UUID) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(
            offset, 30,
            "Legacy partition header must consume 30 bytes for UUID key"
        );

        // For contrast: the writer (data_writer.rs) uses u16 BE key length (Cassandra-canonical).
        // That format is: [u16 key_len][key_bytes][DeletionTime].
        // The legacy reader and the writer serve different format variants.
        // Both are intentional and consistent with their respective format specs.
    }

    // -------------------------------------------------------------------------
    // C5: Missing-columns bitmap — bit=1 means MISSING (Cassandra convention)
    //
    // Cassandra Columns.java:519-530:
    //   For superset < 64 cols: single unsigned VInt where bit=1 = column ABSENT
    //   For superset >= 64 cols: delta + column indices
    //
    // CQLite parse_row_metadata() uses parse_vuint and comment says "bit=1 means missing".
    //
    // Verdict: CORRECT_BUT_UNTESTED → now tested.
    // -------------------------------------------------------------------------

    /// C5-a: NOT HAS_ALL_COLUMNS → bitmap present; bit=1 means column MISSING.
    ///
    /// Row: HAS_TIMESTAMP (0x04) only (NOT HAS_ALL_COLUMNS).
    /// bitmap = 0x05 = 0b00000101: columns 0 and 2 absent, column 1 present.
    #[test]
    fn s1_c5_missing_columns_bitmap_bit1_means_absent() {
        let ts_delta: u64 = 0;
        let bitmap: u64 = 0x05; // cols 0 and 2 missing

        let mut ts_bytes = Vec::new();
        encode_unsigned(ts_delta, &mut ts_bytes);
        let mut bm_bytes = Vec::new();
        encode_unsigned(bitmap, &mut bm_bytes);

        let mut data = Vec::new();
        data.push(0x04u8); // HAS_TIMESTAMP only (no HAS_ALL_COLUMNS)
        data.push(0x00u8); // row_size = 0
        data.push(0x00u8); // prev_size = 0
        data.extend_from_slice(&ts_bytes);
        data.extend_from_slice(&bm_bytes);

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.missing_columns_bitmap,
            Some(0x05),
            "Missing columns bitmap must be 0x05"
        );

        let bm = row_header.missing_columns_bitmap.unwrap();
        // Cassandra bit=1 means column ABSENT:
        assert_ne!(bm & (1 << 0), 0, "Column 0 must be MISSING (bit 0 set)");
        assert_eq!(bm & (1 << 1), 0, "Column 1 must be PRESENT (bit 1 clear)");
        assert_ne!(bm & (1 << 2), 0, "Column 2 must be MISSING (bit 2 set)");
    }

    /// C5-b: HAS_ALL_COLUMNS (0x20) → no bitmap field → None.
    #[test]
    fn s1_c5_has_all_columns_no_bitmap() {
        let data = vec![0x20u8, 0x00u8, 0x00u8]; // HAS_ALL_COLUMNS only, row_size=0, prev_size=0

        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.missing_columns_bitmap, None,
            "HAS_ALL_COLUMNS must not read a bitmap"
        );
    }

    // -------------------------------------------------------------------------
    // VInt correctness: unsigned VInt vs ZigZag encoding test vectors
    // -------------------------------------------------------------------------

    /// VInt-a: Cassandra unsigned VInt (writeUnsignedVInt) encoding test vectors.
    /// Verified against Cassandra VIntCoding.java algorithm and facts-B1.md §VInt.
    #[test]
    fn s1_vint_unsigned_encoding_test_vectors() {
        use crate::parser::vint::parse_vuint;

        let test_cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),                 // single byte 0
            (1, &[0x01]),                 // single byte 1
            (127, &[0x7F]),               // max single byte
            (128, &[0x80, 0x80]),         // min 2-byte
            (1000, &[0x83, 0xE8]),        // 2-byte: 10_000011 11101000
            (5000, &[0x93, 0x88]),        // audit report-B1 finding #30: unsigned(5000) = 0x93 0x88
            (7200, &[0x9C, 0x20]),        // audit report-B1 finding #31: unsigned(7200) = 0x9C 0x20
            (16383, &[0xBF, 0xFF]),       // max 2-byte
            (16384, &[0xC0, 0x40, 0x00]), // min 3-byte
        ];

        for (value, expected) in test_cases {
            let mut buf = Vec::new();
            encode_unsigned(*value, &mut buf);
            assert_eq!(
                buf.as_slice(),
                *expected,
                "encode_unsigned({}) = {:?}, expected {:?}",
                value,
                buf,
                expected
            );

            let (rem, decoded) = parse_vuint(&buf).unwrap();
            assert!(
                rem.is_empty(),
                "parse_vuint must consume all bytes for {}",
                value
            );
            assert_eq!(decoded, *value, "round-trip failed for {}", value);
        }
    }

    /// VInt-b: ZigZag encoding test vectors (used only for signed fields, NOT for SSTable temporal fields).
    /// Facts-B1.md confirms: ZigZag is used only in on-wire messaging, not SSTable row serialization.
    #[test]
    fn s1_vint_zigzag_encoding_test_vectors() {
        use crate::parser::vint::{zigzag_decode, zigzag_encode};

        let test_cases: &[(i64, u64)] = &[
            (0, 0),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (63, 126),
            (-64, 127),
            (64, 128),
        ];
        for (signed, unsigned) in test_cases {
            assert_eq!(
                zigzag_encode(*signed),
                *unsigned,
                "zigzag_encode({})",
                signed
            );
            assert_eq!(
                zigzag_decode(*unsigned),
                *signed,
                "zigzag_decode({})",
                unsigned
            );
        }
    }
}
