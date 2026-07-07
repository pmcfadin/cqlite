//! Cell-level encoding: regular cells, TTL/expiring cells, tombstone cells, and range-tombstone bounds.
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, flag constants, and crate imports
//! re-exported from `data_writer/mod.rs`. No emitted bytes change.

use super::*;

impl DataWriter {
    /// Write a single cell
    ///
    /// Format:
    /// ```text
    /// [flags: u8]
    /// [timestamp_delta: VInt if NOT USE_ROW_TIMESTAMP]
    /// [value_length: VInt]
    /// [value_bytes]
    /// ```
    ///
    /// NOTE: NULL values should NOT be written - they are represented by absence in the bitmap.
    /// This function will return an error if called with Value::Null.
    pub(super) fn write_cell(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
    ) -> Result<()> {
        // NULL values should not be written as cells - they are represented by absence
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        // Cell flags
        let mut flags = CELL_USE_ROW_TIMESTAMP; // Use row timestamp by default

        // Empty string: set HAS_EMPTY_VALUE flag
        // This is for actual empty strings (''), not NULLs
        let is_empty_string = matches!(value, Value::Text(s) if s.is_empty());
        if is_empty_string {
            flags |= CELL_HAS_EMPTY_VALUE;
        }

        buf.push(flags);

        // Timestamp (skip if USE_ROW_TIMESTAMP)
        // Fix #644 (S6): Cell timestamp delta is UNSIGNED VInt per Cassandra
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp).
        if (flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, buf);
        }

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        // Value
        let value_bytes = serialize_value(value)?;

        // Bounds check: value length must fit in i64
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
    }

    /// Write a live cell that carries its own timestamp (no USE_ROW_TIMESTAMP).
    ///
    /// Used for cells merged into a row from a different mutation than the
    /// one providing the row's liveness timestamp.
    ///
    /// Format:
    /// ```text
    /// [flags: u8]                ← 0x00 (or HAS_EMPTY_VALUE for empty text)
    /// [timestamp_delta: VUInt]   ← delta from min_timestamp
    /// [value_length: VInt]       ← variable-length types only
    /// [value_bytes]
    /// ```
    pub(super) fn write_cell_explicit_ts(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
    ) -> Result<()> {
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let mut flags = 0u8;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        // Timestamp delta (UNSIGNED VInt)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        let value_bytes = serialize_value(value)?;
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        buf.extend_from_slice(&value_bytes);
        Ok(())
    }

    /// Write a cell with TTL (expiring cell)
    ///
    /// Format:
    /// ```text
    /// [flags: u8]                    ← CELL_IS_EXPIRING (0x02) set
    /// [timestamp_delta: VInt]        ← Delta from min_timestamp (NOT USE_ROW_TIMESTAMP for TTL cells)
    /// [local_deletion_time_delta: VUInt]  ← When the cell expires (relative to min_local_deletion_time)
    /// [ttl_delta: VUInt]            ← TTL value (relative to min_ttl)
    /// [value_length: VInt]
    /// [value_bytes]
    /// ```
    ///
    /// CRITICAL: TTL cells MUST NOT use USE_ROW_TIMESTAMP or USE_ROW_TTL flags.
    /// They need explicit timestamp and TTL deltas.
    ///
    /// `explicit_ldt` (issue #1538): when `Some`, the cell's `localDeletionTime`
    /// (Cassandra `localExpirationTime`) is stamped VERBATIM from this authoritative
    /// per-cell value (e.g. a surviving expiring cell preserved through compaction),
    /// so the emitted bytes are byte-identical to the source cell. When `None`, the
    /// LDT is derived from `now_seconds + ttl` (historical fresh-write behavior).
    ///
    /// `now_seconds` (issue #2038 Scope B, roborev): the CALLER's single captured
    /// wall-clock reading for this whole write operation (mirrors Cassandra's
    /// `FBUtilities.nowInSeconds()`, captured once per mutation) — NOT a fresh
    /// `SystemTime::now()` read here. Passing a shared value keeps every
    /// expiring cell/row/element of one write on the identical clock reading;
    /// see `DataWriter::capture_now_seconds`.
    pub(super) fn write_cell_with_ttl(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
        ttl_seconds: u32,
        explicit_ldt: Option<i32>,
        now_seconds: i32,
    ) -> Result<()> {
        // NULL values should not be written as cells
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let local_deletion_time = match explicit_ldt {
            Some(ldt) => ldt,
            None => self.expiring_local_deletion_time(now_seconds, ttl_seconds),
        };

        // Cell flags - CELL_IS_EXPIRING, NO USE_ROW_TIMESTAMP or USE_ROW_TTL
        let mut flags = CELL_IS_EXPIRING;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        // Timestamp delta (required for expiring cells)
        // Fix #644 (S6): Cell timestamp delta is UNSIGNED VInt.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        // Local deletion time delta
        let ldt_delta = (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
        if ldt_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "Local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        encode_unsigned(ldt_delta as u64, buf);

        // TTL delta
        let ttl_delta = (ttl_seconds as i64) - (self.stats.min_ttl as i64);
        if ttl_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "TTL {} is less than min_ttl {}",
                ttl_seconds, self.stats.min_ttl
            )));
        }
        encode_unsigned(ttl_delta as u64, buf);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        // Value
        let value_bytes = serialize_value(value)?;

        // Bounds check: value length must fit in i64
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
    }

    pub(super) fn write_cell_with_row_ttl(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        _timestamp: i64,
        _ttl_seconds: u32,
    ) -> Result<()> {
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let mut flags = CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        let value_bytes = serialize_value(value)?;
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        buf.extend_from_slice(&value_bytes);
        Ok(())
    }

    /// Capture the wall-clock "now" (seconds since epoch) ONCE per write
    /// operation — the single fallible clock read for a whole row/mutation
    /// write, mirroring Cassandra's `FBUtilities.nowInSeconds()` (captured
    /// once per mutation apply, not once per cell).
    ///
    /// Issue #2038 Scope B (roborev blocker): `expiring_local_deletion_time`
    /// used to read `SystemTime::now()` on every call, so a multi-element
    /// complex column (or a multi-field UDT) written under one uniform TTL
    /// could get a DIFFERENT `localDeletionTime` per element if the wall
    /// clock ticked mid-write. The read-side `ExpiryHomogeneity` check
    /// requires an EXACT match across all elements to surface `TTL(col)`, so
    /// that skew silently defeated the whole feature. Callers now capture
    /// `now_seconds` ONCE at the top of a row/static-row write and thread it
    /// through every expiring cell of that write (row liveness, complex
    /// column elements, UDT fields).
    pub(super) fn capture_now_seconds(&self) -> Result<i32> {
        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Storage(format!("System time error: {}", e)))?
            .as_secs() as i32;
        Ok(now_seconds)
    }

    /// Derive an expiring cell's `localDeletionTime` from a SHARED
    /// `now_seconds` (see [`Self::capture_now_seconds`]) plus its TTL —
    /// infallible now that the clock read happens exactly once per write,
    /// upstream of every call site.
    pub(super) fn expiring_local_deletion_time(&self, now_seconds: i32, ttl_seconds: u32) -> i32 {
        now_seconds.saturating_add(ttl_seconds as i32)
    }

    /// Write a tombstone cell
    ///
    /// Tombstones require:
    /// - IS_DELETED flag set
    /// - Own timestamp (NOT USE_ROW_TIMESTAMP - tombstones need explicit timestamps)
    /// - local_deletion_time field
    /// - No value data
    pub(super) fn write_tombstone_cell(
        &self,
        buf: &mut Vec<u8>,
        _column: &str,
        timestamp: i64,
        local_deletion_time: i32,
    ) -> Result<()> {
        // Cell flags for tombstone
        // CRITICAL: Do NOT set USE_ROW_TIMESTAMP - tombstones need their own timestamp
        //
        // Issue #716: HAS_EMPTY_VALUE MUST be set. Cassandra's Cell.Serializer
        // derives `hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0`, so a deleted
        // cell without this flag makes Cassandra read a value that was never
        // written, desyncing the row stream (EOFException on readback).
        let flags = CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE;
        buf.push(flags);

        // Timestamp delta (VInt) - required for tombstones
        // Fix #644 (S6): tombstone timestamp delta is UNSIGNED VInt per Cassandra.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        // Local deletion time delta (VUInt) - required for tombstones
        let deletion_time_delta =
            (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
        if deletion_time_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "Local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        encode_unsigned(deletion_time_delta as u64, buf);

        // No value length or value bytes for tombstones
        // Parser returns immediately after reading local_deletion_time
        Ok(())
    }

    /// Write a single range tombstone bound marker.
    ///
    /// On-disk layout (must mirror the reader's `skip_range_tombstone_marker`
    /// and Cassandra's `UnfilteredSerializer.serialize(RangeTombstoneMarker)`):
    /// ```text
    /// [flags: u8]                      ← IS_MARKER (0x02)
    /// [bound_kind: u8]                 ← ClusteringPrefix.Kind ordinal
    /// [cluster_count: u16 BE]          ← bound.size()
    /// [cluster_header: VUInt]          ← only when cluster_count > 0
    /// [cluster_values: ...]
    /// [marker_body_size: VUInt]        ← size of (prev_size + deletion times)
    /// [prev_unfiltered_size: VUInt]
    /// [marked_for_delete_at: VUInt]    ← delta from min_timestamp (µs)
    /// [local_deletion_time: VUInt]     ← delta from min_local_deletion_time (s)
    /// ```
    ///
    /// Issue #717: the previous writer emitted private bound-kind ordinals,
    /// no u16 cluster count, and no marker_body_size/prev_size VInts — bytes
    /// no Cassandra (or CQLite) reader could parse.
    ///
    /// Returns the total serialized marker size (for prev_unfiltered_size
    /// threading).
    pub(super) fn write_range_bound(
        &mut self,
        bound: &ClusteringBound,
        is_open: bool,
        deletion_time: i64,
        local_deletion_time: i32,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        // Marker flag
        self.buffer.push(IS_MARKER);

        // Bound kind (ClusteringPrefix.Kind ordinal) + clustering values.
        // Bottom/Top are the full-partition bounds: an inclusive bound with
        // zero clustering values.
        let (bound_kind, clustering) = match (is_open, bound) {
            (true, ClusteringBound::Inclusive(ck)) => (INCL_START_BOUND, Some(ck)),
            (true, ClusteringBound::Exclusive(ck)) => (EXCL_START_BOUND, Some(ck)),
            (false, ClusteringBound::Inclusive(ck)) => (INCL_END_BOUND, Some(ck)),
            (false, ClusteringBound::Exclusive(ck)) => (EXCL_END_BOUND, Some(ck)),
            (true, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_START_BOUND, None),
            (false, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_END_BOUND, None),
        };
        self.buffer.push(bound_kind);

        // Cluster count (u16 BE) — ClusteringBoundOrBoundary.Serializer
        // writes `out.writeShort(bound.size())` before the values.
        let cluster_count = clustering.map_or(0, |ck| ck.columns.len());
        if cluster_count > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Range tombstone bound has too many clustering values: {}",
                cluster_count
            )));
        }
        self.buffer
            .write_all(&(cluster_count as u16).to_be_bytes())?;

        // Clustering header + values (only when the bound carries values).
        if let Some(ck) = clustering {
            self.write_clustering_prefix(ck, schema)?;
        }

        // Deletion time: Cassandra canonical order (markedForDeleteAt first,
        // then localDeletionTime), both UNSIGNED VInt deltas.
        //
        // Issue #853 / #889: localDeletionTime and minLocalDeletionTime are Java
        // `int`s; Cassandra's DeletionTime.serialize emits
        // `writeUnsignedVInt32(localDeletionTime - minLocalDeletionTime)`, a 32-bit
        // subtraction zero-extended into [0, 2^32). A far-future LDT in [2^31, 2^32)
        // is a negative i32 here; widening to i64 first (the previous code) produced
        // a 64-bit wrapped delta with a different byte length than Cassandra's i32
        // form, corrupting both the bytes and the marker_body_size vint. Reject only
        // a genuine below-baseline ordering violation in normal (non-negative i32)
        // time space; a far-future LDT (negative as i32) is legitimate.
        if local_deletion_time >= 0
            && self.stats.min_local_deletion_time >= 0
            && local_deletion_time < self.stats.min_local_deletion_time
        {
            return Err(Error::InvalidInput(format!(
                "Range tombstone: local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        let mut deletion = Vec::new();
        let ts_delta = (deletion_time - self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, &mut deletion);
        let ldt_delta = local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(ldt_delta as u64, &mut deletion);

        // marker_body_size covers the prev_size VInt + deletion times (same
        // convention as row_size for rows).
        let body_size = unsigned_len(prev_size) as u64 + deletion.len() as u64;
        encode_unsigned(body_size, &mut self.buffer);
        encode_unsigned(prev_size, &mut self.buffer);
        self.buffer.extend_from_slice(&deletion);

        Ok(self.buffer.len() - start_len)
    }

    /// Write a range-tombstone BOUNDARY marker (issue #1220).
    ///
    /// A boundary closes the previous range and opens the next at the SAME
    /// clustering point, so it carries TWO deletion-time pairs. Mirrors
    /// Cassandra's `UnfilteredSerializer.serialize` for a
    /// `RangeTombstoneBoundaryMarker`: it serializes `endDeletionTime()` (the
    /// closing range) THEN `startDeletionTime()` (the opening range), each via
    /// `header.writeDeletionTime` (mfda delta then ldt delta). On-disk layout:
    ///
    /// ```text
    /// [IS_MARKER: 0x02]
    /// [boundary_kind: u8]              ← 2 (EXCL_END_INCL_START) | 5 (INCL_END_EXCL_START)
    /// [cluster_count: u16 BE]
    /// [cluster_header + values]
    /// [marker_body_size: VUInt]        ← size of (prev_size + the TWO deletion pairs)
    /// [prev_unfiltered_size: VUInt]
    /// [end_marked_for_delete_at: VUInt]    ← primary  (close of previous range)
    /// [end_local_deletion_time: VUInt32]
    /// [start_marked_for_delete_at: VUInt]  ← secondary (open of next range)
    /// [start_local_deletion_time: VUInt32]
    /// ```
    ///
    /// Returns the total serialized marker size (for prev_unfiltered_size
    /// threading). A boundary always sits at a concrete clustering value, so
    /// (unlike a bound) it never carries the open-ended `Bottom`/`Top` form.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_range_boundary(
        &mut self,
        boundary_kind: u8,
        clustering: &ClusteringKey,
        end_deletion_time: i64,
        end_local_deletion_time: i32,
        start_deletion_time: i64,
        start_local_deletion_time: i32,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        self.buffer.push(IS_MARKER);
        self.buffer.push(boundary_kind);

        // Cluster count (u16 BE) + clustering header/values.
        let cluster_count = clustering.columns.len();
        if cluster_count > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Range tombstone boundary has too many clustering values: {}",
                cluster_count
            )));
        }
        self.buffer
            .write_all(&(cluster_count as u16).to_be_bytes())?;
        self.write_clustering_prefix(clustering, schema)?;

        // Reject only a genuine below-baseline ordering violation in normal
        // (non-negative i32) time space; a far-future LDT (negative as i32) is
        // legitimate — same contract as `write_range_bound` (issue #853/#889).
        for (which, ldt) in [
            ("end", end_local_deletion_time),
            ("start", start_local_deletion_time),
        ] {
            if ldt >= 0
                && self.stats.min_local_deletion_time >= 0
                && ldt < self.stats.min_local_deletion_time
            {
                return Err(Error::InvalidInput(format!(
                    "Range tombstone boundary: {which} local deletion time {} is less than \
                     min_local_deletion_time {}",
                    ldt, self.stats.min_local_deletion_time
                )));
            }
        }

        // TWO deletion-time pairs, canonical Cassandra order: primary = end
        // (close of previous range), secondary = start (open of next range);
        // within each pair markedForDeleteAt delta first, then localDeletionTime.
        let mut deletion = Vec::new();
        for (dt, ldt) in [
            (end_deletion_time, end_local_deletion_time),
            (start_deletion_time, start_local_deletion_time),
        ] {
            let ts_delta = (dt - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, &mut deletion);
            let ldt_delta = ldt.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, &mut deletion);
        }

        // marker_body_size covers the prev_size VInt + the two deletion pairs.
        let body_size = unsigned_len(prev_size) as u64 + deletion.len() as u64;
        encode_unsigned(body_size, &mut self.buffer);
        encode_unsigned(prev_size, &mut self.buffer);
        self.buffer.extend_from_slice(&deletion);

        Ok(self.buffer.len() - start_len)
    }
}
