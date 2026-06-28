//! Decoder for the BIG ("nb") format promoted index payload (Issue #993).
//!
//! The promoted index lives inside each wide-partition Index.db entry. It lets a
//! reader seek to a clustering-key range inside a partition larger than 64 KiB
//! without scanning the whole partition. The CQLite **writer** is the authoritative
//! oracle for this format (`writer/index_writer.rs::serialize_promoted_index` /
//! `serialize_index_info`); this module mirrors that layout exactly on the read path.
//!
//! Previously the read path DECODED-AWAY (skipped) this payload. This module decodes
//! it instead, so wide-partition boundary metadata round-trips.
//!
//! # On-disk layout (BIG "nb" — `RowIndexEntry.IndexedEntry.serialize()`)
//!
//! ```text
//! [headerLength: unsigned VInt]    ← Data.db byte offset from partition start to first row
//! [DeletionTime: 12 bytes]         ← NB legacy form: [localDeletionTime: i32 BE][markedForDeleteAt: i64 BE]
//! [count: unsigned VInt]           ← number of IndexInfo blocks
//! [IndexInfo[0]..IndexInfo[N-1]]   ← serialized blocks, in order
//! [offset[0]: i32 BE] ...          ← relative offsets from first IndexInfo start, one per block
//! ```
//!
//! Each `IndexInfo` block:
//! ```text
//! [firstName: ClusteringPrefix]    ← min clustering key in block (header VInt + value bytes)
//! [lastName: ClusteringPrefix]     ← max clustering key in block
//! [offset: unsigned VInt]          ← byte offset from partition start
//! [width: signed VInt]             ← (actual_width - WIDTH_BASE), zigzag-encoded
//! [endOpenMarker: bool byte]       ← 0x00 = none; 0x01 = a DeletionTime (12 bytes) follows
//! ```
//!
//! # Format ambiguity resolved against the oracle (Issue #993)
//!
//! `firstName` / `lastName` are serialized `ClusteringPrefix` byte sequences with the
//! shape `[header VInt][value bytes…]`. Fixed-width clustering values carry **no**
//! per-value length prefix, so a `ClusteringPrefix` is only self-delimiting when the
//! decoder knows the clustering column types (the table's serialization header).
//! Cassandra's `IndexInfo.Serializer.deserialize()` likewise threads a
//! `ClusteringComparator` + `SerializationHeader` to split the names.
//!
//! Mirroring the writer, this decoder therefore takes a caller-supplied
//! [`PrefixLen`] callback that returns the byte length of one serialized
//! `ClusteringPrefix` at the start of a slice (authoritative, schema-driven — no
//! heuristics, per Issue #28). The width delta is decoded as an unsigned VInt then
//! zigzag-decoded, exactly inverting the writer's `encode_signed` (zigzag →
//! `encode_unsigned`).
//!
//! Sources:
//! - `writer/index_writer.rs` (the oracle: `serialize_promoted_index`, `serialize_index_info`)
//! - Cassandra `RowIndexEntry.IndexedEntry.serialize()` / `IndexInfo.Serializer`
//! - `DeletionTime.LegacySerializer` (NB form: ldt i32 BE, then mfda i64 BE)

use crate::error::{Error, Result};
use crate::parser::vint::{parse_vuint, zigzag_decode};

/// Byte size of an NB (legacy) `DeletionTime`: i32 BE localDeletionTime + i64 BE markedForDeleteAt.
const NB_DELETION_TIME_SIZE: usize = 12;

/// Delta base for `IndexInfo.width` (Cassandra `IndexInfo.WIDTH_BASE` = 64 KiB).
const INDEX_INFO_WIDTH_BASE: i64 = 64 * 1024;

/// Decoded partition-level `DeletionTime` (NB legacy encoding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotedDeletionTime {
    /// `localDeletionTime` (seconds). `i32::MAX` == LIVE (no deletion).
    pub local_deletion_time: i32,
    /// `markedForDeleteAt` (microseconds). `i64::MIN` == LIVE (no deletion).
    pub marked_for_delete_at: i64,
}

impl PromotedDeletionTime {
    /// True when this is the LIVE sentinel (`i32::MAX` / `i64::MIN`).
    pub fn is_live(&self) -> bool {
        self.local_deletion_time == i32::MAX && self.marked_for_delete_at == i64::MIN
    }
}

/// One decoded `IndexInfo` block from a promoted index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedIndexInfo {
    /// Serialized `ClusteringPrefix` bytes for the first unfiltered in this block.
    pub first_name: Vec<u8>,
    /// Serialized `ClusteringPrefix` bytes for the last unfiltered in this block.
    pub last_name: Vec<u8>,
    /// Byte offset from the partition start to this block's first unfiltered.
    pub offset: u64,
    /// Total width (bytes) of this block's data (delta already added back to `WIDTH_BASE`).
    pub width: u64,
    /// Open range-tombstone marker at the block boundary, if present:
    /// `(local_deletion_time, marked_for_delete_at)`.
    pub end_open_marker: Option<(i32, i64)>,
}

/// A fully decoded promoted index payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPromotedIndex {
    /// Data.db byte offset from partition start to the first row (`headerLength`).
    pub header_length: u64,
    /// Partition-level deletion time.
    pub deletion_time: PromotedDeletionTime,
    /// Number of IndexInfo blocks (== `entries.len()`).
    pub count: u32,
    /// Decoded IndexInfo blocks, in order.
    pub entries: Vec<DecodedIndexInfo>,
    /// Trailing offsets array: relative offset (bytes) from the first IndexInfo start
    /// to each block's start. One entry per block.
    pub offsets: Vec<i32>,
}

/// Callback returning the byte length of one serialized `ClusteringPrefix` at the
/// start of `slice`.
///
/// Supplied by the caller from authoritative schema (clustering column types) — there
/// is deliberately no heuristic fallback (Issue #28). Returns `Err` if the slice does
/// not contain a complete prefix.
pub type PrefixLen<'a> = dyn Fn(&[u8]) -> Result<usize> + 'a;

/// Helper: read a big-endian `i32` from the front of `input`, returning the rest.
fn take_be_i32(input: &[u8]) -> Result<(&[u8], i32)> {
    if input.len() < 4 {
        return Err(Error::Corruption(
            "promoted index: truncated i32 (need 4 bytes)".to_string(),
        ));
    }
    let (head, rest) = input.split_at(4);
    let v = i32::from_be_bytes([head[0], head[1], head[2], head[3]]);
    Ok((rest, v))
}

/// Helper: read a big-endian `i64` from the front of `input`, returning the rest.
fn take_be_i64(input: &[u8]) -> Result<(&[u8], i64)> {
    if input.len() < 8 {
        return Err(Error::Corruption(
            "promoted index: truncated i64 (need 8 bytes)".to_string(),
        ));
    }
    let (head, rest) = input.split_at(8);
    let mut buf = [0u8; 8];
    buf.copy_from_slice(head);
    Ok((rest, i64::from_be_bytes(buf)))
}

/// Decode one NB legacy `DeletionTime` (12 bytes).
fn decode_deletion_time(input: &[u8]) -> Result<(&[u8], PromotedDeletionTime)> {
    let (input, ldt) = take_be_i32(input)?;
    let (input, mfda) = take_be_i64(input)?;
    Ok((
        input,
        PromotedDeletionTime {
            local_deletion_time: ldt,
            marked_for_delete_at: mfda,
        },
    ))
}

/// Decode an unsigned VInt, mapping nom errors to a defensive `Error::Corruption`.
fn decode_vuint(input: &[u8], what: &str) -> Result<(usize, u64)> {
    match parse_vuint(input) {
        Ok((rest, value)) => Ok((input.len() - rest.len(), value)),
        Err(_) => Err(Error::Corruption(format!(
            "promoted index: truncated/invalid VInt while reading {what}"
        ))),
    }
}

/// Decode a single `IndexInfo` block from the front of `input`.
///
/// `prefix_len` splits `firstName`/`lastName` using authoritative schema.
fn decode_index_info<'a>(
    input: &'a [u8],
    prefix_len: &PrefixLen<'_>,
) -> Result<(&'a [u8], DecodedIndexInfo)> {
    // firstName ClusteringPrefix
    let first_len = prefix_len(input)?;
    if input.len() < first_len {
        return Err(Error::Corruption(
            "promoted index: firstName clustering prefix exceeds block".to_string(),
        ));
    }
    let (first_name, input) = input.split_at(first_len);

    // lastName ClusteringPrefix
    let last_len = prefix_len(input)?;
    if input.len() < last_len {
        return Err(Error::Corruption(
            "promoted index: lastName clustering prefix exceeds block".to_string(),
        ));
    }
    let (last_name, input) = input.split_at(last_len);

    // offset (unsigned VInt)
    let (consumed, offset) = decode_vuint(input, "IndexInfo.offset")?;
    let input = &input[consumed..];

    // width delta (signed VInt = zigzag) → actual width
    let (consumed, width_delta_u) = decode_vuint(input, "IndexInfo.width")?;
    let input = &input[consumed..];
    let width = INDEX_INFO_WIDTH_BASE
        .checked_add(zigzag_decode(width_delta_u))
        .filter(|w| *w >= 0)
        .ok_or_else(|| {
            Error::Corruption("promoted index: IndexInfo.width out of range".to_string())
        })? as u64;

    // endOpenMarker presence byte, optionally followed by a DeletionTime.
    let (&marker_byte, input) = input
        .split_first()
        .ok_or_else(|| Error::Corruption("promoted index: missing endOpenMarker".to_string()))?;
    let (input, end_open_marker) = match marker_byte {
        0 => (input, None),
        1 => {
            let (rest, dt) = decode_deletion_time(input)?;
            (
                rest,
                Some((dt.local_deletion_time, dt.marked_for_delete_at)),
            )
        }
        other => {
            return Err(Error::Corruption(format!(
                "promoted index: invalid endOpenMarker byte {other:#x} (expected 0 or 1)"
            )))
        }
    };

    Ok((
        input,
        DecodedIndexInfo {
            first_name: first_name.to_vec(),
            last_name: last_name.to_vec(),
            offset,
            width,
            end_open_marker,
        },
    ))
}

/// Decode a complete promoted index payload (the bytes that follow the
/// `promoted_index_size` VInt in a BIG Index.db entry).
///
/// `prefix_len` returns the byte length of one serialized `ClusteringPrefix`,
/// derived from authoritative schema (clustering column types). It is required
/// because `firstName`/`lastName` are not self-delimiting without type info.
///
/// Returns `Err` (never panics) on any truncation or inconsistency.
pub fn decode_promoted_index(
    payload: &[u8],
    prefix_len: &PrefixLen<'_>,
) -> Result<DecodedPromotedIndex> {
    // headerLength (unsigned VInt)
    let (consumed, header_length) = decode_vuint(payload, "headerLength")?;
    let input = &payload[consumed..];

    // DeletionTime (12 bytes)
    if input.len() < NB_DELETION_TIME_SIZE {
        return Err(Error::Corruption(
            "promoted index: truncated partition DeletionTime".to_string(),
        ));
    }
    let (input, deletion_time) = decode_deletion_time(input)?;

    // count (unsigned VInt)
    let (consumed, count_u64) = decode_vuint(input, "count")?;
    let input = &input[consumed..];
    let count = u32::try_from(count_u64)
        .map_err(|_| Error::Corruption("promoted index: count too large".to_string()))?;

    // Bound `count` against the remaining payload BEFORE allocating: `count` is
    // read from untrusted on-disk bytes, so a corrupt Index.db could declare a
    // huge value and trigger a multi-hundred-GB `Vec::with_capacity` that aborts
    // the process via `handle_alloc_error` (it would never reach the loop that
    // returns Err). Every IndexInfo block consumes at least 1 byte for its
    // payload AND contributes a 4-byte trailing offset entry, so a payload with
    // `remaining` bytes can hold at most `remaining` blocks. `count > remaining`
    // is therefore provably corrupt — fail closed instead of allocating. This
    // upholds the module's "Returns Err (never panics)" guarantee for untrusted
    // input.
    if count as usize > input.len() {
        return Err(Error::Corruption(format!(
            "promoted index: declared block count {count} exceeds remaining payload \
             length {} bytes (corrupt: each block needs >= 1 byte plus a 4-byte \
             trailing offset entry)",
            input.len()
        )));
    }

    // IndexInfo[count] blocks.
    let mut entries = Vec::with_capacity(count as usize);
    let mut info = input;
    for i in 0..count {
        let (rest, entry) = decode_index_info(info, prefix_len)
            .map_err(|e| Error::Corruption(format!("promoted index: IndexInfo block {i}: {e}")))?;
        debug_assert!(rest.len() < info.len(), "IndexInfo decode must advance");
        entries.push(entry);
        info = rest;
    }

    // Trailing offsets array: one i32 BE per block.
    let mut offsets = Vec::with_capacity(count as usize);
    let mut rest = info;
    for i in 0..count {
        let (next, off) = take_be_i32(rest)
            .map_err(|e| Error::Corruption(format!("promoted index: offsets[{i}]: {e}")))?;
        offsets.push(off);
        rest = next;
    }

    if !rest.is_empty() {
        return Err(Error::Corruption(format!(
            "promoted index: {} trailing bytes after offsets array",
            rest.len()
        )));
    }

    Ok(DecodedPromotedIndex {
        header_length,
        deletion_time,
        count,
        entries,
        offsets,
    })
}

/// Read just the IndexInfo block `count` from a promoted-index payload without
/// decoding the (schema-dependent) `firstName`/`lastName` prefixes.
///
/// The leading `headerLength` (VInt), `DeletionTime` (12 bytes) and `count` (VInt)
/// are schema-free, so the count is always recoverable. Returns `Err` on a
/// truncated/invalid header (never panics).
pub fn peek_block_count(payload: &[u8]) -> Result<u32> {
    let (consumed, _header_length) = decode_vuint(payload, "headerLength")?;
    let input = &payload[consumed..];
    if input.len() < NB_DELETION_TIME_SIZE {
        return Err(Error::Corruption(
            "promoted index: truncated DeletionTime in peek_block_count".to_string(),
        ));
    }
    let input = &input[NB_DELETION_TIME_SIZE..];
    let (consumed, count_u64) = decode_vuint(input, "count")?;
    let count = u32::try_from(count_u64)
        .map_err(|_| Error::Corruption("promoted index: count too large".to_string()))?;

    // Apply the SAME untrusted-`count` sanity bound as `decode_promoted_index`: the
    // count is read straight from on-disk Index.db bytes, so a corrupt payload could
    // declare an implausible value. Every IndexInfo block consumes at least 1 byte of
    // body plus a 4-byte trailing offset, so the remaining payload can hold at most
    // `remaining` blocks. Surfacing a bogus `count` here would otherwise leak into
    // `block_count()` callers (stats `total_promoted_entries`, the parity validator)
    // even though `decode_promoted_index` would reject the same payload. Fail closed
    // to keep the trust posture consistent (still never panics).
    let remaining = &input[consumed..];
    if count as usize > remaining.len() {
        return Err(Error::Corruption(format!(
            "promoted index: declared block count {count} exceeds remaining payload \
             length {} bytes (corrupt: each block needs >= 1 byte plus a 4-byte \
             trailing offset entry)",
            remaining.len()
        )));
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::vint::encode_vuint;

    /// Test-local unsigned VInt encoder (ungated; the production `encode_unsigned`
    /// lives behind the `write-support` feature). `encode_vuint` is the inverse of
    /// `parse_vuint`, which is exactly what the decoder uses.
    fn encode_unsigned(value: u64, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&encode_vuint(value));
    }

    /// Test-local signed VInt encoder: zigzag, then unsigned VInt — byte-identical
    /// to the writer's `encode_signed`, but without the write-support gate.
    fn encode_signed(value: i64, buf: &mut Vec<u8>) {
        let zz = ((value << 1) ^ (value >> 63)) as u64;
        encode_unsigned(zz, buf);
    }

    /// A prefix-length callback that mirrors the test fixtures' clustering prefix
    /// encoding: a 1-byte header VInt (`0x00` = single present column / no columns),
    /// followed by a fixed number of value bytes.
    ///
    /// `value_bytes` is the count of value bytes following the header byte. This is
    /// the authoritative, schema-derived length the production caller would compute.
    fn fixed_prefix_len(value_bytes: usize) -> impl Fn(&[u8]) -> Result<usize> {
        move |slice: &[u8]| {
            let need = 1 + value_bytes;
            if slice.len() < need {
                return Err(Error::Corruption(
                    "test prefix_len: slice shorter than prefix".to_string(),
                ));
            }
            Ok(need)
        }
    }

    fn ck(value_bytes: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00u8]; // header VInt: all present
        v.extend_from_slice(value_bytes);
        v
    }

    /// Hand-build a promoted-index payload (no writer dependency) so the decoder can
    /// be exercised in the minimal feature set. Mirrors `serialize_promoted_index`.
    /// Each block tuple = (first_name, last_name, offset, width, end_open_marker).
    #[allow(clippy::type_complexity)]
    fn build_payload(
        header_length: u64,
        blocks: &[(Vec<u8>, Vec<u8>, u64, u64, Option<(i32, i64)>)],
    ) -> Vec<u8> {
        let mut info: Vec<u8> = Vec::new();
        let mut starts: Vec<u32> = Vec::new();
        for (first, last, offset, width, marker) in blocks {
            starts.push(info.len() as u32);
            info.extend_from_slice(first);
            info.extend_from_slice(last);
            encode_unsigned(*offset, &mut info);
            encode_signed((*width as i64) - (64 * 1024), &mut info);
            match marker {
                None => info.push(0x00),
                Some((ldt, mfda)) => {
                    info.push(0x01);
                    info.extend_from_slice(&ldt.to_be_bytes());
                    info.extend_from_slice(&mfda.to_be_bytes());
                }
            }
        }
        let mut payload = Vec::new();
        encode_unsigned(header_length, &mut payload);
        payload.extend_from_slice(&i32::MAX.to_be_bytes()); // LIVE ldt
        payload.extend_from_slice(&i64::MIN.to_be_bytes()); // LIVE mfda
        encode_unsigned(blocks.len() as u64, &mut payload);
        payload.extend_from_slice(&info);
        for s in &starts {
            payload.extend_from_slice(&(*s as i32).to_be_bytes());
        }
        payload
    }

    #[test]
    fn test_zigzag_decode_inverts_encode_signed() {
        for v in [-1_000_000i64, -65, -1, 0, 1, 63, 64, 1_000_000] {
            let mut buf = Vec::new();
            encode_signed(v, &mut buf);
            let (_, u) = decode_vuint(&buf, "test").unwrap();
            assert_eq!(
                zigzag_decode(u),
                v,
                "zigzag_decode must invert encode_signed"
            );
        }
    }

    #[test]
    fn test_decode_handbuilt_two_blocks_with_end_open_marker() {
        // Block 1: width BELOW base (negative delta), no marker.
        // Block 2: width ABOVE base (positive delta), end-open-marker deletion.
        let blocks = vec![
            (ck(b"aa"), ck(b"am"), 0u64, 50_000u64, None),
            (
                ck(b"an"),
                ck(b"zz"),
                70_000u64,
                100_000u64,
                Some((123, -456)),
            ),
        ];
        let payload = build_payload(18, &blocks);
        let decoded = decode_promoted_index(&payload, &fixed_prefix_len(2)).unwrap();

        assert_eq!(decoded.header_length, 18);
        assert!(decoded.deletion_time.is_live());
        assert_eq!(decoded.count, 2);

        assert_eq!(decoded.entries[0].first_name, ck(b"aa"));
        assert_eq!(decoded.entries[0].last_name, ck(b"am"));
        assert_eq!(decoded.entries[0].offset, 0);
        assert_eq!(decoded.entries[0].width, 50_000);
        assert_eq!(decoded.entries[0].end_open_marker, None);

        assert_eq!(decoded.entries[1].first_name, ck(b"an"));
        assert_eq!(decoded.entries[1].last_name, ck(b"zz"));
        assert_eq!(decoded.entries[1].offset, 70_000);
        assert_eq!(decoded.entries[1].width, 100_000);
        assert_eq!(decoded.entries[1].end_open_marker, Some((123, -456)));

        assert_eq!(decoded.offsets.len(), 2);
        assert_eq!(decoded.offsets[0], 0);
        assert!(decoded.offsets[1] > decoded.offsets[0]);
        // Monotonic offsets within the partition.
        assert!(decoded.entries[1].offset > decoded.entries[0].offset);
    }

    #[test]
    fn test_decode_handbuilt_width_exactly_base_zero_delta() {
        let blocks = vec![
            (ck(b""), ck(b""), 0u64, 64 * 1024u64, None),
            (ck(b""), ck(b""), 64 * 1024u64, 64 * 1024u64, None),
        ];
        let payload = build_payload(18, &blocks);
        let decoded = decode_promoted_index(&payload, &fixed_prefix_len(0)).unwrap();
        assert_eq!(decoded.entries[0].width, 64 * 1024);
        assert_eq!(decoded.entries[1].width, 64 * 1024);
    }

    #[test]
    fn test_truncated_payload_returns_err_not_panic() {
        let blocks = vec![
            (ck(b"aa"), ck(b"bb"), 0u64, 70_000u64, None),
            (ck(b"cc"), ck(b"dd"), 70_000u64, 70_000u64, None),
        ];
        let payload = build_payload(18, &blocks);

        // Truncate at every length and assert no panic + Err for short buffers.
        for cut in 0..payload.len() {
            let res = decode_promoted_index(&payload[..cut], &fixed_prefix_len(2));
            assert!(
                res.is_err(),
                "truncated payload (len {cut}) must return Err, not Ok"
            );
        }
        // Full payload still decodes.
        assert!(decode_promoted_index(&payload, &fixed_prefix_len(2)).is_ok());
    }

    #[test]
    fn test_huge_count_short_body_returns_err_no_alloc_abort() {
        // A corrupt payload that declares a gigantic IndexInfo block count but
        // carries almost no body. Without the pre-allocation bound this would do
        // `Vec::with_capacity(~4 billion)` and abort the process via
        // handle_alloc_error before ever reaching the decode loop. With the bound
        // it must return Err (never panic/abort).
        let mut payload = Vec::new();
        encode_unsigned(18, &mut payload); // headerLength
        payload.extend_from_slice(&i32::MAX.to_be_bytes()); // LIVE ldt
        payload.extend_from_slice(&i64::MIN.to_be_bytes()); // LIVE mfda
        encode_unsigned(u32::MAX as u64, &mut payload); // count = ~4 billion (untrusted)
        payload.extend_from_slice(&[0x00, 0x01, 0x02]); // tiny body, far short of count

        let res = decode_promoted_index(&payload, &fixed_prefix_len(0));
        assert!(
            res.is_err(),
            "huge declared count with a short body must return Err (no alloc abort)"
        );
        match res {
            Err(Error::Corruption(msg)) => assert!(
                msg.contains("exceeds remaining payload"),
                "expected bound-check corruption message, got: {msg}"
            ),
            other => panic!("expected Error::Corruption, got {other:?}"),
        }
    }

    #[test]
    fn test_count_exactly_at_payload_bound_then_short_returns_err() {
        // count == remaining payload length passes the cheap bound check (each
        // block could in principle be 1 byte), but the body is still too short to
        // decode that many blocks → must Err in the decode loop, not panic.
        let mut payload = Vec::new();
        encode_unsigned(18, &mut payload);
        payload.extend_from_slice(&i32::MAX.to_be_bytes());
        payload.extend_from_slice(&i64::MIN.to_be_bytes());
        let body = [0x00u8; 4];
        encode_unsigned(body.len() as u64, &mut payload); // count == remaining len
        payload.extend_from_slice(&body);
        let res = decode_promoted_index(&payload, &fixed_prefix_len(0));
        assert!(
            res.is_err(),
            "count == remaining but unfillable body must Err"
        );
    }

    #[test]
    fn test_peek_block_count_schema_free() {
        let blocks = vec![
            (ck(b"aa"), ck(b"bb"), 0u64, 70_000u64, None),
            (ck(b"cc"), ck(b"dd"), 70_000u64, 70_000u64, None),
        ];
        let payload = build_payload(18, &blocks);
        // peek does not need the prefix_len callback.
        assert_eq!(peek_block_count(&payload).unwrap(), 2);
        // Truncated header → Err, not panic.
        assert!(peek_block_count(&payload[..2]).is_err());
    }

    #[test]
    fn test_peek_block_count_huge_count_short_body_returns_err() {
        // A corrupt payload declaring a gigantic block count but carrying almost no
        // body. peek_block_count must apply the same "count <= remaining payload"
        // bound as decode_promoted_index and return Err (never surface the bogus
        // count to block_count() callers).
        let mut payload = Vec::new();
        encode_unsigned(18, &mut payload); // headerLength
        payload.extend_from_slice(&i32::MAX.to_be_bytes()); // LIVE ldt
        payload.extend_from_slice(&i64::MIN.to_be_bytes()); // LIVE mfda
        encode_unsigned(u32::MAX as u64, &mut payload); // count = ~4 billion (untrusted)
        payload.extend_from_slice(&[0x00, 0x01, 0x02]); // tiny body, far short of count

        let res = peek_block_count(&payload);
        match res {
            Err(Error::Corruption(msg)) => assert!(
                msg.contains("exceeds remaining payload"),
                "expected bound-check corruption message, got: {msg}"
            ),
            other => panic!("expected Error::Corruption, got {other:?}"),
        }
    }

    #[test]
    fn test_trailing_garbage_rejected() {
        let blocks = vec![
            (ck(b"a"), ck(b"b"), 0u64, 70_000u64, None),
            (ck(b"c"), ck(b"d"), 70_000u64, 70_000u64, None),
        ];
        let mut payload = build_payload(18, &blocks);
        payload.push(0xFF); // extra trailing byte
        let res = decode_promoted_index(&payload, &fixed_prefix_len(1));
        assert!(res.is_err(), "trailing bytes must be rejected");
    }

    #[test]
    fn test_invalid_end_open_marker_byte_rejected() {
        // Manually craft a single-block payload with an invalid marker byte (0x02).
        let mut info: Vec<u8> = Vec::new();
        info.extend_from_slice(&ck(b"x")); // first_name
        info.extend_from_slice(&ck(b"y")); // last_name
        encode_unsigned(0, &mut info); // offset
        encode_signed(0, &mut info); // width delta
        info.push(0x02); // INVALID marker byte
        let mut payload = Vec::new();
        encode_unsigned(18, &mut payload);
        payload.extend_from_slice(&i32::MAX.to_be_bytes());
        payload.extend_from_slice(&i64::MIN.to_be_bytes());
        encode_unsigned(1, &mut payload); // count = 1
        payload.extend_from_slice(&info);
        payload.extend_from_slice(&0i32.to_be_bytes()); // offsets[0]
        let res = decode_promoted_index(&payload, &fixed_prefix_len(1));
        assert!(res.is_err(), "invalid endOpenMarker byte must be rejected");
    }

    // ── Writer round-trip (the oracle) — requires the write path ───────────────
    #[cfg(feature = "write-support")]
    mod writer_roundtrip {
        use super::*;
        use crate::storage::sstable::writer::{
            serialize_promoted_index_for_test, PromotedIndexBlock,
        };

        /// Encode with the authoritative writer, decode, assert byte-exact recovery.
        #[test]
        fn test_writer_roundtrip_two_blocks_varied_widths() {
            let block1 = PromotedIndexBlock {
                first_name: ck(b"aa"),
                last_name: ck(b"am"),
                offset: 0,
                width: 50_000, // < 64 KiB → negative width delta
                oss50_separator: None,
            };
            let block2 = PromotedIndexBlock {
                first_name: ck(b"an"),
                last_name: ck(b"zz"),
                offset: 70_000,
                width: 100_000, // > 64 KiB → positive width delta
                oss50_separator: None,
            };
            // raw_key_len = 4 → headerLength = 2 + 4 + 12 = 18
            let payload = serialize_promoted_index_for_test(&[block1.clone(), block2.clone()], 4);
            let decoded = decode_promoted_index(&payload, &fixed_prefix_len(2)).unwrap();

            assert_eq!(decoded.header_length, 18);
            assert!(decoded.deletion_time.is_live());
            assert_eq!(decoded.count, 2);

            assert_eq!(decoded.entries[0].first_name, block1.first_name);
            assert_eq!(decoded.entries[0].last_name, block1.last_name);
            assert_eq!(decoded.entries[0].offset, block1.offset);
            assert_eq!(decoded.entries[0].width, block1.width);
            assert_eq!(decoded.entries[0].end_open_marker, None);

            assert_eq!(decoded.entries[1].first_name, block2.first_name);
            assert_eq!(decoded.entries[1].last_name, block2.last_name);
            assert_eq!(decoded.entries[1].offset, block2.offset);
            assert_eq!(decoded.entries[1].width, block2.width);

            // Offsets monotonic, first is 0.
            assert_eq!(decoded.offsets[0], 0);
            assert!(decoded.offsets[1] > decoded.offsets[0]);
        }

        /// Single-block input → writer emits no promoted index (None); modeled here
        /// as an empty payload that decodes to a count-0 / no-blocks structure when
        /// promoted_len == 0 is handled by the caller (parse_big_index_entry).
        #[test]
        fn test_writer_roundtrip_width_exactly_base() {
            let block1 = PromotedIndexBlock {
                first_name: ck(b""),
                last_name: ck(b""),
                offset: 0,
                width: 64 * 1024,
                oss50_separator: None,
            };
            let block2 = PromotedIndexBlock {
                first_name: ck(b""),
                last_name: ck(b""),
                offset: 64 * 1024,
                width: 64 * 1024,
                oss50_separator: None,
            };
            let payload = serialize_promoted_index_for_test(&[block1, block2], 4);
            let decoded = decode_promoted_index(&payload, &fixed_prefix_len(0)).unwrap();
            assert_eq!(decoded.entries[0].width, 64 * 1024);
            assert_eq!(decoded.entries[1].width, 64 * 1024);
        }
    }
}
