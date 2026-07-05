//! Authoritative zstd frame-header parsing (issue #1414).
//!
//! CQLite ships **no-dictionary** zstd only. A zstd **dictionary**-compressed
//! chunk is a valid but UNSUPPORTED feature — not corruption — and must be
//! rejected fail-closed BEFORE a plain decode is attempted. Detection is done by
//! parsing the frame header per the zstd frame format (RFC 8878 §3.1), never by
//! guessing from byte patterns (no-heuristics mandate, issue #28).

use super::compression_info::CompressionInfo;

/// The zstd frame magic number as it appears on the wire (little-endian of
/// `0xFD2FB528`). Every zstd frame begins with these four bytes.
pub(crate) const ZSTD_MAGIC_LE: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Detect an unsupported zstd dictionary and return the typed rejection message,
/// or `None` when no dictionary is present (the normal decode path proceeds).
///
/// Checks AUTHORITATIVE metadata first (a dictionary option in
/// `CompressionInfo.db`), then the frame header (a nonzero `Dictionary_ID`). Both
/// are authoritative sources — never a byte-pattern guess (issue #1414, #28). The
/// caller wraps the message in `Error::UnsupportedFormat`, failing closed BEFORE
/// attempting a plain decode.
pub(crate) fn zstd_dictionary_rejection(
    info: &CompressionInfo,
    compressed_data: &[u8],
    chunk_index: usize,
) -> Option<String> {
    let chunk_offset = info.chunk_offsets.get(chunk_index).copied().unwrap_or(0);
    if let Some(option) = info.zstd_dictionary_option() {
        return Some(format!(
            "zstd dictionary compression (CompressionInfo.db option '{option}') is unsupported \
             for chunk {chunk_index} at offset 0x{chunk_offset:x}"
        ));
    }
    if let Some(dict_id) = zstd_frame_dictionary_id(compressed_data) {
        return Some(format!(
            "zstd dictionary compression (Dictionary_ID={dict_id}) is unsupported for chunk \
             {chunk_index} at offset 0x{chunk_offset:x}"
        ));
    }
    None
}

/// Parse the AUTHORITATIVE zstd frame header to extract a nonzero `Dictionary_ID`.
///
/// Per the zstd frame format (RFC 8878 §3.1): a frame starts with the 4-byte
/// magic `0xFD2FB528` (little-endian on disk), then a 1-byte
/// `Frame_Header_Descriptor` whose low two bits are the `Dictionary_ID_flag`
/// (values 0/1/2/3 map to 0/1/2/4 `Dictionary_ID` bytes) and whose bit 5 is the
/// `Single_Segment_flag` (a 1-byte `Window_Descriptor` follows the descriptor
/// iff that flag is 0). The `Dictionary_ID`, when present, is a little-endian
/// unsigned integer immediately after the (optional) window descriptor.
///
/// Returns `Some(id)` only for a well-formed frame carrying a NONZERO dictionary
/// ID — authoritative evidence of dictionary compression, which CQLite does not
/// support (issue #1414). Returns `None` for a plain (no-dictionary) frame, a
/// non-zstd byte run, or a header too short to decide; the normal decode path
/// then handles those. This is pure frame-header parsing per the spec — NOT a
/// byte-pattern heuristic (no-heuristics mandate, issue #28).
pub(crate) fn zstd_frame_dictionary_id(frame: &[u8]) -> Option<u32> {
    if frame.len() < 5 || frame[0..4] != ZSTD_MAGIC_LE {
        return None;
    }
    let frame_header_descriptor = frame[4];
    // Low two bits: Dictionary_ID_flag → 0/1/2/4 ID bytes.
    let did_size = match frame_header_descriptor & 0x03 {
        0 => return None, // no Dictionary_ID field present
        1 => 1usize,
        2 => 2usize,
        _ => 4usize, // flag value 3
    };
    // Bit 5: Single_Segment_flag. A Window_Descriptor byte follows the descriptor
    // iff this flag is 0.
    let single_segment = (frame_header_descriptor & 0x20) != 0;
    let did_start = 5 + usize::from(!single_segment);
    let did_end = did_start.saturating_add(did_size);
    if frame.len() < did_end {
        return None;
    }
    let mut id: u32 = 0;
    for (i, &b) in frame[did_start..did_end].iter().enumerate() {
        id |= u32::from(b) << (8 * i);
    }
    (id != 0).then_some(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_frame_dictionary_id_parses_authoritatively() {
        // Magic 0xFD2FB528 (LE on disk): 28 B5 2F FD.
        const MAGIC: [u8; 4] = ZSTD_MAGIC_LE;

        // Not a zstd frame / too short → None.
        assert_eq!(zstd_frame_dictionary_id(&[]), None);
        assert_eq!(
            zstd_frame_dictionary_id(&[0x00, 0x01, 0x02, 0x03, 0x04]),
            None
        );

        // Dictionary_ID_flag = 0 → no ID field → None (plain frame).
        // FHD 0x20 sets Single_Segment_flag (no window byte), flag bits 0.
        assert_eq!(
            zstd_frame_dictionary_id(&[MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x20]),
            None
        );

        // 1-byte Dictionary_ID, single-segment (no window descriptor):
        // FHD = 0x21 → Single_Segment=1, DID flag=1 (1 ID byte). ID byte = 0x2A.
        let frame_1b = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x21, 0x2A];
        assert_eq!(zstd_frame_dictionary_id(&frame_1b), Some(0x2A));

        // 4-byte Dictionary_ID, single-segment: FHD = 0x23 → DID flag=3 (4 bytes).
        // LE 0x04 0x03 0x02 0x01 → 0x01020304.
        let frame_4b = [
            MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x23, 0x04, 0x03, 0x02, 0x01,
        ];
        assert_eq!(zstd_frame_dictionary_id(&frame_4b), Some(0x0102_0304));

        // NOT single-segment: FHD = 0x01 → DID flag=1 (1 byte) AND a
        // Window_Descriptor byte precedes the ID. window=0x00, ID=0x2A.
        let frame_win = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x01, 0x00, 0x2A];
        assert_eq!(zstd_frame_dictionary_id(&frame_win), Some(0x2A));

        // Header truncated before the ID bytes → None (defer to the decode path).
        let truncated = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x23, 0x04];
        assert_eq!(zstd_frame_dictionary_id(&truncated), None);

        // Explicit zero Dictionary_ID → None (a present-but-zero ID is no dict).
        let frame_zero = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], 0x21, 0x00];
        assert_eq!(zstd_frame_dictionary_id(&frame_zero), None);
    }
}
