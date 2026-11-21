//! A strict `Statistics.db` layout + checksum check (spec R7), written from Cassandra's reader.
//!
//! The production `StatisticsReader` is deliberately lenient: it degrades a damaged file to
//! whatever it could parse, and the merge then silently re-orders columns and re-bases every
//! timestamp. `diff` must never emit a report computed that way, so each generation is checked
//! against Cassandra's own integrity rules before anything is read:
//!
//! `cassandra-5.0.8` `io/sstable/metadata/MetadataSerializer.java:146-212` (`deserialize`):
//! `[i32 count][crc32]`, then `count` x `[i32 type][i32 offset]` followed by a `crc32` over the
//! count and every pair, then each component's bytes followed by a `crc32` of those bytes; a
//! component's length is the next offset minus its own (the last one runs to end of file). Every
//! checksum mismatch is a `CorruptSSTableException` (`:214-227`). `hasMetadataChecksum()` is true
//! for every in-scope format (BIG `na`+ at `format/big/BigFormat.java:404`; BTI `da`).

/// Why a `Statistics.db` failed the strict check.
pub(super) fn validate_metadata_layout(bytes: &[u8]) -> Result<(), String> {
    let int_at = |at: usize| -> Result<i32, String> {
        bytes
            .get(at..at + 4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| format!("truncated: needs 4 bytes at offset {at}, file is {} bytes", bytes.len()))
    };
    let mut crc = crc32fast::Hasher::new();

    let count = int_at(0)?;
    crc.update(&count.to_be_bytes());
    let expected = int_at(4)?;
    let actual = crc.clone().finalize() as i32;
    if actual != expected {
        return Err(format!("TOC count checksum mismatch (stored {expected:#x}, computed {actual:#x})"));
    }
    if count < 1 {
        return Err(format!("TOC declares {count} components"));
    }
    let count = usize::try_from(count).map_err(|_| "TOC count overflows".to_string())?;
    // 4 (count) + 4 (crc) + 8 per pair + 4 (crc): reject a count the file cannot hold before
    // looking at any pair.
    let toc_end = count
        .checked_mul(8)
        .and_then(|pairs| pairs.checked_add(12))
        .ok_or_else(|| "TOC size overflows".to_string())?;
    if toc_end > bytes.len() {
        return Err(format!(
            "truncated: TOC of {count} components needs {toc_end} bytes, file is {} bytes",
            bytes.len()
        ));
    }
    let mut offsets = Vec::with_capacity(count);
    for i in 0..count {
        let at = 8 + 8 * i;
        crc.update(&int_at(at)?.to_be_bytes());
        let offset = int_at(at + 4)?;
        crc.update(&offset.to_be_bytes());
        offsets.push(usize::try_from(offset).map_err(|_| format!("component {i} has a negative offset"))?);
    }
    let expected = int_at(8 + 8 * count)?;
    let actual = crc.finalize() as i32;
    if actual != expected {
        return Err(format!("TOC checksum mismatch (stored {expected:#x}, computed {actual:#x})"));
    }

    for (i, &start) in offsets.iter().enumerate() {
        let end = offsets.get(i + 1).copied().unwrap_or(bytes.len());
        if start < toc_end || end < start + 4 || end > bytes.len() {
            return Err(format!(
                "component {i} spans {start}..{end}, outside the {} byte file or shorter than its checksum",
                bytes.len()
            ));
        }
        let body = &bytes[start..end - 4];
        let expected = int_at(end - 4)?;
        let actual = crc32fast::hash(body) as i32;
        if actual != expected {
            return Err(format!(
                "component {i} checksum mismatch (stored {expected:#x}, computed {actual:#x})"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_metadata_layout;

    /// Build a file the way `MetadataSerializer.serialize` (`:67-112`) lays it out.
    fn build(components: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        let count = components.len() as i32;
        out.extend(count.to_be_bytes());
        let mut crc = crc32fast::Hasher::new();
        crc.update(&count.to_be_bytes());
        out.extend((crc.clone().finalize() as i32).to_be_bytes());
        let mut position = 4 + 8 * components.len() + 8;
        for (ordinal, body) in components.iter().enumerate() {
            out.extend((ordinal as i32).to_be_bytes());
            crc.update(&(ordinal as i32).to_be_bytes());
            out.extend((position as i32).to_be_bytes());
            crc.update(&(position as i32).to_be_bytes());
            position += body.len() + 4;
        }
        out.extend((crc.finalize() as i32).to_be_bytes());
        for body in components {
            out.extend(*body);
            out.extend((crc32fast::hash(body) as i32).to_be_bytes());
        }
        out
    }

    #[test]
    fn a_well_formed_file_passes() {
        assert_eq!(validate_metadata_layout(&build(&[b"abc", b"defgh", b"i"])), Ok(()));
    }

    #[test]
    fn truncation_at_every_length_is_refused() {
        let file = build(&[b"abc", b"defgh", b"i"]);
        for len in 0..file.len() {
            assert!(validate_metadata_layout(&file[..len]).is_err(), "len {len}");
        }
    }

    #[test]
    fn any_single_bit_flip_is_refused() {
        let file = build(&[b"abc", b"defgh", b"i"]);
        for byte in 0..file.len() {
            let mut damaged = file.clone();
            damaged[byte] ^= 0x01;
            assert!(validate_metadata_layout(&damaged).is_err(), "byte {byte}");
        }
    }
}
