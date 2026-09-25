//! Chunk-CRC pre-flight + partition-boundary enumeration for rebuild (design
//! D1; spec R5).
//!
//! Rebuild has NO authoritative boundary source of its own to trust — the
//! whole point of the tool is that `Index.db` / the BTI `Partitions.db` trie
//! may be exactly what is missing or damaged. Boundaries are therefore
//! derived by walking `Data.db` STRUCTURALLY via
//! [`SSTableReader::distinct_partition_keys_with_positions`] — the same
//! decompression-transparent, partition-granular walk `verify`'s BTI
//! cross-check already relies on — never by scanning for a plausible header
//! (spec R2.3; `scripts/tests/test_rebuild_no_resync_scan.sh` enforces the
//! absence of any byte-pattern search primitive in this directory outside
//! tests).

use super::{Refusal, RefusalReason};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::chunk_reader::ChunkReader;
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::SSTableReader;
use std::path::Path;

fn data_corrupt(detail: impl std::fmt::Display, offset: Option<u64>) -> Refusal {
    Refusal {
        reason: RefusalReason::DataCorrupt,
        remedy: format!(
            "Data.db cannot be trusted as the source of truth: {detail} — remedy: cqlite \
             salvage (issue #4196) to recover a fresh generation from what is still decodable"
        ),
        offset,
    }
}

/// Validate every compressed chunk's inline CRC32 BEFORE any structural walk
/// (design D1/R5). Walks every chunk (unlike the read path's fail-fast
/// decompression, which may tolerate a chunk it never actually needs to
/// decompress) so a corrupt chunk is caught even when the partition walk
/// below would not otherwise touch it.
///
/// Uncompressed input has no equivalent PRE-flight here — its chunk-CRC
/// signal (`CRC.db`) may be exactly the component being regenerated, so an
/// uncompressed input's corruption surfaces only as a structural decode
/// failure during the partition walk itself, still refused by the same
/// [`RefusalReason::DataCorrupt`] path (see [`enumerate_partitions`]'s
/// caller in `components.rs`).
pub(super) fn compressed_chunk_preflight(
    data_path: &Path,
    info: &CompressionInfo,
) -> std::result::Result<(), Refusal> {
    let file = std::fs::File::open(data_path)
        .map_err(|e| data_corrupt(format!("cannot open Data.db: {e}"), None))?;
    let total_size = file
        .metadata()
        .map_err(|e| data_corrupt(format!("cannot stat Data.db: {e}"), None))?
        .len();
    let mut reader = ChunkReader::new(std::io::BufReader::new(file), info.clone(), total_size);
    for i in 0..info.chunk_offsets.len() {
        if let Err(e) = reader.read_chunk(i) {
            let offset = info.compressed_chunk_offset(i);
            return Err(data_corrupt(
                format!("chunk {i} failed CRC validation: {e}"),
                offset,
            ));
        }
    }
    Ok(())
}

/// Enumerate this input's partition boundaries: `(data_offset, raw_key)`
/// pairs in on-disk order, decompression-transparent for compressed input
/// (design D1). `data_offset` is in the SAME logical (decompressed-stream)
/// domain `Index.db`/the BTI trie encode, so it is exactly the value the
/// regenerated component must carry.
///
/// Any error escaping the underlying structural parse (a malformed
/// partition header, an out-of-range length field, ...) IS the R5 "cannot be
/// trusted" signal for an uncompressed input — the caller classifies it as
/// [`RefusalReason::DataCorrupt`].
///
/// `schema` is passed through explicitly (issue #4197) rather than relying
/// on the reader's own header-derived resolution, which depends on
/// `Statistics.db` being present at OPEN time — exactly the component
/// rebuild may be asked to regenerate, or relocate for repair-field
/// recovery (spec R4.2).
pub(super) async fn enumerate_partitions(
    reader: &SSTableReader,
    schema: &TableSchema,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let entries = reader
        .distinct_partition_keys_with_positions(Some(schema))
        .await?;
    if entries.len() < 2 {
        return Ok(entries);
    }
    for i in 1..entries.len() {
        if entries[i].0 <= entries[i - 1].0 {
            return Err(Error::corruption(format!(
                "partition boundaries are not strictly ascending in data_offset: entry {} at \
                 offset {} is followed by entry {} at offset {} — Data.db itself is corrupt \
                 (non-monotonic partition order)",
                i - 1,
                entries[i - 1].0,
                i,
                entries[i].0
            )));
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_corrupt_refusal_names_salvage_remedy() {
        let refusal = data_corrupt("chunk 3 failed CRC validation: mismatch", Some(4096));
        assert_eq!(refusal.reason, RefusalReason::DataCorrupt);
        assert!(refusal.remedy.contains("salvage"));
        assert_eq!(refusal.offset, Some(4096));
    }
}
