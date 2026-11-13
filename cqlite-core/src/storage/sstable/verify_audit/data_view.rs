//! Header-only logical reads of `Data.db` (invariant 1, and the BTI key
//! resolution for invariants 3/4).
//!
//! # Format authority
//!
//! A partition in `Data.db` starts with its key written by
//! `ByteBufferUtil.writeWithShortLength` inside `SortedTablePartitionWriter.start`
//! (cassandra-5.0.8): an unsigned 16-bit BIG-ENDIAN length, then the key bytes,
//! with no flags byte. This module reads exactly that and nothing else — no row
//! is decoded, no partition is read past its key. (CQLite's row decoder models
//! the length differently for keys >= 256 bytes, #3999; it is deliberately NOT
//! used here.)
//!
//! Offsets are LOGICAL: for a compressed `Data.db` the covering chunk is
//! located through `CompressionInfo` and decompressed with its inline CRC
//! checked; for an uncompressed one they are file offsets.

use crate::storage::sstable::chunk_decompressor::ChunkDecompressor;
use crate::storage::sstable::verify::CompressionState;
use crate::storage::sstable::version_gate::SsTableFormat;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

thread_local! {
    static HEADER_BYTES_REQUESTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Test hook: logical `Data.db` bytes the audit requested on THIS thread since the
/// last call (then reset). Lets a test prove invariant 1 reads partition headers
/// only. Meaningful on a current-thread runtime (`#[tokio::test]`).
#[doc(hidden)]
pub fn take_header_bytes_requested() -> u64 {
    HEADER_BYTES_REQUESTED.with(|c| c.replace(0))
}

enum Backing {
    Plain,
    Chunked(Box<ChunkDecompressor>),
}

/// A logical-offset reader over one `Data.db`, or the reason none exists.
pub(super) struct DataView {
    state: Result<(File, Backing, u64), String>,
    /// Logical bytes requested so far (the header-only read bound; tests read it).
    pub(super) bytes_requested: u64,
}

impl DataView {
    pub(super) fn open(path: &Path, compression: &CompressionState, format: SsTableFormat) -> Self {
        let state = (|| {
            let file = File::open(path).map_err(|e| format!("cannot open Data.db: {e}"))?;
            match compression {
                CompressionState::Uncompressed => {
                    let len = file
                        .metadata()
                        .map_err(|e| format!("cannot stat Data.db: {e}"))?
                        .len();
                    Ok((file, Backing::Plain, len))
                }
                CompressionState::Compressed(info) => {
                    let len = info.data_length;
                    let dec =
                        ChunkDecompressor::new(info.clone(), super::decompressor_version(format))
                            .map_err(|e| format!("cannot build chunk decompressor: {e}"))?;
                    Ok((file, Backing::Chunked(Box::new(dec)), len))
                }
                CompressionState::Unreadable => {
                    Err("CompressionInfo.db unreadable; Data.db cannot be addressed".to_string())
                }
            }
        })();
        Self {
            state,
            bytes_requested: 0,
        }
    }

    /// Logical length of `Data.db`, when addressable.
    pub(super) fn logical_len(&self) -> Option<u64> {
        self.state.as_ref().ok().map(|s| s.2)
    }

    /// Why the data cannot be addressed, if so.
    pub(super) fn unavailable(&self) -> Option<&str> {
        self.state.as_ref().err().map(String::as_str)
    }

    fn read(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        let (file, backing, logical_len) = self.state.as_mut().map_err(|e| e.clone())?;
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| "offset + length overflows".to_string())?;
        if end > *logical_len {
            return Err(format!(
                "read of {len} bytes at offset {offset} ends at {end}, past Data.db's {logical_len} bytes"
            ));
        }
        self.bytes_requested = self.bytes_requested.saturating_add(len as u64);
        HEADER_BYTES_REQUESTED.with(|c| c.set(c.get().saturating_add(len as u64)));
        match backing {
            Backing::Plain => {
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| e.to_string())?;
                let mut buf = vec![0u8; len];
                file.read_exact(&mut buf).map_err(|e| e.to_string())?;
                Ok(buf)
            }
            Backing::Chunked(dec) => dec.read_data(file, offset, len).map_err(|e| e.to_string()),
        }
    }

    /// The partition key at the partition start `offset` (header only).
    pub(super) fn key_at(&mut self, offset: u64) -> Result<Vec<u8>, String> {
        let len = self.read(offset, 2)?;
        let key_len = usize::from(u16::from_be_bytes([len[0], len[1]]));
        self.read(offset.saturating_add(2), key_len)
    }
}
