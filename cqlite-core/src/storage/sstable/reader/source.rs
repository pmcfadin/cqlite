//! Backing store abstraction for SSTable block I/O.
//!
//! The SSTable reader accesses Data.db through a single seekable byte source.
//! Historically this was always a buffered file handle (`BufReader<File>`).
//! [`BlockSource`] generalizes that to two interchangeable backends:
//!
//! - [`BlockSource::Buffered`]: standard buffered async file I/O. Reads go
//!   through the OS page cache with kernel read-ahead. This is the safe,
//!   universally-portable default and is used for small files (where mmap
//!   setup overhead is not worth it) or when mmap is disabled.
//! - [`BlockSource::Mapped`]: a memory-mapped view of the file. The OS maps
//!   the file into the process address space and serves reads straight from
//!   the page cache with no per-block `read` syscall and no extra copy into a
//!   buffered reader. This is well-suited to local SSTable analysis where the
//!   same files are scanned repeatedly across queries (the page cache is
//!   shared and reused), which is also the strategy Cassandra itself uses for
//!   its read path (`disk_access_mode: mmap`).
//!
//! Both variants implement [`tokio::io::AsyncRead`] and
//! [`tokio::io::AsyncSeek`], so every existing call site
//! (`seek` / `stream_position` / `read` / `read_exact`) works against either
//! backend without modification. The mmap backend is purely in-memory, so its
//! poll methods always complete synchronously (`Poll::Ready`).

use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use memmap2::Mmap;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, BufReader, ReadBuf};

/// A seekable byte source backing an [`SSTableReader`](super::types::SSTableReader).
///
/// See the module documentation for the trade-offs between the two backends.
pub(crate) enum BlockSource {
    /// Buffered file I/O through the OS page cache.
    Buffered(BufReader<File>),
    /// Memory-mapped file view served directly from the page cache.
    Mapped(MmapCursor),
}

impl BlockSource {
    /// Create a buffered (non-mmap) source from an open tokio file.
    pub(crate) fn buffered(file: File) -> Self {
        BlockSource::Buffered(BufReader::new(file))
    }

    /// Create a memory-mapped source from a previously mapped file.
    pub(crate) fn mapped(mmap: Arc<Mmap>) -> Self {
        BlockSource::Mapped(MmapCursor::new(mmap))
    }

    /// Returns `true` when this source is backed by a memory map.
    #[cfg(test)]
    pub(crate) fn is_mmap(&self) -> bool {
        matches!(self, BlockSource::Mapped(_))
    }
}

impl AsyncRead for BlockSource {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BlockSource::Buffered(r) => Pin::new(r).poll_read(cx, buf),
            BlockSource::Mapped(c) => Pin::new(c).poll_read(cx, buf),
        }
    }
}

impl AsyncSeek for BlockSource {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match self.get_mut() {
            BlockSource::Buffered(r) => Pin::new(r).start_seek(position),
            BlockSource::Mapped(c) => Pin::new(c).start_seek(position),
        }
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match self.get_mut() {
            BlockSource::Buffered(r) => Pin::new(r).poll_complete(cx),
            BlockSource::Mapped(c) => Pin::new(c).poll_complete(cx),
        }
    }
}

/// An in-memory read cursor over a memory-mapped file.
///
/// Implements the same `AsyncRead`/`AsyncSeek` contract as a buffered file
/// reader, but every operation completes immediately since the data is already
/// resident (modulo lazy page faults handled transparently by the OS).
pub(crate) struct MmapCursor {
    mmap: Arc<Mmap>,
    /// Current read position. May legally point past the end of the map, in
    /// which case reads yield zero bytes (matching `File` EOF semantics).
    pos: u64,
}

impl MmapCursor {
    fn new(mmap: Arc<Mmap>) -> Self {
        Self { mmap, pos: 0 }
    }
}

impl AsyncRead for MmapCursor {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let data: &[u8] = &this.mmap;
        let len = data.len() as u64;
        // At or past EOF: read nothing and leave the position untouched. A real
        // `File` preserves a seeked-past-EOF position across a zero-byte read,
        // so we must not clamp `pos` back to `len` here (that divergence was the
        // source-level parity bug). Returning zero bytes still drives
        // `read_exact` to `UnexpectedEof`, which the block-header readers use to
        // detect EOF.
        if this.pos >= len {
            return Poll::Ready(Ok(()));
        }
        let pos = this.pos as usize;
        let remaining = &data[pos..];
        let n = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..n]);
        this.pos += n as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for MmapCursor {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let len = this.mmap.len() as u64;
        let new_pos = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => offset_from(len, offset)?,
            SeekFrom::Current(offset) => offset_from(this.pos, offset)?,
        };
        this.pos = new_pos;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().pos))
    }
}

/// Apply a signed offset to a base position, rejecting seeks before byte 0.
///
/// Seeking past the end is permitted (matching `std`/`tokio` `File` behaviour);
/// subsequent reads simply return zero bytes.
fn offset_from(base: u64, offset: i64) -> io::Result<u64> {
    let result = if offset >= 0 {
        // Overflowing the u64 address space is rejected rather than clamped, to
        // mirror `std::fs::File`, which errors on a seek that overflows.
        base.checked_add(offset as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to an overflowing position",
            )
        })?
    } else {
        base.checked_sub(offset.unsigned_abs()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            )
        })?
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn cursor(bytes: &[u8]) -> MmapCursor {
        // Build an anonymous memory map and copy bytes into it so tests do not
        // depend on touching the filesystem.
        let mut mmap = memmap2::MmapMut::map_anon(bytes.len().max(1)).unwrap();
        mmap[..bytes.len()].copy_from_slice(bytes);
        let mmap = mmap.make_read_only().unwrap();
        MmapCursor::new(Arc::new(mmap))
    }

    #[tokio::test]
    async fn reads_sequentially() {
        let mut c = cursor(b"hello world");
        let mut buf = [0u8; 5];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        assert_eq!(c.stream_position().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn seek_start_current_end() {
        let mut c = cursor(b"0123456789");
        c.seek(SeekFrom::Start(3)).await.unwrap();
        let mut b = [0u8; 2];
        c.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"34");

        c.seek(SeekFrom::Current(2)).await.unwrap();
        c.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"78");

        // SeekFrom::End(0) should land at EOF and report total length.
        let end = c.seek(SeekFrom::End(0)).await.unwrap();
        assert_eq!(end, 10);
    }

    #[tokio::test]
    async fn read_past_eof_is_unexpected_eof() {
        let mut c = cursor(b"abc");
        c.seek(SeekFrom::Start(2)).await.unwrap();
        let mut b = [0u8; 8];
        let err = c.read_exact(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn negative_seek_before_start_errors() {
        let mut c = cursor(b"abc");
        let err = c.seek(SeekFrom::Current(-5)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn block_source_reports_backend() {
        // Mapped variant reports as mmap-backed.
        let mmap = memmap2::MmapMut::map_anon(8).unwrap();
        let mmap = mmap.make_read_only().unwrap();
        assert!(BlockSource::mapped(Arc::new(mmap)).is_mmap());

        // Buffered variant does not.
        let dir = std::env::temp_dir();
        let path = dir.join("cqlite_blocksource_backend_test.bin");
        tokio::fs::write(&path, b"buffered").await.unwrap();
        let file = tokio::fs::File::open(&path).await.unwrap();
        assert!(!BlockSource::buffered(file).is_mmap());
        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn positive_seek_overflow_errors() {
        // A positive offset that overflows the u64 address space is rejected
        // with InvalidInput (matching `File`), not saturated to u64::MAX.
        let mut c = cursor(b"abc");
        // Park the cursor at the very top of the address space (seeking past EOF
        // is legal), then a positive Current offset overflows.
        c.seek(SeekFrom::Start(u64::MAX)).await.unwrap();
        let err = c.seek(SeekFrom::Current(1)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // A large-but-non-overflowing positive offset still succeeds (parity:
        // seeking past EOF is permitted, only overflow is an error).
        let mut c2 = cursor(b"abc"); // len 3
        let landed = c2.seek(SeekFrom::End(i64::MAX)).await.unwrap();
        assert_eq!(landed, 3u64 + i64::MAX as u64);
    }

    #[tokio::test]
    async fn seek_past_eof_preserves_position_like_file() {
        // Parity with std/tokio `File`: seeking past EOF and issuing a read
        // returns zero bytes WITHOUT collapsing the cursor back to EOF.
        let mut c = cursor(b"abc"); // len 3
        let landed = c.seek(SeekFrom::Start(10)).await.unwrap();
        assert_eq!(landed, 10);

        let mut b = [0u8; 4];
        let n = c.read(&mut b).await.unwrap();
        assert_eq!(n, 0, "read past EOF yields no bytes");

        // The position must still be 10, exactly as a real File would report,
        // not clamped down to the file length (3).
        assert_eq!(c.stream_position().await.unwrap(), 10);

        // Seeking back to a valid offset still reads correctly afterwards.
        c.seek(SeekFrom::Start(1)).await.unwrap();
        let mut one = [0u8; 1];
        c.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"b");
    }

    #[tokio::test]
    async fn seek_past_eof_position_matches_real_file() {
        // Cross-check the cursor's behaviour against an actual tokio File so the
        // parity claim is verified against the reference implementation, not
        // just asserted in isolation.
        let bytes = b"abc";
        let dir = std::env::temp_dir();
        let path = dir.join("cqlite_mmapcursor_eof_parity.bin");
        tokio::fs::write(&path, bytes).await.unwrap();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        file.seek(SeekFrom::Start(10)).await.unwrap();
        let mut fb = [0u8; 4];
        let file_n = file.read(&mut fb).await.unwrap();
        let file_pos = file.stream_position().await.unwrap();
        tokio::fs::remove_file(&path).await.ok();

        let mut c = cursor(bytes);
        c.seek(SeekFrom::Start(10)).await.unwrap();
        let mut cb = [0u8; 4];
        let cur_n = c.read(&mut cb).await.unwrap();
        let cur_pos = c.stream_position().await.unwrap();

        assert_eq!(cur_n, file_n, "byte count parity");
        assert_eq!(cur_pos, file_pos, "post-read position parity");
    }

    #[tokio::test]
    async fn multipage_read_across_page_boundary() {
        // Exercise a map larger than a single OS page and read straddling the
        // 4096-byte boundary, ensuring slicing/position math holds beyond one
        // page.
        let len = 10_000usize;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8; // deterministic, spans the page boundary
        }
        let mut c = cursor(&data);

        // Read a window that starts before page 1 and ends after it.
        c.seek(SeekFrom::Start(4090)).await.unwrap();
        let mut window = [0u8; 16]; // covers 4090..4106, crossing 4096
        c.read_exact(&mut window).await.unwrap();
        for (k, b) in window.iter().enumerate() {
            assert_eq!(*b, ((4090 + k) % 251) as u8);
        }
        assert_eq!(c.stream_position().await.unwrap(), 4106);

        // Read the final bytes right up to EOF.
        c.seek(SeekFrom::Start((len - 4) as u64)).await.unwrap();
        let mut tail = [0u8; 4];
        c.read_exact(&mut tail).await.unwrap();
        for (k, b) in tail.iter().enumerate() {
            assert_eq!(*b, ((len - 4 + k) % 251) as u8);
        }
        assert_eq!(c.stream_position().await.unwrap(), len as u64);
    }

    #[tokio::test]
    async fn partial_read_returns_available_bytes() {
        let mut c = cursor(b"abcd");
        c.seek(SeekFrom::Start(2)).await.unwrap();
        let mut b = [0u8; 8];
        // A single read() yields only the available bytes, not an error.
        let n = c.read(&mut b).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&b[..2], b"cd");
    }
}
