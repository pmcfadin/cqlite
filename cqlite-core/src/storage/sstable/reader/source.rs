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
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

use memmap2::Mmap;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, BufReader, ReadBuf};
use tokio::sync::Mutex;

use crate::Result;

/// A seekable byte source backing an [`SSTableReader`](super::types::SSTableReader).
///
/// See the module documentation for the trade-offs between the two backends.
pub(crate) enum BlockSource {
    /// Buffered file I/O through the OS page cache. `len` caches the immutable
    /// file length so the block-I/O layer never re-probes it with `seek(End)`
    /// per chunk (issue #1586).
    Buffered {
        reader: BufReader<File>,
        len: Option<u64>,
    },
    /// Memory-mapped file view served directly from the page cache.
    Mapped(MmapCursor),
    /// Direct I/O (`O_DIRECT` / `F_NOCACHE`) that bypasses the page cache.
    #[cfg(unix)]
    Direct(DirectCursor),
}

impl BlockSource {
    /// Create a buffered (non-mmap) source with the length resolved lazily on the
    /// first [`Self::len`] call. Test-only: production paths know the length at
    /// open and use [`Self::buffered_sized`] (issue #1586).
    #[cfg(test)]
    pub(crate) fn buffered(file: File) -> Self {
        BlockSource::Buffered {
            reader: BufReader::new(file),
            len: None,
        }
    }

    /// Create a buffered source with its (immutable) file length already known,
    /// so the block-I/O layer never issues a size probe at all (issue #1586).
    pub(crate) fn buffered_sized(file: File, len: u64) -> Self {
        BlockSource::Buffered {
            reader: BufReader::new(file),
            len: Some(len),
        }
    }

    /// The total byte length of the backing file. Resident for mmap/direct; for a
    /// buffered source it is cached (derived once via `metadata`, SSTables being
    /// immutable) rather than re-probed with `seek(End)` per chunk (issue #1586).
    pub(crate) async fn len(&mut self) -> io::Result<u64> {
        match self {
            BlockSource::Buffered { reader, len } => match *len {
                Some(l) => Ok(l),
                None => {
                    let l = reader.get_ref().metadata().await?.len();
                    *len = Some(l);
                    Ok(l)
                }
            },
            BlockSource::Mapped(c) => Ok(c.mmap.len() as u64),
            #[cfg(unix)]
            BlockSource::Direct(c) => Ok(c.len),
        }
    }

    /// Create a memory-mapped source from a previously mapped file.
    pub(crate) fn mapped(mmap: Arc<Mmap>) -> Self {
        BlockSource::Mapped(MmapCursor::new(mmap))
    }

    /// Create a direct-I/O source (page-cache-bypassing) over `cursor`.
    #[cfg(unix)]
    pub(crate) fn direct(cursor: DirectCursor) -> Self {
        BlockSource::Direct(cursor)
    }

    /// Whether reads from this backend BLOCK the calling thread synchronously
    /// (issue #1593, F3). A memory map faults in cold pages with a disk read at
    /// first access, and an `O_DIRECT`/`F_NOCACHE` cursor issues an uncached
    /// blocking `pread`; both do that work synchronously inside `poll_read`, so a
    /// scan reading them on a tokio async worker would starve the runtime. The
    /// buffered backend, by contrast, is genuinely async (`tokio::fs`, reactor-
    /// driven), so it returns `false` and its reads stay inline on the runtime.
    ///
    /// The windowed scan uses this to route a faulting backend's read loop onto a
    /// `spawn_blocking` thread. Keying on the ACTUAL backend (not the configured
    /// intent) matters: a `Direct` request that degrades to `Buffered` at open
    /// returns `false` here and is read inline — never driven under a non-tokio
    /// executor that lacks the reactor `tokio::fs` needs.
    pub(crate) fn faults_synchronously(&self) -> bool {
        match self {
            BlockSource::Buffered { .. } => false,
            BlockSource::Mapped(_) => true,
            #[cfg(unix)]
            BlockSource::Direct(_) => true,
        }
    }

    /// Returns `true` when this source is backed by a memory map.
    #[cfg(test)]
    pub(crate) fn is_mmap(&self) -> bool {
        matches!(self, BlockSource::Mapped(_))
    }

    /// Returns `true` when this source is backed by direct I/O.
    #[cfg(all(test, unix))]
    pub(crate) fn is_direct(&self) -> bool {
        matches!(self, BlockSource::Direct(_))
    }
}

/// Template for minting fresh, independent [`BlockSource`]s — one per scan.
///
/// Issue #815: concurrent scans on a single `SSTableReader` must not share a
/// mutable file position or chunk index, otherwise their seeks interleave and
/// corrupt each other's reads (the bug #805 fixed by serializing with a mutex).
/// Instead of serializing, each scan now opens its own [`ScanCursor`] from this
/// template, so they run in parallel while staying correct. SSTable files are
/// immutable, so minting extra views is always safe.
pub(crate) enum ScanSource {
    /// Reopen the file for each scan, giving it its own OS file handle and seek
    /// position. The per-handle cost is a small buffered reader.
    Buffered {
        /// Immutable file length captured at reader open (issue #1586).
        file_len: u64,
    },
    /// Share the underlying read-only memory map; each scan gets its own cursor
    /// position over the same mapped bytes (just an `Arc` clone, no new mapping).
    Mapped(Arc<Mmap>),
    /// Reopen the file with direct I/O for each scan, giving it its own
    /// page-cache-bypassing handle and aligned read-ahead window.
    #[cfg(unix)]
    Direct {
        /// Read-ahead window (bytes) for each scan's [`DirectCursor`].
        window: usize,
        /// Immutable file length captured at reader open (issue #1586), used by
        /// the buffered fallback when direct I/O is refused.
        file_len: u64,
    },
}

impl ScanSource {
    /// Mint a fresh, independent [`BlockSource`] for one scan.
    pub(crate) async fn open(&self, path: &Path) -> Result<BlockSource> {
        // A5 read-work counters (FILE_OPENS; consumer C2): each arm below counts the
        // exact number of open(2)s it performs — the Buffered arm and the Direct
        // buffered-fallback each do one File::open, and the Direct cursor open(2) is
        // counted inside `DirectCursor::open_direct`. The Mapped arm reuses the
        // shared `Arc<Mmap>` (no open(2)), so it records nothing. No-op in release
        // (design.md Decision 1/2). This is the per-scan open site C2 targets.
        use crate::storage::sstable::read_work_counters::record_file_open;
        Ok(match self {
            ScanSource::Buffered { file_len } => {
                record_file_open();
                BlockSource::buffered_sized(File::open(path).await?, *file_len)
            }
            ScanSource::Mapped(mmap) => BlockSource::mapped(mmap.clone()),
            #[cfg(unix)]
            ScanSource::Direct { window, file_len } => {
                // Reopen with O_DIRECT for this scan. If the platform/filesystem
                // refuses direct I/O, degrade to buffered rather than failing the
                // scan (mirrors the open()-time fallback).
                match DirectCursor::open(path, *window) {
                    Ok(cursor) => BlockSource::direct(cursor),
                    Err(e) => {
                        tracing::warn!(
                            "Direct-I/O reopen of {} for scan failed ({}); using buffered I/O",
                            path.display(),
                            e
                        );
                        record_file_open();
                        BlockSource::buffered_sized(File::open(path).await?, *file_len)
                    }
                }
            }
        })
    }
}

/// An independent read cursor for a single scan.
///
/// Bundles a private file handle (uncontended `Arc<Mutex<BlockSource>>`) with a
/// private `chunk_index`, so concurrent scans on the same `SSTableReader` never
/// touch shared mutable I/O state (issue #815). The mutex is per-scan and so is
/// effectively uncontended — it only exists because the block-I/O helpers need
/// `&mut` access to seek/read the source.
pub(crate) struct ScanCursor {
    pub(crate) file: Arc<Mutex<BlockSource>>,
    /// Wrapped in `Arc` (issue #1593, F3) so the windowed scan can hand the
    /// chunk index to a `spawn_blocking` I/O loop for synchronously-faulting
    /// backends while the cursor stays usable. `Arc<AtomicUsize>` is one pointer,
    /// so `ScanCursor` remains two pointers wide (the 16-byte pin below holds).
    pub(crate) chunk_index: Arc<AtomicUsize>,
}

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)). One
// `ScanCursor` is allocated per concurrent full scan (issue #815), so its
// footprint is on the read hot path. Measured 16 bytes today (two pointers: the
// `Arc<Mutex<BlockSource>>` file handle + the `Arc<AtomicUsize>` chunk index,
// issue #1593) on 64-bit targets. Update this pin DELIBERATELY, never silently:
// any change — growth or shrink — must be a reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<ScanCursor>() == 16);

impl ScanCursor {
    /// Wrap a freshly-minted source as a scan cursor positioned at chunk 0.
    pub(crate) fn new(source: BlockSource) -> Self {
        Self {
            file: Arc::new(Mutex::new(source)),
            chunk_index: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl AsyncRead for BlockSource {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BlockSource::Buffered { reader, .. } => Pin::new(reader).poll_read(cx, buf),
            BlockSource::Mapped(c) => Pin::new(c).poll_read(cx, buf),
            #[cfg(unix)]
            BlockSource::Direct(c) => Pin::new(c).poll_read(cx, buf),
        }
    }
}

impl AsyncSeek for BlockSource {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match self.get_mut() {
            BlockSource::Buffered { reader, .. } => Pin::new(reader).start_seek(position),
            BlockSource::Mapped(c) => Pin::new(c).start_seek(position),
            #[cfg(unix)]
            BlockSource::Direct(c) => Pin::new(c).start_seek(position),
        }
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match self.get_mut() {
            BlockSource::Buffered { reader, .. } => Pin::new(reader).poll_complete(cx),
            BlockSource::Mapped(c) => Pin::new(c).poll_complete(cx),
            #[cfg(unix)]
            BlockSource::Direct(c) => Pin::new(c).poll_complete(cx),
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

/// Open a file handle in cache-bypassing (direct-I/O) mode for the current
/// platform. Shared by the scan-cursor [`DirectCursor`] and the point-read
/// [`DirectReadAt`](super::read_at::DirectReadAt) so both honor the exact same
/// per-platform primitive.
///
/// - Linux/Android: `O_DIRECT`.
/// - macOS: `F_NOCACHE` (the per-fd equivalent; no `O_DIRECT`).
/// - other Unix: unsupported → the caller falls back to buffered I/O.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn open_direct_file(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

#[cfg(target_os = "macos")]
pub(crate) fn open_direct_file(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    let file = std::fs::File::open(path)?;
    // SAFETY: `fcntl` with `F_NOCACHE` on a freshly opened, valid fd.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(file)
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
pub(crate) fn open_direct_file(_path: &Path) -> io::Result<std::fs::File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "direct I/O is not supported on this platform",
    ))
}

/// I/O alignment (bytes) for direct reads. `O_DIRECT` requires the file
/// offset, transfer length, and user buffer to all be aligned to the logical
/// block size of the underlying device. 4096 is a safe superset of the common
/// 512/4096-byte sector sizes, so aligning to it satisfies every mainstream
/// local filesystem.
#[cfg(unix)]
const DIRECT_IO_ALIGN: usize = 4096;

/// A page-cache-bypassing read cursor over a file opened with direct I/O.
///
/// Direct I/O (`O_DIRECT` on Linux, `F_NOCACHE` on macOS) serves reads straight
/// from the device without populating or evicting the OS page cache. This is
/// the right backend for a single large scan (a Data.db file bigger than a
/// configurable fraction of system RAM): it keeps that one-shot scan from
/// thrashing the page cache and evicting everything else the host has warm.
///
/// Because `O_DIRECT` mandates aligned transfers, the cursor maintains its own
/// aligned read-ahead buffer: each refill issues one aligned `pread` of up to
/// `window` bytes, and `poll_read` copies the requested sub-range out of it.
/// The window therefore doubles as the prefetch granularity.
///
/// Like [`MmapCursor`], the actual `pread` runs synchronously inside
/// `poll_read`; direct reads are uncached disk I/O, so callers should keep this
/// backend to the blocking-friendly contexts where the buffered/mmap backends
/// already perform synchronous page faults.
#[cfg(unix)]
pub(crate) struct DirectCursor {
    file: std::fs::File,
    /// Total file length, used for EOF detection.
    len: u64,
    /// Logical read position (may sit past EOF; reads then yield zero bytes).
    pos: u64,
    /// Read-ahead window size (bytes), already rounded up to [`DIRECT_IO_ALIGN`].
    window: usize,
    /// Aligned scratch buffer holding the most recently read window.
    buf: AlignedBuf,
    /// File offset of `buf[0]` (always a multiple of [`DIRECT_IO_ALIGN`]).
    buf_off: u64,
    /// Number of valid bytes currently in `buf`.
    buf_len: usize,
}

#[cfg(unix)]
impl DirectCursor {
    /// Open `path` with direct I/O and a `window`-byte aligned read-ahead buffer.
    ///
    /// `window` is rounded up to [`DIRECT_IO_ALIGN`] (and never less than one
    /// alignment unit). Returns an error if the platform or filesystem does not
    /// support direct I/O, so callers can fall back to buffered reads.
    pub(crate) fn open(path: &Path, window: usize) -> io::Result<Self> {
        let file = Self::open_direct(path)?;
        // A5 read-work counter (FILE_OPENS; consumer C2): one per SUCCESSFUL
        // direct-I/O open(2) (used by both the reader's cold-open direct path and
        // per-scan direct reopens). Counted after `open_direct` succeeds so a failed
        // direct open that falls back to a buffered open (which counts its own
        // open(2)) is not double-counted. No-op in release (design.md Decision 1/2).
        crate::storage::sstable::read_work_counters::record_file_open();
        let len = file.metadata()?.len();
        let align = DIRECT_IO_ALIGN;
        // Round the window up to the alignment, but saturate instead of
        // overflowing (a near-`usize::MAX` `direct_io_prefetch_bytes` would
        // otherwise panic in debug / wrap to 0 in release). A huge window then
        // simply fails the allocation below and the caller falls back to
        // buffered I/O, rather than crashing.
        let window = window
            .max(align)
            .checked_next_multiple_of(align)
            .unwrap_or(usize::MAX & !(align - 1));
        let buf = AlignedBuf::new(window, align)?;
        Ok(Self {
            file,
            len,
            pos: 0,
            window,
            buf,
            buf_off: 0,
            buf_len: 0,
        })
    }

    /// Open a file handle in cache-bypassing mode for the current platform.
    fn open_direct(path: &Path) -> io::Result<std::fs::File> {
        open_direct_file(path)
    }

    /// Ensure the byte at `self.pos` is resident in `buf`, refilling with one
    /// aligned `pread` if not.
    fn ensure_buffered(&mut self) -> io::Result<()> {
        let covered = self.buf_len > 0
            && self.pos >= self.buf_off
            && self.pos < self.buf_off + self.buf_len as u64;
        if covered {
            return Ok(());
        }
        let align = DIRECT_IO_ALIGN as u64;
        let aligned_off = (self.pos / align) * align;
        let slice = self.buf.as_mut_slice();
        debug_assert_eq!(slice.len(), self.window);
        // A single aligned pread. O_DIRECT returns the full window unless it
        // straddles EOF, where it returns just the available tail (a legal
        // short read). `read_at` does not advance the file's own offset, so
        // concurrent cursors over reopened handles never interfere.
        use std::os::unix::fs::FileExt;
        let n = self.file.read_at(slice, aligned_off)?;
        self.buf_off = aligned_off;
        self.buf_len = n;
        Ok(())
    }
}

#[cfg(unix)]
impl AsyncRead for DirectCursor {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // At or past EOF: yield zero bytes, leaving position untouched (matching
        // `File`/`MmapCursor` semantics so `read_exact` reports `UnexpectedEof`).
        if this.pos >= this.len {
            return Poll::Ready(Ok(()));
        }
        if let Err(e) = this.ensure_buffered() {
            return Poll::Ready(Err(e));
        }
        let rel = (this.pos - this.buf_off) as usize;
        if rel >= this.buf_len {
            // The aligned read landed entirely before `pos` (only possible at a
            // short EOF read); nothing more to hand back.
            return Poll::Ready(Ok(()));
        }
        let available = &this.buf.as_slice()[rel..this.buf_len];
        let n = available.len().min(buf.remaining());
        buf.put_slice(&available[..n]);
        this.pos += n as u64;
        Poll::Ready(Ok(()))
    }
}

#[cfg(unix)]
impl AsyncSeek for DirectCursor {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        this.pos = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => offset_from(this.len, offset)?,
            SeekFrom::Current(offset) => offset_from(this.pos, offset)?,
        };
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().pos))
    }
}

/// A heap allocation whose start address is aligned to a requested boundary,
/// as required for `O_DIRECT` transfer buffers.
#[cfg(unix)]
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

// SAFETY: `AlignedBuf` uniquely owns its allocation for its whole lifetime; the
// raw pointer is never aliased or shared, so moving it across threads is sound.
#[cfg(unix)]
unsafe impl Send for AlignedBuf {}

#[cfg(unix)]
impl AlignedBuf {
    fn new(size: usize, align: usize) -> io::Result<Self> {
        let size = size.max(align);
        let layout = std::alloc::Layout::from_size_align(size, align)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: `layout` has non-zero size (size >= align >= 1).
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(raw)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "aligned alloc failed"))?;
        Ok(Self { ptr, layout })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `layout.size()` initialised bytes (zeroed
        // on alloc, then overwritten by reads) and borrowed immutably here.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid for `layout.size()` bytes and uniquely borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

#[cfg(unix)]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` are exactly the values returned from the
        // matching `alloc_zeroed`, freed exactly once here.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
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

    /// Direct-I/O cursor reads back exactly what was written, including across
    /// the aligned read-ahead window boundary and at a non-aligned EOF. Skips
    /// when the temp filesystem refuses `O_DIRECT` (common on tmpfs/overlayfs).
    #[cfg(unix)]
    #[tokio::test]
    async fn direct_cursor_reads_and_seeks() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // A size that is NOT a multiple of the alignment, spanning multiple
        // windows, to exercise the partial-final-block path.
        let len = DIRECT_IO_ALIGN * 3 + 123;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }

        let dir = std::env::temp_dir();
        let path = dir.join(format!("cqlite_directcursor_{}.bin", std::process::id()));
        tokio::fs::write(&path, &data).await.unwrap();

        // Small window (2 alignment units) so a full read crosses windows.
        let mut cursor = match DirectCursor::open(&path, DIRECT_IO_ALIGN * 2) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("O_DIRECT unsupported here ({e}); skipping direct cursor test");
                tokio::fs::remove_file(&path).await.ok();
                return;
            }
        };

        // Sequential read of the whole file.
        let mut got = Vec::with_capacity(len);
        let mut chunk = [0u8; 1000];
        loop {
            let n = cursor.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(got, data, "direct sequential read must match contents");

        // Seek to a non-aligned offset and read a window-crossing span.
        cursor.seek(SeekFrom::Start(4090)).await.unwrap();
        let mut window = [0u8; 16];
        cursor.read_exact(&mut window).await.unwrap();
        for (k, b) in window.iter().enumerate() {
            assert_eq!(*b, ((4090 + k) % 251) as u8, "byte {k} after seek");
        }

        // Reading past EOF reports UnexpectedEof.
        cursor
            .seek(SeekFrom::Start((len - 4) as u64))
            .await
            .unwrap();
        let mut tail = [0u8; 8];
        let err = cursor.read_exact(&mut tail).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        tokio::fs::remove_file(&path).await.ok();
    }
}
