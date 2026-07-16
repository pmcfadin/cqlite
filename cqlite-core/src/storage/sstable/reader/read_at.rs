//! Positional (`pread`-style) read backends for the SSTable point-read path
//! (issue #1573, Epic C / C2).
//!
//! # Why this exists
//!
//! The historical point-read path served every read through a single
//! [`BlockSource`](super::source::BlockSource) behind an
//! `Arc<Mutex<BlockSource>>`. That mutex exists ONLY to guard a mutable seek
//! cursor, and it was held **across disk I/O**, so N concurrent point reads on
//! one SSTable serialized one-at-a-time (the convoy). The BTI variant sidestepped
//! the mutex by paying `File::open` **per lookup** instead — trading the convoy
//! for per-op `open(2)` syscalls and an fd-exhaustion risk under load.
//!
//! Both pathologies share one root cause: a *stateful* file cursor. [`ReadAt`]
//! removes the state. A positioned read carries its offset as a call parameter,
//! not as shared mutable file state, so:
//!
//! - **No mutable position** — the trait has no seek cursor.
//! - **No `&mut self`** — every method takes `&self`, so concurrent callers share
//!   one value with no exclusive borrow and no mutex.
//! - **No seek** — the offset is a parameter of the read.
//!
//! A positioned read on a shared file descriptor is independent of every other
//! positioned read (POSIX `pread`/`FileExt::read_at` does not touch the fd's
//! offset), so the lock is not merely moved off the point path — it is removed.
//!
//! # Backends
//!
//! | Backend | `read_at` | Notes |
//! |---------|-----------|-------|
//! | [`PlainFileReadAt`] | `FileExt::read_at` (Unix, lock-free) / `Mutex`-guarded `seek_read` (Windows) on ONE shared `File` | Unix `pread` is positional so reads are independent (no lock); Windows `seek_read` moves the shared cursor, so it is serialized under a `Mutex`. Positioned reads on a shared fd are independent — no lock. |
//! | [`MmapReadAt`] | slice indexing into the resident map | Bounds-checked; zero syscalls per read. |
//! | [`DirectReadAt`] (Unix) | aligned positioned read honoring the `O_DIRECT` 4K rule | A per-call aligned bounce buffer keeps `&self` (no shared cursor). |
//!
//! The trait is intentionally shaped so the windowed streaming scan can adopt it
//! later (audit F3) without requiring it now; this change migrates only the
//! point-read path (design.md Decision 4).

use std::sync::Arc;

use memmap2::Mmap;

use crate::{Error, Result};

/// A positional byte source: reads are addressed by an explicit `offset`, never
/// by a shared mutable cursor.
///
/// Every method takes `&self`. Two concurrent `read_at` calls at different
/// offsets are independent — neither observes nor disturbs the other's position,
/// because there is no position. This is the whole point of the refactor
/// (issue #1573): concurrent point reads on one SSTable no longer serialize on a
/// cross-I/O mutex, and BTI lookups no longer `open(2)` per call.
pub(crate) trait ReadAt: Send + Sync {
    /// Read up to `buf.len()` bytes starting at absolute file `offset`, returning
    /// the number of bytes read. A short read (fewer than `buf.len()`) is legal
    /// at/near EOF, exactly like POSIX `pread`.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Total length of the backing file in bytes.
    fn len(&self) -> u64;

    /// Read EXACTLY `buf.len()` bytes starting at `offset`, erroring with
    /// [`std::io::ErrorKind::UnexpectedEof`] if the source is exhausted first.
    ///
    /// Default impl loops [`read_at`](Self::read_at) so every backend gets an
    /// exact-read helper for free; backends may override if they can do better.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = self.read_at(offset + filled as u64, &mut buf[filled..])?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "read_exact_at: reached EOF after {filled} of {} bytes at offset 0x{offset:x}",
                        buf.len()
                    ),
                )));
            }
            filled += n;
        }
        Ok(())
    }

    /// Like [`read_exact_at`](Self::read_exact_at) but may REUSE the caller-owned
    /// [`DirectScratch`] aligned bounce buffer across calls, so the Direct-I/O
    /// windowed scan does not allocate a fresh ~chunk-sized aligned buffer per
    /// chunk read (issue #2319 — the regression #1940 introduced by routing the
    /// Direct windowed scan through the generic per-call [`DirectReadAt::read_at`]
    /// instead of the pre-#1940 per-scan `DirectCursor` that reused ONE window
    /// buffer).
    ///
    /// The default impl IGNORES `scratch` and defers to
    /// [`read_exact_at`](Self::read_exact_at): the mmap and plain-file backends
    /// never allocate a bounce buffer at all, so buffer reuse is a no-op for them
    /// and the window-as-`Bytes` substrate (#1940) is untouched. ONLY
    /// [`DirectReadAt`] overrides it, threading the reusable aligned buffer through
    /// its per-read alignment math.
    fn read_exact_at_reusing(
        &self,
        offset: u64,
        buf: &mut [u8],
        _scratch: &mut DirectScratch,
    ) -> Result<()> {
        self.read_exact_at(offset, buf)
    }
}

/// A caller-owned reusable aligned bounce buffer for the Direct-I/O windowed-scan
/// read path (issue #2319).
///
/// The windowed feed creates ONE `DirectScratch` per scan and threads a `&mut` to
/// it through every chunk read via [`ReadAt::read_exact_at_reusing`]. When the
/// backend is [`DirectReadAt`], the read reuses this single aligned buffer instead
/// of allocating one per chunk — restoring the pre-#1940 per-scan reused-window
/// behaviour. For every other backend it carries no state and costs nothing (the
/// aligned buffer is `Option`-wrapped and stays `None`). Since the feed owns it
/// exclusively for the scan's lifetime, no cursor/mutex is needed and concurrent
/// scans (each with their own `DirectScratch`) never share the buffer.
pub(crate) struct DirectScratch {
    /// The reused aligned buffer, grown on demand to the working-set high-water
    /// mark. `None` until the first Direct read; always `None` on non-Direct
    /// backends and non-Unix targets (no direct I/O there).
    #[cfg(unix)]
    bounce: Option<AlignedBuf>,
}

impl DirectScratch {
    /// A fresh scratch with no buffer allocated yet.
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(unix)]
            bounce: None,
        }
    }
}

/// Delegating impl so an `Arc<dyn ReadAt>` (or `Arc<ConcreteBackend>`) is itself a
/// `ReadAt`. Lets the point path hold a single `Arc<dyn ReadAt>` and lets a test
/// wrap the reader's existing shared source without unsizing gymnastics.
impl<T: ReadAt + ?Sized> ReadAt for Arc<T> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        (**self).read_at(offset, buf)
    }
    fn len(&self) -> u64 {
        (**self).len()
    }
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact_at(offset, buf)
    }
    // Forward the reusing read so a `DirectReadAt` behind an `Arc<dyn ReadAt>`
    // (exactly `point_source`'s type) still reaches its override — otherwise the
    // Direct windowed scan would silently fall back to the default per-chunk-alloc
    // path (issue #2319).
    fn read_exact_at_reusing(
        &self,
        offset: u64,
        buf: &mut [u8],
        scratch: &mut DirectScratch,
    ) -> Result<()> {
        (**self).read_exact_at_reusing(offset, buf, scratch)
    }
}

/// A `ReadAt` that sleeps before delegating — models a slow backend so the convoy
/// test can prove concurrent point reads overlap instead of serializing, and
/// counts invocations so a routing assertion can prove the point path reached the
/// trait at all. Test-only (issue #1573 convoy scenario).
#[cfg(test)]
pub(crate) struct SleepingReadAt<R: ReadAt> {
    inner: R,
    delay: std::time::Duration,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl<R: ReadAt> SleepingReadAt<R> {
    pub(crate) fn new(
        inner: R,
        delay: std::time::Duration,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            delay,
            calls,
        }
    }
}

#[cfg(test)]
impl<R: ReadAt> ReadAt for SleepingReadAt<R> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::thread::sleep(self.delay);
        self.inner.read_at(offset, buf)
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

/// CONTROL double for the convoy scenario: a slow source whose `read_at` holds a
/// `std::sync::Mutex` ACROSS the sleep+read, exactly reproducing the pre-#1573
/// `Arc<Mutex<BlockSource>>` cursor that serialized point reads. Used to prove the
/// convoy harness CAN serialize (~N×delay), so the parallel result on the real
/// migrated `point_source` (no mutex) is a real speedup, not a fast harness.
#[cfg(test)]
pub(crate) struct SerializingReadAt<R: ReadAt> {
    inner: R,
    delay: std::time::Duration,
    lock: std::sync::Mutex<()>,
}

#[cfg(test)]
impl<R: ReadAt> SerializingReadAt<R> {
    pub(crate) fn new(inner: R, delay: std::time::Duration) -> Self {
        Self {
            inner,
            delay,
            lock: std::sync::Mutex::new(()),
        }
    }
}

#[cfg(test)]
impl<R: ReadAt> ReadAt for SerializingReadAt<R> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // Hold the lock across the "I/O" (sleep) — the convoy the refactor removes.
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        std::thread::sleep(self.delay);
        self.inner.read_at(offset, buf)
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

/// Positional reads over a plain file handle.
///
/// Wraps ONE shared `std::fs::File`, opened once at reader open and shared for the
/// reader's lifetime (design.md Decision 3).
///
/// On **Unix**, `FileExt::read_at` issues a `pread` that does not touch the fd's
/// offset, so concurrent reads on this single handle are independent — no lock, no
/// per-lookup `open(2)`.
///
/// On **Windows** there is no positional `pread`: `seek_read` MOVES the handle's
/// file pointer, so two concurrent reads on one shared handle would race the cursor
/// and return the wrong bytes (silent corruption). The Windows arm therefore guards
/// the handle with a `std::sync::Mutex` and performs the seek+read under the lock,
/// serializing Windows point reads for correctness while still opening the fd only
/// once. The Unix arm keeps its lock-free `Arc<File>` (issue #1573 roborev finding).
pub(crate) struct PlainFileReadAt {
    /// Unix: shared handle, read lock-free via positional `pread` (no cursor).
    #[cfg(unix)]
    file: Arc<std::fs::File>,
    /// Windows: `seek_read` mutates the shared cursor, so the handle is `Mutex`-
    /// guarded and read+seek happen atomically under the lock (see struct docs).
    #[cfg(windows)]
    file: std::sync::Mutex<std::fs::File>,
    len: u64,
}

impl PlainFileReadAt {
    /// Wrap an already-open file handle whose length is `len`.
    pub(crate) fn new(file: std::fs::File, len: u64) -> Self {
        Self {
            #[cfg(unix)]
            file: Arc::new(file),
            #[cfg(windows)]
            file: std::sync::Mutex::new(file),
            len,
        }
    }

    /// Open `path` read-only for positioned reads, recording one `open(2)` in the
    /// read-work counters (consumer C2). This is the single per-reader open the
    /// BTI point path used to pay per lookup.
    pub(crate) fn open(path: &std::path::Path, len: u64) -> Result<Self> {
        crate::storage::sstable::read_work_counters::record_file_open();
        let file = std::fs::File::open(path)?;
        Ok(Self::new(file, len))
    }
}

impl ReadAt for PlainFileReadAt {
    #[cfg(unix)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        Ok(self.file.read_at(buf, offset)?)
    }

    #[cfg(windows)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // Windows has no positional `pread`; `seek_read` MOVES the handle's file
        // pointer, so two concurrent `read_at` calls on the one shared handle would
        // race the cursor and return the wrong bytes (silent corruption). We hold a
        // `Mutex` across the seek+read so Windows point reads are serialized and
        // correct, while still opening the fd only once (issue #1573 roborev
        // finding). This intentionally trades Windows concurrency for correctness;
        // the Unix arm above needs no lock because `FileExt::read_at` is positional
        // and never touches the fd offset. Recover a poisoned lock in-place (a
        // panicking reader cannot leave the handle in a torn state) rather than
        // `unwrap`/`expect` in library code.
        use std::os::windows::fs::FileExt;
        let file = self.file.lock().unwrap_or_else(|e| e.into_inner());
        Ok(file.seek_read(buf, offset)?)
    }

    fn len(&self) -> u64 {
        self.len
    }
}

/// Positional reads over a memory-mapped file.
///
/// A slice read out of the resident map — zero syscalls, and trivially `&self`
/// (the map is immutable). Shares the reader's existing `Arc<Mmap>` when the
/// backend is mmap, so no extra mapping or fd is created.
pub(crate) struct MmapReadAt {
    mmap: Arc<Mmap>,
}

impl MmapReadAt {
    pub(crate) fn new(mmap: Arc<Mmap>) -> Self {
        Self { mmap }
    }
}

impl ReadAt for MmapReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let data: &[u8] = &self.mmap;
        let len = data.len() as u64;
        // At/past EOF: zero bytes, matching `File`/pread semantics.
        if offset >= len {
            return Ok(0);
        }
        let start = offset as usize;
        let avail = &data[start..];
        let n = avail.len().min(buf.len());
        buf[..n].copy_from_slice(&avail[..n]);
        Ok(n)
    }

    fn len(&self) -> u64 {
        self.mmap.len() as u64
    }
}

/// I/O alignment (bytes) for `O_DIRECT`: the offset, length, and buffer must all
/// be a multiple of the device logical block size. 4096 is a safe superset of the
/// common 512/4096 sector sizes (mirrors `source::DIRECT_IO_ALIGN`).
#[cfg(unix)]
const DIRECT_IO_ALIGN: usize = 4096;

/// Positional reads over a file opened with direct I/O (`O_DIRECT` / `F_NOCACHE`).
///
/// `O_DIRECT` mandates aligned transfers, so each `read_at` allocates its OWN
/// aligned bounce buffer, `pread`s an aligned superset of the requested range,
/// and copies out the requested sub-range. Because the bounce buffer is per-call
/// (never shared), the backend keeps `&self` with no shared mutable cursor — two
/// concurrent `read_at`s never touch the same buffer.
#[cfg(unix)]
pub(crate) struct DirectReadAt {
    file: Arc<std::fs::File>,
    len: u64,
}

#[cfg(unix)]
impl DirectReadAt {
    /// Open `path` with direct I/O for positioned reads. Records one `open(2)`
    /// (consumer C2). Returns an error if the platform/filesystem refuses direct
    /// I/O so the caller can fall back to buffered.
    pub(crate) fn open(path: &std::path::Path, len: u64) -> Result<Self> {
        let file = super::source::open_direct_file(path)?;
        crate::storage::sstable::read_work_counters::record_file_open();
        Ok(Self {
            file: Arc::new(file),
            len,
        })
    }

    /// Test-only: build a `DirectReadAt` over an ALREADY-OPEN plain file handle,
    /// bypassing the `O_DIRECT`/`F_NOCACHE` open (issue #2319). The aligned-bounce
    /// read path (`read_at_into` / `read_exact_at_reusing`) is byte-identical
    /// regardless of whether the fd was opened with direct I/O — the alignment math
    /// and bounce-buffer REUSE it exercises are pure user-space logic. This lets the
    /// buffer-reuse test run on any filesystem (macOS/tmpfs/CI) WITHOUT needing a
    /// working direct-I/O syscall, which is frequently unavailable and probing it
    /// live is what stalled the prior implementer.
    #[cfg(test)]
    pub(crate) fn from_plain_fd_for_test(file: std::fs::File, len: u64) -> Self {
        Self {
            file: Arc::new(file),
            len,
        }
    }
}

#[cfg(unix)]
impl DirectReadAt {
    /// Aligned positioned read shared by the per-call [`ReadAt::read_at`] and the
    /// buffer-reusing [`ReadAt::read_exact_at_reusing`] paths (issue #2319).
    ///
    /// `bounce` is a slot for the aligned transfer buffer. When it already holds a
    /// buffer big enough for this read's aligned span, that buffer is REUSED
    /// (no allocation); otherwise a new one is allocated INTO the slot so a later
    /// read reuses it. `read_at` passes a throwaway local slot (per-call alloc, as
    /// before); the windowed scan passes its per-scan [`DirectScratch`] slot so one
    /// buffer serves every chunk read.
    fn read_at_into(
        &self,
        offset: u64,
        buf: &mut [u8],
        bounce: &mut Option<AlignedBuf>,
    ) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        if offset >= self.len || buf.is_empty() {
            return Ok(0);
        }
        let align = DIRECT_IO_ALIGN as u64;
        let aligned_off = (offset / align) * align;
        let head = (offset - aligned_off) as usize;
        // Round the aligned span up to cover [offset, offset+buf.len()).
        let want_end = offset
            .checked_add(buf.len() as u64)
            .ok_or_else(|| Error::corruption("direct read_at: offset+len overflow"))?
            .min(self.len);
        let span = (want_end - aligned_off) as usize;
        let aligned_len = span
            .checked_next_multiple_of(DIRECT_IO_ALIGN)
            .unwrap_or(usize::MAX & !(DIRECT_IO_ALIGN - 1));
        // Reuse the slot's buffer when it is already large enough; otherwise
        // (re)allocate and keep it for subsequent reads. `pread` fills only the
        // aligned span we ask for, so a larger reused buffer is fine — we read into
        // its leading `aligned_len` bytes.
        //
        // Grow GEOMETRICALLY (issue #2319 MEDIUM): a grow-to-exact `aligned_len`
        // reallocated on EVERY chunk whose aligned span exceeded the previous one —
        // for a windowed scan over progressively larger records that meant a realloc
        // per chunk, defeating the reuse this issue restores. Doubling the previous
        // capacity (bounded below by the required `aligned_len`) stabilizes the
        // buffer after a few growths. Both operands are multiples of
        // `DIRECT_IO_ALIGN` (`aligned_len` via `next_multiple_of`; a prior capacity
        // was itself allocated as such a multiple), so `new_cap` stays a 4096
        // multiple and the `O_DIRECT` alignment rule holds.
        let need_alloc = bounce
            .as_ref()
            .map(|b| b.capacity() < aligned_len)
            .unwrap_or(true);
        if need_alloc {
            let old_cap = bounce.as_ref().map(AlignedBuf::capacity).unwrap_or(0);
            let new_cap = aligned_len.max(old_cap.saturating_mul(2));
            *bounce = Some(AlignedBuf::new(new_cap, DIRECT_IO_ALIGN)?);
        }
        // SAFETY of unwrap avoidance: `bounce` is `Some` here (just set, or already
        // large enough). Match to keep the no-`unwrap` rule in library code.
        let slot = match bounce.as_mut() {
            Some(b) => b,
            None => return Err(Error::corruption("direct read_at: bounce buffer missing")),
        };
        let got = self
            .file
            .read_at(&mut slot.as_mut_slice()[..aligned_len], aligned_off)?;
        if got <= head {
            return Ok(0);
        }
        let usable = &slot.as_slice()[head..got];
        let n = usable.len().min(buf.len());
        buf[..n].copy_from_slice(&usable[..n]);
        Ok(n)
    }
}

#[cfg(unix)]
impl ReadAt for DirectReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // Per-call bounce buffer (no reuse) — the point-read path allocates one per
        // call, exactly as before issue #2319.
        let mut bounce: Option<AlignedBuf> = None;
        self.read_at_into(offset, buf, &mut bounce)
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn read_exact_at_reusing(
        &self,
        offset: u64,
        buf: &mut [u8],
        scratch: &mut DirectScratch,
    ) -> Result<()> {
        // Reuse the caller's per-scan aligned bounce buffer across every chunk read
        // (issue #2319). Loops `read_at_into` for exact semantics, threading the ONE
        // `scratch.bounce` buffer so no per-chunk aligned allocation occurs.
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = self.read_at_into(
                offset + filled as u64,
                &mut buf[filled..],
                &mut scratch.bounce,
            )?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "read_exact_at_reusing: reached EOF after {filled} of {} bytes at \
                         offset 0x{offset:x}",
                        buf.len()
                    ),
                )));
            }
            filled += n;
        }
        Ok(())
    }
}

/// A heap allocation whose start address is aligned to a requested boundary, as
/// required for `O_DIRECT` transfer buffers. Local to the point-read path so it
/// stays independent of the scan-cursor `AlignedBuf` in `source.rs`.
#[cfg(unix)]
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

// SAFETY: `AlignedBuf` uniquely owns its allocation for its whole lifetime; the
// raw pointer is never aliased or shared, so moving it across threads is sound.
#[cfg(unix)]
unsafe impl Send for AlignedBuf {}

/// Test-only counter of [`AlignedBuf`] allocations, so the issue #2319 reuse test
/// can PROVE the Direct windowed-scan read path allocates ONE aligned bounce buffer
/// across N chunk reads (reused) rather than one per read. Process-global; the
/// reuse test serializes on it and resets before measuring.
#[cfg(all(test, unix))]
pub(crate) static ALIGNED_BUF_ALLOCS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(unix)]
impl AlignedBuf {
    fn new(size: usize, align: usize) -> Result<Self> {
        #[cfg(test)]
        ALIGNED_BUF_ALLOCS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let size = size.max(align);
        let layout = std::alloc::Layout::from_size_align(size, align)
            .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;
        // SAFETY: `layout` has non-zero size (size >= align >= 1).
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(raw).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "aligned alloc failed",
            ))
        })?;
        Ok(Self { ptr, layout })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `layout.size()` initialised bytes (zeroed on
        // alloc, then overwritten by reads) and borrowed immutably here.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid for `layout.size()` bytes and uniquely borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }

    /// The number of aligned bytes this buffer can hold — used to decide whether a
    /// reused buffer is big enough for the next read's aligned span (issue #2319).
    fn capacity(&self) -> usize {
        self.layout.size()
    }
}

#[cfg(unix)]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` are exactly the values from the matching
        // `alloc_zeroed`, freed exactly once here.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn temp_file(bytes: &[u8], tag: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("cqlite_readat_{}_{}.bin", tag, std::process::id()));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn plain_file_positioned_reads_return_their_own_bytes() {
        let data: Vec<u8> = (0..=255u8).cycle().take(9000).collect();
        let path = temp_file(&data, "plain");
        let src = PlainFileReadAt::open(&path, data.len() as u64).unwrap();

        let mut a = [0u8; 8];
        src.read_exact_at(100, &mut a).unwrap();
        assert_eq!(&a, &data[100..108]);

        let mut b = [0u8; 8];
        src.read_exact_at(5000, &mut b).unwrap();
        assert_eq!(&b, &data[5000..5008]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn mmap_positioned_reads_return_their_own_bytes() {
        let data: Vec<u8> = (0..=255u8).cycle().take(9000).collect();
        let path = temp_file(&data, "mmap");
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: read-only map of a file we exclusively control in this test.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
        let src = MmapReadAt::new(Arc::new(mmap));

        let mut a = [0u8; 8];
        src.read_exact_at(100, &mut a).unwrap();
        assert_eq!(&a, &data[100..108]);
        let mut b = [0u8; 8];
        src.read_exact_at(5000, &mut b).unwrap();
        assert_eq!(&b, &data[5000..5008]);
        std::fs::remove_file(&path).ok();
    }

    /// Interleaved concurrent positioned reads at different offsets each return
    /// their own bytes — no cross-contamination from the other read's position
    /// (there is no position). Proves the `&self`, no-shared-cursor contract.
    #[test]
    fn interleaved_concurrent_reads_do_not_cross_contaminate() {
        let data: Vec<u8> = (0..=255u8).cycle().take(1 << 16).collect();
        let path = temp_file(&data, "interleave");
        let src = Arc::new(PlainFileReadAt::open(&path, data.len() as u64).unwrap());

        let barrier = Arc::new(Barrier::new(2));
        let off_a = 111u64;
        let off_b = 40000u64;
        let expect_a = data[off_a as usize..off_a as usize + 32].to_vec();
        let expect_b = data[off_b as usize..off_b as usize + 32].to_vec();

        let (s1, b1) = (Arc::clone(&src), Arc::clone(&barrier));
        let t = std::thread::spawn(move || {
            let mut acc = Vec::new();
            b1.wait();
            for _ in 0..200 {
                let mut buf = [0u8; 32];
                s1.read_exact_at(off_a, &mut buf).unwrap();
                acc.push(buf.to_vec());
            }
            acc
        });

        barrier.wait();
        for _ in 0..200 {
            let mut buf = [0u8; 32];
            src.read_exact_at(off_b, &mut buf).unwrap();
            assert_eq!(buf.to_vec(), expect_b, "offset B read got A's/other bytes");
        }
        for got in t.join().unwrap() {
            assert_eq!(got, expect_a, "offset A read got B's/other bytes");
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_exact_at_past_eof_is_unexpected_eof() {
        let data = b"abc";
        let path = temp_file(data, "eof");
        let src = PlainFileReadAt::open(&path, data.len() as u64).unwrap();
        let mut buf = [0u8; 8];
        let err = src.read_exact_at(1, &mut buf).unwrap_err();
        match err {
            Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    /// The `O_DIRECT` backend's per-call 4K alignment math (`aligned_off`/`head`/
    /// `want_end`/`checked_next_multiple_of`), aligned bounce buffer, and
    /// short-read/EOF handling must return EXACTLY the bytes a plain positioned
    /// read would, at every awkward alignment: offset 0, an unaligned offset, an
    /// offset spanning a 4K block boundary, and a partial final block at/near EOF.
    /// `PlainFileReadAt` over the same file is the oracle. Guarded `#[cfg(unix)]`
    /// and SKIPS GRACEFULLY when the temp filesystem refuses `O_DIRECT` (common on
    /// tmpfs/overlayfs) — mirroring `source::direct_cursor_reads_and_seeks`.
    #[cfg(unix)]
    #[test]
    // Shares the process-global `ALIGNED_BUF_ALLOCS` counter with the #2319 reuse
    // test, so both run serially to keep that counter's measurements isolated.
    #[serial_test::serial(aligned_buf_allocs)]
    fn direct_read_at_matches_plain_oracle_at_boundaries() {
        // File size deliberately NOT a 4096 multiple → forces a partial final
        // block and non-trivial `aligned_len` rounding.
        let len = DIRECT_IO_ALIGN * 2 + 1234;
        let mut data = vec![0u8; len];
        // Deterministic pseudo-random content (LCG) so mismatches are meaningful.
        let mut x = 0x1234_5678u32;
        for b in data.iter_mut() {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *b = (x >> 24) as u8;
        }
        let path = temp_file(&data, "direct");
        let oracle = PlainFileReadAt::open(&path, data.len() as u64).unwrap();

        // Open may fail on filesystems without O_DIRECT support → skip cleanly.
        let direct = match DirectReadAt::open(&path, data.len() as u64) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("O_DIRECT open unsupported here ({e}); skipping DirectReadAt test");
                std::fs::remove_file(&path).ok();
                return;
            }
        };
        // Some filesystems accept the O_DIRECT open but refuse aligned reads
        // (EINVAL) → probe once and skip cleanly rather than fail.
        let mut probe = [0u8; 16];
        if let Err(e) = direct.read_at(0, &mut probe) {
            eprintln!("O_DIRECT read unsupported here ({e}); skipping DirectReadAt test");
            std::fs::remove_file(&path).ok();
            return;
        }

        // (offset, length) pairs exercising each alignment corner.
        let align = DIRECT_IO_ALIGN as u64;
        let cases: &[(u64, usize)] = &[
            (0, 100),                  // offset 0, within first block
            (37, 200),                 // unaligned, within first block
            (align - 10, 50),          // spans the first 4K boundary
            (align, DIRECT_IO_ALIGN),  // aligned start, spans into next block
            (align + 7, 5000),         // unaligned start crossing multiple blocks
            ((len - 300) as u64, 300), // exact final bytes (partial final block)
            ((len - 10) as u64, 5),    // near EOF within the partial final block
        ];
        for &(off, n) in cases {
            let mut want = vec![0u8; n];
            oracle.read_exact_at(off, &mut want).unwrap();
            let mut got = vec![0u8; n];
            direct.read_exact_at(off, &mut got).unwrap();
            assert_eq!(got, want, "DirectReadAt mismatch at off={off} n={n}");
        }

        // Short read at EOF: request more than remains → exactly the tail, and
        // byte-for-byte identical to the oracle's short read.
        let tail_off = (len - 100) as u64;
        let mut big = vec![0u8; 500];
        let got_n = direct.read_at(tail_off, &mut big).unwrap();
        let mut oracle_big = vec![0u8; 500];
        let oracle_n = oracle.read_at(tail_off, &mut oracle_big).unwrap();
        assert_eq!(got_n, oracle_n, "short-read length must match oracle");
        assert_eq!(got_n, 100, "should read exactly the 100 remaining bytes");
        assert_eq!(&big[..got_n], &oracle_big[..oracle_n]);

        // Fully past EOF → 0 bytes (matches File/pread and MmapReadAt).
        let mut none = [0u8; 32];
        assert_eq!(direct.read_at(len as u64, &mut none).unwrap(), 0);
        assert_eq!(direct.read_at(len as u64 + align, &mut none).unwrap(), 0);

        // read_exact_at fully past EOF → UnexpectedEof, consistent with the
        // other backends.
        let err = direct.read_exact_at(len as u64, &mut none).unwrap_err();
        match err {
            Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }

        std::fs::remove_file(&path).ok();
    }

    /// Issue #2319: the Direct-I/O windowed-scan read path REUSES one aligned bounce
    /// buffer across chunk reads instead of allocating a fresh ~chunk-sized aligned
    /// buffer per read (the per-chunk-alloc regression #1940 introduced).
    ///
    /// This asserts the ALLOCATION BEHAVIOUR of the code path -- the pure user-space
    /// alignment + bounce-buffer logic -- so it needs NO working `O_DIRECT` syscall
    /// (frequently unavailable on macOS/tmpfs/CI; probing it live is what stalled a
    /// prior implementer). `DirectReadAt::from_plain_fd_for_test` runs that exact
    /// path over a regular fd; the bounce buffer, alignment math, and reuse decision
    /// are identical whether or not the fd was opened with direct I/O.
    ///
    /// The three scenarios live in ONE `#[test]` function on purpose: they share the
    /// process-global [`ALIGNED_BUF_ALLOCS`] counter (serialized on `aligned_buf_allocs`)
    /// and folding them here keeps the crate's test-function COUNT identical to before
    /// #2319, so the in-process test parallelism -- and thus the timing of unrelated
    /// process-global work-counter tests -- is unperturbed.
    ///
    /// Scenario 1 (equal-sized chunks -- reuse vs per-call contrast):
    /// - The OLD per-chunk path (`read_at`, still the point-read behaviour) allocates
    ///   ONE aligned buffer PER read -> `N` allocs for `N` reads (> 1). This is the
    ///   regression state the reuse path fixes; the assertion below fails on it.
    /// - The FIXED windowed path (`read_exact_at_reusing` with a shared
    ///   [`DirectScratch`]) allocates the buffer ONCE and reuses it -> exactly 1 alloc
    ///   across all `N` reads, in steady state.
    ///
    /// Scenario 2 (increasing sizes -- geometric growth, issue #2319 MEDIUM): with the
    /// reused scratch fed reads of STRICTLY INCREASING aligned span, the buffer grows
    /// GEOMETRICALLY -- a small constant number of reallocations, NOT one per read. The
    /// pre-fix grow-to-exact code reallocated on every size increase, preserving the
    /// very per-chunk-alloc regression #2319 removes; scenario 1 cannot catch it
    /// because its span never grows.
    ///
    /// Scenario 3 (Arc-dyn forward -- wiring evidence): production reaches the reuse
    /// path through an `Arc<dyn ReadAt>` (`point_source`), whose blanket-impl forward
    /// ([`ReadAt for Arc<T>`]) must delegate `read_exact_at_reusing` to the concrete
    /// `DirectReadAt` override. If that forward hop were dropped the `Arc` would fall
    /// back to the per-chunk-alloc default and production would regress while a
    /// concrete-typed assertion stayed green -- so this exercises reuse THROUGH the dyn
    /// `Arc` and asserts a single allocation.
    ///
    /// Serialized on the process-global [`ALIGNED_BUF_ALLOCS`] counter.
    #[cfg(unix)]
    #[test]
    #[serial_test::serial(aligned_buf_allocs)]
    fn direct_windowed_read_reuses_one_bounce_buffer_across_chunks() {
        use std::sync::atomic::Ordering;

        // Deterministic pseudo-random content (LCG) so mismatches are meaningful.
        fn fill(data: &mut [u8], mut x: u32) {
            for b in data.iter_mut() {
                x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *b = (x >> 24) as u8;
            }
        }

        // ---- Scenario 1: equal-sized chunks, reuse vs per-call contrast ----
        // 8 aligned, equal-sized "chunks" so every read's aligned span is identical
        // (`head == 0`, span == chunk) -> the reused buffer never needs to grow, so a
        // reused buffer means EXACTLY one allocation across all reads.
        let chunk = DIRECT_IO_ALIGN; // 4096
        let n_chunks = 8usize;
        let len = chunk * n_chunks;
        let mut data = vec![0u8; len];
        fill(&mut data, 0x9e37_79b9);
        let path = temp_file(&data, "reuse");
        let file = std::fs::File::open(&path).unwrap();
        let direct = DirectReadAt::from_plain_fd_for_test(file, len as u64);

        // FIXED windowed path: one shared scratch reused across every chunk read.
        ALIGNED_BUF_ALLOCS.store(0, Ordering::Relaxed);
        let mut scratch = DirectScratch::new();
        for i in 0..n_chunks {
            let off = (i * chunk) as u64;
            let mut got = vec![0u8; chunk];
            direct
                .read_exact_at_reusing(off, &mut got, &mut scratch)
                .unwrap();
            assert_eq!(
                got,
                &data[i * chunk..(i + 1) * chunk],
                "reused-buffer read returned wrong bytes for chunk {i}"
            );
        }
        let reuse_allocs = ALIGNED_BUF_ALLOCS.load(Ordering::Relaxed);
        assert_eq!(
            reuse_allocs, 1,
            "#2319: the Direct windowed-scan path must allocate ONE aligned bounce buffer \
             and reuse it across all {n_chunks} chunk reads (got {reuse_allocs} allocs)"
        );

        // OLD per-chunk path: `read_at` allocates a fresh bounce buffer per call.
        // This is the regression the reuse path removes; it allocates once per read,
        // so `> 1` for `N > 1` -- the exact behaviour the assertion above rejects.
        ALIGNED_BUF_ALLOCS.store(0, Ordering::Relaxed);
        for i in 0..n_chunks {
            let off = (i * chunk) as u64;
            let mut got = vec![0u8; chunk];
            direct.read_at(off, &mut got).unwrap();
        }
        let percall_allocs = ALIGNED_BUF_ALLOCS.load(Ordering::Relaxed);
        assert_eq!(
            percall_allocs, n_chunks,
            "#2319 (regression witness): the per-call point-read path allocates one aligned \
             buffer PER read ({n_chunks} reads -> {n_chunks} allocs); the windowed scan must \
             NOT use it (got {percall_allocs})"
        );
        assert!(
            reuse_allocs < percall_allocs,
            "#2319: reuse ({reuse_allocs}) must allocate strictly fewer buffers than the \
             per-chunk path ({percall_allocs})"
        );
        std::fs::remove_file(&path).ok();

        // ---- Scenario 2: increasing sizes must grow GEOMETRICALLY (#2319 MEDIUM) ----
        // `M` reads at offset 0 of length `(i+1)*4096` need caps 4096, 8192, ... 32768.
        // - grow-to-exact: every cap strictly exceeds the last -> `M` allocs (== reads).
        // - geometric (max(need, old*2)): 4096->8192->16384->32768 -> ~log2(M) allocs,
        //   well under `M`. The bound below fails on the old code and passes on the fix.
        let n_reads = 8usize;
        let grow_len = chunk * n_reads; // largest read spans the whole file
        let mut grow_data = vec![0u8; grow_len];
        fill(&mut grow_data, 0x1357_9bdf);
        let grow_path = temp_file(&grow_data, "reuse_grow");
        let grow_file = std::fs::File::open(&grow_path).unwrap();
        let grow_direct = DirectReadAt::from_plain_fd_for_test(grow_file, grow_len as u64);

        ALIGNED_BUF_ALLOCS.store(0, Ordering::Relaxed);
        let mut grow_scratch = DirectScratch::new();
        for i in 0..n_reads {
            let want_len = (i + 1) * chunk; // strictly increasing aligned span
            let mut got = vec![0u8; want_len];
            grow_direct
                .read_exact_at_reusing(0, &mut got, &mut grow_scratch)
                .unwrap();
            assert_eq!(
                got,
                &grow_data[..want_len],
                "increasing-size reused-buffer read returned wrong bytes at read {i}"
            );
        }
        let grow_allocs = ALIGNED_BUF_ALLOCS.load(Ordering::Relaxed);
        // A few geometric growths, NOT one per read. Bound comfortably above the
        // ~log2(M)=4 growths yet strictly below `n_reads` so grow-to-exact (which
        // reallocates every read -> n_reads allocs) fails here.
        assert!(
            grow_allocs <= 5,
            "#2319: geometric growth must bound reallocations to a small constant across \
             {n_reads} increasing-size reads (got {grow_allocs} allocs)"
        );
        assert!(
            grow_allocs < n_reads,
            "#2319: growth must allocate strictly fewer times than {n_reads} reads -- \
             grow-to-exact reallocated on every size increase (got {grow_allocs})"
        );
        std::fs::remove_file(&grow_path).ok();

        // ---- Scenario 3: reuse must survive the Arc<dyn ReadAt> forward hop ----
        let arc_chunks = 6usize;
        let arc_len = chunk * arc_chunks;
        let mut arc_data = vec![0u8; arc_len];
        fill(&mut arc_data, 0x2468_ace0);
        let arc_path = temp_file(&arc_data, "reuse_arc");
        let arc_file = std::fs::File::open(&arc_path).unwrap();
        // Erase to the exact production type: an `Arc<dyn ReadAt>` (point_source).
        let src: Arc<dyn ReadAt> = Arc::new(DirectReadAt::from_plain_fd_for_test(
            arc_file,
            arc_len as u64,
        ));

        ALIGNED_BUF_ALLOCS.store(0, Ordering::Relaxed);
        let mut arc_scratch = DirectScratch::new();
        for i in 0..arc_chunks {
            let off = (i * chunk) as u64;
            let mut got = vec![0u8; chunk];
            // Goes Arc<dyn ReadAt> -> forward -> DirectReadAt::read_exact_at_reusing.
            src.read_exact_at_reusing(off, &mut got, &mut arc_scratch)
                .unwrap();
            assert_eq!(
                got,
                &arc_data[i * chunk..(i + 1) * chunk],
                "Arc-dyn reused-buffer read returned wrong bytes for chunk {i}"
            );
        }
        let arc_allocs = ALIGNED_BUF_ALLOCS.load(Ordering::Relaxed);
        assert_eq!(
            arc_allocs, 1,
            "#2319: the Arc<dyn ReadAt> forward must reach DirectReadAt's reuse override -- \
             exactly ONE aligned buffer across {arc_chunks} equal-sized chunk reads (got \
             {arc_allocs}); a dropped forward would fall back to the per-chunk-alloc default"
        );
        std::fs::remove_file(&arc_path).ok();
    }
}
