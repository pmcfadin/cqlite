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
//! | [`PlainFileReadAt`] | `FileExt::read_at` (Unix) / `seek_read` (Windows) on ONE shared `File` | Positioned reads on a shared fd are independent — no lock. |
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
/// Wraps ONE shared `std::fs::File`. On Unix, `FileExt::read_at` issues a `pread`
/// that does not touch the fd's offset, so concurrent reads on this single handle
/// are independent — no lock, no per-lookup `open(2)`. Opened once at reader open
/// and shared for the reader's lifetime (design.md Decision 3).
pub(crate) struct PlainFileReadAt {
    file: Arc<std::fs::File>,
    len: u64,
}

impl PlainFileReadAt {
    /// Wrap an already-open file handle whose length is `len`.
    pub(crate) fn new(file: std::fs::File, len: u64) -> Self {
        Self {
            file: Arc::new(file),
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
        // Windows has no `pread`; `seek_read` is the platform equivalent. It DOES
        // move the handle's file pointer, so concurrent callers on one handle can
        // race the cursor — acceptable here because the correctness scenarios run
        // on the Unix backends in CI (design.md Risks: "Windows drift"). Windows
        // builds that need concurrent point reads would front this with per-caller
        // handles; not required for the Cassandra-5.0 read floor CQLite targets.
        use std::os::windows::fs::FileExt;
        Ok(self.file.seek_read(buf, offset)?)
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
}

#[cfg(unix)]
impl ReadAt for DirectReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
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
        let mut bounce = AlignedBuf::new(aligned_len, DIRECT_IO_ALIGN)?;
        let got = self.file.read_at(bounce.as_mut_slice(), aligned_off)?;
        if got <= head {
            return Ok(0);
        }
        let usable = &bounce.as_slice()[head..got];
        let n = usable.len().min(buf.len());
        buf[..n].copy_from_slice(&usable[..n]);
        Ok(n)
    }

    fn len(&self) -> u64 {
        self.len
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

#[cfg(unix)]
impl AlignedBuf {
    fn new(size: usize, align: usize) -> Result<Self> {
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
}
