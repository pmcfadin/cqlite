//! Unit-level regression proofs for the COMPRESSED arm of the BIG (`nb`)
//! clustering/reverse seek window builder
//! ([`compressed_partition_window`](super::big_promoted::compressed_partition_window)),
//! issue #1869.
//!
//! These drive the window builder DIRECTLY against hand-built `CompressionInfo` +
//! in-memory `ReadAt` fixtures — no full write-engine + compressing-writer + SQL
//! roundtrip. That matters: the pre-#1869 SQL-roundtrip regression test always called
//! the builder with the PARTITION-START offset (`within == 0`), so it structurally
//! could not reproduce the `within > 0` panic classes the fix addresses. Each test
//! here constructs the exact `(offset, end_bound, chunk layout)` that triggers a
//! specific panic/corruption path and asserts a typed `Err` (never a panic, never a
//! silently-short `Ok`) or a correct round-trip.
//!
//! What each test would do on the PRE-fix code:
//!   * `out_of_range_chunk_with_within_gt0_errs` — up-front out-of-range guard
//!     (restored by faf9dcad4). Without it the loop `break`s on the first EOF signal,
//!     leaving `window` empty while `within > 0` → caller `&window[within..]` PANICS.
//!   * `end_bound_before_window_base_errs` — Issue 2: `needed == 0` so the loop body
//!     never runs, `window` stays empty, the up-front guard does NOT fire (chunk in
//!     range) → pre-fix returns `Ok(Some((<empty>, within)))` → caller PANICS. The
//!     choke-point guard now returns `Err`.
//!   * `incompressible_raw_chunk_round_trips` — Issue 3: a RAW (incompressible) chunk
//!     stored uncompressed by Cassandra. Pre-fix it was force-fed to the LZ4
//!     decompressor → spurious `Error::corruption`. The raw-chunk fallback now returns
//!     the plaintext verbatim.

#![cfg(all(test, not(feature = "tombstones")))]

use super::big_promoted::compressed_partition_window;
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::read_at::ReadAt;
use crate::{Error, Result};

/// In-memory [`ReadAt`] over a byte buffer (mirrors block_io's `MemReadAt`).
struct MemReadAt(Vec<u8>);
impl ReadAt for MemReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let start = offset as usize;
        if start >= self.0.len() {
            return Ok(0);
        }
        let avail = &self.0[start..];
        let n = avail.len().min(buf.len());
        buf[..n].copy_from_slice(&avail[..n]);
        Ok(n)
    }
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}

/// Lay out `payloads` as consecutive `[payload][big-endian CRC32]` records — exactly
/// the on-disk compressed-chunk framing `read_compressed_chunk_at` verifies — and
/// return the file bytes plus the per-chunk absolute offsets.
fn build_chunked_file(payloads: &[Vec<u8>]) -> (Vec<u8>, Vec<u64>) {
    let mut file = Vec::new();
    let mut offsets = Vec::with_capacity(payloads.len());
    for p in payloads {
        offsets.push(file.len() as u64);
        file.extend_from_slice(p);
        file.extend_from_slice(&crc32fast::hash(p).to_be_bytes());
    }
    (file, offsets)
}

fn comp_info(
    chunk_length: u32,
    max_compressed_length: u32,
    data_length: u64,
    chunk_offsets: Vec<u64>,
) -> CompressionInfo {
    CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        option_pairs: vec![],
        chunk_length,
        max_compressed_length,
        data_length,
        chunk_offsets,
    }
}

/// Issue 1 (genuine regression): a resolved chunk index PAST the last recorded chunk,
/// with a NON-chunk-aligned offset so `within > 0`, must FAIL CLOSED — not panic.
///
/// `offset = 150`, `chunk_length = 100` ⇒ `target_chunk = 1`, `window_base = 100`,
/// `within = 50`. Only ONE chunk is recorded, so `target_chunk (1) >= chunk_offsets.len()
/// (1)` and the up-front guard fires. Pre-fix (before the guard) the stitch loop would
/// `break` on the first EOF, hand back an EMPTY window with `within = 50`, and the
/// caller's `&window[50..]` would slice-index-panic on the empty vec.
#[test]
fn out_of_range_chunk_with_within_gt0_errs() {
    let (file, offsets) = build_chunked_file(&[b"only-one-chunk".to_vec()]);
    let ci = comp_info(100, i32::MAX as u32, 100_000, offsets);
    let src = MemReadAt(file.clone());

    let err = compressed_partition_window(&src, &ci, None, file.len() as u64, 150, Some(300))
        .expect_err("out-of-range target_chunk with within>0 must fail closed, not panic");
    match err {
        Error::Corruption(m) => {
            assert!(m.contains("out of range"), "unexpected corruption text: {m}")
        }
        other => panic!("expected Corruption(out of range), got {other:?}"),
    }
}

/// Issue 2 (residual panic path, found by BOTH reviewers): a non-monotonic / corrupt
/// end bound where `end_offset <= window_base < offset`. Then `needed == 0`, so the
/// stitch loop body NEVER runs and `window` stays empty, yet `target_chunk` is
/// perfectly IN range (so the up-front guard does NOT fire) and `within > 0`.
///
/// `offset = 150`, `chunk_length = 100` ⇒ `target_chunk = 1`, `within = 50`. TWO
/// chunks are recorded (so chunk 1 is in range). `end_bound = Some(50)` with
/// `50 <= window_base (100)`. Pre-fix this returned `Ok(Some((<empty>, 50)))` and the
/// caller's `&window[50..]` panicked; the single choke-point guard now returns `Err`.
#[test]
fn end_bound_before_window_base_errs() {
    let (file, offsets) = build_chunked_file(&[b"chunk-zero".to_vec(), b"chunk-one".to_vec()]);
    let ci = comp_info(100, i32::MAX as u32, 100_000, offsets);
    let src = MemReadAt(file.clone());

    let err = compressed_partition_window(&src, &ci, None, file.len() as u64, 150, Some(50))
        .expect_err("end_bound <= window_base (needed==0) with within>0 must fail closed");
    match err {
        Error::Corruption(m) => assert!(
            m.contains("shorter than the required intra-window offset"),
            "unexpected corruption text: {m}"
        ),
        other => panic!("expected Corruption(short window), got {other:?}"),
    }
}

/// A well-formed full-partition read (offset 0, `within == 0`) still round-trips — the
/// choke-point guard must NOT reject a legitimate case. Two LZ4-compressed chunks.
#[cfg(feature = "lz4")]
#[test]
#[serial_test::serial]
fn valid_compressed_window_round_trips() {
    use crate::storage::sstable::compression::{Compression, CompressionAlgorithm};

    let c = Compression::new(CompressionAlgorithm::Lz4).expect("lz4");
    let plain0 = vec![0u8; 64];
    let plain1 = vec![0x5Au8; 64];
    let (file, offsets) =
        build_chunked_file(&[c.compress(&plain0).unwrap(), c.compress(&plain1).unwrap()]);
    // max_compressed_length large so both stored chunks are treated as compressed.
    let ci = comp_info(64, i32::MAX as u32, 128, offsets);
    let src = MemReadAt(file.clone());

    let (window, within) =
        compressed_partition_window(&src, &ci, Some(&c), file.len() as u64, 0, Some(128))
            .expect("valid read ok")
            .expect("valid read present");
    assert_eq!(within, 0);
    let mut expected = plain0.clone();
    expected.extend_from_slice(&plain1);
    assert_eq!(window, expected, "round-tripped window must equal plaintext");
}

/// Issue 3 (real correctness gap on real Cassandra data): a multi-chunk partition whose
/// MIDDLE chunk was stored RAW (Cassandra writes a chunk uncompressed when its would-be
/// compressed size meets/exceeds `max_compressed_length`). The migrated consolidated
/// loop must detect the raw chunk via the same `compressed.len() >= max_compressed_length`
/// test the sibling `read_compressed_offset_window` uses, and NOT force it through the
/// decompressor.
///
/// Layout: chunk0 = LZ4(plain0), chunk1 = RAW plain1 (64 B, stored uncompressed),
/// chunk2 = LZ4(plain2); `max_compressed_length = 64`. The LZ4 chunks compress well
/// (< 64 B stored) → decompressed; the raw chunk's stored len is 64 (>= 64) → taken
/// verbatim. Pre-fix, chunk1's raw bytes were LZ4-decoded and the fabricated 0xFFFFFFFF
/// size-prefix (see below) tripped the decompression-bomb guard → spurious corruption.
#[cfg(feature = "lz4")]
#[test]
#[serial_test::serial]
fn incompressible_raw_chunk_round_trips() {
    use crate::storage::sstable::compression::{Compression, CompressionAlgorithm};

    let c = Compression::new(CompressionAlgorithm::Lz4).expect("lz4");
    let plain0 = vec![0u8; 64];
    let plain2 = vec![0x5Au8; 64];
    // A genuinely-incompressible 64-byte chunk. Its first 4 bytes are 0xFF so that, if
    // ever fed to the LZ4 decoder, the little-endian size prefix (u32::MAX) exceeds the
    // 128 MiB decompression-bomb cap and errors — making the raw fallback load-bearing.
    let mut plain1 = vec![0xFFu8; 4];
    plain1.extend((0..60u8).map(|i| i.wrapping_mul(37).wrapping_add(11)));
    assert_eq!(plain1.len(), 64);

    let comp0 = c.compress(&plain0).unwrap();
    let comp2 = c.compress(&plain2).unwrap();
    assert!(comp0.len() < 64 && comp2.len() < 64, "LZ4 chunks must be < max_compressed_length");

    // The raw chunk stored verbatim (Cassandra's incompressible path).
    let (file, offsets) = build_chunked_file(&[comp0, plain1.clone(), comp2]);
    let ci = comp_info(64, 64, 192, offsets);
    let src = MemReadAt(file.clone());

    // Guard the guard: without the fallback, this raw chunk WOULD fail LZ4 decode.
    assert!(
        c.decompress(&plain1).is_err(),
        "fixture invariant: raw chunk must be undecodable as LZ4 (proves fallback is load-bearing)"
    );

    let (window, within) =
        compressed_partition_window(&src, &ci, Some(&c), file.len() as u64, 0, Some(192))
            .expect("raw-chunk read must succeed via the incompressible fallback")
            .expect("window present");
    assert_eq!(within, 0);
    let mut expected = plain0.clone();
    expected.extend_from_slice(&plain1);
    expected.extend_from_slice(&plain2);
    assert_eq!(
        window, expected,
        "raw middle chunk must be taken verbatim; compressed chunks decompressed"
    );
}
