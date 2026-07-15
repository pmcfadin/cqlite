//! Issue #1396 (roborev Fix 1) — the BIG (`nb`) promoted-index / reverse-lookup
//! read path must verify uncompressed `CRC.db` chunks before parsing.
//!
//! [`SSTableReader::big_reverse_partition_rows`] is `pub(crate)`, so this proof
//! lives in-crate rather than in `tests/`. It routes through
//! `decompress_partition_window`, whose uncompressed read now flows through the
//! single CRC-checked accessor (`read_uncompressed_verified`). We prove BOTH:
//!
//! * the CLEAN uncompressed source drives the reverse path to `Ok(Some(rows))`
//!   (so the path is genuinely exercised, not short-circuited to `Ok(None)`), and
//! * the bit-flipped fixture (flip in Data.db chunk 1) returns a typed
//!   `Error::Corruption` naming that chunk — never `Ok(None)` / corrupt bytes.
//!
//! The `window_builder` submodule (issue #1869) adds unit-level proofs for the
//! COMPRESSED arm's window builder
//! ([`compressed_partition_window`](super::big_promoted::compressed_partition_window)),
//! driven DIRECTLY against hand-built `CompressionInfo` + in-memory `ReadAt` fixtures —
//! the SQL-roundtrip test above always calls the builder with the partition-start
//! offset (`within == 0`), so it structurally cannot reproduce the `within > 0` panic
//! classes those tests target.

use crate::storage::sstable::reader::SSTableReader;
use crate::{Config, Error, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// `pk INT, ck INT, body TEXT, PRIMARY KEY (pk, ck)` — the schema of the
/// `test_comp.uncompressed_table` wide partition the fixture derives from.
const SCHEMA_CQL: &str =
    "CREATE TABLE test_comp.uncompressed_table (pk int, ck int, body text, PRIMARY KEY (pk, ck))";

/// The single wide partition's key: `pk = INT 1` → 4-byte big-endian.
const PARTITION_KEY: &[u8] = &[0, 0, 0, 1];

const CORRUPT_DATA_DB: &str =
    "corruption/test_comp_corrupt/uncompressed_data_bit_flip/nb-1-big-Data.db";

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Whether the *corrupt corpus* is required to be present (hard-fail on absent),
/// as opposed to the generic fetched dataset (`require_fixtures`).
///
/// Issue #1799 (red main): the corrupt uncompressed fixture
/// (`corruption/test_comp_corrupt/uncompressed_data_bit_flip/nb-1-big-Data.db`)
/// is **not** part of the fetched dataset — its binaries are gitignored and are
/// only regenerated from a Cassandra container by the strict
/// `compression-corruption-parity` lane (`generate-corruption-corpus.sh`). The
/// generic `Core lib/doc tests` lane sets `CQLITE_REQUIRE_FIXTURES=1` for the
/// ~70 fetched-dataset tests but never provides this corpus, so gating this
/// test's hard assertion on `require_fixtures()` panicked deterministically
/// there. Gate the hard requirement on a *corpus-specific* flag that is set
/// only in the lane that actually regenerates the fixture; everywhere else an
/// absent corrupt fixture is a clean SKIP.
fn require_corrupt_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_CORRUPT_FIXTURES")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

fn corrupt_data_db() -> Option<PathBuf> {
    let path = datasets_root().map(|r| r.join(CORRUPT_DATA_DB));
    match path {
        Some(p) if p.exists() => Some(p),
        _ => {
            // The corrupt corpus is only regenerated in the strict
            // compression-corruption-parity lane (issue #1799); hard-fail only
            // when that lane explicitly requires it, otherwise SKIP clean.
            assert!(
                !require_corrupt_fixtures(),
                "CQLITE_REQUIRE_CORRUPT_FIXTURES=1 but the corrupt uncompressed fixture is \
                 absent: {CORRUPT_DATA_DB}"
            );
            eprintln!("SKIP: corrupt uncompressed fixture absent ({CORRUPT_DATA_DB}).");
            None
        }
    }
}

fn clean_source_data_db() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_comp");
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with("uncompressed_table-") {
            let candidate = entry.path().join("nb-1-big-Data.db");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

async fn open_reader(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("opening the structurally valid uncompressed fixture should succeed")
}

fn assert_typed_uncompressed_chunk_corruption(err: &Error) {
    assert!(
        matches!(err, Error::Corruption(_)),
        "uncompressed CRC mismatch must be Error::Corruption, got: {err}"
    );
    assert!(
        !err.is_recoverable(),
        "a bad chunk is non-recoverable, got recoverable: {err}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("chunk 1"),
        "corruption error must name the failing chunk ('chunk 1'), got: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("CRC"),
        "corruption error should identify the CRC mismatch, got: {msg}"
    );
}

/// The reverse path is genuinely exercised on the CLEAN source: it must resolve
/// the promoted index + partition window and return the partition's rows.
#[tokio::test]
async fn clean_reverse_partition_path_returns_rows() {
    let Some(path) = clean_source_data_db() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but the clean uncompressed_table source is absent"
        );
        eprintln!("SKIP: clean uncompressed_table source absent.");
        return;
    };
    let reader = open_reader(&path).await;
    let schema = crate::schema::parse_cql_schema(SCHEMA_CQL).expect("parse schema");

    let rows = reader
        .big_reverse_partition_rows(PARTITION_KEY, Some(&schema))
        .await
        .expect("clean reverse-partition read must succeed");
    let rows = rows.expect(
        "the reverse/promoted-index path must apply to this wide partition (Some), \
         otherwise the corrupt-fixture test below would not exercise the read",
    );
    assert!(
        !rows.is_empty(),
        "clean wide partition must yield rows via the reverse path"
    );
}

/// The bit-flipped fixture makes the promoted-index / reverse-lookup read of the
/// corrupt uncompressed chunk fail fast with a typed corruption error — never
/// `Ok(None)`, never corrupt bytes.
#[tokio::test]
async fn reverse_partition_path_over_corrupt_chunk_fails_fast() {
    let Some(path) = corrupt_data_db() else {
        return;
    };
    let reader = open_reader(&path).await;
    let schema = crate::schema::parse_cql_schema(SCHEMA_CQL).expect("parse schema");

    match reader
        .big_reverse_partition_rows(PARTITION_KEY, Some(&schema))
        .await
    {
        Ok(Some(rows)) => panic!(
            "FIXTURE ROT or read-path regression: reverse-partition read over the bit-flipped \
             uncompressed chunk returned Ok(Some) with {} rows; it must fail with corruption.",
            rows.len()
        ),
        Ok(None) => panic!(
            "reverse-partition read over the corrupt chunk returned Ok(None) — the CRC check was \
             bypassed / the read path silently swallowed the corruption."
        ),
        Err(err) => assert_typed_uncompressed_chunk_corruption(&err),
    }
}

/// Unit-level regression proofs for the COMPRESSED arm of the BIG (`nb`)
/// clustering/reverse seek window builder
/// ([`compressed_partition_window`](super::big_promoted::compressed_partition_window)),
/// issue #1869.
///
/// These drive the window builder DIRECTLY against hand-built `CompressionInfo` +
/// in-memory `ReadAt` fixtures — no full write-engine + compressing-writer + SQL
/// roundtrip. That matters: the SQL-roundtrip regression test in this crate always
/// called the builder with the PARTITION-START offset (`within == 0`), so it
/// structurally could not reproduce the `within > 0` panic classes the fix addresses.
///
/// What each test would do on the PRE-fix code:
///   * `out_of_range_chunk_with_within_gt0_errs` — up-front out-of-range guard.
///     Without it the loop `break`s on the first EOF, leaving `window` empty while
///     `within > 0` → caller `&window[within..]` PANICS.
///   * `end_bound_before_window_base_errs` — Issue 2: `needed == 0` so the loop body
///     never runs, `window` stays empty, the up-front guard does NOT fire (chunk in
///     range) → pre-fix returns `Ok(Some((<empty>, within)))` → caller PANICS. The
///     choke-point guard now returns `Err`.
///   * `incompressible_raw_chunk_round_trips` — Issue 3: a RAW (incompressible) chunk
///     stored uncompressed by Cassandra. Pre-fix it was force-fed to the LZ4
///     decompressor → spurious `Error::corruption`. The raw-chunk fallback now returns
///     the plaintext verbatim.
mod window_builder {
    use super::super::big_promoted::compressed_partition_window;
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

    /// Lay out `payloads` as consecutive `[payload][big-endian CRC32]` records —
    /// exactly the on-disk compressed-chunk framing `read_compressed_chunk_at` verifies
    /// — and return the file bytes plus the per-chunk absolute offsets.
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

    /// Issue 1 (genuine regression): a resolved chunk index PAST the last recorded
    /// chunk, with a NON-chunk-aligned offset so `within > 0`, must FAIL CLOSED — not
    /// panic. `offset = 150`, `chunk_length = 100` ⇒ `target_chunk = 1`,
    /// `window_base = 100`, `within = 50`. Only ONE chunk is recorded, so
    /// `target_chunk (1) >= chunk_offsets.len() (1)` and the up-front guard fires.
    /// Pre-fix (before the guard) the stitch loop would `break` on the first EOF, hand
    /// back an EMPTY window with `within = 50`, and `&window[50..]` would panic.
    #[test]
    fn out_of_range_chunk_with_within_gt0_errs() {
        let (file, offsets) = build_chunked_file(&[b"only-one-chunk".to_vec()]);
        let ci = comp_info(100, i32::MAX as u32, 100_000, offsets);
        let src = MemReadAt(file.clone());

        let err = compressed_partition_window(&src, &ci, None, file.len() as u64, 150, Some(300))
            .expect_err("out-of-range target_chunk with within>0 must fail closed, not panic");
        match err {
            Error::Corruption(m) => {
                assert!(
                    m.contains("out of range"),
                    "unexpected corruption text: {m}"
                )
            }
            other => panic!("expected Corruption(out of range), got {other:?}"),
        }
    }

    /// Issue 2 (residual panic path, found by BOTH reviewers): a non-monotonic /
    /// corrupt end bound where `end_offset <= window_base < offset`. Then
    /// `needed == 0`, so the stitch loop body NEVER runs and `window` stays empty, yet
    /// `target_chunk` is perfectly IN range (up-front guard does NOT fire) and
    /// `within > 0`. `offset = 150`, `chunk_length = 100` ⇒ `target_chunk = 1`,
    /// `within = 50`; TWO chunks recorded; `end_bound = Some(50)` with
    /// `50 <= window_base (100)`. Pre-fix returned `Ok(Some((<empty>, 50)))` and the
    /// caller's `&window[50..]` panicked; the choke-point guard now returns `Err`.
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

    /// A well-formed full-partition read (offset 0, `within == 0`) still round-trips —
    /// the choke-point guard must NOT reject a legitimate case. Two LZ4-compressed
    /// chunks.
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
        assert_eq!(
            window, expected,
            "round-tripped window must equal plaintext"
        );
    }

    /// Issue 3 (real correctness gap on real Cassandra data): a multi-chunk partition
    /// whose MIDDLE chunk was stored RAW (Cassandra writes a chunk uncompressed when
    /// its would-be compressed size meets/exceeds `max_compressed_length`). The
    /// migrated consolidated loop must detect the raw chunk via the same
    /// `compressed.len() >= max_compressed_length` test the sibling
    /// `read_compressed_offset_window` uses, and NOT force it through the decompressor.
    ///
    /// Layout: chunk0 = LZ4(plain0), chunk1 = RAW plain1 (64 B, stored uncompressed),
    /// chunk2 = LZ4(plain2); `max_compressed_length = 64`. The LZ4 chunks compress well
    /// (< 64 B stored) → decompressed; the raw chunk's stored len is 64 (>= 64) → taken
    /// verbatim. Pre-fix, chunk1's raw bytes were LZ4-decoded and the fabricated
    /// 0xFFFFFFFF size-prefix tripped the decompression-bomb guard → spurious
    /// corruption.
    #[cfg(feature = "lz4")]
    #[test]
    #[serial_test::serial]
    fn incompressible_raw_chunk_round_trips() {
        use crate::storage::sstable::compression::{Compression, CompressionAlgorithm};

        let c = Compression::new(CompressionAlgorithm::Lz4).expect("lz4");
        let plain0 = vec![0u8; 64];
        let plain2 = vec![0x5Au8; 64];
        // A genuinely-incompressible 64-byte chunk. Its first 4 bytes are 0xFF so that,
        // if ever fed to the LZ4 decoder, the little-endian size prefix (u32::MAX)
        // exceeds the 128 MiB decompression-bomb cap and errors — making the raw
        // fallback load-bearing.
        let mut plain1 = vec![0xFFu8; 4];
        plain1.extend((0..60u8).map(|i| i.wrapping_mul(37).wrapping_add(11)));
        assert_eq!(plain1.len(), 64);

        let comp0 = c.compress(&plain0).unwrap();
        let comp2 = c.compress(&plain2).unwrap();
        assert!(
            comp0.len() < 64 && comp2.len() < 64,
            "LZ4 chunks must be < max_compressed_length"
        );

        // The raw chunk stored verbatim (Cassandra's incompressible path).
        let (file, offsets) = build_chunked_file(&[comp0, plain1.clone(), comp2]);
        let ci = comp_info(64, 64, 192, offsets);
        let src = MemReadAt(file.clone());

        // Guard the guard: without the fallback, this raw chunk WOULD fail LZ4 decode.
        assert!(
            c.decompress(&plain1).is_err(),
            "fixture invariant: raw chunk must be undecodable as LZ4 (proves fallback \
             is load-bearing)"
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
}
