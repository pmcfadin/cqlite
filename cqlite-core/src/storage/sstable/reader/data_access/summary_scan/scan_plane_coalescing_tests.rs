//! COMBINED-INTERACTION regression: the #2877 coalescing window must issue every
//! one of its reads on the #2876 SCAN plane, and none on the `MADV_RANDOM` point
//! plane.
//!
//! # Why this test exists at all (neither PR's own tests can see the failure)
//!
//! Two fixes land back-to-back on the same code path:
//!
//! - **#2876** split the read intents. The Summary-guided compressed scan walk was
//!   reading `Data.db` through `point_source`, the mapping deliberately advised
//!   `MADV_RANDOM` (issue #2210) to suppress kernel readahead for scattered point
//!   faults. On a forward walk that advice is exactly backwards — ~one 4 KiB fault
//!   per partition instead of a readahead window. Scan-shaped walks now pass the
//!   reader's never-`MADV_RANDOM` `scan_positional_source`.
//! - **#2877** coalesces that walk's reads per CHUNK rather than per PARTITION,
//!   with a doubling ramp up to a 4 MiB window.
//!
//! Each fix's own tests are blind to the interaction. #2876's tests count reads
//! per plane but say nothing about read SIZE, so they stay green if the window
//! collapses back to per-partition reads. #2877's tests count refills/decompress
//! calls but are plane-agnostic, so they stay green if every widened read is
//! issued on the advised point plane — silently reinstating #2876's field
//! regression, because a bigger `pread` into a `MADV_RANDOM` mapping still gets no
//! readahead. Green + green would then equal a reintroduced regression.
//!
//! **CASSANDRA-15452 is the upstream precedent for exactly this failure mode.**
//! Cassandra's fix for the same problem was a userspace scan-only readahead buffer
//! *with no madvise*, and they discovered that an enabled ChunkCache DEFEATED it:
//! the userspace buffer was correct in isolation while the layer underneath
//! silently negated it. A userspace coalescing window is only worth its complexity
//! if it reads a plane that actually reads ahead. That is a property of the PAIR,
//! so it needs a test of the pair.
//!
//! # Why in-crate (`src/`) and not `tests/`
//!
//! The per-plane spies are the only way to make this assertion, and they require
//! `SSTableReader::{clone,set}_point_source` / `{clone,set}_scan_positional_source`
//! plus
//! [`SleepingReadAt`](crate::storage::sstable::reader::read_at::SleepingReadAt) —
//! all `#[cfg(test)]` + `pub(crate)`, so structurally unreachable from an
//! integration test (`tests/` compiles the library without its `test` cfg). This
//! mirrors `reader::read_at_point_tests`, which lives in-crate for the same
//! reason. The sibling integration file
//! `cqlite-core/tests/issue_2877_scan_chunk_coalescing.rs` keeps the
//! public-surface coalescing oracles (refills / decompress calls) that need no
//! private surface.
//!
//! It is declared from `summary_scan` (the walk it tests) rather than beside
//! `reader::read_at_point_tests`, because `reader/mod.rs` is already over the
//! campsite-rule source threshold and even a 6-line `mod` declaration there trips
//! the gate's growth ratchet (epic #1116). Declaring it next to the code under
//! test costs nothing and keeps the ratchet green without an override.
//!
//! # What is asserted, and why each assert cannot be satisfied by a regression
//!
//! One scan, three independent properties, all measured from the SAME spied run:
//!
//! 1. **Plane routing.** `scan_reads > 0` AND `point_reads == 0`. The positive half
//!    is asserted DIRECTLY, never inferred from the point plane's silence: the
//!    full-ring fallback (`stream_all_partitions_for_compaction`) reads through
//!    `BlockSource`, which is on NEITHER spied plane, so "no point reads" alone is
//!    also true of a total bypass. Requiring `scan_reads > 0` is therefore
//!    fallback-proof (verified during #2876's roborev: the fallback scores
//!    `scan_reads == 0`).
//! 2. **Coalescing genuinely wired.** A POSITIVE lower bound on window refills
//!    derived from the fixture's own byte geometry — never `0 <= 0`.
//! 3. **Reads are WINDOW-sized, not PARTITION-sized.** Each scan-plane read must
//!    advance the walk by at least one authoritative
//!    `CompressionInfo.chunk_length` of uncompressed data-section bytes (coverage
//!    per read, NOT raw read width — on a compressed table the raw width is one
//!    compressed chunk and therefore tracks the compression ratio, not the
//!    coalescing; see the assert's own comment). This is the assert that fails if
//!    coalescing regresses to per-partition reads while the plane split stays
//!    intact — the "green + green, still regressed" case above.
//!
//! No timing is measured anywhere: every oracle is a counter (`--lite`-safe, no
//! wall-clock race, per CLAUDE.md's mechanized wall-clock guard).
//!
//! No-heuristics (issue #28): `chunk_length` and the chunk count come from the
//! authoritative `CompressionInfo` sidecar the fixture writes and the reader
//! parses — never inferred from read sizes or byte patterns.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::read_at::ReadAt;
use crate::storage::sstable::reader::SSTableReader;
use crate::{Config, Platform};

/// Partition count for the combined fixture. Sized (with
/// [`COMBINED_MIN_PAYLOAD_LEN`]) so the uncompressed data section comfortably
/// exceeds ONE 4 MiB window target — the window must therefore refill while
/// walking, which is what makes assert 2 a real streaming claim rather than a
/// single-fill tautology.
const COMBINED_PARTITIONS: i32 = 440;

/// Per-partition payload floor (~20 KiB), so `COMBINED_PARTITIONS` partitions span
/// ~8.8 MiB — over 2x the window target.
const COMBINED_MIN_PAYLOAD_LEN: usize = 20 * 1024;

/// Uncompressed `CompressionInfo.chunk_length` the fixture is repacked at. 64 KiB
/// is Cassandra's own compressed-chunk order of magnitude and holds SEVERAL
/// ~20 KiB partitions, which is precisely the "many partitions per chunk"
/// condition that makes per-partition vs per-chunk reads distinguishable.
const COMBINED_CHUNK_SIZE: usize = 64 * 1024;

/// `COMPRESSED_SCAN_WINDOW_TARGET_BYTES` (private to
/// `data_access::summary_scan::compressed_scan_window`), mirrored for the refill
/// derivation below.
const WINDOW_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// Lower bound on coalescing-window refills for a full scan of a
/// `data_section_len`-byte section packed into `chunk_len`-byte chunks — the same
/// derivation the sibling integration test uses, restated here because that file's
/// helper is not importable from `src/`.
///
/// Derivation (an upper bound on bytes-per-refill, inverted): the window tiles
/// `[0, data_section_len)` with no gaps and no overlaps; one refill reads
/// `max(this partition's span, ramped floor)` rounded UP to a chunk boundary; the
/// floor is capped at `WINDOW_TARGET_BYTES` and every partition here is orders of
/// magnitude smaller than that. So no refill covers more than
/// `WINDOW_TARGET_BYTES + chunk_len`, hence at least
/// `ceil(data_section_len / (WINDOW_TARGET_BYTES + chunk_len))` refills.
/// Deliberately conservative — the RAMP means real runs need MORE (~8 here), so a
/// fixture retune can only weaken this bound, never make it flaky.
fn min_refills_for_full_scan(data_section_len: usize, chunk_len: usize) -> u64 {
    (data_section_len as u64).div_ceil((WINDOW_TARGET_BYTES + chunk_len) as u64)
}

/// A `ReadAt` spy that counts calls AND accumulates total bytes requested, so a
/// test can assert on read SIZE (the property that distinguishes a coalesced
/// window from a per-partition read) and not merely on read COUNT.
///
/// [`SleepingReadAt`](crate::storage::sstable::reader::read_at::SleepingReadAt) (#2876's spy) already counts
/// calls; this adds the byte total, which those tests did not need. Delegation is
/// otherwise identical, and no delay is introduced — the oracle is arithmetic,
/// never timing.
struct SizingReadAt {
    inner: Arc<dyn ReadAt>,
    calls: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}

impl SizingReadAt {
    fn new(inner: Arc<dyn ReadAt>, calls: Arc<AtomicUsize>, bytes: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            calls,
            bytes,
        }
    }
}

impl ReadAt for SizingReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        // Count the REQUESTED length: that is the read the kernel sees, and the
        // quantity readahead effectiveness is a function of.
        self.bytes.fetch_add(buf.len(), Ordering::Relaxed);
        self.inner.read_at(offset, buf)
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

/// Build a genuinely LZ4-compressed BIG fixture that keeps `Index.db` + `Summary.db`
/// intact (both required by the Summary-guided walk), and return the temp dir, the
/// `Data.db` path, the schema, the authoritative chunk count, and the UNCOMPRESSED
/// data-section length.
///
/// Technique (issue #1406: CQLite's production write surface never emits
/// compression, so a compressed fixture must be synthesized): write uncompressed
/// via `SSTableWriter`, then repack the data section through
/// `CompressedDataWriter`/`CompressionInfoWriter`. Sound because uncompressed BIG
/// `nb` is HEADERLESS and `Index.db`/`Summary.db` offsets are in the UNCOMPRESSED
/// domain — exactly the domain `CompressionInfo` chunk offsets are relative to — so
/// re-chunking needs no sidecar rewrite. Same recipe as
/// `tests/issue_2877_scan_chunk_coalescing.rs` and
/// `issue_1293_compressed_big_reverse_seek.rs`.
async fn build_compressed_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    crate::schema::TableSchema,
    usize,
    usize,
) {
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{
        create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
        SSTableWriter,
    };
    use crate::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;

    const KS: &str = "scan_plane_ks";
    const TBL: &str = "items";

    let schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).expect("writer");

    // The writer enforces ascending token order, so sort by the AUTHORITATIVE
    // decorated key rather than by id.
    let mut keyed: Vec<_> = (1..=COMBINED_PARTITIONS)
        .map(|id| {
            let mut value = format!("cqlite-2877x2876-{id}-");
            while value.len() < COMBINED_MIN_PAYLOAD_LEN {
                value.push('x');
            }
            let m = Mutation::new(
                TableId::new(KS, TBL),
                PartitionKey::single("id", Value::Integer(id)),
                None,
                vec![CellOperation::Write {
                    column: "payload".to_string(),
                    value: Value::text(value),
                }],
                1_000_000 + id as i64,
                None,
            );
            let key = m.decorated_key(&schema).expect("decorated key");
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).expect("write");
    }
    let info = writer.finish().await.expect("finish");
    let data_path = info.data_path.clone();

    // Repack the data section as a genuine LZ4 chunk stream.
    let header_size = open_reader(&data_path).await.calculate_header_size();
    let raw = std::fs::read(&data_path).expect("read Data.db");
    let data_section = &raw[header_size..];
    let data_section_len = data_section.len();

    let compressor = create_compressor(CompressionAlgorithm::Lz4).expect("lz4 compressor");
    let mut cw = CompressedDataWriter::with_chunk_size(compressor, COMBINED_CHUNK_SIZE);
    cw.write(data_section).expect("compress");
    let (compressed, metadata) = cw.finish().expect("finish compression");
    let chunk_count = metadata.chunk_count();
    std::fs::write(&data_path, &compressed).expect("overwrite Data.db");

    let base = data_path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_end_matches("-Data").to_string())
        .expect("Data.db stem");
    let parent = data_path.parent().expect("Data.db parent");
    CompressionInfoWriter::new(parent.join(format!("{base}-CompressionInfo.db")))
        .write(&metadata)
        .expect("write CompressionInfo.db");

    // The uncompressed CRC.db checksums the OLD chunking and is now meaningless;
    // compressed BIG carries per-chunk CRCs inline in Data.db instead (the
    // CRC-before-decompress ordering of #1411/#1773 is unaffected).
    for entry in std::fs::read_dir(parent).expect("read dir").flatten() {
        if entry.file_name().to_string_lossy().ends_with("-CRC.db") {
            std::fs::remove_file(entry.path()).ok();
        }
    }

    (temp, data_path, schema, chunk_count, data_section_len)
}

async fn open_reader(path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open reader")
}

/// THE combined-interaction test (issues #2876 x #2877).
///
/// Drives ONE full compressed scan through the public Summary-guided query surface
/// (`stream_all_partitions_for_query` — the same chain the Flight warm `do_get`
/// path drives) with independent spies on BOTH positional planes, then asserts
/// three properties of that single run:
///
/// 1. every read landed on the UNADVISED scan plane and ZERO on the `MADV_RANDOM`
///    point plane (`scan_reads > 0` asserted positively, `point_reads == 0`);
/// 2. the coalescing window genuinely refilled at least
///    `min_refills_for_full_scan(...)` times (positive lower bound, not `0 <= 0`);
/// 3. each scan-plane read advances the walk by at least one authoritative
///    `CompressionInfo.chunk_length` — i.e. reads are window-sized, not
///    partition-sized, which is what makes kernel readahead on the unadvised plane
///    effective in the first place.
///
/// Mutation-proven in both directions (issue #2877 combined-test brief): pointing
/// the window's refills back at `point_source` fails assert 1; disabling the
/// ramp/coalescing (per-partition reads) fails assert 3.
///
/// `#[serial_test::serial(work_counters)]` and a `multi_thread` runtime: the
/// refill counter is process-global (`SCAN_WINDOW_REFILLS`), shared with every
/// other test in this binary, so the measurement is serialized on the same key the
/// existing work-counter tests use.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(work_counters)]
async fn coalescing_window_reads_only_the_unadvised_scan_plane() {
    let (_temp, data_path, schema, chunk_count, data_section_len) =
        build_compressed_fixture().await;

    // ---- Fixture preconditions: fail LOUD, never skip. This fixture is built
    // in-test (not a fetched dataset that can legitimately be absent), so an
    // unmet precondition is a broken test, not an absent environment. Every one
    // of them is a condition WITHOUT which some assert below goes vacuous.
    assert!(
        data_section_len > 2 * WINDOW_TARGET_BYTES,
        "fixture must exceed 2x the 4 MiB window target so the window MUST refill \
         while walking (assert 2 would otherwise be a single-fill tautology); got \
         {data_section_len} bytes"
    );
    assert!(
        (chunk_count as i32) < COMBINED_PARTITIONS,
        "fixture must pack MANY partitions per chunk ({chunk_count} chunks for \
         {COMBINED_PARTITIONS} partitions) or per-chunk and per-partition reads are \
         indistinguishable"
    );

    let mut reader = open_reader(&data_path).await;
    // AUTHORITATIVE chunk geometry (no-heuristics, #28): read `chunk_length` off the
    // parsed `CompressionInfo` sidecar, never inferred from observed read sizes.
    let ci = reader
        .compression_info
        .as_deref()
        .cloned()
        .expect("fixture must be COMPRESSED — the coalescing window is compressed-only");
    let chunk_length = ci.chunk_length as usize;
    assert_eq!(
        chunk_length, COMBINED_CHUNK_SIZE,
        "the reader must parse the chunk_length the fixture wrote"
    );
    assert!(
        reader.index_reader.is_some(),
        "the Summary-guided walk requires a raw-key Index.db"
    );
    assert!(
        reader.bti_partitions_db.is_none(),
        "fixture must be BIG, not BTI"
    );
    let summary_entries = reader
        .summary_reader
        .as_ref()
        .map(|s| s.get_entries().len())
        .unwrap_or(0);
    assert!(
        summary_entries > 0,
        "the Summary-guided walk requires a usable Summary.db (got \
         {summary_entries} samples)"
    );

    // ---- Independent spies on the two planes. They are distinct reader fields, so
    // replacing one never redirects the other — even on the Buffered/Direct backends
    // where both begin as clones of one backing source.
    let point_calls = Arc::new(AtomicUsize::new(0));
    let point_bytes = Arc::new(AtomicUsize::new(0));
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let scan_bytes = Arc::new(AtomicUsize::new(0));
    // Both planes get the byte-totalling spy, so a failure can report HOW MUCH
    // leaked to the advised plane, not merely that something did.
    let real_point = reader.clone_point_source();
    reader.set_point_source(Arc::new(SizingReadAt::new(
        real_point,
        point_calls.clone(),
        point_bytes.clone(),
    )));
    let real_scan = reader.clone_scan_positional_source();
    reader.set_scan_positional_source(Arc::new(SizingReadAt::new(
        real_scan,
        scan_calls.clone(),
        scan_bytes.clone(),
    )));

    // ---- ONE full scan through the public Summary-guided surface.
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut rows = 0usize;
    reader
        .stream_all_partitions_for_query(Some(&schema), &cancel, None, |_row| {
            rows += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("the Summary-guided compressed scan must succeed");

    let point_reads = point_calls.load(Ordering::Relaxed);
    let point_bytes_total = point_bytes.load(Ordering::Relaxed);
    let scan_reads = scan_calls.load(Ordering::Relaxed);
    let scan_bytes_total = scan_bytes.load(Ordering::Relaxed);
    let refills = SSTableReader::scan_window_refill_count();

    // Non-vacuity on the DATA (CLAUDE.md: 0-rows-when-present is a failure).
    assert_eq!(
        rows, COMBINED_PARTITIONS as usize,
        "the scan must emit exactly one row per fixture partition"
    );

    // ---- ASSERT 1a: the scan plane was POSITIVELY used.
    //
    // Asserted directly, never inferred from the point plane's silence: the
    // full-ring fallback (`stream_all_partitions_for_compaction`) reads through
    // `BlockSource`, which is on NEITHER spied plane, so a total bypass would ALSO
    // report `point_reads == 0`. Only a positive scan-plane count excludes it.
    assert!(
        scan_reads > 0,
        "the coalescing window must read Data.db through the UNADVISED \
         `scan_positional_source` (issue #2876); got 0 reads there \
         ({point_reads} landed on the point plane). Two causes, both regressions: \
         (a) the window's reads were REDIRECTED to another plane — if the point \
         count above is nonzero, that is the #2876 field regression reinstated; or \
         (b) the Summary-guided walk never ran and FELL BACK to \
         `stream_all_partitions_for_compaction`, whose `BlockSource` reads are on \
         neither spied plane, which would make every other assert here vacuous"
    );

    // ---- ASSERT 1b: the MADV_RANDOM point plane was NOT touched.
    //
    // This is the half that fails if #2877's window widens its reads but issues them
    // on the advised mapping: readahead stays suppressed and #2876's field
    // regression is silently back, with both PRs' own suites green
    // (CASSANDRA-15452's lesson — a userspace scan buffer defeated by the layer
    // beneath it).
    assert_eq!(
        point_reads, 0,
        "the #2877 coalescing window must NOT read Data.db through the \
         `MADV_RANDOM` point plane (issue #2876): {point_reads} reads \
         ({point_bytes_total} bytes) leaked there. \
         A wider read into an `MADV_RANDOM` mapping still gets NO kernel readahead, \
         so coalescing on the advised plane reinstates the #2876 field regression \
         while both PRs' own tests stay green"
    );

    // ---- ASSERT 2: the coalescing is genuinely WIRED (positive lower bound).
    //
    // Every other coalescing oracle in the sibling integration test is an UPPER
    // bound, and upper bounds are all satisfied by a window that never streams (one
    // whole-section fill: 1 refill, 0 compactions) or by no window at all (0/0).
    // This floor is the streaming-tiling claim.
    let min_refills = min_refills_for_full_scan(data_section_len, chunk_length).max(2);
    assert!(
        refills >= min_refills,
        "the coalescing window must have refilled at least {min_refills} times \
         (derivation: ceil({data_section_len} data-section bytes / (4 MiB window \
         target + {chunk_length}-byte chunk_length)), floored at 2 because the \
         fixture exceeds 2x the window target); got {refills}. 1 means one giant \
         non-streaming fill and 0 means the window was bypassed — either way the \
         coalescing walk under test never ran (issue #2877 wiring evidence)"
    );

    // ---- ASSERT 3: reads are WINDOW-sized (chunk-granular), not PARTITION-sized.
    //
    // The property that makes kernel readahead on the unadvised plane pay off, and
    // the one assert that fails if coalescing regresses to per-partition reads while
    // the plane split stays intact.
    //
    // Measured as UNCOMPRESSED data-section bytes DELIVERED per scan-plane read,
    // which must be at least one authoritative `chunk_length`.
    //
    // Why not the raw read length: on a COMPRESSED SSTable the scan plane's unit of
    // work is one compressed chunk — `block_io::read_compressed_chunk_at` issues
    // exactly one `read_exact_at` of `chunk_data_size + 4` (payload + inline CRC) per
    // chunk, even when the window asks for 4 MiB. So the raw bytes reflect the
    // COMPRESSION RATIO, not the coalescing (this fixture's `xxxx` filler compresses
    // ~180:1, making a perfectly coalesced read only ~371 bytes wide — a raw-size
    // bound would be asserting on entropy, and would flip on any filler change).
    // The invariant that actually distinguishes the two shapes is COVERAGE per read:
    //
    // - coalesced (correct): the window fetches each chunk-aligned span ONCE and
    //   serves every partition inside it from memory => at most `chunk_count` reads,
    //   i.e. >= `chunk_length` uncompressed bytes per read;
    // - per-partition (regressed): each partition's span re-reads its covering
    //   chunk(s) => ~`COMBINED_PARTITIONS` reads, i.e. ~one ~20 KiB partition body
    //   per read, well under the 64 KiB chunk.
    //
    // That ratio is exactly the readahead-relevant quantity: how much of the forward
    // walk one kernel-visible read advances. Stated exactly (no off-by-one on the
    // partial tail chunk, which drags a mean-bytes-per-read bound just under
    // `chunk_length`): a forward-tiling window reads each chunk AT MOST once, so
    // `scan_reads <= chunk_count`, i.e. ~`chunk_length` of walk progress per read.
    let uncompressed_per_read = data_section_len / scan_reads;
    let mean_raw_read = scan_bytes_total / scan_reads;
    assert!(
        scan_reads <= chunk_count,
        "the coalescing window's scan-plane reads must be CHUNK-granular, not \
         PARTITION-granular: {scan_reads} reads for a section of only {chunk_count} \
         chunks ({data_section_len} uncompressed bytes, {uncompressed_per_read} \
         bytes of walk progress per read vs a {chunk_length}-byte authoritative \
         `CompressionInfo.chunk_length`). A coalescing window reads each chunk ONCE \
         and serves every partition inside it from memory; a per-partition walk \
         re-reads a covering chunk for each of {COMBINED_PARTITIONS} partitions \
         (~{COMBINED_MIN_PAYLOAD_LEN} bytes of progress per read). Reads that \
         advance the walk by less than a chunk are what left kernel readahead \
         unable to help (issue #2877 x #2876). Mean raw read was {mean_raw_read} \
         bytes ({scan_bytes_total} total) — informational only: on a compressed \
         table one read is one compressed chunk, so raw width tracks the \
         compression ratio, not the coalescing"
    );

    // ---- Cross-check tying asserts 2 and 3 together: the whole data section was
    // covered by ~`refills` window reads, so bytes-per-refill must be at least a
    // chunk too. This catches a shape where a few huge reads coexist with a long
    // tail of per-partition reads (which could keep the MEAN above a chunk while
    // most reads are tiny) — the refill count is then far above `chunk_count`.
    assert!(
        refills <= chunk_count as u64,
        "a coalescing window must need FEWER refills than the section has chunks \
         ({chunk_count}); {refills} refills means the window degenerated toward a \
         per-chunk (or per-partition) read walk (issue #2877)"
    );
}
