//! Chunk-CRC pre-flight for salvage (design D1): validate every `Data.db`
//! chunk BEFORE the recovery loop and record the bad-chunk set, so a
//! partition whose byte range intersects a CRC-failed chunk is a loss with
//! class `chunk-crc` EVEN IF its bytes happened to still parse (#3782: a
//! flipped byte that still decodes). Cassandra's chunk CRC is the authority
//! on what is trustworthy, not whether the parser tolerated the bytes.
//!
//! Unlike `verify::check_inline_chunk_crc` / `check_uncompressed_crc_db`
//! (which report the FIRST bad chunk and stop, matching the read path's
//! fail-fast posture), this module walks EVERY chunk and collects the WHOLE
//! bad set — salvage needs to know which PARTITIONS are affected, not just
//! that the file has a problem.

use super::ComponentFinding;
use crate::storage::sstable::chunk_reader::ChunkReader;
use crate::storage::sstable::compression_info::CompressionInfo;
use std::collections::BTreeSet;
use std::path::Path;

/// The largest `bad_chunks` set [`uncompressed_chunk_preflight`] will
/// accumulate before refusing the WHOLE input outright (roborev, issue
/// #4196, round 17 Medium finding — the "same shape" second half: this
/// function's chunk-by-chunk walk has NO independent cap on `chunk_index`
/// the way the compressed sibling does via `CompressionInfo::parse`'s
/// `chunk_count <= 1_000_000` — it runs until real EOF, floored only at
/// `CrcDb`'s own `chunk_size >= 4096`). A `CRC.db` genuinely shorter than
/// `Data.db` needs (every chunk past its coverage takes the "no entry"
/// `Err` arm below) makes EVERY remaining chunk of a large file bad, so a
/// multi-GB/TB uncompressed input with a truncated `CRC.db` sidecar could
/// otherwise grow this `BTreeSet<u64>` into the tens of millions of
/// entries — hundreds of MB to over a GB, well past the crate's <128 MB
/// target, before a single partition is even examined.
///
/// Deliberately a REFUSAL (matching this function's existing
/// `header_size != 0` fail-closed arm), never a silent truncation of WHICH
/// indices are tracked: `recover.rs` intersects a partition's chunk range
/// against this set to decide whether it is `chunk-crc`-trustworthy, so
/// silently dropping later bad indices from the set would make partitions
/// PAST the cap read as falsely CLEAN — accepting unverifiable data as
/// verified, exactly the class of silent-wrong-answer this whole preflight
/// exists to prevent (no-heuristics mandate, issue #28). A file this
/// damaged (tens of thousands of untrustworthy chunks) is evidence the
/// input itself is not a good salvage candidate; refusing the generation
/// cleanly, naming the count, is more honest than either OOMing or
/// guessing.
const MAX_UNCOMPRESSED_BAD_CHUNKS: u64 = 65_536;

/// Bad chunk indices plus a summary finding, when any chunk failed.
pub(super) struct ChunkPreflight {
    pub(super) bad_chunks: BTreeSet<u64>,
    pub(super) finding: Option<ComponentFinding>,
    /// The uncompressed chunk size that bounds this file's chunking — the
    /// same domain `Index.db`/`Partitions.db` offsets live in, so a
    /// partition's `[offset, end)` maps to chunk indices via
    /// [`chunks_for_range`] using this value.
    pub(super) chunk_size: u64,
    /// The total decompressed/uncompressed data-section length, when known
    /// (compressed: `CompressionInfo.data_length`; uncompressed: the total
    /// bytes the CRC.db walk actually scanned). `recover.rs` uses this — NOT
    /// `entry.data_offset + 1` — as the LAST partition's chunk-range `end`,
    /// so the pre-flight covers every chunk that partition's bytes actually
    /// span, not just the one containing its start (roborev, issue #4196).
    /// `0` when unknown (no `CRC.db`, uncompressed).
    pub(super) data_length: u64,
}

/// Which chunk indices (inclusive) a partition's decompressed-domain byte
/// range `[offset, end)` intersects, given `chunk_size`. `end` is exclusive;
/// an empty/zero-length range still names the ONE chunk holding `offset`.
pub(super) fn chunks_for_range(offset: u64, end: u64, chunk_size: u64) -> Vec<u64> {
    if chunk_size == 0 {
        return Vec::new();
    }
    let start_chunk = offset / chunk_size;
    let last_byte = end.saturating_sub(1).max(offset);
    let end_chunk = last_byte / chunk_size;
    (start_chunk..=end_chunk).collect()
}

/// Validate every inline chunk CRC32 of a compressed `Data.db` (#998),
/// without decompressing (matches `verify::check_inline_chunk_crc`'s
/// posture) — collecting every failing chunk index instead of stopping at
/// the first.
pub(super) fn compressed_chunk_preflight(
    data_path: &Path,
    compression_info: &CompressionInfo,
) -> std::io::Result<ChunkPreflight> {
    use std::fs::File;

    let file = File::open(data_path)?;
    let total_size = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);
    let mut chunk_reader = ChunkReader::new(reader, compression_info.clone(), total_size);

    let mut bad_chunks = BTreeSet::new();
    let mut first_detail: Option<String> = None;
    // roborev, issue #4196, round-14 Low finding: the summary below used to
    // report every entry in `bad_chunks` as a CRC32 validation failure, but
    // the round-11 plausibility pre-check below can ALSO add to
    // `bad_chunks` for chunks that were deliberately NEVER READ (their own
    // framing is already nonsensical) — count the two causes separately so
    // the summary's claim matches what actually happened for each chunk
    // (an implausible-offset `CompressionInfo.db` must not read as a CRC/
    // bit-rot problem in `Data.db`).
    let mut implausible_count: u64 = 0;
    let mut crc_failure_count: u64 = 0;
    for i in 0..chunk_reader.chunk_count() {
        // roborev, issue #4196, round-11 Medium finding: `ChunkReader::read_chunk`
        // sizes its allocation as `compressed_chunk_size(i, total_size)` —
        // for a NON-LAST chunk that is `chunk_offsets[i+1] - chunk_offsets[i]`
        // (`compression_info.rs::compressed_chunk_size`), bounded ONLY by
        // `CompressionInfo::validate`'s ascending-order check (via
        // `checked_sub`, so it cannot underflow) — NEVER cross-checked
        // against the file's real length. A corrupt-but-still-ascending
        // offset table (e.g. `[0, 2^50, 2^51]`, a plausible shape for a
        // partially-overwritten `CompressionInfo.db`) makes `read_chunk`
        // allocate ~1 PB for chunk 0 alone — the SAME unbounded-allocation
        // class rounds 9 and 10 fixed for the boundary source and for
        // `data_length`, reachable here through a THIRD field neither of
        // those fixes touches. Bound each chunk's declared `[offset,
        // offset+size)` against the REAL, already-measured `total_size`
        // BEFORE ever calling `read_chunk` — an implausible chunk is
        // recorded as bad (its CRC cannot be trusted if its own framing is
        // already nonsensical) rather than handed to an allocation sized
        // from the untrusted field.
        let plausible = match (
            compression_info.compressed_chunk_offset(i),
            compression_info.compressed_chunk_size(i, total_size),
        ) {
            (Some(offset), Some(size)) => offset
                .checked_add(size)
                .is_some_and(|end| end <= total_size),
            _ => false,
        };
        if !plausible {
            bad_chunks.insert(i as u64);
            implausible_count += 1;
            if first_detail.is_none() {
                first_detail = Some(format!(
                    "chunk {i}: declared chunk offset/size exceeds Data.db's real length \
                     ({total_size} bytes) — not read"
                ));
            }
            continue;
        }
        if let Err(e) = chunk_reader.read_chunk(i) {
            bad_chunks.insert(i as u64);
            crc_failure_count += 1;
            if first_detail.is_none() {
                first_detail = Some(format!("chunk {i}: {e}"));
            }
        }
    }

    let finding = first_detail.map(|detail| ComponentFinding {
        class: "ChunkDecompressionError".to_string(),
        component: "Data.db".to_string(),
        detail: format!(
            "{} of {} chunk(s) untrustworthy ({crc_failure_count} CRC failure(s), \
             {implausible_count} implausible framing); first: {detail}",
            bad_chunks.len(),
            chunk_reader.chunk_count()
        ),
    });

    // roborev, issue #4196, round-10 High finding (comment provenance
    // corrected, round-11 Low finding: the cap below is `CompressionInfo::parse`'s,
    // not `validate`'s — see that correction): `compression_info.data_length`
    // is taken verbatim from `CompressionInfo.db`'s 8-byte field —
    // `CompressionInfo::parse` bounds `chunk_count` (<= 1,000,000,
    // `compression_info.rs:242`); `CompressionInfo::validate` separately
    // checks `chunk_length != 0` (NO upper bound at all) and offset
    // monotonicity — but nothing anywhere cross-checks `data_length`
    // against either. A single flipped byte in THAT field alone (chunk
    // offsets/table/Data.db all intact, so the preflight loop above reports
    // zero bad chunks) reinstated the exact unbounded chunk-range
    // allocation round-9's fix removed for the boundary-source case:
    // `recover.rs`'s clamp trusts THIS returned `data_length` as the safe
    // bound, so an unvalidated field here defeats it. `chunk_count() *
    // chunk_length` is the REAL, independently-bounded maximum this file's
    // chunk table can possibly cover — `chunk_count` per `parse`'s cap
    // above, `chunk_length` per `CompressionInfo::validate`'s non-zero
    // check ANDED with `u32::MAX` (its own on-disk width) — take the
    // smaller of the declared value and that bound, so a corrupted
    // `data_length` can never exceed what the chunk table actually
    // supports.
    let chunk_table_bound =
        (chunk_reader.chunk_count() as u64).saturating_mul(compression_info.chunk_length as u64);
    // roborev, issue #4196, round-12 Medium finding: `compression_info.data_length`
    // has NO independent LOWER bound either — `CompressionInfo::validate`
    // never checks it at all, so a ZEROED field parses fine and
    // `.min(chunk_table_bound)` would then yield exactly `0`. BOTH
    // downstream OOM guards in `recover.rs` are conditioned on `data_length
    // > 0` (matching the uncompressed "no CRC.db" case's LEGITIMATE `0`,
    // meaning "chunking info unavailable") — so a zeroed compressed
    // `data_length` silently DISABLES the very clamp round 10 added,
    // reinstating the unbounded `chunks_for_range` materialization through
    // the opposite corruption direction (zeroed rather than inflated).
    // `chunk_table_bound` is ALWAYS positive for a validated
    // `CompressionInfo` (`chunk_count >= 1`, `chunk_length > 0`), so a
    // declared `data_length` of exactly `0` is itself implausible for a
    // compressed input with a real chunk table — fall back to the
    // structurally-derived bound instead of propagating a value that
    // disables clamping.
    let data_length = if compression_info.data_length == 0 {
        chunk_table_bound
    } else {
        compression_info.data_length.min(chunk_table_bound)
    };

    Ok(ChunkPreflight {
        bad_chunks,
        finding,
        chunk_size: compression_info.chunk_length as u64,
        data_length,
    })
}

/// Validate every uncompressed `Data.db` chunk against `CRC.db` (issue
/// #1396), collecting every failing chunk index. Absent `CRC.db` is
/// warn-and-proceed (design D4, matching `verify`): an empty bad-chunk set,
/// no finding — the chunk-CRC signal simply is not available.
///
/// # Domain assumption (roborev, issue #4196) — now GUARDED, fail-closed
///
/// This function chunks `Data.db` from byte 0 (file-ABSOLUTE indices —
/// `CRC.db`'s own chunking convention), while `recover.rs`'s
/// `chunks_for_range` maps `Index.db`-derived offsets, which are
/// DATA-SECTION-relative (i.e. relative to `SSTableReader::calculate_header_size()`).
/// The two agree only when that header size is `0` — true for the headerless
/// `nb`/`da` layouts salvage targets today, but NOT asserted structurally
/// anywhere else, so `header_size` (the caller's already-open reader's own
/// `calculate_header_size()`) is threaded in and checked HERE: a non-zero
/// value would silently mis-attribute `chunk-crc` losses by one chunk on
/// every intersection, so it fails closed with a typed `Error::Corruption`
/// (classified `ComponentUnreadable` by the caller) rather than compute a
/// wrong answer. Round-3/round-4 rounds left this a declared, unasserted
/// gap because this function had no reader instance to consult — fixed by
/// threading the value through instead of computing it locally.
pub(super) async fn uncompressed_chunk_preflight(
    data_path: &Path,
    crc_path: &Path,
    header_size: usize,
) -> crate::Result<ChunkPreflight> {
    use crate::storage::sstable::reader::crc::CrcDb;
    use tokio::io::AsyncReadExt;

    if !crc_path.exists() {
        return Ok(ChunkPreflight {
            bad_chunks: BTreeSet::new(),
            // Affirmative-zero doctrine (roborev, issue #4196, round-6 Medium
            // finding 2): a manifest with zero `chunk-crc` losses must not
            // read the same whether every chunk validated OR chunk-CRC
            // validation never ran at all — a #3782-class flipped-but-still-
            // parseable byte is undetectable without CRC.db, so the absence
            // is recorded as a named finding rather than silently skipped.
            finding: Some(ComponentFinding {
                class: "ChunkCrcUnavailable".to_string(),
                component: "CRC.db".to_string(),
                detail: "CRC.db is absent for this uncompressed input; chunk-CRC validation \
                         did not run, so a flipped-but-still-parseable byte would not be \
                         detected by the chunk-crc loss class"
                    .to_string(),
            }),
            // No CRC.db to derive a chunk size from; 0 disables chunk-range
            // mapping (callers treat an empty bad-chunk set as "nothing to
            // map" regardless).
            chunk_size: 0,
            data_length: 0,
        });
    }
    if header_size != 0 {
        // The domain-assumption guard (see this function's doc): a non-zero
        // header size means `CRC.db`'s file-absolute chunk indices and
        // `Index.db`-derived data-section-relative offsets would disagree by
        // a fixed skew this function has no way to correct blindly. Refuse
        // rather than silently mis-map every chunk-range intersection.
        return Err(crate::Error::corruption(format!(
            "uncompressed chunk pre-flight cannot map Index.db-derived offsets to CRC.db's \
             file-absolute chunking: this input's header is {header_size} bytes (expected 0 for \
             the headerless nb/da layouts salvage targets)"
        )));
    }

    // roborev, issue #4196, round-8 Low finding: `.unwrap_or(0)` swallowed a
    // `Data.db` metadata failure into `data_len = 0`, which makes
    // `CrcDb::open`'s size-sanity check reject any REAL `CRC.db` as
    // "oversized" — misattributing a `Data.db` read failure to `CRC.db`.
    // Propagate the metadata error instead so the refusal names the right
    // component.
    let data_len = tokio::fs::metadata(data_path).await?.len();
    let crc = CrcDb::open(crc_path, data_len).await?;
    let chunk_size = crc.chunk_size() as u64;

    let mut file = tokio::fs::File::open(data_path).await?;
    let mut bad_chunks = BTreeSet::new();
    let mut first_detail: Option<String> = None;
    let mut chunk_index: u64 = 0;
    let mut total_scanned: u64 = 0;
    let mut buf = vec![0u8; chunk_size.max(1) as usize];
    loop {
        let mut filled = 0usize;
        loop {
            match file.read(&mut buf[filled..]).await? {
                0 => break,
                n => {
                    filled += n;
                    if filled == buf.len() {
                        break;
                    }
                }
            }
        }
        if filled == 0 {
            break;
        }
        total_scanned += filled as u64;
        let computed = crc32fast::hash(&buf[..filled]);
        match crc.crc_for_chunk(chunk_index as usize) {
            Ok(expected) if expected == computed => {}
            Ok(_) => {
                bad_chunks.insert(chunk_index);
                if first_detail.is_none() {
                    first_detail = Some(format!("chunk {chunk_index}: CRC32 mismatch"));
                }
            }
            Err(e) => {
                bad_chunks.insert(chunk_index);
                if first_detail.is_none() {
                    first_detail = Some(format!("chunk {chunk_index}: CRC.db has no entry: {e}"));
                }
            }
        }
        // See `MAX_UNCOMPRESSED_BAD_CHUNKS`'s doc: refuse the WHOLE input
        // rather than let this set (and the memory it costs) grow without
        // bound, and rather than silently stop tracking later bad indices
        // (which would make partitions past this point read as falsely
        // chunk-crc-clean).
        if bad_chunks.len() as u64 >= MAX_UNCOMPRESSED_BAD_CHUNKS {
            return Err(crate::Error::corruption(format!(
                "uncompressed chunk pre-flight found {} untrustworthy chunks (of {} scanned so \
                 far) and stopped early rather than risk unbounded memory growth or silently \
                 undercounting later bad chunks; this input is too damaged for a per-partition \
                 chunk-CRC pre-flight to remain meaningful — first: {}",
                bad_chunks.len(),
                chunk_index + 1,
                first_detail.as_deref().unwrap_or("(no detail recorded)")
            )));
        }
        chunk_index += 1;
        if filled < buf.len() {
            break;
        }
    }

    let finding = first_detail.map(|detail| ComponentFinding {
        class: "UncompressedChunkCrcMismatch".to_string(),
        component: "Data.db".to_string(),
        detail: format!(
            "{} of {} chunk(s) failed CRC.db validation; first: {detail}",
            bad_chunks.len(),
            chunk_index
        ),
    });

    // roborev, issue #4196, round-14 Medium finding: `total_scanned == 0`
    // (an uncompressed `Data.db` truncated to zero bytes) must report
    // `chunk_size: 0` ALONGSIDE `data_length: 0` — round 12 fixed exactly
    // this class for the COMPRESSED branch (a zeroed `data_length` with a
    // non-zero `chunk_size`/`chunk_table_bound` silently disables both of
    // `recover.rs`'s OOM/plausibility guards, which are keyed on
    // `data_length > 0`), and it applied only there. `CrcDb::open` enforces
    // `chunk_size >= 4096` unconditionally, so without this a zero-byte
    // `Data.db` with an intact `CRC.db` and a corrupt `Index.db` offset
    // reaches `chunks_for_range` with a raw, unvalidated `end_bound`.
    let (chunk_size, data_length) = if total_scanned == 0 {
        (0, 0)
    } else {
        (chunk_size, total_scanned)
    };

    Ok(ChunkPreflight {
        bad_chunks,
        finding,
        chunk_size,
        data_length,
    })
}

#[cfg(test)]
mod tests {
    use super::{chunks_for_range, uncompressed_chunk_preflight, MAX_UNCOMPRESSED_BAD_CHUNKS};

    /// Roborev, issue #4196, round 17 Medium finding (second half): a
    /// `CRC.db` with ZERO real entries against a large `Data.db` makes
    /// EVERY chunk take the "no entry" `Err` arm — this must REFUSE the
    /// whole input once `bad_chunks` would otherwise grow past
    /// `MAX_UNCOMPRESSED_BAD_CHUNKS`, not silently keep tracking (or
    /// silently keep SCANNING) without bound. `Data.db` is
    /// SPARSE-EXTENDED via `File::set_len()` (logical length only, no real
    /// bytes written/allocated — the same technique
    /// `issue_4196_salvage_round15_bounds.rs` uses for its 128 MiB
    /// span-ceiling test) so this stays a fast, deterministic unit test
    /// rather than needing a genuinely multi-hundred-MB fixture.
    #[tokio::test]
    async fn short_crc_db_refuses_rather_than_grow_bad_chunks_unbounded() {
        use crate::storage::sstable::reader::crc::MIN_CRC_CHUNK_SIZE;

        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_path = temp.path().join("nb-1-big-Data.db");
        let crc_path = temp.path().join("nb-1-big-CRC.db");

        let chunk_size = MIN_CRC_CHUNK_SIZE as u64;
        // Comfortably past the cap so the scan is guaranteed to cross it
        // before reaching real EOF.
        let sparse_len = (MAX_UNCOMPRESSED_BAD_CHUNKS + 100) * chunk_size;
        let file = std::fs::File::create(&data_path).expect("create Data.db");
        file.set_len(sparse_len).expect("sparse-extend Data.db");

        // CRC.db: header (chunk_size) only, ZERO trailing CRC entries — every
        // real chunk lookup misses from chunk 0 onward.
        std::fs::write(&crc_path, (chunk_size as i32).to_be_bytes()).expect("write CRC.db header");

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            uncompressed_chunk_preflight(&data_path, &crc_path, 0),
        )
        .await
        .expect("must not hang — the whole point of the cap is a bounded-time refusal");

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!(
                "a CRC.db with zero real entries against a huge Data.db must REFUSE once \
                 accumulated bad chunks cross MAX_UNCOMPRESSED_BAD_CHUNKS, not silently keep \
                 growing the tracked set (memory) or keep scanning to genuine EOF (time)"
            ),
        };
        let msg = err.to_string();
        assert!(
            msg.contains(&MAX_UNCOMPRESSED_BAD_CHUNKS.to_string()),
            "refusal should name the cap so an operator understands why; got: {msg}"
        );
    }

    /// roborev, issue #4196, round-14 Medium finding: a zero-byte `Data.db`
    /// (`total_scanned == 0`) must return `chunk_size: 0` ALONGSIDE
    /// `data_length: 0` — not a real, positive `chunk_size` sourced
    /// independently from `CRC.db`'s own header while `data_length` reads
    /// the "unmeasurable" sentinel. Both fields must move together so a
    /// caller keying an OOM/plausibility guard on `data_length > 0` (as
    /// `recover.rs` does) is NEVER left with a real `chunk_size` and a
    /// disabled clamp at the same time — the exact decoupling round 12
    /// already closed for the COMPRESSED sibling
    /// (`compressed_chunk_preflight`/`CompressionInfo.data_length`).
    ///
    /// Called DIRECTLY (this function is `pub(super)`, reachable from this
    /// module's own test) rather than through a `salvage_sstable(..)`
    /// fixture: `SSTableReader::open` itself requires at least 8 bytes to
    /// parse Data.db's header-detection buffer, so `recover.rs`'s real call
    /// order (`open_reader` before this pre-flight — see
    /// `salvage_sstable`'s own comment on that ordering) makes a literal
    /// 0-byte `Data.db` UNREACHABLE via the end-to-end CLI path today; the
    /// underlying invariant this function must hold is still worth fixing
    /// and guarding directly, both as defense-in-depth against that call
    /// order ever changing and because the function's own documented
    /// contract ("`0` when unknown") must be internally consistent
    /// regardless of who currently enforces it.
    #[tokio::test]
    async fn zero_byte_data_db_zeroes_chunk_size_too() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_path = temp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, []).expect("write zero-byte Data.db");
        let crc_path = temp.path().join("nb-1-big-CRC.db");
        // A real, positive chunk_size header (64 KiB, Cassandra's default) —
        // zero trailing CRC entries, matching a genuinely 0-byte `data_len`
        // (mirrors a real `CrcDb::open(..., data_len: 0)` call, whose
        // `max_len` bound is exactly this: header only, `n_chunks == 0`).
        std::fs::write(&crc_path, 65536i32.to_be_bytes()).expect("write CRC.db header");

        let preflight = uncompressed_chunk_preflight(&data_path, &crc_path, 0)
            .await
            .expect("a well-formed (if empty) Data.db/CRC.db pair must not error");

        assert_eq!(
            preflight.data_length, 0,
            "a zero-byte Data.db must report data_length: 0"
        );
        assert_eq!(
            preflight.chunk_size, 0,
            "chunk_size must be zeroed ALONGSIDE data_length — a real, positive chunk_size \
             here (sourced independently from CRC.db's header) would let a caller keyed on \
             `data_length > 0` alone believe chunking is unknown/disabled while chunk_size \
             still passes a `chunk_size > 0` gate, exactly the decoupling this fix closes"
        );
        assert!(
            preflight.bad_chunks.is_empty(),
            "a genuinely empty file has no chunks to flag either way"
        );
    }

    /// The healthy control for the test above: a NON-empty, matching
    /// Data.db/CRC.db pair (one full chunk, no corruption) reports the
    /// REAL positive `chunk_size` and `data_length` — proving the zero-
    /// fallback above is keyed on `total_scanned == 0` specifically, not on
    /// something that also (wrongly) zeroes a legitimate small file.
    #[tokio::test]
    async fn non_empty_data_db_keeps_the_real_chunk_size() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_path = temp.path().join("nb-1-big-Data.db");
        let payload = vec![0xABu8; 100];
        std::fs::write(&data_path, &payload).expect("write Data.db");
        let crc_path = temp.path().join("nb-1-big-CRC.db");
        let mut crc_bytes = Vec::new();
        crc_bytes.extend_from_slice(&65536i32.to_be_bytes());
        crc_bytes.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        std::fs::write(&crc_path, &crc_bytes).expect("write CRC.db");

        let preflight = uncompressed_chunk_preflight(&data_path, &crc_path, 0)
            .await
            .expect("a well-formed Data.db/CRC.db pair must not error");

        assert_eq!(preflight.data_length, 100);
        assert_eq!(preflight.chunk_size, 65536);
        assert!(
            preflight.bad_chunks.is_empty(),
            "the CRC matches the real payload — nothing should be flagged"
        );
    }

    /// The common case: an offset and end within one chunk name that one
    /// chunk alone.
    #[test]
    fn range_within_one_chunk() {
        assert_eq!(chunks_for_range(0, 100, 1000), vec![0]);
        assert_eq!(chunks_for_range(500, 999, 1000), vec![0]);
    }

    /// A range spanning several whole chunks names every one of them,
    /// inclusive of the chunk holding the exclusive `end - 1` byte.
    #[test]
    fn range_spanning_several_chunks() {
        assert_eq!(chunks_for_range(0, 2500, 1000), vec![0, 1, 2]);
        assert_eq!(chunks_for_range(1000, 2000, 1000), vec![1]);
    }

    /// An empty/zero-length range (`end <= offset`) still names the ONE
    /// chunk holding `offset` — this function's own documented contract.
    #[test]
    fn empty_range_names_the_offsets_own_chunk() {
        assert_eq!(chunks_for_range(500, 500, 1000), vec![0]);
        assert_eq!(chunks_for_range(500, 0, 1000), vec![0]);
    }

    /// `chunk_size == 0` is the "chunking unknown" signal (no `CRC.db`, or
    /// uncompressed with `chunk_size` never established) — callers already
    /// skip calling this at all in that case, but the function itself
    /// degrades to an empty result rather than dividing by zero.
    #[test]
    fn zero_chunk_size_yields_empty() {
        assert_eq!(chunks_for_range(0, 1000, 0), Vec::<u64>::new());
    }

    /// Roborev, issue #4196, round-9 High finding: this function itself
    /// performs NO bounds checking against a "real" file size — it is a
    /// pure arithmetic mapping, by design. The safety fix (clamping
    /// `chunk_range_end` to the measured `data_length` before EVER calling
    /// this) lives in the CALLER (`recover.rs`'s per-partition loop, see its
    /// own comment at the call site) — this test documents that this
    /// function's caller-facing contract is "give me a bounded range", not
    /// "bound the range for me", so a regression that removes the caller's
    /// clamp would NOT be caught here; it is caught end-to-end by
    /// `issue_4196_salvage_corruption_corpus.rs`'s
    /// `implausible_last_offset_does_not_oom_and_classifies_truncated`
    /// instead (a bounded-time integration assertion, since actually
    /// allocating an unbounded `Vec` here to prove the absence of a bound
    /// would itself be the hazard this fix exists to prevent).
    #[test]
    fn large_but_bounded_range_is_the_callers_responsibility() {
        // A merely large (not absurd) range still materializes fully here —
        // proving this function computes correctly at scale, without ever
        // approaching a size this test would regret allocating.
        let result = chunks_for_range(0, 10_000_000, 65_536);
        assert_eq!(result.len(), 153);
        assert_eq!(result[0], 0);
        assert_eq!(*result.last().unwrap(), 152);
    }
}
