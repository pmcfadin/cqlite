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
/// accumulate — genuine CRC32 MISMATCHES only (roborev, issue #4196, round
/// 21 Medium finding — corrects round 17/19's OWN conflation, see
/// `ChunkPreflight::bad_chunks`'s doc) — before refusing the WHOLE input
/// outright. `CompressionInfo::parse`'s compressed sibling caps
/// `chunk_count` at `1_000_000`; this uncompressed function has no such
/// independent cap, so a file with tens of thousands of GENUINELY
/// bit-rotted chunks (not merely unverified ones — see below) is itself
/// evidence the input is too damaged for a per-partition chunk-CRC
/// pre-flight to remain a meaningful signal; refusing cleanly, naming the
/// count, is more honest than either OOMing or guessing. A `CRC.db`
/// shorter than `Data.db` needs no longer reaches this cap at all — see
/// `unverified_from`'s doc for why that case is now bounded by
/// CONSTRUCTION rather than by counting up to this limit.
const MAX_UNCOMPRESSED_BAD_CHUNKS: u64 = 65_536;

/// Chunk pre-flight outcome: genuinely bad chunks, an unverified tail (when
/// `CRC.db` is shorter than `Data.db`), and a summary finding per cause.
pub(super) struct ChunkPreflight {
    /// Chunk indices whose STORED CRC32 (from `CRC.db`, or inline for a
    /// compressed input) did NOT match the computed one — genuine,
    /// evidenced corruption. `recover.rs` intersects a partition's chunk
    /// range against this set to decide `LossClass::ChunkCrc`.
    ///
    /// Does NOT include chunks `CRC.db` has no entry for at all (roborev,
    /// issue #4196, round 21 Medium finding — corrects a round 17/19
    /// conflation that lived here): a `CRC.db` shorter than `Data.db`
    /// needs (every chunk past its own coverage) is NOT evidence those
    /// chunks are corrupt — Cassandra's own writer never guarantees a
    /// `CRC.db` covers a `Data.db` that grew or was concatenated after it,
    /// and a WHOLLY ABSENT `CRC.db` already gets the more honest
    /// "unverified, proceed anyway" treatment (`ChunkCrcUnavailable`) — a
    /// PARTIALLY-covering one was, until this round, treated WORSE than a
    /// wholly-absent one: every chunk past its coverage became an
    /// unrecoverable `chunk-crc` loss reporting "failed CRC validation",
    /// which is factually false (never validated at all). See
    /// `unverified_from` for that case instead.
    pub(super) bad_chunks: BTreeSet<u64>,
    /// The first uncompressed chunk index `CRC.db` carries no entry for —
    /// `Some` when `CRC.db` is SHORTER than `Data.db` needs, `None` when
    /// coverage is complete. `CrcDb`'s `crcs: Vec<u32>` is a flat,
    /// sequentially-indexed array (`crc.rs`), so a missing entry can only
    /// ever be a MONOTONIC TAIL truncation — the first `Err` means every
    /// later index is ALSO uncovered — never a hole in the middle. That
    /// fact is what makes this a single `Option<u64>` rather than a set:
    /// `uncompressed_chunk_preflight` STOPS scanning the instant it finds
    /// this (no need to read/hash the remaining file just to rediscover
    /// the same fact per-chunk), so this case is bounded by CONSTRUCTION
    /// (O(1) space) rather than needing `MAX_UNCOMPRESSED_BAD_CHUNKS`-style
    /// counting at all — the exact unbounded-growth shape rounds 17/19
    /// fixed for `bad_chunks` never applies here in the first place.
    /// `recover.rs` does NOT intersect partition ranges against this value
    /// (unverified is not evidence of corruption — those partitions are
    /// attempted normally, same as a wholly-absent `CRC.db`); it is
    /// surfaced to the manifest only as the `ChunkCrcUnavailable`
    /// `ComponentFinding` already built from it, INSIDE this function,
    /// before this struct is returned — so no PRODUCTION caller reads this
    /// field again once that finding exists. Kept on the struct (rather
    /// than a local-only value) because it is genuinely useful test
    /// introspection: a direct `Some(N)`/`None` assertion is a much
    /// cleaner regression oracle than parsing the finding's prose `detail`
    /// string for a chunk index.
    #[allow(
        dead_code,
        reason = "read only by this module's own tests; see doc above"
    )]
    pub(super) unverified_from: Option<u64>,
    /// One finding per CAUSE (mismatch, unverified-tail), never conflated
    /// into one.
    pub(super) findings: Vec<ComponentFinding>,
    /// The uncompressed chunk size that bounds this file's chunking — the
    /// same domain `Index.db`/`Partitions.db` offsets live in, so a
    /// partition's `[offset, end)` maps to chunk indices via
    /// [`chunks_for_range`] using this value.
    pub(super) chunk_size: u64,
    /// The total decompressed/uncompressed data-section length, when known
    /// (compressed: `CompressionInfo.data_length`; uncompressed: the total
    /// bytes the CRC.db walk actually scanned, OR — when it stopped early
    /// at `unverified_from` — the real file length from filesystem
    /// metadata, a trusted source independent of any on-disk field).
    /// `recover.rs` uses this — NOT `entry.data_offset + 1` — as the LAST
    /// partition's chunk-range `end`, so the pre-flight covers every chunk
    /// that partition's bytes actually span, not just the one containing
    /// its start (roborev, issue #4196). `0` when unknown (no `CRC.db`,
    /// uncompressed).
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
///
/// # Why no `MAX_UNCOMPRESSED_BAD_CHUNKS`-style cap here (roborev, issue
/// # #4196, round 21 Low finding — considered and deliberately NOT applied)
///
/// This function's `bad_chunks` has no explicit cap-and-refuse of its own,
/// unlike its uncompressed sibling — but it is ALREADY bounded, by TWO
/// independent, pre-existing mechanisms, to the SAME order of magnitude
/// that sibling's cap targets:
///
/// 1. `bad_chunks.len()` cannot exceed `chunk_reader.chunk_count()`, which
///    is `compression_info.chunk_offsets.len()` — and `CompressionInfo::parse`
///    already rejects `chunk_count > 1_000_000` at METADATA-parse time
///    (`compression_info.rs`), before this function ever runs. So
///    `bad_chunks` (a `BTreeSet<u64>`) is capped at ~1,000,000 entries
///    regardless of corruption — an ESTIMATED ~48 MB of B-tree node
///    overhead in the worst case (std's `BTreeSet` amortizes to roughly
///    32-50 bytes/entry depending on fill factor; not independently
///    measured for THIS crate, so read as an order-of-magnitude estimate,
///    not a precise figure), comparable to the uncompressed cap's own
///    explicitly-computed ~3 MB at 65,536 entries times an order of
///    magnitude — still within the crate's <128 MB target as a standalone
///    structure.
/// 2. The finding also named `chunks_for_range`'s PER-PARTITION
///    materialization as a residual risk if `chunk_length` were corrupted
///    to something tiny (more chunks per byte span) — but `data_length`
///    (used as `chunks_for_range`'s `end` bound via `recover.rs`'s clamp)
///    is ITSELF computed below as `min(declared, chunk_count *
///    chunk_length)` (`chunk_table_bound`, round-10/12 fix): a corrupted,
///    tiny `chunk_length` shrinks `chunk_table_bound` PROPORTIONALLY, so
///    the chunk COUNT any single partition's `chunks_for_range` call can
///    ever produce stays bounded by the SAME `chunk_count <= 1,000,000`
///    ceiling — never independent of it. Cross-checked directly against
///    `chunk_table_bound`'s own computation below, not asserted blindly.
///
/// A dedicated cap-and-refuse mirroring the uncompressed sibling's would
/// therefore be REDUNDANT with mechanism (1) here, not a new bound — added
/// only if a future change removes `CompressionInfo::parse`'s `chunk_count`
/// ceiling.
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
        unverified_from: None,
        findings: finding.into_iter().collect(),
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
            unverified_from: None,
            findings: vec![ComponentFinding {
                class: "ChunkCrcUnavailable".to_string(),
                component: "CRC.db".to_string(),
                detail: "CRC.db is absent for this uncompressed input; chunk-CRC validation \
                         did not run, so a flipped-but-still-parseable byte would not be \
                         detected by the chunk-crc loss class"
                    .to_string(),
            }],
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
    let mut unverified_from: Option<u64> = None;
    let mut first_mismatch_detail: Option<String> = None;
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
            // A genuine CRC32 mismatch — `CRC.db` covers this chunk and
            // disagrees with it. Real, evidenced corruption.
            Ok(_) => {
                bad_chunks.insert(chunk_index);
                if first_mismatch_detail.is_none() {
                    first_mismatch_detail = Some(format!("chunk {chunk_index}: CRC32 mismatch"));
                }
            }
            // roborev, issue #4196, round 21 Medium finding: `CRC.db` has
            // NO ENTRY for this chunk at all — `CRC.db` is SHORTER than
            // `Data.db` needs — which is UNVERIFIED, not evidence of
            // corruption. `CrcDb`'s backing `Vec<u32>` is a flat,
            // sequentially-indexed array (`crc.rs`), so this can only ever
            // be a MONOTONIC TAIL: every later index will ALSO fail the
            // same way. STOP here rather than keep reading/hashing the
            // rest of a potentially enormous file just to rediscover that
            // same fact per-chunk — `unverified_from`'s `Option<u64>`
            // shape (see its own doc) makes the WHOLE tail representable
            // in O(1) space, so this case no longer needs
            // `MAX_UNCOMPRESSED_BAD_CHUNKS`-style counting at all.
            Err(_) => {
                unverified_from = Some(chunk_index);
                break;
            }
        }
        // See `MAX_UNCOMPRESSED_BAD_CHUNKS`'s doc: refuse the WHOLE input
        // rather than let this set (and the memory it costs) grow without
        // bound, and rather than silently stop tracking later bad indices
        // (which would make partitions past this point read as falsely
        // chunk-crc-clean). Only genuine mismatches reach this cap now —
        // the unverified-tail case above already stopped before ever
        // inserting into a growing set.
        if bad_chunks.len() as u64 >= MAX_UNCOMPRESSED_BAD_CHUNKS {
            return Err(crate::Error::corruption(format!(
                "uncompressed chunk pre-flight found {} chunk(s) with a genuine CRC32 mismatch \
                 (of {} scanned so far) and stopped early rather than risk unbounded memory \
                 growth or silently undercounting later bad chunks; this input is too damaged \
                 for a per-partition chunk-CRC pre-flight to remain meaningful — first: {}",
                bad_chunks.len(),
                chunk_index + 1,
                first_mismatch_detail
                    .as_deref()
                    .unwrap_or("(no detail recorded)")
            )));
        }
        chunk_index += 1;
        if filled < buf.len() {
            break;
        }
    }

    let mut findings = Vec::new();
    if let Some(detail) = first_mismatch_detail {
        findings.push(ComponentFinding {
            class: "UncompressedChunkCrcMismatch".to_string(),
            component: "Data.db".to_string(),
            detail: format!(
                "{} of {} chunk(s) failed CRC.db validation; first: {detail}",
                bad_chunks.len(),
                chunk_index
            ),
        });
    }
    // roborev, issue #4196, round 21 Medium finding: a DISTINCT finding
    // from the mismatch one above — matching the wholly-absent-`CRC.db`
    // wording pattern (`ChunkCrcUnavailable`), scoped to the uncovered
    // TAIL only. `total_chunks_estimate` uses `data_len` (filesystem
    // metadata, a trusted source) rather than continuing to read/hash the
    // rest of the file just to count it — the whole point of stopping
    // early.
    if let Some(from) = unverified_from {
        let total_chunks_estimate = data_len.div_ceil(chunk_size.max(1));
        findings.push(ComponentFinding {
            class: "ChunkCrcUnavailable".to_string(),
            component: "CRC.db".to_string(),
            detail: format!(
                "CRC.db has no entry for chunk {from} onward (~{total_chunks_estimate} chunk(s) \
                 total, estimated from Data.db's real size) — shorter than Data.db needs; \
                 chunk-CRC validation did not run for that tail, so a flipped-but-still-\
                 parseable byte there would not be detected by the chunk-crc loss class (the \
                 partitions living there are still attempted normally, not treated as lost)"
            ),
        });
    }

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
    //
    // roborev, issue #4196, round 21 Medium finding: when the scan stopped
    // EARLY at `unverified_from`, `total_scanned` reflects only what was
    // physically read up to that point — NOT the real file length. Use
    // `data_len` (filesystem metadata, already measured above to open
    // `CrcDb`) instead, so `recover.rs`'s OOM-prevention clamp still sees
    // the file's TRUE size rather than an artificially-short one that
    // would make it refuse a perfectly recoverable CRC-verified prefix.
    let (chunk_size, data_length) = if unverified_from.is_some() {
        (chunk_size, data_len)
    } else if total_scanned == 0 {
        (0, 0)
    } else {
        (chunk_size, total_scanned)
    };

    Ok(ChunkPreflight {
        bad_chunks,
        unverified_from,
        findings,
        chunk_size,
        data_length,
    })
}

#[cfg(test)]
#[path = "chunks_tests.rs"]
mod tests;
