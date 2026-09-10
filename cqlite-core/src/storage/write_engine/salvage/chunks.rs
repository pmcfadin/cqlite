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
            if first_detail.is_none() {
                first_detail = Some(format!("chunk {i}: {e}"));
            }
        }
    }

    let finding = first_detail.map(|detail| ComponentFinding {
        class: "ChunkDecompressionError".to_string(),
        component: "Data.db".to_string(),
        detail: format!(
            "{} of {} chunk(s) failed inline CRC32 validation; first: {detail}",
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
    let data_length = compression_info.data_length.min(chunk_table_bound);

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

    Ok(ChunkPreflight {
        bad_chunks,
        finding,
        chunk_size,
        data_length: total_scanned,
    })
}

#[cfg(test)]
mod tests {
    use super::chunks_for_range;

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
