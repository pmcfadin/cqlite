//! Unit tests for `chunks.rs`. Split out (round 21, campsite rule / epic
//! #1116) when `chunks.rs` crossed the ~800-line source threshold — a PURE
//! MOVE, no behavior changed. `chunks.rs` is a file THIS PR created
//! (issue #4196), so the `CQLITE_ALLOW_FILE_GROWTH=1` opt-out — reserved
//! for pre-existing files — does not apply to it; split rather than
//! carried under an opt-out. Wired via `#[path = "chunks_tests.rs"] mod
//! tests;` inside `chunks.rs` so `super::` here still resolves to
//! `chunks.rs`'s own scope, matching the flat-sibling-file convention
//! `recover.rs`/`recover_helpers.rs` already established.

use super::{chunks_for_range, uncompressed_chunk_preflight, MAX_UNCOMPRESSED_BAD_CHUNKS};

/// Roborev, issue #4196, round 22 Low finding: this doc paragraph itself
/// used to state the PRE-round-21 contract here, contradicting this
/// function's own name and every assertion in its body — fixed to
/// describe the CURRENT contract.
///
/// A `CRC.db` with ZERO real entries against a large `Data.db` makes the
/// VERY FIRST chunk take the "no entry" `Err` arm — `uncompressed_chunk_preflight`
/// STOPS scanning right there (`CrcDb`'s backing array is a flat,
/// sequentially-indexed `Vec`, so a missing entry is always a monotonic
/// tail — every later index would ALSO miss), reports `unverified_from ==
/// Some(0)`, and returns `Ok` — the input is NOT refused; those partitions
/// are attempted normally, same as a wholly-absent `CRC.db`. `Data.db` is
/// SPARSE-EXTENDED via `File::set_len()` (logical length only, no real
/// bytes written/allocated — the same technique
/// `issue_4196_salvage_round15_bounds.rs` uses for its 128 MiB
/// span-ceiling test) so this stays a fast, deterministic unit test
/// rather than needing a genuinely multi-hundred-MB fixture; it is
/// comfortably past the size that would have crossed
/// `MAX_UNCOMPRESSED_BAD_CHUNKS` under the PRE-round-21 behavior, proving
/// the fix does not merely raise that threshold but removes it from this
/// code path entirely.
///
/// **Round 17 Medium finding's ORIGINAL contract (superseded round 21,
/// stated here for history only — do NOT restore it)**: this same
/// scenario used to be required to REFUSE the whole input once
/// `bad_chunks` grew past `MAX_UNCOMPRESSED_BAD_CHUNKS`, because "no
/// entry" was conflated into the same set as a genuine CRC32 mismatch.
/// Round 21 (roborev Medium finding) split the two: "no entry" is
/// UNVERIFIED, not evidence of corruption, so refusing on it was itself
/// the defect that round fixed.
#[tokio::test]
async fn short_crc_db_stops_early_and_reports_unverified_not_bad() {
    use crate::storage::sstable::reader::crc::MIN_CRC_CHUNK_SIZE;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_path = temp.path().join("nb-1-big-Data.db");
    let crc_path = temp.path().join("nb-1-big-CRC.db");

    let chunk_size = MIN_CRC_CHUNK_SIZE as u64;
    // Comfortably past the OLD cap (`MAX_UNCOMPRESSED_BAD_CHUNKS`) so a
    // pre-fix implementation that still conflated "no entry" with
    // "mismatch" would have hit it; this fix's whole point is that the
    // scan now stops at chunk 0 instead, so this large a file is no
    // longer even fully read.
    let sparse_len = (MAX_UNCOMPRESSED_BAD_CHUNKS + 100) * chunk_size;
    let file = std::fs::File::create(&data_path).expect("create Data.db");
    file.set_len(sparse_len).expect("sparse-extend Data.db");

    // CRC.db: header (chunk_size) only, ZERO trailing CRC entries — every
    // real chunk lookup misses from chunk 0 onward.
    std::fs::write(&crc_path, (chunk_size as i32).to_be_bytes()).expect("write CRC.db header");

    // roborev, issue #4196, round 21 Medium finding: a `CRC.db` that
    // covers NONE (or only some) of `Data.db`'s real chunks is
    // UNVERIFIED, not evidence of corruption — the pre-fix behavior
    // (refuse the whole input once `MAX_UNCOMPRESSED_BAD_CHUNKS` was
    // crossed by conflating "no entry" into the bad-chunks count) has
    // been replaced with: stop scanning at the first uncovered index
    // (bounded, O(1) space — no cap needed at all for this case) and
    // report it as `unverified_from`, letting the input proceed to
    // recovery like a WHOLLY absent `CRC.db` already does.
    let preflight = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        uncompressed_chunk_preflight(&data_path, &crc_path, 0),
    )
    .await
    .expect(
        "must not hang — stopping at the first unverified chunk is a bounded-time \
         operation regardless of how large Data.db claims to be",
    )
    .expect(
        "a CRC.db shorter than Data.db needs must NOT refuse the whole input — those \
         partitions are unverified, not corrupt",
    );

    assert_eq!(
        preflight.unverified_from,
        Some(0),
        "CRC.db covers ZERO chunks here, so the very first chunk index must be reported \
         unverified; got {:?}",
        preflight.unverified_from
    );
    assert!(
        preflight.bad_chunks.is_empty(),
        "an unverified chunk must NEVER be recorded as a genuine CRC mismatch — got {:?}",
        preflight.bad_chunks
    );
    assert!(
        preflight
            .findings
            .iter()
            .any(|f| f.class == "ChunkCrcUnavailable"),
        "the unverified tail must surface as a ChunkCrcUnavailable finding, matching the \
         wholly-absent-CRC.db case's posture; got {:?}",
        preflight.findings
    );
    assert!(
        !preflight
            .findings
            .iter()
            .any(|f| f.class == "UncompressedChunkCrcMismatch"),
        "no genuine mismatch was ever found (nothing was ever CHECKED), so no mismatch \
         finding may be present; got {:?}",
        preflight.findings
    );
    // `data_length` must still be a real, usable value (the file's true
    // size via metadata) — NOT zeroed just because the scan stopped
    // early — so `recover.rs`'s OOM-prevention clamp keeps working.
    assert_eq!(preflight.data_length, sparse_len);
}

/// The genuine-mismatch cap still works, unaffected by the split: a
/// `CRC.db` that DOES cover every chunk, but every stored CRC32 is
/// WRONG, must still refuse once the (now mismatch-only)
/// `MAX_UNCOMPRESSED_BAD_CHUNKS` cap is crossed — proving the round-21
/// split did not silently disable the round-17/19 memory-safety
/// guarantee for the case it was ACTUALLY built for (real corruption).
#[tokio::test]
async fn genuinely_corrupt_crc_db_still_refuses_at_the_cap() {
    use crate::storage::sstable::reader::crc::MIN_CRC_CHUNK_SIZE;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_path = temp.path().join("nb-1-big-Data.db");
    let crc_path = temp.path().join("nb-1-big-CRC.db");

    let chunk_size = MIN_CRC_CHUNK_SIZE as u64;
    let chunk_count = MAX_UNCOMPRESSED_BAD_CHUNKS + 100;
    let sparse_len = chunk_count * chunk_size;
    let file = std::fs::File::create(&data_path).expect("create Data.db");
    file.set_len(sparse_len).expect("sparse-extend Data.db");

    // A sparse (all-zero) Data.db chunk has ONE real CRC32 value —
    // compute it once and write a DELIBERATELY WRONG one (its bitwise
    // complement, guaranteed different) as every entry, so every chunk
    // is a genuine, real mismatch, not merely uncovered.
    let real_crc = crc32fast::hash(&vec![0u8; chunk_size as usize]);
    let wrong_crc = !real_crc;
    let mut crc_db_bytes = (chunk_size as i32).to_be_bytes().to_vec();
    for _ in 0..chunk_count {
        crc_db_bytes.extend_from_slice(&wrong_crc.to_be_bytes());
    }
    std::fs::write(&crc_path, &crc_db_bytes).expect("write CRC.db");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        uncompressed_chunk_preflight(&data_path, &crc_path, 0),
    )
    .await
    .expect("must not hang");

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!(
            "a CRC.db covering every chunk with a GENUINELY WRONG CRC32 must still refuse \
             once MAX_UNCOMPRESSED_BAD_CHUNKS mismatches accumulate"
        ),
    };
    let msg = err.to_string();
    assert!(
        msg.contains(&MAX_UNCOMPRESSED_BAD_CHUNKS.to_string()),
        "refusal should name the cap so an operator understands why; got: {msg}"
    );
    assert!(
        msg.contains("genuine CRC32 mismatch"),
        "refusal must be attributed to genuine mismatches, not conflated with 'no entry'; \
         got: {msg}"
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
