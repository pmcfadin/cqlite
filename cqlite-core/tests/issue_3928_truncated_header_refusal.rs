//! Issue #3928 — a partition header that RAN OUT of bytes, on a buffer the
//! caller has PROVEN complete, is DATA LOSS and must be REFUSED.
//!
//! The sibling lane (`issue_3928_corrupt_header_refusal.rs`) flips a header
//! BYTE, which makes `partition_header_readiness` answer `Malformed` or
//! `Ready`-then-structurally-invalid. A file that ENDS inside a header makes it
//! answer `Incomplete` — a different classifier arm, which those cases cannot
//! reach — and review found two swallows there.
//!
//! # Oracle (#3042)
//!
//! The bytes handed to each parse are a PREFIX of the fixture's own stitched
//! decompressed data section, which is exactly what a `Data.db` truncated by a
//! crashed writer or a short read presents to the reader. The cut offset is the
//! LAST partition header's position from `Index.db`, cross-checked against
//! `Data.db`'s own 2-byte key length AND its key bytes
//! (`support/corrupt_byte_fixture.rs::last_partition_header_offset`) — two
//! independently-written Cassandra components agreeing. Nothing is rewritten, so
//! there is no re-compression or CRC arithmetic to get wrong.
//!
//! # Pre-fix measurements
//!
//! Round 1, on `main` @ 05134c947 + the round-0 fix:
//!
//! * the two stitched walks DISAGREED — with one header byte surviving the block
//!   walk answered `Ok` with **99** of 100 partition keys while the cell-metadata
//!   walk answered `Err("Unexpected end at partition key length")`, on the same
//!   bytes. `data_access/mod.rs:249` and `:288` hand both the SAME stitched
//!   `Complete` buffer, so that is `SELECT *` answering `Ok` and
//!   `SELECT *, WRITETIME(c)` answering `Err` over one file.
//! * the sliding drivers reported a truncated header as CLEAN COMPLETION: a
//!   header declaring a 16-byte key with only 10 bytes present answered `Done`.
//!
//! Round 2 (findings C1/C2), after the round-1 fix:
//!
//! * a ONE-BYTE final chunk was still `Done` — and that byte can be the
//!   surviving first byte of a key length, so the partition IS lost. Measured:
//!   `Done after emitting 0 row(s) over 1 byte(s) of a header that declares a
//!   16-byte key`, and three-way `driver: Ok, block-emit walk: Err,
//!   cell-metadata walk: Err` — a DRIVER-vs-block-emit divergence that the
//!   block-vs-metadata assertion could not see.
//! * a BOUNDED walk (`row_body_window`) still resynced past the malformed
//!   INITIAL header: `Skipping malformed partition header at offset 0 … key
//!   length of 0 … (partition=0)`.
//!
//! Tolerance is now one explicit state, `HeaderTolerance` (in
//! `row_decoder/buffer_extent.rs`): *can a byte still arrive, or is this walk's
//! progress no longer attributable?* Both header arms consult it and no byte
//! count decides anything.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    // Same three load-bearing gates, for the same reasons, as the sibling lane:
    // `cli-helpers` for the `ingestion`/`Database` surface and `lz4` because
    // every fixture read here is an LZ4-compressed Cassandra 5.0 SSTable.
    // `Cargo.toml`'s `required-features` must agree with this list.
    feature = "lz4"
))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;
#[path = "support/multiset.rs"]
mod multiset;
#[path = "support/header_refusal.rs"]
mod support;

use fixture::BIG_COMPOSITE_HEADER;
use support::*;

#[tokio::test]
async fn both_stitched_walks_agree_and_refuse_a_truncated_final_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let dir = fixture_dir(spec);
    let schema = table_schema(spec);
    let reader = open_reader(&dir).await;

    for cut in [
        HeaderCut::OneByte,
        HeaderCut::InsideKey,
        HeaderCut::InsideDeletionTime,
    ] {
        let (dec, hdr, keep) = truncation(&dir, cut);

        // CONTROL: the untruncated section decodes completely on BOTH walks, and
        // they agree. This is what makes a refusal below attributable to the
        // truncation rather than to this parser's configuration.
        let control_block = block_walk(spec, &schema, &reader, &dec).unwrap_or_else(|e| {
            panic!("{cut:?}: the PRISTINE section must decode on the block walk: {e}")
        });
        let control_meta = metadata_walk(spec, &schema, &reader, &dec).unwrap_or_else(|e| {
            panic!("{cut:?}: the PRISTINE section must decode on the metadata walk: {e}")
        });
        assert!(
            !control_block.is_empty(),
            "0-rows-when-present: {cut:?} control block walk emitted nothing"
        );
        assert_eq!(
            multiset::multiset(control_block.iter().cloned()),
            multiset::multiset(control_meta.iter().cloned()),
            "{cut:?}: the two stitched walks must agree on the PRISTINE section"
        );

        let got_block = block_walk(spec, &schema, &reader, &dec[..keep]);
        let got_meta = metadata_walk(spec, &schema, &reader, &dec[..keep]);

        // The DIVERGENCE assertion: whatever the answer is, one query variant may
        // not disagree with the other about the same bytes.
        assert_eq!(
            got_block.is_err(),
            got_meta.is_err(),
            "{cut:?} (truncated to {keep} of {} bytes, last header at {hdr}): the two \
             stitched walks DISAGREE — block walk {} / metadata walk {}. These are \
             `SELECT *` and `SELECT *, WRITETIME(col)` over one file.",
            dec.len(),
            got_block
                .as_ref()
                .map_or_else(|e| format!("Err({e})"), |k| format!("Ok({} keys)", k.len())),
            got_meta
                .as_ref()
                .map_or_else(|e| format!("Err({e})"), |k| format!("Ok({} keys)", k.len())),
        );

        // And on an UNBOUNDED, proven-complete buffer both must refuse: the
        // partition Cassandra wrote there is gone, so answering `Ok` reports a
        // table with one fewer partition than the file was written with.
        if let Ok(keys) = &got_block {
            let got = multiset::multiset(keys.iter().cloned());
            let control = multiset::multiset(control_block.iter().cloned());
            let lost = multiset::deficit(&got, &control);
            panic!(
                "{cut:?} (truncated to {keep} of {} bytes): the block walk answered Ok with \
                 {} key occurrence(s) against a control of {} — {} LOST [{}]. A truncated \
                 header on a proven-complete buffer is DATA LOSS and must be refused.",
                dec.len(),
                keys.len(),
                control_block.len(),
                lost.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&lost)
            );
        }
    }
}

/// B1(a)/C1 — the sliding drivers' `Incomplete`-at-the-final-chunk answer.
///
/// `DriverHeader::Done` is read by BOTH drivers as clean completion
/// (`ParseStep::Done` / `PartitionStreamStep::AllDone`), so it is truthful only
/// where there is genuinely nothing there. At the final chunk a NON-EMPTY
/// incomplete header is a header Cassandra wrote whose bytes were truncated
/// away — including the one-byte case, because that byte can be the surviving
/// first byte of a partition key length.
///
/// Round 2 (C1) made the one-byte case fatal too. It had been kept tolerant as a
/// both-directions guard, which was guarding the wrong thing: the proof that
/// this case cannot pass by refusing everything now comes from a leg that is
/// LEGITIMATELY tolerant — the same cuts at `at_final_chunk == false`, where
/// more bytes can still arrive and `NeedMore` is the correct answer.
///
/// Evidence that no caller legitimately leaves one byte at the final chunk,
/// since making it fatal rests on that: (1) both drivers early-return on an
/// EMPTY buffer (`partition_driver.rs`, `compaction_stream.rs`), so the arm sees
/// at least one byte by construction; (2) every `Emitted(consumed)` /
/// `PartitionDone(consumed)` counts the structural terminator, and the callers
/// advance by exactly that (`scan_stream_windowed.rs`'s `window.consume(take)`,
/// `drain_compaction_window`), so a well-formed walk leaves ZERO bytes, never
/// one; (3) measured — the AC3 counters recorded ZERO arrivals at this arm at
/// the final chunk across 126 tables / 148 SSTables on four surfaces, two of
/// which drive these drivers.
#[tokio::test]
async fn the_sliding_driver_refuses_any_non_empty_truncated_header_at_the_final_chunk() {
    let spec = &BIG_COMPOSITE_HEADER;
    let dir = fixture_dir(spec);
    let schema = table_schema(spec);
    let reader = open_reader(&dir).await;

    // CONTROL: the last partition, whole, drives to an Emitted step.
    let (dec, hdr, _) = truncation(&dir, HeaderCut::OneByte);
    let whole = driver_at_final_chunk(spec, &schema, &reader, &dec[hdr..]).unwrap_or_else(|e| {
        panic!("the PRISTINE last partition must drive cleanly at the final chunk: {e}")
    });
    assert!(
        whole.starts_with("Emitted"),
        "control: the whole last partition must report Emitted(consumed), got {whole}"
    );

    for cut in [
        HeaderCut::OneByte,
        HeaderCut::InsideKey,
        HeaderCut::InsideDeletionTime,
    ] {
        let (dec, hdr, keep) = truncation(&dir, cut);
        let declared = u16::from_be_bytes([dec[hdr], dec[hdr + 1]]);

        // The LEGITIMATELY-TOLERANT leg: the same bytes mid-stream, where a
        // header may still be completed by the next chunk. `NeedMore` is the
        // correct answer and this case must not turn it into an error — that is
        // what stops it passing by refusing everything.
        let midstream =
            driver_mid_stream(spec, &schema, &reader, &dec[hdr..keep]).unwrap_or_else(|e| {
                panic!(
                    "{cut:?}: mid-stream a truncated header must ask for more bytes, not \
                     refuse — more bytes can still arrive there: {e}"
                )
            });
        assert!(
            midstream.starts_with("NeedMore"),
            "{cut:?}: mid-stream must report NeedMore, got {midstream}"
        );

        // At the final chunk every one of them is DATA LOSS.
        let got = driver_at_final_chunk(spec, &schema, &reader, &dec[hdr..keep]);
        assert!(
            got.is_err(),
            "C1 {cut:?}: a NON-EMPTY truncated header must be REFUSED at the final chunk, \
             not reported as completion. The driver answered {} over {} byte(s) of a header \
             that declares a {declared}-byte key — the partition Cassandra wrote there is \
             gone, and both drivers read that answer as a clean end of walk.",
            got.as_ref().map_or_else(|e| e.clone(), |s| s.clone()),
            keep - hdr
        );
    }
}

/// C1's second half — the DRIVER path and the two BLOCK-EMIT walks must agree.
///
/// `both_stitched_walks_agree_and_refuse_a_truncated_final_header` pins the
/// block-vs-metadata pair, so it stays green while a DRIVER-vs-block-emit
/// divergence survives — and one did: with one header byte surviving, the
/// drivers reported clean completion while `block_emit` refused the identical
/// bytes. Three surfaces, ONE buffer, one verdict.
#[tokio::test]
async fn the_driver_and_both_block_walks_agree_on_a_truncated_final_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let dir = fixture_dir(spec);
    let schema = table_schema(spec);
    let reader = open_reader(&dir).await;

    for cut in [
        HeaderCut::OneByte,
        HeaderCut::InsideKey,
        HeaderCut::InsideDeletionTime,
    ] {
        let (dec, hdr, keep) = truncation(&dir, cut);
        // ONE buffer for all three: it begins at a partition header and is
        // declared complete and unbounded, so every surface is being asked the
        // same question about the same bytes.
        let buf = &dec[hdr..keep];
        let driver = driver_at_final_chunk(spec, &schema, &reader, buf);
        let block = block_walk(spec, &schema, &reader, buf);
        let meta = metadata_walk(spec, &schema, &reader, buf);

        let verdicts = [
            ("driver (at_final_chunk)", driver.is_err()),
            ("block-emit walk", block.is_err()),
            ("cell-metadata walk", meta.is_err()),
        ];
        assert!(
            verdicts.iter().all(|(_, e)| *e) || verdicts.iter().all(|(_, e)| !*e),
            "{cut:?}: the three walks DISAGREE about the same {} byte(s) — {}. A truncated \
             header cannot be corruption on one surface and clean completion on another.",
            buf.len(),
            verdicts
                .iter()
                .map(|(n, e)| format!("{n}: {}", if *e { "Err" } else { "Ok" }))
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert!(
            verdicts.iter().all(|(_, e)| *e),
            "{cut:?}: all three walks must REFUSE — they agreed on Ok, which reports a \
             table with one fewer partition than the file holds"
        );
    }
}

/// C2 — a BOUNDED walk must still refuse a malformed INITIAL header.
///
/// The `bounded` flag disabled refusal for the WHOLE call, so
/// `BufferExtent::Complete` plus `row_body_window: Some(..)` byte-resynced past
/// the FIRST partition's header — before any window logic had run, and therefore
/// before the window had introduced any uncertainty at all. The window's
/// uncertainty begins only once the bounded row-body endpoint has been reached.
///
/// The resync WARN's absence is asserted alongside the refusal, because on this
/// fixture the resync's garbage can trip #3782's row arm and produce an
/// incidental `Err` — the same trap the BIG AC1 case documents. The positive
/// control is the same bounded walk on a `Window` extent, which must still
/// resync and log.
#[tokio::test]
async fn a_bounded_walk_still_refuses_a_malformed_initial_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "bounded-initial");
    let schema = table_schema(spec);
    let reader = open_reader(&staged.mutated_dir).await;
    let mutated = fixture::stitched_data_section(&staged.mutated_dir);

    // A well-formed clustering-slice request. The VALUES are immaterial — the
    // malformed header sits at offset 0 and is reached before `row_body_window`
    // is consulted at all — so they are taken from the PRISTINE section's first
    // partition body so the request is a realistic one rather than arbitrary.
    let pristine = fixture::stitched_data_section(&staged.control_dir);
    let key_len = usize::from(u16::from_be_bytes([pristine[0], pristine[1]]));
    let body_start = 2 + key_len + 12; // nb: fixed 12-byte partition DeletionTime
    let window = (body_start, (body_start + 64).min(mutated.len()));

    // POSITIVE CONTROL: on a Window the bounded walk must still take the
    // tolerant resync and LOG it, which proves the sink captures and the needles
    // are current.
    let window_sink = LogSink::default();
    let window_result = {
        let _dispatch = tracing::subscriber::set_default(warn_subscriber(&window_sink));
        bounded_walk(
            spec,
            &schema,
            &reader,
            &mutated,
            window,
            cqlite_core::storage::sstable::reader::BufferExtent::Window,
        )
    };
    let window_logs = window_sink.text();
    let observed: Vec<&str> = RESYNC_WARNS
        .iter()
        .copied()
        .filter(|n| window_logs.contains(n))
        .collect();
    assert!(
        !observed.is_empty(),
        "positive control FAILED: a BOUNDED walk on a Window extent must still resync past \
         the malformed initial header and LOG it. Expected one of {RESYNC_WARNS:?}; the walk \
         answered {} and captured:\n{window_logs}",
        window_result
            .as_ref()
            .map_or_else(|e| format!("Err({e})"), |k| format!("Ok({} keys)", k.len()))
    );

    // The subject: the SAME bounded walk on a COMPLETE buffer must refuse the
    // initial header, and must not have resynced past it.
    let sink = LogSink::default();
    let got = {
        let _dispatch = tracing::subscriber::set_default(warn_subscriber(&sink));
        bounded_walk(
            spec,
            &schema,
            &reader,
            &mutated,
            window,
            cqlite_core::storage::sstable::reader::BufferExtent::Complete,
        )
    };
    let logs = sink.text();
    for needle in RESYNC_WARNS {
        assert!(
            !logs.contains(needle),
            "C2: a bounded walk RESYNCHRONISED past the malformed INITIAL header — it logged \
             {needle:?}. The row-body window introduces no uncertainty until its endpoint is \
             reached, so partition 0's own header is as attributable as on an unbounded walk \
             and must be REFUSED. (Capture proved live above: {observed:?}.) Captured:\n{logs}"
        );
    }
    assert!(
        got.is_err(),
        "C2: a bounded walk over a PROVEN-COMPLETE buffer must refuse a malformed initial \
         header; it answered Ok with {} key occurrence(s)",
        got.map_or(0, |k| k.len())
    );
}
