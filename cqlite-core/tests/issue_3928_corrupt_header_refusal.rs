//! Issue #3928 — a partition HEADER that cannot be decoded on a buffer PROVEN
//! COMPLETE is DATA LOSS, so the walk must REFUSE instead of skipping a byte and
//! resynchronising.
//!
//! # The defect
//!
//! #3782 closed the ROW arm: a row decode error on a proven-complete stitched
//! buffer (`BufferExtent::Complete`) or at the sliding driver's final chunk
//! (`at_final_chunk`) now surfaces. The partition-HEADER arms were untouched and
//! still answered a malformed or undecodable header with
//! `tracing::warn!` + `offset += 1` (block-emit) / `ParseStep::Emitted(1)` and
//! `PartitionStreamStep::Consumed(1)` (the two drivers — the same byte resync,
//! spelled as a consumed-byte count).
//!
//! Two consequences, and the second is the bad one:
//!
//! 1. the malformed partition is silently DROPPED;
//! 2. the one-byte resync can land on MISALIGNED bytes that parse as a plausible
//!    header, so the walk INVENTS a partition that does not exist.
//!
//! Per the issue body (a figure from #3782's own measurement probe, cited here
//! rather than re-measured): **15 of 35** corrupted read-path losses were
//! attributable to the header arm against 20 to the row arm, so this was the
//! larger share. The numbers this file establishes itself are below.
//!
//! **The row COUNT goes UP while data is lost**, which is why nothing in this
//! file compares counts: every assertion is over the MULTISET of partition keys
//! against the pristine, Cassandra-written control.
//!
//! # Oracle (#3042)
//!
//! Every expectation comes from Cassandra-written bytes or from the pinned
//! `cassandra-5.0.8` source, never from CQLite's prior behaviour:
//!
//! * the control leg is the untouched Cassandra fixture, and the mutated leg
//!   differs from it by exactly ONE decompressed byte (asserted by the harness);
//! * both sites are inside the header `SortedTablePartitionWriter.start` writes
//!   — the writer BOTH formats share
//!   (`SortedTablePartitionWriter.java:97-105`): a key with
//!   `ByteBufferUtil.writeWithShortLength` (`ByteBufferUtil.java:362-368`), then
//!   the partition-level `DeletionTime` in the version-selected form
//!   (`DeletionTime.java:191-196`);
//! * the BIG (`nb`) site is the low byte of that 2-byte key length, zeroed — so
//!   the header declares a 0-byte key while the key Cassandra wrote still
//!   follows, and every later structure is misframed;
//! * the BTI (`da`) site is the partition-level `DeletionTime` discriminator,
//!   which Cassandra writes as exactly `IS_LIVE_DELETION = 0b1000_0000` for a
//!   live partition and whose own reader THROWS on any other byte with that bit
//!   set — `if ((flags & 0xFF) != IS_LIVE_DELETION) throw new IOException(
//!   "Corrupted sstable. Invalid flags found deserializing DeletionTime")`
//!   (`DeletionTime.java:208-230`). So refusal is the FORMAT's expectation.
//!
//! # Pre-fix measurement
//!
//! Measured by running THIS file against `main` @ 05134c947 (i.e. with only the
//! src half of the fix reverted), `CQLITE_DATASETS_ROOT=/data/datasets`.
//!
//! On the BTI (`da`) fixture — the discriminator flipped to `0xFF` — **all six
//! surfaces answered `Ok`**:
//!
//! | surface                                   | control | before the fix |
//! |-------------------------------------------|---------|----------------|
//! | `distinct_partition_keys`                 | 3 keys  | `Ok`, **5** keys — 1 real key LOST, 3 FABRICATED |
//! | `partition_verify_scan`                   | 3       | `Ok`, **5** — same split |
//! | `get_all_entries`                         | 468 rows| `Ok`, **0** — the whole table, silently |
//! | `iterate_all_partitions`                  | 468     | `Ok`, **0** |
//! | `iterate_all_partitions_for_compaction`   | 468     | `Ok`, **404** — 180 rows LOST, 116 FABRICATED |
//! | `stream_all_partitions_for_compaction`    | 468     | `Ok`, **404** — same split |
//!
//! The fabricated keys are byte strings like `"bos8-p1bos8-p1…"` — clustering
//! and payload bytes read as a partition key, i.e. partitions the SSTable does
//! not contain. Compaction would have written that back to disk.
//!
//! On the BIG (`nb`) fixture the pre-fix surfaces already answered `Err`, but
//! INCIDENTALLY: the header arm resynchronised and #3782's ROW arm then refused
//! the garbage it landed on. That is why the BIG case asserts the ABSENCE of the
//! resync WARN as well as the refusal — see its own comment.
//!
//! # The SIBLING lane — a header that RAN OUT (`Incomplete`)
//!
//! A flipped byte and a truncated file are different corruption classes and
//! reach different classifier arms, so the cases here CANNOT see a truncation
//! swallow — review found two, plus a driver-vs-block-emit divergence. Those
//! live in `issue_3928_truncated_header_refusal.rs`, with their own pre-fix
//! measurements; the scaffolding both lanes share is
//! `support/header_refusal.rs`.
//!
//! # DECLARED RESIDUAL — the partition-key LENGTH model (#3999)
//!
//! Every refusal here that rests on a declared key length says what **CQLite
//! READ**, never what Cassandra declared, and carries a `#3999` note. Cassandra
//! writes that length as an unsigned 2-byte big-endian value with NO flags byte
//! (`SortedTablePartitionWriter.start` →
//! `ByteBufferUtil.writeWithShortLength`); CQLite reads byte 0 as flags and byte
//! 1 as a one-byte length, so the models agree only for keys under 256 bytes.
//! For a longer key CQLite reads a length of `0` and these arms would refuse a
//! legitimate table. Correcting the model is a different family needing a
//! 256-byte-key fixture the corpus does not have — **#3999**. The refusal stays
//! (loud beats silent, which is this issue's whole subject) and names the issue.
//! No corpus table has a key that long, which is why the negative control below
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    // EVERY fixture this target reads is LZ4-compressed (the harness ASSERTS it
    // before mutating), and without the `lz4` feature the production
    // decompressor answers `Err("LZ4 compression not available")`, so the target
    // would still RUN and fail on its PRISTINE CONTROLS — a false FAIL wearing a
    // correctness failure's clothes (#3950). `Cargo.toml`'s `required-features`
    // for this target must always agree with this list.
    feature = "lz4"
))]

use std::collections::BTreeSet;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;
#[path = "support/multiset.rs"]
mod multiset;
#[path = "support/header_refusal.rs"]
mod support;

use fixture::{BIG_COMPOSITE_HEADER, BTI_MULTICLUSTERING_HEADER};
use support::*;

/// partition is lost unless the error is reported.
#[tokio::test]
async fn every_proven_complete_surface_refuses_a_malformed_partition_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "hdr-big");
    let schema = table_schema(spec);

    let control_partitions = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control_partitions, spec);

    // The refusal is asserted TOGETHER with the absence of the resync, because on
    // THIS fixture the refusal alone does not pin #3928: measured pre-fix, all six
    // surfaces already answered `Err` here — but with a ROW decode error
    // (`Clustering 'clustering_key_1': invalid UTF-8`) raised by #3782's arm AFTER
    // the header arm had resynchronised onto misaligned bytes. That green was
    // incidental: it depended on the garbage the resync landed on failing later.
    // So this case also requires the resync itself not to have happened — which
    // pre-fix it demonstrably did, twice, and which no surface in this set may do,
    // since every one of them walks a full extent.
    // I1: a bare `!logs.contains(..)` cannot tell "the resync did not happen"
    // from "the capture is broken" — and post-fix the mutated leg is expected to
    // warn little or nothing, so an EMPTY buffer would satisfy it. A
    // `multi_thread` flavour, a subscriber change or a `warn!`→`debug!` move
    // would all make it pass vacuously.
    //
    // So the absence is asserted only alongside a POSITIVE CONTROL that differs
    // in exactly ONE property: the SAME corrupt bytes, through the SAME parse, on
    // a `BufferExtent::Window` instead of `Complete`. The tolerant path is
    // load-bearing there (a header may straddle the chunk tail), so it MUST still
    // resync and MUST still log — which proves the sink captures, the needle is
    // current, and the discriminator is the extent and nothing else.
    //
    // This is stronger than borrowing a neighbouring subsystem's WARN (the #2302
    // detour, which is what the sibling `issue_3782_corrupt_row_refusal.rs` case
    // uses): that would prove the capture works while saying nothing about which
    // arm produced it.
    let mutated_section = fixture::stitched_data_section(&staged.mutated_dir);
    let reader = open_reader(&staged.mutated_dir).await;

    let window_sink = LogSink::default();
    let window_result = {
        let _dispatch = tracing::subscriber::set_default(warn_subscriber(&window_sink));
        window_walk(spec, &schema, &reader, &mutated_section)
    };
    let window_logs = window_sink.text();
    let observed: Vec<&str> = RESYNC_WARNS
        .iter()
        .copied()
        .filter(|n| window_logs.contains(n))
        .collect();
    assert!(
        !observed.is_empty(),
        "positive control FAILED: the SAME corrupt bytes on a `BufferExtent::Window` must \
         still take the tolerant resync and LOG it, or the absence assertion below proves \
         nothing about the arm. Expected one of {RESYNC_WARNS:?}; the Window walk answered \
         {} and captured:\n{window_logs}",
        window_result
            .as_ref()
            .map_or_else(|e| format!("Err({e})"), |k| format!("Ok({} keys)", k.len()))
    );

    let sink = LogSink::default();
    // A THREAD-LOCAL dispatcher (`set_default`), never a global one: sibling cases
    // in this target walk the whole corpus on other threads and a global sink
    // would mix their WARNs in.
    let mutated = {
        let _dispatch = tracing::subscriber::set_default(warn_subscriber(&sink));
        observe(&staged.mutated_dir, &schema).await
    };
    let logs = sink.text();
    for needle in RESYNC_WARNS {
        assert!(
            !logs.contains(needle),
            "AC1: a full-extent walk RESYNCHRONISED past the corrupt header — it logged \
             {needle:?} and skipped a byte, which both drops this partition and can invent \
             another out of misaligned bytes. On a proven-complete buffer that arm must \
             REFUSE. (The capture is proved live by the Window positive control above, \
             which observed {observed:?}.) Captured WARN output was:\n{logs}"
        );
    }

    let tolerated = tolerating(&control_observed, &mutated);
    assert!(
        tolerated.is_empty(),
        "AC1: with the first partition header's key-length byte zeroed (decompressed offset {}, \
         {} position(s) changed) on a single-chunk fixture, every surface reaches its parse with \
         the buffer PROVEN COMPLETE, so each must REFUSE rather than byte-resync. {} of {} \
         surfaces still answered Ok:\n  {}",
        staged.mutated_offset,
        staged.mutated_span,
        tolerated.len(),
        mutated.len(),
        tolerated.join("\n  ")
    );
}

/// AC2 — NO FABRICATION: a single corrupted header byte may never cause a
/// partition to be emitted that is not in the pristine fixture.
///
/// This is deliberately weaker than AC1 and therefore MORE durable: it holds
/// even for a surface that legitimately tolerates (a window-bounded walk), and
/// it is the property the resync violated. A surplus key occurrence — including
/// a SURPLUS DUPLICATE of a real key, which a set comparison cannot see — is a
/// partition the SSTable does not contain.
///
/// Asserted over the multiset of emitted partition keys against the pristine
/// control, NEVER over a row count: the measured failure RAISES the count.
#[tokio::test]
async fn no_surface_fabricates_a_partition_from_a_corrupted_header_byte() {
    // BOTH fixtures, because the pre-fix fabrication was MEASURED on the BTI one
    // (`distinct_partition_keys` answered Ok with 5 keys where the fixture holds
    // 3) while the BIG one lost partitions instead. A case that covered only the
    // fixture where the harm was loss would assert this property where it was
    // never violated.
    let mut fabricated: Vec<String> = Vec::new();
    for (spec, tag) in [
        (&BIG_COMPOSITE_HEADER, "fab-big"),
        (&BTI_MULTICLUSTERING_HEADER, "fab-bti"),
    ] {
        let staged = fixture::stage_spec(spec, &fixture_dir(spec), tag);
        let schema = table_schema(spec);
        let control_partitions = control_keys(&staged.control_dir, spec).await;
        let control_observed = observe(&staged.control_dir, &schema).await;
        assert_control_is_healthy(&control_observed, &control_partitions, spec);
        let mutated = observe(&staged.mutated_dir, &schema).await;
        fabricated.extend(
            fabricating(&control_observed, &mutated)
                .into_iter()
                .map(|d| format!("{}.{}: {d}", spec.keyspace, spec.table)),
        );
    }
    let fabricating = fabricated;
    assert!(
        fabricating.is_empty(),
        "AC2: one corrupted header byte must never make a surface emit a partition the pristine \
         Cassandra fixture does not contain (the pre-fix one-byte resync landed on misaligned \
         bytes that parsed as a plausible header). Surfaces that FABRICATED:\n  {}",
        fabricating.join("\n  ")
    );
}

/// AC1 on the user-facing `SELECT` surface, materializing and streaming.
///
/// SCOPE, stated because a green run here must not be read as this issue's pin:
/// measured pre-fix, `SELECT` ALREADY refused on this fixture — the header arm
/// resynchronised and #3782's ROW arm then refused the garbage it landed on. So
/// this is a REGRESSION guard for the surface a user actually calls; the
/// evidence for #3928 is the resync-absence assertion in
/// `every_proven_complete_surface_refuses_a_malformed_partition_header` and the
/// two BTI-covering cases, all three of which FAIL pre-fix.
#[tokio::test]
async fn select_refuses_a_malformed_partition_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "sel-big");
    let sql = format!("SELECT * FROM {}.{}", spec.keyspace, spec.table);

    let control = open_db(spec, staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine fixture must SELECT cleanly")
        .rows
        .len();
    assert!(
        control > 0,
        "0-rows-when-present: the control SELECT must return rows"
    );

    let db = open_db(spec, staged.mutated_root.clone()).await;
    match db.execute(&sql).await {
        Err(e) => assert_corruption_kind(&e, "Database::execute"),
        Ok(r) => panic!(
            "a malformed partition header must REFUSE, not resync: `SELECT` answered Ok with {} \
             rows against a control of {control} — and a count at or ABOVE the control is the \
             fabrication shape, not evidence of health",
            r.rows.len()
        ),
    }

    let cfg = cqlite_core::query::result::StreamingConfig {
        buffer_size: 8,
        ..Default::default()
    };
    let mut saw_error = false;
    let mut ok_rows = 0usize;
    match db.execute_streaming(&sql, cfg).await {
        Err(_) => saw_error = true,
        Ok(mut it) => {
            while let Some(item) = it.next_async().await {
                match item {
                    Ok(_) => ok_rows += 1,
                    Err(_) => saw_error = true,
                }
            }
        }
    }
    assert!(
        saw_error,
        "the streaming SELECT must surface the header decode error; it silently yielded \
         {ok_rows} rows against a control of {control}"
    );
}

/// AC1 on BTI (`da`) — a DIFFERENT corruption class through a DIFFERENT route.
///
/// `da` carries `hasUIntDeletionTime`, so its partition-level `DeletionTime` HAS
/// an invalid encoding (`DeletionTime.java:222-230` throws on it) and the header
/// reaches CQLite's `parse_partition_header_full` **error** arm rather than the
/// key-length **validation** arm the BIG lane exercises. The route differs too:
/// a `da` full scan stitches the whole data section in
/// `bti_scan_with_metadata_cancellable`, where BIG goes through
/// `sequential_scan`.
#[tokio::test]
async fn bti_scan_refuses_a_partition_header_cassandra_itself_rejects() {
    let spec = &BTI_MULTICLUSTERING_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "hdr-bti");
    let schema = table_schema(spec);

    let control_partitions = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control_partitions, spec);

    let mutated = observe(&staged.mutated_dir, &schema).await;
    let tolerated = tolerating(&control_observed, &mutated);
    assert!(
        tolerated.is_empty(),
        "AC1 (BTI): the partition-level DeletionTime discriminator at decompressed offset {} is \
         0xFF, which Cassandra's own DeletionTime.Serializer.deserialize throws on, so every \
         surface must REFUSE. {} of {} surfaces still answered Ok:\n  {}",
        staged.mutated_offset,
        tolerated.len(),
        mutated.len(),
        tolerated.join("\n  ")
    );

    let sql = format!("SELECT * FROM {}.{}", spec.keyspace, spec.table);
    let control_rows = open_db(spec, staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine BTI fixture must SELECT cleanly")
        .rows
        .len();
    assert!(
        control_rows > 0,
        "0-rows-when-present: the BTI control SELECT must return rows"
    );
    match open_db(spec, staged.mutated_root.clone())
        .await
        .execute(&sql)
        .await
    {
        Err(e) => assert_corruption_kind(&e, "BTI Database::execute"),
        Ok(r) => panic!(
            "the BTI SELECT must REFUSE a header Cassandra rejects: got Ok with {} rows against \
             a control of {control_rows}",
            r.rows.len()
        ),
    }
}

/// AC3 — the NEGATIVE CONTROL for the two SLIDING-DRIVER arms this change
/// touches, over the whole discovered corpus.
///
/// `issue_3782_corrupt_row_refusal.rs`'s corpus case calls
/// `iterate_all_partitions` and `get_all_entries` and explicitly does NOT call
/// either `*_for_compaction` surface — so the header arms in
/// `drive_partition_sliding` and `stream_partition_body_incremental`, whose
/// discriminator is `at_final_chunk`, had no well-formed-corpus control at all.
/// This case adds it: on a well-formed table those arms must never fire at the
/// final chunk, so nothing may start refusing.
///
/// The subject set is the UNION of every candidate root, deduplicated by table
/// identity, with each table's bytes read from the root that actually carries
/// them (#3220/#3104), and the committed subjects in [`MUST_RUN`] are asserted
/// PER CASE so this cannot pass by omission.
///
/// # AC3, measured rather than argued (2026-09-03, `CQLITE_DATASETS_ROOT=/data/datasets`)
///
/// Counters were planted at every tolerance site — the three ROW arms and the
/// six HEADER arms — and the whole corpus was walked on `iterate_all_partitions`,
/// `get_all_entries` and both `*_for_compaction` surfaces, on the PRE-fix tree
/// and on the POST-fix tree. **The two runs are identical:** 126 tables / 148
/// SSTables / 71313 emitted elements, **542** mid-stream row tolerations (all on
/// the streaming compaction driver, all at `at_final_chunk == false`), **0**
/// firings at ANY header arm in either direction, and **0** errors.
///
/// RE-TAKEN after fix round 1 added two more refusing arms (the drivers'
/// truncated-`Incomplete` refusal and the block walk's sub-two-byte tail): the
/// same figures again, to the unit — 542, and 0 at all six header sites.
///
/// RE-TAKEN AGAIN after fix round 2 replaced the predicate with
/// `HeaderTolerance` and made every non-empty incomplete header fatal at the
/// final chunk (C1). Same subject set, same 542, still ZERO at every REFUSING
/// header site and zero errors — plus one figure the earlier runs did not
/// separate out: the drivers' `Incomplete` arm fires **3** times MID-STREAM
/// (`NeedMore`) on the well-formed corpus.
///
/// Those 3 are the legitimate straddling header — #1741's whole subject, a
/// header split across a chunk boundary — and they are the measured reason C1's
/// refusal is gated on `at_final_chunk` rather than applied to every incomplete
/// header: three real corpus tables would red if it were not. The final-chunk
/// side of that same arm fires **0** times, which is why making it fatal costs
/// nothing.
///
/// RE-TAKEN A THIRD TIME after round 3 (B1's termination at the row-body bound
/// and B2's call into the shared oa/da sizing rule): identical again — 542, the
/// same 3 mid-stream straddles, and **0** at every refusing header site with 0
/// errors. Two things that measurement says, beyond "nothing moved":
///
/// * B1's termination costs no well-formed read. None of these four surfaces
///   passes a `row_body_window` — the bounded path is the point/promoted
///   readers' — so this corpus scan does not exercise it, and
///   `a_bounded_walk_stops_at_its_bound_and_reads_no_further_partition` in the
///   truncation lane is what covers it.
/// * the block-emit `Incomplete` arm fires **0** times here, which is exactly
///   why B2's straddling deleted header needs a hand-built buffer:
///   `regression_1741k_tests.rs` records that a real fixture cannot place a
///   deleted partition's 12-byte `DeletionTime` astride a chunk boundary, since
///   Cassandra purges tombstone-covered rows at flush and chunk boundaries are
///   not byte-addressable. A corpus scan can never reach that shape.
///
/// RE-TAKEN A FOURTH TIME after the round-4 fix made a `Ready`-then-unparseable
/// header refuse at every extent: identical again — 542, the same 3 mid-stream
/// straddles, and **0** at every refusing header site including the newly
/// unconditional one, with 0 errors.
///
/// That last zero is the point, and it is why "no measured loss" was NOT a
/// reason to keep tolerating there: this arm fires **0** times on 126 well-formed
/// tables, so the corpus cannot evidence it in EITHER direction. Measured where
/// it CAN be reached — the real `da` fixture with its discriminator flipped,
/// parsed under a `Window` — the pre-fix answer was `Ok` with 401 of 468 rows,
/// 180 LOST and 113 FABRICATED. An arm the corpus cannot reach needs a
/// constructed oracle, never an argument from silence.
///
/// So the header-arm refusal costs nothing on well-formed input, and the row
/// arm's toleration count did not move — which is AC3's property. The
/// instrumentation was temporary and is not committed; re-take the measurement
/// the same way if the number is ever in doubt.
///
/// #3782 records this figure as **614** over "42 well-formed corpus tables
/// (10913 rows)". That is a DIFFERENT measurement — a different surface set on a
/// smaller subject set — not a regression: 542 is what these four surfaces fire
/// on this 126-table corpus, and it is the SAME 542 before and after, which is
/// the only comparison that can answer "did this change move it".
#[tokio::test]
async fn corpus_wide_well_formed_tables_still_compact_without_refusal() {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut scanned_tables: BTreeSet<(String, String)> = BTreeSet::new();
    let mut scanned_sstables = 0usize;
    let mut with_rows = 0usize;

    for (keyspace, table) in corpus_table_identities() {
        for data in data_files_for_table(&keyspace, &table) {
            let Ok(reader) = SSTableReader::open(&data, &config, platform.clone()).await else {
                // Unopenable (an out-of-scope format, a sidecar-only fixture):
                // not a subject of this lane. A MUST_RUN table that never opens
                // is caught by the per-case assertion below.
                continue;
            };
            scanned_sstables += 1;
            scanned_tables.insert((keyspace.clone(), table.clone()));
            // No schema is threaded in: these surfaces take `Option<&TableSchema>`
            // and the corpus spans 100+ tables. `None` still drives the partition
            // HEADER walk — the subject of this issue — through both drivers.
            let keys = reader.distinct_partition_keys().await.unwrap_or_else(|e| {
                panic!("#3928 regression: well-formed {data:?} now REFUSES the stitched partition-key walk: {e}")
            });
            let buffered = reader
                .iterate_all_partitions_for_compaction(None)
                .await
                .unwrap_or_else(|e| {
                    panic!("#3928 regression: well-formed {data:?} now REFUSES buffered compaction: {e}")
                });
            let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
            let mut streamed = 0usize;
            reader
                .stream_all_partitions_for_compaction(None, &cancel, |_row| {
                    streamed += 1;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .await
                .unwrap_or_else(|e| {
                    panic!("#3928 regression: well-formed {data:?} now REFUSES streaming compaction: {e}")
                });
            if !keys.is_empty() || !buffered.is_empty() || streamed > 0 {
                with_rows += 1;
            }
        }
    }

    for (keyspace, table) in MUST_RUN {
        assert!(
            scanned_tables.contains(&((*keyspace).to_string(), (*table).to_string())),
            "must_run corpus subject {keyspace}.{table} was never scanned; {}",
            datasets_root::describe_search(keyspace, table)
        );
    }
    assert!(
        scanned_tables.len() >= 20,
        "case floor: expected a real corpus, scanned only {} tables ({} SSTables) ({})",
        scanned_tables.len(),
        scanned_sstables,
        datasets_root::describe_roots()
    );
    assert!(
        with_rows > 0,
        "0-rows-when-present: {} tables / {scanned_sstables} SSTables scanned and none yielded a \
         partition",
        scanned_tables.len()
    );
}

/// Fix round 4 — a `Ready` header that fails the full parse is refused at EVERY
/// extent, `BufferExtent::Window` included.
///
/// `partition_header_readiness` answering `Ready` is an AFFIRMATIVE guarantee
/// that every header byte is present — the key length, the key, and the
/// `DeletionTime` for its live/deleted form, sized by peeking the discriminator
/// (#1741). So a parse failure after `Ready` cannot be truncation, and **a
/// `Ready` header cannot straddle**. That is what decides this arm: the tolerant
/// break protects a straddling ROW, and AC1 licenses tolerance only where "a
/// header can legitimately straddle" — which this one provably cannot. No later
/// chunk can repair an invalid deletion-time discriminator, so resynchronising
/// past it dropped a real partition and could invent misaligned ones, while the
/// sliding drivers propagated the identical error regardless of extent.
///
/// Oracle: the real Cassandra `da` fixture with its FIRST partition's
/// `DeletionTime` discriminator flipped to `0xFF` — a byte Cassandra's own
/// `DeletionTime.Serializer.deserialize` throws on
/// (`DeletionTime.java:222-230`), and one that leaves the header COMPLETE, which
/// is exactly the `Ready`-then-`Err` shape. The `da` gate path is required: nb's
/// legacy 12-byte `DeletionTime` has no invalid encodings, so an nb-gated parse
/// could not produce this at all.
///
/// Both directions, because a fix here could equally break healthy windowed
/// reads: the PRISTINE section must still walk cleanly under the SAME `Window`
/// extent and emit the same partition keys it emits under `Complete`.
#[tokio::test]
async fn a_windowed_block_read_refuses_a_complete_but_structurally_invalid_header() {
    use cqlite_core::storage::sstable::reader::BufferExtent;

    let spec = &BTI_MULTICLUSTERING_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "ready-invalid");
    let schema = table_schema(spec);
    let reader = open_reader(&staged.mutated_dir).await;
    let pristine = fixture::stitched_data_section(&staged.control_dir);
    let mutated = fixture::stitched_data_section(&staged.mutated_dir);

    // PRECONDITION as a measured byte-level fact, not an assumption: the header
    // is COMPLETE (its discriminator is present, so nothing is straddling) and
    // that discriminator is the value Cassandra rejects. `partition_header_readiness`
    // is crate-internal, so its `Ready` verdict is stated in these bytes instead.
    let key_len = usize::from(u16::from_be_bytes([mutated[0], mutated[1]]));
    let discriminator_at = 2 + key_len;
    assert!(
        discriminator_at < mutated.len(),
        "the discriminator at {discriminator_at} must be PRESENT for this to be the \
         complete-but-invalid shape rather than a straddle"
    );
    assert_eq!(
        mutated[discriminator_at], 0xFF,
        "this case needs the harness's DeletionTimeDiscriminator mutation"
    );
    assert_eq!(
        pristine[discriminator_at], 0x80,
        "control: Cassandra wrote the LIVE sentinel there"
    );

    // CONTROL, and the both-directions guard: the PRISTINE section walks cleanly
    // under BOTH extents and agrees on the partition keys. A fix that made
    // healthy windowed reads fatal reds here.
    let ctl_window = da_block_walk(spec, &schema, &reader, &pristine, BufferExtent::Window)
        .expect("a PRISTINE section must walk cleanly under a Window extent");
    let ctl_complete = da_block_walk(spec, &schema, &reader, &pristine, BufferExtent::Complete)
        .expect("a PRISTINE section must walk cleanly under a Complete extent");
    assert!(
        !ctl_window.is_empty(),
        "0-rows-when-present: the pristine Window walk emitted nothing"
    );
    assert_eq!(
        multiset::multiset(ctl_window.iter().cloned()),
        multiset::multiset(ctl_complete.iter().cloned()),
        "the extent must not change what a HEALTHY section decodes"
    );

    // The subject: the same walk over the mutated section must REFUSE at BOTH
    // extents. Pre-fix the `Window` leg answered Ok, having resynchronised past
    // a header no later chunk could ever repair.
    for extent in [BufferExtent::Window, BufferExtent::Complete] {
        let got = da_block_walk(spec, &schema, &reader, &mutated, extent);
        match got {
            Err(e) => assert!(
                e.contains("Data corruption"),
                "{extent:?}: the refusal must carry the decode error's own kind; got {e}"
            ),
            Ok(keys) => {
                let control = multiset::multiset(ctl_complete.iter().cloned());
                let got_ms = multiset::multiset(keys.iter().cloned());
                panic!(
                    "{extent:?}: a COMPLETE but structurally invalid header must be REFUSED — \
                     `Ready` guarantees every header byte is present, so no later chunk can \
                     repair it and it cannot be a straddle. Got Ok with {} key occurrence(s) \
                     against a control of {}: {} LOST [{}], {} FABRICATED [{}]",
                    keys.len(),
                    ctl_complete.len(),
                    multiset::deficit(&got_ms, &control)
                        .iter()
                        .map(|(_, n)| n)
                        .sum::<usize>(),
                    multiset::describe(&multiset::deficit(&got_ms, &control)),
                    multiset::surplus(&got_ms, &control)
                        .iter()
                        .map(|(_, n)| n)
                        .sum::<usize>(),
                    multiset::describe(&multiset::surplus(&got_ms, &control)),
                );
            }
        }
    }
}
