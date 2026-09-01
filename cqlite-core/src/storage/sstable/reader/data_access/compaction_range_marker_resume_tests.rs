//! Issue #2299 (roborev should-fix): range-marker resume parity.
//!
//! The trickiest genuinely-new logic the streaming compaction path adds is
//! [`CompactionPartitionState::pending_range_start`] — the cross-chunk carry that
//! lets a range tombstone whose CLOSE bound arrives in a LATER window refill still
//! pair with the START bound seen in an earlier one. The two default-gate e2e
//! tests (`test_issue_2299_stream_readback` + the dhat memory test) are
//! deliberately tombstone-FREE (to keep the write-side `stream_rows_directly` gate
//! true), so before this test the range-marker resume path through
//! [`V5CompressedLegacyParser::stream_partition_body_incremental`] was asserted
//! only by comment, never executed.
//!
//! ## What this proves
//!
//! Over a REAL, `SSTableWriter`-produced SSTable holding a partition with a bounded
//! range tombstone, the row-granular streaming drain (driven here with a window
//! refilled ONE BYTE AT A TIME, so EVERY multi-byte structure — including the
//! range-tombstone bound markers — is assembled across many `NeedMore` refills and
//! the range's START/END bounds land in SEPARATE refill chunks) emits a
//! `CompactionRow` sequence BYTE-IDENTICAL to the buffered
//! [`V5CompressedLegacyParser::parse_block_for_compaction`] output on the SAME
//! stitched bytes. Byte-by-byte refill is the strongest possible chunk-straddling
//! stress: it guarantees the marker resume path (and every row's mid-structure
//! `NeedMore`) is exercised, not merely reachable.
//!
//! This is a READER-path parity test: it constructs the input SSTable directly via
//! the writer (a range tombstone in a flushed SSTable — the SAME shape
//! `issue_933_range_tombstone_compaction` relies on) and never touches the
//! write-side direct-stream gate.

use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::storage::sstable::reader::parsing::{
    CompactionPartitionState, PartitionStreamStep, RowColumnResolution,
};
use crate::storage::sstable::reader::window_cursor::WindowCursor;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::Value;
use crate::{Config, Platform};
use std::io::SeekFrom;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncSeekExt;

const KS: &str = "issue2299_rt_resume_ks";
const TBL: &str = "rt_items";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn write_row(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// A range-tombstone mutation covering `[start, end]` on partition `id`.
fn range_delete(id: i32, start: ClusteringBound, end: ClusteringBound, ts: i64) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![],
        ts,
        None,
    );
    m.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: ts,
        // Far-future LDT so gc-grace never purges the marker in this test.
        local_deletion_time: 2_000_000_000,
    });
    m
}

fn incl(ck: i32) -> ClusteringBound {
    ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

/// Flush one uncompressed 'nb' SSTable holding TWO partitions, EACH carrying live
/// rows plus a bounded range tombstone, and return (kept-alive tempdir, Data.db).
/// Two partitions force a real partition boundary (PartitionDone/reset) between the
/// range-marker carries, so the per-partition `pending_range_start` reset is also
/// exercised.
async fn write_fixture() -> (TempDir, std::path::PathBuf) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("inputs");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // Two partitions, each: live rows ck 0..=6 plus a range tombstone [2, 4].
    for id in [1, 2] {
        for ck in 0..=6 {
            engine
                .write(write_row(id, ck, &format!("p{id}-v{ck}"), 100))
                .expect("write row");
        }
        engine
            .write(range_delete(id, incl(2), incl(4), 200))
            .expect("write range tombstone");
    }
    let info = engine
        .flush()
        .await
        .expect("flush")
        .expect("non-empty sstable");
    let data_path = info.data_path.clone();
    (temp, data_path)
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// The whole decompressed data section (header stripped) — the SAME bytes both the
/// buffered and the streaming compaction decoders consume.
async fn stitch_data_section(reader: &SSTableReader) -> Vec<u8> {
    let cursor = reader.new_scan_cursor().await.unwrap();
    let header_size = reader.calculate_header_size();
    {
        let mut file_guard = cursor.file.lock().await;
        file_guard
            .seek(SeekFrom::Start(header_size as u64))
            .await
            .unwrap();
    }
    let whole = reader.stitch_all_chunks(&cursor).await.unwrap();
    assert!(!whole.is_empty(), "stitched data section must be non-empty");
    whole
}

/// Drive the row-granular streaming compaction decode over `whole` with the window
/// refilled ONE BYTE AT A TIME — the strongest chunk-straddling stress, so every
/// multi-byte structure (range-tombstone bound markers included) is assembled
/// across many `NeedMore` refills and each range's START/END bounds land in
/// SEPARATE refill chunks. Returns the emitted rows in order.
fn stream_byte_by_byte(
    reader: &SSTableReader,
    parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    schema: &TableSchema,
    resolution: &RowColumnResolution,
    whole: &[u8],
) -> Vec<crate::storage::sstable::reader::compaction_row::CompactionRow> {
    let mut window = WindowCursor::new();
    let mut state = CompactionPartitionState::new();
    let mut out = Vec::new();
    let mut fed = 0usize;

    // Feed one byte, then drain every structure the window can now confirm. Repeat
    // until the whole buffer is fed, then run a final at_final_chunk drain.
    loop {
        let at_final = fed >= whole.len();
        // Refill with exactly one more byte (if any remain) before draining.
        if !at_final {
            window.refill(&whole[fed..fed + 1]);
            fed += 1;
        }
        loop {
            if window.is_empty() {
                break;
            }
            let step = parser
                .stream_partition_body_incremental(
                    window.as_slice(),
                    Some(schema),
                    reader,
                    Some(resolution),
                    at_final,
                    &mut state,
                    &mut |row| {
                        out.push(row);
                        Ok(std::ops::ControlFlow::Continue(()))
                    },
                )
                .expect("streaming decode must not error");
            match step {
                PartitionStreamStep::Consumed(n) | PartitionStreamStep::PartitionDone(n) => {
                    if n == 0 {
                        // No progress possible on this buffer (terminal 0-byte
                        // done): stop draining and await more bytes / finish.
                        break;
                    }
                    window.consume(n);
                }
                PartitionStreamStep::Break(n) => {
                    window.consume(n);
                    return out;
                }
                PartitionStreamStep::NeedMore | PartitionStreamStep::AllDone => break,
            }
        }
        if at_final {
            return out;
        }
    }
}

/// Byte-identical parity: the row-granular streaming drain over a byte-by-byte
/// refilled window (range-tombstone START/END bounds forced into separate refill
/// chunks) emits exactly the buffered `parse_block_for_compaction` sequence.
#[tokio::test(flavor = "multi_thread")]
async fn range_marker_resumes_across_window_refill_byte_identical() {
    let schema = schema();
    let (_temp, data_path) = write_fixture().await;
    let reader = open_reader(&data_path).await;

    // `nb` uncompressed → the compaction stream stitches the data section.
    assert!(
        reader.requires_chunk_stitching(),
        "fixture must take the chunk-stitching compaction path"
    );

    // Stitch the whole decompressed data section (header stripped) — the SAME
    // bytes both the buffered and streaming decoders consume.
    let whole = stitch_data_section(&reader).await;

    let parser = reader.build_v5_parser(false);
    let resolution = RowColumnResolution::build(&schema, &reader);

    // Reference: the buffered whole-partition decode.
    let buffered = parser
        .parse_block_for_compaction(&whole, Some(&schema), &reader)
        .expect("buffered compaction decode");

    // Non-vacuity: the fixture really produced a RangeMarker per partition, so the
    // resume path under test is actually exercised (not a rows-only false pass).
    let marker_count =
        buffered
            .iter()
            .filter(|r| {
                matches!(
                r.row_data,
                crate::storage::sstable::reader::compaction_row::CompactionRowData::RangeMarker {
                    ..
                }
            )
            })
            .count();
    assert!(
        marker_count >= 2,
        "fixture must emit >= 2 range markers (one per partition), got {marker_count}"
    );

    // Streaming: byte-by-byte refill forces cross-chunk resume of every structure.
    let streamed = stream_byte_by_byte(&reader, &parser, &schema, &resolution, &whole);

    assert_eq!(
        streamed, buffered,
        "the row-granular streaming drain (range-marker START/END bounds split \
         across window refills) must emit a CompactionRow sequence byte-identical \
         to the buffered parse_block_for_compaction output on the same bytes"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #3721 (roborev job 16) — a marker PARSE failure is a chunk boundary on a
// NON-final chunk and CORRUPTION on the final one.
//
// `CompactionPolicy::on_range_marker` used to answer every marker parse failure
// with `MarkerOutcome::Stop`, which both compaction drivers convert on the final
// chunk into a SUCCESSFUL partition completion — the marker silently dropped from
// output that is WRITTEN, resurrecting the rows it shadowed.
//
// Both tests below assert the two halves over the SAME prefixes, so each is the
// other's control:
//
// * the FINAL-chunk half proves the prefix really does reach the marker-parse
//   failure (the refusal text names it), which is this pair's non-vacuity proof —
//   without it a sweep that never truncated a marker would pass trivially;
// * the NON-final half proves that path still asks for a refill. A fix that
//   refused every unparseable marker regardless of chunk state would break
//   chunked compaction outright: every marker straddling a window boundary is
//   unparseable at the moment it is first seen.
//
// The prefixes are the fixture's own stitched bytes truncated at every length, so
// nothing is synthesised and no offset is hard-coded. Prefixes that fail EARLIER
// than the marker (a truncated row whose column decode fails — the sibling #3721
// fix) are skipped by the same text test rather than asserted about here.
// ─────────────────────────────────────────────────────────────────────────────

/// Does `rendered` name the range-tombstone marker refusal this section is about
/// (as opposed to a truncated ROW's column-decode failure, which the same sweep
/// also produces and which belongs to a different lane)?
fn is_marker_parse_refusal(rendered: &str) -> bool {
    rendered.contains("range-tombstone marker")
        && rendered.contains("could not be PARSED")
        && rendered.contains("FINAL chunk")
}

/// Buffered driver (`parse_one_partition_for_compaction` ->
/// `drive_partition_sliding`).
#[tokio::test(flavor = "multi_thread")]
async fn buffered_marker_parse_failure_refills_mid_stream_and_refuses_at_the_final_chunk() {
    let schema = schema();
    let (_temp, data_path) = write_fixture().await;
    let reader = open_reader(&data_path).await;
    let whole = stitch_data_section(&reader).await;
    let parser = reader.build_v5_parser(false);

    let drive = |prefix: &[u8], at_final: bool| {
        parser.parse_one_partition_for_compaction(
            prefix,
            Some(&schema),
            &reader,
            at_final,
            &mut |_row| Ok(std::ops::ControlFlow::Continue(())),
        )
    };

    let mut refused = 0usize;
    for len in 1..whole.len() {
        let prefix = &whole[..len];
        let Err(err) = drive(prefix, true) else {
            continue;
        };
        let rendered = err.to_string();
        if !is_marker_parse_refusal(&rendered) {
            continue;
        }
        refused += 1;

        match drive(prefix, false) {
            Ok(step) => assert_eq!(
                step,
                crate::storage::sstable::reader::parsing::ParseStep::NeedMore,
                "the SAME {len}-byte prefix on a NON-final chunk must ask for a refill: the \
                 marker body may simply straddle the window boundary"
            ),
            Err(e) => panic!(
                "a marker truncated at {len} bytes must be NeedMore on a non-final chunk, not \
                 an error — refusing it would break every chunked compaction whose marker \
                 straddles a window boundary; got: {e}"
            ),
        }
    }

    assert!(
        refused > 0,
        "non-vacuity: at least one prefix of the {}-byte fixture must truncate a range-tombstone \
         marker and be REFUSED at the final chunk — otherwise the non-final half above asserted \
         nothing",
        whole.len()
    );
}

/// Row-granular streaming driver (`stream_partition_body_incremental`), which owns
/// its own copy of the same decision. Returns the terminal step, or the error.
fn drive_incremental(
    reader: &SSTableReader,
    parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    schema: &TableSchema,
    resolution: &RowColumnResolution,
    bytes: &[u8],
    at_final: bool,
) -> crate::Result<PartitionStreamStep> {
    let mut window = WindowCursor::new();
    window.refill(bytes);
    let mut state = CompactionPartitionState::new();
    loop {
        if window.is_empty() {
            return Ok(PartitionStreamStep::AllDone);
        }
        let step = parser.stream_partition_body_incremental(
            window.as_slice(),
            Some(schema),
            reader,
            Some(resolution),
            at_final,
            &mut state,
            &mut |_row| Ok(std::ops::ControlFlow::Continue(())),
        )?;
        match step {
            PartitionStreamStep::Consumed(n) | PartitionStreamStep::PartitionDone(n) => {
                if n == 0 {
                    return Ok(step);
                }
                window.consume(n);
            }
            PartitionStreamStep::Break(_)
            | PartitionStreamStep::NeedMore
            | PartitionStreamStep::AllDone => return Ok(step),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_marker_parse_failure_refills_mid_stream_and_refuses_at_the_final_chunk() {
    let schema = schema();
    let (_temp, data_path) = write_fixture().await;
    let reader = open_reader(&data_path).await;
    let whole = stitch_data_section(&reader).await;
    let parser = reader.build_v5_parser(false);
    let resolution = RowColumnResolution::build(&schema, &reader);

    let mut refused = 0usize;
    for len in 1..whole.len() {
        let prefix = &whole[..len];
        let Err(err) = drive_incremental(&reader, &parser, &schema, &resolution, prefix, true)
        else {
            continue;
        };
        let rendered = err.to_string();
        if !is_marker_parse_refusal(&rendered) {
            continue;
        }
        refused += 1;

        match drive_incremental(&reader, &parser, &schema, &resolution, prefix, false) {
            Ok(step) => assert_eq!(
                step,
                PartitionStreamStep::NeedMore,
                "the SAME {len}-byte prefix on a NON-final chunk must ask for a refill"
            ),
            Err(e) => panic!(
                "a marker truncated at {len} bytes must be NeedMore on a non-final chunk, not \
                 an error; got: {e}"
            ),
        }
    }

    assert!(
        refused > 0,
        "non-vacuity: at least one prefix must truncate a range-tombstone marker and be REFUSED \
         at the final chunk"
    );
}
