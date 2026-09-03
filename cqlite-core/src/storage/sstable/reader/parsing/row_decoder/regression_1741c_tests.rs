//! Issue #1741 (roborev pass C): regression pins for two correctness holes found
//! in the initial single-gen shadowing change.
//!
//! Finding 1 (HIGH) — the clustering-slice fast-forward in
//! [`super::V5CompressedLegacyParser::parse_block_emit_windowed`] jumped straight
//! to `body_start`, skipping any range-tombstone marker that OPENS before the
//! slice. Such a marker can cover rows inside the slice, so a targeted slice read
//! could return rows a full scan correctly hides. The fix replays the pre-window
//! markers into `PartitionShadow` before fast-forwarding.
//!
//! Finding 2 (MEDIUM) — the two sliding parsers
//! (`parse_one_partition_with_timestamps`, `parse_one_partition_for_compaction`)
//! required the 12-byte `nb` deletion-time header minimum, so a small valid oa/da
//! (`hasUIntDeletionTime`, 1-byte LIVE) partition whose total bytes were below the
//! `nb` minimum was treated as truncated-at-EOF and DROPPED. The fix branches the
//! deletion-time minimum on `has_uint_deletion_time()`.

use super::V5CompressedLegacyParser as V5;
use crate::error::Result;

// ---------------------------------------------------------------------------
// Finding 1: range-tombstone shadowing must survive the clustering-slice
// fast-forward.
// ---------------------------------------------------------------------------

/// Locate the first `test_deltas/range_tombstones-*` fixture Data.db (nb-BIG).
fn range_tombstones_data_db() -> Option<std::path::PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let ks = std::path::PathBuf::from(root)
        .join("sstables")
        .join("test_deltas");
    if !ks.is_dir() {
        return None;
    }
    for entry in std::fs::read_dir(&ks).ok()?.flatten() {
        let p = entry.path();
        let is_rt = p.is_dir()
            && p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("range_tombstones-"))
                .unwrap_or(false);
        if is_rt {
            let data = p.join("nb-1-big-Data.db");
            if data.exists() {
                return Some(data);
            }
        }
    }
    None
}

/// Count rows a windowed parse emits over `block`.
fn count_emitted(
    parser: &V5,
    block: &[u8],
    schema: &crate::schema::TableSchema,
    reader: &crate::storage::sstable::reader::SSTableReader,
    window: Option<(usize, usize)>,
) -> Result<usize> {
    let mut n = 0usize;
    // #3782: `block` is the whole stitched data section (see the callers), so
    // the truthful extent is `Complete`; the `window` argument narrows the
    // row-index BODY, which is a different axis.
    parser.parse_block_emit_windowed(
        block,
        super::BufferExtent::Complete,
        Some(schema),
        reader,
        window,
        |_entry| {
            n += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    Ok(n)
}

/// A range tombstone that OPENS before a targeted clustering slice must hide the
/// older rows inside that slice on a WINDOWED (slice) read exactly as it does on a
/// full scan (issue #1741, Finding 1).
///
/// A real single-gen fixture cannot carry rows physically covered by a range
/// tombstone (Cassandra purges them at flush), so we reassemble one from real
/// range_tombstones bytes: keep the partition header + the first range-tombstone
/// START marker, DROP the matching END marker, and append the rows that followed
/// it. Dropping the END keeps the range open over those (older) rows, making them
/// covered — the exact "covered rows inside the slice" shape Finding 1 is about.
///
/// Revert-verify: with the `block_emit_windowed.rs` fast-forward priming removed,
/// the slice read returns the covered rows (`n_slice == n_physical > 0`) while the
/// full scan returns 0 — the two disagree and this test FAILS. With the fix both
/// return 0.
#[tokio::test]
async fn range_tombstone_before_slice_is_shadowed_on_windowed_read() {
    let Some(data_db) = range_tombstones_data_db() else {
        eprintln!("SKIP range_tombstone_before_slice_is_shadowed_on_windowed_read: fixture absent");
        return;
    };

    let config = crate::Config::default();
    let platform = match crate::platform::Platform::new(&config).await {
        Ok(p) => std::sync::Arc::new(p),
        Err(e) => {
            eprintln!("SKIP: platform init failed: {e}");
            return;
        }
    };
    let reader =
        match crate::storage::sstable::reader::SSTableReader::open(&data_db, &config, platform)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                eprintln!("SKIP: reader open failed: {e}");
                return;
            }
        };

    let Some(schema) = reader.get_table_schema(None) else {
        eprintln!("SKIP: no schema derivable from range_tombstones header");
        return;
    };

    // Whole (decompressed) data section — range_tombstones is a handful of tiny
    // partitions, so this is a few hundred bytes.
    let cursor = reader.new_scan_cursor().await.expect("scan cursor");
    let buf = reader.stitch_all_chunks(&cursor).await.expect("stitch");
    assert!(!buf.is_empty(), "fixture Data.db must decompress to bytes");

    // Physical parser (no shadowing) is used both to WALK the real bytes and to
    // prove the reassembled covered rows are physically present.
    let p_phys = reader.build_v5_parser(false);
    let p_shadow = reader.build_v5_parser(true);
    let resolution = super::RowColumnResolution::build(&schema, &reader);

    // Walk partition-by-partition; select the first partition whose reassembly
    // yields a genuinely COVERING range (full-scan shadow hides every collected
    // row) so the differential is meaningful regardless of which fixture UUID/dir
    // resolved.
    let mut poff = 0usize;
    let mut asserted = false;
    while poff < buf.len() {
        let (_pk, mut off, _del) = match p_phys.parse_partition_header_full(&buf, poff) {
            Ok(v) => v,
            Err(_) => break,
        };
        let header = buf[poff..off].to_vec();
        let mut rt_start: Vec<u8> = Vec::new();
        let mut covered: Vec<u8> = Vec::new();
        let mut seen_rt = false;

        loop {
            if off >= buf.len() {
                break;
            }
            let flags = buf[off];
            if V5::is_end_of_partition(flags) {
                off += 1;
                break;
            }
            if off != poff && p_phys.peek_is_partition_header(&buf, off) {
                break; // next partition starts here
            }
            if V5::is_range_tombstone_marker(flags) {
                let next = match p_phys.skip_range_tombstone_marker(&buf, off, &schema) {
                    Ok(n) => n,
                    Err(_) => break,
                };
                // Keep the FIRST marker (a range START) as the pre-window opener;
                // drop every later marker so the range stays open over the rows.
                if !seen_rt {
                    rt_start = buf[off..next].to_vec();
                    seen_rt = true;
                }
                off = next;
                continue;
            }
            let next = match p_phys.parse_row_data_with_offset(
                &buf,
                off,
                Some(&schema),
                &reader,
                false,
                &resolution,
                None,
            ) {
                Ok((.., n, _is_static, _complex)) => n,
                Err(_) => break,
            };
            if seen_rt {
                covered.extend_from_slice(&buf[off..next]);
            }
            off = next;
        }

        let next_partition = off;

        if !rt_start.is_empty() && !covered.is_empty() {
            // Reassemble: header + range-START + covered rows + END_OF_PARTITION.
            let mut synth = header.clone();
            synth.extend_from_slice(&rt_start);
            let body_start = synth.len();
            synth.extend_from_slice(&covered);
            let body_end = synth.len();
            synth.push(0x01); // END_OF_PARTITION

            // Rows are physically present (no shadow → emitted).
            let n_physical = count_emitted(&p_phys, &synth, &schema, &reader, None).unwrap();
            // Full scan WITH shadowing hides them (open range covers older rows).
            let n_full = count_emitted(&p_shadow, &synth, &schema, &reader, None).unwrap();

            if n_physical == 0 || n_full != 0 {
                // Not a covering range for this partition — try the next one.
                if next_partition <= poff {
                    break;
                }
                poff = next_partition;
                continue;
            }

            // Slice (windowed) read must hide EXACTLY what the full scan hides.
            // Pre-fix this returns `n_physical` (> 0) because the fast-forward
            // skipped the pre-window range-START marker.
            let n_slice = count_emitted(
                &p_shadow,
                &synth,
                &schema,
                &reader,
                Some((body_start, body_end)),
            )
            .unwrap();

            assert_eq!(
                n_slice, 0,
                "Finding 1: a range tombstone opening before the slice must hide the covered \
                 rows on a windowed slice read (physical rows = {n_physical}), matching the full \
                 scan (= {n_full})"
            );
            assert_eq!(n_slice, n_full, "slice read must match the full scan");
            asserted = true;
            break;
        }

        if next_partition <= poff {
            break;
        }
        poff = next_partition;
    }

    assert!(
        asserted,
        "range_tombstones fixture present but no covering-range partition was found to exercise \
         Finding 1 — fixture structure changed (not an empty pass)"
    );
}

// ---------------------------------------------------------------------------
// Finding 2: sliding parsers must accept a small oa/da live partition.
// ---------------------------------------------------------------------------

/// pk-only schema (`tiny(pk int, v text)`) — no clustering — so a single-row
/// partition is as small as possible.
#[cfg(feature = "write-support")]
fn tiny_schema() -> crate::schema::TableSchema {
    use crate::schema::{Column, KeyColumn};
    crate::schema::TableSchema {
        keyspace: "test_ks".to_string(),
        table: "tiny".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// A small BTI (`da`) LIVE partition — whose total bytes fall below the legacy
/// `nb` 12-byte deletion-time header minimum — must be read back by BOTH sliding
/// parsers (streaming scan + compaction), not dropped as truncated-at-EOF
/// (issue #1741, Finding 2).
///
/// Revert-verify: with the sliding parsers' `header_min_size` pinned to the `nb`
/// `1 + 1 + key_len + 12`, both parsers return `ParseStep::Done` and emit ZERO
/// rows for this partition, so the row-count asserts FAIL. With the fix (branch on
/// `has_uint_deletion_time()` → 1 byte for oa/da) they emit the row.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn small_oa_live_partition_survives_sliding_parsers() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;

    let schema = tiny_schema();
    let dir = tempfile::TempDir::new().unwrap();

    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    // One tiny live row: pk=42, v="x". ts chosen equal to the SSTable min so the
    // row's timestamp delta is 0 (1 byte), keeping the partition minimal.
    let mutation = Mutation::new(
        TableId::new("test_ks", "tiny"),
        PartitionKey::single("pk", Value::Integer(42)),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("x".to_string()),
        }],
        1_000_000,
        None,
    );
    let key = mutation.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![mutation]).unwrap();
    let info = writer.finish().await.unwrap();

    // Open the produced da SSTable and read its decompressed data section.
    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();

    let cursor = reader.new_scan_cursor().await.unwrap();
    let buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    assert!(
        buf.len() >= 2,
        "partition must have at least flags + key_len"
    );

    // The reader must be on the oa/da (hasUIntDeletionTime) path.
    let parser = reader.build_v5_parser(false);

    // Precondition that makes this a genuine Finding-2 differential: the whole
    // (single) partition is smaller than the legacy nb 12-byte-deletion minimum,
    // so the pre-fix sliding parsers would DROP it at the header_min_size gate.
    let key_len = buf[1] as usize;
    let nb_min = 1 + 1 + key_len + 12;
    let oa_min = 1 + 1 + key_len + 1;
    assert!(
        buf.len() >= oa_min,
        "sanity: partition ({} bytes) is below even the oa minimum ({oa_min})",
        buf.len()
    );
    assert!(
        buf.len() < nb_min,
        "Finding 2 precondition: the tiny da partition ({} bytes, key_len={key_len}) must be \
         smaller than the nb 12-byte-deletion minimum ({nb_min}) so the pre-fix sliding parsers \
         drop it; shrink the fixture if this trips",
        buf.len()
    );

    // Streaming-scan sliding parser: must EMIT the row, not return Done.
    let mut ts_rows = 0usize;
    let step = parser
        .parse_one_partition_with_timestamps(&buf, Some(&schema), &reader, true, &mut |_e| {
            ts_rows += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(step, super::ParseStep::Emitted(_)),
        "Finding 2: streaming-scan sliding parser must EMIT the small oa partition, got {step:?}"
    );
    assert!(
        ts_rows >= 1,
        "the small oa live partition's row must be read back by the streaming scan sliding parser"
    );

    // Compaction sliding parser: must also EMIT the row.
    let mut comp_rows = 0usize;
    let cstep = parser
        .parse_one_partition_for_compaction(&buf, Some(&schema), &reader, true, &mut |_r| {
            comp_rows += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(cstep, super::ParseStep::Emitted(_)),
        "Finding 2: compaction sliding parser must EMIT the small oa partition, got {cstep:?}"
    );
    assert!(
        comp_rows >= 1,
        "the small oa live partition's row must survive a compaction read pass"
    );
}
