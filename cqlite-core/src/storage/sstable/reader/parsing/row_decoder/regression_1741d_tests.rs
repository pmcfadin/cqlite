//! Issue #1741 (roborev pass D): regression pins for three correctness/perf holes.
//!
//! Finding 1 (CORRECTNESS) — a STATIC row was stored and later merged into every
//! surviving clustering row WITHOUT applying partition-tombstone / TTL shadowing to
//! the static row itself. A partition tombstone (or static-row TTL) that shadows the
//! static data while a newer clustering row survives therefore leaked the stale
//! static value into `SELECT` output. The fix evaluates the static row header with
//! `PartitionShadow` before saving `static_cells`.
//!
//! Finding 2 (PERF) — the slice-read fast-forward primed the pre-window prefix on
//! EVERY user-facing windowed read, regressing O(slice) to O(prefix+slice). Priming
//! is now (a) gated on authoritative EncodingStats evidence that the SSTable carries
//! deletions (hence possibly range tombstones), and (b) marker-only (no cell decode).
//! `sstable_may_have_range_tombstones()` is pinned here; the round-C
//! `range_tombstone_before_slice_is_shadowed_on_windowed_read` still pins that RT
//! shadowing survives the (now cheaper) priming.
//!
//! Finding 3's collection-element TTL aggregate is pinned in `complex_column.rs`.

use super::V5CompressedLegacyParser as V5;

// ---------------------------------------------------------------------------
// Finding 2: the no-range-tombstone fast path is preserved authoritatively.
// ---------------------------------------------------------------------------

/// The priming gate is driven ONLY by authoritative `EncodingStats`
/// (`minLocalDeletionTime`): the LIVE sentinel `Integer.MAX_VALUE` proves the
/// SSTable has NO deletions (hence no range tombstones), so the slice read keeps
/// the O(slice) fast-forward and skips prefix priming. Any smaller min means a
/// tombstone/TTL exists, so priming runs (correctly, over-approximating RTs).
#[test]
fn range_tombstone_gate_follows_min_local_deletion_time() {
    // No deletions: minLocalDeletionTime == Integer.MAX_VALUE LIVE sentinel.
    let live = V5::new("k".into(), "t".into(), 0, i32::MAX as i64, None);
    assert!(
        !live.sstable_may_have_range_tombstones(),
        "an SSTable whose EncodingStats minLocalDeletionTime is the LIVE sentinel \
         (Integer.MAX_VALUE) has no deletions — the slice read must keep the O(slice) \
         fast path and NOT prime the prefix"
    );

    // A real (smaller) minLocalDeletionTime means at least one deletion/TTL — prime.
    let has_del = V5::new("k".into(), "t".into(), 0, 1_700_000_000, None);
    assert!(
        has_del.sstable_may_have_range_tombstones(),
        "a real (< Integer.MAX_VALUE) minLocalDeletionTime means the SSTable carries a \
         deletion/TTL, so priming must run to catch a range tombstone before the slice"
    );

    // The no-stats fallback (min == 0) conservatively primes (never skips wrongly).
    let no_stats = V5::new("k".into(), "t".into(), 0, 0, None);
    assert!(
        no_stats.sstable_may_have_range_tombstones(),
        "with no Statistics.db (min == 0) the gate must conservatively prime"
    );
}

// ---------------------------------------------------------------------------
// Finding 1: a static row shadowed by the partition tombstone must NOT leak its
// stale value into a surviving (newer) clustering row.
// ---------------------------------------------------------------------------

/// `t(pk int, ck int, s text STATIC, v text)`, PRIMARY KEY ((pk), ck).
#[cfg(feature = "write-support")]
fn static_schema() -> crate::schema::TableSchema {
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
                name: "pk".to_string(),
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
                name: "s".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
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

/// Collect every emitted live-row's cells over `block` (markers dropped).
#[cfg(feature = "write-support")]
fn emit_rows(
    parser: &V5,
    block: &[u8],
    schema: &crate::schema::TableSchema,
    reader: &crate::storage::sstable::reader::SSTableReader,
) -> Vec<crate::types::RowCells> {
    let mut out = Vec::new();
    parser
        // #3782: `block` is the whole stitched data section (see the callers).
        .parse_block_emit(
            block,
            super::BufferExtent::Complete,
            Some(schema),
            reader,
            |(_t, _k, row)| {
                if let crate::types::ScanRow::Row(cells) = row {
                    out.push(cells);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )
        .unwrap();
    out
}

/// A static row shadowed by the partition tombstone must be dropped from the merge
/// so a surviving NEWER clustering row does NOT carry the stale static value —
/// matching a Cassandra `SELECT` (issue #1741, Finding 1).
///
/// A single flushed generation cannot naturally hold a partition tombstone that
/// coexists with older-but-unpurged static data (the writer purges at flush), so we
/// construct the repro deterministically: write ONE partition with a static cell
/// `s='OLD'` at ts=1_000_000 and a clustering row `ck=1, v='new'` at ts=3_000_000
/// (both LIVE), then patch the flushed nb partition header to carry a
/// `markedForDeleteAt = 2_000_000` — between the two write timestamps. That shadows
/// the static row (deletes: 1e6 <= 2e6) while the newer clustering row (3e6 > 2e6)
/// survives.
///
/// Revert-verify: with the static-shadow check removed the shadowed static value is
/// still merged into the surviving row, so `s` reappears and the final assertion
/// FAILS. With the fix the surviving row carries no `s`, while the physical
/// (no-shadow) parse still shows the leak — proving the differential is real.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn shadowed_static_row_is_not_merged_into_surviving_rows() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = static_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    // Static cell s='OLD' at the OLDER timestamp.
    let static_mut = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "s".to_string(),
            value: Value::text("OLD".to_string()),
        }],
        1_000_000,
        None,
    );
    // Clustering row ck=1, v='new' at the NEWER timestamp (survives the tombstone).
    let row_mut = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("new".to_string()),
        }],
        3_000_000,
        None,
    );
    let key = static_mut.decorated_key(&schema).unwrap();
    writer
        .write_partition(key, vec![static_mut, row_mut])
        .unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();

    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    assert!(buf.len() >= 2, "decompressed partition must exist");

    // Patch the single partition header (offset 0) to carry a partition tombstone.
    // nb layout: flags(1) | key_len(1) | key | localDeletionTime(i32 BE) |
    // markedForDeleteAt(i64 BE). The Big writer emits `nb` (12-byte deletion), so
    // this is a same-size in-place patch.
    let key_len = buf[1] as usize;
    let del_off = 2 + key_len;
    assert!(
        buf.len() >= del_off + 12,
        "nb partition header must carry a 12-byte deletion field"
    );
    let local_deletion_time: i32 = 1_700_000_000; // real (not the i32::MAX LIVE sentinel)
    buf[del_off..del_off + 4].copy_from_slice(&local_deletion_time.to_be_bytes());
    let marked_for_delete_at: i64 = 2_000_000; // between static ts (1e6) and row ts (3e6)
    buf[del_off + 4..del_off + 12].copy_from_slice(&marked_for_delete_at.to_be_bytes());

    let p_phys = reader.build_v5_parser(false);
    let p_shadow = reader.build_v5_parser(true);

    let phys = emit_rows(&p_phys, &buf, &schema, &reader);
    let shadow = emit_rows(&p_shadow, &buf, &schema, &reader);

    // Physical parse: the clustering row carries the (older) static value 's'.
    assert_eq!(
        phys.len(),
        1,
        "physical parse must emit the one clustering row"
    );
    assert!(
        phys[0].iter().any(|(n, _)| &**n == "s"),
        "physical (no-shadow) parse must leak the static value 's' into the clustering row \
         (anti-empty-pass: proves the merge happens)"
    );

    // Shadow parse: the NEWER clustering row survives the partition tombstone...
    assert_eq!(
        shadow.len(),
        1,
        "the newer clustering row (ts=3e6 > markedForDeleteAt=2e6) must survive the tombstone"
    );
    // ...but the shadowed static value must NOT be merged into it.
    assert!(
        !shadow[0].iter().any(|(n, _)| &**n == "s"),
        "Finding 1: the partition-tombstone-shadowed static row must NOT be merged into the \
         surviving clustering row (its 's' cell is stale and a SELECT must hide it)"
    );
    // Sanity: the surviving row keeps its own live cell.
    assert!(
        shadow[0].iter().any(|(n, _)| &**n == "v"),
        "the surviving clustering row must keep its own live 'v' cell"
    );
}

/// Issue #1741 (Finding 2): the marker-only prefix skip (`skip_row_framing`) must
/// advance over a data row to EXACTLY the same offset the full cell-decode
/// (`parse_row_data_with_offset`) reaches — otherwise a primed prefix would
/// desynchronise and either miss or double-feed a range-tombstone marker. This pins
/// the framing-only fast path (flags + clustering + row_size) against the
/// authoritative decode for both the static row and the clustering rows of a real
/// writer-produced partition.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn skip_row_framing_matches_full_decode_offset() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = static_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    let static_mut = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(7)),
        None,
        vec![CellOperation::Write {
            column: "s".to_string(),
            value: Value::text("stat".to_string()),
        }],
        1_000_000,
        None,
    );
    let mut muts = vec![static_mut];
    for ck in 1..=3 {
        muts.push(Mutation::new(
            TableId::new("ks", "t"),
            PartitionKey::single("pk", Value::Integer(7)),
            Some(ClusteringKey::single("ck", Value::Integer(ck))),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::text(format!("v{ck}")),
            }],
            2_000_000 + ck as i64,
            None,
        ));
    }
    let key = muts[0].decorated_key(&schema).unwrap();
    writer.write_partition(key, muts).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let buf = reader.stitch_all_chunks(&cursor).await.unwrap();

    let parser = reader.build_v5_parser(false);
    let resolution = super::RowColumnResolution::build(&schema, &reader);

    // Walk the single partition; for each row compare the framing-only skip offset
    // to the full-decode next offset.
    let (_pk, mut off, _del) = parser.parse_partition_header_full(&buf, 0).unwrap();
    let mut rows_checked = 0usize;
    loop {
        if off >= buf.len() || V5::is_end_of_partition(buf[off]) {
            break;
        }
        // Only peek AFTER at least one row has been consumed — the first row's bytes
        // can look like a partition header to the peek probe (mirrors the real loop).
        if rows_checked > 0 && parser.peek_is_partition_header(&buf, off) {
            break;
        }
        let full_next = match parser.parse_row_data_with_offset(
            &buf,
            off,
            Some(&schema),
            &reader,
            false,
            &resolution,
            None,
        ) {
            Ok((.., n, _is_static, _c)) => n,
            Err(_) => break,
        };
        let framed_next = parser
            .skip_row_framing(&buf, off, &schema)
            .expect("skip_row_framing must succeed on a valid row");
        assert_eq!(
            framed_next, full_next,
            "skip_row_framing offset must equal the full-decode next offset (row at {off})"
        );
        rows_checked += 1;
        off = full_next;
    }
    assert!(
        rows_checked >= 4,
        "expected the static row + 3 clustering rows to be framing-checked, got {rows_checked}"
    );
}

// ---------------------------------------------------------------------------
// roborev pass F: per-CELL tombstone/TTL shadowing (Findings 1 & 2). The
// shadowing is now applied PER CELL (and per whole collection), not just per
// row, matching Cassandra which shadows individual cells written at or before a
// covering deletion or expired by TTL.
// ---------------------------------------------------------------------------

/// `t(pk int, ck int, a text, b text)`, PRIMARY KEY ((pk), ck) — two regular
/// (non-static) columns so a single row can carry two cells at DIFFERENT write
/// timestamps (via `Mutation::cell_write_timestamps`).
#[cfg(feature = "write-support")]
fn two_col_schema() -> crate::schema::TableSchema {
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
    let col = |name: &str, ty: &str, is_static: bool| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static,
    };
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
            col("pk", "int", false),
            col("ck", "int", false),
            col("a", "text", false),
            col("b", "text", false),
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Patch the single (offset-0) `nb` partition header in-place to carry a
/// partition tombstone with `markedForDeleteAt = mfda` (µs). Layout: flags(1) |
/// key_len(1) | key | localDeletionTime(i32 BE) | markedForDeleteAt(i64 BE).
#[cfg(feature = "write-support")]
fn patch_partition_tombstone(buf: &mut [u8], mfda: i64) {
    let key_len = buf[1] as usize;
    let del_off = 2 + key_len;
    assert!(
        buf.len() >= del_off + 12,
        "nb partition header must carry a 12-byte deletion field"
    );
    let local_deletion_time: i32 = 1_700_000_000; // real (not the i32::MAX LIVE sentinel)
    buf[del_off..del_off + 4].copy_from_slice(&local_deletion_time.to_be_bytes());
    buf[del_off + 4..del_off + 12].copy_from_slice(&mfda.to_be_bytes());
}

/// Finding 1 (partial-row survival): a row under a partition tombstone at
/// `markedForDeleteAt = 2e6` where cell `a` was written at ts=1e6 (<= 2e6, so
/// shadowed) and cell `b` at ts=3e6 (> 2e6, survives). A Cassandra `SELECT`
/// returns the row with ONLY `b` populated — `a` is stale and must be absent.
///
/// Revert-verify: with per-cell filtering removed the whole row is decided at
/// once, so the stale `a` cell reappears and the `!contains("a")` assertion
/// FAILS; the physical (no-shadow) parse still shows both cells (anti-empty-pass).
#[cfg(feature = "write-support")]
#[tokio::test]
async fn partial_row_survival_drops_shadowed_cell() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = two_col_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    // One row, two cells: a@1e6 (via per-cell override), b@3e6 (row ts). Row
    // liveness marker is at 3e6, so the marker survives the tombstone too.
    let mut m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![
            CellOperation::Write {
                column: "a".to_string(),
                value: Value::text("AAA".to_string()),
            },
            CellOperation::Write {
                column: "b".to_string(),
                value: Value::text("BBB".to_string()),
            },
        ],
        3_000_000,
        None,
    );
    m.cell_write_timestamps = Some(std::collections::HashMap::from([(
        "a".to_string(),
        1_000_000i64,
    )]));
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    patch_partition_tombstone(&mut buf, 2_000_000);

    let phys = emit_rows(&reader.build_v5_parser(false), &buf, &schema, &reader);
    let shadow = emit_rows(&reader.build_v5_parser(true), &buf, &schema, &reader);

    assert_eq!(phys.len(), 1, "physical parse emits the row");
    assert!(
        phys[0].iter().any(|(n, _)| &**n == "a") && phys[0].iter().any(|(n, _)| &**n == "b"),
        "physical (no-shadow) parse must carry BOTH cells (anti-empty-pass)"
    );

    assert_eq!(
        shadow.len(),
        1,
        "the row survives (cell b @3e6 and the marker @3e6 are newer than the tombstone @2e6)"
    );
    assert!(
        shadow[0].iter().any(|(n, _)| &**n == "b"),
        "the surviving cell b (ts 3e6 > 2e6) must be present"
    );
    assert!(
        !shadow[0].iter().any(|(n, _)| &**n == "a"),
        "Finding 1: the shadowed cell a (ts 1e6 <= 2e6) must be dropped per-cell, not returned stale"
    );
}

/// `t(pk int, ck int, tags set<int>)`, PRIMARY KEY ((pk), ck) — a non-frozen
/// set column so an ELEMENT can carry its OWN write timestamp distinct from the
/// row liveness (via `CellOperation::WriteComplexElement`).
#[cfg(feature = "write-support")]
fn set_schema() -> crate::schema::TableSchema {
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![col("pk", "int"), col("ck", "int"), col("tags", "set<int>")],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Finding 2 (collection element newer than a partition tombstone): a non-frozen
/// `set<int>` element written at ts=3e6 while the row liveness marker is at ts=1e6
/// and a partition tombstone is patched at markedForDeleteAt=2e6. Because the
/// element (3e6) is newer than the tombstone (2e6), the element AND the row must
/// survive — even though the row marker (1e6) predates the tombstone.
///
/// Revert-verify: with only `row_header.timestamp` folded into the row's
/// max-cell timestamp (the pre-fix behavior), the row's max ts is 1e6 <= 2e6, so
/// the whole row is wrongly shadowed and the shadow parse returns 0 rows; the
/// physical parse still returns 1 (anti-empty-pass). With `max_element_writetime`
/// folded in, the row survives.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn collection_element_newer_than_partition_tombstone_survives() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = set_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    // Row marker at 1e6; one set element `7` written at its OWN ts=3e6.
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: 7i32.to_be_bytes().to_vec(),
            value: None, // set members store the element in the cell path
            timestamp_micros: 3_000_000,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        }],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    patch_partition_tombstone(&mut buf, 2_000_000);

    let phys = emit_rows(&reader.build_v5_parser(false), &buf, &schema, &reader);
    let shadow = emit_rows(&reader.build_v5_parser(true), &buf, &schema, &reader);

    assert_eq!(
        phys.len(),
        1,
        "physical parse emits the row with the set element (anti-empty-pass)"
    );
    assert_eq!(
        shadow.len(),
        1,
        "Finding 2: the row must survive because its set element (ts 3e6) is newer than the \
         partition tombstone (2e6), even though the row marker (1e6) predates it"
    );
    assert!(
        shadow[0].iter().any(|(n, _)| &**n == "tags"),
        "the surviving set column must be present"
    );
}

/// `t(pk int, ck int, tags set<int>, note text)`, PRIMARY KEY ((pk), ck) — a
/// non-frozen set PLUS a scalar column, so the row can survive a tombstone via a
/// live scalar cell while its collection is entirely shadowed (test 3).
#[cfg(feature = "write-support")]
fn set_scalar_schema() -> crate::schema::TableSchema {
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
            col("pk", "int"),
            col("ck", "int"),
            col("tags", "set<int>"),
            col("note", "text"),
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Extract the `tags` set column from an emitted row as a sorted `Vec<i32>` of its
/// integer members (empty when the column is absent).
#[cfg(feature = "write-support")]
fn tag_ints(row: &crate::types::RowCells) -> Vec<i32> {
    let mut out = Vec::new();
    for (n, v) in row.iter() {
        if &**n == "tags" {
            if let crate::types::Value::Set(members) = v {
                for m in members {
                    if let crate::types::Value::Integer(i) = m {
                        out.push(*i);
                    }
                }
            }
        }
    }
    out.sort_unstable();
    out
}

/// Issue #1741 (per-element filtering, test 1 — mixed old/new): a SURVIVING
/// non-frozen `set<int>` must return ONLY the elements newer than the covering
/// tombstone. Element `1` is written at ts=1e6 (<= tombstone 2e6, shadowed) and
/// element `2` at ts=3e6 (> 2e6, survives); the row marker is at 1e6. A Cassandra
/// `SELECT` returns the row with `tags = {2}` — element `1` is stale and must be
/// dropped from WITHIN the surviving collection, and the row survives because its
/// newer element (3e6) outlives the tombstone.
///
/// Revert-verify: with per-ELEMENT filtering removed the whole collection is kept
/// or dropped together, so the stale element `1` reappears in the returned set and
/// the `== [2]` assertion FAILS; the physical (no-shadow) parse still shows both
/// elements (anti-empty-pass).
#[cfg(feature = "write-support")]
#[tokio::test]
async fn surviving_collection_drops_shadowed_elements() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = set_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    // Row marker at 1e6; element `1` at its OWN ts=1e6 (shadowed), element `2` at
    // ts=3e6 (survives). Set members store the element in the cell path.
    let elem = |v: i32, ts: i64| CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: v.to_be_bytes().to_vec(),
        value: None,
        timestamp_micros: ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![elem(1, 1_000_000), elem(2, 3_000_000)],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    patch_partition_tombstone(&mut buf, 2_000_000);

    let phys = emit_rows(&reader.build_v5_parser(false), &buf, &schema, &reader);
    let shadow = emit_rows(&reader.build_v5_parser(true), &buf, &schema, &reader);

    assert_eq!(phys.len(), 1, "physical parse emits the row");
    assert_eq!(
        tag_ints(&phys[0]),
        vec![1, 2],
        "physical (no-shadow) parse must carry BOTH set elements (anti-empty-pass)"
    );

    assert_eq!(
        shadow.len(),
        1,
        "the row survives because its element 2 (ts 3e6) is newer than the tombstone (2e6)"
    );
    assert_eq!(
        tag_ints(&shadow[0]),
        vec![2],
        "per-element filtering: only element 2 (ts 3e6 > 2e6) survives; the shadowed \
         element 1 (ts 1e6 <= 2e6) must be dropped from WITHIN the surviving set"
    );
}

/// Issue #1741 (per-element filtering, test 3 — all elements shadowed): a
/// non-frozen `set<int>` whose EVERY element predates the covering tombstone must
/// read as ABSENT (no `tags` column), even when the ROW itself survives via a
/// DIFFERENT live cell. Set elements `1` and `2` are both at ts=1e6 (<= tombstone
/// 2e6, shadowed) while a scalar `note='keep'` cell (and the row marker) is at
/// ts=3e6 (> 2e6, survives). A Cassandra `SELECT` returns the row with `note` but
/// NO `tags` — an all-shadowed non-frozen collection is null and must not keep the
/// column present.
///
/// Revert-verify: with per-element filtering removed the whole collection is kept
/// (its effective max ts folds the surviving row ts 3e6 > 2e6, so the old
/// whole-collection drop does NOT fire), so both stale elements reappear and the
/// `tags`-absent assertion FAILS; the physical parse still shows them
/// (anti-empty-pass).
#[cfg(feature = "write-support")]
#[tokio::test]
async fn all_shadowed_collection_reads_as_absent() {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;

    let schema = set_scalar_schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    let elem = |v: i32, ts: i64| CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: v.to_be_bytes().to_vec(),
        value: None,
        timestamp_micros: ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    // Scalar `note` + row marker at 3e6 (survives the tombstone); both set elements
    // at 1e6 (shadowed).
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![
            CellOperation::Write {
                column: "note".to_string(),
                value: Value::text("keep".to_string()),
            },
            elem(1, 1_000_000),
            elem(2, 1_000_000),
        ],
        3_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();
    patch_partition_tombstone(&mut buf, 2_000_000);

    let phys = emit_rows(&reader.build_v5_parser(false), &buf, &schema, &reader);
    let shadow = emit_rows(&reader.build_v5_parser(true), &buf, &schema, &reader);

    assert_eq!(phys.len(), 1, "physical parse emits the row");
    assert_eq!(
        tag_ints(&phys[0]),
        vec![1, 2],
        "physical (no-shadow) parse must carry BOTH shadowed elements (anti-empty-pass)"
    );

    assert_eq!(
        shadow.len(),
        1,
        "the row survives via its live scalar cell (note @3e6 > tombstone 2e6)"
    );
    assert!(
        shadow[0].iter().any(|(n, _)| &**n == "note"),
        "the surviving row must keep its live scalar 'note' cell"
    );
    assert!(
        !shadow[0].iter().any(|(n, _)| &**n == "tags"),
        "per-element filtering: every set element (ts 1e6 <= 2e6) is shadowed, so the \
         collection reads as ABSENT and must NOT resurface the stale elements"
    );
}
