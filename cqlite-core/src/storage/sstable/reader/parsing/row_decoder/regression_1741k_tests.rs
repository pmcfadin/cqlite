//! Issue #1741 (roborev pass K): regression pin for a chunk-boundary desync
//! introduced by the earlier oa/da deletion-time header-min-size fix.
//!
//! Finding (HIGH): the two sliding parsers
//! (`parse_one_partition_with_timestamps`, `parse_one_partition_for_compaction`)
//! sized the oa/da (`hasUIntDeletionTime`) partition header minimum at a flat
//! 1 byte. But only the LIVE sentinel (`0x80`) is 1 byte; a DELETED oa/da
//! partition carries the full 12-byte DeletionTime (8-byte `markedForDeleteAt`
//! plus 4-byte unsigned `localDeletionTime`). If a deleted partition header was
//! split across a NON-FINAL chunk with only the first deletion byte present, the
//! guard wrongly allowed parsing: `parse_partition_header_full` failed mid-buffer
//! and the sliding parser returned `Emitted(1)` (a byte skip) instead of
//! `NeedMore`, desyncing the scan. On the COMPACTION path that dropped the
//! partition tombstone, letting older/deleted data survive or resurface.
//!
//! The fix routes both parsers' need-more decision through
//! [`super::V5CompressedLegacyParser::partition_header_readiness`], which peeks the
//! DeletionTime discriminator to size the header correctly and returns `Incomplete`
//! (mapped to `NeedMore` on a non-final chunk) when the full deleted DeletionTime
//! is not yet present.
//!
//! Test shape (two layers):
//!
//! - Deterministic unit tests on `partition_header_readiness` itself. Both sliding
//!   parsers route their emit/need-more decision through this one classifier, so a
//!   unit test on it deterministically pins both. No reader/schema/feature needed.
//! - End-to-end tests (write-support gated) that drive the ACTUAL sliding parsers
//!   over a hand-constructed split buffer, using a real `da` reader for the schema
//!   resolution. A real chunked fixture is not synthesizable here: Cassandra purges
//!   rows covered by a partition tombstone at flush and compression chunk
//!   boundaries are not byte-addressable, so a real SSTable cannot place a deleted
//!   partition's 12-byte DeletionTime exactly astride a chunk boundary. The
//!   hand-built split buffer reproduces that exact shape directly.

use super::V5CompressedLegacyParser;
use crate::storage::sstable::version_gate::{BigVersionGates, BtiVersionGates, VersionGates};
use std::sync::Arc;

use super::PartitionHeaderReadiness;

/// Build a parser on the oa (BIG, `hasUIntDeletionTime`) path.
fn oa_parser() -> V5CompressedLegacyParser {
    let gates = VersionGates::Big(BigVersionGates::from_version("oa").expect("oa gates"));
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
        .with_version_gates(Arc::new(gates))
}

/// Build a parser on the da (BTI, `hasUIntDeletionTime`) path.
fn da_parser() -> V5CompressedLegacyParser {
    let gates = VersionGates::Bti(BtiVersionGates::from_version("da").expect("da gates"));
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
        .with_version_gates(Arc::new(gates))
}

/// Build a parser on the nb (BIG, signed 12-byte deletion time) path.
fn nb_parser() -> V5CompressedLegacyParser {
    // `new` defaults to the nb-compatible gates (`has_uint_deletion_time == false`).
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
}

/// Bytes of a DELETED oa/da partition header for a 4-byte (int) partition key.
///
/// Layout: `flags(1) | key_len(1) | key(4) | markedForDeleteAt(i64 BE, 8) |
/// localDeletionTime(u32 BE, 4)`. The first `markedForDeleteAt` byte is `0x00`
/// (bit 7 clear) so the discriminator is NOT the `0x80` LIVE sentinel — i.e. this
/// is unambiguously the 12-byte DELETED form.
fn deleted_oa_partition_header() -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x00); // partition flags
    buf.push(0x04); // key length (int pk = 4 bytes)
    buf.extend_from_slice(&42i32.to_be_bytes()); // partition key
                                                 // markedForDeleteAt = 1_000_000 µs → high byte 0x00 (bit 7 clear).
    buf.extend_from_slice(&1_000_000i64.to_be_bytes());
    // localDeletionTime = 1_600_000_000 s (~year 2020), a real tombstone LDT.
    buf.extend_from_slice(&1_600_000_000u32.to_be_bytes());
    buf
}

/// The offset of the DeletionTime discriminator byte in the header above.
const DELETION_DISCRIMINATOR_OFFSET: usize = 2 + 4; // flags + key_len + key

// ---------------------------------------------------------------------------
// Layer 1: deterministic unit tests on the shared need-more classifier.
// ---------------------------------------------------------------------------

/// A DELETED oa/da partition header split so a NON-FINAL chunk holds only the
/// partition key + the FIRST deletion byte (not the full 12-byte DeletionTime)
/// must be classified `Incomplete` (→ `NeedMore`), never `Ready`.
///
/// Revert-verify: with the classifier sizing the oa/da header minimum at a flat
/// 1 byte (`deletion_time_min = 1`), this split buffer is `Ready` (the pre-fix
/// bug), so the `Incomplete` assertion FAILS.
#[test]
fn deleted_oa_header_split_at_first_deletion_byte_needs_more() {
    let full = deleted_oa_partition_header();
    // Keep only up to and including the first deletion byte.
    let split = &full[..=DELETION_DISCRIMINATOR_OFFSET];
    assert_eq!(
        split.len(),
        DELETION_DISCRIMINATOR_OFFSET + 1,
        "sanity: split holds key + exactly one deletion byte"
    );

    for parser in [oa_parser(), da_parser()] {
        assert_eq!(
            parser.partition_header_readiness(split),
            PartitionHeaderReadiness::Incomplete,
            "a deleted oa/da header with only the first deletion byte present must be Incomplete"
        );
        // Once the full 12-byte DeletionTime is present, it becomes Ready.
        assert_eq!(
            parser.partition_header_readiness(&full),
            PartitionHeaderReadiness::Ready,
            "the complete deleted oa/da header must be Ready"
        );
    }
}

/// The discriminator byte itself not yet present (chunk ends right after the key)
/// is also `Incomplete` — we must not guess the deletion form.
#[test]
fn oa_header_without_discriminator_byte_needs_more() {
    let full = deleted_oa_partition_header();
    let no_disc = &full[..DELETION_DISCRIMINATOR_OFFSET]; // flags + key_len + key only
    assert_eq!(
        oa_parser().partition_header_readiness(no_disc),
        PartitionHeaderReadiness::Incomplete
    );
}

/// A LIVE oa/da partition (1-byte `0x80` sentinel) is `Ready` with just that one
/// byte present — the fix must not over-require bytes for the live form.
#[test]
fn live_oa_header_is_ready_with_one_deletion_byte() {
    let mut buf = vec![0x00u8, 0x04];
    buf.extend_from_slice(&42i32.to_be_bytes());
    buf.push(super::row_framing::OA_IS_LIVE_DELETION); // 0x80 LIVE sentinel
    for parser in [oa_parser(), da_parser()] {
        assert_eq!(
            parser.partition_header_readiness(&buf),
            PartitionHeaderReadiness::Ready,
            "a live oa/da partition needs only its single 0x80 sentinel byte"
        );
    }
}

/// The nb (signed) path is unchanged: it always requires the fixed 12-byte
/// DeletionTime regardless of the deletion state.
#[test]
fn nb_header_always_requires_full_twelve_bytes() {
    let parser = nb_parser();
    let mut buf = vec![0x00u8, 0x04];
    buf.extend_from_slice(&42i32.to_be_bytes());
    // Only 1 of the 12 deletion bytes present → Incomplete for nb.
    buf.push(0x00);
    assert_eq!(
        parser.partition_header_readiness(&buf),
        PartitionHeaderReadiness::Incomplete
    );
    // Fill out the remaining 11 bytes → Ready.
    buf.extend_from_slice(&[0u8; 11]);
    assert_eq!(
        parser.partition_header_readiness(&buf),
        PartitionHeaderReadiness::Ready
    );
}

/// An invalid header shape (zero key length) is `Malformed` (→ skip a byte),
/// distinct from `Incomplete`.
#[test]
fn zero_key_length_is_malformed() {
    assert_eq!(
        oa_parser().partition_header_readiness(&[0x00, 0x00]),
        PartitionHeaderReadiness::Malformed
    );
}

/// Issue #3928 (fix round 3, B2) — the BLOCK-EMIT header arm must reach the same
/// verdict as the classifier above, because #1741's fix is a property of the
/// SIZING RULE and not of one caller.
///
/// The sliding drivers route through `partition_header_readiness`, so the cases
/// above pin them. The block-emit arm
/// (`block_emit_windowed/partition_header_arm.rs`) re-derived the rule with a
/// FLAT minimum — `if has_uint_deletion_time() { 1 } else { 12 }`, with no look
/// at the discriminator — so a legitimate 12-byte DELETED DeletionTime
/// straddling a `BufferExtent::Window` passed the 1-byte minimum, failed the
/// full parse, and returned `Resync`: a discarded byte on HEALTHY data, which
/// `buffer_extent.rs` records as being as much a defect as a swallow. That is
/// what drift between two implementations of one rule looks like — the driver
/// path got #1741's fix and this one did not — so the arm now CALLS the
/// classifier instead of re-deriving it.
///
/// BOTH directions are pinned here, because a fix that over-requires bytes is
/// the same defect mirrored: the split deleted form must NOT parse, and the
/// 1-byte LIVE form MUST.
#[test]
fn deleted_oa_header_split_at_first_deletion_byte_does_not_resync_the_block_walk() {
    use super::block_emit_windowed::partition_header_arm::HeaderStep;
    use super::buffer_extent::HeaderTolerance;
    use super::BufferExtent;

    let full = deleted_oa_partition_header();
    let split = &full[..=DELETION_DISCRIMINATOR_OFFSET];
    // A LIVE header needs only its single 0x80 sentinel byte.
    let mut live = vec![0x00u8, 0x04];
    live.extend_from_slice(&42i32.to_be_bytes());
    live.push(super::row_framing::OA_IS_LIVE_DELETION);

    for parser in [oa_parser(), da_parser()] {
        // A tolerant WINDOW never refuses, so `?` cannot fire here; the verdict
        // is which tolerant answer it gives.
        let tolerant = HeaderTolerance::for_extent(BufferExtent::Window);

        let step = parser
            .block_partition_header(split, 0, tolerant, 0)
            .expect("a tolerant window must not refuse");
        assert!(
            matches!(step, HeaderStep::EndOfBlock),
            "a DELETED oa/da header holding only its first deletion byte is INCOMPLETE, so \
             the tolerant walk must end and let the caller refill — it must not discard a \
             byte (Resync), which loses a header byte of HEALTHY straddling data"
        );

        // Control 1: the COMPLETE deleted header parses.
        match parser.block_partition_header(&full, 0, tolerant, 0) {
            Ok(HeaderStep::Parsed(..)) => {}
            other => panic!(
                "the complete deleted oa/da header must parse, got {}",
                describe_step(&other)
            ),
        }

        // Control 2 (the mirror defect): the 1-byte LIVE form must NOT be
        // over-required. A fix that simply raised the minimum to 12 would pass
        // the assertion above and red here.
        match parser.block_partition_header(&live, 0, tolerant, 0) {
            Ok(HeaderStep::Parsed(..)) => {}
            other => panic!(
                "a LIVE oa/da partition needs only its single 0x80 sentinel byte, got {}",
                describe_step(&other)
            ),
        }
    }
}

/// Render a `block_partition_header` outcome for an assertion message.
fn describe_step(
    r: &crate::Result<super::block_emit_windowed::partition_header_arm::HeaderStep>,
) -> String {
    use super::block_emit_windowed::partition_header_arm::HeaderStep;
    match r {
        Ok(HeaderStep::Parsed(k, next, del)) => {
            format!(
                "Parsed(key {} bytes, next {next}, deletion {del:?})",
                k.0.len()
            )
        }
        Ok(HeaderStep::EndOfBlock) => "EndOfBlock".to_string(),
        Ok(HeaderStep::Resync) => "Resync".to_string(),
        Err(e) => format!("Err({e})"),
    }
}

// ---------------------------------------------------------------------------
// Layer 2: end-to-end over the ACTUAL sliding parsers with a real da reader.
// ---------------------------------------------------------------------------

/// pk-only `tiny(pk int, v text)` schema used to mint a real `da` reader.
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

/// Open a freshly written tiny `da` SSTable, returning `(reader, schema)`. The
/// reader is only needed for the sliding parsers' schema→column resolution and to
/// put the parser on the oa/da (`hasUIntDeletionTime`) path; the hand-built buffers
/// we feed carry their own bytes.
#[cfg(feature = "write-support")]
async fn da_reader() -> (
    crate::storage::sstable::reader::SSTableReader,
    crate::schema::TableSchema,
) {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;

    let schema = tiny_schema();
    let dir = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();
    let mutation = Mutation::new(
        TableId::new("test_ks", "tiny"),
        PartitionKey::single("pk", Value::Integer(7)),
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

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    assert!(
        reader.has_uint_deletion_time(),
        "sanity: the da reader must be on the hasUIntDeletionTime path"
    );
    (reader, schema)
}

/// A full DELETED-partition buffer that parses cleanly: the deleted header
/// followed by an END_OF_PARTITION marker (0x01) so the row loop terminates with
/// zero rows.
///
/// Gated `write-support` (issue #1981) to match its only callers — the two
/// `#[cfg(feature = "write-support")]` sliding-parser tests below (and the
/// sibling `tiny_schema`/`da_reader` helpers) — so it is not dead-code under the
/// minimal feature set's `-D warnings` build.
#[cfg(feature = "write-support")]
fn deleted_partition_full_buffer() -> Vec<u8> {
    let mut buf = deleted_oa_partition_header();
    buf.push(0x01); // END_OF_PARTITION
    buf
}

/// Windowed streaming sliding parser: a deleted oa/da header split so the
/// non-final chunk holds only key + the first deletion byte must return
/// `NeedMore`, then parse cleanly once the rest of the DeletionTime arrives.
///
/// Revert-verify: with the flat 1-byte oa minimum, the split chunk passes the
/// header guard, `parse_partition_header_full` fails on the truncated 12-byte
/// DeletionTime and the parser returns `Emitted(1)` — the desync. The `NeedMore`
/// assertion FAILS.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn windowed_parser_needs_more_on_split_deleted_header() {
    let (reader, schema) = da_reader().await;
    let parser = reader.build_v5_parser(false);

    let full = deleted_partition_full_buffer();
    let split = &full[..=DELETION_DISCRIMINATOR_OFFSET];

    // Non-final chunk with only the first deletion byte → NeedMore, emit nothing.
    let mut emitted = 0usize;
    let step = parser
        .parse_one_partition_with_timestamps(split, Some(&schema), &reader, false, &mut |_e| {
            emitted += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(step, super::ParseStep::NeedMore),
        "split deleted header on a non-final chunk must return NeedMore, got {step:?}"
    );
    assert_eq!(emitted, 0, "no row may be emitted from the truncated chunk");

    // Once the full buffer is available, the partition parses cleanly.
    let step = parser
        .parse_one_partition_with_timestamps(&full, Some(&schema), &reader, true, &mut |_e| {
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(step, super::ParseStep::Emitted(_)),
        "the complete deleted partition must parse as Emitted, got {step:?}"
    );
}

/// Compaction sliding parser: the same split must return `NeedMore` (not skip a
/// byte), and once complete it must surface the partition tombstone as a
/// `PartitionDelete` `CompactionRow` — i.e. the tombstone is NOT dropped.
///
/// Revert-verify: with the flat 1-byte oa minimum, the split chunk returns
/// `Emitted(1)` and NO `PartitionDelete` row is produced — the partition tombstone
/// is dropped (the resurrection risk). Both assertions FAIL.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn compaction_parser_preserves_tombstone_across_split() {
    use crate::storage::sstable::reader::compaction_row::CompactionRowData;

    let (reader, schema) = da_reader().await;
    let parser = reader.build_v5_parser(false);

    let full = deleted_partition_full_buffer();
    let split = &full[..=DELETION_DISCRIMINATOR_OFFSET];

    // Non-final split chunk → NeedMore, no CompactionRow.
    let mut rows: Vec<crate::storage::sstable::reader::compaction_row::CompactionRow> = Vec::new();
    let step = parser
        .parse_one_partition_for_compaction(split, Some(&schema), &reader, false, &mut |r| {
            rows.push(r);
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(step, super::ParseStep::NeedMore),
        "split deleted header on a non-final chunk must return NeedMore, got {step:?}"
    );
    assert!(
        rows.is_empty(),
        "nothing may be emitted from the truncated chunk"
    );

    // Full buffer (final) → the partition tombstone survives as a PartitionDelete.
    let step = parser
        .parse_one_partition_for_compaction(&full, Some(&schema), &reader, true, &mut |r| {
            rows.push(r);
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert!(
        matches!(step, super::ParseStep::Emitted(_)),
        "the complete deleted partition must parse as Emitted, got {step:?}"
    );
    assert!(
        rows.iter()
            .any(|r| matches!(r.row_data, CompactionRowData::PartitionDelete { .. })),
        "the partition tombstone must survive compaction parsing as a PartitionDelete row \
         (dropping it would resurrect older data); got {rows:?}"
    );
}
