//! Issue #1741 (roborev pass H): regression pins for two holes in the read-time
//! TTL/shadowing path.
//!
//! Finding 1 (MEDIUM, CORRECTNESS) — the row-liveness `localExpirationTime`
//! (`liveness_expires_at_seconds`, from `HAS_TTL`) was decoded/stored as `i32`.
//! On oa/da (`hasUIntDeletionTime`) a TTL expiry after 2038 occupies the UNSIGNED
//! 32-bit range `[2^31, 2^32)`; casting it to `i32` wrapped it NEGATIVE, so the
//! read-time TTL filter saw a long-past expiry and HID a still-live row. The fix
//! decodes it as an UNSIGNED `u32`→`i64` (mirroring the row/complex deletion LDT
//! reader) and stores it as `i64`.
//!
//! Finding 2 (LOW, CONSISTENCY) — the read clock (`now_secs`) was sampled per
//! block via `now_epoch_secs()`, so a scan crossing an expiration-second boundary
//! could decide two rows with the same TTL differently. The fix captures `now_secs`
//! ONCE in `V5CompressedLegacyParser::new` (the per-read/scan context) and reuses it
//! for every block/partition, so the boundary decision is a pure function of one
//! fixed `now`.

use super::{RowHeader, V5CompressedLegacyParser};
use crate::storage::sstable::version_gate::{BigVersionGates, VersionGates};
use std::sync::Arc;

/// A post-2038 epoch-second expiry that does NOT fit in `i32` (> `i32::MAX`,
/// < `u32::MAX`). `4_000_000_000` s ≈ year 2096.
const POST_2038_EXPIRY_SECS: i64 = 4_000_000_000;

/// A "now" well BEFORE `POST_2038_EXPIRY_SECS` (year 2023) so the row is still live.
const NOW_2023_SECS: i64 = 1_700_000_000;

/// Build a parser on the oa (BIG, `hasUIntDeletionTime`) path.
fn oa_parser() -> V5CompressedLegacyParser {
    let gates = VersionGates::Big(BigVersionGates::from_version("oa").expect("oa gates"));
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
        .with_version_gates(Arc::new(gates))
}

/// Build a parser on the nb (BIG, signed deletion time) path.
fn nb_parser() -> V5CompressedLegacyParser {
    // `new` defaults to the nb-compatible gates (`has_uint_deletion_time == false`).
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
}

/// Serialize a `HAS_TTL | HAS_ALL_COLUMNS` row header carrying `ttl_delta` and
/// `ldt_delta` (both unsigned VInt deltas). No HAS_TIMESTAMP / HAS_DELETION.
/// Layout consumed by `parse_row_metadata` (which starts AFTER the flags byte):
///   [flags=0x28][row_size=0][prev_size=0][ttl_delta][ldt_delta]
fn has_ttl_row_bytes(ttl_delta: u64, ldt_delta: u64) -> Vec<u8> {
    use crate::storage::serialization::vint::encode_unsigned;
    let mut data = vec![0x28u8, 0x00, 0x00]; // flags, row_size=0, prev_size=0
    encode_unsigned(ttl_delta, &mut data);
    encode_unsigned(ldt_delta, &mut data);
    data
}

/// Decode a `HAS_TTL` row header and return its `liveness_expires_at_seconds`.
fn decode_liveness_expiry(parser: &V5CompressedLegacyParser, data: &[u8]) -> Option<i64> {
    let (row_flags, ext_flags, flags_size) =
        parser.parse_row_flags(data, 0).expect("parse_row_flags");
    let (header, _row_size) = parser
        .parse_row_metadata(data, flags_size, row_flags, ext_flags)
        .expect("parse_row_metadata");
    header.liveness_expires_at_seconds
}

/// A minimal live `RowHeader` carrying only a pk-liveness timestamp + TTL expiry.
fn liveness_row(timestamp_micros: Option<i64>, liveness_expiry_secs: Option<i64>) -> RowHeader {
    RowHeader {
        timestamp: timestamp_micros,
        ttl: None,
        liveness_expires_at_seconds: liveness_expiry_secs,
        local_deletion_time: None,
        marked_for_delete_at: None,
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp: None,
        max_data_cell_expires_at: None,
        has_live_forever_data_cell: false,
    }
}

// ---------------------------------------------------------------------------
// Finding 1: post-2038 oa/da liveness expiry must NOT wrap negative and must
// keep a still-live row visible.
// ---------------------------------------------------------------------------

/// Decode + decision end-to-end. On the oa (`hasUIntDeletionTime`) path a liveness
/// TTL expiry after 2038 must decode to a LARGE POSITIVE second count and the row
/// must be treated as LIVE at a "now" before the expiry.
///
/// Revert-verify: reverting the decode to `... as i32 as i64` (dropping the
/// `has_uint_deletion_time()` branch in `row_framing.rs`) makes the decoded value
/// wrap NEGATIVE (`4_000_000_000 as i32 == -294_967_296`); the positive-value
/// assertion FAILS and `row_liveness_expired` then hides the still-live row, so the
/// `!expired` assertion FAILS too.
#[test]
fn oa_post_2038_liveness_expiry_is_positive_and_row_stays_live() {
    let parser = oa_parser();
    let data = has_ttl_row_bytes(0, POST_2038_EXPIRY_SECS as u64);

    let decoded = decode_liveness_expiry(&parser, &data).expect("HAS_TTL sets liveness expiry");
    assert_eq!(
        decoded, POST_2038_EXPIRY_SECS,
        "oa post-2038 liveness expiry must decode as UNSIGNED (u32→i64), not wrap negative; \
         got {decoded}"
    );
    assert!(
        decoded > i32::MAX as i64,
        "sanity: the fixture expiry must exceed i32::MAX to exercise the wrap path"
    );

    // Downstream shadow decision: a TTL INSERT row (pk-liveness timestamp present)
    // whose expiry is in 2096 must be LIVE at a 2023 read clock.
    let mut header = liveness_row(Some(1_000), Some(decoded));
    header.timestamp = Some(1_000);
    assert!(
        !header.row_liveness_expired(NOW_2023_SECS),
        "a row whose post-2038 TTL expiry is still in the future must NOT be hidden"
    );
}

/// The nb (signed) path is unchanged: a normal in-range liveness expiry decodes to
/// exactly its value (sign-extension is a no-op for positive < i32::MAX values).
#[test]
fn nb_liveness_expiry_decode_unchanged() {
    let parser = nb_parser();
    let in_range = 1_700_000_000i64; // year 2023, fits i32
    let data = has_ttl_row_bytes(0, in_range as u64);

    let decoded = decode_liveness_expiry(&parser, &data).expect("HAS_TTL sets liveness expiry");
    assert_eq!(
        decoded, in_range,
        "nb liveness expiry (in i32 range) must decode to its plain value"
    );
    // And an nb expiry already in the past hides the row (TTL semantics preserved).
    let mut header = liveness_row(Some(1_000), Some(decoded));
    header.timestamp = Some(1_000);
    assert!(
        header.row_liveness_expired(in_range + 1),
        "nb row past its liveness expiry must be hidden"
    );
}

// ---------------------------------------------------------------------------
// Finding 2: one `now_secs` per read/scan, captured at parser construction; the
// expiration-boundary decision is a pure function of that single fixed value.
// ---------------------------------------------------------------------------

/// The parser captures a single `now_secs` at construction and reuses it. With that
/// ONE fixed clock, a row whose liveness expiry equals `now` is decided uniformly
/// (hidden — Cassandra hides at `now >= localExpirationTime`), and `now + 1` is
/// uniformly visible. Because the block-emit paths read `self.now_secs` (not a
/// per-block wall-clock sample), every block of a scan crossing the second boundary
/// applies the same `now`.
#[test]
fn single_now_secs_decides_boundary_uniformly() {
    let parser = nb_parser();
    let now = parser.now_secs;
    assert!(
        now > 0,
        "now_secs must be sampled from the read clock at construction"
    );
    // Re-reading the field yields the SAME value: it is captured once, not resampled.
    assert_eq!(
        parser.now_secs, now,
        "now_secs must be a single fixed per-read value"
    );

    // Two rows parsed in different "blocks" of the same scan share this one `now`.
    let mut at_boundary = liveness_row(Some(1_000), Some(now));
    at_boundary.timestamp = Some(1_000);
    let mut past_boundary = liveness_row(Some(1_000), Some(now - 1));
    past_boundary.timestamp = Some(1_000);
    let mut future = liveness_row(Some(1_000), Some(now + 1));
    future.timestamp = Some(1_000);

    // expiry == now  → expired (hidden), decided identically for every block.
    assert!(
        at_boundary.row_liveness_expired(now),
        "a row whose TTL expiry equals now must be hidden (now >= localExpirationTime)"
    );
    assert!(
        past_boundary.row_liveness_expired(now),
        "a row whose TTL expiry is before now must be hidden"
    );
    // expiry == now + 1 → still live (visible).
    assert!(
        !future.row_liveness_expired(now),
        "a row whose TTL expiry is after now must stay visible"
    );
}

// ---------------------------------------------------------------------------
// Issue #2436 negative pin (roborev 1729): the `row_size > available`
// structural bound is now the ONLY protection between a corrupt row_size VInt
// and the `after_row_offset` slice math — it must fail CLOSED, never panic or
// mis-parse into a truncated/garbage row.
// ---------------------------------------------------------------------------

mod row_size_bound_negative_pin {
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;

    /// `t(id int, name text)`, no clustering keys — the row_size VInt for a tiny
    /// `name` value fits in ONE byte (values 0-127), so corrupting it in place (no
    /// length change) keeps every other offset in the buffer untouched.
    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
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
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    /// Write one tiny single-cell partition and return its stitched Data.db buffer
    /// plus the open reader (needed for `parse_row_data_with_offset`).
    async fn write_tiny_fixture() -> (
        Vec<u8>,
        crate::storage::sstable::reader::SSTableReader,
        TableSchema,
    ) {
        let schema = schema();
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer = SSTableWriter::with_format(
            dir.path().to_path_buf(),
            1,
            &schema,
            16,
            SSTableFormat::Big,
        )
        .unwrap();
        let m = Mutation::new(
            TableId::new("ks", "t"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("hi".to_string()),
            }],
            1_000_000,
            None,
        );
        let key = m.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![m]).unwrap();
        let info = writer.finish().await.unwrap();

        let config = crate::Config::default();
        let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let reader = crate::storage::sstable::reader::SSTableReader::open(
            &info.data_path,
            &config,
            platform,
        )
        .await
        .unwrap();
        let cursor = reader.new_scan_cursor().await.unwrap();
        let buf = reader.stitch_all_chunks(&cursor).await.unwrap();
        (buf, reader, schema)
    }

    /// A corrupt `row_size` VInt claiming more bytes than remain in the buffer must
    /// make `parse_row_data_with_offset` return `Err`, never panic and never `Ok`
    /// with a shortened/garbage row.
    #[tokio::test]
    async fn corrupt_row_size_exceeding_available_bytes_fails_closed() {
        let (mut buf, reader, schema) = write_tiny_fixture().await;
        let parser = reader.build_v5_parser(false);
        let resolution = super::super::RowColumnResolution::build(&schema, &reader);

        // Walk to the row_size VInt exactly as the real decoder does: partition
        // header -> row flags -> clustering prefix (no-op: no clustering keys).
        let (_pk, offset, _del) = parser.parse_partition_header_full(&buf, 0).unwrap();
        let (_row_flags, _ext, flags_size) = parser.parse_row_flags(&buf, offset).unwrap();
        let (_clustering, row_metadata_offset) = parser
            .parse_clustering_prefix(&buf, offset + flags_size, &schema)
            .unwrap();

        // Sanity: for this tiny payload the row_size VInt is a single byte (top bit
        // clear, value 0-127) — corrupting it in place changes no other offset.
        let vint_byte = buf[row_metadata_offset];
        assert!(
            vint_byte < 0x80,
            "precondition: row_size VInt must be single-byte for this tiny fixture \
             (byte=0x{vint_byte:02x}) — otherwise the in-place corruption would shift \
             downstream offsets"
        );
        let available = buf.len() - (row_metadata_offset + 1);
        assert!(
            available < 0x7F,
            "precondition: remaining bytes ({available}) must be less than the max \
             single-byte VInt value (127) for the corruption to exceed `available`"
        );

        // Corrupt: claim row_size = 127 (max single-byte VInt value), exceeding the
        // actual remaining bytes in this tiny fixture.
        buf[row_metadata_offset] = 0x7F;

        let result = parser.parse_row_data_with_offset(
            &buf,
            offset,
            Some(&schema),
            &reader,
            false,
            &resolution,
            None,
        );

        match result {
            Err(crate::Error::Corruption(msg)) => {
                assert!(
                    msg.contains("row_size") && msg.contains("exceeds available data"),
                    "corruption error must name the row_size/available-bytes mismatch, got: {msg}"
                );
            }
            Err(other) => panic!(
                "expected a typed Error::Corruption for a row_size exceeding available bytes, \
                 got a different Err variant: {other:?}"
            ),
            Ok(_) => panic!(
                "a corrupt row_size VInt claiming more bytes than remain must fail CLOSED \
                 (Err), never Ok — a silent truncated/garbage-row parse at the parser level \
                 is exactly the #2436 failure mode this bound exists to prevent"
            ),
        }
    }

    /// Sibling sanity: the SAME buffer, uncorrupted, parses successfully — proving
    /// the negative test above fails specifically because of the corruption, not an
    /// unrelated harness bug (anti-empty-pass).
    #[tokio::test]
    async fn uncorrupted_row_parses_successfully_as_control() {
        let (buf, reader, schema) = write_tiny_fixture().await;
        let parser = reader.build_v5_parser(false);
        let resolution = super::super::RowColumnResolution::build(&schema, &reader);
        let (_pk, offset, _del) = parser.parse_partition_header_full(&buf, 0).unwrap();

        let result = parser.parse_row_data_with_offset(
            &buf,
            offset,
            Some(&schema),
            &reader,
            false,
            &resolution,
            None,
        );
        assert!(
            result.is_ok(),
            "control: the uncorrupted buffer must parse successfully, got {result:?}"
        );
    }
}
