//! Issue #1627 — the block-path schema decoder must NOT silently blob-decode
//! scalar CQL types.
//!
//! [`SSTableReader::parse_value_with_schema_type`] and its sibling
//! [`SSTableReader::parse_value_with_comparator`] are `pub(in ...reader)`, so
//! this proof lives in-crate rather than in `tests/`.
//!
//! ## Reach status (verification-first, issue #1627)
//!
//! These two methods sit on the `parse_partition_data` block path, which is only
//! reached through `iterate_all_partitions` when the Index.db digest lookup
//! resolves *every* Summary.db entry. On the real Cassandra corpus that lookup
//! never fully resolves, so `iterate_all_partitions` falls back to
//! `sequential_scan` (the V5CompressedLegacy path, which decodes these types
//! correctly) — see issue #500. User-facing `scan`/`get`/BTI never touch these
//! methods either. The defect is therefore **latent**: no live corpus read
//! currently reaches it. Per the issue's verification-first rule we prove the
//! defect (and its fix) by driving the exact production methods directly with
//! authoritatively-encoded bytes, asserting the correct typed `Value` variants
//! instead of `Value::Blob`.

use crate::parser::vint::encode_vint;
use crate::storage::sstable::reader::SSTableReader;
use crate::types::Value;
use crate::{Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// A real Cassandra 5.0 `nb` fixture whose header carries a full schema, so the
/// opened reader has the machinery `parse_value_with_schema_type` needs. We use
/// it only to obtain a genuine `SSTableReader` instance for the direct call.
fn simple_table_data_db() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_basic");
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with("simple_table-") {
            let candidate = entry.path().join("nb-1-big-Data.db");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

async fn open_reader() -> Option<SSTableReader> {
    let Some(path) = simple_table_data_db() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but the test_basic.simple_table fixture is absent"
        );
        eprintln!("SKIP: test_basic.simple_table fixture absent.");
        return None;
    };
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    Some(
        SSTableReader::open(&path, &config, platform)
            .await
            .expect("opening the structurally valid nb fixture should succeed"),
    )
}

/// Every scalar CQL type that previously fell through to `Value::Blob` must now
/// decode to its correct typed variant via `parse_value_with_schema_type`.
#[tokio::test]
async fn scalar_types_decode_typed_not_blob() {
    let Some(reader) = open_reader().await else {
        return;
    };

    // float (Float32): 4-byte big-endian IEEE-754.
    let bytes = 3.5f32.to_be_bytes();
    assert_eq!(
        reader
            .parse_value_with_schema_type(&bytes, "float")
            .unwrap(),
        Value::Float32(3.5),
        "CQL float must decode to Value::Float32, not Value::Blob"
    );

    // double (Float): 8-byte big-endian IEEE-754.
    let bytes = 6.25f64.to_be_bytes();
    assert_eq!(
        reader
            .parse_value_with_schema_type(&bytes, "double")
            .unwrap(),
        Value::Float(6.25),
        "CQL double must decode to Value::Float, not Value::Blob"
    );

    // timestamp: 8-byte big-endian millis.
    let ts: i64 = 1_700_000_000_123;
    assert_eq!(
        reader
            .parse_value_with_schema_type(&ts.to_be_bytes(), "timestamp")
            .unwrap(),
        Value::Timestamp(ts),
        "CQL timestamp must decode to Value::Timestamp, not Value::Blob"
    );

    // varint: raw big-endian two's-complement bytes preserved verbatim.
    let varint_bytes = vec![0x01, 0x00];
    assert_eq!(
        reader
            .parse_value_with_schema_type(&varint_bytes, "varint")
            .unwrap(),
        Value::Varint(varint_bytes.clone()),
        "CQL varint must decode to Value::Varint, not Value::Blob"
    );

    // decimal: 4-byte scale + unscaled bytes.
    let mut dec = Vec::new();
    dec.extend_from_slice(&2i32.to_be_bytes());
    dec.extend_from_slice(&[0x30, 0x39]); // unscaled = 12345
    assert_eq!(
        reader
            .parse_value_with_schema_type(&dec, "decimal")
            .unwrap(),
        Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39],
        },
        "CQL decimal must decode to Value::Decimal, not Value::Blob"
    );

    // duration: three signed (zigzag) VInts months/days/nanos; all-zero => 0x00*3.
    let dur = vec![0x00, 0x00, 0x00];
    assert_eq!(
        reader
            .parse_value_with_schema_type(&dur, "duration")
            .unwrap(),
        Value::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        },
        "CQL duration must decode to Value::Duration, not Value::Blob"
    );

    // time: 8-byte big-endian nanoseconds-since-midnight.
    let t: i64 = 4_500_000_000_000; // 01:15:00.000000000
    assert_eq!(
        reader
            .parse_value_with_schema_type(&t.to_be_bytes(), "time")
            .unwrap(),
        Value::Time(t),
        "CQL time must decode to Value::Time, not Value::Blob"
    );

    // inet: raw address bytes (IPv4 here).
    let ip = vec![10u8, 0, 0, 1];
    assert_eq!(
        reader.parse_value_with_schema_type(&ip, "inet").unwrap(),
        Value::Inet(ip.clone()),
        "CQL inet must decode to Value::Inet, not Value::Blob"
    );
}

/// The sibling collection-element decoder must also decode scalar elements to
/// their typed variants (issue #1627: same `_ => blob` defect class, same path).
/// Driving `list<double>` routes each element through
/// `parse_value_with_comparator`, which previously blob-decoded `double`.
#[tokio::test]
async fn collection_scalar_elements_decode_typed_not_blob() {
    let Some(reader) = open_reader().await else {
        return;
    };

    // list<double> with two elements [1.5, 2.5]. CollectionSerializer: VInt count,
    // then per element VInt length + big-endian f64.
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2));
    for v in [1.5f64, 2.5f64] {
        data.extend_from_slice(&encode_vint(8));
        data.extend_from_slice(&v.to_be_bytes());
    }

    let decoded = reader
        .parse_value_with_schema_type(&data, "list<double>")
        .unwrap();
    assert_eq!(
        decoded,
        Value::List(vec![Value::Float(1.5), Value::Float(2.5)]),
        "list<double> elements must decode to Value::Float, not Value::Blob"
    );
}
