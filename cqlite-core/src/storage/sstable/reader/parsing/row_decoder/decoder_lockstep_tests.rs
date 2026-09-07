//! Decoder lockstep parity net (issue #1617, Epic H / finding H4).
//!
//! CQLite has (at least) two live per-CQL-type value decoders plus the write
//! side, and nothing asserts they agree. This module PINS that equivalence so
//! the J1/J2 consolidation refactors (epic #1603) can merge the decoders safely.
//! It is a **safety net**, not a fix: where the paths legitimately diverge TODAY
//! the divergence is recorded here with an explicit assertion + owning-issue link
//! (never silently tolerated), so the net documents the known-divergence set.
//!
//! ## The two read paths (and their DIFFERENT framing)
//!
//! * **v5 string-ladder** — [`V5CompressedLegacyParser::parse_cell_value_schema_order`]
//!   (the per-cell decoder `row_data.rs` drives for BIG and BTI). It is a
//!   *streaming* decoder that consumes a whole on-disk **cell**: `[flags byte]
//!   [conditional timestamp/ttl VInts][framed value]`. Variable-width values
//!   carry an unsigned-VInt length prefix; fixed-width values do not. It returns
//!   the decoded value *and the new offset*.
//! * **block / `ComparatorType` path** — [`SSTableReader::parse_value_with_schema_type`]
//!   (`value_parsing.rs`). An *exact-slice* decoder: it receives only the value
//!   bytes (the outer framing already stripped by the caller) and decodes them
//!   whole. This is the SAME convention the **write side**
//!   ([`TypeSerializer::serialize_value`], `serialization/types.rs`) emits — "no
//!   length prefix for the top-level value" — which is why the write↔read
//!   round-trip anchors naturally to the block path for scalars.
//!
//! Because the two paths use different framing, the lockstep is asserted at the
//! **logical-value** level: for each type + logical value we produce the exact
//! value bytes (the write-side / block convention) and *frame* them for the v5
//! path ([`frame_v5_cell`]), then assert the decoded [`Value`]s agree (or that
//! the divergence is documented).
//!
//! ## Two lockstep dimensions
//! * **Dimension A — dispatch lockstep** (always compiled): the same logical
//!   value through BOTH read paths agrees, and malformed input errors on BOTH
//!   (never one-`Ok`-one-`Err`), for every scalar type + representative
//!   frozen/collection/tuple.
//! * **Dimension B — read↔write codec lockstep** (`write-support` only): the
//!   write side serializes each value to the SAME canonical bytes, and both read
//!   paths decode them back. This pins the read decoders to the write-side truth
//!   (the audit's exemplary VInt/type reference).
//!
//! ## Known-divergence set recorded by this net (do NOT "fix" here — #1617)
//! * **`float`**: CONVERGED (issue #1884) — both paths now decode CQL `float` to
//!   [`Value::Float32`]. The v5 ladder previously widened to `Value::Float`
//!   (`f32 as f64`); that lossy widening is fixed.
//! * **`varint`**: CONVERGED (issue #1885) — the v5 ladder gained a `varint` arm,
//!   so both paths decode CQL `varint` to [`Value::Varint`]. It previously fell
//!   through to the blob default → `Value::Blob`.
//! * **non-frozen `list`/`set`/`map`**: the v5 *single-cell* ladder STUBS these
//!   to an empty collection — production routes non-frozen collections through
//!   the multi-cell complex-column path instead. Owning issue: **#162 / J1**.
//! * **frozen collections/tuple framing**: the write side + v5 frozen path use
//!   `i32`-BE count/length framing; the block `ComparatorType` collection decoder
//!   uses **VInt** framing (see `comparator_value_parsing.rs` doc). They do not
//!   round-trip each other's bytes. Owning issue: **J2**. Recorded in dimension B.
//!
//! ## Inherent framing artifacts NOT asserted as divergences
//! The v5 ladder is a *streaming* decoder (consumes what it needs, ignores
//! trailing bytes) while the block decoder is *exact-slice*. So "oversized /
//! trailing-byte" and "mid-VInt-length truncation" inputs are decided differently
//! by construction — that is a framing artifact J2 unifies, not a decode bug, and
//! is deliberately outside the malformed assertions here.
//!
//! ## Reuse for J1/J2
//! [`scalar_cases`] (the canonical type list + byte-buffer corpus) and the
//! framing/decode helpers are `pub(crate)` under `#[cfg(test)]` so the J1/J2
//! consolidation tests can drive the merged decoder against the same corpus.

use super::V5CompressedLegacyParser;
use crate::parser::vint::{encode_vint, encode_vuint};
use crate::schema::Column;
use crate::storage::sstable::reader::types::SSTableReader;
use crate::types::Value;
use crate::{Config, Platform};
#[cfg(not(feature = "write-support"))]
use std::path::PathBuf;
use std::sync::Arc;

// ===========================================================================
// Shared corpus + framing/decode helpers (reused by J1/J2)
// ===========================================================================

/// How the v5 ladder frames a value of this type on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum V5Framing {
    /// Fixed-width value with NO length prefix (int, bigint, uuid, float, …).
    Fixed,
    /// Variable-width value preceded by an unsigned-VInt length (text, blob,
    /// decimal, date, time, smallint, tinyint, inet, counter, duration, …).
    VintLen,
}

/// A documented, deliberate divergence between the two read paths (issue #1617).
///
/// Retained scaffolding: the float/varint divergences it recorded are now
/// converged (issues #1884/#1885), so no `ScalarCase` currently sets one. Kept so
/// the next genuinely-divergent type can be recorded without re-deriving the
/// machinery; `assert_ne`-based recording in the tests still exercises it once a
/// case populates `divergence: Some(..)`.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct Divergence {
    /// Owning issue/epic that will unify the paths.
    pub owner: &'static str,
    /// Human note explaining the divergence.
    pub note: &'static str,
    /// What the block / `ComparatorType` path decodes to today.
    pub block: Value,
    /// What the v5 string-ladder path decodes to today.
    pub v5: Value,
}

/// One canonical (type, logical value, exact wire bytes) corpus entry.
pub(crate) struct ScalarCase {
    /// CQL type string as it appears in a schema (`"int"`, `"text"`, …).
    pub cql_type: &'static str,
    /// The canonical logical value both paths should agree on (when convergent).
    pub value: Value,
    /// Exact value bytes (the block-path / write-side convention, no framing).
    pub value_bytes: Vec<u8>,
    /// How to frame `value_bytes` into a v5 on-disk cell body.
    pub framing: V5Framing,
    /// `true` for types that accept an arbitrary-length payload (text/blob/
    /// varint/ascii/varchar) — an empty payload is VALID, not malformed.
    pub arbitrary_len: bool,
    /// `Some` when the two paths legitimately diverge today (recorded, not fixed).
    pub divergence: Option<Divergence>,
}

/// The canonical scalar corpus: every CQL scalar type CQLite decodes, with a
/// representative value and its exact wire encoding.
pub(crate) fn scalar_cases() -> Vec<ScalarCase> {
    let uuid_bytes: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    // Date: Cassandra stores days-from-epoch offset by 2^31 (unsigned, for
    // byte-order comparability).
    let date_days: i32 = 19_000;
    let date_bytes = (date_days as u32)
        .wrapping_add(0x8000_0000)
        .to_be_bytes()
        .to_vec();
    // Duration: three signed (zigzag) VInts.
    let mut duration_bytes = encode_vint(1);
    duration_bytes.extend(encode_vint(2));
    duration_bytes.extend(encode_vint(3));

    vec![
        ScalarCase {
            cql_type: "boolean",
            value: Value::Boolean(true),
            value_bytes: vec![0x01],
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "tinyint",
            value: Value::TinyInt(7),
            value_bytes: vec![0x07],
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "smallint",
            value: Value::SmallInt(300),
            value_bytes: 300i16.to_be_bytes().to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "int",
            value: Value::Integer(42),
            value_bytes: 42i32.to_be_bytes().to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "bigint",
            value: Value::BigInt(9_000_000_000),
            value_bytes: 9_000_000_000i64.to_be_bytes().to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "counter",
            value: Value::Counter(12_345),
            value_bytes: 12_345i64.to_be_bytes().to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        // CONVERGED (issue #1884): both paths now decode CQL float to Value::Float32.
        ScalarCase {
            cql_type: "float",
            value: Value::Float32(3.5),
            value_bytes: 3.5f32.to_be_bytes().to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "double",
            value: Value::Float(6.25),
            value_bytes: 6.25f64.to_be_bytes().to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "text",
            value: Value::text("hi".to_string()),
            value_bytes: b"hi".to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: true,
            divergence: None,
        },
        ScalarCase {
            cql_type: "ascii",
            value: Value::text("abc".to_string()),
            value_bytes: b"abc".to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: true,
            divergence: None,
        },
        ScalarCase {
            cql_type: "varchar",
            value: Value::text("vc".to_string()),
            value_bytes: b"vc".to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: true,
            divergence: None,
        },
        ScalarCase {
            cql_type: "blob",
            value: Value::blob(vec![0xde, 0xad, 0xbe, 0xef]),
            value_bytes: vec![0xde, 0xad, 0xbe, 0xef],
            framing: V5Framing::VintLen,
            arbitrary_len: true,
            divergence: None,
        },
        ScalarCase {
            cql_type: "uuid",
            value: Value::Uuid(uuid_bytes),
            value_bytes: uuid_bytes.to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            // timeuuid decodes to Value::Uuid on BOTH paths (consistent).
            cql_type: "timeuuid",
            value: Value::Uuid(uuid_bytes),
            value_bytes: uuid_bytes.to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "timestamp",
            value: Value::Timestamp(1_600_000_000_000),
            value_bytes: 1_600_000_000_000i64.to_be_bytes().to_vec(),
            framing: V5Framing::Fixed,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "date",
            value: Value::Date(date_days),
            value_bytes: date_bytes,
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "time",
            value: Value::Time(3_600_000_000_000),
            value_bytes: 3_600_000_000_000i64.to_be_bytes().to_vec(),
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "duration",
            value: Value::Duration {
                months: 1,
                days: 2,
                nanos: 3,
            },
            value_bytes: duration_bytes,
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        // CONVERGED (issue #1885): v5 ladder gained a varint arm; both paths decode
        // CQL varint to Value::Varint.
        ScalarCase {
            cql_type: "varint",
            value: Value::varint(vec![0x01, 0x00]),
            value_bytes: vec![0x01, 0x00],
            framing: V5Framing::VintLen,
            arbitrary_len: true,
            divergence: None,
        },
        ScalarCase {
            cql_type: "decimal",
            value: Value::Decimal {
                scale: 2,
                unscaled: vec![0x30, 0x39],
            },
            value_bytes: {
                let mut b = 2i32.to_be_bytes().to_vec();
                b.extend([0x30, 0x39]);
                b
            },
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
        ScalarCase {
            cql_type: "inet",
            value: Value::inet(vec![10, 0, 0, 1]),
            value_bytes: vec![10, 0, 0, 1],
            framing: V5Framing::VintLen,
            arbitrary_len: false,
            divergence: None,
        },
    ]
}

/// Frame exact value bytes into a v5 on-disk cell body the ladder can decode.
///
/// Uses flags `0x08` (`USE_ROW_TIMESTAMP`): a live cell with a value and no
/// per-cell timestamp/ttl/deletion fields, so the ladder proceeds straight to
/// the value.
pub(crate) fn frame_v5_cell(framing: V5Framing, value_bytes: &[u8]) -> Vec<u8> {
    let mut cell = vec![0x08u8];
    match framing {
        V5Framing::Fixed => cell.extend_from_slice(value_bytes),
        V5Framing::VintLen => {
            cell.extend_from_slice(&encode_vuint(value_bytes.len() as u64));
            cell.extend_from_slice(value_bytes);
        }
    }
    cell
}

/// A single-column schema `Column` for the given CQL type.
pub(crate) fn test_column(cql_type: &str) -> Column {
    Column {
        name: "c".to_string(),
        data_type: cql_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// A minimal v5 parser (zeroed EncodingStats baselines — flags `0x08` skips the
/// delta-decoded timestamp/ttl fields, so the baselines are unused here).
pub(crate) fn v5_parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("test_ks".to_string(), "test_tbl".to_string(), 0, 0, None)
}

/// Decode a value through the v5 string-ladder per-cell decoder.
pub(crate) fn decode_v5(
    parser: &V5CompressedLegacyParser,
    reader: &SSTableReader,
    cql_type: &str,
    cell_bytes: &[u8],
) -> crate::Result<Value> {
    let col = test_column(cql_type);
    parser
        .parse_cell_value_schema_order(cell_bytes, 0, &col, None, None, reader)
        .map(|(v, _ts, _exp, _off)| v)
}

/// Decode a value through the block / `ComparatorType` path.
pub(crate) fn decode_block(
    reader: &SSTableReader,
    cql_type: &str,
    value_bytes: &[u8],
) -> crate::Result<Value> {
    reader.parse_value_with_schema_type(value_bytes, cql_type)
}

// ---------------------------------------------------------------------------
// Reader handle for the decoders (issue #1617, roborev — dataset-independent).
//
// Both decode paths need ONLY an `&SSTableReader` *handle*, never its file bytes:
// the v5 ladder takes it as an unused `&_reader`, and the block path is a method
// that uses `self` solely for recursion (`ComparatorType::from_data_type` +
// standalone value decoders). So the reader's own on-disk contents/schema are
// irrelevant to every value decoded here — all decoded bytes are synthesized.
//
// * With `write-support` (the gate's `write-tests` lane + the explicit
//   verification command), `open_reader` builds the handle from a MINIMAL
//   synthetic BIG SSTable in a leaked tempdir, so the whole lockstep net runs
//   UNCONDITIONALLY with NO on-disk dataset fixture. This is what fixes the
//   silent-no-op coverage gap: the J1/J2 safety net now executes in any lane that
//   doesn't fetch datasets.
// * Without `write-support` (no `SSTableWriter` available — e.g. the `--lite`
//   `cli-helpers`-only scoped run), it falls back to the real dataset fixture, or
//   soft-SKIPs when absent (unless `CQLITE_REQUIRE_FIXTURES`), matching the repo
//   convention. Dimension A remains always-compiled either way.
// ---------------------------------------------------------------------------

/// `true` when byte-parity fixtures are required (gate/CI); otherwise a missing
/// fixture is a soft SKIP (matches the repo convention).
#[cfg(not(feature = "write-support"))]
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

#[cfg(not(feature = "write-support"))]
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

/// A real Cassandra 5.0 `nb` fixture used ONLY to obtain a genuine
/// `SSTableReader` instance (both decoders require one — the v5 ladder takes it
/// as an unused `&reader`, the block path is a method on it). The fixture bytes
/// are not otherwise consulted; every value decoded here is synthesized. Only the
/// non-`write-support` build reaches for this (the `write-support` build synthesizes
/// its own reader with no dataset dependency).
#[cfg(not(feature = "write-support"))]
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

/// A single-partition BIG-format schema `t(pk int, v text)` PRIMARY KEY (pk). The
/// concrete columns are irrelevant to the decoders (see the module note above);
/// this table exists only so the writer emits a structurally valid, openable
/// SSTable.
#[cfg(feature = "write-support")]
fn synthetic_schema() -> crate::schema::TableSchema {
    use crate::schema::KeyColumn;
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    };
    crate::schema::TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("pk", "int", false), col("v", "text", true)],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Build a genuine `SSTableReader` from a MINIMAL synthetic BIG SSTable written to
/// a leaked tempdir — NO on-disk dataset fixture required (issue #1617 roborev).
/// The reader is a decoder handle only; its bytes are never consulted here.
#[cfg(feature = "write-support")]
async fn synthetic_reader() -> SSTableReader {
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};

    let schema = synthetic_schema();
    // Leak the tempdir so its backing files outlive the reader's file handles for
    // the whole test (mirrors `regression_1741k`). The decoders never read the
    // file, but keeping the files present keeps `open` robust to any access.
    let dir = Box::leak(Box::new(
        tempfile::TempDir::new().expect("create tempdir for synthetic sstable"),
    ));
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 1, SSTableFormat::Big)
            .expect("create synthetic SSTableWriter");

    let m = Mutation::new(
        TableId::new("test_ks", "test_tbl"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("x".to_string()),
        }],
        1_000_000,
        None,
    );
    let key = m
        .decorated_key(&schema)
        .expect("decorate synthetic partition key");
    writer
        .write_partition(key, vec![m])
        .expect("write synthetic partition");
    let info = writer.finish().await.expect("finish synthetic sstable");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(&info.data_path, &config, platform)
        .await
        .expect("opening the synthetic BIG sstable should succeed")
}

pub(crate) async fn open_reader() -> Option<SSTableReader> {
    #[cfg(feature = "write-support")]
    {
        // Dataset-independent: the lockstep net ALWAYS runs, no fixture needed.
        Some(synthetic_reader().await)
    }
    #[cfg(not(feature = "write-support"))]
    {
        let Some(path) = simple_table_data_db() else {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the test_basic.simple_table fixture is absent"
            );
            eprintln!("SKIP decoder_lockstep: test_basic.simple_table fixture absent.");
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
}

// ===========================================================================
// Dimension A — dispatch lockstep (block vs v5), always compiled
// ===========================================================================

/// Valid encodings: the same logical value decodes identically through both
/// paths, OR the divergence is the documented (recorded) one.
#[tokio::test]
async fn dispatch_lockstep_scalars_valid() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    for case in scalar_cases() {
        let block = decode_block(&reader, case.cql_type, &case.value_bytes);
        let cell = frame_v5_cell(case.framing, &case.value_bytes);
        let v5 = decode_v5(&parser, &reader, case.cql_type, &cell);

        match &case.divergence {
            None => {
                let b = block
                    .unwrap_or_else(|e| panic!("block decode of {} failed: {e:?}", case.cql_type));
                let v =
                    v5.unwrap_or_else(|e| panic!("v5 decode of {} failed: {e:?}", case.cql_type));
                assert_eq!(
                    b, case.value,
                    "block path decoded {} to the wrong value",
                    case.cql_type
                );
                assert_eq!(
                    v, case.value,
                    "v5 path decoded {} to the wrong value",
                    case.cql_type
                );
                assert_eq!(
                    b, v,
                    "LOCKSTEP BREAK: block vs v5 disagree on {} (undocumented divergence)",
                    case.cql_type
                );
            }
            Some(d) => {
                let b = block.unwrap_or_else(|e| {
                    panic!("block decode of divergent {} failed: {e:?}", case.cql_type)
                });
                let v = v5.unwrap_or_else(|e| {
                    panic!("v5 decode of divergent {} failed: {e:?}", case.cql_type)
                });
                assert_eq!(
                    b, d.block,
                    "documented divergence changed (block side) for {} — owner {}: {}",
                    case.cql_type, d.owner, d.note
                );
                assert_eq!(
                    v, d.v5,
                    "documented divergence changed (v5 side) for {} — owner {}: {}",
                    case.cql_type, d.owner, d.note
                );
                assert_ne!(
                    d.block, d.v5,
                    "recorded divergence for {} is no longer a divergence — remove the \
                     record and re-converge the lockstep (owner {})",
                    case.cql_type, d.owner
                );
            }
        }
    }
}

/// Malformed / empty input: for fixed-shape types BOTH paths must `Err` (never
/// one-`Ok`-one-`Err`). For arbitrary-length types an empty payload is VALID and
/// the two paths must still agree (or diverge exactly as recorded).
#[tokio::test]
async fn dispatch_lockstep_scalars_empty_payload() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    for case in scalar_cases() {
        if case.arbitrary_len {
            // Empty value is legitimate: block gets an empty slice, v5 gets a
            // properly framed zero-length cell.
            let block = decode_block(&reader, case.cql_type, &[]);
            let cell = frame_v5_cell(V5Framing::VintLen, &[]);
            let v5 = decode_v5(&parser, &reader, case.cql_type, &cell);
            let b = block.unwrap_or_else(|e| {
                panic!("block empty decode of {} failed: {e:?}", case.cql_type)
            });
            let v =
                v5.unwrap_or_else(|e| panic!("v5 empty decode of {} failed: {e:?}", case.cql_type));
            match &case.divergence {
                None => assert_eq!(
                    b, v,
                    "LOCKSTEP BREAK: block vs v5 disagree on empty {}",
                    case.cql_type
                ),
                Some(d) => assert_ne!(
                    b, v,
                    "recorded divergence for empty {} vanished (owner {})",
                    case.cql_type, d.owner
                ),
            }
        } else {
            // Fixed-shape type with no value bytes: both paths must reject.
            // block: empty slice. v5: a flags-only cell (no framed value).
            assert!(
                decode_block(&reader, case.cql_type, &[]).is_err(),
                "block path must reject an empty {} payload",
                case.cql_type
            );
            let flags_only = vec![0x08u8];
            assert!(
                decode_v5(&parser, &reader, case.cql_type, &flags_only).is_err(),
                "v5 path must reject a valueless {} cell",
                case.cql_type
            );
        }
    }
}

/// Truncated fixed-width value: dropping the last byte must `Err` on BOTH paths.
#[tokio::test]
async fn dispatch_lockstep_scalars_truncated_fixed() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    for case in scalar_cases() {
        if case.framing != V5Framing::Fixed || case.value_bytes.len() <= 1 {
            continue;
        }
        let trunc = &case.value_bytes[..case.value_bytes.len() - 1];
        assert!(
            decode_block(&reader, case.cql_type, trunc).is_err(),
            "block path must reject a truncated {}",
            case.cql_type
        );
        let cell = frame_v5_cell(V5Framing::Fixed, trunc);
        assert!(
            decode_v5(&parser, &reader, case.cql_type, &cell).is_err(),
            "v5 path must reject a truncated {}",
            case.cql_type
        );
    }
}

/// DOCUMENTED DIVERGENCE (#162 / J1): the v5 *single-cell* ladder stubs
/// non-frozen collections to an empty collection (production routes them through
/// the multi-cell complex-column path), while the block path attempts a real
/// decode and so rejects the flags-only/empty payload.
#[tokio::test]
async fn dispatch_divergence_nonfrozen_collections_documented() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    for cql in ["list<int>", "set<text>", "map<text,int>"] {
        // v5 single-cell ladder: empty-collection stub regardless of bytes.
        let cell = vec![0x08u8];
        let v5 = decode_v5(&parser, &reader, cql, &cell)
            .unwrap_or_else(|e| panic!("v5 decode of {cql} failed: {e:?}"));
        let is_empty_stub = matches!(&v5, Value::List(e) if e.is_empty())
            || matches!(&v5, Value::Set(e) if e.is_empty())
            || matches!(&v5, Value::Map(e) if e.is_empty());
        assert!(
            is_empty_stub,
            "v5 single-cell ladder must stub non-frozen {cql} to an empty collection \
             (#162 / J1); got {v5:?}"
        );
        // block path attempts a real (VInt-framed) decode -> rejects empty input.
        assert!(
            decode_block(&reader, cql, &[]).is_err(),
            "block path attempts a real decode of {cql} and must reject empty input \
             (diverges from the v5 stub; owner J2)"
        );
    }
}

// ===========================================================================
// Dimension B — read ↔ write codec lockstep (write-support only)
// ===========================================================================

#[cfg(feature = "write-support")]
mod write_read {
    use super::*;
    use crate::storage::serialization::types::TypeSerializer;

    /// The write side serializes each canonical value to EXACTLY the corpus wire
    /// bytes — pinning the corpus (and thus the read decoders) to the write-side
    /// truth (the audit's exemplary VInt/type reference).
    #[test]
    fn write_side_matches_canonical_bytes() {
        let ser = TypeSerializer::new();
        for case in scalar_cases() {
            let got = ser
                .serialize_value(&case.value, case.cql_type)
                .unwrap_or_else(|e| {
                    panic!("write side failed to serialize {}: {e:?}", case.cql_type)
                });
            assert_eq!(
                got, case.value_bytes,
                "write side wire bytes drifted from the canonical corpus for {}",
                case.cql_type
            );
        }
    }

    /// Round-trip: write side -> BOTH read paths -> original value (or the
    /// recorded divergence). Sources the bytes from the serializer so the read
    /// decoders are pinned against the write-side encoder end-to-end.
    #[tokio::test]
    async fn codec_lockstep_write_read_roundtrip_scalars() {
        let Some(reader) = open_reader().await else {
            return;
        };
        let parser = v5_parser();
        let ser = TypeSerializer::new();

        for case in scalar_cases() {
            let bytes = ser
                .serialize_value(&case.value, case.cql_type)
                .unwrap_or_else(|e| panic!("serialize {} failed: {e:?}", case.cql_type));

            let block = decode_block(&reader, case.cql_type, &bytes)
                .unwrap_or_else(|e| panic!("block decode of {} failed: {e:?}", case.cql_type));
            let cell = frame_v5_cell(case.framing, &bytes);
            let v5 = decode_v5(&parser, &reader, case.cql_type, &cell)
                .unwrap_or_else(|e| panic!("v5 decode of {} failed: {e:?}", case.cql_type));

            match &case.divergence {
                None => {
                    assert_eq!(
                        block, case.value,
                        "write->block round-trip {}",
                        case.cql_type
                    );
                    assert_eq!(v5, case.value, "write->v5 round-trip {}", case.cql_type);
                }
                Some(d) => {
                    assert_eq!(block, d.block, "write->block divergence {}", case.cql_type);
                    assert_eq!(v5, d.v5, "write->v5 divergence {}", case.cql_type);
                }
            }
        }
    }

    /// Structural types via write->v5: the write side and the v5 frozen/tuple
    /// paths share `i32`-BE count/length framing, so write->v5 always
    /// round-trips.
    ///
    /// On the block side BOTH structural families now converge:
    /// * **frozen collections** — CONVERGED by issue #2339 (roborev job 124). The
    ///   block decoder used **VInt** element framing and so did not reproduce the
    ///   i32-BE write-side bytes: the divergence formerly DOCUMENTED here and owned
    ///   by **J2**. Both `ComparatorType::Frozen` arms in `value_parsing.rs` and the
    ///   one in `comparator_value_parsing.rs` now route through
    ///   `frozen_value_parsing::parse_frozen_inner_with`, so a frozen collection body
    ///   is read as i32-BE element-framed on every path.
    /// * **tuple** — the block tuple decoder already used `i32`-BE field lengths
    ///   (`parse_tuple_value_with`), so it always CONVERGED.
    ///
    /// This test was the TRIPWIRE for that convergence: it asserted the divergence
    /// was still PRESENT and instructed whoever resolved it to "re-converge the
    /// structural lockstep", which is what flipping the three `block_converges`
    /// flags to `true` does. It now pins the convergence in the other direction — a
    /// regression to VInt framing on any frozen path reds it.
    #[tokio::test]
    async fn codec_lockstep_write_v5_structural() {
        let Some(reader) = open_reader().await else {
            return;
        };
        let parser = v5_parser();
        let ser = TypeSerializer::new();

        // (cql_type, value, block_converges_with_v5)
        let cases: Vec<(&str, Value, bool)> = vec![
            (
                "frozen<list<int>>",
                Value::List(vec![Value::Integer(1), Value::Integer(2)]),
                true,
            ),
            (
                "frozen<set<text>>",
                Value::Set(vec![Value::Text("a".into()), Value::Text("b".into())]),
                true,
            ),
            (
                "frozen<map<text,int>>",
                Value::Map(vec![(Value::Text("k".into()), Value::Integer(9))]),
                true,
            ),
            (
                "tuple<int,text>",
                Value::Tuple(vec![Value::Integer(5), Value::Text("hi".into())]),
                true,
            ),
        ];

        for (cql, value, block_converges) in cases {
            let bytes = ser
                .serialize_value(&value, cql)
                .unwrap_or_else(|e| panic!("serialize {cql} failed: {e:?}"));
            // v5 frozen collection / tuple cell: [flags][VUInt blob_len][i32-BE body].
            let mut cell = vec![0x08u8];
            cell.extend_from_slice(&encode_vuint(bytes.len() as u64));
            cell.extend_from_slice(&bytes);

            let v5 = decode_v5(&parser, &reader, cql, &cell)
                .unwrap_or_else(|e| panic!("v5 decode of {cql} failed: {e:?}"));
            let expected = if cql.starts_with("frozen<") {
                Value::Frozen(Box::new(value.clone()))
            } else {
                value.clone()
            };
            assert_eq!(v5, expected, "write->v5 structural round-trip {cql}");

            let block = decode_block(&reader, cql, &bytes);
            let block_matches = matches!(&block, Ok(v) if *v == expected);
            if block_converges {
                assert!(
                    block_matches,
                    "LOCKSTEP BREAK: block path must round-trip i32-BE {cql} bytes \
                     (got {block:?})"
                );
            } else {
                // DOCUMENTED divergence (J2): block VInt framing != i32-BE write bytes.
                assert!(
                    !block_matches,
                    "block path unexpectedly round-tripped i32-BE {cql} bytes — the \
                     VInt-vs-i32-BE collection-framing divergence (owner J2) may be \
                     resolved; re-converge the structural lockstep"
                );
            }
        }
    }
}

// ===========================================================================
// Issue #1885 — v5 ladder `varint` arm (mirror of the block path)
// ===========================================================================

/// The v5 ladder now decodes `varint` to `Value::Varint` (not `Value::Blob`),
/// byte-for-byte with the block / `ComparatorType::Varint` path, across the edge
/// cases the block path handles: negative, zero, and a value larger than i64
/// (varint is arbitrary-precision). The stored bytes are raw two's-complement
/// big-endian — `num_bigint::BigInt::from_signed_bytes_be` recovers the integer.
#[tokio::test]
async fn v5_varint_arm_decodes_edge_cases() {
    use num_bigint::BigInt;

    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    // (label, canonical BigInt) — encoded to signed BE bytes, framed for both paths.
    let cases: Vec<(&str, BigInt)> = vec![
        ("zero", BigInt::from(0)),
        ("small_positive", BigInt::from(42)),
        ("negative", BigInt::from(-12_345_678_i64)),
        ("minus_one", BigInt::from(-1)),
        // Larger than i64::MAX — arbitrary-precision.
        (
            "greater_than_i64",
            BigInt::from(i64::MAX) * BigInt::from(1_000_000) + BigInt::from(7),
        ),
        // More negative than i64::MIN.
        (
            "less_than_i64_min",
            BigInt::from(i64::MIN) * BigInt::from(1_000_000) - BigInt::from(7),
        ),
    ];

    for (label, n) in cases {
        // Cassandra `varint` on-disk bytes = Java BigInteger.toByteArray() = signed
        // two's-complement big-endian (never empty for a real value; zero → [0x00]).
        let value_bytes = n.to_signed_bytes_be();

        // Block path: exact-slice raw bytes.
        let block = decode_block(&reader, "varint", &value_bytes)
            .unwrap_or_else(|e| panic!("block varint {label} failed: {e:?}"));
        // v5 path: VInt-length-prefixed framing.
        let cell = frame_v5_cell(V5Framing::VintLen, &value_bytes);
        let v5 = decode_v5(&parser, &reader, "varint", &cell)
            .unwrap_or_else(|e| panic!("v5 varint {label} failed: {e:?}"));

        // Both paths must produce Value::Varint with the SAME raw bytes.
        assert_eq!(
            v5,
            Value::varint(value_bytes.clone()),
            "v5 varint {label} must decode to Value::Varint (not Blob)"
        );
        assert_eq!(
            block, v5,
            "LOCKSTEP: block vs v5 disagree on varint {label}"
        );

        // And the bytes must round-trip back to the canonical integer.
        if let Value::Varint(bytes) = &v5 {
            assert_eq!(
                BigInt::from_signed_bytes_be(bytes),
                n,
                "varint {label} bytes must round-trip to the canonical BigInt"
            );
        } else {
            panic!("varint {label}: expected Value::Varint, got {v5:?}");
        }
    }
}

/// An empty `varint` cell decodes to `Value::Varint([])` on the v5 path, matching
/// the block path's empty-slice handling (issue #1885). This covers BOTH v5 wire
/// framings that can carry a zero-length varint:
///  1. A live cell with a VInt length prefix of `0` (`[0x08][0x00]`) — the
///     `CellKind::Complex("varint")` ladder arm (in `cell_value_complex`) reads an
///     empty payload via `read_vint_length_prefixed_bytes` → `Value::Varint([])`.
///  2. A flags-only cell with the `CELL_HAS_EMPTY_VALUE` bit set and NO value bytes
///     (`[0x0C]` = `CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE`) — the empty-value
///     arm in `parse_cell_value_schema_order` matches `CellKind::Complex("varint")`
///     → `Value::Varint(Vec::new())` (roborev coverage gap: this arm was previously
///     never exercised).
#[tokio::test]
async fn v5_varint_empty_matches_block() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    let block = decode_block(&reader, "varint", &[])
        .unwrap_or_else(|e| panic!("block empty varint failed: {e:?}"));

    // Framing 1: live cell, VInt length prefix = 0.
    let cell_len0 = frame_v5_cell(V5Framing::VintLen, &[]);
    let v5_len0 = decode_v5(&parser, &reader, "varint", &cell_len0)
        .unwrap_or_else(|e| panic!("v5 zero-length varint failed: {e:?}"));
    assert_eq!(
        v5_len0,
        Value::varint(Vec::new()),
        "zero-length-prefix v5 varint → Varint([])"
    );
    assert_eq!(
        block, v5_len0,
        "zero-length-prefix varint block vs v5 must agree"
    );

    // Framing 2: flags-only cell with CELL_HAS_EMPTY_VALUE set — no value bytes at
    // all. 0x0C = CELL_USE_ROW_TIMESTAMP (0x08) | CELL_HAS_EMPTY_VALUE (0x04); the
    // row-timestamp bit means no timestamp delta is read, so the whole cell is the
    // single flags byte. This is the branch that hits the empty `Value::Varint(vec)`
    // arm rather than reading a length-prefixed payload.
    let cell_empty_flag = vec![0x0Cu8];
    let v5_empty_flag = decode_v5(&parser, &reader, "varint", &cell_empty_flag)
        .unwrap_or_else(|e| panic!("v5 empty-value-flag varint failed: {e:?}"));
    assert_eq!(
        v5_empty_flag,
        Value::varint(Vec::new()),
        "empty-value-flag (0x0C) v5 varint → Varint([]) via the empty arm"
    );
    assert_eq!(
        block, v5_empty_flag,
        "empty-value-flag varint block vs v5 must agree"
    );
}

// ===========================================================================
// Issue #1632 (item b) — per-cell v5 duration i32 overflow hardening
// ===========================================================================

/// Frame a v5 per-cell `duration` body from three signed VInt components:
/// `[flags 0x08][VUInt body_len][months][days][nanos]`.
#[cfg(test)]
fn frame_v5_duration_cell(months: i64, days: i64, nanos: i64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(months));
    body.extend_from_slice(&encode_vint(days));
    body.extend_from_slice(&encode_vint(nanos));
    frame_v5_cell(V5Framing::VintLen, &body)
}

/// Sanity: an in-range per-cell duration still decodes through the v5 ladder
/// (`parse_cell_value_schema_order`) — the guard rejects nothing valid.
#[tokio::test]
async fn v5_cell_duration_in_range_ok() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();
    let cell = frame_v5_duration_cell(1, 2, 3);
    let v = decode_v5(&parser, &reader, "duration", &cell)
        .expect("in-range duration should decode through the v5 cell path");
    assert_eq!(
        v,
        Value::Duration {
            months: 1,
            days: 2,
            nanos: 3
        }
    );
}

/// Issue #1632 (item b): the per-cell `duration` arm (`cell_value.rs`) must
/// REJECT a months/days VInt outside the i32 range instead of silently wrapping
/// via `as i32`. Pre-fix, `months as i32` wraps `i32::MAX + 1` to a negative
/// value and returns Ok; the guard turns it into an error.
#[tokio::test]
async fn v5_cell_duration_out_of_i32_range_errors() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();

    let over = frame_v5_duration_cell(i32::MAX as i64 + 1, 0, 0);
    assert!(
        decode_v5(&parser, &reader, "duration", &over).is_err(),
        "months > i32::MAX must error, not wrap via `as i32`"
    );

    let under = frame_v5_duration_cell(0, i32::MIN as i64 - 1, 0);
    assert!(
        decode_v5(&parser, &reader, "duration", &under).is_err(),
        "days < i32::MIN must error, not wrap via `as i32`"
    );
}
