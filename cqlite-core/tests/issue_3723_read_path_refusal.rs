//! Issue #3723: a fixed-width collection element whose declared on-disk length
//! is one Cassandra REFUSES must surface as an ERROR at the READ PATH — never a
//! silently partial row, and never a set that is quietly missing a member.
//!
//! ## Why this file exists (wiring evidence, not helper-only coverage)
//!
//! The width guard itself lives in `raw_value/fixed_width.rs` and was pinned by
//! decoder-level unit tests. Those tests could not see that BOTH consumers of
//! the guard SWALLOWED its error:
//!
//! * the complex-column loop logged at `debug!` and `break`, returning an
//!   apparently-successful row with a SHORT cell list;
//! * the multicell-`set` member decode mapped any error to `None`, OMITTING the
//!   member, and an EMPTY cell path never reached the decoder at all.
//!
//! So the refusal was unobservable from a real read. (Those are two of FIVE
//! tolerant sites; the complete census, including the FIFTH that is deliberately
//! left tolerant under issue #3778, is in `raw_value/fatal_decode_error.rs` and
//! is characterised by section 4 below.)
//!
//! Every case below therefore goes through the PUBLIC reader surface
//! (`SSTableReader::open` + `iterate_all_partitions`) over a byte-patched
//! `Data.db`.
//!
//! ## Oracle
//!
//! Expectations derive from the pinned `cassandra-5.0.8` source, NOT from
//! CQLite's prior output:
//!
//! * `serializers/Int32Serializer.java` `validate(...)`:
//!   `if (accessor.size(value) != 4 && !accessor.isEmpty(value)) throw new
//!   MarshalException(String.format("Expected 4 or 0 byte int (%d)", ...))` — a
//!   3-byte or 5-byte `int` is refused at the ELEMENT level, by throwing.
//! * `serializers/SetSerializer.java` `validate(...)`/`deserialize(...)`: the
//!   per-element `elements.validate(value, accessor)` exception is NOT caught,
//!   and a leftover byte additionally throws `"Unexpected extraneous bytes after
//!   set value"`. Cassandra rejects the WHOLE value; it does not drop the bad
//!   element and hand back the rest of the collection.
//!
//! The zero-length case is the one place this decoder is deliberately STRICTER
//! than `Int32Serializer.validate` (which admits `0`, deserializing to Java
//! `null`): there is no `Value` here meaning "this set member deserialized to
//! null", so issue #3723's AC2 refuses it. That decision, its reasons, and the
//! exact remedy if a real Cassandra-written fixture is ever found carrying one
//! are recorded in `raw_value/fixed_width.rs`. Its DISPOSITION differs from a
//! wrong width's — refused, but TOLERATED, because these bytes already errored
//! (as `Error::Corruption`) before this branch and every tolerant site absorbed
//! it.
//! `raw_value/fatal_decode_error.rs` holds that split; the two cases are pinned
//! separately below.
//!
//! Needs `write-support` for the `SSTableWriter` that synthesizes the fixture
//! (the corpus has no deliberately-corrupt SSTable). The default feature set
//! includes it, so the gate's `core-tests` component executes this target.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::{ScanRow, Value};
use cqlite_core::{Config, Error, Platform};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// A distinctive 4-byte `int` set member: its big-endian bytes appear exactly
/// once in the written `Data.db`, so the byte patch below can locate the element
/// unambiguously (asserted, not assumed).
const MEMBER: i32 = 0x7B5C_3A19;

/// A distinctive `set<text>` member used by the TOLERANCE control.
const TEXT_MEMBER: &str = "zqxjw";

fn schema(set_type: &str) -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "tags".to_string(),
                data_type: set_type.to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn set_mutation(set_value: Value) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: set_value,
        }],
        1_000_000,
        None,
    )
}

/// Write a single-partition, single-row uncompressed SSTable whose `tags` column
/// is a MULTICELL set (each member stored in its own cell PATH — the layout
/// `write_set_complex_cells` emits and `complex_column.rs` decodes).
async fn write_fixture(temp: &TempDir, set_type: &str, set_value: Value) -> std::path::PathBuf {
    let schema = schema(set_type);
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();
    let m = set_mutation(set_value);
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();
    info.data_path
}

async fn read_rows(data_path: &std::path::Path) -> cqlite_core::Result<Vec<ScanRow>> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let reader = SSTableReader::open(data_path, &config, platform).await?;
    Ok(reader
        .iterate_all_partitions()
        .await?
        .into_iter()
        .map(|(_k, row)| row)
        .collect())
}

/// The cell-path framing the writer emits for ONE `int` set member:
/// `[path_len unsigned-VInt = 4][4 BE bytes]` (`write_set_complex_cells`:
/// `encode_unsigned(path_bytes.len())` then the serialized element).
fn int_member_frame() -> Vec<u8> {
    let mut v = vec![0x04];
    v.extend_from_slice(&MEMBER.to_be_bytes());
    v
}

/// Offset of the UNIQUE occurrence of `needle` in `hay`.
///
/// Uniqueness is ASSERTED: a patch applied to the wrong occurrence would test
/// nothing, and `Data.db` also carries the partition key and header bytes.
fn unique_offset(hay: &[u8], needle: &[u8], what: &str) -> usize {
    let hits: Vec<usize> = hay
        .windows(needle.len())
        .enumerate()
        .filter(|(_, w)| *w == needle)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "expected exactly ONE occurrence of the {what} byte frame in Data.db, found {} — \
         the byte patch below would be ambiguous",
        hits.len()
    );
    hits[0]
}

/// Recompute the uncompressed `CRC.db` chunk checksums after a byte patch.
///
/// The reader verifies every uncompressed chunk against `CRC.db`
/// (`block_io::verify_uncompressed_chunks`, `crc32fast::hash` per chunk), so a
/// raw byte patch would otherwise be refused as a CHECKSUM failure and this file
/// would prove nothing about element widths. Refreshing the sidecar puts the
/// fixture in the state that matters: a file that is internally consistent
/// everywhere EXCEPT the property under test — which is also the state a real
/// Cassandra-written file with a wrong declared element width would be in.
///
/// Layout mirrored from `reader/crc.rs`: `[chunk_size i32 BE]` then one
/// `[crc32 u32 BE]` per chunk.
fn refresh_crc_db(data_path: &std::path::Path) {
    let data = std::fs::read(data_path).unwrap();
    let crc_path = data_path.with_file_name(
        data_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .replace("-Data.db", "-CRC.db"),
    );
    let old = std::fs::read(&crc_path).unwrap();
    assert!(old.len() >= 4, "CRC.db must carry its chunk-size header");
    let chunk_size = i32::from_be_bytes([old[0], old[1], old[2], old[3]]);
    assert!(chunk_size > 0, "CRC.db chunk size must be positive");
    let mut out = old[..4].to_vec();
    for chunk in data.chunks(chunk_size as usize) {
        out.extend_from_slice(&crc32fast::hash(chunk).to_be_bytes());
    }
    assert_eq!(
        out.len(),
        old.len(),
        "an in-place patch must not change the chunk COUNT — otherwise the fixture \
         differs from the written one in more than the property under test"
    );
    std::fs::write(&crc_path, out).unwrap();
}

/// Rewrite the `int` set member's declared cell-path LENGTH in place.
///
/// Only the length byte changes, so no downstream offset (and no `row_size`)
/// shifts: the row framing stays exactly as written and the ONLY thing wrong
/// with the file is the element's declared width — which is the property under
/// test.
fn patch_declared_length(data_path: &std::path::Path, declared: u8) {
    let mut bytes = std::fs::read(data_path).unwrap();
    let frame = int_member_frame();
    let at = unique_offset(&bytes, &frame, "int set member");
    assert_eq!(bytes[at], 0x04, "precondition: writer emitted path_len = 4");
    bytes[at] = declared;
    std::fs::write(data_path, bytes).unwrap();
    refresh_crc_db(data_path);
}

fn expect_width_mismatch(result: cqlite_core::Result<Vec<ScanRow>>, declared: usize, case: &str) {
    match result {
        Err(Error::FixedWidthLengthMismatch {
            ref cql_type,
            expected,
            actual,
            ..
        }) => {
            assert_eq!(expected, 4, "{case}: `int` admits exactly 4 bytes");
            assert_eq!(
                actual, declared,
                "{case}: the error must report the DECLARED on-disk length"
            );
            assert!(
                cql_type.contains("int"),
                "{case}: the error must name the offending CQL type, got {cql_type:?}"
            );
        }
        Err(other) => panic!(
            "{case}: a wrong declared width must surface as the NAMED \
             FixedWidthLengthMismatch, got a different Err: {other:?}"
        ),
        Ok(rows) => panic!(
            "{case}: a fixed-width set member with a declared length Cassandra REFUSES \
             (Int32Serializer.validate throws) must surface as an Err at the read path — \
             a silently partial row/collection is the #3723 defect. Got rows: {rows:?}"
        ),
    }
}

/// The `tags` cells of the single row, or `None` when the column is absent.
fn tags_of(rows: &[ScanRow]) -> Option<Value> {
    assert_eq!(rows.len(), 1, "fixture writes exactly one row: {rows:?}");
    let cells = rows[0].clone().into_cells()?;
    cells
        .into_iter()
        .find(|(name, _)| &**name == "tags")
        .map(|(_, v)| v)
}

// ---------------------------------------------------------------------------
// 1. NEGATIVE CONTROL — the unpatched fixture reads correctly.
// ---------------------------------------------------------------------------

/// Anti-empty-pass control: the SAME fixture, unpatched, reads the multicell set
/// back intact. Without this, a harness that errored (or returned no rows) for
/// an unrelated reason would make every refusal case below pass vacuously.
#[tokio::test]
async fn well_formed_multicell_set_reads_back_intact() {
    let temp = TempDir::new().unwrap();
    let path = write_fixture(&temp, "set<int>", Value::Set(vec![Value::Integer(MEMBER)])).await;

    let rows = read_rows(&path).await.expect("control fixture must read");
    assert_eq!(
        tags_of(&rows),
        Some(Value::Set(vec![Value::Integer(MEMBER)])),
        "control: a well-formed multicell set<int> must read back intact"
    );
}

// ---------------------------------------------------------------------------
// 2. The refusal is OBSERVABLE at the read path (blockers 1 + 2a + 2b).
// ---------------------------------------------------------------------------

/// OVERLONG (5 declared bytes for an `int`).
///
/// Pre-fix this was the worst shape: the guard fired, `complex_column.rs` mapped
/// it to `None`, and the row surfaced with `tags` MISSING its only member — an
/// apparently-successful read of corrupt data.
#[tokio::test]
async fn overlong_fixed_width_set_member_is_refused_at_the_read_path() {
    let temp = TempDir::new().unwrap();
    let path = write_fixture(&temp, "set<int>", Value::Set(vec![Value::Integer(MEMBER)])).await;
    patch_declared_length(&path, 5);
    expect_width_mismatch(read_rows(&path).await, 5, "declared 5 bytes");
}

/// SHORT (3 declared bytes for an `int`) — `Expected 4 or 0 byte int (3)`.
#[tokio::test]
async fn short_fixed_width_set_member_is_refused_at_the_read_path() {
    let temp = TempDir::new().unwrap();
    let path = write_fixture(&temp, "set<int>", Value::Set(vec![Value::Integer(MEMBER)])).await;
    patch_declared_length(&path, 3);
    expect_width_mismatch(read_rows(&path).await, 3, "declared 3 bytes");
}

/// ZERO-LENGTH (blocker 2b): the `else if !cell.path_bytes.is_empty()` guard
/// meant an empty cell path BYPASSED the decoder entirely, so the member was
/// dropped without ever reaching the width guard — the exact opposite of this
/// branch's AC2. The empty path is now decoded like any other, so the refusal
/// happens and is NAMED.
///
/// Its DISPOSITION, though, is the pre-#3723 one: TOLERATED. On `origin/main`
/// this same fixture read back as a row whose `tags` was an EMPTY set (the
/// member silently dropped by the `is_empty()` bypass), and #3723 does not
/// change the behaviour of a path that already failed before it — only a WRONG
/// declared width, a class this branch introduced, is fatal. So the row must
/// still come back, with the member omitted.
#[tokio::test]
async fn zero_length_fixed_width_set_member_is_tolerated_at_the_read_path() {
    let temp = TempDir::new().unwrap();
    let path = write_fixture(&temp, "set<int>", Value::Set(vec![Value::Integer(MEMBER)])).await;
    patch_declared_length(&path, 0);

    let rows = read_rows(&path).await.expect(
        "a ZERO-length fixed-width set member must keep its pre-#3723 tolerated \
         disposition: the row is returned, not an Err (only a WRONG width is fatal)",
    );
    assert_eq!(
        tags_of(&rows),
        Some(Value::Set(vec![])),
        "the zero-length member is omitted and the row survives — exactly what \
         origin/main returned for these bytes"
    );
}

// ---------------------------------------------------------------------------
// 3. NEGATIVE CONTROL — the fix did NOT blanket-propagate.
// ---------------------------------------------------------------------------

/// A pre-#3723 TOLERATED decode failure must still be tolerated at the read
/// path: the row is returned, not an `Err`.
///
/// The subject is an invalid-UTF-8 `set<text>` member. The text arm returns
/// `Error::corruption` (via `std::str::from_utf8`), which is NOT in the fatal
/// set (`raw_value::fatal_decode_error` names exactly one variant), so the
/// pre-existing behaviour — omit the member, keep the row — must survive.
/// A blanket `?` conversion in either call site turns this test RED, which is
/// precisely what it is here to catch.
#[tokio::test]
async fn tolerated_decode_failure_still_yields_a_row_not_an_error() {
    let temp = TempDir::new().unwrap();
    let path = write_fixture(
        &temp,
        "set<text>",
        Value::Set(vec![Value::text(TEXT_MEMBER.to_string())]),
    )
    .await;

    // Patch the member's BYTES (not its length) to a lone continuation byte,
    // which `std::str::from_utf8` rejects. The declared length stays 5, so no
    // fixed-width guard is involved and no offset moves.
    let mut bytes = std::fs::read(&path).unwrap();
    let mut frame = vec![TEXT_MEMBER.len() as u8];
    frame.extend_from_slice(TEXT_MEMBER.as_bytes());
    let at = unique_offset(&bytes, &frame, "text set member");
    bytes[at + 1] = 0x80;
    std::fs::write(&path, bytes).unwrap();
    refresh_crc_db(&path);

    let rows = read_rows(&path)
        .await
        .expect("a TOLERATED decode failure must NOT become a read error (#3723 fixed only the width-mismatch class)");
    assert_eq!(
        tags_of(&rows),
        Some(Value::Set(vec![])),
        "the undecodable text member is omitted and the row survives — the exact \
         pre-#3723 tolerant behaviour"
    );
}

// ---------------------------------------------------------------------------
// 4. CHARACTERISATION — the FIFTH tolerant site, DECLARED not fixed (#3778).
// ---------------------------------------------------------------------------

/// The surviving control column of the frozen fixture. `note` sorts BEFORE
/// `tags`, so it is decoded first and must still be present — which is what
/// distinguishes "one column silently absent" from "the whole row failed".
const NOTE: &str = "kept";

/// Two REGULAR columns on purpose: `note text` (decoded first) and
/// `tags frozen<list<int>>` (the subject). With only the subject column the row
/// degenerates to a `ScanRow::Marker` — true, but it would not show WHICH fact
/// is being characterised.
fn frozen_schema() -> TableSchema {
    let mut s = schema("frozen<list<int>>");
    s.columns.insert(
        1,
        Column {
            name: "note".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        },
    );
    s
}

async fn write_frozen_fixture(temp: &TempDir) -> std::path::PathBuf {
    let schema = frozen_schema();
    let m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::Write {
                column: "note".to_string(),
                value: Value::text(NOTE.to_string()),
            },
            CellOperation::Write {
                column: "tags".to_string(),
                value: Value::Frozen(Box::new(Value::List(vec![Value::Integer(MEMBER)]))),
            },
        ],
        1_000_000,
        None,
    );
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    writer.finish().await.unwrap().data_path
}

/// Rewrite the frozen element's declared i32 LENGTH in place, low byte only.
///
/// A SHORT declared length (3) is used rather than an overlong one: an overlong
/// length trips `read_frozen_element`'s `*offset + len > blob_end` bound first
/// and yields `Error::Corruption`, a DIFFERENT (always-tolerated) class.
/// Declaring 3 hands `parse_value_from_raw_bytes` a 3-byte slice, so the `int`
/// arm produces exactly the `FixedWidthLengthMismatch` this branch made fatal at
/// the other four sites — which is the property under test.
fn patch_frozen_element_length(data_path: &std::path::Path, declared: u8) {
    let mut bytes = std::fs::read(data_path).unwrap();
    // The element frame `frozen.rs::read_frozen_element` reads back:
    // `[i32 BE elem_len = 4][4 BE bytes]`.
    let mut frame = vec![0x00, 0x00, 0x00, 0x04];
    frame.extend_from_slice(&MEMBER.to_be_bytes());
    let at = unique_offset(&bytes, &frame, "frozen list int element");
    assert_eq!(
        bytes[at + 3],
        0x04,
        "precondition: writer emitted the element length as i32 BE 4"
    );
    bytes[at + 3] = declared;
    std::fs::write(data_path, bytes).unwrap();
    refresh_crc_db(data_path);
}

/// Cell names of the single row, or `None` when it carries no cells at all.
fn cell_names(rows: &[ScanRow]) -> Option<Vec<String>> {
    assert_eq!(rows.len(), 1, "fixture writes exactly one row: {rows:?}");
    Some(
        rows[0]
            .clone()
            .into_cells()?
            .into_iter()
            .map(|(n, _)| n.to_string())
            .collect(),
    )
}

/// CONTROL for the characterisation below: the SAME two-column frozen fixture,
/// unpatched, reads both columns back intact. Without it the case below could
/// pass because the frozen column never decoded at all, in which case it would
/// characterise nothing.
#[tokio::test]
async fn well_formed_frozen_list_reads_back_intact() {
    let temp = TempDir::new().unwrap();
    let path = write_frozen_fixture(&temp).await;

    let rows = read_rows(&path).await.expect("control fixture must read");
    assert_eq!(
        tags_of(&rows),
        Some(Value::Frozen(Box::new(Value::List(vec![Value::Integer(
            MEMBER
        )])))),
        "control: a well-formed frozen<list<int>> must read back intact"
    );
    let names = cell_names(&rows).expect("control: a DATA row");
    assert!(
        names.iter().any(|n| n == "note"),
        "control: the surviving column must be present in the well-formed read too: {names:?}"
    );
}

/// CHARACTERISATION of a KNOWN-TOLERATED GAP — **this is not desired
/// behaviour**, and the assertions below are NOT a guard.
///
/// A frozen collection/tuple is a SIMPLE (non-multicell) cell, so its decode
/// runs `cell_value_complex.rs:62` -> `frozen.rs:123` `?` and lands at the
/// SIMPLE-cell arm `row_data.rs:777` `Err(e) => break`. That is the FIFTH
/// tolerant site, and it deliberately has NO `is_fatal_decode_error` arm (unlike
/// `row_data.rs:614`, `raw_value/set_member.rs:104`, `block_emit.rs:271` and
/// `block_emit_windowed.rs:592`). Consequence: a wrong-width element inside a
/// frozen column yields an apparently-successful row with that column SILENTLY
/// ABSENT — the same unobservable-refusal shape section 2 above closes for
/// multicell sets, and one of AC1's five nesting positions.
///
/// It is NOT fixed here, on a standing lead ruling: adding a sixth, seventh, ...
/// guard arm does not converge, and the class closes as a whole in **#3778**
/// (nested consumption / refusal propagation). The five-site census lives in
/// `raw_value/fatal_decode_error.rs`.
///
/// So this test PINS TODAY'S DISPOSITION, and fails in BOTH directions:
///
/// * if the read starts returning `Err` (someone silently made the site fatal —
///   a real behaviour change, which must be a deliberate #3778 commit updating
///   this test and the census together, not a side effect);
/// * if `tags` starts coming back with a value (someone made the decode tolerant
///   a different way, e.g. an opaque `Blob` fallback, which would additionally
///   be a no-heuristics violation — issue #28).
#[tokio::test]
async fn wrong_width_in_a_frozen_column_is_tolerated_today_known_gap_3778() {
    let temp = TempDir::new().unwrap();
    let path = write_frozen_fixture(&temp).await;
    patch_frozen_element_length(&path, 3);

    let rows = read_rows(&path).await.expect(
        "KNOWN-TOLERATED GAP (#3778): the simple-cell arm row_data.rs:777 has no \
         is_fatal_decode_error arm, so today a wrong-width frozen element does NOT \
         fail the read. If this now Errs the disposition changed — update the \
         five-site census in raw_value/fatal_decode_error.rs and this test together",
    );
    // Asserted on the cell NAMES rather than through `tags_of`, which maps a
    // `ScanRow::Marker` (no cells at all) to `None` too: that `None` would not
    // distinguish "the column is absent" from "this is not a data row", and
    // characterising the wrong fact is worse than characterising nothing.
    let names = cell_names(&rows).expect(
        "KNOWN-TOLERATED GAP (#3778): a DATA row must still be emitted, carrying the \
         column decoded before the frozen one",
    );
    assert!(
        names.iter().any(|n| n == "note"),
        "the column decoded BEFORE the frozen one must survive — otherwise this \
         characterises a whole-row failure, not a silently-absent column: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "tags"),
        "KNOWN-TOLERATED GAP (#3778): the frozen column is SILENTLY ABSENT from the \
         row. Not desired behaviour — characterised so a change of disposition in \
         EITHER direction is visible: {names:?}"
    );
}
