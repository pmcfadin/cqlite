//! Issue #3809 (found reviewing #3721) — a cell decoder must not read past its own
//! row, and a row's columns must TILE its body exactly.
//!
//! # The defect these cases pin
//!
//! Row assembly bounded column decoding by `data.len()` — the length of the whole
//! materialised parse unit, which holds every FOLLOWING row and partition — rather
//! than by the current row's authoritative end (`(row_metadata_offset +
//! row_size_vint_len) + row_size`, Cassandra's `getFilePointer()` arithmetic, issue
//! #237). Two consequences, and both are SILENT WRONG ANSWERS rather than missing
//! checks, which is what makes them worse than the swallow #3721 removed:
//!
//! * **over-consumption.** A cell whose length prefix (or fixed width) runs past
//!   the row boundary still had bytes to read — the next row's — so it decoded
//!   SUCCESSFULLY and returned a value built from another row's data.
//! * **under-consumption.** A cell that consumed FEWER bytes than the row body
//!   holds left a remainder no column accounted for, i.e. a cursor that was
//!   misaligned inside the body, and every value decoded from it is suspect.
//!
//! In BOTH directions the row loop's framing stays correct — `next_offset` derives
//! from the authoritative `row_size`, never from where the columns stopped — so the
//! walk does not desynchronise, the following rows read normally, and nothing
//! downstream looks wrong. The read reports SUCCESS with one plausible garbage
//! value in it, which is strictly worse than a failure: a wrong answer is
//! indistinguishable from a right one.
//!
//! # Measured on this file's own fixture, before the fix (branch @ 6c88ecb91)
//!
//! Each row is `SELECT *`'s own output for the SAME one-byte edit these cases make:
//!
//! ```text
//! +6 on the length vint of row 1's `body` -> Ok, 600 rows; ck=1's `body` ends
//!                                            `…babababab$\0\0\0\0\x02`, i.e. SIX
//!                                            bytes of the NEXT row's framing
//!                                            (flags 0x24 = `$`, clustering header,
//!                                            ck=2) appended to its text
//! -6 on the same vint                     -> Ok, 600 rows; ck=1's `body` silently
//!                                            SIX characters short
//! unpatched                               -> Ok, 600 rows, every value as written
//! ```
//!
//! In both patched runs the row COUNT is right, the other 599 rows are right, and no
//! error is raised anywhere — the corruption surfaces only as six characters of one
//! value. After the fix both are an `Error::ColumnDecode` naming `body`. Every
//! assertion below is at the PUBLIC surface (`Database::execute`), because the whole
//! defect is that a read of a corrupt row looks exactly like a read of a sound one.
//!
//! # Why the corruption is a ONE-BYTE edit of a REAL Cassandra fixture
//!
//! The bytes being framed must come from Cassandra, not from CQLite's own writer: a
//! CQLite-written, CQLite-read round trip is invariant to a uniform framing error
//! on both sides (#3042). So the fixture is the committed, UNCOMPRESSED
//! `test_comp.uncompressed_table` (`pk int, ck int, body text` — ONE partition, 600
//! clustering rows, so "the next row's bytes" is literal), and the only edit is the
//! low byte of the FIRST row's `body` cell length vint. Cassandra's own writer
//! produced everything else, including the `row_size` the fix measures against.

use std::path::{Path, PathBuf};

use cqlite_core::error::ErrorCategory;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, Error};

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

const KEYSPACE: &str = "test_comp";
const TABLE: &str = "uncompressed_table";
const DIR: &str = "uncompressed_table-25a5ca7071a911f19b3225f9984c6a77";
const DATA_DB: &str = "nb-1-big-Data.db";

/// The fixture's untruncated size, pinned so a corpus regeneration cannot silently
/// move the patch below onto some other byte.
const FULL_LEN: usize = 195_018;
/// Rows the untruncated fixture holds (one partition, `ck` 1..=600).
const FULL_ROWS: usize = 600;

/// Byte offset of the LOW byte of the first row's `body` cell length vint, and the
/// value Cassandra wrote there. DERIVED from the fixture, not guessed:
///
/// ```text
/// 0x00..0x11  partition header (2-byte key length, 4-byte pk, 4-byte localDeletionTime,
///             8-byte markedForDeleteAt) = 18 bytes
/// 0x12        row flags 0x24 = HAS_ALL_COLUMNS | HAS_TIMESTAMP
/// 0x13        ClusteringPrefix header byte
/// 0x14..0x17  ck = 1 (Int32Type is fixed width: no length prefix)
/// 0x18..0x19  row_size vint 0x81 0x3c = 316, counted from 0x1a
/// 0x1a        prev unfiltered size vint = 18
/// 0x1b        liveness timestamp delta vint = 0
/// 0x1c        cell flags 0x08 = USE_ROW_TIMESTAMP
/// 0x1d..0x1e  cell value length vint 0x81 0x37 = 311   <-- PATCHED HERE
/// 0x1f..      311 bytes of text, ending at 0x1f + 311 = 342
/// ```
///
/// `0x1a + 316 = 342` — the cell ends EXACTLY at the row's authoritative end, which
/// is what makes a `±16` edit of this one byte an exact over/under-consumption of 16
/// bytes with every other field left as Cassandra wrote it.
const BODY_LEN_VINT_LOW_BYTE: usize = 0x1e;
const BODY_LEN_VINT_LOW_VALUE: u8 = 0x37;
/// The row's authoritative end, and the offset the patched cell decode reaches from.
const ROW_BODY_END: usize = 342;
/// Byte offset the `body` cell's own decode starts at (its flags byte).
const BODY_CELL_START: usize = 0x1c;
/// Bytes the patch moves the cell's end by, in EITHER direction. The value is
/// bounded above by what the over-consumption case needs, and the bound is MEASURED,
/// not chosen for neatness: the bytes immediately after the row are the next row's
/// framing —
///
/// ```text
/// 342: 0x24 flags | 343: 0x00 clustering header | 344..347: 0x00 0x00 0x00 0x02 (ck=2)
/// 348: 0x81 — the high half of that row's row_size vint
/// ```
///
/// — and `0x81` is not a legal UTF-8 leading byte, so a skew of 7 or more is caught
/// by the text decoder's pre-existing UTF-8 check (measured at `+16`: `Cell 'body':
/// invalid UTF-8 … from index 317`) and the case would then PASS WITHOUT EXERCISING
/// the row bound at all. At `+6` every over-read byte is valid UTF-8, so the cell
/// decodes SUCCESSFULLY and the row-bound refusal is the only thing that can catch
/// it — which is the whole point. It also keeps the length vint 2 bytes wide, so the
/// edit changes ONE byte and nothing shifts.
const SKEW: u8 = 6;

const SCHEMA: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_comp WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_comp;
CREATE TABLE IF NOT EXISTS test_comp.uncompressed_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
);
";

/// Resolve the fixture root, or FAIL — never skip (issue #3220). The fixture is
/// committed to git, so absence is a resolution defect and not a legitimate state.
fn fixture_root() -> PathBuf {
    sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "{KEYSPACE}.{TABLE} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (issue #3220) — {}.\n  remedy: git restore --source=HEAD -- \
             test-data/datasets/sstables",
            describe_search(KEYSPACE, TABLE)
        )
    })
}

/// Copy the fixture into a scratch tree, replacing zero or more bytes of `Data.db`,
/// and omitting the checksum components (a byte edit invalidates them, and the
/// controls below prove their absence is not what fails a read).
///
/// Each patch is `(offset, expected_current_byte, new_byte)` and the CURRENT byte is
/// ASSERTED before it is replaced, so a corpus regeneration that moves any of these
/// fields fails loudly here instead of silently patching some other field and
/// selecting a different condition.
fn patched_copy(patches: &[(usize, u8, u8)]) -> (tempfile::TempDir, PathBuf) {
    let src = fixture_root().join(KEYSPACE).join(DIR);
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("sstables");
    let dst = root.join(KEYSPACE).join(DIR);
    std::fs::create_dir_all(&dst).expect("scratch dir");
    for entry in std::fs::read_dir(&src).expect("read committed fixture dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains("CRC") || name.contains("Digest") {
            continue;
        }
        let mut bytes = std::fs::read(entry.path()).expect("read component");
        if name == DATA_DB {
            assert_eq!(
                bytes.len(),
                FULL_LEN,
                "{KEYSPACE}.{TABLE}'s Data.db is {} bytes, not the {FULL_LEN} these cases \
                 pin — the fixture was regenerated and the byte offsets below no longer \
                 select the condition under test",
                bytes.len()
            );
            for &(at, expect, to) in patches {
                assert_eq!(
                    bytes[at], expect,
                    "byte {at:#x} must be the {expect:#x} Cassandra wrote there; found {:#x} \
                     — the fixture changed and this patch no longer edits the field it names",
                    bytes[at]
                );
                bytes[at] = to;
            }
        }
        std::fs::write(dst.join(&name), &bytes).expect("write component");
    }
    (tmp, root)
}

fn write_schema(dir: &Path) -> PathBuf {
    let path = dir.join("schema.cql");
    std::fs::write(&path, SCHEMA).expect("write scratch schema");
    path
}

async fn open_db(root: &Path, schema: &Path) -> Database {
    open_db_filtered(root, schema, KEYSPACE).await
}

/// The same ingestion, for a scratch tree holding a DIFFERENT keyspace — the
/// Cassandra-written row-tombstone control reads `test_tomb`.
async fn open_db_filtered(root: &Path, schema: &Path, keyspace: &str) -> Database {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

/// `SELECT *` over a scratch copy — the PUBLIC read path. Returns the `body` value
/// of every row keyed by `ck`, so a case can assert on the VALUE and not merely on
/// the row count (a wrong value with the right count is the defect).
async fn select_bodies(patches: &[(usize, u8, u8)]) -> Result<Vec<(i32, String)>, Error> {
    let (tmp, root) = patched_copy(patches);
    let schema = write_schema(tmp.path());
    let db = open_db(&root, &schema).await;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await?;
    let mut out: Vec<(i32, String)> = result
        .rows
        .iter()
        .map(|row| {
            let mut ck = None;
            let mut body = None;
            for (name, value) in row.values.iter() {
                match name.as_ref() {
                    "ck" => ck = Some(format!("{value:?}")),
                    "body" => body = Some(format!("{value:?}")),
                    _ => {}
                }
            }
            let ck = ck.expect("every row must carry its clustering key");
            let ck: i32 = ck
                .trim_start_matches(|c: char| !c.is_ascii_digit() && c != '-')
                .trim_end_matches(|c: char| !c.is_ascii_digit())
                .parse()
                .unwrap_or_else(|e| panic!("clustering key must be an int, got {ck}: {e}"));
            (ck, body.unwrap_or_default())
        })
        .collect();
    out.sort();
    Ok(out)
}

/// The failure must be the dedicated, MATCHABLE per-column variant naming the
/// column — the variant the row/partition loops match on so this is NOT folded into
/// "end of partition" (issue #3721) — and it must SAY WHAT IT MEASURED. A bare
/// "corrupt SSTable" would leave an operator no better off than a log line.
fn assert_column_decode_at(err: &Error, column: &str, at: usize, needles: &[&str]) {
    let Error::ColumnDecode {
        column: named,
        offset,
        source,
        ..
    } = err
    else {
        panic!("expected Error::ColumnDecode for column '{column}', got: {err:?}");
    };
    assert_eq!(named, column, "the error must NAME the column");
    assert_eq!(
        *offset, at,
        "the failure must be reported at the byte the refused decode was measured from"
    );
    let cause = source.to_string();
    for needle in needles {
        assert!(
            cause.contains(needle),
            "the surfaced cause must name `{needle}` — otherwise this is some other \
             failure aborting the same read; got: {cause}"
        );
    }
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
}

/// OVER-consumption. A cell whose length vint reaches past its own row read the NEXT
/// row's bytes and returned them as this row's value. It must FAIL the read.
///
/// Pre-fix this returned `Ok` with all 600 rows and `ck=1`'s `body` carrying 16 bytes
/// of the following row's framing appended to its text — a wrong answer that no
/// consumer can tell from a right one.
///
/// WHICH LAYER REFUSES CHANGED, AND THIS TEST FOLLOWED IT (#3721, roborev job 75).
/// The decoders are now handed `&data[..after_row_offset]` — THIS ROW's bytes — rather
/// than the whole parse unit, so a cell reaching past its row is refused by the length
/// check AT the row boundary and can no longer read or ALLOCATE from later rows at all.
/// `row_bound_check`'s "PAST the end of its own row body" is therefore no longer what
/// fires here; it is retained as a BACKSTOP for the case bounding cannot reach — a
/// decoder that over-ADVANCES its returned offset without needing bytes past the row —
/// and its message is asserted below to be ABSENT, so a future change that silently
/// re-widens the decoder's input reds this test instead of quietly passing it.
///
/// The assertion is the ARITHMETIC, not the offsets: the cell must ask for exactly
/// `SKEW` more bytes than the row has left. That is what makes the visible bound
/// provably the ROW's extent — the whole parse unit is 195_018 bytes and would have
/// satisfied this read, which is precisely the pre-fix wrong answer.
#[tokio::test]
async fn a_cell_reaching_past_its_row_fails_the_select_instead_of_returning_the_next_rows_bytes() {
    let err = select_bodies(&[(
        BODY_LEN_VINT_LOW_BYTE,
        BODY_LEN_VINT_LOW_VALUE,
        BODY_LEN_VINT_LOW_VALUE + SKEW,
    )])
    .await
    .expect_err(
        "a cell that consumes bytes past its own row's authoritative end has read another \
         row's data, so the value it produced is not this row's — the read must fail, not \
         return it",
    );
    let Error::ColumnDecode {
        column: named,
        offset,
        source,
        ..
    } = &err
    else {
        panic!("expected Error::ColumnDecode for column 'body', got: {err:?}");
    };
    assert_eq!(named, "body", "the error must NAME the column");
    assert_eq!(
        *offset, BODY_CELL_START,
        "the failure must be reported at the byte the refused decode was measured from"
    );
    let cause = source.to_string();
    // `Cell '<col>': need <need> bytes for <type>, only <available> available`
    let nums: Vec<usize> = cause
        .split(|c: char| !c.is_ascii_digit())
        .filter(|t| !t.is_empty())
        .map(|t| t.parse().unwrap_or_default())
        .collect();
    assert!(
        cause.contains("need") && cause.contains("available") && nums.len() >= 2,
        "the cause must report the shortfall it MEASURED (need N .. only M available), \
         otherwise this is some other failure aborting the same read; got: {cause}"
    );
    let (need, available) = (nums[0], nums[1]);
    assert_eq!(
        need - available,
        SKEW as usize,
        "the cell must come up short by exactly the {SKEW} bytes it was skewed by, \
         measured against THIS ROW's remaining extent — need={need} available={available} \
         in: {cause}"
    );
    assert!(
        available < FULL_LEN,
        "the bound the decoder saw must be the ROW's, not the parse unit's ({FULL_LEN} \
         bytes, which would have satisfied this read — the pre-fix wrong answer); got \
         available={available}"
    );
    assert!(
        !cause.contains("PAST the end of its own row body"),
        "bounding the decoder's input must make the over-read UNREACHABLE, not merely \
         detected after the fact — if the backstop fired, the decoder was handed bytes \
         beyond this row again; got: {cause}"
    );
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
}

/// UNDER-consumption. The converse: the cell stopped SHORT, leaving bytes inside the
/// row body that no column accounted for. Also a silent wrong answer pre-fix (the
/// value was quietly truncated and the walk continued normally, because `next_offset`
/// comes from `row_size`), so it must fail too.
#[tokio::test]
async fn a_row_body_left_partly_unconsumed_fails_the_select_instead_of_returning_a_short_value() {
    let err = select_bodies(&[(
        BODY_LEN_VINT_LOW_BYTE,
        BODY_LEN_VINT_LOW_VALUE,
        BODY_LEN_VINT_LOW_VALUE - SKEW,
    )])
    .await
    .expect_err(
        "bytes inside a row body accounted for by no column mean the cursor was misaligned \
         somewhere in the row, so every value decoded from it is suspect — the read must \
         fail, not return them",
    );
    assert_column_decode_at(
        &err,
        "body",
        ROW_BODY_END - SKEW as usize,
        &[
            "row body under-consumed",
            &format!("{ROW_BODY_END}"),
            &format!("{SKEW} byte(s)"),
            "no column",
        ],
    );
}

// ─── The row whose DECLARED SIZE ends inside its own METADATA (#3721, roborev job 13)
//
// The two cases above both reach the row-extent reconciliation through a COLUMN's
// decoder. The shape below reaches it with NO column decoded at all, which is the
// state no per-column bound check can see — and which an earlier revision of the
// reconciliation asserted away with a `debug_assert!`, i.e. PANICKED on in any debug
// build. The patch turns Cassandra's own first row into that shape with three
// one-byte edits, each of which changes a field IN PLACE (no byte shifts):
//
//   0x12  row flags   0x24 -> 0x04   clear HAS_ALL_COLUMNS, keep HAS_TIMESTAMP, so a
//                                    missing-columns bitmap VInt is now expected
//   0x1c  bitmap      0x08 -> 0x01   bit 0 SET = on-disk column 0 (`body`) is ABSENT;
//                                    this byte was the `body` cell's flags byte, which
//                                    the bitmap now consumes (header_size 4 -> 5)
//   0x18  row_size    0x81 -> 0x80 } row_size 316 -> 0, still a TWO-byte vint, so the
//   0x19  row_size    0x3c -> 0x00 } row body ends at 0x1a + 0 = 26 — BEFORE the
//                                    metadata's own end at 0x18 + 5 = 29
//
// A table with NO data columns reaches the identical state (`columns_in_order` empty,
// so the loop body never runs); no committed fixture has such a table — verified over
// every `CREATE TABLE` in `test-data/schemas/**` — so the all-columns-absent bitmap is
// how the state is constructed here.

/// The first row's flags byte, and the `HAS_ALL_COLUMNS | HAS_TIMESTAMP` Cassandra
/// wrote there.
const ROW_FLAGS_BYTE: usize = 0x12;
const ROW_FLAGS_VALUE: u8 = 0x24;
/// `HAS_TIMESTAMP` alone: `HAS_ALL_COLUMNS` (0x20) cleared, so the row header carries
/// a missing-columns bitmap (`Columns.serializer` subset encoding).
const ROW_FLAGS_NO_ALL_COLUMNS: u8 = 0x04;
/// The byte the cleared flag turns into that bitmap — Cassandra wrote the `body`
/// cell's flags byte (0x08) here.
const BITMAP_BYTE: usize = 0x1c;
const BITMAP_OLD_VALUE: u8 = 0x08;
/// Bit 0 set = on-disk column 0 (`body`, the table's only data column) is MISSING, so
/// the row declares NO column present and no column's decoder runs.
const BITMAP_ALL_ABSENT: u8 = 0x01;
/// The two bytes of the row_size vint, and the 316 Cassandra encoded in them.
const ROW_SIZE_VINT_HIGH_BYTE: usize = 0x18;
const ROW_SIZE_VINT_HIGH_VALUE: u8 = 0x81;
const ROW_SIZE_VINT_LOW_BYTE: usize = 0x19;
const ROW_SIZE_VINT_LOW_VALUE: u8 = 0x3c;
/// `row_size = 0` in the SAME two-byte encoding (leading `10` = one extra byte, all
/// value bits zero), so nothing shifts.
const ROW_SIZE_ZERO_HIGH: u8 = 0x80;
const ROW_SIZE_ZERO_LOW: u8 = 0x00;
/// Where the row's metadata ENDS, with the bitmap byte the cleared `HAS_ALL_COLUMNS`
/// flag adds included: `0x18 + header_size(5)`. With no column decoded this is also
/// where the cursor stops, since only a column's decoder advances it.
const ROW_METADATA_END: usize = 0x1d;
/// The row body's end with `row_size = 0`: counted from AFTER the row_size vint
/// (issue #237), i.e. `0x1a + 0` — three bytes BEFORE the metadata ends.
const ROW_END_SIZE_ZERO: usize = 0x1a;

/// Every edit that makes the row declare no present column.
const ALL_COLUMNS_ABSENT: [(usize, u8, u8); 2] = [
    (ROW_FLAGS_BYTE, ROW_FLAGS_VALUE, ROW_FLAGS_NO_ALL_COLUMNS),
    (BITMAP_BYTE, BITMAP_OLD_VALUE, BITMAP_ALL_ABSENT),
];
/// The two further edits that shrink the declared row body to zero bytes.
const ROW_SIZE_ZERO: [(usize, u8, u8); 2] = [
    (
        ROW_SIZE_VINT_HIGH_BYTE,
        ROW_SIZE_VINT_HIGH_VALUE,
        ROW_SIZE_ZERO_HIGH,
    ),
    (
        ROW_SIZE_VINT_LOW_BYTE,
        ROW_SIZE_VINT_LOW_VALUE,
        ROW_SIZE_ZERO_LOW,
    ),
];

/// A row whose declared `row_size` ends INSIDE its own metadata, with every column
/// absent, must FAIL the read with a NAMED error — never PANIC (#3721, roborev job
/// 13).
///
/// No column is decoded, so neither per-column bound runs even once: the pre-decode
/// `row_body_exhausted` check is evaluated only for a column the bitmap declares
/// PRESENT, and the post-decode `row_bound_check` only for a column whose decoder
/// ran. The reconciliation at the end of row assembly therefore saw a cursor PAST the
/// row's end, a state it `debug_assert!`ed could not happen — so it PANICKED on
/// malformed bytes in every debug build (measured, at `row_data.rs:870`), which is
/// the same class of defect as an `unwrap()` in library code. Issue #3721 exists
/// because malformed data produced a SILENT SUCCESS; a panic on a different
/// malformed shape is not the fix.
///
/// The report must also be ACCURATE about what happened: no column consumed anything,
/// so the cursor's position is where the row's own METADATA ended, and the message
/// says that rather than blaming a column set that never ran.
#[tokio::test]
async fn a_row_whose_declared_size_ends_inside_its_metadata_fails_the_select_instead_of_panicking()
{
    let patches: Vec<(usize, u8, u8)> = ALL_COLUMNS_ABSENT
        .iter()
        .chain(ROW_SIZE_ZERO.iter())
        .copied()
        .collect();
    let err = select_bodies(&patches).await.expect_err(
        "a row whose declared extent ends inside its own metadata cannot be read, and no \
         column's bound check can see it — row assembly must return a named error, and must \
         never panic on malformed bytes",
    );
    assert_column_decode_at(
        &err,
        // No column was decoded, so none can be named — and the report says exactly
        // that instead of attributing the overrun to a column whose decoder never ran.
        "<no column decoded>",
        // The cursor is where the row's metadata ended: nothing else advances it.
        ROW_METADATA_END,
        &[
            // The condition, why no column can be blamed, the two extents that
            // disagree, and the arithmetic between them.
            "row body over-consumed with NO column decoded",
            "every one of them is ABSENT",
            &format!("cursor at offset {ROW_METADATA_END}"),
            &format!("{} byte(s) PAST", ROW_METADATA_END - ROW_END_SIZE_ZERO),
            &format!("row body its header declares at {ROW_END_SIZE_ZERO}"),
            "declared size ends INSIDE its metadata",
            // The column set the row declares but cannot hold.
            "1 on-disk column(s)",
        ],
    );
}

/// DISCRIMINATING CONTROL for the case above: the SAME all-columns-absent row with
/// Cassandra's own `row_size` left intact is a DIFFERENT state and must be reported as
/// that one — the row's 316-byte body is then accounted for by no column at all, which
/// is under-consumption.
///
/// Without this, a fix that refused every all-columns-absent row would satisfy the
/// case above. It also pins the "no column decoded" attribution: there is no column to
/// name, and the report says so rather than naming an unrelated one.
#[tokio::test]
async fn the_same_all_columns_absent_row_with_its_real_row_size_is_reported_as_under_consumption() {
    let err = select_bodies(&ALL_COLUMNS_ABSENT).await.expect_err(
        "a row body no column accounts for means the cursor was misaligned inside it, so the \
         read must fail",
    );
    assert_column_decode_at(
        &err,
        "<no column decoded>",
        ROW_METADATA_END,
        &[
            "row body under-consumed",
            &format!("{ROW_BODY_END}"),
            &format!("{} byte(s)", ROW_BODY_END - ROW_METADATA_END),
            "no column decoded",
        ],
    );
}

// ─── CONTROLS: the patched bytes are what fail the read ─────────────────────

/// The SAME scratch copy, UNPATCHED, returns every row with the value Cassandra
/// wrote. Without this, a fix that failed every read of a checksum-less SSTable — or
/// of this fixture at all — would satisfy both cases above.
#[tokio::test]
async fn the_unpatched_scratch_copy_reads_every_row() {
    let rows = select_bodies(&[])
        .await
        .expect("the unpatched scratch copy must read cleanly");
    assert_eq!(
        rows.len(),
        FULL_ROWS,
        "present fixture must return all {FULL_ROWS} rows — a short count is a read \
         regression, never a pass"
    );
    // The patched cases both edit the FIRST row's `body`, so pin that row's real
    // value: its length is what the ±16 skews are measured against.
    let (ck, body) = &rows[0];
    assert_eq!(*ck, 1, "the first row must be ck=1");
    assert!(
        body.contains("compressible_payload_row_00001_"),
        "ck=1's body must be the text Cassandra wrote; got: {body}"
    );
}

// ─── The PURE ROW TOMBSTONE whose declared size ends inside its metadata ──────
//     (#3721, roborev job 14)
//
// A row carrying ROW_HAS_DELETION with no cell bytes takes a FAST PATH that returns
// before the column walk — so neither per-column bound nor the post-walk
// reconciliation above can see it. That path decided "no cells" with `cell_data_start
// < after_row_offset` being FALSE, i.e. it accepted TWO states as one:
//
//   ==  the row's metadata ends exactly at the row's declared end: a genuine pure
//       tombstone, the state the fast path exists for.
//   >   the declared `row_size` ends INSIDE the row's own metadata, which no
//       well-formed row can do (`row_size` is counted from after its own vint and
//       spans the whole body, `cassandra-5.0.8`
//       db/rows/UnfilteredSerializer.serialize).
//
// The `>` state returned `Ok` with `next_offset` = the row's declared end, i.e. a
// cursor BEHIND where the row's metadata actually ended: the row loop then re-read
// this row's own metadata bytes as the next row's header. That misparses the REST OF
// THE PARTITION, not just one row, and every value it produces looks well-formed.
//
// The patch turns Cassandra's own first row into that shape with three one-byte edits,
// each changing a field IN PLACE (no byte shifts):
//
//   0x12  row flags 0x24 -> 0x34   add HAS_DELETION (0x10), so the row header now
//                                  carries markedForDeleteAt + localDeletionTime
//                                  vints, read from the bytes Cassandra wrote as the
//                                  `body` cell's flags (0x1c = 0x08, one byte) and
//                                  its length vint (0x1d..0x1e = 0x81 0x37, two
//                                  bytes) -> header_size 4 -> 7, metadata ends 0x1f
//   0x18  row_size  0x81 -> 0x80 } row_size 316 -> 0 in the SAME two-byte encoding,
//   0x19  row_size  0x3c -> 0x00 } so the row body ends at 0x1a + 0 = 0x1a — FIVE
//                                  bytes BEFORE the metadata's own end at 0x1f
//
// HAS_ALL_COLUMNS (0x20) stays set, so no missing-columns bitmap is read and the row
// declares every column present; the fast path is reached purely because the declared
// extent leaves no room for cell bytes.

/// `HAS_ALL_COLUMNS | HAS_TIMESTAMP | HAS_DELETION`: the flags byte with only
/// `ROW_HAS_DELETION` (0x10) added to what Cassandra wrote.
const ROW_FLAGS_WITH_DELETION: u8 = 0x34;
/// Where the metadata of the HAS_DELETION row ends: `0x18 + header_size(7)` — the
/// row_size vint (2) + prev-size vint (1) + timestamp delta (1) + markedForDeleteAt
/// vint (1) + localDeletionTime vint (2). Also `cell_data_start`, since cell data
/// begins immediately after the row header.
const TOMBSTONE_METADATA_END: usize = 0x1f;

/// The one edit that makes the row a ROW-LEVEL DELETION.
const ROW_HAS_DELETION: (usize, u8, u8) =
    (ROW_FLAGS_BYTE, ROW_FLAGS_VALUE, ROW_FLAGS_WITH_DELETION);

/// `row_size = 5` in the same two-byte encoding: exactly the 5 bytes of row body the
/// HAS_DELETION metadata occupies after its own vint (`0x1a..0x1f`), so the row's
/// declared end EQUALS its metadata's end — a well-formed PURE ROW TOMBSTONE.
const ROW_SIZE_EXACT_METADATA: [(usize, u8, u8); 2] = [
    (
        ROW_SIZE_VINT_HIGH_BYTE,
        ROW_SIZE_VINT_HIGH_VALUE,
        ROW_SIZE_ZERO_HIGH,
    ),
    (ROW_SIZE_VINT_LOW_BYTE, ROW_SIZE_VINT_LOW_VALUE, 0x05),
];

/// A PURE ROW TOMBSTONE whose declared `row_size` ends INSIDE its own metadata must
/// FAIL the read with a NAMED, propagating error — never be returned as `Ok` with the
/// cursor moved BACK to that impossible end (#3721, roborev job 14).
///
/// The fast path is the only reader of this row: it returns before the column walk, so
/// neither the per-column `row_bound_check` nor the post-walk `row_body_reconcile`
/// (the two cases above) is evaluated even once.
///
/// # Measured pre-fix, on exactly these bytes
///
/// The fast path returned this row as `Ok` and resumed at its declared end — FIVE bytes
/// INSIDE the metadata it had just read — so the row loop re-read that metadata as the
/// next row's header. The read did not fail HERE; it failed 100 bytes later, and about
/// something else entirely:
///
/// ```text
/// Corruption("range-tombstone marker at offset 131 of partition 0 could not be
///  represented faithfully … unknown range tombstone bound kind 97")
/// ```
///
/// Bound kind 97 is the ASCII `a` of the text Cassandra wrote — a PHANTOM marker
/// decoded out of the middle of a cell value, caught only by the unrelated #3808
/// refusal, naming an offset and a condition that have nothing to do with the malformed
/// row. That is the harm this case pins: the backward cursor move misparses the REST OF
/// THE PARTITION, and whether the misparse happens to raise (as here) or to yield
/// plausible rows (as it would where the following bytes parse) is an accident of the
/// data. The row must be refused where it is READ.
#[tokio::test]
async fn a_pure_row_tombstone_whose_size_ends_inside_its_metadata_fails_the_select() {
    let patches: Vec<(usize, u8, u8)> = std::iter::once(ROW_HAS_DELETION)
        .chain(ROW_SIZE_ZERO.iter().copied())
        .collect();
    let err = select_bodies(&patches).await.expect_err(
        "a row-tombstone row whose declared extent ends inside its own metadata has no \
         readable framing: returning it moves the cursor BACKWARD and misparses the rest of \
         the partition, so row assembly must return a named error instead",
    );
    assert_column_decode_at(
        &err,
        // A pure tombstone decodes no column, so none can be named — the same sentinel
        // the post-walk reconciliation uses when no column ran.
        "<no column decoded>",
        // The refusal is measured at the metadata's end, which is where cell data would
        // have begun.
        TOMBSTONE_METADATA_END,
        &[
            // The condition, the two extents that disagree, the arithmetic between
            // them, and the consequence of accepting it.
            "pure row tombstone REFUSED",
            "HAS_DELETION metadata",
            &format!("ends at {TOMBSTONE_METADATA_END}"),
            &format!("declares at {ROW_END_SIZE_ZERO}"),
            &format!(
                "{} byte(s) beyond it",
                TOMBSTONE_METADATA_END - ROW_END_SIZE_ZERO
            ),
            "cursor BACK",
        ],
    );
}

/// DISCRIMINATING CONTROL for the case above, on the SAME row of the SAME fixture:
/// with `row_size` ending EXACTLY where the HAS_DELETION metadata does, the row is a
/// well-formed pure tombstone and the fast path must ACCEPT it — the refusal above must
/// come from the declared extent, not from HAS_DELETION with no cell bytes.
///
/// Because this patch shortens Cassandra's 316-byte row 1 to 5 bytes, the bytes that
/// follow are the MIDDLE of the text it wrote, so the read as a whole may still fail on
/// a phantom row further on (measured: `body` at offset 160, `invalid cell flags 0x61`
/// — the ASCII `a` of that text). That is the pre-existing behaviour of a walk pointed
/// at non-row bytes and is not this case's subject, so the assertion is on the ONE
/// property that discriminates: whatever the read does, it must NOT be the
/// pure-tombstone refusal, and must not be reported at this row's own extent.
///
/// The end-to-end proof that a REAL pure row tombstone reads is the next case, on
/// Cassandra-written bytes with no patch at all.
#[tokio::test]
async fn a_well_formed_pure_row_tombstone_is_accepted_by_the_fast_path() {
    let patches: Vec<(usize, u8, u8)> = std::iter::once(ROW_HAS_DELETION)
        .chain(ROW_SIZE_EXACT_METADATA.iter().copied())
        .collect();
    if let Err(err) = select_bodies(&patches).await {
        let text = format!("{err:?}");
        assert!(
            !text.contains("pure row tombstone REFUSED"),
            "a HAS_DELETION row with no cell bytes whose row_size ends EXACTLY at its \
             metadata's end is well-formed and must not be refused; got: {text}"
        );
        if let Error::ColumnDecode { offset, .. } = &err {
            assert_ne!(
                *offset, TOMBSTONE_METADATA_END,
                "the failure must not be attributed to the well-formed tombstone row's own \
                 extent; got: {text}"
            );
        }
    }
}

// ─── NEGATIVE CONTROL on Cassandra-written bytes: a REAL pure row tombstone ───
//
// The cases above construct their tombstone by editing a row Cassandra wrote for a
// different purpose. This one reads a row tombstone Cassandra ITSELF wrote, with NO
// patch of any kind, so nothing about the shape is this test's construction (#3042:
// an on-disk framing property must be pinned against Cassandra-written bytes).
//
// `test_tomb.static_with_tombstones` (committed, single `nb` flush) holds in pk=1,
// per its committed sstabledump golden:
//   * a static block with one live cell, `stat_col = 'surviving_static'`
//   * ck=1  live liveness marker + live `row_col = 'row_1'`
//   * ck=2  a ROW DELETION with ZERO cells — the pure tombstone this case exists for
//   * ck=3  live marker whose only regular cell is a cell tombstone
//   * a range tombstone bound pair covering [ck=4, ck=5] (no rows inside it)
//   * ck=6  live liveness marker + live `row_col = 'row_6'`

const TOMB_KEYSPACE: &str = "test_tomb";
const TOMB_TABLE: &str = "static_with_tombstones";
const TOMB_DIR: &str = "static_with_tombstones-4cdb9780702011f1b8f419c9a388d558";
const TOMB_SCHEMA_FILE: &str = "tombstone-parity.cql";
/// The clustering key of the row Cassandra wrote as a PURE ROW TOMBSTONE.
const TOMB_DELETED_CK: i32 = 2;
/// Clustering keys whose rows are LIVE, so the case cannot pass on an empty result.
const TOMB_LIVE_CKS: [i32; 2] = [1, 6];

/// A PURE ROW TOMBSTONE WRITTEN BY CASSANDRA still reads: the fast path's `==` state is
/// the state it exists for, and tightening it to equality must not refuse it (#3721,
/// roborev job 14).
///
/// Without this the fix could refuse every cell-less row-tombstone row — silently
/// breaking every tombstoned read — and the refusal case above would still pass. The
/// tombstoned row must be ABSENT from the result (its deletion is honoured) while the
/// live rows are present with the values Cassandra wrote, so the case cannot pass
/// vacuously on a read that returned nothing.
#[tokio::test]
async fn a_cassandra_written_pure_row_tombstone_still_reads() {
    let root = sstables_root_for_table(TOMB_KEYSPACE, TOMB_TABLE).unwrap_or_else(|| {
        panic!(
            "{TOMB_KEYSPACE}.{TOMB_TABLE} is COMMITTED to git and must resolve in every \
             checkout, unconditionally (issue #3220) — {}.\n  remedy: git restore \
             --source=HEAD -- test-data/datasets/sstables",
            describe_search(TOMB_KEYSPACE, TOMB_TABLE)
        )
    });
    let schema = datasets_root::schema_path(TOMB_SCHEMA_FILE).unwrap_or_else(|| {
        panic!(
            "{TOMB_SCHEMA_FILE} is COMMITTED source under test-data/schemas and must resolve \
             in every checkout (#3148)"
        )
    });
    // Copy just this table's directory, so the read never depends on the presence of
    // any other (fetch-only) table in the keyspace.
    let tmp = tempfile::tempdir().expect("tempdir");
    let scratch = tmp.path().join("sstables");
    let dst = scratch.join(TOMB_KEYSPACE).join(TOMB_DIR);
    std::fs::create_dir_all(&dst).expect("scratch dir");
    let src = root.join(TOMB_KEYSPACE).join(TOMB_DIR);
    for entry in std::fs::read_dir(&src).expect("read committed fixture dir") {
        let entry = entry.expect("dir entry");
        std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy component");
    }

    let db = open_db_filtered(&scratch, &schema, TOMB_KEYSPACE).await;
    let result = db
        .execute(&format!(
            "SELECT pk, ck, row_col FROM {TOMB_KEYSPACE}.{TOMB_TABLE} WHERE pk = 1"
        ))
        .await
        .expect(
            "a row tombstone Cassandra wrote with no cell bytes is the state the \
             pure-tombstone fast path exists for — the read must succeed",
        );
    let cks: Vec<i32> = result
        .rows
        .iter()
        .filter_map(|row| row.values.iter().find(|(n, _)| n.as_ref() == "ck"))
        .map(|(_, v)| {
            let shown = format!("{v:?}");
            shown
                .trim_start_matches(|c: char| !c.is_ascii_digit() && c != '-')
                .trim_end_matches(|c: char| !c.is_ascii_digit())
                .parse::<i32>()
                .unwrap_or_else(|e| panic!("clustering key must be an int, got {shown}: {e}"))
        })
        .collect();
    assert!(
        !cks.contains(&TOMB_DELETED_CK),
        "ck={TOMB_DELETED_CK} is a pure ROW TOMBSTONE and must be honoured (absent from the \
         result), not surfaced; got clustering keys {cks:?}"
    );
    for live in TOMB_LIVE_CKS {
        assert!(
            cks.contains(&live),
            "ck={live} is LIVE in this fixture and must be returned — a result missing it is a \
             read regression, and an empty result would let this control pass vacuously; got \
             clustering keys {cks:?}"
        );
    }
}
