//! Issue #3721 (roborev job 80) — a BTI point read must never report a partition
//! that EXISTS but cannot be DECODED as absent.
//!
//! # The defect
//!
//! `data_access::bti_point`'s `bti_decompress_and_parse_target` answered its parse
//! with a three-state catch-all:
//!
//! ```text
//! match parse_result {
//!     Ok(()) if emitted_our_key => return Ok(found),
//!     _ => { if chunk_targeted { continue; }  // retry to EOF -> Ok(None)
//!            return Ok(None); }               // "could not be parsed -> absent"
//! }
//! ```
//!
//! The `_` conflates an `Err`, the emit closure never firing, and a partition whose
//! decoded key is FOREIGN. Only the first can be a decode failure, and a BTI trie
//! hit is NOT followed by a scan fallback — so an undecodable partition was
//! reported `Ok(None)`, which no caller can tell apart from a genuine miss. The
//! comment stated the false inference outright: "could not be parsed" is not
//! "absent".
//!
//! # How the failure is induced on REAL Cassandra bytes
//!
//! The fixture is Cassandra 5.0-written BTI (`test_da.simple_table`,
//! `da-2-bti-Data.db`, `LZ4Compressor`). Its data section is ONE 191-byte chunk, so
//! a raw byte patch would normally corrupt the LZ4 block rather than the row — but
//! this block stores the row payload as LZ4 **literals** (191 -> 155 bytes: mostly
//! literal runs), and a literal byte is copied to the output verbatim. Patching one
//! literal byte therefore leaves the block structurally intact and changes exactly
//! one decompressed byte. Nothing is recompressed and no CQLite writer is involved:
//! the bytes under test are Cassandra's own.
//!
//! The patched byte is the `name` cell's LENGTH VInt for the row "Bob Johnson"
//! (`0x0b` = 11) in partition `2222…`, raised to `0x7f` = 127. The cell then claims
//! 127 bytes inside a row body that has far fewer, so the decode refuses with
//! [`Error::ColumnDecode`] — the exact state the catch-all swallowed.
//!
//! Hand-decoded (chunk 0 = file `[0, 159)`, its last 4 bytes the CRC32 of the
//! preceding 155; the body opens with a 4-byte little-endian uncompressed length):
//!
//! ```text
//! decompressed 41  0x0b                     `name` length VInt   <- the patch
//! decompressed 42  "Bob Johnson"            the value
//! compressed   35  0x0b                     the SAME byte, literal-backed
//! ```
//!
//! Both offsets are ASSERTED before the patch, so a regenerated fixture fails here
//! rather than silently patching some other byte.
//!
//! # The CRC is re-sealed, and the re-seal is PROVEN first
//!
//! Every compressed chunk carries a trailing big-endian CRC32 of its compressed
//! body, verified on read before decompression, so a raw patch would otherwise
//! surface as a checksum failure and never reach the decoder. The scratch copy
//! recomputes it — and the recomputation is verified against the COMMITTED sidecar
//! value first, so the framing is proven against Cassandra-written bytes before
//! anything depends on it.
//!
//! # Three controls, because "it errors now" could just mean point reads are broken
//!
//! 1. the UNPATCHED copy must RETURN the row (`Ok(Some(_))`);
//! 2. a genuinely ABSENT key must still be `Ok(None)` — on BOTH copies, so the fix
//!    did not turn absence into an error;
//! 3. on the PATCHED copy the OTHER, intact partition must still return its row —
//!    the refusal is scoped to the damaged partition, not to the file.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::error::ErrorCategory;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Error, Platform, RowKey, TableId};

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

const KEYSPACE: &str = "test_da";
const TABLE: &str = "simple_table";
const DATA_DB: &str = "da-2-bti-Data.db";
const KS_TABLE: &str = "test_da.simple_table";

/// File offset of the literal-backed `name` length VInt for partition `2222…`.
const NAME_LEN_OFFSET: usize = 35;
/// Its committed value: 11, the length of "Bob Johnson".
const NAME_LEN_ON_DISK: u8 = 0x0b;
/// The patched value: a length no row body here can satisfy.
const NAME_LEN_PATCHED: u8 = 0x7f;
/// The value bytes that must follow it, so the patch cannot land on some other
/// length-shaped byte.
const NAME_VALUE_ON_DISK: &[u8] = b"Bob Johnson";
/// Chunk 0 spans file `[0, CHUNK0_END)`; its last 4 bytes are the CRC32 of the
/// preceding [`CHUNK0_BODY_LEN`] bytes.
const CHUNK0_END: usize = 159;
const CHUNK0_BODY_LEN: usize = CHUNK0_END - 4;

/// The partition holding the patched row.
const PATCHED_PARTITION: u8 = 0x22;
/// A partition of the same fixture that the patch does not touch.
const INTACT_PARTITION: u8 = 0x11;
/// A key that is genuinely not in the fixture.
const ABSENT_PARTITION: u8 = 0x99;

/// Resolve the fixture directory, or FAIL — never skip (issue #3220).
fn fixture_dir() -> PathBuf {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "{KEYSPACE}.{TABLE} must resolve in every checkout with a fetched corpus \
             (issue #3220) — {}.\n  remedy: bash test-data/scripts/fetch-datasets.sh",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let ks = root.join(KEYSPACE);
    let dir = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| panic!("read {}: {e}", ks.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .unwrap_or_else(|| panic!("no {TABLE}-* directory under {}", ks.display()));
    assert!(
        dir.join(DATA_DB).is_file(),
        "the generation this lane decodes by hand ({DATA_DB}) must be present in {}",
        dir.display()
    );
    assert!(
        dir.join(DATA_DB.replace("Data.db", "CompressionInfo.db"))
            .is_file(),
        "this lane's literal patch depends on the fixture being LZ4-COMPRESSED \
         (a CompressionInfo.db must be present) in {}",
        dir.display()
    );
    dir
}

/// Assert the byte grammar the patch depends on, then that the CRC recomputation
/// reproduces the COMMITTED chunk CRC — proving the framing before relying on it.
fn assert_fixture_grammar(data: &[u8]) {
    assert!(
        data.len() >= CHUNK0_END,
        "Data.db is shorter than chunk 0 ({} bytes)",
        data.len()
    );
    assert_eq!(
        data[NAME_LEN_OFFSET], NAME_LEN_ON_DISK,
        "expected the committed `name` length VInt at file offset {NAME_LEN_OFFSET:#x}"
    );
    assert_eq!(
        &data[NAME_LEN_OFFSET + 1..NAME_LEN_OFFSET + 1 + NAME_VALUE_ON_DISK.len()],
        NAME_VALUE_ON_DISK,
        "expected the literal value bytes immediately after the length VInt — the \
         patch offset is only meaningful with them there"
    );
    let stored = u32::from_be_bytes([
        data[CHUNK0_BODY_LEN],
        data[CHUNK0_BODY_LEN + 1],
        data[CHUNK0_BODY_LEN + 2],
        data[CHUNK0_BODY_LEN + 3],
    ]);
    assert_eq!(
        stored,
        crc32fast::hash(&data[..CHUNK0_BODY_LEN]),
        "the CRC32 recomputed over the COMMITTED chunk-0 body must equal the committed \
         trailing CRC — otherwise this lane's re-seal is wrong and a patched fixture \
         would fail on the checksum, not on the decode"
    );
}

/// Copy the fixture into `dir`, optionally patching the one literal byte and
/// re-sealing chunk 0's CRC. The committed fixture is never written to.
fn stage(dir: &Path, patch: bool) -> PathBuf {
    let src = fixture_dir();
    let dest = dir.join("data").join(KEYSPACE).join(
        src.file_name()
            .expect("fixture directory has a name")
            .to_str()
            .expect("fixture directory name is UTF-8"),
    );
    std::fs::create_dir_all(&dest).expect("create scratch fixture directory");

    for entry in std::fs::read_dir(&src).expect("read fixture directory") {
        let entry = entry.expect("fixture directory entry");
        if !entry.path().is_file() {
            continue;
        }
        let name = entry.file_name();
        let mut bytes = std::fs::read(entry.path()).expect("read fixture component");
        if name == std::ffi::OsStr::new(DATA_DB) {
            assert_fixture_grammar(&bytes);
            if patch {
                bytes[NAME_LEN_OFFSET] = NAME_LEN_PATCHED;
                let crc = crc32fast::hash(&bytes[..CHUNK0_BODY_LEN]);
                bytes[CHUNK0_BODY_LEN..CHUNK0_END].copy_from_slice(&crc.to_be_bytes());
            }
        }
        std::fs::write(dest.join(name), bytes).expect("write scratch fixture component");
    }
    dest.join(DATA_DB)
}

async fn open_reader(data_db: &Path) -> Arc<SSTableReader> {
    let cfg = Config::default();
    let platform = Arc::new(Platform::new(&cfg).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &cfg, platform)
        .await
        .expect("the staged fixture copy must OPEN (its chunk CRC is re-sealed)");
    assert_eq!(
        reader.format_version().expect("format version"),
        "da",
        "this lane's hand decode is of the BTI (`da`) fixture"
    );
    Arc::new(reader)
}

/// `SSTableReader::get` for a 16-byte UUID partition key of `repeat` bytes.
async fn get(data_db: &Path, repeat: u8) -> Result<Option<cqlite_core::types::ScanRow>, Error> {
    let reader = open_reader(data_db).await;
    reader
        .get(&TableId::new(KS_TABLE), &RowKey::from(vec![repeat; 16]))
        .await
}

// ─────────────────────────────────────────────────────────────────────────────
// The defect
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_read_refuses_an_undecodable_partition_instead_of_reporting_it_absent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_db = stage(tmp.path(), true);

    match get(&data_db, PATCHED_PARTITION).await {
        Ok(None) => panic!(
            "`get` reported partition 0x{PATCHED_PARTITION:02x}… ABSENT when it EXISTS and \
             merely cannot be decoded (issue #3721, roborev job 80): a BTI trie hit has no \
             scan fallback behind it, so this answer is final and indistinguishable from a \
             genuine miss"
        ),
        Ok(Some(row)) => panic!(
            "`get` returned a row for a partition whose `name` cell declares more bytes than \
             its row body has — the decode failure was swallowed, not surfaced. Row: {row:?}"
        ),
        Err(e) => {
            let rendered = e.to_string();
            assert!(
                matches!(e, Error::ColumnDecode { .. }),
                "the refusal must be the dedicated per-column variant, so no caller can fold \
                 it back into absence; got: {e:?}"
            );
            assert_eq!(e.category(), ErrorCategory::Data);
            assert!(!e.is_recoverable());
            assert!(
                rendered.contains("name"),
                "the refusal must name the column it could not decode; got: {rendered}"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Controls — without these, "it errors now" could just mean point reads broke
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_point_read_returns_the_row_over_the_unpatched_fixture() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_db = stage(tmp.path(), false);
    let row = get(&data_db, PATCHED_PARTITION)
        .await
        .expect("the UNPATCHED fixture must read cleanly")
        .expect("the partition is present in the fixture");
    let rendered = format!("{row:?}");
    assert!(
        rendered.contains("Bob Johnson"),
        "the control must return the very row the patch damages; got: {rendered}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_genuinely_absent_key_is_still_absent_on_both_copies() {
    for patch in [false, true] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_db = stage(tmp.path(), patch);
        let got = get(&data_db, ABSENT_PARTITION).await;
        assert!(
            matches!(got, Ok(None)),
            "a key that is genuinely not in the fixture must still be reported ABSENT \
             (patched = {patch}) — the fix must refuse an UNDECODABLE partition, never turn \
             absence into an error; got: {got:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_intact_partition_of_the_patched_copy_still_returns_its_row() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_db = stage(tmp.path(), true);
    let row = get(&data_db, INTACT_PARTITION)
        .await
        .expect("an intact partition of the patched fixture must still read")
        .expect("the partition is present in the fixture");
    let rendered = format!("{row:?}");
    assert!(
        rendered.contains("Alice Smith"),
        "the refusal must be scoped to the DAMAGED partition, not to the file; got: {rendered}"
    );
}
