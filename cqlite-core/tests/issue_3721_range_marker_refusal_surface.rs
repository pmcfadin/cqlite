//! Issue #3721 — a range-tombstone MARKER the read-side shadow FSM cannot
//! represent must be OBSERVABLE, never a successful `SELECT` that silently drops
//! the marker AND every later row of the partition.
//!
//! # The defect this pins
//!
//! `issue_3721_column_decode_error_surface.rs` pins the COLUMN-level instances of
//! the swallow. A closed census then found the same construct one structural level
//! up — three range-tombstone MARKER handlers, each inside the `Ok` arm of
//! `parse_range_tombstone_marker_full` (so a valid `next_offset` was already bound
//! and then discarded):
//!
//! ```text
//! block_emit.rs:133           if sh.feed_range_marker(..).is_err() { break; }
//! block_emit_windowed.rs:370  if let Err(e) = sh.feed_range_marker(..) { .. break; }
//! timestamp_policy.rs:125     if sh.feed_range_marker(..).is_err() { MarkerOutcome::Stop }
//! ```
//!
//! Each left the partition's row loop and the read then returned `Ok`, so the
//! marker and every remaining row of that partition vanished from a SUCCESSFUL
//! read. `PartitionShadow::feed_range_marker` returns `Err` on exactly one
//! condition — a bound kind it has no faithful representation for — which is
//! corruption, not a framing boundary: the marker is framed, and the partition
//! body demonstrably continues at the `next_offset` the caller threw away.
//!
//! # The three surfaces, one per defect site
//!
//! | site | public surface | query |
//! |------|----------------|-------|
//! | `block_emit_windowed` (D2) | full scan / point read | `SELECT *`, `SELECT … WHERE id = 1` |
//! | `block_emit` (D1) | the `WRITETIME`/`TTL` cell-metadata scan | `SELECT …, WRITETIME(v) …` |
//! | `timestamp_policy` (D5) | the bounded streaming scan | `SELECT COUNT(*)` |
//!
//! # How an unrepresentable marker is produced
//!
//! The fixture is REAL Cassandra-written, UNCOMPRESSED data
//! (`test_compaction_tombstone_ttl.rt_cross_gen`, `compression = {'enabled':
//! 'false'}`, so the marker's bytes are addressable on disk without re-framing an
//! LZ4 chunk). A scratch COPY has ONE byte changed: the first marker's bound-kind
//! byte, from a valid bound kind to `0x03`, which is outside the set the shadow FSM
//! represents. Nothing else is touched, and the committed fixture is never mutated.
//!
//! Hand-decoded marker grammar of the committed `nb-3-big-Data.db`
//! (`parse_range_tombstone_marker_with_ldt`: flags, bound kind, `u16` cluster
//! count, clustering prefix, body):
//!
//! ```text
//! 0x00  00 04 | 00 00 00 01 | 7fff ffff 8000 0000 0000 0000   partition header (id=1, LIVE)
//! 0x12  ..                                                    row, clustering ck=5
//! 0x22  02                                                    marker flags (IS_MARKER)
//! 0x23  01                                                    bound kind  <-- the patched byte
//! 0x24  00 01                                                 cluster count = 1
//! 0x26  00 | 00 00 00 0a                                      clustering prefix, ck=10
//! ..                                                          two further markers (ck=15, ck=25)
//! 0x4f  ..                                                    row, clustering ck=30
//! ```
//!
//! Every one of those bytes is ASSERTED before the patch is applied, so a
//! regenerated fixture FAILS loudly instead of silently patching the wrong byte.
//!
//! # The `CRC.db` is RE-SEALED, deliberately
//!
//! An uncompressed BIG SSTable carries a `CRC.db` and CQLite verifies every
//! covering chunk on EVERY read (issue #1396), so a raw byte patch would surface as
//! a checksum failure and never reach the marker handler — a test that passed for
//! the wrong reason. The scratch copy therefore recomputes the chunk CRC32 over the
//! patched `Data.db` (`4`-byte big-endian chunk-size header, then one big-endian
//! CRC32 per chunk — `reader::crc::CrcDb`). The recomputation is itself verified:
//! the CRC computed over the UNPATCHED bytes must equal the committed sidecar's
//! stored value, so the algorithm and framing are proven against Cassandra-written
//! data before either is relied on. The result is that the ONLY thing wrong with
//! the patched fixture is the bound kind.
//!
//! # The control, and why it is load-bearing
//!
//! Each case is run TWICE — over the unpatched copy and over the patched copy. The
//! unpatched read must return both live rows (`ck = 5` and `ck = 30`, neither
//! inside the deleted `[10, 25]` range), which is exactly the output the pre-fix
//! swallow could not produce for the patched copy: it returned `Ok` with `ck = 5`
//! alone. So "Err on the patched copy" is attributable to the one changed byte,
//! and a fix that turned EVERY marker into an error would fail the unpatched half.

use std::path::{Path, PathBuf};

use cqlite_core::error::ErrorCategory;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::{Config, Database, Error};

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

const KEYSPACE: &str = "test_compaction_tombstone_ttl";
const TABLE: &str = "rt_cross_gen";
const DATA_DB: &str = "nb-3-big-Data.db";

/// Offset of the FIRST range-tombstone marker's flags byte in the committed
/// `Data.db` (see the module docs for the full hand decode).
const MARKER_FLAGS_OFFSET: usize = 0x22;
/// Offset of that marker's bound-kind byte — the ONE byte the scratch copy patches.
const BOUND_KIND_OFFSET: usize = 0x23;
/// The committed fixture's bound kind at [`BOUND_KIND_OFFSET`]: an inclusive START
/// bound (the JSONL golden's first `range_tombstone_bound`, `clustering [10]`).
const BOUND_KIND_ON_DISK: u8 = 0x01;
/// A bound kind OUTSIDE the set `PartitionShadow::feed_range_marker` represents, so
/// the marker parses (framing is driven by the cluster count, not the kind) and the
/// FSM then refuses it. That refusal is the condition under test.
const BOUND_KIND_UNREPRESENTABLE: u8 = 0x03;

/// Offset of that marker's `marker_body_size` VUInt — the point
/// [`Patch::TruncatedAtMarker`] CUTS the file at. It follows the clustering prefix:
/// flags(1) kind(1) cluster_count(2) prefix_header(1) ck(4) = 0x22 + 9.
const MARKER_BODY_SIZE_OFFSET: usize = 0x2b;
/// The committed fixture's `marker_body_size` at [`MARKER_BODY_SIZE_OFFSET`]: 4
/// bytes (`prev_size` VUInt + the deletion-time pair).
const MARKER_BODY_SIZE_ON_DISK: u8 = 0x04;
/// The four body bytes the committed `marker_body_size` covers, at
/// [`MARKER_BODY_SIZE_OFFSET`] + 1: `prev_unfiltered_size` VUInt (`0x10`), the
/// `markedForDeleteAt` delta VUInt (`0x87 0xd0`, a 2-byte encoding) and the
/// `localDeletionTime` delta VUInt (`0x00`). Asserted before either
/// body-size patch, because both depend on the body being EXACTLY these four
/// bytes wide: [`Patch::MarkerBodySizeTooSmall`] must cut the LAST field off, and
/// [`Patch::MarkerBodySizeTooLarge`] must overrun into the byte after them.
const MARKER_BODY_ON_DISK: [u8; 4] = [0x10, 0x87, 0xd0, 0x00];
/// A `marker_body_size` one byte SHORT of the truth — the DOWNWARD corruption.
/// Bounding the body decode to `body_end` then runs the `localDeletionTime` read
/// out of bytes; unbounded (pre-fix) it read that field from the NEXT
/// unfiltered's bytes and returned `Ok` with the cursor left INSIDE the marker.
const MARKER_BODY_SIZE_TOO_SMALL: u8 = 0x03;
/// A `marker_body_size` one byte LONG — still inside the buffer, so the upward
/// guard (`vuint_length_within`) cannot see it. The declared extent then ends one
/// byte past where the fields do, which only the exact-consumption assert catches;
/// pre-fix `pos` was OVERWRITTEN with that extent and the next unfiltered's flags
/// byte at [`NEXT_UNFILTERED_OFFSET`] was skipped.
const MARKER_BODY_SIZE_TOO_LARGE: u8 = 0x05;
/// The offset of the unfiltered that FOLLOWS the first marker — the byte a
/// mis-declared body size makes the caller resume at (one early for the too-small
/// patch, one late for the too-large one). It is the SECOND marker's flags byte.
const NEXT_UNFILTERED_OFFSET: usize = 0x30;

/// `CRC.db` layout (`reader::crc::CrcDb::parse`): a 4-byte big-endian chunk-size
/// header followed by one 4-byte big-endian CRC32 per Data.db chunk.
const CRC_HEADER_LEN: usize = 4;

/// The two live clustering keys of the fixture's single partition. Both lie outside
/// the deleted `[10, 25]` clustering range, so a correct read returns both.
const LIVE_CLUSTERING: [i64; 2] = [5, 30];

const SCHEMA: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_compaction_tombstone_ttl WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS test_compaction_tombstone_ttl.rt_cross_gen (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
);
";

/// Which one-byte patch a staged scratch copy carries. The committed fixture is
/// never written to, and every case names its patch explicitly so a control can
/// never silently become a defect case.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Patch {
    /// No patch — the control. The marker is well formed, so it must be paired and
    /// applied normally even on the final chunk.
    None,
    /// The marker's BOUND KIND byte -> a kind outside the set the readers can
    /// represent. The marker still PARSES (framing is driven by the cluster count,
    /// not the kind), so there IS a resume offset and the condition is a refusal
    /// (issue #3808).
    UnrepresentableBoundKind,
    /// The `Data.db` CUT at [`MARKER_BODY_SIZE_OFFSET`] — the file ends INSIDE the
    /// first marker, after its clustering prefix and before its body. The marker
    /// therefore cannot be PARSED at all and there is NO resume offset (issue
    /// #3721, roborev job 16), and — because the cut is at the end of the file —
    /// nothing follows to trip a later decode: on the final chunk this used to be
    /// converted into a SUCCESSFUL partition completion with the tombstone gone.
    ///
    /// This is deliberately the same shape a marker cut in half by a window
    /// boundary has, which is exactly why the final-vs-non-final decision must come
    /// from the chunking state and never from the bytes.
    TruncatedAtMarker,
    /// The `marker_body_size` VUInt corrupted DOWNWARD by one byte (issue #3721,
    /// roborev job 75). Nothing else changes — and because the committed value is
    /// a SINGLE-byte VUInt the file length does not change either, so every other
    /// structure of the fixture stays exactly where it was.
    ///
    /// The declared body then ends one byte BEFORE the fields do. Decoding the
    /// body within that extent runs the `localDeletionTime` read out of bytes, so
    /// the marker cannot be parsed. Pre-fix the fields were read from the WHOLE
    /// data slice — i.e. the last one came from the NEXT unfiltered's bytes — and
    /// `pos` was then overwritten with the declared end, so the parse returned
    /// `Ok` with the cursor left INSIDE this marker.
    MarkerBodySizeTooSmall,
    /// The `marker_body_size` VUInt corrupted UPWARD by one byte, staying well
    /// inside the buffer so the pre-existing upward guard (`vuint_length_within`,
    /// which only refuses a size exceeding the data) cannot see it.
    ///
    /// The fields then end one byte BEFORE the declared extent, which nothing but
    /// the exact-consumption assert can detect: pre-fix `pos` was overwritten with
    /// the declared end, so the caller resumed one byte LATE — past the next
    /// unfiltered's flags byte at [`NEXT_UNFILTERED_OFFSET`].
    MarkerBodySizeTooLarge,
}

impl Patch {
    /// Apply the patch to a scratch copy of the committed `Data.db`. Takes and
    /// returns the buffer because one variant changes its LENGTH.
    fn apply(self, mut data: Vec<u8>) -> Vec<u8> {
        match self {
            Patch::None => data,
            Patch::UnrepresentableBoundKind => {
                data[BOUND_KIND_OFFSET] = BOUND_KIND_UNREPRESENTABLE;
                data
            }
            Patch::TruncatedAtMarker => {
                data.truncate(MARKER_BODY_SIZE_OFFSET);
                data
            }
            Patch::MarkerBodySizeTooSmall => {
                data[MARKER_BODY_SIZE_OFFSET] = MARKER_BODY_SIZE_TOO_SMALL;
                data
            }
            Patch::MarkerBodySizeTooLarge => {
                data[MARKER_BODY_SIZE_OFFSET] = MARKER_BODY_SIZE_TOO_LARGE;
                data
            }
        }
    }
}

/// Resolve the fixture root, or FAIL — never skip (issue #3220).
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
        "the fixture generation this lane decodes by hand ({DATA_DB}) must be present in {}",
        dir.display()
    );
    assert!(
        !dir.join(DATA_DB.replace("Data.db", "CompressionInfo.db"))
            .is_file(),
        "the fixture must stay UNCOMPRESSED — a CompressionInfo.db means the marker \
         bytes are no longer addressable on disk and the hand decode is void"
    );
    dir
}

/// Assert the marker grammar the patch depends on, byte by byte. A regenerated
/// fixture fails HERE rather than silently having some other byte patched.
fn assert_marker_grammar(data: &[u8]) {
    assert!(
        data.len() > BOUND_KIND_OFFSET + 8,
        "Data.db is shorter than the hand-decoded marker ({} bytes)",
        data.len()
    );
    // IS_MARKER (0x02) set, END_OF_PARTITION (0x01) clear, HAS_EXTENDED_FLAGS
    // (0x08) clear — so the bound kind is the very next byte.
    assert_eq!(
        data[MARKER_FLAGS_OFFSET], 0x02,
        "expected the first range-tombstone marker's flags byte at 0x{MARKER_FLAGS_OFFSET:02x}"
    );
    assert_eq!(
        data[BOUND_KIND_OFFSET], BOUND_KIND_ON_DISK,
        "expected the committed inclusive-START bound kind at 0x{BOUND_KIND_OFFSET:02x}"
    );
    assert_eq!(
        &data[BOUND_KIND_OFFSET + 1..BOUND_KIND_OFFSET + 3],
        &[0x00, 0x01],
        "expected cluster_count = 1 (u16 big-endian) after the bound kind"
    );
    assert_eq!(
        &data[BOUND_KIND_OFFSET + 4..BOUND_KIND_OFFSET + 8],
        &10_i32.to_be_bytes(),
        "expected the marker's clustering value ck = 10 (the JSONL golden's first bound)"
    );
    assert_eq!(
        data[MARKER_BODY_SIZE_OFFSET], MARKER_BODY_SIZE_ON_DISK,
        "expected the committed marker_body_size VUInt at 0x{MARKER_BODY_SIZE_OFFSET:02x}"
    );
    // The four body bytes the declared size covers, and the unfiltered that
    // follows them. BOTH body-size patches depend on this exact layout: too-small
    // must cut the LAST field off, too-large must overrun by exactly one byte into
    // the next unfiltered's flags.
    assert_eq!(
        &data[MARKER_BODY_SIZE_OFFSET + 1..MARKER_BODY_SIZE_OFFSET + 1 + MARKER_BODY_ON_DISK.len()],
        &MARKER_BODY_ON_DISK,
        "expected the marker body (prev_size VUInt + the markedForDeleteAt/localDeletionTime \
         delta pair) at 0x{:02x}",
        MARKER_BODY_SIZE_OFFSET + 1
    );
    assert_eq!(
        MARKER_BODY_SIZE_OFFSET + 1 + MARKER_BODY_ON_DISK.len(),
        NEXT_UNFILTERED_OFFSET,
        "the declared body must end exactly where the next unfiltered begins, or neither \
         body-size patch is off by the one byte it claims to be"
    );
    assert_eq!(
        data[NEXT_UNFILTERED_OFFSET], 0x02,
        "expected the SECOND range-tombstone marker's flags byte at \
         0x{NEXT_UNFILTERED_OFFSET:02x} — the byte a too-large body size makes the caller skip"
    );

    // The cut must land INSIDE the marker, or `Patch::TruncatedAtMarker` would be
    // cutting somewhere else entirely.
    assert!(
        MARKER_FLAGS_OFFSET < MARKER_BODY_SIZE_OFFSET && MARKER_BODY_SIZE_OFFSET < data.len(),
        "the truncation offset 0x{MARKER_BODY_SIZE_OFFSET:02x} must lie strictly inside the \
         first marker of the {} byte Data.db",
        data.len()
    );
}

/// Re-seal `CRC.db` over `data`: recompute one CRC32 per `chunk_size` chunk of the
/// raw `Data.db` bytes. Verifies the recomputation against the committed sidecar
/// FIRST (called with the unpatched bytes), so a framing or algorithm mistake fails
/// here rather than silently producing a fixture that fails for the wrong reason.
fn reseal_crc(crc_db: &[u8], data: &[u8], verify_only: bool) -> Vec<u8> {
    assert!(
        crc_db.len() > CRC_HEADER_LEN && (crc_db.len() - CRC_HEADER_LEN) % 4 == 0,
        "CRC.db must be a 4-byte chunk-size header plus one 4-byte CRC32 per chunk; got {} bytes",
        crc_db.len()
    );
    let chunk_size = u32::from_be_bytes([crc_db[0], crc_db[1], crc_db[2], crc_db[3]]) as usize;
    assert!(chunk_size > 0, "CRC.db chunk size must be positive");

    let mut out = crc_db.to_vec();
    for (chunk_index, chunk) in data.chunks(chunk_size).enumerate() {
        let at = CRC_HEADER_LEN + chunk_index * 4;
        assert!(
            at + 4 <= out.len(),
            "CRC.db has no entry for Data.db chunk {chunk_index} ({} bytes for {} bytes of data)",
            crc_db.len(),
            data.len()
        );
        let computed = crc32fast::hash(chunk);
        if verify_only {
            let stored = u32::from_be_bytes([out[at], out[at + 1], out[at + 2], out[at + 3]]);
            assert_eq!(
                stored, computed,
                "the CRC32 recomputed over the COMMITTED Data.db chunk {chunk_index} must equal \
                 the committed CRC.db entry — otherwise this lane's re-seal is wrong and a \
                 patched fixture would fail on the checksum, not on the marker"
            );
        } else {
            out[at..at + 4].copy_from_slice(&computed.to_be_bytes());
        }
    }
    out
}

/// Copy the fixture into `dir` as `<keyspace>/<table>-*/…`, applying `patch` to the
/// first range-tombstone marker and re-sealing `CRC.db` over the result. The
/// committed fixture is never written to.
fn stage(dir: &Path, patch: Patch) -> PathBuf {
    let src = fixture_dir();
    let dest_root = dir.join("data");
    let dest = dest_root.join(KEYSPACE).join(
        src.file_name()
            .expect("fixture directory has a name")
            .to_str()
            .expect("fixture directory name is UTF-8"),
    );
    std::fs::create_dir_all(&dest).expect("create scratch fixture directory");

    let original = std::fs::read(src.join(DATA_DB)).expect("read fixture Data.db");
    assert_marker_grammar(&original);
    let patched = patch.apply(original.clone());

    for entry in std::fs::read_dir(&src).expect("read fixture directory") {
        let entry = entry.expect("fixture directory entry");
        if !entry.path().is_file() {
            continue;
        }
        let name = entry.file_name();
        let bytes = std::fs::read(entry.path()).expect("read fixture component");
        let bytes = if name == std::ffi::OsStr::new(DATA_DB) {
            patched.clone()
        } else if name.to_str().is_some_and(|n| n.ends_with("-CRC.db")) {
            // Prove the re-seal against the committed sidecar, THEN apply it.
            reseal_crc(&bytes, &original, true);
            reseal_crc(&bytes, &patched, false)
        } else {
            bytes
        };
        std::fs::write(dest.join(name), bytes).expect("write scratch fixture component");
    }
    dest_root
}

/// The fixture's schema, as `TableSchema` (the compaction and reader surfaces
/// take the struct, not CQL text). Mirrors the committed
/// `test_compaction_tombstone_ttl.rt_cross_gen` DDL.
///
/// Lives at the top level (not inside `d6_compaction`) because the reader-driver
/// cases in `d8_marker_body_size` need it too and are NOT gated on `write-support`.
fn fixture_table_schema() -> TableSchema {
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: name == "v",
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
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
        columns: vec![col("id", "int"), col("ck", "int"), col("v", "text")],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// The staged fixture's `Data.db` (the patched or unpatched copy `stage` wrote).
fn staged_data_db(data_root: &Path) -> PathBuf {
    let ks = data_root.join(KEYSPACE);
    let table_dir = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| panic!("read {}: {e}", ks.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_dir())
        .unwrap_or_else(|| panic!("no staged table directory under {}", ks.display()));
    let data = std::fs::read_dir(&table_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", table_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .unwrap_or_else(|| panic!("no staged Data.db under {}", table_dir.display()));
    data
}

async fn open_db(data_root: &Path, schema: &Path) -> Database {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: data_root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

/// Run `query` against a staged copy of the fixture (patched or not) and return the
/// rendered rows.
async fn run(query: &str, patch: Patch) -> Result<Vec<Vec<(String, String)>>, Error> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_root = stage(tmp.path(), patch);
    let schema = tmp.path().join("schema.cql");
    std::fs::write(&schema, SCHEMA).expect("write scratch schema");
    let db = open_db(&data_root, &schema).await;
    let result = db.execute(query).await?;
    Ok(result
        .rows
        .iter()
        .map(|row| {
            let mut kv: Vec<(String, String)> = row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), format!("{v:?}")))
                .collect();
            kv.sort();
            kv
        })
        .collect())
}

/// The refusal must reach the caller as damaged data naming the condition, the
/// partition and both offsets — the marker's own and the resume point whose
/// existence is what proves this is not the end of the partition body.
fn assert_marker_refusal(err: &Error, query: &str) {
    assert_marker_refusal_text(&err.to_string(), query);
    assert_eq!(
        err.category(),
        ErrorCategory::Data,
        "an unrepresentable on-disk marker classifies as damaged data"
    );
}

/// The refusal MESSAGE, asserted apart from the category because one surface
/// cannot carry the category: the streaming merge flattens every non-`Cancelled`
/// producer error into `Error::Storage(String)`
/// (`merge::producer_msg::MergeProducerError::to_error`), so the compaction case
/// below asserts the text only. The text survives verbatim.
fn assert_marker_refusal_text(rendered: &str, query: &str) {
    for needle in [
        "range-tombstone marker",
        "could not be represented faithfully",
        "partition body continues at offset",
    ] {
        assert!(
            rendered.contains(needle),
            "`{query}` must report the marker refusal naming `{needle}`; got: {rendered}"
        );
    }
    assert!(
        rendered.contains("bound kind"),
        "the report must carry the parser's own cause (the unrepresentable bound \
         kind); got: {rendered}"
    );
}

/// Both live rows, in clustering order — the output the pre-fix swallow could not
/// produce for the patched copy (it returned `ck = 5` alone, as `Ok`).
fn assert_both_live_rows(rows: &[Vec<(String, String)>], query: &str) {
    assert_eq!(
        rows.len(),
        LIVE_CLUSTERING.len(),
        "`{query}` over the UNPATCHED fixture must return both live rows \
         (ck = {LIVE_CLUSTERING:?}, neither inside the deleted [10, 25] range); got: {rows:#?}"
    );
    for ck in LIVE_CLUSTERING {
        assert!(
            rows.iter().any(|row| {
                row.iter()
                    .any(|(k, v)| k == "ck" && v.contains(&ck.to_string()))
            }),
            "`{query}` must return the live row ck = {ck}; got: {rows:#?}"
        );
    }
}

/// One case: the query must SUCCEED with both live rows over the unpatched copy and
/// FAIL with the marker refusal over the patched copy.
async fn case(query: &str) {
    let clean = run(query, Patch::None)
        .await
        .unwrap_or_else(|e| panic!("`{query}` must succeed over the UNPATCHED fixture: {e}"));
    assert_both_live_rows(&clean, query);

    match run(query, Patch::UnrepresentableBoundKind).await {
        Ok(rows) => panic!(
            "`{query}` returned Ok over a fixture whose range-tombstone marker carries an \
             unrepresentable bound kind — the marker and every later row of the partition \
             were SWALLOWED (issue #3721). Rows: {rows:#?}"
        ),
        Err(e) => assert_marker_refusal(&e, query),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// D2 — `block_emit_windowed`: the full-scan and point/slice read path.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn d2_full_scan_surfaces_the_marker_refusal() {
    case(&format!("SELECT * FROM {KEYSPACE}.{TABLE}")).await;
}

#[tokio::test]
async fn d2_point_read_surfaces_the_marker_refusal() {
    case(&format!("SELECT * FROM {KEYSPACE}.{TABLE} WHERE id = 1")).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// D1 — `block_emit`: the WRITETIME/TTL cell-metadata scan
// (`ProjectionFlags::include_cell_metadata` -> `scan_with_cell_metadata` ->
// `parse_block_with_cell_metadata` -> `parse_block_emit_with_metadata`).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn d1_cell_metadata_scan_surfaces_the_marker_refusal() {
    case(&format!(
        "SELECT id, ck, v, WRITETIME(v) FROM {KEYSPACE}.{TABLE}"
    ))
    .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// D5 — `timestamp_policy`: the bounded STREAMING scan, whose marker refusal used
// to become `MarkerOutcome::Stop` -> `flush_and_emitted!(offset, false)` -> `Ok`
// with a truncated partition.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn d5_streaming_scan_surfaces_the_marker_refusal() {
    let query = format!("SELECT COUNT(*) FROM {KEYSPACE}.{TABLE}");
    let clean = run(&query, Patch::None)
        .await
        .unwrap_or_else(|e| panic!("`{query}` must succeed over the UNPATCHED fixture: {e}"));
    let rendered = format!("{clean:?}");
    assert!(
        rendered.contains(&LIVE_CLUSTERING.len().to_string()),
        "the UNPATCHED streaming aggregate must count both live rows; got: {clean:#?}"
    );

    match run(&query, Patch::UnrepresentableBoundKind).await {
        Ok(rows) => panic!(
            "`{query}` returned Ok over a fixture whose range-tombstone marker carries an \
             unrepresentable bound kind — the partition was silently TRUNCATED and the \
             count reported as success (issue #3721). Rows: {rows:#?}"
        ),
        Err(e) => assert_marker_refusal(&e, &query),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// D6 (issue #3808) — `compaction::CompactionPolicy`: the COMPACTION read path,
// whose unknown-bound-kind arm skipped the marker and returned
// `MarkerOutcome::Advanced`. This is the most consequential instance of the same
// swallow, because this policy's output is WRITTEN: dropping an unrepresentable
// deletion marker resurrects the rows it shadowed DURABLY, on disk.
//
// The fixture partition holds no rows inside its deleted `[10, 25]` clustering
// range, so the resurrection is made observable by compacting the fixture
// TOGETHER with a second, OLDER SSTable that reintroduces two covered rows
// (ck = 15, 20 at timestamp 1 µs — the real Cassandra tombstone is ~10^15 µs
// newer). Pre-fix, the patched input compacted to `Ok` with those rows present in
// the output; post-fix the compaction REFUSES and no output is adopted.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "write-support")]
mod d6_compaction {
    use std::path::{Path, PathBuf};

    use cqlite_core::schema::TableSchema;
    use cqlite_core::storage::write_engine::merge::{
        compact_sstables, KWayMerger, MergeStep, RowData,
    };
    use cqlite_core::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use cqlite_core::types::Value;

    use super::{
        assert_marker_refusal_text, fixture_table_schema as schema, stage, staged_data_db, Patch,
        KEYSPACE, LIVE_CLUSTERING, TABLE,
    };

    /// Clustering keys covered by the PATCHED marker's range and by NO other
    /// marker of the fixture — which is what makes the resurrection observable.
    ///
    /// The committed golden's marker sequence (`nb-3-big-Data.db.jsonl`) is:
    ///
    /// ```text
    /// bound     INCL_START [10]            deleted at 3000 µs   <-- the patched marker
    /// boundary  EXCL_END [15] / INCL_START [15]  (3000 / 3001 µs)
    /// bound     INCL_END   [25]            deleted at 3001 µs
    /// ```
    ///
    /// So the deletion is TWO ranges, `[10, 15)` and `[15, 25]`, and only the
    /// FIRST is opened by the marker this lane patches. Rows at `ck = 11, 12` are
    /// therefore covered by the patched range ALONE: skipping that marker leaves
    /// them unshadowed, while the surviving boundary/end markers still cover
    /// `[15, 25]`. (Picking a `ck` inside `[15, 25]` would have measured nothing —
    /// the sibling markers cover it with or without the fix.)
    const COVERED_CLUSTERING: [i32; 2] = [11, 12];
    /// The older input's write timestamp, in µs — older than BOTH range
    /// tombstones (3000 / 3001 µs) and than both live rows, so a correct
    /// compaction shadows these rows entirely.
    const OLDER_TS: i64 = 1;

    /// Write ONE CQLite SSTable holding the covered rows at [`OLDER_TS`], and
    /// return its `Data.db`.
    fn write_older_input(dir: &Path, schema: &TableSchema) -> PathBuf {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = dir.join("older");
        let mut engine = WriteEngine::new(WriteEngineConfig::new(
            data_dir.clone(),
            dir.join("older-wal"),
            schema.clone(),
        ))
        .expect("WriteEngine::new");
        for ck in COVERED_CLUSTERING {
            engine
                .write(Mutation::new(
                    TableId::new(KEYSPACE, TABLE),
                    PartitionKey::single("id", Value::Integer(1)),
                    Some(ClusteringKey::single("ck", Value::Integer(ck))),
                    vec![CellOperation::Write {
                        column: "v".to_string(),
                        value: Value::text(format!("covered-{ck}")),
                    }],
                    OLDER_TS,
                    None,
                ))
                .expect("write covered row");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("sstable info");
        rt.block_on(engine.close()).expect("close");

        fn find(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    find(&path, out);
                } else if path.to_string_lossy().ends_with("-Data.db") {
                    out.push(path);
                }
            }
        }
        let mut found = Vec::new();
        find(&data_dir, &mut found);
        assert_eq!(
            found.len(),
            1,
            "expected exactly one written input SSTable; got {found:#?}"
        );
        found.remove(0)
    }

    /// Read `data_path` back through the COMPACTION read contract: the surviving
    /// live clustering keys and the number of surviving range markers.
    fn read_back(data_path: PathBuf, schema: &TableSchema) -> (Vec<i32>, usize) {
        let mut merger = KWayMerger::new(vec![data_path], schema).expect("KWayMerger::new");
        let mut live = Vec::new();
        let mut markers = 0usize;
        loop {
            match merger.step().expect("merger step") {
                MergeStep::Complete => break,
                MergeStep::Partition { rows, .. } => {
                    for entry in rows {
                        if entry.range_deletion.is_some() {
                            markers += 1;
                            continue;
                        }
                        if let RowData::Live { cells } = &entry.row_data {
                            let has_data =
                                cells.iter().any(|c| c.column != "ck" && c.column != "id");
                            if !has_data {
                                continue;
                            }
                            if let Some(Value::Integer(ck)) = entry
                                .clustering_key
                                .as_ref()
                                .and_then(|k| k.columns.first().map(|(_, v)| v.clone()))
                            {
                                live.push(ck);
                            }
                        }
                    }
                }
            }
        }
        live.sort_unstable();
        (live, markers)
    }

    /// Compact the staged fixture (patched or not) together with the older input
    /// holding the covered rows. Returns the compaction outcome and the output
    /// directory, so a spurious `Ok` can be inspected for resurrected rows.
    fn compact_with_fixture(
        patch: Patch,
    ) -> (
        tempfile::TempDir,
        TableSchema,
        Result<PathBuf, cqlite_core::Error>,
    ) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_root = stage(tmp.path(), patch);
        let schema = schema();
        // Run index 0 = newest: the Cassandra fixture carries the tombstone.
        let inputs = vec![
            staged_data_db(&data_root),
            write_older_input(tmp.path(), &schema),
        ];
        let out_dir = tmp.path().join("out");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome = rt
            .block_on(compact_sstables(
                inputs, &out_dir, &schema, 9001, None, None, /* purge_safe */ true,
            ))
            .map(|report| report.output.data_path);
        (tmp, schema, outcome)
    }

    /// The CONTROL for BOTH defect cases below, and it is load-bearing three times
    /// over: a WELL-FORMED marker must still be paired and applied normally on the
    /// FINAL chunk (a fix that refused every marker, or every marker whose parse
    /// fails at the end of the data, would fail here), and the covered rows must be
    /// SHADOWED in the output — which is the property the patched halves prove
    /// cannot be silently lost.
    #[test]
    fn d6_recognised_bound_kind_still_compacts_and_shadows_the_covered_rows() {
        let (_tmp, schema, outcome) = compact_with_fixture(Patch::None);
        let output = outcome.expect("compaction over the UNPATCHED fixture must succeed");
        let (live, markers) = read_back(output, &schema);
        assert_eq!(
            live,
            LIVE_CLUSTERING.iter().map(|ck| *ck as i32).collect::<Vec<_>>(),
            "the compaction output must hold exactly the rows outside the deleted \
             [10, 15) + [15, 25] ranges — the covered rows {COVERED_CLUSTERING:?} from the older input must be \
             shadowed, and both live rows must survive"
        );
        assert!(
            markers > 0,
            "the surviving range marker must be PERSISTED to the output, or a later \
             compaction against a non-compacted SSTable resurrects the covered rows (#933)"
        );
    }

    /// The defect (#3808): an unrepresentable bound kind must make COMPACTION
    /// fail, so no output is ever adopted. Pre-fix it returned `Ok` and the
    /// output durably resurrected the covered rows.
    #[test]
    fn d6_compaction_refuses_an_unrepresentable_bound_kind_and_resurrects_nothing() {
        let (_tmp, schema, outcome) = compact_with_fixture(Patch::UnrepresentableBoundKind);
        match outcome {
            // Text only, not the category: see `assert_marker_refusal_text`.
            Err(e) => assert_marker_refusal_text(&e.to_string(), "compact_sstables"),
            Ok(output) => {
                let (live, markers) = read_back(output, &schema);
                let resurrected: Vec<i32> = live
                    .iter()
                    .copied()
                    .filter(|ck| COVERED_CLUSTERING.contains(ck))
                    .collect();
                panic!(
                    "compaction returned Ok over a fixture whose range-tombstone marker carries \
                     an unrepresentable bound kind: the marker was SKIPPED (issue #3808), so its \
                     output holds rows {resurrected:?} that the deletion covered — resurrected \
                     durably, on disk. Output rows: {live:?}, markers: {markers}"
                );
            }
        }
    }

    /// The defect (#3721, roborev job 16): a marker that cannot be PARSED on the
    /// FINAL chunk must make COMPACTION fail. Pre-fix the policy answered
    /// `MarkerOutcome::Stop`, which both compaction drivers convert on the final
    /// chunk into a SUCCESSFUL partition completion — so the tombstone was dropped,
    /// the output SSTable was still written, and the rows the tombstone covered came
    /// back durably, on disk.
    ///
    /// Asserted on the compaction OUTPUT rather than on the parser, because that is
    /// where the harm lands: an `Ok` here is only interesting for WHAT it wrote.
    ///
    /// The same bytes on a NON-final chunk must still be a refill request — that
    /// half cannot be observed through `compact_sstables` (it owns its own
    /// chunking), so it is pinned one level down against BOTH drivers in
    /// `data_access::compaction_range_marker_resume_tests`.
    #[test]
    fn d7_compaction_refuses_an_unparseable_marker_at_the_final_chunk_and_resurrects_nothing() {
        let (_tmp, schema, outcome) = compact_with_fixture(Patch::TruncatedAtMarker);
        match outcome {
            // Text only, not the category: see `assert_marker_refusal_text`.
            Err(e) => {
                let rendered = e.to_string();
                for needle in [
                    "range-tombstone marker",
                    "could not be PARSED",
                    "FINAL chunk",
                ] {
                    assert!(
                        rendered.contains(needle),
                        "the compaction refusal must name `{needle}`; got: {rendered}"
                    );
                }
                assert!(
                    rendered.contains("Failed to parse marker_body_size"),
                    "the report must PRESERVE the parser's own cause (the marker body it could \
                     not read past the end of the data), not a re-synthesised message; got: \
                     {rendered}"
                );
            }
            Ok(output) => {
                let (live, markers) = read_back(output, &schema);
                let resurrected: Vec<i32> = live
                    .iter()
                    .copied()
                    .filter(|ck| COVERED_CLUSTERING.contains(ck))
                    .collect();
                panic!(
                    "compaction returned Ok over a fixture whose FIRST range-tombstone marker \
                     cannot be parsed and whose data ENDS there: the marker was silently dropped \
                     and the partition reported complete (issue #3721), so the output holds rows \
                     {resurrected:?} that the deletion covered — resurrected durably, on disk. \
                     Output rows: {live:?}, markers: {markers}"
                );
            }
        }
    }
    /// The durable-harm level for the same two patches (#3721, roborev job 75):
    /// an end-to-end `compact_sstables` must REFUSE, so no output SSTable is
    /// adopted. `compact_sstables` drives the STREAMING reader driver through the
    /// merge producer, whose `MergeProducerError::to_error` flattens the cause
    /// into `Error::Storage(String)` — so the text is asserted and the category is
    /// not (see `assert_marker_refusal_text`).
    ///
    /// Asserted on the OUTPUT rather than on the parser because that is where the
    /// harm lands: pre-fix the parse returned `Ok` with a resume point one byte
    /// wrong, so what matters about an `Ok` here is WHAT it wrote.
    fn compaction_refuses_body_size(patch: Patch, cause: &str) {
        let (_tmp, schema, outcome) = compact_with_fixture(patch);
        match outcome {
            Err(e) => {
                let rendered = e.to_string();
                for needle in [
                    "range-tombstone marker",
                    "could not be PARSED",
                    "FINAL chunk",
                ] {
                    assert!(
                        rendered.contains(needle),
                        "the compaction refusal for {patch:?} must name `{needle}`; got: {rendered}"
                    );
                }
                assert!(
                    rendered.contains(cause),
                    "the compaction refusal for {patch:?} must PRESERVE the parser's own cause \
                     (`{cause}`); got: {rendered}"
                );
            }
            Ok(output) => {
                let (live, markers) = read_back(output, &schema);
                let resurrected: Vec<i32> = live
                    .iter()
                    .copied()
                    .filter(|ck| COVERED_CLUSTERING.contains(ck))
                    .collect();
                panic!(
                    "compaction returned Ok over a fixture whose first range-tombstone marker \
                     declares a `marker_body_size` disagreeing with its own field encodings \
                     ({patch:?}): the marker's deletion times and its resume point cannot both \
                     be right, and the output was still WRITTEN (issue #3721). Rows the \
                     deletion covered that came back: {resurrected:?}. Output rows: {live:?}, \
                     markers: {markers}"
                );
            }
        }
    }

    #[test]
    fn d8_compaction_refuses_a_body_size_corrupted_downward_and_resurrects_nothing() {
        compaction_refuses_body_size(
            Patch::MarkerBodySizeTooSmall,
            "Failed to parse localDeletionTime in marker body",
        );
    }

    #[test]
    fn d8_compaction_refuses_a_body_size_corrupted_upward_and_resurrects_nothing() {
        compaction_refuses_body_size(
            Patch::MarkerBodySizeTooLarge,
            "declared size and field encodings disagree",
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// D8 (issue #3721, roborev job 75) — the marker's own `marker_body_size` VUInt
// corrupted by ONE byte, in BOTH directions, over BOTH compaction read drivers.
//
// `parse_range_tombstone_marker_with_ldt` read the declared body size, then
// decoded `prev_unfiltered_size` and the deletion-time pair(s) from the WHOLE
// data slice — unbounded by the declared extent — and finally OVERWROTE its
// cursor with that extent. So the size and the field encodings were allowed to
// disagree silently, in either direction:
//
// * declared SHORT: the last field was read from the NEXT unfiltered's bytes and
//   the caller resumed INSIDE this marker;
// * declared LONG (still within the buffer, so the pre-existing
//   `vuint_length_within` guard cannot see it): the fields ended before the
//   declared extent and the caller resumed one byte PAST the next unfiltered's
//   flags byte.
//
// Both returned `Ok`. On this path that `Ok` is WRITTEN: a tombstone carrying
// timestamps that are not its own either fails to shadow the rows it covers
// (resurrection) or shadows rows it does not (data loss), durably on disk.
//
// The fix decodes the body within `&data[..body_end]` and requires that extent to
// be consumed EXACTLY. Each direction is therefore refused by a DIFFERENT half of
// the fix, and each case asserts the half it exercises by name:
//
// | patch | refused by | cause named |
// |-------|-----------|-------------|
// | too small | the bounded slice | `Failed to parse localDeletionTime in marker body` |
// | too large | the exact-consumption assert | `declared size and field encodings disagree` |
//
// ## Why these surfaces
//
// The two COMPACTION read drivers each own a copy of the marker decision and are
// both reached through `SSTableReader`'s public compaction API:
//
// * BUFFERED — `iterate_all_partitions_for_compaction` -> `parse_block_for_compaction`
//   -> `parse_one_partition_for_compaction` (used in production by the write
//   engine's sweep);
// * STREAMING — `stream_all_partitions_for_compaction` -> `stream_partition_body_incremental`
//   (the row-granular drain every k-way merge producer thread drives, so it is
//   also what `compact_sstables` below runs through).
//
// The ordinary `SELECT` path does NOT reach this parser: the read-side surfaces
// call `parse_range_tombstone_marker_full`, whose `_with_ldt` delegate is only
// reached through `CompactionPolicy::on_range_marker`. (`_full` does call
// `_with_ldt`, so a `SELECT` inherits the same bound — but it cannot be
// distinguished there: the size/encoding disagreement makes the marker
// unparseable, which the read paths already had to answer, so a `SELECT` case
// would be pinning `column_decode`/`end-of-partition` behaviour, not this fix.
// The refusal that IS specific to this fix is asserted where it is decided.)
//
// ## What pre-fix actually did — measured, not assumed
//
// Reverting ONLY the marker hunk (the sibling column-decode bound of the same
// commit left in place) makes all six defect cases below fail, and the way they
// fail is the point: the marker parse returns `Ok`, and the corruption then
// surfaces LATER, misattributed to a subject that has nothing to do with it.
//
// * too small — `column 'v' (column type text) failed to decode ... invalid cell
//   flags 0x87 ... offset misalignment`: the caller resumed INSIDE the marker, so
//   the marker's own trailing byte was read as the next unfiltered's row header and
//   the report blames a column of a row that does not exist;
// * too large — `marker_body_size=48 at pos=6 exceeds data length 7` for a marker
//   at a bogus partition: the caller resumed one byte PAST the next unfiltered's
//   flags byte, and the cascade re-emerges as an unrelated marker's size guard.
//
// So each case asserts the parser's own cause TEXT, not merely that an error
// occurred — the pre-fix cascade also errors here, and an error naming the wrong
// subject is what stops the next person looking. The `Ok` arm still panics, so a
// return to the fully silent behaviour fails these tests too.
//
// Both drivers here run at `at_final_chunk = true` over the whole data section,
// so an unparseable marker is corruption rather than a window boundary — the
// non-final half of that decision belongs to a different lane and is pinned in
// `data_access::compaction_range_marker_resume_tests`.
// ─────────────────────────────────────────────────────────────────────────────

mod d8_marker_body_size {
    use std::ops::ControlFlow;
    use std::path::Path;
    use std::sync::Arc;

    use cqlite_core::error::ErrorCategory;
    use cqlite_core::storage::scan_cancel::ScanCancel;
    use cqlite_core::storage::sstable::reader::compaction_row::{CompactionRow, CompactionRowData};
    use cqlite_core::storage::sstable::SSTableReader;
    use cqlite_core::{Config, Error, Platform};

    use super::{fixture_table_schema, stage, staged_data_db, Patch};

    /// Which compaction read driver a case drives. Named in every assertion
    /// message so a failure says WHICH copy of the decision broke.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Driver {
        /// `iterate_all_partitions_for_compaction` — whole data section decoded in
        /// one buffered pass.
        Buffered,
        /// `stream_all_partitions_for_compaction` — the row-granular sliding-window
        /// drain the merge producers use.
        Streaming,
    }

    impl Driver {
        fn name(self) -> &'static str {
            match self {
                Driver::Buffered => "iterate_all_partitions_for_compaction (buffered)",
                Driver::Streaming => "stream_all_partitions_for_compaction (streaming)",
            }
        }
    }

    /// Open a reader over the staged copy `stage` wrote (patched or not) and
    /// confirm it is the `nb` generation this lane hand-decodes.
    async fn open_staged(data_root: &Path) -> Arc<SSTableReader> {
        let data_db = staged_data_db(data_root);
        let cfg = Config::default();
        let platform = Arc::new(Platform::new(&cfg).await.expect("platform"));
        let reader = SSTableReader::open(&data_db, &cfg, platform)
            .await
            .expect("the staged fixture copy must open (its CRC.db is re-sealed)");
        assert_eq!(
            reader.format_version().expect("format version"),
            "nb",
            "the hand decode in this lane's module docs is of the `nb` fixture"
        );
        Arc::new(reader)
    }

    /// Run `driver` over a staged copy of the fixture carrying `patch`.
    async fn run(driver: Driver, patch: Patch) -> Result<Vec<CompactionRow>, Error> {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_root = stage(tmp.path(), patch);
        let reader = open_staged(&data_root).await;
        let schema = fixture_table_schema();
        match driver {
            Driver::Buffered => {
                reader
                    .iterate_all_partitions_for_compaction(Some(&schema))
                    .await
            }
            Driver::Streaming => {
                let cancel = ScanCancel::default();
                let mut rows = Vec::new();
                reader
                    .stream_all_partitions_for_compaction(Some(&schema), &cancel, |row| {
                        rows.push(row);
                        Ok(ControlFlow::Continue(()))
                    })
                    .await?;
                Ok(rows)
            }
        }
    }

    /// How many of `rows` are range-tombstone markers.
    fn markers(rows: &[CompactionRow]) -> usize {
        rows.iter()
            .filter(|r| matches!(r.row_data, CompactionRowData::RangeMarker { .. }))
            .count()
    }

    /// The CONTROL, and it is load-bearing twice over: it proves the staged copy
    /// is readable at all through this driver (so a refusal below is attributable
    /// to the ONE patched byte and not to the staging or the re-sealed `CRC.db`),
    /// and it proves the driver really does reach the marker parser — without a
    /// non-zero marker count the patched halves could be refusing for any reason.
    async fn control(driver: Driver) {
        let rows = run(driver, Patch::None).await.unwrap_or_else(|e| {
            panic!(
                "{} over the UNPATCHED fixture must succeed: {e}",
                driver.name()
            )
        });
        assert!(
            markers(&rows) >= 1,
            "{} must decode at least one range-tombstone marker from the UNPATCHED fixture, \
             or the patched cases below prove nothing about the marker parser; got {} rows: \
             {rows:#?}",
            driver.name(),
            rows.len()
        );
    }

    /// The parser's own cause each patch direction must produce — the half of the
    /// fix that direction exercises, asserted by name so the two cases cannot pass
    /// on each other's refusal.
    fn expected_cause(patch: Patch) -> &'static str {
        match patch {
            // The body slice ends one byte early, so the LAST field of the
            // deletion-time pair runs out of bytes.
            Patch::MarkerBodySizeTooSmall => "Failed to parse localDeletionTime in marker body",
            // The fields end one byte before the declared extent, which only the
            // exact-consumption assert can see.
            Patch::MarkerBodySizeTooLarge => "declared size and field encodings disagree",
            other => panic!("{other:?} is not a marker_body_size patch"),
        }
    }

    /// One defect case: the driver must FAIL, naming the marker, the final-chunk
    /// decision and the parser's own cause for THIS direction.
    async fn case(driver: Driver, patch: Patch) {
        control(driver).await;

        match run(driver, patch).await {
            Ok(rows) => panic!(
                "{} returned Ok over a fixture whose first range-tombstone marker declares a \
                 `marker_body_size` that DISAGREES with its own field encodings ({patch:?}). \
                 The declared size and the encodings are both authoritative and they \
                 contradict each other, so the deletion times and the resume point cannot \
                 both be right — and this decode feeds WRITTEN compaction output (issue \
                 #3721). Rows: {} ({} markers): {rows:#?}",
                driver.name(),
                rows.len(),
                markers(&rows)
            ),
            Err(e) => {
                let rendered = e.to_string();
                for needle in [
                    "range-tombstone marker",
                    "could not be PARSED",
                    "FINAL chunk",
                ] {
                    assert!(
                        rendered.contains(needle),
                        "{}'s refusal for {patch:?} must name `{needle}`; got: {rendered}",
                        driver.name()
                    );
                }
                let cause = expected_cause(patch);
                assert!(
                    rendered.contains(cause),
                    "{}'s refusal for {patch:?} must PRESERVE the parser's own cause (`{cause}`) \
                     rather than a re-synthesised message — that text is what names WHICH half \
                     of the size/encoding disagreement fired; got: {rendered}",
                    driver.name()
                );
                assert_eq!(
                    e.category(),
                    ErrorCategory::Data,
                    "a marker whose declared body size contradicts its own encodings is \
                     damaged data ({}, {patch:?})",
                    driver.name()
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffered_refuses_a_body_size_corrupted_downward() {
        case(Driver::Buffered, Patch::MarkerBodySizeTooSmall).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_refuses_a_body_size_corrupted_downward() {
        case(Driver::Streaming, Patch::MarkerBodySizeTooSmall).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffered_refuses_a_body_size_corrupted_upward_within_the_buffer() {
        case(Driver::Buffered, Patch::MarkerBodySizeTooLarge).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_refuses_a_body_size_corrupted_upward_within_the_buffer() {
        case(Driver::Streaming, Patch::MarkerBodySizeTooLarge).await;
    }

    /// The control on its own, as its own test: a well-formed marker must still
    /// decode through BOTH drivers. A fix that refused every marker — or every
    /// marker at the end of the data — would fail HERE while leaving the four
    /// cases above green.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_drivers_still_decode_the_unpatched_marker() {
        control(Driver::Buffered).await;
        control(Driver::Streaming).await;
    }
}
