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

/// Copy the fixture into `dir` as `<keyspace>/<table>-*/…`, optionally patching the
/// marker's bound-kind byte and re-sealing `CRC.db` over the result. The committed
/// fixture is never written to.
fn stage(dir: &Path, patch_bound_kind: bool) -> PathBuf {
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
    let mut patched = original.clone();
    if patch_bound_kind {
        patched[BOUND_KIND_OFFSET] = BOUND_KIND_UNREPRESENTABLE;
    }

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
async fn run(query: &str, patch_bound_kind: bool) -> Result<Vec<Vec<(String, String)>>, Error> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_root = stage(tmp.path(), patch_bound_kind);
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
    let rendered = err.to_string();
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
        "the report must carry the shadow FSM's own cause (the unrepresentable bound \
         kind); got: {rendered}"
    );
    assert_eq!(
        err.category(),
        ErrorCategory::Data,
        "an unrepresentable on-disk marker classifies as damaged data"
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
    let clean = run(query, false)
        .await
        .unwrap_or_else(|e| panic!("`{query}` must succeed over the UNPATCHED fixture: {e}"));
    assert_both_live_rows(&clean, query);

    match run(query, true).await {
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
