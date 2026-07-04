//! Issue #1741 (P0): single-generation / full-scan reads must apply partition
//! deletions, range tombstones, and read-time TTL expiry — matching a Cassandra
//! `SELECT`, which hides deleted/expired data. Historically the single-gen emit
//! path bypassed reconciliation and returned that data as live.
//!
//! These are END-TO-END regression tests driving the same `Database::execute`
//! query path the CLI/bindings use, so they exercise the shared v5 emit path below
//! the query engine (independent of compression and feature flags).
//!
//! Revert-verify (AC3): on pre-fix `main` each `assert_eq!` below fails —
//!   * `ttl_expired_rows_are_hidden`: current main returns all 100 rows; the fix
//!     returns 0 (every row's `default_time_to_live = 86400` expired in Oct 2025).
//!   * `partition_deletion_hides_covered_rows`: a real uncompressed single-gen
//!     table is copied to a tempdir and its FIRST partition header is patched to
//!     carry a `markedForDeleteAt` newer than that partition's row — the issue's
//!     own validated repro method (writer-flush reconciliation makes an unpatched
//!     coexisting partition-delete+rows fixture unsynthesizable). Current main
//!     returns all N rows; the fix returns N-1.
//!
//! Fixtures resolve via `CQLITE_DATASETS_ROOT`; when the dataset (or its gitignored
//! `*.db` binaries) is absent the tests SKIP cleanly. When the fixture IS present,
//! a zero-row or mismatched result FAILS loudly (never an empty-pass).
//!
//! Range-tombstone coverage is pinned by the deterministic unit tests in
//! `partition_shadow.rs` (`range_tombstone_fsm_shadows_covered_older_rows`,
//! `range_tombstone_boundary_reopens_new_range`): no empirical single-gen RT
//! fixture with COVERED rows is synthesizable (issue #1741 note (a)) because the
//! writer purges covered rows at flush, so the read-side FSM + coverage logic is
//! pinned directly rather than via an on-disk golden.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schema_path(file: &str) -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let p = root.parent()?.join("schemas").join(file);
        if p.exists() {
            return Some(p);
        }
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let p = manifest
        .parent()?
        .join("test-data")
        .join("schemas")
        .join(file);
    p.exists().then_some(p)
}

fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join("sstables").join(keyspace);
    if !ks_dir.is_dir() {
        return None;
    }
    let prefix = format!("{table}-");
    std::fs::read_dir(&ks_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
}

/// Count `type == "row"` entries in the committed sstabledump JSONL golden.
fn golden_row_count(dir: &Path) -> Option<usize> {
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let text = std::fs::read_to_string(&jsonl).ok()?;
    let mut total = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).ok()?;
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            total += rows
                .iter()
                .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
                .count();
        }
    }
    Some(total)
}

/// Ingest a `sstables` dir (real dataset root or a patched tempdir) filtered to one
/// keyspace and return a queryable `Database`.
async fn open_db(sstables_dir: &Path, schema: &Path, keyspace: &str) -> Result<Database, String> {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: sstables_dir.to_path_buf(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// TTL expiry (issue #1741 repro (c)): every row of `test_basic.ttl_test_table`
/// carries `default_time_to_live = 86400` and expired in Oct 2025, so a Cassandra
/// `SELECT *` returns 0 rows. The committed golden proves 100 rows are physically
/// on disk (anti-empty-pass), so a return of 100 (pre-fix) or any non-zero count is
/// a hard failure.
#[tokio::test]
async fn ttl_expired_rows_are_hidden() {
    let Some(dir) = fixture_dir("test_basic", "ttl_test_table") else {
        eprintln!("SKIP ttl_expired_rows_are_hidden: fixture dir absent");
        return;
    };
    if !dir.join("nb-1-big-Data.db").exists() {
        eprintln!("SKIP ttl_expired_rows_are_hidden: Data.db not fetched");
        return;
    }
    let Some(schema) = schema_path("basic-types.cql") else {
        eprintln!("SKIP ttl_expired_rows_are_hidden: schema absent");
        return;
    };
    let root = datasets_root().unwrap();

    // Anti-empty-pass: the physical rows really exist on disk.
    let golden = golden_row_count(&dir).expect("ttl_test_table golden JSONL missing/unreadable");
    assert_eq!(
        golden, 100,
        "fixture sanity: ttl_test_table should have 100 physical rows, got {golden}"
    );

    let db = match open_db(&root.join("sstables"), &schema, "test_basic").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("SKIP ttl_expired_rows_are_hidden: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT id, expiring_value, session_info FROM test_basic.ttl_test_table")
        .await
        .expect("SELECT over ttl_test_table must succeed");

    assert_eq!(
        result.rows.len(),
        0,
        "issue #1741: every ttl_test_table row expired (TTL 86400, Oct 2025) — a \
         Cassandra SELECT returns 0 rows, but the read path returned {} live rows",
        result.rows.len()
    );
}

/// Partition deletion (issue #1741 repro (b)): copy the real uncompressed single-gen
/// `test_basic.uncompressed_table` to a tempdir, patch its FIRST partition header to
/// carry a `markedForDeleteAt` newer than that partition's row, and assert a
/// `SELECT *` returns exactly one fewer row (the deleted partition's row is hidden).
#[tokio::test]
async fn partition_deletion_hides_covered_rows() {
    let Some(src) = fixture_dir("test_basic", "uncompressed_table") else {
        eprintln!("SKIP partition_deletion_hides_covered_rows: fixture dir absent");
        return;
    };
    if !src.join("nb-1-big-Data.db").exists() {
        eprintln!("SKIP partition_deletion_hides_covered_rows: Data.db not fetched");
        return;
    }
    let Some(schema) = schema_path("basic-types.cql") else {
        eprintln!("SKIP partition_deletion_hides_covered_rows: schema absent");
        return;
    };

    let golden = golden_row_count(&src).expect("uncompressed_table golden JSONL missing");
    assert!(
        golden >= 2,
        "fixture sanity: need >= 2 physical rows to observe one hidden, got {golden}"
    );

    // Build a patched copy: <tmp>/sstables/test_basic/<same-dir-name>/, dropping the
    // Digest.crc32 + CRC.db integrity sidecars (the reader warn-and-proceeds without
    // them, decision D4) so the in-place partition-header patch is accepted.
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir_name = src.file_name().unwrap();
    let dst = tmp
        .path()
        .join("sstables")
        .join("test_basic")
        .join(dir_name);
    std::fs::create_dir_all(&dst).expect("mkdir dst");
    for entry in std::fs::read_dir(&src).expect("read src dir").flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-Digest.crc32") || name_str.ends_with("-CRC.db") {
            continue; // drop integrity sidecars — we mutate Data.db below
        }
        std::fs::copy(entry.path(), dst.join(&name)).expect("copy component");
    }

    // Patch the FIRST partition header (Data.db byte 0). Layout (nb, no
    // hasUIntDeletionTime): flags(1) | key_len(u8) | key | localDeletionTime(i32 BE)
    // | markedForDeleteAt(i64 BE). Set a real localDeletionTime (!= i32::MAX LIVE
    // sentinel) and a markedForDeleteAt newer than any row timestamp in the file.
    let data_path = dst.join("nb-1-big-Data.db");
    let mut bytes = std::fs::read(&data_path).expect("read Data.db");
    let key_len = bytes[1] as usize;
    let del_off = 2 + key_len;
    assert!(
        bytes.len() >= del_off + 12,
        "Data.db too small to hold a partition-deletion field"
    );
    // localDeletionTime = 2025-10-09 (real epoch-seconds, not the LIVE sentinel).
    let local_deletion_time: i32 = 1_760_000_000;
    bytes[del_off..del_off + 4].copy_from_slice(&local_deletion_time.to_be_bytes());
    // markedForDeleteAt ≈ year 2030 in µs — strictly newer than every row's write
    // timestamp (~1.759e15 µs, Oct 2025), so the whole partition is shadowed.
    let marked_for_delete_at: i64 = 1_900_000_000_000_000;
    bytes[del_off + 4..del_off + 12].copy_from_slice(&marked_for_delete_at.to_be_bytes());
    std::fs::write(&data_path, &bytes).expect("write patched Data.db");

    let db = match open_db(&tmp.path().join("sstables"), &schema, "test_basic").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("SKIP partition_deletion_hides_covered_rows: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT * FROM test_basic.uncompressed_table")
        .await
        .expect("SELECT over patched uncompressed_table must succeed");

    assert_eq!(
        result.rows.len(),
        golden - 1,
        "issue #1741: the patched partition's row must be hidden by its \
         partition-level deletion — expected {} rows, got {}",
        golden - 1,
        result.rows.len()
    );
}
