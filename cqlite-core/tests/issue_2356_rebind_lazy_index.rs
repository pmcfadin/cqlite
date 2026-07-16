//! Issue #2356 roborev (Rust side) — a #2383 warm inode-rebind must follow the
//! LAZY `Index.db` path, not only `Data.db`.
//!
//! Under #2412's lazy Summary-guided BIG open, `SSTableReader` defers the
//! `Index.db` parse: the point-read path resolves a partition through ONE
//! `Summary.db`-bounded `Index.db` interval, opening the `Index.db` file fresh
//! at read time. The warm rebind (`SSTableReader::rebind_path`) originally
//! repointed ONLY the `Data.db` `ArcSwap`, leaving the lazy `IndexReader` pinned
//! to its open-time snapshot path. So when that snapshot dir is torn down and the
//! reader is rebound to a fresh same-generation dir, a NOT-YET-MATERIALIZED reader
//! would `File::open` the DEAD `Index.db` path on the next point read → ENOENT
//! (the #2352 class, for the dominant point-read shape).
//!
//! This pins the fix at the reader public surface: open lazily from snapshot dir
//! A, tear A down, rebind to a fresh dir B, and a point read must SUCCEED (open
//! B's `Index.db`), not error on the dead A path. Fails on the Data.db-only rebind.
//!
//! Run with:
//! ```text
//! CQLITE_DATASETS_ROOT=<datasets> cargo test -p cqlite-core \
//!   --test issue_2356_rebind_lazy_index
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Config;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// A `*-Data.db` in `<datasets>/sstables/<keyspace>/<table>-*/` with sibling
/// `*-Index.db` AND `*-Summary.db` (a BIG SSTable with a usable summary → lazy open).
fn find_big_data_file_with_summary(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let dir = e.path();
        let files = std::fs::read_dir(&dir).ok()?;
        let mut data_file: Option<PathBuf> = None;
        let (mut has_index, mut has_summary) = (false, false);
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                data_file = Some(f.path());
            } else if name.ends_with("-Index.db") {
                has_index = true;
            } else if name.ends_with("-Summary.db") {
                has_summary = true;
            }
        }
        if has_index && has_summary {
            if let Some(df) = data_file {
                return Some(df);
            }
        }
    }
    None
}

/// Copy every component of `src_dir` into a fresh unique temp dir and return the
/// copied `*-Data.db` path. Byte-identical copies stand in for the same-inode
/// hardlinks a real snapshot dir would carry; the reader-level rebind trusts the
/// caller's identity proof, so a copy is sufficient to exercise path-following.
fn copy_sstable_dir(src_dir: &Path, tag: &str) -> PathBuf {
    let tmp = std::env::temp_dir().join(format!(
        "cqlite-2356-rebind-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp).expect("mkdir temp snapshot dir");
    let mut data_path = None;
    for entry in std::fs::read_dir(src_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        let dest = tmp.join(&name);
        std::fs::copy(entry.path(), &dest).expect("copy component");
        if name.ends_with("-Data.db") {
            data_path = Some(dest);
        }
    }
    data_path.expect("copied snapshot dir must include a Data.db")
}

#[test]
fn point_read_after_rebind_follows_lazy_index_to_the_live_snapshot_dir() {
    let Some(src_data) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!(
            "Skipping (#2356 rebind lazy Index.db): BIG test_basic/simple_table fixture \
             (with Summary.db) absent"
        );
        return;
    };
    let src_dir = src_data.parent().expect("fixture has a parent dir");

    // Snapshot dir A: the reader opens from here (lazy). Dir B: the fresh
    // same-generation snapshot the warm rebind repoints to.
    let dir_a_data = copy_sstable_dir(src_dir, "a");
    let dir_b_data = copy_sstable_dir(src_dir, "b");
    let dir_a = dir_a_data.parent().expect("dir A parent").to_path_buf();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    let reader = rt
        .block_on(SSTableReader::open(&dir_a_data, &config, platform))
        .expect("BIG fixture with Summary.db must open");

    // Precondition: the reader is genuinely LAZY (Index.db deferred, not yet
    // parsed) — otherwise this would not exercise the deferred-open path.
    assert!(
        !reader.index_is_materialized(),
        "a BIG open with a usable Summary.db must be lazy (Index.db deferred, #2412)"
    );

    // Tear the ORIGINAL snapshot dir down: A's Index.db path is now dead. The
    // reader's already-open Data.db handle survives (Unix unlink semantics), but a
    // deferred fresh Index.db `File::open` on the A path would now ENOENT.
    std::fs::remove_dir_all(&dir_a).expect("tear down snapshot dir A");

    // Warm inode-rebind to the fresh dir B (the fix repoints BOTH Data.db AND the
    // lazy Index.db path to B's hardlink siblings).
    reader.rebind_path(&dir_b_data);

    // A point read now resolves through the Summary-guided bounded Index.db
    // interval, which opens the Index.db file fresh. The key content is irrelevant
    // (a miss is fine) — what matters is that the open targets the LIVE dir B, not
    // the dead dir A. Pre-fix (Data.db-only rebind) this ENOENTs on A/Index.db.
    let key: &[u8] = &[0x00, 0x00, 0x00, 0x01];
    let result = rt.block_on(reader.lookup_partition_with_index(key));
    assert!(
        result.is_ok(),
        "a point read after a rebind must open the LIVE snapshot's Index.db, not the \
         torn-down open-time path (issue #2356 roborev, #2352 class); got {result:?}"
    );

    let _ = std::fs::remove_dir_all(dir_b_data.parent().expect("dir B parent"));
}
