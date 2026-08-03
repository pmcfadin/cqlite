//! Issue #3220: the TABLE-granular fixture-root selection rule, pinned against
//! synthetic roots.
//!
//! # What defect this exists to catch
//!
//! Three dataset lanes selected a corpus root by **keyspace** (`root.join(keyspace)
//! .is_dir()`) and then committed to it with no fallback, even though every caller
//! needs a specific **table**. On a machine whose `CQLITE_DATASETS_ROOT` corpus held
//! `test_da/` but not the git-committed `test_da/multiclustering_table-*`, that rule
//! selected the env root, missed the table, and the #3032 multi-component clustering
//! differential case SKIPPED SILENTLY behind a green suite.
//!
//! # Why the assertions are against SYNTHETIC roots
//!
//! The real candidate list is half environment and half a COMPILE-TIME checkout path,
//! so a test that reads it can only ever observe THIS machine's layout — and on a
//! machine where every root happens to hold every table, the broken keyspace-granular
//! rule and the correct table-granular one return the same answer. Only an explicit
//! two-root layout where the FIRST root holds the keyspace WITHOUT the table
//! discriminates between them, which is exactly the layout built below. Hence the
//! pure `first_root_with_table` seam.
//!
//! These are directory-shape assertions, not format assertions: no SSTable is parsed
//! here, so an empty file named `*-Data.db` is a legitimate stand-in for "the binaries
//! are present" (whether they DECODE is the lanes' own business, and a truncated one
//! makes them fail loudly — the second AC2 direction, exercised end-to-end in the PR).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{first_root_with_table, table_has_data};
use std::path::{Path, PathBuf};

/// Create `<root>/<keyspace>/<table>-<uuid>/` and populate it with `files`.
fn make_table_dir(root: &Path, keyspace: &str, table: &str, uuid: &str, files: &[&str]) -> PathBuf {
    let dir = root.join(keyspace).join(format!("{table}-{uuid}"));
    std::fs::create_dir_all(&dir).expect("create fixture dir");
    for f in files {
        std::fs::write(dir.join(f), b"").expect("write fixture component");
    }
    dir
}

/// THE #3220 case: candidate 1 holds the KEYSPACE but not the TABLE. The rule must
/// fall through to candidate 2, which actually carries the table's `*-Data.db`.
///
/// Under the retired keyspace-granular rule candidate 1 wins and the table is then
/// reported absent — the silent-skip defect.
#[test]
fn a_root_holding_the_keyspace_without_the_table_is_skipped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let env_root = tmp.path().join("env/sstables");
    let checkout_root = tmp.path().join("checkout/sstables");

    // Candidate 1: the keyspace exists (another table lives there), the wanted table
    // does not — the real /data/datasets shape that triggered #3220.
    make_table_dir(
        &env_root,
        "test_da",
        "wide_table",
        "aaaa",
        &["da-2-bti-Data.db"],
    );
    // Candidate 2: the committed fixture.
    make_table_dir(
        &checkout_root,
        "test_da",
        "multiclustering_table",
        "bbbb",
        &["da-2-bti-Data.db"],
    );

    let roots = vec![env_root.clone(), checkout_root.clone()];
    assert!(
        env_root.join("test_da").is_dir(),
        "precondition: candidate 1 must hold the keyspace, else this test cannot \
         discriminate the keyspace-granular rule from the table-granular one"
    );
    assert_eq!(
        first_root_with_table(&roots, "test_da", "multiclustering_table"),
        Some(checkout_root.as_path()),
        "resolution must fall through a root that holds the keyspace but not the table"
    );
    // The sibling table still resolves from the FIRST root: preference order is
    // preserved, the rule only skips roots that cannot serve the request.
    assert_eq!(
        first_root_with_table(&roots, "test_da", "wide_table"),
        Some(env_root.as_path()),
        "candidate order must still win when the first root does hold the table"
    );
}

/// Fixture-absent direction of the fail-closed contract (AC2a): when NO candidate
/// carries the table, resolution yields `None` — the value every caller turns into a
/// hard failure (`must_run` / `CQLITE_REQUIRE_FIXTURES`), never a silent success.
#[test]
fn absent_everywhere_resolves_to_none() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root_a = tmp.path().join("a/sstables");
    let root_b = tmp.path().join("b/sstables");
    make_table_dir(
        &root_a,
        "test_da",
        "wide_table",
        "aaaa",
        &["da-2-bti-Data.db"],
    );
    std::fs::create_dir_all(root_b.join("test_da")).expect("empty keyspace dir");

    let roots = vec![root_a, root_b];
    assert_eq!(
        first_root_with_table(&roots, "test_da", "multiclustering_table"),
        None,
        "a table no candidate root carries must resolve to None, so the caller fails closed"
    );
}

/// A `<table>-<uuid>/` directory holding only the committed SIDECARS (the
/// `*-Data.db.jsonl` physical golden, `*-Statistics.db.txt`) is NOT usable: the repo
/// commits those for fixtures whose binaries are gitignored. Presence must be judged
/// on a real `*-Data.db` component, else a lane would select a root it cannot read
/// from and report the fixture as fetched.
#[test]
fn a_sidecar_only_directory_does_not_count_as_present() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sidecar_root = tmp.path().join("sidecar/sstables");
    let real_root = tmp.path().join("real/sstables");
    make_table_dir(
        &sidecar_root,
        "test_tomb",
        "resurrection_gc0",
        "aaaa",
        &[
            "nb-1-big-Data.db.jsonl",
            "nb-1-big-Statistics.db.txt",
            "nb-1-big-TOC.txt",
        ],
    );
    make_table_dir(
        &real_root,
        "test_tomb",
        "resurrection_gc0",
        "bbbb",
        &["nb-1-big-Data.db"],
    );

    assert!(
        !table_has_data(&sidecar_root, "test_tomb", "resurrection_gc0"),
        "a sidecar-only directory must not be reported as carrying fetched binaries"
    );
    let roots = vec![sidecar_root, real_root.clone()];
    assert_eq!(
        first_root_with_table(&roots, "test_tomb", "resurrection_gc0"),
        Some(real_root.as_path())
    );
}

/// `<table>-` is a PREFIX match, so a longer table name sharing the prefix must not
/// satisfy a request for the shorter one (`wide_table` vs `wide_table_v2`).
#[test]
fn a_prefix_sharing_sibling_table_does_not_satisfy_the_request() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("sstables");
    make_table_dir(
        &root,
        "test_da",
        "wide_table_v2",
        "aaaa",
        &["da-2-bti-Data.db"],
    );

    assert!(
        !table_has_data(&root, "test_da", "wide_table"),
        "`wide_table_v2-<uuid>` must not answer a request for `wide_table`"
    );
    assert!(table_has_data(&root, "test_da", "wide_table_v2"));
}
