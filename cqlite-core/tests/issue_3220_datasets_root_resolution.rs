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

//! DELIBERATELY UNGATED by cargo features: this target parses no SSTable and calls
//! no feature-gated API, so a `#![cfg(all(feature = "state_machine", feature =
//! "cli-helpers"))]` (copied from the lanes that DO need them) would compile the only
//! pins on the resolution rule away under a plain `cargo test -p cqlite-core` — a
//! rule guarded by nothing in the very default build most developers run.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{
    first_root_with_table, sstables_root_candidates, table_generation_dirs, table_has_data,
    CHECKOUT_SSTABLES_ROOT_OVERRIDE_ENV,
};
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

/// The CHECKOUT corpus must ALWAYS be a candidate root — the property the whole
/// fail-closed contract rests on: a `must_run` case may assert unconditionally only
/// because a committed fixture is reachable whatever `CQLITE_DATASETS_ROOT` names.
/// Drop the checkout from the candidate list (say, by "preferring" the env root) and
/// every committed-fixture guard in the sibling lanes turns into a false failure.
///
/// NON-RACY: it mutates nothing. The override seam is read ONCE into a local (a second
/// `var_os` could observe a different value if a sibling test wrote the environment),
/// and the expectation is derived from that single observation — so this test neither
/// sets a process-global nor depends on another test's timing.
#[test]
fn the_checkout_corpus_is_always_a_candidate_root() {
    let override_root = match std::env::var_os(CHECKOUT_SSTABLES_ROOT_OVERRIDE_ENV) {
        Some(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => None,
    };
    let expected = override_root.clone().unwrap_or_else(|| {
        datasets_root::fixture_roots::checkout_test_data_dir()
            .join("datasets")
            .join("sstables")
    });

    let candidates = sstables_root_candidates();
    assert!(
        !candidates.is_empty(),
        "the candidate list must never be empty: the checkout root is unconditional"
    );
    assert!(
        candidates.contains(&expected),
        "the checkout corpus {} must always be a candidate root, whatever \
         CQLITE_DATASETS_ROOT names; got {candidates:?}",
        expected.display()
    );
    assert_eq!(
        candidates.last(),
        Some(&expected),
        "the checkout root is the LAST candidate: an env corpus that can serve the request \
         wins the tie, and the checkout is the always-present fallback"
    );
    // Deduplicated: a CQLITE_DATASETS_ROOT that already points at the checkout must not
    // report the same path twice.
    let mut deduped = candidates.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        candidates.len(),
        "candidate roots must be deduplicated: {candidates:?}"
    );

    // And it is not merely a path: with no override in force, the checkout candidate
    // really carries a git-committed fixture, which is what makes `must_run: true`
    // assertable unconditionally in the sibling lanes. (Skipped when the harness
    // override is in force — that seam exists precisely to hide fixtures.)
    if override_root.is_none() {
        assert!(
            table_has_data(&expected, "test_da", "multiclustering_table"),
            "the checkout candidate {} must carry the committed #3032 fixture; remedy: \
             git restore --source=HEAD -- test-data/datasets/sstables",
            expected.display()
        );
    }
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

/// Issue #3782 (roborev job 57 finding 2) — GENERATION selection must be
/// deterministic AND must require the component, not take the first `read_dir`
/// hit.
///
/// `read_dir` order is unspecified, so a lane that takes "the first `<table>-*`
/// directory" is nondeterministic as soon as a table has more than one
/// generation directory — and if it also does not require a `*-Data.db` it can
/// bind to a sidecar-only generation and fail on one machine while passing on
/// another with the same bytes on disk. That pairing is live in this corpus: the
/// checkout's `test_basic/composite_key_table-…/` holds sidecars only while the
/// fetched root's copy is complete.
///
/// Synthetic roots, because the real layout can only ever show THIS machine's
/// directory order — the same reason [`first_root_with_table`] is tested this way.
#[test]
fn generation_selection_is_deterministic_and_skips_a_sidecar_only_generation() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("sstables");
    // Created in an order that is NOT the sorted order, and with the
    // sidecar-only generation created first — the shape that makes an
    // order-dependent selection pick the unusable directory.
    let sidecar_only = make_table_dir(
        &root,
        "test_basic",
        "composite_key_table",
        "0000sidecar",
        &["nb-1-big-Data.db.jsonl", "nb-1-big-TOC.txt"],
    );
    let later = make_table_dir(
        &root,
        "test_basic",
        "composite_key_table",
        "cccc",
        &["nb-1-big-Data.db"],
    );
    let earlier = make_table_dir(
        &root,
        "test_basic",
        "composite_key_table",
        "bbbb",
        &["nb-1-big-Data.db"],
    );

    let dirs = table_generation_dirs(&root, "test_basic", "composite_key_table");
    assert!(
        !dirs.contains(&sidecar_only),
        "a generation directory with no *-Data.db is not usable and must not be \
         offered: {dirs:?}"
    );
    // SORTED, whatever order the directories were created in — the whole point.
    assert_eq!(
        dirs,
        vec![earlier.clone(), later],
        "generation directories must be returned in sorted order, never read_dir order"
    );
    // Which one a single-fixture lane takes: the FIRST of that defined order.
    assert_eq!(dirs.first(), Some(&earlier));
}

/// The fail-closed half: a table whose only generation directory carries
/// sidecars has NO usable generation, and that must be reported as absence — the
/// loud named failure every #3782 caller turns into a panic — never as a
/// directory a lane then tries to read.
#[test]
fn a_table_whose_only_generation_is_sidecar_only_has_no_usable_generation() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("sstables");
    make_table_dir(
        &root,
        "test_basic",
        "composite_key_table",
        "aaaa",
        &["nb-1-big-Data.db.jsonl", "nb-1-big-Statistics.db.txt"],
    );
    assert!(
        table_generation_dirs(&root, "test_basic", "composite_key_table").is_empty(),
        "a sidecar-only table must offer no usable generation directory"
    );
    assert!(!table_has_data(&root, "test_basic", "composite_key_table"));
}
