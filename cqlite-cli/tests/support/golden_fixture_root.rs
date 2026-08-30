//! WHICH root supplies a case's fixture — and why a git-committed case must be
//! served from the CHECKOUT (issue #1491 review finding J1).
//!
//! # The defect this module removes
//!
//! The lane used to resolve every case's fixture through the shared #3220
//! resolver [`super::datasets_root::sstables_root_for_table`], which walks the
//! candidate roots in order — `CQLITE_DATASETS_ROOT` first, then the checkout — and
//! returns the first that carries the table. That rule is right for a fetched-corpus
//! fixture and WRONG for a committed one: on this fleet `/data/datasets/sstables`
//! carries its own copy of most committed tables, so the lane compared an EXTERNAL
//! copy — possibly stale, regenerated, or simply a different generation — while the
//! census still reported the git-committed table as covered. A regression specific
//! to the committed values would then pass unnoticed.
//!
//! A committed fixture's whole purpose is pinning committed values, so for a
//! committed case the checkout copy IS the oracle:
//!
//!   * the fixture DIRECTORY and the `*-Data.db` inside it are taken from
//!     `git ls-files`, so the compared path is literally the git-tracked one — an
//!     untracked stray directory under the checkout cannot shadow it either;
//!   * the checkout root is derived from the compile-time workspace anchor with NO
//!     environment override, because an env-settable checkout root would be the same
//!     substitution written a second way; and
//!   * an absent committed fixture is a FAILURE naming the path, never a fallback to
//!     an external root.
//!
//! A fetched-corpus case keeps the evidence-based walk: it has no committed copy to
//! prefer, and neither root is a superset of the other (#3104), so nothing here
//! changes for that tier.
//!
//! The choice is VISIBLE: [`RootSource`] is reported per case in the run census, so a
//! reader can see the committed cases came from the checkout.
//!
//! This is deliberately scoped to THIS lane. The shared
//! `cqlite-core/tests/support/datasets_root.rs` resolver is untouched — other lanes
//! depend on its rule, and generalizing it is #3104's territory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Which candidate root supplied a case's fixture.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RootSource {
    /// The checkout's own `test-data/datasets/sstables` — the git-committed copy.
    Checkout,
    /// A fetched corpus root named by `CQLITE_DATASETS_ROOT`.
    Corpus,
}

impl RootSource {
    /// The census token for this root.
    pub fn as_str(self) -> &'static str {
        match self {
            RootSource::Checkout => "checkout (git-committed)",
            RootSource::Corpus => "fetched corpus",
        }
    }
}

/// The `<dir-name>`/`<file-name>` of one git-tracked `*-Data.db`, relative to
/// `test-data/datasets/sstables/<keyspace>/`.
pub type CommittedSstable = (String, String);

/// Every git-committed fixture, keyed by `(keyspace, table)`.
pub type CommittedFixtures = BTreeMap<(String, String), BTreeSet<CommittedSstable>>;

/// The checkout's own committed `sstables/` root.
///
/// Anchored on the compile-time workspace marker, with NO environment override: the
/// whole point of the committed tier is that no runtime environment can substitute
/// another copy for it.
pub fn checkout_sstables_root() -> PathBuf {
    super::datasets_root::fixture_roots::checkout_test_data_dir()
        .join("datasets")
        .join("sstables")
}

/// Every git-tracked path under `test-data/datasets/sstables`, one per element.
///
/// Read from `git ls-files` at run time, so a newly committed fixture is picked up
/// without editing a list here.
pub fn committed_listing() -> Result<Vec<String>, String> {
    let root = super::datasets_root::repo_root();
    let output = Command::new("git")
        .args(["ls-files", "test-data/datasets/sstables"])
        .current_dir(&root)
        .output()
        .map_err(|e| format!("cannot run `git ls-files` in {}: {e}", root.display()))?;
    if !output.status.success() {
        return Err(format!(
            "`git ls-files` failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect())
}

/// What one committed fixture path names.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommittedPath {
    pub keyspace: String,
    pub table: String,
    /// The `<table>-<uuid>` directory holding it.
    pub dir: String,
    /// The final component: `<gen>-Data.db`, or `<gen>-Data.db.jsonl` for a golden.
    pub file: String,
    /// True for the `*-Data.db.jsonl` sstabledump golden, false for the SSTable.
    pub is_golden: bool,
}

/// Classify one `git ls-files` line.
///
/// `Ok(None)` for a path that is neither a `*-Data.db` nor its `*-Data.db.jsonl`
/// golden; `Err` for one of those two whose path is not
/// `test-data/datasets/sstables/<keyspace>/<table>-<uuid>/<file>` shaped — an
/// unrecognised shape is refused rather than guessed at, since every consumer here
/// keys on the keyspace and table it carries.
pub fn classify(line: &str) -> Result<Option<CommittedPath>, String> {
    let is_golden = line.ends_with("-Data.db.jsonl");
    if !is_golden && !line.ends_with("-Data.db") {
        return Ok(None);
    }
    let parts: Vec<&str> = line.split('/').collect();
    if parts.len() != 6 {
        return Err(format!("unexpected committed fixture path shape: {line}"));
    }
    let (keyspace, dir, file) = (parts[3], parts[4], parts[5]);
    let Some((table, _uuid)) = dir.rsplit_once('-') else {
        return Err(format!("fixture dir has no -<uuid> suffix: {dir}"));
    };
    if table.is_empty() {
        return Err(format!("fixture dir has an empty table name: {dir}"));
    }
    Ok(Some(CommittedPath {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        dir: dir.to_string(),
        file: file.to_string(),
        is_golden,
    }))
}

/// Every git-committed `*-Data.db` (the SSTables, not the goldens), keyed by
/// `(keyspace, table)`.
pub fn committed_fixtures(listing: &[String]) -> Result<CommittedFixtures, String> {
    let mut out = CommittedFixtures::new();
    for line in listing {
        if let Some(path) = classify(line)? {
            if !path.is_golden {
                out.entry((path.keyspace, path.table))
                    .or_default()
                    .insert((path.dir, path.file));
            }
        }
    }
    Ok(out)
}

/// The git-committed fixture directory to compare for a COMMITTED case.
///
/// `sstables` is that table's entry from [`committed_fixtures`]; an empty set means
/// git tracks no `*-Data.db` for it, which for a case declared committed is a
/// failure, not a fallback. When git tracks several SSTables for one table the
/// lexicographically first is taken, matching the deterministic choice the
/// evidence-based lookup makes among sibling generations.
pub fn committed_fixture_dir(
    sstables: Option<&BTreeSet<CommittedSstable>>,
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<PathBuf, String> {
    let Some((dir, file)) = sstables.and_then(|s| s.iter().next()) else {
        return Err(format!(
            "{keyspace}.{table} is declared a git-committed case but `git ls-files` \
             tracks no *-Data.db for it"
        ));
    };
    let fixture = checkout.join(keyspace).join(dir);
    let data_db = fixture.join(file);
    if !data_db.is_file() {
        return Err(format!(
            "the git-tracked {} is missing from the checkout — a committed fixture is \
             the oracle for its committed values and is never served from an external \
             corpus root",
            data_db.display()
        ));
    }
    Ok(fixture)
}

/// The fixture directory for a FETCHED-CORPUS case, plus which root supplied it.
///
/// Keeps the shared #3220 rule: walk every candidate root and take the first that
/// actually carries this table's `*-Data.db`.
pub fn corpus_fixture_dir(
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<(PathBuf, RootSource), String> {
    let root = super::datasets_root::sstables_root_for_table(keyspace, table)
        .ok_or_else(|| super::datasets_root::describe_search(keyspace, table))?;
    let dir = super::compare::fixture_dir_in(&root, keyspace, table)?;
    let source = if root == checkout {
        RootSource::Checkout
    } else {
        RootSource::Corpus
    };
    Ok((dir, source))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
        std::fs::write(path, bytes).expect("write");
    }

    fn fixtures(entries: &[(&str, &str, &str, &str)]) -> CommittedFixtures {
        let mut out = CommittedFixtures::new();
        for (ks, tbl, dir, file) in entries {
            out.entry((ks.to_string(), tbl.to_string()))
                .or_default()
                .insert((dir.to_string(), file.to_string()));
        }
        out
    }

    #[test]
    fn a_committed_fixture_resolves_under_the_checkout_root() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let checkout = tmp.path().join("checkout");
        write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
        let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
        let dir = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect("resolves");
        assert_eq!(dir, checkout.join("ks").join("t-abc"));
    }

    /// The J1 property: an external copy of a committed table is NOT consulted, so a
    /// committed fixture missing from the checkout FAILS instead of silently
    /// resolving to the corpus copy.
    #[test]
    fn a_committed_fixture_absent_from_the_checkout_is_a_failure_not_a_fallback() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let checkout = tmp.path().join("checkout");
        let corpus = tmp.path().join("corpus");
        // The table exists in the external corpus, and only there.
        write(&corpus.join("ks/t-abc/nb-1-big-Data.db"), b"x");
        std::fs::create_dir_all(&checkout).expect("mkdir");
        let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
        let why = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect_err("must not fall back to the corpus copy");
        assert!(
            why.contains("missing from the checkout") && why.contains("nb-1-big-Data.db"),
            "the failure must name the git-tracked path: {why}"
        );
        assert!(
            !why.contains(&corpus.display().to_string()),
            "the corpus root is not a candidate for a committed fixture: {why}"
        );
    }

    #[test]
    fn a_case_git_tracks_no_sstable_for_is_a_named_failure() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let why = committed_fixture_dir(None, "ks", "t", tmp.path())
            .expect_err("an untracked table cannot be a committed case");
        assert!(why.contains("tracks no *-Data.db"), "{why}");
    }

    /// An untracked directory sitting beside the tracked one cannot be chosen: the
    /// compared path comes from `git ls-files`, not from a directory scan.
    #[test]
    fn an_untracked_sibling_directory_cannot_shadow_the_tracked_fixture() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let checkout = tmp.path().join("checkout");
        write(&checkout.join("ks/t-0000/nb-9-big-Data.db"), b"stray");
        write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
        let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
        let dir = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect("resolves");
        assert_eq!(dir, checkout.join("ks").join("t-abc"));
    }

    fn key(ks: &str, table: &str) -> (String, String) {
        (ks.to_string(), table.to_string())
    }

    #[test]
    fn classify_reads_the_committed_path_shape() {
        let data = classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db")
            .expect("well-shaped")
            .expect("a fixture path");
        assert_eq!(
            (
                data.keyspace.as_str(),
                data.table.as_str(),
                data.dir.as_str(),
                data.file.as_str(),
                data.is_golden
            ),
            ("ks", "t", "t-abc", "nb-1-big-Data.db", false)
        );
        let golden = classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db.jsonl")
            .expect("well-shaped")
            .expect("a golden path");
        assert!(golden.is_golden);
        assert_eq!(golden.table, "t");
        assert!(
            classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Index.db")
                .expect("well-shaped")
                .is_none()
        );
    }

    #[test]
    fn classify_refuses_an_unrecognised_shape_rather_than_guessing() {
        for line in [
            "test-data/datasets/sstables/ks/nb-1-big-Data.db",
            "test-data/datasets/sstables/ks/t-abc/deeper/nb-1-big-Data.db",
            "test-data/datasets/sstables/ks/tabc/nb-1-big-Data.db",
        ] {
            assert!(
                classify(line).is_err(),
                "an unrecognised committed path must be refused: {line}"
            );
        }
    }

    #[test]
    fn committed_fixtures_keeps_the_sstables_and_drops_the_goldens() {
        let listing: Vec<String> = [
            "test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db",
            "test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db.jsonl",
            "test-data/datasets/sstables/ks/t-abc/nb-1-big-Statistics.db",
            "test-data/datasets/sstables/ks/u-def/nb-2-big-Data.db",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let out = committed_fixtures(&listing).expect("well-shaped listing");
        assert_eq!(out.len(), 2);
        assert_eq!(
            out.get(&key("ks", "t")).map(|s| s.len()),
            Some(1),
            "the golden must not add a second SSTable entry"
        );
    }

    /// `git ls-files` is the real subject: the repository must actually track the
    /// committed fixtures this lane's committed tier depends on.
    #[test]
    fn the_repository_tracks_committed_fixtures_under_the_checkout_root() {
        let listing = committed_listing().expect("git ls-files");
        let committed = committed_fixtures(&listing).expect("well-shaped listing");
        assert!(
            !committed.is_empty(),
            "no git-tracked *-Data.db under test-data/datasets/sstables — the committed \
             tier would have no subject"
        );
        let checkout = checkout_sstables_root();
        for ((ks, table), sstables) in &committed {
            committed_fixture_dir(Some(sstables), ks, table, &checkout)
                .unwrap_or_else(|why| panic!("{ks}.{table}: {why}"));
        }
    }
}
