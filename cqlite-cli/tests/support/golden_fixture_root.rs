//! WHICH root supplies a case's fixture — and why a git-committed case must be
//! served from the CHECKOUT (issue #1491 review finding J1).
//!
//! # The defect this module removes
//!
//! The lane used to resolve every case's fixture through the shared #3220
//! resolver [`super::datasets_root::sstables_root_for_table`], which walks the
//! candidate roots in order — `CQLITE_DATASETS_ROOT` first, then the checkout — and
//! returns the first that carries the table. That rule is right for a fetched-corpus
//! fixture and WRONG for a committed one. Measured on a fleet box, the
//! `CQLITE_DATASETS_ROOT=/data/datasets` corpus carried its own copy of 22 of this
//! lane's 24 committed cases, so for all 22 the lane compared an EXTERNAL copy —
//! possibly stale, regenerated, or simply a different generation — while the census
//! still reported the git-committed table as covered. A regression specific to the
//! committed values would then pass unnoticed.
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
//! reader can see the committed cases came from the checkout — and so is
//! [`Fixture::of_dirs`], the number of SSTable directories the case could have been
//! compared from, so comparing one of several is a DECLARED narrowing rather than a
//! silent pick (finding L3).
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
    /// A root other than the checkout's — in an ordinary run, the
    /// `CQLITE_DATASETS_ROOT` corpus.
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
    if parts.len() != 6 || parts[..3] != ["test-data", "datasets", "sstables"] {
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

/// WHICH fixture directory a case is compared from, and how many there were.
///
/// `of_dirs` is the number of SSTable DIRECTORIES that were candidates for this
/// table under the root that supplied it; `dir` is the first in sorted order. The
/// count travels with the choice so the caller can DECLARE the narrowing in its
/// census instead of picking one of N silently (issue #1491 review finding L3).
/// Comparing a second directory is a different staged table and a different
/// golden, so it is a narrowing of coverage — not, like two SSTables inside ONE
/// directory, an unsound comparison (`compare::golden_path` fails on that).
#[derive(Clone, Debug)]
pub struct Fixture {
    pub dir: PathBuf,
    pub source: RootSource,
    pub of_dirs: usize,
}

/// The git-committed fixture directory to compare for a COMMITTED case.
///
/// `sstables` is that table's entry from [`committed_fixtures`]; an empty set means
/// git tracks no `*-Data.db` for it, which for a case declared committed is a
/// failure, not a fallback. When git tracks several SSTables for one table the
/// lexicographically first `(directory, file)` is taken, so the DIRECTORY chosen is
/// the same one the evidence-based lookup's sorted directory scan would choose; how
/// many distinct directories git tracks is returned in [`Fixture::of_dirs`] for the
/// caller to declare.
pub fn committed_fixture_dir(
    sstables: Option<&BTreeSet<CommittedSstable>>,
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<Fixture, String> {
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
    let dirs: BTreeSet<&String> = sstables
        .map(|s| s.iter().map(|(dir, _)| dir).collect())
        .unwrap_or_default();
    Ok(Fixture {
        dir: fixture,
        source: RootSource::Checkout,
        of_dirs: dirs.len(),
    })
}

/// Why a FETCHED-CORPUS fixture could not be resolved. The two are different
/// verdicts and must not be flattened onto one (issue #1491 review finding M3):
/// flattening them made an unreadable or self-contradictory corpus produce a GREEN
/// run labelled "NOT PRESENT", i.e. a failure wearing a skip's clothes — the exact
/// shape this lane exists to prevent (CLAUDE.md: never let a dataset-dependent test
/// pass on an empty dataset).
pub enum CorpusMiss {
    /// Every candidate root ANSWERED, and none carries this table's `*-Data.db`.
    /// For a fetched-corpus case that is a LEGAL skip, declared in the census.
    Absent(String),
    /// A candidate root could NOT answer: its keyspace directory, or a fixture
    /// directory inside it, exists and cannot be read. "I could not tell" is not
    /// "there is nothing there", so it is a FAILURE naming the root and the cause
    /// (finding N4).
    Unusable(String),
}

/// The fixture directory for a FETCHED-CORPUS case, plus which root supplied it.
///
/// Keeps the shared #3220 rule — walk every candidate root and take the first that
/// actually carries this table's `*-Data.db` — but walks it THREE-VALUED (issue
/// #1491 review finding N4). The source is reported as [`RootSource::Checkout`]
/// when the root that won is the checkout's own `sstables/` root and as
/// [`RootSource::Corpus`] otherwise, so the census states what was actually read
/// rather than what the tier implies.
///
/// # Why this lane cannot use the shared `Option`-returning resolver
///
/// `datasets_root::sstables_root_for_table` answers `Option`, and every filesystem
/// predicate under it collapses a read FAILURE onto `false` (CLAUDE.md records the
/// rule: a two-valued predicate must collapse "cannot tell" onto one of its
/// answers, and it always picks the permissive one). So an inaccessible corpus —
/// a keyspace directory that exists and cannot be read, a fixture directory whose
/// contents cannot be listed — was indistinguishable from one that simply does not
/// carry the table, and EVERY optional corpus case then reported `NOT PRESENT` and
/// passed. That is a failure wearing a skip's clothes, which is the exact shape
/// finding M3 removed one level down and this removes at the root walk itself.
///
/// The three answers, and where the line is drawn:
///
///   * **present** — the root holds a `<table>-*` directory with a `*-Data.db`;
///   * **verified absent** — the filesystem ANSWERED that there is nothing there
///     (the keyspace directory does not exist, or it does and holds no such
///     directory). A legal skip;
///   * **unreadable** — the filesystem could not answer (permissions, a non-
///     directory in the path, an I/O error). A FAILURE naming the root and the
///     cause, and it ABORTS the walk rather than falling through to the next
///     candidate: a later root's answer cannot stand in for an unknown earlier
///     one, since the earlier root is the one the shared rule would have picked.
///
/// The boundary is deliberate: `ENOENT` is an answer, not an absence of one. A
/// `CQLITE_DATASETS_ROOT` pointing at a path that does not exist therefore still
/// skips (nothing here can tell it from an unset one) — that whole-corpus
/// preflight is the gate's `missing-fixtures` component (#2078) and #3104's
/// territory, not this lane's.
pub fn corpus_fixture_dir(
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<Fixture, CorpusMiss> {
    corpus_fixture_from(
        &super::datasets_root::sstables_root_candidates(),
        keyspace,
        table,
        checkout,
    )
}

/// PURE form of [`corpus_fixture_dir`], parameterized on the candidate list — the
/// same seam the shared `datasets_root::first_root_with_table` keeps, and for the
/// same reason: the real list is half environment and half a COMPILE-TIME checkout
/// path, so a test reading it can only observe this machine's layout.
pub fn corpus_fixture_from(
    roots: &[PathBuf],
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<Fixture, CorpusMiss> {
    for root in roots {
        match corpus_fixture_in(root, keyspace, table, checkout) {
            Ok(fixture) => return Ok(fixture),
            // This root VERIFIABLY does not carry the table: keep walking, which
            // is the shared evidence-based rule (#3220).
            Err(CorpusMiss::Absent(_)) => continue,
            // This root could not answer. Reported, never walked PAST: the
            // unreadable root is the one the shared rule would have picked, so a
            // later root's answer would silently stand in for an unknown one.
            Err(unusable) => return Err(unusable),
        }
    }
    Err(CorpusMiss::Absent(super::datasets_root::describe_search(
        keyspace, table,
    )))
}

/// PURE form of [`corpus_fixture_dir`] for ONE candidate root: present, verifiably
/// absent, or unreadable.
///
/// Factored out so all three answers are testable against synthetic roots — the
/// real candidate list is half environment and half a compile-time checkout path,
/// so a test reading it can only observe this machine's layout.
pub fn corpus_fixture_in(
    root: &Path,
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<Fixture, CorpusMiss> {
    let mut dirs = table_dirs_in(root, keyspace, table)?;
    if dirs.is_empty() {
        return Err(CorpusMiss::Absent(format!(
            "{} is readable and holds no {table}-* directory with a *-Data.db for \
             {keyspace}.{table}",
            root.display()
        )));
    }
    let of_dirs = dirs.len();
    Ok(Fixture {
        dir: dirs.remove(0),
        source: if root == checkout {
            RootSource::Checkout
        } else {
            RootSource::Corpus
        },
        of_dirs,
    })
}

/// Every `<table>-<uuid>` directory under `root/keyspace` holding a `*-Data.db`,
/// in sorted order — or [`CorpusMiss::Unusable`] when the filesystem could not
/// answer.
///
/// The lane's own scan rather than `compare::fixture_dirs_in`, because that one
/// answers "no `*-Data.db`" for a fixture directory it could not READ
/// (`has_data_db` ends in `.unwrap_or(false)`), which is this finding's shape one
/// level in: an unreadable directory would make the root verifiably absent and the
/// case skip.
fn table_dirs_in(root: &Path, keyspace: &str, table: &str) -> Result<Vec<PathBuf>, CorpusMiss> {
    let ks_dir = root.join(keyspace);
    let entries = match std::fs::read_dir(&ks_dir) {
        Ok(entries) => entries,
        // The filesystem ANSWERED: there is no such keyspace under this root.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(CorpusMiss::Unusable(format!(
                "{keyspace}.{table}: cannot read {} ({e}) — a candidate corpus root \
                 that cannot be READ is not a root that verifiably lacks the table, \
                 so this is a failure and not a NOT PRESENT skip",
                ks_dir.display()
            )))
        }
    };
    let prefix = format!("{table}-");
    let mut matches: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            CorpusMiss::Unusable(format!(
                "{keyspace}.{table}: cannot enumerate {} ({e})",
                ks_dir.display()
            ))
        })?;
        let path = entry.path();
        let is_candidate = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with(&prefix))
            .unwrap_or(false);
        if !is_candidate || !path.is_dir() {
            continue;
        }
        if holds_data_db(&path, keyspace, table)? {
            matches.push(path);
        }
    }
    matches.sort();
    Ok(matches)
}

/// Does this fixture directory hold a `*-Data.db`? Three-valued like its caller: a
/// directory that cannot be listed is `Unusable`, never "holds none".
fn holds_data_db(dir: &Path, keyspace: &str, table: &str) -> Result<bool, CorpusMiss> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // Gone between the listing and the read: verifiably not there now.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => {
            return Err(CorpusMiss::Unusable(format!(
                "{keyspace}.{table}: cannot read the fixture directory {} ({e})",
                dir.display()
            )))
        }
    };
    for entry in entries {
        let entry = entry.map_err(|e| {
            CorpusMiss::Unusable(format!(
                "{keyspace}.{table}: cannot enumerate {} ({e})",
                dir.display()
            ))
        })?;
        if entry
            .file_name()
            .to_str()
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return Ok(true);
        }
    }
    Ok(false)
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
        let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect("resolves");
        assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
        assert_eq!(fixture.of_dirs, 1, "one tracked directory");
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
        let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect("resolves");
        assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
    }

    /// M3/N4: a corpus root whose keyspace directory cannot be READ is a FAILURE,
    /// not an absence. Flattened onto the absence verdict, an unreadable corpus
    /// produced a green run labelled "NOT PRESENT".
    #[test]
    fn a_selected_root_whose_keyspace_cannot_be_read_is_unusable_not_absent() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path().join("root");
        // A FILE where the keyspace directory belongs: `read_dir` fails, which is the
        // same class of answer as a permission failure and needs no chmod.
        write(&root.join("ks"), b"not a directory");
        let miss = match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
            Err(miss) => miss,
            Ok(_) => panic!("an unreadable keyspace directory cannot resolve"),
        };
        match miss {
            CorpusMiss::Unusable(why) => assert!(
                why.contains("ks.t") && why.contains("cannot read"),
                "the failure must name the table and the cause: {why}"
            ),
            CorpusMiss::Absent(why) => {
                panic!("an unreadable corpus must not read as absent: {why}")
            }
        }
    }

    /// The other side of the same line: a root that IS readable and holds no
    /// `<table>-*` directory with a `*-Data.db` is VERIFIABLY absent, so the walk
    /// keeps going and the case may legally skip.
    ///
    /// This is not a softening — it is what the corpus really looks like. This
    /// repository commits the `test_types` goldens WITHOUT their gitignored
    /// binaries, so every checkout carries
    /// `test-data/datasets/sstables/test_types/nb_*-<uuid>/` directories holding a
    /// `*-Data.db.jsonl` and no `*-Data.db`. Calling that malformed would fail
    /// every checkout-only run of the fetched-corpus tier, which is the tier's
    /// legal skip. (The old "the root walk and the directory scan disagree"
    /// verdict is gone with the second opinion that produced it: there is now ONE
    /// scan, and it is three-valued — see [`corpus_fixture_from`].)
    #[test]
    fn a_readable_root_with_no_data_db_is_absent_and_the_walk_continues() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_only = tmp.path().join("jsonl-only");
        write(&jsonl_only.join("ks/t-abc/nb-1-big-Data.db.jsonl"), b"{}");
        let corpus = tmp.path().join("corpus");
        write(&corpus.join("ks/t-abc/nb-1-big-Data.db"), b"x");

        match corpus_fixture_in(&jsonl_only, "ks", "t", tmp.path()) {
            Err(CorpusMiss::Absent(why)) => assert!(
                why.contains("ks.t") && why.contains(&jsonl_only.display().to_string()),
                "the absence must name the root it was verified against: {why}"
            ),
            Err(CorpusMiss::Unusable(why)) => {
                panic!("a readable root without the table is absent, not unusable: {why}")
            }
            Ok(_) => panic!("a directory with no *-Data.db cannot resolve"),
        }

        // …and the walk goes on to the root that does carry it.
        let fixture = corpus_fixture_from(&[jsonl_only, corpus.clone()], "ks", "t", tmp.path())
            .unwrap_or_else(|e| match e {
                CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => {
                    panic!("the second root carries the table: {why}")
                }
            });
        assert_eq!(fixture.dir, corpus.join("ks").join("t-abc"));
    }

    /// N4: an UNREADABLE candidate root FAILS the case and is never walked past —
    /// not even when a later root carries the table.
    ///
    /// "I could not tell" is not "there is nothing there". The shared
    /// `sstables_root_for_table` answers `Option` and every predicate beneath it
    /// ends in `.unwrap_or(false)`, so an inaccessible corpus read as absent and
    /// EVERY optional corpus case reported `NOT PRESENT` and passed. And falling
    /// through would be the same defect wearing a different hat: the unreadable
    /// root is the one the walk would have picked, so a later root's answer cannot
    /// stand in for it.
    #[test]
    fn an_unreadable_candidate_root_fails_and_is_not_walked_past() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let broken = tmp.path().join("broken");
        // A FILE where the keyspace directory belongs: `read_dir` fails with a
        // non-NotFound error, which is the same branch a permission failure takes
        // and needs no chmod (a chmod-based case passes vacuously as root).
        write(&broken.join("ks"), b"not a directory");
        let good = tmp.path().join("good");
        write(&good.join("ks/t-abc/nb-1-big-Data.db"), b"x");

        match corpus_fixture_from(&[broken.clone(), good], "ks", "t", tmp.path()) {
            Err(CorpusMiss::Unusable(why)) => {
                assert!(
                    why.contains("ks.t") && why.contains(&broken.display().to_string()),
                    "the failure must name the table and the root: {why}"
                );
                assert!(
                    why.contains("NOT PRESENT"),
                    "the failure must say why it is not a skip: {why}"
                );
            }
            Err(CorpusMiss::Absent(why)) => {
                panic!("an unreadable root must not read as absent: {why}")
            }
            Ok(_) => panic!("the walk must not resolve past a root it could not read"),
        }
    }

    /// The same three-valued rule ONE LEVEL IN: a fixture directory that cannot be
    /// listed is unusable, never "holds no `*-Data.db`".
    ///
    /// `compare::fixture_dirs_in`'s `has_data_db` ends in `.unwrap_or(false)`, so
    /// reusing it would have made an unreadable fixture directory a verified
    /// absence and skipped the case — the finding's own shape, one level down.
    /// Classification is by `ErrorKind::NotFound` alone, so a permission failure
    /// takes exactly this branch.
    #[test]
    fn an_unlistable_fixture_directory_is_unusable_not_empty() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let not_a_dir = tmp.path().join("t-abc");
        write(&not_a_dir, b"x");
        match holds_data_db(&not_a_dir, "ks", "t") {
            Err(CorpusMiss::Unusable(why)) => assert!(
                why.contains("cannot read the fixture directory") && why.contains("ks.t"),
                "{why}"
            ),
            Err(CorpusMiss::Absent(why)) => panic!("not an absence: {why}"),
            Ok(answer) => panic!("an unreadable directory cannot answer {answer}"),
        }
        // A directory that has VANISHED did answer, so it is absent, not unusable.
        assert_eq!(
            holds_data_db(&tmp.path().join("gone"), "ks", "t").ok(),
            Some(false)
        );
    }

    /// When no candidate root carries the table at all, the verdict is the legal
    /// skip and its message names the search.
    #[test]
    fn no_candidate_root_carrying_the_table_is_the_legal_skip() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let empty = tmp.path().join("empty");
        std::fs::create_dir_all(&empty).expect("mkdir");
        match corpus_fixture_from(&[empty], "ks", "t", tmp.path()) {
            Err(CorpusMiss::Absent(why)) => {
                assert!(why.contains("ks.t"), "the skip must name the table: {why}")
            }
            Err(CorpusMiss::Unusable(why)) => panic!("a verified absence is not a failure: {why}"),
            Ok(_) => panic!("nothing to resolve"),
        }
    }

    /// And the ordinary shape still resolves, so the two failures above are
    /// attributable to what they synthesize rather than to the scaffolding.
    #[test]
    fn a_usable_corpus_root_resolves_and_reports_its_source() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path().join("root");
        write(&root.join("ks/t-abc/nb-1-big-Data.db"), b"x");
        let fixture = corpus_fixture_in(&root, "ks", "t", tmp.path()).unwrap_or_else(|e| {
            panic!(
                "a root holding the table must resolve: {}",
                match e {
                    CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => why,
                }
            )
        });
        assert_eq!(fixture.dir, root.join("ks").join("t-abc"));
        assert_eq!(fixture.of_dirs, 1);
        // The checkout passed above is NOT this root, so the source is the corpus.
        assert!(matches!(fixture.source, RootSource::Corpus));
    }

    /// L3: when git tracks SEVERAL SSTable directories for one table, the first is
    /// compared and the COUNT travels with it, so the caller's census can declare
    /// how many directories went untested. Without the count the choice is a silent
    /// pick of one of N — the property this lane exists to prevent.
    #[test]
    fn several_tracked_directories_are_counted_not_just_narrowed_to_one() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let checkout = tmp.path().join("checkout");
        write(&checkout.join("ks/t-aaa/nb-1-big-Data.db"), b"x");
        write(&checkout.join("ks/t-bbb/nb-1-big-Data.db"), b"y");
        let committed = fixtures(&[
            ("ks", "t", "t-bbb", "nb-1-big-Data.db"),
            ("ks", "t", "t-aaa", "nb-1-big-Data.db"),
        ]);
        let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
            .expect("resolves");
        assert_eq!(
            fixture.dir,
            checkout.join("ks").join("t-aaa"),
            "the sorted-first directory is the one compared"
        );
        assert_eq!(
            fixture.of_dirs, 2,
            "both tracked directories must be counted, so the narrowing can be declared"
        );

        // Two SSTables tracked in ONE directory is one directory, not two: that
        // shape is refused by `compare::golden_path`, not counted here.
        let one_dir = fixtures(&[
            ("ks", "u", "u-aaa", "nb-1-big-Data.db"),
            ("ks", "u", "u-aaa", "nb-2-big-Data.db"),
        ]);
        write(&checkout.join("ks/u-aaa/nb-1-big-Data.db"), b"x");
        write(&checkout.join("ks/u-aaa/nb-2-big-Data.db"), b"y");
        let fixture = committed_fixture_dir(one_dir.get(&key("ks", "u")), "ks", "u", &checkout)
            .expect("resolves");
        assert_eq!(fixture.of_dirs, 1);
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
            "elsewhere/datasets/sstables/ks/t-abc/nb-1-big-Data.db",
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
