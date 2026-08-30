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

use super::fs_probe;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// WHERE a case's fixture came from — reported from what was ESTABLISHED about it,
/// never from what its tier implies (issue #1491 review finding T3).
///
/// The three are distinct facts, and the first two used to share one spelling. A
/// COMMITTED case's fixture is git-tracked, established by `git ls-files` in
/// [`committed_fixture_dir`]. A FETCHED-CORPUS case's fixture is found by the
/// evidence walk, and that walk can land on the CHECKOUT's own dataset root — at
/// which point nothing has established that the file is tracked, and
/// `resolve_fixture` has in fact already established the OPPOSITE (a case declared
/// `Presence::Corpus` whose table git tracks is a mis-declaration that fails). So
/// reporting it as "checkout (git-committed)" told a reader the oracle was the
/// committed copy when it was not — and this lane's whole value rests on knowing
/// which bytes were the oracle, so a provenance line that can be wrong is worse
/// than none.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RootSource {
    /// `git ls-files` tracks this exact `*-Data.db`, and it was read from the
    /// checkout — the only provenance that may claim the committed copy.
    GitTracked,
    /// The evidence walk landed on the checkout's own `test-data/datasets/sstables`,
    /// but nothing established that the file is git-tracked. Reached only by a
    /// fetched-corpus case, i.e. one whose table git tracks no `*-Data.db` for.
    CheckoutUntracked,
    /// A root other than the checkout's — in an ordinary run, the
    /// `CQLITE_DATASETS_ROOT` corpus.
    Corpus,
}

impl RootSource {
    /// The census token for this provenance. The three are deliberately
    /// distinguishable at a glance: a reader scanning a census must be able to tell
    /// the git-tracked oracle from a same-path file nothing tracks.
    pub fn as_str(self) -> &'static str {
        match self {
            RootSource::GitTracked => "checkout, git-tracked",
            RootSource::CheckoutUntracked => "checkout root, NOT git-tracked",
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

/// WHICH of a table's git-committed `*-Data.db` files a committed case compares.
///
/// The lexicographically first `(directory, file)`, which is the same DIRECTORY the
/// evidence-based lookup's sorted directory scan would choose. Exposed because the
/// coverage census has to name the SAME generation this resolver stages: two copies
/// of the rule could drift, and a census that reported one generation as compared
/// while the lane compared another would be a claim about bytes nothing read (issue
/// #1491 review round 21).
pub fn selected_committed_sstable(
    sstables: &BTreeSet<CommittedSstable>,
) -> Option<&CommittedSstable> {
    sstables.iter().next()
}

/// WHICH fixture directory a case is compared from, and how many there were.
///
/// `of_dirs` is the number of SSTable DIRECTORIES that were candidates for this
/// table under the root that supplied it; `dir` is the first in sorted order. The
/// count travels with the choice so the caller can DECLARE the narrowing in its
/// census instead of picking one of N silently (issue #1491 review finding L3).
/// Comparing a second directory is a different staged table and a different
/// golden, so it is a narrowing of coverage — not, like two SSTables inside ONE
/// directory, an unsound comparison (`compare::golden_path` fails on that). For a
/// git-COMMITTED table the narrowing is nonetheless a failure elsewhere: the
/// coverage census classifies per committed `*-Data.db` and refuses the generation
/// nothing compares, so `of_dirs > 1` survives as a declared gap only for a
/// fetched-corpus fixture, which git tracks nothing for.
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
    let Some((dir, file)) = sstables.and_then(selected_committed_sstable) else {
        return Err(format!(
            "{keyspace}.{table} is declared a git-committed case but `git ls-files` \
             tracks no *-Data.db for it"
        ));
    };
    let fixture = checkout.join(keyspace).join(dir);
    let data_db = fixture.join(file);
    // Three-valued (issue #1491 review finding V1's sweep). `is_file()` answers
    // `false` for a path it could not describe, so an UNREADABLE committed fixture
    // was reported as one that is "missing from the checkout" — the right verdict
    // (both fail) reached through a false statement, and the reader is then sent
    // looking for a file that is right there.
    match fs_probe::presence(&data_db) {
        Ok(fs_probe::Presence::File) => {}
        Ok(fs_probe::Presence::Absent) => {
            return Err(format!(
                "the git-tracked {} is missing from the checkout — a committed fixture \
                 is the oracle for its committed values and is never served from an \
                 external corpus root",
                data_db.display()
            ))
        }
        Ok(other) => {
            return Err(format!(
                "the git-tracked {} is {}, where a *-Data.db must be — a committed \
                 fixture is the oracle for its committed values and is never served \
                 from an external corpus root",
                data_db.display(),
                other.describe()
            ))
        }
        Err(why) => {
            return Err(format!(
                "the git-tracked {} could not be examined: {why}",
                data_db.display()
            ))
        }
    }
    let dirs: BTreeSet<&String> = sstables
        .map(|s| s.iter().map(|(dir, _)| dir).collect())
        .unwrap_or_default();
    Ok(Fixture {
        dir: fixture,
        // Established, not assumed: `sstables` is this table's entry from
        // `git ls-files` and `dir`/`file` are the tracked names, so the path
        // returned IS the git-tracked one.
        source: RootSource::GitTracked,
        of_dirs: dirs.len(),
    })
}

/// Why a FETCHED-CORPUS fixture could not be resolved. The two are different
/// verdicts and must not be flattened onto one (issue #1491 review finding M3):
/// flattening them made an unreadable or self-contradictory corpus produce a GREEN
/// run labelled "NOT PRESENT", i.e. a failure wearing a skip's clothes — the exact
/// shape this lane exists to prevent (CLAUDE.md: never let a dataset-dependent test
/// pass on an empty dataset).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CorpusMiss {
    /// Every candidate root ANSWERED, and none carries this table's `*-Data.db`.
    /// For a fetched-corpus case that is a LEGAL skip, declared in the census.
    Absent(String),
    /// A candidate root could not be USED, so nothing it might have carried was
    /// established. Two causes, and each is a FAILURE naming the root and the cause:
    /// it could not ANSWER — its keyspace directory, or a fixture directory inside
    /// it, exists and cannot be read, and "I could not tell" is not "there is nothing
    /// there" (finding N4) — or `CQLITE_DATASETS_ROOT` is set to something that
    /// answered and is not a corpus, and "it is a readable path" is not "it is a
    /// corpus" (finding W1).
    Unusable(String),
}

/// The fixture directory for a FETCHED-CORPUS case, plus which root supplied it.
///
/// Keeps the shared #3220 rule — walk every candidate root and take the first that
/// actually carries this table's `*-Data.db` — but walks it THREE-VALUED (issue
/// #1491 review finding N4). The source is reported as
/// [`RootSource::CheckoutUntracked`] when the root that won is the checkout's own
/// `sstables/` root and as [`RootSource::Corpus`] otherwise, so the census states
/// what was actually read rather than what the tier implies. Never
/// [`RootSource::GitTracked`]: this walk establishes only that a root HOLDS the
/// table, and every case that reaches it is one `git ls-files` tracks no
/// `*-Data.db` for (finding T3).
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
/// # Readable is not the same question as VALID (issue #1491 review finding W1)
///
/// The paragraph above is about whether the filesystem could ANSWER. A separate
/// question is whether the answer describes a CORPUS. `CQLITE_DATASETS_ROOT` used to
/// be accepted on ANY successful answer — a regular file, a socket, a path that does
/// not exist, or a directory with no `sstables/` subtree — because "the filesystem
/// answered" was read as "the root is fine". The shared candidate list then drops or
/// walks past such a value, so EVERY corpus-only case reported `NOT PRESENT` and
/// passed while an explicitly configured corpus was silently contributing nothing.
/// That is CLAUDE.md's "never let a dataset-dependent test pass on an empty dataset"
/// at the root level: point the variable at a tarball, at a parent directory, or at a
/// typo that happens to exist, and half the lane disappears from a green run.
///
/// So when the variable is SET and nonblank this lane requires a corpus: the value
/// must be a directory whose `sstables/` subtree is a directory too, and anything
/// else is [`CorpusMiss::Unusable`] naming the root and what was wrong with it. The
/// checkout candidate is still searched, so a COMMITTED case is unaffected either
/// way — it resolves from `git ls-files` and reads no environment at all — but a
/// corpus-only case may no longer report an absence its configured root never
/// established.
///
/// The three operator situations a corpus miss can come from are told apart in the
/// message, because they call for three different actions ([`EnvCorpus`]):
///
///   * **not configured** — the variable is unset or blank, so no fetched corpus was
///     asked for and only the checkout was searched. A legal skip;
///   * **configured but unusable: `<why>`** — a corpus WAS asked for and the value is
///     not one. A FAILURE;
///   * **valid corpus, table absent** — the value is a corpus and this table is not
///     in it. A legal skip.
///
/// One boundary MOVES and one stays. A nonexistent path is still an ANSWER, but it is
/// no longer waved through: the set/nonblank test tells it from an unset variable, so
/// "nothing here can tell them apart" — the old reason for skipping it — was simply
/// untrue. What stays is the SCOPE: this is a per-CASE check of ONE variable, not a
/// whole-corpus preflight, so whether a valid corpus carries the tables a run needs
/// remains the gate's `missing-fixtures` component (#2078) and #3104's territory.
pub fn corpus_fixture_dir(
    keyspace: &str,
    table: &str,
    checkout: &Path,
) -> Result<Fixture, CorpusMiss> {
    // The candidate LIST is the shared resolver's, and its env-root test is
    // `p.is_dir()` — two-valued, so a `CQLITE_DATASETS_ROOT` the filesystem could
    // not DESCRIBE contributes no candidate at all and the walk below never gets to
    // ask it. The walk would then report an absence established by the remaining
    // candidates alone, which is this finding's shape one level ABOVE the walk
    // (issue #1491 review finding V1's sweep). Probed three-valued here so that
    // collapse cannot reach the verdict.
    //
    // The shared resolver is deliberately left alone: other lanes depend on its
    // rule, and making it three-valued is #3104's territory.
    let configured = env_datasets_root_usable(keyspace, table)?;
    corpus_fixture_from(
        &super::datasets_root::sstables_root_candidates(),
        keyspace,
        table,
        checkout,
    )
    // A verified absence is a legal skip, and WHICH legal skip it is decides what an
    // operator should do about it — so the census line says whether a corpus was
    // configured at all (finding W1).
    .map_err(|miss| match miss {
        CorpusMiss::Absent(why) => {
            CorpusMiss::Absent(format!("{why} — {}", configured.describe_absence()))
        }
        unusable => unusable,
    })
}

/// What `CQLITE_DATASETS_ROOT` was found to name, for the cases that are NOT a
/// failure — the two legal-skip situations, kept apart because an operator acts
/// differently on each (issue #1491 review finding W1).
#[derive(Clone, PartialEq, Eq, Debug)]
enum EnvCorpus {
    /// Unset, or blank by the same test the shared candidate list applies: no fetched
    /// corpus was asked for, so only the checkout's committed root was searched.
    NotConfigured,
    /// Set, and established as a corpus: the value is a directory and so is the
    /// `sstables/` subtree the shared candidate list appends. Carries that subtree so
    /// the diagnostic can name the root the table was verified absent from.
    Corpus { sstables: PathBuf },
}

impl EnvCorpus {
    /// Which legal-skip situation this is, appended to the absence message.
    ///
    /// The third situation — configured but not a corpus — never reaches here: it is
    /// a [`CorpusMiss::Unusable`] failure, so it can only be described by
    /// [`datasets_root_usable`].
    fn describe_absence(&self) -> String {
        let env = super::datasets_root::fixture_roots::DATASETS_ROOT_ENV;
        match self {
            EnvCorpus::NotConfigured => format!(
                "not configured: {env} is unset or blank, so no fetched corpus was \
                 searched — only the checkout's own committed sstables root"
            ),
            EnvCorpus::Corpus { sstables } => format!(
                "valid corpus, table absent: {env} names a corpus ({} is a directory) \
                 and it does not carry this table",
                sstables.display()
            ),
        }
    }
}

/// Is `CQLITE_DATASETS_ROOT` usable as a corpus, and if not, why not?
///
/// Reads the variable and hands the raw value to [`datasets_root_usable`]; see
/// [`corpus_fixture_dir`] for what "usable" requires and why an unusable value is a
/// failure rather than a skip.
fn env_datasets_root_usable(keyspace: &str, table: &str) -> Result<EnvCorpus, CorpusMiss> {
    datasets_root_usable(
        std::env::var_os(super::datasets_root::fixture_roots::DATASETS_ROOT_ENV).as_deref(),
        keyspace,
        table,
    )
}

/// PURE form of [`env_datasets_root_usable`], parameterized on the raw value — the
/// same seam the shared `fixture_roots::resolve_datasets_root_if_present` keeps, and
/// for the same reason: a test that MUTATES the environment races every other test in
/// the binary, since the environment is process-global.
///
/// `Ok` in exactly the two legal-skip situations of [`EnvCorpus`]. Everything else is
/// `Unusable`, including a value the filesystem could not describe at all — a root
/// that could not be classified has not been established to lack the table.
fn datasets_root_usable(
    raw: Option<&std::ffi::OsStr>,
    keyspace: &str,
    table: &str,
) -> Result<EnvCorpus, CorpusMiss> {
    let env = super::datasets_root::fixture_roots::DATASETS_ROOT_ENV;
    let unusable = |why: String| {
        CorpusMiss::Unusable(format!(
            "{keyspace}.{table}: configured but unusable: {why}. {env} is set, so a \
             fetched corpus WAS asked for, and a value that is not a corpus cannot \
             establish that this table is absent — so this is a failure and not a NOT \
             PRESENT skip"
        ))
    };
    // Blank exactly as the shared candidate list judges it (a trim-empty value there
    // contributes no candidate, and a non-UTF-8 one counts as nonblank). The two must
    // agree: if this said "configured" where the shared list said "not configured",
    // the lane would fail a run whose corpus really was unset.
    let Some(raw) = raw.filter(|v| v.to_str().map(|t| !t.trim().is_empty()).unwrap_or(true)) else {
        return Ok(EnvCorpus::NotConfigured);
    };
    let root = Path::new(raw);
    match fs_probe::presence(root).map_err(&unusable)? {
        fs_probe::Presence::Dir => {}
        fs_probe::Presence::Absent => {
            return Err(unusable(format!(
                "{env}={} names a path that does not exist",
                root.display()
            )))
        }
        other => {
            return Err(unusable(format!(
                "{env}={} is {}, not a directory",
                root.display(),
                other.describe()
            )))
        }
    }
    // The subtree the shared candidate list appends. Its LISTABILITY is not asked here
    // — that is answered per keyspace, three-valued, by `table_dirs_in`; what is
    // asked is whether the corpus layout is there at all.
    let sstables = root.join("sstables");
    match fs_probe::presence(&sstables).map_err(&unusable)? {
        fs_probe::Presence::Dir => Ok(EnvCorpus::Corpus { sstables }),
        other => Err(unusable(format!(
            "{env}={} is a directory, but {} is {} — the layout this lane reads is \
             <root>/sstables/<keyspace>/<table>-<uuid>/*-Data.db (fetch: bash \
             test-data/scripts/fetch-datasets.sh)",
            root.display(),
            sstables.display(),
            other.describe()
        ))),
    }
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
            // The checkout's own dataset root — but this walk asked only whether a
            // root HOLDS the table, so nothing here establishes that the file is
            // git-tracked, and for every case that reaches this walk git tracks no
            // `*-Data.db` for the table at all (finding T3).
            RootSource::CheckoutUntracked
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
/// The lane's own scan rather than `compare::staging::fixture_dirs_in`, because the
/// two answer different questions: that one serves an ALREADY CHOSEN root and
/// answers `Result<_, String>`, i.e. "readable and holds nothing" and "could not be
/// read" both end the search, while this one is choosing AMONG roots and must tell
/// [`CorpusMiss::Absent`] (keep walking, and a legal skip if no root has it) from
/// [`CorpusMiss::Unusable`] (stop, and fail). Both are now three-valued at the
/// filesystem — every question here and there goes through `super::fs_probe`.
fn table_dirs_in(root: &Path, keyspace: &str, table: &str) -> Result<Vec<PathBuf>, CorpusMiss> {
    let ks_dir = root.join(keyspace);
    let unusable = |why: String| {
        CorpusMiss::Unusable(format!(
            "{keyspace}.{table}: {why}. A candidate corpus root that cannot be READ is \
             not a root that verifiably lacks the table, so this is a failure and not a \
             NOT PRESENT skip"
        ))
    };
    // `Ok(None)`: the filesystem ANSWERED that there is no such keyspace here.
    let Some(entries) = fs_probe::dir_entries(&ks_dir).map_err(unusable)? else {
        return Ok(Vec::new());
    };
    let prefix = format!("{table}-");
    let mut matches: Vec<PathBuf> = Vec::new();
    for entry in entries {
        if !fs_probe::name_starts_with(&entry.file_name(), &prefix) {
            continue;
        }
        let path = entry.path();
        // THREE-VALUED, where `path.is_dir()` stood (issue #1491 review finding V1):
        // that predicate answers `false` for an entry it could not describe, so an
        // INACCESSIBLE `<table>-*` entry was skipped, the root then read as
        // verifiably lacking the table, and an optional corpus case passed with a
        // `NOT PRESENT` it had not established.
        if !fs_probe::is_dir(&path).map_err(unusable)? {
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
    let unusable = |why: String| {
        CorpusMiss::Unusable(format!(
            "{keyspace}.{table}: cannot read the fixture directory — {why}"
        ))
    };
    // Gone between the listing and the read: verifiably not there now.
    let Some(entries) = fs_probe::dir_entries(dir).map_err(unusable)? else {
        return Ok(false);
    };
    Ok(entries
        .iter()
        .any(|e| fs_probe::name_ends_with(&e.file_name(), "-Data.db")))
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
        assert_eq!(
            fixture.source,
            RootSource::GitTracked,
            "`git ls-files` established the tracking, so this provenance may claim it"
        );
    }

    /// T3: the SAME path, found by the evidence walk instead, is NOT reported as the
    /// git-committed copy.
    ///
    /// Only a fetched-corpus case reaches that walk, and `resolve_fixture` has
    /// already established that git tracks no `*-Data.db` for its table — so calling
    /// this "checkout (git-committed)" told a reader the oracle was the committed copy
    /// when nothing had established that, and something had established the opposite.
    #[test]
    fn a_fixture_found_under_the_checkout_root_is_not_reported_as_git_tracked() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let checkout = tmp.path().join("checkout");
        write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");

        let fixture = corpus_fixture_in(&checkout, "ks", "t", &checkout).unwrap_or_else(|e| {
            panic!(
                "the checkout root holds the table, so the walk resolves: {}",
                match e {
                    CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => why,
                }
            )
        });
        assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
        assert_eq!(
            fixture.source,
            RootSource::CheckoutUntracked,
            "the walk established only that this root HOLDS the table"
        );
    }

    /// And the three provenances must be tellable apart in a census line, since that
    /// line is the only record of which bytes were the oracle. Pinned as a property of
    /// the tokens rather than by transcribing them: exactly one may claim git
    /// tracking, and no two may read the same.
    #[test]
    fn every_provenance_has_its_own_census_token_and_only_one_claims_git_tracking() {
        let all = [
            RootSource::GitTracked,
            RootSource::CheckoutUntracked,
            RootSource::Corpus,
        ];
        let tokens: BTreeSet<&str> = all.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            tokens.len(),
            all.len(),
            "two provenances sharing a token is the T3 defect itself: {tokens:?}"
        );
        let claim_tracking: Vec<&str> = all
            .iter()
            .map(|s| s.as_str())
            .filter(|t| t.contains("git-tracked") && !t.contains("NOT git-tracked"))
            .collect();
        assert_eq!(
            claim_tracking,
            vec![RootSource::GitTracked.as_str()],
            "only the `git ls-files`-established provenance may claim git tracking"
        );
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
                why.contains("ks.t") && why.contains("cannot be listed"),
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
    /// collapses a read failure onto `false`, so an inaccessible corpus read as
    /// absent and EVERY optional corpus case reported `NOT PRESENT` and passed. This
    /// lane's walk therefore asks `super::fs_probe` instead. And falling
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

    /// V1's own site: a `<table>-*` ENTRY the filesystem cannot DESCRIBE is
    /// unusable, and a verified-absent one is still a legal skip.
    ///
    /// `path.is_dir()` stood here and answers `false` for both, so an inaccessible
    /// candidate directory was indistinguishable from one that is not there: the
    /// root read as verifiably lacking the table and an optional corpus case passed
    /// reporting `NOT PRESENT`.
    ///
    /// Both directions are staged with symlinks (`#[cfg(unix)]`) because that is the
    /// one way to make `metadata` fail on an entry that IS in the listing without a
    /// chmod, which passes vacuously as root: a SELF-REFERENTIAL link cannot be
    /// resolved (`ELOOP`), while a DANGLING one resolves to `ENOENT`, which is an
    /// answer.
    #[cfg(unix)]
    #[test]
    fn an_undescribable_table_entry_is_unusable_and_an_absent_one_still_skips() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path().join("root");
        std::fs::create_dir_all(root.join("ks")).expect("mkdir");
        std::os::unix::fs::symlink("t-dangling-target", root.join("ks/t-dangling"))
            .expect("symlink");
        match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
            Err(CorpusMiss::Absent(why)) => assert!(
                why.contains("ks.t") || why.contains("t-"),
                "a dangling link resolves to ENOENT, which IS an answer: {why}"
            ),
            Err(CorpusMiss::Unusable(why)) => {
                panic!("a verified absence must stay a legal skip: {why}")
            }
            Ok(_) => panic!("nothing to resolve"),
        }

        // Self-referential: the entry is in the listing and cannot be described.
        std::os::unix::fs::symlink("t-loop", root.join("ks/t-loop")).expect("symlink");
        match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
            Err(CorpusMiss::Unusable(why)) => {
                assert!(
                    why.contains("t-loop") && why.contains("cannot be described"),
                    "the failure must name the entry and the cause: {why}"
                );
                assert!(
                    why.contains("NOT PRESENT"),
                    "and must say why it is not a skip: {why}"
                );
            }
            Err(CorpusMiss::Absent(why)) => {
                panic!("an entry the filesystem could not describe must not read as absent: {why}")
            }
            Ok(_) => panic!("an undescribable candidate cannot resolve"),
        }
    }

    /// The same three-valued rule ONE LEVEL IN: a fixture directory that cannot be
    /// listed is unusable, never "holds no `*-Data.db`".
    ///
    /// `has_data_db` used to end in `.unwrap_or(false)`, which made an unreadable
    /// fixture directory a verified absence and skipped the case — the finding's own
    /// shape, one level down. Classification is by `ErrorKind::NotFound` alone (in
    /// `super::fs_probe`), so a permission failure takes exactly this branch.
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

    /// The sweep's outermost site: the shared candidate LIST tests
    /// `CQLITE_DATASETS_ROOT` with `p.is_dir()`, so a value the filesystem could not
    /// describe contributes no candidate and the walk answers from the remaining
    /// candidates alone.
    ///
    /// Exercised through the pure form, because mutating the environment would race
    /// every other test in this binary. Staged as a path THROUGH a regular file,
    /// which cannot be resolved (`ENOTDIR`) — the same branch a permission failure
    /// takes, and it needs no chmod (a chmod-based case passes vacuously as root).
    #[test]
    fn an_unclassifiable_datasets_root_env_value_is_unusable_not_absent() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let file = tmp.path().join("f");
        write(&file, b"not a directory");

        let through = file.join("inside").into_os_string();
        match datasets_root_usable(Some(&through), "ks", "t") {
            Err(CorpusMiss::Unusable(why)) => assert!(
                why.contains("ks.t")
                    && why.contains("CQLITE_DATASETS_ROOT")
                    && why.contains("cannot be described"),
                "the failure must name the table, the variable and the cause: {why}"
            ),
            Err(CorpusMiss::Absent(why)) => panic!("not a verified absence: {why}"),
            Ok(state) => panic!(
                "a value that could not be classified cannot be waved \
                                through as {state:?}"
            ),
        }
    }

    /// W1: a value that IS classifiable and is not a corpus is a FAILURE too.
    ///
    /// Every one of these answered — a regular file, a nonexistent path, a directory
    /// with no `sstables/`, an `sstables` that is a file — and every one was accepted,
    /// because the check asked only whether the filesystem could answer. The shared
    /// candidate list then drops the value (not a directory) or walks past it (no
    /// `sstables/<keyspace>`), so every corpus-only case in the lane reported
    /// `NOT PRESENT` and passed with an explicitly configured corpus contributing
    /// nothing. "It is a readable path" is not "it is a corpus".
    #[test]
    fn a_configured_root_that_is_not_a_corpus_is_unusable_not_a_skip() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let tarball = tmp.path().join("datasets.tar.gz");
        write(&tarball, b"not a corpus");
        let no_sstables = tmp.path().join("parent");
        write(&no_sstables.join("some-other-tree/x"), b"x");
        let sstables_is_a_file = tmp.path().join("weird");
        write(&sstables_is_a_file.join("sstables"), b"not a directory");

        for (label, root, expected) in [
            (
                "a regular file",
                tarball,
                "is a regular file, not a directory",
            ),
            (
                "a path that does not exist",
                tmp.path().join("typo"),
                "names a path that does not exist",
            ),
            ("a directory with no sstables/", no_sstables, "is absent"),
            (
                "an sstables/ that is a file",
                sstables_is_a_file,
                "is a regular file",
            ),
        ] {
            let raw = root.clone().into_os_string();
            match datasets_root_usable(Some(&raw), "ks", "t") {
                Err(CorpusMiss::Unusable(why)) => {
                    assert!(
                        why.contains("configured but unusable")
                            && why.contains("ks.t")
                            && why.contains("CQLITE_DATASETS_ROOT")
                            && why.contains(&root.display().to_string())
                            && why.contains(expected),
                        "{label}: the failure must name the table, the variable, the \
                         root and what was wrong with it: {why}"
                    );
                    assert!(
                        why.contains("NOT PRESENT"),
                        "{label}: and must say why it is not a skip: {why}"
                    );
                }
                Err(CorpusMiss::Absent(why)) => {
                    panic!("{label}: a configured non-corpus is not a verified absence: {why}")
                }
                Ok(state) => panic!("{label}: must not be accepted as {state:?}"),
            }
        }
    }

    /// The other side of W1: the two situations that are NOT failures, and each says
    /// which one it is.
    ///
    /// An unset or blank variable asked for no corpus, and a real corpus that does not
    /// carry the table is the tier's legal skip. Blankness is judged by the SAME test
    /// the shared candidate list applies — a trim-empty value contributes no candidate
    /// there, so calling it "configured" here would fail a run whose corpus really was
    /// unset.
    #[test]
    fn an_unconfigured_root_and_a_corpus_without_the_table_are_the_two_legal_skips() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        for (label, raw) in [
            ("unset", None),
            ("blank", Some(std::ffi::OsString::from(""))),
            ("whitespace only", Some(std::ffi::OsString::from("  \t "))),
        ] {
            assert_eq!(
                datasets_root_usable(raw.as_deref(), "ks", "t").ok(),
                Some(EnvCorpus::NotConfigured),
                "{label} asked for no fetched corpus, so the case may legally skip"
            );
        }

        let corpus = tmp.path().join("corpus");
        std::fs::create_dir_all(corpus.join("sstables/other_ks")).expect("mkdir");
        let raw = corpus.clone().into_os_string();
        assert_eq!(
            datasets_root_usable(Some(&raw), "ks", "t").ok(),
            Some(EnvCorpus::Corpus {
                sstables: corpus.join("sstables")
            }),
            "a real corpus missing this table is usable — the absence is verified against it"
        );
    }

    /// And the three situations must be tellable apart in the census line, since that
    /// line is what an operator acts on: "not configured", "configured but unusable"
    /// and "valid corpus, table absent" call for three different actions.
    #[test]
    fn the_three_corpus_situations_are_distinguishable_in_the_diagnostic() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let corpus = tmp.path().join("corpus");
        std::fs::create_dir_all(corpus.join("sstables")).expect("mkdir");

        let unconfigured = EnvCorpus::NotConfigured.describe_absence();
        let absent_from_corpus = EnvCorpus::Corpus {
            sstables: corpus.join("sstables"),
        }
        .describe_absence();
        let raw = tmp.path().join("typo").into_os_string();
        let unusable = match datasets_root_usable(Some(&raw), "ks", "t") {
            Err(CorpusMiss::Unusable(why)) => why,
            other => panic!("a configured non-corpus is a failure: {other:?}"),
        };

        assert!(
            unconfigured.contains("not configured"),
            "the unset situation must say so: {unconfigured}"
        );
        assert!(
            absent_from_corpus.contains("valid corpus, table absent")
                && absent_from_corpus.contains(&corpus.join("sstables").display().to_string()),
            "the absent-from-a-real-corpus situation must say so, and name the root: \
             {absent_from_corpus}"
        );
        assert!(
            unusable.contains("configured but unusable"),
            "the unusable situation must say so: {unusable}"
        );
        let phrases = [
            "not configured",
            "configured but unusable",
            "valid corpus, table absent",
        ];
        for (label, message) in [
            ("unconfigured", &unconfigured),
            ("absent from a corpus", &absent_from_corpus),
            ("unusable", &unusable),
        ] {
            let hits: Vec<&str> = phrases
                .iter()
                .copied()
                .filter(|p| message.contains(p))
                .collect();
            assert_eq!(
                hits.len(),
                1,
                "{label}: exactly one situation may be claimed, or a reader cannot tell \
                 which it is: {hits:?} in {message}"
            );
        }
    }

    /// The end-to-end shape of W1 through the walk: an unusable configured root fails
    /// the case BEFORE any candidate is consulted, so a corpus-only case cannot skip
    /// on an absence the invalid root never established.
    #[test]
    fn an_unusable_configured_root_fails_before_the_candidate_walk() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let file = tmp.path().join("datasets.tar.gz");
        write(&file, b"not a corpus");
        // The walk itself would have SUCCEEDED from a second root holding the table,
        // so the failure is attributable to the configured root and to nothing else.
        let good = tmp.path().join("good");
        write(&good.join("ks/t-abc/nb-1-big-Data.db"), b"x");
        let raw = file.clone().into_os_string();
        match datasets_root_usable(Some(&raw), "ks", "t") {
            Err(CorpusMiss::Unusable(_)) => {}
            other => panic!("an unusable configured root must fail: {other:?}"),
        }
        let fixture = corpus_fixture_from(std::slice::from_ref(&good), "ks", "t", tmp.path())
            .unwrap_or_else(|e| match e {
                CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => {
                    panic!("the candidate walk on its own resolves: {why}")
                }
            });
        assert_eq!(fixture.dir, good.join("ks").join("t-abc"));
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
