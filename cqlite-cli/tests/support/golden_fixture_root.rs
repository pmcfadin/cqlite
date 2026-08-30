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
        // `-z`: NUL-separated and, with it, UNQUOTED. Without `-z` git renders an
        // unusual path through `core.quotePath` — `"…/\303\251.db"`, quotes and
        // C-escapes included — so the listing's own spelling of a path would depend
        // on a git config, and a newline in a name would end the "line".
        .args(["ls-files", "-z", "test-data/datasets/sstables"])
        .current_dir(&root)
        .output()
        .map_err(|e| format!("cannot run `git ls-files` in {}: {e}", root.display()))?;
    if !output.status.success() {
        return Err(format!(
            "`git ls-files` failed in {}: {}",
            root.display(),
            // A MESSAGE, so lossy is right here: nothing decides anything from it.
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    parse_listing(&output.stdout)
}

/// PURE form of [`committed_listing`]: split a `git ls-files -z` buffer into paths.
///
/// A path that is not valid UTF-8 is REFUSED, never converted lossily. This is the
/// same rule the golden pairing follows for a `Path` (findings W2/L3), applied to a
/// listing: `String::from_utf8_lossy` substitutes ONE U+FFFD per invalid byte, so
/// two DISTINCT tracked fixtures can converge onto ONE string — and the committed
/// set keys on those strings (a `BTreeSet` of `(dir, file)` per table), so the
/// second fixture would not be reported as a conflict, it would silently VANISH
/// from the census and from the coverage claim built on it. A lossy conversion is
/// fine in a diagnostic and never where something is keyed, compared or opened.
///
/// The refusal is fail-closed and loud: every consumer here propagates it.
fn parse_listing(stdout: &[u8]) -> Result<Vec<String>, String> {
    stdout
        .split(|b| *b == 0)
        .filter(|element| !element.is_empty())
        .map(|element| {
            std::str::from_utf8(element)
                .map(str::to_string)
                .map_err(|why| {
                    format!(
                        "`git ls-files -z` listed a path that is not valid UTF-8 \
                         ({} bytes, {why}); read lossily it could collide with another \
                         tracked fixture and drop it from the census: {}",
                        element.len(),
                        String::from_utf8_lossy(element)
                    )
                })
        })
        .collect()
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
        source: if is_checkout_root(root, checkout)? {
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

/// Is the root that won the walk the checkout's own `sstables/` root?
///
/// Asked of RESOLVED paths, because the question is about the OBJECT and not about
/// the spelling (issue #1491 review finding BB2). Lexical `Path` equality answered
/// `false` for a root that IS the checkout written differently — a relative
/// `CQLITE_DATASETS_ROOT`, one with a `..` component, a symlink into the checkout —
/// so the census reported `fetched corpus` for bytes that came from the checkout.
/// The provenance line is the only record of which bytes were the oracle, so a line
/// that can be wrong is worse than none, which is why an unresolvable path is a
/// named FAILURE here and not a guess.
fn is_checkout_root(root: &Path, checkout: &Path) -> Result<bool, CorpusMiss> {
    let unusable = |why: String| {
        CorpusMiss::Unusable(format!(
            "{why}. The provenance of a fixture cannot be established without              resolving both paths, and a census that misnames the oracle is worse              than one that says nothing"
        ))
    };
    let Some(resolved_root) = fs_probe::canonical(root).map_err(unusable)? else {
        // `root` answered a moment ago that it HOLDS this table, so a verified
        // absence now is a race — reported, never resolved by assuming which root
        // this is.
        return Err(unusable(format!(
            "{} held the table and then verifiably did not exist",
            root.display()
        )));
    };
    // The checkout's own `sstables/` root need not exist on every machine (a
    // corpus-only keyspace, a checkout with no committed fixtures at all); a
    // VERIFIED absence is an answer, and a root that does exist is not it.
    match fs_probe::canonical(checkout).map_err(unusable)? {
        Some(resolved_checkout) => Ok(resolved_root == resolved_checkout),
        None => Ok(false),
    }
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
#[path = "golden_fixture_root_tests.rs"]
mod tests;
