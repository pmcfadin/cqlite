//! The git-committed fixture SET, and each committed fixture's ORACLE (issue #1491
//! review finding BB1).
//!
//! # A committed case's oracle must be committed too
//!
//! An earlier round pinned a committed case's FIXTURE to the checkout copy,
//! resolved from `git ls-files` with no environment override (finding J1) — the fix
//! that corrected 22 of this lane's 24 committed cases comparing the fleet corpus's
//! own copy of the same table. It established the trackedness of the `*-Data.db`
//! and never of the GOLDEN, and the golden IS the oracle: `compare::golden_path`
//! resolves it from the FILESYSTEM, which cannot tell a git-tracked golden from one
//! a fetched corpus, a stray local file or a previous run left in the same
//! directory. So a committed case could be certified by a non-committed oracle —
//! the same defect J1 removed, one file over.
//!
//! Everything here is answered from the `git ls-files` listing: the PAIRING rule
//! (which golden describes which committed `*-Data.db`) and, for a committed case,
//! WHICH golden is the only admissible oracle. No environment is read, for the same
//! reason `fixture_root::committed_fixture_dir` reads none — an env-settable oracle
//! is the substitution written a second way.
//!
//! Split out of `issue_1491_coverage_census.rs` (which owned the pairing rule) so
//! the census and the comparison ask ONE implementation of it: two copies could
//! drift, and a census that verified one file while the lane read another would be
//! a claim about bytes nothing read.

use super::fixture_root::{self, CommittedFixtures};
use std::collections::BTreeMap;
use std::path::Path;

/// ONE git-committed `*-Data.db`: the census's accounting unit, and the granularity
/// of this lane's coverage guarantee.
///
/// Ordered by `(keyspace, table, dir, file)`, the same order
/// [`fixture_root::selected_committed_sstable`] resolves a committed case's fixture
/// in, so "the generation the lane compares" is a comparison here and not a second
/// copy of that rule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct FixtureId {
    pub keyspace: String,
    pub table: String,
    pub dir: String,
    pub file: String,
}

impl FixtureId {
    pub fn new(keyspace: &str, table: &str, dir: &str, file: &str) -> Self {
        Self {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            dir: dir.to_string(),
            file: file.to_string(),
        }
    }

    /// How this fixture is named in the census and in every failure: the table AND
    /// the generation, because a message naming only the table cannot say WHICH
    /// generation is unaccounted for.
    pub fn describe(&self) -> String {
        format!(
            "{}.{} [{}/{}]",
            self.keyspace, self.table, self.dir, self.file
        )
    }
}

/// The git-tracked fixture set, split into what may and may not verify a claim.
#[derive(Debug)]
pub struct CommittedSet {
    /// Every git-committed `*-Data.db`, grouped exactly as
    /// [`fixture_root::committed_fixtures`] groups them for the parity lane's own
    /// resolver — one grouping rule, so the census and the lane cannot disagree
    /// about what is committed.
    pub tables: CommittedFixtures,
    /// The golden PAIRED with each committed `*-Data.db`, when git tracks one, as
    /// the repository-relative path git listed. At most one per fixture: the pairing
    /// is by name in the same directory.
    pub goldens: BTreeMap<FixtureId, String>,
    /// Orphan goldens beside a table git DOES track an SSTable for — the near
    /// misses, named in the census.
    pub orphans_of_tracked_tables: Vec<String>,
    /// Orphan goldens for a table with no tracked SSTable at all: counted, not
    /// named.
    pub orphans_of_untracked_tables: usize,
}

impl CommittedSet {
    /// Every committed fixture, flattened, in `(keyspace, table, dir, file)` order.
    pub fn fixtures(&self) -> Vec<FixtureId> {
        self.tables
            .iter()
            .flat_map(|((keyspace, table), sstables)| {
                sstables
                    .iter()
                    .map(move |(dir, file)| FixtureId::new(keyspace, table, dir, file))
            })
            .collect()
    }

    /// The generation a committed case COMPARES, and the golden git tracks for it.
    ///
    /// `Ok((id, Some(path)))` when git tracks the paired golden too;
    /// `Ok((id, None))` when it does not — the BB1 shape, which every caller must
    /// treat as a failure and none may substitute an untracked file for. `Err` when
    /// git tracks no `*-Data.db` for the table at all.
    pub fn selected(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<(FixtureId, Option<&str>), String> {
        let sstables = self.tables.get(&(keyspace.to_string(), table.to_string()));
        let Some((dir, file)) = sstables.and_then(fixture_root::selected_committed_sstable) else {
            return Err(format!(
                "{keyspace}.{table}: `git ls-files` tracks no *-Data.db for it"
            ));
        };
        let id = FixtureId::new(keyspace, table, dir, file);
        let golden = self.goldens.get(&id).map(String::as_str);
        Ok((id, golden))
    }
}

/// Split a `git ls-files` listing into the tracked SSTables and the goldens that
/// may verify a claim about each of them.
///
/// The PAIRING is `compare::golden_path`'s rule and no other: a golden verifies a
/// claim about a fixture only when it is `<that SSTable's name>.jsonl` in the SAME
/// tracked directory. The repository tracks ORPHAN goldens describing generations
/// whose SSTable is not committed, so a union over every tracked JSONL let an orphan
/// supply an exclusion's declared unsupported shape while the committed SSTable's own
/// golden was comparable — the census and the resolver disagreeing about which bytes
/// are the oracle (issue #1491 review finding T2).
///
/// Pure and separated from its callers so both halves of the rule are testable
/// against a synthetic listing; the census and the lane read the real repository,
/// where the orphan shape exists but the near-miss it would license does not.
pub fn committed_set(listing: &[String]) -> Result<CommittedSet, String> {
    let tables = fixture_root::committed_fixtures(listing)?;
    let mut goldens: BTreeMap<FixtureId, String> = BTreeMap::new();
    let mut orphans_of_tracked_tables: Vec<String> = Vec::new();
    let mut orphans_of_untracked_tables = 0usize;
    for line in listing {
        // The same path parser the fixture-root selection uses, so "committed" means
        // one thing in this lane: an unrecognised shape is refused, not guessed at.
        let Some(path) = fixture_root::classify(line)? else {
            continue;
        };
        if !path.is_golden {
            continue;
        }
        let Some(sstable) = path.file.strip_suffix(".jsonl") else {
            // `classify` only reports `is_golden` for a `-Data.db.jsonl` path, so
            // this cannot happen; reported rather than skipped so a change to that
            // rule cannot silently drop a golden from the verification set.
            return Err(format!("a golden path without a `.jsonl` suffix: {line}"));
        };
        let table_key = (path.keyspace.clone(), path.table.clone());
        let paired = tables
            .get(&table_key)
            .is_some_and(|s| s.contains(&(path.dir.clone(), sstable.to_string())));
        if paired {
            let id = FixtureId::new(&path.keyspace, &path.table, &path.dir, sstable);
            // A second golden for the SAME fixture is impossible from `git ls-files`
            // (paths are unique) and is refused rather than silently overwritten, so
            // an injected or future listing cannot make one fixture's oracle depend
            // on iteration order.
            if let Some(first) = goldens.insert(id.clone(), line.to_string()) {
                return Err(format!(
                    "two goldens pair with {}: {first} and {line}",
                    id.describe()
                ));
            }
        } else if tables.contains_key(&table_key) {
            // A golden BESIDE a tracked table but describing another generation —
            // the only orphan that could ever have justified an exclusion, so it is
            // named rather than counted.
            orphans_of_tracked_tables.push(format!("{}.{}: {line}", path.keyspace, path.table));
        } else {
            orphans_of_untracked_tables += 1;
        }
    }
    Ok(CommittedSet {
        tables,
        goldens,
        orphans_of_tracked_tables,
        orphans_of_untracked_tables,
    })
}

/// The repository-relative path of the golden PAIRED with one committed
/// `*-Data.db` — the pairing rule written as a path, so every caller that has to
/// name an UNTRACKED oracle names the one file git would have to track, in one
/// spelling.
pub fn paired_golden_rel(id: &FixtureId) -> String {
    format!(
        "test-data/datasets/sstables/{}/{}/{}.jsonl",
        id.keyspace, id.dir, id.file
    )
}

/// The oracle a COMMITTED case was compared against must BE the git-tracked golden
/// paired with the git-tracked `*-Data.db` (issue #1491 review finding BB1).
///
/// `resolved` is what `compare::golden_path` found on the filesystem — a question
/// about bytes on disk, which cannot establish trackedness. This asks the git
/// listing the same question and requires the two answers to be the same file, so
/// an untracked golden beside a tracked SSTable is a FAILURE naming the path and
/// saying it is untracked, never a silent substitution and never a fallback to a
/// corpus copy.
///
/// The expected path is built from the listing exactly as
/// `fixture_root::committed_fixture_dir` builds the fixture's: `<checkout>` +
/// keyspace + the tracked directory + the tracked file name, with `.jsonl`
/// appended by the pairing rule. Both names come from the one listing, so the
/// comparison below is between two paths of the same provenance.
pub fn require_tracked_oracle(
    set: &CommittedSet,
    keyspace: &str,
    table: &str,
    checkout: &Path,
    resolved: &Path,
) -> Result<(), String> {
    let (id, golden) = set.selected(keyspace, table)?;
    let expected_rel = paired_golden_rel(&id);
    let Some(tracked) = golden else {
        return Err(format!(
            "{}: the golden that would describe it, {expected_rel}, is NOT git-tracked \
             — a committed case's ORACLE is committed too, so an untracked golden (a \
             fetched corpus copy, a stray local file, a previous run's leftover) may \
             never certify it",
            id.describe()
        ));
    };
    if tracked != expected_rel {
        // Unreachable while the pairing rule is the one above; reported rather than
        // trusted, so a change to `committed_set` cannot silently make the tracked
        // path and the expected path two different files.
        return Err(format!(
            "{}: git tracks {tracked} as its paired golden, but the pairing rule names \
             {expected_rel}",
            id.describe()
        ));
    }
    let expected = checkout
        .join(&id.keyspace)
        .join(&id.dir)
        .join(format!("{}.jsonl", id.file));
    if resolved != expected.as_path() {
        return Err(format!(
            "{}: compared against {}, but the git-tracked oracle is {} — a committed \
             case is compared against the committed golden and only it",
            id.describe(),
            resolved.display(),
            expected.display()
        ));
    }
    Ok(())
}

#[path = "golden_committed_set_tests.rs"]
#[cfg(test)]
mod tests;
