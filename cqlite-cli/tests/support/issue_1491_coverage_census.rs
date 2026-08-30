//! The COVERAGE CENSUS half of the AD2 lane (issue #1491): every git-committed
//! `*-Data.db` fixture is either a compared case or a NAMED, verified exclusion.
//!
//! Split out of `issue_1491_json_csv_golden_parity.rs` under the campsite rule
//! (CLAUDE.md, epic #1135), which had reached the ~1500-line test-file target. A
//! different question from egress parity: this half asks which fixtures the lane
//! is accountable for, and answers it from `git ls-files` plus the goldens
//! themselves — never from a hand-kept count.
//!
//! Declared as a child module of that lane, so [`super::CASES`] (the compared set)
//! and its helpers are the same declarations the comparison uses; there is no
//! second copy of the case table to drift.
//!
//! # The UNIT is one committed `*-Data.db`, not one table
//!
//! The guarantee above is per FIXTURE, and the accounting used to be per
//! `(keyspace, table)`: a table's tracked goldens were unioned and the union was
//! asked whether it carried an exclusion's declared shapes. So a table that gained
//! a SECOND committed generation whose golden is comparable still validated —
//! another generation supplied the declared shape — while the new fixture itself was
//! compared by nothing and excluded by nothing (issue #1491 review round 21). That is
//! the third and finest pass at one theme: round 6 compared only the
//! lexicographically first generation (fixed by pairing each golden to its SSTable by
//! name), round 13 unioned every tracked JSONL including orphans with no committed
//! SSTable (fixed to paired goldens only), and this one fixes the UNIT — every
//! committed `(keyspace, table, directory, *-Data.db)` is now classified in its own
//! right, and an exclusion is verified against THAT generation's own paired golden.
//!
//! Measured on this checkout when the unit changed: 32 committed `*-Data.db` across
//! 32 tables, i.e. no table has a second tracked generation today, so the defect was
//! LATENT and no verdict moved. The guard is pinned against synthetic listings in
//! [`per_fixture`] instead, because the census reads `git ls-files` under the
//! compile-time checkout anchor with no environment override — a scratch tree cannot
//! reach it without staging files in the real repository, which this lane never does.

use super::{repo_root, Presence, CASES};
use crate::golden::dump_shapes::{unsupported_shapes, Unsupported};
use crate::golden::{fixture_root, golden_rows};
use std::collections::{BTreeMap, BTreeSet};

/// A committed table that CANNOT be compared this way, with the shapes that make
/// it so.
struct Excluded {
    keyspace: &'static str,
    table: &'static str,
    /// The shapes EVERY committed generation of this table carries. VERIFIED
    /// per generation by [`census`] against that generation's own paired golden —
    /// declaring the wrong set, or a set that has gone stale for any one of them,
    /// fails.
    shapes: &'static [Unsupported],
    /// Human context for the census line. Never load-bearing: the `shapes` set is
    /// what is checked.
    note: &'static str,
}

/// Committed tables that CANNOT be compared this way, and why.
///
/// Each reason is a *read-time reconciliation* property: the physical dump
/// enumerates on-disk cells including shadowed/expired ones, so the CLI's
/// reconciled `SELECT` result set is legitimately a different set of rows.
/// Weakening the value comparison to absorb that would defeat the point of the
/// lane, so those tables are excluded by name instead.
///
/// The `shapes` set is CHECKED, not trusted: the census requires it to equal the
/// set [`unsupported_shapes`] finds in the golden of EACH committed generation of
/// the table. Equality, not containment — a golden that grows a shape the entry does
/// not name is a declaration that has stopped describing its subject, which is the
/// same defect as one that names a shape the golden never had (issue #1491 review
/// finding F4).
///
/// And each generation is checked against ITS OWN paired golden, by the same
/// `<gen>-Data.db` + `.jsonl` rule `compare::golden_path` resolves a case's oracle
/// with. Two earlier rules were both too coarse: unioning every tracked JSONL let an
/// ORPHAN golden — the repository tracks goldens for generations whose SSTable is not
/// committed — supply the declared shape (finding T2), and unioning a table's PAIRED
/// goldens let one committed generation supply the shape for another whose golden is
/// comparable and untested (round 21). An entry therefore excludes a table only while
/// every committed generation of it is genuinely not comparable.
const NOT_COMPARABLE: &[Excluded] = &[
    Excluded {
        keyspace: "test_big",
        table: "wide_partition",
        shapes: &[Unsupported::RangeTombstone],
        note: "range tombstone bounds in the dump",
    },
    Excluded {
        keyspace: "test_compaction_tombstone_ttl",
        table: "rt_cross_gen",
        shapes: &[Unsupported::RangeTombstone],
        note: "range tombstone bounds and boundaries across two generations",
    },
    Excluded {
        keyspace: "test_compaction_tombstone_ttl",
        table: "shadow_row_delete",
        shapes: &[Unsupported::RowDeletion],
        note: "a row deletion marker the dump keeps and a SELECT drops",
    },
    Excluded {
        keyspace: "test_compaction_tombstone_ttl",
        table: "ttl_expired_live",
        shapes: &[Unsupported::Ttl],
        // The golden also carries cell deletions, which are NOT listed: a cell
        // tombstone reconciles to null and this lane compares it (see
        // `test_types.nb_absent_vs_null_regular`), so it is not a reason to
        // exclude anything. Only shapes the golden reader REFUSES may be listed.
        note: "TTL expiry: expired cells the dump keeps and a SELECT drops",
    },
    Excluded {
        keyspace: "test_da",
        table: "ttl_table",
        shapes: &[Unsupported::Ttl],
        note: "row TTL",
    },
    Excluded {
        keyspace: "test_deltas",
        table: "static_with_rows",
        shapes: &[Unsupported::StaticBlock],
        note: "static block: static-column projection is reconciliation",
    },
    Excluded {
        keyspace: "test_tomb",
        table: "static_with_tombstones",
        shapes: &[
            Unsupported::RangeTombstone,
            Unsupported::RowDeletion,
            Unsupported::StaticBlock,
        ],
        note: "static block, row deletions and range tombstone bounds together",
    },
    Excluded {
        keyspace: "test_writeparity",
        table: "static_clustering_shape",
        shapes: &[Unsupported::StaticBlock],
        note: "static block",
    },
];

/// ONE git-committed `*-Data.db`: the census's unit, and the granularity of its
/// guarantee.
///
/// Ordered by `(keyspace, table, dir, file)`, the same order
/// [`fixture_root::selected_committed_sstable`] resolves a committed case's fixture
/// in, so "the generation the lane compares" is a comparison here and not a second
/// copy of that rule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct FixtureId {
    keyspace: String,
    table: String,
    dir: String,
    file: String,
}

impl FixtureId {
    fn new(keyspace: &str, table: &str, dir: &str, file: &str) -> Self {
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
    fn describe(&self) -> String {
        format!(
            "{}.{} [{}/{}]",
            self.keyspace, self.table, self.dir, self.file
        )
    }
}

/// The git-tracked fixture set, split into what may and may not verify a claim.
#[derive(Debug)]
struct Tracked {
    /// Every git-committed `*-Data.db`, grouped exactly as
    /// [`fixture_root::committed_fixtures`] groups them for the parity lane's own
    /// resolver — one grouping rule, so the census and the lane cannot disagree
    /// about what is committed.
    tables: fixture_root::CommittedFixtures,
    /// The golden PAIRED with each committed `*-Data.db`, when git tracks one. At
    /// most one per fixture: the pairing is by name in the same directory.
    goldens: BTreeMap<FixtureId, String>,
    /// Orphan goldens beside a table git DOES track an SSTable for — the near
    /// misses, named in the census.
    orphans_of_tracked_tables: Vec<String>,
    /// Orphan goldens for a table with no tracked SSTable at all: counted, not
    /// named.
    orphans_of_untracked_tables: usize,
}

impl Tracked {
    /// Every committed fixture, flattened, in `(keyspace, table, dir, file)` order.
    fn fixtures(&self) -> Vec<FixtureId> {
        self.tables
            .iter()
            .flat_map(|((keyspace, table), sstables)| {
                sstables
                    .iter()
                    .map(move |(dir, file)| FixtureId::new(keyspace, table, dir, file))
            })
            .collect()
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
/// Pure and separated from the census so both halves of the rule are testable
/// against a synthetic listing; the census reads the real repository, where the
/// orphan shape exists but the near-miss it would license does not.
fn tracked_fixtures(listing: &[String]) -> Result<Tracked, String> {
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
    Ok(Tracked {
        tables,
        goldens,
        orphans_of_tracked_tables,
        orphans_of_untracked_tables,
    })
}

/// What the census established, and what it refuses.
#[derive(Default, Debug)]
struct Census {
    /// Committed fixtures the lane COMPARES.
    compared: usize,
    /// Committed fixtures excluded by an entry whose declared shapes were VERIFIED
    /// in that fixture's own paired golden.
    excluded: usize,
    /// One line per verified exclusion, for the run's census output.
    lines: Vec<String>,
    /// Committed fixtures that are neither compared nor excluded — the coverage hole
    /// this census exists to make loud.
    unclassified: Vec<String>,
    /// Declarations that no longer describe their subject.
    stale: Vec<String>,
}

fn labels(set: &BTreeSet<Unsupported>) -> String {
    set.iter().map(|s| s.label()).collect::<Vec<_>>().join(", ")
}

/// Classify EVERY committed fixture, and verify every exclusion against the golden
/// paired with the generation it excludes.
///
/// Pure: the listing arrives already split by [`tracked_fixtures`], the compared set
/// and the exclusions are parameters, and golden bytes come from `read_golden`. So
/// the real test can run it over the checkout while [`per_fixture`] runs it over
/// synthetic listings the checkout does not contain.
///
/// `cases` is the `(keyspace, table)` of every entry in [`CASES`] — including the
/// `Presence::Corpus` tier, deliberately: whether a case's declared tier matches what
/// git tracks is the parity lane's own `Presence` cross-check, and duplicating it here
/// would report one defect as two.
fn census(
    tracked: &Tracked,
    cases: &BTreeSet<(&str, &str)>,
    exclusions: &[Excluded],
    read_golden: &dyn Fn(&str) -> Result<String, String>,
) -> Census {
    let mut out = Census::default();
    for ((keyspace, table), sstables) in &tracked.tables {
        let qualified = format!("{keyspace}.{table}");
        let is_case = cases.contains(&(keyspace.as_str(), table.as_str()));
        let entry = exclusions
            .iter()
            .find(|e| e.keyspace == keyspace && e.table == table);
        if is_case && entry.is_some() {
            // A table-level contradiction, reported once: every generation of it is
            // unaccounted for, and saying so N times would bury the cause.
            out.unclassified.push(format!(
                "{qualified} is BOTH a comparable case and a declared exclusion"
            ));
            continue;
        }
        // The generation the lane actually stages, asked of the resolver rather than
        // re-derived here.
        let selected = fixture_root::selected_committed_sstable(sstables);
        for sstable in sstables {
            let (dir, file) = sstable;
            let id = FixtureId::new(keyspace, table, dir, file);
            if is_case {
                if Some(sstable) == selected {
                    out.compared += 1;
                } else {
                    // The case compares exactly one generation, so a second one is
                    // covered by nothing. In a SEPARATE directory that used to be a
                    // declared narrowing of the parity lane's own census (finding L3)
                    // while this census still called the table covered; in the SAME
                    // directory `compare::golden_path` additionally REFUSES to stage
                    // it, so the parity case fails too. The two agree — both are red —
                    // and this one names the generation that nothing read.
                    out.unclassified.push(format!(
                        "{}: this generation is compared by nothing (the case compares \
                         {}) and no NOT_COMPARABLE entry excludes it",
                        id.describe(),
                        selected
                            .map(|(d, f)| format!("{d}/{f}"))
                            .unwrap_or_else(|| "nothing".to_string())
                    ));
                }
            } else if let Some(entry) = entry {
                match verify_exclusion(&id, entry, tracked, read_golden) {
                    Ok(line) => {
                        out.excluded += 1;
                        out.lines.push(line);
                    }
                    Err(why) => out.stale.push(why),
                }
            } else {
                out.unclassified.push(format!(
                    "{}: neither a CASES entry nor a NOT_COMPARABLE entry",
                    id.describe()
                ));
            }
        }
    }

    // A declared exclusion must name a table that exists and state a reason at all.
    // Whether the reason DESCRIBES each generation is checked per fixture above; this
    // sweep catches the two things a per-fixture loop cannot see, because an entry
    // with no fixtures is visited by nothing.
    for entry in exclusions {
        let qualified = format!("{}.{}", entry.keyspace, entry.table);
        if !tracked
            .tables
            .contains_key(&(entry.keyspace.to_string(), entry.table.to_string()))
        {
            out.stale.push(format!(
                "{qualified} ({}) names no committed fixture",
                entry.note
            ));
            continue;
        }
        if entry.shapes.is_empty() {
            out.stale.push(format!(
                "{qualified}: an exclusion with no declared shape states no reason at all"
            ));
        }
    }
    out
}

/// One generation of an excluded table, verified against ITS OWN paired golden.
///
/// `Ok` with the census line, or `Err` with what stopped the verification. No
/// paired golden at all is a FAILURE, not a pass: an exclusion no golden can
/// corroborate is exactly the unverifiable claim this check exists for — and neither
/// an orphan golden beside the fixture (finding T2) nor another generation's golden
/// (round 21) makes it verifiable, so the message says which bytes were missing.
fn verify_exclusion(
    id: &FixtureId,
    entry: &Excluded,
    tracked: &Tracked,
    read_golden: &dyn Fn(&str) -> Result<String, String>,
) -> Result<String, String> {
    let declared: BTreeSet<Unsupported> = entry.shapes.iter().copied().collect();
    let Some(file) = tracked.goldens.get(id) else {
        return Err(format!(
            "{}: no committed *-Data.db.jsonl golden PAIRED with THIS *-Data.db, so the \
             declared shapes [{}] are not verified for this generation",
            id.describe(),
            labels(&declared)
        ));
    };
    let text = read_golden(file).map_err(|why| format!("{}: {file}: {why}", id.describe()))?;
    let present =
        unsupported_shapes(&text).map_err(|why| format!("{}: {file}: {why}", id.describe()))?;
    if present != declared {
        return Err(format!(
            "{}: declares [{}] but its OWN paired golden {file} carries [{}] — the \
             exclusion does not describe this generation",
            id.describe(),
            labels(&declared),
            labels(&present)
        ));
    }
    Ok(format!(
        "{} EXCLUDED, verified in its paired golden {file}: {} ({})",
        id.describe(),
        labels(&declared),
        entry.note
    ))
}

/// Every git-committed `*-Data.db` fixture is either a comparable case or a NAMED,
/// reasoned exclusion. Derived from committed source at run time, so a newly
/// committed fixture — including a second generation of a table already covered —
/// must be classified instead of being silently uncovered.
#[test]
fn committed_fixture_coverage_census() {
    let root = repo_root();
    let listing = fixture_root::committed_listing()
        .unwrap_or_else(|why| panic!("cannot read the committed fixture set: {why}"));
    let tracked = tracked_fixtures(&listing).unwrap_or_else(|why| panic!("{why}"));

    eprintln!(
        "AD2 census: {} tracked golden(s) PAIRED with a tracked *-Data.db; {} orphan(s) \
         beside a tracked table and {} for tables with no tracked SSTable at all — an \
         orphan describes a generation this checkout does not carry, so it verifies \
         nothing",
        tracked.goldens.len(),
        tracked.orphans_of_tracked_tables.len(),
        tracked.orphans_of_untracked_tables
    );
    for orphan in &tracked.orphans_of_tracked_tables {
        eprintln!("AD2 census:   ORPHAN golden (not the paired oracle) {orphan}");
    }
    let fixtures = tracked.fixtures();
    assert!(
        !fixtures.is_empty(),
        "no committed *-Data.db fixtures found under {} — the census has no subject",
        root.display()
    );

    let cases: BTreeSet<(&str, &str)> = CASES.iter().map(|c| (c.keyspace, c.table)).collect();
    let report = census(&tracked, &cases, NOT_COMPARABLE, &|file| {
        std::fs::read_to_string(root.join(file)).map_err(|e| e.to_string())
    });
    for line in &report.lines {
        eprintln!("AD2 census: {line}");
    }
    eprintln!(
        "AD2 census: {} committed *-Data.db fixture(s) across {} table(s) — {} compared, \
         {} excluded by {} declared entr(ies); plus {} fetched-corpus case(s) whose \
         fixtures git tracks no *-Data.db for",
        fixtures.len(),
        tracked.tables.len(),
        report.compared,
        report.excluded,
        NOT_COMPARABLE.len(),
        CASES
            .iter()
            .filter(|c| c.presence != Presence::Committed)
            .count()
    );
    assert!(
        report.unclassified.is_empty(),
        "every committed *-Data.db fixture must be classified (compared, or excluded \
         with a reason) — issue #1491:\n  {}",
        report.unclassified.join("\n  ")
    );
    assert!(
        report.stale.is_empty(),
        "every NOT_COMPARABLE entry must name shapes the golden paired with each \
         committed generation really carries — issue #1491:\n  {}",
        report.stale.join("\n  ")
    );
    // The accounting, stated affirmatively: with nothing unclassified and nothing
    // stale, the two verdicts must add up to the fixture set — so a fixture cannot be
    // dropped by a branch that neither counted it nor complained.
    assert_eq!(
        report.compared + report.excluded,
        fixtures.len(),
        "every committed fixture must be accounted for exactly once: {} compared + {} \
         excluded against {} committed *-Data.db",
        report.compared,
        report.excluded,
        fixtures.len()
    );
}

/// The other half of the exclusion contract: every shape an entry may declare is
/// one the golden reader REFUSES.
///
/// Without this the two halves could drift — the census would verify that a shape
/// is in the golden while the reader happily parsed it, so the table was
/// comparable after all and the exclusion was pure coverage loss. Each minimal
/// golden carries exactly one shape and is otherwise a well-formed, comparable
/// single-column row.
#[test]
fn every_declarable_shape_is_one_the_golden_reader_refuses() {
    // The list's own integrity: sorted and duplicate-free, so an entry cannot be
    // a silent copy of its neighbour (see the note on `Unsupported::ALL` for what
    // this can and cannot establish).
    let mut sorted: Vec<Unsupported> = Unsupported::ALL.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.as_slice(),
        Unsupported::ALL,
        "Unsupported::ALL must be sorted and duplicate-free"
    );

    // A baseline the reader ACCEPTS, so a refusal below is attributable to the
    // shape under test rather than to the scaffolding.
    let rows =
        golden_rows(LIVE_GOLDEN, &["id"], &[], &[]).expect("the baseline golden is comparable");
    assert_eq!(rows.len(), 1, "the baseline must yield its one row");

    for shape in Unsupported::ALL {
        let jsonl = shape.minimal_golden();
        assert_eq!(
            unsupported_shapes(jsonl).map(|s| s.into_iter().collect::<Vec<_>>()),
            Ok(vec![*shape]),
            "the shape scan must find exactly `{}` in its own minimal golden",
            shape.label()
        );
        let why = golden_rows(jsonl, &["id"], &[], &[]).expect_err(&format!(
            "a golden carrying `{}` must be REFUSED — otherwise excluding a table for \
             it is pure coverage loss",
            shape.label()
        ));
        assert!(!why.is_empty(), "a refusal must state a reason");
    }
}

/// A comparable golden: one live row, no shape any exclusion could rest on. Shared
/// by the refusal test above (as its accepted baseline) and by [`per_fixture`] (as
/// the golden of a generation that is NOT excludable).
const LIVE_GOLDEN: &str = r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z"},"cells":[{"name":"v","value":"x"}]}]}"#;

/// The pairing and per-fixture rules, pinned against synthetic listings.
///
/// The census reads `git ls-files` under the compile-time checkout anchor with no
/// environment override (`fixture_root::committed_listing`), so a scratch tree cannot
/// be substituted for the repository and the shapes below cannot be demonstrated by
/// synthesizing files — that would mean staging fixtures in the real checkout, which
/// this lane never does. They are pinned here instead, over listings the checkout does
/// not contain: today it tracks 32 committed `*-Data.db` across 32 tables, so no table
/// has a second generation and every shape below is LATENT.
#[cfg(test)]
mod per_fixture {
    use super::*;

    fn listing(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|l| (*l).to_string()).collect()
    }

    /// The injected golden reader: an unknown path is an ERROR, never empty text, so
    /// a test cannot pass because a golden it meant to supply was never read.
    fn reader<'a>(texts: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Result<String, String> + 'a {
        move |path: &str| {
            texts
                .iter()
                .find(|(p, _)| *p == path)
                .map(|(_, text)| (*text).to_string())
                .ok_or_else(|| format!("no injected golden for {path}"))
        }
    }

    const DIR: &str = "test-data/datasets/sstables/ks/t-abc";
    const DIR2: &str = "test-data/datasets/sstables/ks/t-def";
    const CASE: fn() -> BTreeSet<(&'static str, &'static str)> = || BTreeSet::from([("ks", "t")]);
    const NO_CASES: fn() -> BTreeSet<(&'static str, &'static str)> = BTreeSet::new;

    const STATIC_BLOCK: &[Excluded] = &[Excluded {
        keyspace: "ks",
        table: "t",
        shapes: &[Unsupported::StaticBlock],
        note: "static block",
    }];

    fn static_block_golden() -> &'static str {
        Unsupported::StaticBlock.minimal_golden()
    }

    /// One committed generation, compared: the positive control every refusal below
    /// is measured against.
    #[test]
    fn the_generation_the_resolver_stages_is_the_compared_one() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        ]))
        .expect("classifies");

        let report = census(&tracked, &CASE(), &[], &reader(&[]));
        assert_eq!(report.compared, 1);
        assert!(report.unclassified.is_empty(), "{:?}", report.unclassified);
        assert!(report.stale.is_empty(), "{:?}", report.stale);
    }

    /// A SECOND committed generation of a compared table is compared by nothing: the
    /// case stages one fixture, so the census names the other rather than reporting
    /// the table as covered.
    #[test]
    fn a_second_generation_of_a_compared_table_is_compared_by_nothing() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            &format!("{DIR2}/nb-9-big-Data.db"),
            &format!("{DIR2}/nb-9-big-Data.db.jsonl"),
        ]))
        .expect("classifies");

        let report = census(&tracked, &CASE(), &[], &reader(&[]));
        assert_eq!(
            report.compared, 1,
            "the case stages exactly one generation — `selected_committed_sstable`'s"
        );
        assert_eq!(report.unclassified.len(), 1, "{:?}", report.unclassified);
        let named = &report.unclassified[0];
        assert!(
            named.contains("t-def/nb-9-big-Data.db") && named.contains("t-abc/nb-1-big-Data.db"),
            "the refusal must name the uncovered generation AND the one compared: {named}"
        );
    }

    /// THE ROUND-21 DEFECT. An excluded table gains a second committed generation
    /// whose golden is COMPARABLE. The union over the table's paired goldens still
    /// carries the declared shape — that union is what the table-level unit checked,
    /// and it accepted exactly this listing — so the exclusion validated while the new
    /// generation was compared by nothing and excluded by nothing. Per fixture, the
    /// comparable generation is refused by name.
    #[test]
    fn an_exclusion_is_verified_against_each_generations_own_golden() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            &format!("{DIR2}/nb-9-big-Data.db"),
            &format!("{DIR2}/nb-9-big-Data.db.jsonl"),
        ]))
        .expect("classifies");
        let texts = [
            (
                format!("{DIR}/nb-1-big-Data.db.jsonl"),
                static_block_golden(),
            ),
            (format!("{DIR2}/nb-9-big-Data.db.jsonl"), LIVE_GOLDEN),
        ];
        let injected: Vec<(&str, &str)> = texts.iter().map(|(p, t)| (p.as_str(), *t)).collect();

        // The evidence the coarser unit accepted, stated as a fact about this
        // listing rather than by re-running the retired rule: unioned over the
        // table's PAIRED goldens, the declared shape is present.
        let mut union: BTreeSet<Unsupported> = BTreeSet::new();
        for file in tracked.goldens.values() {
            union.extend(
                unsupported_shapes(&reader(&injected)(file).expect("injected")).expect("parses"),
            );
        }
        assert_eq!(
            union,
            BTreeSet::from([Unsupported::StaticBlock]),
            "the union over both goldens carries the declared shape, which is why the \
             table-level unit passed this listing"
        );

        let report = census(&tracked, &NO_CASES(), STATIC_BLOCK, &reader(&injected));
        assert_eq!(
            report.excluded, 1,
            "only the generation whose OWN golden carries the shape is excluded"
        );
        assert_eq!(report.stale.len(), 1, "{:?}", report.stale);
        let why = &report.stale[0];
        assert!(
            why.contains("t-def/nb-9-big-Data.db")
                && why.contains("static block")
                && why.contains("does not describe this generation"),
            "the refusal must name the generation the exclusion no longer describes: {why}"
        );
        assert!(
            report.unclassified.is_empty(),
            "the shape is a STALE declaration, not an unclassified fixture: {:?}",
            report.unclassified
        );
    }

    /// And a table whose EVERY committed generation carries the declared shape is
    /// still excludable — the finer unit refuses unverified generations, not
    /// multi-generation exclusions.
    #[test]
    fn an_exclusion_every_generation_corroborates_still_verifies() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            &format!("{DIR2}/nb-9-big-Data.db"),
            &format!("{DIR2}/nb-9-big-Data.db.jsonl"),
        ]))
        .expect("classifies");
        let texts = [
            (
                format!("{DIR}/nb-1-big-Data.db.jsonl"),
                static_block_golden(),
            ),
            (
                format!("{DIR2}/nb-9-big-Data.db.jsonl"),
                static_block_golden(),
            ),
        ];
        let injected: Vec<(&str, &str)> = texts.iter().map(|(p, t)| (p.as_str(), *t)).collect();

        let report = census(&tracked, &NO_CASES(), STATIC_BLOCK, &reader(&injected));
        assert_eq!(report.excluded, 2);
        assert_eq!(report.lines.len(), 2, "each generation gets its own line");
        assert!(report.stale.is_empty(), "{:?}", report.stale);
        assert!(report.unclassified.is_empty(), "{:?}", report.unclassified);
    }

    /// An excluded generation with no golden of its OWN is refused: another
    /// generation's golden, and an orphan beside it, both describe other bytes.
    #[test]
    fn an_excluded_generation_with_no_paired_golden_is_refused() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            // Tracked, and its `-Data.db.jsonl` is not.
            &format!("{DIR2}/nb-9-big-Data.db"),
        ]))
        .expect("classifies");
        let texts = [(
            format!("{DIR}/nb-1-big-Data.db.jsonl"),
            static_block_golden(),
        )];
        let injected: Vec<(&str, &str)> = texts.iter().map(|(p, t)| (p.as_str(), *t)).collect();

        let report = census(&tracked, &NO_CASES(), STATIC_BLOCK, &reader(&injected));
        assert_eq!(report.excluded, 1);
        assert_eq!(report.stale.len(), 1, "{:?}", report.stale);
        assert!(
            report.stale[0].contains("t-def/nb-9-big-Data.db")
                && report.stale[0].contains("PAIRED with THIS *-Data.db"),
            "{}",
            report.stale[0]
        );
    }

    /// A golden that cannot be READ is a refusal, never an empty shape set: "I could
    /// not tell" must not read as "this generation carries nothing".
    #[test]
    fn an_unreadable_golden_is_refused_not_read_as_empty() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        ]))
        .expect("classifies");

        let report = census(&tracked, &NO_CASES(), STATIC_BLOCK, &reader(&[]));
        assert_eq!(report.excluded, 0);
        assert_eq!(report.stale.len(), 1, "{:?}", report.stale);
        assert!(
            report.stale[0].contains("no injected golden for"),
            "the read failure must be reported verbatim: {}",
            report.stale[0]
        );
    }

    /// An entry naming no committed fixture is stale — an exclusion with nothing to
    /// exclude, which a per-fixture loop alone cannot see.
    #[test]
    fn an_exclusion_naming_no_committed_fixture_is_stale() {
        let tracked = tracked_fixtures(&listing(&[
            "test-data/datasets/sstables/ks/other-abc/nb-1-big-Data.db",
        ]))
        .expect("classifies");

        let report = census(&tracked, &NO_CASES(), STATIC_BLOCK, &reader(&[]));
        assert_eq!(report.stale.len(), 1, "{:?}", report.stale);
        assert!(
            report.stale[0].contains("names no committed fixture"),
            "{}",
            report.stale[0]
        );
        assert_eq!(
            report.unclassified.len(),
            1,
            "and `ks.other` is itself unaccounted for"
        );
    }

    /// An entry declaring NO shape states no reason at all, and must not pass just
    /// because a golden carrying nothing "equals" an empty declaration.
    #[test]
    fn an_exclusion_with_no_declared_shape_states_no_reason() {
        const NO_SHAPES: &[Excluded] = &[Excluded {
            keyspace: "ks",
            table: "t",
            shapes: &[],
            note: "declares nothing",
        }];
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        ]))
        .expect("classifies");
        let texts = [(format!("{DIR}/nb-1-big-Data.db.jsonl"), LIVE_GOLDEN)];
        let injected: Vec<(&str, &str)> = texts.iter().map(|(p, t)| (p.as_str(), *t)).collect();

        let report = census(&tracked, &NO_CASES(), NO_SHAPES, &reader(&injected));
        assert!(
            report.stale.iter().any(|s| s.contains("states no reason")),
            "{:?}",
            report.stale
        );
    }

    /// A table that is both a case and an exclusion is reported ONCE, naming the
    /// contradiction rather than each of its generations.
    #[test]
    fn a_table_that_is_both_a_case_and_an_exclusion_is_reported_once() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            &format!("{DIR2}/nb-9-big-Data.db"),
        ]))
        .expect("classifies");

        let report = census(&tracked, &CASE(), STATIC_BLOCK, &reader(&[]));
        assert_eq!(report.unclassified.len(), 1, "{:?}", report.unclassified);
        assert!(
            report.unclassified[0].contains("BOTH a comparable case and a declared exclusion"),
            "{}",
            report.unclassified[0]
        );
        assert_eq!(report.compared, 0);
        assert_eq!(report.excluded, 0);
    }

    /// A committed fixture in neither list is refused BY GENERATION, so the message
    /// says which bytes nothing reads.
    #[test]
    fn an_unlisted_fixture_is_refused_by_generation() {
        let tracked =
            tracked_fixtures(&listing(&[&format!("{DIR}/nb-1-big-Data.db")])).expect("classifies");

        let report = census(&tracked, &NO_CASES(), &[], &reader(&[]));
        assert_eq!(report.unclassified.len(), 1);
        assert!(
            report.unclassified[0].contains("ks.t [t-abc/nb-1-big-Data.db]")
                && report.unclassified[0].contains("neither a CASES entry"),
            "{}",
            report.unclassified[0]
        );
    }
}

/// The T2 pairing rule, pinned against a synthetic listing.
///
/// The real repository has the orphan SHAPE (`test_deltas.static_with_rows` tracks
/// two goldens whose SSTable is not committed) but not the near miss it would
/// license — all three of that table's goldens carry the same static block — so the
/// property has to be pinned here or nothing pins it at all. Reported as measured on
/// the committed tree: restricting the verification set to paired goldens changed
/// ZERO exclusion verdicts, which is the answer, not the reason for the change.
#[cfg(test)]
mod pairing {
    use super::*;

    fn listing(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|l| (*l).to_string()).collect()
    }

    const DIR: &str = "test-data/datasets/sstables/ks/t-abc";

    /// Only the golden named `<the tracked SSTable>.jsonl` verifies a claim about
    /// that fixture. The orphan beside it is NAMED, because a table git does track an
    /// SSTable for is exactly where an orphan could have supplied an exclusion's
    /// evidence.
    #[test]
    fn only_the_golden_paired_with_a_tracked_sstable_verifies_anything() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-2-big-Data.db"),
            &format!("{DIR}/nb-2-big-Data.db.jsonl"),
            // The orphan: no `nb-1-big-Data.db` is tracked.
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        ]))
        .expect("classifies");

        assert_eq!(
            tracked.goldens,
            BTreeMap::from([(
                FixtureId::new("ks", "t", "t-abc", "nb-2-big-Data.db"),
                format!("{DIR}/nb-2-big-Data.db.jsonl")
            )]),
            "the paired golden, and only it, may verify a claim about nb-2"
        );
        assert_eq!(
            tracked.orphans_of_tracked_tables,
            vec![format!("ks.t: {DIR}/nb-1-big-Data.db.jsonl")],
            "the orphan beside a tracked table is named"
        );
        assert_eq!(tracked.orphans_of_untracked_tables, 0);
        assert_eq!(
            tracked.fixtures(),
            vec![FixtureId::new("ks", "t", "t-abc", "nb-2-big-Data.db")]
        );
    }

    /// A golden in a DIFFERENT fixture directory of the same table is an orphan too:
    /// the pairing is per directory, exactly as `compare::golden_path` resolves it
    /// (it reads one fixture directory and requires the golden beside the SSTable).
    #[test]
    fn the_pairing_is_per_directory_not_per_table() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            "test-data/datasets/sstables/ks/t-def/nb-1-big-Data.db.jsonl",
        ]))
        .expect("classifies");

        assert!(
            tracked.goldens.is_empty(),
            "a same-named golden in another directory does not describe this SSTable"
        );
        assert_eq!(
            tracked.orphans_of_tracked_tables,
            vec!["ks.t: test-data/datasets/sstables/ks/t-def/nb-1-big-Data.db.jsonl".to_string()],
        );
    }

    /// A golden for a table git tracks no SSTable for is not a near miss — it is
    /// counted, not named, so the census line stays readable. Both counters are
    /// asserted, so neither can absorb the other.
    #[test]
    fn a_golden_for_an_untracked_table_is_counted_and_not_named() {
        let tracked = tracked_fixtures(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
            "test-data/datasets/sstables/system/local-abc/nb-1-big-Data.db.jsonl",
        ]))
        .expect("classifies");

        assert!(tracked.orphans_of_tracked_tables.is_empty());
        assert_eq!(tracked.orphans_of_untracked_tables, 1);
        assert_eq!(
            tracked.fixtures(),
            vec![FixtureId::new("ks", "t", "t-abc", "nb-1-big-Data.db")],
            "a table with only a golden is not a committed fixture"
        );
    }

    /// And a path shape the classifier refuses is an ERROR here, not a skip — the
    /// census cannot report on a listing it only partly understood.
    #[test]
    fn an_unrecognised_fixture_path_is_refused() {
        let why = tracked_fixtures(&listing(&[
            "test-data/datasets/sstables/ks/nb-1-big-Data.db",
        ]))
        .expect_err("an unrecognised path shape must be refused");
        assert!(!why.is_empty(), "the refusal must state a reason");
    }
}
