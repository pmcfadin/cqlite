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
use crate::golden::committed_set::{committed_set, paired_golden_rel, CommittedSet, FixtureId};
use crate::golden::dump_shapes::{unsupported_shapes, Unsupported};
use crate::golden::{fixture_root, golden_rows};
use std::collections::BTreeSet;

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
    /// Compared committed fixtures whose oracle IS the git-tracked golden paired
    /// with them. Counted affirmatively rather than derived by subtraction, so the
    /// census line states a measurement that was taken and not one inferred from the
    /// absence of the bad case.
    compared_with_tracked_oracle: usize,
    /// Compared committed fixtures whose ORACLE is not committed: the lane reads a
    /// golden git does not track, so a committed case would be certified by a
    /// fetched-corpus copy, a stray local file or a previous run's leftover (issue
    /// #1491 review finding BB1). Kept apart from [`Census::unclassified`] because
    /// the fixture IS classified — it is the oracle that is not accountable.
    untracked_oracle: Vec<String>,
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
/// Pure: the listing arrives already split by [`committed_set`], the compared set
/// and the exclusions are parameters, and golden bytes come from `read_golden`. So
/// the real test can run it over the checkout while [`per_fixture`] runs it over
/// synthetic listings the checkout does not contain.
///
/// `cases` is the `(keyspace, table)` of every entry in [`CASES`] — including the
/// `Presence::Corpus` tier, deliberately: whether a case's declared tier matches what
/// git tracks is the parity lane's own `Presence` cross-check, and duplicating it here
/// would report one defect as two.
fn census(
    tracked: &CommittedSet,
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
                    // The fixture's trackedness is established by the listing this
                    // loop walks; the ORACLE's is a second question, and nothing
                    // asked it (finding BB1). `compare::golden_path` resolves the
                    // golden from the FILESYSTEM, which cannot tell a committed
                    // golden from an untracked file of the same name — so a
                    // committed case could be certified by bytes no checkout
                    // carries. Asked here of the same listing, for the generation
                    // the lane really compares.
                    if tracked.goldens.contains_key(&id) {
                        out.compared_with_tracked_oracle += 1;
                    } else {
                        out.untracked_oracle.push(format!(
                            "{}: the lane compares this generation, and {} — the \
                             golden that would describe it — is NOT git-tracked, so \
                             its oracle is not committed",
                            id.describe(),
                            paired_golden_rel(&id)
                        ));
                    }
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
    tracked: &CommittedSet,
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
    let tracked = committed_set(&listing).unwrap_or_else(|why| panic!("{why}"));

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
    // AFFIRMATIVE, and at 0 too (CLAUDE.md: a positive verdict requires a
    // measurement): how many of the compared generations are certified by the golden
    // git tracks for them. A census that only reported the bad case would read the
    // same whether the check ran or not.
    eprintln!(
        "AD2 census: {} of {} compared generation(s) are certified by the git-tracked \
         golden PAIRED with them; {} by an untracked file",
        report.compared_with_tracked_oracle,
        report.compared,
        report.untracked_oracle.len()
    );
    assert!(
        report.untracked_oracle.is_empty(),
        "a committed case's ORACLE is committed too: the golden paired with the \
         compared *-Data.db must be git-tracked, resolved from the same listing and \
         with no environment override — issue #1491 finding BB1:\n  {}",
        report.untracked_oracle.join("\n  ")
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
        let tracked = committed_set(&listing(&[
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
            committed_set(&listing(&[&format!("{DIR}/nb-1-big-Data.db")])).expect("classifies");

        let report = census(&tracked, &NO_CASES(), &[], &reader(&[]));
        assert_eq!(report.unclassified.len(), 1);
        assert!(
            report.unclassified[0].contains("ks.t [t-abc/nb-1-big-Data.db]")
                && report.unclassified[0].contains("neither a CASES entry"),
            "{}",
            report.unclassified[0]
        );
    }

    /// BB1: a compared generation whose paired golden git does NOT track is refused,
    /// and the refusal names the golden that would have to be committed.
    ///
    /// The fixture is classified — it is the COMPARED one — so this cannot show up as
    /// `unclassified`; what is missing is the accountability of the ORACLE, which is
    /// why it has its own verdict. Measured on this checkout when the check landed:
    /// all 32 committed `*-Data.db` pair with a git-tracked golden, so the shape is
    /// LATENT and can only be pinned over an injected listing.
    #[test]
    fn a_compared_generation_whose_paired_golden_is_untracked_is_refused() {
        let tracked =
            committed_set(&listing(&[&format!("{DIR}/nb-1-big-Data.db")])).expect("classifies");

        let report = census(&tracked, &CASE(), &[], &reader(&[]));
        assert_eq!(
            report.compared, 1,
            "the generation is still the compared one"
        );
        assert_eq!(
            report.compared_with_tracked_oracle, 0,
            "and nothing about it is certified by a committed golden"
        );
        assert!(
            report.unclassified.is_empty(),
            "the fixture IS classified; it is the oracle that is not: {:?}",
            report.unclassified
        );
        assert_eq!(
            report.untracked_oracle.len(),
            1,
            "{:?}",
            report.untracked_oracle
        );
        assert!(
            report.untracked_oracle[0].contains("ks.t [t-abc/nb-1-big-Data.db]")
                && report.untracked_oracle[0].contains(&format!("{DIR}/nb-1-big-Data.db.jsonl"))
                && report.untracked_oracle[0].contains("NOT git-tracked"),
            "{}",
            report.untracked_oracle[0]
        );
    }

    /// And with that golden committed, the same listing is fully certified — so the
    /// refusal above is attributable to trackedness and not to the scaffolding.
    #[test]
    fn a_compared_generation_paired_with_a_tracked_golden_is_certified() {
        let tracked = committed_set(&listing(&[
            &format!("{DIR}/nb-1-big-Data.db"),
            &format!("{DIR}/nb-1-big-Data.db.jsonl"),
        ]))
        .expect("classifies");

        let report = census(&tracked, &CASE(), &[], &reader(&[]));
        assert_eq!(report.compared, 1);
        assert_eq!(report.compared_with_tracked_oracle, 1);
        assert!(
            report.untracked_oracle.is_empty(),
            "{:?}",
            report.untracked_oracle
        );
    }
}
