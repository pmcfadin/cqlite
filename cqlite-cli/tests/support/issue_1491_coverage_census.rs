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

use super::{repo_root, Presence, CASES};
use crate::golden::dump_shapes::{unsupported_shapes, Unsupported};
use crate::golden::{fixture_root, golden_rows};
use std::collections::{BTreeMap, BTreeSet};

/// A committed fixture that CANNOT be compared this way, with the shapes that
/// make it so.
struct Excluded {
    keyspace: &'static str,
    table: &'static str,
    /// The shapes this table's golden carries. VERIFIED against the golden by
    /// [`committed_fixture_coverage_census`] — declaring the wrong set, or a set
    /// that has gone stale, fails.
    shapes: &'static [Unsupported],
    /// Human context for the census line. Never load-bearing: the `shapes` set is
    /// what is checked.
    note: &'static str,
}

/// Committed fixtures that CANNOT be compared this way, and why.
///
/// Each reason is a *read-time reconciliation* property: the physical dump
/// enumerates on-disk cells including shadowed/expired ones, so the CLI's
/// reconciled `SELECT` result set is legitimately a different set of rows.
/// Weakening the value comparison to absorb that would defeat the point of the
/// lane, so those tables are excluded by name instead.
///
/// The `shapes` set is CHECKED, not trusted: the census requires it to equal the
/// set [`unsupported_shapes`] finds in the table's committed golden. Equality, not
/// containment — a golden that grows a shape the entry does not name is a
/// declaration that has stopped describing its subject, which is the same defect
/// as one that names a shape the golden never had (issue #1491 review finding F4).
///
/// And the goldens it is checked against are only those PAIRED with a tracked
/// `*-Data.db`, by the same `<gen>-Data.db` + `.jsonl` rule `compare::golden_path`
/// resolves a case's oracle with. The repository tracks ORPHAN goldens describing
/// generations whose SSTable is not committed (`test_deltas.static_with_rows` has
/// two), so unioning every tracked JSONL let an orphan supply the declared
/// unsupported shape while the actual committed SSTable's own golden was
/// comparable — an exclusion resting on evidence that does not describe its
/// subject (issue #1491 review finding T2).
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

/// The git-tracked fixture set, split into what may and may not verify a claim.
#[derive(Debug)]
struct Tracked {
    /// Every `(keyspace, table)` git tracks a `*-Data.db` for, sorted and deduped.
    committed: Vec<(String, String)>,
    /// Per table, the tracked goldens PAIRED with a tracked `*-Data.db`.
    goldens: BTreeMap<(String, String), Vec<String>>,
    /// Orphan goldens beside a table git DOES track an SSTable for — the near
    /// misses, named in the census.
    orphans_of_tracked_tables: Vec<String>,
    /// Orphan goldens for a table with no tracked SSTable at all: counted, not
    /// named.
    orphans_of_untracked_tables: usize,
}

/// Split a `git ls-files` listing into the tracked SSTable tables and the goldens
/// that may verify a claim about them.
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
    let mut committed: Vec<(String, String)> = Vec::new();
    let mut sstables: BTreeSet<(String, String, String, String)> = BTreeSet::new();
    let mut all_goldens: Vec<(String, String, String, String, String)> = Vec::new();
    for line in listing {
        // The same path parser the fixture-root selection uses, so "committed" means
        // one thing in this lane: an unrecognised shape is refused, not guessed at.
        let Some(path) = fixture_root::classify(line)? else {
            continue;
        };
        if path.is_golden {
            all_goldens.push((
                path.keyspace,
                path.table,
                path.dir,
                path.file,
                line.to_string(),
            ));
        } else {
            sstables.insert((
                path.keyspace.clone(),
                path.table.clone(),
                path.dir,
                path.file,
            ));
            committed.push((path.keyspace, path.table));
        }
    }
    committed.sort();
    committed.dedup();

    let mut goldens: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let mut orphans_of_tracked_tables: Vec<String> = Vec::new();
    let mut orphans_of_untracked_tables = 0usize;
    for (keyspace, table, dir, file, line) in all_goldens {
        let Some(sstable) = file.strip_suffix(".jsonl") else {
            // `classify` only reports `is_golden` for a `-Data.db.jsonl` path, so
            // this cannot happen; reported rather than skipped so a change to that
            // rule cannot silently drop a golden from the verification set.
            return Err(format!("a golden path without a `.jsonl` suffix: {line}"));
        };
        let key = (keyspace, table);
        if sstables.contains(&(key.0.clone(), key.1.clone(), dir, sstable.to_string())) {
            goldens.entry(key).or_default().push(line);
        } else if committed.binary_search(&key).is_ok() {
            // A golden BESIDE a tracked table but describing another generation —
            // the only orphan that could ever have justified an exclusion, so it is
            // named rather than counted.
            orphans_of_tracked_tables.push(format!("{}.{}: {line}", key.0, key.1));
        } else {
            orphans_of_untracked_tables += 1;
        }
    }
    Ok(Tracked {
        committed,
        goldens,
        orphans_of_tracked_tables,
        orphans_of_untracked_tables,
    })
}

/// Every git-committed `*-Data.db` fixture is either a comparable case or a NAMED,
/// reasoned exclusion. Derived from committed source at run time, so a newly
/// committed fixture must be classified instead of being silently uncovered.
#[test]
fn committed_fixture_coverage_census() {
    let root = repo_root();
    let listing = fixture_root::committed_listing()
        .unwrap_or_else(|why| panic!("cannot read the committed fixture set: {why}"));

    let Tracked {
        committed,
        goldens,
        orphans_of_tracked_tables,
        orphans_of_untracked_tables,
    } = tracked_fixtures(&listing).unwrap_or_else(|why| panic!("{why}"));

    eprintln!(
        "AD2 census: {} tracked golden(s) PAIRED with a tracked *-Data.db; {} orphan(s) \
         beside a tracked table and {orphans_of_untracked_tables} for tables with no \
         tracked SSTable at all — an orphan describes a generation this checkout does \
         not carry, so it verifies nothing",
        goldens.values().map(Vec::len).sum::<usize>(),
        orphans_of_tracked_tables.len()
    );
    for orphan in &orphans_of_tracked_tables {
        eprintln!("AD2 census:   ORPHAN golden (not the paired oracle) {orphan}");
    }
    assert!(
        !committed.is_empty(),
        "no committed *-Data.db fixtures found under {} — the census has no subject",
        root.display()
    );

    let mut unclassified: Vec<String> = Vec::new();
    for (keyspace, table) in &committed {
        let is_case = CASES
            .iter()
            .any(|c| c.keyspace == keyspace && c.table == table);
        let is_excluded = NOT_COMPARABLE
            .iter()
            .any(|e| e.keyspace == keyspace && e.table == table);
        if is_case && is_excluded {
            unclassified.push(format!(
                "{keyspace}.{table} is BOTH a comparable case and a declared exclusion"
            ));
        } else if !is_case && !is_excluded {
            unclassified.push(format!(
                "{keyspace}.{table} is neither a CASES entry nor a NOT_COMPARABLE entry"
            ));
        }
    }
    let committed_cases = CASES
        .iter()
        .filter(|c| c.presence == Presence::Committed)
        .count();
    eprintln!(
        "AD2 census: {} committed fixture tables — {committed_cases} compared, {} declared \
         not-comparable; plus {} fetched-corpus case(s)",
        committed.len(),
        NOT_COMPARABLE.len(),
        CASES.len() - committed_cases
    );
    assert!(
        unclassified.is_empty(),
        "every committed fixture must be classified (compared, or excluded with a \
         reason) — issue #1491:\n  {}",
        unclassified.join("\n  ")
    );

    // A declared exclusion must name a fixture that exists AND its declared shapes
    // must be the ones that fixture's golden actually carries. Naming an existing
    // fixture was all this used to check, so a stale or wrong reason could hide a
    // table that is in fact comparable (issue #1491 review finding F4).
    let mut stale: Vec<String> = Vec::new();
    for entry in NOT_COMPARABLE {
        let qualified = format!("{}.{}", entry.keyspace, entry.table);
        if !committed
            .iter()
            .any(|(ks, tbl)| *ks == entry.keyspace && *tbl == entry.table)
        {
            stale.push(format!(
                "{qualified} ({}) names no committed fixture",
                entry.note
            ));
            continue;
        }
        let declared: BTreeSet<Unsupported> = entry.shapes.iter().copied().collect();
        assert!(
            !declared.is_empty(),
            "{qualified}: an exclusion with no declared shape states no reason at all"
        );
        let files = goldens
            .get(&(entry.keyspace.to_string(), entry.table.to_string()))
            .map(Vec::as_slice)
            .unwrap_or_default();
        // No PAIRED golden at all is a FAILURE, not a pass: an exclusion no golden
        // can corroborate is exactly the unverifiable claim this check exists for —
        // and an orphan golden beside the fixture does not make it verifiable
        // (finding T2), so the message says which set was empty.
        if files.is_empty() {
            stale.push(format!(
                "{qualified}: no committed *-Data.db.jsonl golden PAIRED with a tracked \
                 *-Data.db, so the declared shapes {declared:?} cannot be verified"
            ));
            continue;
        }
        let mut present: BTreeSet<Unsupported> = BTreeSet::new();
        for file in files {
            let text = match std::fs::read_to_string(root.join(file)) {
                Ok(text) => text,
                Err(e) => {
                    stale.push(format!("{qualified}: cannot read {file}: {e}"));
                    continue;
                }
            };
            match unsupported_shapes(&text) {
                Ok(shapes) => present.extend(shapes),
                Err(why) => stale.push(format!("{qualified}: {file}: {why}")),
            }
        }
        if present != declared {
            let names = |set: &BTreeSet<Unsupported>| {
                set.iter().map(|s| s.label()).collect::<Vec<_>>().join(", ")
            };
            stale.push(format!(
                "{qualified}: declares [{}] but its committed golden carries [{}] — the \
                 exclusion no longer describes the fixture",
                names(&declared),
                names(&present)
            ));
            continue;
        }
        eprintln!(
            "AD2 census: {qualified} EXCLUDED, verified in {} PAIRED golden file(s): {} ({})",
            files.len(),
            declared
                .iter()
                .map(|s| s.label())
                .collect::<Vec<_>>()
                .join(", "),
            entry.note
        );
    }
    assert!(
        stale.is_empty(),
        "every NOT_COMPARABLE entry must name shapes its committed golden really \
         carries — issue #1491:\n  {}",
        stale.join("\n  ")
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
    let live = r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z"},"cells":[{"name":"v","value":"x"}]}]}"#;
    let rows = golden_rows(live, &["id"], &[], &[]).expect("the baseline golden is comparable");
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

        let key = ("ks".to_string(), "t".to_string());
        assert_eq!(
            tracked.goldens.get(&key).map(Vec::as_slice),
            Some([format!("{DIR}/nb-2-big-Data.db.jsonl")].as_slice()),
            "the paired golden, and only it, may verify a claim about nb-2"
        );
        assert_eq!(
            tracked.orphans_of_tracked_tables,
            vec![format!("ks.t: {DIR}/nb-1-big-Data.db.jsonl")],
            "the orphan beside a tracked table is named"
        );
        assert_eq!(tracked.orphans_of_untracked_tables, 0);
        assert_eq!(tracked.committed, vec![key]);
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

        let key = ("ks".to_string(), "t".to_string());
        assert!(
            !tracked.goldens.contains_key(&key),
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
            tracked.committed,
            vec![("ks".to_string(), "t".to_string())],
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
