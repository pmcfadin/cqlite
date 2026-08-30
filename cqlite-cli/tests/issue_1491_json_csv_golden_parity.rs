//! AD2 — JSON/CSV egress **value** parity against the `sstabledump` goldens
//! (issue #1491, epic #1469 finding AD2).
//!
//! # What this lane asserts, and what it replaces
//!
//! For every committed fixture table in [`CASES`] it runs the real CLI —
//! `cqlite --schema <cql> --data-dir <staged> export <out> --format json|csv
//! --table <ks.tbl>` — reads the output back, pairs rows with the committed
//! `*-Data.db.jsonl` golden by primary key, and compares EVERY cell value.
//!
//! Until #1491 nothing did that. `one_shot_e2e_tests.rs::validate_json_structure`
//! checked "non-empty array of objects with `len <= reference.len()`";
//! `export_integration_tests.rs`'s determinism tests checked shape and row counts.
//! A `ValueFormatter` / `value_to_json` regression — blob hex casing, decimal
//! digits, timestamp spelling, an absent cell rendered as something other than
//! `null` — was invisible to all of them.
//!
//! # Fail-closed, per case (#3220)
//!
//! Most entries in [`CASES`] are **git-committed** fixtures, so they are present in
//! any checkout and there is deliberately NO skip path for them: an unresolvable
//! fixture, an empty golden, an empty egress, or a zero-cell comparison each fail
//! that case. A small [`Presence::Corpus`] tier covers null/empty/absent-cell
//! properties no committed fixture has; those report `NOT PRESENT` in the census
//! when the fetched corpus is absent, and are compared with identical strictness
//! when it is there. The datasets root is resolved per TABLE by evidence (does this
//! table's `*-Data.db` exist under that root), never by an env-first/
//! checkout-first preference. There is no suite-wide `assert!(ran > 0)`, which
//! cannot see one case skipping behind its siblings.
//!
//! # Coverage census
//!
//! [`committed_fixture_coverage_census`] enumerates the git-committed
//! `*-Data.db` fixtures from `git ls-files` and requires each to be either a
//! compared case or a NAMED entry in [`NOT_COMPARABLE`] with a reason. A new committed fixture
//! therefore has to be classified rather than silently uncovered — derived at run
//! time from committed source, not from a hand-kept count.
//!
//! Every declared reason in [`NOT_COMPARABLE`] is also ENFORCED rather than
//! trusted: the golden reader refuses a golden carrying the shape the reason
//! names, so a mis-stated exclusion cannot quietly hide a comparable table.

#![cfg(feature = "state_machine")]

#[path = "support/golden_value_parity.rs"]
mod golden;

use golden::compare::{
    cli_csv_rows, cli_json_rows, compare_rows, fixture_dir, golden_path, stage_single_table,
};
use golden::{golden_rows, Egress, Multicell};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Whether a case's fixture is guaranteed present.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Presence {
    /// Git-committed under `test-data/datasets/sstables/`: present in EVERY
    /// checkout, so the case is `must_run` and fails closed unconditionally.
    Committed,
    /// Present only in the FETCHED corpus (`fetch-datasets.sh`). Reported as
    /// `NOT PRESENT` when the corpus is absent, and compared with the same
    /// strictness when it is. These carry properties no committed fixture has —
    /// an absent regular cell, a scalar cell tombstone, empty text vs empty blob
    /// vs null — which is the whole reason the tier exists.
    Corpus,
}

/// One comparable table.
struct Case {
    presence: Presence,
    keyspace: &'static str,
    table: &'static str,
    /// The committed CQL schema under `test-data/schemas/` (without `.cql`).
    schema: &'static str,
    /// Partition-key columns in key order, from that `CREATE TABLE`.
    pk: &'static [&'static str],
    /// Clustering columns in key order, from that `CREATE TABLE`.
    ck: &'static [&'static str],
    /// NON-frozen collection columns and their storage shape, from the DDL. A
    /// multi-cell column the golden carries and this list omits is a hard error —
    /// the kind is never inferred from the bytes (#28).
    multicell: &'static [(&'static str, Multicell)],
    /// Columns excluded from the value comparison, each with the defect it is
    /// waiting on. Reported in the run census so an exclusion is never silent.
    skip_columns: &'static [(&'static str, &'static str)],
}

/// Committed fixture tables whose golden is a pure set of live rows, so the
/// physical dump and the CLI's reconciled result set are the same rows.
///
/// Key columns are transcribed from the committed `CREATE TABLE` named by
/// `schema`. A wrong transcription cannot pass: the column names become row keys
/// compared against the CLI's own, and the golden's key arity is asserted against
/// the declared arity per row.
const CASES: &[Case] = &[
    // test-data/schemas/compression-parity.cql — (pk int, ck int, body text).
    // Seven codec variants: the same logical rows through LZ4 / Snappy / Deflate /
    // Zstd / uncompressed / a short final chunk, plus a BLOB payload table.
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "lz4_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "snappy_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "deflate_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "zstd_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "uncompressed_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "short_final_chunk",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // `payload BLOB` — the blob `0x…` hex rendering, compared byte-exactly.
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "incompressible_uncompressed_chunk",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // test-data/schemas/compaction-parity.cql
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparity",
        table: "live_no_clustering",
        schema: "compaction-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparity",
        table: "live_clustering",
        schema: "compaction-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // test-data/schemas/compaction-parity-udt.cql — frozen UDTs and frozen
    // collections OF UDTs, i.e. the `_type`-discriminator and map-spelling rules.
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_frozen_person",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_collections",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_null_inner",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_nested",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        // MEASURED DIVERGENCE, not a normalization: `employee.home` is a
        // `frozen<address>` nested inside a `frozen<employee>`. The golden decodes
        // it (`{"street": "1 Navy Way", …}`); both CLI egress formats emit the
        // inner UDT's RAW BYTES as blob hex
        // (`0x0000000a31204e617679205761790000000941726c696e67746f6e…`). The other
        // two fields of the same UDT ARE compared, so the case is not a no-op.
        skip_columns: &[(
            "e",
            "nested frozen UDT renders as blob hex, not a decoded object",
        )],
    },
    // test-data/schemas/signed-collection-parity.cql — NON-frozen and frozen
    // collections of signed numerics: the "path is a JSON string, CLI element is a
    // JSON number" rule, and exact 30-digit decimal text.
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_int_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[("s", Multicell::Set), ("m", Multicell::Map)],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "frozen_int_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_width_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("sb", Multicell::Set),
            ("ss", Multicell::Set),
            ("st", Multicell::Set),
        ],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_special_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[("sd", Multicell::Set), ("sf", Multicell::Set)],
        // MEASURED DIVERGENCE: `sf` is a `set<double>` containing `Infinity`,
        // `-Infinity` and `NaN`. The golden carries them by name; JSON has no
        // literal for them and the CLI emits `null`, losing the value. `sd`
        // (`set<decimal>`, exact 30-digit text) IS compared in this same case.
        skip_columns: &[("sf", "float Infinity/-Infinity/NaN render as JSON null")],
    },
    // test-data/schemas/da-test.cql — BTI (`da`) format, timestamp/uuid/boolean
    // scalars plus non-frozen set/list/map.
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "collection_table",
        schema: "da-test",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("tags", Multicell::Set),
            ("scores", Multicell::List),
            ("properties", Multicell::Map),
        ],
        skip_columns: &[],
    },
    // BTI wide/multi-clustering shapes: many rows per partition, so row pairing
    // and clustering-column rendering are exercised at scale.
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "wide_table",
        schema: "wide-table-bti",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "multiclustering_table",
        schema: "multiclustering-table-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "wide_multiclustering_small",
        schema: "wide-multiclustering-small-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skip_columns: &[],
    },
    // test-data/schemas/write-load-parity.cql
    Case {
        presence: Presence::Committed,
        keyspace: "test_writeparity",
        table: "finished_data",
        schema: "write-load-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skip_columns: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_writeparity",
        table: "partition_boundary",
        schema: "write-load-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // ---------------------------------------------------------------------
    // FETCHED-corpus tier (test-data/schemas/cql-type-parity.cql). These four
    // tables carry the null/empty/absent properties NO committed fixture has —
    // verified by scanning every committed golden: none of them has a row that
    // omits a regular cell, so without this tier "an absent cell renders as
    // null" and "a cell tombstone renders as null" would be unasserted.
    // ---------------------------------------------------------------------

    // Row 1 omits `reg` (never written), row 2 carries a CELL TOMBSTONE for it,
    // row 3 writes it as the empty string: absent vs deleted vs empty, the three
    // spellings a formatter can confuse.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_absent_vs_null_regular",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // `target_text`/`target_blob` cycle through absent / NULL / '' / 0x with live
    // neighbours either side, so a shifted or swallowed value is visible.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_null_empty_text_blob",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // text/blob at length 0, 1, 127, 128, 255, 256, 16383, 16384 — the
    // length-prefix edges, where a truncating formatter shows up.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_length_prefix_edges",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skip_columns: &[],
    },
    // An EMPTY multicell collection is stored ABSENT by Cassandra (the dump holds
    // only a complex deletion) while an empty FROZEN one persists as a present
    // empty value. `fl`/`fs`/`fm` therefore pin `[]` and `{}` as PRESENT empty
    // containers, which is the half of the property CQLite gets right.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_empty_collections",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[
            ("ml", Multicell::List),
            ("ms", Multicell::Set),
            ("mm", Multicell::Map),
        ],
        // MEASURED DIVERGENCE: for the row whose multicell collections were
        // written EMPTY, the golden carries a complex deletion and no cells — i.e.
        // the column is absent, and Cassandra's `SELECT` returns `null` (the DDL
        // comment in cql-type-parity.cql states this, and the same on-disk shape is
        // what `DELETE ml FROM …` writes). Both CQLite egress formats instead
        // render a PRESENT empty container (`[]`, `{}`), which is a different
        // value. Non-empty multicell rendering stays covered by four other cases
        // (test_da.collection_table and the three test_signed_coll tables).
        skip_columns: &[
            (
                "ml",
                "empty multicell list renders as [] where Cassandra reads null",
            ),
            (
                "ms",
                "empty multicell set renders as {} where Cassandra reads null",
            ),
            (
                "mm",
                "empty multicell map renders as {} where Cassandra reads null",
            ),
        ],
    },
];

/// Committed fixtures that CANNOT be compared this way, and why.
///
/// Each reason is a *read-time reconciliation* property: the physical dump
/// enumerates on-disk cells including shadowed/expired ones, so the CLI's
/// reconciled `SELECT` result set is legitimately a different set of rows.
/// Weakening the value comparison to absorb that would defeat the point of the
/// lane, so those tables are excluded by name instead — and the golden reader
/// independently REFUSES each of these shapes, so a wrong reason here surfaces as
/// a failure rather than as silent coverage loss.
const NOT_COMPARABLE: &[(&str, &str, &str)] = &[
    (
        "test_big",
        "wide_partition",
        "range tombstone bounds in the dump",
    ),
    (
        "test_compaction_tombstone_ttl",
        "rt_cross_gen",
        "range tombstone bounds/boundaries",
    ),
    (
        "test_compaction_tombstone_ttl",
        "shadow_row_delete",
        "row deletion marker",
    ),
    (
        "test_compaction_tombstone_ttl",
        "ttl_expired_live",
        "TTL expiry + cell deletion",
    ),
    ("test_da", "ttl_table", "row TTL"),
    (
        "test_deltas",
        "static_with_rows",
        "static block: static-column projection is reconciliation",
    ),
    (
        "test_tomb",
        "static_with_tombstones",
        "static block + row/cell deletions",
    ),
    (
        "test_writeparity",
        "static_clustering_shape",
        "static block",
    ),
];

fn repo_root() -> PathBuf {
    golden::datasets_root::repo_root()
}

fn schema_file(schema: &str) -> PathBuf {
    repo_root()
        .join("test-data/schemas")
        .join(format!("{schema}.cql"))
}

/// Run `export` for one table into `out`, returning its contents.
fn export(case: &Case, data_dir: &Path, out: &Path, format: &str) -> String {
    let schema = schema_file(case.schema);
    assert!(
        schema.is_file(),
        "committed schema {} is unreadable (see #3148)",
        schema.display()
    );
    let qualified = format!("{}.{}", case.keyspace, case.table);
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            &schema.to_string_lossy(),
            "--data-dir",
            &data_dir.to_string_lossy(),
            "export",
            &out.to_string_lossy(),
            "--format",
            format,
            "--table",
            &qualified,
        ])
        .output()
        .unwrap_or_else(|e| panic!("{qualified}: cannot run the CLI: {e}"));
    assert!(
        output.status.success(),
        "{qualified}: export --format {format} failed ({:?})\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::read_to_string(out)
        .unwrap_or_else(|e| panic!("{qualified}: cannot read {}: {e}", out.display()))
}

/// JSON egress: every cell value deep-compared against the golden.
#[test]
fn json_egress_matches_sstabledump_goldens() {
    run_lane(Egress::Json);
}

/// CSV egress: every SCALAR cell compared against the golden (container syntax is
/// a CQLite-only text form with no external authority — see the support module).
#[test]
fn csv_egress_matches_sstabledump_goldens() {
    run_lane(Egress::Csv);
}

fn run_lane(egress: Egress) {
    let format = match egress {
        Egress::Json => "json",
        Egress::Csv => "csv",
    };
    let mut failures: Vec<String> = Vec::new();
    let mut census: Vec<String> = Vec::new();
    let mut containers_compared = 0usize;
    let mut containers_refused = 0usize;

    for case in CASES {
        let qualified = format!("{}.{}", case.keyspace, case.table);
        // must_run: a committed fixture is never allowed to skip.
        let fixture = match fixture_dir(case.keyspace, case.table) {
            Ok(dir) => dir,
            Err(why) => {
                match case.presence {
                    // A committed fixture is present in every checkout, so an
                    // unresolvable one is a real failure, never a skip.
                    Presence::Committed => {
                        failures.push(format!("{qualified}: fixture unresolvable: {why}"))
                    }
                    // A fetched-corpus fixture may legitimately be absent; the
                    // absence is DECLARED in the census rather than swallowed.
                    Presence::Corpus => census.push(format!(
                        "  {qualified}: NOT PRESENT (fetched corpus) — {why}"
                    )),
                }
                continue;
            }
        };
        let golden_file = match golden_path(&fixture) {
            Ok(path) => path,
            Err(why) => {
                failures.push(format!("{qualified}: {why}"));
                continue;
            }
        };
        let jsonl = match std::fs::read_to_string(&golden_file) {
            Ok(text) => text,
            Err(e) => {
                failures.push(format!(
                    "{qualified}: cannot read {}: {e}",
                    golden_file.display()
                ));
                continue;
            }
        };
        let expected = match golden_rows(&jsonl, case.pk, case.ck, case.multicell) {
            Ok(rows) => rows,
            Err(why) => {
                failures.push(format!(
                    "{qualified}: golden is not comparable ({why}) — either the case \
                     declaration is wrong or the table belongs in NOT_COMPARABLE"
                ));
                continue;
            }
        };
        if expected.is_empty() {
            failures.push(format!(
                "{qualified}: golden {} yielded 0 rows — a committed fixture must never \
                 compare empty",
                golden_file.display()
            ));
            continue;
        }

        let staging = match tempfile::TempDir::new() {
            Ok(dir) => dir,
            Err(e) => {
                failures.push(format!("{qualified}: cannot create a temp dir: {e}"));
                continue;
            }
        };
        if let Err(why) = stage_single_table(staging.path(), case.keyspace, &fixture) {
            failures.push(format!("{qualified}: staging failed: {why}"));
            continue;
        }
        let out = staging.path().join(format!("egress.{format}"));
        let text = export(case, staging.path(), &out, format);
        let actual = match egress {
            Egress::Json => cli_json_rows(&text),
            Egress::Csv => cli_csv_rows(&text),
        };
        let actual = match actual {
            Ok(rows) => rows,
            Err(why) => {
                failures.push(format!("{qualified}: unreadable {format} egress: {why}"));
                continue;
            }
        };
        if actual.is_empty() {
            failures.push(format!(
                "{qualified}: {format} egress produced 0 rows while the golden has {}",
                expected.len()
            ));
            continue;
        }

        let skip: Vec<&str> = case.skip_columns.iter().map(|(c, _)| *c).collect();
        let report = compare_rows(&expected, &actual, case.pk, case.ck, &skip, egress);
        if report.diffs.is_empty() && report.compared_cells == 0 {
            failures.push(format!(
                "{qualified}: {format} comparison examined 0 cells — a vacuous pass"
            ));
            continue;
        }
        containers_compared += report.container_cells;
        containers_refused += report.ambiguous_container_cells;
        if report.diffs.is_empty() {
            census.push(format!(
                "  {qualified}: {} rows, {} cells compared ({} of them containers){}{}",
                expected.len(),
                report.compared_cells,
                report.container_cells,
                // A refusal is a DECLARED GAP in the same style as `skip_columns`:
                // named at run time, never left as a bare counter.
                if report.ambiguous_container_cells > 0 {
                    format!(
                        ", DECLARED GAP: {} container cell(s) REFUSED as \
                         CSV-unrepresentable: {}",
                        report.ambiguous_container_cells,
                        report.ambiguity_reasons.join("; ")
                    )
                } else {
                    String::new()
                },
                if case.skip_columns.is_empty() {
                    String::new()
                } else {
                    format!(
                        ", DECLARED GAP: {}",
                        case.skip_columns
                            .iter()
                            .map(|(c, why)| format!("{c} ({why})"))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )
                }
            ));
        } else {
            let shown: Vec<String> = report.diffs.iter().take(8).cloned().collect();
            failures.push(format!(
                "{qualified}: {} of {} compared {format} cells diverge from {}:\n    {}",
                report.diffs.len(),
                report.compared_cells,
                golden_file.display(),
                shown.join("\n    ")
            ));
        }
    }

    eprintln!("AD2 {format} egress parity census ({} cases):", CASES.len());
    for line in &census {
        eprintln!("{line}");
    }
    // A narrowed lane DECLARES its narrowing at run time (CLAUDE.md), and states
    // it affirmatively: `0 REFUSED` is a measurement that the ambiguity scan ran
    // and found nothing, which a bare absent line could never convey. Only the
    // CSV lane has an ambiguity scan to report — JSON carries its own types, so
    // there is nothing there to refuse and claiming `0 REFUSED` would advertise a
    // check that does not exist.
    let refusals = match egress {
        Egress::Csv => format!(", {containers_refused} REFUSED as CSV-unrepresentable"),
        Egress::Json => String::new(),
    };
    eprintln!(
        "AD2 {format} container coverage: {containers_compared} collection/UDT cell(s) \
         value-compared{refusals}"
    );
    assert!(
        failures.is_empty(),
        "AD2 {format} egress value parity failed for {} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Every git-committed `*-Data.db` fixture is either a comparable case or a NAMED,
/// reasoned exclusion. Derived from committed source at run time, so a newly
/// committed fixture must be classified instead of being silently uncovered.
#[test]
fn committed_fixture_coverage_census() {
    let root = repo_root();
    let output = Command::new("git")
        .args(["ls-files", "test-data/datasets/sstables"])
        .current_dir(&root)
        .output()
        .unwrap_or_else(|e| panic!("cannot run `git ls-files` in {}: {e}", root.display()));
    assert!(
        output.status.success(),
        "`git ls-files` failed in {}: {}",
        root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let listing = String::from_utf8_lossy(&output.stdout);

    let mut committed: Vec<(String, String)> = Vec::new();
    for line in listing.lines() {
        if !line.ends_with("-Data.db") {
            continue;
        }
        let parts: Vec<&str> = line.split('/').collect();
        // test-data/datasets/sstables/<keyspace>/<table>-<uuid>/<gen>-Data.db
        let (Some(keyspace), Some(dir)) = (parts.get(3), parts.get(4)) else {
            panic!("unexpected committed fixture path shape: {line}");
        };
        let table = dir
            .rsplit_once('-')
            .map(|(table, _uuid)| table.to_string())
            .unwrap_or_else(|| panic!("fixture dir has no -<uuid> suffix: {dir}"));
        committed.push(((*keyspace).to_string(), table));
    }
    committed.sort();
    committed.dedup();
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
            .any(|(ks, tbl, _)| ks == keyspace && tbl == table);
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

    // A declared exclusion must name a fixture that exists, or the reason is stale.
    for (keyspace, table, reason) in NOT_COMPARABLE {
        assert!(
            committed
                .iter()
                .any(|(ks, tbl)| ks == keyspace && tbl == table),
            "NOT_COMPARABLE names {keyspace}.{table} ({reason}) but no such committed \
             fixture exists"
        );
    }
}
