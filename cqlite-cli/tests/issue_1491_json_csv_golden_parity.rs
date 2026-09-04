//! AD2 — JSON/CSV egress **value** parity against the `sstabledump` goldens
//! (issue #1491, epic #1469 finding AD2).
//!
//! # What this lane asserts, and what it replaces
//!
//! For every table in [`CASES`] it runs the real CLI —
//! `cqlite --schema <cql> --data-dir <staged> export <out> --format json|csv
//! --table <ks.tbl>` — reads the output back, pairs rows with that table's
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
//! when it is there. Which tier a case is in is CHECKED against `git ls-files`, not
//! trusted, and that listing also decides where its golden comes from: a committed
//! case is compared against the CHECKOUT copy and only that copy, so an external
//! `CQLITE_DATASETS_ROOT` corpus carrying its own copy of the same table cannot
//! stand in for the committed values (finding J1); a fetched-corpus case is resolved
//! per TABLE by evidence (does this table's `*-Data.db` exist under that root),
//! never by an env-first/checkout-first preference. The census names the root that
//! supplied each golden. There is no suite-wide `assert!(ran > 0)`, which cannot see
//! one case skipping behind its siblings.
//!
//! # Declared gaps, and their SCOPE
//!
//! A gap in the value comparison is a [`Skip`]: a path, the egress FORMAT(S) it
//! applies to, and the measured divergence. The format scope is load-bearing —
//! `Infinity`/`NaN` are lost by JSON's value vocabulary and carried verbatim by
//! CSV, so a gap declared for both formats drops a column from the format that
//! renders it correctly. Every applicable gap is named, WITH its scope, in the run
//! census, and one that matched nothing in a lane's walk fails that lane.
//!
//! The CSV lane additionally REFUSES a position whose golden content cannot
//! survive the unquoted rendering (see `golden::csv_container`). A refusal is
//! decided at the granularity the comparison walks — per member, per depth — so an
//! indistinguishable nested member suppresses only itself while its siblings, its
//! container's member count and every enclosing frame stay compared (review
//! finding P2). There is no coarser tier: an UNBALANCED bracket breaks the bracket
//! depth a split relies on and can therefore reach levels ABOVE the member holding
//! it, but it too is asked per node, so it refuses exactly the levels whose split
//! it actually breaks (review finding S1 — balance is a property of the
//! CONCATENATED rendering, not of each scalar in isolation). Refusals are counted
//! and named, by path, in the census too. Neither kind of gap is ever silent.
//!
//! Both egress readers and the golden reader parse JSON STRICTLY — a duplicate
//! object key is malformed output on the CLI side and a discarded oracle on the
//! golden side, never something to reconcile (see `golden::strict_json`).
//!
//! # Coverage census
//!
//! In [`coverage_census`] (its own file under the campsite rule, a CHILD of this
//! lane so it reads the same [`CASES`] declaration):
//! `committed_fixture_coverage_census` enumerates the git-committed `*-Data.db`
//! fixtures from `git ls-files` and requires each to be either a compared case or
//! a NAMED entry in `coverage_census::NOT_COMPARABLE` with a reason. A new
//! committed fixture therefore has to be classified rather than silently
//! uncovered — derived at run time from committed source, not from a hand-kept
//! count.
//!
//! Its UNIT is one committed `*-Data.db`, not one table: the generation the
//! resolver stages is the compared one, and any OTHER committed generation of the
//! same table is named as unaccounted for rather than covered by its sibling
//! (review round 21).
//!
//! Every such entry names its unsupported shapes as an `Unsupported` SET rather
//! than as prose, and the census VERIFIES that set against the golden PAIRED with
//! each committed generation it excludes: the shapes it declares must be exactly
//! the ones `unsupported_shapes` finds in that generation's own
//! `*-Data.db.jsonl`. A stale or mis-stated exclusion therefore FAILS instead of
//! quietly hiding a fixture that is in fact comparable.
//!
//! The two halves are tied together by
//! `every_declarable_shape_is_one_the_golden_reader_refuses`, which builds a
//! minimal golden carrying each shape and requires `golden_rows` to reject it. So
//! "this shape is why the table is excluded" is a checked claim on both sides: the
//! shape is in the golden, and the reader refuses that shape.

#![cfg(feature = "state_machine")]

#[path = "support/golden_value_parity.rs"]
mod golden;

// The coverage census — which committed fixtures this lane is accountable for —
// lives in its own file under the campsite rule, as a CHILD of this lane so it
// reads the same `CASES` declaration the comparison does.
#[path = "support/issue_1491_coverage_census.rs"]
mod coverage_census;

use golden::committed_set::{require_tracked_oracle, CommittedSet};
use golden::compare::gap::Divergence;
use golden::compare::{cli_csv_rows, cli_json_rows, compare_rows, golden_path, stage_single_table};
use golden::fixture_root;
use golden::schema::{ColumnKind, CqlType, TableSchema};
use golden::{golden_rows, Egress, Multicell};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Whether a case's fixture is guaranteed present.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    /// Value paths excluded from the comparison, each naming the egress format(s)
    /// the divergence is observed in and the defect it is waiting on. Reported in
    /// the run census so an exclusion is never silent.
    skips: &'static [Skip],
}

/// One declared value-comparison gap.
///
/// # Why the scope is per FORMAT
///
/// A divergence is frequently a property of ONE format's value vocabulary rather
/// than of the value: JSON has no literal for `Infinity`/`-Infinity`/`NaN`, so
/// the JSON egress renders them `null` and the value is lost, while CSV renders
/// every cell as text and carries the same three tokens verbatim — measured on
/// `test_signed_coll.signed_special_collections`, whose CSV `sf` field is
/// `{-Infinity, -1.5, -0e0, 0e0, 2.5, Infinity, NaN}` against the golden's
/// `["-Infinity","-1.5","-0.0","0.0","2.5","Infinity","NaN"]`.
///
/// A gap declared for BOTH formats when only one diverges is therefore pure
/// coverage loss: it drops the column from the format that renders it correctly
/// (issue #1491 review finding K1). The scope is checked, not just declared — a
/// path listed for a format where nothing diverges is reported by
/// `Report::stale_skips` as a stale exclusion and FAILS that lane — and so is one
/// whose divergence has since been FIXED, which is how a gap here retires itself.
struct Skip {
    /// The fully-qualified path from the row: `sf` for a whole column, `e.home`
    /// for one field of a UDT column.
    path: &'static str,
    /// The egress format(s) this gap applies to. Never empty — a gap that
    /// applies to no format states nothing, and is rejected when the case is
    /// validated against the DDL.
    formats: &'static [Egress],
    /// WHICH divergence this gap stands for, in a form the comparator CHECKS at
    /// the path (see `golden::compare::gap::Divergence`).
    ///
    /// Without it a gap suppressed whatever happened at its path, so each one was
    /// a permanent blind spot for its whole column: the empty-collection gaps
    /// below also suppressed those columns' NON-EMPTY rows, and `e.home` changing
    /// from blob hex to arbitrary text would have passed as the documented gap
    /// (issue #1491 review round 17). A mismatch that is NOT the declared
    /// divergence is now an ordinary diff.
    divergence: Divergence,
    /// The measured divergence in prose, for the census. States the same thing
    /// [`Self::divergence`] states, for a reader rather than for the comparator.
    why: &'static str,
}

/// Both egress formats, for a divergence measured in each of them.
const BOTH: &[Egress] = &[Egress::Json, Egress::Csv];

impl Skip {
    fn applies_to(&self, egress: Egress) -> bool {
        self.formats.contains(&egress)
    }

    /// How this gap is named in the run census. Names the CHECKED divergence
    /// alongside the prose, so the census states which rule the run applied and
    /// not only how the gap was described.
    fn describe(&self) -> String {
        let formats: Vec<&str> = self
            .formats
            .iter()
            .map(|f| match f {
                Egress::Json => "json",
                Egress::Csv => "csv",
            })
            .collect();
        format!(
            "{} [{}] ({} — checked as {:?})",
            self.path,
            formats.join(","),
            self.why,
            self.divergence
        )
    }
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "snappy_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "deflate_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "zstd_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "uncompressed_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "short_final_chunk",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
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
        skips: &[],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparity",
        table: "live_clustering",
        schema: "compaction-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_collections",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_null_inner",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
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
        // NO SKIPS, and removing the one that was here is COVERAGE RECOVERED.
        // `e.home` (a frozen<address> inside a frozen<employee>) was excluded
        // because the golden decoded it while both CLI formats emitted the inner
        // UDT's raw bytes as blob hex — one instance of the two-divergent-decoder
        // defect #3722 fixed. The sides now AGREE, and THIS LANE caught the stale
        // exclusion: it refuses one that suppresses no divergence. So `e.home` is
        // value-compared like its `e.name`/`e.level` siblings.
        // `Divergence::NestedFrozenUdtRendersAsBlobHex` is KEPT — still fixture
        // data for the gap machinery's own unit tests, which exercise the
        // exclusion MECHANISM, not live behaviour.
        skips: &[],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "frozen_int_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    // The inet/time multicell-collection ORDERING fixture (#3790). COMPARED, not
    // excluded: its only non-plain shape is a multicell cell tombstone, which
    // `golden_dump_shapes` says "cannot justify an exclusion".
    Case {
        presence: Presence::Committed,
        keyspace: "test_comparator_order",
        table: "collection_order",
        schema: "issue-3790-comparator-ordering",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("inet_set", Multicell::Set),
            ("inet_map", Multicell::Map),
            ("time_set", Multicell::Set),
            ("time_map", Multicell::Map),
            ("pair_set", Multicell::Set),
        ],
        // Two MEASURED rendering divergences, neither of them ordering. What each
        // skip costs for ORDER differs, and saying "order is still compared" of
        // both would be false (roborev job 69):
        //   * inet_set: the matcher proves the two sides are the SAME address, so a
        //     reordered set fails — element [0] would pair ::1 against 9.0.0.1.
        //   * pair_set: NestedFrozenValueLeftUndecodedByGolden compares no content,
        //     so THIS LANE would not notice the tuples being reordered with the
        //     count preserved. That order is pinned instead by
        //     cqlite-core/tests/issue_3790_collection_order_cassandra_golden.rs,
        //     which asserts pair_set's cell-path sequence (inet-major, time-minor,
        //     both partitions) directly against the same golden.
        skips: &[
            Skip {
                path: "inet_set",
                formats: BOTH,
                divergence: Divergence::InetIpv6RendersCompressed,
                why: "golden spells IPv6 expanded (getHostAddress), egress compressed; the matcher proves both parse to the SAME address, and element ORDER is still compared",
            },
            Skip {
                path: "inet_map",
                formats: BOTH,
                divergence: Divergence::InetMapKeyIpv6SpellingNotPairableByThisLane,
                why: "inet map keys never pair across the two IPv6 spellings, so the column is NOT COMPARED AT ALL (null/malformed/wrong-COUNT unchecked); its ORDER is pinned by issue_3790_collection_order_cassandra_golden.rs",
            },
            Skip {
                path: "pair_set",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen tuple<inet, time> as colon-joined text while the CLI decodes it; only the SHAPE is checked — so a REORDERING of the tuples is NOT detected here either, and pair_set's order is pinned by issue_3790_collection_order_cassandra_golden.rs instead",
            },
        ],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_special_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[("sd", Multicell::Set), ("sf", Multicell::Set)],
        // TWO declared gaps, JSON-lane-only, and NEITHER is an egress defect
        // awaiting a fix (issue #3644 items 2 and 3) — each variant's own docs
        // carry the oracle. `sf`'s non-finite `null` is what
        // `DoubleType.toJSONString:114-123` returns, and the golden's quoted
        // `"Infinity"`/`"NaN"` are the cell-PATH `getString` artifact
        // (`JsonTransformer.java:452`), not the egress oracle; `sd`'s remaining gap
        // is this COMPARATOR's f64 parse, the egress having been fixed to emit the
        // unquoted number `DecimalType.toJSONString:314-317` requires. Both columns
        // are compared IN FULL in the CSV lane, where every cell is text and the
        // three tokens and all 33 digits survive verbatim — a `BOTH` scope dropped
        // the whole column from a format that renders it correctly (finding K1).
        skips: &[
            Skip {
                path: "sf",
                formats: &[Egress::Json],
                divergence: Divergence::NonFiniteFloatRendersAsJsonNull,
                why: "CORRECT BEHAVIOUR, not a defect: cassandra-5.0.8 \
                      DoubleType.toJSONString:114-123 returns the literal `null` for \
                      NaN/Infinity/-Infinity, and CQLite matches it; the golden's quoted \
                      tokens are the cell-PATH getString artifact \
                      (JsonTransformer.java:452), not the egress oracle. The set's \
                      FINITE members are compared",
            },
            Skip {
                path: "sd",
                formats: &[Egress::Json],
                divergence: Divergence::ExactDecimalNotCarriedByThisLanesJsonParse,
                why: "COMPARATOR LIMITATION, not an egress divergence: the CLI emits the \
                      unquoted number DecimalType.toJSONString:314-317 requires, but this \
                      lane's JSON parse holds it as an f64; both sides must still be the \
                      same double, the CSV lane compares every digit, and the egress text \
                      is pinned by tests/issue_3644_json_decimal_unquoted.rs",
            },
        ],
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
        skips: &[],
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
        skips: &[],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "multiclustering_table",
        schema: "multiclustering-table-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "wide_multiclustering_small",
        schema: "wide-multiclustering-small-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skips: &[],
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
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_writeparity",
        table: "partition_boundary",
        schema: "write-load-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
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
        skips: &[],
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
        skips: &[],
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
        skips: &[],
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
        skips: &[
            Skip {
                path: "ml",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell list renders as [] where Cassandra reads null",
            },
            Skip {
                path: "ms",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell set renders as {} where Cassandra reads null",
            },
            Skip {
                path: "mm",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell map renders as {} where Cassandra reads null",
            },
        ],
    },
    // test-data/schemas/nested-udt-keys.cql — (id int PRIMARY KEY) plus ten
    // columns nesting the `key_part` UDT inside tuples, sets, lists and maps,
    // both as map KEYS and as map VALUES (issue #3500). Comparable: the golden
    // carries no shape `unsupported_shapes` refuses — no partition or row
    // deletion, no TTL, no static block, and only `row` elements. Its
    // `marked_deleted` markers are all CELL-level collection tombstones on the
    // six non-frozen columns, which reconcile to a value this lane compares
    // (see `coverage_census::NOT_COMPARABLE`'s note on cell deletions), so an
    // exclusion here would be coverage loss rather than a reason.
    //
    // `multicell` names exactly the six NON-frozen collections from the DDL;
    // the four `f_*` columns are `frozen<…>` and therefore single-cell.
    Case {
        presence: Presence::Committed,
        keyspace: "test_nested_udt_keys",
        table: "nested_udt_keys",
        schema: "nested-udt-keys",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("s_tuple_udt", Multicell::Set),
            ("s_set_udt", Multicell::Set),
            ("m_tuple_udt", Multicell::Map),
            ("s_list_udt", Multicell::Set),
            ("s_map_udt_key", Multicell::Set),
            ("s_map_udt_val", Multicell::Set),
        ],
        // EVERY column except `id` and `f_set_tuple_udt` is excluded, in TWO
        // distinct classes. Stated as the SURVIVING SET rather than as a count of
        // the excluded: a count here drifted once already (roborev job 308 caught
        // "Eight" after a ninth skip was added), and it drifted even though it sits
        // in the same file as the `Skip` list that invalidates it — co-location is
        // not enough when the edit that changes the number is not the edit that
        // reads it. The surviving set is also what a reader needs: `f_set_tuple_udt`
        // is a genuinely nested frozen column that IS compared, which is why this is
        // a CASES entry rather than a NOT_COMPARABLE one. The authoritative count is
        // emitted by the census line itself ("N cells compared, M of them
        // containers") and needs no prose duplicate.
        //
        // CLASS 1 — the golden leaves the nested frozen element UNDECODED (raw
        // bytes as hex for a collection, colon-joined text for a tuple) while the
        // CLI decodes it. A value disagreement, in the direction OPPOSITE to
        // `NestedFrozenUdtRendersAsBlobHex`.
        //
        // CLASS 2 — a LANE LIMITATION, not a disagreement: the declared map KEY is
        // a container and this lane has no pairing rule for one, so the two sides
        // are never compared. Tracked for real support in #3726; when that lands
        // these four skips go stale and FAIL, which is what removes them.
        skips: &[
            Skip {
                path: "s_tuple_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen tuple element as sstabledump's colon-joined text while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_set_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner set as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_list_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner list as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_map_udt_key",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner map as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_map_udt_val",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner map (UDT as VALUE) as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            // THE FOUR CONTAINER-KEYED MAPS — a LANE limitation, not a value
            // disagreement. `compare_map` pairs entries by canonical SCALAR key form
            // and refuses a container key outright, so these columns are not compared
            // AT ALL. The skip is whole-column and therefore OVER-SKIPS: it also
            // suppresses a null, a malformed {key,value} array, a wrong entry count
            // and a wrong tuple arity here. That cost is accepted and documented
            // rather than bounded — three review rounds (roborev 302/305/306) showed
            // that bounding it means reimplementing `compare_map`'s own feature list,
            // because a Skip is path-scoped to a column and cannot express "compare
            // everything except the keys". Real support needs a container
            // representation in `Canon` (scalar-only today); tracked in #3726, and
            // these four skips go stale and FAIL the lane when it lands.
            Skip {
                path: "m_tuple_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is tuple<key_part, int> — this lane pairs map keys by canonical scalar form only, so the column is NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_tuple_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen tuple<key_part, int> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_set_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen set<key_part> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array and a wrong entry COUNT are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_tuple_list_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen tuple<list<key_part>, int> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
        ],
    },
];

fn repo_root() -> PathBuf {
    golden::datasets_root::repo_root()
}

fn schema_file(schema: &str) -> PathBuf {
    repo_root()
        .join("test-data/schemas")
        .join(format!("{schema}.cql"))
}

/// Cross-check the hand-transcribed case declaration against the committed DDL.
///
/// The case table names the keyspace, the key columns and the multicell kinds; the
/// DDL is the authority for all three. A disagreement is reported (and fails the
/// case) instead of being tolerated, because a wrong transcription weakens every
/// comparison built on it — the wrong pk means rows pair wrongly, a missed
/// multicell column means the golden reader reconstructs the wrong container, and a
/// wrong keyspace means no fixture is found at all, which a fetched-corpus case
/// reports as a legal skip.
fn schema_agrees_with_case(case: &Case, table: &TableSchema) -> Vec<String> {
    let mut out = Vec::new();
    // The KEYSPACE, cross-checked like every other declaration this case makes
    // (review round 19, finding Y1). It is the declaration whose typo is HARDEST to
    // see: a mistyped keyspace does not fail anything by itself — it makes
    // `resolve_fixture` look for `<typo>/<table>`, which no root holds, so a
    // `Presence::Corpus` case resolves as `NOT PRESENT` (a LEGAL skip) and its whole
    // coverage disappears behind a green run. Checked here, before any fixture is
    // resolved, so the typo fails on every machine.
    //
    // CQL identifiers are case-insensitive unless quoted, and the reader lowercases
    // what it parses, so the comparison lowercases the case's side too.
    match table.keyspace.as_deref() {
        Some(declared) if declared == case.keyspace.to_ascii_lowercase() => {}
        Some(declared) => out.push(format!(
            "the case declares keyspace `{}` but the committed schema declares this \
             table in keyspace `{declared}`",
            case.keyspace
        )),
        // UNVERIFIABLE is not agreement: a positive verdict needs an affirmative
        // measurement (CLAUDE.md), and a schema that states no keyspace at all (no
        // `USE`, no qualified name) measures nothing about this declaration. Every
        // committed schema this lane reads does state one, so this arm is a broken
        // fixture rather than a rule that reds on correct input.
        None => out.push(format!(
            "the case declares keyspace `{}`, and the committed schema states no \
             keyspace for this table (no `USE` and no keyspace-qualified name) — the \
             declaration cannot be checked against anything",
            case.keyspace
        )),
    }
    let declared_pk: Vec<&str> = table.partition_key.iter().map(String::as_str).collect();
    let declared_ck: Vec<&str> = table.clustering.iter().map(String::as_str).collect();
    if declared_pk != case.pk {
        out.push(format!(
            "the case declares partition key {:?} but the committed CREATE TABLE declares \
             {declared_pk:?}",
            case.pk
        ));
    }
    if declared_ck != case.ck {
        out.push(format!(
            "the case declares clustering key {:?} but the committed CREATE TABLE declares \
             {declared_ck:?}",
            case.ck
        ));
    }
    for column in &table.columns {
        let name = column.name.as_str();
        let declared = case
            .multicell
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, kind)| *kind);
        // The DDL's own answer: a NON-frozen collection is multicell, a frozen one
        // is a single value cell.
        let from_ddl = if column.is_multicell() {
            match &column.ty {
                CqlType::Set(_) => Some(Multicell::Set),
                CqlType::List(_) => Some(Multicell::List),
                CqlType::Map(_, _) => Some(Multicell::Map),
                // A non-frozen UDT is multicell too, and the golden reader has no
                // reconstruction rule for it — so it is named, never guessed at.
                other => {
                    out.push(format!(
                        "column `{name}` is a NON-frozen {} — multicell shapes other than \
                         set/list/map are not supported by this lane",
                        other.describe()
                    ));
                    continue;
                }
            }
        } else {
            None
        };
        if declared != from_ddl {
            out.push(format!(
                "column `{name}` ({}): the case declares multicell {declared:?} but the \
                 committed CREATE TABLE implies {from_ddl:?}",
                column.ty.describe()
            ));
        }
        if column.kind == ColumnKind::Static {
            out.push(format!(
                "column `{name}` is STATIC — a static block is read-time reconciliation, so \
                 the table belongs in NOT_COMPARABLE"
            ));
        }
    }
    for (name, _) in case.multicell {
        if table.column(name).is_none() {
            out.push(format!(
                "the case declares multicell column `{name}`, which the committed CREATE \
                 TABLE does not declare"
            ));
        }
    }
    for (i, skip) in case.skips.iter().enumerate() {
        let path = skip.path;
        // A gap that names no format states nothing and suppresses nothing, so it
        // could only ever read as coverage that exists.
        if skip.formats.is_empty() {
            out.push(format!(
                "the case declares a skip for `{path}` with no egress format — a gap that \
                 applies to no format states nothing"
            ));
        }
        // Two entries for the same (path, format) would each be handed to
        // `SkipPaths`, so a stale one could be masked by its twin's hit.
        for earlier in &case.skips[..i] {
            if earlier.path == path {
                let overlap: Vec<&Egress> = skip
                    .formats
                    .iter()
                    .filter(|f| earlier.formats.contains(f))
                    .collect();
                if !overlap.is_empty() {
                    out.push(format!(
                        "the case declares `{path}` twice for the same egress format(s) \
                         {overlap:?} — one entry would mask the other's staleness"
                    ));
                }
            }
        }
        let (column, rest) = match path.split_once('.') {
            Some((column, rest)) => (column, Some(rest)),
            None => (path, None),
        };
        let Some(declared) = table.column(column) else {
            out.push(format!(
                "the case declares a skip for `{path}`, whose column `{column}` the \
                 committed CREATE TABLE does not declare — the declared gap is stale"
            ));
            continue;
        };
        if let Some(rest) = rest {
            if let Err(why) = resolve_field_path(&declared.ty, rest) {
                out.push(format!(
                    "the case declares a skip for `{path}`, which the committed DDL \
                     does not resolve: {why} — the declared gap is stale"
                ));
            }
        }
    }
    out
}

/// Resolve a dotted `field.subfield` tail of a skip path against the committed
/// DDL, so a field-scoped exclusion naming a field that does not exist FAILS
/// instead of silently matching nothing.
///
/// Only UDT field steps are resolvable: a case table can name
/// `column.field.subfield`, and nothing else. A collection element or map entry
/// has no stable name to write down, so the walk's positional path segments
/// (`col[0]`, `col[key]`) are deliberately NOT expressible here — an exclusion
/// that cannot be checked against the DDL is not one this lane accepts.
fn resolve_field_path(ty: &CqlType, rest: &str) -> Result<(), String> {
    let (step, tail) = match rest.split_once('.') {
        Some((step, tail)) => (step, Some(tail)),
        None => (rest, None),
    };
    let CqlType::Udt(udt) = ty else {
        return Err(format!(
            "`{step}` is a field step, but the declared type here is `{}`, which has no \
             named fields",
            ty.describe()
        ));
    };
    let field_ty = udt
        .fields
        .iter()
        .find(|(name, _)| name == step)
        .map(|(_, t)| t)
        .ok_or_else(|| format!("the UDT `{}` declares no field `{step}`", udt.name))?;
    match tail {
        Some(tail) => resolve_field_path(field_ty, tail),
        None => Ok(()),
    }
}

/// Run `export` for one table into `out`, returning its contents.
fn export(case: &Case, data_dir: &Path, out: &Path, format: &str) -> String {
    let schema = schema_file(case.schema);
    // Three-valued (issue #1491 review finding V1's sweep): `is_file()` collapses an
    // unreadable path onto an absent one, so both were reported as "unreadable" —
    // fail-closed either way, but naming the wrong cause sends the reader looking for
    // a file that is right there. The committed schemas are checkout-relative source,
    // so any answer but "a regular file" is a broken checkout (#3148).
    match golden::fs_probe::presence(&schema) {
        Ok(golden::fs_probe::Presence::File) => {}
        Ok(other) => panic!(
            "committed schema {} is {} (see #3148)",
            schema.display(),
            other.describe()
        ),
        Err(why) => panic!("committed schema: {why} (see #3148)"),
    }
    let qualified = format!("{}.{}", case.keyspace, case.table);
    // Every PATH argument is handed to the CLI as an `OsStr`, never through
    // `to_string_lossy()`: that substitutes U+FFFD for each byte that is not valid
    // UTF-8, so a staged directory or an output file under a path that is not valid
    // UTF-8 would be handed to the CLI as a DIFFERENT path than the one this test
    // reads back — the same defect as the golden pairing in
    // `golden_fixture_staging` (issue #1491 review finding W2). A lossy conversion
    // is fine in a diagnostic message and never in a path something opens.
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("export")
        .arg(out)
        .arg("--format")
        .arg(format)
        .arg("--table")
        .arg(&qualified)
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

/// Why a case's fixture could not be resolved.
enum FixtureError {
    /// Fail-closed: this fails the case whatever tier it is in.
    Failure(String),
    /// A fetched-corpus fixture that is legitimately absent — declared in the
    /// census rather than swallowed.
    CorpusAbsent(String),
}

/// A case's fixture directory, and which root supplied it.
///
/// A git-committed case is served from the CHECKOUT copy and only from there: an
/// external `CQLITE_DATASETS_ROOT` corpus may carry its own copy of the same table —
/// stale, regenerated, or simply a different generation — and comparing that copy
/// while the census reports the committed table as covered is finding J1. A
/// fetched-corpus case keeps the evidence-based walk over the candidate roots.
///
/// The `Presence` declaration is CHECKED against `git ls-files`, never trusted: a
/// case declared committed whose table git does not track, and a case declared
/// fetched-corpus that git DOES track, are both mis-declarations that fail here.
///
/// A COMMITTED case can never reach the skip path: this function's only
/// [`FixtureError::CorpusAbsent`] arm is the fetched-corpus one, so every way a
/// committed fixture can fail to resolve is a [`FixtureError::Failure`]. Pinned by
/// `a_committed_case_can_never_resolve_to_a_skip`.
fn resolve_fixture(
    case: &Case,
    committed: &CommittedSet,
    checkout: &Path,
) -> Result<fixture_root::Fixture, FixtureError> {
    let tracked = committed
        .tables
        .get(&(case.keyspace.to_string(), case.table.to_string()));
    match (case.presence, tracked) {
        (Presence::Committed, tracked) => {
            fixture_root::committed_fixture_dir(tracked, case.keyspace, case.table, checkout)
                .map_err(FixtureError::Failure)
        }
        (Presence::Corpus, Some(_)) => Err(FixtureError::Failure(
            "declared Presence::Corpus, but `git ls-files` tracks a *-Data.db for it — \
             a committed fixture must be declared Presence::Committed so it is compared \
             from the checkout copy, and fails closed when absent"
                .to_string(),
        )),
        // The two corpus misses are DIFFERENT verdicts: genuinely absent is the
        // legal skip, while a selected-but-unusable root is a failure. Flattening
        // both onto `CorpusAbsent` made an unreadable or self-contradictory corpus
        // produce a green run labelled "NOT PRESENT" (review finding M3).
        (Presence::Corpus, None) => {
            fixture_root::corpus_fixture_dir(case.keyspace, case.table, checkout)
                .map_err(corpus_miss)
        }
    }
}

/// A fetched-corpus miss as a case verdict. Genuinely absent is the legal skip; a
/// root that was selected and then could not be used is a FAILURE.
///
/// A named function rather than an inline `match` so the mapping is pinned by
/// `an_unusable_corpus_is_a_failure_and_only_a_true_absence_skips` — flattening the
/// two produced a green run labelled "NOT PRESENT" for an unreadable or malformed
/// corpus (review finding M3), and nothing else in the lane can observe that.
fn corpus_miss(miss: fixture_root::CorpusMiss) -> FixtureError {
    match miss {
        fixture_root::CorpusMiss::Absent(why) => FixtureError::CorpusAbsent(why),
        fixture_root::CorpusMiss::Unusable(why) => FixtureError::Failure(why),
    }
}

/// JSON egress: every cell value deep-compared against the golden.
#[test]
fn json_egress_matches_sstabledump_goldens() {
    run_lane(Egress::Json);
}

/// CSV egress: every cell value compared against the golden.
///
/// Containers included: the flat `{k: v}` / `[a, b]` text is decoded back into the
/// golden's shape and each member compared by the same rules the JSON lane uses.
/// The SYNTAX carrying them is a CQLite-only text form with no external authority
/// and is asserted as nothing more than a grammar to invert; the VALUES it carries
/// are the golden's. A cell the golden's own content cannot survive unquoted is
/// refused and named in the census — see the `csv_container` support module.
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
    // Cases compared from ONE of several SSTable directories (finding L3). Reported
    // affirmatively, at 0 as well, so "every covered table has exactly one" is a
    // measurement a reader can see rather than an assumption.
    let mut narrowed: Vec<String> = Vec::new();

    // The git-committed fixture set, read from `git ls-files` once per lane: it decides which
    // root each case's golden comes from (finding J1) and CHECKS every `Presence`
    // declaration. An unusable listing fails the lane rather than being worked
    // around, since without it no case's tier is known.
    let committed = match fixture_root::committed_listing()
        .and_then(|listing| golden::committed_set::committed_set(&listing))
    {
        Ok(committed) => committed,
        Err(why) => panic!("AD2 {format}: cannot read the committed fixture set: {why}"),
    };
    let checkout = fixture_root::checkout_sstables_root();

    for case in CASES {
        let qualified = format!("{}.{}", case.keyspace, case.table);
        // The committed CREATE TABLE is the authority for the row's column set and
        // each value's CQL type (issue #1491 review findings). Loaded per case, and
        // an unreadable/unparseable schema is a hard failure — a case with no
        // declared column set could only compare permissively.
        //
        // FIRST, before the fixture is resolved (review finding U2). The schemas are
        // committed source, resolved checkout-relative and therefore present on every
        // machine (#3148), whereas a fetched-corpus fixture may legitimately be
        // absent — and the absent-fixture branch below `continue`s. Loading the schema
        // after it meant a stale `pk`, `ck`, `multicell` or skip-path declaration for
        // an optional case was checked ONLY on a machine that happened to hold the
        // corpus: a machine-dependent silent gap. Checked here, a stale declaration
        // fails everywhere.
        let table = match golden::schema::load(&schema_file(case.schema), case.table) {
            Ok(table) => table,
            Err(why) => {
                failures.push(format!("{qualified}: committed schema unusable: {why}"));
                continue;
            }
        };
        // The case table transcribes the key columns and the multicell kinds by
        // hand; cross-check both against the DDL so a wrong transcription is a
        // failure here rather than a weaker comparison later.
        for why in schema_agrees_with_case(case, &table) {
            failures.push(format!("{qualified}: {why}"));
        }
        // must_run: a committed fixture is never allowed to skip.
        let resolved = match resolve_fixture(case, &committed, &checkout) {
            Ok(resolved) => resolved,
            // A committed fixture is present in every checkout, so an unresolvable
            // one is a real failure, never a skip — and so is a `Presence` the git
            // listing contradicts.
            Err(FixtureError::Failure(why)) => {
                failures.push(format!("{qualified}: {why}"));
                continue;
            }
            // A fetched-corpus fixture may legitimately be absent; the absence is
            // DECLARED in the census rather than swallowed.
            Err(FixtureError::CorpusAbsent(why)) => {
                census.push(format!(
                    "  {qualified}: NOT PRESENT (fetched corpus) — {why}"
                ));
                continue;
            }
        };
        let fixture = resolved.dir;
        let root_source = resolved.source;
        // The census line below states this case's PROVENANCE, so the claim is
        // CHECKED against the tier rather than trusted (issue #1491 review finding
        // T3): only a case whose table `git ls-files` tracks may be reported as the
        // git-tracked oracle, and the walk a fetched-corpus case takes establishes
        // only that some root HOLDS the table — even when the root it lands on is the
        // checkout's own. A mismatch is a FAILURE, because a census that misnames the
        // oracle is worse than one that says nothing.
        let tracked_provenance = root_source == fixture_root::RootSource::GitTracked;
        if tracked_provenance != (case.presence == Presence::Committed) {
            failures.push(format!(
                "{qualified}: declared {:?} but its fixture resolved with provenance \
                 `{}` — the census would state the wrong oracle",
                case.presence,
                root_source.as_str()
            ));
            continue;
        }
        // The narrowing, COUNTED: a table with several SSTable directories is
        // compared from the first, so the others are untested — declared here (and
        // tallied for the lane's own summary line) rather than left silent. For a
        // git-COMMITTED fixture this is now only half the report: the coverage
        // census classifies per `*-Data.db` and REFUSES the generation nothing
        // compares, so a committed narrowing fails there while this line says which
        // generation was read. A fetched-corpus narrowing is the one that stands on
        // its own — git tracks nothing for it, so no census accounts for it.
        if resolved.of_dirs > 1 {
            narrowed.push(format!(
                "{qualified}: SSTable directory 1 of {} compared ({} untested)",
                resolved.of_dirs,
                resolved.of_dirs - 1
            ));
        }
        let golden_file = match golden_path(&fixture) {
            Ok(path) => path,
            Err(why) => {
                failures.push(format!("{qualified}: {why}"));
                continue;
            }
        };
        // A COMMITTED case's ORACLE is committed too (review finding BB1). The line
        // above asked the FILESYSTEM which golden describes the staged SSTable, and
        // that question cannot tell a git-tracked golden from an untracked file of
        // the same name — one a fetched corpus, a stray local copy or a previous run
        // left in the tracked directory. So the golden the case was compared against
        // is required to BE the one `git ls-files` pairs with the tracked
        // `*-Data.db`; it is the same fix as pinning the FIXTURE to the checkout copy
        // (finding J1), one file over, and it keeps `golden_path`'s own refusals (a
        // directory holding two `*-Data.db`, a golden describing another generation)
        // rather than replacing them. Keyed on the ESTABLISHED provenance, not on the
        // declaration — the two were just cross-checked above.
        if root_source == fixture_root::RootSource::GitTracked {
            if let Err(why) = require_tracked_oracle(
                &committed,
                case.keyspace,
                case.table,
                &checkout,
                &golden_file,
            ) {
                failures.push(format!("{qualified}: {why}"));
                continue;
            }
        }
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

        // FORMAT-SCOPED: only the gaps declared for THIS egress format suppress
        // anything, so a column that diverges in one format keeps being compared
        // in the other (review finding K1).
        let applicable: Vec<&Skip> = case.skips.iter().filter(|s| s.applies_to(egress)).collect();
        // The comparator is handed the DIVERGENCE alongside the path: a gap
        // suppresses the divergence it names and nothing else, so the declaration
        // has to travel with the exclusion (review round 17).
        let skip: Vec<(&str, Divergence)> =
            applicable.iter().map(|s| (s.path, s.divergence)).collect();
        let report = compare_rows(&expected, &actual, &table, case.pk, case.ck, &skip, egress);
        if report.diffs.is_empty() && report.compared_cells == 0 {
            failures.push(format!(
                "{qualified}: {format} comparison examined 0 cells — a vacuous pass"
            ));
            continue;
        }
        containers_compared += report.container_cells;
        containers_refused += report.ambiguous_container_cells;
        // A declared gap that did not suppress ITS DECLARED DIVERGENCE is a gap
        // that no longer describes the output: leaving it standing hides the fact
        // that it has closed (or was mis-stated) and keeps the coverage it costs
        // switched off. The cause travels with the entry — agreed, never reached,
        // unevaluable, or a divergence the gap does not declare — so the failure
        // says which of the four happened, and the remedy differs: the first three
        // mean the DECLARATION must go or be re-scoped, the fourth means the
        // divergence beside it must be explained (it is reported as a diff too).
        for stale in &report.stale_skips {
            failures.push(format!(
                "{qualified}: the declared gap {stale} suppressed no declared divergence \
                 in the {format} comparison — such a gap must be removed, re-scoped or \
                 (where something else diverged) explained, not left standing"
            ));
        }
        if report.diffs.is_empty() {
            census.push(format!(
                "  {qualified}: golden from {} — {} rows, {} cells compared ({} of them \
                 containers){}{}",
                root_source.as_str(),
                expected.len(),
                report.compared_cells,
                report.container_cells,
                // A refusal is a DECLARED GAP in the same style as `Skip`: named
                // at run time, never left as a bare counter. Each reason names the
                // refused POSITION, which is the granularity the refusal is decided
                // at — a cell's own root node (`fs (…)`) or one member of one
                // (`nl[0] (…)`, review finding P2).
                if report.ambiguous_container_cells > 0 {
                    format!(
                        ", DECLARED GAP: {} container cell(s) holding a REFUSED \
                         CSV-unrepresentable position: {}",
                        report.ambiguous_container_cells,
                        report.ambiguity_reasons.join("; ")
                    )
                } else {
                    String::new()
                },
                if applicable.is_empty() {
                    String::new()
                } else {
                    format!(
                        ", DECLARED GAP: {}",
                        applicable
                            .iter()
                            .map(|s| s.describe())
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
        Egress::Csv => {
            format!(", {containers_refused} holding a REFUSED CSV-unrepresentable position")
        }
        Egress::Json => String::new(),
    };
    eprintln!(
        "AD2 {format} container coverage: {containers_compared} collection/UDT cell(s) \
         value-compared{refusals}"
    );
    // The generation narrowing, stated the same way and affirmative at zero: a
    // table with several SSTable directories is compared from ONE of them, so the
    // count of such tables is part of what this lane measured (finding L3). A
    // second SSTable inside ONE directory is a different matter and is not counted
    // here — `compare::golden_path` FAILS on it, because the staged directory would
    // feed the CLI more data than any single golden describes. Nor does this line
    // stand alone for a git-committed fixture: the coverage census refuses the
    // generation nothing compares, so a committed narrowing is a FAILURE there and
    // only a fetched-corpus one is a declared gap.
    if narrowed.is_empty() {
        eprintln!(
            "AD2 {format} generation coverage: 0 case(s) narrowed — every compared \
             table has exactly ONE SSTable directory under the root that supplied it"
        );
    } else {
        eprintln!(
            "AD2 {format} generation coverage: DECLARED GAP, {} case(s) compared from \
             one of several SSTable directories:\n  {}",
            narrowed.len(),
            narrowed.join("\n  ")
        );
    }
    assert!(
        failures.is_empty(),
        "AD2 {format} egress value parity failed for {} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Every declared value-comparison gap names at least one egress format.
///
/// Checked here as well as in `schema_agrees_with_case` because that check runs
/// only for a case whose fixture resolved, so a `Presence::Corpus` case's
/// declaration would otherwise go unchecked wherever the fetched corpus is
/// absent. A gap that applies to no format suppresses nothing and states nothing,
/// so it could only ever read as coverage that exists.
#[test]
fn every_declared_gap_names_at_least_one_egress_format() {
    let mut bad: Vec<String> = Vec::new();
    for case in CASES {
        for skip in case.skips {
            let qualified = format!("{}.{}", case.keyspace, case.table);
            if skip.formats.is_empty() {
                bad.push(format!(
                    "{qualified}: the gap for `{}` names no egress format",
                    skip.path
                ));
            }
            // The census line is what makes a gap non-silent, so it must name the
            // path, the scope AND the divergence the run actually CHECKED — a gap
            // described only in prose could not be told from one whose checked rule
            // says something else (review round 17).
            let described = skip.describe();
            let checked = format!("{:?}", skip.divergence);
            if !described.contains(skip.path)
                || !described.contains('[')
                || !described.contains(&checked)
            {
                bad.push(format!(
                    "{qualified}: the census description of `{}` does not name its \
                     path, scope and checked divergence ({checked}): {described}",
                    skip.path
                ));
            }
        }
    }
    assert!(bad.is_empty(), "issue #1491 (K1):\n  {}", bad.join("\n  "));
}

/// Y1: the case's KEYSPACE is a checked declaration, and an UNVERIFIABLE one is
/// not agreement.
///
/// The demonstration this pins: with the check absent, mistyping a
/// `Presence::Corpus` case's keyspace made `resolve_fixture` find no fixture for
/// `<typo>.<table>`, which is the LEGAL fetched-corpus skip — so the run stayed
/// green with the case's whole comparison silently gone. Measured on
/// `test_types.nb_absent_vs_null_regular` before the fix: census `NOT PRESENT`, 0
/// failures.
#[test]
fn a_case_keyspace_the_committed_ddl_contradicts_is_a_failure() {
    let case = |keyspace| Case {
        presence: Presence::Corpus,
        keyspace,
        table: "t",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    };
    let declared = golden::schema::from_ddl(
        "USE right_ks; CREATE TABLE t (pk int, ck int, PRIMARY KEY (pk, ck));",
        "t",
    )
    .expect("the DDL parses");
    assert_eq!(
        schema_agrees_with_case(&case("right_ks"), &declared),
        Vec::<String>::new(),
        "the ordinary shape — an agreeing keyspace — must report nothing"
    );
    let why = schema_agrees_with_case(&case("wrong_ks"), &declared).join("; ");
    assert!(
        why.contains("wrong_ks") && why.contains("right_ks"),
        "the failure must name BOTH keyspaces: {why}"
    );
    // A schema stating no keyspace at all measures nothing about the declaration,
    // which is a failure and not agreement.
    let silent = golden::schema::from_ddl(
        "CREATE TABLE t (pk int, ck int, PRIMARY KEY (pk, ck));",
        "t",
    )
    .expect("the DDL parses");
    let why = schema_agrees_with_case(&case("right_ks"), &silent).join("; ");
    assert!(
        why.contains("states no keyspace"),
        "an unverifiable keyspace must fail, naming what is missing: {why}"
    );
}

/// M3: the corpus-miss mapping itself. An `Unusable` corpus must fail the case and
/// only a true `Absent` may skip it.
#[test]
fn an_unusable_corpus_is_a_failure_and_only_a_true_absence_skips() {
    match corpus_miss(fixture_root::CorpusMiss::Unusable("broken".to_string())) {
        FixtureError::Failure(why) => assert_eq!(why, "broken"),
        FixtureError::CorpusAbsent(why) => {
            panic!("an unusable corpus must not skip the case: {why}")
        }
    }
    match corpus_miss(fixture_root::CorpusMiss::Absent("not fetched".to_string())) {
        FixtureError::CorpusAbsent(why) => assert_eq!(why, "not fetched"),
        FixtureError::Failure(why) => {
            panic!("a genuinely absent fetched fixture is a legal skip: {why}")
        }
    }
}

/// M3's structural half: a COMMITTED case can never take the skip path, whatever
/// goes wrong. A committed fixture is present in every checkout, so an
/// unresolvable one is a real failure — and the census's "NOT PRESENT (fetched
/// corpus)" line must be reachable only from a fetched-corpus case.
#[test]
fn a_committed_case_can_never_resolve_to_a_skip() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    // An EMPTY committed set and an empty checkout: every resolution path a
    // committed case has is broken at once.
    let committed = golden::committed_set::committed_set(&[])
        .expect("an empty listing classifies, as the set of nothing committed");
    let case = Case {
        presence: Presence::Committed,
        keyspace: "no_such_ks",
        table: "no_such_table",
        schema: "basic-types",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    };
    match resolve_fixture(&case, &committed, tmp.path()) {
        Err(FixtureError::Failure(why)) => assert!(
            why.contains("tracks no *-Data.db"),
            "the failure must name what is missing: {why}"
        ),
        Err(FixtureError::CorpusAbsent(why)) => {
            panic!("a committed case must never take the skip path: {why}")
        }
        Ok(_) => panic!("nothing can resolve from an empty checkout"),
    }
}
