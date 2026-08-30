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
//! The CSV lane additionally REFUSES a container cell whose golden content cannot
//! survive the unquoted rendering (see `golden::csv_container`); refusals are
//! counted and named in the census too. Neither kind of gap is ever silent.
//!
//! Both egress readers and the golden reader parse JSON STRICTLY — a duplicate
//! object key is malformed output on the CLI side and a discarded oracle on the
//! golden side, never something to reconcile (see `golden::strict_json`).
//!
//! # Coverage census
//!
//! [`committed_fixture_coverage_census`] enumerates the git-committed
//! `*-Data.db` fixtures from `git ls-files` and requires each to be either a
//! compared case or a NAMED entry in [`NOT_COMPARABLE`] with a reason. A new committed fixture
//! therefore has to be classified rather than silently uncovered — derived at run
//! time from committed source, not from a hand-kept count.
//!
//! Every entry in [`NOT_COMPARABLE`] names its unsupported shapes as an
//! [`Unsupported`] SET rather than as prose, and the census VERIFIES that set
//! against the committed golden: the shapes it declares must be exactly the ones
//! [`unsupported_shapes`] finds in that table's `*-Data.db.jsonl`. A stale or
//! mis-stated exclusion therefore FAILS instead of quietly hiding a table that is
//! in fact comparable.
//!
//! The two halves are tied together by
//! [`every_declarable_shape_is_one_the_golden_reader_refuses`], which builds a
//! minimal golden carrying each shape and requires `golden_rows` to reject it. So
//! "this shape is why the table is excluded" is a checked claim on both sides: the
//! shape is in the golden, and the reader refuses that shape.

#![cfg(feature = "state_machine")]

#[path = "support/golden_value_parity.rs"]
mod golden;

use golden::compare::{cli_csv_rows, cli_json_rows, compare_rows, golden_path, stage_single_table};
use golden::dump_shapes::{unsupported_shapes, Unsupported};
use golden::fixture_root;
use golden::schema::{ColumnKind, CqlType, TableSchema};
use golden::{golden_rows, Egress, Multicell};
use std::collections::{BTreeMap, BTreeSet};
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
    /// The measured divergence, stated for the formats named above and only
    /// those.
    why: &'static str,
}

/// Both egress formats, for a divergence measured in each of them.
const BOTH: &[Egress] = &[Egress::Json, Egress::Csv];

impl Skip {
    fn applies_to(&self, egress: Egress) -> bool {
        self.formats.contains(&egress)
    }

    /// How this gap is named in the run census.
    fn describe(&self) -> String {
        let formats: Vec<&str> = self
            .formats
            .iter()
            .map(|f| match f {
                Egress::Json => "json",
                Egress::Csv => "csv",
            })
            .collect();
        format!("{} [{}] ({})", self.path, formats.join(","), self.why)
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
        // `frozen<address>` nested inside a `frozen<employee>`. The golden decodes
        // it (`{"street": "1 Navy Way", …}`); both CLI egress formats emit the
        // inner UDT's RAW BYTES as blob hex
        // (`0x0000000a31204e617679205761790000000941726c696e67746f6e…`).
        //
        // The exclusion is FIELD-scoped (`e.home`, not `e`) so the sibling fields
        // `e.name` and `e.level` are still value-compared. Excluding the whole
        // column left this case comparing nothing but its primary key while the
        // comment claimed otherwise (review finding F5).
        skips: &[Skip {
            path: "e.home",
            formats: BOTH,
            why: "nested frozen UDT renders as blob hex, not a decoded object",
        }],
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
        // MEASURED DIVERGENCE, and a JSON-ONLY one: `sf` is a `set<double>`
        // containing `Infinity`, `-Infinity` and `NaN`. The golden carries all
        // three by name (`["-Infinity",…,"Infinity","NaN"]`); JSON has no literal
        // for them, so the JSON egress emits `null` and the value is lost. The CSV
        // egress renders every cell as text and carries the same three tokens
        // verbatim (`{-Infinity, -1.5, -0e0, 0e0, 2.5, Infinity, NaN}`, which the
        // decimal canonicalization reads as the golden's `-0.0`/`0.0`), so CSV IS
        // compared here — a `BOTH` scope dropped the whole column from a format
        // that renders it correctly (review finding K1).
        //
        // A SECOND measured divergence, and JSON-only for the same reason: `sd`
        // (`set<decimal>`, exact 30-digit text) is compared in the CSV lane, where
        // every cell is text and the 30-digit values match exactly, but in the JSON
        // lane the egress renders a `decimal` as a JSON STRING
        // (`"-999999999999999999999999999999.999"`) where `cassandra-5.0.8`
        // `DecimalType.toJSONString` returns `BigDecimal.toString()` UNQUOTED, i.e.
        // a JSON number. The divergence is a property of the type, not of the
        // position, so it would show on a scalar `decimal` column too; it surfaced
        // here because `sd` is the only `decimal` in any compared case. It only
        // became visible once the kinding relaxation stopped being applied to the
        // CLI side (review finding M1) — while it was symmetric, the CLI's string
        // was read as a number at this stringified position.
        skips: &[
            Skip {
                path: "sf",
                formats: &[Egress::Json],
                why: "set<double> Infinity/-Infinity/NaN render as JSON null — JSON has \
                      no literal for them",
            },
            Skip {
                path: "sd",
                formats: &[Egress::Json],
                why: "decimal renders as a JSON string where cassandra-5.0.8 \
                      DecimalType.toJSONString emits an unquoted number",
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
                why: "empty multicell list renders as [] where Cassandra reads null",
            },
            Skip {
                path: "ms",
                formats: BOTH,
                why: "empty multicell set renders as {} where Cassandra reads null",
            },
            Skip {
                path: "mm",
                formats: BOTH,
                why: "empty multicell map renders as {} where Cassandra reads null",
            },
        ],
    },
];

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
/// The case table names the key columns and the multicell kinds; the DDL is the
/// authority for both. A disagreement is reported (and fails the case) instead of
/// being tolerated, because a wrong transcription weakens every comparison built
/// on it — the wrong pk means rows pair wrongly, and a missed multicell column
/// means the golden reader reconstructs the wrong container.
fn schema_agrees_with_case(case: &Case, table: &TableSchema) -> Vec<String> {
    let mut out = Vec::new();
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
fn resolve_fixture(
    case: &Case,
    committed: &fixture_root::CommittedFixtures,
    checkout: &Path,
) -> Result<fixture_root::Fixture, FixtureError> {
    let tracked = committed.get(&(case.keyspace.to_string(), case.table.to_string()));
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
        (Presence::Corpus, None) => {
            fixture_root::corpus_fixture_dir(case.keyspace, case.table, checkout)
                .map_err(FixtureError::CorpusAbsent)
        }
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
        .and_then(|listing| fixture_root::committed_fixtures(&listing))
    {
        Ok(committed) => committed,
        Err(why) => panic!("AD2 {format}: cannot read the committed fixture set: {why}"),
    };
    let checkout = fixture_root::checkout_sstables_root();

    for case in CASES {
        let qualified = format!("{}.{}", case.keyspace, case.table);
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
        // The narrowing, COUNTED: a table with several SSTable directories is
        // compared from the first, so the others are untested — declared here (and
        // tallied for the lane's own summary line) rather than left silent.
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

        // The committed CREATE TABLE is the authority for the row's column set and
        // each value's CQL type (issue #1491 review findings). Loaded per case, and
        // an unreadable/unparseable schema is a hard failure — a case with no
        // declared column set could only compare permissively.
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
        let skip: Vec<&str> = applicable.iter().map(|s| s.path).collect();
        let report = compare_rows(&expected, &actual, &table, case.pk, case.ck, &skip, egress);
        if report.diffs.is_empty() && report.compared_cells == 0 {
            failures.push(format!(
                "{qualified}: {format} comparison examined 0 cells — a vacuous pass"
            ));
            continue;
        }
        containers_compared += report.container_cells;
        containers_refused += report.ambiguous_container_cells;
        // A declared gap that did not SUPPRESS A DIVERGENCE is stale: it no
        // longer describes the output, so leaving it standing hides the fact that
        // the gap has closed (or was mis-stated) and keeps the coverage it costs
        // switched off. The cause travels with the entry — agreed, never reached,
        // or unevaluable — so the failure says which of the three happened.
        for stale in &report.stale_skips {
            failures.push(format!(
                "{qualified}: the declared gap {stale} suppressed no divergence in the \
                 {format} comparison — a declared gap that no longer applies to this \
                 egress format must be removed or re-scoped, not left standing"
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
                // at run time, never left as a bare counter.
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
        Egress::Csv => format!(", {containers_refused} REFUSED as CSV-unrepresentable"),
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
    // feed the CLI more data than any single golden describes.
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

/// Every git-committed `*-Data.db` fixture is either a comparable case or a NAMED,
/// reasoned exclusion. Derived from committed source at run time, so a newly
/// committed fixture must be classified instead of being silently uncovered.
#[test]
fn committed_fixture_coverage_census() {
    let root = repo_root();
    let listing = fixture_root::committed_listing()
        .unwrap_or_else(|why| panic!("cannot read the committed fixture set: {why}"));

    let mut committed: Vec<(String, String)> = Vec::new();
    // Every committed golden, per table: a table may have several SSTables and the
    // exclusion is a property of the SET, so the shape scan unions all of them.
    let mut goldens: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for line in &listing {
        // The same path parser the fixture-root selection uses, so "committed" means
        // one thing in this lane: an unrecognised shape is refused, not guessed at.
        let Some(path) = fixture_root::classify(line).unwrap_or_else(|why| panic!("{why}")) else {
            continue;
        };
        let key = (path.keyspace, path.table);
        if path.is_golden {
            goldens.entry(key).or_default().push(line.to_string());
        } else {
            committed.push(key);
        }
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
        // No golden at all is a FAILURE, not a pass: an exclusion no golden can
        // corroborate is exactly the unverifiable claim this check exists for.
        if files.is_empty() {
            stale.push(format!(
                "{qualified}: no committed *-Data.db.jsonl golden, so the declared shapes \
                 {declared:?} cannot be verified"
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
            "AD2 census: {qualified} EXCLUDED, verified in {} golden file(s): {} ({})",
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
            // path AND the scope.
            let described = skip.describe();
            if !described.contains(skip.path) || !described.contains('[') {
                bad.push(format!(
                    "{qualified}: the census description of `{}` does not name its \
                     path and scope: {described}",
                    skip.path
                ));
            }
        }
    }
    assert!(bad.is_empty(), "issue #1491 (K1):\n  {}", bad.join("\n  "));
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
