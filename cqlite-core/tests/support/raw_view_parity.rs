//! Physical-dump-parity sweep harness for the raw SSTable view (issue #4309).
//!
//! # Why this module exists
//!
//! Issue #4222 shipped the raw SSTable view (`<ks>.<table>_raw_sstable_data`)
//! with its stated correctness oracle — "physical dump parity is the
//! correctness oracle" — **unbuilt**: task 6.3 of that change was never
//! checked, only two fixtures were golden-asserted at all, and roughly half
//! the pinned column contract had no oracle coverage anywhere. This module is
//! that oracle: ONE comparison function, reused by every
//! `issue_4309_raw_view_parity_sweep_*_test.rs` lane, that reads a fixture's
//! Cassandra-written `sstabledump` golden and asserts the raw view reproduces
//! every physical timestamp / TTL / local-deletion-time / tombstone-kind fact
//! it states, byte-exact, in both directions.
//!
//! # The oracle, and what is NOT the oracle (#3041/#3042)
//!
//! Every expectation is derived at RUNTIME from the `*-Data.db.jsonl`
//! sstabledump golden that sits beside the very `Data.db` the query reads.
//! Nothing is transcribed into a Rust literal, and CQLite's own prior output
//! is never the check — a CQLite `file:line` is evidence of what CQLite does,
//! never of what is correct. Every accessor below PANICS rather than
//! defaulting when a golden lacks a fact, so a regenerated or truncated
//! fixture FAILs instead of passing vacuously.
//!
//! # How the golden is READ — the pinned Cassandra serializer, not inference
//!
//! Interpreting the JSONL needs the emitter's rules, and those come from the
//! pinned upstream source, never from what CQLite happens to do
//! (`git show cassandra-5.0.8:src/java/org/apache/cassandra/tools/JsonTransformer.java`):
//!
//! * `serializeRow` — `type` is `static_block` when `row.isStatic()`, else
//!   `row`; `serializeClustering` is called ONLY for a non-static row, so a
//!   static block carries no `clustering` at all. `liveness_info` is written
//!   only when `!liveInfo.isEmpty()` (an UPDATE-only row therefore has none),
//!   and its `ttl`/`expires_at`/`expired` only when `liveInfo.isExpiring()`.
//! * `serializeClustering` — writes the literal `"*"` for every component
//!   beyond `clustering.size()` (the PREFIX-bound case), and writes NO
//!   `clustering` field at all for a size-0 prefix.
//! * `serializeCell` — `tstamp` is written only when
//!   `liveInfo.isEmpty() || cell.timestamp() != liveInfo.timestamp()`, and
//!   `ttl`/`expires_at`/`expired` only when
//!   `cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl())`.
//!   An OMITTED field is therefore a statement that the cell agrees with the
//!   row's liveness marker, which is why [`golden::fold_simple_cell`] inherits it
//!   rather than treating it as absent.
//! * `serializeColumnData` — the pathless `{name, deletion_info}` entry is
//!   written only when `!complexData.complexDeletion().isLive()`, so its
//!   ABSENCE is the golden's way of saying `_complex_deletion = false`.
//! * `serializeTombstone` — a `range_tombstone_boundary` writes its OPEN
//!   bound (rendered `start`) and its CLOSE bound (rendered `end`) with their
//!   OWN `DeletionTime`s, in ONE entry — hence the two expected rows this
//!   harness derives from it.
//!
//! STATED ORACLE ASSUMPTION, because the JSONL cannot distinguish it: when a
//! row's liveness carries a TTL and a cell omits `ttl`, the serializer's
//! condition above permits two readings — the cell is expiring with the SAME
//! TTL (inherited here), or the cell is not expiring at all. The reading is
//! corroborated rather than assumed: CQLite decodes each cell's expiry from
//! the on-disk cell flags INDEPENDENTLY of the row marker, so a wrong
//! inheritance shows up as a FAILING assertion, not as a quiet pass. It
//! passes on every fixture in the sweep.
//!
//! # Column-contract coverage (issue #4309 AC5)
//!
//! Source of truth for the contract:
//! `cqlite-core/src/query/select_executor/raw_view/columns.rs`. The compared
//! set is DERIVED from the query result's own `metadata.columns` (never
//! transcribed), and every name below is asserted present before any row is
//! examined, so a contract column that silently disappears FAILs the sweep.
//!
//! COVERED — byte-exact against the golden, on every fixture that has the fact:
//!
//! | Column | Golden field |
//! |---|---|
//! | `<col>_timestamp` | cell `tstamp`, else the row's `liveness_info.tstamp` |
//! | `<col>_ttl` | cell `ttl`, else `liveness_info.ttl` |
//! | `<col>_local_deletion_time` | cell `deletion_info.local_delete_time` (tombstoned cell), else cell `expires_at`, else `liveness_info.expires_at` |
//! | `<col>_tombstone` | presence of the cell's `deletion_info` |
//! | `<col>_complex_deletion` | presence of the column's pathless complex-deletion marker |
//! | `<col>_complex_deletion_time` | that marker's `local_delete_time` |
//! | `<col>_complex_deletion_timestamp` | that marker's `marked_deleted` |
//! | `row_timestamp` | `liveness_info.tstamp` |
//! | `row_ttl` | `liveness_info.ttl` |
//! | `row_liveness_expires_at` | `liveness_info.expires_at` |
//! | `row_local_deletion_time` | row `deletion_info.local_delete_time` |
//! | `row_tombstone` | presence of the row's `deletion_info` |
//! | `row_deletion_timestamp` | row `deletion_info.marked_deleted` |
//! | `partition_deletion_time` | `partition.deletion_info.local_delete_time` |
//! | `partition_deletion_timestamp` | `partition.deletion_info.marked_deleted` |
//! | `bound_inclusive` | bound `type` (`inclusive`/`exclusive`) |
//! | `range_deletion_time` | bound `deletion_info.local_delete_time` |
//! | `range_deletion_timestamp` | bound `deletion_info.marked_deleted` |
//! | `row_kind` | golden entry type (row / static_block / partition deletion / bound side) — part of the row IDENTITY, so a wrong kind fails as a missing+unexplained row pair |
//!
//! DECLARED GAPS — facts this sweep deliberately does NOT compare, each with
//! its reason. None is a silent omission.
//!
//! * `sstable`, `generation`, `format` — source identity, not a
//!   timestamp/TTL/LDT/tombstone fact (AC2's scope). `sstable` IS used as the
//!   per-generation grouping key, so a row attributed to the wrong generation
//!   still fails as a missing+unexplained pair; `generation`/`format` are
//!   already value-asserted by the #4222 lanes.
//! * `position` — documented scope limitation of the view itself
//!   (`columns.rs`): real on the point-read path, always `NULL` on the
//!   full-scan path, so it has no single golden-comparable value across the
//!   two producers this sweep runs. Value-asserted against the golden's
//!   partition offset by `issue_4222_raw_view_point_read_test.rs` and
//!   `issue_4222_raw_view_bti_point_read_test.rs`.
//! * Base data-column VALUES (the golden's `value` fields) — not
//!   timestamp/TTL/LDT/tombstone metadata; value parity is the
//!   query-semantics oracle's job, not this lane's.
//! * Per-ELEMENT collection metadata (a golden cell with a `path`) — the
//!   view's contract gives a collection column only the `_complex_deletion`
//!   trio (design.md D7), so per-element write times and per-element
//!   tombstones (`collection_ops` pk=3's `tags['remove_me']`) have no column
//!   to land in. A view-level gap, not a harness one.
//! * `liveness_info.expired` / a cell's `expired` — sstabledump computes it
//!   from ITS OWN wall clock at dump time, so it is not an on-disk fact and
//!   asserting it would make this lane time-dependent (#2642).
//! * Static-vs-clustering row KIND — `CompactionRow` does not carry the
//!   decoder's `is_static` classification, so a golden `static_block` and a
//!   clustering row both render `row_kind = 'row'` (declared gap in
//!   `row_map.rs`, spec.md's "A static row is distinguishable from a
//!   clustering row" scenario, DEFERRED by #4222). This sweep therefore
//!   models a `static_block` as a `row_kind = 'row'` row with EVERY
//!   clustering component absent, and still compares all of its cell
//!   metadata byte-exact — it declines only to assert a distinction the
//!   view does not yet expose. Fixtures: `test_tomb.static_with_tombstones`,
//!   `test_tomb.dropped_static_col`, `test_deltas.static_with_rows`.
//!
//! # Coverage census — families AND shapes (issue #4309 AC5)
//!
//! A sweep whose golden lost the very shape its lane exists for would compare
//! cleanly and report green, because the golden and the expectation model
//! lose it together. So [`SweepOutcome::require_observed`] enforces TWO kinds
//! of token per case, each an affirmative zero: metadata FAMILIES
//! (`cell_ttl`, `row_tombstone`, `complex_deletion_time`, …) observed with a
//! non-`Absent` golden value, and golden ENTRY SHAPES — `entry:row`,
//! `entry:static_block`, `entry:partition_deletion`,
//! `entry:range_tombstone_bound`, `entry:range_tombstone_boundary`,
//! `shape:prefix_bound`, `shape:row_update_without_liveness`,
//! `shape:multi_generation`. [`SweepOutcome`] is `#[must_use]`, so a case
//! that makes no coverage claim at all is a compile error under the gate's
//! `-D warnings`.
//!
//! # Fixture discipline (#3220/#3121)
//!
//! Roots are resolved PER TABLE via
//! [`datasets_root::sstables_root_for_table`], never by keyspace and never by
//! a fixed env-first/checkout-first order. Each fixture case asserts on its
//! own: a [`Discipline::GitCommitted`] fixture is `must_run` and PANICS when
//! absent (a broken checkout, never an unfetched corpus); a
//! [`Discipline::FetchOnly`] fixture SKIPs cleanly, and
//! `CQLITE_REQUIRE_FIXTURES=1` (#972 strict mode) turns even that SKIP into a
//! panic. No lane ends in a suite-wide `assert!(ran > 0)`.
//!
//! ## WHAT THE GATE OF RECORD ACTUALLY CERTIFIES — read this before citing
//! ## "22/22 byte-exact" (roborev finding R1, issue #4309)
//!
//! **Sixteen of the 22 cases are `FetchOnly`, and the full gate's
//! `core-tests` component runs WITHOUT `CQLITE_REQUIRE_FIXTURES=1`** (the
//! gate exports that variable for `node-bindings` only; `core-tests`
//! deliberately does not, because most of `test_tomb/**` is fetched and
//! gitignored, so pinning it there would make the component depend on a
//! fetched corpus). On any box or CI lane lacking the fetched corpus those
//! sixteen therefore SKIP, `require_observed` no-ops (`ran == false`), and
//! the gate certifies only the SIX whose `Data.db` is git-committed:
//! `test_tomb.static_with_tombstones`, `test_deltas.static_with_rows`,
//! `test_da.wide_table`, `test_comp.lz4_table`,
//! `test_comp.uncompressed_table`, `test_compactionparity.live_clustering`.
//!
//! So a green gate is NOT by itself evidence that all 22 fixtures were
//! compared. To certify the whole sweep, run it against a fetched corpus
//! with strict mode on, and cite THAT:
//!
//! ```text
//! env CQLITE_DATASETS_ROOT=<fetched root> CQLITE_REQUIRE_FIXTURES=1 \
//!   cargo test --package cqlite-core --features cli-helpers \
//!   --test issue_4309_raw_view_parity_sweep_tomb_test \
//!   --test issue_4309_raw_view_parity_sweep_deltas_test \
//!   --test issue_4309_raw_view_parity_sweep_formats_test
//! ```
//!
//! This is a PRE-EXISTING property of every dataset-backed lane in this repo,
//! shared verbatim with all the `issue_4222_raw_view_*_test.rs` files — not
//! something this sweep introduced, and not something it can fix from inside
//! a test file. Wiring a `CQLITE_REQUIRE_FIXTURES=1` gate component for these
//! targets (precedent: the #3032 component, which does exactly that for its
//! two committed-fixture targets) is tracked as **issue #4311**. It is stated
//! here rather than left implied because a doc that reads as though
//! full-corpus certification happens automatically is the false assurance
//! this repo exists to avoid.
//!
//! # A note on duplication (roborev finding R3, issue #4309)
//!
//! `iso_to_micros`/`iso_to_secs`, `require_fixtures_strict`, the golden
//! JSONL line-parsing loop and the `IngestionConfig`/`schema_path` open are
//! near-duplicates of the private copies in
//! `cqlite-core/tests/issue_4222_raw_view_deltas_metadata_parity_test.rs`
//! (and, for the last two, of several other `issue_4222_raw_view_*` lanes).
//! They are NOT consolidated here on purpose: folding those lanes onto this
//! harness would rewrite files outside this issue's scope, and #3220 is the
//! standing warning about what happens when copies of one selection rule
//! drift apart. Flagged as a consolidation candidate so the duplication is a
//! recorded decision rather than an accident — the #4222 lanes are the ones
//! to migrate, and this module is the destination.

#![allow(dead_code)]

#[path = "datasets_root.rs"]
pub mod datasets_root;

/// The ORACLE half — golden loading, the column-contract classification and
/// the expectation model — split out under the campsite rule (#1135).
#[path = "raw_view_golden.rs"]
pub mod golden;

use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};
use golden::{
    build_expectations, classify_columns, fact_of, load_goldens, render_actual_clustering,
    ColumnRoles, ExpectedRow, Fact, RowIdentity,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Fixture description
// ---------------------------------------------------------------------------

/// Whether a fixture's `Data.db` is git-committed (`must_run`) or fetch-only
/// (skip-allowed). Recorded per fixture rather than probed at runtime: "is
/// this path git-tracked" is not observable from a test binary, and the
/// distinction decides between a hard failure and a clean SKIP.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Discipline {
    /// `git ls-files` carries this fixture's `Data.db` — its absence means a
    /// broken checkout, so the case PANICS rather than skipping.
    GitCommitted,
    /// Only the JSONL/`.txt`/`.crc32` sidecars are committed; the binaries
    /// come from `fetch-datasets.sh`. A clean SKIP when no candidate root
    /// carries them, a PANIC under `CQLITE_REQUIRE_FIXTURES=1`.
    FetchOnly,
}

/// One table of the sweep.
pub struct FixtureSpec {
    pub keyspace: &'static str,
    pub table: &'static str,
    /// Committed CQL schema under `test-data/schemas/`, resolved
    /// checkout-relative (#3148) — never derived from `CQLITE_DATASETS_ROOT`.
    pub schema_file: &'static str,
    /// The base table's partition-key columns, in contract order. Used to
    /// split the raw view's leading key columns into partition vs clustering
    /// (the rest of the classification is derived from the result metadata).
    pub partition_key_columns: &'static [&'static str],
    pub discipline: Discipline,
}

impl FixtureSpec {
    fn id(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

fn actual_text(row: &QueryRow, col: &str) -> Option<String> {
    match row.values.get(col) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn actual_identity(row: &QueryRow, roles: &ColumnRoles, ctx: &str) -> RowIdentity {
    let kind = actual_text(row, "row_kind")
        .unwrap_or_else(|| panic!("{ctx}: every raw-view row must carry a text 'row_kind'"));
    let clustering = roles
        .clustering_columns
        .iter()
        .map(|c| {
            row.values
                .get(c.as_str())
                .filter(|v| !matches!(v, Value::Null))
                .map(|v| render_actual_clustering(v, ctx))
        })
        .collect();
    (kind, clustering)
}

/// Compare ONE `(sstable, partition key)` group in BOTH directions, returning
/// the number of column comparisons it actually performed (roborev finding
/// S3, issue #4309: the reported total is MEASURED here, never re-derived
/// from `rows x columns` — a count nothing counted is the kind of figure this
/// repo's affirmative-zero rule exists to forbid).
fn assert_group(
    fixture: &str,
    source: &str,
    sstable: &str,
    key: &str,
    expected: &[&ExpectedRow],
    actual: &[&QueryRow],
    roles: &ColumnRoles,
) -> usize {
    let ctx = format!("{fixture} [{source}] {sstable} pk={key}");

    let mut expected_by_id: BTreeMap<RowIdentity, &ExpectedRow> = BTreeMap::new();
    for row in expected {
        let id = row.identity();
        assert!(
            expected_by_id.insert(id.clone(), row).is_none(),
            "{ctx}: the golden describes two physical rows with the SAME identity {id:?} — \
             the sweep's row model cannot distinguish them, so it must not silently compare \
             one twice"
        );
    }
    let mut actual_by_id: BTreeMap<RowIdentity, &QueryRow> = BTreeMap::new();
    for row in actual {
        let id = actual_identity(row, roles, &ctx);
        assert!(
            actual_by_id.insert(id.clone(), row).is_none(),
            "{ctx}: the raw view returned two rows with the SAME identity {id:?}"
        );
    }

    let expected_ids: BTreeSet<&RowIdentity> = expected_by_id.keys().collect();
    let actual_ids: BTreeSet<&RowIdentity> = actual_by_id.keys().collect();
    let missing: Vec<&&RowIdentity> = expected_ids.difference(&actual_ids).collect();
    let unexplained: Vec<&&RowIdentity> = actual_ids.difference(&expected_ids).collect();
    assert!(
        missing.is_empty(),
        "{ctx}: the raw view is MISSING physical rows the sstabledump golden states exist: \
         {missing:?} (golden rows: {}, raw-view rows: {})",
        expected.len(),
        actual.len()
    );
    assert!(
        unexplained.is_empty(),
        "{ctx}: the raw view returned rows the sstabledump golden does not explain: \
         {unexplained:?} (golden rows: {}, raw-view rows: {})",
        expected.len(),
        actual.len()
    );

    let mut comparisons = 0usize;
    for (id, expected_row) in &expected_by_id {
        let actual_row = actual_by_id[id];
        for column in &roles.compared_columns {
            let want = expected_row
                .facts
                .get(column.as_str())
                .cloned()
                .unwrap_or(Fact::Absent);
            let got = fact_of(actual_row.values.get(column.as_str()));
            assert_eq!(
                got, want,
                "{ctx}: column '{column}' on {id:?} must match the sstabledump golden \
                 byte-exact (golden entry: {})",
                expected_row.origin
            );
            comparisons += 1;
        }
    }
    comparisons
}

// ---------------------------------------------------------------------------
// Fixture resolution + database open
// ---------------------------------------------------------------------------

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Resolve the fixture's root PER TABLE, or decide the case's fate per its
/// declared discipline. `None` is the ONE sanctioned skip.
fn resolve_root(spec: &FixtureSpec) -> Option<PathBuf> {
    if let Some(root) = sstables_root_for_table(spec.keyspace, spec.table) {
        return Some(root);
    }
    match spec.discipline {
        Discipline::GitCommitted => panic!(
            "issue #4309: '{}' is a GIT-COMMITTED fixture, so its absence is a broken \
             checkout, not an unfetched corpus — this case must_run: {}",
            spec.id(),
            describe_search(spec.keyspace, spec.table)
        ),
        Discipline::FetchOnly => {
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 but '{}' was not found under any candidate \
                     root — fetch the corpus first \
                     (bash test-data/scripts/fetch-datasets.sh): {}",
                    spec.id(),
                    describe_search(spec.keyspace, spec.table)
                );
            }
            eprintln!(
                "SKIP: '{}' (fetch-only fixture) was not found under any candidate root — {}",
                spec.id(),
                describe_search(spec.keyspace, spec.table)
            );
            None
        }
    }
}

async fn open_database(spec: &FixtureSpec, root: &Path) -> Database {
    let schema = schema_path(spec.schema_file).unwrap_or_else(|| {
        panic!(
            "committed schema {} must be readable (#3148)",
            spec.schema_file
        )
    });
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        // TABLE-granular, never keyspace-granular: several of these keyspaces
        // hold nine tables and each case needs exactly one of them.
        table_directory_filter: Some(format!("/{}/{}-", spec.keyspace, spec.table)),
    };
    let result = ingest(cfg)
        .await
        .unwrap_or_else(|e| panic!("ingestion of {} must succeed: {e}", spec.id()));
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load for {}, else the raw view would refuse with \
         Error::Schema",
        spec.id()
    );
    result.database
}

/// Render a golden partition key as the CQL literal a point query needs.
///
/// Fail-closed on anything but an integer single-column key: every fixture in
/// the sweep has one, and silently quoting or interpolating an unvalidated
/// string would build a query this harness cannot reason about.
fn partition_key_predicate(spec: &FixtureSpec, key: &str) -> String {
    let components: Vec<&str> = key.split('|').collect();
    assert_eq!(
        components.len(),
        spec.partition_key_columns.len(),
        "issue #4309: {}'s key '{key}' must have one component per partition-key column",
        spec.id()
    );
    spec.partition_key_columns
        .iter()
        .zip(components)
        .map(|(column, value)| {
            let literal: i64 = value.parse().unwrap_or_else(|_| {
                panic!(
                    "issue #4309: this sweep only builds point predicates for INTEGER \
                     partition keys; '{}' column '{column}' rendered '{value}'. Extend \
                     partition_key_predicate before adding such a fixture.",
                    spec.id()
                )
            });
            format!("{column} = {literal}")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

// ---------------------------------------------------------------------------
// Affirmative-zero coverage census (issue #4309 AC5)
// ---------------------------------------------------------------------------

/// The metadata FAMILY a compared column belongs to.
///
/// Suffix matching is safe here and only here: the input is already known to
/// be a SYNTHESIZED contract column (`ColumnRoles::compared_columns` is built
/// from `simple_columns`/`complex_columns`, never from a name pattern), so a
/// real base column that merely LOOKS like one can never reach this function
/// — the misclassification #4222 round 9 fixed. The `_complex_deletion*` arms
/// must precede the generic `_timestamp`/`_time` arms.
fn fact_kind(column: &str) -> &'static str {
    match column {
        "row_timestamp" => "row_timestamp",
        "row_ttl" => "row_ttl",
        "row_liveness_expires_at" => "row_liveness_expires_at",
        "row_local_deletion_time" => "row_local_deletion_time",
        "row_tombstone" => "row_tombstone",
        "row_deletion_timestamp" => "row_deletion_timestamp",
        "partition_deletion_time" => "partition_deletion_time",
        "partition_deletion_timestamp" => "partition_deletion_timestamp",
        "bound_inclusive" => "bound_inclusive",
        "range_deletion_time" => "range_deletion_time",
        "range_deletion_timestamp" => "range_deletion_timestamp",
        c if c.ends_with("_complex_deletion_timestamp") => "complex_deletion_timestamp",
        c if c.ends_with("_complex_deletion_time") => "complex_deletion_time",
        c if c.ends_with("_complex_deletion") => "complex_deletion",
        c if c.ends_with("_local_deletion_time") => "cell_local_deletion_time",
        c if c.ends_with("_timestamp") => "cell_timestamp",
        c if c.ends_with("_ttl") => "cell_ttl",
        c if c.ends_with("_tombstone") => "cell_tombstone",
        other => panic!(
            "issue #4309: compared column '{other}' belongs to no known metadata family — \
             the coverage census must never silently drop a column it cannot classify"
        ),
    }
}

/// What ONE fixture's sweep actually measured.
///
/// Row counts alone cannot show a sweep is meaningful: a corpus where every
/// metadata column happens to be ABSENT would compare thousands of absences
/// and report green. So each case states the metadata FAMILIES and golden
/// ENTRY SHAPES its fixture is there to exercise, and
/// [`SweepOutcome::require_observed`] FAILs when a named one was never
/// observed — an affirmative zero, never a bare one.
///
/// `#[must_use]` is load-bearing, not decoration (roborev finding I1, issue
/// #4309): the census is the ONLY thing standing between this sweep and a
/// "compared thousands of absences, reported green" pass, and nothing else
/// obliges a caller to consult it. Dropping this value as a statement is
/// therefore a hard error under the gate's `-D warnings`, so a future lane
/// cannot quietly add a fixture case with no coverage claim at all.
#[must_use = "issue #4309: call `.require_observed(&[..])` on this outcome, naming the \
metadata families and golden entry shapes the fixture exists to exercise — the census is \
the only thing stopping a case from passing having compared nothing but absences"]
pub struct SweepOutcome {
    fixture: String,
    /// `false` ONLY when a fetch-only fixture was legitimately absent.
    pub ran: bool,
    /// MEASURED (roborev finding S3): incremented once per column actually
    /// compared, never re-derived as `rows x columns`.
    pub compared_facts: usize,
    observed: BTreeMap<&'static str, usize>,
}

impl SweepOutcome {
    fn skipped(spec: &FixtureSpec) -> Self {
        Self {
            fixture: spec.id(),
            ran: false,
            compared_facts: 0,
            observed: BTreeMap::new(),
        }
    }

    /// Assert this fixture really contributed every named coverage token:
    /// a metadata FAMILY (`cell_ttl`, `row_tombstone`, …) observed with a
    /// non-`Absent` golden value, or a golden ENTRY SHAPE (`entry:static_block`,
    /// `entry:range_tombstone_boundary`, `shape:prefix_bound`,
    /// `shape:row_update_without_liveness`, `shape:multi_generation`, …) the
    /// lane exists to exercise. A no-op for a legitimately skipped fetch-only
    /// fixture (there is nothing to have observed).
    pub fn require_observed(&self, kinds: &[&str]) {
        if !self.ran {
            return;
        }
        for kind in kinds {
            let count = self.observed.get(kind).copied().unwrap_or(0);
            assert!(
                count > 0,
                "issue #4309: {} was swept for '{kind}' but its golden states ZERO of \
                 them — the fixture no longer exercises the shape/family this case exists \
                 for, so the case proves nothing even though every comparison matched. \
                 {} column-comparisons ran; observed: {:?}",
                self.fixture,
                self.compared_facts,
                self.observed
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------

/// Assert the raw SSTable view of `spec`'s table reproduces its sstabledump
/// golden exactly, through the PUBLIC surface (`Database::execute` — the same
/// path `cqlite query` takes), over BOTH producers:
///
///   1. a bare `SELECT *` (the full-scan producer), whose partition-key set
///      must equal the golden's exactly — no golden key missing, no
///      unexplained key present; and
///   2. one `SELECT * ... WHERE <pk> = …` point query per golden partition key
///      (the point-read producer).
///
/// Both are compared with the SAME fact model, so a divergence between the two
/// producers fails too.
pub async fn assert_raw_view_matches_golden(spec: &FixtureSpec) -> SweepOutcome {
    let Some(root) = resolve_root(spec) else {
        return SweepOutcome::skipped(spec);
    };
    // Load the goldens BEFORE opening the database: `load_goldens` is where
    // the harness states its own limits (a missing sidecar, a golden with no
    // partitions, two generation directories sharing a `Data.db` name), and
    // those diagnostics are far more useful than whatever an ingestion of the
    // same corpus would say first.
    let goldens = load_goldens(&root, spec);
    let db = open_database(spec, &root).await;
    let view = format!("{}.{}_raw_sstable_data", spec.keyspace, spec.table);

    let scan = db
        .execute(&format!("SELECT * FROM {view}"))
        .await
        .unwrap_or_else(|e| panic!("full scan over {} must succeed: {e}", spec.id()));
    assert!(
        !scan.rows.is_empty(),
        "issue #4309: the full scan of {} returned no rows — a dataset-dependent assertion \
         must never pass on an empty measurement",
        spec.id()
    );

    let roles = classify_columns(&scan.metadata.columns, spec);
    let (expected, mut observed) = build_expectations(&goldens, &roles, spec);

    // The FAMILY half of the census is taken from the EXPECTATION model, and
    // the comparison below then proves every one of those facts was matched
    // byte-exact — so a family counted here is a family really compared. The
    // SHAPE half arrives from `build_expectations` in the same map.
    for row in &expected {
        for (column, fact) in &row.facts {
            // `Fact::Bool(false)` on `<col>_complex_deletion` is the ABSENCE
            // of a complex-deletion marker (roborev finding S1, issue #4309):
            // counting it would let a fixture with no marker at all "observe"
            // the family. Every other boolean — `bound_inclusive = false`, an
            // EXCLUSIVE bound — is a genuine positive observation.
            let is_negative_marker =
                *fact == Fact::Bool(false) && fact_kind(column) == "complex_deletion";
            if *fact != Fact::Absent && !is_negative_marker {
                *observed.entry(fact_kind(column)).or_insert(0) += 1;
            }
        }
    }

    // Group the expectation model by (sstable, partition key).
    let mut expected_groups: BTreeMap<(String, String), Vec<&ExpectedRow>> = BTreeMap::new();
    for row in &expected {
        expected_groups
            .entry((row.sstable.clone(), row.key.clone()))
            .or_default()
            .push(row);
    }
    let golden_keys: BTreeSet<String> = expected.iter().map(|r| r.key.clone()).collect();
    let golden_sstables: BTreeSet<String> = goldens.iter().map(|g| g.data_db.clone()).collect();

    // --- Producer 1: the full scan -----------------------------------------
    let mut compared_facts = assert_rows_match(
        spec,
        "full scan",
        &scan.rows.iter().collect::<Vec<_>>(),
        &expected_groups,
        &golden_sstables,
        &roles,
    );
    let scanned_keys: BTreeSet<String> = scan
        .rows
        .iter()
        .map(|r| actual_partition_key(r, spec))
        .collect();
    assert_eq!(
        scanned_keys,
        golden_keys,
        "issue #4309: {}'s raw view must expose EXACTLY the partition keys the sstabledump \
         golden states — no golden key missing, no unexplained key present",
        spec.id()
    );

    // --- Producer 2: one point query per golden key ------------------------
    for key in &golden_keys {
        let predicate = partition_key_predicate(spec, key);
        let result = db
            .execute(&format!("SELECT * FROM {view} WHERE {predicate}"))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "point-key query over {} ({predicate}) must succeed: {e}",
                    spec.id()
                )
            });
        let rows: Vec<&QueryRow> = result.rows.iter().collect();
        assert!(
            !rows.is_empty(),
            "issue #4309: {} pk={key} is in the golden but the raw view's point read \
             returned nothing",
            spec.id()
        );
        let mut scoped: BTreeMap<(String, String), Vec<&ExpectedRow>> = BTreeMap::new();
        for ((sstable, k), v) in &expected_groups {
            if k == key {
                scoped.insert((sstable.clone(), k.clone()), v.clone());
            }
        }
        compared_facts += assert_rows_match(
            spec,
            "point-key query",
            &rows,
            &scoped,
            &golden_sstables,
            &roles,
        );
    }

    SweepOutcome {
        fixture: spec.id(),
        ran: true,
        compared_facts,
        observed,
    }
}

fn actual_partition_key(row: &QueryRow, spec: &FixtureSpec) -> String {
    spec.partition_key_columns
        .iter()
        .map(|c| {
            let value = row.values.get(*c).unwrap_or_else(|| {
                panic!(
                    "issue #4309: every {} raw-view row must carry its partition-key column \
                     '{c}'",
                    spec.id()
                )
            });
            render_actual_clustering(value, &spec.id())
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Split an actual row set into `(sstable, key)` groups and compare each
/// against its expectation group, in both directions. Returns the number of
/// column comparisons actually performed.
fn assert_rows_match(
    spec: &FixtureSpec,
    source: &str,
    rows: &[&QueryRow],
    expected_groups: &BTreeMap<(String, String), Vec<&ExpectedRow>>,
    golden_sstables: &BTreeSet<String>,
    roles: &ColumnRoles,
) -> usize {
    let fixture = spec.id();
    let mut actual_groups: BTreeMap<(String, String), Vec<&QueryRow>> = BTreeMap::new();
    for row in rows {
        let sstable = actual_text(row, "sstable").unwrap_or_else(|| {
            panic!("{fixture} [{source}]: every raw-view row must carry a text 'sstable'")
        });
        assert!(
            golden_sstables.contains(&sstable),
            "{fixture} [{source}]: the raw view attributed a row to '{sstable}', which has \
             no sstabledump golden — the sweep cannot certify a generation it has no oracle \
             for"
        );
        let key = actual_partition_key(row, spec);
        actual_groups.entry((sstable, key)).or_default().push(row);
    }

    let expected_ids: BTreeSet<&(String, String)> = expected_groups.keys().collect();
    let actual_ids: BTreeSet<&(String, String)> = actual_groups.keys().collect();
    let mut comparisons = 0usize;
    for id in expected_ids.union(&actual_ids) {
        let empty_expected: Vec<&ExpectedRow> = Vec::new();
        let empty_actual: Vec<&QueryRow> = Vec::new();
        let expected = expected_groups.get(*id).unwrap_or(&empty_expected);
        let actual = actual_groups.get(*id).unwrap_or(&empty_actual);
        comparisons += assert_group(&fixture, source, &id.0, &id.1, expected, actual, roles);
    }
    comparisons
}
