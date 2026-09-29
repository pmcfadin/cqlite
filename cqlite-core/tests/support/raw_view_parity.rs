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
//! ATTRIBUTION, so the next reader does not debug the wrong side: if that
//! assertion ever DOES fail on an inherited `<col>_ttl` /
//! `<col>_local_deletion_time`, it is a HARNESS-ASSUMPTION failure FIRST and a
//! view failure only second. CQLite inherits a row TTL only when the cell's
//! on-disk `USE_ROW_TTL` (0x10) flag is set (`row_data.rs`, #1743), whereas
//! this harness inherits whenever the golden omits the field — so a TTL'd
//! INSERT later partially overwritten by a plain `UPDATE` would make the
//! harness demand a fact the view correctly reports absent. Fix the rule in
//! [`golden::fold_simple_cell`] before touching the view (roborev, #4309).
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
//! * Physical row ORDER (roborev, issue #4309). [`assert_group`] reduces both
//!   sides to a `BTreeMap` keyed by `(row_kind, clustering)`, so the golden's
//!   on-disk entry SEQUENCE is discarded before any comparison: this sweep
//!   compares the row SET and every row's metadata, never the order they
//!   appear in. sstabledump's JSONL *is* an ordered physical dump, so this is
//!   a real limit and it is stated here rather than left implied — a
//!   clustering-order regression that returns the right rows in the wrong
//!   order passes this sweep green.
//!
//!   A BLANKET order assertion is NOT achievable, which is why the gap is
//!   declared rather than closed: for range markers CQLite and sstabledump
//!   order differently BY CONSTRUCTION.
//!   `row_decoder/compaction.rs::on_range_marker` buffers an open bound in
//!   `pending_range_start` and emits the paired `RangeMarker` only when the
//!   matching END bound closes it, so CQLite emits the start row AFTER the
//!   intervening `row` entries, whereas sstabledump emits the open bound in
//!   its own physical position. Asserting order for the `row` /
//!   `static_block` / `partition_deletion` kinds within each
//!   `(sstable, key)` group IS achievable and is the stronger follow-up
//!   (#4314); it is not done here because it is a behaviour change to the
//!   comparison, not a confirmation-round fix.
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
//! **Every token must witness the property it NAMES, not a proxy for it**
//! (roborev job 42, issue #4309). A token derived from something weaker than
//! its own name reintroduces the census's own blind spot inside the census:
//! `shape:multi_generation` once counted `goldens.len() > 1` — "this fixture
//! has more than one SSTable" — while the five lanes claiming it exist to
//! show ONE PARTITION KEY yielding one unreconciled row per generation, so a
//! regeneration with DISJOINT keys per generation kept the token positive
//! with the shape gone. It is now derived from the expectation model (count
//! of keys appearing under two or more distinct `sstable` values), and
//! `issue_4309_raw_view_census_selftest.rs` is the census's own oracle:
//! synthetic goldens of known shape, opening no database and reading no
//! corpus, so every case in it is `must_run` on EVERY gate.
//!
//! WHICH TOKENS THAT LANE ACTUALLY CONTROLS, stated exactly rather than
//! blanket (roborev jobs 46 and 50, issue #4309 — first an overclaim that
//! every token had a control, then a wrong claim about which ones the gate
//! reaches). SIX of the eight shape tokens have a positive control AND a
//! near-miss negative control there: `shape:multi_generation`,
//! `entry:partition_deletion`, `entry:range_tombstone_bound`,
//! `entry:range_tombstone_boundary`, `shape:prefix_bound`,
//! `shape:row_update_without_liveness`.
//!
//! FIVE of those six are covered there because they have NO gate-executed
//! claimant at all — `entry:partition_deletion`,
//! `entry:range_tombstone_boundary`, `shape:prefix_bound`,
//! `shape:row_update_without_liveness` and `shape:multi_generation` are
//! claimed only by `FetchOnly` lanes, which SKIP under the corpus-less
//! `core-tests`, so their derivations would otherwise be unexercised on the
//! gate of record. (`shape:multi_generation`'s five claimants —
//! `skipped_partition_delete`, `resurrection_gc0`,
//! `resurrection_gc_positive`, `dropped_regular_col`, `dropped_static_col`
//! — are every one of them `FetchOnly`; calling it defence-in-depth was the
//! job-50 error inverted, and roborev job 52 caught it.)
//!
//! The SIXTH, `entry:range_tombstone_bound`, is the only DEFENCE-IN-DEPTH
//! control: it IS gate-executed, because `static_with_tombstones` is
//! `GitCommitted` and claims it, and its committed golden really carries two
//! `range_tombstone_bound` entries (one `start`-only, one `end`-only).
//!
//! The remaining two, `entry:row` and `entry:static_block`, need no
//! synthetic control: both are claimed by `GitCommitted` lanes that run on
//! every gate (`entry:row` additionally has a vacuity guard in the
//! self-test). Extend that lane when you add a token, and say here which
//! case controls it — and check the claimant's DISCIPLINE before writing
//! "no gate-executed fixture reaches this".
//!
//! One further token, `shape:point_path_resolved`, records how many
//! point-read rows resolved a non-NULL `position` — the PATH WITNESS that
//! proves the point-read producer ran rather than being served from the
//! full-scan path. It is recorded for visibility but is NOT claimable by a
//! lane — `require_observed` rejects it by name (`UNCLAIMABLE_TOKENS`),
//! because under `feature = "tombstones"` the witness is necessarily ZERO
//! and a claim would be unsatisfiable in that build while passing the
//! default one. Unlike the tokens above it does not depend on a lane
//! claiming it: the
//! witness is asserted directly in `assert_raw_view_matches_golden`, per
//! build, so it cannot be left unenforced by omission (roborev, #4309).
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
//! ## "27/27 byte-exact" (roborev finding R1, issue #4309)
//!
//! The sweep is **27 fixture cases** — 9 `tomb` + 9 `deltas` + 9 `formats`.
//! **Sixteen of them are `FetchOnly`, and the full gate's `core-tests`
//! component runs WITHOUT `CQLITE_REQUIRE_FIXTURES=1`** (SEVERAL targeted
//! components DO export it — `node-bindings`, the #3032 committed-reference
//! lane, `compaction-byte-parity`, the compaction-tombstone-TTL lane, and
//! the strict branch — but `core-tests`, which is what runs these lanes,
//! deliberately does not, because most of `test_tomb/**` is fetched and
//! gitignored, so pinning it there would make the component depend on a
//! fetched corpus). On any box
//! or CI lane lacking the fetched corpus those sixteen therefore SKIP,
//! `require_observed` no-ops (`ran == false`), and the gate certifies the
//! **ELEVEN** whose `Data.db` is git-committed:
//! `test_tomb.static_with_tombstones`, `test_deltas.static_with_rows`,
//! `test_da.wide_table`, `test_compactionparity.live_clustering`, and the
//! seven `test_comp` tables — `lz4_table`, `snappy_table`, `deflate_table`,
//! `zstd_table`, `short_final_chunk`,
//! `incompressible_uncompressed_chunk`, `uncompressed_table`.
//!
//! KEEP THESE THREE NUMBERS IN STEP (roborev job 46, issue #4309): the
//! counts went stale the moment the formats lane grew 4 -> 9 cases, and an
//! UNDERCOUNT of gate-covered cases is false assurance in reverse — this is
//! the one section whose entire purpose is stating precisely what a green
//! gate certifies. Total = 27, `FetchOnly` = 16, `GitCommitted` = 11, and
//! the enumerated list above must name all eleven. The formats lane's own
//! doc repeats the denominator and must be updated with it.
//!
//! So a green gate is NOT by itself evidence that all 27 fixtures were
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
use cqlite_core::{
    ingestion::ingest_with_selection, ingestion::IngestionConfig, ingestion::TableDirSelection,
    Config, Database,
};
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

async fn open_database(spec: &FixtureSpec, root: &Path, generation_dir: &Path) -> Database {
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
        // NOT USED. Exactly one directory is required here, and a
        // SUBSTRING FILTER CANNOT EXPRESS THAT (roborev job 62).
        // `IngestionConfig::table_directory_filter`'s own doc says so for
        // exactly this shape: a filter of `/<ks>/<table>-<uuid>` also
        // matches a SIBLING whose name EXTENDS it
        // (`<table>-<uuid>-backup`), which silently adds generations to the
        // ingest. An earlier attempt here argued the `<uuid>` made the
        // substring safe; that addresses uuid-vs-uuid collision, not name
        // EXTENSION, which is the documented counterexample (#3234). The
        // selection is made with `TableDirSelection::Exact` below, which
        // compares canonicalized complete path components.
        table_directory_filter: None,
    };
    let only = [generation_dir.to_path_buf()];
    let result = ingest_with_selection(cfg, TableDirSelection::Exact(&only))
        .await
        .unwrap_or_else(|e| panic!("ingestion of {} must succeed: {e}", spec.id()));
    // FAIL CLOSED on the selection not being exactly the oracle's directory.
    // The whole point of the pin is that oracle and query read the SAME
    // bytes; asserting it costs nothing and turns a silent extra generation
    // into a named failure instead of a misleading "the raw view attributed
    // a row to '<sstable>', which has no sstabledump golden".
    assert_eq!(
        result.discovery_summary.table_directories.len(),
        1,
        "issue #4309: {} must ingest EXACTLY the generation directory its oracle read \
         ({}), got {:?}",
        spec.id(),
        generation_dir.display(),
        result.discovery_summary.table_directories
    );
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
/// How a [`FACT_KIND_RULES`] entry matches a column name.
#[derive(Clone, Copy)]
pub enum FactKindMatch {
    /// The whole column name, for the row/partition/range metadata columns
    /// that are not per-base-column.
    Exact(&'static str),
    /// A synthesized suffix on a base column name.
    Suffix(&'static str),
}

/// The metadata-family rules, IN PRIORITY ORDER — this table IS
/// [`fact_kind`]'s arm order, not a copy of it.
///
/// ORDER IS LOAD-BEARING. The `_complex_deletion*` rules MUST precede the
/// generic `_timestamp` / `_local_deletion_time` rules: a column named
/// `tags_complex_deletion_timestamp` ends with `_timestamp` too, and a
/// reorder would classify it as `cell_timestamp`, silently retiring the
/// complex family. `complex_deletion_timestamp` must likewise precede
/// `complex_deletion_time`, which must precede `complex_deletion`, since
/// each is a suffix of the previous one's column names. Pinned by
/// `issue_4309_raw_view_census_selftest.rs`.
///
/// DRIVEN FROM A TABLE RATHER THAN A `match` so the rule set is
/// ENUMERABLE (roborev job 59). The self-test asserts that the families
/// this table can produce, together with the shapes `build_expectations`
/// bumps, are EXACTLY [`KNOWN_COVERAGE_TOKENS`]. With a `match` that claim
/// was unenforceable: a new arm changed neither side of the comparison, so
/// a new family could become silently unclaimable — `require_observed`
/// would reject every lane's claim for it as an unknown name while the
/// census happily counted it. Adding a rule here now FAILs the self-test
/// until the vocabulary is updated with it.
pub const FACT_KIND_RULES: &[(FactKindMatch, &str)] = &[
    (FactKindMatch::Exact("row_timestamp"), "row_timestamp"),
    (FactKindMatch::Exact("row_ttl"), "row_ttl"),
    (
        FactKindMatch::Exact("row_liveness_expires_at"),
        "row_liveness_expires_at",
    ),
    (
        FactKindMatch::Exact("row_local_deletion_time"),
        "row_local_deletion_time",
    ),
    (FactKindMatch::Exact("row_tombstone"), "row_tombstone"),
    (
        FactKindMatch::Exact("row_deletion_timestamp"),
        "row_deletion_timestamp",
    ),
    (
        FactKindMatch::Exact("partition_deletion_time"),
        "partition_deletion_time",
    ),
    (
        FactKindMatch::Exact("partition_deletion_timestamp"),
        "partition_deletion_timestamp",
    ),
    (FactKindMatch::Exact("bound_inclusive"), "bound_inclusive"),
    (
        FactKindMatch::Exact("range_deletion_time"),
        "range_deletion_time",
    ),
    (
        FactKindMatch::Exact("range_deletion_timestamp"),
        "range_deletion_timestamp",
    ),
    // --- the complex arms, which MUST stay above the generic ones ---
    (
        FactKindMatch::Suffix("_complex_deletion_timestamp"),
        "complex_deletion_timestamp",
    ),
    (
        FactKindMatch::Suffix("_complex_deletion_time"),
        "complex_deletion_time",
    ),
    (
        FactKindMatch::Suffix("_complex_deletion"),
        "complex_deletion",
    ),
    // --- the generic per-cell arms ---
    (
        FactKindMatch::Suffix("_local_deletion_time"),
        "cell_local_deletion_time",
    ),
    (FactKindMatch::Suffix("_timestamp"), "cell_timestamp"),
    (FactKindMatch::Suffix("_ttl"), "cell_ttl"),
    (FactKindMatch::Suffix("_tombstone"), "cell_tombstone"),
];

pub fn fact_kind(column: &str) -> &'static str {
    for (rule, kind) in FACT_KIND_RULES {
        let hit = match rule {
            FactKindMatch::Exact(name) => column == *name,
            FactKindMatch::Suffix(suffix) => column.ends_with(suffix),
        };
        if hit {
            return kind;
        }
    }
    panic!(
        "issue #4309: compared column '{column}' belongs to no known metadata family — \
         the coverage census must never silently drop a column it cannot classify"
    )
}

/// Every coverage token a case may legitimately claim.
///
/// The METADATA-FAMILY half is exactly [`fact_kind`]'s return set; the SHAPE
/// half is exactly what `build_expectations` can `bump`, plus the
/// `shape:point_path_resolved` path witness, which lives in
/// [`UNCLAIMABLE_TOKENS`] instead. Keep all of them in step — a
/// token added to one and not here is refused by name, which is the
/// intended failure.
pub const KNOWN_COVERAGE_TOKENS: &[&str] = &[
    // metadata families (fact_kind)
    "row_timestamp",
    "row_ttl",
    "row_liveness_expires_at",
    "row_local_deletion_time",
    "row_tombstone",
    "row_deletion_timestamp",
    "partition_deletion_time",
    "partition_deletion_timestamp",
    "bound_inclusive",
    "range_deletion_time",
    "range_deletion_timestamp",
    "complex_deletion_timestamp",
    "complex_deletion_time",
    "complex_deletion",
    "cell_local_deletion_time",
    "cell_timestamp",
    "cell_ttl",
    "cell_tombstone",
    // golden entry shapes
    "entry:row",
    "entry:static_block",
    "entry:partition_deletion",
    "entry:range_tombstone_bound",
    "entry:range_tombstone_boundary",
    "shape:prefix_bound",
    "shape:row_update_without_liveness",
    "shape:multi_generation",
    // `shape:point_path_resolved` is DELIBERATELY ABSENT — see
    // `UNCLAIMABLE_TOKENS`.
];

/// Tokens the census RECORDS but which a lane must never CLAIM.
///
/// `shape:point_path_resolved` counts point-read rows that resolved a
/// non-NULL `position`. It is asserted directly in
/// `assert_raw_view_matches_golden`, per build, so it needs no claim path —
/// and claiming it would be actively WRONG (roborev job 52): under
/// `feature = "tombstones"`, `point.rs`'s cfg'd `point_rows_for_key` routes
/// every reader through `scan_and_filter_one_reader`, so the witness is
/// necessarily ZERO and `require_observed`'s `count > 0` is UNSATISFIABLE.
/// A lane claiming it would pass the default build and fail an
/// all-features one. Rejected by name so that contradiction cannot be
/// written rather than merely discouraged in prose.
pub const UNCLAIMABLE_TOKENS: &[&str] = &[SHAPE_POINT_PATH_RESOLVED];

/// Named once so the vocabulary entry and the census insert site cannot
/// drift apart (roborev job 67) — the same reason
/// `golden::SHAPE_MULTI_GENERATION` exists. This is the one shape token
/// `bump` does not mechanize, because it is recorded by the sweep itself
/// rather than derived from a golden entry.
pub const SHAPE_POINT_PATH_RESOLVED: &str = "shape:point_path_resolved";

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
    /// Build an outcome directly, so the census self-test can drive
    /// [`Self::require_observed`]'s guards (roborev job 54). Those guards
    /// never fire on any real run — no lane claims a bad token — so
    /// inverting one, or moving it back below the `ran` early return, would
    /// leave every gate green while re-opening the vacuous-claim hole it was
    /// added to close. Controls need a way to construct the input.
    pub fn for_control(fixture: &str, ran: bool, observed: &[(&'static str, usize)]) -> Self {
        Self {
            fixture: fixture.to_string(),
            ran,
            compared_facts: 0,
            observed: observed.iter().copied().collect(),
        }
    }

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
    /// lane exists to exercise.
    ///
    /// The CLAIM ITSELF is always validated — non-empty, every token a known
    /// name, none of them [`UNCLAIMABLE_TOKENS`] — whether or not the
    /// fixture ran. Only the per-token OBSERVATION COUNTS are skipped for a
    /// legitimately skipped fetch-only fixture, which genuinely has nothing
    /// to have observed. That split is the job-47/52 fix: 16 of the 27 cases
    /// skip under the gate's corpus-less `core-tests`, so a claim validated
    /// only on the `ran` path is a claim never validated on the gate.
    pub fn require_observed(&self, kinds: &[&str]) {
        // An EMPTY claim is not a claim (roborev, issue #4309). `#[must_use]`
        // forces a caller to CALL this, but nothing forced the claim to say
        // anything: `require_observed(&[])` satisfied the lint, ran the loop
        // below zero times, and let a case compare thousands of absences and
        // report green — the exact hole the census exists to close. Asserted
        // BEFORE the `ran` early return, so an empty claim fails even on a
        // fixture that skipped.
        assert!(
            !kinds.is_empty(),
            "issue #4309: {} called require_observed(&[]) — an empty coverage claim. \
             Name the metadata families and golden entry shapes this case exists to \
             exercise; a case that claims nothing certifies nothing.",
            self.fixture
        );
        // TOKEN NAMES ARE VALIDATED BEFORE THE `ran` EARLY RETURN, for the
        // same reason the emptiness check above is (roborev job 47, issue
        // #4309). `require_observed` otherwise validates a name only
        // IMPLICITLY, by looking it up in `observed` — so on a fixture that
        // skipped, a MISTYPED token (`"cell_tombstonee"`,
        // `"entry:static_blocks"`, `"shape:prefix_bounds"`) is silently
        // accepted. Sixteen of the sweep's 27 cases are `FetchOnly` and skip
        // under the gate's corpus-less `core-tests`, so a typo in the
        // `tomb`/`deltas` lanes would be accepted FOREVER on the gate of
        // record and surface only on a strict-mode run against a fetched
        // corpus — a coverage claim that silently claims nothing, which is
        // this census's own failure mode. Checked here, a typo fails on
        // EVERY gate whether or not the fixture ran.
        for kind in kinds {
            assert!(
                !UNCLAIMABLE_TOKENS.contains(kind),
                "issue #4309: {} claims coverage token '{kind}', which the census RECORDS \
                 but no lane may CLAIM. It is asserted directly in \
                 assert_raw_view_matches_golden, per build, so it needs no claim — and \
                 under `feature = \"tombstones\"` it is necessarily ZERO, so the claim \
                 would be unsatisfiable in that build while passing the default one.",
                self.fixture
            );
            assert!(
                KNOWN_COVERAGE_TOKENS.contains(kind),
                "issue #4309: {} claims coverage token '{kind}', which is not a known \
                 metadata family or golden entry shape. A mistyped token can never be \
                 observed, so it would make the claim vacuous rather than failing. Known \
                 tokens: {KNOWN_COVERAGE_TOKENS:?}",
                self.fixture
            );
        }
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
    // The oracle picked the generation directory; the query must read that
    // same one (roborev job 61).
    let generation_dir = goldens[0].source_dir.clone();
    let db = open_database(spec, &root, &generation_dir).await;
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
    // The SCAN's RESULT-METADATA column set. Retained as a cheap structural
    // check, but read the next paragraph before citing it as the #3890
    // assertion — IT IS NOT, AND CANNOT FAIL FOR `SELECT *` (roborev job 47,
    // issue #4309).
    //
    // Authority is the production source, not this harness:
    // `raw_view/mod.rs:194` computes `let (columns, metadata_names) =
    // raw_view_columns(&base_schema)?` ONCE, BEFORE the
    // `PartitionLookupOutcome` branch that chooses the point vs full-scan
    // producer, and the `SelectClause::All` arm returns it UNCHANGED
    // (`mod.rs:366`). Both queries here are `SELECT *`, so
    // `result.metadata.columns` is byte-identical on the two paths BY
    // CONSTRUCTION — it is never derived from what the producer actually put
    // in a row. Comparing it therefore asserts a tautology.
    //
    // The hole it was supposed to close lives in `QueryRow::values`: for a
    // column whose golden value is `Absent` across the whole fixture,
    // `fact_of(None)` and `fact_of(Some(Null))` are both `Fact::Absent`, so
    // a column DROPPED from the point row's values compares clean. That is
    // asserted below, per row, against the matching scan row — see
    // `assert_value_key_sets_match`.
    let scan_columns: BTreeSet<String> = scan
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
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
            // The oracle NEVER stores an explicit absence: `row_entry_facts`,
            // `bound_facts`, `fold_simple_cell` and `fold_complex_column`
            // only ever insert a fact that has a value, and a missing fact is
            // simply not in the map — `Fact::Absent` is synthesized later, by
            // `assert_group`'s `unwrap_or`. A `*fact != Fact::Absent` filter
            // here could therefore never be false (roborev job 65), so the
            // invariant is ASSERTED once instead of being filtered on
            // forever: a future `insert(.., Fact::Absent)` would make the
            // census count an absence as an observation, which is the
            // vacuous-coverage failure this census exists to forbid.
            assert_ne!(
                *fact,
                Fact::Absent,
                "issue #4309: {} stored an explicit Fact::Absent for '{column}'. The \
                 expectation model states only facts it HAS; absence is represented by \
                 the column being missing. Counting an explicit absence would let a \
                 fixture observe a family it never exercised",
                spec.id()
            );
            if !is_negative_complex_marker(column, fact) {
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

    // --- PATH WITNESS, scan side (roborev, issue #4309) ---------------------
    //
    // The sweep's headline claim is that it compares BOTH producers, and the
    // formats lane's whole BTI rationale rests on `test_da.wide_table`
    // exercising trie descent through `point.rs`. Nothing in the fact model
    // could back that: `position` is the ONLY contract column whose value
    // differs by access path, and it is a DECLARED GAP, so a point query that
    // silently routed to the full-scan producer — or a BTI point resolution
    // that fell back — would leave all 27 cases passing byte-exact while the
    // second producer certified NOTHING.
    //
    // Authority for the asymmetry is the production source, not this harness:
    // `raw_view/columns.rs` records that `scan.rs`'s full-scan producer NEVER
    // consults an index per row and always reports `Value::Null`, while
    // `point.rs::resolve_position` resolves a real byte offset via a second
    // index lookup. So `position` is the only available discriminator, and it
    // is used here as a PATH WITNESS only — never as a golden comparison,
    // which is why it stays a declared gap in the column contract.
    for row in &scan.rows {
        let position = row.values.get("position");
        assert!(
            matches!(position, None | Some(Value::Null)),
            "issue #4309: {} — the full-scan producer must report `position` as NULL for \
             every row (`scan.rs` consults no per-row index; `columns.rs` documents that \
             NULL as an honest 'not measured on this access path'). Got {position:?}, so \
             this row did NOT come from the scan path and the two producers are no longer \
             distinguishable",
            spec.id()
        );
    }

    // --- The scan's PER-ROW value-column sets, for the #3890 comparison ----
    //
    // Keyed by the full physical identity `(sstable, partition key, row_kind,
    // clustering)`, so each point row is held against the SAME physical row
    // the scan produced rather than against an aggregate.
    let mut scan_value_keys: BTreeMap<(String, String, RowIdentity), BTreeSet<String>> =
        BTreeMap::new();
    for row in &scan.rows {
        let ctx = format!("{} [full scan]", spec.id());
        let sstable = actual_text(row, "sstable")
            .unwrap_or_else(|| panic!("{ctx}: every raw-view row must carry a text 'sstable'"));
        let pk = actual_partition_key(row, spec);
        let identity = actual_identity(row, &roles, &ctx);
        scan_value_keys.insert((sstable, pk, identity), value_key_set(row));
    }

    // --- Producer 2: one point query per golden key ------------------------
    let mut point_positions_resolved = 0usize;
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
        // BOTH DIRECTIONS (#3890): set equality, so a column dropped from the
        // point projection AND one that appears only there both fail by name.
        let point_columns: BTreeSet<String> = result
            .metadata
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(
            point_columns,
            scan_columns,
            "issue #4309: {} pk={key} — the point-read producer must expose the SAME \
             column contract as the full scan. Missing from the point read: {:?}; \
             present ONLY in the point read: {:?}",
            spec.id(),
            scan_columns.difference(&point_columns).collect::<Vec<_>>(),
            point_columns.difference(&scan_columns).collect::<Vec<_>>(),
        );
        let rows: Vec<&QueryRow> = result.rows.iter().collect();
        assert!(
            !rows.is_empty(),
            "issue #4309: {} pk={key} is in the golden but the raw view's point read \
             returned nothing",
            spec.id()
        );
        // THE #3890 COMPARISON, per row and in BOTH directions. Held against
        // the matching SCAN row by full physical identity, so a point row
        // that silently dropped a metadata column fails by NAME even where
        // the fact model would compare it clean as an absence.
        for row in &rows {
            let ctx = format!("{} [point read] pk={key}", spec.id());
            let sstable = actual_text(row, "sstable")
                .unwrap_or_else(|| panic!("{ctx}: every raw-view row must carry a text 'sstable'"));
            let pk = actual_partition_key(row, spec);
            let identity = actual_identity(row, &roles, &ctx);
            let scan_keys = scan_value_keys
                .get(&(sstable.clone(), pk.clone(), identity.clone()))
                .unwrap_or_else(|| {
                    panic!(
                        "issue #4309: {} — the point read produced a physical row \
                         (sstable={sstable}, pk={pk}, {identity:?}) that the FULL SCAN \
                         never produced. The two producers must expose the same physical \
                         rows; a point-only row means one of them is fabricating or \
                         dropping data",
                        spec.id()
                    )
                });
            assert_value_key_sets_match(&spec.id(), key, &identity, &value_key_set(row), scan_keys);
        }

        let mut scoped: BTreeMap<(String, String), Vec<&ExpectedRow>> = BTreeMap::new();
        for ((sstable, k), v) in &expected_groups {
            if k == key {
                scoped.insert((sstable.clone(), k.clone()), v.clone());
            }
        }
        // PATH WITNESS, point side. Counted across the WHOLE fixture, never
        // asserted per row: `resolve_position` is documented best-effort — a
        // lookup miss or error also yields `Null` — so "every row resolves" is
        // not a property the view promises. "At least one resolved somewhere in
        // this fixture" is, and it is enough to prove the point path ran.
        for row in &rows {
            if !matches!(row.values.get("position"), None | Some(Value::Null)) {
                point_positions_resolved += 1;
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

    // The witness is FEATURE-DEPENDENT, and both builds assert something
    // (roborev, issue #4309). `point.rs` compiles TWO `point_rows_for_key`
    // implementations: the default one resolves a real offset, but the
    // `#[cfg(feature = "tombstones")]` one routes every reader through
    // `scan_and_filter_one_reader`, which builds
    // `RawViewSource::from_reader(reader, None)` — so `position` is NULL on
    // every point row in that build and "at least one resolved" is
    // UNSATISFIABLE there.
    //
    // Deliberately NOT `#[cfg(not(feature = "tombstones"))]` on the whole
    // witness: skipping it under `tombstones` would make an unmeasured build
    // read exactly like a measured one, which is the failure mode this sweep
    // exists to prevent. Instead each build asserts the behaviour ITS
    // producer actually has, so a change to either one fails here by name.
    if cfg!(feature = "tombstones") {
        assert_eq!(
            point_positions_resolved,
            0,
            "issue #4309: {} — under `feature = \"tombstones\"`, `point.rs`'s cfg'd \
             `point_rows_for_key` routes every reader through \
             `scan_and_filter_one_reader` (`from_reader(reader, None)`), so EVERY \
             point-read `position` must be NULL. {} resolved — that producer now \
             resolves offsets, so this witness (and the default-build branch below) \
             needs revisiting",
            spec.id(),
            point_positions_resolved
        );
    } else {
        // A fixture where NOTHING resolved is indistinguishable from one whose
        // point queries all served from the scan path — the hole this witness
        // exists to close.
        assert!(
            point_positions_resolved > 0,
            "issue #4309: {} — not one point-read row across the whole fixture resolved \
             a non-NULL `position`, so nothing here proves the point-read producer was \
             exercised at all rather than served from the full-scan path. \
             {} point key(s) queried",
            spec.id(),
            golden_keys.len()
        );
    }
    *observed.entry(SHAPE_POINT_PATH_RESOLVED).or_insert(0) += point_positions_resolved;

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

/// Is this fact the ABSENCE of a complex-deletion marker rather than an
/// observation of one? (roborev finding S1, issue #4309.)
///
/// `<col>_complex_deletion` is a BOOLEAN: `false` states "this collection
/// column has no complex-deletion marker in this row". Counting that as an
/// observation would let a fixture with no marker anywhere "observe" the
/// `complex_deletion` family and satisfy a coverage claim it does not meet.
/// Every OTHER boolean is a genuine positive — `bound_inclusive = false` is
/// an EXCLUSIVE bound, which is a real measurement.
///
/// Split out and `pub` so it has a control in the census self-test: the
/// only fixture in the sweep with a collection column is
/// `test_deltas.collection_ops`, which is `FetchOnly`, so this rule would
/// otherwise never execute on the gate of record (roborev job 50).
pub fn is_negative_complex_marker(column: &str, fact: &Fact) -> bool {
    *fact == Fact::Bool(false) && fact_kind(column) == "complex_deletion"
}

/// The set of column names a raw-view row ACTUALLY carries in its values
/// map, which is the thing a producer can truncate — unlike
/// `result.metadata.columns`, which both producers share by construction.
///
/// `position` is excluded because it is the ONE legitimately path-divergent
/// column: the full-scan producer reports it `Null` (or omits it) while the
/// point-read producer resolves a real byte offset, so its presence differs
/// by access path BY DESIGN. It is the sweep's path witness, asserted
/// separately, and a declared gap in the column contract.
pub fn value_key_set(row: &QueryRow) -> BTreeSet<String> {
    row.values
        .keys()
        .map(|k| k.to_string())
        .filter(|k| k != "position")
        .collect()
}

/// The REAL #3890 assertion CLAUDE.md pins — "point/seek-vs-scan tests use
/// `SELECT *` and assert the column set in BOTH directions" — applied to the
/// column set that can actually diverge (roborev job 47, issue #4309).
///
/// Split out and `pub` so it has its own negative control: a comparison that
/// cannot fail is exactly the defect this replaced, so
/// `issue_4309_raw_view_census_selftest.rs` proves this one CAN, in both
/// directions, without needing a corpus.
pub fn assert_value_key_sets_match(
    fixture: &str,
    key: &str,
    identity: &RowIdentity,
    point: &BTreeSet<String>,
    scan: &BTreeSet<String>,
) {
    assert_eq!(
        point,
        scan,
        "issue #4309: {fixture} pk={key} row {identity:?} — the point-read producer must \
         populate the SAME value columns as the full scan. Present on the SCAN row but \
         MISSING from the point row: {:?}; present ONLY on the point row: {:?}. (A column \
         missing from the point row's values compares CLEAN against the fact model \
         whenever its golden value is Absent, because fact_of(None) and \
         fact_of(Some(Null)) are both Fact::Absent — which is why this is asserted on the \
         key sets rather than the values.)",
        scan.difference(point).collect::<Vec<_>>(),
        point.difference(scan).collect::<Vec<_>>(),
    );
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
