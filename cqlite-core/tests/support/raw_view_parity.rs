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
//!   row's liveness marker, which is why [`fold_simple_cell`] inherits it
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

#![allow(dead_code)]

#[path = "datasets_root.rs"]
pub mod datasets_root;

use chrono::DateTime;
use cqlite_core::query::result::{ColumnInfo, QueryRow};
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table, table_generation_dirs};
use serde_json::Value as Json;
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
// One comparable metadata fact
// ---------------------------------------------------------------------------

/// A single metadata-column value, or its ABSENCE.
///
/// Absence is a first-class outcome, not a `None` to be shrugged at: the
/// difference between "this cell carries no TTL" and "this cell's TTL is 0"
/// is exactly the fabricated-fact class the raw view exists to avoid
/// (issue #28), so the sweep compares absences as strictly as values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fact {
    Absent,
    Bool(bool),
    Int(i32),
    BigInt(i64),
    Text(String),
}

/// `Value::Null` collapses to [`Fact::Absent`]: the view writes a genuine
/// `Null` only for `position` on the full-scan path (not a compared column),
/// and a metadata column is otherwise either inserted with a value or not
/// inserted at all.
fn fact_of(value: Option<&Value>) -> Fact {
    match value {
        None | Some(Value::Null) => Fact::Absent,
        Some(Value::Boolean(b)) => Fact::Bool(*b),
        Some(Value::Integer(i)) => Fact::Int(*i),
        Some(Value::BigInt(i)) => Fact::BigInt(*i),
        Some(Value::Text(b)) => Fact::Text(String::from_utf8_lossy(b).into_owned()),
        Some(other) => panic!(
            "issue #4309: a raw-view METADATA column rendered an unexpected value shape \
             {other:?} — the sweep compares only boolean/int/bigint/text metadata, so an \
             unrecognized shape is a contract change that must be reviewed, never silently \
             skipped"
        ),
    }
}

// ---------------------------------------------------------------------------
// sstabledump golden loading
// ---------------------------------------------------------------------------

/// One generation's golden: the `Data.db` file name it describes, the
/// generation directory it came from, and every partition object of the
/// sidecar JSONL.
struct GoldenSstable {
    data_db: String,
    source_dir: PathBuf,
    partitions: Vec<Json>,
}

fn load_goldens(root: &Path, spec: &FixtureSpec) -> Vec<GoldenSstable> {
    let dirs = table_generation_dirs(root, spec.keyspace, spec.table);
    assert!(
        !dirs.is_empty(),
        "issue #4309: no *-Data.db-bearing {}-* directory under {}/{} even though that root \
         was selected as carrying the table",
        spec.table,
        root.display(),
        spec.keyspace
    );

    let mut goldens: Vec<GoldenSstable> = Vec::new();
    for dir in &dirs {
        let entries = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("reading {} must succeed: {e}", dir.display()));
        let mut data_dbs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .collect();
        data_dbs.sort();
        for data_db in data_dbs {
            let jsonl = data_db.with_file_name(format!(
                "{}.jsonl",
                data_db
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
            ));
            let text = std::fs::read_to_string(&jsonl).unwrap_or_else(|e| {
                panic!(
                    "issue #4309: the sstabledump golden {} must be readable — it is THE \
                     oracle for this sweep, so its absence is a FAILURE, never a skip: {e}",
                    jsonl.display()
                )
            });
            let partitions: Vec<Json> = text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| {
                    serde_json::from_str(l).unwrap_or_else(|e| {
                        panic!("golden {} line must be a JSON object: {e}", jsonl.display())
                    })
                })
                .collect();
            assert!(
                !partitions.is_empty(),
                "golden {} carried no partitions — a fixture that stopped exercising this \
                 sweep must FAIL, not pass vacuously",
                jsonl.display()
            );
            goldens.push(GoldenSstable {
                data_db: data_db
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string(),
                source_dir: dir.clone(),
                partitions,
            });
        }
    }
    goldens.sort_by(|a, b| (&a.data_db, &a.source_dir).cmp(&(&b.data_db, &b.source_dir)));
    assert!(
        !goldens.is_empty(),
        "issue #4309: {} resolved to a root with no readable golden at all",
        spec.id()
    );

    // FAIL LOUDLY on two generation directories contributing the SAME
    // `Data.db` file name (roborev finding I3, issue #4309). This is latent,
    // not hypothetical: the corpus already ships THREE `<table>-<uuid>`
    // directories for several `test_deltas`/`test_tomb` tables, and today
    // only one of each carries real binaries — the moment a second does, two
    // goldens both called e.g. `nb-1-big-Data.db` appear.
    //
    // It cannot be fixed by qualifying the group key, because the fact the
    // grouping is matched against is the raw view's OWN `sstable` column,
    // which carries the bare FILE NAME (`RawViewSource::from_reader` takes
    // `file_path().file_name()`). There is therefore NO information in the
    // query result that could tell the two apart, so the honest outcome is a
    // refusal that names the cause, never a silent collapse of both
    // generations onto one group (which would surface as a torrent of
    // "missing"/"unexplained" rows blaming the view for a harness limit).
    for (i, a) in goldens.iter().enumerate() {
        for b in &goldens[i + 1..] {
            assert_ne!(
                a.data_db,
                b.data_db,
                "issue #4309: {} has two generation directories contributing a golden \
                 named '{}' ({} and {}). The raw view's `sstable` column reports only the \
                 bare file name, so the sweep cannot attribute a returned row to one of \
                 them — scope the fixture to a single generation directory, or extend the \
                 harness with a directory-aware source identity, before relying on this \
                 lane for that table",
                spec.id(),
                a.data_db,
                a.source_dir.display(),
                b.source_dir.display()
            );
        }
    }
    goldens
}

/// Parse an sstabledump RFC3339 timestamp into epoch MICROSECONDS — the unit
/// every `*_timestamp` column reports.
fn iso_to_micros(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp_micros()
}

/// Parse an sstabledump RFC3339 timestamp into epoch SECONDS — the unit every
/// `*_local_deletion_time` / `*_deletion_time` / `row_liveness_expires_at`
/// column reports.
fn iso_to_secs(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp()
}

/// A required golden STRING field, or a panic naming what was missing — never
/// a silent default that would turn a drifted fixture into a pass.
fn golden_str<'a>(node: &'a Json, field: &str, ctx: &str) -> &'a str {
    node[field]
        .as_str()
        .unwrap_or_else(|| panic!("{ctx}: golden node must carry a string '{field}' — got {node}"))
}

/// Narrow a golden `i64` to the `i32` an `int` column reports, PANICKING on a
/// value that would not fit rather than silently wrapping — `as i32` would
/// make the equality assertion compare a WRAPPED number, i.e. an oracle blind
/// to the defect it exists to catch.
fn golden_i32(value: i64, what: &str) -> i32 {
    i32::try_from(value)
        .unwrap_or_else(|_| panic!("golden {what} {value} must fit the i32 an int column reports"))
}

// ---------------------------------------------------------------------------
// Column-contract classification, derived from the PUBLIC surface
// ---------------------------------------------------------------------------

const ROW_LEVEL_METADATA: &[&str] = &[
    "row_timestamp",
    "row_ttl",
    "row_liveness_expires_at",
    "row_local_deletion_time",
    "row_tombstone",
    "row_deletion_timestamp",
];
const PARTITION_METADATA: &[&str] = &["partition_deletion_time", "partition_deletion_timestamp"];
const RANGE_METADATA: &[&str] = &[
    "bound_inclusive",
    "range_deletion_time",
    "range_deletion_timestamp",
];

/// Which raw-view columns are keys, which are base data columns of each
/// shape, and therefore which metadata columns this sweep compares.
///
/// Derived ENTIRELY from the query result's own `metadata.columns` plus the
/// fixture's declared partition-key names — never from a transcribed list, so
/// a contract change shows up as a comparison-set change rather than as a
/// stale literal nobody updated.
pub struct ColumnRoles {
    pub clustering_columns: Vec<String>,
    pub simple_columns: Vec<String>,
    pub complex_columns: Vec<String>,
    pub compared_columns: Vec<String>,
}

fn classify_columns(columns: &[ColumnInfo], spec: &FixtureSpec) -> ColumnRoles {
    let names: BTreeSet<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    // A base column is "simple" when the contract synthesized a `_timestamp`
    // sibling for it, and "complex" when it synthesized a `_complex_deletion`
    // sibling. The `_complex_deletion` exclusion is load-bearing:
    // `tags_complex_deletion` itself has a `tags_complex_deletion_timestamp`
    // sibling and would otherwise be misread as a simple base column.
    let mut simple_columns: Vec<String> = Vec::new();
    let mut complex_columns: Vec<String> = Vec::new();
    for name in &names {
        if names.contains(format!("{name}_complex_deletion").as_str()) {
            complex_columns.push((*name).to_string());
        } else if !name.ends_with("_complex_deletion")
            && names.contains(format!("{name}_timestamp").as_str())
        {
            simple_columns.push((*name).to_string());
        }
    }

    // The contract emits partition keys, then clustering keys, then the first
    // base data column (or `row_timestamp` for a table with no non-key
    // columns). Everything before that boundary is a key column.
    let boundary = |n: &str| {
        n == "row_timestamp"
            || simple_columns.iter().any(|c| c == n)
            || complex_columns.iter().any(|c| c == n)
    };
    let key_columns: Vec<String> = columns
        .iter()
        .map(|c| c.name.to_string())
        .take_while(|n| !boundary(n))
        .collect();
    let pk: Vec<String> = spec
        .partition_key_columns
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert!(
        key_columns.starts_with(&pk),
        "issue #4309: {}'s raw view must lead with its declared partition-key columns \
         {pk:?}, got leading key columns {key_columns:?}",
        spec.id()
    );
    let clustering_columns = key_columns[pk.len()..].to_vec();

    let mut compared_columns: Vec<String> = Vec::new();
    for c in &simple_columns {
        compared_columns.push(format!("{c}_timestamp"));
        compared_columns.push(format!("{c}_ttl"));
        compared_columns.push(format!("{c}_local_deletion_time"));
        compared_columns.push(format!("{c}_tombstone"));
    }
    for c in &complex_columns {
        compared_columns.push(format!("{c}_complex_deletion"));
        compared_columns.push(format!("{c}_complex_deletion_time"));
        compared_columns.push(format!("{c}_complex_deletion_timestamp"));
    }
    for name in ROW_LEVEL_METADATA
        .iter()
        .chain(PARTITION_METADATA)
        .chain(RANGE_METADATA)
    {
        compared_columns.push((*name).to_string());
    }
    for name in &compared_columns {
        assert!(
            names.contains(name.as_str()),
            "issue #4309: {}'s raw view must declare the contract column '{name}' — a \
             column that silently disappeared from the contract would make this sweep \
             compare nothing for it",
            spec.id()
        );
    }
    compared_columns.sort();

    ColumnRoles {
        clustering_columns,
        simple_columns,
        complex_columns,
        compared_columns,
    }
}

// ---------------------------------------------------------------------------
// The golden-derived expectation model
// ---------------------------------------------------------------------------

/// A physical row the golden says must exist, with every metadata fact it
/// states. Identity is `(row_kind, clustering)` within one `(sstable, key)`
/// group.
struct ExpectedRow {
    sstable: String,
    key: String,
    row_kind: &'static str,
    clustering: Vec<Option<String>>,
    facts: BTreeMap<String, Fact>,
    origin: String,
}

type RowIdentity = (String, Vec<Option<String>>);

impl ExpectedRow {
    fn identity(&self) -> RowIdentity {
        (self.row_kind.to_string(), self.clustering.clone())
    }
}

/// Render a golden clustering array to the canonical string form the actual
/// row's clustering values are rendered to.
///
/// sstabledump writes an UNSPECIFIED trailing component of a PREFIX bound as
/// the literal `"*"`, which the raw view reports as an ABSENT column. A real
/// text clustering value of `"*"` would be indistinguishable here; no fixture
/// in the sweep has one, and the `row`-entry tripwire below makes the
/// ambiguity impossible for ordinary rows (which always carry every
/// component).
fn render_golden_clustering(node: &Json, width: usize, ctx: &str) -> Vec<Option<String>> {
    let arr = node.as_array().unwrap_or_else(|| {
        panic!(
            "{ctx}: golden entry carries no 'clustering' array. sstabledump omits it only \
             for a SIZE-0 clustering prefix (an open BOTTOM/TOP range bound — \
             `JsonTransformer.serializeClustering`), a shape no fixture in this sweep has \
             and this harness does not model; extend it rather than letting the entry pass \
             unchecked"
        )
    });
    assert_eq!(
        arr.len(),
        width,
        "{ctx}: golden clustering {arr:?} must have one component per clustering column"
    );
    arr.iter()
        .map(|v| match v {
            Json::Number(n) => Some(n.to_string()),
            Json::String(s) if s == "*" => None,
            Json::String(s) => Some(s.clone()),
            Json::Bool(b) => Some(b.to_string()),
            other => panic!("{ctx}: unsupported golden clustering component {other}"),
        })
        .collect()
}

/// Render an actual raw-view clustering value to the same canonical string.
fn render_actual_clustering(value: &Value, ctx: &str) -> String {
    match value {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Text(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Boolean(b) => b.to_string(),
        other => panic!(
            "{ctx}: this sweep renders only int/text/bool clustering components; extend it \
             before adding a fixture whose clustering is {other:?}"
        ),
    }
}

/// Fold one golden cell's facts into a row's expectation map.
///
/// # sstabledump's omission rule (the oracle's own shape)
///
/// `sstabledump` prints a cell's `tstamp`/`ttl`/`expires_at` ONLY when they
/// differ from the enclosing row's `liveness_info` — measured on this corpus,
/// 5411 of 5438 cells carry no `tstamp` at all, while every one of those rows
/// carries a `liveness_info.tstamp`. An omitted field therefore MEANS "same as
/// the row's liveness marker", and inheriting it is reading the golden, not
/// guessing. The inheritance is fail-closed: a cell with neither its own
/// `tstamp` nor an enclosing `liveness_info` PANICS rather than defaulting.
fn fold_simple_cell(
    facts: &mut BTreeMap<String, Fact>,
    cell: &Json,
    liveness: Option<&Json>,
    name: &str,
    ctx: &str,
) {
    let tstamp = cell
        .get("tstamp")
        .and_then(Json::as_str)
        .or_else(|| {
            liveness
                .and_then(|l| l.get("tstamp"))
                .and_then(Json::as_str)
        })
        .unwrap_or_else(|| {
            panic!(
                "{ctx}: golden cell '{name}' carries neither its own 'tstamp' nor an \
                 enclosing liveness_info.tstamp — the sweep refuses to invent a write time"
            )
        });
    facts.insert(
        format!("{name}_timestamp"),
        Fact::BigInt(iso_to_micros(tstamp)),
    );

    if let Some(deletion) = cell.get("deletion_info") {
        // A tombstoned cell: its local-deletion-time is the tombstone's GC
        // clock and it has no TTL of its own.
        facts.insert(format!("{name}_tombstone"), Fact::Text("cell".to_string()));
        facts.insert(
            format!("{name}_local_deletion_time"),
            Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", ctx))),
        );
        return;
    }

    let ttl = cell
        .get("ttl")
        .and_then(Json::as_i64)
        .or_else(|| liveness.and_then(|l| l.get("ttl")).and_then(Json::as_i64));
    if let Some(ttl) = ttl {
        facts.insert(
            format!("{name}_ttl"),
            Fact::Int(golden_i32(ttl, "liveness/cell ttl")),
        );
    }
    let expires_at = cell
        .get("expires_at")
        .and_then(Json::as_str)
        .or_else(|| {
            liveness
                .and_then(|l| l.get("expires_at"))
                .and_then(Json::as_str)
        })
        .map(|s| s.to_string());
    if let Some(expires_at) = expires_at {
        facts.insert(
            format!("{name}_local_deletion_time"),
            Fact::BigInt(iso_to_secs(&expires_at)),
        );
    }
}

/// Fold a collection/UDT column's complex-deletion marker into the row's
/// expectation map. The marker is the PATHLESS golden cell carrying a
/// `deletion_info`; per-element cells (those with a `path`) are a declared gap.
fn fold_complex_column(facts: &mut BTreeMap<String, Fact>, cells: &[Json], name: &str, ctx: &str) {
    let present = cells.iter().any(|c| c["name"] == *name);
    if !present {
        // The column contributed no cell at all in this row — the view emits
        // nothing for it, so every one of its three columns must be absent.
        return;
    }
    let marker = cells.iter().find(|c| {
        c["name"] == *name && c.get("path").is_none() && c.get("deletion_info").is_some()
    });
    facts.insert(
        format!("{name}_complex_deletion"),
        Fact::Bool(marker.is_some()),
    );
    if let Some(marker) = marker {
        let deletion = &marker["deletion_info"];
        facts.insert(
            format!("{name}_complex_deletion_timestamp"),
            Fact::BigInt(iso_to_micros(golden_str(deletion, "marked_deleted", ctx))),
        );
        facts.insert(
            format!("{name}_complex_deletion_time"),
            Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", ctx))),
        );
    }
}

/// Every metadata fact a golden `row`/`static_block` entry states.
fn row_entry_facts(entry: &Json, roles: &ColumnRoles, ctx: &str) -> BTreeMap<String, Fact> {
    let mut facts: BTreeMap<String, Fact> = BTreeMap::new();
    let liveness = entry.get("liveness_info");
    if let Some(liveness) = liveness {
        facts.insert(
            "row_timestamp".to_string(),
            Fact::BigInt(iso_to_micros(golden_str(liveness, "tstamp", ctx))),
        );
        if let Some(ttl) = liveness.get("ttl").and_then(Json::as_i64) {
            facts.insert(
                "row_ttl".to_string(),
                Fact::Int(golden_i32(ttl, "liveness ttl")),
            );
            facts.insert(
                "row_liveness_expires_at".to_string(),
                Fact::BigInt(iso_to_secs(golden_str(liveness, "expires_at", ctx))),
            );
        }
    }
    if let Some(deletion) = entry.get("deletion_info") {
        facts.insert("row_tombstone".to_string(), Fact::Text("row".to_string()));
        facts.insert(
            "row_local_deletion_time".to_string(),
            Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", ctx))),
        );
        facts.insert(
            "row_deletion_timestamp".to_string(),
            Fact::BigInt(iso_to_micros(golden_str(deletion, "marked_deleted", ctx))),
        );
    }

    let empty: Vec<Json> = Vec::new();
    let cells = entry["cells"].as_array().unwrap_or(&empty);
    for cell in cells {
        let name = cell["name"]
            .as_str()
            .unwrap_or_else(|| panic!("{ctx}: golden cell must carry a 'name'"));
        if roles.complex_columns.iter().any(|c| c == name) {
            continue; // handled once per column below
        }
        assert!(
            roles.simple_columns.iter().any(|c| c == name),
            "{ctx}: golden names a cell '{name}' that is not a base column of the raw \
             view's contract — the sweep would silently ignore its metadata"
        );
        fold_simple_cell(&mut facts, cell, liveness, name, ctx);
    }
    for name in &roles.complex_columns {
        fold_complex_column(&mut facts, cells, name, ctx);
    }
    facts
}

/// Facts a golden range-tombstone bound side states.
fn bound_facts(bound: &Json, ctx: &str) -> BTreeMap<String, Fact> {
    let mut facts: BTreeMap<String, Fact> = BTreeMap::new();
    let inclusive = match golden_str(bound, "type", ctx) {
        "inclusive" => true,
        "exclusive" => false,
        other => panic!("{ctx}: unexpected sstabledump bound type '{other}'"),
    };
    facts.insert("bound_inclusive".to_string(), Fact::Bool(inclusive));
    let deletion = &bound["deletion_info"];
    facts.insert(
        "range_deletion_timestamp".to_string(),
        Fact::BigInt(iso_to_micros(golden_str(deletion, "marked_deleted", ctx))),
    );
    facts.insert(
        "range_deletion_time".to_string(),
        Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", ctx))),
    );
    facts
}

/// Turn every golden partition of every generation into the physical rows the
/// raw view must produce, alongside a census of the golden ENTRY SHAPES seen.
///
/// The shape census is the other half of the affirmative zero (roborev
/// finding I2, issue #4309). Counting metadata FAMILIES alone cannot catch a
/// fixture that silently stopped exercising the shape its lane exists for,
/// because the golden and the expectation model lose that shape TOGETHER: a
/// regenerated `static_with_rows` with zero `static_block` entries, or an
/// `adjacent_ranges` with zero `range_tombstone_boundary` entries (plain
/// bounds emit the identical families), would compare cleanly and report
/// green. Each case therefore names the SHAPES it is there to exercise, not
/// only the families.
fn build_expectations(
    goldens: &[GoldenSstable],
    roles: &ColumnRoles,
    spec: &FixtureSpec,
) -> (Vec<ExpectedRow>, BTreeMap<&'static str, usize>) {
    let width = roles.clustering_columns.len();
    let mut expected: Vec<ExpectedRow> = Vec::new();
    let mut shapes: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut bump = |shape: &'static str| *shapes.entry(shape).or_insert(0) += 1;
    if goldens.len() > 1 {
        // A cross-generation fixture: several lanes exist ONLY to show one key
        // yielding one row per generation with no reconciliation, and a
        // regeneration that collapsed to a single SSTable would silently
        // retire that property.
        bump("shape:multi_generation");
    }
    for golden in goldens {
        for partition in &golden.partitions {
            let key_components = partition["partition"]["key"]
                .as_array()
                .unwrap_or_else(|| panic!("golden partition must carry a 'key' array"));
            assert_eq!(
                key_components.len(),
                spec.partition_key_columns.len(),
                "issue #4309: {}'s golden partition key {key_components:?} must have one \
                 component per declared partition-key column",
                spec.id()
            );
            let key = key_components
                .iter()
                .map(|c| {
                    c.as_str()
                        .unwrap_or_else(|| {
                            panic!("sstabledump renders partition-key components as strings")
                        })
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join("|");
            let ctx = format!("{} {} pk={key}", spec.id(), golden.data_db);

            if let Some(deletion) = partition["partition"].get("deletion_info") {
                bump("entry:partition_deletion");
                let mut facts: BTreeMap<String, Fact> = BTreeMap::new();
                facts.insert(
                    "partition_deletion_timestamp".to_string(),
                    Fact::BigInt(iso_to_micros(golden_str(deletion, "marked_deleted", &ctx))),
                );
                facts.insert(
                    "partition_deletion_time".to_string(),
                    Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", &ctx))),
                );
                expected.push(ExpectedRow {
                    sstable: golden.data_db.clone(),
                    key: key.clone(),
                    row_kind: "partition_tombstone",
                    clustering: vec![None; width],
                    facts,
                    origin: format!("{ctx} partition deletion_info"),
                });
            }

            let empty: Vec<Json> = Vec::new();
            for entry in partition["rows"].as_array().unwrap_or(&empty) {
                let kind = entry["type"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{ctx}: golden entry must carry a 'type'"));
                match kind {
                    "row" => {
                        bump("entry:row");
                        // An UPDATE-only row: no primary-key liveness marker,
                        // yet real cells — `row_timestamp` must be ABSENT
                        // while every cell still carries its own write time.
                        // A row TOMBSTONE also lacks liveness, so the
                        // non-empty cell set is what makes this the partial-
                        // UPDATE shape specifically.
                        if entry.get("liveness_info").is_none()
                            && entry["cells"].as_array().is_some_and(|c| !c.is_empty())
                        {
                            bump("shape:row_update_without_liveness");
                        }
                        let clustering =
                            render_golden_clustering(&entry["clustering"], width, &ctx);
                        assert!(
                            clustering.iter().all(Option::is_some),
                            "{ctx}: a golden 'row' entry must carry every clustering \
                             component — a '*' here would collide with the PREFIX-bound \
                             encoding this sweep relies on"
                        );
                        let origin = format!("{ctx} row clustering={clustering:?}");
                        expected.push(ExpectedRow {
                            sstable: golden.data_db.clone(),
                            key: key.clone(),
                            row_kind: "row",
                            clustering,
                            facts: row_entry_facts(entry, roles, &origin),
                            origin,
                        });
                    }
                    "static_block" => {
                        bump("entry:static_block");
                        // Declared gap (module doc): the view cannot yet mark
                        // a static row, so it renders as `row_kind = 'row'`
                        // with no clustering. Its CELL metadata is still
                        // compared byte-exact.
                        let origin = format!("{ctx} static_block");
                        expected.push(ExpectedRow {
                            sstable: golden.data_db.clone(),
                            key: key.clone(),
                            row_kind: "row",
                            clustering: vec![None; width],
                            facts: row_entry_facts(entry, roles, &origin),
                            origin,
                        });
                    }
                    "range_tombstone_bound" | "range_tombstone_boundary" => {
                        bump(match kind {
                            "range_tombstone_boundary" => "entry:range_tombstone_boundary",
                            _ => "entry:range_tombstone_bound",
                        });
                        // A BOUNDARY closes one range and opens the next at
                        // the same clustering position, and sstabledump
                        // renders both sides in ONE entry with their OWN
                        // deletion times — so it expands to TWO raw-view rows,
                        // exactly as a `bound` pair does.
                        for (side, row_kind) in [
                            ("end", "range_tombstone_end"),
                            ("start", "range_tombstone_start"),
                        ] {
                            let Some(bound) = entry.get(side) else {
                                continue;
                            };
                            let clustering =
                                render_golden_clustering(&bound["clustering"], width, &ctx);
                            // A PREFIX bound: sstabledump rendered a trailing
                            // clustering component as `"*"`, and the view must
                            // report it ABSENT rather than fabricate a value.
                            if clustering.iter().any(Option::is_none) {
                                bump("shape:prefix_bound");
                            }
                            let origin = format!("{ctx} {kind}.{side} clustering={clustering:?}");
                            expected.push(ExpectedRow {
                                sstable: golden.data_db.clone(),
                                key: key.clone(),
                                row_kind,
                                clustering,
                                facts: bound_facts(bound, &origin),
                                origin,
                            });
                        }
                    }
                    other => panic!(
                        "{ctx}: unrecognized sstabledump entry type '{other}' — the sweep \
                         refuses to ignore a physical entry it cannot model"
                    ),
                }
            }
        }
    }
    assert!(
        !expected.is_empty(),
        "issue #4309: {}'s goldens describe no physical rows at all",
        spec.id()
    );
    (expected, shapes)
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
