//! The ORACLE half of the raw-SSTable-view parity sweep (issue #4309):
//! reading a Cassandra-written `sstabledump` golden and turning it into the
//! set of physical rows, and metadata facts, the raw view must reproduce.
//!
//! Split out of `raw_view_parity.rs` along a responsibility seam under the
//! campsite rule (CLAUDE.md, epic #1135): that file now owns the SWEEP —
//! fixture resolution, the two producers, comparison and the coverage census
//! — while this one owns the GOLDEN. Read `raw_view_parity.rs`'s module doc
//! first: it carries the column-contract checklist, every declared gap, and
//! the pinned-`cassandra-5.0.8` `JsonTransformer` rules this file implements.
//!
//! Included as a CHILD module of `raw_view_parity`, so it reaches the shared
//! fixture types and the datasets-root helpers through `super::`.

#![allow(dead_code)]

use super::datasets_root::table_generation_dirs;
use super::FixtureSpec;
use chrono::DateTime;
use cqlite_core::query::result::ColumnInfo;
use cqlite_core::types::Value;
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

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
pub fn fact_of(value: Option<&Value>) -> Fact {
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
pub struct GoldenSstable {
    pub data_db: String,
    pub source_dir: PathBuf,
    pub partitions: Vec<Json>,
}

pub fn load_goldens(root: &Path, spec: &FixtureSpec) -> Vec<GoldenSstable> {
    let dirs = table_generation_dirs(root, spec.keyspace, spec.table);
    assert!(
        !dirs.is_empty(),
        "issue #4309: no *-Data.db-bearing {}-* directory under {}/{} even though that root \
         was selected as carrying the table",
        spec.table,
        root.display(),
        spec.keyspace
    );

    // ONE GENERATION DIRECTORY, chosen deterministically (roborev job 61,
    // issue #4309). `table_generation_dirs` returns only `Data.db`-BEARING
    // directories, sorted, so `dirs[0]` is stable across runs and machines.
    //
    // WHY, and why this does not lose a generation. The corpus ships THREE
    // `<table>-<uuid>/` directories for several `test_deltas` tables — they
    // are separate REGENERATIONS of the same table, with DIFFERENT golden
    // content (verified: the three `static_with_rows` sidecars have three
    // distinct md5s), and only one carries binaries. Every genuinely
    // multi-generation fixture instead keeps `nb-1` AND `nb-2` inside a
    // SINGLE directory — verified for all five: `skipped_partition_delete`,
    // `resurrection_gc0`, `resurrection_gc_positive`, `dropped_regular_col`,
    // `dropped_static_col`. So binding to one directory preserves every
    // declared shape, `shape:multi_generation` included.
    //
    // Enumerating ALL of them was a latent hard failure on a `must_run`
    // case: two directories would then contribute goldens both named
    // `nb-1-big-Data.db`, and the raw view's `sstable` column reports only
    // the bare FILE NAME (`RawViewSource::from_reader` takes
    // `file_path().file_name()`), so nothing in the query result can
    // attribute a row to one of them. `test_deltas.static_with_rows` is
    // `Discipline::GitCommitted`, so it cannot skip — it would hard-FAIL on
    // exactly the fetched-corpus, strict-mode run this module's doc
    // prescribes as the way to certify the sweep. Selecting one directory
    // removes the ambiguity at its source instead of refusing on it.
    //
    // The choice is ANNOUNCED, never silent: a skipped sibling is named
    // below, because a harness that quietly ignores half a corpus is the
    // failure mode this sweep exists to catch.
    let chosen = &dirs[0];
    if dirs.len() > 1 {
        let skipped: Vec<String> = dirs[1..].iter().map(|d| d.display().to_string()).collect();
        eprintln!(
            "NOTE: {} binds to ONE generation directory, {} — {} OTHER \
             Data.db-bearing {}-* {} RECOGNISED and deliberately NOT read: {}. Each is a \
             separate regeneration with its own golden content; the sweep compares one. \
             Pinning a specific directory is issue #4314.",
            spec.id(),
            chosen.display(),
            skipped.len(),
            spec.table,
            if skipped.len() == 1 {
                "directory was"
            } else {
                "directories were"
            },
            skipped.join(", ")
        );
    }

    let mut goldens: Vec<GoldenSstable> = Vec::new();
    {
        let dir = chosen;
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

    // ASSERT THE PROPERTY THAT CAN CHANGE, not one that cannot (roborev job
    // 65). The previous cross-directory duplicate check could no longer fire
    // at all once the selection was pinned: all goldens come from ONE
    // filesystem directory, where two files cannot share a name — its own
    // message said "impossible on a normal filesystem", which is the
    // signature of an inert guard.
    //
    // What a future edit CAN break is the pin itself, by letting a golden in
    // from a directory other than the chosen one. That is checked here, and
    // it is the property `assert_raw_view_matches_golden` relies on when it
    // takes `goldens[0].source_dir` as the ingest selection: if the goldens
    // spanned directories, oracle and query would read different bytes.
    for g in &goldens {
        assert_eq!(
            &g.source_dir,
            chosen,
            "issue #4309: {} produced a golden ('{}') from {} rather than the chosen \
             generation directory {}. The sweep pins ONE directory so the oracle and \
             the ingest selection read the same bytes; a golden from elsewhere breaks \
             that binding",
            spec.id(),
            g.data_db,
            g.source_dir.display(),
            chosen.display()
        );
    }
    goldens
}

/// Parse an sstabledump RFC3339 timestamp into epoch MICROSECONDS — the unit
/// every `*_timestamp` column reports.
pub fn iso_to_micros(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp_micros()
}

/// Parse an sstabledump RFC3339 timestamp into epoch SECONDS — the unit every
/// `*_local_deletion_time` / `*_deletion_time` / `row_liveness_expires_at`
/// column reports.
pub fn iso_to_secs(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp()
}

/// A required golden STRING field, or a panic naming what was missing — never
/// a silent default that would turn a drifted fixture into a pass.
pub fn golden_str<'a>(node: &'a Json, field: &str, ctx: &str) -> &'a str {
    node[field]
        .as_str()
        .unwrap_or_else(|| panic!("{ctx}: golden node must carry a string '{field}' — got {node}"))
}

/// Narrow a golden `i64` to the `i32` an `int` column reports, PANICKING on a
/// value that would not fit rather than silently wrapping — `as i32` would
/// make the equality assertion compare a WRAPPED number, i.e. an oracle blind
/// to the defect it exists to catch.
pub fn golden_i32(value: i64, what: &str) -> i32 {
    i32::try_from(value)
        .unwrap_or_else(|_| panic!("golden {what} {value} must fit the i32 an int column reports"))
}

// ---------------------------------------------------------------------------
// Column-contract classification, derived from the PUBLIC surface
// ---------------------------------------------------------------------------

pub const ROW_LEVEL_METADATA: &[&str] = &[
    "row_timestamp",
    "row_ttl",
    "row_liveness_expires_at",
    "row_local_deletion_time",
    "row_tombstone",
    "row_deletion_timestamp",
];
pub const PARTITION_METADATA: &[&str] =
    &["partition_deletion_time", "partition_deletion_timestamp"];
/// Contract columns this sweep deliberately does NOT compare against the
/// golden, each with its reason recorded in the `raw_view_parity` module
/// doc's declared-gap table. Named here so `classify_columns` can prove the
/// contract contains nothing BESIDES these and the columns it compares
/// (roborev finding R2, issue #4309).
///
///   * `row_kind` — not a gap but an IDENTITY component: it discriminates the
///     expected/actual row sets, so a wrong kind already fails as a
///     missing+unexplained pair rather than as a value mismatch.
///   * `sstable` — the per-generation GROUPING key; a row attributed to an
///     SSTable with no golden is refused outright.
///   * `generation`, `format` — source identity, outside this sweep's
///     timestamp/TTL/LDT/tombstone scope; value-asserted by the #4222 lanes.
///   * `position` — the VIEW's own documented point-vs-scan divergence, so it
///     has no single golden-comparable value across the two producers run
///     here; value-asserted against the golden's partition offset by
///     `issue_4222_raw_view_point_read_test.rs` and
///     `issue_4222_raw_view_bti_point_read_test.rs`.
pub const DECLARED_GAP_COLUMNS: &[&str] =
    &["row_kind", "sstable", "generation", "format", "position"];

pub const RANGE_METADATA: &[&str] = &[
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

pub fn classify_columns(columns: &[ColumnInfo], spec: &FixtureSpec) -> ColumnRoles {
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

    // The CONVERSE direction (roborev finding R2, issue #4309). The check
    // above is one-way: it proves every column the sweep MEANS to compare
    // exists. It cannot see a column the CONTRACT gained that the sweep never
    // heard of — and `ROW_LEVEL_METADATA`/`PARTITION_METADATA`/
    // `RANGE_METADATA` are literal lists, so a new metadata column added to
    // `raw_view_columns` later would simply never be compared, silently. That
    // is the exact gap class this whole sweep exists to close, so it must not
    // exist in the sweep itself.
    //
    // Every column of the contract must therefore fall into one of five
    // accounted-for buckets, and an unrecognized one FAILs until someone
    // either compares it or declares it a gap on purpose.
    let accounted: BTreeSet<&str> = key_columns
        .iter()
        .map(String::as_str)
        .chain(simple_columns.iter().map(String::as_str))
        .chain(complex_columns.iter().map(String::as_str))
        .chain(compared_columns.iter().map(String::as_str))
        .chain(DECLARED_GAP_COLUMNS.iter().copied())
        .collect();
    for column in &names {
        assert!(
            accounted.contains(column),
            "issue #4309: {}'s raw view declares a column '{column}' this sweep neither \
             compares against the golden nor declares as a gap. A contract column nobody \
             accounts for is compared by nothing and noticed by nobody — add it to the \
             compared set, or to DECLARED_GAP_COLUMNS with its reason in the \
             `raw_view_parity` module doc",
            spec.id()
        );
    }

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
pub struct ExpectedRow {
    pub sstable: String,
    pub key: String,
    pub row_kind: &'static str,
    pub clustering: Vec<Option<String>>,
    pub facts: BTreeMap<String, Fact>,
    pub origin: String,
}

pub type RowIdentity = (String, Vec<Option<String>>);

impl ExpectedRow {
    pub fn identity(&self) -> RowIdentity {
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
pub fn render_golden_clustering(node: &Json, width: usize, ctx: &str) -> Vec<Option<String>> {
    let arr = node.as_array().unwrap_or_else(|| {
        panic!(
            "{ctx}: golden entry carries no 'clustering' array. \
             `JsonTransformer.serializeClustering` guards on `clustering.size() > 0`, so \
             the field is absent in TWO distinct cases, and this harness models NEITHER: \
             (1) a SIZE-0 clustering PREFIX — an open BOTTOM/TOP range bound; and (2) EVERY \
             `row` entry of a table declaring NO clustering columns at all (width == {width}; \
             e.g. `test_compactionparity.live_no_clustering`, which lives in the same \
             `compaction-parity.cql` this sweep already loads for `live_clustering`). If you \
             are extending the sweep to a clustering-free table, this panic is case (2) and \
             NOT a range-bound modelling problem — treat a missing `clustering` as an empty \
             prefix there. Extend the harness rather than letting the entry pass unchecked"
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
pub fn render_actual_clustering(value: &Value, ctx: &str) -> String {
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
pub fn fold_simple_cell(
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
        //
        // ATTRIBUTION, same trap as the inherited-TTL note below (roborev,
        // issue #4309): the view has TWO cell-tombstone kinds — `row_map.rs`
        // renders "expired" for `TombstoneType::TtlExpiration` and "cell"
        // otherwise — but the golden's `deletion_info` does not distinguish
        // them, so this model hard-codes "cell". No fixture in the sweep
        // surfaces the expiration variant today; if one ever does (the
        // `gc_before_boundary` / `ttl_cells` families are the plausible
        // source) the resulting `<col>_tombstone` mismatch is a
        // HARNESS-MODELLING gap FIRST and a view failure only second. Accept
        // both kinds here before concluding the view is wrong.
        facts.insert(format!("{name}_tombstone"), Fact::Text("cell".to_string()));
        facts.insert(
            format!("{name}_local_deletion_time"),
            Fact::BigInt(iso_to_secs(golden_str(deletion, "local_delete_time", ctx))),
        );
        return;
    }

    // INHERITED TTL — read the attribution note before debugging the view.
    //
    // `serializeCell` omits `ttl` in two cases we cannot distinguish from the
    // JSONL: the cell expires with the SAME ttl as the row marker (inherited
    // here), or the cell is NOT EXPIRING AT ALL. CQLite inherits only when the
    // cell's on-disk USE_ROW_TTL (0x10) flag is set (`row_data.rs`, #1743), so
    // for a row whose TTL'd liveness marker is later partially overwritten by
    // a plain UPDATE, this harness would demand a `<col>_ttl` /
    // `<col>_local_deletion_time` the view CORRECTLY reports as absent.
    //
    // So: a mismatch on an INHERITED ttl/expires_at is a HARNESS-ASSUMPTION
    // failure FIRST and a view failure only second. No fixture in the sweep
    // has that shape today (all 27 pass); if you add one, fix the inheritance
    // rule here before you touch the view. Scoping inheritance to cells the
    // golden shows as expiring is the candidate fix (roborev, issue #4309).
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
pub fn fold_complex_column(
    facts: &mut BTreeMap<String, Fact>,
    cells: &[Json],
    name: &str,
    ctx: &str,
) {
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
pub fn row_entry_facts(entry: &Json, roles: &ColumnRoles, ctx: &str) -> BTreeMap<String, Fact> {
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
pub fn bound_facts(bound: &Json, ctx: &str) -> BTreeMap<String, Fact> {
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

/// Every golden ENTRY SHAPE `build_expectations` can count.
///
/// The enumerable counterpart of `FACT_KIND_RULES` for the shape half of
/// the coverage vocabulary (roborev job 61). `bump` asserts membership, and
/// `issue_4309_raw_view_census_selftest.rs` asserts this list plus every
/// `FACT_KIND_RULES` family is EXACTLY `KNOWN_COVERAGE_TOKENS`.
/// Named once so the vocabulary entry and the insert site cannot drift
/// apart — `shape:multi_generation` is DERIVED after the entry loop, so it
/// is the one shape that does not go through `bump`'s membership check.
pub const SHAPE_MULTI_GENERATION: &str = "shape:multi_generation";

pub const SHAPE_TOKENS: &[&str] = &[
    "entry:row",
    "entry:static_block",
    "entry:partition_deletion",
    "entry:range_tombstone_bound",
    "entry:range_tombstone_boundary",
    "shape:prefix_bound",
    "shape:row_update_without_liveness",
    SHAPE_MULTI_GENERATION,
];

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
pub fn build_expectations(
    goldens: &[GoldenSstable],
    roles: &ColumnRoles,
    spec: &FixtureSpec,
) -> (Vec<ExpectedRow>, BTreeMap<&'static str, usize>) {
    let width = roles.clustering_columns.len();
    let mut expected: Vec<ExpectedRow> = Vec::new();
    let mut shapes: BTreeMap<&'static str, usize> = BTreeMap::new();
    // Every shape token this function can emit is validated against
    // `SHAPE_TOKENS` as it is bumped (roborev job 61). The FAMILY half of
    // the census vocabulary was mechanized from `FACT_KIND_RULES` (jobs
    // 56/59); the shape half was still enumerated by hand in the self-test,
    // so a new `bump("shape:…")` without a matching `KNOWN_COVERAGE_TOKENS`
    // entry landed in neither set and left the equality assertion green —
    // while the census counted the shape and `require_observed` rejected
    // every lane's claim for it as an unknown name. Now a new shape must be
    // declared here, and the self-test folds this list into the equality.
    let mut bump = |shape: &'static str| {
        assert!(
            SHAPE_TOKENS.contains(&shape),
            "issue #4309: build_expectations bumped '{shape}', which is not in \
             SHAPE_TOKENS. Add it there AND to KNOWN_COVERAGE_TOKENS, or no lane will \
             ever be able to claim the shape this counts"
        );
        *shapes.entry(shape).or_insert(0) += 1
    };
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

    // `shape:multi_generation` is derived from the EXPECTATION MODEL, never
    // from `goldens.len()` (roborev job 42, issue #4309).
    //
    // Five lanes claim this token — `skipped_partition_delete`,
    // `resurrection_gc0`, `resurrection_gc_positive`, `dropped_regular_col`,
    // `dropped_static_col` — and every one of them exists to show ONE
    // PARTITION KEY yielding one UNRECONCILED row per generation: the raw
    // view is physical, so a key written in gen-1 and shadowed/resurrected in
    // gen-2 must surface TWICE, once per `Data.db`, rather than being
    // reconciled into one logical row. Bumping the token from "this fixture
    // has more than one SSTable" witnesses a far weaker property: a
    // regeneration whose generations hold DISJOINT keys (or whose gen-2
    // deletes land on different `pk`s than gen-1's inserts) would keep the
    // token positive with the cross-generation shape gone — reproducing,
    // inside the census, exactly the "golden and expectation model lose the
    // shape together and compare cleanly" failure the census exists to close.
    //
    // So count the keys that genuinely span generations. `expected` is the
    // post-modelling row set, so this counts what the sweep will actually
    // COMPARE, not what the fixture directory happens to contain. The
    // negative control is
    // `issue_4309_raw_view_census_selftest.rs::disjoint_keys_across_generations_do_not_claim_multi_generation`,
    // which fails against the old derivation.
    let mut generations_per_key: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for row in &expected {
        generations_per_key
            .entry(row.key.as_str())
            .or_default()
            .insert(row.sstable.as_str());
    }
    let cross_generation_keys = generations_per_key
        .values()
        .filter(|sstables| sstables.len() > 1)
        .count();
    if cross_generation_keys > 0 {
        shapes.insert(SHAPE_MULTI_GENERATION, cross_generation_keys);
    }

    (expected, shapes)
}
