//! Table-driven state-machine tests for [`StaticMergeSource`] (issue #3095).
//!
//! These pin every TRANSITION of the adapter directly, by scripting the exact
//! `SourceStep` sequences an inner [`RowSource`] can produce — including the ones no
//! committed fixture reaches through `do_get` (a partition boundary with no
//! `PartitionEnd`, a static row arriving after a clustering row, a token-excluded
//! partition). Both #3095 review blockers (`emitted_clustering_row` set before
//! suppression, and the deferred-step raw passthrough) were invisible to the
//! fixture-level lanes precisely because this level had no tests.
//!
//! Each case asserts the OUTPUT SEQUENCE the drive loop would see, so a phantom
//! `ck = null` row, a missing static value, a lost static-only row, and a
//! double-emitted row are all distinguishable failures.

use cqlite_core::query::QueryRow;
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{CellData, MergeEntry, RowData};
use cqlite_core::storage::write_engine::{ClusteringKey, DecoratedKey};
use cqlite_core::types::Value;

use super::StaticMergeSource;
use crate::filter::ScanSpec;
use crate::producer::MergeProducer;
use crate::row_source::{PendingRow, RowSource, SourceStep};
use crate::ticket::FlightTicket;

const KS: &str = "statics_ks";
const TBL: &str = "statics_tbl";

// ---------------------------------------------------------------------------
// Fixtures: schema, entries, and a scripted inner source
// ---------------------------------------------------------------------------

fn col(name: &str, ty: &str, is_static: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static,
    }
}

/// `CREATE TABLE (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))`.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            col("s", "text", true),
            col("v", "text", false),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// The same shape WITHOUT a static column, to pin that `wrap` declines.
fn no_static_schema() -> TableSchema {
    let mut s = static_schema();
    for c in &mut s.columns {
        c.is_static = false;
    }
    s
}

fn producer(schema: TableSchema) -> MergeProducer {
    MergeProducer::new(schema, 1024).expect("producer builds")
}

/// A producer whose scan spec carries the token range `(start, end]`, built through
/// the SAME public ticket path `do_get` uses (`TokenFilter`'s fields are private).
fn producer_with_token(schema: TableSchema, start: i64, end: i64) -> MergeProducer {
    let ticket = FlightTicket {
        keyspace: KS.into(),
        table: TBL.into(),
        ddl: String::new(),
        token_start: Some(start),
        token_end: Some(end),
        ..Default::default()
    };
    let spec = ScanSpec::from_ticket(&ticket, &schema).expect("scan spec");
    assert!(spec.token.is_some(), "the ticket must carry a token filter");
    MergeProducer::with_spec(schema, 1024, spec).expect("producer builds")
}

fn key(pk: i32, token: i64) -> DecoratedKey {
    DecoratedKey::new(token, pk.to_be_bytes().to_vec())
}

fn cell(column: &str, value: Value) -> CellData {
    CellData::new(column.to_string(), value, 1_000)
}

/// The partition's reconciled STATIC row: `clustering_key: None`, static cells only —
/// exactly what `write_engine/merge` streams (sorted first within the partition).
fn static_entry(pk: i32, token: i64, s: &str) -> PendingRow {
    PendingRow::Merged(Box::new(MergeEntry::new(
        0,
        key(pk, token),
        None,
        1_000,
        RowData::Live {
            cells: vec![cell("s", Value::text(s))],
        },
    )))
}

/// A clustering row. The merger surfaces clustering columns as pseudo-cells, so `ck`
/// rides in the cell set exactly as it does in production.
fn clustering_entry(pk: i32, token: i64, ck: i32, v: &str) -> PendingRow {
    PendingRow::Merged(Box::new(MergeEntry::new(
        0,
        key(pk, token),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        1_000,
        RowData::Live {
            cells: vec![cell("ck", Value::Integer(ck)), cell("v", Value::text(v))],
        },
    )))
}

/// A whole-row tombstone: `entry_to_row` suppresses it, so it must NOT count as one
/// of Cassandra's `partition.hasNext()` rows (issue #3095 B1, merge-arm half).
fn row_tombstone_entry(pk: i32, token: i64, ck: i32) -> PendingRow {
    PendingRow::Merged(Box::new(MergeEntry::new(
        0,
        key(pk, token),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        2_000,
        RowData::Tombstone {
            deletion_time: 2_000,
            local_deletion_time: 1_700_000_000,
        },
    )))
}

/// An inner [`RowSource`] replaying a scripted step sequence, then `Complete`
/// forever. Counts pulls so a test can prove the adapter defers rather than drops.
///
/// `fail_after` makes the source ERROR instead of yielding its Nth step, modelling a
/// producer that dies mid-walk — the #3106 fail-closed event, whose interaction with a
/// HELD-BACK static row is otherwise untested.
struct Scripted {
    steps: std::vec::IntoIter<SourceStep>,
    pulls: usize,
    fail_after: Option<usize>,
}

impl Scripted {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self {
            steps: steps.into_iter(),
            pulls: 0,
            fail_after: None,
        }
    }

    /// Yield `n` steps, then fail with a #3106-shaped dead-producer error.
    fn failing_after(steps: Vec<SourceStep>, n: usize) -> Self {
        Self {
            steps: steps.into_iter(),
            pulls: 0,
            fail_after: Some(n),
        }
    }
}

impl RowSource for Scripted {
    fn next_step(&mut self) -> Result<SourceStep, crate::producer::ProducerError> {
        if self.fail_after == Some(self.pulls) {
            self.pulls += 1;
            // The shape #3106 surfaces: a non-recoverable `Error::Internal` reporting a
            // producer that died without its terminal sentinel.
            return Err(crate::producer::ProducerError::Merge(
                cqlite_core::Error::internal(
                    "query row stream: the producer thread disconnected WITHOUT its \
                     terminal Done sentinel (issue #3106)",
                ),
            ));
        }
        self.pulls += 1;
        Ok(self.steps.next().unwrap_or(SourceStep::Complete))
    }
}

/// One observed output step, flattened for readable assertions.
#[derive(Debug, PartialEq)]
enum Observed {
    /// A materialized row: `(pk, ck, s, v)` rendered, `None` = the column is absent
    /// (which is what Arrow encodes as NULL).
    Row(Option<i32>, Option<i32>, Option<String>, Option<String>),
    /// A row increment carrying no output row.
    Suppressed,
    PartitionEnd,
    Complete,
}

fn int_of(row: &QueryRow, col: &str) -> Option<i32> {
    match row.values.get(col) {
        Some(Value::Integer(i)) => Some(*i),
        Some(Value::BigInt(i)) => Some(*i as i32),
        _ => None,
    }
}

fn text_of(row: &QueryRow, col: &str) -> Option<String> {
    match row.values.get(col) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    }
}

fn observe(step: SourceStep) -> Observed {
    match step {
        SourceStep::Row(_, PendingRow::Materialized(row)) => Observed::Row(
            int_of(&row, "pk"),
            int_of(&row, "ck"),
            text_of(&row, "s"),
            text_of(&row, "v"),
        ),
        SourceStep::Row(_, PendingRow::Suppressed) => Observed::Suppressed,
        SourceStep::Row(_, _) => panic!(
            "the adapter must never hand a RAW PendingRow downstream — that is the \
             #3095 B3 passthrough regression"
        ),
        SourceStep::PartitionEnd(_) => Observed::PartitionEnd,
        SourceStep::Complete => Observed::Complete,
    }
}

/// Drive the adapter over `steps` until `Complete`, returning the observed output.
fn drive(producer: &MergeProducer, steps: Vec<SourceStep>) -> Vec<Observed> {
    let mut inner = Scripted::new(steps);
    let mut source =
        StaticMergeSource::wrap(producer, &mut inner).expect("the schema declares statics");
    let mut out = Vec::new();
    for _ in 0..64 {
        let step = source.next_step().expect("no error");
        let observed = observe(step);
        let done = observed == Observed::Complete;
        out.push(observed);
        if done {
            return out;
        }
    }
    panic!("the adapter did not complete within 64 steps: {out:?}");
}

fn row(pk: i32, ck: Option<i32>, s: Option<&str>, v: Option<&str>) -> Observed {
    Observed::Row(Some(pk), ck, s.map(str::to_string), v.map(str::to_string))
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// `wrap` declines for a schema without a static column, so a non-static table keeps
/// the unadapted merge source (zero behaviour change, and the
/// `clustering_key: None` == static-row inference is never applied where it would be
/// unsound).
#[test]
fn wrap_declines_without_a_static_column() {
    let p = producer(no_static_schema());
    let mut inner = Scripted::new(vec![]);
    assert!(StaticMergeSource::wrap(&p, &mut inner).is_none());
}

/// `wrap` declines when the table has NO clustering column — the case where
/// `clustering_key: None` cannot identify a static row (every row has it). CQL
/// forbids a static column there, so this is a fail-closed guard.
#[test]
fn wrap_declines_without_a_clustering_column() {
    let mut schema = static_schema();
    schema.clustering_keys.clear();
    let p = producer(schema);
    let mut inner = Scripted::new(vec![]);
    assert!(StaticMergeSource::wrap(&p, &mut inner).is_none());
}

/// AC1: static + N clustering rows → EXACTLY N rows, each carrying the static value,
/// and NO `ck = null` row. The static increment is handed on as `Suppressed` so the
/// drive loop's per-partition accounting is unchanged.
#[test]
fn static_plus_clustering_rows_injects_and_emits_no_phantom_row() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(1, 10), static_entry(1, 10, "s1")),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "v1")),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 2, "v2")),
            SourceStep::PartitionEnd(key(1, 10)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(1, Some(1), Some("s1"), Some("v1")),
            row(1, Some(2), Some("s1"), Some("v2")),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// AC2: a partition with a live static row and ZERO clustering rows → EXACTLY one
/// row, null clustering + null regular columns, emitted at the partition's end.
#[test]
fn static_only_partition_emits_exactly_one_row_at_partition_end() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(9, 90), static_entry(9, 90, "only")),
            SourceStep::PartitionEnd(key(9, 90)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(9, None, Some("only"), None),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// Issue #3095 B1 (merge-arm half): a partition whose ONLY clustering rows are ROW
/// TOMBSTONES counts as having no rows — Cassandra's `hasNext()` is over the
/// already-filtered iterator — so its static content IS returned, as one row.
#[test]
fn a_partition_whose_only_rows_are_tombstones_still_returns_its_static_row() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(5, 50), static_entry(5, 50, "x")),
            SourceStep::Row(key(5, 50), row_tombstone_entry(5, 50, 1)),
            SourceStep::Row(key(5, 50), row_tombstone_entry(5, 50, 2)),
            SourceStep::PartitionEnd(key(5, 50)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            Observed::Suppressed,
            Observed::Suppressed,
            row(5, None, Some("x"), None),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// Issue #3095 B3 (rust-reviewer BLOCKER): a partition BOUNDARY with NO
/// `PartitionEnd` between the partitions. The deferred step must be re-fed through
/// the FULL adaptation — if it were returned verbatim (the pre-fix fast path), B's
/// static row would surface as a phantom `ck = null` row AND B's clustering row would
/// carry a NULL static.
#[test]
fn a_partition_boundary_without_partition_end_still_adapts_the_next_partition() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            // Partition A: static only, never closed by a PartitionEnd.
            SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
            // Partition B starts immediately.
            SourceStep::Row(key(2, 20), static_entry(2, 20, "sb")),
            SourceStep::Row(key(2, 20), clustering_entry(2, 20, 7, "vb")),
            SourceStep::PartitionEnd(key(2, 20)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            // A's static row is held back…
            Observed::Suppressed,
            // …and flushed at the boundary, before B's first increment.
            row(1, None, Some("sa"), None),
            // B's static row is RECORDED (not emitted verbatim).
            Observed::Suppressed,
            // …and injected into B's clustering row.
            row(2, Some(7), Some("sb"), Some("vb")),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// Issue #3095 B2 (roborev BLOCKER), Flight half: a NORMAL partition followed by a
/// STATIC-ONLY one. Un-reset per-partition state would leave `emitted_clustering_row`
/// set and permanently suppress the later partition's row.
#[test]
fn a_normal_partition_followed_by_a_static_only_partition_still_emits_its_row() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
            SourceStep::PartitionEnd(key(1, 10)),
            SourceStep::Row(key(2, 20), static_entry(2, 20, "sb")),
            SourceStep::PartitionEnd(key(2, 20)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(1, Some(1), Some("sa"), Some("va")),
            Observed::PartitionEnd,
            Observed::Suppressed,
            row(2, None, Some("sb"), None),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// Interleaving pin: each partition's rows carry THEIR OWN static value, never the
/// previous partition's (the leak an un-reset `statics` would cause).
#[test]
fn each_partition_injects_only_its_own_static_value() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
            SourceStep::PartitionEnd(key(1, 10)),
            SourceStep::Row(key(2, 20), static_entry(2, 20, "sb")),
            SourceStep::Row(key(2, 20), clustering_entry(2, 20, 1, "vb")),
            SourceStep::PartitionEnd(key(2, 20)),
            // Partition 3 has NO static row at all: its rows must read NULL for `s`.
            SourceStep::Row(key(3, 30), clustering_entry(3, 30, 1, "vc")),
            SourceStep::PartitionEnd(key(3, 30)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(1, Some(1), Some("sa"), Some("va")),
            Observed::PartitionEnd,
            Observed::Suppressed,
            row(2, Some(1), Some("sb"), Some("vb")),
            Observed::PartitionEnd,
            row(3, Some(1), None, Some("vc")),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// An empty stream completes without emitting anything (and without panicking on the
/// absent partition state).
#[test]
fn an_empty_stream_completes_without_emitting() {
    let p = producer(static_schema());
    assert_eq!(drive(&p, vec![]), vec![Observed::Complete]);
}

/// A source that COMPLETES without a final `PartitionEnd` must still not drop the
/// owed static-only row (the defensive `Complete` flush), and the parked `Complete`
/// must be replayed exactly once.
#[test]
fn complete_without_a_final_partition_end_still_emits_the_owed_static_row() {
    let p = producer(static_schema());
    let observed = drive(
        &p,
        vec![SourceStep::Row(key(9, 90), static_entry(9, 90, "only"))],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(9, None, Some("only"), None),
            Observed::Complete,
        ]
    );
}

/// Issue #3095 B6: a static row arriving AFTER a clustering row of the same partition
/// violates the merger's static-sorts-first invariant, and the rows already emitted
/// carry a NULL static — so it must FAIL LOUDLY, never be silently dropped.
#[test]
fn a_static_row_after_a_clustering_row_is_a_loud_error() {
    let p = producer(static_schema());
    let mut inner = Scripted::new(vec![
        SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "v1")),
        SourceStep::Row(key(1, 10), static_entry(1, 10, "late")),
    ]);
    let mut source = StaticMergeSource::wrap(&p, &mut inner).expect("wrapped");
    assert!(matches!(
        observe(source.next_step().expect("first row")),
        Observed::Row(..)
    ));
    let msg = match source.next_step() {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("an out-of-order static row must be an error, not a silent drop"),
    };
    assert!(
        msg.contains("static row") && msg.contains("AFTER"),
        "the error must name the broken ordering invariant, got: {msg}"
    );
}

/// Issue #3095 B5: a TOKEN-EXCLUDED partition is never materialized — the adapter
/// applies the same token predicate the drive loop does, BEFORE materializing, so the
/// documented lazy-materialization invariant (`row_source::PendingRow`) holds for a
/// static-bearing table too. Observable consequence: the excluded partition produces
/// only `Suppressed` increments and NO static-only row, while an INCLUDED partition
/// in the same stream is adapted normally.
#[test]
fn a_token_excluded_partition_is_never_materialized() {
    // Range `(15, 25]`: token 20 is IN, token 10 is OUT.
    let p = producer_with_token(static_schema(), 15, 25);
    let observed = drive(
        &p,
        vec![
            // EXCLUDED partition: a static row and a clustering row.
            SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
            SourceStep::PartitionEnd(key(1, 10)),
            // INCLUDED partition: static-only, must still emit its one row.
            SourceStep::Row(key(2, 20), static_entry(2, 20, "sb")),
            SourceStep::PartitionEnd(key(2, 20)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            // Neither increment of the excluded partition was materialized…
            Observed::Suppressed,
            Observed::Suppressed,
            // …and no static-only row was synthesized for it.
            Observed::PartitionEnd,
            Observed::Suppressed,
            row(2, None, Some("sb"), None),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// A carrier entry (`clustering_key: None` that materializes to nothing — a
/// range/partition-tombstone carrier) must NOT be mistaken for the static row: it
/// stays `Suppressed` and leaves an already-recorded static row intact.
#[test]
fn a_carrier_entry_does_not_clobber_the_recorded_static_row() {
    let p = producer(static_schema());
    // An empty `RowData::Live` has no live data cell and no live marker, so
    // `entry_to_row` suppresses it — exactly a re-emitted marker carrier.
    let carrier = PendingRow::Merged(Box::new(MergeEntry::new(
        0,
        key(9, 90),
        None,
        1_500,
        RowData::Live { cells: Vec::new() },
    )));
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(9, 90), static_entry(9, 90, "only")),
            SourceStep::Row(key(9, 90), carrier),
            SourceStep::PartitionEnd(key(9, 90)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            Observed::Suppressed,
            row(9, None, Some("only"), None),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// A consumer that stops pulling mid-partition simply stops, and the adapter has NOT
/// read ahead through the rest of the partition — it pulls at most one inner step per
/// output step, so a `LIMIT`/cancel that stops the drive loop stops the merge too.
#[test]
fn a_consumer_that_stops_mid_partition_has_not_read_ahead() {
    let p = producer(static_schema());
    let mut inner = Scripted::new(vec![
        SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
        SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
        SourceStep::Row(key(1, 10), clustering_entry(1, 10, 2, "vb")),
        SourceStep::PartitionEnd(key(1, 10)),
    ]);
    {
        let mut source = StaticMergeSource::wrap(&p, &mut inner).expect("wrapped");
        assert_eq!(
            observe(source.next_step().expect("static")),
            Observed::Suppressed
        );
        assert_eq!(
            observe(source.next_step().expect("first row")),
            row(1, Some(1), Some("sa"), Some("va"))
        );
    }
    assert_eq!(
        inner.pulls, 2,
        "the adapter must pull at most one inner step per output step"
    );
}

/// A static row whose static COLUMN is absent from the materialized row (e.g. its
/// only static cell was a cell tombstone dropped at assembly) records no values and
/// injects nothing — the clustering rows read NULL rather than a stale value.
#[test]
fn a_static_row_without_static_values_injects_nothing() {
    let p = producer(static_schema());
    // A `clustering_key: None` entry whose only cell is a NON-static column: it
    // materializes (so it is the partition's static row) but carries no `s` value.
    let odd = PendingRow::Merged(Box::new(MergeEntry::new(
        0,
        key(1, 10),
        None,
        1_000,
        RowData::Live {
            cells: vec![cell("v", Value::text("not-static"))],
        },
    )));
    let observed = drive(
        &p,
        vec![
            SourceStep::Row(key(1, 10), odd),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
            SourceStep::PartitionEnd(key(1, 10)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(1, Some(1), None, Some("va")),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

/// Sanity: the schema's static column list is read from the SCHEMA (authoritative),
/// so a column named like a static one but not declared static is never injected.
#[test]
fn only_schema_declared_static_columns_are_injected() {
    let mut schema = static_schema();
    for c in &mut schema.columns {
        if c.name == "s" {
            c.is_static = true;
        }
    }
    let p = producer(schema);
    let observed = drive(
        &p,
        vec![
            // The static row also carries a `v` cell: `v` is NOT static, so it must
            // not be propagated into the clustering row.
            SourceStep::Row(
                key(1, 10),
                PendingRow::Merged(Box::new(MergeEntry::new(
                    0,
                    key(1, 10),
                    None,
                    1_000,
                    RowData::Live {
                        cells: vec![cell("s", Value::text("sa")), cell("v", Value::text("leak"))],
                    },
                ))),
            ),
            SourceStep::Row(key(1, 10), clustering_entry(1, 10, 1, "va")),
            SourceStep::PartitionEnd(key(1, 10)),
        ],
    );
    assert_eq!(
        observed,
        vec![
            Observed::Suppressed,
            row(1, Some(1), Some("sa"), Some("va")),
            Observed::PartitionEnd,
            Observed::Complete,
        ]
    );
}

// ---------------------------------------------------------------------------
// Interaction with #3106's fail-closed producer death
// ---------------------------------------------------------------------------

/// Issue #3106 × #3095: a producer that dies MID-PARTITION while this adapter is
/// holding a static row back must surface the ERROR — the held row must NOT be flushed
/// as if the partition had ended.
///
/// #3106 made a producer death fatal precisely so a TRUNCATED result set can never be
/// reported as success. The static adapter sits between the source and the drive loop
/// and holds a row back across steps, so it is exactly the shape that could re-open
/// that hole by treating "the source stopped" as "the partition ended". It must not:
/// the partition's row set is incomplete, so emitting its static-only row would be a
/// fabricated answer, and the error is the only correct outcome.
#[test]
fn a_producer_death_while_a_static_row_is_held_surfaces_the_error() {
    let p = producer(static_schema());
    // Step 0: the static row (held back). Step 1: the producer dies.
    let mut inner = Scripted::failing_after(
        vec![SourceStep::Row(key(9, 90), static_entry(9, 90, "held"))],
        1,
    );
    let mut source = StaticMergeSource::wrap(&p, &mut inner).expect("wrapped");
    assert_eq!(
        observe(source.next_step().expect("the static row is recorded")),
        Observed::Suppressed,
        "the static row is held back, not emitted"
    );
    let msg = match source.next_step() {
        Err(e) => format!("{e}"),
        Ok(step) => panic!(
            "a producer death must surface as an ERROR, not as the held static row or a \
             clean end of stream — got {:?} (this would re-open #3106's truncation hole)",
            observe(step)
        ),
    };
    assert!(
        msg.contains("#3106") || msg.contains("Done sentinel"),
        "the producer's own terminal error must propagate unchanged, got: {msg}"
    );
}

/// The same guarantee at a PARTITION BOUNDARY: the previous partition's owed
/// static-only row is emitted first (it IS complete), and the death then surfaces on
/// the following pull rather than being masked by that emission.
#[test]
fn a_producer_death_after_a_completed_partition_still_surfaces() {
    let p = producer(static_schema());
    // Partition A's static row, then partition B's first row (which rotates A out and
    // flushes A's owed row), then death.
    let mut inner = Scripted::failing_after(
        vec![
            SourceStep::Row(key(1, 10), static_entry(1, 10, "sa")),
            SourceStep::Row(key(2, 20), static_entry(2, 20, "sb")),
        ],
        2,
    );
    let mut source = StaticMergeSource::wrap(&p, &mut inner).expect("wrapped");
    assert_eq!(
        observe(source.next_step().expect("A's static row recorded")),
        Observed::Suppressed
    );
    // A is complete (B started), so A's static-only row is legitimately emitted.
    assert_eq!(
        observe(source.next_step().expect("A's owed row")),
        row(1, None, Some("sa"), None)
    );
    // The deferred B input is re-adapted next, recording B's static row…
    assert_eq!(
        observe(source.next_step().expect("B's static row recorded")),
        Observed::Suppressed
    );
    // …and THEN the producer death surfaces. B's held row is NOT flushed.
    match source.next_step() {
        Err(e) => assert!(
            format!("{e}").contains("#3106") || format!("{e}").contains("Done sentinel"),
            "expected the producer's terminal error, got: {e}"
        ),
        Ok(step) => panic!(
            "the death must surface; B's partition never completed so its static row \
             must not be emitted — got {:?}",
            observe(step)
        ),
    }
}
