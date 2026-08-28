//! Benchmark scenarios and arms for the DataFusion spike (issue #2605).
//!
//! # The three numbers this produces, and why they must stay separate
//!
//! `docs/architecture/throughput-program-2026-07.md` (M15 item 1) requires TWO
//! deltas reported separately, never blended into one ratio:
//!
//! 1. **decode-to-column** — what the current pipeline pays to go SSTable bytes →
//!    row → Arrow columns, i.e. the cost a COLUMNAR PRODUCER would remove. Read
//!    from the existing `#2819` sub-phase instrument: `stream_encode` is the
//!    row→column transpose, `stream_merge` the row materialize, `stream_decompress`
//!    the chunk decompression. The spike invents no new timing.
//! 2. **vectorized-exec** — [`ArmKind::RowEngine`] vs [`ArmKind::DataFusion`]
//!    over the SAME batches.
//! 3. **the shared batch-production floor** — [`ArmKind::Floor`]: stream batches
//!    and discard. This is the ceiling ANY execution engine is capped by, and it
//!    is what makes (1) and (2) separable instead of a single blended ratio.
//!
//! [`ArmKind::RowPushdown`] is a fourth, reference arm: the real production
//! `do_get` shape, with projection and predicate pushed into the `ScanSpec`. Its
//! batches DIFFER from the other arms' by construction, so it is reported beside
//! them and never compared row-for-row against them.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use cqlite_core::schema::TableSchema;
use serde::Serialize;

use crate::df_spike::provider::{CqliteTableProvider, SpikeError};
use crate::df_spike::rowwise::{self, RowLiteral, RowOp};
use crate::df_spike::rss::RssSampler;
use crate::df_spike::scan::{self, ScanOutcome, ScanTarget};
use crate::filter::ScanSpec;
use crate::ticket::{PredicateExpr, PredicateOp};

/// Which query shape is being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioKind {
    /// Scenario 1: `SELECT count(*) FROM t` over the whole table.
    FullScanCount,
    /// Scenario 2: `SELECT <2-3 columns> FROM t` — a projected scan, drained.
    ProjectedScan,
    /// Scenario 3: `SELECT count(*) FROM t WHERE <col> <op> <literal>`.
    FilteredScan,
}

impl ScenarioKind {
    /// Every scenario, in report order.
    pub fn all() -> [Self; 3] {
        [Self::FullScanCount, Self::ProjectedScan, Self::FilteredScan]
    }

    /// Stable identifier for the results file.
    pub fn id(self) -> &'static str {
        match self {
            Self::FullScanCount => "full_scan_count",
            Self::ProjectedScan => "projected_scan",
            Self::FilteredScan => "filtered_scan",
        }
    }

    /// Parse a CLI spelling.
    pub fn parse(raw: &str) -> Option<Self> {
        Self::all().into_iter().find(|s| s.id() == raw)
    }

    /// Whether this scenario's SQL returns a SCALAR aggregate (`count(*)`)
    /// rather than a row stream.
    ///
    /// The DataFusion arm reads its `rows_result` from the scalar when — and
    /// ONLY when — this is true. Detecting "scalar" from the batch SHAPE instead
    /// (one row, one column) is wrong for a projected scan: at
    /// `--batch-size 1` its first batch is 1x1 too, and the harness would report
    /// the first projected VALUE as a row count.
    pub fn is_scalar_aggregate(self) -> bool {
        match self {
            Self::FullScanCount | Self::FilteredScan => true,
            Self::ProjectedScan => false,
        }
    }
}

/// Which execution arm is being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArmKind {
    /// Stream batches and discard — the shared batch-production floor.
    Floor,
    /// Row-at-a-time evaluation over the produced batches.
    RowEngine,
    /// DataFusion vectorized execution over the SAME produced batches.
    DataFusion,
    /// Reference arm: production `do_get` shape, projection + predicate pushed
    /// into the scan. Its batches differ from the other arms' on purpose.
    RowPushdown,
}

impl ArmKind {
    /// Every arm, in report order.
    pub fn all() -> [Self; 4] {
        [
            Self::Floor,
            Self::RowEngine,
            Self::DataFusion,
            Self::RowPushdown,
        ]
    }

    /// Stable identifier for the results file.
    pub fn id(self) -> &'static str {
        match self {
            Self::Floor => "floor",
            Self::RowEngine => "row_engine",
            Self::DataFusion => "datafusion",
            Self::RowPushdown => "row_pushdown",
        }
    }

    /// Parse a CLI spelling.
    pub fn parse(raw: &str) -> Option<Self> {
        Self::all().into_iter().find(|a| a.id() == raw)
    }
}

/// One scenario/arm pairing to run.
#[derive(Debug, Clone, Copy)]
pub struct Scenario {
    /// Query shape.
    pub kind: ScenarioKind,
    /// Execution arm.
    pub arm: ArmKind,
}

/// Everything the runner needs.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Table directory holding the `*-Data.db` components.
    pub dir: PathBuf,
    /// Authoritative table schema (parsed from the corpus DDL).
    pub schema: TableSchema,
    /// Rows per Arrow batch (production default 8192).
    pub batch_size: usize,
    /// Columns for the projected-scan scenario.
    pub projection: Vec<String>,
    /// Filter column for the filtered scenario.
    pub filter_column: String,
    /// Filter operator.
    pub filter_op: RowOp,
    /// Filter operand.
    pub filter_value: RowLiteral,
    /// Iterations per (scenario, arm).
    pub iterations: usize,
    /// DataFusion `target_partitions`, or `None` for DataFusion's default (one
    /// per core).
    ///
    /// **This knob exists because the first measurement was misleading and had
    /// to be corrected.** With DataFusion's default parallelism the DataFusion
    /// arm ran ~1.6x faster than the row/floor arms over IDENTICAL batches — not
    /// because its kernels are faster, but because its multi-threaded runtime
    /// drains and DROPS the wide batches on several worker threads while the
    /// single-threaded direct arms do recv-and-drop serially behind a
    /// 2-slot channel. That is PIPELINE CONCURRENCY, not vectorization, and
    /// reporting it as a vectorized-exec delta would have been wrong. Pinning
    /// `target_partitions = 1` equalises the thread count so the residual delta
    /// is attributable to execution; the default-parallelism figure is reported
    /// SEPARATELY as the concurrency effect, which is real and worth knowing.
    pub df_target_partitions: Option<usize>,
}

/// One measured run.
#[derive(Debug, Clone, Serialize)]
pub struct BenchOutcome {
    /// Query shape.
    pub scenario: ScenarioKind,
    /// Execution arm.
    pub arm: ArmKind,
    /// 1-based iteration index.
    pub iteration: usize,
    /// Wall time of the whole run, nanoseconds.
    pub elapsed_nanos: u64,
    /// Rows the producer emitted (the scan's input row count).
    pub rows_scanned: u64,
    /// Rows the query RESULT accounts for (a count value, or drained rows).
    pub rows_result: u64,
    /// Batches the producer emitted.
    pub batches: u64,
    /// Post-prune `*-Data.db` count reconciled by this scan.
    pub sources: usize,
    /// Whether the k-way MERGE arm demonstrably ran (see `read_path_probe`).
    pub merge_arm_observed: bool,
    /// Compaction-reconciler entries observed during this scan.
    pub reconcile_entries: u64,
    /// Per-row cell-metadata maps allocated during this scan (merge arm only).
    pub cell_metadata_maps: u64,
    /// Sub-phase: cold body-chunk page-in, nanoseconds.
    pub subphase_cold_fault_nanos: u64,
    /// Sub-phase: chunk decompression, nanoseconds.
    pub subphase_decompress_nanos: u64,
    /// Sub-phase: merge + reconcile + row materialize, nanoseconds.
    pub subphase_merge_nanos: u64,
    /// Sub-phase: Arrow array build — the row→column TRANSPOSE, nanoseconds.
    /// This is the decode-to-column figure.
    pub subphase_encode_nanos: u64,
    /// Sub-phase: channel send incl. backpressure park, nanoseconds.
    pub subphase_grpc_write_nanos: u64,
    /// Peak RSS during this run, bytes. `None` when RSS could not be measured —
    /// never a fabricated zero.
    pub peak_rss_bytes: Option<u64>,
    /// Whether projection/predicate were pushed into the scan for this arm.
    pub pushdown: bool,
    /// DataFusion `target_partitions` in force for this run (`None` for the
    /// direct arms, which have no DataFusion runtime).
    pub df_target_partitions: Option<usize>,
    /// The query text the DataFusion arm ran, when applicable.
    pub sql: Option<String>,
}

/// Errors from the harness.
#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    /// Provider/scan setup failed.
    #[error(transparent)]
    Spike(#[from] SpikeError),
    /// The producer failed mid-scan.
    #[error("scan failed: {0}")]
    Producer(#[from] crate::producer::ProducerError),
    /// Row-wise evaluation failed.
    #[error("row-wise arm failed: {0}")]
    RowWise(String),
    /// DataFusion failed.
    #[error("datafusion failed: {0}")]
    DataFusion(String),
    /// A run finished but produced no measurements to report. Fail closed: a
    /// benchmark that cannot measure must say so, not report zeros.
    #[error("{0}: the scan produced no measurement record")]
    NotMeasured(&'static str),
    /// A precondition for a meaningful comparison was violated.
    #[error("precondition violated: {0}")]
    Precondition(String),
    /// A Tokio runtime could not be built for the DataFusion arm.
    #[error("could not build a runtime for the DataFusion arm: {0}")]
    Runtime(String),
}

/// Runs the scenario/arm matrix.
pub struct BenchRunner {
    config: BenchConfig,
}

impl BenchRunner {
    /// Build a runner.
    pub fn new(config: BenchConfig) -> Self {
        Self { config }
    }

    /// The scan target implied by the config.
    fn target(&self) -> ScanTarget {
        ScanTarget {
            schema: self.config.schema.clone(),
            dir: self.config.dir.clone(),
            batch_size: self.config.batch_size,
        }
    }

    /// Run one (scenario, arm) pairing once.
    pub fn run_one(
        &self,
        scenario: Scenario,
        iteration: usize,
    ) -> Result<BenchOutcome, BenchError> {
        match scenario.arm {
            ArmKind::DataFusion => self.run_datafusion(scenario.kind, iteration),
            ArmKind::Floor | ArmKind::RowEngine | ArmKind::RowPushdown => {
                self.run_direct(scenario, iteration)
            }
        }
    }

    /// The `Floor`, `RowEngine` and `RowPushdown` arms: drive the producer
    /// directly and consume the batches synchronously.
    fn run_direct(&self, scenario: Scenario, iteration: usize) -> Result<BenchOutcome, BenchError> {
        let target = self.target();
        let pushdown = scenario.arm == ArmKind::RowPushdown;
        let spec = if pushdown {
            self.pushdown_spec(scenario.kind)?
        } else {
            ScanSpec::default()
        };
        let producer = Arc::new(scan::build_producer(&target, spec)?);
        let paths = scan::resolve_paths(&producer, &target)?;
        if paths.is_empty() {
            return Err(SpikeError::NoSources(target.dir.clone()).into());
        }

        let rss = RssSampler::start();
        let started = Instant::now();
        scan::note_scan_started();
        let running = scan::spawn_scan(producer, paths);
        let sources = running.sources;
        let mut batches = running.batches;
        let mut rows_result: u64 = 0;

        while let Some(item) = batches.blocking_recv() {
            let batch = item?;
            rows_result = rows_result.saturating_add(self.consume_direct(scenario, &batch)?);
        }
        let elapsed = started.elapsed();
        let outcome = running
            .done
            .join()
            .map_err(|_| BenchError::NotMeasured("producer thread panicked"))?;
        let peak_rss = rss.finish();
        if let Err(e) = &outcome.result {
            return Err(BenchError::Precondition(format!(
                "scan did not complete cleanly: {e}"
            )));
        }

        Ok(self.assemble(
            scenario,
            iteration,
            elapsed,
            rows_result,
            sources,
            &outcome,
            peak_rss,
            pushdown,
            None,
        ))
    }

    /// Per-batch consumption for a direct arm.
    fn consume_direct(&self, scenario: Scenario, batch: &RecordBatch) -> Result<u64, BenchError> {
        match (scenario.arm, scenario.kind) {
            // The floor deliberately touches nothing: it is the production
            // ceiling, not an execution measurement.
            (ArmKind::Floor, _) => Ok(0),
            // The production reference arm counts what the scan emitted; the
            // projection/predicate were already applied inside the scan.
            (ArmKind::RowPushdown, _) => Ok(batch.num_rows() as u64),
            (ArmKind::RowEngine, ScenarioKind::FullScanCount) => {
                Ok(rowwise::count_rows_rowwise(batch))
            }
            (ArmKind::RowEngine, ScenarioKind::ProjectedScan) => {
                let projected = self.project(batch)?;
                Ok(rowwise::count_rows_rowwise(&projected))
            }
            (ArmKind::RowEngine, ScenarioKind::FilteredScan) => rowwise::count_matching_rowwise(
                batch,
                &self.config.filter_column,
                self.config.filter_op,
                &self.config.filter_value,
            )
            .map_err(|e| BenchError::RowWise(e.to_string())),
            (ArmKind::DataFusion, _) => Err(BenchError::Precondition(
                "the DataFusion arm does not use the direct consumer".to_string(),
            )),
        }
    }

    /// Post-scan column selection for the projected scenario — the same
    /// `RecordBatch::project` the provider performs with pushdown off, so the
    /// arms stay comparable.
    fn project(&self, batch: &RecordBatch) -> Result<RecordBatch, BenchError> {
        let schema = batch.schema();
        let mut indices = Vec::with_capacity(self.config.projection.len());
        for name in &self.config.projection {
            let index = schema.index_of(name).map_err(|_| {
                BenchError::Precondition(format!("projected column '{name}' is not in the schema"))
            })?;
            indices.push(index);
        }
        batch
            .project(&indices)
            .map_err(|e| BenchError::RowWise(e.to_string()))
    }

    /// The `ScanSpec` the production reference arm pushes for a scenario.
    fn pushdown_spec(&self, kind: ScenarioKind) -> Result<ScanSpec, BenchError> {
        let mut spec = ScanSpec::default();
        match kind {
            ScenarioKind::FullScanCount => {}
            ScenarioKind::ProjectedScan => {
                spec.projection = Some(self.config.projection.clone());
            }
            ScenarioKind::FilteredScan => {
                let candidate = predicate_for(
                    &self.config.filter_column,
                    self.config.filter_op,
                    self.config.filter_value.json(),
                );
                let lowered = crate::df_spike::pushdown::lower(&candidate, &self.config.schema)
                    .map_err(|e| BenchError::Precondition(e.to_string()))?;
                spec.filter = Some(lowered);
            }
        }
        Ok(spec)
    }

    /// The DataFusion arm: register the provider and run the scenario as SQL.
    ///
    /// Pushdown is OFF, so the provider produces exactly the batches the direct
    /// arms produce and DataFusion does the projection/filter/aggregation with
    /// its vectorized kernels — which is what makes this a measurement of
    /// EXECUTION and not of scan narrowing.
    fn run_datafusion(
        &self,
        kind: ScenarioKind,
        iteration: usize,
    ) -> Result<BenchOutcome, BenchError> {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures::StreamExt;

        let provider = Arc::new(CqliteTableProvider::open(
            self.config.schema.clone(),
            self.config.dir.clone(),
            self.config.batch_size,
            false,
        )?);
        let sql = self.sql_for(kind);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| BenchError::Runtime(e.to_string()))?;

        let rss = RssSampler::start();
        let started = Instant::now();
        let provider_for_query = provider.clone();
        let sql_for_query = sql.clone();
        let target_partitions = self.config.df_target_partitions;
        // Decided from the SCENARIO (the query we asked for), never from the
        // shape of a batch that came back. See `is_scalar_aggregate`.
        let scalar_result = kind.is_scalar_aggregate();
        let rows_result = runtime.block_on(async move {
            let ctx = match target_partitions {
                Some(n) => SessionContext::new_with_config(
                    SessionConfig::new().with_target_partitions(n.max(1)),
                ),
                None => SessionContext::new(),
            };
            ctx.register_table("t", provider_for_query)
                .map_err(|e| BenchError::DataFusion(e.to_string()))?;
            let frame = ctx
                .sql(&sql_for_query)
                .await
                .map_err(|e| BenchError::DataFusion(e.to_string()))?;
            let mut stream = frame
                .execute_stream()
                .await
                .map_err(|e| BenchError::DataFusion(e.to_string()))?;
            let mut collected: Vec<RecordBatch> = Vec::new();
            let mut drained: u64 = 0;
            while let Some(next) = stream.next().await {
                let batch = next.map_err(|e| BenchError::DataFusion(e.to_string()))?;
                drained = drained.saturating_add(batch.num_rows() as u64);
                // Only a scalar AGGREGATE result is retained, and only for the
                // `count(*)` scenarios; a projected scan's batches are dropped as
                // they arrive so the arm stays inside the stated resident bound
                // AND so a 1x1 projected batch can never be mistaken for a count.
                if scalar_result
                    && collected.is_empty()
                    && batch.num_rows() == 1
                    && batch.num_columns() == 1
                {
                    collected.push(batch);
                }
            }
            Ok::<u64, BenchError>(if scalar_result {
                scalar_or_drained(&collected, drained)
            } else {
                drained
            })
        })?;
        let elapsed = started.elapsed();
        let peak_rss = rss.finish();

        let outcome = provider
            .last_scan_outcome()
            .ok_or(BenchError::NotMeasured("datafusion arm"))?;
        if let Err(e) = &outcome.result {
            return Err(BenchError::Precondition(format!(
                "scan did not complete cleanly: {e}"
            )));
        }
        let sources = provider.source_count();

        Ok(self.assemble(
            Scenario {
                kind,
                arm: ArmKind::DataFusion,
            },
            iteration,
            elapsed,
            rows_result,
            sources,
            &outcome,
            peak_rss,
            false,
            Some(sql),
        ))
    }

    /// The SQL text for a scenario.
    pub(crate) fn sql_for(&self, kind: ScenarioKind) -> String {
        match kind {
            ScenarioKind::FullScanCount => "SELECT count(*) FROM t".to_string(),
            ScenarioKind::ProjectedScan => {
                let columns = self
                    .config
                    .projection
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SELECT {columns} FROM t")
            }
            ScenarioKind::FilteredScan => format!(
                "SELECT count(*) FROM t WHERE \"{}\" {} {}",
                self.config.filter_column,
                self.config.filter_op.sql(),
                self.config.filter_value.sql()
            ),
        }
    }

    /// Assemble the outcome record from the measured parts.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        &self,
        scenario: Scenario,
        iteration: usize,
        elapsed: std::time::Duration,
        rows_result: u64,
        sources: usize,
        outcome: &ScanOutcome,
        peak_rss_bytes: Option<u64>,
        pushdown: bool,
        sql: Option<String>,
    ) -> BenchOutcome {
        BenchOutcome {
            scenario: scenario.kind,
            arm: scenario.arm,
            iteration,
            elapsed_nanos: elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            rows_scanned: outcome.rows,
            rows_result,
            batches: outcome.batches,
            sources,
            merge_arm_observed: outcome.probe.merge_arm_observed(),
            reconcile_entries: outcome.probe.reconcile_entries,
            cell_metadata_maps: outcome.probe.cell_metadata_maps,
            subphase_cold_fault_nanos: outcome.subphase.cold_fault,
            subphase_decompress_nanos: outcome.subphase.decompress,
            subphase_merge_nanos: outcome.subphase.merge,
            subphase_encode_nanos: outcome.subphase.encode,
            subphase_grpc_write_nanos: outcome.subphase.grpc_write,
            peak_rss_bytes,
            pushdown,
            df_target_partitions: match scenario.arm {
                ArmKind::DataFusion => Some(self.effective_df_partitions()),
                _ => None,
            },
            sql,
        }
    }

    /// The `target_partitions` a DataFusion run will actually use: the explicit
    /// pin, else DataFusion's default of one per available core. Resolved (rather
    /// than recorded as "default") so a results file read on another machine
    /// still says how much parallelism the number was produced with.
    pub(crate) fn effective_df_partitions(&self) -> usize {
        self.config.df_target_partitions.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
    }
}

/// A single-cell `Int64` result (a `count(*)`) as the result row count, else the
/// number of rows drained.
///
/// Only called for a scenario whose SQL IS a scalar aggregate
/// ([`ScenarioKind::is_scalar_aggregate`]) — the caller decides that from the
/// query, not from the batch shape.
fn scalar_or_drained(collected: &[RecordBatch], drained: u64) -> u64 {
    let Some(batch) = collected.first() else {
        return drained;
    };
    let Some(array) = batch.column(0).as_any().downcast_ref::<Int64Array>() else {
        return drained;
    };
    if array.len() == 1 && !array.is_null(0) {
        // A negative count is impossible; refuse to report one rather than wrap.
        u64::try_from(array.value(0)).unwrap_or(drained)
    } else {
        drained
    }
}

/// The ticket predicate for `column op value`.
///
/// `<>` has no ticket operator of its own, so it becomes `NOT (column = value)`
/// — a NEGATION, never a silent substitution of `=`. Substituting `=` here would
/// make the production reference arm count the complement of the rows the other
/// arms count, and the harness would report the two as comparable.
fn predicate_for(column: &str, op: RowOp, value: serde_json::Value) -> PredicateExpr {
    let equality = |op| PredicateExpr::Compare {
        column: column.to_string(),
        op,
        value: value.clone(),
    };
    match op {
        RowOp::Eq => equality(PredicateOp::Equal),
        RowOp::NotEq => PredicateExpr::Not {
            expr: Box::new(equality(PredicateOp::Equal)),
        },
        RowOp::Lt => equality(PredicateOp::Lt),
        RowOp::LtEq => equality(PredicateOp::Lte),
        RowOp::Gt => equality(PredicateOp::Gt),
        RowOp::GtEq => equality(PredicateOp::Gte),
    }
}
