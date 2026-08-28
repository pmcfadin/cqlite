//! The spike's `TableProvider` (issue #2605).
//!
//! Thin by design: it owns the table's decode contract (a `TableSchema` parsed
//! from the corpus DDL), the resolved SSTable path set, and the translation from
//! DataFusion's projection/filter/limit into the EXISTING
//! [`crate::filter::ScanSpec`]. It adds no decode logic of its own.
//!
//! # Pushdown is a switch, and the benchmark's default is OFF
//!
//! With pushdown ON the provider narrows the scan itself (fewer columns
//! materialized, rows dropped before Arrow encode) — which is what a production
//! integration would want, but which makes the two benchmark arms read DIFFERENT
//! batches: the DataFusion arm would look faster because it did less work, not
//! because vectorized execution is faster. The `vectorized-exec delta` the
//! throughput program asks for is only meaningful over identical batches, so the
//! bench runs pushdown OFF for the headline comparison and reports the
//! pushdown-ON figure separately as "what pushdown buys".
//!
//! With pushdown OFF the provider still applies DataFusion's projection, as a
//! post-scan [`arrow::record_batch::RecordBatch::project`] column selection (a
//! pointer copy, not a decode change) — the plan's declared schema must match
//! the batches it emits, and DataFusion is entitled to rely on that.

use std::any::Any;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use cqlite_core::schema::TableSchema;

use crate::df_spike::exec::CqliteScanExec;
use crate::df_spike::pushdown;
use crate::df_spike::scan::{self, ScanOutcome, ScanTarget};
use crate::filter::ScanSpec;

/// Where a plan publishes its finished scan's measurements. `Option` inside
/// because the plan takes the record out once read — a benchmark must never read
/// the same measurement twice and treat it as two runs.
pub type OutcomeSlot = Arc<Mutex<Option<ScanOutcome>>>;

/// Errors building a spike provider.
#[derive(Debug, thiserror::Error)]
pub enum SpikeError {
    /// The merge producer could not be constructed, or path resolution failed.
    #[error("cqlite scan setup failed: {0}")]
    Scan(#[from] crate::producer::ProducerError),
    /// A pushed predicate failed to lower against the table schema.
    #[error("predicate lowering failed: {0}")]
    Filter(#[from] crate::filter::FilterError),
    /// The table directory holds no `*-Data.db` files, so there is nothing to
    /// measure. Fail closed: a zero-source scan would report a vacuous 0-row
    /// benchmark result that looks like a successful run.
    #[error("no SSTables found under {0}")]
    NoSources(PathBuf),
}

/// A DataFusion table over one CQLite table's SSTables.
#[derive(Debug)]
pub struct CqliteTableProvider {
    /// What to scan and how to decode it.
    target: ScanTarget,
    /// Post-prune `*-Data.db` paths, resolved ONCE here (synchronously) so no
    /// later step performs blocking discovery or builds a nested Tokio runtime.
    paths: Arc<Vec<PathBuf>>,
    /// The full (unprojected) Arrow schema clients see.
    schema: SchemaRef,
    /// Whether projection/filter/limit are pushed into the scan.
    pushdown_enabled: bool,
    /// Slot the most recently finished plan publishes its measurements into.
    last_outcome: Arc<Mutex<Option<OutcomeSlot>>>,
}

impl CqliteTableProvider {
    /// Open the table directory `dir` with the decode contract `schema`.
    ///
    /// Performs the blocking path discovery up front and refuses an empty source
    /// set, so a mis-pointed corpus path fails loudly instead of yielding a
    /// 0-row "success".
    pub fn open(
        schema: TableSchema,
        dir: PathBuf,
        batch_size: usize,
        pushdown_enabled: bool,
    ) -> Result<Self, SpikeError> {
        let target = ScanTarget {
            schema,
            dir: dir.clone(),
            batch_size,
        };
        let probe = scan::build_producer(&target, ScanSpec::default())?;
        let paths = scan::resolve_paths(&probe, &target)?;
        if paths.is_empty() {
            return Err(SpikeError::NoSources(dir));
        }
        let arrow_schema = probe.arrow_schema()?;
        Ok(Self {
            target,
            paths: Arc::new(paths),
            schema: Arc::new(arrow_schema),
            pushdown_enabled,
            last_outcome: Arc::new(Mutex::new(None)),
        })
    }

    /// Post-prune source count — the authoritative "how many generations does
    /// this scan reconcile" figure the harness asserts on.
    pub fn source_count(&self) -> usize {
        self.paths.len()
    }

    /// The resolved SSTable paths.
    pub fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    /// Whether pushdown is enabled on this provider.
    pub fn pushdown_enabled(&self) -> bool {
        self.pushdown_enabled
    }

    /// The table's decode contract.
    pub fn table_schema(&self) -> &TableSchema {
        &self.target.schema
    }

    /// Measurements of the most recent COMPLETED scan issued through this
    /// provider, or `None` if none has completed. Never a fabricated zero.
    pub fn last_scan_outcome(&self) -> Option<ScanOutcome> {
        let slot = {
            // Release the outer lock before taking the inner one, so the two are
            // never held at once (and no temporary outlives its guard).
            let guard = self.last_outcome.lock().ok()?;
            guard.clone()?
        };
        let mut inner = slot.lock().ok()?;
        inner.take()
    }

    /// Build the `ScanSpec` for a request, honouring the pushdown switch.
    ///
    /// Returns the spec, the producer's own column names (so a post-scan
    /// projection can be mapped), and a description of what was pushed.
    fn spec_for(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(ScanSpec, Option<Vec<usize>>, String), SpikeError> {
        if !self.pushdown_enabled {
            // Nothing narrows the scan; the projection is applied post-scan.
            let described = "none (pushdown disabled)".to_string();
            return Ok((ScanSpec::default(), projection.cloned(), described));
        }

        let mut spec = ScanSpec::default();
        let mut described: Vec<String> = Vec::new();

        // An EMPTY projection is `count(*)`, and it must NEVER be pushed.
        //
        // DataFusion asks a `TableProvider` for zero columns when the query needs
        // only a row count. Forwarding `Some(vec![])` into `ScanSpec.projection`
        // makes the producer emit ZERO-COLUMN batches, which carry no explicit
        // row count — so the rows vanish and `count(*)` returns 0. A benchmark
        // arm that answers 0 instantly is the worst possible failure mode: fast
        // AND wrong.
        //
        // Instead the scan is narrowed to ONE column (the first partition key,
        // which every table has and every row populates) and the empty
        // projection is applied AFTER production, where `RecordBatch::project`
        // carries `row_count` through explicitly. The scan stays narrow — the
        // point of pushdown — and the row count survives.
        let mut post_projection: Option<Vec<usize>> = None;
        if let Some(indices) = projection {
            if indices.is_empty() {
                let anchor = self.count_anchor_column()?;
                described.push(format!("projection={anchor} (count-only anchor)"));
                spec.projection = Some(vec![anchor]);
                // Against the producer's emitted columns, not the table schema:
                // zero columns selected out of the one column produced.
                post_projection = Some(Vec::new());
            } else {
                let names = self.projected_names(indices)?;
                described.push(format!("projection={}", names.join(",")));
                spec.projection = Some(names);
            }
        }

        if let Some(candidate) = pushdown::translate_all(filters, &self.target.schema) {
            spec.filter = Some(pushdown::lower(&candidate, &self.target.schema)?);
            described.push(format!("filters={}", filters.len()));
        }

        if let Some(limit) = limit {
            spec.limit = Some(limit as u64);
            described.push(format!("limit={limit}"));
        }

        if described.is_empty() {
            described.push("none".to_string());
        }
        // With the projection pushed into the scan, the producer already emits
        // exactly the output columns — hence no post-scan projection, EXCEPT for
        // the `count(*)` anchor above, which must be projected away afterwards.
        Ok((spec, post_projection, described.join(" ")))
    }

    /// The single column a `count(*)` scan is narrowed to.
    ///
    /// The first partition key: it is declared on every table, it is present in
    /// every row (a row cannot exist without its key), and it is typically the
    /// narrowest column on the table — so it is the cheapest column that still
    /// makes the producer emit one row per row.
    fn count_anchor_column(&self) -> Result<String, SpikeError> {
        self.target
            .schema
            .partition_keys
            .first()
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                SpikeError::Scan(crate::producer::ProducerError::Merge(
                    cqlite_core::Error::Internal(
                        "table schema declares no partition key, so a count(*) scan cannot be \
                         anchored to a column"
                            .to_string(),
                    ),
                ))
            })
    }

    /// Column names for DataFusion's projection indices, against the FULL schema.
    fn projected_names(&self, indices: &[usize]) -> Result<Vec<String>, SpikeError> {
        let fields = self.schema.fields();
        indices
            .iter()
            .map(|i| {
                fields.get(*i).map(|f| f.name().clone()).ok_or_else(|| {
                    SpikeError::Scan(crate::producer::ProducerError::Merge(
                        cqlite_core::Error::Internal(format!(
                            "projection index {i} is outside the {}-column schema",
                            fields.len()
                        )),
                    ))
                })
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for CqliteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `Exact` ONLY for filters [`pushdown::classify`] can translate AND lower
    /// against the schema, and only when pushdown is enabled. Anything else is
    /// `Unsupported` and stays in DataFusion's `FilterExec` — claiming `Exact`
    /// for a predicate the scan does not apply would silently return rows that
    /// should have been filtered out.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        if !self.pushdown_enabled {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        Ok(pushdown::classify(filters, &self.target.schema))
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (spec, post_projection, pushed) = self
            .spec_for(projection, filters, limit)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let producer = scan::build_producer(&self.target, spec)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let producer_schema: SchemaRef = Arc::new(
            producer
                .arrow_schema()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );

        let exec = CqliteScanExec::try_new(
            Arc::new(producer),
            self.paths.clone(),
            producer_schema,
            post_projection,
            pushed,
        )?;
        // Remember where this plan will publish its measurements so the harness
        // can read them back after collecting the result.
        if let Ok(mut slot) = self.last_outcome.lock() {
            *slot = Some(exec.outcome_slot());
        }
        Ok(Arc::new(exec))
    }
}
