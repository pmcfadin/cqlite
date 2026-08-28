//! The spike's `ExecutionPlan`: a DataFusion scan node over the existing
//! CQLite merge producer (issue #2605).
//!
//! One partition, one merge. The plan holds the ALREADY-RESOLVED post-prune
//! `*-Data.db` paths (resolved synchronously when the provider was opened) so
//! that [`ExecutionPlan::execute`] — which DataFusion calls from inside its own
//! Tokio runtime — performs no blocking path discovery and never constructs a
//! nested runtime.
//!
//! Batches arrive from [`crate::df_spike::scan::spawn_scan`], the same seam the
//! row-engine arm of the benchmark consumes, so the two arms are compared over
//! IDENTICAL batches by construction rather than by coincidence.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DfResult, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::df_spike::scan::{self, ScanOutcome};
use crate::producer::MergeProducer;

/// A single-partition scan of one CQLite table through the Flight merge producer.
///
/// `Debug` is hand-written: `MergeProducer` is not `Debug` (it holds a
/// `TableSchema`, an optional `UdtRegistry` and an aggregation plan), and
/// DataFusion's `ExecutionPlan` requires `Debug`. Printing the plan's SHAPE is
/// what `EXPLAIN` needs; dumping the producer's internals is not.
pub struct CqliteScanExec {
    /// Configured producer (projection/filter pushdown already applied to its
    /// `ScanSpec`, if the provider was built with pushdown enabled).
    producer: Arc<MergeProducer>,
    /// Post-prune `*-Data.db` paths, resolved once at provider construction.
    paths: Arc<Vec<std::path::PathBuf>>,
    /// The schema this plan emits — the producer's schema, further projected when
    /// the projection was NOT pushed into the scan.
    schema: SchemaRef,
    /// Column indices to select from each produced batch, or `None` when the
    /// batches already have exactly the output shape.
    post_projection: Option<Arc<Vec<usize>>>,
    /// DataFusion plan properties (one unbounded-source-free, bounded partition).
    properties: PlanProperties,
    /// Where the finished scan's measurements are published. `Mutex` because the
    /// plan is shared (`Arc`) and `execute` takes `&self`; contention is one lock
    /// per scan, not per batch.
    outcome: crate::df_spike::provider::OutcomeSlot,
    /// Human-readable description of what was pushed, for `EXPLAIN` output.
    pushed: String,
}

impl CqliteScanExec {
    /// Build the plan.
    ///
    /// `producer_schema` is what the producer emits; `post_projection` (when
    /// present) selects the output columns from each produced batch. Column
    /// selection via [`RecordBatch::project`] is a pointer copy per column, not a
    /// data copy — it deliberately does NOT reduce the SCAN work, which is what
    /// keeps the two benchmark arms reading the same bytes when pushdown is off.
    pub fn try_new(
        producer: Arc<MergeProducer>,
        paths: Arc<Vec<std::path::PathBuf>>,
        producer_schema: SchemaRef,
        post_projection: Option<Vec<usize>>,
        pushed: String,
    ) -> DfResult<Self> {
        let schema = match &post_projection {
            Some(indices) => project_schema(&producer_schema, Some(indices))?,
            None => producer_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            producer,
            paths,
            schema,
            post_projection: post_projection.map(Arc::new),
            properties,
            outcome: Arc::new(Mutex::new(None)),
            pushed,
        })
    }

    /// The measurements of the most recent finished scan, if one has finished.
    ///
    /// Returns `None` while a scan is still running — a benchmark MUST treat that
    /// as a failure to measure rather than as a zero, so the caller decides.
    pub fn take_outcome(&self) -> Option<ScanOutcome> {
        self.outcome.lock().ok().and_then(|mut slot| slot.take())
    }

    /// Handle to the outcome slot, so a provider can read it after execution.
    pub fn outcome_slot(&self) -> crate::df_spike::provider::OutcomeSlot {
        self.outcome.clone()
    }
}

impl fmt::Debug for CqliteScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CqliteScanExec")
            .field("sources", &self.paths.len())
            .field("columns", &self.schema.fields().len())
            .field("pushed", &self.pushed)
            .finish()
    }
}

impl DisplayAs for CqliteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CqliteScanExec: sources={}, cols={}, pushed=[{}]",
            self.paths.len(),
            self.schema.fields().len(),
            self.pushed
        )
    }
}

impl ExecutionPlan for CqliteScanExec {
    fn name(&self) -> &str {
        "CqliteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // A leaf scan.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "CqliteScanExec is a leaf and takes no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CqliteScanExec has exactly 1 partition; asked for {partition}"
            )));
        }
        scan::note_scan_started();

        let running = scan::spawn_scan(self.producer.clone(), self.paths.as_ref().clone());
        let scan::RunningScan {
            batches,
            sources: _,
            done,
        } = running;

        let projection = self.post_projection.clone();
        let outcome_slot = self.outcome_slot();
        let schema = self.schema.clone();

        // Drain the producer channel into a DataFusion stream. The terminal item
        // publishes the scan's measurements, so a consumer that reaches the end
        // of the stream can always read them back.
        let stream = futures::stream::unfold(Some((batches, done, outcome_slot)), move |state| {
            let projection = projection.clone();
            async move {
                let (mut batches, done, outcome_slot) = state?;
                match batches.recv().await {
                    Some(Ok(batch)) => {
                        let item = project_batch(batch, projection.as_deref());
                        Some((item, Some((batches, done, outcome_slot))))
                    }
                    Some(Err(e)) => {
                        // A producer error is terminal: surface it and stop.
                        publish_outcome(done, &outcome_slot);
                        Some((Err(DataFusionError::External(Box::new(e))), None))
                    }
                    None => {
                        publish_outcome(done, &outcome_slot);
                        None
                    }
                }
            }
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DfResult<Statistics> {
        // No authoritative row count is available without scanning (issue #28: no
        // guessing from file sizes or `Statistics.db` estimates), so report
        // "unknown" rather than a fabricated estimate the optimizer would trust.
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Select the output columns from a produced batch.
fn project_batch(batch: RecordBatch, projection: Option<&Vec<usize>>) -> DfResult<RecordBatch> {
    match projection {
        Some(indices) => batch
            .project(indices)
            .map_err(|e| DataFusionError::ArrowError(e, None)),
        None => Ok(batch),
    }
}

/// Join the producer thread and store its measurements.
fn publish_outcome(
    done: std::thread::JoinHandle<ScanOutcome>,
    slot: &crate::df_spike::provider::OutcomeSlot,
) {
    // The producer has closed the channel, so the thread is finishing; the join
    // is bounded. A panicked producer thread leaves the slot empty, which the
    // harness reports as "could not measure" rather than as a zero.
    if let Ok(outcome) = done.join() {
        if let Ok(mut slot) = slot.lock() {
            *slot = Some(outcome);
        }
    }
}
