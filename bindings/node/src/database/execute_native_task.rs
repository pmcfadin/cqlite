//! The `executeNative()` async task: off-loop query execution plus on-loop
//! native-typed row materialization.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion; behaviour, spans and error mapping are unchanged.

use std::sync::Arc;

#[cfg(feature = "write-support")]
use std::sync::Mutex;

use crate::error::{runtime_init_error, to_napi_error};
// `simple_error` is only reachable from the DML branch of `compute_inner()`.
#[cfg(feature = "write-support")]
use crate::error::simple_error;

use super::Database;

/// Async task for executing queries with native type conversion.
pub struct ExecuteNativeTask {
    /// Fields are `pub(super)` only because `Database::execute_native()` lives in
    /// the parent module after the issue #1464 split; they were private when both
    /// sat in one file and are not reachable outside `database`.
    pub(super) inner: Arc<cqlite_core::Database>,
    pub(super) query: String,
    /// Per-handle default traceparent for the per-call span (issue #1040).
    pub(super) traceparent: Option<String>,
    /// On-event-loop row-materialization bound (issue #1442); read on the JS thread.
    pub(super) max_native_rows: usize,
    /// Write engine handle, present only when write support is compiled and writable=true.
    #[cfg(feature = "write-support")]
    pub(super) write_engine: Option<Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>>,
}

/// Intermediate result from async query execution.
pub struct QueryResultData {
    rows: Vec<std::collections::HashMap<String, cqlite_core::types::Value>>,
    execution_time_ms: u32,
    columns: Vec<cqlite_core::query::result::ColumnInfo>,
    /// Non-zero when the statement was a DML write.
    rows_affected: u32,
}

impl napi::Task for ExecuteNativeTask {
    type Output = QueryResultData;
    type JsValue = napi::JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Firewall the async-worker thread against a panic in the read/decode
        // path (issue #1754): a panic here cannot unwind across the FFI frame and
        // would abort the whole Node process even under `panic=unwind`. Catch it
        // on the worker thread and reject the promise with a typed error instead.
        crate::error::catch_unwind_to_napi("executeNative", || self.compute_inner())
    }

    fn resolve(&mut self, env: napi::Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        self.resolve_inner(env, output)
    }
}

impl ExecuteNativeTask {
    fn compute_inner(&mut self) -> napi::Result<QueryResultData> {
        use tracing::Instrument;

        // Per-call span (issue #1040), parented under the handle's traceparent.
        let span = crate::observability::execute_span("executeNative", self.traceparent.as_deref());

        // Route DML to write engine when write support is present. This path is
        // fully synchronous, so a span guard is correct (no `.await`).
        #[cfg(feature = "write-support")]
        if Database::is_dml_statement(&self.query) {
            if let Some(ref we) = self.write_engine {
                let _entered = span.enter();
                let start = std::time::Instant::now();
                let mut engine = we
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                engine.execute(&self.query).map_err(to_napi_error)?;
                let elapsed_ms = start.elapsed().as_millis() as u32;
                crate::observability::record_rows(&span, 0);
                return Ok(QueryResultData {
                    rows: vec![],
                    execution_time_ms: elapsed_ms,
                    columns: vec![],
                    rows_affected: 1,
                });
            }
        }

        // Fail closed: without the write-support feature, a DML statement must
        // NOT fall through to the read engine (issue #1460). The public
        // `execute_native` entry point already rejects this before creating the
        // task; this is defense-in-depth so the task can never silently no-op.
        #[cfg(not(feature = "write-support"))]
        if Database::is_dml_statement(&self.query) {
            return Err(Database::dml_unsupported_error());
        }

        // Use global runtime for async execution. The future is `.instrument`-ed
        // by the span rather than holding a guard across the runtime boundary.
        let span_for_record = span.clone();
        let query = &self.query;
        let inner = &self.inner;
        let result = crate::runtime::block_on(
            async move {
                inner.execute(query).await.map_err(|e| {
                    crate::observability::record_boundary_error(&e);
                    to_napi_error(e)
                })
            }
            .instrument(span),
        )
        .map_err(runtime_init_error)??;

        // Bound on-event-loop work (issue #1442): the per-row JS-object build in
        // `resolve()` runs on the JS thread and cannot be moved off-loop, so a
        // huge set is rejected here (before the deep clone below) rather than
        // freezing timers/HTTP handlers. Steer the caller to executeStreaming.
        if result.rows.len() > self.max_native_rows {
            return Err(crate::error::native_rows_exceeded_error(
                result.rows.len(),
                self.max_native_rows,
            ));
        }
        let row_count = result.rows.len() as u32;
        crate::observability::record_rows(&span_for_record, row_count as u64);
        Ok(QueryResultData {
            rows: result
                .rows
                .into_iter()
                .map(|r| {
                    r.values
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect()
                })
                .collect(),
            execution_time_ms: result.execution_time_ms as u32,
            columns: result.metadata.columns.clone(),
            rows_affected: row_count,
        })
    }

    fn resolve_inner(
        &mut self,
        env: napi::Env,
        output: QueryResultData,
    ) -> napi::Result<napi::JsObject> {
        let mut result_obj = env.create_object()?;

        // Create rows array with native types
        let mut rows_arr = env.create_array_with_length(output.rows.len())?;
        // Issue #1446: intern SELECT-order column-name keys ONCE per result (not per row) so props emit in authoritative column order, not HashMap hash order.
        let col_names: Vec<String> = output.columns.iter().map(|c| c.name.clone()).collect();
        let col_keys = crate::row::intern_column_keys(&env, &col_names)?;
        // Issue #1448: one conversion context per result caches the global `Set`/`Map` constructors (fetched at most once each here, not per cell).
        let ctx = crate::value::ConvCtx::new(&env);
        for (i, row_values) in output.rows.iter().enumerate() {
            let row_obj = crate::row::row_to_object(&ctx, &col_keys, row_values)?;
            rows_arr.set_element(i as u32, row_obj)?;
        }

        result_obj.set_named_property("rows", rows_arr)?;
        result_obj.set_named_property("rowCount", env.create_uint32(output.rows.len() as u32)?)?;
        result_obj.set_named_property("rowsAffected", env.create_uint32(output.rows_affected)?)?;
        result_obj.set_named_property(
            "executionTimeMs",
            env.create_uint32(output.execution_time_ms)?,
        )?;

        // Create columns array with metadata
        let mut columns_arr = env.create_array_with_length(output.columns.len())?;
        for (i, col) in output.columns.iter().enumerate() {
            let mut col_obj = env.create_object()?;
            col_obj.set_named_property("name", env.create_string(&col.name)?)?;
            col_obj.set_named_property(
                "dataType",
                env.create_string(&format!("{:?}", col.data_type))?,
            )?;
            col_obj.set_named_property("nullable", env.get_boolean(col.nullable)?)?;
            col_obj.set_named_property("position", env.create_uint32(col.position as u32)?)?;
            match &col.table_name {
                Some(name) => col_obj.set_named_property("tableName", env.create_string(name)?)?,
                None => col_obj.set_named_property("tableName", env.get_null()?)?,
            }
            columns_arr.set_element(i as u32, col_obj)?;
        }
        result_obj.set_named_property("columns", columns_arr)?;

        Ok(result_obj)
    }
}
