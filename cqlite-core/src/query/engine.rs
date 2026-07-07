//! Query engine implementation for CQLite
//!
//! This module provides the main query engine that coordinates between
//! parsing, planning, and execution of CQL queries.

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{
    executor::{QueryExecutor, QueryResult},
    parser::QueryParser,
    planner::QueryPlanner,
    prepared::PreparedQuery,
    result::{QueryResultIterator, StreamingConfig},
    QueryStats,
};

use super::engine_stats::AtomicQueryStats;
#[cfg(feature = "state_machine")]
use super::{
    select_executor::SelectExecutor,
    select_optimizer::{OptimizedQueryPlan, SelectOptimizer},
    select_parser,
};
use crate::{
    memory::MemoryManager, schema::SchemaManager, storage::StorageEngine, Config, Error, Result,
    Value,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Query cache entry
///
/// Crate-internal plan-cache entry with no external consumers, so the
/// `hit_count: AtomicU64` field (issue #1595) is kept off the public semver
/// surface by keeping this type `pub(crate)`.
#[derive(Debug)]
pub(crate) struct QueryCacheEntry {
    /// Parsed query
    pub parsed_query: super::ParsedQuery,
    /// Query plan
    pub plan: super::planner::QueryPlan,
    /// Cache timestamp
    pub cached_at: Instant,
    /// Hit count. Lock-free (issue #1595): a plan-cache HIT bumps this via a
    /// shard READ lock (`DashMap::get`) + relaxed `fetch_add`, so concurrent hits
    /// to the same shard no longer contend on a write lock.
    pub hit_count: AtomicU64,
}

// `AtomicU64` is not `Clone`, so the previous `#[derive(Clone)]` no longer
// applies; preserve the public `Clone` surface by hand, snapshotting the counter
// with a relaxed load (issue #1595).
impl Clone for QueryCacheEntry {
    fn clone(&self) -> Self {
        Self {
            parsed_query: self.parsed_query.clone(),
            plan: self.plan.clone(),
            cached_at: self.cached_at,
            hit_count: AtomicU64::new(self.hit_count.load(Ordering::Relaxed)),
        }
    }
}

/// Schema availability status for diagnostic purposes
#[derive(Debug, Clone)]
pub enum SchemaStatus {
    /// Schema is available and ready for queries
    Available { keyspace: String, table: String },
    /// Schema not found in registry
    Missing { table: String, reason: String },
    /// Schema extraction failed from SSTable
    ExtractionFailed {
        table: String,
        cause: String,
        suggestion: String,
    },
}

/// Query engine with caching and statistics
#[derive(Debug)]
pub struct QueryEngine {
    /// Query parser
    parser: QueryParser,
    /// Query planner
    planner: QueryPlanner,
    /// Query executor
    executor: QueryExecutor,
    /// Schema manager reference
    schema_manager: Arc<SchemaManager>,
    /// Advanced SELECT optimizer. `Arc` so prepared SELECTs can share the same
    /// instance and reach the partition-targeted fast path (Issue #961).
    #[cfg(feature = "state_machine")]
    select_optimizer: Arc<SelectOptimizer>,
    /// Advanced SELECT executor (shared with prepared SELECTs, Issue #961).
    #[cfg(feature = "state_machine")]
    select_executor: Arc<SelectExecutor>,
    /// Prepared statement cache
    prepared_cache: DashMap<String, Arc<PreparedQuery>>,
    /// Query plan cache
    plan_cache: DashMap<String, QueryCacheEntry>,
    /// Issue #1587 (E5): plan cache for the modern (state_machine) SELECT path,
    /// keyed by the exact CQL text. The optimized plan is a pure function of the
    /// literal CQL, so a repeated identical ad-hoc SELECT reuses the plan instead
    /// of re-parsing + re-optimizing. Kept separate from `plan_cache` (which
    /// holds legacy `QueryPlan`s and feeds `CacheStats`) so neither disturbs the
    /// other. Value is `(inserted_at, plan)` for the same simple-LRU eviction.
    #[cfg(feature = "state_machine")]
    select_plan_cache: DashMap<String, (Instant, Arc<OptimizedQueryPlan>)>,
    /// Query statistics. Lock-free atomic counters (issue #1595): counters are
    /// bumped with relaxed atomics instead of a `RwLock` taken 2–3× per query.
    stats: AtomicQueryStats,
    /// Configuration
    config: Config,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(
        storage: Arc<StorageEngine>,
        schema: Arc<SchemaManager>,
        _memory: Arc<MemoryManager>,
        config: &Config,
    ) -> Result<Self> {
        let parser = QueryParser::new(config);
        let planner = QueryPlanner::new(schema.clone(), config);
        let executor = QueryExecutor::new(storage.clone(), schema.clone(), config);

        // Initialize advanced SELECT components
        #[cfg(feature = "state_machine")]
        let select_optimizer = Arc::new(SelectOptimizer::new(schema.clone(), storage.clone()));
        // Issue #1582 (D6): wire the byte-bounded result budget from config so
        // the `max_result_bytes` knob is load-bearing on the materializing path.
        #[cfg(feature = "state_machine")]
        let select_executor = Arc::new(
            SelectExecutor::new(schema.clone(), storage)
                .with_max_result_bytes(
                    usize::try_from(config.query.max_result_bytes).unwrap_or(usize::MAX),
                )
                .with_max_result_rows(
                    usize::try_from(config.query.max_result_rows).unwrap_or(usize::MAX),
                ),
        );

        Ok(Self {
            parser,
            planner,
            executor,
            schema_manager: schema,
            #[cfg(feature = "state_machine")]
            select_optimizer,
            #[cfg(feature = "state_machine")]
            select_executor,
            prepared_cache: DashMap::new(),
            plan_cache: DashMap::new(),
            #[cfg(feature = "state_machine")]
            select_plan_cache: DashMap::new(),
            stats: AtomicQueryStats::default(),
            config: config.clone(),
        })
    }

    /// Enforce the byte-bounded result budget (issue #1582 / D6) on a result
    /// materialized by the LEGACY [`QueryExecutor`] — the simple `WHERE id =
    /// <value>` point-lookup path, which is routed to `self.executor` (not the
    /// budgeted `SelectExecutor`) for key-handling consistency with INSERT. A
    /// wide-partition point lookup would otherwise materialize unbounded and
    /// bypass the guard, INCLUDING on a plan-cache HIT (roborev #1582 D6). Reuses
    /// the SAME estimator + enforcement as the optimizer path so the byte ceiling
    /// and the row-count safety valve behave identically. Applied exactly once at
    /// every legacy return site.
    fn enforce_legacy_result_budget(&self, result: &QueryResult) -> Result<()> {
        super::result_budget::enforce_materialized_rows(
            &result.rows,
            usize::try_from(self.config.query.max_result_bytes).unwrap_or(usize::MAX),
            usize::try_from(self.config.query.max_result_rows).unwrap_or(usize::MAX),
        )
        .inspect_err(|e| {
            self.inc_error_queries();
            crate::observability::record_error(e, "query");
        })
    }

    /// Increment the total queries counter (lock-free, issue #1595)
    fn inc_total_queries(&self) {
        self.stats.record_query();
    }

    /// Increment the error queries counter (lock-free, issue #1595)
    fn inc_error_queries(&self) {
        self.stats.record_error();
    }

    /// Record a plan-cache hit (lock-free, issue #1595). `cache_hit_ratio` is
    /// derived at read time as `cache_hits / total_queries`.
    fn record_cache_hit(&self) {
        self.stats.record_cache_hit();
    }

    /// Execute a CQL query
    ///
    /// This is the parent of the query span tree (epic #1031, issue #1035): the
    /// `query.execute` span created here is the context every read-path span
    /// (issue #1034) and SELECT sub-span nests under. Bounded span attributes
    /// (plan type, access path, rows returned) are recorded once the result is
    /// known via [`Self::update_execution_stats`]; the query text is never
    /// attached.
    #[tracing::instrument(
        name = "query.execute",
        skip(self, cql),
        fields(
            cqlite.query.plan_type = tracing::field::Empty,
            cqlite.query.access_path = tracing::field::Empty,
            cqlite.query.rows = tracing::field::Empty,
        )
    )]
    pub async fn execute(&self, cql: &str) -> Result<QueryResult> {
        let start_time = Instant::now();
        self.inc_total_queries();

        // Route SELECT statements through the advanced parser, except simple
        // `WHERE id = <value>` point lookups which must share the normal
        // executor's key-handling path so INSERT and SELECT agree on keys.
        let trimmed_cql = cql.trim().to_uppercase();
        let is_simple_id_lookup = cql.contains("WHERE id =") && cql.split_whitespace().count() <= 8;
        if trimmed_cql.starts_with("SELECT") && !is_simple_id_lookup {
            return self.execute_select_query(cql, start_time).await;
        }
        #[cfg(debug_assertions)]
        if trimmed_cql.starts_with("SELECT") && is_simple_id_lookup {
            tracing::debug!(
                "Routing simple SELECT through normal executor for consistent key handling"
            );
        }

        // Check plan cache first for non-SELECT queries. Issue #1595: serve the
        // HIT under a shard READ lock (`DashMap::get`), bump the lock-free
        // `hit_count`, clone the plan out, and DROP the guard before the `.await`
        // so no shard lock is held across execution and concurrent hits do not
        // contend on a write lock.
        let cached_plan = self.plan_cache.get(cql).map(|entry| {
            entry.hit_count.fetch_add(1, Ordering::Relaxed);
            entry.plan.clone()
        });
        if let Some(plan) = cached_plan {
            self.record_cache_hit();

            let mut result =
                crate::observability::record_result("query", self.executor.execute(&plan).await)?;
            // Issue #1582 (D6): the LEGACY point-lookup path bypasses the
            // SelectExecutor's byte budget; enforce it on the plan-cache HIT
            // return too (roborev finding) so a single very wide row cannot
            // exceed `max_result_bytes` without raising `ResultTooLarge`.
            self.enforce_legacy_result_budget(&result)?;
            self.update_execution_stats(&mut result, start_time);
            return Ok(result);
        }

        let parsed_query = self.parser.parse(cql).inspect_err(|e| {
            self.inc_error_queries();
            crate::observability::record_error(e, "query");
        })?;
        let plan =
            crate::observability::record_result("query", self.planner.plan(&parsed_query).await)?;

        if self.config.query.query_cache_size.unwrap_or(0) > 0 {
            self.cache_query_plan(cql, parsed_query, plan.clone());
        }

        let mut result =
            crate::observability::record_result("query", self.executor.execute(&plan).await)?;
        // Issue #1582 (D6): the LEGACY point-lookup path bypasses the
        // SelectExecutor's byte budget; enforce it on the cold (cache-miss)
        // return, matching the plan-cache-hit return above.
        self.enforce_legacy_result_budget(&result)?;
        self.update_execution_stats(&mut result, start_time);
        Ok(result)
    }

    /// Execute a CQL query with streaming results (Issue #280)
    ///
    /// Returns a `QueryResultIterator` that yields rows incrementally via a bounded
    /// channel, enabling memory-efficient processing of large result sets.
    ///
    /// # Arguments
    ///
    /// * `cql` - The CQL query string to execute (must be a SELECT statement)
    /// * `config` - Streaming configuration (buffer size, chunk hints)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Query is not a SELECT statement
    /// - SQL syntax is invalid
    /// - Query execution fails
    ///
    /// # Memory Budget
    ///
    /// The streaming approach stays within the 128MB target by using bounded channels
    /// and processing rows incrementally rather than materializing all results.
    #[cfg(feature = "state_machine")]
    pub async fn execute_streaming(
        &self,
        cql: &str,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        self.inc_total_queries();

        if !cql.trim().to_uppercase().starts_with("SELECT") {
            return Err(Error::query_execution(
                "Streaming execution only supports SELECT queries",
            ));
        }

        let select_statement =
            select_parser::parse_select(cql).inspect_err(|_| self.inc_error_queries())?;
        let optimized_plan = self.select_optimizer.optimize(select_statement).await?;

        self.select_executor
            .execute_streaming(optimized_plan, config)
            .await
    }

    /// Execute a SELECT query using the advanced parser and optimizer
    async fn execute_select_query(&self, cql: &str, start_time: Instant) -> Result<QueryResult> {
        // Check plan cache first for SELECT queries too. Issue #1595: resolve the
        // outcome under a shard READ lock (`DashMap::get`) — bumping the lock-free
        // `hit_count` and cloning the plan for a reusable entry — then DROP the
        // guard before acting, so the `.await` holds no shard lock and `remove`
        // never runs while a `get` guard on the same shard is held.
        enum HitOutcome {
            Reuse(super::planner::QueryPlan),
            Placeholder,
            Miss,
        }
        let outcome = match self.plan_cache.get(cql) {
            Some(entry) if entry.plan.table.is_some() => {
                entry.hit_count.fetch_add(1, Ordering::Relaxed);
                HitOutcome::Reuse(entry.plan.clone())
            }
            Some(_) => HitOutcome::Placeholder,
            None => HitOutcome::Miss,
        };
        match outcome {
            HitOutcome::Reuse(plan) => {
                self.record_cache_hit();

                let mut result = crate::observability::record_result(
                    "query",
                    self.executor.execute(&plan).await,
                )?;
                // Issue #1582 (D6): this cached SELECT plan is served by the
                // LEGACY executor, which bypasses the SelectExecutor's byte
                // budget; enforce it on this plan-cache HIT return too.
                self.enforce_legacy_result_budget(&result)?;
                self.update_execution_stats(&mut result, start_time);
                return Ok(result);
            }
            HitOutcome::Placeholder => {
                // Placeholder plans without table information are not reusable; drop them.
                self.plan_cache.remove(cql);
            }
            HitOutcome::Miss => {}
        }

        #[cfg(not(feature = "state_machine"))]
        return Err(Error::query_execution(
            "Advanced SELECT parsing requires state_machine feature",
        ));

        #[cfg(feature = "state_machine")]
        {
            // Issue #1587 (E5): reuse a previously-optimized plan for the exact
            // same CQL text. The optimized plan is a pure function of the literal
            // CQL, so this is byte-for-byte equivalent to re-parsing +
            // re-optimizing — it only skips the redundant work.
            let optimized_plan: Arc<OptimizedQueryPlan> =
                if let Some(entry) = self.select_plan_cache.get(cql) {
                    self.record_cache_hit();
                    Arc::clone(&entry.value().1)
                } else {
                    let select_statement = select_parser::parse_select(cql).inspect_err(|e| {
                        self.inc_error_queries();
                        crate::observability::record_error(e, "query");
                    })?;
                    let plan = Arc::new(crate::observability::record_result(
                        "query",
                        self.select_optimizer.optimize(select_statement).await,
                    )?);
                    self.cache_select_plan(cql, Arc::clone(&plan));
                    plan
                };
            let mut result = crate::observability::record_result(
                "query",
                self.select_executor
                    .execute((*optimized_plan).clone())
                    .await,
            )?;
            self.update_execution_stats(&mut result, start_time);
            Ok(result)
        }
    }

    /// Insert an optimized SELECT plan into the modern plan cache, evicting the
    /// oldest entry first when at capacity (simple LRU, matching the legacy
    /// `cache_query_plan`). A configured `query_cache_size` of 0 disables it.
    #[cfg(feature = "state_machine")]
    fn cache_select_plan(&self, cql: &str, plan: Arc<OptimizedQueryPlan>) {
        let cache_size = self.config.query.query_cache_size.unwrap_or(0);
        if cache_size == 0 {
            return;
        }
        if self.select_plan_cache.len() >= cache_size {
            let oldest_key = self
                .select_plan_cache
                .iter()
                .min_by_key(|entry| entry.value().0)
                .map(|entry| entry.key().clone());
            if let Some(key) = oldest_key {
                self.select_plan_cache.remove(&key);
            }
        }
        self.select_plan_cache
            .insert(cql.to_string(), (Instant::now(), plan));
    }

    /// Execute a query with positional `?` parameters (Issue #961).
    ///
    /// The supplied `params` are bound, in source order, into the `?` placeholders
    /// of the parsed statement *before* planning and execution, so the bound
    /// values participate in partition-key classification, encoding, and typed
    /// coercion. A `WHERE pk = ?` therefore engages the same partition-targeted
    /// fast path (#949/#956) as the equivalent literal query.
    ///
    /// Binding is currently supported for SELECT statements only. A non-SELECT
    /// CQL with parameters, or any use of named (`:name`) parameters, is rejected
    /// with a clear error (named-parameter binding is intentionally out of scope:
    /// the SELECT grammar only tokenizes positional `?`).
    ///
    /// # Routing parity with `execute` (Finding 1)
    ///
    /// When the parsed SELECT has **zero** bind markers and `params` is empty,
    /// this delegates straight back to [`Self::execute`] so that a markerless
    /// `execute_with_params(sql, &[])` is byte-for-byte equivalent to
    /// `execute(sql)` — including the legacy simple-`WHERE id = <literal>` point
    /// lookup that `execute` intentionally keeps on the normal executor for
    /// INSERT/SELECT key compatibility. Only when markers are present (`> 0`) is
    /// the statement bound and driven through the SELECT optimizer + executor
    /// pipeline.
    ///
    /// Arity stays strict in both directions: markers `> 0` with a wrong
    /// `params.len()` is an error, and markers `== 0` with a **non-empty**
    /// `params` is also an error (a supplied parameter with no placeholder is a
    /// caller bug). The latter matches [`SelectStatement::bind_parameters`]'s
    /// contract and the documented strictness of this API.
    pub async fn execute_with_params(&self, cql: &str, params: &[Value]) -> Result<QueryResult> {
        let is_select = cql.trim().to_uppercase().starts_with("SELECT");

        if !is_select {
            // Non-SELECT: parameter binding is not supported. With no parameters
            // this is just a normal statement, so defer to the regular path;
            // with parameters it is an explicit, clear error.
            if params.is_empty() {
                return self.execute(cql).await;
            }
            self.inc_total_queries();
            self.inc_error_queries();
            return Err(Error::query_execution(
                "Parameterized execution currently supports SELECT statements only",
            ));
        }

        #[cfg(not(feature = "state_machine"))]
        {
            let _ = params;
            self.inc_total_queries();
            self.inc_error_queries();
            return Err(Error::query_execution(
                "Parameterized SELECT execution requires the state_machine feature",
            ));
        }

        #[cfg(feature = "state_machine")]
        {
            // Parse once so we can count bind markers and decide routing. Parse
            // failures here mirror `execute_select_query`, which would also fail.
            let statement = select_parser::parse_select(cql).inspect_err(|_| {
                self.inc_total_queries();
                self.inc_error_queries();
            })?;
            let marker_count = statement.bind_marker_count();

            // Finding 1: a markerless SELECT with no supplied params must route
            // exactly like a literal `execute(cql)` — including the simple-id
            // legacy point-lookup path — so the two APIs cannot diverge. A
            // marker-free statement with stray params is, however, a caller bug:
            // reject it for strict arity (no placeholder to bind into).
            if marker_count == 0 {
                if params.is_empty() {
                    return self.execute(cql).await;
                }
                self.inc_total_queries();
                self.inc_error_queries();
                return Err(Error::query_execution(format!(
                    "Parameter count mismatch: query has 0 bind marker(s), got {} parameter(s)",
                    params.len()
                )));
            }

            let start_time = Instant::now();
            self.inc_total_queries();

            // Markers present: bind through the SELECT pipeline. Arity is
            // enforced by `bind_parameters` (too few / too many -> error). The
            // bound statement reaches the same optimizer + executor as a literal
            // `execute()`, so the partition-targeted fast path engages.
            let mut statement = statement;
            statement
                .bind_parameters(params)
                .inspect_err(|_| self.inc_error_queries())?;

            let optimized_plan = self.select_optimizer.optimize(statement).await?;
            let mut result = self.select_executor.execute(optimized_plan).await?;
            self.update_execution_stats(&mut result, start_time);
            Ok(result)
        }
    }

    /// Prepare a query for repeated execution
    pub async fn prepare(&self, cql: &str) -> Result<Arc<PreparedQuery>> {
        if let Some(cached) = self.prepared_cache.get(cql) {
            return Ok(cached.clone());
        }

        let parsed_query = self.parser.parse(cql)?;
        let plan = self.planner.plan(&parsed_query).await?;

        // Issue #961: when the prepared statement is a SELECT, attach the SELECT
        // optimizer + executor pipeline so that `?` parameters are bound and the
        // bound query reaches the partition-targeted fast path (#949/#956) — the
        // same path a literal `execute()` takes. Non-SELECTs keep the legacy
        // `QueryExecutor` plan path.
        #[cfg(feature = "state_machine")]
        let prepared = if cql.trim().to_uppercase().starts_with("SELECT") {
            let statement = select_parser::parse_select(cql)?;
            let marker_count = statement.bind_marker_count();
            Arc::new(PreparedQuery::new_select(
                parsed_query,
                plan,
                Arc::new(self.executor.clone()),
                statement,
                marker_count,
                self.select_optimizer.clone(),
                self.select_executor.clone(),
            ))
        } else {
            Arc::new(PreparedQuery::new(
                parsed_query,
                plan,
                Arc::new(self.executor.clone()),
            ))
        };

        #[cfg(not(feature = "state_machine"))]
        let prepared = Arc::new(PreparedQuery::new(
            parsed_query,
            plan,
            Arc::new(self.executor.clone()),
        ));

        self.prepared_cache
            .insert(cql.to_string(), prepared.clone());

        Ok(prepared)
    }

    /// Execute a prepared query
    pub async fn execute_prepared(
        &self,
        prepared: &PreparedQuery,
        params: &[Value],
    ) -> Result<QueryResult> {
        let start_time = Instant::now();
        self.inc_total_queries();

        let mut result = prepared.execute(params).await?;
        self.update_execution_stats(&mut result, start_time);
        Ok(result)
    }

    /// Get query statistics
    pub fn stats(&self) -> QueryStats {
        self.stats.snapshot()
    }

    /// Clear all caches
    pub fn clear_caches(&self) {
        self.prepared_cache.clear();
        self.plan_cache.clear();
        #[cfg(feature = "state_machine")]
        self.select_plan_cache.clear();
    }

    /// Clear prepared statement cache
    pub fn clear_prepared_cache(&self) {
        self.prepared_cache.clear();
    }

    /// Clear query plan cache
    pub fn clear_plan_cache(&self) {
        self.plan_cache.clear();
        #[cfg(feature = "state_machine")]
        self.select_plan_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            prepared_cache_size: self.prepared_cache.len(),
            plan_cache_size: self.plan_cache.len(),
            prepared_cache_hits: self.prepared_cache.len() as u64,
            plan_cache_hits: self.plan_cache.len() as u64,
        }
    }

    /// Optimize a query (return execution plan without executing)
    pub async fn explain(&self, cql: &str) -> Result<ExplainResult> {
        // Parse the query
        let parsed_query = self.parser.parse(cql)?;

        // Plan the query
        let plan = self.planner.plan(&parsed_query).await?;

        Ok(ExplainResult {
            query_type: format!("{:?}", parsed_query.query_type),
            plan_type: format!("{:?}", plan.plan_type),
            estimated_cost: plan.estimated_cost,
            estimated_rows: plan.estimated_rows,
            selected_indexes: plan
                .selected_indexes
                .iter()
                .map(|idx| format!("{} ({:?})", idx.index_name, idx.index_type))
                .collect(),
            execution_steps: plan
                .steps
                .iter()
                .map(|step| {
                    format!(
                        "{:?}: {} (cost: {:.2})",
                        step.step_type,
                        step.columns.join(", "),
                        step.cost
                    )
                })
                .collect(),
            parallelization_info: plan
                .steps
                .iter()
                .filter(|step| step.parallelization.can_parallelize)
                .map(|step| {
                    format!(
                        "Threads: {}, Partition: {:?}",
                        step.parallelization.suggested_threads, step.parallelization.partition_key
                    )
                })
                .collect(),
        })
    }

    /// Analyze query performance
    pub async fn analyze(&self, cql: &str) -> Result<AnalyzeResult> {
        let start_time = Instant::now();

        // Execute the query multiple times to get average performance
        let mut execution_times = Vec::new();
        let mut results = Vec::new();

        for _ in 0..self.config.query.analyze_iterations.unwrap_or(5) {
            let iter_start = Instant::now();
            let result = self.execute(cql).await?;
            execution_times.push(iter_start.elapsed());
            results.push(result);
        }

        let total_time = start_time.elapsed();
        let avg_time =
            execution_times.iter().sum::<std::time::Duration>() / execution_times.len() as u32;
        let no_times = || Error::query_execution("No execution times recorded for analysis");
        let min_time = execution_times.iter().min().ok_or_else(no_times)?;
        let max_time = execution_times.iter().max().ok_or_else(no_times)?;

        // Calculate standard deviation
        let variance = execution_times
            .iter()
            .map(|time| {
                let diff = time.as_nanos() as f64 - avg_time.as_nanos() as f64;
                diff * diff
            })
            .sum::<f64>()
            / execution_times.len() as f64;
        let std_dev = variance.sqrt();

        Ok(AnalyzeResult {
            iterations: execution_times.len(),
            total_time_ms: total_time.as_millis() as u64,
            avg_time_ms: avg_time.as_millis() as u64,
            min_time_ms: min_time.as_millis() as u64,
            max_time_ms: max_time.as_millis() as u64,
            std_dev_ms: (std_dev / 1_000_000.0) as u64, // Convert from nanoseconds to milliseconds
            avg_rows_returned: results.iter().map(|r| r.rows.len()).sum::<usize>() / results.len(),
            cache_hit_ratio: self.stats().cache_hit_ratio,
        })
    }

    /// Cache a query plan, evicting the oldest entry first if at capacity (simple LRU).
    fn cache_query_plan(
        &self,
        cql: &str,
        parsed_query: super::ParsedQuery,
        plan: super::planner::QueryPlan,
    ) {
        let cache_size = self.config.query.query_cache_size.unwrap_or(0);
        if cache_size == 0 {
            return;
        }

        if self.plan_cache.len() >= cache_size {
            let oldest_key = self
                .plan_cache
                .iter()
                .min_by_key(|entry| entry.cached_at)
                .map(|entry| entry.key().clone());
            if let Some(key) = oldest_key {
                self.plan_cache.remove(&key);
            }
        }

        self.plan_cache.insert(
            cql.to_string(),
            QueryCacheEntry {
                parsed_query,
                plan,
                cached_at: Instant::now(),
                hit_count: AtomicU64::new(0),
            },
        );
    }

    /// Check if schema is available for a table
    pub async fn has_schema_for_table(&self, table: &str) -> bool {
        self.schema_manager.get_table_schema(table).await.is_ok()
    }

    /// Get detailed schema status for debugging
    pub async fn schema_status(&self, table: &str) -> SchemaStatus {
        match self.schema_manager.get_table_schema(table).await {
            Ok(schema) => SchemaStatus::Available {
                keyspace: schema.keyspace.clone(),
                table: schema.table.clone(),
            },
            Err(Error::Schema(msg)) if msg.contains("not found") => {
                SchemaStatus::Missing {
                    table: table.to_string(),
                    reason: msg,
                }
            }
            Err(e) => SchemaStatus::ExtractionFailed {
                table: table.to_string(),
                cause: e.to_string(),
                suggestion: "Verify SSTable files are valid Cassandra 5.0 format and Statistics.db contains SerializationHeader".to_string(),
            },
        }
    }

    /// Update execution statistics.
    ///
    /// This is the single chokepoint every materializing query path (cache hit,
    /// parsed-plan, SELECT, parameterized, prepared) funnels through once its
    /// result is known, so it is also where observability emits the end-to-end
    /// query signals exactly once (issue #1035): the [`catalog::QUERY_DURATION`]
    /// histogram and the [`catalog::QUERY_ROWS`] counter, both dimensioned by the
    /// bounded access path the SELECT chose (when available). It also records the
    /// bounded span attributes on the active `query.execute` span so the parent of
    /// the read-path span tree is self-describing. Durations are reported in
    /// seconds, per the catalog convention.
    fn update_execution_stats(&self, result: &mut QueryResult, start_time: Instant) {
        use crate::observability::{self as obs, catalog, AttrValue};

        let execution_time = start_time.elapsed();
        // Ensure any non-zero execution time is at least 1ms for reporting
        result.execution_time_ms = if execution_time.is_zero() {
            0
        } else {
            std::cmp::max(1, execution_time.as_millis() as u64)
        };

        // Bounded access-path dimension, sourced from the honest per-query signal
        // the modern SelectExecutor attaches to the result (epic #951/#960). We
        // CONSUME that existing label rather than reinventing it; `None` (legacy
        // executor / non-SELECT) is reported as "unknown" to keep the dimension
        // bounded without fabricating a path.
        let access_path_label: &'static str = result
            .metadata
            .access_path
            .as_ref()
            .map(|p| p.label())
            .unwrap_or("unknown");
        let plan_type_label: &'static str = result
            .metadata
            .plan_info
            .as_ref()
            .map(|p| Self::plan_type_label(&p.plan_type))
            .unwrap_or("unknown");

        // Emit the end-to-end query metrics exactly once, here.
        obs::record_histogram(
            catalog::QUERY_DURATION,
            execution_time.as_secs_f64(),
            &[
                (catalog::attr::SUBSYSTEM, AttrValue::StaticStr("query")),
                (
                    catalog::attr::ACCESS_PATH,
                    AttrValue::StaticStr(access_path_label),
                ),
                (
                    catalog::attr::PLAN_TYPE,
                    AttrValue::StaticStr(plan_type_label),
                ),
            ],
        );
        obs::add_counter(
            catalog::QUERY_ROWS,
            result.rows.len() as u64,
            &[
                (
                    catalog::attr::ACCESS_PATH,
                    AttrValue::StaticStr(access_path_label),
                ),
                (
                    catalog::attr::PLAN_TYPE,
                    AttrValue::StaticStr(plan_type_label),
                ),
            ],
        );

        // Record bounded attributes on the active `query.execute` span (parent of
        // the read-path span tree). Never attach the query text or key values.
        let span = tracing::Span::current();
        span.record(catalog::attr::PLAN_TYPE, plan_type_label);
        span.record(catalog::attr::ACCESS_PATH, access_path_label);
        span.record("cqlite.query.rows", result.rows.len());

        // Lock-free (issue #1595): accumulate the execution time and rows into
        // relaxed atomics. `avg_execution_time_us` is derived at read time as
        // `exec_time_us_sum / total_queries`.
        let new_time_us = execution_time.as_micros() as u64;
        self.stats
            .record_execution(new_time_us, result.rows_affected);
    }

    /// Map a `PlanInfo.plan_type` (an executor `Debug`-formatted plan family such
    /// as `"TableScan"`) onto a bounded, lower-snake label suitable as a metric
    /// dimension and span attribute (issue #1035). Falls back to `"other"` for
    /// any value outside the known taxonomy so the dimension stays bounded.
    fn plan_type_label(plan_type: &str) -> &'static str {
        match plan_type {
            "TableScan" => "table_scan",
            "IndexScan" => "index_scan",
            "PointLookup" => "point_lookup",
            "RangeScan" => "range_scan",
            "Join" => "join",
            "Aggregation" => "aggregation",
            "Subquery" => "subquery",
            _ => "other",
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of prepared statements cached
    pub prepared_cache_size: usize,
    /// Number of query plans cached
    pub plan_cache_size: usize,
    /// Total prepared cache hits
    pub prepared_cache_hits: u64,
    /// Total plan cache hits
    pub plan_cache_hits: u64,
}

/// Query explanation result
#[derive(Debug, Clone)]
pub struct ExplainResult {
    /// Query type
    pub query_type: String,
    /// Plan type
    pub plan_type: String,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
    /// Selected indexes
    pub selected_indexes: Vec<String>,
    /// Execution steps
    pub execution_steps: Vec<String>,
    /// Parallelization information
    pub parallelization_info: Vec<String>,
}

/// Query analysis result
#[derive(Debug, Clone)]
pub struct AnalyzeResult {
    /// Number of iterations
    pub iterations: usize,
    /// Total analysis time
    pub total_time_ms: u64,
    /// Average execution time
    pub avg_time_ms: u64,
    /// Minimum execution time
    pub min_time_ms: u64,
    /// Maximum execution time
    pub max_time_ms: u64,
    /// Standard deviation of execution times
    pub std_dev_ms: u64,
    /// Average rows returned
    pub avg_rows_returned: usize,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
}

#[cfg(all(test, feature = "state_machine"))]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_query_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        assert_eq!(query_engine.stats().total_queries, 0);
        assert_eq!(query_engine.cache_stats().prepared_cache_size, 0);
        assert_eq!(query_engine.cache_stats().plan_cache_size, 0);
    }

    #[tokio::test]
    #[ignore = "Hangs >60s; needs investigation - gated for M1"]
    async fn test_query_caching() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::test_config();
        config.query.query_cache_size = Some(10);

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Execute a query twice
        let cql = "SELECT * FROM users WHERE id = 1";
        let _ = query_engine.execute(cql).await;
        let _ = query_engine.execute(cql).await;

        // Check that plan was cached
        assert_eq!(query_engine.cache_stats().plan_cache_size, 1);

        // Check cache hit ratio
        let stats = query_engine.stats();
        assert!(stats.cache_hit_ratio > 0.0);
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_prepared_statements() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Prepare a statement
        let cql = "SELECT * FROM users WHERE id = ?";
        let prepared = query_engine.prepare(cql).await.unwrap();

        // Execute it with parameters
        let params = vec![Value::Integer(1)];
        let result = query_engine
            .execute_prepared(&prepared, &params)
            .await
            .unwrap();

        // Check that result was generated
        assert!(result.execution_time_ms > 0);

        // Check that statement was cached
        assert_eq!(query_engine.cache_stats().prepared_cache_size, 1);
    }

    #[tokio::test]
    async fn test_query_explain() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Explain a query
        let cql = "SELECT * FROM users WHERE id = 1";
        let explain_result = query_engine.explain(cql).await.unwrap();

        assert_eq!(explain_result.query_type, "Select");
        assert!(explain_result.estimated_cost > 0.0);
        assert!(!explain_result.selected_indexes.is_empty());
        assert!(!explain_result.execution_steps.is_empty());
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_cache_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.query.query_cache_size = Some(2); // Very small cache

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Execute 3 different queries
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 1")
            .await;
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 2")
            .await;
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 3")
            .await;

        // Cache should only have 2 entries due to eviction
        assert_eq!(query_engine.cache_stats().plan_cache_size, 2);
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_schema_validation_api() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Test has_schema_for_table with non-existent table
        let has_schema = query_engine.has_schema_for_table("nonexistent_table").await;
        assert!(!has_schema, "Should return false for non-existent table");

        // Test schema_status with non-existent table
        let status = query_engine.schema_status("nonexistent_table").await;
        match status {
            SchemaStatus::Missing { .. } | SchemaStatus::ExtractionFailed { .. } => {
                // Expected - either missing or extraction failed is correct
            }
            SchemaStatus::Available { .. } => {
                panic!("Should not be Available for non-existent table");
            }
        }
    }

    /// Issue #1587 (E5): the modern (state_machine) ad-hoc SELECT path caches the
    /// optimized plan keyed by the exact CQL text, so N identical ad-hoc executes
    /// of the same query optimize AT MOST ONCE. This is the `engine.rs` half of
    /// the plan-reuse optimization (the prepared-statement half is covered by
    /// `prepared_select_reuses_optimized_plan_across_executes`).
    ///
    /// `#[tokio::test]` runs on the current-thread runtime, so the optimizer's
    /// thread-local invocation counter reflects exactly this future's calls. The
    /// plan is cached BEFORE execution, so the (empty-store) execute result is
    /// irrelevant — reuse holds regardless.
    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn adhoc_select_reuses_cached_optimized_plan() {
        use crate::query::select_optimizer::OPTIMIZE_INVOCATIONS;

        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        // Enable the modern SELECT plan cache (0 disables it).
        config.query.query_cache_size = Some(16);
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());
        let engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // A non-`WHERE id =` SELECT routes through the modern `execute_select_query`
        // path (see `execute`'s `is_simple_id_lookup` gate), which owns the plan cache.
        let cql = "SELECT * FROM t WHERE age > 5";

        OPTIMIZE_INVOCATIONS.with(|c| c.set(0));
        for _ in 0..75 {
            let _ = engine.execute(cql).await;
        }
        let after_same = OPTIMIZE_INVOCATIONS.with(|c| c.get());
        assert!(
            after_same <= 1,
            "issue #1587: an ad-hoc SELECT must optimize at most once across 75 \
             identical executes (modern plan cache), got {after_same}"
        );

        // Clearing the cache must force exactly one re-optimize on the next run.
        engine.clear_plan_cache();
        let _ = engine.execute(cql).await;
        let after_clear = OPTIMIZE_INVOCATIONS.with(|c| c.get());
        assert_eq!(
            after_clear,
            after_same + 1,
            "clearing the plan cache must re-optimize once (no stale reuse)"
        );
    }
}

// Plan-cache HIT path lock-hygiene coverage (issue #1595, spec Requirement 2:
// "Plan-cache hit path uses a shared (read) lock"). Kept in its own file to
// avoid growing this already-oversized module; declared here (a descendant of
// `engine`) so it can read the private `plan_cache`/`select_plan_cache` fields
// and the `pub` `hit_count` atomic directly, with no new public accessor.
#[cfg(test)]
#[path = "engine_lock_hygiene_tests.rs"]
mod engine_lock_hygiene_tests;

#[cfg(test)]
#[cfg(feature = "experimental")]
mod plan_cache_tests {
    use super::*;
    use crate::{
        memory::MemoryManager, platform::Platform, schema::SchemaManager, storage::StorageEngine,
        Config,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_query_engine(config: &Config) -> (QueryEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let platform = Arc::new(Platform::new(config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        let memory = Arc::new(MemoryManager::new(config).unwrap());

        let engine = QueryEngine::new(storage, schema, memory, config).unwrap();
        (engine, temp_dir)
    }

    /// Register the table schema used by the plan-cache tests.
    ///
    /// The write path was removed in Issue #175, so this only issues the
    /// `CREATE TABLE` DDL — no row inserts. The plan-cache assertions below
    /// depend solely on query *planning* (cache population/eviction on simple
    /// `WHERE id = ?` point lookups routed through the legacy executor), not on
    /// any stored rows, so an empty table exercises the current behavior exactly.
    async fn create_sample_table(engine: &QueryEngine) {
        engine
            .execute(
                "CREATE TABLE plan_cache_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_plan_cache_disabled() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(0);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(engine.cache_stats().plan_cache_size, 0);
    }

    #[tokio::test]
    async fn test_plan_cache_reuse_point_lookup() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(4);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine.clear_plan_cache();

        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();
        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(engine.cache_stats().plan_cache_size, 1);
        assert!(engine.stats().cache_hit_ratio > 0.0);
    }

    #[tokio::test]
    async fn test_plan_cache_eviction_limit() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(2);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine.clear_plan_cache();

        for id in 1..=3 {
            engine
                .execute(&format!("SELECT * FROM plan_cache_test WHERE id = {}", id))
                .await
                .unwrap();
        }

        assert_eq!(engine.cache_stats().plan_cache_size, 2);
    }
}
