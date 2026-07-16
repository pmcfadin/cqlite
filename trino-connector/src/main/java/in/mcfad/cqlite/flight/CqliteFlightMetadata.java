package in.mcfad.cqlite.flight;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.RingEntry;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;

/**
 * Connector metadata backed by Sidecar (DDL discovery) and the cqlite-flight
 * server (Arrow schema → Trino column types via {@link ArrowTypeMapper}).
 */
public class CqliteFlightMetadata implements ConnectorMetadata {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOG = System.getLogger(CqliteFlightMetadata.class.getName());
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final CqliteFlightClient flight;

    /**
     * Tables for which the "hiding unsupported-type columns" warning has already
     * been logged, so a table is warned about at most once (getColumnHandles and
     * getTableMetadata both funnel through {@link #supportedFields}). Concurrent-safe
     * for multi-threaded planning.
     */
    private final Set<SchemaTableName> warnedHiddenColumns = ConcurrentHashMap.newKeySet();

    /**
     * Per-{@code (keyspace, table)} memo of the non-aggregated logical row-count
     * statistics (issue #1336). One optimizer planning pass calls
     * {@link #getTableStatistics} repeatedly for the same table; memoizing here bounds
     * it to at most one {@code tokenRangeReplicas} fetch and one {@code table_stats}
     * fetch per table. Concurrent-safe via {@link ConcurrentHashMap#computeIfAbsent}
     * (Trino may plan on multiple threads); the derivation is idempotent and never
     * returns null (it returns {@link TableStatistics#empty()} on any failure).
     *
     * <p>Negative ({@code empty()}) results are memoized on purpose. This cache is scoped
     * to a single short-lived, per-planning-pass {@code ConnectorMetadata} instance, and
     * memoizing {@code empty()} deliberately avoids re-hammering a failing/timing-out
     * Sidecar (with slow timeouts) within that pass. The accepted tradeoff: a transient
     * blip is not retried until the next planning pass (a new metadata instance). This is
     * consistent with the spec's fail-closed requirement, where {@code empty()} is the
     * correct grounded-or-not answer (issue #1336).
     */
    private final Map<SchemaTableName, TableStatistics> nonAggregatedStatsCache =
            new ConcurrentHashMap<>();

    public CqliteFlightMetadata(CqliteFlightConfig config, SidecarClient sidecar, CqliteFlightClient flight) {
        this.config = config;
        this.sidecar = sidecar;
        this.flight = flight;
    }

    /**
     * Query teardown. Under snapshot REUSE (issue #2356/#2306) a snapshot is shared across queries
     * within a freshness window, so it must OUTLIVE the query that created it — per-query cleanup
     * would defeat reuse (and re-flush on the next query). Retirement is therefore NOT per-query:
     * a superseded window is retired when a fresh one replaces it (see {@code SnapshotManager
     * .resolveWindow}/{@code invalidate}), the connector retires all live windows at shutdown
     * ({@code SnapshotManager.retireAll}), and a Sidecar-side TTL backstop reclaims any miss. This
     * hook is intentionally a no-op.
     */
    @Override
    public void cleanupQuery(ConnectorSession session) {
        // Intentionally empty — see javadoc (reuse: snapshots outlive the query, #2356/#2306).
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // Direct queries resolve via getTableHandle; SHOW SCHEMAS enumeration is
        // not wired (Sidecar exposes no keyspace-list endpoint we model yet).
        return List.of();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        try {
            sidecar.schema(schemaName);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        String keyspace = tableName.getSchemaName();
        String table = tableName.getTableName();
        String keyspaceSchema;
        try {
            keyspaceSchema = sidecar.schema(keyspace).schema();
        } catch (SidecarClient.SidecarException e) {
            if (e.statusCode() == 404) {
                return null; // keyspace genuinely not found
            }
            throw e; // a real Sidecar failure must not masquerade as "no table"
        }
        return CreateTableExtractor.extract(keyspaceSchema, keyspace, table)
                .map(ddl -> (ConnectorTableHandle) new CqliteFlightTableHandle(keyspace, table, ddl))
                .orElse(null);
    }

    /**
     * Translate Trino's predicate ({@code constraint.getExpression()}) into a
     * recursive {@code PredicateExpr} tree pushed down in the Flight ticket.
     * Untranslatable parts are returned as a residual expression so Trino keeps
     * post-filtering them — results are always correct; pushdown is a pure
     * optimization.
     *
     * <p>Trino delivers simple, domain-expressible predicates (e.g. a single-partition
     * point read {@code key = 'v'}) not in the expression but as a {@link
     * io.trino.spi.predicate.TupleDomain} in {@code constraint.getSummary()}, leaving
     * the expression {@code TRUE} (issue #2164). We therefore ALSO translate the
     * summary into pushable nodes and merge (AND) them with the expression path's
     * tree. The summary is still returned <em>unchanged</em> as the remaining filter:
     * the server applies the pushed tree best-effort post-decode and Trino keeps its
     * {@code ScanFilter} above, so {@code filterJson} is a pure optimization, never an
     * enforcement guarantee (same honesty posture as {@code applyLimit}'s
     * {@code limitGuaranteed = false}, #2129). Untranslatable expression conjuncts are
     * returned as the residual expression.
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        CqliteFlightTableHandle table = (CqliteFlightTableHandle) handle;

        // Never push a filter onto an already-aggregated handle: the predicate
        // would apply to aggregate OUTPUTS (HAVING-style), which the server cannot
        // evaluate, and rebuilding the handle here would also drop its aggregation
        // state. Trino keeps such filters above the scan.
        if (table.isAggregated()) {
            return Optional.empty();
        }

        ConnectorExpression expression = constraint.getExpression();

        PredicateTreeTranslator.Result result =
                PredicateTreeTranslator.translate(expression, constraint.getAssignments());

        // Also translate the TupleDomain summary (how Trino actually delivers simple
        // col = literal / IN / range predicates). Merge (AND) with the expression tree.
        Optional<JsonNode> domainPushed =
                PredicateTreeTranslator.translateSummary(constraint.getSummary());
        Optional<JsonNode> newlyPushedOpt = combinePushed(result.pushed(), domainPushed);
        if (newlyPushedOpt.isEmpty()) {
            return Optional.empty(); // nothing translatable to push from either path
        }

        // Trino calls applyFilter iteratively, passing only the predicate at the
        // current FilterNode each time — the previously pushed predicate lives on
        // the handle and is NOT re-passed. So ACCUMULATE: combine this call's tree
        // with whatever the handle already carries. Replacing would silently drop
        // an earlier condition whose residual we already reported as satisfied,
        // returning too many rows.
        JsonNode newlyPushed = newlyPushedOpt.get();
        Optional<String> existing = table.filterJson();
        String newlyPushedJson = serialize(newlyPushed);

        // Termination guard: if the handle already carries exactly this predicate,
        // re-combining would duplicate it and loop. Return empty (nothing new).
        if (existing.map(newlyPushedJson::equals).orElse(false)) {
            return Optional.empty();
        }

        JsonNode combined = existing.isPresent()
                ? PredicateTreeTranslator.and(parseFilter(existing.get()), newlyPushed)
                : newlyPushed;
        String filterJson = serialize(combined);

        // No net change after combining → don't re-apply.
        if (existing.map(filterJson::equals).orElse(false)) {
            return Optional.empty();
        }

        // Preserve any pushed-down LIMIT (#2129): this branch only runs for a
        // non-aggregated handle, so aggregation/finalize stay empty, but a prior
        // applyLimit's cap must NOT be dropped when the handle is rebuilt.
        CqliteFlightTableHandle newHandle = new CqliteFlightTableHandle(
                table.keyspace(), table.table(), table.ddl(),
                Optional.of(filterJson), Optional.empty(), Optional.empty(), table.limit());

        // The residual expression Trino must still evaluate (the untranslatable
        // conjuncts, ANDed). Empty residual => TRUE (fully pushed).
        ConnectorExpression remainingExpression = residualExpression(result.residual());

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                // The summary is returned UNCHANGED as the remaining (unenforced) filter:
                // its predicates are pushed into filterJson only as a best-effort server-
                // side optimization, so Trino must keep applying the full domain itself
                // (honesty contract — filterJson is not an enforcement guarantee, #2164).
                constraint.getSummary(),
                remainingExpression,
                false));
    }

    /**
     * AND the expression-path and summary-path pushed trees, or return whichever is
     * present (empty if neither). Kept tiny and pure so applyFilter stays readable.
     */
    private static Optional<JsonNode> combinePushed(
            Optional<JsonNode> expressionPushed, Optional<JsonNode> domainPushed) {
        if (expressionPushed.isPresent() && domainPushed.isPresent()) {
            return Optional.of(PredicateTreeTranslator.and(expressionPushed.get(), domainPushed.get()));
        }
        return expressionPushed.isPresent() ? expressionPushed : domainPushed;
    }

    /**
     * Push a {@code LIMIT} down to the cqlite-flight server (issue #2129).
     *
     * <p>Records the row cap on the handle so every split's Flight ticket carries
     * it and the server stops its k-way compaction-merge after {@code limit}
     * post-filter rows — turning {@code LIMIT k} into bounded per-split work
     * instead of a full range scan (the reported 235s {@code LIMIT 5}).
     *
     * <p>The returned {@link LimitApplicationResult} sets
     * {@code limitGuaranteed = false}: each of the (token-range) splits caps at
     * {@code limit} INDEPENDENTLY, so their union can exceed {@code limit}. This
     * is the standard bounded-scan pattern — correct because Trino keeps its
     * {@code Limit}/{@code LimitPartial} above the scan to do the final global
     * cut. {@code precalculateStatistics = false}: we do not re-derive stats.
     *
     * <p>Declines (returns {@link Optional#empty()}) for an already-aggregated
     * handle (the aggregate already collapses the row set; the server ignores a
     * limit under aggregation), and is idempotent: if the handle already carries
     * a cap at least as tight as {@code limit}, there is nothing new to push, so
     * it returns empty and the optimizer stops iterating.
     */
    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit) {
        CqliteFlightTableHandle table = (CqliteFlightTableHandle) handle;

        // A row cap under an aggregation is meaningless (partial rows already
        // collapse the set) and the server ignores limit+aggregation — decline.
        if (table.isAggregated()) {
            return Optional.empty();
        }

        // Idempotency / termination: an existing cap at least as tight leaves
        // nothing to improve. Returning empty stops the optimizer re-applying the
        // same limit in a loop.
        if (table.limit().isPresent() && table.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        CqliteFlightTableHandle newHandle = new CqliteFlightTableHandle(
                table.keyspace(), table.table(), table.ddl(),
                table.filterJson(), table.aggregationJson(), table.finalizePlanJson(),
                OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(newHandle, false, false));
    }

    private static String serialize(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize pushed-down filter", e);
        }
    }

    private static JsonNode parseFilter(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Invalid pushed-down filter JSON on table handle", e);
        }
    }

    /** Re-assemble residual conjuncts into a single expression (AND), or TRUE if none. */
    private static ConnectorExpression residualExpression(List<ConnectorExpression> residual) {
        if (residual.isEmpty()) {
            return Constant.TRUE;
        }
        if (residual.size() == 1) {
            return residual.get(0);
        }
        return new Call(BooleanType.BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, List.copyOf(residual));
    }

    /**
     * Push {@code count}/{@code sum}/{@code min}/{@code max}/{@code avg} and a
     * simple {@code GROUP BY} down to the cqlite-flight server (issue #841).
     *
     * <p>Trino does NOT re-aggregate across splits: {@code PushAggregationIntoTableScan}
     * rewrites the plan to {@code Project(TableScan)} and removes the
     * {@code AggregationNode}. So this connector must return the FULLY MERGED result.
     * We carry an {@link AggregationSpec} on the handle; for an aggregated handle the
     * split manager emits ONE finalize split that fans out to all token ranges, pulls
     * each range's PARTIAL aggregate, merges, and emits the final row(s).
     *
     * <p>Supported: {@code count(*)}, {@code count(col)}, {@code sum(col)},
     * {@code min(col)}, {@code max(col)}, {@code avg(col)} — simple aggregates with a
     * single {@link Variable} argument (or no argument for {@code count(*)}), no
     * DISTINCT, no filter, no ordering, no expression args, exactly one grouping set,
     * grouping columns must be plain column handles. {@code avg(x)} is decomposed
     * into server {@code Sum(x)} + {@code Count(x)} and combined connector-side. If
     * ANY aggregate or the grouping is unsupported, returns {@link Optional#empty()}
     * (Trino aggregates locally — the correct fallback).
     *
     * <p>The returned {@link AggregationApplicationResult} follows the trino-spi 481
     * contract: {@code projections} aligns 1:1 with the input {@code aggregates};
     * {@code assignments} declares every new scan output (aggregate result columns
     * and grouping columns) with its Trino result type; {@code groupingColumnMapping}
     * maps each original grouping {@link ColumnHandle} to its new handle.
     */
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        CqliteFlightTableHandle table = (CqliteFlightTableHandle) handle;

        // Already aggregated? Decline — we do not support stacking aggregations.
        if (table.isAggregated()) {
            return Optional.empty();
        }

        // Exactly one grouping set (no ROLLUP/CUBE/GROUPING SETS).
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }
        List<ColumnHandle> groupingSet = groupingSets.get(0);
        boolean hasGroupBy = !groupingSet.isEmpty();

        // Grouping columns must be plain column handles (projectable).
        List<CqliteFlightColumnHandle> groupingColumns = new ArrayList<>();
        java.util.Set<String> groupByNameSet = new java.util.HashSet<>();
        for (ColumnHandle gc : groupingSet) {
            if (!(gc instanceof CqliteFlightColumnHandle col)) {
                return Optional.empty();
            }
            // Decline GROUP BY on float/double: non-finite keys (NaN/±Inf) do not
            // have well-defined SQL grouping/round-trip semantics here, so leave
            // such grouping to Trino. (Grouping on float is rare.)
            if (col.type() instanceof io.trino.spi.type.RealType
                    || col.type() instanceof DoubleType) {
                return Optional.empty();
            }
            groupingColumns.add(col);
            groupByNameSet.add(col.name());
        }

        // Translate each Trino aggregate into one or more server partial aggregates,
        // assigning deterministic output names (agg0, agg1, ...) that are kept
        // DISTINCT from the grouping column names — both share one Arrow partial
        // schema and are looked up by name in the finalize plan, so a real column
        // literally named "agg0" used in GROUP BY must not collide.
        List<AggregationSpec.Aggregate> serverAggregates = new ArrayList<>();
        List<Assignment> assignmentList = new ArrayList<>();
        List<ConnectorExpression> projections = new ArrayList<>();
        List<FinalizeAggregationPlan.OutputColumn> outputPlan = new ArrayList<>();
        int[] outputCounter = {0};

        for (AggregateFunction aggregate : aggregates) {
            // No DISTINCT, no FILTER, no ORDER BY inside the aggregate.
            if (aggregate.isDistinct() || aggregate.getFilter().isPresent()
                    || !aggregate.getSortItems().isEmpty()) {
                return Optional.empty();
            }

            String func = aggregate.getFunctionName().toLowerCase(java.util.Locale.ROOT);
            List<ConnectorExpression> args = aggregate.getArguments();
            Type outputType = aggregate.getOutputType();

            // Resolve the single column argument (or none for count(*)).
            Optional<CqliteFlightColumnHandle> arg = singleColumnArg(args, assignments);
            boolean noArg = args.isEmpty();
            if (!noArg && arg.isEmpty()) {
                return Optional.empty(); // expression arg, multiple args, etc. — unsupported
            }

            switch (func) {
                case "count" -> {
                    // count(*) needs nothing (null column on the wire); count(col) is a
                    // non-null count — the column just needs to exist.
                    String column = noArg ? null : arg.get().name();
                    String out = nextOutput(outputCounter, groupByNameSet);
                    serverAggregates.add(new AggregationSpec.Aggregate(
                            AggregationSpec.Func.Count, column, out));
                    addResult(out, BigintType.BIGINT, assignmentList, projections);
                    outputPlan.add(new FinalizeAggregationPlan.OutputColumn(
                            out, FinalizeAggregationPlan.Kind.DIRECT, out, null));
                }
                case "sum" -> {
                    if (arg.isEmpty() || !supportsValueAggregate(arg.get())) {
                        return Optional.empty();
                    }
                    String out = nextOutput(outputCounter, groupByNameSet);
                    serverAggregates.add(new AggregationSpec.Aggregate(
                            AggregationSpec.Func.Sum, arg.get().name(), out));
                    addResult(out, outputType, assignmentList, projections);
                    outputPlan.add(new FinalizeAggregationPlan.OutputColumn(
                            out, FinalizeAggregationPlan.Kind.DIRECT, out, null));
                }
                case "min", "max" -> {
                    if (arg.isEmpty() || !supportsValueAggregate(arg.get())) {
                        return Optional.empty();
                    }
                    // Float/double min/max push too (issue #896): both the Rust
                    // accumulator and the Java merger order NaN as the largest value
                    // (Double.compare semantics), so the result matches Trino's
                    // non-pushed min/max and is independent of input row order.
                    AggregationSpec.Func f = func.equals("min")
                            ? AggregationSpec.Func.Min : AggregationSpec.Func.Max;
                    String out = nextOutput(outputCounter, groupByNameSet);
                    serverAggregates.add(new AggregationSpec.Aggregate(f, arg.get().name(), out));
                    addResult(out, outputType, assignmentList, projections);
                    outputPlan.add(new FinalizeAggregationPlan.OutputColumn(
                            out, FinalizeAggregationPlan.Kind.DIRECT, out, null));
                }
                case "avg" -> {
                    if (arg.isEmpty() || !supportsValueAggregate(arg.get())) {
                        return Optional.empty();
                    }
                    // avg(x) -> server SumDouble(x) + Count(x); combined connector-side
                    // as ΣSum/ΣCount. SumDouble totals in f64 even for integer columns,
                    // so the numerator never overflows — matching Trino's 128-bit avg
                    // (which never fails), unlike a checked-i64 Sum (issue #902). The
                    // connector emits ONE merged DOUBLE result column named by sumOut
                    // (the pair's first output); Trino references that.
                    String sumOut = nextOutput(outputCounter, groupByNameSet);
                    String countOut = nextOutput(outputCounter, groupByNameSet);
                    serverAggregates.add(new AggregationSpec.Aggregate(
                            AggregationSpec.Func.SumDouble, arg.get().name(), sumOut));
                    serverAggregates.add(new AggregationSpec.Aggregate(
                            AggregationSpec.Func.Count, arg.get().name(), countOut));
                    addResult(sumOut, DoubleType.DOUBLE, assignmentList, projections);
                    outputPlan.add(new FinalizeAggregationPlan.OutputColumn(
                            sumOut, FinalizeAggregationPlan.Kind.AVG, sumOut, countOut));
                }
                default -> {
                    return Optional.empty(); // unsupported aggregate function
                }
            }
        }

        // Cardinality / operator gate (issue #893, #944). Run AFTER aggregate/argument
        // support validation so we only fetch (network) stats for an aggregation that is
        // OTHERWISE PUSHABLE — an aggregation declined above for an unsupported function,
        // argument shape, DISTINCT/FILTER/ORDER BY, etc. never fans out to Sidecar/Flight
        // for stats. Global aggregates (no GROUP BY) are an unconditional data-reduction
        // win and are NEVER gated — they bypass this check. For a GROUP BY, the
        // single-finalize-split design degrades to break-even-to-loss as distinct groups
        // approach the row count, so honor the operator policy and decline when the
        // estimated group ratio is too high. The ratio is only needed for the AUTOMATIC
        // policy, so the stats fetch is skipped under NEVER/ALWAYS too.
        if (hasGroupBy && declineGroupByPushdown(
                groupByPolicy(), groupRatioForGate(table, groupingColumns), maxGroupRatio())) {
            return Optional.empty();
        }

        // Grouping columns pass through. They are threaded ONLY via
        // groupingColumnMapping (original handle -> new handle) — NOT added to
        // `assignments`, which is reserved for the new aggregate-result variables.
        // Adding a grouping column here too would make Trino's symbol<->handle
        // BiMap see the same ColumnHandle under two symbols ("Multiple entries
        // with same value"). The finalize page source still emits a block per
        // grouping column via the GROUP entries in the finalize plan.
        Map<ColumnHandle, ColumnHandle> groupingColumnMapping = new LinkedHashMap<>();
        List<String> groupByNames = new ArrayList<>();
        for (CqliteFlightColumnHandle col : groupingColumns) {
            groupByNames.add(col.name());
            // Passthrough: the grouping column's handle is unchanged in the result.
            groupingColumnMapping.put(col, col);
            outputPlan.add(new FinalizeAggregationPlan.OutputColumn(
                    col.name(), FinalizeAggregationPlan.Kind.GROUP, col.name(), null));
        }

        AggregationSpec spec = new AggregationSpec(groupByNames, serverAggregates);
        String aggregationJson = serialize(spec.toJson(MAPPER));
        FinalizeAggregationPlan plan = new FinalizeAggregationPlan(groupByNames, outputPlan);
        String finalizePlanJson = serialize(plan.toJson(MAPPER));

        CqliteFlightTableHandle newHandle = new CqliteFlightTableHandle(
                table.keyspace(), table.table(), table.ddl(),
                table.filterJson(), Optional.of(aggregationJson), Optional.of(finalizePlanJson));

        return Optional.of(new AggregationApplicationResult<>(
                newHandle,
                projections,
                assignmentList,
                groupingColumnMapping,
                false));
    }

    /** Effective GROUP BY pushdown policy (defaults to AUTOMATIC when no config is set). */
    private GroupByPushdownPolicy groupByPolicy() {
        return config != null ? config.groupByPushdown() : GroupByPushdownPolicy.AUTOMATIC;
    }

    /** Effective groups/rows crossover for the AUTOMATIC gate. */
    private double maxGroupRatio() {
        return config != null ? config.maxGroupRatio() : CqliteFlightConfig.DEFAULT_MAX_GROUP_RATIO;
    }

    /**
     * Decide whether to DECLINE GROUP BY aggregation pushdown (issue #893). Pure
     * decision over the operator policy and an optional estimated groups/rows ratio:
     *
     * <ul>
     *   <li>{@link GroupByPushdownPolicy#NEVER} — always decline.</li>
     *   <li>{@link GroupByPushdownPolicy#ALWAYS} — never decline (push).</li>
     *   <li>{@link GroupByPushdownPolicy#AUTOMATIC} — decline only when an estimate is
     *       present AND it exceeds {@code maxGroupRatio}; with no estimate, push (always
     *       correct, only a possible perf loss in the rare high-cardinality case).</li>
     * </ul>
     */
    static boolean declineGroupByPushdown(
            GroupByPushdownPolicy policy, java.util.OptionalDouble estimatedGroupRatio, double maxGroupRatio) {
        return switch (policy) {
            case NEVER -> true;
            case ALWAYS -> false;
            case AUTOMATIC -> estimatedGroupRatio.isPresent()
                    && estimatedGroupRatio.getAsDouble() > maxGroupRatio;
        };
    }

    /**
     * Resolve the estimated groups/rows ratio for the gate. The ratio only matters
     * under {@link GroupByPushdownPolicy#AUTOMATIC}, so for NEVER/ALWAYS we skip the
     * (network) statistics fetch entirely and return empty. For AUTOMATIC we fetch
     * the authoritative per-table counts and feed them to the pure
     * {@link #estimateGroupRatio} mapping. Any failure fetching stats degrades to
     * empty (no estimate → AUTOMATIC pushes, always correct).
     */
    private java.util.OptionalDouble groupRatioForGate(
            CqliteFlightTableHandle table, List<CqliteFlightColumnHandle> groupingColumns) {
        if (groupByPolicy() != GroupByPushdownPolicy.AUTOMATIC) {
            return java.util.OptionalDouble.empty();
        }
        TableStats stats;
        try {
            stats = fetchTableStats(table);
        } catch (RuntimeException e) {
            // No estimate available → AUTOMATIC pushes (correct, may risk the rare
            // high-cardinality perf loss). Never block a query on a stats fetch.
            return java.util.OptionalDouble.empty();
        }
        List<String> groupByNames = new ArrayList<>();
        for (CqliteFlightColumnHandle col : groupingColumns) {
            groupByNames.add(col.name());
        }
        return estimateGroupRatio(table.ddl(), groupByNames, stats);
    }

    /**
     * Estimate distinct-groups / rows for a GROUP BY (issue #944), used by the
     * AUTOMATIC gate. PURE function of the table DDL, the grouping column names, and
     * the AUTHORITATIVE per-table counts — no network, fully unit-testable.
     *
     * <p><b>Authoritative inputs</b> (from {@code cqlite-core} Statistics.db parse,
     * surfaced via the Flight {@code table_stats} action): {@code stats.liveRows} and
     * {@code stats.partitionCount}. Both are per-SSTable / per-node SUMS, i.e. UPPER
     * BOUNDS on the table's true counts (a partition can appear in several SSTables /
     * be replicated across nodes). An upper bound on distinct groups never
     * under-counts, so the gate never wrongly pushes a high-cardinality GROUP BY.
     *
     * <p><b>Derived mapping</b> (best-effort, from the grouping shape vs the DDL
     * primary key — NOT a true NDV gate; Cassandra 5.0 stores no reliable
     * per-regular-column or per-clustering-prefix NDV, so we never invent per-column
     * cardinality). We only claim a bound in the two shapes the authoritative
     * partition/row counts actually bound:
     * <ul>
     *   <li><b>GROUP BY reaches full row uniqueness</b> (partition key + ALL
     *       clustering columns ⊆ grouping) → groups ≈ rows → ratio ≈ 1.0 → DECLINE.</li>
     *   <li><b>GROUP BY = the full partition key AND the table has NO clustering
     *       columns</b> → one group per partition → groups ≈ partitionCount, bounded
     *       above by rows → ratio = partitionCount / rows.</li>
     *   <li><b>Everything else is UNBOUNDED</b> → empty → default PUSH (safe). This
     *       includes a full partition key plus a PARTIAL subset of clustering columns
     *       (group count can approach row cardinality, e.g. GROUP BY pk, ck1 with
     *       PRIMARY KEY (pk, ck1, ck2) — no per-prefix NDV exists to bound it, so we
     *       must NOT fabricate partitionCount/rows), a partition-key PREFIX, and
     *       grouping on a non-key or low-cardinality column.</li>
     * </ul>
     *
     * <p>Returns {@link java.util.OptionalDouble#empty()} when no row count is
     * available (rows == 0) or the shape is unbounded — the gate then pushes.
     */
    static java.util.OptionalDouble estimateGroupRatio(
            String ddl, List<String> groupingColumns, TableStats stats) {
        // Fail closed to "no estimate" when the per-table counts are INCOMPLETE
        // (some node's Statistics.db failed to decode; issue #944, #28). Partial
        // sums are not authoritative — a skipped SSTable could hold many rows/groups,
        // biasing the ratio low and PUSHING a high-cardinality GROUP BY that should
        // have been declined. Empty here means AUTOMATIC pushes (the safe default).
        if (!stats.complete()) {
            return java.util.OptionalDouble.empty();
        }
        long rows = stats.liveRows();
        long partitions = stats.partitionCount();
        if (rows <= 0 || groupingColumns.isEmpty()) {
            // No rows to reason about, or a global aggregate (handled before the gate).
            return java.util.OptionalDouble.empty();
        }

        PrimaryKeyExtractor.Keys keys = PrimaryKeyExtractor.extract(ddl);
        if (keys.partitionKey().isEmpty()) {
            // DDL primary key not parseable → cannot bound → push.
            return java.util.OptionalDouble.empty();
        }

        // Per-column CQL identifier matching: an UNQUOTED key column matches a
        // grouping name case-insensitively (CQL folds unquoted identifiers); a QUOTED
        // key column matches only its exact case (CQL preserves quoted identifiers).
        // The flag rides on each KeyColumn from the extractor, so quoted "Id" is NOT
        // conflated with unquoted id.
        List<PrimaryKeyExtractor.KeyColumn> partitionKey = keys.partitionKey();
        List<PrimaryKeyExtractor.KeyColumn> clustering = keys.clusteringColumns();

        boolean coversPartitionKey = coversAll(partitionKey, groupingColumns);
        boolean coversAllClustering = coversAll(clustering, groupingColumns);
        // Is EVERY grouping column a partition-key column? Together with
        // coversPartitionKey this is true iff the grouping set is EXACTLY the partition
        // key — no extra clustering AND no extra regular (non-key) columns. A regular
        // column in the grouping (e.g. GROUP BY pk, v) can split a single partition into
        // one group per row, so partitionCount is NOT a valid bound there.
        boolean allGroupingArePartitionKey = allMatchSomeKey(partitionKey, groupingColumns);

        if (coversPartitionKey && coversAllClustering) {
            // Grouping reaches full row uniqueness (partition key + ALL clustering
            // columns) → one group per row → ratio ≈ 1.0 → DECLINE.
            return java.util.OptionalDouble.of(1.0);
        }
        if (coversPartitionKey && allGroupingArePartitionKey) {
            // Grouping is EXACTLY the partition key (every PK column is grouped AND every
            // grouping column is a PK column — no clustering, no regular columns): each
            // group is exactly one distinct partition → groups ≈ partition count, never
            // more than the row count. This is the ONLY shape bounded by the
            // authoritative partition count (whether or not the table also HAS clustering
            // columns, since the grouping does not slice partitions any finer).
            double ratio = (double) Math.min(partitions, rows) / (double) rows;
            return java.util.OptionalDouble.of(ratio);
        }
        // Everything else is UNBOUNDED from the authoritative stats → push (safe
        // default). This includes:
        //   - GROUP BY = full partition key + a PARTIAL (non-empty, non-full) subset
        //     of clustering columns. Per-clustering-prefix NDV is NOT stored by
        //     Cassandra 5.0; group count can approach the row count, so partitionCount
        //     is NOT a valid bound — fabricating partitionCount/rows here would let the
        //     gate push exactly the high-cardinality aggregation it exists to decline.
        //   - GROUP BY = full partition key + a REGULAR (non-key) column (e.g.
        //     GROUP BY pk, v). A regular column has no stored NDV and can produce one
        //     group per row, so partitionCount is NOT a valid bound here either.
        //   - A partition-key PREFIX, or grouping on a non-key / low-cardinality column.
        return java.util.OptionalDouble.empty();
    }

    /**
     * True iff EVERY key column is matched by some grouping name under CQL identifier
     * rules (unquoted: case-insensitive; quoted: exact). An empty key list is trivially
     * covered.
     */
    private static boolean coversAll(
            List<PrimaryKeyExtractor.KeyColumn> keyColumns, List<String> groupingColumns) {
        for (PrimaryKeyExtractor.KeyColumn key : keyColumns) {
            boolean matched = false;
            for (String g : groupingColumns) {
                if (key.matches(g)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    /**
     * True iff EVERY grouping name is matched by some key column in {@code keyColumns}
     * (same CQL identifier rules as {@link #coversAll}). Used with {@link #coversAll} to
     * decide whether a grouping set is EXACTLY the given key set: no extra clustering and
     * no extra regular (non-key) columns. An empty grouping list is trivially all-matched.
     */
    private static boolean allMatchSomeKey(
            List<PrimaryKeyExtractor.KeyColumn> keyColumns, List<String> groupingColumns) {
        for (String g : groupingColumns) {
            boolean matched = false;
            for (PrimaryKeyExtractor.KeyColumn key : keyColumns) {
                if (key.matches(g)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fetch the AUTHORITATIVE per-table statistics by summing the
     * {@code table_stats} action across the REPLICA hosts that own this keyspace
     * (issue #944). The target host set is scoped to the keyspace's read replicas
     * (from {@link SidecarClient#tokenRangeReplicas}) rather than the whole cluster
     * ring: in multi-DC / keyspace-scoped replication a ring entry can be a node that
     * does NOT host this keyspace at all, and fanning out to it would either waste an
     * RPC or — if that node answered {@code not_found} — wrongly taint completeness.
     * Summing across the keyspace's replicas over-counts by the replication factor,
     * which is acceptable for an UPPER-BOUND gate input (it only ever errs toward
     * DECLINING pushdown).
     *
     * <p>If ANY replica host that should have been queried is UNREACHABLE (a transport
     * failure), the aggregate is tainted INCOMPLETE: we fold {@link TableStats#UNAVAILABLE}
     * so the AND-of-{@code complete} in {@link TableStats#plus} becomes {@code false} and
     * the caller fails closed to "no estimate" (issue #944, #28). A replica that RESPONDS
     * {@code not_found} (it does not currently host this keyspace/table — e.g. replica
     * metadata drift) is NOT a failure: it contributes nothing ({@link TableStats#EMPTY})
     * and does NOT taint completeness.
     *
     * <p>Package-private and overridable as a test seam (issue #944): a subclass can
     * observe whether this planning-time stats fetch was invoked. It MUST NOT be for an
     * aggregation declined for an unsupported function/argument shape (the fetch now runs
     * only on the otherwise-pushable path), and MUST be for a supported AUTOMATIC GROUP BY.
     */
    TableStats fetchTableStats(CqliteFlightTableHandle handle) {
        return fetchTableStats(handle, tokenRangeReplicas(handle.keyspace()));
    }

    /**
     * Same as {@link #fetchTableStats(CqliteFlightTableHandle)} but over an
     * ALREADY-FETCHED token-range replica set (issue #1336). The logical-row-count path
     * derives the divisor and the target host set from ONE {@code tokenRangeReplicas}
     * response, so it must not fetch it a second time here. Package-private and
     * overridable as a test seam.
     */
    TableStats fetchTableStats(CqliteFlightTableHandle handle, TokenRangeReplicasResponse replicas) {
        byte[] body = tableStatsRequest(handle.keyspace(), handle.table());
        int port = config.flightPort();
        // Bound each per-replica DoAction with a deadline (issue #944): this runs during
        // query planning, so a slow/half-open endpoint must degrade to "no estimate"
        // (a TIMED_OUT FlightRuntimeException → the fetch-failure → UNAVAILABLE path in
        // aggregateNodeStats), never stall the planner.
        long timeoutMillis = config.tableStatsTimeoutMillis();
        Set<String> hosts = replicaHosts(replicas, config.localDatacenter());
        return aggregateNodeStats(hosts, address -> flight.tableStats(address, port, body, timeoutMillis));
    }

    /**
     * The keyspace's authoritative token-range read replicas from Sidecar. Package-
     * private and overridable as a test seam (issue #1336) so the logical-row-count
     * derivation can be driven end to end over decoded JSON fixtures without a live
     * Sidecar.
     */
    TokenRangeReplicasResponse tokenRangeReplicas(String keyspace) {
        return sidecar.tokenRangeReplicas(keyspace);
    }

    /**
     * Distinct replica HOSTS that own a keyspace, derived from its token-range read
     * replicas (issue #944). The Sidecar returns replicas as {@code "ip:storage_port"}
     * grouped by datacenter; we strip the port (the flight server listens on the
     * configured flight port at the same host) and de-duplicate across ranges.
     *
     * <p>The datacenter selection MUST match {@link CqliteFlightSplitManager#buildSplits}
     * (via {@link CqliteFlightSplitManager#pickReplica}): split selection reads each range
     * ONLY from the configured {@code localDatacenter} when that DC has replicas for the
     * range, falling back to all DCs only when the local DC has none (or no local DC is
     * configured). Fanning {@code table_stats} out to remote-DC replicas that the query
     * would never read adds planning-time timeouts and can wrongly taint completeness
     * against unreachable remote nodes — so this method applies the same per-range
     * DC scoping. Static and package-private for unit testing without a live Sidecar.
     */
    static Set<String> replicaHosts(
            TokenRangeReplicasResponse replicas,
            String localDatacenter) {
        Set<String> hosts = new LinkedHashSet<>();
        if (replicas == null || replicas.readReplicas() == null) {
            return hosts;
        }
        for (var range : replicas.readReplicas()) {
            hosts.addAll(scopedRangeReplicas(range, localDatacenter));
        }
        return hosts;
    }

    /**
     * The distinct scoped read-replica HOSTS (ports stripped) of ONE token range,
     * under the SAME DC-scoping rule {@link #replicaHosts} and split selection use:
     * the local DC's replicas when it has any for the range, else every DC's replicas.
     * Deduplicates replica entries within the range (a host repeated in the range's
     * lists is one host). Returns an empty set for a null/absent replica map.
     */
    private static Set<String> scopedRangeReplicas(ReplicaInfo range, String localDatacenter) {
        Set<String> hosts = new LinkedHashSet<>();
        if (range == null) {
            return hosts;
        }
        Map<String, List<String>> byDc = range.replicasByDatacenter();
        if (byDc == null) {
            return hosts;
        }
        // Mirror buildSplits/pickReplica DC scoping: use the local DC's replicas for
        // this range when present, else fall back to every DC's replicas.
        List<String> local = localDatacenter != null ? byDc.get(localDatacenter) : null;
        if (local != null && !local.isEmpty()) {
            addHosts(hosts, local);
        } else {
            for (List<String> dcReplicas : byDc.values()) {
                addHosts(hosts, dcReplicas);
            }
        }
        return hosts;
    }

    /**
     * The uniform per-token-range distinct scoped read-replica count, or empty when it
     * is not well-defined (issue #1336). This is the divisor that turns the physical,
     * replica-summed {@code table_stats} row total into a logical (de-replicated)
     * cardinality: summing whole-table {@code live_rows} across the distinct scoped
     * replica hosts counts each logical row once per replica that stores it, i.e.
     * exactly {@code R} times when {@code R} is the shared per-range replica count.
     *
     * <p>The count is derived ONLY by counting distinct scoped replicas per range in the
     * authoritative {@code tokenRangeReplicas} response, under the identical
     * {@code localDatacenter} scoping as {@link #replicaHosts} (so the divisor matches
     * the host set the stats sum was collected from). It NEVER parses keyspace
     * {@code replication = {...}} strategy strings and makes no distributional
     * assumption (no-heuristics mandate, issue #28).
     *
     * <p>Returns {@link OptionalInt#empty()} (fail closed → the caller reports no row
     * count) when: there are no ranges, any range has zero scoped replicas, or the
     * per-range counts are not identical across all ranges (e.g. a topology mid-
     * transition). A present value is guaranteed {@code >= 1}.
     */
    static OptionalInt uniformReplicaCount(
            TokenRangeReplicasResponse replicas, String localDatacenter) {
        if (replicas == null || replicas.readReplicas() == null
                || replicas.readReplicas().isEmpty()) {
            return OptionalInt.empty();
        }
        int shared = -1;
        for (var range : replicas.readReplicas()) {
            int count = scopedRangeReplicas(range, localDatacenter).size();
            if (count < 1) {
                // A range with no scoped replica → cannot ground the divisor.
                return OptionalInt.empty();
            }
            if (shared == -1) {
                shared = count;
            } else if (shared != count) {
                // Non-uniform per-range replica counts (topology in transition).
                return OptionalInt.empty();
            }
        }
        return OptionalInt.of(shared);
    }

    private static void addHosts(Set<String> hosts, List<String> replicas) {
        if (replicas == null) {
            return;
        }
        for (String replica : replicas) {
            if (replica != null && !replica.isBlank()) {
                hosts.add(CqliteFlightSplitManager.hostOnly(replica));
            }
        }
    }

    /**
     * Aggregate per-host {@code table_stats} across the DISTINCT replica
     * {@code hosts} by summing {@link TableStats#plus} (issue #944). Pulled out as a
     * package-private static seam so the completeness-classification behavior can be
     * unit tested without a live Sidecar/Flight client.
     *
     * <p>For each distinct, non-null host {@code fetch} is invoked:
     * <ul>
     *   <li>SUCCESS — its (possibly incomplete) response is summed.</li>
     *   <li>{@code not_found} — the host responded that it does not host this
     *       keyspace/table. This is NOT a failure: it contributes
     *       {@link TableStats#EMPTY} (nothing) and does NOT taint completeness. A
     *       legitimate not-hosting replica must not disable the gate everywhere.</li>
     *   <li>any other RuntimeException (transport/unreachable) — the host should have
     *       answered but could not, so it is NOT silently dropped: an incomplete
     *       {@link TableStats#UNAVAILABLE} sentinel is folded in, tainting the
     *       aggregate to {@code complete=false} so the caller fails closed.</li>
     * </ul>
     */
    static TableStats aggregateNodeStats(
            Iterable<String> hosts, java.util.function.Function<String, TableStats> fetch) {
        TableStats total = TableStats.EMPTY;
        Set<String> seen = new LinkedHashSet<>();
        for (String address : hosts) {
            if (address == null || !seen.add(address)) {
                continue;
            }
            try {
                total = total.plus(fetch.apply(address));
            } catch (RuntimeException e) {
                if (isNotHosting(e)) {
                    // The replica responded "table/keyspace absent" (Flight NOT_FOUND).
                    // It legitimately hosts no data for this table — contribute nothing
                    // and do NOT taint completeness, so a not-hosting replica in a
                    // multi-DC / keyspace-scoped layout cannot disable the gate.
                    continue;
                }
                // A replica we should have reached could not answer (transport failure).
                // Do NOT silently drop it: fold an incomplete sentinel so the aggregate
                // is marked incomplete (complete=false), forcing the caller to "no
                // estimate" instead of treating peers' partial totals as authoritative.
                total = total.plus(TableStats.UNAVAILABLE);
            }
        }
        return total;
    }

    /**
     * Classify a {@code table_stats} fetch failure: a Flight {@code NOT_FOUND} status
     * means the queried node does not host this keyspace/table (it responded), which
     * is a legitimate not-a-failure that must NOT taint completeness. Any other error
     * (transport/unreachable, internal) DOES taint. The NOT_FOUND status can be wrapped
     * (the client rethrows runtime exceptions and wraps checked ones), so we walk the
     * cause chain looking for a Flight {@link org.apache.arrow.flight.FlightRuntimeException}
     * carrying {@link org.apache.arrow.flight.FlightStatusCode#NOT_FOUND}.
     */
    static boolean isNotHosting(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof org.apache.arrow.flight.FlightRuntimeException fre
                    && fre.status() != null
                    && fre.status().code() == org.apache.arrow.flight.FlightStatusCode.NOT_FOUND) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /** Build the {@code table_stats} action body (JSON {@code TableStatsRequest}). */
    private static byte[] tableStatsRequest(String keyspace, String table) {
        var root = MAPPER.createObjectNode();
        root.put("keyspace", keyspace);
        root.put("table", table);
        // snapshot=null on purpose: this stats fetch runs during query PLANNING, but the
        // per-query Sidecar snapshot (SnapshotManager) is not created until split planning,
        // which happens AFTER the optimizer has asked for these statistics. There is no
        // snapshot to name yet, so we estimate off the live data directory. In SNAPSHOT
        // read mode the scan itself still reads the immutable snapshot; only this row-count
        // ESTIMATE comes from the live dir. That is a plan-quality concern (the optimizer may
        // pick a slightly different join/agg strategy), never a correctness one — the returned
        // rows always come from the snapshot. We deliberately do NOT create the snapshot
        // earlier just to tighten this estimate (issue #2113 / N4).
        root.putNull("snapshot");
        try {
            return MAPPER.writeValueAsBytes(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize table_stats request", e);
        }
    }

    /**
     * Generate the next deterministic aggregate output name (agg0, agg1, ...),
     * skipping any name that collides with a grouping column — both share the
     * partial Arrow schema and are resolved by name in the finalize plan.
     */
    private static String nextOutput(int[] counter, java.util.Set<String> reserved) {
        String name;
        do {
            name = "agg" + (counter[0]++);
        } while (reserved.contains(name));
        return name;
    }

    /**
     * Resolve an aggregate's single {@link Variable} argument to its column handle.
     * Returns empty if there is not exactly one argument, the argument is not a bare
     * Variable, or the variable does not resolve to a {@link CqliteFlightColumnHandle}.
     */
    private static Optional<CqliteFlightColumnHandle> singleColumnArg(
            List<ConnectorExpression> args, Map<String, ColumnHandle> assignments) {
        if (args.size() != 1) {
            return Optional.empty();
        }
        if (!(args.get(0) instanceof Variable v)) {
            return Optional.empty();
        }
        ColumnHandle ch = assignments.get(v.getName());
        if (ch instanceof CqliteFlightColumnHandle col) {
            return Optional.of(col);
        }
        return Optional.empty();
    }

    /**
     * Sum/min/max/avg need the server to compare/accumulate values, which requires a
     * fully-pushable column (FULL capability). count only needs the column to exist.
     */
    private static boolean supportsValueAggregate(CqliteFlightColumnHandle col) {
        return col.capability() == PushdownCapability.FULL;
    }

    /** Record one aggregate result column: its assignment and its projection Variable. */
    private static void addResult(
            String output, Type type, List<Assignment> assignments, List<ConnectorExpression> projections) {
        assignments.add(new Assignment(output, new CqliteFlightColumnHandle(output, type), type));
        projections.add(new Variable(output, type));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.keyspace(), handle.table());
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Field field : supportedFields(tableName, arrowSchema(handle), warnedHiddenColumns)) {
            handles.put(field.getName(), new CqliteFlightColumnHandle(
                    field.getName(), ArrowTypeMapper.toTrino(field), ArrowTypeMapper.capabilityOf(field)));
        }
        return handles;
    }

    /**
     * Filter an Arrow schema down to the columns whose type the connector can
     * materialize, hiding the rest from the Trino schema (owner decision, issue
     * #2229: <em>hide + warn</em>). Columns of an unsupported type (decimal,
     * varint, collections, tuples, UDTs) are omitted consistently from both the
     * column handles and the table metadata, so DESCRIBE shows only supported
     * columns and {@code SELECT *} succeeds. The first time a given table is seen
     * with hidden columns, a single warning names them and their Arrow type.
     *
     * <p>When <em>every</em> column is unsupported the table is genuinely
     * unqueryable, so we fail with a clear {@link StandardErrorCode#NOT_SUPPORTED}
     * error rather than presenting a zero-column table (which Trino handles poorly
     * and would silently return empty rows).
     */
    static List<Field> supportedFields(
            SchemaTableName tableName, Schema schema, Set<SchemaTableName> warned) {
        List<Field> supported = new ArrayList<>();
        List<String> hidden = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (ArrowTypeMapper.toTrinoOrEmpty(field).isPresent()) {
                supported.add(field);
            } else {
                hidden.add(field.getName() + " (arrow " + field.getType() + ")");
            }
        }
        if (!hidden.isEmpty() && warned.add(tableName)) {
            LOG.log(System.Logger.Level.WARNING,
                    "Table " + tableName + ": hiding " + hidden.size()
                            + " column(s) of unsupported type from the Trino schema: "
                            + String.join(", ", hidden));
        }
        if (supported.isEmpty() && !schema.getFields().isEmpty()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                    "Table " + tableName + " has no Trino-supported columns; all "
                            + schema.getFields().size() + " column(s) use unsupported CQL types: "
                            + String.join(", ", hidden));
        }
        return supported;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {
        CqliteFlightColumnHandle column = (CqliteFlightColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.keyspace(), handle.table());
        List<ColumnMetadata> columns = new ArrayList<>();
        for (Field field : supportedFields(tableName, arrowSchema(handle), warnedHiddenColumns)) {
            columns.add(new ColumnMetadata(field.getName(), ArrowTypeMapper.toTrino(field)));
        }
        return new ConnectorTableMetadata(tableName, columns);
    }

    /**
     * Surface optimizer row-count statistics to Trino (issue #944).
     *
     * <p>For an <em>aggregated</em> handle the scan's OUTPUT cardinality is the
     * aggregate-result cardinality, NOT the base-table row count (issue #944): a
     * GLOBAL aggregate (no GROUP BY) emits exactly one row, so we report {@code
     * ROW_COUNT = 1}; a grouped aggregate's output group count is not authoritatively
     * known, so we return {@link TableStatistics#empty()} and let Trino estimate
     * rather than fabricate (#28).
     *
     * <p>For a <em>non-aggregated</em> handle we report a LOGICAL (de-replicated) row
     * count {@code ROW_COUNT = live_rows / R} (issue #1336). {@code live_rows} is the
     * existing whole-table {@code table_stats} sum across the keyspace's distinct scoped
     * replica hosts (≈ RF × the logical cardinality on a replicated keyspace); {@code R}
     * is the {@link #uniformReplicaCount uniform per-token-range distinct scoped replica
     * count}. Dividing by {@code R} un-does the replica over-count so the optimizer sees
     * one copy of the token space. The derivation FAILS CLOSED to {@link
     * TableStatistics#empty()} (today's behavior) when it cannot be grounded — non-
     * uniform per-range replica counts, a zero-replica range, incomplete {@code
     * table_stats} ({@code complete=false}), or ANY Sidecar/Flight error or timeout — so
     * planning never sees a possibly-wrong number and never fails on a stats error. Note
     * this does NOT affect the GROUP BY pushdown gate ({@link #estimateGroupRatio}),
     * which uses an RF-invariant ratio ({@code partition_count / live_rows}) where
     * replica over-counting cancels in numerator and denominator. Column-level stats are
     * left unknown — Cassandra 5.0 stores no reliable per-regular-column NDV, so
     * reporting any would be a heuristic (#28).
     */
    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        if (handle.isAggregated()) {
            if (handle.hasGroupBy()) {
                // Grouped aggregate: output group count is unknown — do not guess.
                return TableStatistics.empty();
            }
            // Global aggregate (e.g. count(*) with no GROUP BY): exactly one row,
            // regardless of replication factor.
            return TableStatistics.builder().setRowCount(Estimate.of(1)).build();
        }
        // Non-aggregated scan: report the logical (de-replicated) row count, memoized
        // per (keyspace, table) so one planning pass fetches at most once per table.
        // Negative empty() results are cached on purpose (issue #1336): this cache lives
        // only for this short-lived per-planning-pass metadata instance, so memoizing
        // empty() avoids re-hammering a down/slow Sidecar within the pass. The tradeoff is
        // that a transient blip is not retried until the next pass (a new metadata instance).
        SchemaTableName key = new SchemaTableName(handle.keyspace(), handle.table());
        return nonAggregatedStatsCache.computeIfAbsent(
                key, k -> logicalRowCountStatistics(handle));
    }

    /**
     * Derive the non-aggregated logical row-count statistics
     * {@code ROW_COUNT = live_rows / R} (issue #1336), or {@link TableStatistics#empty()}
     * when the derivation cannot be grounded. Fetches the token-range replicas ONCE and
     * reuses that response for BOTH the divisor {@code R} and the {@code table_stats}
     * host set. Any {@link RuntimeException}/timeout anywhere degrades to {@code empty()}
     * (same posture as {@code groupRatioForGate}): statistics failures must never fail
     * query planning.
     */
    private TableStatistics logicalRowCountStatistics(CqliteFlightTableHandle handle) {
        try {
            TokenRangeReplicasResponse replicas = tokenRangeReplicas(handle.keyspace());
            String localDatacenter = config != null ? config.localDatacenter() : null;
            OptionalInt replicaCount = uniformReplicaCount(replicas, localDatacenter);
            if (replicaCount.isEmpty()) {
                // Non-uniform / zero-replica ranges → cannot de-replicate → fail closed.
                return TableStatistics.empty();
            }
            int r = replicaCount.getAsInt();
            if (r < 1) {
                // Defensive: uniformReplicaCount already guarantees R >= 1, but never
                // divide by a non-positive divisor.
                return TableStatistics.empty();
            }
            TableStats stats = fetchTableStats(handle, replicas);
            if (!stats.complete()) {
                // Partial/undecodable per-node stats are not authoritative → fail closed.
                return TableStatistics.empty();
            }
            double logicalRows = (double) stats.liveRows() / r;
            return TableStatistics.builder().setRowCount(Estimate.of(logicalRows)).build();
        } catch (RuntimeException e) {
            // Any Sidecar/Flight error or timeout → no estimate; planning proceeds.
            return TableStatistics.empty();
        }
    }

    /** Resolve the table's Arrow schema by asking any flight node's GetSchema. */
    private Schema arrowSchema(CqliteFlightTableHandle handle) {
        RingEntry node = sidecar.ring().entries().stream()
                .filter(e -> e.address() != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No Cassandra nodes in the ring"));
        byte[] ticket = FlightTicketJson.build(
                handle.keyspace(), handle.table(), handle.ddl(),
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of(), null, null);
        return flight.getSchema(node.address(), config.flightPort(), ticket);
    }
}
