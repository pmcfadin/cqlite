package in.mcfad.cqlite.flight;

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
import java.util.Set;

import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.RingEntry;

/**
 * Connector metadata backed by Sidecar (DDL discovery) and the cqlite-flight
 * server (Arrow schema → Trino column types via {@link ArrowTypeMapper}).
 */
public class CqliteFlightMetadata implements ConnectorMetadata {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final CqliteFlightClient flight;

    public CqliteFlightMetadata(CqliteFlightConfig config, SidecarClient sidecar, CqliteFlightClient flight) {
        this.config = config;
        this.sidecar = sidecar;
        this.flight = flight;
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
     * <p>The {@code TupleDomain} summary is returned unchanged (we do not consume
     * it), and partial-AND pushdown leaves the untranslatable conjuncts in the
     * residual expression.
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
        if (result.pushed().isEmpty()) {
            return Optional.empty(); // nothing translatable to push
        }

        // Trino calls applyFilter iteratively, passing only the predicate at the
        // current FilterNode each time — the previously pushed predicate lives on
        // the handle and is NOT re-passed. So ACCUMULATE: combine this call's tree
        // with whatever the handle already carries. Replacing would silently drop
        // an earlier condition whose residual we already reported as satisfied,
        // returning too many rows.
        JsonNode newlyPushed = result.pushed().get();
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

        CqliteFlightTableHandle newHandle = new CqliteFlightTableHandle(
                table.keyspace(), table.table(), table.ddl(), Optional.of(filterJson));

        // The residual expression Trino must still evaluate (the untranslatable
        // conjuncts, ANDed). Empty residual => TRUE (fully pushed).
        ConnectorExpression remainingExpression = residualExpression(result.residual());

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                constraint.getSummary(), // domain returned unchanged; we don't consume it
                remainingExpression,
                false));
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

        // Cardinality / operator gate (issue #893, #944). Global aggregates (no
        // GROUP BY) are an unconditional data-reduction win and are NEVER gated —
        // they bypass this check. For a GROUP BY, the single-finalize-split design
        // degrades to break-even-to-loss as distinct groups approach the row count,
        // so honor the operator policy and decline when the estimated group ratio is
        // too high. The ratio is only needed for the AUTOMATIC policy, so the
        // (network) stats fetch is skipped under NEVER/ALWAYS.
        if (hasGroupBy && declineGroupByPushdown(
                groupByPolicy(), groupRatioForGate(table, groupingColumns), maxGroupRatio())) {
            return Optional.empty();
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
     * {@code table_stats} action across the distinct flight nodes in the ring
     * (issue #944). Summing across replicas over-counts by the replication factor,
     * which is acceptable for an UPPER-BOUND gate input (it only ever errs toward
     * DECLINING pushdown). A node whose stats call fails is skipped.
     */
    private TableStats fetchTableStats(CqliteFlightTableHandle handle) {
        byte[] body = tableStatsRequest(handle.keyspace(), handle.table());
        TableStats total = TableStats.EMPTY;
        Set<String> seen = new LinkedHashSet<>();
        for (RingEntry node : sidecar.ring().entries()) {
            String address = node.address();
            if (address == null || !seen.add(address)) {
                continue;
            }
            try {
                total = total.plus(flight.tableStats(address, config.flightPort(), body));
            } catch (RuntimeException e) {
                // Skip a node that could not answer; its peers still contribute.
            }
        }
        return total;
    }

    /** Build the {@code table_stats} action body (JSON {@code TableStatsRequest}). */
    private static byte[] tableStatsRequest(String keyspace, String table) {
        var root = MAPPER.createObjectNode();
        root.put("keyspace", keyspace);
        root.put("table", table);
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
        Schema schema = arrowSchema((CqliteFlightTableHandle) table);
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Field field : schema.getFields()) {
            handles.put(field.getName(), new CqliteFlightColumnHandle(
                    field.getName(), ArrowTypeMapper.toTrino(field), ArrowTypeMapper.capabilityOf(field)));
        }
        return handles;
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
        Schema schema = arrowSchema(handle);
        List<ColumnMetadata> columns = new ArrayList<>();
        for (Field field : schema.getFields()) {
            columns.add(new ColumnMetadata(field.getName(), ArrowTypeMapper.toTrino(field)));
        }
        return new ConnectorTableMetadata(new SchemaTableName(handle.keyspace(), handle.table()), columns);
    }

    /**
     * Surface the AUTHORITATIVE per-table row count to the Trino optimizer (issue
     * #944). The cqlite-flight {@code table_stats} action returns Σ {@code totalRows}
     * across the table's SSTables (an upper bound; see {@link TableStats}); we report
     * it as the table's row-count estimate. Column-level stats are left unknown —
     * Cassandra 5.0 stores no reliable per-regular-column NDV, so reporting any would
     * be a heuristic (#28). A failed/zero fetch yields {@link TableStatistics#empty()}
     * (unknown), which is always a safe input for the optimizer.
     *
     * <p>For an <em>aggregated</em> handle the scan's OUTPUT cardinality is the
     * aggregate-result cardinality, NOT the base-table row count (issue #944): a
     * GLOBAL aggregate (no GROUP BY) emits exactly one row, so we report {@code
     * ROW_COUNT = 1}; a grouped aggregate's output group count is not authoritatively
     * known, so we return {@link TableStatistics#empty()} and let Trino estimate
     * rather than fabricate (#28).
     */
    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        if (handle.isAggregated()) {
            if (handle.hasGroupBy()) {
                // Grouped aggregate: output group count is unknown — do not guess.
                return TableStatistics.empty();
            }
            // Global aggregate (e.g. count(*) with no GROUP BY): exactly one row.
            return TableStatistics.builder().setRowCount(Estimate.of(1)).build();
        }
        TableStats stats;
        try {
            stats = fetchTableStats(handle);
        } catch (RuntimeException e) {
            return TableStatistics.empty();
        }
        if (stats.liveRows() <= 0) {
            return TableStatistics.empty();
        }
        return TableStatistics.builder()
                .setRowCount(Estimate.of(stats.liveRows()))
                .build();
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
