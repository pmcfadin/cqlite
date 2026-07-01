package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CqliteFlightMetadata#applyAggregation} directly with constructed
 * AggregateFunction/Variable inputs (no live Sidecar/Flight — applyAggregation touches
 * neither). Verifies the wire spec on the new handle and the SPI result shape.
 */
class CqliteFlightMetadataApplyAggregationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CqliteFlightMetadata metadata = new CqliteFlightMetadata(null, null, null);

    // x: numeric (FULL), c1: grouping column (FULL).
    private static final CqliteFlightColumnHandle X = new CqliteFlightColumnHandle("x", BIGINT, PushdownCapability.FULL);
    private static final CqliteFlightColumnHandle C1 = new CqliteFlightColumnHandle("c1", VARCHAR, PushdownCapability.FULL);
    private static final CqliteFlightColumnHandle NOPUSH =
            new CqliteFlightColumnHandle("y", BIGINT, PushdownCapability.NONE);

    // d: a double column (FULL) — used to verify float min/max is declined.
    private static final CqliteFlightColumnHandle D = new CqliteFlightColumnHandle("d", DOUBLE, PushdownCapability.FULL);

    private static final Map<String, ColumnHandle> ASSIGN = Map.of(
            "x", X, "c1", C1, "y", NOPUSH, "d", D);

    private static final ConnectorTableHandle TABLE = new CqliteFlightTableHandle("ks", "t", "ddl");

    private static AggregateFunction agg(String name, io.trino.spi.type.Type out, ConnectorExpression... args) {
        return new AggregateFunction(name, out, List.of(args), List.of(), false, Optional.empty());
    }

    private static JsonNode aggSpec(CqliteFlightTableHandle handle) throws Exception {
        return MAPPER.readTree(handle.aggregationJson().orElseThrow());
    }

    @Test
    void pushesCountStarGlobal() throws Exception {
        var result = metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of()))
                .orElseThrow();

        var handle = (CqliteFlightTableHandle) result.getHandle();
        assertTrue(handle.isAggregated());
        JsonNode spec = aggSpec(handle);
        assertEquals(0, spec.get("group_by").size());
        JsonNode a0 = spec.get("aggregates").get(0);
        assertEquals("Count", a0.get("func").asText());
        assertTrue(a0.get("column").isNull(), "count(*) has null column");
        assertEquals("agg0", a0.get("output").asText());

        // Projections align 1:1 with input aggregates; one Variable of type BIGINT.
        assertEquals(1, result.getProjections().size());
        Variable proj = (Variable) result.getProjections().get(0);
        assertEquals("agg0", proj.getName());
        assertSame(BIGINT, proj.getType());
        // Assignment declares the new output column.
        Assignment assignment = result.getAssignments().get(0);
        assertEquals("agg0", assignment.getVariable());
        assertSame(BIGINT, assignment.getType());
        assertTrue(result.getGroupingColumnMapping().isEmpty(), "no grouping columns");
    }

    @Test
    void pushesCountColumn() throws Exception {
        var result = metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of())).orElseThrow();
        JsonNode a0 = aggSpec((CqliteFlightTableHandle) result.getHandle()).get("aggregates").get(0);
        assertEquals("Count", a0.get("func").asText());
        assertEquals("x", a0.get("column").asText());
    }

    @Test
    void pushesSumMinMax() throws Exception {
        var result = metadata.applyAggregation(
                null, TABLE,
                List.of(
                        agg("sum", BIGINT, new Variable("x", BIGINT)),
                        agg("min", BIGINT, new Variable("x", BIGINT)),
                        agg("max", BIGINT, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of())).orElseThrow();

        JsonNode aggs = aggSpec((CqliteFlightTableHandle) result.getHandle()).get("aggregates");
        assertEquals("Sum", aggs.get(0).get("func").asText());
        assertEquals("Min", aggs.get(1).get("func").asText());
        assertEquals("Max", aggs.get(2).get("func").asText());
        assertEquals(3, result.getProjections().size(), "1:1 with input aggregates");
    }

    @Test
    void decomposesAvgIntoSumDoublePlusCount() throws Exception {
        // avg(x) -> SumDouble(x) + Count(x) on the wire. SumDouble totals in f64 so
        // an integer avg cannot overflow (issue #902); the decomposition is the
        // same for double columns.
        var result = metadata.applyAggregation(
                null, TABLE, List.of(agg("avg", DOUBLE, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of())).orElseThrow();

        JsonNode aggs = aggSpec((CqliteFlightTableHandle) result.getHandle()).get("aggregates");
        assertEquals(2, aggs.size(), "avg(x) -> SumDouble(x) + Count(x) on the wire");
        assertEquals("SumDouble", aggs.get(0).get("func").asText());
        assertEquals("x", aggs.get(0).get("column").asText());
        assertEquals("Count", aggs.get(1).get("func").asText());
        assertEquals("x", aggs.get(1).get("column").asText());

        // But Trino sees ONE projection (the merged DOUBLE avg result).
        assertEquals(1, result.getProjections().size());
        Variable proj = (Variable) result.getProjections().get(0);
        assertSame(DOUBLE, proj.getType());
    }

    @Test
    void pushesGroupByOneColumn() throws Exception {
        var result = metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)),
                ASSIGN, List.of(List.of(C1))).orElseThrow();

        var handle = (CqliteFlightTableHandle) result.getHandle();
        JsonNode spec = aggSpec(handle);
        assertEquals("c1", spec.get("group_by").get(0).asText());

        // groupingColumnMapping remaps the original grouping handle (passthrough).
        assertEquals(C1, result.getGroupingColumnMapping().get(C1));
        // The grouping column must NOT appear in `assignments` — that list is only
        // for the new aggregate-result variables. Declaring the grouping handle here
        // too made Trino's symbol<->handle BiMap throw "Multiple entries with same
        // value" (regression: GROUP BY query failed end-to-end).
        assertTrue(result.getAssignments().stream().noneMatch(a -> a.getVariable().equals("c1")),
                "grouping column must not be in assignments (only in groupingColumnMapping)");
        // No assignment may point at the grouping column's handle either.
        assertTrue(result.getAssignments().stream().noneMatch(a -> a.getColumn().equals(C1)),
                "no assignment may duplicate the grouping ColumnHandle");
        // The single count aggregate still has its result assignment.
        assertEquals(1, result.getAssignments().size());
    }

    @Test
    void syntheticOutputNameAvoidsGroupingColumnCollision() throws Exception {
        // A real column literally named "agg0" used in GROUP BY must NOT collide
        // with the synthetic aggregate output names (both share the partial Arrow
        // schema and are resolved by name). count(*) here would naively be "agg0".
        var agg0col = new CqliteFlightColumnHandle("agg0", VARCHAR, PushdownCapability.FULL);
        Map<String, ColumnHandle> assign = Map.of("agg0", agg0col, "x", X);
        var result = metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)),
                assign, List.of(List.of(agg0col))).orElseThrow();

        var handle = (CqliteFlightTableHandle) result.getHandle();
        JsonNode spec = aggSpec(handle);
        assertEquals("agg0", spec.get("group_by").get(0).asText());
        String out = spec.get("aggregates").get(0).get("output").asText();
        assertTrue(!out.equals("agg0"), "aggregate output must not collide with grouping column 'agg0'");
    }

    @Test
    void declinesDistinct() {
        var distinct = new AggregateFunction("count", BIGINT,
                List.of(new Variable("x", BIGINT)), List.of(), true, Optional.empty());
        assertTrue(metadata.applyAggregation(null, TABLE, List.of(distinct), ASSIGN, List.of(List.of()))
                .isEmpty());
    }

    @Test
    void declinesExpressionArg() {
        var expr = new Call(BIGINT, StandardFunctions.ADD_FUNCTION_NAME,
                List.of(new Variable("x", BIGINT), new Constant(1L, BIGINT)));
        var sum = agg("sum", BIGINT, expr);
        assertTrue(metadata.applyAggregation(null, TABLE, List.of(sum), ASSIGN, List.of(List.of()))
                .isEmpty());
    }

    @Test
    void declinesMultipleGroupingSets() {
        assertTrue(metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)),
                ASSIGN, List.of(List.of(C1), List.of())).isEmpty(),
                "ROLLUP/CUBE/GROUPING SETS -> multiple grouping sets -> decline");
    }

    @Test
    void declinesSumOnNonFullColumn() {
        assertTrue(metadata.applyAggregation(
                null, TABLE, List.of(agg("sum", BIGINT, new Variable("y", BIGINT))),
                ASSIGN, List.of(List.of())).isEmpty(),
                "sum needs a FULL-capability column");
    }

    @Test
    void pushesAvgOnIntegerAndDouble() throws Exception {
        // avg(bigint) now pushes via SumDouble (issue #902): the f64 numerator
        // cannot overflow, matching Trino's 128-bit avg. avg(double) pushes too.
        var intResult = metadata.applyAggregation(
                null, TABLE, List.of(agg("avg", DOUBLE, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of())).orElseThrow();
        JsonNode intAggs = aggSpec((CqliteFlightTableHandle) intResult.getHandle()).get("aggregates");
        assertEquals("SumDouble", intAggs.get(0).get("func").asText(),
                "integer avg's sum partial must be SumDouble (non-overflowing)");
        assertEquals("Count", intAggs.get(1).get("func").asText());

        var doubleResult = metadata.applyAggregation(
                null, TABLE, List.of(agg("avg", DOUBLE, new Variable("d", DOUBLE))),
                ASSIGN, List.of(List.of())).orElseThrow();
        JsonNode dblAggs = aggSpec((CqliteFlightTableHandle) doubleResult.getHandle()).get("aggregates");
        assertEquals("SumDouble", dblAggs.get(0).get("func").asText());
    }

    @Test
    void declinesGroupByOnDoubleColumn() {
        // Grouping on a float/double column (non-finite key semantics) is left to Trino.
        assertTrue(metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)),
                ASSIGN, List.of(List.of(D))).isEmpty(), "GROUP BY double is not pushed");
    }

    @Test
    void pushesMinMaxOnDoubleColumn() throws Exception {
        // Float/double min/max now pushes (issue #896): the Rust accumulator and
        // the Java merger both order NaN as the largest value (Double.compare),
        // matching Trino and making the result order-independent.
        var minResult = metadata.applyAggregation(
                null, TABLE, List.of(agg("min", DOUBLE, new Variable("d", DOUBLE))),
                ASSIGN, List.of(List.of())).orElseThrow();
        assertEquals("Min",
                aggSpec((CqliteFlightTableHandle) minResult.getHandle())
                        .get("aggregates").get(0).get("func").asText());

        var maxResult = metadata.applyAggregation(
                null, TABLE, List.of(agg("max", DOUBLE, new Variable("d", DOUBLE))),
                ASSIGN, List.of(List.of())).orElseThrow();
        assertEquals("Max",
                aggSpec((CqliteFlightTableHandle) maxResult.getHandle())
                        .get("aggregates").get(0).get("func").asText());

        // sum(double) still pushes too (NaN propagates identically through f64 sum).
        assertTrue(metadata.applyAggregation(
                null, TABLE, List.of(agg("sum", DOUBLE, new Variable("d", DOUBLE))),
                ASSIGN, List.of(List.of())).isPresent(), "sum(double) still pushes");
    }

    @Test
    void declinesUnsupportedFunction() {
        assertTrue(metadata.applyAggregation(
                null, TABLE, List.of(agg("approx_distinct", BIGINT, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of())).isEmpty());
    }

    @Test
    void declinesFilteredAggregate() {
        var filtered = new AggregateFunction("count", BIGINT,
                List.of(new Variable("x", BIGINT)), List.of(), false,
                Optional.of(new Constant(true, BOOLEAN)));
        assertTrue(metadata.applyAggregation(null, TABLE, List.of(filtered), ASSIGN, List.of(List.of()))
                .isEmpty());
    }

    // --- Cardinality / operator gate (issue #893) ---

    private static CqliteFlightMetadata metadataWithPolicy(String policy) {
        var config = CqliteFlightConfig.fromMap(Map.of(
                "cqlite.sidecar-uri", "http://localhost:9043",
                "cqlite.aggregation-pushdown-group-by", policy));
        return new CqliteFlightMetadata(config, null, null);
    }

    @Test
    void neverPolicyDeclinesGroupByPushdown() {
        // GROUP BY is left to Trino entirely when the operator forces it off.
        assertTrue(metadataWithPolicy("never").applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of(C1))).isEmpty(),
                "GROUP BY must not push under aggregation-pushdown-group-by=never");
    }

    @Test
    void neverPolicyStillPushesGlobalAggregate() {
        // Global aggregates are an unconditional win — the gate never blocks them.
        assertTrue(metadataWithPolicy("never").applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of())).isPresent(),
                "global aggregate must push even when GROUP BY pushdown is disabled");
    }

    @Test
    void alwaysPolicyPushesGroupBy() {
        assertTrue(metadataWithPolicy("always").applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of(C1))).isPresent(),
                "GROUP BY must push under aggregation-pushdown-group-by=always");
    }

    @Test
    void automaticPolicyPushesGroupByWhenNoEstimate() {
        // No NDV stats are surfaced yet, so AUTOMATIC has no estimate and pushes
        // (always correct; only risks the rare high-cardinality perf loss).
        assertTrue(metadataWithPolicy("automatic").applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of(C1))).isPresent(),
                "AUTOMATIC pushes GROUP BY when no cardinality estimate is available");
    }

    @Test
    void declineGroupByPushdownDecisionTable() {
        var none = java.util.OptionalDouble.empty();
        var low = java.util.OptionalDouble.of(0.1);   // groups well under rows -> push
        var high = java.util.OptionalDouble.of(0.9);  // groups ~= rows -> decline
        double maxRatio = 0.5;

        // NEVER always declines; ALWAYS never declines — regardless of estimate.
        assertTrue(CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.NEVER, low, maxRatio));
        assertTrue(CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.NEVER, none, maxRatio));
        assertTrue(!CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.ALWAYS, high, maxRatio));
        assertTrue(!CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.ALWAYS, none, maxRatio));

        // AUTOMATIC: no estimate -> push; below threshold -> push; above -> decline.
        assertTrue(!CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.AUTOMATIC, none, maxRatio));
        assertTrue(!CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.AUTOMATIC, low, maxRatio));
        assertTrue(CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.AUTOMATIC, high, maxRatio));
        // Exactly at the threshold is NOT declined (strict >).
        assertTrue(!CqliteFlightMetadata.declineGroupByPushdown(
                GroupByPushdownPolicy.AUTOMATIC, java.util.OptionalDouble.of(0.5), maxRatio));
    }

    // --- Fix 2 (issue #944): fetch stats only for OTHERWISE-PUSHABLE aggregations ---

    /**
     * AUTOMATIC-policy metadata that records whether the planning-time stats fetch
     * (overridable {@link CqliteFlightMetadata#fetchTableStats} seam) was invoked, without
     * any live Sidecar/Flight. A supported GROUP BY must reach the fetch; an aggregation
     * declined for an unsupported function/argument must NOT.
     */
    private static final class FetchSpyMetadata extends CqliteFlightMetadata {
        int fetchCalls = 0;

        FetchSpyMetadata() {
            super(CqliteFlightConfig.fromMap(Map.of(
                    "cqlite.sidecar-uri", "http://localhost:9043",
                    "cqlite.aggregation-pushdown-group-by", "automatic")), null, null);
        }

        @Override
        TableStats fetchTableStats(CqliteFlightTableHandle handle) {
            fetchCalls++;
            // An UNBOUNDED grouping shape → empty group ratio → AUTOMATIC pushes. We only
            // assert WHETHER this ran, so the returned stats just need to be complete.
            return new TableStats(1000, 100, 1, true, 0);
        }
    }

    @Test
    void unsupportedAggregationDoesNotTriggerStatsFetch() {
        // GROUP BY c1 with an UNSUPPORTED aggregate (sum on a non-FULL column). The
        // aggregate-support validation declines it, so under Fix 2 the network stats
        // fetch + group-ratio gate must NEVER run.
        FetchSpyMetadata spy = new FetchSpyMetadata();
        assertTrue(spy.applyAggregation(
                null, TABLE, List.of(agg("sum", BIGINT, new Variable("y", BIGINT))),
                ASSIGN, List.of(List.of(C1))).isEmpty(),
                "unsupported aggregate must be declined");
        assertEquals(0, spy.fetchCalls,
                "an aggregation declined for an unsupported argument must NOT fetch stats");
    }

    @Test
    void unsupportedFunctionWithGroupByDoesNotTriggerStatsFetch() {
        // GROUP BY c1 with an UNSUPPORTED function. Declined before any stats I/O.
        FetchSpyMetadata spy = new FetchSpyMetadata();
        assertTrue(spy.applyAggregation(
                null, TABLE, List.of(agg("approx_distinct", BIGINT, new Variable("x", BIGINT))),
                ASSIGN, List.of(List.of(C1))).isEmpty(),
                "unsupported function must be declined");
        assertEquals(0, spy.fetchCalls,
                "an aggregation declined for an unsupported function must NOT fetch stats");
    }

    @Test
    void supportedGroupByDoesTriggerStatsFetch() {
        // A supported, otherwise-pushable AUTOMATIC GROUP BY DOES consult stats: the
        // group-ratio gate needs them. (Unbounded shape → empty ratio → still pushes.)
        FetchSpyMetadata spy = new FetchSpyMetadata();
        assertTrue(spy.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)),
                ASSIGN, List.of(List.of(C1))).isPresent(),
                "supported GROUP BY count(*) must push");
        assertEquals(1, spy.fetchCalls,
                "a supported AUTOMATIC GROUP BY must fetch stats for the group-ratio gate");
    }

    @Test
    void globalAggregateDoesNotTriggerStatsFetch() {
        // Globals bypass the group-ratio gate entirely (no GROUP BY), so no stats fetch.
        FetchSpyMetadata spy = new FetchSpyMetadata();
        assertTrue(spy.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of()))
                .isPresent(), "global aggregate must push");
        assertEquals(0, spy.fetchCalls, "a global aggregate must NOT fetch stats");
    }

    @Test
    void applyFilterDeclinesOnAggregatedHandle() {
        // After aggregation pushdown, a later filter must NOT be pushed: it would
        // apply to aggregate outputs and rebuilding the handle would drop the
        // aggregation state. The non-aggregated path still pushes filters normally.
        var aggregated = (CqliteFlightTableHandle) metadata.applyAggregation(
                null, TABLE, List.of(agg("count", BIGINT)), ASSIGN, List.of(List.of()))
                .orElseThrow().getHandle();
        assertTrue(aggregated.isAggregated());

        var predicate = new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(new Variable("x", BIGINT), new Constant(1L, BIGINT)));
        var constraint = new io.trino.spi.connector.Constraint(
                io.trino.spi.predicate.TupleDomain.all(), predicate, ASSIGN);
        assertTrue(metadata.applyFilter(null, aggregated, constraint).isEmpty(),
                "filter pushdown must be declined on an already-aggregated handle");
    }
}
