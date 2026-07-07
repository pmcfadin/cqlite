package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CqliteFlightMetadata#applyFilter} directly with a constructed
 * handle and constraint (no live Sidecar/Flight dependency — applyFilter touches
 * neither).
 */
class CqliteFlightMetadataApplyFilterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CqliteFlightMetadata metadata = new CqliteFlightMetadata(null, null, null);

    private static final Map<String, ColumnHandle> ASSIGN = Map.of(
            "age", new CqliteFlightColumnHandle("age", BIGINT, PushdownCapability.FULL),
            "name", new CqliteFlightColumnHandle("name", VARCHAR, PushdownCapability.FULL));

    private static Call compare(io.trino.spi.expression.FunctionName op, String col,
            io.trino.spi.type.Type colType, Object litValue, io.trino.spi.type.Type litType) {
        return new Call(BOOLEAN, op,
                List.of(new Variable(col, colType), new Constant(litValue, litType)));
    }

    @Test
    void pushesTranslatablePredicateAndLeavesResidual() throws Exception {
        // age > 10 AND (age + 1) -- second conjunct is untranslatable
        var translatable = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                "age", BIGINT, 10L, BIGINT);
        var unsupported = new Call(BIGINT, StandardFunctions.ADD_FUNCTION_NAME,
                List.of(new Variable("age", BIGINT), new Constant(1L, BIGINT)));
        var and = new Call(BOOLEAN, StandardFunctions.AND_FUNCTION_NAME,
                List.of(translatable, unsupported));

        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        Constraint constraint = new Constraint(TupleDomain.all(), and, ASSIGN);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> applied =
                metadata.applyFilter(null, handle, constraint);
        assertTrue(applied.isPresent());

        CqliteFlightTableHandle newHandle = (CqliteFlightTableHandle) applied.get().getHandle();
        JsonNode filter = MAPPER.readTree(newHandle.filterJson().orElseThrow());
        assertEquals("Compare", filter.get("type").asText());
        assertEquals("Gt", filter.get("op").asText());

        // The untranslatable conjunct stays as the residual expression.
        assertSame(unsupported, applied.get().getRemainingExpression().orElseThrow());
    }

    @Test
    void fullyPushedLeavesTrueResidual() throws Exception {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var c2 = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "name", VARCHAR,
                Slices.utf8Slice("x"), VARCHAR);
        var and = new Call(BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, List.of(c1, c2));

        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var applied = metadata.applyFilter(null, handle, new Constraint(TupleDomain.all(), and, ASSIGN));
        assertTrue(applied.isPresent());

        CqliteFlightTableHandle newHandle = (CqliteFlightTableHandle) applied.get().getHandle();
        assertEquals("And", MAPPER.readTree(newHandle.filterJson().orElseThrow()).get("type").asText());
        assertSame(Constant.TRUE, applied.get().getRemainingExpression().orElseThrow());
    }

    @Test
    void nothingTranslatableReturnsEmpty() {
        var unsupported = new Call(BIGINT, StandardFunctions.ADD_FUNCTION_NAME,
                List.of(new Variable("age", BIGINT), new Constant(1L, BIGINT)));
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var applied = metadata.applyFilter(null, handle,
                new Constraint(TupleDomain.all(), unsupported, ASSIGN));
        assertTrue(applied.isEmpty());
    }

    @Test
    void sequentialApplyFilterCallsAccumulateBothPredicates() throws Exception {
        // First call pushes age > 10 (residual TRUE — fully handled by connector).
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var first = metadata.applyFilter(null, handle, new Constraint(TupleDomain.all(), c1, ASSIGN))
                .orElseThrow();
        ConnectorTableHandle afterFirst = first.getHandle();

        // Second call on the already-filtered handle pushes a DIFFERENT predicate
        // name = 'x'. The earlier age > 10 must NOT be dropped (its residual was
        // TRUE, so Trino no longer enforces it) — both must be ANDed on the handle.
        var c2 = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "name", VARCHAR,
                Slices.utf8Slice("x"), VARCHAR);
        var second = metadata.applyFilter(null, afterFirst, new Constraint(TupleDomain.all(), c2, ASSIGN))
                .orElseThrow();

        JsonNode filter = MAPPER.readTree(
                ((CqliteFlightTableHandle) second.getHandle()).filterJson().orElseThrow());
        assertEquals("And", filter.get("type").asText());
        JsonNode exprs = filter.get("exprs");
        assertEquals(2, exprs.size(), "both predicates must be retained");
        // Conjuncts: age>10 (Gt) and name='x' (Equal), in accumulation order.
        assertEquals("Gt", exprs.get(0).get("op").asText());
        assertEquals("age", exprs.get(0).get("column").asText());
        assertEquals("Equal", exprs.get(1).get("op").asText());
        assertEquals("name", exprs.get(1).get("column").asText());
    }

    // ---- Domain-delivery path (issue #2164): Trino sends simple predicates as a
    // ---- TupleDomain summary with getExpression()=TRUE, which the old applyFilter
    // ---- ignored (filterJson stayed empty → full scan for a point read). ----

    /** Column handles that ARE the TupleDomain keys (Trino keys the summary by ColumnHandle). */
    private static final CqliteFlightColumnHandle PK =
            new CqliteFlightColumnHandle("pk", VARCHAR, PushdownCapability.FULL);
    private static final CqliteFlightColumnHandle AGE =
            new CqliteFlightColumnHandle("age", BIGINT, PushdownCapability.FULL);
    private static final CqliteFlightColumnHandle UUID_ID =
            new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.EQUALITY);
    private static final CqliteFlightColumnHandle OPAQUE =
            new CqliteFlightColumnHandle("blob_col", VARCHAR, PushdownCapability.NONE);

    private Constraint summaryOnly(TupleDomain<ColumnHandle> summary) {
        // How Trino actually delivers a domain-expressible predicate: it lives in the
        // summary, the expression is TRUE.
        return new Constraint(summary, Constant.TRUE, ASSIGN);
    }

    private JsonNode appliedFilter(Constraint constraint) throws Exception {
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var applied = metadata.applyFilter(null, handle, constraint).orElseThrow();
        // The residual summary is returned UNCHANGED (unenforced): the server applies
        // the pushed filter best-effort post-decode and Trino keeps its ScanFilter above.
        assertEquals(constraint.getSummary(), applied.getRemainingFilter());
        return MAPPER.readTree(
                ((CqliteFlightTableHandle) applied.getHandle()).filterJson().orElseThrow());
    }

    @Test
    void singleValueSummaryPushesEquality() throws Exception {
        // key = '001.25.535688' delivered as a single-value varchar domain.
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                PK, Domain.singleValue(VARCHAR, Slices.utf8Slice("001.25.535688"))));
        JsonNode filter = appliedFilter(summaryOnly(summary));
        assertEquals("Compare", filter.get("type").asText());
        assertEquals("pk", filter.get("column").asText());
        assertEquals("Equal", filter.get("op").asText());
        assertEquals("001.25.535688", filter.get("value").asText());
    }

    @Test
    void singleValueSummaryReturnsSummaryUnenforced() throws Exception {
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                PK, Domain.singleValue(VARCHAR, Slices.utf8Slice("v"))));
        Constraint constraint = summaryOnly(summary);
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var applied = metadata.applyFilter(null, handle, constraint).orElseThrow();
        // Remaining domain == the FULL input summary: Trino keeps enforcing it (the
        // pushdown is a best-effort optimization, not an enforcement guarantee).
        assertEquals(constraint.getSummary(), applied.getRemainingFilter());
        // Expression residual is TRUE (there was no expression predicate).
        assertSame(Constant.TRUE, applied.getRemainingExpression().orElseThrow());
    }

    @Test
    void multiValueSummaryPushesIn() throws Exception {
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                PK, Domain.multipleValues(VARCHAR,
                        List.of(Slices.utf8Slice("a"), Slices.utf8Slice("b")))));
        JsonNode filter = appliedFilter(summaryOnly(summary));
        assertEquals("In", filter.get("type").asText());
        assertEquals("pk", filter.get("column").asText());
        assertEquals(2, filter.get("values").size());
        assertEquals("a", filter.get("values").get(0).asText());
        assertEquals("b", filter.get("values").get(1).asText());
    }

    @Test
    void numericRangeSummaryPushesComparison() throws Exception {
        // age > 10 delivered as a range domain.
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                AGE, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)), false)));
        JsonNode filter = appliedFilter(summaryOnly(summary));
        assertEquals("Compare", filter.get("type").asText());
        assertEquals("age", filter.get("column").asText());
        assertEquals("Gt", filter.get("op").asText());
        assertEquals(10L, filter.get("value").asLong());
    }

    @Test
    void boundedRangeSummaryPushesAndOfComparisons() throws Exception {
        // age BETWEEN 5 AND 10 → one range [5,10] → And(Gte 5, Lte 10).
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                AGE, Domain.create(
                        ValueSet.ofRanges(Range.range(BIGINT, 5L, true, 10L, true)), false)));
        JsonNode filter = appliedFilter(summaryOnly(summary));
        assertEquals("And", filter.get("type").asText());
        JsonNode exprs = filter.get("exprs");
        assertEquals(2, exprs.size());
        assertEquals("Gte", exprs.get(0).get("op").asText());
        assertEquals(5L, exprs.get(0).get("value").asLong());
        assertEquals("Lte", exprs.get(1).get("op").asText());
        assertEquals(10L, exprs.get(1).get("value").asLong());
    }

    @Test
    void noneCapabilitySummaryDoesNotPush() {
        // A domain on a NONE-capability column is unpushable — must not push, must not throw.
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                OPAQUE, Domain.singleValue(VARCHAR, Slices.utf8Slice("x"))));
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertTrue(metadata.applyFilter(null, handle, summaryOnly(summary)).isEmpty());
    }

    @Test
    void rangeOnEqualityOnlyColumnDoesNotPush() {
        // uuid/timeuuid surface as EQUALITY: ordering is NOT safe to push. A range
        // domain on such a column must stay unpushed (left to Trino).
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                UUID_ID, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("m"))), false)));
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertTrue(metadata.applyFilter(null, handle, summaryOnly(summary)).isEmpty());
    }

    @Test
    void equalityOnlyColumnSingleValueSummaryPushes() throws Exception {
        // Exact match IS safe on an EQUALITY column (id = 'uuid-literal').
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                UUID_ID, Domain.singleValue(VARCHAR, Slices.utf8Slice("abc"))));
        JsonNode filter = appliedFilter(summaryOnly(summary));
        assertEquals("Compare", filter.get("type").asText());
        assertEquals("id", filter.get("column").asText());
        assertEquals("Equal", filter.get("op").asText());
    }

    @Test
    void nullAllowedDomainDoesNotPush() {
        // key = 'v' OR key IS NULL: pushing just the value would drop null rows
        // (the server applies the filter destructively). Leave it entirely to Trino.
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                PK, Domain.create(ValueSet.of(VARCHAR, Slices.utf8Slice("v")), true)));
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertTrue(metadata.applyFilter(null, handle, summaryOnly(summary)).isEmpty());
    }

    @Test
    void domainSummaryIsIdempotent() throws Exception {
        // Trino returns the summary unchanged, so it re-passes the SAME domain on the
        // next iteration. Re-applying must NOT duplicate/loop.
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                PK, Domain.singleValue(VARCHAR, Slices.utf8Slice("v"))));
        Constraint constraint = summaryOnly(summary);
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var first = metadata.applyFilter(null, handle, constraint).orElseThrow();
        assertTrue(metadata.applyFilter(null, first.getHandle(), constraint).isEmpty(),
                "re-applying an already-pushed domain must stop (no loop)");
    }

    @Test
    void summaryAndExpressionMergeIntoOneFilter() throws Exception {
        // age > 10 comes in the expression; name = 'x' comes in the summary. Both
        // must land in the pushed tree (ANDed).
        var expr = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                "age", BIGINT, 10L, BIGINT);
        var nameHandle = new CqliteFlightColumnHandle("name", VARCHAR, PushdownCapability.FULL);
        var summary = TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                nameHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("x"))));
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var applied = metadata.applyFilter(null, handle, new Constraint(summary, expr, ASSIGN))
                .orElseThrow();
        JsonNode filter = MAPPER.readTree(
                ((CqliteFlightTableHandle) applied.getHandle()).filterJson().orElseThrow());
        assertEquals("And", filter.get("type").asText());
        assertEquals(2, filter.get("exprs").size(), "both expression and summary predicates pushed");
    }

    @Test
    void idempotentWhenFilterAlreadyApplied() {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        Constraint constraint = new Constraint(TupleDomain.all(), c1, ASSIGN);

        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");
        var first = metadata.applyFilter(null, handle, constraint).orElseThrow();
        ConnectorTableHandle pushedHandle = first.getHandle();

        // Re-applying the same constraint to the already-pushed handle must stop.
        assertTrue(metadata.applyFilter(null, pushedHandle, constraint).isEmpty(),
                "must not re-apply the same filter (infinite-loop guard)");
    }
}
