package com.rustyrazorblade.cqlite.flight;

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
import io.trino.spi.predicate.TupleDomain;
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
