package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.slice.Slices;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that Trino {@link ConnectorExpression} trees translate into the exact
 * internally-tagged PredicateExpr JSON the cqlite-flight server parses, and that
 * untranslatable subtrees are reported so Trino keeps post-filtering them.
 */
class PredicateTreeTranslatorTest {

    private static final Map<String, io.trino.spi.connector.ColumnHandle> ASSIGNMENTS = build();

    private static Map<String, io.trino.spi.connector.ColumnHandle> build() {
        return Map.of(
                // genuine text column: FULL pushdown (equality, range, LIKE, IN).
                "name", new CqliteFlightColumnHandle("name", VARCHAR, PushdownCapability.FULL),
                // uuid surfaced as VARCHAR but EQUALITY-only: range/LIKE unsafe.
                "id", new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.EQUALITY),
                // inet surfaced as VARCHAR but NONE: nothing pushable.
                "inet", new CqliteFlightColumnHandle("inet", VARCHAR, PushdownCapability.NONE),
                "age", new CqliteFlightColumnHandle("age", BIGINT, PushdownCapability.FULL),
                "score", new CqliteFlightColumnHandle("score", INTEGER, PushdownCapability.FULL),
                "active", new CqliteFlightColumnHandle("active", BOOLEAN, PushdownCapability.FULL),
                "ratio", new CqliteFlightColumnHandle("ratio", DOUBLE, PushdownCapability.FULL));
    }

    private static Variable var(String name, Type type) {
        return new Variable(name, type);
    }

    private static Constant lit(Object value, Type type) {
        return new Constant(value, type);
    }

    private static Call call(FunctionName fn, Type type, ConnectorExpression... args) {
        return new Call(type, fn, List.of(args));
    }

    private static Call compare(FunctionName op, String col, Type colType, Object litValue, Type litType) {
        return call(op, BOOLEAN, var(col, colType), lit(litValue, litType));
    }

    private static PredicateTreeTranslator.Result translate(ConnectorExpression expr) {
        return PredicateTreeTranslator.translate(expr, ASSIGNMENTS);
    }

    @Test
    void equalOnVarcharProducesStringValue() {
        var expr = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "name", VARCHAR,
                Slices.utf8Slice("alice"), VARCHAR);
        JsonNode node = translate(expr).pushed().orElseThrow();
        assertEquals("Compare", node.get("type").asText());
        assertEquals("name", node.get("column").asText());
        assertEquals("Equal", node.get("op").asText());
        assertEquals("alice", node.get("value").asText());
        assertTrue(node.get("value").isTextual());
    }

    @Test
    void comparisonOperatorsMapToServerSpellings() {
        assertEquals("Lt", op(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME));
        assertEquals("Lte", op(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME));
        assertEquals("Gt", op(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME));
        assertEquals("Gte", op(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME));
        assertEquals("Equal", op(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME));
    }

    private static String op(FunctionName fn) {
        var expr = compare(fn, "age", BIGINT, 10L, BIGINT);
        JsonNode node = PredicateTreeTranslator.translate(expr, ASSIGNMENTS).pushed().orElseThrow();
        assertEquals("age", node.get("column").asText());
        assertEquals(10, node.get("value").asLong());
        assertTrue(node.get("value").isNumber());
        return node.get("op").asText();
    }

    @Test
    void integerAndBooleanAndDoubleLiteralsMapToJsonScalars() {
        JsonNode intNode = translate(
                compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "score", INTEGER, 42L, INTEGER))
                .pushed().orElseThrow();
        assertTrue(intNode.get("value").isNumber());
        assertEquals(42, intNode.get("value").asInt());

        JsonNode boolNode = translate(
                compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "active", BOOLEAN, true, BOOLEAN))
                .pushed().orElseThrow();
        assertTrue(boolNode.get("value").isBoolean());
        assertTrue(boolNode.get("value").asBoolean());

        JsonNode dblNode = translate(
                compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "ratio", DOUBLE, 1.5d, DOUBLE))
                .pushed().orElseThrow();
        assertTrue(dblNode.get("value").isNumber());
        assertEquals(1.5d, dblNode.get("value").asDouble());
    }

    @Test
    void andSplitsTranslatableAndResidualConjuncts() {
        // age > 10 AND (unsupported add call)
        var translatable = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var unsupported = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT, var("age", BIGINT), lit(1L, BIGINT));
        var and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, translatable, unsupported);

        PredicateTreeTranslator.Result r = translate(and);
        JsonNode pushed = r.pushed().orElseThrow();
        // Only the translatable conjunct is pushed; the And collapses to one child here.
        assertEquals("Compare", pushed.get("type").asText());
        assertEquals("Gt", pushed.get("op").asText());
        // The unsupported conjunct remains as a residual.
        assertEquals(List.of(unsupported), r.residual());
    }

    @Test
    void andWithMultipleTranslatableConjunctsBuildsAndNode() {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var c2 = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "name", VARCHAR,
                Slices.utf8Slice("x"), VARCHAR);
        var and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, c1, c2);

        JsonNode node = translate(and).pushed().orElseThrow();
        assertEquals("And", node.get("type").asText());
        assertEquals(2, node.get("exprs").size());
        assertTrue(translate(and).residual().isEmpty());
    }

    @Test
    void orWithAnyUntranslatablePartIsFullyResidual() {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var unsupported = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT, var("age", BIGINT), lit(1L, BIGINT));
        var or = call(StandardFunctions.OR_FUNCTION_NAME, BOOLEAN, c1, unsupported);

        PredicateTreeTranslator.Result r = translate(or);
        assertTrue(r.pushed().isEmpty(), "OR with an untranslatable branch cannot be pushed");
        assertEquals(List.of(or), r.residual());
    }

    @Test
    void orFullyTranslatableProducesOrNode() {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var c2 = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "name", VARCHAR,
                Slices.utf8Slice("x"), VARCHAR);
        var or = call(StandardFunctions.OR_FUNCTION_NAME, BOOLEAN, c1, c2);

        JsonNode node = translate(or).pushed().orElseThrow();
        assertEquals("Or", node.get("type").asText());
        assertEquals(2, node.get("exprs").size());
    }

    @Test
    void notWrapsTranslatableChild() {
        var c1 = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "age", BIGINT, 10L, BIGINT);
        var not = call(StandardFunctions.NOT_FUNCTION_NAME, BOOLEAN, c1);
        JsonNode node = translate(not).pushed().orElseThrow();
        assertEquals("Not", node.get("type").asText());
        assertEquals("Compare", node.get("expr").get("type").asText());
    }

    @Test
    void notWithUntranslatableChildIsResidual() {
        var unsupported = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT, var("age", BIGINT), lit(1L, BIGINT));
        var not = call(StandardFunctions.NOT_FUNCTION_NAME, BOOLEAN, unsupported);
        PredicateTreeTranslator.Result r = translate(not);
        assertTrue(r.pushed().isEmpty());
        assertEquals(List.of(not), r.residual());
    }

    @Test
    void isNullProducesIsNullNode() {
        var isNull = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, var("name", VARCHAR));
        JsonNode node = translate(isNull).pushed().orElseThrow();
        assertEquals("IsNull", node.get("type").asText());
        assertEquals("name", node.get("column").asText());
    }

    @Test
    void inProducesInNodeWithValuesArray() {
        // age IN (1, 2, 3) modeled as $in(age, $array(1,2,3))
        var array = call(StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME, BIGINT,
                lit(1L, BIGINT), lit(2L, BIGINT), lit(3L, BIGINT));
        var in = call(StandardFunctions.IN_PREDICATE_FUNCTION_NAME, BOOLEAN, var("age", BIGINT), array);
        JsonNode node = translate(in).pushed().orElseThrow();
        assertEquals("In", node.get("type").asText());
        assertEquals("age", node.get("column").asText());
        assertEquals(3, node.get("values").size());
        assertEquals(1, node.get("values").get(0).asInt());
        assertEquals(3, node.get("values").get(2).asInt());
    }

    @Test
    void likePrefixPatternProducesPrefixCompare() {
        var like = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                var("name", VARCHAR), lit(Slices.utf8Slice("foo%"), VARCHAR));
        JsonNode node = translate(like).pushed().orElseThrow();
        assertEquals("Compare", node.get("type").asText());
        assertEquals("Prefix", node.get("op").asText());
        assertEquals("name", node.get("column").asText());
        assertEquals("foo", node.get("value").asText());
    }

    @Test
    void likeWithNonPrefixWildcardIsUntranslatable() {
        var like = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                var("name", VARCHAR), lit(Slices.utf8Slice("%foo%"), VARCHAR));
        PredicateTreeTranslator.Result r = translate(like);
        assertTrue(r.pushed().isEmpty());
        assertEquals(List.of(like), r.residual());

        var underscore = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                var("name", VARCHAR), lit(Slices.utf8Slice("f_o%"), VARCHAR));
        assertTrue(translate(underscore).pushed().isEmpty());
    }

    @Test
    void likeOnUuidBackedVarcharIsUntranslatable() {
        // `id` is a uuid surfaced as VARCHAR (EQUALITY). LIKE 'x%' must NOT be
        // pushed — the server can't prefix-match a uuid — it stays residual.
        var like = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                var("id", VARCHAR), lit(Slices.utf8Slice("1111%"), VARCHAR));
        PredicateTreeTranslator.Result r = translate(like);
        assertTrue(r.pushed().isEmpty(), "LIKE on a uuid-backed VARCHAR must not be pushed");
        assertEquals(List.of(like), r.residual());
    }

    @Test
    void rangeOnUuidBackedVarcharIsUntranslatable() {
        // `id > '1111...'` would order by the CQL uuid type server-side, not by
        // VARCHAR — unsafe, so it must stay a Trino residual.
        var gt = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "id", VARCHAR,
                Slices.utf8Slice("11111111-1111-1111-1111-111111111111"), VARCHAR);
        PredicateTreeTranslator.Result r = translate(gt);
        assertTrue(r.pushed().isEmpty(), "range on a uuid-backed VARCHAR must not be pushed");
        assertEquals(List.of(gt), r.residual());
    }

    @Test
    void equalityOnUuidBackedVarcharIsStillPushed() {
        // Exact-match equality lowers to Value::Uuid and compares correctly, so it
        // remains safe to push even for a uuid-backed VARCHAR.
        var eq = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "id", VARCHAR,
                Slices.utf8Slice("11111111-1111-1111-1111-111111111111"), VARCHAR);
        JsonNode node = translate(eq).pushed().orElseThrow();
        assertEquals("Compare", node.get("type").asText());
        assertEquals("Equal", node.get("op").asText());
        assertEquals("id", node.get("column").asText());
    }

    @Test
    void equalityColumnPushesEqualButNotRangeOrLike() {
        // An EQUALITY column (uuid-as-VARCHAR `id`): Equal pushes; Gt and LIKE do not.
        var eq = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "id", VARCHAR,
                Slices.utf8Slice("11111111-1111-1111-1111-111111111111"), VARCHAR);
        assertTrue(translate(eq).pushed().isPresent(), "Equal on an EQUALITY column pushes");

        var gt = compare(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, "id", VARCHAR,
                Slices.utf8Slice("11111111-1111-1111-1111-111111111111"), VARCHAR);
        PredicateTreeTranslator.Result gtr = translate(gt);
        assertTrue(gtr.pushed().isEmpty(), "Gt on an EQUALITY column stays residual");
        assertEquals(List.of(gt), gtr.residual());

        var like = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                var("id", VARCHAR), lit(Slices.utf8Slice("1111%"), VARCHAR));
        PredicateTreeTranslator.Result likeR = translate(like);
        assertTrue(likeR.pushed().isEmpty(), "LIKE on an EQUALITY column stays residual");
        assertEquals(List.of(like), likeR.residual());
    }

    @Test
    void noneColumnPushesNothing() {
        // A NONE column (inet-as-VARCHAR): even equality must stay a Trino residual,
        // because the server's json_to_value rejects inet operands.
        var eq = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "inet", VARCHAR,
                Slices.utf8Slice("127.0.0.1"), VARCHAR);
        PredicateTreeTranslator.Result r = translate(eq);
        assertTrue(r.pushed().isEmpty(), "Equal on a NONE column must not push");
        assertEquals(List.of(eq), r.residual());

        // IN on a NONE column also stays residual.
        var array = call(StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME, VARCHAR,
                lit(Slices.utf8Slice("127.0.0.1"), VARCHAR));
        var in = call(StandardFunctions.IN_PREDICATE_FUNCTION_NAME, BOOLEAN, var("inet", VARCHAR), array);
        assertTrue(translate(in).pushed().isEmpty(), "IN on a NONE column must not push");
    }

    @Test
    void unsupportedCallIsUntranslatable() {
        var unsupported = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT, var("age", BIGINT), lit(1L, BIGINT));
        PredicateTreeTranslator.Result r = translate(unsupported);
        assertTrue(r.pushed().isEmpty());
        assertEquals(List.of(unsupported), r.residual());
    }

    @Test
    void unknownVariableIsUntranslatable() {
        var expr = compare(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "missing", BIGINT, 1L, BIGINT);
        PredicateTreeTranslator.Result r = translate(expr);
        assertTrue(r.pushed().isEmpty());
        assertFalse(r.residual().isEmpty());
    }
}
