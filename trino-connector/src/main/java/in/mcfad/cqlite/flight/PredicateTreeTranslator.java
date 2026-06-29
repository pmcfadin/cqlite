package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Translates a Trino {@link ConnectorExpression} predicate (from
 * {@code Constraint.getExpression()}) into the recursive, internally-tagged
 * {@code PredicateExpr} JSON tree the cqlite-flight server parses, applying a
 * <em>partial pushdown</em> strategy:
 *
 * <ul>
 *   <li>A top-level {@code AND} is split into conjuncts. Translatable conjuncts
 *       are pushed; untranslatable ones are returned as residual so Trino keeps
 *       post-filtering them. Results are therefore always correct — pushdown is a
 *       pure optimization.</li>
 *   <li>{@code OR} / {@code NOT} are all-or-nothing: if any sub-expression is
 *       untranslatable, the entire {@code OR}/{@code NOT} stays residual (you
 *       cannot partially push a disjunction or negation without changing the
 *       result).</li>
 * </ul>
 *
 * <p>Emitted node shapes (see the server's {@code PredicateExpr} enum):
 * <pre>
 *   {"type":"And","exprs":[..]}              {"type":"Or","exprs":[..]}
 *   {"type":"Not","expr":..}                 {"type":"IsNull","column":".."}
 *   {"type":"Compare","column":"..","op":"Equal|Gt|Gte|Lt|Lte|Prefix","value":..}
 *   {"type":"In","column":"..","values":[..]}
 * </pre>
 */
public final class PredicateTreeTranslator {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PredicateTreeTranslator() {}

    /**
     * Outcome of translating a top-level predicate.
     *
     * @param pushed   the pushable predicate tree, or empty if nothing could be pushed
     * @param residual conjuncts Trino must still apply (the untranslatable parts);
     *                 empty when the whole expression was pushed
     */
    public record Result(Optional<JsonNode> pushed, List<ConnectorExpression> residual) {}

    /**
     * Translate a top-level predicate into a pushed tree plus a residual list.
     * The residual conjuncts should be re-assembled into {@code remainingFilter}
     * so Trino post-filters them.
     */
    public static Result translate(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        // Top-level AND supports partial pushdown: keep translatable conjuncts,
        // leave the rest as residual.
        if (expression instanceof Call call && isFunction(call, StandardFunctions.AND_FUNCTION_NAME)) {
            List<JsonNode> pushedConjuncts = new ArrayList<>();
            List<ConnectorExpression> residual = new ArrayList<>();
            for (ConnectorExpression arg : call.getArguments()) {
                Optional<JsonNode> node = tryTranslate(arg, assignments);
                if (node.isPresent()) {
                    pushedConjuncts.add(node.get());
                } else {
                    residual.add(arg);
                }
            }
            if (pushedConjuncts.isEmpty()) {
                return new Result(Optional.empty(), List.of(expression));
            }
            JsonNode pushed = pushedConjuncts.size() == 1
                    ? pushedConjuncts.get(0)
                    : andNode(pushedConjuncts);
            return new Result(Optional.of(pushed), List.copyOf(residual));
        }

        // Anything else is all-or-nothing.
        Optional<JsonNode> node = tryTranslate(expression, assignments);
        if (node.isPresent()) {
            return new Result(node, List.of());
        }
        return new Result(Optional.empty(), List.of(expression));
    }

    /** Translate a sub-expression fully, or return empty if any part is untranslatable. */
    private static Optional<JsonNode> tryTranslate(
            ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }
        FunctionName fn = call.getFunctionName();
        List<ConnectorExpression> args = call.getArguments();

        if (fn.equals(StandardFunctions.AND_FUNCTION_NAME)) {
            return translateConnective(args, assignments, true);
        }
        if (fn.equals(StandardFunctions.OR_FUNCTION_NAME)) {
            return translateConnective(args, assignments, false);
        }
        if (fn.equals(StandardFunctions.NOT_FUNCTION_NAME)) {
            if (args.size() != 1) {
                return Optional.empty();
            }
            return tryTranslate(args.get(0), assignments).map(child -> {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("type", "Not");
                node.set("expr", child);
                return node;
            });
        }
        if (fn.equals(StandardFunctions.IS_NULL_FUNCTION_NAME)) {
            if (args.size() != 1) {
                return Optional.empty();
            }
            return column(args.get(0), assignments)
                    // IS NULL is safe for EQUALITY and FULL columns; NONE pushes nothing.
                    .filter(col -> col.capability() != PushdownCapability.NONE)
                    .map(col -> {
                        ObjectNode node = MAPPER.createObjectNode();
                        node.put("type", "IsNull");
                        node.put("column", col.name());
                        return node;
                    });
        }
        if (fn.equals(StandardFunctions.IN_PREDICATE_FUNCTION_NAME)) {
            return translateIn(args, assignments);
        }
        if (fn.equals(StandardFunctions.LIKE_FUNCTION_NAME)) {
            return translateLike(args, assignments);
        }

        Optional<String> op = compareOp(fn);
        if (op.isPresent()) {
            return translateCompare(op.get(), args, assignments);
        }
        return Optional.empty();
    }

    private static Optional<JsonNode> translateConnective(
            List<ConnectorExpression> args, Map<String, ColumnHandle> assignments, boolean and) {
        if (args.isEmpty()) {
            return Optional.empty();
        }
        List<JsonNode> children = new ArrayList<>();
        for (ConnectorExpression arg : args) {
            Optional<JsonNode> child = tryTranslate(arg, assignments);
            if (child.isEmpty()) {
                return Optional.empty(); // all-or-nothing inside a connective
            }
            children.add(child.get());
        }
        return Optional.of(and ? andNode(children) : orNode(children));
    }

    private static Optional<JsonNode> translateCompare(
            String op, List<ConnectorExpression> args, Map<String, ColumnHandle> assignments) {
        if (args.size() != 2) {
            return Optional.empty();
        }
        Optional<CqliteFlightColumnHandle> col = column(args.get(0), assignments);
        if (col.isEmpty() || !(args.get(1) instanceof Constant constant)) {
            return Optional.empty();
        }
        // Gate by the server-declared capability of this column:
        //  - NONE: nothing is pushable (the leaf stays a Trino residual).
        //  - EQUALITY: only exact-match Equal is safe (uuid/timeuuid order by the
        //    CQL uuid type server-side, so ordering would filter incorrectly).
        //  - FULL: every operator, including ordering, is safe.
        PushdownCapability capability = col.get().capability();
        if (capability == PushdownCapability.NONE) {
            return Optional.empty();
        }
        boolean ordering = op.equals("Gt") || op.equals("Gte") || op.equals("Lt") || op.equals("Lte");
        if (ordering && capability != PushdownCapability.FULL) {
            return Optional.empty();
        }
        Optional<JsonNode> value = constantValue(constant);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "Compare");
        node.put("column", col.get().name());
        node.put("op", op);
        node.set("value", value.get());
        return Optional.of(node);
    }

    private static Optional<JsonNode> translateIn(
            List<ConnectorExpression> args, Map<String, ColumnHandle> assignments) {
        if (args.size() != 2) {
            return Optional.empty();
        }
        Optional<CqliteFlightColumnHandle> col = column(args.get(0), assignments);
        if (col.isEmpty() || !(args.get(1) instanceof Call array)
                || !isFunction(array, StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return Optional.empty();
        }
        // IN is exact-match membership, safe for EQUALITY and FULL; NONE pushes nothing.
        if (col.get().capability() == PushdownCapability.NONE) {
            return Optional.empty();
        }
        ArrayNode values = MAPPER.createArrayNode();
        for (ConnectorExpression element : array.getArguments()) {
            if (!(element instanceof Constant constant)) {
                return Optional.empty();
            }
            Optional<JsonNode> value = constantValue(constant);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            values.add(value.get());
        }
        if (values.isEmpty()) {
            return Optional.empty();
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "In");
        node.put("column", col.get().name());
        node.set("values", values);
        return Optional.of(node);
    }

    private static Optional<JsonNode> translateLike(
            List<ConnectorExpression> args, Map<String, ColumnHandle> assignments) {
        // Only LIKE 'prefix%' (the sole wildcard a trailing %, no other %/_, no
        // escape argument) maps to a Prefix compare.
        if (args.size() != 2) {
            return Optional.empty();
        }
        Optional<CqliteFlightColumnHandle> col = column(args.get(0), assignments);
        if (col.isEmpty() || !(args.get(1) instanceof Constant pattern)
                || !(pattern.getValue() instanceof Slice slice)) {
            return Optional.empty();
        }
        // Prefix matching is only meaningful (and only correctly lowered server-side)
        // for FULL columns (genuine text). uuid/timeuuid (EQUALITY) surface as VARCHAR
        // but the server lowers Prefix via the CQL uuid type, which errors on a partial
        // value and never matches; NONE columns push nothing. Leave LIKE residual for both.
        if (col.get().capability() != PushdownCapability.FULL) {
            return Optional.empty();
        }
        String text = slice.toStringUtf8();
        Optional<String> prefix = prefixOf(text);
        if (prefix.isEmpty()) {
            return Optional.empty();
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "Compare");
        node.put("column", col.get().name());
        node.put("op", "Prefix");
        node.put("value", prefix.get());
        return Optional.of(node);
    }

    /**
     * Extract the literal prefix of a LIKE pattern that is exactly {@code prefix%}
     * with no other wildcards. Returns empty for any pattern that is not a pure
     * trailing-% prefix (e.g. {@code %x}, {@code a_b%}, {@code a%b}).
     */
    static Optional<String> prefixOf(String pattern) {
        if (pattern.isEmpty() || !pattern.endsWith("%")) {
            return Optional.empty();
        }
        String body = pattern.substring(0, pattern.length() - 1);
        // No other wildcards allowed in the literal prefix. (LIKE has no escape
        // arg here, so a raw % or _ is a wildcard and we cannot push it.)
        if (body.indexOf('%') >= 0 || body.indexOf('_') >= 0) {
            return Optional.empty();
        }
        if (body.isEmpty()) {
            return Optional.empty(); // "%" matches everything; nothing to push
        }
        return Optional.of(body);
    }

    /** Map a Trino comparison FunctionName to the server's op spelling. */
    private static Optional<String> compareOp(FunctionName fn) {
        if (fn.equals(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of("Equal");
        }
        if (fn.equals(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of("Lt");
        }
        if (fn.equals(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of("Lte");
        }
        if (fn.equals(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of("Gt");
        }
        if (fn.equals(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of("Gte");
        }
        return Optional.empty();
    }

    /** Resolve a {@link Variable} argument to the mapped column handle. */
    private static Optional<CqliteFlightColumnHandle> column(
            ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!(expression instanceof Variable variable)) {
            return Optional.empty();
        }
        ColumnHandle handle = assignments.get(variable.getName());
        if (handle instanceof CqliteFlightColumnHandle cqlite) {
            return Optional.of(cqlite);
        }
        return Optional.empty();
    }

    /**
     * Convert a {@link Constant} into the JSON scalar the server decodes for the
     * matching CQL column type. Returns empty for NULL values or types we cannot
     * faithfully translate.
     */
    private static Optional<JsonNode> constantValue(Constant constant) {
        Object value = constant.getValue();
        if (value == null) {
            return Optional.empty();
        }
        Type type = constant.getType();
        if (type instanceof VarcharType) {
            if (value instanceof Slice slice) {
                return Optional.of(MAPPER.getNodeFactory().textNode(slice.toStringUtf8()));
            }
            if (value instanceof String s) {
                return Optional.of(MAPPER.getNodeFactory().textNode(s));
            }
            return Optional.empty();
        }
        if (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType) {
            if (value instanceof Long l) {
                return Optional.of(MAPPER.getNodeFactory().numberNode(l));
            }
            if (value instanceof Number n) {
                return Optional.of(MAPPER.getNodeFactory().numberNode(n.longValue()));
            }
            return Optional.empty();
        }
        if (type instanceof BooleanType) {
            if (value instanceof Boolean b) {
                return Optional.of(MAPPER.getNodeFactory().booleanNode(b));
            }
            return Optional.empty();
        }
        if (type instanceof DoubleType) {
            if (value instanceof Double d) {
                return Optional.of(MAPPER.getNodeFactory().numberNode(d));
            }
            if (value instanceof Number n) {
                return Optional.of(MAPPER.getNodeFactory().numberNode(n.doubleValue()));
            }
            return Optional.empty();
        }
        if (type instanceof RealType) {
            // Trino's native REAL representation is the int bits of a float (Long).
            if (value instanceof Long bits) {
                return Optional.of(MAPPER.getNodeFactory()
                        .numberNode((double) Float.intBitsToFloat(bits.intValue())));
            }
            if (value instanceof Float f) {
                return Optional.of(MAPPER.getNodeFactory().numberNode((double) f));
            }
            return Optional.empty();
        }
        return Optional.empty();
    }

    /**
     * Combine two already-translated predicate trees with a logical {@code And},
     * flattening either side that is itself an {@code And} so repeated
     * accumulation across {@code applyFilter} calls does not nest unboundedly.
     */
    public static JsonNode and(JsonNode left, JsonNode right) {
        List<JsonNode> children = new ArrayList<>();
        appendConjuncts(left, children);
        appendConjuncts(right, children);
        return andNode(children);
    }

    private static void appendConjuncts(JsonNode expr, List<JsonNode> out) {
        if (expr != null && expr.has("type") && "And".equals(expr.get("type").asText())
                && expr.has("exprs") && expr.get("exprs").isArray()) {
            expr.get("exprs").forEach(out::add);
        } else {
            out.add(expr);
        }
    }

    private static ObjectNode andNode(List<JsonNode> children) {
        return connectiveNode("And", children);
    }

    private static ObjectNode orNode(List<JsonNode> children) {
        return connectiveNode("Or", children);
    }

    private static ObjectNode connectiveNode(String type, List<JsonNode> children) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", type);
        ArrayNode exprs = node.putArray("exprs");
        children.forEach(exprs::add);
        return node;
    }

    private static boolean isFunction(Call call, FunctionName name) {
        return call.getFunctionName().equals(name);
    }
}
