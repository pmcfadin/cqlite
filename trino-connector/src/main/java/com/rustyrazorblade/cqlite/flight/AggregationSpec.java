package com.rustyrazorblade.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The server-side aggregation specification pushed down in the Flight ticket.
 *
 * <p>Mirrors the cqlite-flight server's wire contract exactly. The {@code group_by}
 * list names the grouping columns; the {@code aggregates} list names the partial
 * aggregates the server must compute. Server funcs are ONLY
 * {@link Func#Count}/{@link Func#Sum}/{@link Func#Min}/{@link Func#Max} — there is
 * no {@code Avg} on the wire; {@code avg(x)} is decomposed connector-side into
 * {@code Sum(x)} + {@code Count(x)} (see {@link CqliteFlightMetadata#applyAggregation}).
 *
 * <p>The server returns PARTIAL rows whose Arrow columns are, in order:
 * the {@code group_by} columns (their natural types), then one column per aggregate
 * named by its {@code output}. {@code Count} → Int64 (never null);
 * {@code Sum} → Int64 for integer source types or Float64 for float/double (null if
 * no non-null inputs); {@code Min}/{@code Max} → source column type (null if none).
 * A global (empty {@code group_by}) aggregation returns exactly one row even on
 * empty input.
 */
public record AggregationSpec(List<String> groupBy, List<Aggregate> aggregates) {

    /** Server-supported aggregate functions (no {@code Avg} on the wire). */
    public enum Func {
        Count,
        Sum,
        Min,
        Max
    }

    /**
     * One partial aggregate the server computes.
     *
     * @param func   the server function
     * @param column the source column, or {@code null} for {@code count(*)}
     * @param output the deterministic output column name (e.g. {@code agg0})
     */
    public record Aggregate(Func func, String column, String output) {}

    /** Serialize to the {@code aggregation} JSON object the ticket carries. */
    public JsonNode toJson(ObjectMapper mapper) {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode gb = root.putArray("group_by");
        for (String col : groupBy) {
            gb.add(col);
        }
        ArrayNode aggs = root.putArray("aggregates");
        for (Aggregate a : aggregates) {
            ObjectNode node = aggs.addObject();
            node.put("func", a.func().name()); // "Count"/"Sum"/"Min"/"Max" — matches server serde
            if (a.column() == null) {
                node.putNull("column");
            } else {
                node.put("column", a.column());
            }
            node.put("output", a.output());
        }
        return root;
    }
}
