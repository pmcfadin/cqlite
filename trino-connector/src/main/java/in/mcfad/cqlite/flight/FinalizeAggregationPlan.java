package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Connector-internal plan describing how the finalize {@code PageSource} turns the
 * merged server outputs into the Trino result columns declared by
 * {@link CqliteFlightMetadata#applyAggregation}.
 *
 * <p>This is NOT sent to the server (the wire spec is {@link AggregationSpec}); it
 * is carried on the table handle and re-parsed by the page source so the avg
 * decomposition ({@code Sum}/{@code Count} pair → one DOUBLE column) and the
 * group-by passthrough are reconstructed without re-deriving them.
 */
public record FinalizeAggregationPlan(List<String> groupBy, List<OutputColumn> outputs) {

    /** How one Trino result column is produced. */
    public enum Kind {
        /** A passthrough group-by column; {@code primary} is the group-by column name. */
        GROUP,
        /** A direct server output (count/sum/min/max); {@code primary} is its output name. */
        DIRECT,
        /** An avg: ΣSum/ΣCount; {@code primary} = sum output, {@code secondary} = count output. */
        AVG
    }

    /**
     * @param resultName the Trino result column name (the {@code Assignment} variable)
     * @param kind       how to compute it
     * @param primary    the principal source name (group col / sum / direct output)
     * @param secondary  the count output name for {@link Kind#AVG}, else {@code null}
     */
    public record OutputColumn(String resultName, Kind kind, String primary, String secondary) {}

    public JsonNode toJson(ObjectMapper mapper) {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode gb = root.putArray("group_by");
        groupBy.forEach(gb::add);
        ArrayNode outs = root.putArray("outputs");
        for (OutputColumn o : outputs) {
            ObjectNode n = outs.addObject();
            n.put("result_name", o.resultName());
            n.put("kind", o.kind().name());
            n.put("primary", o.primary());
            if (o.secondary() == null) {
                n.putNull("secondary");
            } else {
                n.put("secondary", o.secondary());
            }
        }
        return root;
    }

    public static FinalizeAggregationPlan fromJson(JsonNode node) {
        List<String> groupBy = new ArrayList<>();
        node.get("group_by").forEach(n -> groupBy.add(n.asText()));
        List<OutputColumn> outputs = new ArrayList<>();
        for (JsonNode o : node.get("outputs")) {
            String secondary = o.get("secondary").isNull() ? null : o.get("secondary").asText();
            outputs.add(new OutputColumn(
                    o.get("result_name").asText(),
                    Kind.valueOf(o.get("kind").asText()),
                    o.get("primary").asText(),
                    secondary));
        }
        return new FinalizeAggregationPlan(groupBy, outputs);
    }
}
