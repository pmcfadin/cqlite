package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Builds the JSON Flight ticket consumed by the cqlite-flight server
 * (see the Rust {@code FlightTicket}). Field names and the {@code op} enum
 * spellings must match the server's serde representation.
 */
public final class FlightTicketJson {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int TICKET_VERSION = 2;

    private FlightTicketJson() {}

    /** A pushed-down predicate destined for the ticket. */
    public record Predicate(String column, String op, Object value) {}

    /**
     * Build the ticket JSON bytes.
     *
     * @param filter the recursive {@code PredicateExpr} tree pushed down to the
     *               server, or {@code null} to omit it (the server's
     *               {@code #[serde(default)] Option<PredicateExpr>}). The legacy
     *               flat {@code predicates} list is independent and still emitted.
     * @param aggregation the {@code aggregation} object (group_by + aggregates)
     *               pushed down to the server, or {@code null} to omit it (the
     *               server's {@code #[serde(default)] Option<Aggregation>}).
     */
    public static byte[] build(
            String keyspace,
            String table,
            String ddl,
            Optional<String> snapshot,
            Optional<Long> tokenStart,
            Optional<Long> tokenEnd,
            boolean wraparound,
            Optional<List<String>> columns,
            List<Predicate> predicates,
            JsonNode filter,
            JsonNode aggregation) {
        return build(keyspace, table, ddl, snapshot, tokenStart, tokenEnd, wraparound,
                columns, predicates, filter, aggregation, OptionalLong.empty());
    }

    /**
     * Build the ticket JSON bytes, additionally carrying a row {@code limit}
     * (issue #2129).
     *
     * @param limit a per-split row cap for the server to stop its merge early
     *              (LIMIT pushdown), or {@link OptionalLong#empty()} to omit it —
     *              matching the server's {@code #[serde(default)] Option<u64>}
     *              (an omitted field parses as {@code None} = full scan). The cap
     *              is applied server-side AFTER predicate filtering; because each
     *              split caps independently, the connector sets
     *              {@code limitGuaranteed = false}.
     */
    public static byte[] build(
            String keyspace,
            String table,
            String ddl,
            Optional<String> snapshot,
            Optional<Long> tokenStart,
            Optional<Long> tokenEnd,
            boolean wraparound,
            Optional<List<String>> columns,
            List<Predicate> predicates,
            JsonNode filter,
            JsonNode aggregation,
            OptionalLong limit) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("version", TICKET_VERSION);
        root.put("keyspace", keyspace);
        root.put("table", table);
        root.put("ddl", ddl);
        snapshot.ifPresentOrElse(s -> root.put("snapshot", s), () -> root.putNull("snapshot"));
        tokenStart.ifPresentOrElse(t -> root.put("token_start", t), () -> root.putNull("token_start"));
        tokenEnd.ifPresentOrElse(t -> root.put("token_end", t), () -> root.putNull("token_end"));
        root.put("wraparound", wraparound);
        if (columns.isPresent()) {
            ArrayNode cols = root.putArray("columns");
            columns.get().forEach(cols::add);
        } else {
            root.putNull("columns");
        }
        ArrayNode preds = root.putArray("predicates");
        for (Predicate p : predicates) {
            ObjectNode node = preds.addObject();
            node.put("column", p.column());
            node.put("op", p.op());
            node.set("value", MAPPER.valueToTree(p.value()));
        }
        // Omit when null to match the server's #[serde(default)] Option<PredicateExpr>.
        if (filter != null) {
            root.set("filter", filter);
        }
        // Omit when null to match the server's #[serde(default)] Option<Aggregation>.
        if (aggregation != null) {
            root.set("aggregation", aggregation);
        }
        // Omit when empty to match the server's #[serde(default)] Option<u64>.
        limit.ifPresent(l -> root.put("limit", l));
        try {
            return MAPPER.writeValueAsBytes(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Flight ticket", e);
        }
    }
}
