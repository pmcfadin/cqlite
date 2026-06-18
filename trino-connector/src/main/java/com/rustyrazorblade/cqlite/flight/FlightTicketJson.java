package com.rustyrazorblade.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Optional;

/**
 * Builds the JSON Flight ticket consumed by the cqlite-flight server
 * (see the Rust {@code FlightTicket}). Field names and the {@code op} enum
 * spellings must match the server's serde representation.
 */
public final class FlightTicketJson {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int TICKET_VERSION = 1;

    private FlightTicketJson() {}

    /** A pushed-down predicate destined for the ticket. */
    public record Predicate(String column, String op, Object value) {}

    /** Build the ticket JSON bytes. */
    public static byte[] build(
            String keyspace,
            String table,
            String ddl,
            Optional<String> snapshot,
            Optional<Long> tokenStart,
            Optional<Long> tokenEnd,
            boolean wraparound,
            Optional<List<String>> columns,
            List<Predicate> predicates) {
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
        try {
            return MAPPER.writeValueAsBytes(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Flight ticket", e);
        }
    }
}
