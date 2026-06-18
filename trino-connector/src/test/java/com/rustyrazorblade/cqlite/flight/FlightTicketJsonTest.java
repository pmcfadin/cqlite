package com.rustyrazorblade.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlightTicketJsonTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode parse(byte[] bytes) throws Exception {
        return MAPPER.readTree(bytes);
    }

    @Test
    void buildsTicketMatchingServerSchema() throws Exception {
        byte[] bytes = FlightTicketJson.build(
                "ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY, v int)",
                Optional.of("snap1"),
                Optional.of(-100L), Optional.of(100L), false,
                Optional.of(List.of("id", "v")),
                List.of(new FlightTicketJson.Predicate("v", "Gt", 10)));
        JsonNode node = parse(bytes);

        assertEquals(1, node.get("version").asInt());
        assertEquals("ks", node.get("keyspace").asText());
        assertEquals("t", node.get("table").asText());
        assertEquals("snap1", node.get("snapshot").asText());
        assertEquals(-100, node.get("token_start").asLong());
        assertEquals(100, node.get("token_end").asLong());
        assertEquals(false, node.get("wraparound").asBoolean());
        assertEquals(List.of("id", "v"),
                List.of(node.get("columns").get(0).asText(), node.get("columns").get(1).asText()));

        JsonNode pred = node.get("predicates").get(0);
        assertEquals("v", pred.get("column").asText());
        assertEquals("Gt", pred.get("op").asText(), "op must match Rust enum spelling");
        assertEquals(10, pred.get("value").asInt());
    }

    @Test
    void omittedOptionalsAreNull() throws Exception {
        byte[] bytes = FlightTicketJson.build(
                "ks", "t", "ddl",
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of());
        JsonNode node = parse(bytes);
        assertTrue(node.get("snapshot").isNull());
        assertTrue(node.get("token_start").isNull());
        assertTrue(node.get("columns").isNull());
        assertEquals(0, node.get("predicates").size());
    }
}
