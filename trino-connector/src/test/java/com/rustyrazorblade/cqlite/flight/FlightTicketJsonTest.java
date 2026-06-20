package com.rustyrazorblade.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                List.of(new FlightTicketJson.Predicate("v", "Gt", 10)),
                null);
        JsonNode node = parse(bytes);

        assertEquals(2, node.get("version").asInt());
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

        // A null filter is omitted entirely (server's #[serde(default)] Option).
        assertFalse(node.has("filter"));
    }

    @Test
    void emitsRecursiveFilterTreeWhenPresent() throws Exception {
        // (age > 10 AND name = 'x') OR NOT active IS NULL
        JsonNode filter = MAPPER.readTree("""
                {"type":"Or","exprs":[
                  {"type":"And","exprs":[
                    {"type":"Compare","column":"age","op":"Gt","value":10},
                    {"type":"Compare","column":"name","op":"Equal","value":"x"}]},
                  {"type":"Not","expr":{"type":"IsNull","column":"active"}}]}""");
        byte[] bytes = FlightTicketJson.build(
                "ks", "t", "ddl",
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of(), filter);
        JsonNode node = parse(bytes);

        assertEquals(2, node.get("version").asInt());
        JsonNode emitted = node.get("filter");
        assertEquals("Or", emitted.get("type").asText());
        JsonNode and = emitted.get("exprs").get(0);
        assertEquals("And", and.get("type").asText());
        assertEquals("Gt", and.get("exprs").get(0).get("op").asText());
        assertEquals(10, and.get("exprs").get(0).get("value").asInt());
        assertEquals("x", and.get("exprs").get(1).get("value").asText());
        JsonNode not = emitted.get("exprs").get(1);
        assertEquals("Not", not.get("type").asText());
        assertEquals("IsNull", not.get("expr").get("type").asText());
        assertEquals("active", not.get("expr").get("column").asText());
    }

    @Test
    void omittedOptionalsAreNull() throws Exception {
        byte[] bytes = FlightTicketJson.build(
                "ks", "t", "ddl",
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of(), null);
        JsonNode node = parse(bytes);
        assertTrue(node.get("snapshot").isNull());
        assertTrue(node.get("token_start").isNull());
        assertTrue(node.get("columns").isNull());
        assertEquals(0, node.get("predicates").size());
    }
}
