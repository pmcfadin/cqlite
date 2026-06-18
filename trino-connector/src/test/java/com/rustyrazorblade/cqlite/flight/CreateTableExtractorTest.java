package com.rustyrazorblade.cqlite.flight;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateTableExtractorTest {

    private static final String KEYSPACE_SCHEMA = """
            CREATE TYPE ks.address (street text, zip int);

            CREATE TABLE ks.users (
                id uuid PRIMARY KEY,
                name text
            ) WITH bloom_filter_fp_chance = 0.01;

            CREATE TABLE ks.events (
                pk int,
                ck text,
                val int,
                PRIMARY KEY (pk, ck)
            ) WITH CLUSTERING ORDER BY (ck ASC);
            """;

    @Test
    void extractsTheRequestedTable() {
        String ddl = CreateTableExtractor.extract(KEYSPACE_SCHEMA, "ks", "events").orElseThrow();
        assertTrue(ddl.startsWith("CREATE TABLE ks.events"), ddl);
        assertTrue(ddl.contains("PRIMARY KEY (pk, ck)"));
        assertTrue(ddl.trim().endsWith(";"));
        // Must not bleed into the other table.
        assertTrue(!ddl.contains("users"));
    }

    @Test
    void extractsFirstTableNotTheType() {
        String ddl = CreateTableExtractor.extract(KEYSPACE_SCHEMA, "ks", "users").orElseThrow();
        assertTrue(ddl.startsWith("CREATE TABLE ks.users"), ddl);
        assertTrue(ddl.contains("name text"));
    }

    @Test
    void missingTableReturnsEmpty() {
        assertEquals(true, CreateTableExtractor.extract(KEYSPACE_SCHEMA, "ks", "nope").isEmpty());
    }
}
