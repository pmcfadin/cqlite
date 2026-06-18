package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ConnectorTableHandle;

/**
 * Identifies a table to scan. Carries the CQL DDL (fetched from Sidecar) so the
 * split manager and page source can pass it to the cqlite-flight server in the
 * Flight ticket without re-querying.
 */
public record CqliteFlightTableHandle(String keyspace, String table, String ddl)
        implements ConnectorTableHandle {}
