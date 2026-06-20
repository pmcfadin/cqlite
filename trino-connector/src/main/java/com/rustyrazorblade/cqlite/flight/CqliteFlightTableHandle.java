package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

/**
 * Identifies a table to scan. Carries the CQL DDL (fetched from Sidecar) so the
 * split manager and page source can pass it to the cqlite-flight server in the
 * Flight ticket without re-querying.
 *
 * <p>{@code filterJson} holds the serialized {@code PredicateExpr} tree pushed
 * down by {@link CqliteFlightMetadata#applyFilter}, or empty when no predicate
 * could be pushed. It is threaded through to the Flight ticket so the server
 * pre-filters rows; any untranslatable predicate stays a Trino residual filter,
 * so results are correct regardless.
 */
public record CqliteFlightTableHandle(String keyspace, String table, String ddl, Optional<String> filterJson)
        implements ConnectorTableHandle {

    /** Convenience constructor for a handle with no pushed-down filter. */
    public CqliteFlightTableHandle(String keyspace, String table, String ddl) {
        this(keyspace, table, ddl, Optional.empty());
    }
}
