package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

/**
 * One unit of work: scan a single token range from a single replica's
 * cqlite-flight endpoint. Pinning each range to exactly one replica is how
 * cross-replica duplication is avoided (PLAN §2).
 *
 * <p>{@code tokenStart} is exclusive, {@code tokenEnd} inclusive; {@code wraparound}
 * is set when the range crosses the ring's min-token boundary ({@code start > end}).
 */
public record CqliteFlightSplit(
        String keyspace,
        String table,
        String ddl,
        String host,
        int port,
        long tokenStart,
        long tokenEnd,
        boolean wraparound)
        implements ConnectorSplit {

    @Override
    public List<HostAddress> getAddresses() {
        // Soft locality hint: the replica serving this split.
        return List.of(HostAddress.fromParts(host, port));
    }
}
