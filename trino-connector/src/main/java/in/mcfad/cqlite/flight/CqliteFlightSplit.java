package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

/**
 * One unit of work: scan a single token range from a single replica's
 * cqlite-flight endpoint. Pinning each range to exactly one replica is how
 * cross-replica duplication is avoided (PLAN §2).
 *
 * <p>{@code tokenStart} is exclusive, {@code tokenEnd} inclusive; {@code wraparound}
 * is set when the range crosses the ring's min-token boundary ({@code start > end}).
 *
 * <p>{@code snapshot} is the Sidecar snapshot name the ticket reads (issue #2105):
 * present in {@link ReadMode#SNAPSHOT}, {@link Optional#empty()} in {@link ReadMode#LIVE}.
 */
public record CqliteFlightSplit(
        String keyspace,
        String table,
        String ddl,
        String host,
        int port,
        long tokenStart,
        long tokenEnd,
        boolean wraparound,
        Optional<String> snapshot)
        implements ConnectorSplit {

    @Override
    public List<HostAddress> getAddresses() {
        // Soft locality hint: the replica serving this split.
        return List.of(HostAddress.fromParts(host, port));
    }
}
