package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * One unit of work: scan a single token range from a replica's cqlite-flight
 * endpoint. Each range is still read from ONE replica at a time (so cross-replica
 * duplication is avoided, PLAN §2), but the split carries an ORDERED replica list
 * for availability failover (issue #2241): {@code host} is the primary; {@code
 * fallbackHosts} are the range's other replica owners, in try order, that the page
 * source may fail over to if the primary's Flight endpoint is unreachable.
 *
 * <p>{@code tokenStart} is exclusive, {@code tokenEnd} inclusive; {@code wraparound}
 * is set when the range crosses the ring's min-token boundary ({@code start > end}).
 *
 * <p>{@code snapshot} is the Sidecar snapshot name the ticket reads (issue #2105):
 * present in {@link ReadMode#SNAPSHOT}, {@link Optional#empty()} in {@link ReadMode#LIVE}.
 * In snapshot mode a fallback is only listed here if its host also has the snapshot
 * (issue #2227 creates it per replica host); in live mode every replica owner is eligible.
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
        Optional<String> snapshot,
        List<String> fallbackHosts)
        implements ConnectorSplit {

    /** Normalize the fallback list to an immutable copy (never null). */
    public CqliteFlightSplit {
        fallbackHosts = fallbackHosts == null ? List.of() : List.copyOf(fallbackHosts);
    }

    /**
     * Backward-compatible construction with no availability fallbacks (single-replica
     * split). Kept so existing call sites and tests compile unchanged.
     */
    public CqliteFlightSplit(
            String keyspace,
            String table,
            String ddl,
            String host,
            int port,
            long tokenStart,
            long tokenEnd,
            boolean wraparound,
            Optional<String> snapshot) {
        this(keyspace, table, ddl, host, port, tokenStart, tokenEnd, wraparound, snapshot, List.of());
    }

    /**
     * The ordered replica list to try for this range (issue #2241): the primary
     * {@link #host} first, then each {@link #fallbackHosts} entry in order.
     */
    public List<String> replicaHosts() {
        List<String> all = new ArrayList<>(1 + fallbackHosts.size());
        all.add(host);
        all.addAll(fallbackHosts);
        return all;
    }

    @Override
    public List<HostAddress> getAddresses() {
        // Soft locality hints: every replica that can serve this split (#2241), primary first.
        List<HostAddress> addrs = new ArrayList<>(1 + fallbackHosts.size());
        for (String h : replicaHosts()) {
            addrs.add(HostAddress.fromParts(h, port));
        }
        return addrs;
    }
}
