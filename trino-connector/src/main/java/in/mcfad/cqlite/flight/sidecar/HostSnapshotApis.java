package in.mcfad.cqlite.flight.sidecar;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the {@link SnapshotApi} for a specific replica host (issue #2227).
 *
 * <p>The Cassandra Sidecar runs as a {@code hostNetwork} DaemonSet on every db node at a
 * fixed port, with no stable cluster-wide Service. A snapshot {@code PUT} is therefore
 * <em>instance-local</em>: it creates the snapshot only on the node fronting that one
 * Sidecar. A multi-node scan fans splits across every replica host, so the connector must
 * create the per-query snapshot on <b>each</b> replica host's own Sidecar — otherwise a
 * split routed to any other host reads a directory that does not exist and fails NotFound.
 *
 * <p>Every db node's Sidecar shares the same fixed port and scheme as the configured
 * {@code cqlite.sidecar-uri} (the connector only ever talks to db0 for discovery, but the
 * Sidecar port is uniform across the DaemonSet). This factory therefore derives each
 * host's Sidecar URI from that scheme + port and the split's host address.
 */
public interface HostSnapshotApis {

    /** The snapshot API for the Sidecar co-located with {@code host}. */
    SnapshotApi forHost(String host);

    /**
     * Per-host {@link SidecarClient} factory deriving each host's Sidecar URI from the
     * configured base URI's scheme + port. Clients are cached per host so repeated splits
     * on the same replica reuse one connection pool.
     */
    static HostSnapshotApis fromBaseUri(URI base) {
        return new PerHostSidecarClients(base);
    }

    /** Default {@link #fromBaseUri} implementation. */
    final class PerHostSidecarClients implements HostSnapshotApis {
        private final String scheme;
        private final int port;
        private final Map<String, SnapshotApi> cache = new ConcurrentHashMap<>();

        PerHostSidecarClients(URI base) {
            this.scheme = base.getScheme();
            this.port = base.getPort();
        }

        @Override
        public SnapshotApi forHost(String host) {
            if (port < 0) {
                throw new IllegalStateException(
                        "cqlite.sidecar-uri must include an explicit port so per-host snapshot "
                                + "creation can reach each replica's Sidecar (snapshot read-mode); host=" + host);
            }
            return cache.computeIfAbsent(host,
                    h -> new SidecarClient(URI.create(scheme + "://" + bracketIfIpv6(h) + ":" + port)));
        }

        /** Wrap a bare IPv6 literal in {@code [...]} so the authority parses. */
        private static String bracketIfIpv6(String host) {
            if (host.indexOf(':') >= 0 && host.charAt(0) != '[') {
                return "[" + host + "]";
            }
            return host;
        }
    }
}
