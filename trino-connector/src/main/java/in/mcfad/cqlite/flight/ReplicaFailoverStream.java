package in.mcfad.cqlite.flight;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Shared availability-failover retry loop (issue #2241) for a SINGLE range's batch stream: tries
 * an ordered replica host list (primary first, then fallbacks) — as returned by {@link
 * CqliteFlightSplit#replicaHosts()} — and fails over to the next host on a
 * connection-establishment/UNAVAILABLE-class failure ({@link ReplicaFailover#isConnectClass})
 * BEFORE any batch has been delivered FROM THIS STREAM. Once a batch has been delivered the
 * stream is committed: a later failure is fatal and never retried, because re-reading from
 * another replica could duplicate already-emitted rows. If every host is unreachable the caller
 * sees the last connect failure propagate — loud, never a silent partial/empty result.
 *
 * <p>Used by both the per-split scan page source ({@link CqliteFlightPageSource}) and the
 * aggregate finalize page source's per-range fan-out ({@link CqliteFlightAggregatePageSource}) —
 * a single shared implementation so scan and aggregate paths get identical failover semantics.
 */
final class ReplicaFailoverStream implements AutoCloseable {

    /** One replica's open batch stream — abstracted so failover is unit-testable off-cluster. */
    interface BatchStream extends AutoCloseable {
        /**
         * Advance to the next batch; {@code false} at end of stream. The FIRST call performs the
         * actual gRPC and may throw a connect-class failure that triggers failover.
         */
        boolean next();

        VectorSchemaRoot getRoot();

        @Override
        void close();
    }

    /** Opens a {@link BatchStream} against one replica {@code host:port} for a ticket. */
    @FunctionalInterface
    interface StreamOpener {
        BatchStream open(String host, int port, byte[] ticket);
    }

    private final List<String> hosts;
    private final int port;
    private final byte[] ticket;
    private final StreamOpener opener;

    private BatchStream stream;
    private int hostIndex;
    private boolean started;

    ReplicaFailoverStream(List<String> hosts, int port, byte[] ticket, StreamOpener opener) {
        this.hosts = List.copyOf(hosts);
        this.port = port;
        this.ticket = ticket;
        this.opener = opener;
    }

    /**
     * Advance to the next batch, transparently failing over to the next replica host on a
     * connect-class failure before any batch of THIS stream has been delivered.
     *
     * @return {@code false} at end of stream
     * @throws RuntimeException the underlying failure once no replica is left to try, or ANY
     *                          failure (connect-class or not) once this stream is committed
     */
    boolean next() {
        while (true) {
            try {
                if (stream == null) {
                    stream = opener.open(hosts.get(hostIndex), port, ticket);
                }
                boolean hasNext = stream.next();
                if (hasNext) {
                    started = true;
                }
                return hasNext;
            } catch (RuntimeException e) {
                closeStreamQuietly();
                if (!started && hostIndex + 1 < hosts.size() && ReplicaFailover.isConnectClass(e)) {
                    hostIndex++;
                    continue; // fail over to the next replica that owns this range (#2241)
                }
                throw e; // committed, no replica left, or a non-connect error: fail loudly
            }
        }
    }

    VectorSchemaRoot getRoot() {
        return stream.getRoot();
    }

    @Override
    public void close() {
        closeStreamQuietly();
    }

    private void closeStreamQuietly() {
        if (stream != null) {
            try {
                stream.close();
            } catch (RuntimeException ignore) {
                // best-effort release
            }
            stream = null;
        }
    }

    /** The production opener: a real DoGet stream wrapped so failover sees a uniform interface. */
    static StreamOpener adapt(CqliteFlightClient client) {
        return (host, port, ticket) -> {
            CqliteFlightClient.StreamHandle handle = client.openStream(host, port, ticket);
            return new BatchStream() {
                @Override
                public boolean next() {
                    return handle.stream().next();
                }

                @Override
                public VectorSchemaRoot getRoot() {
                    return handle.stream().getRoot();
                }

                @Override
                public void close() {
                    handle.close();
                }
            };
        };
    }
}
