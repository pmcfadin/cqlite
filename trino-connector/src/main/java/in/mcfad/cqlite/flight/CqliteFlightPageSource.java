package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Streams one split's Arrow Flight batches, converting each to a Trino page.
 *
 * <p>Replica failover (issue #2241): the split carries an ORDERED replica list
 * ({@link CqliteFlightSplit#replicaHosts()}, primary first). Before the first batch is delivered,
 * a connection-establishment/UNAVAILABLE-class failure ({@link ReplicaFailover#isConnectClass})
 * fails over to the next replica that owns the range. Once ANY batch has been delivered the stream
 * is committed: a later failure is fatal and never retried, because re-reading from another replica
 * could duplicate already-emitted rows. If every replica is unreachable the query fails loudly —
 * CQLite never returns a silent partial/empty result.
 */
public class CqliteFlightPageSource implements ConnectorPageSource {

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

    private final List<CqliteFlightColumnHandle> columns;
    private final List<String> hosts;
    private final int port;
    private final byte[] ticket;
    private final StreamOpener opener;

    private BatchStream stream;
    private int hostIndex;
    private boolean started;
    private boolean finished;
    private long completedPositions;

    public CqliteFlightPageSource(
            CqliteFlightClient client,
            CqliteFlightSplit split,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket) {
        this(split.replicaHosts(), split.port(), columns, ticket, adapt(client));
    }

    /** Package-private seam: inject the ordered host list + opener directly for unit tests. */
    CqliteFlightPageSource(
            List<String> hosts,
            int port,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket,
            StreamOpener opener) {
        this.hosts = List.copyOf(hosts);
        this.port = port;
        this.columns = columns;
        this.ticket = ticket;
        this.opener = opener;
    }

    /** The production opener: a real DoGet stream wrapped so failover sees a uniform interface. */
    private static StreamOpener adapt(CqliteFlightClient client) {
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

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        while (true) {
            try {
                if (stream == null) {
                    stream = opener.open(hosts.get(hostIndex), port, ticket);
                }
                if (!stream.next()) {
                    finished = true;
                    return null;
                }
                VectorSchemaRoot root = stream.getRoot();
                Page page = ArrowToTrino.toPage(root, columns);
                completedPositions += page.getPositionCount();
                started = true;
                return SourcePage.create(page);
            } catch (RuntimeException e) {
                // Release the gRPC channel + Arrow buffers; Trino does not guarantee close() on
                // the throw path.
                closeStreamQuietly();
                if (!started && hostIndex + 1 < hosts.size() && ReplicaFailover.isConnectClass(e)) {
                    hostIndex++;
                    continue; // fail over to the next replica that owns this range (#2241)
                }
                // Committed (rows already delivered) or no replica left, or a non-connect error:
                // fail loudly — never a silent partial result.
                finished = true;
                throw e;
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() {
        finished = true;
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
}
