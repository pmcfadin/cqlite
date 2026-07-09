package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Streams one split's Arrow Flight batches, converting each to a Trino page.
 *
 * <p>Replica failover (issue #2241) is handled by the shared {@link ReplicaFailoverStream}: the
 * split's ordered {@link CqliteFlightSplit#replicaHosts()} (primary first) are tried in order,
 * failing over to the next replica on a connect-class failure before the first batch, and
 * failing loudly once committed or when every replica is unreachable — CQLite never returns a
 * silent partial/empty result.
 */
public class CqliteFlightPageSource implements ConnectorPageSource {
    private final List<CqliteFlightColumnHandle> columns;
    private final ReplicaFailoverStream stream;
    private boolean finished;
    private long completedPositions;

    public CqliteFlightPageSource(
            CqliteFlightClient client,
            CqliteFlightSplit split,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket) {
        this(split.replicaHosts(), split.port(), columns, ticket, ReplicaFailoverStream.adapt(client));
    }

    /** Package-private seam: inject the ordered host list + opener directly for unit tests. */
    CqliteFlightPageSource(
            List<String> hosts,
            int port,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket,
            ReplicaFailoverStream.StreamOpener opener) {
        this.columns = columns;
        this.stream = new ReplicaFailoverStream(hosts, port, ticket, opener);
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        try {
            if (!stream.next()) {
                finished = true;
                return null;
            }
            VectorSchemaRoot root = stream.getRoot();
            Page page = ArrowToTrino.toPage(root, columns);
            completedPositions += page.getPositionCount();
            return SourcePage.create(page);
        } catch (RuntimeException e) {
            // Release the gRPC channel + Arrow buffers; Trino does not guarantee close() on the
            // throw path.
            close();
            throw e;
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
        stream.close();
    }
}
