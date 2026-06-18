package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Streams one split's Arrow Flight batches, converting each to a Trino page.
 */
public class CqliteFlightPageSource implements ConnectorPageSource {
    private final List<CqliteFlightColumnHandle> columns;
    private final CqliteFlightClient.StreamHandle handle;
    private boolean finished;
    private long completedPositions;

    public CqliteFlightPageSource(
            CqliteFlightClient client,
            CqliteFlightSplit split,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket) {
        this.columns = columns;
        this.handle = client.openStream(split.host(), split.port(), ticket);
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        try {
            if (!handle.stream().next()) {
                finished = true;
                return null;
            }
            VectorSchemaRoot root = handle.stream().getRoot();
            Page page = ArrowToTrino.toPage(root, columns);
            completedPositions += page.getPositionCount();
            return SourcePage.create(page);
        } catch (RuntimeException e) {
            // Release the gRPC channel + Arrow buffers if streaming/conversion
            // fails — Trino does not guarantee close() on the throw path.
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
        handle.close();
    }
}
