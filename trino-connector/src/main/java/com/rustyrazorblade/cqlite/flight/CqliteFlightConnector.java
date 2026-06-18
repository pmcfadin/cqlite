package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;

public class CqliteFlightConnector implements Connector {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final BufferAllocator allocator;
    private final CqliteFlightClient flight;

    public CqliteFlightConnector(CqliteFlightConfig config, SidecarClient sidecar) {
        this.config = config;
        this.sidecar = sidecar;
        this.allocator = new RootAllocator();
        this.flight = new CqliteFlightClient(allocator);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return CqliteFlightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new CqliteFlightMetadata(config, sidecar, flight);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new CqliteFlightSplitManager(config, sidecar);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new CqliteFlightPageSourceProvider(flight);
    }

    @Override
    public void shutdown() {
        allocator.close();
    }
}
