package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;

public class CqliteFlightConnector implements Connector {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final TypeManager typeManager;

    public CqliteFlightConnector(CqliteFlightConfig config, SidecarClient sidecar, TypeManager typeManager) {
        this.config = config;
        this.sidecar = sidecar;
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return CqliteFlightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new CqliteFlightMetadata(config, sidecar, typeManager);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new CqliteFlightSplitManager(config, sidecar);
    }

    @Override
    public void shutdown() {
        // No persistent resources to release; the Flight clients are per-scan.
    }

    // getPageSourceProvider (Phase 6) uses the Connector default until it lands.
}
