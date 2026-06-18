package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;

public class CqliteFlightConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "cqlite_flight";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        CqliteFlightConfig cfg = CqliteFlightConfig.fromMap(config);
        SidecarClient sidecar = new SidecarClient(cfg.sidecarUri());
        return new CqliteFlightConnector(cfg, sidecar);
    }
}
