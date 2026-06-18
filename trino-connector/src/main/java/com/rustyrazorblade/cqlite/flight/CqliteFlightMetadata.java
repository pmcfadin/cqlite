package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import java.util.List;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;

/**
 * Connector metadata. Phase 4 wires Sidecar-backed discovery; column resolution
 * (via the cqlite-flight server's GetSchema → Arrow → {@link ArrowTypeMapper})
 * and table listing are completed alongside the split manager / page source in
 * Phases 5–6.
 */
public class CqliteFlightMetadata implements ConnectorMetadata {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final TypeManager typeManager;

    public CqliteFlightMetadata(CqliteFlightConfig config, SidecarClient sidecar, TypeManager typeManager) {
        this.config = config;
        this.sidecar = sidecar;
        this.typeManager = typeManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // TODO(phase 5/6): enumerate keyspaces via Sidecar schema.
        return List.of();
    }

    CqliteFlightConfig config() {
        return config;
    }

    SidecarClient sidecar() {
        return sidecar;
    }

    TypeManager typeManager() {
        return typeManager;
    }
}
