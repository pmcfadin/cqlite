package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;
import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.RingEntry;

/**
 * Connector metadata backed by Sidecar (DDL discovery) and the cqlite-flight
 * server (Arrow schema → Trino column types via {@link ArrowTypeMapper}).
 */
public class CqliteFlightMetadata implements ConnectorMetadata {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final CqliteFlightClient flight;

    public CqliteFlightMetadata(CqliteFlightConfig config, SidecarClient sidecar, CqliteFlightClient flight) {
        this.config = config;
        this.sidecar = sidecar;
        this.flight = flight;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // Direct queries resolve via getTableHandle; SHOW SCHEMAS enumeration is
        // not wired (Sidecar exposes no keyspace-list endpoint we model yet).
        return List.of();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        try {
            sidecar.schema(schemaName);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        String keyspace = tableName.getSchemaName();
        String table = tableName.getTableName();
        String keyspaceSchema;
        try {
            keyspaceSchema = sidecar.schema(keyspace).schema();
        } catch (RuntimeException e) {
            return null; // keyspace not found
        }
        return CreateTableExtractor.extract(keyspaceSchema, keyspace, table)
                .map(ddl -> (ConnectorTableHandle) new CqliteFlightTableHandle(keyspace, table, ddl))
                .orElse(null);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        Schema schema = arrowSchema((CqliteFlightTableHandle) table);
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Field field : schema.getFields()) {
            handles.put(field.getName(),
                    new CqliteFlightColumnHandle(field.getName(), ArrowTypeMapper.toTrino(field)));
        }
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {
        CqliteFlightColumnHandle column = (CqliteFlightColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        Schema schema = arrowSchema(handle);
        List<ColumnMetadata> columns = new ArrayList<>();
        for (Field field : schema.getFields()) {
            columns.add(new ColumnMetadata(field.getName(), ArrowTypeMapper.toTrino(field)));
        }
        return new ConnectorTableMetadata(new SchemaTableName(handle.keyspace(), handle.table()), columns);
    }

    /** Resolve the table's Arrow schema by asking any flight node's GetSchema. */
    private Schema arrowSchema(CqliteFlightTableHandle handle) {
        RingEntry node = sidecar.ring().entries().stream()
                .filter(e -> e.address() != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No Cassandra nodes in the ring"));
        byte[] ticket = FlightTicketJson.build(
                handle.keyspace(), handle.table(), handle.ddl(),
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of());
        return flight.getSchema(node.address(), config.flightPort(), ticket);
    }
}
