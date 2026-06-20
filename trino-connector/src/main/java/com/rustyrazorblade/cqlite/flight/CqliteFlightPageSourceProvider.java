package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;

public class CqliteFlightPageSourceProvider implements ConnectorPageSourceProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CqliteFlightClient client;

    public CqliteFlightPageSourceProvider(CqliteFlightClient client) {
        this.client = client;
    }

    private static JsonNode parseFilter(Optional<String> filterJson) {
        if (filterJson.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readTree(filterJson.get());
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Invalid pushed-down filter JSON", e);
        }
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        CqliteFlightSplit flightSplit = (CqliteFlightSplit) split;
        CqliteFlightTableHandle tableHandle = (CqliteFlightTableHandle) table;
        List<CqliteFlightColumnHandle> projected = columns.stream()
                .map(CqliteFlightColumnHandle.class::cast)
                .toList();
        List<String> names = projected.stream().map(CqliteFlightColumnHandle::name).toList();

        // Pushed-down predicate tree (if any) lives on the table handle; parse it
        // back to a JsonNode so it nests into the ticket rather than being escaped.
        JsonNode filter = parseFilter(tableHandle.filterJson());

        byte[] ticket = FlightTicketJson.build(
                flightSplit.keyspace(),
                flightSplit.table(),
                flightSplit.ddl(),
                Optional.empty(), // live data dir (no snapshot) for the first E2E
                Optional.of(flightSplit.tokenStart()),
                Optional.of(flightSplit.tokenEnd()),
                flightSplit.wraparound(),
                names.isEmpty() ? Optional.empty() : Optional.of(names),
                List.of(), // legacy flat predicates unused; tree carried in filter
                filter);

        return new CqliteFlightPageSource(client, flightSplit, projected, ticket);
    }
}
