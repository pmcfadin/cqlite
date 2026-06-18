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

import java.util.List;
import java.util.Optional;

public class CqliteFlightPageSourceProvider implements ConnectorPageSourceProvider {
    private final CqliteFlightClient client;

    public CqliteFlightPageSourceProvider(CqliteFlightClient client) {
        this.client = client;
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
        List<CqliteFlightColumnHandle> projected = columns.stream()
                .map(CqliteFlightColumnHandle.class::cast)
                .toList();
        List<String> names = projected.stream().map(CqliteFlightColumnHandle::name).toList();

        byte[] ticket = FlightTicketJson.build(
                flightSplit.keyspace(),
                flightSplit.table(),
                flightSplit.ddl(),
                Optional.empty(), // live data dir (no snapshot) for the first E2E
                Optional.of(flightSplit.tokenStart()),
                Optional.of(flightSplit.tokenEnd()),
                flightSplit.wraparound(),
                names.isEmpty() ? Optional.empty() : Optional.of(names),
                List.of()); // predicate pushdown wired separately

        return new CqliteFlightPageSource(client, flightSplit, projected, ticket);
    }
}
