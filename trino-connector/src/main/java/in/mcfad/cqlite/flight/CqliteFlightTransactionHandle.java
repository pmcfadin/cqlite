package in.mcfad.cqlite.flight;

import io.trino.spi.connector.ConnectorTransactionHandle;

/** This connector is read-only and stateless, so a single handle suffices. */
public enum CqliteFlightTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}
