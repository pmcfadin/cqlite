package in.mcfad.cqlite.flight;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

/** Trino plugin entry point (registered via ServiceLoader). */
public class CqliteFlightPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new CqliteFlightConnectorFactory());
    }
}
