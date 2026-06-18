package com.rustyrazorblade.cqlite.flight;

import java.net.URI;
import java.util.Map;

/**
 * Connector configuration from the catalog properties file.
 *
 * <pre>
 * connector.name=cqlite_flight
 * cqlite.sidecar-uri=http://cassandra:9043
 * cqlite.flight-port=8815
 * cqlite.local-datacenter=dc1
 * </pre>
 */
public record CqliteFlightConfig(URI sidecarUri, int flightPort, String localDatacenter) {

    public static final int DEFAULT_FLIGHT_PORT = 8815;

    public static CqliteFlightConfig fromMap(Map<String, String> config) {
        String sidecar = require(config, "cqlite.sidecar-uri");
        int port = config.containsKey("cqlite.flight-port")
                ? Integer.parseInt(config.get("cqlite.flight-port"))
                : DEFAULT_FLIGHT_PORT;
        String dc = config.get("cqlite.local-datacenter");
        return new CqliteFlightConfig(URI.create(sidecar), port, dc);
    }

    private static String require(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config property: " + key);
        }
        return value;
    }
}
