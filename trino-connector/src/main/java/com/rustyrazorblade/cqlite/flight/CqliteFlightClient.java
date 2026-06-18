package com.rustyrazorblade.cqlite.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Thin wrapper over the Arrow Flight Java client for talking to a cqlite-flight
 * endpoint: fetch a table's Arrow schema, and open a record-batch stream.
 */
public final class CqliteFlightClient {
    private final BufferAllocator allocator;

    public CqliteFlightClient(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /** Resolve a table's Arrow schema from a flight endpoint (GetSchema). */
    public Schema getSchema(String host, int port, byte[] ticket) {
        try (FlightClient client = connect(host, port)) {
            return client.getSchema(FlightDescriptor.command(ticket)).getSchema();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("GetSchema to " + host + ":" + port + " failed", e);
        }
    }

    /** Open a DoGet stream; the caller must close the returned handle. */
    public StreamHandle openStream(String host, int port, byte[] ticket) {
        FlightClient client = connect(host, port);
        try {
            FlightStream stream = client.getStream(new Ticket(ticket));
            return new StreamHandle(client, stream);
        } catch (RuntimeException e) {
            // Don't leak the gRPC channel if opening the stream fails.
            try {
                client.close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private FlightClient connect(String host, int port) {
        return FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build();
    }

    /** An open Flight stream paired with its client; close releases both. */
    public static final class StreamHandle implements AutoCloseable {
        private final FlightClient client;
        private final FlightStream stream;

        StreamHandle(FlightClient client, FlightStream stream) {
            this.client = client;
            this.stream = stream;
        }

        public FlightStream stream() {
            return stream;
        }

        @Override
        public void close() {
            try {
                stream.close();
            } catch (Exception e) {
                // best-effort
            }
            try {
                client.close();
            } catch (Exception e) {
                // best-effort
            }
        }
    }
}
