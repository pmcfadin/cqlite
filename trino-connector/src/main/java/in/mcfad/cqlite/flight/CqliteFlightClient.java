package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper over the Arrow Flight Java client for talking to a cqlite-flight
 * endpoint: fetch a table's Arrow schema, and open a record-batch stream.
 */
public final class CqliteFlightClient {
    /** Wire name of the per-table statistics action (matches the Rust server). */
    static final String TABLE_STATS_ACTION = "table_stats";
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /**
     * Fetch per-table aggregate statistics from a flight endpoint via the
     * {@code table_stats} DoAction (issue #944). {@code requestBody} is the
     * JSON-encoded {@code TableStatsRequest} (keyspace/table/snapshot). The server
     * returns exactly one {@link Result} whose body is the JSON
     * {@link TableStats}.
     *
     * <p>The call is BOUNDED by {@code timeoutMillis} (a Flight {@link CallOptions#timeout
     * deadline}, issue #944): this DoAction runs during query PLANNING, so a slow or
     * half-open endpoint must not stall the planner. When the deadline fires the gRPC
     * call surfaces a {@link org.apache.arrow.flight.FlightRuntimeException} with status
     * {@code TIMED_OUT}/{@code DEADLINE_EXCEEDED}, which propagates to the caller's
     * fetch-failure path (treated as UNAVAILABLE stats → "no estimate") rather than a
     * planning error. A non-positive {@code timeoutMillis} disables the deadline.
     */
    public TableStats tableStats(String host, int port, byte[] requestBody, long timeoutMillis) {
        CallOption[] options = timeoutMillis > 0
                ? new CallOption[] {CallOptions.timeout(timeoutMillis, TimeUnit.MILLISECONDS)}
                : new CallOption[0];
        try (FlightClient client = connect(host, port)) {
            Iterator<Result> results =
                    client.doAction(new Action(TABLE_STATS_ACTION, requestBody), options);
            if (!results.hasNext()) {
                throw new IllegalStateException(
                        "table_stats to " + host + ":" + port + " returned no result");
            }
            byte[] body = results.next().getBody();
            // Drain any (unexpected) extra results so the gRPC call completes cleanly.
            while (results.hasNext()) {
                results.next();
            }
            return MAPPER.readValue(body, TableStats.class);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("table_stats to " + host + ":" + port + " failed", e);
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
