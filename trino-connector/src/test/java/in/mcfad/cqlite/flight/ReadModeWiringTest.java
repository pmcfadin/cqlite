package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wiring evidence for issue #2105: the {@code cqlite.read-mode} config actually flows
 * config → planning ({@link SnapshotManager} + {@link CqliteFlightSplitManager}) → ticket
 * bytes, asserted end-to-end by inspecting the ticket JSON the page source would send in
 * each mode. This is the connector's read side of the ticket's {@code snapshot} field
 * (the server already reads a Sidecar snapshot dir when the ticket names one).
 */
class ReadModeWiringTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final CqliteFlightTableHandle TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY)");

    private static final TokenRangeReplicasResponse REPLICAS = new TokenRangeReplicasResponse(
            List.of(),
            List.of(new ReplicaInfo("-100", "100", Map.of("dc1", List.of("10.0.0.2:7000")))));

    /** A per-host SnapshotApi factory whose creates/clears succeed (no-op). */
    private static HostSnapshotApis noopSidecar() {
        return host -> new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {}
            @Override
            public void clearSnapshot(String k, String t, String n) {}
        };
    }

    /** Build the exact ticket the non-aggregated page source emits for a split. */
    private static JsonNode ticketFor(CqliteFlightSplit split) throws Exception {
        byte[] ticket = FlightTicketJson.build(
                split.keyspace(), split.table(), split.ddl(),
                split.snapshot(),
                Optional.of(split.tokenStart()), Optional.of(split.tokenEnd()), split.wraparound(),
                Optional.empty(), List.of(), null, null);
        return MAPPER.readTree(ticket);
    }

    @Test
    void snapshotModeNamesSnapshotInTicket() throws Exception {
        SnapshotManager mgr = new SnapshotManager(noopSidecar(), ReadMode.SNAPSHOT, Optional.of("6h"));
        Optional<String> snapshot = mgr.snapshotFor("q1", "ks", "t", List.of("10.0.0.2"));

        List<CqliteFlightSplit> splits =
                CqliteFlightSplitManager.buildSplits(TABLE, REPLICAS, "dc1", 8815, snapshot);

        assertEquals(Optional.of("cqlite-q1"), splits.get(0).snapshot());
        assertEquals("cqlite-q1", ticketFor(splits.get(0)).get("snapshot").asText());
    }

    @Test
    void liveModeLeavesSnapshotNullInTicket() throws Exception {
        SnapshotManager mgr = new SnapshotManager(noopSidecar(), ReadMode.LIVE, Optional.of("6h"));
        Optional<String> snapshot = mgr.snapshotFor("q1", "ks", "t", List.of("10.0.0.2"));

        List<CqliteFlightSplit> splits =
                CqliteFlightSplitManager.buildSplits(TABLE, REPLICAS, "dc1", 8815, snapshot);

        assertEquals(Optional.empty(), splits.get(0).snapshot());
        assertTrue(ticketFor(splits.get(0)).get("snapshot").isNull());
    }
}
