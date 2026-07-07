package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wiring evidence for issue #2105 on the aggregate finalize path (N1 review gap): the
 * finalize split's page source (see {@link CqliteFlightAggregatePageSource#getNextSourcePage})
 * builds one DoGet ticket per token range, and that ticket must carry the range's
 * {@link CqliteFlightSplit#snapshot()} through unchanged. {@link ReadModeWiringTest} already
 * covers this for the non-aggregated scan path; this covers the aggregate finalize path via
 * {@link CqliteFlightAggregatePageSource#buildRangeTicket}, the exact package-private method
 * {@code getNextSourcePage} calls in its per-range fan-out loop.
 *
 * <p>If that call site regressed to {@code Optional.empty()} instead of {@code range.snapshot()},
 * {@link #snapshotModeNamesSnapshotInTicket()} would fail (ticket's {@code snapshot} would be
 * null instead of {@code "cqlite-q1"}).
 */
class CqliteFlightAggregatePageSourceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY)";

    private static CqliteFlightSplit rangeWithSnapshot(Optional<String> snapshot) {
        return new CqliteFlightSplit("ks", "t", DDL, "10.0.0.2", 8815, -100L, 100L, false, snapshot);
    }

    @Test
    void snapshotModeNamesSnapshotInTicket() throws Exception {
        CqliteFlightSplit range = rangeWithSnapshot(Optional.of("cqlite-q1"));

        byte[] ticket = CqliteFlightAggregatePageSource.buildRangeTicket(range, null, null);
        JsonNode node = MAPPER.readTree(ticket);

        assertEquals("cqlite-q1", node.get("snapshot").asText());
    }

    @Test
    void liveModeLeavesSnapshotNullInTicket() throws Exception {
        CqliteFlightSplit range = rangeWithSnapshot(Optional.empty());

        byte[] ticket = CqliteFlightAggregatePageSource.buildRangeTicket(range, null, null);
        JsonNode node = MAPPER.readTree(ticket);

        assertTrue(node.get("snapshot").isNull());
    }
}
