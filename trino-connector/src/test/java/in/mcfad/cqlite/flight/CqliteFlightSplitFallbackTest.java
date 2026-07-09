package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ordered replica fallback list on a split (issue #2241): {@code host} is the primary and
 * {@code fallbackHosts} the availability alternates the page source may fail over to. Splits
 * serialize between coordinator and workers, so the list must survive a JSON round-trip.
 */
class CqliteFlightSplitFallbackTest {
    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY)";

    // Mirror Trino's split codec (airlift ObjectMapperProvider): serialize by FIELDS, not getters,
    // so a ConnectorSplit round-trips its record components without invoking the interface's default
    // methods (e.g. getRetainedSizeInBytes); Optional<>-aware via the jdk8 module (issue #2241).
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static CqliteFlightSplit split(List<String> fallbacks) {
        return new CqliteFlightSplit(
                "ks", "t", DDL, "10.0.0.1", 8815, -100L, 100L, false, Optional.of("cqlite-q1"), fallbacks);
    }

    @Test
    void replicaHostsArePrimaryThenFallbacksInOrder() {
        CqliteFlightSplit s = split(List.of("10.0.0.2", "10.0.0.3"));
        assertEquals(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), s.replicaHosts());
    }

    @Test
    void nineArgConstructorHasNoFallbacks() {
        CqliteFlightSplit s = new CqliteFlightSplit(
                "ks", "t", DDL, "10.0.0.1", 8815, -100L, 100L, false, Optional.empty());
        assertEquals(List.of(), s.fallbackHosts());
        assertEquals(List.of("10.0.0.1"), s.replicaHosts());
    }

    @Test
    void getAddressesListsEveryReplicaPrimaryFirst() {
        CqliteFlightSplit s = split(List.of("10.0.0.2", "10.0.0.3"));
        assertEquals(
                List.of(
                        HostAddress.fromParts("10.0.0.1", 8815),
                        HostAddress.fromParts("10.0.0.2", 8815),
                        HostAddress.fromParts("10.0.0.3", 8815)),
                s.getAddresses());
    }

    @Test
    void fallbackListSurvivesJsonRoundTrip() throws Exception {
        CqliteFlightSplit original = split(List.of("10.0.0.2", "10.0.0.3"));

        byte[] json = MAPPER.writeValueAsBytes(original);
        CqliteFlightSplit restored = MAPPER.readValue(json, CqliteFlightSplit.class);

        assertEquals(original, restored, "whole split round-trips");
        assertEquals(List.of("10.0.0.2", "10.0.0.3"), restored.fallbackHosts());
        assertEquals(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), restored.replicaHosts());
        assertEquals(Optional.of("cqlite-q1"), restored.snapshot());
    }

    @Test
    void emptyFallbackListSurvivesJsonRoundTrip() throws Exception {
        CqliteFlightSplit original = new CqliteFlightSplit(
                "ks", "t", DDL, "10.0.0.1", 8815, 0L, 50L, false, Optional.empty(), List.of());

        byte[] json = MAPPER.writeValueAsBytes(original);
        CqliteFlightSplit restored = MAPPER.readValue(json, CqliteFlightSplit.class);

        assertEquals(original, restored);
        assertTrue(restored.fallbackHosts().isEmpty());
    }
}
