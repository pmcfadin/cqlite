package in.mcfad.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Parsing of the aggregation-pushdown gate properties (issue #893) and existing
 * connector config keys.
 */
class CqliteFlightConfigTest {

    private static Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put("cqlite.sidecar-uri", "http://cassandra:9043");
        return m;
    }

    @Test
    void defaultsWhenGateUnset() {
        CqliteFlightConfig config = CqliteFlightConfig.fromMap(base());
        assertEquals(GroupByPushdownPolicy.AUTOMATIC, config.groupByPushdown());
        assertEquals(CqliteFlightConfig.DEFAULT_MAX_GROUP_RATIO, config.maxGroupRatio());
        assertEquals(CqliteFlightConfig.DEFAULT_FLIGHT_PORT, config.flightPort());
        assertEquals(CqliteFlightConfig.DEFAULT_TABLE_STATS_TIMEOUT_MILLIS,
                config.tableStatsTimeoutMillis());
        // read-mode defaults to snapshot with the backstop TTL (issue #2105).
        assertEquals(ReadMode.SNAPSHOT, config.readMode());
        assertEquals(java.util.Optional.of(CqliteFlightConfig.DEFAULT_SNAPSHOT_TTL),
                config.snapshotTtl());
    }

    @Test
    void parsesReadModeLive() {
        Map<String, String> m = base();
        m.put("cqlite.read-mode", "LIVE");
        assertEquals(ReadMode.LIVE, CqliteFlightConfig.fromMap(m).readMode());
    }

    @Test
    void parsesReadModeSnapshotCaseInsensitively() {
        Map<String, String> m = base();
        m.put("cqlite.read-mode", "SnApShOt");
        assertEquals(ReadMode.SNAPSHOT, CqliteFlightConfig.fromMap(m).readMode());
    }

    @Test
    void rejectsInvalidReadMode() {
        Map<String, String> m = base();
        m.put("cqlite.read-mode", "sometimes");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void parsesExplicitSnapshotTtl() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-ttl", "2d");
        assertEquals(java.util.Optional.of("2d"), CqliteFlightConfig.fromMap(m).snapshotTtl());
    }

    @Test
    void blankSnapshotTtlDisablesIt() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-ttl", "   ");
        assertEquals(java.util.Optional.empty(), CqliteFlightConfig.fromMap(m).snapshotTtl());
    }

    @Test
    void parsesTableStatsTimeout() {
        Map<String, String> m = base();
        m.put("cqlite.table-stats-timeout-ms", "1500");
        assertEquals(1500L, CqliteFlightConfig.fromMap(m).tableStatsTimeoutMillis());
    }

    @Test
    void rejectsNonPositiveTableStatsTimeout() {
        Map<String, String> m = base();
        m.put("cqlite.table-stats-timeout-ms", "0");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));

        Map<String, String> n = base();
        n.put("cqlite.table-stats-timeout-ms", "-5");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(n));
    }

    @Test
    void parsesPolicyCaseInsensitively() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-group-by", "NeVeR");
        assertEquals(GroupByPushdownPolicy.NEVER, CqliteFlightConfig.fromMap(m).groupByPushdown());
    }

    @Test
    void parsesMaxGroupRatio() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-max-group-ratio", "0.25");
        assertEquals(0.25, CqliteFlightConfig.fromMap(m).maxGroupRatio());
    }

    @Test
    void rejectsInvalidPolicy() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-group-by", "sometimes");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithNonRootPath() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "https://proxy.example/sidecar");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithQuery() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://cassandra:9043?token=abc");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithFragment() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://cassandra:9043#frag");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void acceptsRootPathSidecarUri() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://cassandra:9043/");
        assertEquals("/", CqliteFlightConfig.fromMap(m).sidecarUri().getPath());
    }

    @Test
    void acceptsEmptyPathSidecarUri() {
        // base() uses http://cassandra:9043 (no trailing slash) — the existing default must stay valid.
        assertEquals(java.net.URI.create("http://cassandra:9043"),
                CqliteFlightConfig.fromMap(base()).sidecarUri());
    }

    @Test
    void rejectsOutOfRangeRatio() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-max-group-ratio", "1.5");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));

        Map<String, String> z = base();
        z.put("cqlite.aggregation-pushdown-max-group-ratio", "0");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(z));
    }
}
