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
        // Snapshot-reuse freshness window defaults (issue #2356/#2306).
        assertEquals(CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS,
                config.snapshotReuseWindowMillis());
        assertEquals(CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_NANOS,
                config.snapshotReuseWindowNanos());
        // Superseded-window retire-grace defaults (issue #2356 roborev, bounded retention).
        assertEquals(CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_MILLIS,
                config.snapshotRetireGraceMillis());
        assertEquals(CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_NANOS,
                config.snapshotRetireGraceNanos());
    }

    @Test
    void parsesSnapshotRetireGrace() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-retire-grace-ms", "90000");
        CqliteFlightConfig config = CqliteFlightConfig.fromMap(m);
        assertEquals(90_000L, config.snapshotRetireGraceMillis());
        assertEquals(90_000_000_000L, config.snapshotRetireGraceNanos());
    }

    @Test
    void rejectsNegativeSnapshotRetireGrace() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-retire-grace-ms", "-1");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsOversizedSnapshotRetireGraceInsteadOfSilentlyWrapping() {
        // Same overflow guard as the reuse window: a ms value whose ms→ns (* 1_000_000) overflows
        // long must fail fast, not silently wrap.
        Map<String, String> m = base();
        m.put("cqlite.snapshot-retire-grace-ms", "10000000000000");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void parsesSnapshotReuseWindow() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-reuse-window-ms", "7500");
        CqliteFlightConfig config = CqliteFlightConfig.fromMap(m);
        assertEquals(7500L, config.snapshotReuseWindowMillis());
        assertEquals(7_500_000_000L, config.snapshotReuseWindowNanos());
    }

    @Test
    void zeroSnapshotReuseWindowIsAllowedAndDisablesReuse() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-reuse-window-ms", "0");
        assertEquals(0L, CqliteFlightConfig.fromMap(m).snapshotReuseWindowMillis());
    }

    @Test
    void rejectsNegativeSnapshotReuseWindow() {
        Map<String, String> m = base();
        m.put("cqlite.snapshot-reuse-window-ms", "-1");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsOversizedSnapshotReuseWindowInsteadOfSilentlyWrapping() {
        // Blocker 3 (issue #2356 roborev, unchecked arithmetic): a ms value so large that ms→ns
        // (* 1_000_000) overflows long must fail fast at parse time, not silently wrap to a small
        // positive window. Long.MAX_VALUE / 1_000_000 ≈ 9.22e12, so 1e13 ms overflows.
        Map<String, String> m = base();
        m.put("cqlite.snapshot-reuse-window-ms", "10000000000000");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
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
    void rejectsSidecarUriWithoutExplicitPort() {
        // Per-host snapshot addressing derives each replica URI from scheme + port; a portless
        // base (URI.getPort() < 0) would only fail later at first snapshot use (issue #2227 roborev).
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://cassandra");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithoutScheme() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "//cassandra:9043");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithNonHttpScheme() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "ftp://cassandra:9043");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithoutHost() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://:9043");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsSidecarUriWithUserInfo() {
        // Per-host derivation keeps only scheme + port, silently dropping userinfo — a base with
        // credentials would authenticate db0 discovery but not the per-host snapshot PUTs.
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://user:secret@cassandra:9043");
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
        // The offending URI in the message must not leak the credential.
        org.junit.jupiter.api.Assertions.assertFalse(ex.getMessage().contains("secret"));
    }

    @Test
    void rejectsSidecarUriWithPercentEncodedUserInfoWithoutLeaking() {
        // getUserInfo() decodes percent-escapes but URI#toString() renders the raw (encoded)
        // form; redaction must match the RAW userinfo or the encoded credential leaks verbatim
        // into the exception message (roborev job 1567).
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://us%40er:p%23ss@10.0.0.5:9043");
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
        String message = ex.getMessage();
        org.junit.jupiter.api.Assertions.assertFalse(message.contains("p%23ss"),
                "raw-encoded credential must not leak: " + message);
        org.junit.jupiter.api.Assertions.assertFalse(message.contains("p#ss"),
                "decoded credential must not leak: " + message);
        org.junit.jupiter.api.Assertions.assertFalse(message.contains("us%40er"),
                "raw-encoded userinfo must not leak: " + message);
        org.junit.jupiter.api.Assertions.assertFalse(message.contains("us@er"),
                "decoded userinfo must not leak: " + message);
    }

    @Test
    void acceptsIpv4RootSidecarUri() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "http://10.0.0.5:9043");
        assertEquals(java.net.URI.create("http://10.0.0.5:9043"),
                CqliteFlightConfig.fromMap(m).sidecarUri());
    }

    @Test
    void acceptsHttpsRootSidecarUri() {
        Map<String, String> m = base();
        m.put("cqlite.sidecar-uri", "https://cassandra:9043/");
        assertEquals("/", CqliteFlightConfig.fromMap(m).sidecarUri().getPath());
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
