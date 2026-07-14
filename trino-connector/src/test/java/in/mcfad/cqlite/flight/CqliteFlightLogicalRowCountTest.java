package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.flight.CallStatus;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the RF-correct LOGICAL row count reported by
 * {@link CqliteFlightMetadata#getTableStatistics} on a non-aggregated handle
 * (issue #1336), driven THROUGH the public {@code ConnectorMetadata} surface with
 * real {@code SidecarModels} token-range parsing and real {@link TableStats} JSON
 * decode (wiring-evidence doctrine) — not by unit-testing a private helper alone.
 *
 * <p>The stub overrides only the two package-private I/O seams
 * ({@code tokenRangeReplicas} and {@code fetchTableStats}); the derivation under test
 * (uniform-replica-count division, completeness gate, fail-closed posture, memoization)
 * runs unchanged.
 */
class CqliteFlightLogicalRowCountTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A complete per-host {@code table_stats} JSON with the given live-row count. */
    private static String statsJson(long liveRows, long partitions, boolean complete) {
        return "{\"live_rows\":" + liveRows + ",\"partition_count\":" + partitions
                + ",\"sstable_count\":1,\"complete\":" + complete + ",\"skipped_sstables\":0}";
    }

    /** RF=3 over one keyspace: every range has the same three scoped replicas. */
    private static final String RF3_REPLICAS = """
            {"writeReplicas":[],"readReplicas":[
              {"start":"-100","end":"100","replicasByDatacenter":{
                "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"]}}
            ]}""";

    private static CqliteFlightConfig config(String localDatacenter) {
        return new CqliteFlightConfig(
                URI.create("http://sidecar:9043"), 8815, localDatacenter,
                GroupByPushdownPolicy.AUTOMATIC, 0.5, 3000,
                ReadMode.SNAPSHOT, java.util.Optional.of("6h"),
                CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS);
    }

    private static CqliteFlightTableHandle plainHandle() {
        return new CqliteFlightTableHandle("ks", "t", "ddl");
    }

    /**
     * Metadata stub whose only overrides are the two I/O seams. It records how many
     * times each seam is hit (memoization evidence) and decodes REAL per-host
     * {@code table_stats} JSON via {@link TableStats} + real {@code replicaHosts} +
     * real {@code aggregateNodeStats}.
     */
    private static final class StubMetadata extends CqliteFlightMetadata {
        private final String localDatacenter;
        private final String replicasJson;
        private final Map<String, String> perHostStatsJson;
        private final RuntimeException replicasError;
        int replicaFetches = 0;
        int statsFetches = 0;

        StubMetadata(String localDatacenter, String replicasJson,
                Map<String, String> perHostStatsJson, RuntimeException replicasError) {
            super(config(localDatacenter), null, null);
            this.localDatacenter = localDatacenter;
            this.replicasJson = replicasJson;
            this.perHostStatsJson = perHostStatsJson;
            this.replicasError = replicasError;
        }

        @Override
        TokenRangeReplicasResponse tokenRangeReplicas(String keyspace) {
            replicaFetches++;
            if (replicasError != null) {
                throw replicasError;
            }
            return SidecarClient.parseTokenRangeReplicas(replicasJson);
        }

        @Override
        TableStats fetchTableStats(CqliteFlightTableHandle handle, TokenRangeReplicasResponse replicas) {
            statsFetches++;
            Set<String> hosts = CqliteFlightMetadata.replicaHosts(replicas, localDatacenter);
            return CqliteFlightMetadata.aggregateNodeStats(hosts, address -> {
                String json = perHostStatsJson.get(address);
                if (json == null) {
                    // Host not modeled → treat as not-hosting (Flight NOT_FOUND).
                    throw CallStatus.NOT_FOUND.withDescription("no stats for " + address)
                            .toRuntimeException();
                }
                if (UNREACHABLE.equals(json)) {
                    // Transport failure (unreachable) → aggregate folds UNAVAILABLE.
                    throw new RuntimeException(address + " unreachable");
                }
                try {
                    return MAPPER.readValue(json, TableStats.class);
                } catch (Exception e) {
                    throw new IllegalStateException("bad stats json", e);
                }
            });
        }
    }

    /** Sentinel marking a host as unreachable (a transport failure, not not-hosting). */
    private static final String UNREACHABLE = "__unreachable__";

    @Test
    void rf3ThreeHostsEachTwoHundredRowsReportsLogicalTwoHundred() {
        // The issue's acceptance criterion: physical sum is 600 (3 × 200) but the
        // reported optimizer ROW_COUNT is the LOGICAL cardinality 200 = 600 / R(=3).
        Map<String, String> perHost = Map.of(
                "10.0.0.1", statsJson(200, 10, true),
                "10.0.0.2", statsJson(200, 10, true),
                "10.0.0.3", statsJson(200, 10, true));
        StubMetadata metadata = new StubMetadata("dc1", RF3_REPLICAS, perHost, null);

        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());

        assertEquals(200.0, stats.getRowCount().getValue(), 0.0,
                "RF=3, three hosts × 200 rows → logical ROW_COUNT 200 (not the physical sum 600)");
    }

    @Test
    void onePlanningPassFetchesEachSeamAtMostOnce() {
        // Repeated getTableStatistics calls for the same (keyspace, table) reuse the
        // memoized result: exactly one tokenRangeReplicas fetch and one table_stats
        // fetch across the whole planning pass (issue #1336, scenario 3).
        Map<String, String> perHost = Map.of(
                "10.0.0.1", statsJson(200, 10, true),
                "10.0.0.2", statsJson(200, 10, true),
                "10.0.0.3", statsJson(200, 10, true));
        StubMetadata metadata = new StubMetadata("dc1", RF3_REPLICAS, perHost, null);

        CqliteFlightTableHandle handle = plainHandle();
        TableStatistics first = metadata.getTableStatistics(null, handle);
        TableStatistics second = metadata.getTableStatistics(null, handle);
        TableStatistics third = metadata.getTableStatistics(null, handle);

        assertEquals(200.0, first.getRowCount().getValue(), 0.0);
        assertEquals(first.getRowCount().getValue(), second.getRowCount().getValue(), 0.0);
        assertEquals(first.getRowCount().getValue(), third.getRowCount().getValue(), 0.0);
        assertEquals(1, metadata.replicaFetches, "tokenRangeReplicas fetched at most once per table");
        assertEquals(1, metadata.statsFetches, "table_stats fetched at most once per table");
    }

    @Test
    void nonUniformPerRangeReplicaCountsFailClosed() {
        // Range 1 has 3 replicas, range 2 has 2 → non-uniform → empty(); the divisor
        // is ungrounded, so table_stats is not even fetched.
        String replicas = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"0","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"]}},
                  {"start":"0","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.2:7000","10.0.0.3:7000"]}}
                ]}""";
        StubMetadata metadata = new StubMetadata("dc1", replicas, Map.of(), null);

        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());

        assertEquals(TableStatistics.empty(), stats,
                "non-uniform per-range replica counts fail closed to empty");
        assertTrue(stats.getRowCount().isUnknown());
        assertEquals(0, metadata.statsFetches,
                "an ungrounded divisor short-circuits before the table_stats fetch");
    }

    @Test
    void incompleteStatsFailClosed() {
        // One replica reports complete=false → the aggregate is incomplete → empty().
        Map<String, String> perHost = Map.of(
                "10.0.0.1", statsJson(200, 10, true),
                "10.0.0.2", statsJson(200, 10, false),
                "10.0.0.3", statsJson(200, 10, true));
        StubMetadata metadata = new StubMetadata("dc1", RF3_REPLICAS, perHost, null);

        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());

        assertEquals(TableStatistics.empty(), stats,
                "incomplete table_stats (complete=false) fail closed to empty");
        assertTrue(stats.getRowCount().isUnknown());
    }

    @Test
    void unreachableReplicaFailsClosed() {
        // One of the three replicas is unreachable (transport failure, NOT a not-hosting
        // NOT_FOUND): aggregateNodeStats folds UNAVAILABLE, tainting the aggregate to
        // complete=false, so the derivation fails closed to empty().
        Map<String, String> perHost = Map.of(
                "10.0.0.1", statsJson(200, 10, true),
                "10.0.0.2", statsJson(200, 10, true),
                "10.0.0.3", UNREACHABLE);
        StubMetadata metadata = new StubMetadata("dc1", RF3_REPLICAS, perHost, null);

        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());

        assertEquals(TableStatistics.empty(), stats,
                "an unreachable replica taints completeness → fail closed to empty");
    }

    @Test
    void sidecarErrorFailsClosedWithoutEscaping() {
        // Any Sidecar/Flight error during the derivation degrades to empty() and never
        // escapes into query planning (issue #1336).
        StubMetadata metadata = new StubMetadata("dc1", RF3_REPLICAS, Map.of(),
                new RuntimeException("sidecar down"));

        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());

        assertEquals(TableStatistics.empty(), stats,
                "a Sidecar failure degrades to empty(), not a planning exception");
        assertTrue(stats.getRowCount().isUnknown());
    }
}
