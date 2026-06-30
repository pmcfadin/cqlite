package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import org.apache.arrow.flight.CallStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CqliteFlightMetadata#aggregateNodeStats} and
 * {@link CqliteFlightMetadata#replicaHosts} — the keyspace-replica-scoped
 * {@code table_stats} aggregation seam (issue #944).
 *
 * <p>Completeness rules:
 * <ul>
 *   <li>A replica host that is UNREACHABLE (transport failure) taints the aggregate
 *       INCOMPLETE ({@code complete=false}) — its peers' partial totals must not be
 *       treated as authoritative.</li>
 *   <li>A replica host that RESPONDS {@code not_found} (it does not host this
 *       keyspace/table) is NOT a failure: it contributes nothing and does NOT taint.
 *       A legitimate not-hosting node must not disable the gate everywhere.</li>
 * </ul>
 *
 * <p>The seam is package-private and static so a failing/not-hosting node can be
 * modeled by a {@code fetch} function that throws — no live Sidecar/Flight client.
 */
class AggregateNodeStatsTest {

    /** A complete per-node response with the given live-row count. */
    private static TableStats ok(long liveRows, long partitions) {
        return new TableStats(liveRows, partitions, 1, true, 0);
    }

    /** The Flight runtime exception a node throws when it does not host the table. */
    private static RuntimeException notFound() {
        return CallStatus.NOT_FOUND.withDescription("table absent").toRuntimeException();
    }

    @Test
    void allNodesSucceedAggregateIsComplete() {
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch =
                address -> address.equals("10.0.0.1") ? ok(100, 10) : ok(200, 20);

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertTrue(agg.complete(), "every node returned → aggregate is complete");
        assertEquals(300, agg.liveRows());
        assertEquals(30, agg.partitionCount());
        assertEquals(0, agg.skippedSstables());
    }

    @Test
    void unreachableReplicaTaintsCompleteness() {
        // Two distinct replica hosts; the second is UNREACHABLE (transport failure).
        // The first host's totals must NOT be reported as authoritative.
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("node 10.0.0.2 unreachable");
            }
            return ok(100, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertFalse(agg.complete(),
                "a single unreachable replica must taint the aggregate to incomplete");
        assertEquals(100, agg.liveRows(), "the reachable node still contributed its totals");
        assertTrue(agg.skippedSstables() >= 1, "the failed node is visible as a skip");
    }

    @Test
    void firstReplicaUnreachableStillTaintsWhenPeerSucceeds() {
        // Order independence: the unreachable host is queried FIRST. The AND-of-complete
        // in plus() must still make the whole aggregate incomplete.
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.1")) {
                throw new RuntimeException("node 10.0.0.1 unreachable");
            }
            return ok(200, 20);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertFalse(agg.complete(), "fetch-failure order must not affect completeness");
        assertEquals(200, agg.liveRows());
    }

    @Test
    void notFoundResponseDoesNotTaintCompleteness() {
        // A replica that legitimately does NOT host the table responds NOT_FOUND. It
        // contributes nothing and must NOT taint completeness — the aggregate stays
        // complete and the hosting node's totals are authoritative (issue #944).
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw notFound();
            }
            return ok(100, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertTrue(agg.complete(),
                "a not_found (not-hosting) replica must NOT taint completeness");
        assertEquals(100, agg.liveRows(), "the hosting node's totals are authoritative");
        assertEquals(10, agg.partitionCount());
        assertEquals(0, agg.skippedSstables(),
                "a not-hosting node is not counted as a skip");
    }

    @Test
    void wrappedNotFoundIsClassifiedAsNotHosting() {
        // The client may wrap the Flight NOT_FOUND in another runtime exception; the
        // classifier walks the cause chain, so a wrapped NOT_FOUND still does not taint.
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new IllegalStateException("table_stats to 10.0.0.2 failed", notFound());
            }
            return ok(100, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertTrue(agg.complete(), "a wrapped NOT_FOUND must be classified as not-hosting");
        assertEquals(100, agg.liveRows());
    }

    @Test
    void mixedNotFoundAndUnreachableStillTaints() {
        // not_found does not taint, but a separate genuinely unreachable replica still
        // does: the invariant for a replica that SHOULD have data but is unreachable
        // is preserved alongside the new not-hosting carve-out.
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Function<String, TableStats> fetch = address -> switch (address) {
            case "10.0.0.1" -> ok(100, 10);
            case "10.0.0.2" -> throw notFound();          // not hosting → no taint
            case "10.0.0.3" -> throw new RuntimeException("unreachable"); // taints
            default -> throw new AssertionError(address);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertFalse(agg.complete(),
                "an unreachable replica still taints even when a peer responded not_found");
        assertEquals(100, agg.liveRows());
        assertTrue(agg.skippedSstables() >= 1, "the unreachable node is visible as a skip");
    }

    @Test
    void duplicateAddressesAreQueriedOnce() {
        // The host set can list the same address more than once; it is queried once and
        // a success stays complete.
        int[] calls = {0};
        List<String> hosts = java.util.Arrays.asList("10.0.0.1", "10.0.0.1", null);
        Function<String, TableStats> fetch = address -> {
            calls[0]++;
            return ok(50, 5);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertEquals(1, calls[0], "duplicate + null addresses queried at most once");
        assertTrue(agg.complete());
        assertEquals(50, agg.liveRows());
    }

    @Test
    void replicaHostsScopedToKeyspaceReplicasOnly() {
        // replicaHosts must derive the target set from the keyspace's read replicas
        // (ports stripped, de-duplicated across ranges/DCs) — NOT from the whole ring.
        // A non-hosting ring node never appears here, so it is never queried.
        TokenRangeReplicasResponse replicas = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        new ReplicaInfo("-100", "0", Map.of(
                                "dc1", List.of("10.0.0.1:7000", "10.0.0.2:7000"))),
                        new ReplicaInfo("0", "100", Map.of(
                                "dc1", List.of("10.0.0.2:7000"),
                                "dc2", List.of("10.0.0.3:7000")))));

        Set<String> hosts = CqliteFlightMetadata.replicaHosts(replicas, "dc1");

        assertEquals(Set.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), hosts,
                "only distinct keyspace-replica hosts (ports stripped) are targeted");
    }

    @Test
    void replicaHostsHandlesNullsDefensively() {
        assertTrue(CqliteFlightMetadata.replicaHosts(null, "dc1").isEmpty());
        assertTrue(CqliteFlightMetadata.replicaHosts(
                new TokenRangeReplicasResponse(List.of(), null), "dc1").isEmpty());
    }

    @Test
    void incompleteAggregateYieldsEmptyGroupRatio() {
        // End-to-end with the gate's pure function: an incomplete aggregate (from an
        // unreachable node) gives NO group-ratio estimate, so AUTOMATIC pushdown
        // declines to use a biased ratio (issue #944, #28).
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("down");
            }
            return ok(2000, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);
        assertFalse(agg.complete());

        String ddl = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))";
        OptionalDouble ratio =
                CqliteFlightMetadata.estimateGroupRatio(ddl, List.of("pk"), agg);
        assertTrue(ratio.isEmpty(),
                "incomplete cross-ring stats must produce no group-ratio estimate");
    }

    @Test
    void unreachableReplicaTaintsAggregateIncomplete() {
        // A down replica taints the cross-ring aggregate INCOMPLETE so its partial
        // peers' totals are not treated as authoritative (issue #944). This feeds the
        // RF-invariant group-ratio gate; the absolute optimizer row count is reported
        // unknown regardless (see CqliteFlightTableStatisticsTest).
        List<String> hosts = List.of("10.0.0.1", "10.0.0.2");
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("down");
            }
            return ok(2000, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(hosts, fetch);

        assertFalse(agg.complete(),
                "an unreachable replica must taint the cross-ring aggregate incomplete");
    }
}
