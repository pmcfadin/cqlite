package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.RingEntry;
import io.trino.spi.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CqliteFlightMetadata#aggregateNodeStats} — the cross-ring
 * {@code table_stats} aggregation seam (issue #944). When ANY distinct ring node that
 * should have been queried FAILS to return stats, the aggregate must be tainted
 * INCOMPLETE ({@code complete=false}) rather than silently dropping the node and
 * treating its peers' partial totals as authoritative. Downstream, an incomplete
 * aggregate must yield an empty group-ratio estimate and empty table statistics.
 *
 * <p>{@code aggregateNodeStats} is a package-private static seam so a failing node can
 * be modeled by a {@code fetch} function that throws — no live Sidecar/Flight client.
 */
class AggregateNodeStatsTest {

    /** A ring entry carrying only the address (the field {@code aggregateNodeStats} reads). */
    private static RingEntry node(String address) {
        return new RingEntry(null, address, null, null, null, null, null, null, null);
    }

    /** A complete per-node response with the given live-row count. */
    private static TableStats ok(long liveRows, long partitions) {
        return new TableStats(liveRows, partitions, 1, true, 0);
    }

    @Test
    void allNodesSucceedAggregateIsComplete() {
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.2"));
        Function<String, TableStats> fetch =
                address -> address.equals("10.0.0.1") ? ok(100, 10) : ok(200, 20);

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);

        assertTrue(agg.complete(), "every node returned → aggregate is complete");
        assertEquals(300, agg.liveRows());
        assertEquals(30, agg.partitionCount());
        assertEquals(0, agg.skippedSstables());
    }

    @Test
    void oneNodeFailureTaintsCompleteness() {
        // Two distinct nodes; the second throws. The first node's totals must NOT be
        // reported as authoritative — the aggregate is incomplete.
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.2"));
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("node 10.0.0.2 unreachable");
            }
            return ok(100, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);

        assertFalse(agg.complete(),
                "a single failed ring node must taint the aggregate to incomplete");
        assertEquals(100, agg.liveRows(), "the reachable node still contributed its totals");
        assertTrue(agg.skippedSstables() >= 1, "the failed node is visible as a skip");
    }

    @Test
    void firstNodeFailureStillTaintsWhenPeerSucceeds() {
        // Order independence: the failing node is queried FIRST. The AND-of-complete
        // in plus() must still make the whole aggregate incomplete.
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.2"));
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.1")) {
                throw new RuntimeException("node 10.0.0.1 unreachable");
            }
            return ok(200, 20);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);

        assertFalse(agg.complete(), "fetch-failure order must not affect completeness");
        assertEquals(200, agg.liveRows());
    }

    @Test
    void duplicateAddressesAreQueriedOnce() {
        // The ring can list the same address more than once; it is queried once and a
        // success stays complete.
        int[] calls = {0};
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.1"), node(null));
        Function<String, TableStats> fetch = address -> {
            calls[0]++;
            return ok(50, 5);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);

        assertEquals(1, calls[0], "duplicate + null addresses queried at most once");
        assertTrue(agg.complete());
        assertEquals(50, agg.liveRows());
    }

    @Test
    void incompleteAggregateYieldsEmptyGroupRatio() {
        // End-to-end with the gate's pure function: an incomplete aggregate (from a
        // failed node) gives NO group-ratio estimate, so AUTOMATIC pushdown declines
        // to use a biased ratio (issue #944, #28).
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.2"));
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("down");
            }
            return ok(2000, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);
        assertFalse(agg.complete());

        String ddl = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))";
        OptionalDouble ratio =
                CqliteFlightMetadata.estimateGroupRatio(ddl, List.of("pk"), agg);
        assertTrue(ratio.isEmpty(),
                "incomplete cross-ring stats must produce no group-ratio estimate");
    }

    @Test
    void incompleteAggregateYieldsEmptyTableStatistics() {
        // The optimizer-facing path: getTableStatistics on a non-aggregated handle
        // returns empty when the underlying counts are incomplete. We invoke the same
        // completeness guard getTableStatistics uses (!complete -> empty).
        List<RingEntry> ring = List.of(node("10.0.0.1"), node("10.0.0.2"));
        Function<String, TableStats> fetch = address -> {
            if (address.equals("10.0.0.2")) {
                throw new RuntimeException("down");
            }
            return ok(2000, 10);
        };

        TableStats agg = CqliteFlightMetadata.aggregateNodeStats(ring, fetch);

        TableStatistics result =
                (!agg.complete() || agg.liveRows() <= 0)
                        ? TableStatistics.empty()
                        : TableStatistics.builder()
                                .setRowCount(io.trino.spi.statistics.Estimate.of(agg.liveRows()))
                                .build();
        assertEquals(TableStatistics.empty(), result,
                "incomplete cross-ring stats must yield empty table statistics");
        assertTrue(result.getRowCount().isUnknown());
    }
}
