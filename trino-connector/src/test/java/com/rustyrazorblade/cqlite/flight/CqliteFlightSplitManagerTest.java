package com.rustyrazorblade.cqlite.flight;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CqliteFlightSplitManagerTest {

    private static final CqliteFlightTableHandle TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY)");

    private static ReplicaInfo range(String start, String end, Map<String, List<String>> byDc) {
        return new ReplicaInfo(start, end, byDc);
    }

    @Test
    void oneSplitPerRangePinnedToOneReplica() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        // Sidecar returns "ip:storage_port"; the host is stripped.
                        range("-100", "0", Map.of("dc1", List.of("10.0.0.3:7000", "10.0.0.2:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.2:7000")))));

        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);

        assertEquals(2, splits.size(), "one split per token range");
        // Each split pinned to exactly one replica (deterministic: smallest address),
        // with the storage port stripped to leave the host.
        assertEquals("10.0.0.2", splits.get(0).host());
        assertEquals(-100, splits.get(0).tokenStart());
        assertEquals(0, splits.get(0).tokenEnd());
        assertEquals(8815, splits.get(0).port());
        assertEquals("ks", splits.get(0).keyspace());
        assertFalse(splits.get(0).wraparound());
    }

    @Test
    void prefersLocalDatacenter() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("0", "100",
                        Map.of("dc2", List.of("10.0.2.1"), "dc1", List.of("10.0.1.9")))));

        var local = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals("10.0.1.9", local.get(0).host(), "local DC replica chosen");

        // With no local DC preference, falls back to the globally-smallest address.
        var any = CqliteFlightSplitManager.buildSplits(TABLE, resp, null, 8815);
        assertEquals("10.0.1.9", any.get(0).host());
    }

    @Test
    void detectsWraparoundRange() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("100", "-100", Map.of("dc1", List.of("10.0.0.1")))));
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertTrue(splits.get(0).wraparound(), "start > end is a wraparound range");
    }

    @Test
    void skipsRangesWithNoReplica() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("0", "100", Map.of())));
        assertTrue(CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815).isEmpty());
    }
}
