package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void fullRingRangeWithEqualEndpointsScansEverything() {
        // #2228: a single full-ring range represented as `(T, T]` (start == end,
        // a common single-token/single-node representation) must be treated as a
        // wraparound/full-ring split, not the empty set. Otherwise the flight
        // filter evaluates `token > T && token <= T` and `SELECT *` silently
        // returns 0 rows.
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("42", "42", Map.of("dc1", List.of("10.0.0.1")))));
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals(1, splits.size());
        assertEquals(42, splits.get(0).tokenStart());
        assertEquals(42, splits.get(0).tokenEnd());
        assertTrue(
                splits.get(0).wraparound(),
                "start == end is the full ring — must scan everything");
    }

    @Test
    void stampsSnapshotOnEverySplit() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("-100", "0", Map.of("dc1", List.of("10.0.0.2:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.3:7000")))));

        // Snapshot mode: every split carries the same snapshot name (issue #2105).
        var snap = CqliteFlightSplitManager.buildSplits(
                TABLE, resp, "dc1", 8815, java.util.Optional.of("cqlite-q1"));
        assertEquals(2, snap.size());
        snap.forEach(s -> assertEquals(java.util.Optional.of("cqlite-q1"), s.snapshot()));

        // Live-dir overload: no snapshot on any split.
        var live = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        live.forEach(s -> assertEquals(java.util.Optional.empty(), s.snapshot()));
    }

    /**
     * The snapshot-target host set (issue #2227): every distinct replica a split reads,
     * deduplicated, using the same deterministic {@link CqliteFlightSplitManager#pickReplica}
     * choice as {@code buildSplits}. Two ranges pinned to the same host collapse to one host.
     */
    @Test
    void distinctReplicaHostsAreEveryHostSplitsRead() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        range("-100", "0", Map.of("dc1", List.of("10.0.0.3:7000", "10.0.0.2:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.2:7000"))),
                        range("100", "200", Map.of("dc1", List.of("10.0.0.5:7000")))));

        Set<String> hosts = CqliteFlightSplitManager.distinctReplicaHosts(resp, "dc1");

        // Range 1 → 10.0.0.2 (smallest), range 2 → 10.0.0.2 (dedup), range 3 → 10.0.0.5.
        assertEquals(Set.of("10.0.0.2", "10.0.0.5"), hosts);
        // Exactly the hosts the splits are pinned to.
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals(hosts, splits.stream().map(CqliteFlightSplit::host).collect(java.util.stream.Collectors.toSet()));
    }

    /**
     * IPv6 replica forms (issue #2227): a bracketed {@code [v6]:port} yields the bare v6
     * literal for both the split's pinned host and the snapshot-host set, so per-host
     * snapshot URI construction stays consistent. An unbracketed all-colons literal is
     * treated as the whole host (no port stripped).
     */
    @Test
    void ipv6ReplicaFormsPinBareHostConsistently() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        range("-100", "0", Map.of("dc1", List.of("[2001:db8::5]:7000"))),
                        range("0", "100", Map.of("dc1", List.of("2001:db8::9")))));

        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals("2001:db8::5", splits.get(0).host(), "bracketed IPv6 port stripped to bare literal");
        assertEquals("2001:db8::9", splits.get(1).host(), "bare IPv6 kept whole");

        // The snapshot-host set is exactly the splits' pinned hosts under these forms.
        Set<String> hosts = CqliteFlightSplitManager.distinctReplicaHosts(resp, "dc1");
        assertEquals(Set.of("2001:db8::5", "2001:db8::9"), hosts);
        assertEquals(hosts, splits.stream().map(CqliteFlightSplit::host).collect(java.util.stream.Collectors.toSet()));
    }

    private static final Map<String, List<String>> DC1 = Map.of("dc1", List.of("10.0.0.1:7000"));
    private static final String MIN = Long.toString(Long.MIN_VALUE);
    private static final String MAX = Long.toString(Long.MAX_VALUE);

    /**
     * Ring-coverage guard (issue #2237): the UNWRAPPED full-coverage form the real Cassandra
     * Sidecar emits — {@code (MIN,0]} + {@code (0,MAX]}, mirroring the fixture in
     * {@code SidecarClientTest#parsesTokenRangeReplicasAndTokens} — spans the ring exactly once
     * (first start == MIN, last end == MAX, no wrapping range) and MUST be accepted and build
     * splits. This is the regression the fix closes: the old validator forced a wrap range and
     * fail-closed on this healthy real-cluster topology.
     */
    @Test
    void unwrappedFullCoverageFromSidecarAccepted() {
        List<ReplicaInfo> tiling = List.of(
                range(MIN, "0", DC1),
                range("0", MAX, DC1));
        // Does not throw — the unwrapped (MIN, MAX] tiling is full coverage.
        CqliteFlightSplitManager.validateRingCoverage(tiling);
        var resp = new TokenRangeReplicasResponse(List.of(), tiling);
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals(2, splits.size(), "one split per range for the healthy unwrapped ring");
        assertFalse(splits.get(0).wraparound(), "no range wraps in the unwrapped form");
        assertFalse(splits.get(1).wraparound(), "no range wraps in the unwrapped form");
    }

    /**
     * Ring-coverage guard (issue #2237): a set of ranges that tiles the Murmur3 ring
     * exactly once — INCLUDING the wraparound range that crosses the ring boundary —
     * is accepted and splits are built normally (happy-path behavior unchanged).
     */
    @Test
    void exactTilingIncludingWraparoundAccepted() {
        // (MIN,-100] (-100,0] (0,100] (100,MIN] — the last wraps (start 100 > end MIN)
        // and its inclusive end MIN meets the first range's exclusive start MIN, closing
        // the circle with neither overlap nor gap.
        List<ReplicaInfo> tiling = List.of(
                range(MIN, "-100", DC1),
                range("-100", "0", DC1),
                range("0", "100", DC1),
                range("100", MIN, DC1));
        // Does not throw.
        CqliteFlightSplitManager.validateRingCoverage(tiling);
        // Happy path still builds one split per range.
        var resp = new TokenRangeReplicasResponse(List.of(), tiling);
        assertEquals(4, CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815).size());
    }

    /** A single full-ring range `(T, T]` (issue #2228) trivially tiles the circle. */
    @Test
    void singleFullRingRangeAccepted() {
        CqliteFlightSplitManager.validateRingCoverage(List.of(range("42", "42", DC1)));
    }

    /** Overlapping ranges are rejected with an actionable typed error naming the overlap. */
    @Test
    void overlappingRangesRejected() {
        // (MIN,10] and (0,MIN]: sorted, (MIN,10] ends at 10 but the next range starts at 0
        // (< 10) — the two share tokens (0,10] → overlap → duplicate rows.
        List<ReplicaInfo> overlap = List.of(
                range(MIN, "10", DC1),
                range("0", MIN, DC1));
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(overlap));
        assertTrue(ex.getMessage().contains("overlap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("duplicate"), ex.getMessage());
    }

    /** Gapped ranges are rejected with an actionable typed error naming the gap. */
    @Test
    void gappedRangesRejected() {
        // (MIN,0] and (100,MIN]: sorted, (MIN,0] ends at 0 but the next starts at 100 (> 0)
        // — tokens (0,100] are owned by no range → gap → missing rows.
        List<ReplicaInfo> gapped = List.of(
                range(MIN, "0", DC1),
                range("100", MIN, DC1));
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(gapped));
        assertTrue(ex.getMessage().contains("gap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("missing"), ex.getMessage());
    }

    /**
     * A GAP at the ring boundary in the UNWRAPPED form (issue #2237): {@code (MIN,0]} +
     * {@code (100,MAX]} covers only (MIN,0] and (100,MAX] — tokens (0,100] are owned by no
     * range and no range wraps to close the circle → gap → missing rows → rejected.
     */
    @Test
    void unwrappedBoundaryGapRejected() {
        List<ReplicaInfo> gapped = List.of(
                range(MIN, "0", DC1),
                range("100", MAX, DC1));
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(gapped));
        assertTrue(ex.getMessage().contains("gap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("missing"), ex.getMessage());
    }

    /**
     * A GAP at the ring boundary because the ranges do not reach MIN/MAX and no range wraps:
     * {@code (MIN,0]} alone leaves (0, MIN] (the whole rest of the ring) uncovered → gap.
     */
    @Test
    void unwrappedNonFullSpanRejectedAsGap() {
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(List.of(range(MIN, "0", DC1))));
        assertTrue(ex.getMessage().contains("gap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("missing"), ex.getMessage());
    }

    /**
     * An OVERLAP at the ring boundary: {@code (MIN,0]} + a wrapping {@code (-50,MIN]} whose
     * inclusive end MIN is BELOW the first start... the wrap's end past first.start double-covers
     * tokens (-50, 0] → overlap → duplicate rows → rejected.
     */
    @Test
    void wrapBoundaryOverlapRejected() {
        List<ReplicaInfo> overlap = List.of(
                range(MIN, "0", DC1),
                range("-50", MIN, DC1));
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(overlap));
        assertTrue(ex.getMessage().contains("overlap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("duplicate"), ex.getMessage());
    }

    /**
     * Two wrapping ranges (or a wrap that is not the single closing range) double-cover the
     * ring and are rejected as an overlap — closes the false-accept where an interior wrap /
     * full-ring range could otherwise slip through the adjacency check.
     */
    @Test
    void multipleWrappingRangesRejected() {
        List<ReplicaInfo> twoWraps = List.of(
                range("100", MIN, DC1),
                range("200", "50", DC1));
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(twoWraps));
        assertTrue(ex.getMessage().contains("overlap"), ex.getMessage());
        assertTrue(ex.getMessage().contains("double-covers"), ex.getMessage());
    }

    /** Empty ranges fail closed rather than silently scanning nothing. */
    @Test
    void emptyRangesRejected() {
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(List.of()));
        assertTrue(ex.getMessage().contains("not covered"), ex.getMessage());
    }

    /**
     * NIT (issue #2237): a null read-replica list (CqliteFlightMetadata can pass one) is
     * treated like empty — a fail-closed typed error, not a raw NullPointerException.
     */
    @Test
    void nullRangesFailClosedNotNpe() {
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightSplitManager.validateRingCoverage(null));
        assertTrue(ex.getMessage().contains("not covered"), ex.getMessage());
    }

    @Test
    void skipsRangesWithNoReplica() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("0", "100", Map.of())));
        assertTrue(CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815).isEmpty());
    }

    /**
     * Availability failover (issue #2241): in LIVE mode every other replica owner of a range is
     * an ordered fallback (primary first). The primary is a deterministic per-range rotation of
     * the sorted owner set keyed on the range start token (issue #2397) — NOT the lexicographic
     * head, which collapsed every RF==N range onto one replica; the remaining owners follow in
     * sorted order. Updated from the old "primary = smallest address" assertion: that behavior
     * was the round-9 single-node collapse bug.
     */
    @Test
    void liveModeCarriesAllOtherOwnersAsOrderedFallbacks() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(range("-100", "0",
                        Map.of("dc1", List.of("10.0.0.3:7000", "10.0.0.2:7000", "10.0.0.4:7000")))));

        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals(1, splits.size());
        // sorted owners = [.2, .3, .4]; head = floorMod(-100, 3) = 2 → primary .4, rest in order.
        assertEquals("10.0.0.4", splits.get(0).host(), "primary = rotated owner (floorMod start token)");
        assertEquals(List.of("10.0.0.2", "10.0.0.3"), splits.get(0).fallbackHosts());
        assertEquals(List.of("10.0.0.4", "10.0.0.2", "10.0.0.3"), splits.get(0).replicaHosts());
        // Full owner set retained regardless of which owner is primary (failover intact).
        assertEquals(Set.of("10.0.0.2", "10.0.0.3", "10.0.0.4"), Set.copyOf(splits.get(0).replicaHosts()));
    }

    /**
     * Snapshot-mode restriction (issue #2241 × #2227): a fallback is only usable if its host also
     * has the snapshot. The snapshot is created on exactly {@code distinctReplicaHosts} (each
     * range's primary), so a fallback that is not a primary of any range is dropped in snapshot
     * mode but kept in live mode.
     */
    @Test
    void snapshotModeRestrictsFallbacksToSnapshotHosts() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        // 10.0.0.9 owns range A but is never a primary → not a snapshot host.
                        range("-100", "0", Map.of("dc1", List.of("10.0.0.2:7000", "10.0.0.9:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.2:7000")))));

        var snap = CqliteFlightSplitManager.buildSplits(
                TABLE, resp, "dc1", 8815, java.util.Optional.of("cqlite-q1"));
        // Range A primary 10.0.0.2; its only other owner 10.0.0.9 has no snapshot → no fallback.
        assertEquals("10.0.0.2", snap.get(0).host());
        assertEquals(List.of(), snap.get(0).fallbackHosts(), "non-snapshot host dropped as fallback");

        // Live mode keeps 10.0.0.9 as a fallback (all owners eligible).
        var live = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        assertEquals(List.of("10.0.0.9"), live.get(0).fallbackHosts());
    }

    /**
     * In snapshot mode a fallback that IS a primary of another range (hence a snapshot host) is
     * kept — availability failover still works across ranges' primaries.
     */
    @Test
    void snapshotModeKeepsFallbackThatIsAnotherRangesPrimary() {
        var resp = new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        range("-100", "0", Map.of("dc1", List.of("10.0.0.2:7000", "10.0.0.3:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.3:7000")))));

        var snap = CqliteFlightSplitManager.buildSplits(
                TABLE, resp, "dc1", 8815, java.util.Optional.of("cqlite-q1"));
        // 10.0.0.3 is range B's primary → a snapshot host → kept as range A's fallback.
        assertEquals("10.0.0.2", snap.get(0).host());
        assertEquals(List.of("10.0.0.3"), snap.get(0).fallbackHosts());
    }

    /**
     * {@code orderedReplicaHosts} prefers local-DC owners (rotated within, then sorted), then
     * every other DC's owners (sorted) as further fallbacks, mapping to bare hosts and
     * de-duplicating in order. Rotation applies only to the local-DC set (issue #2397); the
     * first entry matches {@link CqliteFlightSplitManager#pickReplica}.
     */
    @Test
    void orderedReplicaHostsPrefersLocalDcThenOthers() {
        Map<String, List<String>> byDc = Map.of(
                "dc1", List.of("10.0.1.5:7000", "10.0.1.2:7000"),
                "dc2", List.of("10.0.2.9:7000"));

        // rotationKey 0 → head = sorted local[0] = .2; local .5 then remote dc2 .9 as fallbacks.
        assertEquals(
                List.of("10.0.1.2", "10.0.1.5", "10.0.2.9"),
                CqliteFlightSplitManager.orderedReplicaHosts(byDc, "dc1", 0));
        assertEquals(
                "10.0.1.2",
                CqliteFlightSplitManager.pickReplica(byDc, "dc1", 0),
                "ordered head equals pickReplica (bare host)");

        // rotationKey 1 rotates WITHIN the local DC only: head = sorted local[1] = .5, then .2;
        // the remote-DC owner stays a trailing fallback (never rotated into primary).
        assertEquals(
                List.of("10.0.1.5", "10.0.1.2", "10.0.2.9"),
                CqliteFlightSplitManager.orderedReplicaHosts(byDc, "dc1", 1));
        assertEquals(
                "10.0.1.5",
                CqliteFlightSplitManager.pickReplica(byDc, "dc1", 1),
                "rotation stays within the local datacenter");
    }
}
