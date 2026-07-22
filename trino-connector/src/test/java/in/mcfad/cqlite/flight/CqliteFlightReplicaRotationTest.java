package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-9 field finding (issue #2397): with RF == N every token range shares one
 * identical owner set, and head-of-sorted primary selection
 * ({@code sorted().findFirst()}) pinned EVERY split to the lexicographically-smallest
 * replica — so 1 of N flight pods did all the work (deterministic single-node collapse
 * under concurrent load). Deterministic per-range rotation must spread the primaries
 * across all N owners while keeping the full owner set per split for failover
 * (issue #2241) and routing the per-host snapshot set (issue #2227) through the same
 * chooser. The pre-existing suite hides this bug because its fixtures deliberately vary
 * the owner set per range.
 */
class CqliteFlightReplicaRotationTest {

    private static final CqliteFlightTableHandle TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY)");

    // RF == N == 3: ONE identical owner set shared by every range (the field's 3-node/RF=3 shape).
    private static final List<String> OWNERS =
            List.of("10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000");
    private static final Set<String> BARE_OWNERS = Set.of("10.0.0.1", "10.0.0.2", "10.0.0.3");

    /** M token ranges, every one owned by the identical RF=N owner set. */
    private static TokenRangeReplicasResponse ringOf(int m) {
        List<ReplicaInfo> ranges = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            ranges.add(new ReplicaInfo(Integer.toString(i), Integer.toString(i + 100),
                    Map.of("dc1", OWNERS)));
        }
        return new TokenRangeReplicasResponse(List.of(), ranges);
    }

    @Test
    void primariesSpreadAcrossAllOwnersWhenRfEqualsN() {
        int m = 9;
        int n = OWNERS.size();
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, ringOf(m), "dc1", 8815);
        assertEquals(m, splits.size(), "one split per token range");

        // (1) The set of pinned primaries spans ALL N owners — the round-9 collapse pinned
        // every split to the lexicographically-smallest replica, so this was 1 (RED).
        Set<String> primaries = splits.stream().map(CqliteFlightSplit::host).collect(Collectors.toSet());
        assertEquals(n, primaries.size(),
                "every replica owner must be primary for some range (was 1 before the fix)");

        // (2) No host is primary for more than ceil(M/N) ranges — a balanced spread.
        Map<String, Long> perHost = splits.stream()
                .collect(Collectors.groupingBy(CqliteFlightSplit::host, Collectors.counting()));
        long cap = ((long) m + n - 1) / n;
        perHost.forEach((h, c) -> assertTrue(c <= cap,
                h + " is primary for " + c + " ranges, exceeding ceil(M/N)=" + cap));

        // (3) Each split still carries the FULL owner set for failover (issue #2241 intact).
        for (var s : splits) {
            assertEquals(BARE_OWNERS, Set.copyOf(s.replicaHosts()),
                    "split retains every owner for availability failover");
            assertEquals(n, s.replicaHosts().size(), "no duplicated owner in the try list");
            assertTrue(BARE_OWNERS.contains(s.host()), "primary is one of the owners");
        }
    }

    @Test
    void selectionIsDeterministicAcrossInvocations() {
        // (4) Same range → same primary across two independent buildSplits calls (stable
        // across Trino re-planning).
        var a = CqliteFlightSplitManager.buildSplits(TABLE, ringOf(9), "dc1", 8815);
        var b = CqliteFlightSplitManager.buildSplits(TABLE, ringOf(9), "dc1", 8815);
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).host(), b.get(i).host(),
                    "same range must map to the same primary across re-planning");
            assertEquals(a.get(i).replicaHosts(), b.get(i).replicaHosts(),
                    "ordered try-list must be stable across re-planning");
        }
    }

    @Test
    void snapshotHostSetCoversEveryRotatedPrimary() {
        // #2227 interplay: the per-host snapshot set must be computed through the SAME rotated
        // chooser, so every pinned primary has its snapshot created (else a split reads a
        // non-existent snapshot dir → NotFound). It must equal the set of splits' primaries.
        var resp = ringOf(9);
        Set<String> snapshotHosts = CqliteFlightSplitManager.distinctReplicaHosts(resp, "dc1");
        var splits = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        Set<String> primaries = splits.stream().map(CqliteFlightSplit::host).collect(Collectors.toSet());

        assertEquals(primaries, snapshotHosts,
                "snapshot set must equal every rotated primary so each pinned host has its snapshot");
        assertEquals(OWNERS.size(), snapshotHosts.size(),
                "with RF==N spread, all N owners are pinned primaries and get a snapshot");
    }

    // --- Slice-granularity invariants at K=4 (issue #2680) ----------------------------------------

    private static final int K = 4;

    private static List<CqliteFlightSplit> buildSliced(TokenRangeReplicasResponse resp, int k) {
        return CqliteFlightSplitManager.buildSplits(
                TABLE, resp, "dc1", 8815, Optional.empty(), Set.of(), k);
    }

    @Test
    void slicingEmitsKSplitsPerRangeEachRetainingTheFullOwnerSet() {
        int m = 9;
        int n = OWNERS.size();
        var splits = buildSliced(ringOf(m), K);
        assertEquals(m * K, splits.size(), "K slices per token range (issue #2680)");

        // #2241 holds at slice granularity: every slice's split keeps the parent's full owner set.
        for (var s : splits) {
            assertEquals(BARE_OWNERS, Set.copyOf(s.replicaHosts()),
                    "every slice retains the parent range's complete owner set for failover");
            assertEquals(n, s.replicaHosts().size(), "no duplicated owner in the try list");
            assertTrue(BARE_OWNERS.contains(s.host()), "slice primary is one of the parent's owners");
        }

        // Primaries span all N owners, and no owner exceeds the count cap at slice granularity.
        Set<String> primaries = splits.stream().map(CqliteFlightSplit::host).collect(Collectors.toSet());
        assertEquals(n, primaries.size(), "sliced primaries spread across every owner");
        Map<String, Long> perHost = splits.stream()
                .collect(Collectors.groupingBy(CqliteFlightSplit::host, Collectors.counting()));
        long cap = ((long) m * K + n - 1) / n;
        perHost.forEach((h, c) -> assertTrue(c <= cap,
                h + " is primary for " + c + " slices, exceeding ceil(totalSlices/N)=" + cap));
    }

    @Test
    void sliceSelectionIsDeterministicAcrossInvocations() {
        // Determinism across re-planning at slice granularity (issue #2680): boundaries, per-slice
        // primaries, and per-slice failover orderings are identical between two builds.
        var a = buildSliced(ringOf(9), K);
        var b = buildSliced(ringOf(9), K);
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).tokenStart(), b.get(i).tokenStart(), "slice start stable");
            assertEquals(a.get(i).tokenEnd(), b.get(i).tokenEnd(), "slice end stable");
            assertEquals(a.get(i).host(), b.get(i).host(), "slice primary stable across re-planning");
            assertEquals(a.get(i).replicaHosts(), b.get(i).replicaHosts(),
                    "slice failover ordering stable across re-planning");
        }
    }

    @Test
    void snapshotHostSetCoversEverySlicePrimaryAtK4() {
        // #2227 at slice granularity: the snapshot host set (computed through the same slicing seam)
        // must equal exactly the set of per-slice primaries so each pinned host has its snapshot.
        var resp = ringOf(9);
        Set<String> snapshotHosts = CqliteFlightSplitManager.distinctReplicaHosts(resp, "dc1", K);
        var splits = buildSliced(resp, K);
        Set<String> primaries = splits.stream().map(CqliteFlightSplit::host).collect(Collectors.toSet());

        assertEquals(primaries, snapshotHosts,
                "snapshot set must equal every per-slice primary so each pinned host has its snapshot");
        assertEquals(OWNERS.size(), snapshotHosts.size(),
                "with RF==N and K>=N, all N owners are pinned slice primaries and get a snapshot");
    }

    @Test
    void k1IsIdentityForRangesHostsAndOwnerSets() {
        // K=1 emits exactly the pre-#2680 one-split-per-range set (ranges, hosts, owner sets, count).
        var resp = ringOf(9);
        var legacy = CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815);
        var k1 = buildSliced(resp, 1);
        assertEquals(legacy.size(), k1.size(), "K=1 is one split per range");
        for (int i = 0; i < legacy.size(); i++) {
            assertEquals(legacy.get(i).tokenStart(), k1.get(i).tokenStart());
            assertEquals(legacy.get(i).tokenEnd(), k1.get(i).tokenEnd());
            assertEquals(legacy.get(i).wraparound(), k1.get(i).wraparound());
            assertEquals(legacy.get(i).host(), k1.get(i).host(), "K=1 primary identical to pre-#2680");
            assertEquals(legacy.get(i).replicaHosts(), k1.get(i).replicaHosts(),
                    "K=1 failover ordering identical to pre-#2680");
        }
    }
}
