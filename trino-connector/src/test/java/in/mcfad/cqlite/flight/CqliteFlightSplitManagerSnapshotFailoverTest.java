package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the exact wiring {@link CqliteFlightSplitManager#getSplits} performs (issue
 * #2241 roborev): a per-host snapshot is created over the FULL replica-owner union — not just
 * {@link CqliteFlightSplitManager#distinctReplicaHosts primaries} — so an availability-failover
 * fallback that is never any range's primary is still eligible when its own snapshot creation
 * succeeds, and is excluded (without failing the query) when it fails.
 *
 * <p>Before the fix, {@code buildSplits} restricted fallbacks to {@code distinctReplicaHosts}
 * (primaries only), so a fallback host that never happened to be another range's primary was
 * dropped even when a real per-host snapshot for it would have succeeded — a primary outage
 * could still fail the scan despite another live replica owning the range.
 */
class CqliteFlightSplitManagerSnapshotFailoverTest {
    private static final CqliteFlightTableHandle TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY)");

    /** Records every create (prefixed with the host) and can be armed to throw per host. */
    private static final class FakeSidecars implements HostSnapshotApis {
        final List<String> creates = Collections.synchronizedList(new ArrayList<>());
        volatile Set<String> failCreateHosts = Set.of();

        @Override
        public SnapshotApi forHost(String host) {
            return new SnapshotApi() {
                @Override
                public void createSnapshot(String keyspace, String table, String name, Optional<String> ttl) {
                    if (failCreateHosts.contains(host)) {
                        throw new SidecarClient.SidecarException("boom on " + host, 500);
                    }
                    creates.add(host);
                }

                @Override
                public void clearSnapshot(String keyspace, String table, String name) {}
            };
        }
    }

    private static ReplicaInfo range(String start, String end, Map<String, List<String>> byDc) {
        return new ReplicaInfo(start, end, byDc);
    }

    /**
     * Range A: primary 10.0.0.2, extra owner 10.0.0.9 (never any range's primary).
     * Range B: primary 10.0.0.2 only.
     */
    private static TokenRangeReplicasResponse twoRangesWithNonPrimaryOwner() {
        return new TokenRangeReplicasResponse(
                List.of(),
                List.of(
                        range("-100", "0", Map.of("dc1", List.of("10.0.0.2:7000", "10.0.0.9:7000"))),
                        range("0", "100", Map.of("dc1", List.of("10.0.0.2:7000")))));
    }

    /** Mirrors {@link CqliteFlightSplitManager#getSplits}'s snapshot-mode wiring exactly. */
    private static List<CqliteFlightSplit> planSnapshotModeSplits(
            TokenRangeReplicasResponse resp, SnapshotManager snapshots) {
        Set<String> primaryHosts = CqliteFlightSplitManager.distinctReplicaHosts(resp, "dc1");
        Optional<String> snapshot = snapshots.snapshotFor("q1", "ks", "t", primaryHosts);
        Set<String> availableHosts = snapshots.availableHosts(
                "q1", "ks", "t", CqliteFlightSplitManager.allReplicaHosts(resp, "dc1"));
        return CqliteFlightSplitManager.buildSplits(TABLE, resp, "dc1", 8815, snapshot, availableHosts);
    }

    @Test
    void fallbackThatIsNeverAPrimaryStillGetsSnapshottedAndSurvives() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager snapshots = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());
        TokenRangeReplicasResponse resp = twoRangesWithNonPrimaryOwner();

        List<CqliteFlightSplit> splits = planSnapshotModeSplits(resp, snapshots);

        assertEquals("10.0.0.2", splits.get(0).host());
        assertEquals(List.of("10.0.0.9"), splits.get(0).fallbackHosts(),
                "non-primary owner survives because its OWN snapshot creation succeeded");
        assertTrue(fake.creates.contains("10.0.0.9"),
                "the snapshot must actually be created on the fallback-only host, not just primaries");
    }

    @Test
    void fallbackWhoseSnapshotCreationFailsIsExcludedWithoutFailingTheQuery() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("10.0.0.9"); // fallback-only host's create fails
        SnapshotManager snapshots = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());
        TokenRangeReplicasResponse resp = twoRangesWithNonPrimaryOwner();

        // Planning must NOT throw — only the primary (required) hosts are fail-closed.
        List<CqliteFlightSplit> splits = planSnapshotModeSplits(resp, snapshots);

        assertEquals("10.0.0.2", splits.get(0).host());
        assertEquals(List.of(), splits.get(0).fallbackHosts(),
                "excluded: its snapshot creation failed, so it cannot serve a snapshot-mode read");
    }

    @Test
    void primarySnapshotCreationFailureStillFailsClosed() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("10.0.0.2"); // the primary itself fails
        SnapshotManager snapshots = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());
        TokenRangeReplicasResponse resp = twoRangesWithNonPrimaryOwner();

        assertThrows(SidecarClient.SidecarException.class, () -> planSnapshotModeSplits(resp, snapshots),
                "a REQUIRED (primary) host's snapshot failure must still fail the whole query (#2227)");
    }
}
