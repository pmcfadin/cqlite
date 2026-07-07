package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-query snapshot lifecycle (issue #2105): create-at-planning, fail-closed,
 * idempotent, best-effort cleanup. Uses a recording fake {@link SnapshotApi} so no
 * live Sidecar / HTTP is needed.
 */
class SnapshotManagerTest {

    /** Records every create/clear and can be armed to throw on create. */
    private static final class FakeSidecar implements SnapshotApi {
        final List<String> creates = new ArrayList<>();
        final List<String> clears = new ArrayList<>();
        boolean failCreate;

        @Override
        public void createSnapshot(String keyspace, String table, String name, Optional<String> ttl) {
            if (failCreate) {
                throw new SidecarClient.SidecarException("boom", 500);
            }
            creates.add(keyspace + "." + table + "/" + name + "/ttl=" + ttl.orElse(""));
        }

        @Override
        public void clearSnapshot(String keyspace, String table, String name) {
            clears.add(keyspace + "." + table + "/" + name);
        }
    }

    @Test
    void liveModeNeverTouchesSidecar() {
        FakeSidecar fake = new FakeSidecar();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.LIVE, Optional.of("6h"));

        assertEquals(Optional.empty(), mgr.snapshotFor("q1", "ks", "t"));
        mgr.cleanup("q1");

        assertTrue(fake.creates.isEmpty(), "live mode creates no snapshot");
        assertTrue(fake.clears.isEmpty(), "live mode clears nothing");
    }

    @Test
    void snapshotModeCreatesNamedSnapshotWithTtl() {
        FakeSidecar fake = new FakeSidecar();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        Optional<String> name = mgr.snapshotFor("20260706_120000_00001_abcde", "ks", "t");

        assertEquals(Optional.of("cqlite-20260706_120000_00001_abcde"), name);
        assertEquals(List.of("ks.t/cqlite-20260706_120000_00001_abcde/ttl=6h"), fake.creates);
    }

    @Test
    void createIsIdempotentPerQueryTable() {
        FakeSidecar fake = new FakeSidecar();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());

        mgr.snapshotFor("q1", "ks", "t");
        mgr.snapshotFor("q1", "ks", "t"); // re-plan same scan

        assertEquals(1, fake.creates.size(), "at most one PUT per (query, keyspace, table)");
    }

    @Test
    void failsClosedOnCreateError() {
        FakeSidecar fake = new FakeSidecar();
        fake.failCreate = true;
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        assertThrows(SidecarClient.SidecarException.class, () -> mgr.snapshotFor("q1", "ks", "t"));

        // A failed create is NOT recorded, so cleanup won't try to delete a phantom snapshot.
        mgr.cleanup("q1");
        assertTrue(fake.clears.isEmpty());
    }

    @Test
    void cleanupDeletesEveryCreatedSnapshot() {
        FakeSidecar fake = new FakeSidecar();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());

        mgr.snapshotFor("q1", "ks", "a");
        mgr.snapshotFor("q1", "ks", "b");
        mgr.cleanup("q1");

        assertEquals(List.of("ks.a/cqlite-q1", "ks.b/cqlite-q1"),
                fake.clears.stream().sorted().toList());
    }

    @Test
    void cleanupSwallowsDeleteFailures() {
        SnapshotApi throwing = new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {}
            @Override
            public void clearSnapshot(String k, String t, String n) {
                throw new SidecarClient.SidecarException("delete failed", 500);
            }
        };
        SnapshotManager mgr = new SnapshotManager(throwing, ReadMode.SNAPSHOT, Optional.empty());
        mgr.snapshotFor("q1", "ks", "t");
        // Best-effort: a delete failure must not propagate (TTL backstop reclaims it).
        mgr.cleanup("q1");
    }

    @Test
    void snapshotNameSanitizesUnsafeChars() {
        assertEquals("cqlite-q_1_x", SnapshotManager.snapshotName("q/1 x"));
    }
}
