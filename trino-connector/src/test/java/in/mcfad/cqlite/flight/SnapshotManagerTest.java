package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Concurrent callers for the same (query, keyspace, table) must PUT exactly once, even
     * while the (slow) create is in flight. The per-key-future memoization (issue #2113 / N5)
     * moves the network call off the ConcurrentHashMap bin lock but keeps exactly-once — this
     * test gates the create on a latch so many threads pile up mid-create, then asserts one
     * PUT and one shared snapshot name.
     */
    @Test
    void concurrentCallersPutExactlyOnce() throws Exception {
        AtomicInteger creates = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        SnapshotApi blocking = new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {
                creates.incrementAndGet();
                try {
                    // Hold the create open so competing threads reach putIfAbsent while it runs.
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            @Override
            public void clearSnapshot(String k, String t, String n) {}
        };
        SnapshotManager mgr = new SnapshotManager(blocking, ReadMode.SNAPSHOT, Optional.empty());

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<Optional<String>>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return mgr.snapshotFor("q1", "ks", "t");
            }));
        }
        start.countDown();          // fire all callers
        Thread.sleep(50);           // let them collide on the in-flight create
        release.countDown();        // let the winning create finish

        for (java.util.concurrent.Future<Optional<String>> f : results) {
            assertEquals(Optional.of("cqlite-q1"), f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        assertEquals(1, creates.get(), "exactly one PUT for concurrent same-key callers");
    }

    /**
     * A winner that dies with a NON-RuntimeException throwable (an {@link Error}) must never
     * leave an incomplete future in the map (roborev on issue #2113): a concurrent waiter
     * must unblock with a failure — not hang on {@code join()} forever — and a subsequent
     * caller must retry with exactly one new PUT. Uses short get() timeouts so a liveness
     * regression fails this test fast instead of hanging the suite.
     */
    @Test
    void errorFromCreateNeverLeavesIncompleteFuture() throws Exception {
        AtomicInteger creates = new AtomicInteger();
        CountDownLatch winnerInCreate = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SnapshotApi api = new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {
                if (creates.incrementAndGet() == 1) {
                    winnerInCreate.countDown();
                    try {
                        // Hold the create open so the waiter joins the in-flight future.
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new AssertionError("sidecar died mid-create"); // an Error, not a RuntimeException
                }
                // Second call (the retry) succeeds.
            }
            @Override
            public void clearSnapshot(String k, String t, String n) {}
        };
        SnapshotManager mgr = new SnapshotManager(api, ReadMode.SNAPSHOT, Optional.empty());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<Optional<String>> winner =
                    pool.submit(() -> mgr.snapshotFor("q1", "ks", "t"));
            assertTrue(winnerInCreate.await(5, TimeUnit.SECONDS), "winner reached the create");
            java.util.concurrent.Future<Optional<String>> waiter =
                    pool.submit(() -> mgr.snapshotFor("q1", "ks", "t"));
            Thread.sleep(50); // let the waiter pile up on the in-flight future
            release.countDown();

            // (a) Both callers FAIL (fail-closed); the waiter does NOT hang on join().
            java.util.concurrent.ExecutionException winnerEx = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> winner.get(5, TimeUnit.SECONDS));
            assertTrue(winnerEx.getCause() instanceof AssertionError,
                    "winner rethrows the original Error, got: " + winnerEx.getCause());
            java.util.concurrent.ExecutionException waiterEx = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> waiter.get(5, TimeUnit.SECONDS));
            assertTrue(waiterEx.getCause() instanceof AssertionError,
                    "waiter surfaces the winner's Error, got: " + waiterEx.getCause());

            // (b) The failed future was removed, so a retry recomputes and succeeds —
            // with exactly one NEW PUT.
            assertEquals(Optional.of("cqlite-q1"), mgr.snapshotFor("q1", "ks", "t"));
            assertEquals(2, creates.get(), "one failed PUT + one successful retry PUT");

            // cleanup deletes only the successfully created snapshot and must not throw.
            mgr.cleanup("q1");
        } finally {
            pool.shutdownNow();
        }
    }
}
