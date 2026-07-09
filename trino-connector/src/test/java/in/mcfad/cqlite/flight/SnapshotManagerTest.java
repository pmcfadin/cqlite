package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-query snapshot lifecycle (issues #2105, #2227): create-at-planning on EVERY replica
 * host, fail-closed (naming host + snapshot), idempotent per (query, host, table),
 * best-effort per-host cleanup. Uses a recording fake {@link HostSnapshotApis} so no live
 * Sidecar / HTTP is needed.
 */
class SnapshotManagerTest {

    /** Records every create/clear (prefixed with the host) and can be armed to throw. */
    private static final class FakeSidecars implements HostSnapshotApis {
        final List<String> creates = Collections.synchronizedList(new ArrayList<>());
        final List<String> clears = Collections.synchronizedList(new ArrayList<>());
        volatile Set<String> failCreateHosts = Set.of();

        @Override
        public SnapshotApi forHost(String host) {
            return new SnapshotApi() {
                @Override
                public void createSnapshot(String keyspace, String table, String name, Optional<String> ttl) {
                    if (failCreateHosts.contains(host)) {
                        throw new SidecarClient.SidecarException("boom on " + host, 500);
                    }
                    creates.add(host + "|" + keyspace + "." + table + "/" + name + "/ttl=" + ttl.orElse(""));
                }

                @Override
                public void clearSnapshot(String keyspace, String table, String name) {
                    clears.add(host + "|" + keyspace + "." + table + "/" + name);
                }
            };
        }
    }

    @Test
    void liveModeNeverTouchesSidecar() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.LIVE, Optional.of("6h"));

        assertEquals(Optional.empty(), mgr.snapshotFor("q1", "ks", "t", List.of("h1", "h2")));
        mgr.cleanup("q1");

        assertTrue(fake.creates.isEmpty(), "live mode creates no snapshot");
        assertTrue(fake.clears.isEmpty(), "live mode clears nothing");
    }

    @Test
    void snapshotModeCreatesNamedSnapshotWithTtl() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        Optional<String> name =
                mgr.snapshotFor("20260706_120000_00001_abcde", "ks", "t", List.of("10.0.0.1"));

        assertEquals(Optional.of("cqlite-20260706_120000_00001_abcde"), name);
        assertEquals(List.of("10.0.0.1|ks.t/cqlite-20260706_120000_00001_abcde/ttl=6h"), fake.creates);
    }

    /**
     * AC1/AC2 (issue #2227): the snapshot must be created on EVERY replica host a split will
     * read, not just the configured Sidecar's node — one PUT per distinct host, same name.
     */
    @Test
    void createsSnapshotOnEveryReplicaHost() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        Optional<String> name = mgr.snapshotFor("q1", "ks", "t", List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"));

        assertEquals(Optional.of("cqlite-q1"), name);
        assertEquals(List.of(
                        "10.0.0.1|ks.t/cqlite-q1/ttl=6h",
                        "10.0.0.2|ks.t/cqlite-q1/ttl=6h",
                        "10.0.0.3|ks.t/cqlite-q1/ttl=6h"),
                fake.creates.stream().sorted().toList());
    }

    /**
     * AC3 (issue #2227): a create failure on one host fails closed with an actionable error
     * naming the offending host AND the snapshot — never a bare NotFound / opaque failure.
     */
    @Test
    void failsClosedNamingHostAndSnapshot() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("10.0.0.2");
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        SidecarClient.SidecarException ex = assertThrows(SidecarClient.SidecarException.class,
                () -> mgr.snapshotFor("q1", "ks", "t", List.of("10.0.0.1", "10.0.0.2")));

        assertTrue(ex.getMessage().contains("10.0.0.2"), "error names the failing host: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("cqlite-q1"), "error names the snapshot: " + ex.getMessage());
    }

    @Test
    void createIsIdempotentPerQueryHostTable() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());

        mgr.snapshotFor("q1", "ks", "t", List.of("h1", "h2"));
        mgr.snapshotFor("q1", "ks", "t", List.of("h1", "h2")); // re-plan same scan

        assertEquals(2, fake.creates.size(), "at most one PUT per (query, host, keyspace, table)");
    }

    @Test
    void failsClosedOnCreateError() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("h1");
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.of("6h"));

        assertThrows(SidecarClient.SidecarException.class, () -> mgr.snapshotFor("q1", "ks", "t", List.of("h1")));

        // A failed create is NOT recorded, so cleanup won't try to delete a phantom snapshot.
        mgr.cleanup("q1");
        assertTrue(fake.clears.isEmpty());
    }

    @Test
    void cleanupDeletesEveryCreatedSnapshotOnEveryHost() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.SNAPSHOT, Optional.empty());

        mgr.snapshotFor("q1", "ks", "a", List.of("h1", "h2"));
        mgr.snapshotFor("q1", "ks", "b", List.of("h1"));
        mgr.cleanup("q1");

        assertEquals(List.of("h1|ks.a/cqlite-q1", "h1|ks.b/cqlite-q1", "h2|ks.a/cqlite-q1"),
                fake.clears.stream().sorted().toList());
    }

    @Test
    void cleanupSwallowsDeleteFailures() {
        HostSnapshotApis throwing = host -> new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {}
            @Override
            public void clearSnapshot(String k, String t, String n) {
                throw new SidecarClient.SidecarException("delete failed", 500);
            }
        };
        SnapshotManager mgr = new SnapshotManager(throwing, ReadMode.SNAPSHOT, Optional.empty());
        mgr.snapshotFor("q1", "ks", "t", List.of("h1", "h2"));
        // Best-effort: a delete failure must not propagate (TTL backstop reclaims it).
        mgr.cleanup("q1");
    }

    @Test
    void snapshotNameSanitizesUnsafeChars() {
        assertEquals("cqlite-q_1_x", SnapshotManager.snapshotName("q/1 x"));
    }

    /**
     * Concurrent callers for the same (query, host, keyspace, table) must PUT exactly once,
     * even while the (slow) create is in flight. The per-key-future memoization (issue #2113
     * / N5) moves the network call off the ConcurrentHashMap bin lock but keeps exactly-once
     * — this test gates the create on a latch so many threads pile up mid-create, then
     * asserts one PUT and one shared snapshot name.
     */
    @Test
    void concurrentCallersPutExactlyOnce() throws Exception {
        AtomicInteger creates = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        HostSnapshotApis blocking = host -> new SnapshotApi() {
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
                return mgr.snapshotFor("q1", "ks", "t", List.of("h1"));
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
        HostSnapshotApis api = host -> new SnapshotApi() {
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
                    pool.submit(() -> mgr.snapshotFor("q1", "ks", "t", List.of("h1")));
            assertTrue(winnerInCreate.await(5, TimeUnit.SECONDS), "winner reached the create");
            java.util.concurrent.Future<Optional<String>> waiter =
                    pool.submit(() -> mgr.snapshotFor("q1", "ks", "t", List.of("h1")));
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
            assertEquals(Optional.of("cqlite-q1"), mgr.snapshotFor("q1", "ks", "t", List.of("h1")));
            assertEquals(2, creates.get(), "one failed PUT + one successful retry PUT");

            // cleanup deletes only the successfully created snapshot and must not throw.
            mgr.cleanup("q1");
        } finally {
            pool.shutdownNow();
        }
    }
}
