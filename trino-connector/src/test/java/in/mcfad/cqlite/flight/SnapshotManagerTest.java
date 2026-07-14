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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot lifecycle: per-{@code (keyspace, table)} REUSE within a freshness window (issues
 * #2356/#2306) — one create fan-out per window (not per query), invalidated by window expiry, an
 * observed generation-set change, or an explicit refresh — on top of the preserved per-host
 * fail-closed create model (#2227). Window timing is driven by an injected logical clock (never
 * {@code System.currentTimeMillis}, per #1742). Uses a recording fake {@link HostSnapshotApis} so
 * no live Sidecar / HTTP is needed.
 */
class SnapshotManagerTest {

    private static final long WINDOW = 1_000L; // logical nanos

    /** A settable logical clock so window timing is pinned deterministically (no wall-clock). */
    private static final class ManualClock implements SnapshotManager.Clock {
        volatile long nanos;

        @Override
        public long nanoTime() {
            return nanos;
        }

        void advance(long delta) {
            nanos += delta;
        }
    }

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

    private SnapshotManager snapshotMgr(HostSnapshotApis sidecars, ManualClock clock) {
        return new SnapshotManager(sidecars, ReadMode.SNAPSHOT, Optional.of("6h"), WINDOW, clock);
    }

    // ---- LIVE-mode inertness (flight-snapshot-reuse spec) --------------------------------------

    @Test
    void liveModePerformsNoReuseAndNoSidecarCalls() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = new SnapshotManager(fake, ReadMode.LIVE, Optional.of("6h"), WINDOW, new ManualClock());

        for (int i = 0; i < 5; i++) {
            assertEquals(Optional.empty(), mgr.snapshotFor("ks", "t", List.of("h1", "h2")));
        }
        assertEquals(Set.of("h1", "h2"), mgr.availableHosts("ks", "t", List.of("h1", "h2")));

        assertTrue(fake.creates.isEmpty(), "live mode creates no snapshot");
        assertTrue(fake.clears.isEmpty(), "live mode clears nothing");
        assertEquals(0, mgr.snapshotCreationsTotal(), "live mode never creates");
        assertEquals(0, mgr.snapshotReuseHitsTotal(), "live mode never reuses");
    }

    // ---- Naming + per-host fail-closed create (#2227) ------------------------------------------

    @Test
    void snapshotModeCreatesEpochNamedSnapshotWithTtl() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        Optional<String> name = mgr.snapshotFor("ks", "t", List.of("10.0.0.1"));

        assertEquals(Optional.of("cqlite-ks-t-0"), name);
        assertEquals(List.of("10.0.0.1|ks.t/cqlite-ks-t-0/ttl=6h"), fake.creates);
        assertEquals(1, mgr.snapshotCreationsTotal());
    }

    @Test
    void createsSnapshotOnEveryReplicaHost() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        Optional<String> name = mgr.snapshotFor("ks", "t", List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"));

        assertEquals(Optional.of("cqlite-ks-t-0"), name);
        assertEquals(List.of(
                        "10.0.0.1|ks.t/cqlite-ks-t-0/ttl=6h",
                        "10.0.0.2|ks.t/cqlite-ks-t-0/ttl=6h",
                        "10.0.0.3|ks.t/cqlite-ks-t-0/ttl=6h"),
                fake.creates.stream().sorted().toList());
        assertEquals(1, mgr.snapshotCreationsTotal(), "one window ⇒ one create fan-out");
    }

    @Test
    void failsClosedNamingHostAndSnapshot() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("10.0.0.2");
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        SidecarClient.SidecarException ex = assertThrows(SidecarClient.SidecarException.class,
                () -> mgr.snapshotFor("ks", "t", List.of("10.0.0.1", "10.0.0.2")));

        assertTrue(ex.getMessage().contains("10.0.0.2"), "error names the failing host: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("cqlite-ks-t-0"), "error names the snapshot: " + ex.getMessage());
    }

    @Test
    void nameSanitizesUnsafeChars() {
        assertEquals("cqlite-k_s-t_x-3", SnapshotManager.nameFor("k/s", "t x", 3));
    }

    // ---- Reuse within one window (flight-snapshot-reuse spec) ----------------------------------

    /** Scenario: N queries within one window create exactly one snapshot. */
    @Test
    void nQueriesInOneWindowCreateExactlyOneSnapshot() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        SnapshotManager mgr = snapshotMgr(fake, clock);

        int n = 5;
        String first = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        for (int i = 1; i < n; i++) {
            clock.advance(1); // still well within WINDOW
            assertEquals(Optional.of(first), mgr.snapshotFor("ks", "t", List.of("h1")),
                    "every query in the window receives the SAME snapshot name");
        }

        assertEquals(1, mgr.snapshotCreationsTotal(), "exactly one create fan-out for N queries in one window");
        assertEquals(n - 1L, mgr.snapshotReuseHitsTotal(), "the other N-1 queries are reuse hits");
        assertEquals(1, fake.creates.size(), "exactly one Sidecar PUT on the host");
    }

    @Test
    void rePlanReusesWindowWithoutDuplicatePut() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        mgr.snapshotFor("ks", "t", List.of("h1", "h2"));
        mgr.snapshotFor("ks", "t", List.of("h1", "h2")); // re-plan same scan, same window

        assertEquals(2, fake.creates.size(), "at most one PUT per (window, host)");
        assertEquals(1, mgr.snapshotCreationsTotal());
        assertEquals(1, mgr.snapshotReuseHitsTotal());
    }

    // ---- Invalidation: window expiry / generation change / explicit refresh --------------------

    /** Scenario: A new query after window expiry creates a fresh snapshot. */
    @Test
    void newQueryAfterWindowExpiryCreatesFreshSnapshot() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        SnapshotManager mgr = snapshotMgr(fake, clock);

        String w0 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        clock.advance(WINDOW); // window elapses
        String w1 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();

        assertEquals("cqlite-ks-t-0", w0);
        assertEquals("cqlite-ks-t-1", w1, "post-expiry query gets a fresh (next-epoch) snapshot");
        assertEquals(2, mgr.snapshotCreationsTotal(), "one create per window");
        assertEquals(0, mgr.snapshotReuseHitsTotal(), "no reuse across the window boundary");
        // The superseded window's snapshot is retired.
        assertTrue(fake.clears.stream().anyMatch(c -> c.contains("cqlite-ks-t-0")),
                "the superseded snapshot is retired, got clears=" + fake.clears);
    }

    /** Scenario: An explicit refresh forces a fresh snapshot on the next query. */
    @Test
    void explicitRefreshForcesFreshSnapshot() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        SnapshotManager mgr = snapshotMgr(fake, clock);

        String w0 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        mgr.invalidate("ks", "t");
        String w1 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow(); // still within window

        assertNotEquals(w0, w1, "explicit refresh forces a NEW snapshot even within the window");
        assertEquals(2, mgr.snapshotCreationsTotal());
        assertTrue(fake.clears.stream().anyMatch(c -> c.contains(w0)),
                "the invalidated snapshot is retired, got clears=" + fake.clears);
    }

    /** Scenario: An observed generation-set change invalidates reuse. */
    @Test
    void observedGenerationSetChangeInvalidatesReuse() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        SnapshotManager mgr = snapshotMgr(fake, clock);

        String overG = mgr.snapshotFor("ks", "t", List.of("h1"), 111L).orElseThrow();
        // Same generation within the window ⇒ reuse.
        assertEquals(overG, mgr.snapshotFor("ks", "t", List.of("h1"), 111L).orElseThrow());
        // A changed generation set within the window ⇒ fresh snapshot.
        String overGprime = mgr.snapshotFor("ks", "t", List.of("h1"), 222L).orElseThrow();

        assertNotEquals(overG, overGprime, "a generation-set change forces a fresh snapshot");
        assertEquals(2, mgr.snapshotCreationsTotal(), "one create for G, one for G'");
        assertEquals(1, mgr.snapshotReuseHitsTotal(), "the same-generation repeat was the only reuse");
    }

    /** Scenario: Snapshot creation rate over a query-heavy workload drops by the reuse factor. */
    @Test
    void snapshotCreationRateOverWorkloadDropsByReuseFactor() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        SnapshotManager mgr = snapshotMgr(fake, clock);

        int windows = 3;
        int queriesPerWindow = 4;
        for (int w = 0; w < windows; w++) {
            for (int q = 0; q < queriesPerWindow; q++) {
                mgr.snapshotFor("ks", "t", List.of("h1"));
                clock.advance(1); // stays within the window
            }
            clock.advance(WINDOW); // roll to the next window
        }

        int totalQueries = windows * queriesPerWindow;
        assertEquals(windows, mgr.snapshotCreationsTotal(),
                "flush-inducing create rate is W (one per window), not Q (one per query)");
        assertEquals(totalQueries - windows, mgr.snapshotReuseHitsTotal());
    }

    // ---- availableHosts reuse (#2241) ----------------------------------------------------------

    @Test
    void availableHostsReusesCurrentWindowWithoutCountingOrDuplicatePut() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        mgr.snapshotFor("ks", "t", List.of("10.0.0.2")); // primary (required) create
        Set<String> available = mgr.availableHosts("ks", "t", List.of("10.0.0.2", "10.0.0.9"));

        assertEquals(Set.of("10.0.0.2", "10.0.0.9"), available);
        assertEquals(1, fake.creates.stream().filter(c -> c.startsWith("10.0.0.2|")).count(),
                "the already-created host is not PUT again within the window");
        assertEquals(1, mgr.snapshotCreationsTotal(), "availableHosts does not re-count the same window");
        assertEquals(0, mgr.snapshotReuseHitsTotal(), "availableHosts is part of the same query's planning");
    }

    @Test
    void availableHostsExcludesFailedHostWithoutThrowing() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("10.0.0.9");
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        Set<String> available = mgr.availableHosts("ks", "t", List.of("10.0.0.2", "10.0.0.9"));

        assertEquals(Set.of("10.0.0.2"), available, "the failed host is excluded, not propagated");
    }

    // ---- Retirement / fail-closed edge cases ---------------------------------------------------

    @Test
    void retireAllClearsEveryLiveWindow() {
        FakeSidecars fake = new FakeSidecars();
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        mgr.snapshotFor("ks", "a", List.of("h1", "h2"));
        mgr.snapshotFor("ks", "b", List.of("h1"));
        mgr.retireAll();

        assertEquals(List.of("h1|ks.a/cqlite-ks-a-0", "h1|ks.b/cqlite-ks-b-0", "h2|ks.a/cqlite-ks-a-0"),
                fake.clears.stream().sorted().toList());
    }

    @Test
    void failsClosedOnCreateErrorAndRetiresNoPhantom() {
        FakeSidecars fake = new FakeSidecars();
        fake.failCreateHosts = Set.of("h1");
        SnapshotManager mgr = snapshotMgr(fake, new ManualClock());

        assertThrows(SidecarClient.SidecarException.class, () -> mgr.snapshotFor("ks", "t", List.of("h1")));

        // A failed create left no future, so retirement never deletes a phantom snapshot.
        mgr.retireAll();
        assertTrue(fake.clears.isEmpty(), "no phantom snapshot is cleared, got " + fake.clears);
    }

    @Test
    void retireSwallowsDeleteFailures() {
        HostSnapshotApis throwing = host -> new SnapshotApi() {
            @Override
            public void createSnapshot(String k, String t, String n, Optional<String> ttl) {}
            @Override
            public void clearSnapshot(String k, String t, String n) {
                throw new SidecarClient.SidecarException("delete failed", 500);
            }
        };
        SnapshotManager mgr = new SnapshotManager(throwing, ReadMode.SNAPSHOT, Optional.empty(), WINDOW, new ManualClock());
        mgr.snapshotFor("ks", "t", List.of("h1", "h2"));
        // Best-effort: a delete failure must not propagate (TTL backstop reclaims it).
        mgr.retireAll();
    }

    // ---- Concurrency (per-host exactly-once, off-lock create) ----------------------------------

    /**
     * Concurrent callers for the same {@code (window, host)} must PUT exactly once, even while the
     * (slow) create is in flight — the per-key-future memoization (#2113) moves the network call
     * off the ConcurrentHashMap bin lock but keeps exactly-once.
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
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            @Override
            public void clearSnapshot(String k, String t, String n) {}
        };
        SnapshotManager mgr = new SnapshotManager(blocking, ReadMode.SNAPSHOT, Optional.empty(), WINDOW, new ManualClock());

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<Optional<String>>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return mgr.snapshotFor("ks", "t", List.of("h1"));
            }));
        }
        start.countDown();
        Thread.sleep(50);
        release.countDown();

        for (java.util.concurrent.Future<Optional<String>> f : results) {
            assertEquals(Optional.of("cqlite-ks-t-0"), f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        assertEquals(1, creates.get(), "exactly one PUT for concurrent same-(window,host) callers");
    }

    /**
     * A winner that dies with a NON-RuntimeException throwable (an {@link Error}) must never leave
     * an incomplete future in the map (roborev on #2113): a concurrent waiter unblocks with a
     * failure — not hang on {@code join()} — and a subsequent caller retries with one new PUT.
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
        SnapshotManager mgr = new SnapshotManager(api, ReadMode.SNAPSHOT, Optional.empty(), WINDOW, new ManualClock());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<Optional<String>> winner =
                    pool.submit(() -> mgr.snapshotFor("ks", "t", List.of("h1")));
            assertTrue(winnerInCreate.await(5, TimeUnit.SECONDS), "winner reached the create");
            java.util.concurrent.Future<Optional<String>> waiter =
                    pool.submit(() -> mgr.snapshotFor("ks", "t", List.of("h1")));
            Thread.sleep(50);
            release.countDown();

            java.util.concurrent.ExecutionException winnerEx = assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> winner.get(5, TimeUnit.SECONDS));
            assertTrue(winnerEx.getCause() instanceof AssertionError,
                    "winner rethrows the original Error, got: " + winnerEx.getCause());
            java.util.concurrent.ExecutionException waiterEx = assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> waiter.get(5, TimeUnit.SECONDS));
            assertTrue(waiterEx.getCause() instanceof AssertionError,
                    "waiter surfaces the winner's Error, got: " + waiterEx.getCause());

            // The failed future was removed, so a retry recomputes and succeeds — one NEW PUT.
            assertEquals(Optional.of("cqlite-ks-t-0"), mgr.snapshotFor("ks", "t", List.of("h1")));
            assertEquals(2, creates.get(), "one failed PUT + one successful retry PUT");
        } finally {
            pool.shutdownNow();
        }
    }
}
