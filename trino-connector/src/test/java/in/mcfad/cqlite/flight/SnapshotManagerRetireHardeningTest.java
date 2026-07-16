package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot retirement hardening (issue #2452, follow-ups from PR #2425's endgame):
 *
 * <ol>
 *   <li><b>Ref-counted retirement on fail-closed rollback</b> (roborev job 1721). A fresh window
 *       whose per-host fan-out fails partway must actively retire the hardlinks it already created
 *       (no leak to the ~6h TTL) — EXCEPT when a concurrent query reused the same fresh window and
 *       committed it to its tickets, in which case deleting the shared hardlinks would strand that
 *       reader (NotFound). The window is reference-counted and its partial creates are retired only
 *       when no in-flight query still holds it.</li>
 *   <li><b>Grace-sweep offloaded off the split-planning path</b> (roborev job 1722) — the
 *       superseded-window DELETE fan-out no longer runs synchronously in {@code resolveSnapshot};
 *       it is submitted to a background best-effort {@link SnapshotRetireScheduler}. A periodic tick
 *       also prunes quiet tables that receive no further queries (the #2367 backlog fix).</li>
 * </ol>
 *
 * All timing is driven by an injected logical clock and a controllable scheduler — never wall-clock
 * or {@code sleep}-based (the #1742 pinned-{@code now} discipline).
 */
class SnapshotManagerRetireHardeningTest {

    private static final long WINDOW = 1_000L; // logical nanos
    private static final long GRACE = 5_000L;  // logical nanos

    /** A settable logical clock so window/grace timing is pinned deterministically. */
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

    /** Records every create/clear (prefixed with host) and can block/throw a chosen host's create. */
    private static final class FakeSidecars implements HostSnapshotApis {
        final List<String> creates = Collections.synchronizedList(new ArrayList<>());
        final List<String> clears = Collections.synchronizedList(new ArrayList<>());
        volatile String failHost;
        volatile CountDownLatch failHostReached;
        volatile CountDownLatch failHostRelease;

        @Override
        public SnapshotApi forHost(String host) {
            return new SnapshotApi() {
                @Override
                public void createSnapshot(String keyspace, String table, String name, Optional<String> ttl) {
                    if (host.equals(failHost)) {
                        if (failHostReached != null) {
                            failHostReached.countDown();
                        }
                        if (failHostRelease != null) {
                            try {
                                failHostRelease.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        throw new SidecarClient.SidecarException("boom on " + host, 500);
                    }
                    creates.add(host);
                }

                @Override
                public void clearSnapshot(String keyspace, String table, String name) {
                    clears.add(host + "/" + name);
                }
            };
        }
    }

    // ---- Item 1: ref-counted rollback retirement -----------------------------------------------

    /**
     * Uncontended rollback: a fresh window's fan-out fails on a later host, so the hardlinks it
     * already created on the earlier hosts are ACTIVELY retired (no leak to the TTL). This is the
     * behavior the naive endgame patch added; the ref-count makes it safe (proven by the race test
     * below).
     */
    @Test
    void uncontendedRollbackActivelyRetiresPartialCreates() {
        FakeSidecars fake = new FakeSidecars();
        fake.failHost = "h3";
        SnapshotManager mgr = new SnapshotManager(
                fake, ReadMode.SNAPSHOT, Optional.of("6h"), WINDOW, GRACE, new ManualClock());

        assertThrows(SidecarClient.SidecarException.class,
                () -> mgr.snapshotFor("ks", "t", List.of("h1", "h2", "h3")));

        // h1 and h2 were created then the window rolled back -> both are retired immediately; the
        // failing h3 created nothing (its future was removed on throw), so it is not cleared.
        assertEquals(Set.of("h1/cqlite-ks-t-0", "h2/cqlite-ks-t-0"),
                Set.copyOf(fake.clears),
                "the fresh window's partial creates are actively retired, no TTL leak: " + fake.clears);
    }

    /**
     * THE stranding race (issue #2452 item 1, warned about explicitly): query A creates a fresh
     * window W and fans out; concurrently query B REUSES the same fresh W and commits it to its
     * tickets; then A's fan-out fails on a later host. A's rollback must NOT delete W's shared
     * hardlinks — B is still holding them — or B strands on a NotFound. Deterministic via latches:
     * A blocks in the failing host's create until B has reused-and-committed W.
     */
    @Test
    void concurrentReuserIsNotStrandedByAnotherQueryRollback() throws Exception {
        FakeSidecars fake = new FakeSidecars();
        fake.failHost = "h3";
        fake.failHostReached = new CountDownLatch(1);
        fake.failHostRelease = new CountDownLatch(1);
        SnapshotManager mgr = new SnapshotManager(
                fake, ReadMode.SNAPSHOT, Optional.of("6h"), WINDOW, GRACE, new ManualClock());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            // Query A: creates W on h1, h2, then blocks in h3's failing create.
            java.util.concurrent.Future<?> a = pool.submit(() ->
                    assertThrows(SidecarClient.SidecarException.class,
                            () -> mgr.snapshotFor("ks", "t", List.of("h1", "h2", "h3"))));

            // Wait until A is inside h3's create — W is now live in the map and h1/h2 exist.
            assertTrue(fake.failHostReached.await(5, TimeUnit.SECONDS), "A reached the failing host");

            // Query B reuses the SAME fresh window W and commits its name (its ticket).
            String bName = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
            assertEquals("cqlite-ks-t-0", bName, "B reused A's fresh window");

            // Now let A fail and roll back.
            fake.failHostRelease.countDown();
            a.get(5, TimeUnit.SECONDS);

            // No stranding: A's rollback must NOT have deleted W's hardlinks — B still holds W.
            assertTrue(fake.clears.isEmpty(),
                    "a concurrent reuser must not be stranded — W's hardlinks stay intact: " + fake.clears);
            // And W is B's live, valid snapshot: B's create on h1 succeeded and was never cleared.
            assertTrue(fake.creates.contains("h1"), "B's snapshot on h1 exists: " + fake.creates);
        } finally {
            pool.shutdownNow();
        }
    }

    // ---- Item 2: grace-sweep offload + quiet-table periodic tick -------------------------------

    /** A scheduler that DEFERS submitted sweeps so the test controls exactly when they run. */
    private static final class DeferringScheduler implements SnapshotRetireScheduler {
        final Queue<Runnable> deferred = new ArrayDeque<>();
        Runnable periodic;

        @Override
        public void submitSweep(Runnable sweep) {
            deferred.add(sweep);
        }

        @Override
        public void startPeriodic(Runnable sweep) {
            this.periodic = sweep;
        }

        void drain() {
            Runnable r;
            while ((r = deferred.poll()) != null) {
                r.run();
            }
        }
    }

    /**
     * Item 2 offload: the superseded-window retirement DELETE does NOT run on the split-planning
     * path ({@code resolveSnapshot}) — it is handed to the background scheduler. Proven by a
     * deferring scheduler: right after the resolve that supersedes a past-grace window, nothing is
     * cleared; only draining the background task performs the retire.
     */
    @Test
    void graceSweepIsOffloadedOffThePlanningPath() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        DeferringScheduler scheduler = new DeferringScheduler();
        SnapshotManager mgr = new SnapshotManager(
                fake, ReadMode.SNAPSHOT, Optional.of("6h"), WINDOW, GRACE, clock, scheduler);

        String w0 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        // Elapse the window and resolve again: W0 is superseded and enqueued (grace starts now).
        clock.advance(WINDOW);
        String w1 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        assertTrue(!w0.equals(w1));
        // Let W0's grace fully elapse so it is DUE for retirement on the next sweep.
        clock.advance(GRACE);

        // The DELETE has NOT run inline on the planning path (it was offloaded/deferred) — even
        // though W0 is now past its grace, no sweep executed synchronously in resolveSnapshot.
        assertTrue(fake.clears.isEmpty(),
                "grace-sweep DELETE must not run synchronously in resolveSnapshot: " + fake.clears);

        // Draining the background scheduler runs the offloaded sweep, which retires the due window.
        scheduler.drain();
        assertTrue(fake.clears.stream().anyMatch(c -> c.contains(w0)),
                "the offloaded background sweep retires the due window: " + fake.clears);
    }

    /**
     * Item 2 quiet-table fix (#2367, 714-snapshot accumulation): a table that receives NO further
     * query after its window is superseded must still have its backlog pruned — by the periodic
     * background tick, not a query. Proven by invoking the captured periodic sweep after the grace
     * elapses, with no intervening query.
     */
    @Test
    void periodicTickPrunesQuietTableBacklogWithoutAQuery() {
        FakeSidecars fake = new FakeSidecars();
        ManualClock clock = new ManualClock();
        DeferringScheduler scheduler = new DeferringScheduler();
        SnapshotManager mgr = new SnapshotManager(
                fake, ReadMode.SNAPSHOT, Optional.of("6h"), WINDOW, GRACE, clock, scheduler);
        // start() registers the periodic hook — called explicitly post-construction (issue #2452
        // this-escape fix), mirroring how CqliteFlightConnector calls it after the SnapshotManager
        // instance is fully built.
        mgr.start();

        String w0 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow();
        clock.advance(WINDOW);
        String w1 = mgr.snapshotFor("ks", "t", List.of("h1")).orElseThrow(); // supersedes W0, enqueues
        scheduler.drain(); // any offloaded sweep runs; W0 age 0 < GRACE -> not retired
        assertFalse(w0.equals(w1));
        assertTrue(fake.clears.isEmpty(), "W0 still within grace: " + fake.clears);

        // The table goes quiet — NO further query. Time passes and the periodic tick fires.
        clock.advance(GRACE);
        scheduler.periodic.run();

        assertTrue(fake.clears.stream().anyMatch(c -> c.contains(w0)),
                "the periodic tick prunes the quiet table's superseded backlog: " + fake.clears);
    }

    // ---- Item 2: the production background scheduler actually runs work ------------------------

    @Test
    void backgroundSchedulerRunsSubmittedSweepAndPeriodicTickThenClosesCleanly() throws Exception {
        SnapshotRetireScheduler.BackgroundRetireScheduler scheduler =
                new SnapshotRetireScheduler.BackgroundRetireScheduler(20L);
        try {
            CountDownLatch submitted = new CountDownLatch(1);
            scheduler.submitSweep(submitted::countDown);
            assertTrue(submitted.await(5, TimeUnit.SECONDS), "submitted sweep runs on the background thread");

            CountDownLatch ticked = new CountDownLatch(1);
            scheduler.startPeriodic(ticked::countDown);
            assertTrue(ticked.await(5, TimeUnit.SECONDS), "the periodic tick fires");
        } finally {
            scheduler.close();
            scheduler.close(); // idempotent
        }
    }

    /**
     * Regression guard (issue #2452 item 3, roborev job 1753): a {@code submitSweep} call whose
     * executor has already been closed hits the {@code RejectedExecutionException} branch. That
     * branch must reset the coalescing {@code sweepQueued} flag exactly like a normal completed
     * sweep would — otherwise ANY submission racing (or following) a close() would permanently wedge
     * the flag at {@code true}, silently no-op-ing every future {@code submitSweep} for the rest of
     * the scheduler's life (indistinguishable in the field from the #2367 quiet-table accumulation
     * bug this issue fixes: sweeps are silently never queued again).
     */
    @Test
    void submitSweepAfterCloseDoesNotThrowAndDoesNotWedgeTheCoalescingFlag() {
        SnapshotRetireScheduler.BackgroundRetireScheduler scheduler =
                new SnapshotRetireScheduler.BackgroundRetireScheduler(1_000L);
        scheduler.close();

        assertDoesNotThrow(() -> scheduler.submitSweep(() -> { }),
                "submitSweep after close() must not throw (best-effort, TTL backstops it)");
        assertFalse(scheduler.sweepQueued.get(),
                "the RejectedExecutionException path must reset sweepQueued, not leave it wedged");

        // A second call after close must ALSO not throw and must ALSO not find the flag stuck —
        // proving the reset genuinely un-wedges future calls, not just a one-shot coincidence.
        assertDoesNotThrow(() -> scheduler.submitSweep(() -> { }),
                "a second post-close submitSweep must not throw either");
        assertFalse(scheduler.sweepQueued.get(),
                "sweepQueued must still not be wedged after a second post-close submission");
    }
}
