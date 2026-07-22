package in.mcfad.cqlite.flight;

import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Early-close drain cancel on the scan page source (issue #2782, the P0 that reverted PR #2779).
 *
 * <p>When Trino satisfies a pushed {@code LIMIT} it CANCELS the remaining splits and closes their
 * page sources EARLY — while server batches are still queued and undrained. The prior code merely
 * released the handle ({@code handle.close()}) and the gRPC {@code DoGet} kept waiting on the
 * unconsumed batches, hanging the query for the full 180s harness timeout. The fix explicitly
 * CANCELS the active underlying Flight stream on early close: idempotent, non-blocking, and
 * propagated through {@link ReplicaFailoverStream} to the currently-active underlying stream. This
 * holds at ANY split count (K=1 included), so an un-consumed stream can never block completion.
 */
class CqliteFlightPageSourceDrainCancelTest {
    private static final byte[] TICKET = new byte[] {9, 9};
    private static final List<CqliteFlightColumnHandle> COLUMNS =
            List.of(new CqliteFlightColumnHandle("v", BigintType.BIGINT));

    /** A batch stream that never ends, so a full drain would block forever — cancel must break it. */
    private static final class UnboundedStream implements ReplicaFailoverStream.BatchStream {
        private final VectorSchemaRoot root;
        int cancelCalls;
        int closeCalls;
        boolean cancelledBeforeClosed;

        UnboundedStream(VectorSchemaRoot root) {
            this.root = root;
        }

        @Override
        public boolean next() {
            // Always another batch: draining this to completion would never return — the whole
            // point of the #2782 hang. An early cancel must NOT drain.
            return true;
        }

        @Override
        public VectorSchemaRoot getRoot() {
            return root;
        }

        @Override
        public void cancel() {
            if (closeCalls == 0) {
                cancelledBeforeClosed = true;
            }
            cancelCalls++;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static VectorSchemaRoot bigintRoot(BufferAllocator allocator, long... values) {
        BigIntVector v = new BigIntVector("v", allocator);
        v.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            v.set(i, values[i]);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(v));
        root.setRowCount(values.length);
        return root;
    }

    @Test
    void earlyCloseCancelsUndrainedStreamWithoutBlocking() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = bigintRoot(allocator, 1L);
            UnboundedStream up = new UnboundedStream(root);
            var source = new CqliteFlightPageSource(
                    List.of("host"), 8815, COLUMNS, TICKET, (h, p, t) -> up);

            // Pull ONE page (some but not all batches delivered), then close early like Trino does
            // once a LIMIT is satisfied. close() must CANCEL the active stream and return — a
            // drain-to-end would spin on the unbounded stream forever (the #2782 hang).
            SourcePage first = source.getNextSourcePage();
            assertNotNull(first, "first page delivered before the early close");

            source.close(); // must not block on the never-ending stream

            assertTrue(source.isFinished(), "closed source reports finished");
            assertEquals(1, up.cancelCalls, "early close cancels the underlying DoGet exactly once");
            assertTrue(up.cancelledBeforeClosed, "cancel signalled (not a plain drain-then-close)");
            root.close();
        }
    }

    @Test
    void secondCloseIsANoOp() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = bigintRoot(allocator, 5L);
            UnboundedStream up = new UnboundedStream(root);
            var source = new CqliteFlightPageSource(
                    List.of("host"), 8815, COLUMNS, TICKET, (h, p, t) -> up);
            assertNotNull(source.getNextSourcePage());

            source.close();
            source.close(); // idempotent: the wrapper cleared its reference after the first cancel

            assertEquals(1, up.cancelCalls, "cancel is issued once; a second close is a harmless no-op");
            root.close();
        }
    }

    @Test
    void closeBeforeAnyPageDoesNotThrowAndDoesNotOpen() {
        // Trino may close a scheduled-but-never-started split (a satisfied LIMIT before this split
        // ran). No underlying stream was ever opened, so close must be a quiet no-op.
        Map<String, Supplier<ReplicaFailoverStream.BatchStream>> byHost = new LinkedHashMap<>();
        boolean[] opened = {false};
        var source = new CqliteFlightPageSource(
                List.of("host"), 8815, COLUMNS, TICKET,
                (h, p, t) -> {
                    opened[0] = true;
                    return new UnboundedStream(null);
                });

        source.close();

        assertTrue(source.isFinished());
        assertFalse(opened[0], "close before the first getNextSourcePage never opens a stream");
    }

    // ---- Surplus-split early-close races (issue #2680 / P0 #2782, the intermittent K>1 hang) ----
    // At the default K=4 a residual-LIMIT scan fans out to surplus splits; Trino cancels and closes
    // them the instant the LIMIT is satisfied — from a DIFFERENT thread than the driver advancing
    // the stream. If a close is a no-op because the stream is not yet opened (or is mid-open), the
    // split's DoGet gRPC never gets cancelled and blocks on undrained batches → the 180s hang. These
    // reproduce each racing state deterministically so a regression fails in ./gradlew build.

    @Test
    void closeThenNextNeverOpensAnOrphanStream() {
        // close() arrived BEFORE this scheduled split's first getNextSourcePage() ran. This covers
        // the OUTER page-source guard: close() marks the page source finished, so a later
        // getNextSourcePage() short-circuits and never opens a fresh (uncancellable) DoGet. The
        // stream-level `closed` guard is exercised directly in streamNextAfterCloseFinishes below.
        boolean[] opened = {false};
        var source = new CqliteFlightPageSource(
                List.of("host"), 8815, COLUMNS, TICKET,
                (h, p, t) -> {
                    opened[0] = true;
                    return new UnboundedStream(null);
                });

        source.close();
        assertNull(source.getNextSourcePage(), "a next() after close finishes without opening a stream");

        assertTrue(source.isFinished());
        assertFalse(opened[0], "close before the first advance must prevent any orphan stream open");
    }

    @Test
    void closeRacingAnInFlightOpenCancelsTheJustOpenedStream() {
        // The hardest race: the driver thread is INSIDE opener.open() when Trino's cancel thread
        // calls close(). close() sees stream == null (not yet published) and cancels nothing; the
        // open then completes and publishes a live stream. Without the post-publish re-check that
        // stream is an orphaned, uncancelled DoGet — the exact intermittent #2782 hang. The fix's
        // double-check must cancel it and next() must return false rather than block on the
        // never-ending stream.
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            try (BufferAllocator allocator = new RootAllocator()) {
                VectorSchemaRoot root = bigintRoot(allocator, 1L);
                UnboundedStream up = new UnboundedStream(root);
                CountDownLatch insideOpen = new CountDownLatch(1);
                CountDownLatch closeIssued = new CountDownLatch(1);
                AtomicReference<Boolean> hadNext = new AtomicReference<>();

                var source = new CqliteFlightPageSource(
                        List.of("host"), 8815, COLUMNS, TICKET,
                        (h, p, t) -> {
                            // Signal we are mid-open, then wait until close() has run so the open
                            // deterministically publishes AFTER the close set its flag.
                            insideOpen.countDown();
                            await(closeIssued);
                            return up;
                        });

                Thread driver = new Thread(() -> hadNext.set(source.getNextSourcePage() != null));
                driver.start();
                await(insideOpen);   // driver is blocked inside opener.open()
                source.close();      // close BEFORE the open publishes the stream
                closeIssued.countDown();
                driver.join(Duration.ofSeconds(5).toMillis());

                assertFalse(driver.isAlive(), "the driver's next() returned — no hang on the orphan stream");
                assertEquals(Boolean.FALSE, hadNext.get(), "next() observes the close and yields no page");
                assertEquals(1, up.cancelCalls, "the stream opened during the close is cancelled, not orphaned");
                assertTrue(source.isFinished());
                root.close();
            }
        });
    }

    @Test
    void cancelReachesTheActiveFailoverStream() {
        try (BufferAllocator allocator = new RootAllocator()) {
            // The primary is down: the failover wrapper opens it, fails over (connect-class) to
            // the fallback, delivers a batch from the fallback, then the operator closes early.
            // The cancel must reach the ACTIVE (fallback) stream — not the dead primary.
            VectorSchemaRoot good = bigintRoot(allocator, 7L);
            UnboundedStream fallback = new UnboundedStream(good);
            var opener = new ReplicaFailoverStream.StreamOpener() {
                @Override
                public ReplicaFailoverStream.BatchStream open(String host, int port, byte[] ticket) {
                    if (host.equals("down")) {
                        throw CallStatus.UNAVAILABLE.withDescription("refused").toRuntimeException();
                    }
                    return fallback;
                }
            };
            var source = new CqliteFlightPageSource(
                    List.of("down", "up"), 8815, COLUMNS, TICKET, opener);

            assertNotNull(source.getNextSourcePage(), "batch served by the fallback after failover");
            source.close();

            assertEquals(1, fallback.cancelCalls, "cancel reaches the ACTIVE (failed-over) stream");
            assertTrue(fallback.cancelledBeforeClosed);
            good.close();
        }
    }

    @Test
    void streamNextAfterCloseFinishes() {
        // Drives ReplicaFailoverStream directly (no page-source finished short-circuit) to exercise
        // the stream-level `closed` guard: after close(), next() must observe the flag and return
        // false WITHOUT opening a fresh stream — the guard that stops a surplus split's orphan DoGet.
        boolean[] opened = {false};
        var stream = new ReplicaFailoverStream(
                List.of("host"), 8815, TICKET,
                (h, p, t) -> {
                    opened[0] = true;
                    return new UnboundedStream(null);
                });

        stream.close();

        assertFalse(stream.next(), "next() after close observes the stream-level closed flag");
        assertFalse(opened[0], "a closed stream never opens an underlying DoGet");
    }

    @Test
    void getRootAfterCloseNulledStreamDoesNotThrow() {
        // close() (cancel thread) can null the underlying stream between next() returning true and
        // getRoot() executing on the driver thread. getRoot() must snapshot the volatile and return
        // null gracefully rather than NPE (issue #2680). Deterministic single-thread reproduction:
        // advance, then close (nulls the stream), then call getRoot().
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = bigintRoot(allocator, 3L);
            UnboundedStream up = new UnboundedStream(root);
            var stream = new ReplicaFailoverStream(
                    List.of("host"), 8815, TICKET, (h, p, t) -> up);

            assertTrue(stream.next(), "first advance opens and publishes the stream");
            assertNotNull(stream.getRoot(), "root available before close");

            stream.close(); // nulls the volatile `stream`

            assertNull(stream.getRoot(), "getRoot() after close returns null, never NPEs");
            root.close();
        }
    }

    /** A stream that reports a batch is ready but whose root snapshot is already null (cancel race). */
    private static final class NullRootAfterNextStream implements ReplicaFailoverStream.BatchStream {
        int nextCalls;
        int cancelCalls;

        @Override
        public boolean next() {
            nextCalls++;
            return true; // a batch is "ready" ...
        }

        @Override
        public VectorSchemaRoot getRoot() {
            return null; // ... but the cancel thread already nulled the active stream (getRoot snapshot)
        }

        @Override
        public void cancel() {
            cancelCalls++;
        }

        @Override
        public void close() {}
    }

    @Test
    void nullRootFromCancelRaceFinishesInsteadOfNpe() {
        // Routes the cancel-race null root THROUGH CqliteFlightPageSource.getNextSourcePage() (not
        // getRoot() in isolation): close() (Trino's cancel thread) nulled the active stream between
        // next() returning true and the getRoot() read. getRoot() returns null; the page source must
        // treat that as end-of-stream and return null rather than NPE inside ArrowToTrino.toPage(null)
        // (issues #2782/#2680 — the fix that eliminates the relocated NPE, not just moves it).
        NullRootAfterNextStream up = new NullRootAfterNextStream();
        var source = new CqliteFlightPageSource(
                List.of("host"), 8815, COLUMNS, TICKET, (h, p, t) -> up);

        SourcePage page = source.getNextSourcePage();

        assertNull(page, "a null root on the cancel race yields no page rather than NPE'ing");
        assertTrue(source.isFinished(), "the page source finishes on the cancel-race null root");
        assertEquals(0, source.getCompletedBytes(), "no positions counted for the discarded batch");
        assertNull(source.getNextSourcePage(), "a subsequent poll stays finished");
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS), "latch reached in time");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted awaiting latch", e);
        }
    }
}
