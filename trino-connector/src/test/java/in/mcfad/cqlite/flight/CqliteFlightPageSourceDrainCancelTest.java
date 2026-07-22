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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Early-close cancel on the scan page source (issue #2782, the P0 that reverted PR #2779; root-fixed
 * under #2680).
 *
 * <p><b>The real root cause.</b> The Arrow {@link org.apache.arrow.flight.FlightStream#next()} read
 * BLOCKS the calling thread on a {@code LinkedBlockingQueue.take()} until the server delivers the
 * next batch. The reverted code ran that read DIRECTLY on the Trino driver thread inside
 * {@code getNextSourcePage()}. When Trino satisfies a pushed {@code LIMIT} it cancels the surplus
 * splits from another thread ({@code Driver.close()}), but that close {@code tryLock}s and FAILS
 * while the driver thread is still pinned in a deadline-less {@code next()} — so {@code close()} (and
 * therefore {@link ReplicaFailoverStream#close()} → {@code FlightStream.cancel()}) was DEFERRED
 * forever and the query hung for the full 180s harness timeout. The close→cancel wiring the prior
 * three commits added was correct but could never RUN.
 *
 * <p><b>The fix.</b> {@link CqliteFlightPageSource} runs the blocking read on a background executor
 * and implements {@code isBlocked()}, so {@code getNextSourcePage()} NEVER blocks the driver thread
 * (it returns {@code null} while a fetch is in flight). Trino then runs {@code close()} on the freed
 * driver thread the instant the LIMIT is satisfied; {@code close()} cancels the active Flight stream,
 * which unblocks the parked background {@code next()} via the queue-sentinel the gRPC cancel
 * enqueues — non-blocking, idempotent, and it never drains the remaining batches.
 */
class CqliteFlightPageSourceDrainCancelTest {
    private static final byte[] TICKET = new byte[] {9, 9};
    private static final List<CqliteFlightColumnHandle> COLUMNS =
            List.of(new CqliteFlightColumnHandle("v", BigintType.BIGINT));

    /**
     * A batch stream whose {@code next()} BLOCKS (like a real {@link
     * org.apache.arrow.flight.FlightStream} parked on {@code queue.take()}) until {@code cancel()}
     * or {@code close()} releases it — modelling the exact wait the #2782 hang got stuck in. After
     * release it reports end-of-stream (the gRPC cancel delivers no more batches). It NEVER blocks
     * the caller after cancel, so a correct page source unblocks within the test's bound.
     */
    private static final class BlockingUntilCancelledStream implements ReplicaFailoverStream.BatchStream {
        private final CountDownLatch released = new CountDownLatch(1);
        volatile int cancelCalls;
        volatile int closeCalls;
        volatile boolean cancelledBeforeClosed;
        volatile boolean nextEntered;

        @Override
        public boolean next() {
            nextEntered = true;
            try {
                // Park exactly like FlightStream.next()'s queue.take(); a real cancel enqueues a
                // sentinel that unblocks it. Here cancel()/close() count down the latch.
                released.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false; // released → no further batch (the cancelled DoGet delivers nothing more)
        }

        @Override
        public VectorSchemaRoot getRoot() {
            return null;
        }

        @Override
        public void cancel() {
            if (closeCalls == 0) {
                cancelledBeforeClosed = true;
            }
            cancelCalls++;
            released.countDown(); // unpark a parked next(), exactly as FlightStream.cancel() does
        }

        @Override
        public void close() {
            closeCalls++;
            released.countDown();
        }
    }

    /** A batch stream that yields one batch per poll forever — a full drain would never end. */
    private static final class UnboundedStream implements ReplicaFailoverStream.BatchStream {
        private final VectorSchemaRoot root;
        volatile int cancelCalls;
        volatile int closeCalls;
        volatile boolean cancelledBeforeClosed;

        UnboundedStream(VectorSchemaRoot root) {
            this.root = root;
        }

        @Override
        public boolean next() {
            return true; // always another batch: draining to completion would never return
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

    // ---- The P0 #2782 root-cause proof (issue #2680) ---------------------------------------------

    /**
     * <b>The regression test for the real root cause.</b> A page source whose Flight read BLOCKS
     * (never yields a batch) is closed early — exactly what Trino does to a surplus split once a
     * pushed LIMIT is satisfied. Two properties must hold, both within a hard bound:
     *
     * <ol>
     *   <li>{@code getNextSourcePage()} must NOT block the calling (driver) thread even though the
     *       underlying {@code next()} blocks — it schedules the read on a background thread and
     *       returns {@code null}. On the reverted {@code b18b9a05} code this call blocks the driver
     *       thread forever inside {@code next()} and the {@code assertTimeoutPreemptively} FAILS.</li>
     *   <li>{@code close()} (running on the freed driver thread) cancels the active stream, which
     *       unblocks the parked background read — so the source finishes without a hang.</li>
     * </ol>
     */
    @Test
    void earlyCloseUnblocksAParkedReadWithoutPinningTheDriverThread() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            BlockingUntilCancelledStream blocking = new BlockingUntilCancelledStream();
            var source = new CqliteFlightPageSource(
                    List.of("host"), 8815, COLUMNS, TICKET, (h, p, t) -> blocking);

            // (1) The read blocks, but this poll must return promptly (fetch runs off-thread).
            assertNull(source.getNextSourcePage(),
                    "getNextSourcePage returns null while the background fetch is in flight — "
                            + "it must NOT block the driver thread in next() (the #2782 hang)");

            // Let the background fetch actually enter the blocking next() before we close.
            awaitNextEntered(blocking);

            // (2) Close from this (driver) thread — must cancel + unblock the parked read, not hang.
            source.close();

            assertTrue(source.isFinished(), "closed source reports finished");
            assertEquals(1, blocking.cancelCalls, "early close cancels the underlying DoGet exactly once");
            assertTrue(blocking.cancelledBeforeClosed, "cancel signalled (not a plain drain-then-close)");
        });
    }

    @Test
    void secondCloseIsANoOp() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            BlockingUntilCancelledStream blocking = new BlockingUntilCancelledStream();
            var source = new CqliteFlightPageSource(
                    List.of("host"), 8815, COLUMNS, TICKET, (h, p, t) -> blocking);
            assertNull(source.getNextSourcePage());
            awaitNextEntered(blocking); // the fetch published the stream before parking in next()

            source.close();
            source.close(); // idempotent: the wrapper cleared its reference after the first cancel

            assertEquals(1, blocking.cancelCalls, "cancel is issued once; a second close is a no-op");
        });
    }

    @Test
    void closeBeforeAnyPollDoesNotThrowAndDoesNotOpen() {
        // Trino may close a scheduled-but-never-started split (a satisfied LIMIT before this split
        // ran). No poll ever ran, so no stream was opened; close must be a quiet no-op.
        boolean[] opened = {false};
        var source = new CqliteFlightPageSource(
                List.of("host"), 8815, COLUMNS, TICKET,
                (h, p, t) -> {
                    opened[0] = true;
                    return new UnboundedStream(null);
                });

        source.close();

        assertTrue(source.isFinished());
        assertFalse(opened[0], "close before the first poll never opens a stream");
    }

    @Test
    void closeThenPollNeverOpensAnOrphanStream() {
        // close() arrived BEFORE this scheduled split's first poll. The page source is finished, so
        // a later getNextSourcePage() short-circuits and never schedules a fetch / opens a DoGet.
        boolean[] opened = {false};
        var source = new CqliteFlightPageSource(
                List.of("host"), 8815, COLUMNS, TICKET,
                (h, p, t) -> {
                    opened[0] = true;
                    return new UnboundedStream(null);
                });

        source.close();
        assertNull(source.getNextSourcePage(), "a poll after close finishes without scheduling a fetch");

        assertTrue(source.isFinished());
        assertFalse(opened[0], "close before the first poll must prevent any orphan stream open");
    }

    @Test
    void cancelReachesTheActiveFailoverStream() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
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

                // Drive one non-blocking poll cycle so a batch is served by the fallback.
                assertNull(source.getNextSourcePage()); // schedules the fetch off-thread
                SourcePage page = pollUntilPageOrFinished(source);
                assertTrue(page != null && page.getPositionCount() == 1,
                        "batch served by the fallback after failover");
                source.close();

                assertEquals(1, fallback.cancelCalls, "cancel reaches the ACTIVE (failed-over) stream");
                assertTrue(fallback.cancelledBeforeClosed);
                good.close();
            }
        });
    }

    // ---- ReplicaFailoverStream-level guards (driven directly, no page source) --------------------

    @Test
    void streamNextAfterCloseFinishes() {
        // After close(), next() must observe the stream-level `closed` flag and return false WITHOUT
        // opening a fresh stream — the guard that stops a surplus split's orphan DoGet.
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
        // getRoot() executing on the fetch thread. getRoot() must snapshot the volatile and return
        // null gracefully rather than NPE (issue #2680).
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = bigintRoot(allocator, 3L);
            UnboundedStream up = new UnboundedStream(root);
            var stream = new ReplicaFailoverStream(
                    List.of("host"), 8815, TICKET, (h, p, t) -> up);

            assertTrue(stream.next(), "first advance opens and publishes the stream");
            assertTrue(stream.getRoot() != null, "root available before close");

            stream.close(); // nulls the volatile `stream`

            assertNull(stream.getRoot(), "getRoot() after close returns null, never NPEs");
            root.close();
        }
    }

    /** Wait (bounded) for the background fetch to enter the blocking {@code next()}. */
    private static void awaitNextEntered(BlockingUntilCancelledStream blocking) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!blocking.nextEntered && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(blocking.nextEntered, "the background fetch entered the blocking next()");
    }

    /** Poll the (non-blocking) page source until it yields a page or finishes; bounded. */
    private static SourcePage pollUntilPageOrFinished(CqliteFlightPageSource source) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            SourcePage page = source.getNextSourcePage();
            if (page != null || source.isFinished()) {
                return page;
            }
            source.isBlocked().get(5, TimeUnit.SECONDS);
        }
        throw new AssertionError("page source neither yielded a page nor finished within the bound");
    }
}
