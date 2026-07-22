package in.mcfad.cqlite.flight;

import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
}
