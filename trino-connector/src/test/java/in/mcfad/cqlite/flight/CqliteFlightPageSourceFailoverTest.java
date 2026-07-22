package in.mcfad.cqlite.flight;

import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replica failover on the scan page source (issue #2241): a down primary fails over to the next
 * replica that owns the range BEFORE any batch is delivered; all-down fails loudly (never a silent
 * empty result); and once rows have been delivered a later failure is NOT retried (retrying could
 * duplicate rows — a correctness bug).
 */
class CqliteFlightPageSourceFailoverTest {
    private static final byte[] TICKET = new byte[] {1, 2, 3};
    private static final List<CqliteFlightColumnHandle> COLUMNS =
            List.of(new CqliteFlightColumnHandle("v", BigintType.BIGINT));

    private static RuntimeException unavailable() {
        return CallStatus.UNAVAILABLE.withDescription("connection refused").toRuntimeException();
    }

    /** A batch stream that yields the given rows once, optionally throwing at a chosen point. */
    private static final class FakeStream implements ReplicaFailoverStream.BatchStream {
        private final VectorSchemaRoot root;
        private final RuntimeException throwOnFirstNext;
        private final RuntimeException throwAfterFirstBatch;
        private int nextCalls;
        boolean closed;

        FakeStream(VectorSchemaRoot root, RuntimeException throwOnFirstNext, RuntimeException throwAfterFirstBatch) {
            this.root = root;
            this.throwOnFirstNext = throwOnFirstNext;
            this.throwAfterFirstBatch = throwAfterFirstBatch;
        }

        @Override
        public boolean next() {
            nextCalls++;
            if (nextCalls == 1) {
                if (throwOnFirstNext != null) {
                    throw throwOnFirstNext;
                }
                return true; // deliver the single batch
            }
            if (nextCalls == 2 && throwAfterFirstBatch != null) {
                throw throwAfterFirstBatch; // mid-stream failure after a batch was delivered
            }
            return false; // end of stream
        }

        @Override
        public VectorSchemaRoot getRoot() {
            return root;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Tracks how many times each host was opened, so we can assert failover order + no over-reach. */
    private static final class TrackingOpener implements ReplicaFailoverStream.StreamOpener {
        private final Map<String, Supplier<ReplicaFailoverStream.BatchStream>> byHost;
        final Map<String, Integer> opens = new LinkedHashMap<>();

        TrackingOpener(Map<String, Supplier<ReplicaFailoverStream.BatchStream>> byHost) {
            this.byHost = byHost;
        }

        @Override
        public ReplicaFailoverStream.BatchStream open(String host, int port, byte[] ticket) {
            opens.merge(host, 1, Integer::sum);
            return byHost.get(host).get();
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

    private static List<Long> drain(CqliteFlightPageSource source) {
        List<Long> out = new ArrayList<>();
        SourcePage page;
        while (!source.isFinished()) {
            page = nextPage(source);
            if (page == null) {
                continue; // fetch scheduled/in-flight — drove isBlocked(); poll again
            }
            Block block = page.getBlock(0);
            for (int r = 0; r < page.getPositionCount(); r++) {
                out.add(BigintType.BIGINT.getLong(block, r));
            }
        }
        return out;
    }

    /**
     * Drive one non-blocking poll of the page source (issue #2680): the read runs on a background
     * thread, so {@code getNextSourcePage()} returns {@code null} while a fetch is in flight and the
     * driver would park on {@link CqliteFlightPageSource#isBlocked()}. This mirrors Trino's driver
     * loop deterministically — poll, and if a fetch is pending, block on the signal before the next
     * poll. A fetch ERROR is re-raised from the following {@code getNextSourcePage()}, exactly as
     * Trino would observe it. Bounded so a genuine hang fails the test rather than wedging it.
     */
    static SourcePage nextPage(CqliteFlightPageSource source) {
        SourcePage page = source.getNextSourcePage();
        if (page != null || source.isFinished()) {
            return page;
        }
        try {
            source.isBlocked().get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            // Fetch errors surface from getNextSourcePage(), not the blocked signal; ignore here.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted awaiting the fetch", e);
        }
        return source.getNextSourcePage();
    }

    @Test
    void failsOverToNextReplicaWhenPrimaryUnreachable() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot good = bigintRoot(allocator, 7L, 8L);
            var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                    "downHost", () -> new FakeStream(null, unavailable(), null),
                    "upHost", () -> new FakeStream(good, null, null))));
            var source = new CqliteFlightPageSource(
                    List.of("downHost", "upHost"), 8815, COLUMNS, TICKET, opener);

            assertEquals(List.of(7L, 8L), drain(source), "rows served by the fallback replica");
            assertTrue(source.isFinished());
            assertEquals(1, opener.opens.get("downHost"), "primary attempted once");
            assertEquals(1, opener.opens.get("upHost"), "failed over to fallback exactly once");
            good.close();
        }
    }

    @Test
    void failsOverWhenPrimaryOpenThrowsUnavailable() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot good = bigintRoot(allocator, 42L);
            var opener = new ReplicaFailoverStream.StreamOpener() {
                @Override
                public ReplicaFailoverStream.BatchStream open(String host, int port, byte[] ticket) {
                    if (host.equals("downHost")) {
                        throw unavailable(); // connection failed at establishment
                    }
                    return new FakeStream(good, null, null);
                }
            };
            var source = new CqliteFlightPageSource(
                    List.of("downHost", "upHost"), 8815, COLUMNS, TICKET, opener);
            assertEquals(List.of(42L), drain(source));
            good.close();
        }
    }

    @Test
    void allReplicasUnreachableFailsLoudly() {
        var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                "downA", () -> new FakeStream(null, unavailable(), null),
                "downB", () -> new FakeStream(null, unavailable(), null))));
        var source = new CqliteFlightPageSource(
                List.of("downA", "downB"), 8815, COLUMNS, TICKET, opener);

        // Loud failure — never a silent empty result (CQLite doctrine). The read runs on a
        // background thread now, so the connect-class failure surfaces when the driver re-polls
        // after the fetch completes exceptionally (issue #2680).
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> nextPage(source));
        assertTrue(ReplicaFailover.isConnectClass(thrown));
        assertTrue(source.isFinished());
        assertEquals(1, opener.opens.get("downA"));
        assertEquals(1, opener.opens.get("downB"), "every replica attempted before failing");
    }

    @Test
    void doesNotFailOverOnMidStreamFailureAfterRowsDelivered() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot first = bigintRoot(allocator, 1L);
            var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                    // Primary delivers a batch, then dies UNAVAILABLE mid-stream.
                    "primary", () -> new FakeStream(first, null, unavailable()),
                    "fallback", () -> new FakeStream(bigintRoot(allocator, 1L), null, null))));
            var source = new CqliteFlightPageSource(
                    List.of("primary", "fallback"), 8815, COLUMNS, TICKET, opener);

            // First page delivered from the primary.
            assertEquals(List.of(1L), drainOne(source));
            // The mid-stream UNAVAILABLE must NOT trigger failover (would duplicate the row). It
            // surfaces on the driver's next poll after the background fetch fails (issue #2680).
            assertThrows(RuntimeException.class, () -> nextPage(source));
            assertTrue(source.isFinished());
            assertEquals(1, opener.opens.get("primary"));
            assertNull(opener.opens.get("fallback"), "committed stream must never retry a fallback");
            first.close();
        }
    }

    private static List<Long> drainOne(CqliteFlightPageSource source) {
        SourcePage page = nextPage(source);
        List<Long> out = new ArrayList<>();
        Block block = page.getBlock(0);
        for (int r = 0; r < page.getPositionCount(); r++) {
            out.add(BigintType.BIGINT.getLong(block, r));
        }
        return out;
    }
}
