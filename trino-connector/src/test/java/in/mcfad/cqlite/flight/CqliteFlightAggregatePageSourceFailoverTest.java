package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
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
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replica failover on the aggregate finalize fan-out (issue #2241 roborev): the per-range DoGet
 * in {@link CqliteFlightAggregatePageSource#getNextSourcePage} uses the same shared {@link
 * ReplicaFailoverStream} as the scan path, so a down primary fails over to that range's next
 * replica before any of its partial rows are consumed; an all-down range fails the whole query
 * loudly (an aggregate partial must never be silently dropped); and a mid-stream failure after a
 * range's first batch is committed is NOT retried.
 */
class CqliteFlightAggregatePageSourceFailoverTest {
    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY)";
    private static final String AGGREGATION_JSON =
            "{\"group_by\":[],\"aggregates\":[{\"func\":\"Count\",\"column\":null,\"output\":\"agg0\"}]}";
    private static final String FINALIZE_JSON =
            "{\"group_by\":[],"
                    + "\"outputs\":[{\"result_name\":\"cnt\",\"kind\":\"DIRECT\",\"primary\":\"agg0\","
                    + "\"secondary\":null}]}";
    private static final List<CqliteFlightColumnHandle> COLUMNS =
            List.of(new CqliteFlightColumnHandle("cnt", BigintType.BIGINT));

    private static RuntimeException unavailable() {
        return CallStatus.UNAVAILABLE.withDescription("connection refused").toRuntimeException();
    }

    private static CqliteFlightSplit range(String host, List<String> fallbacks) {
        return new CqliteFlightSplit("ks", "t", DDL, host, 8815, -100L, 100L, false, Optional.empty(), fallbacks);
    }

    private static CqliteFlightAggregateSplit finalizeSplit(List<CqliteFlightSplit> ranges) {
        return new CqliteFlightAggregateSplit(
                "ks", "t", DDL, ranges, Optional.empty(), AGGREGATION_JSON, FINALIZE_JSON);
    }

    /** A batch stream yielding one count row once, optionally throwing at a chosen point. */
    private static final class FakeStream implements ReplicaFailoverStream.BatchStream {
        private final VectorSchemaRoot root;
        private final RuntimeException throwOnFirstNext;
        private final RuntimeException throwAfterFirstBatch;
        private int nextCalls;

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
                return true;
            }
            if (nextCalls == 2 && throwAfterFirstBatch != null) {
                throw throwAfterFirstBatch;
            }
            return false;
        }

        @Override
        public VectorSchemaRoot getRoot() {
            return root;
        }

        @Override
        public void close() {}
    }

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

    private static VectorSchemaRoot countRoot(BufferAllocator allocator, long count) {
        BigIntVector v = new BigIntVector("agg0", allocator);
        v.allocateNew(1);
        v.set(0, count);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(v));
        root.setRowCount(1);
        return root;
    }

    private static long resultCount(CqliteFlightAggregatePageSource source) {
        SourcePage page = source.getNextSourcePage();
        Page trinoPage = page.getPage();
        Block block = trinoPage.getBlock(0);
        return BigintType.BIGINT.getLong(block, 0);
    }

    @Test
    void rangeFailsOverToNextReplicaWhenPrimaryUnreachable() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot good = countRoot(allocator, 5L);
            var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                    "downHost", () -> new FakeStream(null, unavailable(), null),
                    "upHost", () -> new FakeStream(good, null, null))));
            CqliteFlightAggregateSplit split =
                    finalizeSplit(List.of(range("downHost", List.of("upHost"))));
            var source = new CqliteFlightAggregatePageSource(opener, split, COLUMNS);

            assertEquals(5L, resultCount(source), "the range's partial served by the fallback replica");
            assertEquals(1, opener.opens.get("downHost"), "primary attempted once");
            assertEquals(1, opener.opens.get("upHost"), "failed over to fallback exactly once");
            good.close();
        }
    }

    @Test
    void allReplicasOfARangeUnreachableFailsTheWholeQueryLoudly() {
        var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                "downA", () -> new FakeStream(null, unavailable(), null),
                "downB", () -> new FakeStream(null, unavailable(), null))));
        CqliteFlightAggregateSplit split =
                finalizeSplit(List.of(range("downA", List.of("downB"))));
        var source = new CqliteFlightAggregatePageSource(opener, split, COLUMNS);

        // Loud failure — an aggregate partial must never be silently dropped (CQLite doctrine).
        RuntimeException thrown = assertThrows(RuntimeException.class, source::getNextSourcePage);
        assertTrue(ReplicaFailover.isConnectClass(thrown));
        assertEquals(1, opener.opens.get("downA"));
        assertEquals(1, opener.opens.get("downB"), "every replica of the range attempted before failing");
    }

    @Test
    void doesNotFailOverOnMidStreamFailureAfterRangeRowsDelivered() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot first = countRoot(allocator, 1L);
            var opener = new TrackingOpener(new LinkedHashMap<>(Map.of(
                    // Primary delivers a batch, then dies UNAVAILABLE mid-stream.
                    "primary", () -> new FakeStream(first, null, unavailable()),
                    "fallback", () -> new FakeStream(countRoot(allocator, 1L), null, null))));
            CqliteFlightAggregateSplit split =
                    finalizeSplit(List.of(range("primary", List.of("fallback"))));
            var source = new CqliteFlightAggregatePageSource(opener, split, COLUMNS);

            // The mid-stream UNAVAILABLE (after the range's first batch was consumed by
            // accumulate()) must NOT trigger failover — retrying could duplicate rows.
            assertThrows(RuntimeException.class, source::getNextSourcePage);
            assertEquals(1, opener.opens.get("primary"));
            assertNull(opener.opens.get("fallback"), "a committed range must never retry a fallback");
            first.close();
        }
    }
}
