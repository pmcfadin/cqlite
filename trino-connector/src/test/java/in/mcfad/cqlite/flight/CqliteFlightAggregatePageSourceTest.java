package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimeType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wiring evidence for issue #2105 on the aggregate finalize path (N1 review gap): the
 * finalize split's page source (see {@link CqliteFlightAggregatePageSource#getNextSourcePage})
 * builds one DoGet ticket per token range, and that ticket must carry the range's
 * {@link CqliteFlightSplit#snapshot()} through unchanged. {@link ReadModeWiringTest} already
 * covers this for the non-aggregated scan path; this covers the aggregate finalize path via
 * {@link CqliteFlightAggregatePageSource#buildRangeTicket}, the exact package-private method
 * {@code getNextSourcePage} calls in its per-range fan-out loop.
 *
 * <p>If that call site regressed to {@code Optional.empty()} instead of {@code range.snapshot()},
 * {@link #snapshotModeNamesSnapshotInTicket()} would fail (ticket's {@code snapshot} would be
 * null instead of {@code "cqlite-q1"}).
 */
class CqliteFlightAggregatePageSourceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY)";

    private static CqliteFlightSplit rangeWithSnapshot(Optional<String> snapshot) {
        return new CqliteFlightSplit("ks", "t", DDL, "10.0.0.2", 8815, -100L, 100L, false, snapshot);
    }

    @Test
    void snapshotModeNamesSnapshotInTicket() throws Exception {
        CqliteFlightSplit range = rangeWithSnapshot(Optional.of("cqlite-q1"));

        byte[] ticket = CqliteFlightAggregatePageSource.buildRangeTicket(range, null, null);
        JsonNode node = MAPPER.readTree(ticket);

        assertEquals("cqlite-q1", node.get("snapshot").asText());
    }

    @Test
    void liveModeLeavesSnapshotNullInTicket() throws Exception {
        CqliteFlightSplit range = rangeWithSnapshot(Optional.empty());

        byte[] ticket = CqliteFlightAggregatePageSource.buildRangeTicket(range, null, null);
        JsonNode node = MAPPER.readTree(ticket);

        assertTrue(node.get("snapshot").isNull());
    }

    /**
     * Issue #2229: a pushed-down {@code GROUP BY} on a TIME column must survive the
     * finalize path. This reproduces exactly what {@code accumulate()} + {@code
     * buildPage()} do to each range's Arrow batch — read every group/aggregate value
     * with {@link ArrowToTrino#readJavaValue} into the {@link PartialAggregateMerger},
     * then write each merged group's TIME key back with {@link ArrowToTrino#writeJavaValue}.
     * Before the fix, {@code readJavaValue} threw on the {@code TimeNanoVector} group
     * column and {@code writeJavaValue} had no TIME case; both would fail here.
     */
    @Test
    void groupByTimeColumnSurvivesFinalizeReadMergeWrite() {
        long nine = 9L * 3600 * 1_000_000_000L;  // 09:00:00
        long ten = 10L * 3600 * 1_000_000_000L;  // 10:00:00
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"));
        var merger = new PartialAggregateMerger(aggregates);

        try (BufferAllocator allocator = new RootAllocator()) {
            // Two "ranges": range A has groups {09:00 -> count 2, 10:00 -> count 1},
            // range B has {09:00 -> count 3}. Same TIME key must merge across ranges.
            accumulateBatch(allocator, merger, new long[] {nine, ten}, new long[] {2L, 1L});
            accumulateBatch(allocator, merger, new long[] {nine}, new long[] {3L});
        }

        // buildPage: write each merged group's TIME key + count into result blocks.
        Map<Long, Long> countByPicos = new HashMap<>();
        for (var g : merger.finish()) {
            long timeNanos = (Long) g.key().values().get(0);
            BlockBuilder timeBuilder = TimeType.TIME_NANOS.createBlockBuilder(null, 1);
            ArrowToTrino.writeJavaValue(TimeType.TIME_NANOS, timeNanos, timeBuilder);
            long picos = TimeType.TIME_NANOS.getLong(timeBuilder.build(), 0);

            BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, 1);
            ArrowToTrino.writeJavaValue(BigintType.BIGINT, g.outputs().get("agg0"), countBuilder);
            long count = BigintType.BIGINT.getLong(countBuilder.build(), 0);
            countByPicos.put(picos, count);
        }

        assertEquals(2, countByPicos.size(), "two distinct time-of-day groups");
        assertEquals(5L, countByPicos.get(nine * 1_000L), "09:00 group merged 2 + 3 across ranges");
        assertEquals(1L, countByPicos.get(ten * 1_000L), "10:00 group");
    }

    /**
     * Issue #2262: a projected group-by column absent from the delivered Arrow batch
     * (schema drift between the connector aggregation projection and the Flight server
     * response) must surface a clear error NAMING the column via the real
     * {@link CqliteFlightAggregatePageSource#accumulate} path — not silently produce
     * null group keys (a corrupted GROUP BY result set). Mirrors #2238's toPage guard.
     */
    @Test
    void missingGroupByVectorRaisesNamingTheColumn() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"));
        var spec = new AggregationSpec(List.of("missing_gc"), aggregates);
        var merger = new PartialAggregateMerger(aggregates);

        try (BufferAllocator allocator = new RootAllocator()) {
            // Batch delivers the aggregate output but NOT the projected group-by column.
            BigIntVector count = new BigIntVector("agg0", allocator);
            count.allocateNew(1);
            count.set(0, 5L);
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(count));
            root.setRowCount(1);

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> CqliteFlightAggregatePageSource.accumulate(root, spec, merger));
            assertTrue(ex.getMessage().contains("missing_gc"),
                    "error must name the missing group-by column, was: " + ex.getMessage());
            root.close();
        }
    }

    /**
     * Issue #2262 (aggregate-output arm): an aggregate-output vector absent from the
     * delivered batch must likewise error naming the column — not silently produce null
     * partial-aggregate values.
     */
    @Test
    void missingAggregateOutputVectorRaisesNamingTheColumn() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg_missing"));
        var spec = new AggregationSpec(List.of("gc"), aggregates);
        var merger = new PartialAggregateMerger(aggregates);

        try (BufferAllocator allocator = new RootAllocator()) {
            // Batch delivers the group-by column but NOT the aggregate output vector.
            BigIntVector gc = new BigIntVector("gc", allocator);
            gc.allocateNew(1);
            gc.set(0, 7L);
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(gc));
            root.setRowCount(1);

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> CqliteFlightAggregatePageSource.accumulate(root, spec, merger));
            assertTrue(ex.getMessage().contains("agg_missing"),
                    "error must name the missing aggregate-output column, was: " + ex.getMessage());
            root.close();
        }
    }

    /**
     * Guardrail (issue #2262, mirrors #2238): a null CELL within a PRESENT vector is
     * normal and must still yield a null value with NO error — only an ENTIRELY absent
     * vector errors. The group key is retained; the aggregate partial is a normal null.
     */
    @Test
    void nullCellWithinPresentVectorStaysNullNotError() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "v", "agg0"));
        var spec = new AggregationSpec(List.of("gc"), aggregates);
        var merger = new PartialAggregateMerger(aggregates);

        try (BufferAllocator allocator = new RootAllocator()) {
            BigIntVector gc = new BigIntVector("gc", allocator);
            BigIntVector agg0 = new BigIntVector("agg0", allocator);
            gc.allocateNew(1);
            agg0.allocateNew(1);
            gc.set(0, 42L);
            agg0.setNull(0); // null CELL within a PRESENT vector — a normal null, not drift
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(gc, agg0));
            root.setRowCount(1);

            // Must NOT throw: the present-vector null cell is normal null handling.
            CqliteFlightAggregatePageSource.accumulate(root, spec, merger);

            var groups = merger.finish();
            assertEquals(1, groups.size(), "one group survives with its non-null key");
            assertEquals(42L, groups.get(0).key().values().get(0), "group key read normally");
            assertTrue(groups.get(0).outputs().get("agg0") == null,
                    "sum of a single null partial stays null (no regression)");
            root.close();
        }
    }

    /** Mirror {@code accumulate()}: read one Arrow batch's group + count columns into the merger. */
    private static void accumulateBatch(
            BufferAllocator allocator, PartialAggregateMerger merger, long[] timeNanos, long[] counts) {
        TimeNanoVector time = new TimeNanoVector("t", allocator);
        BigIntVector count = new BigIntVector("agg0", allocator);
        time.allocateNew(timeNanos.length);
        count.allocateNew(counts.length);
        for (int i = 0; i < timeNanos.length; i++) {
            time.set(i, timeNanos[i]);
            count.set(i, counts[i]);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(time, count));
        root.setRowCount(timeNanos.length);
        for (int r = 0; r < timeNanos.length; r++) {
            Object key = ArrowToTrino.readJavaValue(root.getVector("t"), r);
            Map<String, Object> partials = new HashMap<>();
            partials.put("agg0", ArrowToTrino.readJavaValue(root.getVector("agg0"), r));
            merger.combine(new PartialAggregateMerger.GroupKey(List.of(key)), partials);
        }
        root.close();
    }
}
