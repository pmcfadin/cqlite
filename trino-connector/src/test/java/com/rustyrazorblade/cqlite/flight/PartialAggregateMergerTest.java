package com.rustyrazorblade.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the partial-merge logic ({@link PartialAggregateMerger}): feed two
 * partial batches/group maps and assert merged count/sum/min/max, and the avg
 * (ΣSum/ΣCount) derived downstream.
 */
class PartialAggregateMergerTest {

    private static PartialAggregateMerger.GroupKey key(Object... values) {
        return new PartialAggregateMerger.GroupKey(List.of(values));
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void mergesGlobalCountSumMinMax() {
        // count(*)=agg0, sum(x)=agg1, min(x)=agg2, max(x)=agg3.
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "x", "agg1"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Min, "x", "agg2"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Max, "x", "agg3"));
        var merger = new PartialAggregateMerger(aggregates);

        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", 3L, "agg1", 10L, "agg2", 2L, "agg3", 7L));
        merger.combine(globalKey, row("agg0", 2L, "agg1", 5L, "agg2", 1L, "agg3", 9L));

        var groups = merger.finish();
        assertEquals(1, groups.size(), "global aggregation has one group");
        var out = groups.get(0).outputs();
        assertEquals(5L, out.get("agg0"), "count = 3 + 2");
        assertEquals(15L, out.get("agg1"), "sum = 10 + 5");
        assertEquals(1L, out.get("agg2"), "min(2, 1)");
        assertEquals(9L, out.get("agg3"), "max(7, 9)");
    }

    @Test
    void sumMinMaxNullWhenNoNonNullInputs() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "x", "agg1"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Min, "x", "agg2"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        // Both partials report null sum/min (every range had only null x).
        merger.combine(globalKey, row("agg0", 0L, "agg1", null, "agg2", null));
        merger.combine(globalKey, row("agg0", 0L, "agg1", null, "agg2", null));

        var out = merger.finish().get(0).outputs();
        assertEquals(0L, out.get("agg0"), "count never null");
        assertNull(out.get("agg1"), "sum null when no non-null inputs");
        assertNull(out.get("agg2"), "min null when no non-null inputs");
    }

    @Test
    void mergesGroupedByKeyWithNullKeyTogether() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"));
        var merger = new PartialAggregateMerger(aggregates);

        merger.combine(key("a"), row("agg0", 2L));
        merger.combine(key("b"), row("agg0", 1L));
        merger.combine(key("a"), row("agg0", 3L));
        // Null group key must group together across partials.
        var nullKey = new PartialAggregateMerger.GroupKey(java.util.Arrays.asList((Object) null));
        merger.combine(nullKey, row("agg0", 4L));
        merger.combine(nullKey, row("agg0", 1L));

        Map<Object, Long> byKey = new HashMap<>();
        for (var g : merger.finish()) {
            byKey.put(g.key().values().get(0), (Long) g.outputs().get("agg0"));
        }
        assertEquals(5L, byKey.get("a"), "a: 2 + 3");
        assertEquals(1L, byKey.get("b"));
        assertEquals(5L, byKey.get(null), "null key: 4 + 1 grouped together");
    }

    @Test
    void avgDerivedFromSumAndCount() {
        // avg(x) carried as Sum(x)=agg0 + Count(x)=agg1; downstream computes Σsum/Σcount.
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "x", "agg0"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, "x", "agg1"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", 10L, "agg1", 2L));
        merger.combine(globalKey, row("agg0", 5L, "agg1", 3L));

        var out = merger.finish().get(0).outputs();
        long sum = ((Number) out.get("agg0")).longValue();
        long count = ((Number) out.get("agg1")).longValue();
        assertEquals(15L, sum);
        assertEquals(5L, count);
        assertEquals(3.0, (double) sum / count, 1e-9, "avg = 15 / 5");
    }

    @Test
    void minMaxKeepBigintPrecisionAbove2Pow53() {
        // Values that are indistinguishable as doubles (2^53 and 2^53+1) must
        // compare exactly so min/max pick the right extreme across ranges.
        long lo = (1L << 53);       // 9007199254740992
        long hi = (1L << 53) + 1;   // 9007199254740993 — same double as lo
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Min, "x", "agg0"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Max, "x", "agg1"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", hi, "agg1", lo));
        merger.combine(globalKey, row("agg0", lo, "agg1", hi));

        var out = merger.finish().get(0).outputs();
        assertEquals(lo, out.get("agg0"), "min keeps the smaller bigint exactly");
        assertEquals(hi, out.get("agg1"), "max keeps the larger bigint exactly");
    }

    @Test
    void byteArrayGroupKeysMergeByContent() {
        // VARBINARY/blob group keys arrive as byte[]; identity-based equality would
        // split the same key across ranges. Equal content must merge into one group.
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Count, null, "agg0"));
        var merger = new PartialAggregateMerger(aggregates);
        var k1 = new PartialAggregateMerger.GroupKey(List.of((Object) new byte[] {1, 2, 3}));
        var k2 = new PartialAggregateMerger.GroupKey(List.of((Object) new byte[] {1, 2, 3}));
        var other = new PartialAggregateMerger.GroupKey(List.of((Object) new byte[] {9}));
        merger.combine(k1, row("agg0", 2L));
        merger.combine(k2, row("agg0", 3L));
        merger.combine(other, row("agg0", 1L));

        var groups = merger.finish();
        assertEquals(2, groups.size(), "equal-content byte[] keys merge into one group");
        long total = groups.stream()
                .filter(g -> g.key().values().get(0) instanceof byte[] b && b.length == 3)
                .mapToLong(g -> (Long) g.outputs().get("agg0")).sum();
        assertEquals(5L, total, "the {1,2,3} group merged 2 + 3");
    }

    @Test
    void integerSumOverflowThrowsNotWraps() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "x", "agg0"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", Long.MAX_VALUE));
        // Combining another range's partial overflows i64 — must throw, not wrap
        // (matches Trino's non-pushed bigint sum).
        org.junit.jupiter.api.Assertions.assertThrows(ArithmeticException.class,
                () -> merger.combine(globalKey, row("agg0", 1L)));
    }

    @Test
    void textMinMaxUsesUtf8ByteOrder() {
        // "￿" (UTF-8 EF BF BF) vs "𐀀" = U+10000 (UTF-8 F0 90 80 80).
        // UTF-16 String.compareTo orders the supplementary char FIRST (surrogate
        // 0xD800 < 0xFFFF); UTF-8 byte order puts "￿" first. The server/Trino
        // use UTF-8, so min must be "￿".
        String bmp = "￿";
        String supp = "𐀀";
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Min, "s", "agg0"),
                new AggregationSpec.Aggregate(AggregationSpec.Func.Max, "s", "agg1"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", supp, "agg1", supp));
        merger.combine(globalKey, row("agg0", bmp, "agg1", bmp));

        var out = merger.finish().get(0).outputs();
        assertEquals(bmp, out.get("agg0"), "UTF-8 min is \\uFFFF, not the supplementary char");
        assertEquals(supp, out.get("agg1"), "UTF-8 max is the supplementary char");
    }

    @Test
    void mergesDoubleSums() {
        var aggregates = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Func.Sum, "x", "agg0"));
        var merger = new PartialAggregateMerger(aggregates);
        var globalKey = new PartialAggregateMerger.GroupKey(List.of());
        merger.combine(globalKey, row("agg0", 1.5));
        merger.combine(globalKey, row("agg0", 2.25));
        assertEquals(3.75, (Double) merger.finish().get(0).outputs().get("agg0"), 1e-9);
    }
}
