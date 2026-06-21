package com.rustyrazorblade.cqlite.flight;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges the PARTIAL aggregate rows returned by each token range's cqlite-flight
 * endpoint into the FULLY MERGED result Trino expects (issue #841).
 *
 * <p>Trino does not re-aggregate across splits, so the finalize {@code PageSource}
 * pulls every range's partial (≈one row per group per range), feeds the rows here,
 * and the accumulated state becomes the final output.
 *
 * <p>Combine rules per server function:
 * <ul>
 *   <li>{@code Count} → sum (Int64, never null)</li>
 *   <li>{@code Sum} → sum, skipping null partials (null if every partial was null)</li>
 *   <li>{@code SumDouble} → double sum, skipping null (the avg numerator; never overflows)</li>
 *   <li>{@code Min} → min, skipping null</li>
 *   <li>{@code Max} → max, skipping null</li>
 * </ul>
 * {@code avg} is not a server function — it is computed downstream as ΣSum/ΣCount
 * (null when ΣCount == 0) from a {@code SumDouble}+{@code Count} pair (see
 * {@link FinalizeAggregationPlan}).
 *
 * <p>Grouping: rows are keyed by their group-by values (null keys group together,
 * so the key uses a sentinel-free equals/hashCode via {@link GroupKey}). A global
 * (empty group-by) aggregation has a single key; the server already returns exactly
 * one partial row per range even on empty input, so merging yields exactly one row.
 */
public final class PartialAggregateMerger {

    /** A merged accumulator for one server output column within one group. */
    public static final class Accumulator {
        final AggregationSpec.Func func;
        Long count;          // for Count: running total (0-based)
        Object value;        // for Sum/Min/Max: running value (Long or Double or Comparable)
        boolean seen;        // whether any non-null partial has been combined (Sum/Min/Max)

        Accumulator(AggregationSpec.Func func) {
            this.func = func;
            if (func == AggregationSpec.Func.Count) {
                this.count = 0L;
            }
        }
    }

    /** Hashable, null-tolerant group key built from the group-by column values. */
    public static final class GroupKey {
        private final List<Object> values;

        public GroupKey(List<Object> values) {
            this.values = values;
        }

        public List<Object> values() {
            return values;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof GroupKey k) || values.size() != k.values.size()) {
                return false;
            }
            for (int i = 0; i < values.size(); i++) {
                if (!deepEquals(values.get(i), k.values.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (Object v : values) {
                h = 31 * h + deepHashCode(v);
            }
            return h;
        }

        // VARBINARY/blob group keys arrive as byte[], whose natural equals/hashCode
        // are identity-based — the same blob key from two ranges would otherwise
        // form separate groups and double-emit. Compare/hash byte[] by content.
        private static boolean deepEquals(Object a, Object b) {
            if (a instanceof byte[] ab && b instanceof byte[] bb) {
                return java.util.Arrays.equals(ab, bb);
            }
            return java.util.Objects.equals(a, b);
        }

        private static int deepHashCode(Object v) {
            return (v instanceof byte[] b) ? java.util.Arrays.hashCode(b) : java.util.Objects.hashCode(v);
        }
    }

    // group -> (output name -> accumulator). LinkedHashMap to keep deterministic order.
    private final Map<GroupKey, Map<String, Accumulator>> groups = new LinkedHashMap<>();
    private final List<AggregationSpec.Aggregate> aggregates;

    public PartialAggregateMerger(List<AggregationSpec.Aggregate> aggregates) {
        this.aggregates = aggregates;
    }

    /**
     * Combine one partial row.
     *
     * @param key            the group-by values for the row (empty list for global)
     * @param partialsByOutput map of server output name → partial value for this row
     *                         ({@code Long} for Count, {@code Long}/{@code Double} for
     *                         Sum, the source value for Min/Max, {@code null} if the
     *                         server reported null for that aggregate)
     */
    public void combine(GroupKey key, Map<String, Object> partialsByOutput) {
        Map<String, Accumulator> accs = groups.computeIfAbsent(key, k -> {
            Map<String, Accumulator> m = new LinkedHashMap<>();
            for (AggregationSpec.Aggregate a : aggregates) {
                m.put(a.output(), new Accumulator(a.func()));
            }
            return m;
        });
        for (AggregationSpec.Aggregate a : aggregates) {
            Accumulator acc = accs.get(a.output());
            Object partial = partialsByOutput.get(a.output());
            combineInto(acc, partial);
        }
    }

    private static void combineInto(Accumulator acc, Object partial) {
        switch (acc.func) {
            case Count -> {
                // Count partials are never null; treat null defensively as 0.
                long c = (partial == null) ? 0L : ((Number) partial).longValue();
                acc.count += c;
            }
            case Sum -> {
                if (partial == null) {
                    return;
                }
                if (acc.value == null) {
                    acc.value = partial;
                } else if (partial instanceof Double || acc.value instanceof Double) {
                    acc.value = ((Number) acc.value).doubleValue() + ((Number) partial).doubleValue();
                } else {
                    // Integer sum: addExact throws on i64 overflow rather than
                    // wrapping, matching Trino's non-pushed bigint sum (a wrapped
                    // value would be silently wrong). The ArithmeticException
                    // surfaces as a query failure.
                    acc.value = Math.addExact(((Number) acc.value).longValue(), ((Number) partial).longValue());
                }
                acc.seen = true;
            }
            case SumDouble -> {
                // The avg numerator: always accumulate in double, so the running
                // total never overflows (matches Trino's non-overflowing avg).
                if (partial == null) {
                    return;
                }
                double next = (acc.value == null ? 0.0 : ((Number) acc.value).doubleValue())
                        + ((Number) partial).doubleValue();
                acc.value = next;
                acc.seen = true;
            }
            case Min -> {
                if (partial == null) {
                    return;
                }
                if (acc.value == null || compareValues(partial, acc.value) < 0) {
                    acc.value = partial;
                }
                acc.seen = true;
            }
            case Max -> {
                if (partial == null) {
                    return;
                }
                if (acc.value == null || compareValues(partial, acc.value) > 0) {
                    acc.value = partial;
                }
                acc.seen = true;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            // Integral types (BIGINT/counter values can exceed 2^53) must compare
            // exactly — doubleValue() would lose precision and pick the wrong
            // extreme. Only fall back to floating comparison for float/double.
            if (isIntegral(na) && isIntegral(nb)) {
                return Long.compare(na.longValue(), nb.longValue());
            }
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        // Text min/max must order by UTF-8 byte order to match the server (Rust
        // String Ord) and Trino VARCHAR — NOT String.compareTo, whose UTF-16 code
        // -unit order disagrees for supplementary (non-BMP) characters.
        if (a instanceof String sa && b instanceof String sb) {
            return compareUtf8(sa, sb);
        }
        return ((Comparable) a).compareTo(b);
    }

    /** Lexicographic comparison of two strings by unsigned UTF-8 bytes. */
    private static int compareUtf8(String a, String b) {
        byte[] ab = a.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int n = Math.min(ab.length, bb.length);
        for (int i = 0; i < n; i++) {
            int cmp = Integer.compare(ab[i] & 0xFF, bb[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(ab.length, bb.length);
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    /** A finished group: its key and the merged value for each server output name. */
    public record MergedGroup(GroupKey key, Map<String, Object> outputs) {}

    /**
     * Snapshot the merged groups. For each output: Count → its {@code Long} total;
     * Sum/Min/Max → the running value, or {@code null} if no non-null partial was seen.
     */
    public List<MergedGroup> finish() {
        List<MergedGroup> result = new ArrayList<>();
        for (Map.Entry<GroupKey, Map<String, Accumulator>> e : groups.entrySet()) {
            Map<String, Object> outputs = new LinkedHashMap<>();
            for (Map.Entry<String, Accumulator> oe : e.getValue().entrySet()) {
                Accumulator acc = oe.getValue();
                if (acc.func == AggregationSpec.Func.Count) {
                    outputs.put(oe.getKey(), acc.count);
                } else {
                    outputs.put(oe.getKey(), acc.seen ? acc.value : null);
                }
            }
            result.add(new MergedGroup(e.getKey(), outputs));
        }
        return result;
    }
}
