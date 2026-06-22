package com.rustyrazorblade.cqlite.flight;

import java.util.Locale;

/**
 * Operator control over GROUP BY aggregation pushdown (issue #893).
 *
 * <p>Global aggregates (no GROUP BY) are an unconditional win and are ALWAYS pushed,
 * independent of this policy. This setting governs only aggregates that carry a
 * {@code GROUP BY}, where the single-finalize-split design degrades to break-even (and
 * slightly negative on bytes) once the number of distinct groups approaches the row
 * count — see {@code docs/plans/2026-06-20-issue-841-aggregation-pushdown-benefit-eval.md}.
 *
 * <ul>
 *   <li>{@link #AUTOMATIC} — push when a cardinality estimate says the group count is
 *       comfortably below the row count; decline (let Trino aggregate locally) above the
 *       configured ratio. When no estimate is available the connector pushes, which is
 *       always correct and only risks the rare high-cardinality perf loss.</li>
 *   <li>{@link #ALWAYS} — always push supported GROUP BY shapes (the pre-#893 behavior).</li>
 *   <li>{@link #NEVER} — never push GROUP BY; Trino always aggregates locally.</li>
 * </ul>
 */
public enum GroupByPushdownPolicy {
    AUTOMATIC,
    ALWAYS,
    NEVER;

    /** Parse a catalog property value (case-insensitive); blank/null → {@link #AUTOMATIC}. */
    public static GroupByPushdownPolicy fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return AUTOMATIC;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid value for cqlite.aggregation-pushdown-group-by: '" + value
                            + "' (expected automatic, always, or never)");
        }
    }
}
