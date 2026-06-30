package in.mcfad.cqlite.flight;

import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CqliteFlightMetadata#getTableStatistics} and the
 * {@link CqliteFlightTableHandle#hasGroupBy()} signal it relies on (issue #944).
 *
 * <p>After aggregation pushdown the scan's OUTPUT cardinality is the aggregate
 * result cardinality, NOT the base-table row count: a GLOBAL aggregate emits one
 * row; a grouped aggregate's group count is unknown. These tests pin that the
 * optimizer-facing stats reflect the output, not the underlying SSTable rows.
 *
 * <p>A NON-aggregated handle now reports {@link TableStatistics#empty()} (unknown):
 * the {@code table_stats} row total is summed across ALL keyspace replicas (≈ RF ×
 * logical cardinality) and is not de-duplicated to one copy of the token space, so
 * we deliberately do not expose it as an optimizer row count (issue #944). Both
 * branches of {@code getTableStatistics} now return before touching Sidecar/Flight,
 * so a {@code new CqliteFlightMetadata(null, null, null)} suffices to exercise them.
 */
class CqliteFlightTableStatisticsTest {

    private final CqliteFlightMetadata metadata = new CqliteFlightMetadata(null, null, null);

    /** Aggregation JSON exactly as {@code AggregationSpec.toJson} serializes it. */
    private static String aggregationJson(String... groupBy) {
        StringBuilder gb = new StringBuilder();
        for (int i = 0; i < groupBy.length; i++) {
            if (i > 0) {
                gb.append(',');
            }
            gb.append('"').append(groupBy[i]).append('"');
        }
        return "{\"group_by\":[" + gb + "],"
                + "\"aggregates\":[{\"func\":\"Count\",\"column\":null,\"output\":\"agg0\"}]}";
    }

    private static CqliteFlightTableHandle aggregatedHandle(String... groupBy) {
        return new CqliteFlightTableHandle(
                "ks", "t", "ddl",
                Optional.empty(),
                Optional.of(aggregationJson(groupBy)),
                Optional.of("{\"group_by\":[],\"outputs\":[]}"));
    }

    @Test
    void globalAggregateOutputsExactlyOneRow() {
        // count(*) with no GROUP BY → the finalize split emits a single merged row.
        CqliteFlightTableHandle handle = aggregatedHandle();
        assertTrue(handle.isAggregated());
        assertFalse(handle.hasGroupBy(), "no group_by → global aggregate");

        TableStatistics stats = metadata.getTableStatistics(null, handle);
        assertEquals(Estimate.of(1), stats.getRowCount(),
                "global aggregate output cardinality is exactly 1");
    }

    @Test
    void groupedAggregateReportsUnknownRowCount() {
        // GROUP BY c1 → output group count is not authoritatively known; do not
        // fabricate it (#28) — return empty so Trino estimates.
        CqliteFlightTableHandle handle = aggregatedHandle("c1");
        assertTrue(handle.isAggregated());
        assertTrue(handle.hasGroupBy(), "non-empty group_by → grouped aggregate");

        TableStatistics stats = metadata.getTableStatistics(null, handle);
        assertEquals(TableStatistics.empty(), stats,
                "grouped aggregate row count is unknown → empty (no fabrication)");
        assertTrue(stats.getRowCount().isUnknown(), "row count must be unknown for a grouped aggregate");
    }

    @Test
    void multiColumnGroupByIsStillGrouped() {
        CqliteFlightTableHandle handle = aggregatedHandle("c1", "c2");
        assertTrue(handle.hasGroupBy());
        assertEquals(TableStatistics.empty(), metadata.getTableStatistics(null, handle));
    }

    @Test
    void nonAggregatedHandleHasNoGroupBy() {
        // A plain (non-aggregated) handle never reports a GROUP BY.
        CqliteFlightTableHandle plain = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertFalse(plain.isAggregated());
        assertFalse(plain.hasGroupBy());
    }

    @Test
    void nonAggregatedHandleReportsUnknownRowCount() {
        // The table_stats row total is replica-summed (≈ RF × logical cardinality)
        // and not de-duplicated to one copy of the token space, so we do NOT expose
        // it as an optimizer row count (issue #944) — report unknown so Trino
        // estimates instead of trusting a knowably-wrong physical-replica total.
        CqliteFlightTableHandle plain = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertFalse(plain.isAggregated());

        TableStatistics stats = metadata.getTableStatistics(null, plain);
        assertEquals(TableStatistics.empty(), stats,
                "non-aggregated row count is replica-summed → empty (not exposed)");
        assertTrue(stats.getRowCount().isUnknown(),
                "non-aggregated row count must be unknown (replica-summed, not logical)");
    }

    @Test
    void hasGroupByIsFalseForUnparseableAggregationJson() {
        // Defensive: a malformed aggregation JSON is treated as a global aggregate
        // (one row), never crashes the optimizer call.
        CqliteFlightTableHandle handle = new CqliteFlightTableHandle(
                "ks", "t", "ddl",
                Optional.empty(),
                Optional.of("{not valid json"),
                Optional.of("{\"group_by\":[],\"outputs\":[]}"));
        assertTrue(handle.isAggregated());
        assertFalse(handle.hasGroupBy());
        assertEquals(Estimate.of(1), metadata.getTableStatistics(null, handle).getRowCount());
    }
}
