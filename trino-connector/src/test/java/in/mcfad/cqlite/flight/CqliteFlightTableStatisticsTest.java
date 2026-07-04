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
 * <p>A NON-aggregated handle now reports a LOGICAL (de-replicated) row count
 * {@code live_rows / R} WHEN it can be grounded, and FAILS CLOSED to
 * {@link TableStatistics#empty()} when it cannot (issue #1336). The end-to-end
 * grounded derivation (RF=3 → 200, memoization, per-condition fail-closed) is pinned
 * in {@link CqliteFlightLogicalRowCountTest}; here we pin (a) the aggregated branches
 * (which still return before touching Sidecar/Flight) and (b) that the non-aggregated
 * branch fails closed to {@code empty()} when the derivation cannot be grounded
 * (here: no Sidecar available), never throwing into query planning.
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
    void nonAggregatedHandleFailsClosedWhenDerivationUngrounded() {
        // Non-aggregated: the connector now reports a LOGICAL row count live_rows / R
        // when it can ground R from tokenRangeReplicas AND get complete table_stats
        // (pinned end to end in CqliteFlightLogicalRowCountTest). When it CANNOT be
        // grounded — here there is no Sidecar/Flight at all — it fails closed to
        // empty() (today's safe behavior) and never throws into query planning
        // (issue #1336).
        CqliteFlightTableHandle plain = new CqliteFlightTableHandle("ks", "t", "ddl");
        assertFalse(plain.isAggregated());

        TableStatistics stats = metadata.getTableStatistics(null, plain);
        assertEquals(TableStatistics.empty(), stats,
                "an ungrounded logical-row-count derivation fails closed to empty");
        assertTrue(stats.getRowCount().isUnknown(),
                "row count must be unknown when the derivation cannot be grounded");
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
