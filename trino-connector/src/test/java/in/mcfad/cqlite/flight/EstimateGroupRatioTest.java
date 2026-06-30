package in.mcfad.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CqliteFlightMetadata#estimateGroupRatio} — the DDL-driven
 * mapping from a GROUP BY shape + authoritative partition/row counts to an
 * estimated groups/rows ratio (issue #944). Pure function; no Sidecar/Flight.
 */
class EstimateGroupRatioTest {

    // Wide table: 10 partitions, 2000 rows (mirrors the sensor_data fixture).
    private static final String WIDE_DDL =
            "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))";
    private static final TableStats WIDE_STATS = new TableStats(2000, 10, 1);

    // Single-row-partition table: 1000 partitions, 1000 rows (simple_table shape).
    private static final String SIMPLE_DDL =
            "CREATE TABLE ks.t (id int PRIMARY KEY, name text)";
    private static final TableStats SIMPLE_STATS = new TableStats(1000, 1000, 1);

    @Test
    void groupByFullPartitionKeyOnWideTableIsLowRatio() {
        // GROUP BY pk → groups ≈ partitionCount (10) / rows (2000) = 0.005 → PUSH.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of("pk"), WIDE_STATS);
        assertTrue(ratio.isPresent());
        assertEquals(10.0 / 2000.0, ratio.getAsDouble(), 1e-9);
    }

    @Test
    void groupByPartitionPlusAllClusteringReachesFullRowUniqueness() {
        // GROUP BY pk, ck reaches per-row uniqueness → ratio ≈ 1.0 → DECLINE.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of("pk", "ck"), WIDE_STATS);
        assertTrue(ratio.isPresent());
        assertEquals(1.0, ratio.getAsDouble(), 1e-9);
    }

    @Test
    void groupBySinglePartitionKeyTableIsRatioOne() {
        // PK = id, no clustering: GROUP BY id covers the full key AND all clustering
        // (empty) → full row uniqueness → ratio ≈ 1.0 → DECLINE.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                SIMPLE_DDL, List.of("id"), SIMPLE_STATS);
        assertTrue(ratio.isPresent());
        assertEquals(1.0, ratio.getAsDouble(), 1e-9);
    }

    @Test
    void groupByPartitionKeyPrefixIsUnboundedAndPushes() {
        // Composite partition key (a, b); GROUP BY a is only a PREFIX → cannot be
        // bounded from partition/row counts → empty → PUSH (safe).
        String ddl = "CREATE TABLE ks.t (a int, b int, v int, PRIMARY KEY ((a, b)))";
        TableStats stats = new TableStats(500, 500, 1);
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                ddl, List.of("a"), stats);
        assertTrue(ratio.isEmpty(), "partition-key prefix is unbounded → push");
    }

    @Test
    void groupByNonKeyColumnIsUnboundedAndPushes() {
        // GROUP BY a regular column → no partition/row bound → empty → PUSH.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of("v"), WIDE_STATS);
        assertTrue(ratio.isEmpty());
    }

    @Test
    void zeroRowsYieldsNoEstimate() {
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of("pk"), TableStats.EMPTY);
        assertTrue(ratio.isEmpty());
    }

    @Test
    void emptyGroupingYieldsNoEstimate() {
        // A global aggregate has no grouping; it bypasses the gate before this call,
        // but the mapping is defensively empty for an empty grouping set too.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of(), WIDE_STATS);
        assertTrue(ratio.isEmpty());
    }

    @Test
    void caseInsensitiveGroupingMatch() {
        // Unquoted identifiers fold to lower-case on both sides.
        OptionalDouble ratio = CqliteFlightMetadata.estimateGroupRatio(
                WIDE_DDL, List.of("PK"), WIDE_STATS);
        assertTrue(ratio.isPresent());
        assertEquals(10.0 / 2000.0, ratio.getAsDouble(), 1e-9);
    }

    @Test
    void ratioFeedsTheGateDecisionEndToEnd() {
        // Wide GROUP BY pk (ratio 0.005) PUSHES; GROUP BY pk,ck (ratio 1.0) DECLINES,
        // both under the default 0.5 crossover.
        double max = CqliteFlightConfig.DEFAULT_MAX_GROUP_RATIO;
        OptionalDouble low = CqliteFlightMetadata.estimateGroupRatio(WIDE_DDL, List.of("pk"), WIDE_STATS);
        OptionalDouble high = CqliteFlightMetadata.estimateGroupRatio(WIDE_DDL, List.of("pk", "ck"), WIDE_STATS);

        assertFalse(CqliteFlightMetadata.declineGroupByPushdown(
                GroupByPushdownPolicy.AUTOMATIC, low, max), "low-cardinality GROUP BY pushes");
        assertTrue(CqliteFlightMetadata.declineGroupByPushdown(
                GroupByPushdownPolicy.AUTOMATIC, high, max), "full-row GROUP BY declines");

        // NEVER always declines, ALWAYS always pushes — regardless of ratio.
        assertTrue(CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.NEVER, low, max));
        assertFalse(CqliteFlightMetadata.declineGroupByPushdown(GroupByPushdownPolicy.ALWAYS, high, max));
    }
}
