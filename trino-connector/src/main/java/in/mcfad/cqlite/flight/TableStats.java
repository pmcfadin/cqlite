package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-table aggregate statistics returned by the cqlite-flight server's
 * {@code table_stats} DoAction (issue #944, see the Rust
 * {@code cqlite_flight::stats::TableStatsResponse}).
 *
 * <p>All three fields are AUTHORITATIVE per-SSTable sums decoded from each
 * {@code Statistics.db} STATS component (no heuristics, #28):
 *
 * <ul>
 *   <li>{@code liveRows} — Σ {@code totalRows} across the table's SSTables. An
 *       UPPER BOUND on the table's true row count (pre-compaction overlap).</li>
 *   <li>{@code partitionCount} — Σ {@code estimatedPartitionSize} histogram counts.
 *       An UPPER BOUND on the table's true distinct-partition count.</li>
 *   <li>{@code sstableCount} — number of SSTables that contributed.</li>
 * </ul>
 *
 * <p>These per-SSTable / per-node sums are intentionally upper bounds: an upper
 * bound on distinct groups never under-counts, so the AUTOMATIC aggregation-
 * pushdown gate never wrongly pushes a genuinely high-cardinality GROUP BY.
 *
 * <p>Field names are snake_case on the wire to match serde; the Jackson
 * annotations map them to camelCase accessors.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TableStats(
        @JsonProperty("live_rows") long liveRows,
        @JsonProperty("partition_count") long partitionCount,
        @JsonProperty("sstable_count") long sstableCount) {

    /** All-zero stats (no SSTables / no data). */
    public static final TableStats EMPTY = new TableStats(0, 0, 0);

    /** Element-wise sum, used to aggregate per-node responses across the ring. */
    public TableStats plus(TableStats other) {
        return new TableStats(
                Math.addExact(liveRows, other.liveRows),
                Math.addExact(partitionCount, other.partitionCount),
                Math.addExact(sstableCount, other.sstableCount));
    }
}
