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
 * <p>{@code complete} reports whether the sums cover EVERY SSTable: a per-node
 * response is incomplete when any of that node's {@code Statistics.db} files failed
 * to decode (issue #944, #28). Partial sums are NOT authoritative, so a consumer
 * MUST fail closed to "no estimate" rather than derive a (biased) ratio or row
 * count from them. The flag defaults to {@code false} when absent on the wire (an
 * older server that predates the field) so we never treat unknown completeness as
 * complete.
 *
 * <p>Field names are snake_case on the wire to match serde; the Jackson
 * annotations map them to camelCase accessors.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TableStats(
        @JsonProperty("live_rows") long liveRows,
        @JsonProperty("partition_count") long partitionCount,
        @JsonProperty("sstable_count") long sstableCount,
        @JsonProperty("complete") boolean complete,
        @JsonProperty("skipped_sstables") long skippedSstables) {

    /**
     * All-zero stats (no SSTables / no data). Trivially COMPLETE: there is nothing
     * that could have been skipped — the per-node aggregate starts here and the
     * {@code complete} flag is ANDed in {@link #plus}, so an empty start does not
     * spuriously taint a real node's completeness.
     */
    public static final TableStats EMPTY = new TableStats(0, 0, 0, true, 0);

    /**
     * An INCOMPLETE, all-zero sentinel folded into the cross-ring aggregate when a
     * ring node's {@code table_stats} call FAILS (issue #944). Because {@link #plus}
     * ANDs the {@code complete} flags, folding this taints the whole aggregate to
     * {@code complete=false} — so a node we should have queried but could not reach is
     * never silently dropped, and a partial cross-ring total is correctly reported as
     * "no estimate" rather than authoritative. It carries one {@code skippedSstables}
     * so the failure is visible in the aggregate.
     */
    public static final TableStats UNAVAILABLE = new TableStats(0, 0, 0, false, 1);

    /**
     * Element-wise sum, used to aggregate per-node responses across the ring. The
     * {@code complete} flag is the logical AND of both sides: the aggregate is
     * complete only if EVERY contributing node reported a complete decode, so a
     * single node with an undecodable {@code Statistics.db} makes the whole
     * cross-ring total incomplete (and thus "no estimate" for the gate).
     */
    public TableStats plus(TableStats other) {
        return new TableStats(
                Math.addExact(liveRows, other.liveRows),
                Math.addExact(partitionCount, other.partitionCount),
                Math.addExact(sstableCount, other.sstableCount),
                complete && other.complete,
                Math.addExact(skippedSstables, other.skippedSstables));
    }
}
