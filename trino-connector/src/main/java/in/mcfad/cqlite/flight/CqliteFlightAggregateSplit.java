package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The single "finalize split" for an aggregated table handle (issue #841).
 *
 * <p>Trino removes the {@code AggregationNode} and does NOT re-aggregate across
 * splits, so the connector must return the FULLY MERGED result. This one split
 * carries ALL token-range→replica assignments ({@code ranges}, built by
 * {@link CqliteFlightSplitManager#buildSplits} so each row is read exactly once)
 * plus the pushed filter, the wire {@link AggregationSpec} JSON, and the
 * connector-side {@link FinalizeAggregationPlan} JSON. Its {@code PageSource}
 * fans out to every range's pinned replica, pulls each range's PARTIAL aggregate,
 * merges them, and emits the final row(s).
 */
public record CqliteFlightAggregateSplit(
        String keyspace,
        String table,
        String ddl,
        List<CqliteFlightSplit> ranges,
        Optional<String> filterJson,
        String aggregationJson,
        String finalizePlanJson)
        implements ConnectorSplit {

    @Override
    public List<HostAddress> getAddresses() {
        // No single locality home: the finalize split fans out to every range's replica.
        // Emit one soft hint per DISTINCT range PRIMARY (issue #2397), preserving order and
        // deduplicating, so the hint reflects the rotated spread rather than always pinning
        // to the first range's host — the fan-out itself dials each range's rotated primary.
        if (ranges.isEmpty()) {
            return List.of();
        }
        List<HostAddress> addresses = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (CqliteFlightSplit range : ranges) {
            if (seen.add(range.host())) {
                addresses.add(HostAddress.fromParts(range.host(), range.port()));
            }
        }
        return addresses;
    }

    /**
     * Scheduler-meaningful upper bound on the finalize split's weight proportion (issue #2680).
     *
     * <p>Rationale for capping BELOW {@link CqliteFlightSplit#MAX_WEIGHT_PROPORTION} (1000): the
     * finalize split runs on ONE driver, fanning out to its ranges SEQUENTIALLY, so its per-node
     * concurrency footprint is a single slot no matter how many ranges it merges. Trino's split
     * weight is a per-node ADMISSION budget (default {@code node-scheduler.max-splits-per-node}
     * = 256 standard splits), so past roughly the node's whole budget more weight changes NO
     * scheduling decision — it only starves the finalize node of co-scheduled work while this
     * split sits blocked on network I/O. 100× standard says "the heaviest split class on this
     * node" while staying inside the default budget, and keeps the weight STRICTLY proportional
     * for every realistic ring (a 3-node/16-vnode topology is ~48 ranges); beyond ~100 ranges it
     * saturates deliberately, and the saturated value is still the correct ordering signal.
     */
    public static final double MAX_AGGREGATE_WEIGHT_PROPORTION = 100.0;

    /**
     * The finalize split fans out to every range's partial, so its {@link SplitWeight} is the
     * clamped SUM of the per-range weight proportions it covers (issue #2680) — proportional to
     * the total token span merged. Clamped to Trino's valid {@code fromProportion} range (so an
     * empty or extreme fan-out never yields a zero weight or an exception) and further to
     * {@link #MAX_AGGREGATE_WEIGHT_PROPORTION}, the point past which more weight is scheduling
     * noise. Note the split manager builds the aggregate path at K=1, so {@code ranges} are
     * PARENT token ranges, not K-way slices.
     */
    @Override
    public SplitWeight getSplitWeight() {
        double sum = 0.0;
        for (CqliteFlightSplit range : ranges) {
            sum += range.weightProportion();
        }
        double clamped = Math.min(
                MAX_AGGREGATE_WEIGHT_PROPORTION, CqliteFlightSplit.clampProportion(sum));
        return SplitWeight.fromProportion(clamped);
    }
}
