package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

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
        // No single locality home: the finalize split fans out to every range's
        // replica. Returning the first range as a soft hint at most.
        if (ranges.isEmpty()) {
            return List.of();
        }
        CqliteFlightSplit first = ranges.get(0);
        return List.of(HostAddress.fromParts(first.host(), first.port()));
    }
}
