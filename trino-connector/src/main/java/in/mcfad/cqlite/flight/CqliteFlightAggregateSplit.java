package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
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
}
