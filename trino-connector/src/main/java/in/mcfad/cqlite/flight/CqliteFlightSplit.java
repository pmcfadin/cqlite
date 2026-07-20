package in.mcfad.cqlite.flight;

import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * One unit of work: scan a single token range from a replica's cqlite-flight
 * endpoint. Each range is still read from ONE replica at a time (so cross-replica
 * duplication is avoided, PLAN §2), but the split carries an ORDERED replica list
 * for availability failover (issue #2241): {@code host} is the primary; {@code
 * fallbackHosts} are the range's other replica owners, in try order, that the page
 * source may fail over to if the primary's Flight endpoint is unreachable.
 *
 * <p>{@code tokenStart} is exclusive, {@code tokenEnd} inclusive; {@code wraparound}
 * is set when the range crosses the ring's min-token boundary ({@code start > end}).
 *
 * <p>{@code snapshot} is the Sidecar snapshot name the ticket reads (issue #2105):
 * present in {@link ReadMode#SNAPSHOT}, {@link Optional#empty()} in {@link ReadMode#LIVE}.
 * In snapshot mode a fallback is only listed here if its host also has the snapshot
 * (issue #2227 creates it per replica host); in live mode every replica owner is eligible.
 *
 * <p>{@code weightProportion} is this split's {@link SplitWeight} proportion (issue #2680):
 * proportional to the slice's token span, scaled so a mean-span slice is {@code 1.0}
 * ({@link SplitWeight#standard()}), and clamped to Trino's valid range so an extreme (near-zero
 * or very large) slice never yields a zero weight or an exception. The split manager computes it
 * from the sliced ring; splits built through the backward-compatible constructors default to the
 * standard weight ({@code 1.0}).
 */
public record CqliteFlightSplit(
        String keyspace,
        String table,
        String ddl,
        String host,
        int port,
        long tokenStart,
        long tokenEnd,
        boolean wraparound,
        Optional<String> snapshot,
        List<String> fallbackHosts,
        double weightProportion)
        implements ConnectorSplit {

    /** The standard-weight proportion — a mean-span slice, and the pre-#2680 default. */
    public static final double STANDARD_WEIGHT_PROPORTION = 1.0;

    /**
     * Minimum {@link SplitWeight} proportion (issue #2680). Trino's {@code fromProportion}
     * requires a strictly positive proportion; clamping here keeps a near-zero-span slice from
     * yielding a zero weight or an exception. {@code round(0.01 * 100) = 1}, the smallest raw weight.
     */
    public static final double MIN_WEIGHT_PROPORTION = 0.01;

    /** Maximum {@link SplitWeight} proportion (issue #2680) — a sanity cap for a very large slice. */
    public static final double MAX_WEIGHT_PROPORTION = 1000.0;

    /** Normalize the fallback list to an immutable copy (never null) and clamp the weight. */
    public CqliteFlightSplit {
        fallbackHosts = fallbackHosts == null ? List.of() : List.copyOf(fallbackHosts);
        weightProportion = clampProportion(weightProportion);
    }

    /**
     * Clamp a raw span-derived proportion to Trino's valid {@link SplitWeight#fromProportion}
     * range (issue #2680): a strictly positive value in {@code [MIN, MAX]}, so no split ever
     * reports a zero weight or throws. A non-finite input degrades to the standard weight.
     */
    static double clampProportion(double proportion) {
        if (Double.isNaN(proportion)) {
            return STANDARD_WEIGHT_PROPORTION;
        }
        return Math.min(MAX_WEIGHT_PROPORTION, Math.max(MIN_WEIGHT_PROPORTION, proportion));
    }

    /**
     * Backward-compatible construction with an ordered fallback list but the standard weight
     * (issue #2680). Kept so existing call sites and tests compile unchanged.
     */
    public CqliteFlightSplit(
            String keyspace,
            String table,
            String ddl,
            String host,
            int port,
            long tokenStart,
            long tokenEnd,
            boolean wraparound,
            Optional<String> snapshot,
            List<String> fallbackHosts) {
        this(keyspace, table, ddl, host, port, tokenStart, tokenEnd, wraparound, snapshot,
                fallbackHosts, STANDARD_WEIGHT_PROPORTION);
    }

    /**
     * Backward-compatible construction with no availability fallbacks (single-replica
     * split) and the standard weight. Kept so existing call sites and tests compile unchanged.
     */
    public CqliteFlightSplit(
            String keyspace,
            String table,
            String ddl,
            String host,
            int port,
            long tokenStart,
            long tokenEnd,
            boolean wraparound,
            Optional<String> snapshot) {
        this(keyspace, table, ddl, host, port, tokenStart, tokenEnd, wraparound, snapshot, List.of());
    }

    /**
     * This split's {@link SplitWeight} (issue #2680), proportional to the slice's token span so
     * Trino's scheduler accounts for uneven slices instead of counting every split as equal.
     */
    @Override
    public SplitWeight getSplitWeight() {
        return SplitWeight.fromProportion(weightProportion);
    }

    /**
     * The ordered replica list to try for this range (issue #2241): the primary
     * {@link #host} first, then each {@link #fallbackHosts} entry in order.
     */
    public List<String> replicaHosts() {
        List<String> all = new ArrayList<>(1 + fallbackHosts.size());
        all.add(host);
        all.addAll(fallbackHosts);
        return all;
    }

    @Override
    public List<HostAddress> getAddresses() {
        // Soft locality hints: every replica that can serve this split (#2241), primary first.
        List<HostAddress> addrs = new ArrayList<>(1 + fallbackHosts.size());
        for (String h : replicaHosts()) {
            addrs.add(HostAddress.fromParts(h, port));
        }
        return addrs;
    }
}
