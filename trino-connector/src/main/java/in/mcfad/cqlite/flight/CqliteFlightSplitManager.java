package in.mcfad.cqlite.flight;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import in.mcfad.cqlite.flight.sidecar.HostAddresses;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;

/**
 * Produces one split per token range, each pinned to a single replica so a row
 * (which lives on RF replicas) is read exactly once across the cluster.
 */
public class CqliteFlightSplitManager implements ConnectorSplitManager {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final SnapshotManager snapshots;

    public CqliteFlightSplitManager(CqliteFlightConfig config, SidecarClient sidecar, SnapshotManager snapshots) {
        this.config = config;
        this.sidecar = sidecar;
        this.snapshots = snapshots;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        TokenRangeReplicasResponse replicas = sidecar.tokenRangeReplicas(handle.keyspace());
        // Ring-coverage guard (issue #2237): during Cassandra topology transitions
        // (bootstrap/decommission) the Sidecar's read-replica ranges can transiently
        // OVERLAP (→ duplicate rows) or GAP (→ missing rows), both silent. Verify the
        // returned ranges tile the token ring exactly once BEFORE building splits and
        // fail closed with an actionable error otherwise.
        validateRingCoverage(replicas.readReplicas());
        // Read-mode wiring (issues #2105, #2227): in snapshot mode create (once per query)
        // a Sidecar snapshot on EVERY distinct replica host the scan's splits will read —
        // a snapshot PUT is instance-local, so a snapshot made only on the configured
        // Sidecar's node leaves splits on other hosts reading a non-existent directory
        // (NotFound). We stamp the same snapshot name into every split's ticket so the whole
        // scan reads one immutable file set per host; in live mode this is Optional.empty().
        // Fails closed — a create error on any host propagates (naming host + snapshot) and
        // the query fails.
        Set<String> replicaHosts = distinctReplicaHosts(replicas, config.localDatacenter());
        Optional<String> snapshot =
                snapshots.snapshotFor(session.getQueryId(), handle.keyspace(), handle.table(), replicaHosts);
        List<CqliteFlightSplit> ranges =
                buildSplits(handle, replicas, config.localDatacenter(), config.flightPort(), snapshot);

        // Aggregated handle: Trino does not re-aggregate across splits, so return
        // ONE finalize split carrying all range→replica assignments. Its PageSource
        // fans out, pulls each range's partial, merges, and emits the final rows.
        if (handle.isAggregated()) {
            CqliteFlightAggregateSplit finalize = new CqliteFlightAggregateSplit(
                    handle.keyspace(),
                    handle.table(),
                    handle.ddl(),
                    ranges,
                    handle.filterJson(),
                    handle.aggregationJson().orElseThrow(),
                    handle.finalizePlanJson().orElseThrow());
            return new FixedSplitSource(List.of(finalize));
        }

        return new FixedSplitSource(ranges);
    }

    /**
     * Fail-closed ring-coverage guard (issue #2237). The Sidecar's read-replica ranges
     * are expected to tile the Murmur3 token ring (signed 64-bit, a circle) EXACTLY once,
     * but during topology transitions (bootstrap/decommission) they can transiently OVERLAP
     * (silent duplicate rows) or GAP (silent missing rows). This verifies the exact tiling
     * before any split is built and throws an actionable {@link TrinoException} otherwise.
     *
     * <p>Ranges are {@code (start, end]} — start exclusive, end inclusive (Cassandra
     * convention, see {@link ReplicaInfo}); a range with {@code start >= end} wraps the ring
     * boundary. A full tiling is accepted in EITHER of the two forms the Sidecar emits:
     *
     * <ul>
     *   <li><b>Unwrapped form</b> — the form the real Sidecar emits (see
     *       {@code SidecarClientTest#parsesTokenRangeReplicasAndTokens}: {@code (MIN,0]} +
     *       {@code (0,MAX]}): sorted ranges whose FIRST start is {@link Long#MIN_VALUE} and
     *       whose LAST end is {@link Long#MAX_VALUE}, with no range wrapping. The chain
     *       {@code (MIN, …] … (…, MAX]} spans the whole ring exactly once.</li>
     *   <li><b>Wrap form</b> — a single range wraps the boundary and closes the circle: the
     *       last (highest-start) range wraps ({@code start >= end}) and its inclusive end
     *       equals the first range's exclusive start. A single {@code (T, T]} (issue #2228)
     *       is the degenerate one-range case of this form (the whole ring).</li>
     * </ul>
     *
     * <p>Both forms first require the interior to join perfectly: after sorting by start,
     * each range's {@code end} equals the NEXT range's {@code start} (for adjacent
     * {@code (a, b]} and {@code (b, c]} the shared token {@code b} is the inclusive end of
     * the first and the exclusive start of the second — no overlap, no gap). Any deviation
     * — interior, or at the ring-closing boundary — is classified via a signed token
     * comparison as an overlap (double coverage → duplicate rows) or a gap (uncovered
     * tokens → missing rows) and named in the error. Because at most ONE range may wrap and
     * it must be the closing (last) range, more than one wrapping range, or a wrapping /
     * full-ring range anywhere but last, is rejected as an overlap (it double-covers the
     * ring). {@code null} or an empty list fails closed (would silently return 0 rows).
     */
    static void validateRingCoverage(List<ReplicaInfo> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology returned no read-replica ranges, so the "
                            + "token ring is not covered; refusing to scan (would silently return "
                            + "0 rows). This is usually a transient topology transition — retry.");
        }
        List<ReplicaInfo> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(ReplicaInfo::startToken));
        int n = sorted.size();

        // A valid tiling has AT MOST ONE wrapping range (start >= end), and it must be the
        // closing (last, highest-start) range. Two wrapping ranges, or a wrap / full-ring
        // (T,T] range anywhere but last, necessarily double-covers the ring -> overlap. This
        // also closes a false-accept: an interior (T,T] full-ring range whose neighbours
        // share its token would otherwise pass the adjacency check below.
        int wrapCount = 0;
        int lastWrapIndex = -1;
        for (int i = 0; i < n; i++) {
            ReplicaInfo r = sorted.get(i);
            if (r.startToken() >= r.endToken()) {
                wrapCount++;
                lastWrapIndex = i;
            }
        }
        if (wrapCount > 1 || (wrapCount == 1 && lastWrapIndex != n - 1)) {
            ReplicaInfo w = sorted.get(lastWrapIndex);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology does not tile the ring exactly once: "
                            + "overlap — a ring-wrapping range (" + w.start() + ", " + w.end()
                            + "] is not the single closing range, so it double-covers the ring. "
                            + "This risks duplicate rows and is usually a transient topology "
                            + "transition (bootstrap/decommission) — retry.");
        }

        // Interior adjacency: each range's end must meet the next range's start exactly.
        // Iterates consecutive pairs only (does NOT wrap the last back to the first); the
        // ring is closed separately below so both coverage forms are handled.
        for (int i = 0; i + 1 < n; i++) {
            ReplicaInfo cur = sorted.get(i);
            ReplicaInfo next = sorted.get(i + 1);
            if (cur.endToken() != next.startToken()) {
                throw tilingFault(cur, next, cur.endToken(), next.startToken());
            }
        }

        ReplicaInfo first = sorted.get(0);
        ReplicaInfo last = sorted.get(n - 1);
        if (wrapCount == 0) {
            // Unwrapped chain (first.start, last.end] — no range crosses the boundary.
            // (a) Full coverage iff it spans (MIN, MAX].
            if (first.startToken() == Long.MIN_VALUE && last.endToken() == Long.MAX_VALUE) {
                return;
            }
            // Otherwise the ring boundary (past last.end, around to first.start) is uncovered
            // — always a GAP; a non-wrapping chain can never double-cover the boundary.
            throw ringBoundaryGap(last, first);
        }
        // wrapCount == 1 and it is the closing (last) range. (b) The circle closes iff the
        // wrap's inclusive end meets the first range's exclusive start (subsumes the single
        // (T, T] full ring). Otherwise the wrap end PAST first.start double-covers (overlap)
        // or falls SHORT (gap) — the signed comparison classifies correctly here.
        if (last.endToken() == first.startToken()) {
            return;
        }
        throw tilingFault(last, first, last.endToken(), first.startToken());
    }

    /**
     * The ring-closing GAP for an unwrapped chain that does not span the full {@code (MIN, MAX]}
     * ring: the tokens past {@code last.end} around the boundary to {@code first.start} are owned
     * by no range (→ missing rows). Reported as a gap regardless of token magnitudes because a
     * non-wrapping chain cannot double-cover.
     */
    private static TrinoException ringBoundaryGap(ReplicaInfo last, ReplicaInfo first) {
        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Cassandra token-range topology does not tile the ring exactly once: gap at the "
                        + "ring boundary — the ranges cover only (" + first.start() + ", "
                        + last.end() + "] and no range wraps to close the circle, so tokens past "
                        + last.end() + " (around to " + first.start() + ") are uncovered. This "
                        + "risks missing rows and is usually a transient topology transition "
                        + "(bootstrap/decommission) — retry.");
    }

    /**
     * Build the actionable typed error for a ring-tiling fault, classifying it as an overlap
     * (double coverage → duplicate rows) when {@code curEnd} is past {@code nextStart}, else a
     * gap (uncovered tokens → missing rows), naming the offending boundary.
     */
    private static TrinoException tilingFault(ReplicaInfo cur, ReplicaInfo next, long curEnd, long nextStart) {
        String kind = Long.compare(curEnd, nextStart) > 0 ? "overlap" : "gap";
        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Cassandra token-range topology does not tile the ring exactly once: "
                        + kind + " between range (" + cur.start() + ", " + cur.end() + "] and "
                        + "range (" + next.start() + ", " + next.end() + "] — range 1 ends at "
                        + "token " + curEnd + " but range 2 starts at token " + nextStart
                        + ". This risks "
                        + ("overlap".equals(kind) ? "duplicate" : "missing")
                        + " rows and is usually a transient topology transition "
                        + "(bootstrap/decommission) — retry.");
    }

    /**
     * Backward-compatible overload for a live-dir scan (no snapshot). Delegates with
     * {@link Optional#empty()}.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort) {
        return buildSplits(table, replicas, localDatacenter, flightPort, Optional.empty());
    }

    /**
     * Pure split-building: one split per read-replica token range, each pinned to
     * a single deterministically-chosen replica, all stamped with the same
     * {@code snapshot} (issue #2105). Static for unit testing without a live Sidecar
     * or Trino session.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort,
            Optional<String> snapshot) {
        List<CqliteFlightSplit> splits = new ArrayList<>();
        for (ReplicaInfo range : replicas.readReplicas()) {
            String replica = pickReplica(range.replicasByDatacenter(), localDatacenter);
            if (replica == null) {
                continue; // range with no known replica — nothing to read
            }
            // Sidecar returns replicas as "ip:storage_port"; the flight server
            // listens on flightPort at the same host, so keep only the host.
            String host = hostOnly(replica);
            long start = range.startToken();
            long end = range.endToken();
            // #2228: equal endpoints (start == end) denote the FULL ring — the
            // Cassandra convention for a range `(T, T]` — not the empty set. The
            // Sidecar can emit this for single-token/single-node topologies.
            // Treating it as wraparound makes the flight-side filter accept every
            // token (`token > T || token <= T`), so `SELECT *` scans everything
            // instead of silently returning 0 rows.
            boolean wraparound = start >= end;
            splits.add(new CqliteFlightSplit(
                    table.keyspace(),
                    table.table(),
                    table.ddl(),
                    host,
                    flightPort,
                    start,
                    end,
                    wraparound,
                    snapshot));
        }
        return splits;
    }

    /**
     * The distinct set of replica hosts the scan's splits will read — the same
     * deterministic {@link #pickReplica} choice used by {@link #buildSplits}, one host per
     * range, deduplicated. Order-preserving (insertion order) so per-host snapshot creation
     * is stable across re-planning. This is exactly the set of hosts a per-query snapshot
     * must be created on (issue #2227).
     */
    static Set<String> distinctReplicaHosts(TokenRangeReplicasResponse replicas, String localDatacenter) {
        Set<String> hosts = new LinkedHashSet<>();
        for (ReplicaInfo range : replicas.readReplicas()) {
            String replica = pickReplica(range.replicasByDatacenter(), localDatacenter);
            if (replica != null) {
                hosts.add(hostOnly(replica));
            }
        }
        return hosts;
    }

    /**
     * Choose one replica for a range: prefer the local datacenter, else any DC.
     * Selection is deterministic (lexicographically smallest address) so repeated
     * planning yields stable splits.
     */
    static String pickReplica(Map<String, List<String>> replicasByDatacenter, String localDatacenter) {
        if (localDatacenter != null) {
            List<String> local = replicasByDatacenter.get(localDatacenter);
            if (local != null && !local.isEmpty()) {
                return local.stream().sorted().findFirst().orElseThrow();
            }
        }
        return replicasByDatacenter.values().stream()
                .flatMap(List::stream)
                .sorted()
                .findFirst()
                .orElse(null);
    }

    /**
     * Normalize an {@code ip:port} (or bracketed IPv6) replica address to a bare host
     * literal via the single {@link HostAddresses} authority, so the split's pinned host
     * and the per-host snapshot URI (issue #2227) agree on the exact host string.
     */
    static String hostOnly(String address) {
        return HostAddresses.hostOnly(address);
    }
}

