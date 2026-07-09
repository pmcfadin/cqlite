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
     * boundary. Because the ring is a circle, the canonical exact-tiling test is: sort ranges
     * by start token; each range's {@code end} must equal the NEXT range's {@code start}
     * (cycling the last range back to the first). For adjacent ranges {@code (a, b]} and
     * {@code (b, c]}, the shared token {@code b} is the inclusive end of the first and the
     * exclusive start of the second, so they meet with neither overlap nor gap. A single
     * range with {@code start == end} is the whole ring ({@code (T, T]}, issue #2228) and
     * trivially closes the cycle. Any deviation is classified — via a signed token comparison
     * against the connecting boundary — as an overlap or a gap and named in the error.
     */
    static void validateRingCoverage(List<ReplicaInfo> ranges) {
        if (ranges.isEmpty()) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology returned no read-replica ranges, so the "
                            + "token ring is not covered; refusing to scan (would silently return "
                            + "0 rows). This is usually a transient topology transition — retry.");
        }
        List<ReplicaInfo> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(ReplicaInfo::startToken));
        int n = sorted.size();
        for (int i = 0; i < n; i++) {
            ReplicaInfo cur = sorted.get(i);
            ReplicaInfo next = sorted.get((i + 1) % n);
            long curEnd = cur.endToken();
            long nextStart = next.startToken();
            if (curEnd == nextStart) {
                continue; // ranges meet exactly: no overlap, no gap
            }
            // curEnd is the connecting boundary; nextStart is where the next range begins.
            // For the closing (wraparound) pair, nextStart is the minimum start token, so the
            // same signed comparison classifies the fault direction consistently.
            String kind = Long.compare(curEnd, nextStart) > 0 ? "overlap" : "gap";
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology does not tile the ring exactly once: "
                            + kind + " between range (" + cur.start() + ", " + cur.end() + "] and "
                            + "range (" + next.start() + ", " + next.end() + "] — range 1 ends at "
                            + "token " + curEnd + " but range 2 starts at token " + nextStart
                            + " (expected them to be equal). This risks "
                            + ("overlap".equals(kind) ? "duplicate" : "missing")
                            + " rows and is usually a transient topology transition "
                            + "(bootstrap/decommission) — retry.");
        }
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

