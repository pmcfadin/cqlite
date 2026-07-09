package in.mcfad.cqlite.flight;

import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    /** Strip a trailing {@code :port} from an {@code ip:port} replica address. */
    static String hostOnly(String address) {
        int colon = address.lastIndexOf(':');
        if (colon > 0 && address.indexOf(':') == colon
                && address.substring(colon + 1).chars().allMatch(Character::isDigit)) {
            return address.substring(0, colon);
        }
        return address; // no port, or IPv6 (left as-is)
    }
}

