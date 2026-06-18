package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;
import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;

/**
 * Produces one split per token range, each pinned to a single replica so a row
 * (which lives on RF replicas) is read exactly once across the cluster.
 */
public class CqliteFlightSplitManager implements ConnectorSplitManager {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;

    public CqliteFlightSplitManager(CqliteFlightConfig config, SidecarClient sidecar) {
        this.config = config;
        this.sidecar = sidecar;
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
        List<CqliteFlightSplit> splits =
                buildSplits(handle, replicas, config.localDatacenter(), config.flightPort());
        return new FixedSplitSource(splits);
    }

    /**
     * Pure split-building: one split per read-replica token range, each pinned to
     * a single deterministically-chosen replica. Static for unit testing without
     * a live Sidecar or Trino session.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort) {
        List<CqliteFlightSplit> splits = new ArrayList<>();
        for (ReplicaInfo range : replicas.readReplicas()) {
            String host = pickReplica(range.replicasByDatacenter(), localDatacenter);
            if (host == null) {
                continue; // range with no known replica — nothing to read
            }
            long start = range.startToken();
            long end = range.endToken();
            splits.add(new CqliteFlightSplit(
                    table.keyspace(),
                    table.table(),
                    table.ddl(),
                    host,
                    flightPort,
                    start,
                    end,
                    start > end));
        }
        return splits;
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
}
