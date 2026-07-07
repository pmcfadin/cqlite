package in.mcfad.cqlite.flight.sidecar;

import java.util.Optional;

/**
 * The Cassandra Sidecar snapshot lifecycle operations the connector needs
 * (issue #2105). Extracted as an interface so the snapshot manager can be unit
 * tested without a live Sidecar / real HTTP; {@link SidecarClient} is the
 * production implementation.
 *
 * <p>Both operations map to the Sidecar v1 snapshots route
 * {@code /api/v1/keyspaces/{keyspace}/tables/{table}/snapshots/{snapshot}}:
 * {@code create} = {@code PUT}, {@code clear} = {@code DELETE}
 * (apache/cassandra-sidecar {@code ApiEndpointsV1.SNAPSHOTS_ROUTE}).
 */
public interface SnapshotApi {

    /**
     * Create (PUT) a snapshot of one table's current SSTable set. Fails closed:
     * a non-2xx response or transport error throws {@link SidecarClient.SidecarException}.
     *
     * @param ttl optional Cassandra 4.1+ TTL (e.g. {@code 6h}) after which Cassandra
     *            auto-drops the snapshot — a backstop against leaks if explicit cleanup
     *            never runs.
     */
    void createSnapshot(String keyspace, String table, String snapshotName, Optional<String> ttl);

    /** Clear (DELETE) a previously created snapshot. */
    void clearSnapshot(String keyspace, String table, String snapshotName);
}
