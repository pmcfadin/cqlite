package in.mcfad.cqlite.flight;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;

public class CqliteFlightConnector implements Connector {
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final SnapshotManager snapshots;
    private final BufferAllocator allocator;
    private final CqliteFlightClient flight;

    public CqliteFlightConnector(CqliteFlightConfig config, SidecarClient sidecar) {
        this.config = config;
        this.sidecar = sidecar;
        // One shared snapshot manager (issues #2105, #2227): the split manager creates the
        // per-query snapshot on each replica host, the metadata cleans it up — both must see
        // the same registry. Each host's Sidecar is derived from the configured URI's scheme
        // + port (uniform across the hostNetwork Sidecar DaemonSet) and the split host.
        // The superseded-snapshot grace-sweep runs on a background best-effort scheduler (issue
        // #2452 item 2), NOT synchronously on the split-planning path — a hot table would otherwise
        // pay a multi-host DELETE fan-out in planning latency (roborev job 1722). The periodic tick
        // (cadence = the retire-grace period) also prunes QUIET tables that receive no further query,
        // so a short reuse window plus a long TTL never accumulates snapshot dirs (the #2367 field
        // accumulation). Retired at connector shutdown via SnapshotManager.close().
        this.snapshots = new SnapshotManager(
                HostSnapshotApis.fromBaseUri(config.sidecarUri()), config.readMode(), config.snapshotTtl(),
                config.snapshotReuseWindowNanos(), config.snapshotRetireGraceNanos(),
                new SnapshotManager.SystemClock(),
                new SnapshotRetireScheduler.BackgroundRetireScheduler(config.snapshotRetireGraceMillis()));
        // Fail fast at catalog load if arrow-java's off-heap memory init is broken by a missing JVM
        // flag (issues #2193, #2290) — otherwise every do_get dies far downstream with a cryptic
        // "Failed to read message". Runs before the first RootAllocator (the earliest Arrow touch)
        // and is a no-op when the flag is present.
        ArrowMemoryPreflight.verify();
        this.allocator = new RootAllocator();
        this.flight = new CqliteFlightClient(allocator);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return CqliteFlightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new CqliteFlightMetadata(config, sidecar, flight);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new CqliteFlightSplitManager(config, sidecar, snapshots);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new CqliteFlightPageSourceProvider(flight);
    }

    @Override
    public void shutdown() {
        // Retire every live reused snapshot (issue #2356): a reused snapshot outlives the query
        // that created it, so retirement is not per-query — the connector releases them at
        // shutdown (the Sidecar TTL backstop covers a crash/miss).
        snapshots.retireAll();
        // Release the background retire scheduler (issue #2452 item 2) after draining the snapshots.
        snapshots.close();
        allocator.close();
    }
}
