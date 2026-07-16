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
        // Fail fast at catalog load if arrow-java's off-heap memory init is broken by a missing JVM
        // flag (issues #2193, #2290) — otherwise every do_get dies far downstream with a cryptic
        // "Failed to read message". Runs before the first RootAllocator (the earliest Arrow touch)
        // and is a no-op when the flag is present.
        //
        // Deliberately BEFORE the SnapshotManager/scheduler construction below (issue #2452 item 1,
        // roborev job 1753 blocker): the background scheduler starts a live daemon thread, and this
        // preflight (like RootAllocator below) is designed to throw at catalog load on a broken JVM
        // flag. Connector construction has no shutdown() hook to unwind on a thrown constructor —
        // Trino simply discards the half-built object — so constructing the thread-owning
        // SnapshotManager BEFORE either of these throw-prone steps would leak a scheduler thread (and
        // its periodic task) for the JVM's lifetime, once per failed catalog-load retry.
        ArrowMemoryPreflight.verify();
        this.allocator = new RootAllocator();
        this.flight = new CqliteFlightClient(allocator);
        // One shared snapshot manager (issues #2105, #2227): the split manager creates the
        // per-query snapshot on each replica host, the metadata cleans it up — both must see
        // the same registry. Each host's Sidecar is derived from the configured URI's scheme
        // + port (uniform across the hostNetwork Sidecar DaemonSet) and the split host.
        // The superseded-snapshot grace-sweep runs on a background best-effort scheduler (issue
        // #2452 item 2), NOT synchronously on the split-planning path — a hot table would otherwise
        // pay a multi-host DELETE fan-out in planning latency (roborev job 1722). The periodic tick
        // (cadence = the retire-grace period) is the ACTUAL #2367 fix: it prunes QUIET tables that
        // receive no further query, so a short reuse window plus a long TTL never accumulates
        // snapshot dirs — this is what closes the 714-snapshot field accumulation, independent of any
        // query ever running again. The rollback-retire (item 1) is a NARROWER, separate hardening —
        // it only fires on an UNCONTENDED creator's own fan-out failure (see the accepted hot-table
        // degradation on {@code Window#activeHolds}); the general (any-window, any-time)
        // active-retire is tracked as follow-up issue #2580, not this fix. Scheduler retired at
        // connector shutdown via SnapshotManager.close().
        this.snapshots = new SnapshotManager(
                HostSnapshotApis.fromBaseUri(config.sidecarUri()), config.readMode(), config.snapshotTtl(),
                config.snapshotReuseWindowNanos(), config.snapshotRetireGraceNanos(),
                new SnapshotManager.SystemClock(),
                new SnapshotRetireScheduler.BackgroundRetireScheduler(config.snapshotRetireGraceMillis()));
        // Started only after the SnapshotManager instance is fully constructed (this-escape nit,
        // roborev job 1753): registering the periodic hook from WITHIN the constructor would hand the
        // background thread a lambda closing over a not-yet-fully-initialized `this`.
        this.snapshots.start();
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
        // Close the scheduler FIRST (roborev job 1755 finding 3): retireAll() below runs its own
        // retirement synchronously on THIS thread (it never goes through the scheduler), so closing
        // the scheduler before calling it means no background sweep can still be running concurrently
        // with — or outlive — retireAll()'s drain or the allocator.close() that follows.
        snapshots.close();
        // Retire every live reused snapshot (issue #2356): a reused snapshot outlives the query
        // that created it, so retirement is not per-query — the connector releases them at
        // shutdown (the Sidecar TTL backstop covers a crash/miss).
        snapshots.retireAll();
        allocator.close();
    }
}
