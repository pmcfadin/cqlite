package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Owns the per-query Sidecar snapshot lifecycle (issues #2105, #2227). One instance is
 * shared by the split manager (which creates the snapshot at split-planning time and names
 * it in every ticket) and the metadata (whose {@code cleanupQuery} best-effort deletes it
 * at query end).
 *
 * <h2>Design: per-query snapshot named {@code cqlite-<queryId>}, created on EVERY replica host</h2>
 * <ul>
 *   <li><b>Create</b> at split planning ({@link CqliteFlightSplitManager#getSplits}), once
 *       per (query, host, keyspace, table) — memoized so re-planning never double-PUTs.
 *       A Sidecar snapshot {@code PUT} is instance-local (issue #2227): it only creates the
 *       snapshot on the node fronting that one Sidecar. Because the scan's splits fan out
 *       across every replica host, the snapshot must be created on each host's own Sidecar
 *       (via {@link HostSnapshotApis}); otherwise a split on any other host reads a
 *       non-existent directory and fails NotFound. Every split names the same snapshot, so
 *       the whole scan reads one immutable file set per host while Cassandra compacts
 *       underneath.</li>
 *   <li><b>Fail closed</b> in {@link ReadMode#SNAPSHOT}: if creation errors on ANY host we
 *       propagate an actionable error naming the host + snapshot and the query fails. We
 *       never silently fall back to a live read — a silent fallback would hand back a
 *       compaction-racing result the operator asked to avoid, and a NotFound on a
 *       not-created host would be an opaque failure.</li>
 *   <li><b>Clean up</b> best-effort at query end ({@link #cleanup}) on each host the snapshot
 *       was created on; a failed delete is logged, not fatal. A Sidecar-side TTL (see
 *       {@link CqliteFlightConfig#snapshotTtl}) is the backstop so a coordinator crash
 *       between create and cleanup can't leak the snapshot permanently.</li>
 * </ul>
 *
 * <p>In {@link ReadMode#LIVE} this manager is inert: {@link #snapshotFor} returns empty
 * (ticket {@code snapshot=null}, the pre-#2105 behavior) and no Sidecar calls are made.
 */
public final class SnapshotManager {
    private static final Logger LOG = Logger.getLogger(SnapshotManager.class.getName());

    /** Trino query ids are safe already; strip anything outside the Sidecar-safe charset. */
    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

    private final HostSnapshotApis sidecars;
    private final ReadMode readMode;
    private final Optional<String> ttl;

    /** queryId → ((host, keyspace, table) → snapshotName) created for that query. */
    private final Map<String, Map<SnapshotKey, CompletableFuture<String>>> created = new ConcurrentHashMap<>();

    /** One created (or in-flight) snapshot: which Sidecar host, and which table. */
    private record SnapshotKey(String host, String keyspace, String table) {}

    public SnapshotManager(HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl) {
        this.sidecars = sidecars;
        this.readMode = readMode;
        this.ttl = ttl;
    }

    /**
     * The snapshot name to put in this scan's tickets, creating the snapshot on the Sidecar
     * of every replica host a split will read (issue #2227). Fails closed on the first host
     * whose create errors, naming that host + snapshot.
     *
     * @param hosts every distinct replica host the scan's splits will read
     * @return {@link Optional#empty()} in {@link ReadMode#LIVE}; otherwise the created
     *         snapshot's name ({@code cqlite-<queryId>}).
     * @throws SidecarClient.SidecarException if creation fails on any host (fail closed)
     */
    public Optional<String> snapshotFor(String queryId, String keyspace, String table, Collection<String> hosts) {
        if (readMode == ReadMode.LIVE) {
            return Optional.empty();
        }
        String name = snapshotName(queryId);
        Map<SnapshotKey, CompletableFuture<String>> forQuery =
                created.computeIfAbsent(queryId, q -> new ConcurrentHashMap<>());
        for (String host : hosts) {
            createOnHost(forQuery, new SnapshotKey(host, keyspace, table), name);
        }
        return Optional.of(name);
    }

    /**
     * Best-effort per-host snapshot availability for a candidate host set (issue #2241):
     * attempts creation (the same memoized per-(query, host, keyspace, table) fan-out as
     * {@link #snapshotFor}, so a host already created there — e.g. a split's primary — resolves
     * instantly with no duplicate PUT) on every host in {@code hosts}, but — unlike {@link
     * #snapshotFor} — does NOT fail closed on an individual host's creation failure. A host
     * whose creation fails is logged and excluded from the returned set; the caller ({@link
     * CqliteFlightSplitManager#getSplits}) uses this to restrict availability-failover fallback
     * lists to hosts that actually have the snapshot, without failing the whole query over an
     * optional fallback host (a required PRIMARY host still fails closed via {@link
     * #snapshotFor}).
     *
     * <p>In {@link ReadMode#LIVE} this is a no-op returning every candidate host unchanged
     * (fallback restriction is meaningful only in snapshot mode).
     *
     * @param hosts every candidate replica host to check/create snapshot availability for
     * @return the subset of {@code hosts} that have (or now have) the snapshot
     */
    public Set<String> availableHosts(String queryId, String keyspace, String table, Collection<String> hosts) {
        if (readMode == ReadMode.LIVE) {
            return Set.copyOf(hosts);
        }
        String name = snapshotName(queryId);
        Map<SnapshotKey, CompletableFuture<String>> forQuery =
                created.computeIfAbsent(queryId, q -> new ConcurrentHashMap<>());
        Set<String> available = new LinkedHashSet<>();
        for (String host : hosts) {
            SnapshotKey key = new SnapshotKey(host, keyspace, table);
            try {
                createOnHost(forQuery, key, name);
                available.add(host);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING,
                        () -> "Snapshot unavailable on replica host " + host + " for " + keyspace + "." + table
                                + " (issue #2241): excluded from availability-failover fallback "
                                + "candidates — " + e.getMessage());
            }
        }
        return available;
    }

    /**
     * Create the snapshot on one host, once per (query, host, keyspace, table).
     *
     * <p>Per-key-future memoization: the PUT happens at most once even if Trino re-plans the
     * same scan — WITHOUT ever holding a ConcurrentHashMap bin lock across the (up to 30s)
     * network call. We insert an empty future under the bin lock (cheap); only the thread
     * that won the insert performs the Sidecar PUT OUTSIDE the lock, then completes the
     * future. Every other caller joins that future's result.
     */
    private void createOnHost(Map<SnapshotKey, CompletableFuture<String>> forQuery, SnapshotKey key, String name) {
        CompletableFuture<String> mine = new CompletableFuture<>();
        CompletableFuture<String> winner = forQuery.putIfAbsent(key, mine);
        if (winner == null) {
            // We won: do the network I/O off-lock, then publish the result.
            try {
                sidecars.forHost(key.host()).createSnapshot(key.keyspace(), key.table(), name, ttl);
            } catch (Throwable t) {
                // Fail closed AND never leave an incomplete future behind (roborev, #2113):
                // on ANY throw drop the future so a retry can recompute, and fail it so
                // concurrent joiners unblock with the same error instead of blocking forever.
                // cleanup therefore never sees (and never tries to delete) a snapshot that was
                // never made. RuntimeExceptions are wrapped into an actionable error naming the
                // host + snapshot (issue #2227); Errors and sneaky checked throwables keep their
                // identity so liveness/error-propagation invariants hold (see SnapshotManagerTest).
                forQuery.remove(key, mine);
                Throwable toThrow = (t instanceof RuntimeException re) ? actionable(key, name, re) : t;
                mine.completeExceptionally(toThrow);
                if (toThrow instanceof RuntimeException re) {
                    throw re;
                }
                if (toThrow instanceof Error err) {
                    throw err;
                }
                throw new IllegalStateException("Unexpected checked throwable from createSnapshot", toThrow);
            }
            mine.complete(name);
            return;
        }
        // Someone else is creating (or already created) it — adopt their result. If their
        // create failed, join() rethrows the same failure here (still fail-closed), and the
        // failing thread already removed the future so the next call recomputes.
        try {
            winner.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    /** Wrap a per-host create failure into a fail-closed error naming host + snapshot. */
    private static SidecarClient.SidecarException actionable(SnapshotKey key, String name, RuntimeException cause) {
        int status = (cause instanceof SidecarClient.SidecarException se) ? se.statusCode() : -1;
        return new SidecarClient.SidecarException(
                "Failed to create snapshot '" + name + "' on replica host " + key.host()
                        + " for " + key.keyspace() + "." + key.table()
                        + " — snapshot read-mode fails closed: every replica host a split reads must have "
                        + "the snapshot (per-host creation, issue #2227). Cause: " + cause.getMessage(),
                status, cause);
    }

    /**
     * Best-effort delete of every snapshot created for {@code queryId}, on each host it was
     * created on. Called from {@code ConnectorMetadata.cleanupQuery} at query teardown
     * (success or failure). Individual delete failures are logged and swallowed — the TTL
     * backstop covers a miss — but every (host, table) is attempted regardless of earlier
     * failures.
     */
    public void cleanup(String queryId) {
        Map<SnapshotKey, CompletableFuture<String>> forQuery = created.remove(queryId);
        if (forQuery == null) {
            return;
        }
        for (Map.Entry<SnapshotKey, CompletableFuture<String>> e : forQuery.entrySet()) {
            SnapshotKey key = e.getKey();
            String name;
            try {
                // Successful creates leave a completed future here; a create that failed
                // already removed its own future, so anything still present resolves to a
                // real snapshot name (join blocks only for a rare concurrent in-flight
                // create, then returns it).
                name = e.getValue().join();
            } catch (RuntimeException ex) {
                // A create that raced to failure: nothing was made, nothing to delete.
                continue;
            }
            try {
                sidecars.forHost(key.host()).clearSnapshot(key.keyspace(), key.table(), name);
            } catch (RuntimeException ex) {
                LOG.log(Level.WARNING,
                        () -> "Best-effort snapshot cleanup failed for " + key.keyspace() + "." + key.table()
                                + " on host " + key.host() + " snapshot=" + name
                                + " (TTL backstop will reclaim it): " + ex.getMessage());
            }
        }
    }

    /** The deterministic per-query snapshot name. */
    static String snapshotName(String queryId) {
        String safe = UNSAFE.matcher(queryId).replaceAll("_").toLowerCase(Locale.ROOT);
        return "cqlite-" + safe;
    }
}
