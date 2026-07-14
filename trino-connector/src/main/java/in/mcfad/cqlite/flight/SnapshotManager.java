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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Owns the Sidecar snapshot lifecycle (issues #2105, #2227, #2356, #2306). One instance is
 * shared by the split manager (which resolves the snapshot at split-planning time and names
 * it in every ticket) and the metadata.
 *
 * <h2>Snapshot REUSE per {@code (keyspace, table)} within a freshness window (#2356/#2306)</h2>
 *
 * Instead of one snapshot per {@code queryId} — which cleared the prior dir every query and so
 * defeated the flight warm-handle investment (#2356) and flushed the memtable per query (#2306,
 * flush-on-snapshot is BY DESIGN per #2305) — a single snapshot is REUSED for a
 * {@code (keyspace, table)} across queries while it is fresh. The snapshot is named
 * {@code cqlite-<ks>-<table>-<epoch>} where {@code epoch} identifies the freshness window. N
 * queries in one window pay ONE create fan-out (→ one flush per host), not N, and the resolved
 * snapshot PATH stays stable so the flight server's warm readers stay warm with zero rebind
 * churn within the window (see {@code cqlite-flight}'s warm registry / {@code rebind_hits_total}).
 *
 * <p>A reused window is invalidated — forcing a fresh create on the next query — when the FIRST
 * of these occurs (design §C):
 * <ol>
 *   <li><b>Window expiry</b> — the freshness window elapses on the {@link Clock} seam. The clock
 *       is injectable so tests pin invalidation deterministically (never {@code System
 *       .currentTimeMillis}, per the #1742 pinned-{@code now} discipline).</li>
 *   <li><b>Generation-set change</b> — the observed live SSTable generation-set fingerprint for
 *       the table changed since the snapshot was taken (a flush/compaction). Authoritative, never
 *       guessed (#28). The fingerprint is passed by the caller; {@link #NO_GENERATION} means
 *       "not observed" and disables this lever (window + explicit refresh still apply).</li>
 *   <li><b>Explicit refresh</b> — {@link #invalidate(String, String)} (the {@code
 *       Database::refresh()} analog).</li>
 * </ol>
 * A reused snapshot is ALWAYS a valid immutable Cassandra point-in-time (snapshots are hardlinks);
 * invalidation is a FRESHNESS lever, not a correctness requirement. The staleness bound is thus
 * {@code min(window, time-since-last-generation-change)}, documented in the connector docs.
 *
 * <p><b>Per-host, fail-closed create model preserved (#2227).</b> A Sidecar snapshot PUT is
 * instance-local, so the snapshot is created on EVERY replica host a split will read, memoized
 * per {@code (window, host)}. {@link #snapshotFor} fails closed on the first host whose create
 * errors (naming host + snapshot); {@link #availableHosts} is best-effort for optional fallback
 * hosts. A SUPERSEDED window is NOT actively retired (issue #2356 roborev, retire-race): a
 * long-running query may still be reading its snapshot when a later query rolls to a fresh window,
 * so superseded windows are reclaimed solely by the Sidecar-side TTL backstop
 * ({@link CqliteFlightConfig#snapshotTtl}). Active retirement ({@link #retire}) happens ONLY on
 * explicit {@link #invalidate} and {@link #retireAll} (shutdown).
 *
 * <p>In {@link ReadMode#LIVE} this manager is inert: {@link #snapshotFor} returns empty
 * (ticket {@code snapshot=null}), no reuse cache entry is created, and no Sidecar calls are made.
 */
public final class SnapshotManager {
    private static final Logger LOG = Logger.getLogger(SnapshotManager.class.getName());

    /** Strip anything outside the Sidecar-safe charset from a name component. */
    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

    /**
     * Sentinel generation fingerprint meaning "the caller did not observe the generation set".
     * When passed, the generation-set-change invalidation lever is disabled (window + explicit
     * refresh still apply). The flight server does not yet expose a generation fingerprint, so
     * the split-manager call path uses this; the fingerprint-taking overload exists so a future
     * server-exposed fingerprint (design §C.2) wires in without an API change.
     */
    public static final long NO_GENERATION = Long.MIN_VALUE;

    private final HostSnapshotApis sidecars;
    private final ReadMode readMode;
    private final Optional<String> ttl;
    private final long reuseWindowNanos;
    private final Clock clock;

    /** The current reused snapshot window per {@code (keyspace, table)}. */
    private final Map<TableRef, Window> windows = new ConcurrentHashMap<>();
    /** Monotonic freshness-window epoch counter per {@code (keyspace, table)}. */
    private final Map<TableRef, AtomicLong> epochs = new ConcurrentHashMap<>();

    /** Snapshot create fan-outs performed (one per freshness window) — the #2306 flush proxy. */
    private final AtomicLong snapshotCreationsTotal = new AtomicLong();
    /** Queries served by an already-live (reused) snapshot — the reuse-factor numerator. */
    private final AtomicLong snapshotReuseHitsTotal = new AtomicLong();

    /** A logical clock seam so tests pin window timing deterministically (no wall-clock). */
    public interface Clock {
        /** A monotonically non-decreasing nanosecond reading (never wall-clock in tests). */
        long nanoTime();
    }

    /** Production clock: {@link System#nanoTime()}. */
    public static final class SystemClock implements Clock {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }
    }

    /** The logical-table half of the reuse cache key. */
    private record TableRef(String keyspace, String table) {}

    /**
     * One reused snapshot window: its name, the freshness epoch, when it was taken (on the
     * injectable clock), the generation fingerprint it was taken over, and the per-host create
     * memoization (create at most once per host for this window's snapshot).
     */
    static final class Window {
        final String name;
        final long epoch;
        final long createdNanos;
        final long generationFingerprint;
        final Map<String, CompletableFuture<String>> hostCreates = new ConcurrentHashMap<>();

        Window(String name, long epoch, long createdNanos, long generationFingerprint) {
            this.name = name;
            this.epoch = epoch;
            this.createdNanos = createdNanos;
            this.generationFingerprint = generationFingerprint;
        }

        /** The resolved snapshot name stamped into this window's tickets. */
        String name() {
            return name;
        }
    }

    /**
     * Full constructor (production + tests). {@code reuseWindowNanos <= 0} disables reuse (every
     * query gets a fresh window — the pre-#2356 per-query cadence, without the queryId naming).
     */
    public SnapshotManager(
            HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl,
            long reuseWindowNanos, Clock clock) {
        this.sidecars = sidecars;
        this.readMode = readMode;
        this.ttl = ttl;
        this.reuseWindowNanos = Math.max(0L, reuseWindowNanos);
        this.clock = clock;
    }

    /**
     * Convenience constructor: the default freshness window on the production {@link SystemClock}.
     * Used where a config-driven window is not threaded through (and by tests that do not exercise
     * window timing).
     */
    public SnapshotManager(HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl) {
        this(sidecars, readMode, ttl,
                CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_NANOS, new SystemClock());
    }

    /** Snapshot create fan-outs performed so far (one per freshness window). */
    public long snapshotCreationsTotal() {
        return snapshotCreationsTotal.get();
    }

    /** Queries served by an already-live reused snapshot so far. */
    public long snapshotReuseHitsTotal() {
        return snapshotReuseHitsTotal.get();
    }

    /**
     * The snapshot name to put in this scan's tickets, reusing the current fresh {@code (keyspace,
     * table)} snapshot when possible (design §B, #2356/#2306) and otherwise creating a fresh one
     * on the Sidecar of every replica host a split will read (#2227). Fails closed on the first
     * host whose create errors, naming that host + snapshot.
     *
     * @param hosts every distinct replica host the scan's splits will read
     * @return {@link Optional#empty()} in {@link ReadMode#LIVE}; otherwise the reused/created
     *         snapshot's name ({@code cqlite-<ks>-<table>-<epoch>}).
     * @throws SidecarClient.SidecarException if creation fails on any host (fail closed)
     */
    public Optional<String> snapshotFor(String keyspace, String table, Collection<String> hosts) {
        return snapshotFor(keyspace, table, hosts, NO_GENERATION);
    }

    /**
     * As {@link #snapshotFor(String, String, Collection)}, additionally invalidating a reused
     * snapshot whose {@code generationFingerprint} differs from the current live generation set
     * (design §C.2). {@link #NO_GENERATION} disables that lever.
     */
    public Optional<String> snapshotFor(
            String keyspace, String table, Collection<String> hosts, long generationFingerprint) {
        return resolveSnapshot(keyspace, table, hosts, generationFingerprint).map(Window::name);
    }

    /**
     * Resolve the reuse window ONCE (reusing the current fresh one or creating a fresh one on the
     * Sidecar of every replica host in {@code hosts}, fail-closed) and return the resolved
     * {@link Window}, so the caller can thread the EXACT same window into {@link #availableHosts}
     * rather than independently re-resolving it (issue #2356 roborev, double-resolve race: if the
     * freshness window elapsed between a separate {@code snapshotFor} + {@code availableHosts} pair,
     * the two could resolve DIFFERENT windows, decoupling the tickets from the per-host snapshot
     * actually ensured). Returns {@link Optional#empty()} in {@link ReadMode#LIVE}.
     */
    public Optional<Window> resolveSnapshot(String keyspace, String table, Collection<String> hosts) {
        return resolveSnapshot(keyspace, table, hosts, NO_GENERATION);
    }

    /** As {@link #resolveSnapshot(String, String, Collection)} with a generation fingerprint. */
    public Optional<Window> resolveSnapshot(
            String keyspace, String table, Collection<String> hosts, long generationFingerprint) {
        if (readMode == ReadMode.LIVE) {
            return Optional.empty();
        }
        Window window = resolveWindow(new TableRef(keyspace, table), generationFingerprint, true);
        for (String host : hosts) {
            createOnHost(window, host, keyspace, table);
        }
        return Optional.of(window);
    }

    /**
     * Best-effort per-host snapshot availability for a candidate host set (#2241): ensures the
     * ALREADY-RESOLVED {@code window}'s snapshot exists on every host in {@code hosts} (the same
     * memoized per-(window, host) create as {@link #resolveSnapshot}, so a host already created
     * there resolves with no duplicate PUT), but — unlike the fail-closed resolve — does NOT fail
     * closed on an individual host's creation failure (a failed host is logged and excluded).
     * Counters are NOT touched here: this is part of the SAME query's planning as the preceding
     * {@link #resolveSnapshot} (which already counted the create/reuse), so it reuses that window
     * without double-counting.
     *
     * <p>The caller passes the {@link Window} returned by {@link #resolveSnapshot} so both operate
     * on the EXACT same window even if the freshness window would otherwise have rolled between the
     * two calls (issue #2356 roborev, double-resolve race).
     *
     * @param window the window resolved for this query by {@link #resolveSnapshot}
     * @param hosts every candidate replica host to check/create snapshot availability for
     * @return the subset of {@code hosts} that have (or now have) the snapshot
     */
    public Set<String> availableHosts(
            Window window, String keyspace, String table, Collection<String> hosts) {
        Set<String> available = new LinkedHashSet<>();
        for (String host : hosts) {
            try {
                createOnHost(window, host, keyspace, table);
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
     * Force a fresh snapshot for {@code (keyspace, table)} on the next query (design §C.3, the
     * {@code Database::refresh()} analog): drop the current reused window and retire its snapshots.
     */
    public void invalidate(String keyspace, String table) {
        TableRef ref = new TableRef(keyspace, table);
        Window removed = windows.remove(ref);
        if (removed != null) {
            retire(removed, ref);
        }
    }

    /**
     * Retire every live reused window (connector shutdown): best-effort clear of each window's
     * snapshots on every host it was created on. The Sidecar TTL backstop covers any miss.
     */
    public void retireAll() {
        for (Map.Entry<TableRef, Window> e : windows.entrySet()) {
            Window w = windows.remove(e.getKey());
            if (w != null) {
                retire(w, e.getKey());
            }
        }
    }

    /**
     * Resolve the current reused window for {@code ref}, reusing an existing fresh window or
     * creating a new one (bumping the epoch) atomically. When {@code countStats}, records exactly
     * one {@link #snapshotReuseHitsTotal} (reuse) or {@link #snapshotCreationsTotal} (create).
     *
     * <p><b>A superseded window is NOT retired here (issue #2356 roborev, retire-race).</b> A
     * long-running query A can still be reading a window's snapshot (splits not yet opened, or a
     * cold remote host) when a later query B rolls to a fresh window; actively deleting A's snapshot
     * dir on supersede would break A's in-flight reads. Superseded windows are therefore reclaimed
     * solely by the Sidecar-side TTL backstop ({@link CqliteFlightConfig#snapshotTtl}). Active
     * retirement ({@link #retire}) happens ONLY on explicit {@link #invalidate} (the
     * {@code Database::refresh()} analog) and {@link #retireAll} (shutdown), never as a side effect
     * of rolling to a new window.
     */
    private Window resolveWindow(TableRef ref, long generationFingerprint, boolean countStats) {
        long now = clock.nanoTime();
        boolean[] reused = {false};
        Window window = windows.compute(ref, (k, existing) -> {
            if (existing != null && isFresh(existing, generationFingerprint, now)) {
                reused[0] = true;
                return existing;
            }
            long epoch = epochs.computeIfAbsent(k, r -> new AtomicLong()).getAndIncrement();
            return new Window(nameFor(k, epoch), epoch, now, generationFingerprint);
        });
        if (countStats) {
            if (reused[0]) {
                snapshotReuseHitsTotal.incrementAndGet();
            } else {
                snapshotCreationsTotal.incrementAndGet();
            }
        }
        return window;
    }

    /**
     * Whether {@code w} is still reusable at {@code now}: within the freshness window AND (when a
     * generation fingerprint was observed) taken over the same generation set. {@code now -
     * createdNanos} uses long subtraction so a {@link System#nanoTime()} origin never trips it.
     */
    private boolean isFresh(Window w, long generationFingerprint, long now) {
        if (now - w.createdNanos >= reuseWindowNanos) {
            return false;
        }
        return generationFingerprint == NO_GENERATION || w.generationFingerprint == generationFingerprint;
    }

    /**
     * Create the snapshot on one host, once per {@code (window, host)}.
     *
     * <p>Per-host-future memoization: the PUT happens at most once per window even if Trino
     * re-plans the same scan — WITHOUT ever holding a ConcurrentHashMap bin lock across the (up to
     * 30s) network call. We insert an empty future under the bin lock (cheap); only the thread that
     * won the insert performs the Sidecar PUT OUTSIDE the lock, then completes the future. Every
     * other caller joins that future's result.
     */
    private void createOnHost(Window window, String host, String keyspace, String table) {
        CompletableFuture<String> mine = new CompletableFuture<>();
        CompletableFuture<String> winner = window.hostCreates.putIfAbsent(host, mine);
        if (winner == null) {
            // We won: do the network I/O off-lock, then publish the result.
            try {
                sidecars.forHost(host).createSnapshot(keyspace, table, window.name, ttl);
            } catch (Throwable t) {
                // Fail closed AND never leave an incomplete future behind (roborev, #2113): on ANY
                // throw drop the future so a retry can recompute, and fail it so concurrent joiners
                // unblock with the same error instead of blocking forever. RuntimeExceptions are
                // wrapped into an actionable error naming the host + snapshot (#2227); Errors and
                // sneaky checked throwables keep their identity so liveness/error-propagation
                // invariants hold (see SnapshotManagerTest).
                window.hostCreates.remove(host, mine);
                Throwable toThrow = (t instanceof RuntimeException re)
                        ? actionable(host, keyspace, table, window.name, re) : t;
                mine.completeExceptionally(toThrow);
                if (toThrow instanceof RuntimeException re) {
                    throw re;
                }
                if (toThrow instanceof Error err) {
                    throw err;
                }
                throw new IllegalStateException("Unexpected checked throwable from createSnapshot", toThrow);
            }
            mine.complete(window.name);
            return;
        }
        // Someone else is creating (or already created) it — adopt their result. If their create
        // failed, join() rethrows the same failure here (still fail-closed), and the failing thread
        // already removed the future so the next call recomputes.
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
    private static SidecarClient.SidecarException actionable(
            String host, String keyspace, String table, String name, RuntimeException cause) {
        int status = (cause instanceof SidecarClient.SidecarException se) ? se.statusCode() : -1;
        return new SidecarClient.SidecarException(
                "Failed to create snapshot '" + name + "' on replica host " + host
                        + " for " + keyspace + "." + table
                        + " — snapshot read-mode fails closed: every replica host a split reads must have "
                        + "the snapshot (per-host creation, issue #2227). Cause: " + cause.getMessage(),
                status, cause);
    }

    /**
     * Best-effort delete of every snapshot an EXPLICITLY-invalidated {@code window} created (via
     * {@link #invalidate} or {@link #retireAll} only — never on supersede, see {@link #resolveWindow}),
     * on each host it was created on. Individual delete failures are logged and swallowed — the
     * Sidecar TTL backstop covers a miss.
     */
    private void retire(Window window, TableRef ref) {
        for (Map.Entry<String, CompletableFuture<String>> e : window.hostCreates.entrySet()) {
            String host = e.getKey();
            String name;
            try {
                // Successful creates leave a completed future; a create that failed already removed
                // its own future, so anything still present resolves to a real snapshot name.
                name = e.getValue().join();
            } catch (RuntimeException ex) {
                continue; // a create that raced to failure: nothing was made, nothing to delete.
            }
            try {
                sidecars.forHost(host).clearSnapshot(ref.keyspace(), ref.table(), name);
            } catch (RuntimeException ex) {
                LOG.log(Level.WARNING,
                        () -> "Best-effort snapshot retirement failed for " + ref.keyspace() + "." + ref.table()
                                + " on host " + host + " snapshot=" + name
                                + " (TTL backstop will reclaim it): " + ex.getMessage());
            }
        }
    }

    /** The reused-window snapshot name {@code cqlite-<ks>-<table>-<epoch>}. */
    static String nameFor(String keyspace, String table, long epoch) {
        return "cqlite-" + sanitize(keyspace) + "-" + sanitize(table) + "-" + epoch;
    }

    private static String nameFor(TableRef ref, long epoch) {
        return nameFor(ref.keyspace(), ref.table(), epoch);
    }

    /** Strip unsafe chars and lowercase a name component for the Sidecar-safe charset. */
    static String sanitize(String component) {
        return UNSAFE.matcher(component).replaceAll("_").toLowerCase(Locale.ROOT);
    }
}
