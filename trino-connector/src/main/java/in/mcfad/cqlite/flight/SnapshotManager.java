package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * hosts.
 *
 * <p><b>Bounded retirement of superseded windows (issue #2356 roborev, resource retention).</b> A
 * window is NOT deleted the instant it is superseded (window expiry / generation change): a
 * long-running query may still be reading its snapshot when a later query rolls to a fresh window,
 * and deleting the hardlink set out from under an in-flight read is the retire-race. But it must
 * ALSO not leak until the ~6h Sidecar TTL backstop — a hot table with a 3s window would then
 * accumulate on the order of {@code ttl / window} live snapshot dirs (each a full hardlink set) per
 * table per host. So a superseded window is enqueued and actively retired ({@link #retire}) after a
 * <b>grace period</b> ({@code retireGraceNanos}) configured to safely exceed the longest Trino query
 * (see {@link CqliteFlightConfig#DEFAULT_SNAPSHOT_RETIRE_GRACE_MILLIS}): once the grace elapses no
 * query that resolved that window can still be planning/reading it, so the delete is race-free.
 * Retirement is swept lazily on the next {@link #resolveSnapshot} (no background thread) and bounds
 * steady-state retained superseded dirs to roughly {@code grace / window} per table per host, well
 * under the TTL. Explicit {@link #invalidate} still retires the current window immediately (the
 * {@code Database::refresh()} analog) and {@link #retireAll} (shutdown) drains both the live windows
 * and the pending-retire queue; the Sidecar-side TTL backstop ({@link CqliteFlightConfig#snapshotTtl})
 * still covers a coordinator crash between supersede and sweep.
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
    private final long retireGraceNanos;
    private final Clock clock;
    /**
     * Where superseded-window retirement sweeps run (issue #2452 item 2). Default is
     * {@link SnapshotRetireScheduler.InlineRetireScheduler} (synchronous, thread-free); production
     * ({@link CqliteFlightConnector}) injects {@link SnapshotRetireScheduler.BackgroundRetireScheduler}
     * so the DELETE fan-out is offloaded off the split-planning path and a periodic tick prunes
     * quiet tables.
     */
    private final SnapshotRetireScheduler retireScheduler;
    /** Guards {@link #start()} so a second call cannot double-register the periodic cadence
     * (roborev job 1754 finding 6). */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** The current reused snapshot window per {@code (keyspace, table)}. */
    private final Map<TableRef, Window> windows = new ConcurrentHashMap<>();
    /** Monotonic freshness-window epoch counter per {@code (keyspace, table)}. */
    private final Map<TableRef, AtomicLong> epochs = new ConcurrentHashMap<>();
    /**
     * Superseded windows awaiting grace-period retirement, oldest first (the clock is
     * non-decreasing and windows are enqueued in supersede order, so peek() is the oldest).
     */
    private final Queue<PendingRetire> pendingRetire = new ConcurrentLinkedQueue<>();

    /** A superseded window and the clock reading at which it was superseded (grace starts here). */
    private record PendingRetire(TableRef ref, Window window, long supersededNanos) {}

    /** The outcome of resolving a reuse window: the window plus whether it was reused (vs created). */
    private record Resolved(Window window, boolean reused) {}

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
        /**
         * NOT a true refcount (roborev job 1755 finding 2, renamed from {@code holders}) — a
         * one-shot safety latch: incremented atomically under the {@code windows.compute} bin lock
         * when a query resolves (creates or reuses) this window, but decremented ONLY by a query's
         * OWN failing rollback and NEVER by a successful one, since there is no query/read-completion
         * hook to release a success (tracked as follow-up issue #2580). It guards only the CREATOR's
         * own rollback against stranding a concurrent reuser that already committed this window to
         * its tickets — it is not a general liveness/usage count.
         *
         * <p><b>ACCEPTED hot-table active-retire degradation, pinned so it is discoverable in
         * code+test rather than the field:</b> because a successful hold is never released,
         * {@code activeHolds} is permanently &ge;1 the moment ANY second query has ever resolved a
         * given window — so on essentially any table with real concurrent traffic, the active
         * rollback-retire this field enables degrades to a permanent no-op: the failing query's own
         * rollback always finds {@code remaining > 0} and leaves its partial per-host creates to the
         * grace/TTL backstop. This is the deliberately SAFE direction (a suppressed retire only ever
         * costs a bounded, already-covered leak; an unconditional retire could strand a live reader).
         * See issue #2580 for the read-completion-hook follow-up, and
         * {@code SnapshotManagerRetireHardeningTest
         * #successfulReuseHoldIsPermanentSoRollbackRetireDegradesToNoOpOnHotTables}, which pins both
         * the suppression and its permanence.
         */
        final AtomicInteger activeHolds = new AtomicInteger();
        /**
         * Set (not a run-once gate) the first time {@link #retire} is invoked for this window —
         * tracked for tests/observability, per roborev job 1755 finding 4. {@code retire()} MAY
         * legitimately run more than once for the same window (see the hazard note on the rollback
         * catch block in {@code resolveSnapshot}); this flag intentionally does NOT skip a repeat
         * invocation, since doing so would silently reintroduce the finding-1 leak (a later call can
         * still need to clear hosts a racing {@link #invalidate}/{@link #retireAll} created before it
         * ran).
         */
        final AtomicBoolean retired = new AtomicBoolean(false);

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
     * {@code retireGraceNanos} is the delay after a window is superseded before it is actively
     * retired (issue #2356 roborev, bounded retention); it must safely exceed the longest query so
     * an in-flight read never loses its snapshot.
     */
    public SnapshotManager(
            HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl,
            long reuseWindowNanos, long retireGraceNanos, Clock clock) {
        this(sidecars, readMode, ttl, reuseWindowNanos, retireGraceNanos, clock,
                new SnapshotRetireScheduler.InlineRetireScheduler());
    }

    /**
     * DI constructor (issue #2452 item 2): the caller supplies the {@link SnapshotRetireScheduler}.
     * Production passes {@link SnapshotRetireScheduler.BackgroundRetireScheduler} (offload +
     * periodic quiet-table sweep); tests pass a controllable or inline scheduler for determinism.
     *
     * <p>Does NOT register the periodic sweep itself — call {@link #start()} once construction
     * (and, in production, the caller's own field assignment) has fully completed. Registering the
     * periodic hook here would pass a lambda closing over {@code this} to the scheduler BEFORE the
     * constructor returns (a partial-construction / this-escape hazard, Java reviewer nit on #2452):
     * the background thread could invoke {@code sweepRetireDue} on a not-yet-fully-initialized
     * instance. {@link #start()} is a no-op for the inline scheduler either way.
     */
    public SnapshotManager(
            HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl,
            long reuseWindowNanos, long retireGraceNanos, Clock clock,
            SnapshotRetireScheduler retireScheduler) {
        this.sidecars = sidecars;
        this.readMode = readMode;
        this.ttl = ttl;
        this.reuseWindowNanos = Math.max(0L, reuseWindowNanos);
        this.retireGraceNanos = Math.max(0L, retireGraceNanos);
        this.clock = clock;
        this.retireScheduler = retireScheduler;
    }

    /**
     * Start the periodic quiet-table sweep (issue #2452 item 2, #2367 accumulation fix): a table
     * that receives no further query must still have its superseded-window backlog pruned rather
     * than accumulating until the multi-hour TTL. Call ONCE, after the instance (and, in production,
     * every field of the owning object) is fully constructed — never from within the constructor
     * itself (this-escape). A no-op for a scheduler whose {@code startPeriodic} does not tick (e.g.
     * {@link SnapshotRetireScheduler.InlineRetireScheduler}).
     *
     * <p>Idempotent (roborev job 1754 finding 6): a second call is a silent no-op so double-starting
     * (e.g. a duplicate wiring mistake) cannot double the periodic sweep cadence.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // Deliberate semantic shift (roborev job 1754 finding 5): the periodic tick evaluates grace
        // due-ness against `clock.nanoTime()` read AT TICK TIME, not any `now` captured at start()
        // call time — each tick must see the CURRENT clock, not a stale snapshot from registration.
        retireScheduler.startPeriodic(() -> sweepRetireDue(clock.nanoTime()));
    }

    /**
     * Constructor without an explicit retire-grace: uses the default grace
     * ({@link CqliteFlightConfig#DEFAULT_SNAPSHOT_RETIRE_GRACE_NANOS}). Kept for existing call sites
     * that do not pin grace-period retirement timing.
     */
    public SnapshotManager(
            HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl,
            long reuseWindowNanos, Clock clock) {
        this(sidecars, readMode, ttl, reuseWindowNanos,
                CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_NANOS, clock);
    }

    /**
     * Convenience constructor: the default freshness window + retire grace on the production
     * {@link SystemClock}. Used where config-driven timing is not threaded through (and by tests
     * that do not exercise window/grace timing).
     */
    public SnapshotManager(HostSnapshotApis sidecars, ReadMode readMode, Optional<String> ttl) {
        this(sidecars, readMode, ttl, CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_NANOS,
                CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_NANOS, new SystemClock());
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
        TableRef ref = new TableRef(keyspace, table);
        Resolved resolved = resolveWindow(ref, generationFingerprint);
        Window window = resolved.window();
        try {
            for (String host : hosts) {
                createOnHost(window, host, keyspace, table);
            }
        } catch (RuntimeException e) {
            // Roborev (issue #2356, half-created window): a fail-closed fan-out must not leave a
            // freshly-created window cached as "fresh" (a later query would reuse a snapshot that
            // never fully materialized) nor count a create/flush that did not complete. A REUSED
            // window is left entirely alone here (never removed, never retired): a transient host
            // error on an added fallback host must not nuke a live shared window.
            if (!resolved.reused()) {
                // Best-effort: a no-op if invalidate()/retireAll() already removed this EXACT window
                // out from under this in-flight resolve — see the liveness check below, which is what
                // actually decides whether to retire, not whether THIS call's removal won.
                windows.remove(ref, window);
            }
            int remaining = window.activeHolds.decrementAndGet();
            // Retire once nobody can still JOIN this window as a holder: true whenever `window` is no
            // longer the live `windows[ref]` entry — whether *I* just removed it above, or
            // invalidate()/retireAll() removed it earlier while my OWN fan-out was still adding hosts
            // (issue #2452 roborev job 1755 finding 1: the prior `removedFresh`-only gate — "did MY
            // OWN remove call win" — missed exactly this: an ordinary invalidate() racing an in-flight
            // creator retires whatever existed at THAT moment and returns, then the creator goes on to
            // create MORE hosts that nobody ever cleans up). `remaining == 0` alone already proves no
            // reader can be stranded (a live successful hold never releases — see `activeHolds`), so
            // checking liveness too only adds "retire promptly" cases, never an unsafe one. A REUSED
            // window's rollback never reaches here (guarded above).
            //
            // HAZARD (roborev job 1754 f.1 / job 1755 f.4): retire() below CAN run more than once for
            // the same window — BY DESIGN, to catch hosts created after an earlier call already ran.
            // `Window#retired` is a tracked fact for tests, not a run-once gate (gating it would
            // silently reintroduce the leak just described). Re-invocation is safe regardless: a
            // repeat clearSnapshot swallows its own 404/NotFound, so every call converges on the same
            // outcome. See {@code SnapshotManagerRetireHardeningTest} (double-retire + invalidate-race
            // tests).
            if (!resolved.reused() && remaining == 0 && windows.get(ref) != window) {
                retire(window, ref);
            }
            throw e;
        }
        // Count (the #2306 flush proxy) only once the full fan-out succeeded: exactly one reuse hit
        // or one create per resolved window.
        if (resolved.reused()) {
            snapshotReuseHitsTotal.incrementAndGet();
        } else {
            snapshotCreationsTotal.incrementAndGet();
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
     * Retire every live reused window AND every superseded window still awaiting grace-period
     * retirement (connector shutdown): best-effort clear of each window's snapshots on every host it
     * was created on. The Sidecar TTL backstop covers any miss.
     */
    public void retireAll() {
        for (Map.Entry<TableRef, Window> e : windows.entrySet()) {
            Window w = windows.remove(e.getKey());
            if (w != null) {
                retire(w, e.getKey());
            }
        }
        PendingRetire pending;
        while ((pending = pendingRetire.poll()) != null) {
            retire(pending.window(), pending.ref());
        }
    }

    /**
     * Release the background retirement scheduler (issue #2452 item 2). Called from the connector's
     * shutdown BEFORE {@link #retireAll} (roborev job 1755 finding 3): {@link #retireAll} runs its
     * own retirement synchronously on the CALLER's thread (it never goes through the scheduler), so
     * closing the scheduler first ensures no background sweep can still be running concurrently with
     * (or after) {@link #retireAll}'s drain — no argued-safe race to reason about, just none possible.
     * Idempotent and a no-op for the inline scheduler.
     */
    public void close() {
        retireScheduler.close();
    }

    /**
     * Resolve the current reused window for {@code ref}, reusing an existing fresh window or
     * creating a new one (bumping the epoch) atomically. Counting is done by the caller AFTER the
     * per-host fan-out succeeds (issue #2356 roborev), not here.
     *
     * <p><b>A superseded window is NOT retired on supersede (issue #2356 roborev, retire-race)</b> —
     * a long-running query A can still be reading a window's snapshot (splits not yet opened, or a
     * cold remote host) when a later query B rolls to a fresh window; deleting A's snapshot dir on
     * supersede would break A's in-flight reads. Instead the superseded window is ENQUEUED with the
     * supersede time and retired later by {@link #sweepRetireDue} once the grace period elapses
     * (bounded retention, see the class javadoc). Each resolve also sweeps any now-due pending
     * window. Immediate active retirement ({@link #retire}) still happens on explicit
     * {@link #invalidate} (the {@code Database::refresh()} analog) and {@link #retireAll} (shutdown).
     */
    private Resolved resolveWindow(TableRef ref, long generationFingerprint) {
        long now = clock.nanoTime();
        boolean[] reused = {false};
        Window[] superseded = {null};
        Window window = windows.compute(ref, (k, existing) -> {
            if (existing != null && isFresh(existing, generationFingerprint, now)) {
                // Latch this query's hold under the bin lock (issue #2452 item 1), atomic with
                // the reuse decision so a concurrent creator's rollback sees this reuser as a holder.
                existing.activeHolds.incrementAndGet();
                reused[0] = true;
                return existing;
            }
            if (existing != null) {
                superseded[0] = existing;
            }
            long epoch = epochs.computeIfAbsent(k, r -> new AtomicLong()).getAndIncrement();
            Window fresh = new Window(nameFor(k, epoch), epoch, now, generationFingerprint);
            fresh.activeHolds.incrementAndGet();
            return fresh;
        });
        if (superseded[0] != null) {
            pendingRetire.add(new PendingRetire(ref, superseded[0], now));
        }
        // Offload the due-retirement sweep off the split-planning path (issue #2452 item 2, roborev
        // job 1722): a hot table must not pay a synchronous multi-host DELETE fan-out in planning
        // latency. The inline scheduler still runs it now (deterministic tests); the background
        // scheduler hands it to a bounded best-effort executor. Deliberate semantic shift (roborev
        // job 1754 finding 5): the lambda re-reads `clock.nanoTime()` when the sweep actually EXECUTES
        // — NOT the `now` captured above for THIS resolve's supersede decision — since a background
        // sweep can run an arbitrary (possibly long) delay later and must judge grace due-ness
        // against the CURRENT clock, not a stale reading from enqueue time.
        retireScheduler.submitSweep(() -> sweepRetireDue(clock.nanoTime()));
        return new Resolved(window, reused[0]);
    }

    /**
     * Retire every pending superseded window whose grace period has elapsed at {@code now}. The
     * queue is ordered oldest-first (non-decreasing clock + supersede-order enqueue), so the first
     * not-yet-due window ends the sweep. Concurrent sweeps race on {@link Queue#remove}; only the
     * winner of the atomic remove performs the retire, so each window is retired at most once.
     */
    private void sweepRetireDue(long now) {
        while (true) {
            PendingRetire head = pendingRetire.peek();
            if (head == null || now - head.supersededNanos() < retireGraceNanos) {
                return;
            }
            if (pendingRetire.remove(head)) {
                retire(head.window(), head.ref());
            }
        }
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
     * Best-effort delete of every snapshot {@code window} created, on each host it was created on.
     * Called on explicit {@link #invalidate}, on {@link #retireAll} (shutdown), and on
     * {@link #sweepRetireDue} once a superseded window's grace period elapses (issue #2356 roborev)
     * — never the instant a window is superseded. Individual delete failures are logged and
     * swallowed — the Sidecar TTL backstop covers a miss.
     */
    private void retire(Window window, TableRef ref) {
        window.retired.set(true); // tracked fact, not a gate — see the Window#retired javadoc.
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

    /**
     * Test-support hook (roborev job 1754 finding 1): directly invoke the best-effort retire logic
     * for an already-resolved {@link Window}, so a test can pin that retiring the SAME window TWICE
     * — the rollback-races-a-sweep / hits-an-already-cleared-snapshot hazard roborev raised — never
     * throws and converges on the same logical outcome. Production code never needs this: {@link
     * #invalidate}, {@link #retireAll}, and the grace-sweep are the only retire entry points, and
     * (per the hazard-downgrade note on the rollback catch block above) the {@code windows} map's
     * identity-based conditional removal already makes a genuine double-retire of the SAME
     * {@code Window} instance from two DIFFERENT production code paths structurally impossible.
     */
    void retireForTest(Window window, String keyspace, String table) {
        retire(window, new TableRef(keyspace, table));
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
