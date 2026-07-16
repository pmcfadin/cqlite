package in.mcfad.cqlite.flight;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Where superseded-snapshot retirement sweeps run (issue #2452 item 2, roborev job 1722).
 *
 * <p>Historically {@link SnapshotManager} swept due retirements INLINE from
 * {@code resolveSnapshot} on the split-planning path, so a hot table with a short reuse window paid
 * a synchronous multi-host {@code clearSnapshot} DELETE fan-out on most planning cycles — planning
 * latency (roborev job 1722 Low #1). Retirement is best-effort (the ~6h Sidecar TTL is the
 * backstop), so it is offloaded off the planning path.
 *
 * <p>Two triggers feed the sweep:
 * <ul>
 *   <li>{@link #submitSweep} — a one-off best-effort sweep enqueued when a query supersedes a
 *       window; it runs on the background executor rather than the caller's planning thread.</li>
 *   <li>{@link #startPeriodic} — a periodic tick so a QUIET table (no further queries) still prunes
 *       its superseded backlog rather than accumulating snapshot dirs until the TTL reclaims them
 *       (the #2367 714-snapshot field accumulation).</li>
 * </ul>
 *
 * <p>{@link InlineRetireScheduler} preserves the original synchronous behavior (used by the
 * deterministic unit tests and any caller that does not want a background thread);
 * {@link BackgroundRetireScheduler} is the production offload.
 */
public interface SnapshotRetireScheduler {

    /** Run a best-effort retirement sweep. Inline impl runs it now; background impl offloads it. */
    void submitSweep(Runnable sweep);

    /**
     * Register the periodic quiet-table sweep. The background impl schedules it on a fixed cadence;
     * the inline impl ignores it (a purely inline scheduler has no thread to tick on, so quiet-table
     * pruning is a background-only guarantee — the TTL still backstops the inline path).
     */
    default void startPeriodic(Runnable sweep) {}

    /** Release any background resources (production executor). Idempotent; inline impl is a no-op. */
    default void close() {}

    /** Synchronous, thread-free scheduler: runs each submitted sweep immediately in-line. */
    final class InlineRetireScheduler implements SnapshotRetireScheduler {
        @Override
        public void submitSweep(Runnable sweep) {
            sweep.run();
        }
    }

    /**
     * Production scheduler: a single daemon thread runs both the one-off submitted sweeps and a
     * periodic tick. Submitted sweeps are COALESCED — at most one is queued at a time (the sweep is
     * idempotent and the periodic tick backstops any dropped one) — so a burst of planning cycles
     * cannot pile unbounded retire tasks onto the executor.
     */
    final class BackgroundRetireScheduler implements SnapshotRetireScheduler {
        private static final Logger LOG = Logger.getLogger(BackgroundRetireScheduler.class.getName());

        /** Floor for the periodic cadence (issue #2452 roborev nit): guards against a near-zero or
         * negative caller-supplied period turning the periodic tick into a busy-loop. */
        private static final long MIN_PERIOD_MILLIS = 1_000L;

        private final ScheduledExecutorService exec;
        private final long periodMillis;
        /**
         * Coalesce submitted sweeps: at most one queued at a time. Package-private (not private) so
         * {@code SnapshotManagerRetireHardeningTest} can observe that a rejected-on-close submission
         * still resets the flag (issue #2452 item 3) rather than permanently wedging all future
         * coalesced sweeps — a stuck-flag regression would silently disable grace-sweeps exactly like
         * the #2367 bug this issue fixes.
         */
        final AtomicBoolean sweepQueued = new AtomicBoolean(false);

        BackgroundRetireScheduler(long periodMillis) {
            this.periodMillis = Math.max(MIN_PERIOD_MILLIS, periodMillis);
            ThreadFactory daemon = r -> {
                Thread t = new Thread(r, "cqlite-snapshot-retire");
                t.setDaemon(true);
                return t;
            };
            this.exec = Executors.newSingleThreadScheduledExecutor(daemon);
        }

        @Override
        public void submitSweep(Runnable sweep) {
            // Only enqueue if none is already pending (coalesce). The task clears the flag when it
            // starts, so the next supersede after it begins can enqueue a fresh sweep.
            if (!sweepQueued.compareAndSet(false, true)) {
                return;
            }
            try {
                exec.execute(() -> {
                    sweepQueued.set(false);
                    runSafely(sweep);
                });
            } catch (RejectedExecutionException e) {
                // Executor already shut down (connector shutdown raced a late supersede): the sweep
                // is best-effort and the TTL backstops it, so drop it. Reset the flag for tidiness.
                sweepQueued.set(false);
            }
        }

        @Override
        public void startPeriodic(Runnable sweep) {
            try {
                exec.scheduleWithFixedDelay(
                        () -> runSafely(sweep), periodMillis, periodMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // Already closed — nothing to schedule.
            }
        }

        @Override
        public void close() {
            exec.shutdownNow();
        }

        private static void runSafely(Runnable sweep) {
            try {
                sweep.run();
            } catch (Throwable t) {
                // A best-effort background sweep must NEVER let an uncaught throwable escape (roborev
                // job 1753 fix #2): scheduleWithFixedDelay PERMANENTLY cancels all future executions
                // of a periodic task the moment its Runnable throws ANYTHING uncaught — catching only
                // RuntimeException left a single Error free to silently kill quiet-table pruning for
                // the rest of the JVM's life, a silent regression back to the exact #2367
                // 714-snapshot accumulation bug this issue fixes. The TTL backstop reclaims anything
                // a failed sweep left behind, so swallowing (after logging) is safe here.
                LOG.log(Level.WARNING, t, () -> "Background snapshot retire sweep failed (TTL backstop reclaims)");
            }
        }
    }
}
