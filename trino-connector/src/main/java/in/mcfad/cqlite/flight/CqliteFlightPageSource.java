package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streams one split's Arrow Flight batches, converting each to a Trino page.
 *
 * <p>Replica failover (issue #2241) is handled by the shared {@link ReplicaFailoverStream}: the
 * split's ordered {@link CqliteFlightSplit#replicaHosts()} (primary first) are tried in order,
 * failing over to the next replica on a connect-class failure before the first batch, and
 * failing loudly once committed or when every replica is unreachable — CQLite never returns a
 * silent partial/empty result.
 *
 * <h2>Non-blocking read — the P0 #2782 root fix (issue #2680)</h2>
 *
 * <p>The underlying Arrow {@link org.apache.arrow.flight.FlightStream#next()} BLOCKS the calling
 * thread on a {@code LinkedBlockingQueue.take()} until the server delivers the next batch. The
 * pre-fix page source ran that read DIRECTLY on the Trino driver thread inside
 * {@code getNextSourcePage()}. When Trino satisfies a pushed {@code LIMIT} it cancels the remaining
 * (surplus) splits' drivers from ANOTHER thread ({@code Driver.close()}), but that close does a
 * {@code tryLock} that FAILS while the driver thread is still pinned inside a deadline-less
 * {@code next()} — so the close (and therefore {@link ReplicaFailoverStream#close()} →
 * {@code FlightStream.cancel()}) was DEFERRED forever and the query hung for the full 180s harness
 * timeout (the #2782 P0 that reverted PR #2779). The prior close→cancel wiring was correct but could
 * never RUN because the driver thread never returned.
 *
 * <p>The fix runs the blocking {@code next()}/{@code getRoot()}/page-conversion on a dedicated
 * single-thread background executor and implements {@link #isBlocked()} so the DRIVER THREAD is
 * never pinned: {@code getNextSourcePage()} returns {@code null} while a fetch is in flight and the
 * driver yields on the {@link #isBlocked()} future. Trino can then run {@link #close()} on the freed
 * driver thread the instant the LIMIT is satisfied; {@code close()} cancels the active Flight stream
 * (which unblocks the background {@code next()} via the queue-sentinel the gRPC cancel enqueues) and
 * shuts the executor down — non-blocking, idempotent, and it never drains the remaining batches.
 * Cross-thread cancellation of an in-flight {@code next()} is exactly what Arrow's
 * {@code FlightStream.cancel()} is designed for. This holds at ANY split count (K=1 included).
 */
public class CqliteFlightPageSource implements ConnectorPageSource {
    private final List<CqliteFlightColumnHandle> columns;
    private final ReplicaFailoverStream stream;
    /** Runs the blocking Flight read OFF the Trino driver thread so a close can cancel it. */
    private final ExecutorService fetchExecutor;
    private final AtomicLong completedPositions = new AtomicLong();

    /**
     * The in-flight background fetch, or {@code null} when none is scheduled (before the first
     * poll, or after end-of-stream / close). Only ever touched by the driver thread in
     * {@code getNextSourcePage()}/{@code isBlocked()} (Trino serializes those under the driver
     * lock); {@code volatile} so a concurrent {@code close()} sees the latest reference.
     */
    private volatile CompletableFuture<FetchResult> pending;
    private volatile boolean finished;

    public CqliteFlightPageSource(
            CqliteFlightClient client,
            CqliteFlightSplit split,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket) {
        this(split.replicaHosts(), split.port(), columns, ticket, ReplicaFailoverStream.adapt(client));
    }

    /** Package-private seam: inject the ordered host list + opener directly for unit tests. */
    CqliteFlightPageSource(
            List<String> hosts,
            int port,
            List<CqliteFlightColumnHandle> columns,
            byte[] ticket,
            ReplicaFailoverStream.StreamOpener opener) {
        this.columns = columns;
        this.stream = new ReplicaFailoverStream(hosts, port, ticket, opener);
        this.fetchExecutor = Executors.newSingleThreadExecutor(daemonFactory());
    }

    /** A single daemon thread so a lingering fetch never blocks JVM shutdown. */
    private static ThreadFactory daemonFactory() {
        return runnable -> {
            Thread t = new Thread(runnable, "cqlite-flight-fetch");
            t.setDaemon(true);
            return t;
        };
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        CompletableFuture<FetchResult> current = pending;
        if (current == null) {
            // First poll (or the poll after a delivered page consumed its fetch): schedule the
            // background read and report blocked — the driver thread is NOT pinned in next().
            pending = scheduleFetch();
            return null;
        }
        if (!current.isDone()) {
            // A fetch is still running on the background thread; yield the driver thread. Trino
            // parks on the isBlocked() future rather than spinning.
            return null;
        }
        // The background fetch completed.
        FetchResult result;
        try {
            result = current.join();
        } catch (CompletionException e) {
            // A read/failover error surfaced on the background thread — release everything and
            // propagate loudly (Trino does not guarantee close() on the throw path).
            pending = null;
            close();
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw e;
        }
        pending = null;
        if (result.end()) {
            finished = true;
            return null;
        }
        Page page = result.page();
        completedPositions.addAndGet(page.getPositionCount());
        // Pipeline the NEXT read now so the following poll can observe it via isBlocked().
        pending = scheduleFetch();
        return SourcePage.create(page);
    }

    /**
     * Advance one batch on the background executor and convert it to a Trino page. The
     * {@link VectorSchemaRoot} is only valid until the next {@code next()} call, so the conversion
     * happens HERE (same task, before any subsequent advance) — never on the driver thread.
     */
    private CompletableFuture<FetchResult> scheduleFetch() {
        try {
            return CompletableFuture.supplyAsync(
                    () -> {
                        if (!stream.next()) {
                            return FetchResult.END;
                        }
                        VectorSchemaRoot root = stream.getRoot();
                        if (root == null) {
                            // close() (Trino's cancel thread) nulled the active stream between next()
                            // returning true and this getRoot() — the just-cancelled batch is
                            // discarded. Treat as end-of-stream (issues #2782/#2680).
                            return FetchResult.END;
                        }
                        return FetchResult.of(ArrowToTrino.toPage(root, columns));
                    },
                    fetchExecutor);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // close() (Trino's cancel thread) shut the executor down between this poll passing the
            // `finished` check and scheduling here. That IS an early close — yield end-of-stream
            // rather than surfacing the rejection as a query error (issue #2680).
            return CompletableFuture.completedFuture(FetchResult.END);
        }
    }

    @Override
    public CompletableFuture<?> isBlocked() {
        CompletableFuture<FetchResult> current = pending;
        if (current == null || current.isDone()) {
            return NOT_BLOCKED;
        }
        // Complete (NORMALLY, via handle) exactly when the background fetch does — success OR
        // failure — so Trino unparks the driver and re-polls getNextSourcePage(), which is where a
        // fetch error surfaces (never from this signal). handle() also yields a fresh stage, so a
        // Trino-side cancellation of the returned future cannot abort our in-flight fetch.
        return current.handle((result, error) -> null);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() {
        finished = true;
        // Cancel the active Flight DoGet stream (issue #2782): this unblocks a background next()
        // parked in FlightStream.take() by enqueuing the gRPC-cancel sentinel, so the fetch thread
        // returns promptly instead of waiting on undrained server batches. Idempotent + non-blocking.
        stream.close();
        // Stop accepting/keep running background fetches; interrupt a parked read as a backstop
        // (LinkedBlockingQueue.take() is interruptible). Non-blocking — we never join the executor.
        fetchExecutor.shutdownNow();
    }

    /** The outcome of one background fetch: either a converted page or end-of-stream. */
    private static final class FetchResult {
        private static final FetchResult END = new FetchResult(null);

        private final Page page;

        private FetchResult(Page page) {
            this.page = page;
        }

        static FetchResult of(Page page) {
            return new FetchResult(page);
        }

        boolean end() {
            return page == null;
        }

        Page page() {
            return page;
        }
    }
}
