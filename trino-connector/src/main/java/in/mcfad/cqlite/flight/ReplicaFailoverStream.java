package in.mcfad.cqlite.flight;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Shared availability-failover retry loop (issue #2241) for a SINGLE range's batch stream: tries
 * an ordered replica host list (primary first, then fallbacks) — as returned by {@link
 * CqliteFlightSplit#replicaHosts()} — and fails over to the next host on a
 * connection-establishment/UNAVAILABLE-class failure ({@link ReplicaFailover#isConnectClass})
 * BEFORE any batch has been delivered FROM THIS STREAM. Once a batch has been delivered the
 * stream is committed: a later failure is fatal and never retried, because re-reading from
 * another replica could duplicate already-emitted rows. If every host is unreachable the caller
 * sees the last connect failure propagate — loud, never a silent partial/empty result.
 *
 * <p>Used by both the per-split scan page source ({@link CqliteFlightPageSource}) and the
 * aggregate finalize page source's per-range fan-out ({@link CqliteFlightAggregatePageSource}) —
 * a single shared implementation so scan and aggregate paths get identical failover semantics.
 */
final class ReplicaFailoverStream implements AutoCloseable {

    /** One replica's open batch stream — abstracted so failover is unit-testable off-cluster. */
    interface BatchStream extends AutoCloseable {
        /**
         * Advance to the next batch; {@code false} at end of stream. The FIRST call performs the
         * actual gRPC and may throw a connect-class failure that triggers failover.
         */
        boolean next();

        VectorSchemaRoot getRoot();

        @Override
        void close();

        /**
         * Explicitly CANCEL this stream on an EARLY close — before it is fully drained (issue
         * #2782). Trino cancels the operator once a pushed {@code LIMIT} is satisfied and closes
         * the page source while server batches are still queued; merely releasing the handle then
         * left the underlying {@code DoGet} gRPC waiting on unconsumed batches and the query hung.
         * Cancellation MUST be idempotent and NON-BLOCKING (it signals the server to stop and
         * releases the client resources — it never waits to consume the remaining batches). The
         * default releases the stream via {@link #close()}; the production {@code DoGet} adapter
         * overrides it to first send the Flight-level cancel signal.
         */
        default void cancel() {
            close();
        }
    }

    /** Opens a {@link BatchStream} against one replica {@code host:port} for a ticket. */
    @FunctionalInterface
    interface StreamOpener {
        BatchStream open(String host, int port, byte[] ticket);
    }

    private final List<String> hosts;
    private final int port;
    private final byte[] ticket;
    private final StreamOpener opener;

    // The active underlying stream and the early-close flag are read/written from TWO threads
    // (issue #2680 / P0 #2782): the driver thread advances in {@link #next()} while Trino's
    // cancellation thread calls {@link #close()} the instant a pushed LIMIT is satisfied on
    // ANOTHER surplus split. Both are volatile so a close is visible to a concurrent open, and
    // {@code next()} double-checks {@code closed} AFTER publishing a freshly-opened stream so a
    // close that raced just ahead of the open still cancels it — otherwise that surplus split's
    // DoGet gRPC blocks on undrained batches forever (the intermittent K>1 hang).
    private volatile BatchStream stream;
    private volatile boolean closed;
    private int hostIndex;
    private boolean started;

    ReplicaFailoverStream(List<String> hosts, int port, byte[] ticket, StreamOpener opener) {
        this.hosts = List.copyOf(hosts);
        this.port = port;
        this.ticket = ticket;
        this.opener = opener;
    }

    /**
     * Advance to the next batch, transparently failing over to the next replica host on a
     * connect-class failure before any batch of THIS stream has been delivered.
     *
     * @return {@code false} at end of stream
     * @throws RuntimeException the underlying failure once no replica is left to try, or ANY
     *                          failure (connect-class or not) once this stream is committed
     */
    boolean next() {
        while (true) {
            try {
                // An early close raced ahead of this advance (Trino cancelled a surplus split
                // once the LIMIT was satisfied): stop WITHOUT opening a new stream, so a fresh
                // DoGet is never started after close() already ran and cleared its reference.
                if (closed) {
                    return false;
                }
                BatchStream active = stream;
                if (active == null) {
                    active = opener.open(hosts.get(hostIndex), port, ticket);
                    stream = active;
                    // close() may have run between the null-check and publishing `active` above;
                    // it would have found stream == null and cancelled nothing. Re-check under
                    // the volatile flag and cancel THIS just-opened stream ourselves so its DoGet
                    // gRPC cannot block on undrained batches (the intermittent K>1 #2782 hang).
                    if (closed) {
                        cancelActiveQuietly(active);
                        stream = null;
                        return false;
                    }
                }
                boolean hasNext = active.next();
                if (hasNext) {
                    started = true;
                }
                return hasNext;
            } catch (RuntimeException e) {
                closeStreamQuietly();
                if (!started && hostIndex + 1 < hosts.size() && ReplicaFailover.isConnectClass(e)) {
                    hostIndex++;
                    continue; // fail over to the next replica that owns this range (#2241)
                }
                throw e; // committed, no replica left, or a non-connect error: fail loudly
            }
        }
    }

    VectorSchemaRoot getRoot() {
        // Single volatile read: close() (Trino's cancel thread) may null `stream` between next()
        // returning true and this read on the driver thread. Snapshot it and, if the stream was
        // just cancelled, return null (no current root) rather than NPE — the batch is discarded on
        // the cancel path anyway. Non-blocking and idempotent, mirroring how next() finishes when
        // closed (issue #2680).
        BatchStream active = stream;
        if (active == null) {
            return null;
        }
        return active.getRoot();
    }

    /**
     * Release this stream, CANCELLING the currently-active underlying {@code DoGet} stream
     * (issue #2782). Trino calls this when it closes the page source — including EARLY, once a
     * pushed {@code LIMIT} is satisfied and unconsumed server batches remain — so a plain
     * release left the gRPC call blocked on undrained batches and the query hung. Cancel
     * propagates the Flight-level stop signal to the ACTIVE underlying stream (not just the
     * wrapper), is NON-BLOCKING (it never drains remaining batches), and is IDEMPOTENT: once
     * released the reference is cleared, so a second {@code close()} is a harmless no-op.
     */
    @Override
    public void close() {
        // Set the flag BEFORE touching the stream: a concurrent next() that has not yet opened
        // its stream then sees `closed` and refuses to open one (or cancels the one it just
        // published), so no surplus DoGet outlives this close (issue #2680 / P0 #2782).
        closed = true;
        BatchStream active = stream;
        if (active != null) {
            stream = null;
            cancelActiveQuietly(active);
        }
    }

    /** Best-effort cancel of one underlying stream; never lets close/next throw. */
    private static void cancelActiveQuietly(BatchStream active) {
        try {
            active.cancel();
        } catch (RuntimeException ignore) {
            // best-effort cancel + release; never let close throw
        }
    }

    private void closeStreamQuietly() {
        BatchStream active = stream;
        if (active != null) {
            stream = null;
            try {
                active.close();
            } catch (RuntimeException ignore) {
                // best-effort release
            }
        }
    }

    /** The production opener: a real DoGet stream wrapped so failover sees a uniform interface. */
    static StreamOpener adapt(CqliteFlightClient client) {
        return (host, port, ticket) -> {
            CqliteFlightClient.StreamHandle handle = client.openStream(host, port, ticket);
            return new BatchStream() {
                @Override
                public boolean next() {
                    return handle.stream().next();
                }

                @Override
                public VectorSchemaRoot getRoot() {
                    return handle.stream().getRoot();
                }

                @Override
                public void close() {
                    handle.close();
                }

                @Override
                public void cancel() {
                    // Early close (satisfied LIMIT, #2782): signal the server-side DoGet to stop
                    // and release the client, WITHOUT draining the remaining queued batches.
                    handle.cancel();
                }
            };
        };
    }
}
