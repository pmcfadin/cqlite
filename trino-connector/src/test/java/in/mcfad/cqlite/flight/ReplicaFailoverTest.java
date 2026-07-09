package in.mcfad.cqlite.flight;

import org.apache.arrow.flight.CallStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The failover-eligibility predicate (issue #2241): only connection-establishment/UNAVAILABLE
 * failures are safe to retry on another replica; every other status (and non-Flight errors) is
 * fatal at the call site.
 */
class ReplicaFailoverTest {

    @Test
    void unavailableIsConnectClass() {
        assertTrue(ReplicaFailover.isConnectClass(
                CallStatus.UNAVAILABLE.withDescription("connection refused").toRuntimeException()));
    }

    @Test
    void notFoundIsNotConnectClass() {
        // A responding endpoint that lacks the snapshot/table is NOT an availability failure —
        // retrying elsewhere would mask a real error.
        assertFalse(ReplicaFailover.isConnectClass(
                CallStatus.NOT_FOUND.withDescription("no such table").toRuntimeException()));
    }

    @Test
    void internalIsNotConnectClass() {
        assertFalse(ReplicaFailover.isConnectClass(
                CallStatus.INTERNAL.withDescription("server bug").toRuntimeException()));
    }

    @Test
    void plainRuntimeExceptionIsNotConnectClass() {
        assertFalse(ReplicaFailover.isConnectClass(new IllegalStateException("boom")));
    }

    @Test
    void wrappedUnavailableCauseIsConnectClass() {
        // The client rethrows runtime exceptions and wraps checked ones; the status can be nested.
        RuntimeException wrapped = new IllegalStateException(
                "openStream failed", CallStatus.UNAVAILABLE.toRuntimeException());
        assertTrue(ReplicaFailover.isConnectClass(wrapped));
    }
}
