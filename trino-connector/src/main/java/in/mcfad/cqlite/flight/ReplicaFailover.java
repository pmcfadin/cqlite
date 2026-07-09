package in.mcfad.cqlite.flight;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

/**
 * Replica-failover policy for split reads (issue #2241): which stream-open failures are safe to
 * retry against the next replica in the split's ordered list.
 */
final class ReplicaFailover {
    private ReplicaFailover() {}

    /**
     * True iff {@code error} (or a cause) is a connection-establishment /
     * {@link FlightStatusCode#UNAVAILABLE}-class Flight failure — the endpoint is down or
     * unreachable, so another replica that owns the range can serve the read. The status can be
     * wrapped (the client rethrows runtime exceptions and wraps checked ones), so we walk the
     * cause chain, mirroring {@code CqliteFlightMetadata#isNotHosting}.
     *
     * <p>This must be consulted ONLY before the split has delivered any batch: retrying after rows
     * were consumed could duplicate them (a correctness bug), so a mid-stream failure — even an
     * UNAVAILABLE one — is fatal at the call site regardless of this predicate.
     */
    static boolean isConnectClass(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof FlightRuntimeException fre
                    && fre.status() != null
                    && fre.status().code() == FlightStatusCode.UNAVAILABLE) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
