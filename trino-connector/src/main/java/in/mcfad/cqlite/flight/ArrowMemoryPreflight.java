package in.mcfad.cqlite.flight;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;

/**
 * Fail-fast preflight for arrow-java's off-heap memory initialization (issues #2193, #2290).
 *
 * <p>On JDK 17+ arrow-java's {@code org.apache.arrow.memory.util.MemoryUtil} static initializer
 * needs the {@code java.nio} module opened to it via the JVM flag
 * {@value #REQUIRED_ADD_OPENS}. When the flag is absent, {@code MemoryUtil.<clinit>} throws an
 * {@link ExceptionInInitializerError} on the FIRST Arrow off-heap touch (here, constructing a
 * {@link RootAllocator}); every later touch throws {@link NoClassDefFoundError} because the class
 * failed to initialize. On the Flight read path this surfaces far downstream as a cryptic
 * {@code Failed to read message} on the first {@code do_get} frame, with nothing pointing at the
 * real cause.
 *
 * <p>Trino 481's stock {@code jvm.config} does not carry the flag, and deployment kits that do not
 * inject it (e.g. the k8s {@code trino-cqlite} kit before #2290) leave every query broken. This
 * preflight runs once, at connector construction (before any split/query), probes the exact
 * initialization arrow-java would perform, and on failure raises a {@link
 * StandardErrorCode#CONFIGURATION_INVALID} error naming the exact missing flag and the
 * {@code jvm.config} remedy — so the operator sees an actionable message at catalog load instead of
 * a masked read-time failure.
 *
 * <p>The probe is side-effect-free when the flag IS present: it opens a {@link RootAllocator} and
 * allocates a 1-byte off-heap {@link ArrowBuf} solely to force {@code MemoryUtil} initialization
 * along the real off-heap/DirectByteBuffer path, then closes both immediately (try-with-resources).
 */
final class ArrowMemoryPreflight {
    /** The exact arrow-java-documented flag that must be present in Trino's {@code jvm.config}. */
    static final String REQUIRED_ADD_OPENS =
            "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED";

    private ArrowMemoryPreflight() {}

    /**
     * Probe arrow-java off-heap memory initialization. Returns normally (and leaves no allocator
     * open) when the required JVM flag is present; otherwise throws a {@link TrinoException} whose
     * message names the exact missing flag and the {@code jvm.config} remedy.
     */
    static void verify() {
        try (RootAllocator probe = new RootAllocator();
                ArrowBuf ignored = probe.buffer(1)) {
            // Constructing the allocator AND allocating a 1-byte off-heap buffer forces
            // MemoryUtil.<clinit> along the real DirectByteBuffer/off-heap path (a bare RootAllocator
            // may not). Success => the flag is present; both are closed immediately by
            // try-with-resources, so the probe has no lasting effect.
            assert ignored != null;
        } catch (LinkageError | RuntimeException e) {
            throwIfArrowMemoryInit(e);
        }
    }

    /**
     * If {@code failure} is (or is caused by) an arrow-java {@code MemoryUtil} initialization
     * failure, throw an actionable {@link TrinoException}; otherwise rethrow {@code failure}
     * unchanged so unrelated errors are never masked. Package-private for direct unit testing of
     * both the arrow-init path and the pass-through path without manipulating JVM flags.
     */
    static void throwIfArrowMemoryInit(Throwable failure) {
        if (isArrowMemoryInitFailure(failure)) {
            throw new TrinoException(StandardErrorCode.CONFIGURATION_INVALID, formatMessage(failure), failure);
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(failure);
    }

    /**
     * True when {@code failure} (or any cause in its chain) points at arrow-java's {@code MemoryUtil}
     * static initializer — matched by message text or by a {@code MemoryUtil} frame in a stack trace.
     */
    static boolean isArrowMemoryInitFailure(Throwable failure) {
        // Bounded walk: cap the depth so a self-referential (A->A) OR multi-node (A->B->A) cause
        // cycle can never make this loop forever.
        int depth = 0;
        for (Throwable cause = failure; cause != null && depth < MAX_CAUSE_CHAIN_DEPTH;
                cause = cause.getCause(), depth++) {
            String message = cause.getMessage();
            if (message != null && message.contains("MemoryUtil")) {
                return true;
            }
            for (StackTraceElement frame : cause.getStackTrace()) {
                if (frame.getClassName().contains("MemoryUtil")) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Hard cap on how many links of a Throwable cause chain to walk (guards against cause cycles). */
    private static final int MAX_CAUSE_CHAIN_DEPTH = 50;

    /**
     * Build the operator-facing remedy message. Always names {@value #REQUIRED_ADD_OPENS} verbatim so
     * an operator can copy it straight into {@code jvm.config}.
     */
    static String formatMessage(Throwable failure) {
        String detail = "unknown cause";
        if (failure != null) {
            detail = failure.getClass().getSimpleName();
            if (failure.getMessage() != null) {
                detail = detail + ": " + failure.getMessage();
            }
        }
        return "CQLite Flight connector cannot initialize Apache Arrow off-heap memory (" + detail + "). "
                + "arrow-java requires the java.nio module to be opened to it on JDK 17+. "
                + "Add this exact line to Trino's jvm.config and restart every coordinator and worker:\n    "
                + REQUIRED_ADD_OPENS + "\n"
                + "See the connector README section \"Required JVM configuration\" for details.";
    }
}
