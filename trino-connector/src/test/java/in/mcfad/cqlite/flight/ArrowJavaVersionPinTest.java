package in.mcfad.cqlite.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.security.CodeSource;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Version-drift guard for issue #2193: assert the arrow-java stack ACTUALLY on
 * the runtime classpath is 19.0.0, and that netty is pinned to the 4.1.x line
 * arrow-19's allocator requires.
 *
 * <p>The round-5 field failure was a SILENT version problem: the connector
 * bundled arrow-java 18.1.0, whose Flight decoder throws {@code Failed to read
 * message} on the field's schema shape, while every offline test passed. Bumping
 * {@code arrowVersion} in {@code build.gradle.kts} is necessary but not
 * sufficient — a future transitive dependency (or a careless edit) could let
 * Gradle conflict-resolution resolve a DIFFERENT arrow-java or drag netty back up
 * to 4.2.x (which breaks arrow's {@code NettyAllocationManager} static init on
 * the first {@code RootAllocator}; see {@code build.gradle.kts}). This test reads
 * the version off the RESOLVED jar filenames (via each class's {@link CodeSource})
 * — the exact artifacts the field loads — so any silent downgrade/upgrade fails
 * here loudly rather than in the field.
 *
 * <p>Keep {@link #EXPECTED_ARROW_VERSION} / {@link #EXPECTED_NETTY_VERSION} in
 * lockstep with {@code arrowVersion} / {@code nettyVersion} in
 * {@code build.gradle.kts}.
 */
class ArrowJavaVersionPinTest {

    /** Must match {@code arrowVersion} in build.gradle.kts. */
    private static final String EXPECTED_ARROW_VERSION = "19.0.0";

    /** Must match {@code nettyVersion} in build.gradle.kts. */
    private static final String EXPECTED_NETTY_VERSION = "4.1.130.Final";

    @Test
    void flightCoreJarIsExpectedArrowVersion() {
        assertEquals(
                EXPECTED_ARROW_VERSION,
                versionFromJar(FlightClient.class, "flight-core"),
                "resolved flight-core arrow-java version drifted from the #2193 pin");
    }

    @Test
    void arrowVectorJarIsExpectedArrowVersion() {
        assertEquals(
                EXPECTED_ARROW_VERSION,
                versionFromJar(VectorSchemaRoot.class, "arrow-vector"),
                "resolved arrow-vector version drifted from flight-core — a split arrow-java stack");
    }

    @Test
    void nettyBufferJarIsPinnedToArrow19Baseline() {
        // arrow-19's netty allocator breaks on netty 4.2.x (issue #2193); this
        // guards the enforced netty-bom that holds the stack at 4.1.x.
        Class<?> nettyBuffer;
        try {
            nettyBuffer = Class.forName("io.netty.buffer.ByteBufAllocator");
        } catch (ClassNotFoundException e) {
            throw new AssertionError("netty-buffer is not on the runtime classpath", e);
        }
        assertEquals(
                EXPECTED_NETTY_VERSION,
                versionFromJar(nettyBuffer, "netty-buffer"),
                "resolved netty-buffer drifted off arrow-19's tested 4.1.x baseline");
    }

    /**
     * Extract the {@code <version>} token from the {@code <artifact>-<version>.jar}
     * filename backing {@code type} on the classpath. The jar filename literally
     * encodes the resolved Maven version, so this reflects exactly what the field
     * loads — independent of any (often absent) Manifest {@code Implementation-Version}.
     */
    private static String versionFromJar(Class<?> type, String artifact) {
        CodeSource src = type.getProtectionDomain().getCodeSource();
        assertNotNull(src, "no CodeSource for " + type.getName() + " (cannot locate its jar)");
        URL location = src.getLocation();
        assertNotNull(location, "no jar location for " + type.getName());
        String path = location.getPath();
        String file = path.substring(path.lastIndexOf('/') + 1);
        assertTrue(
                file.startsWith(artifact + "-") && file.endsWith(".jar"),
                type.getName() + " is not loaded from a " + artifact + "-*.jar (was " + file + ")");
        Matcher m = Pattern.compile("^" + Pattern.quote(artifact) + "-(.+)\\.jar$").matcher(file);
        assertTrue(m.matches(), "unexpected " + artifact + " jar filename: " + file);
        return m.group(1);
    }
}
