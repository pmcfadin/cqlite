package in.mcfad.cqlite.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.RootAllocator;
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
    void arrowMemoryCoreJarIsExpectedArrowVersion() {
        // RootAllocator lives in arrow-memory-core — the SAME module whose
        // ImmutableConfig -> BaseAllocator startup path was the site of the
        // 18->19 regression this test guards against.
        assertEquals(
                EXPECTED_ARROW_VERSION,
                versionFromJar(RootAllocator.class, "arrow-memory-core"),
                "resolved arrow-memory-core version drifted from flight-core — a split arrow-java stack");
    }

    @Test
    void arrowMemoryNettyJarIsExpectedArrowVersion() {
        // NettyAllocationManager is the exact class whose static <clinit> threw
        // ExceptionInInitializerError under netty 4.2.x (issue #2193) — the
        // allocator module a version/netty-baseline mismatch would reproduce the
        // field failure in, even with flight-core/arrow-vector/netty-buffer green.
        // It is loaded reflectively by DefaultAllocationManagerFactory and so is a
        // runtime-only (not compile-time) dependency — resolved via Class.forName,
        // same as the netty-buffer and buffer-patch checks below.
        Class<?> nettyAllocationManager;
        try {
            nettyAllocationManager = Class.forName("org.apache.arrow.memory.netty.NettyAllocationManager");
        } catch (ClassNotFoundException e) {
            throw new AssertionError("arrow-memory-netty's NettyAllocationManager is not on the runtime classpath", e);
        }
        assertEquals(
                EXPECTED_ARROW_VERSION,
                versionFromJar(nettyAllocationManager, "arrow-memory-netty"),
                "resolved arrow-memory-netty version drifted — the allocator module that broke under 18->19");
    }

    @Test
    void arrowMemoryNettyBufferPatchJarIsExpectedArrowVersion() {
        // arrow-memory-netty-buffer-patch is a DISTINCT bundled artifact (see
        // build/plugin/cqlite_flight/) that shims netty's UnsafeDirectLittleEndian
        // for arrow's allocator; it is the artifact that actually depends on the
        // 4.1.x netty baseline pinned in build.gradle.kts, so verify it directly
        // by its known member class rather than only asserting siblings.
        Class<?> patchClass;
        try {
            patchClass = Class.forName("org.apache.arrow.memory.patch.ArrowByteBufAllocator");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(
                    "arrow-memory-netty-buffer-patch's ArrowByteBufAllocator is not on the "
                            + "runtime classpath",
                    e);
        }
        assertEquals(
                EXPECTED_ARROW_VERSION,
                versionFromJar(patchClass, "arrow-memory-netty-buffer-patch"),
                "resolved arrow-memory-netty-buffer-patch version drifted from the #2193 pin");
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
     * Minimum arrow-group jars expected on the runtime classpath (issue #2193):
     * flight-core, arrow-format, arrow-vector, arrow-memory-core, arrow-memory-netty,
     * arrow-memory-netty-buffer-patch. A lower count means the enumeration is broken
     * (e.g. scanning the wrong classpath) rather than a legitimately smaller graph.
     */
    private static final int MIN_ARROW_JARS = 6;

    /**
     * Minimum "core" (non-tcnative) netty-group jars expected — netty-buffer,
     * -codec(+http/http2/socks), -common, -handler(+proxy), -resolver, -transport
     * (+native-unix-common) as pulled in by flight-core:19.0.0.
     */
    private static final int MIN_NETTY_CORE_JARS = 8;

    /**
     * Exhaustive full-set version check, closing the gap the per-class checks above
     * leave open: those check ONE representative class per module, so a NEW
     * transitive arrow/netty module Gradle resolution pulls in later (never anchored
     * by a hand-picked class here) could silently drift to the wrong version and go
     * undetected. This scans every jar on the runtime classpath, reads each one's
     * OWN embedded Maven {@code pom.properties} for its authoritative groupId
     * (rather than guessing from the filename — {@code flight-core-19.0.0.jar}
     * contains no {@code "arrow"} substring, so a filename-prefix heuristic would
     * silently miss it), and asserts EVERY {@code org.apache.arrow:*} jar is
     * {@link #EXPECTED_ARROW_VERSION} and EVERY {@code io.netty:*} jar (excluding
     * {@code netty-tcnative-*}, whose native-binding release train is
     * independently versioned — 2.0.74.Final is not a drift, it never tracks
     * netty core's 4.x line) is {@link #EXPECTED_NETTY_VERSION}. Non-empty minimum
     * counts guard against a broken/vacuous scan silently passing.
     */
    @Test
    void everyArrowAndNettyJarOnClasspathIsPinnedVersion() {
        List<MavenCoordinate> coords = scanClasspathMavenCoordinates();

        List<MavenCoordinate> arrowJars =
                coords.stream().filter(c -> c.groupId.equals("org.apache.arrow")).collect(Collectors.toList());
        assertTrue(
                arrowJars.size() >= MIN_ARROW_JARS,
                "expected >= " + MIN_ARROW_JARS + " org.apache.arrow:* jars on the classpath, found "
                        + arrowJars.size() + ": " + arrowJars);
        for (MavenCoordinate c : arrowJars) {
            assertEquals(
                    EXPECTED_ARROW_VERSION,
                    c.version,
                    "arrow jar drifted from the #2193 pin: " + c);
        }

        List<MavenCoordinate> nettyCoreJars = coords.stream()
                .filter(c -> c.groupId.equals("io.netty"))
                .filter(c -> !c.artifactId.startsWith("netty-tcnative"))
                .collect(Collectors.toList());
        assertTrue(
                nettyCoreJars.size() >= MIN_NETTY_CORE_JARS,
                "expected >= " + MIN_NETTY_CORE_JARS + " core io.netty:* jars on the classpath, found "
                        + nettyCoreJars.size() + ": " + nettyCoreJars);
        for (MavenCoordinate c : nettyCoreJars) {
            assertEquals(
                    EXPECTED_NETTY_VERSION,
                    c.version,
                    "netty jar drifted off arrow-19's tested 4.1.x baseline: " + c);
        }
    }

    /** A resolved {@code groupId:artifactId:version} read from a jar's embedded Maven metadata. */
    private record MavenCoordinate(String groupId, String artifactId, String version) {
        @Override
        public String toString() {
            return groupId + ":" + artifactId + ":" + version;
        }
    }

    /**
     * Walk every jar on {@code java.class.path} — the actual resolved runtime
     * classpath the test JVM (and, by the same Gradle resolution, the field's
     * bundled plugin) loads classes from — and read each one's
     * {@code META-INF/maven/<groupId>/<artifactId>/pom.properties} entry, the
     * authoritative Maven coordinates Gradle/Maven-published jars embed. Jars
     * without one (e.g. non-Maven-published) are silently skipped; none of the
     * arrow/netty artifacts this test cares about lack one.
     */
    private static List<MavenCoordinate> scanClasspathMavenCoordinates() {
        String classpath = System.getProperty("java.class.path");
        assertNotNull(classpath, "java.class.path system property is unset");
        List<MavenCoordinate> out = new ArrayList<>();
        for (String entry : classpath.split(File.pathSeparator)) {
            if (!entry.endsWith(".jar")) {
                continue;
            }
            File jarFile = new File(entry);
            if (!jarFile.isFile()) {
                continue;
            }
            try (JarFile jar = new JarFile(jarFile)) {
                jar.stream()
                        .filter(e -> e.getName().startsWith("META-INF/maven/") && e.getName().endsWith("pom.properties"))
                        .findFirst()
                        .ifPresent(e -> out.add(readCoordinate(jar, e)));
            } catch (IOException e) {
                throw new AssertionError("failed to open classpath jar " + entry, e);
            }
        }
        return out;
    }

    private static MavenCoordinate readCoordinate(JarFile jar, JarEntry pomProperties) {
        Properties props = new Properties();
        try (var in = jar.getInputStream(pomProperties)) {
            props.load(in);
        } catch (IOException e) {
            throw new AssertionError("failed to read " + pomProperties.getName() + " from " + jar.getName(), e);
        }
        return new MavenCoordinate(
                props.getProperty("groupId"), props.getProperty("artifactId"), props.getProperty("version"));
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
