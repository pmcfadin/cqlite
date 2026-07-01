import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

/*
 * Compaction-parity harness (issue #842).
 *
 * Builds against a pinned Apache Cassandra 5.0.2 source checkout produced by
 * scripts/bootstrap-cassandra.sh. The harness runs Cassandra's compaction in-JVM
 * (the reference) and shells out to the `cqlite compact` binary (the candidate),
 * then compares the outputs (logical tier now; byte tier in a follow-up).
 */

plugins {
    java
}

// ── Locate the Cassandra source checkout ────────────────────────────────────
val defaultCassandraSrc =
    "${System.getProperty("user.home")}/.cache/cqlite/cassandra-src/cassandra-5.0.2"
val cassandraSrc: String =
    (findProperty("cassandraSrc") as String?)
        ?: System.getenv("CQLITE_CASSANDRA_SRC")
        ?: defaultCassandraSrc

val cassandraMainClasses = file("$cassandraSrc/build/classes/main")
val cassandraTestClasses = file("$cassandraSrc/build/test/classes")
val jammAgent = file("$cassandraSrc/lib/jamm-0.4.0.jar")
val cassandraYaml = file("$cassandraSrc/test/conf/cassandra.yaml")
val sstabledump = file("$cassandraSrc/tools/bin/sstabledump")

fun cassandraJars() = files(
    fileTree("$cassandraSrc/lib") { include("*.jar") },
    fileTree("$cassandraSrc/build/lib/jars") { include("*.jar") },
    fileTree("$cassandraSrc/build/test/lib/jars") { include("*.jar") },
)

java {
    // Cassandra 5.0 runs on JDK 11 or 17 (NOT 21). Pin the test JVM to 17.
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

dependencies {
    // Cassandra main + test-tree classes, plus all of Cassandra's dependency jars.
    testImplementation(files(cassandraMainClasses, cassandraTestClasses))
    testImplementation(cassandraJars())
}

// ── cqlite binary under test ────────────────────────────────────────────────
// Default to the workspace debug build; override with -Dcqlite.bin or $CQLITE_BIN.
val cqliteBin: String =
    System.getProperty("cqlite.bin")
        ?: System.getenv("CQLITE_BIN")
        ?: file("${rootDir.parent}/target/debug/cqlite").absolutePath

// JDK 17 module access required by Cassandra's internals (mirrors build.xml).
val jvm17Args = listOf(
    "-Djdk.attach.allowAttachSelf=true",
    "-XX:+UseG1GC",
    "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports", "java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED",
    "--add-exports", "java.rmi/sun.rmi.registry=ALL-UNNAMED",
    "--add-exports", "java.rmi/sun.rmi.server=ALL-UNNAMED",
    "--add-exports", "java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED",
    "--add-exports", "java.sql/java.sql=ALL-UNNAMED",
    "--add-exports", "java.base/java.lang.ref=ALL-UNNAMED",
    "--add-exports", "jdk.unsupported/sun.misc=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.module=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.math=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.module=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.util.jar=ALL-UNNAMED",
    "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/java.io=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang=ALL-UNNAMED",
    "--add-opens", "java.base/java.util=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED",
    "--add-opens", "java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
)

// Shared configuration for both parity tiers (issue #1016). The `label` keeps the
// per-task storage + artifacts dirs distinct so the logical and byte tiers never
// clobber each other when both run in one CI job.
fun Test.configureParityHarness(label: String) {
    useJUnit() // Cassandra's test tree is JUnit 4

    // Fail fast and loudly if the harness isn't wired up, rather than silently passing.
    doFirst {
        require(cassandraMainClasses.isDirectory && cassandraTestClasses.isDirectory) {
            "Cassandra classes not found under $cassandraSrc. Run scripts/bootstrap-cassandra.sh first."
        }
        require(file(cqliteBin).exists()) {
            "cqlite binary not found at $cqliteBin. Build it with " +
                "`cargo build --features write-support` or set -Dcqlite.bin."
        }
    }

    jvmArgs(jvm17Args)
    jvmArgs("-javaagent:${jammAgent.absolutePath}")

    systemProperty("cassandra.config", cassandraYaml.toURI().toString())
    systemProperty(
        "cassandra.storagedir",
        layout.buildDirectory.dir("cassandra-storage-$label").get().asFile.absolutePath,
    )
    systemProperty("cqlite.bin", cqliteBin)
    systemProperty("cassandra.src", cassandraSrc)
    systemProperty("cassandra.sstabledump", sstabledump.absolutePath)
    // Where the harness preserves per-scenario artifacts (inputs, both outputs,
    // schema, command lines, stdout/stderr, JSONL, checksums, byte diff).
    systemProperty(
        "parity.artifacts.dir",
        layout.buildDirectory.dir("parity-artifacts-$label").get().asFile.absolutePath,
    )
    // Issue #1027: the SHARED, scenario-id-keyed failure-bundle root. On a failure
    // the harness writes <root>/parity-failures/<tier>/<scenario_id>/failure-artifact.json
    // (+ diffs/ + repro/). The root is `build/`, so the bundle lands at
    // compaction-parity/build/parity-failures/<tier>/<scenario_id>/ — exactly the
    // glob compaction-parity.yml uploads (Wave 2b). A single shared root across
    // both tiers is safe because the logical (required_parity) and byte
    // (nightly_docker) tiers key into distinct tier subdirectories.
    systemProperty(
        "parity.failures.dir",
        layout.buildDirectory.get().asFile.absolutePath,
    )
    // Pinned Cassandra provenance recorded in the failure-artifact record so it is
    // comparable to the manifest cassandra_source pin (issue #1027).
    systemProperty("parity.cassandra.version", "5.0.2")
    systemProperty("parity.cassandra.git.sha", "f278f6774fc76465c182041e081982105c3e7dbb")

    maxParallelForks = 1

    testLogging {
        events(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.SKIPPED)
        exceptionFormat = TestExceptionFormat.FULL
        showStandardStreams = true
    }
}

// LOGICAL tier (hard gate): canonical sstabledump equality. Byte diffs are still
// computed and preserved as artifacts here, but NOT asserted.
tasks.test {
    configureParityHarness("test")
}

// BYTE tier (#842 north star): same scenarios, but additionally assert every
// output component is byte-identical with NO allowlist. This is the
// `nightly_docker`-tier assertion run — the per-PR `test` task already computes
// and persists the same byte diff + checksums as artifacts, so CI runs byteParity
// on the nightly schedule + workflow_dispatch (see
// .github/workflows/compaction-parity.yml) to avoid doubling the expensive
// compaction on PRs. Non-blocking until the writer is byte-stable;
// promote by dropping continue-on-error on the workflow step. Invoke with
// `gradle byteParity`.
val byteParity by tasks.registering(Test::class) {
    description = "Byte-for-byte compaction parity tier (per-component cmp, no allowlist; nightly/dispatch)."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    configureParityHarness("byteParity")
    systemProperty("parity.tier", "byte")
}
