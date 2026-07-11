import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.30.0"
}

group = "in.mcfad"

// Version derives from the `version` project property so CI can pass the release
// tag (`-Pversion=0.13.0`). A bare local build (property absent → Gradle's
// "unspecified") falls back to a defined dev version so the build never breaks.
version = (findProperty("version") as String?)
    ?.removePrefix("v")
    ?.takeIf { it.isNotBlank() && it != "unspecified" }
    ?: "0.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

// Latest Trino at time of writing. The SPI is `compileOnly` — Trino provides it
// at runtime from the engine classpath; bundling it would clash. It must stay
// out of the published POM's runtime dependencies for the same reason.
val trinoVersion = "481"
// arrow-java pinned to 19.0.0 to align with Trino 481's own arrow-consuming
// plugins (pinot/bigquery) and to fix the round-5 field decode failure
// ("Failed to read message" on every table read) under 18.1.0 (issue #2193).
// `flight-core` is the sole arrow artifact declared; every other arrow module
// (arrow-vector, arrow-memory-core/-netty, arrow-format, flight-grpc) rides in
// transitively at this SAME version, so bumping this one variable moves the
// whole arrow-java stack in lockstep. Enforced at runtime by
// ArrowJavaVersionPinTest against a silent dependency-resolution downgrade.
val arrowVersion = "19.0.0"
val jacksonVersion = "2.18.2"
// Netty is pinned to the 4.1.x line arrow-java 19.0.0 was built and tested
// against (issue #2193). flight-core:19.0.0's published Gradle metadata drags
// several netty modules UP to 4.2.9.Final via conflict resolution, but arrow's
// own `arrow-memory-netty-buffer-patch:19.0.0` and `grpc-netty:1.79.0` both
// request netty 4.1.130.Final — and arrow's netty allocator is INCOMPATIBLE
// with netty 4.2.x: its `UnsafeDirectLittleEndian.<init>` calls
// `EmptyByteBuf.memoryAddress()`, which netty 4.2 changed to throw
// `UnsupportedOperationException`, failing `NettyAllocationManager`'s static
// init (`ExceptionInInitializerError`) on the FIRST RootAllocator — i.e. every
// table read. The enforced BOM below forces the whole netty stack down to the
// arrow-19-tested 4.1.130.Final so the allocator initializes. This mirrors a
// real Trino runtime, which supplies netty 4.1.x.
val nettyVersion = "4.1.130.Final"

// The exact netty core modules flight-core:19.0.0 (+ grpc-netty:1.79.0) drag onto
// the runtime classpath. Several are requested transitively at 4.2.9.Final and are
// only forced down to the arrow-19-tested 4.1.130.Final by the enforced BOM below.
// Declaring each one EXPLICITLY (issue #2300) is what makes the pin survive into the
// published Maven POM's <dependencies> — see the note on the BOM import below. This
// list is the DECLARATION site only: `verifyPublishedPomNettyPin` derives the
// EXPECTED set from the RESOLVED runtime graph (not this list), so if flight-core /
// grpc later drags an ADDITIONAL io.netty core module in via the BOM without a
// matching entry here, that task FAILS CLOSED naming the un-declared module (it
// would otherwise be silently omitted from the POM). tcnative is intentionally
// excluded: its native-binding train (2.0.74.Final) is versioned independently of
// netty's 4.x line and is not part of the 4.1.x allocator pin.
val nettyCoreModules = listOf(
    "netty-buffer",
    "netty-codec",
    "netty-codec-http",
    "netty-codec-http2",
    "netty-codec-socks",
    "netty-common",
    "netty-handler",
    "netty-handler-proxy",
    "netty-resolver",
    "netty-transport",
    "netty-transport-native-unix-common",
)

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // The BOM constrains THIS build's resolution. A Maven/Central consumer assembling
    // the plugin reads only the POM: a BOM `import` lands in the POM's
    // <dependencyManagement>, and Maven dependencyManagement is NOT transitive to
    // downstream consumers, so it would NOT stop a Maven assembly from resolving
    // flight-core's transitive netty at 4.2.x (re-exposing the arrow-19 allocator
    // break; issue #2300). The explicit per-module declarations below land in the
    // POM's <dependencies> with pinned versions, so a downstream Maven build resolves
    // them at 4.1.130.Final by nearest-wins — those 11 pins carry the version
    // authority for both this build and the published component.
    //
    // Use a NON-enforced `platform(...)`, not `enforcedPlatform(...)` (issue #2334):
    // Gradle 9.1's publication validation rejects an enforced platform in a published
    // component's `runtimeElements` variant because forced constraints leak to Gradle
    // consumers as hard overrides (`generateMetadataFileForMavenPublication` FAILS).
    // Enforcement is redundant given the explicit pins; if `platform()` alone ever
    // lets a transitive netty core module float in the RESOLVED graph, the
    // graph-derived `verifyPublishedPomNettyPin` task below catches it — resolve any
    // such drift with an explicit pin, never by re-enforcing.
    implementation(platform("io.netty:netty-bom:$nettyVersion"))
    // STRICT version pins (issue #2334): a plain `implementation("io.netty:x:4.1.130")`
    // declaration LOSES to grpc-netty/flight-core's transitive 4.2.x under Gradle's
    // highest-version-wins, so the graph floats to 4.2.9.Final (re-exposing the arrow-19
    // allocator break). A `strictly` constraint forces the downgrade to 4.1.130.Final,
    // wins against the transitive requests, and — unlike `enforcedPlatform` — publishes
    // cleanly as the module's own strict requirement (no enforced-platform leak, so
    // Gradle 9.1's publication validation passes). These 11 strict pins ARE the version
    // authority for the build and the published component.
    nettyCoreModules.forEach {
        implementation("io.netty:$it") { version { strictly(nettyVersion) } }
    }
    implementation("org.apache.arrow:flight-core:$arrowVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    testImplementation("io.trino:trino-spi:$trinoVersion")
    // Optional<>-aware (de)serialization mirrors Trino's split codec so the
    // CqliteFlightSplit JSON round-trip test (issue #2241) is faithful.
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

// Central requires a javadoc jar; the JDK 25 doclint is strict, so relax it to a
// warning-free build rather than failing the release on documentation lint.
tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

// Assemble the Trino plugin directory (jar + runtime deps) for docker-compose to
// mount at /usr/lib/trino/plugin/cqlite_flight. Trino loads each plugin from its
// own isolated classloader directory, so runtime deps must be co-located.
tasks.register<Sync>("installPlugin") {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("plugin/cqlite_flight"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}

// Arrow on JDK 25 needs the foreign-memory module opened for off-heap access.
tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
}

// --- Published-POM netty-pin oracle (issue #2300) ----------------------------
// Assert the CONSUMER-FACING artifact: every netty core module that ACTUALLY
// RESOLVES onto the runtime classpath appears in the generated POM's <dependencies>
// at the pinned nettyVersion. This is the property a downstream Maven assembly reads
// (the enforced BOM alone lands only in <dependencyManagement>, which is not
// transitive to consumers).
//
// The EXPECTED netty set is DERIVED FROM THE RESOLVED runtimeClasspath GRAPH, not
// from the hand-maintained `nettyCoreModules` list — so this check cannot degrade to
// a list-echo. If flight-core / grpc later drags an ADDITIONAL io.netty core module
// in via the BOM without a matching explicit declaration, that module resolves onto
// the classpath but is OMITTED from the POM, and this task FAILS CLOSED naming it.
// The netty-tcnative-* native-binding train is versioned independently
// (nettyTcnativeVersion) and is intentionally NOT declared in the POM, so it is
// checked against its own expected version but not required in <dependencies>.
// Reads the real generated pom-default.xml — not build.gradle.kts — so it cannot
// pass vacuously.
val nettyTcnativeVersion = "2.0.74.Final"
val verifyPublishedPomNettyPin by tasks.registering {
    description =
        "Assert every RESOLVED netty core module is version-pinned in the published POM <dependencies> (#2300)."
    group = "verification"
    dependsOn("generatePomFileForMavenPublication")
    val pomFile = layout.buildDirectory.file("publications/maven/pom-default.xml")
    val expectedNettyVersion = nettyVersion
    val expectedTcnativeVersion = nettyTcnativeVersion
    // Captured lazily at configuration time; the artifact set resolves at execution.
    val runtimeArtifacts = configurations.runtimeClasspath.get().incoming.artifacts
    inputs.file(pomFile)
    inputs.files(configurations.runtimeClasspath)
    doLast {
        // 1. Derive the resolved io.netty artifact set (name -> version) from the
        //    runtime dependency graph — the authoritative source of truth.
        val resolvedNetty = runtimeArtifacts.artifacts.mapNotNull { art ->
            val id = art.id.componentIdentifier
            if (id is org.gradle.api.artifacts.component.ModuleComponentIdentifier && id.group == "io.netty") {
                id.moduleIdentifier.name to id.version
            } else {
                null
            }
        }.toMap()
        require(resolvedNetty.isNotEmpty()) {
            "no io.netty artifacts resolved on runtimeClasspath — graph-derivation is broken (#2300)"
        }
        // The tcnative native-binding train is independently versioned and
        // intentionally excluded from the POM pin; every other io.netty artifact is a
        // core module that MUST land in the published POM.
        val (tcnative, core) = resolvedNetty.entries.partition { it.key.startsWith("netty-tcnative") }

        // 2. Parse the generated POM's top-level <dependencies> (NOT
        //    <dependencyManagement> — a downstream Maven consumer resolves versions
        //    from <dependencies>).
        val pom = pomFile.get().asFile
        require(pom.isFile) { "generated POM not found at $pom" }
        val doc = javax.xml.parsers.DocumentBuilderFactory.newInstance()
            .apply { isNamespaceAware = false }
            .newDocumentBuilder()
            .parse(pom)
        val depsNodes = (0 until doc.documentElement.childNodes.length)
            .map { doc.documentElement.childNodes.item(it) }
            .filter { it.nodeName == "dependencies" }
        require(depsNodes.isNotEmpty()) { "published POM has no top-level <dependencies> block: $pom" }
        val pomDeps = mutableMapOf<String, String>()
        depsNodes.forEach { deps ->
            val nodes = deps.childNodes
            for (i in 0 until nodes.length) {
                val dep = nodes.item(i)
                if (dep.nodeName != "dependency") continue
                var groupId: String? = null
                var artifactId: String? = null
                var version: String? = null
                val fields = dep.childNodes
                for (j in 0 until fields.length) {
                    when (fields.item(j).nodeName) {
                        "groupId" -> groupId = fields.item(j).textContent.trim()
                        "artifactId" -> artifactId = fields.item(j).textContent.trim()
                        "version" -> version = fields.item(j).textContent.trim()
                    }
                }
                if (groupId != null && artifactId != null && version != null) {
                    pomDeps["$groupId:$artifactId"] = version
                }
            }
        }

        val problems = mutableListOf<String>()
        // 3a. Every RESOLVED core netty module must be declared in the POM at the pin.
        //     An un-declared resolved module is the exact fail-closed case (#2300).
        core.forEach { (module, resolvedVersion) ->
            if (resolvedVersion != expectedNettyVersion) {
                problems +=
                    "resolved io.netty:$module at $resolvedVersion on runtimeClasspath, expected $expectedNettyVersion"
            }
            when (val pomVersion = pomDeps["io.netty:$module"]) {
                null -> problems +=
                    "resolved io.netty:$module is NOT declared in the published POM <dependencies> — it would be " +
                        "omitted from downstream Maven resolution; add it to nettyCoreModules"
                expectedNettyVersion -> {}
                else -> problems += "io.netty:$module declared at $pomVersion in POM, expected $expectedNettyVersion"
            }
        }
        // 3b. tcnative train: pinned to its own independent version, POM-excluded by design.
        tcnative.forEach { (module, resolvedVersion) ->
            if (resolvedVersion != expectedTcnativeVersion) {
                problems +=
                    "resolved io.netty:$module at $resolvedVersion, expected tcnative train $expectedTcnativeVersion"
            }
        }
        // 3c. Reject a stale hand-list: any netty dep declared in the POM that no
        //     longer resolves would ship a dead <dependency> to consumers.
        pomDeps.keys.filter { it.startsWith("io.netty:") }.forEach { key ->
            val module = key.removePrefix("io.netty:")
            if (!resolvedNetty.containsKey(module)) {
                problems +=
                    "$key is declared in the POM but no longer resolves on runtimeClasspath (stale nettyCoreModules entry)"
            }
        }
        require(problems.isEmpty()) {
            "published POM netty pin drift (#2300):\n  " + problems.joinToString("\n  ")
        }
        logger.lifecycle(
            "verifyPublishedPomNettyPin: ${core.size} resolved netty core modules pinned to " +
                "$expectedNettyVersion in POM <dependencies>; ${tcnative.size} tcnative artifacts at " +
                "$expectedTcnativeVersion (#2300)",
        )
    }
}

// Run the POM oracle as part of `check` so `./gradlew check` (and any CI that runs
// it) enforces the published pin. The connector PR lane runs it explicitly.
tasks.named("check") { dependsOn(verifyPublishedPomNettyPin) }

// --- Maven Central publication (Central Portal via vanniktech) ---------------
// `publishToMavenLocal` / `publishToMavenCentral` produce main + sources +
// javadoc jars and a Central-compliant POM. `trino-spi` is compileOnly, so it is
// absent from the POM's runtime scope; `flight-core` + `jackson-databind` are
// `implementation`, so they appear as runtime dependencies.
//
// `automaticRelease = true` (issue #2156): the pinned 0.30.0 plugin's
// `publishToMavenCentral(SonatypeHost, automaticRelease: Boolean)` overload
// defaults `automaticRelease` to `false` when omitted — verified by decompiling
// the cached 0.30.0 plugin jar: the Kotlin `$default` bridge loads `iconst_0`
// (`false`) for the boolean slot when the caller doesn't pass it. That default
// is why every prior publish (0.13.0/0.13.1/0.13.2) landed
// VALIDATED-but-PENDING in Central Portal Deployments and needed a manual
// Publish click. Passing `true` here makes a successful
// `publishToMavenCentral` invocation release automatically — no portal step.
mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL, automaticRelease = true)
    coordinates("in.mcfad", "cqlite-trino", version.toString())
    configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = true))

    // Sign only when an in-memory GPG key is configured (env SIGNING_KEY /
    // SIGNING_PASSWORD → ORG_GRADLE_PROJECT_signingInMemoryKey* in CI). A
    // secret-free `publishToMavenLocal` then still succeeds without signatures.
    if (project.hasProperty("signingInMemoryKey")) {
        signAllPublications()
    }

    pom {
        name.set("cqlite-trino")
        description.set(
            "Trino connector for CQLite — query Apache Cassandra SSTables through the " +
                "CQLite Arrow Flight service, no Cassandra cluster required.",
        )
        url.set("https://github.com/pmcfadin/cqlite")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("pmcfadin")
                name.set("Patrick McFadin")
                url.set("https://github.com/pmcfadin")
            }
        }
        scm {
            connection.set("scm:git:https://github.com/pmcfadin/cqlite.git")
            developerConnection.set("scm:git:ssh://git@github.com/pmcfadin/cqlite.git")
            url.set("https://github.com/pmcfadin/cqlite")
        }
    }
}
