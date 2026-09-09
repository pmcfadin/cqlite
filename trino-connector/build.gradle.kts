import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost
// Imported rather than written fully-qualified on purpose: in a Kotlin DSL build
// script the `java` extension accessor (from the `java` plugin, configured below)
// SHADOWS the root `java` package, so a fully-qualified `java.util.zip.ZipFile`
// fails to resolve with a misleading "Unresolved reference 'util'".
import java.io.ByteArrayInputStream
import java.util.Properties
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.30.0"
    // Shaded ("fat") jar for the single-file Trino plugin drop (issue #2869).
    //
    // 9.4.3 is the newest shadow release whose Gradle floor (9.0) is satisfied by
    // THIS project's wrapper — 9.5.0+ requires Gradle 9.2 and 9.7.0+ requires 9.4.
    // VERIFIED, not assumed: `./gradlew shadowJar` under wrapper 9.1.0 builds and
    // emits build/libs/cqlite-trino-<v>-all.jar.
    //
    // Do NOT resolve a future shadow floor by bumping the wrapper. The wrapper
    // version is in LOCKSTEP with the easy-db-lab kit's `gradle:9.1.0-jdk25` init
    // image (rustyrazorblade/easy-db-lab#731): that image is what a consumer builds
    // this connector with, so wrapper != image is a broken contract on their side.
    // Moving either one is a coordinated, two-repo change.
    id("com.gradleup.shadow") version "9.4.3"
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

// --- Shaded single-file plugin jar (issue #2869) ------------------------------
// KEEP THE MAVEN CENTRAL PUBLICATION BYTE-IDENTICAL. Applying shadow would
// pollute it by DEFAULT: `ShadowJavaPlugin.configureComponents()` adds a
// `shadowRuntimeElements` variant into the `java` software component, and
// vanniktech's `configure(JavaLibrary(...))` below publishes exactly
// `components["java"]` — so the shaded jar would appear in Central's Gradle
// module metadata as an extra variant. Central keeps publishing the THIN jar
// only; there is deliberately no `:all` classifier there, because the fat jar is
// a GitHub Release asset with its own lifecycle, not a library coordinate.
// `verifyShadowNotPublished` below asserts this from the GENERATED metadata.
shadow {
    addShadowVariantIntoJavaComponent = false
    // `assemble`/`build` stay thin-jar-only; the fat jar is built by an explicit
    // `shadowJar`/`installPluginFat`/`check` request, so a routine build does not
    // pay 19 MiB of shading.
    addShadowJarToAssembleLifecycle = false
}

tasks.shadowJar {
    // Shadow's default, set explicitly because the FILENAME is the contract:
    // easy-db-lab caches the asset on `cqlite-trino-<version>-all.jar`.
    archiveClassifier = "all"

    // NO RELOCATION, deliberately. Trino 481's PluginManager gives each plugin
    // directory a child-first classloader whose ONLY parent-first packages are
    // SPI_PACKAGES (`io.trino.spi.`, `com.fasterxml.jackson.annotation.`,
    // `io.airlift.slice.`, `io.opentelemetry.api.`, `io.opentelemetry.context.`).
    // So bundled netty / grpc / arrow / jackson-databind are ALREADY isolated from
    // the engine's copies — relocation would buy nothing and cost three ways:
    //   * jackson ANNOTATIONS must resolve to the ENGINE's copy for ConnectorSplit
    //     JSON interop, and a package rewrite would break that delegation;
    //   * relocating netty breaks its `META-INF/native/lib*.so` name lookup, which
    //     is computed from the (unrelocated) package name at runtime;
    //   * arrow's netty allocator reaches across both stacks (see the #2193 pin
    //     note above), so rewriting one and not the other splits it.
    // If a future Trino release makes one of these packages parent-first, relocate
    // THAT package only, and record the SPI_PACKAGES evidence here.

    // Merge duplicated ServiceLoader descriptors instead of taking the first.
    mergeServiceFiles()
    // netty's per-module version attestation: 9 modules each ship a 9-line
    // properties file at the SAME path, so first-wins leaves the runtime claiming
    // one module exists. Concatenation is correct here — the keys are
    // module-qualified (`netty-codec-http2.version=...`).
    append("META-INF/io.netty.versions.properties")

    // THE CRITICAL BIT (#2869): the two calls above are a NO-OP without this.
    //
    // Shadow's `duplicatesStrategy` default is EXCLUDE, and it takes PRECEDENCE over
    // transforming: the 2nd..Nth copy of a duplicated resource is dropped before it
    // ever reaches ServiceFileTransformer / AppendingTransformer, so `mergeServiceFiles()`
    // and `append(...)` silently degrade to first-wins. MEASURED on this graph with
    // both calls configured and this block removed — `verifyFatJar` reported that
    // META-INF/services/io.grpc.NameResolverProvider lost
    // `io.grpc.internal.DnsNameResolverProvider` (grpc-core, beaten by grpc-netty) and
    // io.grpc.LoadBalancerProvider lost `io.grpc.internal.PickFirstLoadBalancerProvider`
    // (grpc-core, beaten by grpc-util), and that 10 of 11 netty modules were missing
    // from io.netty.versions.properties. The failure is NEARLY INVISIBLE at runtime:
    // io.trino.spi.Plugin is single-source, so Trino's startup `checkState` passes and
    // the plugin LOADS — then the first query has no default gRPC name resolver or load
    // balancer.
    //
    // EXCLUDE stays the correct GLOBAL default (LICENSE, NOTICE and arrow-git.properties
    // all want first-wins), so the strategy is bypassed ONLY on the paths the two
    // transformers above own. `arrow-git.properties` (6 copies) deliberately stays at
    // first-wins: unlike netty's, its keys are NOT module-qualified, so concatenating
    // six of them yields duplicate keys and a misleading build/commit attribution.
    filesMatching(listOf("META-INF/services/**", "META-INF/io.netty.versions.properties")) {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }

    // Fail rather than ship a jar with two entries of one name. Note this is NOT
    // contradicted by the INCLUDE above: INCLUDE admits the later copies into the
    // TRANSFORMER, which then emits exactly one merged entry per path.
    failOnDuplicateEntries = true
}

// The single-jar counterpart to `installPlugin`. `Sync`, never `Copy`, so a
// version bump cannot leave a stale second jar in the directory — Trino would load
// both and the newer classes might lose. A SEPARATE output root
// (build/plugin-fat/) keeps the multi-jar `build/plugin/cqlite_flight` tree
// untouched, since docker-compose still mounts that one.
tasks.register<Sync>("installPluginFat") {
    dependsOn(tasks.shadowJar)
    into(layout.buildDirectory.dir("plugin-fat/cqlite_flight"))
    from(tasks.shadowJar)
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

// --- Shaded-jar contents oracle (issue #2869) --------------------------------
// The consumer-facing artifact here is ONE jar dropped into a Trino plugin
// directory (easy-db-lab fetches it once and mounts it per node via hostPath
// instead of resolving the 50-artifact runtime tree in an initContainer at every
// Trino pod start). So the property that matters is: everything the multi-jar
// `installPlugin` tree provided is still REACHABLE from this single file.
//
// Modelled deliberately on `verifyPublishedPomNettyPin` above — expectations are
// DERIVED FROM THE RESOLVED runtime graph (never from a hand-maintained list, so
// the check cannot degrade to a list-echo), read back out of the REAL artifact on
// disk (so it cannot pass by reading build.gradle.kts), java.base only (no new
// dependency), and every failure names the offender.
//
// Zip reading uses TWO java.base APIs for two different reasons:
//   * `ZipInputStream` for the ordered entry census. It walks LOCAL headers and
//     de-duplicates NOTHING by construction, so a duplicated entry name shows up
//     twice. A central-directory read (`ZipFile.entries()`) collapses duplicates
//     on some paths, which would make the duplicate check vacuous.
//   * `ZipFile` for content reads (random access by name).
//
// The archive NAME is part of the contract: easy-db-lab caches on
// `cqlite-trino-<version>-all.jar`, so the oracle resolves the artifact by that
// exact path and fails closed if a shadow bump ever renames it.
val shadedJarLocation = "libs/cqlite-trino-$version-all.jar"

val verifyFatJar by tasks.registering {
    description =
        "Assert the shaded cqlite-trino-<v>-all.jar is complete, service-merged and version-pinned (#2869)."
    group = "verification"
    val shadedJar = layout.buildDirectory.file(shadedJarLocation)
    val expectedNettyVersion = nettyVersion
    val expectedTcnativeVersion = nettyTcnativeVersion
    val expectedArrowVersion = arrowVersion
    // The single service entry this plugin owns. Trino's PluginManager reads it at
    // startup; a `checkState` there is the ONLY runtime signal that the jar is a
    // plugin at all, and it passes even when every OTHER service descriptor has
    // silently degraded to first-wins — see the merge note on `shadowJar`.
    val expectedPluginClass = "in.mcfad.cqlite.flight.CqliteFlightPlugin"
    val nettyVersionsPath = "META-INF/io.netty.versions.properties"
    dependsOn(tasks.shadowJar)
    // Captured lazily at configuration time; the artifact set resolves at execution.
    val runtimeArtifacts = configurations.runtimeClasspath.get().incoming.artifacts
    inputs.files(configurations.runtimeClasspath)
    doLast {
        val jar = shadedJar.get().asFile
        require(jar.isFile) {
            "shaded jar not found at $jar — shadowJar produced no artifact under the contracted " +
                "consumer name cqlite-trino-$version-all.jar (#2869)"
        }

        // A service descriptor is a provider list: strip `#` comments and blanks so a
        // comment-only file cannot masquerade as a populated one.
        fun providerLines(bytes: ByteArray): Set<String> =
            bytes.toString(Charsets.UTF_8)
                .lineSequence()
                .map { it.substringBefore('#').trim() }
                .filter { it.isNotEmpty() }
                .toSet()

        fun propertyVersion(bytes: ByteArray): String? =
            Properties()
                .apply { load(ByteArrayInputStream(bytes)) }
                .getProperty("version")
                ?.trim()

        // 1. Ordered LOCAL-header census of the shaded jar (see the note above on why
        //    this is ZipInputStream and not ZipFile).
        val entryNames = mutableListOf<String>()
        ZipInputStream(jar.inputStream().buffered()).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                if (!entry.isDirectory) entryNames += entry.name
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        require(entryNames.isNotEmpty()) { "shaded jar $jar holds no non-directory entries" }
        val entries = entryNames.toSet()

        // 2. Derive every expectation from the RESOLVED runtime graph: module
        //    coordinates, the per-source-jar service census and every source jar's
        //    entry list, in ONE pass over the tree.
        val artifacts = runtimeArtifacts.artifacts.toList()
        require(artifacts.isNotEmpty()) {
            "runtimeClasspath resolved to zero artifacts — graph-derivation is broken, so every " +
                "expectation below would be vacuous (#2869)"
        }
        val resolvedNetty = mutableMapOf<String, String>()
        val resolvedArrow = mutableMapOf<String, String>()
        val serviceUnion = mutableMapOf<String, MutableSet<String>>()
        val serviceSources = mutableMapOf<String, MutableSet<String>>()
        val sourceEntries = mutableMapOf<String, List<String>>()
        val unreadable = mutableListOf<String>()
        for (art in artifacts) {
            val label = art.id.componentIdentifier.displayName
            val id = art.id.componentIdentifier
            if (id is org.gradle.api.artifacts.component.ModuleComponentIdentifier) {
                when (id.group) {
                    "io.netty" -> resolvedNetty[id.moduleIdentifier.name] = id.version
                    "org.apache.arrow" -> resolvedArrow[id.moduleIdentifier.name] = id.version
                }
            }
            val file = art.file
            if (!file.isFile || !file.name.endsWith(".jar")) {
                // Not skipped silently: an unreadable member of the graph would make the
                // contribution arithmetic below dishonest.
                unreadable += "$label -> ${file.name}"
                continue
            }
            val names = mutableListOf<String>()
            ZipFile(file).use { zf ->
                for (entry in zf.entries().asSequence()) {
                    if (entry.isDirectory) continue
                    val name = entry.name
                    names += name
                    // Only top-level `META-INF/services/<fqcn>` descriptors, never a
                    // nested path (which is not a ServiceLoader lookup key).
                    if (name.startsWith("META-INF/services/") && name.count { it == '/' } == 2) {
                        val lines = providerLines(zf.getInputStream(entry).use { it.readBytes() })
                        if (lines.isNotEmpty()) {
                            serviceUnion.getOrPut(name) { mutableSetOf() } += lines
                            serviceSources.getOrPut(name) { mutableSetOf() } += label
                        }
                    }
                }
            }
            sourceEntries[label] = names
        }

        // Probe selection is DERIVED, not allowlisted. A probe must be an entry name
        // that (a) shadow does not legitimately strip and (b) is UNIQUE to one source
        // jar across the whole graph. Uniqueness is what makes absence conclusive: a
        // name several jars share can be legitimately absent-as-a-copy under the
        // first-wins EXCLUDE strategy, so it could never distinguish "bundled" from
        // "dropped". Classes outside META-INF/ are preferred (they are what a
        // classloader actually needs); a resource is accepted when a jar has no unique
        // class of its own.
        //
        // Three artifacts in TODAY's graph have no such probe, all three genuinely
        // content-free placeholders — measured, not assumed:
        //   io.grpc:grpc-context           holds ONLY META-INF/MANIFEST.MF (the API
        //                                  moved into grpc-api),
        //   com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava
        //                                  is the well-known empty conflict placeholder,
        //   io.netty:netty-tcnative-boringssl-static (classifier-less) is a marker POM
        //                                  jar; the natives live in the per-platform
        //                                  CLASSIFIED jars, which this graph does not
        //                                  resolve (hence zero META-INF/native entries).
        // They are reported by NAME in the census as INDISTINGUISHABLE rather than
        // waived silently or hard-coded into an allowlist that would drift: a jar whose
        // every entry is shadow-stripped or byte-shared with a sibling contributes
        // nothing a probe CAN see, and that is a measurement, not an exception.
        fun strippedByShadow(name: String): Boolean =
            name.endsWith("module-info.class") ||
                name == "META-INF/INDEX.LIST" ||
                (
                    name.startsWith("META-INF/") &&
                        (name.endsWith(".SF") || name.endsWith(".DSA") || name.endsWith(".RSA"))
                    )
        val globalNameCounts = sourceEntries.values.flatten().groupingBy { it }.eachCount()
        fun eligible(name: String): Boolean = globalNameCounts[name] == 1 && !strippedByShadow(name)
        val probes = mutableMapOf<String, String>()
        val indistinguishable = mutableListOf<String>()
        sourceEntries.forEach { (label, names) ->
            val probe = names.firstOrNull { eligible(it) && it.endsWith(".class") && !it.startsWith("META-INF/") }
                ?: names.firstOrNull { eligible(it) }
            if (probe == null) indistinguishable += label else probes[label] = probe
        }
        require(probes.isNotEmpty()) {
            "no resolved runtime artifact yielded a unique probe entry — the contribution census " +
                "would prove nothing, so the graph has changed shape (#2869)"
        }

        val problems = mutableListOf<String>()
        problems += unreadable.map { "resolved runtime artifact is not a readable jar: $it" }

        ZipFile(jar).use { zf ->
            fun read(path: String): ByteArray? =
                zf.getEntry(path)?.let { e -> zf.getInputStream(e).use { it.readBytes() } }

            // 3. Our own Plugin descriptor survived, and names the real entry point.
            val pluginDescriptor = "META-INF/services/io.trino.spi.Plugin"
            when (val bytes = read(pluginDescriptor)) {
                null -> problems += "$pluginDescriptor is ABSENT — Trino would not recognise this jar as a plugin"
                else -> {
                    val declared = providerLines(bytes)
                    if (declared.isEmpty()) {
                        problems += "$pluginDescriptor is blank after stripping comments/whitespace"
                    } else if (expectedPluginClass !in declared) {
                        problems += "$pluginDescriptor declares $declared, expected to include $expectedPluginClass"
                    }
                }
            }

            // 4. trino-spi is `compileOnly`, so it CANNOT be bundled. Asserted rather
            //    than assumed: flipping that declaration to `implementation` would
            //    otherwise silently ship the engine SPI inside the plugin jar and
            //    break Trino's SPI_PACKAGES parent-first delegation.
            val bundledSpi = entries.filter { it.startsWith("io/trino/spi/") }
            if (bundledSpi.isNotEmpty()) {
                problems += "${bundledSpi.size} io/trino/spi/** entries are bundled (e.g. ${bundledSpi.first()}) — " +
                    "trino-spi must stay compileOnly"
            }

            // 5. Service-file merge, graph-derived: the shaded provider set for every
            //    descriptor must be a SUPERSET of the union across its source jars.
            //    Shadow's `duplicatesStrategy` default is EXCLUDE and takes precedence
            //    over transforming, so `mergeServiceFiles()` ALONE silently degrades to
            //    first-wins for exactly the multi-source paths that need merging.
            val multiSourcePaths = serviceSources.filterValues { it.size > 1 }.keys.sorted()
            require(multiSourcePaths.isNotEmpty()) {
                "no META-INF/services descriptor has more than one source jar on the resolved " +
                    "runtime classpath — the merge assertion would prove nothing, so the graph has " +
                    "changed shape and this oracle needs re-deriving (#2869)"
            }
            serviceUnion.forEach { (path, expectedProviders) ->
                val actual = read(path)?.let { providerLines(it) }
                if (actual == null) {
                    problems += "service descriptor $path is ABSENT from the shaded jar; provided by " +
                        "${serviceSources[path]}"
                } else {
                    val missing = expectedProviders - actual
                    if (missing.isNotEmpty()) {
                        problems += "service descriptor $path lost ${missing.size} provider(s) $missing — " +
                            "sources ${serviceSources[path]}; the merge degraded to first-wins"
                    }
                }
            }

            // 6. Netty pin, graph-derived. The tcnative native-binding train is
            //    versioned independently of netty's 4.x line, exactly as
            //    verifyPublishedPomNettyPin partitions it.
            val (tcnative, nettyCore) = resolvedNetty.entries.partition { it.key.startsWith("netty-tcnative") }
            require(nettyCore.isNotEmpty()) {
                "no io.netty core module resolved on runtimeClasspath — the netty pin assertion " +
                    "would be vacuous (#2869)"
            }
            val nettyVersionsKeys = read(nettyVersionsPath)
                ?.let { bytes -> providerLines(bytes).mapNotNull { it.substringBefore('=').trim().ifEmpty { null } } }
                ?.toSet()
            if (nettyVersionsKeys == null) {
                problems += "$nettyVersionsPath is ABSENT from the shaded jar — netty's own version " +
                    "attestation is how a runtime reports which modules it has"
            }
            nettyCore.forEach { (module, resolvedVersion) ->
                if (resolvedVersion != expectedNettyVersion) {
                    problems += "resolved io.netty:$module at $resolvedVersion, expected $expectedNettyVersion"
                }
                val attestation = "META-INF/maven/io.netty/$module/pom.properties"
                when (val attested = read(attestation)?.let { propertyVersion(it) }) {
                    null -> problems += "io.netty:$module resolves onto the runtime classpath but is NOT " +
                        "attested at $attestation inside the shaded jar"
                    expectedNettyVersion -> {}
                    else -> problems += "$attestation attests version $attested, expected $expectedNettyVersion"
                }
                if (nettyVersionsKeys != null && nettyVersionsKeys.none { it.startsWith("$module.") }) {
                    problems += "$nettyVersionsPath carries no line for resolved module $module — the " +
                        "per-jar copies were not appended, so the attestation is one jar's only"
                }
            }
            tcnative.forEach { (module, resolvedVersion) ->
                if (resolvedVersion != expectedTcnativeVersion) {
                    problems += "resolved io.netty:$module at $resolvedVersion, expected tcnative train " +
                        expectedTcnativeVersion
                }
            }

            // 7. Arrow present and pinned. FlightClient is the class the connector's
            //    hot path actually loads, so its presence is the wiring evidence.
            val flightClient = "org/apache/arrow/flight/FlightClient.class"
            if (flightClient !in entries) {
                problems += "$flightClient is ABSENT — the shaded jar cannot talk Arrow Flight"
            }
            require(resolvedArrow.isNotEmpty()) {
                "no org.apache.arrow module resolved on runtimeClasspath — the arrow pin assertion " +
                    "would be vacuous (#2869)"
            }
            resolvedArrow.forEach { (module, resolvedVersion) ->
                if (resolvedVersion != expectedArrowVersion) {
                    problems += "resolved org.apache.arrow:$module at $resolvedVersion, expected $expectedArrowVersion"
                }
                val attestation = "META-INF/maven/org.apache.arrow/$module/pom.properties"
                when (val attested = read(attestation)?.let { propertyVersion(it) }) {
                    null -> problems += "org.apache.arrow:$module resolves onto the runtime classpath but is " +
                        "NOT attested at $attestation inside the shaded jar"
                    expectedArrowVersion -> {}
                    else -> problems += "$attestation attests version $attested, expected $expectedArrowVersion"
                }
            }
        }

        // 8. Shadow's DEFAULT exclusions, ASSERTED rather than re-declared in the task
        //    config. A re-declaration that drifts from the default is how a future
        //    shadow bump silently regresses; asserting the OUTCOME survives the bump.
        val signatureFiles = entries.filter {
            it.startsWith("META-INF/") && (it.endsWith(".SF") || it.endsWith(".DSA") || it.endsWith(".RSA"))
        }
        val indexLists = entries.filter { it == "META-INF/INDEX.LIST" }
        val moduleInfos = entries.filter { it == "module-info.class" || it.endsWith("/module-info.class") }
        problems += signatureFiles.map { "jar signature file $it survived — it invalidates the shaded jar's seal" }
        problems += indexLists.map { "$it survived — a stale index breaks classloading of merged content" }
        problems += moduleInfos.map { "$it survived — a shaded jar must not claim to be a named module" }

        // 9. No duplicate entry NAMES in the local-header census. `failOnDuplicateEntries`
        //    on shadowJar covers the same property; this re-reads the finished artifact so
        //    a future config change cannot turn the guarantee off unnoticed.
        val duplicates = entryNames.groupingBy { it }.eachCount().filterValues { it > 1 }
        problems += duplicates.entries.map { (name, count) -> "entry $name appears $count times in the shaded jar" }

        // 10. Per-artifact contribution census: every probeable runtime jar put its
        //     unique probe entry into the shaded jar. This proves "the whole runtime tree
        //     is bundled" FROM THE GRAPH, rather than from a magic entry-count floor that
        //     drifts on every dependency bump.
        val missingContributors = probes.filterValues { it !in entries }
        problems += missingContributors.entries.map { (label, probe) ->
            "resolved runtime artifact $label contributed nothing: its unique probe entry $probe is absent"
        }
        val contributed = probes.size - missingContributors.size

        require(problems.isEmpty()) {
            "shaded jar contract violations (#2869), ${problems.size} problem(s):\n  " +
                problems.joinToString("\n  ")
        }

        // Affirmative census — a clean run must not read like an unmeasured one, so every
        // zero is reported as an explicit RECOGNISED count.
        val providerLineTotal = serviceUnion.values.sumOf { it.size }
        val (tcnativeCount, nettyCoreCount) = resolvedNetty.keys
            .partition { it.startsWith("netty-tcnative") }
            .let { (t, c) -> t.size to c.size }
        val indistinguishableNote =
            if (indistinguishable.isEmpty()) {
                "0 INDISTINGUISHABLE RECOGNISED"
            } else {
                "${indistinguishable.size} INDISTINGUISHABLE (content-free placeholders, unprobeable): " +
                    indistinguishable.sorted().joinToString(", ")
            }
        logger.lifecycle(
            "verifyFatJar: ${jar.name} (${jar.length() / (1024 * 1024)} MiB) — " +
                "${entryNames.size} ENTRIES EXAMINED; " +
                "${artifacts.size} RUNTIME ARTIFACTS RESOLVED, ${probes.size} PROBED, " +
                "$contributed CONTRIBUTED, $indistinguishableNote; " +
                "${serviceUnion.size} SERVICE DESCRIPTORS EXAMINED across " +
                "${serviceUnion.keys.count { serviceSources[it]!!.size > 1 }} MULTI-SOURCE PATH(S) " +
                "carrying $providerLineTotal PROVIDER LINES RECOGNISED; " +
                "$nettyCoreCount NETTY CORE MODULES ATTESTED at $expectedNettyVersion, " +
                "$tcnativeCount TCNATIVE ARTIFACTS at $expectedTcnativeVersion; " +
                "${resolvedArrow.size} ARROW MODULES ATTESTED at $expectedArrowVersion; " +
                "${signatureFiles.size} SIGNATURE FILES RECOGNISED, " +
                "${indexLists.size} INDEX.LIST RECOGNISED, " +
                "${moduleInfos.size} MODULE-INFO RECOGNISED, " +
                "${duplicates.size} DUPLICATE ENTRY NAMES RECOGNISED (#2869)",
        )
    }
}

tasks.named("check") { dependsOn(verifyFatJar) }

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
