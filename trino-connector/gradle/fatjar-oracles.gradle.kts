// --- Shaded-jar verification oracles (issue #2869) ---------------------------
//
// Split out of build.gradle.kts by RESPONSIBILITY under the campsite rule: that
// file had reached 884 lines against the repo's ~800 source target, and these two
// tasks are VERIFICATION, not build configuration. What stays over there is the
// configuration half — the `shadow { }` block, `shadowJar`'s transformers and
// duplicates strategy, the no-relocation rationale, and `installPluginFat`.
// `verifyPublishedPomNettyPin` (#2300) also stays: it is pre-existing and predates
// this work.
//
// Applied with `apply(from = ...)`, which compiles this file SEPARATELY from
// build.gradle.kts. Two consequences are load-bearing here:
//
//  1. NO TYPE-SAFE ACCESSORS. `configurations.runtimeClasspath` and
//     `tasks.shadowJar` are generated accessors that do not exist across this
//     boundary, so every lookup below is explicit — `configurations.named(...)`,
//     and `dependsOn("shadowJar")` by name. Task REGISTRATION and the `check`
//     wiring are unaffected: both tasks are registered on the same Project, so
//     `./gradlew verifyFatJar verifyShadowNotPublished` resolves them by name
//     exactly as before (Unit D's release workflow and the CI lane call them that
//     way), and `tasks.named("check") { dependsOn(...) }` still gates them.
//
//  2. build.gradle.kts's `val nettyVersion` / `nettyTcnativeVersion` /
//     `arrowVersion` LOCALS ARE NOT VISIBLE here. They are passed deliberately
//     through `extra` rather than re-typed as literals: two copies of a pinned
//     version is precisely the drift `verifyPublishedPomNettyPin` exists to catch,
//     and a second copy here would let this oracle certify a pin the build no
//     longer uses. A missing extra throws — it cannot silently default.
//
// Imported rather than written fully-qualified for the same reason as in
// build.gradle.kts: keep the `java.*` package reachable regardless of whether a
// `java` extension accessor is in scope.
import java.io.ByteArrayInputStream
import java.util.Properties
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

// The pins under test, sourced from build.gradle.kts's single declaration site.
val pinnedNettyVersion = extra["cqliteNettyVersion"] as String
val pinnedTcnativeVersion = extra["cqliteNettyTcnativeVersion"] as String
val pinnedArrowVersion = extra["cqliteArrowVersion"] as String

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
    val expectedNettyVersion = pinnedNettyVersion
    val expectedTcnativeVersion = pinnedTcnativeVersion
    val expectedArrowVersion = pinnedArrowVersion
    // The single service entry this plugin owns. Trino's PluginManager reads it at
    // startup; a `checkState` there is the ONLY runtime signal that the jar is a
    // plugin at all, and it passes even when every OTHER service descriptor has
    // silently degraded to first-wins — see the merge note on `shadowJar`.
    val expectedPluginClass = "in.mcfad.cqlite.flight.CqliteFlightPlugin"
    val nettyVersionsPath = "META-INF/io.netty.versions.properties"
    dependsOn("shadowJar")
    // Captured lazily at configuration time; the artifact set resolves at execution.
    val runtimeArtifacts = configurations.named("runtimeClasspath").get().incoming.artifacts
    inputs.files(configurations.named("runtimeClasspath"))
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
        // Exactly ONE artifact in today's graph has no such probe, and it is genuinely
        // content-free — MEASURED, not assumed: `io.grpc:grpc-context:1.79.0` holds only
        // META-INF/MANIFEST.MF (its API moved into grpc-api). Two other placeholders
        // (guava's `listenablefuture:9999.0-empty-to-avoid-conflict-with-guava`, and the
        // classifier-less `netty-tcnative-boringssl-static` marker jar whose natives live
        // in per-platform CLASSIFIED jars this graph never resolves) ARE probed, via
        // their uniquely-named META-INF/maven pom.properties — which is the point of
        // deriving eligibility rather than allowlisting: the rule found coverage a
        // hand-written waiver list would have thrown away.
        // A jar with no probe is reported BY NAME in the census as INDISTINGUISHABLE, not
        // waived silently: every one of its entries is either shadow-stripped or
        // byte-shared with a sibling, so nothing a probe can see distinguishes its
        // contribution. That is a measurement, not an exception.
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

// --- Publication-purity oracle (issue #2869) ---------------------------------
// The owner's hard requirement is that adding shadow leaves the Maven Central
// publication BYTE-IDENTICAL. The `shadow { addShadowVariantIntoJavaComponent =
// false }` switch above is the mechanism; this is the assertion, and it reads the
// GENERATED metadata rather than the build script, so it cannot pass by echoing
// its own config. Central publishes the THIN jar only — there is deliberately no
// `:all` classifier there, because the shaded jar is a GitHub Release asset with
// its own lifecycle, not a library coordinate.
val verifyShadowNotPublished by tasks.registering {
    description = "Assert the shaded jar/variant does NOT leak into the Maven Central publication (#2869)."
    group = "verification"
    dependsOn("generateMetadataFileForMavenPublication", "generatePomFileForMavenPublication")
    val moduleFile = layout.buildDirectory.file("publications/maven/module.json")
    val pomFile = layout.buildDirectory.file("publications/maven/pom-default.xml")
    doLast {
        val module = moduleFile.get().asFile
        require(module.isFile) { "generated Gradle module metadata not found at $module" }
        val pom = pomFile.get().asFile
        require(pom.isFile) { "generated POM not found at $pom" }

        // module.json is Gradle-authored JSON. Rather than add a JSON dependency to a
        // build script that has none, read the facts that matter with bounded regexes,
        // keyed on the ONE structural cue that distinguishes a variant record from any
        // other `"name"` field in the document (a dependency's, or a file's): a variant
        // is `"name": "<v>"` immediately followed by its `"attributes"` object. If a
        // future Gradle reorders those keys this parse yields zero variants and the
        // require below FAILs — the check refuses rather than passing vacuously.
        val text = module.readText()
        val variantNames = Regex("\"name\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*\"attributes\"")
            .findAll(text)
            .map { it.groupValues[1] }
            .toList()
        require(variantNames.isNotEmpty()) {
            "no variant records parsed out of $module — the assertions below would be vacuous, so " +
                "either the publication produced no variants or the metadata shape changed (#2869)"
        }

        val problems = mutableListOf<String>()
        variantNames.filter { it.contains("shadow", ignoreCase = true) }.forEach {
            problems += "published variant \"$it\" is a shadow variant — set " +
                "`shadow { addShadowVariantIntoJavaComponent = false }`; Central publishes the thin jar only"
        }
        // The attribute a Gradle consumer's variant selection actually keys on. Shadow
        // stamps its variant `shadowed`; every thin variant is `external`. Checked
        // independently of the variant NAME so a shadow rename cannot slip past.
        val bundlings = Regex("\"org\\.gradle\\.dependency\\.bundling\"\\s*:\\s*\"([^\"]+)\"")
            .findAll(text)
            .map { it.groupValues[1] }
            .toList()
        require(bundlings.isNotEmpty()) {
            "no org.gradle.dependency.bundling attribute found in $module — the shadowed-bundling " +
                "assertion would be vacuous (#2869)"
        }
        bundlings.filter { it != "external" }.distinct().forEach {
            problems += "published variant declares org.gradle.dependency.bundling=\"$it\", expected " +
                "\"external\" on every variant — a shaded/embedded variant is leaking into Central"
        }
        // Any `-all.jar` file entry is the shaded jar itself leaking.
        Regex("\"name\"\\s*:\\s*\"([^\"]*-all\\.jar)\"").findAll(text).forEach {
            problems += "published module metadata lists shaded artifact ${it.groupValues[1]}"
        }
        if (pom.readText().contains("-all.jar")) {
            problems += "published POM $pom references a -all.jar artifact"
        }
        require(problems.isEmpty()) {
            "shaded jar leaked into the Maven Central publication (#2869):\n  " + problems.joinToString("\n  ")
        }

        logger.lifecycle(
            "verifyShadowNotPublished: ${variantNames.size} PUBLISHED VARIANTS EXAMINED " +
                "(${variantNames.joinToString(", ")}); ${bundlings.size} BUNDLING ATTRIBUTES EXAMINED, " +
                "all \"external\"; 0 SHADOW VARIANTS RECOGNISED, 0 SHADED ARTIFACT REFERENCES " +
                "RECOGNISED in module.json or pom-default.xml (#2869)",
        )
    }
}

tasks.named("check") { dependsOn(verifyShadowNotPublished) }
