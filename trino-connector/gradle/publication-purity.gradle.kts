// --- Maven Central publication-purity oracles (issue #2869) ------------------
//
// Split out of gradle/fatjar-oracles.gradle.kts, which had reached 699 of the
// repo's ~800-line source target with a fourth task arriving. The responsibility
// boundary is clean: that file is about the SHADED JAR's contents and the plugin
// directory; this one is about what reaches MAVEN CENTRAL. Central is IMMUTABLE,
// so everything here is a pre-flight for an irreversible action.
//
// Two tasks, deliberately layered — a name-and-variant check and an artifact-SET
// check — because the first cannot be made complete and the second can:
//
//   verifyShadowNotPublished    reads the GENERATED metadata and refuses a shadow
//                               variant, a non-`external` bundling attribute, and
//                               any `-all.jar` reference. Cheap, no publish
//                               prerequisites, so it stays safe to run on the
//                               release pre-flight path.
//   verifyPublishedArtifactSet  publishes into an ISOLATED file repository and
//                               asserts the EXACT SET of artifacts produced.
//
// Why both, and why the set check exists (finding F): the first keys on NAMES.
// A shaded jar attached with a non-standard classifier — `-shaded`, `-fat`,
// `-uber`, or a spelling nobody has thought of — matches none of its three checks,
// rides an existing `external` variant, and reaches Central. Extending the name
// matcher is the carve-it-again trap (#3229): each new spelling only moves the
// argument to the next one. An artifact-SET assertion is DECIDABLE instead —
// enumerate what the publication actually produced, compare against the expected
// set, and refuse anything else — which closes every classifier at once.
//
// Imported rather than fully-qualified: see the note in fatjar-oracles.gradle.kts
// about the `java` extension accessor shadowing the root `java` package.
import java.io.File

// An ISOLATED file repository that exists only for `verifyPublishedArtifactSet`.
// It ADDS a repository; it does not touch `mavenLocal`, the real `~/.m2`, or the
// Central target, so `publishToMavenLocal` and `publishToMavenCentral` behave
// exactly as before. Nothing in CI runs the aggregate `publish` task (verified:
// the only gradlew invocations across all workflows are `test`,
// `verifyPublishedPomNettyPin`, the two `generate*` tasks,
// `verifyShadowNotPublished`, `shadowJar`, `verifyFatJar`, `installPlugin`,
// `installPluginFat`, `verifyInstallPluginFatPrunesStale`, `publishToMavenLocal`
// and `publishToMavenCentral`), so the extra publish task this creates is reachable
// only by name.
val publicationAuditRepoDir = layout.buildDirectory.dir("publication-audit-repo")

// `publishing { }` is a TYPE-SAFE ACCESSOR and does not exist across an
// `apply(from = ...)` boundary (same class of edge as `configurations.runtimeClasspath`
// and `tasks.shadowJar` — see the note in fatjar-oracles.gradle.kts). Configure the
// extension explicitly instead.
extensions.configure<org.gradle.api.publish.PublishingExtension> {
    repositories.maven {
        name = "publicationAudit"
        url = uri(publicationAuditRepoDir)
    }
}

// Referenced BY NAME only. `maven-publish` synthesises
// `publish<Pub>PublicationTo<Repo>Repository` after the project is evaluated, so a
// `tasks.named(...)` lookup at script-evaluation time throws UnknownTaskException;
// `dependsOn(String)` resolves lazily at task-graph time and does not.
val publishToPublicationAudit = "publishMavenPublicationToPublicationAuditRepository"

// The artifact set a Central release is supposed to consist of. `coordinates(...)`
// in build.gradle.kts fixes the artifactId, and vanniktech's
// `JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = true)` fixes which
// jars exist; `.module` is Gradle module metadata, which maven-publish emits
// alongside the POM. Anything NOT in this set is a finding — that is the whole
// point, so do not extend it to accommodate a new artifact without deciding that
// the new artifact belongs on Central.
val verifyPublishedArtifactSet by tasks.registering {
    description =
        "Publish to an isolated repo and assert the EXACT artifact set reaching Central (#2869 finding F)."
    group = "verification"
    dependsOn(publishToPublicationAudit)
    val repoDir = publicationAuditRepoDir
    val artifactId = "cqlite-trino"
    val publishedVersion = version.toString()
    doLast {
        val versionDir = File(repoDir.get().asFile, "in/mcfad/$artifactId/$publishedVersion")
        // An unmeasurable set is a REFUSAL, not a pass.
        require(versionDir.isDirectory) {
            "publication audit repo has no directory at $versionDir — the artifact set could not be " +
                "measured, so this check cannot pass (#2869)"
        }
        val allFiles = (versionDir.listFiles() ?: emptyArray()).filter { it.isFile }.map { it.name }
        require(allFiles.isNotEmpty()) {
            "publication audit repo directory $versionDir is EMPTY — the artifact set could not be " +
                "measured, so this check cannot pass (#2869)"
        }
        // Checksums and signatures are per-artifact side files, not artifacts. Strip
        // them to get the set of PRIMARY artifacts, and refuse a suffix we do not
        // recognise rather than silently treating it as primary.
        val sideCarSuffixes = listOf(".md5", ".sha1", ".sha256", ".sha512", ".asc")
        val primaries = allFiles.filter { name -> sideCarSuffixes.none { name.endsWith(it) } }.toSortedSet()
        val expected = sortedSetOf(
            "$artifactId-$publishedVersion.jar",
            "$artifactId-$publishedVersion-sources.jar",
            "$artifactId-$publishedVersion-javadoc.jar",
            "$artifactId-$publishedVersion.pom",
            "$artifactId-$publishedVersion.module",
        )
        val unexpected = primaries - expected
        val missing = expected - primaries
        val problems = mutableListOf<String>()
        unexpected.forEach {
            problems += "UNEXPECTED artifact $it would reach Maven Central. Central is IMMUTABLE. If this " +
                "is a shaded/uber jar under any classifier, it must NOT be published — the fat jar is a " +
                "GitHub Release asset. If it genuinely belongs on Central, add it to the expected set " +
                "deliberately"
        }
        missing.forEach {
            problems += "MISSING artifact $it — the publication no longer produces an artifact Central " +
                "requires (main jar, sources, javadoc, POM and module metadata)"
        }
        require(problems.isEmpty()) {
            "published artifact set is wrong (#2869), ${problems.size} problem(s):\n  " +
                problems.joinToString("\n  ")
        }
        logger.lifecycle(
            "verifyPublishedArtifactSet: ${primaries.size} ARTIFACTS EXAMINED in $versionDir " +
                "(${primaries.joinToString(", ")}); ${allFiles.size - primaries.size} CHECKSUM/SIGNATURE " +
                "SIDE FILES RECOGNISED; 0 UNEXPECTED RECOGNISED, 0 MISSING RECOGNISED (#2869)",
        )
    }
}

// Publish into a FRESH directory: a leftover artifact from an earlier run at the SAME
// version would otherwise be reported as unexpected, turning a stale working directory
// into a spurious failure. Done in `whenReady` (which fires after configuration and
// before any execution) and ONLY when this verification task is in the graph, so no
// other caller's publish is affected. The audit repo is disposable by definition.
gradle.taskGraph.whenReady {
    if (hasTask(verifyPublishedArtifactSet.get())) {
        publicationAuditRepoDir.get().asFile.deleteRecursively()
    }
}

tasks.named("check") { dependsOn(verifyPublishedArtifactSet) }

// --- Metadata-level shadow-leak oracle (issue #2869) -------------------------
// The owner's hard requirement is that adding shadow leaves the Maven Central
// publication BYTE-IDENTICAL. `shadow { addShadowVariantIntoJavaComponent = false }`
// in build.gradle.kts is the mechanism; this is the assertion, and it reads the
// GENERATED metadata rather than the build script, so it cannot pass by echoing its
// own config. Central publishes the THIN jar only — there is deliberately no `:all`
// classifier there, because the shaded jar is a GitHub Release asset with its own
// lifecycle, not a library coordinate.
//
// SCOPE, STATED EXACTLY (finding F). This task decides THREE things and no more:
// no variant whose name contains "shadow"; no variant whose
// `org.gradle.dependency.bundling` is anything but `external`; and no `-all.jar`
// reference in either module.json or the POM. That covers the PLAUSIBLE-ACCIDENT
// path — flipping `addShadowVariantIntoJavaComponent` back on, which the positive
// control below really does catch — but it is keyed on NAMES, so it does NOT decide
// the general question. A shaded jar attached under a different classifier
// (`-shaded`, `-fat`, …) matches none of these three and would pass here.
// `verifyPublishedArtifactSet` above is what closes that, by asserting the artifact
// SET instead of matching names. Do not read a green here as "nothing shaded can
// reach Central"; that claim belongs to the set check.
//
// This task deliberately keeps NO publish prerequisites — only the two `generate*`
// tasks — so it stays cheap enough to sit on the release pre-flight path
// (trino-publish.yml, trino-connector-fatjar.yml) without pulling javadoc
// generation or signing into it.
val verifyShadowNotPublished by tasks.registering {
    description = "Assert no shadow VARIANT and no -all.jar reference in the published metadata (#2869)."
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
