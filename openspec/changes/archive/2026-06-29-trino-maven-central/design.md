# Design — Publish the Trino connector to Maven Central

## Context

`trino-connector/` is a Gradle (Kotlin DSL) Java 25 project, `rootProject.name = "cqlite-trino"`,
currently `group = "com.rustyrazorblade.cqlite"`, `version = "0.11.0"`. It builds a Trino plugin
loaded via the SPI service descriptor `META-INF/services/io.trino.spi.Plugin`. `trino-spi` is
`compileOnly` (the engine provides it at runtime). The owner has registered + DNS-verified the
`in.mcfad` namespace on the Central Portal; the original contributor (Jon Haddad) approved moving
the Java packages to that namespace.

The Rust agent gate (`scripts/agent-gate.sh`) does **not** build this Java project, so the
verification surface for this change is the Gradle build, not the Rust gate. This is stated
explicitly so the done-bar is honest.

## Decision 1 — Publishing mechanism: vanniktech `com.vanniktech.maven.publish`

**Chosen:** the `com.vanniktech.maven.publish` Gradle plugin (Central Portal mode).

**What it beats:**
- *Raw `maven-publish` + `signing` + manual Central Portal upload.* Workable, but you hand-wire
  the sources/javadoc jars, the signing wiring, and the Central Portal REST upload yourself.
- *`nexus-staging` / OSSRH (`oss.sonatype.org`).* Sunset in 2025 — not an option for a new
  namespace; new namespaces only publish through the Central Portal.

vanniktech has first-class Central Portal support: it auto-generates the sources + javadoc jars,
wires GPG signing from in-memory keys, validates the required POM metadata, and exposes
`publishToMavenCentral` (and `publishAndReleaseToMavenCentral` for auto-release). It also supports
`publishToMavenLocal` for the secret-free local verification this change relies on.

**Fallback if the plugin is undesirable:** raw `maven-publish` + `signing` with
`withSourcesJar()`/`withJavadocJar()` and the Central Portal publisher API. The *spec* is written
against observable outputs (coordinates, POM contents, signed artifacts, gating) so it holds under
either mechanism; the task list assumes vanniktech and notes the raw alternative.

## Decision 2 — Verification without secrets (the de-facto gate for this change)

Two secret-free Gradle checks stand in for the Rust gate and provide wiring evidence:

1. **Rename E2E:** `./gradlew test installPlugin` must stay green after the package move, and
   `build/plugin/cqlite_flight/` must contain the connector jar whose SPI descriptor now names
   `in.mcfad.cqlite.flight.CqliteFlightPlugin`. This proves the rename is internally consistent
   (no dangling `com.rustyrazorblade` references, SPI resolves).
2. **Publication E2E:** `./gradlew publishToMavenLocal -Pversion=0.13.0` (or the tag-derived
   equivalent) must produce, in the local repo, `in/mcfad/cqlite-trino/0.13.0/` containing the
   main jar, `-sources.jar`, `-javadoc.jar`, and a POM with the required metadata and **no**
   `trino-spi` runtime dependency. Signing is exercised when a key is configured and skipped
   (without failing the local check) when it is not — so this runs in CI/dev without the real key.

A throwaway/test GPG key MAY be used to additionally assert that `.asc` signatures are produced;
this is optional and must never use or require the real `SIGNING_KEY`.

## Decision 3 — Secret-gated tag trigger that cannot break existing releases

The publish runs on `push: tags: ['v*']`, the same trigger family as the existing release / flight
image workflows. The hazard: a publish step that errors when secrets are absent would turn a tag
push into a failed release for any tag pushed before the secrets land.

**Mitigation:** a guard step reads whether the required secrets are non-empty and exposes a boolean
output; the actual `publishToMavenCentral` step (and any steps that import the key) run only
`if:` that output is true. When the secrets are absent the job logs a loud
`::notice::` ("Maven Central secrets absent — skipping connector publish") and succeeds. Secrets
are not addressable in a job-level `if:` directly, hence the guard-step-to-output indirection.

**Placement:** a dedicated `.github/workflows/trino-publish.yml` (clean isolation, independently
gated, easy to reason about) is preferred over a job grafted into `release.yml`. Either satisfies
the spec; the dedicated workflow is the recommendation.

## Decision 4 — Version derivation

At publish time the Gradle `version` derives from the tag: CI passes `-Pversion=${GITHUB_REF_NAME#v}`
(`v0.13.0` → `0.13.0`); `build.gradle.kts` reads the `version` project property and falls back to a
local dev version (e.g. the existing literal or `0.0.0-SNAPSHOT`) when the property is absent, so
local builds still work. This keeps the connector in lockstep with the crate + container release on
each `v*` tag without a second source of truth.

## Decision 5 — POM scope hygiene

`trino-spi` stays `compileOnly` and MUST NOT appear in the published POM's runtime dependencies
(the engine supplies it; bundling/declaring it would clash with the host classpath). The
implementation runtime deps (`flight-core`, `jackson-databind`) DO belong in the POM. The
publication check asserts this by inspecting the generated POM.

## Risks / notes

- A Trino plugin is consumed as a *directory of jars*, not a single artifact. Publishing to Central
  makes the connector reproducibly consumable, but a consumer still assembles the plugin dir
  (mirroring `installPlugin`). The README must say so to avoid a "drop one jar in" misconception.
- Javadoc on a Java 25 toolchain can be strict; the javadoc jar must build cleanly in CI (relax
  doclint if needed) since Central requires it.
- Auto-release vs. manual release on the Portal: default to staged + auto-release
  (`publishAndReleaseToMavenCentral`) only once a first manual release has proven the pipeline;
  the spec does not mandate auto-release, leaving it a low-risk follow-up toggle.
