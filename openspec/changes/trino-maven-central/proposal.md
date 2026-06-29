## Why

The Java Trino connector (`trino-connector/`) is currently only buildable from source and
hand-pinned at `0.11.0` under the placeholder group `com.rustyrazorblade.cqlite`. There is no
released, signed, consumable artifact, so a Trino operator cannot pull the connector by Maven
coordinates and it drifts out of lockstep with the crate + container releases.

This change makes the connector a **first-class published artifact**: a branded, domain-verified
namespace (`in.mcfad`), a Gradle publishing configuration that produces a Central-compliant
signed deployment (sources + javadoc + POM), a version derived from the release tag so the
connector ships in lockstep with `v*` releases, and a tag-triggered, **secret-gated** workflow
that publishes to Maven Central without breaking the existing tag-release path.

- **Milestone:** maintenance / release infrastructure. **Design-driven** (release/CI process —
  there is no Cassandra format oracle here).
- Adds a new `trino-connector-release` capability.
- Decisions are already resolved on issue #1101 (groupId `in.mcfad`; artifactId `cqlite-trino`;
  package rename `com.rustyrazorblade.cqlite.*` → `in.mcfad.cqlite.*`, approved by the original
  contributor Jon Haddad on 2026-06-28; tag-derived version). This change formalizes them as a
  verifiable spec.

## What Changes

- **Java namespace rename** `com.rustyrazorblade.cqlite.*` → `in.mcfad.cqlite.*` across all 38
  `.java` files (package + import declarations), the physical source directories
  (`src/{main,test}/java/com/rustyrazorblade/cqlite/...` → `.../in/mcfad/cqlite/...`), and the
  Trino SPI service descriptor (`src/main/resources/META-INF/services/io.trino.spi.Plugin`).
- **Gradle publishing config** in `trino-connector/build.gradle.kts`: `group = "in.mcfad"`,
  a `maven-publish`-based publication with sources + javadoc jars, GPG signing of all artifacts
  using in-memory keys from CI secrets, and a complete POM (name, description, url, license,
  developers, SCM). The provided-by-engine `trino-spi` dependency stays `compileOnly` and out of
  the published POM's runtime dependencies.
- **Tag-derived version**: at publish time the Gradle `version` derives from the release tag
  (`v0.13.0` → `0.13.0`) rather than the hand-pinned literal.
- **A tag-triggered, secret-gated release workflow** (on `push: tags: ['v*']`) that runs the
  Gradle publish to Maven Central. The publish step is gated on the publishing/signing secrets
  being present, so an untagged repo or a tag pushed before the secrets exist does **not** fail
  the release run — it skips cleanly with a loud notice.
- **README** documents the published coordinates (`in.mcfad:cqlite-trino:<version>`) and how to
  assemble the Trino plugin directory from the published artifact.

## Capabilities

### Added Capabilities
- `trino-connector-release`: namespace rename, Central-compliant publication + POM, tag-derived
  version, secret-gated tag-triggered publish workflow, and consumer documentation.

## Impact

- **Renamed/moved:** all of `trino-connector/src/{main,test}/java/com/rustyrazorblade/...` →
  `.../in/mcfad/...` (38 files) + the SPI descriptor.
- **Modified:** `trino-connector/build.gradle.kts` (group, publishing, signing, version),
  `trino-connector/README.md`.
- **New:** a release workflow (`.github/workflows/trino-publish.yml`, or a guarded job in an
  existing release workflow).
- **No Rust / cqlite-core / binding code changes.** `scripts/agent-gate.sh` (the Rust gate) does
  not cover the Java connector; verification for this change is the Gradle build —
  `./gradlew test installPlugin` (rename E2E) and `./gradlew publishToMavenLocal` (publication +
  POM + naming, signing exercised when a key is present).
- **No-heuristics mandate / memory budget / public binding surfaces:** unaffected.

## Non-goals

- **Owner prerequisites are out of scope for this change.** Registering the `in.mcfad` namespace
  (✅ already done + DNS-verified) and adding the CI secrets (`MAVEN_CENTRAL_USERNAME/PASSWORD`,
  `SIGNING_KEY`, `SIGNING_PASSWORD` + publishing the public key to a keyserver) are the owner's
  actions; this change builds and reviews the implementation that consumes them.
- **No live Maven Central deployment is performed or asserted by this change.** The actual
  publish fires on a future `v*` tag once the secrets exist; this change verifies the publication
  is correctly configured (assembles, names, and signs locally), not that Central accepted it.
- **No change to the `cqlite-flight` gRPC container image** (`ghcr.io/pmcfadin/cqlite-flight`) —
  that is a separate artifact on its own path.
- **No change to the crate/Python/Node release flows.**
- **No auto-publish of the connector outside a `v*` tag** (no publish on every push/PR).
