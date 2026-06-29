# trino-connector-release Specification

## Purpose
TBD - created by archiving change trino-maven-central. Update Purpose after archive.
## Requirements
### Requirement: Java namespace renamed to the published groupId
The Trino connector's Java sources SHALL live under the `in.mcfad.cqlite` package root, matching
the published groupId. No `com.rustyrazorblade` package or import declaration SHALL remain in
`trino-connector/src`, the physical source directories SHALL reflect the new package path, and the
Trino SPI service descriptor SHALL name the relocated plugin class.

#### Scenario: No legacy package references remain
- **WHEN** the connector sources are searched after the rename
- **THEN** no `.java` file under `trino-connector/src` contains a `com.rustyrazorblade` package or import declaration
- **AND** the source files reside under `src/{main,test}/java/in/mcfad/cqlite/...`, not under `.../com/rustyrazorblade/...`

#### Scenario: SPI descriptor names the relocated plugin
- **WHEN** `src/main/resources/META-INF/services/io.trino.spi.Plugin` is read
- **THEN** it names `in.mcfad.cqlite.flight.CqliteFlightPlugin`
- **AND** `./gradlew test installPlugin` is green and `build/plugin/cqlite_flight/` contains the connector jar

### Requirement: Connector publishes Central-compliant coordinates and POM
The build SHALL define a Maven publication with groupId `in.mcfad`, artifactId `cqlite-trino`, that
produces a main jar, a sources jar, and a javadoc jar, and a POM carrying the Central-required
metadata (name, description, url, license, developers, SCM). The provided-by-engine `trino-spi`
dependency SHALL remain `compileOnly` and SHALL NOT appear among the published POM's runtime
dependencies.

#### Scenario: Local publish produces the expected coordinates and artifacts
- **WHEN** `./gradlew publishToMavenLocal -Pversion=0.13.0` runs
- **THEN** the local repository contains `in/mcfad/cqlite-trino/0.13.0/` with the main jar, a `-sources.jar`, and a `-javadoc.jar`
- **AND** the generated POM declares name, description, url, a license, at least one developer, and an SCM connection

#### Scenario: trino-spi is excluded from published runtime dependencies
- **WHEN** the generated POM's dependencies are inspected
- **THEN** `io.trino:trino-spi` is absent from the runtime/compile scope of the POM
- **AND** the implementation runtime dependencies (`flight-core`, `jackson-databind`) are present

#### Scenario: Artifacts are signed when a signing key is configured
- **WHEN** a publish runs with a GPG signing key configured (via in-memory key properties)
- **THEN** a `.asc` detached signature is produced for each published artifact (jar, sources, javadoc, POM)
- **AND** when no signing key is configured the local verification build still succeeds without producing signatures

### Requirement: Published version derives from the release tag
At publish time the connector version SHALL derive from the release tag (`v0.13.0` → `0.13.0`) so
the connector releases in lockstep with the crate and container artifacts, while local builds
without the tag property still succeed with a fallback version.

#### Scenario: Tag-derived version drives the artifact coordinates
- **WHEN** the build runs with the version project property set from the tag (e.g. `-Pversion=0.13.0`)
- **THEN** the produced artifacts are versioned `0.13.0` (not the hand-pinned literal)
- **AND** a build with no version property supplied still resolves to a defined fallback version rather than failing

### Requirement: Tag-triggered, secret-gated publish workflow
A CI workflow SHALL publish the connector to Maven Central on `v*` tag pushes, deriving the version
from the tag. The publish SHALL be gated on the publishing and signing secrets being present, so a
tag pushed when the secrets are absent SHALL NOT fail the release run — it SHALL skip the publish
with a visible notice. The workflow SHALL NOT publish on non-tag events (pushes/PRs).

#### Scenario: Tag push with secrets present publishes
- **WHEN** a `v*` tag is pushed and the Maven Central + signing secrets are present
- **THEN** the workflow runs the Gradle publish to Maven Central using the tag-derived version

#### Scenario: Tag push without secrets skips cleanly
- **WHEN** a `v*` tag is pushed but the required secrets are absent
- **THEN** the workflow logs a visible notice that the connector publish is skipped
- **AND** the workflow run does not fail on account of the missing secrets

#### Scenario: Non-tag events do not publish
- **WHEN** a normal branch push or pull request triggers CI
- **THEN** no connector publish to Maven Central is attempted

### Requirement: Consumer documentation
`trino-connector/README.md` SHALL document the published coordinates and how to assemble the Trino
plugin directory from the published artifact.

#### Scenario: README documents coordinates and plugin assembly
- **WHEN** `trino-connector/README.md` is read after this change
- **THEN** it states the published coordinates `in.mcfad:cqlite-trino:<version>`
- **AND** it explains that a Trino plugin is a directory of jars and how to assemble it (e.g. via `installPlugin`) from the published artifact

