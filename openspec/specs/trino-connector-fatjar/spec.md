# trino-connector-fatjar Specification

## Purpose
TBD - created by archiving change trino-fatjar-release-asset. Update Purpose after archive.
## Requirements
### Requirement: The build produces a self-contained shaded connector jar

A `shadowJar` task SHALL produce `trino-connector/build/libs/cqlite-trino-<version>-all.jar`
containing the connector's own classes and its complete runtime dependency closure, such that the
jar needs no dependency resolution to function as a Trino plugin. `io.trino:trino-spi` SHALL be
excluded, because the engine provides it and a second copy in the plugin directory would clash with
the engine's. Entries under `META-INF/services/` SHALL be **merged** across the connector and every
bundled dependency rather than overwritten, so that `META-INF/services/io.trino.spi.Plugin` — without
which the plugin does not register at all — survives shading, and so that a multi-source descriptor
retains **every** contributed provider. Declaring the merge is not sufficient on its own: with
`mergeServiceFiles()` configured but shadow's duplicates-strategy bypass withheld on the
transformer-owned paths, `io.grpc.internal.PickFirstLoadBalancerProvider` was **observed** to be
dropped from `META-INF/services/io.grpc.LoadBalancerProvider` — gRPC's default load balancer, so the
loss breaks every channel while the plugin still loads and the catalog still registers. The merge
SHALL therefore be asserted against the built jar's contents, not against the build configuration.
Bundled packages SHALL NOT be
relocated: Trino's plugin classloader is child-first except for `SPI_PACKAGES`
(`io.trino.spi.`, `com.fasterxml.jackson.annotation.`, `io.airlift.slice.`, `io.opentelemetry.api.`,
`io.opentelemetry.context.`), so the bundled netty/grpc/arrow/`jackson-databind` are already isolated,
and `com.fasterxml.jackson.annotation.` MUST keep resolving to the engine's copy for `ConnectorSplit`
JSON interop. A `verifyFatJar` task SHALL assert these contents against the **built jar**, not
against the build script, and SHALL fail the build when any of them does not hold.

#### Scenario: The shaded jar carries the connector and its runtime closure

- **WHEN** `./gradlew shadowJar` runs
- **THEN** `build/libs/cqlite-trino-<version>-all.jar` exists
- **AND** it contains the connector's `in/mcfad/cqlite/` classes
- **AND** it contains the Arrow `flight-core`, grpc, netty and `jackson-databind` runtime classes it depends on

#### Scenario: The Trino plugin service descriptor survives the merge

- **WHEN** `META-INF/services/io.trino.spi.Plugin` is read from the shaded jar
- **THEN** it names `in.mcfad.cqlite.flight.CqliteFlightPlugin`
- **AND** service files contributed by bundled dependencies are also present, with their entries merged rather than one file having replaced another

#### Scenario: A multi-source service descriptor retains every provider

- **WHEN** `META-INF/services/io.grpc.LoadBalancerProvider` is read from the shaded jar
- **THEN** it lists `io.grpc.internal.PickFirstLoadBalancerProvider` — gRPC's default load balancer — alongside the other contributed providers
- **AND** a build in which that entry is lost fails `verifyFatJar` rather than producing a jar that loads and registers the catalog but fails on the first query

#### Scenario: trino-spi is absent from the shaded jar

- **WHEN** the shaded jar's entries are enumerated
- **THEN** no `io/trino/spi/` class from `io.trino:trino-spi` is present

#### Scenario: verifyFatJar fails on a jar missing a required element

- **WHEN** `./gradlew verifyFatJar` runs against a shaded jar from which the plugin service descriptor is absent
- **THEN** the task fails, naming the missing element
- **AND** when it runs against the correctly-built shaded jar the task passes

#### Scenario: No package is relocated

- **WHEN** the shaded jar's package names are compared against the coordinates they came from
- **THEN** bundled classes retain their original packages (no shaded/relocated package prefix)
- **AND** `com/fasterxml/jackson/annotation/` classes, if present, are at their original package path so the engine's parent-first copy is the one that resolves

### Requirement: The shaded jar is published as a GitHub Release asset at a stable, version-addressed URL

A workflow `.github/workflows/trino-connector-fatjar.yml` SHALL attach
`cqlite-trino-<version>-all.jar` and a `cqlite-trino-<version>-all.jar.sha256` sidecar to a GitHub
Release, on two channels: a **release** channel keyed on the `v<version>` tag, reachable at
`https://github.com/pmcfadin/cqlite/releases/download/v<version>/cqlite-trino-<version>-all.jar`,
and a **dev** channel using one long-lived `trino-connector-dev` release, reachable at
`https://github.com/pmcfadin/cqlite/releases/download/trino-connector-dev/cqlite-trino-<version>-all.jar`.
The workflow SHALL trigger on `push` of `v*` tags and on `workflow_dispatch` accepting a `version`
and a `channel` input whose default is `dev`. Because the asset name carries the version, the single
dev release SHALL be able to hold many version-stamped assets concurrently, and it SHALL be marked
`prerelease: true` so GitHub never promotes it to the repository's latest release. The dev release
tag SHALL NOT match `v*`: every registry publish lane in the release train triggers on that glob, so
a `v*`-shaped dev tag would start the crates.io / PyPI / npm / Maven Central / GHCR lanes for a
version present in none of the four manifest fields. The sidecar SHALL be published for every asset,
so that a consumer caching the jar across restarts can distinguish a complete cached file from a
truncated download without re-fetching it.

#### Scenario: A release tag publishes the asset and its sidecar

- **WHEN** a `v<version>` tag is pushed
- **THEN** the workflow builds the shaded jar and attaches `cqlite-trino-<version>-all.jar` to the `v<version>` release
- **AND** it attaches `cqlite-trino-<version>-all.jar.sha256` alongside it
- **AND** the jar is downloadable at `https://github.com/pmcfadin/cqlite/releases/download/v<version>/cqlite-trino-<version>-all.jar`

#### Scenario: A dispatch to the dev channel publishes without minting a release tag

- **WHEN** `gh workflow run trino-connector-fatjar.yml -f version=0.17.1-dev.1 -f channel=dev` is invoked
- **THEN** the assets are attached to the `trino-connector-dev` release
- **AND** that release is marked as a prerelease
- **AND** no `v0.17.1-dev.1` tag is created

#### Scenario: The dev release accumulates version-stamped assets rather than replacing them

- **WHEN** a second dev-channel dispatch runs for a different version
- **THEN** the `trino-connector-dev` release holds both versions' assets, each under its own version-stamped name

#### Scenario: The dev tag starts no registry publish lane

- **WHEN** the `trino-connector-dev` release tag is matched against the release train's triggers
- **THEN** it matches no `v*` trigger, so no crates.io, PyPI, npm, Maven Central or GHCR publish lane is started by it

#### Scenario: A dispatch can backfill an already-released version

- **WHEN** `gh workflow run trino-connector-fatjar.yml -f version=0.17.0 -f channel=release --ref v0.17.0` is invoked for a version already shipped
- **THEN** the asset and sidecar are attached to the existing `v0.17.0` release
- **AND** no package registry is published to and no manifest version is changed

#### Scenario: The published checksum verifies the published jar

- **WHEN** the jar and its `.sha256` sidecar are both downloaded and checked (`shasum -a 256 -c`)
- **THEN** verification succeeds
- **AND** verification fails for a truncated copy of the jar

### Requirement: Publishing the shaded jar does not alter the Maven Central publication

Introducing the shaded artifact SHALL NOT change what is published to Maven Central. The
`in.mcfad:cqlite-trino:<version>` component SHALL continue to consist of the thin connector jar plus
its sources jar, javadoc jar and POM, and SHALL gain **no** shadow/shaded variant, **no** `-all.jar`
artifact and **no** `:all` classifier — the shaded jar is a GitHub Release asset exclusively. Because
applying a shadow plugin can wire its output into a publication as a side effect, this SHALL be
asserted by a `verifyShadowNotPublished` task that inspects the publication rather than being left to
review, and that task SHALL fail the build if a shaded artifact appears in the publication.

#### Scenario: A local publish produces no shaded artifact

- **WHEN** `./gradlew publishToMavenLocal -Pversion=<version>` runs
- **THEN** the local repository contains `in/mcfad/cqlite-trino/<version>/` with the main jar, `-sources.jar`, `-javadoc.jar` and POM
- **AND** it contains no `-all.jar` and no artifact with an `all` classifier

#### Scenario: verifyShadowNotPublished fails when a shaded artifact enters the publication

- **WHEN** `./gradlew verifyShadowNotPublished` runs against a build whose Maven publication includes the shadow component
- **THEN** the task fails, naming the offending artifact
- **AND** it passes against the shipped build configuration

### Requirement: The shaded jar installs as a Trino plugin from inside the plugin directory

A consumer SHALL be able to run the connector with the shaded jar as the **only** file in the plugin
directory: `/usr/lib/trino/plugin/cqlite_flight/cqlite-trino-<version>-all.jar`. Because
`ServerPluginsProvider.loadPlugins` filters the plugin path with `Files::isDirectory`, placing the jar
**as** `/usr/lib/trino/plugin/cqlite_flight` instead of inside it is silently ignored by Trino, so the
supported layout SHALL be the jar nested one level inside the plugin directory. An `installPluginFat`
task SHALL assemble exactly that layout locally, under a **separate output root**:
`build/plugin-fat/cqlite_flight/cqlite-trino-<version>-all.jar`. The root SHALL NOT be
`build/plugin/`, which `installPlugin` owns — keeping the roots distinct is deliberate, so that
assembling the one-jar layout leaves the ~50-jar `build/plugin/cqlite_flight` tree intact (the docker
stack still mounts that one) and a reader cannot mistake a leftover multi-jar tree for the fat
layout. The task SHALL be a `Sync` rather than a `Copy`, so a version bump cannot leave a stale
second shaded jar in the directory for Trino to load alongside the new one. The docker E2E harness
SHALL accept
`docker/e2e-test.sh --plugin-flavor=multi|fat`, defaulting to `multi` so the existing lane is
unchanged, and the `fat` flavor SHALL exercise the shaded jar through a real Trino query. The JVM
requirement is unchanged: the shaded jar still needs
`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` in Trino's `jvm.config`.

#### Scenario: installPluginFat assembles a one-jar plugin directory

- **WHEN** `./gradlew installPluginFat` runs
- **THEN** `build/plugin-fat/cqlite_flight/` contains the shaded jar
- **AND** it contains no other dependency jars
- **AND** an existing `build/plugin/cqlite_flight/` multi-jar tree from a prior `installPlugin` run is left untouched
- **AND** re-running the task after a version bump leaves exactly one shaded jar in the directory, not two

#### Scenario: The fat flavor serves a real query end to end

- **WHEN** `docker/e2e-test.sh --plugin-flavor=fat` runs
- **THEN** Trino registers the `cqlite` catalog from the plugin directory holding only the shaded jar
- **AND** a `SELECT` over `cqlite.<keyspace>.<table>` returns rows

#### Scenario: The default flavor is unchanged

- **WHEN** `docker/e2e-test.sh` runs with no flavor argument
- **THEN** it runs the `multi` flavor — the directory of jars produced by `installPlugin` — exactly as before this change

### Requirement: Consumer documentation states the directory rule, the channels, and the declared residuals

`trino-connector/README.md` and `website/src/content/docs/user-docs/flight-trino.md` SHALL document
the shaded jar as an install option. Both SHALL state that a Trino plugin is a **directory** (which
may contain exactly one self-contained jar) and SHALL NOT claim a plugin is "not a single jar", since
that becomes false once this artifact exists. Both SHALL warn prominently that mounting the jar **as**
`/usr/lib/trino/plugin/cqlite_flight` is silently ignored and that it must land inside that
directory. Both SHALL give the release-channel and dev-channel URL patterns, explain that the
`.sha256` sidecar exists so a consumer caching the jar in a per-node `hostPath` can distinguish a
complete cached file from a truncated download, state that Maven Central still publishes the **thin**
jar only (no `:all` classifier), and point at the existing `--add-opens` section rather than
duplicating it. The documented size SHALL be the **measured** figure (18.9 MB, `du -h` → `19M`) and
SHALL NOT be published as an estimate once a build has measured it. `RELEASING.md` SHALL list the
fat-jar lane in the publish fan-out and the
resumability table and SHALL document the `trino-connector-dev` channel, including why that tag is
deliberately not a `v*` tag. The declared residual — duplicate `LICENSE`/`NOTICE`/`DEPENDENCIES`
entries resolving **first-wins** rather than being aggregated, with aggregation deliberately deferred
— SHALL be recorded rather than omitted.

#### Scenario: The README documents the fat jar without the stale "not a single jar" claim

- **WHEN** `trino-connector/README.md` is read after this change
- **THEN** it describes a Trino plugin as a directory that may hold exactly one self-contained jar, and does not claim a plugin is "not a single jar"
- **AND** it gives both the release-channel and dev-channel download URLs, the `.sha256` sidecar and its purpose, the `installPluginFat` task, and the measured jar size
- **AND** it warns that mounting the jar as the plugin directory is silently ignored by Trino
- **AND** it states that Maven Central publishes the thin jar only, with no `:all` classifier
- **AND** it records the first-wins duplicate `LICENSE`/`NOTICE`/`DEPENDENCIES` residual and that aggregation was deferred

#### Scenario: The user-docs page offers the fat jar alongside the directory assembly

- **WHEN** `website/src/content/docs/user-docs/flight-trino.md` is read after this change
- **THEN** it still shows the directory-assembly install path (`installPlugin` and the Maven `Sync` recipe)
- **AND** it additionally documents the release asset, its URL patterns, the sidecar, and the mount-inside-not-as warning

#### Scenario: RELEASING.md documents the lane and the dev channel

- **WHEN** `RELEASING.md` is read after this change
- **THEN** the publish fan-out table has a row for `trino-connector-fatjar.yml` producing `cqlite-trino-<version>-all.jar`, marked re-runnable
- **AND** the resumability table states that re-running the lane re-uploads the same asset names idempotently
- **AND** a "Connector dev channel" section states that `trino-connector-dev` is one prerelease tag carrying many version-stamped assets, that it is deliberately not a `v*` tag because such a tag would start the registry publish lanes and could not pass the four-manifest preflight, and gives both `gh workflow run` invocations

