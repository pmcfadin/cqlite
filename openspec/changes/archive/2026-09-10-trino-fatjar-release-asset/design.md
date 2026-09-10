# Design: trino-fatjar-release-asset (issue #2869)

## Context

`trino-connector/` builds a Trino SPI 481 connector on Java 25. It publishes to Maven Central as
`in.mcfad:cqlite-trino:<version>` — a **thin** jar with `compileOnly("io.trino:trino-spi")` (so
`trino-spi` never enters the published runtime scope) and its remaining dependencies declared in the
POM. Installing it therefore means resolving that POM: `./gradlew installPlugin` in this repo, or the
throwaway `Sync` recipe in `trino-connector/README.md` for a consumer, either way producing
`build/plugin/cqlite_flight/` — a directory of ~50 jars.

Two facts about Trino constrain everything below, and both were read from Trino's own source rather
than assumed:

- **`io.trino.server.ServerPluginsProvider.loadPlugins` filters the plugin path with
  `Files::isDirectory`.** A plugin is a *directory*. A bare file placed at
  `/usr/lib/trino/plugin/cqlite_flight` is not an error — it is skipped, silently.
- **`PluginManager` gives each plugin a child-first classloader** whose only parent-first packages are
  `SPI_PACKAGES`: `io.trino.spi.`, `com.fasterxml.jackson.annotation.`, `io.airlift.slice.`,
  `io.opentelemetry.api.`, `io.opentelemetry.context.`.

The consumer forcing the change is easy-db-lab (rustyrazorblade/easy-db-lab#731, PR #860): one jar,
fetched once, cached in a per-node `hostPath`, mounted per node — no resolver in the pod-start path.

## Goals / Non-Goals

**Goals:**
- One self-contained artifact a consumer can fetch by version with `curl`, with an integrity sidecar.
- A stable, guessable URL for both released and pre-release versions.
- The Maven Central component provably unchanged.
- The directory rule documented where a consumer will hit it, because violating it fails silently.

**Non-Goals:**
- A shaded artifact on Maven Central (no `:all` classifier).
- Package relocation.
- Aggregated in-jar legal metadata.
- Porting the in-repo easy-db-lab kit (downstream, needs a live cluster).
- A measured size figure on this branch.

## Decisions

### D1 — GitHub Release asset, not Maven Central

The fat jar is published **exclusively** as a GitHub Release asset. Two reasons, and they point the
same way:

1. **Meaning.** `in.mcfad:cqlite-trino:<version>` currently means "the connector, dependencies
   declared in the POM". Adding a shaded variant under the same coordinates changes what a Maven or
   Gradle consumer can end up resolving. The existing `trino-connector-release` spec pins the POM's
   contents deliberately; this change must not erode it.
2. **Purpose.** The whole point of the artifact is that it needs **no resolver**. Putting it in a
   resolver's repository serves nobody who wanted it.

Consequence, asserted rather than trusted: `verifyShadowNotPublished` fails the build if the Maven
publication gains a shadow variant or an `-all.jar`. Applying the shadow plugin can wire
`shadowJar` into a publication as a side effect, so this is a real failure mode, not a hypothetical.

### D2 — Merge `META-INF/services/*`; do not overwrite

The connector is discovered by Trino through `META-INF/services/io.trino.spi.Plugin`. Several bundled
dependencies (grpc, arrow, jackson) also ship service files — the built jar carries **8 service
descriptors, 2 of them contributed by more than one dependency**. A naive shade that takes the last
writer drops the plugin descriptor and the plugin does not register **at all** — a failure that looks
like "the catalog is missing" rather than "the jar is wrong". So service-file entries are **merged**,
and `verifyFatJar` asserts the merged descriptor is present in the built jar (alongside: connector
classes present, `trino-spi` absent).

**Measured, and worse than assumed: `mergeServiceFiles()` alone is not sufficient.** With the merge
configured but shadow's duplicates-strategy bypass withheld on the transformer-owned paths, the build
silently dropped `io.grpc.internal.PickFirstLoadBalancerProvider` from
`META-INF/services/io.grpc.LoadBalancerProvider`. That is gRPC's **default** load balancer, so
first-wins there does not degrade an edge case — it breaks **every** channel, i.e. the jar loads,
Trino registers the catalog, and the first query dies. `append()` degraded identically, leaving 10 of
11 netty modules with no attestation line. The bypass is therefore load-bearing, and this is why
`verifyFatJar` must assert against the **built jar** and not against the build script: the build
script said "merge" while the jar said otherwise.

### D3 — No relocation, deliberately

Shading normally implies relocating bundled packages to avoid clashing with the host. Here the host
already isolates them: Trino's plugin classloader is child-first except for `SPI_PACKAGES`, so the
bundled netty / grpc / arrow / `jackson-databind` cannot collide with the engine's copies. Relocation
would buy nothing and would cost something real — `com.fasterxml.jackson.annotation.` is parent-first
by design, because `ConnectorSplit` instances are serialized to JSON *by the engine*; relocating
those annotations moves them out of the package the engine loads and breaks split interop.

So: bundle without relocating. This is recorded because "shaded but not relocated" reads like an
oversight and is not one.

### D4 — Two channels, one asset name; the dev tag is deliberately not `v*`

| Channel | Git/release tag | URL |
|---|---|---|
| release | `v<version>` | `.../releases/download/v<version>/cqlite-trino-<version>-all.jar` |
| dev | `trino-connector-dev` | `.../releases/download/trino-connector-dev/cqlite-trino-<version>-all.jar` |

The asset **name carries the version**, so one dev release tag can hold many builds and the URL for
any given pre-release version stays stable and guessable.

`trino-connector-dev` must not be shaped like `v*`, and the reason is structural, not stylistic:
**every** publish lane in the release train (crates.io, PyPI, npm, Maven Central, GHCR) triggers on
`v*`. A dev tag matching that glob would start all of them for a version that exists in none of the
four manifest fields. `trino-connector-dev` matches zero release-train triggers, so it cannot start
them; and the shared `release-preflight.yml` (`scripts/bump-version.sh check <version>`) would fail
it anyway, which is a second, independent barrier rather than the primary one. The release is created
with `prerelease: true` so GitHub never promotes the rolling dev tag to "latest".

### D5 — The `.sha256` sidecar earns its place

A checksum next to a download is often ceremony. Here it is load-bearing for the actual consumer: the
jar is cached in a **per-node `hostPath` that outlives the pod**, and a download-if-missing
initContainer must decide "already cached" versus "a truncated leftover from an interrupted
download" without re-fetching ~19 MB on every Trino pod start. File presence cannot distinguish
those two states; a checksum can. Publishing the sidecar is what makes the caching strategy sound.

### D6 — E2E flavors: `multi` stays the default

`docker/e2e-test.sh --plugin-flavor=multi|fat` defaults to `multi`, so the existing lane's behaviour
is bit-for-bit unchanged and the `fat` flavor is an added path rather than a substitution. The `fat`
flavor is what actually demonstrates the D2 service-file merge and the directory rule end to end — a
jar that fails either produces a Trino with no `cqlite` catalog, which the E2E assertions detect.

### D7 — Documentation: correct the "not a single jar" claim rather than adding beside it

`trino-connector/README.md` and `website/.../flight-trino.md` both state a Trino plugin is "**not a
single jar**". That sentence was a serviceable shorthand for "assemble a directory"; once a
self-contained jar exists it is simply false, and a consumer who believes it will not look for this
artifact. Both are reworded to the accurate rule — a plugin is a **directory**, which may contain
exactly one self-contained jar — and both carry the mount-inside-never-as warning, because that
failure is silent and is the most likely way the downstream integration breaks.

## Risks / Trade-offs

- **Silent-failure risk is inherent, not fixable here.** `Files::isDirectory` filtering means a
  wrong mount produces no diagnostic from Trino. Nothing in this repository can make Trino warn. The
  only available mitigation is documentation prominence (D7) plus the `fat` E2E flavor demonstrating
  the correct layout, and that is what this change ships.
- **Duplicate legal metadata resolves first-wins — CONFIRMED in the built jar.** `META-INF/LICENSE`,
  `META-INF/NOTICE` and `META-INF/DEPENDENCIES` collide across bundled dependencies; one copy lands
  and the rest are dropped, so the in-jar aggregated `NOTICE` is incomplete. This is **observed**: the
  built jar has **zero duplicate entry names**, so the colliding copies really do collapse rather than
  accumulate. Aggregating properly (`ApacheNoticeResourceTransformer`) needs additional shadow
  duplicates-strategy bypasses for a gain that changes no behaviour, so it is **deferred and
  declared** — in the spec, in the README, and here — rather than omitted. Per-dependency licences
  remain discoverable from the POM and from an `installPlugin` directory.
- **Size: measured, 18.9 MB** (`du -h` → `19M`) from a clean `build/` under shadow 9.4.3 / Gradle
  9.1.0, against a **172 KB** thin jar. Published for `hostPath` cache sizing (~19 MB per cached
  version). Shrinking it is a non-goal.
- **A second distribution channel is a second thing to keep correct.** Mitigated by the lane being
  the only non-immutable one in the train: release assets are mutable, so the workflow is
  re-runnable and can backfill an already-shipped version without a new tag or a version bump.
- **Pre-existing drift, NOT fixed here:** `openspec/specs/trino-connector-release/spec.md` still
  requires a secrets-absent `v*` tag push to "skip the publish with a visible notice", which #2156
  replaced with fail-closed behaviour. Out of scope for this change; recorded so the next reader of
  that spec knows it is stale.
