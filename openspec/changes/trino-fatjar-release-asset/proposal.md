# Proposal: a self-contained `cqlite-trino` fat jar published as a GitHub Release asset (issue #2869)

**Milestone:** maintenance (distribution) · **Routing:** design-driven — there is no external oracle
for *how* an artifact is distributed. The Cassandra format is untouched; what has latitude is the
packaging shape, the channel, and what the Maven publication is allowed to become. · **Issue:** #2869 ·
**Downstream driver:** rustyrazorblade/easy-db-lab#731 (PR #860).

## Why

Installing this connector today means assembling a *directory of jars*: `in.mcfad:cqlite-trino` is a
thin jar whose runtime closure resolves to roughly **50 artifacts** (Arrow `flight-core`, grpc, netty,
`jackson-databind`, …). Every consumer therefore needs a dependency resolver at install time. The
`easy-db-lab-kits/trino-cqlite` overlay in this repository does exactly that: a
`gradle:9.1.0-jdk25` initContainer runs a throwaway `Sync` against Maven Central, into an `emptyDir`,
**on every Trino pod start**.

That is the shape easy-db-lab's maintainer has rejected while porting the cqlite kits in-tree
(rustyrazorblade/easy-db-lab#731, PR #860): the requirement there is **one jar, fetched once**, cached
in a per-node `hostPath` and mounted per node — not a Gradle configuration pass plus an outbound
HTTPS resolve from every app-node pod at every restart. The same constraint shows up in every
air-gapped or image-baked deployment.

Nothing in the current release train produces such an artifact, and Maven Central is the wrong place
to put one: adding a shaded variant to a published component changes what those coordinates mean for
every existing Maven/Gradle consumer, and "fetchable with one `curl`" is not a reason to add a file
to a resolver's repository.

## What Changes

1. **A shaded jar is built** — `trino-connector/build/libs/cqlite-trino-<version>-all.jar` — bundling
   the connector plus its full runtime closure, with `META-INF/services/*` entries **merged** rather
   than overwritten. `trino-spi` is excluded (engine-provided). **No packages are relocated**, and
   that is deliberate, not an omission (see `design.md`).
2. **It is published as a GitHub Release asset, with a `.sha256` sidecar**, on two channels:
   - release — `.../releases/download/v<version>/cqlite-trino-<version>-all.jar`
   - dev — `.../releases/download/trino-connector-dev/cqlite-trino-<version>-all.jar`, one long-lived
     `prerelease: true` tag carrying many version-stamped assets.
3. **The Maven Central publication is unchanged, and that is asserted** by a build check, not left to
   care: no shadow variant, no `-all.jar`, no `:all` classifier on Central.
4. **Build + verification tasks**: `shadowJar`, `verifyFatJar`, `verifyShadowNotPublished`,
   `installPluginFat`.
5. **The E2E stack can run either flavor**: `docker/e2e-test.sh --plugin-flavor=multi|fat`
   (`multi` remains the default, so the existing lane is unchanged).
6. **Documentation states the directory rule prominently.** Trino loads a plugin only from a
   *directory*, so the jar must land **inside** `/usr/lib/trino/plugin/cqlite_flight/`; mounting it
   *as* that path is silently ignored. Two docs currently assert a Trino plugin is "not a single
   jar", which becomes actively misleading once this artifact exists — both are corrected.

## Capabilities

### New Capabilities
- `trino-connector-fatjar`: the shaded self-contained connector artifact — what it contains, where it
  is published and at what URL, and the guarantee that publishing it does not alter the Maven Central
  component.

### Modified Capabilities
<!-- None. `trino-connector-release` keeps every requirement it has: the thin jar, the coordinates,
     the POM, the tag-derived version and the Central publish lane are all unchanged by this change.
     `flight-trino-user-docs` requires the user docs to SHOW the directory-assembly path; documenting
     an additional one-jar option does not remove or contradict that, so no delta is needed there
     either. -->

## Impact

- `trino-connector/build.gradle.kts` — shadow plugin, the four tasks, publication guard.
- `trino-connector/docker/*` — `e2e-test.sh --plugin-flavor`, plugin-dir assembly for the `fat` flavor.
- `.github/workflows/trino-connector-fatjar.yml` (new) + `.github/ci-gating-tiers.yml` +
  `scripts/ci/validate-workflows.rb`.
- Docs: `trino-connector/README.md`, `website/src/content/docs/user-docs/flight-trino.md`,
  `RELEASING.md`, `easy-db-lab-kits/trino-cqlite/README.md.template`.
- No Rust crate, no binding, no on-disk format, and no decode path is touched — the **no-heuristics
  mandate**, the public Python/Node/CLI surfaces and the <128 MB memory budget are all unaffected.

## Non-goals

- **Not porting the easy-db-lab kit** to fetch-and-`hostPath`. The in-repo overlay keeps its
  Gradle-resolve initContainer; verifying the ported shape needs a live multi-node Kubernetes cluster,
  which nothing in this repository's CI can provide. Tracked downstream in easy-db-lab#731 / PR #860,
  and recorded as a NOTE in the kit's README template.
- **Not publishing a shaded variant to Maven Central.** No `:all` classifier, ever — this change
  asserts the opposite.
- **Not relocating bundled packages.** Trino's child-first plugin classloader already isolates them,
  and relocating Jackson *annotations* would break `ConnectorSplit` JSON interop.
- **Not aggregating duplicate `LICENSE`/`NOTICE`/`DEPENDENCIES` entries.** First-wins is accepted and
  declared as a residual; aggregating needs extra duplicates-strategy bypasses for a non-functional
  gain.
- **Not changing the `--add-opens` requirement**, the catalog properties, or any query behaviour.
- **Not optimising the artifact's size.** It is **18.9 MB** as measured on this branch (vs a 172 KB
  thin jar); that is documented for `hostPath` cache sizing, and shrinking it is not a goal here.
