# Tasks: trino-fatjar-release-asset (issue #2869)

> Design decided in `design.md`. In one line: build a shaded `cqlite-trino-<version>-all.jar` with
> merged service files and no relocation, publish it as a GitHub Release asset on a `v*` release
> channel and a rolling `trino-connector-dev` prerelease channel with a `.sha256` sidecar, prove the
> Maven Central publication is unchanged, and document the directory rule that makes or breaks the
> consumer integration. AC→requirement map at the top of
> `specs/trino-connector-fatjar/spec.md`.
>
> **Work is split across five units.** The surface each task names is the unit boundary — do not edit
> another unit's files.

## 1. Shaded artifact (surface: `trino-connector/build.gradle.kts`) — unit A

- [ ] 1.1 Apply the shadow plugin and configure `shadowJar` to emit
      `build/libs/cqlite-trino-<version>-all.jar` from the runtime closure, excluding
      `io.trino:trino-spi`.
- [ ] 1.2 Merge `META-INF/services/*` (service-file transformer). Verify
      `META-INF/services/io.trino.spi.Plugin` naming `in.mcfad.cqlite.flight.CqliteFlightPlugin`
      survives — without it the plugin does not register at all.
- [ ] 1.3 Do **not** relocate any package (D3). Jackson *annotations* must keep resolving to the
      engine's parent-first copy or `ConnectorSplit` JSON interop breaks.
- [ ] 1.4 `verifyFatJar` — assert against the **built jar**: connector classes present, merged plugin
      service descriptor present, `trino-spi` absent. Fail the build naming the missing element.
- [ ] 1.5 `verifyShadowNotPublished` — assert the Maven publication gained no shadow variant, no
      `-all.jar` and no `all` classifier. Applying the shadow plugin can wire `shadowJar` into a
      publication as a side effect, so this is a real failure mode.
- [ ] 1.6 `installPluginFat` — assemble `build/plugin/cqlite_flight/cqlite-trino-<version>-all.jar`
      and nothing else.
- [ ] 1.7 Emit the `.sha256` sidecar next to the jar (or have the workflow compute it — decide once,
      in one place, and do not do it twice).
- [x] 1.8 **Record the MEASURED jar size.** Done: **18.9 MB** (`du -h` → `19M`) from a clean `build/`
      under shadow 9.4.3 / Gradle 9.1.0, against a **172 KB** thin jar. `verifyFatJar` census: 11142
      entries, 49 runtime artifacts resolved / 48 contributing, 8 service descriptors across 2
      multi-source paths, 11 netty core modules at 4.1.130.Final, 6 arrow modules at 19.0.0. The
      earlier ~18–20 MB *estimate* (input jar sizes; no JDK on the authoring box) has been replaced by
      the measurement in `trino-connector/README.md`, the website page, `design.md` and `spec.md` — it
      is no longer hedged anywhere.
- [x] 1.9 Confirm the first-wins duplicate `LICENSE`/`NOTICE`/`DEPENDENCIES` behaviour actually holds
      in the built jar, since it is documented as a declared residual. **Confirmed**: the built jar has
      **zero duplicate entry names**, i.e. the colliding copies collapse first-wins rather than
      accumulating. Stated as observed in the README and `design.md`; the deferral reason is unchanged.
- [x] 1.10 **`mergeServiceFiles()` alone is insufficient — proven, and worse than documented.** With
      the merge configured but the duplicates bypass withheld on the transformer-owned paths, the build
      dropped `io.grpc.internal.PickFirstLoadBalancerProvider` (gRPC's **default** load balancer, so
      first-wins breaks every channel, not an edge case: the jar loads, the catalog registers, the
      first query dies), and `append()` degraded identically, leaving 10 of 11 netty modules with no
      attestation line. Named explicitly in the README, `design.md` D2 and the spec's shaded-artifact
      requirement + a dedicated scenario.

## 2. Docker E2E flavors (surface: `trino-connector/docker/*`) — unit B

- [ ] 2.1 `docker/e2e-test.sh --plugin-flavor=multi|fat`, default `multi` — the existing lane's
      behaviour must be unchanged with no argument.
- [ ] 2.2 The `fat` flavor mounts the shaded jar **inside** `/usr/lib/trino/plugin/cqlite_flight/`.
      Mounting it *as* that path is silently ignored by Trino (`Files::isDirectory`) and would produce
      a green-looking stack with no `cqlite` catalog.
- [ ] 2.3 The `fat` flavor runs a real `SELECT` through the shaded jar — the only check that proves
      the service-file merge worked end to end.
- [ ] 2.4 Reject an unrecognised `--plugin-flavor` value fail-closed; never silently fall back to
      `multi`.

## 3. CI registry + workflow validation (surface: workflow YAMLs, `.github/ci-gating-tiers.yml`) — unit C

- [ ] 3.1 Register the new workflow in `.github/ci-gating-tiers.yml` with its tier, and state whether
      it is `required`-gating.
- [ ] 3.2 Reconcile with the release train: the new lane must not change any existing lane's triggers.

## 4. Publish workflow (surface: `.github/workflows/trino-connector-fatjar.yml`, `scripts/ci/validate-workflows.rb`) — unit D

- [ ] 4.1 Triggers: `push: tags: v*` and `workflow_dispatch` with `version` + `channel`
      (`channel` default `dev`).
- [ ] 4.2 Release channel → the `v<version>` release. Dev channel → one long-lived
      `trino-connector-dev` release, `prerelease: true`, accumulating version-stamped assets.
- [ ] 4.3 Upload the jar **and** the `.sha256` sidecar for every publish.
- [ ] 4.4 Validate the `version` input fail-closed before it reaches any `run:` step or asset name —
      never interpolate `${{ inputs.* }}` directly into `run:` (GHA injection; the `roborev-lints`
      gate component fails `--lite` on it). Pass via a quoted env var.
- [ ] 4.5 Extend `scripts/ci/validate-workflows.rb` to cover the new workflow.
- [ ] 4.6 Verify the `trino-connector-dev` tag matches **no** `v*` trigger in any other workflow, so
      it cannot start a registry publish lane.

## 5. Documentation + spec (surface: docs + `openspec/changes/trino-fatjar-release-asset/**`) — unit E

- [x] 5.1 `trino-connector/README.md`: correct the "not a single jar" claim to the directory rule; add
      a "Self-contained fat jar (GitHub Release asset)" section with both channel URLs, the `.sha256`
      sidecar and why it exists, the mount-inside-never-as warning, `trino-spi` exclusion, the
      no-relocation rationale, the measured size, and a pointer to the existing
      `--add-opens` section; mention `installPluginFat` in the task list; record the first-wins
      LICENSE/NOTICE residual.
- [x] 5.2 `website/src/content/docs/user-docs/flight-trino.md`: a subsection mirroring the README
      option, placed inside the existing "Install the plugin" content, without restructuring the page;
      same directory-rule correction.
- [x] 5.3 `RELEASING.md`: a publish fan-out row and a resumability row for
      `trino-connector-fatjar.yml`, plus a "Connector dev channel" section (rolling prerelease tag,
      why it is deliberately not `v*`, one tag / many version-stamped assets, both
      `gh workflow run` invocations).
- [x] 5.4 `easy-db-lab-kits/trino-cqlite/README.md.template`: a NOTE under "Approach: init-container,
      not a baked image" that a pre-built fat jar now exists, with the URL pattern, that this kit
      **still** uses the Gradle-resolve initContainer, and that the hostPath port is tracked
      downstream in easy-db-lab#731 / PR #860. Do not rewrite the approach section — porting the kit
      is out of scope (it needs a live k8s cluster).
- [x] 5.5 This OpenSpec change: `proposal.md`, `design.md`, `tasks.md`, and the
      `specs/trino-connector-fatjar/spec.md` delta covering the shaded contents + service merge, the
      Release-asset URL contract, and publication non-pollution.
- [x] 5.6 `openspec validate trino-fatjar-release-asset --strict` clean.
- [ ] 5.7 **Flag, do not fix**: `openspec/specs/trino-connector-release/spec.md` still requires a
      secrets-absent `v*` tag push to "skip the publish with a visible notice", which #2156 replaced
      with fail-closed behaviour. Pre-existing drift, out of scope here; raise it for a follow-up.

## 6. Delivery

- [ ] 6.1 `--lite` green each fix round (summary-file redirect). Note the docs+YAML shape of this
      diff: run `scripts/ci/classify-docs-only.sh` before assuming a test failure is yours.
- [ ] 6.2 `rust-reviewer` (if any Rust is touched — it should not be) + sanctioned
      `scripts/flow/roborev-review.sh --agent … --model …` on the lite-green diff BEFORE the first
      full gate.
- [ ] 6.3 PR body: the full `AGENT-GATE SUMMARY`, the measured jar size (task 1.8), and the declared
      residuals.
- [ ] 6.4 `flow-closer` endgame: ONE full gate of record → C (spec-auditor) → final roborev →
      `premerge-assert` → `gh pr merge --auto --squash --delete-branch`.
