# Design — publish-flight-container

## Context

The publish pipeline (`flight-image.yml`, `cqlite-flight/Dockerfile`) is complete and merged
(PR #1102). What it lacks is a **proof step**: nothing confirms the artifact it pushes is actually
pullable and runnable. The acceptance criteria on #1145 are exactly those two properties. Since the
Rust agent-gate cannot see a container publish, the only place to enforce them is the publish
workflow itself.

The image's runtime contract (from the Dockerfile): `debian:bookworm-slim`, non-root `uid 10001`,
`EXPOSE 8815`, `ENTRYPOINT ["/usr/local/bin/cqlite-flight"]`; the binary takes a **required**
`--data-dir` and defaults `--listen` to `0.0.0.0:8815`.

## Decision — how to prove "runs and serves :8815", per architecture

**Chosen: a native per-arch TCP-readiness smoke-test inside the existing publish path.**

After the manifest is pushed, run the published image on each architecture's **native runner**
(amd64 on `ubuntu-latest`, arm64 on `ubuntu-24.04-arm` — the same runners the build matrix already
uses), pointed at a throwaway, well-formed empty data dir, and assert the container **binds and
accepts a TCP connection on `:8815`** within a bounded wait, then tear it down. A bound, accepting
`:8815` is the precise, low-flake signal for acceptance bullet 2 ("the pulled image runs the Flight
server listening on `:8815`"); the successful `docker pull` of the published manifest on a runner
authenticated only with the default token is the signal for acceptance bullet 1 (manifest exists and
is pullable for that arch).

Realization options for *where* the per-arch run happens (left to the implementer + `test-validator`
during flow-implement; the requirement is the outcome, not the mechanism):
- extend each matrix `build` job to `docker run` its **own** freshly built single-arch image
  (loaded locally) as an immediate self-check, **and** add a post-`merge` job that pulls the merged
  manifest by tag on the amd64 runner; or
- add a dedicated post-`merge` smoke job per arch that pulls the published tag on the matching native
  runner.

Readiness check: poll a TCP connect to `127.0.0.1:8815` (e.g. `nc -z` / a tiny socket loop) with a
timeout (~30s) rather than `sleep`; fail loudly on timeout. We deliberately do **not** require a
full Flight `do_get` round-trip in CI — that needs a real SSTable fixture mounted into the
container and would couple the publish gate to test-data plumbing; port-readiness is sufficient
evidence the entrypoint launched and bound the listener, and it is far less flaky.

### What it beat
- **Docs-only / manual smoke (rejected as the sole approach).** Just fix the README and trust a
  human to `docker run` once. This is the lightweight path, but it leaves both acceptance guarantees
  unverified on every future release — exactly the "built but unwired, passes green" failure mode
  this project guards against. The README fix is still **included**; it is just not sufficient alone.
- **Full `do_get` round-trip smoke against a mounted fixture (rejected as too heavy).** Strongest
  proof, but requires shipping/mounting a real SSTable tree into the container in CI and asserting
  Arrow output — high flake surface and test-data coupling for marginal gain over port-readiness.
  Out of scope; the existing `cargo test -p cqlite-flight` suite already covers `do_get` semantics.
- **QEMU-emulated cross-arch smoke on one runner (rejected).** Would let one amd64 runner "run" the
  arm64 image, but QEMU emulation is slow and flaky — the same reason PR #1102 chose native runners
  for the build. Use the native arm64 runner that already exists in the matrix.

## Decision — README accuracy

State the **current** availability truthfully (the image is published by this workflow on a `v*`
tag or a manual dispatch; until the first run, no tag is in GHCR yet) and make the **unauthenticated**
pull explicit (`docker pull ghcr.io/pmcfadin/cqlite-flight:<tag>` requires no `docker login` **once
the package is public** — an owner action). Keep the existing run quickstart; remove the implication
that a specific `:v0.12.0` tag is already pullable.

### What it beat
- Leaving the present-tense "is published … on every release tag" wording (rejected — it is untrue
  until the first run and misleads an operator into pulling a tag that 404s).

## Risks / trade-offs

- **arm64 runner availability.** `ubuntu-24.04-arm` is already used by the build matrix, so the
  smoke adds no new runner dependency.
- **Package-visibility coupling.** The unauthenticated-pull assertion only holds once the package is
  public (owner action). CI runs authenticated with `GITHUB_TOKEN`, so the CI smoke proves
  *pullability + serving*, not *anonymous* pullability; the anonymous guarantee is documented and
  owner-gated (Non-goal), matching how trino-connector-release handled its owner prerequisites.
- **First-run discovery.** The smoke first executes on the next dispatch/tag; a `workflow_dispatch
  image_tag: dev` run is the intended pre-release proof (and the recommended first action post-merge).
