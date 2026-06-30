## Why

`@rustyrazorblade` needs to run the `cqlite-flight` Arrow Flight server as a container on every
Cassandra node without building from source (#1145). The build/publish **pipeline already exists**:
`cqlite-flight/Dockerfile` + `.github/workflows/flight-image.yml` landed in **PR #1102** (multi-arch
`linux/amd64` + `linux/arm64`, native-runner builds assembled into one manifest, `GITHUB_TOKEN`
auth, `v*`-tag + `workflow_dispatch` triggers, `vX.Y.Z` / `X.Y` / `latest` tag scheme).

But the pipeline has **never run** (0 workflow runs; the only release, v0.12.0, predates it), so
nothing is in GHCR, and two acceptance guarantees are currently **unverified by any automated gate**:

1. that a published image is **pullable unauthenticated** for both architectures, and
2. that the pulled image **actually runs and serves Flight on `:8815`**.

Today those are manual-trust claims, and the README compounds the gap: it states the server *"is
published … on every release tag"* (present tense) and lists `:v0.12.0` as pullable — neither is
true, because nothing has shipped.

This change closes the verification gap and corrects the docs, so the first real publish (and every
one after) is proven rather than assumed.

- **Milestone:** maintenance / release infrastructure. **Design-driven** (CI/release process — there
  is no Cassandra SSTable format oracle here).
- Adds a new `flight-container-distribution` capability.
- The distribution **decisions** (registry, arches, triggers, tag scheme, auth, image shape) are
  already resolved and implemented in PR #1102; this change formalizes the required end state as a
  verifiable spec and adds the missing post-publish proof + doc accuracy.

## What Changes

- **Post-publish container smoke-test in `flight-image.yml`.** After the multi-arch manifest is
  pushed, CI pulls the just-published image and runs the container to assert the Flight gRPC
  listener accepts connections on `:8815`, covering **each published architecture** natively (the
  amd64 check on the amd64 runner, the arm64 check on the `ubuntu-24.04-arm` runner). This converts
  acceptance bullets 1–2 into wiring-evidence: a publish that produces an unpullable or
  non-serving image fails the run instead of shipping silently.
- **README accuracy + unauthenticated-pull quickstart.** Correct the present-tense "is published"
  claim and the `:v0.12.0`-is-pullable implication to reflect actual availability, and document the
  **unauthenticated** `docker pull` + `docker run` path against a read-only mounted data dir
  (the run quickstart already exists; this makes the "no login required" guarantee explicit and the
  publish status truthful).

## Capabilities

### Added Capabilities
- `flight-container-distribution`: a verified multi-arch GHCR publish for `cqlite-flight` —
  post-publish per-arch smoke-test proving the image runs and serves `:8815`, plus truthful,
  unauthenticated pull/run documentation.

## Impact

- **Modified:** `.github/workflows/flight-image.yml` (add the smoke-test step/job),
  `cqlite-flight/README.md` (publish-status accuracy + unauthenticated-pull note).
- **No Rust / cqlite-core / cqlite-flight source changes.** `scripts/agent-gate.sh` (the Rust gate)
  does not exercise the container publish; verification for the smoke-test is the workflow itself
  (validated structurally + by a `workflow_dispatch image_tag: dev` run that must go green end to
  end, including the new smoke step).
- **No-heuristics mandate / memory budget / public binding surfaces:** unaffected.

## Non-goals

- **Owner prerequisites are out of scope for this change** (mirrors the trino-connector-release
  pattern):
  - **Making the GHCR package public** — the first push creates `cqlite-flight` as a *private*
    user-account package; flipping it to **Public** is a GHCR package-settings action only the owner
    can take. This change makes the *pull path verifiable*; it cannot change package visibility.
  - **Triggering the first publish** — fires on a `workflow_dispatch` (`image_tag: dev`) or the next
    `v*` tag; this change does not itself publish a release image.
- **No change to the distribution decisions from PR #1102** (registry, arch matrix, native-runner
  strategy, tag scheme, `GITHUB_TOKEN` auth, image shape) — they are accepted as-is.
- **No change to the `trino-connector` Maven artifact** — separate artifact, separate path.
- **No change to the crate / Python / Node release flows.**
- **No publish on normal branch pushes or PRs** — the smoke-test runs only inside the existing
  tag/dispatch publish path.
