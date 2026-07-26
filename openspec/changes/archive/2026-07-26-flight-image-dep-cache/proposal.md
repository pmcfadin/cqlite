# Proposal: cargo-chef dep-layer caching + buildx GHA cache for flight-image builds (issue #2870)

**Milestone:** unset (intentionally unscheduled) · **Priority:** P2 · **Routing:** design-driven
(CI/build process) · **Issue:** #2870

## Why

`cqlite-flight/Dockerfile` is the naive worst case for a Rust image build. At `origin/main`
(`4012d506e`) it is 18 lines, and the build is a single layer:

```dockerfile
FROM rust:1.97.1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p cqlite-flight --features observability
```

Under `[profile.release]` = `codegen-units = 1`, `lto = true`, `panic = "abort"`, `strip = true`
(`Cargo.toml:170-174`), every image build recompiles the entire dependency tree plus the workspace
from scratch — **per architecture**, since `flight-image.yml` builds on a native-runner matrix
(`linux/amd64` on `ubuntu-latest`, `linux/arm64` on `ubuntu-24.04-arm`) rather than a QEMU platforms
list. And `docker/build-push-action@v6` is configured with **no `cache-from` / `cache-to`**
(`flight-image.yml:112-118`), so nothing survives between CI runs.

Image cadence is high — rc1 and rc2 inside one week during 0.16, plus round images throughout every
field sprint — so this is a large, recurring wall-clock cost on the critical path to field feedback.

## What Changes

1. **Commit `Cargo.lock`** (owner decision, 2026-07-24). Remove it from `.gitignore:3` and track it.
   This is the precondition that makes dependency-layer caching meaningful: cargo-chef derives
   `recipe.json` from the manifests **and the lockfile**, so without a tracked lockfile the cook layer
   can invalidate whenever any upstream crate publishes a semver-compatible release. It is also
   standard practice for a workspace that ships binaries (`cqlite-flight`, `cqlite-cli`).
2. **Split the Dockerfile into cargo-chef `planner` → `cook` → `build` stages**, leaving the runtime
   stage byte-for-byte equivalent (same `debian:bookworm-slim`, same `useradd -r -u 10001 flight`,
   same `USER`/`EXPOSE 8815`/`ENTRYPOINT`, same binary features and profile).
3. **Configure buildx GitHub Actions cache** (`cache-from`/`cache-to`, `type=gha`) keyed per
   architecture, on **both** `docker/build-push-action@v6` call sites that consume this Dockerfile —
   `flight-image.yml` (the tag train and the `workflow_dispatch` round/rc path) and `flight-ci.yml`.
4. **Record measured cold-vs-warm evidence** on the PR, showing the warm rebuild skipping dependency
   compilation.

## Expected side effect (call it out, do not silently absorb)

At least eight workflows key their Rust cache on `hashFiles('**/Cargo.lock')` — `coverage.yml:73`,
`quality-gates.yml:50`, `delta-roundtrip.yml:88`, `compaction-parity.yml:132`,
`soak-resource-leak.yml:53`, `docs-site.yml:138`, `coverage-baseline.yml:45`,
`live-cell-compaction-parity.yml:108`. With the file untracked, `hashFiles` returns an empty string
and each key collapses to a constant, so those caches never invalidate on a dependency change.
Committing the lockfile makes those keys start varying correctly — a repair, but one that **invalidates
their caches once** (a single cold run per workflow). This change does not otherwise touch them; the
underlying key-hygiene question is filed separately.

## Non-goals

- **No change to the shipped runtime image contents** beyond build provenance — same binary, same
  feature set, same base. The uncompressed-write claim boundary and every runtime behavior are untouched.
- **No library or product code.** CI/build tooling only; no parity, no-heuristics, or memory-budget impact.
- **No rework of the eight lockfile-keyed cache entries** beyond the automatic repair above.
- **No switch away from the native-runner matrix** to a QEMU `platforms:` list.
- **No registry-cache backend** (`type=registry`); GHA cache only.

## Doctrine impact

`cqlite-flight/Dockerfile` gains chef stages that each pin a Rust base image, so the **#1990 lockstep
checklist** must cover every new `FROM rust:<pin>` line — a toolchain bump has to move all of them
together or the stages diverge. `docs/development/ci-toolchain-policy.md` is updated accordingly.
Committing `Cargo.lock` is a contributor-visible repo-policy change and is noted in the same pass.

## Wiring evidence

The public surface is CI itself: the cache-hit timing evidence (criterion 3 of the issue) plus a green
image smoke job that starts the container and confirms the port comes up, with the multi-arch
manifest/digest-pin flow and its #2803 guards unaffected.
