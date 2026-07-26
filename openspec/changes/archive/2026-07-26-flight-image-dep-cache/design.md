# Design: flight-image-dep-cache (issue #2870)

## D1 — Commit `Cargo.lock`, and build `--locked`

**Chosen** (owner decision, 2026-07-24). Drop `Cargo.lock` from `.gitignore:3`, track it, and have the
image build pass `--locked` so a stale lockfile **fails the build** instead of silently re-resolving.

**What it beat.** (a) Generating the lockfile inside the planner stage (`cargo generate-lockfile`) —
keeps repo policy unchanged, but `recipe.json` then shifts whenever any upstream crate publishes a
compatible version, so cook-layer hits degrade unpredictably and images are not reproducible.
(b) Dropping chef and adding only buildx cache — with `COPY . .` as one layer, any source edit busts
the whole cache, which is precisely the case we build for.

**Consequence to watch:** `--locked` turns "someone edited a manifest without refreshing the lock"
into a red image build. That is the intended failure mode, but it is a new one, so the spec requires
the failure to be legible rather than a generic cargo error.

## D2 — Four stages: `chef` → `planner` → `cook`+build → runtime

**Chosen.**

```
FROM rust:1.97.1-bookworm AS chef      # cargo install cargo-chef --locked   (stable, cached ~forever)
FROM chef AS planner                    # COPY . . ; cargo chef prepare --recipe-path recipe.json
FROM chef AS builder                    # COPY --from=planner recipe.json ; cargo chef cook … ; COPY . . ; cargo build …
FROM debian:bookworm-slim               # unchanged runtime stage
```

The planner stage still does `COPY . .` — that is fine and is how chef is meant to work: the planner's
output is only `recipe.json`, which changes solely when manifests or the lockfile change, so the
expensive `cook` layer downstream stays valid across ordinary source edits.

**Runtime stage is untouched** — same `debian:bookworm-slim`, `useradd -r -u 10001 flight`,
`COPY --from=builder /src/target/release/cqlite-flight`, `USER flight`, `EXPOSE 8815`, `ENTRYPOINT`.
The `COPY --from=` source stage name changes from `build` to `builder`; nothing else in that stage moves.

**Default-target compatibility matters here.** Three consumers build this file: `flight-image.yml`,
`flight-ci.yml`, and `trino-connector/docker/docker-compose.yml:63-65` (`context: ../..`, no `--target`).
Because the runtime stage remains **last**, an untargeted `docker build` still produces the runtime
image, so docker-compose keeps working with no edit.

## D3 — `cook` flags must match the final build exactly

**Chosen.** Both invocations carry the identical triple:
`--release -p cqlite-flight --features observability`.

**Why this is load-bearing.** cargo reuses a cached artifact only when the profile *and* feature
resolution match. A `cook` that omits `--features observability`, or that cooks the whole workspace
while the build compiles one package, produces artifacts the final `cargo build` will not reuse — the
image still builds, the cache still "hits" at the Docker layer level, and the compile happens **anyway**.
That is a silent no-op: green, cached, and pointless. Criterion 3's warm-rebuild timing is what proves
it did not happen, which is why the spec makes that evidence mandatory rather than nice-to-have.

Scoping cook to `-p cqlite-flight` also sidesteps the workspace's manifest-shape hazards for a
manifests-only copy: the root package `cqlite` declares no `src/` (`Cargo.toml:24-27` — it hosts the
top-level `tests/*.rs`), and the workspace declares many explicit target paths (`cqlite-core` 20
`[[…]]` blocks, `tests` 16, `cqlite-cli` 8) that must exist for `cargo metadata` to resolve.
`bindings/node/build.rs` is likewise out of the dependency closure of `-p cqlite-flight`.

## D4 — GHA cache scoped per architecture, on both call sites

**Chosen.** `cache-from: type=gha,scope=flight-<arch>` and `cache-to: type=gha,mode=max,scope=flight-<arch>`,
where `<arch>` derives from the matrix platform.

**Why per-arch scoping.** The build matrix runs native `linux/amd64` and `linux/arm64` runners
concurrently. A shared scope would have the two architectures overwrite each other's layer manifests,
producing cross-arch misses that look like cache flakiness. `mode=max` exports intermediate layers,
which is the point — the `cook` layer is an intermediate.

Applied to **both** `docker/build-push-action@v6` sites (`flight-image.yml:112-118` and
`flight-ci.yml`), since both consume this Dockerfile; caching only the release path would leave the
per-PR lane on cold builds.

## D5 — Toolchain lockstep across every new `FROM rust:` line

**Chosen.** All chef-family stages pin the same `rust:1.97.1-bookworm` as today, and
`docs/development/ci-toolchain-policy.md`'s #1990 lockstep checklist is extended to name **every**
`FROM rust:<pin>` line in the Dockerfile, not "the Dockerfile" generically.

**Why.** #2856 had just repaired exactly this class of drift (the base was three minor versions behind
the pin). Splitting one `FROM rust:` into several multiplies the drift surface, and the base is
currently in a good state — `rust:1.97.1-bookworm` matches `rust-toolchain.toml` `channel = "1.97.1"`
exactly. Introducing caching must not quietly re-open the hole #2856 closed.

## Risk

The measurement in criterion 3 is the part most likely to be faked by accident. A warm rebuild can look
fast because Docker reused a layer for reasons unrelated to chef, so the spec requires the *dependency
compile* to be shown absent from the warm build's log — not merely a smaller wall-clock number.
