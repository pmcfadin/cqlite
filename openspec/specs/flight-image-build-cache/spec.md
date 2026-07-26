# flight-image-build-cache Specification

## Purpose
TBD - created by archiving change flight-image-dep-cache. Update Purpose after archive.
## Requirements
### Requirement: The dependency lockfile is tracked and builds are locked
`Cargo.lock` SHALL be tracked in version control, and the flight image build SHALL resolve
dependencies with `--locked` so that a lockfile inconsistent with the manifests fails the build
rather than silently re-resolving.

#### Scenario: The lockfile is present in a clean checkout
- **WHEN** the repository is cloned fresh and `git ls-files --error-unmatch Cargo.lock` is run
- **THEN** the command succeeds, and `.gitignore` contains no entry that excludes `Cargo.lock`

#### Scenario: A manifest change without a lockfile refresh fails the image build
- **GIVEN** a manifest edit that adds a dependency without updating `Cargo.lock`
- **WHEN** the flight image is built
- **THEN** the build fails, and the failure output names the lockfile inconsistency rather than failing later as an unrelated compile error

### Requirement: The image build separates dependency compilation from workspace compilation
`cqlite-flight/Dockerfile` SHALL use cargo-chef stages so that dependency compilation occurs in a
layer that is invalidated only by a change to the manifests or the lockfile, and SHALL NOT be
invalidated by a change confined to workspace source files.

#### Scenario: A source-only edit reuses the cached dependency layer
- **GIVEN** an image built once with a warm cache
- **WHEN** a workspace source file is modified with no change to any manifest or to `Cargo.lock`, and the image is rebuilt
- **THEN** the dependency-compilation layer is reused, and the rebuild log shows no compilation of third-party dependencies

#### Scenario: A dependency change invalidates the dependency layer
- **GIVEN** an image built once with a warm cache
- **WHEN** a dependency is added or its version changed in a manifest with `Cargo.lock` updated to match, and the image is rebuilt
- **THEN** the dependency-compilation layer is rebuilt

### Requirement: Cooked artifacts match the final build's profile and features
The dependency-cook invocation SHALL use the same release profile, package selection, and feature set
as the final build (`--release -p cqlite-flight --features observability`), so that the cooked
artifacts are reusable rather than recompiled.

#### Scenario: The cook and build invocations agree
- **WHEN** `cqlite-flight/Dockerfile` is inspected
- **THEN** the cook step and the final build step specify the same profile, the same `-p` package selection, and the same `--features` value

#### Scenario: A warm rebuild does not recompile dependencies
- **WHEN** a warm rebuild is performed after a source-only edit
- **THEN** its log contains no `Compiling` line for any third-party dependency, demonstrating that the cooked artifacts were reused rather than rebuilt behind a hit layer

### Requirement: CI configures a per-architecture build cache on every consuming workflow
Every `docker/build-push-action` invocation that builds `cqlite-flight/Dockerfile` SHALL configure
`cache-from` and `cache-to` against the GitHub Actions cache, scoped per architecture so concurrent
matrix legs do not evict one another. This SHALL apply to both the `v*` tag path and the
`workflow_dispatch` round/rc path.

#### Scenario: Both consuming workflows configure the cache
- **WHEN** the workflows that build this Dockerfile are inspected
- **THEN** each `docker/build-push-action` step specifies `cache-from` and `cache-to` of `type=gha`, and no consuming workflow is left without them

#### Scenario: Concurrent architecture legs do not share a cache scope
- **WHEN** the `linux/amd64` and `linux/arm64` matrix legs run concurrently
- **THEN** their cache scopes are distinct, and each leg's second run reuses its own dependency layer

#### Scenario: The dispatch path caches like the tag path
- **WHEN** the image is built through `workflow_dispatch` with an `image_tag` input
- **THEN** the same cache configuration applies as on a `v*` tag build

### Requirement: Cold-versus-warm build evidence is recorded
The change SHALL record, on the pull request, the measured cold build time and the warm-cache rebuild
time for at least one architecture, together with the log evidence that the warm rebuild skipped
dependency compilation.

#### Scenario: The PR carries both timings and the skip evidence
- **WHEN** the pull request is reviewed
- **THEN** it states the cold and warm build durations for a named architecture, and includes the log excerpt showing dependency compilation absent from the warm rebuild

### Requirement: The shipped runtime image and its publish flow are unchanged
The runtime stage SHALL remain equivalent to the pre-change image — same base, same non-root user,
same exposed port, same entrypoint, and a binary built with the same profile and features — and the
multi-arch manifest, digest-pin flow, and their #2803 guard conditions SHALL be unaffected. An
untargeted `docker build` SHALL still produce the runtime image.

#### Scenario: The runtime stage is equivalent
- **WHEN** the pre-change and post-change runtime stages are compared
- **THEN** the base image, the non-root user creation, the exposed port, and the entrypoint are unchanged, and the binary is built with the same profile and feature set

#### Scenario: The smoke job still passes
- **WHEN** the image smoke job runs against the published image
- **THEN** the container starts and its port accepts a connection, as before

#### Scenario: An untargeted build yields the runtime image
- **GIVEN** a consumer that builds this Dockerfile without specifying a target stage
- **WHEN** it builds the image
- **THEN** the resulting image is the runtime image, not an intermediate build stage

#### Scenario: The publish guards are intact
- **WHEN** the image workflow is inspected after the change
- **THEN** the merge and smoke jobs retain their #2803 guard conditions, and the digest-pin flow is unchanged

### Requirement: Every Rust base pin moves in lockstep
All build stages SHALL pin the same Rust base image version, matching `rust-toolchain.toml`, and the
#1990 lockstep checklist SHALL enumerate every `FROM rust:<version>` line in the Dockerfile.

#### Scenario: All stages agree with the toolchain pin
- **WHEN** the Dockerfile is inspected
- **THEN** every `FROM rust:<version>` line names the same version, and that version matches the `channel` in `rust-toolchain.toml`

#### Scenario: The lockstep checklist names each stage
- **WHEN** the CI toolchain policy document is read
- **THEN** its lockstep checklist identifies every `FROM rust:<version>` line in this Dockerfile as requiring update on a toolchain bump

