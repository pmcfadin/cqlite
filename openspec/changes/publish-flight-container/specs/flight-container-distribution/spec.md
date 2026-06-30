## ADDED Requirements

### Requirement: Multi-arch image is published to GHCR on tag and dispatch
The `cqlite-flight` Arrow Flight server SHALL be published to `ghcr.io/<owner>/cqlite-flight` as a
single multi-architecture manifest covering `linux/amd64` and `linux/arm64`, built on native runners
and authenticated with the workflow's built-in `GITHUB_TOKEN`. Publication SHALL trigger
automatically on `v*` tag pushes and on demand via `workflow_dispatch` with a custom image tag, and
SHALL NOT trigger on normal branch pushes or pull requests.

#### Scenario: Tag push publishes a multi-arch manifest
- **WHEN** a `v*` tag is pushed
- **THEN** the workflow builds `linux/amd64` and `linux/arm64` on native runners and assembles them into one manifest under `ghcr.io/<owner>/cqlite-flight`
- **AND** the manifest is tagged `vX.Y.Z` and `X.Y`, plus `latest` for a non-prerelease tag

#### Scenario: Manual dispatch publishes under the supplied tag
- **WHEN** the workflow is run via `workflow_dispatch` with `image_tag: dev`
- **THEN** the multi-arch manifest is published as `ghcr.io/<owner>/cqlite-flight:dev`

#### Scenario: Ordinary CI events do not publish
- **WHEN** a normal branch push or a pull request triggers CI
- **THEN** no `cqlite-flight` image publish to GHCR is attempted

### Requirement: Published image is verified to run and serve :8815 per architecture
The publish workflow SHALL, for each published architecture, pull the published image on a runner
native to that architecture and run the container to confirm the Flight gRPC listener binds and
accepts a TCP connection on port `8815` within a bounded wait. A pulled image that cannot be run, or
that fails to accept a connection on `:8815`, SHALL fail the workflow run rather than publish
silently. The check SHALL cover both `linux/amd64` and `linux/arm64`.

#### Scenario: Each published architecture is smoke-tested natively
- **WHEN** the publish workflow runs (tag or dispatch)
- **THEN** the just-published image is pulled and run on a `linux/amd64` runner and on a `linux/arm64` runner
- **AND** each run asserts a TCP connection to `127.0.0.1:8815` is accepted within the bounded wait, then tears the container down

#### Scenario: A non-serving image fails the run
- **WHEN** the pulled container does not accept a connection on `:8815` before the timeout
- **THEN** the smoke-test step exits non-zero and the workflow run fails
- **AND** no run is reported as successful for that publish

### Requirement: Documentation states truthful availability and the unauthenticated pull/run path
`cqlite-flight/README.md` SHALL document pulling and running the GHCR image, and SHALL describe
availability truthfully — it SHALL NOT claim an image is published before any publish has occurred,
and SHALL NOT present a specific version tag as pullable unless it has been published. The docs SHALL
state that, once the GHCR package is public, `docker pull` of the image requires no authentication,
and SHALL show the `docker run` invocation that serves Flight on `:8815` against a read-only mounted
data dir.

#### Scenario: README documents the unauthenticated pull and run
- **WHEN** `cqlite-flight/README.md` is read after this change
- **THEN** it shows `docker pull ghcr.io/<owner>/cqlite-flight:<tag>` and states it needs no `docker login` once the package is public
- **AND** it shows a `docker run -p 8815:8815 -v <data>:<data>:ro ghcr.io/<owner>/cqlite-flight … --data-dir <data> --listen 0.0.0.0:8815` invocation

#### Scenario: README does not overstate availability
- **WHEN** the README's container section is read
- **THEN** it does not assert in the present tense that the image "is published" independent of a tag/dispatch run having occurred
- **AND** it does not imply a specific version tag is already pullable when no image has been published
