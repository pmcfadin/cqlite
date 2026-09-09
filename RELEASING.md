# Releasing CQLite

How a CQLite release is cut, why it is **not atomic**, and how to resume a
release train that failed partway. Read this before pushing a `v*` tag.

Issue [#2652](https://github.com/pmcfadin/cqlite/issues/2652) (epic
[#2636](https://github.com/pmcfadin/cqlite/issues/2636)).

## What a release tag does

Pushing an annotated `v<version>` tag (e.g. `v0.15.0`) fans out to **7+
independent publish workflows**, each triggered by the same tag:

| Lane | Workflow | Target | Immutable? |
|------|----------|--------|:---------:|
| crates.io | `release.yml` (`publish-crate`) | `cqlite-core`, `cqlite-cli` | yes |
| PyPI | `python-release.yml` (`publish-pypi`) | `cqlite-py` | yes |
| npm | `node-release.yml` (`publish-npm`) | `@cqlite/node` | yes |
| Maven Central | `trino-publish.yml` (`publish`) | `in.mcfad:cqlite-trino` | yes |
| GHCR image | `flight-image.yml` | `ghcr.io/pmcfadin/cqlite-flight` | tags mutable, digests not |
| Homebrew tap | `release.yml` (`update-homebrew-tap`) | `pmcfadin/homebrew-cqlite` | git, re-runnable |
| GitHub releases | `release.yml` / `*-release.yml` | release assets | re-runnable |
| GitHub release asset | `trino-connector-fatjar.yml` | `cqlite-trino-<version>-all.jar` (+ `.sha256`) | re-runnable |

Every package registry above is **immutable**: once a version is published it can
**never** be overwritten or deleted-and-replaced.

## The version lives in four manifest fields

A release version is hand-maintained in **four** places, and every publish lane
reads its own:

1. `Cargo.toml` → `[package].version` (workspace-root package)
2. `Cargo.toml` → `[workspace.package].version` (inherited by every crate)
3. `bindings/python/pyproject.toml` → `[project].version`
4. `bindings/node/package.json` → `.version`

Edit them by hand and it is easy to ship a tag whose manifests disagree — one
lane publishes, a later lane hard-fails on its own private tag==manifest check,
and the immutable registries are left **half-populated**.

### Bump the version with `scripts/bump-version.sh`

Never hand-edit the four fields. Use the script — it rewrites all four together
(atomically: every file is validated in a staging copy before *any* file is
moved into place) and re-checks agreement:

```bash
scripts/bump-version.sh current          # print the agreed version
scripts/bump-version.sh check            # assert the four fields agree
scripts/bump-version.sh check v0.16.0    # also assert they equal a tag/version
scripts/bump-version.sh set 0.16.0       # rewrite all four to 0.16.0
```

`set`/`check` reject any non-semver version
(`^[0-9]+.[0-9]+.[0-9]+(-[0-9A-Za-z.-]+)?$`), so a typo or crafted value can
never be written into a manifest.

The **CI agreement check** runs `scripts/bump-version.sh check` on every PR
(via the Required PR Gate), so a commit that leaves the four manifests
disagreeing is caught before it ever becomes a release tag. The same command is
the shared **release preflight** (below).

## The shared preflight — gate before any lane publishes

`.github/workflows/release-preflight.yml` is a reusable
(`workflow_call`) workflow that runs `scripts/bump-version.sh check <version>`:
it asserts the tag equals **every** manifest field and fails closed on any
missing/empty field, any disagreement, or a tag/manifest mismatch.

Every registry publish lane invokes it as a `needs:` of its publish job, so **no
lane uploads anything until the tag has been proven to agree with all four
manifests**. A manifest disagreement therefore fails the train *up front*,
before any irreversible upload — instead of after a partial publish.

> The preflight makes the fan-out **fail-fast and consistent**. It does **not**
> make it **atomic** — see below.

## Why a release is NOT atomic (and cannot be)

The publish targets are separate, immutable, third-party registries
(crates.io, PyPI, npm, Maven Central, GHCR). There is no cross-registry
transaction: you cannot "commit" all seven at once, and you cannot roll back a
crate that already published because PyPI later failed. Atomicity across
registries is **out of scope and impossible** — the honest guarantees are:

- **fail-fast consistency** — the preflight blocks a train whose manifests
  disagree before anything publishes;
- **resumability** — a train that fails *after* some lanes published can be
  re-run and will **skip the already-published lanes** rather than hard-fail on
  a duplicate.

## Resuming a partially-failed train

If a lane fails after others have already published (a flaky runner, an expired
credential, a registry outage), **do not push a new tag and do not bump the
version**. The published artifacts are immutable and correct; you only need the
failed lanes to complete. Every registry lane is now **idempotent**:

| Lane | Resume behavior |
|------|-----------------|
| crates.io | `publish-crate` checks the crates.io API and **skips** any version already published (`cqlite-core`/`cqlite-cli`). |
| PyPI / TestPyPI | `gh-action-pypi-publish` runs with `skip-existing: true` — already-uploaded files (sdist + some wheels) are skipped. |
| npm | `publish-npm` runs `npm view @cqlite/node@<version>` first and **skips** the publish if that exact version already exists. |
| Maven Central | `trino-publish` probes `repo1.maven.org` for the POM and **skips** the publish if `in.mcfad:cqlite-trino:<version>` is already released. |
| GHCR image | `flight-image` republishes the same digests/tags; re-running is safe. |
| Trino fat jar (GitHub release asset) | `trino-connector-fatjar.yml` re-uploads `cqlite-trino-<version>-all.jar` + its `.sha256`, overwriting the same asset names on the same release. Release assets are **mutable**, so a re-run is safe and idempotent. |

So a re-run **resumes** rather than restarts: it re-runs to completion, publishing
only the lanes that had not yet succeeded.

> **The fat-jar lane is the one lane that is not immutable.** It writes GitHub
> release assets, not a package registry, so it can be re-run freely and can also
> **backfill an already-shipped version** — no new tag, no version bump. That is
> why it is the only lane whose resume story is "just run it again", and why a
> failure there never leaves a half-populated registry behind.

### How to re-run

1. **Re-run the failed workflow(s)** from the GitHub Actions UI (the
   tag-triggered run), or `gh run rerun --failed <run-id>`. Because every lane
   is idempotent, the already-published lanes no-op and the failed lane
   completes.
2. **npm / Maven backfill without re-tagging.** `node-release.yml` and
   `flight-image.yml` also expose a `workflow_dispatch` republish path (issue
   #2026 / #2117): supply the `version`/`publish_version` input; it publishes
   from the selected branch **without rewriting the git tag**, and refuses
   unless the `v<version>` tag already exists. `trino-publish.yml` accepts a
   manual `version` dispatch (`dry_run` defaults to **true** — a real Central
   publish requires `dry_run=false`, issue #2639).
3. **Verify** each registry shows `<version>` before announcing the release.

### Never do

- **Never** overwrite or delete a published version and re-publish. The
  registries are immutable; instead ship a new patch version.
- **Never** push a second tag to "fix" a partial train — re-run the lanes.
- **Never** hand-edit the four manifest fields — use `scripts/bump-version.sh`.

## Connector dev channel (`trino-connector-dev`)

`.github/workflows/trino-connector-fatjar.yml` builds the shaded connector jar
(`cqlite-trino-<version>-all.jar` + `.sha256`) and attaches it to a GitHub
Release. It has two triggers: `push` on `v*` tags (the normal release lane, above)
and `workflow_dispatch` with a `version` and a `channel` input — `channel`
defaults to **`dev`**.

The dev channel publishes to **one long-lived container tag, `trino-connector-dev`**,
which carries **many version-stamped assets** (`cqlite-trino-0.17.1-dev.1-all.jar`,
`cqlite-trino-0.18.0-dev.3-all.jar`, …) rather than one tag per build. So a
downstream consumer gets a stable URL for a pre-release jar:

```
https://github.com/pmcfadin/cqlite/releases/download/trino-connector-dev/cqlite-trino-<version>-all.jar
```

**Why `trino-connector-dev` is deliberately NOT a `v*` tag.** Every publish lane in
the fan-out table above triggers on `v*`. A dev tag shaped like a release tag would
therefore start the crates.io, PyPI, npm, Maven Central and GHCR lanes — against a
version that exists in no manifest. `trino-connector-dev` matches **zero**
release-train triggers, so it cannot start any of them; and it would never get past
the shared preflight anyway, since a dev version does not equal all four manifest
fields. It is also created with **`prerelease: true`**, so GitHub never promotes it
to the repository's "latest release".

Both dispatch shapes:

```bash
# Backfill the fat jar for an already-shipped release (the v0.17.0 tag exists;
# nothing is re-published to any registry, only a release asset is uploaded).
gh workflow run trino-connector-fatjar.yml -f version=0.17.0 -f channel=release --ref v0.17.0

# Publish a pre-release jar to the rolling dev channel.
gh workflow run trino-connector-fatjar.yml -f version=0.17.1-dev.1 -f channel=dev
```
