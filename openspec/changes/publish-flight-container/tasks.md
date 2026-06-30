# Tasks — publish-flight-container

## 1. Post-publish container smoke-test (`.github/workflows/flight-image.yml`)
- [ ] 1.1 Add a per-architecture smoke step/job that runs after the manifest is pushed, on the
      native runner for each arch (`ubuntu-latest` for amd64, `ubuntu-24.04-arm` for arm64).
      Surface exercised: the published manifest `ghcr.io/<owner>/cqlite-flight:<tag>` (pull) and the
      container `ENTRYPOINT` (run).
- [ ] 1.2 In each smoke run: pull the published tag, `docker run -d` the image with a throwaway
      empty `--data-dir` and `--listen 0.0.0.0:8815`, publish/forward `:8815`, then poll a TCP
      connect to `127.0.0.1:8815` with a bounded timeout (~30s); fail non-zero on timeout; tear the
      container down in all paths.
- [ ] 1.3 Ensure the smoke runs only inside the existing tag/`workflow_dispatch` publish path (never
      on branch pushes or PRs) and does not weaken the existing `latest`/`X.Y`/`vX.Y.Z` tag logic.

## 2. README accuracy + unauthenticated pull/run (`cqlite-flight/README.md`)
- [ ] 2.1 Correct the present-tense "is published … on every release tag" wording and the
      `:v0.12.0`-pullable implication to reflect that publishing happens on a `v*` tag or a manual
      dispatch (no image is in GHCR until the first run).
- [ ] 2.2 Make the **unauthenticated** pull explicit: `docker pull ghcr.io/<owner>/cqlite-flight:<tag>`
      needs no `docker login` once the package is public; keep the existing read-only `docker run`
      quickstart that serves `:8815`.

## 3. Validation
- [ ] 3.1 `openspec validate publish-flight-container --strict` is clean.
- [ ] 3.2 `scripts/agent-gate.sh` — run for the SKIP-aware summary; expected to show no Rust deltas
      (this change touches only the workflow + README). Paste the AGENT-GATE SUMMARY verbatim.
- [ ] 3.3 Workflow proof: a `workflow_dispatch` run with `image_tag: dev` goes green **including the
      new smoke step** on both arches (this is the wiring-evidence for Requirement 2; it is also the
      recommended first post-merge action). Note: requires the smoke step to be on the default branch
      to be dispatchable, so this proof runs post-merge.
- [ ] 3.4 C — `spec-auditor` anchored to `openspec/changes/publish-flight-container/specs/**`:
      every requirement `satisfied` with its evidence (workflow structure for Req 1–2, README text
      for Req 3). PASS required before archive.
- [ ] 3.5 roborev (`/roborev-review-branch --base origin/main`) clean.

## Owner actions (Non-goals — surfaced, not done by this change)
- [ ] O.1 Trigger the first publish: run `flight-image.yml` via *Run workflow* (`image_tag: dev`),
      or cut the next `v*` tag.
- [ ] O.2 Make the GHCR `cqlite-flight` package **Public** (package → Settings → Change visibility)
      so anonymous `docker pull` works.
