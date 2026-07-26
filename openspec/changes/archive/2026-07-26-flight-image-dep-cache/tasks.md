# Tasks: flight-image-dep-cache (issue #2870)

## 1. Lockfile

- [ ] 1.1 Remove `Cargo.lock` from `.gitignore` (line 3) and `git add` the lockfile.
      **Surface exercised:** a fresh clone resolves the same dependency graph.
- [ ] 1.2 Verify the committed lockfile is consistent with every workspace manifest
      (`cargo metadata --locked` succeeds) before committing.
- [ ] 1.3 Note the contributor-visible policy change in the PR body: the lockfile is now tracked, and
      manifest edits must be accompanied by a refreshed lock.

## 2. Dockerfile

- [ ] 2.1 Add a `chef` stage on `rust:1.97.1-bookworm` installing `cargo-chef --locked`.
- [ ] 2.2 Add a `planner` stage: `COPY . .` then `cargo chef prepare --recipe-path recipe.json`.
- [ ] 2.3 Add a `builder` stage: `COPY --from=planner /src/recipe.json`, then
      `cargo chef cook --release -p cqlite-flight --features observability --recipe-path recipe.json`,
      then `COPY . .` and the final `cargo build --release --locked -p cqlite-flight --features observability`.
- [ ] 2.4 Keep the runtime stage last and otherwise unchanged; update only the `COPY --from=` stage name.
      **Surface exercised:** an untargeted `docker build` still yields the runtime image
      (`trino-connector/docker/docker-compose.yml:63-65` builds with no `--target`).
- [ ] 2.5 Confirm cook and build flags are character-for-character equivalent in profile, `-p`, and
      `--features` (design D3 — a mismatch makes the cache a silent no-op).

## 3. CI wiring

- [ ] 3.1 `flight-image.yml` (`:112-118`): add `cache-from: type=gha,scope=flight-<arch>` and
      `cache-to: type=gha,mode=max,scope=flight-<arch>`, deriving `<arch>` from the matrix platform.
- [ ] 3.2 Apply the same to the `docker/build-push-action@v6` step in `flight-ci.yml`.
- [ ] 3.3 Verify the cache config applies on BOTH the `v*` tag path and the `workflow_dispatch`
      round/rc path.
- [ ] 3.4 Confirm the #2803 guard conditions on the merge and smoke jobs and the digest-pin flow are
      untouched.

## 4. Evidence

- [ ] 4.1 Measure a cold build (cache dropped) for one architecture; record the duration.
- [ ] 4.2 Measure a warm rebuild after a source-only edit; record the duration.
- [ ] 4.3 Capture the log excerpt proving the warm rebuild emitted no `Compiling` line for any
      third-party dependency — the wall-clock delta alone is NOT sufficient evidence (design "Risk").
- [ ] 4.4 Post cold time, warm time, and the excerpt in the PR body.

## 5. Docs

- [ ] 5.1 Extend the #1990 lockstep checklist in `docs/development/ci-toolchain-policy.md` to enumerate
      every `FROM rust:<version>` line in `cqlite-flight/Dockerfile`.
- [ ] 5.2 Record the tracked-lockfile policy where contributors will see it.

## 6. Follow-up (do NOT fold into this change)

- [ ] 6.1 File the lockfile-keyed cache-hygiene issue: 8 workflows key on `hashFiles('**/Cargo.lock')`
      (`coverage.yml:73`, `quality-gates.yml:50`, `delta-roundtrip.yml:88`, `compaction-parity.yml:132`,
      `soak-resource-leak.yml:53`, `docs-site.yml:138`, `coverage-baseline.yml:45`,
      `live-cell-compaction-parity.yml:108`). Committing the lockfile repairs them automatically but
      invalidates each once; the key-hygiene review is its own scope.

## 7. Quality stages

- [ ] 7.1 `--lite` green each fix round (summary-file redirect).
- [ ] 7.2 `rust-reviewer` + roborev on the lite-green diff, BEFORE the full gate (review-first).
      Note the GHA-injection lint class applies to the workflow edits.
- [ ] 7.3 Open the PR; hand the endgame to `flow-closer`.
- [ ] 7.4 ONE full `scripts/agent-gate.sh` of record — serialized, and per the #2751 workaround run
      WITHOUT `AGENT_GATE_SUMMARY_FILE`, reading `<worktree>/.agent-gate-summary.txt`.
- [ ] 7.5 `spec-auditor` (C) anchored to `openspec/changes/flight-image-dep-cache/specs/**`.
- [ ] 7.6 Final roborev pass → merge-on-green → `flow-finalize`.
