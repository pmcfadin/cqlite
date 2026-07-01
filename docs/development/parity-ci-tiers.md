# Parity CI Tier Contracts

> Source-of-truth contract for the five Cassandra parity CI tiers (epic #966 →
> #974, issue #1022). Defines what each tier *promises*: purpose, accepted
> evidence, skip/failure policy, artifact retention, and promotion rules.
>
> This contract is the reference that downstream gate work (#1023 claim lint,
> #1024 gate hardening, #1025 nightly Docker, #1026 exhaustive regen) builds on.
> See also the [release checklist](./parity-release-checklist.md) and the
> [manifest reference](./cassandra-parity-manifest.md).
>
> **Doctrine cross-link:** this contract sits beside the
> [gate contract](https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/)
> on the agent developer site; the local site mirror
> (`website/src/content/docs/agents-developing/gate-contract.md`) carries a
> "Parity CI tier contracts" section pointing back here.

## The documented tier enum (machine-parseable)

The cross-check (`cargo run -p cassandra-parity -- tier-contract-check`) parses
the fenced block below as **the documented enum**. It MUST equal
`enums::CI_TIER` in `tools/cassandra-parity/src/enums.rs` and the `ci.tier`
enum in `test-data/cassandra-parity-manifest.schema.json`. Keep the block exact:
one tier name per line, lowercase `snake_case`, no surrounding prose. Do not add
list markers or commentary inside the fence.

```parity-ci-tiers
fast_pr
required_parity
nightly_docker
exhaustive_regeneration
manual_debug
```

## Gate-strength classification

Every gate has a *strength* that bounds what it can prove. Strengths map to the
`evidence.type` values already defined in the manifest schema:

| Gate strength       | `evidence.type`       | What it proves |
|---------------------|-----------------------|----------------|
| smoke               | `smoke`               | CQLite parses/loads the artifact without error. **Not** byte parity and **not** value parity. |
| canonical-semantic  | `canonical_semantic`  | Decoded values match Cassandra after a documented normalization (e.g. JSONL goldens). Proves *value* parity, not byte layout. |
| byte-for-byte       | `byte_for_byte`       | CQLite output is byte-identical to Cassandra's (bytes/offsets/checksums/component files). The strongest claim. |

The remaining two `evidence.type` values are **non-proving** and cannot back a
parity claim on their own:

- `partial` — partial coverage; requires `known_limitations` plus `scope.gap`
  and `scope.next_step` in the manifest.
- `out_of_scope` — explicitly excluded; requires a rationale, boundary, and a
  safe-claim statement in the manifest.

**P0 data-loss rule:** smoke evidence alone CANNOT satisfy a `P0` /
`p0_data_loss` scenario. Such a scenario must either carry canonical-semantic or
byte-for-byte evidence, or record an explicit `scope.gap` acknowledging the
missing proof (enforced by `cassandra-parity lint`). A green smoke gate over a
P0 data-loss path without a recorded gap is a contract violation.

## Tier contracts

### `fast_pr`

- **Purpose.** The cheap, always-on PR gate. Static and structural checks that
  run on every pull request with no heavy dependencies (no Docker, no live
  Cassandra, no downloaded dataset binaries). The tier-contract cross-check
  itself runs here.
- **Allowed `evidence.type`.** `smoke`, `partial`, `out_of_scope`. (A scenario
  may *also* have stronger evidence proven in a higher tier; `fast_pr` only
  asserts the cheap checks.)
- **Skip policy.** Must not skip on PRs that touch parity surfaces. Path-filtered
  workflows may legitimately not trigger when no parity file changed; that is a
  non-trigger, not a skip.
- **Failure policy.** Blocking. A red `fast_pr` check blocks the PR.
- **Artifact retention.** Logs only; default CI retention. No fixtures produced.
- **Promotion.** A `fast_pr` scenario is promoted to `required_parity` once a
  deterministic, dataset-backed comparison (canonical-semantic or byte-for-byte)
  exists and is wired into a named workflow. Record the workflow path in
  `ci.workflow` when promoting.

### `required_parity`

- **Purpose.** The blocking parity gate on PRs and `main`: deterministic
  comparisons against committed reference goldens (JSONL, TOC, digests, byte
  fixtures) that run without spinning up Cassandra.
- **Allowed `evidence.type`.** `canonical_semantic`, `byte_for_byte` (the
  proving strengths). `partial` is permitted only with a recorded `scope.gap`
  and `scope.next_step`.
- **Skip policy.** Must not skip on the release commit. A skipped
  `required_parity` is treated as a failure for release purposes (see the
  release checklist).
- **Failure policy.** Blocking. Every `required_parity` scenario MUST name a
  workflow (`ci.workflow`); a missing workflow is a lint error. The named
  workflow MUST also *actually run* the scenario's mapped test in a step that
  **can fail the build** and is **fail-closed**. This is machine-enforced by
  `cassandra-parity lint` (issue #1228): it parses the named workflow YAML into
  its jobs → steps structure and fails the lint unless SOME step:
  1. invokes a mapped `cqlite.coverage.tests` target as `--test <name>` in a real
     test RUN — a `--no-run` (compile-only) invocation or a commented-out token
     does NOT count — or, for a JVM harness scenario, invokes `gradle`;
  2. can fail the build: either the step is not `continue-on-error: true`, or it
     is a `continue-on-error` step whose recorded `steps.<id>.outcome` is checked
     by a later BLOCKING aggregator step that runs `exit 1` (the standard
     run-then-aggregate fail-closed pattern); and
  3. is fail-closed — it arms `CQLITE_REQUIRE_FIXTURES` /
     `CQLITE_PARITY_REQUIRE_DATASETS` at the step, job, or workflow level — so a
     vanished/unfetched dataset PANICS instead of silently green-passing.

  So a `required_parity` claim can never point at a workflow that does not
  exercise it (e.g. a manifest-lint-only workflow), nor at one that runs the test
  only in a non-blocking informational step or only as a compile-only `--no-run`.
- **Artifact retention.** On failure, retain the diff/failure artifacts named in
  `evidence.failure_artifacts` long enough to triage (>= 14 days recommended).
- **Promotion.** A `canonical_semantic` `required_parity` scenario is promoted
  to `byte_for_byte` once byte/offset/checksum fixtures and a strict comparison
  command exist. Heavy regeneration of those fixtures moves to `nightly_docker`
  or `exhaustive_regeneration`.

### `nightly_docker`

- **Purpose.** Scheduled (nightly) verification that regenerates or re-validates
  fixtures inside a real Cassandra Docker image — catching drift the committed
  goldens cannot (version bumps, environment differences).
- **Allowed `evidence.type`.** `canonical_semantic`, `byte_for_byte`.
- **Skip policy.** May skip on ordinary PRs (it is scheduled, not per-PR). It
  must run on its schedule; a chronically skipped/disabled nightly invalidates
  the "recent nightly pass" release requirement.
- **Failure policy.** Non-blocking for in-flight PRs, but a failure files/updates
  a tracking issue and blocks release until resolved (see release checklist). That
  filing/updating is implemented by
  [`.github/workflows/parity-failure-issue.yml`](../../.github/workflows/parity-failure-issue.yml)
  (issue #1028): a `workflow_run`-triggered, non-gating automation that, when a lane
  concludes `failure` on a scheduled/main run, computes a stable per-scenario
  fingerprint (`scripts/parity-failure-issue.py`) and creates or updates one
  deduplicated `parity-failure` issue per fingerprint. It never gates the parity result
  and never auto-closes; a subsequent green run posts a resolution comment only.
- **Artifact retention.** Retain regenerated fixtures and logs for the comparison
  window (>= 30 days recommended) so a release can cite a recent pass.
- **Promotion.** Scenarios do not "promote" out of `nightly_docker`; rather, a
  `required_parity` scenario whose fixtures need a live Cassandra to regenerate
  is *attached* to the nightly so its goldens stay fresh.

### `exhaustive_regeneration`

- **Purpose.** The full, expensive regeneration of the entire fixture corpus
  across the storage-format matrix (`nb`/`oa`/`da`/`big`/`bti`), run for release
  candidates and major format work — the broadest proof the program offers.
- **Allowed `evidence.type`.** `byte_for_byte`, `canonical_semantic`.
- **Skip policy.** Not run per-PR. Required for release candidates; skipping it
  for an RC means broad parity claims cannot ship (see release checklist).
- **Failure policy.** Blocking for release candidates. A failure blocks the RC
  until the corpus regenerates cleanly.
- **Artifact retention.** Retain the full regenerated corpus + logs for the RC's
  lifetime (>= 90 days recommended) as the citable evidence behind the release
  claim.
- **Promotion.** Terminal tier — it is the strongest, broadest gate. New
  format-matrix scenarios are *added* here once their generation command exists.
- **Backing lane (issue #1026).** This tier is realized by
  [`.github/workflows/exhaustive-regeneration.yml`](../../.github/workflows/exhaustive-regeneration.yml)
  — a `workflow_dispatch` + weekly-cron lane (never on PRs) that orchestrates the
  existing generators (`regenerate-datasets.sh`, `generate-deltas.sh`,
  `generate-corruption-corpus.sh`), records a per-run provenance record (Cassandra
  version/ref/sha, Docker image, generator commands, dataset asset name + SHA256),
  and runs the corpus audit:
  `cargo run -p cassandra-parity -- corpus-audit --corpus . --manifest <manifest> --provenance <record>`.
  The audit **hard-fails** (non-zero exit, naming the offender) on a missing/stale
  manifest reference, an unclassified high-relevance Cassandra file, an unexpected
  component presence/checksum change, a provenance/manifest version divergence, or a
  corruption-fixture coverage gap. The lane uploads ONE report artifact (provenance +
  audit report + generator logs) and never commits regenerated binaries or publishes a
  dataset asset.

### `manual_debug`

- **Purpose.** Investigative, human-run scenarios used to triage a specific
  failure or explore a format question. Not an automated gate.
- **Allowed `evidence.type`.** Any, including `partial` and `out_of_scope`,
  because the tier itself proves nothing automatically.
- **Skip policy.** Always "skipped" in automation — it never runs in CI by
  design.
- **Failure policy.** Non-gating. Findings feed back into a stronger tier or an
  issue; they never block a PR or a release on their own.
- **Artifact retention.** Ad hoc; attach artifacts to the relevant issue.
- **Promotion.** A `manual_debug` scenario is promoted to `fast_pr` or
  `required_parity` once it is made deterministic and dataset-free (for
  `fast_pr`) or backed by committed goldens (for `required_parity`).

## Derived-report staleness: the merge-race hazard and its safeguard (issue #1338)

`docs/reports/cassandra-test-parity.md` is a **committed derived artifact** —
rendered from `test-data/cassandra-parity-manifest.yml` by
`cargo run -p cassandra-parity -- report`. Every PR that edits the manifest also
regenerates the report in the same commit, and the `parity-manifest` workflow's
`Report is not stale` step (`report ... --check`) blocks any PR whose report drifts
from a fresh render. Authors regenerate correctly per PR.

Even so, the report can go **stale on `main`** via a **semantic merge race** that no
per-PR check can catch:

1. PR A and PR C both change manifest scenario counts; each renders the report
   against its base and is green on its own `--check`.
2. C merges to `main`. A merges next — git sees no textual conflict (different
   lines), so it keeps A's report, which was rendered *without* C's manifest
   entries. `main` now has a report matching neither branch's view → stale.
3. GitHub does not require a branch to be up to date with `main` before merging, so
   a report regenerated against a stale base regresses silently on merge.

Because the staleness exists **only post-merge**, a per-PR `--check` is structurally
incapable of catching it, and a single stale report blocks the **entire** PR queue
(PR CI merges against `main`'s tip).

**Safeguards (two layers):**

- **Post-merge self-healing.** `.github/workflows/cassandra-parity.yml` has a
  `parity-report-heal` job that runs only on push to `main`: when `report --check`
  is stale, it regenerates the report and opens (or force-updates) a single
  regeneration PR from the fixed bot branch `auto/parity-report-regen` — it never
  pushes to protected `main` directly. The PR touches only the report; merging it
  makes `--check` green on the new tip, terminating the cycle. **The heal job
  authenticates with a dedicated PAT/GitHub-App token (repo secret
  `PARITY_HEAL_TOKEN`, `contents` + `pull-requests` write)** — a PR opened by the
  default `GITHUB_TOKEN` does not trigger `pull_request` CI (GitHub's recursion
  guard), so it would land with no checks. When the secret is absent the job SKIPs
  with a `::notice::` (and the report must be regenerated manually) rather than
  opening a check-less PR; provision the secret to enable full self-healing. The
  existing `parity-manifest` `--check` step stays as the detector (and a plain
  failing gate on PRs). If self-healing proves noisy, the documented fallback is to
  require
  branches be up to date before merge (a branch-protection/merge-queue toggle),
  which defeats the race by forcing a re-render against tip.
- **Local/gate coverage of the single-PR case.** `scripts/agent-gate.sh` includes a
  SKIP-aware `parity-report` component that runs the same `--check` so a forgotten
  single-PR regeneration is caught locally, before push — the layer the post-merge
  healer does not cover.

## Promotion ladder (summary)

```
manual_debug ──▶ fast_pr ──▶ required_parity ──▶ nightly_docker / exhaustive_regeneration
 (investigate)   (cheap,      (committed-golden    (live-Cassandra regen; RC-wide
                  static)      comparison)          corpus regeneration)
```

A scenario is only as strong as the evidence backing it: moving up the ladder
requires upgrading the `evidence.type` to a proving strength
(`canonical_semantic` or `byte_for_byte`) and recording the gate's workflow.
