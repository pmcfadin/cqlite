# Parity Release Checklist

> Run this checklist **before publishing any broad public Cassandra parity
> claim** (release notes, README, blog, talk). It gates the claim on
> demonstrably-green parity gates. Copy it into the release issue and check every
> box on the **release commit / RC tag**, not on a stale branch.
>
> Tier definitions live in the [parity CI tier contracts](./parity-ci-tiers.md).
> Manifest mechanics live in the [manifest reference](./cassandra-parity-manifest.md).
> When a gate is red, the forensic bundle for the failed scenario is described in
> the [parity failure-artifacts reference](./parity-failure-artifacts.md).

## Required green gates

A broad parity claim MUST NOT ship unless **all** of the following hold on the
exact release commit:

- [ ] **Manifest lint green.** `cargo run -p cassandra-parity -- lint` exits 0 on
      the release commit (schema + cross-field parity rules pass).
- [ ] **Tier contract intact.** `cargo run -p cassandra-parity -- tier-contract-check`
      exits 0 — the documented tier enum, the schema enum, and `enums::CI_TIER`
      agree, and every manifest `ci.tier` is a documented tier.
- [ ] **`required_parity` green on the release commit.** The `required_parity`
      workflow(s) passed on the commit being released — not a parent, not a
      retried-after-edit run. A skipped `required_parity` counts as **not green**.
- [ ] **Recent `nightly_docker` pass.** A `nightly_docker` run passed within the
      release window (within the retention window in the tier contract; a stale or
      disabled nightly does not satisfy this item).
- [ ] **Recent `exhaustive_regeneration` pass (release candidates).** For any RC
      or major format change, an `exhaustive_regeneration` run over the full
      storage-format matrix passed within the release window. This is the
      [`exhaustive-regeneration.yml`](../../.github/workflows/exhaustive-regeneration.yml)
      lane (weekly + `workflow_dispatch`, issue #1026); cite the run URL and its
      uploaded report artifact (provenance record + corpus-audit report). The audit
      hard-fails on any corpus/manifest or provenance divergence, so a green run is
      the citable evidence.

## Triaging a near-release red gate

When any required gate above is red on the release commit, do not ship the claim.
Triage from the **failure bundle keyed by scenario id** (issue #1027):

- [ ] **Locate the failure bundle.** The red lane uploads a single
      `parity-failures-<workflow>` artifact whose subdirectories are the failed
      `cass.*` scenario ids: `parity-failures/<tier>/<scenario_id>/`. Cite the
      failing scenario id in the release-blocking note.
- [ ] **Read the record.** Open `failure-artifact.json` for the scenario — e.g. a
      red byte gate for scenario `cass.X.Y` yields
      `parity-failures/required_parity/cass.X.Y/failure-artifact.json` with the
      Cassandra version/git-sha, dataset SHA256, `diffs[]` (byte/offset/checksum
      or `jsonl_diff`), and the `repro/` command. Bundle contents per
      `evidence_type` are documented in the
      [parity failure-artifacts reference](./parity-failure-artifacts.md).
- [ ] **Reproduce before re-claiming.** Run `repro/command.sh` from the bundle to
      confirm the failure (or the fix) locally before re-running the gate on the
      release commit.

## Claim-wording gate

- [ ] **No unqualified "same tests as Cassandra" claims.** Reject any wording
      that asserts CQLite runs the *same tests* as Apache Cassandra, or implies
      blanket byte-for-byte parity. Parity claims MUST be scoped to the gate
      strength that actually backs them (smoke / canonical-semantic /
      byte-for-byte per the [tier contracts](./parity-ci-tiers.md)) and to the
      capabilities and storage formats covered by the manifest. A `smoke`-only
      capability MUST NOT be described as "verified parity".

## Evidence sources (link these in the release notes)

- [ ] [Cassandra test index](../cassandra_test_index.md) — the inventory of
      Cassandra tests/files the program tracks.
- [ ] [Cassandra test parity assessment](../reports/cassandra-test-parity-assessment.md)
      — the relevance/coverage assessment behind the manifest.
- [ ] [Generated parity report](../reports/cassandra-test-parity.md) — the report
      rendered from the manifest (`cargo run -p cassandra-parity -- report`); confirm
      it is not stale (`report ... --check` exits 0).

## Sign-off

- [ ] Release manager has confirmed every box above on the release commit and
      linked the three evidence sources in the published claim.
