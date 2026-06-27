# Parity Release Checklist

> Run this checklist **before publishing any broad public Cassandra parity
> claim** (release notes, README, blog, talk). It gates the claim on
> demonstrably-green parity gates. Copy it into the release issue and check every
> box on the **release commit / RC tag**, not on a stale branch.
>
> Tier definitions live in the [parity CI tier contracts](./parity-ci-tiers.md).
> Manifest mechanics live in the [manifest reference](./cassandra-parity-manifest.md).

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
      storage-format matrix passed within the release window.

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
