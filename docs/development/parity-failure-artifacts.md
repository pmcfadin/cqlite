# Parity Failure Artifacts

> Source-of-truth reference for the uniform Cassandra-parity **failure-artifact
> record** and the scenario-id-keyed **failure bundle** every parity lane emits
> on a red gate (issue #1027). It sits beside the
> [parity CI tier contracts](./parity-ci-tiers.md), the
> [release checklist](./parity-release-checklist.md), and the
> [manifest reference](./cassandra-parity-manifest.md).
>
> **Doctrine cross-link:** this reference sits beside the
> [gate contract](https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/)
> on the agent-developer site. The canonical copy lives here under
> `docs/development/`; the `agents-developing/` website mirror is maintained
> separately (issue #1022) and should point back to this page.

## Why this exists

Before this contract, each parity workflow uploaded a differently-named artifact
with an ad-hoc path glob and retention, none keyed to the manifest scenario id,
and the Rust `required_parity` byte/offset/checksum/JSONL checks emitted only a
panic string. Triage meant reverse-engineering each lane's layout.

This contract defines **one** forensic record shape and **one** on-disk layout so
that a red gate maps mechanically to its `cass.*` scenario, and every surface
(the Rust parity tests in `cqlite-core`, the `cassandra-parity` tool, and the
compaction Java harness) produces the identical shape.

## The join key

The unit that ties the failure record, its on-disk location, and the manifest
together is the **manifest scenario id** (`cass.<capability>.<name>`) plus its
`ci.tier` and `evidence.type`. Everything hangs off that key — there is no new
identifier.

## The failure-artifact record

Each failed scenario emits one `failure-artifact.json` conforming to
[`test-data/parity-failure-artifact.schema.json`](../../test-data/parity-failure-artifact.schema.json).
Its `tier` and `evidence_type` enums are the **same closed sets** as the parity
manifest schema (`test-data/cassandra-parity-manifest.schema.json`).

Required top-level fields:

| Field                | Meaning |
|----------------------|---------|
| `schema_version`     | Record schema version (currently `1`). |
| `scenario_id`        | Failing manifest scenario id — the join key back to the manifest. |
| `lane`               | The emitting workflow file, e.g. `sstabledump-parity-gate.yml`. |
| `tier`               | The manifest `ci.tier` for the scenario (enforced enum). |
| `evidence_type`      | The manifest `evidence.type` (enforced enum) — determines the required `diffs/` contents. |
| `artifacts_compared` | What was compared, e.g. `["bytes", "offsets", "checksums", "component_files"]`. |
| `provenance`         | Full reproduction context (see below). |
| `diffs[]`            | Typed pointers into the bundle (`{kind, path}`). |
| `repro_bundle`       | Pointer to the `repro/` directory. |

### `provenance`

The `provenance` object records the full reproduction context: the Cassandra
version, the Cassandra source ref/git-sha (comparable to the manifest
`cassandra_source` pin), the dataset SHA256, the fixture path, the component
list, the exact `command_line` that was run, and relative pointers to captured
`stdout`/`stderr`.

### `diffs[].kind` enum

Each `diffs[]` entry is `{kind, path}` where `path` resolves inside the bundle.
`kind` is one of:

`byte_diff`, `offset_diff`, `checksum_diff`, `jsonl_diff`, `component_inventory`,
`live_log`, `audit_report`.

## Bundle layout (keyed by scenario id)

```
<root>/parity-failures/<tier>/<scenario_id>/
  failure-artifact.json        # the record above
  stdout.txt / stderr.txt      # captured output
  diffs/                       # contents depend on evidence_type (see below)
  repro/
    inputs/                    # fixture paths + dataset SHA256 (never the full dataset copy)
    command.sh                 # exact reproduction command line
    INSTRUCTIONS.md            # how to reproduce locally
```

The directory name is the manifest `scenario_id`, so a red gate maps mechanically
to its `cass.*` scenario. A **passing** scenario writes no bundle.

### Per-`evidence_type` `diffs/` contents

- **`byte_for_byte`** — for each compared component: `<component>.byte-diff.txt`
  (first differing byte + hex window), `<component>.offset-diff.txt` (offset
  table), `checksums.txt` (SHA-256 per component, both engines), and
  `component_inventory.txt` (expected vs actual component set). The record's
  `diffs[]` carries `byte_diff`, `offset_diff`, `checksum_diff`, and
  `component_inventory` entries.
- **`canonical_semantic`** — `jsonl.diff` (normalized diff) plus BOTH raw source
  files: `reference.jsonl` (Cassandra) and `candidate.jsonl` (CQLite). The
  record's `diffs[]` carries a `jsonl_diff` entry.
- **`smoke`** — `load.log` (the parse/load attempt output).
- **`partial`** — `gap.txt` echoing the manifest `scope.gap` / `scope.next_step`.

The `nightly_docker` lane additionally captures the failing comparison's
stdout/stderr as a `live_log` diff entry; the `exhaustive_regeneration` lane
carries an `audit_report` diff entry (the corpus-audit report).

## The reproduction bundle

`repro/` lets a maintainer rerun the failing check without the full dataset:

- `command.sh` — the exact comparison command line.
- `INSTRUCTIONS.md` — how to reproduce locally.
- `inputs/` — the fixture(s) identified by **path plus dataset SHA256** (per the
  owner decision, no permanently-stored full dataset copy). The record's
  `repro_bundle` field points here.

## Manifest artifact descriptors

A scenario declares which failure artifacts it produces via typed descriptors of
the form `artifact.<tier>.<kind>` in its manifest `evidence` block (replacing the
old free-text `evidence.failure_artifacts` strings):

| Descriptor id                                    | Tier                      | Points at |
|--------------------------------------------------|---------------------------|-----------|
| `artifact.required_parity.byte_diff`             | `required_parity`         | `diffs[].kind = byte_diff` |
| `artifact.required_parity.offset_diff`           | `required_parity`         | `diffs[].kind = offset_diff` |
| `artifact.required_parity.checksum_diff`         | `required_parity`         | `diffs[].kind = checksum_diff` |
| `artifact.nightly_docker.live_logs`              | `nightly_docker`          | `diffs[].kind = live_log` |
| `artifact.exhaustive_regeneration.audit_report`  | `exhaustive_regeneration` | `diffs[].kind = audit_report` |
| `artifact.manual_debug.reproduction_bundle`      | `manual_debug`            | `repro_bundle` |

A descriptor's `<tier>` segment MUST equal the scenario's `ci.tier`, and its
`<kind>` MUST be a diff/bundle kind the scenario's `evidence_type` is allowed to
emit. `cassandra-parity lint` enforces both rules.

## Workflow upload + retention

Every parity workflow uploads the whole `parity-failures/**` tree as a
uniformly-named artifact `parity-failures-<workflow-basename>`, on failure
(`if: always()`), so a red run yields exactly one predictably-named artifact
whose subdirectories are the failed scenario ids. Each lane's `retention-days`
meets the **strictest** tier minimum among the scenarios it gates:

| Tier                      | Minimum `retention-days` |
|---------------------------|--------------------------|
| `required_parity`         | 14 |
| `nightly_docker`          | 30 |
| `exhaustive_regeneration` | 90 |

`fast_pr` and `manual_debug` have no minimum (logs only / attach to issue). The
authoritative table (with rationale + the machine-parseable
`parity-retention-minimums` block) lives in
[parity-ci-tiers.md](./parity-ci-tiers.md#artifact-retention-policy-enforced-minimums).

**Enforcement.** `cargo run -p cassandra-parity -- retention-check` groups
manifest scenarios by their `ci.workflow`, reads each referenced workflow's
`upload-artifact` `retention-days`, and fails if any is below the strictest tier
minimum it gates. It runs in CI in `.github/workflows/cassandra-parity.yml`
alongside `lint` and `tier-contract-check`, so the policy is fail-closed, not
merely documented.
