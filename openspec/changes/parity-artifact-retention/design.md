# Design: parity-artifact-retention

## Context

Three things are out of alignment today and this change brings them into one model:

1. **Record contents.** No schema says what a failure bundle contains. The compaction Java harness has a
   good de-facto layout (`inputs/`, both engines' output, `commands.txt`, stdout/stderr, normalized
   JSONL, `checksums.txt`, `byte-diff.txt`); the Rust `required_parity` byte/offset/checksum/JSONL
   checks emit only a panic string.
2. **Where it lives + naming.** Each workflow uploads a differently-named artifact with a different path
   glob and retention; none is keyed by the manifest scenario id.
3. **Retention.** The tier contract recommends 14/30/90-day windows in prose; nothing enforces them.

The unit that ties all three together already exists: the manifest **scenario id** (`cass.<capability>.<name>`)
plus its `ci.tier` and `evidence.type`. The design hangs everything off that key.

## Recommended design (5–10 lines)

Define one **failure-artifact record** (`failure-artifact.json`) emitted per failed scenario, carrying
`{schema_version, scenario_id, lane, tier, evidence_type, artifacts_compared, provenance, diffs[],
repro_bundle}`. Persist it inside a **bundle directory keyed by scenario id**
(`<root>/parity-failures/<tier>/<scenario_id>/`) whose required contents are determined by `evidence_type`
(byte_for_byte → byte/offset/checksum diff + component inventory; canonical_semantic → normalized JSONL
diff + raw source JSONL; smoke → load log; partial → recorded gap). Every parity workflow uploads
`<root>/parity-failures/**` as a uniformly-named artifact (`parity-failures-<workflow>`) with a
`retention-days` that meets its tier minimum. The manifest's `evidence.failure_artifacts` strings are
replaced by typed **artifact descriptors** (`artifact.<tier>.<kind>`) that name the record path + kind,
and `cassandra-parity lint` validates both the descriptors and the lanes' retention. A small Rust
emitter in `tools/cassandra-parity` (or a shared lib used by the Rust parity tests + harness) writes the
record so all surfaces produce the identical shape.

**Why this wins:** it reuses the manifest scenario id as the single join key (no new identifier), it
formalizes the layout the compaction harness already proved out (low migration cost), and it makes the
existing free-text `failure_artifacts` field machine-checkable without adding a new top-level manifest
section.

## The uniform failure-artifact record schema

`failure-artifact.json` (one per failed scenario):

```jsonc
{
  "schema_version": 1,
  "scenario_id": "cass.compression_checksum.digest_crc32_byte_for_byte_parity",
  "lane": "sstabledump-parity-gate.yml",     // the emitting workflow file
  "tier": "required_parity",                 // manifest ci.tier (enforced enum)
  "evidence_type": "byte_for_byte",          // manifest evidence.type
  "artifacts_compared": ["bytes", "offsets", "checksums", "component_files"],
  "provenance": {
    "cassandra_version": "5.0.2",
    "cassandra_git_sha": "f278f6774fc76465c182041e081982105c3e7dbb",
    "dataset_sha256": "…",                   // SHA of the dataset asset the fixture came from
    "fixture_path": "test-data/datasets/sstables/…/nb-1-big-Data.db",
    "component_list": ["Data.db", "Index.db", "Statistics.db", "TOC.txt", "Digest.crc32"],
    "command_line": "CQLITE_DATASETS_ROOT=… cargo test -p cqlite-core --test … <name>",
    "stdout": "stdout.txt",                   // pointer, relative to the bundle dir
    "stderr": "stderr.txt"
  },
  "diffs": [                                  // pointers into the bundle, typed by what was compared
    { "kind": "byte_diff",     "path": "diffs/Data.db.byte-diff.txt" },
    { "kind": "offset_diff",   "path": "diffs/Data.db.offset-diff.txt" },
    { "kind": "checksum_diff", "path": "diffs/checksums.txt" }
  ],
  "repro_bundle": "repro/"                    // dir with inputs + command + instructions
}
```

`diffs[].kind` enum mirrors the manifest `evidence.artifacts` values plus the
`{byte,offset,checksum,jsonl}_diff` shapes: `byte_diff`, `offset_diff`, `checksum_diff`, `jsonl_diff`,
`component_inventory`, `live_log`, `audit_report`.

## Bundle layout + naming (keyed by manifest scenario id)

```
<root>/parity-failures/<tier>/<scenario_id>/
  failure-artifact.json        # the record above
  stdout.txt / stderr.txt
  diffs/                       # contents depend on evidence_type (see below)
  repro/
    inputs/                    # fixture(s) or a manifest of fixture paths + dataset SHA
    command.sh                 # exact reproduction command line
    INSTRUCTIONS.md            # how to reproduce locally
```

Per `evidence_type`, `diffs/` MUST contain:

- **byte_for_byte** — `<component>.byte-diff.txt` (first differing byte + hex window),
  `<component>.offset-diff.txt` (offset table), `checksums.txt` (SHA-256 per component, both engines),
  and `component_inventory.txt` (expected vs actual component set).
- **canonical_semantic** — `jsonl.diff` (normalized diff), `reference.jsonl` + `candidate.jsonl`
  (raw source JSONL preserved per the issue's acceptance criteria).
- **smoke** — `load.log` (the parse/load attempt output).
- **partial** — `gap.txt` echoing the manifest `scope.gap`/`scope.next_step`.

Workflows upload the whole tree as `parity-failures-<workflow-basename>` so a red run yields exactly one
predictably-named artifact whose subdirectories are the failed scenario ids.

## Manifest artifact descriptors

Replace the free-text `evidence.failure_artifacts: [string]` with typed descriptors. The issue lists the
intended ids:

| Descriptor id                                  | Tier                     | Record/diff kind it points at |
|------------------------------------------------|--------------------------|-------------------------------|
| `artifact.required_parity.byte_diff`           | required_parity          | `diffs[].kind = byte_diff`    |
| `artifact.required_parity.offset_diff`         | required_parity          | `diffs[].kind = offset_diff`  |
| `artifact.required_parity.checksum_diff`       | required_parity          | `diffs[].kind = checksum_diff`|
| `artifact.nightly_docker.live_logs`            | nightly_docker           | `diffs[].kind = live_log`     |
| `artifact.exhaustive_regeneration.audit_report`| exhaustive_regeneration  | `diffs[].kind = audit_report` |
| `artifact.manual_debug.reproduction_bundle`    | manual_debug             | `repro_bundle`                |

A descriptor's `<tier>` segment MUST equal the scenario's `ci.tier`, and its kind MUST be a value the
scenario's `evidence_type` is allowed to emit (byte/offset/checksum descriptors only on byte_for_byte
scenarios, etc.). `cassandra-parity lint` enforces this.

## Retention policy (single enforced source)

Promote the tier-contract recommendations to an enforced minimum table:

| Tier                      | Minimum `retention-days` | Rationale                                  |
|---------------------------|--------------------------|--------------------------------------------|
| fast_pr                   | default (logs only)      | no fixtures produced                       |
| required_parity           | 14                       | enough to triage a blocked PR              |
| nightly_docker            | 30                       | covers the "recent nightly pass" window    |
| exhaustive_regeneration   | 90                       | release-candidate citable evidence         |
| manual_debug              | n/a (attach to issue)    | ad hoc                                      |

A lint/audit check parses each parity workflow's `upload-artifact` step and fails if its `retention-days`
is below the minimum for the tier(s) of the scenarios it gates.

## Alternatives considered

1. **New top-level manifest `artifacts:` section** (parallel to `scenarios:`/`claims:`).
   Rejected: it duplicates the scenario↔tier↔evidence linkage that already lives on each scenario, and
   would need its own cross-field lint to stay in sync. Hanging descriptors off the existing
   `evidence` block keeps one source of truth.

2. **Keep `failure_artifacts` as free text; just standardize the workflow upload step.**
   Rejected: standardizing only the upload name/retention leaves the *contents* unstructured, so triage
   still depends on reading each lane's ad-hoc files. The issue's acceptance criteria explicitly require
   a structured per-failure metadata record.

3. **Bundle keyed by run id / timestamp instead of scenario id.**
   Rejected: a run-id key cannot be joined back to the manifest, defeating the "map a red gate to its
   `cass.*` scenario" goal. Scenario id is the stable join key the whole program already uses.

4. **Emit JUnit XML only and rely on the test-report viewer.**
   Rejected: JUnit captures pass/fail + a message but not byte/offset/checksum diffs, raw + normalized
   JSONL, or a reproduction bundle. It can coexist (workflows already upload JUnit) but cannot be the
   forensic record.

5. **A separate `parity-artifacts` crate vs. extending `tools/cassandra-parity`.**
   Recommend extending `tools/cassandra-parity` (it already owns the manifest model, enums, and lint), so
   the record schema, descriptor validation, and retention lint live next to the things they constrain.
   A standalone crate is deferred unless the emitter is needed from a context that cannot depend on the
   tool.

## OWNER DECISIONS (resolved 2026-06-30 — Seam 1 approval)

1. **Retention durations per tier.** ✅ Enforce the tier-contract values as **minimums**:
   `required_parity` 14d, `nightly_docker` 30d, `exhaustive_regeneration` 90d. A lane may set higher.

2. **Gate on artifact presence?** ✅ **Fail-closed.** A parity workflow MUST fail when a scenario
   reported a failure but did not emit a conforming `failure-artifact.json`. Triage forensics are
   guaranteed to exist; this matches CQLite's fail-closed gate doctrine. (Implementers: take care the
   emitter path is exercised so a missed emitter does not introduce flakiness.)

3. **Migration of existing `failure_artifacts` strings.** ✅ **Convert all ~119 existing free-text
   entries to typed descriptors in this change.** Lint goes green for the whole manifest at once; no
   mixed state, no follow-up debt. Larger diff is accepted.

4. **Repro bundle for byte fixtures.** ✅ Record fixture **paths + dataset SHA256 only** (no full
   dataset copy — permanent dataset storage is out of scope per the issue).

5. **Live-Cassandra logs scope (nightly_docker).** ✅ Capture the **failing comparison's
   stdout/stderr** as the `live_log` entry, not the full Cassandra container log (size vs. completeness
   trade resolved toward the failing comparison).
