# Cassandra Parity Manifest — Schema Guide

The Cassandra parity manifest is the single source of truth for CQLite's
byte-for-byte parity claims against Apache Cassandra (epics
[#966](https://github.com/pmcfadin/cqlite/issues/966) /
[#967](https://github.com/pmcfadin/cqlite/issues/967)).

- Manifest: [`test-data/cassandra-parity-manifest.yml`](../../test-data/cassandra-parity-manifest.yml)
- Schema: [`test-data/cassandra-parity-manifest.schema.json`](../../test-data/cassandra-parity-manifest.schema.json)
- Taxonomy source: [`docs/reports/cassandra-test-parity-assessment.md`](../reports/cassandra-test-parity-assessment.md)
- Generated report: [`docs/reports/cassandra-test-parity.md`](../reports/cassandra-test-parity.md)

## Tooling

```bash
# Validate the manifest against the schema + cross-field parity rules
cargo run -p cassandra-parity -- lint --manifest test-data/cassandra-parity-manifest.yml

# Print coverage by capability / relevance and warn on unclassified high-relevance areas
cargo run -p cassandra-parity -- coverage --manifest test-data/cassandra-parity-manifest.yml

# Regenerate the public parity report from the manifest
cargo run -p cassandra-parity -- report \
  --manifest test-data/cassandra-parity-manifest.yml \
  --output docs/reports/cassandra-test-parity.md

# CI stale check: fail if the checked-in report differs from a fresh render
cargo run -p cassandra-parity -- report \
  --manifest test-data/cassandra-parity-manifest.yml \
  --output docs/reports/cassandra-test-parity.md --check
```

The tool needs no Docker, live Cassandra, or downloaded dataset binaries — it
only reads the manifest, the repository tree, and the assessment report.

## Top-level fields

| Field | Meaning |
|---|---|
| `manifest_version` | Always `1`. |
| `cassandra_source.repo` / `ref` / `sha` | Apache Cassandra repo, tag, and exact git SHA the fixtures trace to. |
| `cassandra_source.index` | Path to `docs/cassandra_test_index.md`. |
| `cassandra_source.assessment_report` | Path to the taxonomy source-of-truth. |
| `program.parent_epic` / `reporting_epic` | `966` / `967`. |
| `scenarios[]` | The parity scenarios (execution units, not 1:1 Cassandra test ports). |

## Scenario fields

Required: `id`, `title`, `status`, `capability`, `priority`, `risk`,
`cassandra`, `cqlite`, `fixtures`, `evidence`, `ci`, `scope`.

### Scenario IDs

Stable dotted identifiers: `cass.<capability_group>.<scenario_slug>`, e.g.
`cass.sstable_format.descriptor_component_resolution`. IDs must be unique and
match `^cass\.[a-z0-9_]+(\.[A-Za-z0-9_]+)+$`. IDs are an API: do not rename them
once published; deprecate instead.

### `status`

| Status | Meaning | Requires |
|---|---|---|
| `mirrored` | CQLite has real coverage. | `cqlite.coverage.tests` **or** `fixtures.references`. |
| `partial` | Some evidence; a named gap remains. | `scope.gap` + `scope.next_step`. |
| `planned` | Target named; evidence not yet claimed. | `scope.target_issue` **or** `scope.target_suite`. |
| `out_of_scope` | Intentionally not claimed. | see [out-of-scope](#out-of-scope-taxonomy). |

### `capability` (16 canonical groups)

`sstable_format`, `component_discovery`, `data_db_decode`, `index_summary`,
`statistics_metadata`, `compression_checksum`, `corruption_verify`,
`filter_db_bloom`, `cql_types`, `schema_evolution`, `tombstone_ttl`,
`delta_scan`, `compaction_merge`, `write_load_path`, `bti_big_version_matrix`,
`cli_reporting`. Free-text values are rejected.

### `cqlite.coverage.suite` (13 stable suite names)

`sstable_parity_data_db_jsonl`, `sstable_parity_delta_scan`,
`sstable_parity_statistics_db`, `sstable_parity_index_db_big`,
`sstable_parity_summary_db_big`, `sstable_parity_bti_partitions_rows`,
`sstable_parity_filter_db_bloom`, `sstable_parity_compression_info_chunks`,
`sstable_parity_corruption_verify`, `sstable_parity_component_manifest`,
`sstable_writer_cassandra_fixture_parity`, `compaction_parity_tombstone_ttl`,
`schema_parity_serialization_header`. Public suite organization uses these
names, never issue-number test file names.

### `priority` / `risk`

- `priority`: `P0`, `P1`, `P2`.
- `risk`: `p0_data_loss`, `p1_correctness`, `p2_coverage`, `node_behavior`,
  `tooling_only`.

### `evidence`

`evidence.type` grades the strength of the claim, strongest first:
`byte_for_byte` → `canonical_semantic` → `smoke` → `partial` → `out_of_scope`.

A passing test never upgrades the evidence type. Parse/load success stays
`smoke`; canonical JSONL checks stay `canonical_semantic`; only
byte/offset/checksum/component-file diffs against a Cassandra reference are
`byte_for_byte`.

Other evidence fields: `strict`, `artifacts` (`bytes`, `offsets`, `checksums`,
`component_files`, `jsonl`, `logs`, `generated_report`), `cassandra_version`,
`cassandra_git_sha`, `storage_format_version` (`nb`/`oa`/`da`/`big`/`bti`),
`fixture_generation_command`, `comparison_command`, `reference_paths`,
`failure_artifacts`, `normalization`, `known_limitations`.

**Evidence rules enforced by lint:**

- Fixture-backed scenarios (anything not `planned`/`out_of_scope`) must record
  `cassandra_version`, `cassandra_git_sha`, `storage_format_version`, and
  `fixture_generation_command` — so the byte evidence is reproducible against a
  snapshot or patched build, not just a release tag.
- `byte_for_byte` requires `strict: true`, at least one of
  bytes/offsets/checksums/component-file artifacts, a `comparison_command`,
  `reference_paths`, and `failure_artifacts`.
- `canonical_semantic` requires `normalization` and a `jsonl` artifact /
  reference.
- `smoke` requires `known_limitations` stating parse/load success is not byte
  parity; it cannot satisfy a P0 `p0_data_loss` scenario without `scope.gap`.
- `partial` requires `known_limitations` plus `scope.gap`/`scope.next_step`.
- `out_of_scope` must not define a `comparison_command`.

### `ci`

`ci.tier`: `fast_pr`, `required_parity`, `nightly_docker`,
`exhaustive_regeneration`, `manual_debug`. `required_parity` entries must name a
`ci.workflow` path that exists.

## Out-of-scope taxonomy

"Out of scope does not mean unimportant." These are node-level behaviors CQLite
(an SSTable reader/writer/compactor) intentionally does not mirror. Every
`out_of_scope` scenario requires `scope.out_of_scope_category`,
`scope.rationale`, `scope.cqlite_boundary`, `scope.safe_claim`, and
`scope.related_in_scope_scenarios`. High-relevance Cassandra files may only be
marked out of scope with an explicit `scope.cqlite_boundary`.

| Category | Definition |
|---|---|
| `commitlog_replay` | Commitlog segment writing and replay/recovery. CQLite reads already-flushed SSTables only. |
| `repair_coordinator` | Anti-entropy repair: Merkle trees, validation compaction, anti-compaction. |
| `read_repair_coordinator` | Query-time cross-replica reconciliation and digest-mismatch resolution. |
| `streaming_protocol` | SSTable range streaming between nodes (bootstrap/repair/decommission). |
| `node_lifecycle` | Node join/leave/drain/gossip and lifecycle transitions. |
| `nodetool_jmx_metrics` | nodetool, JMX, live metrics, scheduling, operational controls. |
| `distributed_consensus` | Paxos/Accord serialization and cluster-wide consensus. |
| `sai_sasi_query` | SAI/SASI secondary-index on-disk components and query semantics (unless CQLite implements them). |
| `memtable_internals` | In-memory memtable structures, except the generated SSTable flush artifacts. |
| `java_tooling` | Java-only tooling/nodetool surfaces CQLite does not reimplement. |
| `unsupported_compression_dictionary` | Compression-dictionary features CQLite does not support. |
| `not_sstable_reader_writer_compactor` | Anything outside the SSTable reader/writer/compactor boundary. |

## Adding or changing a scenario

1. Edit `test-data/cassandra-parity-manifest.yml`.
2. Run `cargo run -p cassandra-parity -- lint --manifest test-data/cassandra-parity-manifest.yml`.
3. Regenerate the report (`report` subcommand) and commit it alongside the
   manifest — CI runs `report --check` and fails on a stale report.
4. Never widen a public claim beyond what the manifest evidence supports.
