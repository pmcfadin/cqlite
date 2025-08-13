## CQLite Engineering Execution Playbook (Issues #28, #34, #35, #36, #38, #30, #31, #32, #37)

This document provides step‑by‑step instructions for the engineering team to complete the high‑priority work needed to achieve reliable Cassandra 5 reading. Issue #25 is in progress; start here with #28 and proceed in order.

For each issue:
- Follow the “What to do” steps
- Use the “References” to align with upstream
- Complete the “Validation” checklist
- Ensure the “Acceptance criteria” are met
- Open a PR with the requested title and checklist; assign for review

---

### #28 — Implement schema‑driven parsing architecture (P0)

Objective
- Make all modern parsing strictly schema/comparator‑driven (no guessing). Thread table schema and comparators through the read path and decode keys/values accordingly.

What to do
1) Schema plumbing
   - Introduce a `SchemaRegistry` API that provides `TableSchema` and comparator info by table.
   - Thread `TableSchema` + partition/clustering comparator types into the modern reader entry points and the row/cell state machine.

2) Key decoding with comparators
   - Decode partition and clustering keys using the exact comparator types (including multi‑component keys). Ensure byte‑comparable vs typed ordering consistency.

3) Value decoding by type
   - Decode simple types, collections (list/set/map), tuples, UDTs, and frozen (including nested) using the actual column types from the schema.
   - Eliminate any remaining type detection/guessing branches.

4) Minimal migration for legacy paths
   - Keep 3.x/legacy paths isolated. Do not regress them, but do not use guessing in modern paths.

References
- Cassandra comparators: `org.apache.cassandra.db.marshal.*`
- sstabledump output as ground truth

Validation
- Unit tests:
  - Multi‑component partition/clustering keys with various comparators
  - Nested collections, tuples, UDTs, and frozen
  - Round‑trip ordering tests (byte‑comparable vs typed)
- Integration tests:
  - Zero‑tolerance sstabledump parity across 3 representative tables (simple, collections, UDT/frozen)

Acceptance criteria
- All modern parsing paths are schema/comparator‑driven
- No type guessing remains in modern readers/state machine
- Tests pass with zero‑diff parity vs sstabledump for the representative datasets

PR
- Title: “Schema‑driven parsing architecture – Issue #28”
- Include: brief design note for `SchemaRegistry`, test results/artifacts, and coverage summary

---

### #34 — Compression metadata/CRC validation across algorithms (P0, critical)

Objective
- Enforce chunk boundaries and per‑chunk CRC using authoritative `CompressionInfo.db`/CompressionMetadata across LZ4/Snappy/Zstd/Deflate; remove any decompression guessing.

What to do
1) Reader enforcement
   - Read chunk offsets/lengths/CRC from CompressionInfo/CompressionMetadata.
   - For each chunk, verify CRC; on mismatch, return a deterministic corruption error (file, chunk offset, expected/actual CRC).
   - Remove “try raw data” and similar guessing for modern paths.

2) Dataset matrix
   - Generate data for four compressors × three chunk sizes (16, 64, 128 KiB).
   - Ensure corresponding `CompressionInfo.db` exists for each SSTable set.

3) Negative tests
   - Corrupt one chunk’s CRC in a copy of each dataset; assert reader fails with the expected error.

References
- Cassandra 4.0 Compression docs
- `CompressedRandomAccessReader` and `CompressionMetadata` (upstream 5.0)

Validation
- Zero‑diff vs sstabledump for all compressor/size combinations
- CRC corruption is detected deterministically with precise error reporting

Acceptance criteria
- No decompression guessing in modern paths
- CI matrix for compressors/sizes added; failures block merge

PR
- Title: “Compression metadata/CRC validation (all algorithms) – Issue #34”
- Include: matrix results, example corruption failure, link to CI run

---

### #35 — Index/Summary/Statistics parsing and validation (P0, critical)

Objective
- Implement/harden Index.db (incl. promoted index), Summary.db, and Statistics.db parsing. Use them for partition lookup/iteration and metadata (min/max timestamps, token coverage). Validate vs sstabledump.

What to do
1) Implement/harden readers
   - Index.db: full structure, promoted index handling; verify offsets and key digests.
   - Summary.db: parse summary entries and sampling.
   - Statistics.db: parse min/max timestamps, partition‑level markers, token coverage; validate checksums.

2) Testing
   - Datasets with wide partitions to force promoted index.
   - Cross‑check min/max timestamps and token coverage vs sstabledump JSON.
   - Validate index‑based lookups resolve to correct data offsets and rows.

References
- Big format sources (5.0): `org.apache.cassandra.io.sstable.format.big.*`
- sstabledump JSON fields for index/summary/statistics

Validation
- Zero‑diff vs sstabledump for Index/Summary/Statistics across existing and wide‑partition datasets
- Random sampled partition lookups read correct rows

Acceptance criteria
- Readers complete and pass parity
- CI includes index/summary/statistics parity; failures block merge

PR
- Title: “Index/Summary/Statistics: spec readers + validation – Issue #35”
- Include: dataset descriptions, parity artifacts, and a few lookup proof cases

---

### #36 — BTI validation suite (P0, critical)

Objective
- Validate BTI (Cassandra 5.0) end‑to‑end: Partitions.db trie traversal, Rows.db decoding, and byte‑comparable keys, with parity vs sstabledump.

What to do
1) Datasets (BTI)
   - Multi‑component partition keys, multiple clustering keys, wide partitions
   - Complex types (nested collections, UDTs), range tombstones

2) Tests
   - Trie traversal for lookups and iteration across token ranges
   - Rows.db decoding and clustering navigation
   - Byte‑comparable round‑trip invariants for all key components

References
- CEP‑25 (Trie‑indexed SSTable format)
- Cassandra `bytecomparable` utilities (5.0)

Validation
- Zero‑diff vs sstabledump for values and metadata (writeTime, TTL, tombstones)
- Iteration/order complete and correct across ranges

Acceptance criteria
- BTI datasets pass parity; trie and row index behavior correct
- CI BTI suite added; failures block merge

PR
- Title: “BTI validation suite (Partitions/Rows/Byte‑comparable) – Issue #36”
- Include: dataset specs, parity artifacts, traversal/ordering proofs

---

### #38 — CI sstabledump parity gating (P0, critical)

Objective
- Make the zero‑diff sstabledump parity suite a required CI gate for BIG and BTI, compressors, complex types, and tombstones.

What to do
1) Corpus
   - Include BIG + BTI, compressors (LZ4/Snappy/Zstd/Deflate), multiple chunk sizes
   - Complex types: nested collections, tuples, UDTs, frozen
   - Wide partitions (promoted index), tombstone scenarios (range, TTL)

2) Workflow
   - Extend/confirm `.github/workflows/sstabledump-validation.yml`:
     - Start Docker stacks
     - Generate/prepare data
     - Run validator with `--zero-tolerance`; produce JUnit and a concise summary artifact
     - Fail fast on first diff and post PR comment with diff summary

Validation
- Workflow runs complete suite on branch; artifacts uploaded
- Intentional regression produces clear diff and blocks merge

Acceptance criteria
- CI gate is mandatory on main and PRs; any parity failure blocks merge

PR
- Title: “CI gating: zero‑diff sstabledump parity – Issue #38”
- Include: workflow changes, sample PR comment output, artifact links

---

### #30 — Test sstabledump validator against real Cassandra (P0)

Objective
- Wire the existing sstabledump validator into Docker infrastructure and run against real SSTables across versions.

What to do
1) Docker
   - Bring up the 5.0 cluster stack; reuse multi‑version stacks where needed.
2) Data
   - Use the existing eight SSTable collections; generate additional sets as needed.
3) Validator runs
   - Execute the validator in zero‑tolerance mode across the datasets and record results.

Validation
- Validator successfully runs across the stacks and all datasets; results recorded

Acceptance criteria
- Zero operational issues; logs and reports available for #38 CI gate

PR
- Title: “Validator on Docker infra (real SSTables) – Issue #30”
- Include: commands used, logs, and result summaries

---

### #31 — Validate sstabledump parser accuracy vs real output (P0/critical)

Objective
- Ensure the validator’s parser correctly handles real sstabledump JSON across versions and complex data types.

What to do
1) Collect real outputs for existing datasets (3.7–5.0)
2) Parser enhancements for:
   - Basic types, nested collections, UDTs, tuples, frozen
   - Metadata: timestamps, TTLs, deletion info
   - Version‑specific formatting differences

Validation
- 0% false positive/negative across the datasets
- Sub‑second per‑MB parsing target met on typical files

Acceptance criteria
- Parser robust across versions and edge cases; feeds #38 reliably

PR
- Title: “Validator parser accuracy against real sstabledump – Issue #31”
- Include: sample inputs/outputs, benchmarks, and edge‑case proofs

---

### #32 — Automated test harness leveraging Docker (P0)

Objective
- Provide a one‑command harness that starts Docker, generates data, and runs the validator over the full corpus; integrate with CI.

What to do
1) Orchestration script
   - Start stacks, generate data, run validator over all dataset directories, and collate reports.
2) CI integration
   - Call the script in CI; upload JUnit and summary artifacts.

Validation
- Local and CI runs execute end‑to‑end under 10 minutes (target) and produce artifacts

Acceptance criteria
- Harness in repo; CI calls it; artifacts visible on runs

PR
- Title: “Automated validator harness (Docker + CI) – Issue #32”
- Include: script path, CI step, sample artifacts

---

### #37 — Tombstone reconciliation semantics (P1)

Objective
- Test and enforce correct read‑time reconciliation: row/cell tombstones, range tombstones, TTL expiry, and write‑time ordering.

What to do
1) Targeted datasets
   - Overlapping writes, expired TTLs, row vs cell deletes, range tombstones (inclusive/exclusive bounds)
2) Engine behavior
   - Ensure read path applies reconciliation rules per Cassandra
3) Dual validation
   - Compare visibility and metadata vs sstabledump and a live Cassandra query (cqlsh) of the same data

Validation
- Zero discrepancies in visibility and metadata across scenarios

Acceptance criteria
- Regression tests added; runs in CI within time budget

PR
- Title: “Tombstone reconciliation validation – Issue #37”
- Include: scenario specs, parity evidence, and follow‑ups if any

---

Administrative notes
- #29 (compression correctness) was previously closed but is superseded by #34; do not close #34 until CI matrix passes.
- Always attach sstabledump parity artifacts (or links) to PRs for traceability.
- Assign PRs for review and include the acceptance checklist in the description.

