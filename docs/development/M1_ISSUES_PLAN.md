## CQLite M1 Readiness: Uncompromising Plan and Issue Breakdown

Order of execution for github issues (must be completed in this order):
1) #28 — Schema/comparator-only parsing: remove heuristics and blob fallbacks (P0)
2) #28 — Complete comparator threading and key digest correctness (P0)
3) #34 — Compression metadata and per-chunk CRC enforcement (P0)
4) #35 — Index/Summary/Statistics integration + checksum validation (P0)
5) #36 — BTI end-to-end validation (P0)
6) #30 — Validator on Docker infra (real SSTables) (P0)
7) #38 — CI gating: zero-diff sstabledump parity (P0)
8) #31 — Validator parser accuracy against real sstabledump (P0)
9) #32 — Automated validator harness (Docker + CI) (P0)
10) #37 — Tombstone reconciliation semantics validation (P1)
11) #51 — Coverage gate ≥90% for core reading modules (P1)
12) #52 — Human-verifiable validation workflow (P1)

This document captures the authoritative, test-driven path to achieve PRD Milestone M1 (Core Reading Library). It enumerates concrete, non-negotiable tasks, with zero-heuristics acceptance criteria and TDD-first validation. Each section is formatted to be copy-pasted into a GitHub Issue. Priorities: P0 (blocker), P1 (high), P2 (nice-to-have).

References:
- `docs/development/PRD.md` – M1 definition
- `docs/development/ISSUE_EXECUTION_PLAYBOOK.md` – Issue tracks #28, #34, #35, #36, #38, #30, #31, #32, #37, #51, #52

---

### P0 — Eliminate all heuristic parsing in modern paths (follow-up to Issue #28)

Proposed Title: Schema/comparator-only parsing: remove heuristics and blob fallbacks (P0)

Scope:
- Remove header and compression metadata heuristics from modern formats; retain only gated legacy/diagnostic paths.
- Enforce comparator-driven key/value decoding end-to-end; no blob fallback when schema is present.

Evidence of gaps:
- Header heuristics in modern path:
```730:845:cqlite-core/src/storage/sstable/reader.rs
fn estimate_header_size_heuristic(header_buffer: &[u8]) -> Result<usize> {
    // ... entropy-based heuristic and fallback sizes ...
}
```
- CompressionInfo “alternative” format with guessing and single-chunk fallback:
```180:221:cqlite-core/src/storage/sstable/compression_info.rs
let chunk_length = Self::detect_chunk_size(remaining_data).unwrap_or(16384);
let data_length = remaining_data.len() as u64;
let chunk_offsets = vec![0]; // Single chunk assumption for fallback
```
- Blob fallback in row/cell SM when schema available (should never happen in modern):
```774:805:cqlite-core/src/storage/sstable/row_cell_state_machine.rs
// parse_cql_value(...) else -> Value::Blob(...)
```

Tasks:
- Remove or guard header heuristics behind `--allow-legacy-heuristics` test-only flag; default disabled for modern (BIG v5, BTI).
- Delete `CompressionInfo::parse_alternative_format` for modern; require authoritative `CompressionInfo.db` with per-chunk CRCs.
- In `RowCellStateMachine` and `SchemaAwareReader`, make schema mandatory for modern paths; delete blob fallback branches when context present.
- Add unit tests that fail if any heuristic branch executes on modern formats.

Validation:
- Unit tests proving no heuristic code paths execute for BIG v5/BTI datasets (branch coverage asserts or feature-flag assertions).
- Parity runs (see P0 — CI Gating) pass with zero diff.

Acceptance criteria:
- Modern read paths have no heuristics; all heuristic code is legacy-guarded and not used by default.

---

### P0 — Comparator-driven key decoding and integration (Issue #28 completion)

Proposed Title: Complete comparator threading and key digest correctness (P0)

Evidence of gaps:
- Index lookup uses a placeholder digest:
```2917:2961:cqlite-core/src/storage/sstable/reader.rs
// compute_partition_key_digest: DefaultHasher, TODO for schema-driven comparator
```

Tasks:
- Implement exact Cassandra key digest/byte-comparable encoding for BIG and BTI.
- Use `SchemaRegistry::get_parsing_context` to thread comparators into all key decode paths.
- Replace any use of simple hashing for Index.db with spec-correct digest.
- Add unit/property tests for multi-component partition/clustering keys ensuring byte- vs typed-ordering equivalence.

Validation:
- Targeted unit tests for composite keys, nested types in keys; ordering and equality invariants.
- sstabledump parity on tables with composite keys shows zero diff and correct iteration order.

Acceptance criteria:
- All key digests and encodings match Cassandra; no default hashing remains.

---

### P0 — Compression metadata and per-chunk CRC enforcement across algorithms (Issue #34)

Proposed Title: Enforce CompressionInfo/CRC for LZ4/Snappy/Zstd/Deflate + negative tests (P0)

Status:
- Per-chunk CRC validation exists in `ChunkDecompressor` (modern) but alternative/fallback parsing remains; dataset matrix and negative tests incomplete.

Tasks:
- Remove `parse_alternative_format` for modern formats; require authoritative metadata.
- Create dataset matrix: 4 compressors × 3 chunk sizes (16, 64, 128 KiB).
- Implement negative tests: intentionally corrupt per-chunk CRC and assert deterministic error message (file, offset, expected/actual).
- Validate checksums in `Statistics.db` (currently TODO).

Validation:
- New tests under `tests/` cover all matrix combinations including negative corruption.
- CI job runs matrix and fails fast on any CRC discrepancy.

Acceptance criteria:
- No decompression guessing; deterministic CRC enforcement across all algorithms and sizes.

---

### P0 — Index/Summary/Statistics integration into read path (Issue #35)

Proposed Title: Integrate spec readers into lookup/iteration + checksum validation (P0)

Evidence of gaps:
- Index lookup wired but digest incorrect (see comparator issue).
- Statistics checksum explicitly skipped:
```70:79:cqlite-core/src/storage/sstable/statistics_reader.rs
// TODO: Add proper checksum validation for nb format
```

Tasks:
- Complete Index.db integration with correct key digest; validate offsets and sizes.
- Implement promoted index handling and token-range iteration with Summary.db.
- Implement Statistics.db checksum validation and expose min/max timestamps, token coverage; cross-check against sstabledump JSON.
- Add wide-partition datasets to exercise promoted index paths.

Validation:
- Integration tests resolve random partition lookups via index to correct data; offsets verified.
- Parity of min/max timestamps and token coverage vs sstabledump.

Acceptance criteria:
- Spec readers fully used in lookup/iteration; checksums validated; parity passes.

---

### P0 — BTI end-to-end validation (Issue #36)

Proposed Title: BTI trie traversal, Rows.db decoding, byte-comparable invariants (P0)

Tasks:
- Add TDD tests for trie traversal for lookups/iteration, Rows.db decoding, and byte-comparable round-trip for all key components.
- Integrate with parity validator for BTI datasets (including range tombstones, complex types).

Validation:
- Zero-diff vs sstabledump for BTI datasets; iteration order correct across ranges.

Acceptance criteria:
- BTI suite green in CI; failures block merge.

---

### P0 — CI gating: zero-diff sstabledump parity (Issue #38)

Proposed Title: Make sstabledump parity a mandatory CI gate (P0)

Evidence of gaps:
- Existing workflows contain placeholders/mocks and do not actually run Cassandra tools or real parity.

Tasks:
- Wire `tools/sstabledump-validator` to real Docker Cassandra stacks from `test-data/docker/`.
- Ensure validator runs `sstabledump` from the container and compares against CQLite output.
- Upload JUnit and summary artifacts; fail fast on first diff; post PR comment.

Validation:
- CI runs full corpus (BIG + BTI, compressors, complex types, tombstones) and blocks merges on any diff.

Acceptance criteria:
- CI parity gate is enforced on PRs and main.

---

### P0 — Validator on Docker infra against real SSTables (Issue #30)

Proposed Title: Run validator across real SSTables in Docker (P0)

Tasks:
- Start 5.0 and multi-version stacks via `test-data/docker/*.yml`.
- Use existing datasets under `test-data/` and generate new sets as needed.
- Run validator in zero-tolerance mode across datasets and archive results.

Validation:
- Successful runs; logs and reports available for CI gate.

Acceptance criteria:
- No operational issues; artifacts present for #38.

---

### P0 — Parser accuracy vs real sstabledump outputs (Issue #31)

Proposed Title: Harden validator parser across versions/complex types (P0)

Tasks:
- Collect real sstabledump outputs for 3.7–5.0.
- Ensure parser covers nested collections, UDTs, tuples, frozen; metadata (timestamps, TTL, deletions); version-specific diffs.

Validation:
- 0% false positive/negative across datasets; parser benchmarks meet sub-second per MB.

Acceptance criteria:
- Parser robust and reliable input to CI gate.

---

### P1 — Tombstone reconciliation semantics (Issue #37)

Proposed Title: Enforce read-time reconciliation: row/cell tombstones, range tombstones, TTL (P1)

Tasks:
- Datasets for overlapping writes, expired TTLs, row vs cell deletes, inclusive/exclusive range tombstones.
- Enforce engine behavior per Cassandra semantics; dual validation vs sstabledump and live cqlsh.

Validation:
- Zero discrepancies in visibility and metadata across scenarios.

Acceptance criteria:
- Regression tests added and run in CI.

---

### P1 — Coverage and test rigor for M1 (Issue #51)

Proposed Title: Achieve ≥90% coverage for core reading codepaths with TDD (P1)

Tasks:
- Add coverage tooling in CI; enforce threshold for `cqlite-core` reading modules.
- Expand unit/property tests for edge cases (nested UDTs, frozen, large collections, varints, negative timestamps, etc.).

Validation:
- CI coverage report ≥90% for core reading modules; no flaky tests.

Acceptance criteria:
- Coverage gate enforced in CI.

---

### P1 — Human verification guide (manual trust-building) (Issue #52)

Proposed Title: Human-verifiable, reproducible validation workflow (P1)

Steps (document and script under `scripts/validation/`):
1) Start Cassandra 5.0 stack:
   - `docker compose -f test-data/docker/docker-compose-cassandra5.yml up -d`
2) Generate test data (use existing scripts under `scripts/testing/`):
   - `bash scripts/testing/run_cql_validation_tests.sh`
3) Run sstabledump validator zero-tolerance across datasets:
   - `cargo run -p sstabledump-validator -- validate /absolute/path/to/sstable-dir --fail-on-diff --detailed`
4) Manually spot-check a table:
   - `tools/sstabledump-validator/target/release/sstabledump-validator validate ...`
   - Compare a few keys by hand; verify timestamps/TTLs match.
5) Export via CLI and diff:
   - `cargo run -p cqlite-cli -- dump --schema test-data/schemas/basic-types.cql --data-dir /abs/data --out json > cqlite.json`
   - `sstabledump ... > cassandra.json` (from container)
   - `jq -S . cqlite.json > a && jq -S . cassandra.json > b && diff -u a b`

Acceptance criteria:
- Steps are reliable on a clean machine; results are zero-diff; artifacts archived.

---

### Final P0 — M1 Verification and Sign-off

Proposed Title: M1 verification: Core Reading Library complete (P0, final gate)

Exit Criteria (from PRD M1):
- Reads any Cassandra 5 SSTable (BIG + BTI); all CQL/UDT types; compression OK.
- Comparator-driven parsing only; zero heuristics in modern paths.
- CI parity gate green; coverage ≥90% for core reading; unit + property tests comprehensive.

Deliverables:
- CI run links showing parity suite green across corpus + compressors matrix.
- Coverage report ≥90% for `cqlite-core` reading modules.
- Validator reports attached; manual verification log attached.

Merge Policy:
- Tag and track as the final task that depends on all P0 issues above; merging this closes M1.

---

## Prioritized Timeline (suggested)

Week 1:
- P0 Heuristics removal, Comparator-driven key digest, Compression metadata enforcement.

Week 2:
- P0 Index/Summary/Statistics integration; P0 Validator wiring and CI gating; BTI TDD tests.

Week 3:
- Complete parity corpus runs; wide-partition datasets; negative CRC tests; Parser hardening (#31).

Week 4:
- Tombstone reconciliation (P1), coverage hardening; M1 final verification and sign-off.

