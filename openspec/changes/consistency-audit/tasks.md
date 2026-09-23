# Tasks — consistency-audit (issue #4195)

Ordered. Group 0 is premises, 1–3 the library, 4 corruption fixtures, 5 tests, 6 the CLI, 7
review-first, 8 the endgame. Commit after every group (#3042). `--lite` after every fix round; ONE
full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 `verify.rs` (currently 2761 lines) and its Quick/Full check set match design.md's D1 table
      exactly — re-read `check_toc_and_presence`, `check_digest`, `check_filter_false_negatives`,
      `check_bti_structure`/`bti_partition_identity_mismatch`, `check_compression_info`,
      `check_statistics`, `check_summary`, `check_big_index` and confirm none of them ALREADY covers
      an invariant this change treats as a gap (re-verify against `origin/main` at task-start time —
      #4194 may have merged by then and could have touched this file).
- [ ] 0.2 The header-only partition-decode primitive for D2 exists and is callable at an arbitrary
      `(data_path, data_offset)` without decoding rows — locate the exact entry point (design.md
      names `reader/parsing/row_decoder/partition_driver/header_arm.rs` and the point-read call sites
      under `reader/data_access/` as the likely location; this activation found the module but did
      NOT trace the call signature). If no such entry point exists standalone, name the smallest
      extraction needed and re-scope task 2.1 accordingly — do NOT approximate with a full row scan
      (would violate the runtime-bound establishment).
- [ ] 0.3 `SummaryEntry.index_position` is confirmed a byte offset INTO `Index.db` (not `Data.db`) —
      re-read `summary_reader/mod.rs` doc comments and, ideally, one real `(Summary.db, Index.db)`
      pair from the committed corpus to confirm empirically before building D3 on this premise.
- [ ] 0.4 `CompressionInfo.data_length` is the LOGICAL (uncompressed) total size, and
      `ceil(data_length / chunk_length)` is the correct expected chunk count (re-derive against
      Cassandra source at `cassandra-5.0.8` — `CompressionMetadata`/`CompressionInfo.Writer` — not
      just against CQLite's own field, per CLAUDE.md's #3041 format-authority rule).
- [ ] 0.5 The write-path running-stats accumulator (`storage/write_engine/merge/mod.rs` /
      `merge/fully_expired.rs`) is confirmed either directly reusable read-only over an arbitrary
      reader's decoded stream, or the extraction needed to make it so is named and small. If neither
      holds within this change's size budget, scope task 3.4 down to the `partition_count`-only half
      of invariant 8 and record the timestamp/LDT half as a DECLARED GAP in
      `specs/verify-audit/spec.md` (§A8), same style as #4194's L1.4.
- [ ] 0.6 Confirm BTI `Filter.db` presence and whether the committed/fetched corpus has a compressed
      BTI fixture (for D12's compression-coverage BTI combination) — if absent, mark that one
      combination a declared gap rather than fabricating a fixture family that doesn't otherwise
      exist in this corpus.
- [ ] 0.7 Corruption corpus present under the fetched root (`test_comp_corrupt/*`); confirm
      `generate-corruption-corpus.sh`'s exact schema fields (this activation read the manifest
      header and several entries but not the generator script itself) before adding new fixtures.

## 1. Audit report shape — surface: `storage::sstable::verify_audit`

- [ ] 1.1 `verify_audit.rs` (new file, or `verify_audit/` dir if sizing requires per 0.1's re-measure):
      `AuditSummary`, `AuditRow`, `AuditVerdict` (design.md §D0), `derive(Debug, Clone, PartialEq)` to
      match `VerifyReport`'s existing derives. Declared `mod verify_audit;` in `sstable/mod.rs` next
      to `pub mod verify;`.
- [ ] 1.2 `verify.rs`: add `pub audit: Option<AuditSummary>` to `VerifyReport`; add 5 new
      `VerifyErrorClass` variants (`IndexPositionKeyMismatch`, `SummarySampleMismatch`,
      `BtiDataOffsetOutOfBounds`, `CompressionCoverageGap`, `StatisticsScanMismatch`) — every
      existing variant/call site unchanged.
- [ ] 1.3 `VerifyMode` gains `Audit` (design.md's third mode); `verify_components` dispatches to
      `verify_audit::run(...)` when `mode == Audit`, populating `report.audit`. `--deep` is threaded
      through as a `bool` parameter added to `verify_sstable`/`verify_sstable_generation`'s signature
      — additive parameter, default `false`, so every EXISTING caller (Quick/Full) is unaffected
      (confirm call-site count and whether a default-valued wrapper avoids touching them at all).
- [ ] 1.4 `--lite`; commit.

## 2. Invariants 1–2 (BIG: index-data-identity, summary-index-correlation) — surface: `verify_audit`

- [ ] 2.1 Shared `Index.db` re-walk capturing `(byte_offset_in_index_db, key, data_offset_in_data_db)`
      per entry (design.md §D2/§D3) — either extend `check_big_index`'s existing walk to return this,
      or a dedicated walk in `verify_audit.rs`; task 0.1's re-read decides which is less invasive.
- [ ] 2.2 `check_index_data_identity` (D2): header-only read at each `data_offset` via 0.2's confirmed
      entry point; `IndexPositionKeyMismatch` finding + `Fail` row on any mismatch.
- [ ] 2.3 `check_summary_index_correlation` (D3): match each `Summary.db` sample's `index_position`
      against the D2.1 walk's captured offsets; `SummarySampleMismatch` finding + `Fail` row on any
      mismatch or unresolved sample.
- [ ] 2.4 Both `Skip("boundary source unreadable")` when `Index.db` is itself unhealthy (D8);
      `Skip("no Summary.db")` for 2.3 when absent.
- [ ] 2.5 `--lite`; commit.

## 3. Invariants 3–4, 7–8 — surface: `verify_audit`

- [ ] 3.1 Extend `check_filter_false_negatives` (or a thin `verify_audit` wrapper calling the same
      probe logic) to run for BTI, sourcing present keys from `bti_leaves`'s resolved raw keys
      instead of `Index.db` entries (design.md §D4). `Skip("boundary source unreadable")` when the
      BTI trie itself is unhealthy.
- [ ] 3.2 `DataOffset` leaf bounds check inside `check_bti_structure` (design.md §D5):
      `BtiDataOffsetOutOfBounds` finding when `data_position >= data_len`. Fold into the
      `bti-trie-bounds` audit row alongside the existing reachability/identity checks (wired as-is,
      D1's already-correct half).
- [ ] 3.3 `check_compression_coverage` (design.md §D6): `ceil(data_length/chunk_length)` vs
      `chunk_offsets.len()`, plus the last-chunk-physical-end-equals-file-size check.
      `CompressionCoverageGap` finding on mismatch; `Skip("no CompressionInfo: uncompressed")` when
      absent.
- [ ] 3.4 `check_statistics_vs_scan` (design.md §D7, `--deep` only): scope per 0.5's finding — either
      the full four-value comparison, or the `partition_count`-only half with the timestamp/LDT half
      recorded as a declared gap. `StatisticsScanMismatch` finding on mismatch; `Skip("requires
      --deep")` when `--deep` was not passed.
- [ ] 3.5 `scripts/tests/test_verify_audit_no_resync_scan.sh` (design.md §D11), registered in
      `tooling-tests`, modeled on #4194's `test_verify_location_no_resync_scan.sh`.
- [ ] 3.6 `--lite`; commit.

## 4. New corruption fixtures (design.md §D12)

- [ ] 4.1 Extend `test-data/scripts/generate-corruption-corpus.sh` and
      `test_comp_corrupt/corruption-manifest.yml` with the six new fixtures:
      `index_entry_offset_swap`, `summary_sample_position_corrupt`, `filter_db_bit_flip_bti`,
      `bti_data_offset_out_of_bounds`, `compression_info_short_coverage`,
      `statistics_timestamp_mismatch` — each with a committed clean-source path, exact byte
      offset(s)/mutation, before/after sha256, and (where a Cassandra-side check exists per §D13) a
      captured `cassandra_verdict`; otherwise `verdict_parity: "n/a: no Cassandra-side check for this
      invariant"`.
- [ ] 4.2 Confirm each new fixture is INVISIBLE to every EXISTING verify check (stays green under
      `--mode full` minus the one new finding) — a fixture that trips an old check too is not
      isolating the new invariant.
- [ ] 4.3 `.sha256.txt` regenerated; `.db` binaries gitignored per convention; commit the manifest +
      generator script changes (never commit the binaries).

## 5. Tests — surface: `verify_sstable`'s `audit` field

- [ ] 5.1 `cqlite-core/tests/issue_4195_component_audit.rs`: one test per invariant, format-scoped per
      design.md §D0's applicability table, × {healthy, mutated} where a fixture exists — expected
      outcomes computed by the test from the CLEAN source (Index.db/Summary.db/CompressionInfo
      positions, never from `verify_audit`'s own output on the corrupt copy). Corpus gating per #1094
      (`CQLITE_REQUIRE_FIXTURES=1` hard-requires). Any combination declared a gap in 0.6/3.4 is
      documented in `specs/verify-audit/spec.md`, not silently absent from this file's coverage.
- [ ] 5.2 A vacuous-audit test (temp dir with every optional component absent, or `--mode audit`
      against a directory where every invariant Skips) asserting exit-`2`/`audit: 0 invariants
      MEASURED` (§D9).
- [ ] 5.3 Confirm existing suites unaffected: `sstable_parity_corruption_verify.rs` (unmodified),
      `index_summary_correlation_test.rs` (unmodified) — regression checks, not new test code.
- [ ] 5.4 Confirm the new `cqlite-core` target executes under `core-tests` (#3522) — no gate-script
      edit expected (glob auto-coverage, matching #4194's confirmed pattern); say so if it needs one.
- [ ] 5.5 `--lite`; commit; push.

## 6. CLI — surface: the built binary

- [ ] 6.1 `cli_types.rs`: `VerifyModeArg::Audit`; `deep: bool` arg on `Verify`, `long_about` stating
      it is only meaningful with `--mode audit` (usage error otherwise).
- [ ] 6.2 `commands/verify.rs`: `--deep`-without-`--mode audit` usage-error path (exit `1`, names the
      requirement, before any `verify_sstable` call); render the 8-row checklist in `print_text`/
      `print_json` when `audit.is_some()`; exit-code wiring per §D9.
- [ ] 6.3 `cqlite-cli/tests/` CLI test(s) for the checklist rendering (text + JSON) and the `--deep`
      usage-error path, named in the gate's `cli-tests` list per #3522.
- [ ] 6.4 Docs: `dev-cookbook.md` "Audit cross-component consistency" entry.
- [ ] 6.5 `--lite`; commit; push.

## 7. Review-first

- [ ] 7.1 roborev on the lite-green diff BEFORE any full gate (`--agent claude-code --model
      claude-opus-5`, the only sanctioned invocation). Pre-empt the standing findings classes: integer
      overflow in offset/position arithmetic (`saturating_add`/`saturating_mul` throughout, matching
      #4194's own round-1 findings), no-heuristics (task 3.5's guard covers this mechanically),
      wall-clock races (n/a — no timing asserts in this change's tests).

## 8. Endgame — `flow-closer`

- [ ] 8.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree integrity
      clean, own run-id.
- [ ] 8.2 C: `spec-auditor` vs `openspec/changes/consistency-audit/specs/**`.
- [ ] 8.3 Roborev LAST via the sanctioned script; a FRESH round after the full gate.
- [ ] 8.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`.
- [ ] 8.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4195, tick #4192.
