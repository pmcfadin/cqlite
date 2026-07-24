# Tasks: chunk-source-scan-decode (issue #2165)

## 1. Migrate the decompress site (surface: `parse_block_entries` → `SSTableReader::scan` / `iterate_all_partitions`)
- [ ] 1.1 Replace the inline `Compression::new(...)` + `decompress(block_data)` at
      `parsing/block_entries.rs:111` with `ChunkSource::decompress_only(...)` (stitch-path
      precedent, `data_access/mod.rs:474`); preserve the `compression_reader == None` raw
      passthrough unchanged.
- [ ] 1.2 Remove the silent decompress-failure → raw-bytes fallback (lines 120–125); propagate the
      corruption error.
- [ ] 1.3 Flip `test_decompression_fallback_on_failure` (`block_entries.rs:1347`) to assert the
      fail-closed error through the `parse_block_entries` surface.

## 2. Architecture test + docs (surface: `chunk_decode_single_plane.rs`)
- [ ] 2.1 Delete the `parsing/mod.rs` + `parsing/block_entries.rs` exclusions (lines 43–47) so the
      scan covers `parsing/` fully; test must pass.
- [ ] 2.2 Update stale doc-comments naming #2165 as pending: `warm_windowed_scan_skips_decompress`
      (chunk_decode_single_plane.rs:267–271) and the `chunk_source.rs:4–9` module doc.
- [ ] 2.3 Comment the premise correction on issue #2165 (`parse_partition_at_offset` already gone;
      scope reduced to one site). No scope/title change.

## 3. Verify (public surfaces, real fixtures; `CQLITE_DATASETS_ROOT` → main repo datasets)
- [ ] 3.1 Diff-scoped: `chunk_decode_single_plane`, flipped fallback test, `cqlite-core --lib`.
- [ ] 3.2 Parity/no-regression targets: `v5_compressed_legacy_parity_test`,
      `v5_compressed_legacy_row_count_parity`, `issue_1085_tombstones_full_scan_parity`,
      `index_size_zero_integration_test`, `point_vs_full_differential`,
      `query_semantics_oracle_parity`; 33-table smoke.
- [ ] 3.3 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).

## 4. Review + endgame (per the implement loop)
- [ ] 4.1 `rust-reviewer` + roborev on the lite-green diff (review-first).
- [ ] 4.2 Open PR (1:1:1:1; branch `issue-2165-chunk-source-scan-decode`).
- [ ] 4.3 `flow-closer`: ONE full gate → C (`spec-auditor` on this change's `specs/**`) → final
      roborev → premerge-assert + `--auto` merge → finalize (telemetry stamp + archive + close).
