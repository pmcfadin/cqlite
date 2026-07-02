# Tasks — consolidate the two integrity paths

## 1. Make the legacy path a projection over the authoritative engine
- [ ] 1.1 Rewrite `reader/integrity::perform_integrity_check` to call `verify::verify_sstable(dir, VerifyMode::Full, config, platform)` — deriving dir/config/platform from the open `SSTableReader` (`self`). Surface: `SSTableReader::perform_integrity_check`.
- [ ] 1.2 Project `VerifyReport → IntegrityCheckResult`: any finding ⟹ `IntegrityStatus::Corrupted`, else `Healthy`; `rows_scanned ⟹ total_entries`; `parsing_errors ⟵ findings' detail`; `corrupted_blocks` best-effort/empty. Remove the independent `read_next_block`/`parse_block_entries` walk that produced divergent verdicts.

## 2. Remove the dead Degraded branch
- [ ] 2.1 Delete the unreachable `IntegrityStatus::Degraded` logic driven by the never-incremented `checksum_mismatches` (`reader/integrity.rs`, `reader/types.rs`). Keep `IntegrityStatus` variants only if still referenced; if `Degraded` becomes unused, remove it (note any re-export impact). Surface: `IntegrityStatus`.

## 3. Do NOT touch the authoritative engine or CLI contract
- [ ] 3.1 Leave `verify::verify_sstable` signature, `VerifyErrorClass`, and `cqlite verify` text/JSON/exit-code output unchanged (pinned by #1236). Confirm by diff that `verify.rs` and `cqlite-cli/src/commands/verify.rs` output paths are untouched (or trivially unchanged).

## 4. Prove it (coverage-preservation + contract)
- [ ] 4.1 #1236 parity: `sstable_parity_corruption_verify.rs` + `issue_1000_verifier.rs` green — every verify check + CLI contract preserved. Surface: `verify_sstable` / `cqlite verify`.
- [ ] 4.2 Projection: `issue_1396_uncompressed_crc_verify.rs` (drives BOTH paths) — `perform_integrity_check` returns `Corrupted` on CRC mismatch, `Healthy` clean; `comprehensive_sstable_test_suite.rs` + `issue_17_real_test.rs` still pass.
- [ ] 4.3 Add a test that a corruption verify catches (e.g. corrupt Index.db / out-of-order keys) makes `perform_integrity_check` report `Corrupted` (would return `Healthy` pre-change) — proves the divergence is gone. Fails pre-fix.
- [ ] 4.4 CLI golden: `cqlite verify --out json` schema + exit code byte-stable before/after.

## 5. Quality gates
- [ ] 5.1 `scripts/agent-gate.sh` PASS (paste AGENT-GATE SUMMARY). Run with `CQLITE_DATASETS_ROOT` at the main repo's `test-data/datasets`. Reproduce the #1236 fixture-gated verify-parity lane locally (regenerate/require-fixtures) — the local gate may skip it (L2: gate PASS ≠ CI green for fixture-gated parity lanes).
- [ ] 5.2 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` clean; no `unwrap()`/`expect()` in library code.
- [ ] 5.3 Intent audit **C** (spec-auditor anchored to `openspec/changes/consolidate-verify/specs/**`) PASS.
- [ ] 5.4 roborev (`--agent codex --base origin/main`) clean.
