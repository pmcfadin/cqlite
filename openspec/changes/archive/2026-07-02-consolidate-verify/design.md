# Design — consolidate the two integrity paths

## Current state

| | `verify::verify_sstable` | `reader/integrity::perform_integrity_check` |
|---|---|---|
| Input | directory (resolves a generation) | an already-open `SSTableReader` |
| Checks | 16 `VerifyErrorClass`: TOC, Digest, CompressionInfo, Index walk, BTI trie+identity, inline+uncompressed CRC, Statistics, Summary, Filter FN, full row scan, key-order/LDT | Data.db block-walk (`read_next_block`+`parse_block_entries`) only |
| Result | `VerifyReport { findings, rows_scanned, toc_components }` (corruption = data, not `Err`) | `IntegrityCheckResult { total_blocks_checked, corrupted_blocks, checksum_mismatches, unreadable_blocks, total_entries, parsing_errors, overall_status }` |
| Callers | CLI `cqlite verify`; #1236 parity oracle; many tests | **none in production** — 3 tests only |
| Parity | bound to #1236 oracle | none |

**Divergence to remove:** the two can return contradictory verdicts on the same file (integrity says `Healthy`
for a corrupt Index/Digest/Summary/Filter/ordering that verify FAILs). `perform_integrity_check` also has a
**dead** `Degraded` branch — `checksum_mismatches` is never incremented, so it is unreachable.

**Coverage union the unified path MUST retain:** all 16 verify checks + integrity's block-level result shape
(the 3 tests assert on `IntegrityStatus`). No *check* is unique to integrity; only its report *shape* is.

## Options

### (A) `perform_integrity_check` becomes a thin projection over `verify_sstable` — RECOMMENDED
`perform_integrity_check` derives dir/config/platform from `self`, calls `verify_sstable(dir, Full, …)`, and
projects `VerifyReport → IntegrityCheckResult`:
- any finding ⟹ `overall_status = Corrupted`; else `Healthy` (the dead `Degraded` branch is dropped).
- `rows_scanned ⟹ total_entries`; `parsing_errors ⟵ findings' detail strings`; `corrupted_blocks` becomes best-effort/empty (no production consumer reads per-block indices).
- **Retains all checks by construction; CLI `verify` + #1236 oracle untouched; no breaking pub-API removal.**
- Cost: mild redundancy (verify re-opens a reader). Loses genuine per-*block* indices — acceptable (no prod
  consumer). Risk: **low**.

### (B) Extract a shared `integrity_core` both call — REJECTED
Pull the check implementations into a core module; `verify.rs` becomes report/formatter over it. Larger refactor
of the 2374-line `verify.rs` — high churn against the exact file the #1236 P0 parity oracle binds to. Higher
risk for no additional coverage. Rejected.

### (C) Delete `perform_integrity_check` outright, migrate the 3 tests to `verify_sstable` — cleaner end-state, OWNER CALL
No production caller; strictly weaker subset. Smallest long-term surface. But it is a **breaking `pub` API
removal** (`IntegrityCheckResult`/`IntegrityStatus` re-exports at `reader/mod.rs`, `reader/types.rs`) for any
external embedder — needs the owner's sign-off. Can be a follow-up after (A).

## Decision

**Option (A).** Single source of truth with zero coverage loss, the #1236 contract frozen, and no breaking
removal. **Escalate at Seam 1:** whether the owner prefers (C) — delete the legacy path now — since that is the
cleaner end state but a breaking pub-API change. Default is (A) unless the owner chooses (C).

## Correctness invariant (audit/tests must prove)

1. Exactly one integrity engine: every reported check originates in `verify_sstable`; `perform_integrity_check`
   adds no independent check and cannot contradict verify.
2. Coverage union preserved: all 16 `VerifyErrorClass` checks still fire (no check dropped).
3. `cqlite verify` text/JSON/exit-code output is byte-identical; the #1236 parity oracle stays green.
4. The 3 integrity tests still pass (correct `IntegrityStatus` via the projection).
5. The dead `Degraded`/`checksum_mismatches` path is gone.

## Testing

- #1236 fail-closed parity: `sstable_parity_corruption_verify.rs` + `issue_1000_verifier.rs` prove every verify
  check + CLI contract preserved (run the public surface unchanged).
- `issue_1396_uncompressed_crc_verify.rs` (drives BOTH paths) proves the projection returns correct
  `IntegrityStatus` (Corrupted on CRC mismatch, Healthy clean).
- CLI golden on `cqlite verify --out json` guards the text/JSON schema + exit code.

## Risks

- Touching `verify.rs` at all risks the #1236 oracle — mitigate by NOT modifying `verify_sstable`; only
  `perform_integrity_check` changes (calls it).
- Re-opening a reader inside `perform_integrity_check` — acceptable; it is a test-only path.
