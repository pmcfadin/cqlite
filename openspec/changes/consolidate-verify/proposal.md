## Why

Two overlapping SSTable integrity paths exist (follow-up from #1236, out of scope of its tasks.md 4.3):

- **`verify::verify_sstable`** (`cqlite-core/src/storage/sstable/verify.rs`) — the rich, public verifier behind
  `cqlite verify`. 16 stable `VerifyErrorClass` checks (TOC completeness, Digest.crc32, CompressionInfo
  bounds, Index.db walk, BTI trie + identity, inline/uncompressed chunk CRC, Statistics/Summary/Filter,
  full row scan, key-order/LDT). It is the surface bound to the #1236 Cassandra corruption-parity oracle.
- **`reader/integrity::perform_integrity_check`** (`reader/integrity.rs`) — a legacy Data.db block-walker
  returning a tri-state `IntegrityStatus` (Healthy/Degraded/Corrupted) + block counts.

They are two "integrity" APIs that can **disagree**: `perform_integrity_check` only walks Data.db blocks, so a
corrupt `Index.db`/`Digest.crc32`/`Summary.db`/`Filter.db` or out-of-order keys reads **`Healthy`** under it
while `verify_sstable` correctly **FAILs**. Divergence risk the issue wants removed. Audit facts:

- `perform_integrity_check` has **zero production callers** (test-only: 3 tests). It is a strict subset of
  `verify_sstable`'s coverage — its only unique element is the `IntegrityCheckResult` *report shape*, not any
  check.
- Its `Degraded` branch is **dead**: `checksum_mismatches` is never incremented, so `IntegrityStatus::Degraded`
  is unreachable.
- `verify_sstable`'s signature, `VerifyErrorClass` set, and the `cqlite verify` text/JSON/exit-code contract are
  **pinned** by the #1236 capabilities (`sstable-verify-parity`, `corruption-verify-oracle`) — they must stay
  byte-stable or those fail-closed parity tests break.

## What changes

- **Milestone:** maintenance / integrity hardening. **Design-driven** (consolidation approach + API surface
  have latitude).
- Establishes a **single source of truth** for SSTable integrity: `verify_sstable` is the authoritative engine;
  the legacy `perform_integrity_check` no longer implements an independent (weaker, divergent) check pipeline.
- Adds an `integrity-single-source` capability documenting the invariant: there is exactly ONE integrity engine,
  and every check any path reports comes from it (no divergent verdicts, no dead branches).

## Recommended approach (Option A — see design.md)

Make `perform_integrity_check` a **thin projection over `verify_sstable`** (`Full` mode): it maps the resulting
`VerifyReport` to the existing `IntegrityCheckResult`/`IntegrityStatus` (findings ⟹ `Corrupted`, clean ⟹
`Healthy`; `rows_scanned` ⟹ `total_entries`). This preserves ALL checks by construction, freezes the CLI
`verify` contract and the #1236 oracle (both untouched), keeps the legacy `pub` type alive (no breaking
removal), and deletes the divergent verdict logic + the dead `Degraded`/`checksum_mismatches` branch.

## Non-goals

- No change to `verify_sstable`'s signature, `VerifyErrorClass` set, or the `cqlite verify` text/JSON/exit-code
  output contract (pinned by #1236 — must stay byte-stable).
- No loss of any integrity check: the consolidated path retains the **union** of both paths' coverage
  (coverage-preservation is the core requirement).
- No change to `get_health_metrics` (a separate health/cache concern in the same file).
- Not a rewrite of `verify.rs` into engine+formatter (Option B) — higher churn against the #1236 oracle;
  rejected in design.md.

## Doctrine impact

None to CLAUDE.md / the site. Internal consolidation; the #1236 verify capabilities remain the public contract.
