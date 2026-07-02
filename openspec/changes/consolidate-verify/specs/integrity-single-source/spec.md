## ADDED Requirements

### Requirement: A single integrity engine is the source of truth
SSTable integrity SHALL have exactly one authoritative engine — `verify::verify_sstable`. Any other integrity
entry point (e.g. `reader/integrity::perform_integrity_check`) SHALL derive its result from that engine and SHALL
NOT implement an independent, divergent check pipeline. Two integrity APIs SHALL NOT be able to return
contradictory verdicts for the same SSTable.

#### Scenario: The legacy integrity check delegates to the authoritative engine
- **WHEN** `perform_integrity_check` is invoked on an open reader
- **THEN** its verdict is derived from `verify_sstable` (full mode) over the same SSTable
- **AND** it does not run a separate block-walk pipeline that could disagree with `verify_sstable`

#### Scenario: A corruption the authoritative engine catches is not reported healthy by the legacy path
- **WHEN** an SSTable has a corruption `verify_sstable` classifies as a finding (e.g. a corrupt Index.db, Digest.crc32, Summary.db, Filter.db, or out-of-order keys)
- **THEN** the legacy integrity path reports the SSTable as corrupted (not `Healthy`)

### Requirement: No integrity check coverage is lost in consolidation
The consolidated path SHALL retain the union of both prior paths' coverage. All existing `verify_sstable` check
classes SHALL continue to fire, and the legacy `IntegrityCheckResult`/`IntegrityStatus` result contract SHALL
continue to be produced (as a projection of the authoritative report) so existing consumers/tests keep working.

#### Scenario: All verify check classes still fire
- **WHEN** `verify_sstable` runs in full mode after consolidation
- **THEN** every previously-supported `VerifyErrorClass` check executes and is reported exactly as before
- **AND** the #1236 Cassandra corruption-parity oracle test remains green

#### Scenario: The legacy result contract is preserved
- **WHEN** `perform_integrity_check` returns
- **THEN** it yields an `IntegrityCheckResult` with a correct `IntegrityStatus` (`Corrupted` when the engine reports findings, `Healthy` when clean)
- **AND** the existing tests that assert on `IntegrityStatus` pass unchanged

### Requirement: The `cqlite verify` output contract is unchanged
Consolidation SHALL NOT alter `verify_sstable`'s signature, its `VerifyErrorClass` set, or the `cqlite verify`
text/JSON/exit-code output contract, which is pinned by the #1236 verify capabilities.

#### Scenario: verify CLI output is byte-identical
- **WHEN** `cqlite verify <path> --mode full --out json` (and `--out text`) runs before and after the change
- **THEN** the output bytes and the process exit code are identical
- **AND** a clean SSTable exits 0 while a corrupt one exits non-zero as before

### Requirement: The dead Degraded branch is removed
The consolidated integrity projection SHALL produce only reachable status values and SHALL NOT retain the
unreachable `IntegrityStatus::Degraded` path (driven by `checksum_mismatches`, a counter that was never
incremented).

#### Scenario: Integrity status is always a reachable value
- **WHEN** `perform_integrity_check` returns any status
- **THEN** the status is one the projection can actually produce (`Healthy` or `Corrupted`)
- **AND** there is no code path that pretends to compute `Degraded` from an always-zero counter
