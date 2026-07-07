# write-span-levels

## ADDED Requirements

### Requirement: Write-side spans emit at DEBUG, not INFO

Write-side and compaction `#[tracing::instrument]` spans SHALL be emitted at
DEBUG level, matching the read side's `debug_span!` discipline. Span names and
attribute keys SHALL remain unchanged; only the level changes.

#### Scenario: Per-batch span count is O(1), not O(N), at INFO

- **GIVEN** a `tracing` subscriber installed at INFO (the CLI default)
- **WHEN** a batch of N mutations is written (each driving `write.mutation` +
  `memtable.insert` + `wal.append`)
- **THEN** the count of INFO-level spans is O(1) per batch (not ≥3N)
- **AND** a counting-subscriber test asserts this and FAILS on `main` (≥3N today).

#### Scenario: Per-partition and per-chunk spans are not at INFO

- **WHEN** a compaction/flush drives `merger.step`, `writer.write_partition`, and
  `compression.write_chunk`
- **THEN** none of those spans are emitted at INFO — each is DEBUG.

#### Scenario: Span names and attribute keys are preserved

- **WHEN** a demoted span is emitted at DEBUG
- **THEN** its `name = "…"` and every `fields(...)` / attribute key is byte-identical
  to before (only `level = "debug"` is added), so dashboards and docs that
  reference those names/keys keep working.

### Requirement: SELECT path is quiet at the default level

The per-query SELECT path SHALL emit at most one INFO line at the default level;
the remaining per-query chatter SHALL be at DEBUG.

#### Scenario: One info line per SELECT at INFO

- **GIVEN** a `tracing` subscriber at INFO
- **WHEN** a single SELECT executes
- **THEN** at most one INFO line is emitted (vs ~5–7 today), the rest DEBUG
- **AND** message content is unchanged (level/volume only; AG5 owns content).

### Requirement: Subscriber-on overhead is measured

The overhead gate SHALL include a variant that measures overhead with a real
`tracing` subscriber installed at INFO (the CLI default posture), and SHALL
record that number. The variant SHALL be advisory-first (record and warn, not
fail) so the previously-unmeasured default posture becomes visible.

#### Scenario: Subscriber-on overhead number is recorded

- **WHEN** the overhead gate runs
- **THEN** it produces, alongside the existing subscriber-less number, a
  subscriber-on number measured with a fmt/`tracing` subscriber at INFO
- **AND** the variant is advisory (records + warns) in this change, leaving a
  later change to promote it to a failing threshold.
