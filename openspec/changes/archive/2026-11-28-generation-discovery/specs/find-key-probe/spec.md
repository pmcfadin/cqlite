# find-key-probe — delta (generation-discovery, issue #4626)

`cqlite find` enumerates the generations it probes through the `generation-discovery` module (replacing
the listing in `cqlite-cli/src/commands/find.rs`). F1 to F7 are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD7 — find probes the Live set and reports a generation it cannot probe

`find` SHALL probe every generation in the Live set (Scope Live, TocMissing accepted), UUID ids
included. It SHALL NOT silently omit a generation it cannot probe: a below-floor, `Corrupt` or
`Unreadable` generation SHALL appear in the per-generation list with that condition and SHALL prevent an
`unflushed` proof (F4), as an unreadable generation already does. A `TxnInconsistent` directory SHALL be
refused. F5's zero-generation case counts only the Live set.

#### Scenario: GD7.1 a hidden generation is not probed
- **Given** a committed compaction that removed `nb-1-big` while its files remain on disk
- **When** `find` runs for a key held by `nb-1-big`
- **Then** `nb-1-big` is not probed and the key is reported from the surviving output only.

#### Scenario: GD7.2 unprobeable generation blocks `unflushed`
- **Given** `nb-2-big-Statistics.db` truncated
- **When** `find` runs for a key absent from every readable generation
- **Then** the result is not `unflushed`, and `nb-2-big` is listed with `Corrupt`.
