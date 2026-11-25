# cli-explain — delta (generation-discovery, issue #4626)

`explain` resolves its generations through the `generation-discovery` module, replacing
`explain_resolve::discover_live_generations`. Existing requirements R5 to R9 are unchanged. All
requirements are ADDED.

## ADDED Requirements

### Requirement: GD5 — explain lists the Live set, shows conditions, and refuses by name

`explain` SHALL list the table directory with Scope Live and TocMissing accepted. The first line's
`generations=<n> [<basename>…]` (R5) SHALL list every generation in id order, UUID ids included, sequence
ids first. A `TocMissing` generation SHALL be listed with the suffix `(toc-missing)`. A below-floor
generation, a `Corrupt` or `Unreadable` generation, and a `TxnInconsistent` directory SHALL exit `2`
naming the path (R8), with stdout carrying no cell lines. An `Orphan` SHALL be named on stderr and
SHALL NOT appear in the generation list.

#### Scenario: GD5.1 committed compaction's inputs are not read
- **Given** a Cassandra-written directory with `nb-1-big`, `nb-2-big`, `nb-3-big` and a committed
  `nb_txn_compaction_<uuid>.log` removing 1 and 2
- **When** `explain` runs
- **Then** the first line says `generations=1 [nb-3-big]`.

#### Scenario: GD5.2 UUID ids are ordered after sequence ids
- **Given** `nb-7-big-*` and `nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-*`
- **When** `explain` runs
- **Then** the list is `[nb-7-big, nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big]`.

#### Scenario: GD5.3 TocMissing is visible
- **Given** `nb-1-big-TOC.txt` deleted
- **When** `explain` runs
- **Then** `nb-1-big (toc-missing)` appears in the first line and the exit code is `0`.

#### Scenario: GD5.4 inconsistent directory is refused
- **Given** an incomplete transaction log whose old files are missing
- **When** `explain` runs
- **Then** exit `2`, stderr names the log, stdout carries no cell lines.
