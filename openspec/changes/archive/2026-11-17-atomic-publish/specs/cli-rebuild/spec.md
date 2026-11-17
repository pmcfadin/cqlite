# cli-rebuild — delta for atomic-publish (issue #4529)

## ADDED Requirements

### Requirement: R11 — Output is published by rename; a crash never leaves a partial set Cassandra would load

`cqlite rebuild` SHALL write every regenerated and copied component under a staging directory
`<out>/.cqlite-publish-<tag>.tmp/`, fsync each file, rename each into place with `TOC.txt` second to
last and `Data.db` last, fsync the destination directory, and remove the staging directory. The
Cassandra 5.0.8 citations and ordering rationale are those of cli-salvage R10. Before publication starts, a refused or
failed run removes its staging directory and leaves `--out` as it found it; once publication has
started, a failed run keeps its staging directory so the leftover stays recognisable (R11.2).

#### Scenario: R11.1 a crash at any point of the publish window never leaves a loadable partial set
- **Given** a Cassandra-written BIG or BTI input
- **When** the process is killed before the n-th rename, for every n from 0 to the file count
- **Then** during the rename window final-named files may exist, but never `Data.db` until the set is
  identical to an uninterrupted run's; Cassandra never loads a partial set (AC1, owner ruling
  2026-11-16, as cli-salvage R10.1)

#### Scenario: R11.2 leftovers are recognisable and a re-run refuses naming them
- **Given** `<out>` holding a `.cqlite-publish-*.tmp` directory from a killed run
- **When** `cqlite rebuild … --out <out>` runs again
- **Then** it exits `1`, stderr names the staging directory as an interrupted run's leftover, and
  nothing is overwritten or deleted
