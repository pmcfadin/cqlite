# cli-rebuild — delta for atomic-publish (issue #4529)

## ADDED Requirements

### Requirement: R11 — Output is published by rename; a crash never leaves a Cassandra-discoverable partial set

`cqlite rebuild` SHALL write every regenerated and copied component under a staging directory
`<out>/.cqlite-publish-<tag>.tmp/`, fsync each file, rename each into place with `TOC.txt` second to
last and `Data.db` last, fsync the destination directory, and remove the staging directory. The
Cassandra 5.0.8 citations and ordering rationale are those of cli-salvage R10. A refused or failed
run removes its staging directory and leaves `--out` as it found it.

#### Scenario: R11.1 a crash at any point of the publish window leaves nothing Cassandra would open
- **Given** a Cassandra-written BIG or BTI input
- **When** the process is killed before the n-th rename, for every n from 0 to the file count
- **Then** no file under `<out>` outside the staging directory parses as a Cassandra component name
  unless `Data.db` is absent from them or the set equals an uninterrupted run's

#### Scenario: R11.2 leftovers are recognisable and a re-run refuses naming them
- **Given** `<out>` holding a `.cqlite-publish-*.tmp` directory from a killed run
- **When** `cqlite rebuild … --out <out>` runs again
- **Then** it exits `1`, stderr names the staging directory as an interrupted run's leftover, and
  nothing is overwritten or deleted
