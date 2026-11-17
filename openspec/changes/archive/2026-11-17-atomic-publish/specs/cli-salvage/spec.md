# cli-salvage — delta for atomic-publish (issue #4529)

## ADDED Requirements

### Requirement: R10 — Output is published by rename; a crash never leaves a partial set Cassandra would load

`cqlite salvage` SHALL stage every component under `<out>/.cqlite-publish-<tag>.tmp/`, fsync each file,
rename each into place with `TOC.txt` second to last and `Data.db` last, fsync the directories and
remove the staging directory. Cassandra 5.0.8 discovers by file name (`Directories.SSTableLister.getFilter`,
`TOCComponent.loadOrCreate`) and refuses a set without `Data.db` (`SSTableReaderLoadingBuilder.build`).

#### Scenario: R10.1 a crash at any point of the publish window never leaves a loadable partial set
- **Given** a Cassandra-written BIG (`test_comp.lz4_table`) or BTI (`test_da.multiclustering_table`) input
- **When** the process is killed before the n-th rename, for every n from 0 to the file count
- **Then** during the rename window final-named files may exist, but never `Data.db` until the set is
  identical to an uninterrupted run's; Cassandra never loads a partial set. AC1 (owner ruling
  2026-11-16): "After a crash at any boundary, Cassandra never loads a partial set: any final-named partial set lacks `Data.db`." Citation: `SSTableReaderLoadingBuilder.java:74` at `cassandra-5.0.8`,
  `checkArgument(components.contains(Components.DATA), "Data component is missing…")`
  (`cqlite-cli/tests/issue_4529_atomic_publish.rs`, #4498 `issue_4529_*`)

#### Scenario: R10.2 leftovers are recognisable and a re-run refuses naming them
- **Given** `<out>` holding a `.cqlite-publish-*.tmp` directory from a killed run
- **When** `cqlite salvage … --out <out>` runs again
- **Then** it exits `1` and stderr names the staging directory as an interrupted run's unpublished,
  incomplete files; nothing is overwritten or deleted
