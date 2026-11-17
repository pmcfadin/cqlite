# atomic-publish — issue #4529

**Milestone:** 0.19. **Routing:** OpenSpec — it changes the on-disk failure behaviour of two shipped
verbs (`cqlite salvage`, `cqlite rebuild`). Owner approved the remedy and both acceptance criteria
2026-11-14; AC1 was reworded by the owner ruling of 2026-11-16 (see design.md).

## Why

Found by the #4498 crash-consistency harness. Both verbs wrote each component under its FINAL name
and relied on `TOC.txt` being written last as the "publication barrier". That protects CQLite
readers only. Cassandra 5.0.8 discovers SSTables by file name:

- `Directories.SSTableLister.getFilter` passes every file in the directory to
  `SSTable.tryComponentFromFilename` -> `Descriptor.fromFileWithComponent` and groups the parsable
  ones by `Descriptor`; no TOC is required.
- `TOCComponent.loadOrCreate` REBUILDS a missing TOC from the component files present.
- `SSTableReaderLoadingBuilder.build` begins with
  `checkArgument(components.contains(Components.DATA), "Data component is missing for sstable %s")`.

A SIGKILL mid-write therefore left a final-named partial set Cassandra would load (21 of 27 crash
points for rebuild, 25 of 35 for salvage).

## What changes

Every component is written under a hidden staging directory `<out>/.cqlite-publish-<tag>.tmp/`
(Cassandra's lister does not descend into it; the name fails `Descriptor.filenameTokens` /
`Version.validate`). When the whole set is on disk each file is fsynced, renamed into place with
`Data.db` LAST and `TOC.txt` second to last, the directories are fsynced and the staging directory is
removed. A crash before the first rename leaves only the staging directory. During the rename window
final-named files may exist, but never `Data.db` until the set is identical to an uninterrupted run;
Cassandra refuses a Data.db-less set. AC1 (owner ruling 2026-11-16): "After a crash at any boundary, Cassandra never loads a partial set: any final-named partial set lacks `Data.db`." A re-run refuses, naming
the leftover.
