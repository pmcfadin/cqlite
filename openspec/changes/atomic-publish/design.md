# Design — atomic-publish (#4529)

**D1 Staging directory, not per-file temp names.** One directory `<out>/.cqlite-publish-<tag>.tmp/`
holds the generation in its final layout (`<keyspace>/<table>/` for salvage, flat for rebuild).
Writers are unchanged apart from their root. A dot-prefixed `.tmp` directory is recognisable, never a
keyspace/table directory, and Cassandra lists one directory without recursing.

**D2 Publish order: everything else, then `TOC.txt`, then `Data.db`.** Renames cannot be collectively
atomic. At 5.0.8 a final-named set lacking `Data.db` is refused (`SSTableReaderLoadingBuilder.build`
`checkArgument`), whereas a set with `Data.db` could be opened from whichever components exist. So the
rename window never exposes an openable partial set; it exposes at worst a Data.db-less set.

**D3 Leftovers refuse, they are not auto-cleaned.** An existing `.cqlite-publish-*.tmp` makes the verb
exit `1` naming it: interleaving with an interrupted run's files, or deleting files the operator may
want to inspect, are both worse than asking. The verbs already refuse a non-empty `--out`; the message
now names the cause. A refused or failed run removes its own staging directory.

**D4 Test seam.** In debug builds `CQLITE_TEST_PUBLISH_ABORT=<n>` aborts before the n-th rename.
Release builds compile it out. The kill-at-every-boundary proof is #4498's `issue_4529_*` harness.
