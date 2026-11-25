# Glossary

Terms the code, specs and issues use with one meaning. Source of the discovery terms:
`openspec/changes/generation-discovery/` (#4626).

**Generation.** One SSTable: the files that share a descriptor (`nb-3-big-Data.db`,
`da-5-bti-Partitions.db`, ...) in one directory.
Avoid: "sstable file", "table", "run" (a run is a compaction notion).

**Generation id.** The `<id>` part of the descriptor: a sequence id (`3`, at most 10 digits, within the
Java `int` range) or a UUID id (`3gb2_0rbd_2lhua2pw7a2hlyvbsp`). BIG and BTI, and every version letter,
share one id space. Sequence ids sort before UUID ids.
Avoid: "generation number" (UUID ids are not numbers), "sstable id" in new code.

**Live set.** The generations Cassandra would load from a directory after applying transaction logs. A
generation without `TOC.txt` is live; an Orphan is not.
Avoid: "all SSTables in the directory", "active sstables".

**Scope.** Which one directory a listing reads: Live, Backups, Live+Backups, or Snapshot(tag). Never
recursive.
Avoid: "path filter", "recursive scan".

**Condition.** One of a closed set of named facts about a generation: `TocMissing`, `Orphan`,
`UnsupportedVersion`, `Unreadable`, `Corrupt`, `Absent`. Each verb words it its own way.
Avoid: "error", "warning", "status" for these.

**Trusted metadata.** A generation's `Statistics.db` content as returned by the module's one accessor:
floor checked, bounded read, integrity checked, parsed once, cached (value or typed failure).
Avoid: "stats", "lenient parse", "best-effort metadata".

**Verb.** A `cqlite` subcommand (`compact`, `explain`, `rebuild`, ...). A verb has its own output
vocabulary and exit codes.
Avoid: "command" when the subcommand is meant, "tool".

**Forensics tool.** A read-only verb that inspects possibly damaged data without changing it (`explain`,
`find`, `diagnose`, `diff`, `verify`, the 0.19 set). It accepts TocMissing and reports conditions rather
than hiding generations.
Avoid: "debug command", "repair tool" (repair writes).

**Gate of record.** The one full `scripts/agent-gate.sh` run on Linux, once per issue right before merge,
whose `==== AGENT-GATE SUMMARY ====` block is the verdict. Ad-hoc cargo runs and `--lite` or `--only`
runs never count.
Avoid: "CI" (CI is not yet the gate of record), "the tests".
