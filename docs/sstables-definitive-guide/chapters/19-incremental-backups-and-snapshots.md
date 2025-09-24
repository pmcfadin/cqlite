## Incremental Backups and Snapshots

Snapshots and incremental backups are filesystem-level artifacts that preserve SSTables at points in time and capture subsequent changes. This chapter shows their minimal directory layout and flags restore caveats without diving into operator policy.

### In this chapter you will learn
- How snapshots and incremental backups are organized on disk
- How they relate to SSTable components and lifecycle
- Basic restore considerations and pitfalls
- Where to look for metadata

## Directory Layout
Tiny example (trimmed) of a snapshot directory structure:

```startLine:endLine:filepath
# Example reference listing sourced from canonical datasets (names illustrative)
# keyspace/table-<uuid>/
# ├─ snapshots/<snapshot_name>/
# │  ├─ nb-1-big-Data.db
# │  ├─ nb-1-big-Index.db
# │  ├─ nb-1-big-Summary.db
# │  ├─ nb-1-big-Statistics.db
# │  ├─ nb-1-big-CompressionInfo.db
# │  └─ nb-1-big-TOC.txt
# └─ backups/
#    ├─ nb-2-big-Data.db
#    └─ ...
```

Notes:
- Snapshots are usually hardlinks to immutable SSTable components at a moment in time.
- Incremental backups collect subsequently flushed SSTables under `backups/`.

## Restore Considerations
Brief guidance:
- Restores must respect component sets listed in `TOC.txt`; partial copies are unsafe.
- Hardlinks preserve inode identity; copying should avoid breaking reference integrity.
- Verify `Digest.crc32` and per-chunk CRCs where present before placing files live.
- After restore, run validation tools and allow compaction to normalize overlap.

For component identification tips during restore (BIG vs BTI specifics), see Appendix C.

## Disaster Recovery Scenarios (High-Level)

- Single-node disk loss:
  - Restore latest snapshot components for affected tables
  - Reapply incremental backups in generation order; validate `TOC.txt` and digests per step
  - Run verification tool; allow repair to reconcile any residual gaps

- Multi-node partial loss:
  - Prioritize restoring quorum coverage per keyspace
  - Stagger restores to reduce cross-repair load; validate per table before enabling traffic

- Operator error (dropped table accidentally):
  - Restore snapshot under an alternate path; verify integrity
  - Use `sstabledump` to confirm content, then move into live directory once safe

## Validation Workflows

- Pre-activation checklist:
  - Components complete per `TOC.txt`
  - `Digest.crc32` matches; for compressed tables, sample per-chunk CRCs
  - Directory scanner reports no missing/unknown components

- Post-activation checks:
  - Run a light read sweep on a subset of keys; monitor errors
  - Schedule compaction to normalize overlap introduced by restore

### Key Takeaways
- Snapshots are point-in-time hardlinked component sets; backups collect later SSTables.
- Always restore complete component sets per `TOC.txt`; avoid mixing partial files.
- Validate `Digest.crc32` and chunk CRCs before activation.
- Post-restore compaction cleans overlap and rebuilds summaries if needed.

### References
- Cassandra 5.0.0 tools and storage:
  - Tools root: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/tools`
  - Descriptor (component naming): `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java`
  
For implementation details, see Appendix C.


