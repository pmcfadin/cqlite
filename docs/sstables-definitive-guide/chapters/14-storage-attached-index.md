## Storage-Attached Index (SAI)

SAI provides secondary indexing capabilities over SSTables for numeric/text (and optionally vector, where implemented) data types. This chapter outlines on-disk artifacts at a high level and clarifies OSS Cassandra 5.0 scope.

### Scope disclaimer (OSS 5.0)

- Vector support may not be universally present in OSS Cassandra 5.0 distributions. Treat vector similarity features as implementation-dependent. Verify with your distribution’s documentation and source before relying on it in production.
- Anchor classes: `org.apache.cassandra.index.sai.*` (numeric/text). For vector capabilities, consult vendor/distribution-specific packages and release notes.

### Lifecycle notes

- SAI files are colocated with base SSTables and participate in compaction lifecycle.
- Query flow: candidate retrieval via SAI → verification against base table in `Data.db` remains authoritative.

### References

- OSS Cassandra 5.0: `index.sai` package (numeric/text) — review for exact classes present.

