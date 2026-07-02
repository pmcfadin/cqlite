## ADDED Requirements

### Requirement: A fully-expired SSTable is detected from authoritative Statistics.db metadata only

The compaction engine SHALL classify a candidate SSTable as *fully expired* for a compaction with cutoff
`gcBefore` iff its `Statistics.db` `TimestampStatistics.max_deletion_time` (Cassandra
`StatsMetadata.maxLocalDeletionTime`) is strictly less than `gcBefore`. The decision MUST be made from
that single metadata field and MUST NOT read, decode, or scan any cell of the candidate SSTable
(no-heuristics mandate, issue #28). When `gcBefore` is `None` (invalid/absent gc_grace disables purging)
or the candidate's `Statistics.db` cannot be read or parsed, the candidate SHALL NOT be classified as
fully expired.

#### Scenario: An all-expired SSTable is classified fully expired without a cell scan
- **GIVEN** a candidate SSTable whose `Statistics.db` reports `max_deletion_time` strictly less than the compaction's `gcBefore`
- **WHEN** the engine computes the fully-expired classification
- **THEN** the SSTable is classified fully expired
- **AND** the classification consulted only the `max_deletion_time` metadata field and read no rows/cells from the candidate's Data.db

#### Scenario: An SSTable holding live (non-expiring) data is never classified fully expired
- **GIVEN** a candidate SSTable whose `max_deletion_time` equals the LIVE sentinel (i32::MAX / `NO_DELETION_TIME`) or is >= `gcBefore`
- **WHEN** the engine computes the fully-expired classification
- **THEN** the SSTable is NOT classified fully expired

#### Scenario: Unknown gcBefore or unreadable Statistics.db is treated conservatively
- **GIVEN** a candidate SSTable for a compaction whose `gcBefore` is `None`, OR whose sibling `Statistics.db` is absent or fails to parse
- **WHEN** the engine computes the fully-expired classification
- **THEN** the SSTable is NOT classified fully expired
- **AND** the SSTable proceeds through the normal merge unchanged

### Requirement: A fully-expired SSTable that could shadow data outside the compaction set is not dropped (overlap safety)

The compaction engine SHALL drop a fully-expired SSTable whole ONLY when it is proven not to shadow data
living in an overlapping SSTable outside the compaction set. The proof SHALL be that the candidate's
`TimestampStatistics.max_timestamp` is strictly less than the minimum write timestamp
(`EncodingStats.minTimestamp`) across every outside overlapping SSTable — the same bound
`compute_max_purgeable_timestamp` computes. For a full/major compaction the outside set is empty and the
bound is treated as `+inf` (every fully-expired SSTable is droppable). When the outside bound is UNKNOWN
in a partial compaction (any outside `Statistics.db` unreadable), no fully-expired SSTable SHALL be
dropped.

#### Scenario: A fully-expired SSTable that shadows older data outside the set is retained
- **GIVEN** a fully-expired candidate SSTable in a partial compaction
- **AND** an EXCLUDED overlapping SSTable whose minimum write timestamp is <= the candidate's `max_timestamp`
- **WHEN** the engine computes the drop-set
- **THEN** the candidate is NOT dropped whole
- **AND** the candidate proceeds through the normal merge so its tombstones/deletions still shadow the outside data on read

#### Scenario: A fully-expired SSTable older than everything outside the set is dropped
- **GIVEN** a fully-expired candidate SSTable in a partial compaction
- **AND** every EXCLUDED overlapping SSTable has a minimum write timestamp strictly greater than the candidate's `max_timestamp`
- **WHEN** the engine computes the drop-set
- **THEN** the candidate is included in the drop-set

#### Scenario: A major compaction drops every fully-expired SSTable (empty outside set)
- **GIVEN** a full/major compaction whose input set spans every SSTable for the table (empty outside set)
- **AND** a fully-expired candidate among the inputs
- **WHEN** the engine computes the drop-set
- **THEN** the candidate is included in the drop-set (the `+inf` overlap bound is always satisfied)

#### Scenario: An unknown outside bound in a partial compaction retains all fully-expired SSTables
- **GIVEN** a partial compaction with a fully-expired candidate
- **AND** at least one EXCLUDED overlapping SSTable whose `Statistics.db` cannot be read or parsed
- **WHEN** the engine computes the drop-set
- **THEN** no SSTable is dropped whole
- **AND** every candidate proceeds through the normal merge

### Requirement: A dropped-whole SSTable is excluded from the merge output and its rows are absent

The compaction engine SHALL exclude every drop-set SSTable from the K-way merger's input list before the
merge runs, so its rows are never read, decoded, or written to the compaction output. The compaction
SHALL delete the dropped SSTable's components only after the merged output is atomically published, using
the same reclamation path as the merged inputs.

#### Scenario: Major compaction of an all-expired SSTable plus a live SSTable omits the expired rows
- **GIVEN** an SSTable containing only expired-past-grace TTL cells and a separate SSTable containing live rows
- **WHEN** a major compaction runs over both
- **THEN** none of the expired SSTable's rows appear in the compaction output
- **AND** every live row from the live SSTable appears in the compaction output
- **AND** the expired SSTable's rows were never read into the merger (it was excluded from the merger input list)

### Requirement: The compaction plan/stats record which SSTables were dropped whole

The compaction report SHALL expose the set of SSTables that were dropped whole (their paths and a count),
distinct from the merged inputs, so the drop decision is assertable from the plan/stats and not only
inferable from output absence.

#### Scenario: The report names the dropped-whole SSTables
- **GIVEN** a major compaction that drops one fully-expired SSTable and merges one live SSTable
- **WHEN** the compaction completes
- **THEN** the compaction report lists exactly the dropped SSTable in its dropped-whole set
- **AND** the dropped SSTable does not appear in the report's merged-input accounting

#### Scenario: A compaction that drops nothing reports an empty dropped-whole set
- **GIVEN** a compaction where no input is fully expired (or the overlap gate retains all candidates)
- **WHEN** the compaction completes
- **THEN** the report's dropped-whole set is empty
- **AND** the merge output is byte-for-byte identical to the pre-change behavior for that input set

### Requirement: Read parity across generations is unchanged before and after compaction

Dropping fully-expired SSTables whole SHALL NOT change the query result for any partition or generation
compared with the pre-change compaction behavior. A dropped SSTable contributes no live data (it is fully
expired), so its removal is observationally equivalent to purging its cells through the normal merge.

#### Scenario: Query results are identical before and after a drop-whole compaction
- **GIVEN** a set of SSTables containing live data plus one fully-expired SSTable
- **WHEN** the same query is run against the SSTables before compaction and against the compaction output afterwards
- **THEN** the two result sets are equal for every partition and clustering row
- **AND** the result equals the result of a compaction that merged (rather than dropped) the fully-expired SSTable through the normal purge path
