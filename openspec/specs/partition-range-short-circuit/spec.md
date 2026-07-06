# partition-range-short-circuit Specification

## Purpose
TBD - created by archiving change bti-first-last-key-short-circuit. Update Purpose after archive.
## Requirements
### Requirement: A point read outside the SSTable key range short-circuits with zero presence work
A point read SHALL return authoritative absence (`Ok(None)`) — BEFORE any bloom check,
`Index.db` probe, or BTI trie descent — whenever its query partition key sorts OUTSIDE the
SSTable's authoritative `[first_key, last_key]` bound. The bound SHALL come only from
authoritative metadata (`Summary.db`), and the comparison SHALL be performed in Cassandra
partition order — ascending Murmur3 token, ties broken by unsigned-lexicographic key bytes —
not by raw bytes. A work counter (`RANGE_SHORT_CIRCUITS`) incremented at the single
short-circuit site SHALL make the behavior verifiable.

#### Scenario: An out-of-range point read performs zero presence work
- **WHEN** a partition key whose token sorts strictly outside the SSTable's
  `[first_key, last_key]` bound is point-read via `get_with_resolution`, with the read-work
  counters reset immediately before
- **THEN** the read returns `Ok(None)` (authoritative absence)
- **AND** the `RANGE_SHORT_CIRCUITS` counter reads exactly 1
- **AND** the `INDEX_PROBES` counter reads 0 (the presence path was never reached)

#### Scenario: An in-range point read is unchanged
- **WHEN** a present in-range partition key is point-read via `get_with_resolution`, with the
  counters reset immediately before
- **THEN** the `RANGE_SHORT_CIRCUITS` counter reads 0 (the short-circuit did not fire)
- **AND** the read reaches the real presence path (`INDEX_PROBES >= 1`)

### Requirement: The range bound never rules out a present partition
The range short-circuit SHALL NOT change the result of any in-range read and SHALL NOT drop a
present partition. The `Summary.db` `first_key`/`last_key` SHALL equal the min-token /
max-token present partition keys, the bound SHALL be INCLUSIVE at both ends (a key equal to
`first_key` or `last_key` is in range), and a key that is in range but absent SHALL NOT be
short-circuited. When no authoritative bound is available (no `Summary.db`, e.g. a BTI reader)
the check SHALL report "cannot rule out" so the normal presence path runs unchanged.

#### Scenario: The Summary bound equals the true token extent
- **WHEN** the `Summary.db` `first_key`/`last_key` are read for a BIG SSTable and every raw
  partition key is enumerated from `Index.db`
- **THEN** `first_key` equals the min-token present key and `last_key` equals the max-token
  present key (byte-for-byte)

#### Scenario: No present or boundary or in-range-absent key is ruled out
- **WHEN** `partition_key_out_of_range` is evaluated for every present partition key,
  including the two boundary keys equal to `first_key`/`last_key`, and for an in-range-but-
  absent key
- **THEN** it returns `false` for all of them (none is short-circuited)
- **AND** it returns `true` only for a key whose token sorts strictly outside the bound

### Requirement: The dead and wrong BTI trie scaffolding is removed
The unused, incorrectly-decoding BTI trie scaffolding SHALL be deleted after proving it has
no live references. `bti/nodes.rs` (`NodeParser`, `TrieNode`, and their helpers) and
`BtiNode::get_transitions` (whose `Dense` arm silently returns an empty transition set) SHALL
be removed, and the crate SHALL continue to build on every feature configuration.

#### Scenario: The deleted scaffolding has zero references and the crate still builds
- **WHEN** the workspace is searched for references to the deleted symbols after removal
- **THEN** no non-test, non-doc-comment reference to `bti::nodes`, `NodeParser`, `TrieNode`,
  or `BtiNode::get_transitions` remains
- **AND** the crate builds under the default features, the `cli-helpers,work-counters`
  configuration, and the minimal `--no-default-features --features all-compression`
  configuration

