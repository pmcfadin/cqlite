# bti-candidate-prune Specification

## Purpose
TBD - created by archiving change hoist-candidate-key-rehash. Update Purpose after archive.
## Requirements
### Requirement: A multi-candidate BTI point read hashes the query key exactly once
A `WHERE pk = ?` point read against a set of BTI (`da`) candidate SSTables SHALL compute the
Murmur3 hash + byte-comparable encoding of the query partition key EXACTLY ONCE per read,
regardless of how many candidate generations are pruned — not once per candidate. The
encoding SHALL be hoisted out of the candidate-prune loop and reused for every candidate's
trie prune. A pre-encoded prune entry point (`might_contain_partition_encoded`) that accepts
the already-encoded key SHALL be provided, and a work counter (`KEY_HASH_CALLS`) incremented
at the single BTI key-encoding site SHALL make the property verifiable.

#### Scenario: The query key is hashed once across an N-generation fan-out
- **WHEN** a present partition key is pruned across N distinct BTI candidate readers (each
  with an independent same-key memo) via the hoisted pre-encoded prune, with the read-work
  counters reset immediately before
- **THEN** the `KEY_HASH_CALLS` counter reads exactly 1
- **AND** pruning the SAME N candidates via the retained per-candidate raw-key path reads
  `KEY_HASH_CALLS == N` (the redundant per-candidate rehash this change removes)

#### Scenario: An absent key is also hashed once across the fan-out
- **WHEN** an absent partition key is pruned across N distinct BTI candidate readers via the
  hoisted pre-encoded prune, with the counters reset immediately before
- **THEN** the `KEY_HASH_CALLS` counter reads exactly 1
- **AND** every candidate reports definitive trie absence (the key is admitted by none)

#### Scenario: A real point read through the query engine hashes the key once
- **WHEN** a known-present key is point-read through the public `Database` query API against
  a BTI SSTable, with the counters reset immediately before
- **THEN** the `KEY_HASH_CALLS` counter reads exactly 1
- **AND** the read returns the expected rows

### Requirement: The hoisted prune is byte-identical to the per-candidate prune
Hoisting the key encoding SHALL NOT change the pruning decision or the resolved partition
location. The set of candidates admitted by the pre-encoded prune SHALL equal the set
admitted by the per-candidate raw-key prune for the same key, and the `Partitions.db` trie
SHALL remain the authoritative presence oracle for BTI (a trie miss is definitive absence).
The per-SSTable trie walk SHALL remain per-candidate (only the key hash is hoisted, not the
walk), and the pinned `test_da/simple_table` resolved offsets (0/63/125) SHALL be unchanged.

#### Scenario: The hoisted prune admits the identical candidate set
- **WHEN** the same present key is pruned across the same N BTI candidates via both the
  hoisted pre-encoded path and the retained per-candidate raw-key path
- **THEN** both admit all N candidates (identical decision)
- **AND** the hoisted path still records at least N trie walks (one per candidate), proving
  only the hash — not the walk — was hoisted

#### Scenario: A BIG candidate is pruned unchanged
- **WHEN** a candidate reader is BIG (`nb`) rather than BTI (it has no `Partitions.db`
  encoding to hoist)
- **THEN** the pre-encoded prune falls back to its raw-key bloom check, so a non-BTI or mixed
  candidate set is pruned exactly as before

