# bti-trie-descent Specification

## Purpose
TBD - created by archiving change bti-zero-copy-slice-walk. Update Purpose after archive.
## Requirements
### Requirement: BTI partition lookup walks the resident trie buffer with no whole-file copy
The BTI `Partitions.db` partition lookup on the production read path SHALL resolve a partition key by
walking a borrowed `&[u8]` view of the reader's already-resident trie buffer, and SHALL NOT allocate a
buffer proportional to the trie/file size per lookup. A slice-based lookup entry point that reads the
big-endian root offset from the last 8 bytes of the buffer and walks it in place SHALL be provided, and
the resolved `BtiPartitionLocation` SHALL be byte-identical to the retained `Read + Seek` entry point.

#### Scenario: The slice lookup returns the same location as the stream lookup
- **WHEN** the slice-based lookup and the `Read + Seek` lookup are run over the same `Partitions.db`
  bytes for the same encoded key (a synthetic trie and the real `test_da/simple_table` fixture)
- **THEN** both return the identical `BtiPartitionLocation` for every present key
- **AND** both return `None` for an absent key

#### Scenario: Pinned test_da offsets resolve unchanged
- **WHEN** the slice lookup resolves the `test_da/simple_table` fixture keys whose leaves sit at trie
  offsets 0, 3, and 6
- **THEN** they resolve to Data.db offsets 0, 63, and 125 respectively

#### Scenario: A BTI point read allocates no trie-sized buffer
- **WHEN** a single BTI partition point lookup runs on the production path over a resident
  `Partitions.db` buffer
- **THEN** no heap allocation proportional to the `Partitions.db` size (the former whole-file copy) is
  made for that lookup

### Requirement: BTI child descent resolves one child pointer in place without allocation
Descending one BTI trie node to follow one key byte SHALL decode only the target child pointer directly
from the node's byte slice, for every `TrieNode` ordinal (PayloadOnly, Single 4/8/12/16, Sparse
8/12/16/24/40, Dense 12/16/24/32/40, LongDense), and SHALL NOT allocate the node's full child table.
The resolved child offset SHALL equal the offset produced by the existing full node parse, and a
structurally invalid or truncated node SHALL yield an error, never a silent miss.

#### Scenario: In-place child resolution matches the full parse for every ordinal
- **WHEN** the in-place `find_child_offset` and the full `parse_bti_node(...).find_child(...)` are run
  on the same crafted node bytes for each node ordinal, including a Dense node whose first real child
  is at absolute offset 0 and whose range has a gap (delta-0 sentinel)
- **THEN** both agree on `Some(child_offset)` for a present transition byte
- **AND** both agree on `None` for an absent transition byte or a delta-0 Dense slot

#### Scenario: A truncated node is an error, not a miss
- **WHEN** `find_child_offset` is asked to follow a byte through a node whose pointer area is truncated
  past the end of the buffer
- **THEN** it returns an error (the same structural failure the full parse reports), not `Ok(None)`

### Requirement: A single-candidate BTI point read descends the trie exactly once
A single-candidate `WHERE pk = ?` point read against a BTI SSTable SHALL descend the `Partitions.db`
trie exactly once — the candidate-prune resolution SHALL be reused by the partition seek rather than
re-walked. The `Partitions.db` trie SHALL remain the authoritative presence oracle for BTI (no bloom
filter), and the resolved partition location SHALL be unchanged.

#### Scenario: TRIE_WALKS is 1 for a single-candidate BTI point read
- **WHEN** a known-present key is point-read through the public query API against a single BTI SSTable,
  with the read-work counters reset immediately before
- **THEN** the `TRIE_WALKS` counter reads exactly 1 (previously 2)
- **AND** the read returns the same rows it returned before the change

#### Scenario: A trie miss is still authoritative absence
- **WHEN** a key that has no trie path is point-read against a BTI SSTable
- **THEN** the lookup reports definitive absence (no rows) using the trie as the presence oracle

