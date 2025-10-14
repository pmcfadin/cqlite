## Data.db Format

This chapter describes the on-disk layout of partitions, rows, and cells in `Data.db`: how headers reference schema, how unfiltered rows, range tombstones, and markers are encoded, and how encodings like vints and cell flags are interpreted.

### In this chapter you will learn
- Partition headers and row/cluster layout basics
- Cell value encodings, varints/vints, collections/UDTs
- Deletions, range tombstones, TTLs and expiring cells
- How readers interpret flags and headers during parsing

## Partition and Row Layout

Minimal annotated example from `test_basic/simple_table` (trimmed and formatted):

```text
partition key = 4d4321e2-662b-4ba1-b75f-48e080727a52
row liveness ts = 2025-09-16T22:14:23.739Z
cells: account_balance=21088.5, active=false, age=75, name=(utf8) ...
```

Underlying file shows a partition stream with a serialization header followed by unfiltered rows and optional tombstone markers.

![Data.db row layout](diagrams/data-db-row-layout)
- Alt text: Annotated Data.db partition/row/cell structure
- Caption: Serialization header → unfiltered rows/markers → cells with flags and vints

## Encodings and Flags

VInt parsing (Cassandra-compatible), used across headers and lengths. For a concise implementation walkthrough, see Appendix C.

Readers interpret row/cell flags to distinguish live cells, TTLs, and tombstones; see Chapter 11 for tombstone semantics. Cross-link to Appendix B for a compact encoding summary.

Common cell flags (high level):
- live cell vs tombstone
- presence of timestamp, ttl, local deletion time
- empty/expiring cells

Bit-level flags (Cassandra 5.0, authoritative references):

| Bit | Meaning                    | When present                                |
|-----|----------------------------|---------------------------------------------|
| 0   | isDeleted                  | Cell is a tombstone                         |
| 1   | isExpiring                 | TTL fields follow                           |
| 2   | hasEmptyValue              | Zero-length value                           |
| 3   | hasTimestamp               | Timestamp present in cell header            |
| 4   | hasLocalDeletionTime       | Local deletion time present                 |
| 5+  | format extensions/reserved | Format-specific                             |

Authoritative classes to consult in Cassandra 5.0:
- `org.apache.cassandra.db.rows.*` (e.g., `Unfiltered`, `Cell`, `BufferCell`)
- `org.apache.cassandra.db.SerializationHeader`
- `org.apache.cassandra.db.rows.SerializationHelper`

Endianness:
- Integers in SSTable payloads are big-endian unless otherwise specified; varints are MSB-first variable-length.
- Network/binary compatibility relies on consistent big-endian parsing for fixed-width fields.

## Deletions and TTL Semantics

- Partition tombstone: marks entire partition deleted at a timestamp
- Row tombstone: targets a specific clustering row
- Range tombstone: spans clustering ranges
- TTL/expiring: cells carry ttl and local deletion time; expired cells are omitted at read

### Collections and UDTs (overview)
- Collections (list/set/map) serialize element counts and element/value pairs; element-level tombstones are possible and must be merged (see Ch. 11).
- UDTs serialize fields in schema order with presence bits for null handling.

### Key Takeaways
- `Data.db` is schema-driven and encodes partitions as unfiltered row streams.
- VInts and bit flags compactly encode sizes, timestamps, and cell metadata.
- Tombstones and TTLs are first-class and affect reconciliation.

### Troubleshooting
- If parsed sizes seem inconsistent, verify VInt decoding and endian assumptions.
- For collections with unexpected nulls, check for element tombstones and TTL expiration handling.

### References
- Cassandra 5.0.0:
  - Rows and tombstones: `org.apache.cassandra.db.rows.*` (`Unfiltered`, `RangeTombstoneMarker`)
  - Serialization header: [org.apache.cassandra.db.SerializationHeader](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java)
  
For implementation details, see Appendix C.


