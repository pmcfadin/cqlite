# Design — Non-allocating partition-boundary peek (K2)

## Context

`parse_partition_header_full` (`row_decoder/row_framing.rs`) is the
single partition-header parse primitive. It does three things: (1) records the H5
`PARTITION_HEADER_TRY_PARSES` gauge, (2) validates the header structure (bounds,
non-zero `u8` key length, per-format DeletionTime framing incl. the oa/da
strict-`0x80` LIVE-sentinel rule), and (3) allocates the partition key
(`data[..].to_vec()` → `RowKey`) and returns it.

The emit loop uses this as a boundary detector after every row via
`peek_is_partition_header`, which prepends a marker-flag reject (END_OF_PARTITION
`0x01` / IS_MARKER `0x02`, issue #229) and then calls the full parser and throws
away everything but the `Ok`/`Err` bit. That means every row pays (1) + (2) + (3)
just to learn a boolean.

K1 (#1640) already extracted the completeness classifier
`partition_header_readiness` → `{ Ready, Incomplete, Malformed }` (issue #1741),
computed WITHOUT consuming/allocating, used to size the need-more decision for
split chunks. K2 reuses that classifier for the peek's truncation judgment and
adds the missing piece: a non-allocating STRUCTURAL scan shared with the full
parser.

## Goals

- The post-row boundary peek allocates nothing and records no
  `PARTITION_HEADER_TRY_PARSES`.
- The peek's accept/reject decision is byte-for-byte identical to the old
  `peek_is_partition_header` for every input (no drift, no weakened validation).
- The real key allocation + gauge increment happen once per partition (at a
  confirmed start), not once per row.

## Decisions

### D1 — One shared structural scan (`scan_partition_header`)

Extract the exact byte walk of `parse_partition_header_full` into a private
`scan_partition_header(data, offset) -> Result<PartitionHeaderLayout>`, where
`PartitionHeaderLayout { key_range: Range<usize>, next_offset, partition_deletion }`
carries byte OFFSETS (not an allocated key). It performs every validation the old
full parser did and returns the identical `Error` on every failure path.

- `parse_partition_header_full` becomes: record the gauge → `scan_partition_header`
  → `data[layout.key_range].to_vec()` → `RowKey`. Its observable behavior (values
  AND error messages) is unchanged.
- The peek calls `scan_partition_header` (no gauge, no `to_vec`).

This is the drift-proofing the audit demands ("derive both from the same helper
where possible so they cannot drift"): a single structural authority means the
peek can never accept what the parser rejects.

### D2 — `peek_partition_boundary -> BoundaryPeek`

```
BoundaryPeek { Header, NotHeader, NeedMoreBytes }
```

Algorithm (all non-allocating, no gauge):

1. `data.get(offset)`: `None` → `NeedMoreBytes` (past end). Otherwise if the byte
   is an END_OF_PARTITION or range-tombstone marker → `NotHeader` (issue #229 —
   markers are not partition headers; preserves the old peek's pre-check).
2. `partition_header_readiness(&data[offset..])` (the #1741 completeness gate):
   `Incomplete` → `NeedMoreBytes`; `Malformed` → `NotHeader`.
3. `Ready` guarantees every header byte is present, so a `scan_partition_header`
   failure here is a genuine STRUCTURAL rejection (e.g. an invalid oa/da IS_LIVE
   byte), never truncation: `Ok` → `Header`, `Err` → `NotHeader`.

`peek_is_partition_header` becomes
`matches!(peek_partition_boundary(..), BoundaryPeek::Header)`.

**Equivalence to the old boolean (proved by the proptest):** the old bool was
`!marker && parse_partition_header_full(..).is_ok()`. Under step 2, a non-`Ready`
verdict maps exactly to a full-parse failure (truncation → `Incomplete`; zero key
length → `Malformed`); under `Ready` the shared `scan_partition_header` is the
same structural test the full parser runs. So `Header` ⟺ old `true` for every
input, including the oa/da byte `0x81` that `readiness` treats as a present
1-byte deletion form but the strict scan rejects (→ `NotHeader`, matching the old
`Err`).

### D3 — Reuse `partition_header_readiness` for the truncation split

Rather than teach `scan_partition_header` to distinguish "truncated" from
"malformed" (a second error taxonomy that could itself drift), the peek delegates
the completeness judgment to the already-tested #1741 classifier and only runs
the strict scan under `Ready`. Under `Ready` the scan cannot fail from
truncation, so the `Err → NotHeader` mapping is unambiguous. The double read of
the tiny header (readiness walks to the discriminator; scan walks the whole
header) is still non-allocating and vastly cheaper than the old
`to_vec`+`format!`+gauge per row.

### D4 — File-size campsite

The new code lands in `row_framing.rs` (already over the ratchet). The extraction
replaces the inline body of `parse_partition_header_full` with the shared helper +
two thin wrappers, so net growth is small; the diff is acknowledged with
`CQLITE_ALLOW_FILE_GROWTH=1` (splitting the 108KB `row_framing.rs` is out of scope
for this localized change — tracked by the #1116 source-split doctrine).

## Risks

- **A peek that accepts what the parser rejects** would mis-detect a boundary and
  desync the scan (a new bug class). Mitigation: D1's single shared scan + D2's
  proptest (`Header` ⟺ `!marker && parse.is_ok()`) over arbitrary byte prefixes
  for both DeletionTime forms.
- **Parity regression from a subtle scan slip.** Mitigation: `scan_partition_header`
  is transcribed line-for-line from the full parser (same errors); the
  multi-partition sstabledump harness + compaction byte-parity suite are the
  invariant.

## Migration

Pure internal refactor. No public API, on-disk format, or config change. New
surfaces are `pub(super)` only.
