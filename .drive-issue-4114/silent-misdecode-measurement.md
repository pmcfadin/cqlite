# #4114 AC1 — is a FULLY SILENT wrong row reachable? Measured: **YES**

Subject: Cassandra-**5.0.8**-written bytes only (`cassandra:5.0.8`, one generator,
`test-data/scripts/generate-issue-4114-vector-float.sh`). No synthesized and no
CQLite-written positive case (#3042). Reader: `./target/debug/cqlite read-sstable
<Data.db> --format json` (v0.16.1). Expected values are the sstabledump goldens
committed beside each Data.db; those agree with the inserted literals.

## The mechanism, in one line
CQLite has no vector decoder, so `VectorType(FloatType , 3)` degrades to a junk
marshal string (`marshal_type.rs:214`) and the 12-byte value is decoded as a
**blob** by `read_vint_length_prefixed_bytes`, which consumes the FIRST BYTE OF
ELEMENT 0 as a vint length. What happens next depends entirely on the numeric
value of that byte — which is user data.

## Three regimes, all measured, all on the same 12-byte type

| leading byte of v3[0] | vint len | consumes | outcome |
|---|---|---|---|
| `0x3f` (`1.0`, `1.5`, most ordinary floats) | 63 | — | **bounds check** `len > remaining` ⇒ Err, exit 5 |
| `0x00` (`0.0`), `0x08` (2^-111) | 0, 8 | 1, 9 of 12 | **row-body accounting** guard ⇒ Err, exit 5 |
| **`0x0b` (2^-105)** | **11** | **1+11 = 12 exactly** | **NO GUARD FIRES — silent wrong row, exit 0** |

The middle regime is `test_vector/vector_last` (id=1 `[0.0,1.0,2.0]` = `00000000
3f800000 40000000`; id=2 `[3.851859888774472e-34 (= 2^-111), 1.0, 2.0]`
= `08000000 3f800000 40000000`). Measured **exit 5, ZERO rows emitted**, error:

> `column 'v3' (column type floattype , 3) failed to decode at byte offset 24 of
> the row: row body under-consumed: the 1 on-disk column(s) this row declares left
> the cursor at offset 24 but the row body ends at 35 ... leaving 11 byte(s) inside
> this row accounted for by no column`

That guard is **arithmetic, not bounds** — cursor 23+1 = 24 vs body end 23+12 = 35.
So it is defeated by making the mis-decode consume the value's width EXACTLY, which
for `1 + len == 12` means `len == 11`, i.e. leading byte `0x0b`. `2^-105` encodes as
precisely `0b 00 00 00` (exponent field 127-105 = 22; top 7 bits of 22 = `0b0001011`).

## The silent case — `test_vector/vector_exact`, EXIT 0

| row | inserted / golden (expected) | CQLite returned (actual) |
|---|---|---|
| id=1 | `v3 = [2.4651903e-32, 1.0, 2.0]` (`0b000000 3f800000 40000000`) | `v3: 0x0000003f80000040000000` |
| id=2 | `v3 = [2.4651903e-32, 4.5, -5.0]` (`0b000000 40900000 c0a00000`) | `v3: 0x00000040900000c0a00000` |

Exit code **0**. Both rows emitted. **Nothing on stderr** — no error, no warning,
not even a `debug` line reaches the user. The returned value is the 11-byte TAIL of
the vector's own 12 bytes: element 0's leading byte has been eaten as a length and
the remaining 11 bytes handed back as a blob. The two blobs differ exactly where
the two rows' tails differ, so the wrong value is data-derived, not a constant.

## Verdict
**A fully silent wrong row IS reachable on Cassandra-5.0.8-written
`vector<float,n>` data.** This is silent data corruption, not merely an
unsupported feature. Two properties make the exposure worse than the single
`0x0b` witness suggests:

1. **Which regime a row lands in is decided by ONE BYTE OF USER DATA**, so a
   single table can silently corrupt some rows while failing closed on others —
   and a partition that fails closed ABORTS the whole scan, which can equally
   mask the silent rows behind it (that is why `vector_exact` is a separate
   table from `vector_last`).
2. **The silent regime is not exotic.** `len == 4n - 1` is one leading-byte value
   out of 256 for any fixed `n`, and every float sharing that leading byte (a
   whole binary32 exponent band, ~2^24 distinct values) hits it. Wider vectors
   have their own satisfying byte; `vector<float,384>` needs `len == 1535`, a
   2-byte vint, so the reachable set differs per dimension but is never empty.

No library source was changed and no decoder was written for this measurement.
