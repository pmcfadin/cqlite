# Issue #3811 — DEMONSTRATED defect: the consumption/bounds contract on `parse_value_from_raw_bytes`

**Status of the claim before this round:** derived entirely from READING source
(`issue-3811-decode-entry-point-census.md` §5-A/§5-B). Nobody had fed bytes through
the reader.

**Status after this round:** demonstrated. Bytes were fed through the bounded entry
point `V5CompressedLegacyParser::parse_value_from_raw_bytes` and the outputs below are
QUOTED FROM A REAL RUN, never predicted.

- **Subject tree**: `/data/lanes/lane-3811`, branch
  `issue-3811-consumption-bounds-contract-repowide`, at commit `13e589515` (the tree
  the tests were written against; they are committed as `a1ee217b9`).
- **Harness**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/issue_3811_consumption_demo_tests.rs`
  (registered in `row_decoder/mod.rs`). No dataset, reader or feature-flag dependency.
- **Command**: `CQLITE_DATASETS_ROOT=/data/datasets cargo test -p cqlite-core --lib issue_3811 -- --nocapture`
- **Oracle**: `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
  static `split(...)` (`UserType extends TupleType`), transcribed in
  `issue-3811-cassandra-oracle.md`. **No expectation in this document comes from
  CQLite's own output** (#3042).
- **Nothing was fixed.** No decoder was edited. This round is demonstration only.

---

## 0. The subject and the two arms

`parse_value_from_raw_bytes` (`raw_value.rs:89`) is documented as bounded — *"The
entire `data` slice IS the value"* (`raw_value.rs:80-86`) — and returns a bare
`Result<Value>`: **there is no consumption channel, so no caller can check even if it
wanted to.** Its two UDT arms discard the count their callee does report:

| arm | discard site | reached by | short offset published at |
|---|---|---|---|
| **marshal-form** | `raw_value.rs:458-459` (`let (val, _offset) = …`) | an `org.apache.cassandra.db.marshal.UserType(...)` type string, via `Self::is_udt_type` | `raw_type_value.rs:907` |
| **registry-resolved bare name** | `raw_value.rs:479-480` (`let (val, _offset) = …`) | a bare UDT name resolved by `UdtRegistry::get_udt_qualified` | `raw_type_value.rs:1087` |

Both were driven with the same four byte-vectors, from one parser instance carrying a
registry entry for `addr { street text, city text }` in keyspace `issue_3811_ks`.

## 1. The oracle's rules, in the order Cassandra tests them

The ORDER is the content: it is what separates a legal omission from a corruption.

1. `position == length` before component `i` ⇒ **LEGAL** short return; `i..n` absent.
2. else `position + 4 > length` ⇒ **throw** `"Not enough bytes to read %dth component"`.
3. after the loop, `position < length` ⇒ **throw** `"Expected N values for <type> column, but got more"`.

### A correction to the plan's vector table, recorded rather than quietly applied

`issue-3811-implementation-plan.md` row 3 spells the partial-prefix case as *"case 1
`|| 0x00`"* and says it yields rule 2 (`"Not enough bytes"`). **Under the oracle that
is not what happens.** With every declared field present the component loop is already
exhausted when the stray byte is reached, so rule 2 is never evaluated and the verdict
is rule 3 (`"but got more"`) — the same class as case 2. Rule 2 is reachable ONLY when
a declared field is still to be read, i.e. from the *legally short* encoding plus 1–3
stray bytes.

That reading is also the only one under which the plan's own claim *"cases 3 and 4 are
one byte apart"* is true (11 B vs 12 B; case 1 `|| 0x00` is 19 B, eight bytes away). So
**case 3 below is `case 4 || 0x00`**, and the plan's literal spelling is carried as a
clearly-labelled SUPPLEMENTARY row per arm so the collapse is on record for it too.

## 2. The vectors

`addr { street text, city text }`; component framing is `[i32 BE length][raw bytes]`.

| case | bytes (hex) | len |
|---|---|---|
| 1 — exact | `00 00 00 07 6d 61 69 6e 20 73 74 00 00 00 03 6e 79 63` | 18 |
| 2 — trailing garbage | `00 00 00 07 6d 61 69 6e 20 73 74 00 00 00 03 6e 79 63 aa` | 19 |
| 3 — partial 1-byte prefix | `00 00 00 07 6d 61 69 6e 20 73 74 00` | 12 |
| 4 — legally short | `00 00 00 07 6d 61 69 6e 20 73 74` | 11 |
| supplementary — exact `\|\| 0x00` | `00 00 00 07 6d 61 69 6e 20 73 74 00 00 00 03 6e 79 63 00` | 19 |

## 3. Results — the 8 mandated rows

Every "CQLite ACTUALLY did" cell is copied verbatim from the run's `--nocapture` output.

### Arm 1 — marshal-form UDT (`raw_value.rs:458-459`)

| # | case | input (hex) | Cassandra `TupleType.split` | CQLite ACTUALLY did | VERDICT |
|---|---|---|---|---|---|
| 1 | exact | `00 00 00 07 6d 61 69 6e 20 73 74 00 00 00 03 6e 79 63` | **accept** — loop completes, `position(18) == length(18)`, no rule fires | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` | **ALREADY CORRECT** (control) |
| 2 | trailing garbage | `… 6e 79 63 aa` | **throw** — rule 3, `position(18) < length(19)` ⇒ `"Expected 2 values for … column, but got more"` (`TupleType.java:329-335`) | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` — the `0xaa` is silently discarded | **DEFECT CONFIRMED** |
| 3 | partial 1-byte prefix | `00 00 00 07 6d 61 69 6e 20 73 74 00` | **throw** — rule 1 does not fire (`position(11) != length(12)`), then rule 2, `position(11) + 4 > length(12)` ⇒ `"Not enough bytes to read 1th component"` (`TupleType.java:311-312`) | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: None }] }))` — the stray `0x00` is read as an OMITTED FIELD | **DEFECT CONFIRMED** |
| 4 | legally short | `00 00 00 07 6d 61 69 6e 20 73 74` | **accept** — rule 1, `position(11) == length(11)` before component 1 ⇒ short return, `city` null (`TupleType.java:308-309`) | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: None }] }))` | **ALREADY CORRECT** — and it must STAY accepted; a naive "all fields present" fix breaks exactly this |
| S | supplementary, exact `\|\| 0x00` | `… 6e 79 63 00` | **throw** — rule 3, as case 2 | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` | **DEFECT CONFIRMED** |

### Arm 2 — registry-resolved bare UDT name (`raw_value.rs:479-480`)

| # | case | input (hex) | Cassandra `TupleType.split` | CQLite ACTUALLY did | VERDICT |
|---|---|---|---|---|---|
| 1 | exact | `00 00 00 07 6d 61 69 6e 20 73 74 00 00 00 03 6e 79 63` | **accept** — as above | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` | **ALREADY CORRECT** (control) |
| 2 | trailing garbage | `… 6e 79 63 aa` | **throw** — rule 3 | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` | **DEFECT CONFIRMED** |
| 3 | partial 1-byte prefix | `00 00 00 07 6d 61 69 6e 20 73 74 00` | **throw** — rule 2 | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: None }] }))` | **DEFECT CONFIRMED** |
| 4 | legally short | `00 00 00 07 6d 61 69 6e 20 73 74` | **accept** — rule 1 | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: None }] }))` | **ALREADY CORRECT** |
| S | supplementary, exact `\|\| 0x00` | `… 6e 79 63 00` | **throw** — rule 3 | `Ok(Udt(UdtValue { type_name: "addr", keyspace: "issue_3811_ks", fields: [UdtField { name: "street", value: Some(Text(b"main st")) }, UdtField { name: "city", value: Some(Text(b"nyc")) }] }))` | **DEFECT CONFIRMED** |

**The two arms are observationally identical on all five vectors.** They are separate
code paths (`raw_type_value.rs:644` vs `:913`) with separate loop guards
(`:697` vs `:934`) and separate offset publications (`:907` vs `:1087`), and both are
wrong in exactly the same two ways — so a fix landing on one arm and not its sibling,
which is #3631's history, would be invisible to a suite that tested only one.

## 4. AC4 — two distinct serialized inputs collapse to ONE `Value`

Both collapses were asserted directly (`collapse_case1_vs_case2_yields_one_value_today`,
`collapse_case4_vs_case3_yields_one_value_today`) and both hold TODAY on BOTH arms:

- **case 1 (18 B, legal) `==` case 2 (19 B, corruption)** — the trailing-bytes half.
- **case 4 (11 B, legal) `==` case 3 (12 B, corruption)** — the partial-prefix half,
  ONE BYTE apart. A suite containing only the first would pass over a fix that got
  this boundary wrong.

## 5. What was NOT demonstrated — declared, not implied

- **`COULD NOT REACH`: none.** Both arms were reachable through the bounded entry
  point with a plainly-constructed parser; nothing here is blocked on a registry or
  fixture that could not be built.
- **Rule 3 of the oracle only — for the census's `position + size > length` case
  (a declared field length that overruns the buffer, plan row 6) no vector was run
  this round.** It is a distinct rule and is untested here; treat it as unmeasured,
  not as passing.
- **Case 3's error MESSAGE class was not observed**, only its acceptance: CQLite
  returns `Ok`, so there is no message to compare. The distinction between rule 2 and
  rule 3 is therefore established from the oracle, not from CQLite behaviour, and will
  only become observable after the fix.
- **These vectors are CQLite-constructed, not Cassandra-written.** That is sound here
  because the property under test is a REFUSAL derived from Cassandra's source
  (#3042's rule is that the *expectation* must not come from CQLite's own output, and
  it does not) — but it means this round says nothing about whether real
  Cassandra-written corpus data would newly hit the refusal. The plan's
  before/after 155-table corpus measurement is still owed and is NOT satisfied by this
  document.
- **Nothing here measures the other census findings (A's collection arms, C, D, E, F,
  G, H).** Only §5-B's two named UDT arms were driven.

## 5b. Run record

```
$ CQLITE_DATASETS_ROOT=/data/datasets cargo test -p cqlite-core --lib issue_3811 -- --nocapture
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 3624 filtered out; finished in 0.00s
```

12 = 10 case tests (5 vectors x 2 arms) + 2 AC4 collapse tests. **They pass because they
pin the DEFECT.** Six of the twelve (`*_is_accepted_today_*` x4, `collapse_*` x2) assert
the accepting behaviour that Cassandra refuses; they are designed to go RED the moment
#3811's fix lands, and the file header carries the flip-on-fix checklist. The four
control tests (cases 1 and 4 on each arm) must stay green through the fix — case 4 in
particular is what a naive "all declared fields must be present" fix would break.

## 6. Consequence for the fix

The demonstration matches the oracle's analysis exactly: `current_offset` goes short by
two distinct routes (the partial-prefix `break` that does not advance, and the
unconsumed trailing bytes), and **both surface as the same observable** — a reported
consumption less than `data.len()`. So one `consumed == slice.len()` comparison at the
bounded caller refuses cases 2, 3 and the supplementary while leaving cases 1 and 4
untouched. Case 4 passing today is the constraint that makes the check a comparison
rather than an "all fields present" assertion.

The harness is written to FAIL when that fix lands: the four `*_is_accepted_today_*`
tests and the two `collapse_*` tests pin the current wrong behaviour deliberately and
carry a flip-on-fix checklist in the file header.
