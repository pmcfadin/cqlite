# Issue #3811 AC1 — structured-decode entry-point census for `row_decoder/`

**Subject tree**: `/data/lanes/lane-3811`, branch `issue-3811-consumption-bounds-contract-repowide`,
`HEAD = d23403d1e9534ac2b45bc5733e1ac54b735cb8d9` (identical to `origin/main` at census time).
**Census date**: 2026-09-01.

> **READ THIS FIRST — #3631 IS NOT MERGED, so this census is of a tree that does not have its
> contract.** `git merge-base --is-ancestor 8099fa723 origin/main` → **NO**. #3631's work lives on
> the open PR **#3820** (`origin/issue-3631-structured-values-blob-degrade`), which adds a new file
> `row_decoder/typed_value.rs` carrying `require_fully_consumed` (line 310),
> `parse_typed_value` (372) and `parse_typed_value_reporting` (394). The issue text for #3811 says
> "#3631 fixes the silent-blob class" and "that contract is now enforced at the sites #3631
> reached" — on `main` **it is enforced at exactly ONE site**, and that site came from **#3612**
> (merged as PR #3736), not #3631. Section 7 records what changes when #3820 lands. Everything in
> sections 1–6 is measured against `HEAD` above.

---

## 1. Definition of "structured-decode entry point", and what was excluded

### Definition used

A function in the subject file set that

- **(a)** receives **serialized bytes** — a `&[u8]`, optionally with a `usize` / `&mut usize`
  cursor into it — **AND**
- **(b)** receives a **DECLARED type** for those bytes from authoritative metadata, in any of the
  spellings this module accepts: a CQL-short/marshal type `&str`, a `&CqlType`, a `&UdtTypeDef`,
  a `&crate::schema::Column`, a `&crate::schema::ClusteringColumn`, or a `&TableSchema` from which
  a per-column/per-clustering type is read — **AND**
- **(c)** produces a `Value`, a `Vec<Value>`, or a struct whose payload is a decoded `Value`.

Private functions are IN. A bounded caller inside the module is still a caller, and #3631's round-3
enumeration failed precisely by stopping at the `pub(super)` surface.

**One deliberate widening of (a):** `create_empty_value_for_type` takes **no** bytes — it is the
ZERO-LENGTH arm of the same dispatch, is one of the four arms #3631 names, and produces a `Value`
from a declared type. Excluding it on a signature technicality would drop a known member of the
subject class, so it is IN and flagged.

### Excluded, and why

| excluded | count | reason |
|---|---|---|
| test files (`*_tests.rs`, `test_support.rs`) | 14 of 38 `.rs` files | not the production surface |
| functions inside `#[cfg(test)]` items in surviving files | 122 of 297 `fn` decls | same |
| `fn`s that take `&[u8]` but produce no `Value` | 41 | framing/skip/probe (`skip_row_framing`, `parse_row_flags`, `parse_row_metadata`, `parse_partition_header*`, `peek_partition_boundary`, `skip_complex_cell`, `read_frozen_count`, `read_frozen_preamble`, `read_vint_length_prefixed_bytes`, the `on_data_row`/`on_range_marker` policy hooks, all `parse_block*` drivers) — they satisfy (a) but not (c) |
| `fn`s that name `Value` but take no bytes | 3 of 4 | the script found 4 (35 `Value`-naming − 31 both). Three are excluded — `peeled_for_inspection`, `extract_clustering_values`, `row_tombstone` — post-decode manipulation, satisfying (c) not (a). The fourth, `create_empty_value_for_type`, is the deliberate widening above and is IN. |
| `block_emit.rs:337 parse_block_emit_delta` | 1 | mechanically matched (a)+(c) but its `Value` occurrences are inside the **emit closure's type**, not a decoded return; it is a block driver |
| remaining non-test `fn`s that satisfy NEITHER (a) nor (c) | 99 | 175 non-test − 76 (the union: 72 byte-taking + 35 `Value`-naming − 31 both) = 99 accessors, constructors, type-string parsers (`extract_*`, `split_type_args`, `parse_udt_type_definition`), predicates, shadow/TTL folds and clock helpers. Not individually listed; the two boundary complements above are, because those are where a misjudgement would hide. |
| `udt.rs:707 parse_counter_context` | 1 | **borderline, called out rather than dropped silently.** Takes bytes + offset, returns `(i64, usize)` and reports consumption exactly. It fails (b) — the declared type is implicit in the call site, not a parameter — and (c) — the caller builds the `Value`. It is nonetheless a structured decode with a real consumption report and is a candidate row if a future reviewer disagrees with (b)/(c). |

### One production function that is now TEST-ONLY, recorded because #3811 names it

`complex_column/cell_path_key.rs:350 parse_cell_path_key` — one of #3631's four named blob-fallback
arms — is `#[cfg(test)]`-gated (attribute at `cell_path_key.rs:349`). #3612 made
`parse_cell_path_key_reporting` (`:378`) the sole production entry and kept the simpler wrapper for
unit call sites. It is therefore **excluded from the 32-row subject set** and its properties are
those of the reporting form it delegates to.

---

## 2. Reproducible enumeration, with starting and surviving counts

All commands run from the repository root at the `HEAD` named above.

```bash
D=cqlite-core/src/storage/sstable/reader/parsing/row_decoder

# (0) file census — the whole directory INCLUDING complex_column/
find $D -name '*.rs' | wc -l                                   # 38
find $D -name '*.rs' ! -name '*_tests.rs' ! -name 'test_support.rs' | sort   # 24 subject files

# (1) every fn declaration in the subject files (the STARTING set)
grep -hcE '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?(unsafe\s+)?fn ' \
  $(find $D -name '*.rs' ! -name '*_tests.rs' ! -name 'test_support.rs') | paste -sd+ | bc   # 297

# (2) subtract fns inside #[cfg(test)] items (brace-balanced span from each
#     `#[cfg(test)]` attribute to the end of the item it annotates)
#     -> scratch script: 297 total, 122 inside cfg(test), 175 non-test

# (3) signature-shape filter, over the 175: params contain `&[u8]`
#     AND the return type names `Value`
#     -> 31 mechanical candidates
#     (the same script also PRINTS the two complements — 41 byte-taking
#      non-Value fns and 4 Value-producing non-byte fns — so nothing is
#      dropped without being looked at; both lists are dispositioned in §1)

# (4) manual disposition of the 31: -1 (parse_block_emit_delta), +2 adds
#     (create_empty_value_for_type — no bytes; parse_complex_cell_value —
#      returns ComplexCellParse{ value: Option<Value>, next_offset })
```

Grep forms used for the per-column evidence (each is quoted with its hits in §4/§5):

```bash
grep -rn 'Value::Blob\|Value::blob' $(find $D -name '*.rs' ! -name '*_tests.rs' ! -name 'test_support.rs')
grep -n 'let (val, _\|let (v, _\|, _offset)\|_) = self.parse\|_) = Self::parse' $D/*.rs $D/complex_column/*.rs
grep -n 'offset += current_offset\|current_offset + 4 >' $D/raw_type_value.rs $D/udt.rs
grep -n -A6 'parse_inline_udt_value($' $D/raw_type_value.rs $D/udt.rs   # literal depth args
```

### Counts

| stage | count |
|---|---|
| `.rs` files in `row_decoder/` (incl. `complex_column/`) | **38** |
| subject files after excluding test files | **24** |
| **STARTING**: `fn` declarations in subject files | **297** |
| minus declarations inside `#[cfg(test)]` items | −122 |
| non-test `fn` declarations | 175 |
| mechanical candidates (`&[u8]` param **and** `Value` in return) | 31 |
| − reviewed removal (`parse_block_emit_delta`) | −1 |
| + reviewed additions (`create_empty_value_for_type`, `parse_complex_cell_value`) | +2 |
| **SURVIVING subject set** | **32** |

Cross-check against the only prior partial enumeration in the tree: `cell_path_key.rs`'s module
header (lines 144–199, from #3612) enumerates **25 reachable decode paths inside
`parse_value_from_raw_bytes`** and names **8 further decoders** it delegates to
(`parse_raw_type_value`, `parse_udt_value`, `parse_nested_udt_from_registry`,
`parse_inline_udt_value`, `parse_tuple_elements_raw`, `parse_frozen_sequence_value_raw`,
`parse_frozen_map_value_raw`, `read_frozen_element`). All 8 appear below. It does **not** reach the
column tier (`cell_value*.rs`, `complex_column.rs`) or the clustering tier (`row_framing.rs`) —
the same omission #3631 round 4 hit — which is 9 further rows here.

---

## 3. Column semantics

- **reports-consumed** — `YES(exact)`: returns a count derived from what was actually read, and it
  can be SHORT of the slice. `YES(declared)`: returns a count that is **always** the declared extent
  (`blob_end`, `offset + blob_len`), so a short read is invisible — **this is the hazard, not a
  pass**. `NO`: returns a bare `Value`; no channel exists.
- **propagates-depth** — `YES`: has a `depth` param and threads it (`depth` or `depth + 1`) to every
  nested structured call. `RESET(n)`: passes a LITERAL to a callee that has one. `NONE`: has no
  depth parameter at all, so any callee's guard restarts.
- **can-blob-fallback** — can it return `Value::Blob` (or `Value::Frozen(Blob)`) when the DECLARED
  type is not `blob`/`bytes`? `YES` requires a named arm; `via <f>` means it can only do so through
  a callee.
- **bounded-caller / enforces-exhaustion** — is this function ever handed a slice whose full length
  is known to be exactly one value, and if so does it require `consumed == slice.len()`?

---

## 4. The table — 32 structured-decode entry points

| # | function | file:line | reports-consumed | propagates-depth | can-blob-fallback | bounded-caller / enforces exhaustion |
|---|---|---|---|---|---|---|
| 1 | `parse_value_from_raw_bytes` | `raw_value.rs:89` | **NO** | YES (`depth`, `+1` on frozen/list/set/map/tuple; `depth` unchanged on the two UDT delegations at `:459`,`:480`) | **YES** — `other =>` arm, `raw_value.rs:484-493` | **BOUNDED BY CONTRACT** (doc `:80-86`: "The entire `data` slice IS the value") — **NO enforcement in any arm**. Fixed-width guards are `data.len() < N` (`:161`,`:172`,`:189`,`:200`,`:212`,`:224`,`:234`,`:243`,`:255`,`:267`,`:281`); composite arms discard the offset at `:366`,`:380`,`:394`,`:458`,`:479` |
| 2 | `parse_raw_type_value` | `raw_type_value.rs:15` | **YES(exact)** — `Ok((value, offset))` `:1157`; CAN BE SHORT | YES (`depth`, `+1` at `:596`,`:586`,`:605`,`:618`,`:632`) — but **RESET(1)** into `parse_inline_udt_value` at `:794`,`:835`,`:865`,`:876`,`:1009`,`:1050`, and **NONE** into `parse_simple_udt_field_value` / `parse_nested_udt_from_registry` (no depth param) | **YES** — `:455`, `:1121`, `:1154` (unknown/unregistered type → length-prefixed Blob); nested-field Blobs at `:993`,`:1012`,`:1032`,`:1054` | not itself bounded; **it is the SHORT-OFFSET PRODUCER** for #3811 (see §5-A) |
| 3 | `parse_udt_value` | `udt.rs:432` | **YES(exact)** — `current_offset` `udt.rs:535`; SHORT on the partial-header break `:461` and on trailing bytes | **NONE** | via `parse_udt_field_value` (row 4) | not bounded itself; **all three of its callers discard the count** (`udt.rs:80`, `cell_value_complex.rs:124`, `:172`) |
| 4 | `parse_udt_field_value` | `udt.rs:539` | **NO** | **NONE** | **YES** — `_ =>` arm, `udt.rs:663-672` (covers declared `varint`, `decimal`, `duration`, `time`, `smallint`, `tinyint`, `counter`, `timeuuid`, `list`, `set`, `map`, `tuple`, `custom`) | **BOUNDED** (caller extracted exactly `field_len` bytes at `udt.rs:509`) — **no enforcement**; nested UDT discards at `udt.rs:660` |
| 5 | `create_empty_value_for_type` | `udt.rs:678` | N/A (no bytes) | N/A | **YES** — `_ => Value::blob(Vec::new())` `udt.rs:685`: an empty `int`/`boolean`/`uuid`/`timestamp` field yields `Blob([])` | N/A (slice is empty by construction) |
| 6 | `parse_simple_udt_field_value` | `udt.rs:830` | **NO** | **NONE** | **YES** — `_ =>` arm, `udt.rs:919-929` | **BOUNDED** (callers slice exactly `field_len`: `raw_type_value.rs:764`, `udt.rs:981`, `udt.rs:1140`) — **no enforcement** |
| 7 | `parse_nested_udt_from_registry` | `udt.rs:936` | **NO** (local `current_offset` dropped) | **NONE** | **YES** — unresolved `CqlType::Custom` → `udt.rs:994`; also `:1011`, `:1032`, `:1054` | **BOUNDED** (callers slice exactly `field_len`) — **no enforcement**; partial-header break at `udt.rs:947` |
| 8 | `parse_inline_udt_value` | `udt.rs:1089` | **NO** | YES internally (`depth + 1` at `:1150`,`:1161`) but **every entry is RESET(1)** by its callers | via `parse_simple_udt_field_value` (row 6) | **BOUNDED** — **no enforcement**; partial-header break at `udt.rs:1108` |
| 9 | `decode_frozen_udt_from_header_type` | `udt.rs:43` | **YES(declared)** — `offset += blob_len` `udt.rs:81` | **NONE** | via row 4 | consumes a VInt-framed blob then **discards** `parse_udt_value`'s count (`udt.rs:80`, `let (udt_value, _)`) |
| 10 | `read_frozen_element` | `frozen.rs:83` | **YES(declared)** — advances `*offset` by the element's own `[i32 BE len]` (`frozen.rs:122-124`) | YES (`depth` forwarded verbatim to row 1 at `frozen.rs:123`) | via row 1 | hands row 1 an **exactly bounded** `elem_data` (`frozen.rs:122`) and **requires nothing** |
| 11 | `parse_frozen_sequence_value` | `frozen.rs:134` | **YES(declared)** — returns `blob_end`, not `offset` (`frozen.rs:168`,`:170`) | **RESET(0)** — literal `0` at `frozen.rs:157` | via row 1 | cell-level; trailing bytes inside the declared blob are silently dropped |
| 12 | `parse_frozen_list_value` | `frozen.rs:175` | **YES(declared)** (delegates to row 11) | **RESET(0)** (row 11) | via row 1 | same as row 11 |
| 13 | `parse_frozen_set_value` | `frozen.rs:190` | **YES(declared)** (row 11) | **RESET(0)** (row 11) | via row 1 | same as row 11 |
| 14 | `parse_frozen_map_value` | `frozen.rs:205` | **YES(declared)** — returns `blob_end` `frozen.rs:243` | **RESET(0)** — literals at `frozen.rs:228`,`:232` | via row 1 | same as row 11 |
| 15 | `parse_frozen_sequence_value_raw` | `frozen.rs:251` | **YES(exact)** — returns real `offset` `frozen.rs:313`,`:315` | YES (`depth` forwarded at `frozen.rs:307`) | via row 1 | not bounded itself; **its bounded callers discard** — `raw_value.rs:366`,`:380`. Its ONE checking caller is `cell_path_key.rs:554-561` |
| 16 | `parse_frozen_list_value_raw` | `frozen.rs:320` | **YES(exact)** (row 15) | YES | via row 1 | discarded at `raw_value.rs:366`; checked at `cell_path_key.rs:554-555` |
| 17 | `parse_frozen_set_value_raw` | `frozen.rs:332` | **YES(exact)** (row 15) | YES | via row 1 | discarded at `raw_value.rs:380`; checked at `cell_path_key.rs:560-561` |
| 18 | `parse_frozen_map_value_raw` | `frozen.rs:344` | **YES(exact)** — `frozen.rs:438` | YES (`frozen.rs:396`,`:432`); real `offset` returned at `frozen.rs:438` | via row 1 | discarded at `raw_value.rs:394`; checked at `cell_path_key.rs:566-567` |
| 19 | `parse_tuple_value` | `frozen.rs:453` | **YES(declared)** — `*offset = blob_end` at `frozen.rs:503`, whose own comment says "regardless of how many elements were consumed (protects against trailing bytes …)" | **RESET(0)** — literal at `frozen.rs:500` | via row 1 | cell-level; **the reset IS the discard** |
| 20 | `parse_tuple_elements_raw` | `frozen.rs:515` | **YES(exact)** — advances `&mut offset` (`frozen.rs:577`) | YES (`depth + 1` at `frozen.rs:634`) | via row 1 | discarded by `raw_value.rs:429-437` and `frozen.rs:500-503`; checked at `cell_path_key.rs:578-586` |
| 21 | `parse_cell_path_key_reporting` | `complex_column/cell_path_key.rs:378` | **NO** (returns `Result<Value>`; consumption is checked internally, not exported) | **RESET(0)** at entry (`cell_path_key.rs:402`, literal `0`) — correct here, this IS the outermost frame | reports it via `opaque_out` rather than hiding it (`cell_path_key.rs:448-450`) but still **returns** the Blob | **BOUNDED, AND THE ONLY ROW THAT ENFORCES.** Width table applied at `:385-396` (from `cell_path_key_allowed_widths` `:740` / `cql_short_allowed_widths` `:764`) + the `consumed != data.len()` refusal at `:426-441`. **This is AC2's working model.** |
| 22 | `decode_reporting_consumption` | `complex_column/cell_path_key.rs:504` | **YES(exact) or `None`** — `Result<(Value, Option<usize>)>`; `None` = "whole slice by construction" | YES (`depth + 1` at `:547`,`:554`,`:560`,`:566`,`:584`; `depth` (NOT `+1`) into UDT at `:602`) | via row 1 | it is the MEASURER; the assert lives in row 21 |
| 23 | `parse_clustering_value` | `row_framing.rs:1341` | **YES(exact)** | N/A (arms are scalars only) | **YES — A FIFTH BLOB-FALLBACK ARM #3631 NEVER NAMED.** `row_framing.rs:1470-1499`: the `match` covers only `timestamp`, `text`/`utf8type`/`varchar`, `int`, `uuid`/`timeuuid`, `bigint`/`counter` (`:1356`,`:1378`,`:1408`,`:1428`,`:1446`); **every other declared clustering type** — `boolean`, `smallint`, `tinyint`, `float`, `double`, `date`, `time`, `decimal`, `varint`, `duration`, `inet`, `blob`, and every `ReversedType(…)` spelling except `ReversedType(TimestampType)` — falls to `Value::blob` | not bounded (reads within the row buffer) |
| 24 | `parse_clustering_prefix` | `row_framing.rs:1220` | **YES(exact)** — `row_framing.rs:1332` | N/A | via row 23; plus its own EMPTY arm `row_framing.rs:1299` (`"blob" => Value::blob(vec![])`, correct — declared blob) | not bounded |
| 25 | `parse_range_tombstone_marker_full` | `row_framing.rs:1000` | **YES(exact)** | N/A | via rows 23/24 | not bounded |
| 26 | `parse_range_tombstone_marker_with_ldt` | `row_framing.rs:1108` | **YES(exact)** | N/A | via rows 23/24 | not bounded |
| 27 | `parse_cell_value_schema_order` | `cell_value.rs:28` | **YES(exact)** — 4-tuple's `usize` | **NONE** (no depth param; the frozen-inner recursion at `cell_value_complex.rs:239-251` re-enters here, so nesting is bounded by nothing at this tier) | via rows 28/29; its own EMPTY arm `cell_value.rs:253-272` is type-correct | not bounded (row buffer) |
| 28 | `decode_complex_cell_value` | `cell_value_complex.rs:20` | **YES(mixed)** — advances `*offset`, but the two frozen-UDT arms use `off += blob_len` (`:125`,`:173`) after **discarding** `parse_udt_value`'s count (`:124`, `:172`) | **NONE** | **YES** — final `else`, `cell_value_complex.rs:335-339` (any unrecognised declared scalar type → VInt-prefixed Blob) | **COLUMN-TIER BOUNDED CALLER** at `:123`/`:171` (`udt_data` is exactly `blob_len` bytes; the counts are dropped at `:124`/`:172`) — **no enforcement**. *(Note: the non-frozen-collection arm `:262-303` returns an empty collection and advances `off` by ZERO. Unreachable in practice because `is_complex_column` routes those to `parse_complex_column`; flagged, not counted as an open instance.)* |
| 29 | `decode_scalar_cell_value` | `cell_value_scalar.rs:22` | **YES(exact)** — advances `*offset` | N/A (scalars) | `CellKind::Blob` only (`:36`) — **NO**, that is a declared blob | not bounded |
| 30 | `parse_complex_column` | `complex_column.rs:171` | **YES(exact)** (delegates to row 31) | **NONE** — passes literal `0` for `row_timestamp`, has no depth at all | via rows 1/21 | not bounded |
| 31 | `parse_complex_column_inner` | `complex_column.rs:219` | **YES(exact)** — `offset = cell.next_offset` at `:494`,`:570`,`:723`,`:862` | **NONE**; passes literal depth `0` to row 1 at `:628`, `:922`, `:1273` | via rows 1/21 | **THREE BOUNDED CALL SITES, ONE CHECKED**: set member from `cell.path_bytes` `:625-629` (unchecked), UDT field from `cell.value` bytes `:922` (unchecked), map key `:783-787` → row 21 (**checked**) |
| 32 | `parse_complex_cell_value` | `complex_column.rs:1064` | **YES(exact)** — `ComplexCellParse.next_offset` `:1290` | **NONE** — literal `0` at `:1273` | via row 1 | **BOUNDED** — `value_data` is exactly `value_len_usize` bytes (`:1262`) and row 1's result is taken with **no exhaustion check** (`:1272-1273`) |

**Nothing in this table is `UNCLEAR`.** Every cell was read from source at the cited line. The one
judgement call recorded as such is row 28's dead non-frozen-collection arm, which is stated as a
flag rather than scored.

---

## 5. OPEN INSTANCES — live defects under #3811's property, most severe first

Severity ordering is by *breadth of the silently-accepted corruption*, not by how recently it was
found.

### A. `parse_value_from_raw_bytes` has NO consumption channel — the root of the class
`raw_value.rs:89`. It is documented as a bounded decoder ("The entire `data` slice IS the value",
`:80-86`) and returns a bare `Result<Value>`, so **no caller can check even if it wanted to**. Its
own composite arms then throw away the counts their callees DO report:
`raw_value.rs:366`, `:380`, `:394` (`let (val, _) = …_raw(…)`), `:458-459` and `:479-480`
(`let (val, _offset) = self.parse_raw_type_value(…)`).

**Corruption a caller accepts today**, for a declared `frozen<list<int>>` element:
`[count=1][len=4][4 bytes]` and `[count=1][len=5][5 bytes]` both decode to the same
`List([Integer(x)])` — two distinct serialized values collapse to one `Value`. Cassandra refuses the
second: `ListSerializer.deserialize` throws `"Unexpected extraneous bytes after list value"`
(`cassandra-5.0.8:src/java/org/apache/cassandra/serializers/ListSerializer.java:135`; the identical
guard is `SetSerializer.java:127-128` and `MapSerializer.java:147`).

### B. #3811's NAMED INSTANCE — the marshal-form and registry-resolved UDT arms return a SHORT offset that bounded callers discard
The issue cites `raw_type_value.rs:794`. **That line has NOT drifted, but it is not the short-offset
site** — at `HEAD` it is the literal `1` depth argument to `parse_inline_udt_value` (defect E). The
short-offset behaviour the issue describes is at:

- **producer, marshal-form arm** — `normalized if Self::is_udt_type(normalized)` at
  `raw_type_value.rs:644`; short offset published by `offset += current_offset;` at
  **`raw_type_value.rs:907`**;
- **producer, registry-resolved arm** — the `_ =>` arm at `raw_type_value.rs:913`, registry branch;
  short offset published at **`raw_type_value.rs:1087`**;
- **the discarding bounded callers** — **`raw_value.rs:458-459`** (marshal) and
  **`raw_value.rs:479-480`** (registry-bare name), both `let (val, _offset) = …`.

`current_offset` goes short by two distinct routes, and each is a named corruption:

1. **partial 1–3 byte field-length prefix accepted.** The loop guard
   `if current_offset + 4 > udt_data.len()` (`raw_type_value.rs:697` marshal, `:934` registry)
   treats 1–3 leftover bytes as "trailing fields omitted", fills the rest with implicit NULL, and
   `break`s **without advancing past them**. Cassandra throws:
   `if (position + 4 > length) throw new MarshalException("Not enough bytes to read %dth component")`
   — `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java:311-312`, and
   `UserType extends TupleType` (`UserType.java:52`), so a UDT is governed by exactly this rule.
2. **trailing bytes after the last declared field silently discarded.** When every declared field is
   read and bytes remain, `current_offset < udt_data.len()` and nothing complains. Cassandra throws
   `"Expected %s values for %s column, but got more"` — `TupleType.java:329-335`.

The correct rule is a three-way one and Cassandra states it in the same method: `position == length`
before a component ⇒ short-return is **legal** (`TupleType.java:308-309`); `position < length` after
the loop ⇒ **throw**. That is why the check must be `consumed == slice.len()` at the BOUNDED CALLER
and not `all fields present` at the decoder.

### C. Three UDT decoders have no consumption channel at all, so B cannot even be checked through them
`parse_udt_field_value` (`udt.rs:539`), `parse_simple_udt_field_value` (`udt.rs:830`),
`parse_nested_udt_from_registry` (`udt.rs:936`) all return `Result<Value>`. Each is reached with an
**exactly bounded** `field_data` slice (`udt.rs:509`, `raw_type_value.rs:764`, `udt.rs:981`,
`udt.rs:1140`) and each accepts trailing bytes. `parse_nested_udt_from_registry` additionally has
route B's own partial-header break at `udt.rs:947`. `parse_udt_value` (`udt.rs:432`) *does* report,
and **all three of its callers discard it** (`udt.rs:80`, `cell_value_complex.rs:124`, `:172`).

### D. Fixed-width scalars are guarded with `<`, never `!=` — over-width input decodes from a prefix
`raw_value.rs:154`, `:166`, `:178`, `:187`, `:200`, `:213`, `:225`, `:235`, `:244`, `:256`, `:268`, `:335`.
A 5-byte declared `int` decodes to `Integer(from_be_bytes(data[0..4]))`. Cassandra:
`if (accessor.size(value) != 4 && !accessor.isEmpty(value)) throw` —
`cassandra-5.0.8:src/java/org/apache/cassandra/serializers/Int32Serializer.java:42-43`. Note the
`|| isEmpty` half: the legal widths are `{4, 0}`, **not** `{4}`, so a naive `!= 4` would be a false
refusal. `cell_path_key.rs:740-787` already encodes the per-type table correctly and is the
reference. (The `udt.rs` field decoders **do** use `!= N` — `udt.rs:549`, `:559`, `:570`, `:580`,
`:591`, `:601`, `:612`, `:625` — so the two families disagree with each other about the same type.)

### E. Nesting depth is RESET or ABSENT at eight boundaries, so #3631's guard does not compose
- **RESET to a literal `1`** into `parse_inline_udt_value`: `raw_type_value.rs:794`, `:835`, `:865`,
  `:876`, `:1009`, `:1050`; `udt.rs:1009`, `udt.rs:1050`. The enclosing `parse_raw_type_value` may be
  at depth 19 of `MAX_TYPE_NESTING_DEPTH`; the callee restarts at 1.
- **RESET to a literal `0`**: `frozen.rs:157`, `:228`, `:232` (`read_frozen_element`),
  `frozen.rs:500` (`parse_tuple_elements_raw`), `complex_column.rs:628`, `:922`, `:1273`
  (`parse_value_from_raw_bytes`).
- **NO depth parameter at all**: `parse_udt_value`, `parse_udt_field_value`,
  `parse_simple_udt_field_value`, `parse_nested_udt_from_registry`,
  `decode_frozen_udt_from_header_type`, `parse_cell_value_schema_order`,
  `decode_complex_cell_value`, `parse_complex_column{,_inner}`, `parse_complex_cell_value`.

A chain that alternates `parse_raw_type_value → parse_inline_udt_value → (nested UDT field) →
parse_raw_type_value` therefore has **no global bound**. This is a resource/termination defect as
well as a correctness one, and it is on the same edges as A–C, which is why fixing consumption
without fixing depth would leave half the boundary.

### F. Frozen collection and tuple decoders report the DECLARED extent, not what they read
`parse_frozen_sequence_value` returns `blob_end` (`frozen.rs:168`, `:170`), `parse_frozen_map_value`
likewise (`frozen.rs:243`), and `parse_tuple_value` writes `*offset = blob_end` at `frozen.rs:503`
under a comment claiming it "protects against trailing bytes". It does the opposite: it makes them
unobservable. A declared `frozen<list<int>>` cell whose blob is 4 bytes longer than its elements
require decodes clean and the row stays byte-aligned, so nothing downstream ever notices. Cassandra's
`ListSerializer.deserialize` throws here (`ListSerializer.java:135`).

### G. Four blob-fallback arms — one of them never named by #3631
#3631 names four (`parse_cell_path_key`, `parse_simple_udt_field_value`, `parse_udt_field_value`,
`create_empty_value_for_type`'s zero-length path). **At `HEAD` none is fixed** (§0), and the census
finds these live arms:

| arm | file:line | in #3631's list? |
|---|---|---|
| `parse_value_from_raw_bytes` unknown-type default | `raw_value.rs:484-493` | no (it is the arm the others delegate to) |
| `parse_udt_field_value` `_ =>` | `udt.rs:663-672` | yes |
| `parse_simple_udt_field_value` `_ =>` | `udt.rs:919-929` | yes |
| `create_empty_value_for_type` `_ =>` | `udt.rs:685` | yes |
| `parse_raw_type_value` unknown/unregistered | `raw_type_value.rs:455`, `:1121`, `:1154` | no |
| `parse_nested_udt_from_registry` unresolved `Custom` | `udt.rs:994`, `:1011`, `:1032`, `:1054` | no |
| `parse_raw_type_value` nested-field unresolved | `:993`, `:1012`, `:1032`, `:1054` | no |
| `decode_complex_cell_value` final `else` | `cell_value_complex.rs:335-339` | no |
| **`parse_clustering_value` `_ =>`** | **`row_framing.rs:1470-1499`** | **no — and it is a whole TIER (clustering keys) that neither #3631 nor #3612 touched.** A `decimal`, `varint`, `duration`, `inet`, `boolean`, `date`, `time`, `float`, `double`, `smallint` or `tinyint` clustering column silently reads as `Value::blob` today |
| `parse_cell_path_key_reporting` opaque default | `cell_path_key.rs:448-456` | yes (as `parse_cell_path_key`) — **this one is DISCLOSED, not silent**: `opaque_out` is set and the caller warns once per column per row |

### H. Three bounded call sites in `complex_column.rs`, one checked
- `complex_column.rs:625-629` — set member decoded from `cell.path_bytes`, which **IS** the member
  in full. Unchecked.
- `complex_column.rs:922` — non-frozen UDT field decoded from the cell's whole value bytes.
  Unchecked.
- `complex_column.rs:1272-1273` — complex cell value decoded from `value_data`, sliced to exactly
  `value_len_usize` at `:1262`. Unchecked.
- `complex_column.rs:783-787` — map cell-path key. **CHECKED**, via row 21.

The asymmetry is deliberate and documented (`cell_path_key.rs:325-335`: "widening it to the
frozen/set routes is out of #3612's scope"). #3811 is that widening.

---

## 6. WHERE THE PROPERTY COULD BE MADE INHERITABLE

AC2 requires the check to live "somewhere a new call site **inherits** rather than somewhere it must
remember to ask". Three candidates, in increasing order of blast radius.

### Candidate 1 — give `parse_value_from_raw_bytes` a reporting twin and make the non-reporting name the ASSERTING one
**Site**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/raw_value.rs:89`.

Split into `parse_value_from_raw_bytes_reporting(…) -> Result<(Value, usize)>` (all arms thread a
real count; the "whole slice by construction" arms return `data.len()`) and keep
`parse_value_from_raw_bytes` as the thin wrapper that calls it and asserts
`consumed == data.len()`. **Inheritance mechanism**: the existing name keeps its existing ~45 call
sites and every one of them silently GAINS the check; a new caller that genuinely needs a short read
has to reach for the longer `_reporting` name, which is a visible, reviewable act.

This is exactly the shape PR #3820 already built one module over —
`typed_value.rs:372 parse_typed_value` wraps `typed_value.rs:394 parse_typed_value_reporting` with
`typed_value.rs:310 require_fully_consumed` — but over `&CqlType`, not over the type-STRING
dispatch, so it does not reach rows 1, 2, 10–20 or 23.

**Cost / blast radius**: the largest single change here, and the one #3612 measured and declined:
`cell_path_key.rs:190-198` puts it at "~45 call sites plus 8 further decoders in the same path,
~100 sites in total". Every arm of rows 15–20 already has the count in hand, so most of that is
mechanical; the genuinely new work is the ~24 scalar arms of row 1 and the two UDT delegations.
Rows 3–8 (the UDT family) need real new plumbing because they have no channel at all.
**Risk**: turning a previously-silent prefix decode into an `Err` interacts with the row-assembly
swallow documented at `cell_path_key.rs:83-99` — an `Err` from a column decode makes `row_data.rs`
`break` the column loop, so the failing column **and every later on-disk column** vanish from the
row. That is a behaviour change on real data and must be measured against the corpus, not assumed.

### Candidate 2 — a `BoundedSlice` newtype that cannot be dropped without being consumed
**Site**: new type beside `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/mod.rs:1`
(the module root that all these files `use super::*` from).

Wrap the "this slice is exactly one value" contract in a type whose only exit is a method that
compares the reported count against its own length. Every current bounded call site
(`udt.rs:509`, `raw_type_value.rs:764`, `complex_column.rs:625`, `:922`, `:1262`,
`cell_value_complex.rs:123`, `:171`, `frozen.rs:122`, `raw_value.rs` arms) would construct one
instead of a `&[u8]`.

**Inheritance mechanism**: the strongest of the three — a new bounded call site cannot *forget*,
because forgetting does not compile. **Cost / blast radius**: touches every signature in rows 1–20
and the three column-tier rows, i.e. strictly a superset of candidate 1, and it also has to survive
the zero-copy `value_borrow::borrow_active` path (`raw_value.rs:148`, `:151` and ~10 more) whose
lifetime story is already load-bearing for #1644. **Honest assessment**: correct, and probably too
large for one PR; it is what candidate 1 evolves into once the counts exist.

### Candidate 3 — generalise the ONE working enforcer, `parse_cell_path_key_reporting`
**Site**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs:378`,
with the measurement in `decode_reporting_consumption` at `:504` and the assert at `:433-441`.

This pair is already the complete mechanism: a dispatcher that returns
`(Value, Option<usize>)` where `None` means "whole slice by construction", plus a caller that
refuses on `Some(n) != data.len()`, plus a per-type allowed-width table (`:740-787`) for the
fixed-width arms where a consumption compare is not expressive enough. It is currently reachable
from **one** call site (`complex_column.rs:783`).

**Inheritance mechanism**: move the pair up to `raw_value.rs` (or the new `typed_value.rs` if #3820
lands first) as *the* bounded entry point and re-point the four unchecked bounded sites at it
(`complex_column.rs:628`, `:922`, `:1273`; `frozen.rs:123`). **Cost / blast radius**: much smaller
than 1 or 2 — no signature change to rows 15–20, whose counts it already consumes. **The cost is a
DEFECT the file already declares against itself**: `cell_path_key.rs:485-500` records that its
dispatcher must MIRROR `parse_value_from_raw_bytes`'s arms in the same order, that this is "a MANUAL
OBLIGATION … no test enforces it", and that an arm added to `parse_value_from_raw_bytes` and not here
falls through to the `None` default and **prefix-decodes silently**. Promoting a second dispatcher to
be the enforcer institutionalises that drift. If this candidate is chosen, the two dispatchers must be
MERGED, not paired — which turns it back into candidate 1.

**Recommendation for planning (not a decision):** candidate 1, scoped to land *after* PR #3820 so
that `typed_value.rs`'s `require_fully_consumed` is the single assert and the type-string path is
made to route through it rather than growing a second one.

---

## 7. What changes when PR #3820 (#3631) merges

Measured with `git diff origin/main...origin/issue-3631-structured-values-blob-degrade`:

- **`create_empty_value_for_type` is DELETED** (no `fn create_empty_value_for_type` anywhere on the
  branch). Row 5 disappears.
- **`parse_simple_udt_field_value` is replaced** by `parse_simple_udt_field_value_at`
  (`typed_value.rs:97`), a reporting form. Row 6's `NO` becomes a count.
- **`parse_udt_field_value` SURVIVES** (branch `udt.rs:607`). Row 4's open instances are **not**
  closed by #3820.
- **`parse_cell_path_key` survives ungated on that branch** — it was `#[cfg(test)]`-gated by #3612,
  which the branch predates; expect a merge interaction there.
- **A new asserting chokepoint appears**: `typed_value.rs` with `require_fully_consumed` (:310),
  `parse_typed_value` (:372), `parse_typed_value_reporting` (:394). It dispatches on `&CqlType`.
- **Untouched by #3820**, and therefore still #3811's whole subject: `raw_value.rs`, `frozen.rs`,
  `row_framing.rs`, `complex_column.rs`, `cell_value*.rs` — i.e. rows 1, 10–20, 23–32.

**Planning consequence**: #3811 must be based on #3820, or it will build a second enforcer.

---

## 8. DECLARED GAPS — what this census cannot see

Stated because a census that declares no gaps claims a completeness it does not have.

1. **A WHOLE SECOND DECODER FAMILY, one directory up, is out of AC1's stated scope and is the most
   likely place this issue's family reappears.**
   `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` has its own
   `parse_udt_value` (`:647`), `parse_tuple_value` (`:633`), `parse_list_value` (`:593`),
   `parse_set_value` (`:605`), `parse_map_value` (`:617`), `parse_udt_value_with` (`:301`),
   `parse_tuple_value_with` (`:235`) and `parse_value_with_comparator_at_depth` (`:481`);
   `comparator_value_parsing.rs` and `custom_scalar.rs` add more
   (`custom_scalar.rs:24 decode_custom_scalar` has an explicit
   `unknown_custom_falls_back_to_blob` test at `:134`); and `cqlite-core/src/parser/types/udt.rs:410
   parse_frozen_udt` is a third. **None is in the subject directory and none was audited.** #3631's
   own five-round history is the family moving one module outward each round; `parsing/` is the next
   module outward.

2. **No dependency closure.** The table records what each function does at its own site and at its
   *direct* call sites. It does not compute a transitive closure, so "via row 1" cells inherit row
   1's disposition without re-deriving it per path.

3. **`#[cfg(test)]` span detection is textual.** The 122-function exclusion is computed by
   brace-balancing from each `#[cfg(test)]` attribute to the end of the item it annotates. A
   `#[cfg(test)]` behind a `cfg_attr`, inside a macro expansion, or gated on a different feature is
   not recognised. Two *item-level* (non-`mod`) `#[cfg(test)]` uses were found and handled
   (`cell_path_key.rs:349`, `row_data.rs:858`); a third form would be missed.

4. **No macro-generated arms and no dynamic dispatch were analysed.** The `SlidingPartitionPolicy`
   trait (`partition_driver.rs:77-131`) dispatches `on_data_row`/`on_range_marker` to three impls
   (`compaction.rs:640`, `timestamp_policy.rs:143`, and the block-emit policies); those are drivers
   rather than value decoders, but a future impl could decode a value and this census would not see
   it. No `macro_rules!` in the subject files generates a decode arm today (`grep -c 'macro_rules!'`
   over the 24 subject files = 0), which is a fact about `HEAD`, not a guarantee.

5. **Callers OUTSIDE `row_decoder/` were only spot-checked.** `grep` found no external caller of the
   row_decoder value entries (they are `pub(super)`/`pub(crate)`), but `parse_complex_column_inner`
   is `pub(crate)` and reachable from anywhere in the crate.

6. **This is a source census, not an execution census.** No test was run and no fixture was decoded.
   Every "corruption a caller accepts today" in §5 is derived from reading the arm and its guard,
   plus the pinned Cassandra rule it violates — **not** from having fed the bytes through the
   reader. AC3/AC4's tests are what convert these into demonstrated facts, and the #3042 rule
   applies: the oracle must be Cassandra-written bytes, never a CQLite round-trip.

7. **Cassandra citations are from the pinned tag over the network, not a local clone.**
   `$CQLITE_CASSANDRA_REPO` is unset and no clone exists on this host; the files were read from
   `https://raw.githubusercontent.com/apache/cassandra/cassandra-5.0.8/…`, which is the same tagged
   content `git show cassandra-5.0.8:<path>` would print. Every Java `file:line` in §5 is from that
   read. No Cassandra claim in this document is second-hand from a CQLite comment.

### Cassandra sources consulted (all at tag `cassandra-5.0.8`)

| path | what it establishes |
|---|---|
| `src/java/org/apache/cassandra/db/marshal/TupleType.java:301-338` | the three-way rule: `position == length` ⇒ legal short return; `position + 4 > length` ⇒ throw; `position < length` after the loop ⇒ throw |
| `src/java/org/apache/cassandra/db/marshal/UserType.java:52` | `UserType extends TupleType` — a UDT is governed by `split`'s rules |
| `src/java/org/apache/cassandra/serializers/SetSerializer.java:94-95, 127-128` | `"Unexpected extraneous bytes after set value"` in **both** `validate` and `deserialize` |
| `src/java/org/apache/cassandra/serializers/ListSerializer.java:89, 135` | same, for lists |
| `src/java/org/apache/cassandra/serializers/MapSerializer.java:110, 147` | same, for maps |
| `src/java/org/apache/cassandra/serializers/Int32Serializer.java:40-44` | fixed-width legality is `{N, 0}`, not `{N}` — a naive `!= N` is a false refusal |
