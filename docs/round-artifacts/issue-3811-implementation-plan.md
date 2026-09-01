# Issue #3811 — implementation plan (design of record)

Derived from `issue-3811-decode-entry-point-census.md` (AC1) and
`issue-3811-cassandra-oracle.md` (AC5). Written BEFORE implementation so the
rebase onto #3820 does not lose it.

## Base

Rebase onto `origin/main` **after PR #3820 (#3631) merges**. Not stacked on its
branch (1:1:1:1 says branch from `origin/main`), and not started on today's
`main`, because #3820 rewrites `raw_type_value.rs` −227/+45 — deleting the very
arms AC3 names — and ships `typed_value.rs::require_fully_consumed`, which must
be the ONE assert rather than the second one this issue exists to prevent.

## Approach — census candidate 1, reusing #3820's assert

`parse_value_from_raw_bytes` (`raw_value.rs:89`) becomes a thin ASSERTING wrapper
over a new `parse_value_from_raw_bytes_reporting -> Result<(Value, usize)>`.

- The assert is **#3820's `require_fully_consumed`**, called — not reimplemented.
  One rule, one implementation.
- Inheritance mechanism: the ~45 existing call sites keep the existing NAME and
  silently GAIN the check. A caller that genuinely needs a short read must reach
  for the longer `_reporting` name, which is a visible, reviewable act.
- The two UDT delegations (`raw_value.rs:458-459`, `:479-480`) stop discarding
  `_offset` and thread it out. That alone closes AC3's named instance, both arms.

**Do NOT copy `cell_path_key.rs`'s `Ok((v, None))` "whole slice by construction"
escape.** That `None` is an opt-out a new arm inherits by accident, and the file
already declares the resulting drift against itself (`:485-500`: a mirror
obligation that "no test enforces"). Arms that consume the whole slice return
`data.len()` explicitly. If candidate 3's dispatcher is touched at all, the two
dispatchers must be MERGED, not paired.

## The risk that must be MEASURED, not assumed

Turning a previously-silent prefix decode into an `Err` is a behaviour change on
real data. An `Err` from a column decode makes `row_data.rs` `break` the column
loop, so the failing column **and every later on-disk column** vanish from the
row (`cell_path_key.rs:83-99`). A strict check could therefore TRUNCATE ROWS on
corpus data that decodes fine today.

**Required before the PR is proposed as correct:**
1. Run the full 155-table corpus (`CQLITE_DATASETS_ROOT=/data/datasets`) and the
   sstabledump goldens BEFORE the change; record row/cell counts.
2. Re-run AFTER; diff. Any table that loses rows or columns is a finding, not a
   footnote — it means real Cassandra-written data is hitting the new refusal,
   and that has to be explained against `TupleType.split` before proceeding.
3. If a real regression appears, the fix is NOT to weaken the check — it is to
   find which arm mis-measures its own consumption.

## Test vectors — all derived from `TupleType.split`, none from CQLite output

Per AC6 each case is labeled **discriminating** (fails if the defect is
reintroduced) or **not**, and the label is proved by reintroducing the defect.

| # | case | bytes | expected | why |
|---|---|---|---|---|
| 1 | UDT, all fields present, exact | `[len][f1][len][f2]` | OK | control — must NOT become an error (guards against over-strictness) |
| 2 | UDT, trailing garbage | case 1 `|| 0xAA` | **corruption** | `TupleType.split` `position < length` ⇒ "but got more" |
| 3 | UDT, partial 1-byte prefix | **case 5** `|| 0x00` | **corruption** | `position + 4 > length` ⇒ "Not enough bytes" — NOT an omitted field |
| 4 | UDT, partial 3-byte prefix | **case 5** `|| 0x00 00 00` | **corruption** | same rule, the boundary case |
| 5 | UDT, legally short (trailing field omitted) | `[len][f1]` only | **OK, f2 = null** | `position == length` ⇒ legal short return. This is the case a naive "all fields present" check would BREAK |
| 3b | UDT, exact `|| 0x00` (supplementary) | case 1 `|| 0x00` | **corruption** | rule 3, NOT rule 2 — see the correction below |
| 6 | UDT, declared field length overruns | `[len=99][2 bytes]` | **corruption** | `position + size > length` |
| 7 | AC4 collapse: cases 1 vs 2 | — | **distinct outcomes** | two distinct serialized inputs must not yield one `Value` |
| 8 | AC4 collapse: cases 1 vs 3 | — | **distinct outcomes** | the partial-prefix half of the same property |

**CORRECTION (this draft was WRONG, fixed after the demonstration run).** Rows 3
and 4 originally hung the stray bytes off case 1. Under `TupleType.split` that does
not reach the partial-prefix rule at all: with every declared field present the
component loop is already exhausted when the stray byte is reached, so rule 2 is
never evaluated and the verdict is rule 3 — the SAME rule as case 2. Two tests of
one rule, labelled as two rules, is precisely the "test whose claim exceeds what it
exercises" AC6 forbids. Rule 2 is reachable ONLY when a declared field is still to be
read, i.e. from the LEGALLY SHORT encoding plus 1-3 stray bytes. Hence rows 3/4 now
hang off **case 5**, which is also the only reading under which "cases 3 and 4 are one
byte apart" is true (11 B vs 12 B; case 1 || 0x00 is 19 B, eight bytes away). The
original spelling is retained as supplementary row 3b so its collapse is on record.
Demonstrated, not argued: `issue-3811-defect-demonstration.md`.

Cases 3, 4 and 5 are one byte apart and are the whole point: 5 must stay legal
while 3 and 4 become errors. A test suite containing 2 but not 3/4/5 would pass
over a fix that got the boundary wrong.

Both AC3 arms (marshal-form AND registry-resolved) need the full set — they are
separate code paths reaching the same rule, and #3631's history is precisely a
fix landing on one arm and not its sibling.

**Write the cases at the BEHAVIOURAL level** (bytes in via the bounded entry
point, error/Value out), not against the internal arm structure — that way they
survive #3820's consolidation instead of testing deleted code.

## Scope (pending lead confirmation on DRIVE-3811-R1, default stated there)

In: census findings **A, B, C, D, F, H**. Out: **E** (depth resets — recorded by
AC1, fixed elsewhere), **G** (#3631's blob-fallback class).
Follow-up to file at merge: the second decoder family in `parsing/`
(`value_parsing.rs`, `comparator_value_parsing.rs`, `custom_scalar.rs`,
`parser/types/udt.rs`) — declared gap 1, and the next module outward.

## File-size constraint (gate ratchet: 800 src / 1500 test)

`raw_value.rs` = 975 and `raw_type_value.rs` = 1224 are ALREADY over 800, so ANY
growth of them FAILs the gate. The reporting twin and all new tests go in NEW
files. This is a constraint on the design, not a formatting detail.
