# Tasks: typed-inner-udt-frozen-collections (#1340)

Anchors at `main` commit `e2694ab5`. Rebase note for the later team: this spec was authored
2026-07-04 and parked; verify the cited line anchors still hold after rebasing onto current
`origin/main` before starting (the mechanism is described in `design.md` §Context if lines moved).

## 1. Marshal element-type extraction (unit-first)

- [x] 1.1 Write failing unit tests for a paren-aware marshal collection-element extractor:
      `ListType(X)`/`SetType(X)` → `X`; `MapType(K,V)` → `(K,V)`; nested
      `FrozenType(ListType(FrozenType(UserType(...))))`; tuple-in-collection; malformed input →
      `None` (never panic). Surface exercised: the new extractor fn (internal), tests colocated
      per campsite rule.
- [x] 1.2 Implement the extractor in `.../v5_compressed_legacy/udt.rs` (or a small sibling
      module if udt.rs is over the file-size ratchet), reusing existing marshal helpers
      (`extract_frozen_inner_type` marshal handling; `parse_udt_type_definition`, udt.rs:157).

## 2. Thread header marshal type into frozen-collection element decode (TDD)

- [x] 2.1 Write the failing integration test FIRST: registry-less `SSTableReader::open` +
      `iterate_all_partitions_for_compaction` over the #1240 fixture asserts typed
      `Value::Frozen(Value::Udt(..))` inner elements with field values matching the JSONL
      golden (spec Req 1, scenarios 1-3). Dataset-guarded: fixture-present-but-zero-rows FAILS.
- [x] 2.2 At `cell_value.rs:846-872`, when the column's `header_type`
      (`RowColumnResolution.header_type`, parsing/mod.rs:211) is present, extract the marshal
      element type(s) ONCE per column and pass them into `parse_frozen_list_value` /
      `parse_frozen_set_value` / `parse_frozen_map_value` (frozen.rs:134-172) alongside the
      schema short form.
- [x] 2.3 In the element path (`read_frozen_element`, frozen.rs:83-126 →
      `parse_value_from_raw_bytes`, raw_value.rs:89-467): a marshal
      `FrozenType(UserType(...))`/`UserType(...)` element type routes to the existing
      marshal-driven UDT decode (same mechanism as `decode_frozen_udt_from_header_type`,
      udt.rs:24-…), producing `Value::Frozen(Value::Udt(..))`. Registry path
      (raw_value.rs:445-456) stays as fallback; Blob fallback (raw_value.rs:457-464) stays last.
- [x] 2.4 Recursion: inner-UDT fields that are themselves UDTs/collections decode via the
      existing recursion (`parse_nested_udt_from_registry` udt.rs:930-… / marshal recursion),
      honoring the existing `depth` guard.
- [x] 2.5 Unit test the Blob fallback: absent `header_type` + no registry → `Value::Blob`,
      no panic (spec Req 1 scenario 4).

## 3. Equivalence + tripwire update (same commit as 2.x lands)

- [x] 3.1 Equivalence test: #1240 fixture decoded registry-less vs registry-wired → equal
      `Value`s (spec Req 2 scenario 1).
- [x] 3.2 Update `cqlite-core/tests/issue_1240_nested_frozen_collection_udt_parity.rs` per its
      embedded guidance (lines 718-732): tier-1b compares typed UDT fields against the JSONL
      golden from CQLite's OWN decode; tier-2 byte-parity assertions UNCHANGED. Both tests
      (#1240 + #1289 null-inner) green.

## 4. Public-surface wiring evidence (e2e)

- [x] 4.1 E2E test through the query surface: `SELECT lp, ma FROM
      test_compactionparityudt.udt_collections` (query engine / cli-helpers path) returns
      structured inner-UDT field values matching the JSONL golden (spec Req 4). Name the
      surface + call chain in the test header comment.
- [x] 4.2 Confirm no golden churn: full 33-table parity + Python parity suite pass with
      unchanged values (spec Req 3 scenario 2).

## 5. Quality stages (definition of done)

- [x] 5.1 Iterate with `scripts/agent-gate.sh --lite` each fix round; internal `rust-reviewer`
      pass before the first full gate (this diff touches a shared decoder → review-first applies).
- [ ] 5.2 Run the FULL `scripts/agent-gate.sh` ONCE (with `CQLITE_DATASETS_ROOT` pointed at the
      main repo's `test-data/datasets` from the worktree) — paste the AGENT-GATE SUMMARY block.
- [ ] 5.3 Spec-auditor (C) anchored to `openspec/changes/typed-inner-udt-frozen-collections/specs/**`
      — every requirement `satisfied` with a public-surface test as evidence.
- [ ] 5.4 roborev clean; PR; merge per autonomy model; `flow-finalize` (archive change, close #1340).
