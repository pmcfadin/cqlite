# Design: typed inner-UDT decode via header-marshal threading

All anchors at `main` commit `e2694ab5`. Written to be implementable by a later team without
re-deriving the investigation — read the fact base in `proposal.md` first.

## Context (the mechanism, precisely)

1. A `frozen<...>` collection is a **single-cell simple column**. Both the query path and the
   compaction read path decode it through the same chain:
   `cell_value.rs:846-872` (dispatch; extracts element type as the **schema short form**
   `"frozen<person>"`) → `frozen.rs:134-172` (`parse_frozen_sequence_value` element loop) →
   `frozen.rs:83-126` (`read_frozen_element`) → `raw_value.rs:89-467`
   (`parse_value_from_raw_bytes(elem_data, "frozen<person>", ..)`).
2. `raw_value.rs:418-426` strips `frozen<...>` → recurses with bare `"person"` → the `other`
   arm (`raw_value.rs:435-465`): `is_udt_type("person")` is false (matches only the marshal
   substring, `udt.rs:130-141`); registry gate at `raw_value.rs:445-456` (typed iff a wired
   `UdtRegistry` resolves it); else **`Value::Blob` at `raw_value.rs:457-464`**.
3. Meanwhile the authoritative on-disk header marshal type for the whole column —
   `FrozenType(ListType(FrozenType(UserType(ks, person, field-types...))))` — sits unused in
   `RowColumnResolution.header_type` (`parsing/mod.rs:211`). Top-level frozen UDTs already
   decode from it, registry-free (`marshal_is_top_level_frozen_udt` gate +
   `decode_frozen_udt_from_header_type`, `cell_value.rs:965-976`, `:1108-1116`; `udt.rs:20-…`,
   `:24-…`).

## Options considered

### Option A — thread the header marshal element type down (CHOSEN)

Pass the marshal type (when available) alongside/instead of the schema short form through the
frozen-collection element decode chain, and decode inner `UserType(...)` elements exactly the
way top-level frozen UDTs are decoded today.

- Sketch: at `cell_value.rs:846-872`, when `header_type` is present for the column, extract the
  **marshal** element type(s) (e.g. `ListType(X)` → `X`; `MapType(K, V)` → `K`, `V`) with a
  marshal-aware sibling of `extract_collection_element_type`, and hand `parse_frozen_*_value`
  the marshal element type. In `raw_value.rs`, a marshal `FrozenType(UserType(...))` /
  `UserType(...)` element routes to the existing marshal-driven UDT decode
  (`decode_frozen_udt_from_header_type` internals / `parse_udt_type_definition`, `udt.rs:157`)
  instead of falling to the bare-short-name arm. Recursion depth guard: reuse the existing
  `depth` parameter of `parse_value_from_raw_bytes`.
- Pros: registry-free (works on default `open()`); uses the **most authoritative** metadata
  (the file's own SerializationHeader — exactly what no-heuristics #28 asks for); symmetric
  with the existing top-level mechanism; one fix covers query + compaction paths; no API change.
- Cons: marshal-string parsing for nested generic types must be careful (angle-bracket/paren
  nesting); touches the hot element loop (keep allocations out of the per-element path — parse
  the marshal element type ONCE per column, not per element).

### Option B — registry-only (status quo, wire more registries)

Keep the decoder as-is; require every surface wanting typed inner UDTs to wire a `UdtRegistry`
(as `merge/mod.rs:499-501` and the CLI already do).

- Rejected as the primary mechanism: leaves default `open()` readers permanently Blob; pushes a
  per-surface wiring obligation onto every embedder (the exact "built-but-unwired" failure mode
  this project keeps re-finding); the DDL-derived registry is *less* authoritative than the
  file's own header for the file at hand. Kept as **fallback** (precedence 2) — it already
  works and covers files whose header lacks usable marshal info.

### Option C — post-hoc conversion layer at export/query surfaces

Decode Blob as today; add a converter that re-decodes opaque frozen bytes into typed UDTs at
the surfaces that want them.

- Rejected: double-decode cost; the contract stays inconsistent across surfaces; a second
  decode implementation to keep in parity with the first; still needs the same type metadata
  threading to know *what* to decode — so it inherits Option A's work plus duplication.

## Decision

**Option A**, with resolution precedence per element type:
1. **Header marshal type** (authoritative, from `RowColumnResolution.header_type`) — typed decode.
2. **`UdtRegistry`** (existing `raw_value.rs:445-456` path) — typed decode, unchanged.
3. **`Value::Blob`** — honest opaque fallback. Never a byte-pattern inference.

Wrapping stays symmetric with top-level: inner element yields `Value::Frozen(Value::Udt(..))`
where top-level yields the same shape today (`raw_value.rs:418-426` wrap).

## Risks / notes for the implementing team

- **Tripwire will fire by design**: `issue_1240_nested_frozen_collection_udt_parity.rs:718-732`
  (`element_bytes`) panics on `Value::Udt`. Update it per its embedded guidance (lines 723-728):
  compare typed UDTs field-by-field against the sstabledump JSONL golden (tier 1b), keep the
  byte-parity tier (tier 2) exactly as-is. Do this in the SAME commit as the decode change or
  the gate goes red between commits.
- **Do not regress the merge path**: the k-way merge producer already decodes typed via the
  registry (`merge/mod.rs:494-522`) and its byte-parity goldens pass — header-marshal decode
  must produce the identical `Value::Udt` structure the registry path produces (add an
  equivalence test: same fixture decoded via marshal-only reader vs registry-wired reader →
  identical `Value`s).
- **CLI/binding parity goldens must stay byte-identical**: with a schema-supplied registry the
  query surface is already typed, so JSON/JSONL output should not move. Run the full parity
  suite (33 tables + Python) and diff outputs.
- **Marshal parsing hygiene**: `header_type` strings for collections nest parens
  (`MapType(UTF8Type,FrozenType(UserType(...)))`); write a small paren-aware splitter (or reuse
  an existing marshal helper if one exists in `udt.rs` — check `extract_frozen_inner_type`'s
  marshal handling, tested at `frozen.rs:661-687/750-783`) with unit tests for list/set/map,
  UDT-in-UDT, and tuple-in-collection shapes.
- **Absent/odd headers**: `header_type` is `Option`; older or synthetic fixtures may omit it —
  precedence 2/3 covers that, and a unit test must pin the Blob fallback (no panic, no guess).
- **Memory**: parse the marshal element type once per column (per `RowColumnResolution`), not
  per element; the <128MB budget and the per-element hot loop must not gain allocations.
