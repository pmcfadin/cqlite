//! Decoding a MULTICELL collection cell's CELL PATH as a map KEY (issue #3612).
//!
//! A non-frozen `map<K, V>` is multicell: each entry is its own cell and the KEY
//! is carried in that cell's CellPath. Cassandra frames a CellPath as
//! `[VInt length][bare serialized key]`
//! (`CollectionType.CollectionPathSerializer`); the caller in
//! [`super::complex_column`] has already stripped the VInt, so the slice reaching
//! this module IS the serialized key and carries NO length prefix of its own.
//!
//! Historically this site carried its OWN type ladder — an allowlist of six
//! scalar families with a `Value::Blob` default — so a COMPOSITE key
//! (`frozen<udt>`, `tuple<…>`, a frozen collection) and roughly ten further
//! scalar families surfaced as raw bytes, while the FROZEN spelling of the very
//! same map decoded structurally. The fix is to delegate to
//! [`super::V5CompressedLegacyParser::parse_value_from_raw_bytes`] — the
//! structural decoder the SET branch already used for a set member's cell path,
//! whose convention ("the entire slice IS the value") is exactly this one, and
//! whose framing for a tuple/UDT (`[i32 BE len][bytes]` per component, `-1` =
//! null, per Cassandra's `TupleType.buildValue`) is byte-identical to a
//! composite CellPath's.
//!
//! Two properties this module owns, because delegation alone would lose them:
//!
//! 1. **The ORIGINAL-CASE type string is forwarded.**
//!    `primitive_marshal_to_cql_short` matches marshal suffixes CASE-SENSITIVELY
//!    (`s.ends_with("Int32Type")`), so lowercasing before delegation would fail
//!    every marshal-form normalization and land straight back in an opaque blob —
//!    which is precisely the no-schema `Statistics.db` path, where the key type
//!    arrives in marshal form.
//!
//! 2. **Fixed-width keys are validated against the widths CASSANDRA accepts.**
//!    `parse_value_from_raw_bytes` rejects only UNDER-width input (`< N`) because
//!    its other callers hand it element bytes already bounded by an outer length
//!    prefix, so trailing bytes cannot occur. A CellPath has no such outer bound
//!    here: the whole slice is the key, so an over-long slice is corruption and
//!    must not be decoded from its prefix.
//!
//!    The authority is `org.apache.cassandra.serializers.*.validate`, read at the
//!    pinned `cassandra-5.0.8` tag (there is no clone on the build hosts; read via
//!    `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/<X>.java`
//!    or the raw tag URL). **It is NOT a uniform `!= N`** — an earlier revision of
//!    this header claimed that and was WRONG, which is why the rule is written out
//!    per type here and mirrored literally by `cql_short_allowed_widths`:
//!
//!    * **`N` or `0`** (`size != N && !isEmpty` → an EMPTY buffer is LEGAL):
//!      `Int32Serializer` 4, `LongSerializer` 8, `FloatSerializer` 4,
//!      `DoubleSerializer` 8, `UUIDSerializer` 16, `TimestampSerializer` 8, and
//!      `CounterSerializer` (which `extends LongSerializer`) 8.
//!    * **strict `!= N`** (no empty buffer): `ShortSerializer` 2,
//!      `ByteSerializer` 1, `SimpleDateSerializer` 4, `TimeSerializer` 8.
//!    * **`size > 1`**, i.e. 0 or 1: `BooleanSerializer`.
//!    * **`InetAddressSerializer`** RETURNS EARLY on empty
//!      (`if (accessor.isEmpty(value)) return;`) and otherwise delegates to
//!      `InetAddress.getByAddress`, so 0, 4 or 16. It is an `N`-or-`0` type, NOT
//!      a strict one — an earlier revision of this header called it "the fifth
//!      strict case" on the strength of a `grep` whose output line had run the
//!      `isEmpty` test together with the `throw` from the
//!      `catch (UnknownHostException)` block below it. Read the whole method, not
//!      a grep of its `if`s.
//!
//!    #3612 encoded the `0` allowances as a FIDELITY fix with no behaviour change,
//!    because the caller then decoded only a NON-EMPTY `path_bytes` — while warning
//!    that "correct only because the caller filters" was one file from breaking.
//!
//!    **#3747 removed that filter, so these `0` rows are now load-bearing.** The
//!    filter WAS the defect #3612 described: a legal empty `text`/`blob` key was
//!    DROPPED from the `Value::Map`. The caller now decodes every cell path, so this
//!    table decides an empty key's fate and the empty-key tests are reached by a real
//!    read (UNIT-ONLY labels gone). Where it ADMITS `0` but no `Value` can carry it,
//!    the decode below preserves the entry OPAQUELY; a typed one is **#3805**'s.
//!
//! # When this site may return `Err` — and why the line is drawn at Cassandra
//!
//! MEASURED through the public surface (issue #3612 review round 1): an `Err`
//! from here does NOT reach the caller of a `SELECT`. It propagates out of
//! `parse_complex_column`, and row assembly then SWALLOWS it — `row_data.rs`'s
//! complex-column `match` has an `Err(e) => { tracing::debug!(…); break; }` arm
//! (the ONLY handler, shared by both the user-facing read and the
//! compaction/elements-out read, which are just the two arms producing
//! `parse_result`). `break` leaves the column loop, so the failing column AND
//! EVERY LATER ON-DISK COLUMN silently vanish from the row. Reproduced with a
//! real `SELECT` over the committed Cassandra fixture: declaring `cm` as
//! `map<int,int>` against its 70-byte on-disk UDT cell path returned exit 0 and
//! `"cm": null, "tm": null` with every other column intact.
//!
//! A silently TRUNCATED ROW is more destructive than one wrongly-typed value, so
//! this site does NOT invent error classes. The rule, and it is a rule:
//!
//! * **`Err` only where Cassandra's own `validate`/`split` THROWS** — a wrong
//!   fixed width, a non-4/16-byte `inet`, or trailing bytes after a composite's
//!   components. Those inputs are corrupt on Cassandra's own terms, so refusing
//!   them adds no availability risk for data Cassandra would have read.
//! * **NEVER `Err` merely because CQLITE cannot model the declared type.**
//!   Cassandra reads such a key fine; only this reader cannot. That case returns
//!   the opaque `Value::Blob` the shared decoder produced and reports it to the
//!   caller, which `warn!`s once per column per row naming the column, the
//!   declared type and how many entries were affected — so the row stays whole and
//!   the gap is visible in the log rather than in a missing column.
//!
//! The swallow itself is a PRE-EXISTING defect of row assembly, not of this
//! module, and is tracked separately (see the PR for #3612).
//!
//! ## The undecodable-key signal is REPORTED to the caller; nothing here `warn!`s
//!
//! There is no `warn!` in this module. [`Self::parse_cell_path_key_reporting`]
//! runs ONCE PER MAP ENTRY and only SETS an `opaque_out` flag; `complex_column`'s
//! map branch counts the entries whose key came back opaque and emits at most ONE
//! line per column per ROW, carrying that count as `affected_entries`. WHY that is
//! the right cardinality is stated ONCE, on `parse_cell_path_key_reporting`'s own
//! doc comment, and is deliberately not restated here — an earlier revision of
//! this header argued the OPPOSITE (that caller-side aggregation was "deliberately
//! NOT taken"), outlived the change that took it, and contradicted that doc
//! comment. One statement, at the site that owns the signal — and, since the
//! prose was what drifted, the cardinality itself is now PINNED by a test:
//! `warn_cardinality_tests` (declared at the foot of this file) asserts that two
//! undecodable entries produce exactly ONE record carrying `affected_entries: 2`.
//!
//! What is recorded here, because the aggregate does not answer it, is a DIFFERENT
//! question: the per-row line is not additionally DEDUPLICATED across rows, and
//! neither available place to hold "already warned" state is worth its cost.
//!
//! * **Per reader instance.** There is no natural home — `V5CompressedLegacyParser`
//!   is plain owned data with no interior mutability and the decode entry point
//!   takes `&self`, so this needs a new `Mutex`/`RefCell` field touched on the hot
//!   decode path. And it would not even work: the instance is built PER BLOCK
//!   (`parsing/block_entries.rs`'s `parse_block_entries_at_now`, called per block
//!   from the scan stream) and per point read, so the dedupe window is one block,
//!   not one scan — a constant factor, for a lock.
//! * **A process-lifetime `static` latch** (`Once`/`AtomicBool`) is worse than the
//!   repetition, not merely insufficient: it would suppress the warning for a
//!   DIFFERENT table read later in the same process, turning a noisy disclosure
//!   into a missing one.
//!
//! # Decoder enumeration and exactness disposition (issue #3612 round 2)
//!
//! Enumerated by following `parse_value_from_raw_bytes`'s `match` rather than
//! from anyone's list: **24 top-level arms**, plus the registry-bare-name UDT
//! sub-path inside the final `other` arm — **25 reachable decode paths**. Every
//! one is EXACT here, by exactly one of three mechanisms:
//!
//! | reachable decoder | how it is made exact |
//! |---|---|
//! | text / ascii / varchar (+3 marshal aliases) | whole slice by construction (UTF-8 validated over all of `data`) |
//! | blob / bytes | whole slice by construction |
//! | varint, inet | whole slice by construction (borrowed entire) |
//! | decimal | whole slice by construction (`scale` = `data[..4]`, unscaled = `data[4..]`) |
//! | int, bigint/counter, boolean, uuid/timeuuid, float, double, smallint, tinyint, timestamp, date, time | caller's ALLOWED-width table, per type, mirroring Cassandra's serializers (stronger than a consumption compare) |
//! | inet (widths) | same table, `[0, 4, 16]` — `N`-or-`0`, two non-empty widths |
//! | frozen list (`parse_frozen_list_value_raw`) | reported offset, was DISCARDED — now checked |
//! | frozen set (`parse_frozen_set_value_raw`) | reported offset, was DISCARDED — now checked |
//! | frozen map (`parse_frozen_map_value_raw`) | reported offset, was DISCARDED — now checked |
//! | tuple (`parse_tuple_elements_raw`) | reported `&mut offset`, was DISCARDED — now checked |
//! | UDT, marshal + registry-bare-name (`parse_raw_type_value`, whose UDT work is TWO inline field loops in `raw_type_value.rs` — the marshal one and the registry-bare one — not a call to `parse_udt_value`) | reported offset, was DISCARDED — now checked |
//! | `frozen<T>` / `FrozenType(T)` | recursion for the VALUE; exactness is the inner arm's, and for a fixed-width inner it comes from the width table, which is why that table peels frozen first (B1) |
//! | duration | the SHARED decoder's own assert, one layer down: its `"duration"` arm reports where the third VInt ended, so this module no longer walks the framing itself (#3631 unification) |
//! | unknown type → opaque `Value::Blob` | whole slice by construction; also raises the caller-aggregated opaque-key signal (the `warn!` is the caller's — see above) |
//!
//! ## Nested consumption: THREE OF FOUR CLASSES ARE NOW REFUSED
//!
//! An earlier revision of this header carried a broad residual — "nested
//! consumption is unchecked at every level below the first" — naming four classes:
//! fixed-width scalars, nested tuples/UDTs, nested collections and `duration`. The
//! first three of those are now REFUSED, so that heading is stated here in the PAST
//! TENSE and only `duration` survives, as a DECIDED disposition rather than a gap.
//!
//! The tightening landed as **#3811** (`parse_value_from_raw_bytes` gained a
//! consumption channel: it is a thin wrapper over
//! `parse_value_from_raw_bytes_reporting` plus a consumption assert, so every
//! bounded caller of the short name inherits the rule) and the UNIFICATION #3811
//! asked for landed as **#3631**: there is ONE `require_fully_consumed`, in
//! `typed_value.rs`, which every layer names, and this module's `duration`
//! consumption special-case is GONE with it — the shared decoder's own assert
//! reaches a trailing byte first (see `decode_reporting_consumption` below).
//! What this module still owns is the fixed-width ALLOWED-width table, which is
//! cell-path-specific and has no equivalent on the value side.
//!
//! The refusals are pinned by name in
//! `row_decoder/raw_value/issue_3811_consumption_demo_tests.rs`:
//!
//! | class, as the old residual named it | now refused, by these tests |
//! |---|---|
//! | fixed-width scalars decoding from a prefix (a 5-byte `int`) | `bounded_int_over_width_is_refused` (control: `bounded_int_exact_decodes_ok`) |
//! | nested tuples and UDTs leaving extra components unread | `bounded_tuple_with_trailing_byte_is_refused` (control: `bounded_tuple_exact_decodes_ok`); `nested_udt_trailing_garbage_is_refused` (control: `nested_udt_exact_decodes_ok`), plus its twins `structural_nested_udt_trailing_garbage_is_refused` and `inline_udt_trailing_garbage_is_refused` |
//! | nested collections stopping at the declared count | `bounded_list_with_trailing_byte_is_refused` and `bounded_set_with_trailing_byte_is_refused` (control: `bounded_list_exact_decodes_ok`) |
//!
//! Consequently the collapse the old residual led with is GONE: measured on this
//! tree, a `frozen<list<int>>` cell path of `[count=1][len=5][5B]` (13 bytes) is now
//! REFUSED ("decoded only 4 of 5 byte(s)" — the NESTED element's width), while the
//! well-formed `[count=1][len=4][4B]` (12 bytes) still decodes to
//! `Frozen(List([Integer(7)]))`. The two no longer decode to one key, so the map
//! entry a Python read used to lose to that collision is not lost any more. (The
//! old header carried a "SYMPTOM: A PYTHON READ CAN SILENTLY LOSE A MAP ENTRY"
//! section for that collapse; it described behaviour that no longer occurs and is
//! deleted rather than kept as a war story.)
//!
//! A GENUINELY SHORT encoding stays LEGAL and that is deliberate: `TupleType.split`
//! rule 1 permits omitted trailing components, which leaves `consumed == len` and is
//! accepted. Cassandra cannot drop a UDT field (`AlterTypeStatement` at
//! `cassandra-5.0.8` offers only `AddField`, `RenameFields`, `AlterField`), so
//! schema evolution yields exactly these short encodings — refusing them would break
//! ordinary evolved data.
//!
//! ### `duration` IS tolerated — parity-correct, decided under #3778 Option A
//!
//! The fourth class survives, at TWO sites, and only one of them is nested:
//!
//! * `raw_type_value.rs`'s `"duration"` arm — a duration NESTED inside a frozen
//!   composite (its errors read `Frozen element '{}'`). The third `parse_vint` binds
//!   `_remaining` and DISCARDS it, then `offset += duration_len` advances by the
//!   DECLARED length.
//! * `cell_value_scalar.rs`'s `CellKind::Duration` arm — a plain top-level CELL,
//!   NOT nested. Leftover bytes emit one `warn!("… has {} extra bytes after
//!   parsing")` and the value is returned.
//!
//! **Cassandra tolerates the same bytes**, which is why this is not a defect. Read
//! at the pinned tag: `serializers/DurationSerializer.java` `deserialize` reads three
//! VInts and returns, with no consumption check, and its `validate` (`:80-105`)
//! enforces only `size >= 3` ("Expected at least 3 bytes for a duration (%d)"), the
//! months/days 32-bit bounds and the same-sign rule — no upper bound.
//! `grep -n "remaining\|hasRemaining\|limit()"` over that file plus
//! `db/marshal/DurationType.java` gives ZERO hits. `TupleType.split` rule 4 refuses
//! trailing bytes at the TUPLE FRAMING level, not inside one element's declared
//! length.
//!
//! So this is a **DECIDED disposition, not an open gap**: the #3778 lead ruling took
//! Option A (keep the tolerance, pin it, declare it) and explicitly REFUSED Option B
//! (refuse the bytes), which would have converted reads Cassandra performs
//! successfully into hard CQLite failures with no oracle supporting the strictness.
//! An earlier revision of this header called the disposition open and tracked it as
//! #3723; the deciding issue is **#3778**.
//!
//! The residual it leaves, accepted deliberately: two byte strings differing ONLY in
//! trailing bytes inside `duration_len` decode to ONE, EQUAL `Value::Duration`. That
//! is pinned at both sites by `row_decoder/issue_3778_duration_parity_tests.rs`
//! (`two_encodings_differing_only_in_trailing_bytes_decode_equal_nested` and
//! `…_cell`), whose header carries the oracle so a future reader does not "fix" it.
//! That file also pins the ASYMMETRY: the third duration arm, `raw_value`'s bounded
//! one, REFUSES the same input via `require_fully_consumed` — stricter than the
//! oracle, filed as follow-up **#4038** (direction: relax to match the oracle) and
//! deliberately unchanged here.
//!
//! None of this is patched with a second framing walk here: a call-site validator
//! that must know about every decoder is precisely the shape this module replaced.
//!
//! # Presenting the key EXACTLY as the FROZEN spelling does (issue #3612, R3-F2/R7)
//!
//! **This is now true for every composite subject the corpus has, and it is
//! achieved in ONE place, and that is the point of the current shape.**
//!
//! **`marshal_element::map_key_type_for_decode` is the single rule**, called by the
//! MULTICELL map reader (`complex_column`) and the FROZEN map reader
//! (`cell_value_complex`). Both hand the SAME key-type string to the SAME decoder,
//! so their `Value` keys are equal by construction. Nothing in THIS file adjusts a
//! key's presentation any more; the checks below borrow a peeled VIEW
//! (`peeled_for_inspection`) and return the decoded value untouched.
//!
//! It got here in two steps, and the first was insufficient in a way worth keeping
//! on the record:
//!
//! * **R7** made the multicell branch prefer the authoritative MARSHAL key type
//!   (`prefer_udt_marshal_element`, #1340), fixing UDT and tuple keys, and paired
//!   it with a value-level wrapper fixup on the multicell side only.
//! * **Round 8** found that fixup was the instance fix, not the class fix: a
//!   multicell `map<frozen<set<frozen<U>>>, int>` supplies
//!   `FrozenType(SetType(UserType(..)))` while the frozen spelling supplies
//!   `SetType(UserType(..))`, giving `Frozen(Set(Udt))` against `Set(Udt)` — the
//!   same defect one nesting level down. Two readers producing two presentations
//!   of one value is a CONSOLIDATION problem, so the fixup was deleted and the
//!   normalization moved into the shared rule, where it strips the redundant outer
//!   `FrozenType` from a MARSHAL key only. The strip is a NO-OP on the frozen
//!   reader (Cassandra omits that marker inside a frozen collection), which is why
//!   wiring both sides changes only the multicell side's output.
//!
//! Why the marker differs at all is Cassandra's own metadata: a MULTICELL map key
//! must be explicitly frozen, so its marshal keeps `FrozenType`, while everything
//! inside a FROZEN collection is already frozen, so the inner marker is omitted.
//!
//! Measured parity, per subject, unpeeled (`cqlite-core/tests/issue_3612_multicell_map_composite_key.rs`
//! for the fixture-backed subjects; `cell_path_key_tests.rs` for the set-keyed one,
//! which no fixture in the corpus supplies):
//!
//! | subject (multicell vs frozen) | before | after |
//! |---|---|---|
//! | `cm` vs `fcm` (UDT key) | `Udt` == `Udt` | unchanged, still equal |
//! | `tm` vs `ftm` (UDT key) | `Udt` == `Udt` | unchanged, still equal |
//! | `m_tuple_udt` vs `f_map_tuple_udt` (TUPLE key) | `Frozen(Tuple[Frozen(Udt),Int])` vs `Tuple[Udt,Int]` — **UNEQUAL** (fixed in R7) | `Tuple[Udt,Int]` both — **EQUAL, equal hash** |
//! | set-of-UDT key (no fixture; unit-tested from the two marshal spellings) | `Frozen(Set[Udt])` vs `Set[Udt]` — **UNEQUAL** (round 8) | `Set[Udt]` both — **EQUAL, equal hash** |
//!
//! No subject remains non-parity. Value equality is asserted wherever the fixture
//! stores the SAME logical key in both spellings (`cm`/`fcm`, `tm`/`ftm`, and
//! `m_tuple_udt` id=3); the other tuple rows hold deliberately different data, so
//! they are covered by `Value`-nesting equality, which is the property that
//! generalises.
//!
//! # The asymmetry across the three cell-path/key readers (issue #3612)
//!
//! THIS PASSAGE IS THE SINGLE STATEMENT OF THE CROSS-READER COMPARISON.
//! `read_assembly.rs` CITES it rather than paraphrasing: three paraphrases there were
//! false, each one written while fixing the last (rounds 1-3 of #3612 review).
//!
//! For a key type CQLite models nowhere, TWO of the three serve an opaque `Value::Blob`:
//! this multicell path (plus the caller-aggregated `warn!`) and the frozen-map reader
//! (`parse_frozen_map_value`, via `read_frozen_element`). The THIRD — the multi-generation
//! MERGED read — FAILS CLOSED instead, returning `Error::unsupported_format` from
//! `composite_collection_unsupported` before any ordering decision is reached; serving an
//! opaque blob there was deliberately abandoned. So there IS an availability difference
//! between the single-generation readers and the merged read, tracked by issue #2339.
//!
//! For a key type CQLite DOES model (`tuple<…>`, a nested collection, a resolvable UDT)
//! the divergence is WIDER, not narrower: the single-generation readers decode it
//! STRUCTURALLY while the merged read still FAILS CLOSED. So #2339 is MERGE-side work,
//! and nothing on this path waits on it.
//!
//! (`key_is_opaque_composite` IS that check's predicate: both guard sites — set element,
//! map key — test it and return `composite_collection_unsupported`. Its only other consumer
//! (its own `Frozen` arm recurses) is `sort_elements_by_cell_path`'s raw-byte ordering arm,
//! DEFENSIVE only because the guard fires first. An earlier revision cited the predicate for
//! "NO availability difference" — right symbol, conclusion exactly backwards.)
//!
//! They DIVERGE on CORRUPTION: only this path validates fixed widths and full
//! consumption, so a multicell key with a wrong width or trailing bytes is
//! REFUSED here (and, until the row-assembly swallow is fixed, that manifests as
//! a truncated row) while the frozen spelling of the same map would decode it
//! from a prefix. That asymmetry is intentional — this is the one site where the
//! whole slice is known to BE the key — but it is not symmetric, and widening it
//! to the frozen/set routes is out of #3612's scope.

use super::*;
use crate::types::EmptyValueType;

impl V5CompressedLegacyParser {
    /// Parse a MULTICELL map's cell-path key, DISCARDING the undecodable-key
    /// signal; see [`Self::parse_cell_path_key_reporting`], which is the
    /// production entry point and carries the contract.
    ///
    /// TEST-ONLY, and gated so `-D dead-code` says so honestly: the only production
    /// caller (`complex_column`'s map branch) takes the reporting form, because it
    /// aggregates the signal across a row's entries. This wrapper exists solely to
    /// keep this module's unit call sites on the simpler signature; an
    /// `#[allow(dead_code)]` would have silenced a true statement instead.
    #[cfg(test)]
    pub(super) fn parse_cell_path_key(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
    ) -> Result<Value> {
        let mut opaque = false;
        self.parse_cell_path_key_reporting(data, type_str, column_name, &mut opaque)
    }

    /// Decode a MULTICELL map's cell-path key and REPORT whether it surfaced as
    /// opaque bytes.
    ///
    /// `data` is the bare serialized key (the CellPath's VInt length prefix is
    /// already stripped by the caller). `type_str` is the map's declared KEY type
    /// in whatever spelling the authoritative source provided — a CQL short form
    /// from the schema (`frozen<collide>`, `int`) or a Cassandra marshal form from
    /// `Statistics.db` (`org.apache.cassandra.db.marshal.UserType(…)`) — and is
    /// forwarded WITH ITS CASE INTACT (see the module header).
    ///
    /// `opaque_out` is set when the declared type is one this reader cannot model,
    /// so the caller can aggregate. It is a caller-side signal rather than a
    /// `warn!` here because this function runs ONCE PER MAP ENTRY: a scan of a
    /// table with an unmodellable key type would emit `entries x rows` identical
    /// lines, which floods the log, can exhaust log storage, and destroys the one
    /// number an operator actually wants — HOW MANY entries were affected
    /// (roborev round 8, finding 2). The caller emits at most one line per column
    /// per row, carrying that count; the message's content is unchanged.
    pub(super) fn parse_cell_path_key_reporting(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
        opaque_out: &mut bool,
    ) -> Result<Value> {
        let allowed = self.cell_path_key_allowed_widths(type_str);
        if !allowed.is_empty() && !allowed.contains(&data.len()) {
            return Err(Error::corruption(format!(
                "Map key '{}' of type '{}' requires exactly {} bytes, got {}",
                column_name,
                type_str,
                allowed
                    .iter()
                    .map(|w| w.to_string())
                    .collect::<Vec<_>>()
                    .join(" or "),
                data.len()
            )));
        }
        // ═══ THE EMPTY-BUFFER ADMISSION GATE ═══
        //
        // Consulted for EVERY empty cell path, BEFORE the decode — never only on
        // a decode FAILURE. That placement is the whole content of roborev job
        // 449 finding C: a gate that fires only in the `Err` arm cannot reach a
        // family whose decoder ACCEPTS the empty buffer, so `varint` and `inet`
        // — both ADMITTED by the tag table — kept a SECOND spelling of the empty
        // buffer (`Varint(b"")` / `Inet(b"")`) beside the canonical
        // `Value::Empty(tag)`. Issue #4079; CLOSED here.
        //
        // TWO facts decide this, and neither is this call site's to invent:
        //
        //  * The ADMISSION GATE is [`EmptyValueType::for_cql_type`] — the ONE
        //    place the legal/corruption line is drawn, on Cassandra's
        //    `validate()` rather than on decodability. A `Some` tag means
        //    `cassandra-5.0.8`'s serializer for that family accepts the empty
        //    buffer AND maps it to null (see that function's membership rule),
        //    so the key is PRESENT, TYPED and MEANINGLESS-VALUED. Because the
        //    gate is drawn on `validate()` and not on decodability, it is
        //    CORRECT to consult it without regard to what the decoder would have
        //    done — which is exactly what moving it above the decode expresses.
        //  * `opaque_out` stays UNSET on the typed branch. The flag exists to
        //    diagnose a key this reader CANNOT MODEL, and it now can; leaving it
        //    set would emit a `warn!` per column per row for correct data, which
        //    is the misleading-diagnostic half of #3612.
        //
        // # THE GATE IS THE TAG TABLE, NOT THE WIDTH TABLE
        // The guard was `data.is_empty() && allowed.contains(&0)`, i.e. it asked
        // the WIDTH table whether the empty buffer was legal. Those are two
        // authorities answering two DIFFERENT questions, and they disagree on
        // exactly one family: `cql_short_allowed_widths` returns the EMPTY slice
        // for `decimal` because it is VARIABLE-width (Cassandra accepts `0` or
        // `>= 4`), so `allowed.contains(&0)` was FALSE and a legal empty
        // `decimal` key was REFUSED — while `for_cql_type` admits it from
        // `DecimalSerializer.java:31-34,58-63`, whose own message reads
        // *"Expected 0 or at least 4 bytes"* (issue #3805 REQ-3805-02; the
        // committed claim that an empty `decimal` is corrupt was WRONG, and
        // `empty_value.rs`'s `Decimal` variant says so too). A width table cannot
        // express `{0} ∪ [4, ∞)`, so it is the wrong oracle for this question;
        // the tag table is the right one.
        //
        // # WHAT THE GATE DOES *NOT* REACH — the bound, MEASURED not reasoned
        //  * `text`/`ascii`/`varchar`/`blob`: `for_cql_type` is `None`, because an
        //    empty buffer is a legal, MEANINGFUL value there
        //    (`AbstractTextSerializer.java:72-77`, `BytesSerializer.java:57-62`
        //    override `isNull` to say so). They keep `Text(b"")`/`Blob(b"")`, and
        //    `regression_3747_empty_map_key_tests::text_and_blob_empty_keys_keep_
        //    their_native_spelling_and_the_table_says_so` asserts BOTH halves —
        //    the table's `None` and the decode result — rather than trusting the
        //    table.
        //  * `tinyint`/`smallint`/`date`/`time`: refused by the WIDTH check above
        //    (their `allowed` excludes `0`, exactly as Cassandra's bare `!= N`
        //    `validate` does), and `for_cql_type` is `None` for them anyway, so
        //    this gate is reached only for a spelling the width table did not
        //    classify and then declines it.
        //  * `duration`, every composite (`list`/`set`/`map`/`tuple`/UDT/
        //    `frozen<collection>`) and `custom`: `for_cql_type` is `None`, so
        //    they fall through to the decode and keep their existing outcome —
        //    an `Err` for `duration` and the collections, a structural decode for
        //    `tuple`, an opaque blob for an unresolvable UDT name. Pinned by
        //    `no_other_empty_width_family_becomes_a_sentinel`.
        if data.is_empty() {
            if let Some(tag) = self
                .cell_path_key_cql_type(type_str)
                .as_ref()
                .and_then(EmptyValueType::for_cql_type)
            {
                return Ok(Value::Empty(tag));
            }
        }
        // ONE decode, which also REPORTS what it consumed (see
        // `decode_reporting_consumption`).
        let (decoded, consumed) =
            match self.decode_reporting_consumption(data, type_str, column_name, 0) {
                Ok(v) => v,
                // #3747's DOOR 1 — an empty buffer the ADMISSION GATE above did
                // NOT admit. (DOOR 2 is the decode-succeeded-with-`Null` case
                // below; the two labels are the file's own cross-reference and
                // both are kept.) Behaviour here is EXACTLY what it was before
                // #3805 slice 2, which is what makes the gate a strict ADDITION
                // rather than a widening:
                //   * the WIDTH table admitted the empty buffer but no sentinel
                //     speaks for the family -> the pre-existing opaque policy.
                //     Unreachable today (every `0`-admitting width family has a
                //     tag, MEASURED) and kept because the width table and the tag
                //     table are two authorities that could diverge again.
                //   * otherwise -> the decoder's OWN error, verbatim. That is what
                //     keeps `duration` (`for_cql_type` -> `None`, empty key `Err`,
                //     MEASURED) and every composite refused instead of silently
                //     becoming an opaque blob.
                Err(_) if data.is_empty() && allowed.contains(&0) => {
                    *opaque_out = true;
                    return Ok(Value::blob(Vec::new()));
                }
                Err(e) => return Err(e),
            };
        // A PEELED VIEW FOR THE CHECKS ONLY — the value itself is returned exactly
        // as the shared decoder produced it (see the return, and the module header's
        // parity section). A `frozen<absent_udt>` key can come back as
        // `Frozen(Blob)`, so the opaque-value test below must look THROUGH any
        // wrapper or it silently stops diagnosing every frozen-spelled undecodable
        // key. Borrowing rather than rebinding is what keeps the inspection from
        // becoming a presentation change, which is the defect roborev round 8 found
        // one nesting level down.
        let probe = Self::peeled_for_inspection(&decoded);
        // #3747's DOOR 2: the decode SUCCEEDED with `Null`. Keyed on the PEELED
        // answer, never the width table (`inet` admits 0 and decodes) and never a
        // byte length. Full argument + the four defects it caused: `frozen_map`.
        if data.is_empty() && matches!(probe, Value::Null) {
            *opaque_out = true;
            return Ok(Value::blob(Vec::new()));
        }
        // THE EXACTNESS RULE. For a cell path the whole slice IS the key, so a
        // decoder that stopped short read a PREFIX and two distinct byte strings
        // would collapse to one logical key. Where the decoder can say how far it
        // got, require it to have reached the end.
        //
        // This one comparison subsumes three separate behaviours, which is why it
        // replaced the hand-rolled framing validator that preceded it (issue #3612
        // review round 2): trailing bytes after the components (`pos < len`) are
        // REFUSED; a partial 1-3 byte component-length header (also `pos < len`,
        // because the decoders treat it as "trailing fields omitted" and do NOT
        // advance past it) is REFUSED; and a genuinely SHORT encoding, whose
        // omitted components leave `pos == len`, is ACCEPTED — which is exactly
        // Cassandra 5.0.8 `TupleType.split`'s pair of rules (`if (position ==
        // length) return copyOfRange(...)` and `if (position < length) throw`).
        if let Some(consumed) = consumed {
            if consumed != data.len() {
                return Err(Error::corruption(format!(
                    "Map key '{}' of type '{}' decoded only {} of {} byte(s); the whole \
                     cell path must be the key (trailing bytes, or a partial trailing \
                     component header, are corruption)",
                    column_name,
                    type_str,
                    consumed,
                    data.len()
                )));
            }
        }
        // The declared type is one this reader cannot model, so the shared
        // decoder handed back the raw bytes. Report it to the caller — which
        // `warn!`s once per column per row with the count — but do NOT return
        // `Err`: an `Err` here is swallowed by row assembly into a silently
        // truncated row (see the module header's error-budget rule), which is
        // more destructive than the opaque value, and Cassandra itself reads
        // such a key without complaint. Raising the signal only HERE is what
        // distinguishes this from a key DECLARED `blob`, which is a correct
        // decode and stays silent — the misleading-diagnostic half of #3612.
        if matches!(probe, Value::Blob(_)) && !self.cell_path_key_declares_blob(type_str) {
            *opaque_out = true;
        }
        // Returned EXACTLY as the shared decoder produced it. There is deliberately
        // no presentation fixup here any more: both map readers now receive the same
        // key-type string from `map_key_type_for_decode`, so they decode to the same
        // `Value` by construction. A fixup on this side was how round 3 achieved
        // parity for UDT keys and how it missed collection keys.
        Ok(decoded)
    }

    /// Decode a cell-path key AND report how many bytes the decode consumed.
    ///
    /// `Ok((value, Some(n)))` — the decoder reported that it consumed `n` bytes.
    /// `Ok((value, None))`    — the arm consumes the WHOLE slice by construction,
    ///                          so there is nothing to compare (see below).
    ///
    /// # Why this exists rather than a post-hoc framing validator
    /// Round 1 of review added a hand-rolled walk over the component framing to
    /// catch trailing bytes. Round 2 found two more holes in the SAME class —
    /// frozen list/set/map keys and `duration`, plus a partial trailing header —
    /// because a validator at the call site has to know about every decoder, and
    /// this one knew about two. Every composite decoder ALREADY reports a consumed
    /// offset, which `parse_value_from_raw_bytes` used to DISCARD (`let (val, _)`)
    /// until #3811 gave it a reporting twin; the correct shape is to keep that
    /// offset instead of re-deriving it.
    ///
    /// # The `None` arms are exact, not unchecked
    /// `None` is returned only where the arm's contract IS "the entire slice is the
    /// value": text/ascii/varchar (validated UTF-8 over all of `data`), blob/bytes,
    /// varint and inet (each borrows the whole slice), and decimal (scale from
    /// `data[..4]`, unscaled from `data[4..]`). Fixed-width scalars also return
    /// `None`, and for them exactness comes from the caller's ALLOWED-width table
    /// instead — which is why that table must be consulted on the PEELED type
    /// (finding B1: while it classified the raw string, a `frozen<int>` reached
    /// this `None` with no width pinned anywhere). The opaque `Value::Blob`
    /// default likewise borrows all of `data`.
    ///
    /// # Dispatch must mirror `parse_value_from_raw_bytes`
    /// The guards below are the same predicates, in the same ORDER (frozen before
    /// UDT, because `is_udt_type` is a substring match that also matches
    /// `FrozenType(UserType(..))`).
    ///
    /// ## ADDING AN ARM THERE IS A MANUAL OBLIGATION HERE — no test enforces it
    /// `cell_path_key_tests::every_composite_cell_path_key_spelling_is_consumption_checked`
    /// runs a HAND-CURATED list and requires each spelling to refuse a trailing byte,
    /// so it catches an arm BROKEN OR REMOVED here (its `cases.len()` pin catches a
    /// case dropped from the list). It cannot catch the OPPOSITE direction — a
    /// composite arm added to `parse_value_from_raw_bytes` and not here falls through
    /// to the `None` "whole slice by construction" default and prefix-decodes
    /// SILENTLY. An earlier revision of this doc claimed the guard did catch it.
    /// Deriving it is FEASIBLE and is simply NOT DONE HERE — the reason is scope, not
    /// impossibility. A scan pairing `parse_value_from_raw_bytes`'s `starts_with` literals
    /// with this file's would match all ten (five inline in both files, five as the `const`s
    /// below), but it needs a scanner that locates a fn body in another module's source, and
    /// the one NON-literal arm — `other if is_udt_type(other)` — carries
    /// no literal for it to pair, so it would stay uncovered either way.
    fn decode_reporting_consumption(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, Option<usize>)> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Map key '{}': type nesting depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        let lower = type_str.to_ascii_lowercase();
        // Full literals rather than `format!("{M}…")`: this runs once per map
        // ENTRY per row, and the six allocations were pure waste on a hot path.
        const M_FROZEN: &str = "org.apache.cassandra.db.marshal.frozentype(";
        const M_LIST: &str = "org.apache.cassandra.db.marshal.listtype(";
        const M_SET: &str = "org.apache.cassandra.db.marshal.settype(";
        const M_MAP: &str = "org.apache.cassandra.db.marshal.maptype(";
        const M_TUPLE: &str = "org.apache.cassandra.db.marshal.tupletype(";

        // frozen<T> / FrozenType(T): recurse on the inner type, then RE-WRAP,
        // mirroring `parse_value_from_raw_bytes`'s frozen arm
        // (`Ok(Value::Frozen(Box::new(inner)))`) exactly.
        //
        // The re-wrap is REQUIRED, and its absence used to be masked. This
        // dispatcher previously returned the bare inner value because the
        // cell-path key entry point peeled and re-applied a wrapper of its own; with
        // that fixup deleted (roborev round 8), not wrapping here would make a
        // SCHEMA-form key diverge from the frozen reader in the opposite direction —
        // `frozen<set<int>>` reaches BOTH readers unchanged (its marshal is not
        // UDT-bearing, so `map_key_type_for_decode` keeps the schema form), and the
        // frozen reader renders it `Frozen(Set)`. Mirroring the reference decoder is
        // what keeps the two equal in the schema case, exactly as the shared rule's
        // strip does in the marshal case.
        //
        // Deliberately BEFORE the UDT arm: `is_udt_type` is a substring match that
        // also matches `FrozenType(UserType(..))`.
        if lower.starts_with("frozen<") || lower.starts_with(M_FROZEN) {
            let inner = self.extract_frozen_inner_type(type_str)?;
            let (value, consumed) =
                self.decode_reporting_consumption(data, &inner, column_name, depth + 1)?;
            return Ok((Value::Frozen(Box::new(value)), consumed));
        }

        if lower.starts_with("list<") || lower.starts_with(M_LIST) {
            let elem = self.extract_collection_element_type(type_str, "list")?;
            let (val, off) =
                self.parse_frozen_list_value_raw(data, 0, &elem, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("set<") || lower.starts_with(M_SET) {
            let elem = self.extract_collection_element_type(type_str, "set")?;
            let (val, off) =
                self.parse_frozen_set_value_raw(data, 0, &elem, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("map<") || lower.starts_with(M_MAP) {
            let (k, v) = self.extract_map_types(type_str)?;
            let (val, off) =
                self.parse_frozen_map_value_raw(data, 0, &k, &v, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("tuple<") || lower.starts_with(M_TUPLE) {
            let element_types = self.extract_tuple_element_types(type_str)?;
            if element_types.is_empty() {
                return Err(Error::schema(format!(
                    "Map key '{}': empty tuple type '{}'",
                    column_name, type_str
                )));
            }
            let mut off = 0usize;
            let elements = self.parse_tuple_elements_raw(
                data,
                &mut off,
                data.len(),
                &element_types,
                column_name,
                depth + 1,
            )?;
            return Ok((Value::Tuple(elements), Some(off)));
        }
        // UDT: both the marshal `UserType(..)` form and a registry-resolved bare
        // name route through `parse_raw_type_value`, which reports the offset after
        // the last field it consumed — including the "trailing fields omitted"
        // early exit, which is what makes a partial trailing header visible here.
        if Self::is_udt_type(type_str)
            // ORIGINAL case, not `lower`: the callee re-looks-up with `type_str`
            // and `get_udt` is a deliberately case-SENSITIVE map get, so a
            // lowercased probe here would make this guard fire on keys the callee
            // cannot resolve (and miss ones it can).
            || self
                .udt_registry
                .as_ref()
                .is_some_and(|r| r.get_udt_qualified(&self.keyspace, type_str).is_some())
        {
            let (val, off) = self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
            return Ok((val, Some(off)));
        }
        // `duration`'s own three-VInt walk USED to live here, and was live only
        // while `parse_value_from_raw_bytes` discarded the count its arm already
        // had. #3811 made the short name assert `consumed == data.len()` and made
        // that arm report where the third VInt ended, so the callee refuses a
        // trailing byte one layer down and the walk here could never fire — dead
        // code that only appeared to do the work. Folded out under #3631's
        // ONE-implementation ruling (recorded beside `require_fully_consumed`);
        // both layers refuse in the shared "decoded only N of M byte(s)" class, so
        // `cell_path_key_tests`'s pinned assertions are unaffected. Everything
        // reaching here consumes the whole slice by construction (see above) or is
        // enforced by the callee: nothing left to compare.
        Ok((
            self.parse_value_from_raw_bytes(data, type_str, column_name, depth)?,
            None,
        ))
    }

    /// The declared KEY type as a [`CqlType`], or `None` when this module's own
    /// classifiers cannot name it.
    ///
    /// Composed from the TWO normalizers already in this decode path — no third
    /// parser, because a second opinion about a type spelling is the drift this
    /// module's header calls out by name:
    ///
    ///   * `frozen<…>` / `FrozenType(…)` is peeled by
    ///     [`Self::peel_frozen_spellings`], the SAME unwrapper
    ///     [`Self::cell_path_key_allowed_widths`] and
    ///     [`Self::cell_path_key_declares_blob`] use, so all three form one opinion
    ///     about which spellings are frozen.
    ///   * a MARSHAL name goes through [`Self::native_marshal_to_cql_type`] — the
    ///     crate's one marshal-class-to-[`CqlType`] table, and the very table
    ///     `primitive_marshal_to_cql_short` (which the width classifier consults)
    ///     is built on. It enforces the package rule, so `com.acme.Int32Type` is
    ///     `None` here rather than silently CQL `int`.
    ///   * everything else is a CQL short form and goes through
    ///     [`CqlType::parse`], the crate's one CQL-type-string parser
    ///     (case-insensitive, like the decoder's own lowercased matching).
    ///
    /// `CqlType::parse` cannot fail for a bare unrecognised name — it yields
    /// `CqlType::Custom`, for which `EmptyValueType::for_cql_type` is `None` — so
    /// an unmodelled type reaches the caller's opaque fallback rather than a
    /// guessed family (#28).
    fn cell_path_key_cql_type(&self, type_str: &str) -> Option<CqlType> {
        let peeled = self.peel_frozen_spellings(type_str);
        if peeled.contains("org.apache.cassandra.db.marshal.") {
            return Self::native_marshal_to_cql_type(&peeled);
        }
        CqlType::parse(&peeled).ok()
    }

    /// Whether `type_str` DECLARES a blob key, i.e. whether `Value::Blob` is the
    /// CORRECT decode result rather than the shared opaque default.
    ///
    /// The distinction cannot be made from the RESULT — a declared `blob` key and
    /// an undecodable key both yield `Value::Blob` — so it is made from the
    /// DECLARED type. `frozen<…>`/`FrozenType(…)` is peeled first: CQL does not
    /// permit `frozen<blob>` as a map key, but a blob is still a blob under any
    /// spelling and must not be misdiagnosed as undecoded.
    pub(super) fn cell_path_key_declares_blob(&self, type_str: &str) -> bool {
        let t = self.peel_frozen_spellings(type_str);
        // CQL spells a CUSTOM type as a SINGLE-QUOTED marshal class name
        // (`'org.apache.cassandra.db.marshal.BytesType'`), so strip the quotes.
        let t = t.trim_matches('\'').trim();

        // EXACT NAMES ONLY — never a suffix match, and never a synthesised package
        // prefix (issue #3612, roborev round 9 finding 2).
        //
        // This used to ask `primitive_marshal_to_cql_short`, whose match is
        // `s.ends_with("BytesType")`, and for a bare name it PREFIXED the canonical
        // package first: `format!("org.apache.cassandra.db.marshal.{t}")`. That
        // defeated the guard every other consumer relies on. `parse_value_from_raw_
        // bytes` (raw_value.rs) and `cell_path_key_allowed_widths` both consult the
        // normalizer ONLY when the name already `contains` the Cassandra package, so
        // a foreign `com.acme.CustomBytesType` never reaches the suffix matcher and
        // correctly decodes to an opaque `Value::Blob`. Synthesising the prefix here
        // made that same name look like `BytesType`, so this function reported "a
        // declared blob" and SUPPRESSED the warning — silencing the diagnostic in
        // precisely the case it exists for, an unmodelled custom type.
        //
        // Deciding a type's identity from a name SUFFIX is inference from a name
        // pattern, which #28 forbids; a closed set of exact names is not.
        //
        // Marshal names are matched CASE-SENSITIVELY on purpose: the decoder's own
        // normalizer is case-sensitive, so `bytestype` does NOT decode as a blob and
        // must NOT be reported as a declared one, or the two would disagree. The CQL
        // short forms are case-INSENSITIVE because the decoder lowercases before
        // matching them.
        const CANONICAL_BLOB_MARSHAL: &str = "org.apache.cassandra.db.marshal.BytesType";
        const BARE_BLOB_MARSHAL: &str = "BytesType";
        if t == CANONICAL_BLOB_MARSHAL || t == BARE_BLOB_MARSHAL {
            return true;
        }
        matches!(t.to_ascii_lowercase().as_str(), "blob" | "bytes")
    }

    /// Strip every `frozen<T>` / `FrozenType(T)` layer off a DECLARED TYPE STRING.
    ///
    /// Shared by the width classifier and the declared-blob test so the two cannot
    /// form different opinions about which spellings are frozen — they did, and the
    /// disagreement was finding B1: the blob test peeled and the width classifier
    /// did not. Peels via the ONE existing unwrapper
    /// (`extract_frozen_inner_type`, which accepts both spellings
    /// case-insensitively); `Err` simply means "not frozen". Bounded by the
    /// decoder's own nesting limit so a pathological string cannot spin.
    fn peel_frozen_spellings(&self, type_str: &str) -> String {
        let mut t = type_str.trim().to_string();
        for _ in 0..MAX_TYPE_NESTING_DEPTH {
            match self.extract_frozen_inner_type(&t) {
                Ok(inner) => t = inner.trim().to_string(),
                Err(_) => break,
            }
        }
        t
    }

    /// A BORROWED view of `value` with any `Value::Frozen` wrappers seen through,
    /// for INSPECTION ONLY.
    ///
    /// Deliberately takes and returns a reference: the checks in
    /// [`Self::parse_cell_path_key_reporting`] must look through a wrapper, and the
    /// returned value
    /// must NOT be changed by having done so. The previous owned version rebound
    /// `decoded`, which turned an inspection into a presentation change and is the
    /// shape of roborev round 8's finding — parity is now the shared key-type
    /// rule's job (`map_key_type_for_decode`), never this function's.
    pub(crate) fn peeled_for_inspection(value: &Value) -> &Value {
        let mut v = value;
        while let Value::Frozen(inner) = v {
            v = inner;
        }
        v
    }

    /// The EXACT byte width a fixed-width cell-path key must have, or `None` for
    /// a variable-width / composite type (where the whole slice is consumed and
    /// no width invariant applies).
    ///
    /// Accepts both spellings of the type. A marshal form is normalized through
    /// the CASE-SENSITIVE [`Self::primitive_marshal_to_cql_short`], which returns
    /// `None` for anything parameterised (`UserType(…)`, `TupleType(…)`,
    /// `FrozenType(…)`, collections), so a composite key is never width-checked.
    /// The byte widths a fixed-width cell-path key MAY have. Empty slice =
    /// variable width (no invariant). A SLICE rather than one `usize` because
    /// several families admit more than one width: every `N`-or-`0` type admits
    /// the empty buffer, and `inet` admits 4 (IPv4) or 16 (IPv6) as well.
    ///
    /// # The declared type is PEELED of `frozen<…>` first, and that is load-bearing
    /// Classifying the RAW string sent every frozen-spelled FIXED-WIDTH key down
    /// the "variable width" branch (`frozen<int>` contains `'<'`;
    /// `FrozenType(Int32Type)` makes `primitive_marshal_to_cql_short` return `None`
    /// on the `(`). `decode_reporting_consumption` then takes its frozen arm,
    /// recurses on `"int"`, matches no composite guard and returns `None`
    /// consumption — so a 5-byte `frozen<int>` cell path decoded
    /// `Value::Integer` from `data[0..4]` with NO width check, NO consumption
    /// check and NO `warn!`: exactly the two-distinct-byte-strings-one-key defect
    /// this module exists to close. `frozen<inet>` accepted a 5-byte address the
    /// same way.
    ///
    /// "`frozen<int>` is not legal CQL" is not a defence, and was not treated as
    /// one: `cell_path_key_declares_blob` two functions below already peels frozen
    /// so `frozen<blob>` is recognised, and the tests pin all three of its
    /// spellings. A spelling cannot be handled in one helper and assumed
    /// impossible in the other.
    fn cell_path_key_allowed_widths(&self, type_str: &str) -> &'static [usize] {
        let peeled = self.peel_frozen_spellings(type_str);
        let type_str: &str = &peeled;
        let short: &str = if type_str.contains("org.apache.cassandra.db.marshal.") {
            match Self::primitive_marshal_to_cql_short(type_str) {
                Some(s) => s,
                None => return &[],
            }
        } else if type_str.contains('<') || type_str.contains('(') {
            // A CQL-short composite (`frozen<…>`, `tuple<…>`, `map<…>`): variable width.
            return &[];
        } else {
            // A CQL short form. `parse_value_from_raw_bytes` matches on the
            // LOWERCASED spelling, so normalize the same way here or a `"Int"`
            // from a hand-written schema would skip the check it then decodes under.
            return Self::cql_short_allowed_widths(&type_str.to_ascii_lowercase());
        };
        Self::cql_short_allowed_widths(short)
    }

    /// The allowed widths of a canonical lowercase CQL short form.
    ///
    /// Kept as a single table so the marshal and short-form routes cannot drift
    /// into two different opinions about a family's width.
    fn cql_short_allowed_widths(short: &str) -> &'static [usize] {
        match short {
            // --- `N` OR `0`: `size != N && !isEmpty` throws, so EMPTY is legal ---
            "int" | "float" => &[0, 4],
            "bigint" | "counter" | "double" | "timestamp" => &[0, 8],
            "uuid" | "timeuuid" => &[0, 16],
            // `BooleanSerializer` is spelled `size > 1`, i.e. 0 or 1.
            "boolean" => &[0, 1],
            // --- STRICT `!= N`: these four admit no empty buffer ---
            "tinyint" | "byte" => &[1],
            "smallint" | "short" => &[2],
            "date" => &[4],
            "time" => &[8],
            // `InetAddressSerializer.validate` RETURNS EARLY on empty and
            // otherwise delegates to `InetAddress.getByAddress`, which takes a 4-
            // or 16-byte address. So `inet` belongs to the `N`-or-`0` family, with
            // TWO non-empty widths.
            "inet" => &[0, 4, 16],
            // Variable-width by definition: text/ascii/varchar, blob/bytes,
            // varint, decimal, duration — plus every composite.
            _ => &[],
        }
    }
}

// Issue #3612 (R8-F2): the CARDINALITY of the undecodable-key diagnostic this
// module raises via `opaque_out` — one line per column per ROW, from the caller,
// carrying the count. Declared here, and sited beside `cell_path_key_tests.rs`,
// because `complex_column.rs` (the emitter) is thousands of lines over the
// file-size ratchet and cannot take a new line (epic #1116). See that file's
// header.
#[cfg(test)]
#[path = "warn_cardinality_tests.rs"]
mod warn_cardinality_tests;
