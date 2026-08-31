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
//!    * **`InetAddressSerializer`** THROWS on empty, then delegates to
//!      `InetAddress.getByAddress` → 4 or 16 only.
//!
//!    Encoding the `0` allowances is a FIDELITY fix with no behaviour change, and
//!    both halves of that are worth stating. No behaviour change: the sole caller
//!    only decodes a NON-EMPTY `path_bytes`, so a 0-byte slice never reaches here;
//!    and even if it did, `parse_value_from_raw_bytes` refuses a 0-byte
//!    fixed-width value on its own. Worth doing anyway: a table that disagrees
//!    with Cassandra is a false rejection waiting for the day someone moves the
//!    call site, and "correct only because the caller filters" is a coupling one
//!    file away from being silently broken.
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
//! `map<int,int>` against its 26-byte on-disk UDT key returned exit 0 and
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
//!   the opaque `Value::Blob` the shared decoder produced, with a `warn!` naming
//!   the column and the declared type, so the row stays whole and the gap is
//!   visible in the log rather than in a missing column.
//!
//! The swallow itself is a PRE-EXISTING defect of row assembly, not of this
//! module, and is tracked separately (see the PR for #3612).
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
//! | inet (widths) | same table, `[4, 16]` — empty THROWS in Cassandra |
//! | frozen list (`parse_frozen_list_value_raw`) | reported offset, was DISCARDED — now checked |
//! | frozen set (`parse_frozen_set_value_raw`) | reported offset, was DISCARDED — now checked |
//! | frozen map (`parse_frozen_map_value_raw`) | reported offset, was DISCARDED — now checked |
//! | tuple (`parse_tuple_elements_raw`) | reported `&mut offset`, was DISCARDED — now checked |
//! | UDT, marshal + registry-bare-name (`parse_raw_type_value` → `parse_udt_value`) | reported offset, was DISCARDED — now checked |
//! | `frozen<T>` / `FrozenType(T)` | recursion; exactness is the inner arm's |
//! | duration | measured from its own three-VInt framing (the decoder ignores the remainder) |
//! | unknown type → opaque `Value::Blob` | whole slice by construction; also `warn!`s |
//!
//! ## The ONE residual, stated rather than left to be rediscovered
//! A NESTED element of a collection or tuple is bounded by its OWN `[i32 BE len]`
//! prefix, and the element decode then uses `parse_value_from_raw_bytes`, whose
//! fixed-width guards are `data.len() < N`, not `!= N`. So for e.g.
//! `frozen<list<int>>` the byte strings `[count=1][len=4][4B]` and
//! `[count=1][len=5][5B]` BOTH satisfy the top-level consumption rule (each
//! consumes its own full length) and decode to the same `List([Integer(x)])`.
//! Making that exact is a one-token change per fixed-width arm in
//! `parse_value_from_raw_bytes`, which every value read in this crate goes
//! through, and that file is already over the file-size threshold so it cannot
//! grow — out of #3612's scope, and deliberately NOT patched with a second
//! framing walk here: a call-site validator that must know about every decoder
//! is precisely the shape this module replaced.
//!
//! # Presenting the key EXACTLY as the FROZEN spelling does (issue #3612, R3-F2)
//!
//! Two spellings of one logical map must present the same key value, or `Value`'s
//! `PartialEq`/`Hash` tell them apart on the public Rust surface. That rule, its
//! per-type measurement, and the reason it is type-dependent live on
//! `frozen_presentation_wrapper`. Read that before changing the strip: an earlier
//! revision of this header claimed the key "presents EXACTLY as the FROZEN
//! spelling" while stripping UNCONDITIONALLY, which was true for UDT keys and
//! FALSE for collection keys — the gap that survived two review rounds because
//! the parity tests covered only UDTs.
//!
//! # The asymmetry across the three cell-path/key readers (issue #3612)
//!
//! For a key type CQLite models nowhere, all three readers agree — each serves
//! an opaque `Value::Blob`: this multicell path (plus a `warn!`), the frozen-map
//! reader (`parse_frozen_map_value`, via `read_frozen_element`), and the
//! multi-generation merge reader (`read_assembly`'s `key_is_opaque_composite`,
//! tracked by issue #2339). There is deliberately NO availability difference.
//!
//! They DIVERGE on CORRUPTION: only this path validates fixed widths and full
//! consumption, so a multicell key with a wrong width or trailing bytes is
//! REFUSED here (and, until the row-assembly swallow is fixed, that manifests as
//! a truncated row) while the frozen spelling of the same map would decode it
//! from a prefix. That asymmetry is intentional — this is the one site where the
//! whole slice is known to BE the key — but it is not symmetric, and widening it
//! to the frozen/set routes is out of #3612's scope.

use super::*;

impl V5CompressedLegacyParser {
    /// Parse a MULTICELL map's cell-path key.
    ///
    /// `data` is the bare serialized key (the CellPath's VInt length prefix is
    /// already stripped by the caller). `type_str` is the map's declared KEY type
    /// in whatever spelling the authoritative source provided — a CQL short form
    /// from the schema (`frozen<collide>`, `int`) or a Cassandra marshal form from
    /// `Statistics.db` (`org.apache.cassandra.db.marshal.UserType(…)`) — and is
    /// forwarded WITH ITS CASE INTACT (see the module header).
    pub(super) fn parse_cell_path_key(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
    ) -> Result<Value> {
        let allowed = Self::cell_path_key_allowed_widths(type_str);
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
        // ONE decode, which also REPORTS what it consumed (see
        // `decode_reporting_consumption`).
        let (decoded, consumed) =
            self.decode_reporting_consumption(data, type_str, column_name, 0)?;
        // ORDER IS LOAD-BEARING: strip BEFORE the opaque-value test below. A
        // `frozen<absent_udt>` key can come back as `Frozen(Blob)`, so only after
        // the strip does `matches!(decoded, Value::Blob(_))` see it. Reordering
        // these two lines silently disables the diagnostic for every
        // frozen-spelled undecodable key — which is the common spelling, since a
        // composite map key must be frozen.
        let decoded = Self::unwrap_frozen_cell_path_key(decoded);
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
        // decoder handed back the raw bytes. Report it LOUDLY but do NOT return
        // `Err`: an `Err` here is swallowed by row assembly into a silently
        // truncated row (see the module header's error-budget rule), which is
        // more destructive than the opaque value, and Cassandra itself reads
        // such a key without complaint. The `warn!` distinguishes this from a
        // key DECLARED `blob`, which is a correct decode and stays silent —
        // which is the misleading-diagnostic half of issue #3612.
        if matches!(decoded, Value::Blob(_)) && !self.cell_path_key_declares_blob(type_str) {
            tracing::warn!(
                target: "cqlite::decode",
                column = column_name,
                declared_type = type_str,
                bytes = data.len(),
                "multicell map key type is not one this reader can decode; the key \
                 is surfaced as opaque bytes (issue #3612). Check that the schema \
                 (or the on-disk SerializationHeader) resolves it, e.g. that a UDT \
                 named here is registered."
            );
        }
        // PRESENTATION, last: re-apply the `Value::Frozen` wrapper exactly where the
        // FROZEN spelling of the same map would carry one (see
        // `frozen_presentation_wrapper`). Deliberately after the opaque-value test
        // above, which must see the PEELED value.
        Ok(Self::frozen_presentation_wrapper(decoded, type_str))
    }

    /// Re-apply the `Value::Frozen` wrapper iff the FROZEN spelling of the same map
    /// would present one, so the two spellings of one map compare EQUAL on the
    /// public Rust surface (`Value`'s `PartialEq`/`Hash` distinguish
    /// `Frozen(Set(..))` from `Set(..)`).
    ///
    /// # The rule is TYPE-DEPENDENT, and that is not a bug — it is what parity
    /// requires. Measured, per key type (issue #3612, R3-F2):
    ///
    /// | key type the frozen side is handed | frozen side | multicell must present |
    /// |---|---|---|
    /// | `UserType(..)` marshal (a UDT)     | `Udt`          | `Udt` (BARE) |
    /// | `frozen<set<int>>`                 | `Frozen(Set)`  | `Frozen(Set)` |
    /// | `frozen<list<int>>`                | `Frozen(List)` | `Frozen(List)` |
    /// | `frozen<map<text,int>>`            | `Frozen(Map)`  | `Frozen(Map)` |
    /// | `tuple<text,int>`                  | `Tuple`        | `Tuple` (BARE) |
    /// | `frozen<tuple<text,int>>`          | `Frozen(Tuple)`| `Frozen(Tuple)` |
    ///
    /// # Why the asymmetry exists, from CASSANDRA'S OWN metadata
    /// The committed fixture's `Statistics.db` (`nb-1-big-Statistics.db.txt`,
    /// `test_udt_collision.udt_collide`) shows Cassandra writing DIFFERENT strings
    /// for the two spellings of the same logical map:
    ///
    /// * MULTICELL `cm map<frozen<collide>, int>` →
    ///   `MapType(FrozenType(UserType(..)),Int32Type)` — the key IS
    ///   `FrozenType`-wrapped, because a multicell map's key must be explicitly
    ///   frozen.
    /// * FROZEN `fcm frozen<map<frozen<collide>, int>>` →
    ///   `FrozenType(MapType(UserType(..),Int32Type))` — the key is NOT wrapped,
    ///   because everything inside a frozen collection is already frozen so
    ///   Cassandra omits the marker (same for `fs`:
    ///   `FrozenType(SetType(UserType(..)))`).
    ///
    /// So the two sides are handed genuinely different type strings by Cassandra,
    /// and the wrapper cannot be equalised by threading metadata — one side has to
    /// normalise. For a UDT the frozen side ends up BARE (its string carries no
    /// `FrozenType`, and `prefer_udt_marshal_element` prefers that marshal string
    /// over the schema spelling anyway, issue #1340), so the multicell side strips.
    /// For a COLLECTION key the frozen side receives no UDT-bearing marshal, so
    /// `prefer_udt_marshal_element` falls back to the SCHEMA short form
    /// `frozen<set<int>>` — which IS frozen-spelled — and wraps. The multicell side
    /// therefore must wrap too.
    ///
    /// # A note recorded rather than acted on
    /// The collection wrapper on the frozen side traces to CQLite's schema-spelling
    /// FALLBACK, not to Cassandra's metadata (whose frozen-collection header omits
    /// the marker). By that authority BARE is arguably right for both sides. Making
    /// that call means changing the frozen read path, which is a cross-path parity
    /// decision for the lead, not this issue's — so this matches the frozen side as
    /// it behaves TODAY and the question is reported upward instead.
    ///
    /// Scalars are deliberately excluded: `frozen<blob>` and friends are not legal
    /// CQL for a map key, so there is no frozen-side behaviour to match, and
    /// wrapping them would blind the opaque-value diagnostic above.
    fn frozen_presentation_wrapper(value: Value, type_str: &str) -> Value {
        let composite = matches!(
            value,
            Value::List(_) | Value::Set(_) | Value::Map(_) | Value::Tuple(_)
        );
        if composite && Self::is_frozen_spelled(type_str) {
            return Value::Frozen(Box::new(value));
        }
        value
    }

    /// Whether `type_str` is spelled `frozen<..>` / `FrozenType(..)` — the exact
    /// predicate `parse_value_from_raw_bytes`'s frozen arm dispatches on, so this
    /// cannot form a second opinion about what the frozen side would have done.
    fn is_frozen_spelled(type_str: &str) -> bool {
        let lower = type_str.trim().trim_matches('\'').to_ascii_lowercase();
        lower.starts_with("frozen<")
            || lower.starts_with("org.apache.cassandra.db.marshal.frozentype(")
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
    /// offset and `parse_value_from_raw_bytes` merely DISCARDS it (`let (val, _)`),
    /// so the correct shape is to keep that offset instead of re-deriving it.
    ///
    /// # The `None` arms are exact, not unchecked
    /// `None` is returned only where the arm's contract IS "the entire slice is the
    /// value": text/ascii/varchar (validated UTF-8 over all of `data`), blob/bytes,
    /// varint and inet (each borrows the whole slice), and decimal (scale from
    /// `data[..4]`, unscaled from `data[4..]`). Fixed-width scalars also return
    /// `None` because the caller's width table has ALREADY pinned `data.len()` to
    /// the exact width — a stronger check than a consumption compare. The opaque
    /// `Value::Blob` default likewise borrows all of `data`.
    ///
    /// # Dispatch must mirror `parse_value_from_raw_bytes`
    /// The guards below are the same predicates, in the same ORDER (frozen before
    /// UDT, because `is_udt_type` is a substring match that also matches
    /// `FrozenType(UserType(..))`). `cell_path_key_tests` asserts that every
    /// composite spelling reports `Some`, so an arm added there and not here shows
    /// up as a failing test rather than as a silent prefix decode.
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
        const M: &str = "org.apache.cassandra.db.marshal.";

        // frozen<T> / FrozenType(T): recurse on the inner type. Deliberately
        // BEFORE the UDT arm, and deliberately without re-wrapping in
        // `Value::Frozen` (see `unwrap_frozen_cell_path_key`).
        if lower.starts_with("frozen<") || lower.starts_with(&format!("{M}frozentype(")) {
            let inner = self.extract_frozen_inner_type(type_str)?;
            return self.decode_reporting_consumption(data, &inner, column_name, depth + 1);
        }

        if lower.starts_with("list<") || lower.starts_with(&format!("{M}listtype(")) {
            let elem = self.extract_collection_element_type(type_str, "list")?;
            let (val, off) =
                self.parse_frozen_list_value_raw(data, 0, &elem, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("set<") || lower.starts_with(&format!("{M}settype(")) {
            let elem = self.extract_collection_element_type(type_str, "set")?;
            let (val, off) =
                self.parse_frozen_set_value_raw(data, 0, &elem, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("map<") || lower.starts_with(&format!("{M}maptype(")) {
            let (k, v) = self.extract_map_types(type_str)?;
            let (val, off) =
                self.parse_frozen_map_value_raw(data, 0, &k, &v, column_name, depth + 1)?;
            return Ok((val, Some(off)));
        }
        if lower.starts_with("tuple<") || lower.starts_with(&format!("{M}tupletype(")) {
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
            || self
                .udt_registry
                .as_ref()
                .is_some_and(|r| r.get_udt_qualified(&self.keyspace, &lower).is_some())
        {
            let (val, off) = self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
            return Ok((val, Some(off)));
        }
        // `duration` is three consecutive signed VInts and the decoder ignores
        // whatever follows the third, so its consumption is measured here from the
        // same framing (`parse_vint` reports the remaining slice). Framing only —
        // the VALUE still comes from the one shared decode below.
        if lower == "duration" || lower == format!("{M}durationtype") {
            let value = self.parse_value_from_raw_bytes(data, type_str, column_name, depth)?;
            let mut pos = 0usize;
            for _ in 0..3 {
                let (rest, _) = parse_vint(&data[pos..]).map_err(|e| {
                    Error::corruption(format!("Map key '{}': duration VInt: {:?}", column_name, e))
                })?;
                pos = data.len() - rest.len();
            }
            return Ok((value, Some(pos)));
        }
        // Everything else consumes the whole slice by construction (see above).
        Ok((
            self.parse_value_from_raw_bytes(data, type_str, column_name, depth)?,
            None,
        ))
    }

    /// Whether `type_str` DECLARES a blob key, i.e. whether `Value::Blob` is the
    /// CORRECT decode result rather than the shared opaque default.
    ///
    /// The distinction cannot be made from the RESULT — a declared `blob` key and
    /// an undecodable key both yield `Value::Blob` — so it is made from the
    /// DECLARED type. `frozen<…>`/`FrozenType(…)` is peeled first: CQL does not
    /// permit `frozen<blob>` as a map key, but a blob is still a blob under any
    /// spelling and must not be misdiagnosed as undecoded.
    fn cell_path_key_declares_blob(&self, type_str: &str) -> bool {
        let mut t = type_str.trim().to_string();
        // Peel via the ONE existing frozen-unwrapper (`extract_frozen_inner_type`,
        // which accepts `frozen<T>` and `FrozenType(T)` case-insensitively), so
        // this cannot form a second opinion about what "frozen" means. Bounded by
        // the decoder's own nesting limit; `Err` simply means "not frozen".
        for _ in 0..MAX_TYPE_NESTING_DEPTH {
            match self.extract_frozen_inner_type(&t) {
                Ok(inner) => t = inner.trim().to_string(),
                Err(_) => break,
            }
        }
        // CQL spells a CUSTOM type as a SINGLE-QUOTED marshal class name
        // (`'org.apache.cassandra.db.marshal.BytesType'`); the quotes would
        // defeat the `ends_with` suffix match below, so strip them first.
        let t = t.trim_matches('\'').trim().to_string();
        if t.contains("org.apache.cassandra.db.marshal.") {
            return Self::primitive_marshal_to_cql_short(&t) == Some("blob");
        }
        // A BARE, unqualified marshal name also occurs (a hand-written schema, or
        // a marshal string whose package prefix was already stripped), so ask the
        // same normalizer with the canonical prefix restored before falling back
        // to the CQL short forms. Both routes reach ONE table, so a blob spelling
        // cannot be recognised by one and rejected by the other.
        if Self::primitive_marshal_to_cql_short(&format!("org.apache.cassandra.db.marshal.{}", t))
            == Some("blob")
        {
            return true;
        }
        matches!(t.to_ascii_lowercase().as_str(), "blob" | "bytes")
    }

    /// Drop the `Value::Frozen` wrapper the structural decoder adds for a
    /// `frozen<…>`-spelled type, so the opaque-value test and the consumption rule
    /// see the real value. The wrapper is re-applied for PRESENTATION, only where
    /// the frozen spelling would carry one — see `frozen_presentation_wrapper`,
    /// which owns the parity rule and its measurement.
    fn unwrap_frozen_cell_path_key(mut value: Value) -> Value {
        while let Value::Frozen(inner) = value {
            value = *inner;
        }
        value
    }

    /// The EXACT byte width a fixed-width cell-path key must have, or `None` for
    /// a variable-width / composite type (where the whole slice is consumed and
    /// no width invariant applies).
    ///
    /// Accepts both spellings of the type. A marshal form is normalized through
    /// the CASE-SENSITIVE [`Self::primitive_marshal_to_cql_short`], which returns
    /// `None` for anything parameterised (`UserType(…)`, `TupleType(…)`,
    /// `FrozenType(…)`, collections), so a composite key is never width-checked.
    /// The byte widths a fixed-width cell-path key MAY have. Empty = variable
    /// width (no invariant). Two entries only for `inet`, which Cassandra's
    /// `InetAddressSerializer.validate` accepts at 4 (IPv4) or 16 (IPv6) bytes
    /// and nothing else — a single-width table cannot express that, which is why
    /// this returns a slice rather than one `usize`.
    fn cell_path_key_allowed_widths(type_str: &str) -> &'static [usize] {
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
            // `InetAddressSerializer.validate` THROWS on empty and otherwise
            // delegates to `InetAddress.getByAddress`, which takes a 4- or
            // 16-byte address and nothing else.
            "inet" => &[4, 16],
            // Variable-width by definition: text/ascii/varchar, blob/bytes,
            // varint, decimal, duration — plus every composite.
            _ => &[],
        }
    }
}
