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
//! 2. **Fixed-width keys are validated for EXACT width.**
//!    `parse_value_from_raw_bytes` rejects only UNDER-width input (`< N`) because
//!    its other callers hand it element bytes already bounded by an outer length
//!    prefix, so trailing bytes cannot occur. A CellPath has no such outer bound
//!    here: the whole slice is the key, so an over-long slice is corruption and
//!    must not be decoded from its prefix. Cassandra agrees, and is the authority
//!    (5.0.8, `org.apache.cassandra.serializers.*.validate`): `Int32Serializer`
//!    `size != 4`, `LongSerializer`/`TimestampSerializer`/`DoubleSerializer`/
//!    `TimeSerializer` `!= 8`, `FloatSerializer`/`SimpleDateSerializer` `!= 4`,
//!    `UUIDSerializer` `!= 16`, `ShortSerializer` `!= 2`, `ByteSerializer`
//!    `!= 1`, `BooleanSerializer` `size > 1` — all raising `MarshalException`.
//!    (Cassandra additionally admits a ZERO-length buffer for most of these; a
//!    zero-length path cannot reach here because the caller only decodes a
//!    NON-EMPTY `path_bytes`.)
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
    /// `frozen<…>`-spelled type, so a MULTICELL map key presents EXACTLY as the
    /// FROZEN spelling of the same map presents it.
    ///
    /// MEASURED, not assumed (issue #3612): reading the committed Cassandra
    /// fixture `test_udt_collision.udt_collide`, the frozen control
    /// `fcm frozen<map<frozen<collide>, int>>` yields a BARE `Value::Udt` key,
    /// because [`super::cell_value_complex`] prefers the on-disk marshal element
    /// type (`UserType(…)`, issue #1340) over the schema's `frozen<collide>` and
    /// so never enters the `frozen<` arm. The multicell sibling
    /// `cm map<frozen<collide>, int>` resolves its key type from
    /// `column.data_type`, which DOES carry the `frozen<…>` (or `FrozenType(…)`)
    /// spelling, so delegation would wrap where the control does not — leaving
    /// the two spellings of one map with different internal shapes.
    ///
    /// The wrapper carries no information at this position: CQL requires a
    /// composite map key to be frozen (`map<collide, int>` is rejected;
    /// `map<frozen<collide>, int>` is the only legal spelling), so `Frozen` here
    /// is constant-true. Every render surface already unwraps it transparently
    /// (`ValueFormatter::format_value`, the CLI JSON writer, Arrow's
    /// `unwrap_frozen_value`, and both bindings), so this normalizes the internal
    /// shape rather than changing any rendered output.
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
            "boolean" | "tinyint" | "byte" => &[1],
            "smallint" | "short" => &[2],
            "int" | "float" | "date" => &[4],
            "bigint" | "counter" | "double" | "timestamp" | "time" => &[8],
            "uuid" | "timeuuid" => &[16],
            // Cassandra `InetAddressSerializer.validate` delegates to
            // `InetAddress.getByAddress`, which accepts ONLY a 4- or 16-byte
            // address.
            "inet" => &[4, 16],
            // Variable-width by definition: text/ascii/varchar, blob/bytes,
            // varint, decimal, duration — plus every composite.
            _ => &[],
        }
    }
}
