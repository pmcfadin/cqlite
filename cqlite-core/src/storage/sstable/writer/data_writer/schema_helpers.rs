//! Schema/UDT helpers: complex-column detection, UDT field resolution, CQL type rendering, and bare-UDT marshal normalization.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers re-exported from
//! `data_writer/mod.rs`. No emitted bytes change.

use super::*;

/// Returns true if the column type is a non-frozen collection (complex column).
///
/// Complex columns are stored as multiple cells with cell paths, unlike
/// frozen collections which are stored as a single cell with blob value.
/// Matches the reader logic in `row_decoder.rs`.
pub(crate) fn is_complex_column(data_type: &str) -> bool {
    #[cfg(test)] // #1674 R3: count calls to prove O(C)/writer, never per row
    super::column_cache::is_complex_scope::record();
    let dt = data_type.to_lowercase();

    // Frozen collections are NOT complex (they're single-cell frozen types)
    if dt.starts_with("frozen<") || dt.starts_with("org.apache.cassandra.db.marshal.frozentype(") {
        return false;
    }

    // CQL-style collection types
    if dt.starts_with("list<") || dt.starts_with("set<") || dt.starts_with("map<") {
        return true;
    }

    // Cassandra internal collection types
    if dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
        || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
    {
        return true;
    }

    // Issue #927: a TOP-LEVEL non-frozen UDT is a first-class multi-cell complex
    // column (each field is a cell keyed by its 2-byte signed-short field index).
    // Frozen UDTs were excluded above; a UDT nested in a collection is matched by
    // its outer list/set/map branch. Bare CQL UDT names (e.g. `address`) are NOT
    // detected here — the writer holds only a `&TableSchema`, no `UdtRegistry`
    // (issue #927 item 4). Compaction inputs always carry the full `UserType(...)`.
    if is_udt_marshal(&dt) {
        return true;
    }

    false
}

/// True iff `data_type` is a TOP-LEVEL non-frozen `UserType(...)` marshal string
/// (issue #927). `dt_lower` is expected already lowercased by the caller.
pub(crate) fn is_udt_marshal(dt_lower: &str) -> bool {
    dt_lower.starts_with("org.apache.cassandra.db.marshal.usertype(")
}

/// Total-order comparator for two complex-column cell paths (issue #927, parity
/// Cassandra `d14c96b8`). UDT field-index paths are 2-byte **signed** `ShortType`
/// values, so a field index in `[32768, 65535]` (negative as `i16`) must sort
/// BEFORE the positive indices — a plain lexicographic byte compare would order
/// it last. Non-UDT paths (collections) keep lexicographic byte ordering.
pub(crate) fn compare_cell_paths(a: &[u8], b: &[u8], is_udt: bool) -> std::cmp::Ordering {
    if is_udt && a.len() == 2 && b.len() == 2 {
        let ai = i16::from_be_bytes([a[0], a[1]]);
        let bi = i16::from_be_bytes([b[0], b[1]]);
        return ai.cmp(&bi);
    }
    a.cmp(b)
}

/// Parse the DECLARED field order (names only) from a top-level `UserType(...)`
/// marshal string (issue #927). Used to map a whole-`Value::Udt` literal's fields
/// to their 2-byte signed-short cell-path index by NAME, never by literal
/// position (the literal may be sparse / out of order).
pub(crate) fn udt_declared_field_names(data_type: &str) -> Result<Vec<String>> {
    let start_marker = "org.apache.cassandra.db.marshal.UserType(";
    let type_lower = data_type.to_lowercase();
    let start_idx = type_lower
        .find(&start_marker.to_lowercase())
        .ok_or_else(|| Error::InvalidInput(format!("Not a UserType: {}", data_type)))?;
    let inner_start = start_idx + start_marker.len();
    let mut paren_depth = 1;
    let mut end_idx = inner_start;
    let chars: Vec<char> = data_type[inner_start..].chars().collect();
    for (i, c) in chars.iter().enumerate() {
        match c {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    end_idx = inner_start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    if paren_depth != 0 {
        return Err(Error::InvalidInput(format!(
            "Unbalanced parentheses in UserType: {}",
            data_type
        )));
    }
    let inner = &data_type[inner_start..end_idx];
    // Split top-level args respecting nested parens.
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    for c in inner.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    // First two args are keyspace + hex name; the rest are `hexname:type`.
    let mut names = Vec::with_capacity(parts.len().saturating_sub(2));
    for field_def in parts.iter().skip(2) {
        let field_def = field_def.trim();
        if field_def.is_empty() {
            continue;
        }
        let colon_idx = field_def.find(':').ok_or_else(|| {
            Error::InvalidInput(format!(
                "Invalid UDT field definition (missing colon): {}",
                field_def
            ))
        })?;
        let name_bytes = hex::decode(&field_def[..colon_idx]).map_err(|e| {
            Error::InvalidInput(format!(
                "Invalid hex-encoded UDT field name '{}': {}",
                &field_def[..colon_idx],
                e
            ))
        })?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| Error::InvalidInput(format!("Invalid UTF-8 in UDT field name: {}", e)))?;
        names.push(name);
    }
    Ok(names)
}

/// Render a [`CqlType`] back to a CQL type string that
/// [`cql_type_to_marshal_type`] can convert to a Cassandra marshal type.
///
/// Used by [`render_udt_marshal`] to emit each UDT field's marshal type. Only
/// the shapes that the string-based converter understands are produced
/// (primitives, `list/set/map/frozen/tuple` wrappers). A nested bare UDT
/// reference (`CqlType::Udt`) has no marshal expansion here — it is rendered as
/// its bare name and will fall through `cql_type_to_marshal_type` to
/// `BytesType`, matching the documented "top-level bare UDT only" scope of
/// issue #929.
pub(crate) fn cql_type_to_cql_string(ty: &CqlType) -> String {
    match ty {
        CqlType::Boolean => "boolean".to_string(),
        CqlType::TinyInt => "tinyint".to_string(),
        CqlType::SmallInt => "smallint".to_string(),
        CqlType::Int => "int".to_string(),
        CqlType::BigInt => "bigint".to_string(),
        CqlType::Counter => "counter".to_string(),
        CqlType::Float => "float".to_string(),
        CqlType::Double => "double".to_string(),
        CqlType::Decimal => "decimal".to_string(),
        CqlType::Text => "text".to_string(),
        CqlType::Ascii => "ascii".to_string(),
        CqlType::Varchar => "varchar".to_string(),
        CqlType::Blob => "blob".to_string(),
        CqlType::Timestamp => "timestamp".to_string(),
        CqlType::Date => "date".to_string(),
        CqlType::Time => "time".to_string(),
        CqlType::Uuid => "uuid".to_string(),
        CqlType::TimeUuid => "timeuuid".to_string(),
        CqlType::Inet => "inet".to_string(),
        CqlType::Duration => "duration".to_string(),
        CqlType::Varint => "varint".to_string(),
        CqlType::List(inner) => format!("list<{}>", cql_type_to_cql_string(inner)),
        CqlType::Set(inner) => format!("set<{}>", cql_type_to_cql_string(inner)),
        CqlType::Map(k, v) => format!(
            "map<{},{}>",
            cql_type_to_cql_string(k),
            cql_type_to_cql_string(v)
        ),
        CqlType::Tuple(fields) => {
            let inner: Vec<String> = fields.iter().map(cql_type_to_cql_string).collect();
            format!("tuple<{}>", inner.join(","))
        }
        CqlType::Frozen(inner) => format!("frozen<{}>", cql_type_to_cql_string(inner)),
        // #4114 is a READ-path issue; this renderer feeds `cql_type_to_marshal_type`,
        // which has no vector arm, so a vector field falls through there exactly as a
        // nested bare UDT does. The spelling is emitted faithfully (dimension
        // included) rather than dropped.
        CqlType::Vector(e, n) => format!("vector<{}, {n}>", cql_type_to_cql_string(e)),
        // Bare UDT reference — emit the name; out of scope for marshal expansion
        // (issue #929 covers top-level bare UDTs only).
        CqlType::Udt(name, _) => name.clone(),
        CqlType::Custom(name) => name.clone(),
    }
}

/// True if `name` (already lowercased) is a native CQL primitive type keyword.
/// Used to keep [`resolve_bare_udt_marshal`] from rewriting a real primitive
/// column even if a same-named UDT is registered (an illegal collision in
/// Cassandra, guarded against defensively). Mirrors the primitive arms of
/// `cql_type_to_marshal_type`.
pub(crate) fn is_cql_primitive_name(lower: &str) -> bool {
    matches!(
        lower,
        "text"
            | "varchar"
            | "int"
            | "bigint"
            | "smallint"
            | "tinyint"
            | "float"
            | "double"
            | "boolean"
            | "blob"
            | "uuid"
            | "timeuuid"
            | "timestamp"
            | "date"
            | "time"
            | "duration"
            | "inet"
            | "ascii"
            | "decimal"
            | "varint"
            | "counter"
    )
}

/// Render a [`UdtTypeDef`] as a TOP-LEVEL non-frozen `UserType(...)` marshal
/// string (issue #929) in exactly the shape [`udt_declared_field_names`] parses
/// and [`is_udt_marshal`] recognizes:
///
/// ```text
/// org.apache.cassandra.db.marshal.UserType(<keyspace>,<hex-name>,<hex-fieldname>:<field-marshal-type>,...)
/// ```
///
/// The keyspace appears as plain text; the UDT name and each field name are
/// lowercase-hex of their UTF-8 bytes. Each field's marshal type comes from the
/// canonical CQL→marshal string converter ([`cql_type_to_marshal_type`]).
///
/// SCOPE / KNOWN LIMITATION: a field that is itself a UDT (or a
/// collection/tuple/frozen *containing* a UDT) renders to `BytesType`, NOT the
/// nested `UserType(...)`. This is deliberate: the direct-write value path
/// ([`serialize_value`] for `Value::Udt`) infers a nested UDT's schema from the
/// literal's own field order and does not pad missing fields, so it cannot yet
/// guarantee declared-order serialization of a sparse / out-of-order nested
/// literal. Advertising the expanded nested `UserType` in the header while the
/// value bytes follow literal order would make the SSTable inconsistent (a
/// reader decoding in declared order would mis-read the fields). Keeping nested
/// UDT fields as `BytesType` leaves the header and the (blob) value bytes
/// self-consistent. Schema-aware nested-UDT value serialization is tracked as a
/// follow-up. Issue #929 targets TOP-LEVEL bare-name non-frozen UDT columns
/// (whose own fields are primitives / collections of primitives).
pub(crate) fn render_udt_marshal(udt: &UdtTypeDef) -> String {
    let prefix = "org.apache.cassandra.db.marshal.UserType(";
    let mut out = String::from(prefix);
    out.push_str(&udt.keyspace);
    out.push(',');
    out.push_str(&hex::encode(udt.name.as_bytes()));
    for field in &udt.fields {
        out.push(',');
        out.push_str(&hex::encode(field.name.as_bytes()));
        out.push(':');
        let field_cql = cql_type_to_cql_string(&field.field_type);
        out.push_str(
            &crate::storage::sstable::writer::stats_writer::cql_type_to_marshal_type(&field_cql),
        );
    }
    out.push(')');
    out
}

/// Render a [`UdtTypeDef`] as a `UserType(...)` marshal string, EXPANDING nested
/// UDT field references into their own `UserType(...)` marshals via `registry`
/// (issue #1020). This is the registry-aware counterpart of [`render_udt_marshal`]
/// (which renders a nested UDT field as `BytesType`); it is used for a
/// `frozen<udt>` regular column so the SerializationHeader matches Cassandra
/// byte-for-byte in shape and the compaction read path
/// (`decode_frozen_udt_from_header_type`) can structurally resolve every field —
/// including nested UDTs — from the on-disk header alone.
///
/// Field-type rule (Cassandra parity, verified against a 5.0.2 reference header):
/// a UDT field that is itself a UDT is spelled as the BARE `UserType(...)` even
/// when declared `frozen<udt>` — the `FrozenType(...)` wrapper is implied for an
/// inner UDT field and is NOT written. Non-UDT fields (primitives, collections of
/// primitives, frozen collections) keep the exact marshal produced by
/// [`cql_type_to_marshal_type`].
pub(crate) fn render_udt_marshal_recursive(
    udt: &UdtTypeDef,
    keyspace: &str,
    registry: &UdtRegistry,
) -> String {
    let prefix = "org.apache.cassandra.db.marshal.UserType(";
    let mut out = String::from(prefix);
    out.push_str(&udt.keyspace);
    out.push(',');
    out.push_str(&hex::encode(udt.name.as_bytes()));
    for field in &udt.fields {
        out.push(',');
        out.push_str(&hex::encode(field.name.as_bytes()));
        out.push(':');
        out.push_str(&render_field_marshal(&field.field_type, keyspace, registry));
    }
    out.push(')');
    out
}

/// Render a single UDT field's `CqlType` as a Cassandra marshal string,
/// expanding any UDT reference into `UserType(...)` via `registry` (issue #1020).
///
/// Field-type rule (Cassandra parity, roborev #1020 Finding 2):
///   * A `FrozenType(...)` wrapper is elided ONLY for a DIRECT UDT field
///     (`frozen<udt>` → bare `UserType(...)`); Cassandra implies frozen-ness for
///     an inner UDT field and does not write the wrapper.
///   * A `frozen<collection/tuple/frozen<...>>` KEEPS its `FrozenType(...)`
///     wrapper and its inner type is rendered RECURSIVELY, so a nested UDT
///     ELEMENT/VALUE expands to `UserType(...)` (e.g.
///     `frozen<list<frozen<address>>>` →
///     `FrozenType(ListType(UserType(...)))`). The previous implementation
///     dropped the wrapper whenever the inner TRANSITIVELY referenced a UDT,
///     mis-spelling such a field as a non-frozen collection and collapsing the
///     nested UDT to `BytesType`.
///   * `ListType/SetType/MapType/TupleType` are emitted structurally, recursing
///     into element/key/value/component types so nested UDTs always expand.
///   * Everything else (primitives) falls through to the canonical string
///     converter.
fn render_field_marshal(ty: &CqlType, keyspace: &str, registry: &UdtRegistry) -> String {
    let prefix = "org.apache.cassandra.db.marshal.";
    match ty {
        // DIRECT frozen UDT: drop the FrozenType wrapper, emit bare UserType.
        CqlType::Frozen(inner) if is_direct_udt(inner, keyspace, registry) => {
            render_field_marshal(inner, keyspace, registry)
        }
        // Frozen around a non-UDT (collection / tuple / nested frozen): KEEP the
        // FrozenType wrapper and recurse so nested UDT elements still expand.
        CqlType::Frozen(inner) => {
            format!(
                "{prefix}FrozenType({})",
                render_field_marshal(inner, keyspace, registry)
            )
        }
        CqlType::List(inner) => {
            format!(
                "{prefix}ListType({})",
                render_field_marshal(inner, keyspace, registry)
            )
        }
        CqlType::Set(inner) => {
            format!(
                "{prefix}SetType({})",
                render_field_marshal(inner, keyspace, registry)
            )
        }
        CqlType::Map(k, v) => {
            format!(
                "{prefix}MapType({},{})",
                render_field_marshal(k, keyspace, registry),
                render_field_marshal(v, keyspace, registry)
            )
        }
        CqlType::Tuple(fields) => {
            let components: Vec<String> = fields
                .iter()
                .map(|f| render_field_marshal(f, keyspace, registry))
                .collect();
            format!("{prefix}TupleType({})", components.join(","))
        }
        CqlType::Udt(name, inline_fields) => {
            render_udt_reference(name, inline_fields, keyspace, registry)
        }
        CqlType::Custom(name) => {
            let clean = name.strip_prefix("udt:").unwrap_or(name);
            if let Some(udt) = resolve_registered_udt(clean, keyspace, registry) {
                render_udt_marshal_recursive(udt, keyspace, registry)
            } else {
                let field_cql = cql_type_to_cql_string(ty);
                crate::storage::sstable::writer::stats_writer::cql_type_to_marshal_type(&field_cql)
            }
        }
        _ => {
            let field_cql = cql_type_to_cql_string(ty);
            crate::storage::sstable::writer::stats_writer::cql_type_to_marshal_type(&field_cql)
        }
    }
}

/// True iff `ty` is DIRECTLY a UDT reference — `CqlType::Udt`, or a
/// `CqlType::Custom` that carries the parser's `udt:` prefix or resolves to a
/// registered UDT in `keyspace`. A collection/tuple/frozen that merely CONTAINS a
/// UDT is NOT direct (its `FrozenType` wrapper must be preserved — roborev #1020
/// Finding 2).
fn is_direct_udt(ty: &CqlType, keyspace: &str, registry: &UdtRegistry) -> bool {
    match ty {
        CqlType::Udt(..) => true,
        CqlType::Custom(name) => {
            let clean = name.strip_prefix("udt:").unwrap_or(name);
            name.starts_with("udt:") || resolve_registered_udt(clean, keyspace, registry).is_some()
        }
        _ => false,
    }
}

/// Render a `CqlType::Udt(name, inline_fields)` reference as `UserType(...)`.
/// Prefers the registry definition (authoritative declared order); falls back to
/// the inline `(name, type)` pairs the parser captured when the name is not
/// registered.
fn render_udt_reference(
    name: &str,
    inline_fields: &[(String, CqlType)],
    keyspace: &str,
    registry: &UdtRegistry,
) -> String {
    if let Some(udt) = resolve_registered_udt(name, keyspace, registry) {
        return render_udt_marshal_recursive(udt, keyspace, registry);
    }
    // Fallback: build a transient definition from the inline pairs so a nested
    // UDT that is not separately registered still renders structurally (never a
    // silent BytesType, which would lose the type — issue #28). A qualified
    // `keyspace.udt` reference contributes its explicit keyspace + bare name to
    // the marshal (the `UserType(...)` carries an unqualified hex name —
    // roborev #1020 Finding 1).
    let (ref_keyspace, bare_name) = crate::schema::split_qualified_udt(name, keyspace);
    let mut out = String::from("org.apache.cassandra.db.marshal.UserType(");
    out.push_str(ref_keyspace);
    out.push(',');
    out.push_str(&hex::encode(bare_name.as_bytes()));
    for (fname, fty) in inline_fields {
        out.push(',');
        out.push_str(&hex::encode(fname.as_bytes()));
        out.push(':');
        out.push_str(&render_field_marshal(fty, keyspace, registry));
    }
    out.push(')');
    out
}

/// Resolve a UDT name in `registry` for `keyspace`: exact match first, then an
/// unambiguous case-insensitive match (unquoted CQL identifiers are
/// case-insensitive). Ambiguous case-insensitive matches resolve to `None`.
///
/// `name` may be KEYSPACE-QUALIFIED (`keyspace.udt`); the explicit keyspace wins
/// over `keyspace` when present (roborev #1020 Finding 1, via the shared
/// [`crate::schema::split_qualified_udt`], promoted here to the schema module in
/// issue #2807 so the read path shares it).
fn resolve_registered_udt<'a>(
    name: &str,
    keyspace: &str,
    registry: &'a UdtRegistry,
) -> Option<&'a UdtTypeDef> {
    let (lookup_keyspace, bare_name) = crate::schema::split_qualified_udt(name, keyspace);
    if let Some(udt) = registry.get_udt(lookup_keyspace, bare_name) {
        return Some(udt);
    }
    let ks_udts = registry.get_keyspace_udts(lookup_keyspace)?;
    let mut matches = ks_udts
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(bare_name));
    let first = matches.next()?;
    if matches.next().is_some() {
        return None; // ambiguous — refuse rather than guess.
    }
    Some(first.1)
}

/// Normalize a schema's column `data_type`s by resolving TOP-LEVEL bare CQL UDT
/// names (e.g. `person`) to their full `UserType(...)` marshal string via the
/// registry (issue #929).
///
/// A direct user write may give a column a bare UDT name as its `data_type`.
/// The writer holds only a `&TableSchema` and cannot otherwise resolve such a
/// name to its fields, so it would incorrectly write the column as a single
/// simple cell. Rewriting the `data_type` to the marshal form lets the existing
/// [`is_complex_column`] / `write_udt_complex_cells` path decompose it into
/// per-field cells unchanged.
///
/// Scope is strictly TOP-LEVEL bare names: a column whose `data_type` already
/// looks like a CQL type the converter understands (primitive, `frozen<...>`,
/// `list<...>`, `set<...>`, `map<...>`, `tuple<...>`) or an
/// `org.apache.cassandra.db.marshal.*` string is left untouched. With no
/// registry, or for an unregistered name, the column is left as-is (documented
/// fallback: single simple cell, no error).
pub(crate) fn normalize_schema_udts(schema: &mut TableSchema, registry: &UdtRegistry) {
    let keyspace = schema.keyspace.clone();
    for column in &mut schema.columns {
        if let Some(marshal) = resolve_bare_udt_marshal(&column.data_type, &keyspace, registry) {
            column.data_type = marshal;
        }
    }
}

/// If `data_type` is `frozen<bare_udt>` (a frozen SCALAR/NESTED UDT, e.g.
/// `frozen<person>` / `frozen<employee>`) whose inner name resolves to a
/// registered UDT, return the full `FrozenType(UserType(...))` marshal string
/// (nested UDT fields expanded recursively); otherwise `None` (issue #1020).
///
/// Scope is strictly `frozen<NAME>` where NAME is a single bare identifier: a
/// `frozen<list<...>>` / `frozen<map<...>>` (inner contains `<`) or
/// `frozen<frozen<...>>` is NOT a frozen-UDT and is left to the existing
/// collection-marshal converter. A NAME that is a CQL primitive or is not a
/// registered UDT also returns `None` (leave the column untouched).
fn resolve_frozen_udt_marshal(
    data_type: &str,
    keyspace: &str,
    registry: &UdtRegistry,
) -> Option<String> {
    let trimmed = data_type.trim();
    let lower = trimmed.to_lowercase();
    // CQL `frozen<...>` short form only (a marshal `FrozenType(...)` is already
    // authoritative and is handled by the marshal-string early return above).
    if !(lower.starts_with("frozen<") && lower.ends_with('>')) {
        return None;
    }
    let inner = trimmed["frozen<".len()..trimmed.len() - 1].trim();
    // The inner must be a single bare identifier: a collection / nested frozen /
    // tuple inner contains '<' (or commas/parens) and is NOT a frozen UDT.
    if inner.is_empty()
        || inner.contains('<')
        || inner.contains('>')
        || inner.contains(',')
        || inner.contains('(')
        || inner.contains(')')
    {
        return None;
    }
    if is_cql_primitive_name(&inner.to_lowercase()) {
        return None;
    }
    let udt = resolve_registered_udt(inner, keyspace, registry)?;
    Some(format!(
        "org.apache.cassandra.db.marshal.FrozenType({})",
        render_udt_marshal_recursive(udt, keyspace, registry)
    ))
}

/// If `data_type` is a FROZEN parameterized CQL type that CONTAINS a UDT
/// reference (e.g. `frozen<list<frozen<person>>>`,
/// `frozen<map<text, frozen<address>>>`, `frozen<tuple<int, frozen<person>>>`),
/// return its full marshal string with every nested UDT expanded to
/// `UserType(...)` via `registry`; otherwise `None` (issue #1020 column-level
/// dispatch).
///
/// Scope is strictly a TOP-LEVEL `frozen<...>` wrapper whose inner is a
/// parameterized collection/tuple (contains `<`) that transitively references a
/// registered UDT. This is deliberately narrow:
///   * A frozen<bare_udt> is already handled by [`resolve_frozen_udt_marshal`].
///   * A NON-frozen collection-of-UDT (`list<frozen<person>>`) is a MULTICELL
///     complex column with a different write path and is NOT rewritten here.
///   * A frozen collection of PRIMITIVES (`frozen<list<int>>`) references no UDT
///     and is left to the generic converter (byte-identical output).
///
/// The rendered marshal reuses [`render_field_marshal`], which applies the exact
/// Cassandra field-type rules (a DIRECT `frozen<udt>` element/value drops its
/// `FrozenType` wrapper to a bare `UserType(...)`; collection/tuple wrappers are
/// preserved and recursed). When no UDT is referenced, returns `None` so the
/// generic path keeps producing identical bytes.
fn resolve_frozen_parameterized_udt_marshal(
    data_type: &str,
    keyspace: &str,
    registry: &UdtRegistry,
) -> Option<String> {
    let trimmed = data_type.trim();
    let lower = trimmed.to_lowercase();
    // Only a CQL `frozen<...>` short form whose inner is itself parameterized.
    if !(lower.starts_with("frozen<") && lower.ends_with('>')) {
        return None;
    }
    let inner = trimmed["frozen<".len()..trimmed.len() - 1].trim();
    // Inner must be a parameterized collection/tuple (contains '<'); a bare
    // inner name is a frozen UDT already handled by `resolve_frozen_udt_marshal`.
    if !inner.contains('<') {
        return None;
    }
    // Parse to a structured CqlType so the renderer can expand nested UDTs. A
    // parse failure leaves the column to the generic path (no silent corruption).
    let parsed = CqlType::parse(trimmed).ok()?;
    // Only rewrite when the type actually references a UDT — otherwise the
    // generic converter already produces the correct (and byte-identical) marshal.
    if !cql_type_references_udt(&parsed, keyspace, registry) {
        return None;
    }
    Some(render_field_marshal(&parsed, keyspace, registry))
}

/// If `data_type` is a TOP-LEVEL bare CQL UDT name that resolves in `registry`,
/// return its rendered `UserType(...)` marshal string; otherwise `None`.
pub(crate) fn resolve_bare_udt_marshal(
    data_type: &str,
    keyspace: &str,
    registry: &UdtRegistry,
) -> Option<String> {
    let trimmed = data_type.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_lowercase();

    // Already a marshal string (incl. UserType / frozen marshal) — leave as-is.
    if lower.starts_with("org.apache.cassandra.db.marshal.") {
        return None;
    }
    // Issue #1020: a `frozen<bare_udt>` regular column (a frozen SCALAR/NESTED
    // UDT, e.g. `frozen<person>`) must resolve to the full
    // `FrozenType(UserType(...))` marshal — NOT be left bare. Without this the
    // SerializationHeader advertises `FrozenType(BytesType)`, the on-disk header
    // type the compaction reader relies on (issue #1080
    // `decode_frozen_udt_from_header_type`) loses the field structure, and the
    // frozen UDT cell is silently DROPPED during compaction (data loss). Frozen
    // is single-cell, so this only changes the advertised type, never the
    // multicell/complex write path (`is_complex_column` stays false for a
    // `FrozenType(...)` marshal). Frozen collections (`frozen<list<...>>`) and
    // frozen collections-of-UDT keep their existing marshal via the converter and
    // are intentionally left to the `<` fallthrough below.
    if let Some(marshal) = resolve_frozen_udt_marshal(trimmed, keyspace, registry) {
        return Some(marshal);
    }
    // Issue #1020 (roborev Finding, column-level dispatch): a FROZEN parameterized
    // column type that CONTAINS a UDT — e.g. `frozen<list<frozen<person>>>` or
    // `frozen<map<text, frozen<address>>>` — must advertise the nested UDT as a
    // full `UserType(...)` in the SerializationHeader (matching Cassandra 5.0.2),
    // NOT the blob-like `BytesType` the generic CQL→marshal converter would emit
    // (it has no registry). Without this both the header is wrong AND
    // `canonicalize_udt_value` skips the column (its marshal carries no
    // `UserType(` token), so the direct-write value bytes can disagree with the
    // declared type. Frozen collections-of-UDT are SINGLE-cell (frozen), so this
    // only widens the advertised marshal and the value canonicalization scope; it
    // never reaches the multicell/complex write path (`is_complex_column` returns
    // false for a `FrozenType(...)` marshal). Verified byte-for-byte against the
    // `udt_collections` fixture's Statistics.db `lp`/`ma` headers:
    //   FrozenType(ListType(UserType(...))) and
    //   FrozenType(MapType(UTF8Type,UserType(...))).
    if let Some(marshal) = resolve_frozen_parameterized_udt_marshal(trimmed, keyspace, registry) {
        return Some(marshal);
    }
    // Parameterized CQL types are not bare names — leave to the existing path.
    if lower.contains('<') {
        return None;
    }
    // A bare name must be a single identifier (no commas / parens).
    if trimmed.contains(',') || trimmed.contains('(') || trimmed.contains(')') {
        return None;
    }
    // A native CQL type name never denotes a UDT. Cassandra forbids creating a
    // UDT whose name shadows a primitive, but guard defensively: a registered
    // UDT colliding with a primitive keyword must not rewrite a real primitive
    // column into a UserType marshal (which would corrupt it).
    if is_cql_primitive_name(&lower) {
        return None;
    }

    // Resolve the bare name in the registry. Try an exact match first, then
    // fall back to a case-insensitive match within the keyspace: unquoted CQL
    // identifiers are case-insensitive, so a `CREATE TYPE Person` registered as
    // `Person` must still resolve a column declared `person` (roborev #1005).
    // The exact match wins when present, so a quoted (case-preserving) name is
    // never shadowed by a differently-cased sibling. The case-insensitive
    // fallback resolves ONLY when it is unambiguous (exactly one match), so two
    // differently-cased quoted names never resolve nondeterministically
    // (roborev #1011).
    let udt = match registry.get_udt(keyspace, trimmed) {
        Some(udt) => udt,
        None => {
            let ks_udts = registry.get_keyspace_udts(keyspace)?;
            let mut matches = ks_udts
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case(trimmed));
            let first = matches.next()?;
            if matches.next().is_some() {
                return None; // ambiguous — refuse rather than guess.
            }
            first.1
        }
    };

    // Only normalize UDTs we can FULLY represent: every field is a primitive or
    // a collection/tuple/frozen OF primitives. A field that is itself a UDT is
    // NOT yet supported on the direct-write value path (serialize_value infers a
    // nested UDT's schema from the literal's own order and cannot guarantee
    // declared-order serialization of a sparse/out-of-order nested literal). If
    // we normalized such a UDT, its nested field would be advertised as
    // `BytesType` while the value bytes follow literal order — losing the nested
    // type semantics. Leaving the column unnormalized keeps it a single,
    // self-consistent simple cell (documented fallback) until schema-aware
    // nested-UDT value serialization exists (roborev #1011).
    if udt
        .fields
        .iter()
        .any(|f| cql_type_references_udt(&f.field_type, keyspace, registry))
    {
        return None;
    }

    Some(render_udt_marshal(udt))
}

/// True if `ty` is, or transitively contains, a UDT reference (a nested
/// `UserType`). Used by [`resolve_bare_udt_marshal`] to skip normalizing a
/// top-level UDT whose fields include another UDT — see the note there.
///
/// A `CqlType::Custom` is a UDT reference when its name carries the parser's
/// `udt:` prefix OR resolves to a registered UDT in `keyspace` (the CQL parser
/// also emits unprefixed lowercase names like `address` for UDT references, so
/// the registry lookup is required — roborev #1013).
pub(crate) fn cql_type_references_udt(
    ty: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry,
) -> bool {
    match ty {
        CqlType::Udt(..) => true,
        CqlType::Custom(name) => {
            let clean = name.strip_prefix("udt:").unwrap_or(name);
            // `clean` may be KEYSPACE-QUALIFIED (`keyspace.udt`); resolve the
            // registry under the explicit keyspace when present, else `keyspace`
            // (roborev #1020 Finding 1, via `split_qualified_udt`).
            let (lookup_keyspace, bare_name) = crate::schema::split_qualified_udt(clean, keyspace);
            name.starts_with("udt:")
                || registry.get_udt(lookup_keyspace, bare_name).is_some()
                || registry
                    .get_keyspace_udts(lookup_keyspace)
                    .is_some_and(|m| m.keys().any(|k| k.eq_ignore_ascii_case(bare_name)))
        }
        CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
            cql_type_references_udt(inner, keyspace, registry)
        }
        CqlType::Map(k, v) => {
            cql_type_references_udt(k, keyspace, registry)
                || cql_type_references_udt(v, keyspace, registry)
        }
        CqlType::Tuple(fields) => fields
            .iter()
            .any(|f| cql_type_references_udt(f, keyspace, registry)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KS: &str = "test_ks";

    fn address_def() -> UdtTypeDef {
        UdtTypeDef::new(KS.to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
    }

    fn registry_with_address() -> UdtRegistry {
        let mut reg = UdtRegistry::new();
        reg.register_udt(address_def());
        reg
    }

    /// roborev #1020 Finding 2: a `frozen<address>` DIRECT UDT field drops the
    /// FrozenType wrapper and renders as a bare `UserType(...)`.
    #[test]
    fn direct_frozen_udt_field_renders_bare_user_type() {
        let reg = registry_with_address();
        let ty = CqlType::Frozen(Box::new(CqlType::Custom("address".to_string())));
        let m = render_field_marshal(&ty, KS, &reg);
        assert!(
            m.starts_with("org.apache.cassandra.db.marshal.UserType("),
            "direct frozen<udt> must be bare UserType, got {m}"
        );
        assert!(!m.contains("FrozenType("), "wrapper must be elided: {m}");
        assert!(m.contains("UTF8Type"), "address fields must expand: {m}");
    }

    /// roborev #1020 Finding 2: a `frozen<list<frozen<address>>>` field KEEPS its
    /// FrozenType wrapper and expands the nested UDT element to `UserType(...)` —
    /// it must NOT collapse to a non-frozen list nor fall back to `BytesType`.
    /// Asserts the EXACT marshal byte shape.
    #[test]
    fn frozen_list_of_frozen_udt_keeps_wrapper_and_expands_udt() {
        let reg = registry_with_address();
        let ty = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
            Box::new(CqlType::Custom("address".to_string())),
        )))));
        let m = render_field_marshal(&ty, KS, &reg);
        // street=737472656574, city=63697479, address=61646472657373.
        let expected = "org.apache.cassandra.db.marshal.FrozenType(\
             org.apache.cassandra.db.marshal.ListType(\
             org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,\
             737472656574:org.apache.cassandra.db.marshal.UTF8Type,\
             63697479:org.apache.cassandra.db.marshal.UTF8Type)))";
        assert_eq!(m, expected);
    }

    /// roborev #1020 Finding 2 (precise byte shape): the nested UDT inside a
    /// frozen collection expands to a full `UserType(...)` (never `BytesType`),
    /// and the outer `FrozenType`/`ListType` wrappers are preserved.
    #[test]
    fn frozen_map_to_frozen_udt_expands_value_udt() {
        let reg = registry_with_address();
        // frozen<map<text, frozen<address>>>
        let ty = CqlType::Frozen(Box::new(CqlType::Map(
            Box::new(CqlType::Text),
            Box::new(CqlType::Frozen(Box::new(CqlType::Custom(
                "address".to_string(),
            )))),
        )));
        let m = render_field_marshal(&ty, KS, &reg);
        assert!(
            m.starts_with(
                "org.apache.cassandra.db.marshal.FrozenType(\
                 org.apache.cassandra.db.marshal.MapType("
            ),
            "outer Frozen+Map wrappers must be preserved: {m}"
        );
        assert!(
            m.contains("org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,"),
            "nested address UDT must expand to UserType, not BytesType: {m}"
        );
        assert!(
            !m.contains("BytesType"),
            "nested UDT must never collapse to BytesType: {m}"
        );
    }

    /// roborev #1020 Finding 1: a KEYSPACE-QUALIFIED `frozen<test_ks.address>`
    /// direct UDT field resolves through the registry split and renders byte-for-
    /// byte identical to the unqualified `frozen<address>` form (bare
    /// `UserType(...)`, no `FrozenType` wrapper, no `BytesType` fallback).
    #[test]
    fn qualified_frozen_udt_field_resolves_identically_to_bare() {
        let reg = registry_with_address();
        let bare = CqlType::Frozen(Box::new(CqlType::Custom("address".to_string())));
        let qualified = CqlType::Frozen(Box::new(CqlType::Custom("test_ks.address".to_string())));
        let m_bare = render_field_marshal(&bare, KS, &reg);
        let m_qual = render_field_marshal(&qualified, KS, &reg);
        assert!(
            m_qual.starts_with("org.apache.cassandra.db.marshal.UserType("),
            "qualified frozen<ks.udt> must be bare UserType, got {m_qual}"
        );
        assert!(
            !m_qual.contains("BytesType"),
            "qualified UDT must resolve, not collapse to BytesType: {m_qual}"
        );
        assert_eq!(
            m_qual, m_bare,
            "qualified frozen<ks.udt> must render identically to the bare form"
        );
    }

    /// roborev #1020 Finding 1: a KEYSPACE-QUALIFIED
    /// `frozen<list<frozen<test_ks.address>>>` resolves the nested qualified UDT
    /// element through the registry split — identical marshal to the unqualified
    /// `frozen<list<frozen<address>>>` (FrozenType+ListType wrappers preserved,
    /// nested UDT expanded to `UserType(...)`, never `BytesType`).
    #[test]
    fn qualified_frozen_list_of_frozen_udt_resolves_identically_to_bare() {
        let reg = registry_with_address();
        let bare = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
            Box::new(CqlType::Custom("address".to_string())),
        )))));
        let qualified = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
            Box::new(CqlType::Custom("test_ks.address".to_string())),
        )))));
        let m_bare = render_field_marshal(&bare, KS, &reg);
        let m_qual = render_field_marshal(&qualified, KS, &reg);
        assert!(
            m_qual.starts_with(
                "org.apache.cassandra.db.marshal.FrozenType(\
                 org.apache.cassandra.db.marshal.ListType("
            ),
            "outer Frozen+List wrappers must be preserved: {m_qual}"
        );
        assert!(
            m_qual.contains("org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,"),
            "nested qualified address UDT must expand to UserType: {m_qual}"
        );
        assert!(
            !m_qual.contains("BytesType"),
            "nested qualified UDT must never collapse to BytesType: {m_qual}"
        );
        assert_eq!(
            m_qual, m_bare,
            "qualified nested frozen<list<frozen<ks.udt>>> must match the bare form"
        );
    }

    /// roborev #1020 Finding 1: `cql_type_references_udt` must detect a UDT behind
    /// a KEYSPACE-QUALIFIED name (so the column-level dispatch rewrites the header
    /// instead of leaving it `BytesType`).
    #[test]
    fn cql_type_references_udt_detects_qualified_name() {
        let reg = registry_with_address();
        let qualified = CqlType::Frozen(Box::new(CqlType::Custom("test_ks.address".to_string())));
        assert!(
            cql_type_references_udt(&qualified, KS, &reg),
            "qualified frozen<ks.udt> must be detected as referencing a UDT"
        );
    }
}
