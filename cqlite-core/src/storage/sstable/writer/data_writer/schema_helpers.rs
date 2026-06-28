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
/// Matches the reader logic in `v5_compressed_legacy.rs`.
pub(crate) fn is_complex_column(data_type: &str) -> bool {
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
    // detected here — the writer holds only a `&TableSchema` and cannot resolve a
    // bare name to a UDT without a `UdtRegistry` (issue #927 item 4, follow-up).
    // Compaction inputs always carry the full `UserType(...)` marshal string, so
    // this covers the compaction path.
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
            name.starts_with("udt:")
                || registry.get_udt(keyspace, clean).is_some()
                || registry
                    .get_keyspace_udts(keyspace)
                    .is_some_and(|m| m.keys().any(|k| k.eq_ignore_ascii_case(clean)))
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
