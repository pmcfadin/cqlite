//! **The ONE package rule** for a Cassandra marshal class name (issue #3631,
//! roborev job 76).
//!
//! # The defect this module exists for
//!
//! `native_marshal_to_cql_type` used to take the text after the LAST `.` of a
//! marshal name and match it against Cassandra's native types — so it ignored the
//! PACKAGE entirely, and a third-party `com.acme.Int32Type` was decoded as CQL
//! `int`. That is a no-heuristics violation (#28): CQLite knew nothing about that
//! class and used its NAME RESEMBLANCE to pick a byte layout.
//!
//! # Authority — the pinned tag, never CQLite's own tables (#3041)
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TypeParser.java:450`
//! (and the identical line 466) is the single place Cassandra turns a name inside
//! a marshal type string into a class:
//!
//! ```java
//! String className = compareWith.contains(".") ? compareWith
//!                  : "org.apache.cassandra.db.marshal." + compareWith;
//! ```
//!
//! So a marshal name has **exactly two** legal spellings — the bare simple name
//! (resolved in the marshal package) and a fully-qualified class name — and every
//! `AbstractType` CQLite maps lives in that ONE package: at the pinned tag every
//! `.java` under `src/java/org/apache/cassandra/db/marshal/` declares
//! `package org.apache.cassandra.db.marshal;`. There is no second legitimate
//! package for a native marshal type. (`org.apache.cassandra.cql3.functions.types`
//! also holds a `TupleType` and a `UserType`, but those are DRIVER types, not
//! `AbstractType`s — `getAbstractType` would load one and then fail to find its
//! `instance` field — so they are not a marshal spelling of anything.)
//!
//! A name qualified with any OTHER package is therefore a third-party
//! `AbstractType` whose byte layout CQLite does not know.

use super::super::*;

impl V5CompressedLegacyParser {
    /// Cassandra's marshal package, trailing `.` included — the ONE spelling of
    /// this string on the read path.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) const MARSHAL_PACKAGE:
        &'static str = "org.apache.cassandra.db.marshal.";

    /// The class-name HEAD of a marshal type string: everything before the first
    /// `(`, trimmed. `MapType(A,B)` -> `MapType`; `Int32Type` -> `Int32Type`.
    ///
    /// The head, and never the whole string, is what the package rule applies to:
    /// the last `.` of `com.acme.VectorType(org.apache.cassandra.db.marshal.Int32Type)`
    /// is inside the ARGUMENTS, so a rule applied to the whole string would read
    /// that foreign class as living in the marshal package.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn marshal_head(
        type_str: &str,
    ) -> &str {
        let head = type_str.trim();
        match head.find('(') {
            Some(open) => head[..open].trim(),
            None => head,
        }
    }

    /// Split a marshal class-name HEAD into `(package, simple)`, where `package`
    /// KEEPS its trailing `.` and is `""` for an unqualified head.
    ///
    /// One parse of the head, so the rule below and the diagnostics that report a
    /// rejection cannot disagree about where the package ends.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn split_marshal_head(
        head: &str,
    ) -> (&str, &str) {
        let head = head.trim();
        match head.rfind('.') {
            // `..=dot` keeps the separating `.`, so `package` is the whole package
            // INCLUDING its terminator — which is what makes the equality below
            // reject `…db.marshalX.`.
            Some(dot) => (&head[..=dot], &head[dot + 1..]),
            None => ("", head),
        }
    }

    /// **THE package rule.** The SIMPLE class name Cassandra's own `TypeParser`
    /// would resolve `head` to, or `None` when `head` is qualified OUTSIDE
    /// `org.apache.cassandra.db.marshal` (see this module's header for the pinned
    /// authority).
    ///
    /// # Why the package is compared for EQUALITY
    /// `starts_with`, `ends_with` and `contains` each admit a package that merely
    /// RESEMBLES the marshal one, and all three shapes are real:
    /// `org.apache.cassandra.db.marshalX.Int32Type` (prefix),
    /// `notorg.apache.cassandra.db.marshal.Int32Type` (suffix) and
    /// `my.org.apache.cassandra.db.marshal.UserType(…)` (substring — the shape the
    /// `UserType(` marker locator admitted). Equality on the package, `.`
    /// terminator included, is the only test that rejects all of them.
    ///
    /// The PACKAGE compare is ASCII-case-insensitive, preserving the tolerance the
    /// reader already applied to the `UserType(` marker; a case variant of the
    /// marshal package can never be a DIFFERENT package, so the tolerance cannot
    /// admit a foreign type. The SIMPLE NAME stays case-SENSITIVE, because it is
    /// the whole identity of the class and `classForName` is case-sensitive.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn marshal_simple_name(
        head: &str,
    ) -> Option<&str> {
        let (package, simple) = Self::split_marshal_head(head);
        (package.is_empty() || package.eq_ignore_ascii_case(Self::MARSHAL_PACKAGE))
            .then_some(simple)
    }

    /// The simple name of a **QUALIFIED** marshal class name — `Some` only for the
    /// canonical `org.apache.cassandra.db.marshal.X` spelling.
    ///
    /// Structural forms and the `UserType(` marker use this NARROWER gate rather
    /// than [`Self::marshal_simple_name`], which is exactly what the reader has
    /// always required of them: roborev jobs 1359/1361 deliberately put top-level
    /// and nested `UserType` parsing on the same QUALIFIED marker, so admitting a
    /// bare `ListType(`/`UserType(` at one site only would recreate the
    /// partial-bare-support inconsistency they closed (nested UDT fields blobbed
    /// while the top level parsed, or the reverse). It is defined THROUGH the
    /// package rule, so "what the marshal package is" still has one definition,
    /// and being strictly narrower it can never admit a foreign package.
    fn qualified_marshal_simple_name(head: &str) -> Option<&str> {
        let head = head.trim();
        if !head.contains('.') {
            return None;
        }
        Self::marshal_simple_name(head)
    }

    /// The argument text of the marshal parameterised form `<pkg>CLASS(…)` — the
    /// slice starting just after the `(` — or `None` when `type_str` is not that
    /// form under the package rule.
    ///
    /// The ONE dispatcher for every structural arm (`FrozenType`, `UserType`,
    /// `ListType`, `SetType`, `MapType`, `TupleType`, `ReversedType`), which each
    /// used to carry its own copy of the fully-qualified literal.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn marshal_parameterised_inner<
        'a,
    >(
        type_str: &'a str,
        class: &str,
    ) -> Option<&'a str> {
        let trimmed = type_str.trim();
        let open = trimmed.find('(')?;
        if Self::qualified_marshal_simple_name(&trimmed[..open])? != class {
            return None;
        }
        Some(&trimmed[open + 1..])
    }

    /// A character that may appear in a Java class-name REFERENCE: an identifier
    /// character or the package separator. Used to find where the class name
    /// preceding a `(` begins.
    fn is_marshal_name_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '.'
    }

    /// The byte index just after the `(` of the outermost marshal `UserType(`
    /// marker in `type_str`, or `None`.
    ///
    /// The marker is searched for (rather than matched at the start) because a
    /// top-level UDT legitimately arrives wrapped — `FrozenType(UserType(…))`.
    /// A plain substring `find` of the qualified marker is what admitted a package
    /// SUFFIX (`my.org.apache.cassandra.db.marshal.UserType(…)`), so each
    /// candidate's class-name HEAD is extracted and put through the package rule.
    fn find_marshal_user_type_inner_start(type_str: &str) -> Option<usize> {
        const NAME: &str = "usertype";
        // `to_ascii_lowercase`, NOT `to_lowercase`: it is length- AND
        // boundary-preserving, so an index found in it is a valid index into
        // `type_str`. `to_lowercase` can change a string's length (`İ` -> `i̇`),
        // which would make the offsets below slice the wrong bytes or panic.
        let lower = type_str.to_ascii_lowercase();
        let mut from = 0usize;
        while let Some(rel) = lower[from..].find(NAME) {
            let name_end = from + rel + NAME.len();
            // Guarantees progress whichever branch below is taken.
            from = name_end;
            if !lower[name_end..].starts_with('(') {
                continue;
            }
            // The class-name head is the identifier/package run ending at the `(`.
            // Stepping back by `char_indices().rev()` keeps every index on a char
            // boundary, which a `rfind(pred).map(|i| i + 1)` would not for a
            // multi-byte delimiter.
            let head_start = type_str[..name_end]
                .char_indices()
                .rev()
                .find(|(_, c)| !Self::is_marshal_name_char(*c))
                .map_or(0, |(i, c)| i + c.len_utf8());
            if Self::qualified_marshal_simple_name(&type_str[head_start..name_end])
                .is_some_and(|simple| simple.eq_ignore_ascii_case(NAME))
            {
                return Some(name_end + 1);
            }
        }
        None
    }

    /// The comma-separated ARGUMENTS of the outermost marshal `UserType(...)` in
    /// `type_str`: `[keyspace, hex(name), hex(field):type, …]`.
    ///
    /// ONE locator + paren walk for both `UserType(` consumers — the `CqlType`
    /// parser (`parse_udt_type_definition_with_depth`) and the raw-marshal one
    /// (`udt_field_marshal_types`) — which each carried an independent copy of
    /// this code, and so each carried the same package-suffix hole.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn marshal_user_type_args(
        type_str: &str,
    ) -> Result<Vec<String>> {
        let inner_start = Self::find_marshal_user_type_inner_start(type_str)
            .ok_or_else(|| Error::schema(format!("Not a UserType: {}", type_str)))?;

        // Find the `)` matching the marker's `(`.
        let mut paren_depth = 1usize;
        let mut end_idx = None;
        for (rel, c) in type_str[inner_start..].char_indices() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        end_idx = Some(inner_start + rel);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end_idx) = end_idx else {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in UserType: {}",
                type_str
            )));
        };

        let parts = Self::split_type_args(&type_str[inner_start..end_idx])?;
        if parts.len() < 2 {
            return Err(Error::schema(format!(
                "UserType requires at least keyspace and name: {}",
                &type_str[inner_start..end_idx]
            )));
        }
        Ok(parts)
    }

    /// The refusal for a marshal name qualified OUTSIDE Cassandra's marshal
    /// package: an accurate, self-describing error rather than a native decode, a
    /// silent [`Value::Blob`] or the misattributed "nested user-defined type"
    /// message (issue #3631 / #28, roborev job 76).
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn foreign_marshal_package_error(
        type_str: &str,
    ) -> Error {
        let (package, simple) = Self::split_marshal_head(Self::marshal_head(type_str));
        Error::unsupported_format(format!(
            "cannot decode declared type '{type_str}': its marshal class is qualified \
             outside Cassandra's marshal package (package '{package}', expected \
             '{expected}'), so it is a third-party AbstractType whose byte layout CQLite \
             does not know. Matching its simple name '{simple}' against Cassandra's \
             native types would be a guess about that class's layout, and returning the \
             raw bytes as a blob would silently discard the declared type \
             (issue #3631 / #28)",
            expected = Self::MARSHAL_PACKAGE,
        ))
    }
}
