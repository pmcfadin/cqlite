## Schema JSON Format Specification (M2)

Defines the canonical JSON format(s) the CLI accepts for schema ingestion in M2. Aligned with `cqlite-core` schema types and designed to be practical for local datasets and tests.

Scope: table and UDT definitions required for read-only SELECT on Cassandra 5 SSTables.

---

### Goals

- Human-editable format for quick iteration in tests and examples
- Deterministic parsing and merging across multiple files and directories
- Clear validation and actionable errors (exit code 3 on failure)

---

### Canonical Formats

The CLI accepts two equivalent JSON schema variants. Fields are case-insensitive unless noted.

1) Minimal Table Schema (recommended for simple cases)

```json
{
  "keyspace": "ks",
  "table": "users",
  "columns": [
    { "name": "id", "type": "uuid" },
    { "name": "name", "type": "text" },
    { "name": "email", "type": "text" },
    { "name": "created_at", "type": "timestamp" }
  ],
  "partition_keys": ["id"],
  "clustering_keys": []
}
```

2) Full Schema (tables + UDTs + options)

```json
{
  "keyspace": "ks",
  "udts": [
    {
      "name": "address_type",
      "fields": [
        { "name": "street", "type": "text" },
        { "name": "city", "type": "text" },
        { "name": "zip", "type": "int" }
      ]
    }
  ],
  "tables": [
    {
      "name": "users",
      "columns": [
        { "name": "id", "type": "uuid" },
        { "name": "name", "type": "text" },
        { "name": "addr", "type": "frozen<address_type>" }
      ],
      "partition_keys": ["id"],
      "clustering_keys": [],
      "options": {
        "comment": "demo table",
        "compression": { "class": "LZ4Compressor" }
      },
      "indexes": []
    }
  ]
}
```

Notes:
- For legacy examples, `data_type` is accepted as a synonym for `type`.
- `primary_key` (array) is accepted as a shorthand for `partition_keys` when no clustering keys are used.

---

### Supported Types (strings)

- Primitives: `ascii`, `text`, `varchar`, `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `varint`, `float`, `double`, `decimal`, `uuid`, `timeuuid`, `timestamp`, `date`, `time`, `blob`.
- Collections: `list<T>`, `set<T>`, `map<K,V>`.
- Tuples: `tuple<T1, T2, ...>`.
- Frozen: `frozen<...>` wrapper.
- UDT: any previously defined UDT name; may be wrapped by `frozen<...>`.

Type strings are case-insensitive. Unknown or malformed type strings produce a schema error.

---

### Validation Rules (M2)

- Required: `keyspace`, `table` (or `tables`), `columns`, `partition_keys`.
- Column `name` must be unique per table.
- `partition_keys` must be non-empty and reference existing columns.
- `clustering_keys` (if present) must reference existing columns; order matters.
- UDTs must be defined before use. Circular UDT references are unsupported in M2 (error).
- Collections and frozen nesting must be syntactically valid.

On error, CLI exits with code `3` and prints:
- Error summary (what failed)
- File path and, when available, line/offset info
- Hint to resolve (e.g., define UDT before table)

---

### Merging & Precedence

Applies when loading multiple files and/or directories (see arch plan §1.3):

1. Inputs are applied in the order provided via repeated `--schema` flags.
2. Within each directory, files are processed in lexical order.
3. Last-wins per fully qualified object name (`keyspace.table`, `keyspace.type`).
4. Two-pass load: UDTs → tables; unresolved references produce an error.

Example: If `users` is defined in `base/users.json` and again in `overrides/users.json`, the latter replaces the former.

---

### Multiple Tables per File

- Minimal format: one table per file.
- Full format: multiple tables allowed via `tables` array.

### UDT-Only Files

UDT-only files (containing `udts` but no `tables`) are supported. This is useful for sharing type definitions across multiple schema files:

```json
{
  "keyspace": "ks",
  "udts": [
    {
      "name": "address_type",
      "fields": [
        { "name": "street", "type": "text" },
        { "name": "city", "type": "text" },
        { "name": "zip", "type": "int" }
      ]
    }
  ]
}
```

UDT-only files are loaded before tables, making the types available for use in table definitions in other files.

---

### Examples

Basic table with collections:

```json
{
  "keyspace": "ks",
  "table": "events",
  "columns": [
    { "name": "id", "type": "timeuuid" },
    { "name": "tags", "type": "set<text>" },
    { "name": "props", "type": "map<text,text>" }
  ],
  "partition_keys": ["id"],
  "clustering_keys": []
}
```

UDT usage with frozen:

```json
{
  "keyspace": "ks",
  "udts": [
    { "name": "point", "fields": [
      { "name": "lat", "type": "double" },
      { "name": "lon", "type": "double" }
    ]}
  ],
  "tables": [
    { "name": "geo", "columns": [
      { "name": "id", "type": "uuid" },
      { "name": "pos", "type": "frozen<point>" }
    ], "partition_keys": ["id"], "clustering_keys": [] }
  ]
}
```

---

### Compatibility & Future Notes

- Index definitions may appear but are ignored in M2 (no index usage in execution).
- Table options are parsed for completeness but do not affect M2 query planning.
- Timestamp rendering in output follows M2 value formatting rules (default UTC) as documented separately.


