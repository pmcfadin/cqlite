# Query Result Contract

What CQLite guarantees about the **shape** of query and `export` output in the `table`, `csv` and
`json` writers.

## Why this file exists

Four places in the tree cited `QUERY_RESULT_CONTRACT.md` as their specification while no such file
existed (issue #3644, test-side item (b)):

| Citation site | Claim it made |
|---|---|
| `cqlite-core/src/util/value_fmt.rs` | "Implements the Value → String mapping per QUERY_RESULT_CONTRACT.md" |
| `cqlite-cli/src/output/mod.rs` | "All writers follow the QUERY_RESULT_CONTRACT.md specification" |
| `cqlite-cli/src/output/csv.rs` | "Implements CSV output format following QUERY_RESULT_CONTRACT.md specification" |
| `cqlite-cli/tests/output_determinism_regression_tests.rs` | "as specified in QUERY_RESULT_CONTRACT.md" |

Each now cites this file by its **path**, not by a bare filename — the bare name is part of why
nobody could tell the document was missing.

A dangling authority reference is worse than none: it reads as though a decision was written down
and settled somewhere, so the next reader stops looking. That already cost time on #1491 — a
reviewer read an adjacent test comment as a contract on export **row** order and raised a finding on
it. The comment was wrong and was corrected in #3580, but the missing document is the deeper version
of the same problem, and the row-order rule below is stated here precisely so the question has one
answer.

## What is normative

This document is **not** a second implementation. Where a rule is mechanical and already exhaustive
in code, the code is normative and this file names it rather than restating it — a prose table of
per-type renderings would decay exactly like a stale comment, which is the failure mode being fixed.

| Concern | Normative artifact |
|---|---|
| `Value` → text (table, CSV, and the stringified JSON arms) | `ValueFormatter::format_value` / `format_into` — `cqlite-core/src/util/value_fmt.rs` |
| Genuine-NULL predicate | `ValueFormatter::is_null` — `value_fmt.rs:39` |
| `Value` → JSON kind | `JSONWriter::value_to_json` — `cqlite-cli/src/output/json.rs:147` |
| CSV framing/escaping | the `csv` crate, as configured in `cqlite-cli/src/output/csv.rs` |

Each rule below names the test that pins it. A rule with no pinning test does not belong here.

## 1. Column order

JSON object keys and CSV columns appear in **`metadata.columns` order** — never alphabetical, never
`HashMap` iteration order — and the two formats agree with each other.

- JSON: `impl Serialize for RowObj` (`json.rs:57`) walks the borrowed key slice in column order.
- CSV: `CSVWriter::write` (`csv.rs:54`) and `StreamingCSVWriter::write_chunk` (`csv.rs:170`).
- Pinned by `cqlite-cli/tests/output_determinism_regression_tests.rs`: JSON at
  `test_json_preserves_non_alphabetical_column_order`,
  `test_json_key_position_in_string_matches_column_order`,
  `test_json_ordering_independent_of_hashmap_insertion`; CSV at
  `test_csv_header_order_matches_metadata_columns`,
  `test_csv_data_order_independent_of_hashmap_insertion`; cross-format at
  `test_json_and_csv_have_consistent_column_order`. `test_json_regression_detection_wrong_order` and
  `test_csv_regression_detection_wrong_header_order` are the negative controls.

## 2. Duplicate output column names

A result with duplicate output column names (`SELECT a, a`, or duplicate aliases) renders **one**
key, at the **first** position, carrying the **last** value — matching the historical
`serde_json::Map::insert` collapse. See `dedup_keys_last_wins` (`json.rs:43`), pinned by
`test_duplicate_column_names_collapse_last_wins_batch` / `..._streaming` (`json.rs`).

## 3. NULL and missing columns

| Case | JSON | CSV |
|---|---|---|
| `Value::Null` | `null` | empty field |
| `Value::Frozen(Null)` | `null` | empty field |
| column absent from the row | `null` | empty field |
| `Value::Text("null")` | `"null"` | `null` (four characters — **not** empty) |

The genuine-null test is `ValueFormatter::is_null`, which unwraps `Value::Frozen` recursively. It
exists because the earlier `format_value(v) == "null"` sentinel collapsed a literal text value
`"null"` to an empty CSV field (issue #1499). Pinned by `test_csv_null_values_become_empty`,
`test_csv_literal_null_text_is_not_emptied` (and its streaming twin),
`test_csv_frozen_null_becomes_empty` (and its streaming twin), `test_csv_missing_columns_become_empty`
(`cqlite-cli/src/output/csv.rs`), and `test_json_ordering_with_null_and_missing_values`.

## 4. Row order

Rows are emitted in Cassandra's on-disk **`(token, key)` order** — the #1577 invariant. This is
deterministic and stable run to run; it is simply **not predictable from the DDL**, because the
partition order is by Murmur3 token rather than by declared key value. Those two things are
different, and conflating them is the specific error #3580 corrected.

`export` applies **no sort of its own**. Gaining one would be a deliberate contract change and must
update the pin in `row_order_divergence` (`cqlite-cli/tests/support/golden_value_compare.rs:561`),
which compares the emitted sequence against the `sstabledump` golden's and reports a reordering.

## 5. Value rendering — text formats

`table` and `csv` render every cell through `ValueFormatter` (`csv.rs:88-89`, `csv.rs:184-185`,
`table.rs:72`); the CLI keeps no separate copy — `cqlite-cli/src/output/value_fmt.rs` is a re-export
shim. So the per-type text mapping is defined by that one module, and CSV has no formatting logic of
its own.

CSV framing (delimiter, quoting, embedded quotes and newlines) is the `csv` crate's, configured at
`csv.rs:150-155`; pinned by `test_csv_special_characters_are_escaped`.

## 6. Value rendering — JSON kinds

JSON is the one egress that is **not** just stringified text: `value_to_json` (`json.rs:147`) chooses
a JSON *kind* per `Value` arm. The oracle for that choice is Cassandra's
`AbstractType.toJSONString` hierarchy at the pinned tag `cassandra-5.0.8` — never CQLite's own prior
output (CLAUDE.md, format-authority rule).

**Not the oracle:** the `*-Data.db.jsonl` `sstabledump` goldens. `JsonTransformer.java:452` writes a
collection cell **path** with `json.writeString(ct.nameComparator().getString(...))` — always a JSON
string — while cell **values** (line 494) go through `writeRawValue(cellType.toJSONString(...))`. For
a `set<T>` the element lives in the path, so the golden's quoted element tokens are a **dump
artifact**. Reading them as an egress oracle produces exactly the wrong answer for non-finite floats.

### Numeric kinds

- Integral types (`tinyint`, `smallint`, `int`, `bigint`, `counter`) → JSON number.
- `float` / `double` → JSON number, except **non-finite**, which is the literal `null`. This matches
  `DoubleType.java:114-123` and `FloatType.java:115-124`, whose own comment reads *"JSON does not
  support NaN, Infinity and -Infinity values. Most of the parser convert them into null."* The loss
  is Cassandra's deliberate choice and CQLite reproduces it rather than inventing an encoding.
  **CSV is unaffected** and carries `NaN` / `Infinity` / `-Infinity` verbatim (`value_fmt.rs:184-215`);
  CSV has no JSON literal constraint and must not be aligned down to one.
- `varint` and `decimal` → **unquoted JSON number**, per `IntegerType.java:488-491` and
  `DecimalType.java:314-317` (both `Objects.toString(...)`, and `DecimalType` deliberately overrides
  the quoting `AbstractType.java:186-189`). Emitted with full precision, not via `f64`. A value whose
  formatted text is not a valid JSON number (a corruption marker) falls back to a JSON string rather
  than emitting invalid JSON.

### Stringified kinds

`blob` (`0x…` hex), `timestamp`, `date`, `time`, `uuid`/`timeuuid` and `duration` render as JSON
strings carrying their `ValueFormatter` text. `text`/`varchar`/`ascii` are JSON strings.

### Container kinds, and one recorded deviation

`list` and `set` render as JSON arrays, elements recursively by these same rules — matching
`SetType.java:230` / `ListType.java:247`, which delegate to the element type.

**`map` deviates deliberately.** CQLite renders a map as an **array of `{"key":…, "value":…}`
objects** (`json.rs:193-200`), where `MapType.java:362-388` renders a JSON **object** and coerces
every key to a string. The deviation is intentional: a CQL map may be keyed by any type, and
Cassandra's form is lossy for non-text keys, which CQLite's preserves. It is recorded here because an
undocumented deviation from the oracle is indistinguishable from a defect — this one is a decision.

## 7. Changing this contract

Any change to a rule above is a change to observable output. It requires: the oracle read at the
pinned `cassandra-5.0.8` tag (for anything Cassandra also renders), an updated pinning test, and an
update to this file in the same change. If a rule here and the code disagree, that is a bug in one of
them — say which, in the change that fixes it.
