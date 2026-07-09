---
title: No-Heuristics Mandate
description: Use authoritative metadata only. No guessing at types or formats. Legacy heuristics behind opt-in feature flags. (Issue #28)
sidebar:
  label: No-heuristics mandate
  order: 2
---

Issue #28 codified this as a hard rule after early prototype code guessed column types
from value patterns. Guessing produced silent wrong answers on valid Cassandra data.

## The rule

**Use authoritative metadata only. Never infer types or formats from value content.**

- When a schema is present, use it for all type decoding. Schema-aware decoding is not optional.
- When no schema is present, use the serialization metadata embedded in `Statistics.db` (encoding stats, column definitions). Do not fall back to type guessing.
- Legacy heuristics (blob fallback, type inference from byte patterns) exist only behind the `legacy-heuristics` feature flag and are disabled by default.

## What this means in practice

**Allowed:**
```rust
// Decode using schema type — correct
let value = schema.column_type("name")?.decode(bytes)?;

// Decode using Statistics.db metadata — correct
let value = stats.column_type("name")?.decode(bytes)?;
```

**Not allowed:**
```rust
// Guess type from value — forbidden without the `legacy-heuristics` feature
if bytes.len() == 16 { return Ok(CqlValue::Uuid(...)) }
if bytes.starts_with(b"\x00") { return Ok(CqlValue::Int(...)) }
```

**Behind the `legacy-heuristics` flag only:**
```rust
#[cfg(feature = "legacy-heuristics")]
fn decode_with_fallback(bytes: &[u8]) -> CqlValue {
    // Heuristic fallback — acceptable only here
}
```

## Rationale

Cassandra's binary format is untagged. A 4-byte sequence could be an `int`, a `float`,
the first four bytes of a `text`, or part of a `blob`. Without schema metadata, the
correct interpretation is unknowable. Guessing will produce plausible-but-wrong output
on real production data.

The one test that exercises legacy blob fallback
(`test_legacy_format_allows_blob_fallback_with_feature`) is explicitly excluded from
the default `core-tests` gate run. It requires `--features legacy-heuristics` and is
intentionally not in CI defaults.

## Code quality requirements

These requirements enforce the mandate at the language level:

- No `unwrap()` or `expect()` in library code — errors must propagate via `?`
- Use `thiserror` for error types — structured errors, not string messages
- `RUSTFLAGS="-D warnings"` must pass — zero clippy warnings
- Memory target: `<128MB` for large files — no unbounded buffering

## Schema-aware decoding flow

```
Statistics.db → EncodingStats → column type metadata
     ↓
Schema file (if present) → overrides / supplements metadata
     ↓
Decoder uses column type to interpret raw bytes
     ↓
Typed CqlValue — never raw bytes exposed to callers
```

If Statistics.db metadata is missing or corrupt for a column, the correct behaviour
is to return an error, not to guess. Callers can decide how to handle missing columns;
the library must not invent values.

## Feature flags and the mandate

| Feature | Heuristics allowed? |
|---------|---------------------|
| `all-compression` (default) | No |
| `state_machine` (default) | No |
| `cli-helpers` | No |
| `metrics` | No |
| `experimental` | No (bloom-filter tests + write stubs, not heuristics) |
| `legacy-heuristics` | Yes — explicitly opt-in |

See [Key source paths](/cqlite/agents-developing/source-map/) for where type decoding lives in the codebase.
The full feature-flags table and feature-gated test notes are in [Gate Contract](/cqlite/agents-developing/gate-contract/).
