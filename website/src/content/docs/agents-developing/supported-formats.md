---
title: Supported formats (version floor)
description: CQLite targets Cassandra 5.0. na+/nb BIG and oa/da BTI are in scope; pre-na (ma–me, Cassandra 3.x) is out of scope — do not introduce, support, or review pre-na correctness. (Issue #1249)
sidebar:
  label: Supported formats
  order: 3
---

Issue #1249 codified this as a hard rule after roborev repeatedly raised
"regression for older SSTable versions" findings on pre-`na` (Cassandra 3.x)
formats — formats CQLite does not read at all. The churn came from dead version-gate
modeling surface that *looked* like pre-`na` support. This page is the single
authoritative statement of the support floor.

## The rule

**CQLite targets Cassandra 5.0. `na`+/`nb` BIG and `oa`/`da` BTI are in scope.
Pre-`na` (`ma`–`me`, Cassandra 3.x) is out of scope and SHALL NOT be introduced,
supported, or reviewed for correctness.**

| Format | In scope | Out of scope |
|--------|----------|--------------|
| BIG    | `na`, `nb`, `oa` | `ma`–`me` (Cassandra 3.x) and anything `< na` |
| BTI    | `da` | any version other than `da` |

## For reviewers (including roborev)

- **Do not re-litigate pre-`na` correctness.** A branch, gate, or test that touches
  `ma`–`me` is not a regression surface — pre-`na` reads are unsupported by design.
- Do not request changes that "fix" pre-`na` behaviour, add pre-`na` gates, or restore
  deleted pre-`na` modeling code. The floor is the contract.

## How it is enforced

The floor lives in version parsing and is propagated by the reader:

- `BigVersionGates::from_version` returns `Error::UnsupportedVersion { version, floor: "na" }`
  for any BIG version `< na`. It never constructs gates for a below-floor version.
- `BtiVersionGates::from_version` returns `Error::UnsupportedVersion { version, floor: "da" }`
  for any BTI version other than `da`.
- `SSTableReader::open` **propagates** `Error::UnsupportedVersion` instead of degrading to
  the `nb` fallback. Only a structurally-unparseable descriptor (not a parsed-but-below-floor
  one) may use the fallback.

The rejection happens before any row data is read. `Error::UnsupportedVersion` maps to the
non-recoverable `ErrorCategory::Data`.
