# Proposal: Extract `cqlite-ffi-common` — one implementation of the byte-math and error identity both bindings share (issue #1452)

**Milestone:** maintenance (bindings / FFI surface, parent epic #1434, capstone epic #1686) ·
**Routing:** **design-driven** — there is no external oracle for *where a Rust function lives* or *what
its signature is*. The Cassandra format decides what DECIMAL/VARINT/INET bytes MEAN (and that part is
already settled and tested); this change decides the crate boundary, the API shape, the dependency
direction, and — unavoidably — which of two currently-divergent rendering policies becomes the single
one. That last part is a product call, which is exactly why this is at Seam 1. · **Issue:** #1452 ·
**Related:** #1434 (parent), #1686 (capstone epic), **#1451 / PR #3378** (the authoritative FFI error
table — MERGED, so this change *moves* it rather than defining it), **#1705 / PR #3382** (the three
`ErrorCategory` enums), #1453 (inet malformed-length parity — MERGED), #1450 (duration/time parity),
**#1741** (Python DECIMAL fail-closed guard — MERGED), **#1754** (Node DECIMAL BigInt render +
exponent form — MERGED), #1712 (`pub-surface` crate-root guard), #1116/#1135 (file-size ratchet).

## Why

The byte-level math that turns raw CQL bytes into a displayable scalar is written **twice**, once per
binding. That duplication is not hypothetical debt — it is the direct cause of the X1/X2 divergences
this epic has been paying down one at a time (#1450 duration/time, #1451 errors, #1453 inet). Two
copies drift because **nothing forces them to agree**; each fix has restored agreement at a point in
time without removing the mechanism that produces the next divergence.

The mechanism is still live today. Since the issue was filed the two DECIMAL implementations have been
hardened **independently and differently** (#1741 Python, #1754 Node), and they now disagree on
observable output for inputs both accept — see the *Verified current state* table below. That is a new
X1/X2 divergence introduced by two correct-in-isolation fixes, which is the clearest possible evidence
that point fixes cannot close this class.

This change extracts a small, pure, dependency-light crate so those paths are shared **by
construction**. It is deliberately a **narrow** extraction, not a general dedupe.

## Verified current state (re-verified 2026-08-29 on `origin/main`; the issue body's 2026-07-01
file:line references are stale — every line number below is current)

| Subject | Node | Python | Status |
|---|---|---|---|
| DECIMAL | `bindings/node/src/value.rs:364` `fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String>` — `num_bigint::BigInt::from_signed_bytes_be` + one base-10 conversion; hard ceiling `DECIMAL_MAX_UNSCALED_BYTES = 32 KiB` (`:335`) → typed corruption error; **exponent form** `<digits>e<-scale>` above `DECIMAL_POSITIONAL_MAX_BYTES = 1024` (`:342`) or `|scale| > DECIMAL_MAX_SCALE_DIGITS = 1_000_000` (`:350`) | `bindings/python/src/value.rs:307` `decimal_to_pydecimal` — Python `int.from_bytes(signed=True)` + `str()` + `format!`; cap is `min(sys.get_int_max_str_digits(), 1_000_000)` (default **4300 digits**) → typed corruption error; **no exponent form** for large magnitudes | **Two implementations that now DISAGREE.** A 2000-byte unscaled with `scale=3` renders in Node (exponent form) and **raises `CqliteError` in Python** (≈4816 digits > 4300 cap). `scale = i32::MIN` renders in Node and raises in Python. Not filed anywhere; found by this verification. |
| VARINT | `bindings/node/src/value.rs:267` `varint_to_bigint(env, bytes)` — hand-rolled: `<=8` bytes via sign-extended `i64`; `>8` bytes pads to a multiple of 8, builds LE `u64` words, then a hand-written two's-complement negate loop, then `env.create_bigint_from_words(is_negative, words)` | `bindings/python/src/value.rs:287` `varint_to_pyint` — `int.from_bytes(bytes, "big", signed=True)` | Same semantics, two code paths. Node's negate loop is the only hand-rolled bignum arithmetic in either binding. |
| INET | `bindings/node/src/value.rs:452` `fn inet_bytes_to_string(&[u8]) -> Result<String, String>` (already pure + unit-testable) + `inet_to_string_js` (`:467`) | `bindings/python/src/value.rs:570` `inet_to_py` — 4/16 dispatch into `ipaddress.IPv4Address`/`IPv6Address` **from packed bytes** (never from a string) | #1453 aligned the **error message** by hand-copying the string `"Invalid inet address length: {n} (expected 4 or 16)"` into both files. The shared thing is the **length dispatch + the error**, not the formatting — Python does not format. |
| DURATION | `bindings/node/src/value.rs:434` `duration_to_object` — sets three fields, no arithmetic | `bindings/python/src/value.rs:225` `#[pyclass] Duration { months, days, nanos }` — no arithmetic | **NOT duplicated math.** Both are pure passthroughs of `(months, days, nanos)`. Nothing to extract. |
| DATE | `bindings/node/src/value.rs:199` inline arm — `days.checked_mul(86_400_000)` → JS `Date` | `bindings/python/src/value.rs:194` `date_to_pydate` — `date.fromordinal(719163) + timedelta(days=…)` | **NOT duplicated math.** Different target types, different (both correct) arithmetic; the only common fact is "days since epoch", which is the `Value::Date` contract, not a helper. Nothing to extract. |
| OTel keys | `bindings/node/src/observability.rs:45` `#[napi(object)] struct OtelOptions` — 7 typed `Option<…>` fields; unknown keys dropped by napi deserialization | `bindings/python/src/observability.rs:85` `const KNOWN_KEYS: &[&str]` (7 entries) — unknown key ⇒ `ValueError` | As the issue describes. Both already delegate `OtelProtocol::parse` + the sampling clamp to `cqlite_core::observability`. Only the **name list** is cleanly shareable. |
| Error table | `bindings/node/src/error.rs:65` `use cqlite_core::ffi_error_contract::contract_for;` | `bindings/python/src/error.rs:25` `use cqlite_core::ffi_error_contract::{contract_for, PyExceptionClass};` | **#1451 MERGED.** The table exists at `cqlite-core/src/ffi_error_contract.rs` (389 lines), declared `pub mod ffi_error_contract;` at `cqlite-core/src/lib.rs:17`, with `cqlite-core/tests/ffi_error_contract_table.rs` (250 lines, 12 tests). Its own module doc (`:14-20`) says: *"When `cqlite-ffi-common` (issue #1452) lands, this module moves there verbatim … It is deliberately a top-level module so that move is a file move."* |

Three further corrections to the issue body, each of which changes the plan:

1. **`bindings/python` and `bindings/node` ARE already workspace members** (`Cargo.toml:2-13` lists
   `cqlite-core`, `cqlite-cli`, `cqlite-flight`, `bindings/python`, `bindings/node`, `tests`,
   `tests/format-compatibility`, `examples`, `tools/*`, `xtask`). The issue's *"the two bindings live
   outside the workspace"* is false. Consequence: no membership surgery is needed on the bindings, and
   the new crate is linted, `cargo test`-ed and gated by the workspace automatically.
2. **`num-bigint` is NOT a workspace dependency.** There is no `num-bigint` entry under
   `[workspace.dependencies]`; three crates each declare `num-bigint = "0.4"` independently
   (`cqlite-core/Cargo.toml:80`, `cqlite-cli/Cargo.toml:104`, `bindings/node/Cargo.toml:38`). The new
   crate declares its own `"0.4"` to match; promoting the four to a workspace entry is optional tidying
   and is a **non-goal** here.
3. **Cross-binding test-support probes already exist** and are the natural carrier for the
   "same shared fn" assertions the issue asks for: Node `error_contract_node_codes()` /
   `error_contract_probe(variant)` (`bindings/node/src/lib.rs:81,113`, re-exported as
   `_errorContractNodeCodes` / `_errorContractProbe`) and Python `_raise_mapped_core_error`
   (`bindings/python/src/lib.rs:111`), `_decimal_from_parts` (`:72`), `_inet_from_bytes`. This change
   extends that pattern rather than inventing one.

## What Changes

1. **New workspace member `cqlite-ffi-common/`** — pure Rust, `cqlite-core` + `num-bigint` only, with
   a **mechanically enforced** prohibition on `pyo3`/`napi`/`napi-derive` at *any* depth (measured with
   `cargo metadata`, not asserted from the manifest text alone).
2. **DECIMAL, VARINT and INET become one implementation each**, living in the new crate and unit-tested
   there; both bindings keep only a thin adapter (bytes → shared fn → Py/JS object).
3. **The #1451 error contract MOVES** from `cqlite-core/src/ffi_error_contract.rs` to
   `cqlite-ffi-common/src/error_contract.rs` (its own doc comment pre-authorises this as a file move),
   its 250-line test moves with it, `pub mod ffi_error_contract;` is **removed** from
   `cqlite-core/src/lib.rs`, and both bindings + `bindings/node/README.md:284` re-point.
4. **`pub const KNOWN_OTEL_KEYS: &[&str]`** moves to the new crate. Python's allowlist consumes it; Node
   gains a unit test asserting its `OtelOptions` fields and the list are the same set, so the list has a
   second enforcing consumer instead of being one binding's private constant with a new home.
5. **Canonical cross-binding test vectors** (`cqlite_ffi_common::vectors`) are exported from the shared
   crate and consumed by *both* binding test suites through their production paths — the mechanism that
   makes the "same shared fn" invariant an assertion rather than a comment.
6. **One DECIMAL rendering policy** replaces the two that disagree today. Which one is an **owner
   decision** (Open Question 3 below); the spec is written so the requirement is *"exactly one policy,
   documented, and identical across bindings"*, with the chosen policy's behaviour pinned by vectors.

## Non-goals (copied from the issue, plus what verification added)

- **Do NOT move the value dispatch bodies.** `value_to_py` (`bindings/python/src/value.rs:23`) and
  `value_to_napi` (`bindings/node/src/value.rs:165`) match arms stay in their bindings; they build
  Py/JS objects and cannot be pyo3/napi-free.
- **Do NOT move runtime glue** (`runtime.rs` in either binding) or the **streaming iterators**
  (`StreamingIterator`, napi `AsyncTask`s) — the 2026-07-01 audit explicitly marks these "not worth
  sharing."
- **Do NOT turn this into a general dedupe.** Only the enumerated byte-math + error table + otel key
  list moves.
- **Do NOT add `pyo3`/`napi`/`napi-derive` to `cqlite-ffi-common`** — that would defeat the point.
- **Do NOT move DURATION or DATE helpers.** Verified above: neither is duplicated math. The issue body
  hedged this (*"only move what is genuinely duplicated"*); the verification answers it — nothing moves.
- **Do NOT unify the OTel validation *mechanism*.** Node's napi struct and Python's dict-allowlist are
  legitimately different shapes. Only the key-name list is shared; the divergence is recorded in the
  crate docs.
- **Do NOT change `Error::category()` / `Error::is_recoverable()` semantics**, and do not re-key the
  binding tables onto `error_schema::classify()` — that is epic-level (Open Question 2).
- **Do NOT promote `num-bigint` to `[workspace.dependencies]`.** Unrelated tidying.
- **Do NOT change the CLI, Flight, Trino, the read path, or any on-disk format.**

## Impact

- **New crate:** `cqlite-ffi-common/` (`src/lib.rs`, `src/decimal.rs`, `src/varint.rs`, `src/inet.rs`,
  `src/error_contract.rs`, `src/otel_keys.rs`, `src/vectors.rs`, `tests/`). Added to `Cargo.toml:2`
  `members`, `[lints] workspace = true` like every sibling.
- **`cqlite-core`:** `src/ffi_error_contract.rs` deleted (moved), `src/lib.rs:17` `pub mod` line
  removed, `tests/ffi_error_contract_table.rs` moved. This is a **public-API removal** from
  `cqlite-core` — see `design.md` D5 for why no deprecated re-export is left behind. Note that the
  `pub-surface` gate component (#1712) checks declaration/inner-`cfg` consistency and is **not** an
  API-drift detector, so it will not flag this; the removal is called out in the PR body instead.
- **`bindings/python`, `bindings/node`:** one new path dependency each; byte-math bodies replaced by
  calls; imports re-pointed; new test-support surfaces for the vector assertions.
- **Public binding surfaces:** unchanged for every value both bindings render the same way today. The
  **only** intended behaviour change is the DECIMAL policy convergence (Open Question 3) — a
  binding-visible change that must be recorded in `CHANGELOG.md` and the binding READMEs.
- **No-heuristics mandate (#28):** unaffected and slightly strengthened — the length dispatch for INET
  becomes a single typed decision instead of two, with no passthrough branch in either copy.
- **<128MB memory budget:** unaffected (per-scalar, allocation-bounded helpers).
- **File-size ratchet (#1116/#1135):** the change *reduces* `bindings/node/src/value.rs` (674 lines)
  and `bindings/python/src/value.rs` (658 lines); the new crate's modules are created small and split
  by responsibility from the start.
