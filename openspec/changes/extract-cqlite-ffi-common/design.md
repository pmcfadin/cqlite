# Design: `cqlite-ffi-common` (issue #1452)

> Every file:line in this document was re-verified on `origin/main` on 2026-08-29. The issue body's
> 2026-07-01 references are stale; see `proposal.md` for the delta table.

## D0. Crate layout and dependency direction

```
bindings/python (pyo3)  ─┐
                          ├─→ cqlite-ffi-common ─→ cqlite-core
bindings/node   (napi)  ─┘        (pure Rust)
```

`cqlite-ffi-common` depends on `cqlite-core` (for `Error`, `ErrorCategory`) and `num-bigint`. It
depends on **nothing else**, and on no FFI framework. `cqlite-core` does **not** depend on it — the
arrow points one way only, so no cycle is possible and `cqlite-core` gains nothing new.

```
cqlite-ffi-common/
  Cargo.toml                 # [lints] workspace = true, like every sibling crate
  src/lib.rs                 # crate docs (incl. the OTel-shape divergence note) + pub mod re-exports
  src/decimal.rs             # decimal_to_string + its policy constants
  src/varint.rs              # varint_to_bigint + the napi words adapter
  src/inet.rs                # InetKind, InetError, inet_kind, inet_bytes_to_string
  src/error_contract.rs      # the #1451 table, moved verbatim from cqlite-core
  src/otel_keys.rs           # KNOWN_OTEL_KEYS
  src/vectors.rs             # canonical cross-binding test vectors (see D7)
  tests/…                    # unit/integration tests incl. the dependency-boundary test (D6)
```

Files are created **small and split by responsibility** from the outset (#1116): one concern per
module, none near the ~800-line source target.

**Workspace membership is one line.** `Cargo.toml:2-13` already lists `bindings/python` and
`bindings/node` as members (the issue body says otherwise — it is wrong), so only
`"cqlite-ffi-common",` is added. Nothing else about the workspace changes.

**`num-bigint` is declared locally as `"0.4"`**, matching `cqlite-core/Cargo.toml:80`,
`cqlite-cli/Cargo.toml:104` and `bindings/node/Cargo.toml:38`. There is no `[workspace.dependencies]`
entry for it and creating one is out of scope (`proposal.md` non-goals).

## D1. DECIMAL — `pub fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String, DecimalError>`

**Signature.** Node's current fn returns `napi::Result<String>`; the shared one must be FFI-free, so it
returns a crate-local typed error:

```rust
/// Rendering policy limits are `pub const` so both bindings and their tests can name them.
pub const DECIMAL_MAX_UNSCALED_BYTES: usize = 32 * 1024;
pub const DECIMAL_POSITIONAL_MAX_BYTES: usize = 1024;
pub const DECIMAL_MAX_SCALE_DIGITS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalError {
    /// Unscaled magnitude beyond the sanity ceiling — corrupt SSTable, fail closed.
    UnscaledTooLarge { scale: i32, unscaled_len: usize, max_unscaled_bytes: usize },
}
impl std::fmt::Display for DecimalError { /* the single canonical message */ }

pub fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String, DecimalError>;
```

Both bindings map `DecimalError` onto `cqlite_core::Error::corruption(err.to_string())` and then
through their **existing** production error path (`to_napi_error` / `to_py_err`), so the resulting JS
error code / Python exception class still come from the #1451 table — one error identity, one message,
by construction.

**Implementation base: Node's #1754 body, moved essentially verbatim** (`bindings/node/src/value.rs:364-430`).
It is already `num-bigint`-based, already pure, already has the O(1) length check before the single
superlinear base conversion, and already avoids the `scale == i32::MIN` overflow by widening to `i64`.
Python's body cannot be the base: its core step is `str()` on a *Python* int and its cap is derived
from `sys.get_int_max_str_digits()`, neither of which exists outside pyo3.

**The consequence, stated plainly, because it is the one behaviour change in this change.** Python's
#1741 guard exists because the *Python* `int → str` conversion raises an uncatchable `ValueError` past
`sys.get_int_max_str_digits()` (default 4300). Once Rust renders the string, **Python never calls
`str()` on a Python int at all** and that failure mode is structurally gone — the guard's *reason*
disappears with the move. But its *effect* is currently observable: today Python raises
`CqliteError` for a 2000-byte unscaled value that Node renders. Adopting Node's policy for both makes
Python render it. That widening is almost certainly right (the value is well-formed and
arbitrary-precision DECIMAL is legal Cassandra), and #1741's actual guarantee — *a corrupt SSTable
raises a typed error, it never aborts the driver* — is **preserved**, because the 32 KiB ceiling still
fails closed and the render is now infallible below it. It is still a **binding-visible behaviour
change on the error path**, so it is Open Question 3 rather than a decision taken here.

## D2. VARINT — `BigInt` core, two thin adapters

```rust
/// The single implementation of the semantic: big-endian two's complement,
/// empty slice == 0.
pub fn varint_to_bigint(bytes: &[u8]) -> num_bigint::BigInt;

/// napi's `create_bigint_from_words(sign_bit, words)` contract: sign-magnitude,
/// little-endian u64 words. A thin wrapper over `BigInt::to_u64_digits()`.
pub fn varint_to_sign_and_le_words(bytes: &[u8]) -> (bool /* is_negative */, Vec<u64>);
```

**Why both forms, rather than picking one** (the issue leaves this open): the two FFI ABIs want
genuinely different shapes, and neither can be derived from the other *inside the binding* without
re-implementing the thing we are extracting.

- **napi** takes `(sign_bit, Vec<u64>)` little-endian **sign-magnitude** words. Today
  `bindings/node/src/value.rs:288-320` hand-rolls the two's-complement → sign-magnitude conversion,
  including a manual carry-propagating negate loop. `BigInt::to_u64_digits()` returns exactly
  `(Sign, Vec<u64>)` in LE magnitude order — so the words adapter is three lines and the hand-rolled
  bignum arithmetic is **deleted**, which is most of this half's value.
- **pyo3** has no napi-style words constructor. Two routes, both acceptable to the spec (which
  requires only that the *semantic* come from the shared fn):
  1. enable pyo3's `num-bigint` feature **in `bindings/python/Cargo.toml`** (the feature belongs to the
     binding, not to the shared crate, so the zero-FFI-deps rule is untouched) and hand the `BigInt`
     straight to Python — **recommended**; or
  2. if that feature is unavailable or abi3-incompatible on the pinned pyo3 `0.23` (`Cargo.toml:152`),
     fall back to `int.from_bytes(bigint.to_signed_bytes_be(), "big", signed=True)`. The round trip is
     deliberate and cheap: the shared fn owns **normalisation** (empty ⇒ 0, sign extension, minimal
     encoding), which is the part that drifts; the byte→int step is a Python builtin, not a second
     implementation of the math.

  Route 1 vs route 2 is an implementation detail decided by whether the feature compiles, not a
  design fork — both satisfy the same spec requirement and the same vectors.

Returning **`BigInt`** rather than `(bool, Vec<u64>)` as the core is deliberate: `BigInt` is the
lossless, testable, FFI-neutral value; the words form is a projection of it. The reverse (words as the
core) would force the Python side to reassemble a number from napi's private representation.

## D3. INET — share the dispatch and the error, not the formatting

Python does **not** format an address string: `bindings/python/src/value.rs:570-595` passes the packed
bytes to `ipaddress.IPv4Address` / `IPv6Address`. So the genuinely shared part is the **4/16 length
dispatch plus the error**, and the API is two functions over one error type:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InetKind { V4, V6 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InetError { pub len: usize }
impl std::fmt::Display for InetError {
    // THE single spelling of "Invalid inet address length: {n} (expected 4 or 16)"
}

pub fn inet_kind(bytes: &[u8]) -> Result<InetKind, InetError>;
pub fn inet_bytes_to_string(bytes: &[u8]) -> Result<String, InetError>;
```

Node calls `inet_bytes_to_string`. Python calls `inet_kind` to select the `ipaddress` class and, on
`Err`, renders `InetError` with `Display`. **The error message stops being hand-copied** — #1453 aligned
the two bindings by literally duplicating the string into both files (`value.rs:461` and
`value.rs:591`), which is the same "same fact written twice" shape this change exists to remove.

The `#[cfg(test)] fn inet_to_string` at `bindings/python/src/value.rs:599` (a *third* inet formatter,
test-only, with a hex fallback that contradicts the production fail-closed behaviour) is **deleted**;
its two unit tests move to the shared crate where they test the production path.

## D4. OTel keys — move the list, and give it a second enforcing consumer

```rust
pub const KNOWN_OTEL_KEYS: &[&str] = &[
    "enabled", "endpoint", "protocol", "service_name",
    "service_version", "sampling_ratio", "timeout_ms",
];
```

Python's `bindings/python/src/observability.rs:85` `KNOWN_KEYS` is replaced by this import; the
rejection behaviour (unknown key ⇒ `ValueError` naming the recognised keys) is unchanged.

**A constant with one consumer is not "shared by construction" — it is a relocated private.** So Node
gains a `#[cfg(test)]` assertion that the snake_case field names of `OtelOptions`
(`bindings/node/src/observability.rs:45-82`) are **exactly** the set in `KNOWN_OTEL_KEYS`. Adding a
field to one and not the other then fails a test instead of shipping a binding-visible asymmetry.
Node's field names are snake_case in Rust and camelCase in JS via `#[napi(js_name)]`; the list is the
**Rust/Python snake_case** spelling and the crate docs say so, because Python's dict keys are the
snake_case form.

**Not unified:** the validation *mechanism*. Python rejects unknown keys; napi silently drops them.
`src/lib.rs` records this divergence explicitly so a future reader does not "fix" it by accident. Both
bindings already delegate `OtelProtocol::parse` and the sampling clamp to `cqlite_core::observability`
(`observability.rs:118` Python, `observability.rs:110` Node) — that stays as-is; it is already shared.

## D5. The #1451 error table — a MOVE, with no re-export left behind

**Ordering taken: #1451 merged first (PR #3378), so this change MOVES the table; it does not define
it.** The table is `cqlite-core/src/ffi_error_contract.rs` (389 lines), declared unconditionally at
`cqlite-core/src/lib.rs:17`, exporting `PyExceptionClass`, `FfiErrorRow`, `FfiErrorVariant` (with
`ALL`, `row()`, `from_name()`, `sample_error()`), `variant_of()` and `contract_for()`. Its module doc
at `:14-20` already pre-authorises this exact move (*"this module moves there verbatim and the
bindings re-point their import; nothing else changes. It is deliberately a top-level module … so that
move is a file move"*).

- File → `cqlite-ffi-common/src/error_contract.rs`. The `ffi_` prefix is dropped: inside a crate named
  `cqlite-ffi-common` it is redundant, and `cqlite_ffi_common::error_contract::contract_for` reads
  better than `…::ffi_error_contract::…`. The **type names are unchanged** (`FfiErrorRow`,
  `FfiErrorVariant`), so the diff in the bindings is import lines only.
- Test → `cqlite-ffi-common/tests/error_contract_table.rs` (all 12 tests, unchanged).
- `pub mod ffi_error_contract;` is **removed** from `cqlite-core/src/lib.rs`.
- Consumers updated: `bindings/python/src/error.rs:25`, `bindings/python/src/lib.rs:108`,
  `bindings/node/src/error.rs:65`, `bindings/node/src/lib.rs:83,115`, and the prose reference at
  `bindings/node/README.md:284`. A repo-wide grep confirms there are **no others**.

**No deprecated re-export from `cqlite-core`.** Two paths to one item is the failure shape this change
removes; a `pub use` would let a future consumer bind the old path and quietly resurrect the
"which import did you mean" problem that #1705 documents (D8). `cqlite-core` is pre-1.0, the module is
three weeks old, and the only consumers in existence are in this repo and updated in this diff. The
removal is recorded in the PR body and `CHANGELOG.md`.

Note for the reviewer: the gate's `pub-surface` component (#1712) asserts *declaration/inner-`cfg`
consistency*, not public-API drift — **nothing in this repo detects a public-API change** (the
principled route is #3366). So this removal will not be flagged mechanically; it is called out in
prose deliberately.

## D6. "Zero pyo3/napi deps" must be measured, not asserted

A manifest-text check answers only *direct* dependencies, and a positive verdict from the absence of a
bad signal is exactly the shape CLAUDE.md forbids. So `cqlite-ffi-common/tests/dependency_boundary.rs`
takes an **affirmative transitive measurement**: it invokes `$CARGO metadata --format-version 1
--locked` (cargo sets `CARGO` in the test environment), resolves the dependency closure **rooted at the
`cqlite-ffi-common` package**, and asserts that no resolved package name is or begins with `pyo3`,
`napi`, or `napi-derive`.

Fail-closed properties, stated because each is a place a guard like this usually goes vacuous:
- `CARGO` unset, `cargo metadata` failing, non-zero exit, unparseable JSON, or the crate's own node not
  found in the resolve graph ⇒ **test failure** naming what could not be measured. There is no
  "could not check, assume fine" branch and no env opt-out.
- The assertion is on the **resolved closure**, not on the manifest, so a transitively-introduced
  `pyo3` fails even though no manifest in this repo mentions it.
- The test additionally asserts the closure is **non-empty and contains `cqlite-core`**, so a resolve
  that silently returned nothing cannot pass vacuously.

## D7. Making "single implementation" an assertion, not a comment

Unit tests inside `cqlite-ffi-common` prove the shared fns are *correct*. They do **not** prove a
binding actually *calls* them — a binding could keep a private copy and stay green, which is precisely
the pre-change state. So:

`cqlite-ffi-common/src/vectors.rs` exports canonical, committed `(input, expected-rendering)` vectors:

```rust
pub struct DecimalVector { pub name: &'static str, pub scale: i32,
                           pub unscaled: &'static [u8], pub expected: Result<&'static str, &'static str> }
pub const DECIMAL_VECTORS: &[DecimalVector];
pub const VARINT_VECTORS: &[VarintVector];   // bytes -> canonical decimal string
pub const INET_VECTORS:   &[InetVector];     // bytes -> Ok(text) | Err(message)
```

Each binding exposes one internal, underscore-prefixed test-support surface that renders **every**
vector through its **production** conversion path (the same pattern already established by
`_decimal_from_parts`, `_inet_from_bytes`, `_raise_mapped_core_error`, `_errorContractProbe`), and each
suite asserts rendered == expected for the whole table. Because both suites read the *same committed
table*, a divergence — or a re-introduced local implementation in either binding — fails **both**
suites. This is the mechanised form of the issue's *"add one assertion per binding that a
decimal/varint/inet value matches the other binding's known output"*, strengthened from one spot-check
to the full table.

The vectors are ordinary `pub const` data (no feature gate): they are inert, tiny, and gating them
behind a `cfg(test)`/feature would make them unreachable from the bindings' own test builds, which is
the entire point.

## D8. The three `ErrorCategory` enums (#1705) — recommended, but the owner's call

`cqlite-core` has three enums named `ErrorCategory`: `error.rs:563` (15 variants, returned by
`Error::category()`, mirrored in the FFI table), `observability/error_schema.rs:70` (12 variants, the
telemetry taxonomy) and `cql/error.rs:415` (6 variants, CQL-local). The first two are
error-handling-relevant, are distinguished **only by import line**, are both stored in a struct field
named `category`, and genuinely disagree (`QueryTimeout` is `Query` in one and `Timeout` in the other).

This change makes the FFI one a **cross-crate** path (`cqlite_ffi_common::error_contract` re-exporting
`cqlite_core::error::ErrorCategory`), which changes the footgun's shape without removing it.

**Recommendation: rename the telemetry one only — `observability::error_schema::ErrorCategory` →
`ObsErrorCategory` — inside this change.** Reasons: it has the smaller call-site count and no FFI
consumers; `error::ErrorCategory` is what `Error::category()` returns and is referenced across the
CLI, Flight and the bindings, so renaming *it* is a large public-API break bought for no extra safety
once its sibling is unambiguous; and one of the two being uniquely named is sufficient to make a wrong
import fail to compile rather than mis-classify. `cql/error.rs`'s is module-local and not confusable
with either — leave it.

This is Open Question 1: it is a scope decision on someone else's issue (#1705), so it is surfaced, not
taken.

## D9. `error_schema::classify()` vs `Error::category()` (epic #1686) — explicitly deferred

Epic #1686 capstone §3 states that `error_schema::classify()` is *"the authority the language bindings'
error tables derive from."* **The code does not do that** — the FFI table mirrors `Error::category()`,
and `cqlite-core/tests/ffi_error_contract_table.rs:77` pins that mirroring. #1705 removed the claim
from three places in core rather than propagate it.

If the answer is "yes, derive from `classify()`", the FFI table's `category` column changes value for
at least `QueryTimeout` (`Query` → `Timeout`) and `Timeout` (`System` → `Io`), which is a
**binding-visible** change to `error.category` in Node — i.e. a different change with a different
blast radius, not a detail of this move. This change therefore **moves the table as-is** and defers the
question (Open Question 2). Nothing here forecloses either answer: after the move the table is in one
file with one test, which is a strictly better place to answer it from.

## D10. What is deliberately NOT extracted, and why the verification matters

The issue body hedged DURATION and DATE (*"only move what is genuinely duplicated"*). Verified:
`duration_to_object` (`bindings/node/src/value.rs:434`) and Python's `Duration` pyclass
(`value.rs:225`) both pass `(months, days, nanos)` straight through with **no arithmetic**; the DATE
arms (`value.rs:199` Node, `value.rs:194` Python) compute *different* things for *different* target
types (JS `Date` at midnight UTC vs `datetime.date`). Extracting either would create a helper with one
caller apiece and a shared-code claim with nothing behind it. Recorded here so the omission reads as a
finding rather than an oversight.
