# binding-shared-scalar-math — delta for extract-cqlite-ffi-common (issue #1452)

**Architecture note (read this first).** The byte-level math that renders a CQL scalar — DECIMAL,
VARINT, INET — and the mapping from a core `Error` variant to a binding-visible error identity are
each written twice, once in `bindings/python/src` and once in `bindings/node/src`. Two copies drift
because nothing forces them to agree; that mechanism produced #1450, #1451 and #1453, and it is
**still live**: #1741 (Python) and #1754 (Node) hardened DECIMAL independently and the two
implementations now disagree on observable output for inputs both accept.

This delta introduces `cqlite-ffi-common`, a pure-Rust workspace member with **no FFI framework
dependency at any depth**, holding exactly one implementation of each shared routine, and requires
that both bindings reach their rendered output *through* it. The single-implementation property is
required to be **asserted**, not asserted-about: a committed vector table exported by the shared crate
is rendered through each binding's production path and compared in both suites, so a re-introduced
local copy fails both.

Requirements are stated so that each is verifiable from a **public or test-support surface** of a
binding, or from a mechanical measurement — never from the presence of a helper unit test alone
(wiring evidence, #949/#963).

**Issue-step → requirement map** (issue #1452's numbered steps and its Definition of Done):

| Issue step / DoD item | Requirement(s) |
|---|---|
| 1 — create `cqlite-ffi-common` as a workspace member, no pyo3/napi | ADDED *A pure shared FFI crate exists as a workspace member*; ADDED *The shared crate depends on no FFI framework at any depth* |
| 2 — move the pure byte-math (decimal, varint, inet; duration/date only if duplicated) | ADDED *DECIMAL rendering has exactly one implementation and one policy*; ADDED *VARINT decoding has exactly one implementation*; ADDED *INET length dispatch and its error have exactly one implementation*; ADDED *Routines that are not duplicated are not extracted* |
| 3 — error table, coordinated with #1451 | ADDED *The FFI error contract has exactly one home and `cqlite-core` no longer exports it* |
| 4 — OTel known-key list only, mechanism not unified | ADDED *The OTel known-key list is shared and has an enforcing consumer in each binding* |
| 5 — wire both bindings, thin adapters | ADDED *Both bindings render shared scalars through the shared crate*; ADDED *Non-goals remain untouched* |
| Tests — unit tests in the crate + cross-binding "same shared fn" invariant | ADDED *A committed vector table makes the cross-binding invariant an assertion* |
| DoD — gate PASS, `-D warnings`, no `unwrap()`/`expect()`, file-size ratchet | ADDED *The change is certified by the agent gate under the repository's code-quality rules* |

## ADDED Requirements

### Requirement: A pure shared FFI crate exists as a workspace member

A crate named `cqlite-ffi-common` SHALL exist at the repository root as a member of the root
`Cargo.toml` `[workspace] members` list, SHALL inherit `[lints] workspace = true` like its sibling
crates, and SHALL be built, linted (`RUSTFLAGS="-D warnings"`) and tested by the default workspace
commands and by `scripts/agent-gate.sh` without any additional flag or feature.

Its module files SHALL be split by responsibility (decimal, varint, inet, error contract, OTel keys,
vectors) and SHALL each stay well under the ~800-line source target (#1116), so the file-size ratchet
is satisfied by construction rather than by an allowance.

#### Scenario: The crate is a first-class workspace member
- **GIVEN** a clean checkout at the change's head
- **WHEN** `cargo metadata --format-version 1 --no-deps` is read
- **THEN** a package named `cqlite-ffi-common` is present in `workspace_members`
- **AND** `cargo test -p cqlite-ffi-common` runs the crate's tests without extra features
- **AND** `cargo clippy -p cqlite-ffi-common --all-targets` under `RUSTFLAGS="-D warnings"` emits no warning

#### Scenario: The crate carries no library `unwrap`/`expect`
- **GIVEN** the crate's `src/**` (excluding `#[cfg(test)]` modules)
- **WHEN** the code is reviewed and compiled
- **THEN** no `unwrap()` or `expect()` call appears in library code, every fallible path returning a typed error

### Requirement: The shared crate depends on no FFI framework at any depth

`cqlite-ffi-common` SHALL NOT depend on `pyo3`, `napi`, or `napi-derive` — directly, transitively, as a
dev-dependency, or as a build-dependency. This SHALL be enforced by an **affirmative measurement of the
resolved dependency closure**, not by an inspection of manifest text and not by the absence of a
failure signal.

The check SHALL be fail-closed in every direction: an unavailable `cargo`, a non-zero `cargo metadata`
exit, unparseable output, an empty resolve graph, or the crate's own node not being found SHALL each
FAIL the check with a message naming what could not be measured. No environment variable SHALL be able
to skip or soften it.

#### Scenario: The resolved closure is measured and is FFI-free
- **GIVEN** the `cqlite-ffi-common` package
- **WHEN** its dependency-boundary test resolves the closure rooted at that package via `cargo metadata`
- **THEN** no resolved package name is, or begins with, `pyo3`, `napi`, or `napi-derive`
- **AND** the closure is non-empty and contains `cqlite-core`, so an empty resolve cannot pass vacuously

#### Scenario: An FFI dependency introduced at any depth fails the check
- **GIVEN** a scratch copy of the crate whose manifest adds a dependency that pulls in `pyo3`
- **WHEN** the dependency-boundary test runs against it
- **THEN** the test FAILS and its message names the offending package

#### Scenario: An unmeasurable closure fails rather than passes
- **GIVEN** an environment in which `cargo metadata` cannot be run or returns unparseable output
- **WHEN** the dependency-boundary test runs
- **THEN** the test FAILS naming the measurement that could not be taken, and does not report success

### Requirement: DECIMAL rendering has exactly one implementation and one policy

`cqlite-ffi-common` SHALL expose `decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String, DecimalError>`
as the ONLY implementation of CQL `decimal` rendering in the repository's binding surface, and both
bindings SHALL obtain their rendered digits from it. Neither binding SHALL retain a local
digit-split/scale/sign implementation.

The rendering **policy** — the magnitude ceiling above which a value is refused as corrupt, and the
conditions under which exponent form is used instead of positional — SHALL be defined once, in the
shared crate, as named `pub const` limits, and SHALL be **identical for both bindings**. It is
currently NOT: for a well-formed 2000-byte unscaled magnitude with `scale = 3`, Node renders exponent
form while Python raises a typed corruption error; for `scale = i32::MIN` Node renders while Python
raises. After this change no input SHALL render in one binding and raise in the other.

Refusal SHALL remain fail-closed and typed: an over-ceiling magnitude SHALL produce a `DecimalError`
that each binding maps onto `cqlite_core::Error::corruption` and thence through its existing production
error path, so the resulting Python exception class and JS `error.code` still come from the one FFI
error contract. Rendering below the ceiling SHALL be infallible, so no well-formed value can abort or
panic (preserving the #1741/#1437/#1440 abort-safety guarantee).

The shared crate SHALL unit-test at minimum: empty `unscaled`; `scale == 0`; positive `scale` shorter
than, equal to, and longer than the digit count; negative `scale`; `scale == i32::MIN` and
`scale == i32::MAX`; negative unscaled values in each of those shapes; a magnitude at the positional
boundary and one just past it; and a magnitude at and just past the refusal ceiling.

#### Scenario: The same decimal renders identically in both bindings
- **GIVEN** the committed DECIMAL vector table exported by `cqlite-ffi-common`
- **WHEN** each vector is rendered through the Python binding's production path and through the Node binding's production path
- **THEN** both produce the vector's expected rendering, character for character
- **AND** for a vector expected to be refused, both raise a typed corruption error carrying the same message

#### Scenario: A large well-formed magnitude has one outcome, not two
- **GIVEN** a 2000-byte well-formed unscaled magnitude with `scale = 3`
- **WHEN** it is rendered through both bindings
- **THEN** both render it, or both refuse it, according to the single documented policy — never one each

#### Scenario: A pathological magnitude is refused, not rendered, in both bindings
- **GIVEN** an unscaled magnitude beyond the documented ceiling
- **WHEN** it is rendered through either binding
- **THEN** a typed corruption error is raised naming the scale, the unscaled length and the ceiling
- **AND** the process does not abort and no uncatchable interpreter-level error escapes

### Requirement: VARINT decoding has exactly one implementation

`cqlite-ffi-common` SHALL expose the single implementation of the CQL `varint` semantic — big-endian
two's complement, an empty slice meaning zero, sign extension for any byte length — as
`varint_to_bigint(bytes: &[u8]) -> num_bigint::BigInt`, together with
`varint_to_sign_and_le_words(bytes: &[u8]) -> (bool, Vec<u64>)` expressing that same value in the
sign-magnitude little-endian `u64` form napi's `create_bigint_from_words` requires.

The Node binding SHALL construct its `BigInt` from the words form and SHALL NOT retain its hand-rolled
padding / word-assembly / two's-complement-negate loop. The Python binding SHALL obtain its integer
from the shared function's value, so the empty-input and sign-extension semantics are decided in one
place. Both `varint_to_bigint` and `varint_to_sign_and_le_words` SHALL agree on every input by
construction (the words form is derived from the `BigInt`, never computed independently).

The shared crate SHALL unit-test at minimum: empty input; a single positive byte; a single negative
byte; a value at exactly 8 bytes; values of 9 and 17 bytes in both signs (crossing the word boundary
that the old Node path special-cased); `i64::MIN` and `i64::MAX` encodings; and a value whose
magnitude requires the carry to propagate across a word boundary during negation.

#### Scenario: The same varint yields the same integer in both bindings
- **GIVEN** the committed VARINT vector table exported by `cqlite-ffi-common`
- **WHEN** each vector is decoded through the Python binding's production path and through the Node binding's production path
- **THEN** the decimal string of the resulting Python `int` and of the resulting JS `BigInt` both equal the vector's expected value

#### Scenario: The words projection never disagrees with the BigInt
- **GIVEN** any varint byte string in the crate's test set, including multi-word negative values
- **WHEN** `varint_to_sign_and_le_words` and `varint_to_bigint` are both evaluated
- **THEN** reassembling the sign and words reproduces the `BigInt` exactly

### Requirement: INET length dispatch and its error have exactly one implementation

`cqlite-ffi-common` SHALL expose the single decision of what a CQL `inet` byte string is — 4 bytes
IPv4, 16 bytes IPv6, anything else a typed error — as `inet_kind(bytes) -> Result<InetKind, InetError>`
and `inet_bytes_to_string(bytes) -> Result<String, InetError>`.

The malformed-length message SHALL have exactly one spelling in the repository, produced by
`InetError`'s `Display`. Both bindings SHALL derive their user-visible message from it; neither SHALL
contain a literal copy of the message text. Neither binding SHALL contain a passthrough or
hex-fallback branch for a malformed length (no-heuristics, #28): the only outcomes are IPv4, IPv6, and
a typed error.

The Python binding's `#[cfg(test)]`-only third inet formatter — which today carries a hex fallback
contradicting the production behaviour — SHALL be removed, its coverage moving to the shared crate
where it exercises the production path.

#### Scenario: Both bindings render the same address text
- **GIVEN** the committed INET vector table exported by `cqlite-ffi-common`, containing 4-byte and 16-byte values
- **WHEN** each vector is converted through the Python binding's production path and through the Node binding's production path
- **THEN** the Python `ipaddress` object's string form and the Node string are equal to the vector's expected text

#### Scenario: A malformed length errors identically in both bindings
- **GIVEN** an `inet` byte string of length 5
- **WHEN** it is converted through either binding
- **THEN** each raises its typed parse error and both messages are character-identical
- **AND** neither returns the raw bytes, a hex string, or any other passthrough

### Requirement: The FFI error contract has exactly one home and `cqlite-core` no longer exports it

The #1451 FFI error contract — `PyExceptionClass`, `FfiErrorRow`, `FfiErrorVariant` (including `ALL`,
`row`, `from_name`, `sample_error`), `variant_of` and `contract_for` — SHALL live in
`cqlite-ffi-common` and nowhere else. `cqlite-core` SHALL NOT declare the module and SHALL NOT provide
a re-export, deprecated or otherwise: there SHALL be exactly one import path to these items.

The move SHALL be behaviour-preserving. The table's rows, the exhaustive `variant_of` match over
`cqlite_core::Error`, the fail-closed `from_name`, and the compile-time obligation that a new core
`Error` variant cannot be added without giving it a row SHALL all be carried over unchanged, and the
contract's existing test suite SHALL move with it and continue to pass unmodified.

The `category` column SHALL continue to mirror `cqlite_core::error::ErrorCategory` as returned by
`Error::category()`. Re-keying the table onto `observability::error_schema::classify()` is out of scope
for this change.

#### Scenario: There is exactly one import path to the contract
- **GIVEN** the repository at the change's head
- **WHEN** every reference to the contract's items is enumerated
- **THEN** all of them resolve through `cqlite_ffi_common`, and no `ffi_error_contract` module is declared or re-exported by `cqlite-core`

#### Scenario: Every core error variant keeps its binding identity
- **GIVEN** each `FfiErrorVariant` and its representative core `Error`
- **WHEN** the error is raised through the Python binding's production path and through the Node binding's production path
- **THEN** the Python exception class equals the row's `py_class` name and the JS `code`, `category` and `isRecoverable` equal the row's columns
- **AND** the row's `category`/`recoverable` still equal `Error::category()` / `Error::is_recoverable()` for that variant

#### Scenario: A new core error variant still cannot ship unmapped
- **GIVEN** a scratch tree adding a variant to `cqlite_core::Error` with no contract row
- **WHEN** the workspace is compiled
- **THEN** compilation FAILS in the contract's exhaustive match

### Requirement: The OTel known-key list is shared and has an enforcing consumer in each binding

`cqlite-ffi-common` SHALL export `pub const KNOWN_OTEL_KEYS: &[&str]` holding the snake_case option
names both bindings accept. The Python binding SHALL consume it in place of its local `KNOWN_KEYS`
allowlist, preserving its current behaviour exactly: a non-string key and an unrecognised key each
raise `ValueError`, and the unrecognised-key message names the recognised keys.

The Node binding SHALL gain a test asserting that the snake_case field names of its `OtelOptions`
napi object and `KNOWN_OTEL_KEYS` are the **same set**, in both directions, so the list has an
enforcing consumer in each binding rather than being one binding's private constant with a new address.

The validation **mechanism** SHALL NOT be unified — Python rejects unknown keys, napi drops them — and
the shared crate's documentation SHALL record that divergence so it is not "fixed" by accident. Protocol
parsing and the sampling clamp SHALL continue to be delegated to `cqlite_core::observability`.

#### Scenario: An unknown Python otel_config key is still rejected, naming the shared list
- **GIVEN** `Database.open(..., otel_config={"endpint": "http://x"})`
- **WHEN** the call is made
- **THEN** a `ValueError` is raised naming the unrecognised key and listing the keys from `KNOWN_OTEL_KEYS`

#### Scenario: A field added to only one side fails a test
- **GIVEN** a scratch tree adding a field to `OtelOptions` without adding its name to `KNOWN_OTEL_KEYS` (or the reverse)
- **WHEN** the Node binding's tests run
- **THEN** the set-equality assertion FAILS, naming the asymmetric key

### Requirement: Both bindings render shared scalars through the shared crate

Each binding SHALL depend on `cqlite-ffi-common` by path and SHALL keep only a **thin adapter** for
each shared routine: bytes → shared function → Py/JS object. The adapter SHALL contain no
digit-splitting, no sign handling, no word assembly, no address formatting, and no length dispatch.

This SHALL be evidenced from the bindings' own public/test-support surfaces — an internal unit test in
the shared crate is explicitly NOT sufficient evidence that a binding calls it (#949/#963).

#### Scenario: A binding's rendered output tracks a change made only in the shared crate
- **GIVEN** a scratch tree in which a shared rendering function's output is deliberately altered
- **WHEN** each binding's vector assertions run
- **THEN** both suites FAIL, demonstrating that each binding's production output flows from the shared implementation

#### Scenario: The existing binding suites stay green on the shared implementation
- **GIVEN** `bindings/python/tests` and `bindings/node/__test__` unchanged except for additive vector assertions
- **WHEN** both suites run against a build of the rewired bindings
- **THEN** every pre-existing type, error, parity and abort-safety test passes
- **AND** no dataset-backed test passes on present-but-empty data

### Requirement: A committed vector table makes the cross-binding invariant an assertion

`cqlite-ffi-common` SHALL export committed canonical vector tables for DECIMAL, VARINT and INET, each
entry pairing an input with its single expected rendering (or its single expected error message). The
tables SHALL be ordinary public data, reachable from the bindings' test builds without a feature flag.

Each binding SHALL expose one internal, underscore-prefixed test-support surface that renders **every**
entry through that binding's production conversion path, and each binding's suite SHALL assert the full
table. Because both suites read the same committed table, a divergence between the bindings — or a
re-introduced local implementation in either — SHALL fail both suites.

The tables SHALL cover, at minimum, every edge case enumerated in the DECIMAL, VARINT and INET
requirements above, including at least one entry whose expected outcome is a typed error.

Every entry's check SHALL be EXACT. An entry whose rendering is short enough to commit verbatim SHALL
be compared character for character. An entry whose rendering is too long to commit verbatim (the
multi-kilobyte boundary magnitudes) SHALL carry, beside its readable collapsed form, the lower-case
SHA-256 hex of the UTF-8 bytes of the **full** expected rendering, and each suite SHALL compare that
hash against the hash of the **full** rendering its own production path produced — never against the
collapsed form alone, which pins only the digit COUNT and so would pass two bindings emitting
different digits of the same length.

The pairing SHALL be structural: it SHALL be impossible to commit a collapsed expectation with no
exact oracle beside it, and a suite SHALL be able to assert that every value entry carries one.

The hash SHALL NOT be exported as a shared routine. Each of the three sides — this crate's own test,
the Python suite, the Node suite — SHALL compute it with its own standard library, so agreement is
evidence of three independent implementations meeting one committed constant rather than of one
shared function agreeing with itself. Because the encoding is fixed (UTF-8 bytes, lower-case hex) and
every rendering is ASCII, no side has an encoding choice to make.

#### Scenario: A multi-kilobyte rendering is checked digit-for-digit, not by digit count
- **GIVEN** a committed DECIMAL entry whose expected rendering is thousands of digits long, committed as a collapsed form plus a SHA-256 of the full rendering
- **WHEN** the Python and Node suites render it through their production paths
- **THEN** each hashes the FULL rendering it produced with its own standard library and asserts the committed hex
- **AND** the collapsed form is compared only as the readable half of a failure message, so a rendering with the right digit count but different digits cannot pass

#### Scenario: No value entry can be checked by digit count alone
- **GIVEN** the exported vector tables
- **WHEN** a suite enumerates every entry whose expected outcome is a rendering
- **THEN** each such entry is either committed verbatim or carries a SHA-256 of its full rendering, and the two cases are distinguishable without inspecting the text

#### Scenario: Both suites assert the whole table, not a spot check
- **GIVEN** the exported vector tables
- **WHEN** the Python and Node suites run
- **THEN** each asserts one expectation per entry, and adding an entry to the shared crate causes both suites to cover it with no per-binding edit

#### Scenario: A binding that re-grows a private implementation is caught
- **GIVEN** a scratch tree in which one binding renders DECIMAL with a private copy that differs on an edge case in the table
- **WHEN** that binding's suite runs
- **THEN** the vector assertion FAILS on that entry

### Requirement: Routines that are not duplicated are not extracted

The change SHALL NOT extract `duration` or `date` helpers. Verification records that both bindings pass
`(months, days, nanos)` through with no arithmetic, and that their `date` arms compute different
results for different target types (a JS `Date` at midnight UTC vs a `datetime.date`). Extracting
either would produce a helper with one caller apiece and a shared-code claim with nothing behind it.

The reason SHALL be recorded in the change's design so the omission reads as a finding rather than an
oversight, and so a later reader does not "complete" the extraction by adding single-caller helpers.

#### Scenario: No single-caller helper is introduced
- **GIVEN** the shared crate at the change's head
- **WHEN** its public surface is enumerated
- **THEN** every exported routine has at least one caller in each of the two bindings, or is documented vector/constant data

### Requirement: Non-goals remain untouched

The change SHALL NOT modify the value dispatch bodies (`value_to_py`, `value_to_napi`) other than to
replace the shared-routine call sites; SHALL NOT modify either binding's `runtime.rs`; SHALL NOT modify
`StreamingIterator` or the napi `AsyncTask`s; SHALL NOT alter `Error::category()` or
`Error::is_recoverable()` semantics; and SHALL NOT change the CLI, Flight, Trino, the read path, or any
on-disk format.

Any user-visible behaviour change (the DECIMAL policy convergence) SHALL be recorded in `CHANGELOG.md`
and in the affected binding README, and SHALL be the only such change in the diff.

#### Scenario: The diff's blast radius is the stated one
- **GIVEN** the change's diff against `origin/main`
- **WHEN** the touched paths are enumerated
- **THEN** they are limited to the new crate, the two bindings' value/error/observability/lib modules and manifests, `cqlite-core`'s error-contract module and crate root, the moved test, the OpenSpec change, and documentation
- **AND** no streaming, runtime, CLI, Flight or storage source file is modified

### Requirement: The change is certified by the agent gate under the repository's code-quality rules

The change SHALL be certified by one full `scripts/agent-gate.sh` run reporting `RESULT: PASS` with
`tree-integrity: PASS`, and the SUMMARY block SHALL be recorded verbatim in the PR. The `file-size`
ratchet SHALL pass without `CQLITE_ALLOW_FILE_GROWTH`: the two binding `value.rs` files shrink and the
new crate's modules are created under the target.

#### Scenario: The gate of record passes
- **GIVEN** the change's head commit on a stable worktree
- **WHEN** the full agent gate runs
- **THEN** it emits `AGENT-GATE SUMMARY` with `RESULT: PASS` and `tree-integrity: PASS`
- **AND** the `file-size` component passes with no growth allowance
