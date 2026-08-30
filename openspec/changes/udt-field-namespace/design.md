# Design: out-of-band UDT type identity

Issue #3504. Adopted option **(a)** — see `proposal.md` for why (b)/(b′)/(c) fail.

## The one decision everything else follows from

**Type identity moves to a namespace the data cannot address; field names get a namespace of their
own.** Today one flat namespace holds both (`{_type, _keyspace, **fields}`), so a field name and a
control marker compete for the same slot and the *data* wins. After this change there is no slot to
compete for.

This is `CLAUDE.md`'s #3312 rule applied verbatim: *when a decision is made from a stream that
carries both your markers and someone else's payload, REMOVE the shared channel — do not choose a
rarer delimiter.* Every rejected alternative in the proposal is a rarer delimiter.

## The shape, per binding

Semantics are identical; spelling is per-language convention (PyO3 exposes snake_case, napi-rs
camelCases). Stating that explicitly because AC3 says the bindings must not desynchronise, and a
reviewer could otherwise read `type_name` vs `typeName` as drift.

| | Python | Node |
|---|---|---|
| type | `cqlite.Udt` (`#[pyclass]`) | plain object, `interface UdtValue` |
| type name | `.type_name` | `.typeName` |
| keyspace | `.keyspace` | `.keyspace` |
| fields | `.fields` → `dict[str, Value]` | `.fields` → `Record<string, Value>` |
| field access | `udt["street"]` (mapping protocol) | `udt.fields.street` |

**Why a `#[pyclass]` in Python and a plain object in Node — this is convention-following, not
inconsistency.** Each binding already has an established idiom for a *value* type and they differ:
Python's is `#[pyclass(module = "cqlite", frozen, eq, hash)]`, exactly as `cqlite.Duration` is
declared; Node's is a plain object / `#[napi(object)]`, and Node's own `Duration` is a hand-built
object declared as `interface Duration`, with `#[napi]` **class** reserved for handle types
(`Database`, `PreparedStatement`). Introducing a napi class for a value would be the anomaly.

**Python keeps the mapping protocol** (`__getitem__`, `__contains__`, `__iter__`, `__len__`,
`keys`/`values`/`items`) delegating to `fields`. This is deliberate breakage-reduction: `udt["street"]`
— what consumer code overwhelmingly does — keeps working, so the break is confined to reading the
*marker* out of the field namespace (`udt["_type"]`), which is the deliverable. `udt["_type"]` now
raises `KeyError` unless a field is genuinely named `_type`, in which case it returns that field's
value. That is the channel being removed, observable.

Node does **not** get top-level field access. Mirroring it would mean re-flattening fields into the
object namespace beside `typeName`/`keyspace` — reintroducing the exact defect. The asymmetry in
*ergonomics* is the price of symmetry in *semantics*, and semantics is what AC3 constrains.

## Site 4 — the projection (Python only)

`value_to_hashable_key`'s `Udt` arm currently emits a `frozenset` of `(name, value)` pairs for
`_type`, `_keyspace`, then each field — so a field named `_type` yields a **duplicate `_type` pair**
(two pairs, different values, nothing dedupes them). It is Python-only: Node's `map_to_js_map` uses a
real JS `Map` keyed by the object itself and needs no hashable projection.

The arm now emits a **`Udt` instance**, with metadata carried by the instance and only real fields in
the pair set. Requirements this must satisfy:

- **Hashable and equatable**, since it is used as a `dict` key (`map_to_py`) and a set member
  (`set_to_py`). `__hash__`/`__eq__` are over `(keyspace, type_name, fields)`, so two UDTs of
  *different* types with identical fields stay distinct — a property the old frozenset had, via the
  metadata pairs, and which must not be lost while removing them.
- **Field values in this position are already recursively projected**, so they are already hashable;
  `Udt.__hash__` therefore succeeds here. Outside the projection a `Udt` may hold unhashable field
  values (a `dict` from a nested map) and hashing raises `TypeError` — the same behaviour as a tuple
  containing a list, and a strict improvement on today's plain `dict`, which is never hashable.
- **Totality is NOT in scope.** `contains_udt`/`value_to_hashable_key` lack `Tuple` and `Set` arms
  and raise `TypeError` on some legal nested UDTs; that is **#3500**. This change fixes the
  duplicate-pair defect in the existing `Udt` arm and adds no arms.

## What this hands to #3497, without doing #3497

Cell-level site 2 (`map<text,X>` carrying a literal `"_type"` key reads as a UDT) exists because at
cell level *no structural signal distinguishes a UDT from a map* — both are `dict`. After this
change a UDT **is a distinct type**, so `isinstance(v, cqlite.Udt)` is an authoritative signal where
previously only content-sniffing existed. That is the signal #3497 needs. Consuming it — retyping the
normalizer's dispatch — is #3497's scope and is deliberately not done here; this change only stops
*producing* the ambiguity.

Consequence within scope: the test-harness normalizer's `if "_type" in value` branch no longer
matches a real UDT (production no longer emits that key), so the harness is updated to key on the
type. Leaving it sniffing would make it silently dead code that still fires on maps.

## No-heuristics (#28)

Strictly improved. The current shape *requires* sniffing to recognise a UDT; recognition becomes
structural. No new inference is introduced anywhere.

## Discovered, deliberately OUT of scope: the CLI injects `_type` too

`cqlite-cli/src/output/json.rs` (`Value::Udt` arm) and a second independent copy in
`cqlite-core/src/query/result.rs` (`ToJson for Value`) both inject `_type` then the fields, so the
CLI has the same site-3 collision for `_type` (not for `_keyspace`, which it never injects).
`M4_spec.md` §5.3's oracle table already records this: `_type` is injected by Python, Node **and**
the CLI, so only `sstabledump`/raw bytes is a valid oracle for it.

Not fixed here, for two reasons: (1) CLI JSON output is a separate public surface with its own
consumers and its own compatibility call — a product decision, not a binding-parity one; (2) the CLI
is the *comparison oracle* for the binding parity tests, and changing an oracle in the same diff as
its subject is the failure mode where a guard and the thing it guards move together and the guard
goes blind. Proposed as a follow-up on the issue thread under `coord:follow-up-proposed`.

## Rejected implementation variants

- **Keep emitting `_type`/`_keyspace` when they do not collide, omit on collision.** Maximum
  compatibility, but it makes the shape *data-dependent*: `udt["_type"]` works for most UDTs and
  `KeyError`s on exactly the pathological one. That converts a loud break into a latent,
  input-dependent bug — strictly worse than removing the key, and it keeps the channel shared.
- **Python: `Udt` subclassing `dict` with fields as its contents.** Preserves `isinstance(x, dict)`
  and `dict(udt)`. Rejected: a `dict` subclass is mutable and unhashable by default, so site 4 would
  need a second representation anyway, and `frozen`/`hash` — the `Duration` precedent, and what the
  projection requires — is not available.
- **Node: `Symbol`-keyed metadata on the existing flat object.** A `Symbol` genuinely cannot collide
  with a string field name, so it does remove the channel. Rejected for cross-binding symmetry (no
  Python analogue) and because symbol-keyed data is invisible to `JSON.stringify`, spreads and
  `Object.keys` — the parity harnesses and every doc example would need special handling to see the
  type at all.

## Where the tests can actually execute (this constrained the plan, so it is recorded)

Three candidate homes execute **nothing**, and picking one would have produced a green diff with no
evidence behind it:

- **`bindings/python/src/**` `#[cfg(test)]` — executes in NO component, anywhere.** `cargo test -p
  cqlite-py` is structurally impossible: a pyo3 `cdylib` test harness cannot link libpython, because
  the crate builds with `extension-module` and without `auto-initialize`. The ~14 existing
  `Python::with_gil` unit tests in that crate already run nowhere; they compile under clippy and stop
  there. `binding-rust-tests` names this as a census omission and only grep-counts the `#[test]`s.
- **`bindings/node/src/value_tests.rs` cannot assert the produced object.** That suite *is* executed
  (`binding-rust-tests`, which never SKIPs), but `udt_to_object` needs a `ConvCtx` holding a live napi
  `Env`, and an `Env` cannot be fabricated off-thread — the suite's own comments say it exists to test
  Env-free helpers only.
- **A corpus-rooted fixture is invisible to the binding suites.** `conftest.py` and `helpers.js` take
  the corpus from `CQLITE_DATASETS_ROOT` and never fall back to the checkout, so anything committed
  under `test-data/datasets/sstables/` is unreachable on a box where that env is set — i.e. on every
  gate run. Hence the fixture is committed **checkout-relative**, on the
  `cqlite-core/tests/fixtures/issue_2225/` precedent, where no env var can hide it.

So the collision assertions live in `bindings/python/tests/*.py` (`python-bindings`) and
`bindings/node/__test__/*.test.js` (`node-bindings`). **Both of those components SKIP when their
toolchain is absent**, so AC2's evidence rides on SKIP-able lanes. That is stated in the PR rather
than engineered around: the alternative — inventing an Env-free abstraction inside `udt_to_object`
purely to host a never-SKIP `binding-rust-tests` case — would be a harness that never reaches the
shipped code path, which is worse than a declared SKIP.

**The fixture is Cassandra-written, not CQLite-written.** Not because the round-trip warning applies
to this defect — it does not; the defect is entirely above decode, so a constructed in-memory
`Value::Udt` would be a legitimate oracle for the *rendering* rule — but because a Cassandra-written
fixture additionally proves the **decoder** can produce such a UDT at all, which a constructed value
cannot, and because it is reusable by #3497/#3500/#1455. Note also that the CQLite write path could
not supply it anyway: nothing in `cqlite-core/src/cql/` ever constructs a `CqlLiteral::Udt`, so an
`INSERT` cannot produce a UDT, and the codec hard-codes an empty keyspace for one.
