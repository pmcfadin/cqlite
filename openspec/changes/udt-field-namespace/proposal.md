# Proposal: carry UDT type identity out of band in both bindings

**Issue**: #3504. **Milestone**: maintenance (bindings correctness), 0.15 line.
**Routing**: **design-driven**. There is no oracle for this. The Cassandra format and
`sstabledump` say nothing about how a *language binding* should surface a UDT's type identity —
that is a public-surface design choice with real latitude, and the issue's own AC1 is *"a decision
among (a)/(b)/(c) recorded with rationale"*. Hence an OpenSpec change rather than a pinned parity
test. (Contrast: the *decode* of a UDT is oracle-driven and is not touched here.)

## Problem

Both bindings render a UDT by injecting `_type` and `_keyspace` as ordinary keys into the **same
namespace as the UDT's own field names**, metadata first and fields second. A UDT field named
`_type` or `_keyspace` — legal CQL via a quoted identifier — silently overwrites the injected
metadata. The type name becomes unrecoverable from the result, and every downstream consumer that
keys on `_type` to recognise a UDT is then reading user data.

This is the control/data channel-sharing shape `CLAUDE.md` records as the umbrella lesson of #3312:
a control marker placed in a namespace the data controls. Two of the four sites in that class are
in scope here:

| Site | Namespace | In scope |
|---|---|---|
| 1 — row dict | column names | No — fixed in #1454/PR #3498 via the explicit `is_row_level` signal |
| 2 — cell-level map | map keys | No — owned by #3497 (needs the declared type); this change must leave it no worse, and hands it a signal it lacked |
| **3 — UDT fields** | field names | **Yes** — field overwrites injected metadata in BOTH bindings |
| **4 — UDT-as-map-key projection** | field names | **Yes** — `value_to_hashable_key`'s `Udt` arm emits pairs for `_type`, `_keyspace`, then each field, so a colliding field yields a DUPLICATE pair in the projected `frozenset` |

## Decision (AC1)

**Option (a) — carry type identity out of band, in a dedicated UDT type — is adopted.** Rationale,
in the order the alternatives fail:

- **(b) "a reserved key that cannot collide" is not expressible.** Any string is a legal quoted CQL
  identifier, so no key is unreachable by data. There is nothing to implement.
- **(b′) "require BOTH `_type` and `_keyspace` before treating a dict as a UDT" was already
  considered and REJECTED on the merged precedent.** PR #3498 declined exactly this narrowing: a UDT
  can have both fields and a legal map can carry both keys, so it is *a rarer delimiter on a channel
  the data controls*. `CLAUDE.md`: remove the shared channel, do not pick a rarer delimiter — each
  narrowing only postpones the next instance. Re-adopting it here would relitigate a settled call.
- **(c) reject at decode time when a UDT declares a field named `_type`/`_keyspace`** is fail-closed
  and cheap, and it is a genuine improvement over silent corruption. But it **refuses data Cassandra
  accepts and the CLI already reads correctly** — it converts a rendering defect into a capability
  regression, and it is permanent: there is no later state in which such a UDT becomes readable
  through the bindings. A stopgap whose cost is a permanent hole in the read surface is worse than
  the fix.
- **(a) is the only option that removes the channel**, which is precisely what the doctrine the
  issue cites requires. The issue body reaches the same conclusion.

**The cost (a) is charged with — a breaking change to the public binding surface — is bounded, and
is paid only by consumers of the marker itself.** Fields keep being addressable by name in both
bindings; every UDT that does *not* declare a colliding field keeps working through the field-access
path. What stops working is reading the *metadata* out of the field namespace (`udt["_type"]` /
`udt._type`) — i.e. exactly the shared channel being removed. That is not incidental breakage; it is
the deliverable. Both bindings are pre-1.0 (M6/M7 precede the v1.0 API freeze), which is when such a
change is cheapest to make.

## Non-goals

- **Type-aware normalization / the cell-level map ambiguity (site 2, instance `b-2`)** — #3497. This
  change makes a UDT structurally distinguishable from a `dict`/plain object, which is the signal
  #3497 lacks, but it does not rewrite the normalizer's type dispatch.
- **Making `contains_udt` / `value_to_hashable_key` total** (missing `Tuple`/`Set` arms → `TypeError`
  on nested UDTs) — #3500. Site 4 here fixes the *duplicate-pair* defect in the existing `Udt` arm
  only; it does not add the missing arms. It does, however, INCIDENTALLY resolve the sub-family of
  #3500's failures whose only cause was a UDT rendering as an unhashable `dict` — measured, with the
  half that still fails and why, in `design.md` ("Note for #3500").
- **The 3-way Python/Node/CLI golden parity harness** — #1455.
- **Any change to CQL→host conversion for non-UDT types**, to the decode path, or to `Value::Udt`
  itself in `cqlite-core`.
- **CLI output — and NOT because it is correct.** Recon found the CLI *does* inject `_type`
  (`cqlite-cli/src/output/json.rs`, plus a second independent copy in
  `cqlite-core/src/query/result.rs`'s `ToJson for Value`), so it carries the same site-3 collision for
  `_type`; it is correct-by-omission only for `_keyspace`. The issue body's "the CLI injects nothing"
  holds for `_keyspace` alone. Excluded here because (1) CLI JSON shape is a separate public surface
  with its own compatibility call, and (2) the CLI is the comparison **oracle** for the binding parity
  tests — moving an oracle and its subject in one diff is how a guard goes blind. Raised on the thread
  as a proposed follow-up.

## Impact

- **Public binding surface (Python + Node): CHANGED.** This is the point of the change; the new
  shape is specified in `specs/udt-type-identity/spec.md` and must be symmetric across bindings
  (AC3). Stub declarations (`.pyi`, `index.d.ts`) and the #1456 stub-fidelity drift alarms are part
  of the diff.
- **No-heuristics mandate (#28): strictly improved.** The current shape *requires* content sniffing
  to recognise a UDT (`"_type" in value`); a dedicated type replaces that with an authoritative
  structural signal. No new inference is introduced.
- **Memory budget (<128MB): unaffected.** Per-value representation change at the binding boundary
  only; no new buffering. `Value::Udt` in core is untouched (it stays boxed per the
  `value-representation` spec).
- **Docs**: `docs/development/M4_spec.md` §5.3 — the class entry narrows to what survives (AC5).
