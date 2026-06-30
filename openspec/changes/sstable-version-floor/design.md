## Context

The Cassandra-5.0 support floor exists only implicitly (magic-number mismatch) and is contradicted by
dead modeling code that spans Cassandra 3.x. The audit (file:line in `proposal.md`) pins three facts
that drive the design:

1. The only thing that rejects pre-`na` today is an **untyped nom error** at header-read time, not a
   version check at descriptor time.
2. `BigVersionGates::is_compatible()` is **dead** and its predicate `v >= "ma"` *admits* `ma`–`me` —
   the exact surface that mislead reviewers on #1021.
3. The reader's `nb_fallback()` arm (`reader/mod.rs:411`) **swallows** descriptor-parse failures and
   continues as `nb`. Any floor check that lives only in descriptor parsing is silently bypassed here.

## Goals / Non-Goals

- **Goal:** one named, enforced floor that rejects pre-`na` at open with a typed error + a test that
  proves it (not implicit magic mismatch).
- **Goal:** remove the pre-`na` correctness surface so reviewers/authors stop trying to make `ma`–`me`
  correct; declare the floor as doctrine.
- **Non-goal:** adding/removing 5.0 format support, re-architecting gates, write-path changes.

## Decisions

### Decision 1 — Floor lives in version parsing, enforced as a typed error; reader fallback is tightened

**Chosen:** Add the floor check at `BigVersionGates::from_version` (`version_gate.rs:300`) — the single
function that validates the BIG version string — returning a new typed `Error::UnsupportedVersion`
when the version is below `na`. AND tighten `reader/mod.rs:402-413` so that arm propagates an
`UnsupportedVersion` error (fatal) instead of degrading to `nb_fallback()`; only a genuinely
*unparseable / structurally-malformed* descriptor (not a parsed-but-below-floor one) may fall back.

- **What it beat — "floor only in `from_version`":** rejected because `reader/mod.rs:411` swallows the
  error and continues as `nb`, so the floor would not bite and AC #1's test could not pass.
- **What it beat — "floor only at the magic table":** rejected because the version *string* (which is
  what `na`/`nb`/`oa`/`da` is) comes from the **filename descriptor**, not the magic; a pre-`na`
  filename with a (hypothetically) matching magic, or the common real case of a pre-`na` file, is best
  caught at the version string with a clear message naming the floor — and the magic path only yields an
  untyped error at a later stage.
- **What it beat — "a brand-new standalone `version_floor()` module":** rejected as redundant;
  `from_version` already shape-validates the version and is the natural single authority. The floor is
  one comparison there, plus the doctrine line names it.

The floor constant is `na` (BIG). BTI is already floored at `da` by `BtiVersionGates::from_version`
(`version_gate.rs:457-463`), which rejects anything but `da` — that path already enforces a floor and a
typed-error upgrade there is in scope so both gate families speak the same error.

### Decision 2 — New typed `Error::UnsupportedVersion` variant (vs reuse `UnsupportedFormat`)

**Chosen:** add `Error::UnsupportedVersion { version: String, floor: String }` (additive enum variant)
mapping to the existing non-recoverable `ErrorCategory::Data`.

- **What it beat — "reuse `UnsupportedFormat(String)`":** a distinct variant lets tests and
  review-criteria key on the *category* "below floor" rather than string-matching, and reads clearly at
  the call site. It is additive (no breaking change to existing variants).

### Decision 3 — Delete the dead pre-`na` modeling surface (vs mark test-only)

**Chosen:** **delete** `BigVersionGates::is_compatible()` (dead; only callers are its own unit test) and
its test, and make `BigVersionGates::from_version` reject `< na` (Decision 1) so the struct no longer
*models* pre-`na` as acceptable. Gate-threshold branches that only mattered for `mb`/`mc`/`md`/`me`
become unreachable once `< na` is rejected and are simplified/removed with a comment that the floor is
`na`.

- **What it beat — "mark `#[cfg(test)]` / leave a comment":** leaving the code (even annotated) keeps a
  predicate that admits `ma` in the tree, which is exactly what re-triggers pre-`na` review churn.
  Deletion removes the surface entirely; doctrine + the floor gate are the durable record.

### Decision 4 — Doctrine in both CLAUDE.md and the agents-developing site

**Chosen:** add a **"Supported formats"** rule (not prose) to `CLAUDE.md` under Development Standards,
and mirror it on the `agents-developing/` doctrine (the no-heuristics / gate-contract neighborhood) so
roborev's repo-context read carries the 5.0 scope and the "do not review pre-`na` correctness"
instruction. The rule states the accepted set (`na`/`nb` BIG, `oa`/`da` BTI) and the explicit
out-of-scope (`ma`–`me`).

## Risks / Trade-offs

- **Risk:** an existing test or fixture relies on `from_version` accepting a `< na` string. *Mitigation:*
  the implementer greps for `ma`/`mb`/`mc`/`md`/`me` literals in tests (the audit found these only in
  `version_gate.rs` tests) and updates/removes them as part of the change; the gate runs the full suite.
- **Trade-off:** tightening `nb_fallback()` narrows a permissive path. *Mitigation:* only a parsed
  below-floor version becomes fatal; a structurally-unparseable descriptor still degrades, preserving
  today's tolerance for odd-but-5.0 filenames.

## Migration

None. No data migration; additive error variant; no public API removed (the deleted method is dead).
