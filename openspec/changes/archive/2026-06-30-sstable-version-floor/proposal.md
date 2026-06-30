## Why

While landing #1021 (repair-metadata-through-compaction), roborev raised a **series of "regression
for older SSTable versions" findings** about pre-`na` (Cassandra 3.x `ma`–`me`) formats — driving ~4
review rounds + 3 implementer dispatches on out-of-scope work before we recognized **CQLite does not
read pre-`na` at all**. The findings were not roborev malfunctioning: the code carries pre-`na`
*modeling surface* (version gates whose predicates span `ma`–`me`), and a reviewer reasonably reads
"this branch exists ⇒ it must be correct." There is **no single authoritative statement — in code or
doctrine — that the support floor is Cassandra 5.0**. This recurs on any change touching the
version-gate / parser layer.

A read-only audit confirmed the floor is real but **emergent, not declared or enforced**:

- `cqlite-core/src/parser/header.rs:330-352` — `SUPPORTED_MAGIC_NUMBERS` contains only Cassandra 5.0 /
  legacy-`oa` magics; **no `ma`–`me` magic exists**, so pre-`na` files are rejected *implicitly* by
  magic mismatch — and that rejection is an **untyped nom parser error** (`header.rs:438,453-456`), not
  a typed version error, surfaced only at header-read time.
- `cqlite-core/src/storage/sstable/version_gate.rs:360` — `BigVersionGates::is_compatible()` is
  **dead/test-only** (its only callers are the unit test in the same file), and its predicate
  `v >= "ma"` actually **admits** Cassandra 3.x. This is precisely the pre-`na` modeling surface that
  invites pre-`na` review findings.
- `BigVersionGates::from_version` (`version_gate.rs:300`) accepts **any** two-lowercase-letter version
  string and models gate thresholds across `mb`/`mc`/`md`/`me`/`na`/`nb`/`oa` — so it half-handles
  pre-`na` by formula.
- `cqlite-core/src/storage/sstable/reader/mod.rs:402-413` — derives gates from the filename, but on
  **any** descriptor parse failure **silently falls back to `nb` BIG gates** (`mod.rs:411`) and
  continues. This escape hatch means a floor check placed in descriptor parsing alone would be
  **bypassed** — it must be addressed for any floor to actually bite.
- `cqlite-core/src/error.rs` — has `InvalidFormat`/`UnsupportedFormat` but **no** typed
  `UnsupportedVersion` variant.
- `CLAUDE.md` — states "reads Cassandra 5.0 data files" in **prose only** (`CLAUDE.md:7`); there is no
  floor stated as a **rule**.

- **Milestone:** maintenance / process hardening. **Design-driven** (doctrine + a small enforcement
  gate; touches contributor doctrine and a public error variant — real latitude in where the floor
  lives and whether the pre-`na` surface is deleted vs quarantined). There is no Cassandra SSTable
  format oracle being newly decoded here.
- Adds a new `sstable-version-floor` capability.

## What Changes

- **Explicit, enforced version-floor gate.** Add a single authoritative minimum-supported-version check
  that rejects pre-`na` (`ma`–`me`) at descriptor/open time with a **typed** error, replacing reliance
  on implicit magic mismatch. One named place declares "we support `na`+ (`nb`/`oa` BIG, `da` BTI);
  everything older is rejected here."
- **Tighten the reader fallback so the floor bites.** The `nb_fallback()` arm in `reader/mod.rs` must
  distinguish a *below-floor* descriptor (fatal — propagate the typed error) from a genuinely
  *unparseable* descriptor; a pre-`na` version string must NOT degrade to `nb` and continue.
- **Quarantine/remove the pre-`na` modeling surface.** Remove the dead `BigVersionGates::is_compatible()`
  (test-only, and its predicate wrongly admits `ma`), and make `BigVersionGates` stop *modeling* pre-`na`
  as an acceptable read surface so no reviewer/author tries to make `ma`–`me` "correct."
- **Doctrine line (source of truth).** Add an explicit **"Supported formats"** rule to `CLAUDE.md` and
  the published `agents-developing/` doctrine: *CQLite targets Cassandra 5.0 (`na`+/`nb` BIG, `oa`/`da`
  BTI); pre-`na` (`ma`–`me`, Cassandra 3.x) is out of scope — do not introduce, support, or review
  pre-`na` correctness.* Includes the "do not review pre-`na`" guidance for reviewers/roborev.

## Non-goals

- **No new format support.** This does not add pre-`na` reading, nor change which 5.0 formats are read.
- **No re-architecture of version gates / VG3 wiring.** The gates' parsing-behavior wiring
  (`reader/types.rs:273-279`) stays as-is; we only add the floor and remove dead pre-`na` surface.
- **No change to the magic-number table or header decode** beyond what the typed floor error requires;
  the magic table already excludes pre-`na`.
- **No write-path changes.** CQLite only writes `na`+/`da`; the floor is a read-path concern.
