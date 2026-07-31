# Design: checkout-relative schemas root + a schemas-aware gate preflight (issues #3148, #3131)

## Context

The gate's fixture preflight (#2078) exists so that a fixture-absent run FAILs closed instead of
SKIP-then-PASSing on zero dataset-backed coverage. It validated exactly one thing:
`$CQLITE_DATASETS_ROOT/sstables/test_basic/*-Data.db` count > 0.

The fixture *contract*, however, has two halves: the SSTable bytes **and** the CQL schema that decodes
them. Deriving the second half from `CQLITE_DATASETS_ROOT` by climbing `..` produced a layout no single
root on the fleet satisfied (#3131) and a preflight whose `STATUS: OK` was positively misleading (#3148).

## The decision (#3148 AC (h))

**Resolve the schemas root checkout-relative. Do not make the corpus layout carry it.**

The alternative on the table was to keep the coupling and repair it — ship `schemas/` inside the fetched
archive, or make the fetch populate `<repo>/test-data/datasets` so the sibling exists. Both were
rejected:

| Option | Verdict | Reason |
|--------|---------|--------|
| ship `schemas/` in the dataset archive (#3131 item 1b) | rejected | duplicates **committed source** into a versioned binary asset; a schema edit then needs a dataset re-release, and the two copies can disagree with no signal |
| make the fetch populate the repo tree (#3131 item 1a) | rejected | does not fix the general case (any relocated root re-breaks it), and it fights the whole point of `CQLITE_DATASETS_ROOT` — a big, relocatable, machine-local cache |
| **resolve checkout-relative (#3148 fix 4)** | **ADOPTED** | a checkout ALWAYS holds these files, so the failure mode is structurally impossible; the symlink trap disappears because no `..` climb remains; no data duplicated; every existing corpus layout — including `/data/datasets` — becomes self-sufficient unchanged |

Consequence worth stating plainly: this **supersedes** #3131 item 1's either/or. The "one documented root
that works" is now *any* corpus root, because the schemas are no longer a property of the corpus root at
all.

### Why an override still exists

`CQLITE_SCHEMAS_ROOT` is honored when set, non-empty **and** a readable directory. It is not the primary
mechanism — it exists for a genuinely out-of-tree run (a packaged corpus + schemas shipped together, no
checkout). The fall-through on an unreadable/empty value is deliberate: a stale export in a shell profile
degrades to the correct checkout answer instead of pinning every fixture load to a path that cannot work.

## Mechanism: one file, `#[path]`-included

`test-data/support/fixture_roots.rs` is std-only and pulled in with
`#[path = …] mod fixture_roots;`. Alternatives considered:

- **A new `cqlite-test-support` crate.** Rejected: a whole workspace member (plus manifest, plus
  dev-dependency edges from two crates) to host ~60 lines of path logic, and it would have to be
  `publish = false`-managed.
- **Host it in `cqlite-core/benches/fixtures/roots.rs`.** Rejected: `cqlite-cli`'s bench would then
  reach cross-crate into `cqlite-core`'s bench internals — which that bench's own module doc explicitly
  disclaims — and the include paths become asymmetric.
- **Host it under `test-data/support/`.** **ADOPTED**: the file encodes the layout of `test-data/`
  itself, it is owned by neither crate, and the include path is symmetric (`../../` from ANY
  `<crate>/tests/` or `<crate>/benches/`, `../../../` from the nested `benches/fixtures/`). `#[path]` on
  a module declared at the top level of a file resolves relative to **that file's** directory, so
  `benches/fixtures/mod.rs` resolves identically no matter which of its ~14 including targets is being
  built. `cargo fmt` and `clippy --all-targets` both reach it through the including targets, and the
  gate's `file-size` ratchet globs `*.rs` repo-wide, so it is fully covered by existing lints.

### The two-shape `datasets_root()` contract (#3148 AC (e))

Three copies existed with **two different semantics** for the same logical root — the silent divergence
that makes this failure class hard to attribute. They are now one implementation with two named shapes,
and the difference is a stated behavioral choice rather than an accident:

| shape | env unset | env set, not a dir | rationale |
|-------|-----------|--------------------|-----------|
| `datasets_root()` | checkout fallback | returns it anyway | benches must run from a plain checkout with no env setup; a per-fixture error later is more actionable than a root-level one |
| `datasets_root_if_present()` | `None` | `None` | a SKIP-gated test must not silently run against a checkout holding only ~19 committed byte-parity references and report a 0-row pass |

`observability_correctness` and the benches take the first; `dead_cache_delete_tests` takes the second —
byte-identical to their pre-change behavior, which was a hard constraint (converting a skip into a panic
would red the gate broadly and is out of scope).

## Mechanism: the gate preflight

`_gate_schemas_root` / `_gate_schemas_root_source` mirror the Rust `schemas_root_resolved()` exactly, so
the gate asserts the **same path** the tests will resolve; both anchor on the checkout, so they cannot
drift by construction. `_schemas_status` is a PURE `OK|FAIL` decision (returning `OK` for `--lite` and
`--only`), and the hidden `--preflight-schemas` hook prints that same decision plus `ROOT`, `SOURCE` and
the unreadable file list — so the self-test asserts the decision the real gate consumes, not a parallel
re-implementation of it. This mirrors #2078's `_fixture_status` / `apply_fixture_preflight` /
`--preflight-fixtures` split deliberately: one idiom for both halves of the fixture contract.

Two decisions inside it:

- **Per-FILE readability, not directory existence.** A root that exists and holds *some* fixtures is the
  realistic partial-copy failure, and a directory check green-lights it.
- **No opt-out**, unlike #2078's `AGENT_GATE_ALLOW_MISSING_FIXTURES`. The fetched corpus is legitimately
  absent on a fresh box; committed source in a checkout never is. An unreachable schemas root means a
  broken checkout or a stale override, and neither may certify a run.

Ordering: the corpus guard runs first, so a run missing both still reports the #2078 cause — the fetched
half is the one an operator must act on.

## Why the self-test is the load-bearing part

The defect being fixed is a **verification** defect, the third of its shape recently (#3130, #3127). The
same mistake is available here: a preflight whose only test is "a good layout passes" is untested, since
`STATUS: OK` is then never shown to be a decision. So the self-test drives layouts the preflight must
**reject** — schemas-less, and present-but-incomplete — and asserts the rejection text, the marker's
separability from #2078's, and the absolute path in the remedy. The same reasoning produced
`fetch-datasets.sh --verify-only`: on the warm path the `.dataset-pin` fast path implies the content
check, so without a non-mutating probe the new guarantee could only ever be observed passing.

Symlink-trap independence (#3148 AC (f)) is asserted **behaviorally**, not claimed: the resolved schemas
root is byte-identical across a real datasets directory, a symlinked one, and a nonexistent one. The
structural half — zero open-coded `join("../schemas")` **expressions** in Rust (doc comments quoting the
retired idiom are exempt) — is a reintroduction guard that reddens the gate.

## Facts the docs pass must state (handled separately on this branch)

1. `CQLITE_DATASETS_ROOT` alone is sufficient; the corpus root no longer needs a `schemas` sibling.
2. The CQL schema fixtures are committed source resolved checkout-relative; `CQLITE_SCHEMAS_ROOT` is an
   optional out-of-tree override.
3. `bash test-data/scripts/fetch-datasets.sh` now prints the exact `export CQLITE_DATASETS_ROOT=…` line
   it guarantees — use THAT, not a remembered default. `--verify-only` probes a root non-destructively.
4. The FULL gate has a second fail-closed fixture cause, `missing-schemas: FAIL-CLOSED (#3148)`, and
   stamps a positive `schemas: N/N …` line on success.
5. The CLAUDE.md line "point `CQLITE_DATASETS_ROOT` at the main repo's `test-data/datasets`" needs the
   caveat that on a fleet box the fetch may have populated a machine-local root instead — the fetch's own
   printed export line is authoritative.

## Risks

- **A crate nested deeper than one level below the workspace root.** Handled: resolution walks
  `CARGO_MANIFEST_DIR`'s ancestors for the first holding `test-data/schemas`, rather than hardcoding
  `../test-data`. The walk builds no `..` component, so nothing it hands the kernel can be re-rooted by a
  symlink.
- **A truly checkout-less run** (fixtures deleted). Then the walk finds no ancestor and returns the
  canonical one-level-up guess purely so the error message can name a concrete absolute path — the
  failure is still a failure, just a diagnosable one.
- **Drift between the gate's shell resolution and the Rust one.** Bounded by both anchoring on the
  checkout with the same override rule, and by the self-test asserting the gate's resolved `ROOT` equals
  `<repo>/test-data/schemas`.
