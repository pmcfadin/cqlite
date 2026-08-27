# Design: feature-matrix-gate-lanes (issue #1699)

## Context

`scripts/agent-gate.sh` (8553 lines) is the gate of record. Components are declared in three places that
must agree: the `COMPONENTS` array (`:2166`), the dispatch `case` (`:8287` region), and — for anything that
needs fixtures — `DATASET_COMPONENTS` (`:7866`). Components run in two lanes: **MAIN** (the shared
`CARGO_TARGET_DIR` cargo pipeline) and **SIDE** (concurrent, each SIDE cargo component getting its **own**
target dir because a divergent feature set or a separate crate would otherwise thrash MAIN's target dir —
measured 72 s → 576 s for python-bindings, #2657/#1737).

This change adds four components. Every decision below is about *what they certify* and *what they cost*.

## D1 — Four components, not one

**Decision: four separately-named components** (`flight-tests`, `legacy-heuristics`, `feature-iso-parquet`,
`feature-iso-delta-scan`), not one `feature-matrix` umbrella.

The SUMMARY block is the only gate text an agent retains. A single umbrella component that FAILs forces the
reader into `gate.log` — which doctrine forbids reading — to learn *which* feature broke. Four names put the
verdict in the block. The cost is four lines in `--list`; the benefit is that a FAIL names its own subject.

The two isolation lanes are separate rather than one `feature-isolation` component for the same reason: their
whole purpose is to distinguish "parquet leaks into delta-scan" from "delta-scan leaks into parquet", and an
umbrella verdict erases exactly that distinction.

## D2 — The isolation lanes test-compile, under `-D warnings`

**Decision: `RUSTFLAGS="-D warnings" cargo test -p cqlite-core --no-default-features --features
all-compression,<one-of> --lib --no-run` — not the issue's literal bare `cargo check`, and not
`cargo check --all-targets`.**

**CORRECTION (this decision previously mandated `cargo check --all-targets`; that instrument was measured
and found wrong).** The *argument* for compiling test code stands unchanged and is restated first, because
it is the part that survives.

**What stands.** The issue asks for `cargo check --package cqlite-core --no-default-features --features
all-compression,parquet`. Taken literally that lane is **blind to the incident it cites**. #1978 was an
ungated `#[cfg(test)]` module referencing a `write-support`-gated item, and a bare `cargo check` never
compiles `cfg(test)` code at all — so the lane would compile the library, go green, and miss it. Any lane
that does not compile test code is not measuring the incident class. `minimal-build` already learned this
exact lesson and carries the comment to prove it ("A plain `cargo build` never does, so a `#[cfg(test)]`
module referencing a write-support-gated item silently escaped this gate").

**What was wrong: the instrument, not the argument.** `--all-targets` over-reaches. The #1978 incident class
lives in `cqlite-core/src/**`'s **inline `#[cfg(test)]` modules**; `--all-targets` additionally compiles
cqlite-core's ~100 **integration** test files, which are written against the **default** feature set and so
fail on modules these lanes deliberately configure out. **Measured**, three representative failures:

| File | Line | Fails on |
|------|------|----------|
| `cqlite-core/tests/issue_1004_primitive_codec_vectors.rs` | 23 | `storage::serialization` |
| `cqlite-core/tests/issue_2412_wraparound_scan.rs` | 42 | `storage::write_engine` |
| `cqlite-core/tests/contract_stability_tests.rs` | 23 | `cqlite_core::query` |

Those are **noise** — the integration suite assuming default features — not cross-feature leakage, which is
the only thing these lanes exist to measure. A lane that reds on its own scaffolding teaches agents to waive
it.

**The correct instrument: `cargo test --lib --no-run`,** exactly the shape `minimal-build` uses, and for
exactly this reason. It compiles the lib **with** its `cfg(test)` modules (the incident class) and pulls in
**no** integration target. Note the trap this closes in the other direction: a plain `cargo check --lib`
does **not** compile `cfg(test)` and would be blind to #1978 — `--lib --no-run` is load-bearing as a pair.

`-D warnings` for the same reason `minimal-build` sets it (#1981): the dead-code lint is how a
feature-orphaned helper surfaces, and a lane without `-D warnings` demotes that to a warning nobody reads.

`--all-compression` stays in both lanes because it is in `default` and dropping it would change what the lane
is measuring from *feature isolation* to *no-compression support*, which is a different (and already
covered) question — `minimal-build` owns that one.

**Cost.** `cargo check` (lib only) was 18 s / 10 s. `--lib --no-run` compiles the same crate graph plus the
lib's test cfg, well short of the `--all-targets` figures recorded in D6, so the fallback D6 reserved
(dropping to a *single* isolation component if the cost proved disproportionate) is not needed.

## D3 — `legacy-heuristics`: EXECUTE, don't just compile

**Premise correction first.** The issue says `legacy-heuristics` "is never test-compiled by the gate". That
was true when the audit was written and is **false at `2bde26a7c`**: `legacy-heuristics` is in clippy's
cqlite-core per-package feature list (`agent-gate.sh:4700`) which runs `--all-targets` under `-D warnings`,
so its test bodies **do** compile on every full gate. Implementing the issue literally — build plus "a smoke
test compile of its gated tests" — would therefore add **almost nothing**: a second compile of code the
clippy pass already compiles.

**Decision: the lane's subject is EXECUTION and ISOLATION, which are the parts genuinely missing.**

1. `RUSTFLAGS="-D warnings" cargo build -p cqlite-core --features legacy-heuristics` — the issue's literal
   AC, kept, and cheap (26 s measured). It is also the only lane that builds the feature at
   `default + legacy-heuristics` rather than at clippy's ~30-feature union.
2. Then **run** the gated tests: `cargo test -p cqlite-core --features legacy-heuristics --lib
   --test <each derived target>`.

Five `cqlite-core/tests/*.rs` files carry `legacy-heuristics` cfg sites; both polarities exist
(`#[cfg(feature = ...)]` bodies *and* `#[cfg(not(feature = ...))]` bodies). The `not` polarity already runs
in `core-tests`. The positive polarity — the code the feature flag actually turns on — has **never been
executed by anything**.

**The target set is DERIVED, not hard-coded.** A literal list drifts the moment someone adds a sixth gated
test file, and the drift is invisible (the lane stays green while its subject shrinks). The component greps
the committed `cqlite-core/tests/*.rs` for `legacy-heuristics` and builds `--test` flags from what it finds.
**Derivation is fail-closed**: zero derived targets is a FAIL naming the derivation, never a PASS — a lane
with no subject has no verdict to give, the same rule `prompt-content:` follows. The lane additionally runs
under the existing `check_no_unexpected_zero_tests` guard so "compiled, ran 0 tests" cannot read as green.

`--lib` is included because eight `cqlite-core/src/**` files carry cfg sites whose inline `#[cfg(test)]`
bodies are gated the same way.

**Open risk, ruled at Seam 1 (D3.RISK).** These tests have never run. If they do not pass, the three exits
are: **(a)** fix them inside this issue; **(b)** `#[ignore]` the failures with a filed follow-up issue and
land the lane green over the rest; **(c)** narrow the lane to the issue's literal compile-only scope.
Recommendation: **(b)** — the lane's value is that it starts running *and stays* running; a bit-rotted legacy
test is a separate, filable defect, and folding an unknown amount of legacy-heuristics repair into a gate-
machinery issue is scope creep with an unbounded tail. **(a)** if the failures are trivial; never **(c)**,
which buys a lane that certifies nothing new. The measured answer is recorded in the Seam-1 comment.

## D4 — `flight-tests`: what "a test lane" means for a 41-file e2e crate

**Premise correction.** "`cqlite-flight` has NO test lane at all" is not quite the state at `2bde26a7c`.
What exists: clippy compiles the whole crate `--all-targets`; `flight-query-semantics-oracle` **runs** two
named integration targets; `memory-budget` runs one dhat target. What does not exist: any local execution of
the other ~38 integration targets or of `--lib`. And CI's Flight tier already runs `--lib` *and* the full
package (`flight-ci.yml:193,229`) with `required` failing closed on it (#2910) — so the gap is precisely
**local, pre-push execution**, which is the whole point of a local-first gate.

**Decision: one `flight-tests` component running the package suite, in the SIDE lane with its own
`CARGO_TARGET_DIR`, dataset-declared, under the zero-tests guard.** Scope (whole package vs `--lib` plus a
bounded target set) is decided on the measured number in D6, and the measurement is reported either way.
`cqlite-flight` is a separate crate built with `arrow`-flavoured cqlite-core features — textbook SIDE class
(a), so putting it on MAIN would thrash the shared target dir.

**Amendment (#3383): the invocation is a DERIVED explicit target list, not `cargo test -p`.** The original
decision above said "the package suite", on the reasoning that a hand-picked target list is a second
registry that drifts silently (the #2039 `cli-tests` lesson). That reasoning was and remains correct — and
it is exactly why the replacement is **derived**, not typed out.

What forced the change is measured, not stylistic. `cqlite-flight`'s
`fast_arm_stream_stops_when_the_client_drops_it` (in `issue_3058_bypass_path_taken`) asserts a **race
outcome**: the client's stream drop must beat the producer. Under `-p`'s intra-package parallelism it does
not reliably win.

| How it was run | Result |
|---|---|
| the target alone, `--test issue_3058_bypass_path_taken`, host load 74 | **3 / 3 PASS** |
| inside the whole-package `flight-tests` lane | **2 of 3 runs FAILED** |

A merge-gate lane that reds 2-in-3 carries **no information**. Worse than uninformative, it is corrosive:
it teaches every worker that this lane's red means "re-run it", which is the habit that lets a *real* red
through. `cargo test -p` offers no way to exclude one target, so the invocation had to become explicit:

```
cargo test --no-fail-fast -p cqlite-flight --lib --bins --test <T1> --test <T2> …
```

Three properties preserve the original decision's intent:

1. **The list is DERIVED from `cargo metadata` at run time**, so the drift the #2039 lesson warns about is
   structurally impossible: adding `cqlite-flight/tests/foo.rs` puts `--test foo` on the command line with
   **no gate edit**, and `scripts/tests/test_agent_gate_feature_matrix_lanes.sh` still proves it by planting
   a brand-new target the gate names nowhere and requiring the lane to red.
2. **Two subtractions, only one of them curated.** Targets whose `required-features` this lane cannot enable
   are omitted because `--test X` on such a target is a **hard error** (where `-p` skipped it silently) —
   derived from cargo metadata. The flake list (`FLIGHT_FLAKE_SKIPS`) is **curated and labelled as such in
   code**, because flakiness is not mechanically decidable from source: nothing in a file says "this
   assertion races". Both halves of every entry are enforced (numeric issue number; target must be
   declared), so the list cannot grow silently or rot into a no-op.
3. **`--lib --bins` are explicit and load-bearing.** An explicit selector suppresses every target kind not
   named, so omitting `--bins` would have silently stopped executing `main.rs`'s 2 unit tests — this change
   would itself have opened the never-executed hole the lane exists to close. (No Rust doctests are lost:
   all 10 doc fences in the crate are ```` ```text ````/```` ```json ````.)

A failed derivation, or an empty run list, is a **FAIL naming the derivation** — never "nothing to run",
which would be a green lane that executed no integration target at all.

**`flight-query-semantics-oracle` is left alone.** `flight-tests` re-running its two targets is a few
seconds of overlap; re-deriving its per-lane fixture SKIP predicates (#3095, which exist specifically so one
lane cannot silently skip behind an unrelated lane's absent fixtures) risks a correctness regression in a
working component to save those seconds. Overlap is the cheaper error.

**No opt-out env var.** `cqlite-flight` is a committed workspace member; it is never legitimately absent.
Fixture-dependent *sub-targets* may SKIP through the existing dataset machinery, but the component itself
cannot be switched off.

## D5 — Observed to fire, not merely present (#3272)

AC2 asks for a regression simulation in a scratch branch, documented in the PR. **Decision: deliver it as a
committed, re-runnable harness plus a recorded observation, not a one-off manual demo.**

`scripts/tests/test_agent_gate_feature_matrix_lanes.sh`:
- creates a throwaway `git worktree add --detach` copy (never mutating the live tree — #2926 makes a
  mid-run tree mutation a FAIL, and a harness that edits the checkout its own gate is running in is the
  bug it is meant to catch);
- for each lane, plants the **minimal** break of that lane's own subject and asserts the lane exits
  **non-zero**;
- asserts the **unbroken** lane exits **zero** on the same tree, so the harness cannot pass by failing
  everything — a planted-break harness that never checks the negative direction is the vacuous-guard shape
  #3229 was about;
- names, per lane, what was planted and what fired.

The planted breaks, one per lane, each chosen to be the *incident class* rather than a generic syntax error:
- `feature-iso-parquet` — a `parquet`-gated item referencing a `delta-scan`-gated item.
- `feature-iso-delta-scan` — the mirror.
- `legacy-heuristics` — a `#[cfg(feature = "legacy-heuristics")]` test body whose assertion is inverted
  (proving the lane *executes* rather than merely compiles — a compile-only lane would stay green).
- `flight-tests` — a failing assertion in a cqlite-flight test the gate does not currently run by name
  (proving the lane's reach beyond the two oracle targets).

**It is OPT-IN, not a default component.** It performs four real compiles; adding that to every full gate
would tax every worker on every run to re-prove a property that does not change. It runs on demand and its
observation is recorded in `docs/reports/`. Enrolling it in the nightly `gate.yml` deep-check is a workflow
change requiring #2910 registry enrollment — deliberately **out of scope**, proposed as a follow-up.

**The cheap half stays in the fast loop.** Registration (in `COMPONENTS`, in `--list`, in the SUMMARY) is
pinned by `scripts/tests/test_agent_gate_summary.sh`, which `--lite` already runs via `tooling-tests`. So a
future edit that drops a lane from the array reds `--lite` in seconds, while the expensive
does-it-actually-fire proof is re-runnable on demand.

## D6 — Cost, measured and reported (AC3)

Measured on this worker box (`cargo` 1.97.1, sccache active, warm dedicated target dir):

| lane | command | secs | verdict today |
|------|---------|------|---------------|
| `feature-iso-parquet` | `check --no-default-features --features all-compression,parquet` | 18 | PASS |
| `feature-iso-delta-scan` | `check --no-default-features --features all-compression,delta-scan` | 10 | PASS |
| `legacy-heuristics` (build half) | `build -p cqlite-core --features legacy-heuristics` | 26 | PASS |
| `legacy-heuristics` (run half) | derived targets, executed | *recorded at Seam 1* | *first-ever execution* |
| `flight-tests` | scope per D4 | *recorded at Seam 1* | — |

**Wall-time is reported as two numbers, because one would be misleading.** Per-component durations come
straight from the SUMMARY. The *added wall time* is measured as a baseline full gate at `origin/main` versus
the gate of record on this branch, run **sequentially on one box** (#2640 pins one gate at a time) — because
three of the four lanes go in the SIDE lane and run concurrently with MAIN, so the naive sum of component
seconds overstates the added wall time, possibly to ~0. Both numbers are posted; neither is presented as the
other.

## D7 — Doctrine, in the same change

CLAUDE.md's gate table enumerates what the **Full** mode covers; four new components change that sentence,
and the website `agents-developing/gate-contract/` page mirrors it. Both are updated here, and the publish is
verified by **grepping the served page for a phrase this change introduces**, not by an HTTP 200 (#3042 —
the CDN serves the previous page for ~3 minutes after a green deploy).

## Alternatives rejected

- **One umbrella `feature-matrix` component** — erases which feature broke from the only text an agent
  retains (D1).
- **Bare `cargo check` per the issue's literal wording** — blind to the `#[cfg(test)]` incident class it
  cites (D2).
- **Compile-only `legacy-heuristics`** — duplicates work clippy already does; adds no coverage (D3).
- **Adding the planted-break harness as a default gate component** — four real compiles on every full gate
  to re-prove a static property (D5).
- **Restructuring `flight-query-semantics-oracle` to avoid overlap** — risks a #3095 correctness regression
  to save seconds (D4).
- **A CI-only lane** — explicitly forbidden by the issue and by local-first doctrine; CI mirrors the gate.
