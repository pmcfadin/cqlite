# Design: ANTLR-Derived CQL Parsing for Cassandra Grammar Compatibility

**Status:** Investigation + proposed path (researched 2026-06-12, pending review)
**Audience:** CQLite maintainers
**Related:**
- `docs/technical/CQL_PARSER_IMPLEMENTATION.md` (earlier hybrid strategy; superseded by the findings here)
- `docs/architecture/parser-overview.md` (current four-parser architecture)
- `cqlite-core/src/cql/antlr_backend.rs` (existing placeholder backend), `cqlite-core/src/cql/factory.rs` (backend selection seam)
- `antlr` feature flag in `cqlite-core/Cargo.toml` (defined, currently empty)

## Purpose

Early in CQLite's development the goal was to consume Apache Cassandra's own
ANTLR grammar files so that CQLite's parser would match Cassandra's grammar
exactly, and so that new syntax in future Cassandra releases could be adopted
"simply by pulling in the new grammar." For the MVP we built our own parsers
instead. This document records what it would actually take to get back to that
goal: what is and isn't possible, the options with pros and cons, and a
recommended path.

**Conclusion in one sentence:** consuming Cassandra's grammar files directly is
not feasible (they are ANTLR 3 with pervasive embedded Java), but an
ANTLR4-based path exists — and the highest-value first step is to use a
generated parser as a *conformance oracle* in CI rather than replacing the
production parser.

## Where we are today

CQLite has four parsing subsystems; two parse CQL text:

| Subsystem | Location | Technology | Size |
|---|---|---|---|
| SELECT parser | `cqlite-core/src/query/select_parser.rs` | hand-written tokenizer + recursive descent | ~1,000 lines |
| Lightweight DML parser | `cqlite-core/src/query/parser.rs` | keyword-based | ~660 lines |
| Schema/DDL + full-AST CQL parser | `cqlite-core/src/schema/cql_parser.rs`, `cqlite-core/src/cql/` (nom backend, mutation parser, visitor) | nom combinators | ~9,500 lines combined |
| ANTLR backend | `cqlite-core/src/cql/antlr_backend.rs` | **stub** — every method returns "not yet implemented" | 136 lines |

Importantly, the architecture already anticipated this work: the
`CqlParser` trait, `ParserBackend::Antlr`, and the factory's backend-selection
logic (`cql/factory.rs`) form a ready-made seam. An ANTLR-generated parser
would slot in behind an existing abstraction rather than requiring a refactor.

## Findings (ground truth, June 2026)

### 1. Cassandra's grammar cannot be consumed as-is

- Cassandra 5.0 and trunk keep the CQL grammar in
  [`src/antlr/`](https://github.com/apache/cassandra/tree/cassandra-5.0/src/antlr)
  (`Cql.g` importing `Parser.g` + `Lexer.g`), built with **ANTLR 3.5.2**
  (pinned in `.build/parent-pom-template.xml`; trunk still pins 3.5.2 as of
  June 2026 — no ANTLR4 migration exists in the codebase, on JIRA, or on the
  dev list).
- `Parser.g` (~1,850 lines) is roughly 35–40% embedded Java. Nearly every rule
  constructs `org.apache.cassandra.cql3` objects inline
  (`selectStatement returns [SelectStatement.RawStatement expr]`, etc.) and
  relies on `@members` helpers and custom error recovery. The grammar is
  inseparable from Cassandra's Java AST without a strip-and-rewrite pass.
- No ANTLR Rust target exists for ANTLR 3 (the ANTLR 3 toolchain itself is
  end-of-life). So "point the ANTLR tool at Cassandra's files and emit Rust"
  is not a thing that can exist without converting the grammar to ANTLR 4
  first.

### 2. The Rust ANTLR toolchain was revived in 2025

- The original `antlr-rust` crate (rrevenantt/antlr4rust) was effectively dead
  after 2022 and at one point required nightly Rust.
- A maintained fork now exists: the [`antlr4rust`](https://crates.io/crates/antlr4rust)
  crate (Alex Snaps, [antlr4rust org](https://github.com/antlr4rust)) — v0.5.2
  (Oct 2025), BSD-3-Clause, **stable Rust, MSRV 1.80**, with the generator
  shipped as a tool jar in their fork of antlr/antlr4
  (`java -jar antlr4-tool.jar -Dlanguage=Rust Grammar.g4`).
- Official ANTLR (antlr4, antlr-ng, antlr5) still has **no Rust target** and
  none is announced. The Rust path depends on this single-maintainer fork
  pinned to a 4.8-lineage tool.

### 3. An action-free ANTLR4 CQL grammar already exists

- [`antlr/grammars-v4/cql3`](https://github.com/antlr/grammars-v4/tree/master/cql3)
  (`CqlLexer.g4` + `CqlParser.g4`) is action-free, BSD-licensed, and has been
  sporadically but genuinely maintained since 2018 (spec-alignment fixes
  Nov 2024, new-features commit Jan 2026). It does not document which
  Cassandra version it targets, so coverage of 5.0 syntax (`VECTOR<FLOAT, n>`,
  `CREATE INDEX ... USING 'sai'`, `ORDER BY ... ANN OF`) must be audited
  rule-by-rule and likely patched.
- Beware the naming trap: `grammars-v4/cql` is Z39.50 Contextual Query
  Language, not Cassandra.

### 4. Precedents — the pipeline works, but nobody escapes the sync problem

- **Instaclustr's `cql_rust`** (2022) generated a Rust parser from the
  grammars-v4 cql3 grammar with the old antlr-rust target. It worked
  mechanically but was abandoned (its main blocker — nightly Rust — is now
  fixed by antlr4rust 0.5.x).
- **`tentacle-scylla/scql`** (Go, Jan 2026) derived an ANTLR4 grammar from
  ScyllaDB's ANTLR3 `Cql.g` by mechanically stripping actions/returns; claims
  full statement coverage with 1,566 test queries. This proves the
  strip-and-retarget approach is automatable.
- **ScyllaDB** forked `Cql.g` and stayed on ANTLR3 with C++ actions —
  inheriting the coupling problem rather than solving it.
- **cqlsh itself does not use the ANTLR grammar** — it ships a hand-written
  Python parser. Even inside the Cassandra project, derived grammars are
  maintained by hand.
- No CQL dialect exists in sqlparser-rs, and no CQL parser crate exists on
  crates.io. `shotover/tree-sitter-cql` exists but is early-stage and aimed at
  highlighting, not semantic parsing.

### 5. Grammar churn is real and ongoing

Cassandra 5.0 added vector types, SAI index syntax, and `ANN OF`. Trunk adds
CEP-15 Accord transactions (`BEGIN TRANSACTION ... COMMIT TRANSACTION` with
`LET`/`IF`) and CEP-42 constraints (`CHECK`). The original motivation — keep
up with new syntax cheaply — remains valid; each major release will bring
grammar changes.

## The core insight: reframe what "pulling in the grammar" buys

Two things the original vision conflated:

1. **Syntax** (does CQLite *accept* exactly what Cassandra accepts?) — a
   generated parser solves this, and grammar updates are cheap.
2. **Semantics** (turning a parse tree into CQLite's AST, types, and
   execution) — a generated parser does **not** solve this. The
   parse-tree-to-`CqlStatement` visitor is hand-written Rust, and every new
   syntax feature still requires hand-written mapping code, plus reader/
   executor support. Cassandra's own grammar handles this with the embedded
   Java we'd be stripping out.

So even in the best case, "pull in the new grammar" automates the *detection
and acceptance* of new syntax, not its *implementation*. That reframing drives
the recommendation: the cheapest 80% of the original goal is achievable by
using a generated parser to *verify* and *diff*, before (or instead of)
betting the production parse path on it.

## Options

### Option A — Status quo: keep extending the nom/hand-written parsers

**Pros**
- Zero new dependencies; no bet on a single-maintainer toolchain.
- Best performance and memory profile (aligned with the <128MB budget and
  zero-copy patterns); nom parsers are already passing 33/33 table validation.
- No disruption to the M5/M6 roadmap.

**Cons**
- Grammar drift is invisible: we learn about syntax gaps from bug reports.
- Every Cassandra release requires manual grammar archaeology against
  `Parser.g`.
- ~3,000+ lines of bespoke parsing code to maintain with no external
  conformance reference.

### Option B — Full ANTLR4 production parser (grammars-v4 cql3 + antlr4rust)

Generate lexer/parser into the existing `antlr_backend.rs` stub; write a
visitor mapping parse trees to the existing `CqlStatement` AST; select via the
existing factory.

**Pros**
- Closest to the original vision: grammar file is the single source of syntax
  truth; syntax updates start with a grammar diff, not parser surgery.
- Inherits ANTLR strengths the factory already advertises for this backend:
  error recovery, better diagnostics, future completion/highlighting.
- Existing trait/factory seam means no architectural change; can ship behind
  the already-defined `antlr` feature flag with nom as default.

**Cons**
- **Toolchain risk:** antlr4rust is a single-maintainer fork; official ANTLR
  has no Rust target. If it dies again (as antlr-rust did in 2022), we own a
  generated-code snapshot we can't regenerate.
- Build complexity: grammar regeneration requires a JVM + fork-specific tool
  jar (CI/codegen pipeline needed; generated code should be vendored).
- Performance/memory: ANTLR runtimes allocate heavily versus nom/zero-copy;
  unproven against the <128MB target and unknown WASM compatibility (M6).
- The grammars-v4 grammar is *not* Cassandra's grammar — it's a third
  rendering that itself drifts; we'd audit/patch it for 5.0 syntax and carry
  patches upstream or in-tree.
- Semantic mapping (visitor) is a large hand-written component (~weeks of
  work) and is where correctness bugs would live — the part ANTLR doesn't
  automate.

### Option C — ANTLR parser as a conformance oracle (not in the product)

Keep nom/hand-written parsers in production. Build the generated parser as a
**dev/CI tool** (separate crate under `tools/`, e.g. `tools/cql-conformance`)
that:
1. Runs the full CQL test corpus (and fuzzed statements) through both parsers
   and reports accept/reject divergence ("CQLite accepts X that Cassandra's
   grammar rejects" and vice versa).
2. Diffs grammar versions across Cassandra releases to produce a syntax
   change report ("5.1 added these rules/tokens; these N statements now parse
   differently").

**Pros**
- Delivers the *actual pain-killer* — knowing exactly where we diverge from
  Cassandra's grammar and what changed per release — without touching the
  production parse path, the memory budget, or the bindings.
- Toolchain risk is contained: if antlr4rust dies, we lose a dev tool, not a
  product feature. (The oracle could even be JVM-based, generated straight
  from a stripped Cassandra `Parser.g`, since it never ships.)
- Cheap: no visitor/AST mapping needed — accept/reject + parse-tree shape is
  enough for conformance checking.
- Produces the test corpus and grammar-sync automation that Option B would
  need anyway; B becomes a follow-on, not a leap.

**Cons**
- Does not improve product error messages or close syntax gaps by itself —
  gaps it finds still get fixed by hand in the nom parsers.
- One more CI tool and corpus to maintain.

### Option D — Automated strip-and-convert of Cassandra's own `Parser.g`

Per release, mechanically strip Java actions/`returns` from Cassandra's
ANTLR3 grammar and convert to ANTLR4 (the scql approach), then feed Option B
or C.

**Pros**
- Truest possible fidelity: derived from the exact grammar Cassandra ships,
  not a community re-rendering; sync is a re-run of the pipeline per release.

**Cons**
- ANTLR3→4 conversion is not fully mechanical (syntax predicates, error
  recovery, rule rewrites); the converter script becomes its own maintained
  artifact.
- Highest up-front cost; only worth it if grammars-v4 proves too stale to
  patch.

## Recommendation

**Phase 0 (spike) → Phase 1 (Option C, conformance oracle) → Phase 2 (Option B
behind the `antlr` feature flag), with Phase 2 gated on what Phases 0–1
reveal.** Do not pursue Option D unless grammars-v4 proves unmaintainable;
revisit if upstream ever migrates to ANTLR4.

This sequencing front-loads the deliverable the original goal was really
about — confidence of exact compatibility and fast detection of new syntax —
while deferring the risky bet (production parser on a single-maintainer
toolchain) until the toolchain has proven itself in CI for a release cycle.

### Phase 0 — Spike (~1 week)

1. Generate Rust from `grammars-v4/cql3` with antlr4rust 0.5.x (tool jar +
   `-Dlanguage=Rust`); vendor generated code in a scratch branch.
2. Verify: builds on stable Rust at workspace MSRV; parses a sample of the
   existing test corpus; rough perf/memory numbers vs the nom parser on the
   same statements.
3. Audit grammars-v4 cql3 against Cassandra 5.0 syntax: vector type, SAI
   `CREATE INDEX ... USING ... WITH OPTIONS`, `ANN OF`, plus the statements in
   our schema files and JSONL corpus. Record the gap list.

**Exit criteria:** generated parser compiles and runs on stable; gap list and
perf numbers documented. If the toolchain fails here, stop — Option A stands,
and this doc records why.

### Phase 1 — Conformance oracle in CI (~2–3 weeks)

1. New `tools/cql-conformance` crate (dev-only, not part of the published
   workspace artifacts): wraps the generated parser; vendors generated code so
   CI needs no JVM; a `scripts/regen-cql-grammar.sh` (JVM required) refreshes
   it.
2. Patch/extend the grammar for the Phase 0 gap list (contribute fixes
   upstream to grammars-v4 where accepted; carry the rest in-tree under
   `tools/cql-conformance/grammar/`).
3. Build the divergence corpus: every statement in `test-data/schemas/`, the
   query strings used across integration/parity tests, plus a curated set of
   valid-and-invalid CQL. CI job reports accept/reject divergence between the
   oracle and the nom/select parsers; divergences become tracked issues.
4. Add a release-watch script: diff our vendored grammar against
   grammars-v4 HEAD and (textually) against Cassandra's `Parser.g` rule
   inventory per Cassandra release; surface a "syntax changed upstream"
   report. This is the "pull in the new grammar" workflow, realized as
   detection.

**Exit criteria:** CI gate reporting zero unexplained divergences on the
corpus; documented per-release sync runbook.

### Phase 2 — Production ANTLR backend (optional, ~4–8 weeks, gated)

Proceed only if Phase 1 runs cleanly for a while **and** there's a concrete
product driver (e.g., rich error diagnostics/completion for the TUI/REPL, or a
divergence rate that makes hand-maintenance untenable).

1. Implement the parse-tree → `CqlStatement` visitor behind
   `antlr_backend.rs`, filling the existing stub; factory selection logic is
   already in place (`ErrorRecovery`/`CodeCompletion` features prefer ANTLR).
2. Ship behind the existing `antlr` feature flag, **off by default**; nom
   remains the default backend (performance, memory, WASM/M6 compatibility).
3. Differential-test the two backends with the Phase 1 corpus (same AST out,
   not just same accept/reject).

**Exit criteria:** AST parity with nom backend on the full corpus; perf and
memory within agreed budgets; gate (`scripts/agent-gate.sh`) green with the
feature enabled.

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| antlr4rust abandonment (single maintainer; happened to its predecessor) | High for Phase 2, Low for Phase 1 | Vendor generated code; oracle-only use until proven; keep nom as default backend forever |
| grammars-v4 cql3 staleness / unknown version target | Medium | Phase 0 audit; in-tree grammar patches; upstream contributions; release-watch diffing |
| ANTLR runtime perf/memory vs <128MB budget | Medium (Phase 2 only) | Measure in Phase 0; nom stays default; ANTLR opt-in |
| WASM (M6) incompatibility of ANTLR runtime | Medium (Phase 2 only) | Feature-gated backend excluded from wasm builds |
| Visitor (semantic mapping) correctness bugs | Medium | Differential testing against nom on full corpus; sstabledump-style golden parity |
| JVM dependency creeping into contributor workflow | Low | Vendored generated code; regeneration is a maintainer-run script |
| Conflating grammar acceptance with feature support (parser accepts `ANN OF`, executor can't run it) | Medium | Keep "parses" vs "executes" matrices separate; oracle reports are about syntax only |

## Open questions

1. Should divergence findings from the oracle gate CI (hard fail) or report
   (tracked issues)? Proposal: report-only for one release cycle, then gate.
2. Do we want the oracle to also validate `mutation_parser.rs` inputs and the
   schema parser, or start with SELECT/DDL only? Proposal: start with the
   statements the product actually accepts (SELECT, DDL, INSERT/UPDATE/DELETE),
   expand later.
3. Upstreaming: invest in pushing 5.0 syntax fixes to grammars-v4 (community
   benefit, less in-tree patch burden) or keep patches local (faster)?
4. Does the old `pmcfadin/cassandra-antlr4-grammar` repo (linked from the
   README) have content worth folding into the Phase 0 audit baseline?

## Relationship to prior strategy doc

`docs/technical/CQL_PARSER_IMPLEMENTATION.md` §"Future-Proofing Strategy"
proposed a similar hybrid (nom + ANTLR fallback) but predates the key facts
established here: Cassandra's grammar is ANTLR3-with-actions and not directly
consumable; no official Rust target exists; antlr4rust was revived in 2025 on
stable Rust; and working precedents (Instaclustr `cql_rust`, scql) define the
realistic pipeline. Its Phase 3 (ML-assisted parsing) is dropped. This
document supersedes that section for ANTLR planning purposes.
