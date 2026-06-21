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
not feasible (they are ANTLR 3 with pervasive embedded Java), and the
highest-value first step is a *conformance oracle* that validates our parser
against Cassandra's **own** Java parser in CI — while, if we later invest in
the production parser itself, a Rust-native **declarative grammar** (e.g.
pest) is a better fit for this project than an ANTLR backend, because the
project's mission is to track a grammar and a grammar file is the artifact
worth owning.

## Where we are today

CQLite has four parsing subsystems; two parse CQL text:

| Subsystem | Location | Technology | Size |
|---|---|---|---|
| SELECT parser | `cqlite-core/src/query/select_parser.rs` | hand-written tokenizer + recursive descent | ~1,000 lines |
| Lightweight DML parser | `cqlite-core/src/query/parser.rs` | keyword-based | ~660 lines |
| Schema/DDL + full-AST CQL parser | `cqlite-core/src/schema/cql_parser.rs`, `cqlite-core/src/cql/` (nom backend, mutation parser, visitor) | nom combinators | ~9,500 lines combined |
| ANTLR backend | `cqlite-core/src/cql/antlr_backend.rs` | **stub** — every method returns "not yet implemented" | 136 lines |

Two facts about the current state matter for the options below:

- **The architecture already anticipated a second backend.** The `CqlParser`
  trait, `ParserBackend::Antlr`, and the factory's backend-selection logic
  (`cql/factory.rs`) form a ready-made seam — *any* alternative parser (ANTLR
  or a Rust-native grammar) slots in behind it without a refactor.
- **There is no grammar file anywhere today.** `pest`/`pest_derive` are
  declared dependencies with a dormant `pest` feature flag and a stub error
  converter (`cql/error.rs`), but no `.pest` grammar exists and nothing uses
  pest at runtime. So "keep our own grammar file" describes a file we would
  *author*, not one we have. Every production parser today is imperative
  (nom combinators + a bespoke recursive-descent tokenizer for SELECT).

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
   grammar-based parser solves this, and grammar updates are cheap. This is
   true whether the grammar is generated from ANTLR or one we author in a
   Rust-native generator.
2. **Semantics** (turning a parse tree into CQLite's AST, types, and
   execution) — no grammar solves this. The parse-tree-to-`CqlStatement`
   visitor is hand-written Rust, and every new syntax feature still requires
   hand-written mapping code, plus reader/executor support. Cassandra's own
   grammar handles this only because of the embedded Java we'd be stripping
   out — which is exactly the part that makes its grammar non-reusable.

So even in the best case, "pull in the new grammar" automates the *detection
and acceptance* of new syntax, not its *implementation*. That reframing drives
the recommendation: the cheapest, highest-value slice of the original goal is
to *verify and diff* against Cassandra's own parser (Option C), independent of
whatever parses CQL in production — and if we do rewrite the production parser,
to own the grammar ourselves (Option E) rather than bet it on an external
ANTLR toolchain.

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

### Option C — Conformance oracle against Cassandra's own parser (not in the product)

Keep our parsers in production. Build a **dev/CI tool** (separate crate under
`tools/`, e.g. `tools/cql-conformance`) that runs the CQL test corpus (and
fuzzed statements) through both our parser and an authoritative reference, and
reports accept/reject divergence ("CQLite accepts X that Cassandra rejects" and
vice versa), plus a per-release syntax-change report.

The critical refinement over earlier framings: the reference should be
**Cassandra's actual Java parser**, not a third-party grammar. A ~100-line JVM
harness calls `org.apache.cassandra.cql3.QueryProcessor.parseStatement(String)`
(or `CQLFragmentParser`) and reports "does Cassandra accept this, and as what
statement type?" — pinned to whatever Cassandra release we target.

- **Do not use `cqlsh` as the oracle.** cqlsh ships its own hand-written Python
  parser (`cqlshlib/pylexotron.py`), *not* the ANTLR grammar — checking against
  cqlsh checks against the wrong thing. `QueryProcessor` is the real grammar.
- The grammars-v4 cql3 grammar is likewise only a re-rendering; using
  Cassandra's jar removes a layer of drift and an audit burden.

**Pros**
- Delivers the *actual pain-killer* the original goal was about — knowing
  exactly where we diverge from Cassandra and what changed per release —
  without touching the production parse path, the memory budget, or the
  bindings.
- **Eliminates the antlr4rust toolchain risk entirely.** A reference oracle
  needs no *Rust* parser generated at all; it just invokes Cassandra's jar,
  the most authoritative source possible.
- Cheap: no visitor/AST mapping needed — accept/reject (and statement type) is
  enough for conformance checking.
- Independent of the production parser technology, so it pays off no matter
  what we choose in Option A/E/B.

**Cons**
- Does not improve product error messages or close syntax gaps by itself —
  gaps it finds still get fixed by hand in our parser.
- Introduces a JVM into the *CI/dev* path (not the product). Mitigated by
  pinning a Cassandra jar and running the oracle as a scheduled/gated job, or
  capturing its verdicts into a checked-in fixture so day-to-day CI needs no
  JVM.
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

### Option E — Own a Rust-native declarative grammar (pest / lalrpop / winnow / chumsky)

This is the option the original "keep our own grammar file" instinct actually
points at, and it was missing from the first draft. Instead of generating a
parser from someone else's grammar, **author our own grammar in a Rust-native
generator** and make that file the single source of truth, then validate it
against Cassandra via Option C.

The strategic argument: the project's mission is *tracking a grammar*. A
~200-line declarative grammar file is diffable against the CQL spec and against
the oracle's findings in a way that ~3,000 lines of imperative nom +
recursive-descent never will be. You get a single source of truth you can edit
when Cassandra adds syntax — the cheap-syntax-update property that motivated
the ANTLR idea — without depending on a single-maintainer ANTLR fork or a JVM
in the product.

There is **no off-the-shelf CQL parser** to drop in (no crate, no
`sqlparser-rs` Cassandra dialect, no maintained CQL crate), so the grammar is
ours to write regardless. The choice is which framework:

| Framework | Shape | Fit for CQLite |
|---|---|---|
| **pest** | PEG grammar in a declarative `.pest` file | The literal "own grammar file." Readable, single source of truth, already a (dormant) dependency. PEG ordered-choice handles CQL's contextual/keyword-as-identifier cases gracefully. You still hand-write tree→AST; perf below nom. |
| **lalrpop** | LR(1) grammar file, build-time codegen | Also declarative, but CQL's case-insensitive/contextual keywords make the LR lexer fiddly and error recovery weaker than pest for this language. |
| **winnow** | Maintained successor to nom (combinators, not a grammar file) | Lowest-risk modernization: keeps our existing combinator approach, better errors, near-mechanical migration from nom 7. No grammar artifact, so it does *not* satisfy the "own a grammar file" goal — it just improves the status quo. |
| **chumsky** | Combinators with best-in-class error recovery | The real ANTLR competitor on *diagnostics*. Reach for it only if error-message quality (REPL/TUI/LSP) is the driver. Heavier, faster-evolving API. |

**Pros**
- Satisfies the original vision's core — a grammar file as single source of
  truth, cheap syntax updates — with **zero ANTLR/JVM/toolchain risk** in the
  product and Rust-native performance/WASM compatibility (M6).
- pest is already a (dormant) dependency, and the existing `pest` feature flag
  + error-converter scaffolding mean the seam is partly built.
- Consolidates today's two divergent CQL parsers (bespoke SELECT recursive
  descent + nom DDL/DML) onto one declarative artifact.

**Cons**
- Rewriting working parsers into a grammar is real effort, and we still
  hand-write the tree→AST mapping (the same component ANTLR wouldn't automate
  either).
- pest/lalrpop run slower than nom/winnow — relevant given the <128MB budget
  and zero-copy ethos; must be measured.
- It is *our* grammar, not Cassandra's, so it only stays faithful if the
  Option C oracle is in place. Option E and Option C are complementary, not
  alternatives.

## Recommendation

Separate the decision into two **independent axes** — they were conflated in
the original goal and in the first draft of this doc:

1. **Compatibility confidence** (does CQLite accept exactly what Cassandra
   accepts, and can we cheaply detect new syntax per release?). Answered by
   **Option C** — and best done against Cassandra's *own* parser, which removes
   the antlr4rust toolchain risk from this axis entirely. This is the
   high-value, low-risk move and it is independent of whatever parser runs in
   production.

2. **Production parser technology** (what actually parses CQL in the shipped
   library). Here, **ANTLR-in-product (Option B) is no longer the lead
   candidate.** If we invest in the parser at all, **Option E — owning a
   Rust-native declarative grammar (pest)** is better aligned with the mission
   (track a grammar, with a grammar file as the artifact) and carries none of
   ANTLR's toolchain/JVM/WASM risk. If the parser isn't causing real pain
   today, the cheapest defensible move is **winnow** (modernize the existing
   combinators in place) and defer the grammar rewrite until syntax drift or
   error-message quality forces it.

**Sequencing: Phase 1 (oracle) now; Phase 2 (parser tech) gated on a concrete
driver.** Phase 1 front-loads the deliverable the original goal was really
about. Phase 2 is only worth starting once the oracle is telling us *where* we
diverge — that data decides whether a parser rewrite is even warranted, and
which framework. Options B and D (ANTLR-in-product, grammar auto-conversion)
are demoted to fallbacks: pursue only if a Rust-native grammar proves unable to
express CQL cleanly, which the precedents suggest is unlikely.

### Phase 0 — Spike (~1 week, two small prototypes)

1. **Oracle harness:** wrap Cassandra's `QueryProcessor.parseStatement` in a
   minimal JVM tool against a pinned 5.0 jar; feed it a sample of our corpus;
   confirm we can capture accept/reject + statement type as machine-readable
   output.
2. **Grammar prototype:** write a `.pest` grammar for *just* SELECT (the most
   bespoke current parser), wire it through the existing factory seam, and
   measure ergonomics + parse perf/memory vs the current recursive-descent on
   the same statements.

**Exit criteria:** oracle emits structured verdicts for our corpus; pest SELECT
prototype parses the corpus with documented perf numbers. If pest can't express
SELECT cleanly or regresses perf badly, Option E falls back to winnow and the
doc records why.

### Phase 1 — Conformance oracle in CI (~2–3 weeks) — do this regardless

1. New `tools/cql-conformance` crate (dev-only): drives the Phase 0 oracle
   harness against a pinned Cassandra jar.
2. Build the divergence corpus: every statement in `test-data/schemas/`, the
   query strings used across integration/parity tests, plus a curated set of
   valid-and-invalid CQL. CI job reports accept/reject divergence between
   Cassandra and our parser; divergences become tracked issues.
3. Capture the oracle's verdicts into a checked-in fixture so day-to-day CI
   needs no JVM; refresh the fixture (JVM required) when bumping the pinned
   Cassandra version via `scripts/refresh-cql-oracle.sh`.
4. Add a release-watch step: re-run the oracle against the next Cassandra
   release's jar and diff verdicts — surfacing "syntax changed upstream / we
   now diverge on N statements." This is the "pull in the new grammar"
   workflow, realized as authoritative detection.

**Exit criteria:** CI gate reporting zero unexplained divergences on the
corpus; documented per-release sync runbook.

### Phase 2 — Production parser investment (optional, gated)

Proceed only if Phase 1 surfaces a divergence rate that makes hand-maintenance
untenable, or there's a concrete product driver (e.g. rich error
diagnostics/completion for the TUI/REPL).

- **Default path — Option E (pest grammar, ~4–8 weeks):** grow the Phase 0
  SELECT grammar to full CQL; hand-write the parse-tree → `CqlStatement`
  visitor behind the existing factory seam; ship behind a feature flag,
  off by default, with the current parser as fallback until parity holds.
  Differential-test against both the current parser and the Phase 1 oracle
  (same accept/reject *and* same AST). Retire the bespoke parsers once the
  grammar reaches parity.
- **Low-effort alternative — winnow:** if the goal is only to reduce
  maintenance risk on the existing combinators (no grammar-file ambition),
  migrate nom → winnow incrementally; no new artifact, no behavior change.
- **Fallback — Option B (ANTLR backend):** only if a Rust-native grammar can't
  express CQL; reuses the same `antlr_backend.rs` stub and `antlr` feature flag.

**Exit criteria:** parity with the current parser and the oracle on the full
corpus; perf and memory within agreed budgets; gate (`scripts/agent-gate.sh`)
green with the new parser enabled.

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| JVM dependency in CI (oracle needs a Cassandra jar) | Low | Pin the jar; capture verdicts into a checked-in fixture so routine CI needs no JVM; refresh on Cassandra version bumps only |
| pest/lalrpop perf or memory vs <128MB budget (Option E) | Medium (Phase 2 only) | Measure in Phase 0 SELECT prototype; fall back to winnow; keep current parser until parity |
| pest can't express CQL contextual keywords cleanly (Option E) | Medium | Phase 0 prototype is the go/no-go; winnow fallback keeps the combinator approach |
| Visitor (tree → AST mapping) correctness bugs (any new parser) | Medium | Differential testing against current parser *and* oracle on full corpus; sstabledump-style golden parity |
| antlr4rust abandonment (single maintainer) | Low (now a fallback only) | ANTLR demoted to Option B fallback; oracle uses Cassandra's jar, not generated Rust |
| Conflating grammar acceptance with feature support (parser accepts `ANN OF`, executor can't run it) | Medium | Keep "parses" vs "executes" matrices separate; oracle reports are about syntax only |
| Owning our own grammar drifts from Cassandra (Option E) | Medium | Option C oracle is the guardrail; E and C ship together, never E alone |

## Open questions

1. Should divergence findings from the oracle gate CI (hard fail) or report
   (tracked issues)? Proposal: report-only for one release cycle, then gate.
2. Do we want the oracle to also validate `mutation_parser.rs` inputs and the
   schema parser, or start with SELECT/DDL only? Proposal: start with the
   statements the product actually accepts (SELECT, DDL, INSERT/UPDATE/DELETE),
   expand later.
3. Oracle jar sourcing: pin an official Cassandra release jar, or build from a
   tagged source tree? Proposal: pin the release jar matching our target
   Cassandra version; document the bump process.
4. If Phase 2 proceeds with Option E, is **pest** the pick, or is error-message
   quality enough of a driver to justify **chumsky**? Proposal: default to pest
   for the grammar-as-source-of-truth property; switch to chumsky only if the
   REPL/TUI/LSP diagnostics case becomes a funded goal.
5. Does the old `pmcfadin/cassandra-antlr4-grammar` repo (linked from the
   README) have content worth seeding a pest grammar from, or is it ANTLR-only?

## Relationship to prior strategy doc

`docs/technical/CQL_PARSER_IMPLEMENTATION.md` §"Future-Proofing Strategy"
proposed a similar hybrid (nom + ANTLR fallback) but predates the key facts
established here: Cassandra's grammar is ANTLR3-with-actions and not directly
consumable; no official Rust target exists; antlr4rust was revived in 2025 on
stable Rust; and working precedents (Instaclustr `cql_rust`, scql) define the
realistic pipeline. It also lands on a different recommendation: validate
against Cassandra's *own* parser (not a generated Rust ANTLR parser, and not
cqlsh), and — if the production parser is rewritten — prefer a **Rust-native
declarative grammar (pest)** over an ANTLR-in-product backend, which is demoted
to a fallback. Its Phase 3 (ML-assisted parsing) is dropped. This document
supersedes that section for parser-strategy planning purposes.
