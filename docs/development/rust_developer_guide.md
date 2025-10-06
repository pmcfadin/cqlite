# Rust Developer Guide for AI Contributors

This document defines how our AI agents should deliver high-quality Rust changes for the CQLite workspace. Follow it literally. Anything missing here defaults to the workspace README, `just` recipes, and crate-level CONTRIBUTING notes.

---

## Core Priorities

- Ship production-grade Rust that compiles cleanly, passes tests, and respects ownership rules.
- Prefer clarity and maintainability over cleverness. If a human reviewer needs to decipher intent, rewrite it.
- Preserve the existing architecture: do not introduce abstractions, dependencies, or patterns that the project does not already use without explicit approval.

---

## Minimal Working Process

1. **Frame the task.** Capture the user request in your own words. Identify affected crates, modules, and data flows before editing.
2. **Collect context.** Read the relevant code, docs, and tests. Use `rg` and existing examples instead of guessing.
3. **Plan before typing.** Outline the changes (control flow, data structures, new types). Keep the plan small and verifiable.
4. **Implement deliberately.** Modify only what the plan requires. Match existing style: module layout, naming, error propagation, logging.
5. **Self-review.** Reread the diff. Remove unused imports, dead code, and speculative helpers. Confirm ownership semantics and lifetimes are intentional.
6. **Validate.** Run the narrowest meaningful checks: unit tests for touched crates, `cargo fmt`, `cargo clippy`, or the specific `just` recipe. Capture failures and fix them; do not rely on humans to interpret compiler output. Use `/code-quailty` when you need the full post-change sweep (rust-code-reviewer + fmt/clippy/check/tests).
7. **Report succinctly.** Summarize the change, list validations, and call out skipped work or risks. Provide file:line references for reviewers.

---

## Implementation Standards

### Structure & Style

- Follow `.rustfmt.toml` (4 spaces, 100 columns) and Rust module conventions (`snake_case` files, `CamelCase` types).
- Keep files under 500 LOC when reasonable by splitting along logical boundaries. Prefer modules over large monoliths.
- Maintain explicit imports; avoid glob imports unless already present locally.

### Error Handling

- Libraries: use `thiserror` for domain errors and return typed results.
- Applications/CLI/tests: use `anyhow::Result` for orchestration layers.
- Never leave `unwrap`, `expect`, or `.ok_or_else(|_| panic!)` in production paths. Handle errors or propagate them with context.
- Produce actionable error messages; include the failing operation and key identifiers.

### Ownership, Concurrency, and Performance

- Choose the simplest ownership model that satisfies borrow checker constraints. Avoid cloning to silence borrow errors without analysis.
- Prefer immutable data and pure functions. Use interior mutability (`RefCell`, `Mutex`) only when justified and documented.
- When adding async code, stick to existing runtimes (Tokio). Do not mix runtimes or spawn uncontrolled tasks.
- Optimize only when measurement or prior regressions justify it. Add benchmarks under `benches/` or Criterion integration when performance-sensitive changes are made.

### Data & Serialization

- Respect existing serialization formats, schema versions, and compatibility requirements. Update fixtures in `tests/fixtures`, `test-data/`, or `real_cassandra5_data/` when behavior changes.
- Do not introduce new dependencies for serialization, compression, or cryptography without approval.

### Unsafe and FFI

- Avoid `unsafe` unless already used in the touched area. If unavoidable, keep the block minimal, comment the invariants it relies on, and add tests that exercise the safe wrapper.
- For FFI/WASM crates, run the relevant `just ffi` or `just wasm` recipe and inspect the diff for unexpected symbols.

---

## Testing Expectations

- Write unit tests alongside the code they cover. Integration or regression tests belong under `tests/` with fixtures explained in `tests/README.md`.
- Prefer real components over mocks when the integration path is cheap. If you must stub, justify it in a comment.
- Cover happy paths, edge cases, and failure scenarios introduced or affected by your change.
- Minimum commands to run before declaring success:
  - Formatting: `cargo fmt --all`
  - Lints: `cargo clippy --workspace --all-targets --all-features`
  - Tests: `cargo test --workspace --all-features` or a tighter scope when the change is isolated. Document any scope reduction.
  - Workspace gates: `just check` prior to PRs or when touching shared code.
- If adding new binaries, features, or config flags, add smoke tests or docs illustrating usage.

---

## Using AI Effectively

- Treat compiler output as ground truth. Feed errors back into the plan instead of patching blindly.
- Prefer deterministic prompts: specify target files, APIs, invariants, and failure modes. Avoid vague instructions like "improve" or "optimize".
- Work in small diffs. Apply patches incrementally and re-read the file after each batch to keep context fresh.
- Never assume model knowledge of repository specifics; restate critical conventions inside the working context (error types, feature gates, CLI options).
- Every AI-generated artifact (code, docs, tests) must be human-review ready. Remove scaffolding, dead comments, and exploratory code before finishing.
- Record open questions or unresolved assumptions in the final response so a human reviewer can act on them quickly.

---

## Quick Checklists

### Before Writing Code
- [ ] Restated the task and identified affected crates/modules.
- [ ] Read existing implementations, tests, and docs relevant to the change.
- [ ] Drafted a plan with concrete steps and validation.

### Before Marking the Task Complete
- [ ] Added or updated tests covering new behavior and edge cases.
- [ ] Ran `cargo fmt`, `cargo clippy`, and the necessary `cargo test`/`just` commands.
- [ ] Reviewed the diff for style, ownership, and error handling correctness.
- [ ] Documented user-facing changes (docs, CLI help, changelog) when applicable.
- [ ] Summarized the change and validations, noting any follow-up work or risks.

---

Maintain this guide aggressively. Trim anything that drifts toward marketing copy, outdated tooling advice, or unverified claims. The goal is fast, correct Rust contributions—nothing else.
