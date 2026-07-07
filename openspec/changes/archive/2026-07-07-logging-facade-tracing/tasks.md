# Tasks — logging-facade-tracing

## 1. Fail-first tests (TDD)
- [ ] 1.1 Add the **bridge-less capture test**: install a `tracing`-only subscriber
      (no `LogTracer`), drive the issue-#586 corruption-warning path, assert the
      event (level + message) is received. Confirm it FAILS on current `main`.
- [ ] 1.2 Add the **grep-guard test**: assert zero `log::{warn,info,debug,error,trace}!`
      event macros in `cqlite-core/src` (word-boundary regex). Confirm it FAILS
      before the sweep.

## 2. Mechanical migration
- [ ] 2.1 Sweep `log::warn!/info!/debug!/error!/trace!` → `tracing::…` across
      `cqlite-core/src` with ast-grep/fastmod (word-boundary; message/fields verbatim).
- [ ] 2.2 Fix imports: `use log::…` → `use tracing::…` or drop for fully-qualified calls.
- [ ] 2.3 Re-run 1.1 + 1.2 until both green.

## 3. Dependency hygiene
- [ ] 3.1 Remove `log` from `cqlite-core/Cargo.toml`; build to confirm no residual
      reference. If irremovable, document the exact transitive reason in the PR.

## 4. Gate + evidence
- [ ] 4.1 `scripts/agent-gate.sh` PASS — paste the SUMMARY block in the PR.
- [ ] 4.2 PR states the option taken (Option 1) and the final migrated-site count.

## Non-goals (guard against scope creep)
- No span changes; no message-content edits (AG5 owns content); no CLI subscriber changes.
