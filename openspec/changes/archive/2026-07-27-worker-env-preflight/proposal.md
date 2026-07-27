# Worker environment preflight — fail loudly on the deltas that silently break a non-macOS worker

## Why

Issue #2942. Three pipeline steps failed on a Linux worker during the #1883 delivery in ways the
doctrine does not describe. Each cost a diagnosis round-trip, and each fails with a message that points
away from the real cause.

1. **`git push` has no credentials.** `gh` is authenticated (`GH_TOKEN`), but git is not, so every raw
   `git push` fails with `fatal: could not read Username for 'https://github.com'`. This is not a corner
   case: `scripts/flow/claim.sh` and `scripts/flow/claim-heartbeat.sh` push on **10+ call sites** — the
   claim ref, adoption CAS, release, heartbeats. The claim protocol itself does not work.
2. **The bootstrap's board check gives a FALSE OK.** `scripts/bootstrap-agent-machine.sh` §3 matches the
   `project` scope string and prints `'project' scope present — board dispatch works`. On this box that
   scope IS present and `gh project item-edit` still fails for a missing `read:org` scope. The check
   validates a scope, not the operation.
3. **Bare `--force-with-lease` fails "stale info"** even when local and remote refs demonstrably match;
   only the explicit `--force-with-lease=<ref>:<sha>` CAS form works.

The unifying defect is not "the docs are incomplete" — it is that **the failures are misattributed**. The
worst instance: `claim.sh` reports a credential failure as a *transient* error and advises a retry —

```
CLAIM: ERROR reason=infra detail=push-rejected-but-ref-absent-on-origin (transient — retry)
```

— so a worker retries a permanently broken operation instead of fixing auth. A preflight that fails loudly
at machine setup is worth more than any amount of prose, because the current failures do not read as auth
problems at the point they occur.

## What changes

- **`scripts/bootstrap-agent-machine.sh`** gains a git-credential check (new section) and its §3 board
  check becomes a **functional probe** rather than a scope-string match.
- **`scripts/flow/claim.sh`** stops classifying an auth failure as `transient — retry`.
- **Doctrine** (`docs/development/fleet-runbook.md` + `agent-machine-setup.md`) records the three deltas
  with the failure message that identifies each.
- **`scripts/tests/test_bootstrap_agent_machine.sh`** covers the new checks.

## Non-goals

- Not fixing the gate poll predicate (#2908), the merge-path silent failures (#2922), closer orphaning
  (#2748), gate disk footprint (#2758), or the claim resume gap (#2945). Cross-linked, separately owned.
- Not changing how credentials are *obtained* — `GH_TOKEN` remains the source of truth.
