# Design: worker environment preflight

## Context

`scripts/bootstrap-agent-machine.sh` (480 lines) is the established "check everything, print the fix"
entry point, with `--yes` to auto-apply, a self-test at `scripts/tests/test_bootstrap_agent_machine.sh`
(429 lines), and an idiom of `ok`/`warn`/`info` + `run_or_print`. Sections already cover the Rust
toolchain, accelerators, mold, gh auth, roborev, datasets, and the single-gate default. It is the right
home: workers are told to run it, and `fleet-runbook.md` already points at it.

`worker-supervisor.sh` has a bounded `preflight_wait`/`preflight_reason`, but that is a *per-iteration*
hold for leftover processes — the wrong layer for a machine-provisioning fact that cannot self-clear.

## Decision 1 — Fix at bootstrap (check + optional auto-fix), not at per-iteration preflight

**Chosen.** These are machine-provisioning facts. They do not change between iterations, so paying the
check once at setup is correct, and `--yes` can remediate. Putting them in the supervisor's per-iteration
preflight would either spin (a hold that never clears — explicitly guarded against by #2670) or re-check
an invariant hundreds of times.

**Rejected — supervisor preflight.** The supervisor's hold loop is bounded precisely because a
non-self-clearing hold is a hang. A missing credential helper never self-clears.

## Decision 2 — Auto-configure the credential helper, but write NO secret to disk

**Chosen.** Configure a helper that reads `$GH_TOKEN` from the environment at call time:

```
git config --global credential.helper '!f(){ echo username=x-access-token; echo password=$GH_TOKEN; };f'
```

The token is **never written to disk** — the helper is a shell snippet that dereferences the env var when
git asks. Rotating `GH_TOKEN` needs no reconfiguration, and a leaked `~/.gitconfig` leaks no credential.

**Rejected — `gh auth setup-git`.** It is the obvious answer and may be the better one on boxes where it
works; it configures `gh` as the credential helper. It is listed as the preferred form when available,
with the env-var helper as the fallback, because the failure observed here is precisely that the `gh`
credential path was not wired.

**Rejected — writing the token into `~/.git-credentials`.** Persists a secret in plaintext, and survives
rotation as a stale credential. This change deliberately does not do it.

**Security note.** Under `--yes` this writes a helper to the user's global git config. That is a real (if
small) posture change on a shared box, so it is called out for owner approval rather than slipped in.

## Decision 3 — The board check becomes a FUNCTIONAL probe

**Chosen.** Replace the scope-string match with an actual attempt against the board, because the observed
failure is a scope-string match passing while the operation fails. Probe read-only, then report which
write path works:

- `gh project item-list` / a `projectV2` GraphQL read to confirm reachability;
- report explicitly that `gh project item-edit` may fail for `read:org` while the
  `updateProjectV2ItemFieldValue` GraphQL mutation succeeds with the same token, and name the mutation as
  the supported fallback.

Keep the scope check as a *cheap pre-filter* (its `project`-token-boundary matching is careful and worth
keeping), but it may no longer be the thing that prints "board dispatch works".

**Rejected — probing with a write.** A bootstrap must not mutate a real board item as a side effect.

## Decision 4 — `claim.sh` must not call an auth failure "transient"

**Chosen.** Distinguish "push rejected because unauthenticated" from a genuine transient. Emitting
`transient — retry` for a permanent failure is worse than a bare error: it tells the worker to do the one
thing guaranteed not to help. Detect the credential signature and emit a distinct, non-retryable verdict
naming the fix.

## Decision 5 — `--force-with-lease` is documented, not mechanized

**Chosen.** Document the explicit `=<ref>:<sha>` form in the runbook. No code change: the flow scripts
already use the explicit form (`claim.sh:30,385,481`); the bare form only bites a human or agent typing
it ad hoc. Mechanizing would mean wrapping git, which is disproportionate.
