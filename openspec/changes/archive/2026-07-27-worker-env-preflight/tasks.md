# Tasks — worker environment preflight

## 1. Git credential check (surface: `scripts/bootstrap-agent-machine.sh`)
- [x] 1.1 New section: detect whether a raw `git push` to `origin` can authenticate, independent of `gh auth status`.
- [x] 1.2 Warn + print remediation when absent; under `--yes` configure `gh auth setup-git` when it works,
      else the `$GH_TOKEN`-dereferencing helper. Never persist the token value.
- [x] 1.3 Verify no bootstrap-written file contains the token.

## 2. Board probe (surface: `scripts/bootstrap-agent-machine.sh` §3)
- [x] 2.1 Replace the scope-string verdict with a READ-ONLY functional probe.
- [x] 2.2 Keep the `project` token-boundary scope match as a cheap pre-filter, not as the verdict.
- [x] 2.3 Name the `updateProjectV2ItemFieldValue` GraphQL fallback when `gh project` is unusable.

## 3. Claim auth verdict (surface: `scripts/flow/claim.sh`)
- [x] 3.1 Detect the credential-failure signature on a push and emit a distinct non-retryable verdict.
- [x] 3.2 Preserve the existing retryable infra verdict for genuine transients (#2665 contract).

## 4. Doctrine
- [x] 4.1 Record all three deltas + identifying symptoms in `fleet-runbook.md` / `agent-machine-setup.md`.
- [x] 4.2 Update CLAUDE.md only if a worker-facing instruction changes.

## 5. Tests
- [x] 5.1 Extend `scripts/tests/test_bootstrap_agent_machine.sh` for the credential check + board probe,
      including the scope-present-but-operation-unavailable false-OK case.
- [x] 5.2 `claim.sh` coverage for auth-failure vs transient classification.

## 6. Gate / review / audit
- [x] 6.1 `--lite` green each round; full gate once pre-merge; review-first; C anchored to this change's specs.
