# worker-environment-preflight

## ADDED Requirements

### Requirement: The machine bootstrap verifies git push credentials, not just `gh` auth

`scripts/bootstrap-agent-machine.sh` SHALL check that a raw `git push` to `origin` can authenticate, and
SHALL NOT treat an authenticated `gh` CLI as evidence that git itself is authenticated — they are separate
credential paths, and the flow tooling (`scripts/flow/claim.sh`, `scripts/flow/claim-heartbeat.sh`) pushes
directly with `git` on 10+ call sites.

When no working credential path is present the check SHALL warn with the exact remediation. Under `--yes`
it SHALL configure one, preferring `gh auth setup-git` when that works and otherwise a helper that
dereferences `$GH_TOKEN` **at call time**. It SHALL NOT write the token itself to disk.

#### Scenario: A box with authenticated gh but no git credential helper is flagged
- **GIVEN** `gh auth status` succeeds and no git credential helper is configured for `github.com`
- **WHEN** `scripts/bootstrap-agent-machine.sh` runs
- **THEN** it reports the git-credential gap as a warning (not an `ok`) and prints the remediation
- **AND** it does NOT report success merely because `gh` is authenticated

#### Scenario: `--yes` configures a credential path that persists no secret
- **GIVEN** the same box
- **WHEN** `scripts/bootstrap-agent-machine.sh --yes` runs
- **THEN** a subsequent `git push` to `origin` authenticates without an explicit `-c credential.helper`
- **AND** no file written by the bootstrap contains the token value

### Requirement: The board check probes the board operation, not the scope string

The bootstrap's board check SHALL verify that the board is actually reachable with the current
credentials, and SHALL NOT report that board dispatch works on the strength of the `project` scope string
alone. A machine has been observed with the `project` scope present where `gh project item-edit` fails for
a missing `read:org` scope while the equivalent `updateProjectV2ItemFieldValue` GraphQL mutation succeeds
with the same token — so a scope match is not evidence the operation works.

The check SHALL name the GraphQL mutation as the supported write path when `gh project` subcommands are
unavailable. The probe SHALL be read-only — a bootstrap SHALL NOT mutate a real board item.

#### Scenario: `project` scope present but `gh project` unusable is reported accurately
- **GIVEN** a token whose scopes include `project` but not `read:org`
- **WHEN** the bootstrap's board check runs
- **THEN** it does NOT print an unqualified "board dispatch works"
- **AND** it names the GraphQL `updateProjectV2ItemFieldValue` fallback as the working write path

#### Scenario: The board probe never mutates board state
- **GIVEN** any credential state
- **WHEN** the bootstrap's board check runs
- **THEN** no board item's field values are modified

### Requirement: An authentication failure is never reported as a retryable transient

`scripts/flow/claim.sh` SHALL distinguish a push that failed because git could not authenticate from a
genuine transient (network/outage) failure, and SHALL NOT emit `transient — retry` for the former.
Observed today: a missing git credential helper surfaces as
`CLAIM: ERROR reason=infra detail=push-rejected-but-ref-absent-on-origin (transient — retry)`, which
directs a worker to retry an operation that can never succeed until the machine is fixed.

#### Scenario: An unauthenticated claim push reports a non-retryable auth verdict
- **GIVEN** a machine with no git credential helper and a free `refs/claims/issue-<N>`
- **WHEN** `claim.sh claim <N>` runs
- **THEN** the emitted verdict identifies the failure as an authentication/configuration problem and names
  the remediation
- **AND** it does NOT describe the failure as transient or advise a retry

#### Scenario: A genuine transient still reports as retryable
- **GIVEN** a machine WITH working credentials and an unreachable remote
- **WHEN** `claim.sh claim <N>` runs
- **THEN** the verdict is still the retryable infra error (no regression in the #2665 contract)

### Requirement: The three deltas are recorded in the fleet doctrine with their identifying symptoms

`docs/development/fleet-runbook.md` (and/or `agent-machine-setup.md`) SHALL record the git-credential
requirement, the explicit `--force-with-lease=<ref>:<sha>` form, and the GraphQL board-write fallback —
each paired with the **failure message that identifies it**, since all three currently fail with messages
that point away from the cause (`fatal: could not read Username`, `stale info`, a `read:org` scope error
on a token that has `project`).

#### Scenario: A worker hitting any of the three symptoms can find the cause by searching the message
- **GIVEN** a worker encounters `fatal: could not read Username`, a `--force-with-lease` `stale info`
  rejection, or a `gh project` `read:org` failure
- **WHEN** they search the fleet doctrine for that message
- **THEN** they find the delta, its cause, and the working form

### Requirement: The new bootstrap checks are covered by the bootstrap self-test

`scripts/tests/test_bootstrap_agent_machine.sh` SHALL cover the git-credential check and the board probe,
including the false-OK case the board check exists to prevent (scope present, operation unavailable), so
a future refactor cannot silently restore a scope-string-only verdict.

#### Scenario: The self-test fails if the board check regresses to a scope-string match
- **GIVEN** a simulated environment with the `project` scope present but `gh project` unusable
- **WHEN** the bootstrap self-test runs
- **THEN** it FAILS if the board check reports unqualified success
