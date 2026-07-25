# board-label-mirror — enforced one-way Status→label projection

## ADDED Requirements

### Requirement: The status label SHALL be a machine-written projection of board Status

The `project-board-sync.yml` workflow SHALL be the single writer of `status:*` labels, deriving each
OPEN issue's label from its board `Status` on the scheduled reconciliation pass, on
`workflow_dispatch`, and on issue events, so that the label is a trustworthy projection rather than a
hand-maintained value. The mapping SHALL be: Ready→`status:ready`, In Progress→`status:in-progress`,
In Review→`status:in-review`, and Backlog or Done→no `status:*` label.

#### Scenario: A Ready item gets exactly the ready label
- **GIVEN** an OPEN issue whose board Status is `Ready`
- **WHEN** the mirror pass runs
- **THEN** the issue SHALL carry `status:ready`
- **AND** it SHALL NOT carry `status:in-progress` or `status:in-review`

#### Scenario: Status change is reflected and the stale label removed
- **GIVEN** an OPEN issue previously labeled `status:ready` whose board Status is now `In Progress`
- **WHEN** the mirror pass runs
- **THEN** the issue SHALL carry `status:in-progress`
- **AND** `status:ready` SHALL be removed

#### Scenario: Backlog and Done carry no status label
- **GIVEN** an OPEN issue whose board Status is `Backlog` (or `Done`)
- **WHEN** the mirror pass runs
- **THEN** the issue SHALL carry none of `status:ready` / `status:in-progress` / `status:in-review`

#### Scenario: The mirror is idempotent
- **GIVEN** an issue whose label already matches its board Status
- **WHEN** the mirror pass runs twice
- **THEN** the second run SHALL make no label change

### Requirement: A drift detector SHALL fail loud on any label/Status disagreement

After the mirror pass, the workflow SHALL verify every OPEN issue's `status:*` label matches its
board Status and SHALL fail the run (non-zero exit + an `::error::` annotation) on any disagreement,
so a re-drift or an out-of-band label write is surfaced as a red run rather than silently tolerated.

#### Scenario: Seeded mismatch fails the run
- **GIVEN** an OPEN issue whose `status:*` label does not match its board Status (past the auto-add
  grace window)
- **WHEN** the drift detector runs
- **THEN** it SHALL emit an `::error::` naming the issue and exit non-zero

#### Scenario: Consistent board passes
- **GIVEN** every OPEN issue's `status:*` label matches its board Status
- **WHEN** the drift detector runs
- **THEN** it SHALL exit zero with no error annotation

#### Scenario: Missing project token fails loud, never silently skips
- **GIVEN** `PROJECTS_TOKEN` is absent
- **WHEN** the workflow runs
- **THEN** it SHALL fail loud (the existing token guard) rather than silently skip the mirror or
  detector

### Requirement: The label SHALL be discovery-only, never the claim authority

The mirrored label SHALL be usable for cheap server-side discovery of candidate work, but SHALL NOT
be the authority that selects or claims an issue; the claim ref plus a fresh board read at claim time
SHALL remain the sole double-work arbiter.

#### Scenario: Cheap discovery reads the label
- **WHEN** a session needs candidate Ready issues
- **THEN** it MAY use `gh issue list --state open --label status:ready` (server-side, no issue
  bodies) to enumerate candidates

#### Scenario: Claiming still goes through the claim ref
- **GIVEN** a candidate issue discovered via the `status:ready` label
- **WHEN** a session decides to work it
- **THEN** it SHALL acquire `refs/claims/issue-<N>` and re-read the board before proceeding
- **AND** the label alone SHALL NOT be treated as proof the issue is unclaimed or still Ready

### Requirement: Flow skills SHALL stop writing status labels and use the label for discovery

The flow-* skills SHALL NOT write `status:*` labels (they set board Status only, letting the mirror
follow), and SHALL use the label query for cheap candidate discovery where they previously pulled
broad issue lists.

#### Scenario: No skill writes a status label
- **WHEN** the flow-* skill sources are searched for `add-label status:` or `remove-label status:`
- **THEN** no match SHALL be found

#### Scenario: Doctrine describes the label as an enforced discovery mirror
- **WHEN** `CLAUDE.md`, `docs/development/pm-operating-loop.md`, and the website
  `agents-developing/` delivery page are read
- **THEN** they SHALL describe `status:*` as an enforced read-mirror of board Status for discovery,
  with the claim ref + fresh board read as the dispatch/claim authority
