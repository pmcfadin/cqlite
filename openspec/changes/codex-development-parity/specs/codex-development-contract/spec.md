## ADDED Requirements

### Requirement: Route development through specifications
Codex SHALL use approved OpenSpec artifacts for design-driven features and an authoritative regression test for oracle-driven fixes.
#### Scenario: New CLI feature
- **WHEN** a new CLI feature has no approved proposal
- **THEN** the agent produces proposal, design, scenarios and tasks before implementation and obtains any missing owner design approval
#### Scenario: Cassandra regression
- **WHEN** the request fixes behavior defined by Cassandra
- **THEN** the agent uses the issue acceptance criteria and a pinned Cassandra parity regression rather than requiring a new design proposal

### Requirement: Require meaningful completion evidence
Codex SHALL distinguish implementation progress from verified completion and merged delivery.
#### Scenario: Helper-only coverage
- **WHEN** tasks are checked but the public surface is untested
- **THEN** the spec and coverage reviews report CHANGES NEEDED
#### Scenario: Missing fixtures
- **WHEN** fixture-backed tests skip because the corpus is absent
- **THEN** the agent reports unverified coverage rather than claiming those scenarios passed
#### Scenario: Source changed after certification
- **WHEN** source changes after a full gate pass
- **THEN** the previous certification cannot certify the new revision
#### Scenario: Unmerged implementation
- **WHEN** implementation tasks are complete but the PR has not merged
- **THEN** the agent leaves the OpenSpec change active and does not finalize delivery

### Requirement: Discoverable compatible guidance
Codex SHALL have a workflow skill, current domain and validation guidance, and read-only spec and coverage reviews without requiring Claude-only tools.
#### Scenario: Fresh session
- **WHEN** a session loads AGENTS.md and the routed skills
- **THEN** it can find the workflow and review instructions, use available Codex tools, and resolve all shared skill targets
