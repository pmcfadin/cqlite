# agent-fleet-runtime — worker-supervisor headless launch

## ADDED Requirements

### Requirement: Default worker invocation SHALL be headless-executable

The supervisor's default `WORKER_CMD` (used when the caller does not export one) SHALL launch a
worker that runs non-interactively to completion against a valid agent with the permissions an
unattended session needs. It SHALL invoke the registered `flow-lead` agent (not the non-existent
`worker` agent), run in print/non-interactive mode (`-p`), and skip interactive permission
prompts (`--dangerously-skip-permissions`).

#### Scenario: Default invocation names a registered agent
- **WHEN** the supervisor starts with no caller-provided `WORKER_CMD`
- **THEN** the resolved `WORKER_CMD` SHALL invoke `--agent flow-lead`
- **AND** it SHALL NOT invoke `--agent worker` (which is a slash-command/skill, not an agent type)

#### Scenario: Default invocation is non-interactive
- **WHEN** the supervisor resolves its default `WORKER_CMD`
- **THEN** the command SHALL include the `-p`/`--print` flag so `claude` runs the prompt to
  completion and exits rather than opening an interactive TUI that blocks on keyboard input

#### Scenario: Default invocation runs without per-command approval
- **WHEN** the supervisor resolves its default `WORKER_CMD`
- **THEN** the command SHALL include `--dangerously-skip-permissions` so a supervisor-spawned
  session can run `gh project`, `gh auth`, `git worktree`, and `git -C` without a human approving
  each prompt

#### Scenario: A default-invocation worker reaches the board and writes a marker
- **GIVEN** a reachable board with at least one Ready item OR an empty Ready column
- **WHEN** the supervisor runs one iteration with the default `WORKER_CMD`
- **THEN** the worker SHALL orient against the board and write a well-formed iteration marker
  (`finalized` / `no-work` / `blocked` / `parked-on-owner`)
- **AND** the iteration SHALL NOT be judged `abnormal` due to a spawn failure, an auto-denied
  permission, or an interactive-TUI wedge

### Requirement: Leftover-worker orphan detection SHALL match the corrected spawn shape

The preflight leftover-worker probe SHALL detect an orphaned unattended worker from a prior
iteration under the corrected spawn shape, while excluding a deliberate interactive `claude`
session (a plain REPL or an interactive `--agent flow-lead` lead session) on the same machine.

#### Scenario: An orphaned unattended worker is detected
- **GIVEN** a surviving process whose argv matches the unattended worker spawn shape
  (`claude … -p … --agent flow-lead …`)
- **WHEN** preflight runs its leftover-worker probe
- **THEN** the probe SHALL count it as a leftover-worker and hold the next spawn

#### Scenario: An interactive session is not misdetected as a leftover worker
- **GIVEN** an interactive `claude --agent flow-lead` session with NO `-p` flag, or a plain
  `claude` REPL
- **WHEN** preflight runs its leftover-worker probe
- **THEN** the probe SHALL NOT count it as a leftover worker

#### Scenario: The probe does not match its own wrapper
- **WHEN** the leftover-worker probe evaluates the running process list
- **THEN** the probe's own `bash -c` wrapper (whose argv contains the pattern text) SHALL NOT
  be counted as a leftover (bracket-trick preserved)

### Requirement: The stuck-on-question watchdog SHALL observe a print-mode worker

The supervisor SHALL ensure the mid-iteration stuck-on-question watchdog can still observe worker
activity under print-mode (`-p`), which directs worker activity to the session transcript rather
than stdout. It SHALL do so by capturing the worker's live event stream into the per-iteration log
the watchdog scans, OR it SHALL explicitly document the watchdog as print-mode-incompatible and
name the breaker + wall-clock budget as the operative backstops.

#### Scenario: A healthy print-mode worker produces a non-empty iteration log
- **GIVEN** the default (print-mode) `WORKER_CMD`
- **WHEN** a worker runs and makes progress (tool calls, subagent dispatch)
- **THEN** the per-iteration log the watchdog scans SHALL grow with the worker's activity
  (not remain 0 bytes)

#### Scenario: A wedged print-mode worker is still classified as stuck
- **GIVEN** a print-mode worker whose captured log shows an interactive-prompt signature in its
  tail and whose log size does not grow across two consecutive watchdog scans
- **WHEN** the watchdog evaluates it
- **THEN** it SHALL classify the iteration as `stuck-on-question`, page the owner, and NOT count
  it toward the crash breaker

### Requirement: Fleet doctrine SHALL instruct the working invocation

User-facing fleet documentation SHALL instruct the validated headless invocation and SHALL NOT
instruct the non-functional `--agent worker` form.

#### Scenario: Doctrine names no non-functional invocation
- **WHEN** `docs/development/fleet-runbook.md`, `CLAUDE.md`, and the issue #2090 references are
  read
- **THEN** they SHALL describe the launch using `--agent flow-lead` with `-p` and
  `--dangerously-skip-permissions`
- **AND** a repository grep for `--agent worker` as a launch instruction SHALL return nothing
  (excluding historical/changelog context that documents the fix)
