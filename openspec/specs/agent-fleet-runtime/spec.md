# agent-fleet-runtime Specification

## Purpose
TBD - created by archiving change worker-supervisor-headless-launch. Update Purpose after archive.
## Requirements
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

### Requirement: Linux bootstrap SHALL provision mold as the cargo link accelerator

On Linux hosts, `scripts/bootstrap-agent-machine.sh` SHALL detect the `mold` linker,
install it via the native package manager when missing and one is available, and otherwise
emit an advisory warning with a cost estimate — following the existing accelerator
`ok/warn` pattern (sccache/nextest). Bootstrap SHALL remain advisory: a missing or
uninstallable mold never fails the run.

#### Scenario: mold present on Linux
- **WHEN** bootstrap runs on a Linux host where `mold` is on PATH
- **THEN** it SHALL report mold `ok` (with version) and proceed to linker configuration

#### Scenario: mold missing with a supported package manager
- **WHEN** bootstrap runs on a Linux host without `mold` and detects apt, dnf, yum, or pacman
- **THEN** it SHALL install `mold` via that package manager (or print the exact command in
  check-only mode)

#### Scenario: mold missing with no supported package manager
- **WHEN** bootstrap runs on a Linux host without `mold` and no supported package manager
- **THEN** it SHALL emit a `warn` naming the estimated cost of linking without mold
- **AND** it SHALL exit successfully without writing any linker configuration

#### Scenario: Darwin behavior unchanged
- **WHEN** bootstrap runs on a macOS host
- **THEN** it SHALL perform no mold detection, no install attempt, and no linker
  configuration, and its output SHALL be byte-identical to pre-change behavior

### Requirement: Per-machine cargo linker configuration SHALL be probe-gated, idempotent, and non-clobbering

When mold is present on Linux, bootstrap SHALL wire it through a delimited managed block in
the per-machine `~/.cargo/config.toml` containing `[target.x86_64-unknown-linux-gnu]` and
`[target.aarch64-unknown-linux-gnu]` sections that link via mold. The block SHALL be
written only after a successful link probe proving the resolved C compiler accepts
`-fuse-ld=mold`; the repo-committed `.cargo/config.toml` SHALL NOT be modified.

#### Scenario: fresh machine gets the managed block
- **GIVEN** a Linux host with mold present and a C compiler that passes the link probe
- **WHEN** bootstrap runs
- **THEN** `~/.cargo/config.toml` SHALL contain exactly one managed block with both Linux
  target sections routing linking through mold

#### Scenario: re-run is idempotent
- **WHEN** bootstrap runs twice on the same host
- **THEN** the managed block SHALL appear exactly once, with no duplicate or conflicting
  target sections

#### Scenario: unrelated user configuration is preserved
- **GIVEN** a `~/.cargo/config.toml` containing user content outside the managed block
- **WHEN** bootstrap runs
- **THEN** all content outside the managed-block markers SHALL be preserved byte-for-byte

#### Scenario: failed link probe writes nothing
- **GIVEN** a Linux host with mold present but no C compiler that accepts `-fuse-ld=mold`
- **WHEN** bootstrap runs
- **THEN** it SHALL emit a `warn` and SHALL NOT write or modify the managed block

#### Scenario: repo config untouched
- **WHEN** bootstrap runs on any host
- **THEN** the repository's committed `.cargo/config.toml` SHALL be unmodified

### Requirement: The gate summary SHALL stamp mold state on Linux hosts

On Linux, every `scripts/agent-gate.sh` summary's `accelerators:` line SHALL carry a
`mold=` token with one of four states: `linked` (binary present and the managed block is
active in the resolved cargo config), `overridden` (managed block active but a non-empty
`RUSTFLAGS` is exported in the gate environment, which suppresses cargo's
`target.rustflags` so the wired `-fuse-ld=mold` is NOT applied), `present-unconfigured`
(binary present, block absent), or `absent` (binary missing). On Darwin the
`accelerators:` line SHALL be unchanged (no mold token).

#### Scenario: configured Linux worker stamps linked
- **GIVEN** a Linux host with mold installed and the managed block active
- **WHEN** any gate mode emits its summary
- **THEN** the `accelerators:` line SHALL contain `mold=linked`

#### Scenario: installed-but-unwired is visible
- **GIVEN** a Linux host with mold on PATH but no managed block
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `mold=present-unconfigured`

#### Scenario: absent is visible
- **GIVEN** a Linux host without mold
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `mold=absent`

#### Scenario: a global RUSTFLAGS override is visible
- **GIVEN** a Linux host with the managed block active AND a non-empty `RUSTFLAGS`
  exported in the gate environment
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `mold=overridden` (never a bare
  `linked`, which would misreport a mold link that env RUSTFLAGS actually suppresses)

#### Scenario: Darwin summary unchanged
- **WHEN** the gate emits its summary on a macOS host
- **THEN** the `accelerators:` line SHALL contain no `mold=` token and SHALL be
  byte-identical in format to pre-change output

### Requirement: The gate summary SHALL stamp perf profiling capability on Linux hosts

On Linux, every `scripts/agent-gate.sh` summary's `accelerators:` line SHALL carry a
`perf=` token, after the `mold=` token, with one of five states read from
`/proc/sys/kernel/{perf_event_paranoid,kptr_restrict}`: `ok` (`perf_event_paranoid <= 0`
AND `kptr_restrict == 0` — unprivileged per-CPU profiling and kernel symbol resolution
both available), `paranoid-<N>` (`perf_event_paranoid = N >= 1`, which forbids CPU-wide
event access and therefore DENIES the `perf stat -C <cpu>` collection the measurement
doctrine mandates), `kptr-restricted` (paranoid permissive but `kptr_restrict != 0`, so
kernel frames resolve to bare addresses — a silent attribution loss), `absent` (the
`/proc` controls are not present, e.g. a container), or `unknown` (present but
unparseable — never guessed). The read SHALL be free, and free SHALL be enforced by a
test rather than asserted in prose: the gate's emit-time path SHALL exec no `perf`, SHALL
spawn no external process, and SHALL contain no command substitution (each `$( )` forks
a subshell, so the token SHALL be returned through a caller-named variable rather than
stdout); the helper SHALL be sourced once per gate run, not per summary. On Darwin the
`accelerators:` line SHALL be unchanged (no `perf=` token), since both controls are Linux
kernel knobs.

#### Scenario: a profileable Linux worker stamps ok
- **GIVEN** a Linux host whose `perf_event_paranoid` is `<= 0` and `kptr_restrict` is `0`
- **WHEN** any gate mode emits its summary
- **THEN** the `accelerators:` line SHALL contain `perf=ok`

#### Scenario: the shipped-image denial is visible
- **GIVEN** a Linux host with `perf_event_paranoid = 4` (the value agent images ship)
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `perf=paranoid-4`, never `perf=ok` —
  a PERMISSION verdict made visible in every pasted block rather than discovered at the
  start of a measurement cycle

#### Scenario: kernel symbols unavailable is visible
- **GIVEN** a Linux host with a permissive `perf_event_paranoid` but `kptr_restrict != 0`
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `perf=kptr-restricted`

#### Scenario: an unreadable or unparseable control is never guessed
- **GIVEN** a Linux host where a `/proc` control is missing or holds a non-integer
- **WHEN** the gate emits its summary
- **THEN** the `accelerators:` line SHALL contain `perf=absent` or `perf=unknown`
  respectively, and SHALL NOT infer a capability from the surrounding state

#### Scenario: Darwin summary unchanged by the perf token
- **WHEN** the gate emits its summary on a macOS host
- **THEN** the `accelerators:` line SHALL contain no `perf=` token

#### Scenario: the free-read cost is enforced by a test, not claimed
- **GIVEN** the gate's emit-time perf path (its token functions plus every helper
  function they reach)
- **WHEN** the tooling suite audits it
- **THEN** the audit SHALL count zero command substitutions statically AND SHALL
  re-execute that same extracted path with an unresolvable `PATH`, asserting the correct
  token, zero spawned subshells and zero attempted external commands — so a
  reintroduced `$( )` or exec FAILS the fast loop instead of surviving as prose

### Requirement: Bootstrap SHALL install and VERIFY the perf sysctl drop-in on Linux

On Linux, `scripts/bootstrap-agent-machine.sh` SHALL install the reboot-surviving
drop-in `/etc/sysctl.d/99-cqlite-perf.conf` (`kernel.perf_event_paranoid = -1`,
`kernel.kptr_restrict = 0`) idempotently, apply it, and then verify the outcome — never
assume it. The verdict SHALL come from reading the values back out of `/proc/sys/kernel`
(a `sysctl` write's return code proves nothing) and from a FUNCTIONAL
`perf stat -C 0 -e cycles` collection requiring BOTH exit 0 AND a non-zero cycle count.
An overall "VERIFIED" report SHALL require BOTH the `/proc` token `ok` AND that functional
pass; a functional result that is not accompanied by both SHALL be reported as PARTIAL
DIAGNOSTIC INFORMATION, explicitly subordinate to the `/proc` verdict, and no run SHALL
emit a non-`ok` token diagnosis and an unqualified "VERIFIED" together. Because
`perf_event_paranoid` restricts UNPRIVILEGED users and ROOT BYPASSES IT, the functional
collection SHALL be attributed to an identity: when bootstrap runs as root it SHALL DROP
PRIVILEGE for the probe where a mechanism exists (`setpriv`, else `runuser -u <name>`, else
`sudo -u '#<uid>'`) targeting an unprivileged identity resolved from `SUDO_UID`/`SUDO_GID`
else the passwd database's `nobody` — never an invented uid — and where no mechanism or no
such identity exists it SHALL label the root result as NOT evidence of unprivileged
capability and SHALL NOT report it as verification.

NO PATH SHALL REACH A CAPABLE/VERIFIED VERDICT FROM AN UNVALIDATED INPUT. Specifically:
the process's own privilege SHALL be determined from an `id -u` that exists, exits 0 and
prints a validated non-negative integer — an unusable `id -u` SHALL yield an explicit
`identity-unknown` state that is NOT evidence of unprivileged capability, and SHALL NEVER
be substituted with an assumed uid; a `SUDO_USER` name SHALL be used only when the passwd
database confirms it resolves to exactly the validated NON-ZERO `SUDO_UID`/`SUDO_GID` and
the name is shell-token safe, otherwise the numeric ids alone SHALL carry the drop; and
the drop-in comparison SHALL be BYTE-exact including trailing newlines, so a file
differing only in a missing final newline or an extra trailing blank line SHALL be judged
NOT current and rewritten.

The privileged destination path SHALL be a hardcoded literal, never derived from the
environment, and every variable the section's control flow reads SHALL be initialised by
the section BEFORE the platform/library guards, so no inherited environment value can
enter a Linux-only implementation on another platform or without its helper library. The
test-only path seams SHALL be inert without their explicit marker, and UNDER the marker
BOTH seams SHALL be MANDATORY: a seam that is not usable SHALL be a loud refusal in the env
guard AND in the path resolvers, so a test-mode run can never fall back to a production
directory and mutate the host.

A seam's usability SHALL be decided by POSITIVE CONTAINMENT and by nothing else: test mode
SHALL take ONE caller-declared sandbox root, and a seam SHALL be usable IFF it is STRICTLY
contained within that root (a resolved-path prefix match with an explicit `/` boundary, so a
sibling whose name merely starts with the root's is outside). Anything not provably inside
the sandbox SHALL be refused. The implementation SHALL NOT decide usability from a list of
forbidden locations or path spellings: such a list cannot be completed — `.`, `..`,
symlinks, `//` (POSIX leaves two leading slashes implementation-defined and `pwd -P` may
preserve them, while on Linux `//etc` opens `/etc`), trailing slashes, bind mounts and
`/proc/self/root/…` all name the same directory — and a set of scattered prohibitions also
lets a NEW seam consumer silently miss them. The sandbox root SHALL itself prove it is a
sandbox by evidence on the filesystem (an absolute, canonically spelled, existing directory
carrying a stamp file), so no environment value alone can nominate a production directory as
the sandbox. EVERY consumer of a seam SHALL route through that one containment check —
writes, write gating, the drop-in path, and every read of configuration (including the
lower-precedence search-path entries and an optional `sysctl.conf` FILE entry, whose parent
SHALL be canonicalized and the resulting file path validated) — and a consumer that does not
SHALL be a failure of the test suite's structural audit rather than an unvalidated path. On
every path that WRITES, gates a write, or reads host configuration, containment SHALL be
judged on the CANONICALIZED candidate and root (`.`, `..` and symlinked ancestors resolved).
The emit-time read path, which writes nothing and is contractually fork-free, MAY apply the
same containment check syntactically, since its guarantee rests on the marker being absent
in production and a mis-accepted spelling there can only read a caller-chosen file.

When the read-back reports a restrictive `perf_event_paranoid`/`kptr_restrict` state, the
diagnostics SHALL NAME the competing configuration files across the COMPLETE
`sysctl --system` search path — `/etc/sysctl.d`, `/run/sysctl.d`,
`/usr/local/lib/sysctl.d`, `/usr/lib/sysctl.d`, `/lib/sysctl.d` and `/etc/sysctl.conf` —
honouring same-basename masking (a basename supplied by a higher-precedence directory makes
the lower copy inert, so naming it would point at a file that is not in effect), and SHALL
distinguish one whose basename sorts AFTER the managed drop-in (an actual override, since
the last assignment wins) from one that sorts before it, and from `/etc/sysctl.conf` (which
is applied after every drop-in and therefore wins regardless of name); a run with no
competitor SHALL say so explicitly. The read-back SHALL happen after EVERY attempted apply, REGARDLESS of the
apply command's exit status, and the apply command's own failure SHALL be reported
separately from the capability verdict — `sysctl --system` applies every drop-in on the
box, so a non-zero exit may belong to an unrelated pre-existing entry, and no wording
SHALL claim "nothing was applied" alongside a good read-back (or the reverse). The
section SHALL be advisory: a box without non-interactive root, without `perf`, or without
the `/proc` controls SHALL warn with an actionable remedy — a write-AND-apply remedy when
the drop-in is missing, an APPLY-ONLY remedy (root-shell or interactive `sudo`, matching
the detected privilege state) when the drop-in is already current — and the run SHALL
still exit 0. On Darwin the section SHALL be an explicit no-op.

#### Scenario: a failing apply whose controls DID take is reported honestly
- **GIVEN** a Linux host where `sysctl --system` exits non-zero (an unrelated
  pre-existing sysctl entry failed) while our controls DID take
- **WHEN** bootstrap runs with `--yes`
- **THEN** it SHALL still read `/proc` back, SHALL report the good verdict from that
  read, SHALL report the command's non-zero exit as a DISTINCT fact about the command,
  SHALL NOT claim that nothing was applied, and SHALL exit 0

#### Scenario: a current drop-in that cannot be applied still prints a runnable remedy
- **GIVEN** a Linux host whose drop-in is already current, whose runtime controls are not
  profileable, and where bootstrap has no non-interactive privilege
- **WHEN** bootstrap runs
- **THEN** it SHALL print an apply remedy runnable ON THAT BOX — `sysctl -q --system` from
  a root shell where no `sudo` binary exists, `sudo sysctl -q --system` where `sudo` needs
  a password — never a diagnosis with no remedy

#### Scenario: an applied value that did not take is reported, not claimed
- **GIVEN** a Linux host where `sysctl --system` exits 0 but `/proc` still reports a
  restrictive value (container, read-only procfs, a later-sorting drop-in or
  `/etc/sysctl.conf` overriding ours)
- **WHEN** bootstrap runs with `--yes`
- **THEN** it SHALL warn that the value did NOT take, SHALL NOT report a successful
  read-back, and SHALL exit 0

#### Scenario: an rc-0 collection with an unusable counter is NOT verified
- **GIVEN** a `perf stat` that exits 0 while reporting `<not supported>`, `<not counted>`
  or a zero count (a virtualised or masked PMU)
- **WHEN** bootstrap runs the functional verification
- **THEN** it SHALL report the capability as NOT verified

#### Scenario: a functional pass never overrides a non-ok /proc verdict
- **GIVEN** a Linux host whose `/proc` reports `paranoid-4` (or `kptr-restricted`) while
  `perf stat -C 0 -e cycles` succeeds with a non-zero count
- **WHEN** bootstrap runs
- **THEN** it SHALL NOT report an unqualified "VERIFIED", SHALL report the functional
  result as partial diagnostic information subordinate to `/proc`, SHALL name `/proc` as
  the authority with the token it read, and SHALL exit 0

#### Scenario: a root-run functional pass is not evidence of unprivileged capability
- **GIVEN** a Linux host where bootstrap runs AS ROOT (e.g. under `sudo`) and
  `perf stat -C 0 -e cycles` succeeds
- **WHEN** a privilege-dropping mechanism and an unprivileged identity are available
- **THEN** bootstrap SHALL run the probe with privilege dropped, SHALL state which
  identity it measured, and only then MAY report VERIFIED (given `/proc` = `ok`)
- **WHEN** no such mechanism or identity is available
- **THEN** bootstrap SHALL label the result as NOT evidence that an unprivileged process
  can profile the box, SHALL NOT report VERIFIED even with `/proc` = `ok`, and SHALL exit 0

#### Scenario: an unknown identity is not evidence of unprivileged capability
- **GIVEN** a Linux host on which `id -u` is missing, exits non-zero, or prints
  unparseable output, while `perf stat -C 0 -e cycles` succeeds and `/proc` reports `ok`
- **WHEN** bootstrap runs
- **THEN** it SHALL report the identity as UNKNOWN, SHALL NOT report VERIFIED, SHALL NOT
  assert that the probe ran as root either, and SHALL exit 0

#### Scenario: an inconsistent SUDO_USER cannot become the drop target
- **GIVEN** `SUDO_UID=1000` with `SUDO_USER=root` (stale or inconsistent) and no `setpriv`
- **WHEN** the privilege-dropping prefix is resolved
- **THEN** the name SHALL be rejected and the drop SHALL use the validated numeric uid
  (`sudo -u '#1000'`), so the probe can never run as root under a "dropped" label

#### Scenario: test mode without a sandbox refuses to act
- **GIVEN** the test-mode marker set, a root identity, and NO path seams set
- **WHEN** bootstrap runs with `--yes`
- **THEN** it SHALL refuse the section with a loud diagnosis, SHALL invoke no privileged
  command, SHALL write nothing (in particular not the real `/etc/sysctl.d` drop-in), SHALL
  claim no verdict, and SHALL exit 0

#### Scenario: a test seam outside the declared sandbox refuses to act
- **GIVEN** the test-mode marker set, a root identity, and a sysctl path seam that is not
  strictly contained in the declared sandbox root — `/tmp/../etc/sysctl.d`,
  `<symlink-to-/etc>/sysctl.d`, `//etc/sysctl.d`, a symlink whose target is outside the
  sandbox, a relative path, or a sibling whose name merely starts with the root's
- **WHEN** bootstrap runs with `--yes`
- **THEN** it SHALL refuse the section with a loud diagnosis NAMING the offending seam, SHALL
  invoke no privileged command, SHALL leave the real drop-in byte- and metadata-unchanged,
  SHALL name no write target at all, and SHALL exit 0
- **AND** the refusal SHALL come from the single containment check, so no per-spelling rule
  is required for any of those forms, nor for a form not yet enumerated

#### Scenario: an unproven sandbox root cannot make containment vacuous
- **GIVEN** the test-mode marker set and a sandbox root that is unset, relative,
  `//`-spelled, non-existent, or an existing directory carrying no sandbox stamp
- **WHEN** any seam consumer resolves a path
- **THEN** it SHALL refuse, naming the sandbox-root variable, so declaring a production
  directory as the sandbox by environment alone cannot succeed

#### Scenario: a seam consumer cannot skip the containment check
- **GIVEN** the helper's source
- **WHEN** the test suite audits every function that dereferences a seam variable
- **THEN** each SHALL route through the containment check, with only an explicitly named and
  justified allowlist (a presence-only predicate and the root reader itself), and finding no
  consumers at all SHALL be a failure rather than a vacuous pass

#### Scenario: re-run writes nothing
- **WHEN** bootstrap runs twice on the same host
- **THEN** the second run SHALL report the drop-in as already current and SHALL NOT
  re-write it, invoking no privileged write command

#### Scenario: a non-canonical drop-in is rewritten
- **GIVEN** an existing drop-in whose bytes differ from the canonical content only in
  trailing newlines (a missing final newline, or an extra trailing blank line), or which
  carries the canonical content followed by a NUL byte and arbitrary trailing bytes
- **WHEN** bootstrap runs with `--yes`
- **THEN** it SHALL NOT report "already current", SHALL rewrite the file, and the result
  SHALL be byte-identical to the canonical content

#### Scenario: a competing sysctl file is named, anywhere on the search path
- **GIVEN** a Linux host whose read-back is non-`ok` and whose `sysctl --system` search path
  holds other files setting `perf_event_paranoid`/`kptr_restrict` — the stock Ubuntu
  `/etc/sysctl.d/10-kernel-hardening.conf`, a later-sorting file in a LOWER-precedence
  directory such as `/run/sysctl.d`, an `/etc/sysctl.conf` entry, and a same-basename copy
  masked by a higher-precedence directory
- **WHEN** bootstrap runs
- **THEN** the diagnostics SHALL name each competing file that is IN EFFECT by path, SHALL
  flag the later-sorting one as an actual OVERRIDE, SHALL flag `/etc/sysctl.conf` as applied
  after every drop-in (winning regardless of name), SHALL state that the earlier-sorting one
  loses to the managed `99-` prefix, and SHALL NOT name the masked copy

#### Scenario: an inherited environment variable cannot enter the section
- **GIVEN** an ambient `PERF_SECTION_OK=1` in bootstrap's environment
- **WHEN** bootstrap runs on macOS, or on a checkout with no `scripts/perf-capability.sh`
- **THEN** the section SHALL take its no-op / missing-library path, SHALL call no helper
  from the Linux-only implementation, and SHALL exit 0

#### Scenario: check mode mutates nothing
- **WHEN** bootstrap runs without `--yes` on a Linux host lacking the drop-in
- **THEN** it SHALL write no file and invoke no privileged mutating command, and SHALL
  print the complete write-AND-apply remedy using the detected privilege prefix

#### Scenario: Darwin bootstrap no-op
- **WHEN** bootstrap runs on a macOS host
- **THEN** the perf section SHALL state that the controls are Linux-only, write nothing,
  and exit 0

