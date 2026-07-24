# agent-fleet-runtime — mold linker on Linux agent workers

## ADDED Requirements

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
`mold=` token with one of three states: `linked` (binary present and the managed block is
active in the resolved cargo config), `present-unconfigured` (binary present, block
absent), or `absent` (binary missing). On Darwin the `accelerators:` line SHALL be
unchanged (no mold token).

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

#### Scenario: Darwin summary unchanged
- **WHEN** the gate emits its summary on a macOS host
- **THEN** the `accelerators:` line SHALL contain no `mold=` token and SHALL be
  byte-identical in format to pre-change output
