# binding-panic-firewall Specification

## Purpose
TBD - created by archiving change panic-unwind-profile. Update Purpose after archive.
## Requirements
### Requirement: Binding cdylib artifacts are compiled with `panic = "unwind"`
The shipped Python wheel and Node.js npm prebuild SHALL be compiled with `panic = "unwind"` so the
PyO3/napi-rs `catch_unwind` firewall at the FFI boundary is active. A dedicated named Cargo profile
SHALL provide this by inheriting the release profile and overriding only the panic strategy; the two
binding build definitions SHALL select that profile.

#### Scenario: A `release-unwind` profile inherits release and flips only panic
- **WHEN** the workspace `Cargo.toml` is inspected
- **THEN** it defines a `[profile.release-unwind]` with `inherits = "release"` and `panic = "unwind"`
- **AND** that profile does not restate `lto`, `codegen-units`, or `strip` (they are inherited from release)

#### Scenario: The Python wheel build selects the unwind profile
- **WHEN** the Python wheel build definition (`.github/workflows/python-release.yml` maturin args, and any `[tool.maturin]` pin in `bindings/python/pyproject.toml`) is inspected
- **THEN** the maturin build selects `--profile release-unwind`
- **AND** it does NOT pass `--release` for the shipped wheel

#### Scenario: The Node prebuild selects the unwind profile
- **WHEN** the Node prebuild build definition (`bindings/node/package.json` `build` script, and `.github/workflows/node-release.yml` if it drives the build differently) is inspected
- **THEN** the napi/cargo build selects the `release-unwind` profile
- **AND** it does NOT pass `--release` for the shipped prebuild

### Requirement: The CLI and core continue to use `panic = "abort"`
The change SHALL NOT alter the abort strategy of the workspace release profile; only the two binding
cdylib artifacts use the unwind profile.

#### Scenario: `[profile.release]` still aborts
- **WHEN** the workspace `Cargo.toml` `[profile.release]` block is inspected
- **THEN** it still sets `panic = "abort"`
- **AND** the CLI/core release build (plain `--release`) is unchanged

### Requirement: A fail-closed guard rejects an abort-built binding artifact
There SHALL be an automated guard, runnable locally (reachable from the agent gate), that FAILS if
either binding build definition would produce an abort-compiled artifact — i.e. if a binding build uses
`--release` (or otherwise omits `--profile release-unwind`). The guard SHALL be fail-closed: if the
build definition it inspects is missing or unparseable, the guard SHALL fail rather than pass silently.

#### Scenario: Guard fails when a binding build reverts to abort
- **WHEN** a binding build definition is edited to use `--release` (or to drop `--profile release-unwind`)
- **THEN** the guard fails and names the offending build definition

#### Scenario: Guard passes when both binding builds use the unwind profile
- **WHEN** both the Python wheel and Node prebuild definitions select `--profile release-unwind`
- **THEN** the guard passes

#### Scenario: Guard fails closed on a missing/unparseable build definition
- **WHEN** a build definition the guard inspects is absent or cannot be parsed
- **THEN** the guard fails (it does not treat "not found" as "compliant")

### Requirement: The binary-size and perf delta of the profile switch is recorded
The change SHALL record the observed delta of switching the binding artifacts from abort to unwind:
the cdylib byte size (each artifact, both ways) and a representative scan micro-benchmark (abort vs
unwind), captured in the PR description.

#### Scenario: The PR documents the measured delta
- **WHEN** the PR for this change is reviewed
- **THEN** it contains, for each binding artifact, the abort-vs-unwind cdylib byte size and a scan micro-benchmark comparison

