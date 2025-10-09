## M2 CLI: Minimal Config File Support (TOML)

### Summary
Add lightweight config file support to reduce command-line verbosity for common operations in M2. The config maps 1:1 to existing flags and integrates with REPL read-only config display. This is explicitly scoped to M2-minimal (no profiles, no saving), matching `M2_CLI_SPEC.md` precedence: CLI > env > project config > user config > defaults.

### Motivation
As a user, repeatedly passing `--schema`, `--data-dir`, `--out`, and pagination flags is verbose and error-prone. A small TOML config file drastically improves ergonomics without changing core behavior or introducing feature creep.

### Scope (In for M2)
- Support `--config <FILE>` and automatic discovery of project and user config files
  - Search order (after `--config`): `./.cqlite.toml` → user config dir (platform-specific)
- Parse TOML config into existing CLI `Config` model
- Keys supported (1:1 with current flags):
  - `data_directory` (string)
  - `schema_paths` (array of strings)
  - `out` ("table" | "json" | "csv")
  - `limit` (integer)
  - `page_size` (integer)
  - `no_color` (bool)
  - `cassandra_version` (string)
  - `auto_detect` (bool)
  - `default_keyspace` (string) [read-only default]
- Precedence: CLI > env > project file > user file > built-in defaults
- REPL: `:config` displays the merged effective configuration (read-only)

### Non-goals (Defer beyond M2)
- Profiles (e.g., `[profiles.prod]`), `--profile`, and `CQLITE_PROFILE`
- `:config save` or `cqlite init` commands
- Discovery include/exclude filters
- Additional schema beyond keys that directly map to existing flags

### Config File Locations
- Explicit: `--config <FILE>` (highest precedence among files)
- Project: `./.cqlite.toml`
- User:
  - macOS: `~/Library/Application Support/cqlite/config.toml`
  - Linux: `$XDG_CONFIG_HOME/cqlite/config.toml` or `~/.config/cqlite/config.toml`
  - Windows: `%AppData%\cqlite\config.toml`

### Environment Variables (already in spec; reiterated)
- `CQLITE_DATA_DIR`, `CQLITE_SCHEMA`, `CQLITE_LIMIT`, `CQLITE_PAGE_SIZE`, `CQLITE_NO_COLOR`, `CQLITE_OUT`

### Example Project Config (M2-minimal)
```toml
# ./.cqlite.toml
data_directory = "./test-data/datasets"
schema_paths   = ["./test-data/schemas"]
out = "table"         # table | json | csv
page_size = 100
limit = 1000
no_color = false
cassandra_version = "5.0"
auto_detect = true
```

### UX Examples
- One-shot with project config:
```bash
cqlite -e "SELECT * FROM ks.users LIMIT 5"
```

- Explicit config path and CLI override (CLI wins):
```bash
cqlite --config ./my.cqlite.toml --out json -e "SELECT id,name FROM ks.users LIMIT 2"
```

- Env var override (env wins over files):
```bash
export CQLITE_OUT=csv
cqlite -e "SELECT * FROM ks.users LIMIT 1"
```

- REPL shows effective config (read-only in M2):
```text
cqlite> :config
data_directory     = ./test-data/datasets
schema_paths       = [./test-data/schemas]
out                = table
page_size          = 100
limit              = 1000
no_color           = false
cassandra_version  = 5.0
auto_detect        = true
source: CLI > ENV > PROJECT_FILE > USER_FILE > DEFAULTS
```

### Error Handling
- Invalid TOML: print a concise error with file path and surface the first few parse errors; continue if `--config` was not specified explicitly; error out if `--config` was explicit.
- Unreadable or missing `data_directory` when required by operation: clear message with next step (e.g., set `--data-dir` or update config).
- Unsupported key values (e.g., `out = "parquet"` in M2): validate and error with acceptable values.

### Acceptance Criteria
- `--config` loads TOML and applies values across one-shot and REPL.
- Automatic discovery of project and user config locations works per OS.
- Precedence strictly enforced: CLI > env > project > user > defaults.
- Only documented keys are accepted; invalid keys are ignored with a warning or cause a helpful error (choose one consistent policy).
- REPL `:config` displays effective values and precedence source summary.
- No regression for existing CLI invocations without any config file.

### Test Plan
Unit tests (Rust):
- Parse: valid/invalid TOML; type mismatches; unknown keys policy.
- Merge order: user < project < env < CLI; assert each layer wins appropriately.
- Path handling: tilde/relative expansion where applicable; arrays of `schema_paths`.
- Value validation: `out` allowable values; numeric bounds for `limit`/`page_size`.

Integration tests (CLI):
- With `.cqlite.toml` present, run one-shot query with no flags; verify defaults applied.
- Set env vars to override file values; verify they win.
- Pass CLI flags to override both env and files; verify CLI wins.
- REPL `:config` prints merged values (snapshot or structured assert).
- Error cases: malformed config file with and without explicit `--config`.

Manual sanity (docs examples):
- macOS and Linux config discovery paths verified.
- Windows path verified (CI optional if available).

### Implementation Notes
- Extend `cqlite-cli/src/config.rs` to support:
  - `Config` struct serde-backed load from TOML
  - `load_user_config()`, `load_project_config()`, `load_from_path(Path)`
  - `merge(base, overlay)` utility with layer ordering
  - Validation for allowable values (e.g., `out`)
- Wire into `cqlite-cli/src/main.rs` so that effective config is resolved before command dispatch.
- REPL: add read-only `:config` rendering using the resolved `Config` and a small formatter.
- Dependencies: `toml` and `serde` (if not already present). For path discovery, `dirs`/`directories` crate.
- Docs: update `M2_CLI_SPEC.md` (config section) and `CLI_USAGE_EXAMPLES.md` to include the above examples.

### Out-of-Scope Follow-ups (post-M2)
- Profiles and `--profile`/`CQLITE_PROFILE` selection
- `:config save` (persist current effective config to project file)
- `cqlite init` to scaffold a `.cqlite.toml`
- Discovery filters (`include_tables`/`exclude_tables`)
- `config print --merged` debugging command

### Risks & Mitigations
- Divergent precedence rules → bake precedence into tests and `:config` output.
- Path portability across OSes → rely on `dirs` crate and add CI matrix checks.

### Definition of Done
- All Acceptance Criteria and Test Plan items implemented and passing.
- Docs updated; examples runnable against `test-data` paths in-repo.
- No linter warnings introduced; CI green.


