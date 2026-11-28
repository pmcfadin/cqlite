# cli-rebuild — delta (generation-discovery, issue #4626)

`cqlite rebuild` enumerates a table directory's generations through the `generation-discovery` module
with Scope Live and TocMissing accepted. Existing requirements are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD4 — The table-directory run lists generations through the module and names every refusal

`cqlite rebuild <table-dir>` SHALL build its per-generation plan from the module's Live set. It SHALL
NOT scan the directory itself. A `UnsupportedVersion` generation SHALL be a manifest entry with
`refused` set and the whole run rolled back as R7.4 specifies. Orphans SHALL appear in the manifest and
SHALL NOT be rebuilt. `snapshots/` and `backups/` SHALL NOT be entered.

#### Scenario: GD4.1 below-floor generation refuses the run
- **Given** a table directory with `nb-2-big-*` and `ma-1-big-Data.db`
- **When** `cqlite rebuild <table-dir>` runs
- **Then** exit `2`, the manifest entry for `ma-1-big` is `refused` with `UnsupportedVersion`, and
  per R7.4 no output subdirectory is left.

#### Scenario: GD4.2 snapshots are not rebuilt
- **Given** a table directory with `nb-1-big-*` and `snapshots/pre/nb-1-big-*`
- **When** `cqlite rebuild <table-dir>` runs
- **Then** exactly one generation is rebuilt: the manifest has one entry and `--out` holds one `Data.db` (a single live generation is written flat per R7.1), and nothing under `snapshots/` is read.
