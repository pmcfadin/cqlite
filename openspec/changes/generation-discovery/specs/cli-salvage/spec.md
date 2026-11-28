# cli-salvage — delta (generation-discovery, issue #4626)

`cqlite salvage` enumerates generations through the `generation-discovery` module. R7 to R10 and
`salvage-scan` are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD10 — salvage lists the Live set, accepts TocMissing, and never rediscovers its own output

`salvage <table-dir>` SHALL salvage each generation of the module's Live set (Scope Live, TocMissing
accepted: a table with a lost `TOC.txt` is a salvage input). A below-floor generation SHALL be refused
as `UnsupportedVersion`; salvage SHALL NOT attempt to read it. An Orphan SHALL be named and skipped
with the condition, not silently.

#### Scenario: GD10.1 TocMissing generation is salvaged
- **Rule detail and Cassandra reference**: Because Scope Live never recurses, an `--out` directory placed inside the input tree SHALL NOT be rediscovered as an additional generation, closing the loop R9's third bullet describes.
- **Given** `nb-1-big-TOC.txt` deleted
- **When** `cqlite salvage <table-dir>` runs
- **Then** the generation is salvaged and the manifest records `TocMissing`.

#### Scenario: GD10.2 output inside the input tree is not an input
- **Given** a prior run left `<table-dir>/recovered/nb-1-big-*`
- **When** salvage runs again
- **Then** only the generations directly in `<table-dir>` are inputs.

#### Scenario: GD10.3 UUID-id generation is refused by name, not skipped
- **Given** `nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-*` beside `nb-1-big-*`
- **When** salvage runs
- **Then** it refuses naming the UUID-id generation and the unsupported id form (salvage's existing
  sequence-id requirement, `salvage/recover.rs`, now raised from the module's typed id per owner Q41),
  and does not proceed with `nb-1-big` alone.
