## ADDED Requirements

### Requirement: A single enforced version floor rejects pre-`na` SSTables at open with a typed error
CQLite SHALL enforce a single, named minimum-supported SSTable version. A BIG-format SSTable whose
version is below `na` (i.e. Cassandra 3.x `ma`–`me`) SHALL be rejected at descriptor/version-parse time
with a typed `Error::UnsupportedVersion` that names the offending version and the floor, rather than
being rejected only implicitly by magic-number mismatch or an untyped parser error. A BTI-format
SSTable whose version is not `da` SHALL likewise be rejected with that typed error. The rejection SHALL
occur before any row data is read.

#### Scenario: A pre-`na` BIG version is rejected at version parse
- **WHEN** the BIG version gate is constructed from a version string below `na` (e.g. `ma`, `mc`, `me`)
- **THEN** construction returns `Err(Error::UnsupportedVersion { .. })` naming the version and the `na` floor
- **AND** no `nb` (or any) gate is returned for that version

#### Scenario: A non-`da` BTI version is rejected with the typed version error
- **WHEN** the BTI version gate is constructed from a version string other than `da`
- **THEN** construction returns `Err(Error::UnsupportedVersion { .. })` (not a generic `InvalidFormat`)

#### Scenario: Supported 5.0 versions are still accepted
- **WHEN** the version gate is constructed from `na`, `nb`, `oa` (BIG) or `da` (BTI)
- **THEN** construction succeeds and returns the corresponding gates

### Requirement: The reader does not silently downgrade a below-floor SSTable
The reader's descriptor-derived gate path SHALL propagate an `UnsupportedVersion` error to the caller
instead of degrading to the default `nb` fallback. Only a genuinely unparseable / structurally
malformed descriptor MAY use the fallback; a descriptor that parses to a below-floor version SHALL be
fatal at open.

#### Scenario: Opening a below-floor SSTable fails instead of falling back to nb
- **WHEN** a reader opens an SSTable whose descriptor version parses to a pre-`na` value
- **THEN** the open fails with `Error::UnsupportedVersion`
- **AND** the reader does NOT proceed using `nb` fallback gates

#### Scenario: A structurally-unparseable descriptor still tolerates fallback
- **WHEN** a reader opens an SSTable whose descriptor version cannot be parsed as a valid version string at all
- **THEN** the existing fallback behavior is preserved (no `UnsupportedVersion` is raised for an unparseable, not below-floor, descriptor)

### Requirement: No read path contains pre-`na` correctness modeling
The codebase SHALL NOT carry version-gate logic whose purpose is to make pre-`na` (`ma`–`me`) reads
"correct." The dead `BigVersionGates::is_compatible()` method (whose predicate admits `ma`) SHALL be
removed, and `BigVersionGates` SHALL NOT model any below-`na` version as an acceptable read surface.

#### Scenario: The dead pre-`na` compatibility predicate is gone
- **WHEN** the source is searched for `BigVersionGates::is_compatible`
- **THEN** no definition or caller exists in production or test code

#### Scenario: BigVersionGates no longer admits below-floor versions
- **WHEN** any code path constructs `BigVersionGates` for a below-`na` version
- **THEN** it cannot obtain a usable gate (construction errors per the floor requirement); there is no branch that returns valid pre-`na` gates

### Requirement: Doctrine states the Cassandra 5.0 support floor as an explicit rule
`CLAUDE.md` and the published `agents-developing/` doctrine SHALL state the support floor as a rule (not
prose): CQLite targets Cassandra 5.0 — `na`+/`nb` BIG and `oa`/`da` BTI are in scope; pre-`na`
(`ma`–`me`, Cassandra 3.x) is out of scope and SHALL NOT be introduced, supported, or reviewed for
correctness. The doctrine SHALL include the explicit guidance for reviewers (incl. roborev) not to
re-litigate pre-`na` correctness.

#### Scenario: CLAUDE.md carries the Supported formats rule
- **WHEN** `CLAUDE.md` is read after this change
- **THEN** it contains a "Supported formats" rule naming the accepted versions (`na`/`nb` BIG, `oa`/`da` BTI) and the out-of-scope pre-`na` (`ma`–`me`) set
- **AND** it instructs that pre-`na` correctness is not to be introduced, supported, or reviewed

#### Scenario: The agents-developing doctrine mirrors the floor rule
- **WHEN** the `agents-developing/` doctrine source is read after this change
- **THEN** it states the same Cassandra 5.0 floor and the do-not-review-pre-`na` guidance for reviewers/roborev
