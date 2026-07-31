# test-fixture-roots — delta for schemas-root-contract (issues #3148, #3131)

**Read this first.** The committed CQL schema fixtures (`test-data/schemas/`, 23 files incl. `legacy/`
and `udts/`) and the fetched SSTable corpus (`test-data/datasets/`) are **different kinds of thing** with
**different owners**. The contract below separates them: the corpus root is owned by
`CQLITE_DATASETS_ROOT` (relocatable, fetched, may legitimately be absent); the schemas root is owned by
the **checkout** (committed source, always present). No requirement below permits the second to be
derived from the first.

**Acceptance-criterion → requirement map.**

| AC | Requirement(s) |
|----|----------------|
| **#3148 (a)** FULL-gate preflight FAILS CLOSED with a named error + remedy when the schemas root is missing/unreadable | *The FULL gate fails closed when a consumed schema fixture is unreadable*; *The preflight validates individual schema files, not directory existence* |
| **#3148 (b)** failure text names the exact absolute path + fix command; marker distinguishable from `missing-fixtures:` | *The FULL gate fails closed when a consumed schema fixture is unreadable*; *A relative schemas-root override is rejected fail-closed* (a relative value would otherwise be printed on the `expected absolute path:` line) |
| **#3148 (c)** positive-control self-test proves the preflight FAILS on a schemas-less root | *The schemas preflight decision is exposed as a pure hook and positively controlled* |
| **#3148 (d)** schemas-root resolution exists once, used by every caller; no open-coded `join("../schemas")` | *Schemas-root resolution has exactly one definition, used by every schemas-consuming fixture site* (scope stated in that requirement); *An unreadable schema fixture fails with an actionable, path-naming message* |
| **#3148 (e)** the two divergent `datasets_root()` implementations reconciled or documented | *The datasets root has one implementation with two documented shapes* |
| **#3148 (f)** the symlinked-`datasets` layout works or is rejected, never silently mis-resolved | *The schemas root is independent of the datasets root* |
| **#3148 (g)** `--lite` and `--only` remain lenient | *Only the FULL gate is strict about fixture roots*; *A summary line SHALL NOT assert a check the running mode did not perform* |
| **#3148 (h)** scope decision recorded | *The schemas root is resolved checkout-relative by decision*; *A relative schemas-root override is rejected fail-closed* |
| **#3131 (1)** one documented root that works | *The schemas root is independent of the datasets root*; *The schemas root is resolved checkout-relative by decision* (which supersedes #3131's item-1 either/or — see `design.md`) |
| **#3131 (2)** `fetch-datasets.sh` must not exit 0 without leaving a usable root | *A dataset fetch reports success only with a verified, named root*; *The guaranteed export line is pasteable, not merely printed*; *Dataset-root usability is probeable without mutating anything*; *Every unrecognized argument to the dataset fetch is rejected fail-closed* |
| **#3131 (3)** preflight detects the schemas half, not just the corpus | *The FULL gate fails closed when a consumed schema fixture is unreadable*; *The preflight validates individual schema files, not directory existence* |

## ADDED Requirements

### Requirement: The schemas root is resolved checkout-relative by decision
The committed CQL schema fixtures SHALL be located relative to the **checkout** (anchored on
`CARGO_MANIFEST_DIR`), and SHALL NOT be derived from `CQLITE_DATASETS_ROOT` by any means, including
`join("..")`. An explicit `CQLITE_SCHEMAS_ROOT` SHALL be honored when it is set, non-empty and names a
readable directory; otherwise resolution SHALL fall through to the checkout rather than fail. This is a
recorded scope decision (#3148 AC (h), proposed fix 4): because a checkout always holds these files, the
"absent schemas root" failure mode is structurally impossible rather than merely detected.

#### Scenario: A machine-local corpus with no schemas sibling resolves correctly
- **GIVEN** `CQLITE_DATASETS_ROOT` points at a corpus root whose parent directory contains no `schemas/` directory
- **AND** `CQLITE_SCHEMAS_ROOT` is unset
- **WHEN** a test or bench loads a fixture that needs a `.cql` schema
- **THEN** the schema resolves from the checkout's `test-data/schemas` and the fixture loads successfully

#### Scenario: The checkout is identified by a checkout marker, not by the fixtures
- **GIVEN** a checkout whose `test-data/schemas` is absent (a sparse checkout, or a worktree created inside another checkout that DOES have fixtures)
- **WHEN** the checkout's `test-data` directory is resolved
- **THEN** it resolves to THIS checkout's own root — identified by the workspace-root manifest — and a missing `test-data/schemas` under it fails loudly naming that path
- **AND** it SHALL NOT silently resolve to an enclosing checkout's fixtures

#### Scenario: An out-of-tree run may override the schemas root
- **GIVEN** `CQLITE_SCHEMAS_ROOT` is set to an ABSOLUTE, readable directory holding the schema fixtures
- **WHEN** a schema fixture is resolved
- **THEN** the override directory is used, and the resolution source is reported as the override

#### Scenario: An unusable absolute override degrades to the checkout rather than breaking every load
- **GIVEN** `CQLITE_SCHEMAS_ROOT` is set to an empty string, or to an ABSOLUTE path that is not a directory
- **WHEN** a schema fixture is resolved
- **THEN** the checkout-relative root is used instead of the unusable override

### Requirement: A relative schemas-root override is rejected fail-closed
`CQLITE_SCHEMAS_ROOT` SHALL be an absolute path. A relative value SHALL be REJECTED — by the resolver and by
the gate preflight alike — and SHALL NOT be resolved against the current working directory. The rejection
message SHALL name the offending value, state why relative values cannot be honored, and give a remedy.

**Why this is fail-closed rather than best-effort.** The gate evaluates the override with CWD = repository
root, while cargo runs each test binary with CWD = the *package* directory. A relative value therefore
resolves to two different places, and the gate would stamp a SUMMARY certifying one schemas root for a run
whose tests read another — the "positively misleading `STATUS: OK`" defect this change exists to remove,
reintroduced by its own fix. Rejection removes the one input class on which the two mirrors could not
possibly agree. Note the guarantee's real strength: the gate's shell resolution and the Rust resolver are two
HAND-WRITTEN mirrors kept **equivalent and pinned by self-tests** — not equivalent by construction. They have
been walked case by case over the whole input table (unset, `""`, whitespace-only, `"  /abs  "`,
absolute-non-dir, absolute-dir, relative) and agree on every one, and each case is asserted against the real
gate.

#### Scenario: A relative override fails the resolver
- **GIVEN** `CQLITE_SCHEMAS_ROOT` is a relative path such as `packaged/schemas`, `./schemas` or `../schemas`
- **WHEN** the schemas root is resolved
- **THEN** resolution fails with a message naming the value, the CWD asymmetry, and the remedy — it does NOT silently use either candidate directory

#### Scenario: A relative override fails the FULL gate closed, under its own reason
- **GIVEN** a FULL gate run with a complete corpus, complete checkout fixtures, and a relative `CQLITE_SCHEMAS_ROOT`
- **WHEN** the preflight runs
- **THEN** the run exits non-zero with the schemas fail-closed marker naming the rejected relative override, stamps NO positive `schemas:` line, and does NOT report a list of "missing" files (the checkout's fixtures are in fact complete)

#### Scenario: An override containing a control character is rejected
- **GIVEN** `CQLITE_SCHEMAS_ROOT` names an EXISTING absolute directory but carries a control character (a trailing newline, a CR, an embedded tab)
- **WHEN** the schemas root is resolved, on either side of the contract
- **THEN** it is REJECTED with a message naming the rule and a remedy
- **AND** the gate SHALL NOT report it as a validated override with the control character silently removed

#### Scenario: A blank override is not an override
- **GIVEN** `CQLITE_SCHEMAS_ROOT` is set to an empty or whitespace-only value
- **WHEN** the schemas root is resolved
- **THEN** it is treated as unset and the checkout-relative root is used, on both the resolver and the gate side

### Requirement: The schemas root is independent of the datasets root
The resolved schemas root SHALL NOT vary with the value, shape or existence of `CQLITE_DATASETS_ROOT`. No
Rust code SHALL contain an open-coded `join("../schemas")` expression. Because `join("..")` is resolved by
the kernel against a **symlink target's** parent rather than lexically, a layout reached through a
symlinked `datasets` directory previously mis-resolved silently; this requirement removes the possibility
rather than detecting it.

#### Scenario: Real, symlinked and absent datasets roots resolve the same schemas root
- **GIVEN** three layouts — a real corpus directory, a symlink to that directory, and a path that does not exist
- **WHEN** the schemas root is resolved under each in turn
- **THEN** all three yield the identical checkout-relative schemas root

#### Scenario: Reintroducing the parent-climb idiom fails the gate
- **GIVEN** a change that re-adds an open-coded `join("../schemas")` expression in Rust code
- **WHEN** the agent gate runs its shell-tooling component set
- **THEN** the run FAILs and names the offending file and line
- **AND** a doc comment that merely quotes the retired idiom does NOT fail it

### Requirement: Schemas-root resolution has exactly one definition, used by every schemas-consuming fixture site
There SHALL be exactly one definition of schemas-root resolution, and every site **in this change's scope**
SHALL use it — including sites in more than one crate. A site in scope SHALL NOT re-implement the resolution
locally.

**Scope, stated precisely so the requirement does not overclaim.** "Every caller" means the four sites #3148
enumerates — the ones that derived the schemas root from `CQLITE_DATASETS_ROOT` by climbing `..` with no
fallback, and therefore *hard-failed* on a corpus root without a `schemas` sibling:
`cqlite-core/benches/fixtures/mod.rs`, `cqlite-core/tests/dead_cache_delete_tests.rs`,
`cqlite-core/tests/observability_correctness.rs`, `cqlite-cli/benches/export_csv.rs`.

Approximately fifteen further `cqlite-core/tests/**` files resolve a schemas directory with a *different*
idiom — `datasets_root.parent()?.join("schemas")` tried **first and then a
`CARGO_MANIFEST_DIR`-anchored checkout fallback** (e.g. `issue_1143_windowed_scan_straddle_parity.rs:71,77`,
`issue_693_writetime_threading.rs:42,48`, `issue_1562_perf_gate_access_path.rs:50,56`). Because they already
fall back to the checkout, they DEGRADE correctly on the layout that broke the four sites above; they are
therefore out of scope here, and no requirement in this delta asserts anything about them. Consolidating them
is a follow-up, not a silent claim of this change.

#### Scenario: The requirement's scope is the four hard-failing sites, not every schemas reference
- **GIVEN** the ~15 test files that try a datasets-sibling schemas path and then fall back to a checkout-anchored path
- **WHEN** the single-definition guard runs
- **THEN** it asserts only the four in-scope sites, and does NOT report those ~15 as violations

#### Scenario: All four historical call sites route through the single definition
- **GIVEN** the four sites that previously open-coded the resolution (`cqlite-core/benches/fixtures/mod.rs`, `cqlite-core/tests/dead_cache_delete_tests.rs`, `cqlite-core/tests/observability_correctness.rs`, `cqlite-cli/benches/export_csv.rs`)
- **WHEN** the shared-definition guard runs
- **THEN** each site is confirmed to include the shared module, and no bench or migrated test reads `CQLITE_DATASETS_ROOT` directly

#### Scenario: A second crate consumes the same definition without a new crate dependency
- **GIVEN** `cqlite-cli`'s `export_csv` bench, which is not permitted to depend on `cqlite-core`'s bench internals
- **WHEN** it resolves a schema fixture
- **THEN** it uses the same shared definition as `cqlite-core`, and the build succeeds with `-D warnings`

### Requirement: An unreadable schema fixture fails with an actionable, path-naming message
When a requested schema fixture cannot be read, the failure message SHALL name the resolved **absolute**
path, the resolved root, how that root was chosen, and the remedy. A bare "path does not exist" raised
from inside ingestion SHALL NOT be the observable failure.

#### Scenario: A missing fixture names its absolute path and the remedy
- **GIVEN** a schemas root that does not contain `basic-types.cql`
- **WHEN** a test requests that fixture
- **THEN** the failure names the absolute `.cql` path, the root and its source, states that these fixtures are committed source not fetched data, and gives the remedy

### Requirement: The datasets root has one implementation with two documented shapes
The fetched-corpus root SHALL have a single implementation exposing two documented shapes: an
**infallible** shape with a checkout-relative fallback, and a **fallible** shape that returns nothing
unless `CQLITE_DATASETS_ROOT` is set and names a directory. The difference SHALL be documented as a
deliberate behavioral choice with a stated reason, and each existing consumer SHALL retain its current
observable behavior.

#### Scenario: A skip-gated test still skips when the corpus root is unset
- **GIVEN** `CQLITE_DATASETS_ROOT` is unset
- **WHEN** a test that uses the fallible shape runs
- **THEN** it skips rather than running against the checkout's committed byte-parity references, and does not report a passing result over zero rows

#### Scenario: A bench still runs from a plain checkout with no environment setup
- **GIVEN** `CQLITE_DATASETS_ROOT` is unset and the checkout holds a fetched corpus
- **WHEN** a bench that uses the infallible shape runs
- **THEN** it resolves the checkout-relative corpus root

### Requirement: The FULL gate fails closed when a consumed schema fixture is unreadable
The FULL agent gate SHALL FAIL CLOSED, before running any compilation or test component, when any schema
fixture its dataset-backed components consume is unreadable. The emitted SUMMARY SHALL carry a marker
**textually distinguishable** from the #2078 corpus marker, and a remedy line naming the exact expected
absolute path and the fix commands. Reporting `STATUS: OK` for such a layout SHALL NOT occur. There SHALL
be no environment opt-out that permits a run to certify with the schemas root unreachable.

#### Scenario: A complete corpus with an unreachable schemas root fails closed at the preflight
- **GIVEN** a dataset root whose canonical corpus is present
- **AND** a schemas root that holds none of the consumed `.cql` files
- **WHEN** the FULL gate runs
- **THEN** it exits non-zero at the preflight without running a cargo component
- **AND** the SUMMARY carries `missing-schemas: FAIL-CLOSED (#3148)` and `RESULT: FAIL`, and never `RESULT: PASS`

#### Scenario: The two fixture failure causes are separable in a pasted SUMMARY
- **GIVEN** a run that fails on the schemas half while the corpus half is complete
- **WHEN** the SUMMARY is read
- **THEN** it carries the schemas marker and does NOT carry the `missing-fixtures:` corpus marker

#### Scenario: The failure names the absolute path and the fix commands
- **GIVEN** the schemas-unreachable failure above
- **WHEN** the SUMMARY and stderr are read
- **THEN** they name the exact expected absolute `.cql` path, the override to unset, and the command that restores the committed fixtures

#### Scenario: A successful check is visible in the SUMMARY
- **GIVEN** a FULL gate run whose schemas root is complete
- **WHEN** the SUMMARY is read
- **THEN** it carries a positive line naming the validated root and how it was resolved, so the block shows the check ran

### Requirement: The preflight validates individual schema files, not directory existence
The preflight SHALL assert **readability of each specific schema file** the gate's dataset-backed
components consume, and SHALL report exactly which files are unreadable. A schemas root that exists but is
incomplete SHALL NOT pass.

#### Scenario: A present-but-incomplete schemas root is rejected, naming only the absentees
- **GIVEN** a schemas root holding two of the consumed `.cql` files and missing the rest
- **WHEN** the preflight decision is evaluated
- **THEN** it is FAIL, and the reported unreadable list contains exactly the missing files and none of the present ones

### Requirement: Only the FULL gate is strict about fixture roots
`--lite` and `--only` SHALL remain lenient with respect to the schemas root, unchanged from the #2078
contract. A lenient mode SHALL NOT emit the schemas failure marker.

#### Scenario: --lite is unaffected by an unreachable schemas root
- **GIVEN** an unreachable schemas root
- **WHEN** `--lite` runs
- **THEN** it exits zero with a LITE summary block carrying no schemas marker

#### Scenario: --only stays lenient even for a dataset-backed selection
- **GIVEN** an unreachable schemas root and an `--only` selection naming a dataset-backed component
- **WHEN** the preflight decision is evaluated
- **THEN** it is OK

#### Scenario: --only stays lenient for a rejected relative override too
- **GIVEN** a relative `CQLITE_SCHEMAS_ROOT` and an `--only` selection naming a dataset-backed component
- **WHEN** the preflight runs
- **THEN** it returns without failing the run — the strict rejection path SHALL NOT bypass the lenient-mode check

### Requirement: A summary line SHALL NOT assert a check the running mode did not perform
The SUMMARY block SHALL NOT carry a positive assertion about the schemas root in a mode that did not
validate it. In a lenient mode **that reached the preflight** the block SHALL instead carry an explicitly
NAMED non-check. Omitting the line silently is insufficient there: a reader of a pasted block would assume
the FULL contract held, which is the same false-confidence failure as a misleading `STATUS: OK`.

**Why the qualifier is load-bearing, not hedging.** Two lenient modes never reach the preflight at all, and
for them there is nothing to name: `--lite` returns before it, and an `--only` selection with no
dataset-backed component skips the whole dataset preflight block. Those blocks carry no schemas line, which
is correct — the requirement is about a mode that ran the preflight and was let through leniently, not about
modes for which the preflight is not part of the run. Without the qualifier this requirement would
contradict its own `--lite` scenario below.

#### Scenario: A lenient --only run names the non-check instead of asserting readability
- **GIVEN** an `--only` selection naming a dataset-backed component, with a schemas root holding none of the consumed `.cql` files
- **WHEN** the preflight runs
- **THEN** the stamped schemas line reads as an explicit "not checked", naming the lenient mode
- **AND** it SHALL NOT claim that any number of `.cql` files are readable

#### Scenario: The positive assertion still appears when the check did run
- **GIVEN** a FULL-mode preflight over a complete schemas root
- **WHEN** the preflight runs
- **THEN** the stamped schemas line asserts the validated count and the resolved root, so the non-check case above is not satisfied by simply never stamping anything

#### Scenario: A --lite block carries no schemas line at all
- **GIVEN** a `--lite` run with an unreachable schemas root
- **WHEN** the LITE summary block is read
- **THEN** it contains neither the failure marker nor any positive schemas assertion

### Requirement: The schemas preflight decision is exposed as a pure hook and positively controlled
The preflight's decision SHALL be available as a side-effect-free hook reporting the decision, the
resolved root, its source and any unreadable files, so a self-test asserts the **same** decision the real
gate consumes. A self-test SHALL prove the preflight **REJECTS** at least one unusable layout; proving
only that a good layout passes SHALL NOT be sufficient coverage.

#### Scenario: The hook reports the same decision the gate acts on
- **GIVEN** a schemas-less root
- **WHEN** the pure hook is invoked
- **THEN** it reports a FAIL decision, the resolved root, the resolution source, and every unreadable file, without running any component

#### Scenario: The self-test runs in the gate and fails on a weakened preflight
- **GIVEN** the agent gate's shell-tooling component set
- **WHEN** it runs
- **THEN** the positive-control self-test executes, and a preflight that stopped rejecting an unusable layout FAILs the component

### Requirement: A dataset fetch reports success only with a verified, named root
`fetch-datasets.sh` SHALL NOT exit zero unless it has verified that the extraction target holds the
required corpus content, on the warm-cache path as well as after a fresh extraction. On success it SHALL
print the exact `export CQLITE_DATASETS_ROOT=<absolute path>` line it guarantees; when that root differs
from the checkout default it SHALL say so and name the cause. On failure it SHALL exit non-zero with a
remedy. The script's `rm -rf` and CI-tracked-file restore behavior SHALL remain unchanged (that is
issue #2878).

#### Scenario: A warm cache still names the root it guarantees
- **GIVEN** a populated dataset root and a matching pin, so the download is skipped
- **WHEN** the script runs
- **THEN** it verifies the content, prints the exact `export CQLITE_DATASETS_ROOT=<absolute path>` line for that root, and exits zero

#### Scenario: A populated root that is not the checkout default is called out
- **GIVEN** `CQLITE_DATASETS_ROOT` already points at a machine-local root
- **WHEN** the script succeeds
- **THEN** it states that the checkout default was NOT populated and that exporting it instead would yield a corpus-less root

#### Scenario: An unusable root is never reported as success
- **GIVEN** a dataset root that does not hold the required content
- **WHEN** usability is evaluated
- **THEN** the script exits non-zero, names what is missing, and gives a remedy command

#### Scenario: The #2878-owned behavior is untouched
- **GIVEN** this change's diff to `fetch-datasets.sh`
- **WHEN** the boundary guard runs
- **THEN** the `rm -rf "${DATASET_ROOT}"` statement and the CI-only short-circuit in the tracked-file restore are present verbatim

### Requirement: The guaranteed export line is pasteable, not merely printed
The printed `export CQLITE_DATASETS_ROOT=…` line SHALL be shell-quoted, so that pasting it reproduces the
exact root even when the path contains spaces or shell metacharacters. A line that is correct only for
metacharacter-free paths does not satisfy "the exact actionable export line".

#### Scenario: A root containing spaces and metacharacters round-trips
- **GIVEN** a usable dataset root whose absolute path contains a space and a shell metacharacter
- **WHEN** the printed export line is evaluated by a shell
- **THEN** the resulting `CQLITE_DATASETS_ROOT` equals the original path exactly

### Requirement: Dataset-root usability is probeable without mutating anything
`fetch-datasets.sh` SHALL provide a mode that reports whether the resolved root is usable — and prints the
guaranteed export line — while performing **no** download, extraction, removal, re-pin **or directory
creation**. This exists so the usability guarantee's **failure** path is exercisable; a check that can only be
observed passing is not a check.

#### Scenario: The probe reports a usable root without touching it
- **GIVEN** a root holding the required content
- **WHEN** the probe runs
- **THEN** it exits zero, prints the verbatim export line for that root, and downloads/extracts/removes nothing

#### Scenario: The probe fails loudly on a hollow root
- **GIVEN** an existing but empty dataset root
- **WHEN** the probe runs
- **THEN** it exits non-zero with the missing-content diagnosis and a remedy

#### Scenario: The probe creates no directory, not even a missing parent
- **GIVEN** a dataset root whose parent directory does not exist
- **WHEN** the probe runs
- **THEN** it exits non-zero AND the parent directory has not been created

### Requirement: Every unrecognized argument to the dataset fetch is rejected fail-closed
`fetch-datasets.sh`'s default path is destructive (it removes the dataset root before extracting), so it SHALL
reject **any** unrecognized argument with a usage error and a non-zero exit, before performing any filesystem
work. An unrecognized or misspelled argument SHALL NOT fall through to the destructive path.

#### Scenario: A misspelled or extra flag never reaches the destructive path
- **GIVEN** an invocation such as `--quiet --verify-only`, `-verify-only`, or any misspelling of the probe flag
- **WHEN** the script runs against a populated dataset root
- **THEN** it exits with a usage error naming the unrecognized argument, and the dataset root's contents are untouched

#### Scenario: The recognized flags still work
- **GIVEN** `--verify-only` or `--help`
- **WHEN** the script runs
- **THEN** the flag is honored (and `--help` documents the probe and exits zero)
