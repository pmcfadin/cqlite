# #3625 — component census design (oracle-driven)

## The measured oracle (cargo's own behaviour), taken 2026-09-01

A throwaway crate, run cold then warm:

| invocation | cold | WARM |
|---|---|---|
| `cargo test --lib --no-run` | `Compiling` + `Finished` + `Executable unittests src/lib.rs (…)` | `Finished` + **`Executable unittests src/lib.rs (…)`** |
| `cargo test` | `Running …` + `running 2 tests` + `test result: ok. 2 passed; …` | `Running …` + `running 2 tests` + **`test result: ok. 2 passed; …`** |

Two facts follow, and they are what AC4 asks for:

1. **cargo caches COMPILATION, never test EXECUTION.** A warm `cargo test` re-runs every test
   binary and re-prints `test result: ok. N passed`. So the `0s` components in the issue's
   two-run table — `tombstones-scan`, `arrow-parity-guard`, `format-compat`,
   `integration-tests`, `query-semantics-oracle`, all of them `cargo test` RUN lanes — **did
   re-verify their subjects**; the duration collapsed because the *build* was cached. The
   count was always in the log; nothing put it in the SUMMARY.
2. **A `--no-run` lane genuinely runs nothing** — `feature-iso-parquet` is
   `cargo test … --lib --no-run`, so warm it does a freshness check and exits. Its honest
   affirmative subject is not tests but **test binaries built/verified**, and cargo prints
   an `Executable …` line for each one warm as well as cold.

So the answer to AC4 is the first disjunct for the `cargo test` lanes (re-verified, now
shown) and a redefined subject for the `--no-run` lanes (binaries, not tests).

## The mechanism

Mirror the `_fm_*` (feature-matrix, #3453) subsystem, which already solved the same shape of
problem — "the SUMMARY line must state what actually executed, derived not curated".

### 1. A declared census KIND per component, closed set, guard-enforced

`_census_kind <component>` returns one of:

- `libtest`   — sum `N` over `test result: ok. N passed` in the component's log. Unit: tests.
- `compile`   — count cargo `Executable ` status lines. Unit: test binaries.
- `both`      — a lane with a `--no-run` pass AND a run pass (integration-tests): report both.
- `emitted`   — the component's own guard prints the contract line
                `AGENT-GATE-CENSUS: <n> <unit>`; the measurer reads it.
- `indirect:<driver>` — the count comes from the driver's own report (jest JSON, pytest tally).
- `gap:<reason>` — NO census is derivable; the reason is PRINTED on the SUMMARY line.

A `COMPONENTS` name that resolves to no kind is a FAIL-CLOSED refusal naming the component,
exactly as `_fm_component_class` does — a new component cannot join the gate with a blank
census.

### 2. Measurement is three-valued and happens at the ONE chokepoint

`record_result` is the single point every component's verdict passes through (it is already
where `_fm_note_if_no_cargo_observed`, `_assert_summary_integrity` and `_assert_tree_integrity`
hang). The census is measured there, from `$LOG_DIR/<name>.log`, and written to a sidecar:

- `COUNT <n> <unit>`      — affirmative measurement
- `ZERO <unit>`           — measured, and the subject count is zero
- `NOT-MEASURED <reason>` — the census could not be taken

`NOT-MEASURED` is **never rendered as verified** and never satisfies AC1; it renders as its own
token. It is DECLARED, not fatal — the same ruling CLAUDE.md records for
`cfg-gated-subtree gaps: N RECOGNISED`: a lane that reds on correct input (here, a transient
log-read failure on an otherwise green gate) is the lane agents learn to waive.

### 3. Every parse is ANSI-immune AT THE PARSE SITE (#3400)

Route through `_ansi_stripped_log`, read by REDIRECTION not a pipe. `test result:` is libtest's
and carries no escapes, but `Executable ` is a CARGO STATUS WORD and is coloured — anchor on the
status word alone, never on `<status> <payload>`. A failed strip is `NOT-MEASURED`, never a
fallback to the coloured original.

### 4. Verdict coupling (AC2) — a new terminal state `VACUOUS`

On `status=PASS` with a `ZERO` census, the component's status becomes **`VACUOUS`** — a distinct
non-passing token, in the gate's existing PASS/FAIL/SKIP vocabulary — rendered with the reason
and the component's own name.

**The aggregation must be made AFFIRMATIVE to carry it.** Today the full gate does
`[ "$_st" = FAIL ] && OVERALL=FAIL`, i.e. only the one named bad token fails and EVERY other
value — including an unrecognised one and an empty one — takes the permissive branch. That is
the exact shape CLAUDE.md forbids ("key a permissive branch on the AFFIRMATIVE value, never on
`!= <bad>`"). Replace with a closed set: `PASS` and `SKIP` are non-failing; anything else
(`VACUOUS`, an unrecognised token, an unreadable result file) is `OVERALL=FAIL`. Every mode —
full, `--lite`, `--delta` — must be covered.

### 5. Rendering

`_fm_summary_line` is the ONE renderer for all six emit sites; the census is appended there so
no mode can render a block the others do not. The `%-18s` prefix and the `(time)` shape are
PRESERVED (#3453 kept them deliberately; existing assertions match on them):

    tombstones-scan:   PASS (0s)  [test cqlite-core --features …]  {verified: 37 tests}
    feature-iso-parquet: PASS (0s)  [test …]  {verified: 1 test binary}
    <name>:            VACUOUS (0s)  […]  {verified NOTHING: <reason>}
    <name>:            PASS (3s)  […]  {census NOT-MEASURED: <reason>}
    <name>:            PASS (2s)  […]  {no census — <declared reason>}

Plus ONE aggregate line in the block:

    census: <A>/<N> components affirmed a count; <G> DECLARED-GAP (RECOGNISED);
            <U> NOT-MEASURED (RECOGNISED); <V> VACUOUS

`0 RECOGNISED`, never a bare `0` — the gap set is curated and the line must not read as a
verified all-clear.

## Non-goals / declared residual

- This does not make the census TRUE, only RECORDED and LABELED — the `workspace-test-disposition`
  precedent (#1716/#3522). A component can declare `libtest` and count a suite that asserts
  nothing; that is not this guard's subject.
- `gap:` entries are a real, declared reduction in coverage, printed on every run.
