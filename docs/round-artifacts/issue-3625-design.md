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

---

## What SHIPPED, and how it differs from the design above

The design was followed. Four deltas, each with its reason:

1. **`indirect:<driver>` measures a tally; an ABSENT tally is `NOT-MEASURED`, not `ZERO`.**
   The design listed `indirect:<driver>` as a kind but did not say what an unrecognised
   driver report means. It matters: for `libtest`/`compile` the subject markers are
   *cargo's own guaranteed output*, so their absence really does mean nothing ran — but a
   third-party driver's report format is not ours, and reading its absence as proof of
   vacuity would red a healthy lane the day pytest or jest changes a line. A tally that is
   PRESENT and says zero is still `ZERO`. The rule is stated in code beside the class.

2. **A fifth kind, `self:<unit>`.** `node-tests` and `shell-selftests` (the dynamic
   `--delta` entries) DELETE their log before returning, so no log-reading measurer could
   ever census them — but each already holds an exact affirmative subject count, so they
   record it directly via `_census_declare`. Without this they would have been gaps for a
   reason that is not a real limitation.

3. **`UNDECLARED` is fatal (status → `FAIL`), not a `VACUOUS`.** The design said
   fail-closed but did not name the terminal state. `VACUOUS` means *measured, and the
   subject count is zero*; an undeclared component was never measured at all, so calling it
   vacuous would be a false statement. It is a named FAIL instead.

4. **The derived `<log>.ansi-stripped` sibling is removed after the tally.** Not in the
   design, and not optional at scale: it is a full COPY of the component log, and
   `core-tests.log` runs to tens of MB — retaining one per component would silently double
   the `logs:` bundle every gate keeps.

### The census, as declared today (37 components + 3 dynamic delta names)

| kind | n | components |
|---|---|---|
| `libtest` | 18 | core-tests, tombstones-scan, scan-offload-guard, work-counters-guard, byte-budget-guard, arrow-parity-guard, memory-budget, format-compat, write-tests, cli-tests, compaction-byte-parity, bti-multiclustering, query-semantics-oracle, flight-query-semantics-oracle, flight-tests, legacy-heuristics, binding-rust-tests, kit-dashboard-drift |
| `compile` | 3 | feature-iso-parquet, feature-iso-delta-scan, minimal-build |
| `both` | 1 | integration-tests |
| `runtime:<why>` | 1 | scoped-tests (moved off `both` by the census audit — see below) |
| `indirect:<driver>` | 2 | python-bindings (pytest), node-bindings (jest) |
| `self:<unit>` | 2 | node-tests, shell-selftests |
| `gap:<reason>` | 14 | fmt, clippy, all-features-check, oom-audit, parity-report, operator-metrics-doc, smoke, file-size, roborev-lints, pub-surface, binding-unwind-profile, delivery-telemetry, tooling-tests, tree-selftest |

> **These counts are DERIVED, and the derivation is the authority, not this table**: case A2
> of `scripts/tests/test_agent_gate_census.sh` prints them from the shipped `_census_kind` on
> every run. A number written in prose decays exactly like a stale comment.

Every `libtest`/`compile`/`both` declaration was verified AT ITS CALL SITE to write its
cargo output into `$LOG_DIR/<name>.log` — directly, via `run_component`'s redirect, or (for
`binding-rust-tests`) via an unconditional `cat` of its per-package logs into `$log` before
`record_result`. A mis-declaration is the one failure mode this subsystem must not have: it
would make a legitimately green component measure `ZERO` and read `VACUOUS`.

The 14 gaps are a real, declared reduction in coverage. They print their reason on every
run and are counted separately on the aggregate `census:` line as `N DECLARED-GAP
(RECOGNISED)`; none of them is one of the components the issue's two-run table names.

### A fifth delta, found by a neighbouring test rather than by design

**The name domain is COMPONENTS + `NAMES+=("<literal>")` + `record_result "<literal>"`.** The
first draft enumerated only the first two — the same enumeration `_fm_component_class`'s guard
uses — and that is narrower than the emit path: the #2926 hidden tree-integrity self-test hook
records a verdict under the name `tree-selftest`, which is in neither static set. Undeclared, it
rendered a real self-test block's row as `FAIL`, caught by
`scripts/tests/test_agent_gate_tree_provenance.sh` J2. `tree-selftest` is now declared (a `gap:`
— it exercises the guard and has no codebase subject to count) and the completeness derivation in
`scripts/tests/test_agent_gate_census.sh` reads BOTH emit sources, with a floor of 4 derived
names so a broken derivation cannot silently shrink the domain back toward `COMPONENTS`.

---

## roborev round 1 (job 360) — two Medium findings, both fixed

### F1 — the pytest reader missed every present-and-zero spelling

The design's rule was right and the parser did not implement it. Residual 2 says an *absent*
driver tally is `NOT-MEASURED` (non-fatal) while *"a tally that is PRESENT and says zero is still
`ZERO`"* — but the reader recognised a summary only by the word `passed`, so
`61 skipped in 1.20s`, `1 xfailed in 0.10s`, `2 deselected in 0.02s` and `3 errors in 0.40s` were
all classified ABSENT. Since `NOT-MEASURED` preserves `PASS`, **a pytest run in which every test
was skipped passed the gate** — the exact vacuous-pass route this issue exists to close.

The fix inverts the order: **recognise the summary LINE first, then read the count off it.** A line
is a pytest terminal summary iff it carries a `<N> <outcome>` pair from pytest's own closed outcome
vocabulary (`passed|failed|error|errors|skipped|xfailed|xpassed|deselected`) AND a
` in <duration>s` tail; `no tests ran` is matched separately because it carries no count.
Requiring BOTH keeps it off cargo's `Finished … target(s) in 41.05s` (duration, no outcome pair),
and libtest's `test result: ok. 5 passed; … finished in 0.00s` — which satisfies both — is excluded
BY NAME, because counting it would attribute rust tests to pytest. (The old parser did exactly
that; case D24 now pins it.)

**The jest sibling was already correct, and is now pinned.** Its recogniser keys on the `Tests:`
line's PRESENCE and treats a line with no `N passed` as zero, so an all-skipped run — which jest
reports as a PASSED suite (CLAUDE.md, #3522 roborev F1) — already measured `ZERO`. That was true by
accident and asserted nowhere; case D25 pins both spellings.

RED arm (measured, in a scratch worktree differing in ONE property — the parser reverted): D23
fails naming each spelling with `…/PASS`, and D24 fails with `libtest-test-result-line-counted-as-pytest`.

### F2 — the tree-integrity boundary block bypassed the ONE renderer

`_tree_boundary_meta_lines` printed its truncated component table with
`printf '%-18s %s (%ss)\n'` directly and emitted no aggregate line, so a run that STOPPED at a
boundary produced rows carrying neither the #3453 feature matrix nor the #3625 census. That is the
one thing both designs' safety argument forbids.

Fixed: both loops route through `_fm_summary_line`, the names are collected, and
`census_summary_line` is emitted above the table (even when the table is EMPTY — `census: 0/0 …` is
a true statement about a run that got nowhere, and omitting it would make a stopped block
indistinguishable from a pre-contract one).

**The corrected emit-site count is SEVEN blocks / EIGHT `_fm_summary_line` call sites** — full,
lite, 2× delta, lite-agg selftest, emit-summary-selftest, and the boundary printer (two loops, one
block). My round-1 report said six; the count came from a grep at the time rather than from the
emit path, which is how the boundary printer stayed invisible.

**Why #3453's own uniformity guard did not catch it, which is the transferable part.** Its needle
was the literal `printf '%-18s %s (%s)'`; the boundary printer spelled the same row `(%ss)`. One
character. A near-miss in a format string was enough to hide an entire emit path from a guard whose
whole purpose is to find exactly that. The needle is now the `%-18s` NAME FIELD (comment-blind,
`^[^#]*printf '%-18s`), whose only legitimate occurrence is the renderer's own definition — so the
expected count is exactly 1 and anything above it is a bypass. **When you assert "everything goes
through ONE X", key the assert on the narrowest thing that MAKES it an X, not on a whole literal a
caller can spell differently.**

RED arm (measured, scratch worktree, boundary fix reverted and nothing else): the two new
behavioural cases in `test_agent_gate_tree_provenance.sh` fail —
`only 0 of 1 boundary row(s) are fully annotated` and `the boundary block has no aggregate census:
line` — and the other 40 cases stay green.

---

## Census audit (batch 2) — 1 HIGH, 1 MEDIUM, 3 LOWs, all fixed

### BLOCKER 1 (HIGH) — `scoped-tests` had no statically correct kind

Declared `both`, but the lane's subject depends on **what the diff routed to**. A diff confined
to `bindings/python/**` dispatches no cargo at all: `classify_scoped_plan` diverts `cqlite-py`
out of the rust set and sets `python_diff=1`, and the `cqlite-core` fallback is deliberately
guarded on `python_diff -eq 0` ("a python-only diff now legitimately leaves pkgs empty"). The
log then holds only maturin + pytest output — no `test result:`, no `N tests run:`, no
`Executable` — so `both` measured `ZERO` → `VACUOUS` → `OVERALL=FAIL` on a **correct** `--lite`
fix round and a **correct** `--delta`, which is a certifying mode. Verified from source, no gate
run needed.

Fixed with a new kind, **`runtime:<why>`**: the lane writes its own complete record before its
verdict is finalized, from the same routing variables the dispatch was made from
(`pkgs[]`, `python_diff`, `PYTHON_TIER_NOTE`). Three routes, three censuses —

| route | census |
|---|---|
| rust packages dispatched | measure `both`, as before |
| python tier only, and it RAN | the pytest tally in the same log, through the same `indirect:pytest` path (so it inherits the corrected present-and-zero rule) |
| nothing executable dispatched (tier SKIPPED, or neither) | an affirmative `NOT-APPLICABLE` naming that there was no executable subject — **not** `VACUOUS` |

The ran/did-not-run discrimination is the gate's own `python-tier: PASS`/`FAIL` convention,
factored into one predicate `_python_tier_ran` that `_delta_python_tier_gap` now also calls — one
concept, one spelling.

**The general rule this leaves behind:** before declaring a lane's subject, ask whether the lane
always HAS that subject. A kind that is right for the common route and wrong for a rarer one is a
guard that reds on correct input, which is the guard agents learn to waive.

### BLOCKER 2 (MEDIUM) — the two `self:` lanes were not coupled to AC2

`run_delta_node_tests` and `run_delta_shell_selftests` called `_census_declare` and then pushed
the **raw** `$status`, never routing through `_census_status_for`. A `ZERO` there would have
rendered `{verified NOTHING: …}` beside a `PASS`, been counted as `VACUOUS` on the aggregate line,
and left the run green. Unreachable today only because both early-return on an empty target set —
i.e. the coupling was absent and something unrelated was holding the line. Both now do
`status=$(_census_finalize <name> "$status")` followed by the closed-set `OVERALL` flip, because
these functions own their own `OVERALL` bookkeeping.

### LOW 1 — the status check ran BELOW the kind dispatch

A FAILing `fmt` rendered its gap reason and was counted under `DECLARED-GAP` instead of
`not-applicable (SKIP/FAIL)`. No verdict changed, but a miscounted census line is what stops the
next person looking. `_census_measure` now resolves the declaration first (an undeclared name is a
fact about the TABLE, not about this run), then checks status, then dispatches on kind.

### LOW 2 — the nextest arm counted tests RUN, not PASSED

`N tests run: X passed, Y failed` has `N = X + Y`, so summing `N` under a `COUNT %d tests passed`
label was a **false label** — only reachable on a PASS today, where the two are equal, which is
exactly why it would have decayed unnoticed. It now reads `X passed` off the same line.

### LOW 3 — the `-q` trap, recorded

`cargo test -q` suppresses the `Executable` status line while leaving libtest's `test result:`
intact (measured). So a `-q` lane is safe as `libtest` and can never be `compile`/`both`: adding a
`--no-run` pass to a quiet lane would silently measure `ZERO test binaries`. `kit-dashboard-drift`
is the only `-q` lane and is correctly `libtest`; the note now lives at `_census_compile_tally` and
case M5 pins the pairing.

### Deliberately NOT fixed (audit LOW 5)

The `<log>.ansi-stripped` full copy written at every `record_result`. It is deleted immediately
after the tallies, it lives inside the per-run `mktemp -d`, and its failure mode degrades to a
non-fatal `NOT-MEASURED`. The disk-pressure consequence — an `ENOSPC` inside `$LOG_DIR` now also
costs a `NOT-MEASURED` — is stated in the code comment rather than defended against, because the
alternative (parsing the coloured original) is the defect the routing exists to prevent.

---

## roborev round 2 (job 368) — three findings, all fixed

### BLOCKER 1 — quiet suppresses the compile census: #3400's second dimension

`Executable` was colour-immune and still **presentation-dependent in a second dimension**.
Measured 2026-09-01 against real cargo, both mechanisms:

| mechanism | cargo status lines (`Compiling`/`Finished`/`Running`/`Executable`) | libtest (`running N tests`, `test result:`) |
|---|---|---|
| `CARGO_TERM_QUIET=true` (env) | **all suppressed** — a `--no-run` run emits a COMPLETELY EMPTY log | unaffected |
| `[term] quiet = true` (`.cargo/config.toml`) | **all suppressed**, identically | unaffected |

Neither is visible at the call site, so a box carrying either would have made
`feature-iso-parquet` and `minimal-build` measure zero and read `VACUOUS` on **every** gate.

**Fix: three-valued, at the parse site.** `_census_compile_tally` now returns
`<Executable lines> <cargo status lines>`; a log with **no cargo status output at all** is
`NOT-MEASURED` naming quiet and its remedy, and only a log that demonstrably carries status
output *and* zero `Executable` lines is a real `ZERO`. `both` probes its two subjects
independently, so a quiet lane reports its measured tests and names the binary count as NOT
MEASURED rather than claiming `0`. **No env belt was added**: #3400 explicitly records that
moving correctness into a setting far from the parse is the worse coupling, and a belt would
have made the three-valued read look optional.

The suppression probe is anchored on the cargo **status word alone** (`$1` after the strip),
the same #3400 rule as the `Executable` anchor beside it, and errs **narrow** on purpose — an
unrecognised status word routes to the non-fatal `NOT-MEASURED`, never to the fatal `ZERO`.

*The generalisation:* **"the marker is absent" and "the marker could not have been printed"
are different facts, and a fatal state may only be derived from the first.**

### BLOCKER 2 — `UNDECLARED` was not fatal at a non-`PASS` status

`_census_status_for` returned every non-`PASS` status without inspecting the census state, so
the fail-closed state that makes *"a new component cannot join the gate with a blank census"*
true was **not fatal when the component SKIPped** — the completeness guarantee failing exactly
where it is least likely to be noticed. Two judgements now, in order: (1) is the RECORD sound?
`UNDECLARED` and any unrecognised/empty state are facts about the TABLE and are fatal at any
status; (2) only then does the run's status decide.

### LOW — the progress line lied

`record_result` can turn a `PASS` into `VACUOUS` or `FAIL`, and ~115 callers printed their own
unchanged local `$status` afterwards, so a no-op component wrote `>>> [x] PASS` to the run log
while the SUMMARY reported failure. `record_result` now publishes `RECORDED_STATUS` and every
progress line prints it. Two sites legitimately keep `$status` — `run_scoped_tests`' terminal
paths, which never reach `record_result` and reassign from `_census_finalize` themselves — and
the guard excludes them **by function, not by count**.

`run_file_size` needed a small reorder (its log-sink verdict line moved below `record_result`).
That comment block's own falsification test was re-checked and still holds; the bounded cost —
a mid-run tree-integrity exit would drop that log's last line — is named in place.

**Fallout worth recording:** the FM annotation guard stubs `record_result`, and an incomplete
stub aborted the real `run_python_bindings` under `set -u`. A stub must honour the *published
contract*, not only the part the case needs.

---

## roborev round 3 (job 371) — one Low, and the sweep it triggered

### The cited finding

The aggregate labelled every `NOT-APPLICABLE` census `(SKIP/FAIL)`, but the `runtime:` route added
in the census-audit round legitimately emits `NOT-APPLICABLE` **while preserving `PASS`** (a
python-only diff that routed to a tier which did not run). So the summary contradicted its own
component row. Ordinary: a new state reaching an old label.

### The sweep — because this was the fourth finding of one shape

| # | where | the false claim |
|---|---|---|
| 1 | job 368 | the progress line printed `PASS` while the SUMMARY said `VACUOUS` |
| 2 | census audit LOW 1 | a FAILing `gap:` component counted under `DECLARED-GAP`, not not-applicable |
| 3 | job 371 | `NOT-APPLICABLE` labelled `(SKIP/FAIL)` on a row that PASSes |
| 4 | **found by the sweep, not cited** | the `ZERO` STATE counted under the heading `VACUOUS`, a STATUS word |

**Instance 4 is reproducible in a shipping mode**, which is why it is recorded as a real defect
and not a theoretical one: `--lite-aggregate-selftest` with a seeded `VACUOUS` row emitted
`fmt: VACUOUS (0s)` beside `0 VACUOUS (RECOGNISED)` — the same contradiction, on the one counter
that names the failure this whole subsystem exists to surface.

**The root was structural.** `census_summary_line` took component NAMES and no statuses, so every
status word in it *had to be* an assumption about which statuses reach a given census state. It
now takes **name/STATUS pairs**, and the output splits in two:

- **seven STATE buckets**, one per row, summing to N, carrying **no status word** — the one that
  did (`VACUOUS`) is now `measured-ZERO`, which is what the state actually is;
- **two STATUS-DERIVED figures**: the `not-applicable` split (`did not PASS` vs
  `no-subject (PASSed)`) and the count of rows whose STATUS is `VACUOUS`.

An **odd argument count is a named refusal**, because a call site that forgot to zip its statuses
would otherwise emit a line that silently omits a row.

**The rule:** a label may name a STATUS only if it was DERIVED from the observed status. Ask of
every label — *is this word derived from the state I am rendering, or from an assumption about
which states get here?* Assumptions were all four.

### A trap this repository had already documented, and I hit anyway

The zip was first written `for _ci in "${!NAMES[@]+"${!NAMES[@]}"}"`. The `+` guard that works for
an array's **values** does **not** work for its **keys**: bash reads `${!NAME[@]+…}` as INDIRECT
expansion, errors `invalid variable name`, and **abandons the enclosing block**. Written that way,
`--emit-summary-selftest` fell straight through into a real 37-component gate. `run_delta`'s own
keys loop carries a comment describing exactly this, five hundred lines away. The correct idiom is
a count check (`[ "${#arr[@]}" -gt 0 ]`), now used at all five zips and pinned by case Q5.

---

## roborev round 4 (job 376) — two Lows, both in the test harnesses

The census and aggregate code came back clean this round; both findings were in the guards.

### Finding 1 — the feature-matrix harness passed without executing its subject

`run_scoped_tests` gained calls to `_census_scoped_record`, `_census_finalize` and
`_status_is_nonfailing`. `test_agent_gate_feature_matrix_annotation.sh` extracts the REAL
`run_scoped_tests` out of the shipped gate but extracted none of those three, so inside `py_run`
they were `command not found`, the diagnostics went to `2>&1 >/dev/null`, and — no `set -e` in
that subshell — every P-case still PASSED. **A test that passes without executing what it claims
to, with the evidence of that redirected away, is the defect class this whole issue exists to
close, sitting inside the fix's own harness.**

Fixed by EXTRACTING the whole census closure from the shipped gate (17 functions), not by
stubbing: a stub is a second implementation whose agreement with the original is only knowable by
testing it.

**The property, not the three names.** Case P6 asserts two independent halves:

- **Definedness (DERIVED, and it covers code paths this run never executes).** Every top-level
  gate function name that the shipped `run_scoped_tests` BODY mentions must resolve to a function
  inside `py_run`'s subshell — extracted, or explicitly stubbed there. It is word membership
  against the gate's own function-name set (no shell parsing), comment lines stripped, and it
  carries a floor of 8 so a broken derivation cannot report "none undefined" having examined
  nothing. Measured: 11 referenced functions today.
- **Stderr (behavioural, covers what actually ran).** `py_run` captures stderr instead of
  discarding it, and no `command not found` may appear — which also catches an unfound EXTERNAL
  command, and anything word-membership cannot see.

So a FUTURE helper added to `run_scoped_tests` and left unextracted reds this suite either way.
RED arm: the three names removed from the extraction list and nothing else → P6 fails naming all
three, P6b likewise.

### Finding 2 — a fourth status token, and three-token literals left behind

`VACUOUS` joined PASS/FAIL/SKIP, so every hard-coded three-token alternation became wrong the
moment it landed — and wrong in the direction hardest to notice, because such a pattern stops
SEEING exactly the rows that report a component verified nothing. The sweep found **three** sites,
one cited:

| site | consequence |
|---|---|
| `test_agent_gate_tree_provenance.sh` boundary `n_rows` (**cited**) | REDS ON CORRECT INPUT: a legitimate VACUOUS boundary row went uncounted while the annotation count beside it counted it, so the consistency assert failed on a healthy block |
| `test_agent_gate_summary.sh` 3453-annot-b (UNDECLARED/UNCLASSIFIED screen) | blind to VACUOUS rows — the rows most worth screening |
| `test_agent_gate_summary.sh` 3453-annot-c (RESULT:-embedding screen) | same |

Two sites were deliberately NOT changed, because they are different artifacts' vocabularies:
`test_roborev_review_guard.sh` (the roborev block's verdict grammar, which continues past those
three) and `test-data/scripts/nightly-docker-parity.sh` (its own leg vocabulary).

Case **R1** is the standing guard: no script may contain the bare three-token group. Its needle is
**split** so the guard cannot match its own source — it did on the first run, and a self-matching
grep is a guard that is always red, which is the guard nobody keeps. **R2** proves the needle
discriminates the bare three from the roborev grammar's longer one, so it cannot red a correct
artifact. RED arm: a planted three-token literal under `scripts/` → R1 fails naming the file.

---

## roborev round 5 (job 379) — one Low, and the convergence that makes the class unexpressible

### The finding, and why it was one defect half-fixed

`_census_measure` (verdict time) carried the batch-2 LOW fix, with a comment stating the rule:
*"a component that did not PASS has no PASS to affirm, and that is true whatever its kind."*
`_census_record` — the render-time fallback — **took no status at all** and dispatched on kind
alone, so it reproduced that defect verbatim: a gap-declared component that CRASHED before
`record_result` (synthetic `FAIL`, no sidecar) rendered its GAP reason and was counted as
`DECLARED-GAP`.

The two functions answer the SAME question and answered it differently for five rounds, because
they were two implementations of it. And the fix could not have landed in the second one, because
**it was not given the status** — the same structural root as job 371 one function over: *a
function required to reason about status that is not handed the status.*

### What changed

`_census_classify <component> <status> <recorded-line> <may-measure>` is now the ONE classifier;
both paths call it. The declaration/status/kind order and every state text live there once.

**The one surviving asymmetry, declared rather than assumed:** the measurer may read the component
log and write a sidecar; the renderer runs in the parent after the component's lane is gone and
must do neither. So the classifier returns `MEASURE <kind>` for the single cell that genuinely
needs the log — `PASS` × a log-measured kind — and the callers differ only there.

**A regression the convergence guard caught before it shipped.** The first version treated
`VACUOUS` as an ordinary non-`PASS`, so a vacuous row rendered
`{no census: component ended VACUOUS}` — discarding the very state that CAUSED the status, on the
line that exists to explain it. `VACUOUS` is the census's own verdict, so it now returns the
record (or says it cannot explain the status), and never `NOT-APPLICABLE`.

### The guard that pins their agreement

Case **S1** drives both paths over the same **64-cell** (kind × status × sidecar) matrix and
requires byte-identical output everywhere the classifier does not say `MEASURE`; the 8 `MEASURE`
cells must be exactly the declared asymmetry (`PASS` × a log-measured kind). **S2/S2b** assert the
cited cell by name on both the row and the aggregate, **S3** is the PASS control, **S4** is
structural (both paths delegate; the renderer is denied `may-measure`; the status reaches the
per-row annotation). Q1's table was rebuilt to choose a subject **per cell** so a state is only
tested on a kind that can reach it, and it now covers the **no-sidecar fallback** for a `gap:`, a
log-measured and an undeclared kind — the row that let this through the round-4 sweep.

RED arms, one property each: revert `_census_record` to the status-less form → S1 names the
divergence, S2/S2b reproduce the finding verbatim, S4 names the missing delegation; remove the
`VACUOUS` arm → F1/F2/G1/Q1 red.

### Two things the round's own guards caught in my work

- **P6 (added last round) fired on this round's change**: `_census_classify` went into the gate and
  not into the feature-matrix harness's extraction list, and the stderr half named it. The guard
  built for job 376 caught a job-379 omission — which is the property it was built for.
- **F2 was scanning for the word `PASS`, not the status field.** Its own RED arm exposed it: with
  the `VACUOUS` arm removed, the row reads `… no PASS to affirm`, whose PROSE contains `" PASS "`,
  so F2 fired for a reason unrelated to the status. Now it extracts the status field. Same lesson
  as Q1's status-claim check — a word scan over a line that legitimately names other statuses is a
  guard that reds on correct input.

---

## roborev round 6 (job 383) — the census measured its INPUTS, not its work

### The finding, and why it is this issue's own thesis

`node-tests` censused `n_targets` — **the number of changed files the lane selected**. #3625's
premise is *"a duration is a proxy for work; a count is the work"*, and a count of INPUTS is
simply a better proxy. It was wrong in **both directions at once**:

| situation | old census | truth |
|---|---|---|
| every selected test SKIPPED (jest exits 0) | `COUNT 2 changed jest test file(s)`, status **PASS** | nothing was verified — the vacuous run this subsystem exists to catch |
| a changed HELPER (non-`*.test.js`) | `COUNT 1 changed jest test file(s)` | jest ran the WHOLE suite — 137 tests |

Both reproduced in the RED arm, verbatim.

### The fix reuses the existing tally

`node-tests` is now **`indirect:jest`** — the same path `node-bindings` takes — so there is ONE
implementation of "what did jest report". The old `self:` rationale ("it deletes its log, so no
log-reading measurer could census it") was an **implementation choice, not a constraint**: the lane
writes to `$LOG_DIR/node-tests.log` like every other component and keeps it, which also puts its
output in the `logs:` bundle instead of discarding the evidence right after tailing 40 lines of it.
It inherits the present-and-zero rule for free — a `Tests:` line reporting zero passed is `ZERO` →
`VACUOUS`; an absent tally stays `NOT-MEASURED`. `n_targets` remains the `DELTA_EXECUTORS` figure,
which is a statement about what was DISPATCHED and is correct as one.

**Regression tests (T1–T5)** drive the REAL `run_delta_node_tests`: `_delta_node_targets` is
stubbed (it is the diff classifier, not the subject) and `node` is a PATH shim emitting a chosen
jest summary — so the two arms differ in exactly ONE property, the tally jest reports. T1
all-skipped → `VACUOUS` + `OVERALL=FAIL`; T2 positive control → `PASS` + `COUNT 41`; T3 the
helper direction → `COUNT 137`, not `1`; T4 both jest lanes share the declaration; T5 structural
(logs into `$LOG_DIR`, keeps it, no self-declared input count — comment-blind, because the body now
carries a comment explaining the removal and a bare substring test would read that explanation as
the thing it forbids).

### The `shell-selftests` ruling — its subject genuinely IS the script

Recorded at the declaration in `_census_kind`, and its premise is MEASURED (case U1) rather than
assumed:

1. **SELECTED == EXECUTED.** `_run_shell_selftest_files` invokes every file it is handed,
   unconditionally — no skip layer, no filter. That is the fact that distinguishes it from
   `node-tests`, whose count was of SELECTIONS while jest decided separately how many to run.
2. **No uniform per-script assertion tally exists to prefer.** These are arbitrary shell guards
   with differing terminal lines (`passed=N failed=M`, `N passed, M failed`, `ok - …`); deriving
   one number across them would be the curation this census refuses — the same reason
   `tooling-tests` is a declared gap.

**Declared residual:** a script that runs and asserts nothing is invisible to this count. The
census records a COUNT, not a TRUTH (the #1716/#3522 precedent); each script's own case floor is
what covers that. U1 fails if `_run_shell_selftest_files` ever grows a skip path, because premise
(1) would no longer hold and the lane would then need `node-tests`' treatment.
