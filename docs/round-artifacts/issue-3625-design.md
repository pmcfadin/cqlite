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

   > **SUPERSEDED FOR `node-tests` by roborev job 383 (round 6, below).** Both halves of that
   > rationale turned out wrong for the jest lane: deleting the log was an implementation
   > CHOICE, not a constraint, and the "exact subject count" it held was of the files it had
   > SELECTED, not of the work jest did. It is `indirect:jest` now. `shell-selftests` remains
   > `self:`, and the ruling for why is recorded at its declaration in `_census_kind`.

3. **`UNDECLARED` is fatal (status → `FAIL`), not a `VACUOUS`.** The design said
   fail-closed but did not name the terminal state. `VACUOUS` means *measured, and the
   subject count is zero*; an undeclared component was never measured at all, so calling it
   vacuous would be a false statement. It is a named FAIL instead.

4. **The derived `<log>.ansi-stripped` sibling is removed after the tally.** Not in the
   design, and not optional at scale: it is a full COPY of the component log, and
   `core-tests.log` runs to tens of MB — retaining one per component would silently double
   the `logs:` bundle every gate keeps.

### The census, as declared today (37 components + 3 dynamic delta names)

**RE-DERIVED FROM THE CODE at the end of every round that moves a declaration, and this table
was stale once already** — it still read `self: 2 | node-tests, shell-selftests` four hundred
lines above its own round-6 section saying otherwise, directly beneath the sentence warning
that a number in prose decays like a stale comment. Recorded rather than quietly corrected,
because the failure it demonstrates is the one this document is about.

| kind | n | components |
|---|---|---|
| `libtest` | 18 | core-tests, tombstones-scan, scan-offload-guard, work-counters-guard, byte-budget-guard, arrow-parity-guard, memory-budget, format-compat, write-tests, cli-tests, compaction-byte-parity, bti-multiclustering, query-semantics-oracle, flight-query-semantics-oracle, flight-tests, legacy-heuristics, binding-rust-tests, kit-dashboard-drift |
| `compile` | 3 | feature-iso-parquet, feature-iso-delta-scan, minimal-build |
| `both` | 1 | integration-tests |
| `runtime:<why>` | 1 | scoped-tests — no statically correct kind; it records what the diff ROUTED to |
| `indirect:<driver>` | 3 | python-bindings (pytest), node-bindings (jest), node-tests (jest) |
| `self:<unit>` | 1 | shell-selftests — selected == executed, and no per-script tally exists to prefer |
| `gap:<reason>` | 14 | fmt, clippy, all-features-check, oom-audit, parity-report, operator-metrics-doc, smoke, roborev-lints, binding-unwind-profile, delivery-telemetry, tooling-tests, tree-selftest |

> **These counts are DERIVED, and the derivation is the authority, not this table**: case A2
> of `scripts/tests/test_agent_gate_census.sh` prints them from the shipped `_census_kind` on
> every run, and the line above was copied from its output —
> `libtest=18 compile=3 both=1 self=1 indirect=3 runtime=1 gap=14` (the `emitted` lanes were
> REVERTED — see the final section). A number written
> in prose decays exactly like a stale comment.

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

---

## Pre-gate round — the C intent audit's findings (#3162)

The C audit returned **AC2, AC3, AC4 satisfied; AC1 partial**, measured against 400 real
historical SUMMARY blocks rather than argued from source. Three fixes, and the sequencing matters
for one of them.

### 1. The printed residual named a CLOSED issue

`_census_kind`'s gap strings said `(#3625 phase 2)` on up to 10 component rows of **every full
gate** — and #3625 was closed `NOT_PLANNED`, absorbed into the OPEN umbrella **#3162**. An
operator following that pointer lands on a dead ticket and the residual then belongs to nobody.
Re-pointed at #3162, here and in this document.

**Sequencing, not preference:** this edits `scripts/agent-gate.sh`, which is in
`GATE_GLOBAL_PATTERNS` and which `--delta` refuses, so it **cannot** be batched after the gate of
record. It had to land before it.

### 2. `emitted` shipped for the two cheapest lanes — the AC1 partial itself

The audit's measurement: **`fmt` (49 of 400 blocks) and `file-size` (45 of 400) are the two most
frequent `PASS (0s)` rows on this fleet**, and both were gaps. `fmt`'s is defensible on the oracle
(`cargo fmt --check` emits no per-file tally). `file-size`'s was not: it already walks a file set
and `wc -l`s every member.

Shipped for `file-size` and `pub-surface` only. The other six guards stay declared gaps under
#3162 — that descope stands, and a fabricated count would be worse than an honest gap.

**The two lanes are NOT the same case, and treating them alike would have reddened correct input
on the commonest diff shape there is:**

| lane | subject | is zero legitimate? |
|---|---|---|
| `file-size` | the changed `.rs` files it measured against the thresholds | **YES** — a docs- or scripts-only diff changes none. Emits `AGENT-GATE-CENSUS: NO-SUBJECT …`, which renders `NOT-APPLICABLE` and PRESERVES `PASS`. Measured, not assumed: every `--lite` round of this branch changed zero `.rs` files. |
| `pub-surface` | the unconditional crate-root `pub mod` declarations verified against their module prologues | **NO** — the guard already REFUSES a crate root with none, so its zero is real vacuity and couples to `VACUOUS` |

Guard cases V1 (the four contract states), V2 (both shipped guards really print the line, and
`file-size`'s count is derived from the set it walked), V3 (**live** — runs the real
`check-pub-surface.sh` and censuses its actual output, not a fixture of it).

**RED arm, and it corrected the guard:** deleting the `printf` from the shipped
`check-pub-surface.sh` red V3 — but NOT V2, because V2's grep matched the *comment explaining the
contract line*. The artifact describing a rule read as compliance with it, the same shape as T5.
V2 is comment-blind now, and the re-run reds both.

### 3. The kind table was stale about its own last change

See the note above the table. Every count in it is now copied from case A2's run-time output.

---

## roborev round 8 (job 389) — the count claimed a measurement that never happened

This one is different in kind from the label family. Every earlier finding was a label asserting
the wrong *state*; **this was the COUNT itself claiming verification that did not happen**, which
is the one thing a census must never do. `n_scanned` was derived by a second pass re-asking
`[ -f "$path" ]`. `[ -f ]` answers *does this path exist right now*; the census claims *I counted
this file's lines*. A selected-but-unreadable file satisfied the predicate and was reported as
MEASURED. **A count that includes files nobody counted is a duration with extra steps.**

### The fix

The count is incremented **in the original loop, immediately after a `wc -l` that produced a
validated number** — never from a predicate about the path, never from a second pass. The
existence test no longer decides the numerator: a selected file that cannot be read is counted as
*uncounted*, so it can leave neither the numerator nor the denominator silently.

**Three states, kept distinct** — and the middle one is new, because collapsing it into either
neighbour is the "could not tell → permissive" slide:

| situation | contract line | renders | status |
|---|---|---|---|
| nothing selected | `NO-SUBJECT <why>` | `NOT-APPLICABLE` | PASS (correct: a docs-only diff) |
| something selected, some/all uncountable | `NOT-MEASURED <n> of <m> …` | `NOT-MEASURED` | PASS, never read as verified |
| all counted | `<n> <unit>` | `COUNT` | PASS |

**Why `NOT-MEASURED` rather than a hard FAIL**, since the choice was offered: the ratchet ALREADY
skipped an unreadable file before this change — the arithmetic comparison simply failed on an
empty value — so that coverage hole is **pre-existing in `run_file_size`, not introduced by the
census**. Converting it to a component failure would change `file-size`'s verdict semantics as a
side effect of adding a census, with a red-on-correct-input risk on any transient FS hiccup. The
census's job is to never claim verification that did not happen, and `NOT-MEASURED` naming the
counts does exactly that. **The underlying ratchet gap is declared as a residual rather than fixed
by accident.**

### The guard, and its RED arm

`test_agent_gate_file_size_log.sh` case14 drives the REAL component over a fixture whose selected
`.rs` file is `chmod 000`, with a positive control differing in exactly one property (whether the
file can be read), and a precondition probe so a root or permissive-FS host skips per-assert
rather than passing vacuously.

**The RED arm fails for the right reason** — checked, after V2's lesson last round: with the count
reverted to the existence predicate, case14 reports
`AGENT-GATE-CENSUS: 1 changed .rs file(s) measured against the thresholds` **for a file that was
never read**, which is the finding verbatim. Two of the four asserts discriminate; the other two
(the control, and the no-collapse check) hold in both arms by design and are stated as such.

Unit-level: V1 covers all four contract states, **V1b** asserts `NO-SUBJECT` and `NOT-MEASURED`
render *distinctly* — both preserve PASS, so a status-only assert could not tell them apart — and
V2 now also fails if the increment ever sits beside an existence predicate again.

---

## Gate of record #1 — FAILED on `tooling-tests`, and what it taught

36 of 37 components PASSed; `tooling-tests` failed on
`test_pub_surface_guard.sh: line 403: AGENT: unbound variable`. **That file is not in this diff.**
The `AGENT-GATE-CENSUS:` line added to `check-pub-surface.sh` broke it.

### Mechanism

The test's *assertion* regex `MEASURED_RE` is properly line-anchored; its *extractions* were not —
`sed -E 's/.*of which ([0-9]+) unconditional.*/\1/'` over the guard's **whole, multi-line** output.
That is the SUBSTITUTE form, which passes every non-matching line through **unchanged**. The new
census line carries the word `unconditional` but no `of which`, so it survived the substitution and
`base_open` became a two-line string starting `AGENT-GATE-CENSUS: 14 unconditional …`.
`$((base_open + 1))` then read `AGENT` as a variable name, and `set -u` made it fatal.

### The fix is the extraction, not the wording

Rewording the census to dodge `unconditional` would trade a descriptive count for a word taboo, and
the next colliding word brings the bug straight back. All four extractions now go through
`ps_measured_field`, which uses `sed -n … p` (matching lines only, never pass-through), anchors on
the guard's own `^pub-surface: ` line, and **validates the result is a single integer** — so a
future reshape is a named failure at the extraction rather than a bash arithmetic error thirty
lines away.

**Case 1b pins the property** — a decoy line carrying the same keyword must not corrupt the
extraction — **with an inline RED control** running the pre-fix form over the same input and
requiring it to produce something other than the integer, so the green cannot be passing for an
unrelated reason. The whole-suite RED arm reproduces the gate failure verbatim
(`AGENT: unbound variable`).

### The sweep — two instances make a family, so the family was enumerated

Prior instance: `test_agent_gate_file_size_log.sh` case8, where a fourth `_fs_emit` moved a pinned
rejected-write count 3 → 4. Same shape: **adding output to a guard breaks a test that parses that
guard's output.** Every consumer of both guards' stdout was then checked, by breakage mode:

| mode | result |
|---|---|
| counts LINES of either guard's output | **none** |
| unanchored substitute-form sed over multi-line output | 6 sites, **all safe** — each is fed by a `grep -oE`/`grep -E`+`head -1` filter that guarantees a single matching line, except case 1b's deliberate RED control |
| exact-equality on whole output | **none** |
| CI workflows invoking either guard | **none** |
| the gate's own `run_pub_surface` | **safe by construction** — it greps the anchored `MEASURED_RE` into a single-line `$measured` FIRST, then seds that one line. Recorded so nobody "fixes" it to match the test |
| `run_file_size`'s persistence-error sibling and its landed-line-count check | **unaffected** — that block is built from the `msg` array, which the census line does not enter |

So the family is closed at one defective instance, and the difference between the broken consumer
and the safe ones is exactly **whether the parse is anchored before it substitutes**.

---

## roborev round 10 (job 396) — a failed enumeration is not an empty diff

`files=$(git diff --name-only --diff-filter=d …)` discarded its exit status. With no `set -e`, a
failed enumeration left `files` empty — **indistinguishable from "no `.rs` changed"** — so the
census emitted `NO-SUBJECT the diff changed no .rs file` and the component PASSED while
affirmatively claiming it had measured an empty diff.

This is the named **`1699-find-tristate`** shape CLAUDE.md records: *"`[ -z "$(find …)" ]`
collapses 'the scan FAILED' onto 'no match' — a three-valued signal read two-valued"*, with the
unmeasured state taking the permissive branch.

### The split as shipped — four states, checked in this order

| state | condition | contract line | status |
|---|---|---|---|
| enumeration **FAILED** | `files_rc != 0` (checked FIRST — it makes every number below meaningless) | `NOT-MEASURED the changed-.rs enumeration FAILED (git diff exited N) …` | PASS, never read as verified |
| a selected file uncountable | `n_uncounted > 0` | `NOT-MEASURED <n> of <m> …` | PASS |
| nothing selected | `n_selected == 0` | `NO-SUBJECT …` | PASS (correct: a docs-only diff) |
| all counted | otherwise | `<n> changed .rs file(s) measured …` | PASS |

`files` is also forced empty on a failed enumeration, so the ratchet cannot iterate a partial
result while the census reports the failure.

**Regression case15** fails ONLY `git diff --name-only --diff-filter=d` — uniquely this
component's enumeration — and delegates every other git call to the real binary, so a red cannot
come from a differently-broken fixture. RED arm: discard the rc again and case15 reports
`AGENT-GATE-CENSUS: NO-SUBJECT the diff changed no .rs file` for a run whose enumeration exited 7
— the finding verbatim. 3 of 4 asserts discriminate; the control holds in both arms by design.

### Why the existing lint did not catch it (for #3162, not fixed here)

The `1699-find-tristate` lint's subject is the literal `find`
(`test_agent_gate_summary.sh` matches `[ -z "$(find …)" ]`), while the SHAPE is
command-agnostic — any command substitution whose emptiness is read without its rc. Widening the
lint's subject set is #3162 follow-up work and deliberately not done on this branch.

### The fourth-instance audit, requested before it was needed

Every command substitution in the `emitted`-lane code, and how its failure is handled:

| site | on failure | verdict |
|---|---|---|
| `files=$(git diff …)` | rc captured, `files` cleared, `NOT-MEASURED` | **fixed this round** |
| `cur=$(wc -l <"$f" …)` | validated as an integer; else counted as uncounted | fixed in job 389 |
| `base=$(git merge-base …)` | rc checked by `&&`; empty base → the pre-existing declared "advisory only" path | not an instance |
| `base_n=$(git show … \| wc -l)` → `${base_n:-0}` | a failed read yields 0, which makes the file report as GROWN — the **fail-closed** direction | not an instance (conservative, and pre-existing) |
| `src=$(_ansi_stripped_log …) \|\| src=""` | `NOT-MEASURED` | already fail-closed |
| `et=$(_census_emitted_tally …) \|\| et=""` | falls to the `*)` arm → `NOT-MEASURED` | already fail-closed |
| the awk inside `_census_emitted_tally` | non-integer or empty unit → `NONE` → `NOT-MEASURED` | already fail-closed |
| `printf … "$OPEN_COUNT"` in `check-pub-surface.sh` | `OPEN_COUNT` is the guard's own validated value, and the guard refuses on zero | not an instance |

**No fourth instance found.** The one adjacent item is already declared residual 18 (a run whose
base ref is unavailable degrades to advisory while the census still counts the files it measured
— the count stays true, it just does not distinguish ratcheted from advisory).

---

## REVERTED: the `emitted` lanes (`file-size`, `pub-surface`) — back to declared gaps

### The paragraph for #3162

> **`emitted` cannot be shipped for `file-size` until the ratchet's failure semantics are
> decided.** Two lanes were added as a post-audit scope addition — `file-size` and
> `pub-surface`, chosen because each already walks a subject set and knows its count — and they
> produced **four consecutive Medium review findings**, all one family, *an unmeasured input
> taking the permissive branch*:
> 1. the count was derived from `[ -f "$path" ]`, i.e. from **existence, not measurement**, so an
>    unreadable selected file was reported as measured (job 389);
> 2. `git diff`'s **exit status was discarded**, so a failed enumeration was indistinguishable
>    from an empty diff and censused as `NO-SUBJECT` (job 396) — the named `1699-find-tristate`
>    shape, which the existing lint missed because **its subject is the literal `find` while the
>    shape is command-agnostic**;
> 3. the `NOT-MEASURED` states from (1) and (2) preserve `PASS`, so the ratchet can pass having
>    examined none or part of its subject (job 397);
> 4. and the `NO-SUBJECT` form itself exists only because the first draft would have reddened
>    **every docs- or scripts-only `--lite` round** — `file-size`'s subject is the *changed* `.rs`
>    files, and most diffs change none.
>
> Finding 3's proper remedy is to **FAIL `file-size` when a selected `.rs` file cannot be read**.
> That changes the RATCHET's failure semantics for every diff — the ratchet has *always* silently
> skipped such a file — and carries its own risk of reddening correct input (a file deleted in the
> diff, a symlink, a transient lock). It is a real decision, it needs its own measurement, and it
> is not the census's to make. **Sequence the ratchet decision first; `emitted` for this lane is
> downstream of it.** `pub-surface` is cleaner (its guard already refuses a crate root with no
> unconditional declarations, so its zero is real vacuity) and could be done alone — but it was
> only ever worth ~1 of 400 observed `PASS (0s)` rows, so on its own it does not pay for a round.
>
> Also worth carrying: **adding a line to a guard's stdout is a change to an INTERFACE.** Doing
> `emitted` broke two consumers that nobody thought of as parsers — `test_pub_surface_guard.sh`'s
> unanchored extraction (which failed a gate of record) and `test_agent_gate_file_size_log.sh`'s
> pinned emit count. Nothing mechanically warns that a guard's output has consumers.

### The boundary of the revert

**Removed:** `check-pub-surface.sh`'s contract line; `file-size`'s census emits and their
`files_rc`/`n_selected`/`n_scanned`/`n_uncounted` plumbing (`run_file_size` is now **byte-identical
to its pre-`emitted` shape**, verified by diff); the `emitted` kind, `_census_emitted_tally`, the
`NO-SUBJECT` contract form; and their guard cases (census section V, file-size case14/case15).
The machinery went with the lanes because **with nothing declaring `emitted` it would be a guard
with an empty subject set, which greens vacuously** — the shape this change exists to remove.

**Checked, not assumed — one part STAYS:** the `NOT-APPLICABLE` state and the aggregate's
`no-subject (PASSed; …)` bucket are reached by the surviving `runtime:` path —
`_census_scoped_record` writes them when a diff routes only to a python tier that did not run.
Q1's `PASS × NOT-APPLICABLE → no-subject` cell uses `scoped-tests` and still exercises it.

**Kept:** every gap reason pointing at the OPEN #3162; the design-doc count corrections;
`test_pub_surface_guard.sh`'s anchored extractions, the decoy case and its non-vacuity control
(correct whether or not a census line exists — its decoy is now synthetic, which is the right
shape: the property is about *any* second line carrying the keyword); the enumeration-derived emit
count in `test_agent_gate_file_size_log.sh` case8, **re-derived from the code** (3 on a clean tree:
thresholds, base ref, no-changed-files) rather than reverted to the old constant; and the entire
core census mechanism, untouched.

---

## roborev round 12 (job 400) — a best-effort write was driving a verdict

### The finding, and why it is an INHERITED assumption rather than an oversight

`_census_write` is deliberately non-fatal, inherited from `_fm_note`, whose comment argues it
correctly: *"a failed append must never fail the component whose matrix it describes — the
consequence of a lost append is a visibly incomplete annotation, never a wrong one."*

**That reasoning was true for the feature matrix and is false for the census.** A lost annotation
is cosmetic; a lost census record is not, because the census now drives a **verdict**. The
`self:`/`runtime:` producers computed a record, **threw the value away**, and finalized by
re-reading the sidecar — so a failed write turned a computed `ZERO` into `NOT-MEASURED`, and
`NOT-MEASURED` preserves `PASS`. A filesystem hiccup bought a false green in a merge gate.

It is CLAUDE.md's recorded shape one directory over: *a fail-closed argument for a
`${VAR:-default}` is only valid for the consumers that existed when it was written* — a new
consumer for which the permissive direction is unsafe inverts the original argument **silently**.
The sentence is now at the fix site, so the next reader of "best-effort" knows the verdict path no
longer relies on it.

### The fix

`_census_declare` and `_census_scoped_record` **return** the record they computed;
`_census_measure`/`_census_finalize` take it as an optional third argument and use it instead of
re-reading. The sidecar remains for RENDERING only on those paths.

**The sibling paths were confirmed, not assumed:** the log-measured kinds never had the problem —
`_census_measure_kind` prints the value it computed, so `record_result` already finalizes from the
value even when the write fails.

### The guard

Section W makes the write fail **the way a real one would** — the sidecar path is occupied by a
directory, so the `printf >` redirect cannot create it — rather than stubbing `_census_write`,
which would test a double instead of the shipped helper. W0 proves the sabotage bites; W1 the
producer returns its record; W2 a computed `ZERO` still becomes `VACUOUS`; **W2 RED** runs the
pre-fix call shape over the same lost write and gets `PASS`, which is the false green verbatim;
W3 a real `COUNT` still passes through the same failed-write path; W4 the same for the `runtime:`
producer; W5 structural.

### The case-floor question, answered by measurement

`CENSUS_CASE_FLOOR` was **88 while the suite reported 110**. The honest answer is that it was NOT
environment variance: the value was set by subtracting when the `emitted` section was removed —
exactly the move that hides a future shrink.

Measured: the suite has **no `skip` path, no `command -v` guard, and no corpus/node/cargo
dependency**, so the count is **environment-invariant**. Confirmed empirically by the P2 RED arm
above: with python3 unavailable the count stayed at 117 (116 + 1 failure), it did not drop.

Raised to **110** against a measured 117, with the reasoning recorded at the constant. The 7-case
margin covers a lean host I have **not** measured (bash 3.2 on macOS, which this repo supports),
and is labelled as that rather than as a known drop.

**Found while measuring:** P2 was the suite's only external-tool dependency and it was the vacuous
shape this very file polices — with python3 absent the derivation printed nothing, `$p_raw` was
empty, and P2 reported *"no progress line prints the pre-census status"* **having examined
nothing**. A case floor could never have caught it, because the case count does not change. It now
fails closed on the derivation's exit status.
