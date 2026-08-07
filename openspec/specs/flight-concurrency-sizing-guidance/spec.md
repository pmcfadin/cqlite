# flight-concurrency-sizing-guidance Specification

## Purpose
TBD - created by archiving change concurrency-admission-defaults. Update Purpose after archive.
## Requirements
### Requirement: The peak-concurrency-by-width curve is re-measured with the shipped default inside the ramp

A concurrency sweep SHALL be run that reports, for each server width in scope, the aggregate
throughput at each offered concurrency `N`, and identifies the throughput-optimal `N`. The ramp SHALL
include the shipped default ceiling (64) and at least one point between the previous ramp top (16)
and it, so the status quo is a measured point rather than an extrapolation. Widths SHALL include at
least one width the derivation formula was **not** fitted to. Each point SHALL be the median of at
least 3 repetitions with per-N minimum, median and maximum published as dispersion. Any point failing
the client-headroom validity gate SHALL be excluded **and reported as excluded**, never silently
dropped.

Where a reported peak coincides with the maximum `N` in the ramp, the report SHALL label it a
**censored** observation ("≥ N, unbounded above") rather than a peak.

#### Scenario: the sweep covers the required widths and ramp

- **GIVEN** a measurement box of the #3217 class with SMT on, both siblings of each core pinned
  together, and a client fixed at 2 exclusive physical cores
- **WHEN** the sweep is run at server widths `S ∈ {1, 2, 3, 4, 6}` physical cores over the ramp
  `N ∈ {1, 2, 4, 8, 16, 24, 32, 64}` with ≥3 repetitions per point
- **THEN** a per-(S, N) table of median throughput with min/max dispersion is published, and the
  throughput-optimal `N` at each width is named — with `S = 3` present as a width the formula was not
  fitted to

#### Scenario: a peak at the top of the ramp is labelled censored

- **GIVEN** a width whose highest measured throughput occurs at the largest `N` in the ramp
- **WHEN** the peak for that width is reported
- **THEN** it is stated as censored (a lower bound on the true peak), not as the peak, and the
  server-utilisation figure at that point is published so the reader can see whether the curve had
  saturated

#### Scenario: the reproduction is comparable to the published #3217 table

- **GIVEN** the widths `S ∈ {1, 2, 4, 6}` that #3217 also measured
- **WHEN** the new medians at `N ∈ {1, 2, 4, 8, 16}` are compared to report §3.1
- **THEN** the step length, repetition count, warm pre-pass, merge path and client core set match
  #3217's, and any width whose peak location differs from the published one is called out explicitly
  rather than averaged away

#### Scenario: the derivation formula is judged against the new curve before the default changes

- **GIVEN** the completed sweep
- **WHEN** the formula `clamp(2 × P, 2, 64)` is evaluated at each measured width and compared to that
  width's measured optimum
- **THEN** the deviation at each width is published as a percentage of the measured peak, and a width
  at which the formula is worse than the current constant blocks the default change until the
  coefficient is re-fitted — the measurement is a gate on the formula, not a confirmation of it

### Requirement: Over-admission cost is reported in throughput and in per-scan latency at every width

For each width, the report SHALL state the cost of running at the shipped default rather than at that
width's measured optimum, in **both** currencies: aggregate throughput loss as a percentage, and
per-scan p50 latency as a multiple. Latency is reported because it degrades far harder than
throughput — #3217 measured per-scan p50 rising from 30,966 ms at N=2 to 301,728 ms at N=16 on one
core, a ~10× cost accompanying a 16.4% throughput *loss* — and a report that published throughput
alone would understate the harm by an order of magnitude.

#### Scenario: both currencies are reported per width

- **GIVEN** the completed sweep
- **WHEN** the over-admission cost table is produced
- **THEN** each width has a row giving its optimal `N`, the throughput at that `N`, the throughput at
  the shipped default, the percentage loss, the per-scan p50 at both points, and the latency multiple

#### Scenario: admission health is reported alongside, so the finding stays a defaults finding

- **GIVEN** every measured point
- **WHEN** the admission rejection counter is read
- **THEN** the report states the total across all points, reproducing (or contradicting) #3217's
  finding of zero rejections at all 83 points — a non-zero count would mean the sweep is measuring
  shedding rather than sizing, and must be investigated before the curve is used

### Requirement: No throughput regression at the widest configuration in scope

At the widest configuration in scope, the throughput at the **derived** default SHALL be measured
against the throughput at the **current** default, and SHALL not be materially worse. "Materially"
is judged against the point's own published dispersion, not an invented threshold. If the derived
default is measurably worse at the widest width, the change SHALL NOT ship as designed.

The widest configuration in scope SHALL be declared explicitly in the report, together with why it is
the widest — not left for a reader to infer from the largest number in a table.

#### Scenario: the widest width is measured at both defaults

- **GIVEN** the widest width in scope (6 physical cores / 12 hardware threads)
- **WHEN** throughput is measured at `N` = the derived default for that width and at `N = 64`, each
  as a median of ≥3 repetitions
- **THEN** both medians and both dispersions are published, and the comparison is stated as a
  percentage with its dispersion beside it

#### Scenario: the scope of "widest" is declared with its reason

- **GIVEN** the report
- **WHEN** the widest configuration is stated
- **THEN** the reason 8 physical cores is out of scope on this rig is recorded — the client requires 2
  exclusive physical cores on the same box and the sweep driver refuses an overlapping server/client
  set — so the limit is visibly a rig constraint, not an unstated choice

### Requirement: Operator documentation states the measured relationship and how to override it

Operator-facing documentation SHALL state the measured relationship between server width and
throughput-optimal concurrency, the cost of over-admission in both currencies, the derived-default
formula and its ceiling, the provenance field in the startup log, and the one-setting override. This
requirement holds **regardless of the AC2 decision**: if the derived default were not adopted, the
documentation would still have to carry the numbers.

The documentation SHALL also record the two known residuals rather than presenting the formula as
fully validated: the −4.8% deviation at the narrowest width, and the absence of any non-SMT
measurement (on a non-SMT host the formula yields half the fitted per-physical-core value).

#### Scenario: a deployer sizing a narrow worker finds the number, not a folk rule

- **GIVEN** the `cqlite-flight` operator documentation
- **WHEN** a reader looks up how to size `--max-concurrent-scans`
- **THEN** they find the measured peak-N-by-width table, the 1-core over-admission cost stated as
  both a throughput percentage and a latency multiple, and the formula the default now uses

#### Scenario: the override recipe is present and complete

- **GIVEN** the same documentation
- **WHEN** a reader needs the pre-#3225 behaviour, or a value of their own
- **THEN** both the flag and the environment variable are named, the precedence over the derived
  default is stated, and the exact setting that restores the previous constant (64) is given

#### Scenario: the known residuals are documented, not hidden

- **GIVEN** the same documentation
- **WHEN** a reader on a non-SMT host, or on a single-core container, consults it
- **THEN** the unvalidated non-SMT extrapolation and the narrowest-width deviation are both stated
  with their magnitudes, beside the override recipe

#### Scenario: the doctrine surfaces are updated in the same change

- **GIVEN** a user-facing behaviour change to a shipped default
- **WHEN** the change is prepared
- **THEN** `CHANGELOG.md` records it as a behaviour change (not a fix), and any published page that
  states the old constant is corrected in the same change

### Requirement: Every reported throughput figure names its byte basis and its fixture geometry

Every throughput figure in the report SHALL name the byte basis it is denominated in — logical /
uncompressed, on-disk compressed, and Arrow buffer capacity are three different numbers and SHALL NOT
be conflated, and Arrow buffer capacity SHALL NOT be described as wire bytes. The fixture used for
the reproduction SHALL have its geometry recorded: row count, bytes per row on each basis, SSTable
format and generation, partition count, and the `sha256` of each `Data.db`.

Because #3217's corpus binaries are absent from the measurement box and are gitignored, the recorded
geometry SHALL be **measured from the corpus actually used**, and its sha SHALL be the new one. Where
the regenerated corpus is compared to #3217's published geometry, the comparison SHALL be shown, not
asserted; a material divergence SHALL be labelled so the cross-round comparison is not read as
like-for-like.

#### Scenario: the headline figure carries all three bases

- **GIVEN** the report's headline throughput figure
- **WHEN** it is stated
- **THEN** it is accompanied by its logical/uncompressed MB/s, its on-disk compressed MB/s and its
  Arrow buffer *capacity* rate, each labelled, with the per-row byte constants that convert between
  them

#### Scenario: the fixture geometry is recorded from the corpus actually measured

- **GIVEN** the corpus staged for this round
- **WHEN** its geometry is recorded
- **THEN** row count, partition count, rows per partition, SSTable format/generation, uncompressed
  and compressed bytes per row, compression ratio and `sha256(Data.db)` are all present and derived
  from that corpus — with an independent row-count oracle agreeing, as #3217 did

#### Scenario: a regenerated corpus is compared rather than assumed equivalent

- **GIVEN** a corpus regenerated to #3217's recipe
- **WHEN** its geometry is compared to `ws0-3217-artifacts/corpus/corpus-geometry.txt`
- **THEN** the comparison is published field by field, the differing `sha256` is stated plainly, and
  any material divergence in rows, bytes-per-row or SSTable count is called out before the new curve
  is compared to the published one

### Requirement: The change records what it does not change

The change SHALL state, in its artifacts and in its report, the boundaries the issue drew: no change
to the admission mechanism, no read-path or encode-path performance work, no change to `--batch-size`
or channel capacities, and no attempt on the single-stream-slows-with-more-cores phenomenon. It SHALL
also record that the measured evidence is a **two-point exact fit with two censored corroborations**,
so no later reader infers a four-point validation from the peak table.

#### Scenario: the scope statement is present and consistent

- **GIVEN** the proposal, the design, the spec deltas and the committed report
- **WHEN** the out-of-scope list is compared across them
- **THEN** all four carry the same boundaries, and none of them contains a change to the admission
  mechanism, the batch-size knob, a channel capacity, or the read/encode path

#### Scenario: the strength of the evidence is stated where the table is

- **GIVEN** any artifact that presents the peak-N-by-width table
- **WHEN** it presents the formula's fit
- **THEN** the exact hits, the deviation, and the censored and unmeasured entries are distinguished
  from one another — no artifact presents four measured confirmations

