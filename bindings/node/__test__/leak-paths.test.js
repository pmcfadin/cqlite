/**
 * Exception-path and abandoned-iterator LEAK BUDGET tests (issue #1465, parent #1436).
 *
 * Error paths are where leaks hide. When a query rejects, or a streaming
 * iterator is abandoned partway through, buffers / channel state / JS objects may
 * never get freed -- and no test noticed steady growth across repeated failures.
 * A long-running Node server hitting errors in a loop would slowly bloat. This
 * file puts budgets on exactly those paths. It is the SIBLING of
 * bindings/python/tests/test_leak_paths.py and shares its SHAPE -- warm up, bound
 * the growth, never assert zero, count every iteration, abandon with work still
 * outstanding -- but NOT its instruments or its constants, and the two files list
 * their deliberate asymmetries rather than claiming parity they do not have.
 *
 * WHAT EACH INSTRUMENT CAN AND CANNOT SEE (issue #1465 review, stated up front
 * because the earlier version of this header overclaimed):
 *   * `heapUsed + external` covers V8-managed memory plus off-heap allocations
 *     V8 has been TOLD about (`Buffer`s, ArrayBuffers, napi external memory
 *     adjustments). That is every JS object the binding hands back -- rows, cells,
 *     error objects, iterator state -- but it is BLIND to an ordinary Rust-side
 *     allocation. A leaked `Vec<u8>`, a retained streaming channel or an
 *     un-dropped reader buffer stays completely FLAT in both numbers while
 *     process memory climbs. So the primary budget bounds the JS-VISIBLE half of
 *     these paths: a real, previously unguarded half, not the whole leak surface.
 *   * `process.memoryUsage().rss` DOES see native allocations, because it is the
 *     OS's resident-set figure for the whole process. It is coarse and jittery
 *     (V8 heap growth, allocator arenas, the addon's Tokio threads), so the
 *     secondary budget on it is deliberately LOOSE: gross native retention only.
 *
 *   Consequence, recorded honestly: a SMALL per-iteration native leak (below the
 *   RSS budget's ~37 KiB/iteration resolution) is invisible to BOTH instruments
 *   here. And note what that resolution is and is not: it is ARITHMETIC
 *   (budget / iterations), NOT a measured floor -- planting a genuine
 *   native-allocator retention needs an addon, so unlike the Python lane (whose
 *   floor is bracketed by a `libc.malloc` control at 16-24 KiB/iteration) this
 *   file's RSS backstop has no native-side RED control. The proper oracle for that class -- an isolated process, RSS measured
 *   against a calibrated NATIVE retention control, or native live-resource
 *   counters -- is issue #3585 and is deliberately not built here.
 *
 * WHY THE GUARD IS A MEASURED BUDGET, NOT jest `--detectLeaks` (issue #1465
 * step 3 authorises this fallback and requires the reason to be documented here):
 * jest-leak-detector watches the jest `TestEnvironment` INSTANCE -- `runTestInternal`
 * in jest-runner's runTest does `new LeakDetector(environment)` when
 * `projectConfig.detectLeaks` is set -- so it
 * answers "was the whole environment collected after this FILE finished", never
 * "does each iteration of this loop retain memory". It is blind to the property
 * under test and can red for unrelated environment retention, so it is NOT
 * enabled anywhere for this file — see the jest.config.js header for the full
 * ruling.
 *
 * AND WHY `--detectOpenHandles` IS NOT ON ANY LANE (issue #1465 rounds 2-3 — it
 * WAS wired on this lane's invocation for one round, then removed; do not re-add
 * it to a lane without answering these four measurements):
 *   1. It has NO ENFORCEMENT. It prints a report after the run and exits 0. In
 *      its only merge-gating execution (the gate's `node-bindings` component) that
 *      report goes to $LOG_DIR/node-bindings.log, which agent doctrine says never
 *      to read — so the report would have no reader at all.
 *   2. Its baseline here is NOT zero: this lane always reported exactly one
 *      handle, `CustomGC`, attributed to `require`-ing the napi addon (napi-rs's
 *      process-global GC integration, present for ANY file that loads the
 *      module). So the "signal" was a human noticing a 2 where a 1 is normal.
 *   3. It carries a HANG hazard, observed: the flag disables jest's force-exit, so
 *      with a handle outstanding jest WAITS on it rather than failing — a planted
 *      uncleared timer turned the run into a 10-minute timeout kill instead of a
 *      red. In a mandatory gate component that is a hung gate, not a failure.
 *   4. The enforceable in-process alternative is VACUOUS on these paths:
 *      `process.getActiveResourcesInfo()` reports `[]` before AND after 300
 *      abandoned iterators and 300 rejections, and `[]` right after loading the
 *      addon, while correctly reporting `["Timeout"]` for a planted
 *      `setInterval`. It is live in general and blind to exactly the napi/Tokio
 *      handle class this path could leak, so asserting on it would measure
 *      nothing while looking like a guard.
 * It stays available to a HUMAN chasing a suspected handle leak, as its own script
 * so no lane can pick it up by accident:
 *   npm run test:leaks:handles     # expect the 1 CustomGC baseline, nothing more
 * That is a debugging recipe, deliberately not part of any lane.
 *
 * WHAT IS ASSERTED (and what is deliberately NOT): the growth of
 * `heapUsed + external` across N iterations must stay under a documented budget.
 * Growth is NEVER asserted to be zero -- V8/GC noise, one-time caches and
 * allocator behaviour make a zero assertion flaky by construction. `external` is
 * summed in ALONGSIDE `heapUsed` because a leaked `Buffer`/native-backed
 * allocation lives OFF the V8 heap and is barely visible to `heapUsed` alone.
 * Measured exactly (min of 9 passes x 300 iterations): a retained 256-byte
 * `Buffer` per iteration moves `external` by 256.0 bytes/iteration -- the bytes
 * themselves -- and `heapUsed` by only 15.7, the JS wrapper object. Summing both
 * is what lets an off-heap retention register at all.
 *
 * NON-VACUITY IS ASSERTED EXPLICITLY (the most likely defect in a budget test):
 * a loop body that silently no-ops -- a "bad" CQL string that resolves instead
 * of rejecting, or a streaming query that yields 0 rows -- would make the budget
 * trivially pass while testing nothing. So every iteration is counted and the
 * counts are asserted, and two separate contract tests pin the shapes: the bad
 * query really rejects, and the broken-out-of stream really was mid-stream and
 * really was closed by `return()`.
 *
 * HOW TO RUN (this lane needs `global.gc`, exactly like conversion-budget.test.js):
 *   cd bindings/node && npm run build && npm run test:leaks
 * A bare `npx jest leak-paths` has no `--expose-gc` and FAILS LOUDLY in
 * `beforeAll` rather than silently measuring GC-deferred garbage.
 *
 * WHO EXECUTES THIS FILE (recomposed onto #3522, round 9): the gate's `node-bindings`
 * component runs the WHOLE jest suite via `npm test` — which includes this file, once,
 * measured — and then AFFIRMS the two budget tests BY NAME from that same run's
 * `--json` report. There is exactly ONE executor; `npm run test:leaks` (and
 * `test:leaks:handles`) remain the human/debug entry points that no lane invokes.
 *
 * ADDING A BUDGET TEST? TWO THINGS ARE REQUIRED, and neither is optional (issue
 * #1465 round 7). That affirmation is by NAME, so a budget test must:
 *   1. carry the title suffix `stay under the leak budget` — that suffix is how the
 *      gate ENUMERATES budget tests, so a differently-titled one is invisible to
 *      both the runtime unexpected-extra arm and the self-test's count check; and
 *   2. be enrolled in `_NODE_LEAK_BUDGET_TESTS` in scripts/agent-gate.sh.
 * Do (1) without (2) and the gate FAILs loudly ("UNEXPECTED budget test … must be
 * enrolled"), which is the intended outcome. Do (2) without (1) and it FAILs too
 * (the name will never be found as passed). Do NEITHER and the new budget test still
 * EXECUTES (the whole suite runs it) but is not AFFIRMED — nothing then notices if a
 * later change skips or renames it, which is the reason this paragraph exists.
 *
 * There is deliberately NO wall-clock/elapsed-time assertion in this file: these
 * are MEMORY budgets. A timing threshold in a correctness test is a known flake
 * class (#2642).
 */
const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

const DIR = global.testPaths.SSTABLES_DIR;
const SCHEMA = global.testPaths.SCHEMA_WIDE_ROWS;

// Rejected at query-planning time -> QueryError. Chosen deliberately over a
// nonexistent-table SELECT, which resolves with 0 rows WITHOUT rejecting
// (measured 2026-08-30) and would make the error-path loop a silent no-op.
const BAD_CQL = 'THIS IS NOT VALID CQL';
// The binding's authoritative identity for that rejection (error-wrapper maps the
// core error category to `code`; see __test__/error.test.js): Query -> 'QUERY'.
const EXPECTED_ERROR_CODE = 'QUERY';

// Widest fixture in the corpus: 101 declared columns (id + col_001..col_100, per
// test-data/schemas/wide-rows.cql) and 50 rows on disk (both counted, not
// estimated), the same table the conversion-budget ratchet uses. A wide row means an abandoned stream has
// really built and dropped a non-trivial per-row value graph, so a leak of that
// graph is visible rather than lost in noise.
const STREAM_QUERY = 'SELECT * FROM test_wide_rows.many_columns_table';

// THE ABANDONMENT MUST LEAVE NATIVE ROWS OUTSTANDING (issue #1465 round 4, roborev).
// `bufferSize` is BOTH the native channel capacity and the per-`next()` batch size
// (see src/streaming.rs / StreamingConfig in src/database.rs), and it defaults to
// 1024 -- larger than this fixture's 50 rows. Measured with the default: the native
// `rowsReceived` counter reads 50 after the FIRST yielded row, i.e. one `next()` had
// already drained the ENTIRE native stream, so "abandoning" at row 5 abandoned only
// JS-side buffered rows and nothing native was in flight. Two consequences, both
// bad: the post-break re-iteration would have yielded zero even if `return()` never
// closed anything, and the budget below measured no native cancellation at all --
// which is the very thing this issue is about.
//
// With `bufferSize: 2` (<= STREAM_ROWS, so at least one refill happens mid-loop) the
// same trace reads [2, 2, 4, 4, 6]: three native refills during the five yielded
// rows, and 44 of the fixture's 50 rows STILL OUTSTANDING natively at the break.
// The contract test below asserts that property rather than trusting this comment.
const STREAM_CONFIG = { bufferSize: 2 };

const ITERATIONS = 300;
// Warm-up iterations run BEFORE any sample so one-time allocations (V8 code
// caches, first-touch native buffers, the streaming machinery's one-time setup)
// are not counted as growth.
const WARMUP = 20;
// Rows pulled before breaking. Must be < the fixture's row count (50 on disk) so
// the iterator is genuinely abandoned mid-stream. The contract test below pins the
// property that actually matters -- that the fixture yields MORE than this -- and
// does not hard-code 50.
const STREAM_ROWS = 5;
// `heapUsed`/`external` deltas are far jitterier than Python's tracemalloc:
// individual passes over the SAME clean loop swung from -133 KB to +349 KB (V8
// growing its heap, or collecting an earlier pass's garbage inside a later one).
// So each budget is measured over several passes and asserted on the MINIMUM of
// the NON-NEGATIVE passes -- not the median, not a single sample, and never a
// negative one.
//
// WHY THE MINIMUM (measured, not aesthetic): a genuine per-iteration leak raises
// EVERY pass -- for all three synthetic leak shapes below, all 9 passes were
// elevated and the minimum non-negative pass still sat 1.5x-5.5x above the budget
// -- whereas GC jitter perturbs individual passes in both directions. The minimum
// needs only ONE quiet pass out of MEASURE_PASSES to read fairly, while a median
// needs a majority: a median-based budget was tried first and flaked (1 red in 10
// runs; medians 616..3200 bytes, with occasional 150-296 KB passes).
//
// WHY NEGATIVE PASSES ARE EXCLUDED, quantified: a negative delta means the pass
// FREED more than it allocated -- deferred garbage from an earlier pass being
// collected inside this one -- so it is unaccounted bookkeeping, not a
// measurement of this pass. Including them was a real loss of sensitivity, not a
// theoretical one: clean minima reached -133,504 bytes, and a run containing such
// a pass moved effective sensitivity from ~109 to ~550 bytes/iteration, enough
// that the smallest shape this test claims to catch (a 64-byte Buffer per
// iteration) would read at or below zero and PASS.
//
// ALL passes negative is a HARD ERROR, never a pass: that state means the
// instrument measured nothing about this loop, and a positive verdict requires an
// affirmative measurement (CLAUDE.md).
//
// AND THE MINIMUM ALONE IS NOT ENOUGH (issue #1465 round 5, roborev H1): the
// minimum is the most FAVOURABLE sample, so one slightly-positive pass would
// excuse eight that blew the budget. That hole is closed by a SECOND assertion on
// the UPPER MEDIAN, against a PER-PATH ceiling (STREAM_MEDIAN_CEILING_BYTES /
// ERROR_MEDIAN_GROSS_CEILING_BYTES), which a majority of passes must satisfy.
//
// WHY NOT THE STRICTEST STATISTIC (max-of-nonneg), which would close the hole
// completely: on the error path a CLEAN run reaches ~428 KB in some pass -- 13x the
// minimum budget -- so a max-based assertion reddened 6 of 10 clean runs, and
// raising the ceiling past that noise would put it ABOVE the RED-control signal,
// destroying discrimination to buy strictness. Measured, not assumed.
//
// RESIDUAL, stated so nobody has to rediscover it: with a median ceiling, up to
// 4 of 9 passes can still exceed the budget while the test passes. That is a
// strictly smaller hole than "8 of 9", and the min assertion still fires on the
// quiet-pass end.
//
// SUPERSEDED MEASUREMENTS (round 5, kept only to show the direction of travel --
// do NOT read these as the current justification; the live numbers are the round-7
// table beside the ceilings, which uses the UPPER median and reports valid-pass
// counts):
//   error path   min 16 (x9), 9,328 (x1) | median 5,312..22,196 | MAX 11,088..428,064
//   stream path  min 56 (x10)            | median 56 (x9), 852  | MAX 56..6,136
// Those medians are the CLASSIC (averaging) median over a statistic that no longer
// exists here; round 7 re-measured the upper median and found the error path
// reaching 137,416, which is why its ceiling is now separate and declared weak.
const MEASURE_PASSES = 9;

// ---------------------------------------------------------------------------
// BUDGET (issue #1465) -- MEASURED, never guessed. Linux x64, Node v20.20.2,
// release-unwind .node, CQLITE_DATASETS_ROOT=/data/datasets, 300 iterations x 9
// passes; the numbers below are the MINIMUM pass (the asserted statistic), over
// several consecutive runs (2026-08-30):
//   error path:   +16 .. +544 bytes over 10 runs (RE-MEASURED in round 9 after the
//                rebase onto #3522/#1464/#1461 and a fresh napi build; a negative
//                pass would mean a later pass collected an earlier pass's deferred
//                garbage, and such passes are EXCLUDED before the statistic)
//   stream path:  +16 .. +56 bytes over 10 runs
// Budget = 32 KiB (109 bytes/iteration at 300 iterations) -- UNCHANGED by the
// re-measure, so this is not a loosening: against the POST-REBASE clean minima it is
// ~60x the largest on the error path (544 bytes) and ~585x on the stream path (56
// bytes), and GC/platform drift cannot red it. (Round 7 quoted ~13x for the error path
// against a 2,496-byte maximum that the re-measure did not reproduce.)
// Measured discrimination, with synthetic leaks injected into these same loop bodies
// at `bufferSize: 2` (again as the minimum non-negative pass):
//   * retain a 256-byte Buffer per iteration: minimum 50,312 B (167.7/iter) TRIPS the
//     minimum ceiling (1.5x), and the stream median ceiling at 133,728 B (445.8/iter,
//     2.0x) -- RE-MEASURED post-rebase; round 7 measured 132,184 B (440.6/iter).
//   * retain a 64-byte Buffer per iteration: stream upper median 84,784 B (282.6/iter)
//     -> TRIPS (1.3x). Still the smallest shape this file CLAIMS to catch.
//   * retain ONE wide row per iteration: 182,400 B (608.0/iter) -> TRIPS (5.6x).
// Those are the realistic shapes of a JS-side or native-buffer leak on these
// paths; the 64-byte case is the smallest leak this test is claimed to catch.
// RED CONTROL, re-run again in round 7 against the QUORUM + UPPER-MEDIAN statistic
// (2026-08-30): planting the 256-byte retention INTO THESE EXACT TEST BODIES still
// fails the committed assertions on both paths -- error path MINIMUM 117,744 bytes
// (392.5/iteration, 3.6x over this budget), stream path UPPER MEDIAN 134,032 bytes
// (446.8/iteration, 2.0x over the stream median ceiling) -- so the guard is known to
// bite the code it ships with, not just a lookalike in a scratch harness. The
// smallest shape this file CLAIMS to catch was re-verified at the same time: a
// retained 64-byte Buffer per iteration reds the stream path at an upper median of
// 84,784 bytes (282.6/iteration, 1.3x over; round 7 measured 83,616 / 278.7 on the
// same plant). Neither number transferred from an
// earlier round; both were re-measured after the statistic changed.
// WHAT THAT CONTROL ESTABLISHES, precisely: the planted objects are JS-visible
// (`Buffer`, plain objects), so it proves the instrument is sensitive to
// JS-VISIBLE retention on these paths. It establishes NOTHING about sensitivity
// to a native (Rust-allocator) leak, which `heapUsed + external` cannot see at
// all -- that is the RSS backstop's job below, and properly, issue #3585's.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BUDGET RELAXATION: OPT-IN, EXPLICIT, DECLARED — and never inferred from the
// environment (issue #1465 round 12, roborev V1).
//
// THE DEFECT THIS REPLACES, because it was live and not hypothetical: the multiplier
// used to be `process.env.CI ? 2 : 1`. That keys on ANY nonempty value, so `CI=false`
// and `CI=0` doubled every ceiling; and `.github/workflows/gate.yml` runs the FULL
// `scripts/agent-gate.sh` nightly on `main` inside GitHub Actions, where the runner
// sets `CI=true` unconditionally and the gate never cleared it. The authoritative
// backstop was therefore running all four ceilings at 2x while presenting itself as a
// strict run — a relaxed verdict indistinguishable from a strict one. The prose even
// claimed "the gate has CI unset", which was an assumption about the environment
// rather than an enforced property. This is the issue's own rule applied to
// configuration: never infer a policy decision from an ambient signal that means
// something else.
//
// WHY THE RELAXATION STILL EXISTS AT ALL (option (b), on the evidence): its only
// beneficiaries are node-ci.yml's three `test`-job legs (ubuntu-latest, macos-14,
// windows-latest), which are a REGISTERED EXEMPTION in .github/ci-gating-tiers.yml —
// a red there is triage noise, not a merge block. Deleting the knob would also change
// those legs from relaxed to strict in the same commit that fixes the gate leak, on
// hardware whose GC/allocator jitter this file has NEVER measured (every number here
// comes from one Linux fleet box). That is two unmeasured changes at once, and the
// downside is the one CLAUDE.md warns about: a flaky red on an exempt lane teaches
// people to ignore the lane. So the knob stays, the legs set it EXPLICITLY, and if a
// leg is later measured strict-clean, deleting it is a one-line follow-up WITH data.
//
// STRICT PARSE: exactly one allowed value, and that value names what it does. `1`,
// `true`, `false`, `0`, `""` and anything else leave the budgets STRICT, so no
// generic truthy marker can ever relax them again.
const LEAK_BUDGET_RELAX_TOKEN = '2x-ceilings';

/**
 * PURE. Decide the relaxation from an environment object (exported for direct tests —
 * the previous version was an inline `process.env` read with no test at all).
 */
function resolveBudgetRelaxation(env) {
  const relaxed = env.CQLITE_LEAK_BUDGET_RELAX === LEAK_BUDGET_RELAX_TOKEN;
  return { relaxed, multiplier: relaxed ? 2 : 1 };
}

const { relaxed: BUDGETS_RELAXED, multiplier: BUDGET_RELAX_MULTIPLIER } =
  resolveBudgetRelaxation(process.env);

// A RELAXED RUN DECLARES ITSELF, wherever a human reads the verdict: a doubled ceiling
// that looks identical to a strict one is the same defect class as a skipped lane that
// looks like a passing one (CLAUDE.md: a narrowed lane declares its own narrowing).
if (BUDGETS_RELAXED) {
  console.log(
    `[#1465] LEAK BUDGETS RELAXED 2x: CQLITE_LEAK_BUDGET_RELAX=${LEAK_BUDGET_RELAX_TOKEN} ` +
      'is set, so every ceiling in this file is DOUBLED. This run does NOT certify the ' +
      'strict budgets. The merge-gating agent-gate node-bindings component strips this ' +
      'variable, so it can never relax the gate of record.'
  );
}

// SCOPE OF THE MULTIPLIER, stated because it is asymmetric: it doubles the CEILINGS
// only. SAMPLE_QUORUM and MAX_MEASURE_ATTEMPTS are NOT scaled, so a relaxed leg faces
// the same statistic-formability requirements on possibly noisier hardware — i.e. a
// below-quorum hard error is somewhat likelier there. Deliberate and low-stakes for the
// reason above. If those legs ever become merge-gating, scale the quorum machinery too
// rather than widening the ceilings.
const BUDGET_BYTES = 32 * 1024 * BUDGET_RELAX_MULTIPLIER;

// SECONDARY, LOOSE, NATIVE-VISIBLE BUDGET (issue #1465 review): total RSS growth
// across the whole measured window (all MEASURE_PASSES x ITERATIONS iterations).
// MEASURED on this machine, 2,700 iterations:
//   error path:  -8,192 .. +6,311,936 bytes (3 consecutive repetitions)
//   stream path: -385,024 .. +21,164,032 bytes (4 consecutive repetitions at
//                `bufferSize: 2`; the largest sample is the first run of a fresh
//                process, the other three were 2.44-3.77 MiB)
// Budget = 96 MiB -- UNCHANGED, i.e. ~4.5x the largest observed value, because RSS
// jitter is dominated by V8 heap growth and allocator arenas rather than by the
// loop. Kept rather than widened: the observed maximum is a cold-start artefact and
// widening a backstop to fit one sample is how a budget stops meaning anything.
//   WHAT IT CATCHES: gross native retention -- at 2,700 iterations it trips on
//       roughly >= 37 KiB/iteration held on the native heap (e.g. an un-dropped
//       per-stream row buffer over a 101-column table). That figure is ARITHMETIC
//       (budget / iterations), not a measured floor: see the header for why this
//       file has no native-side RED control while the Python lane does.
//   WHAT IT DOES NOT CATCH: anything smaller. It is a backstop for the gross
//       case, not an oracle (issue #3585).
// The MEDIAN ceilings (issue #1465 round 5 H1, RE-MEASURED in round 7 after the
// statistic became the UPPER median). PER PATH, because the two paths' noise
// differs by three orders of magnitude at this quantile and one number cannot be
// both flake-free and discriminating on both. Measured over 8 runs of 9 passes x
// 300 iterations at `bufferSize: 2`, upper median of the non-negative passes:
//   stream path: 56 (x7), 224 (x1)                       -> max 224
//   error path:  16 (x4), 544 (x1), 9,856 (x2), 137,416 (x1) -> max 137,416
// RE-MEASURED in round 9 post-rebase (10 runs, two-turn settle): stream upper median
// <= 1,168, error <= 17,784 -- both far inside their ceilings, and the error path's
// 137,416 outlier did not recur. The VALID-PASS COUNTS did move: stream 8-9 of 9,
// error 4-7 of 9, i.e. one run in ten falls below the quorum of 5 on the error path,
// which is why measureGrowth re-collects a below-quorum set up to
// MAX_MEASURE_ATTEMPTS times instead of erroring on the first attempt.
//
// STREAM: 64 KiB -- 56x the largest clean upper median measured post-rebase (1,168
// bytes; 293x against round 7's 224-byte maximum), and it still TRIPS the
// planted RED control (upper median 134,032 bytes post-rebase, 2.0x over; round 7
// measured 134,736 / 2.1x on the same plant). Discriminating AND
// flake-free, so this path gets a real majority constraint.
//
// ERROR: 512 KiB -- 3.8x round 7's largest clean upper median (137,416 bytes) and 29x
// the post-rebase one (17,784); the round-7 figure is the one the ceiling was SIZED
// against, and it is kept as the sizing rationale rather than replaced by the friendlier
// number. This one is HONESTLY
// WEAK and the number says why: the error path's clean upper median REACHES
// 137,416 bytes (round 7; the post-rebase re-measure saw <= 17,784) while the planted
// 256-byte-per-iteration control's ERROR-path median sits at 133,656 (round 7 — the
// only error-path planted MEDIAN ever recorded; the post-rebase re-run measured that
// plant's error-path MINIMUM, 117,744, which is a different statistic)
// -- noise and signal OVERLAP at this quantile, so no ceiling here can be both
// flake-free and sensitive to that plant. 512 KiB is therefore a GROSS-majority
// constraint only: it bites a leak retaining >= ~1.7 KB/iteration in a majority of
// passes, and nothing smaller. Sensitivity on this path comes from the MINIMUM
// ceiling (BUDGET_BYTES, clean 16..544 bytes vs 50,312 planted post-rebase -- see the RED
// control above), which is why both ceilings exist. Widening the median ceiling to
// catch the plant would have meant 256 KiB+, which catches nothing the minimum does
// not already catch, and accepting the 1-in-8 flake was not an option: a lane that
// reds on correct input is the lane people learn to waive.
const STREAM_MEDIAN_CEILING_BYTES = 64 * 1024 * BUDGET_RELAX_MULTIPLIER;
// Named GROSS on purpose (round 8): a call site sees only the identifier, so the
// weakness has to travel with the symbol rather than living in the comment above it.
const ERROR_MEDIAN_GROSS_CEILING_BYTES = 512 * 1024 * BUDGET_RELAX_MULTIPLIER;

// N1 (round 8): label and ceiling are bound in ONE table and passed as ONE argument,
// so transposing them is UNREPRESENTABLE at the call site. Swapping two positional
// arguments used to loosen the stream median ceiling 8x with every committed test
// still passing (the stream RED control at 134,032 sits under the error ceiling, so
// only the minimum would have bitten). A table cannot be transposed; a pair of
// arguments can.
const BUDGET_SUBJECTS = Object.freeze({
  errorPath: Object.freeze({
    label: 'error path (repeated rejections)',
    medianCeiling: ERROR_MEDIAN_GROSS_CEILING_BYTES,
  }),
  abandonedStream: Object.freeze({
    label: 'abandoned streaming iterators',
    medianCeiling: STREAM_MEDIAN_CEILING_BYTES,
  }),
});

const RSS_BUDGET_BYTES = 96 * 1024 * 1024 * BUDGET_RELAX_MULTIPLIER;

// Per-test timeout for the two multi-pass budgets (measured ~0.5s and ~4.5s on
// this machine). Declared here rather than as a project-level `testTimeout`,
// which trips a jest 29 config-validation warning.
const BUDGET_TEST_TIMEOUT_MS = 120_000;

/**
 * Loose, native-visible backstop: RSS growth over the whole measured window.
 *
 * No affirmative-measurement refusal here, deliberately (see budgetStatistics for
 * where there IS one): a non-positive RSS delta is a legitimate clean reading on
 * this metric -- measured clean values span -385,024 .. +21,164,032 bytes -- so
 * refusing <= 0 would red correct runs. The tracked-allocation statistic refuses
 * because its clean values never sit at or below zero. Same rule, different
 * measured baseline.
 */
function assertRssUnderBudget(label, rssGrowth, attempts = 1) {
  const total = attempts * MEASURE_PASSES * ITERATIONS;
  if (rssGrowth >= RSS_BUDGET_BYTES) {
    throw new Error(
      `${label}: RSS grew ${rssGrowth} bytes over ${total} iterations ` +
        `(${(rssGrowth / total).toFixed(1)} bytes/iteration), exceeding the ` +
        `loose ${RSS_BUDGET_BYTES}-byte native-visible budget. Unlike the ` +
        'heapUsed+external budget this one SEES Rust-side allocations, so a trip ' +
        'here points at gross native retention on this path (issue #1465)'
    );
  }
  expect(rssGrowth).toBeLessThan(RSS_BUDGET_BYTES);
}

// A verdict needs a MAJORITY of the passes behind it (issue #1465 round 7,
// roborev): refusing only the all-negative sample set was the affirmative-
// measurement rule applied one level too shallow. Worked example that used to
// PASS both ceilings: seven negative passes plus growth samples of 0 and
// 100,000 bytes -- the minimum reads 0 and the classic median of the two
// survivors reads 50,000, so ONE quiet pass excused the only leaking pass and no
// majority of the nine passes supported the verdict at all. Below quorum is now a
// hard error, in the same voice as the all-negative one.
// MEASURED HEADROOM (round 7, 8 runs of 9 passes at `bufferSize: 2`): the stream
// path produced 8-9 valid non-negative passes per run and the error path 6-7, so
// the quorum of 5 sits 1-4 passes below the worst observed count -- i.e. 2-3 of 9
// passes come back negative on the error path and the quorum still clears by 2.
// If that margin ever tightens, the below-quorum error names the count so the
// reader can see it rather than infer it.
const SAMPLE_QUORUM = Math.floor(MEASURE_PASSES / 2) + 1;

// How many times a pass set may be re-collected when it cannot reach the quorum. See
// measureGrowth for the measured single-attempt rate that sets this at 3.
const MAX_MEASURE_ATTEMPTS = 3;

/**
 * Collect a pass set, RE-COLLECTING up to `maxAttempts` times while it cannot reach the
 * quorum. Returns `{ samples, attempts }` — the LAST set collected and how many
 * collections happened, so callers can scale per-iteration accounting by `attempts`.
 *
 * This does NOT weaken the verdict: assertUnderBudget still refuses a below-quorum set,
 * and a real leak makes passes POSITIVE, so a retry can only ever help a set that had
 * too FEW valid samples — it can never mask a leak.
 *
 * MEASURED NEED, post-rebase, 10 runs per path: GC deferral leaves 2-5 of 9 error-path
 * passes NEGATIVE and 1 run in 10 below the quorum of 5 (the stream path: 0 in 10, 8-9
 * valid). Erroring on the first attempt would therefore red correct code one run in ten,
 * and a lane that reds on correct input is the lane people learn to waive. Three attempts
 * put that at an ESTIMATED ~1 in 1000 — and that estimate ASSUMES the three attempts are
 * INDEPENDENT, which was NOT measured, least of all under machine load, where GC deferral
 * is exactly the kind of thing that correlates across attempts. Treat it as an
 * order-of-magnitude expectation, never a bound; the MEASURED input is the single-attempt
 * rate above (1 run in 10 error path, 0 in 10 stream path). What holds regardless: an
 * instrument that is genuinely broken still ends in the named hard error rather than a
 * pass. Cost is bounded and paid only when needed (~0.5s per extra attempt on the path
 * that needs it).
 *
 * Extracted and directly tested (round 10): it was the one piece of new load-bearing
 * logic with no assertion of its own, and in this issue every such piece has turned out
 * wrong at least once.
 */
async function collectWithQuorum(collect, maxAttempts = MAX_MEASURE_ATTEMPTS) {
  // `attempts` MUST advance every iteration: a variant that failed to increment it was
  // tried as a RED control and did not produce a wrong verdict — it produced a
  // NON-TERMINATING loop (the run hung until killed). So the counter is pinned by the
  // two passing cases asserting `attempts === calls`, not by a red variant.
  let samples = await collect();
  let attempts = 1;
  while (
    samples.filter((sample) => sample >= 0).length < SAMPLE_QUORUM &&
    attempts < maxAttempts
  ) {
    samples = await collect();
    attempts += 1;
  }
  return { samples, attempts };
}

/**
 * PURE. Reduce per-pass samples to the statistics the budgets are asserted on,
 * or THROW a named error when the sample set cannot support a verdict at all.
 *
 * Two refusals, both "no verdict to give" rather than a pass:
 *   * every pass negative -- nothing was measured;
 *   * fewer than SAMPLE_QUORUM non-negative passes -- too little was measured.
 *
 * WHY THESE REFUSALS ARE SOUND HERE WHILE THE PYTHON LANE'S SIGN CHECK WAS NOT
 * (issue #1465 round 10 — the asymmetry is documented so it does not read as an
 * oversight). This is NOT a claim that a negative pass indicates failure: a negative
 * pass is a LEGITIMATE reading of a window that freed more than it allocated, which is
 * exactly why collectWithQuorum RE-COLLECTS instead of erroring, and why they are
 * excluded from the ordering rather than treated as evidence of a broken instrument.
 * The refusals are about STATISTIC FORMABILITY: a minimum and an upper median cannot be
 * computed from an empty set, and a MAJORITY-supported verdict cannot be computed from
 * fewer than a majority of samples. The python lane has one sample and no statistic to
 * form, so the equivalent question there is only "was the instrument on" — answered
 * affirmatively by `tracemalloc.is_tracing()` plus exact iteration counts, never by the
 * sign of its output (its round-8 sign check was removed for manufacturing a flake).
 *
 * The reported statistic is the UPPER median, `sorted[floor(n / 2)]`, which is
 * the middle element for odd `n` and the HIGHER of the two middles for even `n`
 * (n=5 -> index 2; n=4 -> index 2, i.e. the third of four). The classic even-`n`
 * median averages the two middles, which lets the favourable half pull the
 * verdict down -- exactly how the worked example above slipped through. With the
 * upper median, at least half of the valid passes are at or below the number
 * being asserted, so the verdict is majority-supported by construction.
 *
 * Exported for direct unit tests (see the "leak-budget statistic" describe): the
 * statistic had NO committed coverage of its own until round 7, only end-to-end
 * budget runs, which cannot construct a sample set like the worked example.
 */
function budgetStatistics(label, samples) {
  const nonNegative = samples.filter((sample) => sample >= 0);
  if (nonNegative.length === 0) {
    throw new Error(
      `${label}: all ${samples.length} passes measured NEGATIVE growth ` +
        `(samples=[${samples.join(', ')}]) — every pass freed more than it ` +
        'allocated, so this run measured nothing about the loop under test and ' +
        'has no verdict to give. Re-run; if it persists the instrument or the ' +
        'gc settling in settle() is broken (issue #1465)'
    );
  }
  if (nonNegative.length < SAMPLE_QUORUM) {
    throw new Error(
      `${label}: only ${nonNegative.length} of ${samples.length} passes measured ` +
        `non-negative growth, below the quorum of ${SAMPLE_QUORUM} — a verdict ` +
        'from a handful of surviving samples is not a measurement of this loop, ' +
        'so there is no verdict to give (a quiet pass could otherwise excuse a ' +
        'leaking one). Re-run; if it persists the instrument or the gc settling in ' +
        `settle() is broken. Per-pass samples=[${samples.join(', ')}] (issue #1465)`
    );
  }
  const sorted = [...nonNegative].sort((a, b) => a - b);
  return {
    count: nonNegative.length,
    total: samples.length,
    min: sorted[0],
    // UPPER median for BOTH parities -- see the doc comment.
    upperMedian: sorted[Math.floor(sorted.length / 2)],
  };
}

/**
 * Assert the measured growth is under the budgets, with every per-pass sample in
 * the failure message (jest's own `toBeLessThan` output would show only the
 * single asserted number, and the spread is what tells a real leak from a GC
 * artefact).
 *
 * TWO statistics are asserted (see MEASURE_PASSES): the MINIMUM non-negative pass
 * against BUDGET_BYTES (sensitivity) and the UPPER MEDIAN against the subject's own
 * `medianCeiling` (so the favourable half cannot carry the verdict). `subject` is an
 * entry of BUDGET_SUBJECTS -- ONE argument carrying both the label and its ceiling,
 * so the two cannot be transposed. See STREAM_MEDIAN_CEILING_BYTES /
 * ERROR_MEDIAN_GROSS_CEILING_BYTES for why one number cannot serve both paths. An
 * unmeasurable sample set is a hard error, never a pass -- see budgetStatistics.
 */
function assertUnderBudget(subject, samples) {
  const { label, medianCeiling } = subject;
  const { count, min, upperMedian } = budgetStatistics(label, samples);
  if (upperMedian >= medianCeiling) {
    throw new Error(
      `${label}: the UPPER MEDIAN non-negative pass grew ${upperMedian} bytes ` +
        `over ${ITERATIONS} iterations ` +
        `(${(upperMedian / ITERATIONS).toFixed(1)} bytes/iteration), exceeding ` +
        `the ${medianCeiling}-byte median ceiling — at least half of the ` +
        `${count} valid passes are retaining that much, which the minimum alone ` +
        `would not catch. Per-pass samples=[${samples.join(', ')}] (issue #1465)`
    );
  }
  expect(upperMedian).toBeLessThan(medianCeiling);

  if (min >= BUDGET_BYTES) {
    throw new Error(
      `${label}: tracked memory (heapUsed+external) grew by at least ${min} ` +
        `bytes in EVERY non-negative pass (${count} of ${samples.length}) of ` +
        `${ITERATIONS} iterations (${(min / ITERATIONS).toFixed(1)} ` +
        `bytes/iteration), exceeding the ${BUDGET_BYTES}-byte budget. ` +
        `Per-pass samples=[${samples.join(', ')}] (issue #1465)`
    );
  }
  expect(min).toBeLessThan(BUDGET_BYTES);
  return min;
}

/**
 * Total tracked bytes: the V8 heap PLUS the off-heap memory V8 was TOLD about
 * (`Buffer`/ArrayBuffer, napi external-memory adjustments). Not "all native
 * memory" -- an unreported Rust allocation appears in neither; that is the RSS
 * backstop's job.
 */
function trackedBytes() {
  const usage = process.memoryUsage();
  return usage.heapUsed + usage.external;
}

/**
 * Drive GC to a quiet point: collect, let the macrotask queue drain, collect again.
 *
 * TWO macrotask turns, not one (round 9): a pass whose delta comes back NEGATIVE is a
 * pass that freed memory allocated by an EARLIER pass, i.e. deferred collection, and
 * every such pass is one fewer valid sample for the quorum. Post-rebase measurement on
 * the error path put the negative rate at 2-5 of 9 passes with one turn — one run in
 * eight fell below the quorum of 5. Letting the queue drain twice around the
 * collections reduces the deferral rather than weakening the predicate.
 */
async function settle() {
  global.gc();
  global.gc();
  // Let pending microtasks/`setImmediate` callbacks (streaming completions) run
  // so nothing they hold is still reachable when the sample is taken.
  await new Promise((resolve) => setImmediate(resolve));
  global.gc();
  await new Promise((resolve) => setImmediate(resolve));
  global.gc();
}

/**
 * Run `body` WARMUP times, then measure MEASURE_PASSES x ITERATIONS of it.
 *
 * `body(counters)` is responsible for its own non-vacuity counting; this helper
 * only measures. Returns every per-pass growth sample in tracked bytes (the
 * statistic to assert on is chosen by assertUnderBudget()) plus total RSS growth
 * across the whole window, which is the loose native-visible backstop.
 */
async function measureGrowth(body, counters) {
  for (let i = 0; i < WARMUP; i += 1) {
    await body(counters);
  }

  await settle();
  const rssBefore = process.memoryUsage().rss;

  const collectPasses = async () => {
    const collected = [];
    for (let pass = 0; pass < MEASURE_PASSES; pass += 1) {
      await settle();
      const before = trackedBytes();
      for (let i = 0; i < ITERATIONS; i += 1) {
        await body(counters);
      }
      await settle();
      collected.push(trackedBytes() - before);
    }
    return collected;
  };

  const { samples, attempts } = await collectWithQuorum(collectPasses);
  return { samples, attempts, rssGrowth: process.memoryUsage().rss - rssBefore };
}

// ---------------------------------------------------------------------------
// DIRECT tests of the statistic (issue #1465 round 7). These are PURE -- no
// database, no gc, no datasets -- and they exist because an end-to-end budget run
// cannot construct the sample sets that matter: a real run cannot be made to
// produce "seven negative passes plus 0 and 100,000", which is precisely the shape
// that used to pass both ceilings. They live in this file, and therefore inside the
// merge-gating `leaks` project, rather than in a sibling the gate does not run.
// ---------------------------------------------------------------------------
describe('leak-budget statistic (pure, issue #1465)', () => {
  const neg = (n) => Array.from({ length: n }, (_, i) => -(i + 1) * 1000);

  test('budget relaxation is OPT-IN: no ambient marker can double a ceiling', () => {
    // roborev V1: the multiplier used to be `process.env.CI ? 2 : 1`, so `CI=false`
    // and `CI=0` relaxed every ceiling — and gate.yml runs the FULL gate inside
    // GitHub Actions, where `CI=true` is unconditional. Every one of these must
    // leave the budgets STRICT.
    for (const env of [
      {},
      { CI: 'true' },
      { CI: 'false' },
      { CI: '0' },
      { CI: '1' },
      { CI: '' },
      { GITHUB_ACTIONS: 'true' },
      { CQLITE_LEAK_BUDGET_RELAX: '' },
      { CQLITE_LEAK_BUDGET_RELAX: '0' },
      { CQLITE_LEAK_BUDGET_RELAX: 'false' },
      { CQLITE_LEAK_BUDGET_RELAX: 'true' },
      { CQLITE_LEAK_BUDGET_RELAX: '1' },
      { CQLITE_LEAK_BUDGET_RELAX: 'yes' },
      { CQLITE_LEAK_BUDGET_RELAX: '2X-CEILINGS' },
      { CQLITE_LEAK_BUDGET_RELAX: ' 2x-ceilings' },
      { CQLITE_LEAK_BUDGET_RELAX: '2x-ceilings-please' },
    ]) {
      const { relaxed, multiplier } = resolveBudgetRelaxation(env);
      expect({ env, relaxed, multiplier }).toEqual({ env, relaxed: false, multiplier: 1 });
    }
  });

  test('budget relaxation happens ONLY for the exact named token', () => {
    const { relaxed, multiplier } = resolveBudgetRelaxation({
      CQLITE_LEAK_BUDGET_RELAX: LEAK_BUDGET_RELAX_TOKEN,
    });
    expect(relaxed).toBe(true);
    expect(multiplier).toBe(2);
    // ...and the token names what it does, so a reader of an env dump can tell.
    expect(LEAK_BUDGET_RELAX_TOKEN).toBe('2x-ceilings');
  });

  test('a GATE-OF-RECORD run can never be relaxed (defence in depth)', () => {
    // The previous version of this test asserted
    // `BUDGET_RELAX_MULTIPLIER === (BUDGETS_RELAXED ? 2 : 1)`, which cannot fail on a
    // relaxed run — so nothing IN THE LANE could catch a relaxed gate, and the
    // gate-side `env -u` was the sole control. X1 then showed that single control
    // breaking (an operand-order bug made the component exit 127), which is exactly
    // why this needs a second, independent check.
    //
    // The signal: `CQLITE_JEST_JSON` is exported ONLY by the gate's node-bindings
    // component (it is where that component tells jest to write the report the
    // named-budget affirmation reads). Its presence therefore means "this is the
    // merge-gating execution", and in that execution relaxation is forbidden.
    const inGateComponent = (process.env.CQLITE_JEST_JSON || '') !== '';
    if (inGateComponent) {
      expect(process.env.CQLITE_LEAK_BUDGET_RELAX || '<unset>').not.toBe(
        LEAK_BUDGET_RELAX_TOKEN
      );
      expect(BUDGETS_RELAXED).toBe(false);
      expect(BUDGET_BYTES).toBe(32 * 1024);
      expect(STREAM_MEDIAN_CEILING_BYTES).toBe(64 * 1024);
      expect(ERROR_MEDIAN_GROSS_CEILING_BYTES).toBe(512 * 1024);
      expect(RSS_BUDGET_BYTES).toBe(96 * 1024 * 1024);
    }
    // Everywhere (gate or not): each ceiling must be its documented base times the
    // multiplier — this catches a hard-coded doubled base, which no env check would.
    expect(BUDGET_BYTES).toBe(32 * 1024 * BUDGET_RELAX_MULTIPLIER);
    expect(STREAM_MEDIAN_CEILING_BYTES).toBe(64 * 1024 * BUDGET_RELAX_MULTIPLIER);
    expect(ERROR_MEDIAN_GROSS_CEILING_BYTES).toBe(512 * 1024 * BUDGET_RELAX_MULTIPLIER);
    expect(RSS_BUDGET_BYTES).toBe(96 * 1024 * 1024 * BUDGET_RELAX_MULTIPLIER);
    // WHAT THIS DELIBERATELY DOES NOT DO: red on a relaxed run OUTSIDE the gate.
    // node-ci.yml's exempt legs relax on purpose and declare it; failing them here
    // would be the flaky red on an exempt lane that the opt-in exists to avoid.
  });

  test('the quorum is a majority of MEASURE_PASSES', () => {
    expect(SAMPLE_QUORUM).toBe(Math.floor(MEASURE_PASSES / 2) + 1);
    expect(SAMPLE_QUORUM * 2).toBeGreaterThan(MEASURE_PASSES);
  });

  test('all-negative sample set has NO verdict (hard error, never a pass)', () => {
    expect(() => budgetStatistics('t', neg(MEASURE_PASSES))).toThrow(
      /all 9 passes measured NEGATIVE growth/
    );
  });

  test('below quorum has NO verdict — INCLUDING the worked example that used to pass', () => {
    // 7 negative + 0 + 100,000: min reads 0 and the classic median of the two
    // survivors reads 50,000, so BOTH old ceilings passed on one quiet pass.
    const workedExample = [...neg(7), 0, 100_000];
    expect(workedExample).toHaveLength(MEASURE_PASSES);
    expect(() => budgetStatistics('t', workedExample)).toThrow(
      /only 2 of 9 passes measured non-negative growth, below the quorum of 5/
    );
    // ...and the assertion the budget tests actually call must refuse it too.
    expect(() =>
      assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, workedExample)
    ).toThrow(/below the quorum/);
  });

  test('exactly at quorum DOES yield a verdict, from the surviving passes', () => {
    const atQuorum = [...neg(4), 10, 20, 30, 40, 50];
    expect(atQuorum).toHaveLength(MEASURE_PASSES);
    const stats = budgetStatistics('t', atQuorum);
    expect(stats.count).toBe(SAMPLE_QUORUM);
    expect(stats.min).toBe(10);
    expect(stats.upperMedian).toBe(30); // sorted[floor(5/2)] = middle of five
  });

  test('ONE BELOW quorum is refused — the boundary, not just a far-below count', () => {
    // N3 (round 8): the case above uses 2 valid passes, so it also passes against
    // an off-by-one predicate (`< SAMPLE_QUORUM - 1`). This pins the boundary: 4
    // valid of 9 is the largest refused count, and 5 (asserted below) is the
    // smallest accepted one.
    const oneBelow = [...neg(MEASURE_PASSES - (SAMPLE_QUORUM - 1)), 1, 2, 3, 4];
    expect(oneBelow).toHaveLength(MEASURE_PASSES);
    expect(oneBelow.filter((x) => x >= 0)).toHaveLength(SAMPLE_QUORUM - 1);
    expect(() => budgetStatistics('t', oneBelow)).toThrow(
      /only 4 of 9 passes measured non-negative growth, below the quorum of 5/
    );
  });

  test('the statistic is the UPPER median for an EVEN count, not the average', () => {
    // SIX non-negative passes: even, and at/above quorum so a verdict IS issued.
    // (An even count of 4 is below quorum for MEASURE_PASSES=9 and is refused
    // instead — the boundary case directly above covers exactly that count.)
    const evenSet = [...neg(3), 10, 20, 30, 40, 50, 60];
    expect(evenSet).toHaveLength(MEASURE_PASSES);
    const stats = budgetStatistics('t', evenSet);
    expect(stats.count).toBe(6);
    // The classic median would be (30 + 40) / 2 = 35; the upper median is 40, so
    // the favourable half cannot pull the verdict down.
    expect(stats.upperMedian).toBe(40);
    expect(stats.min).toBe(10);
  });

  test('min <= upperMedian always, so the two ceilings cannot contradict', () => {
    for (const set of [
      [0, 0, 0, 0, 0],
      [1, 2, 3, 4, 5],
      [5, 4, 3, 2, 1],
      [7, 7, 7, 7, 7, 7],
      [...neg(3), 100, 1, 50, 2, 3, 4],
    ]) {
      const stats = budgetStatistics('t', set);
      expect(stats.min).toBeLessThanOrEqual(stats.upperMedian);
    }
    expect(STREAM_MEDIAN_CEILING_BYTES).toBeGreaterThan(BUDGET_BYTES);
    expect(ERROR_MEDIAN_GROSS_CEILING_BYTES).toBeGreaterThan(BUDGET_BYTES);
  });

  test('collectWithQuorum RETRIES a below-quorum set and reports the attempt count', async () => {
    const sets = [
      [...neg(6), 1, 2, 3], // 3 valid — below quorum
      [...neg(4), 1, 2, 3, 4, 5], // 5 valid — at quorum
    ];
    let calls = 0;
    const { samples, attempts } = await collectWithQuorum(async () => sets[calls++]);
    expect(calls).toBe(2);
    expect(attempts).toBe(calls); // attempts EQUALS the collection count
    expect(samples).toBe(sets[1]);
    expect(() =>
      assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, samples)
    ).not.toThrow();
  });

  test('collectWithQuorum does NOT retry a set that already meets quorum', async () => {
    let calls = 0;
    const good = [...neg(4), 8, 16, 24, 32, 40];
    const { samples, attempts } = await collectWithQuorum(async () => {
      calls += 1;
      return good;
    });
    expect(calls).toBe(1);
    expect(attempts).toBe(1);
    expect(samples).toBe(good);
  });

  test('three below-quorum sets STOP at maxAttempts and still have no verdict', async () => {
    let calls = 0;
    const { samples, attempts } = await collectWithQuorum(async () => {
      calls += 1;
      return [...neg(7), 10, 20]; // 2 valid every time
    });
    expect(calls).toBe(MAX_MEASURE_ATTEMPTS);
    expect(attempts).toBe(MAX_MEASURE_ATTEMPTS);
    // The retained set is STILL refused — retrying never converts "too few samples"
    // into a pass.
    expect(() => assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, samples)).toThrow(
      /below the quorum/
    );
  });

  test('each budget subject carries its OWN ceiling, stream STRICTER than error', () => {
    // N1: the table is what makes a transposition unrepresentable; this pins the
    // values in it, so a future edit that swaps the two ceilings INSIDE the table
    // still reds. The stream path must be the stricter of the two — that is the
    // whole reason there are two.
    expect(BUDGET_SUBJECTS.abandonedStream.medianCeiling).toBe(
      STREAM_MEDIAN_CEILING_BYTES
    );
    expect(BUDGET_SUBJECTS.errorPath.medianCeiling).toBe(
      ERROR_MEDIAN_GROSS_CEILING_BYTES
    );
    expect(BUDGET_SUBJECTS.abandonedStream.medianCeiling).toBeLessThan(
      BUDGET_SUBJECTS.errorPath.medianCeiling
    );
    // ...and the labels are distinct, so a failure message names the right path.
    expect(BUDGET_SUBJECTS.abandonedStream.label).not.toBe(
      BUDGET_SUBJECTS.errorPath.label
    );
  });

  test('the STREAM ceiling bites a leak the ERROR ceiling would let through', () => {
    // The measured gap this pair exists for: the stream RED control (134,032 B)
    // is over the stream ceiling but UNDER the error one, so a swap would have
    // silently loosened the stream guard 8x with every other test still green.
    const streamRedControl = 134_032;
    const samples = Array.from({ length: MEASURE_PASSES }, () => streamRedControl);
    expect(() =>
      assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, samples)
    ).toThrow(/UPPER MEDIAN/);
    // Same samples, error subject: the median ceiling does NOT bite (declared
    // weakness), and the MINIMUM ceiling is what catches it there.
    expect(() => assertUnderBudget(BUDGET_SUBJECTS.errorPath, samples)).toThrow(
      /grew by at least/
    );
  });

  test('a leak in a MAJORITY of passes trips the median ceiling', () => {
    const leaking = Array.from(
      { length: MEASURE_PASSES },
      () => STREAM_MEDIAN_CEILING_BYTES + 1
    );
    expect(() => assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, leaking)).toThrow(
      /UPPER MEDIAN/
    );
  });

  test('a clean sample set at quorum passes both ceilings', () => {
    expect(() =>
      assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, [...neg(4), 8, 16, 24, 32, 40])
    ).not.toThrow();
  });
});

describe('exception-path / abandoned-iterator leak budgets (issue #1465)', () => {
  let db;

  beforeAll(async () => {
    // FAIL LOUDLY, never skip: a missing/empty corpus must red this lane, since
    // a skipped leak budget is indistinguishable from a passing one.
    assertDatasetsAvailable();
    // Budget measurement is meaningless without gc control; FAIL, do not skip.
    if (typeof global.gc !== 'function') {
      throw new Error(
        'global.gc is unavailable — this lane must be run as `npm run test:leaks` ' +
          '(or any invocation that passes node --expose-gc, as the package.json ' +
          '"test" script does)'
      );
    }
    db = await Database.open(DIR, { schema: SCHEMA });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // -------------------------------------------------------------------------
  // Contract pins: the loop bodies below are only meaningful if these hold.
  // -------------------------------------------------------------------------

  test('executeNative rejects on bad CQL (the error path really is an error path)', async () => {
    // The literal shape the issue asks for, run OUTSIDE the measurement window:
    // jest's `expect(...).rejects` machinery allocates matcher state per call,
    // which would be measured noise inside the budgeted loop.
    for (let i = 0; i < 3; i += 1) {
      await expect(db.executeNative(BAD_CQL)).rejects.toThrow();
    }
    // ...and it is the typed error the measured loop below counts on.
    await expect(db.executeNative(BAD_CQL)).rejects.toMatchObject({
      code: EXPECTED_ERROR_CODE,
    });
  });

  test('breaking out of a stream leaves native rows outstanding, then closes them', async () => {
    // 1. The fixture must hold MORE than STREAM_ROWS rows, or "abandoned
    //    mid-stream" would silently mean "exhausted".
    let total = 0;
    for await (const row of db.executeStreaming(STREAM_QUERY, STREAM_CONFIG)) {
      expect(row).toBeDefined();
      total += 1;
    }
    expect(total).toBeGreaterThan(STREAM_ROWS);

    // 2. Break after STREAM_ROWS rows, sampling the NATIVE fetch counter as we go.
    const stream = db.executeStreaming(STREAM_QUERY, STREAM_CONFIG);
    let pulled = 0;
    let nativeFetchedAtBreak = 0;
    for await (const row of stream) {
      expect(row).toBeDefined();
      pulled += 1;
      // `rowsReceived` is the native iterator's own count of rows FETCHED (not
      // yielded), so it must be read INSIDE the loop: the native `close()` drops
      // that iterator and the getter then reports 0 (see `rows_received` in
      // src/streaming.rs, whose `None` arm returns 0).
      nativeFetchedAtBreak = stream.rowsReceived;
      if (pulled >= STREAM_ROWS) break; // -> iterator.return() -> close()
    }
    expect(pulled).toBe(STREAM_ROWS);

    // 3. THE PROPERTY THIS TEST EXISTS FOR (issue #1465 round 4): the native stream
    //    was still IN FLIGHT at the break -- it had fetched some rows but nowhere
    //    near all of them. With the default bufferSize this assertion fails
    //    (measured: 50 of 50 fetched after the first yield), which is why
    //    STREAM_CONFIG exists.
    expect(nativeFetchedAtBreak).toBeGreaterThanOrEqual(STREAM_ROWS);
    expect(nativeFetchedAtBreak).toBeLessThan(total);

    // 4. Native closure, now a real signal: the getter reports 0 only when the
    //    native iterator is GONE. Since step 3 proved it was NOT exhausted, a 0
    //    here can only mean `return()` -> `close()` dropped it.
    expect(stream.rowsReceived).toBe(0);

    // 5. JS-side closure: re-iterating a closed stream yields nothing.
    let afterBreak = 0;
    for await (const row of stream) {
      expect(row).toBeDefined();
      afterBreak += 1;
    }
    expect(afterBreak).toBe(0);
  });

  // -------------------------------------------------------------------------
  // The budgets.
  // -------------------------------------------------------------------------

  test('repeated query rejections stay under the leak budget', async () => {
    // `wrongType` closes the hole where a synchronous TypeError (a renamed
    // `executeNative`, a closed database) would satisfy non-vacuity: the loop
    // demands the AUTHORITATIVE error identity, matching python's typed
    // `except cqlite.QueryError`. `code` is the binding's authoritative error
    // identity (see __test__/error.test.js): an unsupported statement type is
    // category Query -> code 'QUERY'.
    const counters = { rejected: 0, resolved: 0, wrongType: [] };
    const { samples, attempts, rssGrowth } = await measureGrowth(async (c) => {
      try {
        await db.executeNative(BAD_CQL);
        c.resolved += 1;
      } catch (err) {
        if (err && err.code === EXPECTED_ERROR_CODE) {
          c.rejected += 1;
        } else {
          // Record, do not throw: throwing inside the measured loop would
          // abandon the measurement mid-pass. Asserted right after it.
          c.wrongType.push(`${err && err.name}:${err && err.code}`);
        }
      }
    }, counters);

    // NON-VACUITY: every iteration must have rejected. If BAD_CQL ever stops
    // rejecting, this loop degenerates into a no-op and the budget would pass
    // while measuring nothing.
    const expected = WARMUP + attempts * MEASURE_PASSES * ITERATIONS;
    expect(counters.resolved).toBe(0);
    // Every rejection must be the EXPECTED error, not just any throw.
    expect(counters.wrongType.slice(0, 5)).toEqual([]);
    expect(counters.rejected).toBe(expected);

    // BOUNDED, not zero (see file header).
    assertUnderBudget(BUDGET_SUBJECTS.errorPath, samples);
    assertRssUnderBudget(BUDGET_SUBJECTS.errorPath.label, rssGrowth, attempts);
  }, BUDGET_TEST_TIMEOUT_MS);

  test('abandoned streaming iterators stay under the leak budget', async () => {
    const counters = { rows: 0, iterators: 0 };
    const { samples, attempts, rssGrowth } = await measureGrowth(async (c) => {
      let pulled = 0;
      for await (const row of db.executeStreaming(STREAM_QUERY, STREAM_CONFIG)) {
        pulled += 1;
        // Abandoned with native rows still outstanding (see STREAM_CONFIG), not
        // merely with JS-buffered rows unread.
        if (pulled >= STREAM_ROWS) break;
      }
      c.rows += pulled;
      c.iterators += 1;
    }, counters);

    // NON-VACUITY: a 0-row (or short) stream would make the abandonment a
    // no-op. This is also the FAIL-LOUDLY check for a present-but-unreadable
    // corpus — it fails, it never skips.
    const expectedIterators = WARMUP + attempts * MEASURE_PASSES * ITERATIONS;
    expect(counters.iterators).toBe(expectedIterators);
    expect(counters.rows).toBe(STREAM_ROWS * expectedIterators);

    // BOUNDED, not zero (see file header).
    assertUnderBudget(BUDGET_SUBJECTS.abandonedStream, samples);
    assertRssUnderBudget(BUDGET_SUBJECTS.abandonedStream.label, rssGrowth, attempts);
  }, BUDGET_TEST_TIMEOUT_MS);
});
