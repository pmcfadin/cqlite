# Tasks: concurrency-admission-defaults (issue #3225)

> Design decided in `design.md`. **Scope:** derive the `--max-concurrent-scans` default from
> available parallelism (`clamp(2 × P, 2, 64)`), make provenance visible at startup, re-measure the
> peak-N curve with the *shipped* default (64) inside the ramp, and publish operator sizing guidance.
> **NOT in scope:** the admission mechanism (#2420), read/encode-path perf (#3096/#3058),
> `--batch-size` / channel capacities, the glibc-arena single-stream phenomenon, footprint-aware
> admission (#3306).
>
> **Ordering matters.** §2 (the measurement) gates §3 (the default flip). The formula is a
> hypothesis fitted to two uncensored points; if the sweep contradicts it at any width, re-fit before
> shipping. Do not implement the default first and measure to confirm it.
>
> **Commit after each numbered section.** A subagent starved of CPU by a co-scheduled gate is killed
> by the 600 s stall watchdog and loses every uncommitted change (CLAUDE.md).

## 1. Correct the record (surface: the issue thread, the artifacts)

- [ ] Post a comment on #3225 recording that the shipped default is
      `DEFAULT_MAX_CONCURRENT_SCANS = 64` (`cqlite-flight/src/admission.rs:43`, wired at
      `main.rs:47`), not 16; that `16` is the top of #3217's N ramp; and that this makes the measured
      1-core cost a **lower bound**. Retitling / re-scoping the issue is an **owner action** —
      record, do not execute. Surface it as a NEEDS-YOU item.
- [ ] Confirm with the owner (Seam 1) which of `design.md` D9's options is approved before any code
      lands. The design recommends **B**; **A** (guidance only) drops §3 and keeps §2, §4, §5.

## 2. Measurement — reproduce and extend (ACs 1, 5, 6) — GATES §3

- [ ] Stage the corpus. #3217's binaries are **gone** (`/data/ws0` does not exist on the box) and are
      gitignored. Regenerate to the recipe in
      `docs/reports/ws0-3217-artifacts/corpus/corpus-geometry.txt` (200,000 partitions × 20 rows,
      `nb-16-big`, single SSTable after flush + compact, LZ4 16 KiB chunks).
- [ ] Record the NEW geometry with `harness/corpus-basis.py`: rows, partitions, rows/partition,
      format/generation, uncompressed and compressed B/row, ratio, `sha256(Data.db)`, plus an
      independent row-count oracle. Publish the field-by-field comparison against #3217's geometry;
      label any material divergence. (Surface: the committed report; AC6.)
- [ ] Run `harness/selftest.sh` first — 36 mechanics checks needing neither corpus nor server.
- [ ] Verify the rig against report §2.1: Xeon 8488C class, 8 physical / 16 logical, SMT on, siblings
      read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` (never assumed), client
      pinned to `6,7,14,15`, Cassandra stopped. Confirm `sweep.sh`'s server/client overlap refusal
      fires when given an overlapping set (it is the validity guard).
- [ ] Adapt `partA-run/run-partA.sh` into the #3225 arm chain: widths `S ∈ {1,2,3,4,6}` (S=3 via
      `sweep.sh`'s literal CPU-list form — no code change needed), ramp `1,2,4,8,16,24,32,64`,
      120 s steps, 3 reps, 45 s warm, 5 s settle, bypass merge path. Budget ~1 h per arm, ~5–6 h
      unattended total.
- [ ] Reuse unchanged: `harness/common.sh`, `harness/sweep.sh`, `harness/emit-point.py`,
      `harness/summarize-sweep.py`, `harness/corpus-basis.py`. Do **not** run any of `partB-run/` or
      the `profile-*`/`classify-offcpu`/`runqlat` attribution chain — this round measures a curve, and
      skipping them drops the `perf_event_paranoid`/`kptr_restrict` dependency.
- [ ] Adapt `partA-run/analyze-partA.py` to emit: the per-(S,N) median table with min/max dispersion;
      the peak `N` per width, **labelled censored** where it sits at the ramp top, with server
      utilisation beside it; the over-admission cost table in **both** currencies (throughput %, p50
      multiple); the admission-rejection total across all points; the three byte bases.
- [ ] Evaluate `clamp(2 × P, 2, 64)` against each measured width and publish the deviation as a % of
      that width's measured peak. **A width where the formula is worse than the current constant
      blocks §3 until the coefficient is re-fitted.**
- [ ] AC5: at the widest width in scope (6 physical / 12 threads) publish the derived-default point
      and the `N = 64` point, both as medians of ≥3 with dispersion, and state the comparison against
      that dispersion. Declare in the report **why** 6 is the widest in scope (the client needs 2
      exclusive physical cores on the same box; `sweep.sh` refuses an overlapping set).
- [ ] Highest-value optional extension, if a non-SMT box is available: one width on a non-SMT host,
      to test the hardware-thread basis (`design.md` D3 residual 2). If unavailable, say so.
- [ ] Commit the report + artifacts under `docs/reports/`. **These are reviewed code, not docs** — the
      PR is not a docs-only change and must be roborev-certified.

## 3. The derived default (surface: `cqlite_flight::admission`, `cqlite-flight` CLI) — TDD, tests first

- [ ] Write the failing table test first in `cqlite-flight/src/admission_tests.rs` (or a new
      `cqlite-flight/tests/issue_3225_derived_default.rs`): the pure derivation at
      `P ∈ {1,2,3,4,6,8,12,16,24,31,32,33,64,1024}` → `{2,4,6,8,12,16,24,32,48,62,64,64,64,64}`,
      monotonicity, and the two measurement-pinned points `P=4 → 8`, `P=8 → 16`.
- [ ] Add to `cqlite-flight/src/admission.rs`: `MIN_DERIVED_MAX_CONCURRENT_SCANS = 2`,
      `DERIVED_SCANS_PER_HARDWARE_THREAD = 2`, a pure
      `derive_max_concurrent_scans(p: usize) -> usize`, and `default_max_concurrent_scans()` that
      probes `std::thread::available_parallelism()` and applies it. Keep
      `DEFAULT_MAX_CONCURRENT_SCANS = 64` as the ceiling constant and update its doc comment to say
      it is now the **cap**, citing #3217 and #3225. Keep the file under the ~800-line target.
- [ ] Never `num_cpus::get_physical()`; never read `/proc/cpuinfo` or `/sys/devices/system/cpu/**` on
      this path (`design.md` D3 — `num_cpus-1.17.0/src/linux.rs:59-97` applies neither the cgroup
      quota nor the affinity mask).
- [ ] Add the structural guard test: the derivation path contains no `/proc/cpuinfo`,
      `/sys/devices/system/cpu` or `get_physical` reference. (Surface: the source itself; AC2.)
- [ ] Update `AdmissionConfig::default()` and `from_env()` (`admission.rs:100-131`) to fall back to
      the derived value; preserve the existing "unparseable value falls back rather than failing
      startup" contract and the `>= 1` filter.

## 4. Provenance + precedence (surface: `cqlite-flight` CLI / startup log; AC4)

- [ ] Write the failing precedence tests first, driven through the **real clap parser**: flag > env >
      derived; an explicit value is not clamped toward the derived one; `--max-concurrent-scans 64`
      reproduces the pre-change ceiling on any host.
- [ ] Replace `#[arg(… default_value_t = DEFAULT_MAX_CONCURRENT_SCANS)]` at
      `cqlite-flight/src/main.rs:47` with `Option<usize>` + an explicit resolver returning
      `(usize, Source)`, where `Source ∈ {flag, env, derived, derived-fallback}` distinguished via
      `ArgMatches::value_source`. `default_value_t` cannot tell "user typed 64" from "nobody typed
      anything" — that distinction *is* AC4.
- [ ] `derived-fallback` (the `available_parallelism() == Err` arm) resolves to 64, the pre-#3225
      behaviour, and is labelled distinctly — never reported as `derived`.
- [ ] Extend the existing `tracing::info!(… "cqlite-flight starting")` at `main.rs:162` with
      `max_concurrent_scans_source` and `available_parallelism` (omitted on `Err`). No new log event.
      `max_concurrent_scans` keeps reporting `admission.limit()` (post-clamp).
- [ ] Test each of the four `Source` values from its own input, including the injected-`Err` arm.
- [ ] Affinity conformance: a `taskset`-restricted start logs `available_parallelism` = mask size and
      the derived value follows. If written as a Rust test it must be `#[ignore]`d and marked
      `perf-gate-allow` if it carries any timing assert; prefer a harness script with no timing
      assert at all. Record the **cgroup** arm as evidence captured on the measurement box, not as a
      gate (the gate runner's cgroup is not ours to control).

## 5. Documentation (AC3 — required even if the owner picks option A)

- [ ] `cqlite-flight/README.md`: the measured peak-N-by-width table, the over-admission cost in both
      currencies (16.4% throughput / 31 s → 302 s p50 at one core, refreshed from §2's numbers), the
      derived-default formula and its 64 ceiling, the startup provenance field, and the override
      recipe with the exact pre-#3225 restoration setting.
- [ ] Document both residuals beside the recipe: the −4.8% deviation at the narrowest width, and the
      unvalidated non-SMT extrapolation (half the fitted per-physical-core value on a non-SMT host).
- [ ] Correct every published place that states the old constant behaviour, in this same change:
      `docs/observability/README.md:53`, `docs/development/flight-doget-callgraph.md:41`, the
      `docs/flight-trino/JOURNAL.md` admission entries, and the `--max-concurrent-scans` help text at
      `main.rs:41-46` (whose current doc comment says "sized … not core count" — now false).
- [ ] `CHANGELOG.md`: a **behaviour change** entry under `cqlite-flight`, not a fix.
- [ ] If any `website/src/content/docs/agents-using/` page states the default, update it and verify
      publication by grepping the **served page for the new phrase**, never by HTTP 200 (CDN serves
      stale content for ~3 min after a green deploy).

## 6. Verification

- [ ] `bash scripts/agent-gate.sh --lite` each fix round, summary-file redirect
      (`AGENT_GATE_SUMMARY_FILE=…`), poll on `RESULT: (PASS|FAIL)` — `INCOMPLETE` is a liveness
      sentinel, not a verdict. Check `tree-integrity:` alongside `RESULT:`.
- [ ] `rust-reviewer` + roborev on the lite-green diff **before** the first full gate (review-first).
      roborev via `bash scripts/flow/roborev-review.sh --agent <agent> --model <model> --repo <abs>`
      only — both `--agent` and `--model` are required. The PR carries `docs/reports/*-artifacts/`
      harness executables, so it is **not** a docs-only change and must be roborev-certified.
- [ ] Open the PR; hand the endgame to `flow-closer`: ONE full `scripts/agent-gate.sh` → `spec-auditor`
      (C) against `openspec/changes/concurrency-admission-defaults/specs/**` → final roborev →
      `scripts/flow/premerge-assert.sh <pr> <certified-sha>` → `gh pr merge --auto --squash
      --delete-branch` → `flow-finalize`.
- [ ] Confirm no wall-clock threshold assert entered the correctness test path (the `roborev-lints` /
      `tooling-tests` gate component enforces this; any deliberate perf assert is `#[ignore]`d and
      marked `perf-gate-allow`).
- [ ] Confirm the diff touches no admission-mechanism logic beyond the default value and the log
      fields, and no `--batch-size` / channel-capacity / read-path / encode-path code — the issue's
      out-of-scope list is a merge condition.
