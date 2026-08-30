#!/usr/bin/env bash
# Canonical agent gate (issue #719).
#
# This script IS the gate. A builder claiming "the gate passed" must have run
# this script and pasted its summary block verbatim; ad-hoc cargo invocations
# do not count. It exists because epic #646 shipped three false-green reports
# rooted in "which commands count as the gate" ambiguity (feature-gated tests
# silently skipping, filtered runs reported as full runs).
#
# Components mirror the enforced CI gates (.github/workflows/ci.yml,
# ci-minimal-features.yml, python-ci.yml) plus the local smoke suite:
#   fmt                cargo fmt --all --check
#   clippy             RUSTFLAGS="-D warnings" clippy, SCOPED per-package (issue
#                      #1844): whole-workspace lint that deliberately does NOT
#                      compile the source-built DuckDB C++ amalgamation
#                      (cqlite-cli `duckdb-tests`) or the OpenTelemetry/OTLP stack
#                      (`observability`/`observability-testing`) — both are pure
#                      per-gate tax (-D warnings gives clippy a distinct fingerprint,
#                      so no other component reuses them). parquet/arrow stay linted.
#                      Set CQLITE_CLIPPY_FULL=1 to run the historical
#                      `--workspace --all-targets --all-features` matrix instead; the
#                      nightly gate.yml deep-check runs that full matrix in its own
#                      parallel `clippy-full` job (issue #2662) so the otel/duckdb-
#                      inclusive lint still runs within 24h (coverage moved, not deleted).
#   core-tests         cargo test -p cqlite-core --features cli-helpers (CI skip-list applied)
#   scan-offload-guard cargo test -p cqlite-core --features cli-helpers,scan-offload-probe
#                      --test issue_1143_scan_offload_thread (windowed-scan parse
#                      runs off the async worker pool; probe is feature-gated so
#                      the default core-tests run can't execute it — issue #1143).
#                      Also runs --test issue_1333_scan_scratch_reuse (the
#                      windowed scan's per-partition scratch Vec is reused, not
#                      reallocated per partition — issue #1333) and
#                      --test issue_1589_window_drain_bytes (the scan/compaction
#                      windows advance a cursor + compact once per refill instead
#                      of front-draining per partition — issue #1589); and the F3
#                      I/O-offload guards --test issue_1593_io_offload_thread (an
#                      mmap-backed scan's blocking raw chunk read runs off the async
#                      worker pool) + --test issue_1593_mmap_scan_parity (that
#                      scheduling change is data-transparent — issue #1593); and the
#                      F4 admission guard --test issue_1594_scan_admission_bound
#                      (concurrent windowed scans admitted to the blocking pool
#                      never exceed the admission limit — issue #1594); and the
#                      eager-merge admission guard
#                      --test issue_2063_eager_merge_admission_bound (concurrent
#                      write-support multi-generation EAGER-materialize scans are
#                      bounded by the SAME operation-concurrency semaphore — issue
#                      #2063). It then
#                      runs a SECOND invocation: cargo test -p cqlite-core --lib
#                      --features cli-helpers,scan-offload-probe --
#                      scan_admission issue_1594_fanout_deadlock — the fan-out
#                      DEADLOCK regression
#                      guard is a LIB #[cfg(test, scan-offload-probe)] module (to
#                      reach pub(crate) manager.table_readers), so no --test run
#                      compiles it; this lib run makes it execute (issue #1594
#                      roborev, same gate-wiring class as #1597/#1618); same gate.
#   work-counters-guard cargo test -p cqlite-core --features cli-helpers,work-counters
#                      the read/parser work-counter wiring-evidence tests
#                      (issue_1566/1573/1585 read-work counters + issue_1618 parser
#                      work counters + issue_1570 key-offset-cache INDEX_PROBES/
#                      TRIE_WALKS wiring + issue_1575 candidate-key hash hoist +
#                      issue_1576 first/last-key range short-circuit +
#                      issue_1577 LIMIT decode-stop evidence). The
#                      counter bodies/getters are feature-gated
#                      behind `work-counters`, so the default core-tests run can't
#                      execute them — without this component the wiring evidence
#                      would only run under a manual `--features work-counters`
#                      invocation (issue #1618).
#   byte-budget-guard  cargo test -p cqlite-core --features write-support,cli-helpers,state_machine
#                      --test issue_1582_byte_bounded_result_budget (issue #1582,
#                      Epic D6). Byte-bounded result budget: the materializing
#                      SELECT path fails fast with Error::ResultTooLarge once the
#                      configured max_result_bytes is exceeded, and a LIMITed query
#                      is honored before the budget bites. Feature-gated to a combo
#                      no other gate component runs while naming this target, so the
#                      guard has an executing regression net. Builds its own
#                      WriteEngine fixtures -> needs NO datasets (not in
#                      DATASET_COMPONENTS).
#   arrow-parity-guard cargo test -p cqlite-core --features arrow
#                      --test issue_1495_arrow_accessor_parity (issue #1495, AE1).
#                      The Arrow accessor-hoist byte-identity parity net. This test
#                      is `#![cfg(feature = "arrow")]` + Cargo required-features =
#                      ["arrow"], so NO other gate/CI run compiles it: core-tests
#                      runs only `--features cli-helpers` (arrow OFF -> skipped) and
#                      pr-gate's `--lib --all-features` excludes tests/ integration
#                      targets. Without this component the sole correctness proof of
#                      the refactor never executes (the #1597/#1618 gate-wiring
#                      class). Builds in-memory QueryRows -> needs NO datasets (not
#                      in DATASET_COMPONENTS). Fails CLOSED on a vacuous 0-run:
#                      asserts the reported test count is > 0 so a renamed/removed
#                      target can't read as PASS.
#   memory-budget      dhat allocation/peak-heap regression nets (dhat-heap;
#                      --test-threads=1 since dhat::Profiler is a process-global
#                      singleton). Three lanes: (a) read path — memory_budget.rs
#                      (issue #1565, Epic A/A4), pinning full-scan total-bytes
#                      (~209 MB, ceiling 252 MB) + materializing peak (~4.9 MB,
#                      ceiling 6 MB, also < 128 MiB); (b) export converter —
#                      issue_1494_converter_alloc_budget.rs (issue #1494, AD5;
#                      needs `arrow`), per-row CQL→Arrow alloc count; (c) Flight
#                      producer — cqlite-flight issue_1494_producer_mem_budget.rs
#                      (#1494), producer total/peak bytes; (d) row-assembly path —
#                      issue_2075_row_assembly_alloc_budget.rs (issue #2075),
#                      absolute allocs/row + allocs/cell for the decode->RowCells->
#                      QueryRow scan path across a wide-row + text-heavy shape
#                      (measures/gates the #1645 item 2 smallvec-RowCells win).
#                      dhat counts are machine-independent, so this is the hard,
#                      load-deterministic export/Flight/read signal. Dataset-
#                      dependent lanes fail closed on empty (assert >=1 row/cell/
#                      alloc before reading dhat stats).
#   integration-tests  cargo test -p cqlite-integration-tests: compile ALL targets
#                      (--no-run, whole package) then run the seven CI-enforced ones
#   format-compat      cargo test -p format-compatibility-tests (the 'oa' format crate;
#                      issue #865 folded it into the workspace so fmt/clippy reach it)
#   write-tests        cargo test -p cqlite-core --features write-support (lib + roundtrip + compaction)
#   cli-tests          ENUMERATES the cqlite-cli/tests/*.rs glob (#2039), not a
#                      hardcoded allowlist. Pass 1 (default/read-only): the glob minus
#                      required-features targets minus a documented QUARANTINE of
#                      pre-existing-red targets. Pass 2 (--features write-support):
#                      the write-support-gated targets derived from Cargo.toml
#                      required-features + two self-gated ground-truth targets. New
#                      files are auto-covered; fails closed on zero targets.
#   bti-multiclustering  the compound-clustering BTI ('da') lane (issue #3032/#3220):
#                      issue_3032_multiclustering_rows_trie_shape +
#                      issue_3032_multiclustering_clustering_slice_select against the
#                      COMMITTED test_da/multiclustering_table fixture, pinned to
#                      CQLITE_REQUIRE_FIXTURES=1 so an absent fixture FAILs rather
#                      than silently skipping, PLUS point_vs_full_differential (#3220)
#                      whose AC6 case compares the point and full READ PATHS over that
#                      same fixture and is fail-closed by its own must_run assertion.
#                      Unconditionally fail-closed (the fixtures are in git, so they
#                      are present in every checkout).
#   python-bindings    maturin develop + pytest bindings/python/tests in a throwaway
#                      venv; SKIPs (never silently PASSes) if python3 is unavailable.
#                      Set RUN_SLOW_TESTS=1 to also run the CLI-parity suite.
#                      The full pytest run includes the #1231 Python write→read
#                      content proof (test_write_readback_content.py), so a core
#                      write-format regression reds a binding content test.
#   node-bindings      napi build + the #1231 Node write→read content proof
#                      (npx jest write-readback-content) in bindings/node; SKIPs
#                      (never silently PASSes) if node/npm is unavailable. Scoped
#                      to the content proof (not full `npm test`) so it stays
#                      fast and corpus-free while still failing closed on a Node
#                      write-path regression (#1255).
#   binding-rust-tests EXECUTES the RUST test suites of the two binding-side crates no
#                      other component runs (#3522): `cargo test -p cqlite-ffi-common`
#                      (ALL targets — lib + tests/dependency_boundary.rs +
#                      tests/error_contract_table.rs) and `cargo test -p cqlite-node
#                      --features write-support --lib`. Before it, BOTH executed
#                      NOWHERE — not locally, not in CI: clippy --all-targets COMPILED
#                      them and nothing RAN them, so an inverted assertion in either
#                      could merge with every check green. Compiling is not covering
#                      (#1699), and that holds at PACKAGE granularity too.
#                      DELIBERATELY NOT folded into node-bindings: that component SKIPs
#                      when node/npm is absent, and putting cqlite-node's RUST tests
#                      behind that SKIP would be a coverage hole wearing a SKIP's
#                      clothes. This one needs nothing beyond cargo and NEVER SKIPs.
#                      Every subject set (integration targets + their runner ids,
#                      unittest targets, enabled/declared features) is DERIVED from
#                      cargo at run time, so a new tests/*.rs is covered with no gate
#                      edit; a failed derivation FAILs naming the derivation. Guards are
#                      affirmative, not exit-code-shaped: check_unittest_targets_ran
#                      per package, check_test_targets_observed over the derived
#                      integration set, and check_no_unexpected_zero_tests with an EMPTY
#                      allowed-zero list. Prints a COVERAGE CENSUS on stdout and at the
#                      head of its log naming what it does NOT run (cqlite-py's Rust
#                      tests — structurally unlinkable; the jest suite — node-bindings'
#                      subject; cqlite-node's derived integration-target count; the
#                      features left off). Needs NO fixtures (verified: neither crate's
#                      sources reference CQLITE_DATASETS_ROOT), so NOT in
#                      DATASET_COMPONENTS.
#   parity-report      cassandra-parity report --check: FAILs (naming
#                      docs/reports/cassandra-test-parity.md) when the committed
#                      derived report drifts from a fresh render of
#                      test-data/cassandra-parity-manifest.yml. Catches the
#                      single-PR "changed the manifest, forgot to regenerate the
#                      report" case at the local gate, before push (issue #1338).
#                      SKIP-aware (loud, never silent PASS): SKIPs when the
#                      cassandra-parity crate (tools/cassandra-parity) or the
#                      manifest is absent (a minimal checkout). No Docker/datasets
#                      — reads the manifest + committed report only. NOTE: a stale
#                      report can ALSO arise post-merge from a semantic merge race
#                      (two manifest-changing PRs), which no per-PR/local check can
#                      see; that path self-heals via the push-to-main job in
#                      .github/workflows/cassandra-parity.yml (issue #1338).
#   operator-metrics-doc
#                      gen_operator_metrics_doc --check: FAILs (naming
#                      docs/reports/flight-metrics-reference.md) when the committed
#                      operator-facing Flight metrics reference drifts from a fresh
#                      render of the observability catalog, or when a catalogued
#                      metric lacks its operator annotation (fail-closed). Catches
#                      the "added/renamed a cqlite.* instrument, forgot to
#                      regenerate the field-team doc" case at the local gate
#                      (issue #2426). SKIP-aware (loud, never silent PASS): SKIPs
#                      when cqlite-core is absent (a minimal checkout). No
#                      Docker/datasets — reads the catalog + committed doc only.
#   kit-dashboard-drift
#                      cqlite-core kit_dashboard_metric_drift test: FAILs (naming
#                      the phantom name) when the kit Grafana dashboard
#                      (easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json)
#                      references a cqlite.* metric name absent from
#                      catalog::ALL_METRICS, or when the dashboard JSON is
#                      malformed. Catches the "renamed/removed a cqlite.* instrument,
#                      the kit dashboard now points at a phantom metric" case at the
#                      local gate (issue #2427). SKIP-aware (loud, never silent
#                      PASS): SKIPs when cqlite-core or the dashboard is absent. No
#                      Docker/datasets — reads the catalog + dashboard JSON only.
#   binding-unwind-profile
#                      fail-closed guard (#1440): the shipped Python wheel and
#                      Node prebuild build definitions must select
#                      `--profile release-unwind` (PyO3/napi catch_unwind firewall
#                      active) and never `--release` (abort). Reads the four build
#                      definitions (python-release.yml, pyproject.toml [tool.maturin],
#                      package.json build script, node-release.yml); hard-FAILs on
#                      any abort-built or missing/unparseable definition. Pure
#                      bash/grep/awk — offline, deterministic, no datasets/network.
#   pub-surface        CRATE-ROOT DECLARATION-CONSISTENCY guard for cqlite-core
#                      (scripts/ci/check-pub-surface.sh, issue #1712). Asserts the
#                      crate root TELLS THE TRUTH: for every unconditional,
#                      non-#[doc(hidden)] top-level `pub mod NAME;` in
#                      cqlite-core/src/lib.rs it resolves NAME's own module file
#                      (NAME.rs xor NAME/mod.rs) and reads its PROLOGUE, failing if
#                      the module gates ITSELF with an inner #![cfg(...)] — the
#                      #1712 defect, where an inner #![cfg] hid `benchmarks` from
#                      every default build while the crate root advertised it
#                      unconditionally. Declaration-site attributes are read
#                      structurally over meta-items, never by substring.
#                      NOT PUBLIC-API DRIFT DETECTION: no snapshot, no --regenerate.
#                      The rustdoc-derived snapshot half was removed deliberately
#                      (#1712 descope; the principled route is #3366), so a green
#                      here says nothing about whether the public API changed.
#                      Fail-closed and affirmative — an unparseable crate root, an
#                      unrecognised `pub mod` shape, zero declarations, zero
#                      unconditional declarations, a module file resolving to
#                      neither/both legal paths, an unreadable module file, a block
#                      comment in a prologue or an inner attribute it cannot
#                      classify are each a NAMED FAIL, never a vacuous pass. No env
#                      opt-out. Source-only: no cargo, sub-second, offline, no
#                      datasets.
#                      SKIP-aware (loud): SKIPs only when cqlite-core is absent.
#   tooling-tests      shell-tooling regression tests (fast, no datasets/network):
#                      scripts/tests/test_tools_crate_disposition.sh (+ its
#                      selftest) — #1716/AK5: every crate under tools/ must be
#                      EXPLICITLY classified WIRED / UNWIRED / MIXED, and every
#                      crate carrying orphaned targets must carry a README that
#                      STATES it is not CI-wired. Needs NO cargo, python3, Docker
#                      or network — filesystem and lists only, so it always runs
#                      and cannot be environment-dependent. Fails CLOSED on an
#                      absent or unmeasurable subject; never SKIPs.
#                      SCOPE, deliberately small: it verifies a disposition was
#                      RECORDED and LABELED, not that the record is TRUE, and it is
#                      per-CRATE, so an orphaned bin added to a WIRED crate passes
#                      unchanged. A cargo-derived cross-check that DID verify truth
#                      was built and removed (#1716): 11 review findings landed in
#                      it and none in the list/README part, and its self-tests built
#                      scratch workspaces outside the repo that do not inherit
#                      rust-toolchain.toml — making a MANDATORY component depend on
#                      the host toolchain. Verifying truth properly is its own issue
#                      under epic #1688.
#                      scripts/tests/test_agent_gate_summary.sh — proves the
#                      SUMMARY block survives non-foreground capture (#1175). It
#                      only drives `agent-gate.sh --emit-summary-selftest`, which
#                      exits before running any component, so there is no recursion.
#                      SKIP-aware: no python3 -> SKIP (the selftest's truncation
#                      assertion needs a python reader), never silent PASS.
#                      Also runs scripts/tests/test_agent_gate_tree_integrity.sh
#                      (#2926) — proves a gate whose worktree mutates MID-RUN cannot
#                      certify, and that an unmutated run still does (hermetic fake
#                      checkouts + a stub cargo; nothing compiles, ~3s) — and
#                      scripts/tests/test_agent_gate_tree_portability.sh, its BSD/macOS
#                      half (BSD sed/stat/sort shims + a GNU-only-construct lint over an
#                      inventory DERIVED from the gate), and
#                      scripts/tests/test_agent_gate_tree_provenance.sh, its labelling half
#                      (every detection path publishes the same VERIFIED-START/POST-MUTATION
#                      split; the boundary table covers the running mode; the run's own
#                      stdout redirect target is not a mutation).
#                      Also runs scripts/tests/test_generator_keyspace_scoping.sh
#                      (#1232) — fails if a generate-*.sh enumerates the whole
#                      SSTable corpus and grep -z filters by keyspace; needs no
#                      python3 so it runs even on the SKIP path, and any failure
#                      hard-FAILs this component. Also runs (no python3 needed)
#                      scripts/tests/test_udt_rowbuilder_tuple_shape.sh (#1991) —
#                      pins the nb row-builder's UDT value to a positional tuple
#                      (a dict → KeyError: 0 under prepared inserts) + an
#                      actionable 0-row abort. Also runs (no python3 needed)
#                      scripts/tests/test_agent_gate_python_bindings_determinism.sh
#                      (#1803) — proves the python-bindings import-verify + one-shot
#                      self-heal both self-heals a transient venv-resolution miss to
#                      PASS and fails distinctly on a real binding defect (hermetic,
#                      PATH-shadowed toolchain, no real maturin build). On the
#                      python3 path also runs
#                      scripts/tests/test_gate_concurrency_cap.sh (#1825) — proves
#                      the machine-wide full-gate concurrency cap queues at N,
#                      exempts --lite, and releases a slot on SIGKILL (uses the
#                      gate's hermetic stub mode, never real gate work). Also runs
#                      (no python3 needed) scripts/tests/test_gate_cpu_budget.sh
#                      (#2640) — proves the per-gate core budget (CARGO_BUILD_JOBS +
#                      nextest/cargo --test-threads) is derived from the slot count
#                      (full cores when sole gate, fair share max(1, ncpu/N) when
#                      N>1, caller CARGO_BUILD_JOBS respected) and that the gate
#                      wraps itself in taskpolicy -c utility (macOS) / nice (Linux).
#                      Also runs (ruby only — no python3 on any path — and
#                      SKIP-aware when ruby is absent) the three #2910 self-tests
#                      behind "CI green means the relevant CI ran":
#                      test_aggregate_required_tiers.sh (the sibling-tier aggregator
#                      fails closed on a failed/pending/ABSENT registered tier;
#                      hermetic check-run fixtures, injected deadlines, stub sleep),
#                      test_gating_registry_policy.sh (the enrolment rule forces every
#                      pull_request workflow into .github/ci-gating-tiers.yml), and
#                      test_gating_workflow_semantics.sh (the chain between them: what
#                      conclusion a tier's gate job reports under cancellation, and
#                      whether the supersession grace can actually fire — plus a
#                      GNU-only-construct lint over this mechanism's shell). All three
#                      prove non-vacuity with always-pass stubs or mutants.
#                      Also runs (no python3 needed)
#                      scripts/tests/test_check_skill_flag_tables.sh (#3054) — pins
#                      scripts/ci/check-skill-flag-tables.sh, which asserts the
#                      AUTO-LOADED .claude/skills/sstable-parsing/ row/extended/cell
#                      flag tables match the real row_decoder constants (an agent
#                      trusts a skill table over the code, and the pre-#3054 tables
#                      taught partition-boundary mis-detection). Hermetic temp-sandbox
#                      copy; proves non-vacuity by mutating the copy (shifted value,
#                      invented name, dropped row) and fails closed on a moved
#                      decoder source or a reformatted-away table.
#                      Also runs (no python3/sudo/systemd needed)
#                      scripts/tests/test_perf_run_contained.sh (#3068) — pins the
#                      --mem/--swap validation of the perf-corpus containment wrapper
#                      test-data/scripts/perf-run-contained.sh, whose misparsed or
#                      UNBOUNDED cap would mean a hung host rather than a killed
#                      process — and scripts/tests/test_gen_perf_corpus_3068.sh,
#                      which pins the perf-corpus generator's TABLES validation, its
#                      manifest writer's refusal of an empty table list, and the
#                      tight scoping of its multi-GB stale-corpus pruning. Also runs
#                      scripts/tests/test_gen_perf_corpus_bti.sh (#3234), which pins
#                      the BTI (`da`) perf-corpus generator's acceptance asserts in
#                      BOTH directions (a negative control per assert: nb-* descriptor,
#                      empty Rows.db, sub-8-MiB Data.db incl. the exact 8388608 B
#                      boundary, BIG-only TOC entry or file), its row driver's
#                      (seed, chunk-index) determinism, the cassandra.yaml BTI flip
#                      against a COMMITTED cassandra:5.0.2 excerpt (a missed flip
#                      silently emits `nb`), every guard on its multi-GB stale-corpus
#                      pruning, and — through a stub `docker`
#                      (scripts/tests/fixtures/stub-docker-cassandra-bti.py) — a full
#                      hermetic end-to-end run: the manifest writer's happy path plus
#                      both row-count cross-checks FIRING on injected disagreement.
#                      Also runs scripts/tests/test_bti_perf_scan.sh (#3234), the
#                      automated executor for the AC3 warm-scan harness
#                      (cqlite-core/examples/bti_perf_scan/): it builds the example
#                      and asserts its EXIT CODE for every documented failure mode
#                      (usage incl. `--min-seconds nan`, corpus-absent, zero-rows,
#                      row-count mismatch = the silent-truncation guard, sub-floor
#                      window, mid-scan failure, and an unavailable authoritative row
#                      count) against the git-committed 10 KiB `test_da` BTI fixture —
#                      never the multi-GB perf corpus. And
#                      scripts/tests/check-constraint-comments.py --self-test (#3234):
#                      a comment that STATES a constraint must name or sit beside its
#                      enforcement, or the comment goes — the class that hit #3234 three
#                      times (unobserved manifest claims, a stale committed contract, and
#                      a shape check documenting `<table>-<uuid>` while accepting
#                      `<table>-*`). Runs both directions: the surface must pass, an
#                      injected unenforced claim must FAIL.
#                      Also runs (no python3/sudo/perf needed) the PAIR
#                      scripts/tests/test_perf_capability.sh (the helper's unit
#                      contract) and scripts/tests/test_perf_capability_bootstrap.sh
#                      (the bootstrap section end-to-end), sharing
#                      scripts/tests/lib/perf-capability-test-lib.sh (#3249) — they pin
#                      the perf profiling capability path: scripts/perf-capability.sh
#                      (free /proc token, canonical /etc/sysctl.d/99-cqlite-perf.conf
#                      bytes, byte-exact idempotency compare, side-effect-free
#                      sourcing, privilege-drop identity resolution) and bootstrap's
#                      install + VERIFY section. Load-bearing arms: the FUNCTIONAL
#                      check is HONOURED (a shimmed perf that EXITS 0 while reporting
#                      0 / `<not supported>` must WARN, never "verified"), and NO path
#                      reaches a capable/verified verdict from an UNVALIDATED input —
#                      an unusable `id -u`, an inconsistent SUDO_USER, a missing test
#                      sandbox and a non-canonical drop-in all fail closed. Hermetic —
#                      test-only seams stand in for /proc and /etc/sysctl.d (and test
#                      mode REFUSES to fall back to either production directory), every
#                      privileged tool is a recording shim, asserted mutation-free.
#                      Also runs scripts/tests/test_ws0_report_guards.sh (#3096/#3272) —
#                      pins the MEASUREMENT-INTEGRITY guards of the WS0 rig
#                      (scripts/perf/), every one of which was a real "instrument that
#                      reports success without having measured" defect: a cold Flight rep
#                      must be EXACTLY ONE full-corpus request (else requests 2..N are
#                      WARM and blend into a figure labelled "cold"); a warm rep of
#                      EITHER arm must record an untimed prewarm, and the cold arm's
#                      `skipped-cold-arm` sentinel may satisfy a COLD rep ONLY (a warm rep
#                      carrying it is an UNPREWARMED warm measurement passing the very
#                      guard added to refuse one — and the bare scan is the DENOMINATOR of
#                      the 1.3x ratio), while a COLD rep carrying ANY OTHER value —
#                      `unrecorded` (no status file) included — is REFUSED rather than
#                      captioned, because nothing then establishes it was not prewarmed and
#                      a secretly-warm rep reported cold reads FASTER; the corpus identity
#                      is REQUIRED, so the
#                      full-corpus-per-request check can never be silently skipped while
#                      the report's notes claim it ran; an absent, uncounted
#                      (`<not counted>`/`<not supported>`) or unparseable perf counter is
#                      an ERROR, never a fabricated 0 that would make "setup-subtracted" a
#                      lie; --reps/--scan-passes/--port are validated positive (--reps 0
#                      was a vacuous SUCCESS); completeness is judged against the STATED
#                      selection, so an unselected arm is legitimately absent while a
#                      selected-but-absent one stays fatal; durations parse as DECIMAL
#                      (`010s` was octal 8s, `010000ms` snuck under the cold-step ceiling);
#                      and the host sysctls the rig weakens (perf_event_paranoid,
#                      kptr_restrict) are captured BEFORE mutation and RESTORED on
#                      EXIT/INT/TERM/HUP. A broken instrument publishes a wrong number
#                      rather than crashing, so these need a standing test — and per #3249
#                      (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) the bar is
#                      "OBSERVED TO FIRE", not "present": every case feeds rejectable input
#                      and asserts the exit code AND the diagnostic, with the sysctl
#                      restore checked BEHAVIOURALLY through a recording `sudo` shim plus a
#                      real SIGINT probe on the driver's own trap wiring. Hermetic:
#                      synthetic result dirs + synthetic perf CSVs, no
#                      cargo/perf/sudo/corpus/network, never root.
#                      Also runs scripts/tests/test_ws0_clock_guards.sh (#3248) — the
#                      occupancy-enforced clock derivation (scripts/perf/ws0_clock.py).
#                      16 cases; exists because #3299 published cycles/task-clock as a
#                      frequency, retracted it, then made the same error again, so the
#                      control had to become a tool that REFUSES rather than a caption.
#                      Also runs scripts/tests/test_ws0_cpu_pinning_guards.sh (#3272
#                      item 10) — the MEASUREMENT-APPARATUS half of the same rig, split
#                      out because it asks whether the observations are of the RIGHT
#                      THING (the reporter test asks what the rig does with them) and
#                      uses disjoint fixtures. Two guards, both directions each: the
#                      VERIFIED-SIBLING taskset check (driven over a FAKE 4-core/8-thread
#                      sysfs tree via lib-cpu.sh's injectable topology root — it must
#                      ACCEPT genuine sibling pairs and REFUSE two-different-cores, a
#                      pair valid on another box's layout, a pair-plus-stray, a lone CPU,
#                      a cross-core range, an empty spec and an unreadable entry; the
#                      override itself fails closed in a measurement run, refusal asserted
#                      to PRECEDE the pinning check, so it can never be the bypass), and
#                      the SERVER-OWNERSHIP check — the same question about the right
#                      PROGRAM, driven against the shipped lib-server.sh with a real
#                      listener on a kernel-assigned port: a FOREIGN listener refused
#                      naming its pid, our own accepted, a descendant accepted but a
#                      SIBLING refused (the pgid check accepted those), a dead server and
#                      a readiness TIMEOUT fatal, an unanswerable prober stopping the run.
#                      Hermetic: fake sysfs + a loopback listener under $TMPDIR; no
#                      perf/sudo/taskset/root/hardware.
#                      Also runs scripts/tests/test_ws0_perf_invocation_lint.sh (#3272
#                      item 10) — the THIRD structural guard, split out of the file above
#                      under the campsite rule (it reached 1607 lines against the ~1500
#                      test target) along a RESPONSIBILITY seam: that one asks which CPUs
#                      and which PROGRAM, this one asks whether the COUNTING DOMAIN is the
#                      one spec R2 mandates. Measured at the split the two shared NO helper
#                      and NO fixture. The `perf stat -p` SELF-GREP's FIVE REAL BYPASSES
#                      were all found by driving it: an ATTACHED `-p<pid>` (the pattern
#                      needed a trailing space), ANY line mentioning "self-check" (the
#                      `grep -v` discarded by CONTENT, so a comment suppressed the guard),
#                      a SINGLE-QUOTED attached value, an invocation through a VARIABLE,
#                      and a GLOBAL OPTION between `perf` and `stat`. Hence the mechanism
#                      is an ALLOWLIST (one wrapper invokes perf; any other invocation line
#                      must be marked) + a per-TOKEN option check + a RUNTIME argv check,
#                      asking WHERE a line is rather than what it looks like. Subject is
#                      the whole scripts/perf tree, DISCOVERED by glob and asserted against
#                      `ls` (a hand-maintained list had already drifted past two libs).
#                      Both directions, plus the lint's OWN vacuity states: an empty
#                      subject, an absent/doubled/`-C`-less/empty wrapper, a variable
#                      command word, an UNREADABLE rig file, a mode with no END assertions,
#                      an awk dying under the driver's `set -e -o pipefail`, and the
#                      false-positive direction (`perf_stat_c`/`perf_event_paranoid`/
#                      `target/perf-…` identifiers must NOT flag). Hermetic: driver +
#                      scripts/perf copies under $TMPDIR and a shimmed `perf` function.
#                      Also runs scripts/tests/test_ws0_host_state_guards.sh (#3272
#                      finding 3) — the HOST STATE half, split out of the reporter test
#                      in review round 3 because it is the only part of the rig that
#                      changes anything OUTSIDE its own process tree and the only part
#                      whose failure is SECURITY-ADJACENT rather than a wrong number.
#                      The rig weakens perf_event_paranoid + kptr_restrict and used to
#                      NEVER restore them (its only trap was `trap stop_server EXIT`);
#                      the first fix was itself PARTIAL (the success/warning split keyed
#                      on 'was ANYTHING restored'), so both halves are per-knob and the
#                      root cause is closed too — a knob whose prior could not be
#                      CAPTURED is never MUTATED. Behavioural through a recording `sudo`
#                      shim + a real SIGINT probe; no privileged call, no knob touched.
#                      Also runs scripts/tests/test_ws0_hermeticity.sh (#3272 review round
#                      3, B1) — the MECHANISM that keeps the three files above hermetic ON
#                      LINUX, which is where the property matters and where it broke twice.
#                      The WS0 driver has an argument-validation boundary; BELOW it, it
#                      writes host sysctls via `sudo -n`, runs `cargo build --release`,
#                      drops the page cache and takes 45s `perf stat` measurements. Round 1
#                      ran the world from six accept call sites; round 2 added
#                      `--validate-args-only` + recording shims and left ONE bare (the
#                      cold-ceiling `--temp warm` case, which skips the ceiling and falls
#                      straight past the boundary) — a MANUAL SWEEP MISSED IT TWICE. So the
#                      contract is a STRUCTURAL LINT over every `test_ws0_*.sh` (subject
#                      DISCOVERED by glob; an empty subject or an unreadable file is a
#                      FINDING, not a clean tree), which flags any driver invocation not
#                      routed through `ws0_driver_run`, by LOCATION rather than spelling.
#                      Its discriminating power is MEASURED: six bare spellings must fire
#                      ($DRIVER, ${DRIVER}, a literal path, a $copy, a PATH-prefixed call,
#                      `sh`) and six ordinary lines must not. And the platform property is
#                      OBSERVED end to end on a LINUX-SHAPED fixture (fake sysfs where the
#                      default 2,10 really are siblings, readable non-`-1` sysctl priors,
#                      recording shims): a POSITIVE CONTROL first proves the bare run DOES
#                      write `kernel.perf_event_paranoid=-1` on it, then the same fixture
#                      through `ws0_driver_run` leaves the recording file EMPTY and the
#                      priors UNCHANGED. Hermetic everywhere; a check-count floor closes
#                      the suite-level 0/0.
#                      Also runs `cargo test -p ws0-corpus-gen` (#3272 items 8-9) — a
#                      tools/* package NO other component and no CI lane compiles, so
#                      without this hook the corpus determinism oracle would be a test
#                      nothing executes (#1597/#1618 gate-wiring class). It REGENERATES
#                      the corpus (two, then three, 1,000-row generations, ~0.3s) and
#                      BYTE-COMPARES every emitted component off disk; corroborates the
#                      generator's self-reported sha256 against an independent hash
#                      (anti-circularity); proves the comparison can FAIL (different seed
#                      diverges, a one-byte flip is reported at its offset, a
#                      missing/extra component is reported); and asserts the in-source
#                      measurement-corpus pin (4,000,000 rows / 40,000 partitions /
#                      693.69 B/row / sha256 4a903f6f… / digest 0x0390bfbb81a23fa1 over
#                      31,250 batches) equals the committed corpus-identity.json field for
#                      field, with a per-field perturbation case proving THAT comparison
#                      can fail. Measured non-vacuity: a wall-clock write timestamp, a
#                      per-generation buffer-reuse tail, and a fabricated self-reported
#                      digest each left all 34 pre-existing unit tests GREEN while these
#                      FAIL. Hermetic: tempdir corpora, no datasets/network.
#                      Also runs (no python3/network/datasets needed)
#                      scripts/tests/test_fetch_datasets_tracked_guard.sh (#2878) —
#                      pins fetch-datasets.sh's tracked-fixture guard: its `rm -rf
#                      "${DATASET_ROOT}"` used to DELETE the ~875 git-tracked files
#                      under test-data/datasets (JSONL goldens, force-added parity
#                      *.db, the #2389 commitlog fixtures) because the restore path
#                      was CI-gated off locally and silently prefix-bailed in CI,
#                      red-ing the gate on a pristine main. Also pins the crash-safe
#                      abort window (an abort between the rm -rf and the restore —
#                      bad archive, tar failure, SIGINT — must still restore) and the
#                      refusals that keep restoration possible at all: a target that
#                      IS a repo root (plain OR bare — a bare repo has no .git, and
#                      was silently rm -rf'd with exit 0), contains a nested
#                      checkout/mirror, is an ancestor of the work tree, or has
#                      UNMERGED index entries (git restore cannot rebuild a
#                      conflicted path), or sits at/beneath git's ADMIN storage
#                      (.git/…, mirror.git/objects/…, a linked worktree's admin dir
#                      — deleting it destroys the object store the restore reads
#                      FROM). Those are STRUCTURAL and no env var unlocks them;
#                      CQLITE_DATASETS_ALLOW_UNPROTECTED unlocks only the
#                      guard-availability class. Also pins the GIT_* environment
#                      scrub: a VALID but FOREIGN GIT_INDEX_FILE made the capture
#                      read the wrong index (0 files), after which restore AND
#                      verification both short-circuited on the empty list and the
#                      run deleted fixtures while reporting success — so the test
#                      asserts the captured COUNT, not just a clean tree. The
#                      converse arm is the self-verifying readability precheck: the
#                      capture reads only the INDEX, so before deleting anything the
#                      guard must PROVE every captured blob is readable in the
#                      restore's own scrubbed environment (external/alternate/
#                      quarantine object stores are otherwise invisible to it).
#                      Two more silent-loss paths are pinned: the guard's own capture
#                      list must live in a location PROVEN outside the deletion target
#                      (a TMPDIR at/below it meant the rm -rf ate the list, which then
#                      read as "nothing to restore"), and an absent list with a
#                      nonzero captured count is a hard error, never a no-op; plus
#                      any index entry `git diff` cannot see (skip-worktree tag `S`
#                      or ANY lowercase = assume-unchanged; `ls-files -t` hides the
#                      both-flags case as a plain `H`) is refused up front, because
#                      the integrity check would otherwise be blind; and an
#                      INCOMPLETE nested-repository scan (find failing) must fail
#                      closed instead of reading as "no nested repo".
#                      Hermetic: throwaway git repos +
#                      locally-built partial-overlap tarball + stub curl/tar/git, so
#                      the real rm -rf/extract/restore run against a sandbox and
#                      never the checkout's datasets; both signal arms are
#                      deterministic (no sleeps), and the case that hands `/` to the
#                      script shadows `rm` so its blast radius does not depend on the
#                      checks under test. Proves non-vacuity with nine mutants (guard
#                      disabled; abort-restore disabled; signals left live during
#                      cleanup; GIT_* scrub removed; readability precheck removed;
#                      partial-extraction discard removed; guard state back under
#                      TMPDIR with no consistency check; exact-`S` index-flag match;
#                      failed nested scan read as clean).
#   flight-tests       cargo test -p cqlite-flight --lib --bins (UNIT tests only).
#                      Issue #1699: before this, the gate COMPILED cqlite-flight
#                      (clippy --all-targets) and RAN only three of its ~44 targets by
#                      name (two in flight-query-semantics-oracle, one dhat target in
#                      memory-budget) — so a Flight regression elsewhere was found only
#                      after a push, on CI. NARROWED from the whole package by #3384:
#                      that package's INTEGRATION suite is ~50% non-deterministic under
#                      intra-package parallelism (4 runs PASS/FAIL/PASS/FAIL, 2 distinct
#                      victims), and a lane that reds 1-in-2 carries no information. The
#                      lane therefore PRINTS A COVERAGE CENSUS on every run naming the
#                      integration targets it does NOT execute, the CI Flight tier that
#                      does, and #3384/#3383 — an omission stated is the opposite of the
#                      silent omission #1699 exists to eliminate. Runs under a
#                      unit-scoped zero-tests guard; no opt-out.
#   legacy-heuristics  BUILD cqlite-core at `default + legacy-heuristics` under
#                      -D warnings, then EXECUTE the tests that feature turns on
#                      (issue #1699). clippy already test-compiles those bodies inside a
#                      ~30-feature union, so the two things only this lane does are the
#                      MINIMAL feature set and EXECUTION: an inverted assertion in a
#                      positively-gated test body FAILs here while clippy stays green.
#                      The --test target set is DERIVED from the committed
#                      cqlite-core/tests/*.rs (never hard-coded, so a sixth gated file
#                      needs no gate edit) and the derivation is FAIL-CLOSED — zero
#                      derived targets is a FAIL naming the derivation, never a PASS or
#                      a SKIP. Runs under the zero-tests guard; no opt-out.
#   feature-iso-parquet / feature-iso-delta-scan
#                      RUSTFLAGS=-D warnings cargo test -p cqlite-core
#                      --no-default-features --features all-compression,<one-of>
#                      --lib --no-run, each WITHOUT the other feature (issue #1699).
#                      clippy enables parquet AND delta-scan together, which is the
#                      shape that MASKS cross-feature coupling; these two lanes are
#                      separately named so a SUMMARY FAIL says WHICH direction of
#                      coupling broke. `--lib --no-run` is load-bearing and is the
#                      minimal-build shape: it compiles the lib WITH its inline
#                      `#[cfg(test)]` modules — where the #1978 incident class lives,
#                      invisible to a bare `cargo check` — while pulling in none of the
#                      ~100 integration test files, which assume default features and
#                      fail here as noise, not leakage. No opt-out.
#                      Also runs scripts/tests/test_pub_surface_guard.sh (#1712),
#                      the non-vacuity proof for the pub-surface component. 42 cases,
#                      source-only (no cargo doc since the #1712 descope), each
#                      negative case substituting the artifact in its own detached
#                      scratch worktree — the guard has no test-only seam. The
#                      INHERITED declaration half: the consistency assert must RED on
#                      the pre-#1712 source shape (a bare ungated
#                      `pub mod benchmarks;` whose cfg gate hides inside the module
#                      file), a cosmetic `cfg_attr` must not exempt a crate-root
#                      `pub mod`, a tell-tale token inside an attribute's STRING VALUE
#                      must not either, a SAME-LINE `#[attr] pub mod x;` must be seen
#                      (the first cut dropped it — a false PASS), the two independent
#                      crate-root scans disagreeing must REFUSE, and the three SHARED
#                      blind spots must refuse rather than agree-while-blind (inline
#                      `pub mod x { … }`, an INDENTED depth-0 declaration, and
#                      `*/ pub mod x;` — code after a closing delimiter). The NEW
#                      module-file oracle half: every refusal path (module file
#                      resolving to neither/both legal paths, not a readable regular
#                      file, a block comment in the prologue, an inner attribute that
#                      merely mentions `cfg`, content after an inner attribute on one
#                      line, an unterminated inner attribute) plus green positive
#                      controls, without which a guard hardwired to refuse everything
#                      would satisfy them all. And case 26: THIS component must not
#                      report PASS on a guard that exited 0 having measured nothing.
#   minimal-build      cargo build + `cargo test --lib --no-run` (compile-only)
#                      -p cqlite-core --no-default-features --features all-compression
#   smoke              bash test-data/scripts/smoke-test-all-tables.sh
#   file-size          campsite-rule ratchet (epic #1116 / #1135): lists changed
#                      .rs files over threshold (800 src / 1500 test, total lines)
#                      and FAILs if a change makes an over-threshold file LARGER.
#                      Override an unavoidable growth with CQLITE_ALLOW_FILE_GROWTH=1.
#
# The integration-tests --no-run sweep, the format-compat component, and the
# python-bindings component close the three blind spots from issue #865: a
# compile break in a non-enumerated test target, a fmt/compile break in the
# (previously workspace-excluded) format-compatibility crate, and Python-only
# regressions (LIMIT 0, SET<TEXT> validation) that shipped "gate PASS".
#
# All components run even after a failure so one run reports everything.
# Exit code 0 iff every component passes. Machine-checkable output: the
# summary block between the AGENT-GATE SUMMARY markers, carrying a per-run
# "run-id:" line and ending in "RESULT: PASS" or "RESULT: FAIL".
#
# Usage:
#   scripts/agent-gate.sh             # full gate (the only run that counts)
#   scripts/agent-gate.sh --lite      # FAST ITERATION gate (issue #1821): runs
#                                     # ONLY file-size + fmt + scoped workspace clippy
#                                     # (-D warnings; same #1844 duckdb/otel-excluded
#                                     # scoping as the full gate) + BLAST-RADIUS-SCOPED tests
#                                     # (the touched package's --lib + the diff's
#                                     # new --test targets; NOT core-tests/write/
#                                     # cli/bindings/parity/smoke). A cqlite-core
#                                     # SRC diff also `cargo test --no-run`
#                                     # compile-checks every dependent test crate
#                                     # (issue #2658). ~1-5 min vs
#                                     # 12-25 min. It is NOT the gate of record and
#                                     # emits a DISTINCT "==== AGENT-GATE LITE
#                                     # SUMMARY ====" block (MODE: lite) so it can
#                                     # never be pasted as the full SUMMARY. The
#                                     # full gate MUST PASS once before merge. Its
#                                     # recovery default is .agent-gate-lite-summary.txt.
#                                     # A bindings/python diff routes scoped-tests to
#                                     # maturin develop + fast pytest (issue #1893), so
#                                     # python-diff rounds cost a maturin compile
#                                     # (seconds warm, ~1-3 min cold).
#   scripts/agent-gate.sh --delta <anchor> [--anchor-run-id <id>]
#                                    [--anchor-summary-file <path>]
#                                     # TEST/DOCS-ONLY RE-CERTIFICATION (issue #1892):
#                                     # after a full-gate PASS at <anchor>, re-certify
#                                     # a diff anchor..HEAD that touches ONLY what the
#                                     # re-cert can EXECUTE: rust cargo tests (.rs under
#                                     # tests/ dirs, *_test(s).rs), python binding tests
#                                     # (bindings/python/tests/ — run by the #1893 python
#                                     # tier), and/or docs (*.md anywhere; TOP-LEVEL
#                                     # docs/, website/). FAIL-CLOSED: anything else in
#                                     # the diff REFUSES the re-cert (run the full gate)
#                                     # — incl. node __test__/ and scripts/tests/*.sh,
#                                     # which --delta's components never execute.
#                                     # On pass it runs ONLY file-size + fmt + the diff's
#                                     # changed test targets and emits a DISTINCT
#                                     # "==== AGENT-GATE DELTA SUMMARY ====" block
#                                     # (MODE: delta) that names the gate of record
#                                     # (the full PASS at <anchor>) + the anchor run-id,
#                                     # so it can NEVER be pasted as a full SUMMARY.
#                                     # It is NOT the gate of record; any production
#                                     # change needs a fresh full gate. The nightly
#                                     # gate.yml deep-check is the standing backstop.
#                                     # Record BOTH the anchor's full SUMMARY and this
#                                     # DELTA block in the PR. Recovery default:
#                                     # .agent-gate-delta-summary.txt.
#   scripts/agent-gate.sh --list      # list full-gate components without running
#   scripts/agent-gate.sh --lite-list # list the --lite components without running
#   scripts/agent-gate.sh --delta-list # list the --delta components without running
#   scripts/agent-gate.sh --only fmt,clippy   # debugging aid; output is
#                                     # marked PARTIAL and never counts as the gate
#   scripts/agent-gate.sh --emit-summary-selftest
#                                     # print a representative SUMMARY block
#                                     # through the real emission path (fast, for
#                                     # regression tests — see scripts/tests/) and
#                                     # exit 0; never runs any gate component.
#
# Capturing the gate (issue #1175): the authoritative artifact is the block
# between the AGENT-GATE SUMMARY markers. Under non-foreground capture (a
# `script`/pty, a buffering wrapper, a "drain-until-EOF then write" reader, or a
# backgrounded pipeline) that streamed block can be lost if a gate component
# leaks a descendant that keeps the gate's stdout pipe open: the reader never
# sees EOF, gets killed by a timeout, and discards its in-memory buffer — even
# though the gate exited 0. (Detaching the gate's OWN stdout cannot fix this: a
# leaked child still holds its inherited copy of the pipe write-end open.)
#
# The defense is therefore a recovery path the caller can use WITHOUT reading the
# (possibly-lost) stream:
#   - The gate always writes the complete SUMMARY to a CALLER-KNOWN file whose
#     path the caller chose IN ADVANCE: $AGENT_GATE_SUMMARY_FILE if set, else the
#     stable repo-root default $PWD/.agent-gate-summary.txt (gitignored). A caller
#     can ALWAYS `cat` that file for the complete block even if stdout was 100%
#     lost — no need to parse the stream to learn where the file is. A RELATIVE
#     $AGENT_GATE_SUMMARY_FILE resolves against the caller's CURRENT directory
#     (the gate captures it before it cd's to the repo root); an ABSOLUTE path is
#     used verbatim.
#   - That file is INVALIDATED at startup with a "RESULT: INCOMPLETE" sentinel
#     stamped with this run's run-id, so a stale prior-run summary can never be
#     read as this run's result if the gate exits early or can't write (#1175).
#     Each SUMMARY block carries a "run-id:" line; the recovery file is trusted
#     only when it bears THIS run's run-id, defeating a stale-but-complete file.
#   - It also keeps a copy under $LOG_DIR for the logs bundle.
#
# CONCURRENCY (#1175 roborev): the default $PWD/.agent-gate-summary.txt is
# per-CHECKOUT, not per-run. If you run multiple gates concurrently IN THE SAME
# CHECKOUT, each MUST set a unique $AGENT_GATE_SUMMARY_FILE or they will clobber
# each other's recovery artifact. Separate worktrees are already isolated (each
# has a distinct repo root → a distinct default path); CQLite's normal model
# runs concurrent gates in separate worktrees, so this is a non-issue there. The
# `run-id:` line lets a caller that captured the invocation's run-id confirm it
# is reading the right run; a caller with NO expected run-id whose stream was
# lost cannot disambiguate two same-checkout runs and so MUST use a unique path.
# The streamed copy is best-effort (a plain `cat` of the file). The most robust
# streamed capture is still the foreground redirect:
#   bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
# but if that stream truncates, read the caller-known file — it is always complete.
set -uo pipefail

# Capture the caller's invocation CWD BEFORE we cd to the repo root (#1175
# roborev finding 1). A caller-provided RELATIVE AGENT_GATE_SUMMARY_FILE must
# resolve against the directory the caller ran us from — otherwise the caller
# reads ./gate.summary in its own CWD while the gate wrote <repo>/gate.summary,
# breaking the recovery contract. We resolve the relative path against this
# captured CWD just below, before any further directory change.
INVOCATION_CWD="$PWD"

# ---- Per-gate CPU utility wrapping (issue #2640) ----------------------------
# Wrap the ENTIRE gate process in the OS "run at reduced priority / utility QoS"
# tool so a gate never CPU-oversubscribes a box it shares with other gates or
# interactive work: `taskpolicy -c utility` on macOS (clamps the whole tree to
# the background/utility QoS tier) and `nice` on Linux. This is the CPU-priority
# half of #2640; CARGO_BUILD_JOBS + nextest test-threads (the core-count half)
# are derived from the machine-wide slot count further below.
#
# Mechanism: re-exec THIS script ONCE under the wrapper (guarded by
# AGENT_GATE_WRAPPED so the re-exec'd copy never loops), before any work — that
# way the QoS/priority clamp is inherited by every heavy child (cargo, nextest,
# rustc, python, node). It happens BEFORE `cd`, so the relative "$0" still
# resolves against the caller's CWD, and BEFORE INVOCATION_CWD/args matter (both
# are recomputed identically in the re-exec'd copy). Degrades gracefully to an
# UNWRAPPED run when the tool is absent (no wrapper var set). Escape hatch:
# CQLITE_GATE_NO_NICE=1 disables wrapping entirely. AGENT_GATE_WRAPPER records
# which wrapper (if any) is in effect, for the SUMMARY cpu-budget line + the
# --cpu-budget self-test hook.
if [ "${AGENT_GATE_WRAPPED:-0}" != 1 ] && [ "${CQLITE_GATE_NO_NICE:-0}" != 1 ]; then
  _gate_wrapper=""
  case "$(uname -s 2>/dev/null || echo unknown)" in
    Darwin) command -v taskpolicy >/dev/null 2>&1 && _gate_wrapper="taskpolicy -c utility" ;;
    Linux)  command -v nice       >/dev/null 2>&1 && _gate_wrapper="nice -n ${CQLITE_GATE_NICE:-10}" ;;
  esac
  if [ -n "$_gate_wrapper" ]; then
    export AGENT_GATE_WRAPPED=1 AGENT_GATE_WRAPPER="$_gate_wrapper"
    # shellcheck disable=SC2086  # intentional word-split of "<tool> <flags>"
    exec $_gate_wrapper "${BASH:-bash}" "$0" "$@"
  fi
fi

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

# Absolute path to THIS script, for the hidden self-test hooks that re-invoke it
# in a fresh subshell (e.g. --python-build-verify, issue #1803). It always lives
# under <repo-root>/scripts/ (we just cd'd out of scripts/ via dirname "$0"/..).
GATE_SELF="$REPO_ROOT/scripts/$(basename "$0")"

# Agent sandboxes often run with a minimal PATH; pick up rustup's cargo.
if ! command -v cargo >/dev/null 2>&1 && [ -d "$HOME/.cargo/bin" ]; then
  export PATH="$HOME/.cargo/bin:$PATH"
fi

# sccache auto-detect (issue #1822): if sccache is available, use it as the
# rustc wrapper for incremental compilation cache. Each worktree keeps its own
# target/ dir (no lock contention); the shared object cache deduplicates
# compilation across worktrees. Disabled via CQLITE_DISABLE_SCCACHE=1.
# Cache location: $SCCACHE_DIR (default ~/.cache/sccache on Linux,
# ~/Library/Caches/Mozilla.sccache on macOS). Cache size limit:
# $SCCACHE_CACHE_SIZE (default 10 GiB; raise for multi-user builds).
# Measurement (issue #1822): 25.6% speedup on fresh worktrees with warm cache.
#
# Accelerator state (issue #1848): every optional accelerator the gate depends on
# records a state in ACCEL_* — `on` (detected & used), `absent` (NOT installed →
# a LOUD WARN with the one-line install command, so a machine is never silently
# 3x slower again), or `off` (intentionally disabled via CQLITE_DISABLE_*; no
# WARN). The states are stamped into a machine-checkable `accelerators:` line in
# the SUMMARY block so degradation is visible in the pasted block, not just
# scrollback. All WARN/banner text goes to STDERR: hidden hook modes (--classify-*)
# must keep STDOUT empty, and this detection runs before the hook dispatch.
ACCEL_SCCACHE=absent
if [ "${CQLITE_DISABLE_SCCACHE:-0}" = 1 ]; then
  ACCEL_SCCACHE=off
elif command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER=sccache
  export CARGO_INCREMENTAL=0
  ACCEL_SCCACHE=on
  echo "agent-gate: sccache detected; using as RUSTC_WRAPPER with CARGO_INCREMENTAL=0 (#1822)" >&2
else
  echo "agent-gate: WARN: sccache not installed — cross-worktree compile caching DISABLED (~25.6% slower fresh builds); install: brew install sccache (#1848)" >&2
fi
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"

# cargo-nextest auto-detect (issue #1737): the core-tests execution floor (the
# gate's single dominant cost) runs under `cargo nextest run`, which parallelizes
# across test binaries + cores (typically 2-4x vs serial `cargo test`).
# Auto-detected like sccache: absent on PATH -> the gate falls back to plain
# `cargo test` (identical test set, incl. doctests). Opt out with
# CQLITE_DISABLE_NEXTEST=1. nextest does NOT run doctests, so the nextest path
# additionally runs `cargo test --doc` so doctest coverage is never silently
# dropped (same package/feature selection + same skip).
NEXTEST=0
ACCEL_NEXTEST=absent
if [ "${CQLITE_DISABLE_NEXTEST:-0}" = 1 ]; then
  ACCEL_NEXTEST=off
elif command -v cargo-nextest >/dev/null 2>&1; then
  NEXTEST=1
  ACCEL_NEXTEST=on
  # Banner on STDERR: hidden hook modes (--classify-*, --scoped-noparser-fail-msg)
  # must keep STDOUT empty, and this detection runs before the hook dispatch (same
  # rule the sccache banner above already follows; #1821/#1825).
  echo "agent-gate: cargo-nextest detected ($(cargo nextest --version 2>/dev/null | head -1)); core-tests uses nextest + a cargo test --doc pass (#1737)" >&2
else
  # #1848: absent accelerator → LOUD WARN + one-line install command (STDERR).
  echo "agent-gate: WARN: cargo-nextest not installed — core-tests fall back to serial 'cargo test' (much slower long pole); install: brew install cargo-nextest (#1848)" >&2
fi

# Bounded component parallelism (issue #1737): independent gate components run
# concurrently in a worker pool capped at AGENT_GATE_JOBS, collapsing wall-clock
# toward the core-tests long pole WITHOUT oversubscribing the machine. Multiple
# worktree gates can run at once (and aarch64 emulation raises OOM risk), so this
# per-gate cap composes with the machine-wide bound of #1825. Default:
# min(4, ncpu/2), floor 1. Set AGENT_GATE_JOBS=1 for the legacy strictly
# sequential behavior. Concurrency is corruption-safe: cargo serializes builds on
# the shared target dir via its own advisory lock, sccache dedups the recompiles,
# datasets are read-only, and each component captures its own log + verdict to a
# file (see record_result) so interleaved stdout can never corrupt the
# deterministic end-of-run SUMMARY block.
# AGENT_GATE_TEST_NCPU overrides the detected core count (issue #2640 self-test):
# lets scripts/tests pin ncpu so the CARGO_BUILD_JOBS + test-threads derivation is
# asserted deterministically across machines. Ignored (detection used) when unset.
_ncpu="${AGENT_GATE_TEST_NCPU:-$( { command -v nproc >/dev/null 2>&1 && nproc; } || sysctl -n hw.ncpu 2>/dev/null || echo 4 )}"
case "$_ncpu" in *[!0-9]*|'') _ncpu=4 ;; esac
_default_jobs=$(( _ncpu / 2 ))
[ "$_default_jobs" -gt 4 ] && _default_jobs=4
[ "$_default_jobs" -lt 1 ] && _default_jobs=1
AGENT_GATE_JOBS="${AGENT_GATE_JOBS:-$_default_jobs}"
case "$AGENT_GATE_JOBS" in *[!0-9]*|'') AGENT_GATE_JOBS=1 ;; esac
[ "$AGENT_GATE_JOBS" -lt 1 ] && AGENT_GATE_JOBS=1
# The bounded pool relies on `wait -n` (bash 4.3+). On older bash (e.g. macOS's
# stock /bin/bash 3.2) fall back to sequential execution rather than risk a
# busy-poll race; correctness is identical, only wall-clock differs.
#
# #1848: lanes are a gate accelerator too. lanes=on (parallel), lanes=serial
# (degraded by bash <4.3 → LOUD WARN + install command), or lanes=off (component
# parallelism intentionally not in play, e.g. AGENT_GATE_JOBS=1 or a low core
# count; no WARN).
ACCEL_LANES=off
if [ "$AGENT_GATE_JOBS" -gt 1 ]; then
  if [ "${BASH_VERSINFO[0]:-0}" -gt 4 ] || \
     { [ "${BASH_VERSINFO[0]:-0}" -eq 4 ] && [ "${BASH_VERSINFO[1]:-0}" -ge 3 ]; }; then
    ACCEL_LANES=on
  else
    # Banner on STDERR (see nextest note above): hidden hook modes must keep STDOUT
    # empty, and this runs before the hook dispatch — under stock bash 3.2 this
    # branch is always taken, so an stdout banner here corrupted --classify-* output.
    echo "agent-gate: WARN: bash <4.3 lacks 'wait -n' — gate components run SERIALLY (no parallel lanes; AGENT_GATE_JOBS=1); install: brew install bash (#1848)" >&2
    AGENT_GATE_JOBS=1
    ACCEL_LANES=serial
  fi
fi

# ---- sccache cache-health signal (issue #2641) ------------------------------
# Characterization (issue #2641) of the single reported "sccache served corrupted
# objects under load (loadavg ~150)" incident found NO evidence of a load→corruption
# mechanism: across 31k requests on a sustained-high-load gate machine, sccache's
# OWN authoritative error counters (read/write/errors/timeouts) were all ZERO, the
# eviction-capped cache held zero torn/zero-byte objects, and the disk had ample
# free space (not a disk-full artifact). Blindly auto-disabling caching under load
# is therefore NOT justified — it would forfeit the measured 25.6% build speedup
# and INCREASE build pressure on the loaded machines that can least afford it, to
# defend an unreproduced failure mode.
#
# What the incident DID expose is that sccache's error counters — the one signal
# that WOULD catch real corruption — were invisible in the gate SUMMARY. So the
# evidence-based mitigation is monitoring, not auto-disable: probe those counters
# and surface a cache-health token (na|ok|warn) on the accelerators: line, with a
# LOUD WARN when any counter is non-zero, WITHOUT disabling caching or failing the
# gate. If a future *reproduced* incident correlates errors with load, the per-gate
# counters are now recorded to drive that call on evidence.
#
# States: na (sccache not in use → nothing to probe), ok (all counters 0), warn
# (any error/timeout counter > 0). Memoized and probed ONLY at SUMMARY emission
# (accelerators_line); the latency-sensitive classify hooks exit before reaching it.

# _sccache_error_sum: sum sccache's authoritative failure counters from
# `sccache --show-stats`. Robust text parse (no jq/python dependency). 0 if sccache
# is unavailable or stats can't be read (absence is not a health failure).
_sccache_error_sum() {
  command -v sccache >/dev/null 2>&1 || { printf 0; return; }
  sccache --show-stats 2>/dev/null | awk '
    /^Cache read errors/  { s += $NF }
    /^Cache write errors/ { s += $NF }
    /^Cache errors/       { s += $NF }
    /^Cache timeouts/     { s += $NF }
    END { print s + 0 }' 2>/dev/null || printf 0
}

# _sccache_health: resolve the cache-health state (na|ok|warn), memoized, emitting
# the LOUD WARN exactly once when warn. Test hooks (self-test, issue #2641):
# AGENT_GATE_TEST_SCCACHE_STATE overrides the detected ACCEL_SCCACHE state and
# AGENT_GATE_TEST_SCCACHE_ERRORS forces the error sum — so the na/ok/warn decision
# is asserted deterministically without sccache installed or PATH surgery.
_SCCACHE_HEALTH=""
_sccache_health() {
  [ -n "$_SCCACHE_HEALTH" ] && { printf '%s' "$_SCCACHE_HEALTH"; return; }
  local state="${AGENT_GATE_TEST_SCCACHE_STATE:-${ACCEL_SCCACHE:-absent}}"
  if [ "$state" != on ]; then
    _SCCACHE_HEALTH=na
    printf '%s' "$_SCCACHE_HEALTH"
    return
  fi
  local errsum
  if [ -n "${AGENT_GATE_TEST_SCCACHE_ERRORS:-}" ]; then
    errsum="$AGENT_GATE_TEST_SCCACHE_ERRORS"
  else
    errsum=$(_sccache_error_sum)
  fi
  case "$errsum" in *[!0-9]*|'') errsum=0 ;; esac
  if [ "$errsum" -gt 0 ]; then
    _SCCACHE_HEALTH=warn
    echo "agent-gate: WARN: sccache reports ${errsum} cache read/write/error/timeout event(s) — possible corrupted or torn cache object(s); inspect 'sccache --show-stats' (caching left ENABLED — see the #2641 characterization; auto-disable is NOT evidence-supported)" >&2
  else
    _SCCACHE_HEALTH=ok
  fi
  printf '%s' "$_SCCACHE_HEALTH"
}

# ---- mold link-accelerator state (issue #2859) ------------------------------
# On Linux agent workers the link step is the one build cost sccache cannot cache
# (every --lite round and full gate re-links every test binary from scratch), so
# bootstrap-agent-machine.sh provisions the mold linker and wires it through a
# managed block in the per-machine ~/.cargo/config.toml. The gate surfaces that
# state on the accelerators: line so an installed-but-unwired worker (silent
# degradation) is visible in the pasted block — exactly the contract sccache
# follows. Four states, Linux hosts ONLY:
#   linked                — mold on PATH AND the managed block is active in the
#                           resolved cargo config (bootstrap wired it)
#   overridden            — a non-empty RUSTFLAGS is exported in the gate
#                           environment: env RUSTFLAGS SUPPRESSES cargo's
#                           target.rustflags entirely, so the managed block's
#                           -fuse-ld=mold is NOT applied and a bare `linked` would
#                           LIE. This is the exact footgun the token exists to
#                           surface (never export global RUSTFLAGS on a worker).
#   present-unconfigured  — mold on PATH but no managed block (bootstrap not re-run)
#   absent                — mold not on PATH
# Darwin (and any non-Linux host) emits NO mold token: mold is Linux-only and a
# permanent n/a token would churn every existing summary parser/fixture for zero
# signal. Test hooks (issue #2859 self-test): AGENT_GATE_TEST_OS forces the host
# family and AGENT_GATE_TEST_MOLD_STATE forces the detected state, so the four
# states (and the Darwin no-token case) assert deterministically without mold
# installed or a real ~/.cargo/config.toml.

# The EXACT managed-block begin marker bootstrap-agent-machine.sh writes. Match the
# full line (grep -Fxq) — NOT a prefix — so a user's own `# BEGIN cqlite-mold-...`
# comment can never false-positive as the managed block.
_MOLD_BEGIN_MARKER='# BEGIN cqlite-mold (managed by scripts/bootstrap-agent-machine.sh — do not edit inside)'

# _mold_block_active: true when the bootstrap-managed block is present in the
# per-machine cargo config file cargo ACTUALLY reads. Cargo prefers the
# extension-less `config` over `config.toml` when BOTH exist (a documented legacy
# precedence), so we probe ONLY the effective file — checking both would report
# `linked` on a both-files machine where the block sits in the ignored `config.toml`.
_mold_block_active() {
  local cfg_dir="${CARGO_HOME:-$HOME/.cargo}" f
  if [ -f "$cfg_dir/config" ]; then
    f="$cfg_dir/config"
  elif [ -f "$cfg_dir/config.toml" ]; then
    f="$cfg_dir/config.toml"
  else
    return 1
  fi
  grep -Fxq "$_MOLD_BEGIN_MARKER" "$f" 2>/dev/null
}

# _mold_state: resolve linked|overridden|present-unconfigured|absent, memoized.
# A non-empty RUSTFLAGS **or** CARGO_ENCODED_RUSTFLAGS in the gate environment
# suppresses cargo's target.rustflags entirely (encoded takes even higher
# precedence), so either one turns an otherwise-`linked` state into `overridden`.
_MOLD_STATE=""
_mold_state() {
  [ -n "$_MOLD_STATE" ] && { printf '%s' "$_MOLD_STATE"; return; }
  if [ -n "${AGENT_GATE_TEST_MOLD_STATE:-}" ]; then
    _MOLD_STATE="$AGENT_GATE_TEST_MOLD_STATE"
  elif ! command -v mold >/dev/null 2>&1; then
    _MOLD_STATE=absent
  elif { [ -n "${RUSTFLAGS:-}" ] || [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]; } \
       && _mold_block_active; then
    # Managed block present but a global (encoded-)RUSTFLAGS suppresses it →
    # honest signal that the wired -fuse-ld=mold is NOT in effect.
    _MOLD_STATE=overridden
  elif _mold_block_active; then
    _MOLD_STATE=linked
  else
    _MOLD_STATE=present-unconfigured
  fi
  printf '%s' "$_MOLD_STATE"
}

# _mold_accel_token: the ` mold=<state>` suffix on Linux hosts, empty elsewhere.
_mold_accel_token() {
  local os="${AGENT_GATE_TEST_OS:-$(uname -s 2>/dev/null || echo unknown)}"
  case "$os" in
    Linux|linux) printf ' mold=%s' "$(_mold_state)" ;;
    *) : ;; # Darwin/other: no token — byte-identical to pre-#2859 output
  esac
}

# ---- perf profiling capability token (Linux only, issue #3249) --------------
# Agent boxes ship with kernel.perf_event_paranoid = 4, which denies ALL
# unprivileged perf use — a PERMISSION verdict that reads like a missing
# CAPABILITY, and one that reverts on reboot when no /etc/sysctl.d drop-in exists.
# Stamping it here makes "this box cannot be profiled" visible in every pasted
# SUMMARY instead of being discovered at the start of a measurement cycle.
#
# HARD CONSTRAINT — and it is ENFORCED, not asserted in prose (issue #3249 review):
# the emit-time perf path is the FREE /proc read from scripts/perf-capability.sh and
# NOTHING else: no `perf stat` exec, no new binary dependency, no external process,
# and no command substitution — a `$( )` is a forked subshell, so "no subprocess" that
# is read back through `$( )` would be self-contradictory. That is why the functions
# below take an <outvar> and assign into it instead of printing (and why the helper is
# sourced ONCE, at script scope, rather than re-read on every summary). Case 9f-free
# of scripts/tests/test_agent_gate_summary.sh kills any regression: it runs this exact
# code with an EMPTY PATH and with xtrace subshell-depth counting. The functional
# verification (which DOES exec perf) is bootstrap's job, not the gate's.
#
# NO MEMOIZATION of the state, deliberately: every emit runs inside a `$( )`, so an
# assignment to a script-level cache would land in a subshell and be discarded — a
# cache that looks real and never hits. Two `read`-builtin /proc reads cost nothing,
# so the honest implementation is to just do them.
#
# Sourced HERE, at script scope: the helper is 300+ lines, and re-reading it on every
# emit bought nothing (its functions are all a subshell inherits anyway). Sourcing it
# is documented side-effect free — functions plus PERF_CAPABILITY_* constants only.
_PERF_CAP_LOADED=0
if [ -r "$REPO_ROOT/scripts/perf-capability.sh" ]; then
  # shellcheck source=scripts/perf-capability.sh
  if . "$REPO_ROOT/scripts/perf-capability.sh" 2>/dev/null; then _PERF_CAP_LOADED=1; fi
fi

# _AGENT_GATE_OS: the host OS, resolved ONCE per gate run. `uname` is an external
# process, so the OS question cannot be asked inside the per-emit token path above;
# asking it at script scope costs one fork per RUN instead of one per summary. The
# AGENT_GATE_TEST_OS seam is honoured exactly as before (tests export it before the
# gate starts, so call-time vs init-time resolution is equivalent).
_AGENT_GATE_OS="${AGENT_GATE_TEST_OS:-$(uname -s 2>/dev/null || echo unknown)}"

# _perf_state_into <outvar>: the state token, assigned into <outvar>. Never left
# empty — an empty token would emit a bare `perf=`, which no consumer can parse.
_perf_state_into() {
  local __ps_out="$1" __ps_v=""
  if [ -n "${AGENT_GATE_TEST_PERF_STATE:-}" ]; then
    __ps_v="$AGENT_GATE_TEST_PERF_STATE"
  elif [ "${_PERF_CAP_LOADED:-0}" = 1 ]; then
    perf_capability_token_into __ps_v
  fi
  [ -n "$__ps_v" ] || __ps_v=unknown
  eval "$__ps_out=\$__ps_v"
}

# _perf_accel_token_into <outvar>: the ` perf=<state>` suffix on Linux hosts, empty
# elsewhere — the controls are Linux kernel knobs, so Darwin output stays
# byte-identical (same contract as _mold_accel_token above).
_perf_accel_token_into() {
  local __pat_out="$1" __pat_state=""
  case "${_AGENT_GATE_OS:-unknown}" in
    Linux|linux)
      _perf_state_into __pat_state
      eval "$__pat_out=\" perf=\$__pat_state\"" ;;
    *) eval "$__pat_out=" ;;
  esac
}

# stdout forms, for debugging/ad-hoc use only — NOT the emit path (reading them costs
# the caller the very `$( )` the `_into` forms exist to avoid).
_perf_state()       { local v; _perf_state_into v; printf '%s' "$v"; }
_perf_accel_token() { local v; _perf_accel_token_into v; printf '%s' "$v"; }

# accelerators_line: the machine-checkable one-liner stamped into every SUMMARY
# block (full, lite, and the emission selftest). Values: on|absent|off|serial.
# See the ACCEL_* detection above (#1848). The trailing sccache-health token
# (na|ok|warn, issue #2641) surfaces sccache's own corruption counters. On Linux
# a ` mold=linked|overridden|present-unconfigured|absent` token follows (issue
# #2859), then ` perf=ok|paranoid-<N>|kptr-restricted|absent|unknown` (issue
# #3249); Darwin output is unchanged.
# The perf token is fetched through a VARIABLE, not a `$( )`: its path is
# contractually free of forks (see above), and reading it back through a command
# substitution here would reintroduce exactly the subshell that contract excludes.
accelerators_line() {
  local perf_tok=""
  _perf_accel_token_into perf_tok
  printf 'accelerators: sccache=%s nextest=%s lanes=%s sccache-health=%s%s%s' \
    "${ACCEL_SCCACHE:-unknown}" "${ACCEL_NEXTEST:-unknown}" "${ACCEL_LANES:-unknown}" \
    "$(_sccache_health)" "$(_mold_accel_token)" "$perf_tok"
}

# ---- Per-gate core budget (issue #2640) -------------------------------------
# The #1825 machine-wide cap bounds the NUMBER of concurrent full gates (N), but
# on its own does nothing to stop N gates from EACH spawning ncpu build/test
# threads → ncpu*N oversubscription → SIGKILLs (~15 gate-ish procs) and timing
# flakes (2 gates → test_write_throughput class). We therefore give each gate a
# FAIR SHARE of the cores derived from the very same slot count:
#   per-gate cores = max(1, floor(ncpu / N))
# where N is the resolved machine-wide concurrency (CQLITE_GATE_MAX_CONCURRENCY
# override, else the #1825 default formula). When this gate is the SOLE gate
# (N=1 — the bootstrap default, see bootstrap-agent-machine.sh) it gets the FULL
# core count, i.e. no throttling at all. The budget drives:
#   * CARGO_BUILD_JOBS  — caps rustc codegen-unit / crate parallelism, and
#   * GATE_TEST_THREADS — the nextest / cargo-test `--test-threads` for the
#                         core-tests long pole.
# A caller who exports CARGO_BUILD_JOBS is respected verbatim (never overridden).

# _gate_max_concurrency: resolve N from the #1825 default formula + the
# CQLITE_GATE_MAX_CONCURRENCY override. Defined early (issue #2640) because the
# core-budget derivation below needs it before any cargo runs; the #1825 cap's
# acquire_gate_slot consumes the SAME function further down (single source).
_gate_max_concurrency() {
  local dflt=$(( ( _ncpu - 2 ) / 4 ))
  [ "$dflt" -lt 2 ] && dflt=2
  local v="${CQLITE_GATE_MAX_CONCURRENCY:-$dflt}"
  case "$v" in *[!0-9]*|'') v=$dflt ;; esac
  [ "$v" -lt 1 ] && v=1
  printf '%s' "$v"
}

# _gate_cores_per_gate: the fair-share core count for THIS gate = max(1, ncpu/N).
_gate_cores_per_gate() {
  local n cores
  n=$(_gate_max_concurrency)
  cores=$(( _ncpu / n ))
  [ "$cores" -lt 1 ] && cores=1
  printf '%s' "$cores"
}

# Derive + export the budget now, before any component runs. CARGO_BUILD_JOBS is
# honored if the caller already set it (explicit override wins); GATE_TEST_THREADS
# is always this run's derived value (run_core_tests passes it to nextest/cargo).
GATE_CORES_PER_GATE=$(_gate_cores_per_gate)
GATE_TEST_THREADS="$GATE_CORES_PER_GATE"
if [ -z "${CARGO_BUILD_JOBS:-}" ]; then
  export CARGO_BUILD_JOBS="$GATE_CORES_PER_GATE"
  CARGO_BUILD_JOBS_SOURCE=derived
else
  CARGO_BUILD_JOBS_SOURCE=caller
fi

# cpu_budget_line: machine-checkable one-liner stamped into every SUMMARY block,
# so per-gate CPU throttling (or its absence) is visible in the pasted block, not
# just scrollback (#2640). Names the wrapper (nice/taskpolicy/none), the resolved
# machine-wide concurrency N, the derived per-gate cores, and the build-jobs +
# test-threads the gate actually used.
#
# The `cpu-budget:` line is a space-delimited `key=value`-per-token line, so the
# wrapper field MUST be a SINGLE token: AGENT_GATE_WRAPPER holds the full command
# WITH flags (`taskpolicy -c utility`, `nice -n 10`), whose embedded spaces would
# otherwise inject stray `-c`/`utility`/`-n`/`10` tokens between wrapper= and the
# next key and break any positional/space-splitting parser. Emit only the tool
# name (first word) here; the full command stays in AGENT_GATE_WRAPPER for the
# re-exec (issue #2640).
cpu_budget_line() {
  local _wrapper_tok="${AGENT_GATE_WRAPPER:-none}"
  _wrapper_tok="${_wrapper_tok%% *}"   # first word only: "taskpolicy -c utility" -> "taskpolicy"
  printf 'cpu-budget: wrapper=%s ncpu=%s max-concurrency=%s cores-per-gate=%s build-jobs=%s(%s) test-threads=%s' \
    "$_wrapper_tok" "$_ncpu" "$(_gate_max_concurrency)" \
    "$GATE_CORES_PER_GATE" "${CARGO_BUILD_JOBS:-unset}" "${CARGO_BUILD_JOBS_SOURCE:-unknown}" \
    "$GATE_TEST_THREADS"
}

# Static-golden mandate (coordinator directive for #1737): the local gate runs
# against STATIC GOLDENS only. The live Docker/Cassandra sstabledump parity tests
# (issue #911, the *_under_cassandra5_sstabledump cases) otherwise fire during
# core-tests whenever a Docker daemon + a cassandra:5.0* image are present, adding
# wall-clock and non-determinism (measured ~10s each on a warm image). We default
# CQLITE_SKIP_DOCKER_TESTS=1 so run_core_tests filters them out; that coverage
# still runs in the nightly/dispatch Docker CI lanes, and setting
# CQLITE_SKIP_DOCKER_TESTS=0 restores them here (when Docker is available).
export CQLITE_SKIP_DOCKER_TESTS="${CQLITE_SKIP_DOCKER_TESTS:-1}"

# --- Authoritative Cargo integration-test target mapping (issue #1821) --------
# roborev finding: a Bash `case` glob such as `*/tests/*.rs` also matches NESTED
# helper/module files (e.g. cqlite-core/tests/write_read_roundtrip/data_multi.rs,
# cqlite-cli/tests/common/mod.rs) that are NOT Cargo `--test` targets. Passing
# such a stem as `--test <stem>` makes --lite FAIL on valid helper-only changes.
# We therefore map a changed .rs file to a `--test` target ONLY via authoritative
# Cargo metadata (each integration target's exact src_path + name + required-features
# — no path/name heuristics). A metadata parser (jq OR python3) is a PREREQUISITE
# for per-`--test`-target selection: without one we cannot learn a target's
# required-features, and emitting a feature-gated target feature-less would make
# --lite FAIL spuriously in a minimal shell env (roborev round-3 finding). So when
# NEITHER jq nor python3 is available we emit NO `--test` targets at all — run_lite
# scopes to the touched packages' `--lib` only (safe: --lib carries no per-target
# required-features) and prints a note pointing at the full gate for integration
# coverage. Hand-parsing Cargo.toml for required-features would just be another
# heuristic, so we deliberately do not. These helpers use no Bash-4-only features
# (no associative arrays), so the whole --lite path runs under macOS's Bash 3.2.

# Emit "<abs_src_path>\t<pkg>\t<testname>\t<required-features>" for every Cargo
# test target (required-features comma-joined, empty when none), or nothing if
# metadata cannot be produced/parsed. A single src_path can appear on MULTIPLE
# lines: the workspace-root package `cqlite` and the `cqlite-integration-tests`
# crate both own the top-level tests/*.rs files, and every owning package's
# target must be runnable (issue #1821 roborev finding 1).
_test_target_index() {
  # Test hook (issue #1821 roborev round 3): force the no-metadata-parser path so
  # the tooling self-test can assert the parser-absent behaviour hermetically,
  # without PATH surgery on jq/python3/cargo.
  [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] && return 0
  local meta
  meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null) || return 0
  [ -n "$meta" ] || return 0
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$meta" | jq -r \
      '.packages[] | .name as $p | .targets[]
       | select(.kind[] == "test")
       | "\(.src_path)\t\($p)\t\(.name)\t\((."required-features" // []) | join(","))"'
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$meta" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for p in d["packages"]:
    for t in p["targets"]:
        if "test" in t.get("kind", []):
            feats = ",".join(t.get("required-features") or [])
            print("%s\t%s\t%s\t%s" % (t["src_path"], p["name"], t["name"], feats))
'
  fi
}

# Read changed repo-relative paths on stdin; print "<pkg>|<testname>|<features>"
# for EVERY Cargo `--test` target that a changed path is (features comma-joined,
# possibly empty). A single path may emit MULTIPLE lines when several packages own
# it (root `cqlite` + `cqlite-integration-tests` both own top-level tests/*.rs) —
# all owners are emitted so none is silently dropped (issue #1821 finding 1).
# Nested helper/module files are excluded. Deterministic; Bash 3.2-safe.
#
# Authoritative Cargo metadata (jq OR python3) is REQUIRED: without a parser we
# cannot know a target's required-features, and emitting a feature-gated target
# feature-less would make --lite FAIL spuriously (roborev round-3 finding). So
# when metadata is unavailable this emits NOTHING — the caller (run_scoped_tests)
# then scopes to package --lib only and says so, rather than guessing targets.
classify_test_targets() {
  local index f abs hits
  index=$(_test_target_index)
  # No metadata parser (or metadata unavailable) -> emit no --test targets.
  [ -n "$index" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in *.rs) ;; *) continue ;; esac
    abs="$REPO_ROOT/$f"
    # ALL owning targets (no early exit), "<pkg>|<name>|<features>" per line.
    hits=$(printf '%s\n' "$index" \
      | awk -F'\t' -v p="$abs" '$1 == p { print $2 "|" $3 "|" $4 }')
    [ -n "$hits" ] && printf '%s\n' "$hits"
  done
}

# Emit "<abs_manifest_dir>\t<pkg>\t<has_lib>" for EVERY workspace package, or
# nothing when metadata cannot be produced/parsed. `has_lib` is 1 when the
# package has a library target that `cargo test --lib` can run (a target whose
# kind includes "lib" or "rlib"; a cdylib-only binding crate is 0). This is the
# single authoritative source of package ownership — it covers ALL members
# (core, cli, flight, parity, integration-tests, format-compat, tools/*,
# bindings/*, examples, the workspace-root `cqlite`), so no member can fall
# through a hand-maintained list (issue #1821 recurring roborev finding).
_package_index() {
  # Test hook: force the no-metadata-parser path hermetically (issue #1821).
  [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] && return 0
  local meta
  meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null) || return 0
  [ -n "$meta" ] || return 0
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$meta" | jq -r \
      '.packages[]
       | (.manifest_path | sub("/[^/]+$"; "")) as $dir
       | (if any(.targets[]; (.kind[] == "lib") or (.kind[] == "rlib")) then 1 else 0 end) as $lib
       | "\($dir)\t\(.name)\t\($lib)"'
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$meta" | python3 -c '
import json, os, sys
d = json.load(sys.stdin)
for p in d["packages"]:
    dr = os.path.dirname(p["manifest_path"])
    lib = 1 if any(("lib" in t["kind"]) or ("rlib" in t["kind"]) for t in p["targets"]) else 0
    print("%s\t%s\t%d" % (dr, p["name"], lib))
'
  fi
}

# Given the package index (as $1) and changed repo-relative paths on stdin, print
# "<pkg>|<has_lib>" for the workspace package that OWNS each path: the package
# whose manifest directory is the LONGEST prefix of the path. The workspace-root
# package (manifest dir == repo root) is EXCLUDED as a path owner — its directory
# is a prefix of everything, so treating it as an owner would make it a degenerate
# catch-all for docs/scripts/config changes; it still enters the package set via
# test-target ownership when a top-level tests/*.rs it owns changes. Deterministic;
# one owner per path; Bash 3.2-safe (no associative arrays).
_owners_from_index() {
  local index=$1 f abs
  [ -n "$index" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    abs="$REPO_ROOT/$f"
    printf '%s\n' "$index" | awk -F'\t' -v path="$abs" -v root="$REPO_ROOT" '
      $1 == root { next }
      { if (substr(path, 1, length($1) + 1) == $1 "/" && length($1) > bl) { bl = length($1); best = $2 "|" $3 } }
      END { if (best != "") print best }'
  done
}

# Self-test / debug hook: map stdin paths -> "<pkg>|<has_lib>" via metadata-derived
# longest-prefix ownership. Empty when no metadata parser is available.
classify_package_owners() { _owners_from_index "$(_package_index)"; }

# Emit the name of every workspace package that (a) DECLARES a dependency on
# cqlite-core AND (b) owns at least one Cargo `--test` target — i.e. the dependent
# TEST crates whose test code a cqlite-core API change can break invisibly to
# --lite (issue #2658). cqlite-core itself is excluded (its --lib is already run);
# cdylib bindings (cqlite-py/cqlite-node) fall out naturally (zero test targets).
# Fully metadata-driven (jq OR python3, same parsers as _package_index): the
# declared-dependency edge is present in `cargo metadata --no-deps`. Empty when no
# parser is available (the caller then FAILs loudly — never a silent narrowing).
# Deterministic, sorted; Bash 3.2-safe.
_core_dependent_test_pkgs() {
  # Test hook (issue #2658): force the no-parser path hermetically.
  [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] && return 0
  local meta
  meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null) || return 0
  [ -n "$meta" ] || return 0
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$meta" | jq -r \
      '.packages[]
       | select(.name != "cqlite-core")
       | select(any(.dependencies[]; .name == "cqlite-core"))
       | select(any(.targets[]; .kind[] == "test"))
       | .name' | sort -u
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$meta" | python3 -c '
import json, sys
d = json.load(sys.stdin)
out = set()
for p in d["packages"]:
    if p["name"] == "cqlite-core":
        continue
    if not any(dep["name"] == "cqlite-core" for dep in p["dependencies"]):
        continue
    if any("test" in t["kind"] for t in p["targets"]):
        out.add(p["name"])
for n in sorted(out):
    print(n)
'
  fi
}

# Print cqlite-core's absolute manifest DIRECTORY from the metadata package index,
# or nothing when metadata is unavailable. Single source for "is this path a
# cqlite-core src file" without hardcoding "cqlite-core/src" (worktree-safe).
_core_src_dir() {
  printf '%s\n' "$(_package_index)" \
    | awk -F'\t' '$2 == "cqlite-core" { print $1 "/src"; exit }'
}

# Read changed repo-relative paths on stdin; if ANY is a cqlite-core SOURCE file
# (under cqlite-core/src/), emit "compile-check-pkg: <pkg>" for every dependent
# TEST crate (issue #2658) — the extra `cargo test --no-run` compile-checks a
# core-src diff adds to the --lite plan so a core API change that breaks a SEPARATE
# test crate's code is caught at --lite time, not later as a lite-green->full-red
# wasted round. Emits NOTHING when the diff has no core-src change. Deterministic;
# no side effects; does not invoke cargo test. Consumed by run_scoped_tests and the
# hidden --classify-core-dependent-compile-check self-test hook (single source).
classify_core_dependent_compile_check() {
  local changed coredir abs f is_core=0 pkg
  changed=$(cat)
  coredir=$(_core_src_dir)
  [ -n "$coredir" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    abs="$REPO_ROOT/$f"
    if [ "${abs#"$coredir"/}" != "$abs" ]; then is_core=1; break; fi
  done <<<"$changed"
  [ "$is_core" -eq 1 ] || return 0
  while IFS= read -r pkg; do
    [ -n "$pkg" ] || continue
    echo "compile-check-pkg: $pkg"
  done <<<"$(_core_dependent_test_pkgs)"
}

# Loud-fail message for the no-metadata-parser path (issue #2658). When NEITHER jq
# nor python3 is present we CANNOT derive from Cargo metadata: package ownership,
# per-`--test`-target required-features, NOR the core-dependent compile-check set.
# The pre-#2658 behavior silently NARROWED --lite to `cqlite-core --lib` — a
# FALSE-CONFIDENCE path on a minimal box, where a green --lite could have skipped
# every dependent crate + integration target (and, post-#2658, the whole
# core-src dependent-crate compile-check). So the no-parser path now FAILS LOUDLY,
# naming the missing tooling, instead of running a narrowed subset that reads green.
# Single source of truth for both run_scoped_tests and the --scoped-noparser-fail-msg
# self-test hook (never edit one side alone).
_scoped_noparser_fail_msg() {
  echo "no Cargo-metadata parser (need jq OR python3): --lite cannot derive package ownership, per-target required-features, or the core-dependent compile-check set. Refusing to silently narrow to the cqlite-core library tests only (that reads green while skipping dependent/integration crates). Install jq or python3, or run the full gate."
}

# The FAST python-binding tier --lite runs for a bindings/python diff (issue #1893)
# INSTEAD of the always-libpython-link-failing `cargo test -p cqlite-py`. cqlite-py
# is a pyo3 cdylib, so a plain `cargo test` on it never links libpython and gave
# --lite ZERO python signal on ~1/3 of binding diffs.
#
# REAL single source of truth (roborev job 1449, Medium): the executor in
# run_scoped_tests `eval`s EXACTLY these two component strings, and
# PYTHON_LITE_TIER_CMD — the plan string --classify-scoped-plan advertises and the
# self-test asserts — is composed from the SAME two components, so the advertised
# plan and the executed command can never drift. Never edit one side alone: change
# a component string and both the plan and the execution change together.
PYTHON_LITE_MATURIN_CMD="maturin develop --profile dev -m bindings/python/Cargo.toml"
PYTHON_LITE_PYTEST_CMD="pytest bindings/python/tests -m 'not slow' -q"
PYTHON_LITE_TIER_CMD="$PYTHON_LITE_MATURIN_CMD && $PYTHON_LITE_PYTEST_CMD"

# Python-tier verdict marker for the LITE SUMMARY block (roborev job 1450, Low):
# when a python-binding diff is in scope, the block itself must say what the tier
# did — especially a SKIP (offline/toolchain), where scoped-tests can read PASS
# while the python diff was NOT validated. A pasted green block that validated
# nothing must be detectable from the block alone, not scrollback. Set by
# run_scoped_tests; rendered by run_lite as a `python-tier:` line. Empty (no line)
# when the diff has no python-binding change.
PYTHON_TIER_NOTE=""

# Read changed repo-relative paths on stdin; emit the deduped set of owning Cargo
# workspace packages (one per line) — the union of path-owners + changed
# --test-target owners, derived from `cargo metadata`. Bash 3.2-safe (no
# associative arrays); empty when no metadata parser is available. Inner helper of
# classify_scoped_plan — THE single routing function consumed by both the --lite
# executor (run_scoped_tests) and the --classify-scoped-plan self-test hook.
_scoped_pkgset() {
  local changed index owners newtests pkgset="" key pkg tpkg
  changed=$(cat)
  index=$(_package_index)
  owners=$(printf '%s\n' "$changed" | _owners_from_index "$index")
  newtests=$(printf '%s\n' "$changed" | classify_test_targets)
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    pkg=${key%%|*}
    [ -n "$pkg" ] || continue
    printf '%s\n' "$pkgset" | grep -qxF "$pkg" || pkgset="${pkgset}${pkg}"$'\n'
  done <<<"$owners"
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    tpkg=${key%%|*}
    [ -n "$tpkg" ] || continue
    printf '%s\n' "$pkgset" | grep -qxF "$tpkg" || pkgset="${pkgset}${tpkg}"$'\n'
  done <<<"$newtests"
  printf '%s' "$pkgset" | awk 'NF'
}

# THE scoped-tests ROUTING function (issue #1893; single-sourced per roborev job
# 1450): map stdin changed paths -> the scoped-tests plan. Emits `rust-pkg: <pkg>`
# for every owning rust workspace package EXCEPT cqlite-py, and `python-tier: <cmd>`
# once when a bindings/python change is present (cqlite-py owns it — a pyo3 cdylib
# whose `cargo test` can never link libpython). Node (cqlite-node) and rust-only
# diffs are untouched. Deterministic; no side effects; does not invoke cargo test.
#
# TWO consumers, one computation: run_scoped_tests (the --lite executor) parses
# these lines to decide what to run, and the hidden `--classify-scoped-plan` hook
# exposes the same lines to the py-route self-tests — so the routing the tests
# assert IS the routing the executor performs, never a parallel copy.
classify_scoped_plan() {
  local pkgset python_diff=0 pkg
  pkgset=$(cat | _scoped_pkgset)
  while IFS= read -r pkg; do
    [ -n "$pkg" ] || continue
    if [ "$pkg" = cqlite-py ]; then python_diff=1; continue; fi
    echo "rust-pkg: $pkg"
  done <<<"$pkgset"
  [ "$python_diff" -eq 1 ] && echo "python-tier: $PYTHON_LITE_TIER_CMD"
  return 0
}

# _delta_is_allowed_path <path> (issue #1892): TRUE (0) iff the path is a file the
# delta re-cert can actually EXECUTE (or pure docs); FALSE (non-0) for everything
# else. FAIL-CLOSED by construction — only an explicit executable-test/docs match
# is allowed, so any src, script, workflow, Cargo.*, or config change falls
# through to the refusal path.
#
# ALLOW only what run_delta's components EXECUTE (roborev jobs 1452 / 3323 / 3325 /
# 3327): a class that classifies ALLOW but that file-size/fmt/scoped-tests never
# run would produce a PASS DELTA block for an untested change. Therefore:
#   * rust cargo test code — AUTHORITATIVE, NOT glob-based (roborev job 3327). A
#     `.rs` path is allowed IFF it resolves to a Cargo `--test` target that
#     scoped-tests actually runs (`run_scoped_tests` runs the owning package's
#     `--lib` plus its top-level `--test <name>` targets). The allowed set is the
#     metadata-derived subset of the changed `.rs` files whose absolute `src_path`
#     matches a `--test` target in `_test_target_index` — the SAME authoritative
#     discovery `classify_test_targets` uses (no static globs). This closes at the
#     ROOT the whole class of glob holes the earlier `tests/*.rs` + `*_test(s).rs`
#     approach left open: nested helper mods under a tests/ dir (not targets),
#     repo-wide `*_test(s).rs` that are actually src or scripts (e.g.
#     `scripts/foo_tests.rs`, `cqlite-core/src/reader_test.rs`), and the
#     workspace-EXCLUDED `fuzz/` crate — none is a real, in-workspace `--test`
#     target, so none is allowed. When no cargo-metadata parser is available the
#     index is empty → NO `.rs` is allowed → the delta REFUSES any `.rs` change and
#     forces the full gate (fail-closed).
#   * python binding tests — `bindings/python/tests/*` (the #1893 python tier
#     executes the whole not-slow pytest suite for a cqlite-py-owned diff);
#   * docs — MARKDOWN ONLY (`*.md` anywhere, including under docs/ and website/).
#     NON-markdown files under docs/ or website/ (assets, config like
#     astro.config.mjs / package.json, app code like *.astro, and data artifacts
#     like delivery-telemetry.jsonl) are REFUSED: no delta component builds or
#     validates them, so a blanket docs/ or website/ allow would break the
#     fail-closed promise (roborev job 3325). The old blanket `docs/*` and
#     `website/*` globs were removed for exactly this reason.
# Also EXECUTED (issue #2081): node jest files (`bindings/node/__test__/*`, run by
# run_delta_node_tests against the already-built native module — run_delta REFUSES up
# front if it is not built, never building with cargo) and shell self-tests
# (`scripts/tests/*.sh`, executed verbatim by run_delta_shell_selftests).
# Deliberately REFUSED (require the full gate — --delta cannot execute them):
# any `.rs` that is not a Cargo `--test` target, plus everything outside the allowed
# classes above (src, Cargo.*, workflows, config, test-data). The caller precomputes the
# allowed-.rs set ONCE per delta run (via _delta_rs_target_paths, which calls cargo
# metadata a single time) into _DELTA_RS_ALLOWED_SET; this function consults that
# cached set for `.rs` paths — never invoking cargo per file. Defined before the
# arg-parse case so the hidden --delta-classify hook (and run_delta) can call it.
# Bash 3.2-safe (case globs + grep membership).

# Newline-delimited set of changed .rs paths that ARE executed Cargo `--test`
# targets, precomputed ONCE per delta run by the caller (delta_classify_stdin /
# run_delta) via _delta_rs_target_paths. Empty by default (set -u-safe): with no
# entries, _delta_is_allowed_path refuses every `.rs` path (fail-closed).
_DELTA_RS_ALLOWED_SET=""

# Read changed repo-relative paths on stdin; print the subset that ARE executed
# Cargo `--test` targets (one repo-relative path per line). AUTHORITATIVE: matches
# each `.rs` path's absolute `$REPO_ROOT/<path>` against the FIRST column of
# `_test_target_index` (the metadata-derived src_path→target map that
# `classify_test_targets` also uses), so the allowed set is exactly the files
# `run_scoped_tests` runs as `--test <name>`. Calls `_test_target_index` ONCE
# (cached in a var) — never once per file. When no metadata parser is available the
# index is empty → prints NOTHING → no `.rs` is delta-allowed (fail-closed).
# Non-.rs paths are ignored here (docs/python are handled by _delta_is_allowed_path
# directly). Deterministic; Bash 3.2-safe (no associative arrays).
_delta_rs_target_paths() {
  local index f abs
  index=$(_test_target_index)
  [ -n "$index" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in *.rs) ;; *) continue ;; esac
    abs="$REPO_ROOT/$f"
    if printf '%s\n' "$index" | awk -F'\t' -v p="$abs" '$1 == p { hit = 1 } END { exit !hit }'; then
      printf '%s\n' "$f"
    fi
  done
}

_delta_is_allowed_path() {
  case "$1" in
    # docs — MARKDOWN ONLY, anywhere (incl. under docs/ and website/). NON-md
    # files under docs/ or website/ (config, app code, assets, data artifacts)
    # fall through to the *) refusal: no delta component builds/validates them
    # (roborev job 3325). Do NOT re-add blanket docs/* or website/* allows.
    *.md) return 0 ;;
    # python binding tests — executed by the #1893 python tier (must stay ABOVE the
    # *.rs rule; these are .py so the *.rs case would not match anyway, but keeping
    # it first documents that the python tier runs the whole not-slow pytest suite).
    bindings/python/tests/*) return 0 ;;
    # rust cargo test code — AUTHORITATIVE, not glob-based (roborev job 3327): a
    # `.rs` path is allowed IFF it is an executed Cargo `--test` target, i.e. a
    # member of _DELTA_RS_ALLOWED_SET (precomputed once via _delta_rs_target_paths).
    # This refuses nested helper mods, src `*_test(s).rs`, `scripts/*_tests.rs`, and
    # the workspace-excluded `fuzz/` crate — none are real `--test` targets. With no
    # metadata parser the set is empty → every `.rs` refuses (fail-closed).
    *.rs)
      printf '%s\n' "$_DELTA_RS_ALLOWED_SET" | grep -qxF "$1" && return 0
      return 1
      ;;
    # node jest tests (issue #2081) — run by run_delta_node_tests against the
    # ALREADY-BUILT native module. ALLOWED here; run_delta REFUSES up front if the
    # module is not built (fail-closed — --delta never builds with cargo).
    bindings/node/__test__/*) return 0 ;;
    # shell self-tests (issue #2081) — executed verbatim by run_delta_shell_selftests.
    scripts/tests/*.sh) return 0 ;;
    *) return 1 ;;
  esac
}

# Hidden self-test hook (issue #1892): read changed repo-relative paths on stdin,
# print "ALLOW <path>" / "REFUSE <path>" per path (fail-closed classification via
# _delta_is_allowed_path), then a final "VERDICT: ALLOW" (all test/docs) or
# "VERDICT: REFUSE" (>=1 production file). Pure function — no git, cargo, or tree
# mutation — so scripts/tests can assert the refusal decision hermetically.
delta_classify_stdin() {
  local f verdict=ALLOW changed
  # Precompute the executed-target .rs allow-set ONCE (cargo metadata called a
  # single time), then classify each path against it — the same authoritative
  # decision run_delta makes. The --delta-classify hook runs IN the repo, so cargo
  # metadata is available (fail-closed to REFUSE-all-.rs when it is not).
  changed=$(cat)
  _DELTA_RS_ALLOWED_SET=$(printf '%s\n' "$changed" | _delta_rs_target_paths)
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if _delta_is_allowed_path "$f"; then
      echo "ALLOW $f"
    else
      echo "REFUSE $f"; verdict=REFUSE
    fi
  done <<<"$changed"
  echo "VERDICT: $verdict"
}

# _delta_python_tier_gap <python-tier-note>  (issue #1892, roborev job 3333)
# TRUE (0) iff the delta re-cert has an UNSOUND python gap: the ALLOWED set (read on
# stdin, newline-separated) includes a `bindings/python/tests/*` file — so the #1893
# python tier is REQUIRED to re-certify the diff — AND that tier did NOT actually run
# to a verdict (the note does not begin with "python-tier: PASS" or "python-tier: FAIL",
# i.e. it was SKIPPED — python3 missing or venv/pip/maturin setup failed — or never set).
# Because --delta runs NO clippy, a SKIPPED python tier leaves the changed python tests
# with ZERO compile/test backstop, so a PASS DELTA block would be an unsound green.
# Returns 1 (no gap) when there is no python test file in scope (a docs/rust-only delta
# is unaffected by python3 being absent) or when the tier ran (PASS/FAIL — a FAIL flows
# through as RESULT: FAIL). Pure: reads the allowed set on stdin, no cargo/git/side
# effects — exposed via the hidden --delta-python-gap hook so scripts/tests can assert
# the SAME decision run_delta consumes (single-source; drift is impossible).
_delta_python_tier_gap() {
  local note="${1:-}" line py_tests=0
  while IFS= read -r line; do
    case "$line" in
      bindings/python/tests/*) py_tests=1; break ;;
    esac
  done
  [ "$py_tests" -eq 1 ] || return 1
  case "$note" in
    "python-tier: PASS"*|"python-tier: FAIL"*) return 1 ;;
  esac
  return 0
}

# ---- issue #2078: FULL-gate fail-closed on an absent dataset corpus ------------
# The dataset preflight (far below) counts ALL *-Data.db under
# $CQLITE_DATASETS_ROOT/sstables. A FRESH worktree already carries ~19 tiny
# FORCE-ADDED byte-parity reference *-Data.db (test_compactionparity/,
# test_writeparity/, ...), so that count is > 0 even when the FETCHED validation
# corpus is ABSENT. The main dataset components (core-tests, smoke, python-bindings)
# scan the fetched corpus (test_basic/...); when it is missing they SKIP internally
# and the gate SKIP-then-PASSes — a green SUMMARY validating ZERO dataset-backed
# correctness. The FULL gate must instead FAIL CLOSED. --lite (returns before the
# preflight) and --only (kept lenient by the historical DATA_COUNT==0 check) are
# UNCHANGED.
CANONICAL_FIXTURE_KEYSPACE="test_basic"
# Stamped into the SUMMARY when the opt-out (AGENT_GATE_ALLOW_MISSING_FIXTURES=1)
# restores SKIP, so an intentional opt-out is visible in the pasted block.
MISSING_FIXTURES_MARKER=""

# _missing_fixtures_marker: the machine-checkable OPT-OUT line stamped into the
# SUMMARY. Single-sourced so the real preflight and the hidden --preflight-fixtures
# hook print identical text.
_missing_fixtures_marker() {
  printf '%s' "missing-fixtures: OPT-OUT (AGENT_GATE_ALLOW_MISSING_FIXTURES=1) — canonical corpus '$CANONICAL_FIXTURE_KEYSPACE' absent under $CQLITE_DATASETS_ROOT/sstables; dataset-dependent components SKIP; this run does NOT validate dataset-backed correctness (#2078)"
}

# _fixture_status: PURE decision for the FULL-gate canonical-corpus guard. Echoes
# exactly one token: OK (corpus present, or not the full gate → no-op), OPTOUT
# (corpus absent + AGENT_GATE_ALLOW_MISSING_FIXTURES=1 → restore SKIP + marker), or
# FAIL (corpus absent, no opt-out → the full gate must FAIL CLOSED). No side effects,
# so the hidden --preflight-fixtures hook asserts the SAME decision
# apply_fixture_preflight consumes (single-source; no drift).
_fixture_status() {
  # Only the FULL gate is strict: --only stays lenient, --lite already returned.
  [ -z "$ONLY" ] || { echo OK; return 0; }
  [ "$LITE" -eq 0 ] || { echo OK; return 0; }
  local n
  n=$(find "$CQLITE_DATASETS_ROOT/sstables/$CANONICAL_FIXTURE_KEYSPACE" -name "*-Data.db" 2>/dev/null | wc -l | tr -d ' ')
  [ "${n:-0}" -gt 0 ] && { echo OK; return 0; }
  [ "${AGENT_GATE_ALLOW_MISSING_FIXTURES:-0}" = 1 ] && { echo OPTOUT; return 0; }
  echo FAIL
}

# apply_fixture_preflight: EFFECTFUL FULL-gate canonical-corpus guard (issue #2078).
# Consumes _fixture_status. OK → no-op (byte-identical to pre-#2078). OPTOUT → set
# MISSING_FIXTURES_MARKER + a loud WARN, then return (lenient SKIP restored). FAIL →
# emit a FAIL SUMMARY with a one-line remedy and exit 1 (the full gate fails closed).
# Called only at runtime (after emit_summary is defined + the startup sentinel is
# written), so the FAIL branch may safely emit + exit.
apply_fixture_preflight() {
  case "$(_fixture_status)" in
    OK) return 0 ;;
    OPTOUT)
      MISSING_FIXTURES_MARKER="$(_missing_fixtures_marker)"
      echo "agent-gate: WARN: canonical dataset corpus ($CANONICAL_FIXTURE_KEYSPACE) absent — AGENT_GATE_ALLOW_MISSING_FIXTURES=1 set; dataset coverage SKIPPED, marker stamped in the SUMMARY (#2078)" >&2
      return 0 ;;
    *)
      echo "agent-gate: FAIL: canonical dataset corpus absent — no *-Data.db under $CQLITE_DATASETS_ROOT/sstables/$CANONICAL_FIXTURE_KEYSPACE (#2078)" >&2
      echo "agent-gate: only committed byte-parity references are present; the FETCHED validation corpus is missing, so dataset components would SKIP and the gate would falsely PASS." >&2
      echo "agent-gate: remedy: bash test-data/scripts/fetch-datasets.sh  (or point CQLITE_DATASETS_ROOT at a checkout that has it)" >&2
      echo "agent-gate: intentional opt-out (SKIP, stamped in the SUMMARY): AGENT_GATE_ALLOW_MISSING_FIXTURES=1" >&2
      _tree_meta_array   # #2926: every emitted block carries the tree provenance
      emit_summary FAIL \
        "preflight: FAIL (canonical corpus $CANONICAL_FIXTURE_KEYSPACE absent under $CQLITE_DATASETS_ROOT/sstables — only committed byte-parity refs present)" \
        "missing-fixtures: FAIL-CLOSED (#2078) — dataset-dependent components would SKIP; overall verdict FAIL" \
        "${TREE_META_LINES[@]}" \
        "hint: bash test-data/scripts/fetch-datasets.sh  (opt-out: AGENT_GATE_ALLOW_MISSING_FIXTURES=1 restores SKIP + stamps this block)"
      exit 1 ;;
  esac
}

# ---- issue #3148: FULL-gate fail-closed on an unreachable COMMITTED schemas root -
# #2078 (above) validates the FETCHED SSTable corpus. It says nothing about the
# COMMITTED CQL schema fixtures under test-data/schemas/ (23 files incl. legacy/ and
# udts/), which the dataset-backed components must also read to decode those SSTables.
# Before #3148, `grep -c schemas scripts/agent-gate.sh` was 0: a corpus whose
# sstables/ was complete but whose schema fixtures were unreachable passed the
# preflight with STATUS: OK, built for ~8 minutes, then failed core-tests +
# memory-budget with opaque "Path does not exist: …/basic-types.cql" panics. Worse
# than no preflight at all: `STATUS: OK` is POSITIVELY MISLEADING, so an agent reads
# "fixtures verified" and suspects its own diff (this nearly cost #3095/PR #3141 a
# misattributed triage).
#
# Since #3148 the Rust helpers resolve the schemas root CHECKOUT-RELATIVE
# (test-data/support/fixture_roots.rs — never $CQLITE_DATASETS_ROOT/../schemas), so
# this check is a cheap BELT-AND-BRACES assert on the same root that helper resolves:
# an explicit CQLITE_SCHEMAS_ROOT override when set + readable, else the checkout's
# test-data/schemas. Deliberately NO opt-out (unlike #2078's
# AGENT_GATE_ALLOW_MISSING_FIXTURES): the fetched corpus is legitimately absent
# sometimes, but committed source in a checkout never is — an unreachable schemas root
# is a broken checkout or a stale override, and neither may certify a run.
# --lite/--only stay lenient, unchanged from #2078's contract (#3148 AC (g)).

# The exact schema files the gate's dataset-backed components consume — a directory
# existence check is NOT enough (#3148 fix 1). The six from the shared bench fixture
# catalog (cqlite-core/benches/fixtures/mod.rs, read by memory_budget,
# issue_1494_converter_alloc_budget, issue_2075_row_assembly_alloc_budget,
# tail_latency_harness), which also cover dead_cache_delete_tests (basic-types.cql),
# observability_correctness (basic-types.cql) and cqlite-cli's export_csv bench
# (collections.cql).
CANONICAL_SCHEMA_FILES="basic-types.cql da-test.cql time-series.cql wide-table-bti.cql collections.cql wide-rows.cql"
# Stamped into the SUMMARY on a successful check so the pasted block shows POSITIVELY
# that the schemas root was validated, not merely that nothing complained.
SCHEMAS_LINE=""

# _gate_checkout_test_data_dir: the enclosing checkout's `test-data`, anchored on the
# WORKSPACE-ROOT `Cargo.toml` (nearest ancestor manifest declaring `[workspace]`) exactly
# as workspace_root() does in test-data/support/fixture_roots.rs. Anchoring on a checkout
# MARKER rather than on the fixtures matters: keying on `test-data/schemas` would let a
# sparse checkout — or a worktree nested inside another checkout — resolve to the OUTER
# checkout's fixtures, wrong-but-existing and unreported. Falls back to REPO_ROOT when no
# `[workspace]` manifest is found (not reachable in this repository).
#
# KNOWN EXCEPTION: `fuzz/Cargo.toml` declares its OWN `[workspace]` (deliberately — the
# fuzz crate is excluded from the main workspace, see #1614). So a hypothetical caller
# anchored inside `fuzz/` would resolve `fuzz/test-data`, which does not exist. Benign in
# both mirrors: the result is a LOUD failure naming that absent path, never a silent
# borrow of a wrong tree — and neither the gate (anchored on REPO_ROOT) nor any
# `#[path]`-including target lives under `fuzz/`. Named here so the next reader does not
# have to rediscover that the "nearest [workspace]" rule has an in-repo counterexample.
_gate_checkout_test_data_dir() {
  local d="$REPO_ROOT"
  while [ -n "$d" ] && [ "$d" != "/" ]; do
    if [ -f "$d/Cargo.toml" ] && grep -q '^[[:space:]]*\[workspace\]' "$d/Cargo.toml" 2>/dev/null; then
      printf '%s' "$d/test-data"; return 0
    fi
    d="$(dirname "$d")"
  done
  printf '%s' "$REPO_ROOT/test-data"
}

# _gate_schemas_override_reject: echoes a non-empty REASON when CQLITE_SCHEMAS_ROOT is set
# but must be REJECTED rather than resolved. Today there is exactly one such case, and it
# is the one that nearly reintroduced #3148's own defect: a RELATIVE override.
#
# The gate evaluates a relative path with CWD = REPO_ROOT; cargo runs each test binary
# with CWD = the PACKAGE directory. So `CQLITE_SCHEMAS_ROOT=packaged/schemas` exported
# from the checkout root passes the gate's `-d` test and gets stamped
# `schemas: 6/6 … under packaged/schemas (override)`, while every test binary sees
# `is_dir() == false`, falls back to the checkout, and reads DIFFERENT files. The SUMMARY
# would then certify root A for a run that used root B — a positively misleading block,
# which is the entire defect class #3148 was filed for. It also made the `expected
# absolute path:` remedy line print a relative path, breaking AC (b).
#
# Both sides therefore reject it, removing the one input class on which they could not
# possibly have agreed.
#
# HOW STRONG THAT IS, precisely: this shell resolution and
# test-data/support/fixture_roots.rs are two HAND-WRITTEN mirrors, EQUIVALENT TODAY and
# PINNED BY scripts/tests/test_agent_gate_schemas_preflight.sh — not equivalent by
# construction. They have been walked case by case over the whole input table (unset /
# "" / whitespace-only / control-character-bearing / "  /abs  " / absolute-non-dir /
# absolute-dir / relative) and agree on every one. If you edit EITHER side, re-walk that
# table and re-run that self-test: an unearned "by construction" here is precisely what
# would stop you doing so, and a silent divergence between these two is the
# root-certification mismatch this whole change exists to prevent.

# _gate_schemas_override_present: THE single normalization of "is there an override?".
# Returns a STATUS (0 = present), never text, and every consumer then reads
# `$CQLITE_SCHEMAS_ROOT` DIRECTLY. Two reasons for that shape, both learned the hard way:
#
#   1. Single-sourcing presence (roborev job 9, finding 1): `_gate_schemas_override_reject`
#      treated a WHITESPACE-ONLY value as unset (matching Rust's `v.trim().is_empty()`)
#      while `_gate_schemas_root`/`_gate_schemas_root_source` tested the raw `-n` value. The
#      whole point of the shell/Rust pair is that they answer identically on EVERY input, so
#      one predicate decides presence for all of them.
#   2. NO COMMAND SUBSTITUTION anywhere on the value path (roborev job 10, finding 2). The
#      previous text-returning helper was consumed as `v="$(_gate_schemas_override)"`, and
#      `$( )` STRIPS TRAILING NEWLINES. Measured: with `CQLITE_SCHEMAS_ROOT=$'/abs/dir\n'`
#      the gate reported `STATUS: OK` + `SOURCE: CQLITE_SCHEMAS_ROOT override` + `ROOT:
#      /abs/dir` (newline gone, `-d` true) while Rust kept the newline, got `is_dir() ==
#      false`, and degraded to the checkout — the gate certifying root A for a run that used
#      root B, i.e. the exact mis-certification this change exists to prevent, introduced by
#      the round-2 refactor that fixed (1). Returning a status and reading the variable
#      directly removes the substitution, and control-character values are additionally
#      REJECTED below on both sides so no such value can reach a comparison at all.
#
# Presence is decided on the TRIMMED value while the value USED is the RAW one — exactly as
# Rust does (`trim()` gates presence, `PathBuf::from(raw)` builds the path). Hence
# `"  /abs  "` is *present* and then REJECTED as non-absolute on both sides, never silently
# trimmed into `/abs`. The test is a pure-bash pattern, not `tr`, so it too is
# substitution-free.
_gate_schemas_override_present() {
  case "${CQLITE_SCHEMAS_ROOT:-}" in
    *[![:space:]]*) return 0 ;;
    *) return 1 ;;
  esac
}

# _gate_schemas_override_reject: echoes a non-empty REASON when an override is present but
# must be REJECTED rather than resolved. Mirrors resolve_schemas_root()'s two Err cases.
# _gate_schemas_override_reject_kind: `relative` | `control-chars` | (empty when accepted).
# The KIND, separate from the reason text, so the FAIL emit can name the ACTUAL cause. Before
# this the emit hard-coded "a RELATIVE schemas root ..." and stamped `... relative
# CQLITE_SCHEMAS_ROOT rejected` for EVERY rejection — a FALSE message for a
# control-character value, and untested because the round-3 coverage went through the pure
# hook and never saw the real emit (spec-auditor, AC (b) partial).
_gate_schemas_override_reject_kind() {
  _gate_schemas_override_present || return 0
  case "${CQLITE_SCHEMAS_ROOT:-}" in
    *[[:cntrl:]]*) printf '%s' 'control-chars'; return 0 ;;
  esac
  _gate_schemas_override_is_utf8 || { printf '%s' 'non-utf8'; return 0; }
  case "${CQLITE_SCHEMAS_ROOT:-}" in
    /*) return 0 ;;
    *) printf '%s' 'relative' ;;
  esac
}

# _gate_schemas_override_is_utf8: rc 0 iff CQLITE_SCHEMAS_ROOT is (provably) valid UTF-8.
#
# Bash handles the value as BYTES, so without this the gate validated a non-UTF-8 override and
# stamped it into the SUMMARY while Rust's `var_os(..).to_str()` could not represent it and
# rejected — the gate certifying one schemas root while the tests resolved another (roborev job
# 11, BLOCKER; measured `STATUS: OK` + `SOURCE: CQLITE_SCHEMAS_ROOT override` for a
# `bad\xff\xfedir` path). Now both sides reject it.
#
# Two-step so the common case needs no external tool AND an unverifiable value is never
# accepted:
#   1. Pure printable ASCII is valid UTF-8 by definition, so accept without invoking anything.
#      Done with a SUBPROCESS-FREE `case` under a function-local `LC_ALL=C`, which makes
#      `[[:print:]]` mean ASCII-printable regardless of the caller's locale (verified identical
#      under C, C.UTF-8 and en_US.UTF-8).
#
#      This replaced a `printf | grep -q` probe. DEFENSIVE HARDENING, not a fixed live bug:
#      under the script-wide `set -o pipefail` (:369) a NEGATED pipeline whose LEFT side can
#      take SIGPIPE is a latent branch-inversion hazard (roborev job 12, finding 1) — if it
#      fired, a malformed override would take the "pure ASCII" branch and skip validation. It
#      did NOT reproduce here: bash's BUILTIN `printf` gave `PIPESTATUS=[0 0]` at 5 B / 100 KB
#      / 1 MB / 5 MB on bash 5.2.21, and independently a value big enough to fill a 64 KB pipe
#      is ~16x this platform's `PATH_MAX` of 4096, so it could never name a real directory.
#      Hardened anyway because the hazard is platform-scoped: the `case` form removes the
#      pipeline, the SIGPIPE class, and the `grep` dependency at once.
#
#      ORDER IS LOAD-BEARING — `_gate_schemas_override_reject_kind()` screens control
#      characters BEFORE calling this, and must keep doing so. A newline is VALID UTF-8, so
#      this function ACCEPTS one; were the order reversed, a newline-bearing ABSOLUTE path
#      would be accepted outright instead of classified `control-chars`. (This supersedes the
#      earlier rationale, which cited `grep` treating a newline as a line terminator: the
#      `grep` is gone, the ordering requirement is not.)
#   2. Otherwise validate with `iconv -f UTF-8 -t UTF-8`, which fails on malformed input. If
#      `iconv` is ABSENT we REJECT rather than assume: "could not check" must not mean "accept",
#      or the hole comes straight back on a box without it. That is narrow — only non-ASCII
#      override values are affected, and the remedy (an ASCII path) is always available. The
#      self-test asserts BOTH worlds (accept with `iconv`, fail-closed reject without), so an
#      `iconv`-less host no longer reds the gate on a valid multibyte root (job 12, finding 2).
_gate_schemas_override_is_utf8() {
  local v="${CQLITE_SCHEMAS_ROOT:-}"
  # shellcheck disable=SC2034  # assigned to retarget bash's own pattern-matching locale
  local LC_ALL=C
  case "$v" in
    *[![:print:]]*) : ;;
    *) return 0 ;;
  esac
  command -v iconv >/dev/null 2>&1 || return 1
  LC_ALL=C printf '%s' "$v" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1
}

_gate_schemas_override_reject() {
  _gate_schemas_override_present || return 0
  # Control characters (newline, CR, embedded tab, ...). A path carrying one is never a
  # legitimate schemas root, and admitting it is what let `$( )`-stripping diverge the two
  # mirrors (finding 2 above). Rejecting outright keeps them aligned without relying on
  # every future consumer avoiding command substitution.
  case "${CQLITE_SCHEMAS_ROOT:-}" in
    *[[:cntrl:]]*)
      printf '%s' "CQLITE_SCHEMAS_ROOT must not contain control characters (newline/CR/tab), got $(printf '%q' "${CQLITE_SCHEMAS_ROOT:-}")"
      return 0 ;;
  esac
  _gate_schemas_override_is_utf8 \
    || { printf '%s' "CQLITE_SCHEMAS_ROOT must be valid UTF-8, got $(printf '%q' "${CQLITE_SCHEMAS_ROOT:-}")"; return 0; }
  case "${CQLITE_SCHEMAS_ROOT:-}" in
    /*) return 0 ;;
    *) printf '%s' "CQLITE_SCHEMAS_ROOT must be an ABSOLUTE path, got '${CQLITE_SCHEMAS_ROOT:-}'" ;;
  esac
}

# _gate_schemas_root / _gate_schemas_root_source: mirror resolve_schemas_root() in
# test-data/support/fixture_roots.rs rule for rule — the override applies only when set,
# non-blank, ABSOLUTE and a readable directory (an absolute-but-unusable export degrades
# to the checkout rather than pinning the run to a path that cannot work; a RELATIVE one
# is rejected outright, see above), else checkout-relative. Both sides anchor on the same
# workspace marker, so the gate asserts the path the tests will actually resolve.
_gate_schemas_root() {
  # Reads $CQLITE_SCHEMAS_ROOT DIRECTLY — never through `$( )`, which strips trailing
  # newlines (roborev job 10, finding 2).
  if _gate_schemas_override_present \
     && [ -z "$(_gate_schemas_override_reject)" ] \
     && [ -d "${CQLITE_SCHEMAS_ROOT:-}" ]; then
    printf '%s' "${CQLITE_SCHEMAS_ROOT:-}"
  else
    printf '%s' "$(_gate_checkout_test_data_dir)/schemas"
  fi
}
_gate_schemas_root_source() {
  if [ -n "$(_gate_schemas_override_reject)" ]; then
    printf '%s' "CQLITE_SCHEMAS_ROOT override REJECTED"
  elif _gate_schemas_override_present && [ -d "${CQLITE_SCHEMAS_ROOT:-}" ]; then
    printf '%s' "CQLITE_SCHEMAS_ROOT override"
  else
    printf '%s' "checkout-relative"
  fi
}

# _missing_schema_files: the space-separated subset of CANONICAL_SCHEMA_FILES that is not
# a READABLE REGULAR FILE under the resolved root. Empty means all present.
#
# `-f` as well as `-r`: bare `-r` accepts a DIRECTORY named `basic-types.cql`, while the
# Rust side asks for a readable regular file. Same question on both sides or the gate can
# certify a layout the tests reject (roborev job 8, finding 2; reviewer nit N7).
_missing_schema_files() {
  local root f out=""
  root="$(_gate_schemas_root)"
  # shellcheck disable=SC2086  # intentional word-split over the space-separated list
  for f in $CANONICAL_SCHEMA_FILES; do
    { [ -f "$root/$f" ] && [ -r "$root/$f" ]; } || out="${out:+$out }$f"
  done
  printf '%s' "$out"
}

# _schemas_status: PURE decision for the FULL-gate schemas guard. Echoes exactly one
# token: OK (all canonical .cql readable, or not the full gate → no-op) or FAIL. No
# side effects, so the hidden --preflight-schemas hook asserts the SAME decision
# apply_schemas_preflight consumes (single-source; no drift).
_schemas_status() {
  # Only the FULL gate is strict: --only stays lenient, --lite already returned.
  [ -z "$ONLY" ] || { echo OK; return 0; }
  [ "$LITE" -eq 0 ] || { echo OK; return 0; }
  # A rejected override FAILs even if the checkout's fixtures happen to be complete: the
  # operator asked for a root the contract cannot honor, and quietly using a different
  # one is the mis-certification this guard exists to prevent.
  [ -z "$(_gate_schemas_override_reject)" ] || { echo FAIL; return 0; }
  [ -z "$(_missing_schema_files)" ] && { echo OK; return 0; }
  echo FAIL
}

# apply_schemas_preflight: EFFECTFUL FULL-gate schemas guard (issue #3148). Consumes
# _schemas_status. OK → stamp the positive SCHEMAS_LINE and return. FAIL → emit a FAIL
# SUMMARY carrying `missing-schemas: FAIL-CLOSED (#3148)` — textually DISTINCT from
# #2078's `missing-fixtures:` so the two causes are separable in a pasted block — with
# a remedy naming the exact expected absolute path, then exit 1.
#
# The leniency early-return below is load-bearing TWICE over, and both cases are the
# defect this whole change exists to remove, one mode over:
#
#   1. `_schemas_status` returns OK unconditionally under `--only`/`--lite` (leniency, AC
#      (g)), so the OK branch used to stamp `schemas: 6/6 canonical .cql readable under
#      <root>` for a check that NEVER RAN. A positive assertion about an unperformed check
#      is exactly #3148's misleading `STATUS: OK`. It is stamped as an explicit NAMED
#      non-check rather than simply omitted: silence lets a reader of a pasted block
#      assume the FULL contract held, whereas `schemas: not checked (…)` cannot be
#      misread.
#   2. The REJECT branch below was NOT governed by `_schemas_status`, so a relative
#      override FAILed even an `--only` run — the effectful guard diverging from the pure
#      decision it is documented to consume, i.e. the "single-source, no drift" property
#      claimed for this pair. Gating both on ONE mode check restores it.
apply_schemas_preflight() {
  # REPORT-ONLY is a POSITIONAL ARGUMENT, deliberately not a variable (spec-auditor,
  # requirement 8). The first cut used `${_SCHEMAS_PREFLIGHT_REPORT_ONLY:-}`, which was never
  # initialized — so an INHERITED or EXPORTED value turned the FULL gate's fail-closed
  # `exit 1` into `return 1` at the bare call site, the run continued, and the
  # `missing-schemas: FAIL-CLOSED` text could be stamped inside a block reading
  # `RESULT: PASS`. That is precisely the "no environment opt-out may permit a run to certify
  # with the schemas root unreachable" requirement, defeated by the mechanism added to satisfy
  # a comment fix two rounds earlier.
  #
  # Initializing the variable would only close the INHERITED path: an `export`ed value set
  # after initialization still wins, because the read happens later. A positional parameter is
  # airtight instead — `$1` inside a function comes from the CALL, and no environment variable,
  # `export`, or `env -i` can supply it. The strict call site (the real gate) passes NOTHING,
  # so strictness is the default that requires no state to be correct.
  local mode="${1:-}" root missing reject
  local report_only=0
  [ "$mode" = report-only ] && report_only=1
  root="$(_gate_schemas_root)"

  # Leniency (AC (g), unchanged from #2078's contract): only the FULL gate is strict.
  # --lite never reaches here (run_lite always exits first), but it is checked anyway so
  # the invariant holds by construction rather than by call-site archaeology.
  if [ -n "$ONLY" ] || [ "$LITE" -ne 0 ]; then
    local _mode
    if [ -n "$ONLY" ]; then _mode="--only $ONLY"; else _mode="--lite"; fi
    SCHEMAS_LINE="schemas: not checked ($_mode is lenient, #3148 AC (g)) — this block asserts NOTHING about the schemas root"
    return 0
  fi

  reject="$(_gate_schemas_override_reject)"
  # A REJECTED override gets its own message and hint: "missing files" would be a lie
  # (the checkout's fixtures may be perfectly complete), and the actionable fact is that
  # the requested root cannot be honored identically by the gate and the test binaries.
  if [ -n "$reject" ]; then
    local kind why marker
    kind="$(_gate_schemas_override_reject_kind)"
    case "$kind" in
      control-chars)
        why="a schemas root carrying a CONTROL CHARACTER (newline/CR/tab) cannot round-trip through the gate's shell resolution — command substitution strips trailing newlines — so the gate would validate one path while cargo resolved another."
        marker="missing-schemas: FAIL-CLOSED (#3148) — CQLITE_SCHEMAS_ROOT contains a control character; the gate and the test binaries would resolve DIFFERENT roots; overall verdict FAIL" ;;
      non-utf8)
        why="this shell handles the value as raw BYTES and would accept it, while the Rust resolver cannot represent a non-UTF-8 value as text and rejects it — so the gate would certify one schemas root while the tests resolved another."
        marker="missing-schemas: FAIL-CLOSED (#3148) — CQLITE_SCHEMAS_ROOT is not valid UTF-8; the gate and the test binaries would resolve DIFFERENT roots; overall verdict FAIL" ;;
      *)
        why="a RELATIVE schemas root cannot mean the same thing on both sides: the gate resolves it against $REPO_ROOT, cargo resolves it against each test binary's PACKAGE dir."
        marker="missing-schemas: FAIL-CLOSED (#3148) — relative CQLITE_SCHEMAS_ROOT rejected; the gate and the test binaries would resolve DIFFERENT roots; overall verdict FAIL" ;;
    esac
    echo "agent-gate: FAIL: $reject (#3148)" >&2
    echo "agent-gate: $why" >&2
    echo "agent-gate: continuing would stamp a SUMMARY certifying one schemas root while the tests read another — the exact mis-certification #3148 was filed for." >&2
    echo "agent-gate: remedy: export a clean ABSOLUTE CQLITE_SCHEMAS_ROOT, or unset it to use $root" >&2
    # REPORT-ONLY mode (see the --preflight-schemas-line hook): return instead of emitting.
    # The hook cannot emit a SUMMARY — emit_summary/_tree_meta_array are defined AFTER the
    # arg dispatch — and STUBBING them there defined a SECOND `_tree_meta_array`, which broke
    # test_agent_gate_tree_portability.sh's derived-inventory uniqueness assert (n=45
    # uniq=44) and FAILed `tooling-tests` in the gate of record while passing standalone.
    # A mode flag on the terminal ACTION carries no decision and stamps no text, so it
    # cannot make a lenient path assert something it did not check.
    if [ "$report_only" -eq 1 ]; then
      SCHEMAS_LINE="$marker"
      return 1
    fi
    _tree_meta_array   # #2926
    emit_summary FAIL \
      "preflight: FAIL ($reject)" \
      "$marker" \
      "${TREE_META_LINES[@]}" \
      "hint: export a clean ABSOLUTE CQLITE_SCHEMAS_ROOT, or unset it to use $root"
    exit 1
  fi
  case "$(_schemas_status)" in
    OK)
      local n
      # shellcheck disable=SC2086  # intentional word-split over the space-separated list
      n=$(set -- $CANONICAL_SCHEMA_FILES; echo $#)
      SCHEMAS_LINE="schemas: $n/$n canonical .cql readable under $root ($(_gate_schemas_root_source))"
      return 0 ;;
    *)
      missing="$(_missing_schema_files)"
      echo "agent-gate: FAIL: committed CQL schema fixtures unreachable under $root ($(_gate_schemas_root_source)) (#3148)" >&2
      echo "agent-gate: unreadable: $missing" >&2
      echo "agent-gate: expected absolute path: $root/${missing%% *}" >&2
      echo "agent-gate: these are COMMITTED SOURCE (test-data/schemas, 23 files incl. legacy/ + udts/) — NOT part of the fetched corpus and NOT derived from CQLITE_DATASETS_ROOT." >&2
      echo "agent-gate: dataset-backed components (core-tests, memory-budget, cli-tests) would build for ~8 min and then panic on the missing .cql." >&2
      echo "agent-gate: remedy: unset CQLITE_SCHEMAS_ROOT (it overrides the checkout default), or restore the committed fixtures: git -C $REPO_ROOT restore --source=HEAD -- test-data/schemas" >&2
      if [ "$report_only" -eq 1 ]; then
        SCHEMAS_LINE="missing-schemas: FAIL-CLOSED (#3148) — unreadable: $missing"
        return 1
      fi
      _tree_meta_array   # #2926: every emitted block carries the tree provenance
      emit_summary FAIL \
        "preflight: FAIL (committed CQL schema fixtures unreadable under $root — missing: $missing)" \
        "missing-schemas: FAIL-CLOSED (#3148) — dataset-backed components would panic on an absent .cql; overall verdict FAIL" \
        "${TREE_META_LINES[@]}" \
        "hint: expected $root/${missing%% *} — unset CQLITE_SCHEMAS_ROOT, or: git -C $REPO_ROOT restore --source=HEAD -- test-data/schemas"
      exit 1 ;;
  esac
}

# ---- issue #2081: --delta executes node __test__/ + scripts/tests/*.sh ---------
# --delta re-cert (issue #1892) fail-closed on node jest files + shell self-tests
# purely because its components could not EXECUTE them. It now can: these helpers
# classify + run them so a node-test-only or shell-selftest-only polish round after a
# full-gate PASS re-certifies with --delta instead of forcing a whole new full gate.

# _delta_node_targets / _delta_shell_targets: read repo-relative paths on stdin,
# print the subset that is a node jest test / a shell self-test. Case globs are
# string matches (`*` spans '/'), so nested paths match too.
_delta_node_targets() {
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in bindings/node/__test__/*) printf '%s\n' "$f" ;; esac
  done
}
_delta_shell_targets() {
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in scripts/tests/*.sh) printf '%s\n' "$f" ;; esac
  done
}

# _delta_node_build_ready (issue #2081): TRUE (0) iff node+npm are on PATH AND the
# napi native module is already BUILT (a platform *.node exists in bindings/node/),
# so run_delta_node_tests can run jest WITHOUT a cargo build. FALSE otherwise →
# run_delta REFUSES a node-__test__ delta (fail-closed: --delta must never build with
# cargo nor pass vacuously). Pure check; exposed via the --delta-node-ready hook.
# NOTE: this probe is existence-only (it does not check the .node's mtime/hash
# against the anchor or current tree) — a pre-anchor stale .node could in
# principle serve a vacuous jest pass IF the anchor full gate itself skipped
# node-bindings. This is acceptable ONLY because --delta separately refuses the
# whole re-cert on any src change (including bindings/node/src/**), so in the
# normal flow the module on disk is always >= the anchor commit; it is not a
# correctness gap this check needs to close on its own.
_delta_node_build_ready() {
  command -v node >/dev/null 2>&1 || return 1
  command -v npm  >/dev/null 2>&1 || return 1
  local nd
  nd=$(find "$REPO_ROOT/bindings/node" -maxdepth 1 -name '*.node' 2>/dev/null | head -1)
  [ -n "$nd" ]
}

# _run_shell_selftest_files <path...> (issue #2081): execute each repo-relative
# scripts/tests/*.sh self-test via `bash <repo-root>/<path>`. Echoes
# "shell-selftest: <path> PASS|FAIL" per script; returns 0 iff ALL passed. Shared by
# run_delta_shell_selftests and the hidden --delta-run-shell hook (single-source).
_run_shell_selftest_files() {
  local rc=0 f log
  for f in "$@"; do
    [ -n "$f" ] || continue
    log=$(mktemp "${TMPDIR:-/tmp}/agent-gate-shellst.XXXXXX")
    if bash "$REPO_ROOT/$f" >"$log" 2>&1; then
      echo "shell-selftest: $f PASS"
    else
      echo "shell-selftest: $f FAIL"; tail -30 "$log"; rc=1
    fi
    rm -f "$log"
  done
  return "$rc"
}

# _python_build_verify_venv <venv> <maturin-develop-cmd> [<active-venv-out-file>]
# (issue #1803): build the cqlite python extension into <venv> and GUARANTEE it
# imports, self-healing a stale/half-built editable install EXACTLY ONCE. The
# persistent venv under target/ is REUSED for the healthy path (the speed
# optimization). On a miss (maturin exits 0 but the module does not import — a
# venv-resolution miss from a cleaned target/, an interrupted prior run, or a
# concurrent same-checkout gate) it self-heals into a PRIVATE per-process venv
# rather than tearing down the shared one (roborev round-2 Finding B): a
# concurrent gate in the same checkout (a `--only python-bindings` alongside a
# full gate, or two lite runs) reuses the SAME $venv, so an `rm -rf "$venv"`
# here would race that other process's mid-build/pytest use of it. macOS has no
# flock, so we sidestep the race entirely instead of locking: never destroy
# shared mutable state. Contract (single source of truth for BOTH
# run_python_bindings and the --lite python tier; exposed to the self-test via
# the --python-build-verify hook):
#   exit 0 -> extension built AND `import cqlite._cqlite` verified (possibly via
#             a private self-heal venv — see $3 below).
#   exit 1 -> venv creation / pip install failed: a TOOLCHAIN gap (offline?) — the
#             full component FAILs on it, the lite tier SKIPs (unchanged split).
#   exit 2 -> `maturin develop` exited non-zero WITH cargo+rustc present: a real
#             COMPILE ERROR of our bindings (hard-FAIL in both the full gate and
#             the lite tier).
#   exit 3 -> maturin exited 0 but the module did NOT import even after a clean-
#             venv rebuild: a real binding DEFECT, not a venv miss. Emits the
#             DISTINCT marker line so the failure reads as a code defect, not a
#             transient flake.
#   exit 4 -> `maturin develop` exited non-zero because the build TOOLCHAIN
#             itself is absent (no cargo/rustc on PATH — offline/toolchain gap,
#             roborev round-2 Finding A): same class as exit 1, NOT a compile
#             error — the full gate still hard-FAILs on it (cargo/rustc are
#             always present in a full gate run), but the lite tier SKIPs.
# $3, if given, is a file path this function writes the ACTUALLY-USED venv
# directory into (the shared $venv on the healthy path, or the private heal venv
# after a self-heal) — callers activate THAT path for the subsequent pytest run
# and are responsible for `rm -rf`-ing it afterward when it is not the shared
# venv (a private heal venv must not accumulate under target/).
# The venv/pip/maturin/import operations are PATH-shadowable so the hermetic self-
# test (test_agent_gate_python_bindings_determinism.sh) can simulate the miss +
# heal without a real maturin build; production supplies the real toolchain.
_python_build_verify_venv() {
  local venv="$1" maturin_cmd="$2" out_file="${3:-}"
  local active_venv="$venv"

  _pbv_write_active() {
    [ -n "$out_file" ] && printf '%s' "$active_venv" >"$out_file"
  }
  # venv + pip deps (rc!=0 = toolchain gap). Reuses an existing venv (speed).
  _pbv_setup() {
    [ -x "$active_venv/bin/python" ] || python3 -m venv "$active_venv" || return 1
    (
      set -euo pipefail
      # shellcheck disable=SC1091
      . "$active_venv/bin/activate"
      pip install --quiet --upgrade pip >/dev/null 2>&1 || true
      pip install --quiet maturin pytest
    )
  }
  # maturin develop. rc 4 = the build TOOLCHAIN itself is absent (no cargo/rustc
  # on PATH — an offline/toolchain gap, same class as rc 1). Any other non-zero
  # (cargo+rustc present, maturin still failed) propagates as-is: a REAL compile
  # error of our bindings.
  _pbv_build() {
    if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then
      return 4
    fi
    (
      set -euo pipefail
      # shellcheck disable=SC1091
      . "$active_venv/bin/activate"
      eval "$maturin_cmd"
    )
  }
  # verify the freshly-built editable install actually imports (rc!=0 = miss).
  _pbv_verify() {
    (
      set -euo pipefail
      # shellcheck disable=SC1091
      . "$active_venv/bin/activate"
      python -c 'import cqlite; import cqlite._cqlite'
    ) >/dev/null 2>&1
  }

  local build_rc
  _pbv_setup || { _pbv_write_active; return 1; }
  build_rc=0; _pbv_build || build_rc=$?
  if [ "$build_rc" -eq 4 ]; then _pbv_write_active; return 4; fi
  if [ "$build_rc" -ne 0 ]; then _pbv_write_active; return 2; fi
  if _pbv_verify; then _pbv_write_active; return 0; fi

  # maturin exited 0 but the module did not import → venv-resolution miss. Self-
  # heal ONCE into a PRIVATE per-process venv (PID-suffixed; a $RANDOM fallback
  # guards the vanishingly rare PID collision) — the shared $venv is NEVER
  # rm -rf'd, so a concurrent same-checkout gate reusing it is never raced.
  local heal_venv="${venv}.heal.$$"
  while [ -e "$heal_venv" ]; do heal_venv="${venv}.heal.$$.$RANDOM"; done
  echo "[python-bindings] cqlite._cqlite did not import after 'maturin develop' (exit 0) — self-healing into a private venv ($heal_venv, issue #1803); the shared venv is left untouched to avoid a cross-run teardown race" >&2
  active_venv="$heal_venv"
  _pbv_setup || { _pbv_write_active; return 1; }
  build_rc=0; _pbv_build || build_rc=$?
  if [ "$build_rc" -eq 4 ]; then _pbv_write_active; return 4; fi
  if [ "$build_rc" -ne 0 ]; then _pbv_write_active; return 2; fi
  if _pbv_verify; then _pbv_write_active; return 0; fi

  # Every return path above writes $active_venv to $out_file (a no-op when the
  # caller passed none — e.g. the hermetic self-test), so the caller is SOLELY
  # responsible for `rm -rf`-ing it when it differs from the shared $venv (a
  # private heal venv must never accumulate under target/, but the shared venv
  # must never be torn down by this function). Uniform for every exit code:
  # nothing here special-cases cleanup, so there is exactly one place a caller
  # needs to check.
  _pbv_write_active
  echo "[python-bindings] FAIL: cqlite._cqlite did not import after clean-venv rebuild — real binding defect, not a venv-resolution miss" >&2
  return 3
}

COMPONENTS=(file-size fmt clippy roborev-lints core-tests tombstones-scan scan-offload-guard work-counters-guard byte-budget-guard arrow-parity-guard memory-budget integration-tests format-compat write-tests cli-tests compaction-byte-parity bti-multiclustering query-semantics-oracle flight-query-semantics-oracle flight-tests legacy-heuristics feature-iso-parquet feature-iso-delta-scan python-bindings node-bindings binding-rust-tests delivery-telemetry oom-audit parity-report operator-metrics-doc kit-dashboard-drift binding-unwind-profile pub-surface tooling-tests minimal-build smoke)

# _component_lane <name> (issues #1737, #2657): SINGLE SOURCE OF TRUTH for the
# MAIN-vs-SIDE lane split. Defined early (before the arg-parse dispatch) so the
# hidden --classify-lanes self-test can assert the mapping WITHOUT running any
# cargo/git. Prints "side" for a component that runs in the concurrent SIDE lane
# (its own CARGO_TARGET_DIR — no shared cqlite-core target contention with MAIN),
# else "main". is_side_component (below) delegates here so there is exactly one
# lane definition. Rationale for each SIDE member lives on the is_side_component
# doc block. Everything else stays on the strictly-serial MAIN lane; the SUMMARY
# is reconstructed in canonical COMPONENTS order regardless of lane, so the
# summary-block contract is unchanged by this mapping.
_component_lane() {
  case "$1" in
    python-bindings|node-bindings) printf side ;;
    # binding-rust-tests builds cqlite-ffi-common (a cqlite-core dependent with
    # default-features = false) and cqlite-node (cqlite-core + parquet + cli-helpers +
    # write-support) — class (a) of the SIDE rationale: a feature set that DIVERGES from
    # MAIN's, so sharing MAIN's target dir would thrash it (#2657/#3522).
    binding-rust-tests) printf side ;;
    parity-report|delivery-telemetry|binding-unwind-profile|smoke|memory-budget) printf side ;;
    # #1699 feature-matrix lanes. All four build cqlite-core at a feature set that
    # DIVERGES from MAIN's (cqlite-flight's arrow flavour; default+legacy-heuristics;
    # no-default-features+one-of parquet/delta-scan), which is class (a) of the SIDE
    # rationale below: sharing MAIN's target dir would thrash it (#2657).
    flight-tests|legacy-heuristics|feature-iso-parquet|feature-iso-delta-scan) printf side ;;
    *) printf main ;;
  esac
}
# --lite (issue #1821) runs ONLY this fast subset: file-size ratchet, fmt,
# FULL-workspace clippy (cross-crate API breaks are the cheap-insurance class),
# and blast-radius-scoped tests (the touched package's --lib + the diff's new
# test targets), NOT the full core-tests/write/cli/bindings/parity set. It is the
# FAST ITERATION loop, NOT the gate of record — the full gate must PASS once
# before merge. See run_lite() below.
LITE_COMPONENTS=(file-size fmt clippy roborev-lints scoped-tests)
# --delta (issue #1892): TEST/DOCS-ONLY RE-CERTIFICATION after a full-gate PASS.
# Given an anchor (the commit the full gate PASSed at), it verifies the diff
# anchor..HEAD touches ONLY files the delta can EXECUTE (FAIL-CLOSED if any
# production file changed), then re-certifies with this fast subset — file-size +
# fmt + the diff's changed test targets. The Rust allow decision is AUTHORITATIVE,
# not glob-based (roborev job 3327): a `.rs` file is allowed IFF it is a Cargo
# `--test` target that scoped-tests executes (via _delta_rs_target_paths /
# _test_target_index), so nested helper mods, src `*_test(s).rs`, `scripts/*.rs`,
# and the workspace-excluded `fuzz/` crate all refuse. It is NOT the gate of
# record: the gate of record
# remains the full agent-gate.sh PASS at the anchor, recorded alongside the delta
# evidence in the PR. The standing backstop is the nightly full run on main
# (.github/workflows/gate.yml deep-check). See run_delta() below.
DELTA_COMPONENTS=(file-size fmt scoped-tests)
ONLY=""
SELFTEST=0
LITE=0
LITE_AGG_SELFTEST=0
DELTA=0
DELTA_ANCHOR=""
DELTA_ANCHOR_RUN_ID=""
DELTA_ANCHOR_SUMMARY_FILE=""
# Names of the executors that RAN a --delta re-cert, for the DELTA SUMMARY's
# `delta-executors:` line (issue #2081). Populated by run_delta after run_scoped_tests.
DELTA_EXECUTORS=""
# Optional base-ref override (issue #1892): run_file_size and run_scoped_tests
# resolve their diff base from this when set, instead of merge-base with main.
# --delta points it at the anchor commit so the ratchet + scoping cover exactly
# the anchor..HEAD test/docs diff. Empty everywhere else (unchanged behavior).
GATE_BASE_OVERRIDE=""
case "${1:-}" in
  --list) printf '%s\n' "${COMPONENTS[@]}"; exit 0 ;;
  # --lite alone runs the fast gate; `--lite --emit-summary-selftest` drives the
  # LITE summary block through the real emission path (for tooling-tests) without
  # running any component.
  --lite) LITE=1; [ "${2:-}" = --emit-summary-selftest ] && SELFTEST=1 ;;
  --lite-list) printf '%s\n' "${LITE_COMPONENTS[@]}"; exit 0 ;;
  # Hidden self-test hook (issue #2121): drive the --lite OVERALL aggregation
  # (aggregate_lite_components) hermetically. Seeds per-component .result files from
  # AGENT_GATE_TEST_LITE_RESULTS ("name:status ..."), plus the scoped-tests NAMES entry
  # (AGENT_GATE_TEST_LITE_SCOPED, default PASS), then emits the LITE block and exits on
  # OVERALL — proving a component FAIL flips RESULT + exit WITHOUT running cargo. The
  # execution block lives near the --lite dispatch (aggregate_lite_components must be
  # defined first).
  --lite-aggregate-selftest) LITE=1; LITE_AGG_SELFTEST=1 ;;
  --delta-list) printf '%s\n' "${DELTA_COMPONENTS[@]}"; exit 0 ;;
  # --delta <anchor> [--anchor-run-id <id>] [--anchor-summary-file <path>]
  #                  [--emit-summary-selftest]
  # Re-certify a test/docs-only diff anchor..HEAD (issue #1892). The anchor is the
  # commit the full gate PASSed at. The anchor's full-gate run-id is recorded from
  # --anchor-run-id, else read from --anchor-summary-file (which must itself be a
  # FULL-gate PASS block — a lite/delta block cannot anchor a delta re-cert).
  --delta)
    DELTA=1
    DELTA_ANCHOR="${2:?--delta needs an anchor commit/sha (the commit the full gate PASSed at)}"
    shift 2 || true
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --anchor-run-id) DELTA_ANCHOR_RUN_ID="${2:?--anchor-run-id needs a value}"; shift 2 ;;
        --anchor-summary-file) DELTA_ANCHOR_SUMMARY_FILE="${2:?--anchor-summary-file needs a path}"; shift 2 ;;
        --emit-summary-selftest) SELFTEST=1; shift ;;
        *) echo "unknown --delta option: $1" >&2; exit 2 ;;
      esac
    done
    ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<testname>"
  # for actual Cargo test targets (nested helpers excluded). No side effects.
  --classify-test-targets) classify_test_targets; exit 0 ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<has_lib>"
  # via metadata-derived longest-prefix package ownership. No side effects.
  --classify-package-owners) classify_package_owners; exit 0 ;;
  # Hidden self-test hook (issue #2657): print "<lane> <component>" for every gate
  # component in canonical COMPONENTS order, where <lane> is main|side, so the
  # self-test can assert the parallel-sublane split WITHOUT running cargo/git. No
  # side effects.
  --classify-lanes)
    for _lc in "${COMPONENTS[@]}"; do printf '%s %s\n' "$(_component_lane "$_lc")" "$_lc"; done
    exit 0 ;;
  # Hidden self-test hook (issue #2658): print the no-metadata-parser LOUD-FAIL
  # message so the self-test can assert --lite FAILS (naming the missing tool)
  # rather than silently narrowing to cqlite-core --lib. No side effects.
  --scoped-noparser-fail-msg) _scoped_noparser_fail_msg; exit 0 ;;
  # Hidden self-test hook (issue #2658): map stdin changed paths -> the extra
  # `cargo test --no-run` dependent-crate compile-check targets a core-src diff
  # adds to the --lite plan ("compile-check-pkg: <pkg>" lines). No cargo/git.
  --classify-core-dependent-compile-check) classify_core_dependent_compile_check; exit 0 ;;
  # Hidden self-test hook (issue #1893): map stdin changed paths -> the scoped-tests
  # PLAN ("rust-pkg: <pkg>" / "python-tier: <cmd>") WITHOUT running cargo/maturin, so
  # the self-test can assert python diffs route to the maturin+pytest tier.
  --classify-scoped-plan) classify_scoped_plan; exit 0 ;;
  # Hidden self-test hook (issue #1892): classify stdin paths as test/docs (ALLOW)
  # or production (REFUSE) and print a final VERDICT. No side effects; no cargo/git.
  --delta-classify) delta_classify_stdin; exit 0 ;;
  # Hidden self-test hook (issue #1892, roborev job 3333): read the delta's ALLOWED
  # paths on stdin + the python-tier note as $2; print "GAP" (unsound: python tests
  # in scope but the tier did not run → run_delta REFUSES) or "OK". This is the SAME
  # _delta_python_tier_gap decision run_delta consumes, so the self-test asserts the
  # real fail-closed behavior hermetically (no cargo/maturin/git).
  --delta-python-gap) _delta_python_tier_gap "${2:-}" && echo GAP || echo OK; exit 0 ;;
  # Hidden self-test hook (issue #2078): print the FULL-gate canonical-corpus
  # decision (OK|OPTOUT|FAIL) from _fixture_status; on OPTOUT also print the marker
  # line stamped into the SUMMARY. Pure — the FAIL emit + exit lives in
  # apply_fixture_preflight (exercised by the real gate run).
  --preflight-fixtures)
    _pf_st=$(_fixture_status); echo "STATUS: $_pf_st"
    [ "$_pf_st" = OPTOUT ] && echo "$(_missing_fixtures_marker)"
    exit 0 ;;
  # Hidden self-test hook (issue #3148): print the FULL-gate COMMITTED-SCHEMAS decision
  # (OK|FAIL) from _schemas_status, plus the resolved ROOT, its SOURCE and (on FAIL) the
  # unreadable files — so the positive-control self-test asserts the preflight actually
  # FAILS on a schemas-less root, not merely that it passes on a good one. Pure — the
  # FAIL emit + exit lives in apply_schemas_preflight (exercised by the real gate run).
  # Optional $2 seeds ONLY, so the self-test can assert the --only LENIENCY branch of
  # the SAME pure decision without launching a real --only run (the arg dispatch is a
  # single `case "$1"`, so `--only X --preflight-schemas` is not expressible).
  --preflight-schemas)
    [ -n "${2:-}" ] && ONLY="$2"
    _ps_st=$(_schemas_status); echo "STATUS: $_ps_st"
    echo "ROOT: $(_gate_schemas_root)"
    echo "SOURCE: $(_gate_schemas_root_source)"
    _ps_rj=$(_gate_schemas_override_reject)
    [ -n "$_ps_rj" ] && echo "REJECT: $_ps_rj"
    [ "$_ps_st" = FAIL ] && [ -z "$_ps_rj" ] && echo "MISSING: $(_missing_schema_files)"
    exit 0 ;;
  # Hidden self-test hook (issue #3148): run the REAL apply_schemas_preflight and print the
  # SCHEMAS_LINE it stamped, so the self-test observes the ACTUAL summary text rather than a
  # re-implementation of the decision. Optional $2 seeds ONLY (the arg dispatch is a single
  # `case "$1"`, so `--only X --preflight-schemas-line` is not expressible, and a real
  # `--only core-tests` run would spend minutes in cargo before printing anything).
  # Deliberately drives the effectful function: the whole point is that a POSITIVE line must
  # never be stamped for a check that did not run.
  #
  # It can NOT observe the FAIL SUMMARY, and saying otherwise would send a reader looking for
  # a block that cannot exist: `emit_summary` and `_tree_meta_array` are defined AFTER this
  # dispatch point. So the hook passes `report-only` as apply_schemas_preflight's FIRST
  # ARGUMENT, and the two failure branches then RETURN with the marker in SCHEMAS_LINE instead
  # of emitting + exiting. An argument, not a variable: an uninitialized env-readable flag was
  # itself a way to defeat the fail-closed guard (see that function's header).
  #
  # The first attempt STUBBED those two functions here instead. That defined a SECOND
  # `_tree_meta_array` in this file, which broke test_agent_gate_tree_portability.sh's
  # derived-inventory uniqueness assert (n=45 uniq=44) and FAILed `tooling-tests` in the gate
  # of record — while every self-test passed standalone, because that portability test only
  # runs inside `tooling-tests`. Hence: never add a second definition of a `_tree*` function.
  --preflight-schemas-line)
    [ -n "${2:-}" ] && ONLY="$2"
    apply_schemas_preflight report-only || true
    echo "SCHEMAS_LINE: ${SCHEMAS_LINE:-<none>}"
    exit 0 ;;
  # Hidden self-test hooks (issue #2081): expose the node-build readiness decision and
  # the shell-selftest executor so scripts/tests assert the SAME logic run_delta uses.
  --delta-node-ready) _delta_node_build_ready && echo READY || echo NOT-READY; exit 0 ;;
  --delta-run-shell)
    _drs_changed=$(cat)
    _drs_targets=$(printf '%s\n' "$_drs_changed" | _delta_shell_targets)
    _drs_arr=()
    while IFS= read -r _drs_f; do [ -n "$_drs_f" ] && _drs_arr+=("$_drs_f"); done <<<"$_drs_targets"
    if [ "${#_drs_arr[@]}" -gt 0 ]; then _run_shell_selftest_files "${_drs_arr[@]}"; exit $?; fi
    echo "shell-selftest: (none)"; exit 0 ;;
  # Hidden self-test hook (issue #1803): run the python build + import-verify +
  # one-shot self-heal in isolation. Args: <venv> <maturin-develop-cmd>
  # [<active-venv-out-file>]. Exits with the _python_build_verify_venv contract
  # code (0 ok / 1 venv-pip toolchain / 2 real-compile-error / 3 import-defect-
  # after-clean-rebuild / 4 build-toolchain-absent). Drives the SAME function
  # both real call sites use, so test_agent_gate_python_bindings_determinism.sh
  # can assert the self-heal + private-venv behavior hermetically with
  # PATH-shadowed python3/pip/maturin/python/cargo/rustc — no drift from
  # production.
  --python-build-verify)
    _python_build_verify_venv "${2:?--python-build-verify needs <venv>}" "${3:?--python-build-verify needs <maturin-develop-cmd>}" "${4:-}"
    exit $? ;;
  # Hidden self-test hook (issue #2640): print the per-gate CPU budget the gate
  # derived (the same cpu_budget_line stamped into the SUMMARY) and exit 0. Lets
  # scripts/tests assert the CARGO_BUILD_JOBS + test-threads derivation from the
  # slot count (full cores at N=1, fair share at N>1, caller override respected)
  # WITHOUT running any component. No side effects beyond reading env + ncpu.
  --cpu-budget) cpu_budget_line; echo; exit 0 ;;
  --only) ONLY="${2:?--only needs a comma-separated component list}" ;;
  --emit-summary-selftest) SELFTEST=1 ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

# Summary-block markers + optional MODE line (issue #1821). The DEFAULT (full
# gate) values are the historical literals, so a no-flag run's output is
# byte-for-byte unchanged. --lite swaps in DISTINCT markers plus a MODE line so a
# lite summary can NEVER be mistaken for — or pasted as — the full gate's SUMMARY
# (which remains the only run that counts). Everything that writes/greps the block
# uses these variables; for LITE=0 they equal the old literals exactly.
SUMMARY_START_MARKER="==== AGENT-GATE SUMMARY ===="
SUMMARY_END_MARKER="==== END AGENT-GATE SUMMARY ===="
SUMMARY_MODE_LINE=""
if [ "$LITE" -eq 1 ]; then
  SUMMARY_START_MARKER="==== AGENT-GATE LITE SUMMARY ===="
  SUMMARY_END_MARKER="==== END AGENT-GATE LITE SUMMARY ===="
  SUMMARY_MODE_LINE="MODE: lite (FAST ITERATION — NOT the gate of record; full agent-gate.sh must PASS once before merge)"
elif [ "$DELTA" -eq 1 ]; then
  # DISTINCT delta markers + a MODE line naming the gate of record (issue #1892):
  # a delta summary can NEVER be mistaken for — or pasted as — a full SUMMARY. The
  # gate of record remains the full agent-gate.sh PASS at the anchor.
  SUMMARY_START_MARKER="==== AGENT-GATE DELTA SUMMARY ===="
  SUMMARY_END_MARKER="==== END AGENT-GATE DELTA SUMMARY ===="
  SUMMARY_MODE_LINE="MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $DELTA_ANCHOR)"
fi

# #2874: capture the INHERITED parent-run marker (exported by an ENCLOSING gate)
# NOW, before we mint our own RUN_ID and export our own marker below. A non-empty
# value means THIS invocation is nested inside another gate's component run (e.g.
# a tooling-tests self-test recursively invoking agent-gate.sh). The summary-path
# resolution below uses it to default a nested run to a PRIVATE path so it can never
# write the enclosing checkout's shared default and clobber the parent gate of record.
INHERITED_PARENT_RUN_ID="${AGENT_GATE_PARENT_RUN_ID:-}"
# #2874: record whether the caller EXPLICITLY provided AGENT_GATE_SUMMARY_FILE (before
# we de-export it). The integrity self-test hooks (which seed a foreign block into the
# resolved summary path) fail closed unless this is 1 — a clobber-prevention script must
# never ship a hook that clobbers the checkout default.
EXPLICIT_SUMMARY_FILE=0
[ -n "${AGENT_GATE_SUMMARY_FILE:-}" ] && EXPLICIT_SUMMARY_FILE=1

LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX")
# Per-run nonce (#1175 roborev finding 1): the LOG_DIR is a fresh per-run mktemp
# path, so it uniquely identifies THIS invocation. We stamp it into every SUMMARY
# block as `run-id:` so completeness can be verified for THIS run, never a stale
# prior run's file that happens to still contain an old complete block.
RUN_ID="$LOG_DIR"
# Caller-known summary path (#1175): the caller may pick the path IN ADVANCE via
# AGENT_GATE_SUMMARY_FILE; otherwise we use a stable, documented repo-root default
# the caller can `cat` without parsing stdout. This is THE recovery contract: the
# complete SUMMARY is always at this exact path even if the streamed copy is lost.
# CONCURRENCY (#1175): this default is per-CHECKOUT, shared by every gate run in
# the same $REPO_ROOT. Concurrent same-checkout runs MUST each set a unique
# AGENT_GATE_SUMMARY_FILE or they clobber each other's recovery artifact;
# separate worktrees get distinct repo roots and are already isolated.
# The lite run uses a DISTINCT default recovery filename (issue #1821) so it can
# never clobber the full gate's recovery artifact, and so `cat`-ing the default
# after a lite run can never be misread as the full gate's result.
# #2874: nested-run summary isolation. A nested invocation is one that started with
# an ENCLOSING gate's run marker in its env (INHERITED_PARENT_RUN_ID) and did NOT
# pin its own AGENT_GATE_SUMMARY_FILE. Such a run defaults its summary to a PRIVATE
# path inside its OWN mktemp log dir — NEVER the enclosing checkout's shared default
# (.agent-gate-summary.txt / -lite- / -delta-), which the parent gate of record is
# using. This structurally closes the same-checkout default-path clobber vector for
# EVERY nested invocation, present and future, independent of any self-test's own
# unset/pin discipline (the residual kill surface after #2751 closed the env vector).
# An explicit AGENT_GATE_SUMMARY_FILE from the nested caller still WINS (self-tests
# keep pinning it to assert on summary content).
NESTED_RUN=0
if [ -n "$INHERITED_PARENT_RUN_ID" ] && [ -z "${AGENT_GATE_SUMMARY_FILE:-}" ]; then
  NESTED_RUN=1
fi
if [ -n "${AGENT_GATE_SUMMARY_FILE:-}" ]; then
  SUMMARY_FILE="$AGENT_GATE_SUMMARY_FILE"
elif [ "$NESTED_RUN" -eq 1 ]; then
  # DISTINCT from the LOG_SUMMARY_FILE archival copy ($LOG_DIR/summary.txt), so
  # emit_summary's `cp SUMMARY_FILE -> LOG_SUMMARY_FILE` is a real copy, not a
  # same-file no-op (#2874 review finding 4).
  SUMMARY_FILE="$LOG_DIR/summary-primary.txt"
elif [ "$LITE" -eq 1 ]; then
  SUMMARY_FILE="$REPO_ROOT/.agent-gate-lite-summary.txt"
elif [ "$DELTA" -eq 1 ]; then
  # DISTINCT delta recovery filename (issue #1892) so a delta run can never clobber
  # the full or lite recovery artifact, and `cat`-ing it can never be misread as
  # the full gate's result.
  SUMMARY_FILE="$REPO_ROOT/.agent-gate-delta-summary.txt"
else
  SUMMARY_FILE="$REPO_ROOT/.agent-gate-summary.txt"
fi
# #2874: stamp `nested-under: <parent-run-id>` whenever this run was spawned by an
# enclosing gate (INHERITED_PARENT_RUN_ID non-empty) — INDEPENDENT of the summary
# redirect decision (review finding 6). A nested run that pins its own
# AGENT_GATE_SUMMARY_FILE (the common self-test shape, and the #2751 shape) is still
# traceably marked nested, decoupling traceability from whether the path was redirected.
NESTED_UNDER_LINE=""
[ -n "$INHERITED_PARENT_RUN_ID" ] && NESTED_UNDER_LINE="nested-under: $INHERITED_PARENT_RUN_ID"
# Resolve a caller-provided RELATIVE AGENT_GATE_SUMMARY_FILE against the caller's
# original CWD, not the repo root we cd'd into (#1175 roborev finding 1). Absolute
# paths are used verbatim; the unset default above is already absolute.
case "$SUMMARY_FILE" in
  /*) ;; # absolute (incl. the repo-root default) -> use verbatim
  *)  SUMMARY_FILE="$INVOCATION_CWD/$SUMMARY_FILE" ;;
esac
# #2751: the summary path is now fully resolved into SUMMARY_FILE (the parent's own
# var). Both the startup INCOMPLETE sentinel below and emit_summary write
# SUMMARY_FILE and NEVER re-read AGENT_GATE_SUMMARY_FILE, so the env var has served
# its whole purpose. De-export it here — ONE scrub after resolution, before any
# component runs — so NO child this gate spawns (present or future) can inherit the
# parent's path and clobber the summary file mid-run with a foreign verdict. The
# tooling-tests self-tests recursively invoke agent-gate.sh (the --delta self-test's
# temp-repo runs, the summary self-test's --emit-summary-selftest runs); a nested
# gate that inherited this path would overwrite our file with a DELTA REFUSED block
# or a foreign-run-id INCOMPLETE placeholder (field impact: #2672 read a foreign
# verdict; #2600's full gate died in tooling-tests leaving such a placeholder,
# costing a 57-min re-run). Equivalent to `env -u AGENT_GATE_SUMMARY_FILE` on every
# child. The wrapper re-exec (which must preserve the caller's path) already ran
# above this line, so it is unaffected. The self-test scripts also scrub it
# themselves (belt-and-suspenders, #2751).
# Visible fallback (#2751 roborev r2): this clobber fix was filed FOR a silent
# failure, so it must not itself fail silently. If `export -n` errors on some shell,
# fall back to a plain `unset` (which fully removes it from the env — an even
# stronger scrub) and log one warning line, rather than swallowing it with `|| true`.
if ! export -n AGENT_GATE_SUMMARY_FILE 2>/dev/null; then
  echo "agent-gate: WARN export -n AGENT_GATE_SUMMARY_FILE failed; unsetting instead (#2751)" >&2
  unset AGENT_GATE_SUMMARY_FILE
fi
# #2874: export THIS run's marker so ANY gate we spawn (present or future) detects it
# is nested and defaults to a PRIVATE summary path (never this checkout's shared
# default), structurally closing the same-checkout default-path clobber vector
# regardless of self-test discipline. Distinct from the AGENT_GATE_SUMMARY_FILE we
# just de-exported: this marker is a NONCE ($RUN_ID = the per-run mktemp log dir),
# never a path a child would write, so exporting it can never itself cause a clobber.
export AGENT_GATE_PARENT_RUN_ID="$RUN_ID"
# Keep a copy under the logs bundle for archival.
LOG_SUMMARY_FILE="$LOG_DIR/summary.txt"
declare -a NAMES=() STATUSES=() TIMES=()
OVERALL=PASS

# Set to 1 by emit_summary if the authoritative caller-known summary file could
# NOT be written completely (bad path, perms, disk full, truncated write). The
# final exit logic forces a non-zero / FAIL outcome on this so a green gate can
# never silently lack its promised recovery artifact (#1175 roborev finding 1).
SUMMARY_WRITE_FAILED=0

# FAIL CLOSED for the integrity self-test hooks (#2874 review finding 5) — checked HERE,
# BEFORE the startup sentinel writes $SUMMARY_FILE, so a hook invoked without an explicit
# AGENT_GATE_SUMMARY_FILE never touches the checkout default at all (not even the INCOMPLETE
# sentinel). Those hooks seed a FOREIGN block into the resolved path; a clobber-prevention
# script must never ship a hook that can clobber the checkout default. The hooks THEMSELVES
# run later (they need emit_summary/_assert_summary_integrity, defined below).
if [ "${AGENT_GATE_INTEGRITY_SELFTEST:-0}" != 0 ] && [ "$EXPLICIT_SUMMARY_FILE" != 1 ]; then
  echo "agent-gate: AGENT_GATE_INTEGRITY_SELFTEST requires an explicit AGENT_GATE_SUMMARY_FILE (refusing to touch the checkout default) (#2874)" >&2
  exit 2
fi
# Review finding (MEDIUM): validate the selftest selector STRICTLY, BEFORE any gate work. The
# dispatch `case` below has no default arm, so an unrecognized value (e.g. a `Side` typo) would
# silently fall through and run a REAL full gate under the pinned throwaway summary path. Accept
# only the four known selectors; anything else is a caller error → exit 2 now (before the startup
# sentinel and any component).
case "${AGENT_GATE_INTEGRITY_SELFTEST:-0}" in
  0|1|side|marker|terminal-nomarker) : ;;
  *)
    echo "agent-gate: invalid AGENT_GATE_INTEGRITY_SELFTEST='${AGENT_GATE_INTEGRITY_SELFTEST}' (expected one of: 0 1 side marker terminal-nomarker) (#2874)" >&2
    exit 2 ;;
esac

# #2926 tree-integrity self-test hooks — SAME fail-closed discipline as #2874's, checked
# BEFORE the startup sentinel and before any tree work. The mutating modes WRITE INTO THE
# CHECKOUT, so they refuse unless the caller pinned its own throwaway AGENT_GATE_SUMMARY_FILE
# (never the checkout default) and every mutation target is an existing, repo-relative path.
# NOTE this is a TEST SEAM, never a bypass: no mode here can turn a mutated run green.
case "${AGENT_GATE_TREE_SELFTEST:-0}" in
  0|capture|clean|boundary|side|terminal|postfinalize|validate-manifest|report-lookup|mode-components) : ;;
  *)
    echo "agent-gate: invalid AGENT_GATE_TREE_SELFTEST='${AGENT_GATE_TREE_SELFTEST}' (expected one of: 0 capture clean boundary side terminal postfinalize validate-manifest report-lookup mode-components) (#2926)" >&2
    exit 2 ;;
esac
TREE_SELFTEST_FIXTURE_MARKER=".agent-gate-tree-selftest-fixture"
if [ "${AGENT_GATE_TREE_SELFTEST:-0}" != 0 ]; then
  if [ "$EXPLICIT_SUMMARY_FILE" != 1 ]; then
    echo "agent-gate: AGENT_GATE_TREE_SELFTEST requires an explicit AGENT_GATE_SUMMARY_FILE (refusing to touch the checkout default) (#2926)" >&2
    exit 2
  fi
  # #2926 review B5: ANY mode that can write into the checkout requires a DISPOSABLE
  # fixture, proven by a marker file at the repo root. Checked here, before the sentinel
  # and before any tree work, so a live checkout is refused with nothing written.
  if [ -n "${AGENT_GATE_TREE_SELFTEST_MUTATE:-}" ] || [ "${AGENT_GATE_TREE_SELFTEST_COMMIT:-0}" = 1 ]; then
    if [ ! -f "$REPO_ROOT/$TREE_SELFTEST_FIXTURE_MARKER" ]; then
      echo "agent-gate: AGENT_GATE_TREE_SELFTEST_MUTATE/_COMMIT would mutate $REPO_ROOT, which carries no" >&2
      echo "            $TREE_SELFTEST_FIXTURE_MARKER marker — refusing to write into a live checkout (#2926)" >&2
      exit 2
    fi
  fi
  # shellcheck disable=SC2086  # intentional word-split over the space-separated list
  for _tsp in ${AGENT_GATE_TREE_SELFTEST_MUTATE:-}; do
    case "$_tsp" in
      /*|*..*)
        echo "agent-gate: AGENT_GATE_TREE_SELFTEST_MUTATE must list repo-relative paths (got '$_tsp') (#2926)" >&2
        exit 2 ;;
    esac
    if [ ! -f "$REPO_ROOT/$_tsp" ]; then
      echo "agent-gate: AGENT_GATE_TREE_SELFTEST_MUTATE path '$_tsp' does not exist under $REPO_ROOT (#2926)" >&2
      exit 2
    fi
  done
fi

# ===========================================================================
# TREE IDENTITY (#2926): a gate run whose worktree mutates mid-run SHALL NOT certify.
#
# The `commit:`/`dirty:` stamps are written at SUMMARY-EMIT time and nothing used to
# read tree state at gate START, so a worktree mutated while the gate ran emitted a
# block attributing MIXED-TREE results to the FINAL sha — formally indistinguishable
# from a legitimate certification (field incident 2026-07-26, PR #2916). Three
# components also derive their own SCOPE from git mid-run (file-size's base, --lite's
# blast radius, --delta's fail-closed classification), so a mid-run commit can change
# WHICH tests a run even selects.
#
# Remedy (mirrors the #2874 summary-integrity mechanism exactly — same three hook
# points, same lane-aware marker): capture a TREE IDENTITY at start, re-verify it at
# every `record_result` boundary and once immediately before the terminal emit, and
# FAIL CLOSED on mismatch with a named `tree-integrity: FAIL (tree-mutated-midrun; …)`
# line. There is NO bypass: no environment variable turns a mutated run green.
#
# The identity is a DIGEST OF A PER-PATH CONTENT MANIFEST, never of `git status`
# output: appending to an ALREADY-modified file (the dominant mid-run-fix shape)
# leaves the porcelain listing byte-identical while the content hash moves.
# ===========================================================================

# Per-file content-hash cap for UNTRACKED files (the ONLY knob, and it is a
# PERFORMANCE knob, not a bypass — see the no-bypass contract above). Above the cap an
# untracked file is recorded by size+mtime instead of by content hash, which can only
# WEAKEN detection for one oversized untracked blob; it can never suppress a detected
# mutation, and any use (or any non-default value) is stamped as `tree-hash-cap:`.
#
# The knob is FLOORED at TREE_HASH_CAP_MIN (#2926 review F4). Rejecting only non-numeric
# input accepted `0` and `1`, at which EVERY untracked file — not one oversized blob — is
# recorded by size+mtime, so a same-size content edit that preserves mtime became invisible:
# the header's "can only weaken for one oversized blob" claim was false at a low cap. A
# sub-floor (or unusably large, or non-numeric) value is normalized and the normalization is
# STAMPED, so the weakening is never silent either way.
TREE_HASH_CAP_DEFAULT=8388608
TREE_HASH_CAP_MIN=4096
TREE_HASH_CAP_NOTE=""
TREE_HASH_CAP_BYTES="${AGENT_GATE_TREE_HASH_CAP_BYTES:-$TREE_HASH_CAP_DEFAULT}"
case "$TREE_HASH_CAP_BYTES" in
  ''|*[!0-9]*)
    TREE_HASH_CAP_NOTE="invalid '$TREE_HASH_CAP_BYTES' → default"
    TREE_HASH_CAP_BYTES="$TREE_HASH_CAP_DEFAULT" ;;
esac
# A value too long for shell arithmetic would make the `-lt` below (and `find -size`) error
# out, so it is normalized before any numeric use.
if [ "${#TREE_HASH_CAP_BYTES}" -gt 18 ]; then
  TREE_HASH_CAP_NOTE="out-of-range '$TREE_HASH_CAP_BYTES' → default"
  TREE_HASH_CAP_BYTES="$TREE_HASH_CAP_DEFAULT"
fi
if [ "$TREE_HASH_CAP_BYTES" -lt "$TREE_HASH_CAP_MIN" ]; then
  TREE_HASH_CAP_NOTE="clamped from $TREE_HASH_CAP_BYTES to the ${TREE_HASH_CAP_MIN}-byte floor"
  TREE_HASH_CAP_BYTES="$TREE_HASH_CAP_MIN"
fi
TREE_GUARDED=0            # 1 once a start identity was captured (the guard is live)
# Why the guard is not armed, when it is not (#2926 review F1). Only `no-worktree` — set
# by _tree_capture_start's rc-1 branch — marks a REAL capture attempt that found no git
# worktree; the synthetic emission modes leave it empty so their `selftest` identity is
# never touched by the unguarded-terminal probe.
TREE_UNGUARDED_REASON=""
TREE_MUTATED=0            # 1 once a non-lockfile mid-run mutation was detected
TREE_CAPTURE_FAILED=0     # 1 when a capture ran but could not be validated (fail closed)
TREE_CAPTURE_FAIL_REASON="tree-capture-failed; the tree cannot be proven unchanged"
TREE_MARKER_SEEN=0        # 1 once a SIDE-lane marker has been consumed
TREE_START_HEAD=""; TREE_START_DIRTY=""; TREE_START_DIGEST=""
TREE_END_HEAD=""; TREE_END_DIRTY=""; TREE_END_DIGEST=""
TREE_START_BRANCH=""      # the branch name read ONCE, at the start of the guarded window
# Which capture the block's `commit:` line names (#2926 review H2):
#   end   — the VERIFIED TERMINAL capture (the default, and the C1 property this change
#           exists to establish: a block may only name a sha some capture validated).
#   start — a MAIN-lane boundary detection. The run EXECUTED against the START identity;
#           the post-mutation identity is merely what the guard OBSERVED when it stopped,
#           and naming it on `commit:` would make the failure artifact — the first thing a
#           triager reads — stamp a sha the run never ran against, the exact pattern this
#           change forbids. So a mutation-detected block names the verified start and says
#           so, and the post-mutation identity stays on the labelled `tree-end:` line.
#
# `start` is set from exactly ONE place — _tree_label_post_mutation, the shared labelling
# every mutation-detection path calls (#2926 review J1). It used to be set on the boundary
# path only, which left the TERMINAL path stamping an unlabelled post-mutation sha.
TREE_COMMIT_SOURCE=end
# The `tree-end:` suffix that marks a post-mutation observation. CONTRACT TEXT pinned in
# openspec/changes/gate-tree-integrity/specs/gate-tree-integrity/spec.md — one definition,
# so every detection path publishes the identical wording.
TREE_POST_MUTATION_SUFFIX="(POST-MUTATION observation — NOT the identity this run executed against)"
# The count of untracked files recorded by size+mtime instead of by content hash. It is
# the MAX over the run's captures, never their SUM (#2926 review): the start and the
# terminal capture both count the SAME oversized file, so summing reported "2 untracked
# file(s)" for one file present all run.
TREE_CAP_FALLBACKS=0
TREE_START_LINE="tree-start: (not captured)"
TREE_END_LINE="tree-end: (not captured)"
TREE_INTEGRITY_LINE="tree-integrity: SKIP (no capture)"
TREE_HASH_CAP_LINE=""
declare -a TREE_META_LINES=()

# Exclusions are the repo's OWN ignore rules (`--exclude-standard`) plus exactly ONE
# explicit carve-out: the run's own summary file (and its `.integrity-fail.*` siblings)
# when the caller pinned a RELATIVE, in-repo path — a file this gate writes twice by
# contract. Nothing else is excluded: an over-broad exclusion re-opens the hole, so
# docs/**, *.md, test-data/** and openspec/** all stay INSIDE the digest.
# (Cargo.lock is NOT excluded — it is a named non-fatal class, see _tree_change_class.)
#
# The carve-out compares CANONICAL forms (#2926 review B3): git always reports
# normalized repo-root-relative paths, so a raw prefix strip of a caller-pinned path
# that is not already canonical (`./x.txt`, `sub/../x.txt`, a symlinked path, an
# absolute path through a symlinked root) would fail to match and the run's OWN summary
# file — written twice by contract, and written for the FIRST time only AFTER the start
# capture — would look like a mid-run mutation on every such run.
#
# _tree_canon_rel <path> -> the repo-root-relative NORMALIZED path on stdout; rc 1 when
# the path does not resolve to a location under the repo root. The directory is resolved
# with `cd … && pwd -P` (which normalizes `.`, `..`, duplicate slashes and symlinked
# directory components) and the final component is appended verbatim, so a symlinked
# FILE is compared under the name git knows it by.
#
# A NOT-YET-CREATED parent directory must still canonicalize (#2926 review): `cd` on a
# missing directory fails, and returning 1 there silently DISARMS the carve-out — today
# only benignly (the summary write into that missing directory also fails), but the day
# anything `mkdir -p`s the parent it becomes a guaranteed false FAIL. So we resolve the
# NEAREST EXISTING ancestor physically and re-append the missing components verbatim.
# A `..` inside the missing part cannot be resolved without guessing, so it is refused.
_tree_canon_rel() {
  local p="$1" d b phys rest="" comp
  [ -n "$p" ] || return 1
  d=$(dirname -- "$p") || return 1
  b=$(basename -- "$p") || return 1
  case "$b" in ''|.|..) return 1 ;; esac
  while :; do
    if phys=$(cd "$d" 2>/dev/null && pwd -P); then break; fi
    case "$d" in /|.|..|'') return 1 ;; esac   # nothing left to walk up to
    comp=$(basename -- "$d") || return 1
    case "$comp" in ..) return 1 ;; .) ;; *) rest="$comp${rest:+/$rest}" ;; esac
    d=$(dirname -- "$d") || return 1
  done
  d="$phys${rest:+/$rest}"
  case "$d" in
    "$TREE_REPO_ROOT_PHYS")   printf '%s\n' "$b" ;;
    "$TREE_REPO_ROOT_PHYS"/*) printf '%s/%s\n' "${d#"$TREE_REPO_ROOT_PHYS"/}" "$b" ;;
    *) return 1 ;;
  esac
}
TREE_REPO_ROOT_PHYS=$(cd "$REPO_ROOT" 2>/dev/null && pwd -P) || TREE_REPO_ROOT_PHYS="$REPO_ROOT"
TREE_EXCLUDE_REL="$(_tree_canon_rel "$SUMMARY_FILE" || true)"

# …plus the run's OWN stdout/stderr redirect target (#2926 review J3). The DOCUMENTED
# invocation is `bash scripts/agent-gate.sh > gate.log 2>&1`, and `.gitignore` covers
# `*.log`, so the default is already outside the digest — but a caller redirecting to a
# NON-ignored in-repo path makes the gate trip on the log it is itself writing and report a
# mid-run mutation whose named path is its own output. That is the run's own artifact,
# exactly like the summary file, so it is carved out the same way — and NOTHING wider: only
# the two fds this process holds, only when each resolves to a REGULAR FILE under the repo
# root. An untracked file anything else creates mid-run stays fatal.
#
# `$$` is used deliberately, never `/proc/self`: this runs inside a `$( … )`, where `self`
# is the SUBSHELL whose fd 1 is the substitution pipe. Where /proc is absent (macOS/BSD — a
# first-class gate host) the fd cannot be named at all; the guard then stays fully armed and
# _tree_fail_reason appends TREE_FD_HINT so a reader is told the real cause instead of the
# gate excluding something on a guess.
TREE_STDOUT_REL=""
TREE_STDERR_REL=""
TREE_FD_HINT=""
_tree_fd_target_rel() {   # <fd> -> repo-relative path of that fd's REGULAR-FILE target
  local link
  link=$(readlink "/proc/$$/fd/$1" 2>/dev/null) || return 1
  [ -n "$link" ] || return 1
  [ -f "$link" ] || return 1
  _tree_canon_rel "$link"
}
if [ -e "/proc/$$/fd/1" ] || [ -L "/proc/$$/fd/1" ]; then
  TREE_STDOUT_REL="$(_tree_fd_target_rel 1 || true)"
  TREE_STDERR_REL="$(_tree_fd_target_rel 2 || true)"
else
  TREE_FD_HINT=" (note: this host cannot name the run's own stdout/stderr redirect target, so if you redirected this run's output to a non-ignored path INSIDE the checkout, that file is the change)"
fi

_tree_excluded() {
  case "$1" in
    "") return 1 ;;
  esac
  if [ -n "$TREE_EXCLUDE_REL" ]; then
    case "$1" in
      "$TREE_EXCLUDE_REL") return 0 ;;
      "$TREE_EXCLUDE_REL".integrity-fail.*) return 0 ;;
    esac
  fi
  if [ -n "$TREE_STDOUT_REL" ] && [ "$1" = "$TREE_STDOUT_REL" ]; then return 0; fi
  if [ -n "$TREE_STDERR_REL" ] && [ "$1" = "$TREE_STDERR_REL" ]; then return 0; fi
  return 1
}

# _tree_digest_file <file> -> a content digest of <file>. sha256 where available;
# `git hash-object` is the last resort so the guard can NEVER go inert for want of a
# hashing tool (git is already a hard dependency of every gate mode).
#
# The tool/stat probes are memoized in TREE_SHA_TOOL/TREE_STAT_FLAVOR, which are set by
# _tree_probe_tools ONCE at script level. Probing lazily inside these helpers memoized
# NOTHING (#2926 review): both helpers only ever run inside a command substitution
# (`$(_tree_identity …)`, `$(_tree_digest_file …)`), whose assignments die with the
# subshell — so the `command -v`/`stat` probes re-ran per capture and per file.
TREE_SHA_TOOL=""
TREE_STAT_FLAVOR=""
# The mtime RESOLUTION _tree_mtime can actually record on this host: ns | s | none
# (#2926 review H5). It is a DISCLOSED property, not an internal detail: the size+mtime
# fallback is only as strong as its clock, so a host that can offer whole seconds only
# cannot see a same-size rewrite that lands inside one second — a real, platform-specific
# weakening of a correctness guard. _tree_cap_stamp publishes it whenever the fallback is
# actually in use, so the artifact states the guarantee that host gave rather than
# implying parity with the nanosecond hosts.
TREE_MTIME_RES=none
TREE_SORT0_OK=0
_tree_probe_tools() {
  if command -v sha256sum >/dev/null 2>&1; then TREE_SHA_TOOL=sha256sum
  elif command -v shasum >/dev/null 2>&1; then TREE_SHA_TOOL=shasum
  else TREE_SHA_TOOL=git; fi
  local frac
  if stat -c %Y . >/dev/null 2>&1; then TREE_STAT_FLAVOR=gnu; TREE_MTIME_RES=ns
  elif stat -f %m . >/dev/null 2>&1; then
    # BSD/macOS. `%m` is whole seconds; the newer `%Fm` datum prints FRACTIONAL seconds,
    # so where this stat offers it the resolution gap against GNU's `%.9Y` simply closes.
    # Probed (never assumed) and validated on the OUTPUT: an older stat that does not know
    # the datum either errors or echoes it back, and neither looks like `<digits>.<digits>`.
    TREE_STAT_FLAVOR=bsd; TREE_MTIME_RES=s
    frac=$(stat -f '%Fm' . 2>/dev/null) || frac=""
    case "$frac" in
      *[0-9].[0-9]*) TREE_STAT_FLAVOR=bsd-frac; TREE_MTIME_RES=ns ;;
    esac
  else TREE_STAT_FLAVOR=none; TREE_MTIME_RES=none; fi
  # `sort -z` is NOT universal (#2926 review G1 sweep). An unsupported flag makes sort
  # print usage and emit NOTHING, and the capture's `… | LC_ALL=C sort -z` feeds a
  # `while read -r -d ''` loop — so the manifest would come back EMPTY on a dirty tree
  # and BOTH captures would agree: a silent FAIL-OPEN, the worst possible direction.
  # Probe once; fall back to git's own ordering when the flag is unavailable.
  if printf 'b\0a\0' | LC_ALL=C sort -z >/dev/null 2>&1; then TREE_SORT0_OK=1; else TREE_SORT0_OK=0; fi
}
_tree_probe_tools

# _tree_sort0: NUL-framed sort of stdin, with a portable fallback. The fallback is not a
# weakening: `git ls-files -z` and `git diff --name-only -z` already emit paths in a
# deterministic, path-sorted order, and BOTH captures of a run take the same route — the
# explicit sort only removes the dependency on that git property. What must never happen
# is a sort that silently drops every path (see the probe above).
_tree_sort0() {
  if [ "$TREE_SORT0_OK" = 1 ]; then LC_ALL=C sort -z; else cat; fi
}

_tree_digest_file() {
  case "$TREE_SHA_TOOL" in
    sha256sum) sha256sum < "$1" | awk '{print $1}' ;;
    shasum)    shasum -a 256 < "$1" | awk '{print $1}' ;;
    *)         git --no-optional-locks hash-object --no-filters --stdin < "$1" ;;
  esac
}

# _tree_hex_id_ok <id>: rc 0 iff <id> is a FULL-LENGTH lowercase hex object id — 40 chars
# in a SHA-1 repository, 64 in a SHA-256 one. ONE rule, used by both callers (#2926 review
# G5): _tree_digest_ok below, and the lockfile carve-out's "the END value is a real blob
# id" test, which used to hard-code 40 and therefore never admitted on a SHA-256 repo.
_tree_hex_id_ok() {
  case "$1" in ''|*[!0-9a-f]*) return 1 ;; esac
  case "${#1}" in 40|64) return 0 ;; esac
  return 1
}

# _tree_digest_ok <digest>: a capture digest is USABLE only when it is a full-length
# lowercase hex hash. #2926 review B1: _tree_identity used to print whatever
# _tree_digest_file produced — including NOTHING when the hash tool exited non-zero or
# the manifest write to $LOG_DIR failed (ENOSPC on a 40-60 min run) — and rc 0. The
# empty field then collapsed under `IFS=$'\t' read` and the digest-only comparison
# matched, stamping `tree-integrity: PASS` on a demonstrably mutated tree. A digest that
# cannot be validated is now a FAIL-CLOSED condition, never a comparison input.
# sha256sum/shasum produce 64 hex chars; the `git hash-object` last resort produces 40
# (sha1 repo) or 64 (sha256 repo).
_tree_digest_ok() {
  _tree_hex_id_ok "$1" || return 1
  # 40 chars can ONLY be the `git hash-object` last resort against a SHA-1 repo:
  # sha256sum/shasum always produce 64, so a 40-char digest from those is a short read.
  if [ "${#1}" -eq 40 ] && [ "$TREE_SHA_TOOL" != git ]; then return 1; fi
  return 0
}

# _tree_split_identity <line>: split "<head>\t<dirty>\t<digest>\t<fallbacks>" into
# TREE_F_HEAD/TREE_F_DIRTY/TREE_F_DIGEST/TREE_F_FB and VALIDATE every field. rc 1 when
# the line is malformed or any field fails validation.
#
# `IFS=$'\t' read -r a b c d` is NOT used (#2926 review B1): tab is IFS *whitespace*, so
# read collapses runs of it and an empty field silently shifts later fields left —
# `head<TAB>dirty<TAB><TAB>0` bound the digest to the fallbacks value `0`. The explicit
# %%/# split below preserves empty fields exactly.
TREE_F_HEAD=""; TREE_F_DIRTY=""; TREE_F_DIGEST=""; TREE_F_FB=""
_tree_split_identity() {
  local line="$1" tab=$'\t' rest
  TREE_F_HEAD=""; TREE_F_DIRTY=""; TREE_F_DIGEST=""; TREE_F_FB=""
  case "$line" in *"$tab"*"$tab"*"$tab"*) ;; *) return 1 ;; esac
  TREE_F_HEAD="${line%%$tab*}";   rest="${line#*$tab}"
  TREE_F_DIRTY="${rest%%$tab*}";  rest="${rest#*$tab}"
  TREE_F_DIGEST="${rest%%$tab*}"; TREE_F_FB="${rest#*$tab}"
  [ -n "$TREE_F_HEAD" ] || return 1
  case "$TREE_F_DIRTY" in yes|no) ;; *) return 1 ;; esac
  case "$TREE_F_FB" in ''|*[!0-9]*) return 1 ;; esac
  _tree_digest_ok "$TREE_F_DIGEST" || return 1
  return 0
}

_tree_short() { printf '%.12s' "${1:-?}"; }

# _tree_manifest_ok <file> <nul|nl> <head> <body-count>: rc 0 iff <file> is a COMPLETE
# manifest — first record `H<TAB><head>`, last record `N<TAB><body-count>`, and exactly
# <body-count> records between them. #2926 review C2: checking only the first record
# accepted an ENOSPC/partial write, and two manifests sharing a byte-identical prefix
# then compared EQUAL while the tree had genuinely moved.
_tree_manifest_ok() {
  local f="$1" framing="$2" head="$3" want="$4" tab=$'\t'
  local rec first="" last="" seen=0
  [ -f "$f" ] || return 1
  if [ "$framing" = nul ]; then
    while IFS= read -r -d '' rec; do
      [ "$seen" -eq 0 ] && first="$rec"
      last="$rec"; seen=$(( seen + 1 ))
    done < "$f"
  else
    while IFS= read -r rec; do
      [ "$seen" -eq 0 ] && first="$rec"
      last="$rec"; seen=$(( seen + 1 ))
    done < "$f"
  fi
  [ "$first" = "H${tab}${head}" ] || return 1
  [ "$last" = "N${tab}${want}" ] || return 1
  [ "$seen" -eq $(( want + 2 )) ] || return 1
  return 0
}

# _tree_identity <manifest-out>
#
# Write the NUL-framed per-path manifest to <manifest-out> (plus an escaped,
# newline-framed view at <manifest-out>.report used ONLY to name changed paths on
# failure) and print "<head>\t<dirty>\t<digest>\t<cap-fallbacks>".
#   rc 0 — a VALIDATED identity was printed
#   rc 1 — git cannot be consulted at all (no worktree): the guard SKIPs
#   rc 2 — the capture ran but could not be validated (hash tool failed, manifest write
#          failed/truncated, first record not `H<TAB><head>`): FAIL CLOSED. #2926 review
#          B1 — a capture that cannot be trusted must never reach the comparison.
#
# Manifest records (NUL-TERMINATED, path LAST so a path containing a tab or a newline
# cannot forge a field or a record):
#   H<TAB><head-sha|unborn>
#   T<TAB><blob-sha|DELETED|NONFILE|LINK:target|SIZE:n:MTIME:t><TAB><mode><TAB><path>
#   U<TAB>… same shape, for untracked non-ignored paths
#   N<TAB><body-record-count>          <- the TRAILER, written last
#
# The trailer is what makes TRUNCATION detectable (#2926 review C2). Validating only the
# FIRST record left an ENOSPC truncation AFTER the `H` record comparing EQUAL to a start
# manifest sharing that byte-identical prefix, so a mutation to a later-sorted path
# passed. A manifest is now accepted only when its first record is the `H` header, its
# LAST record is the `N` trailer, and the trailer's count equals the body records
# actually read back — a short write can no longer be mistaken for a shorter tree.
#
# Side-effect freedom (a guard that mutates the repo to check the repo did not mutate
# is the wrong trade): EVERY git call passes --no-optional-locks so nothing refreshes/
# rewrites $GIT_DIR/index (which also makes it safe to call from the ~8 concurrent
# SIDE-lane subshells), and `git hash-object` runs WITHOUT -w so hashes are computed
# and NOTHING is written to the object database (git worktrees SHARE the ODB with the
# root checkout). No temporary index, no `git add`, no working-tree write; the only
# files created live under $LOG_DIR (a per-run mktemp dir outside the repo).
_tree_identity() {
  local out="$1"
  local head dirty=no nl=$'\n'
  head=$(git --no-optional-locks rev-parse HEAD 2>/dev/null) || head=""
  if [ -z "$head" ]; then
    git --no-optional-locks rev-parse --git-dir >/dev/null 2>&1 || return 1
    head="unborn"
  fi

  local p
  local -a tpaths=() upaths=()
  # Tracked side: every path differing from HEAD in the INDEX or the WORKING TREE
  # (content, mode, add, delete). --no-renames so a rename shows as delete+add rather
  # than collapsing to the new name only.
  while IFS= read -r -d '' p; do
    _tree_excluded "$p" || tpaths+=("$p")
  done < <(
    if [ "$head" = unborn ]; then
      git --no-optional-locks ls-files -z
    else
      git --no-optional-locks diff --name-only -z --no-renames HEAD --
    fi | _tree_sort0
  )
  # Untracked side: --exclude-standard is what makes .gitignore the exclusion set.
  while IFS= read -r -d '' p; do
    _tree_excluded "$p" || upaths+=("$p")
  done < <(git --no-optional-locks ls-files --others --exclude-standard -z | _tree_sort0)

  # Oversized UNTRACKED files (one batched find, never a fork per file).
  #
  # Only PLAIN, non-symlink untracked paths are handed to find, and that filter is what
  # makes `bigpaths` an ORDER-PRESERVING SUBSEQUENCE of `upaths`: POSIX find processes its
  # operands in order and a non-directory operand yields at most itself, so the output is
  # the given order with the under-cap paths removed. `git ls-files --others` CAN emit a
  # directory entry (an embedded git repo is listed as `dir/`), which find would recurse
  # into and report paths that are not in `upaths` at all — breaking the subsequence
  # property. Those entries could never match the membership test anyway (the per-path
  # loop below reaches it only under `-f`), so dropping them here changes no record.
  #
  # The subsequence property is what lets the loop test membership with a single forward
  # CURSOR instead of a linear scan (#2926 review K2): membership was O(#untracked ×
  # #oversized-untracked) inside a capture that runs at every component boundary, in every
  # backgrounded SIDE-lane subshell and at the terminal, so the scan cost multiplied.
  local -a bigpaths=() probe=()
  for p in ${upaths[@]+"${upaths[@]}"}; do
    if [ ! -L "$p" ] && [ -f "$p" ]; then probe+=("$p"); fi
  done
  if [ "${#probe[@]}" -gt 0 ]; then
    # `xargs -0 find …` would append the paths AFTER the expression (find requires them
    # BEFORE it), so the paths are placed explicitly via `sh -c … "$@"`.
    while IFS= read -r -d '' p; do bigpaths+=("$p"); done < <(
      printf '%s\0' "${probe[@]}" \
        | TREE_CAP="$TREE_HASH_CAP_BYTES" xargs -0 \
            sh -c 'find -H "$@" -size "+${TREE_CAP}c" -type f -print0 2>/dev/null' sh
    )
  fi

  local -a tags=() vals=() modes=() paths=() batch=()
  local fallbacks=0 tag mode value h isbig
  local bi=0 nbig="${#bigpaths[@]}"
  local -a src=()
  for tag in T U; do
    if [ "$tag" = T ]; then src=(${tpaths[@]+"${tpaths[@]}"}); else src=(${upaths[@]+"${upaths[@]}"}); fi
    for p in ${src[@]+"${src[@]}"}; do
      # The cursor advances at EVERY untracked path, before any branch: a path that was
      # oversized at the find but has since been deleted (or turned into a symlink) still
      # consumes its entry, so the two walks stay in step and a later oversized file is
      # not missed. One string comparison per path, O(1) amortised.
      isbig=0
      if [ "$tag" = U ] && [ "$bi" -lt "$nbig" ] && [ "${bigpaths[$bi]}" = "$p" ]; then
        isbig=1; bi=$(( bi + 1 ))
      fi
      if [ -L "$p" ]; then
        # A symlink's git blob IS its target; never follow it (a dangling link would
        # abort the whole hash-object batch).
        mode=120000; value="LINK:$(readlink "$p" 2>/dev/null || echo '?')"
      elif [ -f "$p" ]; then
        if [ -x "$p" ]; then mode=100755; else mode=100644; fi
        if [ "$isbig" -eq 1 ]; then
          value="SIZE:$(wc -c < "$p" 2>/dev/null | tr -d ' '):MTIME:$(_tree_mtime "$p")"
          fallbacks=$(( fallbacks + 1 ))
        else
          case "$p" in
            *"$nl"*)
              # --stdin-paths is newline-delimited, so a path containing a newline is
              # hashed on its own rather than corrupting the batch.
              value=$(git --no-optional-locks hash-object --no-filters -- "$p" 2>/dev/null) || value=""
              [ -n "$value" ] || value="UNHASHABLE" ;;
            *) value="@H@"; batch+=("$p") ;;
          esac
        fi
      elif [ -e "$p" ]; then
        mode=none; value=NONFILE          # directory / submodule / fifo
      else
        mode=none; value=DELETED
      fi
      tags+=("$tag"); vals+=("$value"); modes+=("$mode"); paths+=("$p")
    done
  done

  # ONE batched `git hash-object --stdin-paths` (no -w) for every ordinary file.
  local -a hashes=()
  if [ "${#batch[@]}" -gt 0 ]; then
    printf '%s\n' "${batch[@]}" \
      | git --no-optional-locks hash-object --no-filters --stdin-paths > "$out.hashes" 2>/dev/null
    while IFS= read -r h; do hashes+=("$h"); done < "$out.hashes"
    rm -f "$out.hashes" 2>/dev/null || true
    if [ "${#hashes[@]}" -ne "${#batch[@]}" ]; then
      # The batch aborted (unreadable file, …). Re-hash per file rather than emit a
      # SHORT manifest — a short manifest could mask a mutation.
      hashes=()
      for p in "${batch[@]}"; do
        h=$(git --no-optional-locks hash-object --no-filters -- "$p" 2>/dev/null) || h=""
        [ -n "$h" ] || h="UNHASHABLE"
        hashes+=("$h")
      done
    fi
  fi

  # The `.report` view is TAB-DELIMITED and parsed with `awk -F'\t'` ($4 = path), so a
  # TAB inside a path (or inside a LINK: target) must be escaped exactly like a newline
  # (#2926 review B6): an unescaped tab truncated $4, which both named the wrong path in
  # the failure line AND fed the Cargo.lock classifier a fragment, so the non-fatal
  # lockfile carve-out could misfire on a path that merely LOOKS like a lockfile.
  #
  # The BACKSLASH is escaped FIRST, and it is what makes the family INJECTIVE (#2926
  # review K1): without it a path literally containing the two characters `\` `n` and a
  # path containing a real newline produced the SAME record, so the escaping that exists
  # to disambiguate could itself be forged. One family, decoded left to right:
  #   `\\` = a literal backslash, `\n` = newline, `\t` = tab
  # and _tree_render_path adds the fourth member (`\s` = space) at RENDER time, where the
  # space is the character that would forge a list boundary.
  local i k=0 esc escv tab=$'\t'
  {
    printf 'H\t%s\0' "$head"
    printf 'H\t%s\n' "$head" >&3
    for (( i = 0; i < ${#paths[@]}; i++ )); do
      value="${vals[$i]}"
      if [ "$value" = "@H@" ]; then value="${hashes[$k]}"; k=$(( k + 1 )); fi
      printf '%s\t%s\t%s\t%s\0' "${tags[$i]}" "$value" "${modes[$i]}" "${paths[$i]}"
      esc=${paths[$i]//\\/\\\\};  esc=${esc//$nl/\\n};   esc=${esc//$tab/\\t}
      escv=${value//\\/\\\\};     escv=${escv//$nl/\\n}; escv=${escv//$tab/\\t}
      printf '%s\t%s\t%s\t%s\n' "${tags[$i]}" "$escv" "${modes[$i]}" "$esc" >&3
    done
    printf 'N\t%s\0' "${#paths[@]}"
    printf 'N\t%s\n' "${#paths[@]}" >&3
  } > "$out" 3> "$out.report"

  [ "${#paths[@]}" -gt 0 ] && dirty=yes

  # Validate our OWN output before anyone can compare it (#2926 review B1/C2): header,
  # trailer and body count, on BOTH views. A truncated manifest is rejected here.
  local digest
  _tree_manifest_ok "$out" nul "$head" "${#paths[@]}" || return 2
  _tree_manifest_ok "$out.report" nl "$head" "${#paths[@]}" || return 2
  digest=$(_tree_digest_file "$out") || return 2
  _tree_digest_ok "$digest" || return 2
  printf '%s\t%s\t%s\t%s\n' "$head" "$dirty" "$digest" "$fallbacks"
}

# _tree_mtime <path> -> mtime (sub-second where the platform's stat offers it, else whole
# seconds). TREE_STAT_FLAVOR/TREE_MTIME_RES are probed once by _tree_probe_tools (above);
# whenever the resolution is coarser than nanoseconds the cap line SAYS SO (#2926 H5).
_tree_mtime() {
  case "$TREE_STAT_FLAVOR" in
    gnu)      stat -c '%.9Y' -- "$1" 2>/dev/null || echo unknown ;;
    bsd-frac) stat -f '%Fm'  -- "$1" 2>/dev/null || echo unknown ;;
    bsd)      stat -f '%m'   -- "$1" 2>/dev/null || echo unknown ;;
    *)        echo unknown ;;
  esac
}

# _tree_cap_note <count>: fold one capture's fallback count into the reported figure.
# MAX, never a running sum (#2926 review): the start and the terminal capture each count
# the SAME oversized untracked file, so summing double-reported it. The max also keeps
# the disclosure when the file existed at only one of the two captures.
_tree_cap_note() {
  case "$1" in ''|*[!0-9]*) return 0 ;; esac
  [ "$1" -gt "${TREE_CAP_FALLBACKS:-0}" ] && TREE_CAP_FALLBACKS="$1"
  return 0
}

# _tree_cap_stamp: stamp `tree-hash-cap:` when the knob is non-default, when it was
# NORMALIZED (clamped/rejected — #2926 review F4), or when the size+mtime fallback was
# actually used, so neither a weakened capture nor a rejected knob value is ever invisible.
#
# It must also CLEAR a line that no longer applies (#2926 review H1). The full gate
# RE-captures at the slot grant, and _tree_capture_start resets TREE_CAP_FALLBACKS to 0
# before re-noting: if the pre-queue capture engaged the fallback and the authoritative
# re-capture does not, a set-only stamp left the OLD line standing and the block advertised
# a weakened capture that is not in force. A stale disclosure is a false statement about
# the run, so the else-branch is part of the contract, not tidiness.
#
# When the fallback IS in use, the record is only as good as the host's mtime resolution,
# so a coarser-than-nanosecond clock is disclosed in the same line (#2926 review H5).
_tree_cap_stamp() {
  local mres=""
  if [ "${TREE_CAP_FALLBACKS:-0}" -gt 0 ]; then
    case "$TREE_MTIME_RES" in
      s)    mres="; mtime resolution: WHOLE SECONDS on this host — a same-size rewrite within one second is NOT detected" ;;
      none) mres="; mtime resolution: UNAVAILABLE on this host — those records are size-only" ;;
    esac
  fi
  if [ "$TREE_HASH_CAP_BYTES" != "$TREE_HASH_CAP_DEFAULT" ] || [ -n "$TREE_HASH_CAP_NOTE" ] \
     || [ "${TREE_CAP_FALLBACKS:-0}" -gt 0 ]; then
    TREE_HASH_CAP_LINE="tree-hash-cap: $TREE_HASH_CAP_BYTES bytes${TREE_HASH_CAP_NOTE:+ ($TREE_HASH_CAP_NOTE)} (${TREE_CAP_FALLBACKS:-0} untracked file(s) recorded by size+mtime$mres)"
  else
    TREE_HASH_CAP_LINE=""
  fi
}

# _tree_capture_start: the start capture. Runs after summary-path resolution and BEFORE
# the startup sentinel, run_lite, run_delta and acquire_gate_slot, so every guarded mode
# is covered; the full gate RE-captures once its slot is granted (_tree_recapture_after_slot).
_tree_capture_start() {
  local id rc
  id=$(_tree_identity "$LOG_DIR/tree-identity.start"); rc=$?
  if [ "$rc" -eq 1 ]; then
    TREE_GUARDED=0
    TREE_UNGUARDED_REASON=no-worktree
    TREE_CAPTURE_FAILED=0
    TREE_START_LINE="tree-start: (capture unavailable — no git worktree)"
    TREE_END_LINE="tree-end: (capture unavailable — no git worktree)"
    TREE_INTEGRITY_LINE="tree-integrity: SKIP (capture unavailable — no git worktree)"
    return 0
  fi
  # rc 2 (or a field that fails validation): the capture RAN but cannot be trusted. The
  # tree can never be proven unchanged from here, so the run is doomed to FAIL — it is
  # NOT downgraded to SKIP, which is reserved for "there is no git worktree" (#2926 B1).
  if [ "$rc" -ne 0 ] || ! _tree_split_identity "$id"; then
    TREE_GUARDED=1
    TREE_CAPTURE_FAILED=1
    TREE_START_HEAD=""; TREE_START_DIRTY=""; TREE_START_DIGEST=""
    TREE_START_LINE="tree-start: (capture failed — identity could not be validated)"
    TREE_END_LINE="tree-end: (not captured)"
    TREE_INTEGRITY_LINE="tree-integrity: FAIL ($TREE_CAPTURE_FAIL_REASON)"
    echo "⚠️ agent-gate: tree-integrity start capture could not be validated — failing closed (#2926)" >&2
    return 0
  fi
  TREE_CAPTURE_FAILED=0
  TREE_UNGUARDED_REASON=""
  TREE_START_HEAD="$TREE_F_HEAD"; TREE_START_DIRTY="$TREE_F_DIRTY"
  TREE_START_DIGEST="$TREE_F_DIGEST"
  TREE_CAP_FALLBACKS=0; _tree_cap_note "$TREE_F_FB"
  # The branch NAME is read ONCE, here, at the start of the guarded window — the emitted
  # `commit:` line must not depend on a fresh git call at emit time (#2926 review C1).
  TREE_START_BRANCH=$(git --no-optional-locks rev-parse --abbrev-ref HEAD 2>/dev/null) \
    || TREE_START_BRANCH=""
  [ -n "$TREE_START_BRANCH" ] || TREE_START_BRANCH=unknown
  TREE_GUARDED=1
  TREE_START_LINE="tree-start: $(_tree_short "$TREE_START_HEAD") dirty: $TREE_START_DIRTY digest: $(_tree_short "$TREE_START_DIGEST")"
  TREE_END_LINE="tree-end: (not captured)"
  TREE_INTEGRITY_LINE="tree-integrity: PENDING"
  _tree_cap_stamp
}

# _tree_recapture_after_slot: the FULL gate's authoritative start capture (#2926 review
# B4). With CQLITE_GATE_MAX_CONCURRENCY pinned to 1, `acquire_gate_slot` can block for
# the length of another 20-25 minute run; during that queue the gate has executed
# NOTHING and certifies NOTHING, so a tree edit while queued must not invalidate a run
# that has not started. The certification window begins when WORK begins: re-capture
# here, immediately after the slot is granted. --lite/--delta exit before that call site
# (and --only self-exempts inside acquire_gate_slot but still passes through it), so
# those modes simply certify from the capture that precedes their own first component.
# The startup sentinel deliberately keeps the EARLY tree-start it was written with — it
# records the tree the process began on; the emitted block records the guarded window.
#
# A TRANSIENT git failure SHALL NOT disarm the guard — in EITHER direction (#2926 review
# C3 + F1). `_tree_capture_start`'s rc-1 branch ("no git worktree") sets TREE_GUARDED=0,
# and a `git rev-parse --git-dir` blip (a concurrent prune/gc, a stuttering network mount)
# is indistinguishable from a genuinely non-git tree at the moment it happens. Both blip
# positions are handled here, conservatively:
#
#   * blip at the RE-CAPTURE (a live guard, C3): the pre-queue capture is snapshotted
#     first and RESTORED, so the run stays guarded and certifies against the older
#     (strictly wider) window.
#   * blip at the FIRST capture (an unarmed guard, F1): the mirror image, and the more
#     dangerous one — second 0 of the process is exactly when a `git gc`/prune is likeliest
#     and a single failure there used to yield `tree-integrity: SKIP` + `RESULT: PASS` for
#     the WHOLE run. So the capture is RE-ATTEMPTED here and the guard is ARMED on success.
#     A genuinely non-git tree simply fails the re-attempt and stays SKIP, so the spec'd
#     no-worktree SKIP contract is unchanged; only a transient failure is recovered.
#
# rc 2 at the RE-capture (a live guard) is treated EXACTLY like the rc-1 blip above
# (#2926 review G4): "the capture ran but could not be validated" is a transient tool/disk
# failure, and a FULLY VALIDATED pre-queue identity is sitting in the globals and on disk,
# so failing the run there is a spurious FAIL, not a safety property — the guard restores
# the pre-queue capture and certifies against the strictly WIDER window, as C3 does.
# Restoring the GLOBALS alone is not enough: unlike rc 1 (which returns before writing
# anything), an rc-2 re-capture has already OVERWRITTEN $LOG_DIR/tree-identity.start[.report]
# with a possibly-truncated manifest that every later comparison reads. So the pre-queue
# manifest is snapshotted BEFORE the re-capture and restored WITH the globals; if that
# snapshot/restore itself fails there is nothing trustworthy to fall back to and the run
# stays FAIL-CLOSED.
# rc 2 at the FIRST capture is different and still fails closed: there is no validated
# identity to fall back to, and `git rev-parse --git-dir` succeeded, so a git worktree
# demonstrably exists (a genuinely non-git tree yields rc 1 and stays SKIP).
_tree_recapture_after_slot() {
  [ "$TREE_CAPTURE_FAILED" -eq 1 ] && return 0
  if [ "$TREE_GUARDED" -ne 1 ]; then
    # F1: the first capture found no worktree. Re-attempt; a blip recovers, a real
    # non-git tree does not (it fails the same way twice and stays SKIP).
    [ "$TREE_UNGUARDED_REASON" = no-worktree ] || return 0
    _tree_capture_start
    if [ "$TREE_GUARDED" -eq 1 ] && [ "$TREE_CAPTURE_FAILED" -ne 1 ]; then
      TREE_START_LINE="$TREE_START_LINE (captured at the slot grant — the first capture found no git worktree)"
      echo "⚠️ agent-gate: tree-integrity start capture was unavailable at process start but SUCCEEDED at the slot grant — the first failure was transient, guard ARMED (#2926)" >&2
    fi
    return 0
  fi
  local s_head="$TREE_START_HEAD" s_dirty="$TREE_START_DIRTY" s_digest="$TREE_START_DIGEST"
  local s_branch="$TREE_START_BRANCH" s_fb="$TREE_CAP_FALLBACKS"
  local s_start="$TREE_START_LINE" s_end="$TREE_END_LINE" s_int="$TREE_INTEGRITY_LINE"
  local s_cap="$TREE_HASH_CAP_LINE"
  local snap="$LOG_DIR/tree-identity.prequeue" snap_ok=0
  if cp "$LOG_DIR/tree-identity.start" "$snap" 2>/dev/null \
     && cp "$LOG_DIR/tree-identity.start.report" "$snap.report" 2>/dev/null; then
    snap_ok=1
  fi
  _tree_capture_start
  local why=""
  if [ "$TREE_CAPTURE_FAILED" -eq 1 ]; then
    # rc 2: the manifest on disk is now untrustworthy — restore it or stay failed closed.
    if [ "$snap_ok" -eq 1 ] \
       && cp "$snap" "$LOG_DIR/tree-identity.start" 2>/dev/null \
       && cp "$snap.report" "$LOG_DIR/tree-identity.start.report" 2>/dev/null; then
      why="could not be validated"
    else
      echo "⚠️ agent-gate: tree-integrity re-capture at the slot grant could not be validated AND the pre-queue capture could not be restored — failing closed (#2926)" >&2
    fi
  elif [ "$TREE_GUARDED" -ne 1 ]; then
    # rc 1: _tree_identity returned before writing, so the on-disk manifest is still the
    # pre-queue one. (Copy it back anyway when a snapshot exists — cheap and explicit.)
    [ "$snap_ok" -eq 1 ] && cp "$snap" "$LOG_DIR/tree-identity.start" 2>/dev/null \
      && cp "$snap.report" "$LOG_DIR/tree-identity.start.report" 2>/dev/null
    why="found no git worktree"
  fi
  if [ -n "$why" ]; then
    TREE_GUARDED=1
    TREE_CAPTURE_FAILED=0
    TREE_START_HEAD="$s_head"; TREE_START_DIRTY="$s_dirty"; TREE_START_DIGEST="$s_digest"
    TREE_START_BRANCH="$s_branch"; TREE_CAP_FALLBACKS="$s_fb"
    TREE_START_LINE="$s_start (pre-queue capture retained — re-capture unavailable)"
    TREE_END_LINE="$s_end"; TREE_INTEGRITY_LINE="$s_int"; TREE_HASH_CAP_LINE="$s_cap"
    echo "⚠️ agent-gate: tree-integrity re-capture at the slot grant $why — retaining the pre-queue capture, guard stays ARMED (#2926)" >&2
  fi
  rm -f "$snap" "$snap.report" 2>/dev/null || true
  return 0
}

# Synthetic-identity modes: they never certify a real tree and several need to emit a
# block with NO git state at all. They stamp the `selftest` identity so the block SHAPE
# stays uniform. (--list and --python-build-verify exit before LOG_DIR even exists.)
if [ "$SELFTEST" -eq 1 ] || [ "$LITE_AGG_SELFTEST" -eq 1 ] || [ -n "${CQLITE_GATE_STUB_RUNDIR:-}" ]; then
  TREE_START_LINE="tree-start: selftest dirty: no digest: selftest"
  TREE_END_LINE="tree-end: selftest dirty: no digest: selftest"
  TREE_INTEGRITY_LINE="tree-integrity: PASS (selftest)"
else
  _tree_capture_start
fi

# Startup invalidation (#1175 roborev finding 2): a stale .agent-gate-summary.txt
# from a PREVIOUS run must never survive into THIS run. If the current run exits
# early (dataset preflight fail, any pre-emit `exit 1`) or can't write later, a
# caller reading the recovery path would otherwise see an OLD complete PASS block
# as if it were this run's result. So, as early as possible — before the dataset
# preflight and before any component — overwrite the caller-known file with an
# INCOMPLETE sentinel stamped with THIS run's run-id. emit_summary replaces it
# with the real block on normal completion. Best-effort: if we cannot write the
# sentinel (unwritable path) we do not abort here; emit_summary's authoritative
# write guard catches an unwritable path at the end and forces a FAIL.
# SENTINEL_WROTE (#2874 review finding 6): record whether OUR run-id sentinel actually
# landed. If it did NOT (unwritable path), a later summary that lacks our run-id is a
# STALE prior-run block / unwritable file — NOT a live foreign clobber — so the
# integrity guard names that cause accurately instead of blaming a "foreign run-id".
SENTINEL_WROTE=0
if {
  echo "$SUMMARY_START_MARKER"
  echo "run-id: $RUN_ID"
  [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
  [ -n "$NESTED_UNDER_LINE" ] && echo "$NESTED_UNDER_LINE"
  # #2926: the sentinel carries `tree-start:` (and NO `tree-end:` — there is no end
  # yet), so even a gate that is killed mid-run leaves an artifact recording the tree
  # it BEGAN on. Its terminal line stays exactly `RESULT: INCOMPLETE (gate did not
  # finish)` — the #2908 liveness placeholder is unchanged.
  echo "$TREE_START_LINE"
  echo "RESULT: INCOMPLETE (gate did not finish)"
  echo "$SUMMARY_END_MARKER"
} > "$SUMMARY_FILE" 2>/dev/null; then
  SENTINEL_WROTE=1
fi

# emit_summary <result> [meta-line ...]
#
# Build the canonical SUMMARY block (start marker .. RESULT .. end marker) ONCE
# and write it to the CALLER-KNOWN file with plain redirection (no pipe), so it is
# complete regardless of stdout state — a closed-stdout SIGPIPE can never truncate
# a file written by `>`. The caller chose this path in advance (or knows the
# documented default), so it can recover the complete block without ever reading
# the stream (#1175). After writing, best-effort `cat` it to stdout for the
# foreground/redirect case. That is the whole emission: there is no stdout
# fd-detach, because detaching the gate's own stdout cannot close the pipe copy a
# leaked descendant already inherited. Both the real run and the
# --emit-summary-selftest mode go through this single function.
#
# Authoritative-write guard (#1175 roborev finding 1): if the caller-known file
# cannot be opened/written (bad path, missing parent dir, perms, disk full) or
# ends up incomplete (no end marker), that MUST NOT pass silently — the recovery
# artifact is the whole contract. We still compute and print the correctness
# verdict (least surprising), but we set SUMMARY_WRITE_FAILED=1 and print a LOUD
# warning to STDERR (more likely to survive than stdout under a leaked-child/pty
# capture). The caller's exit logic turns SUMMARY_WRITE_FAILED into a non-zero
# exit so a green gate never silently lacks its summary file.
emit_summary() {
  local result="$1"; shift
  # Write the complete block to the caller-known file FIRST, with plain
  # redirection (no pipe). This is the authoritative artifact and the advertised
  # recovery path. Capture stderr from the redirection so we can report WHY the
  # write failed (e.g. "No such file or directory", "Permission denied").
  # Capture BOTH the redirection's exit status and its stderr. The write rc is the
  # primary signal (#1175 roborev finding 1): a non-zero rc means the `>` could not
  # open/write the file, so we must NOT trust whatever is on disk — it may be a
  # stale prior-run block that survives the non-empty/end-marker checks. We grab
  # the rc of the redirected command group via the trailing `; printf` trick so it
  # is the redirection's status, not the `$(...)` substitution's.
  local write_err write_rc
  write_err=$(
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      [ -n "$NESTED_UNDER_LINE" ] && echo "$NESTED_UNDER_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE"
      echo "RESULT: $result"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE" 2>&1
    printf '\037rc=%d' "$?"
  ) || true
  # Split the captured rc sentinel (\037 unit-separator) off the tail.
  write_rc="${write_err##*$'\037'rc=}"
  write_err="${write_err%$'\037'rc=*}"
  case "$write_rc" in (*[!0-9]*|'') write_rc=1 ;; esac

  # Verify the authoritative file: the WRITE must have succeeded (rc 0) AND the
  # file must hold the COMPLETE block FOR THIS RUN — non-empty, end marker present,
  # and stamped with THIS run's run-id. The run-id check is what defeats a stale
  # prior-run file: an unwritable path with an OLD complete PASS block on disk
  # would pass the non-empty + end-marker checks, but its run-id is a DIFFERENT
  # run's, so it is correctly rejected as a failed write (#1175 finding 1).
  local reason=""
  if [ "$write_rc" -ne 0 ]; then
    reason="write failed (rc=$write_rc)${write_err:+: $write_err}"
  elif [ ! -s "$SUMMARY_FILE" ]; then
    reason="${write_err:-file missing or empty}"
  elif ! grep -qF "$SUMMARY_END_MARKER" "$SUMMARY_FILE" 2>/dev/null; then
    reason="incomplete write (end marker missing)${write_err:+: $write_err}"
  elif ! grep -qF "run-id: $RUN_ID" "$SUMMARY_FILE" 2>/dev/null; then
    reason="stale file (run-id of this run not found — write did not land)${write_err:+: $write_err}"
  fi
  if [ -n "$reason" ]; then
    SUMMARY_WRITE_FAILED=1
    # LOUD, on STDERR (survives better than stdout under non-foreground capture).
    echo "⚠️ agent-gate: could not write complete summary file $SUMMARY_FILE ($reason)" >&2
    echo "⚠️ agent-gate: recovery artifact is MISSING — gate result forced to FAIL (#1175)" >&2
  fi

  # The RESULT printed in any fallback block MUST match the process exit. Once the
  # authoritative write failed, the gate's exit logic forces a non-zero exit, so
  # the fallback blocks (log + stdout) must say RESULT: FAIL — never the computed
  # PASS — otherwise a consumer parsing the machine-checkable block sees a FALSE
  # GREEN while the process exits non-zero (#1175 roborev finding 1).
  local emit_result="$result"
  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    emit_result=FAIL
  fi

  # Keep a copy in the logs bundle (best-effort; the caller-known file is the
  # contract). NEVER copy a stale/failed caller-known file into the log: when the
  # authoritative write failed, $SUMMARY_FILE may still hold a complete-looking
  # prior-run block (e.g. an old "RESULT: PASS"), and copying it would produce a
  # misleading log artifact for THIS run (#1175 finding 1). Only copy the on-disk
  # file when the write was verified successful; otherwise write THIS run's block
  # (this run's run-id + real RESULT) directly to the log so the artifact always
  # reflects the current run, never a stale one.
  if [ "$SUMMARY_WRITE_FAILED" -eq 0 ]; then
    cp "$SUMMARY_FILE" "$LOG_SUMMARY_FILE" 2>/dev/null || true
  else
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      [ -n "$NESTED_UNDER_LINE" ] && echo "$NESTED_UNDER_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "$SUMMARY_END_MARKER"
    } > "$LOG_SUMMARY_FILE" 2>/dev/null || true
  fi

  # Best-effort stream the (already-complete) file to stdout for the
  # foreground/redirect case. If stdout is gone (closed pipe -> SIGPIPE) or a
  # leaked child has starved an until-EOF reader, this may be lost — that is
  # fine: the caller-known file above is always complete. If the file itself is
  # bad we already warned on stderr; fall back to streaming the intended block so
  # the verdict still reaches a foreground caller.
  if [ "$SUMMARY_WRITE_FAILED" -eq 0 ]; then
    cat "$SUMMARY_FILE" 2>/dev/null || true
  else
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      [ -n "$NESTED_UNDER_LINE" ] && echo "$NESTED_UNDER_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "$SUMMARY_END_MARKER"
    } 2>/dev/null || true
  fi
}

# _assert_summary_integrity <component> (#2874): mid-run summary-clobber detection
# with a NAMED cause. The startup sentinel and emit_summary both stamp $SUMMARY_FILE
# with `run-id: $RUN_ID`, and nothing in a healthy run rewrites the file mid-run
# (emit_summary runs only at the very end). So if, at a component boundary, the file
# exists but no longer carries THIS run's run-id, either a FOREIGN gate clobbered it
# (a nested/concurrent run that wrote the same path) or — when our own startup sentinel
# never landed (SENTINEL_WROTE=0) — the path is unwritable and we are seeing a stale
# prior-run block. Either way, rather than let it surface an hour later as a bare
# INCOMPLETE death (the #2751/#2874 field cost: ~1h/re-run), fail NOW with a named
# cause. Cost: one `grep -q` per component (~30/run), negligible; a belt-and-suspenders
# to emit_summary's end-of-run run-id re-grep (the final backstop).
#
# LANE-AWARENESS (#2874 review finding 1): record_result runs both on the serial MAIN
# FOREGROUND lane and inside BACKGROUNDED SIDE-lane subshells (`run_side_component &`).
# `emit_summary FAIL; exit 1` is only safe on the foreground lane — in a subshell it
# would (a) merely kill the subshell (SIDE_LANE_EXIT stays 0, the .result already
# exists → the clobber is SILENTLY LOST and the terminal emit can overwrite with PASS),
# and (b) write a COMPLETE mid-run FAIL block that a summary-file poller misreads as the
# terminal verdict. So off the foreground lane we instead drop a marker file that the
# post-drain fail-closed check (_apply_integrity_marker) converts into a terminal FAIL,
# and `return 1` without emitting. On the MAIN foreground lane (where tooling-tests —
# the dominant clobber site — runs) the immediate `exit 1` still stops the whole gate
# at once with the named line intact.
# _integrity_fail_block <reason> <comp> <sibling-path> → print the canonical named
# summary-integrity FAIL block to stdout. Shared by every no-clobber publish path so the
# block shape is identical wherever it lands.
_integrity_fail_block() {
  local reason="$1" comp="$2" sibling="$3"; shift 3
  echo
  echo "$SUMMARY_START_MARKER"
  echo "run-id: $RUN_ID"
  [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
  [ -n "$NESTED_UNDER_LINE" ] && echo "$NESTED_UNDER_LINE"
  echo "summary-integrity: FAIL ($reason)"
  echo "detected-after-component: $comp"
  echo "note: contended summary path left intact for its live owner; verdict published to private log + sibling, never the contended path (#2874)"
  # Carry the full SUMMARY_META (commit/branch/dirty, datasets, ci-pins, accelerators, cpu-budget
  # and the per-component results table) so the FAIL block is not information-poorer than a normal
  # terminal FAIL (review job-2107 MED#2). Callers pass "${SUMMARY_META[@]}" where available.
  # ${@+"$@"} not "$@": under `set -u` on bash < 4.4 (macOS /bin/bash 3.2, which this script
  # supports) an empty "$@" is treated as unbound and aborts — the MAIN-lane call passes zero meta
  # (job-2108 MED). ${@+"$@"} expands to nothing when empty, to the quoted args otherwise.
  #
  # #2926: this block is assembled INDEPENDENTLY of emit_summary (it is the no-clobber
  # publish path), so it must thread the tree-provenance lines itself — otherwise a
  # mutated-AND-clobbered run would emit a block missing them. Incoming `tree-*` meta
  # lines are dropped and re-emitted from the live globals below, so exactly ONE
  # authoritative set appears no matter which caller supplied the meta.
  local line
  for line in ${@+"$@"}; do
    case "$line" in
      tree-start:*|tree-end:*|tree-integrity:*|tree-hash-cap:*) continue ;;
    esac
    echo "$line"
  done
  _tree_meta_lines
  echo "logs: $LOG_DIR"
  echo "summary-file: $SUMMARY_FILE (NOT rewritten — live peer owns it)"
  echo "integrity-fail-sibling: $sibling"
  echo "RESULT: FAIL"
  echo "$SUMMARY_END_MARKER"
}

# _publish_integrity_fail <reason> <comp>  (#2874 ratified review job-2106 contract)
# Publish a summary-integrity FAIL for the foreign-LIVE-PEER case (SENTINEL_WROTE=1: our startup
# sentinel landed, then a peer wrote the same path) WITHOUT clobbering the contended $SUMMARY_FILE.
# Reconciles job-2105 (never clobber the live peer) with job-2106 (never leave the caller-pinned
# path holding a foreign block that becomes a foreign PASS) by making the verdict discoverable at
# caller-reachable places that are NOT the contended path:
#   (a) our OWN private log summary ($LOG_SUMMARY_FILE);
#   (b) a NON-CLOBBERING sibling next to the contended path ($SUMMARY_FILE.integrity-fail.$RUN_ID);
#   (c) the full named block on STDOUT (survives for a foreground/redirect caller);
#   (d) a named line to STDERR naming both artifacts.
# The caller still drives a non-zero exit. Used by BOTH the MAIN foreground lane and the SIDE-lane
# post-drain terminal path (one shared contract).
_publish_integrity_fail() {
  local reason="$1" comp="$2"; shift 2   # remaining args = SUMMARY_META lines (job-2107 MED#2)
  # RUN_ID is the unique mktemp LOG DIR *path* — basename it so the sibling filename has no slashes
  # (still unique: mktemp -d basenames don't collide). Sibling sits NEXT TO the contended path.
  local sibling="$SUMMARY_FILE.integrity-fail.$(basename "$RUN_ID")"
  # ${@+"$@"} (not "$@") — empty-"$@"-under-set-u-on-bash-3.2 safety (job-2108 MED); see _integrity_fail_block.
  _integrity_fail_block "$reason" "$comp" "$sibling" ${@+"$@"} > "$LOG_SUMMARY_FILE" 2>/dev/null || true
  _integrity_fail_block "$reason" "$comp" "$sibling" ${@+"$@"} > "$sibling" 2>/dev/null || true
  _integrity_fail_block "$reason" "$comp" "$sibling" ${@+"$@"}   # STDOUT — reaches a foreground/redirect caller
  echo "⚠️ agent-gate: summary-integrity: FAIL ($reason) — detected-after-component: $comp; RESULT: FAIL. Contended path $SUMMARY_FILE left intact for its live owner; verdict in $LOG_SUMMARY_FILE and $sibling (#2874)" >&2
}

_assert_summary_integrity() {
  [ -f "$SUMMARY_FILE" ] || return 0
  grep -qF "run-id: $RUN_ID" "$SUMMARY_FILE" 2>/dev/null && return 0
  local comp="${1:-<component>}" reason
  if [ "${SENTINEL_WROTE:-0}" = 1 ]; then
    reason="foreign run-id detected mid-run; expected $RUN_ID"
  else
    reason="summary-file unwritable / stale prior-run block; expected $RUN_ID"
  fi
  # In a subshell (SIDE lane), $$ is unchanged but BASHPID differs; record a marker and
  # return non-zero — never emit/exit. (bash 3.2 lacks BASHPID → falls back to $$ ==
  # foreground, which is correct there: 3.2 has no parallel pool, everything is serial.)
  if [ "${BASHPID:-$$}" != "$$" ]; then
    # Review finding (marker race): APPEND (not truncate) so two concurrent SIDE-lane detections
    # cannot corrupt each other's record — a single short `printf` to an O_APPEND fd is atomic under
    # PIPE_BUF, and the reader (_apply_integrity_marker) consumes only the FIRST complete line.
    printf '%s\t%s\n' "$comp" "$reason" >> "$LOG_DIR/summary-integrity.fail" 2>/dev/null || true
    echo "⚠️ agent-gate: summary-integrity FAIL after [$comp] ($reason) — recorded for post-drain fail-close (#2874)" >&2
    return 1
  fi
  echo "⚠️ agent-gate: summary-integrity FAIL after [$comp] ($reason) (#2874)" >&2
  # MAIN foreground lane: tear the still-live SIDE-lane sub-pool down BEFORE exiting, so
  # `exit 1` here (the first mid-lane exit in the script) does not orphan its cargo/
  # maturin/node builds against the shared target dir on an already-freed concurrency
  # slot (review finding 2). Best-effort: process-group kill if the shell put the sub-pool
  # in its own group, else the subshell PID; a brief `wait` reaps it. This is the ONE
  # place mid-lane teardown is needed — every other early exit precedes launch_components.
  if [ -n "${SIDE_LANE_PID:-}" ]; then
    echo "agent-gate: killing live SIDE-lane sub-pool (pid $SIDE_LANE_PID) before integrity exit; some backgrounded builds may already be spawned (#2874)" >&2
    # Reap the sub-pool's CHILDREN FIRST (review job-2106): once the sub-pool subshell itself is
    # killed its children reparent to PID 1, after which `pkill -P $SIDE_LANE_PID` matches nothing
    # and the cargo/maturin builds this teardown exists to stop are orphaned against the shared
    # target dir. So pkill -P (direct children) BEFORE killing the subshell, then wait. The
    # negative-PID group form is dropped: the subshell is not in the gate's own group, so it would
    # only risk signalling an unrelated process group that happens to share that id — no benefit.
    pkill -P "$SIDE_LANE_PID" 2>/dev/null || true
    kill "$SIDE_LANE_PID" 2>/dev/null || true
    wait "$SIDE_LANE_PID" 2>/dev/null || true
  fi
  # Branch on SENTINEL_WROTE (review job-2105 + ratified job-2106 reconciliation). When =1, OUR
  # startup sentinel DID land on this path, so a foreign run-id now means a LIVE PEER owns the
  # contended summary file — rewriting it via emit_summary would clobber that peer (job-2105), yet
  # leaving the pinned path holding the peer's block risks a poller reading its later foreign PASS
  # (job-2106). _publish_integrity_fail resolves both: it never touches the contended path but
  # publishes the named FAIL block to our private log + a non-clobbering sibling + stdout + stderr.
  # When =0 (our sentinel never landed → path unwritable / stale block, no live owner) emit_summary
  # is safe (it self-detects the unwritable write and forces FAIL to the private log).
  if [ "${SENTINEL_WROTE:-0}" = 1 ]; then
    _publish_integrity_fail "$reason" "$comp"
    exit 1
  fi
  # #2926: carry the tree provenance here too (this branch assembles its own block, so
  # it would otherwise be the one summary-integrity FAIL that lacks it). The lazy
  # terminal capture inside _tree_meta_lines means a run that is BOTH clobbered AND
  # mutated emits BOTH named lines under a single RESULT: FAIL.
  _tree_meta_array
  emit_summary FAIL \
    "summary-integrity: FAIL ($reason)" \
    "detected-after-component: $comp" \
    "${TREE_META_LINES[@]}"
  exit 1
}

# _apply_integrity_marker (#2874 review finding 1): consume a SIDE-lane summary-integrity
# marker left by a backgrounded component that could not safely emit+exit. Called after
# the component lanes drain (before the terminal emit_summary): it forces OVERALL=FAIL
# and sets a named terminal line so a SIDE-lane clobber is NEVER silently lost to a
# false-green summary. No-op when no marker exists.
SUMMARY_INTEGRITY_LINE=""
INTEGRITY_MARKER_SEEN=0
INTEGRITY_MARKER_COMP=""
INTEGRITY_MARKER_REASON=""
_apply_integrity_marker() {
  [ -f "$LOG_DIR/summary-integrity.fail" ] || return 0
  local m comp reason
  # First complete line only (review finding: concurrent SIDE writers append; one intact record
  # is enough to force the terminal FAIL — never parse a possibly-interleaved whole-file cat).
  m=$(head -n1 "$LOG_DIR/summary-integrity.fail" 2>/dev/null)
  comp=${m%%$'\t'*}; reason=${m#*$'\t'}
  echo "agent-gate: summary-integrity FAIL recorded by a SIDE-lane component '$comp' ($reason)" >&2
  OVERALL=FAIL
  SUMMARY_INTEGRITY_LINE="summary-integrity: FAIL ($reason; detected-after-component: $comp)"
  INTEGRITY_MARKER_SEEN=1
  INTEGRITY_MARKER_COMP="$comp"
  INTEGRITY_MARKER_REASON="$reason"
}

# _emit_terminal_summary <result> <meta-line...>  (#2874 ratified job-2106 + job-2107 MED#1)
# The single terminal-summary emit for the full gate and --only. Decide on the OBSERVABLE condition
# ALONE (job-2107 MED#1): if OUR startup sentinel landed (SENTINEL_WROTE=1) but the contended path
# no longer carries our run-id, a foreign peer owns it — route the FAIL verdict through the
# no-clobber publish helper instead of letting emit_summary rewrite (clobber) the peer, REGARDLESS
# of whether a SIDE lane happened to record a marker at a component boundary. A peer write landing
# AFTER the last record_result leaves no marker yet is the same job-2105 hazard, so the marker is
# NOT a precondition — it only supplies a more specific reason/component when present. Force
# OVERALL=FAIL + SUMMARY_WRITE_FAILED=1 so a marker-less detection still drives a non-zero exit
# (the pinned recovery artifact was deliberately not written). Returns non-zero in that case;
# otherwise defers to emit_summary. This is the one MAIN/SIDE no-clobber contract.
_emit_terminal_summary() {
  local result="$1"; shift
  if [ "${SENTINEL_WROTE:-0}" = 1 ] \
     && [ -f "$SUMMARY_FILE" ] && ! grep -qF "run-id: $RUN_ID" "$SUMMARY_FILE" 2>/dev/null; then
    local reason comp
    if [ "${INTEGRITY_MARKER_SEEN:-0}" = 1 ]; then
      reason="$INTEGRITY_MARKER_REASON"; comp="$INTEGRITY_MARKER_COMP"
    else
      reason="foreign run-id detected at terminal emit; expected $RUN_ID"; comp="<terminal>"
    fi
    OVERALL=FAIL
    SUMMARY_WRITE_FAILED=1
    _publish_integrity_fail "$reason" "$comp" ${@+"$@"}   # ${@+"$@"}: empty-safe under set -u on bash 3.2
    return 1
  fi
  emit_summary "$result" "$@"
}

# ---------------------------------------------------------------------------
# #2926 tree-integrity verification (start capture lives above the startup sentinel).
# ---------------------------------------------------------------------------

# _tree_changed_paths <a.report> <b.report>: the paths whose manifest RECORD differs
# between the two captures (content, mode, presence), one per line. Sort temporaries
# are derived from <b> (which is per-lane unique) so concurrent SIDE-lane callers
# sharing the start report can never race each other.
#
# `comm -3` prefixes COLUMN-2 records (present only in <b>) with ONE TAB. That tab is
# stripped INSIDE awk, by field arithmetic — never with `sed 's/^\t//'` (#2926 review G1):
# BSD/macOS sed does not interpret `\t` in a BRE, so there it strips a literal `t`, the
# tab survives, `awk -F'\t'` shifts by one field and `$4` yields the MODE instead of the
# PATH. macOS is a first-class gate host (the Darwin `taskpolicy` wrapper, the BSD `stat`
# branch below, the bash 3.2 floor), so a Linux-only token here is a real defect: the
# failure line would name `100644`, and `_tree_lockfile_admissible` — which classifies on
# these very paths — would misclassify with it.
#
# A column-2 line therefore presents an EMPTY $1 (the text before its leading tab) with
# every real field shifted one right; a column-1 line has the record's tag (T|U|H|N) in
# $1, which is never empty. That distinction is the whole parse.
_tree_changed_paths() {
  local a="$1" b="$2"
  LC_ALL=C sort "$a" > "$b.cmp-a" 2>/dev/null || return 0
  LC_ALL=C sort "$b" > "$b.cmp-b" 2>/dev/null || return 0
  LC_ALL=C comm -3 "$b.cmp-a" "$b.cmp-b" 2>/dev/null \
    | awk -F'\t' '
        $1 == "" { if (NF >= 5 && $5 != "") print $5; next }
        NF >= 4 && $4 != "" { print $4 }' \
    | LC_ALL=C sort -u
  rm -f "$b.cmp-a" "$b.cmp-b" 2>/dev/null || true
}

# _tree_change_class <a.report> <b.report> <head-a> <head-b>
#   -> "<class><TAB><rendered detail>", class ∈ lockfile | other
#
# `Cargo.lock` is a NAMED NON-FATAL CLASS, not an exclusion: the gate runs cargo
# WITHOUT --locked/--frozen, so the first cargo component may legitimately re-resolve
# a stale lockfile — a tracked-file mutation caused by the gate itself. A lockfile-ONLY
# difference is stamped `lockfile-settled` and proceeds; a lockfile accompanied by ANY
# other change is a full mutation FAIL. Follow-up #2962 adds --locked to the gate's
# cargo invocations, after which this carve-out SHOULD BE DELETED.

# The lookup path is passed through the ENVIRONMENT, never `awk -v` (#2926 review G2):
# `awk -v p=…` performs ESCAPE-SEQUENCE PROCESSING on the assigned value, so the `\t`/`\n`
# the `.report` view deliberately escapes (review B6) would be turned back into a real TAB
# or newline inside awk and `$4 == p` could never match — the two fixes would cancel, and
# a path containing a tab would silently fall out of the classification it was escaped to
# protect. ENVIRON does no such processing: the value arrives byte-for-byte.
#
# _tree_report_tag <report> <path> -> the record's TAG (T|U) for <path>, empty if absent.
_tree_report_tag() {
  TREE_AWK_P="$2" awk -F'\t' '$4 == ENVIRON["TREE_AWK_P"] { print $1; exit }' "$1" 2>/dev/null
}
# _tree_report_value <report> <path> -> the record's VALUE field, empty if absent.
_tree_report_value() {
  TREE_AWK_P="$2" awk -F'\t' '$4 == ENVIRON["TREE_AWK_P"] { print $2; exit }' "$1" 2>/dev/null
}

# _tree_lockfile_admissible <a.report> <b.report> <head-a> <path>
#   rc 0 iff <path> may take the NON-FATAL `lockfile-settled` class.
#
# Matching on the path alone was a fail-closed hole (#2926 review F3): an UNTRACKED file
# that merely happens to be named `…/Cargo.lock` and appears mid-run got the non-fatal
# class, so a real mid-run mutation certified. Admission now requires ALL of:
#   1. the END record is TAG `T` — a path git tracks, never an untracked impostor;
#   2. the START record, if the path was already dirty at the start capture, is also `T`;
#   3. the END value is a real BLOB OBJECT ID (40 hex in a SHA-1 repository, 64 in a
#      SHA-256 one — #2926 review G5; the old hard-coded 40 made the carve-out
#      unreachable on a SHA-256 repo, a spurious FAIL) — a DELETED/NONFILE/LINK/size+mtime
#      record is a lifecycle change, NOT "cargo re-resolved the lockfile" (the near-miss
#      variant of the same defect: a mid-run `rm Cargo.lock` is a mutation, not a settle);
#   4. the path is a BLOB IN THE START COMMIT — the authoritative "this lockfile was part
#      of the tree this run began on" test. (Presence in the START MANIFEST cannot be
#      required: a lockfile that is clean at start and re-resolved mid-run — the dominant
#      legitimate case, and the entire reason the carve-out exists — is absent there.)
# Anything else falls through to the fatal `other` class.
#
# <path> is the ESCAPED spelling the `.report` view carries, so conditions 1-3 (report
# lookups) see it verbatim, while condition 4 hands it to git, which knows the RAW path.
# For the only paths that can differ — one containing a tab, a newline or a backslash —
# git resolves
# nothing and the carve-out is refused: the FATAL direction, which is the correct one.
_tree_lockfile_admissible() {
  local a="$1" b="$2" head_a="$3" p="$4" ta tb vb
  case "$p" in Cargo.lock|*/Cargo.lock) : ;; *) return 1 ;; esac
  [ -n "$head_a" ] && [ "$head_a" != unborn ] || return 1
  tb=$(_tree_report_tag "$b" "$p"); [ "$tb" = T ] || return 1
  ta=$(_tree_report_tag "$a" "$p"); [ -z "$ta" ] || [ "$ta" = T ] || return 1
  vb=$(_tree_report_value "$b" "$p")
  _tree_hex_id_ok "$vb" || return 1
  [ "$(git --no-optional-locks cat-file -t "$head_a:$p" 2>/dev/null)" = blob ] || return 1
  return 0
}

# _tree_render_path <report-path> -> the LIST-SAFE spelling of one path.
#
# The `changed:` list and the `lockfile-settled:` detail are SPACE-JOINED, so the space is
# the character that forges a LIST BOUNDARY exactly as a tab forges a field and a newline
# forges a record (#2926 review K1): rendered raw, the single path `src/a b.rs` reads
# identically to the two paths `src/a` and `b.rs`. This is the one diagnostic a triager
# reads after a mid-run mutation, so it must be unambiguous.
#
# It is the FOURTH member of the `.report` view's own backslash family (`\\`, `\n`, `\t`
# — see _tree_identity), never a second convention: `\s` = a space. The input is already
# the report spelling, in which every literal backslash is doubled, so an `\s` in the
# output can only have come from a real space, and `\\s` is a path that really contains
# `\` `s`. Decoding is single-layer, left to right.
_tree_render_path() {
  local s="$1"
  printf '%s' "${s// /\\s}"
}

_tree_change_class() {
  local a="$1" b="$2" head_a="$3" head_b="$4"
  local list n cls=other rendered p locks=0 nonlock=0 before after detail=""
  list=$(_tree_changed_paths "$a" "$b")
  n=0
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    n=$(( n + 1 ))
    if _tree_lockfile_admissible "$a" "$b" "$head_a" "$p"; then
      locks=$(( locks + 1 ))
    else
      nonlock=1
    fi
  done <<<"$list"
  if [ "$head_a" = "$head_b" ] && [ "$n" -gt 0 ] && [ "$nonlock" -eq 0 ] && [ "$locks" -gt 0 ]; then
    # Name EVERY settled lockfile, not just the first (#2926 review): a workspace can
    # re-resolve several, and a stamp that hides the rest under-reports what moved.
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      before=$(_tree_report_value "$a" "$p")
      after=$(_tree_report_value "$b" "$p")
      detail="${detail:+$detail }$(_tree_render_path "$p") $(_tree_short "${before:-unmodified}")→$(_tree_short "${after:-unmodified}")"
    done <<<"$list"
    printf 'lockfile\t%s\n' "$detail"
    return 0
  fi
  # Render at most 5 paths, then an explicit count of the remainder.
  rendered=""
  local shown=0
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    if [ "$shown" -lt 5 ]; then
      rendered="${rendered:+$rendered }$(_tree_render_path "$p")"
      shown=$(( shown + 1 ))
    fi
  done <<<"$list"
  [ "$n" -gt "$shown" ] && rendered="$rendered (+$(( n - shown )) more)"
  [ -n "$rendered" ] || rendered="(head only — no in-scope path differs)"
  printf '%s\t%s\n' "$cls" "$rendered"
}

# _tree_set_end <head> <dirty> <digest>: record the latest observed identity.
_tree_set_end() {
  TREE_END_HEAD="$1"; TREE_END_DIRTY="$2"; TREE_END_DIGEST="$3"
  TREE_END_LINE="tree-end: $(_tree_short "$1") dirty: $2 digest: $(_tree_short "$3")"
}

# _tree_fail_reason <head-b> <rendered-paths> — the NAMED failure text. It deliberately
# contains no `RESULT:` token, so #2908's poll predicates (the buggy bare-token one and the
# corrected `grep -qE 'RESULT: (PASS|FAIL)'`) behave exactly as they do today.
#
# On a host where the run's OWN stdout/stderr redirect target cannot be named (no /proc —
# macOS/BSD), the text says so (#2926 review J3). There the fd carve-out below cannot arm,
# so a caller who redirected this run's output to a NON-ignored in-repo path gets a REAL
# detection whose cause is the log the gate is writing; naming that possibility is the
# honest alternative to widening the exclusion on a guess.
_tree_fail_reason() {
  printf 'tree-mutated-midrun; head %s→%s; changed: %s%s' \
    "$(_tree_short "$TREE_START_HEAD")" "$(_tree_short "$1")" "$2" "$TREE_FD_HINT"
}

# ---------------------------------------------------------------------------
# The SHARED fail-closed + post-mutation labelling (#2926 review J1)
#
# THREE paths detect a mid-run mutation — a component boundary (_assert_tree_integrity),
# a SIDE-lane marker (_apply_tree_integrity_marker) and the terminal capture
# (_tree_finalize) — and review H2's fix was applied to the FIRST one only. The terminal
# path therefore kept stamping `commit: <post-mutation sha>` with no label: the exact
# defect H2 exists to prevent, on the path that dominates --lite, --delta and the full
# gate's post-last-boundary window. The behaviour now lives in ONE place per concern so a
# fourth path cannot diverge again, and the structural lint in
# scripts/tests/test_agent_gate_tree_provenance.sh pins each concern to its single
# assignment site.
# ---------------------------------------------------------------------------

# _tree_fail_closed <component> <reason>: the fail-closed state EVERY detection shares —
# the run is FAILED and the verdict line names the reason and the detecting component.
# The single assignment site for TREE_MUTATED / OVERALL inside the tree guard.
_tree_fail_closed() {
  TREE_MUTATED=1
  OVERALL=FAIL
  TREE_INTEGRITY_LINE="tree-integrity: FAIL ($2; detected-after-component: $1)"
}

# _tree_label_post_mutation: the SHARED post-mutation labelling — the ONE place that
# decides how a block distinguishes "the identity this run executed against" (`commit:`,
# the VERIFIED START) from "what the tree looked like afterwards" (`tree-end:`). PURE and
# IDEMPOTENT, so any detection path may call it, in any order, more than once.
#
# It labels only when there IS a validated end observation that DIFFERS from the start: a
# mutation reverted before the observation leaves `commit:` naming the start identity
# anyway, and claiming a post-mutation reading there would be an invention.
_tree_label_post_mutation() {
  [ -n "$TREE_END_HEAD" ] || return 0
  _tree_digest_ok "$TREE_END_DIGEST" || return 0
  if [ "$TREE_END_HEAD" = "$TREE_START_HEAD" ] && [ "$TREE_END_DIRTY" = "$TREE_START_DIRTY" ] \
     && [ "$TREE_END_DIGEST" = "$TREE_START_DIGEST" ]; then
    return 0
  fi
  TREE_COMMIT_SOURCE=start
  case "$TREE_END_LINE" in
    *"$TREE_POST_MUTATION_SUFFIX") ;;
    *) TREE_END_LINE="$TREE_END_LINE $TREE_POST_MUTATION_SUFFIX" ;;
  esac
}

# _tree_mark_mutation <component> <reason>: a CONFIRMED mid-run mutation — fail closed AND
# label the provenance. Every mutation-detection path calls exactly this.
_tree_mark_mutation() {
  _tree_fail_closed "$1" "$2"
  _tree_label_post_mutation
}

# _tree_detection_mark <kind> <component> <reason>: dispatch by DETECTION KIND. A capture
# that could not be validated is fail-closed but is NOT a proven mutation, so it must not
# claim a verified-start/post-mutation split it never observed. An unknown or missing kind
# is treated as a mutation: over-labelling a capture failure is a cosmetic error, while
# under-labelling a mutation is the J1 defect itself.
_tree_detection_mark() {
  case "$1" in
    capture-failed) _tree_fail_closed "$2" "$3" ;;
    *)              _tree_mark_mutation "$2" "$3" ;;
  esac
}

# _assert_tree_integrity <component> — the component-boundary check, called from the
# `record_result` chokepoint right after _assert_summary_integrity (summary-integrity
# = who owns the artifact, evaluated first; tree-integrity = what the artifact
# describes). LANE-AWARE, exactly like #2874: on the MAIN foreground lane a mismatch
# stops the run NOW with the named block (saving the rest of an hour-long gate); inside
# a BACKGROUNDED SIDE-lane subshell it must NEVER emit_summary/exit (that would only
# kill the subshell and leave the detection to be overwritten by a later PASS), so it
# appends to a marker file that the post-drain _apply_tree_integrity_marker converts
# into OVERALL=FAIL plus the named terminal line.
_assert_tree_integrity() {
  [ "$TREE_GUARDED" -eq 1 ] || return 0
  local comp="${1:-<component>}"
  # A start capture that could not be validated already doomed the run; say so at the
  # first boundary rather than spending the rest of the gate (#2926 review B1).
  if [ "$TREE_CAPTURE_FAILED" -eq 1 ]; then
    _tree_boundary_fail "$comp" "$TREE_CAPTURE_FAIL_REASON" capture-failed
    return $?
  fi
  local probe="$LOG_DIR/tree-identity.probe.${BASHPID:-$$}"
  local id rc head dirty digest cls rendered
  id=$(_tree_identity "$probe"); rc=$?
  if [ "$rc" -ne 0 ] || ! _tree_split_identity "$id"; then
    rm -f "$probe" "$probe.report" 2>/dev/null || true
    # rc 1 = git momentarily unavailable: the authoritative terminal capture decides.
    # rc 2 / an unvalidatable identity = fail closed here.
    [ "$rc" -eq 1 ] && return 0
    _tree_boundary_fail "$comp" "$TREE_CAPTURE_FAIL_REASON" capture-failed
    return $?
  fi
  head="$TREE_F_HEAD"; dirty="$TREE_F_DIRTY"; digest="$TREE_F_DIGEST"
  # Compare the WHOLE identity, never the digest alone (#2926 review B1): head, dirty
  # flag and digest must all match for the tree to be the one this run began on.
  if [ "$head" = "$TREE_START_HEAD" ] && [ "$dirty" = "$TREE_START_DIRTY" ] \
     && [ "$digest" = "$TREE_START_DIGEST" ]; then
    rm -f "$probe" "$probe.report" 2>/dev/null || true
    return 0
  fi
  # Split explicitly (not with `IFS=$'\t' read`, which collapses empty tab-delimited
  # fields — #2926 review B1) so an empty rendering can never shift the CLASS field.
  cls=$(_tree_change_class "$LOG_DIR/tree-identity.start.report" "$probe.report" "$TREE_START_HEAD" "$head")
  rendered="${cls#*$'\t'}"; cls="${cls%%$'\t'*}"
  rm -f "$probe" "$probe.report" 2>/dev/null || true
  # lockfile-settled: non-fatal here; the terminal capture stamps it.
  [ "$cls" = lockfile ] && return 0
  # The block about to be published describes a run that executed against the START
  # identity and was STOPPED on observing this one. Record BOTH, unambiguously (#2926
  # review H2): `commit:` names the verified start, `tree-end:` the post-mutation
  # observation, each labelled for what it is — through the SHARED labelling every
  # detection path uses (review J1), never a second copy of the rule here.
  _tree_set_end "$head" "$dirty" "$digest"
  _tree_boundary_fail "$comp" "$(_tree_fail_reason "$head" "$rendered")" mutation
  return $?
}

# _tree_boundary_meta_lines: the FULL provenance a MAIN-lane boundary FAIL block carries
# (#2926 review G3). The block used to print only the tree lines plus
# `detected-after-component:`, making the ONE terminal block a reader reaches after a
# mid-run mutation the information-POOREST one in the gate — no `commit:`, no `datasets:`,
# no `ci-pins:`, no accelerator/cpu disclosure, no component verdicts. It now emits the
# same lines, in the same order, as the normal terminal assembly.
#
# Everything is read defensively: this runs at ANY component boundary, in ANY mode, and
# `DATA_COUNT`/`PINS` are established only on the full gate's path (a --lite boundary
# reaches here before they exist, and the script runs under `set -u`). A line whose source
# does not exist yet is simply omitted rather than invented.
#
# NO capture is taken here — every value comes from state already recorded. Calling the
# lazy finalize would overwrite the component-named verdict this block exists to publish.
# _tree_mode_components: the component set the RUNNING MODE actually dispatches, one name
# per line (#2926 review J2). `--lite` and `--delta` run `scoped-tests`, which the full
# gate's COMPONENTS array does not contain, so iterating COMPONENTS unconditionally could
# drop that row from a lite/delta boundary block and undercount `components-completed:`.
_tree_mode_components() {
  if [ "${LITE:-0}" = 1 ]; then
    printf '%s\n' "${LITE_COMPONENTS[@]}"
  elif [ "${DELTA:-0}" = 1 ]; then
    printf '%s\n' "${DELTA_COMPONENTS[@]}"
  else
    printf '%s\n' "${COMPONENTS[@]}"
  fi
}

_tree_boundary_meta_lines() {
  local _c _s _rf _st _secs _done=0 _sel=0 _seen=" "
  _tree_commit_meta_render
  printf '%s\n' "$TREE_COMMIT_LINE"
  if [ -n "${DATA_COUNT:-}" ]; then
    # `command -v` first: the helper is DEFINED alongside DATA_COUNT on the full gate's
    # path, so calling it blind would be a "command not found" on any earlier boundary.
    if command -v selected_needs_datasets >/dev/null 2>&1 && selected_needs_datasets; then
      printf 'datasets: %s Data.db files under %s\n' "$DATA_COUNT" "${CQLITE_DATASETS_ROOT:-<unset>}"
    else
      printf 'datasets: %s\n' "$DATA_COUNT"
    fi
  fi
  [ -n "${MISSING_FIXTURES_MARKER:-}" ] && printf '%s\n' "$MISSING_FIXTURES_MARKER"
  # #3148: the POSITIVE schemas-root assertion (empty until the full gate's preflight
  # has run, so a --lite/--delta boundary simply omits it rather than inventing it).
  [ -n "${SCHEMAS_LINE:-}" ] && printf '%s\n' "$SCHEMAS_LINE"
  [ -n "${PINS:-}" ] && printf 'ci-pins: %s\n' "$PINS"
  # Both printers deliberately emit NO trailing newline (their normal callers capture them
  # into a SUMMARY_META element), so each needs its own '%s\n' here or the whole block
  # collapses onto one line.
  printf '%s\n' "$(accelerators_line)"
  printf '%s\n' "$(cpu_budget_line)"
  [ -n "${SUMMARY_INTEGRITY_LINE:-}" ] && printf '%s\n' "$SUMMARY_INTEGRITY_LINE"
  _tree_meta_render_lines
  # The per-component verdict table, as far as the run got. Canonical order for the mode
  # ACTUALLY RUNNING (#2926 review J2 — this iterated the full gate's COMPONENTS, which does
  # not contain `scoped-tests`, the component --lite and --delta spend their time in), from
  # the `.result` files record_result has written — the same source, order and `printf`
  # shape the terminal assembly uses, so this table is a genuine PREFIX of the one a
  # completed run would emit, never a second dialect of it.
  #
  # Then a SWEEP for any remaining `.result`: a recorded verdict must never be silently
  # dropped from the table (nor from `components-completed:`) merely because no static list
  # names it — that is the same "the set is hand-maintained" failure mode as J2 itself, one
  # step out. LOG_DIR is this run's own mktemp directory, so the sweep can only see verdicts
  # record_result wrote, and the glob is deterministically ordered.
  for _c in $(_tree_mode_components); do
    _rf="$LOG_DIR/$_c.result"
    [ -f "$_rf" ] || continue
    _st=""; _secs=""
    read -r _st _secs < "$_rf" || true
    printf '%-18s %s (%ss)\n' "$_c:" "$_st" "$_secs"
    _seen="$_seen $_c "
    _done=$(( _done + 1 ))
  done
  for _rf in "$LOG_DIR"/*.result; do
    [ -f "$_rf" ] || continue
    _c="${_rf##*/}"; _c="${_c%.result}"
    case "$_seen" in *" $_c "*) continue ;; esac
    _st=""; _secs=""
    read -r _st _secs < "$_rf" || true
    printf '%-18s %s (%ss)\n' "$_c:" "$_st" "$_secs"
    _done=$(( _done + 1 ))
  done
  # Selected-count via the bash-3.2 empty-array-safe idiom used throughout this script
  # (a bare "${ARR[@]}" on an empty array aborts under `set -u` on bash < 4.4).
  for _s in ${SELECTED_MAIN[@]+"${SELECTED_MAIN[@]}"} ${SELECTED_SIDE[@]+"${SELECTED_SIDE[@]}"}; do
    _sel=$(( _sel + 1 ))
  done
  if [ "$_sel" -gt 0 ]; then
    printf 'components-completed: %s of %s selected (run STOPPED at the tree-integrity boundary — the rest never ran)\n' "$_done" "$_sel"
  else
    printf 'components-completed: %s recorded (run STOPPED at the tree-integrity boundary — the rest never ran)\n' "$_done"
  fi
  return 0
}

# _tree_boundary_fail <component> <reason> <kind> — the LANE-AWARE fail-closed action
# shared by every boundary detection. <kind> is `mutation` (a confirmed mid-run change) or
# `capture-failed` (a capture that could not be validated), and it travels WITH the marker
# so the post-drain consumer applies the same labelling the MAIN lane would (#2926 review
# J1). MAIN lane: emit the named block and exit 1. SIDE lane: record a marker, return 1,
# never emit.
_tree_boundary_fail() {
  local comp="$1" reason="$2" kind="${3:-mutation}" _l
  if [ "${BASHPID:-$$}" != "$$" ]; then
    # APPEND (never truncate): two concurrent SIDE-lane detections must not corrupt
    # each other, and the reader consumes only the FIRST complete line.
    printf '%s\t%s\t%s\n' "$kind" "$comp" "$reason" >> "$LOG_DIR/tree-integrity.fail" 2>/dev/null || true
    echo "⚠️ agent-gate: tree-integrity FAIL after [$comp] ($reason) — recorded for post-drain fail-close (#2926)" >&2
    return 1
  fi
  echo "⚠️ agent-gate: tree-integrity FAIL after [$comp] ($reason) (#2926)" >&2
  # MAIN foreground lane: tear the still-live SIDE-lane sub-pool down BEFORE exiting so
  # this mid-lane exit does not orphan its cargo builds against the shared target dir
  # (same teardown _assert_summary_integrity performs).
  if [ -n "${SIDE_LANE_PID:-}" ]; then
    echo "agent-gate: killing live SIDE-lane sub-pool (pid $SIDE_LANE_PID) before tree-integrity exit (#2926)" >&2
    pkill -P "$SIDE_LANE_PID" 2>/dev/null || true
    kill "$SIDE_LANE_PID" 2>/dev/null || true
    wait "$SIDE_LANE_PID" 2>/dev/null || true
  fi
  _tree_detection_mark "$kind" "$comp" "$reason"
  # Route the provenance through the SHARED renderers (#2926 review): hand-assembling the
  # three tree lines here omitted `tree-hash-cap:`, so a run that engaged the size+mtime
  # fallback and then failed at a boundary published a block with no disclosure of the
  # weakened capture — precisely the degraded case where it matters most. Review G3 then
  # widened this to the FULL provenance (commit/datasets/ci-pins/accelerators/cpu-budget
  # and the per-component verdicts): the failure block is exactly where a reader needs it.
  # _tree_boundary_meta_lines is PURE (no lazy finalize): a terminal capture must not run
  # here, or it would overwrite this block's component-named verdict line.
  local -a _meta=()
  while IFS= read -r _l; do _meta+=("$_l"); done < <(_tree_boundary_meta_lines)
  _meta+=("detected-after-component: $comp")
  _emit_terminal_summary FAIL "${_meta[@]}" || true
  exit 1
}

# _apply_tree_integrity_marker: consume a SIDE-lane marker after the lanes drain (before
# the terminal emit) → OVERALL=FAIL + the named terminal line, so a SIDE-lane detection
# is NEVER lost to a false-green. No-op when no marker exists; idempotent.
#
# The marker carries the DETECTION KIND as its first field (#2926 review J1) so this path
# applies exactly the labelling the MAIN lane applies: a SIDE-lane mutation is a mutation
# wherever it was seen. The end identity is not captured yet when this runs (it is the
# first thing _tree_finalize does), so the labelling is re-applied there once it is.
_apply_tree_integrity_marker() {
  [ -f "$LOG_DIR/tree-integrity.fail" ] || return 0
  [ "$TREE_MARKER_SEEN" -eq 1 ] && return 0
  local m kind comp reason rest
  m=$(head -n1 "$LOG_DIR/tree-integrity.fail" 2>/dev/null)
  kind=${m%%$'\t'*};   rest=${m#*$'\t'}
  comp=${rest%%$'\t'*}; reason=${rest#*$'\t'}
  TREE_MARKER_SEEN=1
  _tree_detection_mark "$kind" "$comp" "$reason"
  echo "agent-gate: tree-integrity FAIL recorded by a SIDE-lane component '$comp' ($reason) (#2926)" >&2
  return 0
}

# _tree_finalize: the TERMINAL capture — the authoritative check, run immediately
# before every terminal emit (full, --lite, --delta, --only). A mutation landing after
# the LAST component boundary is still caught here; this is the one check that can
# never be skipped. Forces OVERALL=FAIL on detection (so `--only` reports FAIL rather
# than PARTIAL, and every mode's exit status follows). rc 1 iff mutated.
# _tree_unguarded_terminal_probe: the F1 near-miss cover for the modes that have NO slot
# grant to re-arm at (--lite, --delta, --only). If the FIRST capture reported "no git
# worktree" but a worktree IS present at the terminal capture, the SKIP was produced by a
# TRANSIENT failure, not by a non-git tree — and this run proved nothing about the tree.
# The verdict stays SKIP (the no-worktree SKIP contract is spec'd and is not changed here),
# but the line SAYS SO, so a reader can never read that SKIP as "there was nothing to check".
_tree_unguarded_terminal_probe() {
  [ "$TREE_UNGUARDED_REASON" = no-worktree ] || return 0
  local probe="$LOG_DIR/tree-identity.skipprobe.${BASHPID:-$$}"
  _tree_identity "$probe" >/dev/null 2>&1; local rc=$?
  rm -f "$probe" "$probe.report" 2>/dev/null || true
  [ "$rc" -eq 1 ] && return 0        # still no worktree — the genuine SKIP
  TREE_INTEGRITY_LINE="tree-integrity: SKIP (start capture found no git worktree, but a worktree WAS present at the terminal capture — the start capture failed transiently; this run proves NOTHING about the tree)"
  echo "⚠️ agent-gate: tree-integrity was never armed because the start capture found no git worktree, yet a worktree is present now — the start capture failed transiently (#2926)" >&2
  return 0
}

_tree_finalize() {
  _apply_tree_integrity_marker
  if [ "$TREE_GUARDED" -ne 1 ]; then
    _tree_unguarded_terminal_probe
    return 0
  fi
  # The terminal manifest path is PER-LANE (#2926 review B7): _tree_finalize is reachable
  # from a backgrounded SIDE-lane subshell (via _tree_meta_lines/_tree_meta_array on the
  # integrity-fail publish path), and a fixed filename let two lanes write one file.
  local endbase="$LOG_DIR/tree-identity.end.${BASHPID:-$$}"
  local id rc head dirty digest cls rendered
  if [ "$TREE_CAPTURE_FAILED" -eq 1 ]; then
    TREE_END_LINE="tree-end: (start capture failed)"
    TREE_END_DIGEST="unavailable"
    _tree_fail_closed "<start>" "$TREE_CAPTURE_FAIL_REASON"
    return 1
  fi
  id=$(_tree_identity "$endbase"); rc=$?
  # rc 1 (no git) AND rc 2 (capture ran but could not be validated) BOTH land here: the
  # tree cannot be proven unchanged, so the run fails closed either way.
  if [ "$rc" -ne 0 ] || ! _tree_split_identity "$id"; then
    TREE_END_LINE="tree-end: (terminal capture failed)"
    TREE_END_DIGEST="unavailable"
    _tree_fail_closed "<terminal>" "terminal capture failed — the tree cannot be proven unchanged"
    return 1
  fi
  head="$TREE_F_HEAD"; dirty="$TREE_F_DIRTY"; digest="$TREE_F_DIGEST"
  _tree_set_end "$head" "$dirty" "$digest"
  _tree_cap_note "$TREE_F_FB"
  _tree_cap_stamp
  # A SIDE-lane marker already named the component; keep its more specific line even if
  # the tree was reverted before the terminal capture. The end identity only becomes
  # available HERE, so this is where the marker path's provenance gets labelled (#2926
  # review J1) — by the same shared rule, so it cannot drift from the other two paths.
  if [ "$TREE_MARKER_SEEN" -eq 1 ]; then
    _tree_label_post_mutation
    return 1
  fi
  # head + dirty + digest, never the digest alone (#2926 review B1).
  if [ "$head" = "$TREE_START_HEAD" ] && [ "$dirty" = "$TREE_START_DIRTY" ] \
     && [ "$digest" = "$TREE_START_DIGEST" ]; then
    TREE_INTEGRITY_LINE="tree-integrity: PASS"
    return 0
  fi
  cls=$(_tree_change_class "$LOG_DIR/tree-identity.start.report" "$endbase.report" \
          "$TREE_START_HEAD" "$head")
  rendered="${cls#*$'\t'}"; cls="${cls%%$'\t'*}"
  if [ "$cls" = lockfile ]; then
    TREE_INTEGRITY_LINE="tree-integrity: PASS (lockfile-settled: $rendered)"
    return 0
  fi
  # A TERMINAL detection is a mutation detection: it takes the same shared labelling as the
  # boundary path, so `commit:` names the VERIFIED START and `tree-end:` the post-mutation
  # observation (#2926 review J1 — this path used to stamp the post-mutation sha unlabelled,
  # which is the H2 defect on the path --lite/--delta and the post-last-boundary window of
  # the full gate all take).
  _tree_mark_mutation "<terminal>" "$(_tree_fail_reason "$head" "$rendered")"
  return 1
}

# _tree_commit_meta: set TREE_COMMIT_LINE — the block's `commit: <sha> branch: <b>
# dirty: <yes|no>` stamp — from the VERIFIED TERMINAL CAPTURE, never from a fresh git
# call at emit time (#2926 review C1).
#
# THIS IS THE ORIGINAL #2926 DEFECT. The emit sites used to run
#   commit: $(git rev-parse --short HEAD) … dirty: $(git status --porcelain …)
# AFTER _tree_finalize had taken the authoritative capture. A HEAD move landing in that
# window produced a certified block naming a sha the guard never verified — the exact
# "stamped at emit time" pattern this issue exists to eliminate. Deriving the stamp from
# TREE_END_HEAD/TREE_END_DIRTY closes the window BY CONSTRUCTION: the only sha a block
# can name is one a validated capture observed, and if that sha differs from the start
# capture the same finalize has already forced RESULT: FAIL.
#
# The branch NAME comes from TREE_START_BRANCH (read once, inside the window). It is a
# label, not a certified property: a branch pointer moved without moving HEAD's sha is
# not a tree mutation, and any move that DOES change the sha fails the run.
#
# Sets a global rather than printing, so a caller can never invoke it in a `$( … )`
# subshell whose lazy finalize's OVERALL=FAIL would be discarded (the #2926 B2 hazard).
TREE_COMMIT_LINE=""
_tree_commit_meta() {
  if [ "$TREE_GUARDED" -eq 1 ] && [ -z "$TREE_END_DIGEST" ]; then
    _tree_finalize || true
  fi
  _tree_commit_meta_render
}

# _tree_commit_meta_render: the PURE stamp renderer — derives TREE_COMMIT_LINE from
# whatever capture state exists and takes NO capture (#2926 review G3). The boundary-FAIL
# block needs the `commit:` line but must NOT trigger the lazy terminal finalize, which
# would overwrite its component-named `tree-integrity:` verdict with a `<terminal>` one —
# the same reason that block renders its tree lines through _tree_meta_render_lines.
_tree_commit_meta_render() {
  local branch="${TREE_START_BRANCH:-}"
  if [ "$TREE_GUARDED" -eq 1 ]; then
    [ -n "$branch" ] || branch=unknown
    # A mutation-detected boundary block names the VERIFIED START identity — what the run
    # actually executed against (#2926 review H2). Guarded on the start identity being
    # validated: when it is not (a start capture that failed), the run is fail-closed and
    # falls through to the `unverified` rendering rather than naming anything.
    if [ "$TREE_COMMIT_SOURCE" = start ] && [ -n "$TREE_START_HEAD" ] \
       && _tree_digest_ok "$TREE_START_DIGEST"; then
      TREE_COMMIT_LINE="commit: $(printf '%.7s' "$TREE_START_HEAD") branch: $branch dirty: $TREE_START_DIRTY (VERIFIED START — the identity this run executed against; the tree MUTATED mid-run, see tree-end: for the post-mutation observation)"
    elif [ -n "$TREE_END_HEAD" ] && _tree_digest_ok "$TREE_END_DIGEST"; then
      TREE_COMMIT_LINE="commit: $(printf '%.7s' "$TREE_END_HEAD") branch: $branch dirty: $TREE_END_DIRTY"
    else
      # No validated terminal capture exists (capture failed / no worktree at terminal).
      # The run is FAIL-CLOSED already; it must not name a sha nothing verified.
      TREE_COMMIT_LINE="commit: unverified branch: $branch dirty: unverified"
    fi
    return 0
  fi
  # UNGUARDED (no git worktree at all): there is no capture to derive from, and the block
  # already says `tree-integrity: SKIP`. Report what git says, or `unknown`.
  [ -n "$branch" ] || branch=$(git --no-optional-locks rev-parse --abbrev-ref HEAD 2>/dev/null) || branch=unknown
  TREE_COMMIT_LINE="commit: $(git --no-optional-locks rev-parse --short HEAD 2>/dev/null || echo unknown) branch: ${branch:-unknown} dirty: $(test -n "$(git --no-optional-locks status --porcelain 2>/dev/null)" && echo yes || echo no)"
  return 0
}

# _tree_meta_render_lines: the PURE printer for the provenance lines — no capture, no
# state change. The single place that decides WHICH lines a block carries, so no emit
# path can hand-assemble a subset and drop `tree-hash-cap:` (#2926 review).
_tree_meta_render_lines() {
  printf '%s\n' "$TREE_START_LINE"
  printf '%s\n' "$TREE_END_LINE"
  printf '%s\n' "$TREE_INTEGRITY_LINE"
  [ -n "$TREE_HASH_CAP_LINE" ] && printf '%s\n' "$TREE_HASH_CAP_LINE"
  return 0
}

# _tree_meta_lines: the provenance lines every SUMMARY block carries. Lazily performs
# the terminal capture if a caller reached an emit without one, so NO emission path can
# publish a block whose `tree-end:` was never taken.
_tree_meta_lines() {
  if [ "$TREE_GUARDED" -eq 1 ] && [ -z "$TREE_END_DIGEST" ]; then
    _tree_finalize
  fi
  _tree_meta_render_lines
}

# _tree_meta_array: populate the global TREE_META_LINES array (bash 3.2 has no
# namerefs), for the emit sites that build a SUMMARY_META array.
#
# The lazy finalize runs HERE, in the CURRENT shell, before the process substitution
# (#2926 review B2): `< <(_tree_meta_lines)` runs its body in a SUBSHELL, so a
# _tree_finalize triggered from inside it would set OVERALL=FAIL/TREE_MUTATED=1 in that
# subshell and lose them — only the printed text would survive. Every PASS-capable emit
# site happens to call _tree_finalize explicitly first today, but "one refactor away
# from a false green" is not an acceptable posture for the guard whose job is to fail
# closed.
_tree_meta_array() {
  if [ "$TREE_GUARDED" -eq 1 ] && [ -z "$TREE_END_DIGEST" ]; then
    _tree_finalize || true
  fi
  TREE_META_LINES=()
  local l
  while IFS= read -r l; do TREE_META_LINES+=("$l"); done < <(_tree_meta_lines)
}

# _tree_result <proposed-result>: FAIL overrides any other verdict once a mid-run
# mutation is detected. A mutation is a VERDICT, never a liveness state — it is never
# reported as INCOMPLETE, and never as PARTIAL or REFUSED.
_tree_result() {
  if [ "$TREE_MUTATED" -eq 1 ]; then printf 'FAIL\n'; else printf '%s\n' "$1"; fi
}

# gate_push_signal <result> <branch> <short-sha> <fail-components> (#2667)
#
# Fire ONE advisory push at final-SUMMARY time so a backgrounded FULL gate
# becomes a PUSH signal, not a passive poll target: the moment RESULT lands, the
# waiting closer/worker is called back instead of idle-polling the summary file.
# Delegates delivery to the REPO-OWNED contract in scripts/lib/gate-notify.sh
# (#3119), which builds the ntfy payload itself and publishes it to the server
# ROOT. It does NOT call `agent-notify --category` any more (notify-flag-allow): the installed
# upstream v1.1.0 has no `--category` arm, so that flag fell through to its
# manual "$1"/"$2" mode — title became the literal `--category`, the body became
# the category VALUE, the real title/body were dropped, and a FAIL published
# priority 3 with a green check (a red gate paging as a routine success).
# `agent-notify` survives only as the wrapper's optional, bounded, positional
# local desktop/sound adjunct.
# FULL gate ONLY — the sole call site guards out --lite/--delta/--only/selftest,
# which are iteration aids and never the gate of record.
#
# Advisory by contract: an absent/unreadable wrapper, an unset notify target, a
# missing curl/python3, a notifier that fails OR REJECTS ITS ARGUMENTS, and a
# publish that never completes are ALL silent no-ops — the summary file stays the
# artifact of record, so the notify path NEVER changes the gate's verdict or exit
# status. This function always returns 0 and never exits, traps, or writes state.
#   title: "gate <RESULT> <branch>@<short-sha>"
#   body:  "RESULT: <RESULT>" (+ "— failing: c1,c2" when any component FAILed)
gate_push_signal() {
  local result="$1" branch="$2" short_sha="$3" fail_components="$4"
  local severity=PASS
  case "$result" in PASS) severity=PASS ;; *) severity=FAIL ;; esac
  local title="gate $result ${branch}@${short_sha}"
  local body="RESULT: $result"
  [ -n "$fail_components" ] && body="$body — failing: $fail_components"
  local notify_lib="${REPO_ROOT:-.}/scripts/lib/gate-notify.sh"
  [ -r "$notify_lib" ] || return 0
  # shellcheck disable=SC1090
  . "$notify_lib" >/dev/null 2>&1 || return 0
  command -v gate_notify_publish >/dev/null 2>&1 || return 0
  gate_notify_publish "$severity" "$title" "$body" >/dev/null 2>&1 || true
  return 0
}

# --emit-summary-selftest: prove the SUMMARY block survives capture without
# running the (5-8 min) gate. Emits a representative block through the exact
# emit_summary path the real run uses, then exits 0. Used by
# scripts/tests/test_agent_gate_summary.sh.
if [ "$SELFTEST" -eq 1 ]; then
  NAMES=(fmt clippy core-tests smoke)
  STATUSES=(PASS PASS PASS PASS)
  TIMES=(1s 2s 3s 4s)
  meta=(
    "commit: selftest branch: selftest dirty: no"
    "datasets: 0 Data.db files under (selftest)"
    "ci-pins: (selftest)"
    "$(accelerators_line)"
    "$(cpu_budget_line)"
    # #2926: synthetic tree identity — the block SHAPE stays uniform with no git state.
    "$TREE_START_LINE"
    "$TREE_END_LINE"
    "$TREE_INTEGRITY_LINE"
  )
  for i in "${!NAMES[@]}"; do
    meta+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
  done
  # #2078: when the opt-out is engaged, drive the visible missing-fixtures marker
  # through the real emit path so the self-test can assert it lands in the block.
  if [ "${AGENT_GATE_ALLOW_MISSING_FIXTURES:-0}" = 1 ] && [ "$(_fixture_status)" = OPTOUT ]; then
    meta+=("$(_missing_fixtures_marker)")
  fi
  emit_summary PASS "${meta[@]}"
  # Even the selftest must not exit 0 if it could not write its summary file —
  # the whole point of the selftest is to prove the recovery artifact is produced.
  [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || exit 1
  exit 0
fi

# #2874 hidden self-test hooks: exercise the mid-run summary-integrity guard in
# isolation, deterministically (no component, no timing race). The caller pins
# AGENT_GATE_SUMMARY_FILE to a throwaway path. Modes (AGENT_GATE_INTEGRITY_SELFTEST):
#   1     — MAIN foreground lane, LIVE-PEER case (SENTINEL_WROTE=1: the writable throwaway path
#           took our startup sentinel): seed a FOREIGN run-id then run _assert_summary_integrity
#           at top level (BASHPID==$$). It must write a named `summary-integrity: FAIL` block to
#           its PRIVATE log + the named line to stderr, exit non-zero, and LEAVE THE CONTENDED
#           path intact (never clobber the live peer). A BUG marker on stdout if the guard fails
#           to fire.
#   side  — SIDE lane: seed a FOREIGN run-id then run the guard inside a subshell
#           (BASHPID!=$$); it must record a marker file + return 1 WITHOUT emitting or
#           exiting (the summary file stays the seeded foreign block — no mid-run
#           terminal block). Prints rc/marker/summary-untouched for the self-test.
#   marker — post-drain conversion, LIVE-PEER case: seed a foreign block on the contended path,
#           plant a marker, then run _apply_integrity_marker + _emit_terminal_summary. The verdict
#           must land as FAIL on the private log + a non-clobbering sibling (never rewriting the
#           contended path — ratified job-2106 no-clobber contract); prints contended-untouched +
#           sibling for the self-test (a SIDE-lane clobber is never lost to a false-green, and the
#           live peer is never clobbered).
#   terminal-nomarker — MED#1 post-drain LIVE-PEER window: seed a foreign block, set OVERALL=PASS,
#           record NO marker, then run _emit_terminal_summary. It must still detect the foreign
#           run-id on the observable condition alone, leave the contended path intact, write the
#           sibling, force OVERALL=FAIL, and return non-zero. Prints contended/sibling/overall/rc.
# (fail-closed guard for these hooks ran earlier, before the startup sentinel — #2874)
case "${AGENT_GATE_INTEGRITY_SELFTEST:-0}" in
  1)
    {
      echo "$SUMMARY_START_MARKER"
      echo "run-id: /tmp/agent-gate.FOREIGN-$$"
      echo "RESULT: INCOMPLETE (foreign)"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE"
    _assert_summary_integrity "integrity-selftest"
    echo "integrity-selftest: BUG — guard did NOT fire on a foreign run-id" >&2
    exit 0 ;;
  side)
    {
      echo "$SUMMARY_START_MARKER"
      echo "run-id: /tmp/agent-gate.FOREIGN-$$"
      echo "RESULT: INCOMPLETE (foreign)"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE"
    ( _assert_summary_integrity "side-selftest" ); _side_rc=$?
    printf 'side-integrity-selftest: rc=%s marker=%s\n' "$_side_rc" \
      "$([ -f "$LOG_DIR/summary-integrity.fail" ] && echo yes || echo no)"
    if grep -qF "run-id: /tmp/agent-gate.FOREIGN-$$" "$SUMMARY_FILE" 2>/dev/null; then
      echo "side-integrity-selftest: summary-untouched=yes"
    else
      echo "side-integrity-selftest: summary-untouched=no"
    fi
    exit 0 ;;
  marker)
    # Simulate a SIDE-lane foreign-LIVE-PEER clobber that was detected + recorded: a peer owns the
    # contended path (foreign run-id, and it will later become a foreign PASS), and a SIDE lane left
    # a marker. The post-drain terminal path (_apply_integrity_marker → _emit_terminal_summary) MUST
    # publish FAIL to the private log + non-clobbering sibling WITHOUT rewriting the contended path
    # (ratified job-2106 no-clobber contract). SENTINEL_WROTE=1 here (writable throwaway took our
    # startup sentinel before we seeded the foreign block).
    {
      echo "$SUMMARY_START_MARKER"
      echo "run-id: /tmp/agent-gate.FOREIGN-$$"
      echo "RESULT: PASS (foreign live peer)"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE"
    printf '%s\t%s\n' "smoke" "foreign run-id detected mid-run; expected $RUN_ID" \
      >> "$LOG_DIR/summary-integrity.fail"
    _apply_integrity_marker
    # THREADED, like every other emit path in #2926: these hooks reach the REAL terminal
    # emit, so a hand-built meta with no tree lines would publish an untraceable block —
    # the very "emit sites nobody enumerated" shape this change set out to close (review
    # H3). _tree_meta_array finalizes in the CURRENT shell (never a subshell, B2).
    _tree_meta_array
    _emit_terminal_summary "$OVERALL" "commit: selftest branch: selftest dirty: no" \
      ${SUMMARY_INTEGRITY_LINE:+"$SUMMARY_INTEGRITY_LINE"} \
      "${TREE_META_LINES[@]}" || true
    printf 'marker-integrity-selftest: contended-untouched=%s sibling=%s\n' \
      "$(grep -qF 'run-id: /tmp/agent-gate.FOREIGN-' "$SUMMARY_FILE" 2>/dev/null && echo yes || echo no)" \
      "$([ -f "$SUMMARY_FILE.integrity-fail.$(basename "$RUN_ID")" ] && echo yes || echo no)"
    exit 0 ;;
  terminal-nomarker)
    # job-2107 MED#1: a peer writes the contended path AFTER the last component boundary, so NO
    # SIDE-lane marker exists. The terminal path must STILL detect the foreign run-id on the
    # observable condition alone, refuse to clobber the peer, publish to the sibling, and force a
    # non-zero result — even though OVERALL started PASS (all components passed).
    {
      echo "$SUMMARY_START_MARKER"
      echo "run-id: /tmp/agent-gate.FOREIGN-$$"
      echo "RESULT: PASS (foreign live peer, post-drain)"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE"
    OVERALL=PASS
    _term_rc=0
    # Threaded for the same reason as the `marker` hook above (#2926 review H3).
    _tree_meta_array
    _emit_terminal_summary "$OVERALL" "commit: selftest branch: selftest dirty: no" \
      "${TREE_META_LINES[@]}" || _term_rc=$?
    printf 'terminal-nomarker-selftest: contended-untouched=%s sibling=%s overall=%s rc=%s\n' \
      "$(grep -qF 'run-id: /tmp/agent-gate.FOREIGN-' "$SUMMARY_FILE" 2>/dev/null && echo yes || echo no)" \
      "$([ -f "$SUMMARY_FILE.integrity-fail.$(basename "$RUN_ID")" ] && echo yes || echo no)" \
      "$OVERALL" "$_term_rc"
    exit 0 ;;
esac

# record_result <name> <status> <seconds>
# Components may run concurrently in the bounded pool (issue #1737). A backgrounded
# subshell CANNOT mutate the parent's NAMES/STATUSES/TIMES arrays or OVERALL, so
# every component writes its own verdict to a per-component result file; the parent
# reconstructs the summary arrays (in canonical COMPONENTS order) after the pool
# drains. This keeps the SUMMARY block deterministic regardless of finish order.
record_result() { # <name> <status> <seconds>
  printf '%s %s\n' "$2" "$3" > "$LOG_DIR/$1.result"
  # #2874: every component records its verdict through here, so this is the natural
  # component-boundary chokepoint for the mid-run summary-integrity guard.
  _assert_summary_integrity "$1"
  # #2926: …and for the mid-run tree-mutation guard. Ordering is deliberate:
  # summary-integrity (who owns the artifact) first, then tree-integrity (what the
  # artifact describes). If both fire, both named lines appear and RESULT is FAIL once.
  _assert_tree_integrity "$1"
}

# #2926 hidden self-test hooks: exercise the tree-integrity guard deterministically —
# no cargo, no sleep, no timing race. The START capture already happened (above the
# startup sentinel), so a mutation performed HERE is genuinely mid-run. The caller pins
# AGENT_GATE_SUMMARY_FILE to a throwaway path in a fake checkout (see
# scripts/tests/test_agent_gate_tree_integrity.sh). Modes (AGENT_GATE_TREE_SELFTEST):
#   capture  — print THIS run's start identity (head/dirty/digest/fallbacks) and exit 0,
#              optionally copying the manifest to AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT.
#              Drives the REAL _tree_identity, so digest-sensitivity cases (porcelain-
#              identical append, mode flip, deletion, untracked lifecycle) assert on the
#              production capture, not on a test double.
#   clean    — NO mutation: a boundary check + terminal finalize + terminal emit. The
#              control that proves the guard is not hardwired to FAIL.
#   boundary — mutate, then record_result on the MAIN lane: must emit the named FAIL
#              block and exit non-zero.
#   side     — mutate, then run the boundary check inside a SUBSHELL (SIDE lane): it must
#              record a marker and return non-zero WITHOUT emitting/exiting; the
#              post-drain apply + terminal emit must then publish the named FAIL.
#   terminal — mutate AFTER the last boundary, then finalize + emit: still FAIL.
#   postfinalize — finalize FIRST, THEN mutate/commit, THEN stamp + emit: the review-C1
#              window. The emitted `commit:` must be the sha the terminal capture
#              verified, never a fresh emit-time read of the moved HEAD.
#   validate-manifest — READ-ONLY: run the real _tree_manifest_ok over a caller-supplied
#              file and print the verdict. AGENT_GATE_TREE_SELFTEST_VALIDATE is
#              "<file>|<nul|nl>|<head>|<body-count>". Lets the truncation case assert on
#              the production validator against a REAL, really-truncated manifest.
#   report-lookup — READ-ONLY: run the real _tree_report_tag/_tree_report_value over a
#              caller-supplied `.report` file. AGENT_GATE_TREE_SELFTEST_LOOKUP is
#              "<file>|<escaped-path>". Pins the escaped-path lookup (#2926 review G2).
# AGENT_GATE_TREE_SELFTEST_MUTATE is a space-separated list of repo-relative files to
# append to; AGENT_GATE_TREE_SELFTEST_COMMIT=1 additionally commits (moving HEAD).
if [ "${AGENT_GATE_TREE_SELFTEST:-0}" != 0 ]; then
  # #2926 review B5: the mutating modes WRITE INTO — and with …_COMMIT=1 COMMIT INTO —
  # $REPO_ROOT. Requiring only an explicit summary path did not stop that from being a
  # LIVE checkout, which is exactly the hazard that stranded shared checkouts earlier.
  # A DISPOSABLE-CHECKOUT MARKER is now mandatory: the fixture must carry
  # `.agent-gate-tree-selftest-fixture` at its root (mkrepo in
  # scripts/tests/test_agent_gate_tree_integrity.sh writes it). Absent -> refuse loudly,
  # exit 2, before a single byte is written.
  _tree_selftest_require_fixture() {
    [ -f "$REPO_ROOT/$TREE_SELFTEST_FIXTURE_MARKER" ] && return 0
    echo "agent-gate: AGENT_GATE_TREE_SELFTEST=$AGENT_GATE_TREE_SELFTEST would MUTATE $REPO_ROOT," >&2
    echo "            which is not a disposable fixture: $TREE_SELFTEST_FIXTURE_MARKER is missing." >&2
    echo "            Refusing to write/commit into a live checkout (#2926)." >&2
    exit 2
  }
  _tree_selftest_mutate() {
    local f
    _tree_selftest_require_fixture
    # shellcheck disable=SC2086  # intentional word-split over the space-separated list
    for f in ${AGENT_GATE_TREE_SELFTEST_MUTATE:-}; do
      printf 'tree-selftest mutation\n' >> "$REPO_ROOT/$f"
    done
    if [ "${AGENT_GATE_TREE_SELFTEST_COMMIT:-0}" = 1 ]; then
      git -C "$REPO_ROOT" commit -aqm "tree-selftest mid-run commit" >/dev/null 2>&1 || true
    fi
  }
  case "$AGENT_GATE_TREE_SELFTEST" in
    capture)
      if [ -n "${AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT:-}" ]; then
        cp "$LOG_DIR/tree-identity.start" "$AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT" 2>/dev/null || true
        cp "$LOG_DIR/tree-identity.start.report" "$AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT.report" 2>/dev/null || true
      fi
      printf 'tree-selftest: guarded=%s head=%s dirty=%s digest=%s fallbacks=%s\n' \
        "$TREE_GUARDED" "$TREE_START_HEAD" "$TREE_START_DIRTY" "$TREE_START_DIGEST" "$TREE_CAP_FALLBACKS"
      printf 'tree-selftest: start-line=%s\n' "$TREE_START_LINE"
      printf 'tree-selftest: cap-line=%s\n' "${TREE_HASH_CAP_LINE:-<none>}"
      printf 'tree-selftest: commit-branch=%s\n' "$TREE_START_BRANCH"
      printf 'tree-selftest: exclude-rel=%s\n' "$TREE_EXCLUDE_REL"
      # The run's own stdout/stderr carve-out (#2926 review J3) — reported so a test can
      # assert BOTH that it names this run's redirect target and that it names nothing else.
      printf 'tree-selftest: stdout-rel=%s\n' "${TREE_STDOUT_REL:-<none>}"
      printf 'tree-selftest: stderr-rel=%s\n' "${TREE_STDERR_REL:-<none>}"
      exit 0 ;;
    validate-manifest)
      # READ-ONLY (no fixture marker needed): the production manifest validator, run
      # against a caller-supplied file (#2926 review C2).
      _tsv=${AGENT_GATE_TREE_SELFTEST_VALIDATE:-}
      _tsv_file=${_tsv%%|*}; _tsv_rest=${_tsv#*|}
      _tsv_fram=${_tsv_rest%%|*};                          _tsv_rest=${_tsv_rest#*|}
      _tsv_head=${_tsv_rest%%|*};                          _tsv_cnt=${_tsv_rest#*|}
      if _tree_manifest_ok "$_tsv_file" "$_tsv_fram" "$_tsv_head" "$_tsv_cnt"; then
        printf 'tree-selftest: manifest-ok=yes\n'
      else
        printf 'tree-selftest: manifest-ok=no\n'
      fi
      exit 0 ;;
    report-lookup)
      # READ-ONLY (no fixture marker needed): the production `.report` field lookups, run
      # against a caller-supplied report file (#2926 review G2). AGENT_GATE_TREE_SELFTEST_LOOKUP
      # is "<file>|<path>", where <path> is the ESCAPED spelling the report carries — which
      # is exactly what `awk -v` used to un-escape back into a real tab, so that a path the
      # report deliberately escaped could never be found again.
      _tsl=${AGENT_GATE_TREE_SELFTEST_LOOKUP:-}
      _tsl_file=${_tsl%%|*}; _tsl_path=${_tsl#*|}
      printf 'tree-selftest: report-tag=%s\n' "$(_tree_report_tag "$_tsl_file" "$_tsl_path")"
      printf 'tree-selftest: report-value=%s\n' "$(_tree_report_value "$_tsl_file" "$_tsl_path")"
      exit 0 ;;
    mode-components)
      # READ-ONLY (no fixture marker needed): the component set the RUNNING MODE dispatches,
      # as _tree_boundary_meta_lines sees it (#2926 review J2). Printed on one line so a test
      # can assert the WHOLE set per mode — the boundary block's table is only as complete as
      # this set is, and --lite/--delta run a component the full gate's list does not carry.
      printf 'tree-selftest: mode-components=%s\n' "$(_tree_mode_components | tr '\n' ' ' | sed 's/ *$//')"
      exit 0 ;;
    clean|boundary|terminal)
      [ "$AGENT_GATE_TREE_SELFTEST" = clean ] || _tree_selftest_mutate
      if [ "$AGENT_GATE_TREE_SELFTEST" != terminal ]; then
        record_result "tree-selftest" PASS 0     # MAIN lane: may emit + exit 1
      fi
      _tree_finalize || true
      # The REAL production stamp (#2926 review C1) — these hooks drive the same
      # capture-derived `commit:` line the full/lite/delta emits publish.
      _tree_commit_meta
      _tree_meta_array
      _emit_terminal_summary "$(_tree_result "$OVERALL")" \
        "$TREE_COMMIT_LINE" "${TREE_META_LINES[@]}" || true
      printf 'tree-selftest: mode=%s overall=%s mutated=%s\n' \
        "$AGENT_GATE_TREE_SELFTEST" "$OVERALL" "$TREE_MUTATED"
      case "$OVERALL" in PASS) exit 0 ;; *) exit 1 ;; esac ;;
    postfinalize)
      # #2926 review C1: the HEAD-MOVE-BETWEEN-FINALIZE-AND-EMIT window — the ORIGINAL
      # defect's exact shape. The terminal capture is taken FIRST (authoritative), the
      # tree/HEAD then moves, and only then is the block stamped. The emitted `commit:`
      # MUST be the sha the capture verified; stamping a fresh `git rev-parse --short
      # HEAD` here would publish a certified block naming a sha nothing ever verified.
      _tree_finalize || true
      _tree_selftest_mutate
      _tree_commit_meta
      _tree_meta_array
      _emit_terminal_summary "$(_tree_result "$OVERALL")" \
        "$TREE_COMMIT_LINE" "${TREE_META_LINES[@]}" || true
      printf 'tree-selftest: mode=postfinalize overall=%s mutated=%s commit-line=%s\n' \
        "$OVERALL" "$TREE_MUTATED" "$TREE_COMMIT_LINE"
      case "$OVERALL" in PASS) exit 0 ;; *) exit 1 ;; esac ;;
    side)
      _tree_selftest_mutate
      ( _assert_tree_integrity "side-selftest" ); _tree_side_rc=$?
      printf 'tree-selftest: side-rc=%s marker=%s\n' "$_tree_side_rc" \
        "$([ -f "$LOG_DIR/tree-integrity.fail" ] && echo yes || echo no)"
      # The subshell must NOT have emitted a terminal block: the summary file still holds
      # our INCOMPLETE sentinel at this point.
      printf 'tree-selftest: sentinel-intact=%s\n' \
        "$(grep -q 'RESULT: INCOMPLETE' "$SUMMARY_FILE" 2>/dev/null && echo yes || echo no)"
      _apply_tree_integrity_marker
      _tree_finalize || true
      _tree_commit_meta
      _tree_meta_array
      _emit_terminal_summary "$(_tree_result "$OVERALL")" \
        "$TREE_COMMIT_LINE" "${TREE_META_LINES[@]}" || true
      printf 'tree-selftest: mode=side overall=%s mutated=%s\n' "$OVERALL" "$TREE_MUTATED"
      case "$OVERALL" in PASS) exit 0 ;; *) exit 1 ;; esac ;;
  esac
fi

# run_clippy: the `clippy` component's command (issue #1844). By default it runs a
# SCOPED per-package clippy that lints the whole workspace with -D warnings WITHOUT
# compiling two costly, gate-irrelevant artifacts on every run/worktree:
#   * the source-built DuckDB C++ amalgamation (cqlite-cli `duckdb-tests` feature),
#   * the full OpenTelemetry/OTLP stack (`observability`/`observability-testing` on
#     cqlite-core/cli/flight/bindings — both the tonic AND reqwest transports).
# `--workspace --all-features` would enable EVERY feature on EVERY package and pull
# in both. `-D warnings` alone already gives clippy a distinct compile fingerprint,
# so those artifacts are never reused by any other component — pure per-gate tax.
#
# parquet/arrow are NOT excluded: they are reachable in normal builds (cqlite-cli's
# cli-helpers→state_machine→cqlite-core/parquet chain), so they stay linted here.
# ONLY duckdb + otel move to the nightly backstop.
#
# Coverage of the excluded features is NOT deleted — it moves to a nightly full
# matrix: set CQLITE_CLIPPY_FULL=1 to run the historical
# `--workspace --all-targets --all-features` pass instead. `.github/workflows/gate.yml`
# (the nightly deep-check) runs that full matrix in a dedicated parallel `clippy-full`
# job (issue #2662) — the `gate` job itself runs the full gate with this SCOPED clippy —
# so the full otel/duckdb-inclusive lint still runs within 24h. The explicit per-package
# feature lists below can drift as features are added; that nightly `--all-features` pass
# is the backstop that catches any omission.
run_clippy() {
  if [ "${CQLITE_CLIPPY_FULL:-0}" = 1 ]; then
    env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
    return
  fi

  # (1) Whole workspace at all-features, EXCLUDING the five packages that carry the
  #     duckdb/otel optional features. --all-features only turns on features of the
  #     SELECTED packages, so with these excluded no `duckdb-tests`/`observability`
  #     feature is ever activated — and cqlite-core, built here only as a transitive
  #     dependency of the remaining crates, never gets its `observability` feature.
  env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features \
    --exclude cqlite-core --exclude cqlite-cli --exclude cqlite-flight \
    --exclude cqlite-py --exclude cqlite-node || return 1

  # (2) cqlite-core: every feature EXCEPT observability/observability-testing/metrics
  #     (the OpenTelemetry stack). Keep this in sync with cqlite-core/Cargo.toml when
  #     features are added; the nightly CQLITE_CLIPPY_FULL=1 pass is the drift guard.
  env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-core --all-targets --features \
"all-compression,arrow,bench-internals,benchmarks,ci_zero_tolerance,cli-helpers,deflate,delta-scan,dhat-heap,docker-integration,enhanced-index-validation,events,experimental,extended-index-validation,fuzz,legacy-heuristics,lz4,parquet,pest,scan-offload-probe,snappy,state_machine,test-coverage-tracking,test-infrastructure,test-property-testing,test-quality-gates,test-schema-validation,tombstones,unit-tests-only,wasm,work-counters,write-support,zstd" \
    || return 1

  # (3) cqlite-cli: every feature EXCEPT duckdb-tests + observability. Pulls in
  #     parquet/arrow via state_machine and delta-scan via delta-export, so the
  #     normal-build reachable surface stays linted.
  env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-cli --all-targets --features \
"benchmarks,ci_zero_tolerance,cli-helpers,delta-export,experimental,integration-tests,interactive,state_machine,tui,write-support" \
    || return 1

  # (4) cqlite-flight + the Python/Node bindings at their DEFAULT features (none of
  #     which enable observability), plus cqlite-node's write-support code path. This
  #     lints their real binding/connector surface without linking the otel shim.
  #
  #     INVARIANT (issue #1893): cqlite-py MUST stay in this linted set. --lite's
  #     python tier classifies a venv/pip/maturin toolchain failure as SKIP (not
  #     FAIL) precisely because this clippy pass still COMPILES cqlite-py in the
  #     same lite run — it is the compile backstop that makes the SKIP safe.
  #     Removing cqlite-py here would let a broken bindings/python/src build sail
  #     through an offline --lite green.
  env RUSTFLAGS="-D warnings" cargo clippy --all-targets \
    -p cqlite-flight -p cqlite-py -p cqlite-node --features cqlite-node/write-support \
    || return 1
}

# run_roborev_lints (issue #2656, epic #2636): mechanize the recurring roborev
# BLOCKER classes that have credible low-false-positive mechanical detection, so
# they FAIL in the fast --lite loop instead of costing a review round. Two checks:
#   * check-workflow-injection.sh — the GitHub Actions command-injection class
#     (top-severity, previously UNMECHANIZED anywhere in the gate).
#   * check-no-wallclock-asserts.sh — the #2642 wall-clock-race class. It already
#     ran in the FULL gate (tooling-tests) but NOT in --lite; running it here makes
#     the fast loop catch a reintroduced wall-clock threshold assert.
#   * test_roborev_review_guard.sh — the #2964 VACUOUS-REVIEW class: the sanctioned
#     wrapper scripts/flow/roborev-review.sh must fail closed on every recorded
#     trigger that lets "roborev clean" be recorded without a review having
#     happened. Its verdict gates a merge, so a weakened assert in it means the
#     pipeline merges unreviewed code with no red anywhere. Hermetic (stub roborev
#     on PATH + throwaway git fixtures; no network, no datasets, no cargo) and
#     ~0.5s, so it belongs in the fast loop: a regression FAILs --lite instead of
#     costing a review round.
# Other candidate classes from the pre-roborev self-check are deliberately NOT
# mechanized (see the taxonomy on #2656): manual_range_contains is already caught
# by clippy; integer/decimal overflow, float-ordering-vs-Java, no-heuristics, and
# process-global-counter races are semantic (no low-FP static signal); a
# gitignored-references lint would false-positive on the intentionally-fetched
# dataset corpus. Both scripts are SKIP-aware (no python3 -> loud SKIP, exit 0),
# so this component is safe on a stripped runner; any real violation exits non-zero
# and FAILs the component.
#   * test_roborev_guard_portability.sh — the #3296 PLATFORM class: the guard test above
#     is a shell script that runs on macOS worker boxes and Linux CI, and three GNU-only
#     constructs in its own scaffolding (`sed -i EXPR FILE`, an operand-less `paste -s`,
#     and a literal-vs-canonical `--repo` compare over macOS's /var -> /private/var
#     symlink) made it report `failed: 7` on macOS while every hosted Linux lane was
#     green — i.e. the LOCAL gate of record was red on the whole fleet and no CI signal
#     could see it. This test keeps that class out structurally (a construct table whose
#     every pattern carries a positive control) and behaviourally (the guard test's own
#     helpers, extracted verbatim, exercised under BSD `sed`/`paste` shims), so the
#     platform difference is caught on ANY platform. Hermetic, ~1s.
run_roborev_lints_cmd() {
  bash "$REPO_ROOT/scripts/ci/check-workflow-injection.sh" &&
    bash "$REPO_ROOT/scripts/tests/check-no-wallclock-asserts.sh" &&
    bash "$REPO_ROOT/scripts/tests/test_roborev_review_guard.sh" &&
    bash "$REPO_ROOT/scripts/tests/test_roborev_guard_portability.sh"
}

# check_no_unexpected_zero_tests <label> <logfile> [allowed-zero-target...]
#
# THE zero-tests guard (originally #2039, inline in cli-tests; promoted to a single
# top-level definition by #1699 when a second and third component needed it). Parses
# cargo's OWN per-target "Running tests/<name>.rs" / "test result: ok. N passed"
# output and FAILs CLOSED if any `--test` target ran 0 tests, unless that target is
# named on the caller's explicit allowed-zero list.
#
# The shape it exists to catch: a test target whose body compiles out at the feature
# set the component selected (`#[cfg(feature = ...)]` with the feature off, a
# whole-file `#![cfg(...)]`, a required-features target landing in the wrong pass)
# COMPILES, runs zero tests, and cargo exits 0 — so the component reads PASS while
# its subject was never executed. That is the vacuous-green shape: a positive verdict
# with no affirmative measurement behind it.
#
# Match the FULL zero-line (roborev finding, #2039): "0 passed; 0 failed" alone also
# matches a target whose tests are ALL #[ignore]d ("0 passed; 0 failed; 3 ignored"),
# which is a legitimate, unrelated shape this guard must never fault — only a
# truly-empty run (0 ignored too) is the compiled-out shape.
#
# SCOPE, stated so a caller does not over-trust it: it keys on "Running tests/", i.e.
# INTEGRATION `--test` targets. A `--lib` unit-test run prints "Running unittests
# src/lib.rs", which this deliberately does not claim to cover.
#
# Exported (`export -f`) because the cli-tests component body runs under `bash -c`
# and must see the SAME implementation rather than a second copy of it.
# _ansi_stripped_log <logfile> — echo a path to <logfile> with ANSI escapes removed.
#
# roborev round-15 finding (HIGH), and the premise checked out: `.github/workflows/gate.yml`
# (the nightly FULL gate) sets `CARGO_TERM_COLOR: always`, as do 17 other workflows under
# .github/workflows/ (18 in total, measured — an earlier version of this comment said 8, which was
# never measured) and scripts/local/pre-merge.sh. Cargo then emits
#     ESC[1mESC[92m     RunningESC[0m unittests src/lib.rs (...)
# with the reset sequence sitting BETWEEN `Running` and the path — so every parser keyed on
# the literal text sees nothing. MEASURED on both guards, and the two directions differ:
#   * check_unittest_targets_ran  -> FALSE FAIL: the new lanes would red on every clean
#     nightly run, reporting "no Running unittests line" about a perfectly healthy log.
#   * check_no_unexpected_zero_tests -> VACUOUS PASS: a target running ZERO tests is never
#     associated with its result, so the #2039 guard silently reports OK. That one is
#     PRE-EXISTING and affects its OTHER CALLER on nightly CI too — which is `cli-tests`, both
#     of whose passes call it. An earlier version of this comment also named `core-tests`;
#     core-tests does NOT call this guard, and that claim was never measured. Filed as #3400.
#
# Stripping is done ONCE into a sibling file, not per line and not through a pipe. A pipe
# would put the reading loop in a SUBSHELL and its accumulated verdict variables would be
# discarded — which for these guards means silently passing, the exact failure they exist to
# prevent. The ESC byte is injected via printf rather than written as `\x1b`, because BSD sed
# does not honour `\x` escapes and macOS is a first-class gate host.
_ansi_stripped_log() {
  local logfile="$1" out esc
  # FAIL CLOSED on an unreadable log (roborev round-25, Medium). Returning the original path let the
  # caller parse a file it had just failed to read, and a guard that parses nothing reports nothing
  # wrong. The caller's own fail-closed branch is what should decide, so tell it the truth.
  [ -r "$logfile" ] || return 1
  esc=$(printf '\033')
  out="$logfile.ansi-stripped"
  if sed -E "s/${esc}\\[[0-9;]*[A-Za-z]//g" "$logfile" > "$out" 2>/dev/null; then
    printf '%s' "$out"
  else
    # A FAILED normalisation is not "use the coloured original": under CARGO_TERM_COLOR the
    # coloured original is exactly what the parsers cannot read (round 15), so silently handing it
    # back converts a normalisation failure into a vacuous PASS. Non-zero, and the caller FAILs.
    return 1
  fi
}

# check_test_targets_observed <label> <logfile> <expected-id>... — every expected integration
# target must appear as a `Running` banner in the log.
#
# roborev round-17 preferred this POSITIVE form alongside the zero-test check, and it is the
# right shape: check_no_unexpected_zero_tests can only judge targets it SAW, so a target that
# never appeared at all is invisible to it. This lane derives its target set, so it can assert
# the stronger claim — "these are the targets that executed" — instead of assuming it.
#
# Ids are spelled as the guard spells them (tests-relative under tests/, package-relative
# otherwise), so the two agree by construction.
check_test_targets_observed() {
  local label="$1" logfile="$2"; shift 2
  local -a expected=("$@")
  if [ "${#expected[@]}" -eq 0 ]; then
    echo "$label: FAIL-CLOSED — check_test_targets_observed called with NO expected target; a guard with an empty subject set reports OK having measured nothing (issue #1699)." >&2
    return 1
  fi
  local src seen="" missing="" line t e
  src=$(_ansi_stripped_log "$logfile" 2>/dev/null) || src=""
  if [ -z "$src" ] || [ ! -r "$src" ]; then
    echo "$label: FAIL-CLOSED — could not prepare '$logfile' for parsing, so no target could be observed (issue #1699)." >&2
    return 1
  fi
  while IFS= read -r line; do
    case "$line" in
      *"Running tests/"*)
        t=$(printf '%s' "$line" | sed -E "s#.*Running tests/([^[:space:]]+)\\.rs.*#\\1#")
        seen="$seen $t " ;;
      *"Running "*".rs"*)
        case "$line" in *"Running unittests"*) continue ;; esac
        t=$(printf '%s' "$line" | sed -E "s#.*Running ([^[:space:]]+)\\.rs.*#\\1#")
        seen="$seen $t " ;;
    esac
  done < "$src"
  for e in "${expected[@]}"; do
    case "$seen" in *" $e "*) ;; *) missing="$missing $e" ;; esac
  done
  if [ -n "$missing" ]; then
    echo "$label: FAIL-CLOSED — derived target(s)$missing produced no 'Running' banner, so they did NOT execute. The zero-test guard cannot see this: it judges only targets it observed, and an absent target is not an observed one (issue #1699, roborev round-17)." >&2
    return 1
  fi
  return 0
}
export -f check_test_targets_observed

check_no_unexpected_zero_tests() {
  local label="$1" logfile="$2"; shift 2
  local _orphans=""
  local allowed_zero=" $* "
  local bad="" target="" _banners=0 _results=0 _unit_banners=0
  # FAIL CLOSED if the log cannot be prepared or read (roborev round-16, HIGH). Without
  # this, ANY failure to resolve the parse source — an unexported helper, a deleted file, a
  # sed that could not write — leaves the read loop with nothing to consume, and a guard that
  # consumed nothing found no problem and returns SUCCESS. That is the vacuous pass these
  # guards exist to prevent, arriving through their own plumbing. Checked here rather than
  # relying on every caller exporting the right helpers: the guard is responsible for knowing
  # whether it measured anything.
  local _parse_src
  _parse_src=$(_ansi_stripped_log "$logfile" 2>/dev/null) || _parse_src=""
  if [ -z "$_parse_src" ] || [ ! -r "$_parse_src" ]; then
    echo "$label: FAIL-CLOSED — could not prepare '$logfile' for parsing (resolved to '${_parse_src:-<empty>}'), so this guard parsed NOTHING. A guard that consumed no input has measured nothing and must never report OK (issue #1699, roborev round-16)." >&2
    return 1
  fi
  if [ -s "$logfile" ] && [ ! -s "$_parse_src" ]; then
    echo "$label: FAIL-CLOSED — '$logfile' is non-empty but its prepared copy '$_parse_src' is empty, so this guard would parse nothing (issue #1699)." >&2
    return 1
  fi
  while IFS= read -r line; do
    # Two spellings, deliberately, and the reason is compatibility (roborev rounds 11+13).
    # A target under tests/ keys on its path RELATIVE TO tests/ (`foo`, or `foo/main` for a
    # directory-style target) because that is how every existing caller's allowed-zero list
    # is spelled. A target mapped OUTSIDE tests/ by an explicit `[[test]] path = "..."` keys
    # on its package-relative path: previously it matched nothing, was never associated with
    # a result, and could run ZERO tests unnoticed — a vacuous PASS. Additive, and measured
    # as a no-op today (0 mapped targets across the workspace), so it closes the hole
    # without re-spelling a single existing allowed-zero entry.
    #
    # This SUPERSEDES the round-11 fix, which refused such a target instead of guarding it.
    # That refusal was silently DELETED by a coarse round-12 edit and nothing noticed,
    # because with no such target in the tree the branch never ran — which is why this
    # version is pinned by a behavioural log fixture rather than by its own existence.
    if [[ "$line" == *"Running tests/"* ]]; then
      # An ORPHANED previous target is a FAIL (roborev round-26, Medium). If a new banner arrives
      # while the last integration target still has no parseable result, that target was OBSERVED
      # and never JUDGED — a truncated log, a killed binary, or a result line the parse missed. The
      # guard would otherwise pass, having silently skipped exactly the target it was asked about.
      [ -n "$target" ] && _orphans="$_orphans $target"
      target=$(printf "%s" "$line" | sed -E "s#.*Running tests/([^[:space:]]+)\.rs.*#\1#")
      _banners=$((_banners + 1))
    elif [[ "$line" == *"Running unittests"* ]]; then
      # NOT this guard's subject — check_unittest_targets_ran owns --lib/--bins targets. But
      # it is COUNTED, because the affirmative check below must not red a legitimately
      # unittest-only log (a `--lib` selection produces nothing else). Without this the new
      # round-17 check turned a correct log into a false red; the complement assert caught it.
      _unit_banners=$((_unit_banners + 1))
      [ -n "$target" ] && _orphans="$_orphans $target"
      target=""
    elif [[ "$line" == *"Running "*".rs"* ]]; then
      # `--lib`/`--bins` unittest lines are check_unittest_targets_ran's subject, not ours.
      [ -n "$target" ] && _orphans="$_orphans $target"
      target=$(printf "%s" "$line" | sed -E "s#.*Running ([^[:space:]]+)\.rs.*#\1#")
      _banners=$((_banners + 1))
    elif [[ "$line" == "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"* ]]; then
      _results=$((_results + 1))
      if [ -n "$target" ] && [[ "$allowed_zero" != *" $target "* ]]; then
        bad="$bad $target"
      fi
      target=""
    elif [[ "$line" == "test result:"* ]]; then
      _results=$((_results + 1))
      target=""
    fi
  done < "$_parse_src"
  # EOF with a target still pending is the same orphan case (roborev round-26): the log ended
  # between a banner and its result, which is what a truncated or killed run looks like.
  [ -n "$target" ] && _orphans="$_orphans $target"
  # AN AFFIRMATIVE MEASUREMENT, not the absence of a bad signal (roborev round-17, HIGH).
  # Round 16 made this guard fail when it could not READ its input; that was not enough. A
  # non-empty, perfectly readable log can still contain no PARSEABLE `Running` banner — a
  # cargo output-format change, a normalisation that drops the line, output suppressed by a
  # wrapper — and then `target` and `bad` both stay empty and the guard returns SUCCESS even
  # for `test result: ok. 0 passed`. The vacuous green this whole change exists to close,
  # surviving two rounds of closing it.
  #
  # So: if the log contains test RESULTS, at least one banner must have been ATTRIBUTED to a
  # target. Results with zero recognised banners means the parse is broken, not that
  # everything is fine — and "the parse is broken" is never a pass.
  if [ "$_results" -gt 0 ] && [ "$_banners" -eq 0 ] && [ "$_unit_banners" -eq 0 ]; then
    echo "$label: FAIL-CLOSED — '$logfile' contains $_results cargo test-result line(s) but NOT ONE parseable 'Running <path>.rs' banner, so no result could be attributed to a target. The parse is broken (a cargo format change, a normalisation that dropped the line, or suppressed output); a guard that attributed nothing has measured nothing and must never report OK (issue #1699, roborev round-17)." >&2
    return 1
  fi
  # AND ZERO RESULTS IS ALSO NOT A PASS (roborev round-25, Medium). Round 17 closed
  # "results but no banners"; the complementary hole stayed open — a log with NO parseable
  # `test result:` line at all leaves every counter at zero, `bad` empty, and the guard
  # returning SUCCESS. A truncated log, a killed cargo, a changed output format or a
  # normalisation that ate every line all land here. Every caller of this guard has just run
  # cargo test to a SUCCESSFUL exit, so at least one result line must exist; none means the
  # guard could not see what happened, which is never a pass.
  #
  # `--no-run` callers are not affected because they do not call this guard: the two isolation
  # lanes compile only and are checked by their own exit status.
  if [ -n "$_orphans" ]; then
    echo "$label: FAIL-CLOSED —$_orphans was OBSERVED running (a 'Running' banner) but no parseable 'test result:' line followed it, so this guard never judged whether it ran zero tests. A target observed and not judged is the gap this guard exists to close, not a pass (issue #1699, roborev round-26)." >&2
    return 1
  fi
  if [ "$_results" -eq 0 ]; then
    echo "$label: FAIL-CLOSED — '$logfile' contains NO parseable cargo 'test result:' line at all, so this guard judged ZERO targets. The caller's cargo run exited successfully, so results must exist: a truncated log, a killed process, a changed cargo output format or a failed ANSI normalisation. A guard that judged nothing has measured nothing and must never report OK (issue #1699, roborev round-25)." >&2
    return 1
  fi
  if [ -n "$bad" ]; then
    echo "$label: FAIL-CLOSED —$bad ran 0 tests unexpectedly (issue #2039: a target whose body is #[cfg]-gated out at this component's feature set, and not on the allowed-zero list, would otherwise silently never run)" >&2
    return 1
  fi
  return 0
}
# Exported ALONGSIDE its callers (roborev round-16 finding, HIGH). Both guards call
# _ansi_stripped_log, and the cli-tests component runs its body under `bash -c` — so with the
# helper unexported the command substitution produced the EMPTY STRING, `done < ""` failed,
# the loop body never ran, and the guard returned SUCCESS having parsed nothing. That silently
# disabled the CLI zero-test protection. I had just fixed the same shape in the self-test
# extraction and written a commit message about it, and missed this instance one function away
# — which is why the fail-closed check below matters more than this export line.
export -f _ansi_stripped_log
export -f check_no_unexpected_zero_tests

# check_unittest_targets_ran <label> <logfile> <unittest-src-path>...
#
# The `--lib`/`--bins` ANALOGUE of check_no_unexpected_zero_tests (issue #3384). That
# guard keys on cargo's `Running tests/<name>.rs` lines and explicitly disclaims unit
# runs, which print `Running unittests src/lib.rs (…)`. So on a lane that selects ONLY
# `--lib --bins` it has an EMPTY SUBJECT SET and reports OK — a positive verdict with no
# measurement behind it, i.e. the vacuous pass. This guard supplies the missing subject.
#
# AFFIRMATIVE, in both directions, because "no failure was seen" is not evidence that
# anything ran:
#   * each expected unittest target must be OBSERVED (`Running unittests <path>`) —
#     an absent one means the selector stopped choosing it (an explicit `--lib` without
#     `--bins` silently drops main.rs's tests), which is the never-executed hole; AND
#   * its `running N tests` count must be NON-ZERO — a module tree cfg-gated out
#     compiles, runs 0 tests and exits 0.
# A positive verdict PRINTS the counts, so a pasted log shows the check RAN and on what.
#
# `running N tests` is parsed rather than the `test result:` line because 0 is
# unambiguous there, and because an all-`#[ignore]`d suite (a legitimate shape) still
# reports a non-zero `running N`.
# _package_unittest_srcs <pkg> — the `src` paths of every lib/bin target of <pkg>, in the
# form `cargo test` prints them after `Running unittests` (e.g. `src/lib.rs`).
#
# roborev round-7 finding (Medium): the flight-tests zero-test guard was called with a
# HARD-CODED `src/lib.rs src/main.rs`. `--bins` selects EVERY binary, so adding one to
# cqlite-flight would have let it run zero tests with the guard still reporting OK — the
# vacuous pass the guard exists to prevent, reintroduced as a two-entry registry that
# drifts silently. That is #2039's lesson, which this very lane's report cites; a
# hard-coded list beside a wildcard selector is the same defect in miniature.
#
# Derived from cargo metadata so the guard's subject set tracks the selector. A failed
# derivation returns non-zero and the caller FAILs — never a fallback to a partial list,
# which would silently shrink the guard's subject exactly like the hard-coded pair did.
_package_unittest_srcs() { # <pkg> [kinds] [enabled-features]
  local pkg="$1" kinds="${2:-lib,bin}" enabled="${3:-}"
  local meta out
  meta=$(cargo metadata --format-version 1 --no-deps 2>/dev/null) || return 1
  [ -n "$meta" ] || return 1
  # jq FIRST, then python3, then failure — the same chain and the same direction as
  # _package_index / _package_test_targets / _resolved_package_features. A single-parser
  # helper is a FALSE RED on a jq-only host (roborev round-18, Medium): this gate treats
  # macOS as a first-class host, and here the lane would have failed closed on a healthy
  # tree, which is the verdict agents learn to re-run away from.
  if command -v jq >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | jq -r --arg n "$pkg" --arg kinds "$kinds" --arg en "$enabled" '
      ($kinds | split(",")) as $want
      | ($en | split(" ") | map(select(length > 0))) as $enabled
      | .packages[] | select(.name == $n)
      | ((.manifest_path // "") | split("/") | .[0:-1] | join("/")) as $root
      | .targets[]
      | select([ (.kind // [])[] | select(. as $k | $want | index($k)) ] | length > 0)
      | select(.test != false)
      | ((."required-features" // .required_features // [])) as $rf
      | select((($rf | length) == 0) or ((($rf - $enabled) | length) == 0))
      | (.src_path // "") as $sp
      | if ($root != "" and ($sp | startswith($root + "/")))
        then ($sp | ltrimstr($root + "/")) else $sp end') || return 1
  elif command -v python3 >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | python3 -c '
import json, sys, os
pkg = sys.argv[1]
enabled = set(w for w in (sys.argv[3] if len(sys.argv) > 3 else "").split() if w)
d = json.load(sys.stdin)
for p in d.get("packages", []):
    if p.get("name") != pkg:
        continue
    root = os.path.dirname(p.get("manifest_path", ""))
    for t in p.get("targets", []):
        want = set(sys.argv[2].split(","))
        kinds = t.get("kind") or []
        if not any(k in want for k in kinds):
            continue
        # Only targets cargo will actually RUN under this selection (roborev round-13,
        # Low). `test = false` in the manifest, or required-features this lane does not
        # enable, means cargo legitimately SKIPS the target — and demanding an observation
        # for it would FAIL the gate on correct behaviour. A false red is not the safe
        # direction here: it is what teaches people to re-run until green.
        if t.get("test") is False:
            continue
        rf = t.get("required-features") or t.get("required_features") or []
        if rf and not set(rf).issubset(enabled):
            continue
        sp = t.get("src_path") or ""
        rel = sp[len(root) + 1:] if root and sp.startswith(root + os.sep) else sp
        print(rel)
' "$pkg" "$kinds" "$enabled") || return 1
  else
    return 1
  fi
  [ -n "$out" ] || return 1
  printf '%s\n' "$out"
}

check_unittest_targets_ran() {
  local label="$1" logfile="$2"; shift 2
  local -a expected=("$@")
  if [ "${#expected[@]}" -eq 0 ]; then
    echo "$label: FAIL-CLOSED — check_unittest_targets_ran was called with NO expected unittest target; a guard with an empty subject set would report OK having measured nothing (issue #3384)" >&2
    return 1
  fi
  # NO `declare -A` HERE, DELIBERATELY (roborev round-3 finding, High). This gate
  # explicitly supports stock macOS /bin/bash 3.2 — see the lane-parallelism fallback
  # at the AGENT_GATE_JOBS derivation, which degrades rather than requires bash >= 4.3 —
  # and associative arrays are a bash 4.0 feature. A `declare -A` here would have made a
  # SUCCESSFUL cargo run fail inside its own zero-test guard on a supported platform.
  # A newline-delimited "<target><TAB><count>" list is 3.2-safe; target names come from
  # cargo's own `Running unittests <path>` output and contain no tabs or newlines.
  local line cur="" bad="" seen="" counts=""
  # FAIL CLOSED if the log cannot be prepared or read (roborev round-16, HIGH). Without
  # this, ANY failure to resolve the parse source — an unexported helper, a deleted file, a
  # sed that could not write — leaves the read loop with nothing to consume, and a guard that
  # consumed nothing found no problem and returns SUCCESS. That is the vacuous pass these
  # guards exist to prevent, arriving through their own plumbing. Checked here rather than
  # relying on every caller exporting the right helpers: the guard is responsible for knowing
  # whether it measured anything.
  local _parse_src
  _parse_src=$(_ansi_stripped_log "$logfile" 2>/dev/null) || _parse_src=""
  if [ -z "$_parse_src" ] || [ ! -r "$_parse_src" ]; then
    echo "$label: FAIL-CLOSED — could not prepare '$logfile' for parsing (resolved to '${_parse_src:-<empty>}'), so this guard parsed NOTHING. A guard that consumed no input has measured nothing and must never report OK (issue #1699, roborev round-16)." >&2
    return 1
  fi
  if [ -s "$logfile" ] && [ ! -s "$_parse_src" ]; then
    echo "$label: FAIL-CLOSED — '$logfile' is non-empty but its prepared copy '$_parse_src' is empty, so this guard would parse nothing (issue #1699)." >&2
    return 1
  fi
  while IFS= read -r line; do
    case "$line" in
      *"Running unittests "*)
        cur=$(printf '%s' "$line" | sed -E 's#.*Running unittests ([^[:space:]]+).*#\1#')
        continue
        ;;
    esac
    if [ -n "$cur" ]; then
      case "$line" in
        running\ [0-9]*\ test|running\ [0-9]*\ tests)
          counts="$counts$cur\t$(printf '%s' "$line" | sed -E 's#^running[[:space:]]+([0-9]+)[[:space:]]+tests?$#\1#')
"
          cur=""
          ;;
      esac
    fi
  done < "$_parse_src"
  local p n
  for p in "${expected[@]}"; do
    # Exact whole-field match on the TAB-delimited pair, so one target name cannot
    # match another's prefix.
    n=$(printf '%b' "$counts" | awk -F'\t' -v t="$p" '$1 == t { print $2; exit }')
    if [ -z "$n" ]; then
      bad="$bad $p(NOT OBSERVED: no 'Running unittests $p' line — the cargo selector no longer chooses this target)"
    elif [ "$n" -eq 0 ]; then
      bad="$bad $p(ran 0 tests: the unit suite compiled out at this feature set)"
    else
      seen="$seen $p($n tests)"
    fi
  done
  if [ -n "$bad" ]; then
    echo "$label: FAIL-CLOSED — unittest target(s)$bad (issue #3384: this lane's ONLY subject is its unit suite, so an unobserved or empty unit run is a green lane that executed nothing)" >&2
    return 1
  fi
  echo "$label: unittest targets OK —$seen executed (affirmative measurement, parsed from cargo's own 'Running unittests' / 'running N tests' output)" >&2
  return 0
}
export -f check_unittest_targets_ran

# _package_test_targets <package>: print one TAB-separated line per declared
# INTEGRATION (`test`) target of that workspace package — `<name>\t<required-features
# comma-joined, empty when none>` (issue #1699, roborev round-2 finding 2).
#
# Why this exists. `cargo test -p <pkg>` SILENTLY SKIPS any `[[test]]` target whose
# `required-features` are not all enabled: it is never built, never run, and — the
# part that matters — it emits NO `Running tests/<name>.rs` line at all, so the #2039
# zero-tests guard (which keys on that line) cannot see it. Measured on this package:
# `issue_1494_producer_mem_budget` (`required-features = ["dhat-heap"]`) appeared 0
# times in a lane log that showed 41 `Running` lines, so a lane claiming "every
# integration target" was overstating by exactly the targets nobody can observe.
#
# The oracle is `cargo metadata`, i.e. CARGO ITSELF, for the same reason
# _resolved_package_features uses it rather than parsing `[features]`: a hand-parse of
# `[[test]]` sections would be a SECOND IMPLEMENTATION of cargo's target
# auto-discovery (tests/*.rs are targets with NO stanza at all) and its correctness
# would only be knowable by differential testing against the original.
#
# Fail-closed, same chain and same direction as _resolved_package_features: jq, else
# python3, else failure. An empty result is a FAILED DERIVATION (return 1), never "the
# package declares no test targets" — treating emptiness as an answer would excuse
# every unobserved target at once, which is the vacuous-green shape.
_package_test_targets() {
  # Both parser substitutions carry `|| return 1` (roborev round-27, Medium): a parser that emits
  # SOME records and then fails left this helper returning success with a PARTIAL target census,
  # which understates the coverage gap the flight lane exists to declare — a smaller gap is the
  # permissive direction, and the caller cannot tell a partial census from a complete one.
  local pkg="$1"
  local meta out
  meta=$(cargo metadata --format-version 1 2>/dev/null) || return 1
  [ -n "$meta" ] || return 1
  if command -v jq >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | jq -r --arg n "$pkg" \
      '.packages[] | select(.name == $n) | .targets[]
       | select(.kind | index("test"))
       | [.name, ((."required-features" // []) | join(","))] | @tsv') || return 1
  elif command -v python3 >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | python3 -c '
import json, sys
n = sys.argv[1]
d = json.load(sys.stdin)
for p in d["packages"]:
    if p["name"] != n:
        continue
    for t in p.get("targets", []):
        if "test" in (t.get("kind") or []):
            print("%s\t%s" % (t["name"], ",".join(t.get("required-features") or [])))
' "$pkg") || return 1
  else
    return 1
  fi
  [ -n "$out" ] || return 1
  printf '%s\n' "$out"
}

# _package_integration_target_ids <package>: print one TAB-separated line per declared
# INTEGRATION (`test`) target — `<name>\t<runner-id>\t<required-features comma-joined>`
# — and, UNLIKE _package_test_targets, treat "this package declares none" as a REAL
# ANSWER rather than a failed derivation (issue #3522).
#
# Two reasons it is not just another caller of _package_test_targets.
#
#  1. ZERO IS A FACT HERE. _package_test_targets fails closed on an empty result
#     because every one of its callers' packages declares integration targets, so
#     emptiness there can only be a broken derivation. cqlite-node declares NONE, and
#     the binding-rust-tests census must state that as a DERIVED fact ("this package
#     has no integration targets") rather than as an assumption or a FAIL. Emptiness
#     and failure are therefore distinguished by an explicit PRESENCE check on the
#     package itself: a package cargo does not know about is a FAILED derivation
#     (return 1); a package it knows about that declares no test target prints nothing
#     and returns 0.
#
#  2. THE RUNNER ID. cargo's `Running tests/<path>.rs` banner — the string every
#     observation guard here keys on — carries the target's path RELATIVE TO tests/,
#     which equals the target NAME for a file-style target (`tests/foo.rs` -> `foo`)
#     but NOT for a directory-style one (`tests/foo/main.rs` -> `foo/main`, name
#     `foo`). Deriving the id from cargo's own `src_path` makes the guard's expectation
#     agree with cargo's output BY CONSTRUCTION, so adding a directory-style target
#     cannot turn a healthy lane red. A target mapped outside tests/ by an explicit
#     `[[test]] path = "..."` yields its package-relative path, which is the second
#     spelling check_no_unexpected_zero_tests already recognises.
#
# Same parser chain and same direction as its neighbours: jq, else python3, else a
# FAILED derivation. Never a fallback to a partial or empty census.
_package_integration_target_ids() {
  local pkg="$1"
  local meta present out
  meta=$(cargo metadata --format-version 1 --no-deps 2>/dev/null) || return 1
  [ -n "$meta" ] || return 1
  if command -v jq >/dev/null 2>&1; then
    # PRESENCE first: without it, an unknown/renamed package would print nothing and
    # be reported as "declares no integration targets" — a false all-clear about a
    # package the lane cannot see at all.
    present=$(printf '%s' "$meta" | jq -r --arg n "$pkg" '[.packages[] | select(.name == $n)] | length') || return 1
    [ "$present" = 1 ] || return 1
    out=$(printf '%s' "$meta" | jq -r --arg n "$pkg" \
      '.packages[] | select(.name == $n)
       | ((.manifest_path // "") | split("/") | .[0:-1] | join("/")) as $root
       | .targets[]
       | select(.kind | index("test"))
       | ((.src_path // "")) as $sp
       | (if ($root != "" and ($sp | startswith($root + "/")))
          then ($sp | ltrimstr($root + "/")) else $sp end) as $rel
       | (if ($rel | startswith("tests/")) then ($rel | ltrimstr("tests/")) else $rel end) as $rel2
       | (if ($rel2 | endswith(".rs")) then ($rel2 | .[0:-3]) else $rel2 end) as $id
       | [.name, $id, ((."required-features" // []) | join(","))] | @tsv') || return 1
  elif command -v python3 >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | python3 -c '
import json, os, sys
pkg = sys.argv[1]
d = json.load(sys.stdin)
pkgs = [p for p in d.get("packages", []) if p.get("name") == pkg]
if len(pkgs) != 1:
    sys.exit(1)
p = pkgs[0]
root = os.path.dirname(p.get("manifest_path", ""))
for t in p.get("targets", []):
    if "test" not in (t.get("kind") or []):
        continue
    sp = t.get("src_path") or ""
    rel = sp[len(root) + 1:] if root and sp.startswith(root + os.sep) else sp
    if rel.startswith("tests/"):
        rel = rel[len("tests/"):]
    if rel.endswith(".rs"):
        rel = rel[:-3]
    print("%s\t%s\t%s" % (t["name"], rel, ",".join(t.get("required-features") or [])))
' "$pkg") || return 1
  else
    return 1
  fi
  # NO `[ -n "$out" ] || return 1` HERE, DELIBERATELY — that is the one line that
  # distinguishes this helper from _package_test_targets. See reason 1 above.
  printf '%s' "$out"
  [ -n "$out" ] && printf '\n'
  return 0
}

# THE FLAKE-QUARANTINE PLUMBING IS RETIRED, DELIBERATELY (issues #3383/#3384).
#
# It used to live here: `FLIGHT_FLAKE_SKIPS`, a curated `<target>:<issue>` list that
# excluded named cqlite-flight integration targets from the flight-tests lane, plus
# `_validate_flight_flake_skips`, which failed closed on a malformed or stale entry.
#
# Why it is GONE rather than kept inert. It existed for exactly one purpose: to paper
# over the non-determinism of cqlite-flight's integration suite, one victim at a time.
# That approach was measured and REJECTED (owner ruling, #3384) — two distinct victims
# appeared in four runs, which is not a converging series, so a per-victim quarantine
# has no visible end and would become the dumping ground its own design rule forbids.
# The lane instead runs `--lib --bins` and DECLARES the whole integration half as an
# un-run gap (see run_flight_tests's census). With no lane executing those targets
# locally, the list has NO SUBJECT: an empty curated list, and a validator whose only
# caller is gone, would be a guard reporting OK having measured nothing — the vacuous
# shape both this issue and #1699 exist to eliminate. So there is no code path that
# reads a flake list and silently does nothing, because there is no flake list.
#
# If #3384's fix ever needs a target-granular exclusion again, reintroduce it WITH its
# validator (a curated excusal list is the thing that rots silently, so both halves of
# every entry must be enforced) — git history at this line has the working version.

# check_declared_test_targets_observed <label> <logfile> <enabled-set> <target-metadata> <skips>
#
# RETAINED BUT CURRENTLY UNCALLED — read this first (issues #3384/#1699). Its only
# caller was the flight-tests lane while that lane executed cqlite-flight's integration
# targets. It no longer does: the integration half of that package is ~50%
# non-deterministic under intra-package parallelism, so the lane narrowed to
# `--lib --bins` and now DECLARES the whole integration half as an un-run gap (see
# run_flight_tests's census). Calling this reconciliation from a lane that puts no
# integration target on the command line would FAIL every one of them, correctly and
# uselessly.
#
# It is kept rather than deleted because it is the reconciliation the WIDENED lane will
# call again the moment #3384 makes that suite deterministic, and re-deriving a subtle
# fail-closed guard from scratch is how it comes back weaker. WHAT WILL CALL IT AGAIN:
# run_flight_tests, once its command line carries `--test` targets — with an EMPTY
# <skips> argument, since the flake-quarantine plumbing is retired (see the note above)
# and #3384's resolution is a fix, not a quarantine. `_package_test_targets`, which
# feeds it, IS still called: the lane uses it to count the targets its census reports
# as un-run, which is what makes that census a measurement rather than a claim.
#
# Reconcile the DERIVED set of declared integration targets (see
# _package_test_targets) against the targets actually OBSERVED emitting
# `Running tests/<name>.rs` in the component log, and FAIL CLOSED naming any target
# that is unobserved without BOTH halves of an explanation (issue #1699, roborev
# round-2 finding 2). This is the counterpart to check_no_unexpected_zero_tests: that
# guard catches a target that RAN and executed nothing; this one catches a target that
# was never even BUILT, which prints nothing and is therefore invisible to it.
#
# An unobserved target has exactly THREE possible explanations, each named
# EXPLICITLY in the diagnostic and never folded into another, because they are
# different facts about different actors:
#
#   1. SKIPPED BY THE CALLER — the target is named in the <skips> argument as
#      `<target>:<issue>`, i.e. the CALLING LANE CHOSE not to execute it and the entry
#      names the issue that obliges its return. Distinct from category 2/3 below on
#      purpose: "cargo cannot run it here" and "we decided not to run it" are not the
#      same claim, and collapsing them would hide a deliberate coverage decision behind
#      a mechanical one. NO CALLER PASSES A NON-EMPTY <skips> TODAY (the flake-quarantine
#      plumbing is retired, #3384), so this branch is dormant; a future caller reviving
#      it owes the list its own fail-closed validator, because a curated excusal list is
#      exactly the thing that rots silently.
#
#   Otherwise the target must satisfy BOTH remaining halves, and the reason is printed
#   either way:
#   (a) EXPLAINED — its `required-features` are non-empty AND at least one of them is
#       not in this lane's enabled set, so cargo's silent skip is accounted for by
#       cargo's own rules rather than by a guess; AND
#   (b) ALTERNATE EXECUTOR — some OTHER component of this gate script actually INVOKES
#       the target: a NON-COMMENT `--test <name>` line (memory-budget carries the only
#       one for issue_1494_producer_mem_budget). Mechanical, read from committed source,
#       so there is no curated excusal list — and deliberately NOT a bare substring
#       search, which the comments above would satisfy by themselves: an artifact
#       DESCRIBING the excusal would BECOME the excusal (#3312's shape), and the target
#       would stay excused after its only real executor was deleted.
#
# Anything else — an unobserved target with no off required-feature, or one no
# component names — is the invisible skip this lane exists to prevent, and FAILs
# naming the target and which half is missing. A positive verdict prints the
# affirmative measurement (how many declared targets were observed), so a pasted log
# shows the reconciliation RAN.
check_declared_test_targets_observed() {
  local label="$1" logfile="$2" enabled="$3" meta="$4" skips="$5"
  if [ ! -r "$GATE_SELF" ]; then
    echo "$label: FAIL-CLOSED — cannot read $GATE_SELF, so the alternate-executor half of the declared-vs-observed reconciliation is unmeasurable (issue #1699)" >&2
    return 1
  fi
  local observed declared=0 seen=0
  observed=" $(grep -oE 'Running tests/[^[:space:]]+\.rs' "$logfile" \
    | sed -E 's#^Running tests/(.*)\.rs$#\1#' | sort -u | tr '\n' ' ') "
  local bad="" excused="" flaky="" tname rf rfl off sk skissue
  while IFS=$'\t' read -r tname rf; do
    [ -n "$tname" ] || continue
    declared=$((declared + 1))
    case "$observed" in
      *" $tname "*) seen=$((seen + 1)); continue ;;
    esac
    # Category 1: this lane deliberately did not run it. Checked FIRST and reported
    # under its own label — a flake-skipped target is unobserved with no off
    # required-feature, so without this branch it would FAIL as
    # `unobserved-and-UNEXPLAINED`, and folding it into the required-features excusal
    # instead would misreport a CHOICE as a cargo limitation.
    skissue=""
    for sk in $skips; do
      if [ "${sk%%:*}" = "$tname" ]; then skissue="${sk#*:}"; break; fi
    done
    if [ -n "$skissue" ]; then
      flaky="$flaky $tname(flake-skipped:issue #$skissue)"
      continue
    fi
    off=""
    for rfl in ${rf//,/ }; do
      case "$enabled" in *" $rfl "*) ;; *) off="$rfl"; break ;; esac
    done
    if [ -z "$off" ]; then
      if [ -z "$rf" ]; then
        bad="$bad $tname(unobserved-and-UNEXPLAINED:no-required-features)"
      else
        bad="$bad $tname(unobserved-and-UNEXPLAINED:required-features[$rf]-are-all-enabled)"
      fi
      continue
    fi
    # The alternate executor must be a REAL cargo target reference on a NON-COMMENT
    # line. A bare substring search over this file would be satisfied by PROSE — the
    # comments above name `issue_1494_producer_mem_budget` while explaining this very
    # mechanism, so an artifact DESCRIBING the excusal would BECOME the excusal, and the
    # target would stay excused after its only real executor was deleted (#3312's
    # lesson; the same `#`-blind-scan defect this change filed as #3380 against the
    # roborev guard). Measured: that target appears 5x here — 4 comments and ONE real
    # invocation (`--test issue_1494_producer_mem_budget`, in memory-budget).
    # If a component ever executes a target by some other form (a nextest filter
    # expression, say), this FAILs closed and the pattern gets widened deliberately.
    if ! grep -qE "^[^#]*--test[[:space:]]+$tname([[:space:]]|\$)" "$GATE_SELF"; then
      bad="$bad $tname(required-features[$rf]-off[$off]-but-NO-alternate-executor-INVOKES-it:no non-comment \`--test $tname\` in agent-gate.sh)"
      continue
    fi
    excused="$excused $tname(required-features[$rf]:off[$off];alternate-executor-INVOKES-it:non-comment \`--test $tname\` in agent-gate.sh)"
  done <<< "$meta"
  if [ -n "$flaky" ]; then
    echo "$label: declared-vs-observed FLAKE-SKIPPED (this lane DELIBERATELY did not execute these — a CURATED exclusion, categorically distinct from cargo being unable to run a target; each names the issue obliging its return):$flaky" >&2
  fi
  if [ -n "$excused" ]; then
    echo "$label: declared-vs-observed EXCUSED (cargo silently skips a required-features target it cannot enable; another gate component executes it):$excused" >&2
  fi
  if [ -n "$bad" ]; then
    echo "$label: FAIL-CLOSED —$bad declared as an integration target but NEVER OBSERVED running, and explained by NONE of the three permitted categories (flake-skipped with an issue; an off required-feature WITH an alternate executor) (issue #1699: cargo skips such a target silently, printing no 'Running tests/' line at all, so the #2039 zero-tests guard cannot see it)" >&2
    return 1
  fi
  echo "$label: declared-vs-observed OK — $seen/$declared declared integration targets observed running (cargo metadata vs 'Running tests/' lines)" >&2
  return 0
}

run_component() { # run_component <name> <cmd...>
  local name="$1"; shift
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  echo ">>> [$name] $*"
  start=$(date +%s)
  if "$@" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# python-bindings: build the extension with maturin and run pytest. Unlike the
# Rust components this is SKIP-aware: if there is no usable python3 the component
# records SKIP (loudly, never silently PASS) so a missing toolchain can't mask a
# real Python regression the way it did pre-#865. Anything else (venv/build/test
# failure) is a hard FAIL.
run_python_bindings() {
  local name=python-bindings
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  # Persistent venv under target/ so repeat runs skip the maturin/pytest install.
  local venv="$REPO_ROOT/target/agent-gate-venv"
  echo ">>> [$name] maturin develop + import-verify + pytest (venv: $venv, RUN_SLOW_TESTS=${RUN_SLOW_TESTS:-0})"
  # Build + VERIFY the extension imports (self-heals a stale/half-built editable
  # install once — issue #1803), THEN run pytest against the WRITTEN-BACK active
  # venv. A ModuleNotFoundError from a venv-resolution miss self-heals to a clean
  # rebuild rather than falsely FAILing green code; a genuine build/import defect
  # FAILs with a distinct message (already in $log). The self-heal branch never
  # touches the SHARED $venv (roborev round-2 Finding B: a concurrent same-
  # checkout gate reusing it would otherwise race a mid-build `rm -rf`) — it
  # builds into a private per-process venv instead, whose path the hook writes
  # to $active_venv_file so this shell knows what to activate for pytest and
  # clean up afterward.
  local active_venv_file active_venv
  active_venv_file=$(mktemp "${TMPDIR:-/tmp}/agent-gate-active-venv.XXXXXX")
  if bash "$GATE_SELF" --python-build-verify "$venv" "maturin develop -m bindings/python/Cargo.toml" "$active_venv_file" >"$log" 2>&1; then
    active_venv=$(cat "$active_venv_file" 2>/dev/null); [ -n "$active_venv" ] || active_venv="$venv"
    if RUN_SLOW_TESTS="${RUN_SLOW_TESTS:-0}" bash -c '
        set -euo pipefail
        . "'"$active_venv"'/bin/activate"
        pytest bindings/python/tests -q' >>"$log" 2>&1; then
      status=PASS
    else
      status=FAIL
    fi
  else
    status=FAIL
    active_venv=$(cat "$active_venv_file" 2>/dev/null); [ -n "$active_venv" ] || active_venv="$venv"
  fi
  # Clean up a private self-heal venv (never the shared $venv) so heal venvs
  # don't accumulate under target/.
  [ "$active_venv" != "$venv" ] && rm -rf "$active_venv"
  rm -f "$active_venv_file"
  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# node-bindings: build the napi-rs native module and run the #1231 Node
# write→read CONTENT proof. Symmetric to run_python_bindings and SKIP-aware:
# if there is no node/npm on PATH the component records SKIP (loudly, never
# silently PASS) so a missing toolchain can't mask a real Node write-path
# regression. Anything else (install/build/test failure) is a hard FAIL.
#
# Scope (#1255): we run the content proof specifically (npx jest
# write-readback-content) rather than the full `npm test`. The full Node suite
# pulls in corpus-dependent parity/smoke tests and a slow `--release` napi
# build; scoping to the content proof keeps the gate fast and reliable while
# guaranteeing the load-bearing #1231 test executes fail-closed. The content
# test self-generates its SSTables, so it needs no fixture corpus (hence
# node-bindings is NOT in DATASET_COMPONENTS); CQLITE_DATASETS_ROOT is still
# exported defensively for any test that reads it.
run_node_bindings() {
  local name=node-bindings
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no node/npm on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] npm ci + npm run build + jest write-readback-content (#1231)"
  if CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" bash -c '
      set -euo pipefail
      cd "'"$REPO_ROOT"'/bindings/node"
      if [ -f package-lock.json ]; then npm ci; else npm install; fi
      npm run build
      npx jest write-readback-content' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# _package_declared_features <package>: print every feature NAME the package's own
# manifest declares, one per line (possibly none), from cargo metadata. Exit 1 = the
# derivation failed (no jq/python3, a metadata failure, or no such package) — never a
# silent empty list, which would let a lane report "nothing is turned off" about a
# package it could not read (issue #3522).
#
# Its consumer is the binding-rust-tests census: subtracting the RESOLVED enabled set
# (_resolved_package_features) from this DECLARED set is how the lane states, as a
# derived fact rather than a curated sentence, which of a binding crate's features it
# leaves off — and therefore which `#[cfg(feature = ...)]` test bodies it does not run.
_package_declared_features() {
  local pkg="$1"
  local meta present out
  meta=$(cargo metadata --format-version 1 --no-deps 2>/dev/null) || return 1
  [ -n "$meta" ] || return 1
  if command -v jq >/dev/null 2>&1; then
    present=$(printf '%s' "$meta" | jq -r --arg n "$pkg" '[.packages[] | select(.name == $n)] | length') || return 1
    [ "$present" = 1 ] || return 1
    out=$(printf '%s' "$meta" | jq -r --arg n "$pkg" \
      '.packages[] | select(.name == $n) | (.features // {}) | keys[]') || return 1
  elif command -v python3 >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | python3 -c '
import json, sys
pkg = sys.argv[1]
d = json.load(sys.stdin)
pkgs = [p for p in d.get("packages", []) if p.get("name") == pkg]
if len(pkgs) != 1:
    sys.exit(1)
for f in sorted((pkgs[0].get("features") or {}).keys()):
    print(f)
' "$pkg") || return 1
  else
    return 1
  fi
  # Emptiness is a legitimate answer here (cqlite-ffi-common declares no features at
  # all), for the same reason and with the same presence check as
  # _package_integration_target_ids.
  printf '%s' "$out"
  [ -n "$out" ] && printf '\n'
  return 0
}

# binding-rust-tests: EXECUTE the RUST test suites of the two binding-side crates that
# no other gate component runs — cqlite-ffi-common (whole package) and cqlite-node
# (--lib) (issue #3522).
#
# THE DEFECT IT EXISTS FOR. Compiling is not covering, and #1699 established that at
# FEATURE granularity. The same reasoning holds at PACKAGE granularity, and two crates
# were sitting in exactly that hole: `cqlite-ffi-common` appeared ZERO times in
# scripts/** and .github/workflows/**, so its 37 unit tests and its two integration
# targets (tests/dependency_boundary.rs, tests/error_contract_table.rs) were reached
# only by clippy's `--all-targets` compile and executed NOWHERE — not locally, not in
# CI. `cqlite-node`'s 53 Rust unit tests were in the same position: node-bindings runs
# jest against the BUILT ARTIFACT and never `cargo test`. An inverted assertion in
# either could be committed, merged and released with every check green.
#
# WHY THIS IS A SEPARATE COMPONENT AND NOT PART OF node-bindings. node-bindings SKIPs
# when node/npm is absent — correctly, since without them it can build nothing. Putting
# cqlite-node's RUST tests behind that SKIP would mean a box with no npm silently stops
# executing them: a coverage hole wearing a SKIP's clothes, which is this issue's own
# defect class re-created by the fix. This component therefore depends on NOTHING beyond
# cargo and NEVER SKIPs. The same argument keeps cqlite-ffi-common out of
# python-bindings.
#
# FEATURE SETS, chosen and stated rather than defaulted into:
#   * cqlite-ffi-common — none. The crate declares NO `[features]` table (derived and
#     printed on every run), so there is no other set to choose.
#   * cqlite-node — `--features write-support`, because that is what the SHIPPED
#     artifact is built with (`npm run build` = `napi build … --features write-support`),
#     so this lane runs the Rust half at the feature set the product actually ships. It
#     costs nothing: cqlite-node's cqlite-core dependency already takes default features,
#     which include write-support, so the flag adds no compilation. `observability` is
#     deliberately NOT enabled — building the OTel stack is a cost this gate declines on
#     purpose (#1844 excludes that stack from clippy for the same reason) — and the
#     census DECLARES that, with the un-enabled feature set DERIVED, not listed by hand.
#
# AFFIRMATIVE MEASUREMENT, not a green exit code. A suite whose modules are cfg'd out
# compiles, runs 0 tests and exits 0. Three guards, all over cargo's own output:
#   * check_unittest_targets_ran   — per package, each selected `--lib` target must be
#                                    OBSERVED and must have run a NON-ZERO count.
#   * check_test_targets_observed  — every DERIVED cqlite-ffi-common integration target
#                                    must have produced a `Running` banner (this is the
#                                    guard that makes `dependency_boundary.rs` running in
#                                    the gate of record a checkable fact, not a hope).
#   * check_no_unexpected_zero_tests — with an EMPTY allowed-zero list, so any
#                                    integration target that runs zero tests FAILs.
# The two packages write to SEPARATE log files, deliberately: both print
# `Running unittests src/lib.rs`, and check_unittest_targets_ran keys on that path, so a
# single shared log would let one package's unit run satisfy the guard for the other.
#
# DERIVE, NEVER CURATE. Every subject set — integration targets, their runner ids,
# unittest targets, enabled features, declared features — comes from `cargo metadata` /
# `cargo tree` at run time, so a new `tests/*.rs` in either crate is covered with no gate
# edit. A FAILED derivation is a FAIL NAMING THE DERIVATION, never a fallback to "nothing
# to run": a silently empty subject set is the vacuous green this component was created
# to remove.
#
# NOT IN DATASET_COMPONENTS, verified rather than assumed: neither `cqlite-ffi-common/`
# nor `bindings/node/src/` contains any reference to `CQLITE_DATASETS_ROOT` or `test-data`
# (measured; the jest suite does read the corpus, but that is node-bindings' subject, not
# this lane's). The root is still exported defensively.
run_binding_rust_tests() {
  local name=binding-rust-tests
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local ffi_log="$LOG_DIR/$name.cqlite-ffi-common.log"
  local node_log="$LOG_DIR/$name.cqlite-node.log"
  local start end status
  start=$(date +%s)

  # Declared ONCE and consumed by BOTH the cargo invocation and the enabled-set
  # derivation, so the two can never describe different builds.
  local -a ffi_feature_args=()
  local -a node_feature_args=(--features write-support)

  # --- one place to report a failed DERIVATION -------------------------------
  # Every derivation below is fatal in the same way and for the same reason, so they
  # report through one helper rather than five near-copies that could drift apart.
  local _derivation_failed=0
  _brt_derivation_fail() { # <what> <why...>
    local what="$1"; shift
    {
      echo "[$name] FAIL-CLOSED: could not derive $what."
      echo "        $*"
      echo "        The DERIVATION failed, not the tests. This component's subject sets are"
      echo "        derived from cargo at run time so a new test target is covered with no gate"
      echo "        edit; an underived (or silently empty) subject set is the vacuous green"
      echo "        issue #3522 exists to remove, so it FAILs naming the derivation."
    } >> "$log"
    _derivation_failed=1
  }

  : > "$log"

  # NOTE the empty-but-successful case: cqlite-ffi-common declares no features at all, so
  # this legitimately returns the EMPTY SET. That is a measurement, and treating it as a
  # failure is exactly the false red #3522 corrected in _resolved_package_features.
  local ffi_enabled="" node_enabled=""
  if ! ffi_enabled=$(_resolved_package_features cqlite-ffi-common ${ffi_feature_args[@]+"${ffi_feature_args[@]}"}); then
    _brt_derivation_fail "cqlite-ffi-common's enabled feature set" \
      "'cargo tree -p cqlite-ffi-common' emitted no line for the package (a cargo failure or an offline registry)."
  fi
  if ! node_enabled=$(_resolved_package_features cqlite-node ${node_feature_args[@]+"${node_feature_args[@]}"}); then
    _brt_derivation_fail "cqlite-node's enabled feature set" \
      "'cargo tree -p cqlite-node --features write-support' emitted no line for the package."
  fi

  # cqlite-ffi-common's integration targets. This package HAS them, so an empty result
  # is a broken derivation and is reported as one.
  local ffi_targets=""
  if ! ffi_targets=$(_package_integration_target_ids cqlite-ffi-common); then
    _brt_derivation_fail "cqlite-ffi-common's integration (test) targets" \
      "cargo metadata, its parser, or the package lookup failed."
  elif [ -z "$ffi_targets" ]; then
    _brt_derivation_fail "cqlite-ffi-common's integration (test) targets" \
      "the census counted ZERO, but this package declares tests/dependency_boundary.rs and tests/error_contract_table.rs. A zero here is a broken count, and 'no targets to observe' would silently retire the guard that proves dependency_boundary.rs runs."
  fi

  # cqlite-node's integration targets. Zero is the EXPECTED, DERIVED answer here (the
  # crate is a cdylib with no tests/ directory) — which is why this uses the
  # zero-tolerant helper. The census states the number it measured; it does not assume
  # it.
  local node_targets="" node_targets_n=0
  if ! node_targets=$(_package_integration_target_ids cqlite-node); then
    _brt_derivation_fail "cqlite-node's integration (test) target census" \
      "cargo metadata, its parser, or the package lookup failed — so this lane cannot state whether it is leaving any integration target un-run."
  else
    node_targets_n=$(printf '%s' "$node_targets" | grep -c . || true)
  fi

  # The declared-minus-enabled feature sets: what this lane leaves OFF, derived.
  local ffi_declared node_declared ffi_off="" node_off="" _f
  if ! ffi_declared=$(_package_declared_features cqlite-ffi-common); then
    _brt_derivation_fail "cqlite-ffi-common's declared feature list" "cargo metadata or its parser failed."
  fi
  if ! node_declared=$(_package_declared_features cqlite-node); then
    _brt_derivation_fail "cqlite-node's declared feature list" "cargo metadata or its parser failed."
  fi
  for _f in $ffi_declared; do
    case "$ffi_enabled" in *" $_f "*) ;; *) ffi_off="$ffi_off $_f" ;; esac
  done
  for _f in $node_declared; do
    case "$node_enabled" in *" $_f "*) ;; *) node_off="$node_off $_f" ;; esac
  done

  # The unittest subject sets. `lib,cdylib,bin` rather than `lib,bin`: cqlite-node's
  # library target has kind `cdylib` (it is a napi module), and a `lib,bin` filter
  # returns NOTHING for it — which _package_unittest_srcs correctly reports as a failed
  # derivation, and which a lane that shrugged at would turn into a guard with no
  # subject.
  local -a ffi_unit_srcs=() node_unit_srcs=()
  local _us
  while IFS= read -r _us; do [ -n "$_us" ] && ffi_unit_srcs+=("$_us"); done <<EOF
$(_package_unittest_srcs cqlite-ffi-common lib "$ffi_enabled")
EOF
  while IFS= read -r _us; do [ -n "$_us" ] && node_unit_srcs+=("$_us"); done <<EOF
$(_package_unittest_srcs cqlite-node lib,cdylib,bin "$node_enabled")
EOF
  [ "${#ffi_unit_srcs[@]}" -gt 0 ] || _brt_derivation_fail "cqlite-ffi-common's lib unittest target(s)" \
    "cargo metadata returned none, so the zero-test guard would have no subject."
  [ "${#node_unit_srcs[@]}" -gt 0 ] || _brt_derivation_fail "cqlite-node's lib unittest target(s)" \
    "cargo metadata returned none (note its library target's kind is 'cdylib', not 'lib'), so the zero-test guard would have no subject."

  # The count of Rust #[test] fns in bindings/python/src, for the census's cqlite-py
  # clause. GREP-COUNTED, and the census says so: this counts `#[test]` ATTRIBUTES in
  # committed source, which is a proxy for "test functions" and not a cargo-derived
  # figure — cargo cannot give one, because the target cannot be built (that is the
  # whole point of the clause). Fail-closed on an unreadable subject or a non-numeric
  # result: a census that quietly reports "0 unrun tests" is a false all-clear.
  local py_src="$REPO_ROOT/bindings/python/src" py_test_n=""
  if [ ! -d "$py_src" ] || [ ! -r "$py_src" ]; then
    _brt_derivation_fail "the count of Rust #[test] fns under bindings/python/src" \
      "the directory is missing or unreadable, so the census cannot state the size of the gap it reports."
  else
    py_test_n=$(grep -rhoE --include='*.rs' '^[[:space:]]*#\[test\]' "$py_src" 2>/dev/null | grep -c . || true)
    case "$py_test_n" in
      ''|*[!0-9]*) _brt_derivation_fail "the count of Rust #[test] fns under bindings/python/src" \
                     "the count came back non-numeric ('$py_test_n')." ;;
    esac
  fi

  if [ "$_derivation_failed" -ne 0 ]; then
    status=FAIL
    cat "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ---- THE CENSUS -----------------------------------------------------------
  # Built ONCE and emitted TWICE — as `>>>` lines on the gate's stdout and at the HEAD
  # of the component log — because a lane that omits coverage silently is
  # indistinguishable from one that covers it, and "the log a reviewer actually reads"
  # is both of those. Not a comment: a comment is not read on a run.
  local ffi_ids="" ffi_expect_n=0 ffi_skip="" _tn _tid _trf _troff _trfl
  local -a ffi_expect=()
  while IFS=$'\t' read -r _tn _tid _trf; do
    [ -n "$_tn" ] || continue
    ffi_ids="$ffi_ids $_tid"
    _troff=""
    for _trfl in ${_trf//,/ }; do
      case "$ffi_enabled" in *" $_trfl "*) ;; *) _troff="$_trfl"; break ;; esac
    done
    if [ -n "$_troff" ]; then
      # cargo SILENTLY skips a required-features target it cannot enable, printing no
      # banner at all — so demanding an observation for it would red a healthy lane.
      # Excluded from the expectation and DECLARED, never dropped quietly.
      ffi_skip="$ffi_skip $_tid(required-features[$_trf]:off[$_troff])"
    else
      ffi_expect+=("$_tid")
      ffi_expect_n=$((ffi_expect_n + 1))
    fi
  done <<< "$ffi_targets"

  local -a census=()
  census+=("cargo test --no-fail-fast -p cqlite-ffi-common            (ALL targets: lib + every integration target)")
  census+=("cargo test --no-fail-fast -p cqlite-node --features write-support --lib")
  census+=("WHY THIS LANE EXISTS: before it, BOTH crates' Rust tests executed NOWHERE — not in")
  census+=("     this gate, not in CI. clippy --all-targets COMPILED them and nothing RAN them.")
  census+=("     Compiling is not covering (#1699), and that holds at PACKAGE granularity too.")
  census+=("SUBJECTS (all DERIVED from cargo at run time, never hard-coded):")
  census+=("  cqlite-ffi-common: unittest target(s) [${ffi_unit_srcs[*]}]; $ffi_expect_n integration target(s) [${ffi_ids# }]")
  census+=("  cqlite-node:       unittest target(s) [${node_unit_srcs[*]}]; $node_targets_n integration target(s)")
  census+=("COVERAGE CENSUS — WHAT THIS LANE DOES NOT RUN:")
  census+=("  1. cqlite-py's Rust #[test] fns ($py_test_n occurrences of '#[test]' under")
  census+=("     bindings/python/src, grep-counted from committed source — cargo cannot count them,")
  census+=("     because the target cannot be built). 'cargo test -p cqlite-py' is STRUCTURALLY")
  census+=("     IMPOSSIBLE, not merely unwired: a pyo3 cdylib's test harness cannot link libpython.")
  census+=("     Already documented in this script (search: 'cannot link libpython'). The pytest")
  census+=("     half IS fully covered, by the python-bindings component.")
  census+=("  2. cqlite-node's JavaScript suite. Owned by the node-bindings component, which builds")
  census+=("     the napi artifact and runs jest against it. This lane never builds that artifact.")
  census+=("  3. cqlite-node integration (test) targets: it declares $node_targets_n — a DERIVED count")
  census+=("     from cargo metadata, not an assumption. At $node_targets_n there is nothing to omit; if")
  census+=("     that number ever rises without this lane running them, this line is the alarm.")
  census+=("  4. Feature-gated bodies at features this lane leaves OFF (declared-minus-enabled,")
  census+=("     derived): cqlite-ffi-common ->${ffi_off:- <none: this crate declares no features>};")
  census+=("     cqlite-node ->${node_off:- <none>}. 'observability' is off ON PURPOSE — building the")
  census+=("     OTel stack is a cost this gate declines (#1844 excludes it from clippy likewise).")
  [ -n "$ffi_skip" ] && census+=("  5. cqlite-ffi-common targets cargo cannot run at this feature set:$ffi_skip")
  census+=("SCOPE OF THE #3522 AUDIT, recorded so it is not re-litigated: this component closes TWO")
  census+=("     of the ten gaps that audit found. The other eight are RECORDED, not silently fixed,")
  census+=("     in scripts/tests/workspace-test-disposition.txt (enforced by")
  census+=("     scripts/tests/test_workspace_test_disposition.sh under the tooling-tests component).")
  local cl
  for cl in "${census[@]}"; do echo ">>> [$name] $cl"; done
  echo ">>> [$name] enabled features (cargo tree -p, package-scoped): cqlite-ffi-common [$ffi_enabled] cqlite-node [$node_enabled]"
  {
    echo "==== [$name] COVERAGE CENSUS (issue #3522) ===="
    for cl in "${census[@]}"; do echo "$cl"; done
    echo "enabled features (cargo tree -p, package-scoped): cqlite-ffi-common [$ffi_enabled] cqlite-node [$node_enabled]"
    echo "per-package cargo logs: $ffi_log , $node_log"
    echo "==== end census ===="
  } > "$log"

  # --no-fail-fast for the reason flight-tests and legacy-heuristics carry it: cargo
  # test stops after the first failing test BINARY, and a lane whose purpose is to
  # surface never-executed rot must surface ALL of it in one run rather than as a serial
  # reveal.
  status=PASS

  # ---- cqlite-ffi-common: whole package -------------------------------------
  if env CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
      cargo test --no-fail-fast -p cqlite-ffi-common \
      ${ffi_feature_args[@]+"${ffi_feature_args[@]}"} > "$ffi_log" 2>&1; then
    # Three affirmative guards, ANDed. Order matters only for readability; each writes
    # its own verdict to the log, so a pasted log shows which ones RAN and on what.
    if ! check_unittest_targets_ran "$name/cqlite-ffi-common" "$ffi_log" "${ffi_unit_srcs[@]}" 2>>"$ffi_log"; then
      status=FAIL
    fi
    if [ "$ffi_expect_n" -gt 0 ]; then
      if ! check_test_targets_observed "$name/cqlite-ffi-common" "$ffi_log" "${ffi_expect[@]}" 2>>"$ffi_log"; then
        status=FAIL
      else
        echo "$name/cqlite-ffi-common: integration targets OK — all $ffi_expect_n derived target(s) produced a 'Running' banner:${ffi_ids}" >> "$ffi_log"
      fi
    else
      # Unreachable while this package declares runnable targets (the derivation above
      # FAILs on zero), but stated rather than left implicit: an expectation set that
      # emptied itself would be a guard with no subject.
      echo "$name/cqlite-ffi-common: FAIL-CLOSED — every declared integration target was excused by an unmet required-feature, leaving the observation guard with NO subject (issue #3522)." >> "$ffi_log"
      status=FAIL
    fi
    # EMPTY allowed-zero list, deliberately: no cqlite-ffi-common integration target is
    # permitted to run zero tests, so a cfg change that empties one FAILs here.
    if ! check_no_unexpected_zero_tests "$name/cqlite-ffi-common" "$ffi_log" 2>>"$ffi_log"; then
      status=FAIL
    fi
  else
    status=FAIL
  fi

  # ---- cqlite-node: --lib ---------------------------------------------------
  # Yes, a `crate-type = ["cdylib"]` package: `cargo test --lib` compiles the library as
  # a TEST harness binary, which links fine (measured: 53 tests). This is NOT the
  # cqlite-py situation — that one fails because a pyo3 extension's harness needs
  # libpython at link time, which is a property of pyo3, not of cdylib.
  if env CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
      cargo test --no-fail-fast -p cqlite-node \
      ${node_feature_args[@]+"${node_feature_args[@]}"} --lib > "$node_log" 2>&1; then
    if ! check_unittest_targets_ran "$name/cqlite-node" "$node_log" "${node_unit_srcs[@]}" 2>>"$node_log"; then
      status=FAIL
    fi
  else
    status=FAIL
  fi

  cat "$ffi_log" "$node_log" >> "$log" 2>/dev/null
  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# delivery-telemetry: run the delivery-pipeline telemetry tool's unit tests
# (scripts/tests/test_delivery_telemetry.py) with the stdlib unittest runner.
# SKIP-aware like python-bindings: no python3 -> SKIP (loud, never silent PASS);
# any test failure -> hard FAIL. No third-party deps, no datasets, no network.
run_delivery_telemetry() {
  local name=delivery-telemetry
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] python3 scripts/tests/test_delivery_telemetry.py"
  if python3 "$REPO_ROOT/scripts/tests/test_delivery_telemetry.py" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# oom-audit: the STREAM_RETURNS_VEC static AST audit (issue #2012) run in
# --enforce mode over the v1 scope (data_access/**, query/**, flight producers +
# streaming). SKIP-aware on the delivery-telemetry model: no cargo, an absent
# xtask crate, or a failed xtask build -> SKIP (loud, never a silent PASS); a
# successful build whose enforce run exits non-zero (an unallowlisted finding,
# orphaned/malformed/expired allowlist entry) -> hard FAIL; otherwise PASS. Not
# in DATASET_COMPONENTS: it needs no SSTable fixtures (it reads source only).
#
# Test seams (mirrors parity-report's env overrides, exercised by
# scripts/tests/test_agent_gate_oom_audit.sh via `--only oom-audit`):
#   OOM_AUDIT_XTASK_DIR      - point at an absent dir to force the SKIP path
#   CQLITE_OOM_AUDIT_ROOT    - point the audit at a synthetic tree (a planted
#                              violation for FAIL, a clean tree for PASS)
run_oom_audit() {
  local name=oom-audit
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  local xtask_dir="${OOM_AUDIT_XTASK_DIR:-$REPO_ROOT/xtask}"
  if ! command -v cargo >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no cargo on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  if [ ! -f "$xtask_dir/Cargo.toml" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (xtask crate absent at $xtask_dir)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] cargo run -p xtask -- oom-audit --enforce"
  if ! cargo build -p xtask >"$log" 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (xtask build failed; see $log)"
    record_result "$name" "$status" "$(( $(date +%s) - start ))"
    return 0
  fi
  if cargo run -q -p xtask -- oom-audit --enforce >>"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# compaction-byte-parity: the PR-VISIBLE proxy for the nightly-only Java
# differential byte tier (issue #1405). The two manifest scenarios
# cass.compaction.SSTableRewriterTest.output_component_integrity and
# cass.compaction.harness_byte_tier_artifacts prove Cassandra-vs-CQLite
# byte identity only under `gradle byteParity` (compaction-parity.yml /
# nightly-docker-parity.yml), which fires nightly + on workflow_dispatch — never
# on a PR. A PR could break compaction byte parity and merge green.
#
# This component runs the Rust re-compaction byte-parity SUBSET as the local PR
# proxy: CQLite re-produces the same inputs, runs its own compaction, and diffs
# the output components (Data.db/Index.db/Summary.db/Digest.crc32/CRC.db)
# byte-for-byte against committed Cassandra 5.0.2 compacted references. It does
# NOT replace the nightly Java tier (which diffs the FULL component set over the
# whole scenario matrix from a live Cassandra build) — see
# docs/development/parity-ci-tiers.md for the PR-proxy vs nightly tier contract.
#
# Fixture policy (fail-closed where fixtures are committed, SKIP-aware otherwise):
#   * Group A (issue_1017/1020/1240): references are COMMITTED to git under
#     test_compactionparity/** + test_compactionparityudt/**, so they run under
#     CQLITE_REQUIRE_FIXTURES=1 — an absent/present-but-incomplete committed
#     golden is a hard FAIL, never a silent skip.
#   * Group B (issue_1019): its test_tomb references are fetched-only (not
#     committed), so it runs WITHOUT CQLITE_REQUIRE_FIXTURES — it enforces the
#     byte/header diff when the fixtures are present and cleanly self-skips when
#     they are not (e.g. a checkout that has not fetched test_tomb).
# The whole component SKIPs (loud, never silent PASS) when CQLITE_DATASETS_ROOT
# is unset or the committed test_compactionparity keyspace is absent (a minimal
# checkout). NOT in DATASET_COMPONENTS: it is self-guarding, so it must not trip
# the hard dataset preflight.
run_compaction_byte_parity() {
  local name=compaction-byte-parity
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  local committed_ks="${CQLITE_DATASETS_ROOT:-}/sstables/test_compactionparity"
  if [ -z "${CQLITE_DATASETS_ROOT:-}" ] || [ ! -d "$committed_ks" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (CQLITE_DATASETS_ROOT unset or committed test_compactionparity fixtures absent)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] Rust byte-parity PR proxy for the nightly Java byte tier (#1405)"
  if CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" bash -c '
      set -euo pipefail
      # Group A — committed references, fail-closed (CQLITE_REQUIRE_FIXTURES=1).
      env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT="'"$CQLITE_DATASETS_ROOT"'" \
        cargo test -p cqlite-core --features write-support \
          --test issue_1017_live_cell_compaction_byte_parity \
          --test issue_1020_udt_frozen_compaction_byte_parity \
          --test issue_1240_nested_frozen_collection_udt_parity
      # Group B — fetched-only test_tomb references, skip-aware (no require-fixtures).
      env CQLITE_DATASETS_ROOT="'"$CQLITE_DATASETS_ROOT"'" \
        cargo test -p cqlite-core --features write-support \
          --test issue_1019_static_dropped_collection_compaction_parity' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# bti-multiclustering: the compound-clustering BTI (`da`) lane (issue #3032, extended
# by #3220). The targets read the real Cassandra 5.0 `test_da/multiclustering_table`
# fixture — a 3-component PRIMARY KEY ((pk), bucket, seq) whose Rows.db trie is the
# only in-corpus oracle for the OSS50 byte-comparable clustering encoding and for the
# `ClusteringPrefix.Kind` bound markers. Coverage:
#   * issue_3032_multiclustering_rows_trie_shape          (trie/byte shape)
#   * issue_3032_multiclustering_clustering_slice_select  (SELECT vs JSONL golden)
#   * point_vs_full_differential                          (#3220: the AC6 point-vs-full
#     differential case over the SAME fixture — the only lane comparing the two READ
#     PATHS on a multi-component clustering key)
#
# Why a DEDICATED component (issue #3032 roborev B1): the targets self-skip when the
# fixture is missing, and core-tests runs them WITHOUT CQLITE_REQUIRE_FIXTURES — so a
# deleted/renamed fixture would turn them into silent green no-ops. This lane pins the
# two #3032 targets to CQLITE_REQUIRE_FIXTURES=1, under which every absence path in
# both files asserts instead of skipping.
#
# point_vs_full_differential is run in a SECOND invocation, deliberately WITHOUT
# CQLITE_REQUIRE_FIXTURES — enforced with `env -u`, not merely by omitting it: plain
# `env` INHERITS an exported value, and exporting CQLITE_REQUIRE_FIXTURES=1 is routine
# after a manual fail-closed run. Inherited, the target's `skipped.is_empty()` branch
# fires on any box lacking the fetched test_tomb corpus and this component FAILs for a
# reason unrelated to the fixture it exists to guard (#3220 review B3/R1).
# Why it must not be pinned: most of its corpus (test_tomb/**) is FETCHED and gitignored,
# so pinning that variable here would make this component depend on the fetched corpus
# and break its "no fetched corpus at all" contract below. Its fail-closure instead
# comes from the target itself — `TableCase::must_run`, asserted UNCONDITIONALLY, so
# every COMMITTED-fixture case (both test_da cases + the two committed
# test_compaction_tombstone_ttl ones) must have run while the fetched-only cases still
# SKIP cleanly on a minimal checkout (#3220).
#
# Ordering note (#3220): this is only meaningful because that target now resolves its
# root TABLE-granularly (cqlite-core/tests/support/datasets_root.rs) and therefore
# falls back to the in-repo committed fixture when CQLITE_DATASETS_ROOT names a corpus
# that lacks it. Under the previous keyspace-granular resolution the case skipped.
#
# Third invocation — scripts/tests/test_point_vs_full_failclosed.sh (#3220 AC2), the
# POSITIVE CONTROL for everything above: a green lane proves nothing unless the same
# lane FAILs on a fixture that is absent from every candidate root AND on one that is
# present-but-empty. Both are staged in temp dirs (the tracked fixture and
# $CQLITE_DATASETS_ROOT are never mutated — asserted by the self-test) and the absent
# staging is surgical, hiding exactly one fixture, so the assertion cannot be satisfied
# by some other case failing. It runs HERE rather than in tooling-tests because it
# drives the very test binary this component has just built.
#
# Fixture policy: FAIL-CLOSED, unconditionally. Unlike the fetched-corpus lanes,
# these fixtures are COMMITTED to git (test-data/datasets/sstables/test_da/
# multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf/**, 9 components incl. the
# sstabledump JSONL golden), so they are present in EVERY checkout and there is no
# legitimate SKIP: the tests fall back to the in-repo corpus when
# CQLITE_DATASETS_ROOT is unset. Absent fixture => FAIL, never SKIP. NOT in
# DATASET_COMPONENTS: it needs no fetched corpus at all.
run_bti_multiclustering() {
  local name=bti-multiclustering
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  # An ARRAY, not `${VAR:+NAME="$VAR"}`: the unquoted parameter-expansion form
  # word-splits a root containing whitespace, and `env` would then run its second
  # word as the command.
  local -a ds_env=()
  [ -n "${CQLITE_DATASETS_ROOT:-}" ] && ds_env=(CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT")
  echo ">>> [$name] compound-clustering BTI trie shape + SELECT + point-vs-full lanes, fail-closed (#3032/#3220)"
  if env CQLITE_REQUIRE_FIXTURES=1 "${ds_env[@]}" \
      cargo test -p cqlite-core --features "state_machine cli-helpers" \
        --test issue_3032_multiclustering_rows_trie_shape \
        --test issue_3032_multiclustering_clustering_slice_select >"$log" 2>&1 \
    && env -u CQLITE_REQUIRE_FIXTURES "${ds_env[@]}" \
      cargo test -p cqlite-core --features "state_machine cli-helpers" \
        --test point_vs_full_differential >>"$log" 2>&1 \
    && bash "$REPO_ROOT/scripts/tests/test_point_vs_full_failclosed.sh" >>"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# query-semantics-oracle: the QUERY-SEMANTICS parity lane (issue #1742), DISTINCT
# from the physical sstabledump JSONL goldens. The physical goldens enumerate every
# on-disk cell (tombstones, deleted rows, expired-but-uncompacted TTL cells), so a
# row-count/value comparison against them structurally CANNOT catch a read-time-
# reconciliation regression: when the reader fails to reconcile, both sides still
# contain the shadowed/expired rows and parity passes while a real Cassandra SELECT
# diverges (the #1741 read-path P0). This lane instead compares CQLite SELECT output
# to the POST-RECONCILIATION result set recorded in test-data/query-semantics-oracle.json,
# evaluating TTL at a PINNED `now` per case (deterministic, never wall-clock-flaky).
#
# Fixture policy: the three real Cassandra 5.0.2 fixtures it reads
# (test_compaction_tombstone_ttl/{ttl_expired_live,shadow_row_delete,rt_cross_gen})
# are COMMITTED to git, so the lane runs fail-closed (CQLITE_REQUIRE_FIXTURES=1): an
# absent/present-but-0-row fixture is a hard FAIL, never a silent skip. The whole
# component SKIPs (loud, never silent PASS) only when CQLITE_DATASETS_ROOT is unset or
# the committed keyspace is absent (a minimal checkout). NOT in DATASET_COMPONENTS: it
# is self-guarding, so it must not trip the hard dataset preflight.
run_query_semantics_oracle() {
  local name=query-semantics-oracle
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  local committed_ks="${CQLITE_DATASETS_ROOT:-}/sstables/test_compaction_tombstone_ttl"
  if [ -z "${CQLITE_DATASETS_ROOT:-}" ] || [ ! -d "$committed_ks" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (CQLITE_DATASETS_ROOT unset or committed test_compaction_tombstone_ttl fixtures absent)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] query-semantics parity oracle vs Cassandra SELECT (#1742)"
  if env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
      cargo test -p cqlite-core --features "state_machine cli-helpers" \
        --test query_semantics_oracle_parity >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# flight-query-semantics-oracle: the QUERY-SEMANTICS parity lane routed through the
# FLIGHT do_get path (issue #2374), the sibling of query-semantics-oracle above. The
# in-core lane never exercises the Flight producer/merge path (it drives
# cqlite_core::Database directly), so a read-time reconciliation regression in the
# Flight producer would pass the in-core oracle. This lane replays the SAME oracle
# cases through a REAL in-process do_get at a PINNED `now` and asserts the post-
# reconciliation result set matches — including read-time TTL expiry (issue #2789).
# Same fixture policy + SKIP/fail-closed contract as its sibling: fail-closed
# (CQLITE_REQUIRE_FIXTURES=1) when the committed test_compaction_tombstone_ttl
# fixtures are present, loud SKIP (never silent PASS) when CQLITE_DATASETS_ROOT is
# unset or the committed keyspace is absent. NOT in DATASET_COMPONENTS: self-guarding.
run_flight_query_semantics_oracle() {
  local name=flight-query-semantics-oracle
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  # Each lane has its OWN fixture precondition (issue #3095): the oracle lane needs
  # the committed `test_compaction_tombstone_ttl` keyspace, the STATIC lane needs the
  # committed `test_deltas` + `test_tomb` keyspaces. Sharing one SKIP predicate would
  # let the static lane silently never run whenever the UNRELATED oracle fixtures are
  # absent — which is exactly the "can never green-pass by skipping" claim it must
  # actually satisfy. Selected here, per lane, and reported explicitly.
  local -a targets=() skipped=()
  if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT}/sstables/test_compaction_tombstone_ttl" ]; then
    targets+=(--test query_semantics_flight_parity)
  else
    skipped+=("query_semantics_flight_parity (committed test_compaction_tombstone_ttl absent)")
  fi
  if [ -n "${CQLITE_DATASETS_ROOT:-}" ] \
      && [ -d "${CQLITE_DATASETS_ROOT}/sstables/test_deltas" ] \
      && [ -d "${CQLITE_DATASETS_ROOT}/sstables/test_tomb" ]; then
    # The static lane itself asserts, unconditionally, that BOTH committed Cassandra
    # fixtures ran — so once it is selected it cannot green-pass by skipping.
    targets+=(--test issue_3095_flight_static_columns)
  else
    skipped+=("issue_3095_flight_static_columns (committed test_deltas/test_tomb absent)")
  fi
  local lane
  for lane in ${skipped[@]+"${skipped[@]}"}; do
    echo ">>> [$name] lane SKIPPED: $lane"
  done
  if [ "${#targets[@]}" -eq 0 ]; then
    status=SKIP
    echo ">>> [$name] SKIP (CQLITE_DATASETS_ROOT unset or no committed fixture keyspace present)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] query-semantics parity oracle vs Flight do_get (#2374/#2789) + STATIC-column semantics (#3095)"
  if env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
      cargo test -p cqlite-flight "${targets[@]}" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# _resolved_package_features <package> [cargo-feature-flag…]: print " a b c " — the
# features cargo ACTUALLY enables for that workspace package under a `cargo test`
# resolve, one space-delimited set (issue #1699).
#
# The oracle is CARGO ITSELF — `cargo tree -p <pkg>`, a PACKAGE-SCOPED resolve — deliberately
# rather than a hand-parse of `[features]` in Cargo.toml. A parser here would be a SECOND
# IMPLEMENTATION of cargo's feature resolver, and its correctness would only be knowable by
# differential testing against the original — and it would get this very package wrong today:
# cqlite-flight's `default` is empty, yet `test-util` is on for every test build via the
# self-referential dev-dependency (`cqlite-flight = { path = ".", features = ["test-util"] }`),
# which no reading of `default = []` can see. The resolve reports `default test-util`.
#
# Any feature flags the CALLER passes are forwarded, so the answer can never drift from the
# invocation it describes.
#
# NOT `cargo metadata`, and this header USED to say it was (roborev round-20, Low — the header
# survived the round-6 fix immediately below it and then contradicted its own implementation for
# fourteen rounds, including a "known imprecision" paragraph describing a WORKSPACE-wide resolve
# this function no longer performs and an over-broad enabled set it no longer returns). A stale
# doc block directly above a corrected implementation is worse than no comment: it is read as the
# contract, and here it described the exact defect the code was changed to remove. The measured
# reason for the change is in the body comment below.
#
# THE PRESENCE ORACLE IS THE PACKAGE LINE, NOT A NON-EMPTY FEATURE LIST (issue #3522, and this
# header used to say the opposite). It read: "Emptiness is impossible for a real package
# (`default` is always in the set), so an empty result is a failed derivation." That is FALSE,
# and it was falsified by measurement on the first package that has no `[features]` table at
# all: `cargo tree -p cqlite-ffi-common … -f '{p}|{f}'` prints
# `cqlite-ffi-common v0.16.1 (…)|` — the package line is THERE, the feature field is EMPTY,
# because cargo's `{f}` prints the ENABLED features and an implicit `default = []` enables
# none. Under the old rule that healthy resolve returned FAILURE, so any lane covering such a
# package would have failed closed forever on a correct tree — the false-red shape that teaches
# agents to waive a lane.
#
# The fix keeps the fail-closed DIRECTION and makes the signal PRECISE: the derivation has
# failed IFF cargo emitted no line for the package (a cargo failure, an offline registry, a
# renamed package). A package that IS in the resolve with no features enabled returns the EMPTY
# SET with success — which is a measurement, not an absence of one. Callers whose packages do
# have features are unaffected (cqlite-flight resolves to `default test-util`, cqlite-core to
# nine), and their failure branch now fires on the condition it always meant to name.
_resolved_package_features() {
  # PACKAGE-SCOPED resolve, via `cargo tree -p` — NOT `cargo metadata` (roborev round-6
  # finding, Medium). `cargo metadata` resolves the ENTIRE workspace and unions features
  # across every member, so it reported cqlite-core as having `arrow`,
  # `arrow-shape-corpus`, `cli-helpers`, `parquet` and `producer-fault-injection` enabled.
  # MEASURED: 14 features workspace-wide vs 9 package-scoped, and the five extras are
  # turned on by cqlite-flight / cqlite-py / cqlite-node / ws0-corpus-gen — OTHER
  # members. None is a dev-dependency of cqlite-core (checked per-dependency, including
  # `kind=dev`), so `cargo test -p cqlite-core --features …` does NOT enable them.
  #
  # THE DIRECTION IS WHY THIS IS NOT COSMETIC. The only consumer is the co-required-feature
  # census, which reports a GAP: "this body needs feature X, which is not enabled here".
  # An OVER-BROAD enabled set makes a real gap look reachable and DROPS it from the census
  # — a silent UNDER-report, the permissive direction, in the one output whose entire job
  # is to state omissions. Today nothing is lost (`experimental` is absent from both sets,
  # so the current census is correct either way), but a future test gated on
  # `all(legacy-heuristics, parquet)` would have compiled out of the lane and been
  # announced as covered.
  #
  # An earlier version of this comment claimed the breadth was dev-dependency unification
  # "verified not to be over-broad" via `cargo metadata --manifest-path
  # cqlite-core/Cargo.toml`. That was NOT a control: for a workspace MEMBER, cargo finds
  # the workspace root and resolves the whole workspace anyway, so it necessarily agreed.
  # Recorded because the wrong lesson is "the numbers matched"; the right one is that a
  # control which cannot fail is not a control.
  #
  # Dev edges are requested explicitly (`-e features,normal,build,dev`) so genuine
  # dev-dependency unification — which `cargo test` DOES apply — is still counted; that
  # was measured to make no difference here, but omitting it would bias the other way.
  # A failed resolve returns non-zero and the caller FAILs the lane naming the census, so
  # "could not measure" never becomes "nothing to report".
  local pkg="$1"; shift
  local raw feats
  # The resolve is captured ONCE and interrogated twice, so the presence check and the feature
  # extraction can never describe two different cargo runs.
  raw=$(cargo tree -p "$pkg" "$@" -e features,normal,build,dev --prefix none -f '{p}|{f}' 2>/dev/null) || return 1
  [ -n "$raw" ] || return 1
  # PRESENCE: at least one line for this package. This — not the feature count — is what
  # distinguishes "cargo could not resolve it" from "it enables nothing" (see the header). The
  # awk EXIT STATUS carries the answer, so an empty feature field cannot be read as an absent
  # package.
  printf '%s\n' "$raw" | awk -F'|' -v pat="^$pkg v" '$1 ~ pat { found = 1 } END { exit found ? 0 : 1 }' || return 1
  feats=$(printf '%s\n' "$raw" \
    | awk -F'|' -v pat="^$pkg v" '$1 ~ pat {print $2}' \
    | tr ',' '\n' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e '/^$/d' | sort -u)
  printf ' %s ' "$(printf '%s' "$feats" | tr '\n' ' ' | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//')"
}

# flight-tests: EXECUTE cqlite-flight's UNIT test suite locally (issue #1699).
#
# What the gate covered before this component: clippy COMPILES the crate
# (--all-targets), flight-query-semantics-oracle RUNS two named integration targets
# (query_semantics_flight_parity, issue_3095_flight_static_columns) and memory-budget
# RUNS one dhat target. Everything else in the crate — ~38 integration targets plus
# --lib and --bins — was compiled and never run, so a Flight regression outside those
# three targets was discovered only AFTER a push, on CI's Flight tier. A local-first
# gate has to catch it before the push; that is the gap this closes.
#
# SCOPE — `--lib --bins` ONLY, and the omission is DECLARED ON EVERY RUN (issue #3384).
# Two earlier cuts of this lane executed cqlite-flight's integration targets: first
# `cargo test -p cqlite-flight` (the whole package), then an explicit DERIVED `--test`
# list minus a curated flake quarantine (#3383). Both were withdrawn on measurement:
# the integration half of this package is ~50% NON-DETERMINISTIC under intra-package
# parallelism. Four consecutive whole-package runs went PASS / FAIL / PASS / FAIL with
# TWO DIFFERENT victims (issue_3058_bypass_path_taken's
# `fast_arm_stream_stops_when_the_client_drops_it`, and issue_2370_gauge_readback_test),
# and four hypotheses were ruled out by measurement rather than argued away: whole-box
# load (3/3 PASS standalone at load 74), `nice` (2/2), `--test-threads=2` (2/2), and
# concurrent MAIN-lane compilation (the failures reproduced under `--only`, where MAIN
# runs nothing). A merge-gate lane that reds ~1-in-2 carries no information: it trains
# agents to re-run and to waive, which is worse than not having the lane.
#
# Quarantining the victims one at a time was CONSIDERED AND REJECTED (owner ruling).
# Two victims in four runs is not a converging series, so a per-victim quarantine has
# no visible end, and it would turn the quarantine into the dumping ground its own
# design rule forbids. The general suite-hygiene defect is #3384; #3383 is its first
# individual victim. So the quarantine plumbing (FLIGHT_FLAKE_SKIPS and its validator)
# is RETIRED rather than left inert — it existed only to paper over #3384, and papering
# over #3384 is the approach that was rejected.
#
# THE IMPORTANT HALF: the lane STATES ITS OWN GAP. #1699 exists because a lane that
# silently omits coverage looks identical to a lane that covers it, so a NARROWED lane
# that stayed quiet about the narrowing would reintroduce exactly this issue's defect
# one level down. The census below is therefore DERIVED (from cargo metadata, via
# _package_test_targets — never a hard-coded number that could drift into a false
# claim) and printed BOTH to stdout as `>>>` lines AND into the component log on every
# run, naming: how many integration targets cqlite-flight declares, that THIS LANE DOES
# NOT EXECUTE THEM, which lane does (CI's Flight tier, `.github/workflows/flight-ci.yml`
# line 229 `cargo test --package cqlite-flight`, mandated on `cqlite-flight/**` AND
# `cqlite-core/**`, with `required` failing closed on it per #2910), and the issues that
# own the gap (#3384 general, #3383 first victim). A reviewer reading the log cannot
# miss it, which is the whole point.
#
# What DOES run: 386 unit tests via `--lib` plus main.rs's 2 via `--bins`, observed
# deterministic in every run of this session. `--bins` is on the command line because an
# explicit selector SUPPRESSES every target kind not named: dropping it would silently
# stop executing 2 tests that `-p` and the derived-list cut both ran, i.e. it would
# create in miniature the never-executed hole this lane exists to close. There are no
# Rust doctests in the crate to lose (measured: all 10 doc fences are ```text/```json).
#
# flight-query-semantics-oracle is LEFT ALONE (design D4) and now carries the ONLY
# local execution of any cqlite-flight integration target (two of them), alongside
# memory-budget's one dhat target. That is stated here so the census's "not executed
# locally by this lane" is not misread as "not executed locally at all".
#
# No opt-out env var: cqlite-flight is a committed workspace member and is never
# legitimately absent. Fixture-dependent sub-targets may still SKIP through the
# existing dataset machinery, which reports the skip.
#
# THE ZERO-TESTS GUARD, in the form that has a SUBJECT here. `check_no_unexpected_zero_tests`
# keys on cargo's `Running tests/<name>.rs` lines and explicitly disclaims `--lib`
# ("Running unittests src/lib.rs"), so calling it on a `--lib --bins` selection would be
# a guard with an EMPTY SUBJECT SET reporting OK — the vacuous-pass shape, and the one
# thing this lane may never become. Its `--lib` analogue `check_unittest_targets_ran`
# is called instead: it requires each SELECTED unittest target to be observed AND to
# have executed a non-zero number of tests, so a cfg change that compiles the unit
# suite out FAILs the lane instead of greening it.
#
# The DERIVATION machinery is deliberately RETAINED (not deleted) so that widening this
# lane back once #3384 is fixed is a small change: _package_test_targets feeds the
# census today, and check_declared_test_targets_observed — currently UNCALLED — is the
# reconciliation the widened lane will call again. See their own comments.
#
# Deliberately NOT done: adding `--features observability-testing`. Building the OTel
# stack is a cost the gate declines on purpose (#1844 excludes that stack from clippy
# for the same reason), and reversing that is not this component's call.
run_flight_tests() {
  local name=flight-tests
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)

  # The feature flags THIS component passes to cargo — declared ONCE and consumed by
  # both the test run and the enabled-set derivation, so the two can never disagree.
  # Empty today (cqlite-flight's `default = []`).
  local -a feature_args=()

  # LANE-SPECIFIC FIXTURE PREFLIGHT (roborev round-30, Medium). Enrolling this lane in
  # DATASET_COMPONENTS is NOT sufficient: the generic full-gate preflight only requires the
  # canonical `test_basic` corpus, while the unit suite this lane executes contains a real-fixture
  # test (`cqlite-flight/src/stats.rs`, gather_table_stats over test_timeseries/sensor_data) that
  # RETURNS EARLY — silently, three separate ways — when the fixture or its Statistics.db is absent,
  # EVEN WITH CQLITE_DATASETS_ROOT set. So a partial corpus produced a green lane that had not
  # exercised the coverage it advertises, which is #3220's rule ("never let a dataset-dependent test
  # pass on an empty dataset") and this issue's own thesis in one.
  #
  # Checked HERE rather than by patching another package's test: the silent skip is pre-existing
  # behaviour in cqlite-flight and is filed separately, on the #3420/#3380 precedent that a defect
  # this lane REVEALS is fixed in its own PR. What this lane owes is not to report a green over it.
  #
  # FULL gate only. `--only` and `--lite` stay lenient by design (they are probes, and `--only`
  # cannot be a verdict — it exits 3 on success), which is the same split the #2078 fixture contract
  # uses; an opt-out is deliberately visible rather than silent.
  # "full" spelled the way this script spells it (`-z "$ONLY"` and `LITE -eq 0`), matching the
  # #2078 preflight's own test rather than inventing a MODE variable that does not exist here.
  # HONOURS THE DOCUMENTED OPT-OUT (roborev round-31, Medium). Without this the #2078 escape hatch
  # became INEFFECTIVE: `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` got past the generic preflight and then
  # this lane failed the run anyway, so an opt-out that the SUMMARY reports as taken did not, in
  # fact, let the gate finish. A per-lane check that ignores the global opt-out is a second,
  # undocumented policy — and the opt-out's whole value is that it is VISIBLE in the block
  # (`missing-fixtures: OPT-OUT`), which a silent per-lane veto destroys.
  if [ -z "$ONLY" ] && [ "$LITE" -eq 0 ] && [ -n "${CQLITE_DATASETS_ROOT:-}" ] \
     && [ "${AGENT_GATE_ALLOW_MISSING_FIXTURES:-0}" != 1 ]; then
    # EVERY prefix-matching entry must be usable, not just one (roborev round-32, Medium). The Rust
    # test picks the FIRST entry whose name starts with `sensor_data-` in UNSPECIFIED `read_dir`
    # order and commits to it — so "some matching directory is complete" does not imply the test
    # will find a complete one. A second, incomplete `sensor_data-*` directory (or a prefix-matching
    # REGULAR FILE, which `read_dir` also yields and which makes the test's own `read_dir(&dir)`
    # fail into its early return) is enough to make the test skip while this lane reports PASS.
    #
    # Requiring every match to be a directory carrying a `-Statistics.db` makes the guarantee
    # ORDER-INDEPENDENT, which is the only way to align a shell preflight with a consumer that
    # chooses arbitrarily. The alternative roborev offered — replicating the test's exact selection
    # here — would be a second implementation of the test's choice, and it would go stale the moment
    # the test changed how it selects.
    # The corpus subdirectory must EXIST AND BE READABLE before "nothing matches" is a meaningful
    # statement (self-review). `find … 2>/dev/null` swallows a missing or permission-denied
    # directory and yields zero entries, which is fail-CLOSED (good) but reports the WRONG CAUSE —
    # "no sensor_data-* here" instead of "this path is not readable". Naming the wrong cause is the
    # defect round 20 opened on this very census, one layer down: the verdict is right and the
    # remedy it points at is useless.
    local _fx_base="$CQLITE_DATASETS_ROOT/sstables/test_timeseries"
    local _fx_entry _fx_seen=0 _fx_bad="" _fx_basefail="" _fx_st_out _fx_st_rc _fx_list _fx_enum_rc
    if [ ! -d "$_fx_base" ]; then
      _fx_basefail="not-a-directory"
    elif [ ! -r "$_fx_base" ] || [ ! -x "$_fx_base" ]; then
      _fx_basefail="unreadable"
    fi
    # ENUMERATED BY `find`, NOT BY A GLOB, for the same reason the Statistics.db check is (round 34)
    # and one round earlier than it would otherwise have been found: a glob's meaning depends on
    # ambient shell options this script never sets and cannot control — `nullglob` empties an
    # unmatched pattern, `failglob` makes it an error — and both were reachable through BASHOPTS.
    # `find -maxdepth 1 -name` also matches the CONSUMER's semantics exactly: Rust's `read_dir`
    # yields NAMES, including dangling symlinks, which is the invariant round 33 established (the
    # preflight must judge exactly the set the consumer enumerates). `-print0` because a corpus path
    # may contain spaces.
    # mktemp, NOT "$LOG_DIR/..." — the behavioural harness extracts this block and runs it with
    # no LOG_DIR, and depending on an ambient variable to enumerate the subject set fails open in
    # precisely the way this status capture was added to close (caught by the r32 cases).
    _fx_list=$(mktemp "${TMPDIR:-/tmp}/agent-gate-fixtures.XXXXXX") || _fx_list=""
    if [ -z "$_fx_list" ]; then
      _fx_enum_rc=99
    else
      find -H "$_fx_base" -maxdepth 1 -name 'sensor_data-*' -print0 > "$_fx_list" 2>/dev/null
      _fx_enum_rc=$?
    fi
    # Only when no PRECISE cause is already recorded (roborev job 117, Low — a regression from the
    # previous round). `_fx_base` missing or unreadable is already diagnosed above, and find then
    # fails for that same reason; overwriting `not-a-directory` with a generic enumeration failure
    # sends the reader to the wrong remedy, which is the exact thing the per-cause split exists for.
    if [ "$_fx_enum_rc" -ne 0 ] && [ -z "$_fx_basefail" ]; then
      # FAIL-CLOSED on an unenumerable corpus: the alternative is a check over an unknown subset.
      _fx_basefail="fixture enumeration FAILED (find exit $_fx_enum_rc under $_fx_base) — the"
      _fx_basefail="$_fx_basefail per-fixture checks below would have run over an unknown subset"
    fi
    while IFS= read -r -d '' _fx_entry; do
      _fx_seen=$((_fx_seen + 1))
      if [ -L "$_fx_entry" ] && [ ! -e "$_fx_entry" ]; then
        _fx_bad="$_fx_bad $(basename "$_fx_entry")(dangling-symlink)"
      elif [ ! -d "$_fx_entry" ]; then
        _fx_bad="$_fx_bad $(basename "$_fx_entry")(not-a-directory)"
      elif [ ! -r "$_fx_entry" ] || [ ! -x "$_fx_entry" ]; then
        # Distinguished from "no Statistics.db" for the same reason as the base directory above: a
        # permission problem and a missing fixture need different remedies, and the check that
        # cannot tell them apart sends the reader to the wrong one.
        _fx_bad="$_fx_bad $(basename "$_fx_entry")(unreadable-directory)"
      elif _fx_st_out=$(find -H "$_fx_entry" -maxdepth 1 -name '*-Statistics.db' -print -quit 2>/dev/null); _fx_st_rc=$?; [ "$_fx_st_rc" -ne 0 ]; then
        # A FAILED SCAN IS NOT AN ABSENT FIXTURE (roborev job 114, Medium). `[ -z "$(find ...)" ]`
        # collapsed both onto one branch: a find that died partway produced empty output and was
        # reported `no-Statistics.db`, sending the reader to fetch a corpus they already have. The
        # status is now captured and reported as its own cause. Same three-valued discipline as the
        # grep sites: 0 = answered, non-zero = could not answer, and "could not answer" is never
        # folded into an answer.
        _fx_bad="$_fx_bad $(basename "$_fx_entry")(statistics-scan-failed:find-exit-$_fx_st_rc)"
      elif [ -z "$_fx_st_out" ]; then
        # `-H` so a VALID symlink to a fixture directory is followed (roborev round-35, Medium).
        # `find` defaults to `-P` and does not follow its starting point, so a `sensor_data-*`
        # symlink pointing at a real fixture dir yielded nothing and was reported
        # `no-Statistics.db` — a FALSE RED that would fail the full gate on a legitimate corpus
        # layout, which is the direction that teaches people to waive a check. Rust's `read_dir`
        # + the test's own `read_dir(&dir)` follow it, so the preflight must too. `-H` follows
        # ONLY the command-line argument, which is exactly the entry under judgement.
        #
        # `find`, not `ls <glob>` (roborev round-34, Medium). With `nullglob` inherited through
        # BASHOPTS an unmatched pattern EXPANDS TO NOTHING, so `ls` runs with no arguments, lists
        # the CWD and SUCCEEDS — a directory with no Statistics.db would have passed preflight while
        # the Rust test skipped. The failure depended on an ambient shell option this script never
        # sets and cannot control, which is the worst kind: correct on the author's box, wrong on
        # someone else's. `find` takes the pattern as an ARGUMENT, so no glob expansion is involved
        # at all and the check means the same thing under every shell option.
        _fx_bad="$_fx_bad $(basename "$_fx_entry")(no-Statistics.db)"
      fi
    done < "$_fx_list"
    [ -n "$_fx_list" ] && rm -f "$_fx_list"
    # ENUMERATED VIA A FILE, NOT A PROCESS SUBSTITUTION (roborev job 114, Medium). `done < <(find
    # ...)` DISCARDS find's exit status, so a partial enumeration — a permission error midway, a
    # vanished directory — yielded fewer entries and the "every match must qualify" check passed
    # over the survivors. That is the empty/partial-subject-set shape this component set exists to
    # remove, in the component set itself: fewer subjects cannot fail a per-subject check.
    # A file makes the status observable; it is read AFTER the loop, below.
    # `-H` on the OUTER enumeration as well (roborev round-38, Medium). Round 35 added it to the
    # per-entry find and left this one at the default `-P`, so a corpus whose `test_timeseries`
    # BASE directory is itself a symlink enumerated NOTHING — the preflight then failed the whole
    # gate on a legitimate layout, while the Rust test's `read_dir` follows it happily. Fixed one
    # site and missed its sibling: the same recurrence as rounds 11-13 and 37, fifth instance.
    if [ -n "$_fx_basefail" ] || [ "$_fx_seen" -eq 0 ] || [ -n "$_fx_bad" ]; then
      status=FAIL
      {
        echo "[$name] FAIL-CLOSED: the real-fixture stats test in this unit suite needs"
        echo "        test_timeseries/sensor_data-*/ WITH a -Statistics.db under"
        echo "        CQLITE_DATASETS_ROOT ($CQLITE_DATASETS_ROOT)."
        if [ -n "$_fx_basefail" ]; then
          echo "        The corpus subdirectory itself is $_fx_basefail:"
          echo "          $_fx_base"
          echo "        (so 'nothing matches sensor_data-*' would name the wrong cause)."
        elif [ "$_fx_seen" -eq 0 ]; then
          echo "        NOTHING matches sensor_data-* there."
        else
          echo "        $_fx_seen entry/entries match, and these are unusable:$_fx_bad"
          echo "        EVERY match must qualify: the test takes the FIRST read_dir match in"
          echo "        unspecified order, so one bad entry is enough to make it skip."
        fi
        echo "        That test returns early and PASSES when the fixture is missing, so this lane"
        echo "        would report a green having skipped the coverage it advertises (#3220)."
        echo "        Remedy: bash test-data/scripts/fetch-datasets.sh, then export the"
        echo "        CQLITE_DATASETS_ROOT line it prints."
      } | tee "$log"
      end=$(date +%s)
      record_result "$name" "$status" "$((end - start))"
      echo ">>> [$name] $status ($((end - start))s)"
      return 0
    fi
    echo ">>> [$name] fixture preflight: test_timeseries/sensor_data + Statistics.db present"
  elif [ "${AGENT_GATE_ALLOW_MISSING_FIXTURES:-0}" = 1 ] && [ -z "$ONLY" ] && [ "$LITE" -eq 0 ]; then
    echo ">>> [$name] fixture preflight: SKIPPED (AGENT_GATE_ALLOW_MISSING_FIXTURES=1) — the real-fixture stats test may return early, so this lane does NOT validate the wide-table stats path in this run (#3425)"
  fi

  # The enabled set. A failed derivation is a FAIL naming the derivation, never a
  # fallback to "nothing enabled" — that would be a verdict with no measurement behind
  # it, which is the vacuous-green shape this lane exists to prevent.
  local enabled
  if ! enabled=$(_resolved_package_features cqlite-flight ${feature_args[@]+"${feature_args[@]}"}); then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not derive cqlite-flight's enabled feature set"
      echo "        via 'cargo tree -p cqlite-flight' (a cargo failure, an offline registry,"
      echo "        or no line for the package). The DERIVATION failed, not the tests."
      echo "        NOTE: this oracle is package-scoped 'cargo tree', NOT 'cargo metadata' —"
      echo "        metadata resolves the WHOLE workspace and reports other members'"
      echo "        features as this package's (issue #1699, roborev round-6)."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # The DECLARED integration-target set, from cargo metadata. This lane does not RUN
  # these targets (see the scope note above); it COUNTS them, so the census it prints
  # states the size of its own gap truthfully rather than from a hard-coded number that
  # would drift the moment a target is added or removed. Same fail-closed direction as
  # the enabled set: a failed DERIVATION is a FAIL naming the derivation, never a
  # census that quietly claims a gap of unknown size.
  local target_meta declared_n=0 rf_n=0 rf_reasons=""
  if ! target_meta=$(_package_test_targets cqlite-flight); then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not derive cqlite-flight's declared integration"
      echo "        (test) targets from cargo metadata (no jq/python3, a metadata"
      echo "        failure, or an empty target set). The DERIVATION failed, not the"
      echo "        tests. Without it this lane cannot state the size of the coverage"
      echo "        gap it deliberately carries (#3384), and an UNDECLARED gap is the"
      echo "        silent omission issue #1699 exists to eliminate."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # Count the declared targets, and separately those cargo could not have run here
  # anyway (unmet `required-features`) — reported as a sub-count so the census does not
  # overstate what a widened lane would gain.
  local tname rf rfl off
  while IFS=$'\t' read -r tname rf; do
    [ -n "$tname" ] || continue
    declared_n=$((declared_n + 1))
    off=""
    for rfl in ${rf//,/ }; do
      case "$enabled" in *" $rfl "*) ;; *) off="$rfl"; break ;; esac
    done
    if [ -n "$off" ]; then
      rf_n=$((rf_n + 1))
      rf_reasons="$rf_reasons $tname(required-features[$rf]:off[$off])"
    fi
  done <<< "$target_meta"

  # A census over ZERO declared targets is a FAILED DERIVATION, not a lane with no gap:
  # _package_test_targets already fails closed on an empty result, so reaching here with
  # 0 would mean the count itself broke, and a "0 integration targets un-run" line would
  # be a false all-clear.
  if [ "$declared_n" -eq 0 ]; then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: counted 0 declared integration targets for"
      echo "        cqlite-flight. The COUNT failed, not the tests — this package"
      echo "        declares ~42, and a census claiming an empty gap would be a false"
      echo "        all-clear about the omission this lane must declare (#3384)."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ---- THE CENSUS -------------------------------------------------------------
  # Built ONCE and emitted TWICE — as `>>>` lines on the gate's stdout and as the head
  # of the component log — because "the log a reviewer actually reads" is both of those
  # and a gap stated in only one of them is a gap someone will miss. Not a comment: a
  # comment is not read on a run.
  local -a census=()
  census+=("cargo test --no-fail-fast -p cqlite-flight --lib --bins (UNIT tests only, #1699/#3384)")
  census+=("COVERAGE CENSUS — WHAT THIS LANE DOES NOT RUN:")
  census+=("  cqlite-flight declares $declared_n integration (test) targets. THIS LANE EXECUTES NONE OF THEM.")
  census+=("  ($rf_n of the $declared_n could not run here in any case: unmet required-features.)")
  census+=("  WHY: the integration half of this package is ~50% NON-DETERMINISTIC under")
  census+=("       intra-package parallelism — 4 whole-package runs went PASS/FAIL/PASS/FAIL")
  census+=("       with 2 different victims (issue_3058_bypass_path_taken,")
  census+=("       issue_2370_gauge_readback_test). Ruled out by measurement: box load,")
  census+=("       nice, --test-threads=2, concurrent MAIN-lane compilation.")
  census+=("       Issues: #3384 (the general suite-hygiene defect), #3383 (first victim).")
  # THE OBSERVATION, NOT A TAXONOMY (roborev rounds 20-27; the rationale is on
  # _crate_gated_test_targets). A crate-level `#![cfg(feature = "X")]` with X off means the target
  # COMPILES, runs ZERO tests and exits 0 — so naming a runner for it is the census's most
  # consequential claim, and it is the one that kept being wrong, five rounds running. This lane now
  # reports what it can actually SEE: which targets carry a crate-level gate, the gate text
  # verbatim, and its own enabled feature set. Comparing them is left to the reader, who can do it
  # correctly, and #3375 remains the record of which targets execute nowhere fleet-wide.
  local gated_meta gated_n=0 gated_lines="" grel ggate gname
  if ! gated_meta=$(_crate_gated_test_targets cqlite-flight); then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not derive which cqlite-flight targets carry a CRATE-LEVEL"
      echo "        gate (cargo metadata, the metadata parser, or an unreadable target source)."
      echo "        The DERIVATION failed, not the tests. Reporting no gated targets would be a"
      echo "        false all-clear about the omission this lane exists to declare."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi
  while IFS=$'\t' read -r gname grel ggate; do
    [ -n "$grel" ] || continue
    gated_n=$((gated_n + 1))
    gated_lines="$gated_lines $grel[$ggate]"
  done <<< "$gated_meta"
  census+=("  OF THOSE, $gated_n contain an INNER cfg attribute (#![cfg(...)] / #![cfg_attr(...)]),")
  census+=("       reported as OCCURRENCES with file:line and the attribute OPENING LINE below —")
  census+=("       not verbatim: a multiline attribute is truncated at its first line, so compare the")
  census+=("       file:line, which is authoritative. NOT a claim that each is")
  census+=("       CRATE-level: deciding that needs a Rust parser, and five review rounds showed a")
  census+=("       line scan cannot approximate it (a module-level inner attribute looks identical")
  census+=("       here). The file:line is authoritative — open it. A crate-level gate naming a feature")
  census+=("       that is off means the target COMPILES, runs ZERO tests and exits 0.")
  census+=("       CI's tier passes NO --features, so it enables whatever this package's own")
  census+=("       resolve enables — which is NOT the same as 'default' (roborev round-28, Low):")
  census+=("       cqlite-flight's 'default' is empty, yet the self-referential dev-dependency")
  census+=("       turns 'test-util' on for every test build, so a target gated solely on")
  census+=("       'test-util' DOES run there. Compare each gate below against the resolved")
  census+=("       'enabled features' line printed at the end of this census — not against")
  census+=("       'default', and not against an inference.")
  census+=("  THIS LANE DOES NOT CLASSIFY THEM, DELIBERATELY: #3375 is the record of the targets")
  census+=("       that execute NOWHERE. A classification here was wrong in five consecutive review")
  census+=("       rounds (grammar, stacked gates, conjunctions, compile-only invocations,")
  census+=("       cfg_attr), always the classification and never the observation — so the")
  census+=("       observation is what is reported. The gate text and this lane's enabled features")
  census+=("       are both printed; comparing them is the reader's call, not this lane's guess.")
  census+=("  WHO RUNS THE REST: CI's Flight tier — .github/workflows/flight-ci.yml line 229,")
  census+=("       'cargo test --package cqlite-flight', mandated on cqlite-flight/** AND")
  census+=("       cqlite-core/**, with the 'required' check failing closed on it (#2910). SCOPED:")
  census+=("       that claim does not extend to the crate-gated targets above, because CI's")
  census+=("       invocation enables no features either.")
  census+=("       Locally, flight-query-semantics-oracle runs 2 of these targets and")
  census+=("       memory-budget runs 1 (--test issue_1494_producer_mem_budget).")
  census+=("  This omission is DECLARED, not silent: widening the lane back is a small")
  census+=("       change once #3384 is fixed (the derivation machinery is retained).")
  local cl
  for cl in "${census[@]}"; do echo ">>> [$name] $cl"; done
  [ -n "$rf_reasons" ] && echo ">>> [$name] declared targets with unmet required-features:$rf_reasons"
  [ -n "$gated_lines" ] && echo ">>> [$name] targets with an inner cfg attribute (file:line + attribute OPENING LINE, truncated if multiline; crate-level-ness NOT claimed):$gated_lines"
  echo ">>> [$name] enabled features (cargo tree -p, package-scoped):$enabled"

  # The log opens WITH the census (`>` here, `>>` for cargo below), so the omission is
  # in the component log on every run whether the lane passes or fails.
  {
    echo "==== [$name] COVERAGE CENSUS (issue #1699 / #3384) ===="
    for cl in "${census[@]}"; do echo "$cl"; done
    [ -n "$rf_reasons" ] && echo "declared targets with unmet required-features:$rf_reasons"
    [ -n "$gated_lines" ] && echo "targets with an inner cfg attribute (file:line + attribute OPENING LINE, truncated if multiline; crate-level-ness NOT claimed):$gated_lines"
    echo "enabled features (cargo tree -p, package-scoped):$enabled"
    echo "==== end census ===="
  } > "$log"

  # --no-fail-fast for the same reason legacy-heuristics carries it: cargo test stops
  # after the first failing test BINARY, and a lane whose purpose is to surface
  # never-executed rot must surface ALL of it in one run rather than as a serial reveal.
  if env CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
      cargo test --no-fail-fast -p cqlite-flight ${feature_args[@]+"${feature_args[@]}"} \
      --lib --bins >>"$log" 2>&1; then
    # A green cargo exit is NOT sufficient: a unit suite whose modules are cfg-gated out
    # compiles, runs 0 tests and exits 0. The guard requires BOTH selected unittest
    # targets to be OBSERVED and to have executed a non-zero count — an affirmative
    # measurement, not the absence of a bad signal. Its verdict goes to stderr, so `2>>`
    # lands it in the component log while the `if` tests the GUARD's own exit status.
    # The guard's subject set is DERIVED from cargo metadata, never hard-coded: `--bins`
    # selects every binary, so a hard-coded pair would let a newly added one run zero
    # tests while the guard still reported OK (roborev round-7 finding).
    local -a unit_srcs=()
    local _us_line
    while IFS= read -r _us_line; do
      [ -n "$_us_line" ] && unit_srcs+=("$_us_line")
    done <<EOF
$(_package_unittest_srcs cqlite-flight lib,bin "$enabled")
EOF
    if [ "${#unit_srcs[@]}" -eq 0 ]; then
      echo "[$name] FAIL-CLOSED: could not derive cqlite-flight's lib/bin unittest targets" >>"$log"
      echo "        from cargo metadata. The DERIVATION failed, so the zero-test guard has no" >>"$log"
      echo "        subject — and a guard with no subject reports OK having measured nothing." >>"$log"
      status=FAIL
    elif check_unittest_targets_ran "$name" "$log" "${unit_srcs[@]}" 2>>"$log"; then
      echo ">>> [$name] zero-test guard subject (derived): ${unit_srcs[*]}"
      status=PASS
    else
      status=FAIL
    fi
  else
    status=FAIL
  fi
  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# legacy-heuristics: BUILD cqlite-core at `default + legacy-heuristics` AND EXECUTE
# the tests that feature turns on (issue #1699).
#
# Two properties, neither of which any other component has:
#
#  1. THE FEATURE SET. run_clippy's cqlite-core arm enables legacy-heuristics
#     alongside parquet, delta-scan and ~30 more features at once, so the feature is
#     never compiled at its OWN minimal set. A warning-class defect visible only at
#     `default + legacy-heuristics` surfaces here and nowhere else, which is why this
#     half runs under RUSTFLAGS=-D warnings.
#  2. EXECUTION. That same clippy pass already test-COMPILES the gated bodies
#     (--all-targets), so a compile-only lane would add nothing. What has never
#     happened anywhere — no gate component, no CI job — is RUNNING the positively
#     gated bodies. The `#[cfg(not(feature = "legacy-heuristics"))]` polarity already
#     runs in core-tests; the `#[cfg(feature = ...)]` polarity is the subject here.
#     The distinguishing property: an INVERTED assertion in a positively-gated test
#     body FAILs this component while clippy still passes.
#
# The --test target set is DERIVED from the committed source, never hard-coded: a
# literal list drifts the moment a sixth gated test file is added, and the drift is
# INVISIBLE (the lane stays green while its subject shrinks). Derivation is
# FAIL-CLOSED — zero derived targets is a FAIL naming the derivation, never a PASS and
# never a SKIP, because a lane with no subject has no verdict to give.
#
# --lib is included because cqlite-core/src/** carries legacy-heuristics cfg sites
# whose inline #[cfg(test)] bodies are gated the same way.
#
# No opt-out env var: the committed test files are never legitimately absent.
# _legacy_coreq_sites — report the `legacy-heuristics`-gated cfg SITES that also require a
# feature this lane does not enable. It reports SITES. It deliberately does NOT classify
# what each site gates.
#
# WHY THE CLASSIFIER IS GONE (this is a DESCOPE, on a pre-commitment, not a patch).
# Earlier cuts of this function tried to say how many gated *test bodies* were omitted:
# it distinguished test fns from imports, detected test-ness from attribute paths, and
# inferred Boolean structure. That ambition produced a review finding in FOUR consecutive
# rounds — counting attributes as bodies (r5), assuming conjunction so `any(...)` read as a
# gap (r7), missing stacked attributes that Rust ANDs (r8), and classifying a gated `mod
# tests` as "support code" while ignoring crate-level `#![cfg(...)]` entirely (r10). The
# r10 reviewer's own remedy was "preferably using Rust syntax tooling", which is the tell:
# counting test BODIES requires parsing Rust, and this is a bash gate component.
#
# CLAUDE.md records the precedent and the ruling. #3229's `census-exclusion` oracle was
# DELETED by owner ruling because its defect count was RISING across review rounds and
# later rounds kept finding defects in code the previous fix rounds had introduced — with
# the durable lesson that a guard whose correctness is not establishable is worse than no
# guard. The same signal appeared here, so the PR pre-committed to descoping on the next
# classification finding rather than making that call under pressure afterwards.
#
# WHAT IS LOST, AND WHY IT DID NOT MATTER. The census's job is to tell a human "gated code
# in this file does not execute here, go look". A count of bodies was never needed for
# that, and for a gated MODULE the count is unknowable without parsing anyway (one site can
# gate twenty tests). Reporting sites is both simpler and STRICTLY MORE HONEST: the claim
# "anything gated by this site does not execute in this lane" is true for a test, an
# import, a module, or a crate root, which is exactly why it needs no classification.
#
# WHAT IS KEPT, because it is the conservative half and it is cheap: a site whose Boolean
# shape this function cannot evaluate — `not(...)`, `any(...)`, `cfg_attr` — is reported as
# UNCLASSIFIED rather than as a gap. `any(feature = "legacy-heuristics", feature = "X")` is
# REACHABLE here through the legacy arm, so calling it omitted would be a false claim; and
# `not(...)` inverts the question. Tokens are still accumulated across the whole attribute
# CLUSTER, because Rust ANDs stacked cfg attributes and a per-attribute view reports a
# false zero-gap.
#
# Inner attributes (`#![cfg(...)]`) are now matched too — cheap once nothing is being
# attributed to a following item, and they are how a whole test file gets gated.
#
# Emits one TAB-separated record per site: <site|skip> <TAB> <line no> <TAB> <missing,features>
# ============================================================================================
# STATED LIMIT OF THIS SCANNER — read this before adding a shape to it (issue #1699, #3472).
#
# THIS IS AN OCCURRENCE REPORT, NOT A PARSER, AND ITS COVERAGE IS DELIBERATELY INCOMPLETE.
# It reports the Rust attribute and module shapes it RECOGNISES. The set of shapes it does NOT
# model is OPEN — Rust attribute and module syntax is defined by rustc, not here — so no amount
# of iteration finishes it. Twelve review findings across this branch were all one family: a
# further shape the scanner did not model. Rounds 41 and 42 already made the correct structural
# move (real trivia state; descope the crate-gate scanner to an occurrence report) and the
# findings continued, which is the evidence that the surface is unbounded rather than merely
# large.
#
# WHAT THIS SCAN DOES AND DOES NOT GIVE YOU. Read this instead of relying on a guarantee — there
# deliberately is not one, and the two attempts to state one are recorded below because the second
# was falsified within a day of the first.
#
# THE INTENT: an unrecognised shape should be reported as UNCLASSIFIED rather than omitted, so that
# a miss costs NOISE (an unattributable entry a human reads) rather than BLINDNESS (a clean zero
# over gated code). Every change here must push in that direction, and where the scan can detect
# that it cannot tell, it says so at runtime — an unmodelled string-literal shape prints
# `[UNCLASSIFIED: ...]` rather than resolving.
#
# THE INTENT IS NOT ENFORCED, AND IT CANNOT BE. By construction this scan cannot enumerate the
# shapes it fails to detect: an undetected shape produces no output to mark unclassified. So the
# direction is a design aim, NOT a property you may rely on, and the following are MEASURED
# counterexamples rather than hypotheticals:
#   * a delimiter inside a string literal (`doc = "]"`) closed a cluster early     — FIXED (job 101)
#   * a `mod` form the patterns do not match (`mod r#type;`, a split declaration)  — OPEN  (job 103)
#   * a delimiter inside a line comment (`feature = "x" // ]`)                     — OPEN  (job 105)
# The two open ones resolve to ABSENT, not UNCLASSIFIED, and emit nothing at all.
#
# THEREFORE, THE ONE SENTENCE TO CARRY AWAY: a clean census is evidence that nothing was
# RECOGNISED — never evidence that nothing is THERE.
#
# WHY THIS IS STATED AS A DISCLAIMER AND NOT AS A NARROWER GUARANTEE. It was a guarantee twice.
# Unscoped, it said an unrecognised shape is always UNCLASSIFIED; job 103 falsified that. Scoped to
# ATTRIBUTE AND CLUSTER shapes, it excluded declaration recognition; job 105 falsified that too,
# with a comment-borne delimiter, which IS a cluster shape. A guarantee qualified per path acquires
# a new exception per review round, for exactly the reason the scanner does — so a third
# qualification would be the prose equivalent of an eighth pattern. An overclaimed guarantee is
# worse than an omission: an omission leaves a reader uninformed, an overclaim has them relying on
# cover that is not there, and a reader of this line does not read #3472.
#
# DO NOT FIX EITHER OPEN ITEM WITH ANOTHER PATTERN. #3472 holds the seven measured lexical contexts
# and the reasoning; the answers there are syntax-aware tooling or deleting the scanning half.
#
# WHAT NOT TO DO: do not add a thirteenth shape and call the family closed. If correctness rather
# than advice is ever required of this scan, the answer is syntax-aware tooling or deleting the
# scanning half — not another pattern. #3472 carries the family and the reasoning.
# ============================================================================================
_legacy_coreq_sites() { # _legacy_coreq_sites <file> <enabled-feature-list>
  awk -v LH="legacy-heuristics" -v ENABLED=" $2 " '
    function countch(str, ch,   n, i) {
      n = 0
      for (i = 1; i <= length(str); i++) if (substr(str, i, 1) == ch) n++
      return n
    }
    # A DELIMITER INSIDE A STRING LITERAL IS TEXT, NOT STRUCTURE (roborev job 101, Medium).
    # A stacked multiline attribute containing `")"` terminated collection early, split the
    # cluster, and dropped a later co-required feature — so the census reported ZERO gaps while
    # gated code was compiled out. That is the census under-reporting, i.e. the SILENT direction.
    # Quoted spans are removed before counting. Where the removal itself cannot be trusted — an
    # escaped quote or a raw string, both shapes this scan does not model — the CLUSTER is marked
    # UNCLASSIFIED via the existing path rather than counted on a guess. Declaring the unknown is
    # the whole reason cl_unclass exists; this is one more producer of it, not a new mechanism.
    function nostr(str) { gsub(/"[^"\\]*"/, "", str); return str }
    function litok(str) { return (str !~ /\\"/ && str !~ /r#"/ && nostr(str) !~ /"/) }
    function emit(   kind) {
      if (!(cl_has_lh && cl_miss != "")) return
      kind = cl_unclass ? "skip" : "site"
      printf "%s\t%d\t%s\n", kind, cl_line, cl_miss
    }
    function reset_cluster() {
      cl_has_lh = 0; cl_miss = ""; cl_unclass = 0; cl_line = 0
    }
    function handle_attr(a,   tmp, m) {
      if (cl_line == 0) cl_line = attr_line
      tmp = a
      while (match(tmp, /feature[ \t]*=[ \t]*"[^"]+"/)) {
        m = substr(tmp, RSTART, RLENGTH)
        sub(/^feature[ \t]*=[ \t]*"/, "", m)
        sub(/"$/, "", m)
        if (m == LH) cl_has_lh = 1
        else if (index(ENABLED, " " m " ") == 0 && index(" " cl_miss ",", " " m ",") == 0)
          cl_miss = (cl_miss == "" ? m : cl_miss "," m)
        tmp = substr(tmp, RSTART + RLENGTH)
      }
      # An unmodelled operator anywhere in the cluster makes the CLUSTER unclassifiable.
      if (a ~ /not[ \t]*\(/ || a ~ /any[ \t]*\(/ || a ~ /cfg_attr/) cl_unclass = 1
    }
    {
      t = $0
      sub(/^[ \t]+/, "", t)
      if (collecting) {
        buf = buf " " t
        if (!litok(t)) cl_unclass = 1
        depth += countch(nostr(t), "(") - countch(nostr(t), ")")
        if (depth <= 0) {
          collecting = 0
          handle_attr(buf)
          if (collecting_inner) { emit(); reset_cluster(); collecting_inner = 0 }
        }
        next
      }
      if (t ~ /^#!?\[/) {
        buf = t
        attr_line = NR
        # An INNER attribute (`#![...]`) gates the ENCLOSING scope and attaches to no
        # following item, so it is its own cluster and is emitted immediately. Outer
        # attributes are NOT separated this way: Rust attaches them to the next item across
        # blank lines and comments, so consecutive `#[...]` groups genuinely are one
        # cluster (that is what makes stacked-attribute conjunctions work). Without this
        # split a crate-level `#![cfg(...)]` merged with the attributes of the next item into a
        # single site, under-counting sites and merging their feature lists — caught by the
        # fixture below rather than by review.
        inner = (t ~ /^#!\[/)
        if (!litok(t)) cl_unclass = 1
        depth = countch(nostr(t), "(") - countch(nostr(t), ")")
        if (depth > 0) { collecting = 1; collecting_inner = inner }
        else {
          handle_attr(buf)
          if (inner) { emit(); reset_cluster() }
        }
        next
      }
      # Line comments, BLOCK comments and blanks are all cluster trivia (roborev round-36,
      # Medium). Treating only `//` as trivia meant a `/* … */` between stacked
      # `#[cfg(feature = "legacy-heuristics")]` and `#[cfg(feature = "experimental")]`
      # attributes SPLIT the cluster, so the co-required site was dropped and the census could
      # report a FALSE ZERO GAP — the silent under-report direction, in the one output whose
      # entire job is to state omissions.
      #
      # A multi-line block comment is tracked with a state flag; `in_block` deliberately does not
      # reset the cluster, which is the whole point. Not a Rust parser: it recognises the trivia
      # forms this corpus contains and anything else still ends the cluster, which is the
      # conservative direction (an unrecognised line ends a cluster ⇒ at worst a site is reported
      # separately, never silently merged away).
      if (in_block) { if (t ~ /\*\//) in_block = 0; next }
      if (t ~ /^\/\*/) { if (t !~ /\*\//) in_block = 1; next }
      if (t ~ /^\/\// || t ~ /^$/) next   # comments and blanks keep the cluster intact
      # Any other line ENDS the cluster: emit its verdict, then start fresh. What the
      # cluster gates is deliberately not inspected.
      emit(); reset_cluster()
    }
    END { emit() }
  ' "$1"
}

# _crate_gated_test_targets <pkg> — every declared `test` target of <pkg> whose source carries a
# CRATE-LEVEL inner gate, as `<target-name>\t<rel-path>\t<gate text, VERBATIM>`.
#
# DESCOPED ON A PRE-COMMITMENT (roborev rounds 20-27). This function used to CLASSIFY each gated
# target — off-here vs run-by-another-component vs CI-covered — with a feature grammar, a
# conjunction evaluator, a selector-reconciliation predicate over the gate's own source, and set
# arithmetic over target identities. That machinery produced a finding in five review rounds:
#   r21  an OPEN grammar, so `not(...)`/`any(...)` could read as "off" (false EXECUTE NOWHERE)
#   r21  `ci_n` counted targets cargo skips for unmet required-features
#   r22  only the FIRST of several stacked gates was read (conjunctive; hid a gap)
#   r22  compile-only invocations (`cargo build`, `clippy`, `--no-run`, `--lib`) counted as runners
#   r23  only the FIRST off feature of an `all(...)` was returned
#   r23  UNCLASSIFIED targets printed under the "executes nowhere" heading
#   r27  `#![cfg_attr(..., cfg(...))]` unrecognised, so a compiled-out target read as CI-covered
# EVERY ONE was the CLASSIFICATION being wrong. Not once was the OBSERVATION wrong: the gate is
# there, and here is its text.
#
# The previous commit pre-committed to descoping on the next classification finding rather than
# re-litigating it; r27 produced one, so this is that descope. It is the same call this change
# already made for the legacy co-required census (sites, not bodies) and the same call the owner
# made on #3312's delivery-mode classifier: when a component keeps producing findings, remove the
# part that requires judgement instead of patching the judgement again.
#
# WHY THE VERBATIM FORM IS NOT MERELY SMALLER BUT CORRECT WHERE THE CLASSIFIER WAS NOT: r27's
# `cfg_attr` case needs no handling here, because nothing is interpreted. An unrecognised gate form
# is printed, and a reader seeing `#![cfg_attr(feature = "x", cfg(feature = "y"))]` next to this
# lane's enabled feature set draws the right conclusion, where the classifier had to model
# `cfg_attr` semantics to avoid asserting a wrong one. The pattern therefore matches BOTH `#![cfg(`
# and `#![cfg_attr(` — presence, not meaning.
#
# What is lost: the computed 14/1/27 split. That figure corroborated CLAUDE.md's independently
# recorded #3375 count, which is worth something — so the census still cites #3375 as the record of
# which targets execute nowhere, and simply stops deriving it here.
#
# Fail-closed on a failed derivation or an unreadable declared source (rounds 25-26): an empty
# result must mean "nothing is gated", never "I could not tell".
_crate_gated_test_targets() { # <pkg>  -> name \t rel \t gate-text
  local pkg="$1" meta sp rel gate
  meta=$(_package_test_targets_gated "$pkg" __none__) || return 1
  [ -n "$meta" ] || return 1
  # FIVE fields now (roborev round-36 added required-features to the producer). Reading four would
  # silently append the 5th to `rel`, because the LAST `read` variable absorbs the remainder — so
  # this consumer had to change with the producer even though it ignores the new field. That
  # coupling is exactly why the producer's record shape is documented on its own line.
  while IFS=$'\t' read -r _tn sp _how rel _rf_ignored; do
    # An EMPTY src_path is a FAILED derivation, not a target to skip (self-review of the round-25/26
    # class). Skipping it drops the target from every population, so it lands among the ungated rest
    # BY OMISSION — the same silent-exclusion shape those two rounds fixed for unreadable sources,
    # reachable here through a metadata record cargo produced without a path.
    if [ -z "$sp" ]; then
      echo "NO-SRC-PATH for declared target '$_tn' (cargo metadata record carried no src_path)" >&2
      return 1
    fi
    if [ ! -r "$sp" ]; then
      echo "UNREADABLE $sp (declared target $_tn)" >&2
      return 1
    fi
    # ONLY THE CRATE'S LEADING INNER-ATTRIBUTE REGION (roborev round-35, Low). An inner
    # `#![cfg(...)]` is legal INSIDE an inline module too (`mod m { #![cfg(feature = "x")] … }`),
    # where it gates that module and NOT the target — reporting it as a crate-level gate would
    # overstate the census in the same "names something false" direction round 20 opened. Rust
    # requires crate-level inner attributes to precede every item, so the region is: from the top,
    # across blank lines, comments (`//`, `//!`) and attributes, stopping at the FIRST line that is
    # none of those. Conservative by construction — an attribute after any item is not counted, and
    # nothing is inferred about it.
    #
    # ONE awk PASS, not `grep | tr | sed` (roborev round-28, two Mediums in one line):
    #   * the pipeline's exit status was IGNORED, so a read error produced empty or partial output
    #     and the target silently vanished from the census — the fail-closed contract broken by the
    #     one construct that cannot report failure. awk exits 0 for "no match" and non-zero for a
    #     real error, which is exactly the distinction the pipeline could not make;
    #   * a MULTILINE attribute — `#![cfg(all(` … `))]`, which rustfmt produces for a long
    #     condition — was truncated to `#![cfg(`, discarding the very condition the reader needs.
    #     Parens are balanced across lines instead, so the whole attribute is printed.
    # No such attribute exists in this corpus today (measured: 0 across cqlite-flight/tests), so
    # this is the direction where a defect would have been invisible until someone reformatted.
    # DESCOPED TO AN OCCURRENCE REPORT (roborev rounds 37/38/40/41/42). Five consecutive rounds
    # found the next syntax that fooled a structural scan of the leading region: `//`, `/* */`,
    # `/*! */`, a multiline `#![cfg(all(`, a multiline NON-cfg attribute, and finally brackets
    # inside a STRING LITERAL (`#![doc = "["]`) throwing off the bracket arithmetic. Round 42's
    # reviewer suggested the honest remedy itself — "use Rust syntax tooling" — which a bash gate
    # component cannot have. Deciding *whether an inner attribute is crate-level* requires a Rust
    # parser; a line scan cannot, and every attempt to approximate it produced a new false claim.
    #
    # So this stops deciding. It reports OCCURRENCES: every line that looks like an inner cfg
    # attribute, verbatim, with the file it came from. The census states the limitation in the same
    # breath, so the reader knows a module-level inner attribute is indistinguishable from a
    # crate-level one HERE and can open the file — which is the same call this change already made
    # twice (the classifier became an observation in round 27; the legacy census reports sites, not
    # bodies). What is lost is a claim nobody could support; what remains needs no grammar at all.
    # TRI-STATE, not `|| true` (roborev root pass, Medium). `|| true` swallowed a source-read
    # failure and every transform error, so a partial or failed scan reported "no gated
    # occurrences" — the census's own all-clear, produced by the census failing. grep exits 0 for
    # matches, 1 for none, >=2 for a real error; only the last is fatal, and it FAILs the
    # derivation rather than returning a shorter list.
    local _gr_out _gr_rc=0
    _gr_out=$(grep -nE '^[[:space:]]*#!\[[[:space:]]*cfg(_attr)?[[:space:]]*\(' "$sp") || _gr_rc=$?
    if [ "$_gr_rc" -ge 2 ]; then
      echo "SCAN-ERROR grep exit $_gr_rc on $sp (declared target $_tn)" >&2
      return 1
    fi
    # "OPENING LINE", not "verbatim" (roborev root pass at aabae56ea, Low). The previous label
    # over-claimed: for a multiline `#![cfg(all(` this captures only the first line, so the census
    # omitted the very conditions it tells the reader to compare against the enabled feature set —
    # and the self-test only checked the `L<line>:` prefix, so it could not see the truncation.
    # Collecting the whole attribute needs a Rust parser (see the descope note above), so the claim
    # is narrowed to what the scan can support: file, line, and the opening line, explicitly marked
    # `+` when more of the attribute follows. The line number is what a reader acts on.
    # GUARDED on non-empty: `printf '%s\n' ""` emits ONE BLANK LINE, so the marker sed below
    # turned a file with NO inner cfg attribute into a bare "+" — a false occurrence report on
    # ordinary code. Caught by this suite's own r42 ungated case, not by either hand-built fixture:
    # two fixtures that both HAVE the thing cannot see the empty case.
    if [ -z "$_gr_out" ]; then
      gate=""
    else
      gate=$(printf '%s\n' "$_gr_out" \
        | sed 's/^\([0-9]*\):[[:space:]]*/L\1: /' \
        | sed 's/$/+/; s/\()\][[:space:]]*\)+$/\1/' \
        | tr '\n' ' ' | sed 's/  */ /g; s/^ //; s/ $//')
    fi
    [ -n "$gate" ] || continue
    printf '%s\t%s\t%s\n' "$_tn" "$rel" "$gate"
  done <<< "$meta"
}

# _package_test_targets_gated <pkg> <feature> — one TAB-separated record per `test`
# target of <pkg>: `<name>\t<abs src_path>\t<manifest|source>\t<package-relative path>`,
# where the fourth field is the identifier cargo prints after `Running ` (so the zero-tests
# guard and the allowed-zero list agree by construction), and the third field is
# `manifest` when the target's `required-features` name <feature> (cargo gates it) and
# `source` otherwise (the caller must then scan src_path itself).
#
# roborev round-7 finding (Medium): the legacy lane discovered targets with a
# `tests/*.rs` GLOB plus a cfg-string scan, which cannot see two shapes cargo does:
# a target gated ONLY by `required-features = ["legacy-heuristics"]` (its source may
# contain no cfg string at all), and a DIRECTORY-style target (`tests/foo/main.rs`).
# Either would be silently omitted while the lane's own report claims the set is derived
# so that "a new gated file is picked up with no gate edit" — so the CLAIM was wrong, not
# only the code. cargo is the authority on which targets exist and how they are gated.
_package_test_targets_gated() {
  local pkg="$1" feat="$2"
  local meta out
  meta=$(cargo metadata --format-version 1 --no-deps 2>/dev/null) || return 1
  [ -n "$meta" ] || return 1
  # jq FIRST, then python3, then failure (roborev round-18, Medium) — see
  # _package_unittest_srcs above for why a single-parser helper is a false red rather than
  # a missing convenience. Differentially tested against the python half by
  # test_agent_gate_summary.sh section 31 over this workspace's real metadata.
  if command -v jq >/dev/null 2>&1; then
    out=$(printf '%s' "$meta" | jq -r --arg n "$pkg" --arg feat "$feat" '
      .packages[] | select(.name == $n)
      | ((.manifest_path // "") | split("/") | .[0:-1] | join("/")) as $root
      | .targets[] | select([ (.kind // [])[] | select(. == "test") ] | length > 0)
      | ((."required-features" // .required_features // [])) as $rf
      | (.src_path // "") as $sp
      | [ (.name // ""), $sp,
          (if ($rf | index($feat)) then "manifest" else "source" end),
          (if ($root != "" and ($sp | startswith($root + "/")))
           then ($sp | ltrimstr($root + "/")) else $sp end),
          ($rf | join(",")) ] | @tsv') || return 1
    [ -n "$out" ] || return 1
    printf '%s\n' "$out"
    return 0
  fi
  command -v python3 >/dev/null 2>&1 || return 1
  out=$(printf '%s' "$meta" | python3 -c '
import json, os, sys
pkg, feat = sys.argv[1], sys.argv[2]
d = json.load(sys.stdin)
for p in d.get("packages", []):
    if p.get("name") != pkg:
        continue
    root = os.path.dirname(p.get("manifest_path", ""))
    for t in p.get("targets", []):
        if "test" not in (t.get("kind") or []):
            continue
        rf = t.get("required-features") or t.get("required_features") or []
        how = "manifest" if feat in rf else "source"
        sp = t.get("src_path") or ""
        # The 4th field is the PACKAGE-RELATIVE path, which is exactly what cargo prints
        # after `Running ` and therefore what the zero-tests guard keys on. Derived from the
        # manifest dir rather than by stripping a `tests/` prefix: an explicitly mapped
        # `[[test]] path = "..."` target need not live under tests/ at all, and the strip
        # would have left an ABSOLUTE path that can never match (roborev round-10 finding).
        rel = sp[len(root) + 1:] if root and sp.startswith(root + os.sep) else sp
        # 5th field: the COMPLETE required-features list (roborev round-36). The caller must compare
        # ALL of it against the resolved feature set, because cargo REJECTS an explicit
        # `--test <name>` whose required-features are unmet, so naming such a target is a FALSE RED.
        # NOTE no apostrophes in this comment: it sits inside a single-quoted `python3 -c` body, so
        # one would terminate the string. That has now bitten this file twice (round 25 was the
        # cli-tests body); `bash -n` catches it, which is why it is always worth running.
        print("%s\t%s\t%s\t%s\t%s" % (t.get("name", ""), sp, how, rel, ",".join(rf)))
' "$pkg" "$feat") || return 1
  [ -n "$out" ] || return 1
  printf '%s\n' "$out"
}

# _rust_module_closure <root-file> — every source file reachable from a Rust crate/module
# root, by standard `mod NAME;` resolution plus `#[path = "..."]`. Unresolved `mod`
# declarations go to stderr as `UNRESOLVED <name> <from>`; the caller FAILs on them.
#
# WHY THIS EXISTS (roborev rounds 11 and 12). A cargo test target is a MODULE TREE, not one
# file. Round 11 fixed discovery to look past the root and I approximated the tree with a
# directory guess; round 12 showed that guess misses `#[path = "..."]` modules and modules
# beside a flat root, AND — the part that matters — that the polarity scan and the census
# were still reading only the root file. So a positive gate living in a child module made a
# target ALLOWED-ZERO (excused from the zero-tests guard), and co-required sites in that
# child were absent from the census. Both silent.
#
# THIS IS THE THIRD ROUND IN THE SAME SHAPE — change where the data comes from, forget a
# consumer — so the fix is structural rather than another instance: ONE source set is
# computed per target here, and discovery, polarity and the census all read THAT SET. There
# is no longer a second place that decides which files a target consists of.
#
# It is not a Rust parser, and standard layouts are what it models: a crate root or `mod.rs`
# resolves children in its own directory, a plain `dir/NAME.rs` module resolves children
# under `dir/NAME/`, and `#[path]` resolves relative to the declaring file. An
# UNRESOLVED `mod` is a FAIL, not a shrug: it means the source set is incomplete, and every
# consumer of an incomplete set fails in the SILENT direction. Measured on this corpus:
# 0 unresolved across all 364 cqlite-core test targets, so failing closed costs nothing
# today and stays loud if a layout appears that this does not model.
# ============================================================================================
# STATED LIMIT OF THIS SCANNER — read this before adding a shape to it (issue #1699, #3472).
#
# THIS IS AN OCCURRENCE REPORT, NOT A PARSER, AND ITS COVERAGE IS DELIBERATELY INCOMPLETE.
# It reports the Rust attribute and module shapes it RECOGNISES. The set of shapes it does NOT
# model is OPEN — Rust attribute and module syntax is defined by rustc, not here — so no amount
# of iteration finishes it. Twelve review findings across this branch were all one family: a
# further shape the scanner did not model. Rounds 41 and 42 already made the correct structural
# move (real trivia state; descope the crate-gate scanner to an occurrence report) and the
# findings continued, which is the evidence that the surface is unbounded rather than merely
# large.
#
# WHAT THIS SCAN DOES AND DOES NOT GIVE YOU. Read this instead of relying on a guarantee — there
# deliberately is not one, and the two attempts to state one are recorded below because the second
# was falsified within a day of the first.
#
# THE INTENT: an unrecognised shape should be reported as UNCLASSIFIED rather than omitted, so that
# a miss costs NOISE (an unattributable entry a human reads) rather than BLINDNESS (a clean zero
# over gated code). Every change here must push in that direction, and where the scan can detect
# that it cannot tell, it says so at runtime — an unmodelled string-literal shape prints
# `[UNCLASSIFIED: ...]` rather than resolving.
#
# THE INTENT IS NOT ENFORCED, AND IT CANNOT BE. By construction this scan cannot enumerate the
# shapes it fails to detect: an undetected shape produces no output to mark unclassified. So the
# direction is a design aim, NOT a property you may rely on, and the following are MEASURED
# counterexamples rather than hypotheticals:
#   * a delimiter inside a string literal (`doc = "]"`) closed a cluster early     — FIXED (job 101)
#   * a `mod` form the patterns do not match (`mod r#type;`, a split declaration)  — OPEN  (job 103)
#   * a delimiter inside a line comment (`feature = "x" // ]`)                     — OPEN  (job 105)
# The two open ones resolve to ABSENT, not UNCLASSIFIED, and emit nothing at all.
#
# THEREFORE, THE ONE SENTENCE TO CARRY AWAY: a clean census is evidence that nothing was
# RECOGNISED — never evidence that nothing is THERE.
#
# WHY THIS IS STATED AS A DISCLAIMER AND NOT AS A NARROWER GUARANTEE. It was a guarantee twice.
# Unscoped, it said an unrecognised shape is always UNCLASSIFIED; job 103 falsified that. Scoped to
# ATTRIBUTE AND CLUSTER shapes, it excluded declaration recognition; job 105 falsified that too,
# with a comment-borne delimiter, which IS a cluster shape. A guarantee qualified per path acquires
# a new exception per review round, for exactly the reason the scanner does — so a third
# qualification would be the prose equivalent of an eighth pattern. An overclaimed guarantee is
# worse than an omission: an omission leaves a reader uninformed, an overclaim has them relying on
# cover that is not there, and a reader of this line does not read #3472.
#
# DO NOT FIX EITHER OPEN ITEM WITH ANOTHER PATTERN. #3472 holds the seven measured lexical contexts
# and the reasoning; the answers there are syntax-aware tooling or deleting the scanning half.
#
# WHAT NOT TO DO: do not add a thirteenth shape and call the family closed. If correctness rather
# than advice is ever required of this scan, the answer is syntax-aware tooling or deleting the
# scanning half — not another pattern. #3472 carries the family and the reasoning.
# ============================================================================================
# STDERR carries TWO fail-closed report kinds, and the caller FAILs on either:
#   UNRESOLVED <name> <from>              — a declared `mod` whose file was not found
#   CFG-GATED-MOD <name> <from> [<cfg>]   — a `mod` gated by a cfg this scan does not evaluate
# The second exists because the closure used to follow children while DISCARDING the attributes
# gating them, so a gated child's legacy test read as executable at this lane's feature set.
# Count of DECLARED cfg-gated-subtree gaps in the current legacy-heuristics run. Declared, not
# fatal — see the split in run_legacy_heuristics.
_lh_cfg_gaps=0
_rust_module_closure() { # <root-file>  -> one path per line; both report kinds to stderr
  # ARRAY QUEUE, not newline-delimited string surgery. The first cut used
  # `${queue%%<newline>*}` and produced a line beginning with `}`, which truncated the
  # `awk '/^_rust_module_closure/,/^\}/'` extraction the self-test uses — so the function
  # could not be behaviourally tested at all, and the self-test reported a bogus 0 sources.
  # A guard that cannot be extracted cannot be tested, so the shape matters here.
  local root="$1"
  local -a queue=("$root")
  local seen=" " out="" f dir base childdir kind val
  while [ "${#queue[@]}" -gt 0 ]; do
    f="${queue[0]}"
    queue=(${queue[@]+"${queue[@]:1}"})
    [ -n "$f" ] || continue
    case "$seen" in *" $f "*) continue ;; esac
    seen="$seen$f "
    # An UNREADABLE source is a FAILED derivation, not a file to skip (roborev round-26, Medium):
    # skipping it yields an incomplete closure, which makes the polarity scan and the census read a
    # partial module tree and can drop the target from the lane entirely — a false PASS built from
    # a file nobody could read.
    if [ ! -r "$f" ]; then
      echo "UNREADABLE $f (module closure of $root)" >&2
      return 1
    fi
    out="$out$f
"
    dir="${f%/*}"; base="${f##*/}"
    # Where do THIS file's child modules live? A crate root or mod.rs resolves children in
    # its own directory; a plain `dir/NAME.rs` module resolves them under `dir/NAME/`.
    case "$base" in
      mod.rs|main.rs|lib.rs) childdir="$dir" ;;
      *) if [ "$f" = "$root" ]; then childdir="$dir"; else childdir="${f%.rs}"; fi ;;
    esac
    # THREE fields: the `G` record carries a gate text, and a 2-field `read` would absorb it
    # into `val` and then mis-report the child as UNRESOLVED — a wrong cause on every lane.
    while IFS="$(printf '\t')" read -r kind val extra; do
      [ -n "$kind" ] || continue
      if [ "$kind" = G ]; then
        # A cfg ON the `mod` declaration. Reported, never followed silently: the subtree's
        # reachability at this lane's feature set is UNKNOWN, and every consumer of the source
        # set (membership, allowed-zero polarity, the co-required census) is permissive on an
        # unknown — which is precisely how a gated child's test was counted as executable.
        echo "CFG-GATED-MOD $val $f [$extra]" >&2
        continue
      fi
      if [ "$kind" = P ]; then
        # #[path] resolves relative to the declaring file's directory.
        case "$val" in
          /*) queue+=("$val") ;;
          *)  queue+=("$dir/$val") ;;
        esac
      elif [ -r "$childdir/$val.rs" ]; then
        queue+=("$childdir/$val.rs")
      elif [ -r "$childdir/$val/mod.rs" ]; then
        queue+=("$childdir/$val/mod.rs")
      else
        # REPORTED, never shrugged off: an incomplete source set is silently permissive in
        # every consumer (membership, polarity, census). The caller FAILs on this.
        echo "UNRESOLVED $val $f" >&2
      fi
    done <<EOF
$(awk '
  # Block comments are TRIVIA, statefully (roborev round-41). Without this, a `#[path]` or `mod`
  # inside `/* … */` was read as a real declaration — so the closure could scan a file Rust never
  # includes, or FAIL the lane as unresolved on a commented-out example — and a block comment
  # between a real `#[path]` and its `mod` cleared the pending path, silently unbinding them.
  in_block { if ($0 ~ /\*\//) in_block = 0; next }
  /^[[:space:]]*\/\*/ { if ($0 !~ /\*\//) in_block = 1; next }
  # A single-line `/* … */` anywhere on an otherwise-trivia line is also trivia.
  /^[[:space:]]*\/\*.*\*\/[[:space:]]*$/ { next }
  # A MULTILINE ATTRIBUTE IS ONE CLUSTER (roborev job 99, Medium). rustfmt legitimately writes
  #     #[cfg(all(
  #         feature = "state_machine",
  #         feature = "cli-helpers"
  #     ))]
  #     mod child;
  # and those continuation lines match no attribute pattern, so they fell through to the
  # cluster-end rule, which discarded the pending gate text and left the child reading as
  # UNCONDITIONAL — reintroducing for the multiline form the exact defect just fixed for the
  # single-line one. Note the direction: this was a REGRESSION INTRODUCED BY THAT FIX, since
  # clearing gatetxt at cluster end is what made a continuation line destructive.
  # Balance is counted on SQUARE BRACKETS OUTSIDE STRING LITERALS.
  #
  # THE PREVIOUS COMMENT HERE CLAIMED A SAFETY PROPERTY THIS CODE DOES NOT HAVE, and it is
  # deleted rather than softened: it said a delimiter inside a string literal "can only leave
  # the cluster open longer ... never hide one". That is true of an unmatched `[`, which is the
  # case it was reasoned about, and FALSE of an unmatched `]` — which closes the cluster EARLY,
  # so the real closing line then clears the pending cfg and a gated child reads as
  # UNCONDITIONAL. That HIDES a gap (roborev job 101, Medium). A claimed bound that holds in
  # only one direction is worse than none, because the next reader trusts it.
  #
  # Quoted spans are stripped before counting. Where the strip cannot be trusted — an escaped
  # quote or a raw string — the pending gate becomes UNCLASSIFIED rather than resolved, so the
  # following module is DECLARED as unattributable instead of silently treated as unconditional.
  # This is the same declare-do-not-model choice as the coreq scanner: the set of Rust attribute
  # shapes not modelled here is OPEN, so the only safe behaviour on an unrecognised one is to
  # say so.
  function nostr(str) { gsub(/"[^"\\]*"/, "", str); return str }
  function litok(str) { return (str !~ /\\"/ && str !~ /r#"/ && nostr(str) !~ /"/) }
  attrdepth > 0 {
    t = $0; gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
    if (incfg) gatetxt = gatetxt " " t
    if (!litok($0)) {
      printf "CFG-GATED-MOD <attribute-at-line-%d> %s [UNCLASSIFIED: string-literal shape this scan does not model; cluster balance unknown]\n", NR, FILENAME > "/dev/stderr"
      attrdepth = 0
      incfg = 0
      gatetxt = ""
      next
    }
    nostr_line = nostr($0)
    attrdepth += gsub(/\[/, "[", nostr_line) - gsub(/\]/, "]", nostr_line)
    if (attrdepth < 1) { attrdepth = 0; incfg = 0 }
    next
  }
  # A `#[path = "..."]` ATTRIBUTE only, and a same-line `mod` must still be processed (roborev
  # round-40, Medium). Two defects in one line:
  #   * `next` fired unconditionally, so the very common single-line form
  #     `#[path = "child.rs"] mod child;` recorded the path and then SKIPPED the `mod` — the child
  #     was never queued, so legacy-gated tests inside it escaped discovery, the polarity scan and
  #     the census entirely. Silent, and in the direction that under-reports coverage.
  #   * the pattern matched anywhere on a line, so a doc comment or a string mentioning
  #     `#[path = "..."]` set `haspath` and could bind a STALE path to the next real `mod`.
  # Anchored at the start of the line (attributes are the first thing on their line; a `//`/`//!`
  # comment or an inline mention therefore cannot match), and `next` only when no `mod` follows.
  /^[[:space:]]*#\[[[:space:]]*path[[:space:]]*=/ {
    if (match($0, /"[^"]+"/)) { p = substr($0, RSTART+1, RLENGTH-2); haspath = 1 }
    if ($0 !~ /(^|[[:space:]])mod[[:space:]]/) next
    # falls through to the mod rule below, which consumes `haspath`
  }
  # EVERY visibility form, not just private and plain `pub` (roborev round-13 finding):
  # `pub(crate) mod`, `pub(super) mod` and `pub(in path) mod` are ordinary declarations, and
  # this corpus has 30 semicolon-terminated `pub(crate) mod` lines — so skipping them was
  # LIVE coverage loss. Their child modules were invisible to discovery, the polarity scan
  # AND the census, all three in the silent direction.
  # An optional leading ATTRIBUTE is allowed before `mod` (roborev round-40): the single-line form
  # `#[path = "child.rs"] mod child;` is idiomatic, and without this the declaration was unreachable
  # from here even after the path rule stopped swallowing the line.
  /^[[:space:]]*(#\[[^]]*\][[:space:]]*)?(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?mod[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/ {
    # a SAME-LINE cfg attribute counts too: `#[cfg(feature = "x")] mod child;`
    if ($0 ~ /#\[[[:space:]]*cfg(_attr)?[[:space:]]*\(/) {
      gt = $0; gsub(/^[[:space:]]+|[[:space:]]+$/, "", gt)
      gatetxt = (gatetxt == "" ? gt : gatetxt " " gt)
    }
    n = $0
    sub(/^[[:space:]]*(#\[[^]]*\][[:space:]]*)?(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?mod[[:space:]]+/, "", n)
    sub(/[[:space:]]*;.*$/, "", n)
    # A cfg ON THE `mod` DECLARATION is reported alongside the child (roborev root pass at
    # aabae56ea, Medium). The closure followed children while DISCARDING the attributes that gate
    # them, so `#[cfg(feature = "experimental")] mod child;` read as reachable at the feature
    # set of this lane. NO APOSTROPHE MAY APPEAR IN THIS AWK PROGRAM: it is single-quoted, so one
    # closes the quote early and bash then parses awk source as shell — and `bash -n` can PASS
    # anyway when the stray quotes happen to re-balance, which is how this slipped through twice.
    # A legacy-gated test inside `child` was then counted as executable, an ungated
    # sibling kept the target non-zero, and the co-required census reported NO gap — which is the
    # one thing that census exists to find.
    #
    # Not evaluated here, DECLARED: emitting the gating cfg text with the child lets the caller
    # treat the subtree as an unclassified co-required site instead of silently assuming
    # reachability. Evaluating nested cfg reachability is a Rust-parser problem, and this file has
    # already paid five rounds for approximating one.
    if (gatetxt != "") { printf "G\t%s\t%s\n", n, gatetxt }
    if (haspath) { printf "P\t%s\n", p; haspath = 0 } else { printf "M\t%s\n", n }
    gatetxt = ""
    next
  }
  # An ATTRIBUTE line preserves a pending `#[path]` (roborev round-42). An outer-attribute cluster
  # ACROSS LINES is legal and IS handled here — clearing `haspath` on the intervening attribute
  # resolved the module to the WRONG file, or failed the lane as unresolved.
  #
  # THE SAME-LINE MULTI-ATTRIBUTE FORM IS NOT HANDLED, and this comment used to imply it was
  # (roborev job 111, Medium): it offered `#[path = "mapped.rs"] #[cfg(...)] mod child;` as an
  # example of what works, while the `mod` rule below accepts exactly ONE leading attribute, so
  # that declaration is skipped and its child leaves the closure entirely. The example named the
  # one shape that fails. It is corrected rather than implemented: another regex would be the
  # ninth pattern, and the eighth lexical context belongs in #3472 as a counterexample under the
  # disclaimer above. Measured: 0 such declarations in this workspace, so it is latent. Attributes join blank lines and comments as cluster trivia; anything else still
  # ends the cluster, which stays the conservative direction.
  /^[[:space:]]*#\[/ {
    # Remember a cfg on this declaration so the `mod` rule can DECLARE it with the child.
    iscfg = ($0 ~ /#\[[[:space:]]*cfg(_attr)?[[:space:]]*\(/)
    if (iscfg) {
      t = $0; gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
      gatetxt = (gatetxt == "" ? t : gatetxt " " t)
    }
    # OPEN the cluster when the brackets do not close on this line, so the continuation rule
    # above collects the rest instead of the cluster-end rule destroying it. Counted the SAME
    # way as the continuation rule — on the string-stripped line — or the two halves disagree
    # about where a cluster starts and ends, which is its own defect.
    nostr_open = nostr($0)
    attrdepth = gsub(/\[/, "[", nostr_open) - gsub(/\]/, "]", nostr_open)
    if (attrdepth > 0) { incfg = iscfg } else { attrdepth = 0 }
    if (!litok($0) && iscfg) {
      printf "CFG-GATED-MOD <attribute-at-line-%d> %s [UNCLASSIFIED: string-literal shape this scan does not model; cluster balance unknown]\n", NR, FILENAME > "/dev/stderr"
      gatetxt = ""
    }
    next
  }
  # BOTH pendings die with the cluster (roborev job 97, Medium). Clearing `haspath` while leaving
  # `gatetxt` let a cfg on ANY item — a function, a struct, an impl — leak forward and tag the next
  # UNGATED `mod` as gated, i.e. a false DECLARED GAP on ordinary code. Same class as the
  # `haspath` leak of round-42, and the noleak assert missed it because it only covered a cfg
  # attached to a mod, never a cfg attached to something else entirely.
  { if ($0 !~ /^[[:space:]]*$/ && $0 !~ /^[[:space:]]*\/\//) { haspath = 0; gatetxt = "" } }
' "$f")
EOF
  done
  printf '%s' "$out"
}

# _lh_positive_in_closure <closure> <cfg-site-regex> — does ANY file in this target's module
# closure carry a POSITIVE `legacy-heuristics` cfg reference (i.e. one that survives stripping
# every `not(feature = "legacy-heuristics")` wrapper)?
#
# A portable loop rather than `xargs -r` (roborev round-14): `-r` is GNU-only and this gate
# supports stock macOS, where the lane would otherwise have mis-scanned every target. `sed |
# grep -c` consumes each file whole, so there is no early-close SIGPIPE race either (#3380).
_lh_positive_in_closure() {
  # Returns 0 = POSITIVE (do NOT excuse this target), 1 = negative-only (excusable), 2 = could not
  # read. Note the polarity of the default: anything not affirmatively recognised as the direct
  # negative form yields 0, so a shape this function has never seen costs a target its excusal and
  # never costs the gate a zero-tests check.
  local closure="$1" cfg_site="$2" cf _pc_sites _pc_allowed _pc_rc
  # THE ONE RECOGNISED SHAPE, as a whole attribute: `#[cfg(not(feature = "legacy-heuristics"))]` or
  # its inner `#![...]` form, alone on its line. Anchored end-to-end deliberately — a substring
  # match is what let a nested `not(feature = …)` inside a larger expression look direct.
  local _pc_allow='^[[:space:]]*#!?\[cfg\(not\(feature[[:space:]]*=[[:space:]]*"legacy-heuristics"\)\)\][[:space:]]*$'
  while IFS= read -r cf; do
    [ -n "$cf" ] || continue
    _pc_rc=0
    _pc_sites=$(grep -cE "$cfg_site" "$cf") || _pc_rc=$?
    if [ "$_pc_rc" -ge 2 ]; then
      echo "POLARITY-SCAN-ERROR grep exit $_pc_rc on $cf" >&2
      return 2
    fi
    [ "${_pc_sites:-0}" -gt 0 ] || continue
    _pc_rc=0
    _pc_allowed=$(grep -cE "$_pc_allow" "$cf") || _pc_rc=$?
    if [ "$_pc_rc" -ge 2 ]; then
      echo "POLARITY-SCAN-ERROR grep exit $_pc_rc on $cf (allowlist pass)" >&2
      return 2
    fi
    if [ "${_pc_sites:-0}" -ne "${_pc_allowed:-0}" ]; then
      # Not a claim that the site IS positive — a refusal to claim it is safely negative.
      echo "POLARITY-UNRECOGNISED $cf ($_pc_sites site(s), $_pc_allowed recognised-negative) — not excused" >&2
      return 0
    fi
  done <<EOF
$closure
EOF
  return 1
}

run_legacy_heuristics() {
  # reset per run: state carried in from a previous lane would misreport this one
  _lh_cfg_gaps=0
  local lh_gap_detail=() _gd
  local name=legacy-heuristics
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)

  # Derive the target set: every committed cqlite-core/tests/*.rs carrying a
  # legacy-heuristics CFG SITE, mapped to its target name (basename without .rs).
  # Anchored on REPO_ROOT so the derivation cannot depend on CWD.
  #
  # Matched on the ATTRIBUTE shape `feature = "legacy-heuristics"`, not the bare
  # string: compile_time_heuristic_enforcement.rs alone mentions the feature 15 times
  # in prose, and a doc comment is not a cfg site. (Measured: both spellings yield the
  # same 5 files today, so this is precision for the future, not a change of subject.)
  #
  # ALLOWED-ZERO, DERIVED (never a curated list). A file whose ONLY sites are the
  # NEGATIVE polarity `#[cfg(not(feature = "legacy-heuristics"))]` legitimately
  # executes 0 tests HERE — its bodies compile out when the feature is ON, and they
  # already run in core-tests, where the feature is off. It stays in the executed set
  # (so it must still COMPILE at this feature set) but is passed to the zero-tests
  # guard as allowed-zero, with the reason printed. The guard therefore still FAILs on
  # the case that matters: a POSITIVE-polarity file that executes nothing.
  #
  # Polarity is decided by stripping every `not(feature = "legacy-heuristics")`
  # wrapper and asking whether any cfg-shaped reference survives — mechanical, so a
  # file that gains a positive site stops being allowed-zero with no gate edit.
  local tests_dir="$REPO_ROOT/cqlite-core/tests"
  local -a targets=() allow_zero=() observe_ids=()
  local cfg_site='feature[[:space:]]*=[[:space:]]*"legacy-heuristics"'
  local names="" negonly="" f base count=0 srcs=""
  # CANDIDATES FROM CARGO, NOT A GLOB (roborev round-7 finding). See
  # _package_test_targets_gated for why: a manifest-gated or directory-style target is
  # invisible to `tests/*.rs`. A failed enumeration FAILs and names the derivation — it is
  # never a fallback to the glob, which would silently shrink the target set.
  local meta_targets
  if ! meta_targets=$(_package_test_targets_gated cqlite-core legacy-heuristics); then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not enumerate cqlite-core's test targets from cargo"
      echo "        metadata, so the legacy-heuristics target set is unmeasurable. The"
      echo "        DERIVATION failed; this is deliberately NOT a fallback to a tests/*.rs"
      echo "        glob, which omits manifest-gated and directory-style targets."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi
  # HOISTED ABOVE THE TARGET LOOP (roborev round-36). It used to be resolved here, ~200 lines
  # BELOW the loop that now needs it to compare each target's required-features — so the comparison
  # would have run against an EMPTY set, marked every target unmet, and silently emptied the lane.
  # I wrote that bug into this fix and caught it before it ran; it is the same
  # unmeasured-signal-reaches-a-permissive-branch shape this whole change exists to remove.
  local lh_enabled
  if ! lh_enabled=$(_resolved_package_features cqlite-core --features legacy-heuristics); then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not derive cqlite-core's enabled feature set at"
      echo "        default+legacy-heuristics via 'cargo tree -p cqlite-core' (a cargo"
      echo "        failure or an offline registry), so the co-required-feature census is"
      echo "        unmeasurable. A census that cannot be taken is not reported as empty."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi
  local _mt_name _mt_src _mt_how _mt_rel _mt_rf _mt_dir _mt_hit _mt_cf _obs_id _mt_cnt _mt_rc _pol_rc
  local rf_unmet=""
  while IFS="$(printf '\t')" read -r _mt_name _mt_src _mt_how _mt_rel _mt_rf; do
    [ -n "$_mt_name" ] || continue
    f="$_mt_src"
    # Included when EITHER cargo gates the target on the feature (the arm the glob could
    # not see) OR its own source carries a cfg reference to it.
    # ONE source set per target, shared by membership, polarity and the census (round 12).
    local _mt_closure _mt_unres _mt_gaps_pending=""
    _mt_unres="$LOG_DIR/legacy-unresolved-$_mt_name.txt"
    _mt_closure=$(_rust_module_closure "$f" 2>"$_mt_unres")
    # TWO report kinds, TWO different consequences, and a CLOSED grammar over the rest.
    #
    # UNRESOLVED is fatal: a `mod` whose file was not found means the source set is INCOMPLETE,
    # and an incomplete set is permissive in membership, the polarity scan and the census alike.
    #
    # CFG-GATED-MOD is a DECLARED COVERAGE GAP, not a failure. The child stays in the source set
    # (conservative for membership — a gated test still makes its target a subject), but its
    # reachability at this feature set is unevaluated, so the census must SAY SO instead of
    # reporting no gap (roborev root pass at aabae56ea, Medium). Failing the lane here was tried
    # and is WRONG: the tree legitimately carries `#[cfg(all(feature=..))] #[path=..] mod support;`
    # on shared test helpers, so fail-closed reds ordinary code — and a lane that reds on correct
    # input is the lane agents learn to waive. Declaring the narrowing at run time is what this
    # whole component set is built on.
    #
    # Anything else in that stream FAILs: an unrecognised report is an unmeasured state, and
    # inheriting the permissive branch for it is the shape this PR exists to remove.
    if [ -s "$_mt_unres" ]; then
      local _mt_fatal="$LOG_DIR/legacy-fatal-$_mt_name.txt"
      local _mt_gaps="$LOG_DIR/legacy-cfggaps-$_mt_name.txt"
      # STATUS OBSERVED, and the CLOSED GRAMMAR ACTUALLY IMPLEMENTED (self-review after job 115).
      # `|| true` masked grep exit >=2, so an unreadable stream produced two EMPTY halves and the
      # code below then found neither a fatal report nor a gap — a clean pass derived from a scan
      # that failed. We only reach here because `[ -s "$_mt_unres" ]` was TRUE, so "both halves
      # empty" is impossible unless a read failed: that is the unrecognised-report case the comment
      # above claims to fail on, and it had no branch. grep 1 = no match (fine), >=2 = error.
      local _sp_rc1=0 _sp_rc2=0
      grep -v '^CFG-GATED-MOD ' "$_mt_unres" > "$_mt_fatal" || _sp_rc1=$?
      grep    '^CFG-GATED-MOD ' "$_mt_unres" > "$_mt_gaps"  || _sp_rc2=$?
      if [ "$_sp_rc1" -ge 2 ] || [ "$_sp_rc2" -ge 2 ] \
         || { [ ! -s "$_mt_fatal" ] && [ ! -s "$_mt_gaps" ]; }; then
        status=FAIL
        {
          echo "[$name] FAIL-CLOSED: could not classify the module-closure reports for test"
          echo "        target '$_mt_name' (grep exits $_sp_rc1/$_sp_rc2; non-empty stream split"
          echo "        into two empty halves means a read failed, or a report matched neither"
          echo "        recognised form). A stream we know is non-empty must produce a report;"
          echo "        treating it as silence would be a pass derived from a scan that failed."
          sed 's/^/          /' "$_mt_unres"
        } | tee "$log"
        end=$(date +%s)
        record_result "$name" "$status" "$((end - start))"
        echo ">>> [$name] $status ($((end - start))s)"
        return 0
      fi
      if [ -s "$_mt_fatal" ]; then
        status=FAIL
        {
          echo "[$name] FAIL-CLOSED: could not resolve the module tree of test target"
          echo "        '$_mt_name'. An incomplete source set makes membership, the"
          echo "        allowed-zero polarity scan and the co-required census all fail in the"
          echo "        SILENT direction, so this is a FAIL rather than a partial scan:"
          sed 's/^/          /' "$_mt_fatal"
        } | tee "$log"
        end=$(date +%s)
        record_result "$name" "$status" "$((end - start))"
        echo ">>> [$name] $status ($((end - start))s)"
        return 0
      fi
      # BUFFERED, not emitted here (roborev job 97, Medium + Low). Two reasons, and both were
      # live defects:
      #   (1) this point is BEFORE the membership and required-features filters, so declaring here
      #       reported a "gap" for targets that are not subjects of this lane at all — measured on
      #       issue_2827_partition_access_bytes, which carries no legacy site. A census diluted
      #       with irrelevant entries is a census nobody reads. Decide, THEN record — the same
      #       ordering this lane already enforces for observe_ids and --test.
      #   (2) the detail lines were `tee -a`'d to "$log", which the census below then TRUNCATES
      #       with `>` — so the aggregate said "listed above" and the listing was gone. The
      #       comment three lines under that redirect says it outright: a gap that only appears on
      #       stdout is a gap nobody reads.
      if [ -s "$_mt_gaps" ]; then _mt_gaps_pending="$_mt_gaps"; fi
    fi
    if [ "$_mt_how" != "manifest" ]; then
      [ -f "$f" ] || continue
      # Membership over the WHOLE module tree: a gated test can live in a child module
      # whose root never names the feature.
      # A PORTABLE LOOP, not `xargs -r` (roborev round-14 finding, Medium). `-r` is
      # GNU-only: BSD/macOS xargs rejects it, and this gate explicitly supports stock macOS
      # (see the bash 3.2 note in check_unittest_targets_ran). On that host the lane would
      # have skipped every source-gated target and then reported a failed derivation.
      # Dropping `-r` alone is NOT the fix either: without it GNU xargs runs the command
      # once with NO file arguments, and `grep -lE <pattern>` with no files reads STDIN —
      # which here is the already-consumed pipe. A loop has neither problem, and `grep -c`
      # consumes each file whole so there is no SIGPIPE race (cf. the #3380 class).
      _mt_hit=0
      while IFS= read -r _mt_cf; do
        [ -n "$_mt_cf" ] || continue
        # TRI-STATE (roborev root pass, Medium): `grep -c … 2>/dev/null` reported 0 both for "no
        # match" and for "could not read", so a scan failure silently omitted a source-gated target
        # from the lane — and an omitted target cannot fail the zero-tests guard, which is how an
        # empty run passes. grep: 0 = matched, 1 = no match, >=2 = error.
        _mt_cnt=$(grep -cE "$cfg_site" "$_mt_cf"); _mt_rc=$?
        if [ "$_mt_rc" -ge 2 ]; then
          echo "SCAN-ERROR grep exit $_mt_rc on $_mt_cf (target $_mt_name)" >&2
          status=FAIL
          {
            echo "[$name] FAIL-CLOSED: the legacy cfg-site scan could not read $_mt_cf (grep exit"
            echo "        $_mt_rc). A failed scan reads as 'no legacy site', which silently drops"
            echo "        the target from the lane — and a dropped target cannot fail the"
            echo "        zero-tests guard, so an empty run would pass."
          } | tee -a "$log"
          end=$(date +%s); record_result "$name" "$status" "$((end - start))"
          echo ">>> [$name] $status ($((end - start))s)"; return 0
        fi
        if [ "${_mt_cnt:-0}" -gt 0 ]; then _mt_hit=1; break; fi
      done <<EOF
$_mt_closure
EOF
      [ "$_mt_hit" -eq 1 ] || continue
      # THE NON-MANIFEST BRANCH ENDS HERE (roborev job 111, Medium). The required-features
      # check below used to sit INSIDE it, so a target cargo gates on the feature — classified
      # `manifest` — skipped validation entirely and was then handed to cargo explicitly. Cargo
      # rejects a target whose required-features are unmet, so the lane FAILED on a correct
      # target: a lane that reds on correct input is the lane agents learn to waive, which is
      # the rule this component set is built on. Latent today (0 of 13 required-features targets
      # name legacy-heuristics) and therefore also UNEXERCISED at runtime, which is this issue
      # own distinction applied to its own code.
    fi
    # AFTER MEMBERSHIP, BEFORE INVOCATION (roborev root pass, Low). Round 37 hoisted this to the
    # top of the loop to fix an ordering bug, and created another: the check ran BEFORE the
    # legacy-membership test, so EVERY target with any unmet required-feature was reported as a
    # legacy coverage gap. MEASURED false claims in the shipped census — 5 of them, none
    # legacy-gated: issue_1495_arrow_accessor_parity(arrow),
    # issue_1695_query_timeout(cli-helpers), issue_1869_big_clustering_slice_readat(work-counters),
    # issue_2148_statistics_toc_single_walk(cli-helpers), issue_2302_written_index_resolve. A census
    # inventing gaps is worse than one omitting them: it sends the reader to targets that are fine.
    #
    # The invariant, now stated where both orderings can be seen: membership decides WHETHER this
    # target is our subject; the required-features check decides whether we may INVOKE it; and
    # nothing may RECORD it before both. Round 37 got the second half right and the first wrong.
    #
    # (round 37, Medium) The check must still precede every record of the target —
    # AFTER `observe_ids+=(...)` — so a target excluded here had already been added to the
    # observation set, and `check_test_targets_observed` then demanded a `Running` banner for a
    # target the lane deliberately never invoked: a FALSE RED on valid code, produced by the fix
    # that was meant to prevent one. Anything that records a target must run after the decision to
    # invoke it, so the decision goes first.
    #
    # A target whose FULL `required-features` are not satisfied by this feature set must
    # NOT be named explicitly (roborev round-36, Medium). Cargo REJECTS an explicit
    # `--test <name>` whose required-features are unmet, so the lane would fail on entirely
    # correct code — a FALSE RED, and the kind that looks like a real breakage. The helper
    # already carries the manifest's list; this compares ALL of it, not just the presence of
    # `legacy-heuristics`, and reports the target as a COVERAGE GAP instead of invoking it.
    local _rf_off=""
    if [ -n "${_mt_rf:-}" ]; then
      local _rf1
      for _rf1 in ${_mt_rf//,/ }; do
        [ -n "$_rf1" ] || continue
        case " $lh_enabled " in
          *" $_rf1 "*) ;;
          *) _rf_off="${_rf_off:+$_rf_off,}$_rf1" ;;
        esac
      done
    fi
    if [ -n "$_rf_off" ]; then
      # `$_mt_name`, NOT `$base` (roborev round-38, Medium). `base` is assigned ~60 lines below this
      # point — a consequence of round 37 hoisting this decision to the top of the loop — so the
      # diagnostic named the PREVIOUS iteration's target, or nothing at all on the first one. A
      # coverage census that misattributes its own gaps is worse than one that omits them, because
      # it sends the reader to a target that is fine.
      rf_unmet="$rf_unmet $_mt_name(required-features unmet:$_rf_off)"
      continue
    fi
    base="$_mt_name"
    # The observation set is EVERY selected target, spelled the guard's way, so the lane can
    # assert that each one actually ran (round-17). Distinct from allow_zero, which only says
    # which of them may legitimately run zero tests.
    _obs_id="$_mt_rel"
    case "$_mt_rel" in
      tests/*) _obs_id="${_mt_rel#tests/}" ;;
    esac
    # THE TARGET IS NOW A CONFIRMED SUBJECT — membership passed and required-features are met —
    # so a buffered cfg-gated-subtree gap becomes a DECLARED one here and nowhere earlier.
    if [ -n "${_mt_gaps_pending:-}" ] && [ -s "$_mt_gaps_pending" ]; then
      lh_gap_detail+=("DECLARED GAP: test target '$_mt_name' reaches child module(s) through a cfg")
      lh_gap_detail+=("  this scan does not evaluate, so their contribution to legacy coverage is")
      lh_gap_detail+=("  UNCLASSIFIED — the subtree IS scanned, but a zero or nonzero result from it")
      lh_gap_detail+=("  cannot be attributed. Compare the cfg against the enabled set by hand; move")
      lh_gap_detail+=("  gated tests to their own target if the attribution matters:")
      while IFS= read -r _gd; do
        [ -n "$_gd" ] || continue
        lh_gap_detail+=("    $_gd")
      done < "$_mt_gaps_pending"
      _lh_cfg_gaps=$((_lh_cfg_gaps + 1))
    fi
    observe_ids+=("${_obs_id%.rs}")
    # The census subject is the UNION of every included target's module tree, deduped later
    # (a shared `common/mod.rs` is one site, not one per target that includes it).
    local _cf
    while IFS= read -r _cf; do
      [ -n "$_cf" ] || continue
      srcs="$srcs${_cf#$REPO_ROOT/}	$_cf
"
    done <<EOF
$_mt_closure
EOF
    # Two array elements per target (`--test <name>`), so ${#targets[@]} is NOT the
    # target count — $count is.
    targets+=(--test "$base")
    count=$((count + 1))
    names="$names $base"
    # `grep -cE` and a NUMERIC test, deliberately, NOT `| grep -qE`. Under `set -o
    # pipefail` (line 650) an early-exiting `grep -q` SIGPIPEs its upstream, so the
    # PIPELINE reports 141 — non-zero — on a SUCCESSFUL match. Negated by the `!` here,
    # that made a file WITH a surviving positive cfg site fall into `allow_zero`: the
    # target would then have been EXCUSED from the #2039 zero-tests guard, so a gated
    # target that executed nothing would NOT have failed the lane. That is the exact
    # vacuous excusal this derivation exists to make impossible, arriving through the
    # plumbing rather than the logic. MEASURED: with a matching site on line 1 of a
    # 200k-line file (so `grep -q` exits while `sed` is still writing), the old form
    # mis-classified 6/6 and this form 0/6. `grep -c` consumes all input, so there is no
    # early close and no SIGPIPE — the count is an AFFIRMATIVE measurement, which is what
    # a permissive branch must key on. Same defect class as #3380.
    # A MANIFEST-GATED target is POSITIVELY gated by definition and can NEVER be
    # allowed-zero (roborev round-8 finding, Medium): cargo runs it only when the feature
    # is on, and its source may carry no cfg site at all — so the polarity scan below finds
    # nothing and would have classified it negative-polarity-only, EXCUSING from the
    # zero-tests guard the very target round 7 added discovery for. The round-7 fix was
    # under-propagated: I threaded metadata discovery into the candidate loop and left the
    # classifier reasoning about source text alone.
    # THE THIRD STATE IS DECIDED BEFORE THE CHAIN, because `elif cmd; then` treats every non-zero
    # alike: exit 2 (could not tell) would have taken the same branch as exit 1 (no positive site)
    # and routed the target into allow_zero — the fail-open the function change closes, reintroduced
    # one line away from it. Precomputing keeps the chain two-valued, which is all it can express.
    _pol_rc=0
    if [ "$_mt_how" != "manifest" ]; then
      _lh_positive_in_closure "$_mt_closure" "$cfg_site" || _pol_rc=$?
      if [ "$_pol_rc" -ge 2 ]; then
        status=FAIL
        {
          echo "[$name] FAIL-CLOSED: the polarity scan could not read a file in the module closure"
          echo "        of test target '$_mt_name' (exit $_pol_rc). A failed scan reads as 'no"
          echo "        positive cfg site', which routes the target into allowed-zero — and an"
          echo "        allowed-zero target that IS positively gated can then run zero tests and"
          echo "        PASS. This is the one scan that must not guess."
        } | tee "$log"
        end=$(date +%s)
        record_result "$name" "$status" "$((end - start))"
        echo ">>> [$name] $status ($((end - start))s)"
        return 0
      fi
    fi
    if [ "$_mt_how" = "manifest" ]; then
      :
    # Polarity over the WHOLE module tree, not just the root (round 12): a positive gate in
    # a child module must stop this target being allowed-zero, or the target is EXCUSED from
    # the zero-tests guard on the strength of a file that happened to be silent.
    elif [ "$_pol_rc" -eq 0 ]; then
      :
    else
      # TWO DIFFERENT IDENTIFIERS FOR TWO DIFFERENT CONSUMERS, and conflating them was a
      # real bug (roborev round-9 finding, Medium). `--test <name>` takes cargo's TARGET
      # NAME, but check_no_unexpected_zero_tests parses `Running tests/<path>.rs` and keys
      # on the PATH stem. For a directory-style or explicitly-mapped target those differ —
      # target `foo` with `tests/foo/main.rs` yields `foo/main` — so an allowed-zero entry
      # spelled as the target name would never match, and a legitimately negative-polarity
      # target would FAIL the full gate. Derived from src_path so the two agree by
      # construction rather than by coincidence of naming.
      # The guard captures `Running tests/([^space]+)\.rs`, i.e. the path relative to
      # `tests/` — `foo` for tests/foo.rs, `foo/main` for the directory-style
      # tests/foo/main.rs. So strip that prefix from the package-relative path rather than
      # stripping a `tests/` substring out of the ABSOLUTE path (which left an absolute path
      # for an explicitly mapped target outside tests/, matching nothing — round-10 finding).
      # A target whose source is not under tests/ is invisible to that guard in any case,
      # since it never prints a `Running tests/...` line, so an entry for it simply never
      # matches — harmless, and not a false FAIL.
      # Mirrors the guard's two spellings exactly (see check_no_unexpected_zero_tests):
      # tests-relative under tests/, package-relative otherwise.
      local _az_id="$_mt_rel"
      case "$_mt_rel" in
        tests/*) _az_id="${_mt_rel#tests/}" ;;
      esac
      _az_id="${_az_id%.rs}"
      allow_zero+=("$_az_id")
      negonly="$negonly $_az_id"
    fi
  done <<EOF
$meta_targets
EOF
  if [ "$count" -eq 0 ]; then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: derived ZERO legacy-heuristics --test targets from"
      echo "        cargo metadata's test targets (cfg pattern: $cfg_site; or required-features)."
      echo "        The derivation, not the feature, is what failed — an unreadable or"
      echo "        moved tests dir, or a renamed feature. A lane with no subject has no"
      echo "        verdict to give, so this is a FAIL, never a PASS and never a SKIP."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # Provenance named accurately: candidates come from cargo metadata (which sees
  # manifest-gated and directory-style targets), and membership from each target's own
  # src_path or its required-features. Saying "from tests/*.rs" would misdescribe the
  # derivation in a lane whose subject is accurate declaration.
  echo ">>> [$name] derived${names} ($count target(s); candidates from cargo metadata, gated by cfg site or required-features)"
  # DECLARED, not dropped (roborev round-36): a target excluded because its required-features are
  # unmet is a COVERAGE GAP this lane must state, exactly as the flight lane states its census.
  # Silently omitting it would shrink the subject set with no trace.
  [ -n "$rf_unmet" ] && echo ">>> [$name] NOT invoked — cargo rejects an explicit --test whose required-features are unmet; reported as a coverage gap:$rf_unmet"
  [ -n "$negonly" ] && echo ">>> [$name] allowed-zero (NEGATIVE-polarity only — cfg(not(...)) bodies compile out here and run in core-tests):$negonly"

  # COVERAGE CENSUS — the co-required-feature gap (roborev round-4 finding, Medium).
  #
  # A test body gated `#[cfg(all(feature = "legacy-heuristics", feature = "X"))]` compiles
  # OUT when X is not enabled, and this lane enables only `default + legacy-heuristics`.
  # Those bodies are therefore NOT EXECUTED here — and the #2039 zero-tests guard cannot
  # see it, because sibling ungated tests in the same target keep its count nonzero. That
  # is the same invisible-omission shape this whole issue exists to remove (cf. the
  # required-features targets that print no `Running` line, and the observability-testing
  # targets of #3375), so the lane DECLARES it rather than leaving it to be discovered.
  #
  # DERIVED, not curated: the co-required feature names come from the committed cfg
  # attributes, and membership is tested against cargo's OWN resolved feature set for this
  # invocation — so if such a feature later becomes enabled here, it drops out of the
  # census automatically with no gate edit.
  #
  # ON THE ORACLE, corrected (roborev round-26, Low). This block used to say the resolved set is
  # WIDER than cqlite-core's `default + legacy-heuristics` because it includes
  # arrow/parquet/cli-helpers, attributing that to dev-dependency unification. Both halves are now
  # obsolete: round 6 replaced the oracle with a PACKAGE-SCOPED `cargo tree -p` resolve precisely
  # BECAUSE those extras were a workspace-resolution artifact — they are turned on by
  # cqlite-flight / cqlite-py / cqlite-node / ws0-corpus-gen, other members, measured 14 features
  # workspace-wide against 9 package-scoped. So the set printed below no longer contains them, and
  # the paragraph that explained why it did was describing the defect that was removed.
  #
  # THIS IS THE SECOND STALE ORACLE COMMENT THIS CHANGE HAS SHIPPED (round 20 fixed the header of
  # `_resolved_package_features` itself, which had survived the same round-6 fix). The pattern is
  # worth naming: when an oracle changes, its rationale is usually written in MORE THAN ONE place,
  # and the copies do not move with it — so the search after such a fix is for every paragraph that
  # ARGUES for the old behaviour, not just the one attached to the code.
  #
  # The direction still matters for THIS census specifically — it reports a GAP, so an over-broad
  # enabled set would UNDER-report gaps (the permissive direction), which is why the narrower
  # package-scoped resolve is the correct oracle here and not merely the tidier one.
  #
  # Deliberately NOT done in this change: a second `--features legacy-heuristics,experimental`
  # pass that would actually execute them. It is a third full compile of cqlite-core at a
  # third feature set — measured cost for the existing single pass is ~292s — and the
  # general "experimental executes nowhere" hole is #3373's subject, not this lane's.
  local coreq="" coreq_n=0 f_
  # THE CENSUS SUBJECT MUST COVER WHAT THE LANE EXECUTES, and `--lib` was missing from it
  # (roborev round-9 finding, Medium). The lane runs cqlite-core's inline unit tests, so an
  # inline `#[cfg(all(feature = "legacy-heuristics", feature = "X"))]` test in
  # cqlite-core/src/** compiles out here exactly like a gated integration body — but the
  # census only looked at integration-target roots and would therefore have reported "every
  # gated body is reachable" while one was silently absent. A FALSE ZERO-GAP, the silent
  # direction. The aggregate non-zero unit-test guard cannot see it either: 3478 sibling
  # unit tests keep the count nonzero. Same shape as the guard-subject findings of rounds 7
  # and 8 — the subject was narrower than the claim.
  #
  # Pre-filtered with grep so the awk pass runs only over files that mention the feature
  # (8 of cqlite-core/src's files today, none of them co-required).
  local _libsrc
  # The scan's EXIT STATUS is checked, and "no matches" is distinguished from a READ ERROR
  # (roborev round-37, Low). `grep -rl … 2>/dev/null` swallowed both: an unreadable directory or a
  # partial walk produced an empty list, and an empty list was reported as a CLEAN ZERO-GAP census.
  # grep exits 0 = matches, 1 = none, >=2 = error; only >=2 is fatal, and it FAILs the lane naming
  # the scan rather than reporting a census nobody took.
  local _libsrc_list _libsrc_rc=0
  _libsrc_list=$(grep -rlE 'feature[[:space:]]*=[[:space:]]*"legacy-heuristics"' \
    "$REPO_ROOT/cqlite-core/src" 2>/dev/null | sort) || _libsrc_rc=$?
  if [ "$_libsrc_rc" -ge 2 ]; then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: the library census scan of cqlite-core/src FAILED (grep exit"
      echo "        $_libsrc_rc — an unreadable directory or a partial walk), so the co-required"
      echo "        census would have been taken over an INCOMPLETE source set and reported as a"
      echo "        clean zero gap. A census that could not be taken is never reported as empty."
    } | tee -a "$log"
    end=$(date +%s); record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"; return 0
  fi
  while IFS= read -r _libsrc; do
    [ -n "$_libsrc" ] || continue
    srcs="$srcs${_libsrc#$REPO_ROOT/}	$_libsrc
"
  done <<EOF
$_libsrc_list
EOF
  # Dedupe: one file, one scan, one site list — a helper module shared by several targets
  # must not be reported once per including target.
  srcs=$(printf '%s' "$srcs" | sort -u)
  local lh_sites=0 lh_skip=0 _sites _k _ln _ms _m f_ f_src lh_where=""
  while IFS="	" read -r f_ f_src; do
    [ -n "$f_" ] || continue
    # cargo's OWN src_path, not a reconstructed `tests/<name>.rs` (roborev round-8
    # finding, Low): a directory-style or explicitly-mapped `[[test]]` target does not live
    # at that reconstructed path, so the census silently skipped it while the lane claims
    # to fail closed. And an UNREADABLE included source is a FAIL, never a `continue` — a
    # census that cannot read its subject is unmeasurable, not empty.
    if [ ! -r "$f_src" ]; then
      status=FAIL
      {
        echo "[$name] FAIL-CLOSED: the co-required-feature census could not read the source of"
        echo "        an INCLUDED target: $f_ ($f_src). A census that cannot read its subject is"
        echo "        unmeasurable, and unmeasurable is never reported as empty."
      } | tee "$log"
      end=$(date +%s)
      record_result "$name" "$status" "$((end - start))"
      echo ">>> [$name] $status ($((end - start))s)"
      return 0
    fi
    # A census that CANNOT be taken is never reported as empty (the lane's standing rule):
    # a failed derivation FAILs and names the derivation.
    if ! _sites=$(_legacy_coreq_sites "$f_src" "$lh_enabled"); then
      status=FAIL
      {
        echo "[$name] FAIL-CLOSED: the co-required-feature census could not be derived from"
        echo "        $f_src. The derivation, not the feature, is what failed."
        echo "        A census that cannot be taken is not reported as empty."
      } | tee "$log"
      end=$(date +%s)
      record_result "$name" "$status" "$((end - start))"
      echo ">>> [$name] $status ($((end - start))s)"
      return 0
    fi
    while IFS=$'\t' read -r _k _ln _ms; do
      [ -n "$_k" ] || continue
      case "$_k" in
        site) lh_sites=$((lh_sites + 1)); lh_where="$lh_where $f_:$_ln" ;;
        skip) lh_skip=$((lh_skip + 1)) ;;
      esac
      for _m in $(echo "$_ms" | tr ',' ' '); do
        case " $coreq " in *" $_m "*) ;; *) coreq="$coreq $_m" ;; esac
      done
    done <<EOF
$_sites
EOF
  done <<EOF
$srcs
EOF
  coreq_n=$lh_sites
  local -a lh_census=()
  # THE SAME TWO DECLARATIONS, INTO THE COMPONENT LOG (found by probing the job-111 fix, which is
  # the only reason it surfaced). Both are echoed to stdout above, and stdout-only was the whole of
  # job 108's Low finding: the census write below opens "$log" with `>`, so anything appended before
  # it is TRUNCATED, and a reader inspecting the component log saw neither declaration. The comment
  # beside those echoes already states the principle in this file — a coverage gap this lane must
  # state — and they were stating it only where nobody reads. Routed through the census array so
  # they land AFTER the header, exactly as the cfg-gated-subtree detail is.
  [ -n "$rf_unmet" ] && lh_census+=("NOT invoked (required-features unmet) — DECLARED coverage gap:$rf_unmet")
  [ -n "$negonly" ] && lh_census+=("allowed-zero (NEGATIVE-polarity cfg(not(...)) only):$negonly")
  lh_census+=("polarity: ${#allow_zero[@]} of $count target(s) excusable (allowed-zero). A target is")
  lh_census+=("  excusable ONLY when EVERY legacy-heuristics cfg site in its module closure is the")
  lh_census+=("  recognised direct-negative attribute #[cfg(not(feature = \"legacy-heuristics\"))].")
  lh_census+=("  Any other shape — nested, compound, multiline, unfamiliar — is NOT excused, so an")
  lh_census+=("  unrecognised expression costs a target its excusal and never costs a zero-tests")
  lh_census+=("  check. Per-target reasons are on stdout as POLARITY-UNRECOGNISED.")
  if [ "$lh_sites" -gt 0 ] || [ "$lh_skip" -gt 0 ]; then
    lh_census+=("COVERAGE CENSUS — WHAT THIS LANE DOES NOT EXECUTE:")
    if [ "$lh_sites" -gt 0 ]; then
      local _sw="sites"; [ "$lh_sites" -eq 1 ] && _sw="site"
      # SITES, not bodies. The count of gated test bodies is deliberately NOT claimed: one
      # site can gate a whole module, and counting bodies needs a Rust parser (see
      # _legacy_coreq_sites for the descope and the four review rounds behind it).
      lh_census+=("  $lh_sites legacy-heuristics-gated cfg $_sw ALSO require$coreq, which this")
      lh_census+=("  lane does NOT enable — so whatever each one gates (a test, a module, an")
      lh_census+=("  import) does NOT execute here. Sites, not bodies: one site can gate a whole")
      lh_census+=("  module, so a body count would be a guess.")
      lh_census+=("  The #2039 zero-tests guard CANNOT detect this: sibling ungated tests in the")
      lh_census+=("  same target keep its count nonzero.")
      lh_census+=("  Tracked by #3373 (experimental-gated tests execute in NO lane at all).")
      lh_census+=("  where:$lh_where")
      lh_census+=("  This census is NON-EXHAUSTIVE (#3472): these are the sites it RECOGNISED, not")
      lh_census+=("  necessarily all that exist — a cfg or declaration shape it does not model is")
      lh_census+=("  invisible to it and is absent from the list above, not marked.")
    fi
    if [ "$lh_skip" -gt 0 ]; then
      lh_census+=("  $lh_skip further co-required site(s) use a Boolean shape this census does not")
      lh_census+=("  evaluate (not(...) / any(...) / cfg_attr) and are NOT counted above — a token")
      lh_census+=("  list cannot tell a conjunction from a disjunction, and any(...) IS reachable")
      lh_census+=("  here. Reported rather than guessed: a named unknown beats a wrong entry.")
    fi
  else
    lh_census+=("co-required-feature census: 0 RECOGNISED — no recognised legacy-heuristics-gated cfg")
    lh_census+=("  site requires a feature this lane omits. The scan is NON-EXHAUSTIVE (#3472): this is")
    lh_census+=("  evidence that none was RECOGNISED, never evidence that none is THERE.")
  fi
  # DECLARED alongside the co-required census, because it is the same kind of blind spot: a
  # subtree whose reachability was not evaluated cannot be counted either way, and the census
  # claiming "0 gaps" over it would be the silent direction (roborev root pass at aabae56ea).
  if [ "$_lh_cfg_gaps" -gt 0 ]; then
    lh_census+=("cfg-gated-subtree gaps: $_lh_cfg_gaps subject target(s) reach a child module through")
    lh_census+=("  a cfg this scan does not evaluate — their legacy coverage is UNCLASSIFIED:")
    lh_census+=("  NON-EXHAUSTIVE (#3472): these are the gaps it RECOGNISED, not necessarily all.")
    # the DETAIL goes in the census itself, so it lands in the component log rather than being
    # truncated out of it by the `>` below (roborev job 97, Low). "listed above" has to be true.
    for _gd in "${lh_gap_detail[@]:-}"; do
      [ -n "$_gd" ] || continue
      lh_census+=("  $_gd")
    done
  else
    lh_census+=("cfg-gated-subtree gaps: 0 RECOGNISED — every module the scan RECOGNISED is reached")
    lh_census+=("  unconditionally; a declaration form it does not recognise is invisible to it (#3472).")
  fi
  lh_census+=("enabled features (cargo tree -p, package-scoped):$lh_enabled")
  local _cl
  for _cl in "${lh_census[@]}"; do echo ">>> [$name] $_cl"; done
  # The log OPENS with the census (`>` here; the cargo build below switches to `>>`), so
  # the omission is in the component log on every run, pass or fail — the same contract
  # the flight-tests lane uses. A gap that only appears on stdout is a gap nobody reads.
  {
    echo "==== [$name] COVERAGE CENSUS (issue #1699 / #3373) ===="
    for _cl in "${lh_census[@]}"; do echo "$_cl"; done
    echo "==== end census ===="
  } > "$log"
  echo ">>> [$name] RUSTFLAGS=-D warnings cargo build -p cqlite-core --features legacy-heuristics, then cargo test --no-fail-fast --lib + derived targets (#1699)"
  # --no-fail-fast is load-bearing for THIS lane specifically. cargo test stops after the
  # first failing test BINARY, and this lane is the first thing ever to execute these
  # targets — so fail-fast reports one target's failures, hides the rest, and turns
  # triage into a serial reveal (measured: run 1 showed only P0_4_modern_format_rejection,
  # run 2 then showed 3 more in sstable_discovery_comprehensive). A lane whose purpose is
  # to surface never-executed rot must surface ALL of it in one run.
  # RUSTFLAGS="-D warnings" MUST cover the cargo test compile too (roborev round-3
  # finding, Medium). A `RUSTFLAGS=... cargo build && cargo test` chain applies it to the
  # BUILD ONLY, and `cargo test` then recompiles the lib's cfg(test) code plus the
  # selected --test targets WITHOUT warnings-as-errors — so the warning-class defect this
  # lane exists to catch at this feature set (#1981's dead-code shape: a cfg(test) helper
  # whose only caller is gated out) would have passed silently in exactly the half of the
  # lane that compiles test code. `env` both invocations so neither half is unguarded.
  # The `--lib` half needs its OWN guard (roborev round-8 finding, Medium). The existing
  # zero-tests guard keys on `Running tests/<name>.rs` — integration targets only — so the
  # library unit suite could execute ZERO tests, or `--lib` could be dropped from the
  # invocation entirely, and the lane would stay green on its integration targets alone.
  # cqlite-core's inline `#[cfg(feature = "legacy-heuristics")]` unit tests live exactly
  # there (3478 tests observed at this feature set), and they are half of what this lane
  # claims to execute. Derived, not hard-coded, for the same reason as the flight lane's.
  local -a lh_unit_srcs=()
  local _lh_us
  while IFS= read -r _lh_us; do
    [ -n "$_lh_us" ] && lh_unit_srcs+=("$_lh_us")
  done <<EOF
$(_package_unittest_srcs cqlite-core lib "$lh_enabled")
EOF
  if [ "${#lh_unit_srcs[@]}" -eq 0 ]; then
    status=FAIL
    {
      echo "[$name] FAIL-CLOSED: could not derive cqlite-core's lib unittest target from cargo"
      echo "        metadata, so the --lib half of this lane would run under NO zero-test guard."
    } | tee "$log"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # Both halves go through _deny_warnings, which is what makes `-D warnings` real: a bare
  # `env RUSTFLAGS=...` is silently ignored when CARGO_ENCODED_RUSTFLAGS is set (round-5).
  if _deny_warnings cargo build --package cqlite-core --features legacy-heuristics >>"$log" 2>&1 \
      && _deny_warnings env CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" CARGO_TERM_COLOR=never \
        cargo test --no-fail-fast --package cqlite-core --features legacy-heuristics --lib "${targets[@]}" >>"$log" 2>&1; then
    # Green cargo exit is not sufficient — see the guard's own doc block.
    # The guard writes its verdict to stderr only, so `2>>` lands the message in the
    # component log (where the FAIL branch below tails it) while the `if` still tests
    # the GUARD's exit status directly — not a pipeline's last stage.
    # BOTH halves are guarded: check_no_unexpected_zero_tests covers the integration
    # targets, check_unittest_targets_ran covers the `--lib` unit suite. Either alone
    # leaves half of this lane able to execute nothing while the lane reports PASS
    # (roborev round-8 finding, Medium): the existing guard keys on `Running
    # tests/<name>.rs`, so a zero-test lib suite — or `--lib` dropped from the invocation
    # entirely — was invisible to it, and cqlite-core's inline legacy-gated unit tests
    # (3478 observed at this feature set) are half of what this lane claims to execute.
    if check_no_unexpected_zero_tests "$name" "$log" \
        ${allow_zero[@]+"${allow_zero[@]}"} 2>>"$log" \
        && check_test_targets_observed "$name" "$log" ${observe_ids[@]+"${observe_ids[@]}"} 2>>"$log" \
        && check_unittest_targets_ran "$name" "$log" "${lh_unit_srcs[@]}" 2>>"$log"; then
      echo ">>> [$name] zero-test guards: integration targets + unit suite (derived): ${lh_unit_srcs[*]}"
      status=PASS
    else
      status=FAIL
    fi
  else
    status=FAIL
  fi
  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# run_feature_iso <feature>: ONE isolation lane, parameterized by the feature under
# test (issue #1699). Two dispatch arms consume it — feature-iso-parquet and
# feature-iso-delta-scan — so the rationale lives once, here, and the two lanes can
# never drift apart.
#
# WHY these lanes exist: run_clippy's cqlite-core arm enables legacy-heuristics,
# parquet AND delta-scan together with ~30 more features. That combined shape is
# exactly what MASKS cross-feature coupling — a parquet-gated item that accidentally
# references a delta-scan-gated item compiles fine whenever both features are on, so
# no existing component can see it. Each lane therefore enables exactly ONE of the two
# and asserts the code still compiles without the other. Never --all-features here:
# isolation is the entire point.
#
# `all-compression` stays in both lanes because it is in `default`; dropping it would
# change what the lane measures from FEATURE ISOLATION to NO-COMPRESSION support,
# which is a different (and already covered) question — minimal-build owns that one.
#
# COMPILING TEST CODE IS LOAD-BEARING, but `--all-targets` was the wrong instrument.
# The argument STANDS: the #1978 incident class is an ungated `#[cfg(test)]` module
# referencing a feature-gated item, and a library-only `cargo check` never compiles
# `cfg(test)` code at all — so a bare `cargo check` would compile the library, go
# green, and miss the very incident class this lane cites.
#
# But that incident class lives in `cqlite-core/src/**`'s INLINE `#[cfg(test)]`
# modules, and `--all-targets` reaches far past them: it also compiles cqlite-core's
# ~100 INTEGRATION test files, which are written against the DEFAULT feature set and
# therefore fail here on modules this lane deliberately configures out. Measured:
# issue_1004_primitive_codec_vectors.rs:23 (`storage::serialization`),
# issue_2412_wraparound_scan.rs:42 (`storage::write_engine`),
# contract_stability_tests.rs:23 (`cqlite_core::query`). Those are NOISE — the
# integration suite assuming default features — not cross-feature leakage, which is
# what this lane exists to measure.
#
# The correct instrument is `cargo test --lib --no-run`: it compiles the lib WITH its
# `cfg(test)` modules (the incident class) and pulls in NO integration test target.
# minimal-build is the precedent — it uses this exact shape for exactly this reason.
# Do NOT "simplify" this to `cargo check --lib`: that does not compile `cfg(test)` and
# is blind to #1978.
#
# RUSTFLAGS=-D warnings is load-bearing for the same reason minimal-build sets it
# (#1981): a feature-orphaned helper (a `#[cfg(test)]` helper whose only caller is
# gated out at this feature set) surfaces as a DEAD-CODE WARNING, and a lane without
# -D warnings demotes that to a line nobody reads.
#
# `--no-run` rather than executing keeps the cost proportionate to the purpose: the
# question is "does it still compile in isolation", not "do the tests pass" (core-tests
# owns that, at the default feature set).
#
# No opt-out env var: a committed feature is never legitimately absent.
# _deny_warnings — run a cargo invocation with `-D warnings` ACTUALLY in effect.
#
# roborev round-5 finding (Medium): `env RUSTFLAGS="-D warnings" cargo …` is SILENTLY
# INERT whenever CARGO_ENCODED_RUSTFLAGS is present in the environment, because cargo
# reads the ENCODED variable first and ignores RUSTFLAGS entirely when it is set. Every
# lane in this issue exists to stop a guard that looks enforced from enforcing nothing,
# so a warnings-as-errors guard an inherited env var can switch off is precisely the
# defect class — and this file already knows the precedence rule (the managed-config
# check near the top tests BOTH variables for exactly this reason).
#
# When the encoded form is present it is APPENDED to, not replaced: dropping an
# operator's flags would trade one silent behaviour change for another, and the
# question here is only whether `-D warnings` is added. cargo's element separator is
# US (\x1f), and `-D warnings` is TWO elements (the space-split form RUSTFLAGS would
# have produced). The append is announced on stderr — which every caller redirects into
# its component log — so a non-default flag environment is visible rather than assumed.
#
# The plain branch UNSETS the encoded variable rather than leaving it: an empty-but-set
# value is still "present" to cargo and would suppress RUSTFLAGS, which is the same
# vacuous outcome by a quieter route.
_deny_warnings() {
  # REFUSE inherited lint controls that DEFEAT the appended `-D warnings` (roborev round-35,
  # Medium). `-D warnings` going last wins over another `-D`/`-W`, but it does NOT win over:
  #   --cap-lints allow      caps every lint below deny, so nothing can become an error
  #   --force-warn <spec>    forces the lint back to a warning regardless of later -D
  # Either one makes these lanes' entire warning-class guard SILENTLY INERT while their SUMMARY
  # line stays green — the #1981 defect reintroduced through the environment instead of the code.
  # A guard that can be switched off by an inherited variable is not a guard, and detecting it is
  # cheap, so this fails closed and names the offending flag rather than compiling anyway.
  local _dw_all="${RUSTFLAGS:-} ${CARGO_ENCODED_RUSTFLAGS:-}"
  case "$_dw_all" in
    *--cap-lints*|*--force-warn*)
      echo "[deny-warnings] FAIL-CLOSED: the inherited lint flags contain --cap-lints or" >&2
      echo "[deny-warnings] --force-warn, either of which prevents '-D warnings' from making a" >&2
      echo "[deny-warnings] warning an error — so this lane's warning guard would be inert while" >&2
      echo "[deny-warnings] reporting PASS. Unset them (RUSTFLAGS / CARGO_ENCODED_RUSTFLAGS) and" >&2
      echo "[deny-warnings] re-run; the guard is refusing rather than compiling something it" >&2
      echo "[deny-warnings] cannot vouch for (issue #1699, roborev round-35)." >&2
      return 1
      ;;
  esac
  if [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]; then
    local _us
    _us=$(printf '\037')
    echo "[deny-warnings] CARGO_ENCODED_RUSTFLAGS is set and takes precedence over RUSTFLAGS;" >&2
    echo "[deny-warnings] appending -D warnings to it so the guard is not silently inert." >&2
    CARGO_ENCODED_RUSTFLAGS="${CARGO_ENCODED_RUSTFLAGS}${_us}-D${_us}warnings" "$@"
  else
    # APPEND, never replace (roborev round-12 finding, Medium). The encoded branch above
    # preserves the operator's flags, and this branch replacing them was an indefensible
    # asymmetry: it would silently drop target, sanitizer or codegen flags for THESE LANES
    # ONLY, so the lanes would compile something subtly different from every other component
    # in the same run. `-D warnings` goes last so it wins on conflict.
    env -u CARGO_ENCODED_RUSTFLAGS RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-D warnings" "$@"
  fi
}

run_feature_iso() { # run_feature_iso <feature>
  _deny_warnings cargo test --package cqlite-core \
    --no-default-features --features "all-compression,$1" --lib --no-run
}

# parity-report: verify the committed derived parity report is not stale vs its
# source manifest (issue #1338). Renders test-data/cassandra-parity-manifest.yml
# with `cassandra-parity report --check`; PASS when the committed report matches a
# fresh render, FAIL (naming docs/reports/cassandra-test-parity.md) when it drifts.
# This catches the single-PR "edited the manifest, forgot to regenerate" case at
# the local gate, before push — the layer the post-merge self-healing job cannot
# cover. SKIP-aware like delivery-telemetry/python-bindings: when the
# cassandra-parity crate (tools/cassandra-parity) or the manifest is absent (a
# minimal checkout), it records SKIP (loud, never silent PASS) rather than FAIL.
# The manifest source and the tool-crate dir resolve to their repo defaults but are
# overridable (PARITY_REPORT_MANIFEST / PARITY_REPORT_TOOL_DIR) so the component is
# self-testable without mutating the committed tree; the --output target is always
# the canonical committed report (read-only under --check) so a failure always
# names that file. No Docker, no datasets — reads the manifest + committed report.
run_parity_report() {
  local name=parity-report
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local manifest="${PARITY_REPORT_MANIFEST:-test-data/cassandra-parity-manifest.yml}"
  local tool_dir="${PARITY_REPORT_TOOL_DIR:-tools/cassandra-parity}"
  local report="docs/reports/cassandra-test-parity.md"
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if [ ! -f "$manifest" ] || [ ! -d "$tool_dir" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (cassandra-parity tool or manifest unavailable: manifest=$manifest tool=$tool_dir)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] cargo run -q -p cassandra-parity -- report --check ($report)"
  if cargo run -q -p cassandra-parity -- report \
       --manifest "$manifest" --output "$report" --check >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    # A nonzero --check exit is either a genuine render mismatch (the tool prints
    # "report: STALE — ...") or an invalid manifest (lint errors bail before any
    # render). Only the former is fixed by regenerating; mirror the CI heal job's
    # distinction so the advice is not misleading. grep on the captured $log is
    # injection/quoting-safe (fixed pattern, no interpolation).
    if grep -q 'STALE' "$log"; then
      echo "--- [$name] FAILED: $report is STALE vs the manifest."
      echo "    Regenerate: cargo run -p cassandra-parity -- report --manifest $manifest --output $report"
    else
      echo "--- [$name] FAILED: cannot render $report — the manifest is invalid."
      echo "    Fix the manifest lint/validity error before regenerating: $manifest"
    fi
    echo "--- last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# operator-metrics-doc: verify the committed operator-facing Flight metrics
# reference (docs/reports/flight-metrics-reference.md) is not stale vs the
# observability catalog (issue #2426). Renders it via the always-compiled catalog
# with the cqlite-core `gen_operator_metrics_doc` example in --check mode; PASS
# when the committed doc matches a fresh render, FAIL (naming the doc) when it
# drifts. Mirrors the #1338 parity-report pattern: catches the "added/renamed a
# metric or annotation, forgot to regenerate the field-team doc" case at the
# local gate. SKIP-aware: when cqlite-core (the example's crate) is absent (a
# minimal checkout), it records SKIP (loud, never a silent PASS). No Docker, no
# datasets — reads the catalog + committed doc only. The example resolves the doc
# at its canonical committed path (repo-root-relative), read-only under --check,
# so a failure always names that file.
run_operator_metrics_doc() {
  local name=operator-metrics-doc
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local doc="docs/reports/flight-metrics-reference.md"
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if [ ! -d "$REPO_ROOT/cqlite-core" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (cqlite-core unavailable)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] cargo run -q -p cqlite-core --example gen_operator_metrics_doc -- --check ($doc)"
  if cargo run -q -p cqlite-core --example gen_operator_metrics_doc -- --check >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    # A nonzero --check exit is either a STALE committed doc (regenerate) or a
    # fail-closed generation error (a catalogued metric lacking an operator
    # annotation). grep on the captured $log is injection/quoting-safe.
    if grep -q 'STALE' "$log"; then
      # Name the artifact(s) that ACTUALLY drifted — either the committed report,
      # the published website page, or both — rather than always blaming the
      # report. The example prints `STALE — <path> …` per drifted artifact.
      local drifted
      drifted=$(grep 'STALE' "$log" | grep -oE '[[:graph:]]+\.md' | sort -u | tr '\n' ' ')
      [ -n "$drifted" ] || drifted="$doc"
      echo "--- [$name] FAILED: the following artifact(s) are STALE vs the observability catalog: $drifted"
      echo "    Regenerate: cargo run -p cqlite-core --example gen_operator_metrics_doc"
    else
      echo "--- [$name] FAILED: could not render $doc — a catalogued metric is missing its operator annotation."
      echo "    Add the annotation in cqlite-core/src/observability/operator_docs_annotations.rs, then regenerate."
    fi
    echo "--- last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# kit-dashboard-drift: verify the kit Grafana dashboard
# (easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json) references only
# `cqlite.*` metric names that exist in the observability catalog (issue #2427).
# Runs the cqlite-core `kit_dashboard_metric_drift` test: it parses the dashboard
# JSON, extracts every dotted `cqlite.*` token from panel targets/exprs/titles,
# and asserts each is an exact catalog metric name, a bounded attribute key, or a
# real metric's namespace prefix — FAILing (naming the phantom name) on a
# renamed/removed/typo'd metric. Mirrors the #2426 operator-metrics-doc anti-drift
# component (a committed artifact cross-checked against catalog::ALL_METRICS).
# SKIP-aware (loud, never silent PASS): SKIPs ONLY when cqlite-core or the whole
# kit subtree (easy-db-lab-kits/cqlite-flight/) is absent (a genuine sparse/minimal
# checkout). If the kit subtree IS present but the expected dashboard JSON is
# missing/renamed, that is real drift/breakage in a complete checkout → the test
# FAILs (roborev #2427 r2). No Docker, no datasets — reads the catalog + JSON.
run_kit_dashboard_drift() {
  local name=kit-dashboard-drift
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local kit_root="easy-db-lab-kits/cqlite-flight"
  local dashboard="$kit_root/dashboards/cqlite-flight.json"
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  # Skip ONLY on a genuine sparse checkout: cqlite-core or the whole kit subtree
  # absent. A present kit subtree with a missing dashboard is NOT a skip — it falls
  # through to the test, which FAILs loudly (present-kit + missing-dashboard drift).
  if [ ! -d "$REPO_ROOT/cqlite-core" ] || [ ! -d "$REPO_ROOT/$kit_root" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (cqlite-core or the kit subtree $kit_root is absent — sparse checkout)"
    record_result "$name" "$status" 0
    return 0
  fi
  if [ ! -f "$REPO_ROOT/$dashboard" ]; then
    status=FAIL
    echo ">>> [$name] FAIL: kit subtree $kit_root IS present but the expected dashboard"
    echo "    $dashboard is MISSING (deleted/renamed) — drift/breakage in a complete checkout,"
    echo "    not a sparse checkout. Restore the dashboard or update the drift test's path."
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] cargo test -p cqlite-core --test kit_dashboard_metric_drift ($dashboard)"
  if cargo test -q -p cqlite-core --test kit_dashboard_metric_drift >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED: the kit dashboard references a cqlite.* metric name ABSENT from"
    echo "    catalog::ALL_METRICS (renamed/removed/typo'd), or the dashboard JSON is malformed."
    echo "    Fix the dashboard $dashboard or reconcile the name with the catalog."
    echo "--- last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# pub-surface: the CRATE-ROOT DECLARATION-CONSISTENCY guard for cqlite-core (issue
# #1712, epic #1688). scripts/ci/check-pub-surface.sh asserts that the crate root
# TELLS THE TRUTH about the modules it declares: an unconditional,
# non-`#[doc(hidden)]` top-level `pub mod NAME;` must not be gated by an inner
# `#![cfg(...)]` inside NAME's own file. The defect that motivated it:
# `pub mod benchmarks;` read as shipped public API for months while an inner
# `#![cfg(feature = "benchmarks")]` hidden inside benchmarks/mod.rs configured it out
# of every default build — the declaration site said one thing, the module's own file
# said another, and nothing could tell the difference. BOTH FACTS ARE IN THE SOURCE
# and each is read from a BOUNDED input: the declaration's attributes structurally
# from lib.rs, and the module file's PROLOGUE (which rustc guarantees holds every
# inner attribute the module has).
# THIS IS NOT PUBLIC-API DRIFT DETECTION. There is no snapshot and no `--regenerate`:
# the rustdoc-derived snapshot half was REMOVED deliberately (#1712 descope — five
# review findings were all one class, an unbounded scanner that cannot abstain; the
# principled route to drift detection is #3366). A green here says nothing about
# whether cqlite-core's public API changed.
# Fail-closed and affirmative: an unparseable crate root, an unrecognised `pub mod`
# shape, zero declarations, zero unconditional declarations, a module file resolving
# to neither or both of its legal paths, an unreadable module file, a block comment in
# a prologue or an inner attribute it cannot classify are each a NAMED FAIL, never
# "nothing measured, PASS", and there is no env opt-out. THIS COMPONENT holds the same
# line for the guard itself (#1712 r6 F2): PASS requires the guard's affirmative
# measurement line, not merely a zero exit, so an early `return 0` inside the guard is
# a NAMED FAIL here instead of a vacuous green. Source-only — no cargo at all,
# sub-second, offline, no datasets/network. SKIP-aware (loud, never a silent PASS):
# SKIPs only when cqlite-core is genuinely absent (a sparse checkout). Its own
# self-test lives in tooling-tests.
run_pub_surface() {
  local name=pub-surface
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local guard="scripts/ci/check-pub-surface.sh"
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if [ ! -d "$REPO_ROOT/cqlite-core" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (cqlite-core is absent — sparse checkout)"
    record_result "$name" "$status" 0
    return 0
  fi
  if [ ! -f "$REPO_ROOT/$guard" ]; then
    status=FAIL
    echo ">>> [$name] FAIL: cqlite-core IS present but the guard $guard is MISSING"
    echo "    (deleted/renamed) — that is breakage in a complete checkout, not a sparse"
    echo "    checkout. Restore the guard or update this component's path."
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] bash $guard"
  if bash "$REPO_ROOT/$guard" >"$log" 2>&1; then
    # AFFIRMATIVE MEASUREMENT, not "no error observed" (issue #1712 r6 F2). A zero
    # exit is only HALF the verdict: the guard must also have PRINTED what it
    # measured — `pub-surface: N crate-root declarations scanned … (M pub mod, of
    # which K unconditional); K module-file prologues read from source; 0
    # inconsistent`. Keying PASS on the exit status alone (and echoing
    # that line with `|| true`) meant an accidental early `return 0` anywhere inside
    # the guard reported `pub-surface: PASS` while NOTHING had been enumerated. That
    # is CLAUDE.md's named rule — never derive a pass from the ABSENCE of a bad
    # signal; a positive verdict requires an affirmative measurement — and it matters
    # most HERE, because this is the component that certifies the guard that
    # certifies cqlite-core's public API, so a vacuous PASS here silently unguards
    # every semver decision downstream. The permissive branch is therefore keyed on
    # the AFFIRMATIVE value (the line is present), never on the absence of an error.
    # MATCH THE WHOLE SUCCESS LINE, NOT ITS PREFIX (roborev r7 finding 3). The first
    # version of this check accepted any line starting `pub-surface: `, which is the
    # SAME vacuous-pass shape one level down: a guard printing `pub-surface: starting`
    # and then exiting 0 satisfied it and was recorded PASS. A check against a PREFIX
    # tests a SPELLING; this one tests the STATE, by requiring every element the guard
    # only knows AFTER it has enumerated the crate-root declarations and READ that
    # many module files off disk — and requiring the two load-bearing counts to be
    # NONZERO (`[1-9][0-9]*`), because "0 unconditional declarations, 0 prologues
    # read" is the vacuous measurement itself, and `0 inconsistent` literally.
    # KEPT IN SYNC BY HAND with the guard's own success line and with case 26(b)'s
    # positive control in scripts/tests/test_pub_surface_guard.sh — a wording change
    # must land in all three at once (#1712 descope).
    local measured
    measured="$(grep -m1 -E '^pub-surface: [0-9]+ crate-root declarations scanned in cqlite-core/src/lib\.rs \([0-9]+ pub mod, of which [1-9][0-9]* unconditional\); [1-9][0-9]* module-file prologues read from source; 0 inconsistent$' "$log" || true)"
    # THE SHAPE IS NOT ENOUGH — the COUNTS MUST COHERE (roborev r9 F4). The regex above
    # pins the wording and forbids a zero, but on its own it also accepts arithmetically
    # IMPOSSIBLE lines: `14 unconditional; 1 module-file prologues read` (13 declarations
    # silently unexamined), or `0 pub mod, of which 5 unconditional`. The guard itself
    # asserts prologues == unconditional, so such a line means the guard is not the
    # program that produced it — a stub, a truncation, or a stale build. This component
    # exists to be INDEPENDENT of the guard, so it re-derives the relationships here
    # rather than trusting them: decls >= mods >= uncond > 0, and prologues == uncond.
    if [ -n "$measured" ]; then
      local ps_d ps_m ps_u ps_p
      ps_d="$(printf '%s' "$measured" | sed -E 's/^pub-surface: ([0-9]+) crate-root.*/\1/')"
      ps_m="$(printf '%s' "$measured" | sed -E 's/.*\(([0-9]+) pub mod.*/\1/')"
      ps_u="$(printf '%s' "$measured" | sed -E 's/.*of which ([0-9]+) unconditional.*/\1/')"
      ps_p="$(printf '%s' "$measured" | sed -E 's/.*; ([0-9]+) module-file prologues.*/\1/')"
      if ! { [ "$ps_d" -ge "$ps_m" ] && [ "$ps_m" -ge "$ps_u" ] && [ "$ps_u" -gt 0 ] && [ "$ps_p" -eq "$ps_u" ]; }; then
        echo "❌ [$name] the pub-surface measurement line is ARITHMETICALLY INCOHERENT:" >&2
        echo "    $measured" >&2
        echo "    require: declarations($ps_d) >= pub mod($ps_m) >= unconditional($ps_u) > 0, and prologues($ps_p) == unconditional($ps_u)." >&2
        echo "    A line matching the wording but not the arithmetic did not come from the guard" >&2
        echo "    (stub, truncation or stale build). Refusing to record PASS." >&2
        measured=""
      fi
    fi
    if [ -n "$measured" ]; then
      status=PASS
      # Echo it so a pasted gate log shows the check RAN over a real surface.
      echo "$measured"
    else
      status=FAIL
      echo "--- [$name] FAILED: the guard exited 0 but printed NO affirmative measurement"
      echo "    line (\`pub-surface: <N> crate-root declarations scanned in … (<M> pub mod, of"
      echo "    which <K> unconditional); <K> module-file prologues read from source; 0"
      echo "    inconsistent\`), so NOTHING was measured and this is NOT a PASS"
      echo "    (issue #1712). A zero exit with no measurement is an early return inside"
      echo "    $guard — a real defect in the guard, not a formatting slip; fix the guard"
      echo "    (or, if its success wording moved, update BOTH it and this component)."
      echo "--- last 60 lines of $log ---"
      tail -60 "$log"
      echo "--- end of $name output ---"
    fi
  else
    status=FAIL
    echo "--- [$name] FAILED: a crate-root \`pub mod\` in cqlite-core/src/lib.rs is"
    echo "    advertised unconditionally while its own module file gates it with an inner"
    echo '    `#![cfg(...)]`, or the guard REFUSED over input it cannot classify (issue'
    echo "    #1712). The diagnostic below names the file and line."
    echo "    If the module really is conditional, HOIST the gate to the declaration site"
    echo '    (`#[cfg(feature = "...")] pub mod NAME;`) so the crate root tells the truth;'
    echo "    if the guard refused, the remedy it printed is the fix. There is no snapshot"
    echo "    to regenerate — this guard does not detect public-API drift (see #3366)."
    echo "--- last 60 lines of $log ---"
    tail -60 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# tooling-tests: fast shell-tooling regression tests that have no Rust target and
# no dataset/network needs. Currently scripts/tests/test_agent_gate_summary.sh,
# which verifies the SUMMARY block survives non-foreground capture (#1175), and
# scripts/tests/test_agent_gate_smoke_target_dir.sh, which verifies the smoke step
# resolves the CLI via CARGO_TARGET_DIR (#1247). These two never run the real gate
# components, so wiring them here cannot cause the gate to recurse. Also runs
# scripts/tests/test_agent_gate_parity_report.sh (#1338), which drives nested
# `agent-gate.sh --only parity-report` invocations to assert the SKIP/PASS/FAIL
# outcomes; that nesting is BOUNDED (--only parity-report never selects
# tooling-tests, so it cannot recurse) and the cassandra-parity build is already
# warm from the earlier parity-report component, so it stays cheap. Also runs
# scripts/tests/test_bootstrap_agent_machine.sh (#1921), which proves the
# new-machine bootstrap's pure-check paths never install anything (it runs with
# --skip-smoke, so it never invokes the real gate — no recursion). Also runs
# scripts/tests/test_agent_gate_delta.sh (#1892), which drives the hidden
# --delta-classify hook + --delta entry guards + --delta-...-emit-summary-selftest
# to assert the test/docs-only fail-closed re-cert policy and DISTINCT delta
# markers (hermetic — classification/emission only, never runs cargo). Also runs
# scripts/tests/test_gate_failure_mode.sh (#2662), which pins the decision table
# of scripts/ci/gate-failure-mode.sh — the routing logic behind the nightly
# gate-failure alert workflow, which is otherwise untestable (it only fires on a
# real workflow_run event). Pure/offline, no gh/network. Also runs
# scripts/tests/test_check_skill_flag_tables.sh (#3054), which pins
# scripts/ci/check-skill-flag-tables.sh: the AUTO-LOADED sstable-parsing skill's
# row/extended/cell flag tables must match the real row_decoder constants, so an
# agent can never again be taught a partition-boundary decode bug by a rotted skill
# table (hermetic temp-sandbox copy; no cargo/datasets/network). Pure/offline, no
# gh/network. Also runs scripts/tests/test_agent_gate_schemas_preflight.sh (#3148),
# the POSITIVE CONTROL for the committed-schemas preflight: it proves the preflight
# REJECTS a schemas-less / present-but-incomplete root (the #3148 gap survived because
# `STATUS: OK` was only ever observed on the happy path) and pins the checkout-relative
# resolution contract. Hermetic temp roots; the FULL-gate cases exit AT the preflight,
# so no cargo/datasets/network. Also runs scripts/tests/test_gate_notify_contract.sh (#3119),
# which asserts the PUBLISHED push-signal payload (title/body/priority/tag, POSTed to the ntfy
# server ROOT, message never a JSON document) at the TRANSPORT boundary via a curl-capture shim —
# the companion test_agent_gate_notify.sh asserts only the ADVISORY half, and an argv-level
# assertion is explicitly NOT evidence for payload fidelity (the swallowed `--category` defect was
# invisible to one). Hermetic: no network, no real topic, pristine-agent-notify fixture copied into
# its own tmpdir. SKIP-aware: the summary test's truncation case relies on a python3
# reader, so with no python3 we record SKIP (loud, never silent PASS); any test
# failure -> hard FAIL.
# Also runs scripts/tests/test_pub_surface_guard.sh (#1712), the non-vacuity proof for
# the pub-surface component: 42 cases driving scripts/ci/check-pub-surface.sh through
# 10 greens, 30 reds, the usage case and the kill-safety case, substituting the artifact
# in detached scratch worktrees rather than through any test-only seam. It pins every
# crate-root parse shape the (lexical, not-a-Rust-parser) scan claims to handle, all
# four SHARED blind spots that its two derivations cannot express as a disagreement,
# and every REFUSAL path of the module-file oracle together with the green controls
# that stop a refuse-everything guard from satisfying them. SOURCE-ONLY since the #1712
# descope — no cargo doc, no cargo at all, seconds not minutes; never invokes the gate
# except in case 26 (`--only pub-surface`, which self-exempts from the #1825 slot), so
# it cannot recurse.
run_tooling_tests() {
  local name=tooling-tests
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  : >"$log"

  # NOTE (#2751): AGENT_GATE_SUMMARY_FILE is already de-exported once after summary
  # resolution (see the scrub near the SUMMARY_FILE `case` block), so none of the
  # self-tests below — several of which recursively invoke agent-gate.sh — can
  # inherit the parent's summary path and clobber it. No per-component scrub needed.

  # generator keyspace-scoping guard (#1232): no python3 needed, always runs. A
  # failure here FAILs the component, mirroring the summary selftest semantics.
  echo ">>> [$name] bash scripts/tests/test_generator_keyspace_scoping.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_generator_keyspace_scoping.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (keyspace-scoping guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # UDT row-builder tuple-shape guard (#1991): no python3/Docker needed, always
  # runs. Pins build_udt_value() to a positional tuple (a dict → KeyError: 0
  # under prepared inserts, aborting the exhaustive regen) + an actionable 0-row
  # abort. A failure FAILs the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_udt_rowbuilder_tuple_shape.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_udt_rowbuilder_tuple_shape.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (udt-rowbuilder tuple-shape guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # flight Dockerfile Rust-pin lockstep guard (#2870): no python3/Docker/cargo
  # needed, always runs. Mechanizes #1990 — asserts cqlite-flight/Dockerfile has
  # exactly one `FROM rust:` line matching rust-toolchain.toml's channel. A
  # failure FAILs the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_check_dockerfile_rust_pin.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_check_dockerfile_rust_pin.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (flight Dockerfile rust-pin lockstep); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # auto-loaded-skill flag-table drift guard (#3054): no python3/Docker/cargo
  # needed, always runs. Pins the row/extended/cell flag tables in
  # .claude/skills/sstable-parsing/{SKILL.md,cassandra5-format-reference.md} to the
  # real constants in row_decoder/{mod.rs,row_data.rs} — those skills auto-load into
  # every binary-format agent context, and the pre-#3054 tables taught
  # partition-boundary mis-detection (0x01 labeled IS_MARKER/IS_STATIC). Fails closed
  # when a source split moves the constants or the table is reformatted away. A
  # failure FAILs the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_check_skill_flag_tables.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_check_skill_flag_tables.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (skill flag-table drift guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # roborev vacuous-review guard (#2964): hermetic (stub roborev first on PATH +
  # throwaway git fixtures), no network/datasets/cargo, ~0.5s. Pins every recorded
  # trigger that lets "roborev clean" be recorded without a review having happened
  # (worktree bare-`--branch` enqueueing the base sha, the range form enqueueing
  # neither endpoint, a code-free diff silently discarded, the vacuous token
  # signature, an unpushed branch, and an empty census reported as PASS). The
  # wrapper's verdict gates a merge, so a weakened assert means unreviewed code
  # merges with no red anywhere. Also runs in --lite via roborev-lints; kept here so
  # the full gate covers it in its shell-tooling component set too. A failure FAILs
  # the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_roborev_review_guard.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_roborev_review_guard.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (roborev vacuous-review guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # committed-schemas preflight guard (#3148 AC (c)): hermetic (temp dataset/schemas
  # roots, no real corpus/network/cargo — the FULL-gate cases exit AT the preflight).
  # POSITIVE CONTROL: it proves the preflight REJECTS a schemas-less and a
  # present-but-incomplete root, not merely that it accepts a good one. The #3148 gap
  # survived precisely because `STATUS: OK` was only ever observed on the happy path,
  # so a preflight tested one-sided is untested. Also pins the checkout-relative
  # contract (the schemas root must not vary with CQLITE_DATASETS_ROOT — the retired
  # `..`-climb symlink trap) and the single-definition invariant. A failure FAILs the
  # component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_schemas_preflight.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_schemas_preflight.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (committed-schemas preflight guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # perf-run-contained argument-safety guard (#3068): no python3/sudo/systemd
  # needed, always runs (the wrapper's --check-args hook executes nothing). Pins
  # the --mem/--swap validation of test-data/scripts/perf-run-contained.sh: that
  # wrapper is the containment around multi-GB perf-corpus reads after an
  # uncontained one hard-hung a swapless host for 75 minutes, so a silently
  # misparsed cap is a host-availability bug, not a usability nit. A failure FAILs
  # the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_perf_run_contained.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_perf_run_contained.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (perf-run-contained arg-safety guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # perf-corpus generator guard (#3068): hermetic (no docker/sudo/cassandra —
  # only the generator's --validate-only/--prune-dry-run hooks, which exit before
  # the container starts). Pins (a) TABLES validation BEFORE any destructive work
  # plus the manifest writer's refusal to emit an empty `tables` array — together
  # they stop a typo silently overwriting the committed provenance manifest — and
  # (b) the tight scoping of stale-corpus pruning, which deletes multi-GB paths.
  echo ">>> [$name] bash scripts/tests/test_gen_perf_corpus_3068.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gen_perf_corpus_3068.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (perf-corpus generator guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # BTI perf-corpus generator guard (#3234): hermetic (no docker/sudo/cassandra —
  # --help/--validate-only/--verify-only, the row driver, the manifest writer's
  # pre-container guards, and a full end-to-end run through a STUB `docker`
  # (scripts/tests/fixtures/stub-docker-cassandra-bti.py); nothing here starts a
  # container or needs root). Pins issue
  # #3234's ACCEPTANCE ASSERTS IN BOTH DIRECTIONS against fabricated corpora: a
  # stock Cassandra 5.0 node silently emits `nb` (BIG) when either mandatory yaml
  # setting misses, so an assert only ever observed on a good corpus is untested —
  # every case here carries a negative control (`nb-*` descriptor, empty Rows.db,
  # sub-8-MiB Data.db, a TOC listing the BIG-only Index.db). Also pins the row
  # driver's (seed, chunk) determinism, which is what makes the manifest's
  # per-Data.db sha256 a reproducibility check. A failure FAILs the component,
  # mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_gen_perf_corpus_bti.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gen_perf_corpus_bti.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (BTI perf-corpus generator guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # AC3 warm-scan harness guard (#3234): the automated executor for
  # cqlite-core/examples/bti_perf_scan/, the instrument every #3234 throughput
  # number comes from. Its guards had never been OBSERVED to fire, and its worst
  # failure mode is silent — a TRUNCATED scan reporting `RESULT: PASS` with a short
  # row count. This drives the real binary and asserts its exit code for each
  # documented mode (2 usage incl. `--min-seconds nan`, 3 open-failed, 4 zero-rows,
  # 5 row-count mismatch, 6 window-too-short, 7 scan-failed-mid-stream, 8 no
  # authoritative row count) plus the guarded happy path as positive control.
  # Hermetic and cheap: it runs against the GIT-COMMITTED 10 KiB `test_da`
  # BTI (`da`) fixture (468 Cassandra-written rows), never the ~2 GiB perf corpus,
  # and needs no docker/network/python3/datasets. It does `cargo build -p cqlite-core
  # --example bti_perf_scan --features cli-helpers` (incremental; the full gate has
  # already compiled that graph), and a build failure is a FAILURE, never a skip.
  echo ">>> [$name] bash scripts/tests/test_bti_perf_scan.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_bti_perf_scan.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (AC3 warm-scan harness guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # Unenforced-constraint-comment guard (#3234): a comment that STATES a constraint
  # must name or sit beside its enforcement, or the comment goes. Mechanizes the class
  # that hit this one issue three times (L3's 11 unobserved manifest claims, L4's stale
  # committed contract, F2's `validated_sstable_dir` documenting `<table>-<uuid>` while
  # accepting `<table>-*`) — prose asserting what the adjacent code does not do, caught
  # each time only by a human reviewer. Narrow by design (a named claim table over the
  # #3234 production surface, not an English verifier) and it runs BOTH directions:
  # `--self-test` asserts the real surface passes, that an INJECTED unenforced claim
  # FAILS, and that the same claim passes once its enforcement follows it. Hermetic,
  # python3-only, sub-second.
  echo ">>> [$name] python3 scripts/tests/check-constraint-comments.py --self-test"
  if ! python3 "$REPO_ROOT/scripts/tests/check-constraint-comments.py" --self-test \
    >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (unenforced constraint comment); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ROOT JUNK-FILE guard (#3272 review round 5, F5). An empty file named `0` was
  # committed at the repo root of one branch THREE times — removed, re-added by a
  # `git add -A`, removed again. No committed code produces it: it is the residue of an
  # ad-hoc shell redirect typed during a fix round (`… 2>0`, a mistyped `2>&1`), which a
  # later blanket `git add` swept up. Deleting the file is not the fix; this is.
  #
  # The VERDICT-BEARING subject is the TRACKED root only (#3272 round 8). It was both states
  # until the LINUX gate of record failed this component on a file named `720` that was
  # UNTRACKED, never committed, and already gone when the run was inspected — transient debris
  # from a CONCURRENT STEP OF THE SAME GATE. A guard whose subject is the live working tree can
  # be tripped by a peer step of the run it is certifying, i.e. it reds the gate at random over
  # content no author can act on, and a guard people learn to waive is worse than no guard. So
  # the subject was NARROWED (not given a settle/retry window, which would trade a wrong subject
  # for a timing-dependent verdict): committed content is attributable to a diff and can ship;
  # untracked debris is reported as a NON-FAILING notice.
  #
  # The predicate stays narrow on purpose: only a name that is ENTIRELY a file-descriptor
  # number, an `&`-prefixed one, or bare redirect punctuation, at depth 1. Anything with a
  # letter, dot, dash or underscore is a legitimate file and is not flagged (driven COMMITTED,
  # which is now the only observable path: `2026-report.md`, `v2`, `0.14.0.md`, `0x`, `a0`, `_0`).
  # `--self-test` OBSERVES the tracked half FAILING, the untracked half NOT failing while still
  # being reported, and a tracked finding surviving concurrent untracked debris, in a throwaway
  # git repo under $TMPDIR — then scans THIS checkout, so the hook cannot certify the probe while
  # leaving the real root unexamined. git-only: no python3, no network, hence NO SKIP PATH to
  # record a vacuous success.
  echo ">>> [$name] bash scripts/ci/check-root-junk-files.sh --self-test"
  if ! bash "$REPO_ROOT/scripts/ci/check-root-junk-files.sh" --self-test >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (TRACKED accidental-redirect artifact at the repo root, or its self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 measurement-rig integrity guards (#3096/#3272): hermetic (synthetic result
  # dirs + synthetic perf CSVs — no cargo, perf, sudo, corpus or network, and never
  # root; the driver is only ever reached at argument validation, and the sysctl
  # restore is exercised through a recording `sudo` shim). Pins the rig's
  # measurement-integrity guards, every one of which was a real defect of the same
  # shape — an instrument that reports success without having measured:
  #   * a COLD Flight rep must be EXACTLY ONE full-corpus request (requests 2..N are
  #     WARM and were being blended into a figure labelled "cold");
  #   * a WARM rep of EITHER arm must record an untimed prewarm, and the cold arm's
  #     `skipped-cold-arm` sentinel may satisfy a COLD rep ONLY — a warm rep carrying
  #     it is an UNPREWARMED warm measurement passing the guard added to refuse one,
  #     and the bare scan is the DENOMINATOR of the 1.3x ratio. A COLD rep must carry
  #     that sentinel EXACTLY: any other value (including `unrecorded` = no status file)
  #     means nothing establishes the rep was not prewarmed, and unlike the warm
  #     direction that bias is UNBOUNDED — a secretly-warm rep reported cold reads
  #     FASTER — so it is a REFUSAL, not a captioned figure plus a verdict;
  #   * the corpus identity is REQUIRED, so the full-corpus-per-request check can
  #     never be skipped while the report's notes claim it ran;
  #   * an absent, uncounted or unparseable perf counter is an ERROR, never a
  #     fabricated 0 that would make "setup-subtracted" a lie;
  #   * --reps/--scan-passes/--port are validated positive (--reps 0 was a vacuous
  #     but SUCCESSFUL report);
  #   * completeness is judged against the STATED selection;
  #   * durations parse as DECIMAL (`010s` was octal 8s);
  #   * and the host sysctls the rig weakens (perf_event_paranoid, kptr_restrict) are
  #     captured before mutation and RESTORED on EXIT/INT/TERM/HUP.
  # A broken measurement guard publishes a wrong number instead of failing, so it
  # needs a standing test rather than a review round — and per #3249 the bar is
  # "observed to fire", not "present".
  # The CLOSED verdict-evidence checker (#3248 jobs 73/75/78). Wired HERE rather than left to be
  # run by hand: an unwired suite is dead weight, and this one is the standing cover for a guard
  # that review holed three rounds running.
  echo ">>> [$name] bash scripts/tests/test_ws0_quiescence_evidence_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_quiescence_evidence_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 quiescence-evidence guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    return 1
  fi
  echo ">>> [$name] bash scripts/tests/test_ws0_report_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_report_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 measurement-rig integrity guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 CLOCK guards (#3248) — the occupancy-enforced clock derivation.
  # AC4 of #3248 asked for a reconciliation "stating the clock basis". This test exists
  # because STATING IT DEMONSTRABLY DOES NOT WORK: #3299 published `cycles / task-clock`
  # as a frequency (under CPU-wide `perf stat -C` that is occupancy x frequency, since
  # task-clock accrues elapsed x nCPUs INCLUDING IDLE CPUs), retracted it at a printed
  # 1.271 "GHz", and then reached for the same quantity AGAIN hours later — the first
  # retraction having overridden a caption written specifically to prevent it. A prose
  # control failed twice in the hands of people who knew about it, so the control is now
  # a tool that REFUSES, and this is the standing proof that it still refuses.
  # 16 cases: every guard fed the input it must reject, asserting exit code AND cause
  # token, plus an affirmative accept case that pins the frequency, the exact TSC, two
  # independent occupancy sources and the labelled trap value. Hermetic — the tool
  # consumes a perf CSV and invokes nothing, so no perf, corpus, cargo or network.
  echo ">>> [$name] bash scripts/tests/test_ws0_clock_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_clock_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 occupancy-enforced clock guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 QUIESCENCE guards (#3248) — the box-quiescence gate for a measurement rep.
  # Wired here because roborev job 60 finding 7 caught that it was NOT: the clock suite was
  # added and this one was not, so 19 checks sat in the tree with no standing protection —
  # a guard that exists and never runs, which is this issue's own subject matter.
  echo ">>> [$name] bash scripts/tests/test_ws0_quiescence_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_quiescence_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 box-quiescence guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 CELL-VOLUME guards — "rows were checked and CELLS were not" (#3272 round 17).
  # Split out of the reporter suite above under the campsite rule (that file reached 1602 lines
  # against the ~1500 test target) along a SUBJECT seam, and it must be wired here or the 11
  # checks it carries become a test nothing executes — the #1597/#1618 gate-wiring class this
  # rig has already paid for. The reporter suite asks whether a quantity was validly OBSERVED;
  # this asks whether, given a session whose every quantity IS observed and internally
  # consistent, the WORK the published figure divides by was actually done. A pass returning
  # EVERY ROW WITH MISSING COLUMNS satisfies every check in the sibling suite — right pass count,
  # every pass observing exactly the pinned corpus row count, recorded aggregates equal to the
  # derived sums — while decoding materially less data, and its rows/s is the DENOMINATOR of the
  # rig's only output. Different oracle (`cells_per_row` from the corpus identity, not the row
  # count) and a different non-vacuity mutation site (`ws0_collect.py`'s per-pass requirement).
  # Hermetic: synthetic session dirs under $TMPDIR driven through the shipped reporter; no cargo,
  # perf, sudo, taskset, corpus, network or driver invocation.
  echo ">>> [$name] bash scripts/tests/test_ws0_cell_volume_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_cell_volume_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 cell-volume guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 CORPUS-BOUNDARY guards (#3272 round 21), its own suite under the campsite rule and WIRED
  # HERE IN THE SAME CHANGE — an unwired suite is the same defect class as a fail-open guard.
  # Subject: the pre-measurement pin COPIED `data_db_sha256` and the whole component map out of the
  # corpus's own `corpus-identity.json` instead of hashing the files, so the pin and that sidecar
  # agreed BY CONSTRUCTION however the bytes on disk differed — a claim restated, not a
  # measurement, i.e. #3249's hardcoded `_PERF_STATE="ok"` with extra steps. MEASURED: a component
  # MUTATED during measurement and RESTORED before reporting left the Data.db digest verified, all
  # components verified and all PINNED components verified, while the reps on either side had
  # measured different bytes — the failure biases TOWARD the claim. The pin now HASHES (and
  # COMPARES the sidecar against the measured values), records `components_source` so a copied pin
  # cannot pass as a measured one, and `verify_corpus_boundary` re-hashes the ACTUAL bytes at each
  # measurement boundary against the PIN, refusing the rep and naming what changed — the half no
  # pre/post pair can see. Hermetic: synthetic session dirs and component files under $TMPDIR
  # through the shipped writer/verifier/reporter; no cargo, perf, sudo, corpus, network or driver.
  echo ">>> [$name] bash scripts/tests/test_ws0_corpus_boundary_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_corpus_boundary_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 corpus-boundary guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 BOUNDARY-RECORD COMPLETENESS guards (#3272 round 25), split out of the corpus-boundary
  # suite above under the campsite rule and WIRED HERE IN THE SAME CHANGE — an unwired suite is a
  # test nothing executes, the #1597/#1618 gate-wiring class this rig has already paid for twice.
  # The split follows the SAME SEAM the shipped code does (round 22 split the boundary question into
  # a WRITER module and a READER module), so the two suites are one question each.
  #
  # Subject: round 22 wired `verify_boundary_observations` into the reporter, and the only thing
  # that ever fed it was the fixtures' HEALTHY generator — so MISSING, DUPLICATE and UNEXPECTED were
  # all UNOBSERVED, and a checker returning OK unconditionally would have been indistinguishable
  # from the shipped one (#3249's `_PERF_STATE="ok"`, which survived 118/118 tests). All five
  # directions now fire independently over the SHIPPED generator's record MUTATED — accept, missing,
  # duplicate (with NOTHING missing, so an `observations >= expected` checker would have accepted
  # it), unexpected, and absent/unparseable. Non-vacuity MEASURED for MISSING: the PRE-FIX reporter,
  # reconstructed from `ws0_report.py`'s own text minus exactly its four consuming lines, PUBLISHES a
  # 2.00x ratio over the same short-record session and writes a results.json with no completeness
  # field while claiming the digest and every component verified. Hermetic: synthetic session dirs
  # under $TMPDIR through the shipped fixture generator and reporter; no cargo, perf, sudo, corpus,
  # network or driver invocation.
  echo ">>> [$name] bash scripts/tests/test_ws0_boundary_record_completeness.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_boundary_record_completeness.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 boundary-record completeness guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 ERROR-CODE CROSS-CHECK guards (#3272 round 20), split out of the reporter suite under
  # the campsite rule and WIRED HERE IN THE SAME CHANGE — an unwired suite is a test nothing
  # executes, the #1597/#1618 gate-wiring class this rig has already paid for twice. Subject:
  # `error_codes` was classified `ignored` on the ASSUMED invariant that the map is empty
  # whenever `requests_error` is 0, which nothing enforced — so a record carrying
  # `requests_error: 0` beside `error_codes: {"Internal": 1}` was accepted and published as a
  # clean, failure-free scan with the failing code named nowhere in the output (MEASURED). The
  # invariant now enforced is the SUM, the producer's own (`StepAgg::record_outcome`), which also
  # catches a breakdown disagreeing at a NON-ZERO count. Different oracle from both siblings
  # (another field of the SAME record) and a different non-vacuity mutation site
  # (`ws0_error_codes.py`). Hermetic: synthetic session dirs under $TMPDIR driven through the
  # shipped reporter; no cargo, perf, sudo, taskset, corpus, network or driver invocation.
  echo ">>> [$name] bash scripts/tests/test_ws0_error_code_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_error_code_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 error-code guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 MEASUREMENT-APPARATUS guards (#3272, item 10): the sibling of the reporter
  # test above, split out because it covers a different question over disjoint
  # fixtures — that one asks what the rig DOES with its observations, this one asks
  # whether the observations are of the right thing at all. Two guards, both
  # directions each:
  #   * the VERIFIED-SIBLING taskset check — the load-bearing assumption of the
  #     whole same-session both-arm methodology. If the two pinned CPUs are not one
  #     physical core's hyperthreads, `perf stat -C` counts two different cores and
  #     every per-core figure is a figure of something else, silently. The check had
  #     never been OBSERVED refusing anything (it read a hardcoded /sys path, so it
  #     needed a particular CPU layout to test); lib-cpu.sh now takes an injectable
  #     topology root and this drives it over a FAKE 4-core/8-thread sysfs tree: it
  #     must ACCEPT genuine sibling pairs and REFUSE two-different-cores, a
  #     valid-on-another-box pair, a pair-plus-stray, a lone CPU, a cross-core range,
  #     an empty spec and an unreadable topology entry. The override itself fails
  #     closed in a measurement run (asserted, incl. that the refusal PRECEDES the
  #     pinning check), so it can never become the bypass.
  #     Round 21 adds the CLIENT set, whose check used to fail OPEN: a
  #     `verify_sibling_pair … || echo` swallowed EVERY failure rather than only the
  #     sibling shape it meant to tolerate, so a nonexistent or OFFLINE CPU in the
  #     list was accepted, sched_setaffinity silently reduced the affinity to the
  #     valid subset, and the manifest recorded — and the report printed — CPUs that
  #     never ran an instruction. `verify_cpus_online` now validates EACH expanded CPU
  #     independently (present + online, three states distinguished) and refuses,
  #     naming every unusable CPU, while still requiring NO sibling shape — measured
  #     both ways, including the pre-fix compound's rc=0 on a mixed valid/absent list.
  #   * the SERVER-OWNERSHIP check — the same question about the right PROGRAM.
  #     Readiness used to be inferred solely from a connect probe succeeding, so a
  #     failed bind plus a peer holding the port meant the load generator measured
  #     THAT server while `perf stat -C` counted OUR CPUs. Driven against the shipped
  #     lib-server.sh with a real listener on a kernel-assigned port, both directions
  #     (a foreign listener refused naming its pid, our own accepted, a descendant
  #     accepted but a SIBLING refused, a dead server and a readiness TIMEOUT fatal,
  #     and an unanswerable prober stopping the run rather than passing vacuously).
  # The THIRD guard — the three-layer perf-invocation lint — is its own component
  # below (the campsite-rule split; this file reached 1607 lines against the ~1500
  # test target, and the two subjects share no fixture and no helper).
  # Hermetic: a fake sysfs tree + a loopback listener under $TMPDIR. No perf, sudo,
  # taskset, root, real multi-socket hardware, corpus, network or cargo.
  echo ">>> [$name] bash scripts/tests/test_ws0_cpu_pinning_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_cpu_pinning_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 cpu-pinning / server-ownership guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 PERF-INVOCATION LINT, all three layers (#3272, item 10; split out of the
  # cpu-pinning suite above under the campsite rule, along a RESPONSIBILITY seam —
  # that one asks which CPUs and which PROGRAM an observation is of, this one asks
  # whether its COUNTING DOMAIN is the one spec R2 mandates). Per-process counting
  # measured >2x observer cost, so the driver checks ITSELF at startup. Driving that
  # guard over injected copies found FIVE REAL BYPASSES across two successive
  # deny-list patterns: an ATTACHED `-p<pid>` (the pattern required a trailing
  # space), ANY line mentioning "self-check" (the `grep -v` discarded by CONTENT, so
  # a comment on a real invocation suppressed the guard), a SINGLE-QUOTED attached
  # value, an invocation through a VARIABLE, and a GLOBAL OPTION between `perf` and
  # `stat`. All five fire now, and the mechanism is no longer a deny-list: an
  # ALLOWLIST (perf is invoked in ONE wrapper; any other invocation line must be
  # explicitly marked) plus a per-TOKEN option check plus a RUNTIME argv check —
  # which asks WHERE a line is rather than what it looks like, so a spelling nobody
  # anticipated still fires. The subject is the WHOLE scripts/perf tree, DISCOVERED
  # by glob and asserted against `ls` (a hand-maintained list had already drifted
  # past two libraries). Both directions throughout: the shipped tree must be CLEAN,
  # and the lint must NOT flag `perf_stat_c`/`perf_event_paranoid`/`target/perf-…`
  # identifiers — a guard that reds on ordinary code is the one an operator deletes.
  # Its OWN vacuity states are driven too (an empty subject, an absent or doubled or
  # `-C`-less wrapper, a variable command word, an UNREADABLE rig file, a mode with
  # no END assertions, and an awk that dies under the driver's `set -e -o pipefail`),
  # each an instance of the rule this issue is about: never derive a positive verdict
  # from the absence of a bad signal. Hermetic: driver and scripts/perf copies under
  # $TMPDIR plus a shimmed `perf` function; no perf, sudo, taskset, root, corpus,
  # network or cargo.
  echo ">>> [$name] bash scripts/tests/test_ws0_perf_invocation_lint.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_perf_invocation_lint.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 perf-invocation lint guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 NO-FABRICATED-VALUE guards (#3272 AC3, review round 1). The third file of the
  # rig's self-test set, for one subject: a counter or verdict that was not OBSERVED
  # is an ERROR, never a default. Round 1's review found that rule stated in
  # ws0_report.py's own docstring and then violated FIVE times in the same file, each
  # time by an idiom that reads as harmless —
  #   * `int(rec.get("requests_error", 0)) > 0`, so the "no failed requests" refusal
  #     rested on a FABRICATED 0 and a record with no such key was reported CLEAN
  #     (measured: exit 0 + a full five-line report, the error count never read);
  #   * `block.get("prewarm_all_ok", True)`, a VERDICT key defaulting to the
  #     PERMISSIVE value, so a block that lost the verdict suppressed the warning;
  #   * `(hi-lo)/med*100 if med else 0.0`, printing the DEGENERATE series as the
  #     TIGHTEST one (`0 rows/s [0..0, spread 0.0%]`);
  #   * `scan_rps / fl_rps if fl_rps else float("inf")`, publishing `inf x` as the
  #     bare/flight ratio for an arm that measured nothing;
  #   * `rec = records[-1]`, silently DROPPING every earlier step record (measured: a
  #     rep whose first record held 9 failed requests over a 37-row partial scan
  #     published the second record's clean 250,000 rows/s).
  # Plus the coercion class (a truncating `int()`, a boolean read as a count), the
  # DERIVED flight throughput, and the two structural ast scans that keep a
  # permissive-default idiom and a hand-written "complete inventory" out of the
  # reporting path. The MEASUREMENT-PROVENANCE half — the corpus identity's BYTES, the
  # complete component set, the pre-measurement pin and the manifest-read
  # configuration — is its own component below (review round 6's campsite-rule split).
  # Hermetic: synthetic session dirs, synthetic perf CSVs, and a few-KB synthetic
  # Data.db whose real sha256 is computed with hashlib; no cargo, perf, sudo, corpus,
  # network or root. python3 absence FAILS here (it is a hard requirement of the rig),
  # never skips.
  # ws0 SELF-TEST HERMETICITY, as a MECHANISM (#3272 review round 3, B1). The three files
  # above must not, while testing the rig, RUN the rig: below its argument-validation
  # boundary the driver writes host sysctls via `sudo -n`, runs `cargo build --release`,
  # drops the page cache and takes 45-second `perf stat` measurements — inside this gate
  # component, on this Linux box. Round 1 of the review found six such call sites; round 2
  # introduced `--validate-args-only` + recording shims and left ONE bare, and a manual
  # sweep missed it TWICE. So this is a STRUCTURAL LINT over every `test_ws0_*.sh` rather
  # than a rule in a comment: any driver invocation not routed through `ws0_driver_run` is
  # a finding, judged by LOCATION (the same posture as the perf lint's layer 1), with the
  # subject DISCOVERED by glob so a fourth self-test cannot be added outside the contract.
  # Both directions are measured (six bare spellings fire, six ordinary lines do not), and
  # the platform property is OBSERVED on a LINUX-SHAPED fixture with a positive control
  # proving the bare run really does mutate that host. Hermetic; sub-second.
  # ws0 HOST STATE (#3272 finding 3, split out of the reporter test in review round 3). The
  # rig weakens `kernel.perf_event_paranoid` and `kernel.kptr_restrict` so `perf stat -C` can
  # count CPU-wide, and used to NEVER put them back: its only trap was `trap stop_server
  # EXIT`, so a success, a FATAL and a Ctrl-C all left the host less hardened than the rig
  # found it — permanently, for every subsequent process on a shared fleet machine, with
  # nothing in the output saying so. The FIRST fix of that was itself PARTIAL (the
  # success/warning split keyed on "was ANYTHING restored", so a partial restore printed the
  # affirmative line and no warning), which is why the ROOT CAUSE is closed too: a knob whose
  # prior could not be CAPTURED is never MUTATED. Behavioural, through a recording `sudo`
  # shim plus a real SIGINT probe on the driver's trap wiring — no privileged call happens and
  # no host knob is touched, and the exact `sysctl -w` argv the handler WOULD issue is
  # asserted instead. A separate file from the reporter guards because this is the only part
  # of the rig that changes anything OUTSIDE its own process tree, and the only part whose
  # failure is SECURITY-ADJACENT rather than a wrong number.
  echo ">>> [$name] bash scripts/tests/test_ws0_host_state_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_host_state_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 host-state restore guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  echo ">>> [$name] bash scripts/tests/test_ws0_hermeticity.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_hermeticity.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 self-test hermeticity); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  echo ">>> [$name] bash scripts/tests/test_ws0_fabrication_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_fabrication_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 no-fabricated-value guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 MEASUREMENT PROVENANCE (#3272 review round 6). Split out of the fabrication suite
  # under the campsite rule, by SUBJECT: that suite asserts a property of a NUMBER (a counter
  # or verdict not OBSERVED is an error, never a default), this one a property of the RUN the
  # numbers came from — a report must identify the BYTES and the CONFIGURATION it describes.
  # The two are orthogonal, which is the reason for the seam: every check in the fabrication
  # suite is satisfiable by an artifact set that is internally consistent about the WRONG run.
  # Four findings, each with its accept direction:
  #   * F1 — reps/temps/arms/scan-passes and the CPU pins were the REPORTER'S arguments, tied
  #     to nothing about the session, so a re-report could SUBSTITUTE a configuration and state
  #     it had been verified (measured: a 3-rep session reported at `--reps 1` published rep 1
  #     as the whole run, under CPU pins the session never used, beside a "verified
  #     physical-core siblings" claim). The configuration is now READ FROM the pre-measurement
  #     manifest and the flags are GONE — each is an argparse error, so the substitution cannot
  #     be expressed rather than merely being detected.
  #   * F3 — corpus verification checked ONLY `Data.db`, while a scan reads `Index.db` above
  #     all plus the Statistics/Summary/Filter components that shape how it reads. Measured
  #     with the fix reverted: rewriting `nb-1-big-Index.db` left the report at exit 0 with its
  #     "the identity describes the bytes that were measured" line intact. All 5 recorded
  #     components are now re-stat'ed and re-hashed, and the summary states the count.
  #   * B6 — `corpus-identity.json` was validated for internal consistency and the `Data.db`
  #     was NEVER OPENED, so a 4 KB file beside an identity claiming 700,000 bytes and an
  #     unrelated sha256 exited 0 and printed that sha256 as the measured one. The digest is
  #     skippable for a multi-GB corpus ONLY via --skip-corpus-digest, which STAMPS
  #     `CORPUS DIGEST UNVERIFIED` into the summary and `sha256_verified: false` into
  #     results.json — never a silent skip, and the cheap size half still fires under it.
  #   * The PRE-MEASUREMENT PIN (round 4) — the report-time digest cannot see either sequence
  #     that attributes figures to bytes nobody measured (re-reporting an old session against a
  #     different corpus; a corpus regenerated mid-run), because BOTH are self-consistent AT
  #     REPORT TIME. The driver stamps a pin BEFORE the first rep and the reporter REQUIRES it;
  #     the driver-side wiring is asserted too, including that the stamp PRECEDES the
  #     measurement loop.
  # Hermetic: synthetic session dirs, synthetic perf CSVs, and synthetic few-KB Data.db files
  # whose real sha256 is computed with hashlib; no cargo, perf, sudo, corpus, network or root.
  # python3 absence FAILS here (it is a hard requirement of the rig), never skips, and a
  # check-count floor closes the suite-level 0/0.
  echo ">>> [$name] bash scripts/tests/test_ws0_provenance_guards.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_provenance_guards.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 measurement-provenance guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 OUTPUT-DIRECTORY EXCLUSIVITY (#3272 review round 13, campsite split). Split out of
  # `test_ws0_provenance_guards.sh` (1606 lines against the ~1500 test target — the `file-size`
  # ratchet is `.rs`-ONLY, so a shell file crosses it silently) along a responsibility seam: the
  # parent's subject is WHICH BYTES AND WHICH CONFIGURATION a report describes; this file's is
  # whether the DIRECTORY holding those artifacts belongs to exactly ONE session. Distinct because
  # every parent check is satisfiable by a session whose corpus, components, schema, request and
  # configuration are impeccably pinned and whose rep files were assembled from TWO RUNS that
  # shared a directory — the reporter reads whatever rep files are present, and a pin identifies
  # the corpus of the session that WROTE it, not the provenance of every sibling file beside it.
  # Four findings share the subject, all over `scripts/perf/lib-outdir.sh`: round 6's R1
  # (`mkdir -p` over a second-unique default name, and an explicit `--out` keeping a previous
  # run's rep files), round 9's F4 (the used-dir enumeration's STATUS discarded, so a FAILED
  # `find` was indistinguishable from an empty directory and took the permissive branch), round
  # 7's F3 (R1 hardened the DEFAULT branch and left the EXPLICIT one on `mkdir -p`, so two
  # concurrent runs given the same empty `--out` both proceeded) and the boundary placement both
  # rest on (REFUSAL above `--validate-args-only`, CREATION below it). Hermetic: synthetic
  # directories under $TMPDIR, the SHIPPED `lib-outdir.sh` sourced into subshells, driver
  # invocations ONLY through `ws0_driver_run`; no cargo, perf, sudo, taskset, corpus, network or
  # root. One case SKIPS as root (which bypasses the read bit the F4 trigger needs), with a stated
  # reason rather than a pass.
  echo ">>> [$name] bash scripts/tests/test_ws0_output_dir_exclusivity.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_output_dir_exclusivity.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 output-dir exclusivity guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 CANONICAL-CORPUS COMPARISON (#3272 review round 13, F3). The third campsite split off
  # `test_ws0_provenance_guards.sh`, and a distinct SUBJECT: that file asks whether a report
  # IDENTIFIES the bytes it describes; this asks whether those are the bytes a WS0 BASELINE is
  # DEFINED as. Every provenance check is a SELF-CONSISTENCY check about whatever corpus was
  # supplied — pin matches identity, components match pin, schema matches its digest, rows are an
  # exact multiple of the pinned count — and ALL of it is equally true of a corpus generated with
  # smoke-test row counts or a different seed. So such a corpus passed the driver AND the reporter
  # as a WS0 BASELINE with nothing in the printed report to distinguish it. The canonical shape
  # lives in RUST (`tools/ws0-corpus-gen/src/measurement_corpus.rs`) and NOTHING under `scripts/`
  # referred to it (measured: zero grep hits), so the fix is a CROSS-LANGUAGE BRIDGE — a PARSE of
  # the Rust source, because no gate component or hermetic self-test may `cargo build` and a
  # committed generated copy would be a second copy of every value. A smoke corpus still RUNS,
  # under an explicit `--non-baseline` mode that LABELS the manifest and the report in words
  # (forbidding it would be the FOURTH documented operator command this issue broke). Both
  # directions are measured, with three non-vacuity halves: the pre-fix pin observed accepting a
  # 1000-row corpus uncompared, all 9 values asserted present in the Rust file's own text (so a
  # hand-copied Python literal could not satisfy the check), and a renamed constant observed to be
  # FATAL. Hermetic: synthetic corpora under $TMPDIR, the shipped oracle/writers called directly,
  # driver invocations only through `ws0_driver_run`; the Rust pin is READ, never compiled.
  echo ">>> [$name] bash scripts/tests/test_ws0_canonical_corpus.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_canonical_corpus.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 canonical-corpus guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 PER-REP ROUND METADATA (#3272 review round 4). Split out of the fabrication suite
  # under the campsite rule, by SUBJECT: the driver's loop order, the four RECORDED per-rep
  # fields, the artifact-set INTEGRITY refusals over them (same round set per arm, positions
  # 1..n exactly once, arms_in_round matching, no duplicate instant, labels not contradicting
  # instants, no arm at a fixed position), and — the reason it is its own subject — the
  # assertion that NO INTERLEAVING OR ORDERING CLAIM is made on ANY session shape.
  #
  # That last property is a round-4 owner ruling. Earlier rounds had the reporter print "the
  # reps were INTERLEAVED … OBSERVED FROM THE CLOCK …" and record `round_major_verified: true`;
  # at the rig's default `--reps 1` there is ONE round, so `zip(ordered, ordered[1:])` is
  # empty, ZERO orderings were compared, and the verdict was returned anyway — a positive
  # verdict from an absent measurement, which is the defect the whole issue exists to remove.
  # The claim and all 13 verdict fields were DELETED rather than re-worded a fourth time, and
  # this component is what keeps them deleted: forbidden PHRASES over every session shape
  # (one round, many rounds, a forged one), a shared key-walking assert over results.json
  # (`scripts/tests/ws0_assert_no_verdict_fields.py`), and two structural scans of the
  # reporter's ast-stripped executable code. Re-adding an OBSERVED drift control on real
  # hardware is #3287/#3299. Hermetic: synthetic session dirs only.
  echo ">>> [$name] bash scripts/tests/test_ws0_round_metadata.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_round_metadata.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 round-metadata / no-interleaving-claim guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 NUMERIC WRAPAROUND (#3272 review round 7, F2). Split out of the cpu-pinning suite
  # under the campsite rule, along a responsibility seam: the cpu suite's subject is TOPOLOGY
  # (is the pinning a real physical core?), this one's is ARITHMETIC — can a bound be defeated
  # by the evaluator that checks it? Bash arithmetic is signed 64-bit and WRAPS SILENTLY, and
  # that has now been the root cause of THREE findings in three places: round 4's
  # `parse_duration_ms` (`2305843009213693956s` * 1000 -> 4000ms, UNDER the 5000ms cold-step
  # ceiling, smuggling a blended cold measurement past that guard), round 4's
  # `require_positive_int` (a 20-digit value range-checked as 7766279631452241919), and round
  # 7's `cpu_range_validate` (`9223372036854775809-0` -> a NEGATIVE lo passing BOTH the index
  # ceiling and the expansion cap, whose own `hi - lo + 1` wraps negative too, then driving a
  # ~9.2e18-iteration array append — an OOM mid-measurement from an ACCEPTED argument).
  # Three sites, one class, so the fix is a MECHANISM: `lib-args.sh` owns
  # `decimal_normalize`/`decimal_le`, comparing canonical decimal STRINGS with no arithmetic at
  # all — no digit cap to choose and nothing left to wrap. Every firing case here carries a
  # NON-VACUITY half: a replica of the removed arithmetic, OBSERVED to have accepted the same
  # input. The expansion-loop case runs under `timeout`, because the pre-fix failure mode is a
  # hang and a hanging test is not a failing test. Hermetic: two sourced libraries plus a
  # synthetic sysfs topology in $TMPDIR; sub-second.
  echo ">>> [$name] bash scripts/tests/test_ws0_numeric_wraparound.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_numeric_wraparound.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 numeric-wraparound guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 PRIMARY-PATH ACCEPT DIRECTION (#3272 review round 11). The complement of the five suites
  # above, and it exists because of a ROOT CAUSE rather than a sixth guard: review rounds 9, 10 and
  # 11 each returned four findings, mostly living in the PREVIOUS round's fixes, and the shared
  # cause is that EVERY fix tested its guard REJECTING bad input while NOTHING tested the ACCEPT
  # direction of the primary command. Three documented commands were broken that way — round 9's F1
  # (`--verify-against`), round 10's L1 (the digest-oracle command) and round 10's M2, which broke
  # THE NORMAL MEASUREMENT COMMAND: the mtime-vs-HEAD staleness check was mode-blind on the premise
  # that `cargo build` touches every artifact, and cargo does NOT rewrite an already-current one, so
  # a script/docs-only commit plus a successful build left every mtime before HEAD and the driver
  # refused (round 11's F1). The five reject suites are green throughout and cannot see this: a guard
  # that refuses EVERYTHING satisfies all of them. What this asserts: the documented invocations
  # (`--corpus DIR` alone, then --reps/--temp both/--arm both/--no-build/--out/--scan-passes/the
  # durations/the CPU pins, and the full matrix in one command) reach `ARGUMENTS OK` having executed
  # NOTHING; the staleness check ADMITS a freshly-built binary older than HEAD under `built` while
  # still refusing it under `reused`; the SHIPPED writer's provenance record satisfies the SHIPPED
  # reader (a round trip no fixture-fed reject case can establish); and the schema/ticket/session-pin
  # verifiers admit a legitimate corpus written by the shipped writers. Each accept is paired with a
  # NON-VACUITY half (the same harness refuses `--reps 0` and the illegal cold/scan-passes pair; the
  # pre-fix mode-blind predicate is observed refusing the input now admitted). Hermetic: the driver
  # runs ONLY through `ws0_driver_run` (`--validate-args-only` + recording shims, every shim OBSERVED
  # to record), plus a throwaway git repo and a few KB under $TMPDIR — no cargo, perf, sudo, taskset,
  # corpus, network or root. What it deliberately does NOT reach is stated in the file's own header.
  echo ">>> [$name] bash scripts/tests/test_ws0_primary_path_admits_a_legitimate_run.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_primary_path_admits_a_legitimate_run.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 primary-path ACCEPT-direction guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 BINARY/BUILD PROVENANCE (#3272 review round 11, campsite split). Split out of
  # `test_ws0_provenance_guards.sh` (which reached 1652 lines against the ~1500 test target — the
  # `file-size` ratchet is `.rs`-ONLY, so a shell file crosses it silently) along a responsibility
  # seam: the parent's subject is WHICH BYTES AND WHICH CONFIGURATION a report describes (corpus,
  # components, schema, ticket, output dir, manifest); this file's is WHICH PROGRAMS produced the
  # ratio. That is distinct because every parent check is satisfiable by a session whose corpus,
  # schema, request and configuration are impeccably identified and whose two arms were DIFFERENT
  # BUILDS — and this rig's whole output is a RATIO BETWEEN TWO BINARIES. Four findings share the
  # subject: round 10's M2 (`--no-build` accepted any executable under target/release with neither
  # revision nor digest recorded), round 11's F1 (M2's mtime-vs-HEAD check was mode-blind, so a
  # successful `cargo build` after a script-only commit was refused — its ACCEPT half lives in the
  # primary-path suite above, its "must still refuse under `reused`" half here beside the check), and
  # round 11's F2 (digests taken once before a many-minute session while every rep ran from
  # target/release, where a concurrent rebuild replaces them mid-session — the executables are now
  # COPIED into the session's own measured-bin/ and the copies are what run), and round 21's F5 (F2's
  # copies are SEQUENTIAL and taken after cargo releases its build lock, so a rebuild landing BETWEEN
  # two of them left the session holding binaries from TWO BUILDS while EVERY destination digest still
  # validated — a destination digest hashes what it WROTE, so it proves the copy succeeded and
  # verifies the write against itself; `scripts/perf/ws0_binary_snapshot.py` now captures each source
  # artifact's identity BEFORE the first copy, requires every copy to equal it, and RE-READS every
  # source after the last copy, refusing and naming whichever moved). Hermetic: synthetic
  # session dirs and perf CSVs, a few-KB Data.db hashed with hashlib, and a throwaway `git init` repo
  # in $TMPDIR; no cargo, perf, sudo, taskset, corpus, network or root.
  echo ">>> [$name] bash scripts/tests/test_ws0_binary_provenance.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_binary_provenance.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 binary/build-provenance guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 PREWARM COMPLETENESS — "was the measurement actually WARM?" (#3272 review round 12, F2).
  # Split out of the reporter suite along a responsibility seam: this property is decided at
  # MEASUREMENT time, in `lib-measure.sh`, by classifiers in `ws0_prewarm.py`, over artifacts the
  # reporter never reads (it reads only the one-word `<tag>.prewarm.status` they produce) — so every
  # check in the reporter suite is satisfiable by a session whose statuses all read `ok` and whose
  # prewarms warmed 0.02% of the corpus. Two findings: round 10's F-A (a status taken from the
  # loadgen's EXIT CODE while `--out /dev/null` discarded the only evidence) and round 12's F2 (F-A's
  # replacement was `requests_ok >= 1 AND rows_total >= 1` — a NON-ZERO check where the property is a
  # COMPLETENESS one, plus a bare-scan leg that trusted process success while discarding the bench's
  # own row counts). Both legs now require a FULL corpus scan against the PINNED row count.
  # Hermetic: synthetic prewarm artifacts, a few-KB Data.db hashed with hashlib, a session pin from
  # the shipped writer; no cargo, perf, sudo, taskset, corpus, network, root or driver invocation.
  echo ">>> [$name] bash scripts/tests/test_ws0_prewarm_completeness.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_ws0_prewarm_completeness.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 prewarm-completeness guards); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # ws0 CORPUS-GENERATOR determinism + measurement-corpus pin (#3272, items 8-9).
  # `tools/*` package tests are run by NO other gate component and by no CI lane
  # (ci.yml archives cqlite-core targets; pr-gate is cqlite-core-scoped), so
  # without this hook the determinism oracle would be a test nothing executes —
  # the #1597/#1618 gate-wiring class. What it pins:
  #   * REGENERATE-AND-BYTE-COMPARE: two (and three) independent generations from
  #     the recorded seed, byte-compared file by file over the RAW BYTES read off
  #     disk. The committed corpus's "byte-identical across three generations"
  #     claim was PROSE ONLY; `--verify-against` and the row-content unit tests are
  #     invariant to a whole class of defects (measured: a wall-clock write
  #     timestamp, and a per-generation buffer-reuse tail, each left all 34
  #     pre-existing unit tests GREEN while this test FAILs).
  #   * ANTI-CIRCULARITY: the generator's self-reported `data_db_sha256` is
  #     corroborated against an independently computed hash of the file, so the pin
  #     every future comparison rests on is not a number the generator asserted
  #     about itself (measured: a fabricated constant digest also survived all 34).
  #   * NON-VACUITY: a different SEED must diverge, a one-BYTE flip must be
  #     reported at its offset, and a missing/extra component must be reported — so
  #     the equality is not one that would pass on any two inputs.
  #   * The MEASUREMENT-CORPUS PIN: the in-source constants (4,000,000 rows /
  #     40,000 partitions / 693.69 B/row / sha256 4a903f6f… / digest
  #     0x0390bfbb81a23fa1 over 31,250 batches) must equal the committed
  #     corpus-identity.json field for field, with a perturbation case per field
  #     proving that comparison can FAIL. The full-size verification is
  #     #[ignore]d (it writes ~2.8 GB) and carries the operator command.
  # Cheap and hermetic: 1,000-row corpora in tempdirs (~0.3s total), no datasets,
  # no network. A build failure is a FAILURE, never a skip.
  echo ">>> [$name] cargo test -p ws0-corpus-gen (determinism byte-compare + corpus pin)"
  if ! (cd "$REPO_ROOT" && cargo test -q -p ws0-corpus-gen) >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (ws0 corpus determinism / pin); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # fetch-datasets tracked-fixture guard (#2878): hermetic (throwaway git repo +
  # locally-built partial-overlap tarball + stub curl — no network, and the real
  # test-data/datasets is never touched), no python3 needed, always runs. Pins the
  # capture-before-`rm -rf`/restore-after guard in fetch-datasets.sh: without it a
  # fetch DELETES the git-tracked reference fixtures under test-data/datasets, so
  # this very gate FAILs core-tests + cli-tests on a pristine main and the checkout
  # is left with stageable deletions of tracked files. A failure FAILs the
  # component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_fetch_datasets_tracked_guard.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_fetch_datasets_tracked_guard.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (fetch-datasets tracked-fixture guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # parity-report component self-test (#1338): no python3 needed, always runs. A
  # failure FAILs the component, mirroring the keyspace-scoping guard semantics.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_parity_report.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_parity_report.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (parity-report self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # oom-audit component self-test (#2012): drives nested `agent-gate.sh --only
  # oom-audit` to assert the SKIP (xtask absent) / FAIL (planted violation) /
  # PASS (bounded tree) outcomes. The SKIP case needs no cargo; the FAIL/PASS
  # cases self-report INFO when cargo is unavailable. A failure FAILs the
  # component, mirroring the parity-report/keyspace-scoping guards.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_oom_audit.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_oom_audit.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (oom-audit self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # --delta re-cert self-test (#1892): no python3 needed, always runs (hermetic —
  # classification + entry guards + delta summary emission, no cargo). A failure
  # FAILs the component, mirroring the parity-report/keyspace-scoping guards.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_delta.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_delta.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (delta re-cert self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # python-bindings venv-determinism self-test (#1803): hermetic (PATH-shadowed
  # python3/pip/maturin/python — no real maturin build, no datasets/network),
  # always runs. Proves the import-verify + one-shot self-heal both self-heals a
  # transient venv-resolution miss to PASS AND fails distinctly on a real binding
  # defect. A failure FAILs the component, mirroring the guards above.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_python_bindings_determinism.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_python_bindings_determinism.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (python-bindings venv-determinism self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # cli-tests enumeration self-test (#2039): no python3 needed, always runs
  # (hermetic — asserts the cli-tests component enumerates every cqlite-cli/tests/*.rs
  # target instead of a hardcoded allowlist, and exercises the fail-closed guard; no
  # cargo). A failure FAILs the component, mirroring the delta/parity-report guards.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_cli_tests_enum.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_cli_tests_enum.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (cli-tests enumeration self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # per-gate CPU-budget derivation self-test (#2640): no python3 needed, always
  # runs (hermetic — drives the --cpu-budget hook with a pinned ncpu, asserts the
  # CARGO_BUILD_JOBS + test-threads fair-share derivation from the slot count and
  # the taskpolicy/nice wrapping; no cargo). A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_gate_cpu_budget.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gate_cpu_budget.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (cpu-budget derivation self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # no-wall-clock-assert guard (#2642 / #2369 rule): SKIP-aware (no python3 ->
  # no-op SKIP), always runs its self-test AND scans the `tests/` correctness
  # trees (cqlite-core/tests, cqlite-cli/tests). FAILs the component if a
  # wall-clock THRESHOLD assert is (re)introduced there, mirroring the guards
  # above. NOTE: this covers the `tests/` trees ONLY — `#[cfg(test)]` inline
  # modules under `src/` also run in the default `cargo test` path but are not in
  # the automated scan yet (pre-existing src/ asserts + a tightened regex are
  # deferred to #2705). This prevents reintroduction into the `tests/` trees that
  # #2642 retired.
  echo ">>> [$name] bash scripts/tests/test_check_no_wallclock_asserts.sh && bash scripts/tests/check-no-wallclock-asserts.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_check_no_wallclock_asserts.sh" >>"$log" 2>&1 ||
     ! bash "$REPO_ROOT/scripts/tests/check-no-wallclock-asserts.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (no-wall-clock-assert guard #2642); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # GHA command-injection guard self-test (#2656): no Docker/datasets needed
  # (python3-gated inside the script, always attempts). Regression-guards the
  # roborev-lints component's check-workflow-injection.sh — the real lint runs in
  # the roborev-lints component itself; this proves the lint still catches a planted
  # injection sink and does not false-positive. A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_check_workflow_injection.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_check_workflow_injection.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (GHA injection guard self-test #2656); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # docs-only PR-gate classifier self-test (#2645): no python3 needed, always
  # runs (hermetic — pure-shell allowlist classification + pr-gate.yml structural
  # contract that the required status always reports and the #2644 oracle step is
  # gated fail-closed; no cargo). A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_classify_docs_only.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_classify_docs_only.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (docs-only classifier self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # required-tier aggregation self-test (#2910): no python3/network needed, always
  # runs (hermetic — synthetic check-run fixtures, injected deadlines/poll budgets,
  # a stub sleep; never calls gh). Proves `required` fails closed on a failed,
  # pending, or ABSENT registered gating tier, that re-runs and self-exclusion are
  # decided by run identity rather than name, that a waiver can never excuse a
  # failed tier, and — via an always-exit-0 stub aggregator — that the suite is
  # non-vacuous. SKIP-aware inside the script (no ruby -> SKIP, never silent PASS).
  echo ">>> [$name] bash scripts/tests/test_aggregate_required_tiers.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_aggregate_required_tiers.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (required-tier aggregation self-test #2910); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # gating-tier enrolment self-test (#2910): the other half — the rule that forces
  # every pull_request workflow into .github/ci-gating-tiers.yml (as a tier or an
  # annotated exemption) and rejects a registered tier that cannot emit its context
  # unconditionally. Hermetic synthetic workflow trees; includes an always-pass stub
  # enrolment rule wired through a copy of validate-workflows.rb, so the WIRING is
  # proven non-vacuous, not just the rule. SKIP-aware (no ruby -> SKIP).
  echo ">>> [$name] bash scripts/tests/test_gating_registry_policy.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gating_registry_policy.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (gating-tier enrolment self-test #2910); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # gating workflow-semantics self-test (#2910 round 3): the CHAIN between the two
  # halves above. It models what conclusion a registered tier's gate job actually
  # reports when its run is CANCELLED (an `always()` gate job runs during a
  # cancellation and turns `needs.*.result == cancelled` into a `failure`, which
  # makes the aggregator's supersession grace unreachable), feeds that conclusion
  # into the real registry evaluation, and proves the grace path fires — with an
  # `always()` mutant proving the model discriminates. It also carries the
  # GNU-only-construct lint for this change's shell (macOS is a first-class gate
  # host; #2926's lint is scoped to the gate's own _tree_* functions).
  echo ">>> [$name] bash scripts/tests/test_gating_workflow_semantics.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gating_workflow_semantics.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (gating workflow-semantics self-test #2910); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # TaskCompleted issue-gate hook defusal self-test (#2671, epic #2664): no python3
  # needed, always runs (hermetic — builds a throwaway repo, copies the real hook,
  # and shims the gate + roborev; never runs the real gate). Asserts the hook wires
  # --lite (never the full gate), uses a unique mktemp summary path from the hook's
  # own repo root, fails OPEN on a budget overrun, and runs no roborev. A failure
  # FAILs the component, mirroring the cpu-budget/delta/parity-report guards.
  echo ">>> [$name] bash scripts/tests/test_issue_gate_hook.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_issue_gate_hook.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (issue-gate hook defusal self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # parallel sub-lane scheduling self-test (#2657, epic #2636): no python3 needed,
  # always runs (hermetic — drives the --classify-lanes hook + static invariants;
  # no cargo/git). Asserts the isolatable non-core components run in the concurrent
  # SIDE lane with isolated CARGO_TARGET_DIR, the shared-target cargo components stay
  # on the serial MAIN lane, the serial fallback is intact, and the SUMMARY stays
  # reconstructed in canonical order. A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_sublanes.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_sublanes.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (parallel sub-lane scheduling self-test #2657); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # gate self-test hermeticity lint (#2874): no python3 needed, always runs (pure
  # static scan of scripts/tests/*.sh). FAILs the component if a macOS-unsafe mktemp
  # template (non-trailing X's) or a FIXED `.tmp-*` fixture name is (re)introduced —
  # the residual same-checkout/self-test-fixture sharing that killed full gates of
  # record. A failure FAILs the component, mirroring the guards above.
  echo ">>> [$name] bash scripts/tests/test_gate_selftest_hermetic.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_gate_selftest_hermetic.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (gate self-test hermeticity lint #2874); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # pub-surface guard self-test (#1712): no python3/datasets/network needed, always
  # runs. Proves scripts/ci/check-pub-surface.sh actually FIRES — the consistency
  # assert reds on the pre-#1712 source shape (a bare ungated `pub mod benchmarks;`
  # whose cfg gate hides inside the module file) and a bad argument exits 2. Each
  # negative case substitutes the artifact in its own `git worktree add --detach HEAD`
  # scratch checkout (no test-only seam in the guard); SOURCE-ONLY since the #1712
  # descope, so the 23-case suite is seconds rather than ~125s of rustdoc. It pins
  # every crate-root PARSE shape the scan claims to handle (same-line
  # `#[attr] pub mod x;`, multi-line attrs, trailing comments, block-commented decls,
  # attributes separated from their item by blank/comment lines), since that scan is
  # lexical and its safety rests on the pinned suite; the four SHARED blind spots its
  # two derivations cannot express as a disagreement; and every REFUSAL path of the
  # module-file oracle, each with a green control so a refuse-everything guard cannot
  # satisfy them. A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_pub_surface_guard.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_pub_surface_guard.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (pub-surface guard self-test #1712); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # nested/concurrent-gate isolation regression self-test (#2874): the concurrency
  # phase is python3-SKIP-aware internally; the nested-clobber, explicit-wins, and
  # mid-run summary-integrity phases need no python3 and always run. Proves a nested
  # gate cannot clobber the parent gate of record's summary and that a foreign run-id
  # is caught with a NAMED FAIL (never a bare INCOMPLETE). A failure FAILs the
  # component, mirroring the guards above.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_nested_isolation.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_nested_isolation.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (nested-gate isolation self-test #2874); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # mid-run tree-mutation guard self-test (#2926): no python3/cargo needed, always runs
  # (hermetic — fake checkouts under one per-run mktemp, a stub `cargo`, no compile).
  # Proves a gate whose worktree mutates mid-run cannot certify (MAIN lane, SIDE lane,
  # terminal, --only/--lite/--delta) AND that an unmutated run still does — the two
  # halves are the discrimination proof. A failure FAILs the component.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_tree_integrity.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_tree_integrity.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (tree-integrity self-test #2926); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # …and its PORTABILITY half (#2926 review G1): the same guard re-run against BSD/macOS
  # shims (sed without GNU escapes, stat -f only, a sort(1) with no -z) plus a static lint
  # that FAILs on any GNU-only construct in the tree-integrity code. macOS is a first-class
  # gate host, so a Linux-only token in the guard is a real defect — this is the lane that
  # catches it. Same hermetic shape (fake checkouts, stub cargo, nothing compiles).
  echo ">>> [$name] bash scripts/tests/test_agent_gate_tree_portability.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_tree_portability.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (tree-integrity PORTABILITY self-test #2926); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # …and its PROVENANCE half (#2926 review J1/J2/J3): every mutation-detection path
  # (boundary, SIDE-lane marker, terminal) must publish the SAME labelled `commit:`/
  # `tree-end:` split, the boundary block's component table must cover the mode actually
  # running, and the run's own stdout/stderr redirect target must not be mistaken for a
  # mid-run mutation. Same hermetic shape; each fix carries a discrimination mutant.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_tree_provenance.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_tree_provenance.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (tree-integrity PROVENANCE self-test #2926); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # tools/ crate disposition census (#1716, epic #1688 finding AK5): no
  # python3/Docker/cargo needed, always runs. Every crate under tools/ must be
  # EXPLICITLY classified as CI-wired or as an unwired manual dev tool, and every
  # unwired one must carry a README stating it. The defect it exists for: three
  # tools/ crates were invoked by no workflow, no script and no doc for months
  # while reading as live tooling. `members` globs `tools/*`, so a new crate
  # otherwise joins the workspace with no statement of whether anything runs it,
  # and a deleted README is invisible. Fails closed on an absent/unclassifiable
  # subject; wiredness is RECORDED and reviewed in the diff, never grep-inferred
  # (a grep gets it wrong both ways — see the guard's header). A failure FAILs the
  # component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_tools_crate_disposition.sh; bash scripts/tests/test_tools_crate_disposition_selftest.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_tools_crate_disposition.sh" >>"$log" 2>&1 ||
     ! bash "$REPO_ROOT/scripts/tests/test_tools_crate_disposition_selftest.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (tools/ crate disposition census #1716); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # file-size component-log guard (#3401): hermetic (throwaway git repos under one
  # mktemp, each holding only a copy of the gate script, driven through the real
  # `--only file-size` path — no cargo/python3/datasets/network/Docker, ~2s). The
  # `file-size` component used to write ONLY a bare `file-size.result` (`FAIL 0`),
  # echoing the base ref, the over-threshold advisory list and the exact
  # `path: before -> after (limit N)` arithmetic to STDOUT — i.e. only into gate.log,
  # the one file agents are told never to read. So a `file-size: FAIL` in a pasted
  # SUMMARY named nothing, and every reader re-derived by hand what the component had
  # just computed and discarded — on a FAIL that is routinely EXPECTED. This pins the
  # LOG'S CONTENT (never its mere existence: a zero-byte file would satisfy that and
  # restore the whole defect) across all six paths, PASS ones included. A failure FAILs
  # the component, mirroring the keyspace-scoping guard.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_file_size_log.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_file_size_log.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (file-size component-log guard #3401); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH; selftest truncation reader needs it)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] bash scripts/tests/test_agent_gate_summary.sh; bash scripts/tests/test_agent_gate_notify.sh; bash scripts/tests/test_gate_notify_contract.sh; bash scripts/tests/test_agent_gate_smoke_target_dir.sh; bash scripts/tests/test_gate_concurrency_cap.sh; bash scripts/tests/test_bootstrap_agent_machine.sh; bash scripts/tests/test_perf_capability.sh; bash scripts/tests/test_perf_capability_bootstrap.sh; bash scripts/tests/test_claim_lock.sh; bash scripts/tests/test_claim_heartbeat.sh; bash scripts/flow/tests/claim-resume.test.sh; bash scripts/tests/test_premerge_assert.sh; bash scripts/tests/test_board_label_mirror.sh; bash scripts/tests/test_worker_supervisor.sh; bash scripts/tests/test_gate_failure_mode.sh"
  if bash "$REPO_ROOT/scripts/tests/test_agent_gate_summary.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_agent_gate_notify.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_gate_notify_contract.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_agent_gate_smoke_target_dir.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_gate_concurrency_cap.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_bootstrap_agent_machine.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_perf_capability.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_perf_capability_bootstrap.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_claim_lock.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_claim_heartbeat.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/flow/tests/claim-resume.test.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_premerge_assert.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_board_label_mirror.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_worker_supervisor.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_gate_failure_mode.sh" >>"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# file-size: the campsite-rule ratchet (epic #1116 / #1135). Two parts:
#   advisory  - list every changed .rs file currently over threshold, as a prompt
#               to split it as part of this work.
#   ratchet   - FAIL if a change makes an over-threshold file LARGER (or pushes a
#               file over). You may edit big files freely; you just cannot grow
#               them without either splitting or acknowledging via the override.
# Metric is TOTAL line count (inline tests included) on purpose: the cost being
# controlled is tokens-to-load when an agent reads the file before editing it.
# Degrades to advisory-only (no ratchet) when the base ref can't be resolved.
SRC_LIMIT=800
TEST_LIMIT=1500
# Emit one line to BOTH stdout and the component log (#3401). Everything this component
# computes — the base ref, the advisory list, the `before -> after` growth entries, the
# verdict — used to exist ONLY on stdout, i.e. only in gate.log, the one file agents are
# told never to read; the SUMMARY's `file-size: FAIL` therefore named neither the file nor
# the numbers, and every reader re-derived by hand the arithmetic the component had just
# thrown away. Nothing printed here may go to only one of the two sinks.
# _fs_emit counts appends that FAILED, in `_FS_WRITE_FAILURES` — declared `local` by
# run_file_size and reached here through bash's DYNAMIC SCOPING (deliberately not a global:
# a global could carry a value in from anywhere, #3401 review L3). Tracked rather than
# ignored because the `[ -s "$log" ]` end-state check cannot see a PARTIAL log: a
# filesystem that accepts the first line and rejects the rest leaves a non-empty file
# missing the growth entries — #3401's own defect with extra steps, reported as PASS.
_fs_emit() { # _fs_emit <logfile> <line> [<line>…]  — one output line per argument
  local _fs_log="$1"; shift
  # Zero args must emit NOTHING: `printf '%s\n'` with no operands still prints the format
  # once, i.e. a spurious blank line to BOTH sinks (#3401 review N2).
  [ "$#" -gt 0 ] || return 0
  printf '%s\n' "$@"
  # The append's STATUS is kept, its MESSAGE suppressed: an unwritable log makes the shell
  # print one error per line, ~5 lines of noise burying the diagnostic that has to be
  # unmistakable. ORDER MATTERS and the obvious spelling is WRONG (#3401 review blocker 2):
  # redirections apply LEFT TO RIGHT, so `>>"$log" 2>/dev/null` attempts the open FIRST and
  # bash reports the failure on the still-unredirected stderr. `2>/dev/null` must come
  # first. Verified empirically against a log path that is a directory, not by reasoning.
  printf '%s\n' "$@" 2>/dev/null >>"$_fs_log" ||
    _FS_WRITE_FAILURES=$((_FS_WRITE_FAILURES + 1))
}
run_file_size() {
  local name=file-size
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status=PASS
  # PERSISTENCE (#3401 blocker B): this component now PROMISES a log — the SUMMARY's
  # `logs:` line is the only route an agent has to the ratchet arithmetic. An unverified
  # promise is the original defect one level up (reader follows the pointer, finds
  # nothing, hand-computes line counts again), so both the truncate and the end state are
  # CHECKED and a persistence failure is a FAIL, never a silent PASS.
  local log_persist_err=""
  # Declared HERE so _fs_emit sees it by dynamic scoping and it cannot outlive the call.
  local _FS_WRITE_FAILURES=0
  start=$(date +%s)
  if ! : 2>/dev/null >"$log"; then
    log_persist_err="could not create or truncate it (unwritable path, or not a regular file)"
  fi

  # Base ref: an explicit override (issue #1892 --delta uses the anchor commit),
  # else merge-base with the default branch. If none resolves, we can still do the
  # advisory list but not the growth comparison.
  local base="" base_src="" ref
  if [ -n "${GATE_BASE_OVERRIDE:-}" ]; then
    base="$GATE_BASE_OVERRIDE"
    base_src="GATE_BASE_OVERRIDE"
  else
    for ref in origin/main main origin/master master; do
      if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
        base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] &&
          { base_src="merge-base HEAD $ref"; break; }
      fi
    done
  fi

  # Changed, non-deleted .rs files vs base (committed + working tree). With no
  # base, fall back to changes vs HEAD (uncommitted only).
  local files
  if [ -n "$base" ]; then
    files=$(git diff --name-only --diff-filter=d "$base" -- '*.rs' 2>/dev/null)
  else
    files=$(git diff --name-only --diff-filter=d HEAD -- '*.rs' 2>/dev/null)
  fi

  local -a over=() grew=()
  local f cur lim base_n
  while IFS= read -r f; do
    [ -n "$f" ] && [ -f "$f" ] || continue
    cur=$(wc -l <"$f" | tr -d ' ')
    case "$f" in
      *_test.rs|*_tests.rs|*/tests/*|tests/*|*/benches/*) lim=$TEST_LIMIT ;;
      *) lim=$SRC_LIMIT ;;
    esac
    [ "$cur" -gt "$lim" ] || continue
    over+=("$(printf '%5s/%-4s  %s' "$cur" "$lim" "$f")")
    [ -n "$base" ] || continue
    base_n=$(git show "$base:$f" 2>/dev/null | wc -l | tr -d ' ')
    base_n=${base_n:-0}
    if [ "$cur" -gt "$base_n" ]; then
      grew+=("$(printf '%s: %s -> %s (limit %s)' "$f" "$base_n" "$cur" "$lim")")
    fi
  done <<<"$files"

  local line
  _fs_emit "$log" ">>> [$name] thresholds: src=$SRC_LIMIT test=$TEST_LIMIT (total lines, inline tests included)"
  if [ -n "$base" ]; then
    _fs_emit "$log" ">>> [$name] base ref: $base (via $base_src)"
  fi
  if [ "${#over[@]}" -eq 0 ]; then
    _fs_emit "$log" ">>> [$name] no changed .rs files over threshold"
  else
    _fs_emit "$log" "--- [$name] changed files over threshold (campsite rule — split per epic #1116 / #1135):"
    for line in ${over[@]+"${over[@]}"}; do
      _fs_emit "$log" "      $line"
    done
  fi

  if [ -z "$base" ]; then
    _fs_emit "$log" ">>> [$name] base ref unavailable — growth ratchet skipped (advisory only)"
  elif [ "${#grew[@]}" -gt 0 ]; then
    if [ "${CQLITE_ALLOW_FILE_GROWTH:-0}" = 1 ]; then
      _fs_emit "$log" ">>> [$name] ${#grew[@]} over-threshold file(s) grew; ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1:"
      for line in ${grew[@]+"${grew[@]}"}; do
        _fs_emit "$log" "      $line"
      done
    else
      status=FAIL
      _fs_emit "$log" "--- [$name] FAIL: change makes over-threshold file(s) larger."
      _fs_emit "$log" "    Split per the campsite rule (epic #1116 source / #1135 tests), or, if a split"
      _fs_emit "$log" "    is genuinely out of scope, re-run with CQLITE_ALLOW_FILE_GROWTH=1 to acknowledge:"
      for line in ${grew[@]+"${grew[@]}"}; do
        _fs_emit "$log" "      $line"
      done
      # AC2 (#3401): name the log path in the remedy block. The SUMMARY carries only
      # `logs: <dir>`, so without this the reader has to guess the filename.
      _fs_emit "$log" "    Full detail (thresholds, base ref, every entry above): $log"
    fi
  fi

  end=$(date +%s)

  # What this guarantees, precisely (#3401 review A4): for the case where the LOG FILE
  # cannot be written, the component never reports a silent PASS. It is NOT a general
  # LOG_DIR guarantee — a wholly unwritable LOG_DIR also loses record_result's `.result`,
  # and that falls through to the gate's generic "component produced no result" FAIL, which
  # is fail-closed but carries none of this wording.
  # Three independent signals, because each is blind to a different shape: the truncate
  # check (unwritable path / not a regular file), the append-failure counter (a PARTIAL log
  # — non-empty, so `-s` is satisfied, but missing the growth entries), and the end-state
  # `-s` (created-but-empty, e.g. every write rejected).
  if [ -z "$log_persist_err" ] && [ "$_FS_WRITE_FAILURES" -gt 0 ]; then
    log_persist_err="$_FS_WRITE_FAILURES write(s) to it were rejected, so the log is partial or empty"
  fi
  if [ -z "$log_persist_err" ] && [ ! -s "$log" ]; then
    log_persist_err="the file is absent or empty after writing (filesystem full?)"
  fi
  if [ -n "$log_persist_err" ]; then
    local ratchet_verdict="$status" sib="$LOG_DIR/$name.persistence-error.log" _m _g
    local sib_ok=1 _sib_lines=0 _allow_shown=""
    local -a msg=()
    status=FAIL
    # The diagnostic MUST NOT live on stdout alone: stdout is gate.log, the one file agents
    # are told never to read, and the whole failure mode here is "the log you were pointed
    # at is missing". So it also goes to a NON-CLOBBERING SIBLING in LOG_DIR (the #2874
    # pattern), which is reachable from the SUMMARY's `logs:` line, and the stdout block
    # names that sibling so both routes lead to it (#3401 review blocker B).
    : 2>/dev/null >"$sib" || sib_ok=0
    # WORDING is the ONLY thing that varies by ratchet state; the CONTENT below is
    # unconditional (#3401 review FIX 1).
    if [ "$ratchet_verdict" = FAIL ]; then
      # BOTH failed. Saying "this is NOT a ratchet violation" here would steer the reader
      # away from a REAL growth violation (#3401 review blocker C), so report both.
      msg+=("--- [$name] TWO failures: a REAL size-ratchet violation AND a log-persistence failure.")
      msg+=("    (1) RATCHET (a genuine violation — act on this): the change makes over-threshold")
      msg+=("        file(s) larger. Split per the campsite rule (epic #1116 source / #1135 tests),")
      msg+=("        or re-run with CQLITE_ALLOW_FILE_GROWTH=1 to acknowledge.")
      msg+=("    (2) PERSISTENCE: the diagnostic log could not be written: $log")
      msg+=("        Cause: $log_persist_err")
      msg+=("        Fix the log directory (writable? full?) — that half needs no source split.")
    else
      msg+=("--- [$name] LOG PERSISTENCE FAILURE — this is NOT a campsite-rule / size-ratchet violation.")
      if [ -z "$base" ]; then
        # No base ref means the ratchet never RAN (advisory-only run), so claiming it
        # "computed PASS" would assert a computation that did not happen (#3401 review L1).
        msg+=("    The size ratchet was SKIPPED (base ref unavailable), so no growth comparison was")
        msg+=("    made at all. What failed is PERSISTING the diagnostic log the SUMMARY's 'logs:'")
        msg+=("    line points at: $log")
      else
        msg+=("    The size ratchet itself computed: $ratchet_verdict. What failed is PERSISTING the")
        msg+=("    diagnostic log the SUMMARY's 'logs:' line points at: $log")
      fi
      msg+=("    Cause: $log_persist_err")
      msg+=("    Fix the log directory (writable? full?) and re-run; no source file needs splitting.")
    fi
    # CONTENT — IDENTICAL IN EVERY RATCHET STATE (#3401 review FIX 1). Deriving the
    # sibling's content from the ratchet verdict has now been wrong three times (the FAIL
    # state, then the no-base state, then the PASS / CQLITE_ALLOW_FILE_GROWTH state, where
    # the file names and counts were lost from every reachable artifact — stdout only
    # reaches gate.log). So the arithmetic is restated unconditionally and only the wording
    # above varies.
    msg+=("    --- computed ratchet detail (what the unwritable log would have held) ---")
    msg+=("    thresholds: src=$SRC_LIMIT test=$TEST_LIMIT (total lines, inline tests included)")
    if [ -n "$base" ]; then
      msg+=("    base ref: $base (via $base_src)")
    else
      msg+=("    base ref: unavailable — growth ratchet skipped (advisory only)")
    fi
    if [ "${#over[@]}" -eq 0 ]; then
      msg+=("    over threshold: none")
    else
      msg+=("    over threshold (current/limit  path):")
      for _g in ${over[@]+"${over[@]}"}; do
        msg+=("      $_g")
      done
    fi
    if [ -z "$base" ]; then
      # `grown: none` would assert a COMPLETED comparison that never ran — it contradicts
      # the "ratchet skipped" line above and could conceal real growth (#3401 review
      # item 2, the fourth instance of this class). `none` is reserved for a comparison
      # that finished and found nothing.
      msg+=("    grown: not computed (base unavailable)")
    elif [ "${#grew[@]}" -eq 0 ]; then
      msg+=("    grown: none")
    else
      msg+=("    grown:")
      for _g in ${grew[@]+"${grew[@]}"}; do
        msg+=("      $_g")
      done
      # ITEM 1 / roborev F3: record WHY a populated grown list did not fail the ratchet.
      # Without it the sibling shows grown files beside a non-FAIL verdict with no
      # provenance — and, from the test side, the allowed-growth state emits bytes
      # identical to the FAIL state, so nothing can tell the two apart.
      # "unset" was a CLAIM about a state never determined — the predicate only tested
      # `!= 1`, so `CQLITE_ALLOW_FILE_GROWTH=0` or a typo (`true`, `yes`) was reported as
      # never set, hiding from the reader the one fact that fixes their invocation (#3401
      # review, sixth instance of the compute-claim class). `${VAR+set}` distinguishes the
      # two without a second read. The value is flattened to one line and capped because a
      # multi-line value would break the sibling's landed-line-count check below.
      if [ "${CQLITE_ALLOW_FILE_GROWTH:-0}" = 1 ]; then
        msg+=("    growth allowance: ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1")
      elif [ -n "${CQLITE_ALLOW_FILE_GROWTH+set}" ]; then
        _allow_shown=$(printf '%s' "$CQLITE_ALLOW_FILE_GROWTH" | tr -d '\n\r' | cut -c1-40)
        msg+=("    growth allowance: NOT enabled — CQLITE_ALLOW_FILE_GROWTH is set to '$_allow_shown', expected exactly 1; this IS a ratchet violation")
      else
        msg+=("    growth allowance: NOT enabled — CQLITE_ALLOW_FILE_GROWTH is not set, expected exactly 1; this IS a ratchet violation")
      fi
    fi

    # Write the sibling FIRST and VERIFY WHAT LANDED, then make the claim (#3401 review
    # FIX 2). `sib_ok` from the truncate alone only proved the file could be OPENED: a
    # quota/ENOSPC boundary accepts the open and rejects the writes, leaving an empty or
    # short sibling while stdout asserts the complete block is there. A false pointer is
    # worse than none — it sends the reader to a file that lacks what they were promised.
    if [ "$sib_ok" = 1 ]; then
      for _m in ${msg[@]+"${msg[@]}"}; do
        printf '%s\n' "$_m" 2>/dev/null >>"$sib" || { sib_ok=0; break; }
      done
    fi
    if [ "$sib_ok" = 1 ]; then
      # MUST stay AFTER the write loop: reading a sibling that is a character device such
      # as /dev/full returns zeros forever, so a `wc -l` moved above the loop would HANG.
      # It is safe only because a device like that fails the writes first and leaves
      # sib_ok=0, short-circuiting this read. Do not "optimise" the order.
      _sib_lines=$(wc -l <"$sib" 2>/dev/null | tr -d ' ')
      [ "${_sib_lines:-0}" = "${#msg[@]}" ] || sib_ok=0
    fi
    if [ "$sib_ok" = 1 ]; then
      msg+=("    This block is also written to: $sib")
      printf '%s\n' "    This block is also written to: $sib" 2>/dev/null >>"$sib"
    else
      # Covers all three sub-cases truthfully — truncate failed (no sibling), the append
      # loop broke mid-block (a PARTIAL sibling exists), or the landed line count differs.
      # "the only copy" was false for the middle one (#3401 review item 4).
      msg+=("    (It could NOT be written IN FULL to $sib — stdout carries the complete copy.)")
    fi
    for _m in ${msg[@]+"${msg[@]}"}; do
      printf '%s\n' "$_m"
    done
  fi

  # Terminal verdict, written in TWO halves ON PURPOSE (#3401 blockers B/C) — do NOT tidy
  # these back into one _fs_emit call, either merge direction re-opens one of the two:
  #   * to the LOG *before* record_result, because record_result can TERMINATE the run
  #     (tree-integrity / summary-integrity guards) and the log must still carry the
  #     terminal verdict it promises;
  #   * to STDOUT *after* record_result, because every other component prints in that
  #     order and a gratuitous divergence is its own bug magnet.
  # This LAST log write is deliberately UNVERIFIED — an ACCEPTED RESIDUAL under a lead
  # ruling (Option A, https://github.com/pmcfadin/cqlite/issues/3401#issuecomment-5466376424),
  # not an open question and not a resolved defect: the code here is unchanged, so nothing
  # about the substance was answered. The obvious fix — "attempt and check the terminal
  # append before the final persistence decision" — is not implementable, and the argument
  # is set out here so it can be weighed on its merits instead of re-derived each review
  # round (#3401 review A2, re-raised as job 138 F1):
  #   * CIRCULARITY: this line's CONTENT IS THE VERDICT, and the verdict depends on the
  #     persistence decision. It cannot be written before the decision it reports.
  #   * A POST-WRITE RE-CHECK BUYS NOTHING: a read-back can VERIFY the append without
  #     writing anything, but RECORDING that outcome in the same sink requires a later
  #     write, whose own success is then unverified — the same one-write window, moved
  #     along. Reporting it on STDOUT *is* implementable (a different sink needs no further
  #     log write), but it would only restate what stdout already prints, which is why the
  #     bullet below is the load-bearing one.
  #   * THE LOSS IS BOUNDED: everything #3401 exists for (thresholds, base ref, over/grown
  #     entries) is written AND checked above; a failure here costs only the terminal
  #     verdict LINE, which the SUMMARY carries independently.
  # WHAT WOULD FALSIFY ALL OF THAT — check it before relying on the argument above. The
  # rejection holds ONLY WHILE BOTH of these remain true of this call site:
  #   (a) this line's content IS the component verdict, and
  #   (b) that verdict depends on the persistence decision computed above.
  # Break either — record the verdict earlier, write a provisional marker here and amend
  # it, move the persistence decision after this write — and the circularity is gone, the
  # check becomes implementable, and THIS REJECTION IS VOID: re-examine the finding on its
  # merits rather than citing this comment, which would then be arguing for a constraint
  # that no longer exists.
  printf '%s\n' ">>> [$name] $status ($((end - start))s)" 2>/dev/null >>"$log"
  record_result "$name" "$status" "$((end - start))"
  printf '%s\n' ">>> [$name] $status ($((end - start))s)"
}

# scoped-tests (issue #1821, --lite only): the blast-radius-scoped test component.
# Map each changed path to its cargo package and run ONLY those packages' --lib
# tests plus the diff's new/changed `--test` targets — NOT the full
# core-tests/write/cli/bindings/parity set. Falls back to `cqlite-core --lib` and
# says so when no rust workspace package is in the diff (docs/scripts/bindings-only
# changes). Package detection uses the SAME base-ref resolution as file-size.
#
# Core-src dependent-crate widening (issue #2658): when the diff includes a
# cqlite-core src file, ALSO `cargo test --no-run` (compile-check ONLY) every
# workspace test crate that depends on cqlite-core (integration-tests,
# format-compatibility-tests, cli/flight/root-cqlite test targets). A core API
# change that breaks a SEPARATE test crate's code is otherwise invisible to --lite
# (per-package selection routes only packages the diff itself touches) — the main
# lite-green->full-red wasted-round source. See classify_core_dependent_compile_check.
#
# No-metadata-parser (issue #2658): with NEITHER jq nor python3 present we cannot
# derive ownership/features/the compile-check set, so this component FAILS LOUDLY
# (naming the missing tool) rather than silently narrowing to `cqlite-core --lib`
# (a false-confidence green on minimal boxes). See _scoped_noparser_fail_msg.
#
# Python exception (issue #1893): cqlite-py is a pyo3 cdylib whose
# `cargo test -p cqlite-py` can never link libpython, so a bindings/python diff is
# routed to the fast python tier (maturin develop --profile dev + the not-slow
# pytest tier) instead of the always-failing cargo run. Node (cqlite-node) and
# rust-only diffs are unaffected; a mixed diff runs BOTH the rust-scoped targets
# AND the python tier. See PYTHON_LITE_TIER_CMD / classify_scoped_plan above.
run_scoped_tests() {
  local name=scoped-tests
  local log="$LOG_DIR/$name.log"
  local start end status=PASS
  start=$(date +%s)
  : >"$log"

  local base="" ref
  if [ -n "${GATE_BASE_OVERRIDE:-}" ]; then
    base="$GATE_BASE_OVERRIDE"
  else
    for ref in origin/main main origin/master master; do
      if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
        base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && break
      fi
    done
  fi

  local changed
  if [ -n "$base" ]; then
    changed=$(printf '%s\n%s\n' \
      "$(git diff --name-only "$base"...HEAD 2>/dev/null)" \
      "$(git diff --name-only HEAD 2>/dev/null)")
  else
    changed=$(git diff --name-only HEAD 2>/dev/null)
  fi

  # Package ownership and per-`--test` scoping REQUIRE an authoritative
  # Cargo-metadata parser (jq or python3). Without one we cannot map a path to its
  # owning workspace member, learn a target's required-features, NOR compute the
  # core-dependent compile-check set. The pre-#2658 fallback silently NARROWED to
  # `cqlite-core --lib` — a FALSE-CONFIDENCE green on a minimal box (skips every
  # dependent/integration crate). So when NEITHER parser is present we now FAIL
  # LOUDLY, naming the missing tooling, instead of running a narrowed subset (issue
  # #2658). The AGENT_GATE_TEST_NO_METADATA_PARSER hook forces this branch.
  local have_meta_parser=1
  if [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] || \
     { ! command -v jq >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; }; then
    have_meta_parser=0
  fi

  # No-parser: FAIL LOUDLY (issue #2658). Silently scoping to `cqlite-core --lib`
  # gave a green --lite that had validated NONE of the dependent/integration crates
  # (and, post-#2658, none of the core-src dependent-crate compile-checks) — a
  # false-confidence path on minimal boxes. Emit the shared loud-fail message,
  # mark the component FAIL, and finish here.
  if [ "$have_meta_parser" -eq 0 ]; then
    local msg
    msg=$(_scoped_noparser_fail_msg)
    echo ">>> [$name] FAIL: $msg" | tee -a "$log" >&2
    status=FAIL
    OVERALL=FAIL
    end=$(date +%s)
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
    echo ">>> [$name] $status ($((end - start))s)"
    return
  fi

  # Metadata-derived per-TARGET selection (issue #1821): `pkgindex` is
  # "<manifest_dir>\t<pkg>\t<has_lib>" for every member; `newtests` is every
  # changed --test target ("<pkg>|<testname>|<features>"). Both empty in the
  # no-parser fallback. These drive WHICH --test targets/features run within each
  # routed package — the routing itself (which packages, python tier or not) comes
  # from classify_scoped_plan below.
  local pkgindex="" newtests=""
  if [ "$have_meta_parser" -eq 1 ]; then
    pkgindex=$(_package_index)
    newtests=$(printf '%s\n' "$changed" | classify_test_targets)
  fi

  # has_lib lookup for ANY package name, straight from the metadata index (1 when
  # the package has a lib/rlib target `cargo test --lib` can run, else 0).
  pkg_has_lib() {
    printf '%s\n' "$pkgindex" \
      | awk -F'\t' -v p="$1" '$2 == p { print $3; f = 1; exit } END { if (!f) print 0 }'
  }

  # ROUTING — single source of truth (issue #1893, roborev job 1450): the executor
  # consumes classify_scoped_plan's output — the SAME function the hidden
  # `--classify-scoped-plan` hook exposes and the py-route self-tests assert — so
  # the routing logic (package-set union, cqlite-py exclusion, python-tier flag)
  # exists exactly ONCE. An executor-only edit that re-routed a python diff back to
  # `cargo test -p cqlite-py` is now impossible without also changing the asserted
  # plan. Plan lines: "rust-pkg: <pkg>" and "python-tier: <cmd>".
  local plan line
  plan=$(printf '%s\n' "$changed" | classify_scoped_plan)
  local -a pkgs=()
  local python_diff=0
  while IFS= read -r line; do
    case "$line" in
      "rust-pkg: "*) pkgs+=("${line#rust-pkg: }") ;;
      "python-tier: "*) python_diff=1 ;;
    esac
  done <<<"$plan"
  local scoped_note=""
  [ "${#pkgs[@]}" -gt 0 ] && scoped_note="${pkgs[*]}"
  if [ "$python_diff" -eq 1 ]; then
    scoped_note="${scoped_note:+$scoped_note + }python tier ($PYTHON_LITE_TIER_CMD)"
  fi
  # Fall back to the cqlite-core --lib default ONLY when the diff selected nothing
  # at all — NOT when a python-only diff already routed to the python tier.
  if [ "${#pkgs[@]}" -eq 0 ] && [ "$python_diff" -eq 0 ]; then
    pkgs=(cqlite-core)
    scoped_note="cqlite-core --lib (default; no rust workspace package in the diff)"
  fi
  echo ">>> [$name] blast-radius packages: $scoped_note"

  # Union a comma-list of features into a newline-set (Bash 3.2-safe dedup).
  # The separator is placed BETWEEN elements (not trailing): `add_features` is
  # called via `featset=$(add_features ...)`, and command substitution strips
  # trailing newlines, so a trailing-newline scheme would glue the first element
  # of the next call onto the last existing element (e.g. "write-support" +
  # "delta-export" -> "write-supportdelta-export"). Prepending "$set"+newline
  # only when non-empty keeps every element on its own line regardless.
  add_features() {
    local set=$1 list=$2 x oldifs=$IFS nl
    nl=$'\n'
    IFS=,
    for x in $list; do
      [ -n "$x" ] || continue
      printf '%s\n' "$set" | grep -qxF "$x" || set="${set:+${set}${nl}}${x}"
    done
    IFS=$oldifs
    printf '%s' "$set"
  }

  # Bash 3.2 under `set -u` treats "${pkgs[@]}" of an EMPTY array as unbound, and a
  # python-only diff now legitimately leaves pkgs empty (python tier covers it), so
  # expand with the ${arr[@]+"${arr[@]}"} guard rather than unconditionally.
  local p rest tname feats
  for p in ${pkgs[@]+"${pkgs[@]}"}; do
    local -a args=(test -p "$p")
    local featset=""
    # cqlite-core lib tests need cli-helpers (matches the full gate's core-tests).
    [ "$p" = cqlite-core ] && featset=$(add_features "$featset" cli-helpers)
    # Lib presence comes from Cargo metadata (no src/lib.rs probing). A package
    # with no lib target runs only its changed --test targets (issue #1821).
    local haslib
    haslib=$(pkg_has_lib "$p")
    [ "$haslib" -eq 1 ] && args+=(--lib)
    local -a stems=()
    # Collect every changed --test target this package owns AND union each
    # target's required-features so it is compiled with the features it needs —
    # never invoked feature-less (issue #1821 finding 2).
    while IFS= read -r key; do
      [ -n "$key" ] || continue
      case "$key" in
        "$p|"*)
          rest=${key#*|}          # "<name>|<features>"
          tname=${rest%%|*}
          feats=${rest#*|}        # "" when the target has no required-features
          [ "$feats" = "$rest" ] && feats=""
          stems+=(--test "$tname")
          featset=$(add_features "$featset" "$feats")
          ;;
      esac
    done <<<"$newtests"
    # Bash 3.2 under `set -u` treats "${stems[@]}" of an EMPTY array as unbound,
    # so only expand it when non-empty (count expansion is always safe).
    [ "${#stems[@]}" -gt 0 ] && args+=("${stems[@]}")
    # Pass the unioned required-features (if any) so feature-gated targets
    # (write-support / delta-export / duckdb-tests / ...) actually compile.
    local featjoin
    featjoin=$(printf '%s' "$featset" | awk 'NF{ printf (n++?",":"") $0 }')
    [ -n "$featjoin" ] && args+=(--features "$featjoin")
    # A test-only crate with no changed --test target has nothing runnable to
    # scope to; compile-check it (--no-run) rather than run its whole (slow) suite.
    if [ "$haslib" -eq 0 ] && [ "${#stems[@]}" -eq 0 ]; then
      args+=(--no-run)
    fi
    echo ">>> [$name] cargo ${args[*]}"
    if ! cargo "${args[@]}" >>"$log" 2>&1; then
      status=FAIL
      OVERALL=FAIL
    fi
  done

  # Core-src dependent-crate compile-check (issue #2658): a cqlite-core src change
  # can break the test code of a SEPARATE test crate (integration-tests,
  # format-compatibility-tests, cli/flight/root-cqlite test targets) without
  # touching that crate's files — invisible to the per-package selection above (it
  # only routes packages the diff itself touched), so --lite went green while the
  # full gate went red (the main lite-green->full-red wasted-round source). When the
  # diff includes a cqlite-core src file we `cargo test --no-run` (compile ONLY, no
  # slow run) every dependent test crate so a broken test compile surfaces at --lite
  # time. classify_core_dependent_compile_check is the SINGLE source (same lines the
  # --classify-core-dependent-compile-check self-test hook asserts).
  local ccplan ccpkg
  ccplan=$(printf '%s\n' "$changed" | classify_core_dependent_compile_check)
  if [ -n "$ccplan" ]; then
    while IFS= read -r ccpkg; do
      ccpkg=${ccpkg#compile-check-pkg: }
      [ -n "$ccpkg" ] || continue
      # --all-targets compile-checks every test/bin/example of the crate; features
      # of a per-package target are resolved by cargo (targets whose
      # required-features are off are simply skipped — no costly duckdb/otel build).
      echo ">>> [$name] core-src diff: compile-check dependent crate: cargo test -p $ccpkg --all-targets --no-run"
      if ! cargo test -p "$ccpkg" --all-targets --no-run >>"$log" 2>&1; then
        status=FAIL
        OVERALL=FAIL
      fi
    done <<<"$ccplan"
  fi

  # Python tier (issue #1893): the REAL python signal --lite runs for a
  # bindings/python diff instead of the always-libpython-link-failing
  # `cargo test -p cqlite-py`. Reuses the full gate's persistent venv
  # (target/agent-gate-venv). Both phases `eval` the PYTHON_LITE_*_CMD component
  # constants that also compose the advertised PYTHON_LITE_TIER_CMD plan string
  # (roborev job 1449) — plan/executor drift is structurally impossible.
  #
  # SKIP vs FAIL split (roborev job 1449, Low): TOOLCHAIN failures (venv creation,
  # pip install — e.g. offline, or the maturin build environment itself missing) get
  # a loud SKIP-note, never FAIL — --lite must stay usable offline, and a toolchain
  # gap is not a code failure (clippy in this same lite run still compiles cqlite-py,
  # and the full gate's python-bindings component hard-fails). A PYTEST failure is a
  # real code failure and FAILs. NOTE: a python-diff --lite round costs a maturin
  # compile of the extension (seconds warm via the persistent venv + sccache,
  # ~1-3 min cold).
  if [ "$python_diff" -eq 1 ]; then
    if ! command -v python3 >/dev/null 2>&1; then
      echo ">>> [$name] python binding diff but no python3 on PATH — SKIP python tier (run the full gate)"
      PYTHON_TIER_NOTE="python-tier: SKIPPED (no python3 on PATH) — python-binding diff NOT validated by this lite run; run the full gate"
    else
      local venv="$REPO_ROOT/target/agent-gate-venv"
      local pbv_rc active_venv_file active_venv
      echo ">>> [$name] python tier: $PYTHON_LITE_TIER_CMD (venv: $venv)"
      # Build + VERIFY the extension imports, self-healing a stale/half-built
      # editable install once (issue #1803, symmetric to run_python_bindings).
      # rc 1 (venv create/pip install) and rc 4 (build TOOLCHAIN absent — no
      # cargo/rustc on PATH, roborev round-2 Finding A) are genuine TOOLCHAIN
      # gaps → SKIP (offline, NOT a code failure). rc 2 (`maturin develop`
      # exited non-zero WITH cargo+rustc present) is a REAL COMPILE ERROR of our
      # bindings, not a toolchain gap — masking it as SKIP would hide exactly the
      # class of failure #1803 exists to surface, so it FAILs distinctly
      # (mirrors the full gate's hard-FAIL on a maturin build error). rc 3
      # (imports fail even after a clean-venv rebuild) is a real binding DEFECT
      # → FAIL distinctly. rc 0 → run pytest against the WRITTEN-BACK active
      # venv (possibly a private self-heal venv — Finding B; the shared $venv is
      # never torn down).
      active_venv_file=$(mktemp "${TMPDIR:-/tmp}/agent-gate-active-venv.XXXXXX")
      RUN_SLOW_TESTS=0 bash "$GATE_SELF" --python-build-verify "$venv" "$PYTHON_LITE_MATURIN_CMD" "$active_venv_file" >>"$log" 2>&1
      pbv_rc=$?
      active_venv=$(cat "$active_venv_file" 2>/dev/null); [ -n "$active_venv" ] || active_venv="$venv"
      if [ "$pbv_rc" -eq 2 ]; then
        status=FAIL
        OVERALL=FAIL
        echo ">>> [$name] python tier FAIL (maturin develop failed — a real build/compile failure of our bindings, not a toolchain gap; see $log)"
        PYTHON_TIER_NOTE="python-tier: FAIL (maturin develop failed — real build/compile failure)"
      elif [ "$pbv_rc" -eq 3 ]; then
        status=FAIL
        OVERALL=FAIL
        echo ">>> [$name] python tier FAIL (cqlite._cqlite did not import after clean-venv rebuild — real binding defect, not a venv-resolution miss)"
        PYTHON_TIER_NOTE="python-tier: FAIL (cqlite._cqlite did not import after clean-venv rebuild — real binding defect)"
      elif [ "$pbv_rc" -eq 4 ]; then
        echo ">>> [$name] python tier SKIP (build toolchain absent — no cargo/rustc on PATH, NOT a code failure; see $log; run the full gate when the toolchain is available)"
        PYTHON_TIER_NOTE="python-tier: SKIPPED (toolchain: cargo/rustc absent) — python-binding diff NOT validated by this lite run; run the full gate"
      elif [ "$pbv_rc" -ne 0 ]; then
        echo ">>> [$name] python tier SKIP (venv/pip toolchain setup failed — offline or toolchain gap, NOT a code failure; see $log; run the full gate when the toolchain is available)"
        PYTHON_TIER_NOTE="python-tier: SKIPPED (toolchain: venv/pip setup failed — offline?) — python-binding diff NOT validated by this lite run; run the full gate"
      elif RUN_SLOW_TESTS=0 PY_PYTEST_CMD="$PYTHON_LITE_PYTEST_CMD" bash -c '
          set -euo pipefail
          . "'"$active_venv"'/bin/activate"
          eval "$PY_PYTEST_CMD"' >>"$log" 2>&1; then
        echo ">>> [$name] python tier PASS"
        PYTHON_TIER_NOTE="python-tier: PASS ($PYTHON_LITE_TIER_CMD)"
      else
        status=FAIL
        OVERALL=FAIL
        echo ">>> [$name] python tier FAIL (pytest failure — a real code failure)"
        PYTHON_TIER_NOTE="python-tier: FAIL (pytest failure — a real code failure)"
      fi
      # Clean up a private self-heal venv (never the shared $venv) so heal
      # venvs don't accumulate under target/.
      [ "$active_venv" != "$venv" ] && rm -rf "$active_venv"
      rm -f "$active_venv_file"
    fi
  fi

  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 60 lines of $log ---"
    tail -60 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  echo ">>> [$name] $status ($((end - start))s)"
}

# aggregate_lite_components (issue #2121): the --lite OVERALL aggregator, mirroring
# the full gate's per-component reconstruction (see the `for _c in "${COMPONENTS[@]}"`
# loop) and run_delta's (the `for c in file-size fmt` loop). file-size + fmt + clippy
# run in the FOREGROUND under --lite and record ONLY to their per-component `.result`
# files — run_component/run_file_size are display-only, they never touch OVERALL, the
# same single-source contract the full gate relies on. run_lite previously iterated
# NAMES directly, but NAMES held ONLY the scoped-tests entry run_scoped_tests appended,
# so a fmt/clippy/file-size FAIL neither appeared in the block NOR flipped OVERALL — a
# false-green lite report (the #2121 bug). This rebuilds NAMES/STATUSES/TIMES in
# canonical order (file-size fmt clippy, then the scoped-tests entry) and sets
# OVERALL=FAIL on ANY of those components that FAILED.
#
# PRESENT-ONLY (not fail-closed on a missing result): a component that did not run is
# skipped, never force-failed. This preserves the lenient `--lite --only <c>` contract
# (e.g. bootstrap-agent-machine.sh runs `--lite --only fmt`, which skips file-size and
# clippy). Fail-open is not a risk here because these components run in the foreground
# and always reach record_result; the only way a `.result` is absent is a deliberate
# --only skip. The full gate keeps its own fail-closed missing-result guard for its
# backgrounded pool, which is where a crash-before-record_result can actually happen.
aggregate_lite_components() {
  local -a LN=() LS=() LT=()
  local c rf st secs i
  for c in file-size fmt clippy roborev-lints; do
    rf="$LOG_DIR/$c.result"
    [ -f "$rf" ] || continue   # not run (e.g. --only skip) — do not add, do not fail
    st=""; secs=""
    read -r st secs < "$rf" || true
    [ -n "$st" ] || { st=FAIL; secs=0; }
    LN+=("$c"); LS+=("$st"); LT+=("${secs}s")
    [ "$st" = FAIL ] && OVERALL=FAIL
  done
  # Preserve the scoped-tests (+ any python/node) entries run_scoped_tests appended to
  # NAMES; run_scoped_tests already set OVERALL=FAIL itself on a test failure.
  if [ "${#NAMES[@]}" -gt 0 ]; then
    for i in "${!NAMES[@]}"; do
      LN+=("${NAMES[$i]}"); LS+=("${STATUSES[$i]}"); LT+=("${TIMES[$i]}")
    done
  fi
  NAMES=("${LN[@]+"${LN[@]}"}")
  STATUSES=("${LS[@]+"${LS[@]}"}")
  TIMES=("${LT[@]+"${LT[@]}"}")
}

# run_lite (issue #1821): the FAST ITERATION gate. Runs file-size + fmt +
# FULL-workspace clippy + blast-radius-scoped tests, emits a DISTINCTLY-labeled
# LITE summary, and EXITS — it never falls through to the full-gate flow below, so
# the no-flag path is completely unchanged. It is NOT the gate of record.
run_lite() {
  echo
  echo "==================================================================="
  echo "  AGENT-GATE --lite  :  FAST ITERATION GATE — *NOT* THE GATE OF RECORD"
  echo "  Runs: file-size + fmt + scoped workspace clippy + roborev-lints + blast-radius-scoped tests."
  echo "  It SKIPS core-tests, write/cli, bindings, parity, smoke, etc."
  echo "  Before merge you MUST run the full  scripts/agent-gate.sh  and it must"
  echo "  PASS — its ==== AGENT-GATE SUMMARY ==== block is the ONLY run that counts."
  echo "==================================================================="
  echo

  run_file_size
  run_component fmt cargo fmt --all --check
  run_component clippy run_clippy
  run_component roborev-lints run_roborev_lints_cmd
  run_scoped_tests

  # Aggregate the foreground components (file-size/fmt/clippy) into NAMES + OVERALL
  # BEFORE building the summary (issue #2121). Without this, a fmt/clippy/file-size
  # FAIL was invisible in the block and left OVERALL=PASS → a false-green lite report.
  aggregate_lite_components

  # #2926: --lite is MORE scope-sensitive than the full gate, not less — run_scoped_tests
  # derives its blast radius from `git diff` read MID-RUN, and --lite is the mode that
  # runs during the fix rounds (exactly when a second writer is editing). Terminal
  # capture here; forces OVERALL=FAIL on a mid-run mutation.
  _tree_finalize || true

  declare -a SUMMARY_META=()
  # #2926 review C1: the stamp comes from the VERIFIED terminal capture above, never
  # from a fresh `git rev-parse`/`git status` here — that emit-time read is the original
  # defect (a HEAD move between finalize and emit certified an unverified sha).
  _tree_commit_meta
  SUMMARY_META+=("$TREE_COMMIT_LINE")
  SUMMARY_META+=("lite-scope: file-size fmt clippy roborev-lints scoped-tests (full gate NOT run — run it once before merge)")
  # Python-tier verdict marker (roborev job 1450): when a python-binding diff was
  # in scope, the block carries the tier's verdict — a SKIPPED marker makes a
  # "green but validated nothing" block detectable from the block alone.
  [ -n "$PYTHON_TIER_NOTE" ] && SUMMARY_META+=("$PYTHON_TIER_NOTE")
  SUMMARY_META+=("$(accelerators_line)")
  SUMMARY_META+=("$(cpu_budget_line)")
  _tree_meta_array   # #2926
  SUMMARY_META+=("${TREE_META_LINES[@]}")
  local i
  for i in "${!NAMES[@]}"; do
    SUMMARY_META+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
  done
  # job-2108 MED: --lite/--delta terminals obey the SAME no-clobber contract as the full gate
  # (falls through to emit_summary when no live peer owns the path; forces FAIL + non-zero exit
  # via SUMMARY_WRITE_FAILED when one does).
  _emit_terminal_summary "$OVERALL" "${SUMMARY_META[@]}" || true

  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
    exit 1
  fi
  case "$OVERALL" in
    PASS) exit 0 ;;
    *) exit 1 ;;
  esac
}

# run_delta_node_tests <newline-allowed-paths> (issue #2081): run the changed
# bindings/node/__test__ jest tests against the ALREADY-BUILT native module. No-op
# when no node test changed. Scopes jest to the changed *.test.js by basename; a
# changed non-*.test.js helper/setup runs the WHOLE suite. Appends a node-tests
# verdict to NAMES; a jest failure sets OVERALL=FAIL. Build-readiness is enforced by
# run_delta's up-front node-build refusal, so this only runs when node is ready.
run_delta_node_tests() {
  local allowed="$1" targets f start end status n_targets whole=0
  targets=$(printf '%s\n' "$allowed" | _delta_node_targets)
  [ -n "$(printf '%s' "$targets" | awk 'NF')" ] || return 0
  local -a filters=()
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in
      *.test.js) filters+=("$(basename "$f")") ;;
      *) whole=1 ;;
    esac
  done <<<"$targets"
  n_targets=$(printf '%s\n' "$targets" | awk 'NF' | wc -l | tr -d ' ')
  echo ">>> [node-tests] jest on $n_targets changed bindings/node/__test__ file(s) (already-built module; no cargo build)"
  start=$(date +%s)
  local log jest_filter=""
  log=$(mktemp "${TMPDIR:-/tmp}/agent-gate-nodedelta.XXXXXX")
  [ "$whole" -eq 0 ] && jest_filter="${filters[*]}"
  if CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" JEST_FILTER="$jest_filter" bash -c '
      set -uo pipefail
      cd "'"$REPO_ROOT"'/bindings/node"
      # Regenerate the JS loader from the ALREADY-BUILT .node (no cargo build).
      node scripts/generate-loader.mjs >/dev/null 2>&1 || true
      # shellcheck disable=SC2086  # intentional word-split: multiple jest path filters
      if [ -n "${JEST_FILTER:-}" ]; then npx jest $JEST_FILTER; else npx jest; fi' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL; OVERALL=FAIL
    echo "--- [node-tests] FAILED; last 40 lines ---"; tail -40 "$log"; echo "--- end of node-tests output ---"
  fi
  rm -f "$log"
  end=$(date +%s)
  NAMES+=("node-tests"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  DELTA_EXECUTORS="${DELTA_EXECUTORS:+$DELTA_EXECUTORS }node-tests($n_targets)"
  echo ">>> [node-tests] $status ($((end - start))s)"
}

# run_delta_shell_selftests <newline-allowed-paths> (issue #2081): execute the changed
# scripts/tests/*.sh self-test scripts verbatim. No-op when none changed. Appends a
# shell-selftests verdict to NAMES; a failing script sets OVERALL=FAIL.
run_delta_shell_selftests() {
  local allowed="$1" targets f start end status n_targets
  targets=$(printf '%s\n' "$allowed" | _delta_shell_targets)
  [ -n "$(printf '%s' "$targets" | awk 'NF')" ] || return 0
  local -a tarr=()
  while IFS= read -r f; do [ -n "$f" ] && tarr+=("$f"); done <<<"$targets"
  n_targets=${#tarr[@]}
  echo ">>> [shell-selftests] executing $n_targets changed scripts/tests/*.sh"
  start=$(date +%s)
  if _run_shell_selftest_files "${tarr[@]}"; then status=PASS; else status=FAIL; OVERALL=FAIL; fi
  end=$(date +%s)
  NAMES+=("shell-selftests"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  DELTA_EXECUTORS="${DELTA_EXECUTORS:+$DELTA_EXECUTORS }shell-selftests($n_targets)"
  echo ">>> [shell-selftests] $status ($((end - start))s)"
}

# run_delta <anchor> (issue #1892): TEST/DOCS-ONLY RE-CERTIFICATION. Verifies the
# diff anchor..HEAD (committed + working tree) touches ONLY test/docs files —
# FAIL-CLOSED, naming any offending production file — then re-certifies with
# file-size + fmt + the diff's changed test targets, emits a DISTINCTLY-labeled
# DELTA summary, and EXITS (never falls through to the full-gate flow). It is NOT
# the gate of record: the gate of record remains the full agent-gate.sh PASS at
# the anchor, with the nightly gate.yml deep-check as the standing backstop.
run_delta() {
  local anchor="$1"
  echo
  echo "==================================================================="
  echo "  AGENT-GATE --delta  :  TEST/DOCS-ONLY RE-CERTIFICATION — *NOT* THE GATE OF RECORD"
  echo "  Anchor (full-gate PASS commit): $anchor"
  echo "  Verifies the diff anchor..HEAD touches ONLY test/docs files, then runs:"
  echo "  file-size + fmt + the changed test targets. It SKIPS clippy, core-tests,"
  echo "  write/cli, bindings, parity, smoke, etc. — those were validated by the"
  echo "  full gate at the anchor, and the nightly gate.yml deep-check re-runs the"
  echo "  FULL gate on main. Record BOTH the anchor's full SUMMARY and this DELTA"
  echo "  block in the PR."
  echo "==================================================================="
  echo

  # Resolve the anchor to a full commit sha. A bad/unknown anchor is a usage error
  # (RESULT: ERROR) — we cannot re-certify a diff against a commit that does not
  # resolve.
  local anchor_sha
  if ! anchor_sha=$(git rev-parse --verify -q "${anchor}^{commit}" 2>/dev/null) || [ -z "$anchor_sha" ]; then
    echo "--- [delta] ERROR: anchor '$anchor' does not resolve to a commit." >&2
    echo "    Pass the commit the full gate PASSed at (a sha, tag, or ref)." >&2
    _tree_meta_array   # #2926
    emit_summary ERROR \
      "delta-anchor: $anchor (UNRESOLVED)" \
      "$(accelerators_line)" \
      "${TREE_META_LINES[@]}" \
      "error: anchor does not resolve to a commit — cannot re-certify"
    exit 2
  fi

  # Anchor full-gate run-id: from --anchor-run-id, else read from the anchor
  # summary file if given. The anchor summary file MUST be a FULL-gate PASS block:
  # a lite/delta block cannot anchor a delta re-cert (that would let a fast run
  # masquerade as the gate of record). Refuse loudly if it is not.
  local anchor_run_id="${DELTA_ANCHOR_RUN_ID:-}"
  if [ -z "$anchor_run_id" ] && [ -n "$DELTA_ANCHOR_SUMMARY_FILE" ]; then
    if [ ! -f "$DELTA_ANCHOR_SUMMARY_FILE" ]; then
      echo "--- [delta] ERROR: --anchor-summary-file '$DELTA_ANCHOR_SUMMARY_FILE' not found." >&2
      _tree_meta_array   # #2926
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "${TREE_META_LINES[@]}" \
        "error: --anchor-summary-file not found: $DELTA_ANCHOR_SUMMARY_FILE"
      exit 2
    fi
    if ! grep -qF "==== AGENT-GATE SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null \
       || grep -qF "==== AGENT-GATE LITE SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null \
       || grep -qF "==== AGENT-GATE DELTA SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null; then
      echo "--- [delta] ERROR: --anchor-summary-file is not a FULL-gate SUMMARY block." >&2
      echo "    A delta re-cert must anchor to a full agent-gate.sh PASS, not a lite/delta run." >&2
      _tree_meta_array   # #2926
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "${TREE_META_LINES[@]}" \
        "error: anchor summary is not a full-gate SUMMARY block (lite/delta cannot anchor a delta)"
      exit 2
    fi
    if ! grep -qE '^RESULT: PASS' "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null; then
      echo "--- [delta] ERROR: --anchor-summary-file did not record RESULT: PASS." >&2
      echo "    A delta re-cert must anchor to a full-gate PASS." >&2
      _tree_meta_array   # #2926
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "${TREE_META_LINES[@]}" \
        "error: anchor summary RESULT is not PASS — cannot anchor a delta re-cert"
      exit 2
    fi
    anchor_run_id=$(grep -E '^run-id:' "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null | head -1 | sed 's/^run-id:[[:space:]]*//')
  fi
  [ -n "$anchor_run_id" ] || anchor_run_id="(not provided)"

  # Changed files anchor..HEAD (committed) plus the working tree. Deletions ARE
  # included (no --diff-filter=d) and classified by path via _delta_is_allowed_path:
  # a deleted docs/test file stays allowed, but a deleted production file (script,
  # workflow, Cargo.*, src, config) becomes offending and REFUSES the re-cert. This
  # is required for the fail-closed guarantee — dropping deletions would let a
  # production-file removal produce a green DELTA block. Dedup, drop blanks.
  #
  # --no-renames on BOTH invocations is REQUIRED (roborev job 3338): with git
  # rename detection on (diff.renames), a rename collapses to only the DESTINATION
  # path, so renaming a production file to an allowed *.md/test path would be
  # classified solely by the destination and slip a green delta while hiding the
  # production-file removal. --no-renames enumerates a rename as delete-old +
  # add-new, so the old production path is classified and (non-allowed) REFUSES.
  local changed
  changed=$(printf '%s\n%s\n' \
    "$(git diff --name-only --no-renames "$anchor_sha" HEAD 2>/dev/null)" \
    "$(git diff --name-only --no-renames HEAD 2>/dev/null)" \
    | awk 'NF && !seen[$0]++')

  # Precompute the executed-target .rs allow-set ONCE (cargo metadata called a
  # single time) before the partition loop, so _delta_is_allowed_path consults an
  # authoritative cached set per .rs path rather than static globs (roborev job
  # 3327). Empty when no metadata parser is available → every .rs refuses.
  _DELTA_RS_ALLOWED_SET=$(printf '%s\n' "$changed" | _delta_rs_target_paths)

  # Partition into allowed (test/docs) and offending (everything else). FAIL-CLOSED.
  local f allowed="" offending=""
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if _delta_is_allowed_path "$f"; then
      allowed="${allowed}${f}"$'\n'
    else
      offending="${offending}${f}"$'\n'
    fi
  done <<<"$changed"

  local n_allowed n_offending
  n_allowed=$(printf '%s' "$allowed" | awk 'NF' | wc -l | tr -d ' ')
  n_offending=$(printf '%s' "$offending" | awk 'NF' | wc -l | tr -d ' ')

  # Build the delta-files meta lines (indented list), or a placeholder when empty.
  local -a file_meta=()
  if [ "$n_allowed" -eq 0 ] && [ "$n_offending" -eq 0 ]; then
    file_meta+=("delta-files (0): (no changes anchor..HEAD)")
  else
    file_meta+=("delta-files ($n_allowed allowed / $n_offending offending):")
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      file_meta+=("      [test/docs] $f")
    done <<<"$allowed"
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      file_meta+=("      [PRODUCTION] $f")
    done <<<"$offending"
  fi

  # anchor_meta[0] is a PLACEHOLDER (#2926 review C1): the `commit:` stamp is filled in
  # at each emit site FROM THE VERIFIED TERMINAL CAPTURE (_tree_commit_meta), never from
  # a fresh `git rev-parse` here or at emit time. A site that forgot to stamp would
  # publish this visible placeholder rather than an unverified sha.
  local -a anchor_meta=(
    "commit: (unstamped — no verified capture reached this emit; #2926)"
    "delta-anchor: $anchor_sha (full-gate PASS commit)"
    "delta-anchor-run-id: $anchor_run_id"
    "gate-of-record: full agent-gate.sh run at $anchor_sha (this DELTA re-certifies a test/docs-only diff; it is NOT a substitute for the full gate)"
    "nightly-backstop: .github/workflows/gate.yml deep-check re-runs the FULL gate on main (gate job = scoped clippy; full --all-features clippy in the parallel clippy-full job, #2662)"
  )

  # FAIL-CLOSED: any production file in the diff refuses the delta re-cert. Name the
  # offending files and tell the caller to run the full gate.
  if [ "$n_offending" -gt 0 ]; then
    echo "--- [delta] REFUSED: the diff anchor..HEAD changes files --delta cannot re-certify:" >&2
    while IFS= read -r f; do [ -n "$f" ] && printf '      %s\n' "$f" >&2; done <<<"$offending"
    echo "    A fresh FULL gate is required: scripts/agent-gate.sh" >&2
    echo "    --delta re-certifies ONLY what it can EXECUTE: rust files that ARE a Cargo" >&2
    echo "    --test target (authoritative via cargo metadata, not globs), python binding" >&2
    echo "    tests (bindings/python/tests/), docs (*.md anywhere), node jest tests" >&2
    echo "    (bindings/node/__test__/), and shell self-tests (scripts/tests/*.sh). A .rs" >&2
    echo "    that is not a --test target (nested helper mods, src *_test(s).rs," >&2
    echo "    scripts/*.rs, the excluded fuzz/ crate) and any other file require the full gate." >&2
    _tree_finalize || true   # #2926
    _tree_commit_meta; anchor_meta[0]="$TREE_COMMIT_LINE"   # #2926 review C1
    _tree_meta_array
    emit_summary "$(_tree_result REFUSED)" \
      "${anchor_meta[@]}" \
      "delta-scope: file-size fmt scoped-tests node-tests shell-selftests (NOT RUN — refused before execution)" \
      "$(accelerators_line)" \
      "${TREE_META_LINES[@]}" \
      "${file_meta[@]}" \
      "refusal: $n_offending file(s) --delta cannot re-certify — a full gate is required (--delta executes only rust files that ARE a Cargo --test target [authoritative, not glob-based], bindings/python/tests, *.md docs, bindings/node/__test__/ jest tests, and scripts/tests/*.sh; everything else needs the full gate)"
    [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || { echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2; exit 1; }
    exit 1
  fi

  # FAIL-CLOSED (issue #2081): --delta ALLOWS bindings/node/__test__/* on the premise
  # that run_delta_node_tests runs jest against the ALREADY-BUILT native module.
  # --delta must NEVER build with cargo, so if node test files are in scope but the
  # module is not built (or node/npm is unavailable), the changed node tests cannot be
  # re-certified — a PASS DELTA block would be a vacuous green. Refuse up front (before
  # any executor) so it stays hermetic.
  local node_targets
  node_targets=$(printf '%s\n' "$allowed" | _delta_node_targets)
  if [ -n "$(printf '%s' "$node_targets" | awk 'NF')" ] && ! _delta_node_build_ready; then
    echo "--- [delta] REFUSED: bindings/node/__test__/* changed but the node native module is not built (or node/npm unavailable)." >&2
    echo "    --delta never builds with cargo; build it first (cd bindings/node && npm run build)" >&2
    echo "    or run a fresh FULL gate: scripts/agent-gate.sh" >&2
    _tree_finalize || true   # #2926
    _tree_commit_meta; anchor_meta[0]="$TREE_COMMIT_LINE"   # #2926 review C1
    _tree_meta_array
    emit_summary "$(_tree_result REFUSED)" \
      "${anchor_meta[@]}" \
      "delta-scope: file-size fmt scoped-tests node-tests shell-selftests (NOT RUN — refused before execution)" \
      "$(accelerators_line)" \
      "${TREE_META_LINES[@]}" \
      "${file_meta[@]}" \
      "refusal: node bindings/node/__test__/* changed but the native module is not built (--delta never builds with cargo) — run 'cd bindings/node && npm run build' or a full gate (scripts/agent-gate.sh)"
    [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || { echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2; exit 1; }
    exit 1
  fi

  echo ">>> [delta] diff anchor..HEAD is test/docs-only ($n_allowed file(s)); re-certifying"

  # Re-certify: file-size + fmt + the changed test targets, all scoped to the
  # anchor..HEAD diff (GATE_BASE_OVERRIDE points file-size + scoped-tests at the
  # anchor). run_file_size and run_component write result files; run_scoped_tests
  # appends to NAMES and sets OVERALL on failure.
  GATE_BASE_OVERRIDE="$anchor_sha"
  run_file_size
  run_component fmt cargo fmt --all --check
  run_scoped_tests
  # Executors that always ran (rust/python scoped tests), plus the #2081 node + shell
  # executors (each a no-op unless its file class changed). DELTA_EXECUTORS feeds the
  # DELTA SUMMARY's `delta-executors:` line so the block names what re-certified.
  DELTA_EXECUTORS="scoped-tests(rust/python)"
  run_delta_node_tests "$allowed"
  run_delta_shell_selftests "$allowed"

  # Reconstruct file-size + fmt verdicts from their result files (so a fmt or
  # file-size FAIL fails the delta and shows in the block), then append the
  # scoped-tests entry run_scoped_tests already pushed onto NAMES.
  local -a DN=() DS=() DT=()
  local c rf st secs
  for c in file-size fmt; do
    rf="$LOG_DIR/$c.result"
    if [ -f "$rf" ]; then
      st=""; secs=""
      read -r st secs < "$rf" || true
      [ -n "$st" ] || { st=FAIL; secs=0; }
      DN+=("$c"); DS+=("$st"); DT+=("${secs}s")
      [ "$st" = FAIL ] && OVERALL=FAIL
    else
      DN+=("$c"); DS+=(FAIL); DT+=("0s"); OVERALL=FAIL
    fi
  done
  # Append the scoped-tests entry run_scoped_tests pushed onto NAMES. Guard the
  # KEYS expansion with a count check: the `"${!arr[@]+"${!arr[@]}"}"` empty-array
  # idiom that works for VALUES does NOT work for the keys form `${!arr[@]}` — bash
  # reads `${!NAMES[@]+...}` as INDIRECT expansion and errors ("invalid variable
  # name") on the array's string contents, aborting run_delta before emit_summary.
  # `${#NAMES[@]}` is set -u-safe even when empty.
  local i
  if [ "${#NAMES[@]}" -gt 0 ]; then
    for i in "${!NAMES[@]}"; do
      DN+=("${NAMES[$i]}"); DS+=("${STATUSES[$i]}"); DT+=("${TIMES[$i]}")
    done
  fi

  # FAIL-CLOSED (issue #1892, roborev job 3333): --delta ALLOWS bindings/python/tests/*
  # on the premise that the #1893 python tier actually RUNS them (via run_scoped_tests).
  # But --delta does NOT run clippy, so there is NO compile/test backstop. If the python
  # tier was SKIPPED (python3 missing, or venv/pip/maturin setup failed) while a python
  # test file is in the allowed set, the changed bindings/python/tests/* files were NEVER
  # re-certified — emitting RESULT: PASS would be an unsound green. Refuse instead.
  #   * A python-test PASS or FAIL means the tier ran (PYTHON_TIER_NOTE starts PASS/FAIL);
  #     a FAIL already set OVERALL=FAIL above and flows through as RESULT: FAIL.
  #   * A SKIPPED/never-ran note (or empty) means it did NOT run → REFUSE here.
  #   * A docs/rust-only delta has NO python test file in the allowed set, so python3
  #     being absent is irrelevant and the delta must still PASS normally.
  if printf '%s\n' "$allowed" | _delta_python_tier_gap "$PYTHON_TIER_NOTE"; then
    echo "--- [delta] REFUSED: bindings/python/tests/* changed but the python tier did NOT run (${PYTHON_TIER_NOTE:-python-tier: not run})." >&2
    echo "    --delta cannot re-certify changed bindings/python/tests/* files without the python tier;" >&2
    echo "    a fresh FULL gate is required: scripts/agent-gate.sh" >&2
    _tree_finalize || true   # #2926
    _tree_commit_meta; anchor_meta[0]="$TREE_COMMIT_LINE"   # #2926 review C1
    declare -a SUMMARY_META=()
    SUMMARY_META+=("${anchor_meta[@]}")
    SUMMARY_META+=("delta-scope: file-size fmt scoped-tests (python tier REQUIRED but did NOT run — re-cert incomplete)")
    SUMMARY_META+=("${PYTHON_TIER_NOTE:-python-tier: NOT RUN — python-binding tests NOT validated by this delta run}")
    SUMMARY_META+=("$(accelerators_line)")
    _tree_meta_array
    SUMMARY_META+=("${TREE_META_LINES[@]}")
    SUMMARY_META+=("${file_meta[@]}")
    for i in "${!DN[@]}"; do
      SUMMARY_META+=("$(printf '%-18s %s (%s)' "${DN[$i]}:" "${DS[$i]}" "${DT[$i]}")")
    done
    SUMMARY_META+=("refusal: python tier skipped — cannot re-certify changed bindings/python/tests/* files; run the full gate (scripts/agent-gate.sh)")
    emit_summary "$(_tree_result REFUSED)" "${SUMMARY_META[@]}"
    [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || { echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2; exit 1; }
    exit 1
  fi

  # #2926: terminal capture before the block is built. --delta's entire premise is that
  # `anchor..HEAD` is test/docs-only, classified from git MID-RUN — if the tree moves
  # after that classification, the set that was classified is not the set that was
  # executed and the fail-closed classification is void.
  _tree_finalize || true
  # #2926 review C1: stamp `commit:` from the VERIFIED terminal capture above.
  _tree_commit_meta; anchor_meta[0]="$TREE_COMMIT_LINE"

  declare -a SUMMARY_META=()
  SUMMARY_META+=("${anchor_meta[@]}")
  SUMMARY_META+=("delta-scope: file-size fmt scoped-tests (test/docs-only re-cert; clippy/core/write/cli/bindings/parity/smoke NOT run — see gate-of-record)")
  # #2081: name the executors that actually RAN this re-cert (scoped-tests always; the
  # node/shell executors only when their file class changed).
  SUMMARY_META+=("delta-executors: ${DELTA_EXECUTORS:-scoped-tests(rust/python)} (executors that RAN this re-cert)")
  # Python-tier verdict marker (issue #1893, roborev job 1450): a python-test-only
  # delta diff routes scoped-tests to the maturin+pytest tier, and its verdict —
  # especially a SKIP (offline/toolchain), where the block could otherwise read
  # PASS while the python diff was NOT validated — must be detectable from the
  # DELTA block alone, exactly as run_lite renders it for the LITE block.
  [ -n "$PYTHON_TIER_NOTE" ] && SUMMARY_META+=("$PYTHON_TIER_NOTE")
  SUMMARY_META+=("$(accelerators_line)")
  SUMMARY_META+=("$(cpu_budget_line)")
  _tree_meta_array   # #2926
  SUMMARY_META+=("${TREE_META_LINES[@]}")
  SUMMARY_META+=("${file_meta[@]}")
  for i in "${!DN[@]}"; do
    SUMMARY_META+=("$(printf '%-18s %s (%s)' "${DN[$i]}:" "${DS[$i]}" "${DT[$i]}")")
  done
  # job-2108 MED: --lite/--delta terminals obey the SAME no-clobber contract as the full gate
  # (falls through to emit_summary when no live peer owns the path; forces FAIL + non-zero exit
  # via SUMMARY_WRITE_FAILED when one does).
  _emit_terminal_summary "$OVERALL" "${SUMMARY_META[@]}" || true

  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
    exit 1
  fi
  case "$OVERALL" in
    PASS) exit 0 ;;
    *) exit 1 ;;
  esac
}

# ---- Machine-wide full-gate concurrency cap (issue #1825) -------------------
# A cross-process bounded semaphore around the FULL gate of record ONLY. At most N
# full `agent-gate.sh` runs execute machine-wide at once; excess invocations BLOCK
# (queue) for a slot — they NEVER fail from the cap. EXEMPT (never queued):
#   * --lite runs (issue #1821): cheap fmt+clippy+scoped tests, must stay instant.
#   * --only PARTIAL runs: they don't count as the gate AND are used by nested
#     tooling self-tests (a capped parent runs `agent-gate.sh --only ...` as a
#     child), so capping them could self-deadlock the queue.
#   * --emit-summary-selftest / hidden hooks: they exit earlier, never reaching here.
#
# Mechanism (SIGKILL-safe by construction): N slot lockfiles under a SHARED,
# machine-wide (NOT per-checkout) dir, each guarded by a non-blocking fcntl.flock.
# A tiny background daemon (scripts/lib/gate_slot_daemon.py) acquires ONE slot,
# signals us via a ready file, then HOLDS the lock while polling this gate's
# liveness; it releases the slot when the gate exits. Crucially the daemon is a
# SEPARATE process that opens the lock fd AFTER it is forked, so the gate's heavy
# children (cargo, nextest, ...) never inherit the lock -- a SIGKILL of the gate
# frees the slot within one poll interval even while orphaned children run on. (An
# fd held by the gate shell itself would be inherited by cargo and keep the slot
# locked after a SIGKILL, defeating stale-slot reaping -- hence the daemon.)
#
# N default: max(2, floor((ncpu-2)/4)) -- a conservative fraction of cores that
# still lets a couple of gates run on a small box; overridable via
# CQLITE_GATE_MAX_CONCURRENCY. Slots dir: $CQLITE_GATE_SLOTS_DIR (default
# ${TMPDIR:-/tmp}/cqlite-gate-slots). Poll interval: $CQLITE_GATE_POLL_SECS
# (default 2). The cap is skipped (with a loud stderr note) when python3 or the
# daemon is unavailable, and can be force-disabled with CQLITE_GATE_DISABLE_CAP=1.
# Non-interactive callers block cleanly (waiting on the daemon), never spin-fail.

# N is resolved by _gate_max_concurrency (defined early, near the core-budget
# derivation for #2640) so the per-gate core budget and this machine-wide cap
# share a single source of truth for the slot count.

# PID of the background slot daemon (empty when the cap is inactive for this run).
GATE_SLOT_DAEMON_PID=""

# Release the held slot by terminating the daemon (which closes its lock fd). Run
# from the EXIT trap. Guarded to fire ONLY in the main gate shell: a backgrounded
# `( ... ) &` pool subshell also runs the inherited EXIT trap on its own exit, and
# must NOT tear down the parent's slot. BASHPID (bash 4+) differs from $$ inside a
# subshell; on bash 3.2 (no BASHPID, no pool subshells) it defaults equal, so the
# guard is a no-op there and only the real gate exit releases the slot.
# shellcheck disable=SC2329  # invoked indirectly via `trap '_gate_release_slot' EXIT`
_gate_release_slot() {
  [ "${BASHPID:-$$}" = "$$" ] || return 0
  [ -n "${GATE_SLOT_DAEMON_PID:-}" ] || return 0
  kill "$GATE_SLOT_DAEMON_PID" 2>/dev/null || true
  GATE_SLOT_DAEMON_PID=""
}

# Block until this full-gate run holds one of N machine-wide slots, then return so
# the gate proceeds while the daemon keeps the slot held in the background. No-op
# for the exempt run classes above. Fail-open (cap disabled) if python3/daemon are
# missing or the daemon dies before acquiring -- the gate must never be un-runnable
# because of the cap.
acquire_gate_slot() {
  [ "$LITE" -eq 1 ] && return 0
  [ "$DELTA" -eq 1 ] && return 0
  [ -n "$ONLY" ] && return 0
  [ "${CQLITE_GATE_DISABLE_CAP:-0}" = 1 ] && return 0
  if ! command -v python3 >/dev/null 2>&1; then
    echo "agent-gate: python3 unavailable -- full-gate concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  local n dir poll daemon ready
  n=$(_gate_max_concurrency)
  dir="${CQLITE_GATE_SLOTS_DIR:-${TMPDIR:-/tmp}/cqlite-gate-slots}"
  poll="${CQLITE_GATE_POLL_SECS:-2}"
  daemon="$REPO_ROOT/scripts/lib/gate_slot_daemon.py"
  if [ ! -f "$daemon" ]; then
    echo "agent-gate: slot daemon $daemon missing -- concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  if ! mkdir -p "$dir" 2>/dev/null; then
    echo "agent-gate: cannot create slot dir $dir -- concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  ready="$LOG_DIR/gate-slot.ready"
  rm -f "$ready" 2>/dev/null || true
  # Start the background lock-holder for THIS gate (pid $$). It writes $ready once
  # it owns a slot and holds it until this gate exits. Its std fds are detached to
  # /dev/null so this long-lived background child can NEVER hold the gate's stdout
  # pipe open and truncate a streamed SUMMARY under an until-EOF reader (#1175).
  python3 "$daemon" --slots-dir "$dir" --slots "$n" --gate-pid "$$" \
    --ready-file "$ready" --poll-secs "$poll" </dev/null >/dev/null 2>&1 &
  GATE_SLOT_DAEMON_PID=$!
  trap '_gate_release_slot' EXIT
  # Block until the daemon signals acquisition, printing the queued notice ONCE
  # after a short grace (so an immediately-free slot stays quiet). If the daemon
  # dies before acquiring, fail open rather than hang the gate forever.
  local printed=0 waited=0
  while [ ! -f "$ready" ]; do
    if ! kill -0 "$GATE_SLOT_DAEMON_PID" 2>/dev/null; then
      echo "agent-gate: slot daemon exited before acquiring -- cap DISABLED for this run (#1825)" >&2
      GATE_SLOT_DAEMON_PID=""
      return 0
    fi
    if [ "$printed" -eq 0 ] && [ "$waited" -ge 3 ]; then
      echo "waiting for gate slot ($n in use)…" >&2
      printed=1
    fi
    waited=$(( waited + 1 ))
    sleep 0.2
  done
  [ "$printed" -eq 1 ] && echo "agent-gate: gate slot acquired -- proceeding (#1825)" >&2
}

# Test-only stub (issue #1825 concurrency self-test): when CQLITE_GATE_STUB_RUNDIR
# is set, the gate acquires a real slot (subject to the cap + exemptions above),
# advertises "I am working" by dropping a per-PID marker file, sleeps
# CQLITE_GATE_STUB_SLEEP seconds, then exits 0 WITHOUT running any real component.
# This lets scripts/tests/test_gate_concurrency_cap.sh exercise the machine-wide
# semaphore (queueing at N, --lite exemption, SIGKILL slot release) hermetically,
# without running actual gate work. Never triggered in normal use.
if [ -n "${CQLITE_GATE_STUB_RUNDIR:-}" ]; then
  acquire_gate_slot   # self-exempts for --lite / --only
  mkdir -p "$CQLITE_GATE_STUB_RUNDIR" 2>/dev/null || true
  _stub_marker="$CQLITE_GATE_STUB_RUNDIR/holding.$$"
  : > "$_stub_marker" 2>/dev/null || true
  sleep "${CQLITE_GATE_STUB_SLEEP:-2}"
  rm -f "$_stub_marker" 2>/dev/null || true
  exit 0
fi

# Hidden self-test hook (issue #2121): exercise the --lite OVERALL aggregation in
# isolation. Seeds the per-component .result files + the scoped-tests NAMES entry the
# real run_lite would produce, runs the SAME aggregate_lite_components, emits the LITE
# summary, and exits on OVERALL exactly as run_lite does — so the regression test can
# pin "any component FAIL ⇒ RESULT: FAIL + non-zero exit" without a cargo build.
if [ "$LITE_AGG_SELFTEST" -eq 1 ]; then
  # Seed per-component result files from AGENT_GATE_TEST_LITE_RESULTS="name:status ...".
  # shellcheck disable=SC2086  # intentional word-split over the space-separated pairs
  for _pair in ${AGENT_GATE_TEST_LITE_RESULTS:-}; do
    printf '%s 0\n' "${_pair#*:}" > "$LOG_DIR/${_pair%%:*}.result"
  done
  # Seed the scoped-tests entry run_scoped_tests appends (and flip OVERALL as it does).
  _scoped_st="${AGENT_GATE_TEST_LITE_SCOPED:-PASS}"
  NAMES+=("scoped-tests"); STATUSES+=("$_scoped_st"); TIMES+=("0s")
  [ "$_scoped_st" = FAIL ] && OVERALL=FAIL
  aggregate_lite_components
  declare -a SUMMARY_META=()
  SUMMARY_META+=("commit: selftest branch: selftest dirty: no")
  SUMMARY_META+=("lite-scope: file-size fmt clippy scoped-tests (aggregate selftest)")
  SUMMARY_META+=("$(accelerators_line)")
  SUMMARY_META+=("$(cpu_budget_line)")
  # #2926: synthetic tree identity (no git state needed for the aggregation self-test).
  SUMMARY_META+=("$TREE_START_LINE" "$TREE_END_LINE" "$TREE_INTEGRITY_LINE")
  for _i in "${!NAMES[@]}"; do
    SUMMARY_META+=("$(printf '%-18s %s (%s)' "${NAMES[$_i]}:" "${STATUSES[$_i]}" "${TIMES[$_i]}")")
  done
  # job-2108 MED: --lite/--delta terminals obey the SAME no-clobber contract as the full gate
  # (falls through to emit_summary when no live peer owns the path; forces FAIL + non-zero exit
  # via SUMMARY_WRITE_FAILED when one does).
  _emit_terminal_summary "$OVERALL" "${SUMMARY_META[@]}" || true
  [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || exit 1
  case "$OVERALL" in PASS) exit 0 ;; *) exit 1 ;; esac
fi

# --lite (issue #1821): run the fast subset and EXIT before the full-gate flow.
# Kept fully separate from the full-gate execution below so the no-flag path is
# byte-for-byte unchanged. --lite is EXEMPT from the #1825 cap (never queued).
if [ "$LITE" -eq 1 ]; then
  run_lite
fi

# --delta (issue #1892): test/docs-only re-certification. Verifies anchor..HEAD is
# test/docs-only (fail-closed), runs file-size + fmt + changed test targets, and
# EXITS before the full-gate flow. EXEMPT from the #1825 cap (never queued).
if [ "$DELTA" -eq 1 ]; then
  run_delta "$DELTA_ANCHOR"
fi

# Machine-wide full-gate concurrency cap (issue #1825): block here until a slot is
# free, so at most N full gates run at once across worktrees + the root checkout.
# --lite already returned above; --only PARTIAL runs self-exempt inside.
acquire_gate_slot

# #2926: the full gate's certification window begins HERE — after the (possibly very
# long) queue for that slot, when work actually begins. See _tree_recapture_after_slot.
_tree_recapture_after_slot

# file-size runs first and needs no dataset, so it executes before the dataset
# preflight (which exits early when data is missing).
run_file_size

# Components that actually read SSTable datasets (Data.db) at run time. These are
# the only ones the dataset preflight must guard. Wrongly skipping the preflight
# for a dataset-dependent component is the #646 hazard, so this set must stay
# complete.
#   needs datasets: core-tests, tombstones-scan, scan-offload-guard,
#     work-counters-guard (the wiring-evidence tests scan real Data.db fixtures),
#     memory-budget (dhat lane reads real Data.db and fails closed on empty),
#     integration-tests, write-tests, smoke (read Data.db / golden fixtures),
#     cli-tests (issue #2039: now ENUMERATES every cqlite-cli/tests/*.rs target,
#     several of which — one_shot_real_data_integration_tests, repl_real_data_tests,
#     integration_sstable_tests, read_sstable_stdout_tests, … — read real Data.db;
#     was formerly dataset-free when it ran only the 3-target allowlist), and
#     python-bindings — the pytest suite resolves CQLITE_DATASETS_ROOT and calls
#     skip_if_no_datasets() (bindings/python/tests/conftest.py), so with data
#     absent its dataset-backed coverage *silently skips* and the suite can still
#     report PASS. python-bindings is therefore in this set (#1175 finding 2): the
#     preflight must FAIL loudly rather than let a skipped suite pass green — the
#     same #646 failure mode that motivated guarding the Rust dataset suites.
#     Added by #1699: flight-tests (its --lib unit suite reads real Data.db — e.g.
#     stats.rs's real-fixture test, which SKIPS with a printed notice when
#     CQLITE_DATASETS_ROOT is unset, exactly the silent-skip shape this set guards;
#     it stays enrolled after the #3384 narrowing for that reason) and legacy-heuristics
#     (several of its derived cqlite-core/tests targets — sstable_discovery_*,
#     parsing_improvements_test — read real Data.db).
#   dataset-free (deliberately NOT guarded): fmt, clippy, file-size (operate on
#     source text),
#     parity-report (renders the manifest + diffs the committed report; reads no
#     CQLITE_DATASETS_ROOT, no Data.db — issue #1338),
#     delivery-telemetry + tooling-tests (pure shell/stdlib tool tests; the lone
#     CQLITE_DATASETS_ROOT in test_agent_gate_summary.sh *sets an empty* root to
#     exercise the preflight, it consumes no real data), minimal-build (a cargo
#     build plus a compile-only `cargo test --lib --no-run`; no tests run, no
#     data — issue #1978), the two #1699 feature-isolation lanes
#     (feature-iso-parquet / feature-iso-delta-scan: `cargo test --lib --no-run`,
#     compile-only — nothing executes, so no fixture can be consumed), and format-compat. format-compat is excluded (#1175
#     finding 1): its sole target (cargo test -p format-compatibility-tests,
#     tests/format-compatibility) is pure in-memory byte-level format-compliance
#     assertions with hardcoded vectors — it reads no CQLITE_DATASETS_ROOT and no
#     Data.db — so guarding it just made `--only format-compat` falsely fail the
#     preflight when datasets are absent.
DATASET_COMPONENTS="core-tests tombstones-scan scan-offload-guard work-counters-guard memory-budget integration-tests write-tests cli-tests python-bindings smoke flight-tests legacy-heuristics"

# selected_needs_datasets: true iff at least one SELECTED component reads datasets.
# With no --only, every component runs, so it's always true. With --only, it's true
# only when the selection intersects DATASET_COMPONENTS — so e.g. `--only
# tooling-tests` or `--only fmt` skips the (dataset-requiring) preflight entirely.
selected_needs_datasets() {
  [ -z "$ONLY" ] && return 0
  local sel comp
  for sel in ${ONLY//,/ }; do
    for comp in $DATASET_COMPONENTS; do
      [ "$sel" = "$comp" ] && return 0
    done
  done
  return 1
}

# Dataset preflight: dataset-dependent components must FAIL loudly when data is
# missing, never silently pass on a skipped suite (the #646 failure mode). Run it
# only when the selected component set actually needs datasets (#1175 finding 2),
# so dataset-free selections like `--only tooling-tests` are not blocked by it.
#
# The find/wc over the dataset mount is computed INSIDE this branch (#1175
# finding 2): a dataset-free selection must not traverse $CQLITE_DATASETS_ROOT at
# all (it can be slow or hang on an unavailable mount). When the preflight is
# skipped, DATA_COUNT stays the placeholder below and feeds the summary directly.
DATA_COUNT="(preflight skipped — no dataset-dependent component selected)"
if selected_needs_datasets; then
  DATA_COUNT=$(find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" 2>/dev/null | wc -l | tr -d ' ')
  # FULL-gate canonical-corpus guard (issue #2078): fail closed when the FETCHED
  # validation corpus (test_basic/…) is absent, even though the committed byte-parity
  # references keep DATA_COUNT > 0 in a fresh worktree. A no-op for --only (kept
  # lenient by the DATA_COUNT==0 check below) and --lite (already returned). Honors
  # AGENT_GATE_ALLOW_MISSING_FIXTURES=1 (restores SKIP + stamps MISSING_FIXTURES_MARKER
  # into the SUMMARY). May emit a FAIL SUMMARY and exit 1.
  apply_fixture_preflight
  # FULL-gate COMMITTED-SCHEMAS guard (issue #3148): the SSTable corpus above is only
  # half the fixture contract — the dataset-backed components must also read the
  # committed CQL schemas that decode it. Runs AFTER the corpus guard so a run missing
  # both still reports the #2078 cause first (the corpus is the fetched half an
  # operator must act on). Checkout-relative, so this is a cheap belt-and-braces
  # assert; no opt-out. May emit a FAIL SUMMARY and exit 1.
  apply_schemas_preflight
  # Historical hard preflight: zero Data.db at all is an error. For the FULL gate this
  # is already handled by apply_fixture_preflight above (an empty root has no
  # test_basic corpus either), so restrict it to --only — that way the opt-out can
  # restore SKIP on an empty-root FULL gate while --only stays byte-identical (test 5b).
  if [ "$DATA_COUNT" -eq 0 ] && [ -n "$ONLY" ]; then
    echo "agent-gate: no Data.db files under $CQLITE_DATASETS_ROOT/sstables" >&2
    echo "agent-gate: fetch them first: bash test-data/scripts/fetch-datasets.sh" >&2
    # Overwrite the caller-known recovery file with a FAIL block stamped with this
    # run's run-id (#1175 finding 2). The startup sentinel already guarantees no
    # stale PASS survives; this makes the early exit explicit for a caller reading
    # the recovery path.
    _tree_meta_array   # #2926
    emit_summary FAIL \
      "preflight: FAIL (no Data.db files under $CQLITE_DATASETS_ROOT/sstables)" \
      "${TREE_META_LINES[@]}" \
      "hint: bash test-data/scripts/fetch-datasets.sh"
    exit 1
  fi
else
  echo ">>> dataset preflight: skipped (no selected component needs datasets: --only $ONLY)"
fi

# CI dataset pins, for the CI-parity check (issue #719): local validation must
# target the same asset CI uses.
PIN_FILE=".github/workflows/sstabledump-parity-gate.yml"
PINS=$(grep -E 'DATASET_(TAG|ASSET|SHA256):' "$PIN_FILE" 2>/dev/null | sed 's/^ *//' | tr '\n' ' ' || echo "unavailable")

# ---- issue #1737: nextest core-tests + bounded parallel component pool ----
# core-tests: the 67%-of-wall-clock execution floor. Under nextest it parallelizes
# across test binaries + cores; a separate `cargo test --doc` pass preserves
# doctest coverage nextest does not run. Falls back to plain `cargo test`.
run_core_tests() {
  # Always exclude the legacy blob-fallback test (needs the non-default feature).
  # Under the static-golden mandate (CQLITE_SKIP_DOCKER_TESTS != 0, the gate
  # default) ALSO exclude the live Docker parity tests by name substring; setting
  # CQLITE_SKIP_DOCKER_TESTS=0 restores them. nextest excludes by filter DSL, the
  # cargo fallback by libtest --skip (both keep the doctest pass so no coverage is
  # dropped).
  local nx_filter='not test(test_legacy_format_allows_blob_fallback_with_feature)'
  local -a skip_args=(--skip test_legacy_format_allows_blob_fallback_with_feature)
  if [ "${CQLITE_SKIP_DOCKER_TESTS:-1}" != 0 ]; then
    nx_filter="$nx_filter and not test(under_cassandra5_sstabledump)"
    skip_args+=(--skip under_cassandra5_sstabledump)
  fi
  # Per-gate core budget (#2640): cap the core-tests long pole at this gate's
  # fair share of cores so N concurrent gates never oversubscribe the box.
  # nextest reads --test-threads; the cargo-test fallback takes it after `--`.
  if [ "$NEXTEST" -eq 1 ]; then
    run_component core-tests bash -c '
      cargo nextest run --package cqlite-core --features cli-helpers --test-threads "$1" -E "$2" &&
      cargo test --doc --package cqlite-core --features cli-helpers -- "${@:3}"' \
      cqlite-agent-gate "$GATE_TEST_THREADS" "$nx_filter" "${skip_args[@]}"
  else
    run_component core-tests cargo test --package cqlite-core --features cli-helpers -- \
      --test-threads "$GATE_TEST_THREADS" "${skip_args[@]}"
  fi
}

# run_scan_offload_guard_cmd: the scan-offload-guard component's command (issue
# #1594 roborev). Runs the INTEGRATION offload/admission guards, then a SECOND
# invocation that runs the cqlite-core LIB unit tests WITH the scan-offload-probe
# feature so the fan-out DEADLOCK regression guard actually EXECUTES. That guard —
# `fanout_over_more_generations_than_cap_completes` in
# storage::sstable::issue_1594_fanout_deadlock_test — is a LIB `#[cfg(all(test,
# feature = "scan-offload-probe"))]` module (it must be, to reach the pub(crate)
# `manager.table_readers`), so no `--test <name>` integration run ever compiles it;
# without this lib run the deadlock fix would have no auto-executing gate guard
# (the #1597/#1618 gate-wiring class). The filters keep it fast (only the admission
# + deadlock lib tests) and it inherits CQLITE_DATASETS_ROOT; the deadlock test
# skips-not-fails when the compressed fixture is absent.
run_scan_offload_guard_cmd() {
  cargo test --package cqlite-core \
    --features cli-helpers,scan-offload-probe \
    --test issue_1143_scan_offload_thread \
    --test issue_1333_scan_scratch_reuse \
    --test issue_1589_window_drain_bytes \
    --test issue_1593_io_offload_thread \
    --test issue_1593_mmap_scan_parity \
    --test issue_1594_scan_admission_bound \
    --test issue_2063_eager_merge_admission_bound \
    && cargo test --package cqlite-core --lib \
    --features cli-helpers,scan-offload-probe \
    -- scan_admission issue_1594_fanout_deadlock
}

# run_arrow_parity_guard_cmd: the arrow-parity-guard component's command (issue
# #1495, AE1). Runs the Arrow accessor-hoist byte-identity parity test WITH the
# `arrow` feature it requires. The test is `#![cfg(feature = "arrow")]` with Cargo
# `required-features = ["arrow"]`, so it is compiled+run by NO other gate/CI lane
# (core-tests is cli-helpers-only; pr-gate's --lib --all-features skips tests/).
# Fails CLOSED on a vacuous 0-run: after the test binary runs we assert the cargo
# "test result: ... N passed" line reports N > 0, so a renamed, removed, or
# feature-skipped target reads as FAIL — never a hollow PASS. Builds in-memory
# QueryRows, so it needs no datasets.
run_arrow_parity_guard_cmd() {
  local out
  out=$(cargo test --package cqlite-core --features arrow \
    --test issue_1495_arrow_accessor_parity 2>&1) || { echo "$out"; return 1; }
  echo "$out"
  # Require at least one test to have actually run (guard against a vacuous
  # required-features skip that cargo reports as success with 0 tests).
  local passed
  passed=$(echo "$out" | sed -n 's/^test result: ok\. \([0-9][0-9]*\) passed.*/\1/p' | tail -1)
  if [ -z "$passed" ] || [ "$passed" -lt 1 ]; then
    echo "arrow-parity-guard: FAIL — 0 tests ran (target skipped/absent, not a real PASS)" >&2
    return 1
  fi
}

# _pool_selected <name>: honor the --only filter when building the launch list.
_pool_selected() {
  [ -z "$ONLY" ] && return 0
  grep -qw "$1" <<<"${ONLY//,/ }"
}

# dispatch_component <name>: run exactly one gate component with the SAME command,
# package, and feature selection as the historical sequential gate. Each branch
# records its verdict to $LOG_DIR/<name>.result (see record_result), so it is safe
# to run in a backgrounded subshell.
dispatch_component() {
  case "$1" in
    fmt) run_component fmt cargo fmt --all --check ;;
    clippy) run_component clippy run_clippy ;;
    roborev-lints) run_component roborev-lints run_roborev_lints_cmd ;;
    core-tests) run_core_tests ;;
    tombstones-scan) run_component tombstones-scan cargo test --package cqlite-core \
      --features write-support,cli-helpers,tombstones \
      --test issue_1085_tombstones_full_scan_parity ;;
    scan-offload-guard) run_component scan-offload-guard run_scan_offload_guard_cmd ;;
    work-counters-guard) run_component work-counters-guard cargo test --package cqlite-core \
      --features write-support,cli-helpers,state_machine,work-counters \
      --test issue_1566_read_work_counters \
      --test issue_1573_readat_positional \
      --test issue_1585_read_op_per_chunk \
      --test issue_1597_compression_info_one_parse \
      --test issue_1618_parser_work_counters \
      --test issue_1642_positional_row_emit \
      --test issue_1570_key_offset_cache \
      --test issue_1575_candidate_key_hash_hoist \
      --test issue_1576_range_short_circuit \
      --test issue_1578_aggregate_o1_memory \
      --test issue_1647_rows_floor_walk \
      --test issue_1577_limit_decode_stop \
      --test issue_1599_locate_parity \
      --test issue_2302_written_index_resolve \
      --test issue_1869_big_clustering_slice_readat ;;
    byte-budget-guard) run_component byte-budget-guard cargo test --package cqlite-core \
      --features write-support,cli-helpers,state_machine \
      --test issue_1582_byte_bounded_result_budget \
      --test issue_1578_streaming_aggregate_parity \
      --test issue_1578_limit_exempts_max_results \
      --test issue_1578_streaming_aggregate_multigen_parity \
      --test issue_2069_global_aggregate_empty_table ;;
    arrow-parity-guard) run_component arrow-parity-guard run_arrow_parity_guard_cmd ;;
    memory-budget) run_component memory-budget bash -c '
  # Read-path dhat budgets (issue #1565) + the export/Flight dhat budgets
  # (issue #1494, AD5): the converter per-row allocation guard (needs `arrow`)
  # and the Flight producer total/peak-memory guard (cqlite-flight, dhat-heap) +
  # the row-assembly (RowCells) allocs/row AND allocs/cell budgets (issue #2075).
  # dhat allocation counts are machine-independent, so these are the hard,
  # load-deterministic per-gate signal for the export/Flight/read paths.
  #
  # Run all FOUR dhat lanes UNCONDITIONALLY and aggregate (issue #1494 roborev):
  # `&&`-chaining short-circuits on the first failure and HIDES the others, costing
  # an extra triage round. Each lane reports its own result to the log; the
  # component FAILs if ANY lane failed (rc sticks at 1). --test-threads=1 is
  # mandatory on every lane (the dhat profiler is a process-global allocator).
  rc=0
  cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
    --test memory_budget -- --test-threads=1 || rc=1
  cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
    --test issue_1494_converter_alloc_budget -- --test-threads=1 || rc=1
  cargo test --package cqlite-flight --features dhat-heap \
    --test issue_1494_producer_mem_budget -- --test-threads=1 || rc=1
  # (d) row-assembly (RowCells) path — issue #2075: absolute allocs/row AND
  # allocs/cell budgets for the decode -> RowCells (Vec<(Arc<str>,Value)>) ->
  # QueryRow scan path, across a wide-row + a text-heavy shape. Complements the
  # #1046 width-SCALING guard (which lacks a per-cell metric); measures/gates the
  # #1645 item 2 (smallvec RowCells) win. Same feature set as the sibling lanes to
  # reuse build artifacts.
  cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
    --test issue_2075_row_assembly_alloc_budget -- --test-threads=1 || rc=1
  exit $rc' ;;
    integration-tests) run_component integration-tests bash -c '
  cargo test --package cqlite-integration-tests --no-run &&
  cargo test --package cqlite-integration-tests \
    --test comprehensive_component_integration_tests \
    --test fixture_specific_integration_tests \
    --test golden_path_get_operations_tests \
    --test golden_path_partition_lookup_tests \
    --test golden_path_scan_operations_tests \
    --test golden_path_summary_index_integration_tests' ;;
    format-compat) run_component format-compat cargo test --package format-compatibility-tests ;;
    write-tests) run_component write-tests bash -c '
  cargo test --package cqlite-core --features write-support --lib &&
  cargo test --package cqlite-core --features write-support --test write_read_roundtrip &&
  cargo test --package cqlite-core --features write-support --test compaction_integration' ;;
    cli-tests) run_component cli-tests bash -c '
  # issue #2039: ENUMERATE every cqlite-cli/tests/*.rs integration-test target
  # instead of a hardcoded 3-target allowlist. The old allowlist
  # (unit_tests + write_readback_content_tests + graceful_shutdown_tests) made any
  # NEW test file invisible to the full gate — a false-green (a red integration
  # test the gate never ran; observed on #1483 with read_sstable_stdout_tests).
  #
  # Two feature-correct passes (a single feature set cannot be right for every
  # target — some tests ASSERT read-only-binary behavior and legitimately behave
  # differently once write-support is compiled in, e.g. cli_schema_validation_tests
  # expects the read-only "Schema not found" DML rejection, not the write-capable
  # "DML statements require --writable mode" one), driven off the tests/*.rs glob so
  # future files are auto-covered:
  #
  #  PASS 1 — DEFAULT features (read-only binary): run the glob of tests/*.rs MINUS
  #  (a) targets that DECLARE required-features (cargo would error on `--test X` when
  #  X`s features are unmet) — those are handled by Pass 2 or deliberately excluded
  #  (delta-export/duckdb-tests source-built DuckDB, #916; dhat-heap global
  #  allocator) — and MINUS (b) the QUARANTINE set below. A NEW read-only test file
  #  lands here automatically and its failure FAILs the gate (acceptance #2). Targets
  #  with no required-features but write-support-#[cfg]-gated bodies
  #  (write_readback_content_tests, graceful_shutdown_tests) run 0 tests here and are
  #  executed for real by Pass 2.
  #
  #  PASS 2 — write-support: EXECUTE the write-support-gated tests as an EXPLICIT set
  #  (NOT `--tests`, which would re-run read-only-only targets under a write-capable
  #  binary and fail them). DERIVED, not hardcoded: targets whose required-features
  #  name write-support (cli_dml_integration_tests, issue_1388_compact_major_drop, …)
  #  are read out of cqlite-cli/Cargo.toml, UNIONed with the two self-gated
  #  ground-truth targets (write_readback_content_tests, graceful_shutdown_tests).
  #
  # QUARANTINE (issue #2039 follow-up, tracked under #2188): targets excluded from
  # both passes with a KNOWN reason, loudly here rather than silently unrun. Two
  # distinct sub-classes surfaced by enumeration, both requiring out-of-scope triage
  # rather than an inline fix — this is NOT a license to add new entries casually: a
  # NEW failing (or newly-silent) test must be fixed, never quarantined, unless it
  # matches one of these two documented shapes.
  #  (a) PRE-EXISTING RED — stale dataset-snapshot / SELECT-output tests that NO gate
  #      or CI lane ever ran (the ci.yml "CLI unit tests" job runs the old 3-target
  #      allowlist), bit-rotted against regenerated dataset binaries:
  #      comprehensive_select_test, integration_sstable_tests, parquet_writer_tests,
  #      table_snapshot_tests.
  #  (b) NEVER EXECUTED, UNVERIFIED — whole-file gated on the `integration-tests`
  #      Cargo feature (cqlite-cli/Cargo.toml), which NO CI workflow or gate
  #      component has ever enabled, so these have literally never run a single test
  #      anywhere. Caught by the Pass-1 zero-tests guard below (they would otherwise
  #      silently pass as "0 tests" default-feature targets — exactly the false-green
  #      shape it exists to close): end_to_end_tests, error_handling_tests,
  #      integration_tests, test_runner. test_runner OWN #[test] fns (lines
  #      684/694 as of this writing) live inside the SAME
  #      `#[cfg(all(test, feature = "integration-tests"))] mod tests` as the other
  #      three — its real, non-incidental test count is 0. It only APPEARS to run
  #      >0 tests today because it does `mod test_helpers;` (line 8), and
  #      test_helpers.rs carries its own UNRELATED always-on `#[cfg(test)] mod
  #      tests { #[test] fn test_helper_functions }` that gets transitively
  #      compiled in — a routine future refactor (dropping the unused `mod
  #      test_helpers;`, or moving that helper elsewhere) would silently drop
  #      test_runner to a real 0 and trip the zero-tests guard below for a reason
  #      unrelated to test_runner itself, hence quarantining it here instead.
  QUARANTINE="comprehensive_select_test integration_sstable_tests parquet_writer_tests table_snapshot_tests end_to_end_tests error_handling_tests integration_tests test_runner"
  #
  # Anchor enumeration to REPO_ROOT (roborev finding, #2039): unlike the
  # CWD-independent `cargo test --package cqlite-cli` invocations below, bare
  # relative `cqlite-cli/tests` / `cqlite-cli/Cargo.toml` reads would silently break
  # if this component is ever invoked from a different CWD. REPO_ROOT is baked in
  # here as a literal absolute path (agent-gate.sh sets it before dispatch).
  cli_tests_dir="'"$REPO_ROOT"'/cqlite-cli/tests"
  cli_cargo_toml="'"$REPO_ROOT"'/cqlite-cli/Cargo.toml"

  # Fail closed (never silent-pass) if the glob finds zero files, or if either
  # derived target set comes back empty (a regression in the derivation).
  cli_test_count=$(find "$cli_tests_dir" -maxdepth 1 -name "*.rs" 2>/dev/null | wc -l | tr -d " ")
  if [ "${cli_test_count:-0}" -eq 0 ]; then
    echo "cli-tests: FAIL-CLOSED — no cqlite-cli/tests/*.rs files found to enumerate (#2039)" >&2
    exit 1
  fi
  # Every tests/*.rs basename (each is a cargo integration-test target).
  all_targets=$(for f in "$cli_tests_dir"/*.rs; do basename "$f" .rs; done | sort -u)
  # Targets that declare ANY required-features (name precedes required-features in
  # each [[test]] block) — excluded from the default-feature Pass 1.
  rf_targets=$(awk -F\" "/^[[:space:]]*name[[:space:]]*=/{cur=\$2} /^[[:space:]]*required-features[[:space:]]*=/{if(cur!=\"\")print cur}" "$cli_cargo_toml" | sort -u)
  # Write-support target set for Pass 2: required-features naming write-support, plus
  # the self-gated ground-truth targets. Minus quarantine (defensive; none overlap).
  ws_targets=$( { awk -F\" "/^[[:space:]]*name[[:space:]]*=/{cur=\$2} /^[[:space:]]*required-features[[:space:]]*=/ && /write-support/{print cur}" "$cli_cargo_toml"; \
    printf "%s\n" write_readback_content_tests graceful_shutdown_tests; } | sort -u \
    | grep -vxF -f <(printf "%s\n" $QUARANTINE) )
  # Pass 1 default set = all targets, minus required-features targets, minus quarantine.
  default_targets=$(printf "%s\n" "$all_targets" \
    | grep -vxF -f <(printf "%s\n" $rf_targets $QUARANTINE) )
  if [ -z "$default_targets" ]; then
    echo "cli-tests: FAIL-CLOSED — derived zero default (read-only) targets (#2039)" >&2
    exit 1
  fi
  if [ -z "$ws_targets" ]; then
    echo "cli-tests: FAIL-CLOSED — derived zero write-support targets (#2039)" >&2
    exit 1
  fi
  def_flags=()
  while IFS= read -r t; do [ -n "$t" ] && def_flags+=(--test "$t"); done <<< "$default_targets"
  ws_flags=()
  while IFS= read -r t; do [ -n "$t" ] && ws_flags+=(--test "$t"); done <<< "$ws_targets"
  # required-features targets that run in NEITHER pass (deliberately: delta-export/
  # duckdb-tests source-built DuckDB #916; dhat-heap global allocator) — named here
  # loudly (roborev finding, #2039) rather than only living in a source comment, the
  # same honesty standard as the QUARANTINE notice below. Collapsed to a single
  # space-separated line (not the newline-per-entry pipeline output) so it prints as
  # one visible line like the other notices, rather than silently line-wrapping.
  excluded_both=$(printf "%s\n" "$rf_targets" | grep -vxF -f <(printf "%s\n" $ws_targets) | tr "\n" " " | sed "s/ *\$//")
  echo "cli-tests: ${cli_test_count} tests/*.rs file(s); Pass 1 (default) targets: ${def_flags[*]}"
  echo "cli-tests: Pass 2 (write-support) targets: ${ws_flags[*]}"
  echo "cli-tests: QUARANTINED pre-existing-red targets (NOT run, #2039 follow-up): $QUARANTINE"
  echo "cli-tests: EXCLUDED from BOTH passes (delta-export/duckdb-tests/dhat-heap; deliberate, #916): ${excluded_both:-(none)}"

  # Zero-tests guard (roborev finding on #2039): a target whose body is entirely
  # `#![cfg(feature = "write-support")]`-gated but which does NOT declare
  # `required-features` in Cargo.toml lands in the Pass 1 default set (nothing
  # excludes it) yet executes 0 tests there (its body compiles out under default
  # features), and is invisible to the derived ws_targets set in Pass 2 unless it
  # happens to be one of the two hardcoded self-gated ground-truth names — so a
  # THIRD file with this shape would silently run 0 tests in BOTH passes forever.
  # This shape is proven real: write_readback_content_tests/graceful_shutdown_tests
  # ARE this shape (that is exactly why they are the hardcoded ground truth).
  #
  # The guard itself is agent-gate.sh:check_no_unexpected_zero_tests, a single
  # top-level definition `export -f`-ed into this `bash -c` body (#1699 promoted it
  # out of here when legacy-heuristics/flight-tests needed the same check — one
  # implementation, not three copies). Only the two Pass-1 ground-truth names are
  # allowed to run 0 there; NOTHING is allowed to run 0 in Pass 2 (every real
  # write-support target must execute at least one test).

  # A PRIVATE DIRECTORY, not two bare mktemp files in the shared tmp (roborev round-31, Medium).
  # `_ansi_stripped_log` writes `<log>.ansi-stripped` — a PREDICTABLE sibling — and these tests run
  # for minutes, so another local user could pre-create that sibling as a SYMLINK and have the
  # guard`s `sed` overwrite any file the gate user can write. Inside a 0700 mktemp -d the sibling
  # path is not guessable and not creatable by anyone else. (The other callers were already safe:
  # they log into $LOG_DIR, itself a per-run mktemp -d.)
  # NO `local` here: this body runs under `bash -c`, not inside a function, and bash rejects
  # `local` at the top level with "can only be used in a function" — which would have failed the
  # component on its first real run rather than at parse time (`bash -n` accepts it).
  _cli_tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-cli.XXXXXX") || exit 1
  chmod 700 "$_cli_tmp" 2>/dev/null || true
  log1="$_cli_tmp/pass1.log"; log2="$_cli_tmp/pass2.log"
  # The `.ansi-stripped` siblings too (roborev round-18, Low): the zero-test guards
  # parse a stripped COPY that _ansi_stripped_log writes beside the log, so cleaning
  # only the originals leaks two files per gate run into TMPDIR. NOTE: no apostrophes in
  # this comment — the cli-tests component body is a single-quoted `bash -c` string, so one
  # would terminate it (it did, first try). The lane logs of the other components
  # live under $LOG_DIR, which is retained deliberately as the `logs:` bundle; these
  # two bare mktemps are the only ones nobody else collects.
  trap "rm -rf \"$_cli_tmp\"" EXIT

  cargo test --package cqlite-cli "${def_flags[@]}" 2>&1 | tee "$log1"
  rc=${PIPESTATUS[0]}
  [ "$rc" -eq 0 ] || exit "$rc"
  check_no_unexpected_zero_tests "cli-tests Pass 1 (default)" "$log1" write_readback_content_tests graceful_shutdown_tests || exit 1

  cargo test --package cqlite-cli --features write-support "${ws_flags[@]}" 2>&1 | tee "$log2"
  rc=${PIPESTATUS[0]}
  [ "$rc" -eq 0 ] || exit "$rc"
  check_no_unexpected_zero_tests "cli-tests Pass 2 (write-support)" "$log2"' ;;
    compaction-byte-parity) run_compaction_byte_parity ;;
    bti-multiclustering) run_bti_multiclustering ;;
    query-semantics-oracle) run_query_semantics_oracle ;;
    flight-query-semantics-oracle) run_flight_query_semantics_oracle ;;
    flight-tests) run_flight_tests ;;
    legacy-heuristics) run_legacy_heuristics ;;
    # The two #1699 feature-ISOLATION lanes: cqlite-core with ONE of parquet /
    # delta-scan and NOT the other. The instrument is `cargo test --lib --no-run` under
    # `_deny_warnings` — NOT `--all-targets`, which section 34 of
    # test_agent_gate_summary.sh now FORBIDS here (it pulls in ~100 default-feature
    # integration files, measured noise), and NOT `cargo check`, which is blind to
    # `cfg(test)` and therefore to the #1978 incident class these lanes exist to catch.
    # `-D warnings` via `_deny_warnings` stays load-bearing (#1981: the dead-code lint
    # must be an error, and a bare `env RUSTFLAGS=` is ignored when
    # CARGO_ENCODED_RUSTFLAGS is set). Full rationale on run_feature_iso.
    # (This comment previously claimed `--all-targets` was load-bearing — the opposite of
    # what the lane does. Corrected on the C re-audit; comments beside code are not
    # pinned by section 34, which scans the function body.)
    feature-iso-parquet) run_component feature-iso-parquet run_feature_iso parquet ;;
    feature-iso-delta-scan) run_component feature-iso-delta-scan run_feature_iso delta-scan ;;
    python-bindings) run_python_bindings ;;
    node-bindings) run_node_bindings ;;
    binding-rust-tests) run_binding_rust_tests ;;
    delivery-telemetry) run_delivery_telemetry ;;
    oom-audit) run_oom_audit ;;
    parity-report) run_parity_report ;;
    operator-metrics-doc) run_operator_metrics_doc ;;
    kit-dashboard-drift) run_kit_dashboard_drift ;;
    binding-unwind-profile) run_component binding-unwind-profile bash "$REPO_ROOT/scripts/tests/test_binding_unwind_profile.sh" ;;
    pub-surface) run_pub_surface ;;
    tooling-tests) run_tooling_tests ;;
    minimal-build) run_component minimal-build bash -c '
  # Match the CI "All Compression Build & Test" job byte-for-byte (issue #1981):
  # that job sets RUSTFLAGS=-D warnings, so a warning-class error (e.g. an unused
  # `#[cfg(test)]` helper whose only caller is feature-gated out under the minimal
  # feature set — the dead-code lint) hard-fails CI but slipped past this gate,
  # which ran WITHOUT -D warnings (#1972/#1978/#1981 all escaped locally this way).
  # Export it for BOTH the build and the test-compile so this component enforces
  # exactly what CI enforces.
  export RUSTFLAGS="-D warnings" &&
  cargo build --package cqlite-core --no-default-features --features all-compression &&
  # Test-compile the minimal lane (issue #1978): the CI "All Compression Build &
  # Test" job runs `cargo test --no-default-features --features=all-compression
  # --lib`, which compiles the test targets. A plain `cargo build` never does, so
  # a `#[cfg(test)]` module referencing a write-support-gated item (e.g.
  # storage::serialization) silently escaped this gate. Compile-only (--no-run)
  # keeps it fast; no data fixtures needed for a compile check.
  cargo test --package cqlite-core --no-default-features --features all-compression --lib --no-run' ;;
    smoke) run_component smoke bash -c '
  cargo build --package cqlite-cli --bin cqlite &&
  CQLITE_CLI="${CARGO_TARGET_DIR:-$PWD/target}/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh' ;;
    *) echo "dispatch_component: unknown component $1" >&2; return 2 ;;
  esac
}

# is_side_component / run_side_component: the SIDE lane holds every gate component
# that can run CONCURRENTLY with the shared-target MAIN cargo lane WITHOUT
# introducing cross-lane build thrash. Two classes qualify (issues #1737, #2657):
#
#   (a) SEPARATE-CRATE / DIVERGENT-FEATURE cargo components. python-bindings and
#       node-bindings are the biggest non-core costs and, being separate crates
#       built with binding-specific features, would repeatedly invalidate + rebuild
#       cqlite-core in the SHARED target dir if run concurrently with MAIN
#       (measured: python-bindings ballooned 72s -> 576s under a naive shared-target
#       pool). The same shared-target hazard applies to the other isolatable cargo
#       components (issue #2657): memory-budget compiles cqlite-core with a DIFFERENT
#       feature set (dhat-heap,arrow) than MAIN's cli-helpers — exactly the
#       feature-thrash shape — and smoke + parity-report build cqlite-cli /
#       cassandra-parity (both depend on cqlite-core). Running any of these against
#       MAIN's target dir would thrash it; each therefore gets its OWN
#       CARGO_TARGET_DIR (see run_side_component), which removes the cross-lane cargo
#       feature-thrash and build-lock contention (sccache still dedups the actual
#       compiles across target dirs).
#   (b) NON-CARGO / isolatable script components: delivery-telemetry (python3),
#       binding-unwind-profile (offline bash). These touch MAIN's shared target not
#       at all, so they are trivially safe to overlap with the core cargo lane.
#
# EXCLUDED from SIDE (issue #2657, gate FAIL): tooling-tests stays on the SERIAL MAIN
# lane despite being non-cargo. It embeds TIMING-SENSITIVE shell self-tests --
# notably test_worker_supervisor.sh's exit-latency assertion (#2666, <15s ceiling) --
# that STARVE under co-scheduled SIDE-lane CPU load: measured ~20s under the parallel
# pool vs ~7s in isolation, so parallelizing it degraded the very component it moved.
# Keeping it serial preserves its latency headroom; the other five isolatable
# components still overlap the core long pole.
#
# Everything NOT listed here stays on the strictly-serial MAIN lane (it shares
# cqlite-core's target with a MAIN-compatible feature set), preserving the identical
# build profile of the historical sequential gate. Widening this set only shifts
# WHICH lane a component runs in — the end-of-run SUMMARY is reconstructed in
# canonical COMPONENTS order from per-component .result files, so the summary block
# CONTRACT is unchanged regardless of lane or finish order.
is_side_component() {
  [ "$(_component_lane "$1")" = side ]
}
run_side_component() {
  local base="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
  CARGO_TARGET_DIR="$base/agent-gate-side/$1" dispatch_component "$1"
}

# Track which components were selected and which lane (fail-closed check after lanes drain).
declare -a SELECTED_MAIN=() SELECTED_SIDE=()
SIDE_LANE_EXIT=0
# #2874: PID of the backgrounded SIDE-lane sub-pool while it is live (empty otherwise),
# so a MAIN-lane integrity `exit 1` can tear it down before exiting (review finding 2).
SIDE_LANE_PID=""

# launch_components: two-lane bounded model (issues #1737, #2657). The MAIN lane
# runs every selected shared-target cargo component (those that build cqlite-core
# under MAIN's cli-helpers feature set) SERIALLY in canonical order (identical build
# profile to the historical sequential gate -- no NEW cross-component thrash), with
# nextest cutting the core-tests long pole. The SIDE lane runs every isolatable
# component (see is_side_component: the bindings PLUS the non-core/isolated-feature
# components parity-report, delivery-telemetry, binding-unwind-profile, smoke,
# memory-budget -- tooling-tests is EXCLUDED and stays serial on MAIN, see below),
# each in its OWN CARGO_TARGET_DIR, concurrently with MAIN --
# so the isolatable non-core work overlaps the core cargo long pole instead of
# tailing it. Concurrent heavy processes are bounded by AGENT_GATE_JOBS: MAIN takes
# one slot, the SIDE lane runs up to (AGENT_GATE_JOBS - 1) of its components at once
# (this per-gate cap composes with the machine-wide bound of #1825). AGENT_GATE_JOBS=1
# (or bash < 4.3) collapses to the historical strictly-sequential run. file-size
# already ran inline before the dataset preflight and is skipped here.
launch_components() {
  local -a main_lane=() side_lane=()
  local c
  for c in "${COMPONENTS[@]}"; do
    [ "$c" = file-size ] && continue
    _pool_selected "$c" || continue
    if is_side_component "$c"; then side_lane+=("$c"); SELECTED_SIDE+=("$c")
    else main_lane+=("$c"); SELECTED_MAIN+=("$c"); fi
  done

  # Bash 3.2 under `set -u` treats "${arr[@]}" of an EMPTY array as unbound (fixed
  # in bash 4.4+; #1841 latent bug surfaced by the #1825 concurrency-cap self-test,
  # which runs a nested `--only <one-component>` gate -- exactly the case where
  # main_lane or side_lane is empty). Guard every such expansion below with the
  # `"${arr[@]+"${arr[@]}"}"` idiom, which is a no-op when non-empty and expands to
  # nothing (never unbound) when empty. Same idiom already used for `stems` above.
  if [ "$AGENT_GATE_JOBS" -le 1 ] || [ "${#side_lane[@]}" -eq 0 ]; then
    for c in "${main_lane[@]+"${main_lane[@]}"}"; do dispatch_component "$c"; done
    for c in "${side_lane[@]+"${side_lane[@]}"}"; do run_side_component "$c"; done
    return
  fi

  local side_jobs=$(( AGENT_GATE_JOBS - 1 )); [ "$side_jobs" -lt 1 ] && side_jobs=1
  echo ">>> [pool] MAIN lane (serial, shared target): ${main_lane[*]}"
  echo ">>> [pool] SIDE lane (isolated target, up to $side_jobs concurrent): ${side_lane[*]}"
  # SIDE lane: a background sub-pool capped at side_jobs (each isolated target dir).
  (
    srun=0
    for sc in "${side_lane[@]+"${side_lane[@]}"}"; do
      run_side_component "$sc" &
      srun=$(( srun + 1 ))
      if [ "$srun" -ge "$side_jobs" ]; then wait -n 2>/dev/null || true; srun=$(( srun - 1 )); fi
    done
    wait
  ) &
  # GLOBAL (#2874 review finding 2): the MAIN-lane integrity guard's `exit 1` fires
  # while this SIDE-lane sub-pool is still live; it must be able to tear it down first
  # so orphaned cargo/maturin/node builds don't keep thrashing the shared target dir
  # on an already-freed concurrency slot. See _assert_summary_integrity's MAIN branch.
  SIDE_LANE_PID=$!
  # MAIN lane: serial, foreground (shared target dir, no intra-lane parallelism).
  for c in "${main_lane[@]+"${main_lane[@]}"}"; do dispatch_component "$c"; done
  wait "$SIDE_LANE_PID" || SIDE_LANE_EXIT=$?
  SIDE_LANE_PID=""
}

launch_components

# Fail-closed check (issue #1737 roborev): verify all SELECTED components produced result files.
# A component that was selected but has no .result file crashed/exited before record_result,
# which is a fail-OPEN hole. Treat missing results as synthetic FAIL + force overall FAIL.
# Also check the SIDE lane's exit status. Bash-3.2-safe empty-array guard (#1841,
# same hazard as launch_components above): a `--only <main-only-component>` run
# leaves SELECTED_SIDE empty, and vice versa.
for _sc in "${SELECTED_SIDE[@]+"${SELECTED_SIDE[@]}"}"; do
  [ -f "$LOG_DIR/$_sc.result" ] || {
    echo "agent-gate: SIDE-lane component '$_sc' SELECTED but has no result file (crashed/exited early)" >&2
    NAMES+=("$_sc"); STATUSES+=(FAIL); TIMES+=("0s")
    OVERALL=FAIL
  }
done
for _mc in "${SELECTED_MAIN[@]+"${SELECTED_MAIN[@]}"}"; do
  [ -f "$LOG_DIR/$_mc.result" ] || {
    echo "agent-gate: MAIN-lane component '$_mc' SELECTED but has no result file (crashed/exited early)" >&2
    NAMES+=("$_mc"); STATUSES+=(FAIL); TIMES+=("0s")
    OVERALL=FAIL
  }
done
if [ "$SIDE_LANE_EXIT" -ne 0 ]; then
  echo "agent-gate: SIDE lane exited with status $SIDE_LANE_EXIT (subshell failure)" >&2
  OVERALL=FAIL
fi
# #2874: consume a SIDE-lane summary-integrity marker (a backgrounded component that
# detected a mid-run clobber could not safely emit+exit) → force FAIL + a named
# terminal line, so a SIDE-lane clobber is never silently lost to a false-green.
_apply_integrity_marker
# #2926: same post-drain contract for a SIDE-lane TREE-integrity marker (a backgrounded
# component that detected a mid-run tree mutation could not safely emit+exit) → force
# FAIL + a named terminal line, so a SIDE-lane detection is never lost to a false-green.
_apply_tree_integrity_marker

# Reconstruct the summary arrays from per-component result files (issue #1737):
# the bounded pool ran components in backgrounded subshells that cannot write the
# parent's arrays, so each wrote its verdict to $LOG_DIR/<name>.result. Read them
# back in canonical COMPONENTS order for a deterministic SUMMARY regardless of the
# order components finished; a missing file means the component was not selected.
for _c in "${COMPONENTS[@]}"; do
  _rf="$LOG_DIR/$_c.result"
  [ -f "$_rf" ] || continue
  _st=""; _secs=""
  read -r _st _secs < "$_rf" || true
  NAMES+=("$_c"); STATUSES+=("$_st"); TIMES+=("${_secs}s")
  [ "$_st" = FAIL ] && OVERALL=FAIL
done

# #2926: the TERMINAL tree capture — the authoritative check, taken AFTER the last
# component boundary and BEFORE the block is built, so a mutation landing in that
# window is still caught. Forces OVERALL=FAIL on detection (which also keeps a mutated
# `--only` run from being promoted to PARTIAL below).
_tree_finalize || true

declare -a SUMMARY_META=()
# #2926 review C1: derived from the VERIFIED terminal capture taken immediately above —
# NOT from a fresh `git rev-parse --short HEAD` / `git status --porcelain` at emit time.
# That emit-time read was the original #2926 defect: a HEAD move landing between the
# capture and the stamp certified a sha the guard never verified.
_tree_commit_meta
SUMMARY_META+=("$TREE_COMMIT_LINE")
if selected_needs_datasets; then
  SUMMARY_META+=("datasets: $DATA_COUNT Data.db files under $CQLITE_DATASETS_ROOT")
else
  SUMMARY_META+=("datasets: $DATA_COUNT")
fi
# #2078: stamp the opt-out marker so an intentional AGENT_GATE_ALLOW_MISSING_FIXTURES=1
# run is visible in the pasted SUMMARY block (empty otherwise → no line).
[ -n "$MISSING_FIXTURES_MARKER" ] && SUMMARY_META+=("$MISSING_FIXTURES_MARKER")
# #3148: stamp the POSITIVE committed-schemas assertion (which root was validated, and
# whether via the checkout or a CQLITE_SCHEMAS_ROOT override), so the pasted block shows
# the check RAN rather than merely that nothing complained. Empty when the preflight was
# skipped (no dataset-dependent component selected) → no line.
[ -n "$SCHEMAS_LINE" ] && SUMMARY_META+=("$SCHEMAS_LINE")
SUMMARY_META+=("ci-pins: $PINS")
SUMMARY_META+=("$(accelerators_line)")
SUMMARY_META+=("$(cpu_budget_line)")
# #2874: a SIDE-lane mid-run summary clobber surfaces as a named terminal line.
[ -n "$SUMMARY_INTEGRITY_LINE" ] && SUMMARY_META+=("$SUMMARY_INTEGRITY_LINE")
# #2926: tree provenance (tree-start / tree-end / tree-integrity [/ tree-hash-cap]).
_tree_meta_array
SUMMARY_META+=("${TREE_META_LINES[@]}")
if [ -n "$ONLY" ]; then
  SUMMARY_META+=("mode: PARTIAL (--only $ONLY) - does NOT count as the gate")
  [ "$OVERALL" = "PASS" ] && OVERALL=PARTIAL
fi
for i in "${!NAMES[@]}"; do
  SUMMARY_META+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
done
# #2874 (ratified job-2106): route the terminal emit through the shared MAIN/SIDE contract, so a
# SIDE-lane foreign-live-peer clobber publishes FAIL to the private log + sibling instead of
# rewriting the peer's contended path. OVERALL is already FAIL in that case → exit 1 below.
_emit_terminal_summary "$OVERALL" "${SUMMARY_META[@]}" || true

# #2667: full-gate completion push-signal. This line is reached ONLY by the full
# gate and by --only (never --lite/--delta/selftest, which exit earlier), so we
# additionally exclude --only here — a partial run is not the gate of record and
# must not page a waiting closer. Advisory: gate_push_signal never affects exit.
if [ -z "$ONLY" ] && [ "$LITE" -eq 0 ] && [ "$DELTA" -eq 0 ] && [ "$SELFTEST" -eq 0 ]; then
  _push_result="$OVERALL"
  [ "$SUMMARY_WRITE_FAILED" -ne 0 ] && _push_result=FAIL
  _push_fails=""
  for i in "${!NAMES[@]}"; do
    [ "${STATUSES[$i]}" = FAIL ] && _push_fails="${_push_fails:+$_push_fails,}${NAMES[$i]}"
  done
  # #2926 review C1: the signal names the SAME verified identity the block stamped, not a
  # fresh emit-time read (advisory notification, but it must not disagree with the block).
  _push_sha=$(printf '%s' "$TREE_COMMIT_LINE" | sed -n 's/^commit: \([^ ]*\).*/\1/p')
  _push_branch=$(printf '%s' "$TREE_COMMIT_LINE" | sed -n 's/.* branch: \([^ ]*\).*/\1/p')
  gate_push_signal "$_push_result" \
    "${_push_branch:-unknown}" \
    "${_push_sha:-unknown}" \
    "$_push_fails"
fi

# If we could not produce the authoritative recovery artifact, never report
# green (#1175 finding 1): the correctness verdict above is still printed, but a
# missing summary file forces a non-zero exit so the failure cannot pass silently.
if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
  echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
  exit 1
fi

# Exit 0 only for a full-gate PASS; PARTIAL runs exit 3 so they can never be
# scripted into a green gate claim.
case "$OVERALL" in
  PASS) exit 0 ;;
  PARTIAL) exit 3 ;;
  *) exit 1 ;;
esac
