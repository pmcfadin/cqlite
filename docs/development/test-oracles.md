# Test oracles and their blind spots

Moved verbatim from `CLAUDE.md`, which keeps a short rule for each and links here. Each entry is a
way a green test can hide a real defect. Read the one that matches your test before writing it.

Related: [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/).

- **Resolve fixture roots per TABLE, and assert per CASE (issue #3220)**: a lane that picks its
  corpus root by KEYSPACE (`root.join(keyspace).is_dir()`) and commits to it can pass without ever
  running — a `CQLITE_DATASETS_ROOT` holding `test_da/` but not the git-committed
  `test_da/multiclustering_table-*` made the #3032 case skip silently behind a green suite. Use
  `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table`, which walks EVERY candidate
  root (env, then checkout) for that table's `*-Data.db`. And never terminate a corpus loop with a
  suite-wide `assert!(ran > 0)`: it cannot see one case skipping behind its siblings — assert per
  case (committed fixtures = `must_run`, fail-closed unconditionally).
  Resolve by EVIDENCE, never by a preference ordering: neither root is a superset — a fleet
  `/data/datasets` measured 144 `*-Data.db` over 122 tables yet lacks the one committed
  `test_da/multiclustering_table`, which the checkout's 31 parity references carry — so *any* fixed
  env-first/checkout-first rule picks wrong for one set of tables. That dissolves #3104's "prefer
  the already-exported root" fix for the lanes on this resolver; **#3104 stays open** for what the
  resolver does not reach (whole-corpus `#2078` preflight, count-naming diagnostics, `--lite`
  small-corpus warning, and the doctrine text still telling agents to override the exported root).
- **Two parity oracles (issue #1742)**: *physical-dump parity* (the `*-Data.db.jsonl` sstabledump
  goldens) enumerates every on-disk cell INCLUDING tombstones/deleted/expired-TTL rows, so it CANNOT
  catch a read-time-reconciliation bug (both sides keep the shadowed rows → green while a real
  `SELECT` diverges). *Query-semantics parity* (`test-data/query-semantics-oracle.json`, gate
  component `query-semantics-oracle`, test `query_semantics_oracle_parity.rs`) records the
  post-reconciliation result set of a canonical `SELECT` at a PINNED `now` (never wall-clock). Add
  the correct oracle for the property under test; correctness of `SELECT` output needs the semantic one.
  The CQLite-vs-CQLite complement is the *point-vs-full differential lane* (issue #1918,
  `cqlite-core/tests/point_vs_full_differential.rs`): it runs the same point-eligible query under
  forced `CQLITE_READ_PATH=point` and `=full` and asserts identical rows/values/order at a PINNED
  `now` — catching a divergence between the two read paths that a physical dump cannot see.
- **Third blind spot: a CQLite-WRITTEN + CQLite-READ round-trip test is INVARIANT to a uniform
  framing/serialization error (issue #3042).** Both sides make the *identical* mistake, so the
  round-trip closes and the test stays green while real Cassandra-written data reads wrong — and,
  symmetrically, CQLite-written data is unreadable by Cassandra. Such a test can **never** substitute
  for a Cassandra-written fixture; it validates self-consistency, which is not the property anyone
  cares about. Concrete instance: the only arity-2 BTI test,
  `cqlite-core/tests/issue_908_bti_canonical_write.rs`, is CQLite-written and CQLite-read and asserts
  only ordering/structure, so it is invariant to exactly the framing defect of **#3002 (BTI `Rows.db`
  row-index root base 2 bytes low — missing the `writeWithShortLength` 2-byte prefix, masked by a
  compensating encoder defect that omitted the leading `0x40 NEXT_COMPONENT`)**. Two defects that
  cancel are undetectable by a symmetric test *by construction*. The oracle that caught it is
  `cqlite-core/tests/issue_3002_bti_rows_root_base.rs`, asserting against the real Cassandra 5.0 `da`
  fixture with every expectation derived from Cassandra's writer/reader source — never from CQLite's
  prior behavior. Rule: for any on-disk framing/encoding property, the oracle must be
  **Cassandra-written bytes** (or Cassandra source), never CQLite's own output. Long form:
  [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/).
- **Fourth blind spot: EVERY oracle above is PER-SURFACE, so three surfaces can each be green against
  their own oracle while DISAGREEING WITH EACH OTHER (issue #1455).** Python, Node and the CLI are three
  independent windows onto one SSTable, and each was checked only against its own reference — Python
  against the CLI (`test_cli_parity.py`), Node against the sstabledump JSONL goldens
  (`parity-utils.js`), the CLI against nothing else. Those two normalizers **do not share an oracle, a
  canonical form, or even a comparison direction** (blob canonicalizes to a `"0x…"` STRING on the Python
  side and to a `Buffer` on the Node side; timestamp to a millisecond-truncated string vs a `Date` with a
  ±1 ms tolerance; Node has **no duration rule at all**), so both can pass while a user querying one table
  three ways gets three answers. The cross-surface differential is
  `bindings/parity/` + `bindings/python/tests/test_cross_binding_parity.py`: ONE `SELECT`, all three
  surfaces, canonical JSON, deep-equal per row. **The canonical form is implemented TWICE by construction
  (`canonical.py` / `canonical.mjs`) and the two are DIFFERENTIALLY PINNED** against a shared
  `canonical-vectors.json` — a second implementation's agreement is only knowable by testing it, never by
  care. **SEVEN DECLARED gaps, printed IN FULL at run time from one `DECLARED_GAPS` tuple — because a
  lane that omits coverage silently is indistinguishable from one that covers it, and a README nobody
  opens is not a declaration**: (1) `tuple` vs `list` is UNDETECTABLE here — Node and the CLI both emit
  a plain array and only Python has a distinct type, so it is canonicalized as a plain array; (2) **no
  `varint` column exists anywhere in `test-data/schemas/*.cql`**, so that rule is pinned by
  `canonical-vectors.json` alone and by no fixture; (3) UDT columns are REFUSED by the canonicalizer
  rather than compared, and no fixture uses one; (4) non-finite floats are a real 3-way asymmetry
  (Python `nan` / Node `NaN` / CLI JSON `null`, `cqlite-cli/src/output/json.rs:156-161`) and are avoided
  rather than reconciled; (5) a column absent from one leg is compared as `null`, so the harness cannot
  tell *omitted* from *null* — and the omitting leg is **NODE** (`bindings/node/src/row.rs:130` skips a
  metadata column with no value, while `bindings/python/src/result.rs:447` null-FILLS a shared row
  shape; the first draft of this harness blamed Python, which is backwards); (6) **A UNIFORM
  `cqlite-core` DEFECT IS INVISIBLE TO IT — all three legs read the SAME core, so agreement here is
  agreement about CQLite, not about Cassandra.** That is #3042's round-trip-invariance lesson one level
  up: a differential between SURFACES over a shared engine can only find *surface* divergence, and it
  never substitutes for a Cassandra-written oracle; (7) the 3-way comparison runs in **CI only**.
  **THAT LAST ONE MEANS THIS HARNESS IS NOT MERGE-GATING.** No local gate component can run it — the
  gate runs pytest with `RUN_SLOW_TESTS=0` and builds neither the Node native module nor a release
  `cqlite-cli` — so it lives in `python-ci.yml`'s `cross-binding-parity` job, which is
  `required`-exempt AND in the heavy `ci:bindings-full` tier, i.e. on a routine unlabeled PR it does not
  run at all. A cross-binding divergence can therefore still merge; the `.github/ci-gating-tiers.yml`
  exemption NAMES that residual rather than implying coverage it does not have (#3493). Marking the test
  `@pytest.mark.slow` is deliberate and not an oversight: unmarked, the gate's `python-bindings`
  component would instantiate the `cli_binary` fixture and add a full release `cqlite-cli` build to
  EVERY lane's full gate. **And the fixture-skip route is a defect this harness reproduced inside its own
  first draft, caught in review**: `conftest.py`'s `cli_binary` fixture `pytest.skip`s on build failure
  and is NOT strict-aware, and the CI job invokes only this one file — whose other non-slow tests pass,
  so #1230's session floor never fires. All three parity cases would have skipped and
  `cross-binding-parity` would have reported SUCCESS having compared nothing. The parity lane therefore
  wraps that fixture and `pytest.fail`s under strict mode, and both data tables carry committed **case
  floors** (minimum fixture/vector/refusal counts plus required names and CQL kinds), since an emptied
  table otherwise yields an empty parametrize that pytest reports as one skipped placeholder — #3544's
  case-floor lesson, one directory over.
- **Fifth blind spot: a point-read test that compares a SUBSET of columns against the scan cannot see
  a TRUNCATED point row (issue #3890).** The four above are about which ORACLE you compare against;
  this one is about how much of the row you compare. `assert_point_equals_scan`
  (`cqlite-core/tests/issue_1573_readat_positional.rs`) projected `id` plus ONE named column, and
  `SELECT id, name` decodes the first two cells and stops — so a point read whose LATER cells failed to
  decode compared equal on exactly the columns being compared, for years. Two properties make it
  invisible rather than merely under-tested: a failed cell decode inside the row loop is SWALLOWED
  (`row_decoder/row_data.rs` logs at `debug` and `break`s — #3721 is removing that), so nothing
  propagates; and the missing cells are simply ABSENT from the row's map, so a `get(col)` comparison
  over the columns you named can never notice them. **Rule: a point/seek-vs-scan comparison uses
  `SELECT *` and asserts BOTH directions of the column set** — no scan column absent from the point
  row, no point column the scan lacks — and reports the missing column BY NAME. The corpus-wide
  instance is `cqlite-core/tests/issue_3890_point_read_column_parity_sweep.rs`. **Two rules about
  its per-table key cap, both of which cost a review round: a bound tight enough to cost nothing
  can be tight enough to miss most of what it exists to catch, so measure what your cap EXCLUDES;
  and a cap's detection figure is only meaningful alongside its SELECTION** — capping in scan order
  and sorting afterwards samples different keys than capping over the sorted set, and that alone
  moved the same measurement. **No figure is quoted here on purpose: measuring a guard's detection
  power needs the swallow instrumented AND the fix reverted, so it is not reproducible from
  committed source, and a number nobody can re-derive from the repo is what stops the next person
  looking.** That target's module header carries the numbers with the exact recipe — commands, cap
  values, and how the fix is reverted so detection is measured against the defect PRESENT.
