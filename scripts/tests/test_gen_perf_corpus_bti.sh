#!/usr/bin/env bash
# Self-test for the issue-#3234 BTI (`da`) perf-corpus generator.
#
# What it pins, and why each one matters:
#
#   1. Flag validation happens BEFORE any expensive or destructive work. A typo
#      must never start a container, load millions of rows, and then overwrite the
#      COMMITTED manifest (the lesson #3068's generator learned the hard way).
#      Unrecognized arguments exit 2 (the fetch-datasets.sh convention).
#   2. --smoke lowers the DEFAULTS but NEVER overrides an explicit --keyspace,
#      --rows or --chunk-rows (or their env equivalents), and it defaults the
#      keyspace to perf_bti_smoke so a smoke run cannot clobber a production corpus.
#   3. THE ACCEPTANCE ASSERTS ARE REAL, in both directions. Issue #3234 AC1/AC2 are
#      "`da` descriptors only, >= 1 Data.db > 8 MiB, non-empty Rows.db, BTI TOC" --
#      and a stock Cassandra 5.0 node silently emits `nb` when either yaml setting
#      misses, so an assert that only ever ran on a good corpus is untested. Every
#      case here is driven through --verify-only against a FABRICATED corpus, with
#      a negative control per assert.
#   4. The row driver is deterministic given (seed, chunk-index) and emits exactly
#      the requested row count -- that determinism is what makes the manifest's
#      per-Data.db sha256 a reproducibility check rather than decoration.
#   5. The manifest writer fails closed on an empty / non-BTI SSTable directory
#      instead of emitting a manifest that describes nothing -- AND its happy path
#      actually runs (see 7), so the fields it publishes are asserted, not assumed.
#   6. The cassandra.yaml flip is verified against a COMMITTED cassandra:5.0.2
#      excerpt. It is the most consequential upstream guard in the generator: a
#      stock node emits `nb` (BIG) with no error at all, and the `sed` addresses
#      depend on the shipped file's exact comment markers and two-space indentation.
#   7. Stale-corpus pruning `rm -rf`s multi-GB paths, so every guard (symlink skip,
#      the <table>-<32 hex> name filter, the resolves-outside refusal, the `keep`
#      exclusion, and dry-run deleting nothing) is pinned -- mirroring the BIG
#      sibling test_gen_perf_corpus_3068.sh.
#   8. BOTH row-count cross-checks FIRE, in both directions: "COPY imported N, the
#      CSV held M" and "Statistics.db totalRows == sstabledump rows", plus the
#      manifest writer's plan-vs-Statistics.db rows AND partitions checks and its
#      refusal to fabricate an unobserved partition count.
#   8b. ...and neither layer trusts OUTPUT WITHOUT A STATUS (roborev #3234 M1): the
#      stub can print a complete, valid-looking metadata block and then exit nonzero
#      (STUB_META_EXIT), which both the writer and the generator must refuse -- that is
#      the one shape a returncode-blind parser cannot tell from success.
#   8c. The corpus SHAPE is verified, not assumed (roborev #3234 M2): one SSTable per
#      planned chunk, at generations 1..N, in BOTH layers, with too-few / too-many / a
#      GAP-at-the-right-count negative controls each. The generation count selects the
#      scan route and is what the AC3 figure is attributed to, so a silent drift there
#      misattributes a published number.
#   8d. The COMMITTED small-golden manifest carries NO production-only claim (roborev
#      #3234 L3) and its recorded sha256s still match the committed bytes.
#   8e. A manifest field is OBSERVED or ABSENT (roborev #3234 M1/M2). The deleted claims
#      -- the fixed AC3 throughput figure, the flag that labelled it inapplicable while
#      still printing it, `full_generation_golden`, and the
#      corpus_committed/committed_copy/corpus_note narrative inferred from a Data.db-only
#      hash match -- must not reappear in the writer OR in either committed manifest, and
#      the one surviving location field is asserted to be exactly as wide as its check.
#   8f. The COMMITTED manifests cannot fall behind the WRITER (roborev #3234 L4): the
#      production artifact's key set is compared against a manifest the suite has just
#      written, and against the small golden's, so staleness is a test failure.
#   8g. The TOC is a MANIFEST, not evidence (roborev #3234 M3): --verify-only must reject a
#      component that is listed but absent (one control per component) and one present but
#      unlisted.
#   9. The suite ITSELF cannot report success while having stopped running cases:
#      passes are counted against a declared floor, and each of the two legitimate
#      skips (no python3; < 5 GiB free) declares the case count it drops so that
#      count is credited against the floor and appears in the summary line.
#  10. ...and it cannot report success while a case was never RUN AT ALL. roborev
#      #3234 M1: two `check_reject` calls sat ABOVE the definition of check_reject,
#      so bash printed "command not found", `fails` stayed 0, and the two
#      committed-manifest protections (themselves the fix for an earlier finding)
#      were asserted by nothing. Now: a `command_not_found_handle` makes an unknown
#      name a HARD exit, `self_audit_helper_order` statically re-reads this file for
#      call-before-definition, every helper is defined in the helpers section above
#      its first use, and MIN_CASES is the EXACT pass count -- so deleting or
#      short-circuiting any one case reds the suite (proven by mutation, see below).
#
# Hermetic: no docker, no sudo, no Cassandra, no network, no datasets. The
# container-dependent paths (8, and the manifest happy path in 5) run against
# scripts/tests/fixtures/stub-docker-cassandra-bti.py -- a stub `docker` handed to
# the generator via DOCKER=/--docker, which fabricates the metadata TEXT Cassandra
# would have printed. Everything else uses --help / --validate-only /
# --verify-only / --yaml-flip-check / --prune-dry-run, none of which start
# anything.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEN="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti.sh"
ROWS_PY="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti-rows.py"
MANIFEST_PY="$REPO_ROOT/test-data/scripts/write-perf-corpus-bti-manifest.py"

# Case accounting (rust-reviewer NIT on #3234). `fails=0` alone cannot tell "every
# case passed" from "the suite stopped running cases half way and exited clean", so
# the passes are counted and checked against a declared floor at the end. Two
# blocks here are legitimately conditional (no python3; less than 5 GiB free under
# TMPDIR), so each declares HOW MANY cases it drops via `skip`, the dropped count
# is credited against the floor, and both reach the SUMMARY line -- previously a
# SKIP was a bare echo that no summary ever mentioned.
#
# MIN_CASES is the full-suite pass count; SKIP_PY / SKIP_E2E are the case counts of
# the two conditional blocks (python3-only cases, of which the stub end-to-end cases
# are the inner block). Growing the suite means growing these.
#
# The floor is AUTHORITATIVE, and that is checked rather than asserted: it is set to
# the EXACT full-suite pass count (not a slack lower bound), so deleting or
# short-circuiting any single case drops `passes` below it and reds the suite. Proven
# by mutation, not by inspection (see the header note on the roborev M1 finding).
MIN_CASES=149
SKIP_PY_CASES=59
SKIP_E2E_CASES=22

fails=0
passes=0
skipped_cases=0
skips=0
pass() { echo "ok   - $1"; passes=$((passes + 1)); }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }
# skip <cases-not-run> <reason...>
skip() {
  local n="$1"
  shift
  echo "SKIP - $* ($n case(s) NOT run)"
  skipped_cases=$((skipped_cases + n))
  skips=$((skips + 1))
}

# ------------------------------------------------------- vacuous-case guards ---
# roborev #3234 M1: two `check_reject` calls were placed ABOVE the definition of
# `check_reject`. With no `set -e` bash merely printed "command not found",
# `fails` never moved, and the two committed-manifest protections were asserted by
# NOTHING while the suite reported ALL PASS.
#
# `set -euo pipefail` is NOT usable here and that is a deliberate, load-bearing
# choice: ~100 cases are built on the `out=$(bash "$GEN" ...); rc=$?` idiom, whose
# whole point is to observe an EXPECTED non-zero exit. Under `set -e` the first such
# assignment aborts the run, so `-e` would not harden this suite, it would delete it.
# `set -uo pipefail` is on (above). The two guards below cover the defect class `-e`
# would have covered here:
#
#   1. RUNTIME: a call to a name that is not a command/function records a NAMED
#      failure the summary reds on, not a printed warning that leaves `fails` at 0.
#   2. STATIC: `self_audit_helper_order` (run as its own case, first thing) re-reads
#      THIS file and fails if any helper it defines is invoked on a line ABOVE its
#      definition -- i.e. it detects the exact M1 shape mechanically, for every
#      helper, including ones added later.
#
# MEASURED, and the reason the sentinel FILE exists: bash invokes
# `command_not_found_handle` "in a separate execution environment", so its `exit 97`
# ends only the handler -- the run continues and no in-shell counter it touches
# survives. So it appends the offending name to a sentinel file, which the summary
# turns into a counted `fail`. (`CNF_EXPECT=1` marks the one deliberate probe.)
CNF_SENTINEL=""   # set to a real path once TMP exists
CNF_EXPECT=0
command_not_found_handle() {
  echo "FAIL - internal: '$1' is not a command or function -- a typo, or a helper" \
    "invoked BEFORE it was defined. An unrun case must never be able to look like a" \
    "passing one." >&2
  if [ "$CNF_EXPECT" != 1 ] && [ -n "$CNF_SENTINEL" ]; then
    printf '%s\n' "$1" >>"$CNF_SENTINEL"
  fi
  exit 97
}

# helper_order_findings <file>: prints one line per shell function in <file> whose
# FIRST call site precedes its definition. Deliberately a STATIC pass over the file
# text, so it also audits lines the current run never executes (a case inside a
# skipped conditional block) -- which runtime detection cannot do.
helper_order_findings() {
  awk '
    # A HEREDOC BODY is data, not shell: skip from the `<<TAG` opener to its
    # terminator. (Needed because this suite embeds python and a shell fixture in
    # heredocs; `<<<"$x"` here-STRINGS are not openers and must not match.)
    in_heredoc { if ($0 == hd_tag) in_heredoc = 0; next }
    # COMMENTS FIRST, and that order is load-bearing: a comment is not a call, and a
    # comment that merely MENTIONS `<<TAG` must not be read as a heredoc opener --
    # doing so swallowed the rest of the file and made this auditor miss the very
    # mutation it exists to catch (found by mutation-testing it, not by reading it).
    /^[ \t]*#/ { next }
    match($0, /<<-?[ \t]*'"'"'?[A-Za-z_][A-Za-z0-9_]*'"'"'?/) {
      hd_tag = substr($0, RSTART, RLENGTH)
      gsub(/[<'"'"'\- \t]/, "", hd_tag)
      in_heredoc = 1
    }
    # definition: optional indent, name, "() {"
    match($0, /^[ \t]*[A-Za-z_][A-Za-z0-9_]*\(\)[ \t]*\{/) {
      name = $0
      sub(/^[ \t]*/, "", name)
      sub(/\(\).*$/, "", name)
      if (!(name in defline)) defline[name] = NR
      next
    }
    { lines[NR] = $0 }
    END {
      for (name in defline) {
        for (n = 1; n <= NR; n++) {
          if (!(n in lines)) continue
          # a CALL: the name in COMMAND POSITION (start of line, or after a
          # separator / subshell open), followed by a word boundary.
          if (lines[n] ~ ("(^[ \t]*|[;&|(][ \t]*|\\$\\([ \t]*)" name "([ \t]|$)")) {
            if (n < defline[name]) {
              printf "%s called at line %d but defined at line %d\n", name, n, defline[name]
            }
            break
          }
        }
      }
    }
  ' "$1" | sort
}

# The audit runs as TWO cases: this file is clean, AND the auditor demonstrably
# detects the M1 shape on a fabricated file. Without the negative control the clean
# verdict would be exactly as trustworthy as the finding it is meant to prevent.
self_audit_helper_order() {
  local bad ctl_file ctl
  bad=$(helper_order_findings "$0")
  if [ -z "$bad" ]; then
    pass "static self-audit: no helper in this file is called before it is defined"
  else
    fail "static self-audit found call-before-definition (the roborev M1 shape):
$bad"
  fi
  ctl_file="$TMP/helper-order-negative-control.sh"
  cat >"$ctl_file" <<'CTLEOF'
#!/usr/bin/env bash
later_helper "this call is above the definition"
early_helper() { :; }
early_helper ok
later_helper() { :; }
CTLEOF
  ctl=$(helper_order_findings "$ctl_file")
  if [ "$ctl" = "later_helper called at line 2 but defined at line 5" ]; then
    pass "the static auditor DETECTS a call-before-definition (negative control)"
  else
    fail "helper-order auditor negative control: expected the line-2/line-5 finding, got: $ctl"
  fi
  # ...and the RUNTIME guard: calling a name that is not a command or function must
  # be a hard non-zero exit, never bash's advisory "command not found" that leaves
  # `fails` at 0 (exactly what let the M1 finding hide). Probed in a subshell so the
  # handler's `exit` ends the probe rather than this run; the captured text is
  # deliberately NOT echoed (it contains the word FAIL).
  local probe_out probe_rc
  CNF_EXPECT=1
  probe_out=$(this_helper_does_not_exist_probe 2>&1); probe_rc=$?
  CNF_EXPECT=0
  if [ "$probe_rc" -eq 97 ] && grep -q "is not a command or function" <<<"$probe_out"; then
    pass "an undefined name is a HARD failure (command_not_found_handle, exit 97)"
  else
    fail "command_not_found_handle did not fire (rc=$probe_rc)"
  fi
}

# check_reject <label> <expect-substring> <args...>  (--validate-only --out prepended)
#
# Every rejection must be non-zero AND must not have created the corpus root it was
# pointed at.
#
# The no-write half is asserted on the DESTINATION THE INVOCATION ACTUALLY
# REQUESTED. The earlier shape counted leftovers in a scratch dir no invocation was
# ever pointed at (every case passed --out "$TMP/c"), so `leftovers` was
# structurally always 0 and eleven cases claimed a property nothing checked. The
# per-case --out is unique, so the check is live: it fails the moment validation
# starts creating (or pruning under) --out before the flags are checked.
#
# Defined HERE, above every use, because it used to be defined half way down the
# file -- below two of its callers (roborev #3234 M1).
check_reject() {
  local label="$1" expect="$2"; shift 2
  local dest="$TMP/rej-$RANDOM-$RANDOM/corpus"
  local out rc existed
  # A caller-supplied --out in "$@" deliberately WINS (later wins in the parser),
  # which is how the bad---out cases are expressed; $dest must be untouched either way.
  out=$(bash "$GEN" --validate-only --out "$dest" "$@" 2>&1); rc=$?
  existed=no; [ -e "$dest" ] && existed=yes
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out" && [ "$existed" = no ]; then
    pass "rejects $label"
  else
    fail "$label: expected non-zero + '$expect' + no $dest (rc=$rc, dest-created=$existed, out: $out)"
  fi
}

# int32_probe <python-snippet>: runs the snippet with the row driver imported as
# `mod`, printing whatever the snippet prints plus a terminal ACCEPTED, or
# "REJECTED: <message>" when the snippet's ceiling check raises SystemExit. Lets the
# `pk int` boundary be pinned on the FUNCTIONS (fast, exact) instead of only through
# a multi-hundred-thousand-row generation run.
int32_probe() {
  python3 - "$ROWS_PY" "$1" <<'PROBEEOF'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rows", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
try:
    exec(compile(sys.argv[2], "<int32-probe>", "exec"), {"mod": mod})
except SystemExit as exc:
    print(f"REJECTED: {exc}")
else:
    print("ACCEPTED")
PROBEEOF
}

for f in "$GEN" "$ROWS_PY" "$MANIFEST_PY"; do
  [ -f "$f" ] || { echo "FAIL - missing $f"; exit 1; }
done

TMP="$(mktemp -d)"
CNF_SENTINEL="$TMP/command-not-found-names"
# shellcheck disable=SC2317  # invoked indirectly by the EXIT trap below
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# First cases of the run: this file's own helpers are all defined before use, and the
# auditor that says so is itself proven to detect the opposite (roborev #3234 M1).
self_audit_helper_order

# ------------------------------------------------------------------ usage -----
out=$(bash "$GEN" --help 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^  --smoke' <<<"$out" && grep -q '^  --verify-only' <<<"$out" \
   && grep -q '^  --seed S' <<<"$out" && grep -q '^  --rows N' <<<"$out"; then
  pass "--help exits 0 and lists the modes + flags"
else
  fail "--help: expected 0 and a flag listing (rc=$rc)"
fi

for bad in --bogus -x "--rows"; do
  out=$(bash "$GEN" "$bad" 2>&1); rc=$?
  if [ "$rc" -eq 2 ]; then
    pass "rejects '$bad' with exit 2"
  else
    fail "'$bad': expected exit 2, got $rc"
  fi
done

# --------------------------------------------------------- flag validation ----
out=$(bash "$GEN" --validate-only --out "$TMP/c" --rows 1000 --chunk-rows 250 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VALIDATE-OK rows=1000 chunk_rows=250 chunks=4 " <<<"$out"; then
  pass "--validate-only reports the resolved chunk count and runs nothing"
else
  fail "--validate-only: expected chunks=4 (rc=$rc, out: $out)"
fi

# --smoke lowers rows/chunk-rows and defaults the keyspace away from production.
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=perf_bti_smoke" <<<"$out" && ! grep -q "rows=10200000" <<<"$out"; then
  pass "--smoke lowers the row count and defaults keyspace=perf_bti_smoke"
else
  fail "--smoke: expected a lowered row count + perf_bti_smoke (out: $out)"
fi
out=$(bash "$GEN" --smoke --keyspace mine --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=mine" <<<"$out"; then
  pass "--smoke does not override an explicit --keyspace"
else
  fail "--smoke overrode an explicit --keyspace (out: $out)"
fi
# --smoke is DEFAULTS-only: an explicitly supplied --rows/--chunk-rows (or the
# ROWS/CHUNK_ROWS env equivalent) must survive it. It used to replace both
# unconditionally, silently ignoring what the caller asked for.
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --rows 7000 --chunk-rows 3500 2>&1)
if grep -q "VALIDATE-OK rows=7000 chunk_rows=3500 chunks=2 " <<<"$out"; then
  pass "--smoke keeps an explicit --rows AND --chunk-rows"
else
  fail "--smoke overrode an explicit --rows/--chunk-rows (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --rows 600000 2>&1)
if grep -q "rows=600000 chunk_rows=120000 chunks=5 " <<<"$out"; then
  pass "--smoke keeps an explicit --rows and still lowers --chunk-rows"
else
  fail "--smoke did not combine an explicit --rows with the smoke chunk size (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --chunk-rows 60000 2>&1)
if grep -q "rows=240000 chunk_rows=60000 chunks=4 " <<<"$out"; then
  pass "--smoke keeps an explicit --chunk-rows and still lowers --rows"
else
  fail "--smoke overrode an explicit --chunk-rows (out: $out)"
fi
out=$(ROWS=900000 CHUNK_ROWS=300000 bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "rows=900000 chunk_rows=300000 chunks=3 " <<<"$out"; then
  pass "--smoke keeps ROWS/CHUNK_ROWS supplied through the environment"
else
  fail "--smoke overrode the ROWS/CHUNK_ROWS env values (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "rows=240000 chunk_rows=120000 chunks=2 " <<<"$out"; then
  pass "--smoke lowers both defaults when neither was supplied"
else
  fail "--smoke default plan changed (out: $out)"
fi
out=$(bash "$GEN" --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=perf_bti " <<<"$out"; then
  pass "production default keyspace is perf_bti"
else
  fail "expected keyspace=perf_bti by default (out: $out)"
fi

# ------------------------------- the COMMITTED manifest is never a default ----
# roborev #3234 F2: MANIFEST_OUT used to DEFAULT to the committed
# test-data/perf-corpus-bti-manifest.json, so the advertised `--smoke` invocation
# overwrote a committed provenance artifact with perf_bti_smoke metadata -- after
# which the default full-corpus scan rejects that manifest as foreign (exit 8).
COMMITTED_MANIFEST_REL="test-data/perf-corpus-bti-manifest.json"
for mode_args in "" "--smoke" "--small-golden"; do
  # shellcheck disable=SC2086  # deliberate word split of an optional single flag
  out=$(bash "$GEN" $mode_args --validate-only --out "$TMP/c" 2>&1)
  if grep -q "manifest_out=(none)" <<<"$out"; then
    pass "the committed manifest is NOT a default destination (${mode_args:-production})"
  else
    fail "${mode_args:-production}: expected manifest_out=(none) (out: $out)"
  fi
done
out=$(bash "$GEN" --publish-manifest --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "manifest_out=.*$COMMITTED_MANIFEST_REL" <<<"$out"; then
  pass "--publish-manifest is the EXPLICIT opt-in that targets the committed manifest"
else
  fail "--publish-manifest: expected the committed manifest as the target (rc=$rc, out: $out)"
fi
check_reject "--publish-manifest from a --smoke run" \
  "refusing to write the COMMITTED production manifest" --smoke --publish-manifest
check_reject "--publish-manifest from a --small-golden run" \
  "refusing to write the COMMITTED production manifest" --small-golden --publish-manifest
out=$(bash "$GEN" --publish-manifest --manifest-out "$TMP/m.json" --validate-only \
        --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "mutually exclusive" <<<"$out"; then
  pass "--publish-manifest and --manifest-out are mutually exclusive (exit 2)"
else
  fail "--publish-manifest + --manifest-out: expected exit 2 (rc=$rc, out: $out)"
fi

# ---------------------------------------- --small-golden (the committable oracle) --
out=$(bash "$GEN" --small-golden --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 0 ] \
   && grep -q "rows=600 chunk_rows=600 chunks=1 " <<<"$out" \
   && grep -q "keyspace=test_da table=wide_multiclustering_small " <<<"$out" \
   && grep -q "mode=small_golden" <<<"$out"; then
  pass "--small-golden plans ONE small SSTable under test_da.wide_multiclustering_small"
else
  fail "--small-golden defaults changed (rc=$rc, out: $out)"
fi
# The width mix is what fixes the golden's SIZE and partition shape, so it is
# pinned too: the committed fixture is sized to the repo convention (#3032's
# multiclustering_table, 468 rows / 121,020 B golden), and a silent widths change
# would silently resize a committed fixture on the next regeneration.
if grep -q "widths=400:20,80:30,20:50" <<<"$out"; then
  pass "--small-golden pins the rows-per-partition mix that fixes the fixture's size"
else
  fail "--small-golden widths default changed (out: $out)"
fi
out=$(bash "$GEN" --small-golden --validate-only --out "$TMP/c" --rows 900 --chunk-rows 300 \
        --table mine 2>&1)
if grep -q "rows=900 chunk_rows=300 chunks=3 " <<<"$out" && grep -q "table=mine " <<<"$out"; then
  pass "--small-golden is DEFAULTS-only: an explicit --rows/--chunk-rows/--table wins"
else
  fail "--small-golden overrode explicit flags (out: $out)"
fi
out=$(bash "$GEN" --smoke --small-golden --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "mutually exclusive" <<<"$out"; then
  pass "--smoke and --small-golden are mutually exclusive (exit 2)"
else
  fail "--smoke --small-golden: expected exit 2 (rc=$rc, out: $out)"
fi

# (`check_reject` itself is defined in the helpers section at the top of the file.)
check_reject "--rows 0"            "must be >= 1"          --rows 0
check_reject "a non-integer --rows" "non-negative integer" --rows 12x
check_reject "--chunk-rows > --rows" "exceeds"             --rows 100 --chunk-rows 1000
check_reject "a relative --out"    "absolute path"         --out relative/path
check_reject "an empty --out"      "is empty"              --out ""
check_reject "--out /"             "refusing to use"       --out /
# --out is CANONICALIZED before anything is created or deleted (roborev #3234 F1).
# A lexical `!= "/"` test passed all three of these, and the script then ran
# `rm -rf "$OUT/cassandra-data"` as root -- i.e. deleted an unrelated /cassandra-data.
# Every case here asserts the REFUSAL; nothing is deleted by any of them.
check_reject "an --out that resolves to / through .." "resolves to '/'" --out /tmp/..
ln -sfn / "$TMP/slash-link"
check_reject "an --out SYMLINK resolving to /" "resolves to '/'" --out "$TMP/slash-link"
check_reject "an --out resolving to a system root" "a system root" --out /var/../var
# Positive control for the same mechanism: a legitimate path is ACCEPTED, and the
# resolved (symlink-free) form is what the run reports and therefore derives its
# destructive targets from.
mkdir -p "$TMP/canon-real/corpus"
ln -sfn "$TMP/canon-real" "$TMP/canon-link"
canon_expect="$(cd "$TMP/canon-real" && pwd -P)/corpus"
out=$(bash "$GEN" --validate-only --out "$TMP/canon-link/corpus" --rows 100 --chunk-rows 100 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "out=$canon_expect mode=" <<<"$out"; then
  pass "a symlinked --out is ACCEPTED and reported in its canonical form"
else
  fail "canonicalization positive control: expected out=$canon_expect (rc=$rc, out: $out)"
fi
check_reject "a bad keyspace"      "invalid keyspace"      --keyspace "Bad-KS"
check_reject "a bad table"         "invalid table"         --table "bad table"
check_reject "an empty --seed"     "seed is empty"         --seed ""
check_reject "a malformed --widths" "widths"               --widths "200"
check_reject "duplicate bucket first bytes" "widths"       --buckets "alpha,ateam"
check_reject "an empty --rows"     "non-negative integer"  --rows ""
# An explicitly EMPTY env value is a caller bug, not a request for the default.
emptydest="$TMP/empty-env/corpus"
out=$(ROWS="" bash "$GEN" --validate-only --out "$emptydest" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "non-negative integer" <<<"$out" && [ ! -e "$emptydest" ]; then
  pass "rejects an empty ROWS in the environment (never silently the default)"
else
  fail "empty ROWS env: expected non-zero + no writes (rc=$rc, out: $out)"
fi
# A SUCCESSFUL --validate-only must also write nothing: it runs before preflight,
# which is the only thing allowed to create the corpus root.
okdest="$TMP/validate-writes-nothing/corpus"
bash "$GEN" --validate-only --out "$okdest" --rows 1000 --chunk-rows 500 >/dev/null 2>&1
if [ ! -e "$okdest" ]; then
  pass "a passing --validate-only creates no corpus root either"
else
  fail "--validate-only created $okdest"
fi
# `pk` is a CQL `int`, so chunk N's key base (N * PK_STRIDE) has a hard ceiling.
# REGRESSION (issue #3234): the original 1e9 stride made chunk 3 start at
# 3,000,000,000 > INT32_MAX, and the 27-chunk production run died there — four
# minutes and three SSTables in — with a cqlsh ParseError, while the 2-chunk
# --smoke run never reached it. This pins the refusal at VALIDATE time (before any
# container), and the two cases below pin the boundary itself so a future stride
# change cannot silently reopen the hole.
check_reject "a plan over the \`pk int\` ceiling" "INT32_MAX" \
  --rows 2200000000 --chunk-rows 500000
out=$(bash "$GEN" --validate-only --out "$TMP/c" --rows 13200000 --chunk-rows 500000 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "chunks=27 " <<<"$out"; then
  pass "the 27-chunk production plan fits the \`pk int\` ceiling"
else
  fail "production plan (27 chunks) must validate (rc=$rc, out: $out)"
fi

# --------------------------------------- AC1/AC2 asserts via --verify-only ----
# A fabricated `da` corpus: the asserts are file-level, so no container is needed.
# Data.db is sparse (truncate), which is exactly what the size assert reads.
make_corpus() { # make_corpus <dir> <data-bytes> <rows-db-bytes> [generation]
  local dir="$1" data="$2" rows="$3" g="${4:-1}" stem
  stem="da-$g-bti"
  mkdir -p "$dir"
  truncate -s "$data" "$dir/$stem-Data.db"
  if [ "$rows" -gt 0 ]; then truncate -s "$rows" "$dir/$stem-Rows.db"; else : >"$dir/$stem-Rows.db"; fi
  local c
  for c in Partitions.db Statistics.db CompressionInfo.db Filter.db; do
    truncate -s 64 "$dir/$stem-$c"
  done
  printf 'x\n' >"$dir/$stem-Digest.crc32"
  printf 'Data.db\nStatistics.db\nDigest.crc32\nTOC.txt\nCompressionInfo.db\nFilter.db\nPartitions.db\nRows.db\n' \
    >"$dir/$stem-TOC.txt"
}
# `--rows 1 --chunk-rows 1` => CHUNKS=1, which is what these fabricated one-SSTable
# corpora hold: assert_corpus now requires the SSTable count to EQUAL the configured
# chunk count and the generations to be 1..CHUNKS (roborev #3234 M2), so a verify run
# must declare the shape it is verifying. The default (production) 27-chunk
# configuration would legitimately reject a 1-SSTable corpus -- which is the point, and
# is pinned by its own negative controls below.
verify() { # verify <corpus-root>
  bash "$GEN" --verify-only --out "$1" --keyspace perf_bti --table wide_multiclustering \
    --rows 1 --chunk-rows 1 2>&1
}
# verify_shape <corpus-root> <rows> <chunk-rows>: the same, with an explicit chunk plan,
# for the one-SSTable-per-chunk cases (which need CHUNKS != the SSTable count).
verify_shape() {
  bash "$GEN" --verify-only --out "$1" --keyspace perf_bti --table wide_multiclustering \
    --rows "$2" --chunk-rows "$3" 2>&1
}

root="$TMP/good"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-0123456789abcdef0123456789abcdef" 9437184 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VERIFY-OK " <<<"$out" && grep -q "largest_data_db=9437184" <<<"$out"; then
  pass "--verify-only accepts a well-formed da corpus (positive control)"
else
  fail "--verify-only on a good corpus: expected VERIFY-OK (rc=$rc, out: $out)"
fi
# The ambiguity check is REPORTED on the success line, so a pasted VERIFY-OK shows it ran.
if grep -q "corpus_dirs=1" <<<"$out"; then
  pass "VERIFY-OK reports the corpus-dir count the ambiguity check counted"
else
  fail "expected corpus_dirs=1 on the VERIFY-OK line; out: $out"
fi

# AMBIGUOUS root (roborev #3234 M1). --no-prune leaves several <table>-<uuid> dirs in
# the DISCOVERABLE tree. This used to verify each one independently and still print
# VERIFY-OK, while the SSTable count it reported described only the last -- and a
# consumer scanning that tree sees the UNION, so the generation count (which selects
# the scan route and is what any read-path figure is attributed to) silently changed
# with no assertion anywhere disagreeing. Both dirs here are individually VALID, which
# is the point: nothing else in the verifier can see the problem.
root="$TMP/ambiguous"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-4123456789abcdef0123456789abcdef" 9437184 4096
make_corpus "$root/sstables/perf_bti/wide_multiclustering-5123456789abcdef0123456789abcdef" 9437184 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AMBIGUOUS corpus root: 2 " <<<"$out" \
   && grep -q "generation count" <<<"$out" && grep -q -- "--no-prune" <<<"$out" \
   && ! grep -q "VERIFY-OK" <<<"$out"; then
  pass "--verify-only HARD-FAILS on an ambiguous corpus root and prints no VERIFY-OK"
else
  fail "ambiguous-root case: expected a hard failure naming --no-prune (rc=$rc, out: $out)"
fi

# AC1 negative control: an `nb-*` descriptor is the SILENT failure mode of a
# missed yaml setting and must be a hard failure.
root="$TMP/nb"
d="$root/sstables/perf_bti/wide_multiclustering-1123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
truncate -s 1024 "$d/nb-1-big-Data.db"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AC1: non-BTI descriptor" <<<"$out"; then
  pass "--verify-only HARD-FAILS on an nb-* descriptor (AC1)"
else
  fail "AC1 nb-* case: expected a hard failure (rc=$rc, out: $out)"
fi

# AC2 negative control: an empty Rows.db means no row-index trie to profile.
root="$TMP/emptyrows"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-2123456789abcdef0123456789abcdef" 9437184 0
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "Rows.db is EMPTY" <<<"$out"; then
  pass "--verify-only HARD-FAILS on an empty Rows.db (AC2)"
else
  fail "AC2 empty-Rows.db case: expected a hard failure (rc=$rc, out: $out)"
fi

# AC2 negative control: below 8 MiB the two read planes are the same mapping.
root="$TMP/small"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-3123456789abcdef0123456789abcdef" 1048576 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "needs > 8388608" <<<"$out"; then
  pass "--verify-only HARD-FAILS below the 8 MiB read-plane floor (AC2)"
else
  fail "AC2 8MiB-floor case: expected a hard failure (rc=$rc, out: $out)"
fi

# The floor is STRICT (> 8388608, not >=): MADV_RANDOM is applied at
# `file_size >= 8 MiB`, so a Data.db of EXACTLY 8 MiB leaves nothing above the
# threshold to A/B against. Pin the boundary itself, not just a value far below it.
root="$TMP/exact8m"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-6123456789abcdef0123456789abcdef" 8388608 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "largest Data.db is 8388608 B, needs > 8388608" <<<"$out"; then
  pass "--verify-only HARD-FAILS at EXACTLY 8388608 B (the floor is strict)"
else
  fail "AC2 exact-8MiB boundary: expected a hard failure (rc=$rc, out: $out)"
fi
root="$TMP/exact8m1"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-7123456789abcdef0123456789abcdef" 8388609 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "largest_data_db=8388609" <<<"$out"; then
  pass "--verify-only accepts 8388609 B (one byte over the floor)"
else
  fail "AC2 boundary+1: expected VERIFY-OK (rc=$rc, out: $out)"
fi

# ------------------------------- one SSTable per chunk (roborev #3234 M2) ------
# NOBODY verified that the number of emitted SSTables equals the row plan's chunk
# count. The aggregate row/partition cross-checks CANNOT see this: an unexpected flush
# split or a compaction preserves every row and every partition while destroying the
# promised one-SSTable-per-chunk shape -- and the GENERATION COUNT is what selects the
# scan route and what the AC3 throughput figure is attributed to ("27 generations,
# generation_merge::stream_generations_for_read"), so a corpus with a different
# generation count silently makes that attribution wrong.
#
# Three negative controls, because there are three distinct ways to violate it:
# too few, too many, and a GAP in the numbering at the right count (the compaction
# signature -- a compaction promotes its output to a new, higher generation).
root="$TMP/shape-few"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-9123456789abcdef0123456789abcdef" 9437184 4096
out=$(verify_shape "$root" 2 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AC: 1 SSTable(s) in" <<<"$out" \
   && grep -q "plans" <<<"$out" && grep -q "2 chunk(s)" <<<"$out"; then
  pass "--verify-only HARD-FAILS when there are FEWER SSTables than planned chunks"
else
  fail "shape too-few case: expected a chunk-count failure naming both numbers (rc=$rc, out: $out)"
fi

root="$TMP/shape-many"
d="$root/sstables/perf_bti/wide_multiclustering-a123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096 1
make_corpus "$d" 9437184 4096 2
out=$(verify_shape "$root" 1 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AC: 2 SSTable(s) in" <<<"$out" && grep -q "1 chunk(s)" <<<"$out"; then
  pass "--verify-only HARD-FAILS when there are MORE SSTables than planned chunks"
else
  fail "shape too-many case: expected a chunk-count failure naming both numbers (rc=$rc, out: $out)"
fi

# The generation GAP: the count is RIGHT (2 == 2), so this case can only be caught by
# the generation-mapping check -- i.e. it proves that check is independent of the count.
root="$TMP/shape-gap"
d="$root/sstables/perf_bti/wide_multiclustering-b123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096 1
make_corpus "$d" 9437184 4096 3
out=$(verify_shape "$root" 2 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AC: generation mapping" <<<"$out" \
   && grep -q "expected generations" <<<"$out" && ! grep -q "SSTable(s) in" <<<"$out"; then
  pass "--verify-only HARD-FAILS on a GAP in the generation numbering at the right count"
else
  fail "shape gap case: expected a generation-mapping failure (rc=$rc, out: $out)"
fi

# Positive control for the same assert: 2 SSTables, generations 1..2, 2 planned chunks.
root="$TMP/shape-ok"
d="$root/sstables/perf_bti/wide_multiclustering-c123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096 1
make_corpus "$d" 9437184 4096 2
out=$(verify_shape "$root" 2 1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VERIFY-OK " <<<"$out" && grep -q "sstables=2" <<<"$out" \
   && grep -q "one SSTable per chunk: 2 == 2 chunk(s); generations 1 2" <<<"$out"; then
  pass "--verify-only accepts 2 SSTables at generations 1..2 for 2 chunks (positive control)"
else
  fail "shape positive control: expected VERIFY-OK with sstables=2 (rc=$rc, out: $out)"
fi

# TOC contract: Index.db/Summary.db are BIG-only and must never appear.
# The expected substring must be the ASSERT's own message, not a bare component
# name: the die path echoes the whole TOC, so `grep -q "Index.db"` was satisfied by
# the failure message of ANY unrelated TOC failure.
for bigonly in Index.db Summary.db; do
  root="$TMP/bigtoc-$bigonly"
  d="$root/sstables/perf_bti/wide_multiclustering-4123456789abcdef0123456789abcdef"
  make_corpus "$d" 9437184 4096
  printf '%s\n' "$bigonly" >>"$d/da-1-bti-TOC.txt"
  out=$(verify "$root"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "TOC.txt lists $bigonly" <<<"$out"; then
    pass "--verify-only HARD-FAILS when the TOC lists BIG-only $bigonly"
  else
    fail "TOC $bigonly case: expected 'TOC.txt lists $bigonly' (rc=$rc, out: $out)"
  fi
  # ... and a BIG-only component FILE is fatal even when the TOC does not list it.
  root="$TMP/bigfile-$bigonly"
  d="$root/sstables/perf_bti/wide_multiclustering-8123456789abcdef0123456789abcdef"
  make_corpus "$d" 9437184 4096
  truncate -s 128 "$d/da-1-bti-$bigonly"
  out=$(verify "$root"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "has a $bigonly file" <<<"$out"; then
    pass "--verify-only HARD-FAILS on a stray BIG-only $bigonly file"
  else
    fail "stray $bigonly file: expected 'has a $bigonly file' (rc=$rc, out: $out)"
  fi
done

# A missing BTI component in the TOC is also fatal.
root="$TMP/notoc"
d="$root/sstables/perf_bti/wide_multiclustering-5123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
grep -v '^Rows.db$' "$d/da-1-bti-TOC.txt" >"$d/toc.tmp" && mv "$d/toc.tmp" "$d/da-1-bti-TOC.txt"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "missing Rows.db" <<<"$out"; then
  pass "--verify-only HARD-FAILS when the TOC omits Rows.db"
else
  fail "TOC-omission case: expected a hard failure (rc=$rc, out: $out)"
fi

# ---- the TOC is a MANIFEST, not evidence: the files must EXIST (roborev #3234 M3) ----
# assert_corpus checked that the expected component NAMES appear in TOC.txt and never
# that the corresponding files exist, so DELETING Statistics.db, CompressionInfo.db,
# Partitions.db or Filter.db while leaving the TOC untouched still printed VERIFY-OK.
# That is a fail-closed hole in the verifier itself -- and the exact shape of a
# half-copied, half-pruned or partially-published corpus, i.e. the thing --verify-only
# exists to catch. One negative control per component, plus the other direction (a
# component file the TOC does not list), plus the positive control that the intact corpus
# still verifies (above).
for missing in Statistics.db CompressionInfo.db Partitions.db Filter.db; do
  root="$TMP/tocfile-$missing"
  d="$root/sstables/perf_bti/wide_multiclustering-9123456789abcdef0123456789abcdef"
  make_corpus "$d" 9437184 4096
  rm "$d/da-1-bti-$missing"
  out=$(verify "$root"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "lists $missing but da-1-bti-$missing is" <<<"$out"; then
    pass "--verify-only HARD-FAILS when $missing is DELETED but still in the TOC"
  else
    fail "deleted-$missing case: expected a TOC-lists-but-absent failure (rc=$rc, out: $out)"
  fi
done
root="$TMP/tocextra"
d="$root/sstables/perf_bti/wide_multiclustering-a123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
truncate -s 32 "$d/da-1-bti-Bogus.db"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "component set disagrees with its TOC" <<<"$out"; then
  pass "--verify-only HARD-FAILS on a component FILE the TOC does not list"
else
  fail "unlisted-component case: expected a component-set disagreement (rc=$rc, out: $out)"
fi
# ...and a `*-Data.db.jsonl` sstabledump golden beside the components is NOT a component:
# the real corpus carries one, so excluding it from that comparison is load-bearing.
root="$TMP/tocgolden"
d="$root/sstables/perf_bti/wide_multiclustering-b123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
printf '{"partition":{}}\n' >"$d/da-1-bti-Data.db.jsonl"
out=$(verify "$root"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VERIFY-OK " <<<"$out"; then
  pass "--verify-only accepts an sstabledump golden beside the components (not a component)"
else
  fail "golden-beside-components case: expected VERIFY-OK (rc=$rc, out: $out)"
fi

out=$(bash "$GEN" --verify-only --out "$TMP/nonexistent-root" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no corpus at" <<<"$out"; then
  pass "--verify-only fails closed when there is no corpus"
else
  fail "--verify-only with no corpus: expected a hard failure (rc=$rc, out: $out)"
fi

# ------------------------------------------- the cassandra.yaml BTI flip -------
# The generator's most consequential upstream guard: a stock Cassandra 5.0 node
# emits `nb` (BIG) with NO error when either setting misses, and the `sed`
# addresses depend on the shipped file's exact comment markers and TWO-SPACE
# indentation ("#  selected_format: big"). Driven through --yaml-flip-check, which
# runs the PRODUCTION snippet -- the same text apply_bti_yaml runs in the container
# -- against a copy of the committed cassandra:5.0.2 excerpt.
YAML_FIXTURE="$REPO_ROOT/scripts/tests/fixtures/cassandra-5.0.2-cassandra.yaml.excerpt"
if [ ! -f "$YAML_FIXTURE" ]; then
  fail "missing the committed cassandra.yaml excerpt fixture: $YAML_FIXTURE"
else
  # Fixture-rot guard: the excerpt must still be in the SHIPPED (unflipped) form,
  # or the positive case below would be proving nothing.
  if grep -qx '#sstable:' "$YAML_FIXTURE" \
     && grep -qx '#  selected_format: big' "$YAML_FIXTURE" \
     && grep -qx 'storage_compatibility_mode: CASSANDRA_4' "$YAML_FIXTURE"; then
    pass "the committed cassandra:5.0.2 excerpt is in the shipped (unflipped) form"
  else
    fail "the yaml excerpt fixture is no longer in the shipped form (the flip cases below would be vacuous)"
  fi

  y="$TMP/yaml-ok.yaml"; cp "$YAML_FIXTURE" "$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -eq 0 ] && grep -q "YAML-FLIP-OK" <<<"$out" \
     && grep -qx 'storage_compatibility_mode: NONE' "$y" \
     && grep -qx 'sstable:' "$y" && grep -qx '  selected_format: bti' "$y"; then
    pass "the yaml flip sets BOTH mandatory settings on the shipped 5.0.2 file"
  else
    fail "yaml flip on the shipped file: expected both settings flipped (rc=$rc, out: $out)"
  fi

  # Negative: THREE-space indentation. The sed address no longer matches, so
  # selected_format stays commented and the node would silently emit `nb`.
  y="$TMP/yaml-indent.yaml"
  sed 's|^#  selected_format: big|#   selected_format: big|' "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "selected_format was NOT set to bti" <<<"$out"; then
    pass "yaml flip HARD-FAILS when selected_format's indentation drifts"
  else
    fail "yaml indentation drift: expected a hard failure (rc=$rc, out: $out)"
  fi

  # Negative: the `#sstable:` block header is absent, so the child key would be
  # orphaned even if it flipped.
  y="$TMP/yaml-nosstable.yaml"
  grep -vx '#sstable:' "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "sstable: block was NOT uncommented" <<<"$out"; then
    pass "yaml flip HARD-FAILS when the sstable: block header is missing"
  else
    fail "yaml missing sstable: block: expected a hard failure (rc=$rc, out: $out)"
  fi

  # Negative: the node is not on the shipped CASSANDRA_4 default, so the
  # storage_compatibility_mode substitution finds nothing to replace.
  y="$TMP/yaml-mode.yaml"
  sed 's|^storage_compatibility_mode: CASSANDRA_4|storage_compatibility_mode: UPGRADING|' \
    "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "storage_compatibility_mode was NOT set to NONE" <<<"$out"; then
    pass "yaml flip HARD-FAILS when storage_compatibility_mode is not the shipped default"
  else
    fail "yaml unexpected compatibility mode: expected a hard failure (rc=$rc, out: $out)"
  fi

  out=$(bash "$GEN" --yaml-flip-check "$TMP/no-such-yaml" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "no such cassandra.yaml" <<<"$out"; then
    pass "yaml flip HARD-FAILS on a missing cassandra.yaml"
  else
    fail "yaml missing file: expected a hard failure (rc=$rc, out: $out)"
  fi
fi

# ------------------------------------------------- prune scope (dry run only) --
# prune_stale_table_dirs does `$SUDO rm -rf` on MULTI-GB paths, so each guard is
# pinned: the symlink skip, the ^<table>-<32 hex>$ name filter, the
# resolves-outside refusal, the `keep` exclusion, and "a dry run deletes nothing".
# Mirrors the BIG sibling scripts/tests/test_gen_perf_corpus_3068.sh.
PCORPUS="$TMP/prune/corpus"
PKS="$PCORPUS/sstables/perf_bti"
POUTSIDE="$TMP/prune/outside-the-corpus"
mkdir -p "$PKS" "$POUTSIDE/precious"
UA="8cc9d0708a2711f1a82281d620fbe729"
UB="90c037f08a2711f1a82281d620fbe729"
USYM="${UA//8/a}"
mkdir -p "$PKS/wide_multiclustering-$UA" \
         "$PKS/wide_multiclustering-$UB" \
         "$PKS/wide_multiclustering-backup" \
         "$PKS/wide_multiclustering-$UA/nested/wide_multiclustering-$UB" \
         "$PKS/other_table-$UA"
touch "$PKS/wide_multiclustering-$UA-notes.txt"
ln -s "$POUTSIDE/precious" "$PKS/wide_multiclustering-$USYM"
# WOULD-PRUNE prints the RESOLVED path, so compare against the resolved keyspace dir.
PKS_REAL="$(cd "$PKS" && pwd -P)"

prune_dry() { # prune_dry [env-prefixed args...] -> stdout+stderr of a dry run
  bash "$GEN" --prune-dry-run --out "$PCORPUS" \
    --keyspace perf_bti --table wide_multiclustering 2>&1
}
out=$(prune_dry); rc=$?
would=$(grep '^WOULD-PRUNE ' <<<"$out" | sed 's/^WOULD-PRUNE //' | sort)
expected=$(printf '%s\n' "$PKS_REAL/wide_multiclustering-$UA" \
                         "$PKS_REAL/wide_multiclustering-$UB" | sort)
if [ "$rc" -eq 0 ] && [ "$would" = "$expected" ]; then
  pass "prune targets exactly the <table>-<32 hex> dirs"
else
  fail "prune candidate set wrong (rc=$rc)
  got:
$would
  expected:
$expected"
fi
if grep -q "skipping symlink (never followed)" <<<"$out"; then
  pass "prune skips a symlinked corpus dir explicitly (never followed)"
else
  fail "prune did not report the symlink skip (out: $out)"
fi
if grep -q "skipping 'wide_multiclustering-backup' (not a <table>-<uuid> corpus dir)" <<<"$out"; then
  pass "prune's name filter rejects a non-<uuid> suffix"
else
  fail "prune did not report the name-filter skip (out: $out)"
fi
for never in "$PKS_REAL/wide_multiclustering-backup" \
             "$PKS_REAL/other_table-$UA" \
             "$PKS_REAL/wide_multiclustering-$USYM" \
             "$PKS_REAL/wide_multiclustering-$UA/nested/wide_multiclustering-$UB" \
             "$PKS_REAL/wide_multiclustering-$UA-notes.txt" \
             "$POUTSIDE/precious"; do
  if grep -qF "WOULD-PRUNE $never" <<<"$out"; then
    fail "prune would have removed '$never'"
  else
    pass "prune does not target '${never#"$TMP"/}'"
  fi
done
missing=0
for must_exist in "$PKS/wide_multiclustering-$UA" "$PKS/wide_multiclustering-$UB" \
                  "$PKS/wide_multiclustering-backup" "$PKS/other_table-$UA" \
                  "$PKS/wide_multiclustering-$UA-notes.txt" "$POUTSIDE/precious"; do
  [ -e "$must_exist" ] || { fail "--prune-dry-run deleted $must_exist"; missing=1; }
done
[ "$missing" = 0 ] && pass "--prune-dry-run deletes nothing"

# The `keep` exclusion: publish() passes the basename it is about to publish, and
# that one dir must never be a candidate.
out=$(PRUNE_KEEP="wide_multiclustering-$UA" prune_dry)
if grep -qF "WOULD-PRUNE $PKS_REAL/wide_multiclustering-$UB" <<<"$out" \
   && ! grep -qF "WOULD-PRUNE $PKS_REAL/wide_multiclustering-$UA" <<<"$out"; then
  pass "prune excludes the dir being published (keep)"
else
  fail "prune ignored the keep exclusion (out: $out)"
fi

# A candidate that RESOLVES OUTSIDE the corpus keyspace dir (here: the keyspace dir
# itself is a symlink) must abort the prune, not be deleted through.
ECORPUS="$TMP/prune-escape/corpus"
EOUTSIDE="$TMP/prune-escape/elsewhere"
mkdir -p "$ECORPUS/sstables" "$EOUTSIDE/wide_multiclustering-$UA"
ln -s "$EOUTSIDE" "$ECORPUS/sstables/perf_bti"
out=$(bash "$GEN" --prune-dry-run --out "$ECORPUS" \
        --keyspace perf_bti --table wide_multiclustering 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "resolves OUTSIDE the corpus keyspace dir" <<<"$out" \
   && [ -d "$EOUTSIDE/wide_multiclustering-$UA" ]; then
  pass "prune REFUSES a candidate that resolves outside the corpus keyspace dir"
else
  fail "prune escape case: expected a refusal (rc=$rc, out: $out)"
fi

out=$(bash "$GEN" --prune-dry-run --out "$TMP/prune-never-generated" \
        --keyspace perf_bti --table wide_multiclustering 2>&1); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q WOULD-PRUNE <<<"$out"; then
  pass "no corpus yet: prune is a clean no-op"
else
  fail "prune on a non-existent corpus root: expected a no-op (rc=$rc, out: $out)"
fi

# ------------------------------------------------- row driver determinism -----
if command -v python3 >/dev/null 2>&1; then
  gen_rows() { # gen_rows <out> <plan> <seed> <chunk>
    python3 "$ROWS_PY" --chunk-index "$4" --rows 3000 --seed "$3" --payload-bytes 32 \
      --widths 200:60,800:30 --buckets alpha,bo,charlie,delta \
      --out "$1" --plan-out "$2" >/dev/null 2>&1
  }
  gen_rows "$TMP/a.csv" "$TMP/a.jsonl" 4242 0
  gen_rows "$TMP/b.csv" "$TMP/b.jsonl" 4242 0
  gen_rows "$TMP/c.csv" "$TMP/c.jsonl" 4242 1
  gen_rows "$TMP/d.csv" "$TMP/d.jsonl" 9999 0
  sa=$(sha256sum <"$TMP/a.csv" | cut -d' ' -f1)
  sb=$(sha256sum <"$TMP/b.csv" | cut -d' ' -f1)
  sc=$(sha256sum <"$TMP/c.csv" | cut -d' ' -f1)
  sd=$(sha256sum <"$TMP/d.csv" | cut -d' ' -f1)
  if [ "$sa" = "$sb" ]; then
    pass "row driver is deterministic for the same (seed, chunk)"
  else
    fail "row driver is NOT deterministic for the same (seed, chunk)"
  fi
  if [ "$sa" != "$sc" ] && [ "$sa" != "$sd" ]; then
    pass "row driver varies with both the chunk index and the seed"
  else
    fail "row driver did not vary with chunk index ($sa vs $sc) / seed ($sa vs $sd)"
  fi
  if [ "$(wc -l <"$TMP/a.csv" | tr -d ' ')" = "3000" ]; then
    pass "row driver emits EXACTLY the requested row count"
  else
    fail "row driver emitted $(wc -l <"$TMP/a.csv") rows, expected 3000"
  fi
  planned=$(python3 -c 'import json,sys;print(sum(json.loads(l)["rows"] for l in open(sys.argv[1]) if l.strip()))' "$TMP/a.jsonl")
  if [ "$planned" = "3000" ]; then
    pass "row plan record reports the observed row count"
  else
    fail "row plan reported $planned rows, expected 3000"
  fi
  # Partition keys must not collide across chunks (one partition, one SSTable).
  overlap=$(python3 - "$TMP/a.csv" "$TMP/c.csv" <<'PY'
import sys
def pks(p):
    return {l.split(",", 1)[0] for l in open(p)}
print(len(pks(sys.argv[1]) & pks(sys.argv[2])))
PY
)
  if [ "$overlap" = "0" ]; then
    pass "partition keys never collide across chunks"
  else
    fail "chunks 0 and 1 share $overlap partition keys"
  fi
  for bad in "--widths 200:0" "--buckets alpha,ateam" "--buckets alpha" "--payload-bytes 2"; do
    # shellcheck disable=SC2086  # deliberate word split of the flag pair
    out=$(python3 "$ROWS_PY" --chunk-index 0 --rows 10 --seed 1 --payload-bytes 32 \
      --widths 200:1 --buckets alpha,bo --out "$TMP/z.csv" --plan-out "$TMP/z.jsonl" $bad 2>&1); rc=$?
    if [ "$rc" -ne 0 ]; then
      pass "row driver rejects '$bad'"
    else
      fail "row driver accepted '$bad' (out: $out)"
    fi
  done

  # ------------------------- the determinism claim is PINNED, not argued (M2) ----
  # roborev #3234 M2: the driver used random.choices()/random.sample(), whose
  # ALGORITHMS CPython documents as implementation details, under an unpinned
  # `python3` -- so a different interpreter could silently change every partition
  # width and payload while the manifests kept advertising the old seed identity. The
  # PRNG and both selection algorithms are now VENDORED in the driver, and
  # `--self-check` regenerates a fixed set of configurations and compares the CSV
  # bytes against digests committed IN that file (one of them the committed small
  # golden's exact row set).
  out=$(python3 "$ROWS_PY" --self-check 2>&1); rc=$?
  if [ "$rc" -eq 0 ] && grep -q "^SELF-CHECK-OK 4 pinned determinism vector" <<<"$out"; then
    pass "the row driver's 4 pinned determinism vectors reproduce byte-for-byte"
  else
    fail "row-driver --self-check: expected SELF-CHECK-OK over 4 vectors (rc=$rc, out: $out)"
  fi
  out=$(python3 "$ROWS_PY" --self-check --rows 10 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "takes no other arguments" <<<"$out"; then
    pass "--self-check refuses to be combined with generation flags"
  else
    fail "--self-check with extra args: expected a usage failure (rc=$rc, out: $out)"
  fi
  # ...and the pin is LIVE, in both layers. A digest nobody can make fail is
  # decoration, so break the vendored PRNG core and then the vendored sampling, in
  # COPIES of the driver, and require --self-check to catch each.
  prng_mutation_case() { # prng_mutation_case <label> <sed-expression>
    local label="$1" expr="$2" copy o r
    copy="$TMP/rows-mutated-$RANDOM.py"
    sed "$expr" "$ROWS_PY" >"$copy"
    if cmp -s "$copy" "$ROWS_PY"; then
      fail "$label: the mutation did not change the file (stale sed expression)"
      return
    fi
    o=$(python3 "$copy" --self-check 2>&1); r=$?
    if [ "$r" -ne 0 ] && grep -q "self-check FAILED" <<<"$o"; then
      pass "--self-check CATCHES $label (the pinned digests are live)"
    else
      fail "$label: --self-check passed on a mutated driver (rc=$r, out: $o)"
    fi
  }
  # NOTE on the chosen bits: flipping bit 0 of the tempering mask is NOT a valid
  # mutation -- `y << 7` zeroes the low 7 bits, so mask bits 0..6 are unreachable and
  # the output is unchanged. (Measured: that mutation passed --self-check. The digests
  # were right; the mutation was vacuous. Bit 8 is reachable.)
  prng_mutation_case "a one-bit change to MT19937's MATRIX_A recurrence constant" \
    's/_MT_MATRIX_A = 0x9908B0DF/_MT_MATRIX_A = 0x9908B0DE/'
  prng_mutation_case "a one-bit change to the MT19937 tempering mask (bit 8)" \
    's/0x9D2C5680/0x9D2C5780/'
  prng_mutation_case "a changed range in the vendored sampling algorithm" \
    's/j = rnd\.below(n - i)/j = rnd.below(n)/'

  # ------------------------------- the `pk int` ceiling is INCLUSIVE (L4) --------
  # roborev #3234 L4: keys of chunk N span `N*PK_STRIDE .. N*PK_STRIDE + rows - 1`,
  # so the largest key a plan can emit is `(chunks-1)*PK_STRIDE + chunk_rows - 1`.
  # Both ceiling checks compared `base + rows` (EXCLUSIVE), so a plan whose final key
  # is EXACTLY INT32_MAX -- a perfectly valid `int` key -- was rejected. The boundary
  # is pinned from BOTH sides, on both checks, plus the "use at most N chunks" advice
  # the refusal prints (an off-by-one there would advise an unusable plan).
  #
  # 2148 chunks x stride 1,000,000 -> last base 2,147,000,000; + 483,648 rows - 1
  # = 2,147,483,647 = INT32_MAX exactly.
  got=$(int32_probe 'print("max_pk", mod.max_pk_of_plan(2148, 483648)); mod.plan_fits_int32(2148, 483648)')
  if [ "$got" = "max_pk 2147483647
ACCEPTED" ]; then
    pass "plan_fits_int32 ACCEPTS a plan whose final key is EXACTLY INT32_MAX"
  else
    fail "plan_fits_int32 at the INT32_MAX boundary: expected max_pk 2147483647 + ACCEPTED, got: $got"
  fi
  got=$(int32_probe 'mod.plan_fits_int32(2148, 483649)')
  if grep -q "^REJECTED: plan overflows" <<<"$got" && grep -q "reaches pk 2147483648" <<<"$got"; then
    pass "plan_fits_int32 REJECTS the plan one key past INT32_MAX"
  else
    fail "plan_fits_int32 at INT32_MAX+1: expected a refusal naming 2147483648, got: $got"
  fi
  got=$(int32_probe 'print("base", mod.chunk_fits_int32(2147, 483648))')
  if [ "$got" = "base 2147000000
ACCEPTED" ]; then
    pass "chunk_fits_int32 ACCEPTS a chunk whose final key is EXACTLY INT32_MAX"
  else
    fail "chunk_fits_int32 at the INT32_MAX boundary: expected base 2147000000 + ACCEPTED, got: $got"
  fi
  got=$(int32_probe 'mod.chunk_fits_int32(2147, 483649)')
  if grep -q "^REJECTED: chunk 2147" <<<"$got" && grep -q "reaches pk 2147483648" <<<"$got"; then
    pass "chunk_fits_int32 REJECTS the chunk one key past INT32_MAX"
  else
    fail "chunk_fits_int32 at INT32_MAX+1: expected a refusal naming 2147483648, got: $got"
  fi
  # The advised max chunk count must itself FIT, and one more must not -- i.e. the
  # advice is the true maximum, not an off-by-one guess.
  got=$(int32_probe '
import re
try:
    mod.plan_fits_int32(9999, 500000)
except SystemExit as exc:
    n = int(re.search(r"Use at most (\d+) chunks", str(exc)).group(1))
print("advised", n, "max_pk", mod.max_pk_of_plan(n, 500000))
mod.plan_fits_int32(n, 500000)
try:
    mod.plan_fits_int32(n + 1, 500000)
except SystemExit:
    print("one-more REJECTED")
')
  if [ "$got" = "advised 2147 max_pk 2146499999
one-more REJECTED
ACCEPTED" ]; then
    pass "the refusal's advised chunk count is exactly the largest one that fits"
  else
    fail "max_chunks advice is not the true maximum, got: $got"
  fi

  # ------------------------------------------- manifest writer fail-closed ----
  # shellcheck disable=SC2054  # the commas are inside flag VALUES (--widths/--buckets)
  man_args=(--corpus-root "$TMP" --keyspace perf_bti --table wide_multiclustering
            --image cassandra:5.0.2 --seed 1 --rows-requested 10 --chunk-rows 10
            --payload-bytes 32 --widths 200:1 --buckets alpha,bo --mode smoke
            --row-plan "$TMP/a.jsonl" --out "$TMP/manifest.json")
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/nope" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "not a directory" <<<"$out"; then
    pass "manifest writer rejects a missing --sstable-dir"
  else
    fail "manifest writer with a missing dir: expected a hard failure (rc=$rc, out: $out)"
  fi
  mkdir -p "$TMP/empty-dir"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/empty-dir" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "refusing to write a manifest" <<<"$out" && [ ! -f "$TMP/manifest.json" ]; then
    pass "manifest writer refuses an SSTable-less directory and writes nothing"
  else
    fail "manifest writer on an empty dir: expected a hard failure (rc=$rc, out: $out)"
  fi
  # An nb-* descriptor beside a da-* one is a hard failure BEFORE any container run.
  mkdir -p "$TMP/mixed"
  make_corpus "$TMP/mixed" 1024 64
  truncate -s 64 "$TMP/mixed/nb-1-big-Data.db"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/mixed" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "non-BTI descriptor" <<<"$out" && [ ! -f "$TMP/manifest.json" ]; then
    pass "manifest writer refuses a directory holding an nb-* descriptor"
  else
    fail "manifest writer on a mixed dir: expected a hard failure (rc=$rc, out: $out)"
  fi

  # An unreadable / partial ROW PLAN must be an actionable SystemExit naming the
  # line, not a JSONDecodeError or KeyError traceback out of the aggregation.
  printf '{"chunk": 0, "rows": 10, ' >"$TMP/plan-truncated.jsonl"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/empty-dir" \
          --row-plan "$TMP/plan-truncated.jsonl" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && ! grep -q "Traceback" <<<"$out"; then
    pass "manifest writer reports a truncated row-plan line without a traceback"
  else
    fail "truncated row plan: expected a clean failure (rc=$rc, out: $out)"
  fi

  # ----------------------- the row plan must describe THIS configuration --------
  # roborev #3234 F3: the aggregate rows/partitions cross-checks against
  # Statistics.db cannot see a STALE plan -- one from an earlier run, with a
  # different seed, whose totals happen to match. The manifest would then declare a
  # seed and a generation plan that do not describe the corpus. So the plan's chunk
  # count, contiguity, per-chunk row counts and per-chunk seed material are checked
  # against --seed/--rows-requested/--chunk-rows before anything is written.
  #
  # HERMETIC: the check runs BEFORE the per-SSTable `sstablemetadata` containers, so
  # these cases need no docker. The positive control proves that ordering -- with a
  # MATCHING plan the run gets past the plan check and dies at the metadata step,
  # with `--docker` pointed at a command that is not docker.
  plan_rec() { # plan_rec <chunk> <seed-material> <rows>
    printf '{"chunk":%s,"seed_material":"%s","rows":%s,"partitions":2,"pk_min":0,' "$1" "$2" "$3"
    printf '"pk_max":9,"rows_per_partition_histogram":{"200":1,"800":1},'
    printf '"buckets_per_partition_histogram":{"4":2}}\n'
  }
  mkdir -p "$TMP/plancfg" "$TMP/bin"
  make_corpus "$TMP/plancfg" 1024 64
  # make_corpus's CompressionInfo.db is 64 zero bytes, which the (self-verifying)
  # CompressionInfo parser rejects. The positive control has to get PAST the plan
  # check and reach the metadata step, so give this one dir a REAL header:
  # UTF compressor, option count, chunk length, max compressed length, data length,
  # chunk count, chunk offsets (read-compression-info.py's documented layout).
  python3 - "$TMP/plancfg/da-1-bti-CompressionInfo.db" <<'PY'
import struct, sys
def utf(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">H", len(b)) + b
buf = utf("LZ4Compressor") + struct.pack(">i", 1) + utf("chunk_length_in_kb") + utf("16")
buf += struct.pack(">iiqi", 16384, 2147483647, 1024, 1) + struct.pack(">q", 0)
open(sys.argv[1], "wb").write(buf)
PY
  printf '#!/bin/sh\nexit 1\n' >"$TMP/bin/not-docker"; chmod +x "$TMP/bin/not-docker"
  # shellcheck disable=SC2054  # the commas are inside flag VALUES (--widths/--buckets)
  plancfg_args=(--corpus-root "$TMP" --keyspace perf_bti --table wide_multiclustering
                --image cassandra:5.0.2 --seed 77 --rows-requested 1000 --chunk-rows 400
                --payload-bytes 32 --widths 200:1 --buckets alpha,bo --mode production
                --sstable-dir "$TMP/plancfg" --docker "$TMP/bin/not-docker"
                --out "$TMP/plancfg-manifest.json")
  # plan_case <label> <expect-substring> <plan-file>. NOT a pipeline stage: the
  # right-hand side of a pipeline runs in a SUBSHELL, so `pass`/`fail` would print
  # their line while their counter increment was discarded -- the case would then be
  # invisible to the declared case-count floor.
  plan_case() {
    local label="$1" expect="$2" plan="$3"
    rm -f "$TMP/plancfg-manifest.json"
    local o r
    o=$(python3 "$MANIFEST_PY" "${plancfg_args[@]}" --row-plan "$plan" 2>&1); r=$?
    if [ "$r" -ne 0 ] && grep -q "row-plan/config check FAILED" <<<"$o" \
       && grep -q "$expect" <<<"$o" && [ ! -f "$TMP/plancfg-manifest.json" ]; then
      pass "manifest writer HARD-FAILS on $label, and writes nothing"
    else
      fail "$label: expected a row-plan/config failure naming '$expect' (rc=$r, out: $o)"
    fi
  }
  { plan_rec 0 78:0 400; plan_rec 1 78:1 400; plan_rec 2 78:2 200; } >"$TMP/plan-seed.jsonl"
  plan_case "a plan generated from ANOTHER seed" "seed_material" "$TMP/plan-seed.jsonl"
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; plan_rec 3 77:3 200; } >"$TMP/plan-gap.jsonl"
  plan_case "a NON-CONTIGUOUS chunk set" "chunk index set" "$TMP/plan-gap.jsonl"
  { plan_rec 0 77:0 500; plan_rec 1 77:1 400; plan_rec 2 77:2 200; } >"$TMP/plan-wide.jsonl"
  plan_case "a plan whose chunk rows disagree with --chunk-rows" "puts 400 there" \
    "$TMP/plan-wide.jsonl"
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; } >"$TMP/plan-short.jsonl"
  plan_case "a plan SHORT of the configured chunk count" "chunk count" "$TMP/plan-short.jsonl"
  # Positive control: a matching plan (ONE chunk, matching the one fabricated SSTable)
  # passes BOTH the config check and the one-SSTable-per-chunk check, so the run proceeds
  # to the (deliberately unavailable) sstablemetadata step instead of failing here.
  # `not-docker` exits 1, so what it dies on is the writer's exit-status check.
  { plan_rec 0 77:0 400; } >"$TMP/plan-cfg-ok.jsonl"
  rm -f "$TMP/plancfg-manifest.json"
  out=$(python3 "$MANIFEST_PY" --corpus-root "$TMP" --keyspace perf_bti \
          --table wide_multiclustering --image cassandra:5.0.2 --seed 77 \
          --rows-requested 400 --chunk-rows 400 --payload-bytes 32 --widths 200:1 \
          --buckets alpha,bo --mode production --sstable-dir "$TMP/plancfg" \
          --docker "$TMP/bin/not-docker" --out "$TMP/plancfg-manifest.json" \
          --row-plan "$TMP/plan-cfg-ok.jsonl" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && ! grep -q "row-plan/config check FAILED" <<<"$out" \
     && ! grep -q "one-SSTable-per-chunk check FAILED" <<<"$out" \
     && grep -q "sstablemetadata FAILED for" <<<"$out"; then
    pass "a plan that MATCHES the configuration passes the check (positive control)"
  else
    fail "plan/config positive control: expected to get past the plan check (rc=$rc, out: $out)"
  fi

  # ------------- one SSTable per planned chunk, in the WRITER (roborev #3234 M2) -------
  # Same defect, second layer: the manifest writer published `sstable_count` without ever
  # comparing it to `len(plan["chunks"])`. Hermetic -- the check runs BEFORE the
  # per-SSTable `sstablemetadata` containers, so a wrong-shaped corpus costs no container
  # start (and these cases need no docker).
  #
  # `mk_gens <dir> <gen>...` fabricates just the Data.db files the shape check reads.
  mk_gens() {
    local dir="$1"; shift
    rm -rf "$dir"; mkdir -p "$dir"
    local g
    for g in "$@"; do truncate -s 1024 "$dir/da-$g-bti-Data.db"; done
  }
  # shape_case <label> <expect-substring> <dir> [must-not-appear]: the plan below has 3
  # chunks (1000 rows / 400 per chunk), so the shape is varied by the DIRECTORY.
  shape_case() {
    local label="$1" expect="$2" dir="$3" absent="${4:-}"
    rm -f "$TMP/shapecfg-manifest.json"
    local o r
    o=$(python3 "$MANIFEST_PY" --corpus-root "$TMP" --keyspace perf_bti \
          --table wide_multiclustering --image cassandra:5.0.2 --seed 77 \
          --rows-requested 1000 --chunk-rows 400 --payload-bytes 32 --widths 200:1 \
          --buckets alpha,bo --mode production --sstable-dir "$dir" \
          --docker "$TMP/bin/not-docker" --out "$TMP/shapecfg-manifest.json" \
          --row-plan "$TMP/plan-cfg-shape.jsonl" 2>&1); r=$?
    if [ "$r" -ne 0 ] && grep -q "one-SSTable-per-chunk check FAILED" <<<"$o" \
       && grep -q "$expect" <<<"$o" && [ ! -f "$TMP/shapecfg-manifest.json" ] \
       && { [ -z "$absent" ] || ! grep -q "$absent" <<<"$o"; }; then
      pass "manifest writer HARD-FAILS on $label, and writes nothing"
    else
      fail "$label: expected a one-SSTable-per-chunk failure naming '$expect' (rc=$r, out: $o)"
    fi
  }
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; plan_rec 2 77:2 200; } >"$TMP/plan-cfg-shape.jsonl"
  mk_gens "$TMP/shape-writer-few" 1 2
  shape_case "FEWER SSTables than planned chunks" \
    "2 SSTable(s) on disk, the row plan has 3 chunk(s)" "$TMP/shape-writer-few"
  mk_gens "$TMP/shape-writer-many" 1 2 3 4
  shape_case "MORE SSTables than planned chunks" \
    "4 SSTable(s) on disk, the row plan has 3 chunk(s)" "$TMP/shape-writer-many"
  # Right COUNT, wrong generations: only the generation-mapping half can catch it.
  mk_gens "$TMP/shape-writer-gap" 1 2 4
  shape_case "a GAP in the generation numbering at the right count" \
    "expected generations 1..3" "$TMP/shape-writer-gap" "SSTable count:"

  # ------- the COMMITTED small-golden manifest describes the COMMITTED bytes ------
  # roborev #3234 L3: this manifest is a COMMITTED provenance artifact, and it stated
  # that the 600-row committed corpus is uncommitted and multi-GB, and that a
  # 500,000-row "full generation golden" belongs to it. Both were plainly untrue. This
  # case pins the absence of every production-only claim, and the next one pins that the
  # sha256s it records still match the committed bytes -- so a metadata-only rewrite can
  # never drift from the fixture it describes.
  SMALL_GOLDEN_MANIFEST="$REPO_ROOT/test-data/perf-corpus-bti-small-golden-manifest.json"
  if [ ! -f "$SMALL_GOLDEN_MANIFEST" ]; then
    fail "missing the committed small-golden manifest: $SMALL_GOLDEN_MANIFEST"
  else
    out=$(python3 - "$SMALL_GOLDEN_MANIFEST" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
blob = json.dumps(m)
t = m["tables"][0]
bad = []
if m["mode"] != "small_golden":
    bad.append(f"mode: {m['mode']!r}")
# roborev #3234 M2: the committed-ness NARRATIVE is gone. What is left is the one field
# whose name is its whole claim -- and for this corpus it must be PRESENT and must point
# into the checkout, because the committed Data.db is exactly what this manifest
# describes.
if m.get("data_db_sha256_also_match_at") != (
    "test-data/datasets/" + t["sstable_dir"]
):
    bad.append(f"data_db_sha256_also_match_at: {m.get('data_db_sha256_also_match_at')!r}")
for k in ("corpus_committed", "committed_copy", "corpus_note",
          "read_path_measurement_scope", "recorded_figure", "full_generation_golden",
          "applies_to_this_corpus", "what_the_ac3_figure_measures"):
    if k in blob:
        bad.append(f"deleted key {k} is back")
if "multi-GB" in blob:
    bad.append("a 97,780 B committed fixture is described as multi-GB")
# The production figure and the 500k-row golden must not appear ANYWHERE in the file.
for claim in ("13200000", "103804", "127.163", "160752721", "500,000 rows", "153.3"):
    if claim in blob:
        bad.append(f"production-only claim {claim!r} present")
if "Profileable" in m["purpose"]:
    bad.append("purpose calls a sub-8-MiB correctness oracle profileable")
if t["min_data_db_floor_bytes"] != 0:
    bad.append(
        f"min_data_db_floor_bytes: {t['min_data_db_floor_bytes']} — --small-golden "
        "enforces no read-plane floor, so reporting 8 MiB reads as a violated one"
    )
if t["sstable_count"] != len(m["rows_per_partition"]["chunks"]):
    bad.append("sstable_count != planned chunk count")
if t["sstable_generations"] != [1]:
    bad.append(f"sstable_generations: {t['sstable_generations']!r}")
print("SMALL-GOLDEN-SCOPE-OK" if not bad else "BAD: " + "; ".join(bad))
PY
    )
    if [ "$out" = "SMALL-GOLDEN-SCOPE-OK" ]; then
      pass "the committed small-golden manifest carries NO production-only claims"
    else
      fail "committed small-golden manifest: $out"
    fi

    # ...and it still describes the COMMITTED BYTES: every recorded sha256 re-hashed
    # from the committed Data.db. A metadata-only rewrite must not have touched them.
    out=$(python3 - "$SMALL_GOLDEN_MANIFEST" "$REPO_ROOT" <<'PY'
import hashlib, json, os, sys
m = json.load(open(sys.argv[1]))
root = sys.argv[2]
t = m["tables"][0]
d = os.path.join(root, "test-data", "datasets", t["sstable_dir"])
bad = []
for s in t["sstables"]:
    p = os.path.join(d, f"{s['sstable_basename']}-Data.db")
    if not os.path.exists(p):
        bad.append(f"missing committed {p}")
        continue
    h = hashlib.sha256(open(p, "rb").read()).hexdigest()
    if h != s["data_db_sha256"]:
        bad.append(f"{s['sstable_basename']}: recorded {s['data_db_sha256']}, on disk {h}")
    if os.path.getsize(p) != s["data_db_bytes"]:
        bad.append(f"{s['sstable_basename']}: size {os.path.getsize(p)} != recorded")
print("SMALL-GOLDEN-BYTES-OK" if not bad else "BAD: " + "; ".join(bad))
PY
    )
    if [ "$out" = "SMALL-GOLDEN-BYTES-OK" ]; then
      pass "the committed small-golden manifest's sha256s match the committed Data.db"
    else
      fail "committed small-golden bytes: $out"
    fi
  fi

  # ---- the COMMITTED PRODUCTION manifest is not STALE against the writer (#3234 L4) ----
  # It had fallen three contracts behind the writer that produced it: no
  # `sstable_generations`, no `one_sstable_per_planned_chunk`, no
  # `read_plane_threshold_bytes`. The PRINCIPAL committed artifact therefore did not
  # record the guarantees the writer publishes -- a review finding that should have been a
  # test failure. These cases make staleness fail:
  #
  #   1. the contract keys are PRESENT, and the deleted claims are ABSENT;
  #   2. the manifest is self-consistent (counts, generations, sums, maxima all agree
  #      with the per-SSTable records it carries) -- so a hand-edit is caught too;
  #   3. both committed manifests carry the SAME key set, so one cannot drift alone.
  #
  # Hermetic: pure JSON arithmetic, no corpus and no container. The writer-vs-committed
  # key-set comparison (which needs a freshly written manifest) is in the e2e block below.
  PROD_MANIFEST="$REPO_ROOT/test-data/perf-corpus-bti-manifest.json"
  if [ ! -f "$PROD_MANIFEST" ]; then
    fail "missing the committed production manifest: $PROD_MANIFEST"
  else
    out=$(python3 - "$PROD_MANIFEST" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
t = m["tables"][0]
plan = m["rows_per_partition"]
blob = json.dumps(m)
bad = []
for k in ("issue", "mode", "generated_utc", "generator", "row_driver", "method",
          "cassandra_image", "cassandra_yaml_settings_required", "keyspace", "table",
          "keyspace_ddl", "seed", "row_driver_config", "rows_per_partition",
          "corpus_root", "datasets_root_usage", "reproducibility", "provenance",
          "tables", "row_count_cross_check"):
    if k not in m:
        bad.append(f"top-level key {k} MISSING")
for k in ("sstable_count", "sstable_generations", "one_sstable_per_planned_chunk",
          "rows", "partitions", "data_db_bytes_total", "data_db_bytes_largest",
          "min_data_db_floor_bytes", "read_plane_threshold_bytes",
          "meets_8mib_read_plane_floor", "rows_db_bytes_total", "every_rows_db_non_empty",
          "ddl", "sstables"):
    if k not in t:
        bad.append(f"tables[0] key {k} MISSING")
for k in ("read_path_measurement_scope", "recorded_figure", "applies_to_this_corpus",
          "full_generation_golden", "corpus_committed", "committed_copy", "corpus_note"):
    if k in blob:
        bad.append(f"deleted key {k} is back")
# ...and the three LITERALS that asserted what the evidence beside them already shows.
for owner, k in ((t, "clustering_key"), (t, "clustering_arity"),
                 (m["row_count_cross_check"], "agree")):
    if k in owner:
        bad.append(f"deleted literal {k} is back")
# roborev #3234 M1, at the STRONGEST point: this manifest describes the very corpus the
# AC3 figure was measured on -- the one corpus for which "it applies" would have been
# true -- and it still records NO throughput number. Omission is unconditional, so there
# is no identity a near-match can satisfy to inherit one.
for claim in ("103804", "127.163", "160,752,721", "153.3"):
    if claim in blob:
        bad.append(f"throughput/derived-golden claim {claim!r} present")
# A missing key would make every arithmetic check below raise, and an empty FAIL message
# is a bad diagnostic -- report the missing keys and stop.
if bad:
    print("BAD: " + "; ".join(bad))
    raise SystemExit(0)
# Self-consistency: every aggregate must equal what the per-SSTable records say.
if t["sstable_count"] != len(t["sstables"]):
    bad.append("sstable_count != len(sstables)")
if t["sstable_count"] != len(plan["chunks"]):
    bad.append("sstable_count != planned chunk count")
if t["sstable_generations"] != list(range(1, t["sstable_count"] + 1)):
    bad.append(f"sstable_generations: {t['sstable_generations']!r}")
if t["one_sstable_per_planned_chunk"] is not True:
    bad.append("one_sstable_per_planned_chunk must be True")
if t["rows"] != sum(s["rows"] for s in t["sstables"]) or t["rows"] != plan["rows"]:
    bad.append("rows != sum(per-SSTable rows) or != the row plan")
if t["partitions"] != sum(s["statistics"]["partition_count"] for s in t["sstables"]):
    bad.append("partitions != sum(per-SSTable partition_count)")
if t["data_db_bytes_total"] != sum(s["data_db_bytes"] for s in t["sstables"]):
    bad.append("data_db_bytes_total != sum(per-SSTable data_db_bytes)")
if t["data_db_bytes_largest"] != max(s["data_db_bytes"] for s in t["sstables"]):
    bad.append("data_db_bytes_largest != max(per-SSTable data_db_bytes)")
if t["read_plane_threshold_bytes"] != 8 * 1024 * 1024:
    bad.append(f"read_plane_threshold_bytes: {t['read_plane_threshold_bytes']}")
if t["meets_8mib_read_plane_floor"] is not (t["data_db_bytes_largest"] > 8 * 1024 * 1024):
    bad.append("meets_8mib_read_plane_floor disagrees with the recorded bytes")
if any(len(s["data_db_sha256"]) != 64 for s in t["sstables"]):
    bad.append("a data_db_sha256 is not 64 hex chars")
if {c["seed_material"] for c in plan["chunks"]} != {
        f"{m['seed']}:{c['chunk']}" for c in plan["chunks"]}:
    bad.append("a chunk's seed_material does not derive from the recorded seed")
print("PROD-MANIFEST-CONTRACT-OK" if not bad else "BAD: " + "; ".join(bad))
PY
    )
    if [ "$out" = "PROD-MANIFEST-CONTRACT-OK" ]; then
      pass "the committed production manifest records the writer's contracts and is self-consistent"
    else
      fail "committed production manifest: $out"
    fi

    out=$(python3 - "$PROD_MANIFEST" "$SMALL_GOLDEN_MANIFEST" <<'PY'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
# `data_db_sha256_also_match_at` is emitted only when such a path exists (the small
# golden has one, the multi-GB production corpus does not), so it is the ONE key allowed
# to differ. Everything else must match, in both directions and at every level.
OPTIONAL = {"data_db_sha256_also_match_at"}
bad = []
if set(a) - OPTIONAL != set(b) - OPTIONAL:
    bad.append(f"top-level: only in production {sorted(set(a)-set(b)-OPTIONAL)}, "
               f"only in small_golden {sorted(set(b)-set(a)-OPTIONAL)}")
if set(a["tables"][0]) != set(b["tables"][0]):
    bad.append(f"tables[0]: {sorted(set(a['tables'][0]) ^ set(b['tables'][0]))}")
if set(a["tables"][0]["sstables"][0]) != set(b["tables"][0]["sstables"][0]):
    bad.append("sstables[0]: "
               f"{sorted(set(a['tables'][0]['sstables'][0]) ^ set(b['tables'][0]['sstables'][0]))}")
if set(a["provenance"]) != set(b["provenance"]):
    bad.append(f"provenance: {sorted(set(a['provenance']) ^ set(b['provenance']))}")
print("MANIFEST-KEYSETS-AGREE" if not bad else "BAD: " + "; ".join(bad))
PY
    )
    if [ "$out" = "MANIFEST-KEYSETS-AGREE" ]; then
      pass "both committed manifests carry the same key set (neither can drift alone)"
    else
      fail "committed manifest key sets: $out"
    fi
  fi

  # ---- the writer holds no throughput constant AT ALL (roborev #3234 M1) --------------
  # The figure was a module constant, and every attempt to keep it honest added another
  # guard. A grep is the durable form of "it is not there": re-adding the number reds the
  # suite, whatever guard comes with it.
  if grep -qE '103804|127\.163|rows_per_second|wall_seconds' "$MANIFEST_PY"; then
    fail "the manifest writer carries a throughput figure again ($(grep -nE '103804|127\.163|rows_per_second' "$MANIFEST_PY" | head -2 | tr '\n' ' '))"
  else
    pass "the manifest writer holds NO throughput constant and no inheritable AC3 field"
  fi

  # ---- data_db_sha256_also_match_at claims EXACTLY what it checks (roborev #3234 M2) ---
  # The claim used to be `corpus_committed: true` + a `committed_copy` block reporting a
  # file count, a byte total and "describes the committed bytes" -- all from a Data.db-ONLY
  # hash comparison. The claim is now the size of the check, so these cases pin the check:
  # it must match on the real committed Data.db bytes, and it must return NOTHING (no
  # `false`, no partial block) when the recorded hash differs or the path is absent.
  out=$(python3 - "$MANIFEST_PY" "$REPO_ROOT" <<'PY'
import glob, hashlib, importlib.util, os, sys
spec = importlib.util.spec_from_file_location("w", sys.argv[1])
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
root = sys.argv[2]
cand = sorted(glob.glob(os.path.join(
    root, "test-data", "datasets", "sstables", "test_da",
    "wide_multiclustering_small-*")))
bad = []
if not cand:
    bad.append("no committed small-golden corpus to check against")
else:
    rel = os.path.relpath(cand[0], os.path.join(root, "test-data", "datasets"))
    # A corpus root somewhere else entirely, whose SSTable dir sits at the SAME
    # corpus-relative path as the committed copy -- which is how the real writer finds it.
    corpus_root, sstable_dir = "/nonexistent/corpus", os.path.join("/nonexistent/corpus", rel)
    data_db = os.path.join(cand[0], "da-1-bti-Data.db")
    real = hashlib.sha256(open(data_db, "rb").read()).hexdigest()
    got = w.data_db_sha256_match_path(
        sstable_dir, corpus_root, [{"sstable_basename": "da-1-bti", "data_db_sha256": real}])
    if got != f"test-data/datasets/{rel}":
        bad.append(f"positive control: got {got!r}")
    got = w.data_db_sha256_match_path(
        sstable_dir, corpus_root, [{"sstable_basename": "da-1-bti", "data_db_sha256": "0" * 64}])
    if got is not None:
        bad.append(f"a DIFFERING Data.db must yield nothing, got {got!r}")
    got = w.data_db_sha256_match_path(
        sstable_dir, corpus_root,
        [{"sstable_basename": "da-1-bti", "data_db_sha256": real},
         {"sstable_basename": "da-99-bti", "data_db_sha256": real}])
    if got is not None:
        bad.append(f"an SSTable with NO committed Data.db must yield nothing, got {got!r}")
    got = w.data_db_sha256_match_path(
        "/nonexistent/corpus/sstables/ks/no_such_table-0", "/nonexistent/corpus",
        [{"sstable_basename": "da-1-bti", "data_db_sha256": real}])
    if got is not None:
        bad.append(f"no candidate directory must yield nothing, got {got!r}")
print("MATCH-PATH-OK" if not bad else "BAD: " + "; ".join(bad))
PY
  )
  if [ "$out" = "MATCH-PATH-OK" ]; then
    pass "data_db_sha256_also_match_at is present only on a full Data.db sha256 match"
  else
    fail "data_db_sha256_also_match_at: $out"
  fi

  # "The failed run published no provenance", asserted the way it has to be asserted
  # after roborev #3234 M2. The corpus-local manifest path is the FIRST candidate
  # bti_perf_scan reads, so what matters is not that the path is EMPTY but that nothing
  # AUTHORITATIVE is at it: either no file, or the IN-PROGRESS marker the generator
  # installs before it mutates the published corpus. A file carrying `keyspace` there is
  # a readable manifest and therefore a failure -- which is exactly the old defect (the
  # PREVIOUS run's manifest surviving beside the new corpus).
  no_authoritative_manifest() { # no_authoritative_manifest <corpus-root>
    local m="$1/manifest-bti-3234.json"
    [ -e "$m" ] || return 0
    grep -q '"generation_in_progress"' "$m" && ! grep -q '"keyspace"' "$m"
  }

  # ------------------------------- end-to-end through the stub `docker` ---------
  # The generator's two row-count cross-checks and the manifest writer's HAPPY PATH
  # only execute when a container answers. They are exercised here against
  # scripts/tests/fixtures/stub-docker-cassandra-bti.py: the real row driver, real
  # CSVs, real file-level asserts, a real manifest -- and no container.
  STUB="$REPO_ROOT/scripts/tests/fixtures/stub-docker-cassandra-bti.py"
  COMMITTED_MANIFEST="$REPO_ROOT/test-data/perf-corpus-bti-manifest.json"
  committed_before="$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)"
  mkdir -p "$TMP/bin"
  SUDO_STUB="$TMP/bin/sudo-stub"
  # Stands in for `sudo -n`: runs the command, but never needs root for the two
  # ownership fixups only a real bind mount requires.
  cat >"$SUDO_STUB" <<'SUDOEOF'
#!/usr/bin/env bash
[ "${1:-}" = "-n" ] && shift
case "${1:-}" in chown|chmod) exit 0 ;; esac
exec "$@"
SUDOEOF
  chmod +x "$SUDO_STUB"

  STUB_UUID="a1b2c3d40000000000000000000000ff"
  # Belt and braces: every case below ALSO passes --manifest-out "" so a regression
  # that re-defaults MANIFEST_OUT cannot reach the committed manifest through these
  # runs. The ONE case that must exercise the DEFAULT resolution clears this array
  # (see the smoke-default case).
  E2E_MANIFEST_ARGS=(--manifest-out "")
  # E2E_ROOT_NAME lets a case run into a root ANOTHER case already populated (the #3234
  # M2 stale-provenance control needs a second run over the same published corpus) while
  # keeping its own log.
  e2e_run() { # e2e_run <name> [extra generator args...]; env prefixes are honored
    local name="$1"; shift
    E2E_ROOT="$TMP/e2e-${E2E_ROOT_NAME:-$name}"
    E2E_LOG="$TMP/e2e-$name.log"
    cp "$YAML_FIXTURE" "$TMP/yaml-$name.yaml"
    DOCKER="python3 $STUB" SUDO="$SUDO_STUB" \
    STUB_STATE="$TMP/stub-state-$name" STUB_KS=perf_bti_stub \
    STUB_TBL=wide_multiclustering STUB_YAML="$TMP/yaml-$name.yaml" \
    STUB_PLAN="$E2E_ROOT/work/row-plan.jsonl" \
      bash "$GEN" --out "$E2E_ROOT" --keyspace perf_bti_stub \
        --table wide_multiclustering --rows 1200 --chunk-rows 600 \
        --payload-bytes 32 --widths 200:60,800:30 \
        --buckets alpha,bo,charlie,delta --seed 3234 \
        ${E2E_MANIFEST_ARGS[@]+"${E2E_MANIFEST_ARGS[@]}"} "$@" >"$E2E_LOG" 2>&1
  }

  # The generator's own preflight demands >= 4 GiB free under --out (it sizes for a
  # real multi-GB load), so the stub run needs that much on TMPDIR's filesystem.
  # Reported LOUDLY when it is absent -- never silently dropped.
  tmp_avail_gib="$(df -BG --output=avail "$TMP" 2>/dev/null | tail -1 | tr -dc '0-9')"
  if [ ! -f "$STUB" ]; then
    fail "missing the stub docker: $STUB"
  elif [ "${tmp_avail_gib:-0}" -lt 5 ]; then
    skip "$SKIP_E2E_CASES" "only ${tmp_avail_gib:-?} GiB free under $TMP; the generator's" \
      "preflight needs >= 4 GiB, so the stub end-to-end cases were not run"
  else
    # ---- positive control: the whole pipeline, and the manifest it writes -------
    e2e_run ok; rc=$?
    manifest="$TMP/e2e-ok/manifest-bti-3234.json"
    if [ "$rc" -eq 0 ] && [ -f "$manifest" ]; then
      pass "end-to-end run against the stub docker succeeds and writes a manifest"
    else
      fail "stub end-to-end run failed (rc=$rc, tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    # Both cross-checks must have RUN, not merely not-failed.
    if grep -q "COPY imported" "$TMP/e2e-ok.log" 2>/dev/null \
       || grep -q "imported 600 rows" "$TMP/e2e-ok.log" 2>/dev/null; then
      pass "the per-chunk COPY row-count check ran on every chunk"
    else
      fail "no COPY row-count check in the log (tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    if grep -q "Statistics.db totalRows == sstabledump rows ==" "$TMP/e2e-ok.log" 2>/dev/null; then
      pass "the Statistics.db-vs-sstabledump row-count check ran"
    else
      fail "no Statistics.db/sstabledump cross-check in the log (tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    if [ -f "$manifest" ]; then
      out=$(python3 - "$manifest" "sstables/perf_bti_stub/wide_multiclustering-$STUB_UUID" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
t = m["tables"][0]
plan = m["rows_per_partition"]
bad = []
def eq(label, got, want):
    if got != want:
        bad.append(f"{label}: got {got!r}, want {want!r}")
eq("sstable_count", t["sstable_count"], 2)
eq("rows", t["rows"], plan["rows"])
eq("partitions", t["partitions"], plan["partitions"])
eq("sstable_dir (corpus-root relative)", t["sstable_dir"], sys.argv[2])
eq("meets_8mib_read_plane_floor", t["meets_8mib_read_plane_floor"], True)
eq("every_rows_db_non_empty", t["every_rows_db_non_empty"], True)
eq("ddl.extracted_statements", t["ddl"]["extracted_statements"], True)
# The key shape is read from the captured DDL, not from a `clustering_arity` literal.
eq("clustering_arity literal deleted", "clustering_arity" in t, False)
eq("clustering_key literal deleted", "clustering_key" in t, False)
eq("PRIMARY KEY in the captured DDL", "PRIMARY KEY (pk, bucket, seq)" in t["ddl"]["table_ddl"], True)
# The cross-check is the two NUMBERS agreeing, not an `agree: true` literal beside them.
eq("agree literal deleted", "agree" in m["row_count_cross_check"], False)
eq("cross-check rows agree", m["row_count_cross_check"]["row_driver_rows"],
   m["row_count_cross_check"]["statistics_db_rows"])
eq("cross-check partitions agree", m["row_count_cross_check"]["row_driver_partitions"],
   m["row_count_cross_check"]["statistics_db_partitions"])
eq("cross-check rows", m["row_count_cross_check"]["statistics_db_rows"], plan["rows"])
eq("cross-check partitions",
   m["row_count_cross_check"]["statistics_db_partitions"], plan["partitions"])
eq("plan chunks", len(plan["chunks"]), 2)
# One SSTable per planned chunk, and the generations are the flush order (#3234 M2).
eq("sstable_generations", t["sstable_generations"], [1, 2])
eq("one_sstable_per_planned_chunk", t["one_sstable_per_planned_chunk"], True)
eq("min_data_db_floor_bytes (production default)", t["min_data_db_floor_bytes"], 8388608)
# roborev #3234 M1/M2: a field is OBSERVED or ABSENT. Not one of these may reappear in
# ANY mode -- a fixed throughput figure (nothing here observed it), the flag that used to
# label it inapplicable while still printing it, the fixed full-generation-golden block,
# or the corpus_committed/committed_copy/corpus_note narrative inferred from a
# Data.db-only hash comparison. This corpus is an uncommitted stub, so the one surviving
# location field must be absent too.
blob = json.dumps(m)
for k in ("read_path_measurement_scope", "recorded_figure", "applies_to_this_corpus",
          "full_generation_golden", "corpus_committed", "committed_copy", "corpus_note",
          "corpus_identity"):
    eq(f"{k} absent", k in blob, False)
# ...and this stub corpus has no committed copy, so the one surviving location field is
# absent. Checked at the TOP LEVEL, because `provenance` legitimately DESCRIBES the field.
eq("data_db_sha256_also_match_at absent", "data_db_sha256_also_match_at" in m, False)
for claim in ("103804", "127.163", "13200000"):
    eq(f"inheritable figure {claim} absent", claim in blob, False)
# The two surviving location fields: this run's own --out, and the env line derived from
# it. (Compared to each other, not to a literal: the generator canonicalizes --out.)
eq("corpus_root is this run's --out", m["corpus_root"].endswith("e2e-ok"), True)
eq("datasets_root_usage", m["datasets_root_usage"],
   f"CQLITE_DATASETS_ROOT={m['corpus_root']}")
eq("seed_material of chunk 0", plan["chunks"][0]["seed_material"], "3234:0")
eq("goldens", sum(1 for s in t["sstables"] if s["sstabledump_golden"]), 1)
for s in t["sstables"]:
    eq(f"{s['sstable_basename']} format", s["format"], "da")
    eq(f"{s['sstable_basename']} compressor", s["compression"]["compressor"], "LZ4Compressor")
    eq(f"{s['sstable_basename']} chunk_length_bytes", s["compression"]["chunk_length_bytes"], 16384)
    eq(f"{s['sstable_basename']} sha256 length", len(s["data_db_sha256"]), 64)
    eq(f"{s['sstable_basename']} rows>0", s["rows"] > 0, True)
    eq(f"{s['sstable_basename']} partitions observed",
       isinstance(s["statistics"]["partition_count"], int), True)
    eq(f"{s['sstable_basename']} TOC has no BIG components",
       [c for c in s["toc"] if c in ("Index.db", "Summary.db")], [])
print("MANIFEST-FIELDS-OK" if not bad else "BAD: " + "; ".join(bad))
PY
      )
      if [ "$out" = "MANIFEST-FIELDS-OK" ]; then
        pass "the manifest's happy-path fields are all read back from the bytes"
      else
        fail "manifest fields: $out"
      fi

      # ---- the COMMITTED manifest cannot fall behind the WRITER (roborev #3234 L4) ----
      # This is the direct, mechanical form of the L4 finding: `$manifest` was just
      # written by the CURRENT writer, so comparing its key set against the committed
      # production artifact's makes "the committed manifest no longer records what the
      # writer publishes" a TEST FAILURE instead of a review finding. It catches the
      # class in both directions -- a writer that adds a contract, and a committed
      # manifest that keeps a field the writer dropped.
      out=$(python3 - "$manifest" "$REPO_ROOT/test-data/perf-corpus-bti-manifest.json" <<'PY'
import json, sys
fresh, committed = (json.load(open(p)) for p in sys.argv[1:3])
OPTIONAL = {"data_db_sha256_also_match_at"}
bad = []
def cmp(label, a, b):
    only_fresh, only_committed = set(a) - set(b) - OPTIONAL, set(b) - set(a) - OPTIONAL
    if only_fresh or only_committed:
        bad.append(f"{label}: writer emits {sorted(only_fresh)} the committed manifest "
                   f"lacks; committed manifest has {sorted(only_committed)} the writer "
                   "no longer emits")
cmp("top-level", fresh, committed)
cmp("tables[0]", fresh["tables"][0], committed["tables"][0])
cmp("tables[0].sstables[0]", fresh["tables"][0]["sstables"][0],
    committed["tables"][0]["sstables"][0])
cmp("provenance", fresh["provenance"], committed["provenance"])
cmp("reproducibility", fresh["reproducibility"], committed["reproducibility"])
print("COMMITTED-MANIFEST-CURRENT" if not bad else "BAD: " + "; ".join(bad))
PY
      )
      if [ "$out" = "COMMITTED-MANIFEST-CURRENT" ]; then
        pass "the committed production manifest's key set matches a FRESHLY written one"
      else
        fail "committed production manifest is STALE vs the writer: $out"
      fi
    fi

    # ---- direction 2: each cross-check must FAIL when the two sides disagree ----
    STUB_IMPORT_SHORT=1 e2e_run import-short; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "partial load" "$TMP/e2e-import-short.log" \
       && no_authoritative_manifest "$TMP/e2e-import-short"; then
      pass "COPY importing one row fewer than the CSV is a HARD failure, no manifest"
    else
      fail "import-short case: expected a partial-load failure (rc=$rc, tail: $(tail -6 "$TMP/e2e-import-short.log"))"
    fi

    STUB_META_SHORT=1 e2e_run meta-short; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "row-count mismatch for" "$TMP/e2e-meta-short.log" \
       && no_authoritative_manifest "$TMP/e2e-meta-short"; then
      pass "Statistics.db totalRows != sstabledump rows is a HARD failure, no manifest"
    else
      fail "meta-short case: expected a row-count mismatch (rc=$rc, tail: $(tail -6 "$TMP/e2e-meta-short.log"))"
    fi

    # The manifest writer's own cross-checks (goldens off, so the generator's
    # sstabledump check cannot pre-empt them).
    STUB_ROWS_DELTA=1 e2e_run rows-delta --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "row-count cross-check FAILED" "$TMP/e2e-rows-delta.log" \
       && no_authoritative_manifest "$TMP/e2e-rows-delta"; then
      pass "manifest writer HARD-FAILS when Statistics.db rows != the row plan"
    else
      fail "rows-delta case: expected 'row-count cross-check FAILED' (rc=$rc, tail: $(tail -6 "$TMP/e2e-rows-delta.log"))"
    fi

    STUB_PARTITIONS_DELTA=1 e2e_run parts-delta --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "partition-count cross-check FAILED" "$TMP/e2e-parts-delta.log" \
       && no_authoritative_manifest "$TMP/e2e-parts-delta"; then
      pass "manifest writer HARD-FAILS when Statistics.db partitions != the row plan"
    else
      fail "parts-delta case: expected 'partition-count cross-check FAILED' (rc=$rc, tail: $(tail -6 "$TMP/e2e-parts-delta.log"))"
    fi

    # A partition count that could not be OBSERVED must be an error, never a 0
    # (CLAUDE.md: "a counter not observed is an error, never a fabricated 0").
    STUB_NO_HISTOGRAM=1 e2e_run no-hist --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "refusing to publish an unobserved partition count" "$TMP/e2e-no-hist.log" \
       && no_authoritative_manifest "$TMP/e2e-no-hist"; then
      pass "an unreadable Partition Size histogram is an ERROR, not a fabricated 0"
    else
      fail "no-histogram case: expected an unobserved-partition-count refusal (rc=$rc, tail: $(tail -6 "$TMP/e2e-no-hist.log"))"
    fi

    # ---- roborev #3234 M1: PLAUSIBLE OUTPUT + a NONZERO EXIT ---------------------
    # The manifest writer parsed `sstablemetadata` stdout without ever looking at the
    # exit status, so a command that prints a complete, valid-looking metadata block
    # (real totalRows, real "Partition Size:" histogram) and THEN fails -- a partial
    # read, a JVM error, an OOM kill inside the memory-capped container -- produced an
    # AUTHORITATIVE manifest out of counts nothing stands behind. `STUB_META_EXIT=42`
    # is exactly that shape: nothing in the output distinguishes it from success.
    # Goldens are off so the WRITER's readback is what hits it first.
    STUB_META_EXIT=42 e2e_run meta-exit --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "sstablemetadata FAILED for" "$TMP/e2e-meta-exit.log" \
       && grep -q "exit 42" "$TMP/e2e-meta-exit.log" \
       && grep -q "refusing to read row/partition provenance" "$TMP/e2e-meta-exit.log" \
       && no_authoritative_manifest "$TMP/e2e-meta-exit"; then
      pass "manifest writer HARD-FAILS on a nonzero sstablemetadata exit WITH valid output"
    else
      fail "meta-exit case: expected an exit-status refusal (rc=$rc, tail: $(tail -8 "$TMP/e2e-meta-exit.log"))"
    fi

    # Same fault, the generator's own readback (goldens ON, so verify_dumped_row_counts
    # reaches it first): it too must refuse to cross-check against the output of a
    # command that did not succeed.
    # The expected substring is the GENERATOR's own wording, and the WRITER's wording
    # must be ABSENT: both messages start "sstablemetadata FAILED for", so a laxer grep
    # would be satisfied by the writer catching it later -- which is exactly what
    # happened when this case was mutation-tested with the generator's check removed.
    STUB_META_EXIT=42 e2e_run meta-exit-golden; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "sstablemetadata FAILED for" "$TMP/e2e-meta-exit-golden.log" \
       && grep -q "cross-check row counts against the output of a command" \
            "$TMP/e2e-meta-exit-golden.log" \
       && ! grep -q "refusing to read row/partition provenance" \
            "$TMP/e2e-meta-exit-golden.log" \
       && no_authoritative_manifest "$TMP/e2e-meta-exit-golden"; then
      pass "the generator's row-count cross-check refuses a nonzero sstablemetadata exit"
    else
      fail "meta-exit-golden case: expected a generator-side refusal (rc=$rc, tail: $(tail -8 "$TMP/e2e-meta-exit-golden.log"))"
    fi

    # ---- roborev #3234 M2: a FAILED regeneration must not leave the PREVIOUS -------
    #      manifest beside the NEW corpus.
    # The injected failure is deliberately one that fires AFTER the corpus is published
    # (STUB_META_SHORT hits verify_dumped_row_counts, several steps past the prune +
    # rm -rf + copy) and BEFORE the manifest write -- the exact window that used to
    # leave a syntactically perfect manifest describing bytes that no longer exist.
    # Direction 1: the corpus is regenerated successfully into a root, so a real,
    # readable manifest is sitting there.
    E2E_ROOT_NAME=staleman e2e_run staleman-ok; rc=$?
    stale_root="$TMP/e2e-staleman"
    stale_manifest="$stale_root/manifest-bti-3234.json"
    if [ "$rc" -eq 0 ] && grep -q '"keyspace"' "$stale_manifest" 2>/dev/null; then
      pass "M2 setup: a successful run leaves a readable manifest in the corpus"
    else
      fail "M2 setup: expected a readable manifest at $stale_manifest (rc=$rc, tail: $(tail -6 "$TMP/e2e-staleman-ok.log"))"
    fi
    stale_before="$(sha256sum "$stale_manifest" 2>/dev/null | cut -d' ' -f1)"
    # Direction 2: regenerate the SAME root and fail after publish.
    STUB_META_SHORT=1 E2E_ROOT_NAME=staleman e2e_run staleman-fail; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "row-count mismatch for" "$TMP/e2e-staleman-fail.log"; then
      pass "M2: the second run fails AFTER the corpus was published (post-publish window)"
    else
      fail "M2: expected a post-publish failure (rc=$rc, tail: $(tail -6 "$TMP/e2e-staleman-fail.log"))"
    fi
    if no_authoritative_manifest "$stale_root"; then
      pass "M2: the failed run leaves NO readable manifest beside the new corpus"
    else
      fail "M2: a readable manifest survived a failed regeneration at $stale_manifest -- \
stale provenance in the authoritative position (sha $stale_before)"
    fi
    stale_aside="$(ls "$stale_root"/manifest-bti-3234.json.superseded-* 2>/dev/null | head -1)"
    if [ -n "$stale_aside" ] \
      && [ "$(sha256sum "$stale_aside" | cut -d' ' -f1)" = "$stale_before" ]; then
      pass "M2: the previous manifest survives ONLY under a superseded-* name (forensics)"
    else
      fail "M2: expected the previous manifest moved aside as manifest-bti-3234.json.superseded-* \
(got: ${stale_aside:-none})"
    fi
    if grep -q "authoritative manifest position now holds an IN-PROGRESS marker" \
      "$TMP/e2e-staleman-fail.log" \
      && grep -q "previous corpus manifest moved aside" "$TMP/e2e-staleman-fail.log"; then
      pass "M2: the generator REPORTS the quarantine as it happens"
    else
      fail "M2: expected the quarantine to be logged (tail: $(tail -6 "$TMP/e2e-staleman-fail.log"))"
    fi
    # ...and the marker carries nothing a consumer could mistake for provenance.
    if [ -f "$stale_manifest" ] \
      && ! grep -qE '"(keyspace|table|rows_per_partition)"' "$stale_manifest" \
      && grep -q '"generation_in_progress": true' "$stale_manifest"; then
      pass "M2: the marker carries no keyspace/table/row count, only generation_in_progress"
    else
      fail "M2: the in-progress marker must carry no provenance fields (got: $(head -c 200 "$stale_manifest" 2>/dev/null))"
    fi

    # ---- roborev #3234 F2: a --smoke run with the DEFAULT manifest resolution -----
    # This is the invocation the generator's own header advertises. It used to
    # overwrite the COMMITTED manifest with perf_bti_smoke metadata, after which the
    # default full-corpus scan rejects that manifest as describing another table
    # (bti_perf_scan exit 8). NOTE the empty E2E_MANIFEST_ARGS: this case must run the
    # REAL default resolution, so it is the one run that does NOT pass --manifest-out.
    E2E_MANIFEST_ARGS=()
    e2e_run smoke-default --smoke; rc=$?
    E2E_MANIFEST_ARGS=(--manifest-out "")
    smoke_manifest="$TMP/e2e-smoke-default/manifest-bti-3234.json"
    if [ "$rc" -eq 0 ] && [ -f "$smoke_manifest" ] \
       && grep -q '"mode": "smoke"' "$smoke_manifest"; then
      pass "a --smoke run writes its manifest INSIDE the corpus, marked mode=smoke"
    else
      fail "smoke-default case: expected an in-corpus smoke manifest (rc=$rc, tail: $(tail -8 "$TMP/e2e-smoke-default.log"))"
    fi
    if [ "$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)" = "$committed_before" ]; then
      pass "a DEFAULT --smoke run leaves the committed manifest byte-identical (sha256)"
    else
      fail "a --smoke run with the default manifest resolution OVERWROTE $COMMITTED_MANIFEST"
    fi

    # ---- roborev #3234 L3: PRODUCTION-ONLY metadata stays in the production manifest --
    # Production-only blocks used to be emitted for EVERY mode, which is how the
    # committed small-golden manifest came to carry the AC3 throughput figure and a
    # description of a 500,000-row "full generation golden" belonging to it. A
    # non-production manifest must carry NEITHER: an omitted field cannot be false.
    if [ -f "$smoke_manifest" ]; then
      out=$(python3 - "$smoke_manifest" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
blob = json.dumps(m)
bad = []
for k in ("read_path_measurement_scope", "recorded_figure", "full_generation_golden",
          "what_the_ac3_figure_measures", "applies_to_this_corpus", "corpus_committed",
          "committed_copy", "corpus_note"):
    if k in blob:
        bad.append(f"deleted key {k} present in a smoke manifest")
for claim in ("13200000", "103804", "127.163", "500,000 rows", "multi-GB"):
    if claim in blob:
        bad.append(f"production-only claim {claim!r} present in a smoke manifest")
if "data_db_sha256_also_match_at" in m:
    bad.append("an uncommitted smoke corpus must claim no checkout sha256 match")
if "Profileable" in m["purpose"]:
    bad.append("a smoke run is not the profileable production corpus")
print("SMOKE-SCOPE-OK" if not bad else "BAD: " + "; ".join(bad))
PY
      )
      if [ "$out" = "SMOKE-SCOPE-OK" ]; then
        pass "a non-production manifest carries NO AC3 figure and no production claims"
      else
        fail "smoke manifest scope: $out"
      fi
    fi

    # No run above may touch the COMMITTED manifest.
    if [ "$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)" = "$committed_before" ]; then
      pass "no stub run modified the committed perf-corpus-bti-manifest.json"
    else
      fail "a stub run OVERWROTE the committed manifest $COMMITTED_MANIFEST"
    fi
  fi
else
  skip "$SKIP_PY_CASES" "python3 unavailable: row-driver + manifest-writer cases not run"
fi

echo
# Any name bash could not resolve during the run is a counted failure -- the runtime
# half of the roborev #3234 M1 guard (the handler's own `exit` cannot red the run: it
# executes in a separate execution environment).
if [ -s "$CNF_SENTINEL" ]; then
  fail "unresolved command name(s) during the run -- a typo, or a helper called" \
    "before it was defined; the case(s) that used them did NOT run: $(tr '\n' ' ' <"$CNF_SENTINEL")"
fi
# Case-count floor: a suite that silently stopped running cases must not be able to
# report success on `fails=0` alone. Every legitimate skip declared its case count
# above, so passes + skipped must still reach the declared total.
if [ "$((passes + skipped_cases))" -lt "$MIN_CASES" ]; then
  fail "case-count floor: $passes case(s) ran + $skipped_cases declared skipped =" \
    "$((passes + skipped_cases)), under the $MIN_CASES this suite declares -- cases stopped" \
    "running (or a skip's declared count is stale)."
fi
echo "test_gen_perf_corpus_bti: passes=$passes fails=$fails skips=$skips" \
  "skipped-cases=$skipped_cases (declared floor $MIN_CASES)"
if [ "$fails" -eq 0 ]; then
  echo "test_gen_perf_corpus_bti: ALL PASS ($passes cases, $skipped_cases skipped)"
  exit 0
fi
echo "test_gen_perf_corpus_bti: $fails FAILURE(S)"
exit 1
