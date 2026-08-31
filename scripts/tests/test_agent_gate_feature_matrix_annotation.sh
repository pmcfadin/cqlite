#!/usr/bin/env bash
# test_agent_gate_feature_matrix_annotation.sh — guard for the SUMMARY feature-matrix
# annotation (issue #3453, owner ruling 2026-08-30: "the gate SUMMARY should name the
# feature matrix each component ran so a pasted block states what it certified").
#
# THREE PROPERTIES, in increasing strength:
#
#   (A) COMPLETENESS — every name in agent-gate.sh's COMPONENTS array resolves to a
#       class in _fm_component_class, and no component can render a BLANK annotation.
#       A new component joining the set undeclared FAILS here, which is the whole
#       point: a blank annotation is the vacuous-pass shape (a line that states
#       nothing about what it certified reads exactly like one that certified
#       everything).
#
#   (B) UNIFORMITY — all six per-component SUMMARY emit sites (full, lite, two delta
#       sites, the lite-aggregation self-test, --emit-summary-selftest) render through
#       the ONE _fm_summary_line renderer. Asserted structurally, because a mode that
#       renders an un-annotated block is the exact "pasted block overstates what it
#       certified" defect, and it would be invisible in any single mode's own tests.
#
#   (C) NO DRIFT, MEASURED — for the six components whose cargo calls live inside a
#       single-quoted `bash -c` body (and so cannot be observed by the in-shell `cargo`
#       wrapper), the gate is run under `--only` with a PATH-shim `cargo` that RECORDS
#       ITS ARGV and compiles nothing. The declared annotation is then compared against
#       the argv that ACTUALLY EXECUTED, described through the gate's OWN
#       _fm_describe_cargo — never a re-derivation in this test, per CLAUDE.md's #3283
#       rule that a port's correctness is only knowable by differential testing against
#       the original. This is what makes "derive, never curate" a measurement instead of
#       a claim: change a feature literal in one of those bodies without changing the
#       hoisted variable and this section reds.
#
# Hermetic: no network, no datasets, no real compilation (the shim replaces cargo), no
# node/python/jq. Needs only bash + the checkout.

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$HERE/../.." && pwd)
GATE="$REPO_ROOT/scripts/agent-gate.sh"

PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/fm-annot.XXXXXX") || exit 1
trap 'rm -rf "$tmp"' EXIT

[ -r "$GATE" ] || { echo "FAIL - cannot read $GATE"; exit 1; }

# The annotation functions are EXTRACTED OUT OF THE SHIPPED GATE SCRIPT, never copied
# here — the repo's existing idiom (test_agent_gate_jest_guards.sh,
# test_cargo_output_parsers.sh): a test that re-implements its subject can only prove that
# the copy works. FAILS CLOSED — an unextractable function is a FAIL, never a skip, or
# this guard could pass having tested nothing. (Extraction also defines the `cargo`/`env`
# wrappers in THIS shell; harmless, since AGENT_GATE_FM_COMPONENT is unset except where a
# case sets it.)
for fn in _fm_active _fm_sidecar _fm_note _fm_abbrev_features _fm_describe_cargo \
          _fm_observe_cargo_argv cargo env _fm_component_class _fm_render _fm_annotate \
          _fm_summary_line _fm_note_if_skipped; do
  src=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  if [ -z "$src" ]; then
    echo "FAIL - could not extract $fn from $GATE — renamed or reshaped; this guard must not pass having tested nothing (#3453)" >&2
    exit 1
  fi
  eval "$src" || { echo "FAIL - extracted $fn does not parse" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# (A) COMPLETENESS
# ---------------------------------------------------------------------------
# COMPONENTS is read out of the gate SOURCE, so this cannot drift from the real set.
components_line=$(grep -m1 '^COMPONENTS=(' "$GATE")
if [ -z "$components_line" ]; then
  bad "A0: could not find COMPONENTS=( in $GATE"
else
  comps=${components_line#COMPONENTS=(}
  comps=${comps%)}
  # shellcheck disable=SC2206  # deliberate word-split of the source array literal
  comps_arr=($comps)
  if [ "${#comps_arr[@]}" -lt 30 ]; then
    bad "A0: parsed only ${#comps_arr[@]} components out of COMPONENTS — parse looks wrong"
  else
    ok "A0: parsed ${#comps_arr[@]} components from the gate's COMPONENTS array"
  fi
  unclassified=()
  blank=()
  declare -A class_count=()
  export AGENT_GATE_FM_DIR="$tmp/empty"; mkdir -p "$AGENT_GATE_FM_DIR"
  for c in "${comps_arr[@]}" scoped-tests; do
    if cls=$(_fm_component_class "$c"); then
      case "$cls" in
        indirect:*) key=indirect ;;
        *) key="$cls" ;;
      esac
      class_count[$key]=$(( ${class_count[$key]:-0} + 1 ))
    else
      unclassified+=("$c")
    fi
    ann=$(_fm_annotate "$c")
    [ -n "$ann" ] || blank+=("$c")
  done
  if [ "${#unclassified[@]}" -eq 0 ]; then
    ok "A1: every COMPONENTS name (+scoped-tests) is declared in _fm_component_class"
  else
    bad "A1: UNDECLARED in _fm_component_class: ${unclassified[*]}"
  fi
  if [ "${#blank[@]}" -eq 0 ]; then
    ok "A2: no component renders a BLANK annotation (${class_count[cargo]:-0} cargo, ${class_count[no-cargo]:-0} no-cargo, ${class_count[indirect]:-0} indirect)"
  else
    bad "A2: BLANK annotation for: ${blank[*]}"
  fi
  # An unknown name must be NAMED as unclassified, not silently blank or plausible.
  ann=$(_fm_annotate a-component-that-does-not-exist)
  case "$ann" in
    *UNCLASSIFIED*) ok "A3: an unknown component renders UNCLASSIFIED, not a blank or a guess" ;;
    *) bad "A3: unknown component rendered '$ann'" ;;
  esac
fi

# ---------------------------------------------------------------------------
# (B) UNIFORMITY across the six emit sites
# ---------------------------------------------------------------------------
n_raw=$(grep -c "printf '%-18s %s (%s)'" "$GATE")
n_render=$(grep -c '_fm_summary_line "' "$GATE")
if [ "$n_raw" -eq 0 ]; then
  ok "B1: no per-component SUMMARY line is emitted by a raw printf (all route through _fm_summary_line)"
else
  bad "B1: $n_raw raw per-component printf site(s) remain — that mode's block would carry NO feature matrix"
fi
# 6 emit sites + the definition itself is not matched (it uses positional args).
if [ "$n_render" -ge 6 ]; then
  ok "B2: $n_render _fm_summary_line call sites (>= the 6 known emit sites: full, lite, 2x delta, lite-agg selftest, emit-summary-selftest)"
else
  bad "B2: only $n_render _fm_summary_line call site(s); expected at least 6 — a mode is un-annotated"
fi
# The observer functions must NOT be exported: exporting them makes every bash
# DESCENDANT record, so tooling-tests (which runs nested agent-gate self-tests) would
# attribute a nested run's cargo invocations to itself. See the library header.
# The needle is SPLIT so this guard cannot match its own source line (a self-matching
# grep is a guard that is always red, which is a guard nobody keeps).
fm_export_needle="export"' -f (cargo|env)\b'
if grep -RnE "$fm_export_needle" "$REPO_ROOT/scripts" >/dev/null 2>&1; then
  bad "B3: the cargo/env observers are EXPORTED — nested scripts would pollute a component's matrix"
  grep -RnE "$fm_export_needle" "$REPO_ROOT/scripts" | head -5
else
  ok "B3: the cargo/env observers are not exported (no nested-run attribution)"
fi

# ---------------------------------------------------------------------------
# Unit cases for the descriptor / render / annotate path
# ---------------------------------------------------------------------------
d() { _fm_describe_cargo "$@"; }

expect_desc() { # <label> <expected> <argv…>
  local label="$1" want="$2"; shift 2
  local got rc
  got=$(d "$@"); rc=$?
  if [ "$rc" -eq 0 ] && [ "$got" = "$want" ]; then
    ok "$label"
  else
    bad "$label: rc=$rc got '$got' want '$want'"
  fi
}
expect_reject() { # <label> <argv…>
  local label="$1"; shift
  if d "$@" >/dev/null 2>&1; then
    bad "$label: a metadata query was recorded as a compile/run invocation"
  else
    ok "$label"
  fi
}

expect_desc "U1: package + features"            'test cqlite-core --features cli-helpers' test --package cqlite-core --features cli-helpers --lib
expect_desc "U2: space-separated feature list"  'test cqlite-core --features state_machine,cli-helpers' test -p cqlite-core --features "state_machine cli-helpers"
expect_desc "U3: --all-features"                'check cqlite-core --all-features' check -p cqlite-core --all-features --all-targets
expect_desc "U4: --no-default-features is NAMED" 'build cqlite-core --no-default-features --features all-compression' build --package cqlite-core --no-default-features --features all-compression
expect_desc "U5: no feature flags = default-features (never blank)" 'test cqlite-integration-tests default-features' test --package cqlite-integration-tests --no-run
expect_desc "U6: multi-package scope"           'clippy cqlite-flight+cqlite-py+cqlite-node --features cqlite-node/write-support' clippy --all-targets -p cqlite-flight -p cqlite-py -p cqlite-node --features cqlite-node/write-support
expect_desc "U7: workspace + exclude count"     'clippy workspace(excl 5) --all-features' clippy --workspace --all-targets --all-features --exclude a --exclude b --exclude c --exclude d --exclude e
expect_desc "U8: repeated --features accumulate" 'test cqlite-core --features a,b' test -p cqlite-core --features a --features b
expect_desc "U9: --features=X form"             'test cqlite-core --features a,b' test -p cqlite-core --features=a,b
expect_reject "U10: cargo tree is not a compile" tree -p cqlite-core --all-features
expect_reject "U11: cargo metadata is not a compile" metadata --format-version 1
expect_reject "U12: cargo --version is not a compile" --version
expect_reject "U13: bare cargo is not a compile"

# Abbreviation: bounded width, count-led, elision NAMED (never silent truncation).
long=$(printf 'f%02d,' $(seq 1 33)); long=${long%,}
got=$(_fm_abbrev_features "$long")
if [ "$got" = "33:f01,f02,f03,+30 more" ]; then
  ok "U14: a 33-feature list abbreviates to '33:f01,f02,f03,+30 more' (count + NAMED elision)"
else
  bad "U14: abbreviation was '$got'"
fi
got=$(_fm_abbrev_features "a,b,c,d,e")
[ "$got" = "a,b,c,d,e" ] && ok "U15: <=5 features print in full" || bad "U15: got '$got'"

# ---------------------------------------------------------------------------
# render / annotate behaviour, driven through the real sidecar path
# ---------------------------------------------------------------------------
export AGENT_GATE_FM_DIR="$tmp/side"; mkdir -p "$AGENT_GATE_FM_DIR"
seed() { printf '%s\n' "$@" > "$AGENT_GATE_FM_DIR/$1.features"; }   # first arg is the name

printf '%s\n' 'test cqlite-core --features a' 'test cqlite-core --features a' 'test cqlite-flight default-features' \
  > "$AGENT_GATE_FM_DIR/core-tests.features"
got=$(_fm_annotate core-tests)
if [ "$got" = '[test cqlite-core --features a x2 | test cqlite-flight default-features]' ]; then
  ok "R1: identical sets collapse to 'xN' and distinct sets are BOTH named"
else
  bad "R1: got '$got'"
fi

for i in $(seq 1 9); do printf 'test pkg%s default-features\n' "$i"; done > "$AGENT_GATE_FM_DIR/write-tests.features"
got=$(_fm_annotate write-tests)
case "$got" in
  *'+3 more sets]') ok "R2: beyond 6 distinct sets the remainder is DECLARED ('+3 more sets'), not dropped" ;;
  *) bad "R2: got '$got'" ;;
esac

got=$(_fm_annotate minimal-build)   # cargo class, no sidecar
[ "$got" = '[UNDECLARED]' ] && ok "R3: a cargo component with no observation renders UNDECLARED" || bad "R3: got '$got'"

got=$(_fm_annotate file-size)       # declared no-cargo, no sidecar
[ "$got" = '[no-cargo]' ] && ok "R4: a declared no-cargo component renders no-cargo" || bad "R4: got '$got'"

got=$(_fm_annotate python-bindings) # indirect
case "$got" in
  *maturin*'NOT observed'*) ok "R5: an indirect (driver-built) component names the driver AND that the feature set is NOT observed" ;;
  *) bad "R5: got '$got'" ;;
esac

# Observation BEATS declaration: a mis-declared no-cargo component that really ran cargo
# must show the observed sets AND be flagged, not have its declaration believed.
printf 'test cqlite-core --features x\n' > "$AGENT_GATE_FM_DIR/file-size.features"
got=$(_fm_annotate file-size)
case "$got" in
  *'test cqlite-core --features x'*'!declared-no-cargo'*) ok "R6: observation beats declaration (mis-declared no-cargo is flagged, not believed)" ;;
  *) bad "R6: got '$got'" ;;
esac
rm -f "$AGENT_GATE_FM_DIR/file-size.features"

# SKIP that never reached cargo: recorded as such, so the line does not read UNDECLARED.
AGENT_GATE_FM_COMPONENT=oom-audit _fm_note_if_skipped oom-audit SKIP
got=$(_fm_annotate oom-audit)
case "$got" in
  *'SKIPped'*) ok "R7: a SKIP before any cargo call renders 'no cargo invoked (component SKIPped)'" ;;
  *) bad "R7: got '$got'" ;;
esac
# …and a declared no-cargo component is left exactly as it was.
AGENT_GATE_FM_COMPONENT=pub-surface _fm_note_if_skipped pub-surface SKIP
got=$(_fm_annotate pub-surface)
[ "$got" = '[no-cargo]' ] && ok "R8: the SKIP note does not disturb a declared no-cargo component" || bad "R8: got '$got'"

# The env wrapper must be a pass-through for everything that is not cargo.
got=$(env FM_TEST_VAR=hello sh -c 'printf %s "$FM_TEST_VAR"')
[ "$got" = hello ] && ok "R9: the env wrapper passes non-cargo commands through unchanged" || bad "R9: got '$got'"

# ---------------------------------------------------------------------------
# (B') The emitted block really carries an annotation on every line
# ---------------------------------------------------------------------------
selftest_sum="$tmp/selftest-summary.txt"
if AGENT_GATE_SUMMARY_FILE="$selftest_sum" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1; then
  missing=()
  while IFS= read -r line; do
    case "$line" in
      *'[UNDECLARED]'*|*UNCLASSIFIED*) missing+=("$line") ;;
    esac
  done < <(grep -E '^(fmt|clippy|core-tests|smoke): +(PASS|FAIL|SKIP)' "$selftest_sum")
  n_annot=$(grep -cE '^(fmt|clippy|core-tests|smoke): +(PASS|FAIL|SKIP) \([0-9]+s\)  \[.+\]' "$selftest_sum")
  if [ "$n_annot" -eq 4 ] && [ "${#missing[@]}" -eq 0 ]; then
    ok "B4: --emit-summary-selftest emits 4 annotated component lines, none UNDECLARED"
  else
    bad "B4: $n_annot/4 annotated; unannotated/undeclared: ${missing[*]:-(none)}"
    grep -E '^(fmt|clippy|core-tests|smoke):' "$selftest_sum" || true
  fi
  # The #2908 poll predicate and the one-RESULT invariant must survive the annotation.
  n_result=$(grep -c 'RESULT:' "$selftest_sum")
  [ "$n_result" = 1 ] && ok "B5: exactly ONE 'RESULT:' token — no annotation embeds the token" \
    || bad "B5: $n_result RESULT: tokens in the block"
else
  bad "B4: --emit-summary-selftest exited non-zero"
fi

# ---------------------------------------------------------------------------
# (C) NO DRIFT — declared vs EXECUTED, measured with a recording cargo shim
# ---------------------------------------------------------------------------
shim_dir="$tmp/shim"; mkdir -p "$shim_dir"
cat > "$shim_dir/cargo" <<'SHIM'
#!/usr/bin/env bash
# Recording cargo shim: appends its argv and compiles nothing.
printf '%s\n' "$*" >> "${FM_SHIM_LOG:?}"
exit 0
SHIM
chmod +x "$shim_dir/cargo"

# describe_shim_log <log> <out-sidecar>: turn the argv the shim ACTUALLY received into
# descriptors using the gate's OWN _fm_describe_cargo (no second implementation).
describe_shim_log() {
  local log="$1" out="$2" line desc
  : > "$out"
  while IFS= read -r line; do
    # shellcheck disable=SC2086  # deliberate: re-split the recorded argv
    set -- $line
    desc=$(_fm_describe_cargo "$@" 2>/dev/null) || continue
    printf '%s\n' "$desc" >> "$out"
  done < "$log"
}

# Six components whose cargo calls are inside a `bash -c` body, and how their declared
# set relates to what a shimmed run executes:
#   EXACT      — the whole body runs under the shim, so declared MUST equal executed.
#   CONTAINS   — the body aborts partway under a stub (cli-tests' zero-tests guard fires;
#                smoke needs a real binary), so the executed sets must be a SUBSET of the
#                declared ones. Named here rather than quietly asserted as EXACT: a test
#                that claims a strength it does not have is the defect this issue is about.
run_differential() { # <component> <mode EXACT|CONTAINS>
  local c="$1" mode="$2"
  local sum="$tmp/only-$c.txt" log="$tmp/only-$c.log" shimlog="$tmp/argv-$c.log"
  : > "$shimlog"
  FM_SHIM_LOG="$shimlog" \
  AGENT_GATE_SUMMARY_FILE="$sum" \
  AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  PATH="$shim_dir:$PATH" \
    bash "$GATE" --only "$c" > "$log" 2>&1
  local line ann
  line=$(grep -E "^$c: +(PASS|FAIL|SKIP)" "$sum" 2>/dev/null | head -1)
  if [ -z "$line" ]; then
    bad "C-$c: no '$c:' component line in the emitted block"
    return
  fi
  ann=${line#*\[}; ann="[${ann}"
  case "$ann" in
    '[UNDECLARED]'|*UNCLASSIFIED*|'[]') bad "C-$c: annotation is '$ann'"; return ;;
  esac
  local exec_side="$tmp/exec-$c.features"
  describe_shim_log "$shimlog" "$exec_side"
  if [ ! -s "$exec_side" ]; then
    bad "C-$c: the shim recorded no compile/run cargo invocation — the differential proved nothing"
    return
  fi
  # Render the EXECUTED argv through the same renderer the block uses.
  local saved="$AGENT_GATE_FM_DIR" expected
  export AGENT_GATE_FM_DIR="$tmp/execside"; mkdir -p "$AGENT_GATE_FM_DIR"
  cp "$exec_side" "$AGENT_GATE_FM_DIR/$c.features"
  expected="[$(_fm_render "$c")]"
  export AGENT_GATE_FM_DIR="$saved"
  if [ "$mode" = EXACT ]; then
    if [ "$ann" = "$expected" ]; then
      ok "C-$c: declared matrix == the argv that EXECUTED  $ann"
    else
      bad "C-$c: DRIFT — block says $ann but the executed argv describes $expected"
    fi
  else
    local missing=() dsc
    while IFS= read -r dsc; do
      case "$ann" in *"$dsc"*) ;; *) missing+=("$dsc") ;; esac
    done < <(sort -u "$exec_side")
    if [ "${#missing[@]}" -eq 0 ]; then
      ok "C-$c: every EXECUTED set is named in the declared matrix (CONTAINS; body aborts under a cargo stub)  $ann"
    else
      bad "C-$c: executed set(s) NOT named in the block: ${missing[*]}"
    fi
  fi
}

if [ "${FM_SKIP_DIFFERENTIAL:-0}" = 1 ]; then
  echo "note - (C) differential skipped by FM_SKIP_DIFFERENTIAL=1 (debug only; the gate never sets it)"
else
  run_differential minimal-build     EXACT
  run_differential write-tests       EXACT
  run_differential memory-budget     EXACT
  run_differential integration-tests EXACT
  run_differential cli-tests         CONTAINS
  run_differential smoke             CONTAINS
fi

echo
echo "feature-matrix annotation guard: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
