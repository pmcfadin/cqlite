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
#   (D) TRUTHFUL ON A SHORT-CIRCUIT, MEASURED (roborev job 269, blocker 2) — the same
#       differential re-run with a cargo shim that FAILS, so each `bash -c` body aborts
#       after its FIRST invocation. The declared annotation must then name exactly that
#       one invocation. This is the property a parent-side pre-record could not have: it
#       described INTENT, so a `cli-tests: FAIL` line named the write-support pass that
#       never started. Non-vacuity is asserted, not assumed: the failing run must record
#       strictly FEWER invocations than section (C)'s passing run, or the case proves
#       nothing about short-circuiting.
#
#   (P) THE INDIRECT TIER IS NAMED, NOT GUESSED (roborev job 269, blocker 1) — the scoped
#       --lite python tier builds the extension through maturin in a CHILD process, so no
#       cargo argv passes through the gate's shell. run_scoped_tests itself is driven here
#       (extracted from the shipped script, with its ROUTING stubbed and a fake
#       --python-build-verify child) for a PURE-PYTHON and a MIXED rust+python route, and
#       the rendered annotation is asserted: `[via maturin: feature set NOT observed]` and
#       `[<observed rust set> | via maturin: …]` respectively — additive, never replacing.
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
# A case whose subject is UNOBSERVABLE on this box (a corpus-dependent component that
# SKIPs without its committed fixtures) is reported as a SKIP, counted in NEITHER total,
# so it can never be mistaken for a passing assertion (#3249's rule).
skipped() { printf 'skip - %s\n' "$1"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/fm-annot.XXXXXX") || exit 1

# AN EARLY ABORT MUST NEVER EXIT 0 — measured, not predicted (roborev job 273, F1).
# `tooling-tests` judges this file by its EXIT STATUS, and under real /bin/bash 3.2
# (3.2.57, in a container) the pre-F1 version of this guard hit its `declare -A`, then died
# on the `set -u` unbound-variable that followed — after ONE of 84 cases — and exited 0.
# So the bash-4-only construct did not red the gate on macOS: it degraded this guard to a
# VACUOUS PASS, which is strictly worse than the loud failure F1 described. The construct is
# now linted (test_agent_gate_summary.sh section 8c), but the abort CLASS is general — a
# missing tool, a set -u slip, a syntax error in a future edit — so it is closed here at the
# exit boundary rather than one construct at a time.
FM_REACHED_END=0
_fm_on_exit() {
  local rc=$?
  rm -rf "$tmp"
  if [ "$FM_REACHED_END" -ne 1 ]; then
    printf 'FAIL - the guard ABORTED before its terminal tally (set -u, a syntax error, or a missing tool). Whatever the exit status was, this run certified nothing.\n' >&2
    exit 1
  fi
  exit "$rc"
}
trap _fm_on_exit EXIT

[ -r "$GATE" ] || { echo "FAIL - cannot read $GATE"; exit 1; }

# The annotation functions are EXTRACTED OUT OF THE SHIPPED GATE SCRIPT, never copied
# here — the repo's existing idiom (test_agent_gate_jest_guards.sh,
# test_cargo_output_parsers.sh): a test that re-implements its subject can only prove that
# the copy works. FAILS CLOSED — an unextractable function is a FAIL, never a skip, or
# this guard could pass having tested nothing. (Extraction also defines the `cargo`/`env`
# wrappers in THIS shell; harmless, since AGENT_GATE_FM_COMPONENT is unset except where a
# case sets it.)
for fn in _fm_active _fm_sidecar _fm_note _fm_indirect_desc _fm_unobservable_desc \
          _fm_abbrev_features \
          _fm_describe_cargo _fm_observe_cargo_argv _fm_observe_child _fm_observe_driver \
          cargo env \
          _fm_component_class _fm_render _fm_annotate _fm_summary_line \
          _fm_note_if_no_cargo_observed _fm_note_driver _fm_note_maturin_rc \
          run_scoped_tests run_python_bindings; do
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
  # SCALAR counters, never `declare -A`: associative arrays are bash 4.0+ and macOS ships
  # /bin/bash 3.2, which this repository treats as a supported gate host
  # (test_agent_gate_summary.sh section 8) — and `tooling-tests` ALWAYS runs this file, so a
  # bash-4-only construct here FAILs the gate of record on a supported platform (roborev job
  # 273, F1). Section 8c of test_agent_gate_summary.sh now lints every gate-invoked script
  # for the construct, so a reintroduction here is caught mechanically.
  n_cargo=0; n_nocargo=0; n_indirect=0; n_unobservable=0
  export AGENT_GATE_FM_DIR="$tmp/empty"; mkdir -p "$AGENT_GATE_FM_DIR"
  # THE NAME DOMAIN IS DERIVED FROM THE EMIT PATH, NOT FROM COMPONENTS (roborev job 277 F2).
  # Every name that can appear on a component line must be classifiable. Those names come from
  # COMPONENTS *plus* the dynamic `NAMES+=("<literal>")` appends in the run_delta_* helpers
  # (node-tests, shell-selftests, scoped-tests today). This loop previously hardcoded
  # `scoped-tests` alone, so the other two rendered [UNCLASSIFIED] in a legitimate --delta
  # block and no case here could see it: the enumeration was narrower than its domain, which is
  # the same defect shape as the round-2 findings one level up.
  #
  # Derived, so a FUTURE dynamic append joins this check with no edit here. Only literal
  # appends are derivable; the `NAMES+=("$var")` sites are the COMPONENTS-driven paths already
  # covered by comps_arr, so nothing is silently dropped.
  dyn_names=$(grep -oE 'NAMES\+=\("[a-z0-9][a-z0-9-]*"\)' "$GATE" \
    | sed -E 's/.*\("(.*)"\)/\1/' | sort -u)
  dyn_n=$(printf '%s\n' "$dyn_names" | grep -c . || true)
  # Fail-closed on a broken derivation: an empty set would silently shrink the domain back to
  # COMPONENTS and re-open exactly this finding. There are 3 such names today.
  if [ "${dyn_n:-0}" -lt 1 ]; then
    bad "A0b: derivation of the dynamic summary-name set from $GATE yielded ${dyn_n:-0} names — the domain would silently collapse to COMPONENTS (this is how job 277 F2 escaped)"
  else
    ok "A0b: dynamic summary-name set DERIVED from the emit path: ${dyn_n} name(s) [$(printf '%s' "$dyn_names" | tr '\n' ' ')]"
  fi
  for c in "${comps_arr[@]}" $dyn_names; do
    if cls=$(_fm_component_class "$c"); then
      case "$cls" in
        indirect:*)      n_indirect=$((n_indirect + 1)) ;;
        unobservable:*)  n_unobservable=$((n_unobservable + 1)) ;;
        no-cargo)        n_nocargo=$((n_nocargo + 1)) ;;
        cargo)           n_cargo=$((n_cargo + 1)) ;;
        *)               unclassified+=("$c") ;;
      esac
    else
      unclassified+=("$c")
    fi
    ann=$(_fm_annotate "$c")
    [ -n "$ann" ] || blank+=("$c")
  done
  if [ "${#unclassified[@]}" -eq 0 ]; then
    ok "A1: every COMPONENTS name + every DERIVED dynamic summary name resolves to one of the FOUR declared classes in _fm_component_class"
  else
    bad "A1: undeclared, or declared with a class this guard does not recognise (cargo / no-cargo / indirect:<driver> / unobservable:<why>): ${unclassified[*]}"
  fi
  if [ "${#blank[@]}" -eq 0 ]; then
    ok "A2: no component renders a BLANK annotation ($n_cargo cargo, $n_nocargo no-cargo, $n_indirect indirect, $n_unobservable unobservable)"
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
# THE NEEDLE IS THE `%-18s` NAME FIELD, NOT A WHOLE FORMAT STRING (#3625, roborev job 360
# finding 2). This used to grep for the exact literal `printf '%-18s %s (%s)'`, and the
# tree-integrity BOUNDARY printer spelled its format `printf '%-18s %s (%ss)\n'` — one
# character different — so an entire emit path rendered component rows with NO feature
# matrix and this guard reported zero raw sites. A near-miss in a format string must not be
# able to hide an emit path, so the needle is now the field that MAKES it a component row.
# Comment-blind (`^[^#]*`): a comment quoting the format — the boundary printer now carries
# one explaining this very defect — must not be counted, or an artifact DESCRIBING the rule
# would become a violation of it.
# The renderer's OWN definition is the single legitimate occurrence, so the expected count
# is exactly 1 and anything above it is a bypass.
n_pf=$(grep -cE "^[^#]*printf '%-18s" "$GATE")
n_render=$(grep -c '_fm_summary_line "' "$GATE")
if [ "$n_pf" -eq 1 ]; then
  ok "B1: the ONLY non-comment 'printf %-18s' in the gate is _fm_summary_line's own definition — no mode emits a component row that bypasses the renderer"
else
  bad "B1: $n_pf non-comment 'printf %-18s' site(s) (expected exactly 1, the renderer's definition) — a mode's block would carry NO feature matrix; run: grep -nE \"^[^#]*printf '%-18s\" $GATE"
fi
# The renderer's own definition uses positional args, so it is not matched by this needle.
# SEVEN emit sites now: full, lite, 2x delta, lite-agg selftest, emit-summary-selftest, and
# the tree-integrity boundary printer (whose truncated table has TWO call sites, one per
# loop) — hence a floor of 8 CALL SITES over 7 blocks.
if [ "$n_render" -ge 8 ]; then
  ok "B2: $n_render _fm_summary_line call sites (>= the 8 known: full, lite, 2x delta, lite-agg selftest, emit-summary-selftest, and the boundary printer's 2 loops)"
else
  bad "B2: only $n_render _fm_summary_line call site(s); expected at least 8 — a mode is un-annotated"
fi
# The observer functions must NOT be exported: exporting them makes every bash
# DESCENDANT record, so tooling-tests (which runs nested agent-gate self-tests) would
# attribute a nested run's cargo invocations to itself. See the library header.
# The needle is SPLIT so this guard cannot match its own source line (a self-matching
# grep is a guard that is always red, which is a guard nobody keeps).
# Portable boundary, not GNU `\b`. The needle stays SPLIT (so this guard cannot match its own
# source line), but note what that costs: an assembled needle is invisible to a grep-based
# portability lint, which is one of the two blind spots that got that lint's `\b` arm descoped.
fm_export_needle="export"' -f (cargo|env)([^[:alnum:]_]|$)'
if grep -RnE "$fm_export_needle" "$REPO_ROOT/scripts" >/dev/null 2>&1; then
  bad "B3: the cargo/env observers are EXPORTED — nested scripts would pollute a component's matrix"
  grep -RnE "$fm_export_needle" "$REPO_ROOT/scripts" | head -5
else
  ok "B3: the cargo/env observers are not exported (no nested-run attribution)"
fi

# B8: the CHILD-BODY recorder IS exported, and only it. The eight `bash -c` component
# bodies run in a child bash, which inherits exported FUNCTIONS only — so
# _fm_observe_child (an explicitly-called recorder, which intercepts nothing) and the
# formatter it calls must be exported, while the cargo/env INTERCEPTORS must not be (B3).
# Without the export the bodies would silently record nothing and every one of those
# components would read UNDECLARED.
b8=()
for fn in _fm_observe_child _fm_describe_cargo _fm_abbrev_features _fm_sidecar; do
  # PORTABLE word boundary, not GNU `\b`: POSIX ERE leaves `\b` UNDEFINED and BSD grep on
  # macOS — a first-class gate host — does not interpret it, so the match would silently fail
  # and this MANDATORY guard would report false failures there (roborev job 285).
  grep -qE "^export -f ([^[:space:]]+[[:space:]]+)*$fn([[:space:]]|$)" "$GATE" || b8+=("$fn-not-exported")
done
if [ "${#b8[@]}" -eq 0 ]; then
  ok "B8: the child-body recorder + its formatter are exported (the bash -c bodies can record), while the interceptors are not (B3)"
else
  bad "B8: ${b8[*]} — a bash -c body would record NOTHING and read UNDECLARED"
fi

# B9: NO PARENT-SIDE PRE-RECORD SURVIVES (roborev job 269, blocker 2). Every one of the
# eight `bash -c` components records from INSIDE its body, and the only remaining
# _fm_observe_cargo_argv CALL SITES outside the annotation block are the four lines of the
# --emit-summary-selftest reference block (each carrying its own
# `AGENT_GATE_FM_COMPONENT=<name>` prefix, which a real component branch never has). A new
# parent-side pre-record — the exact defect this blocker names — therefore reds here.
b9=()
for c in core-tests memory-budget integration-tests write-tests cli-tests \
         compaction-byte-parity minimal-build smoke; do
  # Portable boundary (see B8). The class excludes `-` as well as alnum/underscore, because
  # component names contain hyphens and `write-tests` must not match `write-tests-extra`.
  grep -qE "_fm_observe_child $c([^[:alnum:]_-]|$)" "$GATE" || b9+=("$c-does-not-record-in-body")
done
fm_end_for_b9=$(grep -n '^# ==== END feature-matrix annotation' "$GATE" | head -1 | cut -d: -f1)
# COMMENT lines are excluded: this file's own prose names the function repeatedly, and a
# guard that reds on a comment is a guard people learn to waive.
stray=$(grep -nE '(^|[^_[:alnum:]])_fm_observe_cargo_argv ' "$GATE" \
  | awk -F: -v e="${fm_end_for_b9:-0}" '$1 > e' \
  | grep -vE '^[0-9]+:[[:space:]]*#' \
  | grep -v 'AGENT_GATE_FM_COMPONENT=' || true)
[ -n "$stray" ] && b9+=("parent-side-pre-record: $(printf '%s' "$stray" | cut -d: -f1 | tr '\n' ' ')")
if [ "${#b9[@]}" -eq 0 ]; then
  ok "B9: all eight bash -c components record from INSIDE the body, and no parent-side pre-record remains (a record now means an invocation that STARTED)"
else
  bad "B9: ${b9[*]}"
fi

# B10: the indirect rendering has ONE spelling. The class-based arm (python-bindings /
# node-bindings) and the scoped python tier's per-invocation record must both come from
# _fm_indirect_desc — two spellings of one state read as two states.
b10=()
grep -qF '"${class#indirect:}"' "$GATE" \
  || b10+=("the-class-arm-does-not-name-the-driver-of-an-unrecorded-indirect-component")
# The per-invocation driver-reach records (roborev job 273, F3) must use the same spelling:
# _fm_note_driver is the parent-side recorder and _fm_observe_driver the child-callable one,
# and BOTH must render through _fm_indirect_desc.
grep -qE 'reached\) _fm_note "\$1" "\$\(_fm_indirect_desc "\$2"\)"' "$GATE" \
  || b10+=("_fm_note_driver-does-not-use-_fm_indirect_desc")
grep -qE '_fm_indirect_desc "\$driver"' "$GATE" \
  || b10+=("_fm_observe_driver-does-not-use-_fm_indirect_desc")
n_literal=$(grep -c "printf 'via %s: feature set NOT observed'" "$GATE")
[ "$n_literal" = 1 ] || b10+=("the-literal-appears-$n_literal-times-outside-_fm_indirect_desc")
n_unobs=$(grep -c "printf 'cargo not observable: %s'" "$GATE")
[ "$n_unobs" = 1 ] || b10+=("the-unobservable-literal-appears-$n_unobs-times-outside-_fm_unobservable_desc")
if [ "${#b10[@]}" -eq 0 ]; then
  ok "B10: the 'via <driver>' and 'cargo not observable' texts each have exactly ONE definition, used by the class arm AND by both driver-reach recorders"
else
  bad "B10: ${b10[*]}"
fi

# B11: the scoped python tier RECORDS, and does so from the build-verify RC — not from
# merely reaching the branch. rc 1 (venv/pip setup) and rc 4 (no cargo/rustc) mean maturin
# was never invoked, so claiming an unobserved cargo invocation there would be this
# issue's own defect one level down.
# B11: EVERY indirect component records its driver's REACH from an explicit signal, in ONE
# shared implementation (roborev job 273, F3). The rc-aware record existed for the scoped
# python tier alone; python-bindings discarded its rc (a bare if/else on success) and
# node-bindings had no record at all, so both claimed an unobserved cargo invocation on a
# failure that never reached the driver. Asserted structurally as well as behaviourally
# (sections R/PB below) because a NEW indirect component is the case behaviour cannot cover.
b11=()
grep -qE '_fm_note_maturin_rc "\$name" "\$pbv_rc"' "$GATE" \
  || b11+=("run_scoped_tests-does-not-record-the-python-tier-from-its-rc")
grep -qE '^    0\|2\|3\) _fm_note_driver "\$1" maturin reached ;;' "$GATE" \
  || b11+=("_fm_note_maturin_rc-no-longer-conditions-on-rc-0-2-3")
# python-bindings: the rc must be CAPTURED (it used to be discarded) and fed to the shared
# mapper — one call, in the component that owns the driver.
grep -qE '\|\| pbv_rc=\$\?' "$GATE" \
  || b11+=("run_python_bindings-does-not-capture-the-build-verify-rc")
grep -qE '_fm_note_maturin_rc "\$name" "\$pbv_rc"' "$GATE" \
  || b11+=("run_python_bindings-does-not-feed-its-rc-to-the-shared-mapper")
# node-bindings: the recorder must sit IMMEDIATELY BEFORE the driver, inside the body — the
# adjacency IS the property (a record anywhere earlier would describe intent again, which is
# the defect job 269 blocker 2 fixed one level up).
nb_next=$(grep -A2 '_fm_observe_driver node-bindings' "$GATE" \
  | grep -vE '_fm_observe_driver|^--$' | grep -vE '^\s*#' | head -1 | sed 's/^[[:space:]]*//')
case "$nb_next" in
  'npm run build'*) ;;
  *) b11+=("node-bindings-recorder-is-not-immediately-before-npm-run-build(next-line:'${nb_next:-<none>}')") ;;
esac
# …and every `indirect:` class declared must have SOME recording site naming its component,
# derived from the class table rather than a list typed here.
for _ic in python-bindings node-bindings; do
  case "$(_fm_component_class "$_ic")" in
    indirect:*)
      grep -qE "(_fm_observe_driver $_ic|_fm_note_maturin_rc \"\\\$name\")" "$GATE" \
        || b11+=("$_ic-declares-a-driver-but-records-no-reach") ;;
  esac
done
if [ "${#b11[@]}" -eq 0 ]; then
  ok "B11: every indirect component records its driver's reach from an explicit signal (rc 0/2/3 for both maturin callers; an in-body recorder immediately before node's napi build)"
else
  bad "B11: ${b11[*]}"
fi

# B6: `command -v cargo` is UNSAFE once the observer defines a shell function named
# `cargo` — `command -v` finds FUNCTIONS, so a toolchain-presence probe would answer
# "present" on a box with no cargo binary and turn a SKIP into a FAIL (measured: it did,
# in run_oom_audit and the python-bindings build probe). Any such probe must use
# `type -P`, which searches PATH only. The rule is positional and therefore sound: code
# textually AFTER the definition block always runs after it (top-level flow reaches the
# block first, and every function defined below it is called later), while the one probe
# above the block — the PATH-augmentation check near the top — runs before any function
# exists and is correct as written.
fm_begin=$(grep -n '^# ==== BEGIN feature-matrix annotation' "$GATE" | head -1 | cut -d: -f1)
if [ -z "$fm_begin" ]; then
  bad "B6: could not locate the '==== BEGIN feature-matrix annotation' marker in $GATE"
else
  # Portable boundary, not GNU `\b` (POSIX ERE leaves it undefined; BSD grep on macOS ignores
  # it, so this scan would find NOTHING there and B6 would pass vacuously). Note the descoped
  # lint could never have caught this line: its needle was `grep [^|]*\b`, and the alternation
  # `(cargo|rustc)` contains the very `|` that needle refuses to span.
  late_probes=$(grep -nE 'command -v (cargo|rustc)([^[:alnum:]_]|$)' "$GATE" | awk -F: -v b="$fm_begin" '$1 > b')
  if [ -z "$late_probes" ]; then
    ok "B6: no 'command -v cargo/rustc' probe below the observer definition (they use type -P, which cannot match the shell function)"
  else
    bad "B6: a 'command -v cargo/rustc' probe below the observer would always answer 'present' (use type -P): $late_probes"
  fi
fi

# B7: THE ANNOTATION'S SOURCE IS NOT ENV-SELECTABLE (CLAUDE.md #3312 job 27 — "the
# constrained party must not choose its own enforcer"). The annotation IS the block's
# evidence about what was certified, so an env-settable helper path (or an inherited
# component name) would let the environment substitute a fabricator while the SUMMARY
# looked identical — a FORGED annotation, strictly worse than a blank one because it is
# affirmatively false rather than merely absent. Three properties, all read from source:
#   (a) the functions are DEFINED INSIDE the marker block of this very script — nothing is
#       sourced from a path the environment could redirect;
#   (b) the two state variables are assigned UNCONDITIONALLY (no `${…:-…}` default, which
#       is exactly how an inherited value wins), so an inherited component name cannot arm
#       recording before the first component;
#   (c) no `.`/`source` of a variable-named file appears inside the block.
# The needles are SPLIT so this assert cannot match its own source lines.
fm_end_ln=$(grep -n '^# ==== END feature-matrix annotation' "$GATE" | head -1 | cut -d: -f1)
if [ -z "$fm_begin" ] || [ -z "$fm_end_ln" ] || [ "$fm_end_ln" -le "${fm_begin:-0}" ]; then
  bad "B7: could not locate a well-ordered BEGIN/END feature-matrix marker pair in $GATE"
else
  fm_block=$(sed -n "${fm_begin},${fm_end_ln}p" "$GATE")
  b7=()
  for fn in _fm_describe_cargo _fm_annotate _fm_summary_line cargo env; do
    # HERE-STRING, NOT A PIPE. `grep -q` exits on the FIRST match, so under this file's
    # `set -o pipefail` a piped `printf` of a large block gets SIGPIPE and the PIPELINE
    # returns 141 — discarding a successful match and reporting a present function as
    # MISSING. Measured: this made B7 false-FAIL ~2 runs in 3, and WHICH functions were
    # reported missing tracked their offset in the block (the two earliest, at offsets 140
    # and 235 of 577, lost the race; the ones at 365+ did not). This is #3400's rule —
    # "read by redirection, never a pipe" — via a second mechanism: not a subshell
    # swallowing the verdict, but SIGPIPE inverting it.
    grep -q "^$fn() {" <<<"$fm_block" || b7+=("$fn-not-defined-inside-the-block")
  done
  # (b) unconditional assignment of both state variables.
  var_dir="AGENT_GATE_FM""_DIR"
  var_comp="AGENT_GATE_FM""_COMPONENT"
  grep -qE "^$var_dir=\"\\\$LOG_DIR\"$" "$GATE" || b7+=("fm-dir-not-assigned-unconditionally-from-LOG_DIR")
  grep -qE "^$var_comp=\"\"$" "$GATE" || b7+=("fm-component-not-cleared-of-any-inherited-value")
  # …and no env-default form for either, anywhere in the script.
  envdefault="\\\$\{AGENT_GATE_FM_"
  if grep -nE "^(AGENT_GATE_FM_(DIR|COMPONENT))=.*$envdefault" "$GATE" >/dev/null 2>&1; then
    b7+=("an-env-default-selects-the-annotation-state")
  fi
  # (c) no sourcing of a variable path inside the block.
  # Here-string for the same SIGPIPE-under-pipefail reason as the loop above; this one is
  # the more dangerous direction, since a discarded match would report a REAL sourcing of a
  # variable path as absent.
  if grep -qE '^[[:space:]]*(\.|source)[[:space:]]+"?\$' <<<"$fm_block"; then
    b7+=("the-block-sources-a-variable-named-file")
  fi
  if [ "${#b7[@]}" -eq 0 ]; then
    ok "B7: the annotation is defined INLINE in the gate script, its state is assigned unconditionally, and nothing env-settable selects its source (#3312 job 27)"
  else
    bad "B7: ${b7[*]}"
  fi
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
expect_desc "U9b: nextest names its RUN subcommand" 'nextest run cqlite-core --features cli-helpers' nextest run --package cqlite-core --features cli-helpers --test-threads 16
expect_reject "U9c: cargo nextest --version is a version PROBE, not a compile" nextest --version
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

# An indirect component with NO record at all: the driver is NAMED and the state is a
# visible recording gap. It is deliberately NOT the via-driver text (roborev job 273, F3):
# rendering "cargo ran under maturin" for a component that recorded nothing is a claim, and
# the whole point of the class is that the claim must come from an observation.
got=$(_fm_annotate python-bindings) # indirect, empty sidecar
case "$got" in
  *UNDECLARED*maturin*) ok "R5: an indirect component with NO recorded driver outcome NAMES the driver and reads UNDECLARED — it does not assume the driver ran" ;;
  *) bad "R5: got '$got'" ;;
esac
# …and WITH a recorded reach it renders the via-driver text.
AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_driver python-bindings maturin reached
got=$(_fm_annotate python-bindings)
[ "$got" = '[via maturin: feature set NOT observed]' ] \
  && ok "R5b: a recorded driver reach renders 'via <driver>: feature set NOT observed'" \
  || bad "R5b: got '$got'"
rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"

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
AGENT_GATE_FM_COMPONENT=oom-audit _fm_note_if_no_cargo_observed oom-audit SKIP
got=$(_fm_annotate oom-audit)
case "$got" in
  *'SKIPped'*) ok "R7: a SKIP before any cargo call renders 'no cargo invoked (component SKIPped)'" ;;
  *) bad "R7: got '$got'" ;;
esac
# …and a declared no-cargo component is left exactly as it was.
AGENT_GATE_FM_COMPONENT=pub-surface _fm_note_if_no_cargo_observed pub-surface SKIP
got=$(_fm_annotate pub-surface)
[ "$got" = '[no-cargo]' ] && ok "R8: the SKIP note does not disturb a declared no-cargo component" || bad "R8: got '$got'"
# A FAIL that died BEFORE its first cargo call (a fail-closed derivation, a guard script)
# now legitimately leaves an EMPTY sidecar, because the records moved inside the child body
# (roborev job 269, blocker 2). It must say so, not read UNDECLARED.
AGENT_GATE_FM_COMPONENT=cli-tests _fm_note_if_no_cargo_observed cli-tests FAIL
got=$(_fm_annotate cli-tests)
case "$got" in
  *'FAILed before its first cargo build/test invocation'*'metadata probes are not recorded'*)
    ok "R10: a FAIL before any cargo call names that state — and names the METADATA-PROBE exclusion, because a `cargo tree` probe may well have run (measured: three components FAIL exactly there)" ;;
  *) bad "R10: got '$got'" ;;
esac
rm -f "$AGENT_GATE_FM_DIR/cli-tests.features"
# …and an INDIRECT component that FAILs with an EMPTY sidecar did NOT reach its driver
# (roborev job 273, F3). The old rule kept the via-driver rendering here on the reasoning
# that "an indirect component that FAILs DID run cargo" — but python-bindings can fail in
# venv/pip setup before maturin, and node-bindings in `npm ci` before `npm run build`, and
# both then claimed a cargo invocation that never happened. The assumption is unnecessary
# now that every indirect component RECORDS its driver's reach, so an empty sidecar is
# positive evidence and this case is the regression test for the inversion.
rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_if_no_cargo_observed python-bindings FAIL
got=$(_fm_annotate python-bindings)
case "$got" in
  *'FAILed before reaching its driver'*maturin*)
    ok "R11: an INDIRECT component that FAILs with NO driver record says it never reached the driver — it does NOT claim an unobserved cargo run" ;;
  *) bad "R11: got '$got'" ;;
esac
rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
# …and when the driver WAS reached (recorded at execution time), the same terminal FAIL
# keeps the via-driver rendering: a failed build is an invocation that happened.
AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_driver python-bindings maturin reached
AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_if_no_cargo_observed python-bindings FAIL
got=$(_fm_annotate python-bindings)
[ "$got" = '[via maturin: feature set NOT observed]' ] \
  && ok "R11b: a recorded driver reach survives a terminal FAIL (a failed build IS an invocation)" \
  || bad "R11b: got '$got'"
rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
# An indirect component with NO record and NO terminal note is a RECORDING GAP, and it must
# read as one — UNDECLARED, carrying the token every existing detector greps for, and NAMING
# the driver whose outcome is missing. This is what makes a fourth indirect component that
# forgets to record mechanically visible instead of silently claiming a cargo run.
got=$(_fm_annotate node-bindings)
case "$got" in
  *UNDECLARED*'npm run build (napi)'*)
    ok "R11c: an indirect component with no recorded driver outcome renders UNDECLARED naming the driver (a visible recording gap, not a claim)" ;;
  *) bad "R11c: got '$got'" ;;
esac
# The reach mapping is the ONE place both maturin callers agree, so it is measured as a table
# rather than at one call site. THREE INPUT STATES since roborev job 285, because the rc alone
# cannot express execution history: the self-heal path returns 1 or 4 from a SECOND setup
# attempt AFTER a first build already invoked maturin, so an rc-only table claimed
# "never reached maturin" about a maturin run that happened.
_rc_marker="$tmp/pbv-reach-marker"
rc_bad=()
# (i) POSITIVE EVIDENCE WINS. Marker present => reached, whatever the rc says. rc 1 and 4 are
#     the two the old table called not-reached, so they are the cases that matter here.
for _rc in 0 1 2 3 4 none 7; do
  rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
  : > "$_rc_marker"
  AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_maturin_rc python-bindings "$_rc" "$_rc_marker"
  got=$(_fm_annotate python-bindings)
  [ "$got" = '[via maturin: feature set NOT observed]' ] || rc_bad+=("marker+rc$_rc:'$got'")
done
rm -f "$_rc_marker"
# (ii) Marker mechanism ACTIVE but marker ABSENT => the negative is MEASURED, and the rc
#      explains why. 0/2/3 still mean the build ran (maturin exited, however it exited).
for _rc in 0 2 3; do
  rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
  AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_maturin_rc python-bindings "$_rc" "$_rc_marker"
  got=$(_fm_annotate python-bindings)
  [ "$got" = '[via maturin: feature set NOT observed]' ] || rc_bad+=("absent+rc$_rc:'$got'")
done
# (iii) NO marker path at all => UNKNOWN, never a negative. Asserting a not-reached we did not
#       measure is the same unfounded claim this annotation exists to prevent.
for _rc in 1 4 none; do
  rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
  AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_maturin_rc python-bindings "$_rc"
  got=$(_fm_annotate python-bindings)
  case "$got" in
    *'UNKNOWN'*'not measured'*) ;;
    *) rc_bad+=("nomarker+rc$_rc:'$got'") ;;
  esac
done
# (iv) Marker mechanism ACTIVE, marker ABSENT, and an rc that means the driver was never
#      entered => a MEASURED not-reached, with the rc supplying the reason. This is the only
#      state in which a negative claim is legitimate.
for _rc in 1 4 none 7; do
  rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
  AGENT_GATE_FM_COMPONENT=python-bindings _fm_note_maturin_rc python-bindings "$_rc" "$_rc_marker"
  got=$(_fm_annotate python-bindings)
  case "$got" in *'never reached maturin'*) ;; *) rc_bad+=("absent+rc$_rc:'$got'") ;; esac
done
rm -f "$AGENT_GATE_FM_DIR/python-bindings.features"
if [ "${#rc_bad[@]}" -eq 0 ]; then
  ok "R11d: the shared reach table is exact in all THREE input states — a present marker records the invocation whatever the rc (the job-285 false negative), an absent marker with the mechanism active records a MEASURED not-reached, and no marker at all records UNKNOWN rather than a guessed negative"
else
  bad "R11d: ${rc_bad[*]}"
fi
# THE CHILD-CALLABLE RECORDER really works from a CHILD SHELL. `_fm_observe_driver` is
# exported for exactly this (node's napi build is inside a `bash -c` body); an unexported
# recorder would silently record nothing and the component would read UNDECLARED. Driven
# through `bash -c` deliberately — the property IS the export.
rm -f "$AGENT_GATE_FM_DIR/node-bindings.features"
( export -f _fm_observe_driver _fm_indirect_desc _fm_sidecar
  bash -c '_fm_observe_driver node-bindings "npm run build (napi)"' )
got=$(_fm_annotate node-bindings)
[ "$got" = '[via npm run build (napi): feature set NOT observed]' ] \
  && ok "R11e: _fm_observe_driver records from a CHILD shell (the exported-recorder property node-bindings depends on)" \
  || bad "R11e: got '$got'"
rm -f "$AGENT_GATE_FM_DIR/node-bindings.features"
# UNOBSERVABLE (roborev job 273, F2): tooling-tests' cargo runs in ~60 nested child
# scripts and no single driver can be named, so the class asserts NOTHING in either
# direction — and a terminal FAIL must NOT add a "no cargo invoked" note, because that is
# precisely the fact this shell cannot know (the false claim F2 names).
got=$(_fm_annotate tooling-tests)
case "$got" in
  *'cargo not observable'*'nested test scripts'*)
    ok "R12: an UNOBSERVABLE component names that state, claiming neither that cargo ran nor that it did not" ;;
  *) bad "R12: got '$got'" ;;
esac
AGENT_GATE_FM_COMPONENT=tooling-tests _fm_note_if_no_cargo_observed tooling-tests FAIL
got=$(_fm_annotate tooling-tests)
case "$got" in
  *'cargo not observable'*) ok "R12b: a terminal FAIL adds NO 'no cargo invoked' note to an unobservable component (a child cargo build may well have run)" ;;
  *) bad "R12b: got '$got'" ;;
esac
# …and an in-shell observation rides ADDITIVELY beside the class text: the observed sets are
# real, and they are not the whole story.
printf 'test cqlite-core --features x\n' > "$AGENT_GATE_FM_DIR/tooling-tests.features"
got=$(_fm_annotate tooling-tests)
case "$got" in
  *'test cqlite-core --features x'*'| + cargo not observable'*)
    ok "R12c: an observed set on an unobservable component is ADDITIVE — both facts are named" ;;
  *) bad "R12c: got '$got'" ;;
esac
rm -f "$AGENT_GATE_FM_DIR/tooling-tests.features"

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
  done < <(grep -E '^(fmt|clippy|core-tests|smoke): +(PASS|FAIL|SKIP|VACUOUS)' "$selftest_sum")
  n_annot=$(grep -cE '^(fmt|clippy|core-tests|smoke): +(PASS|FAIL|SKIP|VACUOUS) \([0-9]+s\)  \[.+\]' "$selftest_sum")
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
# (W) WRAPPER FIDELITY — the highest-risk property of this change
# ---------------------------------------------------------------------------
# Shadowing `cargo` and `env` as shell functions means that if a wrapper ever fails to
# hand off to the real binary EXACTLY, it breaks every cargo-invoking component at once.
# So the pass-through is MEASURED, not reasoned: a substituted `cargo` artifact on PATH
# (never a path variable — #3312 job 27's corollary) echoes its argv/stdin and exits with
# a chosen status, and the wrapper must reproduce all of it. Recording must add nothing to
# stdout or stderr, because component logs are PARSED (#3400).
fid="$tmp/fidelity"; mkdir -p "$fid"
cat > "$fid/cargo" <<'FID'
#!/usr/bin/env bash
# Substituted cargo: reports argv one-per-line on stdout, a marker on stderr, echoes
# stdin, and exits with $FID_RC.
printf 'ARG[%s]
' "$@"
printf 'STDIN[%s]
' "$(cat)"
printf 'ERRMARK
' >&2
exit "${FID_RC:-0}"
FID
chmod +x "$fid/cargo"

# The wrapper must be ARMED for these cases (an unarmed observer would trivially "pass"
# a no-op test): point it at a live sidecar dir and a component name.
export AGENT_GATE_FM_DIR="$tmp/fid-side"; mkdir -p "$AGENT_GATE_FM_DIR"
export AGENT_GATE_FM_COMPONENT=core-tests

w_out="$tmp/w.out"; w_err="$tmp/w.err"
(
  PATH="$fid:$PATH"
  FID_RC=7 cargo test --package cqlite-core --features cli-helpers "a b" "" "glob*" </dev/null >"$w_out" 2>"$w_err"
  echo "$?" > "$tmp/w.rc"
)
[ "$(cat "$tmp/w.rc")" = 7 ] && ok "W1: the cargo wrapper propagates the real binary's exit status (7)" \
  || bad "W1: exit status was $(cat "$tmp/w.rc"), expected 7"

# argv EXACTLY as given: an argument with a space stays ONE argument, an EMPTY argument
# survives, and a glob character is not expanded (the recording path must not word-split).
want_args=$'ARG[test]\nARG[--package]\nARG[cqlite-core]\nARG[--features]\nARG[cli-helpers]\nARG[a b]\nARG[]\nARG[glob*]'
got_args=$(grep '^ARG\[' "$w_out")
[ "$got_args" = "$want_args" ] && ok "W2: argv reaches the real binary byte-exact (space-bearing, EMPTY and glob args preserved)" \
  || { bad "W2: argv differs"; printf 'got:\n%s\nwant:\n%s\n' "$got_args" "$want_args"; }

# stdout carries ONLY the binary's output (no recording chatter), stderr ONLY the
# binary's — component logs are parsed, so one stray line is a real defect (#3400).
if [ "$(grep -c . "$w_err")" = 1 ] && grep -q '^ERRMARK$' "$w_err"; then
  ok "W3: stderr carries only the real binary's output — the observer writes nothing to it"
else
  bad "W3: stderr was polluted: $(tr '\n' '|' <"$w_err")"
fi
if ! grep -qvE '^(ARG\[|STDIN\[)' "$w_out"; then
  ok "W4: stdout carries only the real binary's output — the observer writes nothing to it"
else
  bad "W4: stdout was polluted: $(grep -vE '^(ARG\[|STDIN\[)' "$w_out" | tr '\n' '|')"
fi

# stdin reaches the real binary (a wrapper that consumed it would break any cargo
# subcommand reading stdin). Driven in THIS shell, deliberately NOT through `bash -c`: a
# child bash does not inherit the (unexported) function, so `cargo` there would resolve
# straight to the substituted binary on PATH and the case would pass having tested the
# artifact instead of the wrapper — a vacuous pass (it did, first try).
got_stdin=$(
  PATH="$fid:$PATH"
  printf hello-stdin | cargo build | sed -n 's/^STDIN\[\(.*\)\]$/\1/p'
)
[ "$got_stdin" = hello-stdin ] && ok "W5: stdin passes through the cargo wrapper untouched" \
  || bad "W5: stdin arrived as '$got_stdin'"

# The env wrapper on the run_clippy path: status + argv + the cargo argv it records.
: > "$AGENT_GATE_FM_DIR/clippy.features"
(
  PATH="$fid:$PATH"
  AGENT_GATE_FM_COMPONENT=clippy \
    env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-core --all-targets --features "a b" </dev/null >"$tmp/e.out" 2>/dev/null
  echo "$?" > "$tmp/e.rc"
)
if [ "$(cat "$tmp/e.rc")" = 0 ] && [ "$(grep -c '^ARG\[' "$tmp/e.out")" = 6 ]; then
  ok "W6: the env wrapper execs the real cargo with argv intact and propagates its status"
else
  bad "W6: rc=$(cat "$tmp/e.rc") argv=$(grep -c '^ARG\[' "$tmp/e.out") (expected 0 / 6)"
fi
got=$(_fm_annotate clippy)
[ "$got" = '[clippy cqlite-core --features a,b]' ] \
  && ok 'W7: an "env VAR=... cargo ..." invocation IS recorded (the run_clippy path, which an env prefix would otherwise hide)' \
  || bad "W7: got '$got'"
unset AGENT_GATE_FM_COMPONENT
export AGENT_GATE_FM_DIR="$tmp/side"

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
# <shim-dir> defaults to the PASSING shim; section (D) passes the FAILING one. It is a
# function ARGUMENT, never an env var: a test-only env seam is one more thing a real
# invoker can set (#3312 job 27's corollary), and the caller here is this file.
# FM_LAST_EXEC_COUNT is set to the number of cargo invocations the shim actually saw, so a
# caller can prove a short-circuit happened instead of assuming it.
# fm_strip_census <annotation-tail>: remove the #3625 component-census suffix, which
# _fm_summary_line appends AFTER the feature-matrix annotation (`…]  {verified: N tests}`).
# This file's subject is the FEATURE MATRIX, so the census must not enter its comparisons —
# and it must not be matched by the `*UNDECLARED*` screens below either, since the census
# has an UNDECLARED state of its own that means something different. `%` (shortest match
# from the END) so a `{` inside a feature descriptor could not truncate the annotation.
fm_strip_census() { printf '%s' "${1%  \{*}"; }

FM_LAST_EXEC_COUNT=0
run_differential() { # <component> <mode EXACT|CONTAINS> [why-not-exact] [shim-dir] [tag]
  local c="$1" mode="$2" why="${3:-}" use_shim="${4:-$shim_dir}" tag="${5:-}"
  local sum="$tmp/only-$c$tag.txt" log="$tmp/only-$c$tag.log" shimlog="$tmp/argv-$c$tag.log"
  : > "$shimlog"
  FM_LAST_EXEC_COUNT=0
  FM_SHIM_LOG="$shimlog" \
  AGENT_GATE_SUMMARY_FILE="$sum" \
  AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  PATH="$use_shim:$PATH" \
    bash "$GATE" --only "$c" > "$log" 2>&1
  local line ann
  line=$(grep -E "^$c: +(PASS|FAIL|SKIP|VACUOUS)" "$sum" 2>/dev/null | head -1)
  if [ -z "$line" ]; then
    bad "C-$c$tag: no '$c:' component line in the emitted block"
    return
  fi
  ann=${line#*\[}; ann="[${ann}"
  ann=$(fm_strip_census "$ann")
  case "$ann" in
    '[UNDECLARED]'|*UNCLASSIFIED*|'[]') bad "C-$c$tag: annotation is '$ann'"; return ;;
  esac
  local exec_side="$tmp/exec-$c$tag.features"
  describe_shim_log "$shimlog" "$exec_side"
  FM_LAST_EXEC_COUNT=$(grep -c . "$exec_side" 2>/dev/null || echo 0)
  if [ ! -s "$exec_side" ]; then
    bad "C-$c$tag: the shim recorded no compile/run cargo invocation — the differential proved nothing"
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
      ok "C-$c$tag: declared matrix == the argv that EXECUTED  $ann"
    else
      bad "C-$c$tag: DRIFT — block says $ann but the executed argv describes $expected"
    fi
  else
    local missing=() dsc
    while IFS= read -r dsc; do
      case "$ann" in *"$dsc"*) ;; *) missing+=("$dsc") ;; esac
    done < <(sort -u "$exec_side")
    if [ "${#missing[@]}" -eq 0 ]; then
      ok "C-$c$tag: every EXECUTED set is named in the declared matrix (CONTAINS${why:+; $why})  $ann"
    else
      bad "C-$c$tag: executed set(s) NOT named in the block: ${missing[*]}"
    fi
  fi
}

# NO opt-out env var here, deliberately (CLAUDE.md #3312 job 27 corollary): a test-only
# seam is one more thing a real invoker can set, and section (C) is the only part of this
# guard that MEASURES rather than inspects — an env flag that silently skipped it would be
# a vacuous green wearing a debug flag's clothes. A case needing a different cargo
# SUBSTITUTES THE ARTIFACT in its own scratch dir (below), never a path variable.
{
  # core-tests' nextest branch is one of the eight `bash -c` bodies (taken only when
  # cargo-nextest is installed), and it is the component whose line is pasted most often.
  # EXACT on BOTH hosts since the records moved INSIDE the body (job 269 blocker 2): the
  # nextest branch records its two passes as they run, and the direct-cargo fallback is
  # observed by the in-shell wrapper — each host's declared set is now, by construction,
  # exactly what that host executed. (It was CONTAINS before, because the PARENT declared
  # the nextest pair even on a host that ran the fallback.)
  run_differential core-tests        EXACT
  run_differential minimal-build     EXACT
  run_differential write-tests       EXACT
  run_differential memory-budget     EXACT
  run_differential integration-tests EXACT
  # compaction-byte-parity is an EIGHTH `bash -c` body, behind an `env` prefix as well.
  # It SKIPs without the committed test_compactionparity fixtures, and a differential over
  # a component that never ran proves nothing — so it is reported as a SKIP rather than
  # asserted, and only when the fixtures are actually here.
  if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-/nonexistent}/sstables/test_compactionparity" ]; then
    run_differential compaction-byte-parity EXACT
  else
    skipped "C-compaction-byte-parity: committed test_compactionparity fixtures absent under CQLITE_DATASETS_ROOT — the component SKIPs, so the differential would prove nothing"
  fi
  # cli-tests and smoke were CONTAINS for exactly the defect job 269 blocker 2 names: the
  # body ABORTS partway under a stub (cli-tests' Pass-1 zero-tests guard fires; the smoke
  # script needs a real built binary), and the parent declared the unreached sets anyway.
  # Now that each pass records itself as it runs, both are EXACT — and EXACT here is the
  # regression test for the blocker: reintroduce a parent-side pre-record and these red.
  run_differential cli-tests         EXACT
  run_differential smoke             EXACT
}

# ---------------------------------------------------------------------------
# (D) TRUTHFUL ON A SHORT-CIRCUIT — measured with a FAILING cargo shim
# ---------------------------------------------------------------------------
# roborev job 269, blocker 2: the records used to be written by the PARENT before the
# child body ran, so they described INTENT. A `cli-tests: FAIL` line then named BOTH of
# its feature sets even when Pass 1 died before Pass 2 started, and write-tests claimed
# the same set `x3` after failing on the first of three `&&`-chained passes. A failure
# summary that claims an invocation which never occurred is affirmatively false.
#
# The instrument: a SUBSTITUTED cargo artifact in its own scratch dir (never a path
# variable — #3312 job 27's corollary) that records its argv and EXITS 1, so every body
# aborts after its first invocation. Two assertions per component, because either alone
# would be weak:
#   (i) NON-VACUITY — the failing run must have executed strictly FEWER invocations than
#       section (C)'s passing run. Without this, a component that legitimately ran
#       everything would "pass" this section having proved nothing about short-circuits.
#  (ii) TRUTHFULNESS — declared == executed (EXACT), rendered through the gate's own
#       renderer, never a curated expectation string.
failshim="$tmp/failshim"; mkdir -p "$failshim"
cat > "$failshim/cargo" <<'FSHIM'
#!/usr/bin/env bash
# Failing cargo shim: records its argv, compiles nothing, and FAILS — so every
# `&&`-chained / `set -e` component body aborts after this first invocation.
printf '%s\n' "$*" >> "${FM_SHIM_LOG:?}"
exit 1
FSHIM
chmod +x "$failshim/cargo"

# cli-tests is deliberately NOT in this loop: under the PASSING shim its Pass-1 zero-tests
# guard already fires, so its baseline is itself a one-invocation short-circuit and the
# "strictly fewer" non-vacuity test has no headroom. C-cli-tests (EXACT) is what measures
# it — and it measures exactly the blocker's motivating case.
for fc in write-tests integration-tests minimal-build core-tests; do
  pass_n=$(grep -c . "$tmp/exec-$fc.features" 2>/dev/null || echo 0)
  run_differential "$fc" EXACT "" "$failshim" "-shortcircuit"
  fail_n=$FM_LAST_EXEC_COUNT
  if [ "${pass_n:-0}" -le 0 ]; then
    bad "D-$fc: section (C) recorded no passing-run baseline, so 'fewer under failure' has no subject"
  elif [ "${fail_n:-0}" -lt "$pass_n" ]; then
    ok "D-$fc: the body really short-circuited under a failing cargo ($fail_n of $pass_n invocation(s) ran) AND the block named only those"
  else
    bad "D-$fc: no short-circuit observed ($fail_n vs $pass_n invocations) — the EXACT assert above proved nothing about a failure path"
  fi
done

# ---------------------------------------------------------------------------
# (P) THE INDIRECT PYTHON TIER IS NAMED, NOT GUESSED — roborev job 269, blocker 1
# ---------------------------------------------------------------------------
# The scoped --lite python tier builds the extension by running maturin in a CHILD
# process, so the cargo invocation maturin makes can never pass through this shell's
# observer. Before this fix a PURE-python --lite reported `scoped-tests: PASS …
# [UNDECLARED]` ("nobody said"), and a MIXED rust+python diff listed only the rust matrix,
# silently omitting the maturin build entirely.
#
# WHAT IS MEASURED vs WHAT IS STUBBED, stated rather than implied: run_scoped_tests is the
# REAL function, extracted from the shipped gate, and the annotation is rendered through
# the real _fm_annotate. Its ROUTING (classify_scoped_plan, separately asserted by the
# py-route cases in test_agent_gate_summary.sh) and its --python-build-verify CHILD are
# stubbed, because they are the two things that would otherwise require a git fixture, a
# cargo workspace and a real maturin toolchain. So this section measures "given this
# route and this build outcome, what does the block say" — which is precisely the
# blocker's subject.
# NOTE the variable name: bash locals are DYNAMICALLY scoped, so a stub referring to
# `$plan` would resolve to run_scoped_tests' OWN `local plan` (empty at the moment the stub
# runs) rather than to this function's argument — the routing then silently fell through to
# the default `cqlite-core --lib` and all four cases below failed for a reason that had
# nothing to do with the subject (it did, first try).
py_run() { # <plan-lines> <build-verify-rc> ; prints the rendered scoped-tests annotation
  local py_plan_in="$1" rc="$2"
  # A FRESH scratch (and therefore a fresh sidecar) per call: two calls sharing one
  # sidecar accumulate each other's records, and the second case then measures the first
  # one's leftovers (it did: `via maturin … x2` on a route that recorded it once).
  local scratch; scratch=$(mktemp -d "$tmp/py-XXXXXX") || return 1
  mkdir -p "$scratch/side"
  cat > "$scratch/fake-gate-self" <<FAKESELF
#!/usr/bin/env bash
# Stands in for \`bash "\$GATE_SELF" --python-build-verify …\`: writes no active-venv
# path (so the caller falls back to the shared venv) and returns the chosen rc.
exit $rc
FAKESELF
  chmod +x "$scratch/fake-gate-self"
  (
    # Collaborators of run_scoped_tests that are NOT the subject here.
    classify_scoped_plan() { printf '%s\n' "$py_plan_in"; }
    _package_index() { printf '%s\t%s\t%s\n' "$REPO_ROOT/cqlite-core" cqlite-core 1; }
    classify_test_targets() { cat >/dev/null; :; }
    classify_core_dependent_compile_check() { cat >/dev/null; :; }
    _scoped_noparser_fail_msg() { printf 'no metadata parser'; }
    PYTHON_LITE_TIER_CMD="maturin develop && pytest"
    PYTHON_LITE_MATURIN_CMD="maturin develop"
    PYTHON_LITE_PYTEST_CMD="true"
    PYTHON_TIER_NOTE=""
    GATE_SELF="$scratch/fake-gate-self"
    # REPO_ROOT drives the venv path only; pointing it at a scratch dir keeps the pytest
    # phase from ever activating a real venv on this box.
    REPO_ROOT="$scratch"
    LOG_DIR="$scratch"
    GATE_BASE_OVERRIDE=HEAD
    OVERALL=PASS
    NAMES=(); STATUSES=(); TIMES=()
    export AGENT_GATE_FM_DIR="$scratch/side"
    PATH="$shim_dir:$PATH" FM_SHIM_LOG="$scratch/argv.log"
    export FM_SHIM_LOG="$scratch/argv.log"
    run_scoped_tests >/dev/null 2>&1
    _fm_annotate scoped-tests
  )
}
# SUBJECT-AVAILABILITY (measured under real /bin/bash 3.2 in a minimal container, where
# these four reported FAIL for a reason that had nothing to do with the subject):
# run_scoped_tests takes its #2658 fail-closed NO-PARSER exit before any routing when
# NEITHER jq nor python3 is present, so on such a host the python tier is never reached and
# these cases have no subject. Reported as SKIPs, counted in neither total (#3249's rule) —
# a case that reds on correct input is the case agents learn to waive. P5 below is
# deliberately NOT gated: it drives that very exit on purpose.
fm_have_meta_parser=0
{ type -P jq >/dev/null 2>&1 || type -P python3 >/dev/null 2>&1; } && fm_have_meta_parser=1
py_plan_pure='python-tier: maturin develop && pytest'
py_plan_mixed='rust-pkg: cqlite-core
python-tier: maturin develop && pytest'
want_via='[via maturin: feature set NOT observed]'
if [ "$fm_have_meta_parser" -eq 0 ]; then
  skipped "P1-P4: neither jq nor python3 on PATH — run_scoped_tests takes its no-parser fail-closed exit before the python tier, so these have no subject here"
else
got=$(py_run "$py_plan_pure" 0)
[ "$got" = "$want_via" ] \
  && ok "P1: a PURE-python route renders $want_via — the state is NAMED as unobservable, not left UNDECLARED" \
  || bad "P1: got '$got' want '$want_via'"
want_mixed='[test cqlite-core --features cli-helpers | via maturin: feature set NOT observed]'
got=$(py_run "$py_plan_mixed" 0)
[ "$got" = "$want_mixed" ] \
  && ok "P2: a MIXED rust+python route renders BOTH — the maturin entry is ADDITIVE, it does not replace the observed rust matrix" \
  || bad "P2: got '$got' want '$want_mixed'"
got=$(py_run "$py_plan_pure" 4)
case "$got" in
  *'never reached maturin'*)
    ok "P3: rc 4 (no cargo/rustc — maturin never invoked) records THAT, not a maturin invocation that did not happen" ;;
  *) bad "P3: rc 4 rendered '$got' — it must not claim a maturin build" ;;
esac
got=$(py_run "$py_plan_pure" 2)
[ "$got" = "$want_via" ] \
  && ok "P4: rc 2 (maturin RAN and failed) still records the maturin invocation — a failed build is an invocation that happened" \
  || bad "P4: got '$got' want '$want_via'"
fi

# (P5) run_scoped_tests' OWN TERMINAL PATHS RECORD, THOUGH THEY BYPASS record_result —
# roborev job 273, F4. This function appends its verdict DIRECTLY to NAMES/STATUSES/TIMES,
# so the note every other component gets from record_result was simply absent: the
# fail-closed no-parser exit (#2658), taken before any cargo runs, rendered `[UNDECLARED]`
# ("nobody said") instead of the fact we know exactly. Driven through the REAL function via
# the AGENT_GATE_TEST_NO_METADATA_PARSER hook the gate already ships for this branch.
got=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 py_run "$py_plan_pure" 0)
case "$got" in
  *'FAILed before its first cargo build/test invocation'*)
    ok "P5: the no-parser fail-closed exit — which bypasses record_result — records that it FAILed before any cargo call, instead of reading UNDECLARED" ;;
  *) bad "P5: got '$got'" ;;
esac

# ---------------------------------------------------------------------------
# (PB) THE PYTHON-BINDINGS DRIVER RECORD, DRIVEN END TO END — roborev job 273, F3
# ---------------------------------------------------------------------------
# run_python_bindings is the REAL function, extracted from the shipped gate. Its
# --python-build-verify CHILD is stubbed (returning a chosen rc) because a real one needs a
# venv, pip and a maturin toolchain; record_result is stubbed to the ONE thing it
# contributes here — the terminal note — because the real one writes result files and runs
# the summary/tree-integrity chokepoints, which need a whole gate run's state. That B11
# asserts record_result really calls _fm_note_if_no_cargo_observed is what keeps the stub
# honest.
#
# WHAT THIS MEASURES that the unit cases cannot: that run_python_bindings CAPTURES the rc
# it used to discard and feeds it to the shared mapper. Before this fix the component ran a
# bare `if bash … --python-build-verify; then`, so a venv/pip failure (rc 1) was
# indistinguishable from a failed maturin build and the block claimed
# `[via maturin: feature set NOT observed]` — a cargo invocation that never happened.
pyb_run() { # <build-verify-rc> ; prints the rendered python-bindings annotation
  local rc="$1" scratch
  scratch=$(mktemp -d "$tmp/pyb-XXXXXX") || return 1
  mkdir -p "$scratch/side"
  cat > "$scratch/fake-gate-self" <<FAKESELF
#!/usr/bin/env bash
# Stands in for \`bash "\$GATE_SELF" --python-build-verify …\`: writes no active-venv path
# (so the caller falls back to the shared venv, which here is a scratch path) and returns
# the chosen rc.
exit $rc
FAKESELF
  chmod +x "$scratch/fake-gate-self"
  (
    ONLY=""
    LOG_DIR="$scratch"
    REPO_ROOT="$scratch"
    GATE_SELF="$scratch/fake-gate-self"
    record_result() { _fm_note_if_no_cargo_observed "$1" "$2"; }
    export AGENT_GATE_FM_DIR="$scratch/side"
    AGENT_GATE_FM_COMPONENT=python-bindings
    run_python_bindings >/dev/null 2>&1
    _fm_annotate python-bindings
  )
}
if type -P python3 >/dev/null 2>&1; then
  got=$(pyb_run 1)
  case "$got" in
    *'never reached maturin'*'venv/pip'*)
      ok "PB1: rc 1 (venv/pip setup failed) — run_python_bindings records that maturin was NEVER REACHED, naming the cause" ;;
    *) bad "PB1: got '$got' — a venv/pip failure must not claim a maturin build" ;;
  esac
  got=$(pyb_run 4)
  case "$got" in
    *'never reached maturin'*'cargo/rustc'*)
      ok "PB2: rc 4 (no cargo/rustc on PATH) — the toolchain gap is recorded as never reaching maturin" ;;
    *) bad "PB2: got '$got'" ;;
  esac
  got=$(pyb_run 2)
  [ "$got" = '[via maturin: feature set NOT observed]' ] \
    && ok "PB3: rc 2 (maturin RAN and failed) — the invocation IS recorded; a failed build is an invocation that happened" \
    || bad "PB3: got '$got'"
else
  skipped "PB1-PB3: no python3 on PATH — run_python_bindings SKIPs before the build-verify child, so the rc plumbing has no subject here"
fi

# ---------------------------------------------------------------------------
# (E) MISCLASSIFICATION IS MECHANICALLY DETECTABLE — the part that stops a round 3
# ---------------------------------------------------------------------------
# roborev job 273, F2 survived a census that claimed "0 UNDECLARED at run time" because
# that census READ THE TABLE for tooling-tests instead of EXERCISING it. So this section
# exercises the classification: every `cargo`-class component is actually RUN, under
# `--only` with the recording cargo shim, and an annotation carrying UNDECLARED is a FAIL.
#
# WHY UNDECLARED IS A MISCLASSIFICATION AND NOT A STYLE POINT: `cargo` class MEANS "this
# component invokes cargo where this shell can observe it (its own shell, or a `bash -c`
# body that calls _fm_observe_child)". A run that reaches its terminal status with an EMPTY
# sidecar and no note therefore says one of two things is wrong — the component's cargo runs
# in a child process (it should be `indirect:<driver>` or `unobservable:<why>`), or it has a
# missing record. Both are the F2 defect, and both are invisible to a table read.
#
# THE SET IS DERIVED from the gate's COMPONENTS array + the extracted _fm_component_class,
# never typed here, so a component that joins the set — or changes class — is covered with
# no edit.
#
# E1, the one exception, and it is a FAIL DIRECTION, not an excusal: a component that CANNOT
# be exercised must not be declared `cargo`, because an unexercisable claim of observability
# is exactly what F2 was. `tooling-tests` runs THIS FILE, so exercising it would re-enter the
# guard recursively; therefore it must be non-`cargo`, asserted here. Reverting it to `cargo`
# reds this case by name — which is the mechanical detection F2 lacked.
fm_recursion_hazard="tooling-tests"
e1=()
for c in $fm_recursion_hazard; do
  if cls=$(_fm_component_class "$c" 2>/dev/null); then
    [ "$cls" = cargo ] && e1+=("$c")
  else
    e1+=("$c(undeclared)")
  fi
done
if [ "${#e1[@]}" -eq 0 ]; then
  ok "E1: the component(s) this guard cannot exercise without recursing ($fm_recursion_hazard) are NOT declared class cargo — an unexercisable observability claim is the F2 defect"
else
  bad "E1: declared class cargo but cannot be exercised here (it runs this guard): ${e1[*]} — its cargo runs in child processes, so it must be indirect:<driver> or unobservable:<why>"
fi

# E2: EXERCISE the rest.
e2_cargo=()
for c in "${comps_arr[@]}"; do
  cls=$(_fm_component_class "$c" 2>/dev/null) || continue
  [ "$cls" = cargo ] || continue
  case " $fm_recursion_hazard " in *" $c "*) continue ;; esac
  e2_cargo+=("$c")
done
e2_bad=(); e2_missing=(); e2_ran=0; e2_observed=0
for c in "${e2_cargo[@]+"${e2_cargo[@]}"}"; do
  e2_sum="$tmp/e2-$c.txt"; e2_log="$tmp/e2-$c.log"; e2_argv="$tmp/e2-$c.argv"
  : > "$e2_argv"
  FM_SHIM_LOG="$e2_argv" \
  AGENT_GATE_SUMMARY_FILE="$e2_sum" \
  AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  PATH="$shim_dir:$PATH" \
    bash "$GATE" --only "$c" > "$e2_log" 2>&1
  e2_line=$(grep -E "^$c: +(PASS|FAIL|SKIP|VACUOUS)" "$e2_sum" 2>/dev/null | head -1)
  if [ -z "$e2_line" ]; then e2_missing+=("$c"); continue; fi
  e2_ran=$((e2_ran + 1))
  e2_ann=${e2_line#*\[}; e2_ann="[$e2_ann"
  e2_ann=$(fm_strip_census "$e2_ann")
  case "$e2_ann" in
    *UNDECLARED*|*UNCLASSIFIED*|'[]') e2_bad+=("$c=$e2_ann") ;;
    *'no cargo build/test invoked'*)  ;;   # a NAMED terminal state, not a gap
    *) e2_observed=$((e2_observed + 1)) ;;
  esac
done
if [ "${#e2_cargo[@]}" -lt 20 ]; then
  bad "E2: derived only ${#e2_cargo[@]} cargo-class component(s) from COMPONENTS — the derivation looks broken, so exercising them would prove almost nothing"
elif [ "${#e2_missing[@]}" -ne 0 ]; then
  bad "E2: no component line emitted for: ${e2_missing[*]} — the exercise did not happen, so no verdict about them exists"
elif [ "$e2_observed" -lt 10 ]; then
  bad "E2: only $e2_observed of $e2_ran exercised components reported an OBSERVED matrix — the shim is probably not being reached, so a green here would be vacuous"
elif [ "${#e2_bad[@]}" -eq 0 ]; then
  ok "E2: all $e2_ran exercised cargo-class components render an observed matrix ($e2_observed) or a NAMED terminal state — none reads UNDECLARED"
else
  bad "E2: MISCLASSIFIED or missing a record (declared cargo, observed nothing): ${e2_bad[*]}"
fi

echo
echo "feature-matrix annotation guard: $PASS passed, $FAIL failed"
# A COUNT FLOOR beside the abort trap: a section that silently stops contributing verdicts
# (an extraction that broke, a subshell dying quietly) shrinks the subject set without
# aborting, and "failed: 0" over a shrunken set is the vacuous pass this file exists to
# prevent. Measured: 84 on a fully-equipped host, 75 under /bin/bash 3.2 in a minimal
# container (no jq/python3/git/cargo — the P/PB sections declare SKIPs there). The floor is
# set below the minimal-host figure so it reds on a structural loss, never on a lean host.
FM_CASE_FLOOR=60
FM_REACHED_END=1
if [ $((PASS + FAIL)) -lt "$FM_CASE_FLOOR" ]; then
  printf 'FAIL - only %s verdicts were produced (floor %s): sections are being skipped or dying silently, and a "0 failed" over a shrunken subject set certifies nothing.\n' \
    "$((PASS + FAIL))" "$FM_CASE_FLOOR" >&2
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
