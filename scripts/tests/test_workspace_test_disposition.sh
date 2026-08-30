#!/usr/bin/env bash
# Workspace test-execution disposition census (issue #3522).
#
# ONE property, at PACKAGE granularity: every cargo workspace member is EXPLICITLY
# classified in scripts/tests/workspace-test-disposition.txt as to whether the full
# agent gate EXECUTES its Rust tests.
#
#   EXECUTED      the gate runs them; the detail names the component.
#   PARTIAL       the gate runs some; the detail names the component, what is not run, and why.
#   NOT-EXECUTED  nothing runs them; the detail says why (+ an issue when one is filed).
#
# Each record carries a SECOND closed field, its CLASS, answering a different question:
# does the gap make a statement THIS REPOSITORY MAKES TO AGENTS false?
#   silent                a real gap no committed doctrine claims is covered, or one
#                         already declared honestly where an agent reads it.
#   contradicts-doctrine  committed doctrine says it is covered when it is not.
#   no-gap                nothing to classify — the record is EXECUTED.
# The two fields are COUPLED and the coupling is checked: EXECUTED <=> no-gap. That is
# the one cross-field property available without modelling the gate, and it closes the
# obvious escape — quietly marking an uncomfortable PARTIAL record `no-gap`.
# The class is DOCUMENTATION, like the detail: this guard does not verify that a
# `silent` record is really unclaimed, only that a classification was RECORDED.
#
# Why it exists. `cargo clippy --workspace --all-targets` compiles every member on
# every full gate, so a crate can be BUILT by every run and EXECUTE NOTHING — and
# before #3522 nothing said so. `cqlite-ffi-common` (52 tests) and `cqlite-node`'s 53
# Rust unit tests sat in exactly that hole, reading as covered because the workspace
# built clean. Without this guard the classification silently decays: the root
# manifest's `members` includes `tools/*` and `bindings/*`, so a NEW crate joins the
# workspace with no statement of whether anything runs its tests. That is caught here.
#
# This is the PACKAGE-granular sibling of test_tools_crate_disposition.sh, which asks
# the same shape of question ("is a disposition RECORDED?") about tools/ crates and
# their READMEs. It is modelled on that script deliberately, including its refusals.
#
# ============================ SCOPE, AND WHAT IT IS NOT ======================
# This is a DOCUMENTATION-COMPLETENESS guard. It verifies that a disposition has been
# RECORDED and LABELED with a label from a CLOSED set. It does NOT verify that the
# recorded disposition is TRUE, and it makes no such claim anywhere. A record saying
# `EXECUTED  core-tests` for a package no component runs passes this guard.
#
# THAT LIMIT IS DELIBERATE, AND THE REASON IS RECORDED SO NOBODY "IMPROVES" IT AWAY.
# test_tools_crate_disposition.sh once DID try to verify truth, by deriving workspace
# dependents from cargo and cross-checking. It was REMOVED (#1716), because:
#   * eleven review findings over eight rounds landed in the derivation machinery and
#     NONE in the list-and-label part — the derivation was where every defect lived;
#   * its self-tests built scratch workspaces outside the repository, which do not
#     inherit rust-toolchain.toml, making a MANDATORY gate component's behaviour
#     host-toolchain-dependent. A flaky mandatory gate is a fleet-wide liability.
# Verifying truth here would be strictly harder (it would have to model every gate
# component's cargo invocation, i.e. re-implement the gate). If it is ever worth doing,
# it is its own issue, not a rider on this one.
#
# Consequences, stated rather than left to be discovered:
#   * Granularity is per-PACKAGE. A new unexecuted TARGET inside an already-recorded
#     package needs no census update and passes unchanged.
#   * A PARTIAL record's account of what it omits is DOCUMENTED, not verified.
# Both are accepted limits of a small guard. A guard that overpromises is worse than a
# small one, and a guard whose FAIL an agent learns to waive is worse than no guard.
#
# NEEDS `cargo metadata` — unlike its tools/ sibling, which needs nothing. The member
# list must come from cargo (a `members` glob resolved by hand would be a second
# implementation of cargo's own resolution). A FAILED derivation is therefore a FAIL
# NAMING THE DERIVATION, never a skip that greens: a guard that could not enumerate its
# subject has measured nothing.
# FAILS CLOSED throughout: an unclassifiable or unmeasurable tree is a FAIL.
# Self-test / negative controls: scripts/tests/test_workspace_test_disposition_selftest.sh
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Resolved from this script's OWN location, with no env override: an override is
# settable by the party the guard constrains (CLAUDE.md #3312 — "the constrained party
# must not choose its own enforcer"). A self-test needing a different tree copies this
# script into that tree; it never redirects this variable.
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
CENSUS="$SCRIPT_DIR/workspace-test-disposition.txt"

# The CLOSED label set. An unrecognised label is a FAIL, not a pass: a permissive
# branch keyed on "not one of the bad values" would accept `EXECUTED?`, `Executed`, or
# a typo, which is a spelling check standing in for a state check.
VALID_LABELS="EXECUTED
PARTIAL
NOT-EXECUTED"

# The CLOSED class set, for the same reason and matched the same way.
VALID_CLASSES="silent
contradicts-doctrine
no-gap"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

[ -f "$CENSUS" ] || fail "the recorded census $CENSUS does not exist — this guard's subject is missing; refusing to pass vacuously"
[ -r "$CENSUS" ] || fail "the recorded census $CENSUS is not readable — unmeasurable is not a pass"

# --- 1. DERIVE the workspace member list from cargo. -------------------------
# `--no-deps` so only workspace members are reported. jq first, then python3, then
# failure — the same chain and the same direction as the gate's own metadata helpers. A
# single-parser guard is a false red on a host that has only the other one.
command -v cargo >/dev/null 2>&1 \
  || fail "cargo is not on PATH, so the workspace member list cannot be DERIVED. This guard's subject set comes from cargo metadata by design (a hand-resolved \`members\` glob would be a second implementation of cargo's resolution); an underived subject set is not a pass"
META=$(cargo metadata --no-deps --format-version 1 --manifest-path "$ROOT/Cargo.toml" 2>/dev/null) \
  || fail "\`cargo metadata --no-deps\` failed under $ROOT — the DERIVATION failed, not the census. An unmeasurable subject set is never a pass"
[ -n "$META" ] || fail "\`cargo metadata --no-deps\` produced no output — the DERIVATION failed"

if command -v jq >/dev/null 2>&1; then
  MEMBERS=$(printf '%s' "$META" | jq -r '.packages[].name' | sort -u) \
    || fail "jq could not extract package names from cargo metadata — the DERIVATION failed"
elif command -v python3 >/dev/null 2>&1; then
  MEMBERS=$(printf '%s' "$META" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for n in sorted({p["name"] for p in d.get("packages", [])}):
    print(n)
') || fail "python3 could not extract package names from cargo metadata — the DERIVATION failed"
else
  fail "neither jq nor python3 is available, so cargo metadata cannot be parsed — the DERIVATION failed; an unparseable subject set is not a pass"
fi
[ -n "$MEMBERS" ] || fail "cargo metadata named ZERO workspace members — the DERIVATION failed (this workspace has 18). A census over an empty subject set would report OK having measured nothing"

n_members=$(printf '%s\n' "$MEMBERS" | grep -c .)
case "$n_members" in ''|*[!0-9]*) fail "could not count the derived workspace members — unmeasurable is not a pass" ;; esac
[ "$n_members" -gt 0 ] || fail "counted 0 derived workspace members — refusing to pass vacuously"

# --- 2. PARSE the census. Every non-blank, non-comment line must be a well-formed
# ---    TAB-separated triple with a label from the closed set.
RECORDED=""
n_records=0
lineno=0
while IFS= read -r line || [ -n "$line" ]; do
  lineno=$((lineno + 1))
  case "$line" in
    ''|'#'*) continue ;;
  esac
  # Field-count first: a line with the wrong shape must be named as malformed rather
  # than silently yielding an empty package or an empty label.
  nf=$(printf '%s' "$line" | awk -F'\t' '{print NF}')
  [ "$nf" -eq 4 ] || fail "$CENSUS line $lineno is not a TAB-separated record (<package> TAB <LABEL> TAB <CLASS> TAB <detail>; found $nf field(s)): $line"
  pkg=$(printf '%s' "$line" | awk -F'\t' '{print $1}')
  label=$(printf '%s' "$line" | awk -F'\t' '{print $2}')
  class=$(printf '%s' "$line" | awk -F'\t' '{print $3}')
  detail=$(printf '%s' "$line" | awk -F'\t' '{print $4}')
  [ -n "$pkg" ]   || fail "$CENSUS line $lineno has an empty package name: $line"
  [ -n "$label" ] || fail "$CENSUS line $lineno has an empty label: $line"
  [ -n "$class" ] || fail "$CENSUS line $lineno has an empty class: $line"
  # A record with no detail is a label with no account behind it, which is the
  # documentation half of this guard's whole purpose.
  [ -n "$detail" ] || fail "$CENSUS line $lineno records '$pkg' as $label with NO detail. EXECUTED must NAME the gate component; PARTIAL must name the component AND what is not run AND why; NOT-EXECUTED must say why (+ the tracking issue, when one is filed)"
  # CLOSED set, matched EXACTLY (grep -qxF): a prefix or case-insensitive match would
  # accept `EXECUTEDish` / `Executed`, i.e. check a spelling rather than a state.
  printf '%s\n' "$VALID_LABELS" | grep -qxF "$label" \
    || fail "$CENSUS line $lineno labels '$pkg' as '$label', which is not in the closed label set ($(printf '%s' "$VALID_LABELS" | tr '\n' '/' | sed 's:/$::')). An unrecognised label is a FAIL, never a pass"
  printf '%s\n' "$VALID_CLASSES" | grep -qxF "$class" \
    || fail "$CENSUS line $lineno classes '$pkg' as '$class', which is not in the closed class set ($(printf '%s' "$VALID_CLASSES" | tr '\n' '/' | sed 's:/$::')). An unrecognised class is a FAIL, never a pass"
  # The one CROSS-FIELD property this guard can check without modelling the gate. Both
  # directions matter: `no-gap` on a PARTIAL/NOT-EXECUTED record is how an uncomfortable
  # record gets excused without relabelling it (the shape the visible-gap floor below
  # exists for, one field over), and a gap class on an EXECUTED record is a record that
  # contradicts itself.
  if [ "$label" = EXECUTED ]; then
    [ "$class" = no-gap ] || fail "$CENSUS line $lineno records '$pkg' as EXECUTED but classes it '$class'. EXECUTED means there is no gap to classify, so its class must be no-gap"
  else
    [ "$class" != no-gap ] || fail "$CENSUS line $lineno records '$pkg' as $label — a real gap — but classes it no-gap. Classify it 'silent' (no committed doctrine claims it is covered) or 'contradicts-doctrine' (doctrine says it is covered and it is not); no-gap is reserved for EXECUTED records"
  fi
  case "$RECORDED" in
    *"|$pkg|"*) fail "$CENSUS records '$pkg' more than once (line $lineno) — a package has exactly ONE disposition" ;;
  esac
  RECORDED="$RECORDED|$pkg|"
  n_records=$((n_records + 1))
done < "$CENSUS"

[ "$n_records" -gt 0 ] || fail "$CENSUS contains no records at all — this guard would then enforce nothing while reporting PASS; refusing to pass vacuously"
ok "census is well-formed ($n_records record(s), every label from the closed set, no duplicates)"

# --- 3. every DERIVED member must be recorded. This is the half that catches a NEW
# ---    crate arriving via the `members` globs with no statement of whether anything
# ---    runs its tests — the #3522 defect itself.
missing=""
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  case "$RECORDED" in
    *"|$pkg|"*) ;;
    *) missing="$missing $pkg" ;;
  esac
done <<< "$MEMBERS"
[ -z "$missing" ] || fail "workspace member(s)$missing exist in \`cargo metadata\` but are recorded in NO disposition (issue #3522). Add a record to $CENSUS: '<package>	EXECUTED	<gate component that runs its tests>', or PARTIAL / NOT-EXECUTED with the reason. Compiling a crate is not covering it: clippy --workspace builds every member on every full gate, so an unrecorded crate reads as covered while executing nothing"
ok "all $n_members derived workspace member(s) carry a recorded disposition"

# --- 4. every RECORDED package must actually EXIST as a member. Without this, a
# ---    renamed or removed crate would leave a stale record that the count check
# ---    below could be satisfied by a compensating addition.
stale=""
recorded_list=$(printf '%s' "$RECORDED" | tr '|' '\n' | grep . | sort -u)
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  printf '%s\n' "$MEMBERS" | grep -qxF "$pkg" || stale="$stale $pkg"
done <<< "$recorded_list"
[ -z "$stale" ] || fail "$CENSUS records package(s)$stale that \`cargo metadata\` does not name as a workspace member — they were renamed or removed; update the census (issue #3522)"
ok "every recorded package is a current workspace member"

[ "$n_records" -eq "$n_members" ] \
  || fail "the census holds $n_records record(s) but cargo names $n_members workspace member(s) — the two halves above passed individually, so this can only mean a counting fault; unmeasurable is not a pass"

# --- 5. affirmative floor on the thing this census EXISTS to make visible: the
# ---    unexecuted set. If every record were EXECUTED the census would be enforcing
# ---    nothing interesting — and, far more likely, someone would have "fixed" the
# ---    uncomfortable records by relabelling them. This is deliberately a FLOOR of one
# ---    and not a fixed number: closing a real gap must never red the guard.
n_gap=$(grep -cE '^[^#[:space:]]+	(NOT-EXECUTED|PARTIAL)	' "$CENSUS" || true)
case "$n_gap" in ''|*[!0-9]*) fail "could not count the NOT-EXECUTED/PARTIAL records — unmeasurable is not a pass" ;; esac
if [ "$n_gap" -eq 0 ]; then
  fail "$CENSUS records ZERO NOT-EXECUTED/PARTIAL packages. Either every workspace member's tests are now fully executed by the gate — in which case delete this floor DELIBERATELY, in a reviewed diff that says so — or an uncomfortable record was relabelled. The visible unexecuted set is this census's entire purpose"
fi
ok "$n_gap of $n_members member(s) are recorded as PARTIAL or NOT-EXECUTED — the gap this census exists to keep visible is stated, not hidden"

# --- 6. AFFIRMATIVE class census. Not a threshold — a REPORT, so a pasted PASS states
# ---    how many gaps are recorded as contradicting committed doctrine rather than
# ---    leaving that count to be rediscovered. `0 RECOGNISED` (never a bare 0), because
# ---    a bare zero in a gate log reads as a verified all-clear from a scan that only
# ---    checks what was RECORDED.
n_doctrine=$(grep -cE '^[^#[:space:]]+	[A-Z-]+	contradicts-doctrine	' "$CENSUS" || true)
n_silent=$(grep -cE '^[^#[:space:]]+	[A-Z-]+	silent	' "$CENSUS" || true)
# Checked SEPARATELY, never as a concatenation: `"$a$b"` is still numeric when one of
# the two is EMPTY, so a failed count would slip through as measured — the fail-open
# shape this file's own header warns about, one level down.
case "$n_doctrine" in ''|*[!0-9]*) fail "could not count the contradicts-doctrine records — unmeasurable is not a pass" ;; esac
case "$n_silent"   in ''|*[!0-9]*) fail "could not count the silent records — unmeasurable is not a pass" ;; esac
ok "gap classes: $n_doctrine RECOGNISED as contradicts-doctrine, $n_silent RECOGNISED as silent (RECORDED classifications, not verified ones — see this script's header)"

echo "PASS: workspace test-execution disposition census is complete and labeled ($n_members members, $n_records records)"
exit 0
