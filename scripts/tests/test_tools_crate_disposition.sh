#!/usr/bin/env bash
# tools/ crate disposition census (issue #1716, epic #1688 finding AK5).
#
# ONE property: every crate under tools/ is EXPLICITLY classified into one of
# THREE dispositions, and every crate carrying unwired targets carries the README
# label that says so:
#
#   WIRED   — something runs it; nothing in it is orphaned.
#   UNWIRED — nothing runs it, and nothing depends on it either. Needs a label.
#   MIXED   — some targets are live and others are orphaned. Needs a label that
#             states BOTH halves, because calling such a crate simply "unwired"
#             is a FALSE census and invites deleting the live half.
#
# The three-way split exists because a two-way one was wrong (roborev job 75 on
# #1716): tools/format-validator's four BINARIES are invoked by nothing, while its
# LIBRARY is a path dependency of tests/format-compatibility — the gate's own
# `format-compat` component. Filing that crate under a crate-level "unwired" made
# a mixed-status crate satisfy a mutually-exclusive classification, so the census
# asserted something untrue about it.
#
# Why. Finding AK5 was that three tools/ crates were invoked by no workflow, no
# script and no doc, and nothing said so — so they read as live tooling for
# months. #1716 labeled them. Without this guard that labeling silently decays:
# the root manifest's `members` globs `tools/*`, so a NEW crate joins the
# workspace with no statement of whether anything runs it, and a deleted README
# is invisible. Both are caught here, at zero cost, with no false positives:
# the rule is a list, not an inference.
#
# TWO HALVES, and the split matters — one is DERIVED, the other RECORDED:
#
#   "something DEPENDS on it" is MECHANICALLY CHECKABLE from the manifests, so it
#   is DERIVED (via `cargo tree --workspace --invert`, cargo as its own authority)
#   and CROSS-CHECKED against the recorded disposition. UNWIRED must have ZERO
#   workspace dependents and MIXED must have AT LEAST ONE — so neither category
#   can be asserted falsely, in either direction.
#
#   "something RUNS it" is NOT mechanically checkable — invocations live in
#   workflows, scripts and prose — so it is RECORDED by a human and reviewed in
#   the diff. It is deliberately NOT grep-inferred: a grep for invocations gets
#   both directions wrong (tools/format-validator is referenced twice under
#   scripts/ purely as a PATH FIXTURE that runs nothing, and tools/ws0-corpus-gen
#   is wired via a binary name that differs from its package name), and a guard
#   whose FAIL an agent learns to waive is worse than no guard (CLAUDE.md).
#
# This split is the fix for roborev job 78: the MIXED label had been verified by
# grepping the README for the generic word "WIRED", which a README could satisfy
# while saying the opposite ("previously WIRED, now entirely NOT CI-wired").
# Verifying a claim by pattern-matching prose is unbounded — the prose author
# chooses the wording — so the load-bearing check moved OFF the prose and onto the
# manifests, and the prose requirement is now to name the crate's ACTUAL,
# DERIVED dependents rather than any fixed word.
#
# FAILS CLOSED: an unclassifiable or unmeasurable tree is a FAIL, never a pass.
# Self-test / negative controls: scripts/tests/test_tools_crate_disposition_selftest.sh
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Resolved from this script's OWN location, with no env override: an override is
# settable by the party the guard constrains (CLAUDE.md #3312 — "the constrained
# party must not choose its own enforcer"). A self-test needing a different tree
# copies this script into that tree; it never redirects this variable.
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

# --- The recorded disposition. Hard-coded ON PURPOSE: one visible location,
# --- inside the diff a reviewer already reads. Moving a crate between these
# --- lists is a deliberate, reviewable act.

# Invoked by at least one CI workflow or script. No README required by this guard.
WIRED_TOOLS="cassandra-parity
flight-loadgen
sstabledump-validator
ws0-corpus-gen"

# Nothing runs these AND nothing depends on them (census: issue #1716). Each MUST
# carry a README.md containing LABEL_MARKER.
UNWIRED_TOOLS="cqlite-validator
memory-safety-runner"

# Some targets live, some orphaned. Each MUST carry a README.md containing BOTH
# LABEL_MARKER (for the orphaned targets) and the names of its DERIVED dependents (the live
# ones, named below from the DERIVED dependent set), so the label cannot describe
# the crate as wholly unwired.
#   format-validator: 4 binaries orphaned; LIBRARY wired into
#   tests/format-compatibility (gate component `format-compat`), and also used as
#   a fixture by scripts/tests/test_agent_gate_summary.sh (owners resolution) and
#   asserted on by xtask/src/oom_audit/scope.rs. NEVER workspace-`exclude` it.
MIXED_TOOLS="format-validator"

# For each MIXED crate, WHAT its live half is. Two kinds exist because "live" has
# two sources, and only one of them is derivable (roborev job 83):
#
#   dependency      the live half is a LIBRARY some workspace package depends on.
#                   DERIVED from cargo; the README must name the real dependents.
#   invoked:a,b,... the live half is BINARIES that CI invokes. Invocation is NOT
#                   mechanically checkable, so the target names are RECORDED — but
#                   each is still cross-checked against the crate's DECLARED
#                   [[bin]] targets, and the README must name each one.
#
# The second kind exists because requiring a workspace dependent for every MIXED
# crate rejected a legitimate shape: a multi-binary tool with one bin wired into CI
# and another orphaned has NO reverse cargo dependency at all, and the guard would
# have told its author to record it UNWIRED — which is false.
# Format: <crate>|<kind>. Every MIXED crate must appear here or the guard FAILs.
MIXED_LIVE="format-validator|dependency"

# The marker every README with orphaned targets must contain, so the label states
# the actual fact rather than merely existing as a file.
LABEL_MARKER="NOT CI-wired"
# A MIXED crate's README must additionally name each of its ACTUAL workspace
# dependents — the package names are DERIVED below, never hard-coded here, so this
# requirement cannot be satisfied by a generic word (roborev job 78).

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

# The recorded live-half KIND for a MIXED crate, from MIXED_LIVE. Returns non-zero
# when the crate has no entry, so an unrecorded MIXED crate fails closed rather
# than defaulting to either kind.
mixed_live_kind() {
  local crate="$1" line
  line=$(printf '%s\n' "$MIXED_LIVE" | grep "^$crate|" | head -1)
  [ -n "$line" ] || return 1
  printf '%s\n' "${line#*|}"
}

# The [[bin]] target names declared by tools/<dir>/Cargo.toml. Used to cross-check
# a RECORDED invoked target against something mechanical: a target CI supposedly
# runs must at least EXIST. Returns non-zero if the manifest cannot be read (an
# unreadable manifest is unmeasurable, not "no bins").
declared_bins() {
  # Two statements, not one: within a SINGLE `local`, bash creates every name
  # before assigning them, so a later word referencing an earlier one expands an
  # UNSET local and dies under `set -u` ("dir: unbound variable"). This only
  # surfaced in the scratch self-tests, because the real repo's one MIXED crate
  # takes the `dependency` branch and never calls this function.
  local dir="$1"
  local manifest="$ROOT/tools/$dir/Cargo.toml"
  [ -r "$manifest" ] || return 1
  awk '
    /^[[:space:]]*\[\[bin\]\]/ { inbin = 1; next }
    /^[[:space:]]*\[/            { inbin = 0 }
    inbin && /^[[:space:]]*name[[:space:]]*=/ {
      if (match($0, /"[^"]+"/)) print substr($0, RSTART + 1, RLENGTH - 2)
    }
  ' "$manifest"
}

# The PACKAGE name declared by tools/<dir>/Cargo.toml. The recorded lists and the
# README live under the DIRECTORY name, but cargo is queried by PACKAGE name, and
# the two are not the same thing — assuming they were made every scratch-tree
# self-test case report "cargo could not answer". All seven of this repo's tools/
# crates happen to match, which is exactly why the assumption would have stayed
# invisible here until someone added one that did not.
#
# Takes the FIRST `name =` line in the manifest: that is the `[package]` one, since
# `[package]` precedes any `[lib]`/`[[bin]]` table that also carries a `name`
# (tools/cassandra-parity has both). Fails closed if none is found.
package_name_of() {
  local dir="$1" name
  name=$(sed -n 's/^name[[:space:]]*=[[:space:]]*"\([^"]\{1,\}\)".*/\1/p' "$ROOT/tools/$dir/Cargo.toml" 2>/dev/null | head -1)
  [ -n "$name" ] || return 1
  printf '%s\n' "$name"
}

# Workspace packages that DIRECTLY depend on $1 (a PACKAGE name), derived from cargo's own
# resolution — never a hand-parse of Cargo.toml, and never a grep for prose.
#
# `--workspace` is REQUIRED: without it `cargo tree` operates on the DEFAULT
# member set, which in this workspace is the root package alone (see the
# `default-members` note in the root Cargo.toml), and every crate would appear to
# have zero dependents — a silent false negative that would let a MIXED crate be
# recorded UNWIRED. Measured: `cargo tree --invert format-validator` reports no
# dependents; adding `--workspace` reports format-compatibility-tests.
#
# `--all-features` and `--target all` are equally REQUIRED, and for the same class
# of reason (roborev job 79): `cargo tree` otherwise resolves only the DEFAULT
# feature set and the HOST target, so a workspace package depending on a tool crate
# behind an OPTIONAL FEATURE, or under a `[target.'cfg(...)'.dependencies]` table,
# is INVISIBLE — and that crate would then be recorded UNWIRED while something
# really does depend on it. Every narrowing of this query is a way for the census to
# be wrong in the permissive direction, so the query is widened to everything cargo
# can see.
#
# Direct dependents suffice: if nothing depends on a crate directly, nothing
# depends on it transitively either.
#
# Prints one package name per line (the subject itself excluded). Returns
# non-zero if cargo could not answer, so the caller can fail CLOSED.
# FAIL-CLOSED CONTRACT, and the two traps designed out of it.
#
# (1) Under `pipefail`, `grep -v` exits 1 when it matches NOTHING — the NORMAL,
#     EXPECTED result for an UNWIRED crate. Folding cargo's call and the filtering
#     into ONE pipeline therefore made "cargo could not answer" and "zero
#     dependents" return the SAME status, collapsing the legitimate case onto the
#     error branch. That is the two-valued-predicate trap: a probe that cannot tell
#     "no result" from "could not measure" must collapse them.
# (2) The obvious patch for (1) — `grep ... || true` — is WORSE, because `|| true`
#     also swallows grep's exit 2 (a REAL error) and any `sed`/`sort` failure,
#     converting an UNMEASURABLE dependency graph into "zero dependents". That
#     inverts the contract: a permissive answer derived from the ABSENCE of a
#     signal, which is the one shape CLAUDE.md says never to build (roborev job 80).
#
# Neither is special-cased. The SHAPE that created the dilemma is gone instead:
# cargo's status is captured ALONE, and the parse is ONE `awk` stage plus `sort` —
# NEITHER of which exits non-zero for empty output. So `pipefail` stays armed with
# no `|| true` anywhere, emptiness is DATA, and a genuine failure in any stage
# still propagates.
workspace_dependents() {
  local crate="$1" out rc parsed
  # `--locked --offline` keeps this inside the tooling-tests contract (fast, no
  # network): --offline forbids registry access and --locked refuses to REWRITE
  # Cargo.lock, so the guard cannot mutate the tree it is inspecting. Neither
  # changes the ANSWER — resolution comes from Cargo.lock plus the local cache
  # (verified: identical output with and without both flags). A cold cargo cache
  # makes this FAIL rather than silently reach the network, which is the correct
  # direction; the caller's message names the remedy.
  out=$(cd "$ROOT" && cargo tree --workspace --invert "$crate" --depth 1 \
          --all-features --target all --locked --offline 2>/dev/null)
  rc=$?
  [ "$rc" -eq 0 ] || return 1
  # cargo always echoes the subject itself as the tree root, so empty output here
  # means cargo answered nothing at all — a measurement failure, not "no deps".
  [ -n "$out" ] || return 1

  # Package name = the token immediately preceding " v<digit>", which skips both
  # the tree glyphs cargo prefixes and the trailing (/path). The SUBJECT itself is
  # dropped HERE, inside awk, rather than by a downstream `grep -v` — see the
  # fail-closed note above the function for why that grep had to go entirely.
  # The parse must PROVE it understood the output before an empty dependent set is
  # allowed to mean "no dependents" (roborev job 82). cargo ALWAYS prints the
  # subject as the tree root, so seeing the subject is positive evidence that the
  # pattern still matches cargo's format; NOT seeing it means the format changed or
  # the filter silently matched nothing — under which an UNWIRED crate with real
  # dependents would have passed. awk exits 3 for that case, distinct from a real
  # awk failure, and both are non-zero so both fail closed.
  parsed=$(printf '%s\n' "$out" | awk -v self="$crate" '
    match($0, /[A-Za-z0-9_-]+ v[0-9]/) {
      name = substr($0, RSTART, RLENGTH)
      sub(/ v[0-9]$/, "", name)
      if (name == self) { seen_self = 1; next }
      print name
    }
    END { if (!seen_self) exit 3 }' | sort -u)
  rc=$?
  [ "$rc" -eq 0 ] || return 1

  # Reaching here means cargo answered AND the parse succeeded — each validated
  # above — so success is asserted from those checks, not assumed. An EMPTY result
  # prints NOTHING, never a blank line (a caller's `[ -n "$deps" ]` would read one
  # blank line as a single dependent named "").
  [ -z "$parsed" ] || printf '%s\n' "$parsed"
  return 0
}

[ -d "$ROOT/tools" ] || fail "no tools/ directory under $ROOT — this guard's subject is missing; refusing to pass vacuously"

wired_sorted=$(printf '%s\n' "$WIRED_TOOLS" | grep . | sort -u)
unwired_sorted=$(printf '%s\n' "$UNWIRED_TOOLS" | grep . | sort -u)
mixed_sorted=$(printf '%s\n' "$MIXED_TOOLS" | grep . | sort -u)
# `grep -c` exits 1 for a count of ZERO while still PRINTING "0", so its status is
# deliberately not used as a signal here — the printed count is the datum, and the
# floors below are what reject zero. count_lines guards the one thing that would
# otherwise slip through: a value that is not a number at all (a grep that died
# printing nothing), which would reach `[ "$n" -gt 0 ]` as a bash syntax error
# rather than as a named cause.
# RETURNS non-zero rather than calling fail(): fail() ends with `exit`, and an
# `exit` inside a command substitution leaves only the SUBSHELL — the parent would
# carry on with an empty value and hit `[ "" -gt 0 ]` as a bash syntax error
# instead of a named cause. So the status is propagated and checked at each call
# site with `|| fail`.
count_lines() {
  local n
  n=$(printf '%s\n' "$1" | grep -c .)
  case "$n" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$n"
}
n_wired=$(count_lines "$wired_sorted")     || fail "cannot count WIRED_TOOLS (grep -c returned no usable number) — unmeasurable is not a pass"
n_unwired=$(count_lines "$unwired_sorted") || fail "cannot count UNWIRED_TOOLS (grep -c returned no usable number) — unmeasurable is not a pass"
n_mixed=$(count_lines "$mixed_sorted")     || fail "cannot count MIXED_TOOLS (grep -c returned no usable number) — unmeasurable is not a pass"
# Crates that carry orphaned targets: UNWIRED (wholly) + MIXED (partly). Both owe
# a label; only MIXED additionally owes a statement of its live half.
labelled_sorted=$(printf '%s\n%s\n' "$unwired_sorted" "$mixed_sorted" | grep . | sort -u)
n_labelled=$(count_lines "$labelled_sorted") || fail "cannot count the label-bearing crate set (grep -c returned no usable number) — unmeasurable is not a pass"

# --- affirmative-measurement floor: the label-bearing set must be non-empty, or
# --- the label loop below would enforce nothing while still reporting PASS.
[ "$n_labelled" -gt 0 ] || fail "UNWIRED_TOOLS and MIXED_TOOLS are both empty — this guard would then enforce no label at all; refusing to pass vacuously"
[ "$n_wired" -gt 0 ]    || fail "the recorded WIRED_TOOLS list is empty — refusing to pass vacuously"

# --- 1. the three lists must be pairwise DISJOINT. A crate in two of them would
# ---    satisfy the accounted-for loop below while making its label requirement
# ---    ambiguous — which is the exact defect the third category exists to fix
# ---    (roborev job 75), so it is asserted rather than assumed.
#
# `comm`'s exit status is CHECKED, not discarded. An unchecked `both=$(comm ...)`
# yields an empty string when comm FAILS, and "empty intersection" is precisely
# this check's PASS condition — so a comm failure would silently certify the lists
# as disjoint. Same shape as roborev jobs 78/80: never let a permissive verdict
# rest on the absence of output that a failure also produces.
check_disjoint() {
  local a="$1" b="$2" name_a="$3" name_b="$4" both rc
  both=$(comm -12 <(printf '%s\n' "$a") <(printf '%s\n' "$b"))
  rc=$?
  [ "$rc" -eq 0 ] || fail "cannot compare $name_a against $name_b (comm exited $rc) — an unmeasurable comparison is not a pass; a comm failure and an EMPTY intersection both produce no output, so this status must be checked"
  [ -z "$both" ] || fail "crate(s) recorded in BOTH $name_a and $name_b: $(printf '%s ' $both)- a crate has exactly ONE disposition"
}
check_disjoint "$wired_sorted"   "$unwired_sorted" WIRED_TOOLS   UNWIRED_TOOLS
check_disjoint "$wired_sorted"   "$mixed_sorted"   WIRED_TOOLS   MIXED_TOOLS
check_disjoint "$unwired_sorted" "$mixed_sorted"   UNWIRED_TOOLS MIXED_TOOLS
ok "recorded disposition is self-consistent ($n_wired wired, $n_unwired unwired, $n_mixed mixed; pairwise disjoint)"

# --- 2. every crate ON DISK under tools/ must appear in exactly one list. This is
# ---    the half that catches a NEW crate arriving via the `tools/*` members glob
# ---    with no statement of whether anything runs it. It reads the FILESYSTEM, so
# ---    a manifest glob that silently failed to match cannot hide one.
n_disk=0
for manifest in "$ROOT"/tools/*/Cargo.toml; do
  [ -f "$manifest" ] || continue
  n_disk=$((n_disk + 1))
  crate=$(basename "$(dirname "$manifest")")
  if printf '%s\n' "$wired_sorted"   | grep -qxF "$crate"; then continue; fi
  if printf '%s\n' "$unwired_sorted" | grep -qxF "$crate"; then continue; fi
  if printf '%s\n' "$mixed_sorted"   | grep -qxF "$crate"; then continue; fi
  fail "tools/$crate exists on disk but is in NONE of the three recorded lists (issue #1716). Classify it in $SCRIPT_DIR/$(basename "$0"): WIRED_TOOLS if a CI workflow or script runs it, UNWIRED_TOOLS if nothing runs it and nothing depends on it, or MIXED_TOOLS if only SOME of its targets are live. The latter two also need a README.md labeling it '$LABEL_MARKER'."
done
[ "$n_disk" -gt 0 ] || fail "found no tools/*/Cargo.toml on disk — the filesystem half of this guard measured nothing; refusing to pass vacuously"
[ "$n_disk" -eq $((n_wired + n_unwired + n_mixed)) ] || fail "the recorded lists name $((n_wired + n_unwired + n_mixed)) crate(s) but $n_disk exist on disk — a recorded crate was renamed or removed without updating $(basename "$0") (issue #1716)"
ok "all $n_disk tools/ crate(s) on disk are classified by the recorded disposition"

# --- 3. every recorded crate must actually EXIST. Without this, deleting a crate
# ---    while leaving it listed would keep the count check above satisfiable by a
# ---    compensating addition.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  [ -f "$ROOT/tools/$crate/Cargo.toml" ] \
    || fail "tools/$crate is recorded in $(basename "$0") but has no Cargo.toml on disk — remove it from the list if the crate is gone (issue #1716)"
done <<< "$wired_sorted
$unwired_sorted
$mixed_sorted"
ok "every recorded crate exists on disk"

# --- 4. the actual acceptance criterion of #1716: every crate carrying orphaned
# ---    targets carries a README that SAYS so. A present-but-silent README does
# ---    not label anything, so the marker is required, not just the file.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  readme="$ROOT/tools/$crate/README.md"
  [ -f "$readme" ] \
    || fail "tools/$crate carries orphaned targets but has no README.md — issue #1716 requires it be LABELED (what it is, that its unwired parts are not CI-wired, how to build/run it)"
  [ -r "$readme" ] \
    || fail "tools/$crate/README.md is not readable — cannot verify its label; unmeasurable is not a pass"
  grep -qF "$LABEL_MARKER" "$readme" \
    || fail "tools/$crate/README.md exists but does not contain '$LABEL_MARKER' — the README must STATE that no CI workflow or script invokes it (or its orphaned targets), not merely exist (issue #1716)"
done <<< "$labelled_sorted"
ok "all $n_labelled crate(s) with orphaned targets carry a README.md stating '$LABEL_MARKER'"

# --- 5. CROSS-CHECK the recorded disposition against the MANIFESTS. This is the
# ---    half that cannot be talked around: "something depends on it" is derived
# ---    from cargo's own resolution, so UNWIRED cannot be claimed for a crate the
# ---    workspace depends on, and MIXED cannot be claimed for one it does not.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  pkg=$(package_name_of "$crate") \
    || fail "cannot read the package name from tools/$crate/Cargo.toml — unmeasurable is not a pass"
  deps=$(workspace_dependents "$pkg") \
    || fail "cannot derive workspace dependents of tools/$crate (package '$pkg'; cargo tree --workspace --invert failed, or its output no longer contains the subject as the tree root) — unmeasurable is not a pass. If the cargo REGISTRY CACHE is cold this guard fails by design rather than reach the network: run \`cargo fetch --locked\` once. If cargo's tree FORMAT changed, fix the parser in $(basename "$0")."
  if [ -n "$deps" ]; then
    fail "tools/$crate is recorded UNWIRED (nothing runs it AND nothing depends on it) but these workspace package(s) DEPEND on it: $(printf '%s ' $deps)- that is a FALSE census. Move it to MIXED_TOOLS (and label its README accordingly), because deleting or workspace-excluding it would break them (issue #1716)."
  fi
done <<< "$unwired_sorted"
ok "every UNWIRED crate has zero workspace dependents (derived from cargo, not asserted)"

if [ "$n_mixed" -gt 0 ]; then
  while IFS= read -r crate; do
    [ -n "$crate" ] || continue
    readme="$ROOT/tools/$crate/README.md"
    kind=$(mixed_live_kind "$crate") \
      || fail "tools/$crate is recorded MIXED but has no entry in MIXED_LIVE — record WHAT its live half is ('$crate|dependency' if a workspace package depends on its library, or '$crate|invoked:<bin>[,<bin>...]' if CI runs some of its binaries). Refusing to guess (issue #1716)."
    case "$kind" in
      dependency)
        # --- DERIVED: cargo must actually report a dependent, and the README must
        # --- name each one.
        pkg=$(package_name_of "$crate") \
          || fail "cannot read the package name from tools/$crate/Cargo.toml — unmeasurable is not a pass"
        deps=$(workspace_dependents "$pkg") \
          || fail "cannot derive workspace dependents of tools/$crate (package '$pkg'; cargo tree --workspace --invert failed, or its output no longer contains the subject as the tree root) — unmeasurable is not a pass. If the cargo REGISTRY CACHE is cold this guard fails by design rather than reach the network: run \`cargo fetch --locked\` once. If cargo's tree FORMAT changed, fix the parser in $(basename "$0")."
        [ -n "$deps" ] \
          || fail "tools/$crate is recorded MIXED via 'dependency' but NO workspace package depends on it. If its live half is instead a BINARY that CI runs, record '$crate|invoked:<bin>' in MIXED_LIVE. If nothing runs it and nothing depends on it, it belongs in UNWIRED_TOOLS (issue #1716)."
        while IFS= read -r dep; do
          [ -n "$dep" ] || continue
          grep -qF "$dep" "$readme" \
            || fail "tools/$crate is recorded MIXED and '$dep' really does depend on it, but its README.md never mentions '$dep'. A mixed crate's label must NAME the live half — otherwise it reads as wholly dead and invites deleting or workspace-excluding the part '$dep' needs (issue #1716)."
        done <<< "$deps"
        ok "tools/$crate (MIXED via dependency) names its real dependent(s) in the README: $(printf '%s ' $deps)"
        ;;
      invoked:*)
        # --- RECORDED, but still cross-checked: a target CI supposedly runs must at
        # --- least be DECLARED as a [[bin]], and the README must name it. This is
        # --- the shape that has no reverse cargo dependency at all — a multi-binary
        # --- tool with one bin wired and another orphaned (roborev job 83).
        targets="${kind#invoked:}"
        [ -n "$targets" ] \
          || fail "tools/$crate is recorded MIXED via 'invoked:' but names no target — record which binaries CI runs, e.g. '$crate|invoked:<bin>' (issue #1716)."
        bins=$(declared_bins "$crate") \
          || fail "cannot read tools/$crate/Cargo.toml to list its [[bin]] targets — unmeasurable is not a pass"
        n_checked=0
        old_ifs=$IFS; IFS=','
        for target in $targets; do
          IFS=$old_ifs
          [ -n "$target" ] || continue
          printf '%s\n' "$bins" | grep -qxF "$target" \
            || fail "tools/$crate records invoked target '$target' in MIXED_LIVE, but the crate declares no [[bin]] by that name (declared: $(printf '%s ' $bins)) — a target CI runs must at least exist (issue #1716)."
          grep -qF "$target" "$readme" \
            || fail "tools/$crate records invoked target '$target' as its live half, but its README.md never mentions '$target'. A mixed crate's label must NAME the live half, or the crate reads as wholly dead (issue #1716)."
          n_checked=$((n_checked + 1))
          IFS=','
        done
        IFS=$old_ifs
        [ "$n_checked" -gt 0 ] \
          || fail "tools/$crate is recorded MIXED via 'invoked:$targets' but no target was actually checked — refusing to pass vacuously"
        ok "tools/$crate (MIXED via $n_checked invoked target(s)) declares and documents each: $targets"
        ;;
      *)
        fail "tools/$crate has an unrecognised MIXED_LIVE kind '$kind' — the only accepted forms are 'dependency' and 'invoked:<bin>[,<bin>...]'. An unrecognised value is refused rather than treated as either (issue #1716)."
        ;;
    esac
  done <<< "$mixed_sorted"
fi

# --- 6. every MIXED_LIVE entry must name a crate that IS recorded MIXED, so a
# ---    stale entry cannot sit there implying coverage it does not provide.
while IFS= read -r entry; do
  [ -n "$entry" ] || continue
  entry_crate="${entry%%|*}"
  printf '%s\n' "$mixed_sorted" | grep -qxF "$entry_crate" \
    || fail "MIXED_LIVE has an entry for '$entry_crate', which is not in MIXED_TOOLS — remove the stale entry (issue #1716)"
done <<< "$(printf '%s\n' "$MIXED_LIVE" | grep .)"
ok "every MIXED_LIVE entry corresponds to a recorded MIXED crate"

echo "PASS: tools/ crate disposition census (#1716)"
