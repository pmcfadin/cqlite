#!/usr/bin/env bash
# Allocator confinement guard (issue #3997, requirements R4.1 and R5.1).
#
# A STATIC assertion over committed source and manifests. No cargo, no build, no
# network, no datasets, no python3 — shell, `git` and `grep` only — so it ALWAYS
# runs and NEVER SKIPs. It is the structural half of the mechanism: its sibling
# `test_flight_allocator_link.sh` proves the right thing IS linked into the
# binary; this one proves nothing else ever gets it.
#
# THREE PROPERTIES:
#
#   1. (R4.1) `#[global_allocator]` occurs in EXACTLY ONE non-test production
#      file in the workspace — `cqlite-flight/src/main.rs` — and that occurrence
#      is guarded by the `jemalloc` feature. Every other occurrence in any `*.rs`
#      file must fall into a RECOGNISED legitimate class. A `src/lib.rs` or
#      `cqlite-core` occurrence outside `cfg(test)` FAILs.
#   2. (R5.1) No `Cargo.toml` outside cqlite-flight (and the workspace root, whose
#      `[workspace.dependencies]` declarations are inert until a member opts in)
#      names `tikv-jemallocator`. R5.1 names `bindings/`, `cqlite-core/` and
#      `cqlite-cli/`; this is deliberately stricter and covers all three.
#   3. (R5.1) Every dependent of `cqlite-flight` links its LIBRARY target, and the
#      set of dependents is exactly the RECORDED one. An ordinary Cargo dependency
#      always links the library target; the only way to depend on a BIN target is
#      the `artifact = "bin"` (bindeps) syntax, so that is what is refused.
#
# WHY IT MATTERS. A `#[global_allocator]` is process-wide, and the whole design of
# #3997 rests on `main.rs` being compiled into the BIN target only: that is what
# keeps the allocator out of the library every embedder, binding and integration
# test links, and out of the memory ratchets that install their OWN allocator in
# their OWN test binaries. Nothing about that is enforced by the compiler — a
# later "move it to lib.rs for convenience" would build fine and silently impose
# jemalloc on every consumer of the library. This guard is that enforcement.
#
# REFUSE, NEVER GUESS. Every occurrence must be classified into a named class; an
# unrecognised shape is a NAMED FAIL, never a skipped line. Same for the manifest
# scan: a dependency declaration in a shape this guard does not recognise FAILs
# with an instruction to extend the guard, because "the scan found nothing it
# understood" must never read the same as "the tree is clean".
#
# AFFIRMATIVE CENSUS. Every count is reported as `N ... RECOGNISED`, never a bare
# number, and each check refuses to pass on an EMPTY subject: a scan that measured
# nothing is not a clean scan.
#
# Every line is prefixed `FLIGHT-ALLOC-CONF: ` so this output cannot be mistaken
# for, or grepped as, a gate SUMMARY.
#
# Positive control (a guard nobody has watched fail is a guard nobody knows works):
# scripts/tests/test_flight_allocator_confinement_selftest.sh, which substitutes
# this artifact into scratch trees carrying each incident class.
set -uo pipefail

P='FLIGHT-ALLOC-CONF: '
say()  { printf '%s%s\n' "$P" "$*"; }
ok()   { say "ok $*"; }
pass() { say "verdict PASS — $*"; exit 0; }
fail() { say "verdict FAIL — $*"; exit 1; }

usage() {
  cat <<'EOF'
Usage: test_flight_allocator_confinement.sh [--help]

Allocator confinement guard (issue #3997, R4.1/R5.1). A static assertion over
committed source and manifests:

  * `#[global_allocator]` occurs in exactly one non-test production file,
    cqlite-flight/src/main.rs, guarded by the `jemalloc` feature;
  * no Cargo.toml outside cqlite-flight (and the workspace root) names
    tikv-jemallocator;
  * every dependent of cqlite-flight links its LIBRARY target, and the dependent
    set is the recorded one.

No options. Needs only a shell, git and grep — it never SKIPs. Exit 0 = PASS,
1 = FAIL, 2 = usage error.
EOF
}

case "${1-}" in
  --help|-h) usage; exit 0 ;;
  '') : ;;
  *) usage >&2; printf '%s\n' "${P}verdict FAIL — unrecognised argument: $1" >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || { usage >&2; printf '%s\n' "${P}verdict FAIL — too many arguments ($#)" >&2; exit 2; }

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Resolved from this script's OWN location, with no env override: an override is
# settable by the party the guard constrains (CLAUDE.md #3312 — "the constrained
# party must not choose its own enforcer"). A self-test needing a different tree
# COPIES this script into that tree.
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

command -v git  >/dev/null 2>&1 || fail "no \`git\` on PATH — this guard enumerates TRACKED files only (an untracked scratch file or a target/ artifact must not be able to satisfy or break it), so git is a hard prerequisite, not an optional accelerator"
command -v grep >/dev/null 2>&1 || fail "no \`grep\` on PATH"
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "$ROOT is not a git work tree — cannot enumerate tracked files; refusing to fall back to a filesystem walk, which would read target/ artifacts and untracked scratch files as if they were committed source"

# The RECORDED site of the one production allocator. Hard-coded on purpose: one
# visible location, inside the diff a reviewer already reads. Moving it is a
# deliberate, reviewable act — and per the design (`openspec/changes/flight-jemalloc/design.md`)
# it may not move at all, because only `main.rs` is bin-target-exclusive.
PRODUCTION_SITE='cqlite-flight/src/main.rs'
# The feature the one production site must be guarded by.
PRODUCTION_FEATURE='feature = "jemalloc"'
# Manifests permitted to name the allocator crate. cqlite-flight is the owner; the
# workspace root is allowed because a `[workspace.dependencies]` entry declares
# nothing for any member until that member writes `workspace = true` — refusing it
# would red a legitimate future refactor, and a guard that reds on correct input is
# the guard agents learn to waive (CLAUDE.md).
ALLOCATOR_CRATE='tikv-jemallocator'
ALLOCATOR_MANIFESTS='Cargo.toml
cqlite-flight/Cargo.toml'
# Every manifest permitted to declare a dependency on cqlite-flight.
#   tools/flight-loadgen  the load generator, which drives the Flight client.
#   cqlite-flight itself  the self-referential DEV-dependency that turns on
#                         `test-util` for the examples/ and tests/ targets.
# Both link the LIBRARY target, which is exactly the point: neither can inherit a
# bin-target allocator.
FLIGHT_DEPENDENTS='cqlite-flight/Cargo.toml
tools/flight-loadgen/Cargo.toml'

# `grep -c` exits 1 for a count of ZERO while still PRINTING "0", so its status is
# deliberately not used as a signal — the printed count is the datum, and the
# non-emptiness floors below are what reject a vacuous scan. This guards only the
# thing that would otherwise slip through: a value that is not a number at all,
# which would reach `[ "$n" -gt 0 ]` as a bash syntax error rather than a named
# cause. Returns non-zero rather than calling fail(), because fail() ends in
# `exit`, and an `exit` inside a command substitution leaves only the SUBSHELL.
count_lines() {
  local n
  n=$(printf '%s\n' "$1" | grep -c .)
  case "$n" in ''|*[!0-9]*) return 1 ;; esac
  printf '%s\n' "$n"
}

# Strip a line's comment tail and surrounding whitespace, for the cfg scan below.
# A `//`-comment can legitimately CONTAIN the text `#[cfg(` (this repo's own
# allocator sites are documented in prose directly above them), so counting cfg
# attributes without stripping comments would read documentation as code.
strip_code() {
  printf '%s\n' "$1" | sed -e 's://.*::' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# =============================================================================
# 1. (R4.1) every `global_allocator` occurrence is classified
# =============================================================================
#
# Two tiers, because the token appears both as an ATTRIBUTE and inside prose:
#
#   * a line whose code (comment tail stripped) STARTS with `#[global_allocator]`
#     is an INSTALL SITE and is classified by location and cfg guard;
#   * a line where the token survives only inside a comment is a MENTION and
#     installs nothing.
#
# Anything else — the token in code, not at the start of an attribute — is a
# NAMED FAIL. It is not a shape this workspace uses, and guessing at it is
# exactly what this guard exists not to do.
occurrences=$(git -C "$ROOT" grep -n 'global_allocator' -- '*.rs')
rc=$?
# rc 1 means "no match", which for THIS token means the subject is missing: the
# one production site and ~25 test-binary sites are committed source. Never a pass.
[ "$rc" -eq 0 ] || fail "\`git grep global_allocator -- '*.rs'\` exited $rc; expected matches (the production site plus the memory-ratchet test binaries are committed source). An empty or failed scan is not a clean scan."
n_occ=$(count_lines "$occurrences") || fail "cannot count the occurrence census — unmeasurable is not a pass"
[ "$n_occ" -gt 0 ] || fail "zero \`global_allocator\` occurrences found — refusing to pass vacuously over an empty subject"

n_attr=0; n_mention=0; n_testdir=0; n_cfgtest=0; n_production=0
production_sites=''

# Classify ONE install site's cfg guard. Walks UP from the attribute, collecting
# attribute lines and skipping pure comment/blank lines, and stops at anything
# else. Prints the joined attribute text.
#
# A multi-line `#[cfg(all(\n ... \n))]` stops the walk early and yields no cfg
# text, so the caller FAILs — with a cause that says the shape was not recognised
# rather than claiming the site is unguarded. Both outcomes are non-passing, so
# the distinction is only in the diagnostic; it exists because "extend the guard"
# and "you left an allocator ungated" send a reader to different places.
collect_attrs() {
  local file="$1" line="$2" i=0 n code out=''
  n=$((line - 1))
  while [ "$n" -ge 1 ] && [ "$i" -lt 30 ]; do
    code=$(strip_code "$(sed -n "${n}p" "$file")")
    if [ -z "$code" ]; then n=$((n - 1)); i=$((i + 1)); continue; fi
    case "$code" in
      '#['*|'#!['*) out="$code $out"; n=$((n - 1)); i=$((i + 1)) ;;
      *) break ;;
    esac
  done
  printf '%s\n' "$out"
}

while IFS= read -r occ; do
  [ -n "$occ" ] || continue
  file=${occ%%:*}
  rest=${occ#*:}
  lineno=${rest%%:*}
  text=${rest#*:}
  case "$lineno" in ''|*[!0-9]*) fail "cannot parse a line number out of the census entry '$occ' — refusing to classify a location this guard cannot read" ;; esac

  code=$(strip_code "$text")
  case "$code" in
    '#[global_allocator]'*)
      n_attr=$((n_attr + 1))
      # --- class A: a test-binary or example install site. `tests/` and
      # --- `examples/` are cargo's own target directories: nothing there is
      # --- compiled into a library or a production binary, so an allocator in one
      # --- is confined by construction.
      case "$file" in
        */tests/*|tests/*|*/examples/*|examples/*)
          n_testdir=$((n_testdir + 1)); continue ;;
      esac

      attrs=$(collect_attrs "$ROOT/$file" "$lineno")
      n_cfg=$(printf '%s\n' "$attrs" | grep -o '#\[cfg(' | grep -c .)
      case "$n_cfg" in ''|*[!0-9]*) fail "$file:$lineno — cannot count the cfg attributes guarding this \`#[global_allocator]\`; unmeasurable is not a pass" ;; esac
      if [ "$n_cfg" -ne 1 ]; then
        fail "$file:$lineno — this \`#[global_allocator]\` is guarded by $n_cfg recognised \`#[cfg(...)]\` attribute(s); exactly 1 is required (issue #3997, R4.1). Either it is UNGATED (a process-wide allocator imposed on every build), or it is preceded by a shape this scan does not recognise — most likely a MULTI-LINE cfg attribute, which the walk stops at. If the latter, extend collect_attrs() in $(basename "$0"). Collected attribute text: '${attrs}'"
      fi

      # --- class B: `cfg(test)` in a crate's own sources. Compiled into that
      # --- crate's unit-test binary only, never into the library an embedder
      # --- links. This is what cqlite-core's CountingAllocator (#1883) and its
      # --- dhat-heap sibling (#1668) are.
      #
      # The `test` token is matched with word boundaries so `feature = "test-util"`
      # or an identifier ending in `test` cannot satisfy it.
      if printf '%s\n' "$attrs" | grep -qE '(^|[^A-Za-z0-9_])test([^A-Za-z0-9_]|$)'; then
        n_cfgtest=$((n_cfgtest + 1)); continue
      fi

      # --- class C: THE one production install site. Must be at the recorded
      # --- path AND guarded by the jemalloc feature. A production allocator
      # --- anywhere else — or here but ungated by the feature — is the incident
      # --- this guard exists for.
      if [ "$file" = "$PRODUCTION_SITE" ]; then
        printf '%s\n' "$attrs" | grep -qF "$PRODUCTION_FEATURE" \
          || fail "$file:$lineno — the production \`#[global_allocator]\` is NOT guarded by \`$PRODUCTION_FEATURE\` (issue #3997, R4.1). \`default = []\`, so an unguarded site would put jemalloc in every build, including one whose operator never asked for it. Collected attribute text: '${attrs}'"
        n_production=$((n_production + 1))
        production_sites="$production_sites$file:$lineno
"
        continue
      fi

      fail "$file:$lineno — a \`#[global_allocator]\` in PRODUCTION sources outside \`$PRODUCTION_SITE\`, and not under \`cfg(test)\` (issue #3997, R4.1). A global allocator is PROCESS-WIDE: in a library's sources it is imposed on every binary that links that library, including the Python/Node bindings and every embedder. The only sanctioned production site is $PRODUCTION_SITE, which rustc compiles into the cqlite-flight BIN target alone. Collected attribute text: '${attrs}'"
      ;;
    *'global_allocator'*)
      # The token survives comment-stripping, so it is in CODE but is not the head
      # of an attribute. Not a shape this workspace uses; refuse rather than guess.
      fail "$file:$lineno — \`global_allocator\` appears in CODE in a shape this guard does not recognise (it is not the start of a \`#[global_allocator]\` attribute). Refusing to guess whether it installs an allocator. Stripped code: '${code}'"
      ;;
    *)
      # Stripped to nothing / the token lived only in a comment: a MENTION. It
      # installs nothing. This repo's allocator sites are documented in prose
      # directly above them, so mentions are the majority of the census.
      n_mention=$((n_mention + 1))
      ;;
  esac
done <<EOF
$occurrences
EOF

# Affirmative floors. Without these, a census that classified nothing — or one
# whose subject vanished — would still reach PASS.
[ "$n_attr" -gt 0 ]     || fail "0 \`#[global_allocator]\` ATTRIBUTE sites recognised out of $n_occ occurrence(s) — every occurrence was read as prose, so the classifier verified nothing; refusing to pass vacuously"
[ "$n_testdir" -gt 0 ]  || fail "0 test/example allocator sites recognised — the memory-ratchet test binaries are committed source, so their absence means this scan is not seeing the tree it thinks it is"
[ "$n_production" -eq 1 ] || fail "$n_production production \`#[global_allocator]\` site(s) recognised; exactly 1 is required, at $PRODUCTION_SITE, guarded by \`$PRODUCTION_FEATURE\` (issue #3997, R4.1). Found: ${production_sites:-<none>}"
ok "$n_occ global_allocator occurrence(s) RECOGNISED: $n_attr install site(s) + $n_mention prose mention(s)"
ok "install sites RECOGNISED: 1 production ($PRODUCTION_SITE, guarded by \`$PRODUCTION_FEATURE\`) + $n_cfgtest cfg(test) + $n_testdir under tests/ or examples/"

# =============================================================================
# 2. (R5.1) no manifest outside the allowed set names the allocator crate
# =============================================================================
alloc_manifests=$(git -C "$ROOT" grep -l -F "$ALLOCATOR_CRATE" -- '*Cargo.toml')
rc=$?
# rc 1 = no manifest names it. That is NOT clean: cqlite-flight's manifest must,
# or the feature does not exist and this guard's whole subject is gone.
[ "$rc" -eq 0 ] || fail "no Cargo.toml names \`$ALLOCATOR_CRATE\` (git grep exited $rc) — the jemalloc feature's dependency has gone missing, so this check has no subject; refusing to pass vacuously"
n_alloc=$(count_lines "$alloc_manifests") || fail "cannot count the allocator-manifest census — unmeasurable is not a pass"
printf '%s\n' "$alloc_manifests" | grep -qxF 'cqlite-flight/Cargo.toml' \
  || fail "cqlite-flight/Cargo.toml does NOT name \`$ALLOCATOR_CRATE\`, but some other manifest does ($(printf '%s ' $alloc_manifests)) — the allocator has moved out of the crate that owns it"
while IFS= read -r m; do
  [ -n "$m" ] || continue
  printf '%s\n' "$ALLOCATOR_MANIFESTS" | grep -qxF "$m" \
    || fail "$m names \`$ALLOCATOR_CRATE\` (issue #3997, R5.1). Only cqlite-flight may depend on the allocator crate, and only the workspace root may DECLARE it in [workspace.dependencies] (inert until a member opts in). A library crate, a binding or the CLI naming it would impose a process-wide allocator on every embedder that links it."
done <<EOF
$alloc_manifests
EOF
ok "$n_alloc manifest(s) naming \`$ALLOCATOR_CRATE\` RECOGNISED, all inside the permitted set: $(printf '%s ' $alloc_manifests)"

# =============================================================================
# 3. (R5.1) every dependent of cqlite-flight links the LIBRARY target
# =============================================================================
#
# Manifest-static on purpose: no cargo, so this stays in the fast, always-runs
# tier. The TOML grammar bounds the scan — a dependency can only be declared by a
# KEY (`cqlite-flight = ...` / `cqlite-flight.<x> = ...`) or by a dependency TABLE
# HEADER (`[<...>dependencies.cqlite-flight]`). A mention inside a string value or
# a comment declares nothing, which is why those are not dependency shapes rather
# than being "assumed harmless".
flight_lines=$(git -C "$ROOT" grep -n -- 'cqlite-flight' -- '*Cargo.toml')
rc=$?
[ "$rc" -eq 0 ] || fail "\`git grep cqlite-flight -- '*Cargo.toml'\` exited $rc; expected matches (the crate declares its own name) — an empty or failed scan is not a clean scan"
n_dep_entries=0
seen_dependents=''
while IFS= read -r entry; do
  [ -n "$entry" ] || continue
  file=${entry%%:*}
  rest=${entry#*:}
  lineno=${rest%%:*}
  text=${rest#*:}
  # Trim only. A TOML comment is `#`-introduced and is rejected below by SHAPE;
  # no comment-tail stripping here, because a `#` inside a string value is not a
  # comment and stripping from it would mangle a real dependency line.
  code=$(printf '%s\n' "$text" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  # A TOML comment declares nothing. Tested FIRST so a comment mentioning a
  # dependency line cannot be read as one.
  case "$code" in '#'*) continue ;; esac

  is_dep=no
  case "$code" in
    'cqlite-flight'[[:space:]]*'='*|'cqlite-flight='*|'cqlite-flight.'*|'"cqlite-flight"'*'='*) is_dep=yes ;;
  esac
  case "$code" in
    '['*'dependencies.cqlite-flight]'|'['*'dependencies.cqlite-flight'[[:space:]]*']')
      fail "$file:$lineno declares the cqlite-flight dependency as a TABLE HEADER, a shape this guard does not parse (its body, where an \`artifact\` key would live, is on other lines). Refusing to certify it. Extend $(basename "$0") to walk the table body, or express the dependency inline." ;;
  esac
  # An inline `[dependencies]` table (legal TOML: `dependencies = { cqlite-flight
  # = { ... } }`) would put the key mid-line. Refuse rather than miss it.
  if [ "$is_dep" = no ]; then
    case "$code" in
      *'cqlite-flight'[[:space:]]*'='*|*'cqlite-flight='*)
        fail "$file:$lineno mentions cqlite-flight followed by \`=\` in a position this guard does not recognise as a top-level dependency key (an inline dependencies table?). Refusing to guess whether it declares a dependency. Line: '${code}'" ;;
    esac
    continue
  fi

  # A dependency KEY. Two things to assert.
  #   * `name = "cqlite-flight"` is the PACKAGE declaration, not a dependency —
  #     but it has key `name`, so it never reaches here.
  #   * `artifact`/`bin` on the entry is cargo's (unstable) way to depend on a
  #     BINARY target, which is the only route by which a dependent could inherit
  #     the bin's global allocator.
  case "$code" in
    *artifact*|*'bin ='*|*'bin='*)
      fail "$file:$lineno depends on cqlite-flight with an \`artifact\`/\`bin\` key (issue #3997, R5.1). That is a dependency on the BINARY target, which is where the jemalloc \`#[global_allocator]\` lives — the dependent would inherit a process-wide allocator. Depend on the LIBRARY target instead. Line: '${code}'" ;;
  esac
  n_dep_entries=$((n_dep_entries + 1))
  seen_dependents="$seen_dependents$file
"
  printf '%s\n' "$FLIGHT_DEPENDENTS" | grep -qxF "$file" \
    || fail "$file:$lineno declares a NEW dependency on cqlite-flight that is not in this guard's recorded dependent set (issue #3997, R5.1). Every dependent widens the blast radius of anything in that crate, so the set is reviewed rather than inferred: if this is intended, add $file to FLIGHT_DEPENDENTS in $(basename "$0") and state in the diff that it links the LIBRARY target."
done <<EOF
$flight_lines
EOF

n_expected=$(count_lines "$FLIGHT_DEPENDENTS") || fail "cannot count the recorded dependent set — unmeasurable is not a pass"
[ "$n_dep_entries" -gt 0 ] || fail "0 cqlite-flight dependency entries RECOGNISED, but the recorded set names $n_expected — the scan found none of them, so it verified nothing; refusing to pass vacuously"
# Every RECORDED dependent must still exist and still declare the dependency.
# Without this, deleting a dependent while leaving it recorded would keep the
# count satisfiable, and the set would decay into fiction.
while IFS= read -r m; do
  [ -n "$m" ] || continue
  [ -f "$ROOT/$m" ] || fail "$m is in this guard's recorded dependent set but does not exist on disk — remove it from FLIGHT_DEPENDENTS in $(basename "$0")"
  printf '%s\n' "$seen_dependents" | grep -qxF "$m" \
    || fail "$m is in this guard's recorded dependent set but declares no cqlite-flight dependency any more — remove it from FLIGHT_DEPENDENTS in $(basename "$0") so the recorded set stays a fact rather than a leftover"
done <<EOF
$FLIGHT_DEPENDENTS
EOF
ok "$n_dep_entries cqlite-flight dependency entr(y|ies) RECOGNISED across the $n_expected recorded dependent(s), 0 ARTIFACT/BIN DEPENDENCIES RECOGNISED — every dependent links the LIBRARY target"

pass "allocator confinement holds: one feature-gated production install site at $PRODUCTION_SITE, $n_cfgtest cfg(test) + $n_testdir test/example sites, \`$ALLOCATOR_CRATE\` named by no manifest outside the permitted set, and every cqlite-flight dependent on the library target (issue #3997, R4.1/R5.1)"
