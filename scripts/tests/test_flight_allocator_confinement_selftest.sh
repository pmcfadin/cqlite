#!/usr/bin/env bash
# Self-test / positive control for scripts/tests/test_flight_allocator_confinement.sh
# (issue #3997, R4.1/R5.1).
#
# A guard is only worth its green if it is capable of red, and for a guard whose
# whole job is to red on a future refactor nobody has made yet, that is the ONLY
# way to know it works. Every case builds a scratch workspace that reproduces the
# real tree's shape in miniature, copies the guard into it at the same relative
# path, and asserts the verdict — and every red case must NAME the planted defect,
# because a bare non-zero exit is produced just as well by an unrelated abort.
#
# THE GUARD RESOLVES ITS ROOT FROM ITS OWN LOCATION, so there is deliberately no
# path or env seam to aim at a fixture (CLAUDE.md #3312: a case needing a different
# subject SUBSTITUTES THE ARTIFACT in its own scratch copy of the tree). A
# test-only seam would be one more thing a real invoker can set.
#
# TWO GREEN CONTROLS. Without them a guard hard-wired to refuse everything would
# satisfy every red case below and look fully tested.
#
# NO CARGO ANYWHERE, and no network: the guard is source-and-manifest static, so
# this suite is too — deterministic, offline, toolchain-independent. The scratch
# trees live OUTSIDE the repository (under mktemp) and are `git init`-ed, because
# the guard enumerates TRACKED files: a tree inside the repo would resolve to the
# repo's own git dir and the fixture would silently measure the real workspace.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GUARD="$SCRIPT_DIR/test_flight_allocator_confinement.sh"
GUARD_BASE=$(basename "$GUARD")
[ -f "$GUARD" ] || { echo "FAIL: guard under test not found at $GUARD" >&2; exit 1; }
command -v git >/dev/null 2>&1 || { echo "FAIL: this suite needs git (the guard enumerates tracked files)" >&2; exit 1; }

TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/flightallocconf.XXXXXX") || exit 1
trap 'rm -rf "$TMPROOT"' EXIT

fails=0
cases=0
pass_case() { echo "ok: $*"; cases=$((cases + 1)); }
fail_case() { echo "FAIL: $*" >&2; fails=$((fails + 1)); cases=$((cases + 1)); }

# make_tree <case> — lay down a MINIMAL but COMPLETE tree that the guard passes,
# so each case below can mutate exactly one thing. Echoes the tree root.
#
# The shape mirrors the real workspace in every respect the guard reads: the one
# feature-gated production install site, two `cfg(test)` sites in a library's own
# sources, a `tests/` site, the allocator crate named only by permitted manifests,
# and both recorded dependents declaring a plain (library) dependency.
make_tree() {
  # SEPARATE statements, deliberately: `local a="$1" b="$TMPROOT/$a"` expands ALL
  # of `local`'s arguments BEFORE the builtin assigns any of them, so the second
  # would read an unset `a` and die under `set -u`.
  local case_name="$1"
  local ws="$TMPROOT/$case_name"
  mkdir -p "$ws/scripts/tests" "$ws/cqlite-flight/src" "$ws/cqlite-flight/tests" \
           "$ws/cqlite-core/src" "$ws/tools/flight-loadgen/src" "$ws/bindings/python"

  cp "$GUARD" "$ws/scripts/tests/$GUARD_BASE"
  chmod +x "$ws/scripts/tests/$GUARD_BASE"

  printf '[workspace]\nmembers = [\n    "cqlite-flight",\n    "cqlite-core",\n]\n' > "$ws/Cargo.toml"

  # The one production install site, feature-gated, with prose ABOVE it that
  # itself contains the token — the real tree documents its allocator sites, and
  # a classifier that read documentation as code would mis-count here.
  cat > "$ws/cqlite-flight/src/main.rs" <<'RS'
//! A `#[global_allocator]` is process-wide; this prose must count as a MENTION.
#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {}
RS
  printf '[package]\nname = "cqlite-flight"\n\n[dependencies]\ntikv-jemallocator = { version = "0.6", optional = true }\n\n[features]\ndefault = []\njemalloc = ["dep:tikv-jemallocator"]\n\n[dev-dependencies]\ncqlite-flight = { path = ".", features = ["test-util"] }\n' \
    > "$ws/cqlite-flight/Cargo.toml"

  # A test-binary install site (the memory-ratchet shape).
  printf '#[global_allocator]\nstatic A: dhat::Alloc = dhat::Alloc;\n' > "$ws/cqlite-flight/tests/mem_budget.rs"

  # Two cfg(test) sites in a library's own sources.
  cat > "$ws/cqlite-core/src/lib.rs" <<'RS'
// only one `#[global_allocator]` per binary — a MENTION.
#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]
#[global_allocator]
static TEST_ALLOC: Counting = Counting;

#[cfg(all(test, feature = "dhat-heap"))]
#[global_allocator]
static DHAT_TEST_ALLOC: dhat::Alloc = dhat::Alloc;
RS
  printf '[package]\nname = "cqlite-core"\n' > "$ws/cqlite-core/Cargo.toml"

  printf '[package]\nname = "flight-loadgen"\ndescription = "load generator for cqlite-flight"\n\n[dependencies]\ncqlite-flight = { path = "../../cqlite-flight", features = ["test-util"] }\n' \
    > "$ws/tools/flight-loadgen/Cargo.toml"
  printf 'fn main() {}\n' > "$ws/tools/flight-loadgen/src/main.rs"
  printf '[package]\nname = "cqlite-py"\n' > "$ws/bindings/python/Cargo.toml"

  git -C "$ws" init -q >/dev/null 2>&1
  git -C "$ws" add -A >/dev/null 2>&1
  printf '%s\n' "$ws"
}

# run_case <label> <tree> <expect: pass|fail> [needle...]
#   A red case must NAME the planted defect: `needle` is grepped (fixed-string)
#   against the guard's output, so an unrelated abort cannot satisfy it.
run_case() {
  local label="$1" ws="$2" expect="$3"; shift 3
  local out rc needle
  out=$(bash "$ws/scripts/tests/$GUARD_BASE" 2>&1); rc=$?
  case "$expect" in
    pass)
      if [ "$rc" -ne 0 ]; then
        fail_case "$label: expected PASS, guard exited $rc. Output:
$out"
        return
      fi
      printf '%s\n' "$out" | grep -q '^FLIGHT-ALLOC-CONF: verdict PASS' \
        || { fail_case "$label: exited 0 but printed no PASS verdict line. Output:
$out"; return; }
      ;;
    fail)
      if [ "$rc" -eq 0 ]; then
        fail_case "$label: expected FAIL, guard exited 0 (a FALSE PASS on a planted defect). Output:
$out"
        return
      fi
      printf '%s\n' "$out" | grep -q '^FLIGHT-ALLOC-CONF: verdict FAIL' \
        || { fail_case "$label: exited $rc but printed no FAIL verdict line. Output:
$out"; return; }
      ;;
    *) fail_case "$label: internal — unknown expectation '$expect'"; return ;;
  esac
  for needle in "$@"; do
    printf '%s\n' "$out" | grep -qF -- "$needle" \
      || { fail_case "$label: verdict was $expect but the output does not NAME '$needle' — a verdict that does not identify its subject is not evidence. Output:
$out"; return; }
  done
  pass_case "$label"
}

# ---------------------------------------------------------------- green controls
ws=$(make_tree g1_pristine)
run_case "G1 pristine tree passes" "$ws" pass \
  'cqlite-flight/src/main.rs' '2 cfg(test)'

# A tree where the workspace ROOT manifest also declares the allocator crate in
# [workspace.dependencies]. That is inert until a member opts in, so it must PASS —
# the second green control, and the one that pins the guard does not simply refuse
# every mention of the crate.
ws=$(make_tree g2_workspace_dep)
printf '\n[workspace.dependencies]\ntikv-jemallocator = { version = "0.6" }\n' >> "$ws/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "G2 [workspace.dependencies] declaration is inert and passes" "$ws" pass \
  '2 manifest(s) naming `tikv-jemallocator` RECOGNISED'

# ------------------------------------------------------------------- red: R4.1
# THE incident class: someone moves the allocator into a library's sources "for
# convenience". It builds fine and imposes jemalloc on every embedder.
ws=$(make_tree r1_allocator_in_lib)
cat >> "$ws/cqlite-core/src/lib.rs" <<'RS'

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ESCAPED: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
RS
git -C "$ws" add -A >/dev/null 2>&1
run_case "R1 a feature-gated allocator in cqlite-core/src/lib.rs is rejected" "$ws" fail \
  'cqlite-core/src/lib.rs' 'PRODUCTION sources outside'

# An UNGATED allocator in a library's sources — the same class with no cfg at all,
# which must not be excused by the cfg-count branch.
ws=$(make_tree r2_ungated_in_lib)
printf '\n#[global_allocator]\nstatic ESCAPED: Sys = Sys;\n' >> "$ws/cqlite-core/src/lib.rs"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R2 an UNGATED allocator in a library's sources is rejected" "$ws" fail \
  'cqlite-core/src/lib.rs' 'guarded by 0 recognised'

# The production site loses its feature gate: `default = []`, so this would put
# jemalloc in every build including one nobody asked for.
ws=$(make_tree r3_production_ungated)
cat > "$ws/cqlite-flight/src/main.rs" <<'RS'
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {}
RS
git -C "$ws" add -A >/dev/null 2>&1
run_case "R3 the production site without the jemalloc feature gate is rejected" "$ws" fail \
  'cqlite-flight/src/main.rs' 'NOT guarded by'

# A SECOND production install site at the recorded path's crate but another file.
ws=$(make_tree r4_second_production_site)
mkdir -p "$ws/cqlite-flight/src"
cat > "$ws/cqlite-flight/src/other.rs" <<'RS'
#[cfg(feature = "jemalloc")]
#[global_allocator]
static SECOND: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
RS
git -C "$ws" add -A >/dev/null 2>&1
run_case "R4 a second production install site in the same crate is rejected" "$ws" fail \
  'cqlite-flight/src/other.rs'

# The production site DISAPPEARS. The guard must red on an absent subject too,
# not merely on an extra one — otherwise deleting the mechanism would pass.
ws=$(make_tree r5_no_production_site)
printf 'fn main() {}\n' > "$ws/cqlite-flight/src/main.rs"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R5 an ABSENT production install site is rejected" "$ws" fail \
  '0 production'

# A multi-line cfg attribute: the walk stops at it, so the guard must refuse with
# the "extend the guard" cause rather than claim the site is unguarded.
ws=$(make_tree r6_multiline_cfg)
cat > "$ws/cqlite-flight/src/main.rs" <<'RS'
#[cfg(all(
    feature = "jemalloc",
    target_os = "linux"
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {}
RS
git -C "$ws" add -A >/dev/null 2>&1
run_case "R6 a MULTI-LINE cfg attribute is refused, not guessed at" "$ws" fail \
  'MULTI-LINE cfg attribute'

# `global_allocator` in code in a shape the classifier does not recognise.
ws=$(make_tree r7_unrecognised_code)
printf 'const X: &str = "global_allocator";\n' > "$ws/cqlite-core/src/odd.rs"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R7 the token in CODE in an unrecognised shape is refused" "$ws" fail \
  'does not recognise'

# NO occurrences at all: an empty subject must never read as clean.
ws=$(make_tree r8_empty_subject)
printf 'fn main() {}\n' > "$ws/cqlite-flight/src/main.rs"
rm -f "$ws/cqlite-flight/tests/mem_budget.rs"
printf '// nothing here\n' > "$ws/cqlite-core/src/lib.rs"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R8 a tree with ZERO occurrences is rejected, not passed vacuously" "$ws" fail \
  'not a clean scan'

# The test/example floor: install sites exist but NONE under tests/ or examples/,
# which means the scan is not seeing the tree it thinks it is.
ws=$(make_tree r9_no_test_sites)
rm -f "$ws/cqlite-flight/tests/mem_budget.rs"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R9 zero test/example install sites is rejected" "$ws" fail \
  '0 test/example allocator sites recognised'

# ------------------------------------------------------------------- red: R5.1
# A binding names the allocator crate — the leak R5 exists to prevent.
ws=$(make_tree r10_binding_names_allocator)
printf '[package]\nname = "cqlite-py"\n\n[dependencies]\ntikv-jemallocator = "0.6"\n' \
  > "$ws/bindings/python/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R10 a binding manifest naming tikv-jemallocator is rejected" "$ws" fail \
  'bindings/python/Cargo.toml' 'R5.1'

# cqlite-core names it.
ws=$(make_tree r11_core_names_allocator)
printf '[package]\nname = "cqlite-core"\n\n[dependencies]\ntikv-jemallocator = "0.6"\n' \
  > "$ws/cqlite-core/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R11 cqlite-core naming tikv-jemallocator is rejected" "$ws" fail \
  'cqlite-core/Cargo.toml'

# The allocator crate leaves cqlite-flight entirely: an absent subject again.
ws=$(make_tree r12_allocator_crate_gone)
printf '[package]\nname = "cqlite-flight"\n\n[features]\ndefault = []\n\n[dev-dependencies]\ncqlite-flight = { path = "." }\n' \
  > "$ws/cqlite-flight/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R12 the allocator dependency vanishing from cqlite-flight is rejected" "$ws" fail \
  'gone missing'

# A dependent asking for the BIN target — the one route by which a dependent could
# inherit the binary's global allocator.
ws=$(make_tree r13_artifact_bin_dep)
printf '[package]\nname = "flight-loadgen"\n\n[dependencies]\ncqlite-flight = { path = "../../cqlite-flight", artifact = "bin" }\n' \
  > "$ws/tools/flight-loadgen/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R13 an artifact=\"bin\" dependency on cqlite-flight is rejected" "$ws" fail \
  'BINARY target'

# A NEW, unrecorded dependent.
ws=$(make_tree r14_new_dependent)
mkdir -p "$ws/tools/newthing"
printf '[package]\nname = "newthing"\n\n[dependencies]\ncqlite-flight = { path = "../../cqlite-flight" }\n' \
  > "$ws/tools/newthing/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R14 a NEW unrecorded dependent of cqlite-flight is rejected" "$ws" fail \
  'tools/newthing/Cargo.toml' 'recorded dependent set'

# A RECORDED dependent that stops declaring the dependency: the recorded set must
# stay a fact rather than decay into a leftover.
ws=$(make_tree r15_recorded_dependent_stale)
printf '[package]\nname = "flight-loadgen"\n' > "$ws/tools/flight-loadgen/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R15 a recorded dependent that no longer declares the dependency is rejected" "$ws" fail \
  'tools/flight-loadgen/Cargo.toml'

# A dependency declared as a TABLE HEADER: a shape the guard does not parse, so it
# must refuse rather than certify a body it never read.
ws=$(make_tree r16_table_header_dep)
printf '[package]\nname = "flight-loadgen"\n\n[dependencies.cqlite-flight]\npath = "../../cqlite-flight"\n' \
  > "$ws/tools/flight-loadgen/Cargo.toml"
git -C "$ws" add -A >/dev/null 2>&1
run_case "R16 a TABLE-HEADER dependency declaration is refused, not certified" "$ws" fail \
  'TABLE HEADER'

# ------------------------------------------------------------- red: prerequisites
# Not a git work tree: the guard must refuse rather than fall back to a filesystem
# walk, which would read target/ artifacts and untracked files as committed source.
ws=$(make_tree r17_not_a_git_tree)
rm -rf "$ws/.git"
run_case "R17 a non-git tree is refused, never walked as a filesystem" "$ws" fail \
  'not a git work tree'

# ------------------------------------------------------------------------ usage
out=$(bash "$GUARD" --help 2>&1); rc=$?
if [ "$rc" -eq 0 ] && printf '%s\n' "$out" | grep -q 'Usage: test_flight_allocator_confinement.sh'; then
  pass_case "U1 --help exits 0 and prints usage"
else
  fail_case "U1 --help: rc=$rc output:
$out"
fi
out=$(bash "$GUARD" --nope 2>&1); rc=$?
if [ "$rc" -eq 2 ]; then
  pass_case "U2 an unrecognised argument exits 2"
else
  fail_case "U2 unrecognised argument: expected exit 2, got $rc. Output:
$out"
fi

# CASE FLOOR (#3544's lesson): a span-replacing edit can silently delete cases and
# leave a green tally over a shrunken suite. 21 cases: 2 green, 17 red, 2 usage.
FLOOR=21
[ "$cases" -ge "$FLOOR" ] || { echo "FAIL: only $cases case(s) ran; the suite declares a floor of $FLOOR — cases were deleted" >&2; fails=$((fails + 1)); }

echo "----"
if [ "$fails" -eq 0 ]; then
  echo "PASS: allocator confinement guard self-test — $cases case(s), 0 failure(s) (#3997)"
  exit 0
fi
echo "FAIL: allocator confinement guard self-test — $cases case(s), $fails failure(s) (#3997)" >&2
exit 1
