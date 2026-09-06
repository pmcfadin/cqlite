#!/usr/bin/env bash
# cqlite-flight linked-allocator guard (issue #3997, requirements R1.1, R1.2, R2.1).
#
# THE GATE-ENFORCING SURFACE for the jemalloc mechanism. It exists because the
# cargo-native test that expresses the same contract
# (`cqlite-flight/tests/issue_3997_allocator_surface.rs`) is executed by NO gate
# component: `flight-tests` runs cqlite-flight at `--lib --bins` only and prints a
# run-time census naming the ~42 integration `--test` targets it does not run
# (#3384). This script is registered in `tooling-tests`, so it does run — and it
# covers a property no cargo test can reach at all: what is actually LINKED INTO
# the binary.
#
# TWO ARMS, WHICH ARE EACH OTHER'S CONTROL. That is the whole design:
#
#   positive  `cargo build -p cqlite-flight --features jemalloc`
#             -> the binary MUST carry jemalloc symbols, and `--version` MUST say
#                `allocator: jemalloc`.
#   negative  `cargo build -p cqlite-flight` (default features; `default = []`)
#             -> the binary MUST carry NONE, and `--version` MUST say
#                `allocator: system`.
#
# A single arm proves nothing: a symbol matcher that matches everything satisfies
# the positive arm, and one that matches nothing satisfies the negative arm. Only
# the pair discriminates, and only the pair can catch the failure that matters —
# the feature being wired to a `#[global_allocator]` that is never installed, or
# `--version` reporting a string that disagrees with what was linked.
#
# WHY DEBUG, NOT RELEASE. Two reasons, both load-bearing. (1) Cost: the gate's
# other components already build this crate in debug, so the negative arm is warm
# and free, and the positive arm adds one cqlite-flight recompile plus jemalloc's
# vendored C source. (2) Correctness of the measurement: jemalloc is STATICALLY
# linked and its symbols are NOT dynamic (measured: `nm -D` reports 0 while plain
# `nm` reports the `_rjem_*` table), so the check must read the ordinary symbol
# table — which a stripped release binary would not have. Never "optimize" this to
# `--release`.
#
# FAIL-CLOSED, AND NON-VACUOUS:
#   * Off Linux           -> SKIP printing the ACTUAL platform. The dependency is
#                            declared under a `cfg(target_os = "linux")` target
#                            section, so there is nothing to link and nothing to
#                            assert; never a silent or vacuous PASS.
#   * Unmeasurable        -> SKIP **naming the cause**: no cargo, no symbol tool,
#                            no `cc`/`make` (jemalloc compiles C from source), no
#                            `timeout` accepting `-k` with which to BOUND the
#                            builds, or a symbol tool whose output this script
#                            cannot parse. An unmeasured check and a clean one must
#                            never read alike.
#   * A cargo build that FAILS -> FAIL naming the arm and the remedy. That is a
#                            broken tree, not an unmeasurable host.
#   * Affirmative zero    -> the negative arm reports
#                            `0 JEMALLOC SYMBOLS RECOGNISED`, never a bare `0`,
#                            and it is accepted ONLY when the symbol tool
#                            demonstrably produced a non-empty symbol table. A
#                            tool that printed nothing at all is UNMEASURED, not
#                            clean.
#
# Every line is prefixed `FLIGHT-ALLOC-LINK: ` so this output cannot be mistaken
# for, or grepped as, a gate SUMMARY. One verdict line per arm plus one terminal
# verdict line.
#
# Needs: cargo, a C toolchain, and `nm` or `readelf`. NO datasets, NO network
# beyond whatever cargo already has vendored/locked, NO Docker, NO python3.
set -uo pipefail

P='FLIGHT-ALLOC-LINK: '
say()  { printf '%s%s\n' "$P" "$*"; }
# Exit statuses: 0 = PASS or SKIP (an unmeasurable host must not red the gate),
# 1 = FAIL, 2 = usage. SKIP exits 0 deliberately and says so LOUDLY; the component
# treats a non-zero exit as FAIL.
pass() { say "verdict PASS — $*"; exit 0; }
skip() { say "verdict SKIP — $*"; say "verdict-detail NOTHING WAS MEASURED; this is not a clean result"; exit 0; }
fail() { say "verdict FAIL — $*"; exit 1; }

usage() {
  cat <<'EOF'
Usage: test_flight_allocator_link.sh [--help]

cqlite-flight linked-allocator guard (issue #3997, R1.1/R1.2/R2.1).

Builds `cqlite-flight` in DEBUG twice — once with `--features jemalloc`, once
with default features — and asserts that jemalloc symbols are present in the
first binary and absent from the second, and that each binary's `--version`
reports the matching `allocator:` line.

No options. Exit 0 = PASS or SKIP (SKIP always names its cause), 1 = FAIL,
2 = usage error. Off Linux it SKIPs, printing the platform it found.
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
# settable by the party the guard constrains (CLAUDE.md #3312).
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

# --- 0. platform. The `jemalloc` feature is inert off Linux BY CONSTRUCTION (the
# --- dependency lives under `[target.'cfg(target_os = "linux")'.dependencies]`),
# --- so there is no linked allocator to find and asserting its absence would
# --- prove nothing about the mechanism. Print what we actually found.
platform=$(uname -s 2>/dev/null)
[ -n "$platform" ] || skip "cannot read the platform (\`uname -s\` produced nothing) — refusing to guess"
if [ "$platform" != "Linux" ]; then
  skip "this guard is Linux-only and this host reports \`uname -s\` = '$platform'; the \`jemalloc\` feature is inert off Linux by construction (the dependency is declared under [target.'cfg(target_os = \"linux\")'.dependencies])"
fi
say "platform Linux ($(uname -m 2>/dev/null || echo 'unknown machine'))"

# --- 1. capabilities. Each absence is its own NAMED skip: "no cargo" and "no C
# --- compiler" send an operator to different remedies.
command -v cargo >/dev/null 2>&1 || skip "no \`cargo\` on PATH — cannot build either arm"
# jemalloc is built from vendored C source by `tikv-jemalloc-sys`' build script, so
# a working C toolchain is a hard prerequisite of the POSITIVE arm specifically.
command -v cc   >/dev/null 2>&1 || skip "no \`cc\` on PATH — tikv-jemalloc-sys compiles jemalloc's vendored C source and cannot build without a C compiler"
command -v make >/dev/null 2>&1 || skip "no \`make\` on PATH — tikv-jemalloc-sys drives jemalloc's own autotools/make build"

# The BOUND. An unbounded `cargo build` can hang the gate on a stalled registry
# fetch or a wedged linker, and a missing bounding capability must NOT inherit the
# permissive branch (CLAUDE.md): probe it by RUNNING it, because presence on PATH
# is not support for `-k` (a SIGTERM-only bound does not bound a child that ignores
# SIGTERM).
command -v timeout >/dev/null 2>&1 || skip "no \`timeout\` on PATH — refusing to run an UNBOUNDED cargo build inside a gate component"
timeout -k 1 5 true >/dev/null 2>&1 || skip "this host's \`timeout\` does not accept \`-k\` (probed by running it) — refusing to run a cargo build under a bound that cannot escalate to SIGKILL"

# The symbol reader. `nm` first, `readelf --syms` as the fallback; BOTH are read
# for the ORDINARY symbol table, never `-D`/`--dyn-syms`: measured, jemalloc's
# statically linked symbols do not appear in the dynamic table at all (`nm -D`
# reports 0 on a binary whose plain `nm` shows the whole `_rjem_*` table), so a
# dynamic-only reader would report a clean negative arm for BOTH builds — a false
# PASS in the exact direction this guard exists to prevent.
SYMTOOL=''
if command -v nm >/dev/null 2>&1; then
  SYMTOOL=nm
elif command -v readelf >/dev/null 2>&1; then
  SYMTOOL=readelf
fi
[ -n "$SYMTOOL" ] || skip "neither \`nm\` nor \`readelf\` is on PATH — cannot read the binary's symbol table"
say "capabilities cargo, cc, make, bounded-timeout, symbol-reader=$SYMTOOL"

# --- 2. where cargo puts the binary. Asked of CARGO, not assumed to be
# --- `$ROOT/target`: the answer honours CARGO_TARGET_DIR and any
# --- `build.target-dir` in a config file, and guessing would make this guard
# --- silently measure a stale artifact from a previous layout.
metadata=$(timeout -k 30 300 cargo metadata --format-version 1 --no-deps --manifest-path "$ROOT/Cargo.toml" 2>/dev/null)
rc=$?
[ "$rc" -eq 0 ] || fail "\`cargo metadata\` exited $rc — the workspace manifests do not read, which is a broken tree rather than an unmeasurable host. Remedy: fix the manifest, then re-run."
TARGET_DIR=$(printf '%s' "$metadata" | tr ',' '\n' | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' | head -1)
[ -n "$TARGET_DIR" ] || skip "could not read \`target_directory\` out of \`cargo metadata\`'s output — the reader did not recognise it, so nothing was measured (a cargo JSON shape change, or a target path holding an escaped character)"
BIN="$TARGET_DIR/debug/cqlite-flight"
say "target-dir $TARGET_DIR"

# Count jemalloc symbols and TOTAL symbols in one read, so a zero jemalloc count
# is always accompanied by the evidence that the tool produced a symbol table at
# all. Emits `<jemalloc-count> <total-count>` on stdout; returns non-zero when the
# read itself failed.
#
# Both accepted prefixes are asserted: `_rjem_` is what the Rust-vendored build
# produces (measured on this tree), `je_` is upstream jemalloc's own prefix, which
# a differently-configured build would emit. R1.1 names both.
read_symbols() {
  local path="$1" out rc_local jem total
  case "$SYMTOOL" in
    nm)      out=$(timeout -k 5 120 nm      -- "$path" 2>/dev/null); rc_local=$? ;;
    readelf) out=$(timeout -k 5 120 readelf --syms -W -- "$path" 2>/dev/null); rc_local=$? ;;
    *)       return 1 ;;
  esac
  [ "$rc_local" -eq 0 ] || return 1
  # `grep -c` exits 1 on a count of ZERO while still printing "0", so its status is
  # deliberately not used as a signal — the printed count is the datum.
  total=$(printf '%s\n' "$out" | grep -c .)
  jem=$(printf '%s\n' "$out" | grep -cE '(_rjem_|[^A-Za-z0-9_]je_|^je_)')
  case "$total$jem" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s %s\n' "$jem" "$total"
}

# `--version` under R2.1's grammar: EXACTLY one line equal to
# `allocator: jemalloc` or `allocator: system`, and its value as expected. Counted,
# because "at least one" is not the contract.
check_version_line() {
  local path="$1" expect="$2" out rc_local lines n
  out=$(timeout -k 5 60 "$path" --version 2>/dev/null)
  rc_local=$?
  [ "$rc_local" -eq 0 ] || {
    say "verdict-detail \`$path --version\` exited $rc_local (expected 0; the flag must short-circuit before required-argument validation, with no --data-dir supplied)"
    return 1
  }
  lines=$(printf '%s\n' "$out" | grep -E '^allocator: (jemalloc|system)$')
  n=$(printf '%s\n' "$lines" | grep -c .)
  case "$n" in ''|*[!0-9]*) say "verdict-detail cannot count the allocator lines in \`--version\` output"; return 1 ;; esac
  [ "$n" -eq 1 ] || {
    say "verdict-detail R2.1 requires EXACTLY ONE line matching '^allocator: (jemalloc|system)\$'; found $n. Full --version output follows."
    printf '%s\n' "$out" | sed "s/^/${P}    | /"
    return 1
  }
  [ "$lines" = "allocator: $expect" ] || {
    say "verdict-detail R2.1: expected 'allocator: $expect', got '$lines'"
    return 1
  }
  return 0
}

# One arm. `$1` = arm label, `$2` = extra cargo args (may be empty), `$3` =
# `present` | `absent`, `$4` = the expected `--version` allocator value.
run_arm() {
  local label="$1" extra="$2" expect_syms="$3" expect_alloc="$4" rc_local counts jem total
  say "arm $label: cargo build -p cqlite-flight ${extra:-<default features>}"
  # 25 min with a 30s SIGKILL escalation: the positive arm compiles jemalloc's
  # vendored C source cold, and the negative arm is the feature set the gate's other
  # components already built, so it is warm.
  # shellcheck disable=SC2086  # $extra is a deliberate word-split of our own literal flags
  timeout -k 30 1500 cargo build -p cqlite-flight $extra >/dev/null 2>&1
  rc_local=$?
  [ "$rc_local" -eq 0 ] || fail "arm $label: \`cargo build -p cqlite-flight ${extra:-(default features)}\` exited $rc_local. This is a BROKEN BUILD, not an unmeasurable host. Remedy: run that exact command by hand and fix what it reports."
  [ -f "$BIN" ] || fail "arm $label: the build reported success but $BIN does not exist — refusing to assert anything about a binary that is not there"

  counts=$(read_symbols "$BIN")
  rc_local=$?
  [ "$rc_local" -eq 0 ] || skip "arm $label: \`$SYMTOOL\` could not be read for $BIN (non-zero exit, timeout, or output this script cannot parse) — nothing was measured"
  jem=${counts%% *}
  total=${counts##* }
  # AFFIRMATIVE MEASUREMENT: a zero jemalloc count is only meaningful if the tool
  # produced a symbol table. Zero total symbols means the read told us nothing —
  # a stripped binary or a silently-empty tool — which must never read as clean.
  [ "$total" -gt 0 ] || skip "arm $label: \`$SYMTOOL\` produced ZERO symbol lines for $BIN, so a zero jemalloc count would be UNMEASURED rather than clean (a stripped binary, or a symbol reader that emitted nothing)"

  case "$expect_syms" in
    present)
      [ "$jem" -gt 0 ] || fail "arm $label (R1.1): expected jemalloc symbols in $BIN and found 0 JEMALLOC SYMBOLS RECOGNISED out of $total symbol lines read. The \`jemalloc\` feature is declared but nothing was linked — check that src/main.rs's #[global_allocator] cfg matches the feature name and that the dependency is not cfg'd out on this target."
      say "arm $label: $jem JEMALLOC SYMBOLS RECOGNISED (of $total symbol lines read)"
      ;;
    absent)
      [ "$jem" -eq 0 ] || fail "arm $label (R1.2): expected NO jemalloc symbols in the default-features binary and found $jem of $total symbol lines read. The allocator has escaped its feature gate — the default build must use the system allocator."
      say "arm $label: 0 JEMALLOC SYMBOLS RECOGNISED (of $total symbol lines read — the reader produced a symbol table, so this zero is MEASURED)"
      ;;
    *) fail "internal: unknown symbol expectation '$expect_syms'" ;;
  esac

  check_version_line "$BIN" "$expect_alloc" \
    || fail "arm $label (R2.1): \`cqlite-flight --version\` did not report 'allocator: $expect_alloc' as its single allocator line (detail above)"
  say "arm $label: --version reports 'allocator: $expect_alloc'"
  say "arm $label VERDICT PASS"
}

# Positive arm FIRST, negative arm SECOND, so the tree is left holding the
# default-features build every other gate component expects.
run_arm positive '--features jemalloc' present jemalloc
run_arm negative ''                    absent  system

pass "both arms discriminated: --features jemalloc links jemalloc and reports it, the default build links none and reports 'system' (issue #3997, R1.1/R1.2/R2.1)"
