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
# EACH ARM ASSERTS ON A PRIVATE SNAPSHOT OF THE BINARY IT ITSELF BUILT, NEVER ON
# THE WELL-KNOWN PATH. This is the ordering hazard the guard is most likely to
# have, so it is closed by mechanism rather than by argument: the positive and the
# negative arm both uplift to the SAME `target/debug/cqlite-flight`, so an
# assertion made against that path is an assertion about whatever last wrote it.
# Instead each arm builds with `--message-format=json`, finds the
# `compiler-artifact` message for the `cqlite-flight` BIN target, and takes TWO
# things from it:
#
#   * `"features":[...]` — cargo's OWN statement of the feature set the artifact
#     was built with. The arm asserts its identity against that, so "this is the
#     jemalloc build" is attested by cargo rather than inferred from the flags we
#     passed. A mismatch is a FAIL, not a skip.
#   * `"executable"` — the artifact path, which is then COPIED to a per-arm
#     private file, with the source's device+inode+size checked either side of the
#     copy. Every symbol and `--version` assertion runs against that private copy.
#
# Two measurements this rests on, both taken on this tree rather than assumed:
# cargo emits the artifact message even for a FULLY FRESH build (the warm case the
# gate hits), and a no-op `cargo build` RE-UPLIFTS — a jemalloc-bearing binary
# planted at `target/debug/cqlite-flight` was replaced by the correct
# default-features one by a build that recompiled nothing (`Finished` in 0.17s,
# inode changed). So the well-known path is refreshed by each arm; the residual
# exposure it leaves is a CONCURRENT build in the same target directory
# overwriting it between our build and our read, and the private copy plus the
# inode check NARROWS that — it does not eliminate it, because a read and a copy
# are two operations.
#
# THAT RESIDUAL CAN ONLY PRODUCE A FALSE RED, NEVER A FALSE PASS, and the reason
# is structural rather than hopeful. The positive arm passes only on a binary with
# jemalloc symbols AND `allocator: jemalloc`; the negative arm only on one with
# neither. Since `ALLOCATOR` is derived from the SAME cfg that installs the
# allocator, a binary satisfying either arm IS a binary of that arm's
# configuration. So a cross-arm mix-up — the negative arm reading the positive
# arm's binary, or the reverse — makes the arm find the WRONG symbol state and
# FAIL. There is no substitution that passes while measuring nothing.
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

# Per-run scratch, for the private per-arm artifact snapshots. Created before any
# arm runs and removed on EXIT (`|| true`, because a failing command in a bash
# EXIT trap under `set -e` would replace the exit status; this script does not set
# -e, but the habit is cheap and the trap must never change a verdict).
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/flightalloclink.XXXXXX") || { printf '%s\n' "${P}verdict SKIP — cannot create a scratch directory for the per-arm artifact snapshots; nothing was measured" >&2; exit 0; }
trap 'rm -rf "$SCRATCH" || true' EXIT

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
say "target-dir $TARGET_DIR (reported for diagnosis only — NO arm asserts on \$TARGET_DIR/debug/cqlite-flight; see the header)"

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

# Resolve the artifact THIS arm's build produced, from that build's own JSON.
# Echoes `<executable-path>\t<comma-separated features>`; returns non-zero when
# the message stream holds no single recognisable bin artifact.
#
# The filter requires ALL of: a compiler-artifact message, the `cqlite-flight`
# name, `"crate_types":["bin"]`, and a non-null `executable`. EXACTLY ONE such
# message must be present — zero means we cannot say what we built, and more than
# one means the stream is not the shape this parser was written against; either
# way, refusing beats picking.
resolve_arm_artifact() {
  local json="$1" lines n exe feats
  lines=$(grep '"reason":"compiler-artifact"' "$json" 2>/dev/null \
          | grep '"name":"cqlite-flight"' \
          | grep '"crate_types":\["bin"\]' \
          | grep -o '{.*"executable":"[^"]*".*}')
  n=$(printf '%s\n' "$lines" | grep -c .)
  case "$n" in ''|*[!0-9]*) return 1 ;; esac
  [ "$n" -eq 1 ] || return 1
  exe=$(printf '%s\n' "$lines" | sed -n 's/.*"executable":"\([^"]*\)".*/\1/p')
  feats=$(printf '%s\n' "$lines" | sed -n 's/.*"features":\[\([^]]*\)\].*/\1/p' | tr -d '"')
  [ -n "$exe" ] || return 1
  printf '%s\t%s\n' "$exe" "$feats"
}

# The identity of a file, as a value that changes when the file is REPLACED.
# `%d:%i` (device:inode) plus size: cargo uplifts by hardlink, so a re-uplift or a
# peer's overwrite lands on a NEW inode. Prints nothing on failure.
file_identity() {
  stat -c '%d:%i:%s' -- "$1" 2>/dev/null
}

# Snapshot `$1` into the per-arm private file `$2`, refusing if the source was
# replaced across the copy. Returns 0 = snapshot taken, 1 = could not, 2 = the
# source changed under us.
snapshot_artifact() {
  local src="$1" dst="$2" id_before id_after
  id_before=$(file_identity "$src")
  [ -n "$id_before" ] || return 1
  cp -- "$src" "$dst" 2>/dev/null || return 1
  id_after=$(file_identity "$src")
  [ -n "$id_after" ] || return 1
  [ "$id_before" = "$id_after" ] || return 2
  [ -s "$dst" ] || return 1
  chmod u+x -- "$dst" 2>/dev/null || return 1
  return 0
}

# One arm. `$1` = arm label, `$2` = extra cargo args (may be empty), `$3` =
# `present` | `absent`, `$4` = the expected `--version` allocator value.
run_arm() {
  local label="$1" extra="$2" expect_syms="$3" expect_alloc="$4"
  local rc_local counts jem total resolved exe feats bin
  local json="$SCRATCH/$label.json" err="$SCRATCH/$label.err"
  say "arm $label: cargo build -p cqlite-flight ${extra:-<default features>}"
  # 25 min with a 30s SIGKILL escalation: the positive arm compiles jemalloc's
  # vendored C source cold, and the negative arm is the feature set the gate's other
  # components already built, so it is warm.
  # JSON on stdout (the artifact record this arm asserts against), human-readable
  # progress and diagnostics on stderr (kept for the failure message).
  # shellcheck disable=SC2086  # $extra is a deliberate word-split of our own literal flags
  timeout -k 30 1500 cargo build -p cqlite-flight $extra --message-format=json >"$json" 2>"$err"
  rc_local=$?
  [ "$rc_local" -eq 0 ] || fail "arm $label: \`cargo build -p cqlite-flight ${extra:-(default features)}\` exited $rc_local. This is a BROKEN BUILD, not an unmeasurable host. Remedy: run that exact command by hand and fix what it reports. Last lines of its stderr: $(tail -5 "$err" 2>/dev/null | tr '\n' '|')"

  # --- resolve the artifact from THIS build, never from the well-known path.
  resolved=$(resolve_arm_artifact "$json")
  rc_local=$?
  [ "$rc_local" -eq 0 ] || skip "arm $label: the build succeeded but its \`--message-format=json\` stream holds no single recognisable \`cqlite-flight\` bin artifact record, so this arm cannot say WHICH binary it produced — nothing was measured (a cargo JSON shape change). Refusing to fall back to \$TARGET_DIR/debug/cqlite-flight: both arms uplift to that path, so an assertion against it is an assertion about whatever last wrote it."
  exe=${resolved%%	*}
  feats=${resolved##*	}

  # --- the arm's IDENTITY, attested by cargo rather than inferred from our flags.
  case "$expect_syms" in
    present)
      printf '%s\n' "$feats" | tr ',' '\n' | grep -qx 'jemalloc' \
        || fail "arm $label: cargo reports this artifact was built with features [$feats], which does NOT include \`jemalloc\` — the positive arm did not build the binary it thinks it did, so any symbol verdict from it would be meaningless" ;;
    absent)
      printf '%s\n' "$feats" | tr ',' '\n' | grep -qx 'jemalloc' \
        && fail "arm $label: cargo reports this artifact was built with features [$feats], which DOES include \`jemalloc\` — the negative arm is not measuring a default-features build. \`default = []\`, so the allocator feature must not be reachable from a plain \`cargo build -p cqlite-flight\`" ;;
  esac
  say "arm $label: cargo attests features [${feats:-<none>}] for $exe"

  # --- private snapshot. Every assertion below runs on this copy, so a concurrent
  # --- build overwriting the uplifted path cannot change what we measured.
  bin="$SCRATCH/$label.bin"
  snapshot_artifact "$exe" "$bin"
  rc_local=$?
  case "$rc_local" in
    0) : ;;
    2) fail "arm $label: $exe was REPLACED while being snapshotted (its device:inode:size changed across the copy) — another build in this target directory is racing this one, so nothing measured here would be attributable to this arm's build. Re-run when no concurrent cargo build shares \$TARGET_DIR." ;;
    *) skip "arm $label: cannot snapshot the built artifact $exe into this run's scratch directory — nothing was measured" ;;
  esac

  counts=$(read_symbols "$bin")
  rc_local=$?
  [ "$rc_local" -eq 0 ] || skip "arm $label: \`$SYMTOOL\` could not be read for the snapshot of $exe (non-zero exit, timeout, or output this script cannot parse) — nothing was measured"
  jem=${counts%% *}
  total=${counts##* }
  # AFFIRMATIVE MEASUREMENT: a zero jemalloc count is only meaningful if the tool
  # produced a symbol table. Zero total symbols means the read told us nothing —
  # a stripped binary or a silently-empty tool — which must never read as clean.
  [ "$total" -gt 0 ] || skip "arm $label: \`$SYMTOOL\` produced ZERO symbol lines for the snapshot of $exe, so a zero jemalloc count would be UNMEASURED rather than clean (a stripped binary, or a symbol reader that emitted nothing)"

  case "$expect_syms" in
    present)
      [ "$jem" -gt 0 ] || fail "arm $label (R1.1): expected jemalloc symbols in the artifact cargo built at $exe and found 0 JEMALLOC SYMBOLS RECOGNISED out of $total symbol lines read. The \`jemalloc\` feature is declared but nothing was linked — check that src/main.rs's #[global_allocator] cfg matches the feature name and that the dependency is not cfg'd out on this target."
      say "arm $label: $jem JEMALLOC SYMBOLS RECOGNISED (of $total symbol lines read)"
      ;;
    absent)
      [ "$jem" -eq 0 ] || fail "arm $label (R1.2): expected NO jemalloc symbols in the default-features artifact cargo built at $exe and found $jem of $total symbol lines read. The allocator has escaped its feature gate — the default build must use the system allocator."
      say "arm $label: 0 JEMALLOC SYMBOLS RECOGNISED (of $total symbol lines read — the reader produced a symbol table, so this zero is MEASURED)"
      ;;
    *) fail "internal: unknown symbol expectation '$expect_syms'" ;;
  esac

  check_version_line "$bin" "$expect_alloc" \
    || fail "arm $label (R2.1): \`cqlite-flight --version\` did not report 'allocator: $expect_alloc' as its single allocator line (detail above)"
  say "arm $label: --version reports 'allocator: $expect_alloc'"
  say "arm $label VERDICT PASS"
}

# Positive arm FIRST, negative arm SECOND, so the tree is left holding the
# default-features build every other gate component expects.
run_arm positive '--features jemalloc' present jemalloc
run_arm negative ''                    absent  system

pass "both arms discriminated: --features jemalloc links jemalloc and reports it, the default build links none and reports 'system' (issue #3997, R1.1/R1.2/R2.1)"
