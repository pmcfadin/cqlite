#!/usr/bin/env bash
# Regression test for #4387: a worktree must never run another worktree's workspace test
# binaries, nor see another worktree's CARGO_MANIFEST_DIR.
#
# Hermetic: builds a throwaway one-crate git repo with two real `git worktree`s under a temp
# dir, a temp shared CARGO_TARGET_DIR, a real cargo (offline, no dependencies). Never touches
# this checkout's target dir. Cleans up on exit.
#
#   1. CONTROL: with ONE shared target dir and byte-identical sources, worktree B's test run
#      prints worktree A's CARGO_MANIFEST_DIR. Proves this harness can see the bug; if cargo
#      ever fixes the fingerprint this stops failing and the control reports it.
#   2. FIX: the dir `scripts/agent-gate.sh --print-target-dir` resolves for A and B differs,
#      and B's test prints B's own manifest dir, from a binary under B's dir.
#   3. A is then removed: B still passes (the deleted-worktree half of the bug).
#   4. Resolution is idempotent, strips an inherited per-worktree suffix, and honours the
#      CQLITE_SHARED_TARGET=1 opt-out and an unset CARGO_TARGET_DIR.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REAL_SCRIPTS=$(cd "$HERE/.." && pwd)
PASS=0 FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

command -v cargo >/dev/null 2>&1 || { echo "SKIP - cargo not on PATH"; exit 0; }

T=$(mktemp -d "${TMPDIR:-/tmp}/wt-iso.XXXXXX")
cleanup() { git -C "$T/main" worktree prune >/dev/null 2>&1 || true; rm -rf "$T"; }
trap cleanup EXIT

SHARED="$T/shared-target"
git init -q "$T/main"
(
  cd "$T/main" || exit 1
  git config user.email t@example.invalid; git config user.name t
  mkdir -p src tests scripts
  printf '[package]\nname = "iso"\nversion = "0.0.0"\nedition = "2021"\n[workspace]\n' >Cargo.toml
  echo '' >src/lib.rs
  cat >tests/where.rs <<'RS'
#[test]
fn where_am_i() { println!("MANIFEST_DIR={}", env!("CARGO_MANIFEST_DIR")); }
RS
  cp -R "$REAL_SCRIPTS"/. scripts/
  git add -A >/dev/null && git commit -qm init
) || { bad "could not build the fixture repo"; exit 1; }
git -C "$T/main" worktree add -q "$T/A" -b a >/dev/null 2>&1
git -C "$T/main" worktree add -q "$T/B" -b b >/dev/null 2>&1
A=$(cd "$T/A" && pwd -P); B=$(cd "$T/B" && pwd -P)

run_where() {  # <worktree> <target-dir> -> prints the MANIFEST_DIR the test binary reports
  ( cd "$1" && env -u RUSTC_WRAPPER CARGO_TARGET_DIR="$2" CARGO_INCREMENTAL=0 \
      cargo test --offline -q --test where -- --nocapture 2>&1 ) | sed -n 's/^MANIFEST_DIR=//p'
}
gate_dir() {  # <worktree> -> CARGO_TARGET_DIR the gate would hand cargo
  ( cd "$1" && env -u CQLITE_SHARED_TARGET CARGO_TARGET_DIR="${2-$SHARED}" bash scripts/agent-gate.sh --print-target-dir 2>/dev/null )
}

# 1. control: shared dir, no isolation
run_where "$A" "$SHARED" >/dev/null
ctl=$(run_where "$B" "$SHARED")
if [ "$ctl" = "$A" ]; then ok "control: shared target dir makes B run A's binary (MANIFEST_DIR=A) -- the bug is visible here"
elif [ "$ctl" = "$B" ]; then bad "control: cargo no longer shares the binary; the harness cannot see the #4387 bug (re-evaluate this test)"
else bad "control: unexpected MANIFEST_DIR '$ctl'"; fi

# 2. fix: the gate's target dir
DA=$(gate_dir "$A"); DB=$(gate_dir "$B")
[ -n "$DA" ] && [ -n "$DB" ] && [ "$DA" != "$DB" ] && ok "gate resolves a different target dir per worktree" || bad "gate resolved A='$DA' B='$DB'"
case "$DA" in "$SHARED"/wt/*) ok "per-worktree dirs live under the shared base" ;; *) bad "A's dir '$DA' is not under $SHARED/wt/" ;; esac
[ "$(cat "$DA/.worktree-root" 2>/dev/null)" = "$A" ] && ok "marker records the worktree root (used by the prune)" || bad "marker missing/wrong in $DA"
run_where "$A" "$DA" >/dev/null
got=$(run_where "$B" "$DB")
[ "$got" = "$B" ] && ok "B's test binary reports B's CARGO_MANIFEST_DIR" || bad "B reported '$got', expected '$B'"
na=$(find "$DB" -type f -name 'where-*' -perm -u+x 2>/dev/null | wc -l)
[ "$na" -ge 1 ] && ok "B's test binary lives under B's own dir" || bad "no test binary under $DB"
[ "$(find "$DA" -type f -name 'where-*' -perm -u+x | wc -l)" -ge 1 ] && ok "A's binary is separate (under A's dir)" || bad "no test binary under $DA"

# 3. deleted-worktree half
git -C "$T/main" worktree remove --force "$T/A" >/dev/null 2>&1
got=$(run_where "$B" "$DB")
[ "$got" = "$B" ] && ok "B still passes after A is removed" || bad "after removing A, B reported '$got'"

# 4. resolution properties
( cd "$B" && unset CQLITE_SHARED_TARGET && . scripts/lib/worktree-target-dir.sh
  [ "$(CARGO_TARGET_DIR=$DB gate_worktree_target_dir "$B")" = "$DB" ] ) && ok "idempotent: an inherited per-worktree dir resolves to itself" || bad "not idempotent"
other=$(mktemp -d "$T/other.XXXXXX")
( cd "$B" && unset CQLITE_SHARED_TARGET && . scripts/lib/worktree-target-dir.sh
  r=$(CARGO_TARGET_DIR=$DB gate_worktree_target_dir "$other"); [ "$r" != "$DB" ] && case "$r" in "$SHARED"/wt/*) [ "${r#"$SHARED"/wt/}" = "${r##*/}" ] ;; *) false ;; esac ) \
  && ok "nested gate on another root moves to its own sibling dir, no wt/<h>/wt/<h> nesting" || bad "inherited suffix not stripped for a different root"
( cd "$B" && [ "$(CQLITE_SHARED_TARGET=1 CARGO_TARGET_DIR=$SHARED bash scripts/agent-gate.sh --print-target-dir 2>/dev/null)" = "$SHARED" ] ) && ok "CQLITE_SHARED_TARGET=1 opts out" || bad "opt-out ignored"
( cd "$B" && [ -z "$(env -u CARGO_TARGET_DIR -u CQLITE_SHARED_TARGET bash scripts/agent-gate.sh --print-target-dir 2>/dev/null)" ] ) && ok "unset CARGO_TARGET_DIR stays unset (cargo's default <root>/target is already per-checkout)" || bad "unset var was set"

# 5. cargo's other target-dir sources: CARGO_BUILD_TARGET_DIR and `[build] target-dir` in config
bd=$( cd "$B" && env -u CQLITE_SHARED_TARGET -u CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR="$T/envbase" bash scripts/agent-gate.sh --print-target-dir 2>/dev/null )
case "$bd" in "$T"/envbase/wt/*) ok "CARGO_BUILD_TARGET_DIR is isolated too (exported as CARGO_TARGET_DIR)" ;; *) bad "CARGO_BUILD_TARGET_DIR gave '$bd'" ;; esac
git -C "$T/main" worktree add -q "$T/C" -b c >/dev/null 2>&1
mkdir -p "$T/C/.cargo"; printf '[build]\ntarget-dir = "%s/cfgbase"\n' "$T" >"$T/C/.cargo/config.toml"
cd_=$( cd "$T/C" && env -u CQLITE_SHARED_TARGET -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR bash scripts/agent-gate.sh --print-target-dir 2>/dev/null )
case "$cd_" in "$T"/cfgbase/wt/*) ok "a .cargo/config.toml build.target-dir is isolated too" ;; *) bad "config build.target-dir gave '$cd_'" ;; esac

# 6. idempotent without the marker: `cargo clean` deletes the dir (marker included)
( cd "$B" && unset CQLITE_SHARED_TARGET && . scripts/lib/worktree-target-dir.sh
  rm -rf "$DB"
  r=$(CARGO_TARGET_DIR=$DB gate_worktree_target_dir "$B")
  [ "$r" = "$DB" ] && [ "$(cat "$DB/.worktree-root")" = "$B" ] ) && ok "after a cargo-clean-style wipe the same target resolves to itself (no nesting) and the marker is rewritten" || bad "wiped target nested or lost its marker"

# 7. a fresh base gets cargo's CACHEDIR.TAG from the helper (the prune refuses a base without one)
[ -f "$SHARED/CACHEDIR.TAG" ] && grep -q '^Signature: 8a477f597d28d172789f06886806bc55' "$SHARED/CACHEDIR.TAG" && ok "fresh base carries a valid CACHEDIR.TAG" || bad "no CACHEDIR.TAG on the base"

printf '\n==== worktree target isolation self-test: PASS=%d FAIL=%d ====\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
