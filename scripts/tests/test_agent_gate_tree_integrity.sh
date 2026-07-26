#!/usr/bin/env bash
# Regression test for issue #2926: a gate run whose WORKTREE MUTATES MID-RUN must not
# certify. `scripts/agent-gate.sh` stamped `commit:`/`dirty:` at SUMMARY-EMIT time and
# read no tree state at start, so a worktree edited while the gate ran emitted a block
# attributing MIXED-TREE results to the FINAL sha — formally indistinguishable from a
# real certification (field incident 2026-07-26, PR #2916, caught only by timing luck).
#
# The guard captures a TREE IDENTITY at start, re-verifies it at every component
# boundary and immediately before the terminal emit, and FAILs CLOSED.
#
# DISCRIMINATION (the point of this file — see #2926 and the five can't-fail guards
# that preceded it): the mutated cases and the unmutated CONTROL cases are run through
# the SAME harness, so
#   * deleting/stubbing the guard  -> every mutated case fails (they would certify), and
#   * hardwiring the guard to FAIL -> every control case fails (they would not certify).
# The porcelain-identical case (an append to an ALREADY-modified file) is the third
# discriminator: a naive `git status --porcelain` implementation passes A and B and
# still misses the dominant real-world shape.
#
# NO TEST-ONLY BYPASS SEAM is introduced to "prove" the guard can fail — such a seam
# would itself be the escape hatch #2926 forbids. The AGENT_GATE_TREE_SELFTEST hooks
# only SEQUENCE a real mutation against real capture/verify code; no mode can turn a
# mutated run green.
#
# Hermetic: every fixture lives under one per-run `mktemp -d …XXXXXX`, no network, no
# shared global state, no repo write outside that namespace. Fast: the real-gate phases
# use `--only fmt` / `--lite` / `--delta` against a FAKE checkout with a stub `cargo`,
# so nothing compiles. Deterministic: the mid-run mutation is performed BY the gate
# process itself (hook modes) or BY the component the gate runs (the stub cargo), never
# sequenced by a sleep, and NO assertion references elapsed time (#2642).
#
# Run standalone:   bash scripts/tests/test_agent_gate_tree_integrity.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# Never inherit a caller's summary path / parent marker (#2751/#2874 discipline).
unset AGENT_GATE_SUMMARY_FILE
unset AGENT_GATE_PARENT_RUN_ID

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-tree.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

GIT_ID=(-c user.email=gate@example.invalid -c user.name=gate-selftest)

# ---------------------------------------------------------------------------
# Fixture: a FAKE checkout. Copying ONLY the gate into <root>/scripts/ makes the gate's
# `cd "$(dirname "$0")/.."` resolve REPO_ROOT to <root>, so every capture, default
# summary path and mutation stays inside this run's mktemp namespace.
# ---------------------------------------------------------------------------
mkrepo() { # mkrepo <name> -> echoes the repo path
  local root="$tmp/$1"
  mkdir -p "$root/scripts"
  cp "$GATE" "$root/scripts/agent-gate.sh"
  printf 'hello\n'            > "$root/README.md"
  printf 'lock v1\n'          > "$root/Cargo.lock"
  printf 'docs body\n'        > "$root/NOTES.md"
  printf 'target/\n*.log\n.agent-gate-summary.txt\n.agent-gate-lite-summary.txt\n.agent-gate-delta-summary.txt\nignored-dir/\n' \
                              > "$root/.gitignore"
  ( cd "$root" && git init -q . && git add -A && git "${GIT_ID[@]}" commit -qm init ) >/dev/null 2>&1
  printf '%s\n' "$root"
}

# A stub `cargo` that (a) always succeeds so no component ever compiles, and (b) — when
# FAKE_CARGO_MUTATE names a file — MUTATES THE CHECKOUT while the `fmt` component is
# running. That is the field shape exactly: a second writer edits the worktree while a
# component executes, with no sleep and no race.
STUBBIN="$tmp/stubbin"
mkdir -p "$STUBBIN"
cat > "$STUBBIN/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = fmt ]; then
  [ -n "${FAKE_CARGO_MUTATE:-}" ] && printf 'mid-run edit\n' >> "$FAKE_CARGO_MUTATE"
  if [ -n "${FAKE_CARGO_CHURN:-}" ]; then
    # Exactly the churn a real gate produces inside the checkout: build output under
    # target/ and a *.log — both covered by the repo's own ignore rules.
    mkdir -p "$FAKE_CARGO_CHURN/target/debug"
    date > "$FAKE_CARGO_CHURN/target/debug/artifact"
    date > "$FAKE_CARGO_CHURN/build-noise.log"
  fi
fi
case "${1:-}" in metadata) printf '{"packages":[],"workspace_members":[],"target_directory":"/tmp"}\n' ;; esac
exit 0
STUB
chmod +x "$STUBBIN/cargo"

run_gate() { # run_gate <repo> <summary-file> <out-file> [-- gate args…]  (env via caller)
  local repo="$1" sum="$2" out="$3"; shift 3
  ( cd "$repo" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      bash "$repo/scripts/agent-gate.sh" "$@" >"$out" 2>&1 )
}

# capture_identity <repo> <field> [env KEY=VAL …] — drive the REAL start capture through
# the `capture` hook and echo one field of the identity it produced.
capture_identity() {
  local repo="$1" field="$2"; shift 2
  local raw
  raw=$( cd "$repo" && env "$@" \
           AGENT_GATE_SUMMARY_FILE="$tmp/capture-sentinel.txt" \
           AGENT_GATE_TREE_SELFTEST=capture \
           bash "$repo/scripts/agent-gate.sh" 2>/dev/null )
  printf '%s\n' "$raw" | sed -n "s/.*[ =]${field}=\([^ ]*\).*/\1/p" | head -1
}
digest_of() { capture_identity "$1" digest "${@:2}"; }

porcelain_of() { ( cd "$1" && git --no-optional-locks status --porcelain ); }
odb_count_of()  { find "$1/.git/objects" -type f 2>/dev/null | wc -l | tr -d ' '; }
index_hash_of() { git hash-object "$1/.git/index" 2>/dev/null; }

# assert_named_fail <label> <summary-file> <rc> — the canonical "did NOT certify" check.
assert_named_fail() {
  local label="$1" sum="$2" rc="$3" missing=()
  grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$sum" 2>/dev/null || missing+=("named-tree-integrity-FAIL-line")
  grep -q '^RESULT: FAIL'   "$sum" 2>/dev/null || missing+=("RESULT:-FAIL")
  grep -q '^RESULT: PASS'   "$sum" 2>/dev/null && missing+=("UNEXPECTED-RESULT:-PASS")
  grep -q '^RESULT: PARTIAL' "$sum" 2>/dev/null && missing+=("UNEXPECTED-RESULT:-PARTIAL")
  [ "$rc" -ne 0 ] || missing+=("non-zero-exit(got $rc)")
  if [ "${#missing[@]}" -eq 0 ]; then
    ok "$label: did NOT certify — named tree-integrity FAIL + RESULT: FAIL + non-zero exit"
  else
    bad "$label: ${missing[*]}"
    echo "------- summary -------"; cat "$sum" 2>/dev/null; echo "-----------------------"
  fi
}

echo "=== phase 1: the digest itself (drives the production capture) ==============="

r1=$(mkrepo digest-repo)

# --- F: idempotence + zero perturbation ---------------------------------------
p_before=$(porcelain_of "$r1"); idx_before=$(index_hash_of "$r1"); odb_before=$(odb_count_of "$r1")
m1="$tmp/manifest-1"; m2="$tmp/manifest-2"
d1=$(capture_identity "$r1" digest AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$m1")
d2=$(capture_identity "$r1" digest AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$m2")
p_after=$(porcelain_of "$r1"); idx_after=$(index_hash_of "$r1"); odb_after=$(odb_count_of "$r1")

if [ -n "$d1" ] && [ "$d1" = "$d2" ]; then
  ok "F: two captures of an unchanged tree produce an identical digest ($d1)"
else
  bad "F: capture is not idempotent (d1='$d1' d2='$d2')"
fi
if cmp -s "$m1" "$m2"; then
  ok "F: two captures produce BYTE-IDENTICAL manifests"
else
  bad "F: manifests differ across two captures of an unchanged tree"
fi
if [ "$p_before" = "$p_after" ] && [ "$idx_before" = "$idx_after" ]; then
  ok "F: capture perturbs neither the git index nor the working tree (--no-optional-locks)"
else
  bad "F: capture perturbed the index/worktree (index $idx_before -> $idx_after)"
fi
if [ "$odb_before" = "$odb_after" ]; then
  ok "F: capture writes NO object to the object database ($odb_before objects before and after)"
else
  bad "F: capture wrote to the ODB ($odb_before -> $odb_after) — hash-object must run without -w"
fi

# --- C (digest level): appending to an ALREADY-MODIFIED file -------------------
printf 'first edit\n' >> "$r1/README.md"
pA=$(porcelain_of "$r1"); dA=$(digest_of "$r1")
printf 'second edit\n' >> "$r1/README.md"
pB=$(porcelain_of "$r1"); dB=$(digest_of "$r1")
if [ "$pA" = "$pB" ]; then
  ok "C: git status --porcelain is BYTE-IDENTICAL across the second append (the trap)"
else
  bad "C: porcelain changed across the append — the case under test was not reproduced"
fi
if [ -n "$dA" ] && [ "$dA" != "$dB" ]; then
  ok "C: the digest DOES change across the porcelain-identical append ($dA -> $dB)"
else
  bad "C: digest unchanged across a porcelain-identical content change — the naive guard"
fi
( cd "$r1" && git checkout -q -- README.md )

# --- mode change with unchanged content ---------------------------------------
d_base=$(digest_of "$r1")
chmod +x "$r1/NOTES.md"
d_mode=$(digest_of "$r1")
if [ "$d_base" != "$d_mode" ]; then
  ok "mode: flipping the executable bit (content unchanged) changes the digest"
else
  bad "mode: an executable-bit flip is invisible to the digest"
fi
chmod -x "$r1/NOTES.md"

# --- deletion ------------------------------------------------------------------
mv "$r1/NOTES.md" "$tmp/NOTES.md.parked"
d_del=$(digest_of "$r1" AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$tmp/manifest-del")
if [ "$d_del" != "$d_base" ] && tr '\0' '\n' < "$tmp/manifest-del" | grep -q 'DELETED.*NOTES.md'; then
  ok "delete: removing a tracked file changes the digest and records DELETED in the manifest"
else
  bad "delete: tracked-file removal not detected (digest '$d_del' vs base '$d_base')"
fi
mv "$tmp/NOTES.md.parked" "$r1/NOTES.md"

# --- D: untracked add / change / remove ----------------------------------------
d_u0=$(digest_of "$r1")
printf 'new\n' > "$r1/untracked.txt"
d_u1=$(digest_of "$r1")
printf 'more\n' >> "$r1/untracked.txt"
d_u2=$(digest_of "$r1")
rm -f "$r1/untracked.txt"
d_u3=$(digest_of "$r1")
if [ "$d_u1" != "$d_u0" ] && [ "$d_u2" != "$d_u1" ] && [ "$d_u2" != "$d_u0" ]; then
  ok "D: untracked add and untracked content-change each yield a distinct digest"
else
  bad "D: untracked add/change not distinguished (u0=$d_u0 u1=$d_u1 u2=$d_u2)"
fi
# Removal restores the ORIGINAL content, so the digest MUST return to the baseline —
# the identity is a content identity, not a counter. What must be detected is the
# removal of an untracked file that was PRESENT at the baseline capture:
if [ "$d_u3" = "$d_u0" ]; then
  ok "D: removing the untracked file restores the baseline digest (content identity)"
else
  bad "D: digest did not return to baseline after the untracked file was removed"
fi
printf 'present at baseline\n' > "$r1/pre-existing.txt"
d_p0=$(digest_of "$r1")
rm -f "$r1/pre-existing.txt"
d_p1=$(digest_of "$r1")
if [ "$d_p0" != "$d_p1" ]; then
  ok "D: removing an untracked file that existed at capture time changes the digest"
else
  bad "D: untracked-file removal is invisible to the digest"
fi

# --- E (digest level): gitignored churn is outside the digest -------------------
d_c0=$(digest_of "$r1")
mkdir -p "$r1/target/debug"; date > "$r1/target/debug/artifact"; date > "$r1/noise.log"
mkdir -p "$r1/ignored-dir"; date > "$r1/ignored-dir/x"
d_c1=$(digest_of "$r1")
if [ "$d_c0" = "$d_c1" ]; then
  ok "E: target/ + *.log + an ignored dir do NOT move the digest (.gitignore IS the exclusion set)"
else
  bad "E: gitignored churn moved the digest — the guard would self-trip on every real run"
fi
# …but a NON-ignored new file must still move it (proving the exclusion is not blanket).
date > "$r1/not-ignored.txt"
d_c2=$(digest_of "$r1")
if [ "$d_c2" != "$d_c1" ]; then
  ok "E: a NON-ignored untracked file still moves the digest (exclusion is not blanket)"
else
  bad "E: a non-ignored untracked file was excluded — the exclusion set is too broad"
fi
rm -f "$r1/not-ignored.txt"

# --- docs / test-data are deliberately NOT excluded -----------------------------
mkdir -p "$r1/docs" "$r1/test-data"
printf 'doc\n' > "$r1/docs/page.md"; printf 'fixture\n' > "$r1/test-data/fixture.json"
( cd "$r1" && git add -A && git "${GIT_ID[@]}" commit -qm docs ) >/dev/null 2>&1
d_d0=$(digest_of "$r1")
printf 'mid-run doc edit\n' >> "$r1/docs/page.md"
d_d1=$(digest_of "$r1")
printf 'mid-run fixture swap\n' >> "$r1/test-data/fixture.json"
d_d2=$(digest_of "$r1")
if [ "$d_d1" != "$d_d0" ] && [ "$d_d2" != "$d_d1" ]; then
  ok "scope: mid-run edits under docs/ and test-data/ BOTH move the digest (never excluded)"
else
  bad "scope: a docs/ or test-data/ edit was excluded from the digest"
fi
( cd "$r1" && git checkout -q -- docs test-data )

# --- the hash cap is stamped, and cannot suppress a detection -------------------
printf 'oversized untracked payload\n' > "$r1/big-untracked.bin"
cap_out=$( cd "$r1" && env AGENT_GATE_SUMMARY_FILE="$tmp/cap-sentinel.txt" \
             AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_TREE_HASH_CAP_BYTES=1 \
             bash "$r1/scripts/agent-gate.sh" 2>/dev/null )
if printf '%s' "$cap_out" | grep -q 'cap-line=tree-hash-cap: 1 bytes (1 untracked file(s) recorded by size+mtime)'; then
  ok "cap: a non-default AGENT_GATE_TREE_HASH_CAP_BYTES and the fallback use are STAMPED"
else
  bad "cap: tree-hash-cap: stamp missing/incorrect"
  printf '%s\n' "$cap_out" | sed -n 's/^tree-selftest: cap-line=/  got: /p'
fi
rm -f "$r1/big-untracked.bin"

echo "=== phase 2: the verdict paths (MAIN lane / SIDE lane / terminal) ==========="

# --- B (hook control): no mutation -> a genuine RESULT: PASS --------------------
r2=$(mkrepo verdict-repo)
sum="$tmp/hook-clean.txt"; out="$tmp/hook-clean.out"
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=clean \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^RESULT: PASS' "$sum" && grep -q '^tree-integrity: PASS$' "$sum"; then
  ok "B(hook): an UNMUTATED run certifies — RESULT: PASS + tree-integrity: PASS (guard is not hardwired to FAIL)"
else
  bad "B(hook): unmutated run did not certify (rc=$rc)"
  cat "$sum" 2>/dev/null
fi
if grep -q '^tree-start: ' "$sum" && grep -q '^tree-end: ' "$sum"; then
  s_start=$(sed -n 's/^tree-start: .*digest: //p' "$sum" | head -1)
  s_end=$(sed -n 's/^tree-end: .*digest: //p' "$sum" | head -1)
  if [ -n "$s_start" ] && [ "$s_start" = "$s_end" ]; then
    ok "B(hook): the block carries tree-start/tree-end with EQUAL digests"
  else
    bad "B(hook): tree-start/tree-end digests differ on an unmutated run ($s_start vs $s_end)"
  fi
else
  bad "B(hook): tree-start:/tree-end: lines missing from the block"
fi

# --- A (MAIN lane boundary): mutation between two boundaries --------------------
sum="$tmp/hook-boundary.txt"; out="$tmp/hook-boundary.out"
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=boundary \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
assert_named_fail "A(hook, MAIN lane)" "$sum" "$rc"
if grep -q 'changed: README.md' "$sum" && grep -q 'detected-after-component: tree-selftest' "$sum"; then
  ok "A: the named line reports the changed path AND the detecting component"
else
  bad "A: the named line does not name the changed path / component"
fi
if grep -qE 'tree-integrity: FAIL \(tree-mutated-midrun; head [0-9a-f]+→[0-9a-f]+;' "$sum"; then
  ok "A: the named line reports both the start and the end HEAD sha"
else
  bad "A: the named line does not report head <a>→<b>"
fi
( cd "$r2" && git checkout -q -- README.md )

# --- SIDE lane: marker survives the drain, never a mid-run emit -----------------
sum="$tmp/hook-side.txt"; out="$tmp/hook-side.out"
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=side \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
assert_named_fail "SIDE lane" "$sum" "$rc"
if grep -q 'tree-selftest: side-rc=1 marker=yes' "$out" \
   && grep -q 'tree-selftest: sentinel-intact=yes' "$out"; then
  ok "SIDE lane: the subshell recorded a marker and returned non-zero WITHOUT emitting or exiting"
else
  bad "SIDE lane: subshell behaviour wrong (expected side-rc=1 marker=yes sentinel-intact=yes)"
  grep 'tree-selftest:' "$out"
fi
if grep -q 'detected-after-component: side-selftest' "$sum"; then
  ok "SIDE lane: the post-drain terminal block names the SIDE component that detected it"
else
  bad "SIDE lane: the terminal block lost the SIDE-lane component name"
fi
( cd "$r2" && git checkout -q -- README.md )

# --- terminal: a mutation after the LAST boundary is still caught ---------------
sum="$tmp/hook-terminal.txt"; out="$tmp/hook-terminal.out"
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
assert_named_fail "terminal (mutation after the last boundary)" "$sum" "$rc"
if grep -q 'detected-after-component: <terminal>' "$sum"; then
  ok "terminal: the block attributes the detection to the terminal capture"
else
  bad "terminal: missing 'detected-after-component: <terminal>'"
fi
( cd "$r2" && git checkout -q -- README.md )

# --- a mid-run COMMIT (the field incident's shape) ------------------------------
sum="$tmp/hook-commit.txt"; out="$tmp/hook-commit.out"
head_before=$( cd "$r2" && git rev-parse HEAD )
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_SELFTEST_COMMIT=1 \
    GIT_AUTHOR_NAME=gate GIT_AUTHOR_EMAIL=gate@example.invalid \
    GIT_COMMITTER_NAME=gate GIT_COMMITTER_EMAIL=gate@example.invalid \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
head_after=$( cd "$r2" && git rev-parse HEAD )
if [ "$head_before" != "$head_after" ]; then
  assert_named_fail "commit landing mid-run" "$sum" "$rc"
  if grep -q "head ${head_before:0:12}→${head_after:0:12}" "$sum"; then
    ok "commit: the named line reports the real start->end HEAD move"
  else
    bad "commit: the named line does not report the HEAD move ${head_before:0:12}→${head_after:0:12}"
  fi
else
  bad "commit: the fixture did not actually commit — the case was not exercised"
fi

# --- lockfile-settled: non-fatal alone, fatal with company ----------------------
r3=$(mkrepo lockfile-repo)
sum="$tmp/hook-lock.txt"
( cd "$r3" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE=Cargo.lock \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-lock.out" 2>&1 ); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^RESULT: PASS' "$sum" \
   && grep -qE '^tree-integrity: PASS \(lockfile-settled: Cargo.lock [0-9a-z]+→[0-9a-f]+\)' "$sum"; then
  ok "lockfile: a Cargo.lock-ONLY difference is stamped lockfile-settled (before→after) and still certifies"
else
  bad "lockfile: a lockfile-only difference did not produce the named non-fatal stamp (rc=$rc)"
  grep -E 'tree-integrity|RESULT' "$sum" 2>/dev/null
fi
( cd "$r3" && git checkout -q -- Cargo.lock )
sum="$tmp/hook-lock2.txt"
( cd "$r3" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE="Cargo.lock README.md" \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-lock2.out" 2>&1 ); rc=$?
assert_named_fail "lockfile + another path" "$sum" "$rc"
if grep -q 'changed: Cargo.lock README.md' "$sum"; then
  ok "lockfile: a lockfile change ALONGSIDE another path is fatal and lists both paths"
else
  bad "lockfile: the fatal case did not list both changed paths"
fi
( cd "$r3" && git checkout -q -- Cargo.lock README.md )

# --- NO BYPASS ------------------------------------------------------------------
sum="$tmp/hook-nobypass.txt"
( cd "$r3" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    AGENT_GATE_TREE_HASH_CAP_BYTES=1 \
    CQLITE_ALLOW_FILE_GROWTH=1 \
    AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
    CQLITE_GATE_MAX_CONCURRENCY=1 \
    AGENT_GATE_JOBS=1 \
    CQLITE_CLIPPY_FULL=1 \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-nobypass.out" 2>&1 ); rc=$?
assert_named_fail "no-bypass (every documented knob set, incl. a 1-byte hash cap)" "$sum" "$rc"
( cd "$r3" && git checkout -q -- README.md )

echo "=== phase 3: the real gate (wiring proof: --only / --lite / --delta) ========"

# --- B (real gate control) ------------------------------------------------------
r4=$(mkrepo real-repo)
sum="$tmp/only-ctl.txt"; out="$tmp/only-ctl.out"
run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
if grep -q '^tree-integrity: PASS$' "$sum" && grep -q '^RESULT: PARTIAL' "$sum" && [ "$rc" -eq 3 ]; then
  ok "B(real --only fmt): an unmutated run certifies normally — tree-integrity: PASS, RESULT: PARTIAL, exit 3"
else
  bad "B(real --only fmt): control run did not certify (rc=$rc)"
  cat "$sum" 2>/dev/null
fi

# --- A (real gate, mutated by the component itself) -----------------------------
sum="$tmp/only-mut.txt"; out="$tmp/only-mut.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
assert_named_fail "A(real --only fmt, mutated mid-component)" "$sum" "$rc"
if grep -q 'detected-after-component: fmt' "$sum"; then
  ok "A(real): detection is wired into record_result — attributed to the fmt boundary"
else
  bad "A(real): the failure is not attributed to a real component boundary"
fi
if grep -q 'PARTIAL' "$sum"; then
  bad "--only: a mutated PARTIAL run must NOT report PARTIAL"
else
  ok "--only: a mutated run reports FAIL, never PARTIAL"
fi
( cd "$r4" && git checkout -q -- README.md )

# --- C (real gate, porcelain-identical) -----------------------------------------
printf 'pre-existing modification\n' >> "$r4/README.md"
p_pre=$(porcelain_of "$r4")
sum="$tmp/only-porcelain.txt"; out="$tmp/only-porcelain.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
p_post=$(porcelain_of "$r4")
if [ "$p_pre" = "$p_post" ]; then
  ok "C(real): git status --porcelain is byte-identical before and after the mid-run append"
else
  bad "C(real): porcelain changed — the porcelain-identical case was not reproduced"
fi
assert_named_fail "C(real, append to an already-modified file)" "$sum" "$rc"
( cd "$r4" && git checkout -q -- README.md )

# --- E (real gate): gate-produced churn must NOT trip the guard ------------------
sum="$tmp/only-churn.txt"; out="$tmp/only-churn.out"
FAKE_CARGO_CHURN="$r4" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
if grep -q '^tree-integrity: PASS$' "$sum" && [ "$rc" -eq 3 ]; then
  ok "E(real): a run writing target/** and *.log still certifies (no false positive)"
else
  bad "E(real): gate-produced churn tripped the guard (rc=$rc)"
  grep -E 'tree-integrity|RESULT' "$sum" 2>/dev/null
fi
rm -rf "$r4/target" "$r4/build-noise.log"

# --- a caller-pinned RELATIVE, in-repo, NON-ignored summary path -----------------
# The gate writes this path twice by contract (sentinel + terminal emit); it must be
# excluded — and ONLY it, not untracked files in general.
out="$tmp/only-relsum.out"
( cd "$r4" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="in-repo-summary.txt" \
    bash "$r4/scripts/agent-gate.sh" --only fmt >"$out" 2>&1 ); rc=$?
if grep -q '^tree-integrity: PASS$' "$r4/in-repo-summary.txt" 2>/dev/null && [ "$rc" -eq 3 ]; then
  ok "exclusion: a caller-pinned relative in-repo summary path does not trip the guard"
else
  bad "exclusion: the run's own in-repo summary path tripped the guard (rc=$rc)"
  cat "$r4/in-repo-summary.txt" 2>/dev/null
fi
rm -f "$r4/in-repo-summary.txt"

# --- G: --lite ------------------------------------------------------------------
sum="$tmp/lite-mut.txt"; out="$tmp/lite-mut.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$sum" "$out" --lite; rc=$?
assert_named_fail "G(--lite mutated)" "$sum" "$rc"
if grep -q 'AGENT-GATE LITE SUMMARY' "$sum"; then
  ok "G(--lite): the refusal lands in the DISTINCTLY-marked LITE block"
else
  bad "G(--lite): the block is not the LITE-marked one"
fi
( cd "$r4" && git checkout -q -- README.md )
sum="$tmp/lite-ctl.txt"; out="$tmp/lite-ctl.out"
run_gate "$r4" "$sum" "$out" --lite; rc=$?
l_start=$(sed -n 's/^tree-start: .*digest: //p' "$sum" | head -1)
l_end=$(sed -n 's/^tree-end: .*digest: //p' "$sum" | head -1)
if grep -q '^tree-integrity: PASS$' "$sum" && [ -n "$l_start" ] && [ "$l_start" = "$l_end" ]; then
  ok "G(--lite control): the LITE block carries tree-start/tree-end/tree-integrity: PASS with equal digests"
else
  bad "G(--lite control): unmutated lite run did not stamp tree-integrity: PASS with equal digests"
  grep -E '^tree-' "$sum" 2>/dev/null
fi

# --- G: --delta -----------------------------------------------------------------
anchor=$( cd "$r4" && git rev-parse HEAD )
printf 'delta docs\n' >> "$r4/NOTES.md"
( cd "$r4" && git add -A && git "${GIT_ID[@]}" commit -qm 'docs-only change' ) >/dev/null 2>&1
sum="$tmp/delta-ctl.txt"; out="$tmp/delta-ctl.out"
run_gate "$r4" "$sum" "$out" --delta "$anchor" --anchor-run-id selftest; rc=$?
if [ "$rc" -eq 0 ] && grep -q '^RESULT: PASS' "$sum" && grep -q '^tree-integrity: PASS$' "$sum"; then
  ok "G(--delta control): a docs-only re-cert certifies with tree-integrity: PASS (RESULT: PASS, exit 0)"
else
  bad "G(--delta control): the unmutated delta did not certify (rc=$rc)"
  grep -E 'tree-|RESULT' "$sum" 2>/dev/null
fi
sum="$tmp/delta-mut.txt"; out="$tmp/delta-mut.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$sum" "$out" --delta "$anchor" --anchor-run-id selftest; rc=$?
assert_named_fail "G(--delta mutated)" "$sum" "$rc"
if grep -q 'AGENT-GATE DELTA SUMMARY' "$sum"; then
  ok "G(--delta): the refusal lands in the DISTINCTLY-marked DELTA block"
else
  bad "G(--delta): the block is not the DELTA-marked one"
fi
( cd "$r4" && git checkout -q -- README.md )

echo "=== phase 4: block shape, the sentinel, and #2908 non-regression ==========="

# The startup sentinel carries tree-start and NO tree-end (the `capture` hook exits
# before any emit, so the sentinel is exactly what a KILLED gate would leave behind).
sent="$tmp/sentinel.txt"
( cd "$r4" && env AGENT_GATE_SUMMARY_FILE="$sent" AGENT_GATE_TREE_SELFTEST=capture \
    bash "$r4/scripts/agent-gate.sh" >/dev/null 2>&1 )
if grep -q '^tree-start: ' "$sent" && ! grep -q '^tree-end: ' "$sent" \
   && grep -q '^RESULT: INCOMPLETE (gate did not finish)$' "$sent"; then
  ok "sentinel: a killed run's startup sentinel records tree-start (and no tree-end), RESULT: INCOMPLETE intact"
else
  bad "sentinel: wrong shape"
  cat "$sent" 2>/dev/null
fi

# #2908 non-regression: no line this capability adds may contain the token `RESULT:`,
# so BOTH the buggy and the corrected poll predicate behave exactly as before.
for f in "$tmp/only-mut.txt" "$tmp/hook-clean.txt" "$sent"; do
  n=$(grep -c 'RESULT:' "$f" 2>/dev/null)
  if [ "$n" = 1 ]; then
    ok "#2908: $(basename "$f") contains exactly ONE 'RESULT:' token (poll predicates unaffected)"
  else
    bad "#2908: $(basename "$f") contains $n 'RESULT:' tokens — a new line embeds the token"
  fi
done
if grep -qE '^RESULT: (PASS|FAIL)' "$tmp/only-mut.txt" \
   && ! grep -q 'RESULT: INCOMPLETE' "$tmp/only-mut.txt"; then
  ok "#2908: a mutated run's verdict is FAIL, never the INCOMPLETE liveness placeholder"
else
  bad "#2908: a mutated run reported INCOMPLETE instead of a verdict"
fi

# The synthetic emission modes stamp a `selftest` identity with no git dependency.
selfsum="$tmp/emit-selftest.txt"
( cd "$r4" && AGENT_GATE_SUMMARY_FILE="$selfsum" bash "$r4/scripts/agent-gate.sh" --emit-summary-selftest ) >/dev/null 2>&1
if grep -q '^tree-start: selftest dirty: no digest: selftest$' "$selfsum" \
   && grep -q '^tree-end: selftest dirty: no digest: selftest$' "$selfsum" \
   && grep -q '^tree-integrity: PASS (selftest)$' "$selfsum"; then
  ok "selftest modes: --emit-summary-selftest stamps the synthetic tree identity"
else
  bad "selftest modes: synthetic tree identity missing from the --emit-summary-selftest block"
  grep -E '^tree-' "$selfsum" 2>/dev/null
fi

# --list stays exempt and unchanged (it exits before the run is established).
list_out=$( cd "$r4" && bash "$r4/scripts/agent-gate.sh" --list 2>&1 )
if printf '%s' "$list_out" | grep -q 'fmt' && ! printf '%s' "$list_out" | grep -q 'tree-'; then
  ok "helper modes: --list is exempt (no capture, output unchanged)"
else
  bad "helper modes: --list output changed"
fi

# --- summary-integrity AND tree-integrity firing in the SAME run ----------------
# The #2874 guard assembles its own block (it is the no-clobber publish path), so the
# tree provenance must be threaded through it too. Seed a FOREIGN run-id at a
# READ-ONLY summary path (our startup sentinel cannot land -> the summary guard fires
# at the first boundary) while the stub cargo mutates the tree in the same component.
coex="$tmp/coexist-summary.txt"
{ echo '==== AGENT-GATE SUMMARY ===='; echo 'run-id: /tmp/agent-gate.FOREIGN-COEXIST'
  echo 'RESULT: INCOMPLETE'; echo '==== END AGENT-GATE SUMMARY ===='; } > "$coex"
chmod 0444 "$coex"
out="$tmp/coexist.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$coex" "$out" --only fmt; rc=$?
chmod 0644 "$coex" 2>/dev/null || true
if grep -q 'summary-integrity: FAIL' "$out" \
   && grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$out" \
   && grep -q '^RESULT: FAIL' "$out" && ! grep -q '^RESULT: PASS' "$out" && [ "$rc" -ne 0 ]; then
  ok "coexistence: a clobbered AND mutated run emits BOTH named lines under a single RESULT: FAIL"
else
  bad "coexistence: the two guards do not compose (rc=$rc)"
  grep -E 'summary-integrity|tree-integrity|^RESULT:' "$out" 2>/dev/null | head
fi
( cd "$r4" && git checkout -q -- README.md )

# --- an early preflight FAIL still carries the provenance ------------------------
# emit_summary sites that assemble their own meta (not via SUMMARY_META) must stamp the
# lines too, or a run that dies before the terminal emit has no tree provenance at all.
sum="$tmp/preflight.txt"; out="$tmp/preflight.out"
mkdir -p "$tmp/empty-datasets"
( cd "$r4" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
    CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
    bash "$r4/scripts/agent-gate.sh" --only core-tests >"$out" 2>&1 ); rc=$?
if grep -q '^preflight: FAIL' "$sum" 2>/dev/null; then
  if grep -q '^tree-start: ' "$sum" && grep -q '^tree-end: ' "$sum" && grep -q '^tree-integrity: ' "$sum"; then
    ok "early exit: a dataset-preflight FAIL block still carries tree-start/tree-end/tree-integrity"
  else
    bad "early exit: the preflight FAIL block has no tree provenance"
    cat "$sum" 2>/dev/null
  fi
else
  bad "early exit: the preflight FAIL path was not reached (rc=$rc) — case not exercised"
  head -20 "$out" 2>/dev/null
fi

echo "=== phase 5: structural wiring (the mechanism cannot go inert) =============="

if awk '/^record_result\(\) \{/,/^\}/' "$GATE" | grep -q '_assert_tree_integrity "\$1"'; then
  ok "WIRING: record_result() calls _assert_tree_integrity \"\$1\" (boundary chokepoint live)"
else
  bad "WIRING: record_result() does NOT call the tree guard — the mechanism is inert on a real gate"
fi
cap_ln=$(grep -n '^  _tree_capture_start$' "$GATE" | head -1 | cut -d: -f1)
lite_ln=$(grep -n '^  run_lite$' "$GATE" | head -1 | cut -d: -f1)
delta_ln=$(grep -n '^  run_delta "\$DELTA_ANCHOR"$' "$GATE" | head -1 | cut -d: -f1)
slot_ln=$(grep -n '^acquire_gate_slot$' "$GATE" | tail -1 | cut -d: -f1)
if [ -n "$cap_ln" ] && [ -n "$lite_ln" ] && [ -n "$delta_ln" ] && [ -n "$slot_ln" ] \
   && [ "$cap_ln" -lt "$lite_ln" ] && [ "$cap_ln" -lt "$delta_ln" ] && [ "$cap_ln" -lt "$slot_ln" ]; then
  ok "WIRING: the start capture precedes run_lite, run_delta and acquire_gate_slot (all modes guarded)"
else
  bad "WIRING: start capture is not before the mode dispatch (capture=$cap_ln lite=$lite_ln delta=$delta_ln slot=$slot_ln)"
fi
n_final=$(grep -c '_tree_finalize' "$GATE")
if [ "$n_final" -ge 5 ]; then
  ok "WIRING: _tree_finalize is called on every terminal path (full, lite, delta, delta-refusals): $n_final references"
else
  bad "WIRING: only $n_final _tree_finalize references — a terminal emit path is unguarded"
fi
# Side-effect freedom is a CODE property: no -w on hash-object, --no-optional-locks on
# every git call inside the capture helpers. Comment lines are stripped first so prose
# ABOUT the rule cannot satisfy (or trip) the check.
code_only() { sed 's/[[:space:]]*#.*$//'; }
if grep 'hash-object' "$GATE" | code_only | grep -qE '(^|[[:space:]])-w([[:space:]]|$)'; then
  bad "SIDE-EFFECTS: a hash-object call uses -w — the capture would write to the shared ODB"
else
  ok "SIDE-EFFECTS: no hash-object call uses -w (nothing is written to the object database)"
fi
bare=$(awk '/^_tree_identity\(\) \{/,/^\}/' "$GATE" | code_only | grep -c 'git ' | tr -d ' ')
locked=$(awk '/^_tree_identity\(\) \{/,/^\}/' "$GATE" | code_only | grep -c 'git --no-optional-locks' | tr -d ' ')
if [ "$bare" = "$locked" ] && [ "$locked" -gt 0 ]; then
  ok "SIDE-EFFECTS: every git call in _tree_identity passes --no-optional-locks ($locked/$bare)"
else
  bad "SIDE-EFFECTS: $((bare - locked)) git call(s) in _tree_identity lack --no-optional-locks"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
