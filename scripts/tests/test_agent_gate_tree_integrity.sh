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

# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.bash
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.bash"

# ---------------------------------------------------------------------------
# Fixture: a FAKE checkout. Copying ONLY the gate into <root>/scripts/ makes the gate's
# `cd "$(dirname "$0")/.."` resolve REPO_ROOT to <root>, so every capture, default
# summary path and mutation stays inside this run's mktemp namespace.
# ---------------------------------------------------------------------------
mkrepo() { # mkrepo <name> [extra `git init` args…] -> echoes the repo path
  # `$tmp` is validated at the top of this file and `$1` is a literal at every call site, so
  # `$root` cannot be empty here — but `cd ""` SUCCEEDS in bash and would run the fixture's
  # git commands in the LIVE checkout, so the invariant is asserted rather than assumed.
  [ -n "${1:-}" ] && [ -n "${tmp:-}" ] \
    || { echo "FATAL: mkrepo needs a name and a scratch root" >&2; exit 1; }
  local root="$tmp/$1"; shift
  mkdir -p "$root/scripts"
  cp "$GATE" "$root/scripts/agent-gate.sh"
  # PIN THE CANONICAL IDENTITY in this fixture's own copy, BEFORE the commit below (#3544 /
  # roborev job 225). The pre-flight validates that `origin` NAMES the canonical upstream
  # before fetching, and the LOCAL bare origin created further down is deliberately NOT
  # canonical — so without the pin every fixture would stop at the pre-flight as
  # `remote-not-canonical` instead of exercising the tree-integrity guard under test.
  # Substituting the ARTIFACT in the scratch copy is the sanctioned pattern (CLAUDE.md); a
  # settable seam would reopen the hole the check closes. Pinning BEFORE the commit keeps the
  # fixture CLEAN — a post-commit pin would leave it dirty, which is itself an input these
  # cases assert on. FATAL rather than silent: an unpinned fixture measures nothing.
  agent_gate_pin_canonical_remote "$root/scripts/agent-gate.sh" "$root.origin.git" \
    || { echo "FATAL: could not pin the canonical identity in fixture '$root'" >&2; exit 1; }
  # …and the component MANIFEST beside the copy (#3544 REQ-3544-01), for the same reason and
  # with the same timing: the pre-flight asserts the working tree's manifest matches the
  # running COMPONENTS array before it fetches, so a gate copy without one stops at
  # `manifest-missing` in the certifying modes; and writing it BEFORE the commit keeps the
  # fixture CLEAN, which these cases assert on.
  agent_gate_install_components_manifest "$root/scripts/agent-gate.sh" \
    || { echo "FATAL: could not install the component manifest in fixture '$root'" >&2; exit 1; }
  # The DISPOSABLE-CHECKOUT MARKER (#2926 review B5): the gate's mutating self-test hooks
  # refuse to write into any checkout that does not carry it, so they can never append to
  # — or commit into — a live repo. Committed, so it is inside the digest yet clean.
  printf 'disposable fixture for scripts/tests/test_agent_gate_tree_integrity.sh\n' \
                              > "$root/.agent-gate-tree-selftest-fixture"
  printf 'hello\n'            > "$root/README.md"
  printf 'lock v1\n'          > "$root/Cargo.lock"
  printf 'docs body\n'        > "$root/NOTES.md"
  printf 'target/\n*.log\n.agent-gate-summary.txt\n.agent-gate-lite-summary.txt\n.agent-gate-delta-summary.txt\nignored-dir/\n' \
                              > "$root/.gitignore"
  # `${1+"$@"}` (never a bare "$@"): expanding an EMPTY "$@" under `set -u` on bash 3.2 —
  # the floor this script declares — is an unbound-variable error (#2926 review B8).
  ( cd "$root" && git init -q ${1+"$@"} . && git add -A && git "${GIT_ID[@]}" commit -qm init ) >/dev/null 2>&1
  # A LOCAL bare `origin` holding this fixture's own commit as `main` (#3544): the gate's
  # component-set pre-flight fetches origin/main and FAILS CLOSED in the certifying modes
  # when the baseline is unobtainable, so a fixture with no remote would now exit at that
  # pre-flight instead of exercising the tree-integrity guard under test. A path remote
  # keeps the fetch REAL and the fixture hermetic (no network), and pushing this very
  # commit makes origin/main an ancestor of HEAD with an identical component set — so the
  # pre-flight PASSes and every case below still measures what it says it measures.
  git init -q --bare "$root.origin.git" >/dev/null 2>&1
  git -C "$root.origin.git" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
  ( cd "$root" \
      && git remote add origin "$root.origin.git" \
      && git push -q origin HEAD:refs/heads/main ) >/dev/null 2>&1
  printf '%s\n' "$root"
}

# A stub `cargo` that (a) always succeeds so no component ever compiles, and (b) — when
# FAKE_CARGO_MUTATE names a file — MUTATES THE CHECKOUT while the `fmt` component is
# running. That is the field shape exactly: a second writer edits the worktree while a
# component executes, with no sleep and no race.
STUBBIN="$tmp/stubbin"
mkdir -p "$STUBBIN"
#
# The mid-run edit shapes it can perform, all inside the `fmt` component:
#   FAKE_CARGO_MUTATE  — append to a file (the dominant shape)
#   FAKE_CARGO_CREATE  — create a NEW untracked file (the #2926 F3 shape: an untracked
#                        `…/Cargo.lock` appearing mid-run must never take the carve-out)
#   FAKE_CARGO_RM      — delete a file (the F3 near-miss: a mid-run `rm Cargo.lock`)
#   FAKE_CARGO_INPLACE — overwrite a file's bytes keeping its SIZE, then restore its MTIME
#                        from FAKE_CARGO_INPLACE_REF (the F4 shape: the only edit a
#                        size+mtime record cannot see, so it needs the content hash)
cat > "$STUBBIN/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = fmt ]; then
  [ -n "${FAKE_CARGO_MUTATE:-}" ] && printf 'mid-run edit\n' >> "$FAKE_CARGO_MUTATE"
  if [ -n "${FAKE_CARGO_CREATE:-}" ]; then
    for _p in $FAKE_CARGO_CREATE; do
      mkdir -p "$(dirname "$_p")"
      printf 'created mid-run\n' > "$_p"
    done
  fi
  [ -n "${FAKE_CARGO_RM:-}" ] && rm -f "$FAKE_CARGO_RM"
  if [ -n "${FAKE_CARGO_INPLACE:-}" ]; then
    cp -p "$FAKE_CARGO_INPLACE" "$FAKE_CARGO_INPLACE_REF"
    _n=$(wc -c < "$FAKE_CARGO_INPLACE" | tr -d ' ')
    # same byte count, different bytes
    _i=0; : > "$FAKE_CARGO_INPLACE"
    while [ "$_i" -lt "$_n" ]; do printf 'Z' >> "$FAKE_CARGO_INPLACE"; _i=$(( _i + 1 )); done
    touch -r "$FAKE_CARGO_INPLACE_REF" "$FAKE_CARGO_INPLACE"
    rm -f "$FAKE_CARGO_INPLACE_REF"
  fi
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
# `${1+"$@"}` (never a bare "$@" or "${@:2}"): on bash 3.2 — the floor agent-gate.sh
# declares, and macOS's /bin/bash — expanding an EMPTY "$@"/"${@:2}" under `set -u` is an
# unbound-variable error, and the common call passes no extra env (#2926 review B8).
capture_identity() {
  local repo="$1" field="$2"; shift 2
  local raw
  raw=$( cd "$repo" && env ${1+"$@"} \
           AGENT_GATE_SUMMARY_FILE="$tmp/capture-sentinel.txt" \
           AGENT_GATE_TREE_SELFTEST=capture \
           bash "$repo/scripts/agent-gate.sh" 2>/dev/null )
  printf '%s\n' "$raw" | sed -n "s/.*[ =]${field}=\([^ ]*\).*/\1/p" | head -1
}
digest_of() { local r="$1"; shift; capture_identity "$r" digest ${1+"$@"}; }

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
# A file LARGER than the cap in force (the floor is 4096 bytes — see F4 below), so the
# size+mtime fallback genuinely engages and must be disclosed.
mkbig() { # mkbig <path> <bytes>
  local i=0
  : > "$1"
  while [ "$i" -lt "$2" ]; do printf '0123456789abcdef' >> "$1"; i=$(( i + 16 )); done
}
mkbig "$r1/big-untracked.bin" 8192
cap_out=$( cd "$r1" && env AGENT_GATE_SUMMARY_FILE="$tmp/cap-sentinel.txt" \
             AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_TREE_HASH_CAP_BYTES=4096 \
             bash "$r1/scripts/agent-gate.sh" 2>/dev/null )
if printf '%s' "$cap_out" | grep -q 'cap-line=tree-hash-cap: 4096 bytes (1 untracked file(s) recorded by size+mtime)'; then
  ok "cap: a non-default AGENT_GATE_TREE_HASH_CAP_BYTES and the fallback use are STAMPED"
else
  bad "cap: tree-hash-cap: stamp missing/incorrect"
  printf '%s\n' "$cap_out" | sed -n 's/^tree-selftest: cap-line=/  got: /p'
fi

# --- F4: the cap is FLOORED, and the normalization is stamped ---------------------
# Rejecting only non-numeric input accepted 0 and 1, at which EVERY untracked file — not
# one oversized blob — falls back to size+mtime, so the "can only weaken for one oversized
# blob" claim was false. Values below the floor are clamped, and the clamp is disclosed.
cap_field() { # cap_field <repo> <cap-value> <field>  (field: cap-line | fallbacks)
  ( cd "$1" && env AGENT_GATE_SUMMARY_FILE="$tmp/cap-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_TREE_HASH_CAP_BYTES="$2" \
      bash "$1/scripts/agent-gate.sh" 2>/dev/null ) \
    | sed -n "s/^tree-selftest: $3=//p;s/.*[ ]$3=\([^ ]*\).*/\1/p" | head -1
}
for lowcap in 0 1 4095; do
  got=$(cap_field "$r1" "$lowcap" cap-line)
  case "$got" in
    "tree-hash-cap: 4096 bytes (clamped from $lowcap to the 4096-byte floor)"*)
      ok "F4: cap=$lowcap is clamped to the 4096-byte floor and the clamp is STAMPED" ;;
    *)
      bad "F4: cap=$lowcap was not clamped/stamped (got '$got')" ;;
  esac
done
got=$(cap_field "$r1" "not-a-number" cap-line)
case "$got" in
  "tree-hash-cap: 8388608 bytes (invalid 'not-a-number' → default)"*)
    ok "F4: a non-numeric cap falls back to the default and the rejection is STAMPED" ;;
  *) bad "F4: a non-numeric cap is not stamped (got '$got')" ;;
esac
got=$(cap_field "$r1" 99999999999999999999999 cap-line)
case "$got" in
  "tree-hash-cap: 8388608 bytes (out-of-range "*)
    ok "F4: an out-of-range cap falls back to the default and is STAMPED (no arithmetic error)" ;;
  *) bad "F4: an out-of-range cap is not normalized (got '$got')" ;;
esac
# …and the CONTROL: the default cap stamps NOTHING when no fallback is used, so the line
# means "this capture was weakened", not "the gate ran".
rm -f "$r1/big-untracked.bin"
got=$(cap_field "$r1" 8388608 cap-line)
if [ "$got" = "<none>" ]; then
  ok "F4 control: the default cap with no fallback stamps no tree-hash-cap: line"
else
  bad "F4 control: the default cap stamped '$got'"
fi
# …and at the floor, an ORDINARY small untracked file is CONTENT-HASHED, not size+mtime:
# the weakening the clamp exists to prevent, measured at the capture.
printf 'a small ordinary untracked file\n' > "$r1/small-untracked.txt"
fb=$(cap_field "$r1" 1 fallbacks)
if [ "$fb" = 0 ]; then
  ok "F4: at cap=1 (clamped) an ordinary small untracked file is content-hashed (0 size+mtime fallbacks)"
else
  bad "F4: a low cap pushed ordinary untracked files onto the size+mtime fallback ($fb file(s))"
fi
rm -f "$r1/small-untracked.txt"

# --- C2: a manifest TRUNCATED after the H record must never compare EQUAL ---------
# ENOSPC on $TMPDIR during a 40-60 minute gate truncates the manifest write. Validating
# only the FIRST record accepted the short file, and two truncations sharing the same
# byte-identical `H<TAB><head>` prefix then compared EQUAL — a mutation to a
# later-sorted path passed as `tree-integrity: PASS`. The manifest now carries an
# `N<TAB><count>` TRAILER that a truncation necessarily loses.
r_t=$(mkrepo trunc-repo)
t_head=$( cd "$r_t" && git rev-parse HEAD )
mT1="$tmp/trunc-1"; mT2="$tmp/trunc-2"
printf 'zz\n' > "$r_t/zz-untracked.txt"          # sorts AFTER the H record
dT1=$(capture_identity "$r_t" digest AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$mT1")
printf 'zz mutated\n' >> "$r_t/zz-untracked.txt"
dT2=$(capture_identity "$r_t" digest AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$mT2")

# the production manifest carries the trailer, and its count is the body count
last_rec=$(tr '\0' '\n' < "$mT1" | tail -1)
body_n=$(tr '\0' '\n' < "$mT1" | sed '$d' | grep -c '^[TU]	')
if [ "$last_rec" = "$(printf 'N\t%s' "$body_n")" ]; then
  ok "C2: the production manifest ends with the N trailer and its count matches the body ($body_n)"
else
  bad "C2: manifest trailer missing/mismatched (last='$last_rec' body=$body_n)"
fi

# truncate BOTH manifests immediately after the H record — the ENOSPC shape
h_bytes=$(( 2 + ${#t_head} + 1 ))
head -c "$h_bytes" "$mT1" > "$mT1.trunc"
head -c "$h_bytes" "$mT2" > "$mT2.trunc"
if [ "$dT1" != "$dT2" ] && cmp -s "$mT1.trunc" "$mT2.trunc"; then
  ok "C2: the two truncations are BYTE-IDENTICAL although the trees differ — the false-PASS shape is reproduced"
else
  bad "C2: the truncated-prefix case was not reproduced (d1=$dT1 d2=$dT2)"
fi

validate_manifest() { # validate_manifest <repo> <file> <nul|nl> <head> <count> -> yes|no
  ( cd "$1" && env AGENT_GATE_SUMMARY_FILE="$tmp/validate-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=validate-manifest \
      AGENT_GATE_TREE_SELFTEST_VALIDATE="$2|$3|$4|$5" \
      bash "$1/scripts/agent-gate.sh" 2>/dev/null ) | sed -n 's/^tree-selftest: manifest-ok=//p' | head -1
}
v_intact=$(validate_manifest "$r_t" "$mT1" nul "$t_head" "$body_n")
v_trunc=$(validate_manifest "$r_t" "$mT1.trunc" nul "$t_head" "$body_n")
if [ "$v_intact" = yes ]; then
  ok "C2: the validator ACCEPTS a complete production manifest (not hardwired to reject)"
else
  bad "C2: the validator rejected an intact manifest (got '$v_intact')"
fi
if [ "$v_trunc" = no ]; then
  ok "C2: the validator REJECTS the same manifest truncated after the H record"
else
  bad "C2: a truncated manifest validated (got '$v_trunc') — the false PASS is still reachable"
fi
# a trailer that disagrees with the body count is rejected too (a partial re-write)
printf 'H\t%s\0T\tabc\t100644\tp\0N\t5\0' "$t_head" > "$tmp/trunc-badcount"
v_bad=$(validate_manifest "$r_t" "$tmp/trunc-badcount" nul "$t_head" 5)
printf 'H\t%s\0T\tabc\t100644\tp\0N\t1\0' "$t_head" > "$tmp/trunc-goodcount"
v_good=$(validate_manifest "$r_t" "$tmp/trunc-goodcount" nul "$t_head" 1)
if [ "$v_bad" = no ] && [ "$v_good" = yes ]; then
  ok "C2: the trailer count must equal the records actually read (5-claimed/1-present rejected, 1/1 accepted)"
else
  bad "C2: the trailer count is not enforced (bad='$v_bad' good='$v_good')"
fi
# and the same for the .report view, which the failure-naming path parses
if [ -f "$mT1.report" ]; then
  v_rep=$(validate_manifest "$r_t" "$mT1.report" nl "$t_head" "$body_n")
  sed '$d' "$mT1.report" > "$mT1.report.trunc"          # drop the trailer line
  v_rep_t=$(validate_manifest "$r_t" "$mT1.report.trunc" nl "$t_head" "$body_n")
  if [ "$v_rep" = yes ] && [ "$v_rep_t" = no ]; then
    ok "C2: the newline-framed .report view is held to the same trailer rule (intact yes / truncated no)"
  else
    bad "C2: the .report view is not trailer-validated (intact='$v_rep' truncated='$v_rep_t')"
  fi
else
  bad "C2: the capture hook did not emit a .report view — the case was not exercised"
fi
rm -f "$r_t/zz-untracked.txt"

# --- the summary carve-out canonicalizes a NOT-YET-CREATED parent directory --------
# _tree_canon_rel returned 1 when the summary path's parent did not exist, silently
# DISARMING the carve-out (#2926 review). Benign only while the sentinel write also
# fails — the day anything mkdir -p's the parent it is a guaranteed false FAIL.
capture_exclude_rel() { # capture_exclude_rel <repo> <pinned-summary-path>
  ( cd "$1" && env AGENT_GATE_SUMMARY_FILE="$2" AGENT_GATE_TREE_SELFTEST=capture \
      bash "$1/scripts/agent-gate.sh" 2>/dev/null ) \
    | sed -n 's/^tree-selftest: exclude-rel=//p' | head -1
}
mkdir -p "$r_t/existing"
xr_have=$(capture_exclude_rel "$r_t" "$r_t/existing/sum.txt")
xr_missing=$(capture_exclude_rel "$r_t" "$r_t/not-yet/deeper/sum.txt")
xr_outside=$(capture_exclude_rel "$r_t" "$tmp/outside-sum.txt")
if [ "$xr_have" = "existing/sum.txt" ]; then
  ok "canon: an in-repo summary path with an EXISTING parent canonicalizes to $xr_have"
else
  bad "canon: existing-parent canonicalization wrong (got '$xr_have')"
fi
if [ "$xr_missing" = "not-yet/deeper/sum.txt" ]; then
  ok "canon: a summary path whose parent does NOT exist still canonicalizes ($xr_missing) — the carve-out stays armed"
else
  bad "canon: a missing parent disarmed the carve-out (got '$xr_missing')"
fi
if [ -z "$xr_outside" ]; then
  ok "canon: a summary path OUTSIDE the repo root is still not excluded (exclusion stays narrow)"
else
  bad "canon: an out-of-repo summary path was excluded as '$xr_outside'"
fi

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
  # …and the block must not STAMP the sha it just refused to certify. This case used to
  # stop at assert_named_fail, and that gap is exactly why the H2 labelling shipped on the
  # boundary path only while the TERMINAL path kept naming the post-mutation sha unlabelled
  # (#2926 review J1). The full cross-path contract lives in
  # scripts/tests/test_agent_gate_tree_provenance.sh; this is its guard on THIS case.
  if grep -q "^commit: ${head_before:0:7} " "$sum" \
     && grep -q '^commit: .*(VERIFIED START — ' "$sum" \
     && ! grep -q "^commit: ${head_after:0:7} " "$sum"; then
    ok "commit: the TERMINAL detection stamps the VERIFIED START ${head_before:0:7}, labelled — never the post-mutation ${head_after:0:7}"
  else
    bad "commit: the terminal block stamps an unlabelled/post-mutation sha (expected the labelled ${head_before:0:7})"
    grep -E '^commit:|^tree-end:' "$sum" 2>/dev/null
  fi
else
  bad "commit: the fixture did not actually commit — the case was not exercised"
fi

# --- H2: the boundary FAIL block names the identity the run EXECUTED against -----
# The whole thesis of #2926 is "never stamp a sha you did not verify". A MAIN-lane boundary
# detection sets tree-end from the POST-mutation probe before publishing, so the ONE block a
# triager reads after a mid-run mutation used to carry a `commit:` line naming the sha the
# run did NOT execute against. The block must name the VERIFIED START, label it, and keep
# the post-mutation observation on its own labelled line (#2926 review H2).
sum="$tmp/hook-bcommit.txt"; out="$tmp/hook-bcommit.out"
b_before=$( cd "$r2" && git rev-parse HEAD )
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=boundary \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_SELFTEST_COMMIT=1 \
    GIT_AUTHOR_NAME=gate GIT_AUTHOR_EMAIL=gate@example.invalid \
    GIT_COMMITTER_NAME=gate GIT_COMMITTER_EMAIL=gate@example.invalid \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
b_after=$( cd "$r2" && git rev-parse HEAD )
b_stamp=$(sed -n 's/^commit: \([^ ]*\).*/\1/p' "$sum" | head -1)
b_end=$(sed -n 's/^tree-end: \([^ ]*\) .*/\1/p' "$sum" | head -1)
if [ "$b_before" != "$b_after" ]; then
  ok "H2: the fixture really moved HEAD at the boundary (${b_before:0:7}→${b_after:0:7}) — start and post-mutation identities differ"
else
  bad "H2: HEAD did not move — the start-vs-post-mutation distinction was not exercised"
fi
assert_named_fail "H2 (boundary FAIL block)" "$sum" "$rc"
if [ -n "$b_stamp" ] && [ "$b_stamp" = "${b_before:0:7}" ] && [ "$b_stamp" != "${b_after:0:7}" ]; then
  ok "H2: the boundary block's commit: names the VERIFIED START ($b_stamp), not the post-mutation ${b_after:0:7}"
else
  bad "H2: commit: is '$b_stamp', expected the verified start ${b_before:0:7} (the block names a sha the run never executed against)"
  grep -E '^commit:|^tree-' "$sum" 2>/dev/null
fi
if grep -q '^commit: .*(VERIFIED START — the identity this run executed against; the tree MUTATED mid-run, see tree-end: for the post-mutation observation)$' "$sum"; then
  ok "H2: the commit: line LABELS itself as the verified start and points at tree-end: for the post-mutation identity"
else
  bad "H2: the commit: line does not disambiguate start from post-mutation"
  grep '^commit: ' "$sum" 2>/dev/null
fi
if [ -n "$b_end" ] && [ "$b_end" = "${b_after:0:12}" ] \
   && grep -q '^tree-end: .*(POST-MUTATION observation — NOT the identity this run executed against)$' "$sum"; then
  ok "H2: tree-end: carries the post-mutation identity (${b_after:0:12}), explicitly labelled as such"
else
  bad "H2: tree-end: is '$b_end' / unlabelled — the post-mutation identity is not separately named"
  grep '^tree-end: ' "$sum" 2>/dev/null
fi

# --- C1: a HEAD move BETWEEN the terminal capture and the emit -------------------
# THE ORIGINAL #2926 DEFECT, in its last surviving hiding place. The guard's terminal
# capture is authoritative — but the block's `commit:`/`dirty:` stamp used to be a FRESH
# `git rev-parse --short HEAD` / `git status --porcelain` run AFTER it, so a HEAD move
# landing in that window produced a CERTIFIED block naming a sha the guard never
# verified. The stamp must come from the verified capture.
sum="$tmp/hook-postfinalize.txt"; out="$tmp/hook-postfinalize.out"
pf_before=$( cd "$r2" && git rev-parse HEAD )
( cd "$r2" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=postfinalize \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_SELFTEST_COMMIT=1 \
    GIT_AUTHOR_NAME=gate GIT_AUTHOR_EMAIL=gate@example.invalid \
    GIT_COMMITTER_NAME=gate GIT_COMMITTER_EMAIL=gate@example.invalid \
    bash "$r2/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
pf_after=$( cd "$r2" && git rev-parse HEAD )
pf_stamp=$(sed -n 's/^commit: \([^ ]*\).*/\1/p' "$sum" | head -1)
pf_dirty=$(sed -n 's/^commit: .*dirty: \([^ ]*\).*/\1/p' "$sum" | head -1)
pf_endd=$(sed -n 's/^tree-end: .*dirty: \([^ ]*\).*/\1/p' "$sum" | head -1)
if [ "$pf_before" != "$pf_after" ]; then
  ok "C1: the fixture really moved HEAD between the terminal capture and the emit (${pf_before:0:7}→${pf_after:0:7})"
else
  bad "C1: HEAD did not move — the post-finalize window was not exercised"
fi
if [ -n "$pf_stamp" ] && [ "$pf_stamp" = "${pf_before:0:7}" ]; then
  ok "C1: the emitted commit: names the sha the TERMINAL CAPTURE VERIFIED ($pf_stamp), not the moved HEAD"
else
  bad "C1: commit: is '$pf_stamp', expected the verified ${pf_before:0:7} (stamped at emit time?)"
fi
if [ -n "$pf_stamp" ] && [ "$pf_stamp" != "${pf_after:0:7}" ]; then
  ok "C1: the block does NOT certify the post-move sha ${pf_after:0:7} — no unverified commit is ever named"
else
  bad "C1: the block certified ${pf_after:0:7}, a sha the guard never verified (rc=$rc)"
  grep -E '^commit:|^tree-|^RESULT:' "$sum" 2>/dev/null
fi
if [ -n "$pf_dirty" ] && [ "$pf_dirty" = "$pf_endd" ]; then
  ok "C1: the commit line's dirty flag is the verified capture's ($pf_dirty), not a fresh porcelain read"
else
  bad "C1: dirty: '$pf_dirty' disagrees with the verified tree-end dirty '$pf_endd'"
fi
# …and the CONTROL: on an unmutated run the same stamp is the real, current sha — the
# capture-derived stamp is not a constant, and not a placeholder.
ctl_head=$( cd "$r2" && git rev-parse HEAD )
ctl_stamp=$(sed -n 's/^commit: \([^ ]*\).*/\1/p' "$tmp/hook-clean.txt" | head -1)
ctl_branch=$(sed -n 's/^commit: .* branch: \([^ ]*\).*/\1/p' "$tmp/hook-clean.txt" | head -1)
if [ -n "$ctl_stamp" ] && [ "$ctl_stamp" != selftest ] && [ "$ctl_stamp" != unverified ]; then
  ok "C1 control: an unmutated run stamps a REAL capture-derived sha ($ctl_stamp), not a constant"
else
  bad "C1 control: the unmutated run's commit: is '$ctl_stamp' — the stamp is not capture-derived"
fi
if [ "$ctl_branch" = master ] || [ "$ctl_branch" = main ]; then
  ok "C1 control: the branch label is captured inside the window ($ctl_branch)"
else
  bad "C1 control: unexpected branch label '$ctl_branch'"
fi
[ -n "$ctl_head" ] || bad "C1 control: fixture HEAD unreadable"

# --- the hash-cap disclosure counts FILES, not CAPTURES ---------------------------
# A run takes at least two captures (start + terminal). Summing their fallback counts
# reported ONE oversized untracked file present all run as "2 untracked file(s)" (#2926
# review). The figure must be the file count, whatever the number of captures.
r_cap=$(mkrepo capcount-repo)
mkbig "$r_cap/one-big.bin" 8192
sum="$tmp/hook-capcount.txt"
( cd "$r_cap" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=clean \
    AGENT_GATE_TREE_HASH_CAP_BYTES=4096 \
    bash "$r_cap/scripts/agent-gate.sh" >"$tmp/hook-capcount.out" 2>&1 ); rc=$?
cap_line=$(grep '^tree-hash-cap: ' "$sum" 2>/dev/null | head -1)
if [ "$rc" -eq 0 ] && [ "$cap_line" = "tree-hash-cap: 4096 bytes (1 untracked file(s) recorded by size+mtime)" ]; then
  ok "cap: ONE oversized untracked file is reported ONCE across the start AND terminal captures"
else
  bad "cap: the fallback count is per-capture, not per-file (rc=$rc, got '$cap_line')"
fi
# …and the boundary-FAIL block — which assembles its own meta — must disclose it too.
# It used to hand-assemble tree-start/tree-end/tree-integrity and DROP tree-hash-cap,
# hiding the weakened capture in exactly the degraded case where it matters (#2926 review).
mkbig "$r_cap/one-big.bin" 8192
sum="$tmp/hook-capfail.txt"
( cd "$r_cap" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=boundary \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_HASH_CAP_BYTES=4096 \
    bash "$r_cap/scripts/agent-gate.sh" >"$tmp/hook-capfail.out" 2>&1 ); rc=$?
assert_named_fail "cap + boundary FAIL" "$sum" "$rc"
if grep -q '^tree-hash-cap: 4096 bytes' "$sum" 2>/dev/null; then
  ok "cap: the component-boundary FAIL block discloses the weakened capture (tree-hash-cap present)"
else
  bad "cap: the boundary FAIL block dropped tree-hash-cap: — the degraded capture is invisible"
  grep -E '^tree-' "$sum" 2>/dev/null
fi
( cd "$r_cap" && git checkout -q -- README.md )
rm -f "$r_cap/one-big.bin"

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

# --- EVERY settled lockfile is named (a workspace can re-resolve several) ---------
# The spec scenario "every settled lockfile is named in the stamp" was previously
# untested because the fixture had only ONE lockfile, leaving the naming LOOP uncovered
# (#2926 review). A second, nested lockfile exercises it.
mkdir -p "$r3/member"
printf 'member lock v1\n' > "$r3/member/Cargo.lock"
( cd "$r3" && git add -A && git "${GIT_ID[@]}" commit -qm 'second lockfile' ) >/dev/null 2>&1
sum="$tmp/hook-lock-multi.txt"
( cd "$r3" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE="Cargo.lock member/Cargo.lock" \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-lock-multi.out" 2>&1 ); rc=$?
lock_line=$(sed -n 's/^tree-integrity: PASS (lockfile-settled: \(.*\))$/\1/p' "$sum" | head -1)
if [ "$rc" -eq 0 ] && grep -q '^RESULT: PASS' "$sum" \
   && printf '%s' "$lock_line" | grep -q 'Cargo.lock ' \
   && printf '%s' "$lock_line" | grep -q 'member/Cargo.lock '; then
  ok "lockfile: BOTH settled lockfiles are named in the stamp ($lock_line)"
else
  bad "lockfile: the multi-lockfile stamp under-reports (rc=$rc, stamp='$lock_line')"
  grep -E 'tree-integrity|RESULT' "$sum" 2>/dev/null
fi
if [ "$(printf '%s\n' "$lock_line" | grep -o '→' | wc -l | tr -d ' ')" = 2 ]; then
  ok "lockfile: each named lockfile carries its own before→after hash pair"
else
  bad "lockfile: the multi-lockfile stamp does not carry one before→after pair per lockfile"
fi
( cd "$r3" && git checkout -q -- Cargo.lock member/Cargo.lock )

# --- the changed-path list is TRUNCATED at 5 with an explicit remainder count -------
# The spec requires the named line to list the changed paths "truncated with an explicit
# count when numerous"; the >5 branch had no test at all (#2926 review), so a renderer
# that dropped the remainder — or listed everything — would have gone unnoticed.
r_many=$(mkrepo manypaths-repo)
many=""
for i in 1 2 3 4 5 6 7; do printf 'body %s\n' "$i" > "$r_many/f$i.txt"; many="${many:+$many }f$i.txt"; done
( cd "$r_many" && git add -A && git "${GIT_ID[@]}" commit -qm 'seven files' ) >/dev/null 2>&1
sum="$tmp/hook-many.txt"
( cd "$r_many" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE="$many" \
    bash "$r_many/scripts/agent-gate.sh" >"$tmp/hook-many.out" 2>&1 ); rc=$?
assert_named_fail "truncation (7 changed paths)" "$sum" "$rc"
changed_list=$(sed -n 's/^tree-integrity: FAIL (tree-mutated-midrun; [^;]*; changed: \(.*\); detected-after-component: .*)$/\1/p' "$sum" | head -1)
n_named=$(printf '%s\n' "$changed_list" | grep -o 'f[0-9]\.txt' | wc -l | tr -d ' ')
if [ "$n_named" = 5 ] && printf '%s' "$changed_list" | grep -q '(+2 more)'; then
  ok "truncation: 7 changed paths render as 5 named + '(+2 more)' ($changed_list)"
else
  bad "truncation: the >5-path branch renders wrongly ($n_named named, list='$changed_list')"
fi
# …and the CONTROL: at or below the limit NOTHING is truncated (the count is a remainder,
# not a constant).
( cd "$r_many" && git checkout -q -- . )
sum="$tmp/hook-many5.txt"
( cd "$r_many" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
    AGENT_GATE_TREE_SELFTEST_MUTATE="f1.txt f2.txt f3.txt f4.txt f5.txt" \
    bash "$r_many/scripts/agent-gate.sh" >"$tmp/hook-many5.out" 2>&1 ); rc=$?
assert_named_fail "truncation control (exactly 5 changed paths)" "$sum" "$rc"
changed5=$(sed -n 's/^tree-integrity: FAIL (tree-mutated-midrun; [^;]*; changed: \(.*\); detected-after-component: .*)$/\1/p' "$sum" | head -1)
if [ "$(printf '%s\n' "$changed5" | grep -o 'f[0-9]\.txt' | wc -l | tr -d ' ')" = 5 ] \
   && ! printf '%s' "$changed5" | grep -q 'more)'; then
  ok "truncation control: exactly 5 changed paths are ALL named with no remainder marker"
else
  bad "truncation control: a 5-path list was truncated or under-reported ('$changed5')"
fi

# --- B1: A FAILING HASH TOOL MUST NOT CERTIFY (the headline regression) -----------
# _tree_identity used to print whatever the hash tool produced and return 0. With an
# empty digest, `IFS=$'\t' read` (tab is IFS-WHITESPACE) collapsed the empty field and
# bound `digest` to the fallbacks value `0`; the digest-ONLY comparison then matched and
# the run stamped `tree-integrity: PASS` on a mutated tree — with a self-contradicting
# `tree-start: … dirty: no` / `tree-end: … dirty: yes` in the very same block.
BADHASH="$tmp/badhash"; mkdir -p "$BADHASH"
printf '#!/bin/sh\nexit 3\n' > "$BADHASH/sha256sum"; chmod +x "$BADHASH/sha256sum"
printf '#!/bin/sh\nexit 3\n' > "$BADHASH/shasum";    chmod +x "$BADHASH/shasum"
# The tree is ALREADY dirty before the run, so the mid-run append moves NEITHER the head
# nor the dirty flag: the digest is the only signal, and an unvalidated (empty) digest is
# the whole failure. This is the strict form of the case.
printf 'pre-existing modification\n' >> "$r3/README.md"
sum="$tmp/hook-badhash.txt"
( cd "$r3" && PATH="$BADHASH:$PATH" env AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_SELFTEST=terminal AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-badhash.out" 2>&1 ); rc=$?
miss=()
grep -q '^RESULT: FAIL' "$sum" 2>/dev/null || miss+=("RESULT:-FAIL")
grep -q '^RESULT: PASS' "$sum" 2>/dev/null && miss+=("UNEXPECTED-RESULT:-PASS")
grep -q '^tree-integrity: FAIL (' "$sum" 2>/dev/null || miss+=("named-tree-integrity-FAIL")
grep -q '^tree-integrity: PASS' "$sum" 2>/dev/null && miss+=("UNEXPECTED-tree-integrity:-PASS")
[ "$rc" -ne 0 ] || miss+=("non-zero-exit(got $rc)")
if [ "${#miss[@]}" -eq 0 ]; then
  ok "B1: a FAILING hash tool + a mutated tree does NOT certify (capture failure is fail-closed)"
else
  bad "B1: a run whose hash tool failed still certified: ${miss[*]}"
  grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
fi
# …and the same with NO mutation: an unvalidatable capture can never be a PASS either,
# because an unproven tree is exactly what this guard exists to refuse.
sum="$tmp/hook-badhash-clean.txt"
( cd "$r3" && PATH="$BADHASH:$PATH" env AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_SELFTEST=clean \
    bash "$r3/scripts/agent-gate.sh" >"$tmp/hook-badhash-clean.out" 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q '^RESULT: FAIL' "$sum" && ! grep -q '^tree-integrity: PASS' "$sum"; then
  ok "B1: an UNMUTATED run whose hash tool fails also refuses to certify (no unproven PASS)"
else
  bad "B1: an unvalidatable capture certified on an unmutated tree (rc=$rc)"
  grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
fi
# No emitted block may claim PASS while its own start/end lines disagree.
for f in "$tmp/hook-badhash.txt" "$tmp/hook-badhash-clean.txt" "$tmp/hook-clean.txt"; do
  s_d=$(sed -n 's/^tree-start: .*dirty: \([^ ]*\).*/\1/p' "$f" | head -1)
  e_d=$(sed -n 's/^tree-end: .*dirty: \([^ ]*\).*/\1/p' "$f" | head -1)
  if grep -q '^tree-integrity: PASS' "$f" 2>/dev/null && [ -n "$s_d" ] && [ "$s_d" != "$e_d" ]; then
    bad "B1: $(basename "$f") stamps PASS while tree-start/tree-end disagree (dirty $s_d vs $e_d)"
  else
    ok "B1: $(basename "$f") never stamps PASS over a self-contradicting start/end pair"
  fi
done
( cd "$r3" && git checkout -q -- README.md )

# --- B5: the mutating hook REFUSES a checkout that is not a disposable fixture ----
r5=$(mkrepo guard-repo)
rm -f "$r5/.agent-gate-tree-selftest-fixture"
before_bytes=$(wc -c < "$r5/README.md" | tr -d ' ')
head5_before=$( cd "$r5" && git rev-parse HEAD )
( cd "$r5" && env AGENT_GATE_SUMMARY_FILE="$tmp/hook-guard.txt" \
    AGENT_GATE_TREE_SELFTEST=terminal AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    AGENT_GATE_TREE_SELFTEST_COMMIT=1 \
    bash "$r5/scripts/agent-gate.sh" >"$tmp/hook-guard.out" 2>&1 ); rc=$?
after_bytes=$(wc -c < "$r5/README.md" | tr -d ' ')
head5_after=$( cd "$r5" && git rev-parse HEAD )
if [ "$rc" -eq 2 ] && [ "$before_bytes" = "$after_bytes" ] && [ "$head5_before" = "$head5_after" ] \
   && grep -q 'refusing to write into a live checkout' "$tmp/hook-guard.out"; then
  ok "B5: the mutating self-test hook refuses (exit 2) a checkout without the disposable-fixture marker — nothing written, no commit"
else
  bad "B5: the hook wrote into / committed to a non-fixture checkout (rc=$rc bytes $before_bytes->$after_bytes head $head5_before->$head5_after)"
fi
# …and the control: WITH the marker the very same invocation runs (and fails closed on
# the mutation), so the guard is a fixture check, not a disabled hook.
printf 'disposable\n' > "$r5/.agent-gate-tree-selftest-fixture"
( cd "$r5" && git add -A && git "${GIT_ID[@]}" commit -qm marker ) >/dev/null 2>&1
sum="$tmp/hook-guard-ok.txt"
( cd "$r5" && env AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_SELFTEST=terminal AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r5/scripts/agent-gate.sh" >"$tmp/hook-guard-ok.out" 2>&1 ); rc=$?
assert_named_fail "B5 control (marker present)" "$sum" "$rc"

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
# The property is that the PARTIAL VERDICT is not published: the gate emits that token in
# exactly two anchored shapes (`RESULT: PARTIAL` from the terminal renderer, and
# `mode: PARTIAL (--only ...)` from the --only assembly), and a mutated run must publish
# neither. Matched at those two anchors rather than as a bare substring anywhere in the
# block: an unanchored grep also fires on any DIAGNOSTIC that uses the word -- the #3800
# `disk-exhaustion:` line declares a MID-RUN PARTIAL WINDOW at this very site -- which would
# make a prose word in an attribution read as a verdict. Narrower, and strictly closer to
# the property.
if grep -qE '^(RESULT|OVERALL): PARTIAL|^mode: PARTIAL' "$sum"; then
  bad "--only: a mutated PARTIAL run must NOT report PARTIAL"
  grep -nE '^(RESULT|OVERALL): PARTIAL|^mode: PARTIAL' "$sum"
else
  ok "--only: a mutated run reports FAIL, never PARTIAL (neither RESULT:/OVERALL: nor mode:)"
fi
# #3800: this is the ONE MID-RUN emit site and the ONE component table not rendered through
# _fm_summary_line. It carries the attribution line, and BOTH halves declare their partial
# window -- because `tree-integrity: FAIL` is itself reachable from ENOSPC (the capture
# manifest is written into $LOG_DIR and TREE_CAPTURE_FAIL_REASON is a fixed constant that
# cannot name disk), so this block must not be the one that hides the host cause.
de_line=$(grep -c '^disk-exhaustion:' "$sum" 2>/dev/null | tr -d ' ')
if [ "$de_line" = 1 ]; then
  ok "#3800: the boundary-FAIL block carries exactly one disk-exhaustion: attribution line"
else
  bad "#3800: expected exactly one disk-exhaustion: line in the boundary-FAIL block, found $de_line"
fi
if grep -qE '^disk-exhaustion: .*MID-RUN PARTIAL WINDOW' "$sum" \
   && grep -qE '^disk-exhaustion: .*SUBJECT SET ALSO PARTIAL' "$sum"; then
  ok "#3800: the mid-run attribution declares BOTH partial windows (free-space start->boundary, and the recorded-so-far subject set)"
else
  bad "#3800: the boundary block's attribution does not declare its partial window:"
  grep -E '^disk-exhaustion:' "$sum" 2>/dev/null
fi
# ...and it stays an ATTRIBUTION: the verdict is the tree-integrity FAIL, unchanged.
if grep -q '^RESULT: FAIL$' "$sum"; then
  ok "#3800: the attribution does not change the boundary block's RESULT: FAIL"
else
  bad "#3800: the boundary block's RESULT is not FAIL"
fi
# G3: the MAIN-lane boundary FAIL block is a FULL block, not a stub. It used to carry only
# the four tree lines + detected-after-component, making the one block a reader reaches
# after a mid-run mutation the information-POOREST in the gate — no commit, no datasets,
# no ci-pins, no accelerator/cpu disclosure, no component verdicts. Every other terminal
# block carries them; so must this one.
prov_missing=""
for need in '^commit: [0-9a-f]+ branch: .* dirty: ' '^datasets: ' '^ci-pins: ' \
            '^accelerators: sccache=' '^cpu-budget: wrapper=' \
            '^tree-start: ' '^tree-end: ' '^tree-integrity: FAIL' \
            '^detected-after-component: fmt$' '^fmt: +(PASS|FAIL)' \
            '^components-completed: [0-9]+ of [0-9]+ selected'; do
  grep -qE "$need" "$sum" 2>/dev/null || prov_missing="${prov_missing:+$prov_missing }[$need]"
done
if [ -z "$prov_missing" ]; then
  ok "G3: the boundary-FAIL block carries the full provenance (commit/datasets/ci-pins/accelerators/cpu-budget/tree lines/component table)"
else
  bad "G3: the boundary-FAIL block is missing standard provenance: $prov_missing"
  grep -vE '^(logs|summary-file):' "$sum" 2>/dev/null
fi
# …and the component table is the REAL one: the boundary that detected the mutation is the
# fmt component, whose verdict must be in the table exactly once, in the terminal format.
if [ "$(grep -cE '^fmt: +(PASS|FAIL) \([0-9]+s\)' "$sum" 2>/dev/null | tr -d ' ')" = 1 ]; then
  ok "G3: the partial component table uses the terminal block's own row format, once per component"
else
  bad "G3: the component table row for fmt is missing or malformed"
  grep -E '^fmt:|^components-completed:' "$sum" 2>/dev/null
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

# --- B3: NON-CANONICAL in-repo summary paths are excluded too ---------------------
# git always reports normalized repo-root-relative paths, so a raw prefix strip could
# not match `./x`, `sub/../x` or an absolute path carrying `/./`. Every one of these is
# a path the gate WRITES (sentinel + terminal emit) and writes for the first time AFTER
# the start capture, so a missed carve-out is a GUARANTEED false FAIL, not a rare one.
mkdir -p "$r4/sub"
for spec in "./in-repo-summary.txt" "sub/../in-repo-summary.txt" "ABS/./in-repo-summary.txt" "ABS/sub/../in-repo-summary.txt"; do
  case "$spec" in ABS/*) pinned="$r4/${spec#ABS/}" ;; *) pinned="$spec" ;; esac
  out="$tmp/only-relsum-nc.out"
  ( cd "$r4" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$pinned" \
      bash "$r4/scripts/agent-gate.sh" --only fmt >"$out" 2>&1 ); rc=$?
  if grep -q '^tree-integrity: PASS$' "$r4/in-repo-summary.txt" 2>/dev/null && [ "$rc" -eq 3 ]; then
    ok "B3: a non-canonical in-repo summary path ('$spec') is canonicalized and excluded"
  else
    bad "B3: '$spec' tripped the guard (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$r4/in-repo-summary.txt" 2>/dev/null
  fi
  rm -f "$r4/in-repo-summary.txt"
done
rmdir "$r4/sub" 2>/dev/null || true

# --- B6: a TAB in a changed path must not corrupt the lockfile classification ------
# The `.report` view is parsed with `awk -F'\t'` ($4 = path). With tabs unescaped, an
# untracked file literally named "Cargo.lock<TAB>extra" presents $4 == "Cargo.lock" —
# the NON-FATAL lockfile carve-out fires and the run CERTIFIES a real mutation.
tabpath="$r4/$(printf 'Cargo.lock\textra')"
sum="$tmp/only-tab.txt"; out="$tmp/only-tab.out"
FAKE_CARGO_MUTATE="$tabpath" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
assert_named_fail "B6 (a tabbed path that mimics Cargo.lock in field 4)" "$sum" "$rc"
if grep -q 'lockfile-settled' "$sum" 2>/dev/null; then
  bad "B6: a tabbed path was misclassified as a settled lockfile — the carve-out misfired"
else
  ok "B6: the tabbed path is NOT misclassified as a lockfile (tabs are escaped in the report)"
fi
rm -f "$tabpath"

# --- F3: the lockfile carve-out is TAG- and TRACKING-checked, not path-matched -----
# `case "$p" in Cargo.lock|*/Cargo.lock)` keyed on the PATH ALONE, so any file whose name
# ends `/Cargo.lock` took the NON-FATAL class — including an UNTRACKED one that appeared
# mid-run, i.e. a real mutation certifying as `lockfile-settled`. Admission now requires a
# tracked (`T`) record whose value is a real blob hash and whose path is a blob in the
# START COMMIT.
sum="$tmp/only-lock-untracked.txt"; out="$tmp/only-lock-untracked.out"
FAKE_CARGO_CREATE="$r4/vendor/Cargo.lock" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
assert_named_fail "F3 (an UNTRACKED Cargo.lock appearing mid-run)" "$sum" "$rc"
if grep -q 'lockfile-settled' "$sum" 2>/dev/null; then
  bad "F3: an untracked mid-run Cargo.lock took the non-fatal carve-out — a real mutation certified"
else
  ok "F3: an untracked mid-run vendor/Cargo.lock is FATAL (tag U never takes the carve-out)"
fi
rm -rf "$r4/vendor"

# …the same for a path that merely ENDS in `Cargo.lock` (the suffix impostor): tracked,
# committed, and edited mid-run — it is not a lockfile and must be a normal mutation.
printf 'not a lockfile\n' > "$r4/notCargo.lock"
mkdir -p "$r4/deps"; printf 'also not a lockfile\n' > "$r4/deps/vendored-Cargo.lock"
( cd "$r4" && git add -A && git "${GIT_ID[@]}" commit -qm 'lockfile impostors' ) >/dev/null 2>&1
for impostor in notCargo.lock deps/vendored-Cargo.lock; do
  sum="$tmp/only-lock-impostor.txt"; out="$tmp/only-lock-impostor.out"
  FAKE_CARGO_MUTATE="$r4/$impostor" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
  assert_named_fail "F3 ('$impostor', a suffix impostor)" "$sum" "$rc"
  if grep -q 'lockfile-settled' "$sum" 2>/dev/null; then
    bad "F3: '$impostor' was misclassified as a settled lockfile"
  else
    ok "F3: '$impostor' is NOT a lockfile — it is a normal fatal mutation"
  fi
  ( cd "$r4" && git checkout -q -- "$impostor" )
done

# …and the near-miss variant of the same defect: the lockfile is TRACKED (tag T) but the
# mid-run change is a DELETION, not a re-resolution. A deleted lockfile is a tree mutation.
sum="$tmp/only-lock-deleted.txt"; out="$tmp/only-lock-deleted.out"
FAKE_CARGO_RM="$r4/Cargo.lock" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
assert_named_fail "F3 (a tracked Cargo.lock DELETED mid-run)" "$sum" "$rc"
if grep -q 'lockfile-settled' "$sum" 2>/dev/null; then
  bad "F3: deleting the lockfile mid-run was stamped 'settled' — a lifecycle change is not a re-resolution"
else
  ok "F3: deleting a tracked Cargo.lock mid-run is FATAL (only a real blob hash can settle)"
fi
( cd "$r4" && git checkout -q -- Cargo.lock )

# …and the CONTROL that keeps the carve-out alive: the dominant legitimate shape is a
# lockfile that is CLEAN at the start capture and re-resolved by the gate's own cargo.
# (This is why admission cannot require presence in the START MANIFEST, which lists only
# paths already differing from HEAD — that requirement would kill the carve-out outright.)
sum="$tmp/only-lock-ctl.txt"; out="$tmp/only-lock-ctl.out"
FAKE_CARGO_MUTATE="$r4/Cargo.lock" run_gate "$r4" "$sum" "$out" --only fmt; rc=$?
if grep -qE '^tree-integrity: PASS \(lockfile-settled: Cargo.lock unmodified→[0-9a-f]{12}\)' "$sum" \
   && [ "$rc" -eq 3 ]; then
  ok "F3 control: a CLEAN-at-start tracked Cargo.lock re-resolved mid-run still stamps lockfile-settled"
else
  bad "F3 control: the legitimate lockfile settle no longer certifies (rc=$rc)"
  grep -E '^tree-integrity|^RESULT:' "$sum" 2>/dev/null
fi
( cd "$r4" && git checkout -q -- Cargo.lock )

# --- F4 (real gate): a LOW cap must not weaken detection for ORDINARY untracked files
# The existing no-bypass case mutates a TRACKED file, which is content-hashed whatever the
# cap, so it never exercised the weakening path. Here an UNTRACKED file is rewritten with
# DIFFERENT BYTES OF THE SAME LENGTH and its MTIME IS RESTORED — the one edit a size+mtime
# record cannot see. At an unfloored cap of 1 every untracked file takes that record and
# this mutation is INVISIBLE; with the 4096-byte floor it is content-hashed and detected.
printf 'untracked payload present at the start capture\n' > "$r4/untracked-payload.txt"
sum="$tmp/only-lowcap-untracked.txt"; out="$tmp/only-lowcap-untracked.out"
( cd "$r4" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_HASH_CAP_BYTES=1 \
    FAKE_CARGO_INPLACE="$r4/untracked-payload.txt" \
    FAKE_CARGO_INPLACE_REF="$tmp/inplace-mtime-ref" \
    bash "$r4/scripts/agent-gate.sh" --only fmt >"$out" 2>&1 ); rc=$?
assert_named_fail "F4 (untracked same-size same-mtime edit at cap=1)" "$sum" "$rc"
if grep -q 'changed: untracked-payload.txt' "$sum" 2>/dev/null \
   && grep -q '^tree-hash-cap: 4096 bytes (clamped from 1 ' "$sum" 2>/dev/null; then
  ok "F4: the mutation is named AND the clamp is disclosed in the same block"
else
  bad "F4: the untracked same-size/same-mtime edit was not named, or the clamp not disclosed"
  grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
fi
rm -f "$r4/untracked-payload.txt" "$tmp/inplace-mtime-ref"

# --- B4: an edit while QUEUED for a gate slot is outside the certification window --
# With CQLITE_GATE_MAX_CONCURRENCY=1 a full gate can sit in `waiting for gate slot` for
# a whole other run; it has executed nothing and certifies nothing, so the window must
# begin when the slot is granted. Sequenced DETERMINISTICALLY (no sleep): a stub python3
# performs the edit and only THEN execs the real slot daemon, which is what writes the
# ready file the gate blocks on.
# The slot-grant cases (B4, C3, F1) have TWO prerequisites: python3, and the slot daemon
# the gate blocks on. Both are named EXPLICITLY here, and a missing daemon is reported as
# its own failure rather than surfacing as three confusing fail-closed cases — a scratch
# copy of this file that brings only scripts/agent-gate.sh says exactly what it is missing.
SLOT_DAEMON="$SCRIPT_DIR/../lib/gate_slot_daemon.py"
REAL_PY=$(command -v python3 2>/dev/null || true)
if [ -n "$REAL_PY" ] && [ ! -f "$SLOT_DAEMON" ]; then
  bad "B4/C3/F1 prerequisite: $SLOT_DAEMON is MISSING — the slot-grant cases cannot be sequenced (copy scripts/lib/gate_slot_daemon.py alongside scripts/agent-gate.sh)"
fi
if [ -n "$REAL_PY" ] && [ -f "$SLOT_DAEMON" ]; then
  r6=$(mkrepo queue-repo)
  mkdir -p "$r6/scripts/lib"
  cp "$SLOT_DAEMON" "$r6/scripts/lib/gate_slot_daemon.py"
  ( cd "$r6" && git add -A && git "${GIT_ID[@]}" commit -qm daemon ) >/dev/null 2>&1
  QSTUB="$tmp/qstub"; mkdir -p "$QSTUB"
  cp "$STUBBIN/cargo" "$QSTUB/cargo"
  cat > "$QSTUB/python3" <<QPY
#!/usr/bin/env bash
case "\$*" in
  *gate_slot_daemon.py*)
    [ -n "\${FAKE_QUEUE_MUTATE:-}" ] && printf 'edited while queued\n' >> "\$FAKE_QUEUE_MUTATE" ;;
esac
exec "$REAL_PY" "\$@"
QPY
  chmod +x "$QSTUB/python3"
  pre_digest=$(digest_of "$r6")
  sum="$tmp/queue.txt"; out="$tmp/queue.out"
  mkdir -p "$tmp/empty-datasets"
  ( cd "$r6" && PATH="$QSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      CQLITE_GATE_SLOTS_DIR="$tmp/slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
      CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
      FAKE_QUEUE_MUTATE="$r6/README.md" \
      bash "$r6/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  q_start=$(sed -n 's/^tree-start: .*digest: //p' "$sum" | head -1)
  q_end=$(sed -n 's/^tree-end: .*digest: //p' "$sum" | head -1)
  post_digest=$(digest_of "$r6")
  if grep -q '^tree-integrity: PASS$' "$sum" 2>/dev/null \
     && [ -n "$q_start" ] && [ "$q_start" = "$q_end" ]; then
    ok "B4: an edit landing WHILE QUEUED for a gate slot does not invalidate the run (window starts at slot grant)"
  else
    bad "B4: a pre-work edit tripped the guard (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
  fi
  if [ -n "$pre_digest" ] && [ "$pre_digest" != "$post_digest" ] \
     && [ "$q_start" = "${post_digest:0:12}" ]; then
    ok "B4: the block's tree-start is the POST-queue re-capture, not the pre-queue one (the edit really happened)"
  else
    bad "B4: the start identity was not re-captured after the slot (pre=$pre_digest post=$post_digest start=$q_start)"
  fi
  ( cd "$r6" && git checkout -q -- README.md )

  # --- C3: a TRANSIENT git blip at the slot grant must not DISARM the guard --------
  # _tree_recapture_after_slot re-runs the start capture; its rc-1 branch ("no git
  # worktree") sets TREE_GUARDED=0, which is correct only at the very FIRST capture. A
  # `git rev-parse --git-dir` blip at the slot grant (a concurrent prune/gc, a stuttering
  # network mount) therefore downgraded a 20-minute full gate to `tree-integrity: SKIP` —
  # a live guard silently disarmed by a hiccup. Sequenced deterministically: the stub
  # python3 arms a one-shot marker as the slot daemon starts, and the stub git fails the
  # NEXT rev-parse pair (exactly the re-capture) and then disarms itself.
  r7=$(mkrepo blip-repo)
  mkdir -p "$r7/scripts/lib"
  cp "$SLOT_DAEMON" "$r7/scripts/lib/gate_slot_daemon.py"
  ( cd "$r7" && git add -A && git "${GIT_ID[@]}" commit -qm daemon ) >/dev/null 2>&1
  REAL_GIT=$(command -v git)
  BSTUB="$tmp/bstub"; mkdir -p "$BSTUB"
  cp "$STUBBIN/cargo" "$BSTUB/cargo"
  BLIP="$tmp/blip-armed"
  cat > "$BSTUB/python3" <<QPY
#!/usr/bin/env bash
case "\$*" in
  *gate_slot_daemon.py*) : > "$BLIP" ;;
esac
exec "$REAL_PY" "\$@"
QPY
  chmod +x "$BSTUB/python3"
  cat > "$BSTUB/git" <<QGIT
#!/usr/bin/env bash
if [ -f "$BLIP" ]; then
  case "\$*" in
    *"rev-parse HEAD"*)      exit 128 ;;
    *"rev-parse --git-dir"*)
      rm -f "$BLIP"
      # BLIP_MUTATE (second run only): edit the tree at the very moment the re-capture
      # fails, so the mutation lands INSIDE the window the retained capture defines.
      [ -n "\${BLIP_MUTATE:-}" ] && printf 'edited at the blip\n' >> "\$BLIP_MUTATE"
      exit 128 ;;
  esac
fi
exec "$REAL_GIT" "\$@"
QGIT
  chmod +x "$BSTUB/git"
  sum="$tmp/blip.txt"; out="$tmp/blip.out"
  ( cd "$r7" && PATH="$BSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      CQLITE_GATE_SLOTS_DIR="$tmp/slots-blip" CQLITE_GATE_MAX_CONCURRENCY=1 \
      CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
      bash "$r7/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if [ ! -f "$BLIP" ]; then
    ok "C3: the one-shot git blip fired during the run (the re-capture really saw 'no git worktree')"
  else
    bad "C3: the blip never fired — the case was not exercised"
  fi
  if grep -q '^tree-integrity: SKIP' "$sum" 2>/dev/null; then
    bad "C3: a transient git blip DISARMED the guard — the run certified with tree-integrity: SKIP (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  else
    ok "C3: the guard stays ARMED across the blip — no tree-integrity: SKIP"
  fi
  if grep -q '^tree-start: .*pre-queue capture retained' "$sum" 2>/dev/null \
     && grep -q '^tree-integrity: PASS' "$sum" 2>/dev/null; then
    ok "C3: the pre-queue capture is retained and still verified at the terminal capture (tree-integrity: PASS)"
  else
    bad "C3: the retained-capture path did not certify (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
  # …and the retained capture still DETECTS: the same blip with a real mutation FAILs.
  sum="$tmp/blip-mut.txt"; out="$tmp/blip-mut.out"
  ( cd "$r7" && PATH="$BSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      CQLITE_GATE_SLOTS_DIR="$tmp/slots-blip2" CQLITE_GATE_MAX_CONCURRENCY=1 \
      CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
      BLIP_MUTATE="$r7/README.md" \
      bash "$r7/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$sum" 2>/dev/null \
     && ! grep -q '^RESULT: PASS' "$sum" 2>/dev/null; then
    ok "C3: a guard retained across the blip still DETECTS a real mid-run mutation"
  else
    bad "C3: the retained guard missed a real mutation (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
  fi
  ( cd "$r7" && git checkout -q -- README.md )

  # --- F1: a git blip at the FIRST capture must not disarm the guard for the WHOLE run
  # The mirror image of C3, and the more dangerous half: `_tree_capture_start`'s rc-1
  # branch ("no git worktree") is indistinguishable from a transient `git rev-parse`
  # failure at second 0 — exactly when a concurrent `git gc`/prune is likeliest — and a
  # single such failure produced `tree-integrity: SKIP` + `RESULT: PASS` for the entire
  # run. The conservative arm: the slot-grant re-capture RE-ATTEMPTS and ARMS on success.
  # (A genuinely non-git tree fails the re-attempt the same way and stays SKIP, so the
  # spec'd no-worktree SKIP contract is untouched.)
  r8=$(mkrepo firstblip-repo)
  mkdir -p "$r8/scripts/lib"
  cp "$SLOT_DAEMON" "$r8/scripts/lib/gate_slot_daemon.py"
  ( cd "$r8" && git add -A && git "${GIT_ID[@]}" commit -qm daemon ) >/dev/null 2>&1
  FSTUB="$tmp/fstub"; mkdir -p "$FSTUB"
  cp "$STUBBIN/cargo" "$FSTUB/cargo"
  FBLIP="$tmp/first-blip-armed"; FCOUNT="$tmp/first-blip-count"
  # `git rev-parse HEAD` is issued by _tree_identity and by NOTHING else in the gate, so
  # counting it counts CAPTURES: #1 = the blipped first capture (armed → fails, and the
  # following --git-dir probe disarms the stub), #2 = the slot-grant re-attempt (the
  # window start), #3 = the next capture inside the window. FBLIP_MUTATE edits the tree at
  # the START of capture #3, so the edit lands INSIDE the re-armed window by construction.
  cat > "$FSTUB/git" <<QGIT
#!/usr/bin/env bash
case "\$*" in
  *"rev-parse HEAD"*)
    [ -f "$FBLIP" ] && exit 128
    _n=\$(cat "$FCOUNT" 2>/dev/null || echo 0); _n=\$(( _n + 1 )); printf '%s' "\$_n" > "$FCOUNT"
    if [ -n "\${FBLIP_MUTATE:-}" ] && [ "\$_n" = 2 ]; then
      printf 'edited after the guard re-armed\n' >> "\$FBLIP_MUTATE"
    fi ;;
  *"rev-parse --git-dir"*)
    if [ -f "$FBLIP" ]; then rm -f "$FBLIP"; exit 128; fi ;;
esac
exec "$REAL_GIT" "\$@"
QGIT
  chmod +x "$FSTUB/git"
  : > "$FBLIP"; rm -f "$FCOUNT"
  sum="$tmp/firstblip.txt"; out="$tmp/firstblip.out"
  ( cd "$r8" && PATH="$FSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      CQLITE_GATE_SLOTS_DIR="$tmp/slots-fb" CQLITE_GATE_MAX_CONCURRENCY=1 \
      CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
      bash "$r8/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if [ ! -f "$FBLIP" ]; then
    ok "F1: the one-shot blip fired at the FIRST capture (the case was exercised)"
  else
    bad "F1: the first-capture blip never fired — the case was not exercised"
  fi
  if grep -q '^tree-integrity: SKIP' "$sum" 2>/dev/null; then
    bad "F1: a blip at the first capture DISARMED the guard for the whole run (tree-integrity: SKIP, rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  else
    ok "F1: a blip at the first capture does NOT leave the run unguarded (no tree-integrity: SKIP)"
  fi
  if grep -q '^tree-start: .*captured at the slot grant' "$sum" 2>/dev/null \
     && grep -q '^tree-integrity: PASS' "$sum" 2>/dev/null; then
    ok "F1: the capture is RE-ATTEMPTED at the slot grant, the guard ARMS, and the run certifies"
  else
    bad "F1: the re-attempt did not arm the guard (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
  # …and the armed-by-re-attempt guard still DETECTS: same blip, plus a real mutation
  # landing after the re-arm.
  : > "$FBLIP"; rm -f "$FCOUNT"
  sum="$tmp/firstblip-mut.txt"; out="$tmp/firstblip-mut.out"
  ( cd "$r8" && PATH="$FSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      CQLITE_GATE_SLOTS_DIR="$tmp/slots-fb2" CQLITE_GATE_MAX_CONCURRENCY=1 \
      CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
      FBLIP_MUTATE="$r8/README.md" \
      bash "$r8/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$sum" 2>/dev/null \
     && ! grep -q '^RESULT: PASS' "$sum" 2>/dev/null && [ "$rc" -ne 0 ]; then
    ok "F1: a guard armed by the re-attempt still DETECTS a real mid-run mutation"
  else
    bad "F1: the re-armed guard missed a real mutation (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
  fi
  ( cd "$r8" && git checkout -q -- README.md )

  # --- G4: an UNVALIDATABLE (rc 2) re-capture at the slot grant ---------------------
  # The third arm of the same policy. rc 1 ("no git worktree") restores the pre-queue
  # capture and rc 1 at the FIRST capture re-attempts — but rc 2 ("the capture RAN and
  # could not be validated") used to do NEITHER, failing a run closed while a FULLY
  # VALIDATED pre-queue identity sat in the globals AND on disk. That is a spurious FAIL,
  # not a safety property: a hash-tool/disk blip at the slot grant killed a 20-minute gate.
  # Sequenced deterministically: the stub python3 arms a one-shot marker as the slot daemon
  # starts, and the stub sha256sum answers the NEXT digest — exactly the re-capture's —
  # with a non-hex string, which _tree_digest_ok rejects (rc 2), then disarms itself.
  r10=$(mkrepo rc2-repo)
  mkdir -p "$r10/scripts/lib"
  cp "$SLOT_DAEMON" "$r10/scripts/lib/gate_slot_daemon.py"
  ( cd "$r10" && git add -A && git "${GIT_ID[@]}" commit -qm daemon ) >/dev/null 2>&1
  RSTUB="$tmp/rstub"; mkdir -p "$RSTUB"
  cp "$STUBBIN/cargo" "$RSTUB/cargo"
  REAL_SHA=$(command -v sha256sum 2>/dev/null || true)
  RC2="$tmp/rc2-armed"
  cat > "$RSTUB/python3" <<QPY
#!/usr/bin/env bash
case "\$*" in
  *gate_slot_daemon.py*) : > "$RC2" ;;
esac
exec "$REAL_PY" "\$@"
QPY
  chmod +x "$RSTUB/python3"
  cat > "$RSTUB/sha256sum" <<QSHA
#!/usr/bin/env bash
if [ -f "$RC2" ]; then
  rm -f "$RC2"
  cat > /dev/null                      # drain stdin, then answer with a NON-HEX digest
  # RC2_MUTATE (second run only): edit the tree at the moment the re-capture fails, so the
  # mutation lands INSIDE the window the retained pre-queue capture defines.
  [ -n "\${RC2_MUTATE:-}" ] && printf 'edited at the unvalidatable re-capture\n' >> "\$RC2_MUTATE"
  printf 'not-a-digest  -\n'
  exit 0
fi
exec "$REAL_SHA" "\$@"
QSHA
  chmod +x "$RSTUB/sha256sum"
  if [ -z "$REAL_SHA" ]; then
    ok "G4: SKIP — no sha256sum on this host, the rc-2 re-capture cannot be sequenced through the digest tool"
  else
    sum="$tmp/rc2.txt"; out="$tmp/rc2.out"
    ( cd "$r10" && PATH="$RSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
        CQLITE_GATE_SLOTS_DIR="$tmp/slots-rc2" CQLITE_GATE_MAX_CONCURRENCY=1 \
        CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
        bash "$r10/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
    if [ ! -f "$RC2" ]; then
      ok "G4: the one-shot digest blip fired (the re-capture really returned an unvalidatable identity)"
    else
      bad "G4: the digest blip never fired — the rc-2 re-capture was not exercised"
    fi
    if grep -q '^tree-start: .*pre-queue capture retained' "$sum" 2>/dev/null \
       && grep -q '^tree-integrity: PASS' "$sum" 2>/dev/null; then
      ok "G4: an unvalidatable re-capture RESTORES the validated pre-queue capture and certifies (no spurious FAIL)"
    else
      bad "G4: the rc-2 re-capture did not restore the pre-queue capture (rc=$rc)"
      grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
    fi
    if grep -q 'tree-capture-failed' "$sum" 2>/dev/null; then
      bad "G4: the run still fails closed on a recoverable rc-2 re-capture"
    else
      ok "G4: the block carries no tree-capture-failed reason — the recoverable case is not reported as one"
    fi
    # …and the DISCRIMINATION STUB: the restored capture must still DETECT. Same blip, with
    # a real mutation landing at the moment of the failed re-capture.
    sum="$tmp/rc2-mut.txt"; out="$tmp/rc2-mut.out"
    ( cd "$r10" && PATH="$RSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
        CQLITE_GATE_SLOTS_DIR="$tmp/slots-rc2b" CQLITE_GATE_MAX_CONCURRENCY=1 \
        CQLITE_DATASETS_ROOT="$tmp/empty-datasets" \
        RC2_MUTATE="$r10/README.md" \
        bash "$r10/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
    if grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$sum" 2>/dev/null \
       && grep -q 'changed: README.md' "$sum" 2>/dev/null \
       && ! grep -q '^RESULT: PASS' "$sum" 2>/dev/null && [ "$rc" -ne 0 ]; then
      ok "G4: the restored pre-queue capture still DETECTS a real mutation, and names the right path (the manifest was restored too)"
    else
      bad "G4: the restored capture missed the mutation or misnamed it (rc=$rc)"
      grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
    fi
    ( cd "$r10" && git checkout -q -- README.md )
  fi

  # --- F1 near-miss: --lite and --delta EXIT BEFORE the slot grant, so they have no
  # re-arm point at all. Their SKIP must at least SAY that a worktree WAS present at the
  # terminal capture, so a transient first-capture failure can never be read as "there was
  # nothing to check". (`--only` self-exempts from the cap but still passes THROUGH
  # acquire_gate_slot, so it re-arms exactly like the full gate — asserted first.)
  : > "$FBLIP"; rm -f "$FCOUNT"
  sum="$tmp/firstblip-only.txt"; out="$tmp/firstblip-only.out"
  ( cd "$r8" && PATH="$FSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      bash "$r8/scripts/agent-gate.sh" --only fmt >"$out" 2>&1 ); rc=$?
  if grep -q '^tree-start: .*captured at the slot grant' "$sum" 2>/dev/null \
     && grep -q '^tree-integrity: PASS' "$sum" 2>/dev/null; then
    ok "F1: an --only run blipped at the first capture also re-arms (it passes through acquire_gate_slot)"
  else
    bad "F1: the --only mode did not re-arm after a first-capture blip (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
  : > "$FBLIP"; rm -f "$FCOUNT"
  sum="$tmp/firstblip-lite.txt"; out="$tmp/firstblip-lite.out"
  ( cd "$r8" && PATH="$FSTUB:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      bash "$r8/scripts/agent-gate.sh" --lite >"$out" 2>&1 ); rc=$?
  if grep -q '^tree-integrity: SKIP (start capture found no git worktree, but a worktree WAS present at the terminal capture' "$sum" 2>/dev/null; then
    ok "F1: a --lite run blipped at its only capture point discloses the transient failure in its SKIP line"
  else
    bad "F1: a blipped --lite run published a bare SKIP — indistinguishable from a non-git tree (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
  # …and the CONTROL: a genuinely non-git tree still reports the plain SKIP (the spec'd
  # no-worktree contract is unchanged — the disclosure is not hardwired on).
  r9="$tmp/nogit-repo"
  mkdir -p "$r9/scripts"; cp "$GATE" "$r9/scripts/agent-gate.sh"
  printf 'hello\n' > "$r9/README.md"
  sum="$tmp/nogit.txt"; out="$tmp/nogit.out"
  ( cd "$r9" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      bash "$r9/scripts/agent-gate.sh" --only fmt >"$out" 2>&1 ); rc=$?
  if grep -q '^tree-integrity: SKIP (capture unavailable — no git worktree)' "$sum" 2>/dev/null; then
    ok "F1 control: a genuinely non-git tree still reports the plain no-worktree SKIP (contract unchanged)"
  else
    bad "F1 control: the no-worktree SKIP contract changed (rc=$rc)"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
else
  ok "B4: SKIP — python3 unavailable, the slot daemon (and therefore the queue) cannot run here"
  ok "C3: SKIP — python3 unavailable, the slot-grant re-capture cannot be sequenced here"
  ok "F1: SKIP — python3 unavailable, the first-capture blip re-attempt cannot be sequenced here"
fi

# --- G: --lite ------------------------------------------------------------------
sum="$tmp/lite-mut.txt"; out="$tmp/lite-mut.out"
FAKE_CARGO_MUTATE="$r4/README.md" run_gate "$r4" "$sum" "$out" --lite; rc=$?
assert_named_fail "G(--lite mutated)" "$sum" "$rc"
if grep -q 'AGENT-GATE LITE SUMMARY' "$sum"; then
  ok "G(--lite): the refusal lands in the DISTINCTLY-marked LITE block"
else
  bad "G(--lite): the block is not the LITE-marked one"
fi
# G3 in --lite: the boundary block's provenance is assembled at ANY boundary in ANY mode,
# and `DATA_COUNT`/`PINS` only exist on the full gate's path. Under `set -u` a naive
# assembly would ABORT here; the correct behaviour is to omit the lines it cannot source
# and still carry everything else.
if grep -qE '^commit: [0-9a-f]+ branch: ' "$sum" && grep -q '^accelerators: ' "$sum" \
   && grep -q '^cpu-budget: ' "$sum" && grep -q '^detected-after-component: fmt$' "$sum" \
   && grep -qE '^fmt: +(PASS|FAIL)' "$sum" && grep -q '^components-completed: ' "$sum" \
   && ! grep -q '^ci-pins: ' "$sum" && ! grep -q '^datasets: ' "$sum"; then
  ok "G(--lite): the boundary block carries its available provenance and OMITS the lines --lite never establishes (no set -u abort, nothing invented)"
else
  bad "G(--lite): the boundary block's provenance is wrong for a mode without datasets/ci-pins"
  grep -vE '^(logs|summary-file):' "$sum" 2>/dev/null
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
cap_ln=$(grep -n '^  _tree_capture_start$' "$GATE" | tail -1 | cut -d: -f1)
lite_ln=$(grep -n '^  run_lite$' "$GATE" | head -1 | cut -d: -f1)
delta_ln=$(grep -n '^  run_delta "\$DELTA_ANCHOR"$' "$GATE" | head -1 | cut -d: -f1)
slot_ln=$(grep -n '^acquire_gate_slot$' "$GATE" | tail -1 | cut -d: -f1)
recap_ln=$(grep -n '^_tree_recapture_after_slot$' "$GATE" | tail -1 | cut -d: -f1)
if [ -n "$cap_ln" ] && [ -n "$lite_ln" ] && [ -n "$delta_ln" ] && [ -n "$slot_ln" ] \
   && [ "$cap_ln" -lt "$lite_ln" ] && [ "$cap_ln" -lt "$delta_ln" ] && [ "$cap_ln" -lt "$slot_ln" ]; then
  ok "WIRING: the start capture precedes run_lite, run_delta and acquire_gate_slot (all modes guarded)"
else
  bad "WIRING: start capture is not before the mode dispatch (capture=$cap_ln lite=$lite_ln delta=$delta_ln slot=$slot_ln)"
fi
# B4: the FULL gate's window opens where work opens — strictly AFTER the slot grant.
if [ -n "$recap_ln" ] && [ -n "$slot_ln" ] && [ "$recap_ln" -gt "$slot_ln" ]; then
  ok "WIRING: _tree_recapture_after_slot runs AFTER acquire_gate_slot (the queue is outside the window)"
else
  bad "WIRING: the post-slot re-capture is missing or misplaced (recap=$recap_ln slot=$slot_ln)"
fi
# B2: the lazy finalize must run in the CURRENT shell, before the process substitution,
# or its OVERALL=FAIL/TREE_MUTATED=1 die in the subshell and only the text survives.
mabody=$(awk '/^_tree_meta_array\(\) \{/,/^\}/' "$GATE")
fin_at=$(printf '%s\n' "$mabody" | grep -n '_tree_finalize' | head -1 | cut -d: -f1)
sub_at=$(printf '%s\n' "$mabody" | grep -n '< <(_tree_meta_lines)' | head -1 | cut -d: -f1)
if [ -n "$fin_at" ] && [ -n "$sub_at" ] && [ "$fin_at" -lt "$sub_at" ]; then
  ok "WIRING: _tree_meta_array finalizes in the current shell BEFORE the process substitution"
else
  bad "WIRING: _tree_meta_array's finalize would run inside the < <(…) subshell (fail-closed assignment discarded)"
fi
# B7: every capture artifact a SIDE lane can write must be per-lane, terminal included.
if awk '/^_tree_finalize\(\) \{/,/^\}/' "$GATE" | grep -q 'tree-identity\.end\.\${BASHPID' \
   && grep -q 'tree-identity\.probe\.\${BASHPID' "$GATE"; then
  ok "WIRING: both the boundary probe AND the terminal capture use per-lane paths (no SIDE-lane race)"
else
  bad "WIRING: the terminal capture path is not per-lane — concurrent SIDE lanes would race it"
fi
# B1: the identity is never split with `IFS=$'\t' read` (which collapses empty fields).
if awk '/^_tree_capture_start\(\) \{/,/^\}/' "$GATE" | grep -q "IFS=\$'\\\\t' read -r TREE_START_HEAD"; then
  bad "WIRING: the start identity is still split with IFS=\$'\\t' read (empty fields collapse)"
else
  ok "WIRING: the identity is split by _tree_split_identity, which preserves empty fields and validates each"
fi
# C4/F2: PER-EMIT-PATH, keyed on the CALL FORM — never on a textual occurrence count and
# never on INDENTATION. Two vacuity classes have been eliminated here in turn:
#   * `grep -c '_tree_finalize'` counted the definition, its doc comment and the
#     _tree_meta_lines/_tree_meta_array internals, all of which survive deleting every
#     call site (#2926 review C4);
#   * keying on `'    _tree_finalize'` (4-space) matched run_delta's three REFUSED sites
#     but NOT its 2-space TERMINAL site, so deleting that one left the check green
#     (#2926 review F2).
# The property asserted is the one that actually matters and has no indentation in it:
# EVERY emit call site in a certifying function is preceded by a terminal capture since
# the previous emit. The site inventory is asserted too, so a body that loses its emits
# (or gains an unguarded one) is reported rather than silently satisfying the check, and
# the check is PROVED discriminating below by deleting each call site in turn.
# fn_body <file> <function-name> — the lines of one top-level function definition.
# The function name reaches awk through ENVIRON, never `awk -v` (#2926 review G2): `-v`
# performs escape-sequence processing on the value, so the ONE convention this file uses
# for handing text to awk is the one that cannot silently rewrite it.
fn_body() {
  TEST_AWK_F="$2() {" awk 'index($0, ENVIRON["TEST_AWK_F"]) == 1 { inf = 1 } inf { print } inf && /^\}/ { inf = 0 }' "$1"
}
# body_has <text> <line-prefix> — rc 0 iff some LINE of <text> starts with <line-prefix>.
# Deliberately pipe-free: this file runs under `set -o pipefail`, and `awk … | grep -q`
# makes the PIPELINE fail on awk's SIGPIPE once grep short-circuits — a structural check
# that reports "missing" for a call site that is plainly present.
body_has() {
  case $'\n'"$1" in *$'\n'"$2"*) return 0 ;; esac
  return 1
}
# unfinalized_emits <gate> <fn> -> prints the ORDINAL of every emit call site in <fn>'s
# body that is not preceded by a `_tree_finalize` call since the previous emit; rc 1 when
# any is. Leading whitespace and trailing comments are stripped BEFORE matching, so the
# check sees the call form only: neither re-indenting a call site nor writing prose about
# one can change the verdict.
# A capture may be taken EXPLICITLY (`_tree_finalize`) or LAZILY (`_tree_meta_array` /
# `_tree_meta_lines`, which finalize in the current shell when no terminal capture has
# been taken). run_delta's four ERROR emits use the lazy form legitimately, so coverage
# accepts all three — and the count of EXPLICIT sites is asserted separately below, so
# the lazy backstop can never be what silently keeps a certifying terminal green.
unfinalized_emits() {
  local out
  out=$(fn_body "$1" "$2" | awk '
    { l = $0; sub(/^[[:space:]]+/, "", l); sub(/[[:space:]]*#.*$/, "", l) }
    l ~ /^(_tree_finalize|_tree_meta_array|_tree_meta_lines)([[:space:]]|$)/ { fin = 1; next }
    l ~ /^(emit_summary|_emit_terminal_summary)([[:space:]]|$)/ { n++; if (!fin) printf "%s ", n; fin = 0 }
  ')
  [ -z "$out" ] || { printf '%s\n' "$out"; return 1; }
  return 0
}
# emit_sites <gate> <fn> -> how many emit call sites the body has. A body with ZERO would
# make the coverage check vacuously true, so the inventory is asserted explicitly.
emit_sites() {
  fn_body "$1" "$2" | awk '
    { l = $0; sub(/^[[:space:]]+/, "", l); sub(/[[:space:]]*#.*$/, "", l) }
    l ~ /^(emit_summary|_emit_terminal_summary)([[:space:]]|$)/ { n++ }
    END { print n + 0 }'
}
# explicit_finalize_sites <gate> <fn> -> how many EXPLICIT `_tree_finalize` CALL LINES the
# body has. Comment-stripped and indentation-stripped, so it counts call sites only — not
# the definition, not its doc comment, not prose (the #2926 C4 vacuity) — and it is blind
# to how a site is indented (the #2926 F2 vacuity).
explicit_finalize_sites() {
  fn_body "$1" "$2" | awk '
    { l = $0; sub(/^[[:space:]]+/, "", l); sub(/[[:space:]]*#.*$/, "", l) }
    l ~ /^_tree_finalize([[:space:]]|$)/ { n++ }
    END { print n + 0 }'
}
tree_finalize_sites() { # <gate-file> -> prints the FAILING sites; rc 1 when any fails
  local g="$1" missing="" m fin emit
  m=$(unfinalized_emits "$g" run_lite)  || missing="run_lite(emit#$m)"
  m=$(unfinalized_emits "$g" run_delta) || missing="${missing:+$missing }run_delta(emit#$m)"
  # The site inventories. Exact, so ADDING an emit path (or removing a capture) makes the
  # author revisit this proof instead of inheriting a check that no longer covers the code.
  [ "$(emit_sites "$g" run_lite)"  -eq 1 ] || missing="${missing:+$missing }run_lite(emit-inventory)"
  [ "$(emit_sites "$g" run_delta)" -eq 8 ] || missing="${missing:+$missing }run_delta(emit-inventory)"
  [ "$(explicit_finalize_sites "$g" run_lite)"  -eq 1 ] || missing="${missing:+$missing }run_lite(explicit-finalize)"
  [ "$(explicit_finalize_sites "$g" run_delta)" -eq 4 ] || missing="${missing:+$missing }run_delta(explicit-finalize)"
  # top level: the column-0 call site must precede the final terminal emit. Matched on the
  # exact CALL form — `^_tree_finalize` alone also matches the DEFINITION line, which
  # survives deleting every call site (the #2926 review C4 vacuity, in miniature).
  fin=$(grep -n '^_tree_finalize || true$' "$g" | tail -1 | cut -d: -f1)
  emit=$(grep -n '^_emit_terminal_summary "\$OVERALL" "\${SUMMARY_META\[@\]}"' "$g" | tail -1 | cut -d: -f1)
  if [ -z "$fin" ] || [ -z "$emit" ] || [ "$fin" -ge "$emit" ]; then
    missing="${missing:+$missing }top-level"
  fi
  [ -z "$missing" ] || { printf '%s\n' "$missing"; return 1; }
  return 0
}
n_lite_emits=$(emit_sites "$GATE" run_lite); n_delta_emits=$(emit_sites "$GATE" run_delta)
n_delta_fin=$(explicit_finalize_sites "$GATE" run_delta)
if miss_sites=$(tree_finalize_sites "$GATE"); then
  ok "WIRING: every emit path is preceded by a terminal capture (run_lite: $n_lite_emits emit(s), run_delta: $n_delta_emits emit(s)/$n_delta_fin explicit finalize(s), top-level pre-emit)"
else
  bad "WIRING: emit path(s) reached with no _tree_finalize: $miss_sites"
fi
# …and the PROOF that the check can fail: delete ONE call site at a time in a scratch copy,
# addressing each by its ORDINAL inside the function (never by its indentation — run_delta's
# three REFUSED sites are 4-space and its terminal site is 2-space, and keying on the former
# is exactly how the terminal site slipped through, #2926 review F2).
mutant_drop_nth() { # mutant_drop_nth <src> <dst> <fn> <n>
  TEST_AWK_F="$3() {" TEST_AWK_N="$4" awk '
    BEGIN { f = ENVIRON["TEST_AWK_F"]; want = ENVIRON["TEST_AWK_N"] + 0 }
    index($0, f) == 1 { inf = 1 }
    { l = $0; sub(/^[[:space:]]+/, "", l); sub(/[[:space:]]*#.*$/, "", l) }
    inf && l ~ /^_tree_finalize([[:space:]]|$)/ { k++; if (k == want) next }
    { print }
    inf && /^\}/ { inf = 0 }
  ' "$1" > "$2"
}
mutant_drop_top() { grep -v '^_tree_finalize || true$' "$1" > "$2"; }
# run_delta's FOUR sites by ordinal: 1-3 are the REFUSED paths, 4 is the TERMINAL one whose
# deletion the previous indentation-keyed check could not see.
for site in "lite:run_lite:1" "delta-refused-1:run_delta:1" "delta-refused-2:run_delta:2" \
            "delta-refused-3:run_delta:3" "delta-terminal:run_delta:4" "top::"; do
  name=${site%%:*}; rest=${site#*:}; mfn=${rest%%:*}; mn=${rest#*:}
  mut="$tmp/mutant-$name.sh"
  if [ "$name" = top ]; then mutant_drop_top "$GATE" "$mut"; else mutant_drop_nth "$GATE" "$mut" "$mfn" "$mn"; fi
  if cmp -s "$GATE" "$mut"; then
    bad "C4: the '$name' mutation removed nothing — the proof is vacuous"
  elif [ "$(( $(wc -l < "$GATE") - $(wc -l < "$mut") ))" -ne 1 ] && [ "$name" != top ]; then
    bad "C4: the '$name' mutation removed $(( $(wc -l < "$GATE") - $(wc -l < "$mut") )) lines, expected exactly 1"
  elif tree_finalize_sites "$mut" >/dev/null 2>&1; then
    bad "C4: the structural check STILL PASSES after deleting the '$name' call site (can't-fail guard)"
  else
    ok "C4: the structural check FAILS when the '$name' call site is deleted (proved discriminating)"
  fi
done
# C1: the `commit:` stamp must never be a fresh emit-time git read — the original defect.
# Comments are stripped (prose ABOUT the defect must not trip the check) and the ONE
# legitimate site is skipped: the UNGUARDED branch of the stamp renderer, reached only when
# there is no git worktree to capture, where the block already stamps tree-integrity: SKIP.
# BOTH halves of the split are skipped (#2926 review G3 moved the body into
# _tree_commit_meta_render so the boundary-FAIL block can stamp WITHOUT a lazy finalize);
# naming only the wrapper would have made this check pass by looking at the wrong function.
emit_time_stamps=$(awk '/^_tree_commit_meta(_render)?\(\) \{/ { skip = 1 } skip { if (/^\}/) skip = 0; next } { print }' "$GATE" \
                     | sed 's/[[:space:]]*#.*$//' | grep -n 'commit: \$(git' || true)
if [ -n "$emit_time_stamps" ]; then
  bad "C1: an emit-time 'commit: \$(git …)' stamp is back — the block can name an unverified sha"
  printf '%s\n' "$emit_time_stamps"
else
  ok "C1: no emit path stamps 'commit:' from a fresh git call (every stamp is capture-derived)"
fi
for fn in run_lite run_delta; do
  if body_has "$(fn_body "$GATE" "$fn")" '  _tree_commit_meta'; then
    ok "C1: $fn() stamps commit: via _tree_commit_meta (verified-capture derived)"
  else
    bad "C1: $fn() does not derive its commit: stamp from the verified capture"
  fi
done
cm_ln=$(grep -n '^_tree_commit_meta$' "$GATE" | tail -1 | cut -d: -f1)
tf_ln=$(grep -n '^_tree_finalize || true$' "$GATE" | tail -1 | cut -d: -f1)
te_ln=$(grep -n '^_emit_terminal_summary "\$OVERALL" "\${SUMMARY_META\[@\]}"' "$GATE" | tail -1 | cut -d: -f1)
if [ -n "$cm_ln" ] && [ -n "$tf_ln" ] && [ -n "$te_ln" ] && [ "$tf_ln" -lt "$cm_ln" ] && [ "$cm_ln" -lt "$te_ln" ]; then
  ok "C1: the top-level stamp is taken AFTER the terminal capture and BEFORE the emit (finalize=$tf_ln stamp=$cm_ln emit=$te_ln)"
else
  bad "C1: the top-level commit stamp is not sequenced between the capture and the emit (finalize=$tf_ln stamp=$cm_ln emit=$te_ln)"
fi
# The provenance renderer is shared: no emit path may hand-assemble a subset of the lines
# and drop `tree-hash-cap:` (#2926 review).
if body_has "$(fn_body "$GATE" _tree_boundary_fail)" '  while IFS= read -r _l; do _meta+=("$_l"); done < <(_tree_boundary_meta_lines)'; then
  ok "WIRING: the boundary-FAIL block renders its provenance through the shared assembly (tree-hash-cap included)"
else
  bad "WIRING: _tree_boundary_fail hand-assembles its provenance lines — tree-hash-cap: can be dropped"
fi
# …and that assembly is PURE: it must render through the shared printers and must NEVER
# call a lazily-finalizing helper, which would take a terminal capture and overwrite the
# component-named verdict the block exists to publish (#2926 review G3).
bml=$(fn_body "$GATE" _tree_boundary_meta_lines)
bml_missing=""
for need in '  _tree_commit_meta_render' '  _tree_meta_render_lines' \
            '  printf '"'"'%s\n'"'"' "$(accelerators_line)"' '  printf '"'"'%s\n'"'"' "$(cpu_budget_line)"'; do
  body_has "$bml" "$need" || bml_missing="${bml_missing:+$bml_missing }${need# *}"
done
if [ -z "$bml_missing" ]; then
  ok "WIRING: the boundary-FAIL assembly renders commit/accelerators/cpu-budget/tree lines through the shared printers"
else
  bad "WIRING: the boundary-FAIL assembly is missing: $bml_missing"
fi
lazy_hits=$(printf '%s\n' "$bml" | sed 's/[[:space:]]*#.*$//' \
              | grep -nE '(^|[[:space:]])(_tree_finalize|_tree_meta_lines|_tree_meta_array|_tree_commit_meta)([[:space:]]|$)' || true)
if [ -z "$lazy_hits" ]; then
  ok "WIRING: the boundary-FAIL assembly takes NO capture (no lazy finalize can overwrite the component-named verdict)"
else
  bad "WIRING: the boundary-FAIL assembly calls a lazily-finalizing helper: $lazy_hits"
fi
# H1: the cap stamp must CLEAR a line that no longer applies. The full gate re-captures at
# the slot grant and _tree_capture_start resets the fallback count first, so a set-only
# stamp left a pre-queue `tree-hash-cap:` standing on a block whose authoritative capture
# engaged no fallback — advertising a weakened capture that is not in force. Asserted on
# the code because the window is the slot grant, which no self-test hook can enter.
cs_body=$(fn_body "$GATE" _tree_cap_stamp)
if body_has "$cs_body" '    TREE_HASH_CAP_LINE=""'; then
  ok "H1: _tree_cap_stamp CLEARS tree-hash-cap: when no cap condition is in effect (no stale disclosure survives a re-capture)"
else
  bad "H1: _tree_cap_stamp only ever SETS the line — a superseded capture's disclosure survives"
  printf '%s\n' "$cs_body"
fi
if body_has "$(fn_body "$GATE" _tree_capture_start)" '  TREE_CAP_FALLBACKS=0; _tree_cap_note "$TREE_F_FB"' \
   && body_has "$(fn_body "$GATE" _tree_capture_start)" '  _tree_cap_stamp'; then
  ok "H1: every start capture re-notes from zero and re-stamps (the clear path is actually reachable)"
else
  bad "H1: _tree_capture_start no longer resets+restamps the cap disclosure — the H1 window moved"
fi
# The manifest trailer is validated, not merely written (#2926 review C2).
if body_has "$(fn_body "$GATE" _tree_identity)" '  _tree_manifest_ok "$out" nul'; then
  ok "WIRING: _tree_identity validates its own manifest through _tree_manifest_ok (header+trailer+count)"
else
  bad "WIRING: the manifest is not trailer-validated — a truncated capture can compare equal"
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
