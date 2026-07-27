#!/usr/bin/env bash
# Portability regression test for the #2926 tree-integrity guard.
#
# WHY THIS FILE EXISTS (#2926 review G1, and #2914 hours before it): the guard shipped a
# GNU-only `sed 's/^\t//'` in its changed-path parser. macOS is a FIRST-CLASS gate host —
# scripts/agent-gate.sh carries a `Darwin) … taskpolicy -c utility` wrapper, a BSD `stat`
# branch and an explicit macOS-/bin/bash-3.2 floor — and BSD sed does not honour `\t` in a
# BRE, so there it strips a literal `t`, the TAB `comm -3` puts in front of every column-2
# record survives, `awk -F'\t'` shifts one field, and the FAIL line names the MODE instead
# of the path (with the lockfile classifier, which keys on those paths, following it into
# the wrong classification). Nothing caught it because the tree suite had NO macOS path —
# the same blind spot #2914 had just closed for the summary suite.
#
# The coverage here is deliberately BOTH kinds:
#   * BEHAVIOURAL — the parsing/classification/mtime paths are re-run against PATH shims
#     that reproduce the BSD divergences (sed's un-honoured `\t`, stat's `-f`-only
#     interface, a sort(1) without `-z`), with AGENT_GATE_TEST_OS=Darwin forcing the
#     host-family branches the gate already honours. This is what fails when a GNU-only
#     construct is reintroduced ON A COVERED PATH.
#   * STATIC — a lint over EVERY tree-integrity function for the GNU-only construct
#     classes, so a reintroduction on an UNCOVERED path fails too. Faithfully simulating
#     all of BSD userland is impossible; the lint is what closes that gap, and it is
#     proved discriminating with one mutant per rule.
#
# Split out of scripts/tests/test_agent_gate_tree_integrity.sh (which keeps the behavioural
# guard phases) to keep both files near the campsite-rule size target — see #1135. Hermetic:
# every fixture lives under one per-run `mktemp -d …XXXXXX`; no network; no repo write
# outside that namespace; NO assertion references elapsed time (#2642).
#
# Run standalone:   bash scripts/tests/test_agent_gate_tree_portability.sh
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

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-tree-port.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

GIT_ID=(-c user.email=gate@example.invalid -c user.name=gate-selftest)

# Fixture: a FAKE checkout (same shape as the integrity suite's). Copying ONLY the gate
# into <root>/scripts/ makes its `cd "$(dirname "$0")/.."` resolve REPO_ROOT to <root>, so
# every capture, default summary path and mutation stays inside this run's namespace.
mkrepo() { # mkrepo <name> [extra `git init` args…] -> echoes the repo path
  local root="$tmp/$1"; shift
  mkdir -p "$root/scripts"
  cp "$GATE" "$root/scripts/agent-gate.sh"
  # The DISPOSABLE-CHECKOUT MARKER (#2926 review B5): the gate's mutating self-test hooks
  # refuse to write into any checkout that does not carry it.
  printf 'disposable fixture for scripts/tests/test_agent_gate_tree_portability.sh\n' \
                              > "$root/.agent-gate-tree-selftest-fixture"
  printf 'hello\n'            > "$root/README.md"
  printf 'lock v1\n'          > "$root/Cargo.lock"
  printf 'target/\n*.log\n.agent-gate-summary.txt\n.agent-gate-lite-summary.txt\n.agent-gate-delta-summary.txt\n' \
                              > "$root/.gitignore"
  # `${1+"$@"}` (never a bare "$@"): expanding an EMPTY "$@" under `set -u` on bash 3.2 —
  # the floor the gate declares — is an unbound-variable error (#2926 review B8).
  ( cd "$root" && git init -q ${1+"$@"} . && git add -A && git "${GIT_ID[@]}" commit -qm init ) >/dev/null 2>&1
  printf '%s\n' "$root"
}

# A stub `cargo` that always succeeds (nothing compiles) and can create a file mid-run.
STUBBIN="$tmp/stubbin"
mkdir -p "$STUBBIN"
cat > "$STUBBIN/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = fmt ] && [ -n "${FAKE_CARGO_CREATE:-}" ]; then
  mkdir -p "$(dirname "$FAKE_CARGO_CREATE")"
  printf 'created mid-run\n' > "$FAKE_CARGO_CREATE"
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

# assert_named_fail <label> <summary-file> <rc> — the canonical "did NOT certify" check.
assert_named_fail() {
  local label="$1" sum="$2" rc="$3" missing=()
  grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$sum" 2>/dev/null || missing+=("named-tree-integrity-FAIL-line")
  grep -q '^RESULT: FAIL'    "$sum" 2>/dev/null || missing+=("RESULT:-FAIL")
  grep -q '^RESULT: PASS'    "$sum" 2>/dev/null && missing+=("UNEXPECTED-RESULT:-PASS")
  grep -q '^RESULT: PARTIAL' "$sum" 2>/dev/null && missing+=("UNEXPECTED-RESULT:-PARTIAL")
  [ "$rc" -ne 0 ] || missing+=("non-zero-exit(got $rc)")
  if [ "${#missing[@]}" -eq 0 ]; then
    ok "$label: did NOT certify — named tree-integrity FAIL + RESULT: FAIL + non-zero exit"
  else
    bad "$label: ${missing[*]}"
    echo "------- summary -------"; cat "$sum" 2>/dev/null; echo "-----------------------"
  fi
}

mkbig() { # mkbig <path> <bytes> — a file larger than the hash cap in force
  local i=0
  : > "$1"
  while [ "$i" -lt "$2" ]; do printf '0123456789abcdef' >> "$1"; i=$(( i + 16 )); done
}

# fn_body <file> <function-name> — the lines of one top-level function definition.
# The name reaches awk through ENVIRON, never `awk -v` (#2926 review G2): `-v` performs
# escape-sequence processing on the value, so the one convention this suite uses for
# handing text to awk is the one that cannot silently rewrite it.
fn_body() {
  TEST_AWK_F="$2() {" awk 'index($0, ENVIRON["TEST_AWK_F"]) == 1 { inf = 1 } inf { print } inf && /^\}/ { inf = 0 }' "$1"
}
# body_has <text> <line-prefix> — rc 0 iff some LINE of <text> starts with <line-prefix>.
# Deliberately pipe-free: this file runs under `set -o pipefail`, and `awk … | grep -q`
# makes the PIPELINE fail on awk's SIGPIPE once grep short-circuits.
body_has() {
  case $'\n'"$1" in *$'\n'"$2"*) return 0 ;; esac
  return 1
}

REAL_SED=$(command -v sed 2>/dev/null || true)
REAL_STAT=$(command -v stat 2>/dev/null || true)
REAL_SORT=$(command -v sort 2>/dev/null || true)

DARWINBIN="$tmp/darwinbin"; mkdir -p "$DARWINBIN"
cp "$STUBBIN/cargo" "$DARWINBIN/cargo"
# BSD/macOS `sed` simulator: the divergence that bit us is that BSD BRE does NOT honour
# GNU's `\t`/`\s`/`\w`/`\d`/`\+`/`\|` escapes — the backslash is dropped and the next
# character matches literally. The shim reproduces exactly that, then delegates to the
# real sed, so every OTHER sed use in the gate keeps working.
{
  printf '#!/usr/bin/env bash\n'
  printf 'REAL_SED=%s\n' "$REAL_SED"
  cat <<'SEDSHIM'
args=()
for a in "$@"; do
  # BSD sed's -i REQUIRES an extension argument; GNU's bare -i is a portability bug.
  [ "$a" = "-i" ] && { printf 'sed: option requires an argument -- i\n' >&2; exit 1; }
  for esc in t s S w W d b '+' '|'; do a=${a//\\$esc/$esc}; done
  args+=("$a")
done
exec "$REAL_SED" "${args[@]}"
SEDSHIM
} > "$DARWINBIN/sed"
chmod +x "$DARWINBIN/sed"
# BSD/macOS `stat`: no `-c`, and `-f` takes a BSD format string (`%m` = mtime seconds).
{
  printf '#!/usr/bin/env bash\n'
  printf 'REAL_STAT=%s\n' "$REAL_STAT"
  cat <<'STATSHIM'
case "${1:-}" in
  -c*) printf 'stat: illegal option -- c\n' >&2; exit 1 ;;
  -f)  fmt="${2:-}"; shift 2
       [ "${1:-}" = "--" ] && shift
       case "$fmt" in
         %m) exec "$REAL_STAT" -c %Y -- "$@" ;;
         *)  printf 'stat: unsupported format %s\n' "$fmt" >&2; exit 1 ;;
       esac ;;
esac
printf 'stat: usage: stat [-f format] [file ...]\n' >&2
exit 1
STATSHIM
} > "$DARWINBIN/stat"
chmod +x "$DARWINBIN/stat"

darwin_env=(AGENT_GATE_TEST_OS=Darwin)
if [ -z "$REAL_SED" ] || [ -z "$REAL_STAT" ]; then
  bad "phase 6 prerequisite: sed/stat not found on PATH — the BSD simulation cannot run"
else
  r_dw=$(mkrepo darwin-repo)
  # --- G1: the changed-path parser under a BSD sed ---------------------------------
  sum="$tmp/darwin-boundary.txt"; out="$tmp/darwin-boundary.out"
  ( cd "$r_dw" && PATH="$DARWINBIN:$PATH" env "${darwin_env[@]}" \
      AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=boundary \
      AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
      bash "$r_dw/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  assert_named_fail "G1 (BSD-sed host, MAIN-lane boundary)" "$sum" "$rc"
  if grep -q 'changed: README.md' "$sum" 2>/dev/null; then
    ok "G1: on a BSD-sed host the FAIL line names the changed PATH (comm's column-2 TAB is handled inside awk)"
  else
    bad "G1: the BSD-sed host named the wrong field — the comm column-2 TAB was not handled"
    grep -E '^tree-integrity:' "$sum" 2>/dev/null
  fi
  if grep -qE 'changed: (100644|100755|120000|none)($|[^A-Za-z0-9._/-])' "$sum" 2>/dev/null; then
    bad "G1: the FAIL line names a MODE instead of a path — the GNU-only tab strip is back"
  else
    ok "G1: the FAIL line names no mode field (the field numbering did not shift)"
  fi
  # …and the Darwin block still carries the full provenance, with no Linux-only mold token
  # (AGENT_GATE_TEST_OS=Darwin is honoured by accelerators_line, #2859/#2914).
  if grep -q '^accelerators: ' "$sum" 2>/dev/null && ! grep -q '^accelerators: .*mold=' "$sum" 2>/dev/null; then
    ok "G1/G3: the Darwin boundary-FAIL block carries accelerators: with NO mold token (Darwin contract)"
  else
    bad "G1/G3: the Darwin boundary block's accelerators line is missing or carries a Linux-only mold token"
    grep '^accelerators: ' "$sum" 2>/dev/null
  fi
  ( cd "$r_dw" && git checkout -q -- README.md )
  # --- the CONTROL: the same host, unmutated, still certifies -----------------------
  sum="$tmp/darwin-clean.txt"; out="$tmp/darwin-clean.out"
  ( cd "$r_dw" && PATH="$DARWINBIN:$PATH" env "${darwin_env[@]}" \
      AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=clean \
      bash "$r_dw/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if [ "$rc" -eq 0 ] && grep -q '^RESULT: PASS' "$sum" && grep -q '^tree-integrity: PASS$' "$sum"; then
    ok "G1 control: an unmutated run on the simulated BSD host certifies (the shims are not just breaking the gate)"
  else
    bad "G1 control: the simulated BSD host cannot certify a clean tree (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
  fi
  # --- the BSD `stat` branch really produces an mtime -------------------------------
  # The size+mtime fallback is the ONE place the guard reads mtime. With no working stat
  # flavour it records `MTIME:unknown` — stable across captures, so nothing FAILS, and the
  # weakening is invisible. Assert the BSD branch yields a real number on a BSD host.
  mkbig "$r_dw/one-big.bin" 8192
  mdw="$tmp/darwin-manifest"
  ( cd "$r_dw" && PATH="$DARWINBIN:$PATH" env "${darwin_env[@]}" \
      AGENT_GATE_SUMMARY_FILE="$tmp/darwin-cap-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_TREE_HASH_CAP_BYTES=4096 \
      AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$mdw" \
      bash "$r_dw/scripts/agent-gate.sh" >"$tmp/darwin-cap.out" 2>&1 )
  if grep -qE '^U	SIZE:8192:MTIME:[0-9]+	' "$mdw.report" 2>/dev/null; then
    ok "G1: the size+mtime fallback records a NUMERIC mtime through the BSD stat branch (stat -f %m)"
  else
    bad "G1: the BSD stat branch produced no usable mtime — the fallback record is degraded"
    grep 'one-big' "$mdw.report" 2>/dev/null
  fi
  rm -f "$r_dw/one-big.bin"
fi

# --- a sort(1) WITHOUT -z: the fail-OPEN half of the same sweep --------------------
# `sort -z` is not universal. An unsupported flag makes sort print usage and emit NOTHING,
# and the capture pipes it into a `while read -r -d ''` loop — so the manifest would come
# back EMPTY on a dirty tree, BOTH captures would agree, and a mutated run would CERTIFY.
# That is the only silent fail-OPEN this sweep found, so it gets a behavioural case.
if [ -z "$REAL_SORT" ]; then
  bad "phase 6 prerequisite: sort not found on PATH — the no-sort-z case cannot run"
else
  NOZBIN="$tmp/nozbin"; mkdir -p "$NOZBIN"
  cp "$STUBBIN/cargo" "$NOZBIN/cargo"
  {
    printf '#!/usr/bin/env bash\n'
    printf 'REAL_SORT=%s\n' "$REAL_SORT"
    cat <<'SORTSHIM'
case " $* " in
  *" -z "*|*" --zero-terminated "*) printf 'sort: unknown option -- z\n' >&2; exit 2 ;;
esac
exec "$REAL_SORT" "$@"
SORTSHIM
  } > "$NOZBIN/sort"
  chmod +x "$NOZBIN/sort"
  r_nz=$(mkrepo nosortz-repo)
  sum="$tmp/nosortz-mut.txt"; out="$tmp/nosortz-mut.out"
  ( cd "$r_nz" && PATH="$NOZBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      AGENT_GATE_TREE_SELFTEST=boundary AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
      bash "$r_nz/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  assert_named_fail "sort-without-z (mutated)" "$sum" "$rc"
  if grep -q 'changed: README.md' "$sum" 2>/dev/null; then
    ok "sort-without-z: the capture still enumerates paths (no silent empty-manifest fail-OPEN)"
  else
    bad "sort-without-z: the mutation was detected but no path was enumerated — the manifest is empty"
    grep -E '^tree-' "$sum" 2>/dev/null
  fi
  ( cd "$r_nz" && git checkout -q -- README.md )
  sum="$tmp/nosortz-clean.txt"; out="$tmp/nosortz-clean.out"
  ( cd "$r_nz" && PATH="$NOZBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$sum" \
      AGENT_GATE_TREE_SELFTEST=clean \
      bash "$r_nz/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
  if [ "$rc" -eq 0 ] && grep -q '^tree-integrity: PASS$' "$sum"; then
    ok "sort-without-z control: an unmutated run on the same host still certifies"
  else
    bad "sort-without-z control: the fallback broke a clean run (rc=$rc)"
    grep -E '^tree-|^RESULT:' "$sum" 2>/dev/null
  fi
fi

# --- G2: the report lookups must find a path by its ESCAPED spelling ----------------
# `awk -v p=…` performs escape-sequence processing on the assigned value, so a path the
# `.report` view escaped to `weird\tdir/Cargo.lock` (review B6) came back into awk with a
# REAL tab and `$4 == p` could never match — B6's escaping and this lookup cancelled each
# other. Asserted against the PRODUCTION helpers through the read-only report-lookup hook.
lk="$tmp/lookup.report"
{
  printf 'H\tdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
  printf 'T\t1111111111111111111111111111111111111111\t100644\tweird\\tdir/Cargo.lock\n'
  printf 'U\tSIZE:1:MTIME:2\t100644\tmulti\\nline\n'
  printf 'N\t2\n'
} > "$lk"
r_lk=$(mkrepo lookup-repo)
lookup_field() { # lookup_field <escaped-path> <tag|value>
  ( cd "$r_lk" && env AGENT_GATE_SUMMARY_FILE="$tmp/lookup-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=report-lookup \
      AGENT_GATE_TREE_SELFTEST_LOOKUP="$lk|$1" \
      bash "$r_lk/scripts/agent-gate.sh" 2>/dev/null ) \
    | sed -n "s/^tree-selftest: report-$2=//p" | head -1
}
if [ "$(lookup_field 'weird\tdir/Cargo.lock' tag)" = T ] \
   && [ "$(lookup_field 'weird\tdir/Cargo.lock' value)" = 1111111111111111111111111111111111111111 ]; then
  ok "G2: a TAB-containing path is found by its escaped spelling (the value is passed to awk without escape processing)"
else
  bad "G2: the escaped-path lookup returned nothing — awk un-escaped the value the report escaped"
fi
if [ "$(lookup_field 'multi\nline' tag)" = U ]; then
  ok "G2: a NEWLINE-containing path is found by its escaped spelling too"
else
  bad "G2: the escaped newline path lookup failed"
fi
if [ -z "$(lookup_field 'weird	dir/Cargo.lock' tag)" ] \
   && [ -z "$(lookup_field 'absent/Cargo.lock' tag)" ]; then
  ok "G2 control: the RAW (unescaped) spelling and an absent path both return nothing — the lookup is not hardwired"
else
  bad "G2 control: the lookup answers for a path the report does not carry"
fi

# --- G5: blob object ids are 64 hex in a SHA-256 repository -------------------------
# The lockfile carve-out required a 40-char value, so on a SHA-256 repo it could NEVER
# admit and every gate-driven `Cargo.lock` re-resolution became a spurious FAIL.
if git init -q --object-format=sha256 "$tmp/sha256-probe" >/dev/null 2>&1; then
  r_s256=$(mkrepo sha256-repo --object-format=sha256)
  s256_head=$( cd "$r_s256" && git rev-parse HEAD )
  if [ "${#s256_head}" -eq 64 ]; then
    ok "G5: the fixture really is a SHA-256 repository (64-hex object ids)"
  else
    bad "G5: the SHA-256 fixture was not created (head is ${#s256_head} chars)"
  fi
  sum="$tmp/sha256-lock.txt"
  ( cd "$r_s256" && env AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST=terminal \
      AGENT_GATE_TREE_SELFTEST_MUTATE=Cargo.lock \
      bash "$r_s256/scripts/agent-gate.sh" >"$tmp/sha256-lock.out" 2>&1 ); rc=$?
  if [ "$rc" -eq 0 ] && grep -qE '^tree-integrity: PASS \(lockfile-settled: Cargo.lock ' "$sum" 2>/dev/null; then
    ok "G5: the lockfile carve-out admits a 64-hex blob id (SHA-256 repo) instead of failing spuriously"
  else
    bad "G5: a lockfile-only change on a SHA-256 repo did not take the non-fatal class (rc=$rc)"
    grep -E '^tree-integrity:|^RESULT:' "$sum" 2>/dev/null
  fi
  ( cd "$r_s256" && git checkout -q -- Cargo.lock )
  # …and the carve-out is still CLOSED there — accepting BOTH id lengths must not turn it
  # into a path match. The F3 shape, on the SHA-256 fixture: an UNTRACKED `…/Cargo.lock`
  # created MID-RUN by the component itself is a real mutation and stays fatal.
  sum="$tmp/sha256-untracked.txt"; out="$tmp/sha256-untracked.out"
  FAKE_CARGO_CREATE="$r_s256/vendor/Cargo.lock" run_gate "$r_s256" "$sum" "$out" --only fmt; rc=$?
  assert_named_fail "G5 control (untracked mid-run Cargo.lock, SHA-256 repo)" "$sum" "$rc"
  if grep -q 'lockfile-settled' "$sum" 2>/dev/null; then
    bad "G5: an untracked mid-run vendor/Cargo.lock took the carve-out on a SHA-256 repo"
  else
    ok "G5 control: the carve-out stays TAG-checked on a SHA-256 repo (an untracked impostor is still fatal)"
  fi
  rm -rf "$r_s256/vendor"
else
  ok "G5: SKIP — this git cannot create a SHA-256 repository; the length rule is pinned structurally below"
fi

# --- the STATIC lint: no GNU-only construct in ANY tree-integrity function ----------
# The behavioural cases above cover the paths they execute; this covers the rest. Rules are
# the classes that have actually cost this repo a round, plus the standard portability list.
# `stat -c` and `sort -z` are allowlisted in the two functions that PROBE for them (the
# probe is the portable pattern — using them unconditionally is the bug).
TREE_FNS="_tree_canon_rel _tree_excluded _tree_probe_tools _tree_sort0 _tree_digest_file \
_tree_hex_id_ok _tree_digest_ok _tree_split_identity _tree_in_list _tree_manifest_ok \
_tree_identity _tree_mtime _tree_cap_note _tree_cap_stamp _tree_capture_start \
_tree_recapture_after_slot _tree_changed_paths _tree_report_tag _tree_report_value \
_tree_lockfile_admissible _tree_change_class _tree_set_end _tree_fail_reason \
_assert_tree_integrity _tree_boundary_meta_lines _tree_boundary_fail \
_apply_tree_integrity_marker _tree_unguarded_terminal_probe _tree_finalize \
_tree_commit_meta _tree_commit_meta_render _tree_meta_render_lines _tree_meta_lines \
_tree_meta_array _tree_result"
GNU_RULES="bre-escape sed-in-place grep-perl date-d readlink-f sort-version sort-nul \
stat-gnu xargs-r find-printf mktemp-p echo-e awk-v"
GNU_ALLOW="_tree_probe_tools:stat-gnu _tree_mtime:stat-gnu _tree_probe_tools:sort-nul _tree_sort0:sort-nul"
gnu_rule_pat() {
  case "$1" in
    bre-escape)   printf '%s' '(sed|grep)[[:space:]].*\\[tsSwWdb+|]' ;;
    sed-in-place) printf '%s' 'sed[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-i([[:space:]]|$)' ;;
    grep-perl)    printf '%s' 'grep[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-[a-zA-Z]*P([[:space:]]|$)' ;;
    date-d)       printf '%s' 'date[[:space:]]+(-d([[:space:]]|$)|--date)' ;;
    readlink-f)   printf '%s' 'readlink[[:space:]]+-[a-zA-Z]*f' ;;
    sort-version) printf '%s' 'sort[[:space:]]+(-[a-zA-Z]*V([[:space:]]|$)|--version-sort)' ;;
    sort-nul)     printf '%s' 'sort[[:space:]]+(-[a-zA-Z]*z([[:space:]]|$)|--zero-terminated)' ;;
    stat-gnu)     printf '%s' 'stat[[:space:]]+-c' ;;
    xargs-r)      printf '%s' 'xargs[[:space:]]+(-[a-zA-Z]*r([[:space:]]|$)|--no-run-if-empty)' ;;
    find-printf)  printf '%s' 'find[[:space:]].*-printf' ;;
    mktemp-p)     printf '%s' 'mktemp[[:space:]]+(-p([[:space:]]|$)|--tmpdir)' ;;
    echo-e)       printf '%s' 'echo[[:space:]]+-e([[:space:]]|$)' ;;
    awk-v)        printf '%s' 'awk[[:space:]].*[[:space:]]-v[[:space:]]' ;;
  esac
}
gnu_allowed() { case " $GNU_ALLOW " in *" $1:$2 "*) return 0 ;; esac; return 1; }
# gnu_only_hits <file> <fn…> -> one "<fn>/<rule>: <line>" per violation; rc 1 when any.
gnu_only_hits() {
  local f="$1"; shift
  local fn body rule hit found=""
  # shellcheck disable=SC2086  # intentional word-split over the space-separated name lists
  for fn in $*; do
    body=$(fn_body "$f" "$fn" | sed 's/[[:space:]]*#.*$//')
    [ -n "$body" ] || continue
    for rule in $GNU_RULES; do
      gnu_allowed "$fn" "$rule" && continue
      hit=$(printf '%s\n' "$body" | grep -nE "$(gnu_rule_pat "$rule")" | head -1)
      [ -n "$hit" ] && found="${found}${fn}/${rule}: ${hit}"$'\n'
    done
  done
  [ -z "$found" ] || { printf '%s' "$found"; return 1; }
  return 0
}
# The inventory is asserted first: a typo'd or deleted function name would make the lint
# scan nothing and pass vacuously.
missing_fns=""
for fn in $TREE_FNS; do
  [ -n "$(fn_body "$GATE" "$fn")" ] || missing_fns="${missing_fns:+$missing_fns }$fn"
done
n_tree_fns=$(printf '%s\n' $TREE_FNS | grep -c . | tr -d ' ')
if [ -z "$missing_fns" ] && [ "$n_tree_fns" -ge 30 ]; then
  ok "PORTABILITY: the lint covers all $n_tree_fns tree-integrity functions (inventory intact)"
else
  bad "PORTABILITY: the lint's function inventory is stale — not found in the gate: ${missing_fns:-<none>} (n=$n_tree_fns)"
fi
if gnu_hits=$(gnu_only_hits "$GATE" $TREE_FNS); then
  ok "PORTABILITY: no GNU-only construct in any tree-integrity function ($(printf '%s\n' $GNU_RULES | grep -c . | tr -d ' ') rules)"
else
  bad "PORTABILITY: GNU-only construct(s) in the tree-integrity code:"
  printf '%s' "$gnu_hits"
fi
# …and the PROOF that each rule can fail: one mutant per rule, in a NON-allowlisted
# function, asserted to be caught. A lint nobody has seen fail is not a lint.
lint_caught=0; lint_total=0
while IFS='|' read -r rule line; do
  [ -n "$rule" ] || continue
  lint_total=$(( lint_total + 1 ))
  printf '_tree_changed_paths() {\n  %s\n}\n' "$line" > "$tmp/lint-mutant.sh"
  if gnu_only_hits "$tmp/lint-mutant.sh" _tree_changed_paths >/dev/null 2>&1; then
    bad "PORTABILITY: the lint does NOT catch the '$rule' mutant ($line)"
  else
    lint_caught=$(( lint_caught + 1 ))
  fi
done <<'MUTANTS'
bre-escape|comm -3 a b | sed 's/^\t//'
sed-in-place|sed -i 's/a/b/' f
grep-perl|grep -P '[0-9]' f
date-d|date -d @1 +%s
readlink-f|readlink -f "$1"
sort-version|sort -V f
sort-nul|git ls-files -z | sort -z
stat-gnu|stat -c '%Y' -- "$1"
xargs-r|xargs -r echo
find-printf|find . -printf '%p'
mktemp-p|mktemp -p /tmp
echo-e|echo -e "a"
awk-v|awk -F'\t' -v p="$2" '$4 == p { print }' "$1"
MUTANTS
if [ "$lint_caught" -eq "$lint_total" ] && [ "$lint_total" -eq 13 ]; then
  ok "PORTABILITY: every one of the $lint_total lint rules is proved discriminating (one mutant each)"
else
  bad "PORTABILITY: only $lint_caught of $lint_total lint rules caught their mutant"
fi
# …and a NON-vacuity control: a portable body must produce NO hit, so the lint is not
# simply flagging everything.
cat > "$tmp/lint-clean.sh" <<'CLEANFN'
_tree_changed_paths() {
  LC_ALL=C comm -3 "$1" "$2" | awk -F'\t' '$1 == "" { print $5; next } { print $4 }'
}
CLEANFN
if gnu_only_hits "$tmp/lint-clean.sh" _tree_changed_paths >/dev/null 2>&1; then
  ok "PORTABILITY control: a portable comm|awk body produces no lint hit (the lint is not flagging everything)"
else
  bad "PORTABILITY control: the lint flags a portable body — it would block correct code"
  gnu_only_hits "$tmp/lint-clean.sh" _tree_changed_paths
fi
# The structural half of G5: the blob-id rule is SHARED, never a second hard-coded length.
if body_has "$(fn_body "$GATE" _tree_lockfile_admissible)" '  _tree_hex_id_ok "$vb" || return 1' \
   && ! printf '%s\n' "$(fn_body "$GATE" _tree_lockfile_admissible)" | grep -q '"${#vb}" -eq 40'; then
  ok "G5: the carve-out reuses _tree_hex_id_ok (40|64) — no second, hard-coded length rule"
else
  bad "G5: the lockfile carve-out carries its own blob-id length rule again"
fi
if body_has "$(fn_body "$GATE" _tree_digest_ok)" '  _tree_hex_id_ok "$1" || return 1'; then
  ok "G5: _tree_digest_ok is built on the same shared hex-id rule"
else
  bad "G5: _tree_digest_ok no longer shares the hex-id rule with the carve-out"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
