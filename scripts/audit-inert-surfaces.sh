#!/usr/bin/env bash
# Lightweight static audit for suspicious "inert" public surfaces (issues #949/#963).
#
# Background: issue #949 exposed a process failure — agents implemented and unit-tested
# a low-level capability while leaving the intended PUBLIC path unwired. The helper was
# green; the user-facing surface never called it. This script greps for the textual
# tells of that pattern so a reviewer (or the PR author) can look before merging.
#
# This is a HEURISTIC, ADVISORY check, not a gate. It does not parse Rust; it pattern-
# matches. Findings are starting points for review, not automatic failures. Real
# verification is an end-to-end test from the public surface (see the PR template's
# "Public Surface & Wiring Evidence" section).
#
# Usage:
#   scripts/audit-inert-surfaces.sh                 # scan tracked Rust sources
#   scripts/audit-inert-surfaces.sh path1 path2     # scan specific paths
#   scripts/audit-inert-surfaces.sh --diff          # scan only files changed vs origin/main
#   scripts/audit-inert-surfaces.sh --help
#
# Exit codes:
#   0  no suspicious patterns found
#   2  suspicious patterns found (advisory — review them; not necessarily a failure)
#   1  usage / environment error
#
# POSIX-bash, no external deps beyond grep + git.

set -u

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
DIFF_MODE=0
PATHS=""

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage 0 ;;
    --diff) DIFF_MODE=1 ;;
    --*) echo "unknown flag: $1" >&2; usage 1 ;;
    *) PATHS="$PATHS $1" ;;
  esac
  shift
done

# Build the list of files to scan.
if [ "$DIFF_MODE" -eq 1 ]; then
  # A missing/invalid base must FAIL loudly rather than silently scan nothing and
  # report a false-clean (exit 0). Resolve and verify the base before diffing.
  if ! git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; then
    echo "audit-inert-surfaces: --diff requires a git repository." >&2
    exit 1
  fi
  if git -C "$REPO_ROOT" rev-parse --verify -q "origin/main^{commit}" >/dev/null 2>&1; then
    BASE="origin/main"
  elif git -C "$REPO_ROOT" rev-parse --verify -q "main^{commit}" >/dev/null 2>&1; then
    BASE="main"
  else
    echo "audit-inert-surfaces: --diff base not found (neither origin/main nor main resolves)." >&2
    echo "  fetch the base or run without --diff to scan all tracked Rust files." >&2
    exit 1
  fi
  # Capture diff output and status BEFORE piping to grep, so a diff failure
  # (e.g. shallow checkout with no merge base) is a hard error, not a false-clean.
  if ! DIFF_OUT=$(git -C "$REPO_ROOT" diff --name-only --diff-filter=ACMR "$BASE"...HEAD 2>&1); then
    echo "audit-inert-surfaces: 'git diff $BASE...HEAD' failed:" >&2
    echo "$DIFF_OUT" >&2
    echo "  (shallow clone? run 'git fetch --unshallow' or fetch the base, or drop --diff.)" >&2
    exit 1
  fi
  FILES=$(printf '%s\n' "$DIFF_OUT" | grep -E '\.rs$' || true)
elif [ -n "$PATHS" ]; then
  # shellcheck disable=SC2086
  FILES=$(git -C "$REPO_ROOT" ls-files $PATHS 2>/dev/null | grep -E '\.rs$' || true)
else
  FILES=$(git -C "$REPO_ROOT" ls-files '*.rs' 2>/dev/null || true)
fi

if [ -z "${FILES:-}" ]; then
  echo "audit-inert-surfaces: no Rust files to scan."
  exit 0
fi

# Each pattern: a label and an extended-regex. These are textual tells of an unwired
# or stubbed public surface. Tune as the codebase teaches us new tells.
#
# Patterns are intentionally broad; expect false positives (e.g. a legitimate "for now"
# comment). The point is to make a reviewer LOOK, not to block automatically.
run_pattern() {
  label="$1"; regex="$2"
  echo "== $label =="
  # -n line numbers, -I skip binary, -E extended regex.
  # shellcheck disable=SC2086
  hits=$(cd "$REPO_ROOT" && printf '%s\n' $FILES \
           | xargs grep -nIE "$regex" 2>/dev/null || true)
  if [ -n "$hits" ]; then
    printf '%s\n' "$hits"
    FOUND=1
  else
    echo "(none)"
  fi
  echo
}

FOUND=0

# 1. Explicit "not done yet" markers.
run_pattern "TODO: Implement / unimplemented / placeholder / 'for now'" \
  'TODO:? *[Ii]mplement|unimplemented!\(|todo!\(|placeholder|[Ff]or now,?|[Nn]ot yet (wired|implemented|hooked)'

# 2. Bind/params accepted but ignored — the #949 smell (e.g. `_params`, `_bind`).
#    Underscore-prefixed params on what should be a wiring point, or "ignored"/"unused".
run_pattern "Ignored params (_params/_bind/_values) on public-looking fns" \
  '\bfn +[a-z_]*\([^)]*\b_(params|bind|values|args|opts)\b|//.*(ignored|unused) (param|argument)'

# 3. Ignored / skipped tests — a capability whose only test is disabled.
#    Note: matches the #[ignore] attribute and skip markers, NOT iterator `.skip(`.
run_pattern "Ignored or skipped tests" \
  '#\[ignore|#\[cfg\(.*never.*\)\]|XFAIL|\bxfail\b'

# 4. Validation-only shells: public fn that validates then returns a default/empty.
#    Heuristic: a returned Ok(Default::default()) / Ok(vec![]) / Ok(None) near a pub fn.
run_pattern "Possible validation-only / empty-return public surface" \
  'Ok\(Default::default\(\)\)|Ok\(vec!\[\]\)|Ok\(Vec::new\(\)\)|return Ok\(None\) *; *//|=> *Ok\(None\)'

echo "----------------------------------------------------------------------"
if [ "$FOUND" -eq 1 ]; then
  cat <<'EOF'
RESULT: SUSPICIOUS SURFACES FOUND (advisory)

These are textual tells, not proof of a bug. For each hit, confirm the intended PUBLIC
surface (CQL execute / streaming / prepared / CLI / REPL / bindings) actually reaches
the new code, and that an end-to-end test exercises it. If a hit is intentional
(internal-only or feature-flagged work), say so in the PR's "Public Surface & Wiring
Evidence" section and link the follow-up issue that wires it up.
EOF
  exit 2
else
  echo "RESULT: clean — no suspicious inert-surface patterns found."
  exit 0
fi
