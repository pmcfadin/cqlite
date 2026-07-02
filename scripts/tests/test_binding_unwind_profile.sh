#!/usr/bin/env bash
# Fail-closed guard for issue #1440 (OpenSpec change `panic-unwind-profile`,
# capability `binding-panic-firewall`).
#
# The shipped Python wheel and Node.js npm prebuild MUST be compiled
# `panic = "unwind"` so the PyO3 / napi-rs `catch_unwind`->exception firewall at
# the FFI boundary is active; a core panic must become a catchable CqliteError,
# not an abort that kills the host process. CLI/core stay `panic = "abort"`.
#
# This guard is deterministic and OFFLINE (no build, no network, no datasets).
# It reads the four binding build DEFINITIONS and FAILS if any of them would
# produce an abort-compiled artifact — i.e. uses `--release` or omits
# `--profile release-unwind`. It fails CLOSED: a definition that is missing or
# unparseable is treated as non-compliant ("not found" != "compliant").
#
# Definitions inspected (relative to the repo root):
#   1. .github/workflows/python-release.yml   (maturin wheel-build args)
#   2. bindings/python/pyproject.toml         ([tool.maturin] must not re-pin --release)
#   3. bindings/node/package.json             ("build" script)
#   4. .github/workflows/node-release.yml     (must not drive an abort build)
#
# Run standalone:   bash scripts/tests/test_binding_unwind_profile.sh
# Or via the gate:  scripts/agent-gate.sh runs it as the `binding-unwind-profile`
#                   component. The default invocation runs the negative-path
#                   self-check (fail-closed proof) AND the real-tree check; both
#                   must pass for exit 0.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

UNWIND_PROFILE="release-unwind"
UNWIND_FLAG="--profile ${UNWIND_PROFILE}"

# ---------------------------------------------------------------------------
# check_definitions <root> : inspect the four binding build definitions rooted
# at <root>. Prints a "FAIL - <reason>" line for every violation and returns the
# number of violations (0 == compliant). Fail-closed: a missing/unparseable
# definition is a violation.
# ---------------------------------------------------------------------------
check_definitions() {
  local root="$1"
  local fails=0
  local py_wf="$root/.github/workflows/python-release.yml"
  local pyproject="$root/bindings/python/pyproject.toml"
  local pkg="$root/bindings/node/package.json"
  local node_wf="$root/.github/workflows/node-release.yml"

  # --- 1. Python wheel workflow -------------------------------------------
  if [ ! -f "$py_wf" ]; then
    echo "FAIL - python-release.yml missing (fail-closed): $py_wf"
    fails=$((fails + 1))
  else
    # The wheel build must select the unwind profile ...
    if ! grep -qF -- "$UNWIND_FLAG" "$py_wf"; then
      echo "FAIL - python-release.yml does not select '${UNWIND_FLAG}' for the wheel build"
      fails=$((fails + 1))
    fi
    # ... and must never pass --release (abort). `--profile release-unwind`
    # does not contain the substring `--release`, so this match is exact.
    if grep -qE -- '--release([[:space:]]|$)' "$py_wf"; then
      echo "FAIL - python-release.yml still passes '--release' (abort) to the wheel build"
      fails=$((fails + 1))
    fi
  fi

  # --- 2. Python pyproject [tool.maturin] ---------------------------------
  if [ ! -f "$pyproject" ]; then
    echo "FAIL - pyproject.toml missing (fail-closed): $pyproject"
    fails=$((fails + 1))
  else
    # Extract the [tool.maturin] table (until the next top-level table header).
    local maturin_section
    maturin_section=$(awk '
      /^\[tool\.maturin\]/ { in_sec = 1; next }
      /^\[/               { in_sec = 0 }
      in_sec              { print }
    ' "$pyproject")
    if ! grep -qF '[tool.maturin]' "$pyproject"; then
      echo "FAIL - pyproject.toml has no [tool.maturin] table (fail-closed)"
      fails=$((fails + 1))
    elif printf '%s\n' "$maturin_section" | grep -qE -- '--release([[:space:]"'\'']|$)'; then
      echo "FAIL - pyproject.toml [tool.maturin] re-pins '--release' (abort)"
      fails=$((fails + 1))
    fi
  fi

  # --- 3. Node package.json build script ----------------------------------
  if [ ! -f "$pkg" ]; then
    echo "FAIL - package.json missing (fail-closed): $pkg"
    fails=$((fails + 1))
  else
    # Isolate the "build": "..." script line (not build:debug / postbuild).
    local build_line
    build_line=$(grep -E '"build"[[:space:]]*:' "$pkg")
    if [ -z "$build_line" ]; then
      echo "FAIL - package.json has no \"build\" script (fail-closed)"
      fails=$((fails + 1))
    else
      if ! printf '%s\n' "$build_line" | grep -qF -- "$UNWIND_FLAG"; then
        echo "FAIL - package.json \"build\" script does not select '${UNWIND_FLAG}'"
        fails=$((fails + 1))
      fi
      if printf '%s\n' "$build_line" | grep -qE -- '--release([[:space:]"'\'']|$)'; then
        echo "FAIL - package.json \"build\" script still passes '--release' (abort)"
        fails=$((fails + 1))
      fi
    fi
  fi

  # --- 4. Node release workflow -------------------------------------------
  # This workflow drives the build via `npm run build` (which uses the
  # package.json build script checked above). It must NOT drive a separate
  # abort build; fail if it introduces a bare `--release`.
  if [ ! -f "$node_wf" ]; then
    echo "FAIL - node-release.yml missing (fail-closed): $node_wf"
    fails=$((fails + 1))
  else
    if grep -qE -- '--release([[:space:]]|$)' "$node_wf"; then
      echo "FAIL - node-release.yml drives an abort build with '--release'"
      fails=$((fails + 1))
    fi
  fi

  return "$fails"
}

# ---------------------------------------------------------------------------
# Negative-path self-check: prove the guard fails CLOSED on a missing definition
# and on an abort definition, and PASSES on a compliant fixture set. Uses
# throwaway fixtures under a temp dir, never the real tree.
# ---------------------------------------------------------------------------
SELF_PASS=0
SELF_FAIL=0
sok()  { printf 'ok   - selftest: %s\n' "$1"; SELF_PASS=$((SELF_PASS + 1)); }
sbad() { printf 'FAIL - selftest: %s\n' "$1"; SELF_FAIL=$((SELF_FAIL + 1)); }

# Write a COMPLIANT fixture tree at $1.
write_compliant_fixture() {
  local d="$1"
  mkdir -p "$d/.github/workflows" "$d/bindings/python" "$d/bindings/node"
  cat >"$d/.github/workflows/python-release.yml" <<'EOF'
      - name: Build wheel
        uses: PyO3/maturin-action@v1
        with:
          args: --profile release-unwind --out dist
EOF
  cat >"$d/bindings/python/pyproject.toml" <<'EOF'
[tool.maturin]
features = ["pyo3/extension-module"]
EOF
  cat >"$d/bindings/node/package.json" <<'EOF'
{
  "scripts": {
    "build": "napi build --platform --profile release-unwind --features write-support"
  }
}
EOF
  cat >"$d/.github/workflows/node-release.yml" <<'EOF'
      - name: Build native module
        run: npm run build
EOF
}

run_selftest() {
  local tmp
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/binding-unwind-selftest.XXXXXX")

  # (a) Compliant fixture -> guard PASSES (0 violations).
  local good="$tmp/good"
  write_compliant_fixture "$good"
  if check_definitions "$good" >/dev/null; then
    sok "compliant fixture passes"
  else
    sbad "compliant fixture should pass but did not"
    check_definitions "$good"
  fi

  # (b) Missing definition -> guard FAILS closed. Remove the python workflow.
  local missing="$tmp/missing"
  write_compliant_fixture "$missing"
  rm -f "$missing/.github/workflows/python-release.yml"
  if check_definitions "$missing" >/dev/null; then
    sbad "missing python-release.yml should FAIL closed but passed"
  else
    sok "missing definition fails closed"
  fi

  # (c) Abort re-pin -> guard FAILS. Flip the node build script back to --release.
  local abort="$tmp/abort"
  write_compliant_fixture "$abort"
  cat >"$abort/bindings/node/package.json" <<'EOF'
{
  "scripts": {
    "build": "napi build --platform --release --features write-support"
  }
}
EOF
  if check_definitions "$abort" >/dev/null; then
    sbad "abort (--release) build script should FAIL but passed"
  else
    sok "abort (--release) build script fails"
  fi

  # (d) Empty tree -> every definition missing -> FAILS closed.
  local empty="$tmp/empty"
  mkdir -p "$empty"
  if check_definitions "$empty" >/dev/null; then
    sbad "empty tree should FAIL closed but passed"
  else
    sok "empty tree fails closed"
  fi

  # Best-effort cleanup; the guard is offline so nothing else touches this dir.
  rm -rf "$tmp"

  echo "---- selftest: passed: $SELF_PASS  failed: $SELF_FAIL"
  [ "$SELF_FAIL" -eq 0 ]
}

# ---------------------------------------------------------------------------
main() {
  local rc=0

  echo ">>> binding-unwind-profile: negative-path self-check (fail-closed proof)"
  if ! run_selftest; then
    echo "FAIL - self-check did not hold; guard logic is unsound"
    rc=1
  fi

  echo ">>> binding-unwind-profile: checking real build definitions under $REPO_ROOT"
  local findings
  findings=$(check_definitions "$REPO_ROOT")
  local real_fails=$?
  if [ "$real_fails" -eq 0 ]; then
    echo "ok   - all four binding build definitions select '${UNWIND_FLAG}' (no --release)"
  else
    printf '%s\n' "$findings"
    echo "FAIL - $real_fails binding build definition(s) would ship an abort-compiled artifact"
    rc=1
  fi

  return "$rc"
}

main "$@"
