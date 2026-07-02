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
#   5. Cargo.toml                             ([profile.release-unwind] must set panic = "unwind")
#
# Check 5 closes the loophole where the build commands correctly select
# `--profile release-unwind` but the profile itself is deleted or flipped to
# `panic = "abort"` — which would silently strip the panic firewall from the
# shipped bindings while every build-command check still passed.
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

# A `--release` token: bounded by start-of-value / EOL or a whitespace, quote
# (" or '), or `=` delimiter. Matches `--release`, `"--release"`, `--release'`,
# `x=--release`. Does NOT match `--profile release-unwind` (no `--` before
# `release`) nor `--release-unwind` (trailing `-` is not a delimiter). Using an
# explicit delimiter class instead of `[[:space:]]|$` closes the loophole where
# a quoted or `=`-delimited `--release` slipped past the abort-build negative
# check. (In the double-quoted string \" is a literal ", ' is literal, and \$ is
# the ERE end-of-line anchor.)
RELEASE_TOKEN_RE="(^|[[:space:]\"'=])--release([[:space:]\"'=]|\$)"

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
  local cargo_toml="$root/Cargo.toml"

  # --- 1. Python wheel workflow -------------------------------------------
  if [ ! -f "$py_wf" ]; then
    echo "FAIL - python-release.yml missing (fail-closed): $py_wf"
    fails=$((fails + 1))
  else
    # Inspect ONLY the maturin-action `args:` value(s) that drive the build, not
    # the whole file. A `#`-comment line (first non-space char is `#`) never
    # begins with `args:` so it is excluded by the anchored match; a trailing
    # ` #...` inline comment is stripped. This closes two false-negative gaps:
    #   - a commented / unrelated `--profile release-unwind` mention elsewhere in
    #     the YAML can no longer satisfy the positive (present) check, and
    #   - a quoted / `=`-delimited `--release` can no longer slip past the
    #     negative (absent) check.
    # Fail CLOSED: if no `args:` line is found, treat it as non-compliant.
    local py_args
    py_args=$(grep -E '^[[:space:]]*args[[:space:]]*:' "$py_wf" | sed -E 's/[[:space:]]+#.*$//')
    if [ -z "$py_args" ]; then
      echo "FAIL - python-release.yml has no maturin-action 'args:' line (fail-closed)"
      fails=$((fails + 1))
    else
      # At least one args value must select the unwind profile (the wheel build).
      if ! printf '%s\n' "$py_args" | grep -qF -- "$UNWIND_FLAG"; then
        echo "FAIL - python-release.yml 'args:' does not select '${UNWIND_FLAG}' for the wheel build"
        fails=$((fails + 1))
      fi
      # No args value may pass --release (abort).
      if printf '%s\n' "$py_args" | grep -qE -- "$RELEASE_TOKEN_RE"; then
        echo "FAIL - python-release.yml 'args:' still passes '--release' (abort) to the wheel build"
        fails=$((fails + 1))
      fi
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
    elif printf '%s\n' "$maturin_section" | grep -qE -- "$RELEASE_TOKEN_RE"; then
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
      if printf '%s\n' "$build_line" | grep -qE -- "$RELEASE_TOKEN_RE"; then
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
    if grep -qE -- "$RELEASE_TOKEN_RE" "$node_wf"; then
      echo "FAIL - node-release.yml drives an abort build with '--release'"
      fails=$((fails + 1))
    fi
  fi

  # --- 5. Workspace Cargo.toml [profile.release-unwind] -------------------
  # The build commands above select `--profile release-unwind`; that profile
  # is only a firewall if it actually exists and pins `panic = "unwind"`.
  # Fail CLOSED: a missing Cargo.toml, an absent section, an unset panic, or
  # any panic value other than "unwind" (e.g. "abort") is a violation.
  if [ ! -f "$cargo_toml" ]; then
    echo "FAIL - Cargo.toml missing (fail-closed): $cargo_toml"
    fails=$((fails + 1))
  elif ! grep -qE '^\[profile\.release-unwind\]' "$cargo_toml"; then
    echo "FAIL - Cargo.toml has no [profile.release-unwind] section (fail-closed)"
    fails=$((fails + 1))
  else
    # Extract the [profile.release-unwind] table (until the next table header).
    local profile_section
    profile_section=$(awk '
      /^\[profile\.release-unwind\]/ { in_sec = 1; next }
      /^\[/                          { in_sec = 0 }
      in_sec                         { print }
    ' "$cargo_toml")
    local panic_line
    panic_line=$(printf '%s\n' "$profile_section" | grep -E '^[[:space:]]*panic[[:space:]]*=')
    if [ -z "$panic_line" ]; then
      echo "FAIL - [profile.release-unwind] does not set panic (fail-closed; must be \"unwind\")"
      fails=$((fails + 1))
    elif ! printf '%s\n' "$panic_line" | grep -qE '^[[:space:]]*panic[[:space:]]*=[[:space:]]*"unwind"[[:space:]]*$'; then
      echo "FAIL - [profile.release-unwind] panic is not \"unwind\" (abort/other strips the FFI firewall)"
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
  cat >"$d/Cargo.toml" <<'EOF'
[profile.release]
panic = "abort"

[profile.release-unwind]
inherits = "release"
panic = "unwind"

[profile.bench]
debug = true
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

  # (e) Profile pinned panic = "unwind" -> guard PASSES.
  #     (The compliant fixture already ships this Cargo.toml; assert explicitly.)
  local prof_ok="$tmp/prof_ok"
  write_compliant_fixture "$prof_ok"
  if check_definitions "$prof_ok" >/dev/null; then
    sok "Cargo.toml [profile.release-unwind] panic=unwind passes"
  else
    sbad "Cargo.toml panic=unwind should pass but did not"
    check_definitions "$prof_ok"
  fi

  # (f) Profile flipped to panic = "abort" -> guard FAILS closed.
  local prof_abort="$tmp/prof_abort"
  write_compliant_fixture "$prof_abort"
  cat >"$prof_abort/Cargo.toml" <<'EOF'
[profile.release]
panic = "abort"

[profile.release-unwind]
inherits = "release"
panic = "abort"

[profile.bench]
debug = true
EOF
  if check_definitions "$prof_abort" >/dev/null; then
    sbad "[profile.release-unwind] panic=abort should FAIL but passed"
  else
    sok "[profile.release-unwind] panic=abort fails closed"
  fi

  # (g) Profile section deleted entirely -> guard FAILS closed.
  local prof_missing="$tmp/prof_missing"
  write_compliant_fixture "$prof_missing"
  cat >"$prof_missing/Cargo.toml" <<'EOF'
[profile.release]
panic = "abort"

[profile.bench]
debug = true
EOF
  if check_definitions "$prof_missing" >/dev/null; then
    sbad "missing [profile.release-unwind] section should FAIL but passed"
  else
    sok "missing [profile.release-unwind] section fails closed"
  fi

  # (h) Python workflow whose wheel-build `args` uses a QUOTED "--release" ->
  #     the quote is the token delimiter, so this must FAIL closed. The old
  #     `--release([[:space:]]|$)` matcher missed it (quote != whitespace/EOL).
  local py_quoted="$tmp/py_quoted"
  write_compliant_fixture "$py_quoted"
  cat >"$py_quoted/.github/workflows/python-release.yml" <<'EOF'
      - name: Build wheel
        uses: PyO3/maturin-action@v1
        with:
          args: "--release --out dist"
EOF
  if check_definitions "$py_quoted" >/dev/null; then
    sbad "python-release.yml quoted \"--release\" args should FAIL closed but passed"
  else
    sok "python-release.yml quoted \"--release\" args fails closed"
  fi

  # (i) Python workflow that mentions `--profile release-unwind` ONLY in a
  #     COMMENT while the real `args` build with `--release` -> must FAIL closed.
  #     The old whole-file positive match was satisfied by the comment and the
  #     abort build slipped through.
  local py_comment="$tmp/py_comment"
  write_compliant_fixture "$py_comment"
  cat >"$py_comment/.github/workflows/python-release.yml" <<'EOF'
      - name: Build wheel
        uses: PyO3/maturin-action@v1
        with:
          # TODO: switch to --profile release-unwind (issue #1440)
          args: --release --out dist
EOF
  if check_definitions "$py_comment" >/dev/null; then
    sbad "python-release.yml comment-only profile w/ --release args should FAIL closed but passed"
  else
    sok "python-release.yml comment-only profile w/ --release args fails closed"
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
    echo "ok   - all binding build definitions select '${UNWIND_FLAG}' (no --release) and Cargo.toml [profile.release-unwind] pins panic = \"unwind\""
  else
    printf '%s\n' "$findings"
    echo "FAIL - $real_fails binding build definition(s) would ship an abort-compiled artifact"
    rc=1
  fi

  return "$rc"
}

main "$@"
