#!/usr/bin/env bash
#
# Regression tests for scripts/bump-version.sh (issue #2652).
#
# Fast + hermetic: every case runs against a throwaway copy of the four release
# manifests under a temp --root, so the repo's real Cargo.toml / pyproject.toml /
# package.json are NEVER touched. No network, no cargo/node/python.
#
# Run standalone:   bash scripts/tests/test_bump_version.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUMP="$SCRIPT_DIR/../bump-version.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -f "$BUMP" ] || { echo "cannot find bump-version.sh at $BUMP" >&2; exit 1; }

# --- fixture builder ---------------------------------------------------------
# Materialize a throwaway root whose four manifests hold the given start version.
# Mirrors the real files' section shape so the section-aware readers are exercised.
make_root() {
  local start="$1"
  local root
  root="$(mktemp -d "${TMPDIR:-/tmp}/bump-version-test.XXXXXX")"
  mkdir -p "$root/bindings/python" "$root/bindings/node"

  cat >"$root/Cargo.toml" <<EOF
[workspace]
members = ["a"]
resolver = "2"

[package]
name = "cqlite"
version = "$start"
edition = "2021"

[dependencies]
some-dep = { version = "1.0" }

[workspace.package]
version = "$start"
edition = "2021"
EOF

  cat >"$root/bindings/python/pyproject.toml" <<EOF
[build-system]
requires = ["maturin>=1.7,<2.0"]

[project]
name = "cqlite-py"
version = "$start"
description = "x"
EOF

  cat >"$root/bindings/node/package.json" <<EOF
{
  "name": "@cqlite/node",
  "version": "$start",
  "main": "lib/index.js"
}
EOF

  printf '%s\n' "$root"
}

field() { # field <root> <label-substring>  (fixed-string match, prints last field)
  bash "$BUMP" --root "$1" check 2>/dev/null | awk -v l="$2" 'index($0, l) {print $NF}'
}

# --- Case 1: check passes on an agreeing fixture -----------------------------
R="$(make_root 0.15.0)"
if bash "$BUMP" --root "$R" check >/dev/null 2>&1; then
  ok "check passes when all four manifests agree"
else
  bad "check should pass when all four manifests agree"
fi
if [ "$(bash "$BUMP" --root "$R" current 2>/dev/null)" = "0.15.0" ]; then
  ok "current prints the agreed version"
else
  bad "current should print 0.15.0"
fi
rm -rf "$R"

# --- Case 2: check with matching expected passes, mismatch fails -------------
R="$(make_root 0.15.0)"
if bash "$BUMP" --root "$R" check v0.15.0 >/dev/null 2>&1; then
  ok "check <expected> passes when expected matches (leading v tolerated)"
else
  bad "check v0.15.0 should pass against 0.15.0 manifests"
fi
if bash "$BUMP" --root "$R" check 0.16.0 >/dev/null 2>&1; then
  bad "check should fail when expected does not match manifests"
else
  ok "check <expected> fails on a tag/manifest mismatch"
fi
rm -rf "$R"

# --- Case 3: set rewrites all four fields (round-trip) -----------------------
R="$(make_root 0.15.0)"
if bash "$BUMP" --root "$R" set 0.16.0 >/dev/null 2>&1; then
  ok "set exits 0 on a valid version"
else
  bad "set 0.16.0 should succeed"
fi
all_new=1
for label in "Cargo.toml [package]" "Cargo.toml [workspace.package]" \
             "pyproject.toml" "package.json"; do
  v="$(field "$R" "$label")"
  [ "$v" = "0.16.0" ] || { bad "field '$label' is '$v', expected 0.16.0"; all_new=0; }
done
[ "$all_new" -eq 1 ] && ok "set rewrote all four manifest fields to 0.16.0"
# And a fresh agreement check must now pass at the new version.
if bash "$BUMP" --root "$R" check 0.16.0 >/dev/null 2>&1; then
  ok "check agrees at the new version after set"
else
  bad "check should agree at 0.16.0 after set"
fi
rm -rf "$R"

# --- Case 4: set accepts a prerelease suffix ---------------------------------
R="$(make_root 0.15.0)"
if bash "$BUMP" --root "$R" set 1.0.0-rc.1 >/dev/null 2>&1 \
   && [ "$(bash "$BUMP" --root "$R" current 2>/dev/null)" = "1.0.0-rc.1" ]; then
  ok "set accepts a semver prerelease suffix"
else
  bad "set 1.0.0-rc.1 should round-trip"
fi
rm -rf "$R"

# --- Case 5: set rejects a non-semver / injection version, leaves files intact
R="$(make_root 0.15.0)"
before="$(cat "$R/Cargo.toml")"
if bash "$BUMP" --root "$R" set '1.2; rm -rf /' >/dev/null 2>&1; then
  bad "set must reject a non-semver version"
else
  ok "set rejects a non-semver / injection version"
fi
if [ "$(cat "$R/Cargo.toml")" = "$before" ]; then
  ok "a rejected set leaves the manifests untouched"
else
  bad "a rejected set must not modify any manifest"
fi
rm -rf "$R"

# --- Case 6: check detects a divergence between manifests --------------------
R="$(make_root 0.15.0)"
# Skew ONLY package.json so the four fields disagree.
sed -i.bak 's/"version": "0.15.0"/"version": "0.14.9"/' "$R/bindings/node/package.json"
rm -f "$R/bindings/node/package.json.bak"
if bash "$BUMP" --root "$R" check >/dev/null 2>&1; then
  bad "check must fail when one manifest diverges"
else
  ok "check detects a diverged manifest (half-published-train guard)"
fi
rm -rf "$R"

# --- summary -----------------------------------------------------------------
echo "----"
echo "bump-version self-test: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
