#!/usr/bin/env bash
#
# bump-version.sh — single source of truth for the CQLite release version across
# every publish manifest (issue #2652, epic #2636).
#
# A CQLite release tag (v*) fans out to 7+ independent publish workflows
# (crates.io, PyPI, npm, Maven Central, the cqlite-flight GHCR image, Homebrew,
# GitHub releases). Each of those registries is IMMUTABLE and the version lives
# in FOUR hand-edited manifest fields:
#
#   1. Cargo.toml            [package].version          (workspace-root package)
#   2. Cargo.toml            [workspace.package].version (inherited by every crate)
#   3. bindings/python/pyproject.toml  [project].version
#   4. bindings/node/package.json      .version
#
# Editing them by hand is how a release train ships a tag whose manifests
# disagree — one lane publishes, a later lane hard-fails on a version mismatch,
# and the immutable registries are now half-populated. This script makes the
# four fields move together and gives CI + the release preflight a single
# agreement check.
#
# Commands:
#   bump-version.sh current            Print the agreed version. Non-zero if the
#                                      four manifest fields disagree.
#   bump-version.sh check [VERSION]    Assert the four fields agree. If VERSION
#                                      is given (leading `v` allowed, e.g. a
#                                      release tag), also assert they equal it.
#                                      This is the CI / preflight agreement gate.
#   bump-version.sh set VERSION        Rewrite all four fields to VERSION
#                                      (atomically: every file is validated in a
#                                      staging copy before ANY file is moved into
#                                      place), then re-check agreement.
#
# Options:
#   --root DIR   Operate on the manifests under DIR instead of the repo root
#                inferred from this script's location (used by the self-test to
#                run against a throwaway copy). Also honored via
#                CQLITE_VERSION_ROOT.
#
# VERSION is validated against a strict semver allowlist so a crafted value can
# never be written into a manifest or flow into downstream tooling:
#   ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$
#
# Pure awk/grep/sed — no node/ruby/python/cargo — so it runs identically in the
# release preflight, in CI, and locally.

set -euo pipefail

SEMVER_RE='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'

die() {
  echo "bump-version: $*" >&2
  exit 1
}

usage() {
  sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# --- root resolution ---------------------------------------------------------
ROOT="${CQLITE_VERSION_ROOT:-}"

# --- manifest readers --------------------------------------------------------
# Read the `version = "X"` value inside a specific TOML section (e.g. "[package]"
# or "[workspace.package]"). Section-aware so the [package] and
# [workspace.package] versions in one Cargo.toml are read independently and a
# `version = ...` in some other section is never picked up.
read_toml_section_version() {
  local file="$1" section="$2"
  awk -v want="$section" '
    /^\[/ { in_s = ($0 == want) }
    in_s && /^[[:space:]]*version[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }
  ' "$file"
}

# Read the top-level "version" string from a package.json (first occurrence).
read_json_version() {
  local file="$1"
  awk '
    !done && /^[[:space:]]*"version"[[:space:]]*:/ {
      if (match($0, /"version"[[:space:]]*:[[:space:]]*"[^"]*"/)) {
        s = substr($0, RSTART, RLENGTH)
        sub(/^"version"[[:space:]]*:[[:space:]]*"/, "", s)
        sub(/"$/, "", s)
        print s
        done = 1
      }
    }
  ' "$file"
}

CARGO_TOML=""
PYPROJECT=""
PACKAGE_JSON=""

resolve_root() {
  if [ -z "$ROOT" ]; then
    ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  fi
  [ -d "$ROOT" ] || die "root '$ROOT' is not a directory"
  CARGO_TOML="$ROOT/Cargo.toml"
  PYPROJECT="$ROOT/bindings/python/pyproject.toml"
  PACKAGE_JSON="$ROOT/bindings/node/package.json"
  local f
  for f in "$CARGO_TOML" "$PYPROJECT" "$PACKAGE_JSON"; do
    [ -f "$f" ] || die "manifest not found: $f (wrong --root?)"
  done
}

# Emit each tracked field as "<label>\t<version>" so callers can report which
# field diverged, not just that something did.
read_all_versions() {
  printf 'Cargo.toml [package]\t%s\n' "$(read_toml_section_version "$CARGO_TOML" '[package]')"
  printf 'Cargo.toml [workspace.package]\t%s\n' "$(read_toml_section_version "$CARGO_TOML" '[workspace.package]')"
  printf 'pyproject.toml [project]\t%s\n' "$(read_toml_section_version "$PYPROJECT" '[project]')"
  printf 'package.json\t%s\n' "$(read_json_version "$PACKAGE_JSON")"
}

# --- agreement check ---------------------------------------------------------
# check [expected-version]
# Prints every field's value; exits non-zero (with a report) on any divergence,
# an empty field, or a mismatch against the expected version when one is given.
cmd_check() {
  resolve_root
  local expected="${1:-}"
  expected="${expected#v}"
  if [ -n "$expected" ] && ! printf '%s' "$expected" | grep -Eq "$SEMVER_RE"; then
    die "expected version '$expected' is not valid semver ($SEMVER_RE)"
  fi

  local versions first="" ok=1 label ver
  versions="$(read_all_versions)"

  while IFS=$'\t' read -r label ver; do
    if [ -z "$ver" ]; then
      echo "  MISSING  $label" >&2
      ok=0
      continue
    fi
    if [ -z "$first" ]; then
      first="$ver"
    elif [ "$ver" != "$first" ]; then
      ok=0
    fi
    printf '  %-32s %s\n' "$label" "$ver"
  done <<< "$versions"

  if [ "$ok" -ne 1 ]; then
    echo "::error::manifest versions disagree — the release train would half-publish" >&2
    return 1
  fi
  if [ -n "$expected" ] && [ "$first" != "$expected" ]; then
    echo "::error::manifest version '$first' does not match expected '$expected'" >&2
    return 1
  fi
  if [ -n "$expected" ]; then
    echo "OK: all manifests agree at $first (matches expected $expected)"
  else
    echo "OK: all manifests agree at $first"
  fi
}

cmd_current() {
  resolve_root
  local versions first="" label ver ok=1
  versions="$(read_all_versions)"
  while IFS=$'\t' read -r label ver; do
    [ -n "$ver" ] || { ok=0; break; }
    if [ -z "$first" ]; then first="$ver"; elif [ "$ver" != "$first" ]; then ok=0; break; fi
  done <<< "$versions"
  [ "$ok" -eq 1 ] && [ -n "$first" ] || die "manifests disagree; run 'bump-version.sh check' to see the divergence"
  printf '%s\n' "$first"
}

# --- rewriters (emit to stdout; caller stages to a temp file) ----------------
rewrite_cargo() {
  awk -v v="$1" '
    /^\[/ { sec = $0 }
    (sec == "[package]" || sec == "[workspace.package]") && /^[[:space:]]*version[[:space:]]*=/ {
      print "version = \"" v "\""
      next
    }
    { print }
  ' "$CARGO_TOML"
}

rewrite_pyproject() {
  awk -v v="$1" '
    /^\[/ { sec = $0 }
    sec == "[project]" && /^[[:space:]]*version[[:space:]]*=/ {
      print "version = \"" v "\""
      next
    }
    { print }
  ' "$PYPROJECT"
}

rewrite_package_json() {
  awk -v v="$1" '
    !done && /^[[:space:]]*"version"[[:space:]]*:/ {
      sub(/"version"[[:space:]]*:[[:space:]]*"[^"]*"/, "\"version\": \"" v "\"")
      done = 1
    }
    { print }
  ' "$PACKAGE_JSON"
}

# set VERSION — atomic-as-possible: every rewrite is staged and verified in a
# temp copy; only once ALL four fields read back as VERSION do we mv the temps
# over the originals. A failure before the mv phase leaves every manifest
# untouched. (True cross-file atomicity is impossible; this is the honest bound.)
cmd_set() {
  local version="${1:-}"
  [ -n "$version" ] || die "set requires a VERSION argument"
  version="${version#v}"
  printf '%s' "$version" | grep -Eq "$SEMVER_RE" \
    || die "refusing to set invalid version '$version' (expected $SEMVER_RE)"

  resolve_root

  local stage
  stage="$(mktemp -d "${TMPDIR:-/tmp}/bump-version.XXXXXX")"
  # shellcheck disable=SC2064  # expand $stage now so the trap cleans the right dir
  trap "rm -rf '$stage'" EXIT

  local cargo_tmp="$stage/Cargo.toml"
  local py_tmp="$stage/pyproject.toml"
  local pkg_tmp="$stage/package.json"

  rewrite_cargo "$version" > "$cargo_tmp"
  rewrite_pyproject "$version" > "$py_tmp"
  rewrite_package_json "$version" > "$pkg_tmp"

  # Verify each staged file reads back exactly VERSION in every tracked field
  # BEFORE moving anything into place. Fail closed if any field did not take.
  local got
  got="$(read_toml_section_version "$cargo_tmp" '[package]')"
  [ "$got" = "$version" ] || die "staged Cargo.toml [package] version is '$got', expected '$version'"
  got="$(read_toml_section_version "$cargo_tmp" '[workspace.package]')"
  [ "$got" = "$version" ] || die "staged Cargo.toml [workspace.package] version is '$got', expected '$version'"
  got="$(read_toml_section_version "$py_tmp" '[project]')"
  [ "$got" = "$version" ] || die "staged pyproject.toml version is '$got', expected '$version'"
  got="$(read_json_version "$pkg_tmp")"
  [ "$got" = "$version" ] || die "staged package.json version is '$got', expected '$version'"

  # Commit phase: move the validated temps into place.
  mv "$cargo_tmp" "$CARGO_TOML"
  mv "$py_tmp" "$PYPROJECT"
  mv "$pkg_tmp" "$PACKAGE_JSON"

  echo "Set all manifests to $version"
  cmd_check "$version"
}

# --- argument parsing --------------------------------------------------------
CMD=""
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --root)
      [ $# -ge 2 ] || die "--root requires a directory"
      ROOT="$2"
      shift 2
      ;;
    --root=*)
      ROOT="${1#--root=}"
      shift
      ;;
    -h|--help)
      usage 0
      ;;
    current|check|set)
      CMD="$1"
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

[ -n "$CMD" ] || usage 1

case "$CMD" in
  current) cmd_current "${ARGS[@]:-}" ;;
  check)   cmd_check "${ARGS[@]:-}" ;;
  set)     cmd_set "${ARGS[@]:-}" ;;
  *)       usage 1 ;;
esac
