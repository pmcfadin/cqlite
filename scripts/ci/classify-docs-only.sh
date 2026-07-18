#!/usr/bin/env bash
# Docs-only classifier for the required PR gate (issue #2645, epic #2636).
#
# pr-gate.yml is a REQUIRED status check with no path awareness: a docs- or
# board-only PR still compiles cqlite-core all-features and runs the read-time
# reconciliation oracle (#2644). This classifier lets the gate short-circuit
# such PRs to green in seconds WITHOUT using paths/paths-ignore on the workflow
# trigger — a path filter would prevent the required check from ever reporting,
# permanently blocking a PR that touches only ignored paths.
#
# Contract (pure + hermetic so it is self-testable):
#   - Reads a newline-delimited changed-file list on STDIN (one repo-relative
#     path per line; blank lines ignored).
#   - Exit 0  => DOCS-ONLY: every changed file is in the conservative docs
#                allowlist. The caller MAY skip the Rust/oracle/heavy steps.
#   - Exit 1  => FULL PATH REQUIRED (fail-closed): at least one file is NOT in
#                the docs allowlist, OR the changed set is empty/ambiguous.
#
# FAIL-CLOSED by construction: this is an ALLOWLIST. Only files that are
# unambiguously documentation short-circuit; ANY other class — Rust sources,
# Cargo manifests, test-data manifests/fixtures, .github workflows/actions,
# scripts, config, lockfiles, or an unrecognized extension — forces the full
# run. An empty changed set is ambiguous and also forces the full run.
#
# Sensitive directories (.github/, scripts/, test-data/) force the full path
# EVEN for a *.md file inside them, so a Markdown edit next to a workflow, a
# gate script, or a parity manifest can never smuggle a code-relevant change
# past the gate.
#
# The script prints a one-word verdict ("docs-only" / "full") to STDOUT and a
# human-readable reason to STDERR; callers key off the EXIT CODE.

set -euo pipefail

# is_docs_file <path> -> 0 if the single path is unambiguously a docs file.
# Fail-closed: unrecognized => 1 (not docs).
is_docs_file() {
  local path="$1"

  # Sensitive dirs force the full path regardless of extension.
  case "$path" in
    .github/* | scripts/* | test-data/*) return 1 ;;
  esac

  case "$path" in
    docs/*) return 0 ;;
    *.md | *.markdown) return 0 ;;
    *.png | *.jpg | *.jpeg | *.gif | *.svg | *.webp | *.ico) return 0 ;;
    LICENSE | LICENSE.* | NOTICE | NOTICE.*) return 0 ;;
    *) return 1 ;;
  esac
}

main() {
  local saw_file=0
  local path
  local non_docs=""

  while IFS= read -r path || [ -n "$path" ]; do
    # Skip blank lines (trailing newline, empty diff output).
    [ -n "$path" ] || continue
    saw_file=1
    if ! is_docs_file "$path"; then
      non_docs="$path"
      break
    fi
  done

  if [ "$saw_file" -eq 0 ]; then
    echo "full"
    echo "classify-docs-only: empty/ambiguous changed set -> FULL PATH (fail-closed)" >&2
    return 1
  fi

  if [ -n "$non_docs" ]; then
    echo "full"
    echo "classify-docs-only: non-docs file '$non_docs' -> FULL PATH (fail-closed)" >&2
    return 1
  fi

  echo "docs-only"
  echo "classify-docs-only: all changed files are docs -> short-circuit to green" >&2
  return 0
}

main "$@"
