#!/usr/bin/env bash
#
# Compatibility wrapper for the legacy all-CI local script.
# The maintained local validation entrypoint is scripts/local/pre-merge.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "${1:-}" in
  -h|--help|help)
    echo "scripts/local/test-all-ci-locally.sh is a compatibility wrapper."
    echo "Equivalent maintained command: scripts/local/pre-merge.sh full"
    echo
    exec "${SCRIPT_DIR}/pre-merge.sh" --help
    ;;
  "")
    ;;
  *)
    echo "error: scripts/local/test-all-ci-locally.sh does not accept arguments" >&2
    echo "Run scripts/local/pre-merge.sh [fast|core|storage|bindings|full] for selectable modes." >&2
    exit 2
    ;;
esac

echo "scripts/local/test-all-ci-locally.sh is a compatibility wrapper."
echo "Delegating to: scripts/local/pre-merge.sh full"
echo

exec "${SCRIPT_DIR}/pre-merge.sh" full "$@"
