#!/usr/bin/env bash
set -euo pipefail

# cloud-setup.sh — prepare a Claude Code on the web (cloud) session to run the
# CQLite delivery pipeline.
#
# A cloud session ships the repo-committed .claude/ (skills/agents/hooks) but not
# user-scoped config or the gitignored dataset binaries, and may not have the
# pipeline CLIs. This script makes `flow-implement` able to run the gate in a
# cloud session by ensuring:
#   1. openspec  (the spec tool the flow-* skills drive)
#   2. gh        (GitHub CLI — issues, PRs, the Project board)
#   3. the test dataset (Data.db binaries) via fetch-datasets.sh
#
# Usage (from the repo root, inside a cloud session):
#   bash test-data/scripts/cloud-setup.sh
#
# Pin note: openspec is published as @fission-ai/openspec on npm. Pin a version
# for reproducibility (OPENSPEC_VERSION); the default tracks the validated pin.

OPENSPEC_PKG="@fission-ai/openspec"
OPENSPEC_VERSION="${OPENSPEC_VERSION:-1.4.1}"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "${repo_root}"

echo "==> [1/3] Ensuring openspec is installed"
if command -v openspec >/dev/null 2>&1; then
  echo "    openspec present: $(openspec --version 2>/dev/null || echo unknown)"
elif command -v npm >/dev/null 2>&1; then
  echo "    Installing ${OPENSPEC_PKG}@${OPENSPEC_VERSION} via npm -g"
  npm install -g "${OPENSPEC_PKG}@${OPENSPEC_VERSION}"
  echo "    openspec installed: $(openspec --version 2>/dev/null || echo unknown)"
else
  echo "error: openspec is missing and npm is unavailable to install it." >&2
  echo "       Install Node/npm, then: npm install -g ${OPENSPEC_PKG}@${OPENSPEC_VERSION}" >&2
  exit 1
fi

echo "==> [2/3] Ensuring gh (GitHub CLI) is available"
if command -v gh >/dev/null 2>&1; then
  echo "    gh present: $(gh --version 2>/dev/null | head -n1)"
else
  echo "error: gh CLI not found on PATH." >&2
  echo "       Install it (https://cli.github.com/) and authenticate before driving the pipeline." >&2
  echo "       For the Project claim board, also grant the scope: gh auth refresh -s project" >&2
  exit 1
fi

echo "==> [3/3] Fetching the test dataset (Data.db binaries) so the gate can run"
if [ -f "test-data/scripts/fetch-datasets.sh" ]; then
  bash test-data/scripts/fetch-datasets.sh
  echo "    Dataset fetched into test-data/datasets"
  echo "    Run the gate with: CQLITE_DATASETS_ROOT=\$PWD/test-data/datasets scripts/agent-gate.sh"
else
  echo "error: test-data/scripts/fetch-datasets.sh not found (run from the repo root)." >&2
  exit 1
fi

echo
echo "Cloud session ready: openspec + gh + dataset present. flow-implement can run the gate here."
echo "Reminder: the two human seams (approve spec, merge PR) are GitHub-mobile-native."
