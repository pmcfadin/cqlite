#!/usr/bin/env bash
# test_check_workflow_injection.sh — self-test for the GHA command-injection
# guard (issue #2656).
#
# Proves check-workflow-injection.sh:
#   1. PASSes on a clean fixture (attacker input passed via a quoted env: var),
#   2. FAILs on a planted `${{ github.event.issue.title }}` inlined into run:,
#   3. FAILs on a planted `${{ github.head_ref }}` inlined into run:,
#   4. does NOT flag a benign `${{ env.* }}` interpolation in run: (false-positive guard),
#   5. does NOT flag `${{ github.event.pull_request.head.sha }}` (a 40-hex SHA, not injectable),
#   6. respects the `injection-lint-allow` escape hatch,
#   7. and PASSes on the real .github/workflows tree.
# Hermetic: writes fixtures to a temp dir, no cargo/network/datasets.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD="$REPO_ROOT/scripts/ci/check-workflow-injection.sh"

if [ ! -f "$GUARD" ]; then
  echo "FAIL: guard script not found at $GUARD"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (guard is a no-op without it)"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# 1. clean fixture: attacker input flows through a quoted env: var, never inlined.
cat >"$tmp/clean.yml" <<'YML'
on: [pull_request_target]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          TITLE: ${{ github.event.issue.title }}
        run: |
          echo "processing: $TITLE"
YML
if ! bash "$GUARD" "$tmp/clean.yml" >/dev/null 2>&1; then
  echo "FAIL: guard flagged a clean env-var fixture"
  bash "$GUARD" "$tmp/clean.yml" || true
  exit 1
fi
echo "OK: clean fixture (env var) PASSes"

# 2. planted violation: issue title inlined into run:.
cat >"$tmp/bad-title.yml" <<'YML'
on: [issues]
jobs:
  triage:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "title is ${{ github.event.issue.title }}"
YML
if bash "$GUARD" "$tmp/bad-title.yml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on an inlined github.event.issue.title"
  exit 1
fi
echo "OK: planted issue-title injection is caught"

# 3. planted violation: head_ref inlined into run:.
cat >"$tmp/bad-headref.yml" <<'YML'
on: [pull_request_target]
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: git checkout ${{ github.head_ref }}
YML
if bash "$GUARD" "$tmp/bad-headref.yml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on an inlined github.head_ref"
  exit 1
fi
echo "OK: planted head_ref injection is caught"

# 4. false-positive guard: a benign ${{ env.* }} interpolation must NOT trip.
cat >"$tmp/benign-env.yml" <<'YML'
on: [push]
jobs:
  b:
    runs-on: ubuntu-latest
    env:
      REGISTRY: ghcr.io/example
    steps:
      - run: docker pull "${{ env.REGISTRY }}:latest"
YML
if ! bash "$GUARD" "$tmp/benign-env.yml" >/dev/null 2>&1; then
  echo "FAIL: guard false-positived on a benign env. interpolation"
  bash "$GUARD" "$tmp/benign-env.yml" || true
  exit 1
fi
echo "OK: benign env. interpolation not flagged"

# 5. false-positive guard: head.sha (40-hex, not injectable) must NOT trip.
cat >"$tmp/benign-sha.yml" <<'YML'
on: [pull_request]
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "${{ github.event.pull_request.head.sha }}" > sha.txt
YML
if ! bash "$GUARD" "$tmp/benign-sha.yml" >/dev/null 2>&1; then
  echo "FAIL: guard false-positived on github.event.pull_request.head.sha"
  bash "$GUARD" "$tmp/benign-sha.yml" || true
  exit 1
fi
echo "OK: head.sha interpolation not flagged"

# 6. escape hatch: a marked, provably-safe interpolation.
cat >"$tmp/allowed.yml" <<'YML'
on: [pull_request]
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      # injection-lint-allow: this workflow only triggers on trusted-branch push, head_ref is our own.
      - run: echo "${{ github.head_ref }}"
YML
if ! bash "$GUARD" "$tmp/allowed.yml" >/dev/null 2>&1; then
  echo "FAIL: guard ignored the injection-lint-allow escape hatch"
  bash "$GUARD" "$tmp/allowed.yml" || true
  exit 1
fi
echo "OK: injection-lint-allow escape hatch respected"

# 7. the real .github/workflows tree must be clean.
if ! bash "$GUARD" >/dev/null 2>&1; then
  echo "FAIL: the real .github/workflows tree contains an injection sink"
  bash "$GUARD" || true
  exit 1
fi
echo "OK: real .github/workflows tree is clean"

echo "PASS: check-workflow-injection self-test"
