#!/usr/bin/env bash
# test_check_dockerfile_rust_pin.sh — self-test for the Dockerfile↔toolchain
# Rust-pin lockstep guard (issue #2870).
#
# Proves check-dockerfile-rust-pin.sh:
#   1. PASSes on a clean fixture (single FROM rust: matching the channel),
#   2. FAILs on a version mismatch (FROM rust: pin != channel),
#   3. FAILs when a SECOND literal `FROM rust:` line is present,
#   4. FAILs when NO `FROM rust:` line is present,
#   5. and PASSes on the REAL cqlite-flight/Dockerfile + rust-toolchain.toml.
# Hermetic: writes fixtures to a temp dir; no cargo/network/datasets.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD="$REPO_ROOT/scripts/ci/check-dockerfile-rust-pin.sh"

if [ ! -f "$GUARD" ]; then
  echo "FAIL: guard script not found at $GUARD"
  exit 1
fi

# Per-run temp dir with a TERMINAL-XXXXXX template (macOS mktemp substitutes only
# a trailing run of X's). Cleaned up on exit.
tmp="$(mktemp -d "${TMPDIR:-/tmp}/dockerfile-pin-test-XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

cat >"$tmp/rust-toolchain.toml" <<'TOML'
[toolchain]
channel = "1.97.1"
components = ["clippy", "rustfmt"]
TOML

# 1. clean fixture: single FROM rust: line matching the channel.
cat >"$tmp/Dockerfile.clean" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
WORKDIR /src
FROM chef AS planner
COPY . .
FROM chef AS builder
RUN cargo build --release
FROM debian:bookworm-slim
DOCKER
if ! bash "$GUARD" "$tmp/Dockerfile.clean" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard flagged a clean, in-lockstep Dockerfile"
  bash "$GUARD" "$tmp/Dockerfile.clean" "$tmp/rust-toolchain.toml" || true
  exit 1
fi
echo "OK: clean single-pin fixture PASSes"

# 2. version mismatch: FROM rust:1.96.0 while channel is 1.97.1.
cat >"$tmp/Dockerfile.mismatch" <<'DOCKER'
FROM rust:1.96.0-bookworm AS chef
FROM chef AS builder
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.mismatch" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a pin/channel version mismatch"
  exit 1
fi
echo "OK: version mismatch is caught"

# 3. a SECOND literal FROM rust: line (stages could diverge).
cat >"$tmp/Dockerfile.two" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
FROM rust:1.97.1-bookworm AS builder
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.two" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on two 'FROM rust:' lines"
  exit 1
fi
echo "OK: a second 'FROM rust:' line is caught"

# 4. NO FROM rust: line at all.
cat >"$tmp/Dockerfile.none" <<'DOCKER'
FROM debian:bookworm-slim
RUN echo hi
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.none" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip when no 'FROM rust:' line is present"
  exit 1
fi
echo "OK: missing 'FROM rust:' line is caught"

# 5. the REAL Dockerfile + toolchain must be in lockstep.
if ! bash "$GUARD" >/dev/null 2>&1; then
  echo "FAIL: the real cqlite-flight/Dockerfile is out of lockstep with rust-toolchain.toml"
  bash "$GUARD" || true
  exit 1
fi
echo "OK: real cqlite-flight/Dockerfile is in lockstep"

echo "PASS: check-dockerfile-rust-pin self-test"
