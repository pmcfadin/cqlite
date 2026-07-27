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

# 5. non-numeric base (rust:latest-bookworm): the pin extraction must yield an
# EMPTY pin and fire the parse error, not a silent whole-line passthrough (L4).
cat >"$tmp/Dockerfile.latest" <<'DOCKER'
FROM rust:latest-bookworm AS chef
FROM chef AS builder
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.latest" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a non-numeric base (rust:latest-bookworm)"
  exit 1
fi
# Capture output separately (a piped grep would be masked by pipefail from the
# guard's intended non-zero exit): assert the failure is the PARSE error, proving
# the extraction yielded an empty pin rather than a whole-line passthrough (L4).
latest_out="$(bash "$GUARD" "$tmp/Dockerfile.latest" "$tmp/rust-toolchain.toml" 2>&1 || true)"
if ! printf '%s' "$latest_out" | grep -q "could not parse a numeric version"; then
  echo "FAIL: rust:latest did not produce the intended parse error (dead-branch regression)"
  printf '%s\n' "$latest_out"
  exit 1
fi
echo "OK: non-numeric base yields the intended parse error"

# 6. cook/build flag equality (#2870 D3): a matching pair PASSes.
cat >"$tmp/Dockerfile.flags-ok" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
FROM chef AS builder
RUN cargo chef cook --release --locked -p cqlite-flight --features observability --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p cqlite-flight --features observability
FROM debian:bookworm-slim
DOCKER
if ! bash "$GUARD" "$tmp/Dockerfile.flags-ok" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard flagged a matching cook/build flag pair"
  bash "$GUARD" "$tmp/Dockerfile.flags-ok" "$tmp/rust-toolchain.toml" || true
  exit 1
fi
echo "OK: matching cook/build flags PASS"

# 7. cook/build --features drift is caught (the silent-no-op class).
cat >"$tmp/Dockerfile.feat-drift" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
FROM chef AS builder
RUN cargo chef cook --release --locked -p cqlite-flight --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p cqlite-flight --features observability
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.feat-drift" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a cook/build --features mismatch"
  exit 1
fi
echo "OK: cook/build --features drift is caught"

# 8. cook/build --locked drift is caught (cook missing --locked).
cat >"$tmp/Dockerfile.locked-drift" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
FROM chef AS builder
RUN cargo chef cook --release -p cqlite-flight --features observability --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p cqlite-flight --features observability
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.locked-drift" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a cook/build --locked mismatch"
  exit 1
fi
echo "OK: cook/build --locked drift is caught"

# 9. cook/build package (-p) drift is caught.
cat >"$tmp/Dockerfile.pkg-drift" <<'DOCKER'
FROM rust:1.97.1-bookworm AS chef
FROM chef AS builder
RUN cargo chef cook --release --locked -p cqlite-core --features observability --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p cqlite-flight --features observability
FROM debian:bookworm-slim
DOCKER
if bash "$GUARD" "$tmp/Dockerfile.pkg-drift" "$tmp/rust-toolchain.toml" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a cook/build -p package mismatch"
  exit 1
fi
echo "OK: cook/build -p package drift is caught"

# 10. the REAL Dockerfile + toolchain must be in lockstep AND flag-equal.
if ! bash "$GUARD" >/dev/null 2>&1; then
  echo "FAIL: the real cqlite-flight/Dockerfile is out of lockstep with rust-toolchain.toml"
  bash "$GUARD" || true
  exit 1
fi
echo "OK: real cqlite-flight/Dockerfile is in lockstep and cook/build flags agree"

echo "PASS: check-dockerfile-rust-pin self-test"
