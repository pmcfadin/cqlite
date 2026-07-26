#!/usr/bin/env bash
# check-dockerfile-rust-pin.sh — mechanize the #1990 lockstep between
# cqlite-flight/Dockerfile's Rust base pin and rust-toolchain.toml (issue #2870).
#
# Since the cargo-chef split (#2870) the Dockerfile has a SINGLE
# `FROM rust:<pin>-bookworm AS chef` line; planner and builder both `FROM chef`
# and carry no `rust:` base of their own. This guard asserts:
#   1. there is EXACTLY ONE `FROM rust:` line in the Dockerfile (a second literal
#      base would let the stages diverge on a toolchain bump), and
#   2. that line's version matches rust-toolchain.toml's `channel` exactly.
#
# Usage: check-dockerfile-rust-pin.sh [DOCKERFILE] [TOOLCHAIN_TOML]
#   Defaults: cqlite-flight/Dockerfile, rust-toolchain.toml (repo root).
# Exit 0 = in lockstep; non-zero (with a named reason) = drift.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dockerfile="${1:-$REPO_ROOT/cqlite-flight/Dockerfile}"
toolchain="${2:-$REPO_ROOT/rust-toolchain.toml}"

if [ ! -f "$dockerfile" ]; then
  echo "::error::check-dockerfile-rust-pin: Dockerfile not found at $dockerfile"
  exit 1
fi
if [ ! -f "$toolchain" ]; then
  echo "::error::check-dockerfile-rust-pin: toolchain file not found at $toolchain"
  exit 1
fi

# rust-toolchain.toml: channel = "1.97.1"
channel="$(grep -E '^[[:space:]]*channel[[:space:]]*=' "$toolchain" \
  | head -n1 | sed -E 's/.*"([^"]+)".*/\1/')"
if [ -z "$channel" ]; then
  echo "::error::check-dockerfile-rust-pin: could not read channel from $toolchain"
  exit 1
fi

# All `FROM rust:...` base lines (a leading `FROM chef` stage is NOT a rust: base).
mapfile -t from_lines < <(grep -nE '^[[:space:]]*FROM[[:space:]]+rust:' "$dockerfile" || true)
count="${#from_lines[@]}"

if [ "$count" -ne 1 ]; then
  echo "::error::check-dockerfile-rust-pin: expected exactly ONE 'FROM rust:' line in $dockerfile, found $count."
  echo "         A second literal rust: base lets the chef/planner/builder stages diverge on a toolchain bump (#1990)."
  printf '         %s\n' "${from_lines[@]}"
  exit 1
fi

# FROM rust:1.97.1-bookworm AS chef  ->  1.97.1
pin="$(printf '%s\n' "${from_lines[0]}" | sed -E 's/.*rust:([0-9]+(\.[0-9]+)*)-.*/\1/')"
if [ -z "$pin" ]; then
  echo "::error::check-dockerfile-rust-pin: could not parse the version from: ${from_lines[0]}"
  exit 1
fi

if [ "$pin" != "$channel" ]; then
  echo "::error::check-dockerfile-rust-pin: Rust base pin drift (#1990/#2870)."
  echo "         $dockerfile pins rust:$pin but rust-toolchain.toml channel = $channel."
  echo "         Bump the 'FROM rust:<pin>-bookworm AS chef' line to rust:$channel-bookworm (they move in lockstep)."
  exit 1
fi

echo "OK: cqlite-flight/Dockerfile Rust base (rust:$pin) is in lockstep with rust-toolchain.toml channel ($channel), single FROM rust: line."
