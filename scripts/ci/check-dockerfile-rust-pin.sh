#!/usr/bin/env bash
# check-dockerfile-rust-pin.sh — mechanize two #2870 build invariants for
# cqlite-flight/Dockerfile:
#
#   A. The #1990 lockstep between the Rust base pin and rust-toolchain.toml.
#      Since the cargo-chef split (#2870) the Dockerfile has a SINGLE
#      `FROM rust:<pin>-bookworm AS chef` line; planner and builder both
#      `FROM chef` and carry no `rust:` base of their own. Asserts:
#        1. EXACTLY ONE `FROM rust:` line (a second literal base lets the stages
#           diverge on a toolchain bump), and
#        2. that line's version matches rust-toolchain.toml's `channel` exactly.
#
#   B. The cook/build flag equality (#2870 D3). cargo reuses a cooked artifact
#      only when profile + package + feature resolution match, so the
#      `cargo chef cook` line and the final `cargo build` line MUST agree on
#      --release, --locked, -p/--package, and --features. A mismatch makes the
#      cache a silent no-op that recompiles behind a hit layer.
#
# Usage: check-dockerfile-rust-pin.sh [DOCKERFILE] [TOOLCHAIN_TOML]
#   Defaults: cqlite-flight/Dockerfile, rust-toolchain.toml (repo root).
# Exit 0 = both invariants hold; non-zero (with a named reason) = drift.
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

# ---- A. pin lockstep -------------------------------------------------------

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

# FROM rust:1.97.1-bookworm AS chef  ->  1.97.1 (genuine match: a non-numeric
# base such as rust:latest-bookworm captures nothing, so pin stays empty and the
# parse error below fires — never a silent whole-line passthrough).
pin=""
if [[ "${from_lines[0]}" =~ rust:([0-9]+(\.[0-9]+)*) ]]; then
  pin="${BASH_REMATCH[1]}"
fi
if [ -z "$pin" ]; then
  echo "::error::check-dockerfile-rust-pin: could not parse a numeric version from: ${from_lines[0]}"
  echo "         The base must be pinned to an explicit rust:<x.y.z>-bookworm matching rust-toolchain.toml."
  exit 1
fi

if [ "$pin" != "$channel" ]; then
  echo "::error::check-dockerfile-rust-pin: Rust base pin drift (#1990/#2870)."
  echo "         $dockerfile pins rust:$pin but rust-toolchain.toml channel = $channel."
  echo "         Bump the 'FROM rust:<pin>-bookworm AS chef' line to rust:$channel-bookworm (they move in lockstep)."
  exit 1
fi

# ---- B. cook/build flag equality (#2870 D3) --------------------------------

cook_line="$(grep -E '^[[:space:]]*RUN[[:space:]]+cargo[[:space:]]+chef[[:space:]]+cook' "$dockerfile" | head -n1 || true)"
build_line="$(grep -E '^[[:space:]]*RUN[[:space:]]+cargo[[:space:]]+build' "$dockerfile" | head -n1 || true)"

if [ -n "$cook_line" ]; then
  if [ -z "$build_line" ]; then
    echo "::error::check-dockerfile-rust-pin: a 'cargo chef cook' line exists but no 'cargo build' line — cannot verify cook/build flag equality (#2870 D3)."
    exit 1
  fi

  has_flag()  { grep -qE -- "(^|[[:space:]])$2([[:space:]]|\$)" <<<"$1" && echo 1 || echo 0; }
  val_after() { sed -nE "s/.*(^|[[:space:]])$2[[:space:]]+([^[:space:]]+).*/\\2/p" <<<"$1" | head -n1; }

  for spec in "--release" "--locked"; do
    if [ "$(has_flag "$cook_line" "$spec")" != "$(has_flag "$build_line" "$spec")" ]; then
      echo "::error::check-dockerfile-rust-pin: cook/build flag mismatch (#2870 D3): '$spec' present on one line but not the other."
      echo "         cook:  $cook_line"
      echo "         build: $build_line"
      exit 1
    fi
  done

  cook_pkg="$(val_after "$cook_line" '-p')"; [ -z "$cook_pkg" ] && cook_pkg="$(val_after "$cook_line" '--package')"
  build_pkg="$(val_after "$build_line" '-p')"; [ -z "$build_pkg" ] && build_pkg="$(val_after "$build_line" '--package')"
  cook_feat="$(val_after "$cook_line" '--features')"
  build_feat="$(val_after "$build_line" '--features')"

  if [ "$cook_pkg" != "$build_pkg" ]; then
    echo "::error::check-dockerfile-rust-pin: cook/build package mismatch (#2870 D3): cook '-p $cook_pkg' vs build '-p $build_pkg'."
    exit 1
  fi
  if [ "$cook_feat" != "$build_feat" ]; then
    echo "::error::check-dockerfile-rust-pin: cook/build feature mismatch (#2870 D3): cook '--features $cook_feat' vs build '--features $build_feat'."
    exit 1
  fi
  echo "OK: cook/build flags agree (-p $cook_pkg, --features $cook_feat, --release/--locked matched)."
fi

echo "OK: cqlite-flight/Dockerfile Rust base (rust:$pin) is in lockstep with rust-toolchain.toml channel ($channel), single FROM rust: line."
