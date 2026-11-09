#!/usr/bin/env bash
# sccache-stable-tmpdir-wrapper.sh - RUSTC_WRAPPER for the tooling-tests pool (#4479).
#
# Each pool suite runs under its own private TMPDIR, deleted when the suite ends. sccache's server
# keeps the TMPDIR it was SPAWNED with, so a server auto-started from inside a suite would hold a dead
# temp dir and fail every compile on the box. This wrapper restores the gate's STABLE TMPDIR for the
# sccache process only (the suite keeps its private TMPDIR for everything else) and execs the real
# wrapper, so a server it spawns starts under the stable dir.
#
#   CQLITE_TT_STABLE_TMPDIR        the stable temp dir (required)
#   CQLITE_TT_REAL_RUSTC_WRAPPER   the real sccache to exec (required)
#
# Checked in and executable, never generated into TMPDIR: a noexec temp filesystem would stop cargo
# from running a generated wrapper. It must stay BASH, not sh: dash drops environment variables whose
# names are not shell identifiers, and cargo passes CARGO_BIN_EXE_<bin-with-hyphen> that way, so an
# sh wrapper made every env!("CARGO_BIN_EXE_...") test fail to compile (measured on ws0-corpus-gen).
TMPDIR=${CQLITE_TT_STABLE_TMPDIR:?sccache-stable-tmpdir-wrapper: CQLITE_TT_STABLE_TMPDIR is not set}
export TMPDIR
exec "${CQLITE_TT_REAL_RUSTC_WRAPPER:?sccache-stable-tmpdir-wrapper: CQLITE_TT_REAL_RUSTC_WRAPPER is not set}" "$@"
