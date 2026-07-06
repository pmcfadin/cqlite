#!/usr/bin/env bash
# Fixture for issue #2081: proves the --delta shell-selftest executor actually RAN
# this script. When DELTA_SHELL_PROBE_SENTINEL is set, touch it (the caller checks the
# file exists); when DELTA_SHELL_PROBE_FAIL=1, exit non-zero to exercise the FAIL path.
# A bare run (no env) is a harmless no-op that exits 0.
set -uo pipefail
[ -n "${DELTA_SHELL_PROBE_SENTINEL:-}" ] && : >"$DELTA_SHELL_PROBE_SENTINEL"
[ "${DELTA_SHELL_PROBE_FAIL:-0}" = 1 ] && exit 7
exit 0
