#!/usr/bin/env bash
# Shared fixture helper for scripts/tests/test_agent_gate_*.sh (issue #3544, roborev job 225).
#
# WHY THIS EXISTS. The gate's component-set pre-flight validates that `origin` names the
# CANONICAL UPSTREAM (`github.com/pmcfadin/cqlite`) before fetching a baseline, because
# `origin` merely EXISTING made `git remote set-url origin <anything>` a git-config-shaped
# opt-out — and because the pre-flight EXTRACTS AND RUNS the baseline's copy of the gate, so a
# loose identity admits arbitrary code, not just a wrong baseline. Every hermetic self-test
# fixture, though, uses a LOCAL bare `origin` (a path remote keeps the fetch real without a
# network), and a local path is deliberately NOT canonical.
#
# THE SANCTIONED RESOLUTION IS TO SUBSTITUTE THE ARTIFACT, NEVER TO ADD A SEAM (CLAUDE.md:
# "a case needing a different enforcer SUBSTITUTES the artifact in its own scratch copy of the
# tree — never a path variable", because a test-only seam is one more thing a real invoker can
# set). Each fixture already holds its OWN COPY of agent-gate.sh; this rewrites that copy's
# single hard-coded canonical-identity literal so the COPY treats the fixture's own local
# origin as its upstream. The SHIPPED script keeps exactly one hard-coded identity with no
# runtime override — an earlier design that made the check weak enough to accept a local path
# was the same fact as the vulnerability, which is what job 225 found.
#
# Sourced, not executed. Run directly it defines the function and exits 0 (harmless: it is
# matched by `--delta`'s scripts/tests/*.sh glob but is not a `test_*.sh` suite).

# agent_gate_pin_canonical_remote <gate-copy> <origin-url>
#   Rewrite <gate-copy>'s canonical-identity literal so <origin-url> is canonical FOR THAT
#   COPY. rc 0 on success; rc 1 with a named reason on stderr otherwise.
#
# FAIL-CLOSED, and the verification step is the load-bearing half: a fixture whose pin
# silently did not take would run against the SHIPPED identity, be rejected as
# `remote-not-canonical`, and stop at the pre-flight — every case in that suite then failing
# (or worse, passing) for a reason that has nothing to do with what it tests. So the pin is
# confirmed by asking the rewritten copy for its own verdict on that URL.
#
# The expected value is computed by the COPY'S OWN report-only `--component-set-remote-identity`
# hook rather than re-implementing the normalisation here: a second implementation of a fold
# is a second thing to drift (the #3283 lesson), and this is fixture CONSTRUCTION, not an
# oracle — nothing about the pre-flight's correctness is inferred from it, and the dedicated
# identity cases run against the UNPINNED shipped script.
agent_gate_pin_canonical_remote() {
  local copy="$1" url="$2" norm verdict tmpf
  if [ ! -f "$copy" ]; then
    echo "pin-canonical: no gate copy at '$copy'" >&2; return 1
  fi
  norm=$(bash "$copy" --component-set-remote-identity "$url" 2>/dev/null | sed -n 's/^NORMALISED: //p')
  if [ -z "$norm" ]; then
    echo "pin-canonical: '$copy --component-set-remote-identity' printed no NORMALISED line (hook missing or renamed)" >&2
    return 1
  fi
  tmpf="$copy.pin.$$"
  # Portable FIRST-MATCH rewrite with an EXACT shape match (`sed '0,/re/'` is a GNU extension
  # BSD/macOS sed rejects). `exit 3` when the literal is absent, so a renamed constant is a
  # loud failure rather than an unchanged copy.
  if ! awk -v v="$norm" '
        BEGIN { done = 0 }
        { if (!done && $0 ~ /^_CS_CANONICAL_REMOTE="[^"]*"$/) {
            print "_CS_CANONICAL_REMOTE=\"" v "\""; done = 1; next }
          print }
        END { if (!done) exit 3 }' "$copy" >"$tmpf"; then
    rm -f "$tmpf"
    echo "pin-canonical: no '_CS_CANONICAL_REMOTE=\"…\"' literal in '$copy' — the constant was renamed" >&2
    return 1
  fi
  mv "$tmpf" "$copy" || { rm -f "$tmpf"; echo "pin-canonical: could not replace '$copy'" >&2; return 1; }
  verdict=$(bash "$copy" --component-set-remote-identity "$url" 2>/dev/null | sed -n 's/^IDENTITY: //p')
  if [ "$verdict" != canonical ]; then
    echo "pin-canonical: '$copy' still answers IDENTITY '$verdict' for '$url' — the pin did not take" >&2
    return 1
  fi
  return 0
}

# agent_gate_install_components_manifest <gate-copy-path> (#3544 REQ-3544-01)
#   Write `<dir of the copy>/agent-gate.components` from THAT COPY'S OWN `--list`.
#
# WHY EVERY FIXTURE THAT COPIES THE GATE NEEDS THIS. The component-set pre-flight reads its
# baseline as DATA (`git show <rev>:scripts/agent-gate.components`) and, before it fetches
# anything, asserts that the WORKING TREE's manifest equals the running `COMPONENTS` array —
# fail-closed as `manifest-missing`/`manifest-stale` in the certifying modes. A fixture that
# copies the gate and no manifest therefore stops at that pre-flight instead of exercising
# whatever it exists to test. This is the same shape as the canonical-identity pin above, and
# it lives beside it so the three suites that copy the gate (component-set, delta,
# tree-integrity) share ONE implementation rather than three that can drift.
#
# DERIVED FROM THE COPY, never from the repository's committed manifest: a fixture whose copy
# was transformed (a sed'd COMPONENTS array) needs a manifest that matches THAT array, and
# copying the real file would make every such fixture `manifest-stale`.
#
# FAIL-CLOSED (rc 1) on an empty or implausibly short list: a manifest that silently failed to
# generate would make every case built on that fixture report `manifest-*`, i.e. a suite that
# tests nothing while looking busy.
agent_gate_install_components_manifest() {
  local copy="${1:-}"
  local dir out n
  [ -n "$copy" ] && [ -f "$copy" ] \
    || { echo "install-components-manifest: needs an existing gate copy (got '${1:-}')" >&2; return 1; }
  dir=$(dirname "$copy")
  out="$dir/agent-gate.components"
  if ! bash "$copy" --list >"$out" 2>/dev/null; then
    echo "install-components-manifest: '$copy --list' failed; cannot derive $out" >&2
    return 1
  fi
  n=$(grep -c . "$out" 2>/dev/null || true)
  case "${n:-0}" in ''|*[!0-9]*) n=0 ;; esac
  if [ "$n" -le 10 ]; then
    echo "install-components-manifest: '$copy --list' produced $n component(s); refusing a manifest that cannot be the component set" >&2
    return 1
  fi
  return 0
}
