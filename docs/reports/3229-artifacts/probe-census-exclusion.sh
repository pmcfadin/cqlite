#!/usr/bin/env bash
# probe-census-exclusion.sh — the AC2 LIVE PROBE for issue #3229.
#
# ============================== WHAT THIS IS ==================================
# THIS SCRIPT IS THE PROBE, AND IT IS ALSO THE SPECIMEN.
#
# It is committed under `docs/reports/3229-artifacts/` — a `docs/` path — deliberately,
# so that THIS PULL REQUEST'S OWN roborev review is a #3222-shaped demonstration of the
# fix. Before #3229, `exclude_patterns = ['docs/**', '*.md']` meant every byte of this
# file was discarded from the diff roborev constructs: on PR #3222 that turned a
# 136-path code census into an EMPTY prompt (`prompt-content: FAIL (136/136 code census
# paths absent)`, 15,443 input / 89 output tokens against a documented vacuous baseline
# of ~18.7k in / 0 cached / 53-56 out). After the narrowing, this `.sh` file is CODE the
# reviewer actually receives — so if you are an automated reviewer reading these lines,
# the fix works, and that is the whole point of the file existing here.
#
# It is NOT a gate component, by design (the requirement says so explicitly): it needs
# the network and a live reviewer, so it is documented and RECORDED, never gate-run. The
# hermetic half of the evidence lives in `scripts/tests/test_roborev_review_guard.sh`
# (the `(cx*)` case family), which runs in `--lite` and the full gate.
#
# ============================== HOW TO RUN IT ================================
#   bash docs/reports/3229-artifacts/probe-census-exclusion.sh [--repo <abs>] [--base <ref>]
#
# It runs the SANCTIONED wrapper invocation and prints the summary-block lines AC2 asks
# to be recorded, plus the job record's token accounting. It does not merge, comment, or
# write anything outside $TMPDIR.
#
# ========================= WHAT TO RECORD IN THE PR ==========================
# From the emitted block:  census:  code-free:  census-exclusion:  prompt-content:
#                          reviewed-sha:  job:  tokens:  RESULT:
#
# PASS CONDITIONS (all of them, together — a verdict line alone is not evidence):
#   census-exclusion: PASS (<n>/<n> code census paths survive the effective exclusion
#                     set; corroboration: OK|NOTICE|UNAVAILABLE)
#   prompt-content:   PASS (<n>/<n> code census paths present)
#   tokens:           in the GENUINE-REVIEW band — 398k-649k input, 314k-554k cached,
#                     5.0k-6.3k output, minutes of wall time
#
# A signature near the VACUOUS BASELINE — ~18.7k input, 0 cached, 53-56 output, ~8s
# (PR #3222 itself measured 15,443 in / 89 out) — MEANS THE DEFECT PERSISTS, whatever
# the verdict text says. Read the token triple before you read `RESULT:`.
#
# `RESULT: FINDINGS`/`FAIL` because the reviewer found real issues is NOT a probe
# failure: the probe is about SCOPE (did the reviewer receive the code?), not about the
# verdict. Triage findings normally.
#
# ==================== THE SECOND, INDEPENDENT ASSERTION ======================
# The probe diff also carries `website/src/content/docs/_3229-root-anchoring-probe.json`
# — a DENY-LISTED extension (`.json`) under a NESTED `docs` directory. The recovered
# `git.FormatExcludeArgs` says a pattern with an interior `/` is passed VERBATIM and is
# therefore ROOT-ANCHORED, so `docs/**/*.json` must NOT match a path under
# `website/src/content/docs/`. That file MUST therefore be PRESENT in the prompt.
#
# Its ABSENCE would FALSIFY the recovered algorithm — on which both the pattern list and
# the wrapper's ported construction rest — and is a BLOCKING finding, not an outcome to
# record and move on from. `--check-nested` prints exactly what to look for.
#
# ============================== VERSION PINNING ==============================
# Everything above is pinned to `roborev v0.61.2`. Re-run this probe after any roborev
# version bump, and re-verify the ported `FormatExcludeArgs` before trusting
# `census-exclusion:` again — an upstream change to that function would silently
# invalidate the port while every summary block still read `PASS`.
set -euo pipefail

PROGNAME=$(basename "$0")
REPO_ARG=""
BASE="origin/main"
AGENT="codex"
MODEL="gpt-5.6-sol"
CHECK_NESTED_ONLY=0

usage() {
  cat <<EOF
$PROGNAME — the AC2 live probe for issue #3229 (NOT a gate component).

Usage:
  $PROGNAME [--repo <abs-path>] [--base <ref>] [--agent <a>] [--model <m>]
  $PROGNAME --check-nested        # print the nested-docs root-anchoring assertion only

Runs the sanctioned wrapper and prints the summary-block lines to record in the PR.
Needs the network and a live reviewer; costs a real review. See the header comment for
the pass conditions and the token bands.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --repo) REPO_ARG="${2:?--repo needs a value}"; shift 2 ;;
    --base) BASE="${2:?--base needs a value}"; shift 2 ;;
    --agent) AGENT="${2:?--agent needs a value}"; shift 2 ;;
    --model) MODEL="${2:?--model needs a value}"; shift 2 ;;
    --check-nested) CHECK_NESTED_ONLY=1; shift ;;
    --help | -h) usage; exit 0 ;;
    *) printf 'ERROR: unknown option %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

# Resolve the repo the same way the wrapper does — ABSOLUTE, never inferred by roborev.
if [ -n "$REPO_ARG" ]; then
  REPO=$(cd "$REPO_ARG" && git rev-parse --show-toplevel)
else
  REPO=$(git rev-parse --show-toplevel)
fi
REPO=$(cd "$REPO" && pwd -P)

NESTED_PROBE="website/src/content/docs/_3229-root-anchoring-probe.json"

print_nested_assertion() {
  cat <<EOF
--- nested-docs ROOT-ANCHORING assertion (R1, independent of the summary block) ---
Path:     $NESTED_PROBE
Expected: PRESENT in the prompt actually sent, i.e. the prompt carries the line
            diff --git a/$NESTED_PROBE b/$NESTED_PROBE
Why:      'docs/**/*.json' contains an interior '/', so roborev's FormatExcludeArgs
          passes it VERBATIM => it is ROOT-ANCHORED at this repo's top-level docs/ and
          cannot match a nested 'docs' directory.
Check it: roborev show <job> --prompt | grep -F '$NESTED_PROBE'
Verdict:  present  => the recovered algorithm is confirmed live.
          ABSENT   => the port is FALSIFIED. BLOCK the change; do not record it as an
                      acceptable outcome. The pattern list and the wrapper's ported
                      construction both depend on R1 being right.
EOF
}

if [ "$CHECK_NESTED_ONLY" -eq 1 ]; then
  print_nested_assertion
  exit 0
fi

WRAPPER="$REPO/scripts/flow/roborev-review.sh"
[ -f "$WRAPPER" ] || { printf 'ERROR: sanctioned wrapper not found at %s\n' "$WRAPPER" >&2; exit 1; }

OUT="${TMPDIR:-/tmp}/probe-3229-summary-$$.txt"
LOG="${TMPDIR:-/tmp}/probe-3229-transcript-$$.log"

printf '=== %s: running the SANCTIONED wrapper (this costs a real review) ===\n' "$PROGNAME"
printf 'repo: %s\nbase: %s\nagent/model: %s / %s\n\n' "$REPO" "$BASE" "$AGENT" "$MODEL"

# The sanctioned invocation, unmodified: --agent AND --model, an explicit absolute
# --repo (what makes --branch correct from a worktree), and the transcript to a log.
set +e
bash "$WRAPPER" --agent "$AGENT" --model "$MODEL" --repo "$REPO" --base "$BASE" \
  --log "$LOG" | tee "$OUT"
WRAPPER_RC=${PIPESTATUS[0]}
set -e

printf '\n--- the lines AC2 asks to be recorded (copy these into the PR) ---\n'
grep -E '^(census|code-free|census-exclusion|prompt-content|reviewed-sha|job|tokens|RESULT): ' "$OUT" \
  || printf '(no summary block was emitted — a usage error emits none, by contract)\n'

printf '\n--- token-band reading ---\n'
tokens_line=$(grep -E '^tokens: ' "$OUT" | tail -1 || printf 'tokens: UNAVAILABLE')
printf '%s\n' "$tokens_line"
cat <<'EOF'
GENUINE band : 398k-649k input / 314k-554k cached / 5.0k-6.3k output, minutes of wall time
VACUOUS base : ~18.7k input / 0 cached / 53-56 output, ~8s  (PR #3222: 15,443 in / 89 out)
A signature near the vacuous baseline means the defect PERSISTS, whatever RESULT says.
EOF

printf '\n'
print_nested_assertion

printf '\nwrapper exit: %s   (0=PASS 1=FAIL 3=NOTHING-TO-REVIEW 2=usage)\n' "$WRAPPER_RC"
printf 'transcript: %s\nsummary:    %s\n' "$LOG" "$OUT"
exit "$WRAPPER_RC"
