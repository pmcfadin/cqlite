#!/usr/bin/env bash
# check-dep-duplicates.sh — the ADVISORY DUPLICATE-DEPENDENCY RATCHET (issue #1700,
# epic #1688 dependency hygiene AH7).
#
# # What this exists for
#
# The AH7 audit counted 88 duplicate dependency instances across 37 distinct crates in
# this workspace. Some of that is unavoidable — the arrow/parquet graph pins
# `hashbrown` five ways and no amount of local nudging changes it — but some of it was
# ours: an unused `wiremock = "0.5"` dev-dependency carrying an entire pre-0.8 lineage,
# and a `base64 = "0.21"` requirement in three of our own crates while the whole rest of
# the graph had moved to 0.22. Both were removed on #1700, and nothing stopped the count
# creeping back up again the next time a dependency was added.
#
# So this guard answers exactly ONE question, on every full gate:
#
#   Has the number of DUPLICATED dependencies gone UP since the committed baseline?
#
# # It is ADVISORY — it may never fail the gate (issue #1700 AC2)
#
# An increase is a signal to a human, not a merge blocker: a legitimate new dependency
# can add a duplicate that no local decision can collapse, and a lane that reds on
# correct input is the lane agents learn to waive (CLAUDE.md). The gate component
# therefore records PASS on an increase too — and prints a LOUD, textually distinct
# `ADVISORY-INCREASE` line naming the delta and the crates responsible, so the increase
# is visible rather than silent.
#
# # …and it may never pass VACUOUSLY either (CLAUDE.md: affirmative measurement)
#
# "Advisory" is not a licence to green on nothing. Every state this script can reach is
# one of THREE, never two:
#
#   MEASURED + COMPARED     exit 0 — the counts were read from `cargo tree` output and
#                           compared against the committed baseline. The verdict is
#                           NO-INCREASE or ADVISORY-INCREASE, and BOTH are affirmative:
#                           a clean result prints `0 INCREASE RECOGNISED`, never a bare
#                           `0`, because a bare zero in a gate log reads as a verified
#                           all-clear from a scan that may not have happened.
#   UNMEASURABLE            exit 3 — no cargo on PATH, NO WAY TO BOUND THE PROBE (see
#                           below), `cargo tree` non-zero (an offline registry, a broken
#                           lockfile), a timeout, or output the parser does not recognise:
#                           nothing recognisable at all (`unparseable-output`), a
#                           column-zero line that ALMOST reads as a duplicate-group head
#                           (`malformed-record` — truncation, a diagnostic on stdout), a
#                           column-zero line matching no recognised shape at all
#                           (`unrecognised-line` — punctuation, JSON, another
#                           subcommand's output), a `[…]` header outside the exact pair
#                           cargo's tree printer emits (`unrecognised-section-header`), or
#                           a crate counted ONCE (`implausible-census` — a duplicate group
#                           has at least two members, the same rule the baseline reader
#                           enforces).
#                           The gate component maps this to SKIP NAMING THE CAUSE. It is
#                           NOT a pass.
#   BASELINE UNUSABLE       exit 4 — the committed baseline is missing or does not parse
#                           under the closed grammar below. Also a SKIP naming the cause:
#                           with no baseline there is no comparison, so there is no
#                           verdict to give.
#
# The distinction between "cargo tree printed NOTHING" (a workspace with genuinely zero
# duplicates — a legitimate measurement of 0) and "cargo tree printed something this
# parser could not read" (UNMEASURABLE) is made explicitly, because collapsing the two
# is how a broken parser reports a perfect score.
#
# # What is measured, pinned
#
#   cargo tree -d --workspace --target all
#
# THE `--workspace` IS LOAD-BEARING and is the audit's own invocation. The BARE
# `cargo tree -d` resolves the ROOT PACKAGE only (this workspace has a root package, so
# cargo's default member set is that one package — CLAUDE.md) and reports 14/6 where the
# workspace reports 88/37. A ratchet over the bare form would be blind to five sixths of
# the subject. `--all-features` is deliberately NOT used: it reports a different, larger
# graph (127/56 at the time of writing), and the baseline must pin ONE invocation.
#
# `--target all` IS LOAD-BEARING FOR A DIFFERENT REASON: THE BASELINE IS COMMITTED.
# `cargo tree` defaults to the HOST target, so without it the census is a function of the
# BOX — a Linux lane and a macOS lane measure different graphs from the same lockfile with
# no dependency change at all, and one of them then reports a phantom ADVISORY delta
# against a file the other committed. A committed baseline has to mean the same thing on
# every gate host, and macOS is a first-class one here (the `gtimeout` handling below is
# the same fact). `--target all` makes the subject the whole lockfile instead of whatever
# this box happens to be. It necessarily reports MORE than the host-only form (73/32 ->
# 114/46 on this workspace), which is the point: those rows were always in the lockfile,
# they were merely invisible from one platform.
#
# THE PROBE IS RUN READ-ONLY, with `--locked --offline` (see the constant below).
# `--locked` makes cargo REFUSE to update `Cargo.lock` rather than silently rewriting it,
# and `--offline` removes the registry access. That is not hygiene: `Cargo.lock` is
# TRACKED, and a component that rewrites a tracked file mid-run FAILS THE GATE OF RECORD
# on #2926's mid-run tree-mutation check — an ADVISORY component reddening the whole gate
# from a mutation it caused itself. A failure under either flag is UNMEASURABLE (SKIP,
# cause named); there is deliberately no unlocked/online retry.
#
# An INSTANCE is one column-zero `<name> v<version>` line of that output — i.e. one
# member of one duplicate group. A CRATE is one distinct name among them. (A crate can
# legitimately appear twice at the SAME version, e.g. `libc v0.2.189` in both the normal
# and the build-dependency graph; that is two instances, one crate, and the ratchet
# counts what cargo prints rather than second-guessing it.)
#
# # Colour-immune parsing (#3400)
#
# Cargo colours the STATUS WORD and colour SURVIVES redirection to a file, so a parse
# anchored on coloured text matches nothing — silently, in both directions. Every parse
# here runs over an ANSI-STRIPPED copy of the captured output, read BY REDIRECTION and
# never through a pipe (a piped `while read` runs in a subshell whose verdict is
# discarded). `CARGO_TERM_COLOR=never` is set as a belt; the strip is the control.
#
# # No invoker-selectable subject
#
# The workspace manifest and the baseline path are derived from THIS SCRIPT'S OWN
# LOCATION and cannot be selected by any flag or environment variable, and none may be
# added: a guard whose invoker chooses its subject can be pointed at a trivial subject
# and greened (CLAUDE.md, "the constrained party must not choose its own enforcer"). A
# test that needs a different subject SUBSTITUTES THE ARTIFACT — it copies this script
# into a scratch tree and puts a shim `cargo` on PATH — which is what
# scripts/tests/test_dep_duplicates_ratchet.sh does.
#
# # Stated boundary (do not overclaim)
#
# A clean result means: the duplicate-instance and duplicate-crate counts of
# `cargo tree -d --workspace --target all` are not GREATER than the committed
# baseline's. It says
# nothing about whether any duplication is justified, nothing about which versions are
# resolved, nothing about security advisories (that is `cargo deny`/`cargo audit`), and
# nothing about features. It is a RATCHET, not a target: the goal is "not worse", and a
# DECREASE is reported as an opportunity to re-tighten the baseline, never as a pass/fail
# event.
#
# Exit 0 = measured and compared (verdict NO-INCREASE or ADVISORY-INCREASE — both
# non-failing). 3 = UNMEASURABLE, cause named. 4 = baseline unusable, cause named.
# 2 = usage error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Hard-coded subject — see "No invoker-selectable subject" above.
readonly BASELINE_REL="scripts/ci/dep-duplicates-baseline.txt"
readonly MANIFEST_REL="Cargo.toml"
BASELINE="$REPO_ROOT/$BASELINE_REL"
MANIFEST="$REPO_ROOT/$MANIFEST_REL"

# The ONE spelling of the measured invocation. Named once so the script, its diagnostics
# and the baseline header cannot drift into three descriptions of two commands.
readonly PROBE_DESC='cargo tree -d --workspace --target all'
# THE PROBE IS READ-ONLY, AND THAT IS NOT HYGIENE — IT IS WHAT KEEPS THIS COMPONENT FROM
# FAILING THE GATE OF RECORD (roborev round 4, #1700).
#
# `cargo tree` without `--locked` will UPDATE `Cargo.lock` whenever it decides the
# manifests need it, and without `--offline` it may reach the registry to do so. Two
# consequences, and the second is severe:
#
#   1. The measured SUBJECT becomes MUTABLE — the thing being measured can be changed by
#      the act of measuring it — which contradicts this component's own read-only,
#      metadata-only contract (it is deliberately not in the gate's DATASET_COMPONENTS and
#      builds nothing).
#   2. IT CAN FAIL THE FULL GATE. `Cargo.lock` is a TRACKED file, and CLAUDE.md #2926: a
#      run whose worktree mutates MID-RUN cannot certify — every gate mode captures a tree
#      identity at start, re-verifies it at each component boundary, and FAILs closed with
#      `tree-integrity: FAIL (tree-mutated-midrun; …)`. So an ADVISORY component that may
#      never emit a FAIL could red the entire gate of record, from a mutation it caused
#      itself.
#
# `--locked` is the control: cargo REFUSES to update the lockfile rather than silently
# rewriting it. `--offline` removes the registry access. On a healthy checkout they cost
# NOTHING — the lockfile is committed and current, so a correct tree needs no update.
#
# AND IF EITHER FLAG MAKES THE PROBE FAIL — a genuinely stale lockfile, a cold registry
# cache — THAT IS `UNMEASURABLE` ⇒ SKIP NAMING THE CAUSE (cargo-tree-failed, quoting
# cargo's own first stderr line). There is deliberately NO retry without them: a fallback
# to an unlocked or online run would restore exactly the mutability these flags remove,
# and would do it silently — the permissive branch a missing capability must never inherit.
#
# They are named SEPARATELY from PROBE_DESC on purpose. PROBE_DESC is the measured
# SUBJECT (which graph is being censused), and the committed baseline's header quotes it;
# these two flags change only HOW the probe is allowed to run, never WHAT it reports, so
# folding them into the subject description would misdescribe the baseline's own subject.
readonly PROBE_READONLY_FLAGS='--locked --offline'
# THE BOUND ON THE PROBE, AND IT IS A REAL BOUND OR THERE IS NO PROBE.
#
# `timeout <n> cmd` sends SIGTERM ONLY. A cargo — or any child of it — that ignores,
# blocks or is stuck in an uninterruptible wait then keeps running and `timeout` keeps
# WAITING, so the claimed bound is not a bound at all and this component can hang a gate
# indefinitely on a registry lock or a credential prompt. `-k <grace>` is what makes it
# real: SIGKILL, which nothing can ignore, <grace> seconds after the SIGTERM.
readonly PROBE_TIMEOUT_SECS=600
readonly PROBE_KILL_GRACE_SECS=30

MODE=check

usage() {
  cat <<'EOF'
Usage: scripts/ci/check-dep-duplicates.sh [--regenerate] [--help]

The ADVISORY duplicate-dependency ratchet (issue #1700). Measures
`cargo tree -d --workspace --target all` and compares the duplicate-instance /
duplicate-crate counts against scripts/ci/dep-duplicates-baseline.txt. `--target all`
is what makes the COMMITTED baseline mean the same thing on every gate host: cargo
tree otherwise defaults to the HOST target. The probe is run READ-ONLY
(`--locked --offline`): it never updates Cargo.lock and never reaches the registry,
because rewriting a TRACKED file mid-gate fails the gate of record (#2926). A failure
under those flags is UNMEASURABLE, never a retry without them.

  (no flags)      Measure and compare. Exit 0 = compared (verdict NO-INCREASE or
                  ADVISORY-INCREASE — an increase is ADVISORY and never fails).
                  Exit 3 = UNMEASURABLE, cause named (no cargo, no timeout(1) to
                  bound the probe with, cargo tree failed, timed out, unparseable
                  output, a malformed column-zero record, or an impossible census
                  in which some crate appears once). Exit 4 = the committed baseline is
                  missing or does not parse. Exit 2 = usage error.
  --regenerate    Re-measure and rewrite the baseline from the current graph. THE one
                  documented regeneration command; run it after a change that
                  legitimately alters the duplicate set, and commit the result.
  --help          This message.

The baseline is a RATCHET, not a target: it records "no worse than this". A DECREASE is
reported as an invitation to re-run --regenerate, never as a failure.

The workspace and the baseline path are derived from this script's own location and
cannot be selected by a flag or an environment variable, deliberately: a guard whose
invoker picks its subject can be pointed at a trivial subject and greened vacuously.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --regenerate) MODE=regenerate ;;
    *)
      echo "check-dep-duplicates.sh: unrecognized argument '$1'" >&2
      echo "" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

# THE one documented regeneration command, named once so the diagnostics, the baseline
# header and --help cannot drift into three spellings of it.
readonly BASELINE_REGEN_CMD="scripts/ci/check-dep-duplicates.sh --regenerate"

# say <text>: every line this script prints is ANCHORED with the same prefix, so a
# consumer (the gate component, the self-test, a human grepping a gate log) can find the
# script's own statements among cargo's output and can never mistake one for the other.
say() { printf 'dep-duplicates: %s\n' "$*"; }

# unmeasurable <cause> <detail>: THE ONE exit for "no measurement was taken". Named
# cause, because "could not measure" and "measured fine" must never be one state, and a
# consumer has to be able to report WHY without parsing prose.
unmeasurable() {
  say "SKIP-UNMEASURABLE cause=$1 detail=$2"
  say "no comparison was made, and this is NOT a pass (issue #1700)."
  exit 3
}

# baseline_unusable <cause> <detail>: the second non-measuring exit, kept TEXTUALLY
# DISTINCT from the first because the operator action differs — one is "this host could
# not run cargo", the other is "the committed baseline in this checkout is broken".
baseline_unusable() {
  say "SKIP-BASELINE-UNUSABLE cause=$1 detail=$2"
  say "remedy: restore $BASELINE_REL, or regenerate it with"
  say "  bash $BASELINE_REGEN_CMD"
  exit 4
}

# require_base_count <value> <lineno> <what>: the closed grammar's COUNT rule, in ONE
# place. A value validated only as "all digits" is still not a NUMBER to the shell: a
# leading zero selects base 8, so `010` would compare and sum as 8 and `08` would abort
# the parse outright with "value too great for base". The generator never emits a leading
# zero, so a canonical baseline is unaffected — but the baseline is a COMPARISON KEY, and
# a key with two spellings for one value is a key that guesses. So a leading zero is
# REFUSED BY NAME rather than normalised with `10#`, which would silently accept both
# spellings of one count. A bare `0` stays legal: a workspace with no duplicates at all is
# a legitimate measurement the generator can publish.
require_base_count() {
  case "$1" in
    ''|*[!0-9]*) baseline_unusable baseline-garbage "line $2: '$1' is not $3" ;;
  esac
  case "$1" in
    0) return 0 ;;
    0*) baseline_unusable baseline-garbage "line $2: '$1' has a leading zero — the count grammar is canonical decimal, and to the shell a leading zero means base 8 ('010' would sum as 8, '08' aborts). Regenerate the baseline rather than hand-editing it." ;;
  esac
  return 0
}

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dep-duplicates.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; return 0; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1) MEASURE. `cargo tree -d --workspace --target all`, captured to a file,
#    ANSI-stripped, parsed by
#    redirection.
# ---------------------------------------------------------------------------
# `type -P`, NEVER `command -v` (agent-gate.sh's B6 rule): `command -v` finds shell
# FUNCTIONS, and the gate defines a function named `cargo` for its feature-matrix
# observer. A `command -v cargo` probe therefore answers "present" on a box with no cargo
# binary at all, turning an honest SKIP into a confusing failure.
CARGO_BIN="$(type -P cargo || true)"
[ -n "$CARGO_BIN" ] || unmeasurable cargo-absent "no cargo binary on PATH (type -P cargo found nothing)"
[ -f "$MANIFEST" ] || unmeasurable workspace-manifest-absent "$MANIFEST_REL is not a file — this is not a workspace root"

# RESOLVE THE BOUNDING TOOL, AND REQUIRE IT TO HAVE THE CAPABILITY WE DEPEND ON.
# `gtimeout` is the GNU coreutils spelling on macOS (a first-class gate host), where
# `timeout` may not exist at all. `-k` is probed AFFIRMATIVELY rather than assumed: a
# tool that rejects the flag would exit 125 and be reported as a cargo failure, which is
# a wrong diagnosis, and "the flag was not rejected" is the only evidence that the hard
# kill will actually happen.
TIMEOUT_BIN="$(type -P timeout || true)"
[ -n "$TIMEOUT_BIN" ] || TIMEOUT_BIN="$(type -P gtimeout || true)"
if [ -n "$TIMEOUT_BIN" ] && ! "$TIMEOUT_BIN" -k 1 1 true >/dev/null 2>&1; then
  TIMEOUT_BIN=""
fi
# FAIL-CLOSED WHERE THE PROBE CANNOT BE BOUNDED, and this is the deliberate choice at
# this branch: a missing capability must NOT inherit the permissive branch (CLAUDE.md —
# the component-set pre-flight refuses on exactly this ground). The permissive branch
# here would be an UNBOUNDED `cargo tree`, i.e. an ADVISORY component that can hang the
# whole gate with no verdict — strictly worse than one that says, by name, that it could
# not measure. So the probe is NOT RUN, and no `INVOKED` line is printed, because none
# happened.
[ -n "$TIMEOUT_BIN" ] || unmeasurable probe-unboundable \
  "no timeout(1) accepting -k on PATH (tried timeout, gtimeout), so the ${PROBE_TIMEOUT_SECS}s bound on '$PROBE_DESC' cannot be ENFORCED; the probe was NOT run rather than run unbounded"

TREE_RAW="$WORK_DIR/tree.txt"
TREE_ERR="$WORK_DIR/tree.err"
probe_rc=0
# The bound is STATED before it is applied, so a gate log shows what the probe was
# actually bounded by rather than leaving a reader to trust this comment.
say "probe bound: $TIMEOUT_BIN ${PROBE_TIMEOUT_SECS}s then SIGKILL after a further ${PROBE_KILL_GRACE_SECS}s"
# STATED, like the bound, and for the same reason: a gate log should show that the probe
# really was constrained to be read-only rather than leave a reader to trust a comment.
say "probe read-only flags: $PROBE_READONLY_FLAGS (cargo REFUSES to update Cargo.lock instead of rewriting it, and makes no network access; a failure under them is UNMEASURABLE, never an unlocked retry)"
# shellcheck disable=SC2086  # PROBE_READONLY_FLAGS is two fixed words, intentionally split.
CARGO_TERM_COLOR=never "$TIMEOUT_BIN" -k "$PROBE_KILL_GRACE_SECS" "$PROBE_TIMEOUT_SECS" \
  "$CARGO_BIN" tree -d --workspace --target all $PROBE_READONLY_FLAGS \
  --manifest-path "$MANIFEST" \
  >"$TREE_RAW" 2>"$TREE_ERR" || probe_rc=$?
# The EXPLICIT REACH SIGNAL the gate component reads (#3453): "cargo was invoked, and
# here is what it returned". Printed unconditionally, before any verdict, so a component
# that has to record whether its driver was REACHED can do so from a signal rather than
# from an assumption about the terminal status.
say "probe $PROBE_DESC INVOKED (rc $probe_rc)"
if [ "$probe_rc" -ne 0 ]; then
  detail="rc $probe_rc"
  # 124 = SIGTERM at the bound. 137 = 128+9, i.e. SIGKILL — the bound's HARD KILL after
  # a SIGTERM that was ignored, OR an external killer (the OOM killer signals the same
  # way). The two are not distinguishable from an exit status, so the detail names both
  # rather than asserting the one that reads better.
  [ "$probe_rc" -eq 124 ] && detail="timed out after ${PROBE_TIMEOUT_SECS}s (SIGTERM at the bound)"
  [ "$probe_rc" -eq 137 ] && detail="killed by SIGKILL (rc 137) — the ${PROBE_TIMEOUT_SECS}s bound's hard kill after an ignored SIGTERM, or an external killer such as the OOM killer"
  first_err=""
  if [ -r "$TREE_ERR" ]; then
    while IFS= read -r line; do
      case "$line" in ''|' '*) continue ;; esac
      first_err="$line"; break
    done <"$TREE_ERR"
  fi
  unmeasurable cargo-tree-failed "$PROBE_DESC exited non-zero ($detail)${first_err:+: $first_err}"
fi

# ANSI STRIP (#3400). Mirrors agent-gate.sh's _ansi_stripped_log, including its
# FAIL-CLOSED behaviour: a failed normalisation is NOT "parse the coloured original" —
# under colour the coloured original is exactly what the parser cannot read, so handing
# it back would convert a normalisation failure into a vacuous verdict.
TREE_TXT="$TREE_RAW.ansi-stripped"
esc=$(printf '\033')
if ! sed -E "s/${esc}\\[[0-9;]*[A-Za-z]//g" "$TREE_RAW" >"$TREE_TXT" 2>/dev/null; then
  unmeasurable ansi-strip-failed "could not normalise the captured $PROBE_DESC output for parsing"
fi
[ -r "$TREE_TXT" ] || unmeasurable ansi-strip-failed "the normalised copy of the $PROBE_DESC output is unreadable"

# Parse. A duplicate-group member is a COLUMN-ZERO `<name> v<version>` line; every other
# line of `cargo tree` output is an indented tree branch (`├──`, `│`, `└──`) or blank.
# Read BY REDIRECTION, never a pipe: a piped `while read` runs in a subshell and its
# accumulated counts would be discarded.
# THE PER-CRATE CENSUS IS A FILE, NOT AN ASSOCIATIVE ARRAY. `declare -A` is bash 4 and
# macOS ships bash 3.2 — a first-class gate host, and the gate's own portability lint
# (`portability-8c` in scripts/tests/test_agent_gate_summary.sh) FAILs on it. Two
# `<name> <count>` files plus a linear lookup is portable and, at ~30 crates, free.
NOW_CENSUS="$WORK_DIR/now.census"
: >"$NOW_CENSUS"

# census_get <file> <name>: print the recorded count for <name>, or nothing. rc 1 when
# absent, so a caller can tell "recorded zero" from "not recorded" — the same
# three-valued discipline the rest of this script keeps.
census_get() {
  local n c
  while read -r n c; do
    if [ "$n" = "$2" ]; then printf '%s' "$c"; return 0; fi
  done <"$1"
  return 1
}
# census_bump <file> <name>: increment <name>'s count, appending it at 1 if new.
census_bump() {
  local cur
  if cur=$(census_get "$1" "$2"); then
    local tmpc="$1.next"
    awk -v want="$2" '{ if ($1 == want) { print $1, $2 + 1 } else { print } }' "$1" >"$tmpc" \
      && mv "$tmpc" "$1"
  else
    printf '%s 1\n' "$2" >>"$1"
  fi
}

# THE MEASUREMENT PARSER IS AS STRICT AS THE BASELINE PARSER BELOW, and that symmetry is
# the point (roborev, #1700). The baseline reader refuses a `crate x 1` and refuses any
# line outside its closed grammar; this parser used to count what it RECOGNISED and
# silently ignore everything else, so partial or malformed output — a `cargo tree` killed
# mid-write, a diagnostic interleaved onto stdout, another subcommand's output — produced
# a NO-INCREASE verdict from an UNDER-COUNT. That is a VACUOUS PASS in the one component
# whose whole reason for existing is never to emit one. A parser strict about the file it
# reads and permissive about the command it runs is guessing on the half that matters.
#
# THE RULE IS "RECOGNISED, ELSE REFUSE" — not "strict about the lines that look like
# records" (roborev round 3, #1700). The first version of this parser decided
# CONTINUATION by a NEGATIVE test: "the first character is not a crate-name character".
# That is the permissive `!= <bad>` branch CLAUDE.md forbids, and it silently swallowed
# every column-zero line beginning with PUNCTUATION — a `{"reason":…}` cargo JSON
# diagnostic, a `*** truncated ***` marker, another subcommand's output — while the
# records around it still produced a NO-INCREASE verdict. A census parsed in part may not
# be published in full. So each line must MATCH one recognised shape; anything else is
# UNMEASURABLE and named:
#   RECORD        a column-zero `<name> v<version>` duplicate-group head. Counted.
#   CONTINUATION  an INDENTED line (every nested entry cargo prints), or a column-zero
#                 tree branch in either of cargo's symbol sets — utf8 `├ │ └ ─` (the
#                 default) or `--charset ascii` `|` and `` ` ``. Cargo chooses the set for
#                 the output device, so recognising only one would red on correct output.
#   HEADER        EXACTLY `[dev-dependencies]` or `[build-dependencies]`. That pair is
#                 what cargo's tree printer emits at column zero (both literals sit beside
#                 the tree code in the cargo binary; this workspace's own output carries
#                 the first). Any OTHER `[…]` line is `unrecognised-section-header`: it
#                 means the document is not the one this parser was written against, and
#                 assuming a header is harmless is how an under-count becomes a verdict.
#   anything else at column zero — `unrecognised-line`. NOT skipped: UNMEASURABLE, named,
#                 quoting the line.
now_instances=0
now_names=""
nonblank=0
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    '') continue ;;
  esac
  nonblank=$((nonblank + 1))
  # AFFIRMATIVE CLASSIFICATION. Every arm below RECOGNISES a shape; the final arm is a
  # refusal, never a skip. Ordering is by first character and the arms are disjoint, so
  # the rule is unambiguous.
  case "$line" in
    # RECORD candidate — column zero, crate-name first character. Validated below.
    [A-Za-z0-9_-]*) ;;
    # CONTINUATION — an indented entry (space or tab: cargo indents with spaces, the tab
    # is admitted because an indented line is a continuation whatever the whitespace).
    [[:blank:]]*) continue ;;
    # CONTINUATION — a column-zero tree branch. utf8 charset (cargo's default) then the
    # `--charset ascii` symbols. Both are cargo's own; neither can begin a crate name.
    '├'*|'│'*|'└'*|'─'*) continue ;;
    '|'*|'`'*) continue ;;
    # HEADER — an EXACT allowlist (whole line), not a `[`-prefix test.
    '[dev-dependencies]'|'[build-dependencies]') continue ;;
    '['*) unmeasurable unrecognised-section-header \
      "$PROBE_DESC printed a column-zero section header this parser does not recognise: '$line' (the recognised pair is [dev-dependencies] and [build-dependencies]); the output is not the document this parser was written against" ;;
    *) unmeasurable unrecognised-line \
      "$PROBE_DESC printed a column-zero line that is neither a '<name> v<version>' duplicate-group head nor a recognised tree branch or section header: '$line'" ;;
  esac
  # A top-level line, so it MUST be a record. Split with parameter expansion, never
  # `set -- $line`: word splitting also GLOBS, and cargo prints `(*)` on a de-duplicated
  # entry, which would expand against the working directory.
  case "$line" in
    *' '*) ;;
    *) unmeasurable malformed-record \
      "$PROBE_DESC printed a column-zero line that is not a '<name> v<version>' duplicate-group head: '$line'" ;;
  esac
  nm="${line%% *}"
  rest="${line#* }"
  ver="${rest%% *}"
  case "$nm" in
    *[!A-Za-z0-9_-]*) unmeasurable malformed-record \
      "$PROBE_DESC printed a column-zero line whose first field '$nm' is not a crate name: '$line'" ;;
  esac
  # THE WHOLE FIELD, not just its first character (roborev round 9, Medium). `v[0-9]*`
  # required only a `v` and ONE digit and let the trailing `*` swallow anything after it,
  # so a TRUNCATED `foo v1` or a garbage `foo v1garbage` satisfied it and was COUNTED —
  # a verdict derived from a document this parser does not actually recognise, which is
  # the one thing the closed grammar exists to prevent. `ver` is never used as a value
  # (the census keys on the crate NAME); validating it is purely how we recognise a line
  # as a cargo duplicate-group head, so it must match cargo's OWN output shape exactly.
  #
  # DERIVED FROM MEASURED OUTPUT, not assumed: `cargo tree -d --workspace --target all`
  # on this workspace prints 114 heads — 110 bare `vN.N.N`, plus `v0.11.1+wasi-snapshot-
  # preview1` and `v1.0.0+spec-1.0.0` style BUILD METADATA (dots AND hyphens inside it).
  # Cargo prints a resolved semver, always three numeric components. PRERELEASE (`-rc.1`)
  # appears nowhere in this corpus but is legal semver that cargo can print, so it is
  # ACCEPTED rather than refused — a guard that reds on correct input is the guard agents
  # learn to waive. Leading zeros in the numeric parts are likewise not refused: they are
  # not what this check is for, and cargo could only print what a manifest resolved to.
  if [[ ! $ver =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$ ]]; then
    unmeasurable malformed-record \
      "$PROBE_DESC printed a column-zero line whose second field '$ver' is not a complete v<major>.<minor>.<patch>[-prerelease][+build]: '$line'"
  fi
  now_instances=$((now_instances + 1))
  census_get "$NOW_CENSUS" "$nm" >/dev/null || now_names="$now_names $nm"
  census_bump "$NOW_CENSUS" "$nm"
done <"$TREE_TXT"

now_crates=0
for _n in $now_names; do now_crates=$((now_crates + 1)); done

# THREE-VALUED, deliberately. `cargo tree -d` prints NOTHING when a workspace has no
# duplicates at all, which is a legitimate measurement of zero. Output that has content
# but yields no parsed instance is a PARSER failure — the two must never be one state,
# because conflating them is how a broken parser reports a perfect score.
if [ "$now_instances" -eq 0 ] && [ "$nonblank" -gt 0 ]; then
  unmeasurable unparseable-output \
    "$PROBE_DESC printed $nonblank non-blank line(s) but NO column-zero '<name> v<version>' duplicate-group line was recognised"
fi

# THE CENSUS MUST BE POSSIBLE, not merely parseable. `cargo tree -d` reports DUPLICATE
# groups, so every crate it prints has at least TWO members — the identical rule the
# baseline reader already enforces on `crate <name> <n>`. A crate counted ONCE means the
# document is not the one this parser thinks it is reading, and a census assembled from an
# unvalidated document may not become a verdict.
for _n in $now_names; do
  _c="$(census_get "$NOW_CENSUS" "$_n")"
  [ "$_c" -ge 2 ] || unmeasurable implausible-census \
    "in the $PROBE_DESC census '$_n' appears $_c time(s) — a DUPLICATE group has at least 2 members, so this output is not a complete duplicate census"
done
say "MEASURED $now_instances duplicate instance(s) / $now_crates duplicated crate(s) from $PROBE_DESC"

# ---------------------------------------------------------------------------
# 2) REGENERATE mode stops here, publishing the measurement as the new baseline.
# ---------------------------------------------------------------------------
if [ "$MODE" = regenerate ]; then
  tmp="$(mktemp "$(dirname "$BASELINE")/dep-duplicates-baseline.XXXXXX")" || {
    say "ERROR could not create a temporary beside $BASELINE_REL"
    exit 1
  }
  {
    cat <<HEADER
# scripts/ci/dep-duplicates-baseline.txt — THE DUPLICATE-DEPENDENCY RATCHET BASELINE
# (issue #1700). GENERATED FILE — do not hand-edit; regenerate with ONE command:
#
#   bash $BASELINE_REGEN_CMD
#
# WHAT IT IS. The duplicate-dependency census of \`$PROBE_DESC\`: one \`instances\` total,
# one \`crates\` total, and one \`crate <name> <n>\` line per duplicated crate. The gate's
# ADVISORY \`dep-duplicates\` component re-measures and compares against it.
#
# IT IS A RATCHET, NOT A TARGET. The property asserted is "no WORSE than this". An
# increase is reported LOUDLY and never fails the gate (#1700 AC2); a DECREASE is
# reported as an invitation to re-run the regeneration command above and commit a
# tighter baseline. Some duplication here is not ours to fix — the arrow/parquet graph
# pins \`hashbrown\` five ways — and pinning it away with \`[patch]\` is explicitly out of
# scope.
#
# THE INVOCATION IS PINNED, AND BOTH FLAGS ARE LOAD-BEARING. \`--workspace\`: the bare
# \`cargo tree -d\` reads the ROOT PACKAGE only and reports a small fraction of this.
# \`--target all\`: \`cargo tree\` otherwise defaults to the HOST target, so this COMMITTED
# file would mean a different thing on a Linux lane than on a macOS one — a phantom
# advisory delta with no dependency change at all. \`--all-features\` reports a different,
# larger graph again and is deliberately not used. Changing the invocation invalidates
# every number below, so change it in scripts/ci/check-dep-duplicates.sh and regenerate
# here in one commit.
#
# GRAMMAR (closed; the reader REFUSES rather than guesses, and does not trim):
#   \`instances <N>\`      exactly once
#   \`crates <N>\`         exactly once
#   \`crate <name> <n>\`   once per duplicated crate, n >= 2
#   blank lines and lines whose FIRST character is \`#\` are skipped
#   the \`crate\` lines must COHERE with the totals (count == crates, sum == instances)
HEADER
    printf 'instances %s\n' "$now_instances"
    printf 'crates %s\n' "$now_crates"
    for _n in $now_names; do
      printf 'crate %s %s\n' "$_n" "$(census_get "$NOW_CENSUS" "$_n")"
    done
  } >"$tmp"
  # Atomic publish, beside the destination (the same reason agent-gate.components is
  # regenerated that way: `mv` is atomic only WITHIN one filesystem, and a reader of a
  # half-copied baseline sees a SHORT census — the silent under-count this guard exists
  # to prevent).
  mv "$tmp" "$BASELINE"
  say "REGENERATED $BASELINE_REL: $now_instances instance(s) / $now_crates crate(s). Commit it."
  exit 0
fi

# ---------------------------------------------------------------------------
# 3) READ THE BASELINE under the closed grammar. No trimming: a line with leading or
#    trailing whitespace is REFUSED, because a parser that trims is a parser that
#    guesses, and this file is the baseline of the comparison.
# ---------------------------------------------------------------------------
[ -f "$BASELINE" ] || baseline_unusable baseline-missing "$BASELINE_REL does not exist"
[ -r "$BASELINE" ] || baseline_unusable baseline-missing "$BASELINE_REL is not readable"

BASE_CENSUS="$WORK_DIR/base.census"
: >"$BASE_CENSUS"
base_instances=""
base_crates=""
base_names=""
base_crate_lines=0
base_sum=0
lineno=0
while IFS= read -r line || [ -n "$line" ]; do
  lineno=$((lineno + 1))
  case "$line" in
    '') continue ;;
    '#'*) continue ;;
  esac
  case "$line" in
    *[[:space:]]) baseline_unusable baseline-garbage "line $lineno has trailing whitespace: '$line'" ;;
    [[:space:]]*) baseline_unusable baseline-garbage "line $lineno has leading whitespace: '$line'" ;;
  esac
  # shellcheck disable=SC2086
  set -- $line
  case "$1" in
    instances)
      [ "$#" -eq 2 ] || baseline_unusable baseline-garbage "line $lineno: 'instances' takes exactly one value: '$line'"
      [ -z "$base_instances" ] || baseline_unusable baseline-garbage "line $lineno: a second 'instances' line"
      require_base_count "$2" "$lineno" "a count"
      base_instances="$2"
      ;;
    crates)
      [ "$#" -eq 2 ] || baseline_unusable baseline-garbage "line $lineno: 'crates' takes exactly one value: '$line'"
      [ -z "$base_crates" ] || baseline_unusable baseline-garbage "line $lineno: a second 'crates' line"
      require_base_count "$2" "$lineno" "a count"
      base_crates="$2"
      ;;
    crate)
      [ "$#" -eq 3 ] || baseline_unusable baseline-garbage "line $lineno: 'crate' takes a name and a count: '$line'"
      case "$2" in ''|*[!A-Za-z0-9_.-]*) baseline_unusable baseline-garbage "line $lineno: '$2' is not a crate name" ;; esac
      require_base_count "$3" "$lineno" "a count"
      [ "$3" -ge 2 ] || baseline_unusable baseline-garbage "line $lineno: '$2' is recorded $3 time(s) — a DUPLICATE needs at least 2"
      census_get "$BASE_CENSUS" "$2" >/dev/null && baseline_unusable baseline-garbage "line $lineno: '$2' is recorded twice"
      printf '%s %s\n' "$2" "$3" >>"$BASE_CENSUS"
      base_names="$base_names $2"
      base_crate_lines=$((base_crate_lines + 1))
      base_sum=$((base_sum + $3))
      ;;
    *)
      baseline_unusable baseline-garbage "line $lineno is not 'instances'/'crates'/'crate' under the closed grammar: '$line'"
      ;;
  esac
done <"$BASELINE"

[ -n "$base_instances" ] || baseline_unusable baseline-garbage "no 'instances' line"
[ -n "$base_crates" ] || baseline_unusable baseline-garbage "no 'crates' line"
# COHERENCE, not just shape (the pub-surface lesson): a file matching the grammar can
# still be arithmetically impossible, and such a file did not come from the generator —
# it is a truncation, a hand-edit or a bad merge, and comparing against it would excuse
# real growth.
[ "$base_crate_lines" -eq "$base_crates" ] || baseline_unusable baseline-garbage \
  "INCOHERENT: 'crates $base_crates' but $base_crate_lines 'crate' line(s)"
[ "$base_sum" -eq "$base_instances" ] || baseline_unusable baseline-garbage \
  "INCOHERENT: 'instances $base_instances' but the 'crate' lines sum to $base_sum"

# ---------------------------------------------------------------------------
# 4) COMPARE. The verdict is driven by the TOTALS — the ratchet's own metric — and the
#    per-crate census is what makes an increase ACTIONABLE by naming who grew.
# ---------------------------------------------------------------------------
grew=""
newly=""
for _n in $now_names; do
  now_n="$(census_get "$NOW_CENSUS" "$_n")"
  if b="$(census_get "$BASE_CENSUS" "$_n")"; then
    if [ "$now_n" -gt "$b" ]; then
      grew="$grew $_n($b->$now_n)"
    fi
  else
    newly="$newly $_n($now_n)"
  fi
done
shrank=0
for _n in $base_names; do
  base_n="$(census_get "$BASE_CENSUS" "$_n")"
  now_n="$(census_get "$NOW_CENSUS" "$_n")" || now_n=0
  [ "$now_n" -lt "$base_n" ] && shrank=$((shrank + 1))
done

# signed_delta <n>: a delta rendered with ITS OWN sign. The advisory branch below fires
# when EITHER metric rises, so the OTHER may have FALLEN — an unconditional `+` printed
# `+-2`, a malformed number in the one line an operator reads to decide whether to
# collapse the duplication or re-tighten the baseline. A zero delta renders as `0`: this
# branch is only reached when at least one metric rose, so a `0` here is the honest
# statement that the OTHER metric did not move.
signed_delta() {
  if [ "$1" -gt 0 ]; then printf '+%s' "$1"; else printf '%s' "$1"; fi
}

if [ "$now_instances" -gt "$base_instances" ] || [ "$now_crates" -gt "$base_crates" ]; then
  # LOUD AND TEXTUALLY DISTINCT, and it names WHO — never a bare number. Non-failing by
  # design (#1700 AC2): the exit status below is 0.
  say "ADVISORY-INCREASE the duplicate census GREW: $now_instances instance(s) vs baseline $base_instances (delta $(signed_delta "$((now_instances - base_instances))")), $now_crates crate(s) vs baseline $base_crates (delta $(signed_delta "$((now_crates - base_crates))"))"
  [ -n "$grew" ] && say "ADVISORY-INCREASE crates that gained instances:$grew"
  [ -n "$newly" ] && say "ADVISORY-INCREASE crates newly duplicated:$newly"
  [ -z "$grew$newly" ] && say "ADVISORY-INCREASE no single crate grew — the totals rose through crates absent from the baseline census, which means the baseline is stale relative to the invocation"
  say "ADVISORY-INCREASE this is ADVISORY and does NOT fail the gate (#1700). Either collapse the"
  say "ADVISORY-INCREASE duplication (a version-req nudge in one of OUR manifests — never a [patch]"
  say "ADVISORY-INCREASE or a pin that fights an upstream ecosystem), or accept it and re-run"
  say "ADVISORY-INCREASE   bash $BASELINE_REGEN_CMD"
  say "verdict ADVISORY-INCREASE ($now_instances/$now_crates vs baseline $base_instances/$base_crates)"
  exit 0
fi

# AFFIRMATIVE, never a bare zero: `0 INCREASE RECOGNISED` states that a scan RAN and
# recognised nothing, where a bare `0` reads as an all-clear from a scan that may not
# have happened at all.
say "0 INCREASE RECOGNISED — $now_instances duplicate instance(s) / $now_crates duplicated crate(s) vs baseline $base_instances/$base_crates"
if [ "$now_instances" -lt "$base_instances" ] || [ "$now_crates" -lt "$base_crates" ]; then
  say "RATCHET-LOOSE the census IMPROVED ($now_instances/$now_crates vs baseline $base_instances/$base_crates)."
  say "RATCHET-LOOSE tighten it in this same change: bash $BASELINE_REGEN_CMD"
elif [ -n "$grew$newly" ] || [ "$shrank" -gt 0 ]; then
  say "NOTE composition changed with no total increase (grew:${grew:- none} newly:${newly:- none} shrank: $shrank crate(s))"
fi
say "verdict NO-INCREASE ($now_instances/$now_crates vs baseline $base_instances/$base_crates)"
exit 0
