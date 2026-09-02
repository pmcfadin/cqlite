#!/usr/bin/env bash
#
# premerge-assert.sh — the #2456 pre-merge SHA guard + the #3465 gate-of-record
# guard, as a script (issues #2668, #3465).
#
# WHY THIS EXISTS
# ---------------
# The flow-closer certifies a SPECIFIC SHA: the exact tree the full gate of
# record and the final roborev pass actually ran on. If the PR's head has
# moved since that certification (a foreign push, a stale un-pushed rebase,
# someone else's commit), then `gh pr merge` would squash a DIFFERENT tree than
# the one the gate covered. That is the 2026-07-14 stale-merge escape on
# #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never
# pushed it, and `gh pr merge` squashed the PR's stale pre-fix head, transiently
# landing a known data-loss blocker on main. The GitHub required check re-runs
# on push but CANNOT catch a "merge of an old green head" — this SHA assertion
# is the real guard.
#
# #3465 adds the OTHER half. Verifying that the PR head equals a *claimed*
# certified sha never verified that a certified sha EXISTS. The gate-of-record
# convention was honour-system doctrine; this script is the one point every merge
# passes through, so the convention becomes a mechanism here: a summary file
# carrying a FULL-gate block with `RESULT: PASS`, `tree-integrity: PASS`,
# `dirty: no` (#3648), and provenance (`commit:` + `tree-start:`) covering the
# certified sha is REQUIRED.
#
# TWO DISTINCT ESCAPES, ONE MECHANISM
#   * #3408 — NO GATE AT ALL. That PR merged on 22 `--lite` PASSes and not one
#     `scripts/agent-gate.sh` run, because nothing in the merge path ever asked
#     for the full `==== AGENT-GATE SUMMARY ====` block. Refused here by the
#     block/marker/RESULT checks: lite and delta emit DISTINCT headers.
#   * #3616 — A REAL GATE, SOMEONE ELSE'S. A closer located its gate run dir by
#     RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER LANE's run dir, saw 33 of
#     37 components PASS, and was about to merge #3616 on PR #3580's verdict. The
#     count was real, the dir was real, the timestamps were plausible; only the
#     `run-id:` line exposed it, and only because a human read it. With
#     14000-27000 stale run dirs per box and up to 4 concurrent gates, recency
#     picks a peer ROUTINELY. This is what the `commit:` + `tree-start:` binding
#     below refuses: a peer's summary for a different PR carries THAT PR's branch
#     head, so requiring both abbreviations to match the certified sha turns a
#     cross-lane verdict from "a human might notice the run-id line" into a
#     mechanical refusal at the merge point. The sha comparison is therefore not
#     bookkeeping — it is the guard for the #3616 class.
#
# The gate-of-record argument is deliberately REQUIRED, not optional: an optional
# argument would leave the honour system exactly where it is. Omitting it is a
# usage failure (exit 3), which breaks pre-#3465 callers loudly and on purpose.
#
# TWO ACCEPTED SHAPES (#3465 review blocker): DIRECT, or ANCHORED DELTA
# --------------------------------------------------------------------
# CLAUDE.md's #1892 post-gate-polish rule MANDATES that a test/docs-only diff on
# top of a full PASS at anchor `X` re-certifies with `scripts/agent-gate.sh
# --delta X` and "never a repeat full gate", and that the PR record BOTH the
# delta block AND the anchor's full SUMMARY. So the merged head `Y` legitimately
# differs from the gate of record's `X`, and a guard that accepted only the
# 3-argument shape red on correct, doctrine-mandated input — the guard agents
# learn to waive. Hence the OPTIONAL fourth argument:
#
#   CASE A (3 args) — DIRECT. The full block's `commit:`/`tree-start:` must
#     cover the certified sha. The gate of record ran on the merged tree itself.
#   CASE B (4 args) — ANCHORED DELTA. The full block is the ANCHOR (its sha need
#     NOT be the certified sha), and the fourth argument must be a `--delta`
#     block that (i) is a PASS with an intact tree, (ii) names that exact anchor
#     in `delta-anchor:`, (iii) whose OWN `commit:`/`tree-start:` cover the
#     certified sha, and (iv) whose anchor is ON THE CERTIFIED SHA'S HISTORY
#     (#3653 — `git merge-base --is-ancestor`, three-valued: BOUND proceeds,
#     NOT-ANCESTOR is exit 2, and anything UNMEASURABLE is exit 3 under
#     `PREMERGE: ANCHOR-UNVERIFIABLE`). Without (iv) the anchor's identity rested
#     on the delta run's SELF-DECLARED `delta-anchor:` line, so ANY full-gate
#     PASS plus a delta naming it satisfied the chain — the #3616 cross-lane
#     class surviving in the one path Case A's sha binding does not cover.
#     The chain is therefore closed end to end: full PASS at X, X on this PR's
#     history → delta re-cert anchored at X → delta ran on Y → Y is the PR head.
#
# In BOTH cases a full-gate PASS must EXIST, and the merged tree is covered
# either directly (A) or by an anchored delta re-cert on top of it (B). What is
# never accepted is a delta or lite block ALONE — the #3408 escape.
#
# We parse gh with gh's built-in `--jq` (jq expression run inside gh), so gh's
# JSON serialization is NOT load-bearing — we never read raw JSON with
# sed/regex. The gate summary is parsed by whole-line-anchored marker matching,
# after an ANSI strip that is BELT rather than the load-bearing part: the summary
# FILE's block lines are `echo`s of computed strings (scripts/agent-gate.sh
# emit_summary), so they are not coloured; `CARGO_TERM_COLOR` colours cargo
# output inside `gate.log`, not the block. The strip covers the case where the
# block was recovered from a coloured CAPTURE rather than from the summary file
# (#3400: colour survives redirection).
#
# FOUR RESIDUALS, STATED RATHER THAN FAKED
# ----------------------------------------
#  1. `run-id:` CANNOT be verified here. The #2874 reader contract says a reader
#     must confirm the summary's `run-id:` matches the run IT launched — this
#     script did not launch the gate, so it has nothing to compare against. It
#     therefore does not look at `run-id:` at all rather than pretend to. That is
#     precisely why the `commit:`/`tree-start:` binding carries the weight for the
#     #3616 cross-lane class above: it is the only property of a peer's summary
#     this script CAN falsify without having launched the run.
#  2. This assert proves a summary EXISTS claiming a full-gate PASS covering this
#     sha with an intact tree. It cannot prove that summary was produced by a
#     genuine gate run rather than hand-written. A HOSTILE INVOKER IS OUT OF THE
#     THREAT MODEL — whoever runs this script controls the process and could edit
#     the script or fabricate the file; no check inside a process defends against
#     the party that controls the process. What this guard defends is ACCIDENT
#     AND DRIFT, which is the observed failure mode: a diligent worker with no
#     step in its path telling it the gate of record was never run.
#
#     "SKIP THE SCRIPT ENTIRELY" IS A DIFFERENT AND LARGER GAP, AND IT IS **IN**
#     THE MODEL (#3653). It used to be filed above, under the hostile invoker —
#     which was not true: #3408's actual escape was ACCIDENT/DRIFT (22 `--lite`
#     PASSes and no full gate, because nothing in the merge path ever asked for
#     the block), and by the triage rule's own terms an accident route is a
#     defect, not out of model. Stated honestly: THIS GUARD CANNOT MAKE ITSELF
#     INVOKED; THE DOCUMENTS THAT ROUTE WORKERS HERE ARE THE CONTROL (CLAUDE.md's
#     flow-closer bullet, `.claude/agents/flow-closer.md`, `.claude/agents/
#     flow-lead.md`). A mechanism that closed it would have to live where the
#     merge is issued, not inside the check the merge path may never call.
#  3. THE CERTIFIED TREE IS NOT THE MERGED TREE (#3650). A squash-merge composes
#     this diff with main's CURRENT tip, not with the base the branch was written
#     against, so for any PR whose base is behind main the tree this script
#     certifies and the tree that lands are DIFFERENT OBJECTS. Measured on
#     #3358/PR #3362: base 2bde26a7c with main 10 commits ahead, whose head gate
#     FAILed `core-tests` only because the fix for a known flake (5e08db201,
#     #3514) was on main and absent from that base — the benign direction. The
#     MALIGN direction is a PASS at a stale head hiding an interaction with
#     something that landed in between: this assert would accept it, and the
#     merge would compose two things never tested together. So this script
#     proves FACT 1 — the diff is unchanged since certification and a full gate
#     of record PASSed on that exact tree — and it explicitly does NOT prove
#     FACT 2 — that the diff was certified against the main it will join. Fact 2
#     is a gate on the MERGE RESULT and is STILL NOT implemented here: it is
#     #3650's SLICE 2, filed separately. The success path SAYS so
#     (`PREMERGE: SCOPE`), because an enforcement that certifies the wrong tree
#     while CLAIMING to close #3465 would be the vacuous-pass shape one level up
#     — worse than the gap it replaces, which is at least visible.
#
#     WHAT SLICE 1 ADDED, AND WHAT IT DELIBERATELY DID NOT. This script now runs
#     `scripts/flow/base-staleness.sh` (resolved from its OWN directory, with no
#     env override — #3312's enforcer rule) and reports its finding on
#     `PREMERGE: ADVISORY` lines: `N` commits behind the merge-base and `M` of
#     those touching this diff's blast radius (paths the diff touches + a
#     hard-coded gate-global set). That is INFORMATION, not enforcement:
#     **the advisory can never change this script's exit code.** An advisory that
#     is absent, fails, or reports `UNMEASURED` is REPORTED and is not fatal in
#     slice 1. Two properties of it a reader must carry:
#       * `UNMEASURED` MUST be treated as STALE by any consumer, never as fresh
#         (#3650 D3) — the standing rule against deriving a pass from the absence
#         of a bad signal. Slice 2 is the consumer that will act on it.
#       * the blast radius is NOT a dependency closure. A commit changing an item
#         this diff CALLS, touching neither this diff's paths nor a gate-global
#         path, is reported as NOT staling. The advisory declares that on every
#         run; it is a real false-negative class, filed, not closed.
#     So the three `PREMERGE: SCOPE` lines are RETAINED: slice 1 does not close
#     the gap they disclose, and removing them would be exactly the overclaim
#     this residual exists to prevent.
#  4. THE #3653 ANCHOR BINDING — WHAT IT PROVES, AND THE BOUNDARY IT DOES NOT
#     CROSS (Case B only). Stated as a BOUNDARY rather than as another list of
#     closed attacks, because three review rounds found three routes into one
#     mechanism (graft -> environment/template -> commit-graph) and the pattern
#     predicts a fourth: that is CLAUDE.md's "one axis closed, space declared
#     done" shape (#3544 job 264), and the repository has already ruled on the
#     unclosable version of it (#3746 / job 311 — DECLARE the boundary in the
#     emitted line, hand the real subject to the issue that owns it).
#
#     IT PROVES: the anchor is an ancestor of the certified sha over the objects
#     and commit metadata THIS BOX'S SHARED OBJECT STORE PRESENTS, read in an
#     isolated scratch repository that is proof against — with a positive control
#     for each — `$GIT_DIR/info/grafts` (job 355), `refs/replace/*`, an inherited
#     `GIT_DIR`/`GIT_COMMON_DIR` and an ambient `GIT_TEMPLATE_DIR`/config
#     `init.templateDir` (job 358), and a trusted `commit-graph` (job 361). Each
#     of those is PEER-WRITABLE on this fleet, because every lane is a worktree
#     of ONE shared `.git`, and each also fires BY ACCIDENT as a leftover of an
#     unrelated debugging session — which is why they are closed and not merely
#     declared.
#
#     IT DOES NOT PROVE, and no further enumeration will change this:
#       (a) that the anchor is on the PR AS GITHUB SEES IT. The local branch may
#           hold commits never pushed, and no `gh` call here reads the PR's commit
#           list. The `commit:`/`tree-start:` binding plus the head compare are
#           what tie the chain to the sha GitHub will merge.
#       (b) ANYTHING AGAINST A HOSTILE SAME-UID PEER. This is the TERMINUS of
#           #3653's hardening line, and it is one fact with many faces. Git does
#           not rehash a packed object against the id it was asked for and not
#           every file in the shared store is content-addressed; AND the SCRATCH
#           is peer-writable too, so a peer can drop `.git/info/grafts` into it,
#           or replace the `repo` pathname, between `git init` and the walk —
#           reproducing the round-1 graft attack inside the thing built to prevent
#           it. Both halves have the SAME cause: every lane on this fleet runs as
#           the SAME USER, so no mode or ownership can admit this process and
#           exclude a peer, and the only alternative is a helper binary, which is
#           not this issue.
#
#           SO WHAT THIS GUARD CLAIMS, EXACTLY: bounded, environment-isolated, and
#           immune to grafts, replace refs, ambient templates and a trusted
#           commit-graph AGAINST ACCIDENT AND DRIFT — a stray graft in the lane
#           repo, a leftover `GIT_DIR`, an inherited template, a stale
#           commit-graph. It is explicitly NOT a boundary against a hostile
#           same-UID peer. The evidence line says so on every run, naming #3746,
#           which already owns "lanes share an object store".
#
#           A LATER SAME-UID-PEER INSTANCE IS NOT A NEW DEFECT. It is this
#           declared boundary, and it belongs to #3746. That is also what #3653
#           asked for: the issue text says the hostile route is largely closed
#           elsewhere and that the DEFECT WAS THE CONSTRAINT NOT BEING STATED
#           WHERE THE GUARD IS READ. Stating it is the fix.
#
# USAGE
#   scripts/flow/premerge-assert.sh <pr-number> <certified-sha> \
#       <gate-of-record-summary> [<delta-summary>]
#
# ENVIRONMENT
#   GH_REPO   the target repo (default: pmcfadin/cqlite). `gh` honors GH_REPO
#             natively; we pass --repo explicitly too so the default applies.
#
# EXIT CODES
#   0   gate of record verified + head matches + PR OPEN
#       — prints "PREMERGE: OK <sha>", "PREMERGE: SCOPE ..." (what was and was
#         NOT proven, #3650), "PREMERGE: ADVISORY ..." (the non-blocking
#         base-staleness report, #3650 slice 1 — it NEVER changes this exit
#         code) and "PREMERGE: GATE-OF-RECORD ..."
#         (plus "PREMERGE: DELTA-RECERT ... anchor-ancestry: BOUND ..." in Case B,
#          the affirmative record that the #3653 ancestry binding RAN)
#   2   no/invalid gate of record (INCLUDING a Case B anchor that is NOT on the
#       certified sha's history, #3653), OR head moved (mismatch), OR PR
#       closed/merged — LOUD multi-line refusal
#   3   gh/network failure, a required TOOL failing, an UNMEASURABLE anchor
#       ancestry, or a usage error — fail closed, never merge on uncertainty. The
#       four are distinguished by the printed marker, NOT by the code:
#       `PREMERGE: USAGE` (you called it wrong), `PREMERGE: TOOL-FAILURE` (a
#       broken box — fix the box, do NOT re-run the gate), `PREMERGE: GH-FAILURE`
#       (auth/network/no-such-PR), `PREMERGE: ANCHOR-UNVERIFIABLE` (#3653 — this
#       box could not measure whether the anchor is on this PR's history: no git,
#       no work tree, an absent object, a shallow/unproven history, a read that
#       TIMED OUT, or no `timeout`/`gtimeout` available to bound the reads at all).
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -euo pipefail

repo="${GH_REPO:-pmcfadin/cqlite}"

usage() {
  # A distinct grepable marker (#3465 review nit 8): exit 3 covers a USAGE error,
  # a TOOL failure and a GH failure, and a caller must be able to tell them apart
  # — "you called me wrong" is not "GitHub is down". The exit CODES are unchanged.
  printf 'PREMERGE: USAGE — the call is wrong (this is NOT a gh/network failure)\n' >&2
  printf 'usage: %s <pr-number> <certified-sha> <gate-of-record-summary> [<delta-summary>]\n' \
    "$(basename "$0")" >&2
  printf '       <gate-of-record-summary> is REQUIRED: the AGENT_GATE_SUMMARY_FILE of the\n' >&2
  printf '       FULL gate (a "==== AGENT-GATE SUMMARY ====" block with RESULT: PASS and\n' >&2
  printf '       tree-integrity: PASS). With 3 args it must be AT the certified sha.\n' >&2
  printf '       <delta-summary> is OPTIONAL: an "==== AGENT-GATE DELTA SUMMARY ====" block\n' >&2
  printf '       whose delta-anchor: is the full block above and whose own commit:/\n' >&2
  printf '       tree-start: are AT the certified sha (the #1892 post-gate-polish route).\n' >&2
  printf '       See #3465.\n' >&2
}

if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
  usage
  exit 3
fi

pr="$1"
certified="$2"
summary_file="$3"
delta_file="${4:-}"

if [ -z "$pr" ] || [ -z "$certified" ] || [ -z "$summary_file" ]; then
  usage
  exit 3
fi
# An EMPTY fourth argument is a usage failure, not "3-arg mode": a caller whose
# variable expanded to nothing must be told, never silently downgraded.
if [ "$#" -eq 4 ] && [ -z "$delta_file" ]; then
  usage
  exit 3
fi

# Normalize the certified SHA to lowercase and require a full 40-char hex SHA —
# an abbreviated or malformed value can never be safely compared to headRefOid.
certified=$(printf '%s' "$certified" | tr '[:upper:]' '[:lower:]')
case "$certified" in
  *[!0-9a-f]* | "")
    printf 'error: certified SHA must be 40 hex chars (got: %s)\n' "$2" >&2
    usage
    exit 3
    ;;
esac
if [ "${#certified}" -ne 40 ]; then
  printf 'error: certified SHA must be a full 40-char hex SHA (got %d chars: %s)\n' \
    "${#certified}" "$2" >&2
  usage
  exit 3
fi

# ---------------------------------------------------------------------------
# GATE OF RECORD (#3465) — checked FIRST, before any `gh` call. It is offline
# and cheap, and "you have no gate of record" must be reportable without a
# network round trip.
# ---------------------------------------------------------------------------

refuse_no_gate() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: NO-GATE-OF-RECORD — REFUSING TO MERGE\n' >&2
  printf '  summary file: %s\n' "$summary_file" >&2
  [ -n "$delta_file" ] && printf '  delta summary file: %s\n' "$delta_file" >&2
  printf '  certified sha: %s\n' "$certified" >&2
  while [ "$#" -gt 0 ]; do
    printf '  %s\n' "$1" >&2
    shift
  done
  printf '  The FULL gate is the only run that counts (#719). Run it once,\n' >&2
  printf '  immediately pre-merge, with the mandated redirect:\n' >&2
  printf '    AGENT_GATE_SUMMARY_FILE=<path> bash scripts/agent-gate.sh > gate.log 2>&1\n' >&2
  printf '  then pass <path> as the third argument.\n' >&2
  printf '  If the ONLY diff since a full PASS at anchor X is test/docs-only, the\n' >&2
  printf '  sanctioned route is the ANCHORED DELTA PAIR (#1892) — not a repeat full\n' >&2
  printf '  gate: scripts/agent-gate.sh --delta X --anchor-run-id <id>, then pass BOTH\n' >&2
  printf '  summaries: <anchor-full-summary> <delta-summary>. See #3465.\n' >&2
  printf '========================================================\n' >&2
  exit 2
}

# A required TOOL failing is NOT "no gate of record" and must NOT be answered
# with "go run a 45-minute gate" (#3465 review nit 6). The header reserves exit 3
# for tool/usage failure; route it there, naming the tool.
refuse_tool_failure() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: TOOL-FAILURE\n' >&2
  printf '  %s failed while parsing %s.\n' "$1" "$2" >&2
  printf '  This is a broken/absent tool on THIS box (missing, ENOMEM, bad PATH),\n' >&2
  printf '  not a verdict about the gate of record. Fix the box and re-run this\n' >&2
  printf '  assert — do NOT re-run the gate. Refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
}

# ---------------------------------------------------------------------------
# CASE B ONLY — THE DELTA ANCHOR MUST BE ON THE CERTIFIED SHA'S HISTORY (#3653)
# ---------------------------------------------------------------------------
# WHAT WAS MISSING. In Case B the anchor's identity rested entirely on the DELTA
# run's SELF-DECLARED `delta-anchor:` line: the checks above prove the full block
# and the delta block AGREE about a sha, never that the sha is on THIS PR. So any
# full-gate PASS, plus a delta naming it, satisfied the chain — the #3616
# cross-lane class surviving in the one path Case A's sha binding does not cover.
# `git merge-base --is-ancestor <anchor> <certified>` closes it offline and for
# free. (The accident path was ALSO narrowed by scripts/agent-gate.sh's own
# fail-closed `--delta` diff classification, which refuses anything but a
# test/docs-only diff between anchor and head — but a constraint that lives in
# ANOTHER script is not a constraint stated where this guard is read.)
#
# THE VERDICT IS THREE-VALUED, because `--is-ancestor`'s rc 1 is ITSELF
# three-valued (#3544): in a SHALLOW clone rc 1 also means "the connecting
# history is absent", so rc 1 is definitive ONLY in a repository proven complete.
# Reading it two-valued would refuse a correct merge in a shallow checkout — the
# guard agents learn to waive.
#   BOUND         rc 0 -> proceed, and RECORD it on the `PREMERGE: DELTA-RECERT`
#                 evidence line. An affirmative record is required: a silent pass
#                 is indistinguishable from a check that never ran.
#   NOT-ANCESTOR  rc 1 AND both objects present AND the repository proven
#                 complete -> exit 2 via refuse_no_gate ("your chain is wrong").
#   UNVERIFIABLE  anything that could not be MEASURED -> exit 3 under its OWN
#                 marker. An unmeasurable result is UNKNOWN, never ok (never
#                 derive a pass from the ABSENCE of a bad signal), and exit 3 is
#                 this script's home for "this box could not measure it — fix the
#                 box, do NOT re-run the gate", a DIFFERENT operator action from
#                 exit 2's "re-certify with a correct anchor".
#
# AMBIENT GIT STATE IS PINNED, the same two pins scripts/flow/base-staleness.sh
# carries and for the same reasons — both are cheap, and each turns a silently
# wrong answer into an ordinary git failure:
#   * `GIT_NO_LAZY_FETCH=1` — in a PARTIAL/PROMISOR clone a plain OBJECT READ
#     fetches over the network and WRITES packfiles, so "this is an offline
#     check" would be an intention rather than a property. A missing object then
#     fails the ANCESTRY call (`merge-base`), which routes here to UNVERIFIABLE —
#     the correct verdict for an unmeasurable read. **NOT every git call behaves
#     that way, and `cat-file -e` measurably does not** — see the presence-probe
#     note in assert_anchor_on_history, which is why the probe there is a
#     diagnostic refinement and not the soundness boundary. Honoured only from
#     git 2.36, so like base-staleness.sh this is a DECLARED precondition, not a
#     detected one.
#   * `GIT_NO_REPLACE_OBJECTS=1`, plus `--no-replace-objects` on every call — one
#     local `refs/replace/*` entry rewrites the ancestry `merge-base` walks, i.e.
#     it can make a foreign anchor LOOK like an ancestor. Honoured by every git
#     that has replacement refs at all, so it needs no version measurement.
# Neither is settable by the caller (#3312: the constrained party must not choose
# its own enforcer, and "which repository decides whether my anchor is on this
# PR" is exactly what a lane wanting to skip a re-cert would redirect).
#
# BUT THE PINS ARE NOT ENOUGH, AND THE WALK DOES NOT RUN IN THE LANE AT ALL
# (roborev job 355, High — the fix mirrors scripts/agent-gate.sh's component-set
# pre-flight rather than inventing anything). `$GIT_DIR/info/grafts` rewrites
# parentage and **`--no-replace-objects` does NOT disable it**: CLAUDE.md records
# the measurement under "A SYMLINK IS A BLOB, AND A GRAFT OUTLIVES
# `--no-replace-objects`" (roborev job 285, on #3544) — `no -> YES -> YES` across
# before-graft / plain / `--no-replace-objects`, i.e. the graft wins in all three.
# Re-measured for THIS check on git 2.43.0 with the same result. So a graft alone
# turns a FOREIGN anchor into `BOUND`, recreating the exact false acceptance this
# binding exists to refuse. It is IN MODEL on BOTH halves of the triage rule:
# every lane on this fleet is a worktree of ONE shared `.git` and grafts live in
# the COMMON git dir, so the planter is a PEER LANE (a non-invoker route) — and a
# leftover debugging graft is the accident route.
#
# When agent-gate.sh hit this in the same place — an ancestry walk reading the
# live repository — the ruling was to MOVE THE WALK, not to flag the graft. Same
# here: the object reads AND the `merge-base --is-ancestor` walk run inside a
# throwaway scratch repository (`git init` under TMPDIR), with the LANE's object
# directory exposed only through `GIT_ALTERNATE_OBJECT_DIRECTORIES`. An alternate
# is PURE OBJECT STORAGE: it carries no config and no `$GIT_DIR`, hence no
# grafts, no replace refs, no promisor, no `insteadOf`, and nothing a remote
# helper could be invoked from. Objects are CONTENT-ADDRESSED, so a read BY SHA
# there is exactly as trustworthy as one in the lane. Failing to build the
# scratch is UNVERIFIABLE — never a fall-back to the live repository, which would
# silently restore the hole.
#   Measured, this fixture, git 2.43.0: with a graft making the foreign anchor a
#   parent of the certified head, `merge-base --is-ancestor` in the lane answers
#   0 (attack works) and the same walk in the scratch answers 1 (refused).
#
# ONE PROBE STILL READS THE LANE ON PURPOSE: `--is-shallow-repository`.
# Shallowness is a property of the repository that HOLDS the history, and a fresh
# scratch is NEVER shallow — probing it there would answer `false`
# unconditionally and turn the shallow guard into a vacuous pass, which is the
# very shape this check is about. It is a state probe, not an object read, and it
# cannot be redirected by a graft.
#
# EVERY GIT CALL RUNS UNDER `env -i` + AN ALLOWLIST — AND THE COMMENT THAT USED
# TO SAY THE OPPOSITE WAS FALSE ONCE THE SCRATCH EXISTED (roborev job 358, High).
# The earlier text argued that these reads need no environment control because
# they are lane-local and addressed BY A SHA, so nothing an environment can set
# changes which bytes a sha resolves to. That argument is CORRECT for a read in
# the lane repository and WRONG for this design, and the difference is the whole
# finding: **an environment variable here does not bend the OBJECT, it bends
# WHICH REPOSITORY ANSWERS.** A scratch whose entire purpose is isolation has a
# load-bearing environment. Two measured routes, both of which manufacture
# `BOUND` (git 2.43.0):
#   * `GIT_DIR` OVERRIDES `-C`. `GIT_DIR=<grafted>/.git git -C <scratch>
#     rev-parse --git-dir` answers `<grafted>/.git`; under `env -i` it answers
#     `.git`. So an inherited `GIT_DIR`/`GIT_COMMON_DIR` redirects the walk
#     straight back into a grafted repository.
#   * A TEMPLATE SEEDS `info/grafts` INTO THE SCRATCH. `git init
#     --template=<dir>` and `GIT_TEMPLATE_DIR=<dir> git init` both copy a planted
#     `info/grafts` into the new repo (measured: content `x` present in both);
#     `env -i … git init --template=` produces none.
# So the isolated hop's environment is an ALLOWLIST (CLAUDE.md job 258: allowlist
# the safe shape, never blocklist the dangerous one — a denylist of git variables
# recedes), and it reaches EVERY site, including the lane DISCOVERY reads (job
# 276: the migrated reads ran under a bare `env` and the earlier hole re-opened
# at the NEW sites). An inherited `GIT_DIR` on a discovery read would make
# `--git-path objects`, `--git-common-dir` and `--is-shallow-repository` answer
# about a DIFFERENT repository, poisoning the alternate and the shallowness
# verdict alike.
#   ADMIT  only what git needs to RUN AT ALL. There is no network here, so —
#          unlike agent-gate.sh's pre-flight — nothing needs HOME for key or
#          `known_hosts` discovery, nothing needs SSH_AUTH_SOCK, and no proxy
#          variable is admitted. The list is PATH and TMPDIR.
#   CLEAR  everything else, BY DEFAULT via `env -i`, which is what makes this
#          closed: a git variable nobody has thought of is cleared without
#          needing to be discovered.
# THE CONFIG FILES ARE NOT SANITISABLE BY THE ENVIRONMENT — `/etc/gitconfig` and
# `~/.gitconfig` are FILES, so clearing `GIT_CONFIG_*` alone does not stop an
# `init.templateDir` set in one of them. They are neutralised by pointing
# `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` at `/dev/null` AND by passing an
# explicit empty `--template=` to `git init`.
#   HOW THOSE TWO ACTUALLY RELATE, MEASURED RATHER THAN ASSERTED, because an
#   earlier draft of this paragraph claimed "neither alone closes it" and the
#   suite falsifies that: remove ONLY the `--template=` and the template arm
#   still refuses (`env -i` cleared `GIT_TEMPLATE_DIR`); remove ONLY the `env -i`
#   and it still refuses (the explicit flag beats the inherited variable); remove
#   BOTH and the attack lands — `exit 0`, a false acceptance. So against every
#   route this suite can construct they are REDUNDANT WITH EACH OTHER, kept as
#   defence in depth rather than because each is separately necessary. What the
#   flag uniquely covers is a config-FILE `init.templateDir` — which the two
#   `/dev/null` neutralisers also cover, and which no test here constructs,
#   because doing so would mean writing to `/etc/gitconfig`.
# A side effect worth stating: clearing `GIT_DIR` makes this file's standing
# claim — "the repository is the CURRENT WORKING DIRECTORY's, with no env
# override" — true, where before a caller's `GIT_DIR` silently won.
#
# WHAT IS BOUNDED, AND WHAT IS NOT (roborev job 358, NARROWED by job 382). The
# EXTERNAL commands are bounded: every git call and `mktemp -d`, through the one
# `_anchor_bounded` runner. NOT BOUNDED, and the evidence line SAYS so rather than
# implying otherwise: `_anchor_canon` (`cd` + `pwd -P`) and the `[ -d … ]` probe on
# the lane object directory are SHELL BUILTINS — there is no process for a runner
# to signal — so on a stalled mount they can still hang this guard before any git
# read happens. Inventing a way to bound a builtin is explicitly NOT the fix; the
# claim is narrowed to what is true instead. One unbounded builtin stat was also
# DELETED rather than bounded (the scratch `.git` check — see its own note below),
# because removing code beats bounding it.
#
# THE BOUNDED READS (job 358, Medium). They read the SHARED
# object store, so a malformed loose object (a FIFO) or a stalled filesystem read
# would hang the merge guard forever instead of producing the documented
# `ANCHOR-UNVERIFIABLE`. In model on both halves: a peer lane can plant into that
# store, and a stalled mount is the accident route. The bound is the runner this
# script ALREADY resolves for the advisory (`resolve_advisory_timeout`, probed
# for `--kill-after` because a TERM-only bound bounds nothing) at the same
# `ADVISORY_TIMEOUT_SECS`/`ADVISORY_KILL_GRACE`; no second timing implementation
# is written, and `timeout(1)` owns and reaps its own child, so job 279's rule
# against signalling a process group you no longer own is not in play here.
# WHERE NO SUPPORTED RUNNER EXISTS THE CHECK REFUSES — `ANCHOR-UNVERIFIABLE`,
# naming the remedy. THIS REVERSES AN EARLIER DECISION IN THIS SAME FILE, and the
# reasoning is recorded because the earlier reasoning was persuasive and wrong.
#
# The earlier rule was: run unbounded, declare it on the evidence line, and do NOT
# refuse — because a hang is a LIVENESS failure that yields no verdict rather than
# a false pass, and refusing a box with no coreutils would be the guard that reds
# on correct input.
#
# WHAT THAT MISSED: a hang in this guard BLOCKS THE MERGE ANYWAY. So the real
# comparison was never "merge proceeds vs merge refused" — it is **hang forever
# with no diagnosis vs refuse immediately with a named cause and remedy**. Those
# have the SAME outcome for the merge, and the refusal strictly dominates: same
# non-merge, plus a diagnosis. "It cannot produce a false pass" was true and
# IRRELEVANT, because the alternative was never a pass. The second half of the old
# argument still stands, and is why the refusal names a ONE-COMMAND remedy rather
# than being a dead end.
#
# AND THE THIRD OPTION IS RULED OUT ON PURPOSE: do NOT hand-roll a portable
# bounded runner here. That is new PROCESS-LIFETIME code, and process lifetime is
# the family that has already produced three defects in this issue's own test
# scaffolding (an orphaning decoy, a vacuous census, an unregistered decoy
# cleanup). The cost of failing closed is bounded and fixed by one named command;
# the cost of a fourth lifetime bug inside a merge guard is not.
#
# The `anchor-reads: bounded-<n>s+<g>s` affirmation is KEPT for the path that does
# run, and there is deliberately NO `UNBOUNDED` spelling left to emit: a run that
# would have needed one never reaches the evidence line.
export GIT_NO_LAZY_FETCH=1
export GIT_NO_REPLACE_OBJECTS=1

# The value published on the DELTA-RECERT evidence line. Initialised to a value
# that would be VISIBLY wrong rather than left unset: if a future reordering ever
# printed the line without running the check, the operator must see that, not a
# `set -u` crash and not a plausible-looking blank.
ANCHOR_ANCESTRY=UNRECORDED

# refuse_anchor_unverifiable <anchor> <certified> <cause> <remedy-line>... —
# exit 3 under a marker TEXTUALLY DISTINCT from `PREMERGE: NO-GATE-OF-RECORD`
# (exit 2 — your chain is wrong) and from `PREMERGE: TOOL-FAILURE` (a broken
# parser on this box). "This box could not measure the ancestry" is a third
# operator action and gets its own name, for the same reason nit 8 split USAGE
# from GH-FAILURE: the exit CODES cannot carry that distinction.
refuse_anchor_unverifiable() {
  local anchor="$1" head="$2" cause="$3"
  shift 3
  printf '========================================================\n' >&2
  printf 'PREMERGE: ANCHOR-UNVERIFIABLE — REFUSING TO MERGE\n' >&2
  printf '  delta anchor:  %s\n' "$anchor" >&2
  printf '  certified sha: %s\n' "$head" >&2
  printf '  cause: %s\n' "$cause" >&2
  while [ "$#" -gt 0 ]; do
    printf '  %s\n' "$1" >&2
    shift
  done
  printf '  An UNMEASURABLE ancestry is UNKNOWN, never ok: a pass may not be derived\n' >&2
  printf '  from the ABSENCE of a bad signal. This is NOT a verdict about your chain\n' >&2
  printf '  and NOT a parser failure — it is "THIS BOX could not measure it". Fix the\n' >&2
  printf '  checkout/box and re-run this assert; do NOT re-run the gate.\n' >&2
  printf '========================================================\n' >&2
  exit 3
}

# ===========================================================================
# ANCHOR-PATH OPERATION AUDIT (roborev job 384) — A DECLARED ARTIFACT
# ===========================================================================
# WHY THIS EXISTS. Seven shipped-script findings on #3653 — graft, environment,
# commit-graph, no-runner, discovery status, filesystem probes, and cleanup twice
# — were all the SAME TWO QUESTIONS asked of a different operation: IS IT BOUNDED,
# and IS ITS TARGET VALIDATED. Letting a reviewer enumerate those one per round
# does not converge. So every operation in the anchor path is listed here with
# both answers, and an operation that is deliberately unbounded or unvalidated
# SAYS SO WITH ITS REASON.
#
# THIS IS A MAINTENANCE OBLIGATION, in the idiom of
# scripts/tests/workspace-test-disposition.txt: a NEW operation added to this path
# must join the table, and an omission is then visible in review rather than
# discovered by a reviewer round. The table records COMPLETENESS AND LABELLING,
# not truth — nothing mechanically enforces it, exactly as #1716 ruled for the
# tools/ disposition list.
#
# LEGEND  bound:  RUNNER = through _anchor_bounded (timeout + --kill-after)
#                 SELF   = bounded by its own construction
#                 NONE   = unbounded, with the reason stated
#         target: what the operation's inputs/targets are checked against
#
# ---------------------------------------------------------------------------
# OPERATION                                    | BOUND   | TARGET / INPUT VALIDATION
# ---------------------------------------------------------------------------
# resolve_advisory_timeout: command -v          | NONE    | closed hard-coded candidate
#   (PATH lookup for timeout/gtimeout)          |  (a)    | set {timeout,gtimeout}
# resolve_advisory_timeout: <cand> --kill-after | SELF    | capability PROBED, not assumed;
#   =1 1 true  (the capability probe)           | (1s)    | a rejecting cand is discarded
# command -v git                                | NONE    | n/a (presence only); absence is
#                                               |  (a)    | its own UNVERIFIABLE refusal
# rev-parse --git-dir       (work-tree probe)   | RUNNER  | cwd's repo, no env override;
#                                               |         | rc three-valued, 124/137 routed
# rev-parse --show-toplevel                     | RUNNER  | rc inspected; empty => refuse
# rev-parse --git-common-dir                    | RUNNER  | rc inspected; empty => refuse
# rev-parse --git-path objects                  | RUNNER  | rc inspected; non-empty, made
#                                               |         | absolute, canonicalised
# rev-parse --is-shallow-repository              | RUNNER  | must equal the literal `false`;
#                                               |         | anything else => UNVERIFIABLE
# _anchor_canon  (sh -c 'cd -- "$1" && pwd -P') | RUNNER  | empty input refused; rc now
#   x5: toplevel, common-dir, TMPDIR, created,  |         | INSPECTED three-valued (job 395):
#       object-dir                              |         | 124/137 -> TIMEOUT cause, rc 1 ->
#                                               |         | unresolvable-path cause. Empty
#                                               |         | output alone no longer decides.
# TMPDIR read                                    | n/a     | canonicalised, then proven
#                                               |         | OUTSIDE work tree AND git dir
# mktemp -d <tmp_canon>/premerge-anchor.XXXXXX   | RUNNER  | result: non-empty, canonical,
#                                               |         | outside repo, DIRECT child of
#                                               |         | tmp_canon, prefix-matched
# git init -q --template=  (scratch repo)       | RUNNER  | target is the validated scratch
#                                               |         | path; empty template forced
# cat-file -e <anchor>^{commit}   (in scratch)  | RUNNER  | input is a validated 40-hex sha
# cat-file -e <certified>^{commit} (in scratch) | RUNNER  | input is a validated 40-hex sha
# merge-base --is-ancestor <a> <b> (in scratch) | RUNNER  | validated 40-hex shas; rc
#                                               |         | three-valued (0/1/>=2)
# scratch removal          (_anchor_cleanup)    | n/a     | *** NOT PERFORMED (c) ***. The
#                                               |         | scratch is LEFT IN PLACE and its
#                                               |         | path reported on stderr, so there
#                                               |         | is no delete to bound and no
#                                               |         | delete target to validate.
# $(pwd) in the no-work-tree diagnostic          | NONE    | n/a (diagnostic text only)
#                                               |  (b)    |
# trap install (EXIT/INT/TERM/HUP)               | n/a     | registered BEFORE any resource
#                                               |         | exists; handlers re-raise. They
#                                               |         | REPORT the retained scratch and
#                                               |         | delete nothing. (This row was
#                                               |         | FALSE between jobs 388 and 390:
#                                               |         | the traps had been removed as
#                                               |         | collateral while the table still
#                                               |         | claimed them. Reconciled.)
# the scratch directory itself, between      | n/a     | *** NOT ISOLATED FROM A SAME-UID
#   `git init` and the walk                     |         | PEER (e) ***. Peer-writable; see
#                                               |         | the terminus statement.
# env -i + allowlist (wraps every git call)      | n/a     | ADMIT list is hard-coded, not
#                                               |         | env-derived; neutralisers last
# GIT_ALTERNATE_OBJECT_DIRECTORIES value         | n/a     | non-empty checked; C-quoted
# ---------------------------------------------------------------------------
#
# THE TWO DECLARED GAPS, both `NONE`, both shell builtins over LOCAL state:
#  (a) `command -v` stats PATH directories. Unboundable (a builtin), and no user
#      or repository data reaches it — PATH comes from the invoking environment.
#      A stalled directory ON PATH would hang it. Accepted, not fixed.
#  (b) `$(pwd)` in one refusal message reads the shell's own cwd. Same class, and
#      it only runs on a path that is already refusing.
# Neither is reachable from the SHARED object store or from TMPDIR, which are the
# surfaces the bounds exist for. The `anchor-reads:` token names both.
#
# AND THE BOUNDARY THIS WHOLE GUARD STOPS AT:
#  (e) THE SCRATCH NAMESPACE IS TRUSTED, NOT VERIFIED — exactly as the shared
#      object store is, for the same reason and with the same owner (#3746). A
#      same-UID peer can drop `.git/info/grafts` into OUR scratch, or replace the
#      `repo` pathname, between `git init` and the walk — reproducing round 1's
#      graft attack INSIDE the thing built to prevent it. There is no permission
#      boundary available: every lane on this fleet runs as the SAME USER, so
#      neither mode nor ownership can admit us and exclude a peer, and the only
#      alternative is a helper binary, which is not this issue. So the CLAIM is
#      narrowed instead of the hole patched.
#
# AND ONE DECLARED NON-OPERATION:
#  (c) THE SCRATCH DIRECTORY IS NOT DELETED. A delete through a peer-mutable
#      pathname cannot be made race-free in shell (`openat`/`unlinkat` do not
#      exist here), and a permission boundary cannot substitute because every lane
#      on this fleet runs as the SAME USER. Three rounds narrowed the same delete
#      and each only shrank the window, so it was removed. Cost: one object-less
#      `git init` under TMPDIR per Case B merge, which the OS reaps. Benefit: no
#      recursive delete can land on a concurrent lane's directory.
#
# WHAT THE AUDIT CHANGED, so the table is not read as a description of what was
# already there: `_anchor_canon` became RUNNER-bounded (it was the FIRST filesystem
# touch in the path and was unbounded); the two `[ -d … ]` builtin stats were
# DELETED as redundant with it rather than bounded; `rm -rf` became RUNNER-bounded
# and target-revalidated; and the `anchor-reads:` token was corrected twice — once
# for overclaiming, once for UNDERclaiming after these fixes.
#
# AND WHAT THE LATEST ROUND CHANGED (job 395): `_anchor_canon` stopped
# suppressing its exit status with `|| true`, so all FIVE canonicalisations now
# distinguish a TIMEOUT (124/137, stalled filesystem) from an unresolvable path
# (rc 1) instead of collapsing both onto "empty output". Its table row narrows
# accordingly: empty output alone no longer decides the cause. This was the one
# site job 374 did not reach, because its own `|| true` hid the status job 374
# taught every other call to inspect — and the comment there ARGUED for the
# suppression on safety grounds, which was true and was not the point.
#
# AND WHAT THE ROUND BEFORE THAT CHANGED (job 388), a MATERIAL change to what this
# table claims: the `rm -rf` row is now a DECLARED NON-DELETION, and the identity
# probe plus the whole delete-time revalidation are GONE with it. The
# `anchor-reads:` token lost `rm` from its bounded externals — its third
# correction, and the first caused by REMOVING an operation rather than by
# mis-describing one.
#   HONEST NOTE ON THE PREVIOUS ROUND: job 387's intended table update never
#   landed. The edit script that carried it aborted on a failed assertion before
#   writing, so the code fix went in while the table kept describing the older
#   delete, and the round's report claimed otherwise. The table state above is
#   written from the code as it now stands, not from that report.
#
# AND WHAT THE ROUND BEFORE THAT CHANGED (job 387): the
# cleanup's revalidation had a TOCTOU the audit did not see — it canonicalised
# first and deleted the RESOLVED path, so a symlink to another `premerge-anchor.*`
# child passed validation and redirected the recursive delete at that directory,
# plausibly a concurrent lane's scratch. This is a REFINEMENT of a row the audit
# correctly LISTED and had just hardened, not a row it missed. The fix deletes the
# RECORDED path with `rm -rf --`, requires it to canonicalise TO ITSELF, and adds
# the device+inode identity above with its measured limits.

# --- THE ISOLATED HOP'S ENVIRONMENT (roborev job 358) -----------------------
# See the header for the two measured routes and the ADMIT/CLEAR line. Built
# once, used by every git call below — lane discovery included.
# THE PRE-COMMAND OPTIONS, IN ONE ARRAY BECAUSE THEY MUST REACH EVERY SITE
# (roborev job 361 + job 276's rule). `-c core.commitGraph=false` is the new one:
# `objects/info/commit-graph` (and the `objects/info/commit-graphs/` chain) is
# reachable through the alternate, it is NOT content-addressed, and git TRUSTS its
# recorded parent edges — so a peer-writable forged graph is a parent-edge source
# the isolation does not otherwise remove.
#
# MEASURED ON THIS BOX (git 2.43.0), with a commit-graph whose CDAT parent slot was
# patched to name a FOREIGN commit and whose trailing checksum was recomputed:
#   rev-list --parents -1 <c2>              -> the FORGED parent   (graph trusted)
#   -c core.commitGraph=false, same command -> the REAL parent      (flag works)
#   rev-list <c2> | grep <foreign>          -> 1 by default, 0 with the flag
#   merge-base --is-ancestor <foreign> <c2> -> "no" in BOTH cases
# So the MECHANISM is real and measured, and the EXPLOIT AGAINST THIS PARTICULAR
# CALL DID NOT REPRODUCE on this git version: `merge-base --is-ancestor` did not
# take the forged edge, while `rev-list` did. The flag is therefore DEFENCE IN
# DEPTH against a measured-trusted metadata source that is one git version, or one
# refactor of this walk, away from mattering — not the fix for a reproduced
# exploit. That distinction is stated here, and in the test, rather than implied.
#
# WHAT WAS MEASURED AND DELIBERATELY *NOT* DISABLED, because widening past the
# measurement is guessing:
#   * `core.multiPackIndex` — with the pack `.idx` removed and a multi-pack-index
#     present, the object was NOT readable (`packfile ... index unavailable`), so
#     no evidence it substitutes for object lookup here, and none that it supplies
#     parent edges. Its hazard class is "which BYTES an oid resolves to", which is
#     #3746's trusted-store boundary, not a parent-edge route.
#   * reachability bitmaps — with a bitmap present, `GIT_TRACE2_PERF=1` during
#     `merge-base --is-ancestor` produced ZERO bitmap mentions. No evidence of
#     consultation. (Bitmaps serve packing and `rev-list --objects`.)
# Both are named so a future reader knows they were measured and left alone, not
# forgotten.
ANCHOR_GIT_OPTS=(--no-replace-objects -c core.commitGraph=false)

ANCHOR_GIT_ENV=()
_anchor_build_git_env() {
  ANCHOR_GIT_ENV=()
  # PATH: find `git` itself, and the bounding `timeout`/`gtimeout`.
  ANCHOR_GIT_ENV+=("PATH=${PATH:-/usr/bin:/bin}")
  # TMPDIR: git's own temporary files. Also where the scratch is created.
  [ -n "${TMPDIR:-}" ] && ANCHOR_GIT_ENV+=("TMPDIR=$TMPDIR")
  # THE NEUTRALISERS, LAST so nothing above can shadow them. The two config
  # variables are what make the FILE-based `init.templateDir` route inert; the
  # explicit `--template=` at the init call is the other half. No prompt: an
  # interactive credential prompt is one of the ways this could hang. Replace
  # refs and lazy fetch off as belt — the structural control is that no object
  # read runs in the live repository at all.
  ANCHOR_GIT_ENV+=("GIT_CONFIG_GLOBAL=/dev/null" "GIT_CONFIG_SYSTEM=/dev/null" \
                   "GIT_TERMINAL_PROMPT=0" "GIT_NO_REPLACE_OBJECTS=1" "GIT_NO_LAZY_FETCH=1")
  return 0
}
_anchor_build_git_env

# --- THE BOUND (roborev job 358) --------------------------------------------
# Resolved ONCE, from the runner this script already has. `ANCHOR_READS` is the
# affirmative token published on the evidence line; it is never silently empty.
ANCHOR_BOUND_RUNNER=""
ANCHOR_READS="UNRECORDED"

# THE DECLARED BOUNDARY, in ONE constant consumed by the ONE renderer (job 361,
# WIDENED at job 390 to name the SCRATCH NAMESPACE as well — see the terminus
# statement in the header. The two halves have the same cause and the same owner:
# every lane runs as the same user, so a peer can write both the shared object
# store and our scratch.
# following #3746 / job 311's mechanics). It is a CONSTANT and not a computed
# value on purpose: there is no measurement this script can take that would make
# it false, so a variable would only invite a future arm to omit it.
ANCHOR_PROVENANCE="ancestry over this box's SHARED object store and SCRATCH namespace: objects, metadata and scratch TRUSTED, not verified (#3746) — closes accident/drift, NOT a same-UID peer"
# _anchor_resolve_bound <anchor> <certified> — REFUSES when there is no supported
# runner (see the reversal in the header). The two shas are taken only so the
# refusal can name them, exactly like every other UNVERIFIABLE cause.
_anchor_resolve_bound() {
  local anchor="$1" head="$2" name
  if name=$(resolve_advisory_timeout) && ANCHOR_BOUND_RUNNER=$(command -v "$name" 2>/dev/null) &&
     [ -n "$ANCHOR_BOUND_RUNNER" ]; then
    # THE TOKEN NAMES EXACTLY WHAT IS BOUNDED (job 382), AND IT WAS CORRECTED AGAIN
    # BY THE job-384 AUDIT — in the UNDERSTATING direction this time, which is the
    # same defect wearing modest clothes. It first read a bare `bounded-<n>s+<g>s`
    # (an overclaim: builtin filesystem probes could hang). The audit then made
    # those probes bounded — canonicalisation via `sh -c 'cd … && pwd -P'`, and the
    # two `[ -d … ]` stats deleted as redundant with it — so the previous token's
    # `UNBOUNDED:cd/test-builtins` had become FALSE in the other direction.
    # What remains genuinely unbounded is named instead: `command -v` PATH lookups
    # and the single `$(pwd)` in the no-work-tree diagnostic, both shell builtins
    # over local state. Full per-operation record: the AUDIT TABLE in the header.
    ANCHOR_READS="bounded-${ADVISORY_TIMEOUT_SECS}s+${ADVISORY_KILL_GRACE}s(external:git,mktemp,sh;UNBOUNDED:command-v+pwd-builtins)"
    return 0
  fi
  ANCHOR_BOUND_RUNNER=""
  refuse_anchor_unverifiable "$anchor" "$head" \
    "no timeout/gtimeout on PATH supporting --kill-after, so the ancestry reads cannot be BOUNDED" \
    "These reads touch the SHARED object store, where a malformed loose object (a" \
    "FIFO) or a stalled mount makes them never return. UNBOUNDED they would HANG" \
    "this guard — which BLOCKS THE MERGE ANYWAY, with no diagnosis. So the choice is" \
    "not merge-vs-refuse: it is hang-forever-silently vs refuse-now-with-a-cause," \
    "and the refusal strictly dominates. A SIGTERM-only bound is not a bound either" \
    "(a child can ignore TERM), which is why --kill-after is PROBED, not assumed." \
    "REMEDY: install GNU coreutils (its timeout is gtimeout on macOS, and IS" \
    "accepted here), then re-run this assert. No gate re-run is needed."
}

# _anchor_run <git-args...> — ONE place every git call in this check goes
# through: `env -i` + the allowlist, bounded when a runner exists. Callers add
# location-specific variables by exporting them in a subshell (see _anchor_git).
# A timeout is reported through the exit code (124 from timeout(1), or 137 when
# the SIGKILL escalation was needed) and every caller maps those to UNVERIFIABLE.
# _anchor_bounded <external-cmd> <args...> — the ONE bounded-external runner
# (roborev job 382). Extracted from `_anchor_run` rather than duplicated, so
# `mktemp` and git share one bound and one env allowlist. Only EXTERNAL commands
# can go through it; a shell builtin has no process to bound, which is why the
# claim is narrowed rather than extended (see the header).
_anchor_bounded() {
  if [ -n "$ANCHOR_BOUND_RUNNER" ]; then
    env -i "${ANCHOR_GIT_ENV[@]}" "$ANCHOR_BOUND_RUNNER" \
      --kill-after="$ADVISORY_KILL_GRACE" "$ADVISORY_TIMEOUT_SECS" "$@"
  else
    env -i "${ANCHOR_GIT_ENV[@]}" "$@"
  fi
}

# MULTI-LINE ON PURPOSE: the structural census in the test suite detects function
# boundaries by a line that is exactly `}`, and a one-line definition left its
# scan running into the NEXT function (job 382 — it flagged a comment there).
_anchor_run() {
  _anchor_bounded git "${ANCHOR_GIT_OPTS[@]}" "$@"
}

# _anchor_refuse_timeout <anchor> <head> <what> [<subject>] — THE ONE timeout
# refusal, shared by EVERY bounded call (roborev job 374). One wording, so a new
# call site cannot invent its own, and so a timeout never borrows another cause's
# REMEDY: the whole reason this script splits exit 2 from exit 3 is to hand the
# operator the right ACTION, and "not inside a git work tree" sends someone to
# check their cwd when the real answer is a stalled filesystem.
#
# THE BODY IS NEUTRAL AND NAMES ITS SUBJECT, and that is the job-410 fix: the text
# used to assert "this is NOT a statement about your cwd, your TMPDIR or your
# object paths" and send the operator to the OBJECT STORE — while being the shared
# destination for timeouts that occurred canonicalising TMPDIR, running `mktemp`,
# or canonicalising the created scratch. So the job-395 change (route all five
# canonicalisations here) moved the misleading-remedy defect one layer DOWN rather
# than removing it: third instance of the class, after jobs 374 and 395.
#
# A NEUTRAL MESSAGE THAT NAMES THE SUBJECT beats five bespoke strings — it is less
# to keep correct, and the "keep it correct" part is exactly what failed twice.
# `<subject>` is the path or store the timed-out operation actually touched; when a
# caller has none to give, the message says so rather than guessing one.
_anchor_refuse_timeout() {
  local subject="${4:-}"
  # An EMPTY 4th arg must not become a blank indented line in the refusal, so the
  # subject is passed as a possibly-empty ARRAY rather than an interpolated string.
  # `${arr[@]+"${arr[@]}"}` is the bash 3.2-safe expansion of a possibly-empty
  # array under `set -u`.
  local subj_lines=()
  [ -n "$subject" ] && subj_lines=("SUBJECT: the timed-out operation was acting on: $subject")
  refuse_anchor_unverifiable "$1" "$2" \
    "$3 timed out after ${ADVISORY_TIMEOUT_SECS}s (+${ADVISORY_KILL_GRACE}s kill grace)" \
    "The call did not return. Every bounded operation here touches a filesystem —" \
    "the SHARED object store, the repository metadata beside it, or TMPDIR where" \
    "the scratch lives — and a stalled mount or a malformed loose object (a FIFO)" \
    "stops any of them. A hang would leave the merge guard with NO verdict at all," \
    "which the bound converts into this one." \
    ${subj_lines[@]+"${subj_lines[@]}"} \
    "REMEDY: check that subject and the filesystem it lives on, then re-run this" \
    "assert. This says nothing about whether your chain is correct (that would be" \
    "exit 2) and nothing about the OTHER paths this script reads — only the one" \
    "named above timed out."
}

# _anchor_timed_out <rc> — TRUE when the runner terminated the call rather than
# git answering. Only meaningful when a runner is in use; with none, no exit code
# means "timed out" and the test is simply never true.
_anchor_timed_out() {
  [ -n "$ANCHOR_BOUND_RUNNER" ] || return 1
  [ "$1" = 124 ] || [ "$1" = 137 ]
}

# _anchor_lane <git-args...> — a DISCOVERY read of the lane repository (cwd).
# Same isolation: an inherited GIT_DIR would make discovery answer about another
# repository, which poisons the alternate and the shallowness verdict (job 276:
# the allowlist has to reach the sites a later change adds).
# (The options come from ANCHOR_GIT_OPTS inside _anchor_run — ONE definition for
# every site, so a new option cannot reach some calls and miss others.)
_anchor_lane() { _anchor_run "$@"; }

# --- THE ISOLATED SCRATCH REPOSITORY (roborev job 355) ----------------------
# REGISTRATION PRECEDES THE RESOURCE (CLAUDE.md job 282): the variable is
# declared and the traps installed HERE, above any `mktemp`, and the handler is
# empty-safe. Nothing here deletes — see below — so the handler only ever REPORTS.
# THE SCRATCH IS DELIBERATELY NOT DELETED (roborev job 388). Three consecutive
# rounds narrowed this one recursive delete — bounded it, validated its target,
# then closed a symlink TOCTOU — and each fix could only SHRINK the check-to-use
# window. A peer can still replace the path with another real directory after
# validation and have it recursively deleted.
#
# IT IS UNCLOSABLE IN SHELL. Descriptor-relative, no-follow deletion needs
# `openat`/`unlinkat`; bash has neither, so every delete here goes through a
# PEER-MUTABLE PATHNAME. A permission boundary cannot substitute either, because
# EVERY LANE ON THIS FLEET RUNS AS THE SAME USER — there is no mode or ownership
# that lets this process delete its own scratch while denying a peer the ability
# to swap it. Removal or nothing, not a tuning problem.
#
# THE TRADE, stated because it is a real cost and not a free win, and MEASURED
# rather than estimated: one small directory per Case B INVOCATION is left under
# TMPDIR. Note INVOCATION, not merge — the test suite exercises Case B dozens of
# times per run, and 240 such directories accumulated on this box during one round
# of development before that was noticed. A caller that invokes this repeatedly
# should point TMPDIR at a directory it owns and removes wholesale (which is what
# scripts/tests/test_premerge_assert.sh now does); for an actual merge it is one — an object-less `git init` on
# the RARE path (Case B is #1892's post-gate-polish route, not the ordinary
# merge) — and the OS reaps TMPDIR. Against that: a recursive delete that can land
# on a concurrent lane's directory. Not comparable, which is the same reasoning
# that already made every refusing branch leave the directory in place.
#
# GONE WITH IT: the persisted root, the basename-prefix revalidation, and the
# device+inode identity probe — whose own measured limits (ext4 REUSES a
# just-freed inode, so a same-path recreate was invisible; and no portable `stat`
# format exists) were the clearest sign this was being narrowed, not solved.
ANCHOR_SCRATCH=""

# _anchor_cleanup — REPORTS the retained scratch path. It deletes nothing (see
# above); its whole job is to make sure an operator learns where the directory
# went, on EVERY post-creation exit path.
#
# THE TRAPS WERE DOCUMENTED IN THE AUDIT TABLE AND DID NOT EXIST (roborev job 390,
# finding 3). They were removed as collateral by a span-replacing edit when the
# delete came out, leaving ONE call on the success path — so every refusal, and
# every signal, retained a scratch directory with NONE of the promised notice
# while the table still claimed the traps were installed. A false claim in a
# declared artifact is worse than a missing one, which is this issue in one line.
#
# Restored NON-DELETING: registered before any resource exists (job 282), signals
# included because bash runs no EXIT trap for a signal at its default disposition,
# each re-raising so the process still dies of what it was sent. There is nothing
# to race here — reporting a path cannot delete anything.
_anchor_cleanup() {
  [ -n "$ANCHOR_SCRATCH" ] || return 0
  printf 'PREMERGE: NOTE scratch dir left in place (NOT deleted): %s\n' "$ANCHOR_SCRATCH" >&2
  printf 'PREMERGE: NOTE  a delete through a peer-mutable pathname cannot be made race-free in\n' >&2
  printf 'PREMERGE: NOTE  shell (no openat/unlinkat), and every lane here runs as the same user,\n' >&2
  printf 'PREMERGE: NOTE  so it was REMOVED rather than narrowed a fourth time (#3653). It is an\n' >&2
  printf 'PREMERGE: NOTE  object-less git init under TMPDIR; the OS reaps it.\n' >&2
  ANCHOR_SCRATCH=""
  return 0
}
trap '_anchor_cleanup' EXIT
trap '_anchor_cleanup; trap - INT;  kill -INT  $$' INT
trap '_anchor_cleanup; trap - TERM; kill -TERM $$' TERM
trap '_anchor_cleanup; trap - HUP;  kill -HUP  $$' HUP

# _anchor_canon <dir> — canonicalize with `cd`+`pwd -P`, EMPTY on failure. The
# convention scripts/flow/base-staleness.sh uses (no `realpath` dependency), and
# every caller treats empty as "could not measure", never as "outside".
#
# AN EMPTY ARGUMENT IS REFUSED EXPLICITLY, AND WITHOUT THAT LINE EVERY
# "could not be resolved" BRANCH WAS UNREACHABLE (found while RED-verifying job
# 374). Measured: **`cd ""` SUCCEEDS in bash and leaves the shell where it is**,
# so `(cd "" && pwd -P)` prints the CURRENT DIRECTORY. A discovery call that
# failed or timed out therefore yielded cwd instead of an empty string, the
# `[ -z … ]` guards never fired, and the check proceeded on a plausible-looking
# wrong value — the RED control for job 374 exited 0 with `PREMERGE: OK`, i.e. a
# FALSE PASS, not merely a wrong remedy. The timeout propagation above closes the
# timeout route; this line closes the ordinary-git-failure route, which no status
# check can, because there the value really is empty.
# BOUNDED SINCE THE job-384 AUDIT, and it is the operation that most needed it:
# canonicalising `TMPDIR` is the FIRST filesystem touch in this path, before any
# git call, so a stalled mount there hung the guard with nothing to stop it. `cd`
# and `pwd` are builtins with no process to bound — but `sh -c 'cd … && pwd -P'`
# is an EXTERNAL command, so it goes through the existing runner. Verified to
# agree with the builtin form, symlinked input included.
#
# A missing/unreadable path and an absent `sh` yield EMPTY, which every caller
# treats as "could not measure" and refuses on — the fail-closed direction. A
# TIMEOUT is no longer in that set: since job 395 the rc is preserved and each
# caller routes 124/137 to the TIMEOUT cause, because failing closed with the
# WRONG REMEDY still sends an operator to the wrong place.
# THE EXIT STATUS IS PRESERVED, NOT SUPPRESSED (roborev job 395). It used to end
# in `|| true`, and the comment above justified that: a timeout, a missing path and
# an absent `sh` all yield EMPTY, and every caller treats empty as
# could-not-measure. That reasoning was right about SAFETY — it fails closed, the
# merge stays blocked, no false pass — and wrong about DIAGNOSIS: an operator sent
# to hunt an unreadable path when the real cause is a stalled mount. That is the
# misleading-remedy class job 374 fixed everywhere else; this was the one site that
# escaped it, because its `|| true` hid the status job 374 taught every other call
# to inspect.
#
# The rc is naturally THREE-VALUED here, which is what makes the split possible:
#   0        canonicalised; stdout is the path
#   1        `cd` failed — a genuinely missing/unreadable path (its own cause)
#   124/137  the runner terminated it — a stalled filesystem (the TIMEOUT cause)
_anchor_canon() {
  [ -n "$1" ] || return 0
  _anchor_bounded sh -c 'cd -- "$1" && pwd -P' sh "$1" 2>/dev/null
}

# _anchor_build_scratch — on success sets ANCHOR_SCRATCH (the mktemp dir),
# ANCHOR_SCRATCH_REPO (the git dir to run in) and ANCHOR_ALT (the value for
# GIT_ALTERNATE_OBJECT_DIRECTORIES). On failure returns 1 with ANCHOR_SCRATCH_ERR
# set to a cause sentence. NEVER falls back to the live repository.
ANCHOR_SCRATCH_REPO=""
ANCHOR_ALT=""
ANCHOR_SCRATCH_ERR=""
_anchor_build_scratch() {
  local anchor="$1" head="$2"
  local lane_objects repo_canon common_canon tmp_req tmp_canon created_canon alt_q
  local created_base
  local rc toplevel commondir lane_objects_canon

  # THE SCRATCH MUST BE PROVABLY OUTSIDE THE REPOSITORY, and "the repository" is
  # BOTH roots: the work tree AND the git COMMON dir (these are worktrees of one
  # shared `.git`, so the common dir is elsewhere entirely). Reasoning and the
  # two-phase check are base-staleness.sh's, reused rather than reinvented: the
  # pre-check answers about the path as it resolved a moment ago, so what was
  # ACTUALLY CREATED is re-validated, because a symlink swapped in between lands
  # the real directory somewhere else.
  # SPLIT so the status is visible: the old form nested the git call inside two
  # command substitutions with a `|| true`, which discarded a timeout entirely and
  # reported it as an unresolvable work-tree root (job 374).
  rc=0
  toplevel=$(_anchor_lane rev-parse --show-toplevel 2>/dev/null) || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "the work-tree root probe (rev-parse --show-toplevel)" \
      "this checkout (the current working directory's repository)"
  fi
  rc=0
  commondir=$(_anchor_lane rev-parse --git-common-dir 2>/dev/null) || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "the git common-directory probe (rev-parse --git-common-dir)" \
      "this checkout (the current working directory's repository)"
  fi
  # A non-timeout failure, or an empty answer, keeps its ORIGINAL cause below:
  # those are real states (an unusable repository) with their own remedy.
  # EACH CANONICALISATION INSPECTS ITS STATUS (job 395), so a stalled mount gets
  # the TIMEOUT cause and its remedy instead of borrowing the unresolvable-path
  # one. The non-timeout failure keeps that original cause below — it is a real
  # state (an unusable repository) with its own remedy.
  rc=0
  repo_canon=$(_anchor_canon "$toplevel") || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "canonicalising the work-tree root (cd + pwd -P)" \
      "$toplevel"
  fi
  rc=0
  common_canon=$(_anchor_canon "$commondir") || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "canonicalising the git common directory (cd + pwd -P)" \
      "$commondir"
  fi
  if [ -z "$repo_canon" ] || [ -z "$common_canon" ]; then
    ANCHOR_SCRATCH_ERR="the work-tree root and/or git common directory could not be resolved, so a scratch location cannot be proven outside them"
    return 1
  fi
  tmp_req="${TMPDIR:-/tmp}"
  rc=0
  tmp_canon=$(_anchor_canon "$tmp_req") || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "canonicalising TMPDIR (cd + pwd -P)" \
      "$tmp_req"
  fi
  if [ -z "$tmp_canon" ]; then
    ANCHOR_SCRATCH_ERR="the scratch root TMPDIR=$tmp_req could not be resolved (absent, unreadable, or not a directory)"
    return 1
  fi
  case "$tmp_canon/" in
    "$repo_canon"/* | "$common_canon"/*)
      ANCHOR_SCRATCH_ERR="the scratch root $tmp_canon resolves INSIDE the repository (work tree $repo_canon / git dir $common_canon); NOTHING was created"
      return 1 ;;
  esac
  # BOUNDED (job 382): `mktemp` is an EXTERNAL command touching the filesystem, so
  # a stalled TMPDIR mount would hang the guard before any git read.
  rc=0
  ANCHOR_SCRATCH=$(_anchor_bounded mktemp -d "$tmp_canon/premerge-anchor.XXXXXX" 2>/dev/null) || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "creating the scratch directory (mktemp -d)" \
      "$tmp_canon"
  fi
  # No `[ -d … ]` stat here: `_anchor_canon` below is BOUNDED and returns empty
  # unless the path is a directory this process can `cd` into, which is a strictly
  # stronger check than an unbounded builtin stat (job-384 audit).
  if [ "$rc" -ne 0 ] || [ -z "$ANCHOR_SCRATCH" ]; then
    ANCHOR_SCRATCH=""
    ANCHOR_SCRATCH_ERR="could not create a scratch directory under $tmp_canon"
    return 1
  fi
  rc=0
  created_canon=$(_anchor_canon "$ANCHOR_SCRATCH") || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "canonicalising the created scratch directory (cd + pwd -P)" \
      "$ANCHOR_SCRATCH"
  fi
  if [ -z "$created_canon" ]; then
    ANCHOR_SCRATCH_ERR="the created scratch directory could not be canonicalized"
    return 1
  fi
  case "$created_canon/" in
    "$repo_canon"/* | "$common_canon"/*)
      ANCHOR_SCRATCH_ERR="the CREATED scratch directory $created_canon resolves INSIDE the repository — the root resolved into the checkout between the pre-check and the create"
      return 1 ;;
  esac
  # THE CREATED PATH MUST BE AN EXPECTED DIRECT CHILD OF THE ROOT IT WAS VALIDATED
  # AGAINST. `mktemp` was given `$tmp_canon/premerge-anchor.XXXXXX`, so anything
  # else means the value did not come from that template. This is RETAINED after
  # the delete was removed (job 388) — it is now validating the directory this run
  # will `git init` into and expose as an alternate, not a delete target.
  created_base="${created_canon##*/}"
  if [ "$created_canon" != "$tmp_canon/$created_base" ] ||
     [ "$created_base" = "${created_base#"premerge-anchor."}" ]; then
    ANCHOR_SCRATCH_ERR="the created scratch directory $created_canon is not a premerge-anchor.* DIRECT child of $tmp_canon, so it did not come from the mktemp template this run asked for"
    return 1
  fi
  ANCHOR_SCRATCH="$created_canon"

  # THE LANE'S OBJECT DIRECTORY, resolved rather than assumed — and this is the
  # ONE read that still happens in the live repository, mirroring the reference
  # implementation (agent-gate.sh: `rev-parse --git-path objects`). In a
  # `git worktree` it answers the SHARED store of the parent checkout, i.e. the
  # objects dir under `--git-common-dir`, which is where HEAD's objects live
  # (measured here: `--git-path objects` = /…/repo/.git/objects while
  # `--git-dir` = /…/repo/.git/worktrees/<lane>). It reads no object and reaches
  # no network; a graft cannot redirect a path.
  rc=0
  lane_objects=$(_anchor_lane rev-parse --git-path objects 2>/dev/null) || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "the object-directory probe (rev-parse --git-path objects)" \
      "this checkout (the current working directory's repository)"
  fi
  [ "$rc" -eq 0 ] || lane_objects=""
  # MADE ABSOLUTE: `--git-path` answers RELATIVE TO THE WORK-TREE ROOT for a
  # plain clone (`.git/objects`) and absolute only for a worktree. A relative
  # value would be resolved against the SCRATCH, a different directory, and the
  # alternate would point at nothing. (`--path-format=absolute` is git >= 2.31,
  # so the prefix is applied here rather than relied upon.)
  case "$lane_objects" in
    '')
      ANCHOR_SCRATCH_ERR="this repository's object directory resolved to an EMPTY path, so its objects cannot be made readable to the isolated repository"
      return 1 ;;
    /*) : ;;
    *)  lane_objects="$repo_canon/$lane_objects" ;;
  esac
  # BOUNDED, and it keeps its own diagnostic (job 374's lesson: a broken object
  # directory must not borrow the absent-object remedy). The unbounded builtin
  # stat this replaces was the last first-touch filesystem probe in the path.
  # SPLIT OUT of an `[ -z "$(...)" ]` test, which discarded the status entirely —
  # the most thorough of the five suppressions (job 395).
  rc=0
  lane_objects_canon=$(_anchor_canon "$lane_objects") || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "canonicalising the object directory (cd + pwd -P)" \
      "$lane_objects"
  fi
  if [ -z "$lane_objects_canon" ]; then
    ANCHOR_SCRATCH_ERR="this repository's object directory ($lane_objects) is not a readable directory"
    return 1
  fi

  ANCHOR_SCRATCH_REPO="$ANCHOR_SCRATCH/repo"
  # `--template=` (EMPTY). `git init --template=<dir>` and `GIT_TEMPLATE_DIR=<dir>
  # git init` BOTH copy a planted `info/grafts` into the new repository
  # (measured), which would make the scratch born grafted and the isolation
  # worthless. This flag and the allowlist's cleared environment are REDUNDANT
  # with each other against that route — measured: removing either alone still
  # refuses, removing both lets the attack land at exit 0 (see the header). Kept
  # as defence in depth, and because the flag additionally beats a config-FILE
  # `init.templateDir` that no environment can clear.
  rc=0
  _anchor_run init -q --template= "$ANCHOR_SCRATCH_REPO" >/dev/null 2>&1 || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "initialising the isolated scratch repository (git init)" \
      "$ANCHOR_SCRATCH_REPO"
  fi
  # THE `[ -d "$ANCHOR_SCRATCH_REPO/.git" ]` CHECK IS DELETED (job 382), not
  # bounded: it was an UNBOUNDED builtin stat, and it was redundant for
  # CORRECTNESS — a `git init` that exits 0 without creating a repository would
  # make every bounded read below fail, so the check still refuses. Removing code
  # beats bounding it.
  #   THE TRADE, stated: in that (essentially impossible) case the refusal would
  #   name "the ANCHOR commit is not present in this repository" rather than a
  #   broken scratch — a less accurate cause for a state that requires a broken
  #   git. An unbounded stat on a stalled mount is a real hang; that is not.
  if [ "$rc" -ne 0 ]; then
    ANCHOR_SCRATCH_ERR="could not initialise an isolated scratch repository at $ANCHOR_SCRATCH_REPO"
    return 1
  fi

  # C-QUOTED, ALWAYS. `GIT_ALTERNATE_OBJECT_DIRECTORIES` is a COLON-separated
  # list, so a colon in the path cannot be expressed raw — but git accepts a
  # C-quoted entry, and agent-gate.sh measured all five shapes (plain, colon,
  # space, embedded `"`, embedded `\`) resolving correctly through it. Quoting
  # unconditionally avoids a second code path for the common case. ORDER IS
  # LOAD-BEARING: escape backslashes FIRST, then quotes, or the backslash pass
  # escapes the backslashes the quote pass just added.
  alt_q="${lane_objects//\\/\\\\}"
  alt_q="${alt_q//\"/\\\"}"
  ANCHOR_ALT="\"$alt_q\""

  # NESTED ALTERNATES NEED NO CARRY-OVER, AND THAT IS MEASURED, NOT ASSUMED. If
  # the lane's object store itself has `objects/info/alternates`, git FOLLOWS it
  # transitively from an entry named in GIT_ALTERNATE_OBJECT_DIRECTORIES —
  # measured on git 2.43.0: an object present ONLY in repo A was resolved from a
  # scratch repository whose alternate named repo B, where B reached A solely
  # through its own `info/alternates`. So an object reachable in the lane only
  # through its alternates is reachable here too, and copying the list would be
  # a second, drift-prone source for the same fact.
  return 0
}

# _anchor_git <git-args...> — a git call INSIDE the isolated scratch repository,
# with the lane's objects supplied as an alternate. This is the only place the
# ancestry reads happen; nothing here consults the lane's config.
_anchor_git() {
  # The alternate is the one location-specific value layered on top of the
  # allowlist — appended AFTER it, so it cannot be shadowed by a neutraliser and
  # cannot smuggle anything else in.
  if [ -n "$ANCHOR_BOUND_RUNNER" ]; then
    env -i "${ANCHOR_GIT_ENV[@]}" "GIT_ALTERNATE_OBJECT_DIRECTORIES=$ANCHOR_ALT" \
      "$ANCHOR_BOUND_RUNNER" --kill-after="$ADVISORY_KILL_GRACE" "$ADVISORY_TIMEOUT_SECS" \
      git "${ANCHOR_GIT_OPTS[@]}" -C "$ANCHOR_SCRATCH_REPO" "$@"
  else
    env -i "${ANCHOR_GIT_ENV[@]}" "GIT_ALTERNATE_OBJECT_DIRECTORIES=$ANCHOR_ALT" \
      git "${ANCHOR_GIT_OPTS[@]}" -C "$ANCHOR_SCRATCH_REPO" "$@"
  fi
}

# assert_anchor_on_history <anchor-40-hex> <certified-40-hex> — the #3653
# binding. Sets ANCHOR_ANCESTRY=BOUND on the one passing verdict; every other
# outcome refuses. Exit codes are captured WITHOUT tripping `set -e`.
assert_anchor_on_history() {
  local anchor="$1" head="$2" rc=0 shallow=""

  if ! command -v git >/dev/null 2>&1; then
    refuse_anchor_unverifiable "$anchor" "$head" "git is not on PATH" \
      "REMEDY: install git, or run this assert from a box that has it."
  fi
  # Resolve the bound BEFORE the first read, so no read is ever unbounded by
  # accident rather than by the measured absence of a runner.
  _anchor_resolve_bound "$anchor" "$head"
  # STATUS IS INSPECTED, NOT DISCARDED (job 374). An `if !` collapses "timed out"
  # onto "not a work tree", which is the two-valued-predicate-over-a-multi-state-
  # signal shape — except here the permissive branch is not the danger, a WRONG
  # REMEDY is.
  rc=0
  _anchor_lane rev-parse --git-dir >/dev/null 2>&1 || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "the git work-tree probe (rev-parse --git-dir)" \
      "this checkout (the current working directory's repository)"
  fi
  if [ "$rc" -ne 0 ]; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "the current directory is not inside a git work tree (cwd: $(pwd))" \
      "REMEDY: run this assert from the ISSUE'S WORKTREE. The repository whose" \
      "history the anchor must lie on is the CURRENT DIRECTORY's, and there is" \
      "deliberately no env override naming a different one (#3312)."
  fi
  # THE ISOLATED REPOSITORY IS BUILT BEFORE ANY OBJECT IS READ, and a failure to
  # build it is UNVERIFIABLE — never a fall-back to the live repository, which is
  # the whole hole job 355 closes.
  if ! _anchor_build_scratch "$anchor" "$head"; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "the isolated scratch repository could not be built: $ANCHOR_SCRATCH_ERR" \
      "The ancestry walk deliberately does NOT run in this checkout: a graft in" \
      "\$GIT_DIR/info/grafts rewrites parentage and survives --no-replace-objects" \
      "(#3544 job 285), so a walk here could report a FOREIGN anchor as BOUND." \
      "Falling back to the live repository would restore exactly that hole, so an" \
      "unbuildable scratch is UNMEASURED instead." \
      "REMEDY: give this process a usable TMPDIR outside the work tree and outside" \
      "the git common directory, then re-run this assert."
  fi

  # THE PRESENCE PROBE IS A DIAGNOSTIC REFINEMENT, NOT THE SOUNDNESS BOUNDARY.
  # It exists to name WHICH object is absent, because the two have different
  # remedies in practice (an anchor from a rebased-away branch vs a certified
  # head that was never fetched). Since job 355 it runs in the SCRATCH, which
  # has no config and therefore no promisor remote — so the #3544 job-268
  # measurement that made this probe unsound in the LANE ("cat-file -e cannot
  # even probe presence": with `GIT_NO_LAZY_FETCH=1` set it answered 0 for an
  # object whose `show` then FAILED, because it answers about PROMISED objects)
  # no longer has a mechanism to fire through here: an object promised but absent
  # in the lane is simply ABSENT to a config-less alternate.
  # That last sentence is REASONING, not a measurement, which is why the probe
  # is still not load-bearing. What is load-bearing, in every case: `merge-base`
  # cannot SUCCEED on an object that is not really available, so it exits >= 2
  # (or 1 in a repository this check then refuses to call complete) and the
  # verdict is UNVERIFIABLE either way. A false "present" costs a less specific
  # diagnostic, never a pass.
  rc=0
  _anchor_git cat-file -e "$anchor^{commit}" >/dev/null 2>&1 || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "reading the ANCHOR object" \
      "the SHARED object store, read through the scratch alternate"
  fi
  if [ "$rc" -ne 0 ]; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "the ANCHOR commit is not present in this repository" \
      "An absent object cannot be shown to lie on any history, and its absence is" \
      "not evidence that it does not (it may simply never have been fetched)." \
      "REMEDY: fetch the branch that carries it (git fetch origin <branch>) and" \
      "re-run this assert."
  fi
  rc=0
  _anchor_git cat-file -e "$head^{commit}" >/dev/null 2>&1 || rc=$?
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "reading the CERTIFIED object" \
      "the SHARED object store, read through the scratch alternate"
  fi
  if [ "$rc" -ne 0 ]; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "the CERTIFIED commit is not present in this repository" \
      "REMEDY: fetch the PR branch (git fetch origin <branch>) and re-run this" \
      "assert from a checkout that holds the sha being merged."
  fi

  rc=0
  _anchor_git merge-base --is-ancestor "$anchor" "$head" >/dev/null 2>&1 || rc=$?
  if [ "$rc" -eq 0 ]; then
    ANCHOR_ANCESTRY=BOUND
    _anchor_cleanup
    return 0
  fi
  if _anchor_timed_out "$rc"; then
    _anchor_refuse_timeout "$anchor" "$head" "the ancestry walk" \
      "the SHARED object store, read through the scratch alternate"
  fi
  if [ "$rc" -ne 1 ]; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "git merge-base --is-ancestor exited $rc — an ERROR, not an answer" \
      "Only 0 (ancestor) and 1 (not, or history absent) are answers; anything else" \
      "is git reporting that it could not decide." \
      "REMEDY: run that command by hand to see git's own diagnostic, fix the" \
      "checkout, then re-run this assert."
  fi

  # rc 1 is itself THREE-valued (#3544). It is a VERDICT only in a repository
  # proven complete; `--is-shallow-repository` must answer the literal `false`.
  # An empty answer (a git too old to know the option, or a failed call) is
  # UNMEASURED and takes the same refusing branch as `true` — the permissive
  # branch is never the default for an unestablished state.
  #
  # PROBED IN THE **LANE**, NOT THE SCRATCH, AND THAT IS NOT AN OVERSIGHT.
  # Shallowness is a property of the repository that HOLDS the history; a freshly
  # `git init`-ed scratch is never shallow, so asking it would answer `false`
  # unconditionally and turn this guard into a vacuous pass — the exact shape
  # this whole check exists to refuse. It is a STATE probe, not an object read,
  # so the graft route job 355 closed does not apply to it.
  rc=0
  shallow=$(_anchor_lane rev-parse --is-shallow-repository 2>/dev/null) || rc=$?
  if _anchor_timed_out "$rc"; then
    # Without this a timeout HERE would be reported as "this repository is not
    # proven complete", i.e. it would borrow the shallow cause's remedy
    # (`git fetch --unshallow`) for a stalled mount (job 374).
    _anchor_refuse_timeout "$anchor" "$head" "the shallowness probe (rev-parse --is-shallow-repository)" \
      "this checkout (the current working directory's repository)"
  fi
  # A non-timeout failure (an old git that does not know the option) legitimately
  # keeps the "not proven complete" cause below — that IS the unmeasured state.
  [ "$rc" -eq 0 ] || shallow=""
  if [ "$shallow" != false ]; then
    refuse_anchor_unverifiable "$anchor" "$head" \
      "--is-ancestor said 'no', but this repository is NOT PROVEN COMPLETE (git rev-parse --is-shallow-repository = '$shallow')" \
      "In a SHALLOW clone rc 1 ALSO means 'the connecting history is absent'" \
      "(#3544), so 'no' is a verdict only where the history is complete. An answer" \
      "that is not the literal 'false' — 'true', empty, or a git too old to answer" \
      "— is UNMEASURED, and an unmeasured ancestry is never read as a pass." \
      "REMEDY: git fetch --unshallow (or fetch the missing history), then re-run."
  fi

  refuse_no_gate \
    "The delta block's 'delta-anchor:' sha is NOT on the certified sha's history." \
    "delta anchor:  $anchor" \
    "certified sha: $head" \
    "git merge-base --is-ancestor <anchor> <certified> answers NO, in a repository" \
    "proven complete: the anchor is not on THIS PR's history. Case B's anchor" \
    "identity otherwise rests on the delta run's SELF-DECLARED 'delta-anchor:'" \
    "line, so an unbound anchor lets ANY full-gate PASS anchor THIS merge — the" \
    "#3616 cross-lane class (a peer lane's real, valid summary) surviving in the" \
    "one path Case A's sha binding does not cover (#3653)." \
    "REMEDY: re-run 'scripts/agent-gate.sh --delta <anchor>' with an anchor that IS" \
    "on this PR's history, and pass THAT pair — never a foreign full-gate summary."
}

# ---------------------------------------------------------------------------
# THE BASE-STALENESS ADVISORY (#3650 slice 1) — INFORMATION, NEVER A VERDICT
# ---------------------------------------------------------------------------
# Resolved from THIS script's own directory, with NO env override and no
# `${...:-...}` fallback: #3312's second rule is that the constrained party must
# not choose its own enforcer, and "which paths stale my certification" is
# exactly what a lane wanting to skip a re-gate would redirect. A test needing a
# different advisory substitutes the ARTIFACT in a scratch copy of the tree.
#
# NOTHING here may alter this script's exit code. Every failure mode — absent,
# not executable, non-zero, empty output, UNMEASURED — is REPORTED on a
# `PREMERGE: ADVISORY` line and then ignored. That is slice 1's whole contract:
# an enforcement built on an information source nobody has read yet would be the
# vacuous-pass shape one level up.
#
# IT MEASURES THE CERTIFIED SHA, NOT THIS CHECKOUT'S HEAD (#3650 review F1)
# ------------------------------------------------------------------------
# The advisory is invoked with `"$certified"` EXPLICITLY. Invoked with no rev it
# defaults to `HEAD`, which is the LOCAL CHECKOUT's head — and the whole point of
# the surrounding assert is that the local head and the sha being approved can
# differ (a foreign push, a stale un-pushed rebase). A report about a DIFFERENT
# diff than the one being merged is the "satisfied and wrong" shape this issue
# exists to remove, and slice 2 will CONSUME this report. If the certified commit
# is not present in this checkout the advisory reports UNMEASURED — correct, and
# non-fatal here by the paragraph above.
# Resolved WITHOUT letting `set -e` kill the run: this executes before argument
# validation, so an unreadable script directory would exit 1 — a code outside the
# documented 0/2/3 set, from a line that only feeds a NON-BLOCKING advisory. An
# unresolvable directory degrades to the ABSENT branch below, which is reported
# and not fatal, exactly like a deleted advisory.
self_dir=""
if ! self_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"; then
  self_dir=""
fi
if [ -n "$self_dir" ]; then
  advisory_script="$self_dir/base-staleness.sh"
else
  advisory_script="<unresolvable script directory>/base-staleness.sh"
fi

# The advisory is BOUNDED (60s). It sits on the merge critical path, its cost
# grows with how far the base is behind, and an unbounded child of the merge
# gate is a hang the closer cannot distinguish from a slow gh call. A timeout is
# just another non-zero exit here: REPORTED on an ADVISORY line (`exit 124`) and
# ignored, per the paragraph above.
#
# AN UNAVAILABLE BOUND SKIPS THE ADVISORY — IT DOES NOT DEGRADE TO AN UNBOUNDED
# CALL (#3650 review B1). `timeout` is not POSIX and is absent on a stock macOS,
# which this repo supports, and the earlier code took the UNBOUNDED branch there
# and said so in this comment as though it were a considered trade. It was not:
# an unbounded child on the MERGE CRITICAL PATH is precisely the hang the bound
# exists to prevent, and a rationale written down is what stops the next reader
# questioning it. Skipping keeps BOTH invariants — the bound is never silently
# dropped, and the advisory still cannot touch this script's exit code, because
# the unavailability is REPORTED on a `PREMERGE: ADVISORY` line naming the
# missing mechanism, exactly like an absent artifact.
#
# TWO THINGS THE FIRST VERSION OF THAT BOUND GOT WRONG (#3650 review R1/R2):
#
#  R1 — `timeout <secs>` SENDS SIGTERM AND THEN WAITS, so a child that traps or
#  ignores TERM runs on indefinitely and the advertised bound bounds NOTHING.
#  The escalation `--kill-after=<grace>` follows with SIGKILL, which cannot be
#  trapped, and it IS the bound. This is the same finding, with the same
#  measurement, that scripts/lib/gate-notify.sh records for the gate's notify
#  path and scripts/bootstrap-agent-machine.sh for its network probes.
#
#  R2 — only `timeout` was resolved, while GNU coreutils installs its timeout as
#  `gtimeout` on stock macOS. The skip diagnostic below TOLD the reader to
#  install coreutils for `gtimeout`, and the code then never looked for it: on
#  the exact configuration the message recommends, the advisory still skipped.
#
# So the resolution follows the repository's existing convention verbatim
# (`_gate_notify_bounded_timeout`, scripts/lib/gate-notify.sh): try `timeout`
# then `gtimeout`, PROBE each for `--kill-after` rather than assuming it (BusyBox
# and older implementations reject the flag, and a non-GNU `timeout` earlier on
# PATH must not win a first-match-wins lookup), and treat a candidate that
# rejects it as NO bounding tool at all. That last part is the B1 rule applied
# one level down: an escapable bound is not a bound, and running behind one
# would be the silent degrade B1 forbids.
ADVISORY_TIMEOUT_SECS=60
# The grace is ADDITIVE wall-clock: the true worst case of the advisory call is
# ADVISORY_TIMEOUT_SECS + ADVISORY_KILL_GRACE. 5s is ample for a well-behaved
# child to finish its own cleanup after TERM.
ADVISORY_KILL_GRACE=5

# resolve_advisory_timeout — print a timeout(1) that supports `--kill-after`, or
# return 1 when no such runner exists. Capability is PROBED, never assumed; see
# the R1/R2 paragraphs above.
resolve_advisory_timeout() {
  local c
  for c in timeout gtimeout; do
    command -v "$c" >/dev/null 2>&1 || continue
    "$c" --kill-after=1 1 true >/dev/null 2>&1 || continue
    printf '%s\n' "$c"
    return 0
  done
  return 1
}

print_base_staleness_advisory() {
  local adv_out adv_rc=0 line adv_to
  if [ ! -f "$advisory_script" ]; then
    printf 'PREMERGE: ADVISORY base-staleness.sh is ABSENT at %s — the base-staleness\n' \
      "$advisory_script"
    printf 'PREMERGE: ADVISORY report could not be produced. NOT fatal in #3650 slice 1: the\n'
    printf 'PREMERGE: ADVISORY advisory changes no verdict, so its absence changes none either.\n'
    return 0
  fi
  if ! adv_to=$(resolve_advisory_timeout); then
    printf 'PREMERGE: ADVISORY base-staleness.sh was NOT RUN: no `timeout`/`gtimeout` on PATH\n'
    printf 'PREMERGE: ADVISORY supporting `--kill-after`, so the %ss bound could not be applied\n' \
      "$ADVISORY_TIMEOUT_SECS"
    printf 'PREMERGE: ADVISORY with a SIGKILL escalation, and the bound is not droppable (#3650\n'
    printf 'PREMERGE: ADVISORY review B1/R1) — an UNBOUNDED child here, or one behind a\n'
    printf 'PREMERGE: ADVISORY SIGTERM-only bound a child can ignore, is the merge-path hang the\n'
    printf 'PREMERGE: ADVISORY bound exists to prevent, so the advisory is SKIPPED.\n'
    printf 'PREMERGE: ADVISORY NOT fatal in #3650 slice 1: the advisory changes no verdict, so\n'
    printf 'PREMERGE: ADVISORY its absence changes none either. Install GNU coreutils (its\n'
    printf 'PREMERGE: ADVISORY timeout is `gtimeout` on macOS, and IS accepted here) to get the\n'
    printf 'PREMERGE: ADVISORY report back.\n'
    return 0
  fi
  adv_out=$("$adv_to" --kill-after="$ADVISORY_KILL_GRACE" "$ADVISORY_TIMEOUT_SECS" \
    bash "$advisory_script" "$certified" 2>&1) || adv_rc=$?
  if [ -z "$adv_out" ]; then
    printf 'PREMERGE: ADVISORY base-staleness.sh produced NO output (exit %s) — reported, and\n' \
      "$adv_rc"
    printf 'PREMERGE: ADVISORY not fatal in #3650 slice 1.\n'
    return 0
  fi
  while IFS= read -r line; do
    printf 'PREMERGE: ADVISORY %s\n' "$line"
  done <<EOF
$adv_out
EOF
  printf 'PREMERGE: ADVISORY exit %s — advisory ONLY (#3650 slice 1): it did NOT affect this\n' \
    "$adv_rc"
  printf 'PREMERGE: ADVISORY assert. A CONSUMER of the advisory (slice 2) must treat exit 5 /\n'
  printf 'PREMERGE: ADVISORY UNMEASURED as STALE, never as fresh.\n'
  return 0
}

# assert_readable_summary <file> <what> — the three file-level preconditions.
assert_readable_summary() {
  if [ ! -f "$1" ]; then
    refuse_no_gate "The $2 file does not exist (or is not a regular file)."
  fi
  if [ ! -r "$1" ]; then
    refuse_no_gate "The $2 file exists but is not readable."
  fi
  if [ ! -s "$1" ]; then
    refuse_no_gate "The $2 file is EMPTY — nothing was certified."
  fi
}

# Parse the summary by REDIRECTION, never a pipe (#3400: a piped `while read`
# runs in a subshell and its verdict is discarded). One awk pass:
#   * strips ANSI escapes and a trailing CR before matching anything (belt — see
#     the header: the summary file's own block lines are not coloured)
#   * counts blocks by WHOLE-LINE-EXACT marker equality, never substring. That
#     anchoring defends against (a) PROSE copies of a marker — indented,
#     `>`-quoted, fenced, or mid-sentence — which CLAUDE.md, issue bodies, PR
#     comments and the very doctrine files this change edits all contain, and
#     (b) a TRUNCATED pattern such as `AGENT-GATE SUMMARY ====`, which matches
#     ALL FOUR markers (full/lite start and end). Note the end marker does NOT
#     contain the start marker as a substring — `END ` sits between `====` and
#     `AGENT-GATE` — so substring matching would fail for the reasons above,
#     not for that one.
#   * counts all three block families so a refusal can NAME what it found
#     (the headers are distinct by construction: scripts/agent-gate.sh)
#   * emits key=value lines with per-key occurrence COUNTS, so a duplicated key
#     inside one block is refusable rather than silently last-wins
# WANT selects which family is "the block": full (default) or delta.
_gate_awk() {
  awk -v WANT="$2" '
  BEGIN {
    FULL_S  = "==== AGENT-GATE SUMMARY ===="
    FULL_E  = "==== END AGENT-GATE SUMMARY ===="
    LITE_S  = "==== AGENT-GATE LITE SUMMARY ===="
    DELTA_S = "==== AGENT-GATE DELTA SUMMARY ===="
    DELTA_E = "==== END AGENT-GATE DELTA SUMMARY ===="
    if (WANT == "delta") { S = DELTA_S; E = DELTA_E } else { S = FULL_S; E = FULL_E }
    blocks = 0; full = 0; lite = 0; delta = 0; open = 0; unterminated = 0
    n_result = 0; n_ti = 0; n_commit = 0; n_ts = 0; n_mode = 0
    n_anchor = 0; n_nested = 0; anchor_unresolved = 0; n_dirty = 0; n_tsdirty = 0
    v_result = ""; v_ti = ""; v_commit = ""; v_ts = ""; v_dirty = ""
    v_mode = ""; v_anchor = ""
  }
  {
    gsub(/\033\[[0-9;]*[a-zA-Z]/, "")
    sub(/\r$/, "")
  }
  $0 == FULL_S  { full++;  if (S == FULL_S)  { blocks++; if (open == 1) unterminated = 1; open = 1 } next }
  $0 == DELTA_S { delta++; if (S == DELTA_S) { blocks++; if (open == 1) unterminated = 1; open = 1 } next }
  $0 == LITE_S  { lite++;  next }
  $0 == E       { if (open == 1) open = 0; next }
  open == 1 {
    if ($1 == "MODE:")                { n_mode++;   v_mode = $2 }
    else if ($1 == "RESULT:")         { n_result++; v_result = $2 }
    else if ($1 == "tree-integrity:") { n_ti++;     v_ti = $2 }
    else if ($1 == "tree-start:") {
      n_ts++; v_ts = $2
      # tree-start: carries its OWN `dirty:`, and it is NOT redundant with the
      # commit: one: commit: renders TREE_END_DIRTY on the normal path
      # (agent-gate.sh:8810), so a run that STARTED dirty and finished clean --
      # legal under the non-fatal `tree-integrity: PASS (lockfile-settled: ...)`
      # class (agent-gate.sh:8754) -- shows `commit: ... dirty: no` while having
      # executed against uncommitted content (#3648 roborev round 4).
      for (i = 2; i <= NF; i++) if ($i == "dirty:") {
        n_tsdirty++
        if (n_tsdirty == 1 && i < NF) v_tsdirty = $(i + 1)
      }
    }
    else if ($1 == "nested-under:")   { n_nested++ }
    else if ($1 == "delta-anchor:") {
      n_anchor++; v_anchor = $2
      for (i = 2; i <= NF; i++) if ($i == "(UNRESOLVED)") anchor_unresolved = 1
    }
    else if ($1 == "commit:") {
      n_commit++; v_commit = $2
      # COUNT every `dirty:` token and keep the FIRST value, never the last.
      # The old loop assigned on every match, so `dirty: yes dirty: no` reduced
      # to `no` and certified a dirty run (#3648 roborev round 2). That is the
      # "last one wins" rule assert_single_key exists to refuse, one field down.
      # Scanned to NF (not NF-1) so a BARE trailing `dirty:` still COUNTS as an
      # occurrence; its value stays empty and refuses on the empty-value path.
      for (i = 2; i <= NF; i++) if ($i == "dirty:") {
        n_dirty++
        if (n_dirty == 1 && i < NF) v_dirty = $(i + 1)
      }
    }
    next
  }
  END {
    if (open == 1) unterminated = 1
    print "blocks=" blocks
    print "full=" full
    print "lite=" lite
    print "delta=" delta
    print "unterminated=" unterminated
    print "n_mode=" n_mode
    print "n_result=" n_result
    print "n_ti=" n_ti
    print "n_commit=" n_commit
    print "n_ts=" n_ts
    print "n_anchor=" n_anchor
    print "n_nested=" n_nested
    print "n_dirty=" n_dirty
    print "n_tsdirty=" n_tsdirty
    print "anchor_unresolved=" anchor_unresolved
    print "v_result=" v_result
    print "v_ti=" v_ti
    print "v_commit=" v_commit
    print "v_ts=" v_ts
    print "v_dirty=" v_dirty
    print "v_tsdirty=" v_tsdirty
    print "v_mode=" v_mode
    print "v_anchor=" v_anchor
  }
' <"$1"
}

# gate_parse_file <file> <want> <what> — run the parse and publish its fields as
# GP_* globals (bash 3.2: no namerefs, no associative arrays). Every COUNT is
# validated as a non-negative integer here, keyed on its AFFIRMATIVE value: an
# unparseable/absent count is refused, never treated as "no problem found".
gate_parse_file() {
  local gp_out gp_k gp_v
  gp_out=$(_gate_awk "$1" "$2") || refuse_tool_failure awk "$3"
  GP_blocks=""; GP_full=""; GP_lite=""; GP_delta=""; GP_unterminated=""
  GP_n_mode=""; GP_n_result=""; GP_n_ti=""; GP_n_commit=""; GP_n_ts=""
  GP_n_anchor=""; GP_n_nested=""; GP_anchor_unresolved=""; GP_n_dirty=""; GP_n_tsdirty=""
  GP_v_result=""; GP_v_ti=""; GP_v_commit=""; GP_v_ts=""; GP_v_dirty=""
  GP_v_mode=""; GP_v_anchor=""; GP_v_tsdirty=""
  while IFS='=' read -r gp_k gp_v; do
    case "$gp_k" in
      blocks)       GP_blocks="$gp_v" ;;
      full)         GP_full="$gp_v" ;;
      lite)         GP_lite="$gp_v" ;;
      delta)        GP_delta="$gp_v" ;;
      unterminated) GP_unterminated="$gp_v" ;;
      n_mode)       GP_n_mode="$gp_v" ;;
      n_result)     GP_n_result="$gp_v" ;;
      n_ti)         GP_n_ti="$gp_v" ;;
      n_commit)     GP_n_commit="$gp_v" ;;
      n_ts)         GP_n_ts="$gp_v" ;;
      n_anchor)     GP_n_anchor="$gp_v" ;;
      n_nested)     GP_n_nested="$gp_v" ;;
      n_dirty)      GP_n_dirty="$gp_v" ;;
      n_tsdirty)    GP_n_tsdirty="$gp_v" ;;
      anchor_unresolved) GP_anchor_unresolved="$gp_v" ;;
      v_result)     GP_v_result="$gp_v" ;;
      v_ti)         GP_v_ti="$gp_v" ;;
      v_commit)     GP_v_commit="$gp_v" ;;
      v_ts)         GP_v_ts="$gp_v" ;;
      v_dirty)      GP_v_dirty="$gp_v" ;;
      v_tsdirty)    GP_v_tsdirty="$gp_v" ;;
      v_mode)       GP_v_mode="$gp_v" ;;
      v_anchor)     GP_v_anchor="$gp_v" ;;
    esac
  done <<GATE_PARSE
$gp_out
GATE_PARSE
  for gp_k in blocks full lite delta unterminated n_mode n_result n_ti n_commit \
              n_ts n_anchor n_nested anchor_unresolved n_dirty n_tsdirty; do
    eval "gp_v=\${GP_$gp_k}"
    case "$gp_v" in
      ''|*[!0-9]*)
        refuse_no_gate "Gate summary parse produced no usable '$gp_k' count for the $3 — refusing (fail closed)."
        ;;
    esac
  done
}

# assert_single_key <count> <label> <what>: the key must appear EXACTLY once.
# Zero certifies nothing; more than one is ambiguous and a "last one wins" rule
# would let a doctored line override the real verdict. Asserted per key,
# immediately before that key is USED, so the diagnostic names the first thing
# that is wrong — e.g. the #3041 launch sentinel (a FULL-header block carrying
# `tree-start:` and `RESULT: INCOMPLETE`, with no `tree-integrity:`/`commit:`
# yet) is reported as the INCOMPLETE verdict it is, not as a missing
# tree-integrity line.
assert_single_key() {
  if [ "$1" -eq 0 ]; then
    refuse_no_gate "The $3 has no '$2:' line — it cannot certify anything."
  fi
  if [ "$1" -gt 1 ]; then
    refuse_no_gate "The $3 has $1 '$2:' lines — AMBIGUOUS, refusing."
  fi
}

# assert_hex_abbrev <label> <value> <what>: the value must be a lowercase-hex
# abbreviation of SOME sha. A non-hex value ("(not captured)", "(capture
# unavailable — no git worktree)", "selftest", "unverified") REFUSES — it is
# never skipped.
assert_hex_abbrev() {
  local n
  case "$2" in
    ''|*[!0-9a-f]*)
      refuse_no_gate \
        "'$1:' value '$2' in the $3 is not lowercase hex — nothing verifiable was recorded." \
        "The gate writes a non-hex placeholder when its capture failed or there was no" \
        "git worktree; such a run proves nothing about which tree it executed against."
      ;;
  esac
  n=${#2}
  # FLOOR 7, not 4 (#3465 review nit 5): 7 is the NARROWEST abbreviation the gate
  # ever emits (`commit:` is `printf '%.7s'`; `tree-start:` is `%.12s`), and a
  # 4-hex value accepted at its own width is a 1-in-65536 accidental cross-lane
  # match — precisely the #3616 class this compare exists to refuse. Accepting a
  # width the gate cannot produce buys nothing and weakens the binding.
  if [ "$n" -lt 7 ] || [ "$n" -gt 40 ]; then
    refuse_no_gate \
      "'$1:' value '$2' in the $3 is $n hex chars — outside the 7..40 range." \
      "The gate emits 7 (commit:) and 12 (tree-start:) hex; a narrower value cannot" \
      "bind a run to a tree (a 4-hex 'match' is 1-in-65536 by accident)."
  fi
}

# assert_covers <label> <value> <full-40-sha> <what> <subject>: the abbreviation
# must be a prefix of the full sha AT ITS OWN EXACT WIDTH.
#
# `commit:` carries a 7-char abbreviation and `tree-start:` a 12-char one (both
# `printf '%.Ns'` of the same VERIFIED capture in scripts/agent-gate.sh), so
# "matches the certified sha" cannot be string equality against the 40-hex sha.
# Compare each value at ITS OWN width, using the value's own length — never a
# glob, never `case $x in $y*)`, never a fixed assumed width. BOTH must match:
# two independent widths off one verified capture is materially stronger than one
# 7-hex compare — and this pair is what refuses the #3616 cross-lane class (a
# peer lane's perfectly valid summary, recovered by recency, naming a DIFFERENT
# PR's head).
assert_covers() {
  local label="$1" val="$2" full="$3" what="$4" subject="$5" n
  assert_hex_abbrev "$label" "$val" "$what"
  n=${#val}
  if [ "${full:0:n}" != "$val" ]; then
    refuse_no_gate \
      "'$label:' value '$val' in the $what does not match the $subject at $n chars." \
      "$subject: $full" \
      "That run executed against a DIFFERENT tree than the one it must cover." \
      "If the only diff since the gate's anchor is test/docs-only, the route is the" \
      "ANCHORED DELTA PAIR below — a fourth argument, not a repeat full gate."
  fi
}

# assert_clean_tree <what> <value>: the run that block records must have executed
# against a COMMITTED tree (#3648).
#
# WHY THIS IS ENFORCED AND NOT MERELY REPORTED. A gate that ran with `dirty: yes`
# certified sha PLUS uncommitted non-ignored content — not the sha. The gate's tree
# capture pairs a tracked-side diff with `git ls-files --others --exclude-standard`,
# so `dirty: yes` is real uncommitted NON-IGNORED content — tracked edits AND/OR
# untracked files the ignore rules do not exclude — never a gitignored log. The escape is then ordinary: a full gate PASSes
# at HEAD X with edits in the worktree, the edits are discarded or simply never
# committed, X is pushed and merged — and the gate of record covered a tree that
# is NOT the one that lands. `commit:`/`tree-start:` cannot see it: both name X in
# exactly that run.
#
# HOW OFTEN THIS FIRES — MEASURED, WITH ITS POPULATION AND ITS LIMITS (2026-09-01,
# #3648). Census of one box's `/tmp/agent-gate.*` summaries, restricted to blocks
# that could ever BE a gate of record: FULL-gate blocks, deduplicated by `run-id`,
# NOT `nested-under:`, and carrying a canonical `RESULT` token — n=2395, of which
# 1608 are `RESULT: PASS`. Of those 1608 PASSes an affirmative `dirty: no` match
# refuses 26 (~1.6%), broken down by cause so the figure can be re-derived rather
# than inherited: 19 `dirty: yes`, 7 carrying NO `dirty:` field at all, and 0
# `unverified` (all 40 `unverified` blocks in the population are already FAIL).
# So the absent-field arm below is not hypothetical — it is 7 of the 26.
# LIMITS, stated because a number in a comment decays like any other claim: this
# is a SINGLE-BOX `/tmp` census over run dirs of unknown age, blind to runs that
# were pruned, and the fixture exclusion (canonical `RESULT` + no `nested-under:`)
# is a heuristic that removes this repo's own planted near-miss summaries, not a
# guarantee that none survive. Percentages taken over the UNfiltered population
# are unstable for exactly that reason; the absolute counts are not.
#
# The compare is AFFIRMATIVE — `= no`, never `!= yes` — for the same reason the
# RESULT/tree-integrity token compares are: a `!= yes` test is a two-valued
# predicate over a multi-state signal and would hand every unmeasured state
# (`unverified`, an absent field, a future value) the PERMISSIVE branch. An absent
# or unrecognised value therefore REFUSES; it is never skipped and never read as
# clean, exactly as a non-hex `commit:`/`tree-start:` placeholder refuses rather
# than being skipped.
#
# `dirty: unverified` IS A REAL EMITTED VALUE, AND THIS ARM IS DEFENCE IN DEPTH —
# NOT A HOLE THIS CHANGE CLOSES. scripts/agent-gate.sh:8814 emits
# `commit: unverified branch: <b> dirty: unverified` deliberately, when no
# validated tree capture exists (the start capture failed, or there is no worktree
# at the terminal emit) — the run is ALREADY fail-closed there and must not name a
# sha nothing verified. Such a block is therefore refused THREE times over: by
# `RESULT: FAIL`, by the non-hex `commit:` placeholder, and now here. The
# redundancy is deliberate: each of those three is a separate key, and a value
# that means "the state was never measured" must not survive on the strength of
# one neighbouring check (the standing rule that no key may delegate its failure
# to its neighbour).
#
# THERE IS NO ENV OPT-OUT AND NONE MAY BE ADDED. A dirty tree is always
# re-gateable — commit or discard, then re-run — so an escape hatch could only
# buy a vacuous green, which is the shape this whole script exists to refuse.
assert_clean_tree() {
  local what="$1" val="$2" kind="$3" line="${5:?assert_clean_tree: line label required}"
  # The REMEDY is per-artifact, because the two artifacts are re-produced by
  # DIFFERENT runs (#3648 roborev round 1, finding 1). Telling the operator to
  # "re-run the FULL gate" over a dirty DELTA block contradicts #1892, which
  # mandates `--delta` — never a repeat full gate — for a test/docs-only diff on
  # top of a full PASS. A refusal naming the wrong remedy sends a correct operator
  # down a route doctrine forbids, which is worse than naming none.
  #
  # `kind` is REQUIRED and an unrecognised value REFUSES. It deliberately takes no
  # `${3:-full}` default: a permissive default is how a new call site silently
  # inherits the wrong remedy, and this file's whole discipline is that an
  # unestablished value is never given the benign branch.
  local rerun
  case "$kind" in
    full)  rerun="re-run the FULL gate on the clean tree and pass that summary" ;;
    delta) rerun="re-run the --delta re-certification on the clean tree and pass that summary (the anchor's own full-gate PASS is unaffected)" ;;
    *)
      refuse_no_gate \
        "INTERNAL: assert_clean_tree was called with remedy kind '$kind', which is not" \
        "'full' or 'delta'. Refusing rather than guessing a remedy (#3648)."
      ;;
  esac
  # AMBIGUITY BEFORE VALUE: more than one `dirty:` on the commit: line means the
  # block states the tree's cleanliness twice, and no reading of it is
  # authoritative. Refused BEFORE the `= no` compare, or `dirty: yes dirty: no`
  # would return 0 here on the second token's value (#3648 roborev round 2).
  if [ "${4:-1}" -gt 1 ] 2>/dev/null; then
    refuse_no_gate \
      "The $what has ${4} 'dirty:' fields on its '$line' line — AMBIGUOUS, refusing." \
      "A block that states its tree state twice authorises nothing: a 'last one wins'" \
      "reading would let a trailing 'dirty: no' override the real value."
  fi
  if [ "$val" = no ]; then
    return 0
  fi
  if [ -z "$val" ]; then
    refuse_no_gate \
      "The $what records NO 'dirty:' value on its '$line' line — nothing was measured" \
      "about whether that run executed against a committed tree, so it cannot certify one." \
      "REMEDY: $rerun."
  fi
  refuse_no_gate \
    "The $what records 'dirty: $val' — the gate of record must be 'dirty: no' (#3648)." \
    "'yes' means that run certified the sha PLUS uncommitted NON-IGNORED content — both" \
    "modified TRACKED files and UNTRACKED files the repo's ignore rules do not exclude," \
    "since the gate's capture pairs a tracked-side diff with" \
    "\`git ls-files --others --exclude-standard\` (so this is never a gitignored log, and it" \
    "is not tracked-only either). Anything that is" \
    "neither 'yes' nor 'no' means the state was never established — and an unestablished" \
    "state is not a clean one. Either way the tree that was gated is not provably the tree" \
    "that will merge: commit:/tree-start: name the same sha in both cases and cannot see it." \
    "REMEDY: commit the edits (or discard them), then $rerun." \
    "There is deliberately NO opt-out: a dirty tree is always" \
    "re-gateable, so an override could only buy a vacuous green."
}

# assert_pass_block <what>: the verdict half every accepted block must satisfy —
# terminated, RESULT: PASS, tree-integrity: PASS, and not a nested sub-gate.
assert_pass_block() {
  local what="$1"
  if [ "$GP_unterminated" != 0 ]; then
    refuse_no_gate \
      "A block in the $what is UNTERMINATED (no exact end marker)." \
      "A truncated summary certifies nothing — the gate may still be running or have died."
  fi

  assert_single_key "$GP_n_result" RESULT "$what"
  # Verdict TOKENS are compared EXACTLY, never by prefix (#3229): a `PASS*` glob
  # accepts `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. it would
  # check a SPELLING rather than a STATE. awk already gave us the first
  # whitespace-delimited token after the key, so this is a token-exact compare.
  if [ "$GP_v_result" != PASS ]; then
    refuse_no_gate \
      "RESULT verdict token in the $what is '$GP_v_result', not PASS." \
      "INCOMPLETE is the launch-time liveness SENTINEL, not a verdict (#3041): it is" \
      "written when the gate starts (before the slot is even granted) and overwritten" \
      "only at the terminal emit. Such a summary means still running, queued, or died." \
      "PARTIAL is an --only run, which does NOT count as the gate."
  fi

  assert_single_key "$GP_n_ti" tree-integrity "$what"
  if [ "$GP_v_ti" != PASS ]; then
    refuse_no_gate \
      "tree-integrity verdict token in the $what is '$GP_v_ti', not PASS." \
      "A run whose worktree mutated mid-run cannot certify (#2926); PENDING means the" \
      "run never reached its terminal emit, and SKIP means the check never ran."
  fi

  # A NESTED sub-gate (#2874: launched by an enclosing gate, stamped
  # `nested-under: <parent-run-id>`) emits the SAME markers at the SAME tree, so
  # the sha binding provably cannot distinguish it from the real thing — this
  # one affirmative line closes the only wrong-file class the sha compare cannot
  # see. A self-test/sub-gate verdict is about the gate's own machinery, never
  # about this PR.
  if [ "$GP_n_nested" -ne 0 ]; then
    refuse_no_gate \
      "The $what carries a 'nested-under:' line — it is a NESTED sub-gate (#2874)." \
      "A sub-gate spawned by an enclosing gate runs at the SAME tree, so the sha" \
      "binding cannot tell it apart; it certifies the gate's machinery, not this PR."
  fi
}

# --- the FULL gate of record (arg 3) -----------------------------------------
assert_readable_summary "$summary_file" "gate summary"
gate_parse_file "$summary_file" full "gate summary"

if [ "$GP_blocks" -eq 0 ]; then
  refuse_no_gate \
    "The file contains ZERO full-gate blocks (found $GP_lite lite, $GP_delta delta)." \
    "--lite and --delta emit DISTINCT headers; NEITHER is the gate of record:" \
    "  --lite  is fast iteration and is never acceptable here." \
    "  --delta re-certifies a post-full-PASS test/docs-only round — pass the ANCHOR's" \
    "          FULL summary as argument 3 and the delta summary as argument 4." \
    "This is the #3408 failure exactly: many lite PASSes, no full gate."
fi

if [ "$GP_blocks" -gt 1 ]; then
  refuse_no_gate \
    "The file contains $GP_blocks full-gate blocks — AMBIGUOUS." \
    "Refusing rather than picking one (a 'take the last block' rule would let a" \
    "stale or foreign run certify this merge). Point at ONE run's summary file."
fi

# Belt for the header separation above: the FULL gate emits NO `MODE:` line;
# --lite and --delta each emit one naming themselves. (An `--only` run emits the
# FULL markers with a LOWERCASE `mode: PARTIAL (--only …)` line, which this
# case-sensitive check deliberately does NOT catch — that run is refused by the
# `RESULT: PARTIAL` compare above, which is the property that matters.)
if [ "$GP_n_mode" -ne 0 ]; then
  refuse_no_gate \
    "The full-gate block carries a MODE: line — the FULL gate emits none." \
    "This block was produced by (or doctored from) a lite/delta run."
fi

assert_pass_block "full-gate block"

assert_single_key "$GP_n_commit" commit "full-gate block"
assert_single_key "$GP_n_ts" tree-start "full-gate block"
full_commit="$GP_v_commit"
full_ts="$GP_v_ts"
full_dirty="$GP_v_dirty"
full_ndirty="$GP_n_dirty"
full_tsdirty="$GP_v_tsdirty"
full_ntsdirty="$GP_n_tsdirty"

if [ -z "$delta_file" ]; then
  # CASE A — DIRECT: the gate of record ran on the merged tree itself.
  assert_covers commit "$full_commit" "$certified" "full-gate block" "certified sha"
  assert_covers tree-start "$full_ts" "$certified" "full-gate block" "certified sha"
else
  # CASE B — ANCHORED DELTA (#1892). The full block is the ANCHOR: its sha need
  # not be the certified sha, but it must still be a real, verifiable sha, and
  # the delta block must name exactly it.
  assert_hex_abbrev commit "$full_commit" "full-gate block"
  assert_hex_abbrev tree-start "$full_ts" "full-gate block"

  assert_readable_summary "$delta_file" "delta summary"
  gate_parse_file "$delta_file" delta "delta summary"

  if [ "$GP_blocks" -eq 0 ]; then
    refuse_no_gate \
      "The fourth argument holds ZERO delta blocks (found $GP_full full, $GP_lite lite)." \
      "It must be the AGENT_GATE_SUMMARY_FILE of a 'scripts/agent-gate.sh --delta' run" \
      "('==== AGENT-GATE DELTA SUMMARY ====' — a DISTINCT header, by construction)."
  fi
  if [ "$GP_blocks" -gt 1 ]; then
    refuse_no_gate \
      "The fourth argument holds $GP_blocks delta blocks — AMBIGUOUS." \
      "Point at ONE run's summary file; picking one would let a stale run re-certify."
  fi

  # The INVERSE of the full block's belt: here a `MODE: delta` line is REQUIRED
  # and asserted AFFIRMATIVELY. A delta block always carries it
  # (scripts/agent-gate.sh SUMMARY_MODE_LINE), so its absence means the block was
  # doctored or is not what its header claims.
  assert_single_key "$GP_n_mode" MODE "delta block"
  if [ "$GP_v_mode" != delta ]; then
    refuse_no_gate \
      "The delta block's MODE token is '$GP_v_mode', not 'delta'." \
      "A --delta run stamps 'MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION …)'; anything" \
      "else is a different mode wearing the delta header."
  fi

  assert_pass_block "delta block"

  # delta-anchor: must name the FULL block above. The gate emits
  # 'delta-anchor: <40-hex> (full-gate PASS commit)' from `git rev-parse
  # --verify`, and 'delta-anchor: <ref> (UNRESOLVED)' on the ERROR path — the
  # latter MUST refuse (it certifies nothing about any tree).
  assert_single_key "$GP_n_anchor" delta-anchor "delta block"
  if [ "$GP_anchor_unresolved" -ne 0 ]; then
    refuse_no_gate \
      "The delta block's 'delta-anchor:' is (UNRESOLVED) — the anchor did not resolve" \
      "to a commit, so that run re-certified nothing against the gate of record."
  fi
  case "$GP_v_anchor" in
    ''|*[!0-9a-f]*)
      refuse_no_gate \
        "The delta block's 'delta-anchor:' value '$GP_v_anchor' is not lowercase hex."
      ;;
  esac
  if [ "${#GP_v_anchor}" -ne 40 ]; then
    refuse_no_gate \
      "The delta block's 'delta-anchor:' value '$GP_v_anchor' is ${#GP_v_anchor} hex chars," \
      "not the full 40 the gate resolves it to (git rev-parse --verify of the anchor)."
  fi
  delta_anchor="$GP_v_anchor"
  # Both of the anchor block's independent widths must prefix that anchor sha.
  assert_covers commit "$full_commit" "$delta_anchor" "full-gate block" "delta block's anchor sha"
  assert_covers tree-start "$full_ts" "$delta_anchor" "full-gate block" "delta block's anchor sha"

  # ...and the delta run's OWN provenance must cover the tree being merged.
  assert_single_key "$GP_n_commit" commit "delta block"
  assert_single_key "$GP_n_ts" tree-start "delta block"
  assert_covers commit "$GP_v_commit" "$certified" "delta block" "certified sha"
  assert_covers tree-start "$GP_v_ts" "$certified" "delta block" "certified sha"
  delta_commit="$GP_v_commit"
  delta_ts="$GP_v_ts"
  delta_dirty="$GP_v_dirty"
  delta_ndirty="$GP_n_dirty"
  delta_tsdirty="$GP_v_tsdirty"
  delta_ntsdirty="$GP_n_tsdirty"
  # The delta run's OWN tree must be clean too: it is the run that covers the
  # tree being merged, so a dirty delta re-cert certifies edits that are not in
  # the PR exactly as a dirty full gate does.
  assert_clean_tree "delta block" "$delta_dirty" delta "$delta_ndirty" commit:
  assert_clean_tree "delta block" "$delta_tsdirty" delta "$delta_ntsdirty" tree-start:
fi

# `dirty:` is REPORTED **AND ENFORCED** (#3648, replacing the deferral note this
# line used to carry). In CASE B this is the ANCHOR's own tree: a full PASS taken
# on a dirty tree anchors the whole chain on a tree nobody can reconstruct, so
# both blocks are held to the same requirement. The evidence line below still
# prints the value — after this call it can only ever read `dirty: no`, which is
# the affirmative record that the check RAN.
assert_clean_tree "full-gate block" "$full_dirty" full "$full_ndirty" commit:
assert_clean_tree "full-gate block" "$full_tsdirty" full "$full_ntsdirty" tree-start:

# CASE B ONLY (#3653) — the anchor must be on the certified sha's history.
# Placed LAST among the offline gate-of-record checks, and deliberately: every
# cheaper structural refusal above reports FIRST (so the diagnostic still names
# the first thing that is wrong), and running it after the full block's own
# `dirty:` enforcement keeps a dirty anchor reported as dirty rather than as
# unverifiable. It is the only check here that reads a repository.
if [ -n "$delta_file" ]; then
  assert_anchor_on_history "$delta_anchor" "$certified"
fi

# ---------------------------------------------------------------------------
# THE ADVISORY IS MEASURED **BEFORE** THE HEAD CHECK (#3650, roborev job 250)
# ---------------------------------------------------------------------------
# The advisory is bounded at ADVISORY_TIMEOUT_SECS + ADVISORY_KILL_GRACE (65s).
# Running it AFTER the `gh pr view` head/state check would leave up to 65s
# between the instant the head was verified and the instant `PREMERGE: OK` is
# emitted -- so a push inside that window would leave this script emitting OK for
# a sha that is no longer the PR head, which is precisely the stale-head merge
# #2456 exists to refuse. The fix is ordering, not a re-check: the advisory is
# MEASURED here and PRINTED later in its original position, so the gh head/state
# check remains the LAST thing that happens before OK.
#
# Capturing changes no output: every line of the report is written to stdout by
# `print_base_staleness_advisory`, and printing it at the original call site keeps
# the order identical -- which matters, because the `PREMERGE: SCOPE ... ADVISORY
# lines below` clause asserts the advisory appears BELOW it.
#
# Cost, accepted: on a refusal path the 65s is already spent. Correctness of the
# approval beats latency of a refusal, and nothing is printed on those paths.
advisory_out=$(print_base_staleness_advisory)

# ---------------------------------------------------------------------------
# PR HEAD + STATE (#2456)
# ---------------------------------------------------------------------------

# Fetch head + state in ONE call, extracted by gh's built-in jq into two
# whitespace-separated tokens: "<headRefOid> <state>". Because gh runs the jq
# expression, its JSON serialization (compact vs pretty) is irrelevant. On any
# gh/network failure -> exit 3 (fail closed).
if ! out=$(gh pr view "$pr" --repo "$repo" --json headRefOid,state \
  --jq '.headRefOid + " " + .state' 2>/dev/null); then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  gh pr view %s --repo %s failed (auth/network/no-such-PR).\n' "$pr" "$repo" >&2
  printf '  Cannot verify the PR head — refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

# Split the two tokens. Empty or malformed --jq output -> exit 3 (fail closed).
actual=$(printf '%s' "$out" | awk '{print $1}')
state=$(printf '%s' "$out" | awk '{print $2}')
actual=$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')

if [ -z "$actual" ] || [ -z "$state" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  Could not parse headRefOid/state from gh --jq output.\n' >&2
  printf '  Refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

if [ "$state" != "OPEN" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: NOT-OPEN\n' >&2
  printf '  PR #%s state is "%s" (expected OPEN).\n' "$pr" "$state" >&2
  printf '  The PR is already closed or merged — do NOT merge again.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

if [ "$actual" != "$certified" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: STALE-HEAD — REFUSING TO MERGE\n' >&2
  printf '  certified SHA: %s\n' "$certified" >&2
  printf '  actual   head: %s\n' "$actual" >&2
  printf '  head moved since certification — the gate of record no longer\n' >&2
  printf '  covers this PR; re-certify before merge.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

printf 'PREMERGE: OK %s\n' "$certified"
# Scope clause (#3650) — printed on EVERY success so `GATE-OF-RECORD` can never be
# read as "certified against main". See residual 3 in the header.
printf 'PREMERGE: SCOPE this proves a full gate PASSed on THIS tree (%s); it does NOT prove\n' \
  "$certified"
printf 'PREMERGE: SCOPE the tree was certified against current main (#3650) — a squash-merge\n'
printf 'PREMERGE: SCOPE composes this diff with main tip, which no gate here has executed.\n'
# One added SCOPE line pointing at the advisory (#3650 slice 1). The three lines
# above are RETAINED verbatim: slice 1 ships INFORMATION, not the merge-result
# gate, so the disclaimer they carry is still true.
printf 'PREMERGE: SCOPE the PREMERGE: ADVISORY lines below measure that gap (non-blocking, #3650 slice 1).\n'
# Printed here, MEASURED earlier (see the note above the head check): the
# advisory's 65s bound must not sit between the head check and OK.
if [ -n "$advisory_out" ]; then
  printf '%s\n' "$advisory_out"
fi
printf 'PREMERGE: GATE-OF-RECORD commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
  "$full_commit" "$full_ts" "$full_dirty" "$summary_file"
if [ -n "$delta_file" ]; then
  # `anchor-ancestry:` is the AFFIRMATIVE record that the #3653 binding RAN. After
  # assert_anchor_on_history it can only ever read BOUND — which is the point: a
  # silent pass is indistinguishable from a check that was never reached.
  # THE BOUNDARY IS DECLARED ON THE LINE, NOT ENUMERATED FOREVER (job 361; the
  # #3746 / job-311 precedent, applied verbatim: a check that claims nothing false
  # is worth more than one claiming a closure it does not deliver). Folded into the
  # ONE suffix the renderer consumes — never appended per verdict arm, or the two
  # spellings would drift.
  #
  # `anchor-reads:` is the AFFIRMATIVE record that the bound was applied
  # (`bounded-<n>s+<g>s`). Since the reversal documented in the header there is no
  # UNBOUNDED spelling: a box with no supported runner REFUSES before reaching
  # here, so this token can never silently mean "we gave up on bounding". Printed
  # rather than assumed, because a bounded path degrading unnoticed is the hazard
  # the token exists for.
  printf 'PREMERGE: DELTA-RECERT anchor: %s anchor-ancestry: %s anchor-reads: %s commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s | %s\n' \
    "$delta_anchor" "$ANCHOR_ANCESTRY" "$ANCHOR_READS" "$delta_commit" "$delta_ts" "$delta_dirty" \
    "$delta_file" "$ANCHOR_PROVENANCE"
fi
exit 0
