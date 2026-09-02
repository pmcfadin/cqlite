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
#     in `delta-anchor:`, and (iii) whose OWN `commit:`/`tree-start:` cover the
#     certified sha. The chain is therefore closed end to end: full PASS at X →
#     delta re-cert anchored at X → delta ran on Y → Y is the PR head.
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
# TWO RESIDUALS, STATED RATHER THAN FAKED
# ---------------------------------------
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
#     THREAT MODEL — whoever runs this script controls the process and could
#     edit the script, fake the file, or skip the script entirely; no check
#     inside a process defends against the party that controls the process. What
#     this guard defends is ACCIDENT AND DRIFT, which is the observed failure
#     mode: a diligent worker with no step in its path telling it the gate of
#     record was never run.
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
#
# THE C (INTENT AUDIT) VERDICT IS REQUIRED AT THE MERGE POINT (#3751)
# -------------------------------------------------------------------
# A delegated review stage used to write NOTHING at any point in its life, so its
# reader had only ABSENCE to reason from — and every consumer of an absence has to
# CHOOSE how to read it. Every measured instance so far was recorded as not-run by
# its own lane and nothing REQUIRED it; no false certification has occurred yet.
# `scripts/flow/review-stage.sh` makes a stage's verdict an ARTIFACT with a CLOSED
# grammar; this script is the point that CONSUMES it, so an absent C can no longer
# reach a merge.
#
#   --c-verdict AUTO     MEASURE whether C is required from the CERTIFIED tree,
#                        then read the stage's verdict. The intended form.
#   --c-verdict <path>   a file holding a captured verdict line, i.e.
#                        `review-stage.sh verdict c --issue <N> > <path>`.
#
# THE FLAG IS REQUIRED AND ITS OMISSION IS EXIT 3 — the #3465 precedent, which
# broke pre-existing callers loudly and on purpose. A silent default of "C is not
# required" would reproduce, inside the enforcer, the exact defect the enforcer
# exists to close. It is a NAMED flag rather than a fifth positional so it
# composes with #3752's sibling required flag in EITHER landing order, and the
# missing-flag census names each absent flag independently, so this one's exit 3
# does not depend on being the only required flag.
#
# ROUTING IS MEASURED FROM THE CERTIFIED TREE, NEVER TAKEN FROM THE CALLER.
# A caller-supplied "C does not apply here" is exactly the escape hatch #3751
# exists to remove, so `AUTO` asks git what THIS BRANCH does to
# `openspec/changes/`: the diff between merge-base(origin/main, <certified>) and
# <certified>, with `openspec/changes/archive/**` AND pure DELETIONS excluded
# (archiving is flow-finalize's work, never a routing signal — and because rename
# detection is pinned off, a real archive MOVE is a deletion plus an addition under
# `archive/`, so counting the deletion refused every finalize PR: #3751 round 1 F4).
# Non-empty ⇒ DESIGN-ROUTED ⇒ C
# REQUIRED; empty ⇒ affirmatively `NOT-APPLICABLE (no openspec change on branch)`.
#   * A plain LISTING of `openspec/changes/` cannot answer this. Measured
#     2026-09-01: `origin/main` carries `archive` PLUS two sibling lanes' in-flight
#     change directories, so every branch would read design-routed and the
#     "measurement" would be vacuous — a check that reds on correct input is the
#     check agents learn to waive.
#   * The base is the MERGE-BASE, never `origin/main`'s TIP (#3392). A tip
#     comparison reports another lane's newly-landed change as a difference of
#     THIS branch, which reds a correct oracle-driven PR.
#   * THE PATHSPEC IS ROOT-ANCHORED (`:(top)`), so the answer does NOT depend on
#     the caller's WORKING DIRECTORY (#3751 round 11, Q1). A bare pathspec is
#     interpreted relative to the cwd, so run from a repository subdirectory this
#     measurement returned EMPTY and a design-routed branch merged with NO C
#     verdict. `diff.relative=false` does NOT cover this — it controls the OUTPUT
#     path prefix, not pathspec interpretation — so BOTH are pinned, and neither
#     substitutes for the other. (The stage LOOKUP has always been
#     cwd-independent, via `c_stage_root`'s `--show-toplevel`; the routing
#     measurement was the one half that was not.)
#   * It measures the CERTIFIED sha, not this checkout's HEAD — the same rule the
#     base-staleness advisory follows: a report about a different tree than the one
#     being merged is the "satisfied and wrong" shape.
#   * ANY failure to measure (no git, no `origin/main`, the certified commit absent
#     from this checkout, a failing diff) is `UNMEASURED` and is TREATED AS
#     REQUIRED. Never derive a pass from the absence of a bad signal.
# There is deliberately NO spelling of the flag that means "not applicable": a
# supplied PATH can only carry a review-stage verdict token, and `NOT-APPLICABLE`
# is not in that closed grammar, so a file asserting it is refused as an
# unrecognised token. Inapplicability is reachable ONLY through AUTO's measurement.
#
# EVERY READ OF UNTRUSTED CONTENT GOES THROUGH `c_capture_map_nul` (#3751 round
# 13, S2), because a COMMAND SUBSTITUTION SILENTLY DISCARDS NUL BYTES and so does
# not merely lose information — it MANUFACTURES the token this script validates.
# Measured on the shipped artifacts: gawk passes a NUL through a field, so a
# `--c-verdict` file whose token is `PA\0SS` — NOT `PASS`, and a token the closed
# set must therefore refuse — arrived here as `PASS` and this script reported
# `PREMERGE: OK` at exit 0. A capture that normalises its input cannot be the thing
# that validates it.
#
# THAT SENTENCE WAS FALSE FOR ONE READER FOR A WHOLE ROUND (#3751 round 14, T1).
# Round 13 routed the `--c-verdict` read and `c_record_bytes` and left `_gate_awk`
# — the parser of the GATE OF RECORD, the artifact #3465 exists to require —
# reading its file RAW. Measured: `RESULT: PA<NUL>SS` yielded `v_result=PA<NUL>SS`
# from awk and the capture in `gate_parse_file` removed the byte, so this script
# read `PASS` from a summary that does not contain it. THREE rounds in a row have
# now found "a boundary exists and one path bypasses it", so the completeness is
# asserted STRUCTURALLY by `scripts/tests/lib/read-boundary-scan.sh` rather than
# by this sentence — round 13's own asserts check the mapping appears exactly ONCE,
# which is a property of the BOUNDARY and not of its CALLERS.
#
# AND EVERY LINE IS PRINTED WITH `printf` OF A LITERAL FORMAT — NEVER `echo`
# (#3751 round 14, T2). `c_safe_display` neutralises the VALUE; the printing
# COMMAND must not re-interpret what it just neutralised. Under the bash option
# `xpg_echo`, settable by an INHERITED environment (`BASHOPTS`, `SHELLOPTS`, a
# `BASH_ENV` file) and never by this script, `echo` performs BACKSLASH ESCAPE
# PROCESSING on its argument, so a `\n` in a legal path splits a line, `\033`
# injects terminal control, `\c` truncates, and octal `\075` manufactures a REAL
# `=`. This script uses no `echo` today — measured, zero occurrences — and
# `scripts/tests/lib/emit-boundary-scan.sh` keeps it that way, also requiring
# every `printf` FORMAT to be a literal this script authored.
#
# ONLY `PASS` AND `AUTHOR-PERFORMED` PROCEED, AND THE SECOND KEEPS ITS OWN TOKEN.
# `AUTHOR-PERFORMED` is review-stage.sh's disclosed hand-audit substitute; it is
# reported on its own `PREMERGE: C-VERDICT` line and is NEVER folded into
# `PREMERGE: OK`, because a reader must be able to see that the intent audit was
# performed by the diff's AUTHOR. `FINDINGS`, `NOT-RUN` and every unrecognised
# token REFUSE, naming the stage and the cause. The token is reduced to its FIRST
# WORD and matched by STRING EQUALITY, never a prefix test (#3544).
#
# ONE DECLARED RESIDUAL. With an explicit `--c-verdict <path>` this script verifies
# the verdict's GRAMMAR (including that the stage KIND is `c`) and TOKEN, not that
# the stage it came from belongs to THIS issue: the verdict line carries a kind, an
# agent and a report path, and no sha. The report path IS printed on the success
# line so a human can see which stage answered. Under `AUTO` the binding is
# MECHANICAL and is why AUTO is the intended form: the stage is located in this
# worktree, two stage records are refused as ambiguous, and TWO INDEPENDENT
# BINDINGS are applied, because they answer different questions and neither
# replaces the other.
#   (a) THE WORKTREE (#3751 round 1, F1). This worktree's HEAD must EQUAL the
#       certified commit before a locally-located stage is trusted at all — every
#       lane on this box is a worktree of ONE shared `.git`, so a peer lane's
#       certified commit RESOLVES here and resolvability is not provenance. Rule 1
#       asserts `headRefOid` == certified, so HEAD == certified binds the local
#       artifact to THIS PR transitively.
#   (b) THE ARTIFACT (#3751 round 3, G1). The stage RECORD's own `head-sha:` — the
#       commit review-stage.sh resolved when the stage was OPENED — must equal the
#       certified commit too. (a) cannot see a STALE ARTIFACT: it is satisfied BY
#       CONSTRUCTION, because a lane stands at the very commit it is certifying, so
#       a `result: PASS` recorded BEFORE a further commit, an amend or a rebase
#       persisted in `.review-stage/` and certified the NEW tree. A record with no
#       `head-sha:`, several of them, or a value that is not a 40-hex sha is a
#       NAMED REFUSAL and never a skip — an older record predating the field must
#       not be readable as certifying. FAIL-CLOSED BY DESIGN: this is the
#       gate-of-record rule (any change after the gate INVALIDATES it) applied to
#       the intent audit, and an audit of an older tree may not certify a newer
#       one. Every one of those refusals prints the same remedy — re-open the stage
#       at this commit with `--force` (which RE-STAMPS `head-sha`, deliberately
#       unlike `spawned-at`) and re-run C.
#   (c) ONE OBSERVATION (#3751 round 9, N2). (b) validates `head-sha` from the stage
#       record, and `review-stage.sh verdict` then RE-READS that record to find which
#       report is current — two reads of one record are TWO DIFFERENT FACTS. An atomic
#       replacement in between yields a verdict from a different GENERATION of the
#       stage, possibly bound to a different commit, under a binding checked on the
#       old one: measured, the success line named `stage-head=<validated>` beside a
#       report belonging to a generation whose own head-sha was forty zeros. So the
#       record is captured ONCE (the `head-sha` is parsed from that capture, not from a
#       second read) and is re-required to be BYTE-IDENTICAL before the token is
#       parsed. A handoff — resolving the report here and passing it to `verdict` —
#       is deliberately NOT the fix: round 4 (H2) deleted `--report` so that nothing
#       outside `review-stage.sh` can name which file holds a verdict, and a path
#       argument would rebuild that channel from the other end.
#   (d) THE GENERATION (#3751 round 10, P2). (c) is defeated by an ABA REPLACEMENT:
#       the record can go from the validated generation A to a foreign generation B
#       while `verdict` reads B, and BACK to A before the byte comparison — two
#       identical observations, comparison passes, accepted verdict came from B.
#       Equality of two observations is not identity of the thing observed at a third
#       instant. So the verdict is bound to the generation ITSELF, using a value it
#       already reports OUTWARD: since round 6 (K2) the report path carries the
#       generation's nonce (`<kind>.<nonce>.md`) and `verdict` publishes it as its
#       mandatory `report=` field, so that field must name the nonce carried by the
#       capture (b) was validated on — read from the SAME capture, never a fresh
#       read. ABA cannot satisfy it: a verdict read from B returns B's nonce. Still
#       nothing is passed INTO `review-stage.sh` — reading a value OUT of the verdict
#       line rebuilds no control channel — and (c) is KEPT as defence in depth: it
#       catches an edit under the SAME nonce and a vanished record, which (d) cannot,
#       and (d) catches what it cannot. Every state that cannot be bound REFUSES and
#       NAMES itself (a legacy record with no `report-nonce:`, several of them, an
#       unusable token, a `report=unresolved`, a foreign nonce). It gates the two
#       tokens the closed grammar lets PROCEED, because acceptance is the only thing
#       that can certify — every other token already refuses, and there
#       `review-stage.sh`'s own cause is the more precise operator action.
#   WHICH REPORT the record names is NOT this script's question (#3751 round 5 J1,
#   round 6 K2). The record also carries a `report-nonce:`, and the report path
#   INCLUDES it (`<kind>.<nonce>.md`; a bare `<kind>.md` only for a record written
#   before the field existed) so that a PREVIOUS, idle agent resuming after a
#   `--force` holds a stale path and cannot write into the current report at all.
#   The nonce is GENERATED, never chosen by scanning what is on disk, so two
#   concurrent opens cannot be handed one path. This script never derives that path:
#   it runs `review-stage.sh verdict`, which resolves the nonce from the same
#   record with the same function `open` wrote it with. A record whose
#   `report-nonce:` cannot be read therefore arrives here as a NON-PASSING
#   TOKEN (`NOT-RUN (stage record unreadable: …)`), which the closed grammar below
#   refuses like any other — no fallback, and no second opinion about which file
#   holds the verdict. It does, since round 10, CHECK the ANSWER — binding (d)
#   requires the returned `report=` to name the validated generation's nonce — which
#   is a different act from deriving the path or naming one inward: the callee still
#   decides which report is current.
#
# USAGE
#   scripts/flow/premerge-assert.sh <pr-number> <certified-sha> \
#       <gate-of-record-summary> [<delta-summary>] --c-verdict <path|AUTO>
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
#         code), "PREMERGE: GATE-OF-RECORD ..." and "PREMERGE: C-VERDICT ..."
#         (plus "PREMERGE: DELTA-RECERT ..." in Case B, and
#         "PREMERGE: C-VERDICT-NOTE ..." when the C token is AUTHOR-PERFORMED)
#   2   no/invalid gate of record, OR no/invalid C verdict where C is required
#       (#3751), OR head moved (mismatch), OR PR closed/merged
#       — LOUD multi-line refusal
#   3   gh/network failure, a required TOOL failing, or a usage error (which now
#       INCLUDES omitting --c-verdict, #3751) — fail
#       closed, never merge on uncertainty. The three are distinguished by the
#       printed marker, NOT by the code: `PREMERGE: USAGE` (you called it wrong),
#       `PREMERGE: TOOL-FAILURE` (a broken box — fix the box, do NOT re-run the
#       gate), `PREMERGE: GH-FAILURE` (auth/network/no-such-PR).
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -euo pipefail

repo="${GH_REPO:-pmcfadin/cqlite}"

usage() {
  # A distinct grepable marker (#3465 review nit 8): exit 3 covers a USAGE error,
  # a TOOL failure and a GH failure, and a caller must be able to tell them apart
  # — "you called me wrong" is not "GitHub is down". The exit CODES are unchanged.
  printf 'PREMERGE: USAGE — the call is wrong (this is NOT a gh/network failure)\n' >&2
  # Optional REASON lines, printed FIRST so the specific complaint is the first
  # thing read. `usage` with no arguments is unchanged, which is why every
  # pre-#3751 call site still reads correctly.
  while [ "$#" -gt 0 ]; do
    printf 'PREMERGE: USAGE   %s\n' "$1" >&2
    shift
  done
  printf 'usage: %s <pr-number> <certified-sha> <gate-of-record-summary> [<delta-summary>] \\\n' \
    "$(basename "$0")" >&2
  printf '           --c-verdict <path|AUTO>\n' >&2
  printf '       <gate-of-record-summary> is REQUIRED: the AGENT_GATE_SUMMARY_FILE of the\n' >&2
  printf '       FULL gate (a "==== AGENT-GATE SUMMARY ====" block with RESULT: PASS and\n' >&2
  printf '       tree-integrity: PASS). With 3 args it must be AT the certified sha.\n' >&2
  printf '       <delta-summary> is OPTIONAL: an "==== AGENT-GATE DELTA SUMMARY ====" block\n' >&2
  printf '       whose delta-anchor: is the full block above and whose own commit:/\n' >&2
  printf '       tree-start: are AT the certified sha (the #1892 post-gate-polish route).\n' >&2
  printf '       See #3465.\n' >&2
  printf '       --c-verdict is REQUIRED (#3751) and has NO default — omitting it is THIS\n' >&2
  printf '       usage failure, never a silent "C is not required":\n' >&2
  printf '         --c-verdict AUTO     MEASURE from the certified tree whether C is\n' >&2
  printf '                              required, then read the stage verdict. The stage\n' >&2
  printf '                              must be BOUND to the certified sha twice: this\n' >&2
  printf '                              worktree HEAD, and the stage record head-sha: it\n' >&2
  printf '                              was opened at. A stale, missing or unparsable\n' >&2
  printf '                              head-sha REFUSES — re-open with --force, re-run C\n' >&2
  printf '                              WRITING INTO THE PATH THAT open PRINTS: a --force\n' >&2
  printf '                              re-open publishes a report under a FRESH NONCE, so\n' >&2
  printf '                              the previous file is no longer read (#3751).\n' >&2
  printf '                              The record is read ONCE and must still be\n' >&2
  printf '                              byte-identical when the verdict comes back: a\n' >&2
  printf '                              generation swapped in mid-check REFUSES (#3751).\n' >&2
  printf '                              AND the verdict itself must NAME that generation:\n' >&2
  printf '                              its report= field must carry the record report-nonce:\n' >&2
  printf '                              that was validated, so a record swapped in and BACK\n' >&2
  printf '                              OUT (byte-identical, verdict read from the other\n' >&2
  printf '                              generation) REFUSES too. A legacy record with no\n' >&2
  printf '                              report-nonce: cannot be bound and REFUSES (#3751).\n' >&2
  printf '                              The routing measure is ROOT-ANCHORED, so it does\n' >&2
  printf '                              not depend on your working directory (#3751).\n' >&2
  printf '         --c-verdict <path>   a file holding a captured verdict line, i.e.\n' >&2
  printf '                              scripts/flow/review-stage.sh verdict c --issue <N> > <path>\n' >&2
  printf '                              Capture it WHOLE: the stage KIND, every mandatory key\n' >&2
  printf '                              AND each key VALUE are validated, so a bare report= or\n' >&2
  printf '                              an emptied elapsed=/deadline=/agent= is refused (#3751).\n' >&2
}

# --- ARGUMENTS: POSITIONALS PLUS NAMED REQUIRED FLAGS (#3751/#3752) ----------
# The three/four positionals are the pre-#3751 contract and are UNCHANGED. What is
# new is that a required argument arrives as a NAMED FLAG, deliberately:
#
#  * #3752 binds this same script to the roborev certification and will add a
#    sibling required flag. A named flag composes in EITHER landing order, where a
#    fifth positional would not — and this parse loop is the one place a new flag
#    is added.
#  * the missing-flag census below names EACH absent required flag independently,
#    so `--c-verdict`'s exit-3-on-omission does NOT depend on being the only
#    required flag. A `[ $# -ne N ]` arity test would have exactly that dependency.
c_verdict=""
c_verdict_set=0
pos_count=0
pos1=""; pos2=""; pos3=""; pos4=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --c-verdict)
      shift
      # An ABSENT value is a usage failure, never an empty default — the same rule
      # the empty fourth positional gets below. A caller whose variable expanded to
      # nothing must be told, never silently downgraded.
      if [ "$#" -eq 0 ]; then
        usage '--c-verdict requires a value: a <path> to a captured verdict line, or AUTO'
        exit 3
      fi
      c_verdict="$1"
      c_verdict_set=1
      ;;
    --c-verdict=*)
      c_verdict="${1#*=}"
      c_verdict_set=1
      ;;
    -*)
      usage "unknown option '$1'"
      exit 3
      ;;
    *)
      pos_count=$((pos_count + 1))
      case "$pos_count" in
        1) pos1="$1" ;;
        2) pos2="$1" ;;
        3) pos3="$1" ;;
        4) pos4="$1" ;;
        *)
          usage "too many positional arguments (got $pos_count, expected 3 or 4)"
          exit 3
          ;;
      esac
      ;;
  esac
  shift
done

if [ "$pos_count" -ne 3 ] && [ "$pos_count" -ne 4 ]; then
  usage "expected 3 or 4 positional arguments, got $pos_count"
  exit 3
fi

# THE MISSING-FLAG CENSUS. One entry per required flag, each named on its own, so
# adding #3752's flag here cannot weaken this one's refusal.
missing_flags=""
[ "$c_verdict_set" -eq 1 ] || missing_flags="$missing_flags --c-verdict"
if [ -n "$missing_flags" ]; then
  usage "MISSING REQUIRED FLAG(S):$missing_flags" \
    "This is deliberately NOT a default (#3751): a silent 'C is not required' would" \
    "reproduce, inside the enforcer, the exact defect the enforcer exists to close —" \
    "an absent review read as a clean one. Pass --c-verdict AUTO to have the routing" \
    "MEASURED from the certified tree."
  exit 3
fi
if [ -z "$c_verdict" ]; then
  usage '--c-verdict was given an EMPTY value' \
    'An empty value is a caller bug (an unset variable), not "AUTO" and not "skip".'
  exit 3
fi

pr="$pos1"
certified="$pos2"
certified_raw="$pos2"
summary_file="$pos3"
delta_file="$pos4"

if [ -z "$pr" ] || [ -z "$certified" ] || [ -z "$summary_file" ]; then
  usage 'an empty <pr-number>, <certified-sha> or <gate-of-record-summary>'
  exit 3
fi
# An EMPTY fourth argument is a usage failure, not "3-arg mode": a caller whose
# variable expanded to nothing must be told, never silently downgraded.
if [ "$pos_count" -eq 4 ] && [ -z "$delta_file" ]; then
  usage 'an EMPTY fourth argument (<delta-summary>)' \
    'This is not "3-arg mode" — a caller whose variable expanded to nothing is told.'
  exit 3
fi

# Normalize the certified SHA to lowercase and require a full 40-char hex SHA —
# an abbreviated or malformed value can never be safely compared to headRefOid.
certified=$(printf '%s' "$certified" | tr '[:upper:]' '[:lower:]')
case "$certified" in
  *[!0-9a-f]* | "")
    printf 'error: certified SHA must be 40 hex chars (got: %s)\n' "$certified_raw" >&2
    usage
    exit 3
    ;;
esac
if [ "${#certified}" -ne 40 ]; then
  printf 'error: certified SHA must be a full 40-char hex SHA (got %d chars: %s)\n' \
    "${#certified}" "$certified_raw" >&2
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
  printf '  summary file: %s\n' "$(c_safe_display "$summary_file")" >&2
  # ROUTED LIKE ITS SIBLING ONE LINE UP (#3751 round 9, N3). This is the CALLER's fourth
  # argument, exactly as invoker-supplied as `$summary_file`, and it went out raw for months
  # because the structural guard could not SEE this line: its scope was anchored at the start of a
  # line and this statement begins `[ -n … ] &&`. The guard is positional now.
  [ -n "$delta_file" ] && printf '  delta summary file: %s\n' "$(c_safe_display "$delta_file")" >&2
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
# THE C (INTENT AUDIT) VERDICT AT THE MERGE POINT (#3751)
# ---------------------------------------------------------------------------
# See the header. Everything below is OFFLINE, so "you have no C verdict" is
# reportable without a network round trip, exactly like the gate-of-record half.
#
# `c` is the stage KIND the C intent audit uses, by convention shared with
# scripts/flow/review-stage.sh's callers (flow-closer opens `c`). It is a constant
# and NOT an option: a caller able to choose which stage counts as C could point
# this check at a stage nobody gates on.
# --- the capture boundary ----------------------------------------------------
# A CAPTURE MUST NOT MANUFACTURE THE TOKEN IT VALIDATES (#3751 round 13, S2).
#
# THE FINDING, measured on the shipped artifacts: bash SILENTLY DISCARDS NUL bytes in a command
# substitution, and BOTH of this script's reads of untrusted content are captures — awk's OUTPUT for
# the `--c-verdict` file, and `c_record_bytes`' read of the stage record. gawk passes a NUL through
# a field, so a verdict file whose token is `PA\0SS` — a token that is NOT `PASS`, and which the
# closed-set match must therefore refuse — arrived here as `PASS` and the merge point reported
# `PREMERGE: OK`. The capture created the very token the flag exists to verify.
#
# THE FIX IS IN THE READ, NOT IN A PROBE: a separate `grep -q`/`wc -c` of the same path is a SECOND
# observation, and one direction of its disagreement is a FALSE PASS (the capture reads the
# NUL-bearing version while the probe reads a clean one). So the ONE read maps NUL to SOH in the
# stream. The byte count is preserved, nothing is lost, and `PA<SOH>SS` fails the closed-set match
# by STRING EQUALITY exactly as `PASSthisNeverRan` does.
#
# ONE LITERAL, THE BYTE DERIVED FROM IT — `tr` needs the four characters `\001` and a detector needs
# the byte, and two hand-written spellings are two places to diverge, where a divergence means the
# detector looks for a byte the mapper never writes (a silent false PASS).
#
# `review-stage.sh` carries the same idiom over its own files and the two are deliberately NOT a
# shared implementation, for the reason `c_record_bytes` states about its sibling: no agreement
# between them is required, since each is used within ONE process over a DIFFERENT file.
C_CAPTURE_NUL_TR='\001'
C_CAPTURE_NUL_BYTE="$(printf '%b' "$C_CAPTURE_NUL_TR")"

# c_capture_map_nul <path> — read <path> for a value that is about to enter a SHELL VARIABLE (or a
# tool whose output will). The ONE mapping implementation in this script.
c_capture_map_nul() {
  LC_ALL=C tr '\000' "$C_CAPTURE_NUL_TR" <"$1"
}

C_STAGE_KIND=c

# The stage-verdict grammar this script CONSUMES (scripts/flow/review-stage.sh):
#   REVIEW-STAGE: <kind> RESULT: <token> elapsed=<s> deadline=<s> agent=<t> report=<p>
# Only these two tokens may proceed. Everything else — FINDINGS, NOT-RUN, and any
# unrecognised value — REFUSES.
C_TOKEN=""          # the token that was read
C_TOKEN_LINE=""     # the verdict line it was read from, for the diagnostic
C_TOKEN_REPORT=""   # the `report=` field, so a human can see WHICH stage answered
C_SOURCE=""         # how the verdict was obtained
C_ROUTING=""        # REQUIRED | NOT-APPLICABLE | UNMEASURED
C_ROUTING_DETAIL=""
# ONE remedy sentence for every stage-binding refusal (#3751 round 3, G1): each of those
# refusals has the SAME next action, and six copies of it is six places for it to drift.
C_REOPEN_REMEDY="Remedy: re-open the stage at THIS commit and re-run C — review-stage.sh open <kind> --issue <N> --agent spec-auditor --force (--force RE-STAMPS head-sha, deliberately unlike spawned-at, and publishes the report under a FRESH NONCE: spawn the auditor with the path that command PRINTS, because the previous file is no longer read) — then read it with: review-stage.sh verdict <kind> --issue <N>"

# c_safe_display <text> — THE ONE EMIT BOUNDARY for text this script did not produce (#3751 round
# 5, J3). The stage verdict line, its token, its `report=` field and its stage KIND all come from a
# file a DELEGATED AGENT wrote, so they are DATA — and they used to reach stderr and stdout
# verbatim. `_c_verdict_awk` strips ANSI CSI sequences as parse hygiene (#3400), which is neither
# complete (a bare ESC, BEL, backspace, VT/FF, DEL or an OSC `ESC ] … BEL` sequence survives it)
# nor a display guarantee. So every value those two printers render goes through here.
#
# Same three classes as `review-stage.sh`'s `one_line`, and the same rendering PER BYTE, so one
# value cannot read two ways depending on which script printed it: the whitespace controls become a
# SPACE (this printer puts ONE argument per line, so a smuggled newline would forge a line of the
# refusal block), every other C0 byte plus DEL becomes a VISIBLE `?`, and NUL is dropped. It does
# NOT squeeze runs of spaces, which `one_line` does: the detail lines of this block are ALIGNED
# with leading and internal spaces, and collapsing them would mangle correct output to normalise
# text nobody reads as a record. The property required here is one line per argument, not a
# canonical spacing.
#
# DISPLAY-ONLY, and that is the whole safety argument: every decision — the token comparison, the
# stage-kind equality, the field census, the exit code — is made EARLIER, on the RAW value, so this
# cannot turn a refusal into a pass. `LC_ALL=C` keeps `tr` byte-oriented, so a UTF-8 sequence
# passes through readable instead of aborting a BSD `tr` mid-pipeline.
c_safe_display() {
  printf '%s' "${1:-}" |
    LC_ALL=C tr -d '\000' |
    LC_ALL=C tr '\n\r\t\013\014' '     ' |
    LC_ALL=C tr '\001-\010\016-\037\177' '?'
}

refuse_no_c_verdict() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: NO-C-VERDICT — REFUSING TO MERGE\n' >&2
  printf '  stage: %s (the C intent audit)\n' "$C_STAGE_KIND" >&2
  printf '  --c-verdict: %s\n' "$(c_safe_display "$c_verdict")" >&2
  printf '  routing: %s%s\n' "$C_ROUTING" \
    "${C_ROUTING_DETAIL:+ ($(c_safe_display "$C_ROUTING_DETAIL"))}" >&2
  [ -z "$C_TOKEN" ] || printf '  verdict token: %s\n' "$(c_safe_display "$C_TOKEN")" >&2
  [ -z "$C_TOKEN_LINE" ] || printf '  verdict line: %s\n' "$(c_safe_display "$C_TOKEN_LINE")" >&2
  # EVERY DETAIL LINE TOO, not only the two fields above: several callers INTERPOLATE a value they
  # read from the verdict stream into their prose (the stage KIND, the value that could not be
  # parsed), and neutralising the fields while leaving the prose raw is the per-site escaping this
  # boundary exists to replace — a list to keep complete is a list that goes stale.
  while [ "$#" -gt 0 ]; do
    printf '  %s\n' "$(c_safe_display "$1")" >&2
    shift
  done
  printf '  An ABSENT review is not a clean one (#3751). Every measured instance so\n' >&2
  printf '  far was recorded as not-run by its own lane, and nothing REQUIRED it.\n' >&2
  printf '  This check is that requirement. The remedy is to RUN\n' >&2
  printf '  the stage and let it record a verdict:\n' >&2
  printf '    bash scripts/flow/review-stage.sh open %s --issue <N> --agent spec-auditor\n' \
    "$C_STAGE_KIND" >&2
  printf '    # ...spawn the auditor with the clause that command prints...\n' >&2
  printf '    bash scripts/flow/review-stage.sh verdict %s --issue <N>\n' "$C_STAGE_KIND" >&2
  printf '  If no independent audit is available, the SANCTIONED FALLBACK is a\n' >&2
  printf '  disclosed substitute WITH ITS WORKING — never a hand-asserted pass:\n' >&2
  printf '    bash scripts/flow/review-stage.sh record-author-performed %s --issue <N> \\\n' \
    "$C_STAGE_KIND" >&2
  printf '      --reason <why-no-peer-audit> --evidence <artifact> --performed-by author\n' >&2
  printf '  It reports the DISTINCT token AUTHOR-PERFORMED, never PASS.\n' >&2
  printf '========================================================\n' >&2
  exit 2
}

# _c_verdict_awk — read a verdict STREAM on stdin, print `key=value` lines.
#
# COLUMN-ZERO ANCHORED (`/^REVIEW-STAGE: /`), never awk's `$1 ==`, which is
# whitespace-insensitive: an INDENTED or `>`-quoted copy of a verdict line is
# DATA — this repository's docs, PR comments and issue bodies all contain such
# copies, and this very script's header will too. Same anchoring rule, same
# reason, as the gate-summary marker matching above (#3312).
#
# The FIRST anchored line supplies the values and every anchored line is COUNTED,
# so two verdicts in one file are AMBIGUOUS and refusable rather than last-wins.
# ANSI is stripped as belt (#3400: colour survives redirection).
#
# THE OTHER READER OF THIS SHAPE IS `review-stage.sh`'s `classify_report`, which
# locates the REPORT's `result:` record. Neither reads the other's file, but both
# answer the same three questions — column zero, exactly one, a closed token set —
# and they DIVERGED TWICE in two review rounds, once per axis, each time with a
# reviewer naming one side (round 2: `classify_report` was not anchored; round 3:
# it did not count). Patching whichever side was named is what let the second
# divergence exist, so their agreement is MECHANICALLY CHECKED: section 44g of
# scripts/tests/test_premerge_assert.sh drives BOTH over ONE shared table of
# adversarial inputs and asserts they agree per row AND reach the expected
# disposition. Change the rule here and that test tells you the other side moved.
#
# IT REPORTS THE WHOLE GRAMMAR, NOT JUST THE TOKEN (#3751 round 1, F2). The
# documented line is
#
#   REVIEW-STAGE: <kind> RESULT: <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>
#
# and the shape is DERIVED FROM WHAT review-stage.sh ACTUALLY EMITS (pinned in
# scripts/tests/test_premerge_assert.sh against a captured real line, so the parser
# cannot drift from the emitter). So `kind` is `$2` and the token is `$4` GATED on
# `$3` being literally `RESULT:` — never a scan for `RESULT:` anywhere on the line,
# which accepted a truncated line and, worse, a SIBLING stage's verdict: a
# `rust-review` PASS line satisfied the C check, measured on this very branch.
# Each mandatory key is COUNTED so the caller can require it EXACTLY ONCE: a
# duplicate is two answers to one question, and a first-wins read is the rule this
# file refuses everywhere else. Keys are counted from field 4 onward because the
# token may be MULTI-WORD (`NOT-RUN (no report written)`), so their positions are
# not fixed; a cause word cannot pose as a key because review-stage.sh neutralises
# `=` in the cause at its emit boundary, and a planted one raises a count and is
# refused as a duplicate — the fail-closed direction.
#
# `report=` IS TAKEN AS THE REMAINDER OF THE LINE, NOT AS ONE FIELD (#3751 round
# 11, Q3). It carries an absolute PATH, and a path may legitimately contain
# WHITESPACE — a checkout at `/tmp/work tree`; this repository itself tracks 40
# space-bearing paths under `docs/`. Read as one whitespace-delimited field the
# value TRUNCATED at the first space, and round 10's nonce match then REFUSED an
# otherwise VALID verdict: a FALSE REFUSAL on correct input, which is the guard
# agents learn to waive (measured, on the shipped artifacts in a checkout named
# `…/work tree`: `verdict reported: /tmp/…/work` beside a `validated generation:`
# that was exactly the one the verdict named).
#
# THE REMAINDER RULE IS SOUND ONLY BECAUSE `report=` IS EMITTED LAST, so that
# assumption is ENFORCED rather than assumed: section 44l of
# scripts/tests/test_premerge_assert.sh RUNS the shipped emitter through every
# state it has and requires no mandatory key to follow `report=` on any line it
# produces, and pins the single emit site's trailing field structurally. Append a
# field after `report=` and that suite reds instead of verdicts silently
# truncating again.
#
# THE CURSOR WALK IS WHAT MAKES THE OFFSET EXACT. `index()` is searched from the
# END OF THE PREVIOUS FIELD, so a field whose text also occurs earlier on the line
# cannot mis-locate it — a bare `index($0, "report=")` could. `fs == 0` cannot
# occur (a field is by construction present at or after that cursor) and there is
# DELIBERATELY NO FALLBACK to the field value: a prefix of the real path is a
# value nobody measured, and this file's standing rule is that a positive verdict
# requires an affirmative measurement. `rep` therefore stays EMPTY, which the
# value validator below refuses BY NAME (`report= is EMPTY`) — the fail-closed
# direction, and it keeps the truncating form out of the source entirely, which is
# what the structural pin in section 44l asserts.
#
# TWO PROPERTIES DELIBERATELY UNCHANGED. (1) The key census still scans EVERY
# field, including the words of a space-bearing path, so a duplicated `report=`
# is still refused; the cost is that a path component spelled `elapsed=…` /
# `deadline=…` / `agent=…` / `report=…` raises a count and REFUSES — fail-closed,
# and declared rather than silently permitted. (2) NOTHING IS TRIMMED from the
# remainder: a parser that trims is a parser that guesses, the emitter's own
# `one_line` leaves no trailing whitespace, and a hand-edited capture that gained
# some fails the nonce match and refuses.
#
# The emitter's `one_line` COLLAPSES runs of whitespace to one space, so a path
# containing a tab or two adjacent spaces is already lossy ON THE PRODUCING SIDE.
# That is not this reader's to repair — and it does not matter to the binding,
# whose pattern matches the FILENAME tail. Stated so the limit is not rediscovered.
_c_verdict_awk() {
  awk '
  BEGIN {
    n = 0; tok = ""; rep = ""; line = ""; kind = ""; rpos = 0
    ke = 0; kd = 0; ka = 0; kr = 0
    ve = ""; vd = ""; va = ""
  }
  { gsub(/\033\[[0-9;]*[a-zA-Z]/, ""); sub(/\r$/, "") }
  /^REVIEW-STAGE: / {
    n++
    if (n == 1) {
      line = $0
      kind = $2
      if ($3 == "RESULT:") {
        rpos = 1
        if (NF >= 4) tok = $4
      }
      cur = 1
      for (i = 1; i <= NF; i++) {
        off = index(substr($0, cur), $i)
        fs = 0
        if (off > 0) { fs = cur + off - 1; cur = fs + length($i) }
        if (i < 4) continue
        if (substr($i, 1, 8) == "elapsed=") { ke++; if (ke == 1) ve = substr($i, 9) }
        else if (substr($i, 1, 9) == "deadline=") { kd++; if (kd == 1) vd = substr($i, 10) }
        else if (substr($i, 1, 6) == "agent=") { ka++; if (ka == 1) va = substr($i, 7) }
        else if (substr($i, 1, 7) == "report=") {
          kr++
          if (kr == 1 && fs > 0) rep = substr($0, fs + 7)
        }
      }
    }
  }
  END {
    print "n=" n; print "token=" tok; print "report=" rep
    print "kind=" kind; print "rpos=" rpos
    print "ke=" ke; print "kd=" kd; print "ka=" ka; print "kr=" kr
    print "velapsed=" ve; print "vdeadline=" vd; print "vagent=" va
    print "line=" line
  }
'
}

# c_parse_verdict <stream-kind:file|text> <value> <what> — publish CV_* from the
# stream. Refuses (exit 2) on zero or several anchored lines: zero certifies
# nothing, and picking one of several is the "last one wins" rule this file
# refuses everywhere else.
c_parse_verdict() {
  local kind="$1" value="$2" what="$3" out k v
  local missing dup key kname kcount vbad
  if [ "$kind" = file ]; then
    # THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 13, S2): read raw, a NUL inside the token
    # survived awk and was then REMOVED by the capture of awk's output, manufacturing the very
    # token this function validates. `pipefail` is set, so an unreadable file still fails the
    # pipeline and reaches `refuse_tool_failure` exactly as the redirection did.
    out=$(c_capture_map_nul "$value" | _c_verdict_awk) || refuse_tool_failure awk "$what"
  else
    # NO MAPPING HERE, AND NOT BECAUSE IT IS UNTESTED: `$value` has ALREADY passed through a shell
    # variable, so any NUL is long gone and a mapping would be theatre. Its producer is
    # `review-stage.sh verdict`, whose every emitted value goes through `one_line`, which DELETES
    # NUL — an affirmative property of the producer, not an absence of evidence about it.
    out=$(printf '%s\n' "$value" | _c_verdict_awk) || refuse_tool_failure awk "$what"
  fi
  CV_N=""; CV_TOKEN=""; CV_REPORT=""; CV_LINE=""
  CV_KIND=""; CV_RPOS=""; CV_KE=""; CV_KD=""; CV_KA=""; CV_KR=""
  CV_VE=""; CV_VD=""; CV_VA=""
  while IFS='=' read -r k v; do
    case "$k" in
      n)      CV_N="$v" ;;
      token)  CV_TOKEN="$v" ;;
      report) CV_REPORT="$v" ;;
      kind)   CV_KIND="$v" ;;
      rpos)   CV_RPOS="$v" ;;
      ke)     CV_KE="$v" ;;
      kd)     CV_KD="$v" ;;
      ka)     CV_KA="$v" ;;
      kr)     CV_KR="$v" ;;
      velapsed)  CV_VE="$v" ;;
      vdeadline) CV_VD="$v" ;;
      vagent)    CV_VA="$v" ;;
      line)   CV_LINE="$v" ;;
    esac
  done <<C_PARSE
$out
C_PARSE
  case "$CV_N" in
    ''|*[!0-9]*)
      C_TOKEN_LINE=""
      refuse_no_c_verdict \
        "The $what parse produced no usable line count — refusing (fail closed)."
      ;;
  esac
  if [ "$CV_N" -eq 0 ]; then
    refuse_no_c_verdict \
      "The $what holds NO verdict line (no line begins 'REVIEW-STAGE: ' at column zero)." \
      "A captured verdict is produced by:  review-stage.sh verdict $C_STAGE_KIND --issue <N> > <path>" \
      "The stage's REPORT file (.review-stage/issue-<N>/$C_STAGE_KIND.<nonce>.md, whose" \
      "name the stage record names — see review-stage.sh) is NOT that line:" \
      "the report is the agent's prose, the verdict line is the closed-grammar reading of it."
  fi
  if [ "$CV_N" -gt 1 ]; then
    C_TOKEN_LINE="$CV_LINE"
    refuse_no_c_verdict \
      "The $what holds $CV_N verdict lines — AMBIGUOUS, refusing rather than picking one." \
      "A 'take the last line' rule would let a stale or foreign stage certify this merge."
  fi
  C_TOKEN_LINE="$CV_LINE"

  # THE FULL GRAMMAR, VALIDATED (#3751 round 1, F2) — because "somewhere on this
  # line it says RESULT: PASS" is not a verdict about the C stage. Two things were
  # unchecked and both are reachable by ACCIDENT, not only by a hostile hand: a
  # SIBLING stage's verdict (this branch's own diff produced a `code-review` stage
  # whose PASS line satisfied `--c-verdict`), and a TRUNCATED capture (a redirect
  # cut short, a copied fragment) with no `elapsed=`/`deadline=`/`agent=`/`report=`
  # at all.
  #
  # THE STAGE KIND IS COMPARED BY STRING EQUALITY, never a prefix or substring test
  # (#3544): `c-review` is a different stage from `c`, exactly as `PASSthisNeverRan`
  # is not `PASS`.
  if [ "$CV_KIND" != "$C_STAGE_KIND" ]; then
    refuse_no_c_verdict \
      "The $what's verdict line names stage kind '$CV_KIND', not '$C_STAGE_KIND'." \
      "This check is about the C INTENT AUDIT and nothing else: a sibling stage's PASS" \
      "(a rust-review or coverage verdict) says nothing about whether the implementation" \
      "matches its acceptance criteria, so it may not certify C." \
      "Capture the C stage's own verdict:  review-stage.sh verdict $C_STAGE_KIND --issue <N> > <path>"
  fi
  # `RESULT:` MUST BE THE THIRD FIELD, because a scan for it anywhere on the line
  # lets any prose that contains the word supply a token.
  if [ "$CV_RPOS" != 1 ]; then
    refuse_no_c_verdict \
      "The $what's verdict line does not carry 'RESULT:' as its THIRD field, so it is not" \
      "a line of the documented grammar:" \
      "  REVIEW-STAGE: <kind> RESULT: <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>" \
      "A 'RESULT:' found anywhere on the line would let prose supply the token."
  fi
  # THE MANDATORY-FIELD CENSUS runs only once the line HAS a token: a tokenless
  # line's specific complaint is that it has no token (reported by the caller's
  # closed-grammar switch, which names it), and naming the missing fields instead
  # would answer a question the operator did not ask.
  if [ -n "$CV_TOKEN" ]; then
    missing=""; dup=""
    for key in elapsed:"$CV_KE" deadline:"$CV_KD" agent:"$CV_KA" report:"$CV_KR"; do
      kname="${key%%:*}"; kcount="${key#*:}"
      case "$kcount" in
        1) ;;
        0) missing="$missing ${kname}=" ;;
        *) dup="$dup ${kname}=(x$kcount)" ;;
      esac
    done
    if [ -n "$missing" ] || [ -n "$dup" ]; then
      # The detail lines are BUILT rather than passed with `${var:+...}` guards: an
      # empty guard still passes an EMPTY argument, and refuse_no_c_verdict prints
      # every argument it is given, so the refusal would carry blank lines where a
      # cause is expected. `set --` is safe here — this function's own positionals
      # are already held in locals.
      set -- \
        "The $what's verdict line is not of the documented grammar:" \
        "  REVIEW-STAGE: <kind> RESULT: <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>"
      [ -z "$missing" ] || set -- "$@" "  ABSENT field(s):$missing — each one is MANDATORY."
      [ -z "$dup" ] || set -- "$@" \
        "  DUPLICATED field(s):$dup — each must appear EXACTLY ONCE. A duplicate is two" \
        "  answers to one question, and a first-wins read is the rule this file refuses" \
        "  everywhere else."
      set -- "$@" \
        "A truncated capture is the shape a cut-short redirect or a copied fragment leaves." \
        "Re-capture it whole:  review-stage.sh verdict $C_STAGE_KIND --issue <N> > <path>"
      refuse_no_c_verdict "$@"
    fi

    # THE VALUES, NOT ONLY THE FIELD NAMES (#3751 round 7, L3). The census above COUNTS each
    # mandatory key and never looked at what it carried, so a `PASS` line ending in a BARE
    # `report=` — or carrying an empty `elapsed=`, `deadline=` or `agent=` — was ACCEPTED and
    # certified a merge. Round 1's F2 closed presence and multiplicity and left the values
    # unmeasured, which is this repository's recurring "counted, not measured" shape: a count is an
    # affirmative measurement of PRESENCE and of nothing else.
    #
    # THE PERMITTED SET IS DERIVED FROM WHAT THE EMITTER CAN PRODUCE, not from what looks
    # reasonable — otherwise this reds on correct input, which is the guard agents learn to waive.
    # `review-stage.sh verdict` was RUN across every state it has (PASS, FINDINGS, AUTHOR-PERFORMED,
    # and each NOT-RUN cause: no report written / report absent / report empty / report unreadable /
    # report ungrammatical / a self-reported cause / stage never opened / stage record unreadable),
    # and the captured lines carry exactly two shapes per field:
    #   elapsed=  digits (`0` included) or the literal `unknown`
    #   deadline= digits (`0` included, from `--deadline-secs 0`) or the literal `unknown`
    #   agent=    a sanitize_field token, or the literal `unknown`
    #   report=   an absolute path, or the literal `unresolved`  (round 6's K1 states)
    # So `unknown`/`unresolved` are ACCEPTED — they are the emitter's honest "not measured" and are
    # NOT a passing verdict on their own (the token is what proceeds, and it is `NOT-RUN` in every
    # state that produces them). Only agent and report are checked for NON-EMPTINESS rather than for
    # a charset, and the REASON differs per field. `report=` DOES arrive whole, spaces and all,
    # since round 11's Q3 (it is emitted LAST and read as the remainder of the line), so a charset
    # there would be a claim about a legitimate absolute PATH — which may contain anything a
    # filesystem allows. `agent=` is written through `review-stage.sh`'s `sanitize_field`, whose
    # character class excludes whitespace, so it cannot legitimately carry a space; a HAND-EDITED
    # record could, and that value would truncate here — a truncated DIAGNOSTIC, never a wrong
    # verdict, because the token is what proceeds and `=` is neutralised at the emit boundary so a
    # path or cause word cannot forge a key.
    vbad=0
    set -- \
      "The $what's verdict line carries a MANDATORY FIELD WITH NO USABLE VALUE:" \
      "  REVIEW-STAGE: <kind> RESULT: <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>"
    case "$CV_VE" in
      unknown) ;;
      "" | *[!0-9]*) set -- "$@" "  elapsed='$CV_VE' — must be a decimal number of seconds, or the literal 'unknown'."; vbad=1 ;;
    esac
    case "$CV_VD" in
      unknown) ;;
      "" | *[!0-9]*) set -- "$@" "  deadline='$CV_VD' — must be a decimal number of seconds, or the literal 'unknown'."; vbad=1 ;;
    esac
    if [ -z "$CV_VA" ]; then
      set -- "$@" "  agent= is EMPTY — the line must name the agent whose silence this stage measures (review-stage.sh emits 'unknown' when the record cannot be read)."
      vbad=1
    fi
    if [ -z "$CV_REPORT" ]; then
      set -- "$@" "  report= is EMPTY — the line must name the report of record (review-stage.sh emits 'unresolved' when no report path could be derived)."
      vbad=1
    fi
    if [ "$vbad" -eq 1 ]; then
      set -- "$@" \
        "A key that is PRESENT but carries nothing is the shape a hand-edited or truncated capture" \
        "leaves, and the field census above only asserts that each key appears EXACTLY ONCE." \
        "Re-capture it whole:  review-stage.sh verdict $C_STAGE_KIND --issue <N> > <path>"
      refuse_no_c_verdict "$@"
    fi
  fi

  C_TOKEN="$CV_TOKEN"
  C_TOKEN_REPORT="$CV_REPORT"
}

# c_measure_routing — is C REQUIRED for the tree being merged? Measured, never
# taken from the caller. See the header for why a plain listing of
# `openspec/changes/` cannot answer it and why the base is the MERGE-BASE.
#
# Sets C_ROUTING to REQUIRED / NOT-APPLICABLE / UNMEASURED. UNMEASURED is treated
# as REQUIRED by the caller: never derive a pass from the absence of a bad signal.
C_ROUTING_BASE_REF=origin/main
c_measure_routing() {
  local main_sha base out rc=0 p slug="" hits=0
  if ! command -v git >/dev/null 2>&1; then
    C_ROUTING=UNMEASURED; C_ROUTING_DETAIL="git is not on PATH"; return 0
  fi
  if ! git rev-parse --git-dir >/dev/null 2>&1; then
    C_ROUTING=UNMEASURED; C_ROUTING_DETAIL="not inside a git work tree"; return 0
  fi
  if ! git rev-parse --verify --quiet "$certified^{commit}" >/dev/null 2>&1; then
    C_ROUTING=UNMEASURED
    C_ROUTING_DETAIL="the certified commit $certified is not present in this checkout"
    return 0
  fi
  if ! main_sha=$(git rev-parse --verify --quiet "$C_ROUTING_BASE_REF^{commit}" 2>/dev/null) ||
    [ -z "$main_sha" ]; then
    C_ROUTING=UNMEASURED
    C_ROUTING_DETAIL="$C_ROUTING_BASE_REF does not resolve to a commit here"
    return 0
  fi
  if ! base=$(git merge-base "$main_sha" "$certified" 2>/dev/null) || [ -z "$base" ]; then
    C_ROUTING=UNMEASURED
    C_ROUTING_DETAIL="no merge-base between $C_ROUTING_BASE_REF and the certified commit"
    return 0
  fi
  # THE PATHSPEC IS ROOT-ANCHORED WITH `:(top)`, AND THAT IS NOT WHAT
  # `diff.relative=false` DOES (#3751 round 11, Q1). A bare `-- openspec/changes/`
  # pathspec is interpreted RELATIVE TO THE CALLER'S CWD, so this script run from a
  # repository SUBDIRECTORY got an EMPTY diff, measured `NOT-APPLICABLE` on a
  # genuinely design-routed branch, and let the merge proceed with NO C verdict —
  # the exact escape `--c-verdict` exists to close, reached by nothing more exotic
  # than the working directory. `:(top)` is git's pathspec magic for "match from the
  # top of the working tree", so the answer no longer depends on where we were
  # invoked from. `diff.relative` is a DIFFERENT axis: it controls the OUTPUT PATH
  # PREFIX, not pathspec interpretation (measured: from a subdirectory,
  # `-c diff.relative=false diff … -- openspec/changes/` is still empty, while
  # `-- ':(top)openspec/changes/'` finds the path). So BOTH are needed and neither
  # substitutes for the other, and BOTH must stay: `:(top)` anchors what is
  # SELECTED, `diff.relative=false` keeps what is PRINTED root-relative, which the
  # `archive/` prefix test and the slug extraction below both depend on.
  #
  # `diff.renames` pinned OFF for the reason F4 records below (and which
  # scripts/flow/base-staleness.sh records in full for its own scan). NUL-delimited,
  # then translated: a path containing a newline would split into two entries, and
  # both halves then fail the `archive/` prefix test, which counts as DESIGN-ROUTED
  # — the fail-closed direction.
  #
  # DELETIONS ARE NOT A ROUTING SIGNAL (`--diff-filter=d`, lowercase = EXCLUDE
  # deletions; #3751 round 1, F4). Because rename detection is pinned off — and it
  # must stay off, for the reasons above — a real `openspec archive` move shows up
  # as a DELETION from `openspec/changes/<slug>/` plus an ADDITION under
  # `archive/`. The addition is excluded below, so counting the deletion made every
  # archive-only finalize PR read design-routed and REFUSE for want of a C verdict:
  # a false refusal on correct, doctrine-mandated input, which is the guard agents
  # learn to waive. A path that is ONLY deleted also contributes nothing to audit —
  # there is no spec delta at the certified tree for C to anchor to. Every ADDED or
  # MODIFIED path under a live `openspec/changes/<slug>/` still routes to C, which
  # is the fail-closed half and is pinned by its own case in the suite.
  out=$(git -c diff.renames=false -c diff.relative=false \
    diff --diff-filter=d --name-only -z "$base" "$certified" -- ':(top)openspec/changes/' 2>/dev/null |
    tr '\0' '\n') ||
    rc=$?
  if [ "$rc" -ne 0 ]; then
    C_ROUTING=UNMEASURED
    C_ROUTING_DETAIL="git diff <merge-base>..<certified> -- openspec/changes/ failed"
    return 0
  fi
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    case "$p" in
      openspec/changes/archive/*) continue ;;
    esac
    hits=$((hits + 1))
    if [ -z "$slug" ]; then
      slug="${p#openspec/changes/}"
      slug="${slug%%/*}"
    fi
  done <<C_ROUTE
$out
C_ROUTE
  if [ "$hits" -gt 0 ]; then
    C_ROUTING=REQUIRED
    C_ROUTING_DETAIL="this branch touches openspec/changes/$slug ($hits path(s) vs merge-base ${base:0:12})"
  else
    C_ROUTING=NOT-APPLICABLE
    C_ROUTING_DETAIL="no openspec change on branch"
  fi
  return 0
}

# c_assert_head_binds_certified — AUTO locates the C stage in the CURRENT
# worktree, so before that stage is TRUSTED, this worktree must BE the one that
# was certified. The binding is HEAD-equality, and it is the whole answer to the
# question "whose artifact is this?".
#
# WHY RESOLVABILITY PROVES NOTHING. On this fleet EVERY lane is a worktree of ONE
# shared `.git` (measured: `/data/lanes/repo/.git/objects` serves lane-3544,
# lane-3473 and lane-3629 alike), so a PEER lane's certified commit RESOLVES from
# any lane — `git rev-parse <peer-sha>` succeeds, `git merge-base` succeeds, the
# routing diff succeeds. Every one of those reads can therefore be about a commit
# that has nothing to do with the `.review-stage/` records sitting in THIS
# directory. That is #3616's peer-artifact class (a closer read a peer lane's gate
# run dir by RECENCY and was about to merge on it), which this very file exists to
# refuse.
#
# WHY HEAD-EQUALITY IS SUFFICIENT. Rule 1 below already asserts the PR's
# `headRefOid` == the certified sha. So HEAD == certified binds this worktree's
# local artifact to THIS PR transitively: certified ties the artifact to the tree,
# and headRefOid ties the tree to the pull request being merged.
#
# CORRECT INPUT IS UNAFFECTED: the closer pushes and then asserts in the lane it
# just certified, where HEAD is the certified commit by construction. A worktree
# that has moved on since certification is not a lane whose stage may certify it.
# Unreadable HEAD is a REFUSAL, never a pass — "cannot tell" must not take the
# permissive branch.
c_assert_head_binds_certified() {
  local head=""
  if ! head=$(git rev-parse --verify --quiet 'HEAD^{commit}' 2>/dev/null) ||
    [ -z "$head" ]; then
    refuse_no_c_verdict \
      "AUTO locates the '$C_STAGE_KIND' stage in THIS worktree, but this checkout's HEAD" \
      "could not be read, so the stage cannot be BOUND to the certified commit." \
      "A stage whose provenance cannot be established may not certify a merge." \
      "Run this assert from the lane that produced the certified commit, or name the" \
      "verdict explicitly: --c-verdict <path>."
  fi
  head=$(printf '%s' "$head" | tr '[:upper:]' '[:lower:]')
  if [ "$head" != "$certified" ]; then
    refuse_no_c_verdict \
      "This worktree's HEAD ($head) is not the certified commit ($certified)," \
      "so a '$C_STAGE_KIND' stage found HERE is not evidence about the tree being merged." \
      "Every lane on this box is a worktree of ONE shared .git, so a peer lane's" \
      "certified commit RESOLVES from any lane — resolvability is not provenance. This is" \
      "the #3616 peer-artifact class: a stage record in this directory says nothing about" \
      "whose branch the certified sha belongs to." \
      "Remedy: run this assert in the lane that produced the certified commit (the closer" \
      "pushes, then asserts, in the lane it just certified), or name the verdict" \
      "explicitly with --c-verdict <path>."
  fi
}

# c_stage_root — the worktree root the AUTO stage lookup is relative to. ONE resolution,
# shared by the locator and the binding assert below, so the two cannot form two opinions
# about WHICH `.review-stage/` they are talking about. Called from a command substitution by
# the locator, so it reports and never refuses.
c_stage_root() {
  local root
  root=$(git rev-parse --show-toplevel 2>/dev/null) || root=""
  [ -n "$root" ] || root="$PWD"
  printf '%s\n' "$root"
}

# c_record_bytes <path> — ONE OBSERVATION OF THE STAGE RECORD (#3751 round 9, N2), in a form two
# reads can be compared for equality: a STATE marker on the first line, the file's bytes after it.
# Prints only that, never dies, so it is safe from any command substitution.
#
# THE COMPLETE READ IS ASSERTED AFFIRMATIVELY, not inferred from an exit status that is easy to
# lose inside a substitution: the sentinel `E` is printed by a SECOND command joined with `&&`, so
# a truncated or failed read cannot produce a value ending in it. A positive verdict requires an
# affirmative measurement, and "this record is the one that was validated" is a positive verdict.
#
# THE STATE MARKER EXISTS SO THAT "ABSENT" AND "EMPTY" ARE DIFFERENT OBSERVATIONS: both are the
# empty string once read, and they are not the same fact.
#
# `review-stage.sh` has a sibling function (`report_bytes`) over the REPORT. They are deliberately
# NOT a shared implementation, and the reason is that no agreement between them is required: each
# is used for EQUALITY WITHIN ONE PROCESS, over a DIFFERENT file, so a divergence cannot make the
# two tools disagree about anything — unlike `_c_verdict_awk` and `classify_report`, which read the
# same grammar and are therefore mechanically reconciled (section 44g).
#
# DECLARED LIMIT: bash DISCARDS NUL bytes in a command substitution, so a change consisting only of
# NUL bytes is not represented here. The stage record is text written by `review-stage.sh` and its
# every field is validated on the read side; stated rather than left as an unexamined blind spot.
c_record_bytes() {
  local p="$1" body rc=0
  if [ ! -f "$p" ]; then printf 'state=no-such-file\n'; return 0; fi
  # THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 13, S2) — a raw capture DROPPED NUL bytes, so a
  # record whose `head-sha:`/`report-nonce:` line carried one was parsed as a line it does not
  # hold. See `c_capture_map_nul`.
  #
  # AND THE COMPLETE READ IS ASSERTED BY *TWO* SIGNALS: the sentinel `E` survives a refactor that
  # folds this assignment into its `local` declaration (where the STATUS would become `local`'s),
  # and the STATUS catches what the sentinel cannot — a read that fails after delivering a prefix
  # whose last byte happens to BE an `E` is textually indistinguishable from a complete one, and
  # `${body%E}` would then eat a real byte. `|| rc=$?`, never `if ! …; then rc=$?`, which reads 0.
  body="$( { c_capture_map_nul "$p" && printf 'E'; } 2>/dev/null )" || rc=$?
  if [ "$rc" -ne 0 ]; then printf 'state=unreadable\n'; return 0; fi
  case "$body" in
    *E) ;;
    *) printf 'state=unreadable\n'; return 0 ;;
  esac
  body="${body%E}"
  # ITS OWN STATE, so the refusal names the byte rather than reporting a permission failure that
  # did not happen: the operator action is to rewrite the record (or re-open the stage), not chmod.
  case "$body" in
    *"$C_CAPTURE_NUL_BYTE"*) printf 'state=unrepresentable\n'; return 0 ;;
  esac
  printf 'state=present\n%s' "$body"
}

# _c_stage_record_awk — read a STAGE RECORD on stdin, print `key=value` lines.
#
# COLUMN-ZERO ANCHORED (`/^head-sha:[ \t]/`, `/^report-nonce:[ \t]/`) and every anchored line
# COUNTED, for the same reasons `_c_verdict_awk` above is: a first-wins read of several candidates
# is the rule this file refuses everywhere else, and an indented copy is DATA. ANSI/CR stripped as
# belt (#3400). `NF == 2` is required AFFIRMATIVELY of BOTH fields: the documented shapes are
# exactly `head-sha: <40-hex>` and `report-nonce: <token>`, so an empty value or trailing junk is
# UNPARSABLE and must not be reduced to its first word — a record that cannot state which tree it
# audited, or which report of it is current, certifies nothing.
#
# IT READS TWO FIELDS IN ONE PASS BECAUSE THEY ARE ASKED OF ONE OBSERVATION (#3751 round 10, P2).
# The head-sha binds the audit to the TREE (round 3, G1); the report-nonce names the GENERATION of
# the stage whose report the accepted verdict came from (round 6, K2). Both questions are asked of
# the single capture in `C_STAGE_RECORD`, so a second pass over a re-read file cannot make them
# two different facts. It was named `_c_stage_head_awk` while it read one field; the name moved
# with the content, because a comment that lies is worse than none.
_c_stage_record_awk() {
  awk '
  BEGIN { n = 0; v = ""; nn = 0; nv = "" }
  { gsub(/\033\[[0-9;]*[a-zA-Z]/, ""); sub(/\r$/, "") }
  /^head-sha:[ \t]/ {
    n++
    if (n == 1 && NF == 2) v = $2
  }
  /^report-nonce:[ \t]/ {
    nn++
    if (nn == 1 && NF == 2) nv = $2
  }
  END { print "n=" n; print "value=" v; print "nn=" nn; print "nonce=" nv }
'
}

# c_assert_stage_binds_certified <issue> — THE ARTIFACT, not the worktree (#3751 round 3, G1).
#
# `c_assert_head_binds_certified` above binds this WORKTREE to the certified commit. That is a
# different question from "is this ARTIFACT about the certified tree?", and the second one was
# unanswerable: the stage record carried no commit identity, so a `result: PASS` recorded
# BEFORE a further commit, an amend or a rebase persisted in `.review-stage/` and certified the
# NEW tree — with the HEAD check satisfied BY CONSTRUCTION, because the lane stands at the very
# commit it is certifying. Both checks are kept; neither replaces the other.
#
# WHY FAIL-CLOSED ON A MISSING OR UNPARSABLE FIELD. This is the gate-of-record rule (any src
# change after the gate INVALIDATES it) applied to the intent audit: an audit of an older tree
# may not certify a newer one, and a record that does not say WHICH tree it audited is not
# evidence about this one. So a record with no `head-sha:`, several of them, or a value that is
# not a 40-hex sha is a NAMED refusal — never a skip, because an older record predating the
# field would otherwise be readable as certifying, which is the permissive branch this whole
# file exists to remove. The remedy is always the same and is printed: re-open the stage
# (`--force` RE-STAMPS `head-sha`, deliberately unlike `spawned-at`) and re-run C.
#
# review-stage.sh's `open` is the SOLE writer of this record. This reader is deliberately
# STRICTER than that writer's own `read_field` (column zero, exactly one line, 40 hex or
# refuse), so a format drift refuses rather than passing — the fail-closed direction.
C_STAGE_HEAD=""
# THE OBSERVATION THE BINDING WAS VALIDATED AGAINST. Set by the assert below, consumed by
# `c_assert_stage_record_unchanged`. Empty means the binding never ran, which that function
# refuses rather than reading as "unchanged".
C_STAGE_RECORD=""
# THE GENERATION THE BINDING WAS VALIDATED ON (#3751 round 10, P2). `report-nonce:` names WHICH
# report of this stage is current, and it is read from the SAME capture as `head-sha:`. The COUNT
# is published beside the value because 0 (a legacy pre-nonce record), 1 and several are three
# DIFFERENT states and only one of them can be bound — see
# `c_assert_verdict_from_validated_generation`, which is where they are judged.
C_STAGE_NONCE=""
C_STAGE_NONCE_N=""
c_assert_stage_binds_certified() {
  local issue="$1" sfile out k v n="" value="" nn="" nonce=""
  sfile="$(c_stage_root)/.review-stage/issue-$issue/$C_STAGE_KIND.stage"
  if [ ! -f "$sfile" ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record for issue $issue is not a readable file:" \
      "  $sfile" \
      "It is the artifact that proves a stage was opened AND records the commit it was" \
      "opened at, so without it nothing binds the audit to the tree being merged." \
      "$C_REOPEN_REMEDY"
  fi
  # ONE READ OF THE RECORD, AND EVERY QUESTION IS ASKED OF THAT ONE OBSERVATION (#3751 round 9,
  # N2). The head-sha used to be parsed by reading `$sfile` again here; that is a SECOND read, and
  # two reads of one record are two different facts — so the value the binding validated could
  # already have come from a generation other than the one the re-verification below compares.
  # Capturing the bytes and parsing THOSE makes the binding and the comparison the same fact.
  C_STAGE_RECORD="$(c_record_bytes "$sfile")"
  case "$C_STAGE_RECORD" in
    'state=present'*) ;;
    'state=unrepresentable')
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' stage record for issue $issue holds a NUL 0x00 or SOH 0x01 byte, so it" \
        "is not a text record and nothing in it may be read as one:" \
        "  record: $sfile" \
        "A shell capture silently DROPS a NUL, so a reader would parse lines this file does not" \
        "hold — which is how a forged 'report-nonce:' could name another generation's report." \
        "$C_REOPEN_REMEDY"
      ;;
    *)
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' stage record for issue $issue could not be READ, so nothing binds the" \
        "audit to the tree being merged:" \
        "  record: $sfile" \
        "This is a permission or an I/O failure, not a missing stage — the file exists. It is a" \
        "NON-measurement, and a non-measurement is never read as a pass." \
        "$C_REOPEN_REMEDY"
      ;;
  esac
  # `printf '%s\n'` guarantees awk a terminated final line, whatever the file ended with.
  out=$(printf '%s\n' "${C_STAGE_RECORD#*$'\n'}" | _c_stage_record_awk) ||
    refuse_tool_failure awk "the C stage record's head-sha and report-nonce"
  while IFS='=' read -r k v; do
    case "$k" in
      n)     n="$v" ;;
      value) value="$v" ;;
      nn)    nn="$v" ;;
      nonce) nonce="$v" ;;
    esac
  done <<C_STAGE_HEAD_PARSE
$out
C_STAGE_HEAD_PARSE
  # PUBLISHED FROM THIS ONE PARSE, AND DELIBERATELY NOT JUDGED HERE (#3751 round 10, P2). The
  # generation is only ever compared against a value `review-stage.sh verdict` reports back, which
  # does not exist yet — and pre-empting that comparison with a refusal HERE would relabel the
  # seam section 44f(d) pins: an unusable `report-nonce:` is diagnosed by `review-stage.sh` itself
  # as `NOT-RUN (stage record unreadable: …)`, which names the field to repair, and this script
  # replacing that with a binding failure would be a worse diagnostic for the same refusal.
  C_STAGE_NONCE_N="$nn"
  C_STAGE_NONCE="$nonce"
  case "$n" in
    ''|*[!0-9]*)
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' stage record parse produced no usable count of head-sha: lines" \
        "— refusing (fail closed)." \
        "  record: $sfile"
      ;;
  esac
  if [ "$n" -eq 0 ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record carries NO 'head-sha:' field, so it does not say which" \
      "tree the audit was about:" \
      "  record: $sfile" \
      "This is the shape a record written before that field existed has, and it may NOT be" \
      "read as certifying: an intent audit of an OLDER tree does not certify a newer one" \
      "(the gate-of-record rule — any change after the audit invalidates it)." \
      "$C_REOPEN_REMEDY"
  fi
  if [ "$n" -gt 1 ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record carries $n 'head-sha:' fields — AMBIGUOUS, refusing" \
      "rather than picking one." \
      "  record: $sfile" \
      "Two answers to one question is not a binding, and a first-wins read is the rule this" \
      "file refuses everywhere else." \
      "$C_REOPEN_REMEDY"
  fi
  # 40 LOWERCASE HEX, ASSERTED AFFIRMATIVELY. `unresolved` is what review-stage.sh records
  # where HEAD is unborn — an honest NON-measurement, and a non-measurement is not a pass.
  case "$value" in
    *[!0-9a-f]* | "")
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' stage record's head-sha is '$value', which is not a 40-hex commit sha," \
        "so the audit cannot be bound to the tree being merged." \
        "  record: $sfile" \
        "review-stage.sh records 'unresolved' where the checkout had no resolvable HEAD; that" \
        "is an honest non-measurement, and a non-measurement is never read as a pass." \
        "$C_REOPEN_REMEDY"
      ;;
  esac
  if [ "${#value}" -ne 40 ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record's head-sha is '$value' (${#value} chars), not a full" \
      "40-char commit sha — an abbreviated value can never be safely compared." \
      "  record: $sfile" \
      "$C_REOPEN_REMEDY"
  fi
  if [ "$value" != "$certified" ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage was OPENED at commit $value, but the commit being certified" \
      "is $certified — so the recorded verdict is an audit of a DIFFERENT tree." \
      "  record: $sfile" \
      "This is the stale-artifact case: a PASS recorded before a further commit, an amend or a" \
      "rebase persists in .review-stage/, and this worktree's HEAD equals the certified sha BY" \
      "CONSTRUCTION, so the HEAD binding cannot see it. An intent audit of an older tree does" \
      "not certify a newer one — the gate-of-record rule (any change after the audit" \
      "invalidates it), applied to C." \
      "$C_REOPEN_REMEDY"
  fi
  C_STAGE_HEAD="$value"
}

# c_assert_stage_record_unchanged <issue> — THE CERTIFICATION RESTS ON ONE OBSERVATION OF THE
# RECORD (#3751 round 9, N2).
#
# THE DEFECT. `c_assert_stage_binds_certified` validates `head-sha` from the stage record; AUTO
# then invokes `review-stage.sh verdict`, which RE-READS that record to find which report is
# current (the `report-nonce:` of round 6's K2). An atomic replacement between the two reads makes
# the ACCEPTED verdict come from a different GENERATION of the stage — and potentially a different
# commit — than the `head-sha` that was validated. Measured: with a second generation swapped in,
# the success line read `C-VERDICT PASS ... stage-head=273cd3dff12c ... report:
# .../c.decoygenerationB.md`, i.e. it asserted a binding to a generation it never read, while the
# decoy's own head-sha was forty zeros. That defeats G1 and the nonce IN COMBINATION, which is the
# pair that stops a stale audit certifying a new tree.
#
# WHY A COMPARISON AND NOT A HANDOFF. The obvious alternative is to resolve the report ONCE here
# and pass it to `review-stage.sh verdict`. That is refused: round 4 (H2) DELETED `--report`
# precisely so that no caller and no data file can name which file holds a verdict, and
# re-introducing a path argument would rebuild that control channel from the other end. So the
# record is re-read and required to be BYTE-IDENTICAL to the observation the binding validated. The
# property that buys: either the record never changed (so `verdict` read the validated one), or it
# changed and this refuses. A change-and-change-back to IDENTICAL BYTES is harmless by definition —
# identical bytes mean the same nonce and the same head-sha, hence the same report.
#
# WHAT IT DOES NOT CLAIM: the REPORT could still change after `verdict` classified it. A verdict is
# a snapshot of a file at a time; that is inherent to reading one, and `review-stage.sh`'s own
# write path is what makes each such read atomic. This function is about the RECORD's generation.
#
# CHECKED BEFORE THE TOKEN IS PARSED, never after: a check placed after the token was produced
# could only report that the token came from somewhere unvalidated.
c_assert_stage_record_unchanged() {
  local issue="$1" sfile now
  sfile="$(c_stage_root)/.review-stage/issue-$issue/$C_STAGE_KIND.stage"
  if [ -z "$C_STAGE_RECORD" ]; then
    # FAIL CLOSED: no captured observation means the binding assert did not run, so there is
    # nothing to compare and no basis for a pass. Never read as "unchanged".
    refuse_tool_failure "c_assert_stage_record_unchanged (no captured stage-record observation)" \
      "the C stage verdict"
  fi
  now="$(c_record_bytes "$sfile")"
  if [ "$now" != "$C_STAGE_RECORD" ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record for issue $issue CHANGED while its verdict was being read:" \
      "  record: $sfile" \
      "The head-sha binding was validated against ONE observation of that record, and" \
      "review-stage.sh then read it AGAIN to find which report is current — so a replacement in" \
      "between hands back a verdict from a DIFFERENT generation of the stage, possibly bound to a" \
      "different commit, under a binding that was checked on the old one. Two reads of one record" \
      "are two different facts, and NOTHING is accepted from the second." \
      "The ordinary cause is a concurrent 'review-stage.sh open --force' (or a hand edit) landing" \
      "mid-check. Re-run this assert once the stage is quiescent." \
      "$C_REOPEN_REMEDY"
  fi
}

# c_assert_verdict_from_validated_generation <issue> — THE ACCEPTED VERDICT MUST DEMONSTRABLY COME
# FROM THE GENERATION THIS SCRIPT VALIDATED (#3751 round 10, P2).
#
# THE DEFECT. Round 9's N2 captures the stage record ONCE, validates `head-sha` from that capture,
# lets `review-stage.sh verdict` re-read the record to pick which report is current, and then
# re-compares the record's bytes. **An ABA REPLACEMENT DEFEATS THE BYTE COMPARISON**: the record
# can go from the validated generation A to a foreign generation B while `verdict` reads B, and
# back to A before the comparison runs. Both observations are then byte-identical and the check
# passes — while the ACCEPTED verdict came from B, which may be stale, may be another lane's, and
# may be bound to a different commit. Equality of two observations is not identity of the thing
# observed at a third instant; that is what "one observation" could not buy on its own.
#
# THE FIX BINDS THE VERDICT ITSELF, USING A VALUE THE VERDICT ALREADY REPORTS OUTWARD. Since round
# 6 (K2) the report path INCLUDES the generation's nonce (`<kind>.<nonce>.md`), and `verdict`
# publishes that path as its mandatory `report=` field. So the returned `report=` is required to
# name the nonce carried by THE CAPTURE the binding was validated on. ABA cannot satisfy that: a
# verdict read from B returns B's nonce, whatever the record says afterwards. "The record looks
# unchanged" becomes "the verdict I am accepting came from the generation I validated".
#
# NOTHING IS PASSED INTO review-stage.sh, AND THAT IS DELIBERATE — DO NOT "SIMPLIFY" IT INTO AN
# ARGUMENT. Round 4 (H2) DELETED `--report` so that no caller and no data file can name which file
# holds a verdict, and round 9 (N2) refused the same shortcut for this reason: an inbound path or
# nonce argument would rebuild that control channel from the other end. This reads a value OUT of
# the verdict line, which creates no such channel — the callee still decides which report is
# current, and this only checks that its answer is about the generation that was validated.
#
# THE COMPARISON IS AGAINST THE SAME SINGLE CAPTURE `head-sha` WAS PARSED FROM (`C_STAGE_NONCE`,
# published by `c_assert_stage_binds_certified`), never a fresh read: a re-read expectation would
# be a second fact, which is the defect one level down and exactly what round 9 removed.
#
# THE BYTE COMPARISON IS KEPT AS DEFENCE IN DEPTH, not replaced. It catches changes this cannot
# see — a `spawned-at`, `agent` or `deadline-secs` edit under the SAME nonce, and the record
# vanishing — and this catches the one it cannot. Neither contains the other.
#
# IT GATES ACCEPTANCE, WHICH IS THE ONLY THING THAT CAN CERTIFY. The caller invokes it for the two
# tokens the closed grammar lets proceed. Every other token already REFUSES, and where the token
# is non-accepting `review-stage.sh`'s OWN cause is the more precise operator action: an unusable
# `report-nonce:` arrives as `NOT-RUN (stage record unreadable: …)`, which names the field to
# repair, and section 44f(d) pins that seam on purpose. This is not a permissive branch — the
# tokens it does not gate do not proceed.
#
# EVERY STATE IT CANNOT BIND REFUSES, AND SAYS WHICH STATE IT WAS. A pass requires an affirmative
# match; there is no state in which an unbindable generation is read as bound.
#
# AUTO-ONLY BY CONSTRUCTION, not by omission. The `--c-verdict <path>` branch reads a verdict from a
# file the CALLER supplied and locates no stage at all (it reports `routing: NOT-CONSULTED`), so
# there is no captured record and no validated generation for a returned nonce to be compared
# against. That branch's own bar is the WHOLE-GRAMMAR validation in `c_parse_verdict` — kind by
# string equality, every mandatory key exactly once, every value measured — which is what it has
# instead. Adding a generation binding there would mean deriving the expected nonce from a stage
# this invocation never bound, i.e. asserting a relationship between two unrelated facts.
c_assert_verdict_from_validated_generation() {
  local issue="$1" sfile
  sfile="$(c_stage_root)/.review-stage/issue-$issue/$C_STAGE_KIND.stage"
  if [ -z "$C_STAGE_RECORD" ]; then
    # FAIL CLOSED: no captured observation means the binding assert did not run, so there is no
    # validated generation to bind to. Never read as bound.
    refuse_tool_failure \
      "c_assert_verdict_from_validated_generation (no captured stage-record observation)" \
      "the C stage verdict"
  fi
  case "$C_STAGE_NONCE_N" in
    ''|*[!0-9]*)
      # The parse produced no usable count. A non-measurement is never read as a pass.
      refuse_tool_failure \
        "c_assert_verdict_from_validated_generation (no usable count of report-nonce: lines)" \
        "the C stage verdict"
      ;;
  esac
  if [ "$C_STAGE_NONCE_N" -eq 0 ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record carries NO 'report-nonce:' field, so it does not say" \
      "which report of this stage is current — and the verdict just read therefore cannot be" \
      "bound to the generation whose head-sha was validated:" \
      "  record: $sfile" \
      "This is the shape a record written before that field existed has (the LEGACY bare" \
      "<kind>.md report). review-stage.sh still READS that shape, deliberately, so an old stage" \
      "reports rather than crashes — but a generation that cannot be named cannot be bound, and" \
      "an unbindable audit may not certify a merge. Re-open the stage: --force publishes the" \
      "report under a FRESH NONCE." \
      "$C_REOPEN_REMEDY"
  fi
  if [ "$C_STAGE_NONCE_N" -gt 1 ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage record carries $C_STAGE_NONCE_N 'report-nonce:' fields —" \
      "AMBIGUOUS, refusing rather than picking one." \
      "  record: $sfile" \
      "Two answers to 'which report of this stage is current' is not a generation, and a" \
      "first-wins read is the rule this file refuses everywhere else." \
      "$C_REOPEN_REMEDY"
  fi
  # ALPHANUMERIC AND NON-EMPTY, ASSERTED AFFIRMATIVELY. This reader is deliberately at least as
  # strict as review-stage.sh's own `nonce_is_valid`, so a format drift REFUSES rather than
  # passing — the same relationship this file's head-sha reader has to that writer. Its LENGTH
  # BOUNDS are deliberately NOT re-implemented here: they belong to review-stage.sh, and a second
  # copy of a bound is a second place for it to drift. What is required here is that the token
  # names exactly one generation.
  case "$C_STAGE_NONCE" in
    "" | *[!A-Za-z0-9]*)
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' stage record's report-nonce is '$C_STAGE_NONCE', which is not an" \
        "alphanumeric token, so the generation the verdict came from cannot be identified." \
        "  record: $sfile" \
        "$C_REOPEN_REMEDY"
      ;;
  esac
  # review-stage.sh's OWN honest non-measurement (round 6's K1 states): it could not derive a
  # report path at all, so its `report=` names no generation. A non-measurement is never a pass,
  # and it is named as ITS OWN state rather than folded into the mismatch below, because the
  # operator action differs — repair the record, not "a peer replaced your stage".
  if [ "$C_TOKEN_REPORT" = unresolved ]; then
    refuse_no_c_verdict \
      "The '$C_STAGE_KIND' stage verdict reports report=unresolved — review-stage.sh could not" \
      "derive WHICH report of this stage is current, so the verdict cannot be bound to the" \
      "generation whose head-sha was validated." \
      "  record: $sfile" \
      "The verdict line above names the record defect it observed; repair that field." \
      "$C_REOPEN_REMEDY"
  fi
  # THE AFFIRMATIVE MATCH. review-stage.sh's `report_path` prints `<dir>/<kind>.<nonce>.md`, so
  # this requires the RETURNED path to end in the validated generation's own name. The pattern's
  # payload is QUOTED (hence literal) and only the directory half is a glob: this script does not
  # reconstruct the path — that would be H2's deleted channel written from this side — it checks
  # that the callee's answer is about the generation that was validated.
  case "$C_TOKEN_REPORT" in
    */"$C_STAGE_KIND.$C_STAGE_NONCE.md") ;;
    *)
      refuse_no_c_verdict \
        "The '$C_STAGE_KIND' verdict just accepted names a report that does NOT belong to the" \
        "generation of the stage this script validated:" \
        "  validated generation: $C_STAGE_KIND.$C_STAGE_NONCE.md (from the report-nonce: of the record the head-sha binding read)" \
        "  verdict reported:     $C_TOKEN_REPORT" \
        "  record: $sfile" \
        "So the verdict came from a DIFFERENT generation of this stage — possibly a stale one," \
        "possibly another lane's, possibly one bound to a different commit — under a binding that" \
        "was checked on the validated one. The byte re-comparison cannot see this on its own: a" \
        "record replaced and put BACK (A to B to A) leaves two identical observations while the" \
        "verdict in between was read from B. Equality of two observations is not identity of the" \
        "thing observed at a third instant." \
        "The ordinary cause is a concurrent 'review-stage.sh open --force' (or a hand edit)" \
        "landing while this assert runs. Re-run it once the stage is quiescent." \
        "$C_REOPEN_REMEDY"
      ;;
  esac
}

# c_auto_locate_issue — find THE open C stage in this worktree, by its stage
# RECORD (`.review-stage/issue-<N>/<kind>.stage`), which is the artifact that
# proves a stage was opened at all. Prints the issue number, or nothing.
# Several stages is AMBIGUOUS and refuses: 1:1:1:1 puts exactly one issue in a
# worktree, so two records mean the caller is not where it thinks it is.
# IT REFUSES NOTHING ITSELF, BY DESIGN (#3751 round 2, S3). Its only caller is a COMMAND
# SUBSTITUTION, and an `exit` inside one terminates the SUBSHELL — so the AMBIGUOUS refusal
# raised here reached the top level ONLY because `set -e` propagates the status of a simple
# assignment whose substitution failed. That made a correct diagnostic depend on a shell option
# a later edit could disturb: with `set -e` off (or the call moved inside a condition, where it
# is suppressed) the refusal became an advisory PRINT followed by "No 'c' stage was ever
# OPENED" — the wrong diagnostic for two stages. So the ambiguity is REPORTED as a value and
# a status, and the CALLER refuses explicitly.
c_auto_locate_issue() {
  local root d n count=0 found=""
  root=$(c_stage_root)
  for d in "$root"/.review-stage/issue-*/"$C_STAGE_KIND".stage; do
    [ -f "$d" ] || continue
    n=$(basename "$(dirname "$d")")
    n="${n#issue-}"
    case "$n" in ''|*[!0-9]*) continue ;; esac
    count=$((count + 1))
    found="$n"
  done
  if [ "$count" -gt 1 ]; then
    printf 'AMBIGUOUS|%s|%s\n' "$count" "$root"
    return 3
  fi
  printf '%s\n' "$found"
}

# c_evaluate — the whole check, called once. Refuses, or leaves C_TOKEN holding a
# token that may proceed (PASS / AUTHOR-PERFORMED / NOT-APPLICABLE).
c_evaluate() {
  local rs issue out rc=0 arc=0 amb_count="" amb_root=""
  if [ "$c_verdict" != AUTO ]; then
    # AN EXPLICIT PATH. Routing is NOT consulted: a supplied path can only carry a
    # review-stage verdict token, and NOT-APPLICABLE is not in that closed
    # grammar, so no caller-supplied value can declare C inapplicable on a branch
    # that carries an OpenSpec change (a file asserting it is refused BELOW, as
    # an unrecognised token). Whether C was required is therefore irrelevant here:
    # a verdict was supplied and it is held to the same bar either way.
    C_ROUTING=NOT-CONSULTED
    C_ROUTING_DETAIL="an explicit verdict path was supplied, so routing was not consulted"
    C_SOURCE="file $c_verdict"
    if [ ! -f "$c_verdict" ]; then
      refuse_no_c_verdict \
        "--c-verdict names '$c_verdict', which does not exist (or is not a regular file)."
    fi
    if [ ! -r "$c_verdict" ]; then
      refuse_no_c_verdict "--c-verdict names '$c_verdict', which is not readable."
    fi
    if [ ! -s "$c_verdict" ]; then
      refuse_no_c_verdict \
        "--c-verdict names '$c_verdict', which is EMPTY — nothing was recorded." \
        "An empty file is the shape a redirect leaves when the command it captured never ran."
    fi
    c_parse_verdict file "$c_verdict" "C verdict file"
  else
    c_measure_routing
    if [ "$C_ROUTING" = NOT-APPLICABLE ]; then
      # AFFIRMATIVE, and it says WHAT WAS MEASURED. This is the one branch that
      # proceeds without a verdict, and it does so on a measurement that SUCCEEDED
      # — never on a failure to measure, which lands in UNMEASURED below.
      C_TOKEN=NOT-APPLICABLE
      C_SOURCE="AUTO (measured: $C_ROUTING_DETAIL)"
      return 0
    fi
    # REQUIRED, or UNMEASURED — which is treated as REQUIRED. Fail closed.
    C_SOURCE="AUTO (routing $C_ROUTING: $C_ROUTING_DETAIL)"
    rs=""
    if [ -n "$self_dir" ]; then
      rs="$self_dir/review-stage.sh"
    fi
    # Resolved from THIS script's own directory, with NO env override — #3312's
    # rule that the constrained party must not choose its own enforcer. A test
    # needing a different one substitutes the ARTIFACT in a scratch copy of the
    # tree, exactly as the base-staleness advisory is substituted.
    if [ -z "$rs" ] || [ ! -f "$rs" ]; then
      refuse_tool_failure "review-stage.sh (expected beside this script${self_dir:+ at $rs})" \
        "the C stage verdict"
    fi
    # BOUND BEFORE TRUSTED (#3751 round 1, F1). Checked here, immediately before
    # the local stage is located, and NOT before the routing measurement: routing
    # is taken against `$certified` EXPLICITLY, so it is correct about the merged
    # tree whatever this checkout's HEAD is, and refusing earlier would relabel
    # the UNMEASURED cause an operator needs to read.
    c_assert_head_binds_certified
    # THE STATUS IS CHECKED HERE, EXPLICITLY, and the refusal is raised in THIS shell (S3).
    # `issue=$(...) || arc=$?` is the correct idiom: `if ! issue=$(...)` would read `$?` as 0.
    arc=0
    issue=$(c_auto_locate_issue) || arc=$?
    if [ "$arc" -ne 0 ]; then
      case "$issue" in
        AMBIGUOUS\|*)
          amb_count=${issue#AMBIGUOUS|}; amb_count=${amb_count%%|*}
          amb_root=${issue#AMBIGUOUS|*|}
          refuse_no_c_verdict \
            "$amb_count '$C_STAGE_KIND' stage records exist under $amb_root/.review-stage/ — AMBIGUOUS." \
            "1:1:1:1 puts exactly ONE issue in a worktree, so two records mean this is not the" \
            "lane you think it is. Name the verdict explicitly: --c-verdict <path>."
          ;;
        *)
          # An unrecognised failure of the locator is a TOOL failure, never "no stage found":
          # "cannot tell" must not take the permissive branch.
          refuse_tool_failure "c_auto_locate_issue (exit $arc)" "the C stage verdict"
          ;;
      esac
    fi
    if [ -z "$issue" ]; then
      refuse_no_c_verdict \
        "No '$C_STAGE_KIND' stage was ever OPENED in this worktree (.review-stage/issue-*/$C_STAGE_KIND.stage)," \
        "so there is no verdict to read and nothing recorded that C was even attempted." \
        "This is the state review-stage.sh reports as 'NOT-RUN (stage never opened)'."
    fi
    # THE ARTIFACT IS BOUND BEFORE IT IS READ (G1). Checked here, after the issue is known
    # and BEFORE the verdict is read, so a stale record can never produce a token at all —
    # a check placed after the token was read could only report the staleness.
    c_assert_stage_binds_certified "$issue"
    out=$(bash "$rs" verdict "$C_STAGE_KIND" --issue "$issue" 2>/dev/null) || rc=$?
    # The LINE is the authority, not the exit status: one grammar, read in one
    # place. review-stage.sh's non-zero exits (4 FINDINGS / 5 NOT-RUN / 6
    # AUTHOR-PERFORMED) are by design, so a non-zero rc with a parseable line is
    # ORDINARY. Only an rc with NO line is a tool failure.
    if [ -z "$out" ]; then
      refuse_tool_failure "review-stage.sh verdict $C_STAGE_KIND --issue $issue (exit $rc, no output)" \
        "the C stage verdict"
    fi
    # ONE OBSERVATION, RE-VERIFIED BEFORE THE TOKEN EXISTS (#3751 round 9, N2). `verdict` re-read
    # the record to pick which report is current, so this requires that record to still be the one
    # whose head-sha was validated above; otherwise the token below would come from a generation
    # nothing bound to the certified tree — and the success line would name a binding it never read.
    c_assert_stage_record_unchanged "$issue"
    # The SOURCE names the verified binding, so a pasted success line shows that the stage
    # was bound to the certified tree rather than merely found in this directory.
    C_SOURCE="AUTO issue=$issue stage=$C_STAGE_KIND stage-head=${C_STAGE_HEAD:0:12} (routing $C_ROUTING)"
    c_parse_verdict text "$out" "C stage verdict for issue $issue"
    # AND THE VERDICT IS BOUND TO THE GENERATION THAT WAS VALIDATED (#3751 round 10, P2). The
    # byte re-comparison above is defeated by an ABA replacement — A to B and back to A leaves two
    # identical observations while `verdict` read B — so the returned `report=` field must name
    # the nonce carried by the capture the head-sha binding was validated on. Read OUT of the
    # verdict line, never passed IN (H2's deleted `--report` channel; see the function).
    #
    # RUN FOR THE TOKENS THAT WOULD PROCEED, because acceptance is the only thing that can
    # certify: every other token already refuses, and for those `review-stage.sh`'s own cause is
    # the more precise operator action (section 44f(d)'s seam). It is placed BEFORE the closed
    # grammar below, so no unbound generation ever reaches an accepting arm.
    case "$C_TOKEN" in
      PASS | AUTHOR-PERFORMED) c_assert_verdict_from_validated_generation "$issue" ;;
    esac
  fi

  # THE CLOSED GRAMMAR, MATCHED BY STRING EQUALITY ON THE FIRST WORD (#3544). awk
  # already gave us the first whitespace-delimited token after `RESULT:`, so this
  # is token-exact: `PASS-BUT-UNMEASURED` equals nothing in the set and refuses.
  case "$C_TOKEN" in
    PASS) return 0 ;;
    AUTHOR-PERFORMED) return 0 ;;
    FINDINGS)
      refuse_no_c_verdict \
        "The $C_STAGE_KIND stage reports FINDINGS — the intent audit found a blocking gap." \
        "An unmet or uncovered requirement BLOCKS the merge (CLAUDE.md's 'done' definition):" \
        "fix it, or get the lead's ruling, then re-run the stage."
      ;;
    NOT-RUN)
      refuse_no_c_verdict \
        "The $C_STAGE_KIND stage reports NOT-RUN — no verdict was ever recorded." \
        "NOT-RUN covers seven distinct states (sentinel-only, report absent, report unreadable," \
        "report empty, report ungrammatical, stage never opened, stage record unreadable) and the" \
        "verdict line above NAMES which, because the operator action differs per cause."
      ;;
    '')
      refuse_no_c_verdict \
        "The $C_STAGE_KIND verdict line carries NO token after 'RESULT:'."
      ;;
    *)
      refuse_no_c_verdict \
        "The $C_STAGE_KIND verdict token is '$C_TOKEN', which is not in the closed set" \
        "{PASS, FINDINGS, NOT-RUN, AUTHOR-PERFORMED}. An unrecognised token is NEVER passed" \
        "through and never read as a pass: this is where a hand-written 'NOT-APPLICABLE' or" \
        "'PASS-BUT-UNMEASURED' lands. Only AUTO's MEASUREMENT of the branch can make C" \
        "inapplicable — never a value a caller supplies."
      ;;
  esac
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

# THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 14, T1). Round 13 (S2) routed
# `c_parse_verdict`'s file read and `c_record_bytes` through `c_capture_map_nul`
# and left THIS reader — the GATE-OF-RECORD summary parser, the other half of the
# merge gate — reading the file RAW. Measured on the shipped script: a summary
# holding `RESULT: PA<NUL>SS` yielded `v_result=PA<NUL>SS` from awk (gawk passes a
# NUL through a field) and the capture of awk's OUTPUT in `gate_parse_file` then
# REMOVED it, so this script read `PASS`. Same defect, third site — a boundary
# introduced with one path left bypassing it, which is round 7 and round 13 over
# again. The `#3400` REDIRECTION rule this comment used to open with is about a
# piped `while read` losing its verdict in a subshell; `pipefail` is set, so an
# unreadable file still fails this pipeline and reaches `refuse_tool_failure`
# exactly as the redirection did.
#
# THE BYTE IS NOT NAMED BY ITS OWN STATE HERE, DELIBERATELY, AND THAT IS WEAKER
# THAN `c_record_bytes` — declared rather than implied. Every field this parser
# publishes is matched AFFIRMATIVELY downstream (block markers by whole-line
# equality, `RESULT:`/`tree-integrity:`/`dirty:` against closed sets, `commit:`/
# `tree-start:` as hex against the certified sha), so a mapped SOH makes each of
# them refuse, and the refusal RENDERS the value through `c_safe_display`, which
# shows SOH as a visible `?` — the operator sees `PA?SS`. A dedicated state would
# buy a better sentence, not a different verdict, and this parser feeds two files
# through one path.
#
# One awk pass:
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
# WANT selects which family is "the block": full (default) or delta. It is now
# the ONLY argument — the file operand went with the redirection, and a parameter
# nothing reads is a parameter a later caller passes wrongly.
_gate_awk() {
  awk -v WANT="$1" '
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
'
}

# gate_parse_file <file> <want> <what> — run the parse and publish its fields as
# GP_* globals (bash 3.2: no namerefs, no associative arrays). Every COUNT is
# validated as a non-negative integer here, keyed on its AFFIRMATIVE value: an
# unparseable/absent count is refused, never treated as "no problem found".
gate_parse_file() {
  local gp_out gp_k gp_v
  # THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 14, T1) — see `_gate_awk`. The
  # mapping is applied HERE, at the read, rather than inside the parser, because
  # the parser is a pure stdin filter and this is the one place a FILE is opened.
  gp_out=$(c_capture_map_nul "$1" | _gate_awk "$2") || refuse_tool_failure awk "$3"
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

# THE C (INTENT AUDIT) VERDICT (#3751) — offline, and checked BEFORE the advisory
# and the `gh` call for the same reason the gate of record is: "you have no C
# verdict" must be reportable without a network round trip. It runs AFTER the
# gate-of-record half so that a run with no gate at all is still reported as the
# more fundamental failure first.
c_evaluate

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
# EVERY DATA VALUE ON AN EMITTED LINE GOES THROUGH THE ONE BOUNDARY (#3751 round 7, L1).
# `commit:`/`tree-start:`/`dirty:` are hex- and closed-token-validated before this point and
# the summary path is the invoker's own argv, so none of them is a route on its own — they are
# routed anyway because ONE rule applied to EVERY value on these lines is what stops the next
# added field being the one that forgot (round 2's S1 reasoning, and the reason L1 existed at
# all: three sites in this class have now been found one at a time). Display-only, as ever.
printf 'PREMERGE: GATE-OF-RECORD commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
  "$(c_safe_display "$full_commit")" "$(c_safe_display "$full_ts")" \
  "$(c_safe_display "$full_dirty")" "$(c_safe_display "$summary_file")"
if [ -n "$delta_file" ]; then
  printf 'PREMERGE: DELTA-RECERT anchor: %s commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
    "$(c_safe_display "$delta_anchor")" "$(c_safe_display "$delta_commit")" \
    "$(c_safe_display "$delta_ts")" "$(c_safe_display "$delta_dirty")" \
    "$(c_safe_display "$delta_file")"
fi
# THE C VERDICT IS REPORTED UNDER ITS OWN TOKEN, NEVER FOLDED INTO `OK` (#3751).
# `PREMERGE: OK` says the head matches and a gate of record covers it; it says
# NOTHING about who performed the intent audit. So the token is printed here, on
# its own line, and `AUTHOR-PERFORMED` — the disclosed hand-audit substitute — is
# textually distinct from `PASS` for the same reason the roborev wrapper's
# `WAIVED` is: nobody grepping the passing token may read a substitute as the real
# thing.
# THE SAME EMIT BOUNDARY ON THE SUCCESS PATH (#3751 round 5, J3). `C_TOKEN` and `C_TOKEN_REPORT`
# are read from a file a delegated agent wrote, and a PASSING run's line is the one that gets
# pasted into a PR comment — so it is neutralised exactly as the refusal's is. The token has
# already been compared by string equality against the closed set at this point, so this is
# display-only and cannot change the verdict.
#
# `C_SOURCE` WAS THE THIRD SITE OF THIS CLASS FOUND ONE AT A TIME (#3751 round 7, L1), and it is
# NOT merely invoker text: on the `AUTO` path it carries `C_ROUTING_DETAIL`, which interpolates a
# `openspec/changes/<slug>` path measured out of the certified tree — and GIT PERMITS NEWLINES IN
# PATHS, exactly the class `base-staleness.sh` sanitizes for. Unrouted, such a path emits a second
# line with no `PREMERGE: ` prefix inside the one block a human reads as the merge verdict. Hence
# the STRUCTURAL guard beside this rule — `scripts/tests/lib/emit-boundary-scan.sh`, run by both
# suites — because patching site three and waiting for site four is the wrong shape.
# `C_STAGE_KIND` is the ONE unrouted value here and it is a script CONSTANT (`c`), not data.
printf 'PREMERGE: C-VERDICT %s stage: %s source: %s%s\n' \
  "$(c_safe_display "$C_TOKEN")" "$C_STAGE_KIND" "$(c_safe_display "$C_SOURCE")" \
  "${C_TOKEN_REPORT:+ report: $(c_safe_display "$C_TOKEN_REPORT")}"
if [ "$C_TOKEN" = AUTHOR-PERFORMED ]; then
  printf 'PREMERGE: C-VERDICT-NOTE the intent audit was performed by the diff'"'"'s AUTHOR, not\n'
  printf 'PREMERGE: C-VERDICT-NOTE independently: an author'"'"'s hand audit is not an independent\n'
  printf 'PREMERGE: C-VERDICT-NOTE one; weight it accordingly. It is the SANCTIONED FALLBACK and\n'
  printf 'PREMERGE: C-VERDICT-NOTE is recorded with its working (review-stage.sh\n'
  printf 'PREMERGE: C-VERDICT-NOTE record-author-performed), which is why it is acceptable at\n'
  printf 'PREMERGE: C-VERDICT-NOTE all — an absent audit is not auditable, a disclosed one is.\n'
fi
exit 0
