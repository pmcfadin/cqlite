#!/usr/bin/env bash
# roborev-review-checks.sh — the five per-review checks behind roborev-review.sh (#2964).
#
# SOURCED, never executed: each function runs inside the wrapper's scope, reads its
# state (LOG, CENSUS, BASE, census_code_paths, the JOB_* facts, the threshold
# constants) and sets the summary key it owns. Same pattern as
# roborev-review-oracles.sh, which holds the two LOCAL oracles (push assert + census).
#
# The division of labour: the ORACLES file answers "what must be reviewed, and is it
# even reviewable" from data we obtain ourselves; THIS file answers "did a review of
# that actually happen" from the job record and the transcript. Two DETERMINISTIC
# checks carry the verdict — review-completed (positive evidence a review finished) and
# prompt-content (our census's own paths inside the prompt actually sent) — with
# findings-gated prose matching (tier 1) and token accounting (tier 2) corroborating.
#
# Split out of the wrapper at 998 lines to stay near the ~800 campsite guidance; the
# wrapper FAILS CLOSED if this file is missing or does not define all five functions,
# because a silently absent checks file would turn every one of them into a no-op while
# the block still read PASS.
#
# Self-test: scripts/tests/test_roborev_review_guard.sh.

# shellcheck disable=SC2034
# ^ every variable assigned here (REVIEW_COMPLETED, PROMPT_CONTENT, FINDINGS, TIER1,
#   TIER2, TOKENS, ROBOREV_EXIT) is READ by the sourcing wrapper, which shellcheck
#   cannot see when it lints this fragment standalone. Lint the set together with
#   `shellcheck -x scripts/flow/roborev-review.sh`.

# --- the SHARED findings-count recogniser (sourced) ---------------------------
# ONE definition of "how many findings does this review text report", shared with
# scripts/flow/premerge-review-binding.sh (issue #4050): #3626's deferral grants only
# when the marker's `count=` EQUALS the count the review observed, and that equality is
# asserted at BOTH ends — so a second implementation of the recogniser would be a second
# place for the two ends to disagree, and a disagreement there is an authorization bypass
# in one direction or the other.
#
# `-f` AS WELL AS `-r`, and the guard is the SAME predicate premerge-review-binding.sh
# uses on this SAME library (pinned byte-identical by scripts/tests/test_roborev_review_guard.sh).
# `.` on a FIFO would BLOCK FOREVER waiting for a writer and `-r` is TRUE for one —
# measured elsewhere in this repo as `timeout 10` -> rc 124 with NO diagnostic at all,
# i.e. a verdict-less stall. A socket, a device or a directory is the same class and `-f`
# is false for every one of them, so ONE predicate covers the class rather than a list of
# types to keep complete. Both predicates FOLLOW a symlink, which is deliberate: a
# symlinked checkout is a legitimate layout. THIS EXPOSURE IS NEW WITH THE EXTRACTION —
# before it the recogniser was inline here and there was no `source` to guard (#3822
# clause 12).
#
# A FAILURE TO LOAD IT IS FAIL-CLOSED THROUGH AN EXISTING MECHANISM, AND DELIBERATELY
# DOES NOT `return`: the wrapper runs under `set -e`, so a non-zero source would abort it
# with exit 1 and NO summary block at all — a verdict-less exit, which is the one thing
# this repo's gate scripts may never do. Instead the cause is named on stderr and
# `roborev_findings_count` simply stays undefined; the wrapper already refuses to proceed
# unless every required function exists and that name is enrolled in the same list, so the
# result is a named ERROR + `finish FAIL 1` before any review is enqueued — never a
# silently no-op findings check.
# THE RESOLUTION CANNOT ABORT THE WRAPPER. It runs under `set -e`, where an assignment whose
# command substitution fails is FATAL — and a fatal exit here is a verdict-less exit, with no
# summary block at all. So a failed `cd` degrades to an EMPTY directory, whose guard then fails
# and routes to the named, block-emitting refusal below.
_rfc_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)" || _rfc_dir=""
ROBOREV_FINDINGS_COUNT_LIB="$_rfc_dir/lib/roborev-findings-count.sh"
# THE SOURCE ITSELF IS CONDITIONAL, not just the readability probe (roborev job 123):
# a readable-but-CORRUPT library makes `.` return non-zero, and a bare `.` under a
# caller's `set -e` would kill the wrapper before its required-function check could fail
# closed. Both failures land on the SAME diagnostic, because both leave the recogniser
# undefined and the remedy is identical; the wording covers both causes.
_rfc_lib_loaded=0
if { [ -f "$ROBOREV_FINDINGS_COUNT_LIB" ] && [ -r "$ROBOREV_FINDINGS_COUNT_LIB" ]; }; then
  # shellcheck source=lib/roborev-findings-count.sh
  if . "$ROBOREV_FINDINGS_COUNT_LIB"; then _rfc_lib_loaded=1; fi
fi
if [ "$_rfc_lib_loaded" -eq 1 ]; then
  :
else
  printf '%s\n' "roborev-review-checks.sh: cannot read or source $ROBOREV_FINDINGS_COUNT_LIB (the shared findings-count recogniser, #4050 — absent, non-regular, unreadable, or corrupt) — the findings count CANNOT be measured; the wrapper's required-function check will fail closed on roborev_findings_count" >&2
fi

roborev_check_review_completed() {
  # --- step 6a: review-completed — POSITIVE evidence that a review HAPPENED ------
  # The allow-list of terminal verdict markers. A review that finished emits either a
  # findings block (a Findings heading / '**Severity**:' lines) or a Summary heading —
  # the shapes a real review actually emits. Anything else — a still-waiting job, a provider 400, a failed
  # job — matches NOTHING here and therefore cannot reach PASS. This is the inverse of
  # the old logic, which inferred success from the ABSENCE of a vacuous phrase.
  # Built from the REAL transcript (MEASURED, issue #2964 round 5), not from guesses:
  #     ## Review Findings
  #     - **Severity**: Medium
  #     ## Summary
  # A finished review is identified by a Findings heading, a `**Severity**:` line, a
  # Summary heading/label, or the older bracket / `Medium:` shapes other agents emit.
  # Anything else — a still-waiting job, a provider 400, a failed job — matches none of
  # them. The previous list was INVENTED rather than measured and rejected a GENUINE
  # codex review: the false-FAIL direction that gets a guard bypassed.
  VERDICT_MARKER_RE='^[[:space:]]*#{1,4}[[:space:]]*(review[[:space:]]+)?findings?|\*\*severity\*\*[[:space:]]*:|^[[:space:]]*#{1,4}[[:space:]]*summary|(^|[^[:alnum:]])summary:|\[(critical|high|medium|low)\]|(^|[^[:alnum:]])(critical|high|medium|low): |^[[:space:]]*findings?[[:space:]:]'
  if [ ! -r "$LOG" ]; then
    REVIEW_COMPLETED="FAIL (transcript unreadable)"
    DETAILS+=("ERROR: review-completed: the transcript at $LOG is not readable, so there is no evidence a review happened. Failing closed.")
  else
    verdict_marker=0
    if grep -qiE "$VERDICT_MARKER_RE" "$LOG"; then
      verdict_marker=1
    fi
    if [ -n "$JOB_STATUS" ] && [ "$JOB_STATUS" != "done" ]; then
      REVIEW_COMPLETED="FAIL (job status '$JOB_STATUS' is not done)"
      DETAILS+=("ERROR: review-completed: the job record reports status '$JOB_STATUS', not 'done', so the review did NOT complete and nothing was certified. Failing closed — the absence of a vacuous phrase is never evidence that a review happened.")
    elif [ "$verdict_marker" -eq 0 ]; then
      REVIEW_COMPLETED="FAIL (no terminal verdict marker)"
      DETAILS+=("ERROR: review-completed: the transcript carries NO terminal verdict marker — no Findings heading, no '**Severity**:' line and no Summary heading/label. A still-waiting job, a provider error (for example the #2433/#3037 model-mismatch 400) and a failed job all look like this, and none of them is a review. Failing closed. Transcript: $LOG")
    else
      REVIEW_COMPLETED="PASS"
      if [ -z "$JOB_STATUS" ]; then
        DETAILS+=("NOTICE: review-completed: the job record's 'status' was unavailable, so completion rests on the transcript's terminal verdict marker alone (the weaker of the two signals).")
      fi
    fi
  fi
}

roborev_check_prompt_content() {
  # --- step 6b: prompt-content — DETERMINISTIC: did the reviewer GET the diff? ----
  # The strongest available check: it reads the prompt actually sent to the agent and
  # looks for OUR census's own file paths in it. Absent paths mean the reviewer never
  # received the diff — the T1/T2 family and any future variant — threshold-free, and
  # judged against our own authoritative census rather than the reviewer's prose.
  # A whitespace-only prompt file is a RETRIEVAL FAILURE — reported distinctly from
  # "the paths are absent", but still a FAIL: see below.
  prompt_bytes=$(tr -d '[:space:]' <"$PROMPT_FILE" | wc -c | tr -d '[:space:]')
  if [ "${prompt_bytes:-0}" -eq 0 ]; then
    # FAIL, not a non-failing UNAVAILABLE (codex, round 6 — BLOCKER). With a NON-EMPTY
    # code census, an unretrievable prompt means there is NO authoritative evidence the
    # reviewer received any diff, and reporting PASS on that basis contradicts the
    # wrapper's entire purpose. It is also not a plausible always-red risk: the prompt is
    # measurably retrievable from the job record's `prompt` field AND from
    # `roborev show <job> --prompt`, so an empty one is a real anomaly.
    PROMPT_CONTENT="FAIL (prompt unretrievable — no evidence any diff was delivered)"
    DETAILS+=("ERROR: prompt-content: the prompt sent to the reviewer could not be retrieved for job '$JOB' (tried the job record's 'prompt' field, then 'roborev show <job> --prompt'), so there is NO authoritative evidence the reviewer received the ${#census_code_paths[@]} code file(s) in this census. Failing closed: a pass here would rest on nothing.")
  else
    # Check the CODE subset of the census, not every path. THE MECHANISM, stated
    # correctly (#3229): **roborev drops exactly what its configured `exclude_patterns`
    # pathspecs match — it makes NO code/non-code judgement of its own.** MEASURED
    # (issue #2964, round 5): on a census of 22 markdown + 5 code files the prompt
    # carried diff headers for exactly the 5 code files, because `*.md` is CONFIGURED —
    # not because the reviewer recognised prose. Checking all 27 would therefore
    # false-FAIL every branch that touches documentation, which is most of them.
    # The CODE subset is the right subset only because this repo's configured set is a
    # prose/artifact deny-list that MIRRORS the census classification. That correspondence is
    # NOT verified pre-enqueue — the oracle that tried is removed (#3283) — so this check IS
    # where a divergence surfaces: a configured pattern that swallows a code path (which
    # `docs/**` did, for 33 executables, on PR #3222) lands here as a FAIL on the paths the
    # reviewer never received. Fail-closed, but AFTER the review round has been paid for.
    # EVERY code path is checked against the prompt's actual `diff --git` HEADERS, never a
    # bare substring (codex, round 5): sampling let a partial prompt pass by naming the
    # sampled files, and a substring match is satisfied by any incidental mention —
    # including this wrapper quoting a path in a comment.
    #
    # BOTH header sides are collected (codex, round 6 — BLOCKER): our census runs with
    # `--no-renames`, so a rename is two paths (old + new), while the reviewer's diff may
    # have rename detection ON and emit a single `diff --git a/old b/new` header. Matching
    # only same-path headers therefore FALSELY REJECTED any review containing a detected
    # rename. Extracting the path set from both sides and comparing whole-line makes the
    # two rename behaviours agree without weakening the check to a substring test.
    #
    # MEMBERSHIP IS DECIDED PER HEADER, BY THE CANONICAL HELPER — no regex, no path-set
    # file, no `grep` (#3229 round 4, blockers F2 + F3). This consumer used to build its
    # own path set with `grep -oE '^diff --git a/[^ ]+ b/[^ ]+$'` plus a both-sides-quoted
    # parse, and then probe it with `grep -Fxq` over a NEWLINE-delimited file. Every one of
    # those three mechanisms was wrong on real input: the regex cannot split a
    # SPACE-bearing header, the quoted parse could not read a MIXED header
    # (`diff --git a/ascii "b/quoted"`, which git emits on a rename), and a
    # newline-delimited set turns a newline-bearing path into ALTERNATIVES (a false PASS).
    #
    # It now asks `roborev_diff_header_has_path` — the SINGLE implementation, in
    # `roborev-review-oracles.sh` beside the normalisation boundary it belongs to — one
    # header at a time. This file performs NO unquoting and knows NOTHING about header
    # shapes; the guard suite asserts that structurally. Census paths arrive RAW from the
    # census's `--numstat -z`, so there is nothing to normalise on this side either.
    #
    # The headers are collected by the oracles file too (#3229 round 5, blocker 1), because
    # a `diff --git` header LINE is irreducibly ambiguous once a path may contain a space
    # and the matcher resolves that ambiguity from the header's OWN `rename from` /
    # `rename to` lines. Which lines those are, and how far the extended-header run
    # extends, is header-shape knowledge — so it lives with the matcher, not here, and this
    # file just carries the three parallel arrays through.
    # ===== ONE QUESTION, NO CLASSIFIER (owner ruling (4), issue #3312) =====
    # Are the census CODE paths present in the prompt the reviewer was sent? That is the whole
    # check. It does NOT ask, and can no longer express, HOW roborev delivered the diff: four
    # consecutive review rounds each found a High-severity false verdict in that inference, whose
    # single cause was inferring structure from prompt text that embeds repository-controlled
    # content. Absence is now a FAIL whatever produced it — a snapshot-delivered diff, a delegated
    # tier, a vacuous review that received nothing — and the only way past it is a human-authorized,
    # sha-bound waiver recorded in the block (see `roborev_absence_waiver_lookup`).
    roborev_collect_prompt_headers "$PROMPT_FILE"
    # EVERY code census path is expected in the prompt. There is NO subtraction and no
    # excusal: NO exclusion set is modelled anywhere in this wrapper (#3283 for the
    # configured half, #3278 for roborev's compiled-in deny-list), so nothing here is
    # licensed to say "do not expect this path". A path the reviewer really did not get
    # therefore FAILs — the fail-closed direction — whether it was eaten by configuration,
    # by a built-in, or by anything else. The cost is diagnostic: the cause names the
    # symptom ("the reviewer did not receive this path") rather than the mechanism. See the
    # exclusion note near the top of `roborev-review-oracles.sh`.
    checked_paths=("${census_code_paths[@]}")
    census_total=${#checked_paths[@]}
    missing_paths=()
    for census_path in ${checked_paths[@]+"${checked_paths[@]}"}; do
      found=0
      for ((hdr_i = 0; hdr_i < ${#_rx_hdrs[@]}; hdr_i++)); do
        if roborev_diff_header_has_path "${_rx_hdrs[$hdr_i]}" "$census_path" \
          "${_rx_hdr_from[$hdr_i]}" "${_rx_hdr_to[$hdr_i]}"; then
          found=1
          break
        fi
      done
      [ "$found" -eq 1 ] || missing_paths+=("$census_path")
    done
    if [ "$census_total" -eq 0 ]; then
      # A `0/0` IS NEVER A PASS (codex round 7 — BLOCKER, #3229). Belt-and-braces behind
      # `code-free:`, which FAILs pre-enqueue on a census with no CODE path at all: with
      # nothing to check there is no evidence whatsoever that the reviewer received a diff,
      # and `PASS (0/0 code census paths present)` is textually indistinguishable from a
      # genuine pass. Refuse to print one — if this key has no subject, it has no verdict to
      # give. Kept as a STRUCTURAL backstop even though it is unreachable through the normal
      # ordering: the whole point is that it does not depend on an upstream check still being
      # there.
      PROMPT_CONTENT="FAIL (no code census path was checkable — a 0/0 is never a pass)"
      DETAILS+=("ERROR: prompt-content: there is not one CODE census path to look for in the prompt (census code paths: ${#census_code_paths[@]}), so this key has NO subject and therefore no verdict to give. Failing closed: 'PASS (0/0 code census paths present)' would be textually identical to a genuine pass while the reviewer received an EMPTY prompt. See code-free:, which fails pre-enqueue for the same reason.")
    elif [ "${#missing_paths[@]}" -gt 0 ]; then
      # ===== ABSENCE IS A FAIL, AND ONLY A HUMAN CAN EXCUSE IT (owner ruling (4)) =====
      # The waiver is looked up ONLY here, so it can excuse ONLY this verdict: every other cause this
      # wrapper can report — an unretrievable prompt, a 0/0 census, a failed sha assert, a review that
      # never completed — is reached on a different path and is untouched by it. A `WAIVED` token
      # therefore always means "the census paths were absent and a named human accepted that", never
      # anything else.
      # BOUND TO THE MERGE-BASE, THE SAME BASE `sha-assert` COMPARES AGAINST (#3392). The scope
      # is the REVIEWED RANGE, and that range is `merge-base..HEAD`; binding it to the base ref's
      # TIP gave the waiver the identical staleness defect the assert had — a waiver written for a
      # failing run went spuriously STALE on `--recheck-job` the moment the base ref advanced,
      # which re-deadlettered the #3312 break-glass exactly when the fleet is busiest. Nothing is
      # weakened: base AND head AND job are all still required and all still verified, and the
      # base is now the one sha that actually identifies the reviewed range.
      roborev_absence_waiver_lookup "${RANGE_BASE_SHA:-}" "${HEAD_SHA:-}" "${JOB:-}"
      WAIVER_REPORT="${ROBOREV_WAIVER_STATE}"
      if [ "$ROBOREV_WAIVER_STATE" = "granted" ]; then
        # A DISTINCT VERDICT TOKEN, deliberately not `PASS (waived …)`: every reader that greps
        # `^prompt-content: PASS` — this suite, closers, agents pasting blocks — must NOT see a waived
        # run as certified. `WAIVED` is admitted by the wrapper's affirmation backstop only when the whole
        # scope and the reason are present and match, so it can never be a silent placeholder.
        PROMPT_CONTENT="WAIVED (${#missing_paths[@]}/${#checked_paths[@]} code census paths absent — authorized by @${ROBOREV_WAIVER_AUTHOR} for ${ROBOREV_WAIVER_SCOPE})"
        WAIVER_REPORT="GRANTED (author=@${ROBOREV_WAIVER_AUTHOR} ${ROBOREV_WAIVER_SCOPE} reason=${ROBOREV_WAIVER_REASON})"
        DETAILS+=("NOTICE: prompt-content: ${#missing_paths[@]} of the ${#checked_paths[@]} CODE census paths are ABSENT from the prompt actually sent to the reviewer, and that FAIL is WAIVED by a PR comment naming THIS review — ${ROBOREV_WAIVER_SCOPE}. Authorizer as recorded by GitHub: @${ROBOREV_WAIVER_AUTHOR}. Reason as given: ${ROBOREV_WAIVER_REASON}. The waiver is bound to the whole review scope (base AND head AND job), so it cannot outlive the review its authorizer judged: a push, a different base or a re-run all require a fresh one. THE AUTHOR IS AUTHORIZED AGAINST AN EXPLICIT ALLOWLIST, and beyond that authorship is PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY VERIFIED: a comment from anyone outside the allowlist cannot grant (this is a public repository, and the base/head/job values are printed in the failing block), but on this fleet the worker, the closer and the owner all post through the SAME login, so this wrapper cannot tell WHICH ALLOWLISTED HUMAN posted this comment — the ruling that only the owner or the coordination lead may grant it rests on process, and on this comment being permanently attributable. Absent paths (first 10):")
      else
        PROMPT_CONTENT="FAIL (${#missing_paths[@]}/${#checked_paths[@]} code census paths absent from the prompt)"
        case "$ROBOREV_WAIVER_STATE" in
          # NOT EVEN THE MARKER PREFIX IS PRINTED (layer 3, job 23): no emitted diagnostic may carry any
          # part of the marker, so no pasted block can be mistaken for one, and the rule is assertable.
          # THIS COMMENT WAS FALSE UNTIL #3626 (roborev job 225). The `*)` branch below interpolates
          # `ROBOREV_WAIVER_DETAIL`, which comes from the scanner — and for a MALFORMED marker the
          # scanner's detail QUOTED THE WHOLE REQUIRED FORM. So this block printed a complete, fillable
          # marker beside a live base/head/job while the comment two lines up asserted it never did. A
          # comment asserting a property the code violates is worse than no comment: it is what stops
          # the next reader checking. The form now lives ONLY in `--help`; the scanner's
          # `MALFORMED_FORM_DETAIL` is what makes that true for BOTH kinds, and the guard suite asserts
          # the absence against every diagnostic-emitting case rather than only the one where it holds
          # trivially.
          # THE CAUSE TEACHES BOTH RULES, not just the absence (#3312 jobs 27/29): the marker must be the
          # SOLE NONBLANK CONTENT of the comment, and the comment must be TOP-LEVEL. An authorizer told
          # merely "no waiver line exists" re-checks their SYNTAX — not the shape of the comment or the
          # channel — and concludes the mechanism is broken. Both rules are load-bearing and both are
          # invisible from a syntactically perfect marker, so the diagnostic states them.
          # THE `none` CAUSE ALSO DECLARES WHETHER THE LINKED-ISSUE THREAD WAS CHECKED (#3759).
          # `NONE` used to be silent about it, so "checked and the marker is not there either" and
          # "never checked" read identically — the same shape as a lane that omits coverage
          # silently being indistinguishable from one that covers it. The declaration comes from
          # the probe's CLOSED rendering set; the `:-` fallback is itself a could-not-check
          # rendering, so a path that reached `none` without running the probe says so rather than
          # implying a completed check.
          none) WAIVER_REPORT="NONE (no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — a marker inside prose, a code fence, a quote or a review body is not read; ${ROBOREV_WAIVER_DETAIL:-the linked-issue thread could NOT be checked: the probe was not reached on this path})" ;;
          # ===== A DEDICATED ARM, NOT A FALL-THROUGH (#3759) =====
          # The generic `*)` arm below would render a syntactically correct `MISPLACED (<detail>)`
          # and NO REMEDY — and this state's entire value IS its remedy. A remedy is not something
          # to leave to a fall-through, so the arm is written out. It names (1) the issue the
          # marker was found on (carried in the detail the probe built), (2) that it GRANTS NOTHING
          # and the FAIL STANDS, and (3) the one operator action that fixes it. No part of either
          # marker stem and no fillable field skeleton appears here — the exact form lives in
          # `--help` only, because summary blocks get pasted into PR comments as a matter of course
          # and an artifact that DESCRIBED the escape hatch became it once already (#3312 job 23).
          misplaced) WAIVER_REPORT="MISPLACED (${ROBOREV_WAIVER_DETAIL:-an authorization for this review was found on a linked issue thread rather than on the pull request}. IT GRANTS NOTHING AND THIS FAIL STANDS — only an authorization on the PULL REQUEST is read. REMEDY: the authorizer re-posts the IDENTICAL line as a TOP-LEVEL COMMENT ON THE PR, as the sole nonblank content of that comment, then verifies with 'gh pr view <PR> --json comments' that it is there; run 'bash scripts/flow/roborev-review.sh --help' for the exact form, which is deliberately not printed here)" ;;
          *) WAIVER_REPORT="$(printf '%s' "$ROBOREV_WAIVER_STATE" | tr '[:lower:]' '[:upper:]') (${ROBOREV_WAIVER_DETAIL:-cause not established})" ;;
        esac
        # ===== LAYER 3 (roborev job 23): THIS DIAGNOSTIC MUST NOT BE A CREDENTIAL =====
        # It used to print a COMPLETE marker carrying the live sha, so pasting this very block into a PR
        # comment — the documented practice throughout this repo — authorized the next run. The exact form
        # now lives ONLY in `--help`, which the requester has to go and read; nothing printed here can be
        # pasted into a grant. The anchoring and placeholder rules make a pasted block harmless anyway,
        # but this layer means the block never carries a live credential in the first place.
        DETAILS+=("ERROR: prompt-content: ${#missing_paths[@]} of the ${#checked_paths[@]} CODE census paths appear on NEITHER side of any 'diff --git' header in the prompt actually sent to the reviewer, so nothing establishes that the reviewer received their diffs. The census is authoritative ($CENSUS for ${BASE}...HEAD); a diff that does not carry a file cannot have reviewed it. THE MACHINE CANNOT TELL WHY THEY ARE ABSENT — a diff roborev delivered by snapshot path and a vacuous review that received nothing look IDENTICAL from here, which is the accepted cost of not inferring delivery mode from injectable prompt text. If this absence is legitimate, the review's token accounting is the evidence a human weighs (genuine reviews measured 398k-649k input / 314k-554k cached; the vacuous baseline is ~18.7k input / 0 cached), and the OWNER or the COORDINATION LEAD may waive it for THIS review only (base ${RANGE_BASE_SHA:-<unknown>} — the merge-base of ${BASE} and HEAD, which is the base of the reviewed range and NOT the tip of ${BASE}, head ${HEAD_SHA:-<unknown>}, job ${JOB:-<unknown>}) with a dedicated anchored PR-comment line. THE EXACT MARKER FORM IS DELIBERATELY NOT PRINTED HERE — run 'bash scripts/flow/roborev-review.sh --help' for it — because a summary block gets pasted into PR comments as a matter of course, and a block that carried a complete marker would authorize the next run by being quoted. Waiver state for this run: ${WAIVER_REPORT} (a well-formed marker from an author outside the waiver allowlist reports UNAUTHORIZED and does not grant). Absent paths (first 10):")
      fi
      printed=0
      for census_path in "${missing_paths[@]}"; do
        [ "$printed" -lt 10 ] || break
        DETAILS+=("  $census_path")
        printed=$((printed + 1))
      done
      if [ "${#missing_paths[@]}" -gt 10 ]; then
        DETAILS+=("  … and $(( ${#missing_paths[@]} - 10 )) more")
      fi
    else
      PROMPT_CONTENT="PASS (${#checked_paths[@]}/$census_total code census paths present)"
    fi
  fi
}

roborev_check_findings() {
  # --- step 6c: findings — STRUCTURED first, prose only inside the block ----------
  # Tier 1 (step 6d) is gated on this answer, so deriving it from a regex over the WHOLE
  # transcript was a real weakness (codex, round 5): incidental or QUOTED severity text
  # such as "[Low]" anywhere in the output set findings: PRESENT, which then exempted a
  # genuinely vacuous "no code changes" verdict from the authoritative tier-1 failure. A
  # gate is only as strong as its input, so:
  #   1. STRUCTURED FIRST — the job record's `verdict` field ("F" = the review reported
  #      findings; a pass letter/true = it did not). Measured on real jobs.
  #   2. PROSE FALLBACK IS SCOPED — only the FINDINGS BLOCK (from a `Findings`/`## Findings`
  #      header up to the `Summary:` line) is scanned, never the whole transcript.
  #   3. CONTRADICTIONS FAIL — "clean" (verdict pass, or exit 0) while the findings block
  #      DOES carry severity markers is an INCONSISTENT state. It fails the run and, being
  #      neither PRESENT nor NONE, cannot exempt tier 1 either.
  FINDINGS_BLOCK_FILE="$LOG.findings"
  # THE RECOGNISER LIVES IN lib/roborev-findings-count.sh (#4050) — extracted VERBATIM,
  # comments included, so the merge-gate leg can derive the SAME count from the SAME
  # daemon-recorded review text by running the SAME CODE. Read that file for what the
  # block boundaries and the marker set are and why (the terminator must stay
  # LINE-INITIAL; under-counting is the fail-closed direction for the tier-1 gate).
  #
  # THE `|| block_marker_count=""` PRESERVES THIS SITE'S BEHAVIOUR EXACTLY. The library is
  # THREE-VALUED — it returns 1 and echoes nothing where the old inline pipeline would
  # have yielded 0 through its `|| true` — and the `:-0` default immediately below is
  # where that unmeasured answer is folded onto 0, which is documented there as the
  # STRICT direction for every consumer of this key. The fold is deliberately kept AT that
  # audited default rather than moved into the library, whose OTHER caller must NOT fold.
  block_marker_count=$(roborev_findings_count "$LOG" "$FINDINGS_BLOCK_FILE") ||
    block_marker_count=""
  # THE `:-0` DEFAULT IS THE FAIL-CLOSED DIRECTION, verified rather than assumed (#3229
  # round-10 sweep audit of every `${VAR:-default}` in these three files). A fail-open default
  # masking a failed measurement is exactly how the `${_census_end:-$_census_start}` bound
  # degraded a broken `awk` into a 1-line scan, so each such default has to be shown to fall the
  # STRICT way. Here it does: a failed `awk`/`grep` yields 0 markers, 0 markers makes
  # `findings:` read NONE rather than PRESENT, and NONE is what makes `vacuity-tier1` treat the
  # "no code changes" phrase as a VACUITY CLAIM and HARD FAIL. PRESENT is the permissive value
  # (it downgrades tier 1 to an advisory NOTICE), and an unmeasurable block can never produce it.
  #
  # RE-DERIVED FOR #3564, because that issue made `findings:` gate the terminal verdict — where
  # `NONE` is the PERMISSIVE value, the opposite polarity to tier 1. The argument SURVIVES, but
  # only because of how the fallback below is built: `NONE` is reachable ONLY from an affirmative
  # STRUCTURED verdict, never from a marker count, so no consumer derives `NONE` from this `0`.
  # An intermediate version of #3564 DID derive `NONE` from the count on the recheck path, which
  # invalidated this paragraph outright (a failed measurement would have read as "no findings" for
  # a merge-gating key) — recorded because the invalidation was silent and the argument still LOOKED
  # sound. THE STANDING RULE: a fail-closed argument for a default is valid only for the consumers
  # that existed when it was written. Re-derive it whenever you add one.
  block_marker_count=${block_marker_count:-0}

  verdict_findings="unknown"
  case "$JOB_VERDICT" in
    P|p|PASS|pass|Pass|true|clean) verdict_findings="none" ;;
    F|f|FAIL|fail|Fail|false) verdict_findings="present" ;;
  esac

  review_ran=0
  if [ -n "$JOB_STATUS" ]; then
    case "$JOB_STATUS" in "done") review_ran=1 ;; esac
  elif [ "$REVIEW_COMPLETED" = "PASS" ]; then
    review_ran=1
  fi

  if [ -n "${RECHECK_JOB:-}" ]; then
    # NO REVIEWER RAN IN THIS INVOCATION, so claiming its exit status PASSed would be a false statement
    # about a process that did not exist (#3312 job 24). `SKIP` is in the block grammar and is deliberately
    # OUTSIDE the affirmation set, so it cannot contribute to a PASS — the original review's outcome is
    # still re-asserted, by `findings:` and `review-completed` from the job record.
    ROBOREV_EXIT="SKIP (recheck: no reviewer ran in this invocation; job $RECHECK_JOB re-decided from its record)"
  elif [ "$REVIEW_RC" -eq 0 ]; then
    ROBOREV_EXIT="PASS"
  elif [ "$review_ran" -eq 1 ]; then
    ROBOREV_EXIT="FINDINGS (exit $REVIEW_RC)"
    DETAILS+=("ERROR: roborev-exit: FINDINGS — 'roborev review' exited $REVIEW_RC because the review REPORTED FINDINGS. The review is GENUINE (job status '${JOB_STATUS:-unknown}') and the reviewer did NOT malfunction: do not retry it and do not bypass it. TRIAGE AND FIX the findings in the transcript ($LOG), then push and re-review. RESULT is FAIL because a review with open findings is not \"roborev clean\".")
  else
    ROBOREV_EXIT="ERROR (exit $REVIEW_RC)"
    DETAILS+=("ERROR: roborev-exit: ERROR — 'roborev review' exited $REVIEW_RC and the job did not complete (status '${JOB_STATUS:-unavailable}'). The REVIEWER itself failed, so nothing was certified — this is an infra condition, not a findings outcome: check the daemon ('roborev status'), the agent's credentials, and the transcript at $LOG.")
  fi

  case "$verdict_findings" in
    present)
      if [ "$block_marker_count" -gt 0 ]; then
        FINDINGS="PRESENT ($block_marker_count)"
      else
        FINDINGS="PRESENT"
      fi
      ;;
    none)
      if [ "$block_marker_count" -gt 0 ]; then
        FINDINGS="INCONSISTENT (verdict clean, $block_marker_count findings marker(s))"
        DETAILS+=("ERROR: findings: the job record's verdict '$JOB_VERDICT' says the review was clean, but its findings block carries $block_marker_count severity marker(s). One of the two is wrong, so the findings state is INCONSISTENT — failed closed, and it cannot exempt the tier-1 vacuity check either.")
      else
        FINDINGS="NONE"
      fi
      ;;
    *)
      # No structured verdict. THE RECHECK CASE FIRST, because the fallback below is keyed on the
      # REVIEWER'S EXIT CODE and a recheck HAS NO REVIEWER — `roborev-exit` is legitimately
      # `SKIP`, which matched neither arm and left `findings: UNKNOWN` on every recheck of a record
      # without a structured `verdict` field. That was invisible while nothing depended on
      # `findings` alone; #3564 made it load-bearing, and left unfixed it would false-FAIL every
      # clean recheck — i.e. break the ONLY path the #3312 absence waiver can travel. A guard that
      # reds on correct input is the guard agents learn to waive.
      #
      # So a recheck re-asserts the findings state from the RECORD'S OWN REVIEW TEXT, which IS the
      # transcript in this mode — the same source `review-completed` and both vacuity tiers are
      # re-asserted from. Scoped to the FINDINGS BLOCK, never the whole transcript: a whole-text
      # scan reads a QUOTED severity word as a finding, and here that would be a false FAIL.
      # `review-completed` has already required a terminal verdict marker, so this is a real review
      # text and not a truncated one.
      #
      # AND NO COUNT OF ZERO REACHES NONE HERE: the marker scan below is POSITIVE-DETECTION ONLY.
      # A marker inside the findings block establishes PRESENT; its ABSENCE establishes nothing, so
      # an unmeasurable or marker-free block is UNKNOWN (which fails) and never NONE. `NONE` is
      # reachable from the `none)` arm above ALONE — i.e. from the record's STRUCTURED verdict
      # letter. A positive verdict requires an affirmative measurement, and prose is not one.
      if [ -n "${RECHECK_JOB:-}" ]; then
        # ===== PROSE CAN EVIDENCE FINDINGS. IT CANNOT EVIDENCE CLEANLINESS. =====
        # (#3564, after two review rounds each finding a review SHAPE the previous recogniser
        # missed.) A recheck has no reviewer, so `roborev-exit` is `SKIP` and the arms below —
        # which are keyed on the reviewer's EXIT CODE — cannot answer. The record's review text is
        # the only other evidence, and the asymmetry between the two directions is total:
        #
        #   a severity marker INSIDE a findings block  =>  POSITIVE evidence of findings. Sayable.
        #   NO marker found                            =>  NOT evidence of cleanliness. Never NONE.
        #
        # WHY THE SECOND DIRECTION IS UNPROVABLE FROM PROSE, which is why this is not a third
        # recogniser: `review-completed` accepts a `## Summary` heading ALONE as a completed
        # review. So a findings review whose findings are prose, under no `Findings` heading and
        # with no severity marker, is INDISTINGUISHABLE from a clean one — measured, not supposed:
        # a real clean review's text is `No issues found.\n\nSummary: ...` (job 154), carrying no
        # `Findings` heading either. Every candidate recogniser (a heading, a marker anywhere, a
        # non-empty block) admits some findings-bearing shape, so the list never closes. That is
        # this repository's #3312 lesson applied here: REMOVE THE CHANNEL, do not pick a rarer
        # delimiter. The wrapper's own facts tool says the same thing — the structured field
        # "must win wherever it exists", and a transcript regex is "a prose heuristic".
        #
        # SO `NONE` IS REACHABLE ONLY FROM THE STRUCTURED `verdict` (the branches above), AND THAT
        # COSTS NOTHING, measured: `roborev show --json` SYNTHESISES a verdict letter from the
        # `reviews.verdict_bool` column for EVERY record — `P` for a clean review (job 154,
        # verdict_bool=1) and `F` for a findings-bearing one (job 162, verdict_bool=0); the
        # `review_jobs` table has no verdict column at all. So a real recheck of a clean job takes
        # the structured path and still PASSes (the #3312 break-glass is intact), and THIS branch
        # is a DEFENSIVE path for a payload shape no observed record produces. Making a defensive
        # path fail closed is free; making it guess is how a merge gate passes over live findings.
        if [ "$block_marker_count" -gt 0 ]; then
          FINDINGS="PRESENT ($block_marker_count)"
        else
          FINDINGS="UNKNOWN"
          DETAILS+=("ERROR: findings: this is a --recheck-job of a record carrying NO structured 'verdict' field, and no severity marker was found in a findings block of its review text. That is UNKNOWN, NOT 'no findings': review-completed accepts a bare '## Summary' heading as a completed review, so a findings review whose findings are prose is indistinguishable from a clean one, and prose can therefore never establish CLEANLINESS. It cannot certify a PASS. This is also an UNEXPECTED payload shape — 'roborev show --json' synthesises a verdict letter ('P'/'F') from reviews.verdict_bool for every observed record — so suspect a roborev version or payload change first. Remedy: re-review rather than recheck, so the verdict rests on a reviewer's own exit status. Transcript: $LOG")
        fi
      elif [ "$ROBOREV_EXIT" = "PASS" ]; then
        if [ "$block_marker_count" -gt 0 ]; then
          FINDINGS="INCONSISTENT (exit 0, $block_marker_count findings marker(s))"
          DETAILS+=("ERROR: findings: 'roborev review' exited 0 (which means no findings) while the findings block carries $block_marker_count severity marker(s), and the job record has no structured verdict to arbitrate. INCONSISTENT — failed closed, and it cannot exempt the tier-1 vacuity check.")
        else
          FINDINGS="NONE"
        fi
      else
        case "$ROBOREV_EXIT" in
          FINDINGS*)
            if [ "$block_marker_count" -gt 0 ]; then
              FINDINGS="PRESENT ($block_marker_count)"
            else
              FINDINGS="PRESENT"
            fi
            ;;
          *) FINDINGS="UNKNOWN" ;;
        esac
      fi
      ;;
  esac
}

roborev_check_tier1() {
  # --- step 6d: tier 1 — AUTHORITATIVE, but gated on `findings:` -----------------
  # The reviewer's own summary claiming there are no code changes, against a census we
  # measured as NON-EMPTY, is trigger T3 — and a merge-gating check must FAIL on it,
  # not merely note it. But the naive form of this check false-FAILs: it once matched
  # anywhere in the transcript, so a review that merely QUOTED the phrase was failed as
  # vacuous (this very wrapper's diff carries the phrase in several files). Agents
  # learning to WAIVE tier-1 failures would restore the defect the guard exists to stop.
  #
  # Two things make the strict version safe:
  #   1. ANCHORING — only the verdict/summary region is matched (the lines carrying a
  #      `Summary:`), never arbitrary finding bodies.
  #   2. GATING ON `findings:` — the deterministic disambiguator computed in step 6c:
  #        findings: NONE     the reviewer is CLAIMING CLEANLINESS, so the phrase is a
  #                           VACUITY CLAIM about a non-empty census  => HARD FAIL
  #        findings: PRESENT  the reviewer demonstrably analysed the diff and produced
  #                           findings, so the phrase is DISCUSSION   => advisory NOTICE
  #        findings: UNKNOWN  we cannot tell whether a review happened. Treated as
  #                           claiming cleanliness => HARD FAIL, because fail-closed is
  #                           the correct direction when the state is unknowable; an
  #                           unparseable findings state must never DISARM this check.
  # `code-free:` remains an independent, strictly earlier check (it fires pre-enqueue).
  # The region is the whole SUMMARY BLOCK, not the lines that happen to contain
  # "Summary:" (codex, round 6 — BLOCKER). The real format is a HEADING:
  #     ## Summary
  #     <blank>
  #     <the summary prose>
  # so a line-matching region missed the prose entirely, and a vacuous clean review whose
  # "no code changes" sentence sits under the heading passed tier 1. The block runs from a
  # Summary heading/label to the next heading (or EOF). A label ANYWHERE on a line also
  # opens the region, so the older single-line "No issues found. Summary: ..." shape is
  # still covered — the block form is a strict superset of the previous line matching.
  VERDICT_REGION_FILE="$LOG.verdict"
  { awk 'BEGIN { inblock = 0 }
         tolower($0) ~ /^[[:space:]]*#{1,4}[[:space:]]*summary/ { inblock = 1; print; next }
       tolower($0) ~ /(^|[^[:alnum:]])summary[[:space:]]*:/ { inblock = 1; print; next }
         /^[[:space:]]*#{1,4}[[:space:]]*[^[:space:]]/ { inblock = 0 }
         inblock { print }' "$LOG" 2>/dev/null || true; } >"$VERDICT_REGION_FILE"
  # NO SUMMARY REGION => `UNAVAILABLE`, and that is PERMISSIVE BY DESIGN — stated here rather
  # than left for the next reader to re-derive (#3229 round-10 sweep). It is the one branch of
  # this file where an unmeasured signal takes the non-failing path, so the reason has to be on
  # the page: tier 1 asks ONE question, "does the reviewer's own summary claim there are no code
  # changes", and with no summary region there is no claim to judge — a genuine NOT-APPLICABLE,
  # not a failure to measure something that exists. A review with no `## Summary` heading is a
  # legitimate shape (`review-completed:` accepts a Findings heading or a `**Severity**:` line as
  # its terminal marker), so FAILing here would red correct input. It cannot manufacture a pass
  # either: tier 1 is a CORROBORATOR, `UNAVAILABLE` is carried into the block (never silent),
  # and the vacuity condition it looks for is independently covered by the deterministic keys —
  # `prompt-content:` (the reviewer's own prompt vs our census) and `code-free:` (our own
  # census classification), both of which fail closed on data the wrapper measured itself.
  if [ ! -s "$VERDICT_REGION_FILE" ]; then
    TIER1="UNAVAILABLE"
  elif grep -qi 'no code changes' "$VERDICT_REGION_FILE"; then
    case "$FINDINGS" in
      PRESENT*)
        TIER1="NOTICE (phrase present in a findings-bearing review)"
        DETAILS+=("NOTICE: vacuity-tier1 (advisory here, does not fail the run): the review's summary mentions 'no code changes' while the census is NON-EMPTY ($CENSUS), but the review reported findings ($FINDINGS) — so it demonstrably analysed the diff and the phrase is discussion, not a vacuity claim.")
        ;;
      *)
        TIER1="FAIL (vacuous verdict vs non-empty census)"
        DETAILS+=("ERROR: vacuity-tier1: the review's summary claims there are NO CODE CHANGES to review while the locally computed census is NON-EMPTY: $CENSUS (${BASE}...HEAD), and the review reported NO findings (findings: $FINDINGS) — so it is CLAIMING CLEANLINESS on a change it did not review. The reviewer's claim contradicts a fact we measured ourselves: this run is NOT reportable as \"roborev clean\".")
        if [ "$FINDINGS" = "UNKNOWN" ]; then
          DETAILS+=("ERROR: vacuity-tier1: the findings state is UNKNOWN (the reviewer errored), which is treated as claiming cleanliness — fail-closed is the correct direction when we cannot tell whether a review happened.")
        fi
        ;;
    esac
  else
    TIER1="PASS"
  fi
}

roborev_check_tier2() {
  # --- step 6e: tier 2 — token accounting; drift is FAILED, absence is not -------
  # Three distinguishable states (see scripts/flow/roborev-job-facts.py):
  #   absent      -> UNAVAILABLE. A build that reports no token data is a legitimate
  #                  difference, not a signal.
  #   unparseable -> FAIL. A token field IS present but no documented alias resolved to
  #                  a number: EXTERNAL-TOOL DRIFT. Chosen as a FAIL rather than a
  #                  NOTICE because this is exactly how the tier was silently disarmed
  #                  (a rename or a `null` degraded it to a non-failing UNAVAILABLE
  #                  while the real counts were the vacuous baseline and the run
  #                  PASSED). A drift FAIL costs one re-run after a one-line alias
  #                  addition; a silently disarmed guard costs an unreviewed merge.
  #   parsed      -> evaluate the thresholds.
  case "${TOKEN_STATE:-}" in
    parsed)
      TOKENS="input=$TOK_IN cached=$TOK_CACHED output=${TOK_OUT:-unknown}"
      if [ "$JOB_HAS_TOKEN_DATA" = false ]; then
        DETAILS+=("NOTICE: vacuity-tier2: the job record says has_token_data=false yet readable counts are present — a payload inconsistency (drift signal). The counts are used, because they are what the vacuity check asserts on.")
      fi
      tier2_trips=()
      if [ "$TOK_IN" -lt "$ROBOREV_VACUITY_MIN_INPUT_TOKENS" ]; then
        tier2_trips+=("observed input=$TOK_IN < ROBOREV_VACUITY_MIN_INPUT_TOKENS=$ROBOREV_VACUITY_MIN_INPUT_TOKENS (highest observed VACUOUS run: 18801)")
      fi
      if [ "$TOK_CACHED" -eq 0 ]; then
        tier2_trips+=("observed cached=$TOK_CACHED == 0 (every observed vacuous run reports exactly 0; the most false-positive-prone term, retained fail-closed)")
      fi
      if [ "${#tier2_trips[@]}" -gt 0 ]; then
        TIER2="FAIL (vacuous token signature)"
        DETAILS+=("ERROR: vacuity-tier2: the token accounting for job '$JOB' carries the vacuous signature against a NON-EMPTY census ($CENSUS):")
        for trip in "${tier2_trips[@]}"; do
          DETAILS+=("  $trip")
        done
      else
        TIER2="PASS"
      fi
      # ADVISORY ONLY — never a FAIL condition (see the constants block: a genuine
      # CLEAN review and a vacuous one emit near-identical output token counts).
      if [ -n "$TOK_OUT" ] && [ "$TOK_OUT" -lt "$ROBOREV_VACUITY_ADVISORY_MIN_OUTPUT_TOKENS" ]; then
        DETAILS+=("NOTICE: vacuity-tier2 advisory (NOT a failure condition): observed output=$TOK_OUT < ROBOREV_VACUITY_ADVISORY_MIN_OUTPUT_TOKENS=$ROBOREV_VACUITY_ADVISORY_MIN_OUTPUT_TOKENS. Output tokens cannot discriminate a genuine CLEAN review from a vacuous one (both emit roughly 20-60), so this is reported and never asserted.")
      fi
      ;;
    unparseable)
      TOKENS="UNAVAILABLE"
      TIER2="FAIL (token accounting present but unparseable — drift)"
      DETAILS+=("ERROR: vacuity-tier2: job '$JOB' DOES carry token accounting, but none of the documented field aliases resolved to a number — the installed roborev build has DRIFTED from the shape this guard reads. That is failed closed on purpose: a silently unreadable payload is exactly how this tier was disarmed while the real counts were the vacuous baseline. Add the new field name to scripts/flow/roborev-job-facts.py (INPUT/CACHED/OUTPUT_TOKEN_KEYS) and re-run; do not waive it.")
      ;;
    absent)
      TOKENS="UNAVAILABLE"
      TIER2="UNAVAILABLE"
      DETAILS+=("NOTICE: vacuity-tier2: UNAVAILABLE — the job record for '$JOB' carries no token accounting at all. A build that reports none is a legitimate difference, not a signal, so this is a degraded-signal notice and never a silent skip: the deterministic checks still govern, and an unavailable tier 2 can never turn a FAIL into a PASS.")
      ;;
    *)
      TOKENS="UNAVAILABLE"
      TIER2="UNAVAILABLE"
      DETAILS+=("NOTICE: vacuity-tier2: UNAVAILABLE — the structured job record for '$JOB' could not be read at all (no python3, no extractor, or no matching job in 'roborev show --json' / 'roborev list --json'). DEGRADED SIGNAL, never a silent skip.")
      ;;
  esac
}


roborev_check_findings_deferral() {
  # --- step 6f: is a findings state that IS present an AUTHORIZED, MATCHED deferral? -----------
  # (Issue #3626.) "roborev clean" means NO UNADDRESSED FINDINGS, not "the tool printed zero". A
  # lead-deferred finding is re-reported by every later round, so before this existed
  # `findings: PRESENT (n)` persisted, `RESULT` stayed FAIL, and the doctrine rule "any non-PASS
  # terminal RESULT is a blocked merge" blocked the merge FOREVER (measured on PR #3572 job 262: two
  # findings, ZERO new, both filed and both already lead-deferred). This is the mechanical route past
  # it, and it is deliberately narrow.
  #
  # RUN AFTER BOTH VACUITY TIERS, not before: `vacuity-tier1` GATES on `findings:` reading `PRESENT*`
  # (a findings-bearing review that mentions "no code changes" is discussion, not a vacuity claim), so
  # rewriting the value first would silently move a correct advisory NOTICE to a HARD FAIL. The
  # deferral changes what the verdict DOES with an established findings state; it must not change what
  # any other check SAW.
  #
  # RECHECK-ONLY, and that is a property of the mechanism rather than a convenience: the authorizer
  # learns the job id AND the findings only from the FINISHED run, and re-running the wrapper to apply
  # a fresh authorization enqueues a DIFFERENT job, which would stale the marker instantly (#3312 job
  # 24 — the absence waiver was a dead letter for exactly this reason until `--recheck-job` existed).
  # `--recheck-job` enqueues nothing and the block declares `MODE: recheck`, so a deferred PASS can
  # never be pasted as evidence of a fresh clean review.
  DEFERRAL_REPORT=""
  # RESET EVERY FIELD THE VERDICT READS. The wrapper's coupled admission test reads these by name, so
  # a value surviving from an earlier lookup — or from the environment — must never be able to stand in
  # for one this run established.
  ROBOREV_DEFERRAL_STATE=""
  ROBOREV_DEFERRAL_AUTHOR=""
  ROBOREV_DEFERRAL_SCOPE=""
  ROBOREV_DEFERRAL_REASON=""
  ROBOREV_DEFERRAL_DETAIL=""
  ROBOREV_DEFERRAL_ISSUES=""
  ROBOREV_DEFERRAL_COUNT=""
  ROBOREV_DEFERRAL_OBSERVED_COUNT=""
  # OUTSIDE RECHECK MODE THERE IS NOTHING TO LOOK FOR, so no key is emitted — the same reason
  # `waiver:` is absent on a run whose prompt content was present: a placeholder would imply a lookup
  # that never happened.
  [ -n "${RECHECK_JOB:-}" ] || return 0
  local observed=""
  case "${FINDINGS%% *}" in
    PRESENT) ;;
    UNKNOWN|SKIP)
      # ===== `UNKNOWN` AND `SKIP` ARE NOT DEFERRABLE, IN ANY MODE =====
      # Those values mean the findings state was never ESTABLISHED. We cannot count what we cannot
      # see, so a deferral over one would be precisely "a pass resting on a state we could not read" —
      # the shape #3586 exists to forbid. Reported rather than silent: an authorizer whose marker was
      # correct needs to know the run never had a countable findings state, not merely that their
      # authorization "did not work".
      DEFERRAL_REPORT="UNAVAILABLE (findings: $FINDINGS — the findings state was never ESTABLISHED, so there is no measured count for an authorization to match; UNKNOWN and SKIP are NOT deferrable in any mode, because a pass may not rest on a state that could not be read)"
      return 0
      ;;
    *) return 0 ;;
  esac
  # ONLY AN AFFIRMATIVELY MEASURED `PRESENT (n)` IS DEFERRABLE. A bare `PRESENT` carries no count, so
  # there is nothing for the marker's `count=` to be matched against and the affirmative half of the
  # binding would be unenforceable. Reported as UNAVAILABLE rather than COUNT-MISMATCH: no comparison
  # happened, and a cause that names a comparison nobody made sends the operator to fix the wrong field.
  case "$FINDINGS" in
    'PRESENT ('*')')
      observed="${FINDINGS#PRESENT (}"
      observed="${observed%)}"
      ;;
  esac
  case "$observed" in
    ''|*[!0-9]*)
      DEFERRAL_REPORT="UNAVAILABLE (findings: $FINDINGS carries no measured count, and only an affirmatively measured 'PRESENT (n)' is deferrable — with no observed count there is nothing for an authorization to be matched against)"
      return 0
      ;;
  esac
  ROBOREV_DEFERRAL_OBSERVED_COUNT="$observed"
  # BOUND TO THE MERGE-BASE, THE SAME BASE `sha-assert` AND THE ABSENCE WAIVER COMPARE AGAINST
  # (#3392): the scope is the REVIEWED RANGE, and that range is `merge-base..HEAD`. Binding to the base
  # ref's TIP is what made the waiver go spuriously STALE the moment the base ref advanced, which
  # dead-lettered that break-glass under fleet load; the same mistake here would dead-letter this one.
  roborev_findings_deferral_lookup "${RANGE_BASE_SHA:-}" "${HEAD_SHA:-}" "${JOB:-}" "$observed"
  if [ "$ROBOREV_DEFERRAL_STATE" = "granted" ]; then
    # A DISTINCT VERDICT TOKEN, and it is NEVER `NONE`. `NONE` stays reachable only from the job
    # record's structured `verdict` letter, so nobody grepping `findings: NONE` — or any PASS-shaped
    # text — reads a deferred run as a clean review. Everything a reader needs to judge the deferral is
    # in the value: how many findings, which issues they went to, who authorized it, and for which job.
    FINDINGS="DEFERRED ($observed, issues=#${ROBOREV_DEFERRAL_ISSUES//,/,#}, authorized @${ROBOREV_DEFERRAL_AUTHOR}, job ${JOB})"
    DEFERRAL_REPORT="GRANTED (author=@${ROBOREV_DEFERRAL_AUTHOR} issues=${ROBOREV_DEFERRAL_ISSUES} count=${ROBOREV_DEFERRAL_COUNT} scope=${ROBOREV_DEFERRAL_SCOPE} reason=${ROBOREV_DEFERRAL_REASON})"
    DETAILS+=("NOTICE: findings: this review reported $observed finding(s) and ALL of them are DEFERRED by a PR comment naming THIS review — ${ROBOREV_DEFERRAL_SCOPE}. Authorizer as recorded by GitHub: @${ROBOREV_DEFERRAL_AUTHOR}. Deferred to issue(s): ${ROBOREV_DEFERRAL_ISSUES}. Reason as given: ${ROBOREV_DEFERRAL_REASON}. THE MATCH IS AFFIRMATIVE: the authorized count equals the observed count, every named issue is an OPEN issue GitHub confirms, and the authorization is bound to base AND head AND job — so a push, a different base, a re-run or ONE NEW FINDING all require a fresh authorization. THE AUTHOR IS AUTHORIZED AGAINST AN EXPLICIT ALLOWLIST, and beyond that authorship is PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY VERIFIED: a comment from anyone outside the allowlist cannot grant, but on this fleet the worker, the closer and the owner all post through the SAME login, so this wrapper cannot tell WHICH ALLOWLISTED HUMAN posted this comment — the ruling that only the owner or the coordination lead may defer rests on process, and on this comment being permanently attributable. This is NOT a clean review: 'findings:' reports DEFERRED and never NONE, so no reader grepping for a clean run counts it as one.")
  else
    case "$ROBOREV_DEFERRAL_STATE" in
      # THE `NONE` CAUSE TEACHES BOTH CHANNEL RULES, not just the absence: the marker must be the SOLE
      # NONBLANK CONTENT of the comment, and the comment must be TOP-LEVEL. An authorizer told merely
      # "no authorization exists" re-checks their SYNTAX — not the shape of the comment or the channel
      # — and concludes the mechanism is broken. Both rules are load-bearing and both are invisible
      # from a syntactically perfect marker, so the diagnostic states them.
      # AND IT DECLARES WHETHER THE LINKED-ISSUE THREAD WAS CHECKED (#3759) — see the waiver's
      # `none` arm for why a silent `NONE` is the defect. Same closed rendering set, same
      # could-not-check fallback.
      none) DEFERRAL_REPORT="NONE (no findings-deferral comment for this review: the authorization must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — one inside prose, a code fence, a quote or a review body is not read; ${ROBOREV_DEFERRAL_DETAIL:-the linked-issue thread could NOT be checked: the probe was not reached on this path})" ;;
      # ===== A DEDICATED ARM, NOT A FALL-THROUGH (#3759) — see the waiver's arm for the reason ====
      # The detail additionally records that the issue-disposition legs are NOT run issue-side and
      # still apply once the marker is on the PR, so the rendering claims "would have been ACCEPTED
      # BY THE CHANNEL" and never "would have granted".
      misplaced) DEFERRAL_REPORT="MISPLACED (${ROBOREV_DEFERRAL_DETAIL:-an authorization for this review was found on a linked issue thread rather than on the pull request}. IT GRANTS NOTHING AND THIS FAIL STANDS — only an authorization on the PULL REQUEST is read. REMEDY: the authorizer re-posts the IDENTICAL line as a TOP-LEVEL COMMENT ON THE PR, as the sole nonblank content of that comment, then verifies with 'gh pr view <PR> --json comments' that it is there; run 'bash scripts/flow/roborev-review.sh --help' for the exact form, which is deliberately not printed here)" ;;
      *) DEFERRAL_REPORT="$(printf '%s' "$ROBOREV_DEFERRAL_STATE" | tr '[:lower:]' '[:upper:]') (${ROBOREV_DEFERRAL_DETAIL:-cause not established})" ;;
    esac
    # THE EXACT MARKER FORM IS NOT PRINTED HERE, and not even its prefix — summary blocks are pasted
    # into PR comments as a matter of course in this repository, and an artifact that DESCRIBED the
    # escape hatch became it once already (#3312 job 23). The form lives ONLY in `--help`.
    # AND THAT WAS FALSE FOR THE MALFORMED STATE UNTIL roborev job 225: the `*)` branch above
    # interpolates `ROBOREV_DEFERRAL_DETAIL`, and the scanner's MALFORMED detail quoted the whole
    # required form — so this key printed a fillable marker while the sentence above denied it. The
    # deferral inherited the leak from the waiver, and one fix (the scanner's MALFORMED_FORM_DETAIL)
    # closed both, because both kinds get their detail from the same structured parse.
    DETAILS+=("NOTICE: findings: $observed finding(s) are reported for job ${JOB:-<unknown>} and NONE of them is covered by an authorized deferral (deferral: ${DEFERRAL_REPORT}). Triage and fix them, or — if a lead has DEFERRED them to filed issues — that lead may authorize the deferral for THIS review only (base ${RANGE_BASE_SHA:-<unknown>} — the merge-base of ${BASE} and HEAD, which is the base of the reviewed range and NOT the tip of ${BASE}, head ${HEAD_SHA:-<unknown>}, job ${JOB:-<unknown>}, count $observed) with a dedicated PR-comment line naming the filed issue numbers. THE EXACT FORM IS DELIBERATELY NOT PRINTED HERE — run 'bash scripts/flow/roborev-review.sh --help' for it — because a summary block gets pasted into PR comments as a matter of course, and a block that carried a complete authorization would authorize the next run by being quoted. A deferral covers ONLY the findings of the job it names, only when the authorized count equals the observed count, and only when every named issue is an OPEN issue GitHub confirms — a CLOSED one tracks nothing, so it is refused too. The PR BODY is not consulted for any of this: it is editable at any time by anyone with write access with no per-edit attribution, so it evidences nothing (#3626).")
  fi
}
