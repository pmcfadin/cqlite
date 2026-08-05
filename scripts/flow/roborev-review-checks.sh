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
    # THE HEADERS COME FROM WHEREVER THE DIFF ACTUALLY IS (#3312). roborev has TWO
    # diff-delivery modes: inline in the prompt, or — when the diff is large — written to a
    # TRANSIENT snapshot file whose path the prompt names, in which case the prompt itself
    # carries ZERO 'diff --git' headers. Reading only the prompt text therefore reported
    # every code path "absent" on genuine large reviews (MEASURED: job 6836, 23 files,
    # 1.47M input / 1.35M cached / 6.1k output, 4 findings with real file:line) — a false
    # FAIL on exactly the diffs that most need review, and the documented way a guard gets
    # waived. The resolver below follows the diff to where it is; the census, the canonical
    # matcher and the fail-closed semantics are untouched.
    roborev_collect_review_diff_headers "$PROMPT_FILE"
    # SELECTED ON THE AFFIRMATIVE STATE NAMES, never on "not a failure".
    case "${ROBOREV_DIFF_SOURCE_STATE:-}" in
      snapshot)
        # ===== C‴: SNAPSHOT MODE IS A NOTICE, NOT A CERTIFICATION =====
        # Owner ruling, after seven review rounds found eleven false-PASS vectors in the machinery that
        # made a snapshot-delivered diff certifiable. The information is PRESERVED — the block records
        # the snapshot path, its digest and the census code-path subset this run expected — so a human
        # closer can act on it; what is gone is the claim that the reviewer demonstrably received those
        # paths. THIS IS A DELIBERATE REDUCTION IN CERTIFICATION STRENGTH, and it is the one place in
        # this wrapper where a previously-FAILing condition no longer fails. It NEVER turns a previous
        # FAIL into a PASS by itself: `prompt-content` becomes NOTICE, and the wrapper's
        # verdict-affirmation backstop admits that NOTICE only for this key and only in this mode.
        # SET ONLY HERE, AND ONLY ON AN AFFIRMATIVE FACT: the resolver reaches `snapshot` only after the path
        # was VALIDLY BOUND, so `SNAPSHOT_NOTICE=1` asserts "a snapshot was bound, and either observed or its
        # non-observation recorded with a cause" — never merely "the mode was selected" (roborev job 16).
        SNAPSHOT_NOTICE=1
        # THE CENSUS SUBSET THIS RUN EXPECTED, for the block. Reported, never asserted (that is the whole
        # of C‴): a closer reads it to know what a certification WOULD have covered.
        SNAPSHOT_EXPECTED="${#census_code_paths[@]} code census path(s) expected, not asserted"
        PROMPT_CONTENT="NOTICE (snapshot mode: not certified — snapshot-path/-digest/-expected record what was observed)"
        if [ -n "${ROBOREV_SNAPSHOT_DIGEST:-}" ]; then
          DETAILS+=("NOTICE: prompt-content: roborev delivered this diff BY SNAPSHOT PATH, so it is OBSERVED AND REPORTED rather than certified (C‴, issue #3312). Observed snapshot: ${ROBOREV_SNAPSHOT_PATH:-<unnamed>} (digest ${ROBOREV_SNAPSHOT_DIGEST}, ${ROBOREV_SNAPSHOT_BYTES:-unknown} bytes). This run EXPECTED the ${#census_code_paths[@]} CODE census path(s) below to be in it; that expectation is NOT asserted, and a closer wanting certainty must inspect the diff or re-review with a smaller range.")
        else
          # NEVER SILENCE: an unobserved snapshot says WHAT could not be observed.
          DETAILS+=("NOTICE: prompt-content: roborev delivered this diff BY SNAPSHOT PATH and the snapshot could NOT be observed by this run, so neither its digest nor its contents are recorded (C‴, issue #3312). Named path: ${ROBOREV_SNAPSHOT_PATH:-<none readable from the prompt>}. Cause: ${ROBOREV_SNAPSHOT_UNOBSERVED_WHY:-not established}. This run EXPECTED the ${#census_code_paths[@]} CODE census path(s) below; that expectation is NOT asserted.")
        fi
        printed=0
        for census_path in ${census_code_paths[@]+"${census_code_paths[@]}"}; do
          [ "$printed" -lt 10 ] || break
          DETAILS+=("  $census_path")
          printed=$((printed + 1))
        done
        if [ "${#census_code_paths[@]}" -gt 10 ]; then
          DETAILS+=("  … and $(( ${#census_code_paths[@]} - 10 )) more (see census: for the total)")
        fi
        return 0
        ;;
      snapshot-unbound|unparseable-instruction)
        # ===== A SELECTED MODE IS NOT AN OBSERVED SNAPSHOT (roborev job 16, blocker 1) =====
        # These two states mean the wrapper received NEITHER an inline diff NOR a snapshot it could read: the
        # named path could not be bound, or an instruction line could not be read at all. The owner ruled that
        # such an input stays a NAMED FAIL — "a review whose reviewer was told to run git itself cannot be
        # verified to have received anything; an unverifiable input is a non-passing verdict by rule 13" — so
        # they must never reach the C‴ NOTICE, and `SNAPSHOT_NOTICE` is deliberately NOT set here. Before this,
        # snapshot mode was selected on the mere PRESENCE of an instruction line, so a compact instruction
        # carrying a git command produced an exempted NOTICE and the run PASSED having received nothing.
        PROMPT_CONTENT="FAIL (snapshot named but unusable: ${ROBOREV_DIFF_SOURCE_STATE})"
        DETAILS+=("ERROR: prompt-content: roborev signalled that the diff was delivered BY PATH, but this run received neither an inline diff nor a snapshot it could read: ${ROBOREV_SNAPSHOT_UNOBSERVED_WHY:-cause not established}. Named path: ${ROBOREV_SNAPSHOT_PATH:-<none readable from the prompt>}. An input that cannot be established is a NON-PASSING verdict — it is not a C‴ NOTICE, because there is no snapshot to report a digest for. Failing closed.")
        DETAILS+=("ERROR: prompt-content: the ${#census_code_paths[@]} CODE census path(s) of $CENSUS (${BASE}...HEAD) are therefore UNVERIFIED — 'we could not check', never 'nothing was wrong'.")
        return 0
        ;;
      inline) ;;
      none)
        # A MEASUREMENT, not an excusal: neither source exists, so the census match below reports every
        # path absent — the fail-closed direction — and this line names the condition.
        DETAILS+=("ERROR: prompt-content: the prompt carries NEITHER an inline diff (no 'diff --git' header) NOR a snapshot diff path (no column-zero 'Read the diff from:' instruction), so nothing in it names a diff the reviewer could have received. This is the T1/T2 family: the review ran against no diff at all.")
        ;;
      *)
        PROMPT_CONTENT="FAIL (diff-source resolver returned the unrecognised state '${ROBOREV_DIFF_SOURCE_STATE:-<unset>}')"
        DETAILS+=("ERROR: prompt-content: the diff-source resolver returned a state this check has never judged. That is a defect in roborev-review-oracles.sh, not in the branch under review — failing closed rather than letting an unplanned state inherit a non-failing path.")
        return 0
        ;;
    esac
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
      PROMPT_CONTENT="FAIL (${#missing_paths[@]}/${#checked_paths[@]} code census paths absent from the prompt)"
      DETAILS+=("ERROR: prompt-content: ${#missing_paths[@]} of the ${#checked_paths[@]} CODE census paths appear on NEITHER side of any 'diff --git' header in the prompt actually sent to the reviewer, so the reviewer never received their diffs. The census is authoritative ($CENSUS for ${BASE}...HEAD); a diff that does not carry a file cannot have reviewed it. Missing (first 10):")
      printed=0
      for census_path in "${missing_paths[@]}"; do
        [ "$printed" -lt 10 ] || break
        DETAILS+=("  $census_path")
        printed=$((printed + 1))
      done
    else
      # The suffix names WHERE the evidence was found, so a pasted block distinguishes an
      # inline-diff review from a snapshot-delivered one. It is EMPTY for the inline case, which
      # keeps the long-standing value spelling byte-identical for every reader that greps it.
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
  # The block runs from a Findings heading/label to the Summary heading/label — the
  # measured real shapes. Everything outside it (a quoted "[Low]" in prose, a severity
  # word inside a Problem sentence) is ignored. The terminator must be a LINE-INITIAL
  # Summary label: matched mid-sentence it closed the block early when a finding's own
  # prose contained the word, under-counting the findings. (Under-counting is the
  # fail-closed direction for the tier-1 gate — fewer markers make a vacuity claim MORE
  # likely to fail — but the count is reported to a human, so it should be right.)
  { awk 'BEGIN { inblock = 0 }
         tolower($0) ~ /^[[:space:]]*#{1,4}[[:space:]]*(review[[:space:]]+)?findings?/ { inblock = 1; next }
         tolower($0) ~ /^[[:space:]]*findings?[[:space:]]*:/ { inblock = 1; next }
         tolower($0) ~ /^[[:space:]]*#{1,4}[[:space:]]*summary/ { inblock = 0 }
         tolower($0) ~ /^[[:space:]]*summary[[:space:]]*:/ { inblock = 0 }
         inblock { print }' "$LOG" 2>/dev/null || true; } >"$FINDINGS_BLOCK_FILE"
  block_marker_count=$({ grep -oiE '\*\*severity\*\*[[:space:]]*:[[:space:]]*(critical|high|medium|low)|\[(critical|high|medium|low)\]|(^|[^[:alnum:]])(critical|high|medium|low): ' "$FINDINGS_BLOCK_FILE" 2>/dev/null || true; } | wc -l | tr -d '[:space:]')
  # THE `:-0` DEFAULT IS THE FAIL-CLOSED DIRECTION, verified rather than assumed (#3229
  # round-10 sweep audit of every `${VAR:-default}` in these three files). A fail-open default
  # masking a failed measurement is exactly how the `${_census_end:-$_census_start}` bound
  # degraded a broken `awk` into a 1-line scan, so each such default has to be shown to fall the
  # STRICT way. Here it does: a failed `awk`/`grep` yields 0 markers, 0 markers makes
  # `findings:` read NONE rather than PRESENT, and NONE is what makes `vacuity-tier1` treat the
  # "no code changes" phrase as a VACUITY CLAIM and HARD FAIL. PRESENT is the permissive value
  # (it downgrades tier 1 to an advisory NOTICE), and an unmeasurable block can never produce it.
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

  if [ "$REVIEW_RC" -eq 0 ]; then
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
      # No structured verdict: fall back to the exit code, still refusing to trust prose
      # over the whole transcript.
      if [ "$ROBOREV_EXIT" = "PASS" ]; then
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

