#!/usr/bin/env bash
#
# observation-boundary-scan.sh <script> — A STRUCTURAL GUARD OVER THE *OBSERVATION* BOUNDARY
# (#3751 round 17, W1).
#
# WHY IT EXISTS. Three review rounds of #3751 found the SAME shape: a decision assembled from
# SEVERAL independent reads of the same subject, so that it described a state that never existed at
# any instant.
#
#   * round 9 (N2): `premerge-assert.sh` validated `head-sha` from one read of the stage record and
#     consumed a SECOND read for the nonce.
#   * round 12 (R2): `classify_report` read the report EIGHT times, so a verdict could be assembled
#     from field values drawn from versions that never coexisted.
#   * round 17 (W1): `record-author-performed` read the REPORT using the generation loaded earlier
#     and then read the STAGE RECORD independently. An `open --force` publishing generation B
#     between those reads left both final re-verifications satisfied — an unchanged report A, an
#     unchanged record B — so `AUTHOR-PERFORMED` was published over B WITHOUT EVER INSPECTING B's
#     verdict, with no `--force`, and with a trace naming A. Measured on the shipped script:
#     `RECORD-OK … supersedes-report-nonce=<A>` at exit 0 while B held `result: FINDINGS`.
#
# Each was fixed at its own site, and the third one proves that is not enough. `review-stage.sh` now
# has ONE primitive that loads a coherent stage observation (`observe_record` for the record half,
# `observe_stage` for the record PLUS the report of the generation those bytes name, re-verified),
# and this guard is what keeps every decision path reasoning from one of its observations instead of
# reading for itself.
#
# WHAT IT ASSERTS, in one sentence: no function in the subject may READ A STAGE FILE except the
# primitive that owns that file, unless the statement is NAMED IN THIS FILE'S ALLOWLIST WITH ITS
# REASON — and every DECISION path must take its observation from the primitive.
#
# WHY IT IS A SIBLING OF `read-boundary-scan.sh` AND NOT A MODE OF IT. That scanner asks a question
# about THE READ (does it go through the one NUL-mapping boundary); this one asks a question about
# THE CALLER (may this function read at all, and did the decision paths observe). It needs
# FUNCTION-BOUNDARY tracking, which that scanner has no notion of, and it would need a second
# allowlist inside a file whose header declares one question — two questions and two allowlists in
# one scanner is how an allowlist entry ends up excusing the wrong thing. `emit-boundary-scan.sh`
# (round 7) and `read-boundary-scan.sh` (round 14) are the precedent: one file per property.
#
# ---------------------------------------------------------------------------------------------
# THE SCOPE, DECLARED — AND WHAT IT DOES NOT COVER
# ---------------------------------------------------------------------------------------------
# A guard that implies more coverage than it has is worse than a smaller one that says so, so the
# scope is printed on EVERY run (`declare_scope`) and repeated here:
#
#   * NOT COVERED: whether the observation is CORRECT — whether the re-verification really pairs the
#     record with the report of the generation it names. This is a WHO-MAY-READ check. That property
#     is behavioural and is pinned by section 31 of `scripts/tests/test_review_stage.sh`, which
#     drives the interleave and requires the named refusal.
#   * NOT COVERED: whether a caller that TOOK an observation actually USES it. A decision path could
#     call the primitive and then ignore its globals; nothing here can see that, and the behavioural
#     cases are what do.
#   * NOT COVERED: a read performed through a helper this scanner does not know to be a reader. The
#     readers are matched BY NAME, from the closed list printed at run time, so a second
#     file-reading implementation introduced under a different name satisfies neither this check nor
#     `read-boundary-scan.sh`'s routing check — that scanner is what sees it, because a new reader
#     would have to capture a file somehow.
#   * NOT COVERED: a reader reached any OTHER way — a backtick substitution, a command word built by
#     expansion, `eval`/`xargs`. The subject uses none of those today, so a new spelling is a GAP
#     rather than a false alarm, which is why this is printed rather than described.
#   * NOT COVERED: the difference between shell code and an EMBEDDED PROGRAM (a single-quoted
#     awk/sed body, a heredoc). Such text is scanned as though it were shell: NOISE, never
#     blindness.
#   * NOT COVERED: `premerge-assert.sh`. It has no primitive-function layer — its single capture
#     lives inline in the assert that consumes it — so this scanner would have nothing to name as
#     the owner. Its own one-capture property (round 9's N2) and its in-window revalidation (round
#     16's V1) are pinned by `scripts/tests/test_premerge_assert.sh`. A subject with no declared
#     primitive is REFUSED here rather than reported clean.
#
# Exit 0 = clean, 1 = at least one violation (each NAMED, with its file and line), 2 = usage.
#
# Run standalone:  bash scripts/tests/lib/observation-boundary-scan.sh scripts/flow/review-stage.sh
set -uo pipefail

P='OBSERVATION-BOUNDARY-SCAN: '

usage() {
  printf '%susage: observation-boundary-scan.sh <scripts/flow/review-stage.sh>\n' "$P" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage
SUBJECT="$1"
[ -f "$SUBJECT" ] || { printf '%sERROR subject not readable: %s\n' "$P" "$SUBJECT" >&2; exit 2; }

# --- the declarations, per subject -------------------------------------------------------------
# READERS: `<reader-fn>=<owner-fn>[,<owner-fn>...]` — a function that reads a stage FILE, and the
# ONLY functions permitted to call it. DECISION: the paths that must take an observation from the
# primitive and must not read for themselves.
case "$(basename "$SUBJECT")" in
  review-stage.sh)
    READERS='stage_record_text=observe_record,observe_stage report_bytes=observe_stage'
    PRIMITIVES='observe_record observe_stage'
    DECISION='cmd_verdict cmd_status cmd_record_author_performed'
    OBSERVER='observe_stage'
    ;;
  *)
    # A NEW SUBJECT IS A REFUSAL, NOT A VACUOUS PASS: a scanner with no declared primitive and no
    # allowlist would report CLEAN for a script it knows nothing about.
    printf '%sERROR no observation primitive is declared for %s — add one (a scan with no primitive is a vacuous pass)\n' \
      "$P" "$(basename "$SUBJECT")" >&2
    exit 2
    ;;
esac

# --- the allowlist -----------------------------------------------------------------------------
# THE GRAMMAR IS THREE LINE KINDS AND NO IN-BAND DELIMITER (#3312's rule, and it fired here on the
# first run): `#` a REASON, `@in <function>` a SCOPE directive, anything else the SOURCE TEXT of one
# allowed statement in the current scope. The first draft carried `<function>|<statement>` on one
# line, and the very first entry — the record re-verification, `… || printf '<unreadable>'` — CONTAINS
# a pipe, so it was truncated and excused nothing. `read-boundary-scan.sh` hit the identical defect
# with its reason field and removed the channel rather than choosing a rarer delimiter; a scope
# directive does the same here, because a shell statement in this subject never begins with `@`.
#
# Matched on TEXT and never on a line number: a line number drifts with every edit above it, while a
# text match makes a CHANGE to one of these reads surface HERE and have to be RE-DECLARED.
#
# THE REASON IS THE POINT: a read outside the primitive is a CLAIM that this read is not a second
# opinion, and a claim with no stated grounds is what this guard exists to remove.
allow_review_stage() {
  cat <<'EOF'
@in cmd_record_author_performed
# THE IN-WINDOW RE-VERIFICATION OF THE REPORT (#3751 round 9 N1, retargeted round 15 U1). Taken
# AFTER the substitute is committed and BEFORE the stage record is published, it is what makes the
# publication a decision about the verdict the operator READ. It is deliberately a FRESH read: its
# whole purpose is to detect a change since the observation, so taking it from the observation
# would make it compare a value with itself. Round 15's structural pin fixes its position between
# the substitute's commit and the publication.
now_obs="$(report_bytes "$STAGE_REPORT")"
# THE IN-WINDOW RE-VERIFICATION OF THE RECORD (#3751 round 15, U1). The same argument, for the file
# this call REWRITES: a concurrent `open --force` that published a new generation must not be
# silently reverted by a rewrite of the bytes this process read before it. Fresh for the same
# reason — it is the comparison, not the observation.
now_rec_obs="$(stage_record_text "$sfile" 2>/dev/null || printf '<unreadable>')"
EOF
}
ALLOW="$(allow_review_stage)"
# COUNTED over ENTRIES, not lines: a `#` line is a reason and an `@in` line is a scope, never an
# entry.
ALLOW_COUNT="$(printf '%s\n' "$ALLOW" | LC_ALL=C grep -c -v -e '^#' -e '^@' -e '^$' || true)"

declare_scope() {
  printf '%sscope %s: every call of a STAGE-FILE READER, attributed to the function it appears in\n' "$P" "$(basename "$SUBJECT")"
  printf '%sreaders and their OWNERS (matched BY NAME): %s\n' "$P" "$READERS"
  printf '%sprimitives: %s\n' "$P" "$PRIMITIVES"
  printf '%sdecision paths required to observe through %s: %s\n' "$P" "$OBSERVER" "$DECISION"
  printf '%sallowlist %s entr(y/ies) consulted, each matched on SOURCE TEXT and carrying its reason\n' "$P" "$ALLOW_COUNT"
  printf '%sfunction attribution: a line matching `^name() {` opens a function and a line `^}` closes it — the subject'"'"'s own convention, and the only one recognised\n' "$P"
  printf '%sNOT COVERED whether the observation is CORRECT — this is a WHO-MAY-READ check; the coherence property is behavioural (section 31 of test_review_stage.sh)\n' "$P"
  printf '%sNOT COVERED whether a caller that TOOK an observation actually USES it\n' "$P"
  printf '%sNOT COVERED a read through a helper not in the reader list (matched by name; read-boundary-scan.sh is what sees a new file capture)\n' "$P"
  printf '%sNOT COVERED a reader reached any other way (a backtick substitution, a command word built by expansion, eval/xargs)\n' "$P"
  printf '%sNOT COVERED shell code vs an EMBEDDED PROGRAM (a single-quoted awk/sed body, a heredoc) — such text is scanned as if it were shell (noise)\n' "$P"
  printf '%sNOT COVERED premerge-assert.sh — it has no primitive-function layer; its one-capture property is pinned in its own suite\n' "$P"
}
declare_scope

# `-v` PERFORMS ESCAPE PROCESSING ON ITS VALUE, so every list travels through `ENVIRON`, which does
# no such processing (measured on `emit-boundary-scan.sh`: a declared line containing `'%s\n'`
# reached awk with a REAL newline in it and matched nothing).
OBS_SUBJECT="$SUBJECT" OBS_READERS="$READERS" OBS_PRIMITIVES=" $PRIMITIVES " \
OBS_DECISION=" $DECISION " OBS_OBSERVER="$OBSERVER" OBS_ALLOW="$ALLOW" OBS_PREFIX="$P" \
LC_ALL=C awk '
function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }

BEGIN {
  prefix = ENVIRON["OBS_PREFIX"]; subject = ENVIRON["OBS_SUBJECT"]
  primitives = ENVIRON["OBS_PRIMITIVES"]; decision = ENVIRON["OBS_DECISION"]
  observer = ENVIRON["OBS_OBSERVER"]
  bad = 0; hits = 0; declared = 0; fn = ""; nallow = 0; nreaders = 0
  # the reader table: reader -> " owner owner "
  n = split(ENVIRON["OBS_READERS"], rd, " ")
  for (i = 1; i <= n; i++) {
    if (rd[i] == "") continue
    p = index(rd[i], "=")
    if (p == 0) continue
    r = substr(rd[i], 1, p - 1); o = substr(rd[i], p + 1)
    gsub(/,/, " ", o)
    owners[r] = " " o " "
    readerlist[++nreaders] = r
    seen[r] = 0
  }
  # the allowlist: an `@in <fn>` directive sets the scope; every other non-comment line is one
  # allowed statement IN that scope. The key is built here, out of band, so no character in a shell
  # statement can change which function an entry belongs to.
  ascope = ""
  n = split(ENVIRON["OBS_ALLOW"], al, "\n")
  for (i = 1; i <= n; i++) {
    t = trim(al[i])
    if (t == "") continue
    if (substr(t, 1, 1) == "#") continue        # a REASON line, never an entry
    if (substr(t, 1, 4) == "@in ") { ascope = trim(substr(t, 5)); continue }
    if (ascope == "") {
      # AN ENTRY WITH NO SCOPE EXCUSES NOTHING AND NAMES NOTHING, so it is a refusal rather than a
      # silently-unmatched line: an allowlist that quietly does not apply is worse than none.
      printf "%sFAIL an allowlist entry appears before any `@in <function>` directive, so it names no scope: %s\n", prefix, t
      bad++
      continue
    }
    allowkey[ascope SUBSEP t] = 1
    allowseen[ascope SUBSEP t] = 0
    allowlist[++nallow] = ascope SUBSEP t
    allowfn[ascope SUBSEP t] = ascope
    allowtx[ascope SUBSEP t] = t
  }
  for (i = 1; i <= nreaders; i++) primseen[readerlist[i]] = 0
  npdefs = 0
  n = split(ENVIRON["OBS_PRIMITIVES"], pl, " ")
  for (i = 1; i <= n; i++) if (pl[i] != "") { prims[++npdefs] = pl[i]; primdef[pl[i]] = 0 }
  ndec = 0
  n = split(ENVIRON["OBS_DECISION"], dl, " ")
  for (i = 1; i <= n; i++) if (dl[i] != "") { decs[++ndec] = dl[i]; observes[dl[i]] = 0 }
}

# --- function attribution ----------------------------------------------------------------------
/^[A-Za-z_][A-Za-z0-9_]*\(\) \{/ { fn = $0; sub(/\(\).*/, "", fn); if (fn in primdef) primdef[fn] = 1; next }
/^\}/ { fn = ""; next }
/^[ \t]*#/ { next }                              # a comment is never a statement

{
  line = $0
  tl = trim(line)
  # A DECLARED read, with its reason in this scanner. MARKED, so an entry that matches nothing is
  # reported as STALE at the end, and COUNTED as examined so the vacuity guard can tell a subject
  # whose out-of-owner reads are all declared from one where nothing matched at all.
  if ((fn SUBSEP tl) in allowkey) { declared++; allowseen[fn SUBSEP tl] = 1; next }

  # Does this statement call the OBSERVER? (used for the decision-path requirement)
  if (fn != "" && (fn in observes)) {
    if (match(line, "(^|[^A-Za-z0-9_])" observer "([ \t\"$]|$)")) observes[fn] = observes[fn] + 1
  }

  for (i = 1; i <= nreaders; i++) {
    r = readerlist[i]
    if (match(line, "(^|[^A-Za-z0-9_])" r "([ \t\"$]|$)") == 0) continue
    hits++
    if (fn != "" && index(owners[r], " " fn " ") > 0) { primseen[r] = 1; continue }
    printf "%sVIOLATION %s:%d `%s` reads a stage file inside `%s`, which is not an owner of it (owners:%s). A decision assembled from SEVERAL reads of one subject can describe a state that never existed: that is #3751 W1, and rounds 9 and 12 before it. Take the observation from `%s`, or declare this statement in observation-boundary-scan.sh with its reason\n", \
      prefix, subject, NR, r, (fn == "" ? "<top level>" : fn), owners[r], observer
    bad++
  }
}

END {
  failed = 0
  # (a) NOTHING EXAMINED AT ALL — a scanner that matched nothing would report CLEAN forever.
  if (hits + declared == 0) {
    printf "%sFAIL no reader call was examined in %s — the reader names matched nothing, which is a vacuous pass\n", prefix, subject
    failed = 1
  }
  # (b) EVERY DECLARED PRIMITIVE MUST EXIST. A renamed primitive would otherwise make every read
  #     "outside its owner" — a flood of false violations — or, worse, make the owner list empty
  #     and every read unattributable.
  for (i = 1; i <= npdefs; i++) {
    if (primdef[prims[i]] == 0) {
      printf "%sFAIL the declared primitive `%s` is not defined in %s — either it was renamed or this scanner is out of date; a scan whose primitive does not exist certifies nothing\n", prefix, prims[i], subject
      failed = 1
    }
  }
  # (c) EVERY READER MUST BE SEEN CALLED BY AN OWNER. A reader nobody calls is either dead code or
  #     a reader this scanner has the wrong owner for; either way a clean report would be vacuous.
  for (i = 1; i <= nreaders; i++) {
    if (primseen[readerlist[i]] == 0) {
      printf "%sFAIL the reader `%s` is called by none of its declared owners in %s — either it is dead or its owner moved; a clean report either way would be vacuous\n", prefix, readerlist[i], subject
      failed = 1
    }
  }
  # (d) EVERY DECISION PATH MUST OBSERVE, EXACTLY ONCE. Zero means it reasons from nothing (or from
  #     globals a previous call left behind); more than once means two observations, which is the
  #     defect itself wearing the name of the primitive.
  for (i = 1; i <= ndec; i++) {
    if (observes[decs[i]] != 1) {
      printf "%sFAIL the decision path `%s` calls `%s` %d time(s) in %s — it must be exactly ONCE: none means it reasons from an observation it did not take, several means two observations\n", prefix, decs[i], observer, observes[decs[i]], subject
      failed = 1
    }
  }
  # (e) A STALE ALLOWLIST ENTRY. An entry that matches no line is an excusal waiting for a read to
  #     drift back into its shape, and it is also the signal that the source it described has
  #     CHANGED — which is why entries are matched on source text rather than by line number.
  for (i = 1; i <= nallow; i++) {
    if (allowseen[allowlist[i]] == 0) {
      printf "%sFAIL STALE allowlist entry in observation-boundary-scan.sh for %s — nothing in the subject matches it, so it excuses nothing and the read it described has changed: in %s: %s\n", prefix, subject, allowfn[allowlist[i]], allowtx[allowlist[i]]
      failed = 1
    }
  }
  if (bad > 0) {
    printf "%sFAIL %d observation-boundary violation(s) over %d reader call(s) and %d declared read(s) in %s\n", prefix, bad, hits, declared, subject
    failed = 1
  }
  if (failed) exit 1
  printf "%sOK %s: %d reader call(s) attributed, %d declared read(s) (every allowlist entry matched), every decision path observes exactly once — no decision reads a stage file for itself\n", prefix, subject, hits, declared
}
' "$SUBJECT"
