#!/usr/bin/env bash
#
# read-boundary-scan.sh <script> — A STRUCTURAL GUARD OVER THE *READ* BOUNDARY (#3751 round 14, T1).
#
# WHY IT EXISTS. `scripts/flow/review-stage.sh` and `scripts/flow/premerge-assert.sh` each have ONE
# faithful-read boundary for untrusted file content (`capture_map_nul` / `c_capture_map_nul`, which
# map NUL to SOH IN THE STREAM so a shell capture cannot silently DROP the byte and thereby
# MANUFACTURE grammar the file does not hold). Round 13 (S2) introduced that boundary and routed
# three of the five non-boundary read sites through it. Two were left reading files directly, and
# BOTH were found by review one round later:
#
#   * `count_field_lines` read the stage record with `grep -c` on the FILE. `grep` is faithful — the
#     ANSWER is not: a record spelt `report-<NUL>nonce: CURRENTX1` holds no `report-nonce:` line, so
#     the count was a truthful `0`, and `0` is exactly the value that means "a pre-nonce record whose
#     single report is the LEGACY bare `<kind>.md`". Measured on the shipped script: a stale legacy
#     `c.md` recording `result: PASS` beside a current sentinel report reported `RESULT: PASS`, rc 0.
#   * `_gate_awk` read the GATE-OF-RECORD summary with `awk … <"$1"`. gawk passes a NUL through a
#     field, and the capture of awk's OUTPUT then removed it, so `RESULT: PA<NUL>SS` — a token the
#     closed set must refuse — arrived as `PASS` at the merge gate.
#
# THREE CONSECUTIVE ROUNDS HAVE NOW FOUND "a boundary exists and one path bypasses it" (round 7's
# emit sites, round 13's record reads, round 14's remaining two), which is this repository's standing
# signal that the answer is a MECHANISM and not a fourth patch. Round 13's own structural asserts
# could not see either site: they checked that the mapping appears EXACTLY ONCE, which is a property
# of the boundary, not of its CALLERS.
#
# WHAT IT ASSERTS, in one sentence: no statement in the subject may read FILE CONTENT except through
# that subject's mapping function, unless the statement is NAMED IN THIS FILE'S ALLOWLIST WITH ITS
# REASON.
#
# HOW IT DECIDES — TWO RECOGNISERS, because shell has two ways to read a file.
#   (1) AN INPUT REDIRECTION from a value: `< "$x"`, `<$x`, `<"$(…)"`. Decidable exactly, and it is
#       what `_gate_awk` used. A here-document/here-string (`<<`, `<<<`) is NOT a file read and is
#       excluded by construction.
#   (2) A READING COMMAND AT THE START OF A PIPELINE with a `$`-bearing operand and NO input
#       redirection of any kind. Pipeline position is the discriminator that makes this low-noise: a
#       reader with something piped INTO it is reading stdin, so `printf … | tr …` is not a file
#       read, while `grep -c "$pat" "$file"` is. This is what `count_field_lines` used.
#
# COMMAND SUBSTITUTIONS ARE **NOT** REDUCED, which is the opposite of `emit-boundary-scan.sh` and is
# deliberate: both defects lived INSIDE a `$( … )`, so collapsing substitutions would have hidden
# exactly the two sites this guard exists to find. A command right after `$(` or `{` is therefore
# recognised as starting a pipeline.
#
# ---------------------------------------------------------------------------------------------
# THE SCOPE, DECLARED — AND WHAT IT DOES NOT COVER
# ---------------------------------------------------------------------------------------------
# A guard that implies more coverage than it has is worse than a smaller one that says so, so the
# scope is printed on EVERY run (`declare_scope`) and is repeated here:
#
#   * NOT COVERED: whether the mapping function is CORRECT. This is a ROUTING check. The behaviour of
#     `capture_map_nul`/`c_capture_map_nul` is pinned by the two suites' byte cases.
#   * NOT COVERED: a read performed by a HELPER this scanner does not know to be the boundary — the
#     boundary is matched BY NAME, per subject, so a second mapping implementation introduced under a
#     different name would satisfy neither the routing check nor anything else here. Round 13's
#     uniqueness assert (the translation appears exactly once) is the check for that, and it stays.
#   * NOT COVERED: a reader reached by a construct the pipeline-start recogniser does not know. The
#     list it DOES know is printed on every run and is CLOSED; a BACKTICK substitution, a command word
#     produced by expansion, and anything invoked through `eval`/`xargs` are outside it. Neither
#     subject uses any of those today, so a new spelling is a GAP rather than a false alarm — which is
#     why the list is printed rather than described.
#   * NOT COVERED: the difference between shell code and an EMBEDDED PROGRAM. This scanner reads the
#     file as shell text and does not know where a single-quoted `awk`/`sed` body or a heredoc begins,
#     so a reading command at a pipeline-start position inside one is scanned as though it were shell.
#     That direction is NOISE (a flag for text the shell never runs), never blindness — and the two
#     awk programs in `premerge-assert.sh` are why the allowlist has entries at all.
#   * NOT COVERED: a `$`-bearing word that is a PATTERN rather than a file operand. Recogniser (2)
#     cannot tell them apart, so a pipeline-leading `grep "$pat"` reading STDIN with no redirection
#     would be flagged. Noise again — and no such site exists in either subject, because a
#     pipeline-leading reader with no redirection and no file operand would hang on the terminal.
#   * NOT COVERED, and it is the important one: WHICH FILES ARE UNTRUSTED. This guard flags every
#     variable-named file read, including the subject's own source (`--help`) — which is why that one
#     is ALLOWLISTED with its reason rather than special-cased. A guard that tried to decide
#     trustedness from a path variable's NAME would be guessing.
#
# Exit 0 = clean, 1 = at least one BYPASS (each NAMED, with its file and line), 2 = usage.
#
# Run standalone:  bash scripts/tests/lib/read-boundary-scan.sh scripts/flow/review-stage.sh
set -uo pipefail

P='READ-BOUNDARY-SCAN: '

# THE READING COMMANDS. A closed list, printed at run time. It is the set of text tools these two
# scripts could plausibly use on a file; a tool absent from it is a declared gap, not a false alarm.
READERS='cat grep egrep fgrep sed awk gawk head tail wc tr od cut sort uniq nl rev comm join paste fold expand dd xxd base64 md5sum sha256sum'

# --- the allowlists ----------------------------------------------------------------------------
# An entry is one SOURCE-TEXT statement, whitespace-trimmed, that reads a file WITHOUT the boundary,
# with its REASON in the comment line(s) directly above it. Matched on TEXT and never on a line
# number: a line number drifts with every edit above it, while a text match makes a CHANGE to one of
# these reads surface HERE and have to be RE-DECLARED rather than drift.
#
# THE REASON IS THE POINT: an unrouted read is a CLAIM that the read cannot be defeated by a NUL, and
# a claim with no stated grounds is what this guard exists to remove.
#
# THE REASON IS A COMMENT AND NOT A FIELD, DELIBERATELY. The first draft put `<source>|<reason>` on
# one line, and the FIRST `|` in a shell statement is the shell's own PIPE — so the `--help`
# entry, which is a two-stage `sed … | sed …` pipeline, had its key silently TRUNCATED at that pipe
# and matched nothing, reporting a bypass for a declared read. Rather than pick a rarer delimiter,
# the channel is REMOVED (#3312): the entry is the whole line, and the reason lives where no
# statement can reach it.
allow_review_stage() {
  cat <<'EOF'
# THE BOUNDARY ITSELF: capture_map_nul's own body — the one faithful read in this script.
LC_ALL=C tr '\000' "$CAPTURE_NUL_TR" <"$1"
# The script's OWN SOURCE, rendered as --help. Script-authored content, not an artifact any agent
# writes; a modified script is invoker-class, which this repository's triage rule puts out of model.
sed -n '2,/^# ---END-HELP---$/p' "$0" | sed -e 's/^# \{0,1\}//' -e '/^---END-HELP---$/d'
EOF
}

allow_premerge_assert() {
  cat <<'EOF'
# THE BOUNDARY ITSELF: c_capture_map_nul's own body — the one faithful read in this script.
LC_ALL=C tr '\000' "$C_CAPTURE_NUL_TR" <"$1"
# `awk -v WANT="$1"` — the `$` here is an awk VARIABLE ASSIGNMENT, not a file operand: `_gate_awk`
# takes no file and reads STDIN, which is what round 14 changed it to. The entry is a CLAIM about
# this exact text, so if a file operand is ever added back the line stops matching and reds.
awk -v WANT="$1" '
EOF
}

usage() {
  printf '%susage: read-boundary-scan.sh <scripts/flow/review-stage.sh|scripts/flow/premerge-assert.sh>\n' "$P" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage
SUBJECT="$1"
[ -f "$SUBJECT" ] || { printf '%sERROR subject not readable: %s\n' "$P" "$SUBJECT" >&2; exit 2; }

case "$(basename "$SUBJECT")" in
  review-stage.sh)
    BOUNDARY='capture_map_nul'
    ALLOW="$(allow_review_stage)"
    ;;
  premerge-assert.sh)
    BOUNDARY='c_capture_map_nul'
    ALLOW="$(allow_premerge_assert)"
    ;;
  *)
    # A NEW SUBJECT IS A REFUSAL, NOT A VACUOUS PASS: a scanner with no boundary name and no
    # allowlist would report CLEAN for a script it knows nothing about.
    printf '%sERROR no boundary is declared for %s — add one (a scan with no boundary is a vacuous pass)\n' \
      "$P" "$(basename "$SUBJECT")" >&2
    exit 2
    ;;
esac

# COUNTED over ENTRIES, not lines: a `#` line is a reason, never an entry.
ALLOW_COUNT="$(printf '%s\n' "$ALLOW" | LC_ALL=C grep -c -v -e '^#' -e '^$' || true)"

declare_scope() {
  printf '%sscope %s: every statement that reads FILE CONTENT — an input redirection from a value, or a READING COMMAND at the START of a pipeline with a $-bearing operand and no redirection\n' "$P" "$(basename "$SUBJECT")"
  printf '%sboundary %s (matched BY NAME; per subject, deliberately not shared between the two scripts)\n' "$P" "$BOUNDARY"
  printf '%sreaders RECOGNISED: %s\n' "$P" "$READERS"
  printf '%sallowlist %s entr(y/ies) consulted, each matched on SOURCE TEXT and carrying its reason\n' "$P" "$ALLOW_COUNT"
  printf '%spipeline-start constructs RECOGNISED: line start | ; | && | || | & | ( | $( | { | ! | case-pattern ) | then | else | elif | do — NOTE a bare pipe is NOT one (a reader after | reads stdin)\n' "$P"
  printf '%sa trailing VARIABLE-ASSIGNMENT PREFIX is stripped before that test, so `out="$(LC_ALL=C grep … "$f")"` is recognised as a pipeline start (without it this scan reported CLEAN on its own subject)\n' "$P"
  printf '%scommand substitutions are NOT reduced — both round-14 defects lived inside $( … ), so collapsing them would hide exactly what this guard looks for\n' "$P"
  printf '%sNOT COVERED whether the mapping is CORRECT — this is a ROUTING check only\n' "$P"
  printf '%sNOT COVERED a second mapping implementation under another name (round 13'"'"'s uniqueness assert covers that, and stays)\n' "$P"
  printf '%sNOT COVERED a reader reached any OTHER way (a backtick substitution, a command word built by expansion, eval/xargs) — the list above is CLOSED\n' "$P"
  printf '%sNOT COVERED shell code vs an EMBEDDED PROGRAM (a single-quoted awk/sed body, a heredoc) is not distinguished — such a site is scanned as if it were shell (noise)\n' "$P"
  printf '%sNOT COVERED a $-bearing word that is a PATTERN rather than a file operand (noise; no such pipeline-leading site exists in either subject)\n' "$P"
  printf '%sNOT COVERED WHICH files are untrusted — every variable-named file read is flagged, the subject'"'"'s own --help source included, which is why that read is ALLOWLISTED rather than special-cased\n' "$P"
}
declare_scope

# `-v` PERFORMS ESCAPE PROCESSING ON ITS VALUE (measured while writing `emit-boundary-scan.sh`: a
# declared line containing `'%s\n'` reached awk with a REAL newline in it and matched nothing), so
# every list travels through `ENVIRON`, which does no such processing.
RBS_SUBJECT="$SUBJECT" RBS_READERS=" $READERS " RBS_BOUNDARY="$BOUNDARY" \
RBS_ALLOW="$ALLOW" RBS_PREFIX="$P" \
LC_ALL=C awk '
function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }

# is_pipe_start(pre) — could a PIPELINE begin right after the text `pre`?
#
# DELIBERATELY NARROWER THAN `emit-boundary-scan.sh`s `is_stmt_start`: a bare `|` is NOT accepted,
# because a reader with something piped INTO it is reading STDIN and not a file. That single
# difference is what keeps this scan low-noise over `printf … | tr …` pipelines while still seeing
# `grep -c "$pat" "$file"`.
# strip_assign_prefix(p) — remove any trailing VARIABLE-ASSIGNMENT PREFIX (`VAR=value cmd`).
#
# WITHOUT THIS THE GUARD REPORTED CLEAN ON ITS OWN SUBJECT (measured against the pre-fix script
# before this function existed). Every cargo-free text call in these two scripts is written
# `LC_ALL=C grep …`, so the text preceding the command word was `out="$(LC_ALL=C`, which ends in `C`
# and is not a pipeline start by any spelling — so `count_field_lines` grep-on-file, the exact defect
# this file exists for, was invisible, and so was a planted `cat "$file"`. A sweep built to close one
# blind spot shipping with its own is a shape this repository has recorded; the positive controls in
# both suites are what caught it here.
function strip_assign_prefix(p,   changed) {
  do {
    changed = 0
    gsub(/[ \t]+$/, "", p)
    if (p ~ /(^|[ \t;&|({!])[A-Za-z_][A-Za-z0-9_]*=[^ \t]*$/) {
      sub(/[A-Za-z_][A-Za-z0-9_]*=[^ \t]*$/, "", p)
      changed = 1
    }
  } while (changed)
  return p
}

function is_pipe_start(pre,   p) {
  p = strip_assign_prefix(pre)
  gsub(/[ \t]+$/, "", p)
  if (p == "") return 1                                   # the start of the logical line
  if (p ~ /[;&({!]$/) return 1                            # after ; && || & ( $( { !
  if (p ~ /\|\|$/) return 1                               # after || (but NOT after a bare |)
  if (p ~ /\)$/) return 1                                 # a case-pattern arm
  if (p ~ /(^|[ \t;&({])(then|else|elif|do)$/) return 1    # after a compound-command keyword
  return 0
}

# has_here_input(s) — does this statement take its input from a here-document or here-string? Such a
# statement reads no file, so recogniser (2) must not consider it. Checked BEFORE the plain `<`
# redirection scan, and the redirection scan skips `<<`/`<<<` for the same reason.
function has_here_input(s) { return (index(s, "<<") > 0) }

BEGIN {
  prefix = ENVIRON["RBS_PREFIX"]; subject = ENVIRON["RBS_SUBJECT"]
  readers = ENVIRON["RBS_READERS"]; boundary = ENVIRON["RBS_BOUNDARY"]
  bypass = 0; hits = 0; declared = 0; routed = 0; pending = 0; logical = ""; startline = 0
  nallow = 0
  n = split(ENVIRON["RBS_ALLOW"], al, "\n")
  for (i = 1; i <= n; i++) {
    t = trim(al[i])
    if (t == "") continue
    if (substr(t, 1, 1) == "#") continue      # a REASON line, never an entry
    allowmap[t] = 1
    allowseen[t] = 0
    allowlist[++nallow] = t
  }
}

{
  raw = $0
  if (pending) { logical = logical " " raw }
  else {
    if (raw ~ /^[ \t]*#/) next                 # a comment is never a statement
    logical = raw; startline = NR
  }
  # LINE CONTINUATION, INCLUDING A TRAILING PIPE OR BOOLEAN. A statement split after `|` is ONE
  # pipeline, and joining it is what makes pipeline position correct: unjoined, the second stage of
  # `printf … |` / `tr …` looks like the START of a pipeline and every such `tr` would be flagged.
  if (logical ~ /\\$/) { sub(/\\$/, "", logical); pending = 1; next }
  if (logical ~ /(\||&&|\|\|)[ \t]*$/) { pending = 1; next }
  pending = 0

  tl = trim(logical)
  # A DECLARED read, with its reason in this scanner. Counted as EXAMINED (so the vacuity guard can
  # tell a subject whose reads are all declared from one where the recognisers matched nothing at
  # all) and MARKED, so an entry that matches nothing is reported as STALE at the end.
  if (tl in allowmap) { declared++; allowseen[tl] = 1; next }

  # A call of the boundary function is the routed shape; count them so the vacuity guard can tell a
  # subject where the boundary is USED from one where it merely exists.
  if (match(tl, "(^|[^A-Za-z0-9_])" boundary "([ \t\"$]|$)")) routed++

  # --- recogniser (1): an input redirection from a VALUE ---------------------------------------
  # `< "$x"`, `<$x`, `<"$(…)"`. A `<<`/`<<<` is not a file read; a `<&` is a descriptor dup.
  t = logical
  while (match(t, /<[ \t]*"?\$/)) {
    seg = substr(t, RSTART, RLENGTH)
    pre2 = substr(t, 1, RSTART - 1)
    t = substr(t, RSTART + RLENGTH)
    if (pre2 ~ /<$/) continue                  # part of a `<<`/`<<<`
    if (index(seg, "<&") > 0) continue
    printf "%sBYPASS %s:%d an input REDIRECTION reads a file named by a value, bypassing %s — a shell capture of the result silently DROPS a NUL, which does not lose information but MANUFACTURES grammar the file does not hold. Route the read through %s, or declare this line in read-boundary-scan.sh with its reason\n", prefix, subject, startline, boundary, boundary
    bypass++
    hits++
  }

  # --- recogniser (2): a READING COMMAND at the START of a pipeline, with a $-bearing operand ---
  if (has_here_input(logical)) next
  if (logical ~ /<[ \t]*"?\$/) next            # already reported by (1)
  nr = split(readers, rw, " ")
  for (i = 1; i <= nr; i++) {
    cmd = rw[i]
    if (cmd == "") continue
    base = 0
    while (1) {
      rest = substr(logical, base + 1)
      if (rest == "") break
      if (match(rest, "(^|[^A-Za-z0-9_])" cmd "([ \t]|$)") == 0) break
      abs = base + RSTART
      if (substr(logical, abs, length(cmd)) == cmd) cstart = abs; else cstart = abs + 1
      pre = substr(logical, 1, cstart - 1)
      if (is_pipe_start(pre)) {
        payload = substr(logical, cstart + length(cmd))
        if (payload ~ /\$/) {
          printf "%sBYPASS %s:%d the reading command `%s` starts a pipeline with a $-bearing operand and no redirection, so it reads a FILE directly, bypassing %s. A faithful reader is not a faithful ANSWER: a NUL inside a key makes the line it belongs to absent, and `absent` is a permissive value. Route the read through %s, or declare this line in read-boundary-scan.sh with its reason\n", prefix, subject, startline, cmd, boundary, boundary
          bypass++
        }
        hits++
        break
      }
      base = abs + RLENGTH - 1
    }
  }
}

END {
  failed = 0
  # AN EMPTY SUBJECT SET IS A FAILURE, NOT A PASS (#1699): a scanner that matched nothing would
  # report CLEAN forever. THREE halves are required, because three different things can be empty.
  #
  # (a) NOTHING EXAMINED AT ALL. `hits + declared` counts every statement either recogniser matched
  #     PLUS every allowlisted read found in the source, so a subject whose file reads are all
  #     DECLARED is not vacuous while a subject where the recognisers fired on nothing and no entry
  #     matched is. (review-stage.sh is legitimately the first case: after round 14 its only file
  #     reads are the boundary body itself and the --help renderer.)
  if (hits + declared == 0) {
    printf "%sFAIL no read statement was examined in %s — neither recogniser matched and no allowlist entry was found, which is a vacuous pass\n", prefix, subject
    failed = 1
  }
  # (b) THE BOUNDARY IS NEVER CALLED. Either the boundary name is wrong for this subject or the
  #     subject has no faithful read at all; a clean report either way would certify nothing.
  if (routed == 0) {
    printf "%sFAIL %s is called nowhere in %s — either the boundary name is wrong here or the subject has no faithful read at all; a clean report either way would be vacuous\n", prefix, boundary, subject
    failed = 1
  }
  # (c) A STALE ALLOWLIST ENTRY. An entry that matches no line is an excusal waiting for a read to
  #     drift back into its shape, and it is also the signal that the source it described has
  #     CHANGED — which is the whole reason entries are matched on source text rather than by line
  #     number. Reported by NAME, and fatal: an unused excusal is exactly what this guard must not
  #     accumulate.
  for (i = 1; i <= nallow; i++) {
    if (allowseen[allowlist[i]] == 0) {
      printf "%sFAIL STALE allowlist entry in read-boundary-scan.sh for %s — nothing in the subject matches it, so it excuses nothing and the read it described has changed: %s\n", prefix, subject, allowlist[i]
      failed = 1
    }
  }
  if (bypass > 0) {
    printf "%sFAIL %d read-boundary bypass(es) over %d recogniser hit(s) and %d declared read(s) in %s\n", prefix, bypass, hits, declared, subject
    failed = 1
  }
  if (failed) exit 1
  printf "%sOK %s: %d recogniser hit(s), %d declared read(s) (every allowlist entry matched), %d call(s) of %s — no read of file content bypasses the boundary\n", prefix, subject, hits, declared, routed, boundary
}
' "$SUBJECT"
