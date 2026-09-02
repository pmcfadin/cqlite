#!/usr/bin/env bash
#
# emit-boundary-scan.sh <script> — A STRUCTURAL GUARD OVER THE EMIT BOUNDARY (#3751 round 7, L1).
#
# WHY IT EXISTS. `scripts/flow/review-stage.sh` and `scripts/flow/premerge-assert.sh` each have ONE
# emit boundary for a value they did not produce (`field_value`/`one_line` and `c_safe_display`).
# That boundary was bypassed at a NEW site in three consecutive review rounds — round 2's S1 (the
# NOT-RUN cause), round 5's J3 (`one_line`'s incomplete control-character map, plus two premerge
# print sites) and round 7's L1 (`C_SOURCE`, plus three values read out of the stage record). Every
# fix was right and the class kept regenerating, which is this repository's standing signal that the
# answer is a MECHANISM, not a fourth patch: CLAUDE.md's rule is to neutralise at ONE boundary and
# NEVER per interpolation site, "because a per-site escape is a list to keep complete".
#
# WHAT IT ASSERTS, in one sentence: on every line of the OPERATOR-FACING output channel, each
# interpolated value must either be routed through a boundary function or be NAMED IN THIS FILE'S
# ALLOWLIST WITH ITS REASON. A new interpolation therefore cannot be added silently; it either
# routes, or its author writes down why it is not data.
#
# HOW IT DECIDES. Command substitutions are reduced INNERMOST-FIRST: a boundary call collapses to an
# opaque marker (so everything inside it is neutralised, `$(field_value "$(stage_file …)")`
# included), any other substitution collapses to a marker naming the function, and a surviving
# non-boundary marker is itself a BYPASS. Whatever `$…` references remain are then checked against
# the allowlist.
#
# WHERE IT LOOKS — ANYWHERE A STATEMENT CAN BEGIN, NOT ONLY AT THE START OF A LINE (#3751 round 9,
# N3). The first version anchored its scope at `^[[:space:]]*(printf|echo)[[:space:]]`, so every
# COMPOUND statement was invisible to it and it reported its subject CLEAN with a real bypass in it:
# the NO-GATE-OF-RECORD block of `premerge-assert.sh` printed the caller-supplied `$delta_file`
# unrouted, from a line beginning `[ -n "$delta_file" ] &&`. Two more sat in `review-stage.sh` for
# the same reason (`$extra` behind a `[ -z … ] ||`, `$token` in a one-line `case` arm). A guard with
# a blind spot invites reliance it cannot support, so the recogniser is POSITIONAL: a command word
# counts when what precedes it (whitespace trimmed) is nothing, or one of `; && || | & ( { !`, or a
# `case`-pattern `)`, or one of the keywords `then else elif do`. It is applied to the REDUCED line,
# which is what keeps a command name inside a command SUBSTITUTION out of it.
#
# AND THE SCAN IS BOUNDED AT THAT COMMAND WORD. Only the text from the output command to the end of
# the logical line is examined. An occurrence BEFORE it — a `[ … ]` guard on a compound line — cannot
# reach the emitted text, and reporting it would force allowlist entries claiming "test only"; such
# an entry would ALSO excuse the same variable where it IS printed, which is exactly the kind of
# claim this guard exists to remove. The cost is stated in the scope block below.
#
# ---------------------------------------------------------------------------------------------
# THE SCOPE, DECLARED — AND WHAT IT DOES NOT COVER
# ---------------------------------------------------------------------------------------------
# A guard that implies more coverage than it has is worse than a smaller one that says so, so the
# scope is printed on EVERY run (see `declare_scope`) and is repeated here:
#
#   * review-stage.sh — `emit` and `note` call sites, i.e. the `REVIEW-STAGE: ` / `[review-stage] `
#     channel. NOT COVERED: the `printf` sites that write the stage RECORD and the REPORT (a
#     different channel with its own boundary, `sanitize_field`, applied on the write side), the
#     value-returning `printf '%s\n'` helpers (a function's stdout return, not output), and
#     `die_usage` (invoker argv rendered to the invoker's own terminal — the repository's triage
#     rule puts a hostile invoker out of model, recorded in round 5's census).
#   * premerge-assert.sh — every `printf`/`echo` statement EXCEPT the value-returning ones
#     enumerated in `value_return_premerge`. That list is matched on SOURCE TEXT, so changing one of
#     those helpers surfaces here as a BYPASS and has to be re-declared rather than drift.
#
#   * NOT COVERED IN EITHER: a POSITIONAL parameter. `$1`/`$2` are allowlisted because at every call
#     site in these two scripts they are script-authored literal detail arguments — but this scanner
#     cannot resolve a positional to its call sites, so a future caller passing DATA through one
#     would not be flagged. Stated rather than implied.
#   * NOT COVERED: whether a boundary function is CORRECT. This is a routing check. The behaviour of
#     `one_line`/`field_value`/`c_safe_display` is pinned by the two suites' byte-census cases.
#   * NOT COVERED: a value printed by a script OTHER than these two (`base-staleness.sh` sanitizes
#     at its own boundary; that is its suite's property, not this one's).
#   * NOT COVERED: an output command reached by a construct the statement-start recogniser does not
#     know. The list it DOES know is printed on every run and is closed; a BACKTICK substitution, a
#     command word produced by expansion, and anything invoked through `eval`/`xargs` are outside
#     it. Neither subject uses any of those today, and the recogniser is a LIST, so a new spelling
#     is a gap rather than a false alarm — which is why the list is printed rather than described.
#   * NOT COVERED: the difference between shell code and an EMBEDDED PROGRAM. This scanner reads the
#     file as shell text and does not know where a single-quoted `awk`/`sed` body or a heredoc
#     begins, so an output command at a statement-start position inside one is scanned as though it
#     were shell. That direction is NOISE (a bypass reported for a value the shell never expands),
#     never blindness.
#   * NOT COVERED, and it is the OTHER direction of the bounded scan: a NON-output command placed
#     AFTER the output command on the same logical line has its values attributed to this emit site.
#     Noise again, and preferred to the alternative for the reason above.
#
# Exit 0 = clean, 1 = at least one BYPASS (each one NAMED, with its file and line), 2 = usage.
#
# Run standalone:  bash scripts/tests/lib/emit-boundary-scan.sh scripts/flow/review-stage.sh
set -uo pipefail

P='EMIT-BOUNDARY-SCAN: '

# THE BOUNDARY FUNCTIONS. One list, shared by both subjects deliberately: `one_line` and
# `c_safe_display` render the same byte classes the same way precisely so ONE value cannot read two
# ways depending on which script printed it (round 5, J3).
BOUNDARIES='field_value one_line sanitize_field c_safe_display'

# COMMAND SUBSTITUTIONS THAT ARE NOT BOUNDARIES AND NEED NONE, per subject, each with its reason.
# Separate from the variable allowlist because the two are different claims: "this NAME is not data"
# versus "this CALL cannot produce data".
SUBS_REVIEW_STAGE=''
# basename "$0" — the script's own name in the usage synopsis; invoker-class argv[0].
SUBS_PREMERGE_ASSERT='basename'

# --- the allowlists ----------------------------------------------------------------------------
# An entry is `<name>|<reason>`. THE REASON IS THE POINT: an unrouted value is a claim that the
# value is not data, and a claim with no stated grounds is what this guard exists to stop.
allow_review_stage() {
  cat <<'EOF'
REFUSE_MARKER|a script constant: one of two literal markers, set once per subcommand
prog|basename of the script's own argv[0]; invoker-class, printed to the invoker's own terminal
kind|validate_kind: a closed charset, refused otherwise, so it can carry neither a control byte nor '='
issue|validate_issue: decimal digits only
KI_KIND|validate_kind, as above (the parsed subcommand's kind)
KI_ISSUE|validate_issue, as above
what|a script-authored literal at every call site ("report-of-record", "stage record")
rc|git check-ignore's exit status, an integer
attempt|a loop counter, an integer
nnonce_lines|count_field_lines' output, an integer measured from the record
reopen_count|an integer derived here
deadline|validate_secs: decimal digits only
agent|sanitize_field via reject_placeholder: a closed charset with '=' excluded
spawned_iso|now_iso: generated here, ISO-8601
head_sha|40-hex from git rev-parse, or a closed token when it cannot be resolved
nonce|nonce_is_valid: alphanumeric, 6-64 chars
STAGE_ELAPSED|`unknown`, or the result of integer arithmetic performed here — never text read from the record
rendered|a closed-set token plus a cause ALREADY routed through field_value
state|a script-authored closed token set (the STATUS cause map)
past|a script-authored closed set: yes / no / unknown
prior_token|a closed-set token from classify_report, matched by string equality before it is printed
performed_by|a closed one-value set ('author'); anything else is refused before this point
reason_tok|sanitize_field's output: a closed charset with '=' excluded
evidence_tok|sanitize_field's output, as above
replaced|a copy of prior_token, a closed-set token
AUTHOR_DISCLOSURE|a script constant, the verbatim disclosure sentence
MAX_INT_VALUE|a numeric script constant: the ten-digit ceiling this tool will compare or add
RESERVE_ATTEMPTS|a numeric script constant: the report reservation's bounded attempt count
1|a script-authored literal detail argument at every call site (see the scope note: positionals are not resolvable here)
2|a script-authored literal detail argument at every call site
EOF
}

allow_premerge_assert() {
  cat <<'EOF'
C_STAGE_KIND|a script constant ('c'), deliberately not an option
certified|validated to 40 lowercase hex characters before any output is produced
certified_raw|the invoker's own argv, echoed in a usage refusal to the invoker's own terminal
advisory_out|base-staleness.sh's output, sanitized at ITS emit boundary; every line carries its own BASE-STALENESS: anchor
line|one line of the same, for the same reason
rc|an exit status, an integer
pr|the invoker's own argv, on a stderr refusal
repo|GH_REPO or the hard-coded default, on a stderr refusal
state|gh's own --jq output: a GitHub state enum
actual|gh's own --jq output: a commit sha, lowercased here
C_TOKEN_REPORT|appears only as the TEST of a `${var:+…}` expansion; the value printed inside it is routed
C_ROUTING_DETAIL|the same: the TEST of a `${var:+…}` expansion whose printed value is routed
C_ROUTING|a script-authored closed token set: REQUIRED / NOT-APPLICABLE / UNMEASURED
advisory_script|a path derived from this script's own directory, not from any input
adv_rc|the advisory's exit status, an integer
ADVISORY_TIMEOUT_SECS|a numeric script constant
ADVISORY_KILL_GRACE|a numeric script constant
1|a script-authored literal detail argument at every call site (see the scope note)
2|a script-authored literal detail argument at every call site
EOF
}

# --- premerge's value-returning printfs -------------------------------------------------------
# Matched on SOURCE TEXT (whitespace-trimmed), never on a line number: a line number drifts with
# every edit above it, and a text match makes a CHANGE to one of these helpers surface here.
value_return_premerge() {
  cat <<'EOF'
printf '%s' "${1:-}" |
printf '%s\n' "$root"
printf 'AMBIGUOUS|%s|%s\n' "$count" "$root"
printf '%s\n' "$found"
printf '%s\n' "$c"
printf 'state=present\n%s' "$body"
EOF
}

usage() {
  printf '%susage: emit-boundary-scan.sh <scripts/flow/review-stage.sh|scripts/flow/premerge-assert.sh>\n' "$P" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage
SUBJECT="$1"
[ -f "$SUBJECT" ] || { printf '%sERROR subject not readable: %s\n' "$P" "$SUBJECT" >&2; exit 2; }

case "$(basename "$SUBJECT")" in
  review-stage.sh)
    SCOPE_CMDS='emit note'
    SCOPE_NAME='emit/note call sites (the REVIEW-STAGE: / [review-stage] channel), WHEREVER a statement can begin'
    NOT_COVERED='the stage-record and report FILE writers (own boundary: sanitize_field), the value-returning printf helpers, and die_usage (invoker argv, invoker terminal)'
    ALLOW="$(allow_review_stage)"
    SUBS="$SUBS_REVIEW_STAGE"
    VRET=""
    ;;
  premerge-assert.sh)
    SCOPE_CMDS='printf echo'
    SCOPE_NAME='every printf/echo statement except the declared value-returning helpers, WHEREVER a statement can begin'
    NOT_COVERED="the $(value_return_premerge | LC_ALL=C grep -c . || true) value-returning printf helpers, declared by SOURCE TEXT in value_return_premerge (a count DERIVED from that list, never written in prose beside it: the two drifted apart the first time one was added)"
    ALLOW="$(allow_premerge_assert)"
    SUBS="$SUBS_PREMERGE_ASSERT"
    VRET="$(value_return_premerge)"
    ;;
  *)
    # A NEW SUBJECT IS A REFUSAL, NOT A VACUOUS PASS. A scanner that greened on an unknown file
    # would report CLEAN for a script it has no scope or allowlist for.
    printf '%sERROR no scope is declared for %s — add one (a scan with no scope is a vacuous pass)\n' \
      "$P" "$(basename "$SUBJECT")" >&2
    exit 2
    ;;
esac

ALLOW_NAMES="$(printf '%s\n' "$ALLOW" | LC_ALL=C sed -e '/^$/d' -e 's/|.*//' | LC_ALL=C tr '\n' ' ')"
ALLOW_COUNT="$(printf '%s\n' "$ALLOW" | LC_ALL=C grep -c '|' || true)"

declare_scope() {
  printf '%sscope %s: %s\n' "$P" "$(basename "$SUBJECT")" "$SCOPE_NAME"
  printf '%sboundaries %s\n' "$P" "$BOUNDARIES"
  printf '%sallowlist %s entries consulted (each carrying its reason, in this scanner)\n' "$P" "$ALLOW_COUNT"
  printf '%sNOT COVERED %s\n' "$P" "$NOT_COVERED"
  printf '%sNOT COVERED a positional parameter cannot be resolved to its call sites; $1/$2 are allowlisted\n' "$P"
  printf '%sNOT COVERED whether a boundary function is CORRECT — this is a ROUTING check only\n' "$P"
  # THE STATEMENT-START LIST IS PRINTED, NOT DESCRIBED (#3751 round 9, N3): it is what decides
  # whether a compound site is examined at all, so a reader can see the gap rather than infer it.
  printf '%sstatement-start constructs RECOGNISED: line start | ; | && | || | pipe | & | ( | { | ! | case-pattern ) | then | else | elif | do\n' "$P"
  printf '%sNOT COVERED an output command reached any OTHER way (a backtick substitution, a command word built by expansion, eval/xargs) — the list above is CLOSED\n' "$P"
  printf '%sNOT COVERED the scan is BOUNDED at the command word: an occurrence BEFORE it (a [ .. ] guard) is not examined, and a non-output command AFTER it is attributed here (noise, never blindness)\n' "$P"
  printf '%sNOT COVERED shell code vs an EMBEDDED PROGRAM (a single-quoted awk/sed body, a heredoc) is not distinguished — such a site is scanned as if it were shell (noise)\n' "$P"
}
declare_scope

# `-v` PERFORMS ESCAPE PROCESSING ON ITS VALUE, so a declared value-returning line containing
# `'%s\n'` arrived at awk with a REAL NEWLINE in it and matched nothing — the list silently had no
# effect and the scan reported four bypasses that were not bypasses. Measured while writing this
# guard; `ENVIRON` does no such processing, so every list travels that way.
EBS_SUBJECT="$SUBJECT" EBS_BOUNDARIES=" $BOUNDARIES " EBS_ALLOW=" $ALLOW_NAMES " \
EBS_SUBS=" $SUBS " EBS_SCOPE_CMDS="$SCOPE_CMDS" EBS_VRET="$VRET" EBS_PREFIX="$P" \
LC_ALL=C awk '
function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }

# reduce(s) — collapse command substitutions innermost-first. A boundary call becomes @B@ (so its
# contents are neutralised); anything else becomes @X:<name>@, which the caller treats as a BYPASS
# unless a boundary consumed it.
function reduce(s,   pos, q, abs, r, close_, seg, name, rep, best, bestclose, guard) {
  guard = 0
  while (index(s, "$(") > 0) {
    if (++guard > 200) return s "@RUNAWAY@"
    best = 0; pos = 1
    while (1) {
      q = index(substr(s, pos), "$(")
      if (q == 0) break
      abs = pos + q - 1
      r = index(substr(s, abs + 2), ")")
      if (r == 0) return s "@UNBALANCED@"
      close_ = abs + 2 + r - 1
      seg = substr(s, abs + 2, close_ - (abs + 2))
      if (index(seg, "$(") == 0) { best = abs; bestclose = close_; break }
      pos = abs + 2
    }
    if (best == 0) return s "@UNBALANCED@"
    seg = substr(s, best + 2, bestclose - (best + 2))
    name = trim(seg)
    if (match(name, /[^A-Za-z0-9_]/)) name = substr(name, 1, RSTART - 1)
    if (index(boundaries, " " name " ") > 0) rep = "@B@"
    else if (index(subs_allow, " " name " ") > 0) rep = "@S@"
    else rep = "@X:" name "@"
    s = substr(s, 1, best - 1) rep substr(s, bestclose + 1)
  }
  return s
}

# is_stmt_start(pre) — could a STATEMENT begin right after the text `pre`? (#3751 round 9, N3)
#
# WHY THIS EXISTS. The scope was `^[[:space:]]*(printf|echo)[[:space:]]` — a LINE-START anchor — so
# every COMPOUND statement was invisible to this guard, and it reported the subject CLEAN with a real
# bypass in it: the NO-GATE-OF-RECORD block of premerge-assert.sh emitted the caller-supplied
# `$delta_file` unrouted from a line beginning `[ -n "$delta_file" ] &&`. That is a shape this
# repository has recorded before — a sweep built to close one blind spot shipping with its own and
# reporting CLEAN on real sites — so the recogniser is positional rather than line-anchored.
#
# IT IS APPLIED TO THE REDUCED LINE, which is what makes it safe: `reduce()` has already collapsed
# every `$( … )`, so a command name inside a command SUBSTITUTION (a value-returning context —
# a value-returning `x=$(printf … | tr …)`) is gone before this runs and cannot be mistaken for a
# statement.
function is_stmt_start(pre,   p) {
  p = pre
  gsub(/[ \t]+$/, "", p)
  if (p == "") return 1                                   # the start of the logical line
  if (p ~ /[;&|({!]$/) return 1                           # after ; && || | & ( { !
  if (p ~ /\)$/) return 1                                 # a case-pattern arm: `*) note …`
  if (p ~ /(^|[ \t;&|({])(then|else|elif|do)$/) return 1   # after a compound-command keyword
  return 0
}

# stmt_cmd_pos(s) — the POSITION of the EARLIEST output command of this subject that appears where
# a statement can begin, or 0. Word-bounded on both sides, and the trailing side requires whitespace
# or end-of-line, exactly as the old line-anchored pattern did.
#
# THE POSITION, NOT A BOOLEAN, BECAUSE THE SCAN IS BOUNDED BY IT. Everything from that command to
# the end of the logical line is the EMITTED STATEMENT and is examined; anything BEFORE it — most
# often a `[ ... ]` guard on a compound line — cannot reach the emitted text, so examining it would
# report bypasses for values that are never printed and would have to be answered with allowlist
# entries claiming "test only". Those claims are the thing this guard exists to avoid: an entry is a
# CLAIM, and a variable that is BOTH tested and printed would then be excused by one. Bounding the
# scan instead makes the printed occurrence the only one that counts.
function stmt_cmd_pos(s,   i, nw, w, cmd, base, rest, abs, cstart, pre, best) {
  nw = split(scope_cmds, w, " ")
  best = 0
  for (i = 1; i <= nw; i++) {
    cmd = w[i]
    if (cmd == "") continue
    base = 0
    while (1) {
      rest = substr(s, base + 1)
      if (rest == "") break
      if (match(rest, "(^|[^A-Za-z0-9_])" cmd "([ \t]|$)") == 0) break
      abs = base + RSTART
      if (substr(s, abs, length(cmd)) == cmd) cstart = abs; else cstart = abs + 1
      pre = substr(s, 1, cstart - 1)
      if (is_stmt_start(pre)) {
        if (best == 0 || cstart < best) best = cstart
        break
      }
      base = abs + RLENGTH - 1
    }
  }
  return best
}

BEGIN {
  prefix = ENVIRON["EBS_PREFIX"]; subject = ENVIRON["EBS_SUBJECT"]
  boundaries = ENVIRON["EBS_BOUNDARIES"]; allow = ENVIRON["EBS_ALLOW"]
  subs_allow = ENVIRON["EBS_SUBS"]; scope_cmds = ENVIRON["EBS_SCOPE_CMDS"]
  bypass = 0; scanned = 0; pending = 0; logical = ""; startline = 0
  n = split(ENVIRON["EBS_VRET"], vr, "\n")
  for (i = 1; i <= n; i++) { t = trim(vr[i]); if (t != "") vrmap[t] = 1 }
}

{
  raw = $0
  if (pending) { logical = logical " " raw }
  else {
    if (raw ~ /^[ \t]*#/) next                 # a comment is never a statement
    logical = raw; startline = NR
  }
  if (logical ~ /\\$/) { sub(/\\$/, "", logical); pending = 1; next }
  pending = 0

  if (trim(logical) in vrmap) next             # a declared value-returning helper

  # REDUCED BEFORE THE SCOPE IS DECIDED (#3751 round 9, N3): the statement-start recogniser must
  # not see command names that live inside a command SUBSTITUTION, and `reduce()` is what removes
  # them. A line that cannot be reduced keeps its original text plus a marker, so it is still
  # CONSIDERED for scope and — if in scope — reported below rather than silently skipped.
  s = reduce(logical)
  cmdpos = stmt_cmd_pos(s)
  if (cmdpos == 0) next
  scanned++
  # BOUNDED AT THE COMMAND WORD: the emitted statement is what is examined (see stmt_cmd_pos).
  payload = substr(s, cmdpos)

  if (index(s, "@UNBALANCED@") > 0 || index(s, "@RUNAWAY@") > 0) {
    printf "%sBYPASS %s:%d could not be parsed (unbalanced or runaway command substitution) — REFUSING rather than reporting clean\n", prefix, subject, startline
    bypass++
    next
  }

  # A surviving non-boundary command substitution: its output reaches the line unrouted.
  t = payload
  while (match(t, /@X:[A-Za-z0-9_]*@/)) {
    nm = substr(t, RSTART + 3, RLENGTH - 4)
    printf "%sBYPASS %s:%d command substitution $(%s ...) reaches an emitted line without a boundary and is not allowlisted\n", prefix, subject, startline, nm
    bypass++
    t = substr(t, RSTART + RLENGTH)
  }

  # Whatever variable references remain.
  t = payload
  while (match(t, /\$\{?#?[A-Za-z_][A-Za-z0-9_]*|\$[0-9]|\$[*@]/)) {
    ref = substr(t, RSTART, RLENGTH)
    t = substr(t, RSTART + RLENGTH)
    nm = ref
    sub(/^\$\{?#?/, "", nm)
    if (index(allow, " " nm " ") > 0) continue
    printf "%sBYPASS %s:%d value $%s is neither routed through a boundary (%s) nor allowlisted with a reason\n", prefix, subject, startline, nm, "field_value/one_line/sanitize_field/c_safe_display"
    bypass++
  }
}

END {
  if (scanned == 0) {
    # AN EMPTY SUBJECT SET IS A FAILURE, NOT A PASS (#1699): a scope regex that matches nothing
    # would report CLEAN forever.
    printf "%sFAIL no in-scope emit site was found in %s — the scope matched NOTHING, which is a vacuous pass\n", prefix, subject
    exit 1
  }
  if (bypass > 0) {
    printf "%sFAIL %d bypass(es) over %d in-scope emit site(s) in %s\n", prefix, bypass, scanned, subject
    exit 1
  }
  printf "%sOK %d in-scope emit site(s) in %s: every interpolated value is routed or allowlisted\n", prefix, scanned, subject
}
' "$SUBJECT"
