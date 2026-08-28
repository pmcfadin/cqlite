#!/usr/bin/env bash
# lib-perf-lint.sh — THE perf-invocation guard for the WS0 measurement rig
# (issue #3096 spec R2 trap 1, hardened by #3272 item 10 and review rounds 1 and 2).
#
# Sourced, not executed. It sets NO shell options: `set -euo pipefail` in a sourced
# library mutates the SOURCING shell's options, which is a caller's decision to make.
#
# Split into its own file under the campsite rule. It is also the honest boundary: the
# lint is a self-contained CHECKER over shell source files, with two entry points and
# no dependency on the driver's state — and moving it out means the awk no longer has
# to skip its OWN function body by position, since the file it lints (the driver) no
# longer contains it.

# --- trap 1: the rig contains no per-process perf invocation -------------------
# Spec R2: the rig "contains no per-process perf invocation". Per-process counting
# measured >2x observer cost on this workload, so CPU-wide counting is mandatory and
# the driver checks ITSELF — and every library it sources — at startup.
#
# # Why this stopped being a deny-list grep (issue #3272, item 10, then review round 1)
#
# The first version was `perf stat[^|]*(-p |--pid)` filtered through
# `grep -v 'self-check'`. Driving it over injected invocations found two bypasses:
# an ATTACHED value (the `-p ` alternative required a trailing SPACE) and ANY LINE
# MENTIONING "self-check" (the `grep -v` discarded by CONTENT, so a comment on a real
# invocation suppressed the guard). Item 10 fixed both — and review round 1 then found
# THREE MORE, all in the fixed pattern, all ordinary bash:
#
#   * a SINGLE-QUOTED attached value: the attached-value class was `[0-9"$]`, which
#     omits `'`, so a genuinely per-process invocation written with single quotes was
#     invisible to it. MEASURED against the item-10 pattern: no match.
#   * an invocation through a VARIABLE, `"$SOME_BIN" stat …`: the pattern anchored on
#     the literal word `perf` immediately before `stat`. MEASURED: no match.
#   * a GLOBAL OPTION between the two words, `perf --no-pager stat …`: the same
#     adjacency anchor. MEASURED: no match.
#
# Three bypasses in the fix for two bypasses is the argument itself. **A deny-list
# over source TEXT must anticipate every spelling of the thing it forbids, and it is
# wrong the moment it misses one — silently, in the permissive direction.** There is no
# amount of pattern care that converts that into a guarantee, because the space of
# spellings is open: a new perf option, a new shell idiom, an `eval`.
#
# # Why the OPTION check is now an ALLOWLIST too (review round 2, R4b)
#
# Round 1's fix enumerated the FORBIDDEN options (`-p`, `--pid`) per token. Round 2
# found the same class one level in: `perf stat -x, -e "$EVENTS" -C "$SERVER_CPUS"
# -t "$SERVER_PID"` is PER-THREAD counting — equally per-process in effect, with the
# same observer cost — and it satisfied layer 1 (it is inside the wrapper and carries
# `-C`) while layers 2 and 3 found no matching token. All three layers green, spec R2
# violated. Adding `-t`/`--tid` to the deny list would be the same mistake a third
# time: `--per-thread`, `--cgroup`, `--pid=`-by-another-name, whatever perf adds next.
#
# So layer 2 is INVERTED: the wrapper may carry only the options this rig actually
# needs, and ANY OTHER option token on a perf/stat line is a finding. Unknown future
# spellings therefore fail CLOSED, which is the property an enumeration can never have.
# The set is deliberately tiny — see `PERF_ALLOWED_OPTS`.
#
#   LAYER 1 (STRUCTURAL, source): `perf_stat_c` is the ONLY place the rig invokes
#     `perf`. Every non-comment line whose tokens include a bare `perf`/`stat` command
#     word must either be inside that function's body or carry an explicit
#     `perf-lint-allow` marker (a diagnostic string, a presence probe). A NEW
#     invocation anywhere else fails this regardless of how it is spelled — through a
#     variable, with global options, with any quoting — because the check does not ask
#     what the line looks like, it asks WHERE IT IS. And an affirmative half: the
#     wrapper's own invocation must carry `-C`, so the allowlist cannot be satisfied by
#     a wrapper that counts nothing CPU-wide.
#   LAYER 2 (TOKEN, source): no such line — marked or not, inside the wrapper or not —
#     may carry an option token outside `PERF_ALLOWED_OPTS`. Applied per
#     WHITESPACE-SEPARATED TOKEN rather than by substring, so attached and separated
#     values are one case and quoting is irrelevant. This is what fires when the edit
#     is made INSIDE the wrapper, where layer 1 has nothing to say.
#   LAYER 3 (RUNTIME, argv): `perf_stat_c` inspects the argv it is about to pass. By
#     then bash has done word-splitting and QUOTE REMOVAL, so `-p'1234'` and `-p1234`
#     and `-p "$x"` are the same tokens — the spelling problem does not exist at this
#     layer at all. It is the backstop for a caller that reaches the wrapper with
#     arguments no source scan saw (a computed option, an `eval`).
#
# LAYER 1'S SUBJECT NOW COVERS AN UNKNOWN COMMAND WORD (review round 2 nit). Layer 2 sits
# behind `if (!mentions) next`, and the nit was that this makes the two layers dependent:
# a line layer 1 did not classify as an invocation was never option-checked either, so
# `"${PS[@]}" -p 1234` — a VARIABLE holding both `perf` and `stat` — escaped both.
#
# The fix is at the classifier, not by dropping the gate. `invokes()` now also returns
# true when the line's COMMAND WORD is a VARIABLE EXPANSION (`$X`, `"$X"`, `"${X[@]}"`),
# because such a line invokes something this file cannot identify — and "cannot identify"
# must resolve to "treat as perf", which is the allowlist posture. Dropping the gate
# instead was tried and REVERTED: an unconditional option scan reds on ordinary code
# (`cargo build --release -p ws0-corpus-gen`, `mkdir -p "$OUT_DIR"` — measured, 6 findings
# across this rig), and a guard that reds on `mkdir -p` is the guard an operator deletes.
# A command substitution (`"$(cpu_list_expand …)"`) is NOT a variable expansion: what it
# runs is a literal, visible on the same line, and covered by the ordinary rules.
#
# THE SUBJECT IS THE WHOLE RIG, DISCOVERED (review round 2, R2). The driver's runtime
# call used to lint `${BASH_SOURCE[0]}` — itself — so the FOUR libraries it sources were
# inside the rig and outside all three layers. `perf_invocation_lint_tree` globs the
# directory instead and discovers which file owns the wrapper, so adding a library
# cannot silently add an unlinted file; a hand-maintained list would drift the moment
# someone did.

# The option spellings are held in VARIABLES so the diagnostics can name them without
# layer 2 matching its own message — the self-match problem removed rather than filtered
# around, which is what the discarded `grep -v` was trying and failing to do.
_PP_SHORT='-p'
_PP_LONG='--pid'
# The PER-THREAD family (review round 2, R4b). Named for the diagnostics; the guard
# itself no longer depends on the enumeration being complete, because layer 2 is an
# allowlist and these are simply not in it.
_PT_SHORT='-t'
_PT_LONG='--tid'

# The ONLY options a perf/stat line in this rig may carry (layer 2 ALLOWLIST).
#
# `-x` CSV output, `-e` the event list, `-C` the CPU list (the whole point), `-o` the
# output file, `--` the command separator. That is the complete set the wrapper needs.
# Anything else — `-p`, `--pid`, `-t`, `--tid`, `--per-thread`, `-a`, `--cgroup`, an
# option perf has not shipped yet — is a finding, WITHOUT this file having to know what
# it means. An option that changes the counting DOMAIN is exactly the class that must
# not be reachable by an unanticipated spelling.
PERF_ALLOWED_OPTS='-x -e -C -o --'

# The ONLY options a `perf record` line may carry (#3248), kept SEPARATE from the `stat`
# allowlist above rather than merged into it.
#
# WHY SEPARATE, AND WHY THIS IS THE WHOLE POINT. A sampling profile needs `-F` (sample
# frequency) and `-g` (call graph); a counting run needs neither. Widening
# `PERF_ALLOWED_OPTS` to admit them would legalise `-F`/`-g` on EVERY `perf stat` line in
# the rig too, which is a real loss: the allowlist's job is to keep the counting path
# minimal, and an option that is correct for one subcommand is unaudited noise on the
# other. So layer 2 is keyed by SUBCOMMAND, and each subcommand gets the smallest set it
# can actually work with.
#
# `-C` is in the set AND separately REQUIRED by the END assertions below, for exactly the
# reason `perf stat` is: a sampling profile pinned to nothing samples the wrong CPUs, and
# a per-process/per-thread sampling run has the same >2x observer cost the counting path
# refuses. `--` separates the command; `-o` names the output; `-e` selects the sampled
# event.
PERF_RECORD_ALLOWED_OPTS='-x -e -C -o -F -g --call-graph --'

# The counting-DOMAIN option families, for LAYER 3's post-command-word check.
#
# It lives HERE, beside the option names it is built from, because `perf_stat_c` is the
# only consumer and a constant defined in the DRIVER while the function that reads it is
# extracted for testing is exactly the cross-file coupling round 2 flagged in
# `lib-args.sh` (`$COLD_STEP_MAX_MS`): under `set -u` the extracted function dies with an
# unbound-variable error instead of producing its diagnostic. An enumeration is
# unavoidable on that side — `$@` legitimately carries `--shape full`, so nothing there
# can be allowlisted — and the reason is recorded at the branch in `perf_stat_c`.
PERF_DOMAIN_OPTS="$_PP_SHORT $_PP_LONG $_PT_SHORT $_PT_LONG --per-thread -a --all-cpus --cgroup"

# perf_invocation_lint <file> [mode] — print one `<lineno>: <reason>` per violation,
# and nothing when the file is clean. Exit status is not used; the CALLER counts
# output, so an awk that dies mid-file cannot read as "clean" (it would print nothing
# AND the caller's own affirmative check for the wrapper would fail).
#
# `mode` is `owner` (default) or `library`:
#
#   owner    — this file DEFINES `perf_stat_c`. The three END assertions apply: the
#              wrapper must exist, must invoke something, and must pass `-C`.
#   library  — this file must define NO wrapper (there is exactly ONE in the rig) and
#              must invoke perf NOWHERE. Layers 1 and 2 apply unchanged; the END
#              assertions would be nonsense here, so a wrapper DEFINITION is the
#              finding instead.
perf_invocation_lint() {
  local mode="${2:-owner}"
  # THE AWK'S STATUS IS ABSORBED, DELIBERATELY (#3272 review round 4 nit).
  #
  # The driver runs under `set -e -o pipefail` and captures this function's output:
  #
  #     _perf_lint_out="$(perf_invocation_lint_tree "$HERE")"
  #
  # so an awk that DIED mid-file made the pipeline non-zero, which made the command
  # substitution non-zero, which under `-e` KILLED THE DRIVER at the assignment — before
  # `[[ -n "$_perf_lint_out" ]]` ever inspected the text. The run died with a bare exit status
  # and no diagnostic, and `_perf_lint_verify_complete`'s "did not COMPLETE over this file"
  # branch — added for exactly that case — was UNREACHABLE on the driver's path.
  #
  # A guard whose diagnostic cannot be printed on the path it was written for is not a guard.
  # So the awk's status is absorbed here: the FINDING travels as TEXT (which is this function's
  # whole contract — the caller counts output), and the missing `#LINT-COMPLETE` marker is what
  # tells `_perf_lint_verify_complete` the scan died. Absorbing the status cannot hide a
  # failure, because the marker's ABSENCE is the signal.
  #
  # `|| true` on the awk alone, not on the pipeline: `_perf_lint_verify_complete`'s own status
  # is meaningful and is left intact.
  # `SQ` carries a literal single quote INTO the awk program, so the program text (itself
  # inside a single-quoted shell string) never has to contain one — see `is_var_command`.
  { awk -v pp_short="$_PP_SHORT" -v pp_long="$_PP_LONG" \
      -v pt_short="$_PT_SHORT" -v pt_long="$_PT_LONG" \
      -v allowed="$PERF_ALLOWED_OPTS" -v recallowed="$PERF_RECORD_ALLOWED_OPTS" \
      -v mode="$mode" -v SQ="'" '
    BEGIN {
      n = split(allowed, a, /[[:space:]]+/); for (i = 1; i <= n; i++) ok[a[i]] = 1
      # The `record` allowlist is a SEPARATE set, not a superset: an option legal for a
      # sampling profile must not become legal on a counting line (#3248).
      m = split(recallowed, b, /[[:space:]]+/); for (i = 1; i <= m; i++) recok[b[i]] = 1
    }
    # A token as the SHELL would see it after word-splitting and quote removal:
    # leading/trailing quotes, parens and `;` stripped. That is what makes the
    # spelling of an invocation irrelevant — `"perf"`, `perf`, `(perf` and `perf;`
    # reduce to the same token.
    #
    # A trailing COMMA is deliberately NOT stripped: `no perf, no sudo` in a prose line
    # would then reduce to the command word `perf` and red every diagnostic string in the
    # rig. (Measured while writing round 2s fix — 6 false findings.) `optname` handles
    # `-x,` on its own, so nothing needs the comma stripped here.
    function bare(t) { gsub(/^[("'\''`]+/, "", t); gsub(/[)"'\''`;]+$/, "", t); return t }
    # An option token reduced to its NAME: the attached value dropped, so `-p1234`,
    # `-p"$x"`, `--pid=1234` and `-x,` reduce to `-p`, `-p`, `--pid`, `-x`. Long options
    # keep their whole name; short options are one letter, which is how perf parses them.
    function optname(t) {
      if (t !~ /^-/) return ""
      if (t == "--") return "--"
      if (t ~ /^--/) { sub(/=.*$/, "", t); return t }
      return substr(t, 1, 2)
    }
    # Does this line INVOKE the tool? A line invokes it when some token IS the
    # command word (`perf`, or any path ending `/perf`, or the subcommand `stat`).
    # This is deliberately NOT a substring test: `perf_stat_c`, `perf_event_paranoid`
    # and `target/perf-ws0-3096` are identifiers and paths, not invocations, and a
    # substring test on them is what makes a text guard noisy enough to be deleted.
    # Matching the SUBCOMMAND too is what closes review round 1s
    # variable-and-global-option bypasses: `"$BIN" stat …` and `perf --no-pager stat …`
    # both carry a bare `stat` token however the tool itself is spelled.
    #
    # It ALSO returns true when the COMMAND WORD is an unresolvable VARIABLE EXPANSION
    # (round 2 nit): `"${PS[@]}" -p 1234` invokes something this file cannot name, and
    # "cannot name" must resolve to "treat as perf" — that is the allowlist posture. The
    # command word is the first token after any `VAR=val` prefixes and control operators.
    # A command SUBSTITUTION is excluded: `x="$(cpu_list_expand …)"` runs a literal that
    # is visible on the same line and covered by the ordinary rules.
    # DELIBERATELY NARROW. Reimplementing shell word-splitting in awk is a second
    # implementation of bash, and a second implementation is only as good as the
    # differential testing nobody is going to do — so this asks ONE cheap, well-defined
    # question: is the FIRST token of the line a variable expansion, on a line that is
    # not an assignment? That is the whole of the bypass the nit named
    # (`"${PS[@]}" -p 1234`).
    # An assignment is excluded because its right-hand side runs nothing in command
    # position; a command SUBSTITUTION is excluded because what it runs is a literal on
    # the same line, covered by the ordinary rules.
    #
    # ASSIGNMENT PREFIXES ARE SKIPPED, NOT USED TO DISMISS THE LINE (#3272 review round 3
    # nit). This used to `return 0` on ANY line beginning with `VAR=`, and the comment
    # claimed the prefixed form `FOO=1 "$BIN" stat …` was "caught by the bare `stat` token
    # instead" — TRUE ONLY WHEN `stat` IS LITERAL. MEASURED against that version:
    #
    #     FOO=1 "$BIN" "$SUB" -p 1234
    #
    # produced NO FINDING. Both the command word and the subcommand are in variables, so
    # there is no bare `perf`/`stat` token for layer 1, `invokes()` returned 0, and layer 2
    # sits behind `if (!mentions) next` — a genuinely per-process invocation escaping all
    # three layers, which is the deny-list-by-another-name failure this file exists to
    # remove. So the prefixes are STEPPED OVER and the first non-prefix token is the
    # command word, which is what bash does.
    #
    # A PURE assignment (`FOO=bar`) still returns 0: after the prefix is skipped there is
    # no command word left, so nothing is invoked in command position.
    #
    # A prefix is only stepped over when it is an UNAMBIGUOUSLY COMPLETE token — balanced
    # quotes. Whitespace-splitting a line is not shell word-splitting, so an assignment
    # whose VALUE contains spaces spans several awk tokens, and treating its first token as
    # a complete prefix would make the SECOND token look like a command word. MEASURED
    # while writing this fix: without the balance test, `want="$(cpu_list_expand "$spec")"`
    # in `lib-cpu.sh` split into `want="$(cpu_list_expand` + `"$spec")"`, the first was
    # skipped as a prefix, and `"$spec")"` was read as a variable command word — SIX false
    # findings across the shipped tree, on ordinary code. A guard that reds on
    # `want="$(f "$x")"` is the guard an operator deletes.
    #
    # Unbalanced => NOT a prefix => the token becomes the candidate command word, which
    # begins with a NAME and so returns 0: the old, safe answer for exactly the lines the
    # old version was right about.
    #
    # Still DELIBERATELY NARROW, and the residual is stated rather than left to be
    # discovered: an assignment prefix whose value contains BOTH a space and balanced
    # quotes (`FOO="a b" "$BIN" "$SUB" …`) is not stepped over, so that shape is not
    # covered. Reimplementing shell word-splitting in awk is a second implementation of
    # bash, only as good as differential testing nobody will do — and the layer-2 allowlist
    # plus the layer-3 runtime argv check remain behind this.
    function is_var_command(line,   t, n, i, tk, q1, q2, po, pc) {
      n = split(line, tk, /[[:space:]]+/)
      t = ""
      for (i = 1; i <= n; i++) {
        if (tk[i] == "") continue
        # An assignment PREFIX: `NAME=` or `NAME[idx]=`, with balanced quotes so the token
        # is known to be complete. Step over it, as bash does.
        if (tk[i] ~ /^[A-Za-z_][A-Za-z0-9_]*(\[[^]]*\])?=/) {
          # `gsub` returns the substitution COUNT, so replacing each quote with itself
          # counts them. The single quote is written as `SQ` (a BEGIN-assigned variable):
          # this awk program is inside a single-quoted shell string, so a literal one
          # would terminate it — the escaping dance is avoided rather than got right.
          # (Two comment lines in this very block once carried an apostrophe and did
          # exactly that: `bash -n` reported a syntax error at the function header.)
          q1 = gsub(/"/, "\"", tk[i])
          q2 = gsub(SQ, SQ, tk[i])
          # PARENS too, for the ARRAY assignment `FOO=(a b c)`: it is not a command prefix
          # at all, and `FOO=(scan` splits at the space exactly as a quoted value does.
          # MEASURED: without this, `_ARM_LIST=(scan $ARMS)` in ws0-baseline.sh had
          # `_ARM_LIST=(scan` skipped as a prefix and `$ARMS)` read as a variable command
          # word — one false finding on the shipped driver.
          po = gsub(/\(/, "(", tk[i])
          pc = gsub(/\)/, ")", tk[i])
          if (q1 % 2 == 0 && q2 % 2 == 0 && po == pc) continue
        }
        t = tk[i]
        break
      }
      if (t == "") return 0        # a pure assignment: no command word at all
      gsub(/^[("'\''!]+/, "", t)
      if (t ~ /^\$\(/ || t ~ /^`/) return 0
      return (t ~ /^"?\$[A-Za-z_{]/)
    }
    function invokes(line,   n, i, t, tk) {
      n = split(line, tk, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        if (tk[i] ~ /^#/) break     # a whitespace-preceded `#` starts a comment: not argv
        t = bare(tk[i])
        if (t == "perf" || t ~ /\/perf$/ || t == "stat") return 1
      }
      return is_var_command(line)
    }
    /^[[:space:]]*#/ { next }                                   # full-line comment
    # The lint FUNCTIONS THEMSELVES are skipped STRUCTURALLY, by position, for the case
    # where this library is pointed at ITSELF (the tree lint below does exactly that).
    # A CONTENT-based exclusion is what the discarded `grep -v self-check` did, and its
    # bypass was a code comment; position cannot be spelled around.
    /^perf_invocation_lint\(\)/ || /^perf_invocation_lint_tree\(\)/ { inlint = 1; next }
    inlint && /^\}/  { inlint = 0; next }
    inlint           { next }
    /^perf_stat_c\(\)/ {
      inwrap = 1
      if (mode == "library") print NR ": defines perf_stat_c, but the rig has exactly ONE wrapper (this file is not it)"
      next
    }
    inwrap && /^\}/    { inwrap = 0; wrapseen = 1; next }
    # THE SECOND SANCTIONED WRAPPER (#3248): `perf_record_c`, for sampling profiles. It lives
    # in the SAME file as perf_stat_c deliberately — `perf_invocation_lint_tree` discovers the
    # owner by grepping for `^perf_stat_c()`, and treats two owners as a finding, so a second
    # wrapper in a second file would read as a rig with two owners. One file, two wrappers,
    # one owner.
    /^perf_record_c\(\)/ {
      inrec = 1
      if (mode == "library") print NR ": defines perf_record_c, but the rig has exactly ONE wrapper file (this file is not it)"
      next
    }
    inrec && /^\}/     { inrec = 0; recseen = 1; next }
    {
      mentions = invokes($0)
      marked   = index($0, "perf-lint-allow") > 0
      if (inwrap) {
        if (mentions) { wrapinvoke++; if (index($0, "-C")) wrapcpuwide++ }
      } else if (inrec) {
        if (mentions) { recinvoke++; if (index($0, "-C")) reccpuwide++ }
      } else if (mentions && !marked) {
        print NR ": perf/stat invocation outside the single perf_stat_c wrapper, unmarked"
      }
      # LAYER 2, applied to a marked line TOO — the marker exempts a line from the
      # ALLOWLIST (a diagnostic string may name the tool), never from the option check.
      # It runs on every line `invokes()` classifies as an invocation, which now includes
      # a line whose command word is an unresolvable variable expansion (round 2 nit).
      # Ordinary code (`taskset -c 1`, `mkdir -p "$d"`) is not option-checked, because an
      # allowlist of PERF options says nothing about it.
      if (!mentions) next
      n = split($0, tok, /[[:space:]]+/)
      # WHICH ALLOWLIST APPLIES IS DECIDED BY THE SUBCOMMAND ON THE LINE (#3248), not by
      # which wrapper we happen to be inside: a `record` line outside the wrapper must be
      # option-checked as a `record` line, or the narrower `stat` set would report it with a
      # misleading reason. Default is the STAT set, so a line whose subcommand cannot be
      # identified gets the STRICTER treatment — an unknown must not inherit the looser rule.
      isrec = 0
      for (i = 1; i <= n; i++) { if (bare(tok[i]) == "record") { isrec = 1; break } }
      for (i = 1; i <= n; i++) {
        if (tok[i] ~ /^#/) break    # trailing comment: prose, not argv
        o = optname(bare(tok[i]))
        if (o == "") continue
        if (isrec) { if (o in recok) continue }
        else if (o in ok) continue
        if (o == pp_short || o == pp_long)
          print NR ": per-process option token `" tok[i] "` on a perf/stat line"
        else if (o == pt_short || o == pt_long)
          print NR ": per-thread option token `" tok[i] "` on a perf/stat line (per-thread counting is per-process counting)"
        else
          print NR ": option token `" tok[i] "` is not in the perf " (isrec ? "record" : "stat") " option allowlist (" (isrec ? recallowed : allowed) ") — an option this rig does not need may change the counting DOMAIN"
      }
    }
    # An AFFIRMATIVE per-file subject check, in BOTH modes (#3272 review round 3 nit).
    # `library` mode used to have NO END assertions at all, so an awk that DIED on a library
    # file — a malformed regex reached on a particular line, a runtime error — printed
    # nothing and read as CLEAN, and the drivers startup lint (which counts OUTPUT) waved
    # the run through. The three `owner` assertions happened to cover that for one file
    # only. Every mode now emits a subject line the CALLER verifies, so "printed nothing"
    # and "never finished" stop being the same observation.
    #
    # The marker goes to the same stream as the findings and is FILTERED by the caller, not
    # by the reader: a diagnostic a human has to remember to ignore is one they will read as
    # a finding.
    END {
      if (mode != "library") {
        if (!wrapseen)     print "0: perf_stat_c() is absent — there is no single wrapper to allow"
        if (!wrapinvoke)   print "0: perf_stat_c() invokes nothing — the allowlist would be vacuous"
        if (!wrapcpuwide)  print "0: perf_stat_c() does not pass -C — the wrapper counts nothing CPU-wide"
        # perf_record_c is OPTIONAL — a rig with no sampling profile is legitimate — but if it
        # is DEFINED it must be non-vacuous and CPU-wide, on the same terms as perf_stat_c. A
        # defined-but-empty wrapper would otherwise be an allowlist entry protecting nothing.
        if (recseen && !recinvoke)  print "0: perf_record_c() invokes nothing — the allowlist would be vacuous"
        if (recseen && !reccpuwide) print "0: perf_record_c() does not pass -C — a sampling profile pinned to nothing samples the wrong CPUs"
      }
      printf "#LINT-COMPLETE lines=%d mode=%s\n", NR, mode
    }
  ' "$1" 2>/dev/null || true; } | _perf_lint_verify_complete "$1" "$mode"
}

# _perf_lint_verify_complete <file> <mode> — pass the findings through, but turn a MISSING
# or ZERO-LINE completion marker into a finding of its own (#3272 review round 3 nit).
#
# Two states that previously read as "clean":
#
#  * THE AWK DIED MID-FILE. It printed whatever it had and exited; the caller counts output,
#    so a file with no findings before the death read as clean. In `library` mode there were
#    no END assertions at all, so nothing could catch it.
#  * THE FILE WAS EMPTY (or held only comments). Nothing to find, nothing printed — but also
#    nothing checked, and a rig file that became empty by accident is a finding.
#
# This is the same rule the tree lint applies to its FILE SET, applied one level down to each
# file's CONTENT: never derive a positive verdict from the absence of a bad signal.
_perf_lint_verify_complete() {
  local file="$1" mode="$2" line complete=0 lines=0
  while IFS= read -r line; do
    case "$line" in
      '#LINT-COMPLETE '*)
        complete=1
        lines="${line##*lines=}"; lines="${lines%% *}"
        ;;
      *) printf '%s\n' "$line" ;;
    esac
  done
  if [[ "$complete" != "1" ]]; then
    echo "0: the lint did not COMPLETE over this file (the awk exited before its END block)."
    echo "   A partial scan prints exactly like a clean one, and in 'library' mode there were"
    echo "   no END assertions at all to catch it (#3272 review round 3)."
    return 0
  fi
  if [[ "${lines:-0}" -eq 0 ]]; then
    echo "0: this file has NO LINES, so the lint's subject was EMPTY — nothing was checked,"
    echo "   which prints exactly like a clean file. Mode was '$mode'."
    return 0
  fi
}

# perf_invocation_lint_tree <dir> — lint EVERY shell file of the rig, printing
# `<file>:<lineno>: <reason>` per violation and nothing when the tree is clean.
#
# THE SUBJECT IS DISCOVERED, NOT ENUMERATED (issue #3272 review round 2, R2). The
# driver's startup call linted `${BASH_SOURCE[0]}` — ITSELF — so `lib-cpu.sh`,
# `lib-host-state.sh`, `lib-args.sh` and this file were inside the rig and outside all
# three layers: a `perf stat -p "$SERVER_PID"` added to any of them fired NOTHING.
# A hand-written list would have the same defect one edit later, so the set is the
# directory glob and the wrapper OWNER is discovered by who defines `perf_stat_c`.
#
# Three vacuity guards, because a checker whose subject is empty prints nothing and
# reads exactly like a clean tree:
#
#   * ZERO files is a finding (a wrong `dir`, a moved rig).
#   * ZERO wrapper definitions is a finding (nothing owns the invocation).
#   * MORE THAN ONE wrapper definition is a finding ("perf is invoked in ONE place" is
#     what layer 1 rests on, so two wrappers dissolve the allowlist).
perf_invocation_lint_tree() {
  local dir="$1" f owner="" count=0 owners=0 out rec_owner="" rec_owners=0
  local -a files=() unreadable=()
  for f in "$dir"/*.sh; do
    # A glob that matched NOTHING yields the pattern itself; that is the empty-subject case
    # below, not an unreadable file.
    [[ -e "$f" ]] || continue
    # AN UNREADABLE `.sh` IS A FINDING, NOT A `continue` (#3272 review round 3 nit). It used
    # to be silently skipped, so a rig file with the wrong mode — or one whose directory a
    # container mounted without read permission — was DROPPED FROM THE SUBJECT and the tree
    # read as clean with the file never scanned. That is the subject-too-small shape the
    # DISCOVERED glob exists to prevent, arriving through the readability test instead of
    # through a hand-written list.
    if [[ ! -r "$f" ]]; then
      unreadable+=("$f")
      continue
    fi
    files+=("$f")
    count=$((count + 1))
    if grep -q '^perf_stat_c()' "$f"; then
      owner="$f"
      owners=$((owners + 1))
    fi
    # THE RECORD WRAPPER IS A SECOND, INDEPENDENT OWNER ROLE (#3248) — optional, because a
    # rig with no sampling profile is legitimate, but unique when present for the same reason
    # the stat owner is unique: layer 1 rests on "perf is invoked in ONE place per role", so
    # two record wrappers dissolve the record allowlist exactly as two stat wrappers would
    # dissolve the stat one. Discovered, never enumerated.
    if grep -q '^perf_record_c()' "$f"; then
      rec_owner="$f"
      rec_owners=$((rec_owners + 1))
    fi
  done
  if [[ "${#unreadable[@]}" -gt 0 ]]; then
    echo "$dir:0: ${#unreadable[@]} rig file(s) are UNREADABLE and were therefore NOT SCANNED:"
    printf '%s:0:   %s\n' "$dir" "${unreadable[@]}"
    echo "$dir:0: a file dropped from the subject prints exactly like a clean one, so this is"
    echo "$dir:0: a finding rather than a skip (#3272 review round 3). Fix the permissions."
    return 0
  fi
  if [[ "$count" -eq 0 ]]; then
    echo "$dir:0: no *.sh found — the lint's subject is EMPTY, which prints exactly like a clean tree"
    return 0
  fi
  if [[ "$owners" -eq 0 ]]; then
    echo "$dir:0: no file defines perf_stat_c — nothing owns the single perf invocation the allowlist rests on"
    return 0
  fi
  if [[ "$owners" -gt 1 ]]; then
    echo "$dir:0: $owners files define perf_stat_c — layer 1 allows ONE wrapper, so two dissolve the allowlist"
    return 0
  fi
  # The RECORD owner is optional but unique when present (#3248): two record wrappers dissolve
  # the record allowlist exactly as two stat wrappers dissolve the stat one. ZERO is NOT a
  # finding here — a rig that takes no sampling profile is legitimate, and demanding one would
  # make every checkout without a profiler fail a guard about a capability it does not use.
  if [[ "$rec_owners" -gt 1 ]]; then
    echo "$dir:0: $rec_owners files define perf_record_c — the record allowlist rests on ONE wrapper too"
    return 0
  fi
  for f in "${files[@]}"; do
    # A file may own EITHER role, or both. `owner` mode is what carries the END assertions, so
    # a record-owning file must be linted in owner mode too or its wrapper's non-vacuity and
    # its mandatory -C would never be asserted — the guard would exist and never run, which is
    # this issue's whole subject.
    if [[ "$f" == "$owner" || ( -n "$rec_owner" && "$f" == "$rec_owner" ) ]]; then
      out="$(perf_invocation_lint "$f" owner)"
    else
      out="$(perf_invocation_lint "$f" library)"
    fi
    [[ -z "$out" ]] || printf '%s:%s\n' "$f" "$out"
  done
}

# perf_lint_tree_subject <dir> — the files the tree lint WOULD examine, one per line.
#
# Exists so a test can assert the subject is the WHOLE directory rather than trusting
# that it is: "the lint covers every library" is a claim about a SET, and a set claim
# needs the set printed. Used by scripts/tests/test_ws0_cpu_pinning_guards.sh.
#
# It lists EVERY EXISTING `.sh`, readable or not, so the printed subject is what the tree
# lint's own file set is judged against (#3272 review round 3 nit). Filtering unreadable
# files out HERE would make the subject claim agree with a tree lint that had silently
# dropped them — the set claim confirming its own omission.
perf_lint_tree_subject() {
  local dir="$1" f
  for f in "$dir"/*.sh; do
    [[ -e "$f" ]] && printf '%s\n' "$f"
  done
}
