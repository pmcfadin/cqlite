#!/usr/bin/env bash
# gate-failed-assert.sh — extract the FAILING ASSERT IDENTITY from one already-
# ANSI-normalised agent-gate component log (issue #3765).
#
# WHY THIS EXISTS. A failing component's SUMMARY line used to name only the #3453
# invocation annotation:
#     tooling-tests:  FAIL (1149s)  [test ws0-corpus-gen default-features | + …]
# The bracket is test-SHAPED, so a reader identifies `ws0-corpus-gen` as the failing test —
# which is wrong; the real assert (`FAIL - 1465-skip-declares: …`) lived only in the
# component log. CLAUDE.md forbids reading gate.log ("the SUMMARY block is the ONLY gate
# text an agent retains") while simultaneously requiring a flake citation to match the
# ASSERT rather than the component, so the two rules were unsatisfiable for a FAIL.
#
# CONTRACT
#   usage: gate-failed-assert.sh <ansi-normalised-log> [max-names]
#   stdout (line-oriented, in this order; absent when nothing was recognised):
#       tier=<assert|guard|toolchain>
#       count=<N>                 total DISTINCT identities recognised (never capped).
#                                 DISTINCT is judged on the FULL identity (bounded only by
#                                 the internal, never-published dedup key at key()), so no
#                                 publication policy and no bound can change the count.
#       name=<published value>    at most <max-names> lines, in file order. A NAME IS AN
#                                 IDENTIFIER, NEVER A PAYLOAD — see PUBLICATION POLICY.
#
#       THIS STDOUT IS INTERNAL, NOT PUBLICATION. Its only consumer is agent-gate.sh's
#       `_failassert_record`, which validates every name against its tier's shape on the
#       OUTPUT PATH and passes it through the ONE emit boundary (`_failassert_clean`:
#       NEUTRALISE -> REDACT -> bound for display).
#   exit 0 — the log was read and scanned (whether or not anything matched)
#   exit 2 — usage error / the log could not be read: the CALLER must render this as a
#            named `not extractable`, never as "no failures found". A guard that could
#            not read its subject has measured nothing (CLAUDE.md: a positive verdict
#            requires an affirmative measurement).
#
# THE CALLER OWNS ANSI NORMALISATION. agent-gate.sh routes the log through
# `_ansi_stripped_log` and passes the STRIPPED path, so there is exactly ONE stripper in
# the repo (#3400: 18 workflows set CARGO_TERM_COLOR=always and colour SURVIVES
# redirection to a file, so a parser keyed on `<status> <payload>` matches NOTHING). A
# second copy here would be a second thing to drift. Every read below is a REDIRECTION
# into awk, never a pipe (#3400: a piped `while read` runs in a subshell and its verdict
# is discarded).
#
# ===== PUBLICATION POLICY — EVERY TIER PUBLISHES AN IDENTIFIER, NEVER ITS PAYLOAD =====
# ONE rule for all three tiers (roborev job 49, blocker 11). This is a SAFETY boundary and
# ALSO a simplification back toward what #3765 asked for, whose own sketch is the TAG:
#     failed-assert: 1465-skip-declares (accounted 419/420, floor 410)
#
# WHAT WENT WRONG BEFORE, AND WHY THE OLD RATIONALE WAS FALSE. Tiers `assert`/`guard` used
# to publish the COMPLETE payload of the matched line, justified as "repository-authored
# text: in-tree, reviewed, diffed by every PR". MEASURED FALSIFICATION: in
# scripts/tests/test_agent_gate_summary.sh alone, 205 `bad "…"` messages INTERPOLATE
# RUNTIME VALUES —
#     bad "leaked-child: caller-known summary file '$caller_file' was not produced"
#     bad "relative-path: summary not created at caller CWD ($rel_caller_dir/$rel_name)"
# — so an assert payload is a repo-authored TEMPLATE carrying runtime values, not a
# constant. This repository's own gate interpolates ORIGIN URLS into diagnostics (that is
# why `_component_set_safe_detail` exists), so a credential-bearing value can reach a
# `FAIL - ` payload, and the neutraliser's DECLARED residual (it cannot see `api_key SECRET`
# or a bare unmarked token) then applies to it.
#
# THE SPLIT THAT RESOLVES IT — count on the payload, publish only an identifier:
#   * DEDUP and COUNT run on the FULL identity (that is F1/F5 and it must not regress: a
#     57-char cap before the dedup collapsed two sibling tests; a cut at the first `:`
#     before the dedup named 13 distinct asserts as one tag).
#   * PUBLICATION is a PROJECTION of that identity onto one of exactly three shapes:
#       pubid(...)     a TAG / test-path IDENTIFIER: the first whitespace-delimited token,
#                      cut at the first single `:` (a `::` module separator is kept),
#                      constrained to a NAMED charset and bounded. The tag position is the
#                      repo-authored CONSTANT of a `bad "<tag>: <detail>"` template; the
#                      runtime values live in the DETAIL, which is no longer published.
#       publabel(...)  a GUARD LABEL: already structured (`<label> (<VERDICT>)`), charset-
#                      constrained and bounded.
#       "<literal>"    a CLOSED-ENUM KIND LABEL chosen by this file (tier toolchain only),
#                      never derived from the log at all.
#   * FREE-FORM DETAIL STAYS IN THE COMPONENT LOG, and the field SAYS SO affirmatively
#     rather than leaving a reader to guess whether the harness looked.
#
# NEITHER BOUND NOR CHARSET EVER RETAINS A PREFIX. An identifier that fails its charset or
# its bound is replaced WHOLESALE by an affirmative `<…>` placeholder naming what happened —
# never by its first N characters. That is the class fixed on blocker 8: a truncation must
# not precede neutralisation at ANY bound, however large, because a severed `scheme://` or
# `@host` defeats every later rule (see the ORDER note at add()).
#
# DISAMBIGUATION WITHOUT DETAIL. Distinct identities routinely share a tag (13
# `FAIL - F2: …` lines in one measured suite), and two distinct asserts must never render
# as one string — that is the misidentification #3765 exists to remove. So a shared
# published identifier is ORDINALISED in FILE ORDER (`F2#1`, `F2#2`), which is a pointer
# into the component log and carries no payload. `#` is excluded from every identifier
# charset, so an ordinal can never be forged by log content. The COUNT is unaffected: it
# was taken from the full identity before any projection ran.
#
# ===== THE RECOGNISER TABLE — one place, one stated rule per entry =====
# Three TIERS, most specific first; the FIRST tier that matches at least once wins, so a
# cargo `error: test failed…` epilogue never displaces the test names above it.
#
#   TIER `assert` — a named test case / assertion identity.
#     A1  `FAIL - <identity>`           the repo's bash test-suite convention: the
#                                       `bad()` helper in scripts/tests/*.sh, which the
#                                       tooling-tests component executes (~16 suites).
#                                       MEASURED: this is the #3765 subject line shape.
#                                       Identity = the WHOLE payload (dedup/count).
#                                       Published = pubid(tagof(payload)), i.e. the tag.
#     A2  `FAIL: <detail>` (col 0)      the other bash-suite spelling (8 measured sites,
#                                       e.g. "FAIL: guard did NOT trip on …"). Anchored
#                                       at column zero so `>>> [x] FAIL: …` (a gate
#                                       progress line) and `oom-audit: FAIL (…)` (tier
#                                       `guard`) are not swallowed here. This spelling
#                                       carries NO tag by convention, so its published
#                                       identifier is the leading token — a weak pointer,
#                                       DECLARED as such.
#     A3  `test <name> ... FAILED`      libtest's per-test line. Also the source of the
#                                       `failures:` block below it, which is therefore
#                                       NOT a separate rule (it would double-count). A
#                                       Rust module path has no whitespace and keeps its
#                                       `::` separators, so it publishes in full.
#     A4  `FAIL [ <t>s] <pkg> <test>`   cargo-nextest's per-test line. Identity = the whole
#                                       `<pkg> <test>` payload; published = the TEST path
#                                       (the last token), which is the identifier a reader
#                                       needs — the package alone would ordinalise every
#                                       failure in one crate.
#     A5  `shell-selftest: <f> FAIL`    _run_shell_selftest_files' per-file verdict.
#
#   TIER `guard` — a named guard/component verdict, when no test-case name is available.
#     B1  `<label>: <VERDICT> …`        VERDICT ∈ FAIL-CLOSED|FAIL|REFUSED|STALE.
#                                       MEASURED on arrow-parity-guard ("…: FAIL — 0
#                                       tests ran"), oom-audit ("…: FAIL (enforce)"),
#                                       parity-report ("report: STALE — …") and
#                                       cli-tests ("cli-tests Pass 1 (default):
#                                       FAIL-CLOSED — …").
#     B2  `[<component>] FAIL[-CLOSED]` the bracketed spelling. MEASURED on
#                                       flight-tests, legacy-heuristics,
#                                       binding-rust-tests, all-features-check.
#     B3  `[FAIL] <detail>`             the smoke script's convention (its own [FAIL]/
#                                       [PASS] prefixes). MEASURED on smoke. The whole
#                                       line is the IDENTITY; the published value is the
#                                       leading token of the detail, since this spelling
#                                       has no label — the same DECLARED weak-pointer
#                                       residual as A2.
#
#   TIER `toolchain` — NO assert identity exists. The matched line is COUNTED (it is the
#   dedup key, so the count stays true) and its KIND is published; the line itself is NOT.
#   The kind is the CLOSED ENUM below — five labels, one per recogniser, each named after
#   the SHAPE it matched rather than the producer it is guessed to come from, because a
#   producer claim would be exactly the heuristic this repo forbids. This tier is
#   deliberately MANY-TO-ONE (17 `npm error` lines publish one `npm-error`), so it passes
#   `collapse=1` and is never ordinalised.
#     C1  `error[E….]: …` / `error: …`  -> kind `rustc-cargo-error`
#     C2  `npm error <msg>`             -> kind `npm-error`   (MEASURED: npm ci ENOENT)
#     C3  `bash: <msg>`                 -> kind `bash-error`  (MEASURED: 18 logs)
#     C4  `Diff in <path> at line <n>`  -> kind `rustfmt-diff`
#                                       DECLARED UNMEASURED: all 16 fmt FAILs in the
#                                       corpus were a cargo-fmt USAGE error, never a
#                                       formatting diff, so this entry is derived from
#                                       rustfmt's documented output rather than observed.
#     C5  `Error: <msg>`                -> kind `capitalised-error`
#                                       MEASURED on python-bindings ("Error: [Errno 28]"),
#                                       but the label names the capitalised-`Error:` SHAPE,
#                                       since node and other tools spell it the same way.
#
# NON-EXHAUSTIVE BY CONSTRUCTION. This set was derived by MEASUREMENT over the 174 FAILed
# component logs present in this box's ~4200 retained agent-gate run directories (29
# distinct components), not predicted from source. A shape it does not know produces
# `count=0` and the caller renders `0 RECOGNISED`, never a bare `0` and never silence.
set -uo pipefail

log=${1:-}
max=${2:-10}

case "$max" in ''|*[!0-9]*) max=10 ;; esac
[ "$max" -ge 1 ] 2>/dev/null || max=10

if [ -z "$log" ] || [ ! -f "$log" ] || [ ! -r "$log" ]; then
  echo "gate-failed-assert: cannot read log '${log:-<none>}'" >&2
  exit 2
fi

awk -v max="$max" '
  # ===== THE ORDER IS: NORMALISE (here) -> REDACT (gate) -> BOUND FOR DISPLAY (gate) =====
  #
  # NO BOUND IN THIS FILE TRUNCATES, and that is a SAFETY property rather than a formatting
  # preference. It is the FIFTH instance on this issue of ONE shape — an operation applied
  # to a value BEFORE the operation that needed the value WHOLE:
  #   F1  a 57-char cap before the DEDUP                -> two sibling tests collapsed, count UNDERCOUNTED
  #   F5  a cut at the first `:` before the DEDUP       -> 13 distinct asserts named as one tag
  #   F6  a 60-char DISPLAY elision before the REDACTION-> a credential reached the SUMMARY
  #   F8  a 4096-char SAFETY truncation before REDACTION-> the same, at a 68x bound
  #   F9  the 4096 bound applied AFTER retention        -> unbounded memory (blocker 12)
  # so it is fixed as a CLASS: every bound here either replaces its value with an
  # affirmative placeholder (publication) or is applied BEFORE retention and NEVER published
  # (the dedup key). There is no prefix anywhere on the publication path to sever a
  # credential out of.
  #
  # MEASURED F6. The middle elision of
  #     npm error 401 Unauthorized while fetching the tarball https://x-access-token:TOK@h.io/p
  # produced `npm error 401 Unauthorized ...ess-token:TOK@h.io/p`, DELETING the `https://`
  # scheme — after which NEITHER of the two gate redaction rules matches (rule 1 needs
  # `scheme://…@`, rule 2 needs `@host:`), so the token reached the SUMMARY block this
  # repo tells agents to paste into PR comments. The elision, not the redactor, was the
  # defect: a bound must never be able to change a safety verdict.
  #
  # WHERE THE DISPLAY BOUND LIVES: agent-gate.sh `_failassert_clean`, applied per NAME
  # AFTER the single `_component_set_redact_text` call (head 27 + `...` + tail 30 = 60).
  #
  # norm() is the IDENTITY: one line, no control characters, whitespace collapsed. It does
  # NOT truncate.
  function norm(s,   t) {
    t = s
    gsub(/[\001-\037\177]/, " ", t)          # control chars (a path CAN hold a newline)
    gsub(/[[:space:]]+/, " ", t)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
    return t
  }
  # key() — THE DEDUP KEY. BOUNDED BEFORE RETENTION, AND NEVER PUBLISHED (blocker 12).
  #
  # The full identity used to become an associative-array key BEFORE any bound applied, so
  # a log full of long minified lines could consume unbounded memory and kill the extractor
  # — or the gate — before any summary was produced. The bound is therefore applied HERE,
  # on the way IN.
  #
  # IT IS SAFE TO RETAIN A WINDOW HERE, AND ONLY HERE, because this value is provably never
  # published: add() derives the published value from its `pub` PARAMETER alone and never
  # from `full`/`k`, and the emit path re-validates every name against its tier shape. So
  # the blocker-8 rule (no truncation upstream of neutralisation) is not weakened: nothing
  # downstream can render this string.
  #
  # DISTINCTNESS, which is F1/F5 and must not regress. The FIRST version of this bound keyed
  # an oversized identity on its EXACT length plus a 1024-char head AND a 1024-char tail, and
  # declared the residual — "two identities agreeing on all three is not a realistic shape" —
  # as if that were the end of it. IT WAS NOT, and this is the SIXTH instance of the class the
  # table above enumerates (blocker 15): a WINDOWED key ALIASES. Two distinct diagnostics of
  # equal length sharing both outer windows and differing only in the MIDDLE collapse into
  # one, so `count` UNDERCOUNTS while the field it feeds advertises a distinct total. The F12
  # fix CREATED it: bounding the key to fix memory reintroduced exactly the aliasing F1 and F5
  # were about.
  #
  # THE TENSION IS BOUNDED MEMORY vs EXACT DISTINCTNESS, AND A DIGEST RESOLVES BOTH — which is
  # what F12 should have answered. The key now carries a checksum computed over the WHOLE
  # normalised identity, so the retained string stays bounded (~2100 characters: two 1024-char
  # windows, the exact length and the two checksums) while distinctness no longer depends on
  # where the two identities happen to differ.
  #
  # WHAT THE DIGEST IS, STATED AT ITS STRENGTH AND NOT ABOVE IT: digest() is a PAIR OF ROLLING
  # POLYNOMIAL CHECKSUMS (Rabin-Karp shape, distinct bases and distinct prime moduli, combined
  # state ~2^61). IT IS NOT A CRYPTOGRAPHIC HASH and awk has no primitive that would make one
  # cheap. THE RESIDUAL, HONESTLY: a collision can be CONSTRUCTED by someone who wants one — a
  # log line is repository- and toolchain-authored text, not an authorization, so a constructed
  # collision buys an attacker one undercounted duplicate in a diagnostic field; an ACCIDENTAL
  # collision between two distinct real identities requires them to agree on the length, both
  # 1024-char windows AND both checksums, which does not happen to test paths, assertion
  # messages or compiler diagnostics. No guarantee stronger than that is implied anywhere.
  function key(t,   L) {
    L = length(t)
    if (L <= 4096) return t
    return "<oversize:" L ":" digest(t) ">" substr(t, 1, 1024) "<...>" substr(t, L - 1023)
  }
  # digest() — the checksum over the COMPLETE normalised identity. In awk, so nothing shells
  # out per line (a per-line `cksum`/`sha256sum` on a log full of long lines is a process per
  # line). Two accumulators, different bases and different prime moduli:
  #   h1 mod 2147483647 (2^31-1), base 131      -> max intermediate ~2.8e11
  #   h2 mod 1000000007,          base 8191     -> max intermediate ~8.2e12
  # both far below 2^53, so every step is EXACT in the doubles awk uses — no silent precision loss
  # turning the digest into a coarser function than it looks.
  #
  # CHARACTER CODES: printable ASCII is seeded in BEGIN, and any OTHER character (a UTF-8
  # character under a multibyte locale, a byte under LC_ALL=C) is assigned a distinct code at
  # first sight. That keeps the map INJECTIVE without depending on the locale or on an awk
  # `ord`, which POSIX awk does not have. The assignment order is per-run, which is fine and
  # deliberate: this key never leaves this process (see the never-published note above), so it
  # needs to be stable within the run, not across runs.
  function digest(t,   L, i, c, v, h1, h2) {
    L = length(t); h1 = 5381; h2 = 52711
    for (i = 1; i <= L; i++) {
      c = substr(t, i, 1)
      if (c in ORD) v = ORD[c]
      else { v = 1000 + ++ORDN; ORD[c] = v }
      h1 = (h1 * 131 + v) % 2147483647
      h2 = (h2 * 8191 + v) % 1000000007
    }
    return sprintf("%d.%d", h1, h2)
  }
  # tagof() — the TAG position of an identity: its first whitespace-delimited token, cut at
  # the first SINGLE `:`. A `::` is a Rust module-path separator, not a tag/detail delimiter,
  # so it is protected by a sentinel first (norm() has already mapped every control
  # character to a space, so \001 provably cannot occur in the input).
  function tagof(full,   t) {
    t = full
    sub(/[[:space:]].*$/, "", t)
    gsub(/::/, "\001", t)
    sub(/:.*$/, "", t)
    gsub(/\001/, "::", t)
    return t
  }
  # pubid() — the PUBLISHED IDENTIFIER for a tag / test path. A NAMED charset and a bound,
  # each REPLACING the value rather than trimming it.
  #
  # CHARSET `[A-Za-z0-9._:-]`, and every exclusion is load-bearing: no `@` (so an scp-form
  # `TOKEN@host` cannot be published), no `/` and no `://` (no URL), no `?`/`&`/`=` (no
  # query string), no whitespace (so a space-separated `api_key SECRET` cannot travel), and
  # no `#` (so an ordinal can never be forged by log content). A value outside it is
  # REPLACED, because a partially-scrubbed identifier is the "improve the sanitiser" move
  # CLAUDE.md forbids for this family.
  #
  # BOUND 256, MEASURED: the widest real assert identifier this repo can produce is a
  # libtest path — module depth 9 plus an 89-character test fn name is ~190 characters —
  # and the widest bash-suite tag measured in scripts/tests/*.sh is 41. 256 leaves headroom
  # for both; anything longer is not an identifier. The gate then MIDDLE-elides to 60 for
  # display, after the redaction, which is safe on a charset that cannot hold an authority.
  # pubdoc() — the PUBLISHED IDENTIFIER for a doctest. A THIRD projection beside pubid() and
  # publabel(), and it exists so the doctest rule does not hand add() a string it assembled
  # itself: every published value goes through exactly one named projection, which is what
  # makes the one-rule structural guard mean something (it caught this while it was being
  # written). Returns the pubid() fallback for anything not matching the closed shape, so the
  # rule never publishes a half-parsed line.
  # pubsh() — the PUBLISHED IDENTIFIER for a delta shell-test verdict (#3765, roborev job 67).
  # A5 emits `shell-selftest: scripts/tests/foo.sh FAIL`, whose identity is a repo-relative
  # PATH, so pubid() rejected it for the `/` and published a placeholder instead of naming the
  # failing script. Same shape as the doctest case, same remedy: a CLOSED grammar rather than a
  # widened charset. The value must be a path in a charset with no `:` and no `@`, ending `.sh`,
  # so a URL cannot satisfy it. Anything else falls back to pubid().
  function pubsh(t) {
    if (t ~ /^[A-Za-z0-9._\/-]+\.sh$/) return "shell-test " t
    return pubid(tagof(t))
  }
  function pubdoc(s,   dp, di, dl) {
    dp = s; sub(/ - .*$/, "", dp)
    di = s; sub(/^[^ ]+ - /, "", di); sub(/ ?\(line [0-9]+\)$/, "", di)
    dl = s; sub(/^.*\(line /, "", dl); sub(/\)$/, "", dl)
    if (dp !~ /^[A-Za-z0-9._\/-]+$/ || dl !~ /^[0-9]+$/) return pubid(tagof(norm(s)))
    # THE ITEM IS OPTIONAL: rustdoc reports an unnamed example as `<path> - (line N)`, which
    # under a mandatory-item rule fell through to the generic rule and published a charset
    # placeholder instead of naming the failing doctest (roborev job 69).
    if (di == "") return "doctest " dp " line " dl
    if (di !~ /^[A-Za-z0-9._:-]+$/) return pubid(tagof(norm(s)))
    return "doctest " dp " line " dl " (" di ")"
  }
  function pubid(t) {
    # ONLY `.` and `:` are stripped. `_` and `-` are VALID IDENTIFIER CHARACTERS, and
    # stripping them made `module::test_` publish as `module::test` — a DIFFERENT, possibly
    # real test — so a reader matching a flake against the field could match the wrong one
    # (#3765 blocker 17). That is the identity-fidelity class this issue keeps producing: a
    # normalisation that makes two distinct identities indistinguishable. The COUNT was never
    # wrong (it is computed on the full identity), but the PUBLISHED identifier is what a
    # reader matches on, which is the whole purpose of this field.
    sub(/[.:]+$/, "", t)                     # sentence/delimiter punctuation only, never identity chars
    if (t == "") return "<no identifier in the matched line>"
    if (t ~ /[^A-Za-z0-9._:-]/) return "<identifier outside the safe charset>"
    if (length(t) > 256) return "<identifier too long to publish safely: " length(t) " chars>"
    return t
  }
  # publabel() — the PUBLISHED IDENTIFIER for a guard label. Same rules, one wider charset:
  # a real guard label carries spaces and parentheses (`cli-tests Pass 1 (default)`). `:`
  # and `/` stay OUT, so `https://x: FAIL` publishes a placeholder rather than a URL.
  #
  # BOUND 70, MEASURED AND NOW DOCUMENTED (blocker 13 — it used to be an undocumented
  # `<= 70` in the B1 rule whose failure was a SILENT DROP): the widest guard label this
  # repo emits is `cli-tests Pass 1 (default)` at 26 characters, and the B1 shape
  # (`<anything>: FAIL`) is broad enough that a whole SENTENCE can match it — 70 is ~2.7x
  # the widest real label, so a real label is never replaced and a sentence always is. An
  # over-long label is COUNTED and publishes a placeholder: a match must never be reported
  # as a non-match.
  function publabel(t) {
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
    if (t == "") return "<no guard label in the matched line>"
    if (t ~ /[^A-Za-z0-9 ._()-]/) return "<guard label outside the safe charset>"
    if (length(t) > 70) return "<guard label too long to publish safely: " length(t) " chars>"
    return t
  }
  # lasttok() — the final whitespace-delimited token (A4: nextest prints `<pkg> <test>`).
  function lasttok(s,   a, n) { n = split(s, a, /[[:space:]]+/); return (n ? a[n] : "") }
  # add(tier, id, pub, collapse) — THE SPLIT BETWEEN COUNTING AND PUBLISHING.
  #   `id`       the FULL identity: the thing `count` counts. NEVER published.
  #   `pub`      the published IDENTIFIER. MANDATORY at every call site — a missing one is
  #              an affirmative placeholder, never the identity (the old empty-means-publish-
  #              the-payload default is exactly what blocker 11 removed).
  #   `collapse` 1 for a MANY-TO-ONE kind label (tier toolchain): a repeated label is named
  #              once and never ordinalised. 0 for a per-identity identifier, where a shared
  #              value is ORDINALISED so two distinct asserts never render as one string.
  # COUNT CORRECTNESS AND PUBLICATION SAFETY ARE SEPARATE CONCERNS AND NEITHER IS TRADED
  # FOR THE OTHER: `n[tier]` counts DISTINCT full identities, while the published values are
  # a projection of them. count >= name-count by construction, and the gate declares the
  # remainder.
  function add(tier, id, pub, collapse,   full, k, idx) {
    full = norm(id)
    if (full == "") return
    k = key(full)                            # bounded BEFORE retention; never published
    if ((tier SUBSEP k) in seen) return
    seen[tier SUBSEP k] = 1
    n[tier]++
    if (pub == "") pub = "<no published identifier: the recogniser supplied none>"
    if (collapse) {
      if ((tier SUBSEP pub) in seenpub) return
      seenpub[tier SUBSEP pub] = 1
    }
    idx = ++m[tier]
    base[tier SUBSEP idx] = pub
    occ[tier SUBSEP pub]++
  }
  # ORD is seeded ONCE, for the 95 printable ASCII characters digest() sees in practice.
  # BEGIN is the only place it can be done once per run; digest() extends it for anything
  # else (see its note on injectivity).
  BEGIN { for (_o = 32; _o <= 126; _o++) ORD[sprintf("%c", _o)] = _o }
  # ---- TIER assert ----
  # The IDENTITY is the WHOLE payload (F5: a tag is routinely SHARED, so tag-only dedup
  # UNDERCOUNTS). The PUBLISHED value is the tag (blocker 11: the detail carries runtime
  # values, and 205 measured `bad "…"` messages interpolate them).
  /^[[:space:]]*FAIL - / {
    s = $0; sub(/^[[:space:]]*FAIL - /, "", s)
    add("assert", s, pubid(tagof(norm(s))), 0); next
  }
  /^FAIL: / {
    s = $0; sub(/^FAIL: /, "", s)
    add("assert", s, pubid(tagof(norm(s))), 0); next
  }
  # A3a DOCTEST, and it MUST precede the generic libtest rule A3 below (#3765, roborev job 61).
  # A doctest failure is `test src/lib.rs - item (line 10) ... FAILED`. Under A3 alone, tagof()
  # cut at the first space and yielded `src/lib.rs`, which pubid() then REJECTED because `/` is
  # outside the safe charset — so the field published `<identifier outside the safe charset>`
  # and named nothing. `core-tests` runs `cargo test --doc` (agent-gate.sh:18725), so this is a
  # class of failure the gate ACTUALLY RUNS, and the whole point of this field is to name it.
  #
  # `/` IS PUBLISHED HERE AND ONLY HERE, and it is safe for a stated reason rather than by
  # assumption: the rule is a CLOSED, FULLY ANCHORED grammar. The whole line must match
  # `test <path> - <item> (line <n>) ... FAILED`, the path is re-validated against a charset
  # that admits NO `:` and NO `@`, and the item and line are matched separately. A URL cannot
  # satisfy that shape, so admitting `/` cannot readmit an authority — which is the property
  # the charset exists to guarantee, not the charset itself.
  /^[[:space:]]*test [^:@]+ - .*\(line [0-9]+\) \.\.\. FAILED/ {
    s = $0; sub(/^[[:space:]]*test /, "", s); sub(/ \.\.\. FAILED.*$/, "", s)
    add("assert", s, pubdoc(s), 0); next
  }
  /^[[:space:]]*test .* \.\.\. FAILED/ {
    s = $0; sub(/^[[:space:]]*test /, "", s); sub(/ \.\.\. FAILED.*$/, "", s)
    add("assert", s, pubid(tagof(norm(s))), 0); next
  }
  /^[[:space:]]*FAIL \[/ {
    s = $0; sub(/^[[:space:]]*FAIL \[[^]]*\][[:space:]]*/, "", s)
    add("assert", s, pubid(tagof(lasttok(norm(s)))), 0); next
  }
  /^shell-selftest: .* FAIL$/ {
    s = $0; sub(/^shell-selftest: /, "", s); sub(/ FAIL$/, "", s)
    add("assert", s, pubsh(norm(s)), 0); next
  }

  # ---- TIER guard ----
  # B1/B2 publish `<label> (<VERDICT>)`, which is already STRUCTURED — the label is charset-
  # constrained and bounded by publabel(), the verdict comes from the closed set matched.
  # Their trailing prose is DROPPED from the identity too, and that is deliberate and NOT
  # the A1 defect: what A1 discarded was a NAME (a shared tag standing in for many distinct
  # asserts), whereas what B1/B2 discard is an EXPLANATION of one verdict by one named
  # guard — two lines with the same label and verdict ARE one guard verdict. The residual is
  # DECLARED: a guard emitting the same label+verdict for two different failures counts once.
  # B3 has no label, so it keeps the FULL line as its identity and publishes the leading
  # token of the detail (the DECLARED weak-pointer residual it shares with A2).
  /^\[FAIL\][[:space:]]/ {
    s = $0; sub(/^\[FAIL\][[:space:]]+/, "", s)
    add("guard", s, pubid(tagof(norm(s))), 0); next
  }
  /^\[[^]]+\] FAIL(-CLOSED)?[: ]/ {
    s = $0; v = "FAIL"
    if (s ~ /^\[[^]]+\] FAIL-CLOSED/) v = "FAIL-CLOSED"
    sub(/^\[/, "", s); sub(/\].*$/, "", s)
    add("guard", s " (" v ")", publabel(norm(s)) " (" v ")", 0); next
  }
  /^[^[:space:]].*: (FAIL-CLOSED|FAIL|REFUSED|STALE)([^A-Za-z0-9-]|$)/ {
    s = $0
    if (match(s, /: (FAIL-CLOSED|FAIL|REFUSED|STALE)([^A-Za-z0-9-]|$)/)) {
      lbl = substr(s, 1, RSTART - 1)
      v = substr(s, RSTART + 2, RLENGTH - 2)
      gsub(/[^A-Za-z-]/, "", v)
      # UNCONDITIONAL. This used to be `if (length(lbl) <= 70) add(...)`, so a longer
      # label was SILENTLY DROPPED and the unconditional `next` below then stopped every
      # LATER tier from seeing the line — the field reported `0 RECOGNISED`, which reads as
      # "scanned, nothing matched", when a guard pattern HAD matched. An absence must be a
      # MEASUREMENT, never a silence (blocker 13). publabel() turns the bound into an
      # affirmative placeholder, so the match is always counted and the tier still wins.
      add("guard", lbl " (" v ")", publabel(norm(lbl)) " (" v ")", 0)
    }
    next
  }

  # ---- TIER toolchain ----
  # EVERY rule here passes a CLOSED-ENUM KIND LABEL LITERAL and `collapse=1`. The matched
  # line is the dedup key (so the count is true) and is NEVER published. A future rule added
  # without a label would publish the placeholder, not the line — and the shape is pinned
  # STRUCTURALLY in scripts/tests/test_agent_gate_summary.sh (3765-toolchain-*), with the
  # gate REFUSING on the OUTPUT PATH any toolchain name that is not a bare label token, since
  # a source scan cannot see a runtime value.
  /^error(\[[A-Za-z0-9]+\])?: / { add("toolchain", $0, "rustc-cargo-error", 1); next }
  /^npm error / { add("toolchain", $0, "npm-error", 1); next }
  /^Error: / { add("toolchain", $0, "capitalised-error", 1); next }
  /^bash: / { add("toolchain", $0, "bash-error", 1); next }
  /^Diff in .* at line [0-9]+:/ { add("toolchain", $0, "rustfmt-diff", 1); next }

  END {
    split("assert guard toolchain", order, " ")
    for (i = 1; i <= 3; i++) {
      t = order[i]
      if (!(t in n)) continue
      printf "tier=%s\n", t
      printf "count=%d\n", n[t]
      # count is over DISTINCT IDENTITIES (n), the names over the PUBLISHED projections
      # (m). For a collapsing tier m <= n and the gate declares the difference as
      # `(+K more)`. A published value shared by two or more DISTINCT identities is
      # ORDINALISED in file order, so no two distinct identities ever render as one string.
      lim = (m[t] < max ? m[t] : max)
      for (c = 1; c <= lim; c++) {
        b = base[t SUBSEP c]
        if (occ[t SUBSEP b] > 1) printf "name=%s#%d\n", b, ++ord[t SUBSEP b]
        else printf "name=%s\n", b
      }
      exit 0
    }
  }
' < "$log"
