#!/usr/bin/env python3
"""Census + extraction of the python `scripts/perf/ws0-baseline.sh` carries INSIDE itself.

Three subcommands, and the split is the same one `ws0_hermeticity_lint.py` makes for the same
reason — a DEFINITION and an ORACLE must not be the same thing:

    census <driver>        classify EVERY `python3` occurrence in the driver; fail closed on a
                           shape this file cannot classify or a block it cannot delimit
    emit <driver> <n>      print embedded block <n>'s SOURCE, exactly as the driver ships it
    compile <driver>       compile EVERY embedded block; print one finding per block that does not

Every mode prints a `#COMPLETE …` marker on success. The CALLER counts findings, so a crash
must not read as "clean" — the marker is what makes that distinguishable, and every caller
checks for it.

# WHY THIS FILE EXISTS (issue #3451)

`ws0-baseline.sh` shipped for months with SEVEN instances of a defect that makes CPython refuse to
parse: a backslash inside an f-string EXPRESSION (the tokenizer reads it as a line continuation).
They were spread across the driver's TWO multi-line embedded blocks — the session-corpus-pin step
and the CPU-pin-verification step — and both steps are fatal-on-failure, so the entire WS0
measurement path was unrunnable end to end.

Nothing caught it because the driver's python is INVISIBLE to every tool that would have:

* it is not a `.py` file, so no linter, formatter or import ever sees it;
* the hermetic self-tests all stop at `--validate-args-only`, which sits ABOVE both steps, and
  #3272 round 2 finding 14 deliberately made the accept-direction cases execute NOTHING (running
  the real driver invoked `sudo sysctl` and three cargo builds inside a gate component);
* `bash -n` parses the file as SHELL, where a python body inside `python3 -c '…'` is one opaque
  single-quoted STRING — syntactically valid whatever it contains.

So the code was PARSED BY NOTHING until an operator ran the rig.

# THE EXTRACTION IS THE POINT, and it is why this is not a copy of the two blocks

A self-test carrying its own copy of the block certifies THE COPY. The copy does not drift when
the driver does — it stays green while the shipped step is broken, which is precisely the state
this issue found. So the subject is read out of the shipped file on every run.

# WHICH INTERPRETER `compile` SPEAKS FOR

`compile()` answers "does this parse on the interpreter running this check", and that is the whole
of what `compile` mode claims. A PEP 701 tokenizer model that tried to answer the cross-interpreter
question lived here for two rounds and was removed (#3451 review round 4): it was wrong twice, the
second time in the FALSE-RED direction on legal triple-quoted code, and a second implementation of
CPython's tokenizer is correct only insofar as it is differentially tested against the original —
which needs the interpreters we do not have. The caller's NOT-REACHED list records the limit and
names the only honest oracle for it.

# FAIL-CLOSED, because an extractor that finds nothing prints like a clean driver

Every `python3` occurrence must land in a classification this file recognises. An occurrence it
cannot classify, or an embedded block whose closing delimiter it cannot find, is a FINDING naming
the driver and the line — never a silent omission from the census. The alternative posture (skip
what you do not understand) is the one that would let instance #8 ship in a shape slightly unlike
the first seven.

## THE CLASSIFICATION IS AN ALLOWLIST, and that is why it closes

Earlier versions recognised DANGEROUS shapes in order to refuse them. That is a BLOCKLIST over an
open set — the list of shell constructs does not close — and it cost SEVEN findings across five
review rounds, ending with a path-qualified `/usr/bin/python3` that was not refused but INVISIBLE.
`census` lists all seven with their measurements.

Now the candidate net is as wide as it goes and exactly four shapes are interpreted:

    python3 -c '…'    bare `python3`, single-quoted, the quote closing at a shell word boundary.
                      Extracted. Covers the driver's two multi-line steps and its inline block.
    python3 <f>.py    bare `python3` running a SCRIPT FILE. Recorded, not extracted — an ordinary
                      python file is something every other tool already reads.
    for … in <list>   the membership probe at `ws0-baseline.sh:421`: a word in a LIST, not a
                      command, so it carries no code.
    anything else     A FINDING. Not classified, not skipped, not modelled.

A construct nobody anticipated therefore becomes a finding by FAILING TO MATCH, rather than by
someone having thought to refuse it.

THE ALLOWLIST CLOSES CLASSIFICATION, NOT DISCOVERY, and the difference is worth stating because it
is the residual (#3451 round 7). A candidate must be FOUND before it can be classified, and a
shell command word can be spelled arbitrarily — a variable, a concatenation, `$(which python3)`,
an alias, `eval`. Enumerating those spellings is the same open list one level down, so it is not
attempted. Instead there are TWO anchors covering the decidable part: a literal `python` word (any
argument shape), and the `-c` FLAG (any command-word spelling — `$PYTHON -c '…'` is found because
the anchor does not depend on the word). What remains undiscovered is an indirectly-spelled
command word combined with a NON-`-c` form (`$PYTHON <<'PY'`) or code reaching python through
`eval`. Closing that needs an interpreter for bash. It is a decision, not an oversight; the
caller's NOT-REACHED list says the same thing where an operator will read it. HEREDOCS ARE NOT SUPPORTED and are findings: support for them
was speculative (this driver has none) and produced three separate false passes — an unquoted
delimiter is shell-expanded before python sees it, a composed delimiter makes bash use a different
tag, and with several redirects bash uses the last. Each compiled a body python never receives. A
future heredoc step is told to teach the census, which is the correct outcome.

## HOW A `-c '…'` BLOCK IS DELIMITED, and the three closer shapes that forced the rule

A bash single-quoted string HAS NO ESCAPE MECHANISM, so the body runs to the NEXT `'` — with one
exception, the `'"'"'` idiom, which CLOSES the string, emits a literal apostrophe from a
double-quoted segment, and REOPENS it. That single sentence is the whole delimiter, and it is
stated as a property of bash rather than as a pattern over lines, because a pattern over lines is
what got this wrong twice:

  shape 1  the closer alone at column 0            `' "$HERE" "$CORPUS" …`   (this driver)
  shape 2  the closer at the END of the last python line, bash arguments trailing
                                                    `print(rows)' "$DEST/x.jsonl"`
  shape 3  `'"'"'` MID-BLOCK — a literal apostrophe inside the body

MEASURED over this repository, same probe, three delimiters:

  column-0 closer only    31 blocks, 5 reported UNDELIMITED (shape 2 is idiomatic and in use at
                          `test-data/scripts/gen-perf-corpus-bti.sh`, `scripts/lib/gate-notify.sh`
                          and `docs/reports/ws0-3217-artifacts/harness/common.sh`)
  exact next quote        59 blocks, 1 FALSE SyntaxError (shape 3 cut mid-body)
  the rule above          59 blocks, 0 undelimited, 0 failing to parse

Both wrong versions are instructive in OPPOSITE directions and neither is safe:

* the column-0 rule UNDER-COUNTS — 31 subjects instead of 59. A loose delimiter does not merely
  mis-cut, it silently drops blocks from the census, which is the vacuous-green shape; and it
  reds the gate on CORRECT code, which is how a guard gets waived into uselessness.
* the exact-next-quote rule MANUFACTURES a finding on a good file. `lib-ws0-fixtures.sh` carries
  the `'"'"'` idiom, and its own comment records what happens when it is mishandled: the library
  is "silently truncated … and it presented as every OTHER case in the suite failing on an absent
  pinning-verification.json."

So the suite asserts BOTH directions: a defect must be reported, and a good file carrying the
idiom must NOT be.

## SCOPE: the caller lints `ws0-baseline.sh`, and the allowlist is that driver's shapes

Run over other files this census reports findings on shapes that driver does not contain — a
script path held in a VARIABLE, a DOUBLE-quoted `-c` body, a heredoc, `python3` reading a pipe.
Those are findings BY DESIGN rather than gaps: each would have to be TAUGHT before this census
could lint the whole tree, and teaching is a deliberate act someone reviews. That generalisation
is not this issue.

## The direction it errs in, stated

Over-inclusion, deliberately, and now at BOTH levels: the candidate net matches more words than
are invocations, and the allowlist admits fewer shapes than are safe. A `python3 -c '…'` written
INSIDE a shell string (an `echo` of an example) is censused as a block; a legitimate construct the
allowlist has not been taught is a finding. Both cost noise, which is recoverable. The opposite
posture — skipping what you do not understand — is how a step reaches an operator having been
parsed by nothing, and it is the state this file was written to end.
"""

from __future__ import annotations

import bisect
import pathlib
import re
import sys

# THE CANDIDATE NET, CAST AS WIDE AS IT GOES (#3451 review round 6).
#
# Any word whose BASENAME is `python`/`python3`/`python3.11` — path-qualified or not — is a
# candidate: `/usr/bin/python3`, `./python3`, `$HOME/bin/python3`, bare `python3`. Deliberately
# over-matching, because a candidate that is SEEN and refused is safe while one that is never seen
# is invisible. That was r6F3: `/usr/bin/python3 -c '<a syntax error>'` produced
# `blocks=0 findings=0 occurrences=0` — a defective block the census did not merely mis-handle but
# never noticed, reported as clean.
#
# The only lexical narrowing is a token boundary: a trailing word or `.` character means this is
# not a word of its own (`requires python3.` inside an `echo` string, `python3x`). That is a rule
# about identifiers, not about shell.
_PY_WORD = re.compile(r"python[0-9]*(?:\.[0-9]+)*(?![\w.])")

# THE SECOND DISCOVERY ANCHOR: the FLAG, not the command word (#3451 review round 7, finding 1).
#
# The allowlist closed CLASSIFICATION — anything not allowlisted is a finding — but not
# DISCOVERY: a candidate has to be found before it can be classified, and `$PYTHON -c '…'` or
# `py"thon3" -c '…'` contains no literal `python` word, so it was INVISIBLE. Anchoring on `-c`
# followed by whitespace and a quote catches EVERY command-word spelling of the `-c` form,
# because the anchor no longer depends on how the command word is written.
#
# Verified false-red-free for this driver before adopting: its only `-c '` occurrences are the
# three python3 blocks (599, 697, 941), each consumed by the python-word branch before this
# anchor is reached; the sole other `-c` is `taskset -c 1` inside a whole-line COMMENT, excluded
# twice over (comment, and no quote follows). No bash/sh/zsh/perl/ruby/node/env `-c` exists here.
_DASH_C_ANCHOR = re.compile(r"(?<![\w-])-c\s+['\"]")

# Characters that cannot appear inside one shell word, used to find where a candidate's word
# STARTS so a path prefix is captured with it.
_WORD_BREAK = frozenset(" \t\n;&|<>()'\"`")

# ---------------------------------------------------------------------------
# THE ALLOWLIST — the only shapes this census will interpret
# ---------------------------------------------------------------------------
# `-c` followed by a single quote. ONE branch covers both the multi-line form (the quote ends the
# line, driver blocks 1-2) and the inline form (driver block 3), because where the string CLOSES is
# found by scanning bash's quoting rules rather than by a line pattern.
_OPEN_DASH_C = re.compile(r"^\s*-c\s+'")

# The `'"'"'` idiom: close, emit a literal apostrophe from a double-quoted segment, reopen. The one
# exception to "a single-quoted string runs to the next quote".
_QUOTE_IDIOM = "'" + '"' + "'" + '"' + "'"

# WHAT MAY LEGALLY FOLLOW A BLOCK'S CLOSING QUOTE (#3451 review round 5, finding 1).
#
# Bash CONCATENATES adjacent word fragments, so `python3 -c 'pass'" +"` runs `pass +`. Extracting
# the quoted part alone approves a program python NEVER RECEIVES — measured: bash raised
# `SyntaxError: invalid syntax` while the census reported `compiled=1 findings=0`. A FALSE PASS.
#
# The boundary set is shell metacharacters, NOT whitespace: the driver's own inline block at
# `ws0-baseline.sh:941` closes `')" || {`, i.e. with a `)` immediately after the quote, so a
# whitespace-only rule would flag 1 of the driver's own 3 blocks on the first run. (The two
# multi-line blocks close `' "$HERE" …`, a space, and are unaffected either way.)
_WORD_BOUNDARY_AFTER_CLOSE = frozenset(" \t\n)&;|<>")

# The ONE presence-probe construct the driver contains: `for tool in perf taskset python3; do`.
# The token is a word in a LIST, not a command.
_FOR_WORD_LIST = re.compile(r"^\s*for\s+[A-Za-z_][A-Za-z0-9_]*\s+in\s")

# A script-file argument, quoted or not, possibly carrying a shell expansion in its directory
# part: `"$HERE/ws0_report.py"` (driver line 981). Recorded, never extracted — an ordinary python
# file is something every other tool already reads.
_SCRIPT = re.compile(r"^[\"']?[^\s\"']*\.py[\"']?(\s|$)")


class Unclassifiable(Exception):
    """A `python3` occurrence, or a block delimiter, this file will not guess about."""

    def __init__(self, lineno: int, reason: str) -> None:
        super().__init__(reason)
        self.lineno = lineno
        self.reason = reason


def _strip_comment(rest: str) -> str:
    """Drop a trailing `# …` comment from the text FOLLOWING a python3 token.

    Only where the `#` starts a word — `a#b` is not a comment in bash, and the driver's
    `# perf-lint-allow` marker is exactly the shape this must remove.
    """
    m = re.search(r"(?:^|\s)#", rest)
    return rest[: m.start()] if m else rest


def _scan_single_quoted(text: str, open_quote: int) -> tuple[str, int]:
    """The bash single-quoted string opening at `text[open_quote]`, and the offset after it.

    Bash gives a single-quoted string NO escape mechanism, so the body runs to the next `'` —
    unless that quote opens the `'"'"'` idiom, which closes the string, emits a literal apostrophe
    and reopens it. Both facts are bash's, which is why this is a scan and not a pattern: the
    closer appears at column 0 in this driver, at the end of the last body line in three other
    files in this repository, and mid-body as the idiom. See the header for the measurements.
    """
    body: list[str] = []
    i = open_quote + 1
    while i < len(text):
        ch = text[i]
        if ch != "'":
            body.append(ch)
            i += 1
            continue
        if text[i : i + len(_QUOTE_IDIOM)] == _QUOTE_IDIOM:
            body.append("'")
            i += len(_QUOTE_IDIOM)
            continue
        return "".join(body), i
    raise Unclassifiable(
        text.count("\n", 0, open_quote) + 1,
        "an embedded `python3 -c '` block is never closed: no terminating single quote before"
        " end-of-file. Extracting to end-of-file would compile a truncated body and report a"
        " defect that is the extractor's own.",
    )


def census(path: pathlib.Path) -> tuple[list[dict], list[dict]]:
    """Classify every `python`/`python3` candidate. Returns (records, findings).

    AN ALLOWLIST, NOT A BLOCKLIST (#3451 review round 6). Seven findings across five review rounds
    were one cause: the census recognised DANGEROUS shapes in order to refuse them, and a
    recogniser of dangerous shapes is a blocklist over an open set. It could not close.

        r2F3  only the FIRST candidate per line was censused
        r4F2  argumentless python3 (stdin / a pipe) classified harmless
        r5F1  adjacent shell-word fragments after the closing quote
        r5F2  an unquoted heredoc body is shell-expanded before python sees it
        r6F1  a composed heredoc delimiter (`<<'PY'X` -> bash uses `PYX`)
        r6F2  multiple heredoc redirects -> bash uses the LAST, this took the first
        r6F3  a path-qualified `/usr/bin/python3` was not matched at all — INVISIBLE

    So the net is cast as wide as possible and only FOUR shapes are interpreted:

        1. bare `python3 -c '<single-quoted>'`, the quote closing at a shell word boundary
           (covers the driver's multi-line blocks 1-2 and its inline block 3);
        2. bare `python3 <path>.py …` — a SCRIPT file, recorded and not extracted;
        3. the `for … in <list>` membership probe, which is a word in a list and not a command;
        4. nothing else.

    EVERYTHING ELSE IS A FINDING — not classified, not skipped, not modelled. A shell construct
    nobody has thought of becomes a finding automatically, by failing to match, so the enumeration
    stops being ours to complete. This is the repository's own posture: allowlist-validate fail
    closed rather than blocklist the dangerous form.

    NOTE the allowlist requires the BARE word `python3`. A path-qualified invocation is a
    candidate (so it is seen) and a finding (so it is named) — that is r6F3 going from invisible
    to reported.

    THE SCAN IS BY POSITION, not by line (r2F3): the cursor advances past exactly what each
    classification consumed, so a second candidate after a `;`, an `&&` or an inline block's
    closing quote is still examined.
    """
    text = path.read_text()
    lines = text.split("\n")
    starts: list[int] = []
    off = 0
    for line in lines:
        starts.append(off)
        off += len(line) + 1
    records: list[dict] = []
    findings: list[dict] = []
    pos = 0
    while True:
        # TWO anchors, earliest wins. A `python3 -c '…'` matches the WORD first (lower offset) and
        # its branch consumes through the closing quote, so the `-c` inside it is never reached —
        # no double report. A `-c '` reached FIRST means no literal python word preceded it, i.e.
        # the command word is spelled indirectly, which is a finding.
        mp = _PY_WORD.search(text, pos)
        mc = _DASH_C_ANCHOR.search(text, pos)
        if mp is not None and (mc is None or mp.start() <= mc.start()):
            m, anchored_on_flag = mp, False
        elif mc is not None:
            m, anchored_on_flag = mc, True
        else:
            break
        idx = bisect.bisect_right(starts, m.start()) - 1
        line = lines[idx]
        line_start = starts[idx]
        if anchored_on_flag:
            pos = m.end()
            if line.lstrip().startswith("#"):
                continue  # a whole-line shell comment carries no code
            # The word before the flag, for the diagnostic only — enough to name what was found
            # without pretending to resolve it.
            before = text[line_start : m.start()].rstrip()
            cmd_word = before[max((before.rfind(c) for c in _WORD_BREAK), default=-1) + 1 :]
            findings.append({
                "line": idx + 1,
                "reason": "a `-c '<program>'` invocation whose command word is"
                          f" {cmd_word or '(unresolvable)'!r}, not the literal `python3` the"
                          " allowlist recognises. Anchored on the FLAG rather than the command"
                          " word, because an indirectly-spelled word (a variable, a"
                          " concatenation) carries code exactly as a literal one does and was"
                          " INVISIBLE before #3451 round 7. Teach the allowlist if intended.",
            })
            continue
        # Walk left to the start of the shell WORD, so a path prefix travels with the candidate.
        word_start = m.start()
        while word_start > line_start and text[word_start - 1] not in _WORD_BREAK:
            word_start -= 1
        word = text[word_start : m.end()]
        pos = m.end()
        if line.lstrip().startswith("#"):
            continue  # a whole-line shell comment carries no code
        if word.rsplit("/", 1)[-1] != text[m.start() : m.end()]:
            # The word's BASENAME is not the matched token, i.e. the token is a SUFFIX of a longer
            # program name (`mypython3`, `jython3`). That is a different program, not a
            # path-qualified python — the path case (`/usr/bin/python3`) has the token AS its
            # basename and is examined. This is the one lexical exclusion, and it is a statement
            # about names rather than about shell.
            continue
        line_end = line_start + len(line)
        raw_rest = text[m.end() : line_end]
        rest = _strip_comment(raw_rest).strip()
        try:
            if word != "python3":
                raise Unclassifiable(
                    idx + 1,
                    f"this python invocation is spelled {word!r}, not the bare `python3` the"
                    " allowlist recognises. It is REPORTED rather than skipped because a"
                    " path-qualified invocation carries code exactly as a bare one does, and"
                    " until #3451 round 6 this shape was INVISIBLE to the census — a defective"
                    " block reported as clean. Teach the allowlist if the spelling is intended.",
                )
            if _FOR_WORD_LIST.match(line) and rest.lstrip(";&|)").strip() in ("", "do", "then"):
                # ALLOWLIST 3: a word in a `for … in <list>` membership test, not a command.
                records.append({"kind": "MENTION", "line": idx + 1, "text": line.strip()})
                continue
            dash_c = _OPEN_DASH_C.match(raw_rest)
            if dash_c:
                # ALLOWLIST 1.
                open_quote = m.end() + dash_c.end() - 1
                body, close = _scan_single_quoted(text, open_quote)
                after = text[close + 1 : close + 2]
                if after and after not in _WORD_BOUNDARY_AFTER_CLOSE:
                    raise Unclassifiable(
                        idx + 1,
                        "this block's closing quote is followed by "
                        f"{after!r} rather than a shell word boundary, so bash CONCATENATES what"
                        " follows onto the program — python would receive something other than"
                        " the quoted text, and compiling the quoted text alone would approve a"
                        " program that is never run. Adjacent fragments are refused, not"
                        " reassembled: put the whole program inside one quoted string.",
                    )
                end_line = text.count("\n", 0, close)
                pos = close + 1
                records.append(
                    {"kind": "BLOCK",
                     "shape": "dash-c-multiline" if "\n" in body else "dash-c-inline",
                     "line": idx + 1, "end": end_line + 1,
                     "body": body if body.endswith("\n") else body + "\n"}
                )
                continue
            if _SCRIPT.match(rest):
                # ALLOWLIST 2.
                records.append({"kind": "SCRIPT", "line": idx + 1, "text": rest})
                continue
            raise Unclassifiable(
                idx + 1,
                "this `python3` invocation matches none of the shapes the census interprets"
                f" ({rest[:60]!r}). It may be carrying embedded code the compile check would"
                " therefore never see — via stdin, a heredoc, a variable, a double-quoted `-c`"
                " body — so it is a finding rather than a skip. If the shape is legitimate,"
                " TEACH THE ALLOWLIST — this finding is about the census, not about the python.",
            )
        except Unclassifiable as exc:
            findings.append({"line": exc.lineno, "reason": exc.reason})
    return records, findings


def _blocks(records: list[dict]) -> list[dict]:
    return [r for r in records if r["kind"] == "BLOCK"]


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__.splitlines()[0], file=sys.stderr)
        print("usage: ws0_embedded_python.py census|compile|emit"
          " <file> [n]", file=sys.stderr)
        return 2
    mode, driver = argv[1], pathlib.Path(argv[2])
    if not driver.is_file():
        # NONZERO (#3451 review round 7, finding 2). This used to `return 0`, so an absent or
        # unreadable driver produced a message and a SUCCESS exit — and a caller that checks the
        # status (rather than parsing the text) read "no subject at all" as "nothing wrong". The
        # message is kept for a human; the status is what a script must be able to trust.
        print(f"{driver}:0: the driver is not a readable file, so the census has NO SUBJECT —"
              " which prints exactly like a driver with nothing wrong in it.")
        return 4
    records, findings = census(driver)
    blocks = _blocks(records)
    if mode == "census":
        for f in findings:
            print(f"{driver}:{f['line']}: {f['reason']}")
        for i, r in enumerate(blocks, start=1):
            print(f"BLOCK\t{i}\t{r['line']}\t{r['end']}\t{r['shape']}")
        for r in records:
            if r["kind"] != "BLOCK":
                print(f"{r['kind']}\t{r['line']}\t{r.get('text', '')}")
        print(f"#COMPLETE blocks={len(blocks)} findings={len(findings)}"
              f" occurrences={len(records)}")
        return 0
    if mode == "compile":
        # EVERY embedded block, so instance #8 anywhere in the driver is caught rather than only
        # the two steps #3451 repaired. A census finding is reported here too: a block the census
        # could not delimit is a block this check did not compile, and silence about it would be
        # the vacuous pass.
        for f in findings:
            print(f"{driver}:{f['line']}: {f['reason']}")
        for i, r in enumerate(blocks, start=1):
            try:
                compile(r["body"], f"{driver}:block{i}@{r['line']}", "exec")
            except SyntaxError as exc:
                print(f"{driver}:{r['line']}: embedded python block {i} ({r['shape']}) DOES NOT"
                      f" COMPILE — {type(exc).__name__}: {exc.msg} (block-relative line"
                      f" {exc.lineno}). This step is fatal-on-failure, so the driver cannot run"
                      " past it.")
        print(f"#COMPLETE compiled={len(blocks)} findings={len(findings)}")
        return 0
    if mode == "emit":
        if len(argv) < 4:
            print("usage: ws0_embedded_python.py emit <driver> <n>", file=sys.stderr)
            return 2
        n = int(argv[3])
        if not 1 <= n <= len(blocks):
            print(f"{driver}:0: block {n} was requested and the census found {len(blocks)}",
                  file=sys.stderr)
            return 3
        sys.stdout.write(blocks[n - 1]["body"])
        return 0
    print(f"unknown mode {mode!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
