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

## The classification, and what makes it closed

A `python3` invocation can only receive code four ways: `-c`, a script path, standard input, or
`-m`. So the census keys on WHICH of those a given occurrence uses rather than on how it is
spelled:

    -c '…'        embedded, single-quoted — the shape both defective steps use. Extracted, by
                  the rule below rather than by a line pattern.
    <<'EOF'       embedded via a heredoc on stdin. Extracted. (None today; covered because a
                  future step written this way must not fall outside the census.)
    <path>.py     a SCRIPT FILE. Not embedded — it is an ordinary python file every other tool
                  already sees — so it is recorded and not extracted.
    (no argument) a MENTION **only** for the one presence-probe construct this driver uses — the
                  word inside a `for … in <list>` membership test. EVERY OTHER argumentless
                  invocation is UNKNOWN, because `python3` with no argument reads its program from
                  STDIN: `producer | python3` is embedded code with no argument at all, and
                  classifying it as harmless was a hole in the fail-closed census (#3451 review
                  round 4). Fail-closed means the shapes do NOT have to be enumerated — recognise
                  the one that occurs, and let the rest be findings.

Anything else — `-c` with a shape that cannot be delimited, `-m`, a bare `-`, a variable where the
code should be — is UNKNOWN, which is a finding.

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

## SCOPE: this file's caller lints `ws0-baseline.sh`, and the UNKNOWN class is calibrated for it

The census is used by `test_ws0_embedded_steps_execute.sh` against ONE driver. Run tree-wide it
reports UNKNOWN on shapes that driver does not contain and that this file deliberately does not
guess about: a script path held in a VARIABLE (`python3 "$ROWS_PY" …`, where nothing on the line
says whether it is python), a DOUBLE-quoted `-c` body (the shell expands it, so the bytes python
receives are not the bytes on disk), and `python3 >/dev/null` reading stdin from a pipe. Each is a
real shape in this repository, each is fail-closed here, and each would have to be taught — not
skipped — before this census could lint the whole tree. That generalisation is deliberately NOT
this issue.

## The direction it errs in, stated

Over-inclusion, deliberately. A `python3 -c '…'` written INSIDE a shell string (an `echo` of an
example, say) is censused as a block and compiled, which costs a spurious finding at worst. The
opposite posture — skipping anything that might not be real code — is how a step reaches an
operator having been parsed by nothing, and it is the state this file was written to end.
"""

from __future__ import annotations

import bisect
import pathlib
import re
import sys

# The word, matched only where it is a standalone token. `requires python3.` inside an `echo`
# string is not an invocation and must not be censused as an unknown shape; a trailing
# word/dot character is the discriminator.
_PY_TOKEN = re.compile(r"(?<![\w./-])python3(?![\w.])")

# The OPENING of a `-c '…'` block: the `-c` flag and the single quote that starts the shell
# string. Where it CLOSES is decided by scanning bash's quoting rules (`_scan_single_quoted`),
# never by a line pattern — see the header's three-shape section.
_OPEN_DASH_C = re.compile(r"^\s*-c\s+'")
# The `'"'"'` idiom: close, emit a literal apostrophe from a double-quoted segment, reopen. The one
# exception to "a single-quoted string runs to the next quote".
_QUOTE_IDIOM = "'" + '"' + "'" + '"' + "'"
# A heredoc redirect: `<<'PY'`, `<<PY`, `<<-'PY'`. The `dash` group is captured because `<<-`
# and `<<` have DIFFERENT terminator rules — see `_delimit_heredoc`.
_HEREDOC = re.compile(
    r"<<(?P<dash>-?)\s*(?P<q>['\"]?)(?P<tag>[A-Za-z_][A-Za-z0-9_]*)(?P=q)"
)
# The ONE presence-probe construct the driver contains: `for tool in perf taskset python3; do`.
# The token is a word in a LIST, not a command. Recognised explicitly so that every OTHER
# argumentless invocation can fall through to UNKNOWN — see the MENTION branch in `census`.
_FOR_WORD_LIST = re.compile(r"^\s*for\s+[A-Za-z_][A-Za-z0-9_]*\s+in\s")

# A script-file argument, quoted or not, possibly carrying a shell expansion in its directory
# part: `"$HERE/ws0_report.py"`.
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


def _delimit_heredoc(
    lines: list[str], start: int, tag: str, dash: bool
) -> tuple[str, int]:
    """Body of the heredoc opened on `lines[start]`, plus the line its terminator sits on.

    THE TERMINATOR RULE IS THE SHELL'S, not `.strip()` (#3451 review round 1, finding 2). The two
    forms differ and conflating them is wrong in both directions:

    * `<<TAG`  — the terminator must be the line EXACTLY. A space-indented `  TAG` is ORDINARY
      BODY to the shell, so accepting it truncates the block early and hands python a body it
      never receives. A `.strip()` comparison accepted it.
    * `<<-TAG` — leading TABS (never spaces) are stripped from the terminator AND from every body
      line. A `.strip()` comparison found the terminator but left the body's tabs in place, so a
      tab-indented body compiled as an IndentationError the shell would never produce.

    Both are latent today — this driver carries no heredoc — but the branch SHIPS and is
    exercised, and a wrong implementation of a shipped branch is a trap for whoever first writes
    a step in that shape.
    """
    body: list[str] = []
    for i in range(start + 1, len(lines)):
        line = lines[i]
        candidate = line.lstrip("\t") if dash else line
        if candidate == tag:
            return "\n".join(body) + "\n", i
        body.append(candidate)
    raise Unclassifiable(
        start + 1,
        f"an embedded python heredoc opened with `{'<<-' if dash else '<<'}{tag}` is never"
        " terminated by a line the SHELL would accept as its terminator"
        + (" (leading tabs stripped)" if dash else " (an exact match, indentation included)")
        + ", so its body cannot be delimited.",
    )


def census(path: pathlib.Path) -> tuple[list[dict], list[dict]]:
    """Classify every `python3` occurrence. Returns (records, findings).

    THE SCAN IS BY POSITION, NOT BY LINE (#3451 review round 2, finding 3). An earlier version
    searched each line ONCE and then skipped whole lines, so a SECOND invocation on the same
    line — after a `;`, an `&&`, or an inline block's closing quote — was silently dropped, which
    contradicted the "every occurrence" guarantee this file's fail-closed posture rests on.
    MEASURED against `python3 -c 'import sys'; python3 -c '<a syntax error>'`: `blocks=1
    occurrences=1`, and the defect in the second block was invisible to the compile check.

    So the cursor advances past exactly what each classification CONSUMED — the closing quote of
    a `-c` block, the line after a heredoc terminator, the token itself for a mention or a script
    invocation — and the remainder of every line is rescanned.
    """
    text = path.read_text()
    lines = text.split("\n")
    # Offset of each line start, so a match position can be turned back into a line number and a
    # per-line remainder. The quoting scanner works over the whole text because a block's closer
    # is not a property of any single line (shapes 1-3 in the header).
    starts: list[int] = []
    off = 0
    for line in lines:
        starts.append(off)
        off += len(line) + 1
    records: list[dict] = []
    findings: list[dict] = []
    pos = 0
    while True:
        m = _PY_TOKEN.search(text, pos)
        if not m:
            break
        idx = bisect.bisect_right(starts, m.start()) - 1
        line = lines[idx]
        if line.lstrip().startswith("#"):
            # A whole-line shell comment carries no code. Advance past THIS occurrence only, not
            # past the line: nothing else on a comment line matters, but the cursor rule stays one
            # rule everywhere.
            pos = m.end()
            continue
        line_end = starts[idx] + len(line)
        raw_rest = text[m.end() : line_end]
        rest = _strip_comment(raw_rest).strip()
        pos = m.end()
        try:
            if not rest or rest.lstrip(";&|)").strip() in ("", "do", "then"):
                # AN ARGUMENTLESS `python3` IS NOT AUTOMATICALLY HARMLESS (#3451 review round 4).
                # With no argument python3 reads its program from STDIN, so `producer | python3`
                # is embedded code carrying no argument at all — and treating every argumentless
                # invocation as a MENTION let exactly that bypass the census.
                #
                # So only the ONE presence-probe construct this driver contains is recognised: the
                # word inside a `for … in <list>` membership test, which is a word in a LIST and
                # not a command at all. Everything else falls through to UNKNOWN below. That is
                # what fail-closed buys — the shapes do not have to be enumerated.
                if _FOR_WORD_LIST.match(line):
                    records.append({"kind": "MENTION", "line": idx + 1, "text": line.strip()})
                    continue
                raise Unclassifiable(
                    idx + 1,
                    "this `python3` invocation has NO ARGUMENT, so it reads its program from"
                    " STDIN — a pipe or a redirect can carry embedded code the compile check"
                    " would never see. Only a `for … in <list>` membership test is recognised as"
                    " a presence probe; anything else is a finding rather than a skip.",
                )
            dash_c = _OPEN_DASH_C.match(raw_rest)
            if dash_c:
                # The opening quote is the LAST character the match consumed.
                open_quote = m.end() + dash_c.end() - 1
                body, close = _scan_single_quoted(text, open_quote)
                end_line = text.count("\n", 0, close)
                pos = close + 1
                records.append(
                    {"kind": "BLOCK",
                     "shape": "dash-c-multiline" if "\n" in body else "dash-c-inline",
                     "line": idx + 1, "end": end_line + 1,
                     "body": body if body.endswith("\n") else body + "\n"}
                )
                continue
            hd = _HEREDOC.search(rest)
            if hd:
                body, end = _delimit_heredoc(
                    lines, idx, hd.group("tag"), hd.group("dash") == "-"
                )
                pos = starts[end] + len(lines[end])
                records.append(
                    {"kind": "BLOCK", "shape": "heredoc", "line": idx + 1, "end": end + 1,
                     "body": body}
                )
                continue
            if _SCRIPT.match(rest):
                records.append({"kind": "SCRIPT", "line": idx + 1, "text": rest})
                continue
            raise Unclassifiable(
                idx + 1,
                "this `python3` invocation is in a shape the census does not recognise"
                f" ({rest[:60]!r}). It may be carrying embedded code the compile check would"
                " therefore never see, so it is a finding rather than a skip. If the shape is"
                " legitimate, TEACH THE CENSUS THE SHAPE — this finding is about the extractor,"
                " not about the python.",
            )
        except Unclassifiable as exc:
            findings.append({"line": exc.lineno, "reason": exc.reason})
            # An occurrence that could not be delimited must not leave the cursor inside whatever
            # it failed to delimit: resume after the line it started on, so the scan terminates
            # and the rest of the file is still censused.
            pos = max(pos, line_end)
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
        print(f"{driver}:0: the driver is not a readable file, so the census has NO SUBJECT —"
              " which prints exactly like a driver with nothing wrong in it.")
        return 0
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
