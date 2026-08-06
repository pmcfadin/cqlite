#!/usr/bin/env python3
"""THE MEASUREMENT-BOUNDARY RECORD, READ BACK (issue #3272 review round 22).

Split out of `ws0_report.py` under the campsite rule: that file was at 788 lines against a ~800
target before this round added anything, so this is a split by RESPONSIBILITY rather than a waiver.
The seam is the one the rig follows everywhere — ONE question per module:

    ws0_corpus_bytes.py         are the bytes still the PINNED bytes, AT THIS BOUNDARY?  (WRITER)
    ws0_boundary_observations.py did EVERY boundary this session owed actually happen?   (READER)

# THE FINDING: the record was WRITTEN AND NEVER READ

Round 21 built `verify_corpus_boundary`; round 22 wired it into the driver's rep loop, where it
refuses a rep whose corpus changed and appends an observation to
`corpus-boundary-observations.jsonl`. Both halves were measured. **And nothing read the file.**

That leaves a live path to a published figure over a session whose corpus changed, and the sequence
is short:

1. the driver writes each rep's `.round` artifact BEFORE its boundary check, so a boundary that
   REFUSES leaves a COMPLETE, REPORTABLE artifact set behind it;
2. the refusal after the final arm kills the driver before it invokes the reporter — but it has
   removed nothing;
3. restore the corpus (the mutation was transient by construction — that is the whole attack) and
   invoke `ws0_report.py --dir … --corpus …` DIRECTLY;
4. every end-state check agrees: the pin matches the restored bytes, the sidecar matches, the
   report-time re-hash matches. The report PUBLISHES, and says `sha256_verified: true` with every
   component verified, over a session in which two reps measured different bytes.

The guard ran. Its refusal was real. Its output went NOWHERE. That is the same defect as round 22's
own subject — a guard whose verdict reaches no consumer — one layer out, and the direction of the
bias is why it is REFUSED rather than captioned: it makes a session whose corpus changed publish as
a clean one, i.e. it biases TOWARD the claim.

# WHY COMPLETENESS, AND NOT PRESENCE

CLAUDE.md's rule is that a positive verdict requires an AFFIRMATIVE MEASUREMENT — never a pass
derived from the absence of a bad signal. A reporter that publishes because no failure reached it is
deriving a pass from an absence: the failure is out-of-band by construction (it killed a different
process), so "nothing complained" is not evidence of anything. The affirmative form is to REQUIRE
the evidence, and to require it to be COMPLETE:

* **MISSING** — a boundary the session owed and no observation records. This is the finding's own
  attack: the refused boundary writes nothing (by design — a refused rep must not leave a passing
  record), so the absence IS the signal, and an absent file is its limit case.
* **DUPLICATE** — two observations for one boundary. The file is append-only across the whole
  session, so a duplicate means a boundary was recorded twice: a re-run whose reps landed in a dir
  that already carried observations, or a line copied to paper over a missing one. Either way, one
  label no longer names one verification.
* **UNEXPECTED** — an observation for a boundary this session's configuration has no place for. It
  is the substitution shape F1 removed from the whole reporter: a record from ANOTHER session (or
  another configuration of this one) sitting in this dir, "completing" the set with a verification
  that is not about these reps.

Each is a different way the record can lie, so each refuses. Refusing only the first would leave a
count that is complete relative to whatever lines happen to be in the file.

# THE EXPECTED SET IS DERIVED, NEVER ENUMERATED

`expected_boundary_labels` computes it from the SESSION's own manifest — `temps x reps x (the bare
scan + every selected flight arm)` — because that is exactly the loop the driver runs, and a
hand-maintained list drifts the moment a temperature, an arm or a rep is added. This branch has
already replaced two such enumerations with derivations (the component set, the declared-input set),
each after the hand-kept version was found short.

The configuration comes from the pre-measurement manifest and NOT from the reporter's command line,
for F1's reason: a value that cannot be supplied cannot disagree, so an incomplete record cannot be
excused by re-reporting with a narrower `--reps`/`--arms`.

# WHAT IT DELIBERATELY DOES NOT COMPARE

The observation's `corpus` field. A corpus can legitimately be MOVED between measurement and
reporting — `verify_session_corpus_pin` already treats a move as reported-not-fatal because the
BYTES decide the question — so requiring the recorded path to equal the reporter's `--corpus` would
refuse every moved-corpus session for a reason that is not about the property.

Nor does it re-verify the corpus: that is the writer's job at the boundary, and re-deriving the
verdict here would be a second implementation whose disagreement with the first would be
undiagnosable. This module asserts that the writer's verdicts EXIST, are well-formed, and cover
every boundary the session owed.

# FAIL-CLOSED, INCLUDING THE ABSENT FILE

An absent or unparseable observations file is REFUSED, never read as "assume verified". The absent
file is the exact attack above, and a malformed one is a record nobody can read back — which round
21 already ruled is an error rather than a silent omission. There is no environment variable and no
flag that relaxes any of it: an opt-out could only buy a vacuous green.
"""

from __future__ import annotations

import json
import pathlib

from ws0_corpus_bytes import (
    BOUNDARY_OBSERVATIONS,
    SESSION_CORPUS_PIN,
    boundary_observations_path,
)
from ws0_validate import Invalid, positive_int

# The BARE SCAN's token in a boundary label. The driver's rep loop treats the bare scan as a PEER of
# the flight arms (`_ARM_LIST=(scan $ARMS)`) and labels its boundary with this literal, so the
# expected set has to include it: the scan is measured in every round and its rows are the ratio's
# NUMERATOR, so the boundary after it is not optional.
SCAN_ARM_TOKEN = "scan"

# Every field an observation must carry to BE one. The set is deliberately a REQUIRED SUBSET rather
# than an exact key census: the writer legitimately adds coverage fields (round 24 added the
# declared-input names), and freezing the whole key set here would red on a peer module's
# improvement rather than on a defect. What is required is what makes a line a RECORD OF A
# VERIFICATION — which boundary, how much was checked, and against what. A line missing any of them
# is refused rather than counted.
REQUIRED_OBSERVATION_FIELDS = (
    "boundary",
    "corpus",
    "components_verified",
    "components_pinned",
    "verified_against",
)


def boundary_label(temp: str, rep: int, arm: str) -> str:
    """The label the driver stamps for the boundary AFTER `arm`'s rep of `temp`.

    ONE spelling of the format, so the expected set and the record cannot be derived from two
    different templates. The driver's counterpart is the shell expansion
    `"$temp-$rep-after-$arm"` at its `verify_corpus_boundary_or_refuse` call site;
    `test_ws0_report_guards.sh` asserts the two agree STRUCTURALLY, because a format that drifted
    would present as every boundary MISSING — a refusal blaming the operator for a rig defect.
    """
    return f"{temp}-{rep}-after-{arm}"


def expected_boundary_labels(temps: list[str], arms: list[str], reps: int) -> list[str]:
    """Every boundary THIS session owed, DERIVED from its own configuration.

    The driver ends each ARM's rep with a boundary — not each ROUND — because the rig's output is a
    bare/flight RATIO whose numerator and denominator are measured by different arms WITHIN one
    round, so a component replaced between them lands directly on the ratio. The expected set is
    therefore the full product: every temperature, every rep, and the bare scan plus every selected
    flight arm.
    """
    return [
        boundary_label(temp, rep, arm)
        for temp in temps
        for rep in range(1, reps + 1)
        for arm in [SCAN_ARM_TOKEN, *arms]
    ]


def _read_observations(path: pathlib.Path) -> list[dict]:
    """Every well-formed observation in `path`, or `Invalid`. Fail-closed on absence."""
    if not path.exists():
        raise Invalid(
            f"this session dir carries no {BOUNDARY_OBSERVATIONS} ({path}), so NOTHING RECORDS that"
            " the corpus was verified UNCHANGED at any measurement boundary. The driver appends one"
            " observation per boundary; an absent file means either that no boundary check ran, or"
            " that one REFUSED (a refused boundary deliberately records nothing) and the reporter"
            " was invoked directly afterwards over the restored corpus. Both publish a figure over"
            " a session whose corpus may have changed mid-run, which every END-STATE check agrees"
            " with by construction — so this is REQUIRED rather than assumed verified (#3272 round"
            " 22). Re-run the session with scripts/perf/ws0-baseline.sh."
        )
    try:
        raw = path.read_text()
    except OSError as exc:
        raise Invalid(
            f"{path} could not be read: {exc}. The boundary record is the only evidence that the"
            " corpus was unchanged mid-run; a record nobody can read back is not evidence, so this"
            " refuses rather than proceeding."
        ) from None
    out: list[dict] = []
    for n, line in enumerate(raw.splitlines(), 1):
        if not line.strip():
            continue
        try:
            obs = json.loads(line)
        except ValueError as exc:
            raise Invalid(
                f"{path} line {n} is not readable JSON: {exc}. Each line is one boundary"
                " observation; an unparseable line cannot be attributed to a boundary, and"
                " skipping it would let a malformed record stand in for a missing verification."
            ) from None
        if not isinstance(obs, dict):
            raise Invalid(
                f"{path} line {n} holds a {type(obs).__name__}, not a JSON object, so it records no"
                " boundary. A line that is not an observation is refused rather than ignored."
            )
        absent = [f for f in REQUIRED_OBSERVATION_FIELDS if f not in obs]
        if absent:
            raise Invalid(
                f"{path} line {n} is not a boundary observation — no {', '.join(absent)}. Every"
                " required field is one that makes the line a RECORD OF A VERIFICATION (which"
                " boundary, how much was checked, against what); a line missing any of them cannot"
                " establish that a boundary was verified, so it is refused rather than counted"
                " toward completeness."
            )
        label = obs["boundary"]
        if not isinstance(label, str) or not label.strip():
            raise Invalid(
                f"{path} line {n} records `boundary` {label!r}, which does not name a boundary. An"
                " observation that cannot be attributed to a rep cannot complete the set."
            )
        verified = positive_int(
            f"{path} line {n} `components_verified`",
            obs["components_verified"],
            "it is the COUNT of components this boundary re-hashed; a boundary that verified"
            " nothing is not a verified boundary.",
        )
        pinned = positive_int(
            f"{path} line {n} `components_pinned`",
            obs["components_pinned"],
            "it is the size of the pinned set the count above is complete RELATIVE TO.",
        )
        if verified != pinned:
            raise Invalid(
                f"{path} line {n} (boundary {label!r}) records {verified} component(s) verified out"
                f" of {pinned} pinned — a PARTIAL boundary. A check that covered some of the pinned"
                " components and reported success is issuing a verdict about the ones it never"
                " looked at, and the omission biases TOWARD the claim. Refused."
            )
        against = obs["verified_against"]
        if against != SESSION_CORPUS_PIN:
            raise Invalid(
                f"{path} line {n} (boundary {label!r}) records `verified_against` {against!r},"
                f" not {SESSION_CORPUS_PIN}. The boundary bytes must be compared against the"
                " PRE-MEASUREMENT pin: the corpus's own corpus-identity.json can be refreshed"
                " beside a replaced component and is therefore self-consistent at every boundary,"
                " so an observation made against it establishes nothing."
            )
        out.append(obs)
    return out


def verify_boundary_observations(
    session_dir: pathlib.Path, temps: list[str], arms: list[str], reps: int
) -> dict:
    """EXACTLY ONE valid observation per boundary this session owed, or `Invalid`.

    Refuses MISSING, DUPLICATE and UNEXPECTED observations — see the module docstring for why all
    three, and for the sequence that publishes a figure without this check.
    """
    path = boundary_observations_path(session_dir)
    observations = _read_observations(path)
    expected = expected_boundary_labels(temps, arms, reps)
    expected_set = set(expected)
    seen: dict[str, int] = {}
    for obs in observations:
        seen[obs["boundary"]] = seen.get(obs["boundary"], 0) + 1
    derivation = (
        f"temperatures [{' '.join(temps)}] x {reps} rep(s) x arms"
        f" [{' '.join([SCAN_ARM_TOKEN, *arms])}] (the bare scan is a PEER of the flight arms in the"
        " driver's rep loop), read from the pre-measurement session manifest"
    )
    missing = [label for label in expected if label not in seen]
    if missing:
        raise Invalid(
            f"THE MEASUREMENT-BOUNDARY RECORD IS INCOMPLETE: {len(missing)} of {len(expected)}"
            f" boundaries have NO observation in {path.name} — {', '.join(missing)}. The expected"
            f" set is DERIVED from this session's own configuration ({derivation}), so this is not"
            " a stale expectation. A boundary with no observation was either never checked or"
            " REFUSED (a refused boundary records nothing, deliberately, so a refused rep cannot"
            " leave a passing record). Both mean reps of this session may have measured different"
            " bytes — which every END-STATE check agrees with once the corpus is restored, because"
            " the pin, the sidecar and the report-time re-hash all see the restored bytes. This"
            " session cannot be reported (#3272 round 22)."
        )
    duplicated = sorted(label for label, n in seen.items() if n > 1)
    if duplicated:
        raise Invalid(
            f"THE MEASUREMENT-BOUNDARY RECORD IS AMBIGUOUS: {path.name} carries more than one"
            f" observation for {', '.join(f'{d} ({seen[d]}x)' for d in duplicated)}. The file is"
            " appended to once per boundary across the whole session, so a duplicate means a"
            " boundary was recorded twice — reps landing in a dir that already held observations,"
            " or a line copied to cover a missing one. One label must name one verification, or the"
            " count is complete only relative to whatever lines happen to be in the file. Refused."
        )
    unexpected = sorted(set(seen) - expected_set)
    if unexpected:
        raise Invalid(
            f"THE MEASUREMENT-BOUNDARY RECORD CONTAINS OBSERVATIONS THIS SESSION HAS NO PLACE FOR:"
            f" {', '.join(unexpected)} in {path.name}, against an expected set DERIVED from the"
            f" session manifest ({derivation}). An observation for a boundary this configuration"
            " never ran is a record from another session — or another configuration of this one —"
            " sitting in this dir and completing the set with a verification that is not about"
            " these reps. Refused rather than ignored: ignoring it is what makes a substituted"
            " record harmless to a count (#3272 round 22, F1's substitution shape)."
        )
    return {
        "source": str(path),
        "boundaries_expected": len(expected),
        "boundaries_verified": len(observations),
        # The LABELS, not just the counts (#3272 round 24's rule, applied here): a bare count is
        # complete relative to whatever list produced it, so the covered set is named.
        "expected_boundaries": expected,
        "observed_boundaries": sorted(seen),
        "expected_set_derivation": derivation,
        "min_components_verified": min(o["components_verified"] for o in observations),
        "verified_against": SESSION_CORPUS_PIN,
        "note": (
            f"every one of the {len(expected)} boundaries this session's configuration owed carries"
            f" EXACTLY ONE well-formed observation in {BOUNDARY_OBSERVATIONS}, re-hashed FROM DISK"
            f" against {SESSION_CORPUS_PIN} at the boundary. MISSING, DUPLICATE and UNEXPECTED"
            " observations each REFUSE the report: a refused boundary records nothing, so an absent"
            " observation is the signature of a mutation that was restored before reporting — the"
            " one state every END-STATE check agrees with (#3272 round 22)"
        ),
    }


def boundary_observation_lines(record: dict) -> list[str]:
    """The human-summary statement of the boundary record, AFFIRMATIVE and specific.

    One line, printed unconditionally: this is the only place a reader of the summary can tell "the
    corpus was re-verified between every rep" from "this rig checks the ends only". The absence of a
    line cannot express either, which is how a written-and-never-read record went unnoticed.
    """
    return [
        f"boundaries   : {record['boundaries_verified']}/{record['boundaries_expected']}"
        " measurement boundaries VERIFIED mid-run — one recorded observation each, re-hashed FROM"
        f" DISK against {record['verified_against']} (>= {record['min_components_verified']}"
        " component(s) per boundary), read back here from"
        f" {pathlib.Path(record['source']).name}",
        f"               expected set DERIVED from {record['expected_set_derivation']}; a MISSING,"
        " DUPLICATE or UNEXPECTED observation REFUSES this report",
    ]


def boundary_observation_note_lines() -> list[str]:
    """The NOTES bullet: what the boundary record covers, and what its absence would mean."""
    return [
        "  * the corpus was re-hashed FROM DISK at EVERY measurement boundary — after each arm's "
        "rep, not merely at the ends — against session-corpus-pin.json, and THIS REPORT READ THAT "
        "RECORD BACK: exactly one observation per (temperature, rep, arm) the session manifest "
        "declares, with a missing, duplicate or unexpected one REFUSING the report "
        "(results.json .boundary_observation_completeness).",
        "    The check exists because a component MUTATED mid-run and RESTORED before reporting is "
        "invisible to every end-state check — the pin, the corpus sidecar and the report-time "
        "re-hash all see the restored bytes and all agree. A refused boundary deliberately records "
        "nothing, so the ABSENCE of an observation is the signal, and a complete artifact set with "
        "an incomplete boundary record is exactly what that attack leaves behind (#3272 round 22).",
    ]
