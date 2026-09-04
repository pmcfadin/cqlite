#!/usr/bin/env python3
"""THE FLIGHT SERVER'S ADMISSION CEILING, READ BACK FROM ITS OWN LOG (issue #3551 item 10).

# The finding

`cqlite-flight` derives its admission ceiling when `--max-concurrent-scans` is not pinned:
`cli::resolve_max_concurrent_scans` defaults to `clamp(2 x available_parallelism, 2, 64)`, and
`available_parallelism` respects the process's **CPU AFFINITY MASK**. So the ceiling is a
FUNCTION OF THE PIN, and `--flight-server-cpus` moves the pin.

A 2-CPU pin and a 4-CPU pin therefore differ in **two** properties, not one: where the work runs
AND how much of it the server will admit at once. A comparison across such arms is not the
one-property comparison the rig's whole drift-control argument rests on.

# Why this is a READ-BACK and not a pin

The obvious fix — pass `--max-concurrent-scans` to force agreement — is REFUSED by design: it
would change the configuration #3248 measured, and it would HIDE exactly the drift this exists to
catch. The server already logs the answer, so the answer is read:

    cqlite-flight starting ... max_concurrent_scans=4 max_concurrent_scans_source="derived"
    available_parallelism=2 ...

The chosen arms keep the logical-CPU COUNT equal (`2,10` -> `2,3`, two CPUs either way), so the
derived ceiling *should* be identical — but "should be" is not a measurement. All three fields are
recorded per rep, and a session whose reps DISAGREE is REFUSED: that disagreement means the arms
differed in a second property and every cross-arm number in the report is about two changes.

# THE LOG IS COLOUR-ESCAPED, and that is a standing rule here (#3400)

The real logs carry ANSI SGR sequences around every field NAME, so a pattern anchored on
`<name>=<value>` across an escape boundary matches NOTHING — and this repository has a standing
rule about exactly that class: route every log parse through an escape strip first, and read by
REDIRECTION rather than a pipe (a piped `while read` runs in a subshell and its verdict is
discarded). This module strips CSI sequences before matching, and reads the file whole rather
than streaming it, so neither half of that rule can be lost here.

# EVERY STATE IS THREE-VALUED

An absent log, an unreadable one, an empty one and one whose fields cannot be parsed are each
COULD-NOT-MEASURE, and each is a REFUSAL naming the rep — never "the ceilings agree". A positive
verdict requires an affirmative measurement, and "the field is absent" and "the field could not
be looked for" are different facts, only one of which could ever support a verdict.
"""

from __future__ import annotations

import pathlib
import re

from ws0_validate import Invalid

# The three fields the server logs at startup. DECLARED here, read below, and asserted in both
# directions by `scripts/tests/test_ws0_flight_arm_guards.sh`: a field declared without a reader,
# or read without being declared, is a test failure rather than a silent gap (the pattern
# `PINNING_RECORD_FIELDS` established).
#
# All three, not just the ceiling: `max_concurrent_scans` alone cannot distinguish "the derivation
# gave the same answer" from "someone pinned it", and `available_parallelism` is the INPUT whose
# dependence on the affinity mask is the whole reason this check exists.
FLIGHT_ADMISSION_FIELDS = (
    "max_concurrent_scans",
    "max_concurrent_scans_source",
    "available_parallelism",
)

# ANSI CSI sequences, stripped before any match (#3400). Deliberately the whole CSI family and not
# just SGR `m`: the log's colouring is a PRESENTATION property, so a parse keyed on any part of it
# is keyed on the wrong thing, and enumerating the final bytes we happen to have seen is the
# deny-list shape this rig refuses.
_CSI = re.compile(r"\x1b\[[0-9;:?]*[ -/]*[@-~]")


def _strip_ansi(text: str) -> str:
    return _CSI.sub("", text)


def server_log_path(session_dir: pathlib.Path, tag: str) -> pathlib.Path:
    """Where a flight rep's server log lives. Named ONCE, so the writer (lib-measure.sh) and this
    reader cannot disagree about the filename — the failure mode would be an absent-artifact
    refusal that looks like a server that logged nothing."""
    return session_dir / f"{tag}.server.log"


def read_admission_record(session_dir: pathlib.Path, tag: str) -> dict[str, str]:
    """The three admission fields for ONE flight rep, or `Invalid` naming the cause.

    Values are returned as STRINGS, verbatim after quote removal: this module's job is to compare
    reps against each other, and a numeric coercion here would be a second opinion about what the
    server said (`4` and `04` are the same ceiling but not the same log line).
    """
    p = server_log_path(session_dir, tag)
    if not p.exists():
        raise Invalid(
            f"flight rep {tag} carries no server log ({p.name}), so the admission ceiling its"
            " server ran under COULD NOT BE MEASURED. That ceiling is DERIVED from"
            " available_parallelism, which respects the CPU AFFINITY MASK, so it moves with"
            " --flight-server-cpus: without it, two arms may differ in the pin AND in how much"
            " work the server admits at once, and every cross-arm figure would be about two"
            " changes (#3551). This is a refusal rather than an assumption — an absent record is"
            " not evidence that the ceilings agreed."
        )
    try:
        text = p.read_text(errors="replace")
    except OSError as exc:
        raise Invalid(f"flight rep {tag}: {p.name} could not be read: {exc}") from None
    if not text.strip():
        raise Invalid(
            f"flight rep {tag}: {p.name} is EMPTY, so the admission ceiling COULD NOT BE"
            " MEASURED. A server that started always logs its startup line, so an empty log is a"
            " failed measurement rather than evidence about the ceiling (#3551)."
        )
    plain = _strip_ansi(text)
    out: dict[str, str] = {}
    absent: list[str] = []
    for field in FLIGHT_ADMISSION_FIELDS:
        # `<name>=<value>`, with the value either bare or double-quoted. Matched on the STRIPPED
        # text, because in the real log an escape sits between the name and the `=` and a pattern
        # spanning that boundary matches nothing at all (#3400).
        m = re.search(rf"(?<![\w.]){re.escape(field)}=(\"[^\"]*\"|\S+)", plain)
        if not m:
            absent.append(field)
            continue
        value = m.group(1)
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        out[field] = value
    if absent:
        raise Invalid(
            f"flight rep {tag}: {p.name} does not record {', '.join(absent)}. The startup line"
            " this rig reads is `cqlite-flight starting ... max_concurrent_scans=N"
            ' max_concurrent_scans_source="derived" available_parallelism=N`, and the parse runs'
            " on the ANSI-STRIPPED text, so colour is not the cause (#3400/#3551). An"
            " unparseable log is COULD-NOT-MEASURE and is refused rather than read as agreement."
        )
    return out


def verify_flight_admission(
    session_dir: pathlib.Path, temps: list[str], arms: list[str], reps: int, tag_of
) -> dict:
    """REQUIRE every selected flight rep's admission record, and require them to AGREE.

    `tag_of` is the SHIPPED tag builder (`ws0_flight_arm.flight_rep_tag`), passed in rather than
    re-spelled here: a second copy of the tag convention would drift, and the failure mode is an
    absent-log refusal for a rep whose log exists under the name the other module writes.

    The expected set is DERIVED from the SELECTION the manifest recorded, so an unselected
    temperature or arm is legitimately absent while a selected one that is missing is fatal — the
    posture `verify_boundary_observations` takes for the same reason.
    """
    per_rep: dict[str, dict[str, str]] = {}
    for temp in temps:
        for arm in arms:
            for rep in range(1, reps + 1):
                tag = tag_of(arm, temp, rep)
                per_rep[tag] = read_admission_record(session_dir, tag)
    if not per_rep:
        raise Invalid(
            "this session selected no flight rep, so there is no admission record to verify."
            " Refused rather than passed: a check that iterates over nothing and returns success"
            " is the vacuous pass this rig exists to refuse (#3551)."
        )
    # THE AGREEMENT, field by field, so the diagnostic names WHICH field moved and with it the
    # likely cause (a changed ceiling with an unchanged available_parallelism is a pin; a changed
    # available_parallelism is the affinity mask).
    disagreements: list[str] = []
    for field in FLIGHT_ADMISSION_FIELDS:
        values = {tag: rec[field] for tag, rec in per_rep.items()}
        distinct = sorted(set(values.values()))
        if len(distinct) > 1:
            detail = ", ".join(f"{tag}={values[tag]!r}" for tag in sorted(values))
            disagreements.append(f"{field}: {detail}")
    if disagreements:
        raise Invalid(
            "the Flight reps of this session did NOT all run under the same admission"
            f" configuration: {'; '.join(disagreements)}."
            " That ceiling is DERIVED from available_parallelism, which respects the CPU AFFINITY"
            " MASK, so it moves with the pin: reps that disagree differed in a SECOND property"
            " besides the one under test, and every cross-arm figure in this report would be"
            " about two changes at once (#3551). It is refused rather than captioned, and the"
            " remedy is NOT to pin --max-concurrent-scans: pinning would change the"
            " configuration #3248 measured and would hide exactly this drift. Re-run the session"
            " with arms whose logical-CPU COUNT is equal."
        )
    agreed = {field: next(iter(per_rep.values()))[field] for field in FLIGHT_ADMISSION_FIELDS}
    return {
        **agreed,
        "per_rep": per_rep,
        "reps_agreeing": len(per_rep),
        "source": "read back from each flight rep's own <tag>.server.log, ANSI-stripped (#3400)",
        "note": (
            "the admission ceiling is DERIVED by the server from available_parallelism, which"
            " respects the CPU affinity mask, so it is a function of --flight-server-cpus. All"
            f" {len(per_rep)} flight rep(s) of this session were OBSERVED to agree on all three"
            " fields; --max-concurrent-scans is deliberately NOT pinned, because pinning would"
            " change the measured configuration and hide the drift this check exists to catch"
            " (#3551)."
        ),
    }
