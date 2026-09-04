#!/usr/bin/env python3
"""THE CPU-PIN VERIFICATION AS A RECORDED OBSERVATION (#3272 review round 9, F6).

Split out of `ws0_session.py` under the campsite rule (that file is at 648 lines and this is a
distinct responsibility, not more of the same one). The seam follows the rig's existing pattern:

    ws0_session.py        the CORPUS's identity  — which DATA was measured?
    ws0_schema_input.py   the SCHEMA's identity  — which SCHEMA was it read WITH?
    ws0_pinning.py        the PINNING's identity — were those CPUs VERIFIED, and by whom?

# The finding

`ws0_report.py` printed, unconditionally:

    "verified": "thread_siblings_list, fail-closed (scripts/perf/lib-cpu.sh)"
    pinning      : server 2,10 (verified physical-core siblings), client 3,11

...about CPU lists it read from the session manifest and never validated. The manifest's own
reader (`session_manifest_config`) deliberately declines to re-check them — its stated reason
being that "re-parsing it here would be a second implementation of a check that has already
fail-closed" — but that check ran against **the driver's argv**, and nothing tied the argv to the
string the manifest recorded. MEASURED by the reviewer: setting `config.server_cpus = "99,99"`
in a fixture session's `session-corpus-pin.json` made the report exit 0 printing
`pinning : server 99,99 (verified physical-core siblings)` — byte-identical to the line the
pre-F1 argv-substitution defect produced. F1 closed the argv route; the identical false claim
survived one layer in, via the artifact.

The compounding detail that made it a blocker rather than a wording nit: the same `results.json`
takes the OPPOSITE posture two fields away (`recorded_round_metadata.source` says "provenance
UNVERIFIED — the reporter reads a directory and cannot establish which program wrote it"). One
document, two contradictory epistemics about its own artifacts.

# Why the fix is a RECORDED OBSERVATION rather than a report-time re-check

Three options were on the table. The choice is not stylistic:

* **re-verify at report time** — validate the manifest's lists against `thread_siblings_list`
  when the report is written. Rejected as the primary mechanism because it is **not the same
  claim**: a report may legitimately be produced on a DIFFERENT HOST from the measurement (a
  results dir copied off the box, which is how these artifacts are reviewed), and there the
  local `thread_siblings_list` describes a machine that never ran the measurement. A check that
  passes or fails depending on where the report is generated is not evidence about the session.
* **weaken the text** — print "RECORDED; not re-checked". Honest, and cheap, but it discards a
  verification that genuinely happened: the driver DID read the real sysfs topology and DID fail
  closed. Throwing that away leaves the rig weaker than it actually is.
* **record it where it was made, assert it where it is used** (this module). The driver verified
  the real argv against the real `/sys/devices/system/cpu/*/topology/thread_siblings_list`; it
  now writes down WHAT it verified, WHICH host it read, and WHAT the sysfs said. The reporter
  requires that record, requires it to AGREE with the manifest's lists, and prints "verified"
  only on the strength of it — naming the recorded observation, not `lib-cpu.sh`.

That is this issue's own doctrine applied to itself: an observation is recorded where it is
made and asserted where it is used, and no claim is printed that nothing backs.

# What this does NOT claim

The record is written by the driver, so it establishes what the DRIVER observed — not an
independent truth about the host. That limit is stated in the record itself
(`provenance`) and is the same limit `recorded_round_metadata` states about itself, which is the
point: the two postures in `results.json` now agree rather than contradicting each other.

What it DOES close is the substitution: a manifest whose `server_cpus` was edited to a value the
driver never verified no longer prints as verified, because the recorded verification names the
list it actually checked and the two are compared.

# THE FLIGHT ARM'S SIX FIELDS ARE REQUIRED, NOT OPTIONAL (#3551) — and why that is safe

`--flight-server-cpus` / `--flight-pin-mode` / `--flight-allocator` add six fields, and this
reader REQUIRES every declared field, so a session dir written by an older driver is REFUSED
rather than half-read. That was a deliberate choice between two options, decided by measurement
rather than by taste:

* **(a) required** — chosen. `git ls-files` holds NO `session-corpus-pin.json` and NO
  `pinning-verification.json`: the ws0 artifacts committed under `docs/reports/ws0-*-artifacts/`
  are report OUTPUTS (`results.json`, `summary.txt`), which this reporter never reads back. So
  nothing in the repository is refused by the new requirement, the only affected session dirs are
  scratch dirs under `target/`, and the existing INCOMPLETE diagnostic already names the remedy
  ("Re-run the session with the current driver") in the idiom `events`/`bin_dir` established when
  #3248 added them.
* **(b) optional with an affirmative absent-marker** — rejected. It would introduce a SECOND
  vocabulary for every one of these claims ("this session predates the flight pin" vs a real
  value), which the report would then have to render, and each of those renderings is a sentence
  about a measurement nobody made. The cost of (a) is re-running a scratch session; the cost of
  (b) is permanent, in the artifact the report's claims rest on.
"""

from __future__ import annotations

import json
import pathlib

from ws0_validate import Invalid

# The artifact the driver writes and the reporter requires. Named once, here, so the writer and
# the reader cannot disagree about the filename (the failure mode would be an absent-artifact
# refusal that looks like a driver that never verified anything).
PINNING_VERIFICATION = "pinning-verification.json"

# The fields the record is DEFINED by. Every one is read by `verify_pinning_record` below —
# asserted in both directions by `scripts/tests/test_ws0_provenance_guards.sh`, so a field added
# here without a reader, or read without being declared, is a test failure rather than a silent
# gap. (This is the oracle `MANIFEST_CONFIG_FIELDS` lacked — #3272 F7.)
PINNING_RECORD_FIELDS = (
    "server_cpus",
    "client_cpus",
    "server_siblings_expanded",
    "topology_root",
    "host",
    "verified_by",
    "provenance",
    # THE FLIGHT ARM (#3551). Six fields, all REQUIRED — see the backwards-compatibility note in
    # the module docstring for why required rather than optional.
    "flight_server_cpus",
    "flight_pin_mode",
    "flight_pin_verified",
    "flight_allocator",
    "flight_allocator_lib",
    "flight_malloc_arena_max",
    "flight_allocator_verification",
)

# THE PIN MODES, AND THE WORDS EACH ONE LICENSES (#3551).
#
# A CLOSED SET, for the reason `baseline_mode` is one: an unrecognised value would otherwise
# reach the report as a claim about which property was verified, and that is the entire content
# of the flag. The mapping to WORDING lives here rather than in `ws0_report.py` because there
# must be exactly ONE place that decides what may be said about a mode: the report may NEVER
# print `physical-core siblings` about a `distinct-cores` pin, and a second copy of that mapping
# is a second place for it to be spelled wrong.
FLIGHT_PIN_CLAIM = {
    "siblings": "physical-core siblings",
    "distinct-cores": "pairwise DISTINCT physical cores",
}

# ...and the allocator arms, closed for the same reason.
FLIGHT_ALLOCATORS = ("system", "jemalloc")


def pinning_record_path(session_dir: pathlib.Path) -> pathlib.Path:
    """Where the pinning verification lives inside a session dir."""
    return session_dir / PINNING_VERIFICATION


def verify_pinning_record(
    session_dir: pathlib.Path,
    server_cpus: str,
    client_cpus: str,
    flight_server_cpus: str,
) -> dict:
    """REQUIRE the driver's recorded sibling verification, and require it to be ABOUT these pins.

    `server_cpus`/`client_cpus` come from the session manifest — the values the report is about
    to print. This is what ties them to a verification that actually happened: the driver wrote
    down which lists it checked against real sysfs, and a manifest naming anything else is
    refused rather than printed as verified.

    REQUIRED, never optional. An absent record means this session dir does not record that its
    pinning was ever verified, and the report would otherwise print "verified" on the strength
    of nothing — which is the finding. The remedy is a re-run with the current driver, and the
    diagnostic says so.

    `flight_server_cpus` is the same fact for the FLIGHT arm's own pin (#3551) and is tied to the
    driver's record by the same comparison, at the new field.
    """
    p = pinning_record_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {PINNING_VERIFICATION} ({p}), so it does not record"
            " that its CPU pinning was ever VERIFIED against the host topology. The report"
            " prints `server <list> (verified physical-core siblings)`, and that claim must rest"
            " on a recorded observation rather than on trust: the reporter cannot re-derive it"
            " (a results dir is routinely reviewed on a different host, whose"
            " thread_siblings_list describes a machine that never ran the measurement). Re-run"
            " the session with scripts/perf/ws0-baseline.sh, which records the verification it"
            " performs (#3272 F6)."
        )
    try:
        rec = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(rec, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(rec).__name__}")
    absent = [f for f in PINNING_RECORD_FIELDS if f not in rec]
    if absent:
        raise Invalid(
            f"{p} is INCOMPLETE — no {', '.join(absent)}. Every field of the record is one the"
            " report's pinning claim rests on, so a partial record cannot establish that the"
            " pinning was verified. Re-run the session with the current driver."
        )
    for key in PINNING_RECORD_FIELDS:
        value = rec[key]
        if not isinstance(value, str) or not value.strip():
            raise Invalid(
                f"{p} `{key}` is {value!r}, which is not a recorded value. The report cites this"
                " record as the evidence for its pinning claim, so an empty field would make the"
                " claim rest on nothing."
            )
    # THE CLOSED GRAMMARS (#3551). Checked BEFORE the substitution comparison below, because a
    # mode or an allocator nobody planned for cannot support any claim at all — and the report's
    # wording is DERIVED from these two values, so an unrecognised one would either crash the
    # reporter or, worse, fall back to a default and describe a property nothing verified.
    mode = rec["flight_pin_mode"]
    if mode not in FLIGHT_PIN_CLAIM:
        raise Invalid(
            f"{p} `flight_pin_mode` is {mode!r}, which is not one of"
            f" {', '.join(repr(m) for m in FLIGHT_PIN_CLAIM)}. The report states WHICH property"
            " the flight pin was verified to have — one physical core's siblings, or pairwise"
            " distinct physical cores — and a value nobody planned for cannot support either"
            " sentence (#3551). It is refused rather than reported verbatim, and never defaulted:"
            " a default would print the wrong one of two mutually exclusive claims."
        )
    allocator = rec["flight_allocator"]
    if allocator not in FLIGHT_ALLOCATORS:
        raise Invalid(
            f"{p} `flight_allocator` is {allocator!r}, which is not one of"
            f" {', '.join(repr(a) for a in FLIGHT_ALLOCATORS)}. The report prints the allocator"
            " the measured server ran under, and an unrecognised value is a claim about an arm"
            " this rig does not have (#3551)."
        )
    # THE SUBSTITUTION CHECK, which is the whole finding. The manifest's lists must be the lists
    # the driver actually verified. A hand-edited `config.server_cpus` (the reviewer's `99,99`)
    # names CPUs no verification was ever performed against, and it is refused here rather than
    # printed under the word "verified".
    #
    # Compared on the RECORDED SPELLING, deliberately: the driver records the argv string it
    # passed to `verify_sibling_pair` and the manifest records the same variable, so equality is
    # the property. Normalising (expanding ranges, sorting) would be a second implementation of
    # `cpu_list_expand` here — the very thing `session_manifest_config` correctly declined to do
    # — and its disagreement with the shell version would be undiagnosable.
    for label, manifest_value, recorded_key in (
        ("server", server_cpus, "server_cpus"),
        ("client", client_cpus, "client_cpus"),
        # THE SAME CHECK AT THE NEW PIN (#3551). Without it, `config.flight_server_cpus` would be
        # exactly what `config.server_cpus` was before F6: an opaque manifest string that reaches
        # a "verified" sentence in the report having been validated by nothing. Compared on the
        # RECORDED SPELLING for the reason stated below — a normalising comparison here would be
        # a second implementation of `cpu_list_expand`.
        ("flight server", flight_server_cpus, "flight_server_cpus"),
    ):
        recorded = rec[recorded_key]
        if manifest_value != recorded:
            raise Invalid(
                f"the session manifest records {label} CPUs {manifest_value!r} but the"
                f" verification in {p} was performed against {recorded!r}. The report would have"
                f" printed {manifest_value!r} as VERIFIED physical-core siblings while the only"
                " verification that ever ran was about different CPUs — which is exactly the"
                " substitution this record exists to catch: MEASURED, a manifest hand-edited to"
                " `99,99` printed `server 99,99 (verified physical-core siblings)` and exited 0"
                " (#3272 F6). Either the manifest or the record was edited after the session."
            )
    return {
        "server_cpus": rec["server_cpus"],
        "client_cpus": rec["client_cpus"],
        "server_siblings_expanded": rec["server_siblings_expanded"],
        "topology_root": rec["topology_root"],
        # THE FLIGHT ARM, read through (#3551). Every one of these is read here — a field
        # declared in PINNING_RECORD_FIELDS and never read is a test failure
        # (`scripts/tests/test_ws0_provenance_guards.sh` asserts both directions), and it would
        # also be a property of the measurement the report cannot see.
        "flight_server_cpus": rec["flight_server_cpus"],
        "flight_pin_mode": mode,
        "flight_pin_verified": rec["flight_pin_verified"],
        # WHAT MAY BE SAID about this pin, derived from the closed mode set above so the sentence
        # and the verification cannot disagree.
        "flight_pin_claim": FLIGHT_PIN_CLAIM[mode],
        "flight_allocator": allocator,
        "flight_allocator_lib": rec["flight_allocator_lib"],
        # THE ARENA CAP (#3551, #3217 partC F1's AC2). Recorded as an affirmative sentence in
        # both directions — a cap, or "not injected … which is deliberately NOT the same as
        # setting it to 0" — because "no cap" and "nobody wrote the field down" must not look
        # the same in the record the report's claim rests on. An arena cap leaves NO mapping, so
        # /proc/<pid>/environ is the ONLY place it is observable and this field is the only place
        # a reader of the report can see which arm they are looking at.
        "flight_malloc_arena_max": rec["flight_malloc_arena_max"],
        "flight_allocator_verification": rec["flight_allocator_verification"],
        "host": rec["host"],
        "verified_by": rec["verified_by"],
        # Carried FORWARD into results.json verbatim, so a reader of the report alone sees the
        # same epistemic limit the record states — the posture `recorded_round_metadata` already
        # takes about itself. One document, one story about its own artifacts (#3272 F6).
        "provenance": rec["provenance"],
        "source": str(p),
        # What the reporter ESTABLISHED, as opposed to what it read. Stated as the specific
        # comparison performed rather than as the word "verified", so a reader can tell which
        # claim is which.
        "note": (
            "the CPU lists this report prints are the lists the DRIVER verified against the"
            f" host's real thread_siblings_list before measuring (recorded in {p.name}); the"
            " reporter asserted the manifest's lists EQUAL the verified ones. The sibling"
            " verification itself was performed on the measuring host and is NOT re-derived"
            " here — a results dir reviewed on another host would read that host's topology,"
            " which never ran the measurement (#3272 F6)."
        ),
    }
