#!/usr/bin/env python3
"""THE SESSION MANIFEST and the corpus IDENTITY VERIFICATION (#3272 review round 5).

Split out of `ws0_validate.py` under the campsite rule: that file was at 855 lines against a
~800 target before F1/F3 added anything, so this is a split by RESPONSIBILITY rather than a
waiver. The seam:

    ws0_validate.py   the QUANTITY domain — was this number validly observed?
    ws0_session.py    the SESSION's identity — is this report ABOUT what it says it is?

Two round-5 findings live here, and they are the same defect at two scopes:

* **F1 — the reporting CONFIGURATION came from the CURRENT CLI.** `ws0_report.py` took
  `--reps`, `--temps`, `--arms`, `--server-cpus`, `--client-cpus`, `--step-duration` and
  `--scan-passes` from whoever invoked it, so re-reporting a session dir with FEWER reps, a
  narrower arm set, different CPU pins or a different `--scan-passes` produced a confident
  report that ignored measured artifacts and printed the REPLACEMENT configuration as though
  it had been verified. The session's configuration is now recorded BEFORE measurement and the
  reporter READS IT FROM THE MANIFEST — see `session_manifest_config`, and the note there on
  why reading beats matching.

* **F3 — corpus verification checked ONLY `Data.db`.** The generator records every emitted
  component with its size and sha256 (`CorpusIdentity.components`), and the scans consume the
  auxiliary components too — `Index.db` above all — so a modified `Index.db` could change
  measured behaviour while the report claimed the corpus identity had been verified.
  `verify_corpus_bytes` now verifies the COMPLETE recorded component set.

The mechanism for both is ONE pre-measurement record: the driver stamps
`session-corpus-pin.json` before rep 1 and the reporter refuses a session whose manifest it
cannot match. Round 4 introduced that file for the corpus digest alone; F1/F3 EXTEND the same
record rather than adding a second parallel one, so there is exactly one place a session's
identity is established.
"""

from __future__ import annotations

import json
import re
import pathlib

from ws0_canonical_corpus import MODE_BASELINE, MODE_NON_BASELINE

# THE CORPUS'S BYTES — split into `ws0_corpus_bytes.py` in round 21, when the pin stopped COPYING
# `corpus-identity.json`'s digests and started HASHING the files. Re-exported here because the
# sibling modules and the reporter import these names from this module; the seam is documented in
# `ws0_corpus_bytes.py`'s docstring. `CORPUS_TABLE_SUBPATH`/`SESSION_CORPUS_PIN`/`sha256_file` are
# re-exported for the same reason — one spelling, so no consumer has to know which module a name
# moved to.
from ws0_corpus_bytes import (  # noqa: F401  (re-exported for this module's consumers)
    BOUNDARY_OBSERVATIONS,
    CORPUS_TABLE_SUBPATH,
    SESSION_CORPUS_PIN,
    boundary_observations_path,
    locate_corpus_data_db,
    measure_component_digests,
    session_pin_path,
    sha256_file,
    verify_corpus_boundary,
    verify_corpus_bytes,
    verify_corpus_components,
)
from ws0_validate import (
    Invalid,
    _SHA256_RE,
    cli_count,
    http_endpoint,
    nonempty_selection,
    positive_int,
    perf_event_list,
)


# The pin's Flight-ticket digest field (#3272 round 10, M1). Defined HERE, in the module that owns
# the pin's shape, and imported by `ws0_ticket_input` — one spelling, so the writer below and the
# reader over there cannot drift onto two names, which would present as an absent-field refusal on
# a session that pinned the ticket correctly. (The dependency runs THIS WAY because
# `ws0_ticket_input` already imports `sha256_file` from here; the reverse would be a cycle.)
PIN_TICKET_FIELD = "ticket_template_sha256"

# THE PIN'S PROVENANCE MARKER (#3272 round 25). A STABLE VERSIONED TOKEN, written by
# `write_session_corpus_pin` below and required by `verify_session_corpus_pin` on EXACT EQUALITY.
#
# # The finding
#
# The reader used to accept any `components_source` CONTAINING the substring `measured`. So
#
#     "not measured; copied from sidecar"
#
# SATISFIED the provenance guard — a value whose plain English states the exact thing the guard
# exists to reject. The guard against round 21's defect admitted that defect's own confession.
#
# # Why a token, compared for equality
#
# A substring test asks "does this look a bit like the good value" where the only sound question is
# "IS this the good value". It is the cousin of the shape CLAUDE.md names as a rule — never derive a
# pass from the ABSENCE of a bad signal; key a permissive branch on the AFFIRMATIVE value — and of
# the trap one level down, where a PREFIX test (`PASS*`) accepts `PASSthisNeverRan`. Same fix in all
# three: reduce to the TOKEN and compare for EQUALITY. No substring, no prefix, no regex that could
# match a longer string, so `measured`, `measured-v2`, `remeasured-v1` and `MEASURED-V1` are all
# refused by construction rather than by enumeration.
#
# VERSIONED so the field can gain meaning later without the reader having to guess: a future writer
# recording something different records `measured-v2`, and THIS reader refuses it loudly instead of
# accepting it as near enough. That is also what makes an OLD-FORMAT artifact fail rather than pass
# silently — a pin carrying round 21's prose sentence is not this token.
COMPONENTS_SOURCE_MEASURED = "measured-v1"


def _measure_ticket_digest(session_dir: pathlib.Path) -> str:
    """The Flight ticket's digest, from `ws0_ticket_input` (#3272 round 10, M1).

    Imported function-locally for the same reason `verify_pinned_schema` and
    `verify_pinned_components` are: that module imports `sha256_file` from THIS one, so a
    module-scope import here would be a cycle. The split is by responsibility — see each module's
    docstring.

    Takes the SESSION dir since round 13's F2 moved the ticket out of the shared corpus and into the
    session's exclusively-claimed output directory.
    """
    from ws0_ticket_input import measure_ticket_digest

    return measure_ticket_digest(session_dir)


def write_session_corpus_pin(
    session_dir: pathlib.Path,
    corpus: pathlib.Path,
    identity: dict,
    config: dict | None = None,
    canonical: dict | None = None,
) -> dict:
    """Record WHICH CORPUS — and, since F1, WHICH CONFIGURATION — this session will measure.

    Called by `ws0-baseline.sh` BEFORE the first rep, and read back by the reporter (see
    `verify_session_corpus_pin`).

    # The finding (#3272 review round 4)

    The corpus digest was verified only against the corpus present AT REPORT TIME. No corpus
    identity was captured in the session dir before measurement, so two real sequences
    attributed measurements to bytes that were never measured:

    * RE-REPORTING an old result dir against a DIFFERENT corpus. `ws0_report.py --dir <old>
      --corpus <other>` re-derives `<other>`'s digest, finds it self-consistent, and prints it
      as the identity of figures measured over something else. Nothing in the old dir said
      which corpus it came from.
    * CHANGING THE CORPUS MID-RUN. A regeneration (or a second lane writing the same path)
      between rep 1 and rep N leaves report time verifying the LAST state of the corpus while
      the earlier reps measured the earlier bytes.

    Verifying at report time cannot see either, because both are consistent at report time.
    The pin is the missing half: an identity captured BEFORE, compared AFTER.

    What is recorded is the corpus path plus the size and the MEASURED digest of every component
    — see the round-21 section below, which retracts the previous version of this paragraph (it
    recorded the sidecar's digests unmeasured, on a cost argument). The digest is ALSO re-derived
    at report time (`verify_corpus_bytes`); what the pin adds is that the identity being
    re-derived is the one the session STARTED with, and `verify_corpus_boundary` adds that it was
    still that one at each measurement boundary in between.

    # SCOPE, corrected (#3272 round 6, B2)

    F3's version of this docstring claimed the pin "covers everything a scan reads". That was
    FALSE in the only sense that matters: the component map was written and read by nothing, so
    the coverage was of the FILE, not of the CHECK. It is true now — `verify_session_corpus_pin`
    compares every pinned component's name, size and digest against both the report-time
    identity and the bytes on disk — and stated as what the pin covers rather than as what it
    records, because "recorded" was the word doing the misleading work.

    # THE DIGESTS ARE MEASURED, not copied (#3272 round 21) — and the paragraph above was WRONG

    Everything above described a pin that COPIED `data_db_sha256` and the whole `components` map
    out of `corpus-identity.json`, on the argument (stated below, in the old text, as a cost
    decision) that hashing is on the measurement's critical path. That made the corpus's own
    sidecar both the subject and the oracle: the pin and the sidecar agreed BY CONSTRUCTION,
    whatever the bytes on disk had done, so every downstream comparison against the pin was the
    sidecar's assertion restated. A pin that copies a claim is not a measurement.

    `measure_component_digests` now HASHES the files, and the sidecar is COMPARED against the
    measured values — a disagreement is refused HERE, before the first rep, naming the component.
    The cost argument was also weaker than it read: the reporter already hashes the whole corpus
    once per run, so this adds one pass on the setup path, not one per rep.
    """
    # HASHED, not copied. Raises `Invalid` naming any component whose bytes disagree with the
    # sidecar, and fails closed on a component that cannot be read (#3272 round 21).
    measured = measure_component_digests(corpus, identity)
    pin = {
        "corpus": str(corpus),
        "rows": identity["rows"],
        # ...from the bytes, with the sidecar's own values already compared against them by
        # `measure_component_digests`. `identity["data_db_bytes"]`/`["data_db_sha256"]` are what
        # this used to record, and recording them was the round-21 finding.
        "data_db_bytes": measured["data_db_bytes"],
        "data_db_sha256": measured["data_db_sha256"],
        # THE COMPLETE COMPONENT SET (#3272 F3), every entry MEASURED FROM DISK (#3272 round 21).
        #
        # READ BACK by `verify_session_corpus_pin`, which compares this map against the
        # report-time identity AND the bytes on disk. That reader did NOT exist when F3 added
        # this field (#3272 round 6, B2, found independently by both reviewers): the map was
        # WRITTEN and compared against NOTHING ANYWHERE IN THE TREE, so the field read as a
        # guard while being inert. What F3 actually closed was `verify_corpus_components`,
        # against the corpus's OWN report-time `corpus-identity.json` — self-consistent at
        # report time, which is precisely the blind spot the PIN exists to cover.
        "components": measured["components"],
        # WHERE THE DIGESTS ABOVE CAME FROM, affirmatively (#3272 round 21). A reader can tell a
        # MEASURED pin from a COPIED one without inferring it, and `verify_session_corpus_pin`
        # REQUIRES this field — a pin that does not say its digests were observed is refused
        # rather than trusted, because a copied pin is textually identical to a measured one.
        #
        # A STABLE VERSIONED TOKEN since round 25, matched EXACTLY by the reader — see
        # `COMPONENTS_SOURCE_MEASURED`. It used to be `measured["source"]`, a PROSE SENTENCE, and
        # the reader tested it with `"measured" in source`, which accepted "not measured; copied
        # from sidecar". The sentence is retained beside the token, as `components_source_note`, for
        # a human reading the artifact: it is DESCRIPTION, and nothing is checked against it.
        "components_source": COMPONENTS_SOURCE_MEASURED,
        # The prose, kept for the reader of the file and deliberately NOT the checked field. Two
        # fields because they have two jobs: a machine needs one token it can compare, a human needs
        # a sentence — and conflating them is what made the guard satisfiable by English.
        "components_source_note": measured["source"],
        # THE SCHEMA DIGEST (#3272 R2). `ws0-events.cql` is a MEASUREMENT INPUT — both arms
        # read it, ASYMMETRICALLY (the bare scan ingests it per invocation; the Flight ticket is
        # generated from it once) — so a modification between setup and a later rep makes the
        # two arms measure DIFFERENT SCHEMAS while every other recorded identity still agrees.
        # READ BACK by `ws0_schema_input.verify_pinned_schema`.
        "schema_sha256": identity.get("schema_sha256"),
        # THE FLIGHT TICKET'S DIGEST (#3272 round 10, M1). `ticket-template.json` IS THE REQUEST
        # — keyspace, table, DDL, token range, column projection, predicates, aggregation, limit —
        # and `flight-loadgen --ticket-template` re-reads it on EVERY invocation of every rep of
        # every arm. It used to be created AFTER this pin and to appear in no verified record, so it
        # could be changed between reps or between ARMS while the corpus stayed untouched: every
        # corpus digest, the component set and the schema all still agreed, and the report exited 0
        # having compared two arms that answered DIFFERENT QUERIES. Same class as round 10's F-B one
        # layer out (F-B: different corpora; this: different requests).
        #
        # DERIVED HERE, from the bytes on disk, rather than accepted as an argument — deliberately.
        # A caller-supplied digest is a value this writer would record without observing, and a
        # recorded value nobody measured is the shape this whole issue exists to remove; it would
        # also give the fixture path a way to pin a request it never wrote. So there is exactly one
        # implementation and the pin's ticket field can only ever describe a real file.
        #
        # The corollary for the DRIVER: the template must EXIST by the time this runs, i.e. it must
        # be created BEFORE the pin. That ordering is the other half of the fix, and this call is
        # what enforces it — an absent template is `Invalid` here, not an absent pin field.
        #
        # Measured from the SESSION DIR, not the corpus (#3272 round 13, F2): the ticket lives in the
        # exclusively-claimed output directory, so a concurrent session measuring the same corpus
        # cannot write the file this digest describes.
        #
        # READ BACK by `ws0_ticket_input.verify_pinned_ticket`.
        PIN_TICKET_FIELD: _measure_ticket_digest(session_dir),
        # THE MEASUREMENT CONFIGURATION (#3272 F1). Recorded here, before rep 1, and READ BACK
        # BY THE REPORTER as its own configuration — see `session_manifest_config` for why the
        # reporter reads it rather than matching against it.
        "config": dict(config or {}),
        # WHETHER THE CORPUS IS THE CANONICAL MEASUREMENT CORPUS (#3272 round 13, F3).
        #
        # The `config.baseline_mode` word above says which claim the RUN makes; this records the
        # COMPARISON that word rests on — which canonical fields were compared, against which
        # source file, and every divergence found. Recorded rather than derived at report time for
        # the same reason the corpus pin exists at all: the canonical pin can be re-pinned between
        # measurement and reporting, and a comparison performed at report time would then be
        # against a shape the session never ran against.
        #
        # DERIVED, never accepted as an argument: `require_canonical_or_declared` performs the
        # comparison and the driver passes its RECORD through. A caller-supplied verdict would be a
        # value this writer records without observing, which is the shape this whole issue removes.
        "canonical_corpus": dict(canonical or {}),
        "note": (
            "the corpus identity AND THE MEASUREMENT CONFIGURATION this session was STARTED"
            " against, stamped before the first rep. ws0_report.py REQUIRES it, refuses a"
            " report whose corpus no longer matches, and TAKES ITS CONFIGURATION FROM HERE"
            " rather than from its own command line — re-reporting an old session dir against"
            " a different corpus, or with fewer reps / a narrower arm set / different CPU pins,"
            " is otherwise invisible because all of it is self-consistent at report time"
            " (#3272 round 4 and round 5 F1/F3)."
        ),
    }
    session_pin_path(session_dir).write_text(json.dumps(pin, indent=1) + "\n")
    return pin


# The configuration fields a session is DEFINED by — every one of which changes what was
# measured, so every one must come from the session rather than from the reporting command line.
#
# `reps`/`scan_passes` are COUNTS (validated as CLI counts); `temps`/`arms` are SELECTIONS;
# the rest are opaque strings recorded verbatim.
#
# # THE DISPOSITION IS DECLARED, AND CHECKED IN BOTH DIRECTIONS (#3272 round 9, F7)
#
# This tuple had NO ORACLE, unlike its two siblings in this rig — `RECORD_FIELD_DISPOSITION` is
# walked against the live `StepRecord`, and `COMPARED_FIELDS` against the artifact's real key set.
# MEASURED: adding an 8th field to the tuple left `session_manifest_config` returning the same 7
# keys with no error, and adding an unclassified `config.prewarm_mode="DISABLED-ENTIRELY"` to a
# manifest was silently ignored, rc=0, absent from results.json. All 7 declared fields ARE
# currently read, so there was no live unread field — but nothing forced the next one, and this
# was the ROOT OF F6: `server_cpus` sat in this list as an opaque string and reached the report's
# "verified physical-core siblings" claim having been validated by nothing.
#
# So each field now DECLARES how it is validated, and `check_manifest_config_surface` closes both
# directions:
#
#   * DECLARED-BUT-UNREAD — every declared field must be produced by the reader (asserted at
#     import against `_MANIFEST_READER_KEYS`), so adding a field to the tuple without wiring it
#     is an ERROR rather than a key that silently never appears in results.json;
#   * PRESENT-BUT-UNCLASSIFIED — a manifest carrying a `config` key nobody declared is REFUSED,
#     so the next configuration field cannot be silently ignored the way `prewarm_mode` was.
#
# The `server_cpus` entry records what F6 established: it is opaque HERE (re-parsing it would be
# a second implementation of `cpu_list_expand`, which is the right call) and it is tied to a real
# verification ELSEWHERE, by `ws0_pinning.verify_pinning_record`. "Opaque" is only acceptable
# because something else is not.
MANIFEST_CONFIG_DISPOSITION: dict[str, str] = {
    "reps": "validated as a CLI COUNT (cli_count), so a hand-edited `reps: 0` cannot produce a"
            " vacuous-but-successful report (#3272 finding 5)",
    "scan_passes": "validated as a CLI COUNT, and cross-checked against the per-pass records by"
                   " the bare-scan collector (#3272 F2)",
    "temps": "validated as a NON-EMPTY SELECTION over TEMPS_ALLOWED",
    "arms": "validated as a NON-EMPTY SELECTION over ARMS_ALLOWED",
    "server_cpus": "a non-empty recorded STRING here — deliberately not re-parsed, because that"
                   " would be a second implementation of cpu_list_expand — and tied to a REAL"
                   " sibling verification by ws0_pinning.verify_pinning_record, which requires"
                   " the driver's record and requires this value to equal the list actually"
                   " verified (#3272 F6). Opaque here is only acceptable because it is checked"
                   " there.",
    "client_cpus": "as server_cpus: a non-empty recorded STRING, tied to the driver's recorded"
                   " verification by ws0_pinning.verify_pinning_record (#3272 F6)",
    "flight_server_cpus": "as server_cpus — a non-empty recorded STRING, opaque HERE and tied to"
                          " a REAL pin verification by ws0_pinning.verify_pinning_record, which"
                          " requires this value to equal the flight list the driver actually"
                          " checked against the host topology (#3551). Recorded rather than"
                          " derived from server_cpus, and that is the substance: the FLIGHT arm"
                          " can now be pinned differently from the bare-scan arm (the SMT-unpin"
                          " trial), which changes both where the measured server ran AND the"
                          " CPU-wide counting domain of every flight rep. A session whose flight"
                          " pin is not recorded cannot be told apart from one where both arms"
                          " shared a core, and those are different measurements. It EQUALS"
                          " server_cpus for every run that does not pass --flight-server-cpus",
    "env_ambient": "validated as a non-empty recorded STRING that NAMES EVERY KEY the rig"
                   " records (LD_PRELOAD, LD_LIBRARY_PATH, RUSTFLAGS, CARGO_ENCODED_RUSTFLAGS,"
                   " MALLOC_VARS), so a field that silently dropped one is REFUSED rather than"
                   " read as `that variable was unset`. The substance: with ONE binary set"
                   " across all arms — deliberate — the ENVIRONMENT is the only thing that"
                   " differs between the system and jemalloc arms, and it was recorded NOWHERE"
                   " (the only os.environ read in this whole path was WS0_BUILD_MODE), which"
                   " made arm C unfalsifiable. AMBIENT is what the DRIVER's own environment"
                   " carried, as measured; the values are affirmative (`<unset>`/`<none>`,"
                   " never blank). docs/reports/ws0-3552-report.md §4 is the governing rule:"
                   " state RUSTFLAGS and CARGO_ENCODED_RUSTFLAGS AS MEASURED, because a"
                   " reproduction only corroborates if its ENVIRONMENT differs — not just its"
                   " tree, box or operator (#3551)",
    "env_injected": "as env_ambient, and SEPARATE from it deliberately: `the operator had a"
                    " stray LD_PRELOAD` and `the rig set one on purpose` are different facts and"
                    " only one of them is a defect, so they are never merged into one field."
                    " Validated as a non-empty recorded STRING naming the flight server as the"
                    " ONLY injection site and the bare scan as receiving nothing — that scoping"
                    " IS the method (an exported LD_PRELOAD would reach ws0-scan-bench and put"
                    " the DRIFT CONTROL on the allocator under test, breaking §3b step 3), and"
                    " the driver asserts it per rep (#3551)",
    "step_duration": "a non-empty recorded STRING (`<warm>/<cold>`), reported verbatim; the"
                     " DURATIONS that bound a rep were validated by the driver's own argument"
                     " checks (lib-args.sh) before the session ran",
    "flight_endpoint": "validated as an ABSOLUTE `http://<host>:<port>` URL (a bare host, a bare"
                       " port, or a trailing path is REFUSED), and — the substance — compared"
                       " EXACTLY against the `endpoint` field of EVERY loadgen record of every rep"
                       " of both arms by ws0_loadgen_record.check_session_bound_inputs, which is"
                       " what makes it provenance rather than a recorded string. `endpoint` was"
                       " classified IGNORED (`the loopback address; not a measurement`), so a"
                       " record produced against a DIFFERENT server — a peer lane's process on"
                       " another port, or a remote host — passed every row/request/error/shed/rate"
                       " check and had its rows divided by THIS session's perf counters, collected"
                       " on pinned local cores that served nothing (#3272 round 14, F2). Pinned"
                       " HERE, before the first rep, so it is the pre-measurement pin every other"
                       " identity in this manifest is an identity OF",
    "events": "parsed into a NON-EMPTY, DUPLICATE-FREE tuple of perf event names by"
              " ws0_validate.perf_event_list, each matching a conservative charset. Recorded"
              " because the report's cycles/row and IPC are claims ABOUT SPECIFIC COUNTERS and"
              " the event set became configurable in #3248 (the AC4 clock basis needs"
              " msr/aperf,msr/mperf,msr/tsc,ref-cycles, which the default two-event set cannot"
              " supply). DUPLICATE-FREE is the substantive part rather than tidiness:"
              " ws0_validate.read_perf_counters SUMS lines by event name, so `-e cycles,cycles`"
              " would emit two `cycles` rows and report DOUBLE the true count as an ordinary"
              " integer, with every derived figure inheriting the factor of two",
    "quiescence": "validated as a non-empty recorded string, and one of exactly two shapes:"
                  " `NOT VERIFIED (no timeseries supplied)` or `judged against <path>`."
                  " Recorded BOTH ways on purpose: a session with no external load timeseries"
                  " is not quiet, it is UNVERIFIED, and a reader cannot otherwise tell a"
                  " checked run from an unchecked one. The rig cannot demand a timeseries"
                  " (it is produced outside the rig, so requiring one would fail every box"
                  " without it), which is exactly why the absence has to be legible instead"
                  " of implicit (#3248, roborev job 62 finding 2)",
    "profile": "validated as either the literal `off` or `on freq=<N>` with a positive N."
               " Recorded because `bin_dir` CANNOT establish whether a sampling profile was"
               " attached -- the same symbol-bearing build runs with and without"
               " `--profile-out`, so a claim that bin_dir distinguishes a profiled run was"
               " simply wrong (#3248, roborev job 60 finding 1). It matters because a profiled"
               " run pays measurable observer overhead (1.6-4.3% on rows/s as measured), so its"
               " throughput figures must never be read as a baseline, and results.json is where"
               " a reader looks to discover that",
    "bin_dir": "validated as a non-empty recorded string, and — the substance — the directory"
               " the measured binaries were taken FROM, which #3248 needs because"
               " [profile.release] sets strip = true and a stripped binary cannot be attributed"
               " per-function at all. Recorded rather than assumed because the reps execute FROZEN"
               " COPIES under $OUT_DIR/measured-bin/, so the digests describe the bytes that ran"
               " but NOT which build produced them; without this field a perfsym run and a"
               " release run are indistinguishable in results.json",
    "baseline_mode": "validated as one of ws0_canonical_corpus.MODE_BASELINE /"
                     " MODE_NON_BASELINE, and — the substance — tied to a REAL pre-measurement"
                     " COMPARISON against the canonical pin in"
                     " tools/ws0-corpus-gen/src/measurement_corpus.rs, recorded in the pin's"
                     " `canonical_corpus` block. `baseline` is only recordable for a corpus that"
                     " was OBSERVED to match every canonical field, because the driver refuses"
                     " the run otherwise (#3272 round 13, F3); a noncanonical corpus can only"
                     " reach `non-baseline`, which the report LABELS in words. Opaque-string"
                     " recording would have been the F6 shape all over again",
}

# Declaration order preserved, and DERIVED from the disposition rather than written twice: two
# copies of the field list is the drift this issue keeps finding.
MANIFEST_CONFIG_FIELDS = tuple(MANIFEST_CONFIG_DISPOSITION)

# The keys `session_manifest_config` actually PRODUCES, besides its own `source`. Named here so
# the declared-but-unread direction can be asserted at import — a field added to the disposition
# and never wired would otherwise be a key that silently never reaches results.json.
_MANIFEST_READER_KEYS = (
    "reps",
    "scan_passes",
    "temps",
    "arms",
    "server_cpus",
    "client_cpus",
    "flight_server_cpus",
    "env_ambient",
    "env_injected",
    "step_duration",
    "flight_endpoint",
    "baseline_mode",
    "events",
    "bin_dir",
    "profile",
    "quiescence",
)

# AT IMPORT, both directions, so a half-wired field cannot ship (the pattern
# `ws0_loadgen_record.py` established for `ZERO_REQUIRED_COUNTERS`).
for _k in MANIFEST_CONFIG_FIELDS:
    if _k not in _MANIFEST_READER_KEYS:
        raise Invalid(
            f"`{_k}` is declared in MANIFEST_CONFIG_DISPOSITION but is not produced by"
            " session_manifest_config — a declared field the reader never emits is a"
            " configuration property the report claims to take from the session and does not"
            " (#3272 F7). Wire it, or remove the declaration."
        )
    if not MANIFEST_CONFIG_DISPOSITION[_k].strip():
        raise Invalid(f"`{_k}` must declare HOW it is validated, not an empty string")
for _k in _MANIFEST_READER_KEYS:
    if _k not in MANIFEST_CONFIG_DISPOSITION:
        raise Invalid(
            f"session_manifest_config produces `{_k}` but it is not declared in"
            " MANIFEST_CONFIG_DISPOSITION — every configuration field must record how it is"
            " validated, because an opaque field reaches the report as an unchecked claim"
            " (#3272 F6/F7)."
        )
del _k


def check_manifest_config_surface(path: pathlib.Path, config: dict) -> None:
    """Refuse a manifest `config` carrying a field nobody CLASSIFIED (#3272 round 9, F7).

    The mirror of `ws0_loadgen_record.check_record_surface`, for the configuration surface.
    MEASURED before this existed: adding `config.prewarm_mode = "DISABLED-ENTIRELY"` to a
    manifest was silently ignored — rc=0, absent from results.json — so a driver that grew a new
    configuration knob would have had it dropped on the floor by the reporter while the report
    continued to describe the session as fully characterised.

    Refused rather than warned, for the reason this whole issue exists: a configuration field
    nobody reads is a property of the measurement the report does not know about, and a report
    that does not know a knob was set cannot say anything true about what it measured.
    """
    unknown = sorted(k for k in config if k not in MANIFEST_CONFIG_DISPOSITION)
    if unknown:
        raise Invalid(
            f"{path} `config` carries field(s) this reporter has never classified:"
            f" {', '.join(unknown)}. Every configuration field must be declared in"
            " MANIFEST_CONFIG_DISPOSITION with HOW it is validated, because an unclassified"
            " field is one nobody reads: MEASURED, a `prewarm_mode` added to a manifest was"
            " silently ignored and absent from results.json (#3272 F7). Declare it — and if it"
            " can change what a figure means, VALIDATE it rather than recording it verbatim."
        )


def session_manifest_config(
    session_dir: pathlib.Path, temps_allowed: tuple[str, ...], arms_allowed: tuple[str, ...]
) -> dict:
    """THE session's measurement configuration, read FROM the manifest (#3272 F1).

    # The finding

    `ws0_report.py` took `--reps`, `--temps`, `--arms`, `--scan-passes`, the CPU pins and the
    step durations from ITS OWN command line. Nothing tied any of them to the session being
    reported, so re-reporting a measured session dir with a DIFFERENT configuration produced a
    successful, confident report:

    * `--reps 1` over a 3-rep session IGNORED two thirds of the measured artifacts and
      published rep 1 as the median of the run;
    * `--arms bypass` over a both-arms session silently dropped the `merge` arm and printed no
      PARTIAL MATRIX banner, because as far as the reporter knew, bypass was all that was asked
      for;
    * different `--server-cpus`/`--client-cpus` printed the REPLACEMENT pins under
      "pinning: … (verified physical-core siblings)" — a verification claim about CPUs the
      session never used;
    * a different `--scan-passes` mismatched the per-pass contract (see F2) or, before F2,
      nothing at all.

    In every case the report ASSERTED the replacement configuration had been verified.

    # Why the reporter READS the configuration rather than MATCHING its arguments against it

    Both close the finding; reading is strictly stronger, and the reason is the one this issue
    keeps rediscovering: **a value that cannot be supplied cannot disagree.** A matching design
    keeps seven parameters on the command line and adds seven comparisons — so it needs each
    comparison to be present and correct, and a parameter added later without a comparison is
    silently unchecked again (exactly F4's shape: a field nobody wired). Reading removes the
    input entirely: there is no `--reps` to be wrong, so there is no `--reps` check to omit.

    The residual cost of reading, stated rather than discovered: the reporter can no longer be
    pointed at a session with a deliberately different configuration. That is the POINT — such
    a report was never valid — and the honest way to report a subset is to say so, which is
    what the recorded selection and its PARTIAL MATRIX banner already do.

    The reporter's `--reps`/`--temps`/`--arms`/`--scan-passes`/CPU flags are therefore REMOVED
    rather than left as ignored arguments: an accepted-and-ignored flag is a silent lie to the
    operator who passed it.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so it does not record"
            " WHAT CONFIGURATION it measured. The reporter takes its reps, temperatures, arms,"
            " scan-passes and CPU pins FROM that record (#3272 F1), because taking them from"
            " the reporting command line let a re-report with fewer reps or a narrower arm set"
            " ignore measured artifacts and still claim the replacement configuration had been"
            " verified. Re-run the session with scripts/perf/ws0-baseline.sh, which stamps the"
            " manifest before the first rep."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    config = pin.get("config")
    if not isinstance(config, dict) or not config:
        raise Invalid(
            f"{p} records no `config` — this session dir predates the configuration manifest"
            " (#3272 F1), so WHAT was measured (how many reps, which temperatures and arms,"
            " how many scan passes, which CPUs) is unknown. It cannot be supplied from the"
            " command line: that is exactly the substitution the manifest exists to prevent."
            " Re-run the session with the current driver."
        )
    absent = [f for f in MANIFEST_CONFIG_FIELDS if f not in config]
    if absent:
        raise Invalid(
            f"{p} `config` is INCOMPLETE — no {', '.join(absent)}. Every field of the"
            " configuration is one the report makes a claim about, so a partial manifest cannot"
            " establish what this session measured. Re-run the session with the current driver."
        )
    # ...and the OTHER direction (#3272 F7): a field PRESENT in the manifest that nobody
    # classified is refused, so the next configuration knob cannot be silently dropped the way a
    # `prewarm_mode` was (measured: rc=0, ignored, absent from results.json).
    check_manifest_config_surface(p, config)
    # Each field through the SAME validator the CLI used to apply, so a hand-edited manifest
    # cannot smuggle a vacuous configuration (`reps: 0` was #3272 finding 5) past the reader.
    out = {
        "reps": cli_count("reps", config["reps"]),
        "scan_passes": cli_count("scan-passes", config["scan_passes"]),
        "temps": nonempty_selection("temps", str(config["temps"]), temps_allowed),
        "arms": nonempty_selection("arms", str(config["arms"]), arms_allowed),
    }
    # A CLOSED SET, not a recorded string (#3272 round 13, F3). An unrecognised value is REFUSED
    # rather than reported verbatim, for the reason the roborev wrapper's verdict scan is a closed
    # grammar: a mode nobody planned for would otherwise reach the report as a claim, and the
    # report's whole job here is to say whether the run IS a baseline. `baseline` additionally
    # requires the recorded canonical COMPARISON below — the word alone establishes nothing.
    mode = config["baseline_mode"]
    if mode not in (MODE_BASELINE, MODE_NON_BASELINE):
        raise Invalid(
            f"{p} `config.baseline_mode` is {mode!r}, which is not"
            f" {MODE_BASELINE!r} or {MODE_NON_BASELINE!r}. An unrecognised mode is refused rather"
            " than reported verbatim: the report states whether this run is a WS0 baseline, and a"
            " value nobody planned for cannot support either answer (#3272 round 13, F3)."
        )
    out["baseline_mode"] = mode
    # THE MEASURED SERVER (#3272 round 14, F2). Validated STRUCTURALLY, not recorded opaquely: it
    # is compared EXACTLY against every loadgen record's `endpoint`, so an unusable pin (a bare
    # host, a bare port, a prose placeholder) would compare unequal to every real record and refuse
    # the session while blaming the ARTIFACT — the error caught at the wrong end. Refused here, where
    # the mis-stamped pin is.
    out["flight_endpoint"] = http_endpoint(
        f"{p} `config.flight_endpoint`",
        config["flight_endpoint"],
        "It is the pre-measurement pin of WHICH SERVER produced the measured rows: every loadgen"
        " record of every rep of both arms is compared against it, because a record from a"
        " different server had its rows divided by THIS session's perf counters, collected on"
        " pinned local cores that served nothing (#3272 round 14, F2).",
    )
    # THE COUNTED EVENTS (#3248). Through the same parser the driver applies, so a hand-edited
    # manifest cannot smuggle an empty or duplicate-bearing event set past the reader — the
    # duplicate case matters because `read_perf_counters` sums by event name and would silently
    # double a repeated counter.
    out["events"] = perf_event_list(f"{p} `config.events`", config["events"])
    prof = str(config["profile"])
    if prof != "off":
        m = re.fullmatch(r"on freq=(\d+)", prof)
        if not m or int(m.group(1)) <= 0:
            raise Invalid(
                f"{p} `config.profile` is {prof!r}, which is neither `off` nor `on freq=<N>`"
                " with a positive N. A closed grammar, not a recorded string: an unrecognised"
                " value would otherwise reach the report as a claim about whether these"
                " figures carry profiler overhead."
            )
    out["profile"] = prof
    quies = str(config["quiescence"])
    if quies != "NOT VERIFIED (no timeseries supplied)" and not quies.startswith(
        "judged against "
    ):
        raise Invalid(
            f"{p} `config.quiescence` is {quies!r}, which is neither the unverified sentinel"
            " nor `judged against <path>`. A closed grammar, not a recorded string: an"
            " unrecognised value would reach the report as a claim about whether this session"
            " was checked for competing load."
        )
    out["quiescence"] = quies
    # THE ENVIRONMENT RECORDS (#3551). `env_ambient` is checked for KEY COMPLETENESS rather
    # than parsed into a mapping: the values are recorded VERBATIM (a RUSTFLAGS containing the
    # renderer's `; ` separator would be visually ambiguous, and mangling it would make the field
    # lie about what was measured), so what is asserted is that every key the rig claims to
    # record is NAMED. A field that silently dropped one would otherwise read as "that variable
    # was unset", which is a different fact and the permissive one.
    for key in ("env_ambient", "env_injected"):
        value = config[key]
        if not isinstance(value, str) or not value.strip():
            raise Invalid(
                f"{p} `config.{key}` is {value!r}, which is not a recorded value. With one binary"
                " set across all arms the ENVIRONMENT is the only thing that distinguishes the"
                " allocator arms, so an empty record here makes them indistinguishable in the"
                " artifact — the state that made arm C unfalsifiable (#3551)."
            )
    absent_keys = [
        k for k in ("LD_PRELOAD", "LD_LIBRARY_PATH", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS",
                    "MALLOC_VARS")
        if f"{k}=" not in config["env_ambient"]
    ]
    if absent_keys:
        raise Invalid(
            f"{p} `config.env_ambient` names no {', '.join(absent_keys)}. Every key the rig"
            " records is one a reproduction has to be able to compare, and an ABSENT key reads"
            " exactly like a variable that was unset — a different fact, and the permissive one"
            " (ws0-3552 §4: state them AS MEASURED). Re-run the session with the current driver."
        )
    out["env_ambient"] = config["env_ambient"]
    out["env_injected"] = config["env_injected"]
    for key in ("server_cpus", "client_cpus", "flight_server_cpus", "step_duration", "bin_dir"):
        value = config[key]
        if not isinstance(value, str) or not value.strip():
            raise Invalid(
                f"{p} `config.{key}` is {value!r}, which is not a recorded value. The report"
                f" prints it as a property of the measurement, so it must have been observed."
            )
        out[key] = value
    out["source"] = str(p)
    return out



def verify_session_corpus_pin(
    session_dir: pathlib.Path,
    corpus: pathlib.Path,
    identity: dict,
    measured_components: dict | None = None,
) -> dict:
    """Require the session's PRE-MEASUREMENT corpus pin, and require it to still match.

    REQUIRED, not optional: an absent pin means this session dir does not record which corpus
    it measured, and a report over it would attribute its figures to whatever `--corpus` the
    reader happened to pass. That is the fail-open shape — a check that silently does not run
    while the summary prints a digest as the measured one.

    Compared on all three of PATH, SIZE and DIGEST, each for a different reason:

    * the recorded DIGEST is the identity itself. A different digest is a different corpus.
    * the recorded SIZE is compared too, so a pin whose digest field was hand-edited to match
      still has to agree on a second, independent number.
    * the PATH is compared last and is the WEAKEST of the three — a corpus can legitimately be
      moved — so a path difference alone is REPORTED in the record rather than fatal. The two
      byte-level fields are what decide.

    ...and, since #3272 round 6 B2, THE COMPLETE PINNED COMPONENT SET, delegated to
    `_verify_pinned_components`. The three `Data.db` fields above cannot see an auxiliary
    component replaced mid-session, which is a real and silent way to change the read pattern
    being measured. That comparison is UNCONDITIONAL: it is not gated on a flag, because a pin
    component check that can be switched off is the fail-open shape one level out.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so it does not record"
            " WHICH CORPUS it measured. A report over it would attribute its figures to"
            " whatever --corpus the reader passed: re-reporting an old result dir against a"
            " different corpus is self-consistent AT REPORT TIME and therefore invisible to"
            " the report-time digest check (#3272 round 4). Re-run the session with"
            " scripts/perf/ws0-baseline.sh, which stamps the pin before the first rep."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    for key in ("rows", "data_db_bytes", "data_db_sha256"):
        if key not in pin:
            raise Invalid(
                f"{p} carries no {key!r} — the session's corpus pin is incomplete, so it"
                " cannot establish which bytes this session measured"
            )
    pinned_rows = positive_int(f"{p}: 'rows'", pin["rows"])
    pinned_bytes = positive_int(f"{p}: 'data_db_bytes'", pin["data_db_bytes"])
    pinned_sha = pin["data_db_sha256"]
    if not isinstance(pinned_sha, str) or not _SHA256_RE.match(pinned_sha):
        raise Invalid(
            f"{p}: 'data_db_sha256' must be 64 lowercase hex characters (got"
            f" {pinned_sha!r}); a truncated pin cannot identify the measured bytes"
        )
    if pinned_sha != identity["data_db_sha256"]:
        raise Invalid(
            f"THE CORPUS CHANGED. This session was started against a corpus whose Data.db"
            f" sha256 is {pinned_sha} (stamped in {SESSION_CORPUS_PIN} before the first rep),"
            f" but --corpus {corpus} now records {identity['data_db_sha256']}. Every figure in"
            " this session was measured over the PINNED bytes; reporting it under this"
            " corpus's identity would attribute the measurements to bytes that were never"
            " measured. Two real ways to get here, both invisible to the report-time digest"
            " check because both are self-consistent at report time: re-reporting an old"
            " result dir against a different corpus, and a corpus regenerated (or written by"
            " another lane) DURING the run (#3272 round 4). Point --corpus at the corpus this"
            " session measured, or re-run the session."
        )
    if pinned_bytes != identity["data_db_bytes"] or pinned_rows != identity["rows"]:
        raise Invalid(
            f"THE CORPUS SHAPE CHANGED under this session. {SESSION_CORPUS_PIN} records"
            f" {pinned_rows:,} rows / {pinned_bytes:,} Data.db bytes; --corpus {corpus} now"
            f" records {identity['rows']:,} rows / {identity['data_db_bytes']:,} bytes."
            " The digest matched, so this is an identity file that was edited rather than"
            " regenerated — two independent numbers must agree, not one."
        )
    # THE PIN'S DIGESTS MUST HAVE BEEN MEASURED (#3272 round 21). A pin that COPIED them out of
    # `corpus-identity.json` is textually indistinguishable from one that hashed the files, so the
    # writer records WHERE they came from and this REQUIRES the record: without it, every
    # comparison below is against the sidecar's own assertion, restated. Refused rather than
    # warned — the failure biases TOWARD the claim (a session that measured inconsistent bytes
    # reports as identity-verified), which is the direction that must never be captioned.
    #
    # EXACT EQUALITY against a stable versioned token (#3272 round 25), NOT a substring. This test
    # used to be `"measured" not in source`, and the value `"not measured; copied from sidecar"`
    # therefore PASSED it — MEASURED against the pre-fix reader, which accepted that string. A guard
    # whose subject is provenance was satisfiable by a sentence CONFESSING the absence of provenance.
    source = pin.get("components_source")
    if source != COMPONENTS_SOURCE_MEASURED:
        raise Invalid(
            f"{p} does not record that its component digests were MEASURED:"
            f" `components_source` is {source!r}, and the only accepted value is"
            f" {COMPONENTS_SOURCE_MEASURED!r} — compared EXACTLY, so no near-miss"
            " (`measured`, `measured-v2`, `remeasured-v1`, `MEASURED-V1`) and no sentence"
            " CONTAINING the word passes. Until #3272 round 21 the pin COPIED `data_db_sha256`"
            " and the whole component map out of corpus-identity.json, so the pin and that"
            " sidecar agreed BY CONSTRUCTION however the bytes on disk differed — every"
            " comparison against such a pin is the sidecar's own claim restated. A pin whose"
            " digests were not observed cannot establish what this session measured. Round 25:"
            " this check was a SUBSTRING test, which accepted the literal value 'not measured;"
            " copied from sidecar' — the guard admitted the confession of the very defect it"
            " exists to reject. A pin written before round 25 carries a prose sentence rather"
            " than this token and is refused HERE, loudly, rather than accepted as near enough."
            " Re-run the session with the current driver."
        )
    # THE PINNED COMPONENT SET — unconditional (#3272 round 6, B2). Raises on any divergence.
    # Imported function-locally, like the schema check below: both sibling modules import
    # `sha256_file`/`CORPUS_TABLE_SUBPATH` from THIS module, so a module-scope import here would
    # be a cycle. The split is by responsibility (see each module's docstring).
    from ws0_pin_components import verify_pinned_components

    comps = verify_pinned_components(p, pin, corpus, identity, measured_components)
    # ...and THE PINNED SCHEMA (#3272 round 6, R2), same argument: verifying the schema against
    # the corpus's own report-time identity cannot see a schema replaced mid-session with that
    # identity refreshed beside it. Imported here rather than at module scope because
    # `ws0_schema_input` imports `sha256_file` from THIS module — a function-local import keeps
    # the dependency one-directional at import time.
    from ws0_schema_input import verify_pinned_schema

    schema = verify_pinned_schema(p, pin, corpus, identity)
    # ...and THE FLIGHT TICKET (#3272 round 10, M1) — THE REQUEST. Same argument as the schema's,
    # one layer out: the schema decides how the bytes are INTERPRETED, the ticket decides WHICH
    # QUERY is asked. `flight-loadgen --ticket-template` re-reads it on every invocation of every
    # rep of every arm, and it was created AFTER this pin and recorded NOWHERE — so a template
    # changed between reps, or between ARMS, left the corpus untouched and therefore left every
    # corpus digest, the component set and the schema all in agreement while the report compared
    # two arms that answered different questions.
    #
    # UNCONDITIONAL, like the component and schema comparisons: a request check that can be
    # switched off is the fail-open shape one level out, and at a few hundred bytes a skip could
    # only buy a vacuous green. Function-local import for the cycle reason above.
    from ws0_ticket_input import verify_pinned_ticket

    # THE SESSION DIR, not the corpus (#3272 round 13, F2): the request is a property of THIS
    # session and lives in its exclusively-claimed output directory.
    ticket = verify_pinned_ticket(p, pin, session_dir)
    return {
        "pinned_before_measurement": True,
        "pinned_corpus_path": pin.get("corpus"),
        "pinned_data_db_sha256": pinned_sha,
        "pinned_data_db_bytes": pinned_bytes,
        "pinned_rows": pinned_rows,
        # The WEAKEST of the three comparisons, reported rather than enforced: a corpus can
        # legitimately be moved, and the two byte-level fields already decided the question.
        "corpus_path_unchanged": pin.get("corpus") == str(corpus),
        # WHERE THE PIN'S DIGESTS CAME FROM (#3272 round 21) — carried into results.json, so a
        # reader of a report can tell a MEASURED identity from a COPIED one.
        "pinned_components_source": source,
        "pinned_components": comps["pinned_components"],
        "pinned_components_verified_size": comps["pinned_components_verified_size"],
        "pinned_components_verified_sha256": comps["pinned_components_verified_sha256"],
        "pinned_components_note": comps["note"],
        "pinned_schema_sha256": schema["pinned_schema_sha256"],
        "pinned_schema_note": schema["note"],
        # THE REQUEST (#3272 round 10, M1) — the Flight ticket, pinned before the first rep and
        # re-derived here from disk.
        "pinned_ticket_sha256": ticket["pinned_ticket_sha256"],
        "pinned_ticket_bytes": ticket["ticket_bytes"],
        "pinned_ticket_note": ticket["note"],
        "note": (
            "the corpus identity was captured in the session dir BEFORE the first rep and"
            " re-compared here on rows + data_db_bytes + sha256, PLUS the complete pinned"
            f" component set ({comps['pinned_components']} component(s)) against both the"
            " report-time identity and the bytes on disk, PLUS the SCHEMA and the FLIGHT TICKET"
            " (the request every Flight rep re-read); the path is reported, not enforced (a corpus"
            " may be moved)"
        ),
    }
