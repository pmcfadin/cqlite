#!/usr/bin/env python3
"""delivery-telemetry.py — delivery-pipeline telemetry ledger + recurring retro.

Closes the pipeline self-improvement loop:
  sense    -> an append-only ledger, one record per delivery cycle (issue, pr) (`record`)
  diagnose -> rank recorded failures, file a deduped flow-meta issue (`retro`)
  improve  -> that issue runs through the normal pipeline

A "delivery cycle" is one shipped PR: a reopened issue that ships more than once
legitimately has one record per shipped PR (e.g. #2264 via PR #2282 then PR #2301), so the
dup key is the (issue, pr) pair, not the issue alone. Retro aggregation by issue should
treat such multi-cycle issues as multiple deliveries (they are), not fold them into one.

The same holds for a SLICE delivery (issue #3550): an issue that ships one or more PRs while
DELIBERATELY remaining OPEN (the shape the lead ruled correct on #3393). Such a record is
stamped with `--slice`, carries `closed_at: null` (the marker), and bounds its cycle time on
the PR's mergedAt. `--slice` asserts a fact about DELIVERY time, which the issue's CURRENT
state cannot decide, so it is refused in THREE states: the issue is closed now; it is open
only because it was REOPENED; or GitHub has not yet recorded this PR's auto-close (the PR
declares it closes the issue, so it is a completed delivery, not a slice). The first two need
the issue timeline replayed to mergedAt (issue #3559); the third clears itself in seconds. Closing the issue to satisfy this tool, or hand-appending a line past the
validator, are both FORBIDDEN — a tool's data model must never decide whether a problem is
recorded as solved, and `retro` reports slice records as their own class.

Authoritative-data-only mandate (CLAUDE.md / issue #28): every field is an observed
event — a GitHub timestamp/label or a run counter supplied by the stamping step — or
arithmetic over authoritative timestamps. Nothing is inferred, estimated, or guessed.
A required run counter that was not observed is an ERROR, never a fabricated zero.

Subcommands:
  record    build one ledger record and append it (one line)
  lint      schema-validate every line of the ledger        (alias: validate)
  retro     rank failure categories; optionally file a deduped flow-meta issue

The two GitHub-touching paths (`record` live-pull, `retro --file`) sit behind explicit
seams (`--from-json`, `--open-issues-json`, `--ledger`) so the unit tests run with no
network and no datasets.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- paths

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = REPO_ROOT / "docs" / "reports" / "delivery-telemetry.jsonl"
DEFAULT_SCHEMA = REPO_ROOT / "docs" / "reports" / "delivery-telemetry.schema.json"

# --------------------------------------------------------------------------- retro policy
#
# Failure-category weights are POLICY CONSTANTS (a documented, deterministic tally —
# not an inferred or learned cost model). The retro rank is Sum(count * weight) per
# category over the ledger. Tuning a weight is a constant edit, never a guess about
# what the data means. Order of magnitude reflects how expensive each failure is to
# the pipeline: a failed gate / rework round costs the most; a single rebase the least.
RETRO_WEIGHTS = {
    "gate_failures": 5,      # an agent-gate.sh FAIL round
    "rework": 4,             # a re-open / re-review round
    "claim_collisions": 3,   # a lost claim race (wasted setup)
    "roborev_findings": 2,   # a roborev finding to resolve
    "rebase_events": 2,      # a rebase / conflict resolution
}

# Required run counters for `record` — each MUST be supplied; a missing one is an error
# (authoritative-data-only: we never fabricate a count we did not observe).
REQUIRED_COUNTERS = ("claim_collisions", "rebase_events", "gate_runs", "roborev_findings", "rework")

# The `gate` value meaning "no full gate of record ran" (issue #3448). It is a legal VALUE,
# never a default: --gate/--gate-runs stay required, and this value is coupled to
# gate_runs == 0 in both directions (see _validate_gate_coupling).
GATE_NOT_RUN = "not-run"


# ============================================================ minimal JSON-Schema check
#
# A pure-stdlib subset validator (keeps the tool dependency-free / SKIP-aware on python3
# alone). The schema file remains the source of truth; this enforces the keywords the
# record schema actually uses: type, required, properties, const, enum, minimum, pattern.

def _type_ok(value, t) -> bool:
    """True when `value` matches JSON-Schema type `t`.

    `t` is a type NAME or a LIST of names (a JSON Schema type union, e.g. the nullable
    `closed_at` of issue #3550): a union holds when ANY member matches.
    """
    if isinstance(t, list):
        return any(_type_ok(value, member) for member in t)
    if t == "object":
        return isinstance(value, dict)
    if t == "array":
        return isinstance(value, list)
    if t == "string":
        return isinstance(value, str)
    if t == "integer":
        # bool is a subclass of int — exclude it
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t == "boolean":
        return isinstance(value, bool)
    if t == "null":
        return value is None
    return True


def _has_type(schema: dict, name: str) -> bool:
    """True when `schema`'s `type` is `name` or a union containing it.

    Used by the structural (array items / object properties) branches so a union type such
    as ["object", "null"] still gets its members walked rather than silently skipped.
    """
    declared = schema.get("type")
    return declared == name or (isinstance(declared, list) and name in declared)


def _validate(value, schema: dict, path: str, errors: list) -> None:
    if "const" in schema:
        const = schema["const"]
        # bool is a subclass of int (True == 1), so guard the coercion: a "schema": true
        # must not satisfy const: 1.
        if value != const or isinstance(value, bool) != isinstance(const, bool):
            errors.append(f"{path}: must equal {const!r}, got {value!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: must be one of {schema['enum']}, got {value!r}")
    if "type" in schema and not _type_ok(value, schema["type"]):
        errors.append(f"{path}: expected type {schema['type']}, got {type(value).__name__}")
        return  # further checks assume the type held
    if "minimum" in schema and isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < schema["minimum"]:
            errors.append(f"{path}: {value} < minimum {schema['minimum']}")
    if "pattern" in schema and isinstance(value, str):
        # fullmatch (anchored) rather than search: the schema is the source of truth for
        # field validity, and our patterns describe the WHOLE field. Matches the
        # re.fullmatch used to derive priority from labels — no partial-match surprises.
        if not re.fullmatch(schema["pattern"], value):
            errors.append(f"{path}: {value!r} does not match /{schema['pattern']}/")
    if schema.get("format") == "date-time" and isinstance(value, str):
        try:
            parsed = _parse_ts(value)
        except ValueError:
            errors.append(f"{path}: {value!r} is not a valid RFC-3339 date-time")
        else:
            # fromisoformat accepts date-only / tz-naive strings on 3.11+; require a full
            # date + time + offset so a value like "2026-06-01" is rejected.
            if "T" not in value or parsed.tzinfo is None:
                errors.append(f"{path}: {value!r} is not a full date-time with offset")
    if _has_type(schema, "array") and isinstance(value, list):
        item_schema = schema.get("items")
        if item_schema:
            for idx, item in enumerate(value):
                _validate(item, item_schema, f"{path}[{idx}]", errors)
    if _has_type(schema, "object") and isinstance(value, dict):
        props = schema.get("properties", {})
        for req in schema.get("required", []):
            if req not in value:
                errors.append(f"{path}: missing required field '{req}'")
        # additionalProperties: false -> a typo'd/extra key (e.g. 'reworkk') is an error,
        # not silently ignored (keeps the schema the source of truth).
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    errors.append(f"{path}: unknown field '{key}' (not in schema)")
        for key, subschema in props.items():
            if key in value:
                _validate(value[key], subschema, f"{path}.{key}" if path else key, errors)


def _validate_severity_pair(record: dict, errors: list) -> None:
    """Cross-field checks for the optional roborev severity split (issue #2088).

    Not expressible in the minimal JSON-Schema subset above (no if/then, no cross-field
    arithmetic) so it lives here as a targeted extra pass. Rules:
      - roborev_blockers and roborev_nits must appear together (both or neither) — a lone
        counter is a user/data error, never silently defaulted (authoritative-only).
      - when both are present alongside roborev_findings, blockers + nits MUST equal
        roborev_findings — the severity split must reconcile with the raw count.
    Guarded with isinstance checks so a wrong-typed field (already flagged by the generic
    walk above) doesn't also raise a confusing TypeError here.
    """
    has_blockers = "roborev_blockers" in record
    has_nits = "roborev_nits" in record
    if has_blockers != has_nits:
        present, missing = ("roborev_blockers", "roborev_nits") if has_blockers else ("roborev_nits", "roborev_blockers")
        errors.append(f": '{present}' is present without '{missing}' — the roborev "
                      f"severity split must be recorded together (both or neither)")
        return
    if not (has_blockers and has_nits):
        return
    blockers, nits = record["roborev_blockers"], record["roborev_nits"]
    findings = record.get("roborev_findings")
    ints_ok = all(isinstance(v, int) and not isinstance(v, bool) for v in (blockers, nits, findings))
    if ints_ok and blockers + nits != findings:
        errors.append(f": roborev_blockers ({blockers}) + roborev_nits ({nits}) = "
                      f"{blockers + nits}, but must equal roborev_findings ({findings})")


def _validate_gate_coupling(record: dict, errors: list) -> None:
    """Cross-field check for gate <-> gate_runs coherence (issue #3448).

    `gate: "not-run"` means NO full gate of record ran, so the only coherent run count is
    0 — and 0 runs cannot have produced a pass/fail outcome. The coupling therefore holds
    in BOTH directions:

        gate == "not-run"  <==>  gate_runs == 0

    Not expressible in the minimal JSON-Schema subset above (no if/then, no
    dependentSchemas), so it lives here alongside _validate_severity_pair. Routing it
    through validate_record means `lint` and `retro` enforce it over the WHOLE ledger, not
    just freshly-recorded lines — an incoherent hand-edited record is caught too.

    Guarded with isinstance checks so a wrong-typed/absent field (already flagged by the
    generic walk above) doesn't also raise a confusing TypeError here.
    """
    gate = record.get("gate")
    runs = record.get("gate_runs")
    if not isinstance(gate, str) or not isinstance(runs, int) or isinstance(runs, bool):
        return  # type/required errors already reported by the generic walk
    if gate == GATE_NOT_RUN and runs != 0:
        errors.append(f": gate '{GATE_NOT_RUN}' requires gate_runs 0 (no full gate of record "
                      f"ran), got {runs}")
    elif gate != GATE_NOT_RUN and runs == 0:
        errors.append(f": gate_runs 0 requires gate '{GATE_NOT_RUN}' (zero runs produced no "
                      f"outcome), got {gate!r}")


def _validate_cycle_time_basis(record: dict, errors: list) -> None:
    """Cross-field check for cycle_time_s's BASIS (issue #3550).

    `closed_at` is the record's kind (total and derivable, so a definition rather than an
    inference), and the kind selects the terminal timestamp cycle_time_s is measured to:

        closed_at non-null (COMPLETED issue) -> cycle_time_s == closed_at - created_at
        closed_at null      (SLICE delivery) -> cycle_time_s == merged_at - created_at

    A slice's terminal timestamp is the PR's mergedAt: the issue deliberately stays open, so
    its closedAt is not the cycle boundary at all. Arithmetic is outside the minimal
    JSON-Schema subset above (and outside standard JSON Schema), so it lives here beside
    _validate_severity_pair / _validate_gate_coupling. Routing it through validate_record
    means `lint` and `retro` enforce it over the WHOLE ledger, not just freshly-recorded
    lines — so a record whose kind and arithmetic tell two different stories is caught.

    Guarded with isinstance checks and a try/except around parsing so a wrong-typed or
    malformed timestamp (already flagged by the generic walk) doesn't also raise a confusing
    traceback here.
    """
    created = record.get("created_at")
    closed = record.get("closed_at")
    merged = record.get("merged_at")
    cycle = record.get("cycle_time_s")
    if not isinstance(created, str) or not isinstance(cycle, int) or isinstance(cycle, bool):
        return  # type/required errors already reported by the generic walk
    if closed is None:
        basis_name, basis = "merged_at (SLICE delivery: closed_at is null)", merged
    else:
        basis_name, basis = "closed_at (completed-issue delivery)", closed
    if not isinstance(basis, str):
        return  # the basis timestamp itself is missing/wrong-typed; already reported
    try:
        expected = _seconds_between(created, basis)
    except (ValueError, TypeError):
        # malformed timestamp already reported by the format check above. TypeError too:
        # `fromisoformat` happily parses a date-only / tz-naive string, and subtracting a
        # naive from an aware datetime raises TypeError rather than ValueError.
        return
    if cycle != expected:
        errors.append(f": cycle_time_s {cycle} != {expected} = {basis_name} - created_at "
                      f"(issue #3550: the basis is selected by closed_at)")


def validate_record(record: dict, schema: dict) -> list:
    """Return a list of human-readable validation errors (empty == valid)."""
    errors: list = []
    _validate(record, schema, "", errors)
    if isinstance(record, dict):
        _validate_severity_pair(record, errors)
        _validate_gate_coupling(record, errors)
        _validate_cycle_time_basis(record, errors)
    return errors


def load_schema(schema_path: Path) -> dict:
    return json.loads(schema_path.read_text())


# ============================================================ timestamp helpers

def _parse_ts(value: str) -> datetime:
    """Parse an RFC-3339 / ISO-8601 UTC timestamp (accepts a trailing 'Z').

    We normalize a trailing 'Z' to '+00:00' ourselves, so `datetime.fromisoformat`
    handles our timestamps on Python 3.9+ (it accepts numeric offsets there; only the
    bare 'Z' shorthand needed pre-normalizing before 3.11). The format validator
    additionally requires a 'T' separator + an offset, so date-only / tz-naive strings
    are rejected regardless of interpreter version.

    By design this enforces the uppercase GitHub canonical form (trailing 'Z', 'T'
    separator). Lowercase RFC-3339 ('...t...z') is intentionally NOT accepted — every
    timestamp in this ledger originates from `gh`, which always emits the canonical form.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _require_full_timestamp(value: str, field: str) -> None:
    """Raise SystemExit unless `value` is a FULL, tz-aware RFC-3339 date-time (issue #3550).

    `_parse_ts` deliberately accepts what `datetime.fromisoformat` accepts, which includes a
    date-only ("2026-06-01") and a tz-NAIVE ("2026-06-01T00:00:00") string. Both parse fine
    and then blow up later: subtracting a naive datetime from an aware one raises TypeError
    out of `_seconds_between`, i.e. a bare traceback from arithmetic instead of a named bad
    invocation. Parseability is therefore NOT the property worth asserting — being a full
    instant is. Mirrors the `format: date-time` check in `_validate`, so the CLI refuses at
    input what the schema would reject at output.
    """
    # The TYPE first: _parse_ts calls .endswith() on its argument, so a truthy non-string
    # (a number, a list, a dict from the --from-json seam) raises AttributeError — which is
    # neither of the exceptions below and would escape as a traceback, the exact outcome this
    # helper exists to prevent. A falsy non-string is already refused by the caller's
    # presence check; this closes the truthy half.
    if not isinstance(value, str):
        raise SystemExit(f"error: authoritative field '{field}' must be an RFC-3339 timestamp "
                         f"string, got {value!r} ({type(value).__name__})")
    try:
        parsed = _parse_ts(value)
    except (ValueError, TypeError) as exc:
        raise SystemExit(f"error: authoritative field '{field}' is not a parseable RFC-3339 "
                         f"timestamp: {value!r} ({exc})")
    if "T" not in value or parsed.tzinfo is None:
        raise SystemExit(f"error: authoritative field '{field}' is not a full date-time with "
                         f"a UTC offset: {value!r} (a date-only or timezone-naive value "
                         f"parses but cannot be differenced against one that has an offset)")


# \Z, not $: Python's `$` also matches immediately BEFORE a trailing newline, so a value
# ending "\n" would be accepted here and then compared raw — the lenient-reader/strict-
# consumer split that fails OPEN. \Z is the true end of string.
_ISSUE_URL_RE = re.compile(r"^https://github\.com/([^/\s]+)/([^/\s]+)/issues/(\d+)\Z")


def _issue_identity(value):
    """Return a canonical ``(owner, repo, number)`` identity, or None if unrecognised.

    Deliberately does NOT strip: a value with surrounding whitespace is UNRECOGNISED, not
    normalised. Normalising here is the defect that recurred throughout issue #3550 — a
    lenient reader accepts a value, a strict consumer then fails to match it, and a
    non-match is indistinguishable from a correct non-match, so the guard fails OPEN.

    Identity is the full triple, never the number alone: issue numbers are repository-scoped,
    so `other/repo#3393` and `pmcfadin/cqlite#3393` are different issues that share a number.
    """
    if not isinstance(value, str):
        return None
    m = _ISSUE_URL_RE.match(value)
    return (m.group(1), m.group(2), int(m.group(3))) if m else None


def _assert_never_closed(gh_fields: dict, issue: int) -> None:
    """Raise SystemExit unless the issue has provably NEVER been closed (issue #3550).

    A null `closed_at` proves the issue is open NOW; only a never-closed issue proves it was
    open WHEN THE PR MERGED, which is what a slice record asserts. A REOPENED issue also has
    `closed_at: null`, so without this an ordinary COMPLETED delivery stamped after a reopen
    would be recorded as a slice — the one direction of this classification that produces a
    WRONG record rather than a refusal. GitHub's `stateReason` separates the two
    affirmatively: empty (gh) / null (REST) when never closed, "REOPENED" when reopened.

    Every non-affirmative state is a refusal, never a shrug: an ABSENT key is UNMEASURED (not
    "never closed"), a non-string value is malformed, and an unrecognised value is
    unattributable. #3559's timeline replay is the general answer for all of them, and each
    message names it. Called for ANY open issue, before the --slice coupling below, so a
    reopened issue is routed straight here rather than told to pass a flag that would then be
    refused.
    """
    if "state_reason" not in gh_fields:
        raise SystemExit(
            "error: the issue's stateReason is required to tell a never-closed issue from a "
            "REOPENED one (both have closed_at null) and it was not supplied — an unmeasured "
            "signal is never read as 'never closed' (issue #3550)")
    raw = gh_fields["state_reason"]
    # Only None or a str is a measurement. `(raw or "")` would fold False/0/[] onto the
    # never-closed answer — the truthiness shape this issue keeps re-finding.
    if raw is not None and not isinstance(raw, str):
        raise SystemExit(
            f"error: the issue's stateReason must be null or a string, got {raw!r} — a falsy "
            f"non-string is malformed input, never an affirmative 'never closed' "
            f"(issue #3550)")
    # Matched EXACTLY against the measured values, with no normalisation of the never-closed
    # answer. `.strip()` folded a whitespace-only string onto "" — i.e. a malformed value took
    # the AFFIRMATIVE branch, the fifth instance of this file's recurring shape. gh emits ""
    # and REST emits null for never-closed; neither ever emits whitespace, so a blank-but-
    # non-empty string is unmeasured input and takes the same refusal as any other.
    if raw is None or raw == "":
        return  # affirmatively never closed
    state_reason = raw.strip().upper()
    if state_reason == "REOPENED":
        raise SystemExit(
            f"error: issue #{issue} is open only because it was REOPENED, so its null "
            f"closed_at does not show it was open when the PR merged — recording this as a "
            f"slice would mislabel an ordinary completed delivery. Deciding it needs the "
            f"issue timeline replayed to mergedAt: issue #3559 — do NOT hand-append the "
            f"record (issue #3550).")
    raise SystemExit(
        f"error: cannot classify issue #{issue}: its stateReason is {raw!r}, which is neither "
        f"the empty value of a never-closed issue nor 'REOPENED' — refusing rather than "
        f"guessing (issue #3559 replays the timeline for the cases this cannot decide).")


def _seconds_between(start: str, end: str) -> int:
    return int((_parse_ts(end) - _parse_ts(start)).total_seconds())


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================ record

def _gh(argv: list) -> str:
    """Run a `gh` command and return stdout; a gh failure is a clean SystemExit.

    Keeps the live network-touching paths consistent with the tool's deliberate
    SystemExit error style instead of surfacing a raw traceback.
    """
    try:
        return subprocess.run(argv, check=True, capture_output=True, text=True).stdout
    except FileNotFoundError:
        raise SystemExit("error: `gh` not found on PATH")
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip() or str(exc)
        raise SystemExit(f"error: `{' '.join(argv)}` failed: {detail}")


def _github_fields(issue: int, pr: int) -> dict:
    """Pull authoritative timestamps/labels live from `gh` (only when not injected)."""
    issue_json = json.loads(_gh(
        ["gh", "issue", "view", str(issue), "--json",
         "createdAt,closedAt,labels,stateReason,url"]))
    pr_json = json.loads(_gh(
        ["gh", "pr", "view", str(pr), "--json", "createdAt,mergedAt,closingIssuesReferences"]))
    if "closingIssuesReferences" not in pr_json:
        raise SystemExit(
            f"error: `gh pr view {pr}` returned no closingIssuesReferences field — refusing "
            f"rather than treating an unmeasured field as 'closes nothing' (issue #3550)")
    # The SHAPE too, not just presence. A null/dict value would fold onto "closes nothing"
    # (or traceback), and a malformed ELEMENT silently FILTERED out reads the same way — a
    # partially-unreadable reply must refuse, not quietly shrink to the permissive answer.
    refs = pr_json["closingIssuesReferences"]
    if not isinstance(refs, list):
        raise SystemExit(
            f"error: `gh pr view {pr}` returned closingIssuesReferences of type "
            f"{type(refs).__name__}, expected a list — refusing rather than reading an "
            f"unparseable reply as 'closes nothing' (issue #3550)")
    closing_identities = []
    for ref in refs:
        # Identity by (owner, repo, number), never by number: issue numbers are
        # repository-scoped, so a PR closing other-repo#N must not read as closing this
        # repo's #N. Refuse an unparseable entry rather than discard it — discarding shrinks
        # the set toward "closes nothing", the permissive answer.
        identity = _issue_identity(ref.get("url")) if isinstance(ref, dict) else None
        if identity is None:
            raise SystemExit(
                f"error: `gh pr view {pr}` returned a closingIssuesReferences entry whose "
                f"'url' is not a canonical GitHub issue URL: {ref!r} — refusing rather than "
                f"discarding it, which would read as 'closes nothing' (issue #3550)")
        closing_identities.append(identity)
    # Every field we asked for must have come back. `stateReason` in particular is the sole
    # never-closed-vs-REOPENED signal, so its absence must be a named refusal here rather
    # than a None that reads downstream as "never closed" (issue #3550).
    missing = [f for f in ("createdAt", "closedAt", "labels", "stateReason", "url")
               if f not in issue_json]
    if missing:
        raise SystemExit(
            f"error: `gh issue view {issue}` returned no {', '.join(missing)} field(s) — "
            f"refusing rather than treating an unmeasured field as a value (issue #3550)")
    if _issue_identity(issue_json["url"]) is None:
        raise SystemExit(
            f"error: `gh issue view {issue}` returned a 'url' that is not a canonical GitHub "
            f"issue URL: {issue_json['url']!r} — refusing rather than comparing an "
            f"unrecognised identity, which could never match (issue #3550)")
    labels = [l.get("name") for l in issue_json.get("labels", []) if l.get("name")]
    prio_labels = [l for l in labels if re.fullmatch(r"P[0-3]", l)]
    # one-priority invariant: a multi-priority issue is a labeling error — surface it
    # rather than silently picking the first (authoritative-data-only).
    if len(prio_labels) > 1:
        raise SystemExit(f"error: issue #{issue} has multiple priority labels "
                         f"{prio_labels} (exactly one P0-P3 expected)")
    priority = prio_labels[0] if prio_labels else None
    # Routing is authoritative, not inferred: set it ONLY from an explicit label. Both
    # labels at once is a labeling error (mirror the one-priority guard); neither leaves it
    # None so build_record requires --routing rather than silently guessing "design".
    has_oracle, has_design = "oracle" in labels, "design" in labels
    if has_oracle and has_design:
        raise SystemExit(f"error: issue #{issue} has both 'oracle' and 'design' routing "
                         f"labels (exactly one expected)")
    routing = "oracle" if has_oracle else ("design" if has_design else None)
    return {
        # The DELIVERY CYCLE this payload was built for — the same (issue, pr) pair that is
        # the ledger's own identity for a record. build_record refuses if either half
        # disagrees with the CLI, so a stale or copied --from-json file cannot be applied to a
        # different delivery (issue #3550). Binding the issue ALONE was insufficient: this
        # payload also carries PR-specific data (pr_opened_at, merged_at,
        # pr_closes_this_issue), so a payload reused across two PRs of the SAME issue — the
        # #3393 shape exactly, three PRs on one open issue — passed and recorded the new pr
        # with the old pr's timestamps and classification.
        #
        # Each half is checked against a CLI FLAG, never against another injected field: two
        # injected values that must agree with EACH OTHER is the shape that regenerated this
        # defect six times, because a non-match and a correct non-match are the same
        # observation. A file-vs-CLI check has no such symmetry.
        "issue": issue,
        "pr": pr,
        "created_at": issue_json["createdAt"],
        "closed_at": issue_json["closedAt"],
        # GitHub's own reason the issue is in its current state. For an OPEN issue this is
        # empty when it has NEVER been closed and "REOPENED" when it has — the one cheap
        # affirmative signal distinguishing the two, and `closed_at: null` alone cannot
        # (see build_record's --slice guard, issue #3550). Indexed, NOT `.get`: a `.get`
        # would map an ABSENT field (a gh/API change, an older gh) to None, which
        # _assert_never_closed reads as affirmative proof the issue was never closed — an
        # unmeasured signal silently inheriting the permissive answer, which is the whole
        # defect that guard exists to prevent. The presence check above makes it a clean
        # refusal instead.
        "state_reason": issue_json["stateReason"],
        "pr_opened_at": pr_json["createdAt"],
        "merged_at": pr_json["mergedAt"],
        # The issues THIS PR declares it closes ("Closes #N"). A SLICE pr by definition
        # closes nothing — the issue deliberately stays open — so this discriminates a real
        # slice from an ordinary completed delivery whose auto-close has not yet PROPAGATED
        # (issue #3550). Measured, not timed: during that window closed_at is null AND
        # stateReason is empty, so both of the other signals look exactly like a never-closed
        # issue and only this one tells the truth.
        # ONE BOOLEAN, not two operands. Both sides are derived here from the SAME two
        # authoritative queries, so they cannot disagree. Passing the URLs through the
        # --from-json seam instead put two values that MUST AGREE in an operator's hands,
        # and every mismatch failed OPEN (a non-match silently disables the guard rather
        # than failing it) — six successive defects on that seam before this collapse.
        "pr_closes_this_issue": _issue_identity(issue_json["url"]) in closing_identities,
        "priority": priority,
        "routing": routing,
    }


def build_record(args, gh_fields: dict) -> dict:
    """Assemble a record from supplied counters + authoritative GitHub fields.

    Error convention: caller-input / precondition errors (a missing counter, a null
    timestamp, an undeterminable priority/routing) `raise SystemExit` here — they are bad
    invocations, like an argparse failure. Ledger-state outcomes (a built record that
    fails schema validation, a duplicate (issue, pr) cycle) are reported by `cmd_record` with a stderr
    message + `return 1`. Both surface as a non-zero exit.
    """
    for counter in REQUIRED_COUNTERS:
        if getattr(args, counter) is None:
            raise SystemExit(
                f"error: --{counter.replace('_', '-')} is required "
                f"(authoritative-data-only: a counter that was not observed is never defaulted)")
    if args.gate is None:
        raise SystemExit("error: --gate {pass|fail|not-run} is required")
    # gate <-> gate_runs coupling (issue #3448), refused here as a bad invocation so the CLI
    # names the offending flags rather than reporting an opaque built-record schema error.
    # This adds a legal VALUE, never a default: --gate/--gate-runs remain required above.
    if args.gate == GATE_NOT_RUN and args.gate_runs != 0:
        raise SystemExit(
            f"error: --gate {GATE_NOT_RUN} means no full gate of record ran, so --gate-runs "
            f"must be 0 (got {args.gate_runs})")
    if args.gate != GATE_NOT_RUN and args.gate_runs == 0:
        raise SystemExit(
            f"error: --gate-runs 0 means no full gate of record ran, so --gate must be "
            f"'{GATE_NOT_RUN}' (got '{args.gate}') — zero runs produced no pass/fail outcome")

    # Optional roborev severity split (issue #2088). Authoritative-only: supply BOTH
    # --roborev-blockers/--roborev-nits or NEITHER — a lone counter is a user error, never
    # silently completed with a fabricated 0. When both are given they must reconcile with
    # --roborev-findings (the split cannot silently drift from the raw count).
    has_severity = args.roborev_blockers is not None or args.roborev_nits is not None
    if has_severity and (args.roborev_blockers is None or args.roborev_nits is None):
        raise SystemExit(
            "error: --roborev-blockers and --roborev-nits must both be supplied together "
            "(authoritative-only: a partial severity split is never inferred) — see "
            "docs/development/roborev-severity.md")
    if has_severity and args.roborev_blockers + args.roborev_nits != args.roborev_findings:
        raise SystemExit(
            f"error: --roborev-blockers ({args.roborev_blockers}) + --roborev-nits "
            f"({args.roborev_nits}) = {args.roborev_blockers + args.roborev_nits}, but must "
            f"equal --roborev-findings ({args.roborev_findings})")

    # Authoritative timestamps must be present — a finalize runs only on a MERGED pr. A
    # null here (e.g. an unmerged PR) is an error, not silent arithmetic on None (which
    # would otherwise raise an opaque AttributeError). `--slice` relaxes NOTHING here: only
    # the issue's closed_at may be null, and only under the explicit opt-in below.
    slice_delivery = bool(args.slice_delivery)
    for key in ("created_at", "pr_opened_at", "merged_at"):
        if not gh_fields.get(key):
            raise SystemExit(
                f"error: authoritative timestamp '{key}' is missing/null "
                f"(a record needs the issue's createdAt and the PR's createdAt/mergedAt — "
                f"finalize records only a MERGED pr)")

    # THE PAYLOAD IS BOUND TO --issue, before any field is read from it (issue #3550). The
    # --from-json seam is how a caller injects GitHub-derived fields, and a STALE or COPIED
    # file built for a different issue would otherwise be applied wholesale: a wrong
    # closed_at/created_at corrupts the kind and the cycle time, and a wrong
    # pr_closes_this_issue disables the auto-close-window guard. One scalar checked against
    # one flag protects every field at once — and deliberately NOT by re-introducing two
    # injected operands that must agree with each other, which is the shape that produced
    # six consecutive defects here.
    for field, flag, expected in (("issue", "--issue", args.issue), ("pr", "--pr", args.pr)):
        if field not in gh_fields:
            raise SystemExit(
                f"error: the injected payload has no '{field}' field naming the delivery it "
                f"was built for, so it cannot be bound to {flag} — an unbound payload could "
                f"be a stale or copied file for a different delivery (issue #3550)")
        got = gh_fields[field]
        if not isinstance(got, int) or isinstance(got, bool):
            raise SystemExit(
                f"error: the injected payload's '{field}' must be an integer, got {got!r} "
                f"({type(got).__name__}) (issue #3550)")
        if got != expected:
            raise SystemExit(
                f"error: the injected payload was built for {field} #{got} but {flag} is "
                f"{expected} — refusing to apply one delivery's authoritative data to "
                f"another. The payload carries BOTH issue-specific and PR-specific fields "
                f"(timestamps, pr_closes_this_issue), so reusing it across PRs of the same "
                f"issue records the new pr with the old pr's data. Re-derive the payload for "
                f"({args.issue}, {args.pr}) (issue #3550)")

    # `closed_at` is read AFFIRMATIVELY, never by truthiness (issue #3550). The `--from-json`
    # seam can inject anything, and a falsy-but-not-null value ("" / 0 / false) or an ABSENT
    # key would otherwise be silently normalized to the slice marker — a permissive branch
    # keyed on the absence of a bad signal, which is the exact shape CLAUDE.md forbids. So:
    # the key MUST be present, and its value MUST be either None or a timestamp string.
    if "closed_at" not in gh_fields:
        raise SystemExit(
            "error: authoritative field 'closed_at' is absent (the key is required: its "
            "VALUE — null or a timestamp — carries the record's kind, so an absent key is "
            "unmeasured, never a slice; issue #3550)")
    closed_at = gh_fields["closed_at"]
    if closed_at is not None and not isinstance(closed_at, str):
        raise SystemExit(
            f"error: authoritative field 'closed_at' must be null or an RFC-3339 timestamp "
            f"string, got {closed_at!r} (issue #3550)")
    if isinstance(closed_at, str) and not closed_at.strip():
        raise SystemExit(
            "error: authoritative field 'closed_at' is an empty string — null is the only "
            "spelling of 'this issue is not closed' (issue #3550)")
    # Every timestamp that reaches the arithmetic must be a FULL instant, checked here rather
    # than only for closed_at: a date-only or tz-naive value from the --from-json seam parses
    # and then raises TypeError out of _seconds_between, whichever field carried it.
    for key in ("created_at", "pr_opened_at", "merged_at"):
        _require_full_timestamp(gh_fields[key], key)
    if isinstance(closed_at, str):
        _require_full_timestamp(closed_at, "closed_at")

    # closed_at <-> --slice, coupled in BOTH directions (issue #3550), refused here as a bad
    # invocation so the CLI names the offending flag rather than reporting an opaque
    # built-record schema/basis error. Mirrors the gate coupling above.
    #
    # `--slice` asserts a fact about DELIVERY time — "when this PR merged, the issue was
    # deliberately open" — and CURRENT state cannot decide it, so this refuses rather than
    # guesses. A TIMESTAMP COMPARISON WAS TRIED AND IS WRONG (issue #3559): an auto-closing
    # PR merges BEFORE GitHub records the closure, so `closedAt > mergedAt` is the NORMAL
    # ordering of an ordinary COMPLETED delivery, not the signature of a late-stamped slice —
    # a `closedAt <= mergedAt` guard would therefore permit --slice on essentially every
    # ordinary delivery while looking like a check. Per CLAUDE.md's #3229 lesson (a guard
    # with known false-PASSes is worse than no guard, because it invites reliance it cannot
    # support) it was REMOVED rather than weakened further. Deciding this properly needs the
    # issue TIMELINE replayed to mergedAt, which is issue #3559.
    #
    # Cost, stated rather than hidden: a genuine slice cannot be stamped once its issue is
    # ever closed. That is fail-closed — an unrecorded delivery is recoverable, a ledger that
    # silently reclassifies a completed delivery as a slice is not.
    if closed_at is None:
        # FIRST: is the issue open, or merely not-yet-recorded-as-closed? GitHub records an
        # auto-close AFTER the merge, and inside that propagation window an ordinary COMPLETED
        # delivery presents EXACTLY as a never-closed issue — closed_at null AND stateReason
        # empty — so both other signals lie and the operator would be told to pass --slice,
        # recording a false slice. This PR's own closing declaration is the one signal that
        # does not lie: a slice pr closes NOTHING, because its issue deliberately stays open.
        # Checked regardless of --slice, and it is NOT a timing heuristic — no clock, no
        # threshold, just an authoritative field already on the PR query.
        # ONE MEASUREMENT, affirmatively required. Everything this guard needs is "did this
        # PR declare it closes this issue" — a single fact both sides of which are derived in
        # _github_fields from the same two authoritative queries. It was previously TWO
        # operands (issue_url + closes_issues) carried across the --from-json seam, where an
        # operator could make them disagree; every mismatch failed OPEN, because a non-match
        # and a correct non-match are the same observation. Six consecutive defects on that
        # seam (closed_at truthiness, stateReason x3, closes_issues value, issue_url binding)
        # were all that one shape, so the operands were removed rather than validated a
        # seventh time. A bool cannot half-agree with itself.
        if "pr_closes_this_issue" not in gh_fields:
            raise SystemExit(
                "error: 'pr_closes_this_issue' is required to tell a slice from a completed "
                "delivery whose auto-close has not propagated yet (both show a null "
                "closed_at) — an unmeasured signal is never read as 'closes nothing' "
                "(issue #3550)")
        closes_this = gh_fields["pr_closes_this_issue"]
        if not isinstance(closes_this, bool):
            raise SystemExit(
                f"error: 'pr_closes_this_issue' must be a boolean, got {closes_this!r} "
                f"({type(closes_this).__name__}) — a truthy/falsy stand-in is unmeasured "
                f"input, never an affirmative answer (issue #3550)")
        if closes_this:
            raise SystemExit(
                f"error: PR #{args.pr} declares it CLOSES issue #{args.issue}, so this is a "
                f"completed delivery, not a slice — its closed_at is null only because "
                f"GitHub has not recorded the auto-close yet (it lands after the merge). "
                f"Re-run once the close is recorded and stamp it WITHOUT --slice; passing "
                f"--slice here would file a false slice record (issue #3550).")
        # The issue is genuinely open. Whether it was open AT DELIVERY is a property of the
        # issue, not of the flag, so it is decided BEFORE the coupling below — otherwise a
        # reopened issue is told to pass --slice and the next invocation refuses it, bouncing
        # the operator between two refusals.
        _assert_never_closed(gh_fields, args.issue)
        if not slice_delivery:
            raise SystemExit(
                f"error: issue #{args.issue} is still OPEN (its closed_at is null), so this "
                f"is a SLICE delivery: a merged PR shipping part of an issue that "
                f"deliberately stays open. Pass --slice to record it — the PR's mergedAt then "
                f"bounds cycle_time_s and closed_at is recorded as null (issue #3550).\n"
                f"       Do NOT route around this refusal. Both available workarounds are "
                f"FORBIDDEN: (1) closing the issue to satisfy this tool — a tool's data model "
                f"must never decide whether a problem is recorded as solved; (2) "
                f"hand-appending a record to the JSONL past the validator — the tool is the "
                f"gate on the ledger's shape.")
    elif slice_delivery:
        raise SystemExit(
            f"error: --slice records a delivery of an issue that was deliberately OPEN when "
            f"the PR merged, but issue #{args.issue} is CLOSED now (closed_at {closed_at}) "
            f"and current state cannot tell a late-stamped slice from an ordinary completed "
            f"delivery — GitHub records an auto-close AFTER the merge, so the timestamps look "
            f"alike. If this WAS a completed delivery, drop --slice. If it was genuinely a "
            f"slice stamped after the issue closed, that needs the issue timeline replayed to "
            f"mergedAt: issue #3559 — do NOT hand-append the record (issue #3550).")

    created = gh_fields["created_at"]
    pr_opened = gh_fields["pr_opened_at"]
    merged = gh_fields["merged_at"]
    # null IS the slice marker. `closed_at` was affirmatively validated above (present, and
    # either None or a non-blank string), so this is a kind selection, not a normalization of
    # something falsy: a slice records null even when the issue has since closed, because the
    # record states what was true at delivery time.
    closed = None if slice_delivery else closed_at
    # cycle_time_s's basis is selected by the record's kind — see _validate_cycle_time_basis,
    # which cross-checks this arithmetic over the whole ledger.
    cycle_basis = merged if slice_delivery else closed
    priority = args.priority or gh_fields.get("priority")
    routing = args.routing or gh_fields.get("routing")
    if priority is None:
        raise SystemExit("error: priority not found on the issue and not supplied via --priority")
    if routing is None:
        raise SystemExit("error: routing not determinable from labels and not supplied via "
                         "--routing {design|oracle} (authoritative-data-only: never inferred)")

    record = {
        "schema": 1,
        "issue": args.issue,
        "slug": args.slug,
        "pr": args.pr,
        "routing": routing,
        "priority": priority,
        "created_at": created,
        "pr_opened_at": pr_opened,
        "merged_at": merged,
        "closed_at": closed,
        "cycle_time_s": _seconds_between(created, cycle_basis),
        "phase_s": {
            "to_pr_s": _seconds_between(created, pr_opened),
            "review_s": _seconds_between(pr_opened, merged),
        },
        "claim_collisions": args.claim_collisions,
        "rebase_events": args.rebase_events,
        "gate": args.gate,
        "gate_runs": args.gate_runs,
        "roborev_findings": args.roborev_findings,
        "rework": args.rework,
        "stamped_at": _now_iso(),
    }
    if has_severity:
        record["roborev_blockers"] = args.roborev_blockers
        record["roborev_nits"] = args.roborev_nits
    # Optional stall-observability counters (issue #2667). Authoritative-only: each is
    # written ONLY when observed (supplied), never defaulted to 0 — an omitted field means
    # "not observed", not "zero", so a Mode-3 incident that WAS measured stays visible in
    # the ledger while an unmeasured cycle does not fabricate a clean signal.
    if args.nudges is not None:
        record["nudges"] = args.nudges
    if args.orphan_minutes is not None:
        record["orphan_minutes"] = args.orphan_minutes
    return record


def cmd_record(args) -> int:
    schema = load_schema(Path(args.schema))
    if args.from_json:
        gh_fields = json.loads(Path(args.from_json).read_text())
    else:
        gh_fields = _github_fields(args.issue, args.pr)

    record = build_record(args, gh_fields)
    errors = validate_record(record, schema)
    if errors:
        print("error: built record is not schema-valid:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    ledger = Path(args.ledger)
    # Idempotency: one record per delivery cycle (issue, pr). A re-run / double finalize of
    # the SAME cycle must not append a second record (which would double-count it in retro).
    # Refuse a same-(issue, pr) re-stamp unless --allow-duplicate is given. A reopened issue
    # that ships again under a NEW pr is a legitimate new cycle and appends freely. This
    # check is best-effort, not atomic — the real cross-session serializer is the per-issue
    # branch lock (one worktree → one finalize); `lint` is the after-the-fact backstop.
    if ledger.exists():
        for lineno, raw in enumerate(ledger.read_text().splitlines(), start=1):
            if not raw.strip():
                continue
            try:
                existing = json.loads(raw)
            except json.JSONDecodeError:
                # surface ledger corruption at write time rather than only at next lint
                print(f"warning: existing ledger line {lineno} is unparseable — run `lint`",
                      file=sys.stderr)
                continue
            if (isinstance(existing, dict)
                    and existing.get("issue") == record["issue"]
                    and existing.get("pr") == record["pr"]
                    and not args.allow_duplicate):
                print(f"error: issue #{record['issue']} / pr #{record['pr']} already has a "
                      f"ledger record (pass --allow-duplicate to override)", file=sys.stderr)
                return 1

    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
    print(f"recorded issue #{record['issue']} (pr #{record['pr']}) -> {ledger}")
    return 0


# ============================================================ lint

def load_ledger(ledger: Path, schema: dict):
    """Parse + validate every ledger line. Returns (records, errors) where errors is a
    list of (lineno, message): malformed JSON, schema violations, AND duplicate delivery
    cycles (one record per (issue, pr)). Shared by `lint` and `retro` so both apply the
    exact same rules — neither can rank/accept a ledger the other would reject.

    The dup key is the (issue, pr) pair, not the issue alone: a REOPENED issue that ships
    more than once legitimately has one record per shipped PR (e.g. #2264 via PR #2282 then
    PR #2301). Only a genuine same-(issue, pr) re-stamp is a duplicate. Retro consumers
    that aggregate by issue should treat such multi-cycle issues as multiple deliveries
    (they are), not fold them into one.
    """
    records, errors = [], []
    seen_cycles: dict = {}
    for lineno, raw in enumerate(ledger.read_text().splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            errors.append((lineno, f"invalid JSON: {exc}"))
            continue
        for e in validate_record(record, schema):
            errors.append((lineno, e))
        # one record per delivery cycle (issue, pr) — a duplicate cycle skews retro. A
        # non-object line already produced a type error above; skip the bookkeeping. A
        # record missing 'pr' already failed schema validation above; key on (issue, None)
        # so we still don't crash and still flag an exact repeat of that malformed line.
        if isinstance(record, dict) and record.get("issue") is not None:
            key = (record.get("issue"), record.get("pr"))
            if key in seen_cycles:
                issue, pr = key
                errors.append((lineno, f"duplicate record for issue #{issue} / pr #{pr} "
                                       f"(first seen line {seen_cycles[key]})"))
            else:
                seen_cycles[key] = lineno
        records.append(record)
    return records, errors


def cmd_lint(args) -> int:
    ledger = Path(args.ledger)
    if not ledger.exists():
        print(f"error: ledger not found: {ledger}", file=sys.stderr)
        return 1
    _, errors = load_ledger(ledger, load_schema(Path(args.schema)))
    for lineno, msg in errors:
        print(f"line {lineno}: {msg}", file=sys.stderr)
    if errors:
        print(f"FAIL: {len(errors)} problem(s)", file=sys.stderr)
        return 1
    print("OK: ledger is well-formed")
    return 0


# ============================================================ retro

def aggregate(records: list) -> dict:
    """Sum each failure category across records (authoritative recorded values only).

    Precondition: every record is already schema-valid (every counter present). Callers
    MUST validate first — `cmd_retro` does. Fields are indexed directly (not `.get` with a
    default) so an unvalidated record raises rather than contributing a fabricated zero,
    consistent with the authoritative-data-only mandate.

    Severity-aware roborev weighting (issue #2088): when a record carries the optional
    roborev_blockers/roborev_nits split, the weighted `roborev_findings` tally counts
    BLOCKERS only (nits are cheap — batched into one follow-up, never a re-verify round —
    so weighing them the same as a blocker would overstate the pipeline's real cost). A
    record without severity data degrades gracefully to the prior behavior: the raw
    roborev_findings count. Nits are still totaled, but reported separately (see
    `roborev_nits_total` / `roborev_severity_records`) rather than folded into the weighted
    score, keeping `rank()`'s RETRO_WEIGHTS-keyed categories unchanged and deterministic.

    Slice deliveries (issue #3550): a record with `closed_at: null` is a SLICE — a merged PR
    shipping part of an issue that DELIBERATELY stays open. It is a real, separate delivery
    (so it is tallied like any other), but it is NOT a completed issue, so it is also counted
    in the informational `slice_records` extra and reported as its own class by `cmd_retro` —
    otherwise an issue shipping three slices would read as three completed deliveries (the
    #2314 multi-cycle problem in a new form). The count requires the key to be PRESENT and
    null (an affirmative measurement): an ABSENT closed_at is a schema violation the
    validated-records precondition rules out, never silently read as a slice.

    Ungated deliveries (issue #3448): a `gate: "not-run"` record contributes 0 failed rounds
    (zero rounds were observed) but is NOT silently indistinguishable from a clean one-run
    pass — it is counted in the informational `gate_not_run_records` extra and reported as
    its own class by `cmd_retro`, so an ungated delivery can never be read off the retro as
    a gated pass.
    """
    tally = {k: 0 for k in RETRO_WEIGHTS}
    nit_total = 0
    severity_records = 0
    not_run_records = 0
    slice_records = 0
    for r in records:
        if "closed_at" in r and r["closed_at"] is None:
            slice_records += 1
        tally["claim_collisions"] += r["claim_collisions"]
        tally["rebase_events"] += r["rebase_events"]
        tally["rework"] += r["rework"]
        has_severity = "roborev_blockers" in r and "roborev_nits" in r
        if has_severity:
            tally["roborev_findings"] += r["roborev_blockers"]
            nit_total += r["roborev_nits"]
            severity_records += 1
        else:
            tally["roborev_findings"] += r["roborev_findings"]
        # Failed gate ROUNDS. By the gate_runs contract (runs stop at the first PASS — see
        # the schema), every run but a terminal pass WAS a failed round, so this is exact,
        # not an inference: terminal pass -> gate_runs-1 failures; terminal fail -> all
        # gate_runs. A "not-run" record (issue #3448) has gate_runs == 0 by the validated
        # coupling and contributes 0 failed rounds — correct, because zero rounds were
        # observed, not because a failure was assumed away. max(0, ...) keeps the arithmetic
        # non-negative for any counted-but-unreachable combination.
        if r["gate"] == GATE_NOT_RUN:
            not_run_records += 1
        passed_final = 1 if r["gate"] == "pass" else 0
        tally["gate_failures"] += max(0, r["gate_runs"] - passed_final)
    # Informational extras, NOT part of the weighted categories: rank() below iterates
    # RETRO_WEIGHTS (not tally.items()) specifically so these extra keys never need a
    # matching weight and can never desync rank()'s output.
    tally["roborev_nits_total"] = nit_total
    tally["roborev_severity_records"] = severity_records
    tally["gate_not_run_records"] = not_run_records
    tally["slice_records"] = slice_records
    return tally


def rank(tally: dict) -> list:
    """Return [(category, count, weight, score)] sorted by score desc, then category.

    Iterates RETRO_WEIGHTS (not tally.items()): `aggregate()` may add informational extra
    keys (e.g. roborev_nits_total) that carry no weight and must never appear as a ranked
    category.
    """
    rows = [(cat, tally[cat], weight, tally[cat] * weight) for cat, weight in RETRO_WEIGHTS.items()]
    rows.sort(key=lambda r: (-r[3], r[0]))
    return rows


def _retro_marker(category: str) -> str:
    return f"<!-- RETRO:{category} -->"


_FLOW_META_LIMIT = 500


def _open_flow_meta_issues(args) -> list:
    if args.open_issues_json:
        return json.loads(Path(args.open_issues_json).read_text())
    issues = json.loads(_gh(
        ["gh", "issue", "list", "--label", "flow-meta", "--state", "open",
         "--json", "number,title,body", "--limit", str(_FLOW_META_LIMIT)]))
    # No silent cap: if the lookup hit the limit, dedupe may be incomplete — say so loudly.
    if len(issues) >= _FLOW_META_LIMIT:
        print(f"warning: open flow-meta lookup hit the {_FLOW_META_LIMIT}-issue cap — "
              f"dedupe may miss an existing tracker beyond it", file=sys.stderr)
    return issues


def cmd_retro(args) -> int:
    ledger = Path(args.ledger)
    if not ledger.exists():
        print(f"error: ledger not found: {ledger}", file=sys.stderr)
        return 1
    # Refuse to rank a non-conforming ledger: a malformed/partial line, a record missing a
    # counter, OR a duplicate (issue, pr) cycle (which would double-count in the tally) must be a clean
    # error (run `lint`), never silently parsed/defaulted/double-counted. Same loader as
    # `lint`, so retro applies the identical bar.
    records, errors = load_ledger(ledger, load_schema(Path(args.schema)))
    if errors:
        lineno, msg = errors[0]
        print(f"error: ledger problem at line {lineno} ({msg}) — run `lint`", file=sys.stderr)
        return 1
    if not records:
        print("ledger is empty — nothing to retro")
        return 0

    tally = aggregate(records)
    ranked = rank(tally)

    print(f"delivery-pipeline retro over {len(records)} record(s):")
    print(f"  {'category':<18} {'count':>6} {'weight':>7} {'score':>7}")
    for cat, cnt, weight, score in ranked:
        print(f"  {cat:<18} {cnt:>6} {weight:>7} {score:>7}")
    # roborev severity (issue #2088): reported separately, never folded into the weighted
    # score — see aggregate()'s docstring. Only printed when at least one record carries
    # the optional split (degrades silently to nothing for an all-legacy ledger).
    if tally["roborev_severity_records"]:
        print(f"  (roborev severity: {tally['roborev_severity_records']} record(s) classified; "
              f"blockers counted above, {tally['roborev_nits_total']} nit(s) excluded — see "
              f"docs/development/roborev-severity.md)")

    # ungated deliveries (issue #3448): reported as their own class so a "not-run" is never
    # read as a gated pass. Not a weighted category — no full gate ran, so no gate round
    # failed; the fact worth surfacing is that the certification is ABSENT, not failed.
    if tally["gate_not_run_records"]:
        print(f"  ({tally['gate_not_run_records']} of {len(records)} record(s): no full gate "
              f"of record ran — gate 'not-run', excluded from gate_failures above; these are "
              f"UNGATED deliveries, not gated passes — see issue #3448)")

    # slice deliveries (issue #3550): reported as their own class so a slice of a
    # deliberately-open issue is never read as a completed issue. Not a weighted category —
    # a slice is a successful delivery, not a failure; the fact worth surfacing is what the
    # record COUNTS AS.
    if tally["slice_records"]:
        print(f"  ({tally['slice_records']} of {len(records)} record(s): SLICE deliveries — a "
              f"merged PR shipping part of an issue that intentionally stays OPEN "
              f"(closed_at null), cycle time bounded by the PR's mergedAt; counted as "
              f"separate deliveries, NOT as completed issues — see issue #3550)")

    top_cat, top_cnt, _, top_score = ranked[0]
    if top_score == 0:
        print("\nno recurring failures recorded — nothing to file")
        return 0
    print(f"\ntop recurring failure: {top_cat} (count={top_cnt}, score={top_score})")

    # dedupe against open flow-meta issues by stable category marker. The live lookup
    # needs gh; in a dry-run a gh failure degrades to a warning (a read-only preview must
    # not hard-require auth), but at filing time gh is mandatory (we won't file blind).
    marker = _retro_marker(top_cat)
    try:
        existing = [i for i in _open_flow_meta_issues(args) if marker in (i.get("body") or "")]
    except SystemExit as exc:
        if args.file:
            raise
        print(f"warning: dedup check skipped ({exc}) — dry-run preview only", file=sys.stderr)
        existing = []
    if existing:
        nums = ", ".join(f"#{i['number']}" for i in existing)
        print(f"already tracked by open flow-meta issue(s): {nums} — skipping filing")
        return 0

    title = f"flow-meta: reduce recurring '{top_cat}' (retro top failure)"
    body = (
        f"{marker}\n\n"
        f"## Retro finding\n"
        f"The recurring-retro over `{ledger}` ranks **{top_cat}** as the highest-cost "
        f"recurring pipeline failure (count={top_cnt}, weighted score={top_score}).\n\n"
        f"## Ranked categories\n"
        + "\n".join(f"- {c}: count={n}, score={s}" for c, n, _, s in ranked)
        + "\n\n_Filed by `scripts/delivery-telemetry.py retro`._\n"
    )

    if not args.file:
        print("\n--- DRY RUN (pass --file to create) ---")
        print(f"title: {title}")
        print(body)
        return 0

    url = _gh(["gh", "issue", "create", "--title", title, "--body", body,
               "--label", "flow-meta", "--label", "P2"])
    print(f"filed: {url.strip()}")
    return 0


# ============================================================ argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="retro weight table (policy constants): "
               + ", ".join(f"{k}={v}" for k, v in RETRO_WEIGHTS.items()),
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--ledger", default=str(DEFAULT_LEDGER), help="ledger path (JSONL)")
        sp.add_argument("--schema", default=str(DEFAULT_SCHEMA), help="JSON Schema path")

    rec = sub.add_parser("record", help="append one ledger record")
    common(rec)
    rec.add_argument("--issue", type=int, required=True)
    rec.add_argument("--pr", type=int, required=True)
    rec.add_argument("--slug", required=True)
    rec.add_argument("--routing", choices=["design", "oracle"], default=None)
    rec.add_argument("--priority", default=None, help="P0..P3 (else read from the issue)")
    rec.add_argument("--gate", choices=["pass", "fail", GATE_NOT_RUN], default=None,
                     help="final full-gate outcome; '%s' (issue #3448) records that NO "
                          "full gate of record ran and REQUIRES --gate-runs 0" % GATE_NOT_RUN)
    rec.add_argument("--gate-runs", dest="gate_runs", type=int, default=None)
    rec.add_argument("--claim-collisions", dest="claim_collisions", type=int, default=None)
    rec.add_argument("--rebase-events", dest="rebase_events", type=int, default=None)
    rec.add_argument("--roborev-findings", dest="roborev_findings", type=int, default=None)
    rec.add_argument("--roborev-blockers", dest="roborev_blockers", type=int, default=None,
                     help="BLOCKER-severity roborev findings (issue #2088); supply together "
                          "with --roborev-nits, summing to --roborev-findings")
    rec.add_argument("--roborev-nits", dest="roborev_nits", type=int, default=None,
                     help="NIT-severity roborev findings (issue #2088); supply together "
                          "with --roborev-blockers")
    rec.add_argument("--rework", type=int, default=None)
    rec.add_argument("--nudges", type=int, default=None,
                     help="manual lead nudges required during the cycle to un-stall background "
                          "work (issue #2667); OPTIONAL — omit when unobserved, never default to 0")
    rec.add_argument("--orphan-minutes", dest="orphan_minutes", type=int, default=None,
                     help="total minutes background work sat completed-but-unowned this cycle "
                          "(issue #2667); OPTIONAL — omit when unobserved, never default to 0")
    rec.add_argument("--slice", dest="slice_delivery", action="store_true",
                     help="record a delivery of an issue that intentionally stays OPEN "
                          "(issue #3550): writes closed_at: null and bounds cycle_time_s on "
                          "the PR's mergedAt. Refused if the issue is closed now, if it is "
                          "open only because it was REOPENED, or if this PR declares it "
                          "closes the issue (a slice PR closes nothing)")
    rec.add_argument("--from-json", dest="from_json", default=None,
                     help="inject GitHub-derived fields from a JSON file (else pull via gh)")
    rec.add_argument("--allow-duplicate", dest="allow_duplicate", action="store_true",
                     help="append even if this (issue, pr) cycle already has a record "
                          "(default: refuse; a reopened issue's NEW pr never needs this)")
    rec.set_defaults(func=cmd_record)

    lint = sub.add_parser("lint", aliases=["validate"], help="schema-validate the ledger")
    common(lint)
    lint.set_defaults(func=cmd_lint)

    ret = sub.add_parser("retro", help="rank failures; optionally file a deduped issue")
    common(ret)
    ret.add_argument("--file", action="store_true", help="file a flow-meta issue (default: dry-run)")
    ret.add_argument("--open-issues-json", dest="open_issues_json", default=None,
                     help="inject open flow-meta issues from a JSON file (else query gh)")
    ret.set_defaults(func=cmd_retro)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
