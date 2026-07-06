#!/usr/bin/env python3
"""delivery-telemetry.py — delivery-pipeline telemetry ledger + recurring retro.

Closes the pipeline self-improvement loop:
  sense    -> an append-only ledger, one record per completed issue (`record`)
  diagnose -> rank recorded failures, file a deduped flow-meta issue (`retro`)
  improve  -> that issue runs through the normal pipeline

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


# ============================================================ minimal JSON-Schema check
#
# A pure-stdlib subset validator (keeps the tool dependency-free / SKIP-aware on python3
# alone). The schema file remains the source of truth; this enforces the keywords the
# record schema actually uses: type, required, properties, const, enum, minimum, pattern.

def _type_ok(value, t: str) -> bool:
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
    if schema.get("type") == "array" and isinstance(value, list):
        item_schema = schema.get("items")
        if item_schema:
            for idx, item in enumerate(value):
                _validate(item, item_schema, f"{path}[{idx}]", errors)
    if schema.get("type") == "object" and isinstance(value, dict):
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


def validate_record(record: dict, schema: dict) -> list:
    """Return a list of human-readable validation errors (empty == valid)."""
    errors: list = []
    _validate(record, schema, "", errors)
    if isinstance(record, dict):
        _validate_severity_pair(record, errors)
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
        ["gh", "issue", "view", str(issue), "--json", "createdAt,closedAt,labels"]))
    pr_json = json.loads(_gh(
        ["gh", "pr", "view", str(pr), "--json", "createdAt,mergedAt"]))
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
        "created_at": issue_json["createdAt"],
        "closed_at": issue_json["closedAt"],
        "pr_opened_at": pr_json["createdAt"],
        "merged_at": pr_json["mergedAt"],
        "priority": priority,
        "routing": routing,
    }


def build_record(args, gh_fields: dict) -> dict:
    """Assemble a record from supplied counters + authoritative GitHub fields.

    Error convention: caller-input / precondition errors (a missing counter, a null
    timestamp, an undeterminable priority/routing) `raise SystemExit` here — they are bad
    invocations, like an argparse failure. Ledger-state outcomes (a built record that
    fails schema validation, a duplicate issue) are reported by `cmd_record` with a stderr
    message + `return 1`. Both surface as a non-zero exit.
    """
    for counter in REQUIRED_COUNTERS:
        if getattr(args, counter) is None:
            raise SystemExit(
                f"error: --{counter.replace('_', '-')} is required "
                f"(authoritative-data-only: a counter that was not observed is never defaulted)")
    if args.gate is None:
        raise SystemExit("error: --gate {pass|fail} is required")

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

    # Authoritative timestamps must all be present — a finalize runs only on a merged,
    # closed issue. A null here (e.g. an unmerged PR) is an error, not silent arithmetic
    # on None (which would otherwise raise an opaque AttributeError).
    for key in ("created_at", "pr_opened_at", "merged_at", "closed_at"):
        if not gh_fields.get(key):
            raise SystemExit(
                f"error: authoritative timestamp '{key}' is missing/null "
                f"(finalize records only a merged, closed issue)")

    created = gh_fields["created_at"]
    pr_opened = gh_fields["pr_opened_at"]
    merged = gh_fields["merged_at"]
    closed = gh_fields["closed_at"]
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
        "cycle_time_s": _seconds_between(created, closed),
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
    # Idempotency: one record per completed issue. A re-run / double finalize must not
    # append a second record (which would double-count that issue in retro). Refuse
    # unless --allow-duplicate is given. This check is best-effort, not atomic — the real
    # cross-session serializer is the per-issue branch lock (one worktree → one finalize);
    # `lint` is the after-the-fact backstop that flags any duplicate that slips through.
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
            if (isinstance(existing, dict) and existing.get("issue") == record["issue"]
                    and not args.allow_duplicate):
                print(f"error: issue #{record['issue']} already has a ledger record "
                      f"(pass --allow-duplicate to override)", file=sys.stderr)
                return 1

    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
    print(f"recorded issue #{record['issue']} (pr #{record['pr']}) -> {ledger}")
    return 0


# ============================================================ lint

def load_ledger(ledger: Path, schema: dict):
    """Parse + validate every ledger line. Returns (records, errors) where errors is a
    list of (lineno, message): malformed JSON, schema violations, AND duplicate issue
    numbers (one-record-per-issue). Shared by `lint` and `retro` so both apply the exact
    same rules — neither can rank/accept a ledger the other would reject.
    """
    records, errors = [], []
    seen_issues: dict = {}
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
        # one record per completed issue — a duplicate 'issue' skews retro. A non-object
        # line already produced a type error above; skip the bookkeeping (no .get crash).
        issue = record.get("issue") if isinstance(record, dict) else None
        if issue is not None and issue in seen_issues:
            errors.append((lineno, f"duplicate record for issue #{issue} "
                                   f"(first seen line {seen_issues[issue]})"))
        elif issue is not None:
            seen_issues[issue] = lineno
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
    """
    tally = {k: 0 for k in RETRO_WEIGHTS}
    nit_total = 0
    severity_records = 0
    for r in records:
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
        # gate_runs. gate_runs >= 1 by schema; max(0, ...) guards the schema-forbidden 0.
        passed_final = 1 if r["gate"] == "pass" else 0
        tally["gate_failures"] += max(0, r["gate_runs"] - passed_final)
    # Informational extras, NOT part of the weighted categories: rank() below iterates
    # RETRO_WEIGHTS (not tally.items()) specifically so these extra keys never need a
    # matching weight and can never desync rank()'s output.
    tally["roborev_nits_total"] = nit_total
    tally["roborev_severity_records"] = severity_records
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
    # counter, OR a duplicate issue (which would double-count in the tally) must be a clean
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
    rec.add_argument("--gate", choices=["pass", "fail"], default=None)
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
    rec.add_argument("--from-json", dest="from_json", default=None,
                     help="inject GitHub-derived fields from a JSON file (else pull via gh)")
    rec.add_argument("--allow-duplicate", dest="allow_duplicate", action="store_true",
                     help="append even if the issue already has a record (default: refuse)")
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
