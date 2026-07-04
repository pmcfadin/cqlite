---
title: "For Agents: Developing CQLite"
description: Contributor doctrine, gate contracts, and development workflows for AI agents working on CQLite itself.
sidebar:
  label: Overview
  order: 0
---

This section documents the contributor doctrine, gate contracts, and development
workflows for AI agents (and humans) working on CQLite itself.

Write for AI agents: terse, imperative, copy-pasteable commands. Skip the prose.

## Pages in this section

| Page | What it covers |
|------|----------------|
| [Gate contract](/cqlite/agents-developing/gate-contract/) | `scripts/agent-gate.sh` — the only run that counts; summary-block format |
| [No-heuristics mandate](/cqlite/agents-developing/no-heuristics/) | Authoritative metadata only; legacy fallbacks behind flags (issue #28) |
| [Supported formats](/cqlite/agents-developing/supported-formats/) | Cassandra 5.0 version floor — `na`/`nb` BIG + `oa`/`da` BTI in scope; pre-`na` out of scope, do not review (issue #1249) |
| [Test data](/cqlite/agents-developing/test-data/) | Fetching datasets, dataset pins, CQLITE_DATASETS_ROOT, missing-data behaviour |
| [Key source paths](/cqlite/agents-developing/source-map/) | Where parsers, writers, query engine, and bindings live |
| [sstabledump validation playbook](/cqlite/agents-developing/validation-playbook/) | JSONL golden files, parity tests, smoke-test-all-tables |
| [Wiring evidence](/cqlite/agents-developing/wiring-evidence/) | Prove the public surface exercises a feature; reject helper-only implementations (issues #949/#963) |
| [Format debugging workflow](/cqlite/agents-developing/format-debugging/) | Hex dumps, definitive-guide chapters, appendix F known limitations |
| [Spec-driven audit](/cqlite/agents-developing/spec-driven-audit/) | OpenSpec as the front door for design-driven work; the intent audit (C) + roborev escalation (B); superpowers mapping |
| [Delivery pipeline](/cqlite/agents-developing/delivery-pipeline/) | The `flow-lead` manager agent + `flow-*` pipeline; specialist roster; the two human seams + pre-authorized merge-on-green |
| [Pre-roborev self-check](/cqlite/agents-developing/roborev-findings/) | The recurring roborev finding classes + the one-line fix for each; scan your diff before handing off (issue #1245) |

## Non-negotiable rules

1. Run `scripts/agent-gate.sh` before opening any PR. Paste its summary block verbatim. Ad-hoc `cargo test` runs do not count.
2. Use authoritative metadata only — no type guessing, no heuristics (see [no-heuristics mandate](/cqlite/agents-developing/no-heuristics/)).
3. CQLite targets Cassandra 5.0 — `na`/`nb` BIG + `oa`/`da` BTI are in scope; pre-`na` (`ma`–`me`) is out of scope. Do not introduce, support, or review pre-`na` correctness (see [Supported formats](/cqlite/agents-developing/supported-formats/)).
4. Integration tests use real SSTable data. Fetch it before running: `bash test-data/scripts/fetch-datasets.sh`.
5. `RUSTFLAGS="-D warnings"` must pass — zero clippy warnings allowed.
6. A feature is done only when its **public surface** exercises it. Name the surface, the
   call chain, and add an end-to-end test from that surface — green helper unit tests are
   not sufficient (see [Wiring evidence](/cqlite/agents-developing/wiring-evidence/)).

## Quick-start for a new agent

```bash
# 0. New machine? Bootstrap accelerators + datasets + gh/roborev config first.
#    (details: docs/development/agent-machine-setup.md)
bash scripts/bootstrap-agent-machine.sh

# 1. Fetch test data (bootstrap --yes does this too)
bash test-data/scripts/fetch-datasets.sh

# 2. Run the gate
scripts/agent-gate.sh

# 3. Paste the AGENT-GATE SUMMARY block in your PR report
```
