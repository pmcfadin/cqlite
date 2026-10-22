---
name: ci-cd-validation
description: Use when validating CQLite changes, preparing a PR, reviewing gate evidence, addressing CI failures, or certifying a revision for merge.
---

# CQLite validation and delivery

Read `docs/development/gate-ops.md` before launching or interpreting a gate.
The script is the component authority; inspect `bash scripts/agent-gate.sh --list`
when needed. Do not maintain a parallel manual checklist or coverage percentage.

- Iterate with targeted tests and `--lite`. Review before the full gate.
- Run the full gate once on the final reviewed revision before merge. A source
  change or rebase requires renewed certification; “once” is not permission to
  reuse a stale PASS. For eligible test/docs-only polish, use the documented
  anchored `--delta` procedure. `--only` is diagnostic, never certification.
- Core changes require `cargo clippy -p cqlite-core --all-targets --all-features -- -D warnings`
  before push; this does not replace the full gate.
- Use unique summary/log paths for the run. Set `AGENT_GATE_SUMMARY_FILE`, redirect
  stdout/stderr to the log, and stdin from `/dev/null`. Retain the summary, not
  raw log output. Use the available Codex process/session tools to track completion;
  follow gate-ops lifecycle rules for long-running work. A queued run is not hung.
- Completion and verdict are separate. Use the mode-specific completion grammar
  in gate-ops, then check the required component verdicts, run-id, source SHA,
  `tree-integrity`, and `dirty: no`. INCOMPLETE is not a verdict; PARTIAL and SKIP
  do not prove required coverage. Do not certify a tree mutated during the run.
- Fetch fixtures using `bash test-data/scripts/fetch-datasets.sh`; use the printed
  export line. Verify an existing root with `--verify-only`. Report missing corpus,
  zero executed tests, compiled-only lanes, and skipped tests explicitly.
- A gate PASS does not imply every workspace test ran. Confirm each new acceptance
  test actually executed and reproduce affected regeneration/CI-only lanes.

Before a roborev round, read `docs/development/roborev-contract.md`. Use only
`bash scripts/flow/roborev-review.sh --agent <agent> --model <model>` with an
accessible requested/configured model. Push first when delivery is authorized.
Only its successful terminal summary establishes a clean review; no-diff or
unavailable review is not PASS. For code-free changes use the contract's recorded
primary-source substitute, not a fabricated roborev result.

When merge is in scope: final rebase → full gate → spec intent audit → final
roborev → premerge assertion plus fresh HOLD check → sanctioned auto-merge.
Read `docs/development/merge-gate.md` and `pm-operating-loop.md` for exact commands.
Confirm MERGED before finalization. Otherwise report local validation status and
remaining certification work without publishing or merging beyond the request.

The two supporting checklist files are pointers to these maintained contracts.
