## 1. Schema + ledger

- [x] 1.1 Author `docs/reports/delivery-telemetry.schema.json` (JSON Schema, draft 2020-12):
      record v1 per design.md — required `schema/issue/slug/routing/priority`, the four GitHub
      timestamps, `cycle_time_s`, `phase_s`, the five counters, `gate`, `gate_runs`,
      `stamped_at`; enums for `routing`/`gate`; counters `integer >= 0`. Surface = the schema
      file consumed by `lint`/`record`.
- [x] 1.2 Create the live ledger `docs/reports/delivery-telemetry.jsonl` (empty or backfilled
      with the records the pipeline can authoritatively reconstruct). Verify `lint` passes on it.

## 2. Telemetry tool (`scripts/delivery-telemetry.py`)

- [x] 2.1 `record` subcommand: pull GitHub timestamps live via `gh` (or inject via
      `--from-json` for tests), require the run counters as flags, compute durations, validate
      against the schema, append one line. Error (not silent-zero) on a missing required
      counter. Surface = `record`.
- [x] 2.2 `lint` / `validate` subcommand: schema-validate every ledger line; non-zero +
      offending line number on any malformed record; zero on a clean ledger. Surface = `lint`.
- [x] 2.3 `retro` subcommand: rank failure categories by `Σ(count × documented fixed weight)`
      over the ledger; print the ranked summary (default dry-run); `--file` files a deduped
      `flow-meta` issue (dedupe via `<!-- RETRO:<category> -->` marker against
      `gh issue list --label flow-meta`, or `--open-issues-json` in tests). Weight table
      documented in `--help`. Surface = `retro`.

## 3. Tests (no network, no datasets)

- [x] 3.1 `scripts/tests/test_delivery_telemetry.py` (stdlib `unittest`):
      - `record --from-json` builds a record that validates against the schema (round-trip);
      - `lint` exits non-zero on a malformed line and zero on a clean ledger;
      - `record` errors on a missing required counter;
      - `retro` against a fixture ledger ranks the known dominant category to the top and
        creates no issue in dry-run;
      - `retro` dedupe skips filing when `--open-issues-json` already has the category marker.
- [x] 3.2 Add fixtures under `scripts/tests/fixtures/` (a small ledger + an open-issues JSON).

## 4. Wire into finalize + manager + gate

- [x] 4.1 `flow-finalize` skill: add the ledger-stamp step (call `record` with the run's
      counters) after merge/cleanup. Document where the counters come from.
- [x] 4.2 `manager` skill + `docs/development/pm-operating-loop.md`: add the recurring
      `retro` step (cadence + when to file).
- [x] 4.3 `scripts/agent-gate.sh`: add a SKIP-aware `delivery-telemetry` component that runs
      `scripts/tests/test_delivery_telemetry.py`; add it to `COMPONENTS`. Verify
      `scripts/agent-gate.sh --only delivery-telemetry`.

## 5. Docs

- [x] 5.1 CLAUDE.md: document the ledger path/schema + retro in the delivery-pipeline section.
- [x] 5.2 Website `agents-developing/delivery-pipeline` page: add a telemetry/retro subsection
      (mirror CLAUDE.md).

## 6. Quality gates (definition of done)

- [ ] 6.1 `scripts/agent-gate.sh` PASS (paste the AGENT-GATE SUMMARY block).
- [ ] 6.2 spec-auditor **C** PASS: every requirement `satisfied` with a public-surface test as
      evidence (anchored to `openspec/changes/delivery-telemetry-ledger/specs/**`).
- [ ] 6.3 roborev clean (`--agent claude-code --model opus`).
