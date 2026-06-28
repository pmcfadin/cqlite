# Tasks — Export progress reporting & final statistics (#284)

## 1. Implementation (`cqlite-cli/src/commands/export.rs`)
- [ ] 1.1 Add a total-resolution helper that maps CLI `--limit` → `Option<u64>` total +
      ETA-eligibility flag (pure, no terminal access). Surface exercised:
      `export_data` total computation.
- [ ] 1.2 Replace the always-spinner construction with a `make_progress(show, total)` helper:
      `Some(total)` → determinate `ProgressBar::new(total)` with a `[{bar}] {percent}%
      ({pos}/{len}) ETA: {eta}` style; `None` → existing spinner. Handle the style-template
      result without `unwrap()`/`expect()` in non-test code.
- [ ] 1.3 Keep the per-chunk `pb.set_position(rows_exported)` drive (works for both bar and
      spinner); `finish_and_clear()` on completion.
- [ ] 1.4 Tighten the final summary so it prints only under the same TTY/`show_progress`
      condition as progress (no summary when piped), preserving the rows/size/time/rate
      fields via `format_bytes` / `format_export_duration`.

## 2. Tests (wiring-evidence)
- [ ] 2.1 Unit test for the total-resolution helper: `Some(n)` → determinate + ETA-eligible;
      `None` → spinner + not ETA-eligible. (Covers the "independently verifiable" requirement.)
- [ ] 2.2 CLI test in `cqlite-cli/tests/export_integration_tests.rs` exercising the public
      `export` subcommand: `--quiet` and piped (non-TTY) runs emit empty stdout while still
      writing the export file (covers both suppression scenarios). Needs
      `CQLITE_DATASETS_ROOT`.
- [ ] 2.3 CLI test asserting the final summary (`Rows:`/`Size:`/`Time:`/`Rate:`) renders on a
      non-suppressed path (forced-render hook so assert_cmd's non-TTY capture can observe it).

## 3. Docs
- [ ] 3.1 One-line note in CLAUDE.md CLI section: export shows a determinate bar + ETA when
      `--limit` is set, a spinner otherwise, and nothing when `--quiet`/piped.

## 4. Quality gates (definition of done)
- [ ] 4.1 `scripts/agent-gate.sh` PASS (run with `CQLITE_DATASETS_ROOT` → main repo datasets);
      paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 4.2 spec-auditor **C** PASS — every requirement `satisfied` with a public-surface test
      as evidence (anchored to `openspec/changes/export-progress-stats/specs/**`).
- [ ] 4.3 roborev clean (`--agent claude-code --model opus`, `--base origin/main`).
- [ ] 4.4 Open PR (do not merge by default).
