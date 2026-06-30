# Tasks — sstable-version-floor (#1249)

## 1. Typed error
- [ ] 1.1 Add `Error::UnsupportedVersion { version: String, floor: String }` to
  `cqlite-core/src/error.rs`; map it to `ErrorCategory::Data` (non-recoverable), matching
  `UnsupportedFormat`. Surface: the `Error` enum + its category/recoverability match arms.

## 2. Enforce the floor in version parsing
- [ ] 2.1 In `cqlite-core/src/storage/sstable/version_gate.rs`, make `BigVersionGates::from_version`
  reject any version `< "na"` with `Error::UnsupportedVersion`. Surface: `BigVersionGates::from_version`.
- [ ] 2.2 Upgrade `BtiVersionGates::from_version` to return `Error::UnsupportedVersion` (not
  `InvalidFormat`) for any version other than `da`. Surface: `BtiVersionGates::from_version`.
- [ ] 2.3 Unit tests: a below-`na` string (`ma`/`mc`/`me`) and a non-`da` BTI string each yield
  `UnsupportedVersion` naming version + floor; `na`/`nb`/`oa`/`da` still succeed.

## 3. Stop the reader from swallowing the floor
- [ ] 3.1 In `cqlite-core/src/storage/sstable/reader/mod.rs:402-413`, propagate `UnsupportedVersion`
  from gate construction instead of degrading to `nb_fallback()`; reserve fallback for a structurally
  unparseable descriptor. Surface: `SSTableReader::open` gate-derivation arm.
- [ ] 3.2 Read-path test (wiring-evidence): opening an SSTable whose descriptor version parses to a
  pre-`na` value fails at open with `UnsupportedVersion` and does NOT proceed on `nb` fallback. Prefer a
  descriptor/open-level test that exercises the public open path, not a helper-only unit test.
- [ ] 3.3 Test: a structurally-unparseable descriptor still uses the existing fallback (no
  `UnsupportedVersion` for the not-below-floor-but-unparseable case).

## 4. Remove the pre-`na` modeling surface
- [ ] 4.1 Delete `BigVersionGates::is_compatible()` and its unit test
  (`version_gate.rs:360`, tests ~`826-836`). Surface: `BigVersionGates`.
- [ ] 4.2 Simplify/remove gate-threshold branches in `from_version` that only mattered for `mb`–`me`
  now that `< na` is rejected; leave a comment that the floor is `na`. Grep tests for `ma`/`mb`/`mc`/
  `md`/`me` literals and update/remove (audit found these only in `version_gate.rs` tests).

## 5. Doctrine
- [ ] 5.1 Add a "Supported formats" **rule** to `CLAUDE.md` under Development Standards: accepted
  `na`/`nb` BIG + `oa`/`da` BTI; out-of-scope pre-`na` (`ma`–`me`); do-not-introduce/support/review
  pre-`na` correctness (incl. reviewers/roborev).
- [ ] 5.2 Mirror the rule on the `agents-developing/` doctrine source (no-heuristics / gate-contract
  neighborhood) so roborev's repo-context read carries the floor + do-not-review guidance.

## 6. Quality gates (definition of done)
- [ ] 6.1 `scripts/agent-gate.sh` PASS (run with `CQLITE_DATASETS_ROOT=~/projects/cqlite/test-data/datasets`);
  paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 6.2 spec-auditor **C** PASS — every requirement `satisfied` with a public-surface test as evidence
  (anchored to `openspec/changes/sstable-version-floor/specs/**`).
- [ ] 6.3 roborev clean (`--agent codex --base origin/main`).
- [ ] 6.4 `openspec validate sstable-version-floor --strict` clean.
