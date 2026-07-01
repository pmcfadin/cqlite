# Pull Request Description

## Related Issue
- Closes #<!-- issue number -->
- Related to #<!-- issue number if applicable -->

## Changes Made
<!-- Provide a clear and concise summary of the changes -->

### Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring
- [ ] Test coverage improvement

### Component(s) Affected
- [ ] cqlite-core
- [ ] cqlite-cli  
- [ ] cqlite-ffi
- [ ] cqlite-wasm
- [ ] SSTable Reader
- [ ] CQL Parser
- [ ] Schema Management
- [ ] Documentation
- [ ] Build System/CI
- [ ] Tests

## Testing
<!-- Describe the tests you ran to verify your changes -->

### Test Environment
- OS: <!-- e.g., Ubuntu 22.04, macOS 14.0, Windows 11 -->
- Rust version: <!-- e.g., 1.75.0 -->
- Test data: <!-- MUST use canonical real data under test-data/datasets; reference metadata.yml and test-data/schemas/ -->

### Tests Run
- [ ] Unit tests (`cargo test`)
- [ ] Integration tests
- [ ] CLI tests
- [ ] Performance tests/benchmarks
- [ ] Manual testing
- Local pre-merge mode used: <!-- e.g., not run, fast, full, or custom command -->

### Test Results
<!-- Include relevant test output, benchmark results, or screenshots -->

```
<!-- Paste test output here if relevant -->
```

## Public Surface & Wiring Evidence
<!--
REQUIRED for any PR that adds or changes feature/perf behavior (issues #949/#963).
A new capability is NOT done when a helper passes unit tests in isolation — it is done
when the intended USER-FACING surface (CQL execute, streaming, prepared/bind params,
CLI, REPL, bindings) actually exercises it. Skip this section ONLY for pure docs/CI/
refactor PRs that change no observable behavior.
-->
- [ ] This PR adds/changes feature or performance behavior (if unchecked, this section may be skipped)
- [ ] **Call-chain evidence**: I listed the path from the public surface down to the new code
      (e.g. `SELECT execute -> QueryEngine -> SSTableReader::seek_partition -> new helper`)
- [ ] **End-to-end test from the public surface**: a test drives the user-facing API/CLI/binding
      (not just the helper) and asserts the new behavior is observable
- [ ] No new public API is a stub / placeholder / validation-only shell
      (no `_params` ignored, no `TODO: Implement`, no `"For now"`, no `unimplemented!()`,
      no public method that only validates inputs then returns a default)
- [ ] If a fallback path exists, the e2e test distinguishes the NEW path from the fallback
      (e.g. via work counters, output difference, or an assertion that the fallback did not run)

**Public surface(s) exercised:**
<!-- e.g. CQL one-shot --query; Python db.execute(); CLI `delta-export` -->

**Call chain (public surface → new code):**
<!-- paste the call chain here -->

**Justified exception** (if no e2e test): <!-- explain why, e.g. intentionally internal, see "Intentionally internal" below -->

### Intentionally internal / feature-flagged work
<!-- Check if this PR's new surface is deliberately NOT yet user-reachable -->
- [ ] This capability is intentionally internal or staged behind a feature flag
- [ ] The flag / internal status is documented and the issue says so (link the follow-up issue that wires it to a public surface)
- [ ] The audit (`scripts/audit-inert-surfaces.sh`) findings for this work are expected and explained below

<!-- For staged/flagged work, name the flag and the tracking issue that will wire it up:
     Flag: <feature-name>  |  Wiring issue: #<n> -->

## Cassandra Compatibility
<!-- If applicable, describe how this affects Cassandra compatibility -->
- [ ] Maintains compatibility with existing Cassandra versions
- [ ] Adds support for new Cassandra features
- [ ] Changes compatibility matrix (update CASSANDRA_COMPATIBILITY_MATRIX.md)

## Performance Impact
<!-- Describe any performance implications -->
- [ ] No performance impact
- [ ] Performance improvement (include benchmark results)
- [ ] Potential performance regression (justified by other benefits)
- [ ] Performance impact unknown/needs testing

## Documentation
<!-- Check all that apply -->
- [ ] Code is self-documenting with clear variable names and comments
- [ ] Public API changes are documented
- [ ] README.md updated if needed
- [ ] Compatibility matrix updated if needed
- [ ] Examples updated if needed

## Checklist
<!-- Ensure all items are checked before requesting review -->
- [ ] My code follows the project's style guidelines
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] For feature/perf work, I added an end-to-end test from the public surface (see "Public Surface & Wiring Evidence" above) — green unit tests for a helper alone are not sufficient
- [ ] I ran `scripts/audit-inert-surfaces.sh` and any flagged surfaces are explained or wired
- [ ] New and existing unit tests pass locally with my changes
- [ ] Any dependent changes have been merged and published

## Additional Notes
<!-- Add any additional context, concerns, or questions for reviewers -->

## Screenshots/Output
<!-- If applicable, add screenshots or command output to demonstrate the changes -->
