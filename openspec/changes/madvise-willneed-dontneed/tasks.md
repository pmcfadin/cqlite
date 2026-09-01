# Tasks — madvise-willneed-dontneed (issue #2824, SLICE 1)

## 1. The policy flip
- [ ] 1.1 `backend_resolve.rs` `mmap_advice_for`: split the `Off | Auto` arm so `Auto` returns
      `Some(Advice::WillNeed)` and `Off` keeps returning `None`. Rewrite the policy doc comment above it
      to state the #2824 rationale and to keep the #1143 `MADV_SEQUENTIAL` prohibition explicit.
- [ ] 1.2 Correct the now-false claim in the `POINT_MMAP_MADV_RANDOM_MIN_BYTES` doc comment
      (`reader/mod.rs:173-175`) that "the default `PrefetchMode::Auto` leaves it unadvised entirely".
- [ ] 1.3 Correct the `PrefetchMode::Auto` variant doc and the `StorageConfig::prefetch` field doc
      (`config.rs:212-219`, `:275-276`).

## 2. Retarget the #1143 pin — never delete it
- [ ] 2.1 `reader/tests.rs:626` `test_mmap_advice_for_auto_is_no_madvise`: rename and rewrite to assert
      the durable invariant — `Auto` never yields `Sequential` — plus the new positive pin
      `Auto == Some(WillNeed)`. Keep the `Off`/`Sequential`/`WillNeed` asserts. The doc comment must
      explain why `WillNeed` does not carry #1143's drop-behind mechanism.
- [ ] 2.2 Correct the four factually-wrong doc-comment lines in
      `tests/issue_1143_mmap_prefetch_tail_guard.rs` (`:7`, `:14`, `:29`, `:305`) that state
      `Auto -> None`. The test body and its observational-only posture are unchanged.
- [ ] 2.3 `tests/config_knob_behavior_guard.rs:218-226`: upgrade the `storage.prefetch`
      `Evidence::Reserved` string now that `Auto`'s advice is directly asserted.

## 3. Slice boundary
- [ ] 3.1 Assert no `MADV_DONTNEED` / `unchecked_advise` is introduced anywhere in this diff.
- [ ] 3.2 Assert no `posix_fadvise` is introduced on the mmap path.

## 4. Measurement
- [ ] 4.1 Run `docs/reports/issue-2824-artifacts/cold-warm-ab.sh` baseline-vs-patched over the
      `ws0.events` fixture, page cache dropped per cold arm.
- [ ] 4.2 Record the result with the host, and state the i4i magnitude as UNMEASURED.
- [ ] 4.3 Run `issue_1143_mmap_prefetch_tail_guard.rs` and record it green.

## 5. Delivery
- [ ] 5.1 `--lite` green each fix round.
- [ ] 5.2 rust-reviewer + roborev on the lite-green diff, before any full gate.
- [ ] 5.3 PR, then flow-closer endgame.
- [ ] 5.4 File slice 2 (AC2) carrying the four findings.
