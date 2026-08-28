# Ruling 4 (machine-check `0x0390bfbb81a23fa1`) is NOT AVAILABLE — reported, not quietly dropped

The coordination lead approved folding the producer-tap Arrow digest during the AC0 run to convert
`ARROW_BUFFER_DIGEST = 0x0390bfbb81a23fa1` from operator-verified to machine-verified, on the
reasoning that this lane holds the real 4M-row corpus and the scan is happening anyway. That
reasoning was sound and the opportunity is real in principle. **It is not reachable, and the reason
is more interesting than the win would have been.**

## The mechanism, verified

1. **The digest ORACLE runs on a test FIXTURE, not on the measurement corpus.**
   `cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs` folds over a 500-row, batch-128
   `ws0.events` fixture built by `cqlite-flight/tests/support/ws0_fixture.rs`.
2. **That oracle REQUIRES a null-bearing corpus, by design.** Its own module docs
   (`issue_3096_arrow_buffer_digest.rs:19-24`): *"Folding a validity bitmap proves nothing if no
   cell is ever null: every bitmap is then absent or all-set, and a misplaced validity bit has
   nothing to misplace. The fixture therefore carries `NullPlan::Pinned`."* This is the
   roborev-finding-2 fix that re-pinned the CI digest to `0xe6eccf8a9ffbca11`.
3. **`NullPlan` does not exist in the measurement-corpus generator.** Verified by census: the only
   two files in the repository mentioning `NullPlan` are the digest test and its fixture support
   module, both under `cqlite-flight/tests/`. `tools/ws0-corpus-gen` — which writes the 4M-row
   measurement corpus — has no null-plan capability at all, and writes every non-key column on
   every row.
4. **So the measurement corpus is all-non-null by construction**, and the rig says so in its own
   report notes: the digest oracle *"is UNREACHABLE for this corpus, because the #3096 digest
   oracle refuses a corpus in which no Arrow validity bitmap ever carries an absent value, and
   ws0-corpus-gen writes every non-key column on every row."* Closing it needs changes to
   **production `flight-loadgen`** (a per-step digest) **and** a null plan in the corpus generator.

There is no path from here to a machine-check that does not either (a) change the corpus — producing
a *different* corpus with a different `Data.db` sha256, which by definition no longer verifies
**this** pin — or (b) bypass a guard the repository deliberately added.

## The finding that replaces the win, which is worth more

**The pin was recorded over a corpus that the repository's own current oracle would refuse as
incapable of detecting the bug class the oracle exists for.**

`ARROW_BUFFER_DIGEST` was captured at #3096 Phase-0 over the all-non-null measurement corpus. The
`NullPlan::Pinned` requirement arrived later, with roborev finding 2, on the reasoning that an
all-non-null fold cannot exercise a validity bitmap. Both statements are true simultaneously, and
together they say something sharper than "this pin is unchecked":

* `measurement_corpus.rs` already marks the pin **NO** for machine-checked, with an honest reason
  ("requires a real 4,000,000-row corpus"; "a gate component may not write 2.8 GB or run for
  minutes"). That framing implies the only obstacle is **cost**.
* It is not only cost. **Even performed, the fold would be a weak check by the repository's own
  current standard** — it would hash value buffers and all-set-or-absent validity bitmaps, and
  could not catch the validity-bit class that the CI digest was re-pinned specifically to catch.

So the correct disposition is not "verify it when convenient" but **"this pin has a narrower
meaning than its name suggests"**: it is a value-buffer digest over a null-free corpus, not a
full Arrow-buffer digest in the sense the CI fixture now establishes. Recording that is the
durable outcome; recording a green check would have overstated it.

## What this lane did instead, at the same near-zero cost

The corpus's **identity** was machine-checked rather than assumed: all 8 components re-hashed from
disk against `corpus-identity.json`, `Data.db` = `4a903f6f…ae269` matching both the issue's pin and
`tools/ws0-corpus-gen/src/measurement_corpus.rs`, and the rig independently re-hashed all 8
components **at every measurement boundary** — after each arm of each rep, not merely at the ends.
That is a real strengthening of the corpus-side evidence; it is simply not the Arrow-digest
strengthening that was hoped for, and the two should not be conflated in the report.
