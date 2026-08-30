//! Configuration validation for [`Config`] (issues #1695, #1696, #1697).
//!
//! Split out of `config.rs` under the campsite rule (epic #1116): that file is
//! already over the size target, and validation is a single responsibility with
//! commentary long enough to dominate it. Every rule states WHY it exists, because
//! a rule with no recorded reason is the thing that gets deleted by the next
//! person who finds it inconvenient.

use super::{Config, StorageConfig};

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> crate::Result<()> {
        // Validate memory limits
        if self.memory.max_memory == 0 {
            return Err(crate::Error::configuration(
                "max_memory must be greater than 0",
            ));
        }

        // Validate the (single) cache budget does not exceed total memory.
        if self.memory.block_cache.max_size > self.memory.max_memory {
            return Err(crate::Error::configuration(
                "block_cache.max_size exceeds max_memory",
            ));
        }

        // Validate storage settings
        if self.storage.memtable_size_threshold == 0 {
            return Err(crate::Error::configuration(
                "memtable_size_threshold must be greater than 0",
            ));
        }

        // Both memtable byte knobs are `u64` on the public surface but `usize`
        // in the engine (see `WriteEngineConfig::from_config`). On a 32-bit or
        // wasm32 target a value above `usize::MAX` cannot be represented, and
        // the bridge's clamp would land it exactly on `usize::MAX` — the state
        // `memtable.rs` names degenerate: `should_flush` never fires and
        // `check_admission`'s `projected > hard_limit` is UNREACHABLE because
        // `saturating_add` caps at `usize::MAX`. That is never-flush AND
        // never-reject: grow until OOM. Reject it here instead (#1697).
        //
        // `usize_max_bytes` is the target's `usize::MAX` widened to `u64` — via
        // `try_from`, never an `as` cast — so on a 64-bit target it equals
        // `u64::MAX` and the comparisons below are trivially false rather than
        // ill-typed. A hypothetical target with `usize` WIDER than `u64` falls
        // back to `u64::MAX`, which is also correct: every `u64` value is then
        // addressable. The bridge keeps its clamp as defense in depth for any
        // path that skips `validate`.
        let usize_max_bytes = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
        for (knob, bytes) in [
            (
                "memtable_size_threshold",
                self.storage.memtable_size_threshold,
            ),
            ("memtable_hard_limit", self.storage.memtable_hard_limit),
        ] {
            if bytes > usize_max_bytes {
                return Err(crate::Error::configuration(format!(
                    "{knob} ({bytes} bytes) exceeds this target's addressable maximum \
                     ({usize_max_bytes} bytes); a memtable that large can never flush \
                     and can never reject a write"
                )));
            }
        }

        // A hard limit below the flush threshold wedges the write engine for
        // EVERY write: the memtable is rejected at the ceiling before a flush can
        // relieve it. Only expressible as a rule now that both knobs live here
        // (#1697).
        //
        // SCOPE OF THIS RULE, stated because it is narrower than it looks
        // (#1697 roborev r2; the engine defect is #3404): passing it does NOT
        // make the write path wedge-free. `WriteEngine::check_admission` rejects
        // `memtable_size + incoming > memtable_hard_limit` without attempting a
        // flush, while auto-flush fires only AFTER a successful insert. So any
        // single mutation larger than `memtable_hard_limit - memtable_size` is
        // rejected while the memtable sits below the flush threshold, and
        // retrying it is rejected forever.
        //
        // NO INEQUALITY BETWEEN THESE TWO KNOBS CAN CLOSE THAT: with one byte of
        // headroom a 3-byte mutation still wedges, and the wedge is a function of
        // the largest single mutation, which config cannot know. So this rule is
        // NOT a wedge-freedom guarantee and must not be read as one; #3404 owns
        // the real fix (flush a nonempty memtable before rejecting a mutation
        // that fits by itself).
        //
        // It nonetheless requires STRICT headroom, because equality is
        // qualitatively worse than any positive headroom rather than merely one
        // step along a continuum. For a mutation of `m` bytes the wedge window is
        // `m - headroom` bytes wide, so at equality an ORDINARY 4 KiB write
        // wedges over a 4 KiB window of memtable sizes — a state normal operation
        // passes through routinely — while at the default 192 MiB of headroom
        // even a 64 MiB mutation cannot wedge at all. Equality also has no
        // legitimate use: it asks the engine to flush at exactly the size where
        // it must instead reject. Rejecting it removes the only regime in which
        // everyday writes livelock, which is worth doing even though it proves
        // nothing about the general case.
        if self.storage.memtable_hard_limit <= self.storage.memtable_size_threshold {
            return Err(crate::Error::configuration(format!(
                "memtable_hard_limit ({} bytes) must be strictly greater than \
                 memtable_size_threshold ({} bytes); with no headroom between them \
                 an ordinary write is rejected at the ceiling while the memtable \
                 sits below the flush trigger, and retrying it never recovers",
                self.storage.memtable_hard_limit, self.storage.memtable_size_threshold
            )));
        }

        // Validate the STCS thresholds threaded into the write engine (#1697).
        // `STCSPolicy::new` rejects these too, but failing here surfaces the
        // problem at config time rather than at engine construction.
        //
        // ONLY when `auto_compaction` is on (#1697 roborev r4). Both fields are
        // documented as "Ignored when `auto_compaction` is `false`", and that is
        // literally true of the code: `WriteEngine::new` constructs
        // `STCSPolicy::new(min, max, ..)` inside `if config.auto_compaction`, and
        // leaves the policy unset otherwise. Judging them unconditionally
        // therefore rejected configurations that work — the thresholds are never
        // read — while contradicting their own documented contract.
        let compaction = &self.storage.compaction;
        if compaction.auto_compaction && compaction.min_threshold == 0 {
            return Err(crate::Error::configuration(
                "compaction.min_threshold must be greater than 0",
            ));
        }
        if compaction.auto_compaction && compaction.max_threshold < compaction.min_threshold {
            return Err(crate::Error::configuration(format!(
                "compaction.max_threshold ({}) must be >= compaction.min_threshold ({})",
                compaction.max_threshold, compaction.min_threshold
            )));
        }

        // Query execution budget (issue #1695). `Duration::ZERO` is the documented
        // "no timeout" sentinel and is therefore explicitly LEGAL: validation must
        // never reject it (pinned by `config_validate_accepts_the_zero_sentinel`
        // in `tests/issue_1695_query_timeout.rs`). Every non-zero value is a real
        // budget honoured at the engine chokepoint — a `Duration` cannot be
        // negative and any positive budget is enforceable — so there is nothing
        // further to reject here. This arm exists so a future "must be > 0" rule
        // cannot be added without confronting the sentinel contract.

        // `direct_io_memory_fraction` is a FRACTION of system RAM (issue #1696,
        // AH3). Before this arm existed it was live but unvalidated: the reader's
        // `resolve_disk_access_mode` silently CLAMPED nonsense — `<= 0.0`, NaN and
        // the infinities fell back to the 0.5 default, and anything above `1.0`
        // was pinned at `1.0`. An operator who wrote `2.0` (meaning "twice RAM")
        // or `-1` therefore got the default and no word about it, which is the
        // same dishonesty as a decorative knob: the value they set was not the
        // value that ran.
        //
        // The rule itself, the range's endpoints and the reasoning for each live
        // on `StorageConfig::validated_direct_io_memory_fraction`, because the
        // open boundaries — `Database::open`, `StorageEngine::open`,
        // `SSTableManager::new`, `SSTableReader::open` — enforce the same rule
        // without going through here (#1696 roborev F2/r3 F2) and one rule must
        // have one definition.
        self.storage.validated_direct_io_memory_fraction()?;

        Ok(())
    }
}

impl StorageConfig {
    /// [`Self::direct_io_memory_fraction`] if it is a legal fraction, else a
    /// configuration error (issue #1696, AH3).
    ///
    /// # Why this is a method and not an inline check in `validate`
    ///
    /// It is enforced at EVERY public boundary that can act on the value, and
    /// several of them are reachable without a `Database`: [`Config::validate`],
    /// `Database::open`, `StorageEngine::open`, `StorageEngine::open_with_sstables`,
    /// `SSTableManager::new`, `SSTableManager::new_from_discovered_paths` and
    /// `SSTableReader::open`. That many call sites is precisely why the rule needs
    /// ONE definition — restated inline they would drift.
    ///
    /// The discovery boundaries matter for a second reason (#1696 roborev r3 F2):
    /// discovery treats a per-file reader-open error as best-effort, logging and
    /// skipping it, so an unvalidated bad fraction there would fail every reader
    /// open and the engine would report SUCCESS with ZERO SSTables — a silent
    /// empty result instead of a named config error.
    ///
    /// # The rule, and why the ends of the range are where they are
    ///
    /// The legal range is the documented `(0.0, 1.0]`. Before this existed the
    /// value was live but unvalidated: the reader's `resolve_disk_access_mode`
    /// silently CLAMPED nonsense — `<= 0.0`, NaN and the infinities fell back to
    /// the `0.5` default, and anything above `1.0` was pinned at `1.0`. An
    /// operator who wrote `2.0` (meaning "twice RAM") or `-1` got the default and
    /// no word about it, which is the same dishonesty as a decorative knob: the
    /// value they set was not the value that ran.
    ///
    /// * **`1.0` is LEGAL** — "all of RAM" is a coherent ceiling.
    /// * **`0.0` is REJECTED, and is NOT read as "never use direct I/O"** — that
    ///   is the whole reason it cannot be accepted. A zero threshold makes EVERY
    ///   nonempty file exceed it, so `Auto` would escalate everything to direct
    ///   I/O: the value reads as "never" and behaves as "always". Inferring which
    ///   one the operator meant would be a guess, and CQLite does not guess
    ///   (issue #28). "Never use direct I/O" is spelled
    ///   [`super::DiskAccessMode::Mmap`] (or [`super::DiskAccessMode::Buffered`]); "always" is
    ///   spelled [`super::DiskAccessMode::Direct`].
    /// * **A subnormal or otherwise tiny positive fraction is LEGAL** and is
    ///   honoured LITERALLY: `1e-300` of RAM rounds to a 0-byte threshold, so
    ///   every nonempty file uses direct I/O. That is the honest consequence of
    ///   what was asked for, and unlike `0.0` it is unambiguous — a real, if
    ///   degenerate, fraction rather than a value whose plain reading contradicts
    ///   its behaviour. It is not clamped and not second-guessed.
    /// * **NaN and both infinities are REJECTED.** The test is written as
    ///   `!(fraction > 0.0 && fraction <= 1.0)` rather than a chain of `<`/`>`
    ///   precisely so NaN — for which every ordered comparison is false — is
    ///   rejected instead of sailing through.
    ///
    /// The reader keeps its internal clamp as defense in depth for any future
    /// caller that reaches `resolve_disk_access_mode` without validating.
    pub fn validated_direct_io_memory_fraction(&self) -> crate::Result<f64> {
        let fraction = self.direct_io_memory_fraction;
        if !(fraction > 0.0 && fraction <= 1.0) {
            return Err(crate::Error::configuration(format!(
                "direct_io_memory_fraction ({fraction}) must be a fraction of system memory in \
                 (0.0, 1.0]; it is not a byte count, and a value outside that range was \
                 previously clamped silently. For \"always bypass the page cache\" set \
                 disk_access_mode = Direct; for \"never\" set Mmap or Buffered"
            )));
        }
        Ok(fraction)
    }
}
