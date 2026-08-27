//! The ONE translation from the public [`Config`] facade to the write engine's
//! [`WriteEngineConfig`] (issue #1697, AH4 — config source of truth).
//!
//! Before this bridge existed CQLite ran two independent config layers for the
//! write path. The public `Config.storage.*` facade an embedder can set carried
//! `memtable_size_threshold` with NO production reader, while the engine's
//! private `WriteEngineConfig` carried its own defaults — a different flush
//! threshold (64MB vs the facade's 16MB) plus STCS `min_threshold` /
//! `max_threshold` with no public counterpart at all. Setting the public knob
//! therefore changed nothing, silently.
//!
//! [`WriteEngineConfig::from_config`] is now the single place public config
//! values are translated, and [`WriteEngineConfig::new`] is defined in terms of
//! it, so **every knob has exactly one literal default** — the one in
//! [`Config::default`].
//!
//! Knobs the public facade does not (yet) model — [`Durability`](super::Durability)
//! and the UDT registry — keep the engine's own defaults here and stay settable
//! through the existing `with_*` builders. Neither is live-divergent: no public
//! field claims to control them, so there is nothing to be dishonest about.

use std::path::PathBuf;

use super::WriteEngineConfig;
use crate::config::Config;
use crate::schema::TableSchema;

/// Narrow the public `u64` byte threshold to the engine's `usize`.
///
/// On a 32-bit target a `u64` config value can exceed `usize::MAX`. Clamping
/// (rather than an unchecked `as` truncation) keeps the requested "flush very
/// rarely" intent: a truncating cast could wrap a huge threshold down to a tiny
/// one and turn a bulk-load config into flush-per-write. The clamp is logged so
/// it is never silent.
fn clamp_threshold_bytes(bytes: u64, knob: &str) -> usize {
    match usize::try_from(bytes) {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(
                "config {} = {} bytes exceeds usize::MAX ({}) on this target; clamping",
                knob,
                bytes,
                usize::MAX
            );
            usize::MAX
        }
    }
}

impl WriteEngineConfig {
    /// Build a write-engine configuration from the public [`Config`] facade —
    /// the single source of truth for every write-path knob (issue #1697).
    ///
    /// Threads, in order: `config.storage.memtable_size_threshold` ->
    /// [`Self::memtable_flush_threshold`],
    /// `config.storage.memtable_hard_limit` -> [`Self::memtable_hard_limit`],
    /// and all three of
    /// `config.storage.compaction` -> [`Self::auto_compaction`] /
    /// [`Self::compaction_min_threshold`] / [`Self::compaction_max_threshold`].
    ///
    /// Threads `config.storage.memtable_hard_limit` too — the admission ceiling
    /// `check_admission` enforces. Callers needing a non-default durability mode
    /// or UDT registry chain the corresponding `with_*` builder afterwards; the
    /// public `Config` does not model those today.
    ///
    /// ```rust,ignore
    /// let mut config = cqlite_core::Config::default();
    /// config.storage.memtable_size_threshold = 8 * 1024 * 1024;
    /// config.storage.compaction.min_threshold = 2;
    /// let engine_config = WriteEngineConfig::from_config(&config, data, wal, schema);
    /// ```
    pub fn from_config(
        config: &Config,
        data_dir: PathBuf,
        wal_dir: PathBuf,
        schema: TableSchema,
    ) -> Self {
        Self {
            data_dir,
            wal_dir,
            memtable_flush_threshold: clamp_threshold_bytes(
                config.storage.memtable_size_threshold,
                "storage.memtable_size_threshold",
            ),
            memtable_hard_limit: clamp_threshold_bytes(
                config.storage.memtable_hard_limit,
                "storage.memtable_hard_limit",
            ),
            schema,
            durability: super::Durability::default(),
            udt_registry: None,
            auto_compaction: config.storage.compaction.auto_compaction,
            compaction_min_threshold: config.storage.compaction.min_threshold,
            compaction_max_threshold: config.storage.compaction.max_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompactionConfig;

    fn schema() -> TableSchema {
        crate::storage::write_engine::test_support::create_test_schema()
    }

    /// `new` must be `from_config` applied to `Config::default()`, so no knob
    /// has a second independent literal default (issue #1697).
    #[test]
    fn new_equals_from_config_of_default_config() {
        let data = PathBuf::from("/tmp/cqlite-bridge-data");
        let wal = PathBuf::from("/tmp/cqlite-bridge-wal");
        let via_new = WriteEngineConfig::new(data.clone(), wal.clone(), schema());
        let via_bridge = WriteEngineConfig::from_config(&Config::default(), data, wal, schema());

        assert_eq!(
            via_new.memtable_flush_threshold,
            via_bridge.memtable_flush_threshold
        );
        assert_eq!(via_new.memtable_hard_limit, via_bridge.memtable_hard_limit);
        assert_eq!(via_new.auto_compaction, via_bridge.auto_compaction);
        assert_eq!(
            via_new.compaction_min_threshold,
            via_bridge.compaction_min_threshold
        );
        assert_eq!(
            via_new.compaction_max_threshold,
            via_bridge.compaction_max_threshold
        );
    }

    /// The engine default MUST equal the public default — one literal per knob.
    #[test]
    fn engine_defaults_originate_from_public_config_defaults() {
        let defaults = Config::default();
        let cfg =
            WriteEngineConfig::new(PathBuf::from("/tmp/d"), PathBuf::from("/tmp/w"), schema());
        assert_eq!(
            cfg.memtable_flush_threshold as u64, defaults.storage.memtable_size_threshold,
            "engine flush threshold must come from Config::default()"
        );
        assert_eq!(
            cfg.compaction_min_threshold,
            defaults.storage.compaction.min_threshold
        );
        assert_eq!(
            cfg.compaction_max_threshold,
            defaults.storage.compaction.max_threshold
        );
        assert_eq!(
            cfg.memtable_hard_limit as u64, defaults.storage.memtable_hard_limit,
            "engine hard limit must come from Config::default()"
        );
        // #1697 kept the RUNNING values as the new public defaults.
        assert_eq!(defaults.storage.memtable_size_threshold, 64 * 1024 * 1024);
        assert_eq!(defaults.storage.memtable_hard_limit, 256 * 1024 * 1024);
    }

    /// Every knob the bridge owns must be carried through, not defaulted.
    #[test]
    fn from_config_threads_every_public_knob() {
        let mut config = Config::default();
        config.storage.memtable_size_threshold = 4096;
        config.storage.memtable_hard_limit = 8192;
        config.storage.compaction.auto_compaction = false;
        config.storage.compaction.min_threshold = 2;
        config.storage.compaction.max_threshold = 3;

        let cfg = WriteEngineConfig::from_config(
            &config,
            PathBuf::from("/tmp/d"),
            PathBuf::from("/tmp/w"),
            schema(),
        );
        assert_eq!(cfg.memtable_flush_threshold, 4096);
        assert_eq!(cfg.memtable_hard_limit, 8192);
        assert!(!cfg.auto_compaction);
        assert_eq!(cfg.compaction_min_threshold, 2);
        assert_eq!(cfg.compaction_max_threshold, 3);
    }

    /// A threshold above `usize::MAX` clamps instead of truncating. On a 64-bit
    /// target `u64::MAX` is the only value that can exceed `usize::MAX`, so the
    /// assertion is expressed against the target's own bound.
    #[test]
    fn oversized_threshold_clamps_and_never_truncates() {
        assert_eq!(clamp_threshold_bytes(u64::MAX, "test"), usize::MAX);
        assert_eq!(clamp_threshold_bytes(0, "test"), 0);
        assert_eq!(clamp_threshold_bytes(4096, "test"), 4096);
    }

    /// Every field of a public [`CompactionConfig`] reaches the engine through
    /// `from_config` — the ONE translation (#1697).
    ///
    /// The exhaustive destructuring is the point, not decoration: adding a knob
    /// to `CompactionConfig` breaks this test at COMPILE time, so it cannot be
    /// threaded into `from_config` and silently forgotten. An N-field checklist
    /// would keep passing at N. That property is also why the second, rival
    /// translation (`with_compaction_config`, zero production callers) was
    /// DELETED rather than tested: nothing could have asserted the two agreed.
    #[test]
    fn from_config_threads_every_compaction_field() {
        let mut config = Config::default();
        config.storage.compaction = CompactionConfig {
            auto_compaction: true,
            min_threshold: 7,
            max_threshold: 9,
        };
        let CompactionConfig {
            auto_compaction,
            min_threshold,
            max_threshold,
        } = config.storage.compaction.clone();

        let cfg = WriteEngineConfig::from_config(
            &config,
            PathBuf::from("/tmp/d"),
            PathBuf::from("/tmp/w"),
            schema(),
        );
        assert_eq!(cfg.auto_compaction, auto_compaction);
        assert_eq!(cfg.compaction_min_threshold, min_threshold);
        assert_eq!(cfg.compaction_max_threshold, max_threshold);
    }
}
