//! Compaction management for SSTable optimization

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::error::Error;
use crate::storage::manifest::Manifest;
use crate::storage::sstable::{SSTableId, SSTableManager};
use crate::{Config, Result};

/// Compaction strategy
#[derive(Debug, Clone)]
pub enum CompactionStrategy {
    /// Size-tiered compaction
    SizeTiered {
        /// Maximum number of SSTables per level
        max_sstables_per_level: u32,
        /// Size ratio threshold
        size_ratio_threshold: f64,
    },
    /// Leveled compaction
    Leveled {
        /// Maximum size per level
        max_level_size: u64,
        /// Level multiplier
        level_multiplier: u32,
    },
    /// Time-window compaction
    TimeWindow {
        /// Window size in hours
        window_size_hours: u32,
        /// Maximum window count
        max_windows: u32,
    },
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        CompactionStrategy::SizeTiered {
            max_sstables_per_level: 4,
            size_ratio_threshold: 0.5,
        }
    }
}

/// Compaction manager
pub struct CompactionManager {
    /// SSTable manager
    sstables: Arc<SSTableManager>,

    /// Manifest for metadata
    manifest: Arc<Manifest>,

    /// Compaction strategy
    strategy: CompactionStrategy,

    /// Configuration
    config: Config,

    /// Running compaction tasks
    running_tasks: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<Result<()>>>>>,

    /// Compaction statistics
    stats: Arc<RwLock<CompactionStats>>,

    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

impl CompactionManager {
    /// Create a new compaction manager
    pub async fn new(
        sstables: Arc<SSTableManager>,
        manifest: Arc<Manifest>,
        config: &Config,
    ) -> Result<Self> {
        let strategy = config.storage.compaction.strategy.clone();

        let manager = Self {
            sstables,
            manifest,
            strategy,
            config: config.clone(),
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CompactionStats::default())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        };

        // Start background compaction if enabled
        if config.storage.compaction.auto_compaction {
            manager.start_background_compaction().await?;
        }

        Ok(manager)
    }

    /// Start background compaction task
    async fn start_background_compaction(&self) -> Result<()> {
        let sstables = self.sstables.clone();
        let manifest = self.manifest.clone();
        let strategy = self.strategy.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(
                config.storage.compaction.check_interval_seconds,
            ));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::run_compaction_check(&sstables, &manifest, &strategy, &stats).await {
                            eprintln!("Compaction check failed: {}", e);
                        }
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }

            Ok(())
        });

        let mut tasks = self.running_tasks.write().await;
        tasks.insert("background_compaction".to_string(), handle);

        Ok(())
    }

    /// Run compaction check
    async fn run_compaction_check(
        sstables: &SSTableManager,
        manifest: &Manifest,
        strategy: &CompactionStrategy,
        stats: &Arc<RwLock<CompactionStats>>,
    ) -> Result<()> {
        let sstable_ids = sstables.list_sstables().await;

        if sstable_ids.len() < 2 {
            return Ok(()); // Nothing to compact
        }

        let candidates =
            Self::select_compaction_candidates(&sstable_ids, strategy, sstables).await?;

        if !candidates.is_empty() {
            Self::run_compaction(sstables, manifest, candidates, stats).await?;
        }

        Ok(())
    }

    /// Select SSTables for compaction
    async fn select_compaction_candidates(
        sstable_ids: &[SSTableId],
        strategy: &CompactionStrategy,
        sstables: &SSTableManager,
    ) -> Result<Vec<SSTableId>> {
        match strategy {
            CompactionStrategy::SizeTiered {
                max_sstables_per_level,
                ..
            } => {
                // Simple size-tiered: compact if we have more than max_sstables_per_level
                if sstable_ids.len() > *max_sstables_per_level as usize {
                    // Select oldest SSTables for compaction
                    let mut candidates = sstable_ids.to_vec();
                    candidates.sort_by(|a, b| a.filename().cmp(b.filename()));
                    candidates.truncate(2.min(candidates.len()));
                    Ok(candidates)
                } else {
                    Ok(Vec::new())
                }
            }
            CompactionStrategy::Leveled { max_level_size, .. } => {
                // Simple leveled: compact if total size exceeds threshold
                let total_size = Self::calculate_total_size(sstable_ids, sstables).await?;
                if total_size > *max_level_size {
                    // Select largest SSTables
                    let mut candidates = sstable_ids.to_vec();
                    candidates.sort_by(|a, b| b.filename().cmp(a.filename()));
                    candidates.truncate(2.min(candidates.len()));
                    Ok(candidates)
                } else {
                    Ok(Vec::new())
                }
            }
            CompactionStrategy::TimeWindow { max_windows, .. } => {
                // Time-window: compact if we have more than max_windows
                if sstable_ids.len() > *max_windows as usize {
                    let mut candidates = sstable_ids.to_vec();
                    candidates.sort_by(|a, b| a.filename().cmp(b.filename()));
                    candidates.truncate(2.min(candidates.len()));
                    Ok(candidates)
                } else {
                    Ok(Vec::new())
                }
            }
        }
    }

    /// Calculate total size of SSTables
    async fn calculate_total_size(
        sstable_ids: &[SSTableId],
        sstables: &SSTableManager,
    ) -> Result<u64> {
        let stats = sstables.stats().await?;
        Ok(stats.total_size)
    }

    /// Run compaction on selected SSTables
    async fn run_compaction(
        sstables: &SSTableManager,
        manifest: &Manifest,
        candidates: Vec<SSTableId>,
        stats: &Arc<RwLock<CompactionStats>>,
    ) -> Result<()> {
        if candidates.len() < 2 {
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        // Update stats - start compaction
        {
            let mut stats = stats.write().await;
            stats.compactions_started += 1;
            stats.sstables_compacted += candidates.len() as u64;
        }

        // Generate new SSTable ID
        let merged_id = SSTableId::new();

        // Perform the merge
        sstables
            .merge_sstables(candidates.clone(), merged_id.clone())
            .await?;

        // Update manifest
        manifest.record_compaction(&candidates, &merged_id).await?;

        // Update stats - complete compaction
        {
            let mut stats = stats.write().await;
            stats.compactions_completed += 1;
            stats.total_compaction_time += start_time.elapsed();
        }

        Ok(())
    }

    /// Trigger compaction manually
    pub async fn maybe_trigger_compaction(&self) -> Result<()> {
        if !self.config.storage.compaction.auto_compaction {
            return Ok(());
        }

        Self::run_compaction_check(&self.sstables, &self.manifest, &self.strategy, &self.stats)
            .await
    }

    /// Run manual compaction
    pub async fn run_compaction(&self) -> Result<()> {
        let sstable_ids = self.sstables.list_sstables().await;

        if sstable_ids.len() < 2 {
            return Ok(());
        }

        let candidates =
            Self::select_compaction_candidates(&sstable_ids, &self.strategy, &self.sstables)
                .await?;

        if !candidates.is_empty() {
            Self::run_compaction(&self.sstables, &self.manifest, candidates, &self.stats).await?;
        }

        Ok(())
    }

    /// Get compaction statistics
    pub async fn stats(&self) -> Result<CompactionStats> {
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }

    /// Shutdown the compaction manager
    pub async fn shutdown(&self) -> Result<()> {
        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for all tasks to complete
        let tasks = {
            let mut tasks = self.running_tasks.write().await;
            let mut handles = Vec::new();
            for (_, handle) in tasks.drain() {
                handles.push(handle);
            }
            handles
        };

        for handle in tasks {
            handle.await.map_err(|e| Error::storage(e.to_string()))??;
        }

        Ok(())
    }
}

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of compactions started
    pub compactions_started: u64,

    /// Number of compactions completed
    pub compactions_completed: u64,

    /// Number of SSTables compacted
    pub sstables_compacted: u64,

    /// Total time spent in compaction
    pub total_compaction_time: Duration,

    /// Average compaction time
    pub avg_compaction_time: Duration,
}

impl CompactionStats {
    /// Update average compaction time
    pub fn update_avg_time(&mut self) {
        if self.compactions_completed > 0 {
            self.avg_compaction_time =
                self.total_compaction_time / self.compactions_completed as u32;
        }
    }
}

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Whether auto-compaction is enabled
    pub auto_compaction: bool,

    /// Compaction strategy
    pub strategy: CompactionStrategy,

    /// Check interval in seconds
    pub check_interval_seconds: u64,

    /// Maximum concurrent compactions
    pub max_concurrent: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            auto_compaction: true,
            strategy: CompactionStrategy::default(),
            check_interval_seconds: 60,
            max_concurrent: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compaction_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let sstables = Arc::new(
            SSTableManager::new(temp_dir.path(), &config, platform.clone())
                .await
                .unwrap(),
        );
        let manifest = Arc::new(Manifest::new(temp_dir.path(), &config).await.unwrap());

        let compaction = CompactionManager::new(sstables, manifest, &config)
            .await
            .unwrap();
        let stats = compaction.stats().await.unwrap();

        assert_eq!(stats.compactions_started, 0);
        assert_eq!(stats.compactions_completed, 0);
    }

    #[tokio::test]
    async fn test_compaction_stats() {
        let mut stats = CompactionStats::default();

        stats.compactions_started = 5;
        stats.compactions_completed = 3;
        stats.total_compaction_time = Duration::from_secs(30);

        stats.update_avg_time();

        assert_eq!(stats.avg_compaction_time, Duration::from_secs(10));
    }

    #[test]
    fn test_compaction_strategy_default() {
        let strategy = CompactionStrategy::default();

        match strategy {
            CompactionStrategy::SizeTiered {
                max_sstables_per_level,
                size_ratio_threshold,
            } => {
                assert_eq!(max_sstables_per_level, 4);
                assert_eq!(size_ratio_threshold, 0.5);
            }
            _ => panic!("Expected SizeTiered strategy"),
        }
    }
}
