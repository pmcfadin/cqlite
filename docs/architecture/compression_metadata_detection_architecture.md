# Compression Metadata Detection Architecture

## Overview

This document outlines the architectural design for robust compression metadata detection in SSTable readers. The solution addresses critical flaws in filename pattern matching, component discovery, and error handling for compression metadata files.

## Problem Analysis

### Current Issues

1. **Rigid Filename Pattern Matching**: Hard-coded patterns fail with different naming conventions
2. **Missing Component Discovery**: No dynamic discovery of compression components
3. **Poor Error Differentiation**: Cannot distinguish missing files from malformed data
4. **Version Compatibility**: Breaks with different SSTable versions and naming schemes
5. **Performance Issues**: Inefficient file system scanning

### Critical Flaws in Existing Code

From `compression_info.rs` analysis:
- Uses hard-coded `-CompressionInfo.db` suffix detection
- No fallback mechanisms for alternative naming patterns
- Binary format detection is fragile and assumes specific layouts
- Limited error context for debugging

## Architectural Solution

### 1. Hierarchical Component Discovery System

#### Component Pattern Hierarchy
```rust
pub struct CompressionComponentPatterns {
    /// Primary patterns (most common)
    primary: Vec<CompressionPattern>,
    /// Fallback patterns (legacy/alternative)
    fallback: Vec<CompressionPattern>,
    /// Dynamic patterns (runtime discovery)
    dynamic: Vec<CompressionPattern>,
}

#[derive(Debug, Clone)]
pub struct CompressionPattern {
    /// Pattern description
    pub name: &'static str,
    /// Regex pattern for matching
    pub pattern: regex::Regex,
    /// Expected file suffix
    pub suffix: &'static str,
    /// Cassandra version compatibility
    pub versions: VersionRange,
    /// Priority (higher = preferred)
    pub priority: u8,
}
```

#### Pattern Examples
```rust
// Primary patterns (Cassandra 5.0+)
"^(?P<base>.+)-CompressionInfo\\.db$"
"^(?P<base>.+)\\.db-CompressionInfo$"

// Legacy patterns (Cassandra 3.x-4.x)
"^(?P<base>.+)-Compression\\.db$"
"^(?P<base>.+)\\.compression$"

// Alternative patterns (different distributions)
"^(?P<base>.+)-ci\\.db$"
"^(?P<base>.+)\\.cidx$"
```

### 2. Dynamic Discovery Algorithm

#### Multi-Stage Discovery Process
```rust
pub struct CompressionDiscoveryEngine {
    /// Cached patterns by directory
    pattern_cache: Arc<Mutex<HashMap<PathBuf, Vec<CompressionPattern>>>>,
    /// File system scanner
    fs_scanner: FileSystemScanner,
    /// Pattern matcher
    pattern_matcher: PatternMatcher,
}

impl CompressionDiscoveryEngine {
    /// Discover compression components with fallback
    pub async fn discover_compression_components(
        &self,
        sstable_path: &Path,
    ) -> Result<CompressionComponentInfo> {
        // Stage 1: Try known patterns
        if let Ok(info) = self.try_known_patterns(sstable_path).await {
            return Ok(info);
        }

        // Stage 2: Directory scanning with pattern detection
        if let Ok(info) = self.scan_directory_patterns(sstable_path).await {
            return Ok(info);
        }

        // Stage 3: Heuristic discovery
        if let Ok(info) = self.heuristic_discovery(sstable_path).await {
            return Ok(info);
        }

        // Stage 4: Return appropriate error
        Err(Error::compression_not_found(sstable_path))
    }
}
```

#### Smart Pattern Matching
```rust
pub struct PatternMatcher {
    /// Compiled patterns by priority
    patterns: Vec<CompressionPattern>,
    /// Runtime statistics
    stats: PatternStats,
}

impl PatternMatcher {
    /// Match file against all patterns
    pub fn match_compression_file(&self, file_path: &Path) -> Option<CompressionMatch> {
        let filename = file_path.file_name()?.to_str()?;

        // Try patterns in priority order
        for pattern in &self.patterns {
            if let Some(captures) = pattern.pattern.captures(filename) {
                return Some(CompressionMatch {
                    pattern: pattern.clone(),
                    base_name: captures.name("base")?.as_str().to_string(),
                    confidence: calculate_confidence(&pattern, file_path),
                });
            }
        }
        None
    }
}
```

### 3. Robust Error Handling Framework

#### Error Classification System
```rust
#[derive(Debug, Clone)]
pub enum CompressionDetectionError {
    /// File not found - no compression files discovered
    NotFound {
        sstable_path: PathBuf,
        searched_patterns: Vec<String>,
        directory_contents: Vec<String>,
    },

    /// File found but corrupted/malformed
    Corrupted {
        compression_path: PathBuf,
        error_type: CorruptionType,
        parsing_error: String,
        hex_dump: Option<String>,
    },

    /// Multiple candidates found - ambiguous
    Ambiguous {
        sstable_path: PathBuf,
        candidates: Vec<CompressionCandidate>,
    },

    /// Version mismatch
    VersionMismatch {
        compression_path: PathBuf,
        detected_version: Option<CassandraVersion>,
        expected_version: CassandraVersion,
    },

    /// Permission/access issues
    AccessDenied {
        compression_path: PathBuf,
        error: std::io::Error,
    },
}

#[derive(Debug, Clone)]
pub enum CorruptionType {
    /// Invalid binary format
    InvalidFormat,
    /// CRC mismatch
    CrcMismatch,
    /// Truncated file
    Truncated,
    /// Unknown algorithm
    UnknownAlgorithm,
}
```

#### Error Context Enhancement
```rust
impl CompressionDetectionError {
    /// Generate detailed diagnostic report
    pub fn diagnostic_report(&self) -> DiagnosticReport {
        match self {
            Self::NotFound { sstable_path, searched_patterns, directory_contents } => {
                DiagnosticReport {
                    error_type: "CompressionNotFound".to_string(),
                    summary: format!("No compression metadata found for {}", sstable_path.display()),
                    details: vec![
                        format!("Searched patterns: {}", searched_patterns.join(", ")),
                        format!("Directory contents: {}", directory_contents.join(", ")),
                        format!("Suggestion: Check if SSTable is compressed or use different patterns"),
                    ],
                    suggested_actions: vec![
                        "Verify SSTable is actually compressed".to_string(),
                        "Check for alternative compression file naming".to_string(),
                        "Use --force-uncompressed flag if appropriate".to_string(),
                    ],
                }
            },
            // ... other error types
        }
    }
}
```

### 4. Flexible Pattern Matching Strategy

#### Adaptive Pattern Learning
```rust
pub struct AdaptivePatternLearner {
    /// Known successful patterns
    learned_patterns: HashMap<PathBuf, CompressionPattern>,
    /// Pattern success statistics
    pattern_stats: HashMap<String, PatternSuccess>,
}

impl AdaptivePatternLearner {
    /// Learn from successful discoveries
    pub fn learn_pattern(&mut self, directory: &Path, pattern: CompressionPattern) {
        self.learned_patterns.insert(directory.to_path_buf(), pattern.clone());

        let stats = self.pattern_stats.entry(pattern.name.to_string()).or_default();
        stats.success_count += 1;
        stats.last_success = Some(std::time::Instant::now());
    }

    /// Get preferred patterns for directory
    pub fn get_preferred_patterns(&self, directory: &Path) -> Vec<CompressionPattern> {
        let mut patterns = Vec::new();

        // Add learned pattern for this directory
        if let Some(learned) = self.learned_patterns.get(directory) {
            patterns.push(learned.clone());
        }

        // Add patterns sorted by success rate
        let mut sorted_patterns: Vec<_> = self.pattern_stats.iter().collect();
        sorted_patterns.sort_by(|a, b| b.1.success_rate().partial_cmp(&a.1.success_rate()).unwrap());

        for (name, _stats) in sorted_patterns.into_iter().take(5) {
            if let Some(pattern) = self.get_pattern_by_name(name) {
                patterns.push(pattern);
            }
        }

        patterns
    }
}
```

#### Version-Aware Pattern Selection
```rust
pub struct VersionAwarePatternSelector {
    /// Patterns by Cassandra version
    version_patterns: HashMap<CassandraVersion, Vec<CompressionPattern>>,
}

impl VersionAwarePatternSelector {
    /// Select patterns based on detected SSTable version
    pub fn select_patterns(&self, sstable_version: CassandraVersion) -> Vec<CompressionPattern> {
        let mut patterns = Vec::new();

        // Exact version match
        if let Some(exact) = self.version_patterns.get(&sstable_version) {
            patterns.extend(exact.clone());
        }

        // Compatible version patterns
        for (version, version_patterns) in &self.version_patterns {
            if version.is_compatible_with(&sstable_version) {
                patterns.extend(version_patterns.clone());
            }
        }

        // Sort by priority and compatibility score
        patterns.sort_by(|a, b| {
            b.priority.cmp(&a.priority)
                .then_with(|| {
                    b.compatibility_score(&sstable_version)
                        .partial_cmp(&a.compatibility_score(&sstable_version))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        patterns
    }
}
```

### 5. Performance-Optimized File System Scanning

#### Cached Directory Scanner
```rust
pub struct CachedDirectoryScanner {
    /// Directory content cache
    dir_cache: Arc<Mutex<LruCache<PathBuf, DirectoryContents>>>,
    /// Cache expiry time
    cache_ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct DirectoryContents {
    /// All files in directory
    files: Vec<PathBuf>,
    /// Potential compression files
    compression_candidates: Vec<PathBuf>,
    /// Cache timestamp
    cached_at: Instant,
}

impl CachedDirectoryScanner {
    /// Scan directory with caching
    pub async fn scan_directory(&self, dir_path: &Path) -> Result<DirectoryContents> {
        // Check cache first
        if let Some(cached) = self.get_cached_contents(dir_path).await {
            if !cached.is_expired(self.cache_ttl) {
                return Ok(cached);
            }
        }

        // Perform actual scan
        let contents = self.scan_directory_impl(dir_path).await?;

        // Cache results
        self.cache_contents(dir_path, &contents).await;

        Ok(contents)
    }

    /// Smart filtering of compression candidates
    async fn scan_directory_impl(&self, dir_path: &Path) -> Result<DirectoryContents> {
        let mut dir = tokio::fs::read_dir(dir_path).await?;
        let mut files = Vec::new();
        let mut compression_candidates = Vec::new();

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                files.push(path.clone());

                // Quick heuristic check for compression files
                if self.is_potential_compression_file(&path) {
                    compression_candidates.push(path);
                }
            }
        }

        Ok(DirectoryContents {
            files,
            compression_candidates,
            cached_at: Instant::now(),
        })
    }

    /// Fast heuristic check
    fn is_potential_compression_file(&self, path: &Path) -> bool {
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            // Fast string checks before regex
            filename.contains("ompression") ||
            filename.contains("-ci.") ||
            filename.ends_with(".cidx") ||
            filename.ends_with("Compression.db")
        } else {
            false
        }
    }
}
```

#### Parallel Pattern Matching
```rust
pub struct ParallelPatternMatcher {
    /// Thread pool for pattern matching
    thread_pool: Arc<tokio::task::JoinSet<PatternMatchResult>>,
}

impl ParallelPatternMatcher {
    /// Match multiple files against patterns in parallel
    pub async fn match_files_parallel(
        &self,
        files: Vec<PathBuf>,
        patterns: Vec<CompressionPattern>,
    ) -> Vec<CompressionMatch> {
        let mut handles = Vec::new();

        for file in files {
            for pattern in &patterns {
                let file_clone = file.clone();
                let pattern_clone = pattern.clone();

                let handle = tokio::spawn(async move {
                    Self::match_single_file(file_clone, pattern_clone).await
                });

                handles.push(handle);
            }
        }

        // Collect results
        let mut matches = Vec::new();
        for handle in handles {
            if let Ok(Some(match_result)) = handle.await {
                matches.push(match_result);
            }
        }

        // Sort by confidence
        matches.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
        matches
    }
}
```

### 6. Integration Strategy

#### SSTable Reader Integration
```rust
impl SSTableReader {
    /// Enhanced compression component loading
    async fn load_compression_component(&mut self) -> Result<Option<CompressionInfo>> {
        let discovery_engine = CompressionDiscoveryEngine::new(self.config.clone());

        match discovery_engine.discover_compression_components(&self.data_file_path).await {
            Ok(compression_info) => {
                self.metrics.compression_enabled = true;
                self.metrics.compression_algorithm = compression_info.algorithm.clone();
                Ok(Some(compression_info))
            },
            Err(CompressionDetectionError::NotFound { .. }) => {
                // Not an error - SSTable might not be compressed
                self.metrics.compression_enabled = false;
                Ok(None)
            },
            Err(other_error) => {
                // Log detailed diagnostic
                log::warn!("Compression detection failed: {}", other_error.diagnostic_report());
                Err(other_error.into())
            }
        }
    }
}
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)
1. Implement `CompressionPattern` and related types
2. Create `PatternMatcher` with regex compilation
3. Basic error classification system
4. Simple directory scanning

### Phase 2: Discovery Engine (Week 2)
1. Implement `CompressionDiscoveryEngine`
2. Multi-stage discovery algorithm
3. Pattern priority and confidence scoring
4. Integration with existing compression_info.rs

### Phase 3: Advanced Features (Week 3)
1. Adaptive pattern learning
2. Version-aware pattern selection
3. Performance optimizations (caching, parallel matching)
4. Comprehensive error diagnostics

### Phase 4: Integration & Testing (Week 4)
1. SSTable reader integration
2. Comprehensive test suite
3. Performance benchmarking
4. Documentation and examples

## Configuration

### Pattern Configuration
```toml
[compression.discovery]
# Enable adaptive pattern learning
adaptive_learning = true

# Cache directory scan results
cache_directory_scans = true
cache_ttl_seconds = 300

# Pattern matching timeout
pattern_match_timeout_ms = 5000

# Enable parallel pattern matching
parallel_matching = true
max_parallel_tasks = 4

[compression.patterns]
# Custom patterns (addition to built-in)
custom_patterns = [
    { name = "custom1", pattern = "^(.+)-custom-compression\\.db$", suffix = "-custom-compression.db", priority = 100 },
]

# Disable specific built-in patterns
disabled_patterns = ["legacy_v3"]
```

## Backward Compatibility

### Migration Strategy
1. **Gradual Rollout**: New discovery system runs alongside existing logic
2. **Fallback Mode**: Falls back to original logic if new system fails
3. **Configuration Override**: Allow disabling new features via config
4. **Metrics Collection**: Track success rates of different approaches

### Legacy Support
```rust
pub struct LegacyCompressionDetector {
    /// Original detection logic
    original_detector: OriginalDetector,
}

impl LegacyCompressionDetector {
    /// Fallback to original detection method
    pub fn detect_legacy(&self, sstable_path: &Path) -> Result<Option<CompressionInfo>> {
        // Use original hard-coded pattern matching
        let compression_path = sstable_path.with_extension("db-CompressionInfo");
        if compression_path.exists() {
            CompressionInfo::parse(&std::fs::read(compression_path)?)
        } else {
            Ok(None)
        }
    }
}
```

## Success Metrics

### Performance Targets
- **Discovery Time**: < 10ms for cached directories
- **Pattern Matching**: < 1ms per file
- **Memory Usage**: < 1MB for pattern cache
- **Success Rate**: > 99% for standard Cassandra installations

### Quality Metrics
- **False Positives**: < 0.1% (files incorrectly identified as compression)
- **False Negatives**: < 0.01% (compression files missed)
- **Error Context**: 100% of errors include actionable diagnostic information

## Testing Strategy

### Test Categories
1. **Unit Tests**: Individual component testing
2. **Integration Tests**: Full discovery workflow
3. **Performance Tests**: Stress testing with large directories
4. **Compatibility Tests**: Different Cassandra versions and configurations
5. **Regression Tests**: Ensure no existing functionality breaks

### Test Data
- Compression files from Cassandra 3.7, 3.11, 4.0, 4.1, 5.0
- Alternative naming conventions from different distributions
- Corrupted and malformed compression files
- Edge cases (very large/small files, special characters in names)

This architecture provides a robust, extensible, and performant solution for compression metadata detection that addresses all identified issues while maintaining backward compatibility and excellent error reporting.