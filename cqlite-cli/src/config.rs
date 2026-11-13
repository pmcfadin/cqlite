use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[path = "config_removed_keys.rs"]
pub mod removed_keys;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub default_database: Option<PathBuf>,
    #[serde(default)]
    pub output: OutputSettings,
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub repl: ReplConfig,
    pub data_directory: Option<PathBuf>,
    pub default_keyspace: Option<String>,

    // Legacy fields for backward compatibility
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_history: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_completion: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_timing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_paging: Option<bool>,
    #[serde(default)]
    pub no_color: bool,

    // M2 one-shot mode fields
    /// Schema file paths (supports multiple sources)
    #[serde(default)]
    pub schema_paths: Vec<PathBuf>,

    /// One-shot execution query (from -e flag)
    #[serde(skip)]
    pub execution_query: Option<String>,

    /// One-shot execution file (from -f flag)
    #[serde(skip)]
    pub execution_file: Option<PathBuf>,

    /// Output mode for query results (table/json/csv)
    pub output_mode: Option<String>,

    /// Maximum rows for queries
    pub query_limit: Option<usize>,

    // Version hint resolution (Issue #130)
    /// Cassandra version hint from CLI flag (for precedence chain)
    #[serde(skip)]
    pub cassandra_version: Option<String>,

    /// Resolved version information (computed async after config load)
    /// TODO(Issue #130): Used by :status meta-command (not yet implemented)
    #[serde(skip)]
    #[allow(dead_code)]
    pub resolved_version: Option<cqlite_core::version_hints::ResolvedVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSettings {
    pub max_rows: Option<usize>,
    pub colors: bool,
}

impl Default for OutputSettings {
    fn default() -> Self {
        Self {
            max_rows: Some(1000),
            colors: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub query_timeout_ms: u64,
    pub memory_limit_mb: Option<u64>,
    pub cache_size_mb: u64,
    /// Maximum logical bytes materialized by a query or `explain` rendering (R9 of the
    /// forensics-explain spec: explain is governed by the SAME `max_result_bytes` budget as the
    /// query surface). Maps to `query.max_result_bytes` in `core_config.rs`.
    #[serde(default = "default_max_result_bytes")]
    pub max_result_bytes: u64,
}

fn default_max_result_bytes() -> u64 {
    cqlite_core::config::DEFAULT_MAX_RESULT_BYTES
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            query_timeout_ms: 30000,
            memory_limit_mb: None,
            cache_size_mb: 64,
            max_result_bytes: default_max_result_bytes(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: Option<PathBuf>,
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: None,
            format: LogFormat::Pretty,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LogFormat {
    Plain,
    Json,
    #[default]
    Pretty,
}

/// OpenTelemetry / observability settings (Issue #1033, Epic #1031).
///
/// Mirrors `LoggingConfig` in spirit: a file/env/flag-driven section that the
/// precedence chain (user -> project -> --config -> env -> flag) populates and
/// `main` maps into `cqlite_core::observability::ObservabilityConfig` before
/// calling `observability::init`. All fields are optional so that an unset
/// field falls through to the core defaults (disabled, localhost:4317, grpc,
/// service name "cqlite", full sampling).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObservabilityConfig {
    /// Master enable switch. When `None`, defers to the core default (disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// OTLP collector endpoint (gRPC endpoint or HTTP base URL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Wire protocol: `grpc` or `http`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// `service.name` resource attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// `service.version` resource attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>,
    /// Trace-ID-ratio sampling probability in `[0.0, 1.0]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_ratio: Option<f64>,
    /// Exporter export timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl ObservabilityConfig {
    /// Map the merged CLI observability settings into the core
    /// `cqlite_core::observability::ObservabilityConfig`.
    ///
    /// Starts from the core defaults and overrides only the fields that were
    /// explicitly set somewhere in the precedence chain. An unrecognised
    /// protocol string is ignored (the core default `grpc` is kept) rather than
    /// erroring, matching the foundation's lenient `from_env` semantics.
    pub fn to_core(&self) -> cqlite_core::observability::ObservabilityConfig {
        use cqlite_core::observability::{ObservabilityConfig as CoreObs, OtelProtocol};
        use std::time::Duration;

        let mut builder = CoreObs::builder();
        if let Some(enabled) = self.enabled {
            builder = builder.enabled(enabled);
        }
        if let Some(ref endpoint) = self.endpoint {
            builder = builder.endpoint(endpoint.clone());
        }
        if let Some(ref protocol) = self.protocol {
            if let Some(p) = OtelProtocol::parse(protocol) {
                builder = builder.protocol(p);
            }
        }
        if let Some(ref name) = self.service_name {
            builder = builder.service_name(name.clone());
        }
        if let Some(ref version) = self.service_version {
            builder = builder.service_version(version.clone());
        }
        if let Some(ratio) = self.sampling_ratio {
            builder = builder.sampling_ratio(ratio);
        }
        if let Some(ms) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(ms));
        }
        builder.build()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplConfig {
    pub enable_history: bool,
    pub enable_completion: bool,
    pub enable_colors: bool,
    pub show_timing: bool,
    pub page_size: usize,
    pub enable_paging: bool,
    pub max_history_size: usize,
    pub prompt: String,
    pub prompt_continuation: String,
    pub history_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_database: None,
            output: OutputSettings::default(),
            performance: PerformanceConfig::default(),
            logging: LoggingConfig::default(),
            observability: ObservabilityConfig::default(),
            repl: ReplConfig::default(),
            data_directory: None,
            default_keyspace: None,
            enable_history: None,
            enable_completion: None,
            show_timing: None,
            page_size: None,
            enable_paging: None,
            no_color: false,
            schema_paths: Vec::new(),
            execution_query: None,
            execution_file: None,
            output_mode: None,
            query_limit: None,
            cassandra_version: None,
            resolved_version: None,
        }
    }
}

impl Config {
    pub fn load(config_path: Option<PathBuf>, cli: &crate::cli_types::Cli) -> Result<Self> {
        let mut builder = ConfigBuilder::from_defaults()
            .with_user_config()? // 1. User config (lowest precedence)
            .with_project_config()?; // 2. Project config (overrides user)

        // 3. Explicit --config flag (overrides discovered configs)
        if let Some(path) = config_path {
            builder = builder.with_explicit_config(path)?;
        }

        // 4. Environment variables (override files)
        // 5. CLI flags (highest precedence)
        Ok(builder.with_env()?.with_flags(cli).build())
    }

    /// Resolve Cassandra version using precedence chain (Issue #130)
    ///
    /// This method implements the version hint precedence:
    /// 1. User override (--cassandra-version flag)
    /// 2. SSTable metadata
    /// 3. metadata.yml
    /// 4. Unknown
    ///
    /// # Arguments
    ///
    /// * `platform` - Platform abstraction for file I/O
    ///
    /// # Errors
    ///
    /// Returns an error only for fatal I/O errors. Missing metadata is not an error.
    ///
    /// TODO(Issue #130): Used by :status meta-command (not yet implemented)
    #[allow(dead_code)]
    pub async fn resolve_version(
        &mut self,
        platform: std::sync::Arc<cqlite_core::Platform>,
    ) -> Result<()> {
        use cqlite_core::version_hints::VersionHintResolver;
        use std::path::PathBuf;

        // Use data_directory if available, otherwise use current directory
        let default_path = PathBuf::from(".");
        let data_dir = self
            .data_directory
            .as_deref()
            .unwrap_or(default_path.as_path());

        self.resolved_version = Some(
            VersionHintResolver::resolve(self.cassandra_version.clone(), data_dir, platform)
                .await?,
        );

        Ok(())
    }

    /// Get resolved version information for display/diagnostics
    ///
    /// Returns `None` if version resolution has not been performed yet.
    /// Call `resolve_version()` first to populate this field.
    ///
    /// TODO(Issue #130): Used by :status meta-command (not yet implemented)
    #[allow(dead_code)]
    pub fn version_info(&self) -> Option<&cqlite_core::version_hints::ResolvedVersion> {
        self.resolved_version.as_ref()
    }

    /// Get version string for display (returns "unknown" if not resolved)
    ///
    /// TODO(Issue #130): Used by :status meta-command (not yet implemented)
    #[allow(dead_code)]
    pub fn version_string(&self) -> String {
        self.resolved_version
            .as_ref()
            .map(|rv| rv.version_or_unknown().to_string())
            .unwrap_or_else(|| "not resolved".to_string())
    }

    /// Get version source description
    ///
    /// TODO(Issue #130): Used by :status meta-command (not yet implemented)
    #[allow(dead_code)]
    pub fn version_source(&self) -> String {
        self.resolved_version
            .as_ref()
            .map(|rv| rv.source.description().to_string())
            .unwrap_or_else(|| "not resolved".to_string())
    }

    fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let (config, warning) = Self::parse_with_removed_key_report(path, &content)?;
        if let Some(warning) = warning {
            eprintln!("{warning}");
        }
        Ok(config)
    }

    /// Deserialize `content` as `path`'s format, returning the loaded config and
    /// the deprecation warning naming every REMOVED key it still sets (#1696).
    ///
    /// Deserialize first, scan only on success, so a warning never accompanies a
    /// document that is not a `Config`. The text asserts NOTHING about whether the
    /// load succeeds — later stages (`to_core_config`) still reject files; see
    /// `removed_keys` (#1696 r5 F1). `pub` so a test asserts this on the real path.
    pub fn parse_with_removed_key_report(
        path: &Path,
        content: &str,
    ) -> Result<(Self, Option<String>)> {
        let config: Config = match path.extension().and_then(|ext| ext.to_str()) {
            Some("toml") => {
                toml::from_str(content).with_context(|| "Failed to parse TOML config")?
            }
            Some("yaml") | Some("yml") => {
                serde_yaml::from_str(content).with_context(|| "Failed to parse YAML config")?
            }
            Some("json") => {
                serde_json::from_str(content).with_context(|| "Failed to parse JSON config")?
            }
            _ => return Err(anyhow::anyhow!("Unsupported config file format")),
        };

        // Only now that the load has succeeded: serde discarded the removed keys
        // silently, so the retained raw document is the only place they can still
        // be seen. The posture is parse-and-ignore PLUS a named warning, never
        // `deny_unknown_fields` — our own shipped example named these keys. See
        // the `removed_keys` submodule for the full rationale.
        let warning = removed_keys::warning_for_file(path, content);
        Ok((config, warning))
    }

    #[allow(dead_code)]
    fn load_default() -> Result<Self> {
        // Look for config file in standard locations
        let config_paths = [
            "cqlite.toml",
            "cqlite.yaml",
            "cqlite.yml",
            "cqlite.json",
            ".cqlite.toml",
            ".cqlite.yaml",
            ".cqlite.yml",
            ".cqlite.json",
        ];

        for path in &config_paths {
            if Path::new(path).exists() {
                return Self::load_from_file(Path::new(path));
            }
        }

        // Also check XDG config directory
        if let Some(config_dir) = dirs::config_dir() {
            let xdg_paths = [
                config_dir.join("cqlite").join("config.toml"),
                config_dir.join("cqlite").join("config.yaml"),
                config_dir.join("cqlite").join("config.yml"),
                config_dir.join("cqlite").join("config.json"),
            ];

            for path in &xdg_paths {
                if path.exists() {
                    return Self::load_from_file(path);
                }
            }
        }

        // Return default config if no file found
        Ok(Self::default())
    }

    #[allow(dead_code)]
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let content = match path.extension().and_then(|ext| ext.to_str()) {
            Some("toml") => toml::to_string_pretty(self)
                .with_context(|| "Failed to serialize config to TOML")?,
            Some("yaml") | Some("yml") => {
                serde_yaml::to_string(self).with_context(|| "Failed to serialize config to YAML")?
            }
            Some("json") => serde_json::to_string_pretty(self)
                .with_context(|| "Failed to serialize config to JSON")?,
            _ => return Err(anyhow::anyhow!("Unsupported config file format")),
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create config directory: {}", parent.display())
            })?;
        }

        fs::write(path, content)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;

        Ok(())
    }
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            enable_history: true,
            enable_completion: true,
            enable_colors: true,
            show_timing: false,
            page_size: 50,
            enable_paging: true,
            max_history_size: 1000,
            prompt: "cqlite> ".to_string(),
            prompt_continuation: "    -> ".to_string(),
            history_file: None,
        }
    }
}

/// Configuration for table formatter output behavior
///
/// This struct controls how query results are formatted and displayed,
/// including color support, row limits, pagination settings, and output destination.
#[derive(Debug, Clone)]
pub struct OutputConfig {
    /// Whether to enable colored output in table formatting.
    /// This is the inverse of the `--no-color` CLI flag.
    /// When `true`, output will include ANSI color codes for better readability.
    pub color_enabled: bool,

    /// Maximum number of rows to display in query results.
    /// When `None`, all rows will be displayed.
    /// This can be used to prevent overwhelming output from large result sets.
    pub limit: Option<usize>,

    /// Number of rows per page for pagination.
    /// When `None`, pagination is disabled and all rows are shown at once.
    /// Default is 50 rows per page, matching cqlsh behavior.
    #[allow(dead_code)]
    pub page_size: Option<usize>,

    /// Output target (stdout or file path).
    /// When `Stdout`, output is written to standard output.
    /// When `File(path)`, output is written atomically to the specified file.
    pub target: crate::output::OutputTarget,

    /// Whether to overwrite existing files when writing to a file target.
    /// Only relevant when `target` is `File`.
    pub overwrite: bool,
}

impl OutputConfig {
    /// Create a new OutputConfig from resolved Config and CLI flags
    ///
    /// This method respects the precedence chain: CLI flags > env vars > config file > defaults.
    /// The `Config` object passed in has already resolved this chain via ConfigBuilder.
    ///
    /// # Arguments
    ///
    /// * `config` - The resolved Config object containing env/file/default values
    /// * `no_color_flag` - The `--no-color` CLI flag (if present, overrides config)
    /// * `limit_flag` - The `--limit` CLI flag (if present, overrides config)
    /// * `page_size_flag` - The `--page-size` CLI flag (if present, overrides config)
    /// * `output_flag` - The `--output` CLI flag for file destination
    /// * `overwrite_flag` - The `--overwrite` CLI flag for overwriting existing files
    ///
    /// # Precedence
    ///
    /// - `color_enabled`: --no-color flag > CQLITE_NO_COLOR env > config.output.colors > default (true)
    /// - `limit`: --limit flag > CQLITE_LIMIT env > config.query_limit > default (None)
    /// - `page_size`: --page-size flag > CQLITE_PAGE_SIZE env > config.repl.page_size > default (50)
    /// - `target`: --output flag > CQLITE_OUTPUT env > default (Stdout)
    /// - `overwrite`: --overwrite flag > default (false)
    ///
    /// # Examples
    ///
    /// ```
    /// use cqlite_cli::config::{Config, OutputConfig};
    /// use cqlite_cli::cli_types::Cli;
    /// use clap::Parser;
    ///
    /// // Create config with defaults
    /// let cli = Cli::parse_from(&["cqlite"]);
    /// let config = Config::load(None, &cli).unwrap();
    /// let output = OutputConfig::from_cli(&config, false, None, None, None, false);
    /// assert!(output.color_enabled);
    /// assert_eq!(output.page_size, Some(50));
    ///
    /// // CLI flag overrides config
    /// let output = OutputConfig::from_cli(&config, true, Some(100), Some(25), None, false);
    /// assert!(!output.color_enabled);
    /// assert_eq!(output.limit, Some(100));
    /// assert_eq!(output.page_size, Some(25));
    /// ```
    pub fn from_cli(
        config: &Config,
        no_color_flag: bool,
        limit_flag: Option<usize>,
        page_size_flag: Option<usize>,
        output_flag: Option<std::path::PathBuf>,
        overwrite_flag: bool,
    ) -> Self {
        use crate::output::OutputTarget;

        Self {
            // CLI flag overrides config value
            color_enabled: if no_color_flag {
                false
            } else {
                config.output.colors
            },
            // CLI flag overrides config.query_limit (which already has env/file/default precedence)
            limit: limit_flag.or(config.query_limit),
            // CLI flag overrides config.repl.page_size (which already has env/file/default precedence)
            page_size: page_size_flag.or(Some(config.repl.page_size)),
            // Output target from CLI flag
            target: output_flag
                .map(OutputTarget::File)
                .unwrap_or(OutputTarget::Stdout),
            // Overwrite flag
            overwrite: overwrite_flag,
        }
    }
}

impl Default for OutputConfig {
    /// Default output configuration
    ///
    /// Returns an OutputConfig with:
    /// - `color_enabled`: `true` (colors enabled by default)
    /// - `limit`: `None` (no row limit)
    /// - `page_size`: `Some(50)` (50 rows per page, matching cqlsh)
    /// - `target`: `Stdout` (write to standard output)
    /// - `overwrite`: `false` (don't overwrite existing files)
    fn default() -> Self {
        Self {
            color_enabled: true,
            limit: None,
            page_size: Some(50),
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        }
    }
}

/// Merge two configs with partial override semantics
/// Only non-default values from overlay replace base values
fn merge_partial_config(base: Config, overlay: Config) -> Config {
    // Determine final no_color value (true if either is true)
    let final_no_color = overlay.no_color || base.no_color;

    // Determine output colors
    // Priority: if no_color is true, colors must be false
    // Otherwise, use overlay value (which has defaults applied during TOML parsing)
    let final_output_colors = if final_no_color {
        false
    } else {
        overlay.output.colors
    };

    Config {
        // Use overlay value if present, otherwise keep base
        data_directory: overlay.data_directory.or(base.data_directory),
        default_keyspace: overlay.default_keyspace.or(base.default_keyspace),

        // For schema_paths, use overlay if non-empty
        schema_paths: if overlay.schema_paths.is_empty() {
            base.schema_paths
        } else {
            overlay.schema_paths
        },

        // Output mode
        output_mode: overlay.output_mode.or(base.output_mode),

        // Numeric limits
        query_limit: overlay.query_limit.or(base.query_limit),

        // Nested structs - merge carefully
        output: OutputSettings {
            max_rows: overlay.output.max_rows.or(base.output.max_rows),
            colors: final_output_colors,
        },
        repl: ReplConfig {
            enable_history: overlay.repl.enable_history,
            enable_completion: overlay.repl.enable_completion,
            enable_colors: overlay.repl.enable_colors,
            show_timing: overlay.repl.show_timing,
            page_size: overlay.repl.page_size,
            enable_paging: overlay.repl.enable_paging,
            max_history_size: overlay.repl.max_history_size,
            prompt: overlay.repl.prompt,
            prompt_continuation: overlay.repl.prompt_continuation,
            history_file: overlay.repl.history_file.or(base.repl.history_file),
        },
        performance: overlay.performance,
        logging: overlay.logging,
        observability: ObservabilityConfig {
            enabled: overlay.observability.enabled.or(base.observability.enabled),
            endpoint: overlay
                .observability
                .endpoint
                .or(base.observability.endpoint),
            protocol: overlay
                .observability
                .protocol
                .or(base.observability.protocol),
            service_name: overlay
                .observability
                .service_name
                .or(base.observability.service_name),
            service_version: overlay
                .observability
                .service_version
                .or(base.observability.service_version),
            sampling_ratio: overlay
                .observability
                .sampling_ratio
                .or(base.observability.sampling_ratio),
            timeout_ms: overlay
                .observability
                .timeout_ms
                .or(base.observability.timeout_ms),
        },

        // Legacy fields (backward compat)
        enable_history: overlay.enable_history.or(base.enable_history),
        enable_completion: overlay.enable_completion.or(base.enable_completion),
        show_timing: overlay.show_timing.or(base.show_timing),
        page_size: overlay.page_size.or(base.page_size),
        enable_paging: overlay.enable_paging.or(base.enable_paging),
        no_color: final_no_color,

        // Skip serialization fields
        execution_query: base.execution_query,
        execution_file: base.execution_file,
        cassandra_version: overlay.cassandra_version.or(base.cassandra_version),
        resolved_version: base.resolved_version,

        // Deprecated - keep base
        default_database: base.default_database,
    }
}

/// Builder for Config with precedence: flags > env > file > defaults
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Start with default configuration
    pub fn from_defaults() -> Self {
        Self {
            config: Config::default(),
        }
    }

    /// Layer config file (overrides defaults)
    ///
    /// Deprecated: Use `with_explicit_config()` instead for --config flag handling
    #[allow(dead_code)]
    pub fn with_file(mut self, path: Option<PathBuf>) -> Result<Self> {
        if let Some(p) = path {
            let loaded = Config::load_from_file(&p)?;
            // Merge loaded config, preserving defaults for unset fields
            self.config = loaded;
        }
        Ok(self)
    }

    /// Layer user config (overrides defaults)
    pub fn with_user_config(mut self) -> Result<Self> {
        if let Some(user_path) = Self::user_config_path() {
            if user_path.exists() {
                let loaded = Config::load_from_file(&user_path).with_context(|| {
                    format!("Failed to load user config: {}", user_path.display())
                })?;
                self.config = merge_partial_config(self.config, loaded);
            }
        }
        Ok(self)
    }

    /// Layer project config (overrides user config and defaults)
    pub fn with_project_config(mut self) -> Result<Self> {
        let project_path = PathBuf::from("./.cqlite.toml");
        if project_path.exists() {
            let loaded = Config::load_from_file(&project_path)
                .with_context(|| "Failed to load project config")?;
            self.config = merge_partial_config(self.config, loaded);
        }
        Ok(self)
    }

    /// Layer explicit config from --config flag (overrides discovered configs)
    pub fn with_explicit_config(mut self, path: PathBuf) -> Result<Self> {
        let loaded = Config::load_from_file(&path)
            .with_context(|| format!("Failed to load config file: {}", path.display()))?;
        self.config = merge_partial_config(self.config, loaded);
        Ok(self)
    }

    /// Get platform-specific user config path
    fn user_config_path() -> Option<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            dirs::home_dir().map(|h| h.join("Library/Application Support/cqlite/config.toml"))
        }
        #[cfg(target_os = "windows")]
        {
            dirs::config_dir().map(|d| d.join("cqlite").join("config.toml"))
        }
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        {
            std::env::var("XDG_CONFIG_HOME")
                .ok()
                .map(|p| PathBuf::from(p).join("cqlite/config.toml"))
                .or_else(|| dirs::home_dir().map(|h| h.join(".config/cqlite/config.toml")))
        }
    }

    /// Layer environment variables (overrides file and defaults)
    pub fn with_env(mut self) -> Result<Self> {
        use std::env;

        // CQLITE_DATA_DIR
        if let Ok(val) = env::var("CQLITE_DATA_DIR") {
            self.config.data_directory = Some(PathBuf::from(val));
        }

        // CQLITE_SCHEMA (can be comma-separated paths)
        if let Ok(val) = env::var("CQLITE_SCHEMA") {
            let paths: Vec<PathBuf> = val.split(',').map(|s| PathBuf::from(s.trim())).collect();
            self.config.schema_paths = paths; // Replace, not extend (Issue #126)
        }

        // CQLITE_LIMIT
        if let Ok(val) = env::var("CQLITE_LIMIT") {
            let limit: usize = val.parse().with_context(|| "Invalid CQLITE_LIMIT value")?;
            if limit == 0 {
                return Err(anyhow::anyhow!("CQLITE_LIMIT must be greater than 0"));
            }
            self.config.query_limit = Some(limit);
        }

        // CQLITE_PAGE_SIZE
        if let Ok(val) = env::var("CQLITE_PAGE_SIZE") {
            let page_size: usize = val
                .parse()
                .with_context(|| "Invalid CQLITE_PAGE_SIZE value")?;
            if page_size == 0 {
                return Err(anyhow::anyhow!("CQLITE_PAGE_SIZE must be greater than 0"));
            }
            self.config.repl.page_size = page_size;
        }

        // CQLITE_NO_COLOR
        if let Ok(val) = env::var("CQLITE_NO_COLOR") {
            let no_color = matches!(val.to_lowercase().as_str(), "1" | "true" | "yes" | "on");
            self.config.no_color = no_color;
            self.config.output.colors = !no_color;
        }

        // CQLITE_OUT
        if let Ok(val) = env::var("CQLITE_OUT") {
            self.config.output_mode = Some(val);
        }

        // CQLITE_OTEL_* (Issue #1033, Epic #1031)
        //
        // The `--otel-*` clap flags carry `env = "CQLITE_OTEL_*"` fallbacks, so
        // clap already merges env -> explicit flag (explicit flag wins) into the
        // parsed `Cli`. That merged value is applied in `with_flags`, which runs
        // after this step and therefore correctly overrides any config-file
        // value. We deliberately do NOT read the OTEL env vars again here to
        // avoid two sources fighting; precedence stays file < env < flag.

        Ok(self)
    }

    /// Layer CLI flags (highest precedence)
    ///
    /// Note: CLI flags completely override environment variables and config file
    /// values for the same setting. For example, --schema replaces CQLITE_SCHEMA
    /// entirely rather than merging paths. This ensures clear precedence semantics.
    pub fn with_flags(mut self, cli: &crate::cli_types::Cli) -> Self {
        // Schema path
        if let Some(ref schema) = cli.schema {
            self.config.schema_paths = vec![schema.clone()];
        }

        // Data directory
        if let Some(ref data_dir) = cli.data_dir {
            self.config.data_directory = Some(data_dir.clone());
        }

        // Execute query
        if let Some(ref query) = cli.execute {
            self.config.execution_query = Some(query.clone());
        }

        // Execute file
        if let Some(ref file) = cli.file {
            self.config.execution_file = Some(file.clone());
        }

        // Output mode
        if let Some(ref out) = cli.out {
            self.config.output_mode = Some(out.as_str().to_string());
        }

        // Limit
        if let Some(limit) = cli.limit {
            self.config.query_limit = Some(limit);
        }

        // Page size
        if let Some(page_size) = cli.page_size {
            self.config.repl.page_size = page_size;
        }

        // No color
        if cli.no_color {
            self.config.no_color = true;
            self.config.output.colors = false;
        }

        // Cassandra version hint (Issue #130)
        if let Some(ref version) = cli.cassandra_version {
            self.config.cassandra_version = Some(version.clone());
        }

        // Observability / OpenTelemetry (Issue #1033, Epic #1031).
        // Each `--otel-*` flag already carries its `CQLITE_OTEL_*` env fallback
        // (resolved by clap, explicit flag winning), so applying the parsed
        // value here gives the documented file < env < flag precedence.
        // `otel_enabled` is carried as a raw string for the same reason as the
        // numeric flags below: a malformed `CQLITE_OTEL_ENABLED` env value must
        // not abort `Cli::parse()`. Parse it leniently here, accepting the same
        // truthy/falsy spellings as `ObservabilityConfig::from_env`. Unlike the
        // numeric flags (which keep any lower-precedence value on bad input), an
        // unrecognized value for this MASTER switch resets it to the core default
        // (disabled) — fail safe to "off" — rather than leaving a lower-precedence
        // config-file `enabled = true` active. None of these cases error.
        if let Some(ref raw) = cli.otel_enabled {
            match raw.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => self.config.observability.enabled = Some(true),
                "0" | "false" | "no" | "off" => self.config.observability.enabled = Some(false),
                // An unrecognized value for the MASTER enable switch falls back to
                // the core default (disabled) — fail safe to "off" — rather than
                // silently leaving a lower-precedence config-file `enabled = true`
                // active. Reset to None so `to_core()` uses the core default.
                _ => self.config.observability.enabled = None,
            }
        }
        if let Some(ref endpoint) = cli.otel_endpoint {
            self.config.observability.endpoint = Some(endpoint.clone());
        }
        if let Some(ref protocol) = cli.otel_protocol {
            self.config.observability.protocol = Some(protocol.clone());
        }
        if let Some(ref name) = cli.otel_service_name {
            self.config.observability.service_name = Some(name.clone());
        }
        if let Some(ref version) = cli.otel_service_version {
            self.config.observability.service_version = Some(version.clone());
        }
        // The numeric OTel flags are carried as raw strings so a malformed
        // `CQLITE_OTEL_*` env value cannot abort `Cli::parse()` for the whole
        // CLI (including builds without the `observability` feature). Parse them
        // here with the same lenient semantics as
        // `cqlite_core::observability::ObservabilityConfig::from_env`: on bad
        // input (NaN/inf/garbage) keep the existing/default value rather than
        // erroring. Non-finite ratios are rejected (they survive `clamp`); the
        // final clamp into [0.0, 1.0] is applied by the core builder in
        // `to_core()`.
        if let Some(ref raw) = cli.otel_sampling_ratio {
            if let Ok(ratio) = raw.trim().parse::<f64>() {
                if ratio.is_finite() {
                    self.config.observability.sampling_ratio = Some(ratio);
                }
            }
        }
        if let Some(ref raw) = cli.otel_timeout_ms {
            if let Ok(ms) = raw.trim().parse::<u64>() {
                self.config.observability.timeout_ms = Some(ms);
            }
        }

        self
    }

    /// Build final configuration
    pub fn build(self) -> Config {
        self.config
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
