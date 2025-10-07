use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub default_database: Option<PathBuf>,
    #[serde(default)]
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub output: OutputSettings,
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub timeout_ms: u64,
    pub retry_attempts: u32,
    pub pool_size: u32,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 30000,
            retry_attempts: 3,
            pool_size: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSettings {
    pub max_rows: Option<usize>,
    pub pager: Option<String>,
    pub colors: bool,
    pub timestamp_format: String,
}

impl Default for OutputSettings {
    fn default() -> Self {
        Self {
            max_rows: Some(1000),
            pager: None,
            colors: true,
            timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub query_timeout_ms: u64,
    pub memory_limit_mb: Option<u64>,
    pub cache_size_mb: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            query_timeout_ms: 30000,
            memory_limit_mb: None,
            cache_size_mb: 64,
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
            connection: ConnectionConfig::default(),
            output: OutputSettings::default(),
            performance: PerformanceConfig::default(),
            logging: LoggingConfig::default(),
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
        }
    }
}

impl Config {
    pub fn load(config_path: Option<PathBuf>, cli: &crate::cli_types::Cli) -> Result<Self> {
        Ok(ConfigBuilder::from_defaults()
            .with_file(config_path)?
            .with_env()?
            .with_flags(cli)
            .build())
    }

    fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = match path.extension().and_then(|ext| ext.to_str()) {
            Some("toml") => {
                toml::from_str(&content).with_context(|| "Failed to parse TOML config")?
            }
            Some("yaml") | Some("yml") => {
                serde_yaml::from_str(&content).with_context(|| "Failed to parse YAML config")?
            }
            Some("json") => {
                serde_json::from_str(&content).with_context(|| "Failed to parse JSON config")?
            }
            _ => return Err(anyhow::anyhow!("Unsupported config file format")),
        };

        Ok(config)
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
/// including color support, row limits, and pagination settings.
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
}

impl OutputConfig {
    /// Create a new OutputConfig from CLI arguments
    ///
    /// # Arguments
    ///
    /// * `no_color` - The `--no-color` flag value from CLI (inverted to `color_enabled`)
    /// * `limit` - Maximum number of rows to display (from `--limit` flag)
    /// * `page_size` - Rows per page for pagination (from `--page-size` flag)
    ///
    /// # Examples
    ///
    /// ```
    /// use cqlite_cli::config::OutputConfig;
    ///
    /// // Create config with colors enabled, no limit, default pagination
    /// let config = OutputConfig::from_cli(false, None, None);
    /// assert!(config.color_enabled);
    /// assert_eq!(config.page_size, Some(50));
    ///
    /// // Create config with colors disabled, limit of 100 rows
    /// let config = OutputConfig::from_cli(true, Some(100), Some(25));
    /// assert!(!config.color_enabled);
    /// assert_eq!(config.limit, Some(100));
    /// assert_eq!(config.page_size, Some(25));
    /// ```
    pub fn from_cli(no_color: bool, limit: Option<usize>, page_size: Option<usize>) -> Self {
        Self {
            color_enabled: !no_color,
            limit,
            page_size: page_size.or(Some(50)),
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
    fn default() -> Self {
        Self {
            color_enabled: true,
            limit: None,
            page_size: Some(50),
        }
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
    pub fn with_file(mut self, path: Option<PathBuf>) -> Result<Self> {
        if let Some(p) = path {
            let loaded = Config::load_from_file(&p)?;
            // Merge loaded config, preserving defaults for unset fields
            self.config = loaded;
        }
        Ok(self)
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
            self.config.schema_paths.extend(paths);
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

        self
    }

    /// Build final configuration
    pub fn build(self) -> Config {
        self.config
    }
}
