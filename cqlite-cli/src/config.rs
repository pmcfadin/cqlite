use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub default_database: Option<PathBuf>,
    pub connection: ConnectionConfig,
    pub output: OutputConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub timeout_ms: u64,
    pub retry_attempts: u32,
    pub pool_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub max_rows: Option<usize>,
    pub pager: Option<String>,
    pub colors: bool,
    pub timestamp_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub query_timeout_ms: u64,
    pub memory_limit_mb: Option<u64>,
    pub cache_size_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: Option<PathBuf>,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    Plain,
    Json,
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
            connection: ConnectionConfig {
                timeout_ms: 30000,
                retry_attempts: 3,
                pool_size: 10,
            },
            output: OutputConfig {
                max_rows: Some(1000),
                pager: None,
                colors: true,
                timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
            },
            performance: PerformanceConfig {
                query_timeout_ms: 300000, // 5 minutes
                memory_limit_mb: None,
                cache_size_mb: 256,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                file: None,
                format: LogFormat::Pretty,
            },
            repl: ReplConfig::default(),
            data_directory: None,
            default_keyspace: None,
            enable_history: None,
            enable_completion: None,
            show_timing: None,
            page_size: None,
            enable_paging: None,
            no_color: false,
        }
    }
}

impl Config {
    pub fn load(config_path: Option<PathBuf>) -> Result<Self> {
        let config = if let Some(path) = config_path {
            Self::load_from_file(&path)?
        } else {
            Self::load_default()?
        };

        Ok(config)
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

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::Pretty
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
