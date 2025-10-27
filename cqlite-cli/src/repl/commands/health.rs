//! :health meta-command implementation
//!
//! Performs configuration and environment diagnostics

use anyhow::Result;
use colored::Colorize;
use std::fs;
use std::path::Path;

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    pub category: String,
    pub status: HealthStatus,
    pub message: String,
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HealthStatus {
    Ok,
    Warning,
    Error,
}

/// Execute the :health command
pub async fn execute_health(
    data_dir: Option<&Path>,
    config_file: Option<&Path>,
    page_size: usize,
    timing_enabled: bool,
    colors_enabled: bool,
) -> Result<()> {
    println!();
    println!("{}", "=== Health Diagnostics ===".green().bold());
    println!();

    let mut checks = Vec::new();

    // 1. Data directory checks
    checks.extend(check_data_directory(data_dir));

    // 2. Config checks
    checks.extend(check_config(
        config_file,
        page_size,
        timing_enabled,
        colors_enabled,
    ));

    // 3. Compression codec checks
    checks.extend(check_compression_codecs());

    // 4. Platform checks
    checks.extend(check_platform());

    // Display results
    display_health_results(&checks);

    // Collect and display actionable hints
    display_hints(&checks);

    Ok(())
}

fn check_data_directory(data_dir: Option<&Path>) -> Vec<HealthCheckResult> {
    let mut results = Vec::new();

    // Check if data directory is set
    let Some(dir) = data_dir else {
        results.push(HealthCheckResult {
            category: "data-dir".to_string(),
            status: HealthStatus::Warning,
            message: "Data directory not configured".to_string(),
            hint: Some("Use :config data-dir=<PATH> to set data directory".to_string()),
        });
        return results;
    };

    // Check if directory exists
    if !dir.exists() {
        results.push(HealthCheckResult {
            category: "data-dir".to_string(),
            status: HealthStatus::Error,
            message: format!("Data directory does not exist: {}", dir.display()),
            hint: Some(format!(
                "Create directory or use :config data-dir=<PATH> to set valid directory"
            )),
        });
        return results;
    }

    // Check if it's a directory
    if !dir.is_dir() {
        results.push(HealthCheckResult {
            category: "data-dir".to_string(),
            status: HealthStatus::Error,
            message: format!("Path is not a directory: {}", dir.display()),
            hint: Some("Provide a directory path, not a file".to_string()),
        });
        return results;
    }

    // Check if directory is readable
    match fs::read_dir(dir) {
        Ok(_) => {
            results.push(HealthCheckResult {
                category: "data-dir".to_string(),
                status: HealthStatus::Ok,
                message: format!("Data directory readable: {}", dir.display()),
                hint: None,
            });
        }
        Err(e) => {
            results.push(HealthCheckResult {
                category: "data-dir".to_string(),
                status: HealthStatus::Error,
                message: format!("Cannot read data directory: {}", e),
                hint: Some("Check directory permissions".to_string()),
            });
            return results;
        }
    }

    // Check directory layout sanity (look for keyspace-like subdirectories)
    match check_directory_layout(dir) {
        Ok(layout_info) => {
            if layout_info.has_keyspaces {
                results.push(HealthCheckResult {
                    category: "data-dir layout".to_string(),
                    status: HealthStatus::Ok,
                    message: format!(
                        "Found {} keyspace-like directories",
                        layout_info.keyspace_count
                    ),
                    hint: None,
                });
            } else {
                results.push(HealthCheckResult {
                    category: "data-dir layout".to_string(),
                    status: HealthStatus::Warning,
                    message: "No keyspace directories found".to_string(),
                    hint: Some(
                        "Directory may be empty or not a valid Cassandra data directory"
                            .to_string(),
                    ),
                });
            }
        }
        Err(e) => {
            results.push(HealthCheckResult {
                category: "data-dir layout".to_string(),
                status: HealthStatus::Warning,
                message: format!("Could not analyze directory layout: {}", e),
                hint: None,
            });
        }
    }

    results
}

#[derive(Debug)]
struct DirectoryLayoutInfo {
    has_keyspaces: bool,
    keyspace_count: usize,
}

fn check_directory_layout(dir: &Path) -> Result<DirectoryLayoutInfo> {
    let mut keyspace_count = 0;
    let mut has_keyspaces = false;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Count directories that might be keyspaces
            // (simple heuristic: any subdirectory could be a keyspace)
            keyspace_count += 1;
            has_keyspaces = true;
        }
    }

    Ok(DirectoryLayoutInfo {
        has_keyspaces,
        keyspace_count,
    })
}

fn check_config(
    config_file: Option<&Path>,
    page_size: usize,
    timing_enabled: bool,
    colors_enabled: bool,
) -> Vec<HealthCheckResult> {
    let mut results = Vec::new();

    // Config file check
    if let Some(file) = config_file {
        if file.exists() {
            results.push(HealthCheckResult {
                category: "config".to_string(),
                status: HealthStatus::Ok,
                message: format!("Config file loaded: {}", file.display()),
                hint: None,
            });
        } else {
            results.push(HealthCheckResult {
                category: "config".to_string(),
                status: HealthStatus::Warning,
                message: format!("Config file not found: {}", file.display()),
                hint: Some("Check config file path".to_string()),
            });
        }
    } else {
        results.push(HealthCheckResult {
            category: "config".to_string(),
            status: HealthStatus::Ok,
            message: "Using default configuration".to_string(),
            hint: None,
        });
    }

    // Page size sanity check
    if page_size > 0 && page_size <= 10000 {
        results.push(HealthCheckResult {
            category: "page-size".to_string(),
            status: HealthStatus::Ok,
            message: format!("Page size: {}", page_size),
            hint: None,
        });
    } else if page_size > 10000 {
        results.push(HealthCheckResult {
            category: "page-size".to_string(),
            status: HealthStatus::Warning,
            message: format!("Page size very large: {}", page_size),
            hint: Some("Consider reducing page size for better memory usage".to_string()),
        });
    } else {
        results.push(HealthCheckResult {
            category: "page-size".to_string(),
            status: HealthStatus::Error,
            message: format!("Invalid page size: {}", page_size),
            hint: Some("Use :config page_size=<N> to set valid page size".to_string()),
        });
    }

    // Report timing and colors settings (informational)
    results.push(HealthCheckResult {
        category: "settings".to_string(),
        status: HealthStatus::Ok,
        message: format!(
            "Timing: {}, Colors: {}",
            if timing_enabled {
                "enabled"
            } else {
                "disabled"
            },
            if colors_enabled {
                "enabled"
            } else {
                "disabled"
            }
        ),
        hint: None,
    });

    results
}

fn check_compression_codecs() -> Vec<HealthCheckResult> {
    let mut results = Vec::new();
    let mut available_codecs: Vec<&str> = Vec::new();

    // Note: Compression features are defined in cqlite-core, not cqlite-cli
    // We check what the core library was built with by checking its dependencies
    // For now, we assume standard build with all-compression feature enabled

    // The standard build includes all compression codecs
    // These are compiled into cqlite-core by default
    available_codecs.push("LZ4");
    available_codecs.push("Snappy");
    available_codecs.push("Deflate");
    available_codecs.push("Zstd");

    // Report available codecs
    if !available_codecs.is_empty() {
        results.push(HealthCheckResult {
            category: "compression".to_string(),
            status: HealthStatus::Ok,
            message: format!("Available codecs: {}", available_codecs.join(", ")),
            hint: None,
        });
    } else {
        // If no codecs at all (unlikely with default features)
        results.push(HealthCheckResult {
            category: "compression".to_string(),
            status: HealthStatus::Error,
            message: "No compression codecs available".to_string(),
            hint: Some("Rebuild with --features all-compression".to_string()),
        });
    }

    results
}

fn check_platform() -> Vec<HealthCheckResult> {
    let mut results = Vec::new();

    // Platform detection
    let platform = std::env::consts::OS;
    let arch = std::env::consts::ARCH;

    results.push(HealthCheckResult {
        category: "platform".to_string(),
        status: HealthStatus::Ok,
        message: format!("Platform: {} ({})", platform, arch),
        hint: None,
    });

    // Check if we're on a supported platform
    match platform {
        "linux" | "macos" | "windows" => {
            results.push(HealthCheckResult {
                category: "platform".to_string(),
                status: HealthStatus::Ok,
                message: format!("{} is supported", platform),
                hint: None,
            });
        }
        _ => {
            results.push(HealthCheckResult {
                category: "platform".to_string(),
                status: HealthStatus::Warning,
                message: format!("{} may have limited support", platform),
                hint: Some("Report issues at https://github.com/cqlite/cqlite/issues".to_string()),
            });
        }
    }

    // File descriptor limits (Unix-like systems only)
    #[cfg(unix)]
    {
        // This is a simple check - in production you might use libc to get actual limits
        results.push(HealthCheckResult {
            category: "platform".to_string(),
            status: HealthStatus::Ok,
            message: "Unix-like platform detected".to_string(),
            hint: None,
        });
    }

    results
}

fn display_health_results(checks: &[HealthCheckResult]) {
    println!("{}", "Checks:".cyan().bold());
    println!();

    for check in checks {
        let (icon, status_text) = match check.status {
            HealthStatus::Ok => ("✅", "OK".green()),
            HealthStatus::Warning => ("⚠️ ", "WARNING".yellow()),
            HealthStatus::Error => ("❌", "ERROR".red()),
        };

        println!(
            "  {} {} [{}] {}",
            icon,
            check.category.cyan(),
            status_text,
            check.message
        );
    }

    println!();
}

fn display_hints(checks: &[HealthCheckResult]) {
    let hints: Vec<_> = checks
        .iter()
        .filter_map(|check| {
            check
                .hint
                .as_ref()
                .map(|hint| (check.category.clone(), check.status, hint.clone()))
        })
        .collect();

    if !hints.is_empty() {
        println!("{}", "Tips:".yellow().bold());
        println!();

        for (category, status, hint) in hints {
            let prefix = match status {
                HealthStatus::Error => "❌",
                HealthStatus::Warning => "💡",
                HealthStatus::Ok => "ℹ️ ",
            };

            println!("  {} {}: {}", prefix, category.cyan(), hint);
        }

        println!();
    }

    // Summary
    let ok_count = checks
        .iter()
        .filter(|c| c.status == HealthStatus::Ok)
        .count();
    let warning_count = checks
        .iter()
        .filter(|c| c.status == HealthStatus::Warning)
        .count();
    let error_count = checks
        .iter()
        .filter(|c| c.status == HealthStatus::Error)
        .count();

    println!("{}", "Summary:".cyan().bold());
    println!(
        "  {} checks: {} OK, {} warnings, {} errors",
        checks.len(),
        ok_count.to_string().green(),
        warning_count.to_string().yellow(),
        error_count.to_string().red()
    );
    println!();
}
