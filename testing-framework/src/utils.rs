use std::fs;
use std::path::Path;
use crate::error::TestingError;

/// Ensure a directory exists, creating it if necessary
pub fn ensure_directory(path: &Path) -> Result<(), TestingError> {
    if !path.exists() {
        fs::create_dir_all(path)
            .map_err(|e| TestingError::IoError(format!("Failed to create directory {}: {}", path.display(), e)))?;
    }
    Ok(())
}

/// Read file content safely
pub fn read_file_safe(path: &Path) -> Result<String, TestingError> {
    fs::read_to_string(path)
        .map_err(|e| TestingError::IoError(format!("Failed to read file {}: {}", path.display(), e)))
}

/// Write file content safely
pub fn write_file_safe(path: &Path, content: &str) -> Result<(), TestingError> {
    if let Some(parent) = path.parent() {
        ensure_directory(parent)?;
    }
    
    fs::write(path, content)
        .map_err(|e| TestingError::IoError(format!("Failed to write file {}: {}", path.display(), e)))
}

/// Format duration in human-readable format
pub fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    
    if secs > 0 {
        format!("{}.{:03}s", secs, millis)
    } else {
        format!("{}ms", millis)
    }
}

/// Truncate string to max length with ellipsis
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Convert bytes to human-readable format
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_secs(2)), "2.000s");
        assert_eq!(format_duration(Duration::from_millis(2500)), "2.500s");
    }
    
    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("hello", 10), "hello");
        assert_eq!(truncate_string("hello world", 8), "hello...");
        assert_eq!(truncate_string("hi", 5), "hi");
    }
    
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(2048), "2.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
    }
}