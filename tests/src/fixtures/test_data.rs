//! Test data fixtures and utilities

use std::path::PathBuf;
use tempfile::TempDir;

/// Sample user data for testing
pub struct TestUser {
    pub id: String,
    pub name: String,
    pub email: String,
}

impl TestUser {
    pub fn new(id: &str, name: &str, email: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            email: email.to_string(),
        }
    }
}

/// Generate sample users for testing
pub fn sample_users() -> Vec<TestUser> {
    vec![
        TestUser::new("1", "John Doe", "john@example.com"),
        TestUser::new("2", "Jane Smith", "jane@example.com"),
        TestUser::new("3", "Bob Wilson", "bob@example.com"),
    ]
}

/// Create a temporary directory with test data
pub fn create_test_data_dir() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let data_path = temp_dir.path().to_path_buf();

    // Create sample CSV file
    let csv_content = "id,name,email\n1,John Doe,john@example.com\n2,Jane Smith,jane@example.com\n3,Bob Wilson,bob@example.com";
    let csv_path = data_path.join("users.csv");
    std::fs::write(&csv_path, csv_content).expect("Failed to write CSV file");

    // Create sample JSON file
    let json_content = r#"[
  {"id": "1", "name": "John Doe", "email": "john@example.com"},
  {"id": "2", "name": "Jane Smith", "email": "jane@example.com"},
  {"id": "3", "name": "Bob Wilson", "email": "bob@example.com"}
]"#;
    let json_path = data_path.join("users.json");
    std::fs::write(&json_path, json_content).expect("Failed to write JSON file");

    (temp_dir, data_path)
}
