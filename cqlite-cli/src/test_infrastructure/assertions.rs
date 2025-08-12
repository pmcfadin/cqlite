//! Test assertion utilities
//!
//! This module provides specialized assertion helpers for validating
//! CLI output, database results, and error conditions.

use serde_json::Value;
use std::collections::HashMap;
use super::TestResult;

/// Assertion helper for validating command results
#[derive(Debug)]
pub struct ResultAssertion {
    actual: CommandResult,
}

/// Assertion helper for validating CLI output
#[derive(Debug)]
pub struct OutputAssertion {
    stdout: String,
    stderr: String,
    exit_code: i32,
}

/// Assertion helper for validating error conditions
#[derive(Debug)]
pub struct ErrorAssertion {
    error: Box<dyn std::error::Error + Send + Sync>,
}

/// Represents a command execution result
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub execution_time: std::time::Duration,
}

/// Represents database query results
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<HashMap<String, Value>>,
    pub column_names: Vec<String>,
    pub row_count: usize,
    pub execution_time: std::time::Duration,
}

/// Represents parsed JSON output
#[derive(Debug, Clone)]
pub struct JsonOutput {
    pub data: Value,
    pub is_array: bool,
    pub is_object: bool,
}

impl ResultAssertion {
    /// Create a new result assertion
    pub fn new(result: CommandResult) -> Self {
        Self { actual: result }
    }
    
    /// Assert that the command succeeded
    pub fn success(self) -> TestResult<Self> {
        if !self.actual.success {
            return Err(format!(
                "Expected command to succeed, but it failed with exit code {}. stderr: {}",
                self.actual.exit_code, self.actual.stderr
            ).into());
        }
        Ok(self)
    }
    
    /// Assert that the command failed
    pub fn failure(self) -> TestResult<Self> {
        if self.actual.success {
            return Err(format!(
                "Expected command to fail, but it succeeded. stdout: {}",
                self.actual.stdout
            ).into());
        }
        Ok(self)
    }
    
    /// Assert specific exit code
    pub fn exit_code(self, expected_code: i32) -> TestResult<Self> {
        if self.actual.exit_code != expected_code {
            return Err(format!(
                "Expected exit code {}, but got {}",
                expected_code, self.actual.exit_code
            ).into());
        }
        Ok(self)
    }
    
    /// Check execution time
    pub fn execution_time_less_than(self, max_time: std::time::Duration) -> TestResult<Self> {
        if self.actual.execution_time > max_time {
            return Err(format!(
                "Expected execution time to be less than {:?}, but got {:?}",
                max_time, self.actual.execution_time
            ).into());
        }
        Ok(self)
    }
    
    /// Get output assertion
    pub fn output(self) -> OutputAssertion {
        OutputAssertion {
            stdout: self.actual.stdout,
            stderr: self.actual.stderr,
            exit_code: self.actual.exit_code,
        }
    }
}

impl OutputAssertion {
    /// Create a new output assertion
    pub fn new(stdout: String, stderr: String, exit_code: i32) -> Self {
        Self {
            stdout,
            stderr,
            exit_code,
        }
    }
    
    /// Assert stdout contains text
    pub fn stdout_contains<S: AsRef<str>>(self, expected: S) -> TestResult<Self> {
        let expected = expected.as_ref();
        if !self.stdout.contains(expected) {
            return Err(format!(
                "Expected stdout to contain '{}', but got: '{}'",
                expected, self.stdout
            ).into());
        }
        Ok(self)
    }
    
    /// Assert stderr contains text
    pub fn stderr_contains<S: AsRef<str>>(self, expected: S) -> TestResult<Self> {
        let expected = expected.as_ref();
        if !self.stderr.contains(expected) {
            return Err(format!(
                "Expected stderr to contain '{}', but got: '{}'",
                expected, self.stderr
            ).into());
        }
        Ok(self)
    }
    
    /// Assert stdout matches exactly
    pub fn stdout_equals<S: AsRef<str>>(self, expected: S) -> TestResult<Self> {
        let expected = expected.as_ref();
        if self.stdout.trim() != expected {
            return Err(format!(
                "Expected stdout to equal '{}', but got: '{}'",
                expected, self.stdout
            ).into());
        }
        Ok(self)
    }
    
    /// Assert stderr is empty
    pub fn stderr_empty(self) -> TestResult<Self> {
        if !self.stderr.trim().is_empty() {
            return Err(format!(
                "Expected stderr to be empty, but got: '{}'",
                self.stderr
            ).into());
        }
        Ok(self)
    }
    
    /// Assert stdout is empty
    pub fn stdout_empty(self) -> TestResult<Self> {
        if !self.stdout.trim().is_empty() {
            return Err(format!(
                "Expected stdout to be empty, but got: '{}'",
                self.stdout
            ).into());
        }
        Ok(self)
    }
    
    /// Parse stdout as JSON and return JSON assertion
    pub fn parse_json(self) -> TestResult<JsonAssertion> {
        let json: Value = serde_json::from_str(&self.stdout)
            .map_err(|e| format!("Failed to parse stdout as JSON: {}", e))?;
        
        Ok(JsonAssertion::new(json))
    }
    
    /// Assert stdout contains valid JSON
    pub fn stdout_is_json(self) -> TestResult<JsonAssertion> {
        self.parse_json()
    }
    
    /// Parse stdout as table output and validate structure
    pub fn stdout_is_table(self) -> TestResult<TableAssertion> {
        TableAssertion::parse_from_string(&self.stdout)
    }
    
    /// Assert stdout matches regex pattern
    pub fn stdout_matches_regex<S: AsRef<str>>(self, pattern: S) -> TestResult<Self> {
        let regex = regex::Regex::new(pattern.as_ref())
            .map_err(|e| format!("Invalid regex pattern: {}", e))?;
        
        if !regex.is_match(&self.stdout) {
            return Err(format!(
                "Expected stdout to match regex '{}', but got: '{}'",
                pattern.as_ref(), self.stdout
            ).into());
        }
        Ok(self)
    }
    
    /// Count lines in stdout
    pub fn stdout_line_count(self, expected_count: usize) -> TestResult<Self> {
        let actual_count = self.stdout.lines().count();
        if actual_count != expected_count {
            return Err(format!(
                "Expected {} lines in stdout, but got {}. Content: '{}'",
                expected_count, actual_count, self.stdout
            ).into());
        }
        Ok(self)
    }
}

/// Assertion helper for JSON data
#[derive(Debug)]
pub struct JsonAssertion {
    data: Value,
}

impl JsonAssertion {
    /// Create a new JSON assertion
    pub fn new(data: Value) -> Self {
        Self { data }
    }
    
    /// Assert JSON is an object
    pub fn is_object(self) -> TestResult<Self> {
        if !self.data.is_object() {
            return Err(format!(
                "Expected JSON to be an object, but got: {}",
                serde_json::to_string(&self.data).unwrap_or_default()
            ).into());
        }
        Ok(self)
    }
    
    /// Assert JSON is an array
    pub fn is_array(self) -> TestResult<Self> {
        if !self.data.is_array() {
            return Err(format!(
                "Expected JSON to be an array, but got: {}",
                serde_json::to_string(&self.data).unwrap_or_default()
            ).into());
        }
        Ok(self)
    }
    
    /// Assert JSON has specific field
    pub fn has_field<S: AsRef<str>>(self, field_name: S) -> TestResult<Self> {
        let field_name = field_name.as_ref();
        if let Value::Object(ref obj) = self.data {
            if !obj.contains_key(field_name) {
                return Err(format!(
                    "Expected JSON to have field '{}', but it was missing. Available fields: {:?}",
                    field_name, obj.keys().collect::<Vec<_>>()
                ).into());
            }
        } else {
            return Err("Cannot check field on non-object JSON".into());
        }
        Ok(self)
    }
    
    /// Assert field equals specific value
    pub fn field_equals<S: AsRef<str>>(self, field_name: S, expected: Value) -> TestResult<Self> {
        let field_name = field_name.as_ref();
        if let Value::Object(ref obj) = self.data {
            if let Some(actual) = obj.get(field_name) {
                if actual != &expected {
                    return Err(format!(
                        "Expected field '{}' to equal {}, but got {}",
                        field_name,
                        serde_json::to_string(&expected).unwrap_or_default(),
                        serde_json::to_string(actual).unwrap_or_default()
                    ).into());
                }
            } else {
                return Err(format!("Field '{}' not found in JSON object", field_name).into());
            }
        } else {
            return Err("Cannot check field on non-object JSON".into());
        }
        Ok(self)
    }
    
    /// Assert array has specific length
    pub fn array_length(self, expected_length: usize) -> TestResult<Self> {
        if let Value::Array(ref arr) = self.data {
            if arr.len() != expected_length {
                return Err(format!(
                    "Expected array to have length {}, but got {}",
                    expected_length, arr.len()
                ).into());
            }
        } else {
            return Err("Cannot check array length on non-array JSON".into());
        }
        Ok(self)
    }
    
    /// Get the underlying JSON value
    pub fn value(&self) -> &Value {
        &self.data
    }
}

/// Assertion helper for table output
#[derive(Debug)]
pub struct TableAssertion {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl TableAssertion {
    /// Parse table from string output
    pub fn parse_from_string(output: &str) -> TestResult<Self> {
        let lines: Vec<&str> = output.lines().collect();
        
        if lines.is_empty() {
            return Err("Empty table output".into());
        }
        
        // Simple table parsing - assumes first line is headers
        // This is a basic implementation; you might want to improve it
        let headers: Vec<String> = lines[0]
            .split('|')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        
        let mut rows = Vec::new();
        for line in &lines[2..] { // Skip header and separator line
            if line.trim().is_empty() {
                continue;
            }
            
            let row: Vec<String> = line
                .split('|')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            
            if !row.is_empty() {
                rows.push(row);
            }
        }
        
        Ok(Self { headers, rows })
    }
    
    /// Assert table has specific number of columns
    pub fn column_count(self, expected_count: usize) -> TestResult<Self> {
        if self.headers.len() != expected_count {
            return Err(format!(
                "Expected {} columns, but got {}. Headers: {:?}",
                expected_count, self.headers.len(), self.headers
            ).into());
        }
        Ok(self)
    }
    
    /// Assert table has specific number of rows
    pub fn row_count(self, expected_count: usize) -> TestResult<Self> {
        if self.rows.len() != expected_count {
            return Err(format!(
                "Expected {} rows, but got {}",
                expected_count, self.rows.len()
            ).into());
        }
        Ok(self)
    }
    
    /// Assert table has specific header
    pub fn has_header<S: AsRef<str>>(&self, header_name: S) -> TestResult<&Self> {
        let header_name = header_name.as_ref();
        if !self.headers.contains(&header_name.to_string()) {
            return Err(format!(
                "Expected header '{}', but headers are: {:?}",
                header_name, self.headers
            ).into());
        }
        Ok(self)
    }
    
    /// Assert cell value at specific position
    pub fn cell_equals<S: AsRef<str>>(&self, row: usize, col: usize, expected: S) -> TestResult<&Self> {
        let expected = expected.as_ref();
        
        if row >= self.rows.len() {
            return Err(format!("Row {} does not exist (only {} rows)", row, self.rows.len()).into());
        }
        
        if col >= self.rows[row].len() {
            return Err(format!(
                "Column {} does not exist in row {} (only {} columns)",
                col, row, self.rows[row].len()
            ).into());
        }
        
        let actual = &self.rows[row][col];
        if actual != expected {
            return Err(format!(
                "Expected cell at ({}, {}) to be '{}', but got '{}'",
                row, col, expected, actual
            ).into());
        }
        
        Ok(self)
    }
}

impl ErrorAssertion {
    /// Create a new error assertion
    pub fn new(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { error }
    }
    
    /// Assert error message contains text
    pub fn message_contains<S: AsRef<str>>(self, expected: S) -> TestResult<Self> {
        let expected = expected.as_ref();
        let actual = self.error.to_string();
        
        if !actual.contains(expected) {
            return Err(format!(
                "Expected error message to contain '{}', but got: '{}'",
                expected, actual
            ).into());
        }
        Ok(self)
    }
    
    /// Assert error is of specific type
    pub fn is_type<T: std::error::Error + 'static>(self) -> TestResult<Self> {
        if self.error.downcast_ref::<T>().is_none() {
            return Err(format!(
                "Expected error to be of type {}, but got different type",
                std::any::type_name::<T>()
            ).into());
        }
        Ok(self)
    }
}

/// Convenient macros for assertions
#[macro_export]
macro_rules! assert_json_field {
    ($json:expr, $field:expr, $expected:expr) => {
        $json.has_field($field)?.field_equals($field, $expected)?
    };
}

#[macro_export]
macro_rules! assert_table_cell {
    ($table:expr, $row:expr, $col:expr, $expected:expr) => {
        $table.cell_equals($row, $col, $expected)?
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_command_result_assertion() {
        let result = CommandResult {
            success: true,
            stdout: "Hello, World!".to_string(),
            stderr: "".to_string(),
            exit_code: 0,
            execution_time: std::time::Duration::from_millis(100),
        };
        
        let assertion = ResultAssertion::new(result);
        assert!(assertion.success().is_ok());
    }
    
    #[test]
    fn test_output_assertion() {
        let assertion = OutputAssertion::new(
            "Hello, World!".to_string(),
            "".to_string(),
            0,
        );
        
        assert!(assertion.stdout_contains("Hello").is_ok());
        assert!(assertion.stdout_contains("Missing").is_err());
    }
    
    #[test]
    fn test_json_assertion() {
        let json_data = json!({
            "name": "test",
            "count": 42,
            "items": [1, 2, 3]
        });
        
        let assertion = JsonAssertion::new(json_data);
        assert!(assertion.is_object().is_ok());
        assert!(assertion.has_field("name").is_ok());
        assert!(assertion.has_field("missing").is_err());
    }
    
    #[test]
    fn test_table_assertion() {
        let table_output = "| Name | Age | Email |\n|------|-----|-------|\n| John | 30  | john@example.com |\n| Jane | 25  | jane@example.com |";
        
        let assertion = TableAssertion::parse_from_string(table_output).unwrap();
        assert!(assertion.column_count(3).is_ok());
        assert!(assertion.row_count(2).is_ok());
        assert!(assertion.has_header("Name").is_ok());
        assert!(assertion.cell_equals(0, 0, "John").is_ok());
    }
}