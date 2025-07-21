//! Memory Safety Test Runner
//! 
//! This module provides tools to run memory safety tests using various tools
//! including Miri, Valgrind, and AddressSanitizer.

use std::process::{Command, Stdio};
use std::env;
use std::fs;
use std::path::Path;

pub struct MemorySafetyRunner {
    pub miri_available: bool,
    pub valgrind_available: bool,
    pub asan_available: bool,
}

impl MemorySafetyRunner {
    pub fn new() -> Self {
        Self {
            miri_available: Self::check_miri_available(),
            valgrind_available: Self::check_valgrind_available(),
            asan_available: Self::check_asan_available(),
        }
    }

    /// Check if Miri is available
    fn check_miri_available() -> bool {
        Command::new("cargo")
            .args(&["miri", "--version"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// Check if Valgrind is available
    fn check_valgrind_available() -> bool {
        Command::new("valgrind")
            .args(&["--version"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// Check if AddressSanitizer is available
    fn check_asan_available() -> bool {
        // Check if we can compile with AddressSanitizer
        env::var("RUSTFLAGS").unwrap_or_default().contains("-Zsanitizer=address")
            || Command::new("rustc")
                .args(&["--print", "target-features"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
    }

    /// Run tests with Miri
    pub fn run_miri_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.miri_available {
            return Err("Miri is not available. Install with: rustup component add miri".into());
        }

        println!("Running memory safety tests with Miri...");
        
        // Set Miri flags for better error detection
        env::set_var("MIRIFLAGS", "-Zmiri-disable-isolation -Zmiri-ignore-leaks");
        
        let output = Command::new("cargo")
            .args(&[
                "miri", "test",
                "--package", "cqlite-core",
                "--lib",
                "memory_safety_tests::",
                "--",
                "--test-threads=1" // Run tests sequentially for better error reporting
            ])
            .output()?;

        if output.status.success() {
            println!("✓ Miri tests passed!");
            println!("{}", String::from_utf8_lossy(&output.stdout));
        } else {
            println!("✗ Miri tests failed!");
            println!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
            println!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
            return Err("Miri detected memory safety issues".into());
        }

        Ok(())
    }

    /// Run tests with Valgrind
    pub fn run_valgrind_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.valgrind_available {
            return Err("Valgrind is not available. Install with your package manager.".into());
        }

        println!("Running memory safety tests with Valgrind...");

        // First, build the test binary
        let output = Command::new("cargo")
            .args(&[
                "test",
                "--package", "cqlite-core",
                "--lib",
                "memory_safety_tests::",
                "--no-run"
            ])
            .output()?;

        if !output.status.success() {
            return Err("Failed to build test binary".into());
        }

        // Find the test binary
        let test_binary = self.find_test_binary()?;

        // Run with Valgrind
        let output = Command::new("valgrind")
            .args(&[
                "--tool=memcheck",
                "--leak-check=full",
                "--show-leak-kinds=all",
                "--track-origins=yes",
                "--verbose",
                "--error-exitcode=1",
                &test_binary
            ])
            .output()?;

        if output.status.success() {
            println!("✓ Valgrind tests passed!");
        } else {
            println!("✗ Valgrind detected memory issues!");
            println!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
            return Err("Valgrind detected memory safety issues".into());
        }

        Ok(())
    }

    /// Run tests with AddressSanitizer
    pub fn run_asan_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running memory safety tests with AddressSanitizer...");

        // Set environment variables for AddressSanitizer
        env::set_var("RUSTFLAGS", "-Zsanitizer=address");
        env::set_var("ASAN_OPTIONS", "detect_odr_violation=0:abort_on_error=1");

        let output = Command::new("cargo")
            .args(&[
                "+nightly", "test",
                "--package", "cqlite-core",
                "--lib",
                "memory_safety_tests::",
                "--target", "x86_64-unknown-linux-gnu"
            ])
            .env("RUSTFLAGS", "-Zsanitizer=address")
            .output()?;

        if output.status.success() {
            println!("✓ AddressSanitizer tests passed!");
            println!("{}", String::from_utf8_lossy(&output.stdout));
        } else {
            println!("✗ AddressSanitizer detected memory issues!");
            println!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
            println!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
            return Err("AddressSanitizer detected memory safety issues".into());
        }

        Ok(())
    }

    /// Find the test binary path
    fn find_test_binary(&self) -> Result<String, Box<dyn std::error::Error>> {
        let target_dir = Path::new("target/debug/deps");
        if !target_dir.exists() {
            return Err("Target directory not found. Build tests first.".into());
        }

        for entry in fs::read_dir(target_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = path.file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("");
                
            if filename.starts_with("cqlite_core-") && 
               filename.contains("memory_safety_tests") &&
               !filename.ends_with(".d") {
                return Ok(path.to_string_lossy().to_string());
            }
        }

        Err("Test binary not found".into())
    }

    /// Run stress tests for memory leaks
    pub fn run_stress_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running stress tests for memory leak detection...");

        let output = Command::new("cargo")
            .args(&[
                "test",
                "--package", "cqlite-core",
                "--release", // Use release mode for realistic performance
                "--lib",
                "test_concurrent_memory_stress",
                "--",
                "--ignored" // Run ignored stress tests
            ])
            .env("RUST_TEST_THREADS", "1") // Single thread for better monitoring
            .output()?;

        if output.status.success() {
            println!("✓ Stress tests passed!");
            println!("{}", String::from_utf8_lossy(&output.stdout));
        } else {
            println!("✗ Stress tests failed!");
            println!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
            println!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
            return Err("Stress tests detected issues".into());
        }

        Ok(())
    }

    /// Run all available memory safety tests
    pub fn run_all_available_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut test_count = 0;
        let mut passed_count = 0;

        // Always run basic stress tests
        test_count += 1;
        if let Err(e) = self.run_stress_tests() {
            println!("Stress tests failed: {}", e);
        } else {
            passed_count += 1;
        }

        // Run Miri if available
        if self.miri_available {
            test_count += 1;
            if let Err(e) = self.run_miri_tests() {
                println!("Miri tests failed: {}", e);
            } else {
                passed_count += 1;
            }
        } else {
            println!("Miri not available, skipping...");
        }

        // Run Valgrind if available
        if self.valgrind_available {
            test_count += 1;
            if let Err(e) = self.run_valgrind_tests() {
                println!("Valgrind tests failed: {}", e);
            } else {
                passed_count += 1;
            }
        } else {
            println!("Valgrind not available, skipping...");
        }

        // Run AddressSanitizer if available
        if self.asan_available {
            test_count += 1;
            if let Err(e) = self.run_asan_tests() {
                println!("AddressSanitizer tests failed: {}", e);
            } else {
                passed_count += 1;
            }
        } else {
            println!("AddressSanitizer not available, skipping...");
        }

        println!("\n=== Memory Safety Test Summary ===");
        println!("Tests run: {}", test_count);
        println!("Tests passed: {}", passed_count);
        println!("Tests failed: {}", test_count - passed_count);

        if passed_count == test_count {
            println!("🎉 All available memory safety tests passed!");
            Ok(())
        } else {
            Err(format!("{} out of {} tests failed", test_count - passed_count, test_count).into())
        }
    }

    /// Print available tools
    pub fn print_available_tools(&self) {
        println!("Memory Safety Tools Available:");
        println!("  Miri: {}", if self.miri_available { "✓" } else { "✗" });
        println!("  Valgrind: {}", if self.valgrind_available { "✓" } else { "✗" });
        println!("  AddressSanitizer: {}", if self.asan_available { "✓" } else { "✗" });
        
        if !self.miri_available {
            println!("  To install Miri: rustup component add miri");
        }
        if !self.valgrind_available {
            println!("  To install Valgrind: sudo apt-get install valgrind (Ubuntu/Debian)");
        }
        if !self.asan_available {
            println!("  To use AddressSanitizer: requires nightly Rust");
        }
    }
}

impl Default for MemorySafetyRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_creation() {
        let runner = MemorySafetyRunner::new();
        
        // Test should not panic
        runner.print_available_tools();
    }

    #[test]
    fn test_tool_detection() {
        let runner = MemorySafetyRunner::new();
        
        // These might be false in CI environments, but should not panic
        println!("Miri available: {}", runner.miri_available);
        println!("Valgrind available: {}", runner.valgrind_available);
        println!("ASAN available: {}", runner.asan_available);
    }
}