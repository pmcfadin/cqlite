use std::fmt;

#[derive(Debug)]
pub enum TestingError {
    IoError(String),
    ConfigError(String),
    ExecutionError(String),
    ComparisonError(String),
    DockerError(String),
}

impl fmt::Display for TestingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestingError::IoError(msg) => write!(f, "IO Error: {}", msg),
            TestingError::ConfigError(msg) => write!(f, "Config Error: {}", msg),
            TestingError::ExecutionError(msg) => write!(f, "Execution Error: {}", msg),
            TestingError::ComparisonError(msg) => write!(f, "Comparison Error: {}", msg),
            TestingError::DockerError(msg) => write!(f, "Docker Error: {}", msg),
        }
    }
}

impl std::error::Error for TestingError {}

impl From<std::io::Error> for TestingError {
    fn from(err: std::io::Error) -> Self {
        TestingError::IoError(err.to_string())
    }
}