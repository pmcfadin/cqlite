//! Real-time Validation Framework
//!
//! This module provides real-time monitoring and validation for Issue #17.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Real-time validator
#[derive(Debug)]
pub struct RealtimeValidator {
    /// Monitoring configuration
    config: MonitoringConfig,
    /// Current validation status
    current_status: ValidationEvent,
    /// Event history
    event_history: Vec<ValidationEvent>,
}

/// Configuration for real-time monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable real-time monitoring
    pub enabled: bool,
    /// Monitoring interval in milliseconds
    pub monitoring_interval_ms: u64,
    /// Maximum events to keep in history
    pub max_event_history: usize,
    /// Event filters
    pub event_filters: Vec<EventFilter>,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            monitoring_interval_ms: 1000, // 1 second
            max_event_history: 1000,
            event_filters: vec![
                EventFilter {
                    event_type: Some(ValidationEventType::Error),
                    enabled: true,
                },
                EventFilter {
                    event_type: Some(ValidationEventType::Warning),
                    enabled: true,
                },
            ],
        }
    }
}

/// Event filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    pub event_type: Option<ValidationEventType>,
    pub enabled: bool,
}

/// Real-time validation event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationEvent {
    pub event_id: String,
    pub event_type: ValidationEventType,
    pub message: String,
    pub details: HashMap<String, String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub duration_ms: Option<u64>,
    pub context: ValidationContext,
}

/// Type of validation event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationEventType {
    /// Validation started
    Started,
    /// Validation completed successfully
    Completed,
    /// Validation failed
    Failed,
    /// Warning during validation
    Warning,
    /// Error during validation
    Error,
    /// Progress update
    Progress,
    /// Performance metrics update
    Metrics,
}

/// Validation context for events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationContext {
    pub validator_name: String,
    pub test_name: Option<String>,
    pub file_path: Option<String>,
    pub progress_percent: Option<f64>,
    pub metrics: Option<HashMap<String, f64>>,
}

impl RealtimeValidator {
    /// Create a new real-time validator
    pub fn new(config: MonitoringConfig) -> Result<Self> {
        let current_status = ValidationEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: ValidationEventType::Started,
            message: "Real-time validation monitoring initialized".to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: None,
            context: ValidationContext {
                validator_name: "RealtimeValidator".to_string(),
                test_name: None,
                file_path: None,
                progress_percent: Some(0.0),
                metrics: None,
            },
        };

        Ok(Self {
            config,
            current_status,
            event_history: Vec::new(),
        })
    }

    /// Get current validation status
    pub fn get_current_status(&self) -> Result<ValidationEvent> {
        Ok(self.current_status.clone())
    }

    /// Start real-time monitoring
    pub async fn start_monitoring(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        log::info!("Starting real-time validation monitoring");

        // In a real implementation, this would start a background monitoring task
        // For now, just log that monitoring is started
        Ok(())
    }

    /// Stop real-time monitoring
    pub async fn stop_monitoring(&self) -> Result<()> {
        log::info!("Stopping real-time validation monitoring");
        Ok(())
    }

    /// Record a validation event
    pub fn record_event(&mut self, event: ValidationEvent) -> Result<()> {
        // Apply event filters
        if self.should_record_event(&event) {
            self.event_history.push(event.clone());
            self.current_status = event;

            // Maintain event history size limit
            if self.event_history.len() > self.config.max_event_history {
                self.event_history.remove(0);
            }
        }

        Ok(())
    }

    /// Check if event should be recorded based on filters
    fn should_record_event(&self, event: &ValidationEvent) -> bool {
        if self.config.event_filters.is_empty() {
            return true;
        }

        self.config.event_filters.iter().any(|filter| {
            if !filter.enabled {
                return false;
            }

            match &filter.event_type {
                Some(event_type) => event_type == &event.event_type,
                None => true, // No specific type filter
            }
        })
    }

    /// Get event history
    pub fn get_event_history(&self) -> &[ValidationEvent] {
        &self.event_history
    }

    /// Get recent events
    pub fn get_recent_events(&self, count: usize) -> Vec<ValidationEvent> {
        let start_index = self.event_history.len().saturating_sub(count);
        self.event_history[start_index..].to_vec()
    }

    /// Get events by type
    pub fn get_events_by_type(&self, event_type: ValidationEventType) -> Vec<ValidationEvent> {
        self.event_history
            .iter()
            .filter(|event| event.event_type == event_type)
            .cloned()
            .collect()
    }

    /// Get validation statistics
    pub fn get_statistics(&self) -> ValidationStatistics {
        let total_events = self.event_history.len();
        let completed_events = self
            .get_events_by_type(ValidationEventType::Completed)
            .len();
        let failed_events = self.get_events_by_type(ValidationEventType::Failed).len();
        let error_events = self.get_events_by_type(ValidationEventType::Error).len();
        let warning_events = self.get_events_by_type(ValidationEventType::Warning).len();

        let success_rate = if total_events > 0 {
            (completed_events as f64 / total_events as f64) * 100.0
        } else {
            0.0
        };

        ValidationStatistics {
            total_events,
            completed_events,
            failed_events,
            error_events,
            warning_events,
            success_rate,
            monitoring_enabled: self.config.enabled,
            monitoring_interval_ms: self.config.monitoring_interval_ms,
        }
    }

    /// Create a progress event
    pub fn create_progress_event(
        validator_name: &str,
        test_name: &str,
        progress_percent: f64,
        message: &str,
    ) -> ValidationEvent {
        ValidationEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: ValidationEventType::Progress,
            message: message.to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: None,
            context: ValidationContext {
                validator_name: validator_name.to_string(),
                test_name: Some(test_name.to_string()),
                file_path: None,
                progress_percent: Some(progress_percent),
                metrics: None,
            },
        }
    }

    /// Create a metrics event
    pub fn create_metrics_event(
        validator_name: &str,
        metrics: HashMap<String, f64>,
        message: &str,
    ) -> ValidationEvent {
        ValidationEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: ValidationEventType::Metrics,
            message: message.to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: None,
            context: ValidationContext {
                validator_name: validator_name.to_string(),
                test_name: None,
                file_path: None,
                progress_percent: None,
                metrics: Some(metrics),
            },
        }
    }

    /// Create an error event
    pub fn create_error_event(
        validator_name: &str,
        test_name: Option<&str>,
        error_message: &str,
    ) -> ValidationEvent {
        ValidationEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: ValidationEventType::Error,
            message: error_message.to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: None,
            context: ValidationContext {
                validator_name: validator_name.to_string(),
                test_name: test_name.map(|s| s.to_string()),
                file_path: None,
                progress_percent: None,
                metrics: None,
            },
        }
    }
}

/// Validation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStatistics {
    pub total_events: usize,
    pub completed_events: usize,
    pub failed_events: usize,
    pub error_events: usize,
    pub warning_events: usize,
    pub success_rate: f64,
    pub monitoring_enabled: bool,
    pub monitoring_interval_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_realtime_validator_creation() {
        let config = MonitoringConfig::default();
        let validator = RealtimeValidator::new(config);
        assert!(validator.is_ok());
    }

    #[test]
    fn test_event_recording() {
        let config = MonitoringConfig::default();
        let mut validator = RealtimeValidator::new(config).unwrap();

        let event = ValidationEvent {
            event_id: "test".to_string(),
            event_type: ValidationEventType::Completed,
            message: "Test completed".to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: Some(100),
            context: ValidationContext {
                validator_name: "TestValidator".to_string(),
                test_name: Some("test1".to_string()),
                file_path: None,
                progress_percent: Some(100.0),
                metrics: None,
            },
        };

        let result = validator.record_event(event);
        assert!(result.is_ok());
        assert_eq!(validator.event_history.len(), 1);
    }

    #[test]
    fn test_event_filtering() {
        let mut config = MonitoringConfig::default();
        config.event_filters = vec![EventFilter {
            event_type: Some(ValidationEventType::Error),
            enabled: true,
        }];

        let mut validator = RealtimeValidator::new(config).unwrap();

        // This should be recorded (Error type)
        let error_event =
            RealtimeValidator::create_error_event("TestValidator", Some("test1"), "Test error");
        validator.record_event(error_event).unwrap();

        // This should be filtered out (Completed type)
        let completed_event = ValidationEvent {
            event_id: "test".to_string(),
            event_type: ValidationEventType::Completed,
            message: "Test completed".to_string(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
            duration_ms: Some(100),
            context: ValidationContext {
                validator_name: "TestValidator".to_string(),
                test_name: Some("test1".to_string()),
                file_path: None,
                progress_percent: Some(100.0),
                metrics: None,
            },
        };
        validator.record_event(completed_event).unwrap();

        // Only the error event should be recorded
        assert_eq!(validator.event_history.len(), 1);
        assert_eq!(
            validator.event_history[0].event_type,
            ValidationEventType::Error
        );
    }

    #[test]
    fn test_event_history_limit() {
        let mut config = MonitoringConfig::default();
        config.max_event_history = 2;

        let mut validator = RealtimeValidator::new(config).unwrap();

        // Add 3 events
        for i in 0..3 {
            let event = ValidationEvent {
                event_id: format!("test{}", i),
                event_type: ValidationEventType::Progress,
                message: format!("Progress {}", i),
                details: HashMap::new(),
                timestamp: chrono::Utc::now(),
                duration_ms: None,
                context: ValidationContext {
                    validator_name: "TestValidator".to_string(),
                    test_name: Some(format!("test{}", i)),
                    file_path: None,
                    progress_percent: Some(i as f64 * 33.3),
                    metrics: None,
                },
            };
            validator.record_event(event).unwrap();
        }

        // Should only keep the last 2 events
        assert_eq!(validator.event_history.len(), 2);
        assert_eq!(validator.event_history[0].event_id, "test1");
        assert_eq!(validator.event_history[1].event_id, "test2");
    }

    #[test]
    fn test_validation_statistics() {
        let config = MonitoringConfig::default();
        let mut validator = RealtimeValidator::new(config).unwrap();

        // Add various types of events
        let events = vec![
            RealtimeValidator::create_error_event("TestValidator", Some("test1"), "Error 1"),
            RealtimeValidator::create_progress_event("TestValidator", "test2", 50.0, "Progress"),
            ValidationEvent {
                event_id: "completed".to_string(),
                event_type: ValidationEventType::Completed,
                message: "Completed".to_string(),
                details: HashMap::new(),
                timestamp: chrono::Utc::now(),
                duration_ms: Some(100),
                context: ValidationContext {
                    validator_name: "TestValidator".to_string(),
                    test_name: Some("test3".to_string()),
                    file_path: None,
                    progress_percent: Some(100.0),
                    metrics: None,
                },
            },
        ];

        for event in events {
            validator.record_event(event).unwrap();
        }

        let stats = validator.get_statistics();
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.completed_events, 1);
        assert_eq!(stats.error_events, 1);
        assert!((stats.success_rate - 33.333).abs() < 0.1); // Approximately 33.33%
    }

    #[test]
    fn test_get_events_by_type() {
        let config = MonitoringConfig::default();
        let mut validator = RealtimeValidator::new(config).unwrap();

        // Add error and progress events
        validator
            .record_event(RealtimeValidator::create_error_event(
                "TestValidator",
                Some("test1"),
                "Error 1",
            ))
            .unwrap();
        validator
            .record_event(RealtimeValidator::create_progress_event(
                "TestValidator",
                "test2",
                50.0,
                "Progress",
            ))
            .unwrap();
        validator
            .record_event(RealtimeValidator::create_error_event(
                "TestValidator",
                Some("test3"),
                "Error 2",
            ))
            .unwrap();

        let error_events = validator.get_events_by_type(ValidationEventType::Error);
        let progress_events = validator.get_events_by_type(ValidationEventType::Progress);

        assert_eq!(error_events.len(), 2);
        assert_eq!(progress_events.len(), 1);
    }
}
