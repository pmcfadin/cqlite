//! Threading utilities

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};

/// Threading provider
#[derive(Debug)]
pub struct ThreadingProvider {
    /// Thread pool for CPU-intensive tasks
    cpu_pool: Arc<Semaphore>,

    /// Thread pool for I/O tasks
    io_pool: Arc<Semaphore>,
}

impl ThreadingProvider {
    /// Create a new threading provider
    pub fn new() -> Self {
        let cpu_count = num_cpus::get();

        Self {
            cpu_pool: Arc::new(Semaphore::new(cpu_count)),
            io_pool: Arc::new(Semaphore::new(cpu_count * 2)),
        }
    }

    /// Execute CPU-intensive task
    pub async fn execute_cpu_task<F, T>(&self, task: F) -> crate::Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let _permit = self
            .cpu_pool
            .acquire()
            .await
            .map_err(|e| crate::Error::storage(e.to_string()))?;

        let result = tokio::task::spawn_blocking(task)
            .await
            .map_err(|e| crate::Error::storage(e.to_string()))?;

        Ok(result)
    }

    /// Execute I/O task
    pub async fn execute_io_task<F, T>(&self, task: F) -> crate::Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let _permit = self
            .io_pool
            .acquire()
            .await
            .map_err(|e| crate::Error::storage(e.to_string()))?;

        let result = tokio::task::spawn_blocking(task)
            .await
            .map_err(|e| crate::Error::storage(e.to_string()))?;

        Ok(result)
    }

    /// Create a new mutex
    pub fn create_mutex<T>(&self, value: T) -> Arc<Mutex<T>> {
        Arc::new(Mutex::new(value))
    }

    /// Create a new read-write lock
    pub fn create_rwlock<T>(&self, value: T) -> Arc<RwLock<T>> {
        Arc::new(RwLock::new(value))
    }

    /// Get CPU count
    pub fn cpu_count(&self) -> usize {
        num_cpus::get()
    }
}
