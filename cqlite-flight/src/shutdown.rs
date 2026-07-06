//! Graceful-shutdown signal for the Flight server (issue #1473).
//!
//! [`shutdown_signal`] resolves when the process is asked to terminate — on
//! `ctrl_c` (SIGINT) or, on unix, SIGTERM. Passed to
//! `tonic::transport::Server::serve_with_shutdown`, it lets the server stop
//! accepting new connections and drain in-flight RPCs instead of tearing the
//! process (and every open stream) down abruptly.

/// Resolve when a shutdown signal (`ctrl_c`, or SIGTERM on unix) arrives.
///
/// A failure to install a listener is logged and that particular source is left
/// pending rather than resolving immediately — a broken handler must never
/// spuriously trigger a drain. On unix both sources race via `select!`; on other
/// platforms only `ctrl_c` is awaited.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for ctrl_c; ignoring this source");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler; ignoring this source");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!("shutdown signal received; draining in-flight RPCs");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The future must be `Send` (tonic drives it on the multi-thread runtime)
    /// and must NOT resolve on its own without a signal — a shutdown that fires
    /// spuriously would drain the server the moment it started.
    #[tokio::test]
    async fn does_not_resolve_without_a_signal() {
        // Type-level assertion that the wiring compiles into a Send future.
        fn assert_send<T: Send>(_: &T) {}
        let fut = shutdown_signal();
        assert_send(&fut);

        let raced = tokio::time::timeout(std::time::Duration::from_millis(100), fut).await;
        assert!(
            raced.is_err(),
            "shutdown_signal resolved without any signal being delivered"
        );
    }
}
