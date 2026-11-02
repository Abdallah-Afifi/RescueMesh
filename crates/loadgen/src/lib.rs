//! Workload generator and client-machine frontend (workstream E).
//!
//! On each client machine `rm-loadgen run` (the frontend) starts K worker
//! processes; each worker runs M simulated users as asynchronous tasks
//! (design log DL-018). Every worker writes its own event log, and the
//! frontend merges the workers' summaries.

pub mod frontend;
pub mod params;
pub mod sampler;
pub mod summary;
pub mod worker;
pub mod workload;

/// Resolves on Ctrl-C (SIGINT) or SIGTERM. The experiment runner and the demo
/// stop processes with SIGTERM, so a run must drain and write its summary on
/// either (DL-067). If a signal cannot be watched, that source never fires;
/// it must not end the run.
pub async fn stop_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut t) => {
                t.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
