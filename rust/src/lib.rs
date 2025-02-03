#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::time::SystemTime;

pub const TIMESTAMP_HEADER_NUM_BYTES: u64 = 10;

pub const METRICS_NAMESPACE: &str = "stress";
pub const BASIN_LABEL: &str = "basin";
pub const STREAM_LABEL: &str = "stream";

pub fn current_timestamp() -> eyre::Result<f64> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs_f64())
}

pub async fn metrics_server() -> eyre::Result<()> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Prefix("".to_string()),
            &[
                0.010, 0.020, 0.030, 0.040, 0.050, 0.075, 0.100, 0.200, 0.300, 0.400, 0.500, 0.750,
                1.0, 2.5, 5.0,
            ],
        )?
        .install_recorder()?;
    let mut router = axum::Router::new();
    router = router.route(
        "/metrics",
        axum::routing::get(move || handle_metrics(handle)),
    );
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn handle_metrics(handle: PrometheusHandle) -> String {
    handle.render()
}
