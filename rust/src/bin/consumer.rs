use clap::Parser;
use eyre::eyre;
use futures::StreamExt;
use s2::{
    types::{ReadOutput, ReadSessionRequest},
    Client, ClientConfig, StreamClient,
};
use std::env;
use stress_stream::{
    current_timestamp, init_rustls, init_tracing, metrics_server, BASIN_LABEL, METRICS_NAMESPACE,
    STREAM_LABEL, TIMESTAMP_HEADER_NUM_BYTES,
};
use tracing::{error, info};

fn read_records_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_read_records_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn read_bytes_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_read_bytes_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn e2e_latency_histogram(basin: String, stream: String) -> metrics::Histogram {
    metrics::histogram!(
        format!("{METRICS_NAMESPACE}_e2e_latency_seconds"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream,
    )
}

fn read_sessions_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_read_sessions_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn read_session_failures_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_read_session_failures_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream,
    )
}

async fn consume_records(
    basin: String,
    stream: String,
    client: StreamClient,
    start_seq_num: &mut u64,
) -> eyre::Result<()> {
    read_sessions_counter(basin.clone(), stream.clone()).increment(1);
    let mut read_stream = client
        .read_session(ReadSessionRequest::new(*start_seq_num))
        .await?;
    while let Some(output) = read_stream.next().await {
        let output = output?;
        match output {
            ReadOutput::Batch(batch) => {
                let mut batch_num_bytes = 0;
                for record in &batch.records {
                    if record.headers.len() != 1 {
                        return Err(eyre!("Unexpected number of headers"));
                    }
                    let header = &record.headers[0];
                    let created_ts = f64::from_be_bytes(header.value.as_ref().try_into()?);
                    e2e_latency_histogram(basin.clone(), stream.clone())
                        .record((current_timestamp()? - created_ts).max(0.0));
                    batch_num_bytes += record.body.len() as u64 + TIMESTAMP_HEADER_NUM_BYTES;
                }
                read_bytes_counter(basin.clone(), stream.clone()).increment(batch_num_bytes);
                read_records_counter(basin.clone(), stream.clone())
                    .increment(batch.records.len() as u64);
                if let Some(record) = batch.records.last() {
                    *start_seq_num = record.seq_num + 1;
                }
            }
            _ => return Err(eyre!("Unexpected output when reading from stream")),
        }
    }
    Ok(())
}

async fn run_consumer(auth_token: String, basin: String, stream: String) -> eyre::Result<()> {
    let config = ClientConfig::new(auth_token);
    let client = Client::new(config)
        .basin_client(basin.clone().try_into()?)
        .stream_client(stream.clone());
    let mut start_seq_num = client.check_tail().await?;
    info!("starting consumption");
    loop {
        let result = consume_records(
            basin.clone(),
            stream.clone(),
            client.clone(),
            &mut start_seq_num,
        )
        .await;
        if result.is_err() {
            error!(err=?result.err().unwrap(), "read session failed");
            read_session_failures_counter(basin.clone(), stream.clone()).increment(1);
        }
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, help = "Name of the basin")]
    basin: String,
    #[arg(long, help = "Name of the stream")]
    stream: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    init_tracing();
    init_rustls();

    let args = Args::parse();
    let auth_token = env::var("S2_AUTH_TOKEN").expect("S2_AUTH_TOKEN env var should be set");
    tokio::task::spawn(metrics_server());
    run_consumer(auth_token, args.basin, args.stream).await?;
    Ok(())
}
