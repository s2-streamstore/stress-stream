use clap::Parser;
use eyre::eyre;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use s2::client::S2Endpoints;
use s2::types::{AppendInput, MeteredBytes};
use s2::{
    types::{AppendRecord, AppendRecordBatch, Header},
    Client, ClientConfig, StreamClient,
};
use std::time::Instant;
use std::{env, ops::Range, time::Duration};
use stress_stream::{
    current_timestamp, init_rustls, init_tracing, metrics_server, BASIN_LABEL, METRICS_NAMESPACE,
    STREAM_LABEL, TIMESTAMP_HEADER_NUM_BYTES,
};
use tokio::task::JoinSet;
use tracing::{error, info};

const CYCLES_PER_SEC: f64 = 10.0;

fn appended_records_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_appended_records_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn appended_bytes_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_appended_bytes_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn append_latency_histogram(basin: String, stream: String) -> metrics::Histogram {
    metrics::histogram!(
        format!("{METRICS_NAMESPACE}_append_latency_seconds"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream,
    )
}

fn appends_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_appends_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream
    )
}

fn append_failures_counter(basin: String, stream: String) -> metrics::Counter {
    metrics::counter!(
        format!("{METRICS_NAMESPACE}_append_failures_total"),
        BASIN_LABEL => basin,
        STREAM_LABEL => stream,
    )
}

async fn append(
    basin: String,
    stream: String,
    client: StreamClient,
    batch: AppendRecordBatch,
    batch_num_bytes: u64,
) -> eyre::Result<()> {
    appends_counter(basin.clone(), stream.clone()).increment(1);
    let start = Instant::now();
    let result = client.append(AppendInput::new(batch)).await;
    match result {
        Ok(output) => {
            append_latency_histogram(basin.clone(), stream.clone())
                .record(start.elapsed().as_secs_f64());
            appended_bytes_counter(basin.clone(), stream.clone()).increment(batch_num_bytes);
            appended_records_counter(basin.clone(), stream.clone())
                .increment(output.end_seq_num - output.start_seq_num);
        }
        Err(err) => {
            error!(?err, "append request failed");
            append_failures_counter(basin.clone(), stream.clone()).increment(1);
        }
    }
    Ok(())
}

async fn produce_records(
    basin: String,
    stream: String,
    mut rng: StdRng,
    client: StreamClient,
    target_num_bytes: u64,
    batch_size: Range<u64>,
    record_body_size: Range<u64>,
) -> eyre::Result<()> {
    let mut total_num_bytes = 0;
    let mut task_set = JoinSet::new();
    while total_num_bytes < target_num_bytes {
        let batch_size = rng.random_range(batch_size.clone());
        let mut batch = AppendRecordBatch::new();
        let mut batch_num_bytes = 0;
        for _ in 0..batch_size {
            let body_size = rng.random_range(record_body_size.clone());
            let mut body = vec![0u8; body_size as usize];
            rng.fill_bytes(&mut body[..]);
            let headers = vec![Header::new(
                "ts",
                current_timestamp()?.to_be_bytes().to_vec(),
            )];
            let record = AppendRecord::new(body)?.with_headers(headers)?;
            let record_num_bytes = body_size + TIMESTAMP_HEADER_NUM_BYTES;
            if batch.metered_bytes() + record.metered_bytes() <= AppendRecordBatch::MAX_BYTES
                && batch.len() < AppendRecordBatch::MAX_CAPACITY
            {
                batch
                    .push(record)
                    .map_err(|_| eyre!("Appending to a batch failed"))?;
                batch_num_bytes += record_num_bytes;
                total_num_bytes += record_num_bytes;
                if total_num_bytes >= target_num_bytes {
                    break;
                }
            } else {
                break;
            }
        }
        if !batch.is_empty() {
            task_set.spawn(append(
                basin.clone(),
                stream.clone(),
                client.clone(),
                batch,
                batch_num_bytes,
            ));
        }
    }
    task_set.join_all().await;
    Ok(())
}

async fn run_producer(
    auth_token: String,
    basin: String,
    stream: String,
    throughput: u64,
    batch_size: Range<u64>,
    record_body_size: Range<u64>,
) -> eyre::Result<()> {
    let endpoints = S2Endpoints::from_env().map_err(|e| eyre!(e))?;
    let config = ClientConfig::new(auth_token).with_endpoints(endpoints);
    let client = Client::new(config)
        .basin_client(basin.clone().try_into()?)
        .stream_client(stream.clone());
    let rng = rand::rngs::StdRng::from_os_rng();
    let per_cycle_target_num_bytes = (throughput as f64 * (1.0 / CYCLES_PER_SEC)) as u64;
    let mut cycle = tokio::time::interval(Duration::from_secs_f64(1.0 / CYCLES_PER_SEC));
    info!("starting production cycles");
    loop {
        cycle.tick().await;

        tokio::task::spawn(produce_records(
            basin.clone(),
            stream.clone(),
            rng.clone(),
            client.clone(),
            per_cycle_target_num_bytes,
            batch_size.clone(),
            record_body_size.clone(),
        ));
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, help = "Name of the basin")]
    basin: String,
    #[arg(long, help = "Name of the stream")]
    stream: String,
    #[arg(long, help = "Target throughput in bytes per second")]
    throughput: u64,
    #[arg(long, help = "Average size of each record in bytes")]
    avg_record_size: u64,
    #[arg(long, help = "Average number of records per batch")]
    avg_batch_size: u64,
    #[arg(long, help = "Whether to randomize record sizes and batch sizes")]
    randomize: bool,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    init_tracing();
    init_rustls();

    let args = Args::parse();
    let auth_token = env::var("S2_AUTH_TOKEN").expect("S2_AUTH_TOKEN env var should be set");
    let batch_size = if args.randomize {
        1..(args.avg_batch_size * 2 - 1)
    } else {
        args.avg_batch_size..(args.avg_batch_size + 1)
    };
    let record_body_size = if args.randomize {
        (TIMESTAMP_HEADER_NUM_BYTES + 1)
            ..(args.avg_record_size * 2 - TIMESTAMP_HEADER_NUM_BYTES + 1)
    } else {
        args.avg_record_size..(args.avg_record_size + 1)
    };
    tokio::spawn(metrics_server());
    run_producer(
        auth_token,
        args.basin,
        args.stream,
        args.throughput,
        batch_size,
        record_body_size,
    )
    .await?;
    Ok(())
}
