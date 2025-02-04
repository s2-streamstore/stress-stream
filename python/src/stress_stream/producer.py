import asyncio
import os
import random
import struct
from datetime import datetime

import typer
import uvloop
from prometheus_client import Counter, Histogram
from streamstore import S2, Stream
from streamstore.schemas import AppendInput, Record
from streamstore.utils import metered_bytes

from stress_stream._constants import (
    HISTOGRAM_BUCKETS,
    METRICS_LABELNAMES,
    METRICS_NAMESPACE,
    S2_AUTH_TOKEN,
)
from stress_stream._utils import metrics_server

CYCLES_PER_SEC = 10

TIMESTAMP_HEADER_NUM_BYTES = 10

MAX_BATCH_SIZE = 1000
BATCH_MAX_METERED_BYTES = 1024 * 1024  # 1MiB

appended_records_counter = Counter(
    name="appended_records_total",
    documentation="Total number of records successfully appended.",
    labelnames=METRICS_LABELNAMES,
    namespace=METRICS_NAMESPACE,
)
appended_bytes_counter = Counter(
    name="appended_bytes_total",
    documentation="Total number of bytes successfully appended.",
    labelnames=METRICS_LABELNAMES,
    namespace=METRICS_NAMESPACE,
)
append_latency_histogram = Histogram(
    name="append_latency_seconds",
    documentation="Time taken for each append operation in seconds.",
    labelnames=METRICS_LABELNAMES,
    namespace=METRICS_NAMESPACE,
    buckets=HISTOGRAM_BUCKETS,
)
appends_counter = Counter(
    name="appends_total",
    documentation="Total number of append operations.",
    labelnames=METRICS_LABELNAMES,
    namespace=METRICS_NAMESPACE,
)
append_failures_counter = Counter(
    name="append_failures_total",
    documentation="Total number of failed append operations.",
    labelnames=METRICS_LABELNAMES,
    namespace=METRICS_NAMESPACE,
)


async def append(basin_name: str, stream: Stream, input: AppendInput) -> None:
    start = datetime.now()
    try:
        appends_counter.labels(basin_name, stream.name).inc()
        output = await stream.append(input)
        appended_bytes_counter.labels(basin_name, stream.name).inc(
            sum(
                len(record.body) + TIMESTAMP_HEADER_NUM_BYTES
                for record in input.records
            )
        )
        appended_records_counter.labels(basin_name, stream.name).inc(
            output.end_seq_num - output.start_seq_num
        )
    except Exception:
        append_failures_counter.labels(basin_name, stream.name).inc()
    elapsed = datetime.now() - start
    append_latency_histogram.labels(basin_name, stream.name).observe(
        elapsed.total_seconds()
    )


async def produce_records(
    basin_name: str,
    stream: Stream,
    batch_size: range,
    record_body_size: range,
    per_cycle_target_num_bytes: float,
):
    cur_cycle_num_bytes = 0
    async with asyncio.TaskGroup() as tg:
        while cur_cycle_num_bytes < per_cycle_target_num_bytes:
            records: list[Record] = []
            batch_metered_bytes = 0
            for _ in range(random.choice(batch_size)):
                record = Record(
                    body=os.urandom(random.choice(record_body_size)),
                    headers=[(b"ts", struct.pack("d", datetime.now().timestamp()))],
                )
                record_metered_bytes = metered_bytes([record])
                if (
                    batch_metered_bytes + record_metered_bytes
                    <= BATCH_MAX_METERED_BYTES
                    and len(records) < MAX_BATCH_SIZE
                ):
                    records.append(record)
                    batch_metered_bytes += record_metered_bytes
                    cur_cycle_num_bytes += len(record.body) + TIMESTAMP_HEADER_NUM_BYTES
                    if cur_cycle_num_bytes >= per_cycle_target_num_bytes:
                        break
                else:
                    break
            if len(records) > 0:
                tg.create_task(append(basin_name, stream, AppendInput(records)))


async def producer(
    basin_name: str,
    stream_name: str,
    throughput: float,
    batch_size: range,
    record_body_size: range,
) -> None:
    async with S2(auth_token=S2_AUTH_TOKEN) as s2:
        stream = s2[basin_name][stream_name]
        per_cycle_target_num_bytes = throughput * (1 / CYCLES_PER_SEC)
        async with asyncio.TaskGroup() as tg:
            while True:
                tg.create_task(
                    produce_records(
                        basin_name,
                        stream,
                        batch_size,
                        record_body_size,
                        per_cycle_target_num_bytes,
                    )
                )
                await asyncio.sleep(1 / CYCLES_PER_SEC)


async def producer_and_metrics_server(
    basin_name: str,
    stream_name: str,
    throughput: float,
    batch_size: range,
    record_body_size: range,
) -> None:
    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            producer(
                basin_name,
                stream_name,
                throughput,
                batch_size,
                record_body_size,
            )
        )
        tg.create_task(metrics_server())


def main(
    basin: str = typer.Option(..., help="Name of the basin."),
    stream: str = typer.Option(..., help="Name of the stream."),
    throughput: int = typer.Option(..., help="Target throughput in bytes per second."),
    avg_record_size: int = typer.Option(
        ...,
        help=(
            f"Average size of each record in bytes. Must be greater than {TIMESTAMP_HEADER_NUM_BYTES} bytes "
            "and meet the limits mentioned in https://s2.dev/docs/limits#records."
        ),
    ),
    avg_batch_size: int = typer.Option(
        ...,
        help="Average number of records per batch. Must meet the limits mentioned in https://s2.dev/docs/limits#records.",
    ),
    randomize: bool = typer.Option(
        False,
        help=(
            "Whether to randomize record sizes and batch sizes. If --no-randomize, all records will have "
            "the same size (i.e. --avg-record-size) and all batches will have the same number (i.e. --avg-batch-size) "
            "of records. If --randomize, record sizes and batch sizes will be randomly chosen such that their "
            "expected values are equal to the provided average values."
        ),
    ),
):
    if avg_record_size <= TIMESTAMP_HEADER_NUM_BYTES:
        raise ValueError(
            f"Average record size must be greater than {TIMESTAMP_HEADER_NUM_BYTES} bytes."
        )
    if avg_batch_size <= 0:
        raise ValueError("Average batch size must be greater than 0.")
    batch_size = (
        range(1, (avg_batch_size * 2) - 1)
        if randomize
        else range(avg_batch_size, avg_batch_size + 1)
    )
    record_body_size = (
        range(
            TIMESTAMP_HEADER_NUM_BYTES + 1,
            (avg_record_size * 2) - TIMESTAMP_HEADER_NUM_BYTES + 1,
        )
        if randomize
        else range(avg_record_size, avg_record_size + 1)
    )
    min_throughput = avg_batch_size * avg_record_size * CYCLES_PER_SEC
    if throughput < min_throughput:
        raise ValueError(
            f"Target throughput must be greater than {int(min_throughput)} to ensure that the "
            "producer can keep up with it."
        )
    uvloop.run(
        producer_and_metrics_server(
            basin,
            stream,
            throughput,
            batch_size,
            record_body_size,
        )
    )


if __name__ == "__main__":
    typer.run(main)
