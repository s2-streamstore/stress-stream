import asyncio
import os
import random
import struct
from datetime import datetime

import typer
from prometheus_client import Counter, Histogram
from streamstore import S2, Stream
from streamstore.schemas import AppendInput, Record
from streamstore.utils import metered_bytes

from stress_stream._constants import HISTOGRAM_BUCKETS, METRICS_NAMESPACE, S2_AUTH_TOKEN
from stress_stream._utils import metrics_server

CYCLE_EVERY = 0.5  # 0.5s or 500ms
MAX_JITTER = 0.2  # 0.2s or 200ms
TIMESTAMP_HEADER_NUM_BYTES = 10

MAX_BATCH_SIZE = 1000
BATCH_MAX_METERED_BYTES = 1024 * 1024  # 1MiB

appended_records_counter = Counter(
    name="appended_records_total",
    documentation="Total number of records successfully appended.",
    namespace=METRICS_NAMESPACE,
)
appended_bytes_counter = Counter(
    name="appended_bytes_total",
    documentation="Total number of bytes successfully appended.",
    namespace=METRICS_NAMESPACE,
)
append_latency_histogram = Histogram(
    name="append_latency_seconds",
    documentation="Time taken for each append operation in seconds.",
    namespace=METRICS_NAMESPACE,
    buckets=HISTOGRAM_BUCKETS,
)
appends_counter = Counter(
    name="appends_total",
    documentation="Total number of append operations.",
    namespace=METRICS_NAMESPACE,
)
append_failures_counter = Counter(
    name="append_failures_total",
    documentation="Total number of failed append operations.",
    namespace=METRICS_NAMESPACE,
)


async def append(stream: Stream, input: AppendInput) -> None:
    start = datetime.now()
    with append_failures_counter.count_exceptions():
        appends_counter.inc()
        output = await stream.append(input)
        appended_bytes_counter.inc(
            sum(
                len(record.body) + TIMESTAMP_HEADER_NUM_BYTES
                for record in input.records
            )
        )
        appended_records_counter.inc(output.end_seq_num - output.start_seq_num)
    elapsed = datetime.now() - start
    append_latency_histogram.observe(elapsed.total_seconds())


async def produce_records(
    stream: Stream,
    batch_size: range,
    record_body_size: range,
    per_cycle_target_num_bytes: float,
):
    cur_cycle_num_bytes = 0
    append_coros = []
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
                batch_metered_bytes + record_metered_bytes <= BATCH_MAX_METERED_BYTES
                and len(records) <= MAX_BATCH_SIZE
            ):
                records.append(record)
                batch_metered_bytes += record_metered_bytes
                cur_cycle_num_bytes += len(record.body) + TIMESTAMP_HEADER_NUM_BYTES
                if cur_cycle_num_bytes >= per_cycle_target_num_bytes:
                    break
            else:
                break
        if len(records) > 0:
            append_coros.append(append(stream, AppendInput(records)))
    await asyncio.gather(*append_coros, return_exceptions=True)


async def producer(
    basin_name: str,
    stream_name: str,
    throughput: float,
    batch_size: range,
    record_body_size: range,
) -> None:
    async with S2(auth_token=S2_AUTH_TOKEN) as s2:
        stream = s2[basin_name][stream_name]
        per_cycle_target_num_bytes = throughput * CYCLE_EVERY
        while True:
            start = datetime.now()
            await asyncio.sleep(random.uniform(0.0, MAX_JITTER))
            await produce_records(
                stream, batch_size, record_body_size, per_cycle_target_num_bytes
            )
            elapsed = (datetime.now() - start).total_seconds()
            await asyncio.sleep(max(0, CYCLE_EVERY - elapsed))


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
        help="Average number of records per batch. Must meet the limits mentioned in https://s2.dev/docs/limits#records",
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
    min_throughput = (
        (batch_size.stop * record_body_size.stop) / CYCLE_EVERY
        if randomize
        else (avg_batch_size * avg_record_size) / CYCLE_EVERY
    )
    if throughput < min_throughput:
        raise ValueError(
            f"Target throughput must be greater than {int(min_throughput)} to ensure that the "
            "producer can keep up with it."
        )
    asyncio.run(
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
