import asyncio
import struct
from datetime import datetime

import typer
from prometheus_client import Counter, Histogram
from streamstore import S2
from streamstore.schemas import SequencedRecord

from stress_stream._constants import HISTOGRAM_BUCKETS, METRICS_NAMESPACE, S2_AUTH_TOKEN
from stress_stream._utils import metrics_server

e2e_latency_histogram = Histogram(
    name="e2e_latency_seconds",
    documentation="End-to-end latency in seconds. This measures the time taken from when a record is produced "
    "(by the producer) to when it is read by the consumer.",
    namespace=METRICS_NAMESPACE,
    buckets=HISTOGRAM_BUCKETS,
)
read_records_counter = Counter(
    name="read_records_total",
    documentation="Total number of records successfully read.",
    namespace=METRICS_NAMESPACE,
)
read_bytes_counter = Counter(
    name="read_bytes_total",
    documentation="Total number of bytes successfully read.",
    namespace=METRICS_NAMESPACE,
)
reads_counter = Counter(
    name="reads_total",
    documentation="Total number of reads in read sessions.",
    namespace=METRICS_NAMESPACE,
)
read_failures_counter = Counter(
    name="read_failures_total",
    documentation="Total number of failed reads in read sessions.",
    namespace=METRICS_NAMESPACE,
)


def observe_metrics(record: SequencedRecord):
    name, val = record.headers[0]
    read_bytes_counter.inc(len(record.body) + len(name) + len(val))
    created_ts = struct.unpack("d", val)[0]
    e2e_latency = (datetime.now() - datetime.fromtimestamp(created_ts)).total_seconds()
    e2e_latency_histogram.observe(e2e_latency)


async def consumer(basin_name: str, stream_name: str):
    async with S2(auth_token=S2_AUTH_TOKEN) as s2:
        stream = s2[basin_name][stream_name]
        start_seq_num = await stream.check_tail()
        while True:
            try:
                reads_counter.inc()
                async for output in stream.read_session(start_seq_num):
                    reads_counter.inc()
                    match output:
                        case list(records):
                            if len(records) > 0:
                                start_seq_num = records[-1].seq_num + 1
                            read_records_counter.inc(len(records))
                            for record in records:
                                observe_metrics(record)
                        case unexpected:
                            raise RuntimeError(
                                "Unexpected output when reading from stream. "
                                f"Expected a batch of records, but got {unexpected}"
                            )
            except Exception:
                read_failures_counter.inc()


async def consumer_and_metrics_server(basin_name: str, stream_name: str):
    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer(basin_name, stream_name))
        tg.create_task(metrics_server())


def main(
    basin: str = typer.Option(..., help="Name of the basin."),
    stream: str = typer.Option(..., help="Name of the stream."),
):
    asyncio.run(consumer_and_metrics_server(basin, stream))


if __name__ == "__main__":
    typer.run(main)
