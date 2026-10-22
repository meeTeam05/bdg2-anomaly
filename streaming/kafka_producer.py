"""Task 3: replay the Silver table (test period, 2017) into Kafka, one thread per site.

Tick k (every PRODUCER_INTERVAL_SEC) = hour start + k: each site thread sends the readings of
all its buildings at that hour, so all sites stay on the same replayed hour.

Live-demo spike: the --spike-count biggest consumers get meter_reading x --spike-factor from
tick --spike-after on (unlike the offline injected-anomaly test of Task 2).

    python streaming/kafka_producer.py --hours 48                                # real Silver
    python streaming/kafka_producer.py --silver streaming/mock_data/clean_data   # mock Silver

Only ANOMALY_METER_TYPES are replayed: the job scores nothing else. Reads Silver with
deltalake (no JVM). The producer only plays the IoT devices; all processing
happens in Spark (streaming_job.py).
"""

import argparse
import json
import logging
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import KafkaException, Producer
from deltalake import DeltaTable

from common.config import (
    ANOMALY_METER_TYPES,
    CLEAN_DATA_PATH,
    CLEAN_DATA_SCHEMA,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    PARTITION_COLUMN,
    PRODUCER_INTERVAL_SEC,
    TEST_START,
)

log = logging.getLogger("kafka_producer")

SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]


def parse_utc(text):
    """'2017-01-01 00:00:00' (taken as UTC) or with an explicit offset -> aware UTC datetime."""
    ts = datetime.fromisoformat(text)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def load_site(table, site, start, hours):
    """Readings of one site in [start, start + hours) of ANOMALY_METER_TYPES, as a pyarrow Table."""
    end = start + timedelta(hours=hours)
    return table.to_pyarrow_table(
        columns=SILVER_COLUMNS,
        filters=[
            (PARTITION_COLUMN, "=", site),
            ("meter_type", "in", ANOMALY_METER_TYPES),
            ("timestamp", ">=", start),
            ("timestamp", "<", end),
        ],
    )


def rows_at(site_table, ts):
    """All rows of a site table at one timestamp, as dicts (empty list if the hour is missing)."""
    ts_type = site_table.schema.field("timestamp").type
    return site_table.filter(
        pc.equal(site_table["timestamp"], pa.scalar(ts, type=ts_type))
    ).to_pylist()


def pick_spike_buildings(site_tables, count):
    """The `count` buildings with the highest mean meter_reading: a spike on a big consumer is
    clearly above the threshold, while one on a near-zero baseline proves nothing."""
    if count <= 0:
        return set()
    means = (
        pa.concat_tables(site_tables.values())
        .group_by("building_id")
        .aggregate([("meter_reading", "mean")])
    )
    top = means.sort_by([("meter_reading_mean", "descending")]).slice(0, count)
    return set(top["building_id"].to_pylist())


def to_message(row, spike_factor=1.0):
    """Silver row -> (key, JSON bytes). Key = building_id keeps each building in order."""
    payload = dict(row, meter_reading=row["meter_reading"] * spike_factor)
    payload["timestamp"] = row["timestamp"].isoformat()
    return row["building_id"], json.dumps(payload).encode()


class Stats:
    """Counters shared by all site threads."""

    def __init__(self):
        self.lock = threading.Lock()
        self.sent = 0
        self.failed = 0
        self.tick = 0
        self.crashed = False

    def on_delivery(self, err, _msg):
        if err is not None:
            with self.lock:
                self.failed += 1
            log.error("delivery failed: %s", err)

    def add_sent(self, n, tick):
        with self.lock:
            self.sent += n
            self.tick = max(self.tick, tick)


def replay_site(site, site_table, producer, args, start, spike_buildings, t0, stats, stop):
    """Send one replayed hour per tick. Ticks are scheduled from t0, so threads do not drift.

    A crash in one site stops every site: a silent half-replay is worse than a clear failure.
    """
    try:
        for tick in range(args.hours):
            if stop.wait(max(0.0, t0 + tick * args.interval - time.monotonic())):
                return
            ts = start + timedelta(hours=tick)
            rows = rows_at(site_table, ts)
            for row in rows:
                spiking = row["building_id"] in spike_buildings and tick >= args.spike_after
                key, value = to_message(row, args.spike_factor if spiking else 1.0)
                producer.produce(args.topic, key=key, value=value, on_delivery=stats.on_delivery)
            producer.poll(0)  # serve delivery callbacks
            stats.add_sent(len(rows), tick)
            log.debug("site %s tick %d (%s): %d rows", site, tick, ts, len(rows))
    except Exception:
        log.exception("site %s crashed, stopping all sites", site)
        stats.crashed = True
        stop.set()


def check_topic(producer, topic):
    """Fail fast with a readable message instead of hanging on a missing broker/topic."""
    try:
        metadata = producer.list_topics(topic, timeout=10)
    except KafkaException as e:
        sys.exit(f"Kafka not reachable ({e}). Start it: make infra-up")
    if metadata.topics[topic].error is not None:
        sys.exit(
            f"Topic '{topic}' does not exist (auto-create is off). "
            "It is created by the kafka-init service in infra/docker-compose.yml."
        )


def parse_args():
    p = argparse.ArgumentParser(description="Replay Silver (2017) into Kafka, one thread per site")
    p.add_argument("--silver", default=CLEAN_DATA_PATH, help="Silver Delta table (Task 1)")
    p.add_argument("--start", default=TEST_START, help="first replayed hour, UTC")
    p.add_argument("--hours", type=int, default=24 * 365, help="number of replayed hours (ticks)")
    p.add_argument("--interval", type=float, default=PRODUCER_INTERVAL_SEC, help="seconds per tick")
    p.add_argument("--sites", nargs="*", help="only these site_id (default: all)")
    p.add_argument("--spike-count", type=int, default=2, help="buildings that spike, 0 = no spike")
    p.add_argument("--spike-after", type=int, default=10, help="tick at which the spike starts")
    p.add_argument("--spike-factor", type=float, default=3.0)
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=KAFKA_TOPIC)
    p.add_argument("--verbose", action="store_true", help="log every site and tick")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [%(threadName)s]: %(message)s",
    )
    if not Path(args.silver).exists():
        sys.exit(
            f"Silver table not found: {args.silver}. Run make task1 or create a mock: "
            "python streaming/mock_inputs.py"
        )

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})
    check_topic(producer, args.topic)

    table = DeltaTable(args.silver)
    sites = sorted(args.sites or {p[PARTITION_COLUMN] for p in table.partitions()})
    start = parse_utc(args.start)
    loaded = time.time()
    site_tables = {site: load_site(table, site, start, args.hours) for site in sites}
    total = sum(t.num_rows for t in site_tables.values())
    log.info(
        "loaded %d rows, %d sites, %d hours from %s in %.1fs",
        total,
        len(sites),
        args.hours,
        args.start,
        time.time() - loaded,
    )
    spike_buildings = pick_spike_buildings(site_tables, args.spike_count)
    if spike_buildings:
        log.info(
            "spike x%.1f from tick %d on: %s",
            args.spike_factor,
            args.spike_after,
            sorted(spike_buildings),
        )

    stats, stop = Stats(), threading.Event()
    t0 = time.monotonic()
    threads = [
        threading.Thread(
            target=replay_site,
            name=f"site-{site}",
            daemon=True,
            args=(site, site_tables[site], producer, args, start, spike_buildings, t0, stats, stop),
        )
        for site in sites
    ]
    for t in threads:
        t.start()
    try:
        while alive := [t for t in threads if t.is_alive()]:
            alive[0].join(args.interval * 5)
            log.info(
                "tick %d/%d: %d sent, %d failed",
                stats.tick + 1,
                args.hours,
                stats.sent,
                stats.failed,
            )
    except KeyboardInterrupt:
        log.info("stopping (Ctrl+C)")
        stop.set()
    for t in threads:
        t.join()
    producer.flush(10)
    log.info("done: %d sent, %d failed", stats.sent, stats.failed)
    if stats.crashed or stats.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
