"""Tests for kafka_producer.py without Kafka (pure functions + a fake producer).

Requirements: TASKS.md Task 3 "Kafka Producer" and "Kịch bản tăng đột biến".
"""

import json
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from common.config import (
    CLEAN_DATA_PATH,
    CLEAN_DATA_SCHEMA,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    PRODUCER_INTERVAL_SEC,
    TEST_START,
)
from streaming import kafka_producer as kp
from streaming.tests.support import STREAMING_DIR

T0 = datetime(2017, 1, 1, tzinfo=UTC)
SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]
ARROW_TYPES = {
    "string": pa.string(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "double": pa.float64(),
    "int": pa.int32(),
}
ARROW_SCHEMA = pa.schema([(name, ARROW_TYPES[dtype]) for name, dtype in CLEAN_DATA_SCHEMA])


def silver_row(building_id, site_id, ts, meter_reading):
    """One Silver row (CLEAN_DATA_SCHEMA) with dummy weather/feature values."""
    return {
        "building_id": building_id,
        "meter_type": "electricity",
        "site_id": site_id,
        "timestamp": ts,
        "meter_reading": meter_reading,
        "air_temperature": 5.0,
        "dew_temperature": 1.0,
        "hour_of_day": ts.hour,
        "day_of_week": 1,
        "is_weekend": 1,
        "square_feet": 1000.0,
        "primary_use": "Office",
    }


def silver_table(buildings, start, hours):
    """pyarrow table: each (building_id, site_id, mean) of `buildings`, `hours` hours from start."""
    rows = [
        silver_row(b, s, start + timedelta(hours=h), mean + h)
        for h in range(hours)
        for b, s, mean in buildings
    ]
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


SITE_A = [("A_big", "A", 500.0), ("A_small", "A", 10.0)]
SITE_B = [("B_mid", "B", 100.0), ("B_tiny", "B", 1.0), ("B_other", "B", 50.0)]


class FakeProducer:
    """Records produce() calls with the monotonic time they happened."""

    def __init__(self, fail_on_key=None):
        self.sent = []
        self.lock = threading.Lock()
        self.fail_on_key = fail_on_key

    def produce(self, topic, key, value, on_delivery=None):
        if key == self.fail_on_key:
            raise RuntimeError("boom")
        with self.lock:
            self.sent.append((time.monotonic(), key, json.loads(value)))

    def poll(self, _timeout):
        return 0


def run_sites(site_tables, args, producer, spike_buildings=frozenset()):
    """Run replay_site for every site in its own thread, like main() does."""
    stats, stop, t0 = kp.Stats(), threading.Event(), time.monotonic()
    threads = [
        threading.Thread(
            target=kp.replay_site,
            args=(site, table, producer, args, T0, spike_buildings, t0, stats, stop),
        )
        for site, table in site_tables.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    return stats, t0


def replay_args(hours=4, interval=0.3, spike_after=2, spike_factor=3.0):
    return SimpleNamespace(
        hours=hours,
        interval=interval,
        spike_after=spike_after,
        spike_factor=spike_factor,
        topic="t",
    )


# ---------- defaults come from common/config.py ----------


def test_defaults_come_from_config(monkeypatch):
    """Default Silver path, start, interval, topic and bootstrap are the config.py constants."""
    monkeypatch.setattr(sys, "argv", ["kafka_producer.py"])
    args = kp.parse_args()
    assert args.silver == CLEAN_DATA_PATH
    assert args.start == TEST_START
    assert args.interval == PRODUCER_INTERVAL_SEC
    assert args.topic == KAFKA_TOPIC
    assert args.bootstrap == KAFKA_BOOTSTRAP


def test_parse_utc_keeps_naive_as_utc_and_converts_offsets():
    """--start without offset is UTC; with an offset it is converted, not relabelled."""
    assert kp.parse_utc(TEST_START) == T0
    assert kp.parse_utc("2017-01-01T07:00:00+07:00") == T0


def test_producer_interval_is_2_to_3_seconds():
    """TASKS.md: one tick every 2-3 s."""
    assert 2 <= PRODUCER_INTERVAL_SEC <= 3


# ---------- rows_at / load_site ----------


def test_rows_at_returns_all_buildings_of_one_hour():
    """rows_at gives every building of the site at exactly that timestamp."""
    table = silver_table(SITE_B, T0, hours=3)
    rows = kp.rows_at(table, T0 + timedelta(hours=1))
    assert sorted(r["building_id"] for r in rows) == ["B_mid", "B_other", "B_tiny"]
    assert {r["timestamp"] for r in rows} == {T0 + timedelta(hours=1)}


def test_rows_at_missing_hour_is_empty():
    """An hour without data gives an empty list, not an error."""
    table = silver_table(SITE_B, T0, hours=3)
    assert kp.rows_at(table, T0 + timedelta(hours=10)) == []


def test_load_site_keeps_only_one_site_and_test_period(tmp_path):
    """load_site reads one site partition and only [start, start + hours), start = TEST_START."""
    before = datetime(2016, 12, 31, 22, tzinfo=UTC)  # 2 hours of train data, then 2017
    table = pa.concat_tables(
        [silver_table(SITE_A, before, hours=6), silver_table(SITE_B, before, hours=6)]
    )
    write_deltalake(str(tmp_path), table, partition_by=["site_id"])
    start = datetime.fromisoformat(TEST_START).replace(tzinfo=UTC)

    site_a = kp.load_site(DeltaTable(str(tmp_path)), "A", start, hours=3)

    assert set(site_a["site_id"].to_pylist()) == {"A"}
    assert sorted(set(site_a["timestamp"].to_pylist())) == [
        T0,
        T0 + timedelta(hours=1),
        T0 + timedelta(hours=2),
    ]
    assert site_a.num_rows == 3 * len(SITE_A)
    assert site_a.column_names == SILVER_COLUMNS


def test_load_site_replays_only_anomaly_meter_types(tmp_path):
    """Steam rows of the same site are not replayed: the job scores electricity only."""
    electricity = silver_table(SITE_A, T0, hours=2)
    steam = electricity.set_column(
        electricity.schema.get_field_index("meter_type"),
        "meter_type",
        pa.array(["steam"] * electricity.num_rows),
    )
    write_deltalake(str(tmp_path), pa.concat_tables([electricity, steam]), partition_by=["site_id"])
    site_a = kp.load_site(DeltaTable(str(tmp_path)), "A", T0, hours=2)
    assert set(site_a["meter_type"].to_pylist()) == {"electricity"}
    assert site_a.num_rows == electricity.num_rows


def test_load_site_on_spark_written_mock_silver(mock_inputs):
    """On the Spark-written mock Silver: only 2017 rows, 4 buildings per hour, UTC timestamps."""
    silver = mock_inputs / "clean_data"
    start = datetime.fromisoformat(TEST_START).replace(tzinfo=UTC)
    fox = kp.load_site(DeltaTable(str(silver)), "Fox", start, hours=24)
    timestamps = fox["timestamp"].to_pylist()
    assert fox.num_rows == 24 * 4
    assert min(timestamps) == T0 and max(timestamps) == T0 + timedelta(hours=23)
    assert len(kp.rows_at(fox, T0 + timedelta(hours=5))) == 4


# ---------- spike buildings ----------


def test_pick_spike_buildings_takes_highest_mean_over_all_sites():
    """The spike goes to the biggest consumers across every site."""
    tables = {"A": silver_table(SITE_A, T0, 3), "B": silver_table(SITE_B, T0, 3)}
    assert kp.pick_spike_buildings(tables, 1) == {"A_big"}
    assert kp.pick_spike_buildings(tables, 2) == {"A_big", "B_mid"}


def test_pick_spike_buildings_zero_means_no_spike():
    """--spike-count 0 disables the spike."""
    tables = {"A": silver_table(SITE_A, T0, 3)}
    assert kp.pick_spike_buildings(tables, 0) == set()


# ---------- message format ----------


def test_to_message_is_silver_json_keyed_by_building():
    """Key = building_id, value = JSON of exactly the Silver columns, ISO timestamp with offset."""
    row = silver_row("A_big", "A", T0 + timedelta(hours=5), 123.5)
    key, value = kp.to_message(row)
    payload = json.loads(value)
    assert key == "A_big"
    assert list(payload) == SILVER_COLUMNS
    assert payload["timestamp"] == "2017-01-01T05:00:00+00:00"
    assert payload["meter_reading"] == 123.5
    assert payload["hour_of_day"] == 5


def test_to_message_spike_only_changes_meter_reading():
    """spike_factor multiplies meter_reading and nothing else (input row untouched)."""
    row = silver_row("A_big", "A", T0, 100.0)
    normal = json.loads(kp.to_message(row)[1])
    spiked = json.loads(kp.to_message(row, spike_factor=3.0)[1])
    assert spiked["meter_reading"] == 300.0
    assert {k: v for k, v in spiked.items() if k != "meter_reading"} == {
        k: v for k, v in normal.items() if k != "meter_reading"
    }
    assert row["meter_reading"] == 100.0


# ---------- threads, ticks, spike (fake producer) ----------


def test_each_tick_sends_all_buildings_of_a_site_at_same_hour():
    """Tick k: every site sends all its buildings for hour start + k; sites stay on one hour."""
    producer = FakeProducer()
    tables = {"A": silver_table(SITE_A, T0, 4), "B": silver_table(SITE_B, T0, 4)}
    stats, _ = run_sites(tables, replay_args(hours=4, spike_after=99), producer)

    assert stats.sent == 4 * 5 and not stats.crashed
    for tick in range(4):
        ts = (T0 + timedelta(hours=tick)).isoformat()
        sent_now = sorted(key for _, key, msg in producer.sent if msg["timestamp"] == ts)
        assert sent_now == ["A_big", "A_small", "B_mid", "B_other", "B_tiny"]


def test_ticks_are_evenly_paced():
    """Hour k is sent about k * interval after the start (scheduled from t0, no drift)."""
    interval = 0.3
    producer = FakeProducer()
    tables = {"A": silver_table(SITE_A, T0, 5), "B": silver_table(SITE_B, T0, 5)}
    _, t0 = run_sites(tables, replay_args(hours=5, interval=interval, spike_after=99), producer)

    for sent_at, _, msg in producer.sent:
        tick = datetime.fromisoformat(msg["timestamp"]).hour
        assert sent_at - t0 == pytest.approx(tick * interval, abs=0.1)


def test_spike_hits_only_chosen_buildings_from_spike_tick_on():
    """Chosen buildings get meter_reading x factor from tick --spike-after on, the others never."""
    producer = FakeProducer()
    tables = {"A": silver_table(SITE_A, T0, 4), "B": silver_table(SITE_B, T0, 4)}
    run_sites(tables, replay_args(hours=4, spike_after=2, spike_factor=3.0), producer, {"A_big"})

    means = {b: m for b, _, m in SITE_A + SITE_B}
    for _, key, msg in producer.sent:
        tick = datetime.fromisoformat(msg["timestamp"]).hour
        original = means[key] + tick  # silver_table uses mean + hour as meter_reading
        expected = original * 3.0 if (key == "A_big" and tick >= 2) else original
        assert msg["meter_reading"] == expected, (key, tick)


def test_crash_in_one_site_stops_all_sites():
    """A site thread that raises marks the run as crashed and stops the other sites early."""
    producer = FakeProducer(fail_on_key="A_big")
    tables = {"A": silver_table(SITE_A, T0, 20), "B": silver_table(SITE_B, T0, 20)}
    started = time.monotonic()
    stats, _ = run_sites(tables, replay_args(hours=20, interval=0.2, spike_after=99), producer)
    assert stats.crashed
    assert time.monotonic() - started < 2  # 20 ticks would take ~4 s
    assert all(not key.startswith("A_") for _, key, _ in producer.sent)


# ---------- fail fast (subprocess, no Kafka needed) ----------


def run_producer(*args, timeout=60):
    return subprocess.run(
        [sys.executable, "kafka_producer.py", *args],
        cwd=STREAMING_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_missing_silver_fails_fast(tmp_path):
    """A missing Silver table stops the producer with a readable message and non-zero exit."""
    result = run_producer("--silver", str(tmp_path / "nope"))
    assert result.returncode != 0
    assert "Silver table not found" in result.stderr


def test_unreachable_kafka_fails_fast(mock_inputs):
    """An unreachable broker stops the producer with a readable message (no hang)."""
    silver = mock_inputs / "clean_data"
    result = run_producer("--silver", str(silver), "--bootstrap", "127.0.0.1:1", "--hours", "1")
    assert result.returncode != 0
    assert "Kafka not reachable" in result.stderr
