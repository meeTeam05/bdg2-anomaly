"""End-to-end: mock Silver -> kafka_producer.py -> Kafka -> streaming_job.py -> Gold (Delta).

Runs both scripts as subprocesses against a unique Kafka topic and writes Gold/checkpoint into
a pytest tmp dir (the real streaming/output/ is never touched). Takes ~2 minutes.

Scenario (one run, shared by the tests below):
  1. start the job, replay 12 h of 2017 (12 buildings -> 144 messages), spike on 1 building
     from tick 6
  2. kill the job; while it is down send 3 bad messages + 6 more hours (72 messages)
  3. restart the job with the SAME checkpoint -> it must continue, not duplicate
  4. stay idle > 10 s, then send a batch made only of bad messages
     (job 2 runs OPTIMIZE every 2 batches)
"""

import csv
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer
from deltalake import DeltaTable

from common.config import (
    ANOMALY_PERSISTENCE_HOURS,
    ENERGY_RESULTS_PATH,
    ENERGY_RESULTS_SCHEMA,
    KAFKA_BOOTSTRAP,
)
from streaming import streaming_job as job
from streaming.tests.support import (
    MOCK_DIR,
    MOCK_READY,
    MOCK_SILVER,
    STREAMING_DIR,
    create_topic,
    delete_topic,
)

pytestmark = pytest.mark.integration

T0 = datetime(2017, 1, 1, tzinfo=UTC)
BUILDINGS = 12  # mock Silver: 3 sites x 4 buildings
HOURS_1, HOURS_2 = 12, 6  # replayed hours before / after the restart
SPIKE_AFTER, SPIKE_FACTOR = 6, 3.0
INTERVAL = 0.5  # producer tick (fast for the test)
BAD_MESSAGES = [b"not json", b'{"building_id": "only_one_field"}', b'{"meter_reading": "abc"}']
ENV = dict(os.environ, PYTHONUNBUFFERED="1")


# ---------- helpers ----------


def wait_for(condition, timeout, what, proc=None, log_path=None):
    """Poll `condition` every 0.5 s; fail with the job log tail if it never becomes true."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return
        if proc is not None and proc.poll() is not None:
            break
        time.sleep(0.5)
    tail = log_path.read_text(errors="replace")[-3000:] if log_path else ""
    pytest.fail(f"timeout/exit while waiting for {what}\n{tail}")


def poll_until(condition, timeout, proc):
    """Like wait_for, but just stops (no failure) on timeout or when the process exits."""
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() is None and not condition():
        time.sleep(0.5)


def start_job(tmp, topic, name, *extra, output=None, checkpoint=None):
    log_path = tmp / f"{name}.log"
    proc = subprocess.Popen(
        [
            sys.executable,
            "streaming_job.py",
            "--model-dir",
            str(MOCK_DIR),
            "--topic",
            topic,
            "--output",
            str(output or tmp / "gold"),
            "--checkpoint",
            str(checkpoint or tmp / "checkpoint"),
            "--trigger",
            "2",
            *extra,
        ],
        cwd=STREAMING_DIR,
        env=ENV,
        stdout=log_path.open("w"),
        stderr=subprocess.STDOUT,
    )
    wait_for(
        lambda: f"streaming {topic}" in log_path.read_text(errors="replace"),
        180,
        f"{name} to start",
        proc,
        log_path,
    )
    return proc, log_path


def run_producer(topic, *args):
    result = subprocess.run(
        [
            sys.executable,
            "kafka_producer.py",
            "--silver",
            str(MOCK_SILVER),
            "--topic",
            topic,
            "--interval",
            str(INTERVAL),
            *args,
        ],
        cwd=STREAMING_DIR,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stderr


def gold_count(path):
    try:
        return DeltaTable(str(path)).to_pyarrow_table(columns=["building_id"]).num_rows
    except Exception:  # table not created yet
        return 0


def metrics_rows(tmp):
    path = tmp / "batch_metrics.csv"
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def send_raw(topic, values):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for value in values:
        producer.produce(topic, value=value)
    assert producer.flush(10) == 0


def consume_all(topic, expected, timeout=30):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages, deadline = [], time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            messages.append(msg)
    consumer.close()
    return messages


def stop(proc):
    if proc.poll() is None:
        proc.kill()  # hard kill = worst case for the checkpoint
        proc.wait(30)


# ---------- the scenario ----------


@pytest.fixture(scope="module")
def run(tmp_path_factory, mock_inputs_module):
    tmp = tmp_path_factory.mktemp("e2e")
    topic = create_topic()
    real_gold_versions = len(list((Path(ENERGY_RESULTS_PATH) / "_delta_log").glob("*.json")))
    procs = []
    try:
        # 1. first run
        job1, log1 = start_job(tmp, topic, "job1")
        procs.append(job1)
        started = time.time()
        producer1 = run_producer(
            topic,
            "--hours",
            str(HOURS_1),
            "--spike-count",
            "1",
            "--spike-after",
            str(SPIKE_AFTER),
            "--spike-factor",
            str(SPIKE_FACTOR),
        )
        producer_sec = time.time() - started
        first = BUILDINGS * HOURS_1
        wait_for(lambda: gold_count(tmp / "gold") >= first, 90, "Gold rows of run 1", job1, log1)
        wait_for(
            lambda: sum(int(r["rows"]) for r in metrics_rows(tmp)) >= first,
            30,
            "metrics of run 1",
            job1,
            log1,
        )
        gold_before_kill = gold_count(tmp / "gold")
        stop(job1)

        # 2. while the job is down: bad messages + 6 more hours
        send_raw(topic, BAD_MESSAGES)
        producer2 = run_producer(
            topic, "--start", "2017-01-01 12:00:00", "--hours", str(HOURS_2), "--spike-count", "0"
        )

        # 3. restart with the same checkpoint
        job2, log2 = start_job(tmp, topic, "job2", "--compact-every", "2")
        procs.append(job2)
        total = BUILDINGS * (HOURS_1 + HOURS_2)
        wait_for(
            lambda: gold_count(tmp / "gold") >= total, 90, "Gold rows after restart", job2, log2
        )

        # 4. idle > 10 s (Spark reports idle progress every 10 s), then a batch of only bad messages
        time.sleep(12)
        send_raw(topic, BAD_MESSAGES)
        all_sent = total + 2 * len(BAD_MESSAGES)
        # no wait_for here: if the job dies, the tests below report it
        poll_until(lambda: sum(int(r["rows"]) for r in metrics_rows(tmp)) >= all_sent, 30, job2)
        poll_until(lambda: "compacted" in log2.read_text(errors="replace"), 60, job2)
        time.sleep(3)  # a couple of triggers more: nothing new must be written
        job2_alive = job2.poll() is None
        stop(job2)

        yield {
            "tmp": tmp,
            "topic": topic,
            "gold": DeltaTable(str(tmp / "gold")),
            "gold_before_kill": gold_before_kill,
            "metrics": metrics_rows(tmp),
            "producer1": producer1,
            "producer2": producer2,
            "producer_sec": producer_sec,
            "job_logs": log1.read_text(errors="replace") + log2.read_text(errors="replace"),
            "job2_alive": job2_alive,
            "kafka": consume_all(topic, all_sent),
            "real_gold_versions": real_gold_versions,
        }
    finally:
        for proc in procs:
            stop(proc)
        delete_topic(topic)


@pytest.fixture(scope="module")
def mock_inputs_module():
    if not MOCK_READY:
        pytest.skip("mock_data/ missing, run: python streaming/mock_inputs.py")


def gold_rows(run):
    return run["gold"].to_pyarrow_table().to_pylist()


def silver_readings():
    table = DeltaTable(str(MOCK_SILVER)).to_pyarrow_table(
        columns=["building_id", "timestamp", "meter_reading"]
    )
    return {(r["building_id"], r["timestamp"]): r["meter_reading"] for r in table.to_pylist()}


def spike_building(run):
    return re.search(r"spike x[\d.]+ from tick \d+ on: \['([^']+)'\]", run["producer1"]).group(1)


# ---------- tests ----------


def test_gold_row_count_equals_messages_sent(run):
    """Every valid message sent ends up as exactly one Gold row."""
    assert f"done: {BUILDINGS * HOURS_1} sent, 0 failed" in run["producer1"]
    assert f"done: {BUILDINGS * HOURS_2} sent, 0 failed" in run["producer2"]
    assert run["gold_before_kill"] == BUILDINGS * HOURS_1
    assert len(gold_rows(run)) == BUILDINGS * (HOURS_1 + HOURS_2)


def test_restart_continues_without_duplicates(run):
    """Restart on the same checkpoint: no duplicated (building, hour), the new hours are there."""
    keys = [(r["building_id"], r["timestamp"]) for r in gold_rows(run)]
    assert len(keys) == len(set(keys))
    expected_hours = {T0 + timedelta(hours=h) for h in range(HOURS_1 + HOURS_2)}
    assert {ts for _, ts in keys} == expected_hours


def test_checkpoint_at_given_location_and_real_output_untouched(run):
    """The job writes its checkpoint where --checkpoint says; the real Gold is untouched."""
    checkpoint = run["tmp"] / "checkpoint"
    assert (checkpoint / "offsets").is_dir() and (checkpoint / "commits").is_dir()
    real_log = Path(ENERGY_RESULTS_PATH) / "_delta_log"
    assert len(list(real_log.glob("*.json"))) == run["real_gold_versions"]


def test_gold_schema_partitioning_readable_with_deltalake(run):
    """Gold = Delta table with ENERGY_RESULTS_SCHEMA, partitioned by site_id, read without Spark."""
    delta_types = {
        "string": "string",
        "timestamp": "timestamp",
        "double": "double",
        "boolean": "boolean",
    }
    schema = run["gold"].schema()
    assert [(f.name, f.type.type) for f in schema.fields] == [
        (name, delta_types[dtype]) for name, dtype in ENERGY_RESULTS_SCHEMA
    ]
    assert run["gold"].metadata().partition_columns == ["site_id"]
    assert sorted(p.name for p in (run["tmp"] / "gold").glob("site_id=*")) == [
        "site_id=Fox",
        "site_id=Panther",
        "site_id=Rat",
    ]
    pandas_free = run["gold"].to_pyarrow_table(filters=[("site_id", "=", "Fox")])
    assert pandas_free.num_rows == 4 * (HOURS_1 + HOURS_2)


def test_gold_values_match_silver_with_utc_timestamps(run):
    """Each Gold row matches the Silver row of its UTC hour; only the spike building differs."""
    silver, spiked = silver_readings(), spike_building(run)
    for row in gold_rows(run):
        original = silver[(row["building_id"], row["timestamp"])]  # KeyError = timestamp shifted
        hour = (row["timestamp"] - T0) // timedelta(hours=1)
        factor = (
            SPIKE_FACTOR
            if (row["building_id"] == spiked and SPIKE_AFTER <= hour < HOURS_1)
            else 1.0
        )
        assert row["meter_reading"] == pytest.approx(original * factor), (row["building_id"], hour)
        assert row["residual"] == pytest.approx(row["meter_reading"] - row["baseline"])
        assert row["baseline"] >= 0


def test_spike_building_is_flagged_after_spike_tick(run):
    """The live-demo spike building is an anomaly in every hour once the spike has lasted
    ANOMALY_PERSISTENCE_HOURS hours (the persistence rule needs that many hours in a row)."""
    spiked = spike_building(run)
    assert spiked == "Fox_education_Dan"  # highest mean consumption in the mock
    rows = [r for r in gold_rows(run) if r["building_id"] == spiked]
    hour = lambda r: (r["timestamp"] - T0) // timedelta(hours=1)  # noqa: E731
    first_flag = SPIKE_AFTER + ANOMALY_PERSISTENCE_HOURS - 1
    during = [r["is_anomaly"] for r in rows if first_flag <= hour(r) < HOURS_1]
    before = [r["is_anomaly"] for r in rows if hour(r) < SPIKE_AFTER]
    assert len(during) == HOURS_1 - first_flag and all(during)
    assert sum(before) < len(before) / 2  # without spike it is mostly normal


def test_bad_messages_are_dropped_and_counted(run):
    """Bad messages never reach Gold and are counted in the 'invalid' column."""
    assert all(r["building_id"] != "only_one_field" for r in gold_rows(run))
    assert sum(int(r["invalid"]) for r in run["metrics"]) == 2 * len(BAD_MESSAGES)


def test_job_survives_a_batch_of_only_bad_messages(run):
    """A micro-batch where every message is invalid does not stop the job."""
    assert run["job2_alive"], run["job_logs"][-3000:]
    assert any(r["rows"] == r["invalid"] == str(len(BAD_MESSAGES)) for r in run["metrics"])


def test_metrics_csv_has_every_batch(run):
    """batch_metrics.csv: one row per non-empty batch; rows add up to every message sent."""
    metrics = run["metrics"]
    assert list(metrics[0]) == job.METRICS_FIELDS
    assert sum(int(r["rows"]) for r in metrics) == BUILDINGS * (HOURS_1 + HOURS_2) + 2 * len(
        BAD_MESSAGES
    )
    assert all(float(r["duration_sec"]) > 0 and float(r["max_latency_sec"]) > 0 for r in metrics)
    assert re.search(
        r"batch \d+: \d+ rows \(\d+ invalid\), \d+ scored, \d+ anomalies \| [\d.]+s, \d+ rows/s",
        run["job_logs"],
    )


def test_kafka_messages_keyed_by_building_and_paced(run):
    """In Kafka: key = building_id, JSON value; hour k is sent ~k * interval after hour 0."""
    messages = [m for m in run["kafka"] if m.key() is not None]
    assert len(messages) == BUILDINGS * (HOURS_1 + HOURS_2)
    first_sent = {}
    for m in messages:
        payload = json.loads(m.value())
        assert m.key().decode() == payload["building_id"]
        first_sent.setdefault(payload["timestamp"], m.timestamp()[1] / 1000)
    run1 = [first_sent[(T0 + timedelta(hours=h)).isoformat()] for h in range(HOURS_1)]
    for h, sent in enumerate(run1):
        assert sent - run1[0] == pytest.approx(h * INTERVAL, abs=0.25)


def test_producer_fails_fast_on_missing_topic(mock_inputs_module):
    """A topic that does not exist (auto-create is off) stops the producer with a clear message."""
    result = subprocess.run(
        [
            sys.executable,
            "kafka_producer.py",
            "--silver",
            str(MOCK_SILVER),
            "--topic",
            "no-such-topic-xyz",
            "--hours",
            "1",
        ],
        cwd=STREAMING_DIR,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "does not exist" in result.stderr


def test_compaction_keeps_gold_readable(run):
    """--compact-every N runs OPTIMIZE; Gold still has every row once (see the tests above)."""
    assert "compacted" in run["job_logs"]
    operations = [h["operation"] for h in run["gold"].history()]
    assert "OPTIMIZE" in operations


def test_crashing_site_thread_exits_non_zero(tmp_path):
    """A crashing site thread (here: a null meter_reading in Silver) makes the producer exit 1."""
    import pyarrow as pa
    from deltalake import write_deltalake

    from streaming.tests.test_producer import SITE_A, SITE_B, silver_table
    from streaming.tests.test_producer import T0 as SILVER_START

    table = pa.concat_tables(
        [silver_table(SITE_A, SILVER_START, 6), silver_table(SITE_B, SILVER_START, 6)]
    )
    meter = table["meter_reading"].to_pylist()
    meter[-1] = None  # last row = site B, hour 5
    table = table.set_column(
        table.schema.get_field_index("meter_reading"),
        "meter_reading",
        pa.array(meter, pa.float64()),
    )
    write_deltalake(str(tmp_path / "silver"), table, partition_by=["site_id"])
    topic = create_topic()
    try:
        result = subprocess.run(
            [
                sys.executable,
                "kafka_producer.py",
                "--silver",
                str(tmp_path / "silver"),
                "--topic",
                topic,
                "--hours",
                "20",
                "--interval",
                "0.2",
                "--spike-count",
                "0",
            ],
            cwd=STREAMING_DIR,
            env=ENV,
            capture_output=True,
            text=True,
            timeout=60,
        )
    finally:
        delete_topic(topic)
    assert result.returncode == 1
    assert "site B crashed, stopping all sites" in result.stderr


def test_job_runs_when_output_folder_does_not_exist_yet(tmp_path):
    """--output in a folder that does not exist yet (checkpoint elsewhere) must not crash."""
    topic = create_topic()
    proc = None
    try:
        proc, log_path = start_job(
            tmp_path,
            topic,
            "job",
            output=tmp_path / "new" / "energy_results",
            checkpoint=tmp_path / "checkpoint",
        )
        time.sleep(5)
        assert proc.poll() is None, log_path.read_text(errors="replace")[-2000:]
    finally:
        if proc is not None:
            stop(proc)
        delete_topic(topic)
