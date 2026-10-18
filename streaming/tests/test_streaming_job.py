"""Tests for streaming_job.py on static DataFrames (no Kafka): parse, score, metrics.

Requirements: TASKS.md Task 3 "Spark Structured Streaming" + section 3 Gold schema,
INTEGRATION_GUIDELINES.md section 2 #9 (bad messages).
"""

import inspect
import math
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.feature import SQLTransformer
from pyspark.sql import functions as F

import config
import kafka_producer as kp
import streaming_job as job
from conftest import STREAMING_DIR
from config import (
    CLEAN_DATA_SCHEMA, ENERGY_RESULTS_CHECKPOINT, ENERGY_RESULTS_PATH, ENERGY_RESULTS_SCHEMA,
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, MODEL_PATH, SPARK_TRIGGER_SEC, TEST_START, to_spark_struct_type,
)

UTC = timezone.utc
T0 = datetime(2017, 1, 1, tzinfo=UTC)
SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]
GOLD_COLUMNS = [name for name, _ in ENERGY_RESULTS_SCHEMA]

GOOD_JSON = ('{"building_id": "A_big", "site_id": "A", "timestamp": "2017-01-01T05:00:00+00:00", '
             '"meter_reading": 12.5, "air_temperature": 5.0, "dew_temperature": 1.0, "hour_of_day": 5, '
             '"day_of_week": 1, "is_weekend": 1, "square_feet": 1000.0, "primary_use": "Office"}')


# Test frames are built from pyarrow tables: createDataFrame(list) needs Python workers
# and costs ~9 s per action on Windows, Arrow data stays in the JVM (~0.1 s).
def kafka_frame(spark, values):
    """Static DataFrame shaped like the Kafka source: value (binary) + timestamp."""
    table = pa.table({
        "value": pa.array([None if v is None else v.encode() for v in values], pa.binary()),
        "timestamp": pa.array([datetime(2026, 1, 1, tzinfo=UTC)] * len(values), pa.timestamp("us", tz="UTC")),
    })
    return spark.createDataFrame(table)


def with_field(**changes):
    """GOOD_JSON with some fields replaced by raw JSON text (or removed with None)."""
    import json
    payload = json.loads(GOOD_JSON)
    for name, raw in changes.items():
        if raw is None:
            payload.pop(name)
        else:
            payload[name] = json.loads(raw)
    return json.dumps(payload)


def fake_model(spark):
    """Tiny 'model': prediction = the fake_prediction column of the input."""
    return PipelineModel(stages=[SQLTransformer(statement="SELECT *, fake_prediction AS prediction FROM __THIS__")])


def readings(spark, pairs):
    """Silver-like rows with (meter_reading, fake_prediction) pairs."""
    table = pa.table({
        "building_id": ["b%d" % i for i in range(len(pairs))],
        "site_id": ["A"] * len(pairs),
        "timestamp": pa.array([T0] * len(pairs), pa.timestamp("us", tz="UTC")),
        "meter_reading": [float(meter) for meter, _ in pairs],
        "fake_prediction": [float(pred) for _, pred in pairs],
    })
    return spark.createDataFrame(table)


# ---------- defaults come from config.py ----------

def test_defaults_come_from_config(monkeypatch):
    """Model, Gold, checkpoint, Kafka and trigger defaults are the config.py constants."""
    monkeypatch.setattr(sys, "argv", ["streaming_job.py"])
    args = job.parse_args()
    assert (args.model, args.output, args.checkpoint) == (MODEL_PATH, ENERGY_RESULTS_PATH, ENERGY_RESULTS_CHECKPOINT)
    assert (args.bootstrap, args.topic, args.trigger) == (KAFKA_BOOTSTRAP, KAFKA_TOPIC, SPARK_TRIGGER_SEC)
    assert not args.log_target


def test_checkpoint_location_is_the_agreed_one():
    """INTEGRATION_GUIDELINES #4: checkpoint at streaming/output/_checkpoints/energy_results."""
    expected = config.REPO_ROOT / "streaming" / "output" / "_checkpoints" / "energy_results"
    assert ENERGY_RESULTS_CHECKPOINT == str(expected)


def test_threshold_is_imported_not_hardcoded():
    """THRESHOLD_RATIO comes from config.py; the number 0.28 does not appear in the job code."""
    assert job.THRESHOLD_RATIO is config.THRESHOLD_RATIO
    assert "0.28" not in inspect.getsource(job)


def test_job_fails_fast_on_missing_model(tmp_path):
    """A missing model directory stops the job before Spark starts, with a clear message."""
    result = subprocess.run([sys.executable, "streaming_job.py", "--model", str(tmp_path / "nope")],
                            cwd=STREAMING_DIR, capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert "Model not found" in result.stderr


# ---------- parse_messages ----------

def test_parse_valid_message_uses_silver_schema(spark):
    """A good message parses into all Silver columns with the right types, is_valid = true."""
    parsed = job.parse_messages(kafka_frame(spark, [GOOD_JSON]))
    assert parsed.columns == SILVER_COLUMNS + ["kafka_ts", "is_valid"]
    for field in to_spark_struct_type(CLEAN_DATA_SCHEMA):
        assert parsed.schema[field.name].dataType == field.dataType
    row = parsed.first()
    assert row.is_valid and row.meter_reading == 12.5 and row.hour_of_day == 5


def test_parse_timestamp_is_utc(spark):
    """'2017-01-01T05:00:00+00:00' becomes 05:00 UTC (epoch check, independent of the PC timezone)."""
    parsed = job.parse_messages(kafka_frame(spark, [GOOD_JSON]))
    epoch = parsed.select(F.unix_timestamp("timestamp").alias("s")).first().s
    assert epoch == int(datetime(2017, 1, 1, 5, tzinfo=UTC).timestamp())


def test_producer_message_round_trip(spark):
    """A message made by kafka_producer.to_message parses back to the same values."""
    row = {"building_id": "Fox_x", "site_id": "Fox", "timestamp": T0 + timedelta(hours=7), "meter_reading": 42.25,
           "air_temperature": -3.5, "dew_temperature": -7.5, "hour_of_day": 7, "day_of_week": 1,
           "is_weekend": 1, "square_feet": 5000.0, "primary_use": "Office"}
    _, value = kp.to_message(row)
    parsed = job.parse_messages(kafka_frame(spark, [value.decode()]))
    got = parsed.withColumn("epoch", F.unix_timestamp("timestamp")).first()
    assert got.is_valid
    assert got.epoch == int(row["timestamp"].timestamp())
    for name in SILVER_COLUMNS:
        if name != "timestamp":
            assert got[name] == row[name], name


BAD_MESSAGES = {
    "not_json": "this is not json",
    "empty": "",
    "null_value": None,
    "json_array": "[1, 2, 3]",
    "missing_field": with_field(square_feet=None),
    "null_field": with_field(air_temperature="null"),
    "meter_reading_text": with_field(meter_reading='"abc"'),
    "hour_is_float": with_field(hour_of_day="5.5"),
    "bad_timestamp": with_field(timestamp='"yesterday"'),
    "truncated": GOOD_JSON[:40],
}


def test_bad_messages_are_invalid_and_do_not_crash(spark):
    """INTEGRATION_GUIDELINES #9: malformed / incomplete / wrong-type messages -> is_valid = false."""
    names = list(BAD_MESSAGES)
    parsed = job.parse_messages(kafka_frame(spark, [BAD_MESSAGES[n] for n in names] + [GOOD_JSON]))
    flags = [r.is_valid for r in parsed.collect()]  # collect must not raise
    bad = {name: flag for name, flag in zip(names, flags)}
    assert bad == {name: False for name in names}
    assert flags[-1] is True


def test_invalid_rows_are_dropped_before_scoring(spark):
    """Only valid rows reach the model; the 'invalid' count matches the bad messages."""
    parsed = job.parse_messages(kafka_frame(spark, [GOOD_JSON, "oops", GOOD_JSON, with_field(site_id=None)]))
    assert parsed.where("is_valid").count() == 2
    assert parsed.select(F.sum((~F.col("is_valid")).cast("int"))).first()[0] == 2


# ---------- score ----------

def test_gold_schema_matches_contract(spark):
    """score() returns exactly ENERGY_RESULTS_SCHEMA: names, order and types."""
    gold = job.score(readings(spark, [(10, 10)]), fake_model(spark))
    expected = to_spark_struct_type(ENERGY_RESULTS_SCHEMA)
    assert [(f.name, f.dataType) for f in gold.schema] == [(f.name, f.dataType) for f in expected]


def test_residual_and_anomaly_formula(spark):
    """residual = meter_reading - baseline, is_anomaly = abs(residual) > baseline * THRESHOLD_RATIO."""
    pairs = [(100, 100), (200, 100), (50, 100), (110, 100), (90, 100), (0, 0), (5, 0)]
    rows = job.score(readings(spark, pairs), fake_model(spark)).orderBy("building_id").collect()
    for row, (meter, pred) in zip(rows, pairs):
        assert row.baseline == pred
        assert row.residual == meter - pred
        assert row.is_anomaly == (abs(meter - pred) > pred * config.THRESHOLD_RATIO)
    assert [r.is_anomaly for r in rows] == [False, True, True, False, False, False, True]


def test_threshold_boundary_is_strict(spark, monkeypatch):
    """Exactly at the threshold is NOT an anomaly, just above is (ratio 0.5 for exact floats)."""
    monkeypatch.setattr(job, "THRESHOLD_RATIO", 0.5)
    pairs = [(150, 100), (150.5, 100), (50, 100), (49.5, 100)]
    rows = job.score(readings(spark, pairs), fake_model(spark)).orderBy("building_id").collect()
    assert [r.is_anomaly for r in rows] == [False, True, False, True]


def test_negative_prediction_is_clamped_to_zero(spark):
    """A negative kWh prediction gives baseline 0 (else every reading would be an anomaly)."""
    row = job.score(readings(spark, [(3, -5)]), fake_model(spark)).first()
    assert row.baseline == 0.0 and row.residual == 3.0 and row.is_anomaly


def test_log_target_applies_expm1(spark):
    """--log-target: the model predicts log1p(kWh), baseline = expm1(prediction)."""
    row = job.score(readings(spark, [(100, math.log1p(100))]), fake_model(spark), log_target=True).first()
    assert row.baseline == pytest.approx(100.0)
    assert not row.is_anomaly


def test_real_mock_model_scores_test_period(spark, mock_inputs):
    """The Task-2-shaped mock PipelineModel loads and gives a sensible baseline for 2017 rows."""
    silver, model_path = mock_inputs
    day_one = (F.col("timestamp") >= TEST_START) & (F.col("timestamp") < "2017-01-02 00:00:00")
    test_rows = spark.read.format("delta").load(str(silver)).where(day_one)  # 12 buildings x 24 h
    gold = job.score(test_rows, PipelineModel.load(str(model_path))).cache()
    stats = gold.agg(F.count("*").alias("n"), F.min("baseline").alias("lo"),
                     F.sum(F.col("baseline").isNull().cast("int")).alias("nulls")).first()
    assert stats.n == 12 * 24 and stats.nulls == 0 and stats.lo >= 0
    big = gold.where("building_id = 'Fox_education_Dan'").agg(F.avg("baseline")).first()[0]
    assert 200 < big < 1000  # mock mean is 500 kWh


# ---------- batch_metrics ----------

def progress(num_rows=24, invalid=1, anomalies=3, oldest_sent_ms=None, duration_ms=2000):
    """A query progress report like the ones in query.recentProgress."""
    started = "2026-10-18T14:14:20.000Z"
    start_ms = datetime(2026, 10, 18, 14, 14, 20, tzinfo=UTC).timestamp() * 1000
    return {
        "batchId": 7, "timestamp": started, "numInputRows": num_rows,
        "durationMs": {"triggerExecution": duration_ms},
        "observedMetrics": {
            "input": {"rows": num_rows, "invalid": invalid,
                      "oldest_sent_ms": oldest_sent_ms if oldest_sent_ms is not None else start_ms - 5000},
            "output": {"anomalies": anomalies},
        },
    }


def test_batch_metrics_idle_trigger_is_none():
    """An idle trigger (0 input rows) gives no metrics row."""
    assert job.batch_metrics({"batchId": 3, "numInputRows": 0}) is None


def test_batch_metrics_throughput_and_latency():
    """rows/s = rows / batch duration; latency = batch end - oldest producer send time."""
    m = job.batch_metrics(progress(num_rows=24, duration_ms=2000))
    assert list(m) == job.METRICS_FIELDS
    assert (m["batch_id"], m["rows"], m["invalid"], m["anomalies"]) == (7, 24, 1, 3)
    assert m["duration_sec"] == 2.0
    assert m["rows_per_sec"] == 12.0
    assert m["max_latency_sec"] == 7.0  # sent 5 s before the trigger + 2 s processing


def test_batch_metrics_no_anomaly_sum_is_zero():
    """If every row was invalid the anomaly sum is null -> reported as 0."""
    m = job.batch_metrics(progress(num_rows=3, invalid=3, anomalies=None))
    assert m["anomalies"] == 0 and m["invalid"] == 3
