"""Tests for streaming_job.py on static DataFrames (no Kafka): parse, score, persistence rule,
foreachBatch writer, metrics.

Requirements: docs/TASKS.md Task 3 + section 3 Gold schema and anomaly rule,
docs/INTEGRATION_GUIDELINES.md section 2 #9 (bad messages).
"""

import inspect
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from deltalake import DeltaTable
from pyspark.ml import PipelineModel
from pyspark.ml.feature import SQLTransformer
from pyspark.sql import functions as F

from common import config
from common.config import (
    ANOMALY_PERSISTENCE_HOURS,
    CLEAN_DATA_SCHEMA,
    DATA_DIR,
    ENERGY_RESULTS_CHECKPOINT,
    ENERGY_RESULTS_PATH,
    ENERGY_RESULTS_SCHEMA,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    METER_PROFILE_PATH,
    METER_THRESHOLDS_PATH,
    MODEL_PATH,
    SPARK_TRIGGER_SEC,
    TEST_START,
    to_spark_struct_type,
)
from streaming import kafka_producer as kp
from streaming import streaming_job as job
from streaming.tests.support import STREAMING_DIR

T0 = datetime(2017, 1, 1, tzinfo=UTC)  # a Sunday: hour_of_day 0, is_weekend 1, month 1
SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]
GOLD_COLUMNS = [name for name, _ in ENERGY_RESULTS_SCHEMA]
HOURS = ANOMALY_PERSISTENCE_HOURS

GOOD = {
    "building_id": "A_big",
    "meter_type": "electricity",
    "site_id": "A",
    "timestamp": "2017-01-01T05:00:00+00:00",
    "meter_reading": 12.5,
    "air_temperature": 5.0,
    "dew_temperature": 1.0,
    "hour_of_day": 5,
    "day_of_week": 1,
    "is_weekend": 1,
    "square_feet": 1000.0,
    "primary_use": "Office",
}
GOOD_JSON = json.dumps(GOOD)


# Test frames are built from pyarrow tables: createDataFrame(list) needs Python workers
# and costs ~9 s per action on Windows, Arrow data stays in the JVM (~0.1 s).
def kafka_frame(spark, values):
    """Static DataFrame shaped like the Kafka source: value (binary) + timestamp."""
    table = pa.table(
        {
            "value": pa.array([None if v is None else v.encode() for v in values], pa.binary()),
            "timestamp": pa.array(
                [datetime(2026, 1, 1, tzinfo=UTC)] * len(values), pa.timestamp("us", tz="UTC")
            ),
        }
    )
    return spark.createDataFrame(table)


def with_field(**changes):
    """GOOD_JSON with some fields replaced by raw JSON text (or removed with None)."""
    payload = dict(GOOD)
    for name, raw in changes.items():
        if raw is None:
            payload.pop(name)
        else:
            payload[name] = json.loads(raw)
    return json.dumps(payload)


def fake_model():
    """Tiny 'model': prediction = the fake_prediction column of the input."""
    return PipelineModel(
        stages=[SQLTransformer(statement="SELECT *, fake_prediction AS prediction FROM __THIS__")]
    )


def silver(spark, rows):
    """Silver rows (+ fake_prediction) from (building, hours after T0, reading, prediction)."""
    data = {name: [] for name in [*SILVER_COLUMNS, "fake_prediction"]}
    for building, hour, reading, prediction in rows:
        ts = T0 + timedelta(hours=hour)
        values = dict(
            GOOD,
            building_id=building,
            timestamp=ts,
            meter_reading=float(reading),
            hour_of_day=ts.hour,
            day_of_week=(ts.isoweekday() % 7) + 1,
            is_weekend=int(ts.isoweekday() in (6, 7)),
            fake_prediction=float(prediction),
        )
        for name in data:
            data[name].append(values[name])
    types = {
        "string": pa.string(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "double": pa.float64(),
    }
    types["int"] = pa.int32()
    schema = pa.schema(
        [(n, types[t]) for n, t in CLEAN_DATA_SCHEMA] + [("fake_prediction", pa.float64())]
    )
    return spark.createDataFrame(pa.table(data, schema=schema))


def profile_for(spark, buildings):
    """A meter profile row for every (month, hour, day type) of the given buildings."""
    grid = [
        (b, "electricity", m, h, w, 1.0, 1.0, 1.0)
        for b in buildings
        for m in range(1, 13)
        for h in range(24)
        for w in (0, 1)
    ]
    return spark.createDataFrame(
        grid,
        "building_id string, meter_type string, month int, hour_of_day int, is_weekend int, "
        "profile_meter_log double, profile_month_log double, profile_slot_log double",
    )


def thresholds_for(spark, pairs):
    return spark.createDataFrame(
        [(b, "electricity", float(t)) for b, t in pairs],
        "building_id string, meter_type string, anomaly_threshold double",
    )


def score_rows(spark, rows, thresholds=()):
    readings = silver(spark, rows)
    buildings = sorted({r[0] for r in rows})
    return job.score(
        readings, fake_model(), profile_for(spark, buildings), thresholds_for(spark, thresholds)
    )


# ---------- defaults come from common/config.py ----------


def test_defaults_come_from_config():
    """Task 2 outputs, Gold, checkpoint, Kafka and trigger defaults are the config constants."""
    args = job.parse_args([])
    assert (args.model, args.profile, args.thresholds) == (
        MODEL_PATH,
        METER_PROFILE_PATH,
        METER_THRESHOLDS_PATH,
    )
    assert (args.output, args.checkpoint) == (ENERGY_RESULTS_PATH, ENERGY_RESULTS_CHECKPOINT)
    assert (args.bootstrap, args.topic, args.trigger) == (
        KAFKA_BOOTSTRAP,
        KAFKA_TOPIC,
        SPARK_TRIGGER_SEC,
    )


def test_model_dir_points_at_the_three_task2_outputs():
    args = job.parse_args(["--model-dir", "x"])
    assert [p.replace("\\", "/") for p in job.task2_paths(args)] == [
        "x/baseline_model",
        "x/meter_profile",
        "x/meter_thresholds",
    ]


def test_checkpoint_location_is_the_agreed_one():
    """INTEGRATION_GUIDELINES #4: checkpoint at data/checkpoints/energy_results."""
    assert ENERGY_RESULTS_CHECKPOINT == str(DATA_DIR / "checkpoints" / "energy_results")


def test_threshold_and_persistence_are_imported_not_hardcoded():
    """The rule constants come from common/config.py; 0.28 does not appear in the job code."""
    assert job.THRESHOLD_RATIO is config.THRESHOLD_RATIO
    assert job.ANOMALY_PERSISTENCE_HOURS is config.ANOMALY_PERSISTENCE_HOURS
    assert "0.28" not in inspect.getsource(job)


def test_job_fails_fast_on_missing_model(tmp_path):
    """A missing model directory stops the job before Spark starts, with a clear message."""
    result = subprocess.run(
        [sys.executable, "streaming_job.py", "--model", str(tmp_path / "nope")],
        cwd=STREAMING_DIR,
        capture_output=True,
        text=True,
        timeout=60,
    )
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
    assert row.is_valid and row.meter_reading == 12.5 and row.meter_type == "electricity"


def test_parse_timestamp_is_utc(spark):
    """'2017-01-01T05:00:00+00:00' becomes 05:00 UTC (epoch check, independent of the PC zone)."""
    parsed = job.parse_messages(kafka_frame(spark, [GOOD_JSON]))
    epoch = parsed.select(F.unix_timestamp("timestamp").alias("s")).first().s
    assert epoch == int(datetime(2017, 1, 1, 5, tzinfo=UTC).timestamp())


def test_producer_message_round_trip(spark):
    """A message made by kafka_producer.to_message parses back to the same values."""
    row = dict(GOOD, building_id="Fox_x", site_id="Fox", timestamp=T0 + timedelta(hours=7))
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
    "missing_meter_type": with_field(meter_type=None),
    "null_field": with_field(air_temperature="null"),
    "meter_reading_text": with_field(meter_reading='"abc"'),
    "hour_is_float": with_field(hour_of_day="5.5"),
    "bad_timestamp": with_field(timestamp='"yesterday"'),
    "truncated": GOOD_JSON[:40],
}


def test_bad_messages_are_invalid_and_do_not_crash(spark):
    """INTEGRATION_GUIDELINES #9: malformed / incomplete / wrong-type messages -> is_valid false."""
    names = list(BAD_MESSAGES)
    parsed = job.parse_messages(kafka_frame(spark, [BAD_MESSAGES[n] for n in names] + [GOOD_JSON]))
    flags = [r.is_valid for r in parsed.collect()]  # collect must not raise
    assert dict(zip(names, flags, strict=False)) == {name: False for name in names}
    assert flags[-1] is True


# ---------- score: baseline, residual, per-meter threshold ----------


def test_score_columns_are_gold_without_is_anomaly_plus_exceeds(spark):
    scored = score_rows(spark, [("b0", 0, 10, 10)])
    assert scored.columns == [c for c in GOLD_COLUMNS if c != "is_anomaly"] + ["exceeds"]


def test_exceeds_uses_the_meter_threshold_and_the_fallback(spark):
    """b_tight has threshold 0.1, b_loose 1.0, b_none has none -> THRESHOLD_RATIO."""
    rows = [("b_tight", 0, 120, 100), ("b_loose", 0, 150, 100), ("b_none", 0, 150, 100)]
    scored = score_rows(spark, rows, thresholds=[("b_tight", 0.1), ("b_loose", 1.0)])
    got = {r.building_id: (r.baseline, r.residual, r.exceeds) for r in scored.collect()}
    assert got["b_tight"] == (100.0, 20.0, True)
    assert got["b_loose"] == (100.0, 50.0, False)
    assert got["b_none"] == (100.0, 50.0, 50 > 100 * config.THRESHOLD_RATIO)


def test_threshold_boundary_is_strict(spark):
    """Exactly at the threshold is NOT exceeding, just above is."""
    rows = [("b0", 0, 150, 100), ("b1", 0, 150.5, 100), ("b2", 0, 50, 100), ("b3", 0, 49.5, 100)]
    scored = score_rows(spark, rows, thresholds=[(f"b{i}", 0.5) for i in range(4)])
    flags = {r.building_id: r.exceeds for r in scored.collect()}
    assert flags == {"b0": False, "b1": True, "b2": False, "b3": True}


def test_negative_prediction_is_clamped_to_zero(spark):
    """A negative kWh prediction gives baseline 0 (else every reading would exceed)."""
    row = score_rows(spark, [("b0", 0, 3, -5)]).first()
    assert row.baseline == 0.0 and row.residual == 3.0 and row.exceeds


def test_only_anomaly_meter_types_and_profiled_meters_are_scored(spark):
    """Other meter types and meters without a 2016 profile get no baseline."""
    readings = silver(spark, [("b0", 0, 10, 10), ("b_new", 0, 10, 10)])
    readings = readings.unionByName(readings.limit(1).withColumn("meter_type", F.lit("steam")))
    scored = job.score(
        readings, fake_model(), profile_for(spark, ["b0"]), thresholds_for(spark, [])
    )
    assert [(r.building_id, r.meter_type) for r in scored.collect()] == [("b0", "electricity")]


# ---------- persistence rule ----------


def anomalies(spark, rows, history_rows=None, threshold=0.28):
    """rows / history_rows: (building, hour, reading, prediction). Returns {(b, hour): flag}."""
    thresholds = [(b, threshold) for b in {r[0] for r in rows + (history_rows or [])}]
    current = score_rows(spark, rows, thresholds)
    history = score_rows(spark, history_rows, thresholds) if history_rows else None
    gold = job.mark_anomalies(current, history).withColumn(
        "hour", ((F.unix_timestamp("timestamp") - int(T0.timestamp())) / 3600).cast("int")
    )
    return {(r.building_id, r.hour): r.is_anomaly for r in gold.collect()}


def test_anomaly_needs_persistence_hours_in_a_row(spark):
    """Only the hour that completes ANOMALY_PERSISTENCE_HOURS exceeding hours is flagged."""
    readings = [100, 150, 100, 150, 150, 150, 150, 100]  # prediction 100, threshold 28%
    flags = anomalies(spark, [("b0", h, r, 100) for h, r in enumerate(readings)])
    first_full_run = 3 + HOURS - 1
    assert [flags[("b0", h)] for h in range(len(readings))] == [
        h in range(first_full_run, 7) for h in range(len(readings))
    ]


def test_history_from_previous_batches_counts(spark):
    """The run can start in earlier micro-batches (rows read back from Gold)."""
    history = [("b0", h, 150, 100) for h in range(HOURS - 1)]
    flags = anomalies(spark, [("b0", HOURS - 1, 150, 100)], history_rows=history)
    assert flags == {("b0", HOURS - 1): True}


def test_missing_hour_breaks_the_run(spark):
    rows = [("b0", h, 150, 100) for h in range(HOURS + 1) if h != 1]
    flags = anomalies(spark, rows)
    assert not any(flags.values())


def test_gold_schema_matches_contract(spark):
    """mark_anomalies() returns exactly ENERGY_RESULTS_SCHEMA: names, order and types."""
    gold = job.mark_anomalies(score_rows(spark, [("b0", 0, 10, 10)]), None)
    expected = to_spark_struct_type(ENERGY_RESULTS_SCHEMA)
    assert [(f.name, f.dataType) for f in gold.schema] == [(f.name, f.dataType) for f in expected]


# ---------- foreachBatch writer (Gold history across batches, idempotent replays) ----------


def test_writer_keeps_state_across_batches_and_replays_are_not_duplicated(spark, tmp_path):
    output = str(tmp_path / "gold")
    profile = profile_for(spark, ["b0"])
    thresholds = thresholds_for(spark, [("b0", 0.28)])
    writer = job.BatchWriter(spark, fake_model(), profile, thresholds, output)

    for hour in range(HOURS):  # one hour per micro-batch, all exceeding
        writer(silver(spark, [("b0", hour, 150, 100)]), hour)
    writer(silver(spark, [("b0", HOURS - 1, 150, 100)]), HOURS - 1)  # replayed batch id

    gold = DeltaTable(output).to_pyarrow_table().to_pylist()
    assert len(gold) == HOURS  # the replay committed nothing
    by_hour = sorted(gold, key=lambda r: r["timestamp"])
    assert [r["is_anomaly"] for r in by_hour] == [False] * (HOURS - 1) + [True]
    assert writer.pop(HOURS - 1) == {"scored": 1, "anomalies": 1}


def test_writer_skips_batches_with_nothing_to_score(spark, tmp_path):
    output = str(tmp_path / "gold")
    writer = job.BatchWriter(
        spark, fake_model(), profile_for(spark, ["b0"]), thresholds_for(spark, []), output
    )
    writer(silver(spark, [("unknown", 0, 1, 1)]), 0)
    assert writer.pop(0) == {"scored": 0, "anomalies": 0}
    assert not (tmp_path / "gold").exists()


def test_real_mock_outputs_score_the_test_period(spark, mock_inputs):
    """The mock Task 2 outputs (made by the real Task 2 code) load and give sane baselines."""
    model_path, profile_path, thresholds_path = job.task2_paths(
        job.parse_args(["--model-dir", str(mock_inputs)])
    )
    day_one = (F.col("timestamp") >= TEST_START) & (F.col("timestamp") < "2017-01-02 00:00:00")
    rows = spark.read.format("delta").load(str(mock_inputs / "clean_data")).where(day_one)
    scored = job.score(
        rows,
        PipelineModel.load(model_path),
        spark.read.format("delta").load(profile_path),
        spark.read.format("delta").load(thresholds_path),
    ).cache()
    stats = scored.agg(
        F.count("*").alias("n"),
        F.min("baseline").alias("lo"),
        F.sum(F.col("baseline").isNull().cast("int")).alias("nulls"),
    ).first()
    assert stats.n == 12 * 24 and stats.nulls == 0 and stats.lo >= 0
    big = scored.where("building_id = 'Fox_education_Dan'").agg(F.avg("baseline")).first()[0]
    assert 200 < big < 1000  # mock mean is 500 kWh


# ---------- batch_metrics ----------


def progress(num_rows=24, invalid=1, oldest_sent_ms=None, duration_ms=2000):
    """A query progress report like the ones in query.recentProgress."""
    started = "2026-10-18T14:14:20.000Z"
    start_ms = datetime(2026, 10, 18, 14, 14, 20, tzinfo=UTC).timestamp() * 1000
    return {
        "batchId": 7,
        "timestamp": started,
        "numInputRows": num_rows,
        "durationMs": {"triggerExecution": duration_ms},
        "observedMetrics": {
            "input": {
                "rows": num_rows,
                "invalid": invalid,
                "oldest_sent_ms": oldest_sent_ms if oldest_sent_ms is not None else start_ms - 5000,
            }
        },
    }


def test_batch_metrics_idle_trigger_is_none():
    """An idle trigger (0 input rows) gives no metrics row."""
    assert job.batch_metrics({"batchId": 3, "numInputRows": 0}, {}) is None


def test_batch_metrics_throughput_latency_and_writer_counts():
    """rows/s = rows / batch duration; latency = batch end - oldest producer send time."""
    m = job.batch_metrics(progress(num_rows=24, duration_ms=2000), {"scored": 20, "anomalies": 3})
    assert list(m) == job.METRICS_FIELDS
    assert (m["batch_id"], m["rows"], m["invalid"], m["scored"], m["anomalies"]) == (
        7,
        24,
        1,
        20,
        3,
    )
    assert m["duration_sec"] == 2.0
    assert m["rows_per_sec"] == 12.0
    assert m["max_latency_sec"] == 7.0  # sent 5 s before the trigger + 2 s processing
