"""Task 3: Kafka -> Spark Structured Streaming -> Gold Delta table (energy_results).

Per micro-batch: parse JSON (Silver schema) -> drop invalid rows -> keep ANOMALY_METER_TYPES
-> meter profile + baseline model (Task 2) -> baseline, residual -> per-meter threshold ->
is_anomaly (threshold exceeded for ANOMALY_PERSISTENCE_HOURS consecutive hours) -> append
to Gold, partitioned by site_id.

The persistence rule needs the previous hours of each meter: they are read back from Gold
itself, so a restarted job keeps the right state without a separate state store. Writes use
Delta's idempotent txnAppId/txnVersion, so a replayed micro-batch is never written twice.

    python streaming/streaming_job.py                                   # real Task 2 outputs
    python streaming/streaming_job.py --model-dir streaming/mock_data   # mock (mock_inputs.py)

One log line and one row in <output>/../batch_metrics.csv per micro-batch (throughput, latency).
"""

import argparse
import csv
import logging
import sys
import threading
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

import pyspark
from delta.tables import DeltaTable
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.config import (
    ANOMALY_METER_TYPES,
    ANOMALY_PERSISTENCE_HOURS,
    CLEAN_DATA_SCHEMA,
    ENERGY_RESULTS_CHECKPOINT,
    ENERGY_RESULTS_PATH,
    ENERGY_RESULTS_SCHEMA,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    METER_PROFILE_PATH,
    METER_THRESHOLDS_PATH,
    MODEL_PATH,
    PARTITION_COLUMN,
    SPARK_TRIGGER_SEC,
    THRESHOLD_RATIO,
    to_spark_struct_type,
)
from common.spark_session import build_spark
from ml_model.meter_profile import METER_KEY, add_profile_features

log = logging.getLogger("streaming_job")

# The connector version must match pyspark exactly, otherwise Spark fails with ClassNotFound.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
STREAMING_SPARK_CONFIG = {
    "spark.ui.showConsoleProgress": "false",  # keep the per-batch log lines readable
    "spark.sql.shuffle.partitions": "8",
    # Delta rebuilds its log state after every commit with 50 tasks by default: about 6-9 s
    # per micro-batch in local mode, longer than the trigger. A few tasks are enough here.
    "spark.databricks.delta.snapshotPartitions": "4",
}
# Stable id for Delta idempotent writes: (app id, batch id) is committed at most once.
TXN_APP_ID = "bdg2-streaming-energy-results"

SILVER_STRUCT = to_spark_struct_type(CLEAN_DATA_SCHEMA)
SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]
GOLD_COLUMNS = [name for name, _ in ENERGY_RESULTS_SCHEMA]
METRICS_FIELDS = [
    "batch_id",
    "batch_end",
    "rows",
    "invalid",
    "scored",
    "anomalies",
    "duration_sec",
    "rows_per_sec",
    "max_latency_sec",
]


def build_streaming_spark(app_name: str) -> SparkSession:
    spark = build_spark(app_name, extra_packages=[KAFKA_PACKAGE], config=STREAMING_SPARK_CONFIG)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_messages(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> Silver columns + kafka_ts (producer send time) + is_valid.

    A message is invalid when it is not JSON or any Silver field is missing/has the wrong type
    (from_json then yields null). The model needs every field, so invalid rows are dropped
    later and only counted.
    """
    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), SILVER_STRUCT).alias("r"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("r.*", "kafka_ts")
    is_valid = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in SILVER_COLUMNS])
    return parsed.withColumn("is_valid", is_valid)


def score(
    readings: DataFrame, model: PipelineModel, profile: DataFrame, thresholds: DataFrame
) -> DataFrame:
    """Silver rows -> Gold columns without is_anomaly, plus `exceeds` (one-hour rule).

    Only ANOMALY_METER_TYPES are scored; meters without a 2016 profile cannot get a baseline
    and are dropped. baseline is clamped at 0: a negative kWh prediction would make every
    reading exceed the threshold. Meters without a calibrated threshold use THRESHOLD_RATIO.
    """
    scored = (
        model.transform(
            add_profile_features(
                readings.filter(F.col("meter_type").isin(ANOMALY_METER_TYPES)), profile
            )
        )
        .withColumn("baseline", F.greatest(F.col("prediction"), F.lit(0.0)))
        .withColumn("residual", F.col("meter_reading") - F.col("baseline"))
        .join(F.broadcast(thresholds), METER_KEY, "left")
    )
    return add_exceeds(scored).select(*[c for c in GOLD_COLUMNS if c != "is_anomaly"], "exceeds")


def add_exceeds(df: DataFrame) -> DataFrame:
    """exceeds = abs(residual) > baseline * threshold (per meter, THRESHOLD_RATIO fallback)."""
    threshold = F.coalesce(F.col("anomaly_threshold"), F.lit(THRESHOLD_RATIO))
    return df.withColumn("exceeds", F.abs("residual") > F.col("baseline") * threshold)


def mark_anomalies(current: DataFrame, history: DataFrame | None) -> DataFrame:
    """is_anomaly = the meter exceeded its threshold at this hour and the previous
    ANOMALY_PERSISTENCE_HOURS - 1 hours (clock hours: a missing hour breaks the run).

    current: this batch's rows with `exceeds`. history: earlier rows of the same meters with
    `exceeds` (from Gold), or None. Returns the current rows in Gold column order.
    """
    hours = ANOMALY_PERSISTENCE_HOURS
    key_cols = [*METER_KEY, "timestamp", "exceeds"]
    combined = current.withColumn("_current", F.lit(True))
    if history is not None:
        combined = combined.unionByName(
            history.select(*key_cols).withColumn("_current", F.lit(False)),
            allowMissingColumns=True,
        )
    run = (
        Window.partitionBy(*METER_KEY)
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-(hours - 1) * 3600, 0)
    )
    hit = F.col("exceeds").cast("int")
    is_anomaly = (F.sum(hit).over(run) == hours) & (F.count(hit).over(run) == hours)
    return combined.withColumn("is_anomaly", is_anomaly).filter("_current").select(*GOLD_COLUMNS)


def gold_history(spark: SparkSession, path: str, thresholds: DataFrame, since: int, until: int):
    """Gold rows with since <= epoch seconds < until, `exceeds` recomputed; None before the
    first write. Epoch bounds, not datetimes: no dependency on the machine's time zone."""
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    epoch = F.unix_timestamp("timestamp")
    rows = (
        spark.read.format("delta")
        .load(path)
        .filter((epoch >= since) & (epoch < until))
        .join(F.broadcast(thresholds), METER_KEY, "left")
    )
    return add_exceeds(rows)


class BatchWriter:
    """foreachBatch sink: score, apply the persistence rule against Gold history, append."""

    def __init__(self, spark, model, profile, thresholds, output):
        self.spark, self.model, self.output = spark, model, output
        self.profile, self.thresholds = profile, thresholds
        self.lock = threading.Lock()
        self.stats: dict[int, dict[str, int]] = {}  # batch id -> scored / anomalies

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        scored = score(batch, self.model, self.profile, self.thresholds).cache()
        first = scored.agg(F.min(F.unix_timestamp("timestamp"))).first()[0]
        if first is None:  # nothing scorable (empty, invalid or other meter types)
            scored.unpersist()
            self._record(batch_id, 0, 0)
            return
        since = first - (ANOMALY_PERSISTENCE_HOURS - 1) * 3600
        history = gold_history(self.spark, self.output, self.thresholds, since, first)
        gold = mark_anomalies(scored, history).cache()
        (
            gold.write.format("delta")
            .mode("append")
            .partitionBy(PARTITION_COLUMN)
            .option("txnAppId", TXN_APP_ID)
            .option("txnVersion", batch_id)
            .save(self.output)
        )
        counts = gold.agg(
            F.count("*").alias("n"), F.sum(F.col("is_anomaly").cast("int")).alias("a")
        ).first()
        self._record(batch_id, counts.n, counts.a or 0)
        gold.unpersist()
        scored.unpersist()

    def _record(self, batch_id, scored, anomalies):
        with self.lock:
            self.stats[batch_id] = {"scored": scored, "anomalies": anomalies}

    def pop(self, batch_id):
        with self.lock:
            return self.stats.pop(batch_id, {"scored": 0, "anomalies": 0})


def build_query(spark, writer: BatchWriter, args):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        # Kafka has no volume: every `docker compose up` is a new demo, so read it from the start.
        .option("startingOffsets", "earliest")
        .load()
    )
    # observe() puts per-batch input counts into query progress without an extra Spark job.
    messages = parse_messages(raw).observe(
        "input",
        F.count(F.lit(1)).alias("rows"),
        F.sum((~F.col("is_valid")).cast("int")).alias("invalid"),
        F.min(F.unix_millis("kafka_ts")).alias("oldest_sent_ms"),
    )
    return (
        messages.where("is_valid")
        .select(*SILVER_COLUMNS)
        .writeStream.foreachBatch(writer)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger} seconds")
        .start()
    )


def batch_metrics(progress, batch_stats):
    """One query progress report + the writer's counts -> dict with METRICS_FIELDS,
    or None for an idle trigger."""
    if progress["numInputRows"] == 0:
        return None
    observed = progress["observedMetrics"]
    started = datetime.fromisoformat(progress["timestamp"].replace("Z", "+00:00"))
    duration = progress["durationMs"]["triggerExecution"] / 1000
    batch_end_ms = started.timestamp() * 1000 + duration * 1000
    return {
        "batch_id": progress["batchId"],
        "batch_end": progress["timestamp"],
        "rows": observed["input"]["rows"],
        "invalid": observed["input"]["invalid"] or 0,
        "scored": batch_stats["scored"],
        "anomalies": batch_stats["anomalies"],
        "duration_sec": round(duration, 2),
        "rows_per_sec": round(progress["numInputRows"] / duration, 1) if duration else 0.0,
        "max_latency_sec": round((batch_end_ms - observed["input"]["oldest_sent_ms"]) / 1000, 2),
    }


def compact(spark, path):
    """Merge the small files written by micro-batches (keeps the dashboard reads fast)."""
    started = time.time()
    DeltaTable.forPath(spark, path).optimize().executeCompaction()
    log.info("compacted %s in %.1fs", path, time.time() - started)


def monitor(spark, query, writer: BatchWriter, args):
    """Log every finished batch, append it to the metrics CSV, compact every N batches."""
    metrics_path = Path(args.output).parent / "batch_metrics.csv"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)  # may run before the first write
    new_file = not metrics_path.exists()
    last_batch_id = -1
    with metrics_path.open("a", newline="") as f:
        csv_writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
        if new_file:
            csv_writer.writeheader()
        # awaitTermination re-raises the streaming error, so a failed query stops this loop.
        while not query.awaitTermination(args.trigger):
            for progress in query.recentProgress:
                if progress["batchId"] <= last_batch_id:
                    continue
                last_batch_id = progress["batchId"]
                m = batch_metrics(progress, writer.pop(progress["batchId"]))
                if m is None:
                    continue
                csv_writer.writerow(m)
                f.flush()
                log.info(
                    "batch %d: %d rows (%d invalid), %d scored, %d anomalies | %.2fs, "
                    "%.0f rows/s | max latency %.1fs",
                    m["batch_id"],
                    m["rows"],
                    m["invalid"],
                    m["scored"],
                    m["anomalies"],
                    m["duration_sec"],
                    m["rows_per_sec"],
                    m["max_latency_sec"],
                )
                if (
                    args.compact_every
                    and m["batch_id"] > 0
                    and m["batch_id"] % args.compact_every == 0
                ):
                    compact(spark, args.output)


def task2_paths(args):
    """--model-dir points at a folder holding the three Task 2 outputs (e.g. mock_data)."""
    if args.model_dir:
        base = Path(args.model_dir)
        return (
            str(base / "baseline_model"),
            str(base / "meter_profile"),
            str(base / "meter_thresholds"),
        )
    return args.model, args.profile, args.thresholds


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Kafka -> Spark Structured Streaming -> Gold")
    p.add_argument("--model", default=MODEL_PATH, help="PipelineModel directory (Task 2)")
    p.add_argument("--profile", default=METER_PROFILE_PATH, help="meter profile Delta table")
    p.add_argument("--thresholds", default=METER_THRESHOLDS_PATH, help="threshold Delta table")
    p.add_argument(
        "--model-dir", help="folder with baseline_model/, meter_profile/, meter_thresholds/"
    )
    p.add_argument("--output", default=ENERGY_RESULTS_PATH, help="Gold Delta table")
    p.add_argument("--checkpoint", default=ENERGY_RESULTS_CHECKPOINT)
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=KAFKA_TOPIC)
    p.add_argument("--trigger", type=int, default=SPARK_TRIGGER_SEC, help="micro-batch interval, s")
    p.add_argument(
        "--compact-every", type=int, default=60, help="OPTIMIZE every N batches, 0 = never"
    )
    return p.parse_args(argv)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = parse_args()
    model_path, profile_path, thresholds_path = task2_paths(args)
    for name, path in [("Model", model_path), ("Meter profile", profile_path)]:
        if not Path(path).exists():
            sys.exit(
                f"{name} not found: {path}. Train it (ml_model/train_baseline.py --save) "
                "or create mocks: python streaming/mock_inputs.py"
            )

    spark = build_streaming_spark("bdg2-streaming-job")
    model = PipelineModel.load(model_path)
    profile = spark.read.format("delta").load(profile_path).cache()
    if Path(thresholds_path).exists():
        thresholds = spark.read.format("delta").load(thresholds_path)
    else:
        log.warning("no thresholds at %s: every meter uses THRESHOLD_RATIO", thresholds_path)
        thresholds = spark.createDataFrame(
            [], "building_id string, meter_type string, anomaly_threshold double"
        )
    writer = BatchWriter(spark, model, profile, thresholds.cache(), args.output)
    query = build_query(spark, writer, args)
    log.info(
        "streaming %s @ %s -> %s (trigger %ss, meter types %s, persistence %dh)",
        args.topic,
        args.bootstrap,
        args.output,
        args.trigger,
        ANOMALY_METER_TYPES,
        ANOMALY_PERSISTENCE_HOURS,
    )
    try:
        monitor(spark, query, writer, args)
    except KeyboardInterrupt:
        log.info("stopping (Ctrl+C)")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
