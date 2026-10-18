"""Task 3: Kafka -> Spark Structured Streaming -> Gold Delta table (energy_results).

Per micro-batch: parse JSON (Silver schema) -> drop invalid rows -> baseline model
-> baseline, residual, is_anomaly -> append to Gold, partitioned by site_id.
The Delta sink + checkpoint give exactly-once writes across restarts.

    python streaming_job.py                                  # real model (Task 2)
    python streaming_job.py --model mock_data/baseline_model # mock model (mock_inputs.py)

One log line and one row in <output>/../batch_metrics.csv per micro-batch (throughput, latency).
"""

import argparse
import csv
import logging
import sys
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root
from config import (  # noqa: E402
    CLEAN_DATA_SCHEMA, ENERGY_RESULTS_CHECKPOINT, ENERGY_RESULTS_PATH, ENERGY_RESULTS_SCHEMA,
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, MODEL_PATH, PARTITION_COLUMN, SPARK_TRIGGER_SEC,
    THRESHOLD_RATIO, to_spark_struct_type,
)
from spark_session import build_spark  # noqa: E402

log = logging.getLogger("streaming_job")

SILVER_STRUCT = to_spark_struct_type(CLEAN_DATA_SCHEMA)
SILVER_COLUMNS = [name for name, _ in CLEAN_DATA_SCHEMA]
GOLD_COLUMNS = [name for name, _ in ENERGY_RESULTS_SCHEMA]
METRICS_FIELDS = ["batch_id", "batch_end", "rows", "invalid", "anomalies",
                  "duration_sec", "rows_per_sec", "max_latency_sec"]


def parse_messages(kafka_df):
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


def score(readings, model, log_target=False):
    """Silver rows -> Gold rows (ENERGY_RESULTS_SCHEMA).

    baseline is clamped at 0: a regression can predict negative kWh, which would make
    `abs(residual) > baseline * ratio` true for every reading.
    log_target: the model was trained on log1p(meter_reading), so undo it with expm1.
    """
    prediction = F.expm1("prediction") if log_target else F.col("prediction")
    return (
        model.transform(readings)
        .withColumn("baseline", F.greatest(prediction, F.lit(0.0)))
        .withColumn("residual", F.col("meter_reading") - F.col("baseline"))
        .withColumn("is_anomaly", F.abs("residual") > F.col("baseline") * THRESHOLD_RATIO)
        .select(*GOLD_COLUMNS)
    )


def build_query(spark, model, args):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        # Kafka has no volume: every `docker compose up` is a new demo, so read it from the start.
        .option("startingOffsets", "earliest")
        .load()
    )
    # observe() puts per-batch counts into query progress without an extra Spark job.
    messages = parse_messages(raw).observe(
        "input",
        F.count(F.lit(1)).alias("rows"),
        F.sum((~F.col("is_valid")).cast("int")).alias("invalid"),
        F.min(F.unix_millis("kafka_ts")).alias("oldest_sent_ms"),
    )
    results = score(messages.where("is_valid"), model, args.log_target).observe(
        "output", F.sum(F.col("is_anomaly").cast("int")).alias("anomalies"),
    )
    return (
        results.writeStream.format("delta")
        .outputMode("append")
        .partitionBy(PARTITION_COLUMN)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger} seconds")
        .start(args.output)
    )


def batch_metrics(progress):
    """One query progress report -> dict with METRICS_FIELDS, or None for an idle trigger."""
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
        "invalid": observed["input"]["invalid"],
        "anomalies": observed["output"]["anomalies"] or 0,
        "duration_sec": round(duration, 2),
        "rows_per_sec": round(progress["numInputRows"] / duration, 1) if duration else 0.0,
        "max_latency_sec": round((batch_end_ms - observed["input"]["oldest_sent_ms"]) / 1000, 2),
    }


def compact(spark, path):
    """Merge the small files written by micro-batches (keeps the dashboard reads fast)."""
    started = time.time()
    DeltaTable.forPath(spark, path).optimize().executeCompaction()
    log.info("compacted %s in %.1fs", path, time.time() - started)


def monitor(spark, query, args):
    """Log every finished batch, append it to the metrics CSV, compact every N batches."""
    metrics_path = Path(args.output).parent / "batch_metrics.csv"
    new_file = not metrics_path.exists()
    last_batch_id = -1
    with metrics_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
        if new_file:
            writer.writeheader()
        # awaitTermination re-raises the streaming error, so a failed query stops this loop.
        while not query.awaitTermination(args.trigger):
            for progress in query.recentProgress:
                if progress["batchId"] <= last_batch_id:
                    continue
                last_batch_id = progress["batchId"]
                m = batch_metrics(progress)
                if m is None:
                    continue
                writer.writerow(m)
                f.flush()
                log.info("batch %d: %d rows (%d invalid), %d anomalies | %.2fs, %.0f rows/s | max latency %.1fs",
                         m["batch_id"], m["rows"], m["invalid"], m["anomalies"],
                         m["duration_sec"], m["rows_per_sec"], m["max_latency_sec"])
                if args.compact_every and m["batch_id"] > 0 and m["batch_id"] % args.compact_every == 0:
                    compact(spark, args.output)


def parse_args():
    p = argparse.ArgumentParser(description="Kafka -> Spark Structured Streaming -> Gold (energy_results)")
    p.add_argument("--model", default=MODEL_PATH, help="PipelineModel directory (Task 2)")
    p.add_argument("--log-target", action="store_true", help="model predicts log1p(meter_reading)")
    p.add_argument("--output", default=ENERGY_RESULTS_PATH, help="Gold Delta table")
    p.add_argument("--checkpoint", default=ENERGY_RESULTS_CHECKPOINT)
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=KAFKA_TOPIC)
    p.add_argument("--trigger", type=int, default=SPARK_TRIGGER_SEC, help="micro-batch interval, seconds")
    p.add_argument("--compact-every", type=int, default=60, help="OPTIMIZE Gold every N batches, 0 = never")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    if not Path(args.model).exists():
        sys.exit(f"Model not found: {args.model}. Train it (ml_model/) or create a mock: python mock_inputs.py")

    spark = build_spark("bdg2-streaming-job")
    model = PipelineModel.load(args.model)
    query = build_query(spark, model, args)
    log.info("streaming %s @ %s -> %s (trigger %ss, threshold %.2f)",
             args.topic, args.bootstrap, args.output, args.trigger, THRESHOLD_RATIO)
    try:
        monitor(spark, query, args)
    except KeyboardInterrupt:
        log.info("stopping (Ctrl+C)")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
