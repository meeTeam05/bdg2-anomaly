"""Step 3 of Task 1: Bronze -> Silver (data/silver/clean_data).

Implements the cleaning decisions D1-D11 from notebooks/eda.ipynb, section 8:
  D1   unpivot the 8 wide meter tables to one long table, readings as double
  D2   drop null and negative readings
  D3   drop non-zero values stuck for >= 24 h (all meters)
  D4   drop zeros stuck for >= 24 h (electricity only; other meters switch off seasonally)
  D5   drop readings > 10 x the meter's own p99
  D6   drop meters left with < 50% of the hours
  D7   weather: air/dew temperature only, linear interpolation for gaps <= 6 h per site
  D8   join in local time, then convert timestamp to UTC; drop DST duplicates
  D9   hour_of_day / day_of_week / is_weekend from local time
  D10  missing primary_use -> "Unknown"
  D11  rename columns to CLEAN_DATA_SCHEMA

Run from the repo root (after data_pipeline/ingest.py):
    python data_pipeline/preprocess.py
"""

import time
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from common.config import (
    BRONZE_DIR,
    CLEAN_DATA_PATH,
    CLEAN_DATA_SCHEMA,
    METER_TYPES,
    PARTITION_COLUMN,
)
from common.spark_session import build_spark

HOURS_PER_SERIES = 17_544  # 2016-01-01 00:00 .. 2017-12-31 23:00, hourly
FLATLINE_MIN_HOURS = 24  # D3, D4
OUTLIER_P99_FACTOR = 10  # D5
MIN_COVERAGE = 0.5  # D6
MAX_WEATHER_GAP_HOURS = 6  # D7
UNKNOWN_USE = "Unknown"  # D10

SERIES_KEY = ["meter_type", "building_id"]
LINEAGE = ["_source_file", "_ingested_at"]
# Serialized, spills to disk: ~50M rows must fit next to a 6 GB driver on a 16 GB laptop.
CACHE = StorageLevel.MEMORY_AND_DISK


def read_bronze(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format("delta").load(f"{BRONZE_DIR}/{name}").drop(*LINEAGE)


def unpivot_meter(wide: DataFrame, meter_type: str) -> DataFrame:
    """D1: (timestamp, <building>...) -> (meter_type, building_id, timestamp, meter_reading).

    Meters that are 100% empty were inferred as string in Bronze, so cast every one.
    """
    buildings = [c for c in wide.columns if c != "timestamp"]
    typed = wide.select("timestamp", *[F.col(c).cast("double").alias(c) for c in buildings])
    return typed.unpivot("timestamp", buildings, "building_id", "meter_reading").select(
        F.lit(meter_type).alias("meter_type"), "building_id", "timestamp", "meter_reading"
    )


def add_run_hours(readings: DataFrame) -> DataFrame:
    """Add `run_hours`: length of the run of identical consecutive values the row belongs to.

    A new run starts whenever the value differs from the previous hour (nulls included,
    so a gap breaks a run).
    """
    by_time = Window.partitionBy(*SERIES_KEY).orderBy("timestamp")
    same_as_prev = F.lag("meter_reading").over(by_time).eqNullSafe(F.col("meter_reading"))
    return (
        readings.withColumn("_run_start", F.when(same_as_prev, 0).otherwise(1))
        .withColumn("_run_id", F.sum("_run_start").over(by_time))
        .withColumn("run_hours", F.count("*").over(Window.partitionBy(*SERIES_KEY, "_run_id")))
        .drop("_run_start", "_run_id")
    )


def drop_invalid(readings: DataFrame) -> DataFrame:
    """D2: drop null and negative readings."""
    return readings.filter(F.col("meter_reading").isNotNull() & (F.col("meter_reading") >= 0))


def drop_flatlines(readings: DataFrame) -> DataFrame:
    """D3 + D4. Expects `run_hours` from add_run_hours(); drops it afterwards."""
    long_run = F.col("run_hours") >= FLATLINE_MIN_HOURS
    stuck_nonzero = long_run & (F.col("meter_reading") != 0)
    stuck_zero_power = (
        long_run & (F.col("meter_reading") == 0) & (F.col("meter_type") == "electricity")
    )
    return readings.filter(~(stuck_nonzero | stuck_zero_power)).drop("run_hours")


def drop_outliers(readings: DataFrame) -> DataFrame:
    """D5: drop readings above OUTLIER_P99_FACTOR x the meter's p99 (p99 over positive values)."""
    p99 = (
        readings.filter(F.col("meter_reading") > 0)
        .groupBy(*SERIES_KEY)
        .agg(F.percentile_approx("meter_reading", 0.99).alias("_p99"))
    )
    return (
        readings.join(p99, SERIES_KEY, "left")
        .filter(
            F.col("_p99").isNull() | (F.col("meter_reading") <= OUTLIER_P99_FACTOR * F.col("_p99"))
        )
        .drop("_p99")
    )


def drop_low_coverage(readings: DataFrame) -> DataFrame:
    """D6: keep meters with at least MIN_COVERAGE of the hours left."""
    kept_hours = F.count("*").over(Window.partitionBy(*SERIES_KEY))
    return (
        readings.withColumn("_hours", kept_hours)
        .filter(F.col("_hours") >= MIN_COVERAGE * HOURS_PER_SERIES)
        .drop("_hours")
    )


def interpolate(frame: DataFrame, column: str, max_gap_hours: int) -> DataFrame:
    """Linear interpolation of `column` over time within each site, for gaps <= max_gap_hours.

    `frame` must hold one row per (site_id, hour), missing hours included as nulls.
    """
    # Both frames are running frames (start .. current row), which Spark evaluates in one
    # pass. "Next known value" = "last known value" with time reversed; a frame ending at
    # unboundedFollowing would rescan the rest of the series for every row (quadratic).
    running = (Window.unboundedPreceding, Window.currentRow)
    before = Window.partitionBy("site_id").orderBy("timestamp").rowsBetween(*running)
    after = Window.partitionBy("site_id").orderBy(F.desc("timestamp")).rowsBetween(*running)
    known_at = F.when(F.col(column).isNotNull(), F.col("timestamp").cast("long"))

    prev_value, prev_at = F.last(column, True).over(before), F.last(known_at, True).over(before)
    next_value, next_at = F.last(column, True).over(after), F.last(known_at, True).over(after)
    missing_hours = (next_at - prev_at) / 3600 - 1
    fraction = (F.col("timestamp").cast("long") - prev_at) / (next_at - prev_at)

    filled = F.when(F.col(column).isNotNull(), F.col(column)).when(
        missing_hours <= max_gap_hours, prev_value + (next_value - prev_value) * fraction
    )
    return frame.withColumn(column, filled)


def prepare_weather(weather: DataFrame) -> DataFrame:
    """D7 + D11: (site_id, timestamp, air_temperature, dew_temperature), short gaps filled."""
    observed = weather.select(
        "site_id",
        "timestamp",
        F.col("airTemperature").alias("air_temperature"),
        F.col("dewTemperature").alias("dew_temperature"),
    )
    bounds = observed.agg(F.min("timestamp").alias("lo"), F.max("timestamp").alias("hi"))
    hours = bounds.select(
        F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 HOUR"))).alias("timestamp")
    )
    # Full hourly grid per site, so missing hours become explicit nulls to interpolate.
    grid = observed.select("site_id").distinct().crossJoin(hours)
    frame = grid.join(observed, ["site_id", "timestamp"], "left")
    for column in ("air_temperature", "dew_temperature"):
        frame = interpolate(frame, column, MAX_WEATHER_GAP_HOURS)
    return frame


def prepare_metadata(metadata: DataFrame) -> DataFrame:
    """D10 + D11: one row per building with the columns Silver needs."""
    return metadata.select(
        "building_id",
        "site_id",
        F.coalesce(F.col("primaryspaceusage"), F.lit(UNKNOWN_USE)).alias("primary_use"),
        F.col("sqft").cast("double").alias("square_feet"),
        "timezone",
    )


def to_silver(readings: DataFrame, metadata: DataFrame, weather: DataFrame) -> DataFrame:
    """D8 + D9 + D11: join in local time, add time features, convert to UTC, apply the schema."""
    joined = (
        readings.join(metadata, "building_id")
        .join(weather, ["site_id", "timestamp"], "left")
        .filter(F.col("air_temperature").isNotNull() & F.col("dew_temperature").isNotNull())
        .withColumn("hour_of_day", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))  # 1 = Sunday ... 7 = Saturday
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumnRenamed("timestamp", "local_timestamp")
        .withColumn("timestamp", F.to_utc_timestamp("local_timestamp", F.col("timezone")))
    )
    # On the spring DST change the series has a local hour that does not exist; it maps to
    # the same UTC hour as the next one. Keep the real (later) local hour.
    latest_local = Window.partitionBy("building_id", "meter_type", "timestamp").orderBy(
        F.desc("local_timestamp")
    )
    deduped = (
        joined.withColumn("_rank", F.row_number().over(latest_local))
        .filter("_rank = 1")
        .drop("_rank")
    )
    return deduped.select(
        *[F.col(name).cast(dtype).alias(name) for name, dtype in CLEAN_DATA_SCHEMA]
    )


def main() -> None:
    started = time.perf_counter()
    spark = build_spark("bdg2_preprocess")
    spark.sparkContext.setLogLevel("WARN")
    steps: list[tuple[str, int]] = []

    meters = [unpivot_meter(read_bronze(spark, m), m) for m in METER_TYPES]
    readings = add_run_hours(reduce(DataFrame.unionByName, meters)).persist(CACHE)
    steps.append(("D1 unpivot (Bronze)", readings.count()))

    valid = drop_invalid(readings)
    steps.append(("D2 drop null / negative", valid.count()))

    no_flat = drop_flatlines(valid)
    steps.append(("D3+D4 drop flatlines >= 24h", no_flat.count()))

    no_outliers = drop_outliers(no_flat).persist(CACHE)
    steps.append(("D5 drop outliers > 10 x p99", no_outliers.count()))
    readings.unpersist()

    clean = drop_low_coverage(no_outliers).persist(CACHE)
    steps.append(("D6 drop meters < 50% hours", clean.count()))
    no_outliers.unpersist()

    weather = prepare_weather(read_bronze(spark, "weather"))
    metadata = prepare_metadata(read_bronze(spark, "metadata"))
    silver = to_silver(clean, metadata, weather)

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy(PARTITION_COLUMN)
        .save(CLEAN_DATA_PATH)
    )
    clean.unpersist()
    written = spark.read.format("delta").load(CLEAN_DATA_PATH)
    steps.append(("D7+D8 join weather, UTC, dedupe", written.count()))

    per_type = written.groupBy("meter_type").agg(
        F.countDistinct("building_id").alias("meters"), F.count("*").alias("rows")
    )
    print(f"\n{'step':<34}{'rows':>14}{'removed':>14}")
    previous = None
    for name, rows in steps:
        removed = "" if previous is None else f"{previous - rows:,}"
        print(f"{name:<34}{rows:>14,}{removed:>14}")
        previous = rows
    print()
    per_type.orderBy(F.desc("rows")).show(truncate=False)
    print(f"Silver written to {CLEAN_DATA_PATH} in {time.perf_counter() - started:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
