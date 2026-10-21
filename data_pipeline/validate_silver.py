"""Step 4 of Task 1: check the Silver table against the data contract before hand-off.

Exits with code 1 if any check fails, so it can gate CI or a demo run.

Run from the repo root (after data_pipeline/preprocess.py):
    python data_pipeline/validate_silver.py
"""

import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import (
    CLEAN_DATA_PATH,
    CLEAN_DATA_SCHEMA,
    METER_TYPES,
    PARTITION_COLUMN,
    TEST_START,
)
from common.spark_session import build_spark

KEY = ["building_id", "meter_type", "timestamp"]
# Local 2016-01-01 00:00 .. 2017-12-31 23:00 converted to UTC across UTC-8 .. UTC+0 sites.
UTC_RANGE = (datetime(2016, 1, 1, 0), datetime(2018, 1, 1, 8))


def run_checks(silver: DataFrame) -> list[tuple[str, bool, str]]:
    """Return (check, passed, detail) for every contract rule."""
    actual_schema = [(f.name, f.dataType.simpleString()) for f in silver.schema]
    c = F.col
    stats = silver.agg(
        F.count("*").alias("rows"),
        F.sum(sum(c(n).isNull().cast("int") for n, _ in CLEAN_DATA_SCHEMA)).alias("nulls"),
        F.sum((c("meter_reading") < 0).cast("int")).alias("negatives"),
        F.sum((~c("hour_of_day").between(0, 23)).cast("int")).alias("bad_hour"),
        F.sum((~c("day_of_week").between(1, 7)).cast("int")).alias("bad_dow"),
        F.sum((c("is_weekend") != c("day_of_week").isin(1, 7).cast("int")).cast("int")).alias(
            "bad_weekend"
        ),
        F.sum((~c("meter_type").isin(METER_TYPES)).cast("int")).alias("bad_meter_type"),
        F.min("timestamp").alias("first"),
        F.max("timestamp").alias("last"),
    ).first()
    duplicates = silver.groupBy(*KEY).count().filter("count > 1").count()
    in_range = UTC_RANGE[0] <= stats.first and stats.last <= UTC_RANGE[1]

    return [
        ("schema = CLEAN_DATA_SCHEMA", actual_schema == CLEAN_DATA_SCHEMA, str(actual_schema)),
        ("not empty", stats.rows > 0, f"{stats.rows:,} rows"),
        ("no nulls", stats.nulls == 0, f"{stats.nulls:,} nulls"),
        ("no negative meter_reading", stats.negatives == 0, f"{stats.negatives:,}"),
        (f"unique {KEY}", duplicates == 0, f"{duplicates:,} duplicated keys"),
        ("hour_of_day in 0..23", stats.bad_hour == 0, f"{stats.bad_hour:,} bad"),
        ("day_of_week in 1..7", stats.bad_dow == 0, f"{stats.bad_dow:,} bad"),
        ("is_weekend matches day_of_week", stats.bad_weekend == 0, f"{stats.bad_weekend:,} bad"),
        ("meter_type in METER_TYPES", stats.bad_meter_type == 0, f"{stats.bad_meter_type:,} bad"),
        ("timestamps in UTC range", in_range, f"{stats.first} .. {stats.last}"),
    ]


def main() -> None:
    spark = build_spark("bdg2_validate_silver")
    spark.sparkContext.setLogLevel("WARN")
    silver = spark.read.format("delta").load(CLEAN_DATA_PATH)

    checks = run_checks(silver)
    partitions = spark.sql(f"DESCRIBE DETAIL delta.`{CLEAN_DATA_PATH}`").first().partitionColumns
    checks.append(
        (f"partitioned by {PARTITION_COLUMN}", partitions == [PARTITION_COLUMN], str(partitions))
    )

    print(f"\nSilver: {CLEAN_DATA_PATH}\n")
    for name, passed, detail in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name:<34} {detail[:80]}")

    print("\nHand-off summary (Task 2 / Task 3):")
    silver.groupBy("meter_type").agg(
        F.countDistinct("building_id").alias("meters"),
        F.count("*").alias("rows"),
        F.sum((F.col("timestamp") < F.lit(TEST_START)).cast("int")).alias("train_rows"),
        F.sum((F.col("timestamp") >= F.lit(TEST_START)).cast("int")).alias("test_rows"),
    ).orderBy(F.desc("rows")).show(truncate=False)
    silver.select(
        F.countDistinct("building_id").alias("buildings"),
        F.countDistinct("site_id").alias("sites"),
        F.countDistinct("primary_use").alias("primary_uses"),
    ).show()
    spark.stop()

    failed = [name for name, passed, _ in checks if not passed]
    if failed:
        print(f"FAILED: {failed}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
