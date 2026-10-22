"""Mock inputs so Task 3 runs without the full Task 1 / Task 2 outputs (TASKS.md section 5).

Writes, with Spark, to streaming/mock_data/ (same layout as data/silver + data/models):
  clean_data/        Silver Delta table, CLEAN_DATA_SCHEMA, partitioned by site_id,
                     3 sites x 4 electricity meters, hourly 2016-12-01 .. 2017-01-31
  meter_profile/     ml_model.meter_profile.build_profile() on the 2016 rows
  baseline_model/    ml_model.train_baseline.build_pipeline("rf", "raw"), trained on 2016
  meter_thresholds/  ml_model.evaluate.calibrate_thresholds() on 2017 predictions
The model, profile and thresholds come from the real Task 2 code, so they have exactly the
shape streaming_job.py gets in production.

    python streaming/mock_inputs.py
Then: python streaming/kafka_producer.py --silver streaming/mock_data/clean_data
      python streaming/streaming_job.py --model-dir streaming/mock_data
Only for developing/testing: the submitted pipeline runs on the full dataset.
"""

import math
from pathlib import Path

from pyspark.sql import functions as F

from common.config import CLEAN_DATA_SCHEMA, PARTITION_COLUMN, TEST_START
from common.spark_session import build_spark
from ml_model.evaluate import calibrate_thresholds
from ml_model.meter_profile import add_profile_features, build_profile
from ml_model.train_baseline import build_pipeline

MOCK_DIR = Path(__file__).resolve().parent / "mock_data"
MOCK_SILVER_PATH = str(MOCK_DIR / "clean_data")
MOCK_MODEL_PATH = str(MOCK_DIR / "baseline_model")
MOCK_PROFILE_PATH = str(MOCK_DIR / "meter_profile")
MOCK_THRESHOLDS_PATH = str(MOCK_DIR / "meter_thresholds")
START, END = "2016-12-01 00:00:00", "2017-01-31 23:00:00"

# (building_id, site_id, primary_use, square_feet, mean kWh). Tiny_Kiosk has a near-zero
# baseline on purpose: the case where a relative threshold raises false alarms.
BUILDINGS = [
    ("Panther_education_Ana", "Panther", "Education", 120000.0, 300.0),
    ("Panther_office_Bob", "Panther", "Office", 60000.0, 150.0),
    ("Panther_lodging_Cara", "Panther", "Lodging/residential", 40000.0, 80.0),
    ("Panther_public_Tiny_Kiosk", "Panther", "Public services", 500.0, 0.3),
    ("Fox_education_Dan", "Fox", "Education", 200000.0, 500.0),
    ("Fox_office_Eve", "Fox", "Office", 80000.0, 200.0),
    ("Fox_office_Finn", "Fox", "Office", 30000.0, 60.0),
    ("Fox_lodging_Gia", "Fox", "Lodging/residential", 50000.0, 90.0),
    ("Rat_education_Hal", "Rat", "Education", 150000.0, 400.0),
    ("Rat_office_Ivy", "Rat", "Office", 70000.0, 170.0),
    ("Rat_public_Jon", "Rat", "Public services", 20000.0, 40.0),
    ("Rat_lodging_Kim", "Rat", "Lodging/residential", 45000.0, 85.0),
]


def make_silver(spark):
    """Daily load curve x weekend dip x 5% noise, plus a daily temperature curve."""
    buildings = spark.createDataFrame(
        BUILDINGS,
        "building_id string, site_id string, primary_use string, square_feet double, "
        "mean_kwh double",
    )
    hours = spark.sql(
        f"SELECT explode(sequence(timestamp'{START}', timestamp'{END}', interval 1 hour)) "
        "AS timestamp"
    )
    hour = F.hour("timestamp")
    air = 5 + 6 * F.sin(2 * math.pi * (hour - 9) / 24) + F.randn(seed=1)
    weekend = F.dayofweek("timestamp").isin(1, 7)  # Spark: 1 = Sunday, 7 = Saturday
    load = (
        F.col("mean_kwh")
        * (1 + 0.6 * F.sin(2 * math.pi * (hour - 6) / 24))
        * F.when(weekend, 0.6).otherwise(1.0)
    )
    df = (
        buildings.crossJoin(hours)
        .withColumn("meter_type", F.lit("electricity"))
        .withColumn("air_temperature", air)
        .withColumn("dew_temperature", F.col("air_temperature") - 4)
        .withColumn("hour_of_day", hour)
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        .withColumn("is_weekend", weekend.cast("int"))
        .withColumn("meter_reading", F.greatest(load * (1 + 0.05 * F.randn(seed=2)), F.lit(0.0)))
    )
    return df.select(*[F.col(name).cast(dtype) for name, dtype in CLEAN_DATA_SCHEMA])


def main():
    spark = build_spark("bdg2-mock-inputs")
    make_silver(spark).write.format("delta").mode("overwrite").partitionBy(PARTITION_COLUMN).save(
        MOCK_SILVER_PATH
    )
    silver = spark.read.format("delta").load(MOCK_SILVER_PATH)

    profile = build_profile(silver)
    profile.write.format("delta").mode("overwrite").save(MOCK_PROFILE_PATH)
    profile = spark.read.format("delta").load(MOCK_PROFILE_PATH)

    rows = add_profile_features(silver, profile)
    train = rows.where(F.col("timestamp") < TEST_START)
    model = build_pipeline("rf", "raw", use_profile=True, num_trees=20).fit(train)
    model.write().overwrite().save(MOCK_MODEL_PATH)

    test_predictions = model.transform(rows.where(F.col("timestamp") >= TEST_START))
    calibrate_thresholds(test_predictions).select(
        "building_id", "meter_type", F.col("p99").alias("anomaly_threshold")
    ).write.format("delta").mode("overwrite").save(MOCK_THRESHOLDS_PATH)

    print(f"Silver: {silver.count()} rows ({train.count()} train) -> {MOCK_SILVER_PATH}")
    print(f"Model, profile, thresholds -> {MOCK_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
