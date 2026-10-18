"""Mock inputs so Task 3 runs before Task 1 / Task 2 deliver (TASKS.md section 5).

Writes, with Spark, to streaming/mock_data/:
  clean_data/      Silver Delta table, CLEAN_DATA_SCHEMA, partitioned by site_id,
                   3 sites x 4 buildings, hourly 2016-12-01 .. 2017-01-31
  baseline_model/  PipelineModel shaped like Task 2's: StringIndexer -> VectorAssembler
                   -> RandomForestRegressor, trained on timestamp < TEST_START

    python mock_inputs.py
Then: python kafka_producer.py --silver mock_data/clean_data
      python streaming_job.py --model mock_data/baseline_model
Only for developing/testing: the submitted pipeline runs on the full dataset.
"""

import math
import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root
from config import CLEAN_DATA_SCHEMA, PARTITION_COLUMN, TEST_START  # noqa: E402
from spark_session import build_spark  # noqa: E402

MOCK_DIR = Path(__file__).resolve().parent / "mock_data"
MOCK_SILVER_PATH = str(MOCK_DIR / "clean_data")
MOCK_MODEL_PATH = str(MOCK_DIR / "baseline_model")
START, END = "2016-12-01 00:00:00", "2017-01-31 23:00:00"

# (building_id, site_id, primary_use, square_feet, mean kWh). Tiny_Kiosk has a near-zero
# baseline on purpose: the case where the 28% threshold raises false alarms.
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
FEATURES = ["hour_of_day", "day_of_week", "is_weekend", "air_temperature", "dew_temperature",
            "square_feet", "primary_use_idx", "site_id_idx"]


def make_silver(spark):
    """Daily load curve x weekend dip x 5% noise, plus a daily temperature curve."""
    buildings = spark.createDataFrame(
        BUILDINGS, "building_id string, site_id string, primary_use string, square_feet double, mean_kwh double")
    hours = spark.sql(f"SELECT explode(sequence(timestamp'{START}', timestamp'{END}', interval 1 hour)) AS timestamp")
    hour = F.hour("timestamp")
    air = 5 + 6 * F.sin(2 * math.pi * (hour - 9) / 24) + F.randn(seed=1)
    weekend = F.dayofweek("timestamp").isin(1, 7)  # Spark: 1 = Sunday, 7 = Saturday
    load = F.col("mean_kwh") * (1 + 0.6 * F.sin(2 * math.pi * (hour - 6) / 24)) * F.when(weekend, 0.6).otherwise(1.0)
    df = (
        buildings.crossJoin(hours)
        .withColumn("air_temperature", air)
        .withColumn("dew_temperature", F.col("air_temperature") - 4)
        .withColumn("hour_of_day", hour)
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        .withColumn("is_weekend", weekend.cast("int"))
        .withColumn("meter_reading", F.greatest(load * (1 + 0.05 * F.randn(seed=2)), F.lit(0.0)))
    )
    return df.select(*[F.col(name).cast(dtype) for name, dtype in CLEAN_DATA_SCHEMA])


def train_model(train):
    indexer = StringIndexer(inputCols=["primary_use", "site_id"], outputCols=["primary_use_idx", "site_id_idx"],
                            handleInvalid="keep")
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    forest = RandomForestRegressor(labelCol="meter_reading", featuresCol="features", numTrees=20, maxDepth=8, seed=42)
    return Pipeline(stages=[indexer, assembler, forest]).fit(train)


def main():
    spark = build_spark("bdg2-mock-inputs")
    make_silver(spark).write.format("delta").mode("overwrite").partitionBy(PARTITION_COLUMN).save(MOCK_SILVER_PATH)
    silver = spark.read.format("delta").load(MOCK_SILVER_PATH)
    train = silver.where(F.col("timestamp") < TEST_START)
    train_model(train).write().overwrite().save(MOCK_MODEL_PATH)
    print(f"Silver: {silver.count()} rows ({train.count()} train) -> {MOCK_SILVER_PATH}")
    print(f"Model -> {MOCK_MODEL_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
