"""Per-meter consumption profile: what "normal" looks like for each meter, learned from 2016.

Without it the model only knows building type, site and floor area, so two similar
buildings that use 10x different amounts both get the same guess and both look anomalous.
With it the model predicts the deviation from the meter's own habits.

The profile is one table with a row for every (building_id, meter_type, month, hour_of_day,
is_weekend) of every meter seen in training, so using it is a single inner join:
  profile_meter_log  mean log1p(meter_reading) of the meter over the train period
  profile_month_log  same, for that calendar month (season)
  profile_slot_log   same, for that hour of day and day type (daily routine)
Combinations never seen in training fall back to the meter mean.

Task 2 trains with it and saves it next to the model (METER_PROFILE_PATH); Task 3 calls
add_profile_features() on the stream before the model.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import TEST_START

METER_KEY = ["building_id", "meter_type"]
SLOT_KEY = ["hour_of_day", "is_weekend"]
PROFILE_KEY = [*METER_KEY, "month", *SLOT_KEY]
PROFILE_FEATURES = ["profile_meter_log", "profile_month_log", "profile_slot_log"]


def build_profile(silver: DataFrame) -> DataFrame:
    """Profile from the train period only (timestamp < TEST_START), so 2017 never leaks in."""
    train = silver.filter(F.col("timestamp") < F.lit(TEST_START)).select(
        *METER_KEY,
        *SLOT_KEY,
        F.month("timestamp").alias("month"),
        F.log1p("meter_reading").alias("_log"),
    )
    meter = train.groupBy(*METER_KEY).agg(F.avg("_log").alias("profile_meter_log"))
    month = train.groupBy(*METER_KEY, "month").agg(F.avg("_log").alias("profile_month_log"))
    slot = train.groupBy(*METER_KEY, *SLOT_KEY).agg(F.avg("_log").alias("profile_slot_log"))

    # Full grid: 12 months x 24 hours x 2 day types per meter.
    calendar = (
        train.sparkSession.range(1, 13)
        .select(F.col("id").cast("int").alias("month"))
        .crossJoin(
            train.sparkSession.range(24).select(F.col("id").cast("int").alias("hour_of_day"))
        )
        .crossJoin(train.sparkSession.range(2).select(F.col("id").cast("int").alias("is_weekend")))
    )
    return (
        meter.crossJoin(F.broadcast(calendar))
        .join(month, [*METER_KEY, "month"], "left")
        .join(slot, [*METER_KEY, *SLOT_KEY], "left")
        .select(
            *PROFILE_KEY,
            "profile_meter_log",
            F.coalesce("profile_month_log", "profile_meter_log").alias("profile_month_log"),
            F.coalesce("profile_slot_log", "profile_meter_log").alias("profile_slot_log"),
        )
    )


def add_profile_features(df: DataFrame, profile: DataFrame) -> DataFrame:
    """Attach the profile columns. Rows of meters with no train history are dropped."""
    return (
        df.withColumn("month", F.month("timestamp"))
        .join(profile, PROFILE_KEY)  # inner: no history -> no baseline possible
        .drop("month")
    )


def profile_prediction_log() -> F.Column:
    """No-model reference in log space: daily routine shifted by the season's level."""
    return F.col("profile_slot_log") + F.col("profile_month_log") - F.col("profile_meter_log")
