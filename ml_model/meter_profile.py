"""Per-meter consumption profile: what "normal" looks like for each meter, learned from 2016.

Without it the model only knows building type, site and floor area, so two similar
buildings that use 10x different amounts both get the same guess and both look anomalous.
With it the model predicts the deviation from the meter's own habits.

The profile is one small table keyed by (building_id, meter_type, hour_of_day, is_weekend):
  profile_meter_log  mean log1p(meter_reading) of the meter over the whole train period
  profile_slot_log   mean log1p(meter_reading) of the meter at that hour on that day type
Task 2 trains with it and saves it next to the model (METER_PROFILE_PATH); Task 3 joins
it to the stream with add_profile_features() before calling the model.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import TEST_START

METER_KEY = ["building_id", "meter_type"]
SLOT_KEY = [*METER_KEY, "hour_of_day", "is_weekend"]
PROFILE_FEATURES = ["profile_meter_log", "profile_slot_log"]


def build_profile(silver: DataFrame) -> DataFrame:
    """Profile from the train period only (timestamp < TEST_START), so 2017 never leaks in."""
    train = silver.filter(F.col("timestamp") < F.lit(TEST_START)).withColumn(
        "_log", F.log1p("meter_reading")
    )
    meter = train.groupBy(*METER_KEY).agg(F.avg("_log").alias("profile_meter_log"))
    slot = train.groupBy(*SLOT_KEY).agg(F.avg("_log").alias("profile_slot_log"))
    return slot.join(meter, METER_KEY)


def add_profile_features(df: DataFrame, profile: DataFrame) -> DataFrame:
    """Attach profile columns; rows of meters without any train history are dropped.

    A meter seen in training but never at this (hour, day type) falls back to its overall mean.
    """
    meter = profile.select(*METER_KEY, "profile_meter_log").distinct()
    slot = profile.select(*SLOT_KEY, "profile_slot_log")
    return (
        df.join(meter, METER_KEY)  # inner: no history -> no baseline possible
        .join(slot, SLOT_KEY, "left")
        .withColumn("profile_slot_log", F.coalesce("profile_slot_log", "profile_meter_log"))
    )
