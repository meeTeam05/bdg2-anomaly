from datetime import datetime

from common.config import CLEAN_DATA_SCHEMA, to_spark_struct_type
from data_pipeline.validate_silver import run_checks

GOOD = (
    "b1",
    "electricity",
    "s1",
    datetime(2016, 1, 1, 14),
    5.0,
    20.0,
    10.0,
    9,
    6,
    0,
    100.0,
    "Office",
)


def failed_checks(spark, rows):
    silver = spark.createDataFrame(rows, to_spark_struct_type(CLEAN_DATA_SCHEMA))
    return [name for name, passed, _ in run_checks(silver) if not passed]


def test_valid_rows_pass_every_check(spark):
    assert failed_checks(spark, [GOOD]) == []


def test_duplicate_negative_and_bad_weekend_are_caught(spark):
    negative = GOOD[:4] + (-1.0,) + GOOD[5:]
    saturday_not_weekend = GOOD[:8] + (7, 0) + GOOD[10:]
    failed = failed_checks(spark, [GOOD, GOOD, negative, saturday_not_weekend])
    assert "unique ['building_id', 'meter_type', 'timestamp']" in failed
    assert "no negative meter_reading" in failed
    assert "is_weekend matches day_of_week" in failed
