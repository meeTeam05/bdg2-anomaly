import math
from datetime import datetime

from ml_model.meter_profile import add_profile_features, build_profile

COLS = "building_id string, meter_type string, timestamp timestamp, hour_of_day int, "
COLS += "is_weekend int, meter_reading double"


def rows(spark, data):
    return spark.createDataFrame(data, COLS)


def test_profile_ignores_the_test_year(spark):
    silver = rows(
        spark,
        [
            ("b1", "electricity", datetime(2016, 6, 1, 9), 9, 0, math.expm1(2.0)),
            ("b1", "electricity", datetime(2017, 6, 1, 9), 9, 0, math.expm1(8.0)),  # 2017
        ],
    )
    row = build_profile(silver).first()
    assert row.profile_meter_log == 2.0
    assert row.profile_slot_log == 2.0


def test_slot_profile_differs_by_hour_and_falls_back_to_meter_mean(spark):
    silver = rows(
        spark,
        [
            ("b1", "electricity", datetime(2016, 6, 1, 3), 3, 0, math.expm1(1.0)),
            ("b1", "electricity", datetime(2016, 6, 1, 14), 14, 0, math.expm1(3.0)),
        ],
    )
    profile = build_profile(silver)
    stream = rows(
        spark,
        [
            ("b1", "electricity", datetime(2017, 1, 2, 14), 14, 0, 1.0),  # seen slot
            ("b1", "electricity", datetime(2017, 1, 7, 14), 14, 1, 1.0),  # weekend: unseen
        ],
    )
    out = {r.is_weekend: r for r in add_profile_features(stream, profile).collect()}
    assert out[0].profile_slot_log == 3.0
    assert out[1].profile_slot_log == 2.0  # meter mean of 1.0 and 3.0
    assert out[1].profile_meter_log == 2.0


def test_meter_without_history_is_dropped(spark):
    profile = build_profile(
        rows(spark, [("b1", "electricity", datetime(2016, 6, 1, 9), 9, 0, 5.0)])
    )
    stream = rows(spark, [("b_new", "electricity", datetime(2017, 1, 2, 9), 9, 0, 5.0)])
    assert add_profile_features(stream, profile).count() == 0
