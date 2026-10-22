import math
from datetime import datetime

from ml_model.meter_profile import add_profile_features, build_profile, profile_prediction_log

COLS = "building_id string, meter_type string, timestamp timestamp, hour_of_day int, "
COLS += "is_weekend int, meter_reading double"


def rows(spark, data):
    return spark.createDataFrame(data, COLS)


def reading(ts, log_value, weekend=0, building="b1"):
    return (building, "electricity", ts, ts.hour, weekend, math.expm1(log_value))


def test_profile_ignores_the_test_year(spark):
    silver = rows(
        spark,
        [reading(datetime(2016, 6, 1, 9), 2.0), reading(datetime(2017, 6, 1, 9), 8.0)],
    )
    row = build_profile(silver).filter("month = 6 AND hour_of_day = 9 AND is_weekend = 0").first()
    assert (row.profile_meter_log, row.profile_month_log, row.profile_slot_log) == (2.0, 2.0, 2.0)


def test_profile_is_a_full_grid_per_meter(spark):
    profile = build_profile(rows(spark, [reading(datetime(2016, 6, 1, 9), 2.0)]))
    assert profile.count() == 12 * 24 * 2


def test_month_and_slot_levels_with_fallback_to_meter_mean(spark):
    silver = rows(
        spark,
        [
            reading(datetime(2016, 1, 4, 3), 1.0),  # January night
            reading(datetime(2016, 7, 4, 14), 3.0),  # July afternoon
        ],
    )
    profile = build_profile(silver)
    stream = rows(
        spark,
        [
            reading(datetime(2017, 7, 3, 14), 0.0),  # seen month and slot
            reading(datetime(2017, 3, 4, 10), 0.0, weekend=1),  # unseen month and slot
        ],
    )
    out = {r.timestamp.month: r for r in add_profile_features(stream, profile).collect()}
    assert (out[7].profile_month_log, out[7].profile_slot_log) == (3.0, 3.0)
    assert (out[3].profile_month_log, out[3].profile_slot_log) == (2.0, 2.0)  # meter mean
    assert out[3].profile_meter_log == 2.0
    assert "month" not in add_profile_features(stream, profile).columns


def test_profile_prediction_combines_routine_and_season(spark):
    silver = rows(
        spark,
        [
            reading(datetime(2016, 1, 4, 3), 1.0),
            reading(datetime(2016, 7, 4, 3), 3.0),
        ],
    )
    stream = rows(spark, [reading(datetime(2017, 7, 3, 3), 0.0)])
    row = (
        add_profile_features(stream, build_profile(silver))
        .select(profile_prediction_log().alias("p"))
        .first()
    )
    assert row.p == 2.0 + 3.0 - 2.0  # 3am routine (mean 2.0) shifted up to the July level


def test_meter_without_history_is_dropped(spark):
    profile = build_profile(rows(spark, [reading(datetime(2016, 6, 1, 9), 2.0)]))
    stream = rows(spark, [reading(datetime(2017, 1, 2, 9), 2.0, building="b_new")])
    assert add_profile_features(stream, profile).count() == 0
