"""One test per cleaning decision (D1-D11, notebooks/eda.ipynb section 8), on tiny data."""

from datetime import datetime, timedelta

from common.config import CLEAN_DATA_SCHEMA
from data_pipeline import preprocess as pp

START = datetime(2016, 1, 1)
READING_COLS = "meter_type string, building_id string, timestamp timestamp, meter_reading double"


def hourly(values, meter_type="electricity", building="b1", start=START):
    return [
        (meter_type, building, start + timedelta(hours=i), None if v is None else float(v))
        for i, v in enumerate(values)
    ]


def kept_values(df):
    return [r.meter_reading for r in df.orderBy("meter_type", "building_id", "timestamp").collect()]


def test_unpivot_casts_empty_meters_to_double(spark):
    wide = spark.createDataFrame(
        [(START, 1.5, None), (START + timedelta(hours=1), 2.5, None)],
        "timestamp timestamp, b1 double, b_empty string",
    )
    long = pp.unpivot_meter(wide, "steam")
    assert long.columns == ["meter_type", "building_id", "timestamp", "meter_reading"]
    assert dict(long.dtypes)["meter_reading"] == "double"
    assert long.filter("building_id = 'b1'").count() == 2
    assert long.filter("building_id = 'b_empty' AND meter_reading IS NULL").count() == 2


def test_drop_invalid_removes_null_and_negative(spark):
    df = spark.createDataFrame(hourly([1, None, -3, 0]), READING_COLS)
    assert kept_values(pp.drop_invalid(df)) == [1.0, 0.0]


def test_nonzero_flatline_of_24h_is_dropped_but_23h_is_kept(spark):
    values = [5] * 24 + [1] + [7] * 23 + [2]
    df = pp.add_run_hours(spark.createDataFrame(hourly(values), READING_COLS))
    assert kept_values(pp.drop_flatlines(df)) == [1.0] + [7.0] * 23 + [2.0]


def test_null_gap_breaks_a_run(spark):
    values = [5] * 12 + [None] + [5] * 12
    df = pp.add_run_hours(spark.createDataFrame(hourly(values), READING_COLS))
    assert pp.drop_flatlines(pp.drop_invalid(df)).count() == 24


def test_long_zero_run_dropped_for_electricity_only(spark):
    rows = hourly([0] * 30, "electricity") + hourly([0] * 30, "water")
    df = pp.add_run_hours(spark.createDataFrame(rows, READING_COLS))
    kept = pp.drop_flatlines(df)
    assert kept.filter("meter_type = 'electricity'").count() == 0
    assert kept.filter("meter_type = 'water'").count() == 30


def test_outlier_above_10x_p99_is_dropped(spark):
    values = list(range(1, 101)) + [10_000]
    df = spark.createDataFrame(hourly(values), READING_COLS)
    kept = kept_values(pp.drop_outliers(df))
    assert 10_000.0 not in kept
    assert len(kept) == 100


def test_meter_with_low_coverage_is_dropped(spark, monkeypatch):
    monkeypatch.setattr(pp, "HOURS_PER_SERIES", 10)
    rows = hourly([1] * 5, building="half") + hourly([1] * 4, building="sparse")
    kept = pp.drop_low_coverage(spark.createDataFrame(rows, READING_COLS))
    assert {r.building_id for r in kept.collect()} == {"half"}


def test_interpolate_fills_short_gaps_only(spark):
    temps = [0, None, None, 3] + [None] * 7 + [10]
    rows = [
        ("s1", START + timedelta(hours=i), None if t is None else float(t))
        for i, t in enumerate(temps)
    ]
    frame = spark.createDataFrame(
        rows, "site_id string, timestamp timestamp, air_temperature double"
    )
    filled = [
        r.air_temperature
        for r in pp.interpolate(frame, "air_temperature", 6).orderBy("timestamp").collect()
    ]
    assert filled[:4] == [0.0, 1.0, 2.0, 3.0]
    assert filled[4:11] == [None] * 7  # 7 missing hours > 6: left as null
    assert filled[11] == 10.0


def test_prepare_weather_fills_missing_hours_in_the_grid(spark):
    rows = [("s1", START, 0.0, 0.0), ("s1", START + timedelta(hours=2), 2.0, 4.0)]
    weather = spark.createDataFrame(
        rows, "site_id string, timestamp timestamp, airTemperature double, dewTemperature double"
    )
    out = pp.prepare_weather(weather).orderBy("timestamp").collect()
    assert [r.air_temperature for r in out] == [0.0, 1.0, 2.0]
    assert [r.dew_temperature for r in out] == [0.0, 2.0, 4.0]


def test_prepare_metadata_fills_unknown_use_and_renames(spark):
    meta = spark.createDataFrame(
        [("b1", "s1", None, 100.0, "US/Eastern")],
        "building_id string, site_id string, primaryspaceusage string, sqft double, "
        "timezone string",
    )
    row = pp.prepare_metadata(meta).first()
    assert row.primary_use == "Unknown"
    assert row.square_feet == 100.0


def _silver_inputs(spark, local_start, n_hours, tz):
    readings = spark.createDataFrame(hourly(range(1, n_hours + 1), start=local_start), READING_COLS)
    meta = spark.createDataFrame(
        [("b1", "s1", "Office", 100.0, tz)],
        "building_id string, site_id string, primary_use string, square_feet double, "
        "timezone string",
    )
    weather = spark.createDataFrame(
        [("s1", local_start + timedelta(hours=i), 20.0, 10.0) for i in range(n_hours)],
        "site_id string, timestamp timestamp, air_temperature double, dew_temperature double",
    )
    return readings, meta, weather


def test_to_silver_matches_contract_and_converts_local_time_to_utc(spark):
    # Friday 2016-01-01 09:00 in New York (UTC-5) is 14:00 UTC.
    silver = pp.to_silver(*_silver_inputs(spark, datetime(2016, 1, 1, 9), 1, "US/Eastern"))
    assert [(f.name, f.dataType.simpleString()) for f in silver.schema] == CLEAN_DATA_SCHEMA
    row = silver.first()
    assert row.timestamp == datetime(2016, 1, 1, 14)
    assert (row.hour_of_day, row.day_of_week, row.is_weekend) == (9, 6, 0)  # local hour, Friday


def test_to_silver_drops_the_nonexistent_dst_hour(spark):
    # 2016-03-13 02:00 does not exist in New York; it maps to the same UTC hour as 03:00.
    silver = pp.to_silver(*_silver_inputs(spark, datetime(2016, 3, 13, 1), 3, "US/Eastern"))
    rows = silver.orderBy("timestamp").collect()
    assert len(rows) == 2
    assert [r.meter_reading for r in rows] == [1.0, 3.0]  # 01:00 and the real 03:00
    assert len({r.timestamp for r in rows}) == 2
