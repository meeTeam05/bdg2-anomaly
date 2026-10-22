from datetime import datetime, timedelta

from ml_model import evaluate as ev

COLS = "building_id string, meter_type string, timestamp timestamp, meter_reading double, "
COLS += "prediction double, fixed double, p95 double, p99 double"
START = datetime(2017, 4, 1)


def frame(spark, readings, threshold=0.28):
    rows = [
        ("b1", "electricity", START + timedelta(hours=i), r, 100.0, 0.28, threshold, threshold)
        for i, r in enumerate(readings)
    ]
    return spark.createDataFrame(rows, COLS)


def flags(df, rule):
    return [r[rule] for r in df.orderBy("timestamp").collect()]


def test_inject_marks_exactly_one_24h_window_and_scales_it(spark):
    df = frame(spark, [100.0] * (24 * 300))
    out = ev.inject(df, 2.0).collect()
    hit = [r for r in out if r.injected]
    assert len(hit) == 24
    assert all(r.actual == 200.0 for r in hit)
    assert all(r.actual == 100.0 for r in out if not r.injected)
    hours = sorted(r.timestamp for r in hit)
    assert hours[-1] - hours[0] == timedelta(hours=23)


def test_persistence_rule_needs_three_consecutive_hours(spark):
    # deviations: one isolated spike, then three in a row
    readings = [100.0, 150.0, 100.0, 100.0, 150.0, 150.0, 150.0, 100.0]
    df = ev.add_rule_flags(
        ev.inject(frame(spark, readings), 1.0).withColumn("injected", ev.F.lit(False))
    )
    assert flags(df, "fixed_28%_1h") == [False, True, False, False, True, True, True, False]
    assert flags(df, "fixed_28%_3h") == [False, False, False, False, False, False, True, False]


def test_gap_in_hours_breaks_persistence(spark):
    rows = [
        ("b1", "electricity", START + timedelta(hours=h), 150.0, 100.0, 0.28, 0.28, 0.28)
        for h in (0, 1, 3)  # hour 2 missing
    ]
    df = ev.add_rule_flags(ev.inject(spark.createDataFrame(rows, COLS), 1.0))
    assert flags(df, "fixed_28%_3h") == [False, False, False]


def test_calibrated_threshold_is_the_meters_own_percentile(spark):
    readings = [100.0 + i for i in range(100)]  # deviation ratio 0.00 .. 0.99
    t = ev.calibrate_thresholds(frame(spark, readings)).first()
    assert 0.9 <= t.p95 <= 0.96
    assert 0.97 <= t.p99 <= 0.99
