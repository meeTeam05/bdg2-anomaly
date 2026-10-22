"""Task 2, step 6: injected anomaly test - does the detection rule catch real anomalies
without flooding the dashboard with false alarms?

1. Predict every 2017 electricity hour with the saved model (+ meter profile).
2. Calibration (2017-01..03): per meter, how far normal hours deviate from the prediction,
   ratio = |actual - baseline| / baseline. Its 95th / 99th percentile is a per-meter threshold.
3. Evaluation (2017-04..12): inject one 24 h anomaly per meter (x1.5, x2, x0.5) and score
   each rule: detection rate (injected windows caught) vs false alarm rate (normal hours
   flagged). Rules: fixed 28% (the original contract) or per-meter thresholds, each
   flagging a single hour or only deviations that persist for 3 consecutive hours.

Run from the repo root (after train_baseline.py --save):
    python ml_model/evaluate.py                       # compare rules
    python ml_model/evaluate.py --save meter_p99_3h   # also save that rule's thresholds
"""

import argparse
import time

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from common.config import (
    ANOMALY_METER_TYPES,
    CLEAN_DATA_PATH,
    METER_PROFILE_PATH,
    METER_THRESHOLDS_PATH,
    MODEL_PATH,
    TEST_START,
    THRESHOLD_RATIO,
)
from common.spark_session import build_spark
from ml_model.meter_profile import METER_KEY, add_profile_features

CALIBRATION_END = "2017-04-01 00:00:00"  # calibration = [TEST_START, CALIBRATION_END)
QUANTILES = {"p95": 0.95, "p99": 0.99}
SCENARIOS = {"x1.5": 1.5, "x2": 2.0, "x0.5": 0.5}  # waste, heavy waste, equipment off
WINDOW_HOURS = 24
EVAL_DAYS = 240  # injected windows start within the first 240 days after CALIBRATION_END
# Rules: (threshold column, consecutive hours required)
RULES = {
    "fixed_28%_1h": ("fixed", 1),
    "fixed_28%_3h": ("fixed", 3),
    "meter_p95_1h": ("p95", 1),
    "meter_p95_3h": ("p95", 3),
    "meter_p99_1h": ("p99", 1),
    "meter_p99_3h": ("p99", 3),
}
MAX_FALSE_ALARM = 0.05  # recommended rule: best detection with at most 5% false alarms


def deviation_ratio(actual: str = "meter_reading") -> F.Column:
    """|actual - baseline| / baseline, the quantity every rule thresholds."""
    baseline = F.greatest(F.col("prediction"), F.lit(1e-6))
    return F.abs(F.col(actual) - F.col("prediction")) / baseline


def calibrate_thresholds(calibration: DataFrame) -> DataFrame:
    """Per-meter percentiles of the normal deviation ratio (one row per meter)."""
    ratio = deviation_ratio()
    return calibration.groupBy(*METER_KEY).agg(
        *[F.percentile_approx(ratio, q).alias(name) for name, q in QUANTILES.items()]
    )


def attach_thresholds(scored: DataFrame, thresholds: DataFrame) -> DataFrame:
    """Add one threshold column per rule family; meters without calibration data fall back
    to the median threshold over all meters."""
    medians = thresholds.agg(
        *[F.percentile_approx(name, 0.5).alias(name) for name in QUANTILES]
    ).first()
    out = scored.join(thresholds, METER_KEY, "left").withColumn("fixed", F.lit(THRESHOLD_RATIO))
    for name in QUANTILES:
        out = out.withColumn(name, F.coalesce(F.col(name), F.lit(medians[name])))
    return out


def inject(scored: DataFrame, factor: float) -> DataFrame:
    """Multiply one 24 h window per meter by `factor`; mark it with `injected`.

    The window start is a deterministic hash of the meter, so every scenario and rule is
    tested on the same windows.
    """
    day = F.abs(F.hash(*METER_KEY)) % EVAL_DAYS
    start = F.to_timestamp(F.lit(CALIBRATION_END)) + F.make_dt_interval(days=day)
    end = start + F.make_dt_interval(hours=F.lit(WINDOW_HOURS))
    injected = (F.col("timestamp") >= start) & (F.col("timestamp") < end)
    return scored.withColumn("injected", injected).withColumn(
        "actual",
        F.when(injected, F.col("meter_reading") * factor).otherwise(F.col("meter_reading")),
    )


def add_rule_flags(df: DataFrame) -> DataFrame:
    """One boolean column per rule. "Nh" rules flag an hour only if it and the previous
    N-1 hours (by clock time, so gaps break the run) all exceed the threshold."""
    ratio = deviation_ratio("actual")
    seconds = F.col("timestamp").cast("long")
    out = df.withColumn("_ratio", ratio).withColumn("_t", seconds)
    for rule, (threshold, hours) in RULES.items():
        exceeds = (F.col("_ratio") > F.col(threshold)).cast("int")
        if hours == 1:
            out = out.withColumn(rule, exceeds == 1)
        else:
            run = Window.partitionBy(*METER_KEY).orderBy("_t").rangeBetween(-(hours - 1) * 3600, 0)
            hit = f"_{rule}_hit"
            out = out.withColumn(hit, exceeds).withColumn(
                rule, (F.sum(hit).over(run) == hours) & (F.count(hit).over(run) == hours)
            )
    return out


def detection_rates(flagged: DataFrame) -> dict[str, float]:
    """Share of injected windows with at least one flagged hour, per rule."""
    windows = (
        flagged.filter("injected")
        .groupBy(*METER_KEY)
        .agg(*[F.max(F.col(r).cast("int")).alias(r) for r in RULES])
    )
    return windows.agg(*[F.avg(r).alias(r) for r in RULES]).first().asDict()


def false_alarm_rates(flagged: DataFrame) -> dict[str, float]:
    """Share of normal (not injected) hours flagged, per rule."""
    normal = flagged.filter(~F.col("injected"))
    return normal.agg(*[F.avg(F.col(r).cast("double")).alias(r) for r in RULES]).first().asDict()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--save",
        choices=[r for r, (t, _) in RULES.items() if t != "fixed"],
        help=f"save that rule's per-meter thresholds to {METER_THRESHOLDS_PATH}",
    )
    args = parser.parse_args()

    started = time.perf_counter()
    spark = build_spark("bdg2_evaluate")
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(MODEL_PATH)
    profile = spark.read.format("delta").load(METER_PROFILE_PATH)
    test_year = (
        spark.read.format("delta")
        .load(CLEAN_DATA_PATH)
        .filter(F.col("meter_type").isin(ANOMALY_METER_TYPES))
        .filter(F.col("timestamp") >= F.lit(TEST_START))
    )
    scored = (
        model.transform(add_profile_features(test_year, profile))
        .select(*METER_KEY, "timestamp", "meter_reading", "prediction")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    in_calibration = F.col("timestamp") < F.lit(CALIBRATION_END)
    calibration, evaluation = scored.filter(in_calibration), scored.filter(~in_calibration)

    thresholds = calibrate_thresholds(calibration).persist(StorageLevel.MEMORY_AND_DISK)
    evaluation = attach_thresholds(evaluation, thresholds).persist(StorageLevel.MEMORY_AND_DISK)
    print(
        f"\nCalibration rows {calibration.count():,} | evaluation rows {evaluation.count():,} "
        f"| meters with thresholds {thresholds.count():,}"
    )
    print(
        "Per-meter threshold, median over meters:",
        {
            name: round(v, 3)
            for name, v in thresholds.agg(
                *[F.percentile_approx(name, 0.5).alias(name) for name in QUANTILES]
            )
            .first()
            .asDict()
            .items()
        },
    )

    false_alarm = false_alarm_rates(add_rule_flags(inject(evaluation, 1.0)))
    detection = {
        s: detection_rates(add_rule_flags(inject(evaluation, f))) for s, f in SCENARIOS.items()
    }

    header = f"{'rule':<14}{'false alarm':>12}" + "".join(f"{'catch ' + s:>12}" for s in SCENARIOS)
    print("\n" + header)
    for rule in RULES:
        row = f"{rule:<14}{false_alarm[rule]:>12.1%}"
        row += "".join(f"{detection[s][rule]:>12.1%}" for s in SCENARIOS)
        print(row)

    eligible = [r for r in RULES if false_alarm[r] <= MAX_FALSE_ALARM]
    if eligible:
        best = max(eligible, key=lambda r: (detection["x1.5"][r], -false_alarm[r]))
        print(
            f"\nRecommended (most x1.5 caught with <= {MAX_FALSE_ALARM:.0%} false alarms): {best}"
        )
    else:
        print(f"\nNo rule keeps false alarms <= {MAX_FALSE_ALARM:.0%}")

    if args.save:
        column = RULES[args.save][0]
        (
            thresholds.select(*METER_KEY, F.col(column).alias("anomaly_threshold"))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", True)
            .save(METER_THRESHOLDS_PATH)
        )
        print(f"Saved {args.save} thresholds to {METER_THRESHOLDS_PATH}")
    print(f"Done in {time.perf_counter() - started:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
