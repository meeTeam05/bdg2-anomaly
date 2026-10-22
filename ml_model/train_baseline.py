"""Task 2: train and compare baseline consumption models on the Silver table.

Silver -> time split (2016 train / 2017 test) -> MLlib Pipeline
(StringIndexer -> VectorAssembler -> Regressor) -> RMSE/MAE on the test year,
for every requested model x target combination. Metrics are always on the original
meter_reading scale, so raw and log1p targets are directly comparable.

With the log1p target the saved pipeline ends with an expm1 stage, so its `prediction`
column is already in meter_reading units (Task 3 does not need to transform it).

Run from the repo root (after data_pipeline/preprocess.py):
    python ml_model/train_baseline.py                          # 1% sample, all combinations
    python ml_model/train_baseline.py --models gbt --targets log1p --sample 1 --save
"""

import argparse
import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import CLEAN_DATA_PATH, MODEL_PATH, TEST_START, THRESHOLD_RATIO
from common.spark_session import build_spark

CATEGORICAL = ["meter_type", "primary_use", "site_id"]
NUMERIC = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "air_temperature",
    "dew_temperature",
    "square_feet",
]
LABEL = "meter_reading"
LOG_LABEL = "log1p_meter_reading"
# Solar meters generate power; anomaly detection covers consumption meters only.
EXCLUDED_METER_TYPES = ["solar"]

MODELS = ["lr", "rf", "gbt"]
TARGETS = ["raw", "log1p"]


def build_regressor(name: str, label_col: str, prediction_col: str):
    """Return an unfitted regressor; tree sizes are kept moderate for the full dataset."""
    common = {"featuresCol": "features", "labelCol": label_col, "predictionCol": prediction_col}
    if name == "lr":
        return LinearRegression(**common)
    if name == "rf":
        return RandomForestRegressor(numTrees=50, maxDepth=10, seed=42, **common)
    if name == "gbt":
        return GBTRegressor(maxIter=50, maxDepth=6, seed=42, **common)
    raise ValueError(f"unknown model {name!r}")


def build_pipeline(model_name: str = "lr", target: str = "raw") -> Pipeline:
    """Feature stages + regressor; the fitted model outputs `prediction` in meter_reading units.

    target "log1p" trains on log1p(meter_reading) (column LOG_LABEL must exist at fit time)
    and appends an expm1 stage so the output scale matches the raw target.
    """
    indexers = [
        # "keep" maps categories unseen in training (e.g. a new site) to an extra index.
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL
    ]
    stages = [*indexers]
    if model_name == "lr":
        # A linear model would read category indices as ordered numbers; one-hot them.
        # Trees use the indices directly (StringIndexer metadata marks them categorical).
        stages.append(
            OneHotEncoder(
                inputCols=[f"{c}_idx" for c in CATEGORICAL],
                outputCols=[f"{c}_vec" for c in CATEGORICAL],
            )
        )
        category_features = [f"{c}_vec" for c in CATEGORICAL]
    else:
        category_features = [f"{c}_idx" for c in CATEGORICAL]
    stages.append(VectorAssembler(inputCols=NUMERIC + category_features, outputCol="features"))

    if target == "log1p":
        stages.append(build_regressor(model_name, LOG_LABEL, "log_prediction"))
        stages.append(
            SQLTransformer(statement="SELECT *, expm1(log_prediction) AS prediction FROM __THIS__")
        )
    elif target == "raw":
        stages.append(build_regressor(model_name, LABEL, "prediction"))
    else:
        raise ValueError(f"unknown target {target!r}")
    return Pipeline(stages=stages)


def with_log_label(df: DataFrame) -> DataFrame:
    """Add the log1p target column (Silver has no negative readings)."""
    return df.withColumn(LOG_LABEL, F.log1p(LABEL))


def score(predictions: DataFrame) -> dict[str, float]:
    """RMSE / MAE / RMSLE on the meter_reading scale, plus the anomaly flag rate.

    flag_rate is the share of test rows that the Gold rule
    abs(residual) > baseline * THRESHOLD_RATIO would flag. The test year is assumed
    normal, so this approximates the false alarm rate.
    """
    error = F.col(LABEL) - F.col("prediction")
    log_error = F.log1p(F.greatest(F.col("prediction"), F.lit(0.0))) - F.log1p(LABEL)
    row = predictions.agg(
        F.sqrt(F.avg(error * error)).alias("rmse"),
        F.avg(F.abs(error)).alias("mae"),
        F.sqrt(F.avg(log_error * log_error)).alias("rmsle"),
        F.avg((F.abs(error) > F.col("prediction") * THRESHOLD_RATIO).cast("double")).alias(
            "flag_rate"
        ),
    ).first()
    return row.asDict()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", type=float, default=0.01, help="fraction of rows (0-1]")
    parser.add_argument("--models", nargs="+", choices=MODELS, default=MODELS)
    parser.add_argument("--targets", nargs="+", choices=TARGETS, default=TARGETS)
    parser.add_argument(
        "--save", action="store_true", help=f"save the best model (by RMSE) to {MODEL_PATH}"
    )
    args = parser.parse_args()

    started = time.perf_counter()
    spark = build_spark("bdg2_train_baseline")
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(CLEAN_DATA_PATH)
    data = silver.filter(~F.col("meter_type").isin(EXCLUDED_METER_TYPES))
    if args.sample < 1:
        data = data.sample(fraction=args.sample, seed=42)

    train = with_log_label(data.filter(F.col("timestamp") < F.lit(TEST_START))).cache()
    test = data.filter(F.col("timestamp") >= F.lit(TEST_START)).cache()
    print(f"\nSample {args.sample:.0%}: train {train.count():,} rows, test {test.count():,} rows")

    mean_reading = train.agg(F.avg(LABEL)).first()[0]
    results = [("mean", "raw", score(test.withColumn("prediction", F.lit(mean_reading))), 0.0)]
    best = None  # (rmse, model, name, target)
    for target in args.targets:
        for name in args.models:
            fit_started = time.perf_counter()
            model = build_pipeline(name, target).fit(train)
            fit_seconds = time.perf_counter() - fit_started
            metrics = score(model.transform(test))
            results.append((name, target, metrics, fit_seconds))
            print(f"{name} / {target}: RMSE {metrics['rmse']:,.2f}, {fit_seconds:.0f}s")
            if best is None or metrics["rmse"] < best[0]:
                best = (metrics["rmse"], model, name, target)

    print(
        f"\n{'model':<6} {'target':<7} {'RMSE':>12} {'MAE':>10} {'RMSLE':>7} "
        f"{'flagged':>8} {'train (s)':>10}"
    )
    for name, target, m, fit_seconds in results:
        print(
            f"{name:<6} {target:<7} {m['rmse']:>12,.2f} {m['mae']:>10,.2f} {m['rmsle']:>7.3f} "
            f"{m['flag_rate']:>8.1%} {fit_seconds:>10.0f}"
        )

    if args.save and best is not None:
        best[1].write().overwrite().save(MODEL_PATH)
        print(f"Best model ({best[2]}, {best[3]}) saved to {MODEL_PATH}")
    print(f"Done in {time.perf_counter() - started:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
