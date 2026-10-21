"""Task 2 starting point: train a baseline consumption model on the Silver table.

Minimal end-to-end version: Silver -> time split -> MLlib Pipeline
(StringIndexer -> VectorAssembler -> LinearRegression) -> RMSE/MAE on the test year.
It also proves the Task 1 hand-off works ("Done when train_baseline.py runs on Silver").

Run from the repo root (after data_pipeline/preprocess.py):
    python ml_model/train_baseline.py                 # 1% sample, nothing saved
    python ml_model/train_baseline.py --sample 1 --save
"""

import argparse
import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from common.config import CLEAN_DATA_PATH, MODEL_PATH, TEST_START
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
# Solar meters generate power; anomaly detection covers consumption meters only.
EXCLUDED_METER_TYPES = ["solar"]


def build_pipeline() -> Pipeline:
    indexers = [
        # "keep" maps categories unseen in training (e.g. a new site) to an extra index.
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL
    ]
    assembler = VectorAssembler(
        inputCols=NUMERIC + [f"{c}_idx" for c in CATEGORICAL], outputCol="features"
    )
    model = LinearRegression(featuresCol="features", labelCol=LABEL)
    return Pipeline(stages=[*indexers, assembler, model])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", type=float, default=0.01, help="fraction of rows (0-1]")
    parser.add_argument("--save", action="store_true", help=f"save the model to {MODEL_PATH}")
    args = parser.parse_args()

    started = time.perf_counter()
    spark = build_spark("bdg2_train_baseline")
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(CLEAN_DATA_PATH)
    data = silver.filter(~F.col("meter_type").isin(EXCLUDED_METER_TYPES))
    if args.sample < 1:
        data = data.sample(fraction=args.sample, seed=42)

    train = data.filter(F.col("timestamp") < F.lit(TEST_START))
    test = data.filter(F.col("timestamp") >= F.lit(TEST_START))

    model = build_pipeline().fit(train)
    predictions = model.transform(test)

    evaluate = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")
    rmse = evaluate.evaluate(predictions, {evaluate.metricName: "rmse"})
    mae = evaluate.evaluate(predictions, {evaluate.metricName: "mae"})
    mean_rmse = evaluate.evaluate(
        test.withColumn("prediction", F.lit(train.agg(F.avg(LABEL)).first()[0])),
        {evaluate.metricName: "rmse"},
    )

    print(f"\nSample {args.sample:.0%}: train {train.count():,} rows, test {test.count():,} rows")
    print(f"LinearRegression  RMSE {rmse:,.2f}  MAE {mae:,.2f}")
    print(f"Mean baseline     RMSE {mean_rmse:,.2f}")
    predictions.select("building_id", "meter_type", "timestamp", LABEL, "prediction").show(5)

    if args.save:
        model.write().overwrite().save(MODEL_PATH)
        print(f"Model saved to {MODEL_PATH}")
    print(f"Done in {time.perf_counter() - started:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
