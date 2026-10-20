"""Step 1 of Task 1: copy every raw BDG2 CSV into a Bronze Delta table, unchanged.

Bronze keeps the source content as-is (same rows, same columns, same values,
nulls included). Only the storage format changes, plus two lineage columns:
  _source_file   which CSV the row came from
  _ingested_at   when this ingestion run started (UTC)

Run from the repo root:
    python data_pipeline/ingest.py                  # all sources
    python data_pipeline/ingest.py weather metadata # only these
"""

import argparse
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import BRONZE_DIR, RAW_DATA_DIR
from common.spark_session import build_spark

# One wide table per meter type: `timestamp` + one column per building.
METER_SOURCES = [
    "electricity",
    "chilledwater",
    "steam",
    "hotwater",
    "gas",
    "water",
    "irrigation",
    "solar",
]
SOURCES = [*METER_SOURCES, "weather", "metadata"]

# BDG2 timestamps have no zone ("2016-01-01 00:00:00"). With the session in UTC,
# Spark stores the same wall-clock value; Silver decides the real time zone.
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def read_raw_csv(spark: SparkSession, source: str) -> DataFrame:
    """Read data/raw/<source>.csv with inferred column types."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .csv(str(Path(RAW_DATA_DIR) / f"{source}.csv"))
    )


def ingest_source(spark: SparkSession, source: str, ingested_at: datetime) -> dict:
    """Write one source to data/bronze/<source> and return its row/column counts."""
    started = time.perf_counter()
    df = read_raw_csv(spark, source).withColumns(
        {
            "_source_file": F.lit(f"{source}.csv"),
            "_ingested_at": F.lit(ingested_at),
        }
    )
    target = str(Path(BRONZE_DIR) / source)
    # Overwrite keeps the run idempotent; Delta still keeps older versions in _delta_log.
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(target)

    bronze = spark.read.format("delta").load(target)
    return {
        "source": source,
        "rows": bronze.count(),
        "columns": len(bronze.columns) - 2,  # without the lineage columns
        "seconds": round(time.perf_counter() - started, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # No `choices=` here: on Python 3.11 it rejects the empty default of nargs="*".
    parser.add_argument("sources", nargs="*", help=f"any of {SOURCES}; default: all")
    args = parser.parse_args()
    unknown = set(args.sources) - set(SOURCES)
    if unknown:
        parser.error(f"unknown sources {sorted(unknown)}; choose from {SOURCES}")

    spark = build_spark("bdg2_ingest")
    spark.sparkContext.setLogLevel("WARN")
    # Timezone-aware on purpose: a naive datetime would be read as the machine's local time.
    ingested_at = datetime.now(UTC)

    results = [ingest_source(spark, s, ingested_at) for s in args.sources or SOURCES]
    spark.stop()

    print(f"\n{'source':<14}{'rows':>10}{'columns':>9}{'seconds':>9}")
    for r in results:
        print(f"{r['source']:<14}{r['rows']:>10,}{r['columns']:>9,}{r['seconds']:>9}")
    print(f"Bronze tables written to {BRONZE_DIR}")


if __name__ == "__main__":
    main()
