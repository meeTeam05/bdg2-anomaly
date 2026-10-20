"""One SparkSession setup for every task: local mode, Delta Lake, UTC."""

import os
import sys
import time

from common.config import TIMEZONE


def build_spark(app_name: str, extra_packages: list[str] | None = None):
    """Create (or reuse) a SparkSession with Delta Lake enabled.

    extra_packages: Maven coordinates on top of Delta, e.g. the Kafka connector
    for streaming. Driver memory comes from SPARK_DRIVER_MEMORY (default 4g),
    enough for the full BDG2 dataset on a laptop.
    """
    # Imported lazily so `common.config` stays usable without pyspark (dashboard).
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # Python workers must use this interpreter (the venv); otherwise Spark picks the
    # system `python3`, and a minor-version mismatch fails with PYTHON_VERSION_MISMATCH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # collect() turns timestamps into Python datetimes in the process time zone; pin it to
    # UTC so a machine in UTC+7 does not show 2016-01-01 00:00 as 07:00.
    os.environ["TZ"] = TIMEZONE
    time.tzset()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages or []
    ).getOrCreate()
