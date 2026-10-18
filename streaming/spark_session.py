"""One SparkSession setup for every Task 3 script: local[*], Delta Lake, Kafka connector, UTC."""

import os
import sys
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root
from config import TIMEZONE  # noqa: E402

# Spark runs on the host, no Spark container (docs/INTEGRATION_GUIDELINES.md).
SPARK_MASTER = "local[*]"
DRIVER_MEMORY = "4g"
LOOPBACK = "127.0.0.1"
# The connector version must match pyspark exactly, otherwise Spark fails with ClassNotFound.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
JARS_CACHE = Path.home() / ".ivy2-bdg2"  # see with_jars()


def build_spark(app_name):
    # On Windows Hadoop needs winutils.exe; without it Spark fails deep inside the JVM.
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        sys.exit("HADOOP_HOME is not set. Windows needs winutils.exe + hadoop.dll, see streaming/README.md.")
    # Python workers must run this interpreter (the venv); a bare `python` on Windows can be
    # the Microsoft Store alias and the worker never connects back.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # Local mode: everything on 127.0.0.1. With the machine hostname (VPN DNS such as Tailscale)
    # or "localhost" (may resolve to IPv6) the executor can hang downloading the Delta/Kafka jars.
    os.environ.setdefault("SPARK_LOCAL_IP", LOOPBACK)
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", LOOPBACK)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.driver.host", LOOPBACK)
        .config("spark.driver.bindAddress", LOOPBACK)
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.ui.showConsoleProgress", "false")  # keep the per-batch log lines readable
        .config("spark.sql.shuffle.partitions", "8")
        # Delta rebuilds its log state after every commit with 50 tasks by default: about 6-9 s
        # per micro-batch in local mode, longer than the trigger. A few tasks are enough here.
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = with_jars(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def with_jars(builder):
    """Add the Delta + Kafka jars.

    First run: download them with spark.jars.packages into JARS_CACHE. Later runs: pass the cached
    jars as `local:` URIs. A downloaded package is served to the local executor over Spark RPC at
    every startup, which on Windows sometimes hangs forever; `local:` means "already on this
    machine", so nothing is served. Delete JARS_CACHE after changing the pyspark / delta-spark version.
    """
    jars = sorted((JARS_CACHE / "jars").glob("*.jar"))
    if jars:
        # file:///C:/x.jar -> local:///C:/x.jar
        return builder.config("spark.jars", ",".join("local:" + jar.as_uri().removeprefix("file:") for jar in jars))
    builder = builder.config("spark.jars.ivy", str(JARS_CACHE))
    return configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE])
