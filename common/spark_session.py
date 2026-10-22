"""One SparkSession setup for every task: local mode, Delta Lake, UTC.

Works on Linux, macOS and Windows (the Windows workarounds come from Task 3).
"""

import hashlib
import os
import sys
import time
from pathlib import Path

from common.config import TIMEZONE

LOOPBACK = "127.0.0.1"
# Windows only, see _with_jars(): cached Delta (+ extra) jars, one folder per package set.
JARS_CACHE = Path.home() / ".ivy2-bdg2"


def build_spark(
    app_name: str,
    extra_packages: list[str] | None = None,
    config: dict[str, str] | None = None,
):
    """Create (or reuse) a SparkSession with Delta Lake enabled.

    extra_packages: Maven coordinates on top of Delta, e.g. the Kafka connector for streaming.
    config: extra Spark settings, applied before the session starts (static ones included).
    Driver memory comes from SPARK_DRIVER_MEMORY (default 6g), enough for the full BDG2
    dataset on a laptop.
    """
    # Imported lazily so `common.config` stays usable without pyspark (dashboard).
    from pyspark.sql import SparkSession

    # On Windows Hadoop needs winutils.exe; without it Spark fails deep inside the JVM.
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        sys.exit(
            "HADOOP_HOME is not set. Windows needs winutils.exe + hadoop.dll, "
            "see streaming/README.md."
        )
    # Python workers must use this interpreter (the venv); otherwise Spark picks the system
    # `python3` (version mismatch) or, on Windows, the Microsoft Store alias (never connects).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # Local mode: everything on 127.0.0.1. With the machine hostname (VPN DNS such as Tailscale)
    # or "localhost" (may resolve to IPv6) the executor can hang downloading the jars.
    os.environ.setdefault("SPARK_LOCAL_IP", LOOPBACK)
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", LOOPBACK)
    # collect() turns timestamps into Python datetimes in the process time zone; pin it to UTC
    # so a machine in UTC+7 does not show 2016-01-01 00:00 as 07:00. time.tzset() does not
    # exist on Windows: there, compare timestamps as epochs (as the streaming tests do).
    if hasattr(time, "tzset"):
        os.environ["TZ"] = TIMEZONE
        time.tzset()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.driver.host", LOOPBACK)
        .config("spark.driver.bindAddress", LOOPBACK)
        .config("spark.sql.session.timeZone", TIMEZONE)
        # Smaller shuffle partitions (default 64m): with 32 cores sharing one 4-6 GB driver
        # heap, 64m sort buffers per task hit Java heap limits on the full dataset.
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32m")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    for key, value in (config or {}).items():
        builder = builder.config(key, value)
    return _with_jars(builder, extra_packages or []).getOrCreate()


def _with_jars(builder, extra_packages: list[str]):
    """Add the Delta (+ extra) jars.

    Linux/macOS: the standard delta-spark helper (Ivy downloads once, then reuses its cache).
    Windows: a downloaded package is served to the local executor over Spark RPC at every
    startup, which sometimes hangs forever. So the first run downloads into JARS_CACHE/<hash>
    and later runs pass those jars as `local:` URIs ("already on this machine"). The folder
    is keyed by the package set, so a Delta-only cache never hides a missing Kafka jar.
    Delete JARS_CACHE after changing the pyspark / delta-spark version.
    """
    from delta import configure_spark_with_delta_pip

    if os.name != "nt":
        return configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
    key = hashlib.sha1(",".join(sorted(extra_packages)).encode()).hexdigest()[:10]
    cache = JARS_CACHE / key
    jars = sorted((cache / "jars").glob("*.jar"))
    if jars:
        # file:///C:/x.jar -> local:///C:/x.jar
        uris = ",".join("local:" + jar.as_uri().removeprefix("file:") for jar in jars)
        return builder.config("spark.jars", uris)
    builder = builder.config("spark.jars.ivy", str(cache))
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
