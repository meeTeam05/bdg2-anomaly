"""Shared constants and data contract for the whole pipeline.

After `pip install -e .` at the repo root, import from anywhere:
    from common.config import THRESHOLD_RATIO, CLEAN_DATA_PATH, CLEAN_DATA_SCHEMA
"""

import os
from pathlib import Path

# Absolute paths so every task resolves the same table regardless of cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]

# All data lives under data/ (not in git). Override with BDG2_DATA_DIR, e.g. for tests.
DATA_DIR = Path(os.environ.get("BDG2_DATA_DIR", REPO_ROOT / "data"))

# Medallion layers (all Delta Lake), see docs/TASKS.md section 3.
RAW_DATA_DIR = str(DATA_DIR / "raw")  # BDG2 CSVs
BRONZE_DIR = str(DATA_DIR / "bronze")  # one Delta table per source file
CLEAN_DATA_PATH = str(DATA_DIR / "silver" / "clean_data")
MODEL_PATH = str(DATA_DIR / "models" / "baseline_model")
ENERGY_RESULTS_PATH = str(DATA_DIR / "gold" / "energy_results")
ENERGY_RESULTS_CHECKPOINT = str(DATA_DIR / "checkpoints" / "energy_results")

# Silver and Gold tables are partitioned by this column.
PARTITION_COLUMN = "site_id"

# Time-based split: train on 2016, test on 2017.
# Task 2 splits on it; Task 3 replays only timestamp >= TEST_START.
TEST_START = "2017-01-01 00:00:00"

# is_anomaly = abs(residual) > baseline * THRESHOLD_RATIO
# Used by Task 2 (evaluation) and Task 3 (streaming).
THRESHOLD_RATIO = 0.28

# All timestamps are UTC. Convert to local time only for display (Task 4).
TIMEZONE = "UTC"

# Task 1 -> Task 2, Task 3
CLEAN_DATA_SCHEMA = [
    ("building_id", "string"),
    ("site_id", "string"),
    ("timestamp", "timestamp"),
    ("meter_reading", "double"),
    ("air_temperature", "double"),
    ("dew_temperature", "double"),
    ("hour_of_day", "int"),
    ("day_of_week", "int"),
    ("is_weekend", "int"),
    ("square_feet", "double"),
    ("primary_use", "string"),
]

# Task 3 -> Task 4
ENERGY_RESULTS_SCHEMA = [
    ("building_id", "string"),
    ("site_id", "string"),
    ("timestamp", "timestamp"),
    ("meter_reading", "double"),
    ("baseline", "double"),
    ("residual", "double"),
    ("is_anomaly", "boolean"),
]


def to_spark_struct_type(schema):
    """Convert a schema list above into pyspark.sql.types.StructType.

    Imports pyspark lazily so this module works without pyspark installed
    (e.g. the Task 4 dashboard).
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "double": DoubleType(),
        "int": IntegerType(),
        "boolean": BooleanType(),
    }
    return StructType([StructField(name, type_map[dtype], nullable=True) for name, dtype in schema])
