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
# Per-meter "normal" profile from 2016; Task 3 joins it before calling the model.
METER_PROFILE_PATH = str(DATA_DIR / "models" / "meter_profile")
# Per-meter anomaly thresholds calibrated by ml_model/evaluate.py --save.
METER_THRESHOLDS_PATH = str(DATA_DIR / "models" / "meter_thresholds")
ENERGY_RESULTS_PATH = str(DATA_DIR / "gold" / "energy_results")
ENERGY_RESULTS_CHECKPOINT = str(DATA_DIR / "checkpoints" / "energy_results")

# Silver and Gold tables are partitioned by this column.
PARTITION_COLUMN = "site_id"

# BDG2 meter files (data/raw/<type>.csv). Silver keeps all of them; one row is one
# reading of one meter, keyed by (building_id, meter_type, timestamp).
METER_TYPES = [
    "electricity",
    "chilledwater",
    "steam",
    "hotwater",
    "gas",
    "water",
    "irrigation",
    "solar",  # generation, not consumption: keep in Silver, skip for anomaly detection
]

# Meter types the baseline model and anomaly detection cover (Task 2 trains on these,
# Task 3 scores only these). Other meters stay in Silver. Electricity only: on the 2017
# test year the per-meter profile alone mis-flags ~29% of normal electricity hours vs
# 64-86% for the seasonal/intermittent types (relative error ~100%), so a single 28%
# threshold cannot work for them (ml_model/README.md).
ANOMALY_METER_TYPES = ["electricity"]

# Time-based split: train on 2016, test on 2017.
# Task 2 splits on it; Task 3 replays only timestamp >= TEST_START.
TEST_START = "2017-01-01 00:00:00"

# Anomaly rule (Task 3 Gold, decided by the injected anomaly test in ml_model/evaluate.py):
#   exceeds    = abs(residual) > baseline * anomaly_threshold
#   is_anomaly = exceeds for ANOMALY_PERSISTENCE_HOURS consecutive hours of that meter
# anomaly_threshold is per meter (METER_THRESHOLDS_PATH: 99th percentile of the meter's
# normal deviation in 2017-Q1); THRESHOLD_RATIO is the fallback for meters without one.
# On 2017-04..12 this cuts false alarms from 29% (fixed 28%, 1 h) to 4% of normal hours.
THRESHOLD_RATIO = 0.28
ANOMALY_PERSISTENCE_HOURS = 3

# All timestamps are UTC. Convert to local time only for display (Task 4).
# hour_of_day / day_of_week / is_weekend are computed from the site's LOCAL time,
# because occupancy follows local clocks.
TIMEZONE = "UTC"

# Task 1 -> Task 2, Task 3
CLEAN_DATA_SCHEMA = [
    ("building_id", "string"),
    ("meter_type", "string"),
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
    ("meter_type", "string"),
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
