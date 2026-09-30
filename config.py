"""Shared constants and data contract for the whole pipeline.

Import from a subfolder:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from config import THRESHOLD_RATIO, CLEAN_DATA_SCHEMA, ENERGY_RESULTS_SCHEMA
"""

# is_anomaly = abs(residual) > baseline * THRESHOLD_RATIO
# Used by Task 2 (evaluation) and Task 3 (streaming).
THRESHOLD_RATIO = 0.28

# All timestamps are UTC. Convert to local time only for display (Task 4).
TIMEZONE = "UTC"

# Task 1 -> Task 2, Task 3
CLEAN_DATA_SCHEMA = [
    ("building_id", "string"),
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
        StructType, StructField,
        StringType, TimestampType, DoubleType, IntegerType, BooleanType,
    )
    type_map = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "double": DoubleType(),
        "int": IntegerType(),
        "boolean": BooleanType(),
    }
    return StructType([
        StructField(name, type_map[dtype], nullable=True)
        for name, dtype in schema
    ])
