"""Fake Task 3: append one batch per interval to a Delta table in the energy_results schema.

Lets the dashboard be developed and demoed without Kafka or Spark. Scenarios:
  B02 spikes (x1.8) from tick SPIKE_FROM_TICK    -> ANOMALY
  B03 stops sending after tick STALL_AFTER_TICK  -> STALE
  B04 always has baseline 0                      -> "baseline <= 0" edge case
  others stay within +-10% of baseline           -> NORMAL

    python mock_stream.py [--path PATH] [--interval 5] [--ticks 0] [--reset]
Then: ENERGY_RESULTS_PATH=<same path> streamlit run dashboard.py
"""
import argparse
import math
import random
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DASHBOARD_REFRESH_SEC, ENERGY_RESULTS_SCHEMA, THRESHOLD_RATIO  # noqa: E402

DEFAULT_PATH = Path(__file__).resolve().parent / ".mock_output" / "energy_results"
START = datetime(2017, 1, 1, tzinfo=timezone.utc)
BUILDINGS = [f"B{i:02d}" for i in range(1, 9)]
SPIKE_BUILDING, SPIKE_FROM_TICK = "B02", 10
STALL_BUILDING, STALL_AFTER_TICK = "B03", 8
ZERO_BASELINE_BUILDING = "B04"

_ARROW_TYPES = {
    "string": pa.string(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "double": pa.float64(),
    "int": pa.int32(),
    "boolean": pa.bool_(),
}
ARROW_SCHEMA = pa.schema([(name, _ARROW_TYPES[dtype]) for name, dtype in ENERGY_RESULTS_SCHEMA])


def make_batch(tick):
    """One record per building for the given tick (1 tick = 1 hour of replayed data)."""
    ts = START + timedelta(hours=tick)
    rows = []
    for i, bid in enumerate(BUILDINGS):
        if bid == STALL_BUILDING and tick > STALL_AFTER_TICK:
            continue
        if bid == ZERO_BASELINE_BUILDING:
            baseline, actual = 0.0, 5.0
        else:
            baseline = 100.0 + 10.0 * i + 30.0 * math.sin(2 * math.pi * ts.hour / 24)
            actual = baseline * (1 + random.Random(f"{bid}-{tick}").uniform(-0.1, 0.1))
            if bid == SPIKE_BUILDING and tick >= SPIKE_FROM_TICK:
                actual = baseline * 1.8
        residual = actual - baseline
        rows.append({
            "building_id": bid,
            "timestamp": ts,
            "meter_reading": actual,
            "baseline": baseline,
            "residual": residual,
            "is_anomaly": abs(residual) > baseline * THRESHOLD_RATIO,
        })
    return pd.DataFrame(rows, columns=[c for c, _ in ENERGY_RESULTS_SCHEMA])


def write_batch(path, df):
    table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)
    write_deltalake(str(path), table, mode="append")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--path", default=str(DEFAULT_PATH))
    parser.add_argument("--interval", type=float, default=DASHBOARD_REFRESH_SEC)
    parser.add_argument("--ticks", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--reset", action="store_true", help="delete an existing table at --path first")
    args = parser.parse_args()

    # Ticks restart at 0 (replayed 2017 timestamps), so appending to an old run would look STALE.
    if Path(args.path).exists():
        if not args.reset:
            sys.exit(f"{args.path} already exists. Delete it or pass --reset.")
        shutil.rmtree(args.path)

    tick = 0
    while args.ticks == 0 or tick < args.ticks:
        batch = make_batch(tick)
        write_batch(args.path, batch)
        print(f"tick {tick}: wrote {len(batch)} rows to {args.path}", flush=True)
        tick += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
