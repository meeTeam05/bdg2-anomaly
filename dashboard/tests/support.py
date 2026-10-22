"""Builders for energy_results rows shared by the dashboard tests."""

import pandas as pd

from common.config import ENERGY_RESULTS_SCHEMA, THRESHOLD_RATIO
from dashboard import logic

EVIL = "<img src=x onerror=alert(1)>"


def rec(bid, ts, actual, baseline, anomaly=None, site="S1", meter_type="electricity"):
    residual = actual - baseline
    if anomaly is None:
        anomaly = abs(residual) > baseline * THRESHOLD_RATIO
    return {
        "building_id": bid,
        "meter_type": meter_type,
        "site_id": site,
        "timestamp": pd.Timestamp(ts, tz="UTC"),
        "meter_reading": float(actual),
        "baseline": float(baseline),
        "residual": float(residual),
        "is_anomaly": anomaly,
    }


def frame(rows):
    return pd.DataFrame(rows, columns=[c for c, _ in ENERGY_RESULTS_SCHEMA])


def latest_of(rows, thresholds=None):
    return logic.latest_per_meter(logic.prepare(frame(rows), thresholds))


def make_table(rows, now=0.0, advance=0.0, thresholds=None):
    """(prepared rows, meter_table) as the dashboard builds them, `advance` s after first sight."""
    prepared = logic.prepare(frame(rows), thresholds)
    latest = logic.latest_per_meter(prepared)
    seen = {}
    logic.update_seen(seen, latest, now)
    return prepared, logic.meter_table(latest, seen, now + advance, stale_after=30)


def status_of(table):
    return dict(zip(table["building_id"], table["status"], strict=True))


def first_row(table, bid):
    return next(table[table["building_id"] == bid].itertuples(index=False))
