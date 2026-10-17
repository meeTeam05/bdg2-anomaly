"""Pure data logic for the dashboard (no Streamlit). See GLOSSARY.md for the terms.

Input is the energy_results table (config.ENERGY_RESULTS_SCHEMA).
"""
import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

STATUS_ORDER = {"ANOMALY": 0, "STALE": 1, "NORMAL": 2}


def table_key(path):
    """(table id, version), or None while the table does not exist yet.

    The id is part of the key because deleting and recreating the table (reset_demo.sh)
    restarts versions at 0; the version alone would collide with a previous run.
    """
    try:
        table = DeltaTable(path)
        return table.metadata().id, table.version()
    except TableNotFoundError:
        return None


def read_results(path):
    """Whole table as a DataFrame, or None while the table does not exist yet."""
    try:
        return DeltaTable(path).to_pandas()
    except TableNotFoundError:
        return None


def prepare(df):
    """Clean the raw table once: drop replay duplicates (last write wins), sort by building and
    time, and settle the nulls so nothing downstream has to.

    is_anomaly: null counts as not anomalous. residual_pct: NaN when baseline is missing or <= 0.
    """
    out = (
        df.drop_duplicates(["building_id", "timestamp"], keep="last")
        .sort_values(["building_id", "timestamp"], kind="stable")
        .reset_index(drop=True)
    )
    out["is_anomaly"] = out["is_anomaly"].fillna(False).astype(bool)
    out["residual_pct"] = (out["residual"] / out["baseline"] * 100).where(out["baseline"] > 0)
    return out


def latest_per_building(prepared):
    return prepared.groupby("building_id", sort=True).tail(1).reset_index(drop=True)


def update_seen(seen, latest, now):
    """Remember, per building, the newest timestamp and the wall-clock time we first saw it.

    `now` is the dashboard wall clock: `timestamp` is replayed history, so it cannot tell whether
    the feed is alive. Any change of the newest timestamp counts as new data, including going
    backwards when the replay restarts after a reset.
    """
    for bid, ts in zip(latest["building_id"], latest["timestamp"]):
        if bid not in seen or ts != seen[bid][0]:
            seen[bid] = (ts, now)
    return seen


def format_age(seconds):
    """Short age for the UI: '41 s', '2 min', '2 h'."""
    if seconds < 90:
        return f"{int(seconds)} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{round(seconds / 3600)} h"


def data_age(seen, now):
    """Seconds since any building last got new data; None before anything was seen."""
    if not seen:
        return None
    return now - max(wall for _, wall in seen.values())


def filter_table(table, status):
    """status: 'ANOMALY' | 'STALE' | 'NORMAL', or 'ALL' to keep every row."""
    if status == "ALL":
        return table
    return table[table["status"] == status].reset_index(drop=True)


def building_table(latest, seen, now, stale_after):
    """One row per building: status, latest readings and a short note. Most urgent first."""
    age = latest["building_id"].map(lambda b: now - seen.get(b, (None, now))[1])
    stale = age > stale_after
    note = pd.Series("", index=latest.index)
    note = note.mask(latest["residual_pct"].isna(), "baseline <= 0")
    note = note.mask(stale, age.map(lambda a: f"no data for {format_age(a)}"))
    status = pd.Series("NORMAL", index=latest.index)
    status = status.mask(latest["is_anomaly"], "ANOMALY").mask(stale, "STALE")
    return (
        latest[["building_id", "meter_reading", "baseline", "residual_pct", "timestamp"]]
        .assign(status=status, note=note)
        .sort_values(["status", "building_id"], key=lambda c: c.map(STATUS_ORDER) if c.name == "status" else c)
        .reset_index(drop=True)
    )


def summary(table):
    counts = table["status"].value_counts()
    return {s: int(counts.get(s, 0)) for s in STATUS_ORDER}


def chart_data(prepared, building_id, n):
    return prepared[prepared["building_id"] == building_id].tail(n).reset_index(drop=True)


def event_log(prepared, building_id, limit):
    """Anomaly events, newest first. building_id=None means all buildings."""
    events = prepared[prepared["is_anomaly"]]
    if building_id is not None:
        events = events[events["building_id"] == building_id]
    return (
        events.sort_values(["timestamp", "building_id"], ascending=[False, True])
        .head(limit)[["timestamp", "building_id", "meter_reading", "baseline", "residual", "residual_pct"]]
        .reset_index(drop=True)
    )
