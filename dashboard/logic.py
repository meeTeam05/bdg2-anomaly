"""Pure data logic for the dashboard (no Streamlit).

Input is the Gold table energy_results (common.config.ENERGY_RESULTS_SCHEMA): one row per
(building_id, meter_type, timestamp). A "meter" below is one (building_id, meter_type).
"""

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from common.config import THRESHOLD_RATIO

METER_KEY = ["building_id", "meter_type"]
STATUS_ORDER = {"ANOMALY": 0, "STALE": 1, "NORMAL": 2}


def table_key(path):
    """(table id, version), or None while the table does not exist yet.

    The id is part of the key because deleting and recreating the table (`make reset`)
    restarts versions at 0; the version alone would collide with a previous run.
    """
    try:
        table = DeltaTable(path)
        return table.metadata().id, table.version()
    except TableNotFoundError:
        return None


def latest_timestamp(table):
    """Newest timestamp of a DeltaTable, from the file statistics in the Delta log (no data
    read). None for an empty table or files written without statistics."""
    actions = pa.table(table.get_add_actions(flatten=True))
    if actions.num_rows == 0 or "max.timestamp" not in actions.column_names:
        return None
    newest = pc.max(actions["max.timestamp"]).as_py()
    return None if newest is None else pd.Timestamp(newest)


def read_window(path, hours):
    """Rows of the last `hours` of replayed time, or None while the table does not exist yet.

    Only files whose timestamp range overlaps the window are opened.
    """
    try:
        table = DeltaTable(path)
    except TableNotFoundError:
        return None
    dataset = table.to_pyarrow_dataset()
    end = latest_timestamp(table)
    if end is None:
        return dataset.to_table().to_pandas()
    start = end - pd.Timedelta(hours=hours)
    return dataset.to_table(filter=ds.field("timestamp") > start).to_pandas()


def read_thresholds(path):
    """Per-meter thresholds from Task 2 ([*METER_KEY, anomaly_threshold]), or None if the
    table does not exist (every meter then uses THRESHOLD_RATIO)."""
    try:
        return DeltaTable(path).to_pandas(columns=[*METER_KEY, "anomaly_threshold"])
    except TableNotFoundError:
        return None


def read_metrics(path):
    """Last row of the streaming job's batch_metrics.csv as a dict, or None."""
    try:
        metrics = pd.read_csv(path)
    except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    return None if metrics.empty else metrics.iloc[-1].to_dict()


def prepare(df, thresholds=None):
    """Clean the raw rows once: drop replay duplicates (last write wins), sort by meter and
    time, settle the nulls, and attach each meter's threshold.

    is_anomaly: null counts as not anomalous. residual_pct: NaN when baseline is missing or
    <= 0. threshold_pct: the meter's anomaly_threshold in %, THRESHOLD_RATIO if it has none.
    """
    out = (
        df.drop_duplicates([*METER_KEY, "timestamp"], keep="last")
        .sort_values([*METER_KEY, "timestamp"], kind="stable")
        .reset_index(drop=True)
    )
    out["is_anomaly"] = out["is_anomaly"].astype("boolean").fillna(False).astype(bool)
    out["residual_pct"] = (out["residual"] / out["baseline"] * 100).where(out["baseline"] > 0)
    ratio = THRESHOLD_RATIO
    if thresholds is not None and not thresholds.empty:
        out = out.merge(thresholds.drop_duplicates(METER_KEY), on=METER_KEY, how="left")
        ratio = out.pop("anomaly_threshold").fillna(THRESHOLD_RATIO)
    out["threshold_pct"] = ratio * 100
    return out


def latest_per_meter(prepared):
    return prepared.groupby(METER_KEY, sort=True).tail(1).reset_index(drop=True)


def meter_ids(df):
    return list(zip(df["building_id"], df["meter_type"], strict=True))


def update_seen(seen, latest, now):
    """Remember, per meter, the newest timestamp and the wall-clock time we first saw it.

    `now` is the dashboard wall clock: `timestamp` is replayed history, so it cannot tell
    whether the feed is alive. Any change of the newest timestamp counts as new data,
    including going backwards when the replay restarts after a reset.
    """
    for meter, ts in zip(meter_ids(latest), latest["timestamp"], strict=True):
        if meter not in seen or ts != seen[meter][0]:
            seen[meter] = (ts, now)
    return seen


def format_age(seconds):
    """Short age for the UI: '41 s', '2 min', '2 h'."""
    if seconds < 90:
        return f"{int(seconds)} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{round(seconds / 3600)} h"


def data_age(seen, now):
    """Seconds since any meter last got new data; None before anything was seen."""
    if not seen:
        return None
    return now - max(wall for _, wall in seen.values())


def meter_table(latest, seen, now, stale_after):
    """One row per meter: status, latest readings and a short note.

    Most urgent first: ANOMALY, STALE, NORMAL, and inside each status the largest
    |residual %| first, so the head of the table is the Top-N.
    """
    age = pd.Series([now - seen.get(m, (None, now))[1] for m in meter_ids(latest)], latest.index)
    stale = age > stale_after
    note = pd.Series("", index=latest.index)
    note = note.mask(latest["residual_pct"].isna(), "baseline <= 0")
    note = note.mask(stale, age.map(lambda a: f"no data for {format_age(a)}"))
    status = pd.Series("NORMAL", index=latest.index)
    status = status.mask(latest["is_anomaly"], "ANOMALY").mask(stale, "STALE")
    columns = [
        "site_id",
        *METER_KEY,
        "meter_reading",
        "baseline",
        "residual_pct",
        "threshold_pct",
        "timestamp",
    ]
    return (
        latest[columns]
        .assign(
            status=status,
            note=note,
            _rank=status.map(STATUS_ORDER),
            _size=-latest["residual_pct"].abs().fillna(-1),
        )
        .sort_values(["_rank", "_size", *METER_KEY], kind="stable")
        .drop(columns=["_rank", "_size"])
        .reset_index(drop=True)
    )


def summary(table):
    counts = table["status"].value_counts()
    return {s: int(counts.get(s, 0)) for s in STATUS_ORDER}


def site_summary(table):
    """One row per site: meters, ANOMALY, STALE, NORMAL. Sites with anomalies first."""
    counts = pd.crosstab(table["site_id"], table["status"]).reindex(
        columns=list(STATUS_ORDER), fill_value=0
    )
    counts.columns.name = None
    return (
        counts.assign(meters=counts.sum(axis=1))
        .reset_index()
        .sort_values(["ANOMALY", "STALE", "site_id"], ascending=[False, False, True])
        .reset_index(drop=True)
    )


def filter_table(table, status="ALL", site=None):
    """status: 'ANOMALY' | 'STALE' | 'NORMAL' | 'ALL'; site: a site_id, or None for all."""
    keep = pd.Series(True, index=table.index)
    if status != "ALL":
        keep &= table["status"] == status
    if site is not None:
        keep &= table["site_id"] == site
    return table[keep].reset_index(drop=True)


def meter_rows(df, meter):
    """Rows of one meter (a (building_id, meter_type) pair), in the order of df."""
    building_id, meter_type = meter
    return df[(df["building_id"] == building_id) & (df["meter_type"] == meter_type)].reset_index(
        drop=True
    )


def event_log(prepared, meter, limit):
    """Anomaly records, newest first. meter=None means all meters."""
    events = prepared[prepared["is_anomaly"]]
    if meter is not None:
        events = meter_rows(events, meter)
    columns = [
        "timestamp",
        "site_id",
        *METER_KEY,
        "meter_reading",
        "baseline",
        "residual",
        "residual_pct",
    ]
    return (
        events.sort_values(["timestamp", *METER_KEY], ascending=[False, True, True])
        .head(limit)[columns]
        .reset_index(drop=True)
    )
