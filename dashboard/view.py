"""HTML and chart builders for the dashboard. Pure functions, no Streamlit.

Layout and colours follow the design mockup "Energy Dashboard Final". Looks live in GLOBAL_CSS
(classes `bdg-*`, state in `data-*` attributes); the builders only emit structure. Everything
from the data goes through `esc`.
"""

from html import escape as esc

import altair as alt
import pandas as pd

from common.config import ANOMALY_PERSISTENCE_HOURS, DASHBOARD_REFRESH_SEC, STALE_AFTER_SEC

RED, FAINT, DARK = "#e0402f", "#8c979e", "#1b2228"
BAR_HALF = 24  # px: the residual bar spans +-100% around its centre

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&display=swap');
html, body, [class*="st-"], .stApp { font-family: Geist, system-ui, -apple-system, sans-serif; }
.stApp { background: #f4f5f4; }
.block-container { max-width: 1600px; padding: 1.2rem 2rem 3rem; }
header[data-testid="stHeader"] { background: transparent; }
[data-testid="stAppDeployButton"] { display: none; }
[data-testid="stVerticalBlockBorderWrapper"] { background: #fff; border-color: #dfe3e1; border-radius: 6px; }
.stApp h3 { font-size: 15px; font-weight: 600; padding: 0; }

.bdg { font-feature-settings: 'tnum'; color: #14181c; font-size: 14px; line-height: 1.45; }
.bdg * { box-sizing: border-box; }
.bdg .num { text-align: right; }
.bdg .faint { color: #8c979e; }
.bdg .muted { color: #5d6870; }

/* dark header: title, live indicator, banner, KPI tiles */
.bdg-dark { background: #1b2228; color: #e9ecee; padding: 18px 24px 24px; border-radius: 6px; }
.bdg-head { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 10px 24px; margin-bottom: 18px; }
.bdg-head h1 { display: inline; font-size: 19px; font-weight: 600; letter-spacing: -.01em; margin: 0 14px 0 0; padding: 0; white-space: nowrap; color: #e9ecee; }
.bdg-head small { font-size: 13px; color: #9aa5ad; }
.bdg-live { --dot: #5fc59a; display: flex; align-items: center; gap: 9px; font-size: 13px; color: #9aa5ad; white-space: nowrap; }
.bdg-live i { position: relative; width: 8px; height: 8px; border-radius: 50%; background: var(--dot); }
.bdg-live i::after { content: ''; position: absolute; inset: 0; border-radius: 50%; background: var(--dot); animation: bdg-ping 2s ease-out infinite; }
.bdg-dark[data-state=waiting] .bdg-live { --dot: #7d8890; }
.bdg-dark[data-state=allstale] .bdg-live { --dot: #ff5d4f; color: #ff8f84; }
.bdg-dark:not([data-state=live]) .bdg-live i::after { animation: none; }
@keyframes bdg-ping { 0% { transform: scale(1); opacity: .7 } 80%, 100% { transform: scale(2.6); opacity: 0 } }
.bdg-banner { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 16px; padding: 12px 16px; margin-bottom: 14px; background: #ff5d4f; color: #1b0a08; }
.bdg-banner strong { white-space: nowrap; }
.bdg-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 460px), 1fr)); gap: 1px; background: #323c44; border: 1px solid #323c44; }
.bdg-kpis > div { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; }
.bdg-tile { background: #1b2228; padding: 16px 20px 18px; }
.bdg-tile .lab { display: flex; align-items: center; gap: 8px; font-size: 13px; font-weight: 500; color: #9aa5ad; white-space: nowrap; }
.bdg-tile .lab span { font-weight: 400; }
.bdg-tile .big { font-size: 60px; font-weight: 600; letter-spacing: -.04em; line-height: 1; margin-top: 12px; }
.bdg-tile .when { font-size: 26px; font-weight: 600; letter-spacing: -.02em; line-height: 1.1; margin-top: 16px; white-space: nowrap; }
.bdg-tile .when span { color: #9aa5ad; }
.bdg-tile .note { font-size: 12px; color: #7d8890; margin-top: 8px; }
.bdg-tile.anomaly .big { color: #5d6870; }
.bdg-dark[data-alert] .bdg-tile.anomaly { background: #3a1d1f; box-shadow: inset 0 3px 0 #ff5d4f; }
.bdg-dark[data-alert] .bdg-tile.anomaly .lab { color: #ff8f84; }
.bdg-dark[data-alert] .bdg-tile.anomaly .big { color: #ff6b5e; }
.bdg-tile.anomaly .lab span { margin-left: auto; }
.bdg-tile.normal .lab { color: #5fc59a; }
.bdg-tile.normal .lab::before, .bdg-tile.stale .lab::before { content: ''; width: 8px; height: 8px; border-radius: 50%; background: #5fc59a; }
.bdg-tile.stale .lab::before { background: none; border: 1.5px solid #9aa5ad; }
.bdg-dark[data-state=allstale] .bdg-tile.normal { opacity: .35; }
.bdg-dark[data-state=allstale] .bdg-tile.stale .big { color: #fff; }

/* tables */
.bdg-table { border: 1px solid #dfe3e1; border-radius: 5px; overflow: hidden; }
.bdg-scroll { overflow: auto; }
.bdg-scroll > div { min-width: var(--min, 660px); }
.bdg-tr { display: grid; grid-template-columns: var(--cols); gap: 0 var(--gap, 8px); padding: 0 16px; height: 38px; align-items: center; border-bottom: 1px solid #edf0ee; background: #fff; }
.bdg-tr.th { position: sticky; top: 0; z-index: 2; height: auto; padding-top: 9px; padding-bottom: 9px; background: #f7f8f8; border-bottom-color: #dfe3e1; font-size: 12px; font-weight: 500; color: #5d6870; white-space: nowrap; }
.bdg-tr.sel { background: #eef2f0; box-shadow: inset 3px 0 0 #1b2228; }
.bdg-tr.hl { background: #f6f8f7; }
.bdg-tr .id { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.bdg-tr .pct { display: flex; justify-content: flex-end; align-items: center; gap: 8px; }
.bdg-tr .pct b { font-weight: 400; }
.bdg-tr .pct i { color: #7d8890; font-style: italic; }
.bdg-tr[data-status=ANOMALY] .pct b { font-weight: 600; color: #b8321f; }
.bdg-tr[data-status=STALE] .pct, .bdg-tr[data-status=STALE] .reading { color: #7d8890; }
.bdg-tr .why { font-size: 12px; color: #5d6870; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.bdg-empty { padding: 36px; text-align: center; color: #8c979e; background: #fff; }
.bdg-foot { display: flex; align-items: center; gap: 8px; padding: 9px 16px; border-top: 1px solid #dfe3e1; font-size: 12px; color: #5d6870; }
.bdg-foot .swatch { width: 14px; height: 8px; background: #dde8e3; flex: none; }
.bdg-ev .dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: #e0402f; margin-right: 7px; }
.bdg .red { font-weight: 600; color: #b8321f; }

/* residual bar: centre line, +-ratio band, fill */
.bdg-bar { position: relative; width: 48px; height: 8px; flex: none; background: #f1f3f2; }
.bdg-bar s { position: absolute; top: 0; bottom: 0; background: #dde8e3; }
.bdg-bar::after { content: ''; position: absolute; left: 24px; width: 1px; top: -2px; bottom: -2px; background: #8c979e; }
.bdg-bar u { position: absolute; top: 2px; bottom: 2px; background: #5d6870; z-index: 1; }
.bdg-tr[data-status=ANOMALY] .bdg-bar u { background: #e0402f; }
.bdg-tr[data-status=STALE] .bdg-bar, .bdg-tr[data-na] .bdg-bar { opacity: .35; }

/* status chip */
.chip { display: inline-flex; align-items: center; gap: 6px; padding: 2px 8px 2px 7px; border-radius: 3px; border: 1px solid; font-size: 11px; font-weight: 600; letter-spacing: .04em; white-space: nowrap; }
.chip i { width: 6px; height: 6px; border-radius: 50%; border: 1.5px solid; }
[data-status=ANOMALY] .chip { color: #b8321f; background: #fdecea; border-color: #fdecea; }
[data-status=ANOMALY] .chip i { background: #e0402f; border-color: #e0402f; }
[data-status=NORMAL] .chip { color: #1a7550; background: #e6f4ed; border-color: #e6f4ed; }
[data-status=NORMAL] .chip i { background: #1f8a5e; border-color: #1f8a5e; }
[data-status=STALE] .chip { color: #5d6870; background: #fff; border: 1px dashed #a9b2b8; }
[data-status=STALE] .chip i { background: transparent; border-color: #7d8890; }

/* selected-building panel and chart legend */
.bdg-panel { display: flex; flex-wrap: wrap; align-items: center; gap: 14px 28px; padding: 14px 16px; border-radius: 5px; background: #f6f8f7; }
.bdg-panel[data-status=ANOMALY] { background: #fdf3f2; }
.bdg-panel .id { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; font-size: 24px; font-weight: 600; letter-spacing: -.02em; line-height: 1; }
.bdg-panel .id small { flex-basis: 100%; font-size: 12.5px; font-weight: 400; letter-spacing: 0; color: #5d6870; }
.bdg-panel .tag { font-size: 11px; font-weight: 600; letter-spacing: .04em; padding: 3px 8px; border-radius: 3px; background: #e6f4ed; color: #1a7550; }
.bdg-panel[data-status=ANOMALY] .tag { background: #e0402f; color: #fff; }
.bdg-panel[data-status=STALE] .tag { background: #e3e7e5; color: #5d6870; }
.bdg-panel .stats { display: flex; flex-wrap: wrap; gap: 10px 28px; margin-left: auto; }
.bdg-panel .stats > div { white-space: nowrap; font-size: 12px; color: #5d6870; }
.bdg-panel .stats b { display: block; font-size: 18px; font-weight: 600; color: #14181c; }
.bdg-panel .stats small { font-size: 12px; font-weight: 400; color: #8c979e; }
.bdg-panel .stats .base b { color: #5d6870; }
.bdg-panel[data-status=ANOMALY] .stats .resid b { color: #b8321f; }
.bdg-panel .stats .when b { font-size: 14px; font-weight: 500; line-height: 27px; }
.bdg-legend { display: flex; flex-wrap: wrap; gap: 6px 18px; align-items: center; font-size: 12.5px; color: #5d6870; }
.bdg-legend > span { display: flex; gap: 7px; align-items: center; white-space: nowrap; }
.bdg-legend .line { width: 22px; border-top: 2.4px solid #1b2228; }
.bdg-legend .dash { width: 22px; border-top: 1.8px dashed #8c979e; }
.bdg-legend .band { width: 18px; height: 10px; background: #e7efec; }
.bdg-legend .dot { width: 8px; height: 8px; border-radius: 50%; background: #e0402f; }

/* waiting */
.bdg-wait { background: #fff; border: 1px solid #dfe3e1; border-radius: 6px; padding: 96px 24px; display: flex; flex-direction: column; align-items: center; gap: 10px; text-align: center; margin-top: 16px; }
.bdg-wait h2 { font-size: 22px; font-weight: 600; margin: 0; padding: 0; }
.bdg-wait p { color: #5d6870; max-width: 440px; margin: 0; }
.bdg-wait code { margin-top: 6px; font-size: 12.5px; padding: 5px 10px; border-radius: 4px; background: #eef1ef; color: #3d474e; }
</style>
"""


def sgn(value, digits=1):
    return ("+" if value >= 0 else "−") + f"{abs(value):.{digits}f}"


def fmt_ts(ts):
    return f"{ts:%Y-%m-%d %H:%M}"


def pct_text(pct):
    return "n/a" if pd.isna(pct) else sgn(pct) + "%"


def stream_text(metrics):
    """Throughput and latency of the last micro-batch (streaming batch_metrics.csv)."""
    if not metrics:
        return "Replayed 2017 history, not today’s date"
    return (
        f"Last batch: {metrics['rows_per_sec']:.0f} rows/s · "
        f"latency {metrics['max_latency_sec']:.1f} s"
    )


def header_html(kind, counts, latest_ts, age_text, metrics=None):
    """Dark block: title, live indicator, optional banner and four KPI tiles.

    kind: 'waiting' | 'live' | 'allstale'. Counts are meters per status.
    """
    live_text = {
        "waiting": "No data yet",
        "allstale": f"No new data for {age_text}",
        "live": f"Live · refreshes every {DASHBOARD_REFRESH_SEC} s",
    }[kind]
    banner = ""
    if kind == "allstale":
        banner = (
            '<div class="bdg-banner"><strong>Data source stopped sending</strong>'
            f"<span>All {sum(counts.values())} meters have been Stale for more than "
            f"{STALE_AFTER_SEC} s. Their status is unknown, not normal. "
            "Check the producer and the streaming job.</span></div>"
        )
    if kind == "waiting":
        anomaly_hint, when = "", "-"
        n = {key: "-" for key in counts}
    else:
        anomaly_hint = (
            "unknown"
            if kind == "allstale"
            else "needs attention"
            if counts["ANOMALY"]
            else "none right now"
        )
        when = f"{latest_ts:%Y-%m-%d} <span>{latest_ts:%H:%M}</span>"
        n = counts
    alert = " data-alert" if counts["ANOMALY"] else ""
    rule = (
        "Actual vs baseline · anomaly when |residual| is above the meter’s threshold "
        f"for {ANOMALY_PERSISTENCE_HOURS} h in a row"
    )
    return f"""
<div class="bdg bdg-dark" data-state="{kind}"{alert}>
  <header class="bdg-head">
    <div><h1>Building Energy Monitor</h1><small>{rule}</small></div>
    <div class="bdg-live"><i></i>{esc(live_text)}</div>
  </header>
  {banner}
  <section class="bdg-kpis">
    <div>
      <div class="bdg-tile anomaly"><div class="lab">Anomaly<span>{anomaly_hint}</span></div>
        <div class="big">{n["ANOMALY"]}</div></div>
      <div class="bdg-tile normal"><div class="lab">Normal</div>
        <div class="big">{n["NORMAL"]}</div></div>
    </div>
    <div>
      <div class="bdg-tile stale"><div class="lab">Stale<span>· status unknown</span></div>
        <div class="big">{n["STALE"]}</div></div>
      <div class="bdg-tile"><div class="lab">Latest data · UTC</div><div class="when">{when}</div>
        <div class="note">{esc(stream_text(metrics))}</div></div>
    </div>
  </section>
</div>"""


def waiting_html(path):
    return f"""
<div class="bdg bdg-wait">
  <h2>Waiting for data...</h2>
  <p>No records from the streaming job yet. This page checks again every
  {DASHBOARD_REFRESH_SEC} s.</p>
  <code>{esc(str(path))}</code>
</div>"""


def _bar(pct, threshold_pct):
    """Residual bar: centre line, the meter's +-threshold band, and the residual fill."""
    p = 0.0 if pd.isna(pct) else max(-1.0, min(1.0, pct / 100.0))
    w = abs(p) * BAR_HALF
    band = min(1.0, threshold_pct / 100.0) * BAR_HALF
    return (
        f'<span class="bdg-bar"><s style="left:{BAR_HALF - band:.1f}px;width:{2 * band:.1f}px">'
        f'</s><u style="left:{BAR_HALF - w if p < 0 else BAR_HALF:.1f}px;width:{w:.1f}px"></u>'
        "</span>"
    )


def _table(head, rows, cols, max_height, foot="", min_width=660, empty="Nothing to show."):
    rows = rows or f'<div class="bdg-empty">{empty}</div>'
    return (
        f'<div class="bdg bdg-table"><div class="bdg-scroll" style="max-height:{max_height}px;'
        f'--cols:{cols};--min:{min_width}px"><div>{head}{rows}</div></div>{foot}</div>'
    )


def sites_html(sites):
    """Overview: one row per site (logic.site_summary)."""
    head = (
        '<div class="bdg-tr th"><span>Site</span><span class="num">Meters</span>'
        '<span class="num">Anomaly</span><span class="num">Stale</span>'
        '<span class="num">Normal</span></div>'
    )
    rows = "".join(
        f'<div class="bdg-tr" data-status="{"ANOMALY" if s.ANOMALY else "NORMAL"}">'
        f'<span class="id">{esc(str(s.site_id))}</span><span class="num">{s.meters}</span>'
        f'<span class="num{" red" if s.ANOMALY else " faint"}">{s.ANOMALY}</span>'
        f'<span class="num faint">{s.STALE}</span><span class="num faint">{s.NORMAL}</span></div>'
        for s in sites.itertuples(index=False)
    )
    return _table(
        head,
        rows,
        "minmax(0,1.4fr) repeat(4,minmax(0,1fr))",
        540,
        min_width=320,
        empty="No sites yet.",
    )


def _meter_row(r, selected):
    stale, na = r.status == "STALE", pd.isna(r.residual_pct)
    pct = "-" if stale else "<i>n/a</i>" if na else sgn(r.residual_pct) + "%"
    sel = " sel" if (r.building_id, r.meter_type) == selected else ""
    return (
        f'<div class="bdg-tr{sel}" data-status="{r.status}"{" data-na" if na else ""}>'
        f'<span class="id" title="{esc(str(r.building_id))}">{esc(str(r.building_id))}</span>'
        f'<span class="muted">{esc(str(r.site_id))}</span>'
        f'<span><span class="chip"><i></i>{r.status}</span></span>'
        f'<span class="num reading">{r.meter_reading:.1f}</span>'
        f'<span class="num faint">{r.baseline:.1f}</span>'
        f'<span class="pct"><b>{pct}</b>'
        f"{_bar(float('nan') if stale else r.residual_pct, r.threshold_pct)}</span>"
        f'<span class="why">{esc(r.note)}</span></div>'
    )


def meters_html(table, selected, total):
    """Top rows of logic.meter_table (already cut to TOP_N by the caller) out of `total`."""
    head = (
        '<div class="bdg-tr th"><span>Building</span><span>Site</span><span>Status</span>'
        '<span class="num">Actual kWh</span><span class="num">Baseline kWh</span>'
        '<span class="num">Residual</span><span>Note</span></div>'
    )
    rows = "".join(_meter_row(r, selected) for r in table.itertuples(index=False))
    foot = (
        f'<div class="bdg-foot"><span>Top {len(table)} of {total}</span>'
        '<span class="faint">·</span><span class="swatch"></span><span>meter’s threshold</span>'
        '<span class="faint">·</span><span>Anomaly → Stale → Normal, largest residual first'
        "</span></div>"
    )
    cols = "minmax(0,1.8fr) 70px 102px minmax(0,1fr) minmax(0,1fr) 124px minmax(0,1fr)"
    return _table(head, rows, cols, 540, foot, 820, "No meters in this filter.")


def panel_html(row):
    """Summary of the selected meter above the chart. `row` is one row of meter_table."""
    unit = "<small>kWh</small>"
    when = "Last record (UTC)" if row.status == "STALE" else "As of (UTC)"
    return f"""
<div class="bdg bdg-panel" data-status="{row.status}">
  <div class="id">{esc(str(row.building_id))}<span class="tag">{row.status}</span>
    <small>site {esc(str(row.site_id))} · {esc(str(row.meter_type))} meter</small></div>
  <div class="stats">
    <div>Actual<b>{row.meter_reading:.1f} {unit}</b></div>
    <div class="base">Baseline<b>{row.baseline:.1f} {unit}</b></div>
    <div class="resid">Residual<b>{pct_text(row.residual_pct)}</b></div>
    <div class="base">Threshold<b>±{row.threshold_pct:.0f}%</b></div>
    <div class="when">{when}<b>{fmt_ts(row.timestamp)}</b></div>
  </div>
</div>"""


def legend_html(n_points, threshold_pct):
    return f"""
<div class="bdg bdg-legend">
  <span><i class="line"></i>Actual</span>
  <span><i class="dash"></i>Baseline</span>
  <span><i class="band"></i>±{threshold_pct:.0f}% of baseline (meter’s threshold)</span>
  <span><i class="dot"></i>Anomaly</span>
  <span class="faint" style="margin-left:auto">Last {n_points} hours</span>
</div>"""


def events_html(events, selected):
    head = (
        '<div class="bdg-tr th"><span>Time (UTC)</span><span>Site</span><span>Building</span>'
        '<span class="num">Actual kWh</span><span class="num">Baseline kWh</span>'
        '<span class="num">Residual kWh</span><span class="num">Residual %</span></div>'
    )
    rows = "".join(
        f'<div class="bdg-tr{" hl" if (e.building_id, e.meter_type) == selected else ""}" '
        f'style="height:36px"><span class="muted">{fmt_ts(e.timestamp)}</span>'
        f'<span class="muted">{esc(str(e.site_id))}</span>'
        f'<span class="id"><i class="dot"></i>{esc(str(e.building_id))}</span>'
        f'<span class="num">{e.meter_reading:.1f}</span>'
        f'<span class="num faint">{e.baseline:.1f}</span>'
        f'<span class="num">{sgn(e.residual)}</span>'
        f'<span class="num red">{pct_text(e.residual_pct)}</span></div>'
        for e in events.itertuples(index=False)
    )
    cols = "150px 70px minmax(0,1.6fr) minmax(0,1fr) minmax(0,1fr) minmax(0,1fr) 110px"
    return _table(head, rows, cols, 320, min_width=820, empty="No anomaly events.").replace(
        "bdg-table", "bdg-table bdg-ev", 1
    )


def chart(df, stale):
    """Actual vs baseline for one meter, with its +-threshold band and anomaly dots."""
    d = df[
        ["timestamp", "meter_reading", "baseline", "residual", "residual_pct", "is_anomaly"]
    ].copy()
    ratio = df["threshold_pct"] / 100
    d["lo"] = (d["baseline"] * (1 - ratio)).clip(lower=0)
    d["hi"] = (d["baseline"] * (1 + ratio)).clip(lower=0)
    d["Time (UTC)"] = d["timestamp"].map(fmt_ts)
    d["Actual"] = d["meter_reading"].map("{:.1f} kWh".format)
    d["Baseline"] = d["baseline"].map("{:.1f} kWh".format)
    d["Residual"] = [
        f"{sgn(r)} kWh · {pct_text(p)}"
        for r, p in zip(d["residual"], d["residual_pct"], strict=True)
    ]

    axis = dict(labelColor=FAINT, titleColor="#a3adb3", titleFontWeight="normal")
    x = alt.X(
        "timestamp:T",
        scale=alt.Scale(type="utc"),
        title="Time (UTC)",
        axis=alt.Axis(
            format="%m-%d %H:%M",
            tickCount=6,
            domainColor="#c6cdd0",
            tickColor="#c6cdd0",
            grid=False,
            **axis,
        ),
    )
    y = alt.Y(
        "meter_reading:Q",
        title="kWh",
        scale=alt.Scale(zero=False),
        axis=alt.Axis(gridColor="#edf0ee", domain=False, ticks=False, **axis),
    )
    base = alt.Chart(d)
    tips = [alt.Tooltip(c) for c in ("Time (UTC)", "Actual", "Baseline", "Residual")]
    layers = [
        base.mark_area(color="#e7efec").encode(
            x=x, y=alt.Y("lo:Q", scale=alt.Scale(zero=False)), y2="hi:Q"
        ),
        base.mark_line(color=FAINT, strokeWidth=1.6, strokeDash=[5, 4]).encode(x=x, y="baseline:Q"),
        base.mark_line(color=DARK, strokeWidth=2).encode(x=x, y=y),
        base.transform_filter("datum.is_anomaly")
        .mark_circle(color=RED, size=60, stroke="#fff", strokeWidth=1.5)
        .encode(x=x, y=y),
        base.mark_circle(opacity=0, size=220).encode(x=x, y=y, tooltip=tips),  # hover targets
    ]
    if stale:
        end = alt.Chart(d[["timestamp"]].tail(1).assign(label="no data after this"))
        layers.append(end.mark_rule(color=FAINT, strokeDash=[2, 3]).encode(x=x))
        layers.append(
            end.mark_text(
                align="right", dx=-4, dy=-6, baseline="bottom", fontSize=11, color="#5d6870"
            ).encode(x=x, y=alt.value(0), text="label:N")
        )
    return alt.layer(*layers).properties(height=340).configure_view(strokeWidth=0)
