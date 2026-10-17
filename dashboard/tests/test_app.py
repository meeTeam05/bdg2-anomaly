"""Smoke tests: run the real Streamlit script headlessly against a mock Delta table."""
import re
import sys
from pathlib import Path

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import mock_stream as m  # noqa: E402

APP = str(Path(__file__).resolve().parents[1] / "dashboard.py")


@pytest.fixture(autouse=True)
def clear_streamlit_caches():
    """dashboard.py keeps process-wide caches (_seen, _load); isolate each test."""
    st.cache_resource.clear()
    st.cache_data.clear()


def run_app(monkeypatch, path):
    monkeypatch.setenv("ENERGY_RESULTS_PATH", str(path))
    at = AppTest.from_file(APP, default_timeout=30)
    at.run()
    return at


def table_rows(html, status):
    """Number of building-table rows with this status (the panel also carries data-status)."""
    return len(re.findall(rf'class="bdg-tr[^"]*" data-status="{status}"', html))


def page_html(at):
    return "\n".join(h.value for h in at.get("html"))


def test_missing_table_shows_waiting(monkeypatch, tmp_path):
    at = run_app(monkeypatch, tmp_path / "nope")
    assert not at.exception
    html = page_html(at)
    assert "Waiting for data..." in html
    assert "No data yet" in html


@pytest.fixture
def mock_path(tmp_path):
    path = tmp_path / "energy_results"
    for tick in range(12):  # past the spike (10) and the stall (8)
        m.write_batch(path, m.make_batch(tick))
    return path


def test_renders_with_data(monkeypatch, mock_path):
    at = run_app(monkeypatch, mock_path)
    assert not at.exception
    html = page_html(at)
    # B02 spikes and B04 has baseline 0 and is flagged: 2 anomalies; the rest normal.
    assert table_rows(html, "ANOMALY") == 2
    assert table_rows(html, "NORMAL") == len(m.BUILDINGS) - 2
    assert table_rows(html, "STALE") == 0
    assert "needs attention" in html
    assert "Data source stopped sending" not in html
    assert at.selectbox[0].value in {"B02", "B04"}  # most urgent building opens first


def test_all_stale_after_feed_stops(monkeypatch, mock_path):
    """Nothing advances for an hour on the dashboard clock: every building is STALE and a banner shows."""
    import time as real_time

    base = real_time.time()
    at = run_app(monkeypatch, mock_path)  # first sight at wall-clock ~base
    assert "Data source stopped sending" not in page_html(at)

    monkeypatch.setattr("time.time", lambda: base + 3600)
    at.run()
    assert not at.exception
    html = page_html(at)
    assert table_rows(html, "STALE") == len(m.BUILDINGS)
    assert "Data source stopped sending" in html
    assert "No new data for 60 min" in html
