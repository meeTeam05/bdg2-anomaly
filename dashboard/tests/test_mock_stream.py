import sys
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import mock_stream as m  # noqa: E402
import logic  # noqa: E402
from config import ENERGY_RESULTS_SCHEMA  # noqa: E402


def test_batch_scenarios():
    early = m.make_batch(0).set_index("building_id")
    assert not early["is_anomaly"][[b for b in m.BUILDINGS if b not in (m.ZERO_BASELINE_BUILDING,)]].any()
    assert early.loc[m.ZERO_BASELINE_BUILDING, "baseline"] == 0
    assert early.loc[m.ZERO_BASELINE_BUILDING, "is_anomaly"]

    late = m.make_batch(m.SPIKE_FROM_TICK).set_index("building_id")
    assert late.loc[m.SPIKE_BUILDING, "is_anomaly"]
    assert m.STALL_BUILDING in m.make_batch(m.STALL_AFTER_TICK)["building_id"].values
    assert m.STALL_BUILDING not in m.make_batch(m.STALL_AFTER_TICK + 1)["building_id"].values


def test_batch_matches_contract_residual():
    b = m.make_batch(3)
    assert ((b["meter_reading"] - b["baseline"]) - b["residual"]).abs().max() < 1e-9


def test_write_roundtrip_matches_energy_results_schema(tmp_path):
    path = tmp_path / "energy_results"
    m.write_batch(path, m.make_batch(0))
    m.write_batch(path, m.make_batch(1))

    dt = DeltaTable(str(path))
    assert dt.version() == 1
    delta_types = {f.name: f.type.type for f in dt.schema().fields}
    assert delta_types == {
        "building_id": "string",
        "timestamp": "timestamp",
        "meter_reading": "double",
        "baseline": "double",
        "residual": "double",
        "is_anomaly": "boolean",
    }
    assert list(delta_types) == [c for c, _ in ENERGY_RESULTS_SCHEMA]

    df = logic.read_results(str(path))
    assert len(df) == 2 * len(m.BUILDINGS)
    assert isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype)


def test_logic_handles_missing_table(tmp_path):
    missing = str(tmp_path / "nope")
    assert logic.table_key(missing) is None
    assert logic.read_results(missing) is None


def test_table_key_changes_when_table_is_recreated_at_same_version(tmp_path):
    """reset_demo.sh recreates the table, so versions restart; the cache key must still differ."""
    path = tmp_path / "energy_results"
    m.write_batch(path, m.make_batch(0))
    first = logic.table_key(str(path))
    assert logic.table_key(str(path)) == first

    import shutil
    shutil.rmtree(path)
    m.write_batch(path, m.make_batch(0))
    second = logic.table_key(str(path))
    assert second[1] == first[1]      # same version number...
    assert second != first            # ...but a different table
